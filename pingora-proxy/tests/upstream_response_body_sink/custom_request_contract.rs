// Copyright 2026 Cloudflare, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0.

//! Real custom-pump coverage for request termination, framing rejection and
//! source EOF normalization. Observations survive a deliberately failed exchange.

use super::*;
use pingora_proxy::{RequestRelayPlan, RequestReplayPolicy, UpstreamRequestBodyDisposition};

pub(super) const SOURCE_EOF: &str = "contract-source-eof";
const TERMINATE: &str = "contract-terminate";
const ORDINARY: &str = "contract-ordinary";
const BODYLESS: &str = "contract-bodyless";
const STREAMED: &str = "contract-streamed";
const BLOCKED: &[u8] = b"blocked";
const BOUND: Duration = Duration::from_secs(2);
const ABSENCE_BOUND: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Event {
    UpstreamReady,
    HeaderWritten,
    Written(Bytes),
    Finished,
    Hook(RequestBodyEvent),
    TerminationArmed,
    Terminate,
    SourceRead,
    SourceEof,
    ReadAfterEof,
    ReportedUnfinished,
    Logged,
}

#[derive(Default)]
struct Probe {
    events: Mutex<Vec<Event>>,
    changed: tokio::sync::Notify,
}

static PROBES: Lazy<Mutex<std::collections::HashMap<String, Arc<Probe>>>> =
    Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

pub(super) fn is_script(script: &str) -> bool {
    matches!(
        script,
        SOURCE_EOF | TERMINATE | ORDINARY | BODYLESS | STREAMED
    )
}

pub(super) async fn wait_for_termination(script: Option<&str>) -> bool {
    if script != Some(TERMINATE) {
        return false;
    }
    let probe = PROBES.lock().unwrap().get(TERMINATE).cloned().unwrap();
    probe.wait(Event::Terminate).await;
    true
}

pub(super) fn record(script: Option<&str>, event: Event) {
    let probe = script.and_then(|script| PROBES.lock().unwrap().get(script).cloned());
    if let Some(probe) = probe {
        probe.events.lock().unwrap().push(event);
        probe.changed.notify_waiters();
    }
}

pub(super) fn record_for_session(session: &Session, event: Event) {
    record(
        session
            .req_header()
            .headers
            .get(CUSTOM_REQUEST_BODY_SCRIPT_HEADER)
            .and_then(|header| header.to_str().ok()),
        event,
    );
}

pub(super) fn relay_plan(session: &Session) -> RequestRelayPlan {
    let disposition = match session.get_header_bytes(CUSTOM_REQUEST_BODY_SCRIPT_HEADER) {
        script if script == BODYLESS.as_bytes() => UpstreamRequestBodyDisposition::Bodyless,
        script if script == STREAMED.as_bytes() => UpstreamRequestBodyDisposition::Streamed,
        _ => return RequestRelayPlan::ordinary(),
    };
    RequestRelayPlan {
        disposition,
        replay: RequestReplayPolicy::Never,
    }
}

pub(super) async fn filter_action(
    session: &mut Session,
    body: &Option<Bytes>,
    event: RequestBodyEvent,
) -> Result<bool> {
    record_for_session(session, Event::Hook(event));
    let terminate_armed = PROBES
        .lock()
        .unwrap()
        .get(TERMINATE)
        .is_some_and(|probe| probe.snapshot().contains(&Event::TerminationArmed));
    if session.get_header_bytes(CUSTOM_REQUEST_BODY_SCRIPT_HEADER) == TERMINATE.as_bytes()
        && terminate_armed
        && event == RequestBodyEvent::Data
        && body.as_ref().is_some_and(|body| !body.is_empty())
    {
        // Honor the public hook contract: complete a local response first.
        let mut response = ResponseHeader::build(403, None)?;
        response.insert_header(http::header::CONTENT_LENGTH, "0")?;
        session
            .write_response_header(Box::new(response), true)
            .await?;
        record_for_session(session, Event::Terminate);
        return Ok(true);
    }
    Ok(false)
}

impl Probe {
    fn register(script: &str) -> Arc<Self> {
        let probe = Arc::new(Self::default());
        assert!(
            PROBES
                .lock()
                .unwrap()
                .insert(script.to_owned(), probe.clone())
                .is_none(),
            "each scenario must own its probe"
        );
        probe
    }

    fn snapshot(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    fn count(&self, predicate: impl Fn(&Event) -> bool) -> usize {
        self.snapshot()
            .iter()
            .filter(|event| predicate(event))
            .count()
    }

    fn written(&self) -> Vec<u8> {
        self.snapshot()
            .into_iter()
            .filter_map(|event| match event {
                Event::Written(bytes) => Some(bytes),
                _ => None,
            })
            .flatten()
            .collect()
    }

    async fn wait_written(&self, expected: &[u8]) {
        tokio::time::timeout(BOUND, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let written = self.written();
                assert!(
                    expected.starts_with(&written),
                    "unexpected bytes: {written:?}"
                );
                if written == expected {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("missing bytes {expected:?}; events: {:?}", self.snapshot()));
    }

    async fn wait(&self, expected: Event) {
        tokio::time::timeout(BOUND, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.snapshot().contains(&expected) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("missing {expected:?}; events: {:?}", self.snapshot()));
    }

    async fn expect_none(&self, predicate: impl Fn(&Event) -> bool) {
        let result = tokio::time::timeout(ABSENCE_BOUND, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                assert!(
                    !self.snapshot().iter().any(&predicate),
                    "forbidden event: {:?}",
                    self.snapshot()
                );
                notified.await;
            }
        })
        .await;
        assert!(
            result.is_err(),
            "absence observation returned before its deadline"
        );
        assert_eq!(self.count(predicate), 0, "whole-log absence count");
    }
}

async fn start_upload(script: &str) -> (TcpStream, Arc<Probe>) {
    let probe = Probe::register(script);
    let mut io = TcpStream::connect(("127.0.0.1", init().custom_proxy_port))
        .await
        .unwrap();
    io.write_all(
        format!(
            "POST /{script} HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\
         {CUSTOM_REQUEST_BODY_SCRIPT_HEADER}: {script}\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    probe.wait(Event::UpstreamReady).await;
    (io, probe)
}

async fn write_fragmented_chunk(
    io: &mut TcpStream,
    probe: &Probe,
    chunk: &[u8],
    expected: &mut Vec<u8>,
) {
    io.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
        .await
        .unwrap();
    for byte in chunk {
        io.write_all(&[*byte]).await.unwrap();
        expected.push(*byte);
        // Gate each byte on actual forwarding, forcing partial chunk callbacks.
        probe.wait_written(expected).await;
    }
    io.write_all(b"\r\n").await.unwrap();
}

async fn completed_record(script: &str, probe: &Probe) -> CustomRequestBodyRecord {
    probe.wait(Event::Logged).await;
    let record = wait_for_custom_request_body_record(&format!("/{script}")).await;
    assert_eq!(record.logging_calls, 1);
    assert_eq!(probe.count(|event| *event == Event::UpstreamReady), 1);
    assert_eq!(probe.count(|event| *event == Event::Logged), 1);
    record
}

#[tokio::test]
async fn ordinary_custom_upload_observes_real_writes_and_completion() {
    let (mut io, probe) = start_upload(ORDINARY).await;
    probe.wait(Event::HeaderWritten).await;
    let mut expected = Vec::new();
    write_fragmented_chunk(&mut io, &probe, b"data", &mut expected).await;
    write_fragmented_chunk(&mut io, &probe, BLOCKED, &mut expected).await;
    io.write_all(b"0\r\n\r\n").await.unwrap();
    let response = read_h1_response_header(&mut io).await;
    assert!(response.starts_with(b"HTTP/1.1 200"));
    let record = completed_record(ORDINARY, &probe).await;
    assert!(!record.had_error, "{record:?}");
    assert_request_body_terminal_events(
        &record.events,
        RequestBodyEvent::Complete,
        DataEventCount::AtLeastOne,
        ORDINARY,
    );
    assert_eq!(probe.count(|event| *event == Event::HeaderWritten), 1);
    assert_eq!(probe.count(|event| *event == Event::Finished), 1);
    assert_eq!(probe.written(), b"datablocked");
    probe.expect_none(|event| *event == Event::Terminate).await;
}

#[tokio::test]
async fn custom_request_terminate_fails_closed_without_forwarding_the_chunk() {
    let (mut io, probe) = start_upload(TERMINATE).await;
    probe.wait(Event::HeaderWritten).await;
    let mut expected = Vec::new();
    write_fragmented_chunk(&mut io, &probe, b"data", &mut expected).await;
    let prefix_writes = probe.count(|event| matches!(event, Event::Written(_)));
    record(Some(TERMINATE), Event::TerminationArmed);
    // Only the first byte of the next declared chunk arrives. Terminate must
    // act on that partial callback, without waiting for the remaining bytes.
    io.write_all(b"7\r\nb").await.unwrap();
    probe.wait(Event::Terminate).await;
    let response = read_h1_response_header(&mut io).await;
    assert!(response.starts_with(b"HTTP/1.1 403"));
    let record = completed_record(TERMINATE, &probe).await;
    assert!(record.had_error, "{record:?}");
    assert_eq!(
        record.error_type,
        Some(pingora_error::ErrorType::InternalError)
    );
    assert!(
        record
            .error_text
            .as_deref()
            .unwrap()
            .contains("terminate is not supported on custom connector sessions"),
        "{record:?}"
    );
    assert_eq!(probe.count(|event| *event == Event::HeaderWritten), 1);
    assert_eq!(probe.count(|event| *event == Event::Terminate), 1);
    assert_eq!(probe.written(), b"data");
    probe
        .expect_none(|event| {
            (matches!(event, Event::Written(_))
                && probe.count(|event| matches!(event, Event::Written(_))) != prefix_writes)
                || *event == Event::Finished
        })
        .await;
    assert_eq!(probe.written(), b"data");
    assert_eq!(
        probe.count(|event| matches!(event, Event::Written(_))),
        prefix_writes
    );
}

#[tokio::test]
async fn custom_nonordinary_dispositions_fail_before_the_upstream_header_write() {
    for script in [BODYLESS, STREAMED] {
        let (_io, probe) = start_upload(script).await;
        probe
            .expect_none(|event| {
                matches!(
                    event,
                    Event::HeaderWritten | Event::Written(_) | Event::Finished
                )
            })
            .await;
        let record = completed_record(script, &probe).await;
        assert_eq!(
            probe.count(|event| matches!(
                event,
                Event::HeaderWritten | Event::Written(_) | Event::Finished
            )),
            0
        );
        assert!(record.had_error, "{script}: {record:?}");
        assert_eq!(
            record.error_type,
            Some(pingora_error::ErrorType::InternalError)
        );
        assert!(record.error_text.as_deref().unwrap().contains(
            "a non-ordinary upstream request body disposition is not supported on custom connector sessions"
        ), "{script}: {record:?}");
    }
}

#[tokio::test]
async fn custom_source_eof_finishes_once_despite_stale_body_done() {
    let probe = Probe::register(SOURCE_EOF);
    let mut io = TcpStream::connect(("127.0.0.1", init().custom_source_eof_proxy_port))
        .await
        .unwrap();
    io.write_u8(1).await.unwrap();
    probe.wait(Event::UpstreamReady).await;
    probe.wait(Event::HeaderWritten).await;
    probe
        .wait(Event::Written(Bytes::from_static(b"data")))
        .await;
    probe.wait(Event::SourceEof).await;
    probe.wait(Event::ReportedUnfinished).await;
    let record = completed_record(SOURCE_EOF, &probe).await;
    assert!(!record.had_error, "{record:?}");
    assert_eq!(
        record.events,
        vec![RequestBodyEvent::Data, RequestBodyEvent::Complete]
    );
    assert_eq!(probe.count(|event| *event == Event::HeaderWritten), 1);
    assert_eq!(probe.count(|event| *event == Event::SourceRead), 2);
    assert_eq!(probe.count(|event| *event == Event::SourceEof), 1);
    assert_eq!(probe.count(|event| *event == Event::Finished), 1);
    probe
        .expect_none(|event| {
            matches!(
                event,
                Event::Hook(RequestBodyEvent::Abandoned) | Event::Terminate | Event::ReadAfterEof
            )
        })
        .await;
    assert_eq!(probe.count(|event| *event == Event::SourceRead), 2);
    assert_eq!(probe.count(|event| *event == Event::Finished), 1);
}
