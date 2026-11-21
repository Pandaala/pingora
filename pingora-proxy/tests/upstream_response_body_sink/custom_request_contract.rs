// Copyright 2026 Cloudflare, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0.

//! Real custom-pump coverage for request termination, framing rejection and
//! source EOF normalization. Observations survive a deliberately failed exchange.

use super::*;
use pingora_proxy::{RequestRelayPlan, RequestReplayPolicy, UpstreamRequestBodyDisposition};

pub(super) const SOURCE_EOF: &str = "contract-source-eof";
pub(super) const SETUP_NO_WRITER: &str = "setup-missing-writer";
pub(super) const SETUP_HOOK_ERROR: &str = "setup-hook-error";
pub(super) const SETUP_UNFINISHED: &str = "setup-unfinished";
pub(super) const SETUP_FINISHED: &str = "setup-finished";
pub(super) const RETRY_FIRST: &str = "retry-first-session";
pub(super) const RETRY_SECOND: &str = "retry-second-session";
pub(super) const RETRY_SUCCESS: &str = "retry-final-session";
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
    Cleanup,
    Released,
    SessionAbandoned,
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
pub(super) struct Probe {
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
    pub(super) fn register(script: &str) -> Arc<Self> {
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

    pub(super) fn assert_failed_writer_retired(&self) {
        assert_eq!(self.count(|event| *event == Event::HeaderWritten), 1);
        assert_eq!(self.count(|event| *event == Event::Logged), 1);
        assert_eq!(
            self.count(|event| matches!(event, Event::Cleanup | Event::Released)),
            0,
            "{:?}",
            self.snapshot()
        );
    }

    pub(super) fn assert_session_cleanup(&self, abandoned: usize, released: usize) {
        assert_eq!(
            self.count(|e| *e == Event::SessionAbandoned),
            abandoned,
            "{:?}",
            self.snapshot()
        );
        assert_eq!(
            self.count(|e| *e == Event::Released),
            released,
            "{:?}",
            self.snapshot()
        );
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

    pub(super) async fn expect_none(&self, predicate: impl Fn(&Event) -> bool) {
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
    probe.assert_session_cleanup(0, 1);
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

#[tokio::test]
async fn custom_finish_failure_still_delivers_complete_response() {
    for (path, script, end_upload) in [
        ("/custom_reject_finish", CUSTOM_REJECT_FINISH, true),
        (
            "/custom_reject_filtered_finish",
            CUSTOM_REJECT_FILTERED_FINISH,
            false,
        ),
    ] {
        let probe = Probe::register(script);
        let (mut io, record) =
            run_h1_custom_upload_script(path, script, &[b"upload"], end_upload).await;
        let mut body = [0; 6];
        tokio::time::timeout(BOUND, io.read_exact(&mut body))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&body, b"result");
        assert!(!record.had_error, "{record:?}");
        if end_upload {
            assert_request_body_terminal_events(
                &record.events,
                RequestBodyEvent::Complete,
                DataEventCount::Any,
                script,
            );
        } else {
            assert!(!record.events.is_empty());
            assert!(
                record
                    .events
                    .iter()
                    .all(|event| *event == RequestBodyEvent::Data),
                "{script}: {record:?}"
            );
        }
        assert_eq!(probe.count(|e| *e == Event::Finished), 1);
        probe.assert_failed_writer_retired();
        probe.assert_session_cleanup(0, 0);
    }
}

#[tokio::test]
async fn custom_header_error_cancels_blocked_upload_writer() {
    let path = "/custom_blocked_writer";
    let probe = Probe::register(CUSTOM_BLOCK_WRITE);
    let mut io = TcpStream::connect(("127.0.0.1", init().custom_proxy_port))
        .await
        .unwrap();
    io.write_all(format!("POST {path} HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n{CUSTOM_REQUEST_BODY_SCRIPT_HEADER}: {CUSTOM_BLOCK_WRITE}\r\n\r\n1\r\na\r\n").as_bytes()).await.unwrap();
    let record = wait_for_custom_request_body_record(path).await;
    assert!(record
        .error_text
        .as_deref()
        .unwrap()
        .contains("header failed while writer was pending"));
    probe.assert_failed_writer_retired();
    probe.assert_session_cleanup(1, 0);
}

#[tokio::test]
async fn custom_failed_upgraded_upload_finishes_on_downstream_half_close() {
    let path = "/custom_upgrade_failed_upload";
    let probe = Probe::register(CUSTOM_UPGRADE_REJECT_WRITE);
    let mut io = TcpStream::connect(("127.0.0.1", init().custom_proxy_port))
        .await
        .unwrap();
    io.write_all(format!("GET {path} HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n{}: {}\r\n{CUSTOM_REQUEST_BODY_SCRIPT_HEADER}: {CUSTOM_UPGRADE_REJECT_WRITE}\r\n\r\n", CUSTOM_EMPTY_UPGRADE_HEADER.0, CUSTOM_EMPTY_UPGRADE_HEADER.1).as_bytes()).await.unwrap();
    assert!(read_h1_response_header(&mut io)
        .await
        .starts_with(b"HTTP/1.1 101"));
    io.write_all(b"client").await.unwrap();
    let mut body = [0; 6];
    tokio::time::timeout(BOUND, io.read_exact(&mut body))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&body, b"server");
    io.shutdown().await.unwrap();
    let record = wait_for_custom_request_body_record(path).await;
    assert!(!record.had_error, "{record:?}");
    probe.assert_failed_writer_retired();
    probe.assert_session_cleanup(0, 0);
}

#[tokio::test]
async fn custom_truncated_response_keeps_failed_upload_error() {
    let path = "/custom_truncated_after_rejection";
    let script = CUSTOM_TRUNCATED_AFTER_REJECTION;
    let probe = Probe::register(script);
    let mut io = TcpStream::connect(("127.0.0.1", init().custom_proxy_port))
        .await
        .unwrap();
    io.write_all(format!("POST {path} HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n{CUSTOM_REQUEST_BODY_SCRIPT_HEADER}: {script}\r\n\r\n1\r\na\r\n").as_bytes()).await.unwrap();
    let record = wait_for_custom_request_body_record(path).await;
    assert!(!record.events.is_empty());
    assert!(
        record
            .events
            .iter()
            .all(|event| *event == RequestBodyEvent::Data),
        "{script}: {record:?}"
    );
    assert_eq!(
        record.error_type,
        Some(pingora_error::ErrorType::WriteError)
    );
    assert_eq!(
        record.error_source,
        Some(pingora_error::ErrorSource::Upstream)
    );
    assert!(record
        .error_text
        .as_deref()
        .unwrap()
        .contains("scripted custom request-body writer rejection"));
    probe.assert_failed_writer_retired();
    probe.assert_session_cleanup(1, 0);
}

#[tokio::test]
async fn custom_session_setup_abandons_only_an_unfinished_response() {
    for (script, expected_error, abandons) in [
        (
            SETUP_UNFINISHED,
            "can't extract custom reader from upstream",
            1,
        ),
        (
            SETUP_FINISHED,
            "can't extract custom reader from upstream",
            0,
        ),
        (
            SETUP_NO_WRITER,
            "custom upstream must have a custom message writer",
            1,
        ),
        (SETUP_HOOK_ERROR, "custom forwarding setup failed", 1),
    ] {
        let probe = Probe::register(script);
        let response = reqwest::Client::new()
            .get(format!("{}/{script}", init().custom_base_url()))
            .header(CUSTOM_REQUEST_BODY_SCRIPT_HEADER, script)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_server_error());
        let record = completed_record(script, &probe).await;
        assert!(record
            .error_text
            .as_deref()
            .unwrap()
            .contains(expected_error));
        assert!(record.events.is_empty());
        assert_eq!(probe.count(|e| *e == Event::HeaderWritten), 1);
        probe.assert_session_cleanup(abandons, 0);
    }
}

#[tokio::test]
async fn custom_retry_abandons_each_failed_session_once_and_releases_success() {
    let first = Probe::register(RETRY_FIRST);
    let second = Probe::register(RETRY_SECOND);
    let success = Probe::register(RETRY_SUCCESS);
    let response = reqwest::Client::new()
        .get(format!("{}/{RETRY_SUCCESS}", init().custom_base_url()))
        .header(CUSTOM_REQUEST_BODY_SCRIPT_HEADER, RETRY_SUCCESS)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    success.wait(Event::Logged).await;
    let record = wait_for_custom_request_body_record(&format!("/{RETRY_SUCCESS}")).await;
    assert!(!record.had_error);
    assert!(!record.events.contains(&RequestBodyEvent::Abandoned));
    assert_eq!(record.logging_calls, 1);
    assert_eq!(success.count(|e| *e == Event::UpstreamReady), 3);
    for probe in [&first, &second] {
        assert_eq!(probe.count(|e| *e == Event::HeaderWritten), 1);
        // The scripted connector already reports response_finished() == true,
        // but a joined task has failed, so the exchange still needs explicit cleanup.
        probe.assert_session_cleanup(1, 0);
    }
    assert_eq!(success.count(|e| *e == Event::HeaderWritten), 1);
    success.assert_session_cleanup(0, 1);
}
