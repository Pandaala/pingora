// Copyright 2026 Cloudflare, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::protocols::{
    raw_connect::ProxyDigest, GetProxyDigest, GetSocketDigest, GetTimingDigest, Peek, Shutdown,
    SocketDigest, Ssl, TimingDigest, UniqueID, UniqueIDType,
};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const WRITE_BOUND: Duration = Duration::from_millis(100);
const WATCHDOG: Duration = Duration::from_secs(4);
const TRAILER_WIRE: &[u8] = b"0\r\nx-end: one\r\nx-end: two\r\n\r\n";

#[derive(Default, Debug)]
struct Observation {
    bytes: Vec<u8>,
    remaining: Option<usize>,
    block_flush: bool,
    flush_polls: usize,
    waker: Option<Waker>,
}

#[derive(Debug)]
struct ControlledIo(Arc<Mutex<Observation>>);

impl AsyncRead for ControlledIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending // The client stays connected throughout the test.
    }
}
impl AsyncWrite for ControlledIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut state = self.0.lock().unwrap();
        let n = state.remaining.unwrap_or(buf.len()).min(buf.len());
        if n == 0 {
            state.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        state.bytes.extend_from_slice(&buf[..n]);
        if let Some(remaining) = state.remaining.as_mut() {
            *remaining -= n;
        }
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut state = self.0.lock().unwrap();
        state.flush_polls += 1;
        if state.block_flush {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[async_trait::async_trait]
impl Shutdown for ControlledIo {
    async fn shutdown(&mut self) {}
}
impl UniqueID for ControlledIo {
    fn id(&self) -> UniqueIDType {
        0
    }
}
impl Ssl for ControlledIo {}
impl GetTimingDigest for ControlledIo {
    fn get_timing_digest(&self) -> Vec<Option<TimingDigest>> {
        vec![]
    }
}
impl GetProxyDigest for ControlledIo {
    fn get_proxy_digest(&self) -> Option<Arc<ProxyDigest>> {
        None
    }
}
impl GetSocketDigest for ControlledIo {
    fn get_socket_digest(&self) -> Option<Arc<SocketDigest>> {
        None
    }
}
impl Peek for ControlledIo {}

fn trailers() -> http::HeaderMap {
    let mut map = http::HeaderMap::new();
    map.append("x-end", HeaderValue::from_static("one"));
    map.append("x-end", HeaderValue::from_static("two"));
    map
}

async fn session() -> (HttpSession, Arc<Mutex<Observation>>) {
    let state = Arc::new(Mutex::new(Observation::default()));
    let mut session = HttpSession::new(Box::new(ControlledIo(state.clone())));
    session.update_resp_headers = false;
    session.request_header = Some(Box::new(RequestHeader::build("GET", b"/", None).unwrap()));
    let mut header = ResponseHeader::build(200, None).unwrap();
    header.insert_header(TRANSFER_ENCODING, "chunked").unwrap();
    session
        .write_response_header(Box::new(header))
        .await
        .unwrap();
    session.write_body(b"abc").await.unwrap();
    session.write_timeout = Some(WRITE_BOUND);
    assert_eq!(session.body_bytes_sent(), 3);
    state.lock().unwrap().bytes.clear();
    (session, state)
}

fn arm(state: &Arc<Mutex<Observation>>, flush: bool) {
    let mut state = state.lock().unwrap();
    state.remaining = if flush { None } else { Some(4) };
    state.block_flush = flush;
    state.flush_polls = 0;
}
fn release(state: &Arc<Mutex<Observation>>) {
    let mut state = state.lock().unwrap();
    state.remaining = None;
    state.block_flush = false;
    if let Some(waker) = state.waker.take() {
        waker.wake();
    }
}
async fn write(session: &mut HttpSession, queued: bool, map: &http::HeaderMap) -> Result<()> {
    if queued {
        session.send_proxy_task(HttpTask::Trailer(Some(Box::new(map.clone()))));
        session.write_proxy_tasks().await.map(|_| ())
    } else {
        session.write_trailers(map).await.map(|_| ())
    }
}

#[tokio::test]
async fn trailer_write_and_flush_stalls_obey_session_timeout() {
    for queued in [false, true] {
        for flush in [false, true] {
            let (mut session, state) = session().await;
            arm(&state, flush);
            let error = tokio::time::timeout(WATCHDOG, write(&mut session, queued, &trailers()))
                .await
                .expect("trailer timeout must beat watchdog")
                .unwrap_err();
            assert_eq!(error.etype(), &WriteTimedout);
            assert!(!session.body_writer.finished());
            assert_eq!(session.body_bytes_sent(), 3);
            let observed = state.lock().unwrap();
            assert_eq!(
                observed.bytes,
                if flush {
                    TRAILER_WIRE
                } else {
                    &TRAILER_WIRE[..4]
                }
            );
            assert_eq!(observed.flush_polls > 0, flush);
        }
    }
}

#[tokio::test]
async fn queued_trailer_deadline_survives_write_and_flush_cancellation() {
    for flush in [false, true] {
        let (mut session, state) = session().await;
        arm(&state, flush);
        session.send_proxy_task(HttpTask::Trailer(Some(Box::new(trailers()))));
        let mut cancellations = 0;
        let result = tokio::time::timeout(WATCHDOG, async {
            loop {
                match tokio::time::timeout(Duration::from_millis(15), session.write_proxy_tasks())
                    .await
                {
                    Ok(result) => break result,
                    Err(_) => cancellations += 1,
                }
            }
        })
        .await
        .expect("cancellation must not restart deadline");
        assert_eq!(result.unwrap_err().etype(), &WriteTimedout);
        assert!(cancellations >= 2);
        assert!(!session.body_writer.finished());
        assert_eq!(session.body_bytes_sent(), 3);
        assert_eq!(
            state.lock().unwrap().bytes,
            if flush {
                TRAILER_WIRE
            } else {
                &TRAILER_WIRE[..4]
            }
        );
        release(&state);
        assert_eq!(
            session.write_proxy_tasks().await.unwrap_err().etype(),
            &WriteTimedout
        );
    }
}

#[tokio::test]
async fn queued_trailer_resumption_preserves_partial_bytes_and_one_terminator() {
    for flush in [false, true] {
        let (mut session, state) = session().await;
        session.write_timeout = Some(Duration::from_secs(2));
        arm(&state, flush);
        session.send_proxy_task(HttpTask::Trailer(Some(Box::new(trailers()))));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), session.write_proxy_tasks())
                .await
                .is_err()
        );
        release(&state);
        assert!(session.write_proxy_tasks().await.unwrap());
        session.send_proxy_task(HttpTask::Done);
        assert!(session.write_proxy_tasks().await.unwrap());
        assert_eq!(state.lock().unwrap().bytes, TRAILER_WIRE);
        assert!(session.body_writer.finished());
        assert_eq!(session.body_bytes_sent(), 3);
    }
}

#[tokio::test]
async fn trailer_success_and_validation_preserve_wire_and_accounting() {
    for queued in [false, true] {
        let (mut session, state) = session().await;
        write(&mut session, queued, &trailers()).await.unwrap();
        session.finish_body().await.unwrap();
        assert_eq!(state.lock().unwrap().bytes, TRAILER_WIRE);
        assert!(session.body_writer.finished());
        assert_eq!(session.body_bytes_sent(), 3);
        let (mut session, state) = self::session().await;
        let mut invalid = trailers();
        invalid.insert(CONTENT_LENGTH, HeaderValue::from_static("3"));
        assert_eq!(
            write(&mut session, queued, &invalid)
                .await
                .unwrap_err()
                .etype(),
            &WriteError
        );
        assert!(state.lock().unwrap().bytes.is_empty());
        assert!(!session.body_writer.finished());
    }
}

#[tokio::test]
async fn trailers_without_timeout_can_wait_and_then_complete() {
    for queued in [false, true] {
        let (mut session, state) = session().await;
        session.write_timeout = None;
        arm(&state, true);
        let observed = state.clone();
        let release_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(observed.lock().unwrap().flush_polls > 0);
            release(&observed);
        });
        let start = Instant::now();
        tokio::time::timeout(WATCHDOG, write(&mut session, queued, &trailers()))
            .await
            .unwrap()
            .unwrap();
        release_task.await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(180));
        assert_eq!(state.lock().unwrap().bytes, TRAILER_WIRE);
    }
}

#[tokio::test]
async fn trailer_minimum_rate_overrides_fixed_timeout_and_has_one_second_floor() {
    for queued in [false, true] {
        let (mut session, state) = session().await;
        session.min_send_rate = Some(1024);
        session.write_timeout = Some(Duration::from_millis(10));
        arm(&state, true);
        let start = Instant::now();
        let error = tokio::time::timeout(WATCHDOG, write(&mut session, queued, &trailers()))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.etype(), &WriteTimedout);
        assert!(start.elapsed() >= Duration::from_millis(950));
        assert_eq!(state.lock().unwrap().bytes, TRAILER_WIRE);
    }
}

#[tokio::test]
async fn trailer_minimum_rate_uses_wire_length_not_application_body_count() {
    for queued in [false, true] {
        let (mut session, state) = session().await;
        session.min_send_rate = Some(1024);
        session.write_timeout = Some(Duration::from_millis(10));
        let mut map = http::HeaderMap::new();
        map.insert("x-large", HeaderValue::from_str(&"a".repeat(2048)).unwrap());
        arm(&state, true);
        let observed = state.clone();
        let release_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1300)).await;
            release(&observed);
        });
        tokio::time::timeout(WATCHDOG, write(&mut session, queued, &map))
            .await
            .unwrap()
            .unwrap();
        release_task.await.unwrap();
        assert!(session.body_writer.finished());
        assert_eq!(session.body_bytes_sent(), 3);
        assert!(state.lock().unwrap().bytes.len() > 2048);
    }
}
