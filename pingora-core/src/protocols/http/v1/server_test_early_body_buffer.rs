// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::protocols::http::body_buffer::{InMemoryRequestBodyBuffer, RequestBodyBuffer};
use async_trait::async_trait;
use http::StatusCode;
use tokio_test::io::Builder;

fn init_log() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Like a disk-spill impl: awaits (yields) inside next_chunk before reading
/// at the cursor, opening a cancellation window mid-replay.
struct YieldingBuffer {
    buf: BytesMut,
    body: Option<Bytes>,
    offset: usize,
    chunk_cap: usize,
}

impl YieldingBuffer {
    fn new() -> Self {
        Self::with_chunk_cap(usize::MAX)
    }

    /// Cap each replay chunk at `chunk_cap` bytes so a small test body
    /// still replays in multiple chunks, like a large body against the
    /// real 64 KiB replay chunk size.
    fn with_chunk_cap(chunk_cap: usize) -> Self {
        YieldingBuffer {
            buf: BytesMut::new(),
            body: None,
            offset: 0,
            chunk_cap,
        }
    }
}

#[async_trait]
impl RequestBodyBuffer for YieldingBuffer {
    async fn write(&mut self, data: &Bytes) -> Result<()> {
        self.buf.extend_from_slice(data);
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        if self.body.is_none() {
            self.body = Some(self.buf.split().freeze());
        }
        Ok(())
    }

    async fn rewind(&mut self) -> Result<()> {
        self.offset = 0;
        Ok(())
    }

    async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
        tokio::task::yield_now().await;
        let Some(body) = self.body.as_ref() else {
            return Ok(None);
        };
        if self.offset >= body.len() {
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(max_bytes.min(self.chunk_cap))
            .min(body.len());
        Ok(Some(body.slice(self.offset..end)))
    }

    fn consume(&mut self, bytes: usize) {
        self.offset = self.offset.saturating_add(bytes);
    }
}

#[tokio::test]
async fn replay_survives_select_cancellation() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(YieldingBuffer::new()))
        .unwrap();
    assert_eq!(
        http_stream.read_body_bytes().await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    // Poll the replay branch once (it suspends at the impl's internal
    // await), then drop the future — exactly what the proxy body pump does
    // to read_body_or_idle when a losing tokio::select! branch is cancelled.
    {
        let mut fut = tokio_test::task::spawn(http_stream.read_body_or_idle(false));
        assert!(fut.poll().is_pending());
    }
    // The cancelled call must not have consumed the chunk.
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    assert!(http_stream
        .read_body_or_idle(false)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn early_body_buffer_captures_and_replays() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, b"abc".as_slice());
    for _ in 0..2 {
        assert!(http_stream.begin_request_body_replay().await.unwrap());
        assert_eq!(
            http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
            b"abc".as_slice()
        );
        assert!(http_stream
            .read_body_or_idle(false)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn expect_continue_capture_sends_100_before_body() {
    init_log();
    let input1 =
        b"POST / HTTP/1.1\r\nHost: pingora.org\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\n";
    let output_100 = b"HTTP/1.1 100 Continue\r\n\r\n";
    let input2 = b"abc";
    // The mock io script is ordered: the body read stays pending until the
    // 100 is written, mirroring a client that only sends the body after
    // seeing the 100. Draining without write_continue_response() first
    // would hang here, which is exactly the deadlock the contract warns
    // about.
    let mock_io = Builder::new()
        .read(&input1[..])
        .write(&output_100[..])
        .read(&input2[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    http_stream.write_continue_response().await.unwrap();
    assert_eq!(
        http_stream.read_body_bytes().await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    assert!(http_stream
        .read_body_or_idle(false)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn write_continue_response_only_sends_once() {
    init_log();
    let input1 =
        b"POST / HTTP/1.1\r\nHost: pingora.org\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\n";
    let input2 = b"abc";
    // Ordered script: once the 100 write expectation is consumed, any
    // duplicate 100 bytes would mismatch the next scripted action and
    // fail the mock.
    let output_100 = b"HTTP/1.1 100 Continue\r\n\r\n";
    let output_200 = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let mock_io = Builder::new()
        .read(&input1[..])
        .write(&output_100[..])
        .read(&input2[..])
        .write(&output_200[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    http_stream.write_continue_response().await.unwrap();
    // A repeat is a no-op: nothing extra reaches the wire.
    http_stream.write_continue_response().await.unwrap();
    assert_eq!(
        http_stream.read_body_bytes().await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    assert!(http_stream
        .read_body_or_idle(false)
        .await
        .unwrap()
        .is_none());
    // The 100 does not block the final response.
    let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response.insert_header(header::CONTENT_LENGTH, "0").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&response)
        .await
        .unwrap();
}

#[tokio::test]
async fn rewind_from_mid_replay_re_serves_full_body() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 6\r\n\r\nabcdef";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    // 2-byte replay chunks: the 6-byte body replays in multiple chunks,
    // like a large body against the real 64 KiB replay chunk size.
    http_stream
        .set_request_body_buffer(Box::new(YieldingBuffer::with_chunk_cap(2)))
        .unwrap();
    assert_eq!(
        http_stream.read_body_bytes().await.unwrap().unwrap(),
        b"abcdef".as_slice()
    );

    // Attempt 1 fails mid-replay: one chunk was delivered (its commit
    // still pending) when the retry rewinds from the Replaying state.
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        b"ab".as_slice()
    );
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    let mut replayed = Vec::new();
    while let Some(chunk) = http_stream.read_body_or_idle(false).await.unwrap() {
        replayed.extend_from_slice(&chunk);
    }
    assert_eq!(replayed, b"abcdef");

    // Attempt 2 fails with a read in flight: the read is cancelled
    // mid-poll (losing select! branch) before the retry rewinds from the
    // Replaying state.
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        b"ab".as_slice()
    );
    {
        let mut fut = tokio_test::task::spawn(http_stream.read_body_or_idle(false));
        assert!(fut.poll().is_pending());
    }
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    let mut replayed = Vec::new();
    while let Some(chunk) = http_stream.read_body_or_idle(false).await.unwrap() {
        replayed.extend_from_slice(&chunk);
    }
    assert_eq!(replayed, b"abcdef");
}

#[tokio::test]
async fn early_response_mid_replay_keeps_downstream_keepalive() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let output = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .write(&output[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Infinite);
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    assert_eq!(
        http_stream.read_body_bytes().await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    // Precondition of the bug: the replay override reports "not done"
    // while the real downstream body was fully captured above.
    assert!(!http_stream.is_body_done());
    // Upstream answered before consuming the replayed body (early 4xx,
    // auth reject). The keepalive guard must consult the real downstream
    // state and keep the connection reusable. Content-Length is set so
    // the response is not close-delimited, which would disable reuse on
    // its own and mask what this test asserts.
    let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response.insert_header(header::CONTENT_LENGTH, "0").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&response)
        .await
        .unwrap();
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Infinite);
}

#[tokio::test]
async fn early_response_mid_capture_still_disables_keepalive() {
    init_log();
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let output = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).write(&output[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Infinite);
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    // Body not read: the downstream really is unfinished, so the guard
    // must still close regardless of the registered buffer. Content-Length
    // is set so the only keepalive-off source is the guard itself.
    let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response.insert_header(header::CONTENT_LENGTH, "0").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&response)
        .await
        .unwrap();
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Off);
}

#[tokio::test]
async fn headers_end_stream_fact_ignores_early_body_buffer() {
    init_log();
    // A bodyless request: no Content-Length, no Transfer-Encoding, so the
    // request framing ended at the header section.
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.request_headers_end_stream());

    // Registering an early body buffer makes `is_body_empty()` report
    // non-empty, because the buffer replays a rewritten, non-empty body
    // upstream. That is an application artifact: the transport fact of
    // what the client sent must not move with it.
    let mut buffer = InMemoryRequestBodyBuffer::new();
    buffer
        .write(&Bytes::from_static(b"injected"))
        .await
        .unwrap();
    buffer.finish().await.unwrap();
    http_stream
        .set_bodyless_request_replay_buffer(Box::new(buffer))
        .unwrap();
    assert!(!http_stream.is_body_empty());
    assert!(http_stream.request_headers_end_stream());
}

#[tokio::test]
async fn early_body_buffer_not_registered() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let _ = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert!(!http_stream.begin_request_body_replay().await.unwrap());
}

/// Delegates to [`InMemoryRequestBodyBuffer`] and reports its own drop, so
/// tests can pin down exactly when the session releases the buffer.
struct DropProbeBuffer {
    inner: InMemoryRequestBodyBuffer,
    dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl DropProbeBuffer {
    fn new() -> (Self, std::sync::Arc<std::sync::atomic::AtomicBool>) {
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        (
            DropProbeBuffer {
                inner: InMemoryRequestBodyBuffer::new(),
                dropped: dropped.clone(),
            },
            dropped,
        )
    }
}

impl Drop for DropProbeBuffer {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl RequestBodyBuffer for DropProbeBuffer {
    async fn write(&mut self, data: &Bytes) -> Result<()> {
        self.inner.write(data).await
    }

    async fn finish(&mut self) -> Result<()> {
        self.inner.finish().await
    }

    async fn rewind(&mut self) -> Result<()> {
        self.inner.rewind().await
    }

    async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
        self.inner.next_chunk(max_bytes).await
    }

    fn consume(&mut self, bytes: usize) {
        self.inner.consume(bytes)
    }
}

fn dropped(flag: &std::sync::Arc<std::sync::atomic::AtomicBool>) -> bool {
    flag.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test]
async fn ready_buffer_released_when_response_commits() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .write(b"HTTP/1.1 200 OK\r\n\r\n")
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false;
    http_stream.read_request().await.unwrap();
    let (probe, probe_dropped) = DropProbeBuffer::new();
    http_stream
        .set_request_body_buffer(Box::new(probe))
        .unwrap();
    let _ = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert!(!dropped(&probe_dropped));

    let response = Box::new(ResponseHeader::build(200, None).unwrap());
    http_stream.write_response_header(response).await.unwrap();
    assert!(dropped(&probe_dropped));
    assert!(!http_stream.request_body_buffer_registered());
    assert!(http_stream.begin_request_body_replay().await.is_err());
}

#[tokio::test]
async fn ready_buffer_released_when_proxy_task_response_commits() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .write(b"HTTP/1.1 200 OK\r\n\r\n")
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false;
    http_stream.read_request().await.unwrap();
    let (probe, probe_dropped) = DropProbeBuffer::new();
    http_stream
        .set_request_body_buffer(Box::new(probe))
        .unwrap();
    let _ = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert!(!dropped(&probe_dropped));

    let response = Box::new(ResponseHeader::build(200, None).unwrap());
    http_stream.send_proxy_task(HttpTask::Header(response, true));
    assert!(http_stream.write_proxy_tasks().await.unwrap());
    assert!(dropped(&probe_dropped));
    assert!(!http_stream.request_body_buffer_registered());
    assert!(http_stream.begin_request_body_replay().await.is_err());
}

#[tokio::test]
async fn buffer_released_when_response_commits_after_replay() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .write(b"HTTP/1.1 100 Continue\r\n\r\n")
        .write(b"HTTP/1.1 200 OK\r\n\r\n")
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false;
    http_stream.read_request().await.unwrap();
    let (probe, probe_dropped) = DropProbeBuffer::new();
    http_stream
        .set_request_body_buffer(Box::new(probe))
        .unwrap();
    let _ = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    assert!(http_stream
        .read_body_or_idle(false)
        .await
        .unwrap()
        .is_none());
    // Replay is done but no response committed yet: a retry could still
    // rewind and replay, so the buffer must survive.
    assert!(!dropped(&probe_dropped));
    // An informational response is not a commitment either.
    let informational = Box::new(ResponseHeader::build(100, None).unwrap());
    http_stream
        .write_response_header(informational)
        .await
        .unwrap();
    assert!(!dropped(&probe_dropped));
    // Committing the real response releases the buffer immediately.
    let response = Box::new(ResponseHeader::build(200, None).unwrap());
    http_stream.write_response_header(response).await.unwrap();
    assert!(dropped(&probe_dropped));
    assert!(!http_stream.request_body_buffer_registered());
    // A replay attempt after release must fail closed, not silently proxy
    // a bodyless request.
    assert!(http_stream.begin_request_body_replay().await.is_err());
}

#[tokio::test]
async fn buffer_released_at_replay_eof_when_response_committed_first() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .write(b"HTTP/1.1 200 OK\r\n\r\n")
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false;
    http_stream.read_request().await.unwrap();
    let (probe, probe_dropped) = DropProbeBuffer::new();
    http_stream
        .set_request_body_buffer(Box::new(probe))
        .unwrap();
    let _ = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    // Early upstream response: the header commits downstream while replay
    // is still in flight — the buffer must survive until replay EOF.
    let response = Box::new(ResponseHeader::build(200, None).unwrap());
    http_stream.write_response_header(response).await.unwrap();
    assert!(!dropped(&probe_dropped));
    assert!(http_stream
        .read_body_or_idle(false)
        .await
        .unwrap()
        .is_none());
    assert!(dropped(&probe_dropped));
    assert!(http_stream.begin_request_body_replay().await.is_err());
}

#[tokio::test]
async fn set_buffer_rejected_for_empty_body() {
    init_log();
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .is_err());
}

#[tokio::test]
async fn set_buffer_rejected_for_upgrade_request() {
    init_log();
    // Upgrade request whose "body" is really the start of a tunnel: the buffer
    // capture-until-EOF model does not apply, registration must fail closed.
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\nUpgrade: websocket\r\nConnection: upgrade\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .is_err());
}

#[tokio::test]
async fn set_buffer_rejected_when_retry_buffering_enabled() {
    init_log();
    // Capture registration: retry buffering already on would tee the
    // drained body into the native buffer and send it twice.
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream.enable_retry_buffering();
    assert!(http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .is_err());

    // Bodyless replay registration: rejected for the same mutual-exclusion
    // contract (a bodyless request needs its own session so the retry
    // check, not the body-empty check, is what fires).
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream.enable_retry_buffering();
    let mut ready = InMemoryRequestBodyBuffer::new();
    ready.finish().await.unwrap();
    assert!(http_stream
        .set_bodyless_request_replay_buffer(Box::new(ready))
        .is_err());
}

#[tokio::test]
async fn set_buffer_rejected_after_body_read() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    // Read the body BEFORE registering: registration must fail closed so a
    // truncated body can never be captured and replayed.
    let _ = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert!(http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .is_err());
}

#[tokio::test]
async fn replay_rejected_before_capture_finishes() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    assert!(http_stream.begin_request_body_replay().await.is_err());
}

#[tokio::test]
async fn drain_discards_buffer_no_tee() {
    // Draining a registered-but-unread body (e.g. keepalive reuse after the app
    // rejected the request) must drop the buffer, not tee the discarded body
    // into it — the reject path is exactly where hostile bodies show up. The
    // discard is remembered: a later replay attempt must fail closed rather
    // than report "no buffer registered" and let the proxy forward a request
    // whose body is gone.
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    assert!(http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .is_err());
    http_stream.drain_request_body().await.unwrap();
    assert!(http_stream.begin_request_body_replay().await.is_err());
}

#[tokio::test]
async fn drain_after_partial_read_poisons_replay() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nab";
    let input3 = b"c";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    assert_eq!(
        http_stream.read_body_bytes().await.unwrap().unwrap(),
        b"ab".as_slice()
    );
    http_stream.drain_request_body().await.unwrap();
    // The partially captured buffer was discarded with the rest of the body:
    // replay must fail closed, not silently proxy a bodyless request.
    assert!(http_stream.begin_request_body_replay().await.is_err());
}

#[tokio::test]
async fn no_op_drain_preserves_completed_capture() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    assert_eq!(
        http_stream.read_body_bytes().await.unwrap().unwrap(),
        b"abc".as_slice()
    );
    http_stream.drain_request_body().await.unwrap();
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        b"abc".as_slice()
    );
}

/// Ignores captured data and replays a fixed non-empty body, like an app
/// rewriting a zero-byte payload.
#[derive(Default)]
struct RewriteToNonEmptyBuffer {
    offset: usize,
}

const REWRITTEN_BODY: &[u8] = b"rewritten";

#[async_trait]
impl RequestBodyBuffer for RewriteToNonEmptyBuffer {
    async fn write(&mut self, _data: &Bytes) -> Result<()> {
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn rewind(&mut self) -> Result<()> {
        self.offset = 0;
        Ok(())
    }

    async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
        if self.offset >= REWRITTEN_BODY.len() {
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(max_bytes)
            .min(REWRITTEN_BODY.len());
        Ok(Some(Bytes::from_static(&REWRITTEN_BODY[self.offset..end])))
    }

    fn consume(&mut self, bytes: usize) {
        self.offset = self.offset.saturating_add(bytes);
    }
}

#[tokio::test]
async fn zero_payload_chunked_body_stays_non_empty_while_registered() {
    // A chunked request terminated by an immediate zero chunk passes
    // registration (its framing permits a body) but captures zero bytes,
    // and the registered buffer may rewrite that into a non-empty body.
    // is_body_empty() feeds the upstream END_STREAM-on-HEADERS decision in
    // proxy_h2, so it must keep reporting non-empty while the buffer is
    // registered — ending the stream at HEADERS would turn the replay DATA
    // into a send-after-end-of-stream error.
    init_log();
    let input1 = b"POST / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(RewriteToNonEmptyBuffer::default()))
        .unwrap();
    assert!(!http_stream.is_body_empty());
    // The zero chunk ends the body immediately: zero bytes captured.
    assert!(http_stream.read_body_bytes().await.unwrap().is_none());
    // Without the replay-source guard the framing now reads Complete(0)
    // and this would flip to true.
    assert!(!http_stream.is_body_empty());
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert!(!http_stream.is_body_empty());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        REWRITTEN_BODY
    );
    assert!(http_stream
        .read_body_or_idle(false)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn finalized_replay_buffer_injects_body_into_empty_request() {
    init_log();
    let input = b"POST / HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_body_empty());

    http_stream
        .set_bodyless_request_replay_buffer(Box::new(RewriteToNonEmptyBuffer::default()))
        .unwrap();
    assert!(!http_stream.is_body_empty());
    assert!(http_stream.begin_request_body_replay().await.unwrap());
    assert_eq!(
        http_stream.read_body_or_idle(false).await.unwrap().unwrap(),
        REWRITTEN_BODY
    );
    assert!(http_stream
        .read_body_or_idle(false)
        .await
        .unwrap()
        .is_none());
}

/// A capture impl that never completes `write()`, opening a cancellation
/// window after the chunk has already been consumed from the transport.
struct PendingCaptureBuffer;

#[async_trait]
impl RequestBodyBuffer for PendingCaptureBuffer {
    async fn write(&mut self, _data: &Bytes) -> Result<()> {
        std::future::pending::<()>().await;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn rewind(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next_chunk(&mut self, _max_bytes: usize) -> Result<Option<Bytes>> {
        Ok(None)
    }

    fn consume(&mut self, _bytes: usize) {}
}

#[tokio::test]
async fn cancelled_capture_poisons_read_and_replay() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
        .set_request_body_buffer(Box::new(PendingCaptureBuffer))
        .unwrap();
    // The first poll consumes "abc" from the transport, then suspends in
    // the buffer's write(). Dropping the future here models a losing
    // select!/timeout branch cancelling the read mid-capture.
    {
        let mut fut = tokio_test::task::spawn(http_stream.read_body_bytes());
        assert!(fut.poll().is_pending());
    }
    // The chunk is gone from both the app's view and the buffer: the
    // session must fail closed on any further read or replay instead of
    // replaying a truncated body.
    assert!(http_stream.read_body_bytes().await.is_err());
    assert!(http_stream.begin_request_body_replay().await.is_err());
}
