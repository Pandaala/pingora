// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use pingora_error::{Error, ErrorType, Result};

const REQUEST_BODY_REPLAY_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestBodyBufferState {
    Capturing,
    Ready,
    Replaying,
    ReplayDone,
}

pub(crate) struct RegisteredRequestBodyBuffer {
    buffer: Box<dyn RequestBodyBuffer>,
    state: RequestBodyBufferState,
    // Bytes peeked and handed downstream by the previous next_chunk() call but
    // not yet committed to the impl via consume(). Committed synchronously at
    // the start of the following next_chunk() call, so a call whose future is
    // dropped mid-poll (select! cancellation) leaves the cursor untouched and
    // the same chunk is re-served.
    uncommitted: usize,
}

impl RegisteredRequestBodyBuffer {
    pub(crate) fn new(buffer: Box<dyn RequestBodyBuffer>) -> Self {
        Self {
            buffer,
            state: RequestBodyBufferState::Capturing,
            uncommitted: 0,
        }
    }

    pub(crate) fn ready(buffer: Box<dyn RequestBodyBuffer>) -> Self {
        Self {
            buffer,
            state: RequestBodyBufferState::Ready,
            uncommitted: 0,
        }
    }

    pub(crate) fn is_replaying(&self) -> bool {
        self.state == RequestBodyBufferState::Replaying
    }

    pub(crate) fn is_replay_done(&self) -> bool {
        self.state == RequestBodyBufferState::ReplayDone
    }

    pub(crate) async fn capture(&mut self, data: &Bytes) -> Result<()> {
        if self.state != RequestBodyBufferState::Capturing {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer received capture data outside capture state",
            );
        }
        self.buffer.write(data).await
    }

    pub(crate) async fn finish_capture(&mut self) -> Result<()> {
        if self.state == RequestBodyBufferState::Capturing {
            self.buffer.finish().await?;
            self.state = RequestBodyBufferState::Ready;
        }
        Ok(())
    }

    pub(crate) async fn begin_replay(&mut self) -> Result<()> {
        if self.state == RequestBodyBufferState::Capturing {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer present but downstream body not fully drained",
            );
        }
        // A retry rewinds to the start; the last chunk delivered before the
        // rewind must not be consumed afterwards or the retry would skip it.
        self.uncommitted = 0;
        self.buffer.rewind().await?;
        self.state = RequestBodyBufferState::Replaying;
        Ok(())
    }

    /// Return the next replay chunk, or `None` at replay EOF.
    ///
    /// Delivery contract: the chunk returned by one call is committed (its
    /// cursor advanced) at the *start* of the following call, on the assumption
    /// that it was fully handed off. A caller that does not forward a returned
    /// chunk must rewind via [`Self::begin_replay`] before reading again; calling
    /// `next_chunk` again instead silently skips the unforwarded chunk.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.state != RequestBodyBufferState::Replaying {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer read outside replay state",
            );
        }
        // Commit the previously delivered chunk synchronously, before the first
        // await point: if this call is cancelled from here on, the cursor is
        // already consistent and the next call re-peeks the current chunk.
        if self.uncommitted > 0 {
            let bytes = std::mem::take(&mut self.uncommitted);
            self.buffer.consume(bytes);
        }
        let chunk = self
            .buffer
            .next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
            .await?;
        if chunk.as_ref().is_some_and(Bytes::is_empty) {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer returned an empty replay chunk",
            );
        }
        if chunk
            .as_ref()
            .is_some_and(|chunk| chunk.len() > REQUEST_BODY_REPLAY_CHUNK_SIZE)
        {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer returned an oversized replay chunk",
            );
        }
        match chunk.as_ref() {
            // Record in the same poll that returns the chunk (no await between
            // here and the return), so delivery and the pending commit are
            // atomic with respect to cancellation.
            Some(chunk) => self.uncommitted = chunk.len(),
            None => self.state = RequestBodyBufferState::ReplayDone,
        }
        Ok(chunk)
    }
}

/// A buffer with size limit. When the total amount of data written to the buffer is below the limit
/// all the data will be held in the buffer. Otherwise, the buffer will report to be truncated.
pub struct FixedBuffer {
    buffer: BytesMut,
    capacity: usize,
    truncated: bool,
}

impl FixedBuffer {
    pub fn new(capacity: usize) -> Self {
        FixedBuffer {
            buffer: BytesMut::new(),
            capacity,
            truncated: false,
        }
    }

    // TODO: maybe store a Vec of Bytes for zero-copy
    pub fn write_to_buffer(&mut self, data: &Bytes) {
        if !self.truncated && (self.buffer.len() + data.len() <= self.capacity) {
            self.buffer.extend_from_slice(data);
        } else {
            // TODO: clear data because the data held here is useless anyway?
            self.truncated = true;
        }
    }
    pub fn clear(&mut self) {
        self.truncated = false;
        self.buffer.clear();
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.len() == 0
    }
    pub fn is_truncated(&self) -> bool {
        self.truncated
    }
    pub fn get_buffer(&self) -> Option<Bytes> {
        // TODO: return None if truncated?
        if !self.is_empty() {
            Some(self.buffer.clone().freeze())
        } else {
            None
        }
    }
}

/// A pluggable buffer for the full request body, supplied by the proxy app to capture
/// the body early (in `request_filter`) and replay it to upstream during forwarding.
/// Storage policy (memory / file) and whether replay returns the captured original or a
/// rewritten body are entirely the impl's choice.
///
/// Scope: capture happens only through `Session::read_request_body` /
/// `read_body_bytes` (the app draining the body in `request_filter`). Rewrite is therefore
/// only possible for captured bodies; the capture path is supported only for requests that
/// *have* a body. An application that
/// needs to inject a body into a request the client sent empty must finalize the buffer
/// itself and register it through `set_bodyless_request_replay_buffer`; that separate path
/// never reads from the downstream transport. A request whose framing merely *permits*
/// a body but carries zero payload bytes (e.g. chunked terminated by an immediate zero
/// chunk, or an empty END_STREAM DATA frame) may use the normal capture registration.
///
/// Contract & limitations:
/// - **Register before reading.** The buffer must be set before any body byte is read;
///   registering after a partial read is rejected (`set_request_body_buffer` fails
///   closed) so a truncated body can never be replayed.
/// - **Tunnel-carrying requests are rejected.** Registration fails closed for HTTP/1.1
///   upgrade requests and HTTP/2 CONNECT (plain or extended, e.g. WebSocket over H2):
///   their "body" is a bidirectional tunnel stream, not a request body, so capturing
///   until EOF would swallow the tunnel until a read timeout fires.
/// - **Registration is a commitment.** There is no un-register: once set, the app must
///   fully read the body (through the capturing reads above) before proxying (a
///   registered-but-undrained body fails the request rather than streaming natively).
///   Backing out mid-capture has no sound semantics — consumed bytes exist only in the
///   buffer.
/// - **Draining discards the buffer and poisons replay.** `drain_request_body` (meant
///   for discarding a rejected request's body, e.g. before an early response over a
///   keepalive connection) drops the registered buffer without capturing the drained
///   bytes. The body then exists in neither the transport nor the buffer, so a later
///   replay attempt fails the request deterministically before anything is sent
///   upstream.
/// - The low-level `poll_read_body_bytes` (HTTP/2) fails while a buffer is registered;
///   drain via `read_request_body` / `read_body_bytes`.
/// - Request trailers (HTTP/1.1 chunked / HTTP/2) are not captured or replayed, matching
///   the native retry buffer.
/// - **The buffer is dropped as soon as it can no longer be needed.** Once replay has
///   reached EOF and a non-informational response header has been committed downstream
///   (in either order), no further upstream retry can legitimately happen, so the session
///   drops the buffer right then instead of holding it for the rest of the response
///   (which may be long-lived, e.g. SSE). Implementations holding large resources (temp
///   file, fd) should release them in `Drop`. After this release a pathological replay
///   attempt fails the request deterministically. If the response never commits (the
///   request errors out first), the buffer is dropped with the session.
/// - **`Expect: 100-continue` requests need an explicit 100 before capture.** Capture
///   moves the body read ahead of upstream forwarding, so the path that normally
///   unblocks such a client — the upstream's 100 relayed downstream — has not happened
///   yet, and the capturing reads do not send one. The client waits for a 100 while the
///   proxy waits for body bytes, until a read timeout fires. Applications must call
///   `Session::write_continue_response()` before draining when the request carries
///   `Expect: 100-continue`. That call is idempotent (a repeat is a no-op) and a final
///   response can still be written after it. To reject such a request instead, skip the
///   100 and write the final response directly — this is why capture does not send the
///   100 automatically. On an HTTP/2 downstream the call is currently a silent no-op —
///   the h2 crate cannot send informational responses — but the deadlock is H1-specific
///   (H2 clients generally do not hard-block on a 100). Note the `Expect` header is
///   still forwarded upstream by default; an upstream 100 relayed downstream is an
///   extra 1xx the client must tolerate (RFC 9110), or the application can strip the
///   header in `upstream_request_filter`.
/// - **Downstream close/reset is not observed while replaying.** While the buffer is in
///   the replay state, `read_body_or_idle` serves chunks from the buffer and does not
///   watch the downstream socket (HTTP/1) or stream (HTTP/2). A client that disconnects
///   after uploading is noticed only once replay completes (idle watching resumes) or
///   the response write back downstream fails — the connection is always released, but
///   the whole buffered body may be pushed upstream for a client that is already gone.
///   The wasted-work window equals the replay duration; configure the upstream
///   `write_timeout` whenever body buffering is enabled so that upstream backpressure
///   cannot stretch the window indefinitely (the timeout bounds each body write, not
///   the replay as a whole).
#[async_trait]
pub trait RequestBodyBuffer: Send + Sync {
    /// Append one captured body chunk. Called once per chunk during capture.
    ///
    /// Cancellation: the session awaits this future inside the app's body-read
    /// call, which apps may wrap in `select!` / timeouts, so the future may be
    /// dropped before completion. The chunk was already consumed from the
    /// transport by then, so the session poisons itself when that happens: no
    /// further capture, read, or replay is attempted and the request fails
    /// closed. Impls therefore never observe another call after a cancelled
    /// `write`; they only need to remain safe to drop.
    ///
    /// Errors: returning `Err` poisons the session the same way — the chunk was
    /// already consumed from the transport, so no further capture, read, drain,
    /// or replay is attempted and the request fails closed. On HTTP/1, whenever
    /// body bytes remain unread, the end-of-request keepalive drain also fails,
    /// so the downstream connection is closed and reuse is forfeited (on HTTP/2
    /// the poison is per-stream and the request's stream fails). Impls that
    /// want a graceful over-limit response (e.g. 413) must not enforce a size
    /// cap by returning `Err` here; instead the application should count bytes
    /// and stop reading at the cap outside `write()`, keeping the session
    /// healthy.
    async fn write(&mut self, data: &Bytes) -> Result<()>;

    /// Finalize capture after downstream EOF. Implementations that spill to disk
    /// should flush pending writes here.
    ///
    /// Cancellation: like [`Self::write`], this future may be dropped before
    /// completion (the session then poisons itself and fails the request
    /// closed). As a defensive contract, `finish` may be invoked again after a
    /// cancelled `finish` and impls must make it idempotent — a repeated call
    /// must not corrupt the captured body. [`InMemoryRequestBodyBuffer`] is
    /// idempotent because it freezes the accumulation buffer only on the first
    /// call (guarded by `body.is_none()`).
    ///
    /// Errors: like [`Self::write`], returning `Err` poisons the session and
    /// fails the request closed.
    async fn finish(&mut self) -> Result<()>;

    /// Reset replay to the beginning. Called before every upstream attempt, so a
    /// retry reads the exact same body again.
    ///
    /// Cancellation: this is invoked outside any `select!` (before the body pump
    /// starts), so unlike [`Self::next_chunk`] its future is not dropped mid-poll
    /// in the current proxy code. Implementations should still keep it a simple
    /// cursor reset.
    async fn rewind(&mut self) -> Result<()>;

    /// Return the replay chunk at the current cursor, or `None` at replay EOF,
    /// WITHOUT consuming it. The cursor is advanced only by [`Self::consume`].
    ///
    /// This is a pure peek: repeated calls without an intervening `consume` must
    /// return the same bytes, and the call must have no side effect on replay
    /// state. This split is what makes replay cancellation-safe — the proxy polls
    /// this future inside a `select!`, so it may be dropped at any internal await
    /// point (e.g. mid disk read). Because consumption is committed separately
    /// and synchronously by the caller, a cancelled peek simply re-reads the same
    /// chunk on the next call; no data can be lost. Implementations must not keep
    /// partial read state in `self` across await points.
    ///
    /// Chunks are fed through the normal request body filters and upstream body
    /// writer. Implementations must return a non-empty chunk of at most
    /// `max_bytes`; violating either bound fails the request.
    ///
    /// Returning a body of a different length than the client's original framing
    /// is allowed (rewrite), but the app must fix `Content-Length` /
    /// `Transfer-Encoding` in `upstream_request_filter` before proxying. Replay
    /// begins only after that filter runs, so the rewritten body's final length
    /// must already be known there — the rewrite decision cannot be deferred to
    /// `next_chunk`.
    ///
    /// Because rewrite is allowed, the proxy cannot cross-check the replayed
    /// total length against the captured total length. Implementations that
    /// replay the captured body verbatim (no rewrite) should enforce that
    /// invariant themselves: record the total captured length at
    /// [`Self::finish`] and return an error — not `None` — when replay reaches
    /// EOF short of it. Otherwise truncation in the backing storage (a short
    /// write during capture, a read boundary bug, a spill file truncated
    /// externally) under-delivers against the request's `Content-Length` and
    /// surfaces only as an upstream hang or reset far from its cause, instead
    /// of an attributable gateway-local error.
    ///
    /// Replay before finalization is a contract violation: if this is called
    /// before [`Self::finish`] has run (e.g. an unfinalized buffer handed to
    /// `set_bodyless_request_replay_buffer`), implementations must return an
    /// error — not `None` — so the intended body is not silently replayed as
    /// empty. [`InMemoryRequestBodyBuffer`] is the reference behavior.
    ///
    /// Errors: a returned error fails the request and the proxy classifies it
    /// as an internal (gateway-local) error — not a client or upstream failure.
    /// Implementations should still attach a distinctive `ErrorType` / context
    /// (e.g. for a disk read failure in a spill-to-disk impl) so the application
    /// can attribute storage failures in its own logging regardless of the
    /// error-source tag.
    async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>>;

    /// Advance the replay cursor past `bytes` bytes previously returned by
    /// [`Self::next_chunk`]. Deliberately synchronous and infallible: the caller
    /// invokes it in the same poll that hands the peeked chunk downstream, so
    /// there is no await point at which the commit can be cancelled.
    fn consume(&mut self, bytes: usize);
}

/// In-memory reference implementation of [`RequestBodyBuffer`]. Production apps
/// should enforce a capture size limit or spill to disk.
#[derive(Debug)]
pub struct InMemoryRequestBodyBuffer {
    buf: BytesMut,
    body: Option<Bytes>,
    replay_offset: usize,
}

impl InMemoryRequestBodyBuffer {
    pub fn new() -> Self {
        InMemoryRequestBodyBuffer {
            buf: BytesMut::new(),
            body: None,
            replay_offset: 0,
        }
    }
}

impl Default for InMemoryRequestBodyBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RequestBodyBuffer for InMemoryRequestBodyBuffer {
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
        self.replay_offset = 0;
        Ok(())
    }

    async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
        let Some(body) = self.body.as_ref() else {
            // Fail closed instead of reporting EOF: an unfinalized buffer
            // handed to `set_bodyless_request_replay_buffer` would otherwise
            // silently replay as an empty body, losing the injected payload.
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer replayed before finish() finalized it",
            );
        };
        if self.replay_offset >= body.len() {
            return Ok(None);
        }
        let end = self.replay_offset.saturating_add(max_bytes).min(body.len());
        Ok(Some(body.slice(self.replay_offset..end)))
    }

    fn consume(&mut self, bytes: usize) {
        self.replay_offset = self.replay_offset.saturating_add(bytes);
    }
}

#[cfg(test)]
mod early_buffer_tests {
    use super::*;

    struct OversizedReplayBuffer;

    #[async_trait]
    impl RequestBodyBuffer for OversizedReplayBuffer {
        async fn write(&mut self, _data: &Bytes) -> Result<()> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        async fn rewind(&mut self) -> Result<()> {
            Ok(())
        }

        async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
            Ok(Some(Bytes::from(vec![0; max_bytes + 1])))
        }

        fn consume(&mut self, _bytes: usize) {}
    }

    #[tokio::test]
    async fn in_memory_captures_and_is_rewindable() {
        let mut b = InMemoryRequestBodyBuffer::new();
        b.write(&Bytes::from_static(b"hello ")).await.unwrap();
        b.write(&Bytes::from_static(b"world")).await.unwrap();
        b.finish().await.unwrap();
        let chunk = b
            .next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(chunk, Bytes::from_static(b"hello world"));
        // next_chunk is a pure peek: repeated calls re-serve the same chunk
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            Some(chunk.clone())
        );
        b.consume(chunk.len());
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            None
        );
        b.rewind().await.unwrap();
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            Some(Bytes::from_static(b"hello world"))
        );
    }

    #[tokio::test]
    async fn in_memory_unfinalized_replay_fails_closed() {
        // Data written but finish() never called: replay must error, not EOF.
        let mut b = InMemoryRequestBodyBuffer::new();
        b.write(&Bytes::from_static(b"payload")).await.unwrap();
        assert!(b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.is_err());
        // A brand-new buffer without finish() must also fail closed.
        let mut fresh = InMemoryRequestBodyBuffer::new();
        assert!(fresh
            .next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn in_memory_finalized_empty_body_replays_eof() {
        let mut b = InMemoryRequestBodyBuffer::new();
        b.finish().await.unwrap();
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn in_memory_replays_in_bounded_chunks() {
        let mut b = InMemoryRequestBodyBuffer::new();
        let body = Bytes::from(vec![b'x'; REQUEST_BODY_REPLAY_CHUNK_SIZE + 1]);
        b.write(&body).await.unwrap();
        b.finish().await.unwrap();
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
                .await
                .unwrap()
                .unwrap()
                .len(),
            REQUEST_BODY_REPLAY_CHUNK_SIZE
        );
        b.consume(REQUEST_BODY_REPLAY_CHUNK_SIZE);
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
                .await
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        b.consume(1);
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn registered_buffer_rejects_oversized_replay_chunk() {
        let mut b = RegisteredRequestBodyBuffer::new(Box::new(OversizedReplayBuffer));
        b.finish_capture().await.unwrap();
        b.begin_replay().await.unwrap();
        assert!(b.next_chunk().await.is_err());
        // A replay error leaves the state as Replaying: the proxy pump relies
        // on this to attribute the surfaced error to the buffer (internal)
        // rather than the client connection (downstream).
        assert!(b.is_replaying());
    }

    /// A peek impl that awaits (yields) before reading at the cursor, like a
    /// disk-spill impl awaiting file I/O. Used to open a cancellation window
    /// inside next_chunk.
    struct YieldingReplayBuffer {
        body: Bytes,
        offset: usize,
    }

    #[async_trait]
    impl RequestBodyBuffer for YieldingReplayBuffer {
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
            tokio::task::yield_now().await;
            if self.offset >= self.body.len() {
                return Ok(None);
            }
            let end = self.offset.saturating_add(max_bytes).min(self.body.len());
            Ok(Some(self.body.slice(self.offset..end)))
        }

        fn consume(&mut self, bytes: usize) {
            self.offset = self.offset.saturating_add(bytes);
        }
    }

    fn replaying_yielding_buffer() -> RegisteredRequestBodyBuffer {
        RegisteredRequestBodyBuffer::new(Box::new(YieldingReplayBuffer {
            body: Bytes::from_static(b"chunk-1"),
            offset: 0,
        }))
    }

    #[tokio::test]
    async fn cancelled_next_chunk_re_serves_the_same_chunk() {
        let mut b = replaying_yielding_buffer();
        b.finish_capture().await.unwrap();
        b.begin_replay().await.unwrap();
        // Poll once (suspends at the impl's internal await), then drop the
        // future — exactly what a losing tokio::select! branch does.
        {
            let mut fut = tokio_test::task::spawn(b.next_chunk());
            assert!(fut.poll().is_pending());
        }
        // The cancelled call must not have consumed anything.
        assert_eq!(
            b.next_chunk().await.unwrap(),
            Some(Bytes::from_static(b"chunk-1"))
        );
        assert_eq!(b.next_chunk().await.unwrap(), None);
    }

    #[tokio::test]
    async fn cancellation_after_delivery_neither_duplicates_nor_skips() {
        let body = Bytes::from(vec![b'x'; REQUEST_BODY_REPLAY_CHUNK_SIZE + 3]);
        let mut b = RegisteredRequestBodyBuffer::new(Box::new(YieldingReplayBuffer {
            body: body.clone(),
            offset: 0,
        }));
        b.finish_capture().await.unwrap();
        b.begin_replay().await.unwrap();
        let c1 = b.next_chunk().await.unwrap().unwrap();
        assert_eq!(c1.len(), REQUEST_BODY_REPLAY_CHUNK_SIZE);
        // This call commits c1 at entry (synchronously) and is then cancelled
        // while peeking c2 — the commit must stick, the peek must not.
        {
            let mut fut = tokio_test::task::spawn(b.next_chunk());
            assert!(fut.poll().is_pending());
        }
        let c2 = b.next_chunk().await.unwrap().unwrap();
        assert_eq!(c2.len(), 3);
        assert_eq!([c1, c2].concat(), body);
        assert_eq!(b.next_chunk().await.unwrap(), None);
    }

    #[tokio::test]
    async fn rewind_discards_pending_commit() {
        let mut b = replaying_yielding_buffer();
        b.finish_capture().await.unwrap();
        b.begin_replay().await.unwrap();
        // Deliver the chunk; its consumption is now pending.
        assert_eq!(
            b.next_chunk().await.unwrap(),
            Some(Bytes::from_static(b"chunk-1"))
        );
        // A retry rewinds; the pending commit must be discarded or the retry
        // would skip the first chunk.
        b.begin_replay().await.unwrap();
        assert_eq!(
            b.next_chunk().await.unwrap(),
            Some(Bytes::from_static(b"chunk-1"))
        );
        assert_eq!(b.next_chunk().await.unwrap(), None);
    }
}
