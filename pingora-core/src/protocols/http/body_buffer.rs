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
use pingora_error::Result;

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
/// Storage policy (memory / file) and whether `get()` returns the captured original or a
/// rewritten body are entirely the impl's choice.
///
/// Scope: capture happens only through `Session::read_request_body` /
/// `read_body_bytes` (the app draining the body in `request_filter`). Rewrite is therefore
/// supported only for requests that *have* a body — a body cannot be injected into a
/// request the client sent empty (for an empty body the proxy never consults the buffer;
/// on HTTP/2 the header END_STREAM has already been sent by then).
#[async_trait]
pub trait RequestBodyBuffer: Send + Sync {
    /// Append one captured body chunk. Called once per chunk during capture.
    async fn write(&mut self, data: &Bytes) -> Result<()>;

    /// Return the body to forward upstream (captured original OR a rewritten body).
    ///
    /// Must be **re-readable**: the proxy calls this once per upstream attempt, so on a
    /// retry it is called again and MUST return the *same* body. An impl that returns a
    /// different body (or `None`) on retry turns a retryable upstream failure into a
    /// non-retryable error.
    ///
    /// `None` is an explicit "cannot produce a body" signal (e.g. the impl went over its
    /// own budget): the proxy then fails the request rather than forwarding a bad body.
    /// Returning `Some(bytes)` of a different length than the client's original framing is
    /// allowed (rewrite), but the impl must then fix `Content-Length` / `Transfer-Encoding`
    /// in `upstream_request_filter`.
    async fn get(&mut self) -> Result<Option<Bytes>>;
}

/// Result of consulting a session's early body buffer at replay time.
#[derive(Debug)]
pub enum EarlyBodyReplay {
    /// No buffer registered — take the native forwarding path.
    NotRegistered,
    /// Forward these bytes to upstream.
    Body(Bytes),
    /// A buffer was registered but produced no body — the proxy must fail the request.
    Unavailable,
}

/// In-memory reference implementation of [`RequestBodyBuffer`]. Production apps
/// (e.g. with file spill) provide their own impl.
#[derive(Debug)]
pub struct InMemoryRequestBodyBuffer {
    buf: BytesMut,
    cached: Option<Bytes>,
}

impl InMemoryRequestBodyBuffer {
    pub fn new() -> Self {
        InMemoryRequestBodyBuffer {
            buf: BytesMut::new(),
            cached: None,
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
        // Once the body has been materialized for replay, ignore further writes
        // (e.g. post-response drain).
        if self.cached.is_none() {
            self.buf.extend_from_slice(data);
        }
        Ok(())
    }

    async fn get(&mut self) -> Result<Option<Bytes>> {
        if self.cached.is_none() {
            self.cached = Some(self.buf.split().freeze());
        }
        Ok(self.cached.clone())
    }
}

#[cfg(test)]
mod early_buffer_tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_captures_and_is_rereadable() {
        let mut b = InMemoryRequestBodyBuffer::new();
        b.write(&Bytes::from_static(b"hello ")).await.unwrap();
        b.write(&Bytes::from_static(b"world")).await.unwrap();
        // get() returns the concatenation, and is callable more than once.
        assert_eq!(b.get().await.unwrap(), Some(Bytes::from_static(b"hello world")));
        assert_eq!(b.get().await.unwrap(), Some(Bytes::from_static(b"hello world")));
    }

    #[tokio::test]
    async fn in_memory_ignores_writes_after_first_get() {
        let mut b = InMemoryRequestBodyBuffer::new();
        b.write(&Bytes::from_static(b"abc")).await.unwrap();
        let _ = b.get().await.unwrap();
        // post-get write (e.g. post-response drain) must not change the captured body.
        b.write(&Bytes::from_static(b"def")).await.unwrap();
        assert_eq!(b.get().await.unwrap(), Some(Bytes::from_static(b"abc")));
    }
}
