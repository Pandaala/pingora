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

//! Bounded emit sink for the upstream response-body filter.
//!
//! A response-body filter mutates its chunk in place for the common 1-in-1-out
//! case. When it needs to emit *additional* chunks, or to end the response
//! early, it goes through this sink.
//!
//! The byte budget is per pump batch, not per chunk: the pump drains up to
//! `TASK_BUFFER_SIZE` upstream tasks and writes them downstream as a unit, so
//! the batch is what bounds resident memory. The pump calls
//! [`ResponseBodySink::reset_batch`] once per batch.

use bytes::Bytes;
use pingora_error::{Error, ErrorType::InternalError, Result};

/// Maximum bytes a filter may emit through the sink within one pump batch.
/// Matches the hard cap on plugin-initiated outbound response bodies.
pub const RESPONSE_BODY_EMIT_BUDGET: usize = 1024 * 1024;

/// Extra chunks and the terminate signal produced by a response-body filter.
pub struct ResponseBodySink {
    extra: Vec<Bytes>,
    remaining: usize,
    terminate: bool,
}

impl Default for ResponseBodySink {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseBodySink {
    pub fn new() -> Self {
        Self {
            extra: Vec::new(),
            remaining: RESPONSE_BODY_EMIT_BUDGET,
            terminate: false,
        }
    }

    /// Queue an additional chunk to be written downstream after the current
    /// one. Empty chunks are dropped: writing an empty body chunk downstream
    /// would end the response.
    ///
    /// Returns an error when the batch budget is exhausted. It never truncates
    /// silently and never records a partially accepted chunk.
    pub fn push(&mut self, chunk: Bytes) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        if chunk.len() > self.remaining {
            return Error::e_explain(
                InternalError,
                format!(
                    "response body emit budget exhausted: {} bytes requested, {} remaining",
                    chunk.len(),
                    self.remaining
                ),
            );
        }
        self.remaining -= chunk.len();
        self.extra.push(chunk);
        Ok(())
    }

    /// End the response after the currently queued chunks are written. Sticky:
    /// once set it survives `reset_batch`.
    pub fn terminate(&mut self) {
        self.terminate = true;
    }

    pub fn is_terminated(&self) -> bool {
        self.terminate
    }

    pub fn remaining_budget(&self) -> usize {
        self.remaining
    }

    /// Drain the queued chunks. The pump calls this to append them to the
    /// downstream task batch.
    pub fn take_extra(&mut self) -> Vec<Bytes> {
        std::mem::take(&mut self.extra)
    }

    /// Borrow the queued chunks without draining them. Used to feed the cache
    /// before the chunks are handed to the downstream writer.
    pub fn peek_extra(&self) -> &[Bytes] {
        &self.extra
    }

    /// Restore the budget for a new pump batch. Any chunk not drained by then
    /// is dropped, because the batch it belonged to has already been written.
    pub fn reset_batch(&mut self) {
        self.extra.clear();
        self.remaining = RESPONSE_BODY_EMIT_BUDGET;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_accumulates_and_take_drains() {
        let mut sink = ResponseBodySink::new();
        sink.push(Bytes::from_static(b"a")).unwrap();
        sink.push(Bytes::from_static(b"bc")).unwrap();
        let extra = sink.take_extra();
        assert_eq!(
            extra,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")]
        );
        assert!(sink.take_extra().is_empty(), "take must drain");
    }

    #[test]
    fn budget_is_consumed_and_exhaustion_errors() {
        let mut sink = ResponseBodySink::new();
        assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET);
        sink.push(Bytes::from_static(b"1234")).unwrap();
        assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET - 4);

        let oversized = Bytes::from(vec![0u8; RESPONSE_BODY_EMIT_BUDGET]);
        assert!(sink.push(oversized).is_err(), "must reject, never truncate");
        // The rejected chunk must not be partially recorded.
        assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET - 4);
        assert_eq!(sink.take_extra().len(), 1);
    }

    #[test]
    fn reset_batch_restores_budget_but_not_terminate() {
        let mut sink = ResponseBodySink::new();
        sink.push(Bytes::from_static(b"xyz")).unwrap();
        sink.terminate();
        sink.reset_batch();
        assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET);
        assert!(
            sink.take_extra().is_empty(),
            "reset_batch drops undelivered extras"
        );
        assert!(sink.is_terminated(), "terminate is sticky across batches");
    }

    #[test]
    fn empty_push_is_free() {
        let mut sink = ResponseBodySink::new();
        sink.push(Bytes::new()).unwrap();
        assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET);
        assert!(
            sink.take_extra().is_empty(),
            "empty chunks are dropped, not queued"
        );
    }
}
