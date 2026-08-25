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
//! The byte budget counts only additional chunks accepted by
//! [`ResponseBodySink::push`]. It does not account for replacing the current
//! chunk with a larger one; filters that expand a chunk in place must enforce
//! their own output bound.
//!
//! The sink budget is per pump batch, not per chunk. Each pump starts with one
//! task and synchronously drains tasks already queued in its bounded channel;
//! the sibling producer cannot be polled again during that drain. With the
//! current topology a batch therefore contains at most the initial task plus
//! the channel capacity. The pump calls [`ResponseBodySink::reset_batch`] once
//! per batch. Revisit this bound if a producer is ever scheduled independently
//! or the channel gains another sender.

use bytes::Bytes;
use pingora_error::{Error, ErrorType::InternalError, Result};

/// Maximum bytes a filter may emit through the sink within one pump batch.
/// In-place growth of the current chunk is outside this limit.
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
    ///
    /// Length contract: a filter that grows the body (here, or by replacing the
    /// in-place chunk with a larger one) MUST declare `changes_body_length()`.
    /// Extra bytes pushed past a committed `content-length` are dropped silently
    /// by h1's `write_body` (which stops at the declared length) and are a
    /// protocol violation on h2 downstream; unlike the terminate direction
    /// (see `warn_response_body_terminate_content_length_leak`), this overflow
    /// direction is not currently diagnosed at runtime.
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

    /// Materialize the current chunk of a synthetic terminal body event before
    /// extras already emitted by the filter chain.
    ///
    /// This is the Header-EOS representation of mutating
    /// `Body(None, true)` into `Some(bytes)`. It is deliberately not charged
    /// against the sink budget: ordinary current-chunk mutation is not sink
    /// output either. Only `push()` output consumes the extra-chunk budget.
    pub(crate) fn prepend_current(&mut self, chunk: Bytes) {
        if chunk.is_empty() {
            return;
        }
        self.extra.insert(0, chunk);
    }

    /// End the response after the currently queued chunks are written. Sticky:
    /// once set it survives `reset_batch`.
    pub fn terminate(&mut self) {
        self.terminate = true;
    }

    pub fn is_terminated(&self) -> bool {
        self.terminate
    }

    /// Consume a terminate signal at a response boundary that is already
    /// naturally terminal.
    pub(crate) fn consume_terminate(&mut self) {
        self.terminate = false;
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

    #[test]
    fn prepend_current_keeps_current_chunk_before_extras_without_spending_budget() {
        let mut sink = ResponseBodySink::new();
        sink.push(Bytes::from_static(b"extra-a")).unwrap();
        sink.push(Bytes::from_static(b"extra-b")).unwrap();
        let remaining = sink.remaining_budget();
        sink.prepend_current(Bytes::from_static(b"current"));
        assert_eq!(sink.remaining_budget(), remaining);
        assert_eq!(
            sink.take_extra(),
            vec![
                Bytes::from_static(b"current"),
                Bytes::from_static(b"extra-a"),
                Bytes::from_static(b"extra-b"),
            ]
        );
    }

    #[test]
    fn synthetic_current_chunk_has_the_same_unbudgeted_semantics_as_body_mutation() {
        let mut sink = ResponseBodySink::new();
        sink.prepend_current(Bytes::from(vec![0; RESPONSE_BODY_EMIT_BUDGET + 1]));
        assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET);
        assert_eq!(sink.take_extra()[0].len(), RESPONSE_BODY_EMIT_BUDGET + 1);

        assert!(sink
            .push(Bytes::from(vec![0; RESPONSE_BODY_EMIT_BUDGET + 1]))
            .is_err());
    }

    #[test]
    fn terminal_boundary_consumes_terminate() {
        let mut sink = ResponseBodySink::new();
        sink.terminate();
        sink.consume_terminate();
        assert!(!sink.is_terminated());
    }
}
