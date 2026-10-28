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
//! The byte and item budgets count only additional chunks accepted by
//! [`ResponseBodySink::push`]. They do not account for replacing the current
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

use crate::ResponseHeadReplacement;
use bytes::Bytes;
use pingora_error::{BError, Error, ErrorType::InternalError, Result};

/// Maximum bytes a filter may emit through the sink within one pump batch.
/// In-place growth of the current chunk is outside this limit.
pub const RESPONSE_BODY_EMIT_BUDGET: usize = 1024 * 1024;

/// Maximum nonempty chunks a filter may emit through the sink within one pump
/// batch. This independently bounds per-chunk task, framing, and cache-write
/// overhead that the byte budget cannot represent.
pub const RESPONSE_BODY_EMIT_CHUNK_BUDGET: usize = RESPONSE_BODY_EMIT_BUDGET / 512;

/// Extra chunks and the terminate signal produced by a response-body filter.
pub struct ResponseBodySink {
    extra: Vec<Bytes>,
    remaining: usize,
    remaining_chunks: usize,
    terminate: bool,
    head_control: ResponseHeadControl,
    head_work: Option<ResponseHeadWorkBudget>,
}

#[derive(Clone, Copy, Debug)]
struct ResponseHeadWorkBudget {
    remaining: u64,
    used: u64,
    exhausted: bool,
}

#[derive(Debug)]
enum ResponseHeadControl {
    Disarmed,
    Armed,
    Release,
    Replace(ResponseHeadReplacement),
    Fail(BError),
}

pub(crate) enum ResponseHeadDecision {
    Release,
    Replace(ResponseHeadReplacement),
    Fail(BError),
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
            remaining_chunks: RESPONSE_BODY_EMIT_CHUNK_BUDGET,
            terminate: false,
            head_control: ResponseHeadControl::Disarmed,
            head_work: None,
        }
    }

    /// Request release of a response head currently held by the bounded
    /// response-head barrier.
    ///
    /// Returns `true` only for the first request while a head is armed. The
    /// default Immediate path is disarmed and returns `false`. This signal is
    /// intentionally independent of [`Self::terminate`]: releasing a head
    /// permits normal streaming to continue.
    pub fn release_response_head(&mut self) -> bool {
        if matches!(self.head_control, ResponseHeadControl::Armed) {
            self.head_control = ResponseHeadControl::Release;
            true
        } else {
            false
        }
    }

    /// Whether a bounded response head is currently awaiting or carrying a
    /// callback-local decision.
    pub fn response_head_is_held(&self) -> bool {
        !matches!(self.head_control, ResponseHeadControl::Disarmed)
    }

    /// Whether the active Hold has a staged callback decision waiting for the
    /// response pipeline to consume it.
    ///
    /// Product processor drivers can use this after their full callback chain
    /// returns to avoid translating a precommit Replace/Fail into the legacy
    /// post-commit stream-termination path.
    pub fn response_head_decision_pending(&self) -> bool {
        matches!(
            self.head_control,
            ResponseHeadControl::Release
                | ResponseHeadControl::Replace(_)
                | ResponseHeadControl::Fail(_)
        )
    }

    /// Reserve units from the response-head Hold work budget before performing
    /// bounded synchronous or external work. Reservations survive pump batch
    /// resets and are aggregated into the final content-free usage report.
    pub fn reserve_response_head_work(&mut self, units: u64) -> Result<()> {
        let Some(work) = self.head_work.as_mut() else {
            return Error::e_explain(InternalError, "response head work requested outside Hold");
        };
        if units > work.remaining {
            work.exhausted = true;
            return Error::e_explain(
                InternalError,
                format!(
                    "response head work budget exhausted: {units} units requested, {} remaining",
                    work.remaining
                ),
            );
        }
        work.remaining -= units;
        work.used = work.used.checked_add(units).ok_or_else(|| {
            Error::explain(InternalError, "response head work accounting overflow")
        })?;
        Ok(())
    }

    /// Replace the held origin prefix with one complete bounded response.
    /// A pending Release may be upgraded within the same callback.
    pub fn replace_response_head(&mut self, replacement: ResponseHeadReplacement) -> Result<()> {
        if matches!(
            self.head_control,
            ResponseHeadControl::Armed | ResponseHeadControl::Release
        ) {
            self.head_control = ResponseHeadControl::Replace(replacement);
            Ok(())
        } else {
            Error::e_explain(
                InternalError,
                "response head Replace requested without an undecided Hold",
            )
        }
    }

    /// Fail the held response before its original head reaches the writer.
    /// A pending Release may be upgraded within the same callback.
    pub fn fail_response_head(&mut self, error: BError) -> Result<()> {
        if matches!(
            self.head_control,
            ResponseHeadControl::Armed | ResponseHeadControl::Release
        ) {
            self.head_control = ResponseHeadControl::Fail(error);
            Ok(())
        } else {
            Error::e_explain(
                InternalError,
                "response head Fail requested without an undecided Hold",
            )
        }
    }

    #[cfg(test)]
    pub(crate) fn arm_response_head_release(&mut self) -> bool {
        self.arm_response_head_release_with_work_limit(u64::MAX)
    }

    pub(crate) fn arm_response_head_release_with_work_limit(
        &mut self,
        max_work_units: u64,
    ) -> bool {
        if !matches!(self.head_control, ResponseHeadControl::Disarmed) {
            return false;
        }
        self.head_control = ResponseHeadControl::Armed;
        self.head_work = Some(ResponseHeadWorkBudget {
            remaining: max_work_units,
            used: 0,
            exhausted: false,
        });
        true
    }

    pub(crate) fn response_head_work_units(&self) -> Option<u64> {
        self.head_work.map(|work| work.used)
    }

    pub(crate) fn response_head_work_limit_exceeded(&self) -> bool {
        self.head_work.is_some_and(|work| work.exhausted)
    }

    pub(crate) fn take_response_head_decision(&mut self) -> Option<ResponseHeadDecision> {
        match std::mem::replace(&mut self.head_control, ResponseHeadControl::Disarmed) {
            ResponseHeadControl::Disarmed => None,
            ResponseHeadControl::Armed => {
                self.head_control = ResponseHeadControl::Armed;
                None
            }
            ResponseHeadControl::Release => Some(ResponseHeadDecision::Release),
            ResponseHeadControl::Replace(replacement) => {
                Some(ResponseHeadDecision::Replace(replacement))
            }
            ResponseHeadControl::Fail(error) => Some(ResponseHeadDecision::Fail(error)),
        }
    }

    pub(crate) fn disarm_response_head_release(&mut self) {
        self.head_control = ResponseHeadControl::Disarmed;
        self.head_work = None;
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
        if self.remaining_chunks == 0 {
            return Error::e_explain(
                InternalError,
                format!(
                    "response body emit chunk budget exhausted: maximum {} nonempty chunks per batch",
                    RESPONSE_BODY_EMIT_CHUNK_BUDGET
                ),
            );
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
        self.remaining_chunks -= 1;
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

    pub fn remaining_chunk_budget(&self) -> usize {
        self.remaining_chunks
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
    pub(crate) fn reset_batch(&mut self) {
        self.extra.clear();
        self.remaining = RESPONSE_BODY_EMIT_BUDGET;
        self.remaining_chunks = RESPONSE_BODY_EMIT_CHUNK_BUDGET;
    }
}

#[cfg(test)]
#[path = "response_body_sink_tests.rs"]
mod tests;
