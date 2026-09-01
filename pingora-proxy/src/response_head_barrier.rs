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

//! Bounded retention for a response head and its semantic body prefix.
//!
//! Applications select the allocation-free [`ResponseHeadCommitPlan::Immediate`]
//! fast path or explicitly opt into a [`ResponseHeadCommitPlan::Hold`] with
//! independent hard limits. The H1/H2 response pumps enforce those limits and
//! the single absolute deadline before writer handoff.

use bytes::Bytes;
use pingora_core::protocols::http::HttpTask;
use pingora_error::{Error, ErrorType::InternalError, Result};
use pingora_http::ResponseHeader;
use std::time::Duration;
use tokio::time::Instant;

const HEAD_METADATA_BASE_COST: usize = 64;
const TRAILER_METADATA_BASE_COST: usize = 32;
const HEADER_FIELD_METADATA_OVERHEAD: usize = 32;

/// The representation source whose final response head is being considered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseHeadSource {
    /// A response produced by the selected upstream exchange.
    Origin,
    /// A response read from the HTTP entity cache.
    Cache,
}

/// Request-local policy for committing the final downstream response head.
///
#[derive(Debug)]
#[non_exhaustive]
pub enum ResponseHeadCommitPlan {
    /// Preserve the existing behavior: retain no response tasks in this layer.
    Immediate,
    /// Retain a bounded response prefix until its processor requests release.
    Hold(ResponseHeadHoldPlan),
}

impl ResponseHeadCommitPlan {
    /// Construct an explicitly bounded Hold plan.
    ///
    /// Hold is supported only for ordinary final origin responses on the H1/H2
    /// pumps. Unsupported cache, custom, upgrade, and tunnel combinations are
    /// reported through the typed boundary hook rather than degrading to
    /// Immediate.
    pub fn hold(limits: ResponseHeadHoldLimits) -> Self {
        Self::Hold(ResponseHeadHoldPlan::new(limits))
    }
}

/// Independent limits for response-prefix retention.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseHeadHoldLimits {
    max_input_bytes: usize,
    max_output_bytes: usize,
    max_nonempty_chunks: usize,
    max_events: usize,
    max_metadata_bytes: usize,
    max_work_units: u64,
    timeout: Duration,
}

impl ResponseHeadHoldLimits {
    /// Define independent hard limits for one response-head Hold.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        max_input_bytes: usize,
        max_output_bytes: usize,
        max_nonempty_chunks: usize,
        max_events: usize,
        max_metadata_bytes: usize,
        max_work_units: u64,
        timeout: Duration,
    ) -> Self {
        Self {
            max_input_bytes,
            max_output_bytes,
            max_nonempty_chunks,
            max_events,
            max_metadata_bytes,
            max_work_units,
            timeout,
        }
    }

    pub const fn max_input_bytes(self) -> usize {
        self.max_input_bytes
    }

    pub const fn max_output_bytes(self) -> usize {
        self.max_output_bytes
    }

    pub const fn max_nonempty_chunks(self) -> usize {
        self.max_nonempty_chunks
    }

    pub const fn max_events(self) -> usize {
        self.max_events
    }

    pub const fn max_metadata_bytes(self) -> usize {
        self.max_metadata_bytes
    }

    pub const fn max_work_units(self) -> u64 {
        self.max_work_units
    }

    pub const fn timeout(self) -> Duration {
        self.timeout
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        max_output_bytes: usize,
        max_nonempty_chunks: usize,
        max_events: usize,
        max_metadata_bytes: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            max_input_bytes: max_output_bytes,
            max_output_bytes,
            max_nonempty_chunks,
            max_events,
            max_metadata_bytes,
            max_work_units: max_events as u64,
            timeout,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_full_for_test(
        max_input_bytes: usize,
        max_output_bytes: usize,
        max_nonempty_chunks: usize,
        max_events: usize,
        max_metadata_bytes: usize,
        max_work_units: u64,
        timeout: Duration,
    ) -> Self {
        Self {
            max_input_bytes,
            max_output_bytes,
            max_nonempty_chunks,
            max_events,
            max_metadata_bytes,
            max_work_units,
            timeout,
        }
    }
}

/// A bounded Hold plan carried by [`ResponseHeadCommitPlan::Hold`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseHeadHoldPlan {
    limits: ResponseHeadHoldLimits,
}

impl ResponseHeadHoldPlan {
    pub const fn new(limits: ResponseHeadHoldLimits) -> Self {
        Self { limits }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(limits: ResponseHeadHoldLimits) -> Self {
        Self::new(limits)
    }
}

/// A complete bounded local response selected before the original head is
/// committed.
///
/// The response pipeline is responsible for validating this value against the
/// active Hold limits and for applying normal downstream framing and module
/// preparation. Constructing a replacement does not itself commit it.
#[derive(Debug)]
#[non_exhaustive]
pub struct ResponseHeadReplacement {
    header: Box<ResponseHeader>,
    body: Vec<Bytes>,
}

impl ResponseHeadReplacement {
    /// Build a replacement from its semantic response head and ordered body
    /// chunks.
    pub fn new(header: Box<ResponseHeader>, body: Vec<Bytes>) -> Self {
        Self { header, body }
    }

    /// Borrow the chosen replacement response head.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// Borrow the ordered replacement body chunks.
    pub fn body(&self) -> &[Bytes] {
        &self.body
    }

    /// Consume the replacement into the representation owned by the response
    /// pipeline.
    pub fn into_parts(self) -> (Box<ResponseHeader>, Vec<Bytes>) {
        (self.header, self.body)
    }
}

/// Aggregate, content-free accounting reported when a response-head plan
/// reaches an observable outcome.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ResponseHeadUsage {
    input_bytes: usize,
    output_bytes: usize,
    nonempty_chunks: usize,
    events: usize,
    metadata_bytes: usize,
    work_units: u64,
    held_for: Duration,
}

impl ResponseHeadUsage {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        input_bytes: usize,
        output_bytes: usize,
        nonempty_chunks: usize,
        events: usize,
        metadata_bytes: usize,
        work_units: u64,
        held_for: Duration,
    ) -> Self {
        Self {
            input_bytes,
            output_bytes,
            nonempty_chunks,
            events,
            metadata_bytes,
            work_units,
            held_for,
        }
    }

    pub fn input_bytes(&self) -> usize {
        self.input_bytes
    }

    pub fn output_bytes(&self) -> usize {
        self.output_bytes
    }

    pub fn nonempty_chunks(&self) -> usize {
        self.nonempty_chunks
    }

    pub fn events(&self) -> usize {
        self.events
    }

    pub fn metadata_bytes(&self) -> usize {
        self.metadata_bytes
    }

    pub fn work_units(&self) -> u64 {
        self.work_units
    }

    pub fn held_for(&self) -> Duration {
        self.held_for
    }
}

/// A terminal or resource boundary that requires the application to choose a
/// fail-closed Hold resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseHeadBoundary {
    Unsupported,
    InputLimit,
    OutputLimit,
    ChunkLimit,
    EventLimit,
    MetadataLimit,
    WorkLimit,
    Timeout,
    CleanTerminalWithoutDecision,
    SourceFailed,
    ApplicationFail,
    ApplicationTerminate,
}

impl ResponseHeadBoundary {
    /// A stable diagnostic label suitable for error context and structured
    /// logging.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::InputLimit => "input-limit",
            Self::OutputLimit => "output-limit",
            Self::ChunkLimit => "chunk-limit",
            Self::EventLimit => "event-limit",
            Self::MetadataLimit => "metadata-limit",
            Self::WorkLimit => "work-limit",
            Self::Timeout => "timeout",
            Self::CleanTerminalWithoutDecision => "clean-terminal-without-decision",
            Self::SourceFailed => "source-failed",
            Self::ApplicationFail => "application-fail",
            Self::ApplicationTerminate => "application-terminate",
        }
    }
}

/// The application's fail-closed resolution for a Hold boundary.
///
/// This type deliberately does not implement `Clone`: [`pingora_error::BError`]
/// owns error context and causes that cannot be cloned soundly.
#[derive(Debug)]
#[non_exhaustive]
pub enum ResponseHeadBoundaryAction {
    /// Discard the original representation and commit a bounded local response.
    Replace(ResponseHeadReplacement),
    /// Discard the held representation and propagate this exact error.
    Fail(pingora_error::BError),
}

/// Final observable disposition of a response-head commit plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseHeadOutcome {
    Immediate,
    Released,
    Replaced,
    Failed(ResponseHeadBoundary),
    Cancelled,
}

/// What the caller should do with the task range after barrier processing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseHeadBarrierOutput {
    /// Run downstream header preparation on this range before writer handoff.
    PrepareFrom(usize),
    /// A bounded replacement was installed at this range.
    PrepareReplacementFrom(usize),
    /// The range was retained by the barrier and no task is ready to write.
    Held,
}

/// A Hold failure that still retains the semantic reason required by the
/// application boundary mapper. Source errors are kept separate because they
/// must bypass that mapper and preserve their original error object.
#[derive(Debug)]
pub(crate) enum ResponseHeadBarrierFailure {
    Boundary(ResponseHeadBoundary),
    Source(pingora_error::BError),
}

pub(crate) type ResponseHeadBarrierResult<T> = std::result::Result<T, ResponseHeadBarrierFailure>;

/// Response-scoped head retention state.
pub(crate) struct ResponseHeadBarrier {
    state: ResponseHeadBarrierState,
}

enum ResponseHeadBarrierState {
    AwaitingFinalHead,
    Immediate,
    Holding(Box<HeldResponseHead>),
    Released(Option<ResponseHeadUsage>),
    Replaced(Option<ResponseHeadUsage>),
    Aborted,
}

struct HeldResponseHead {
    limits: ResponseHeadHoldLimits,
    activated_at: Instant,
    deadline: Instant,
    usage: ResponseHeadRetentionUsage,
    tasks: Vec<HttpTask>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ResponseHeadRetentionUsage {
    input_bytes: usize,
    output_bytes: usize,
    nonempty_chunks: usize,
    events: usize,
    metadata_bytes: usize,
    work_units: u64,
}

impl Default for ResponseHeadBarrier {
    fn default() -> Self {
        Self {
            state: ResponseHeadBarrierState::AwaitingFinalHead,
        }
    }
}

impl ResponseHeadBarrier {
    /// Freeze the plan selected for the final response head exactly once.
    pub(crate) fn select(&mut self, plan: ResponseHeadCommitPlan) -> Result<()> {
        if !matches!(self.state, ResponseHeadBarrierState::AwaitingFinalHead) {
            return Err(Error::explain(
                InternalError,
                "response head commit plan selected more than once",
            ));
        }

        self.state = match plan {
            ResponseHeadCommitPlan::Immediate => ResponseHeadBarrierState::Immediate,
            ResponseHeadCommitPlan::Hold(plan) => {
                let activated_at = Instant::now();
                let deadline = activated_at
                    .checked_add(plan.limits.timeout)
                    .ok_or_else(|| {
                        Error::explain(
                            InternalError,
                            "response head barrier deadline exceeds the monotonic clock range",
                        )
                    })?;
                ResponseHeadBarrierState::Holding(Box::new(HeldResponseHead {
                    limits: plan.limits,
                    activated_at,
                    deadline,
                    usage: ResponseHeadRetentionUsage::default(),
                    tasks: Vec::new(),
                }))
            }
        };
        Ok(())
    }

    pub(crate) fn is_holding(&self) -> bool {
        matches!(self.state, ResponseHeadBarrierState::Holding(_))
    }

    pub(crate) fn is_awaiting_final_head(&self) -> bool {
        matches!(self.state, ResponseHeadBarrierState::AwaitingFinalHead)
    }

    /// The absolute deadline while Hold is active. No timer is constructed for
    /// Awaiting/Immediate/Released states.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        match &self.state {
            ResponseHeadBarrierState::Holding(held) => Some(held.deadline),
            _ => None,
        }
    }

    pub(crate) fn work_limit(&self) -> Option<u64> {
        match &self.state {
            ResponseHeadBarrierState::Holding(held) => Some(held.limits.max_work_units()),
            _ => None,
        }
    }

    /// Charge source bytes before writable mutation. Header/trailer metadata is
    /// deliberately output accounting; the input budget is body bytes only.
    pub(crate) fn observe_input(&mut self, task: &HttpTask) -> ResponseHeadBarrierResult<()> {
        let ResponseHeadBarrierState::Holding(held) = &mut self.state else {
            return Ok(());
        };
        let bytes = match task {
            HttpTask::Body(Some(body), _) | HttpTask::UpgradedBody(Some(body), _) => body.len(),
            _ => 0,
        };
        held.usage.input_bytes = held.usage.input_bytes.checked_add(bytes).ok_or(
            ResponseHeadBarrierFailure::Boundary(ResponseHeadBoundary::InputLimit),
        )?;
        if held.usage.input_bytes > held.limits.max_input_bytes {
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::InputLimit,
            ));
        }
        Ok(())
    }

    pub(crate) fn set_work_usage(&mut self, work_units: u64) -> ResponseHeadBarrierResult<()> {
        let ResponseHeadBarrierState::Holding(held) = &mut self.state else {
            return Ok(());
        };
        held.usage.work_units = work_units;
        if work_units > held.limits.max_work_units {
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::WorkLimit,
            ));
        }
        Ok(())
    }

    /// Abort a currently held prefix after the protocol pump's idle timer
    /// fires. Dropping the Holding state releases every retained task.
    pub(crate) fn claim_boundary(&mut self) -> Option<ResponseHeadUsage> {
        let state = std::mem::replace(&mut self.state, ResponseHeadBarrierState::Aborted);
        let ResponseHeadBarrierState::Holding(held) = state else {
            self.state = state;
            return None;
        };
        Some(held.public_usage())
    }

    #[cfg(test)]
    pub(crate) fn timeout(&mut self) -> pingora_error::BError {
        let was_holding = self.claim_boundary().is_some();
        Error::explain(
            InternalError,
            if was_holding {
                "response head barrier absolute deadline exceeded"
            } else {
                "response head barrier timeout fired outside Hold"
            },
        )
    }

    /// Retain the newly produced range, or flush the complete ordered prefix
    /// when release has been requested.
    ///
    /// `clean_terminal` describes source completion independently of the final
    /// transformed task shape. A clean terminal without release is rejected in
    /// this initial slice because no terminal fallback policy is public yet.
    pub(crate) fn capture_or_release(
        &mut self,
        tasks: &mut Vec<HttpTask>,
        start: usize,
        release_requested: bool,
        clean_terminal: bool,
    ) -> ResponseHeadBarrierResult<ResponseHeadBarrierOutput> {
        if start > tasks.len() {
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::ApplicationFail,
            ));
        }

        let state = std::mem::replace(&mut self.state, ResponseHeadBarrierState::Aborted);
        let ResponseHeadBarrierState::Holding(mut held) = state else {
            self.state = state;
            return match self.state {
                ResponseHeadBarrierState::AwaitingFinalHead
                | ResponseHeadBarrierState::Immediate
                | ResponseHeadBarrierState::Released(_)
                | ResponseHeadBarrierState::Replaced(_) => {
                    Ok(ResponseHeadBarrierOutput::PrepareFrom(start))
                }
                ResponseHeadBarrierState::Aborted => Err(ResponseHeadBarrierFailure::Boundary(
                    ResponseHeadBoundary::ApplicationFail,
                )),
                ResponseHeadBarrierState::Holding(_) => unreachable!(),
            };
        };

        if tasks[start..]
            .iter()
            .any(|task| matches!(task, HttpTask::Failed(_)))
        {
            let current = tasks.split_off(start);
            for task in current {
                if let HttpTask::Failed(error) = task {
                    self.state = ResponseHeadBarrierState::Holding(held);
                    return Err(ResponseHeadBarrierFailure::Source(error));
                }
            }
            unreachable!("the failed task observed above must still be present")
        }

        if Instant::now() >= held.deadline {
            tasks.truncate(start);
            self.state = ResponseHeadBarrierState::Holding(held);
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::Timeout,
            ));
        }

        let candidate = match ResponseHeadRetentionUsage::for_tasks(&tasks[start..]) {
            Ok(candidate) => candidate,
            Err(failure) => {
                self.state = ResponseHeadBarrierState::Holding(held);
                return Err(failure);
            }
        };
        let combined = match held.usage.checked_add(candidate) {
            Ok(combined) => combined,
            Err(failure) => {
                self.state = ResponseHeadBarrierState::Holding(held);
                return Err(failure);
            }
        };
        if let Err(failure) = combined.ensure_fits(held.limits) {
            self.state = ResponseHeadBarrierState::Holding(held);
            return Err(failure);
        }

        if release_requested {
            let current = tasks.split_off(start);
            held.tasks.extend(current);
            let usage = held.public_usage_with(combined);
            tasks.extend(held.tasks);
            self.state = ResponseHeadBarrierState::Released(Some(usage));
            return Ok(ResponseHeadBarrierOutput::PrepareFrom(start));
        }

        if clean_terminal {
            tasks.truncate(start);
            self.state = ResponseHeadBarrierState::Holding(held);
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::CleanTerminalWithoutDecision,
            ));
        }

        held.usage = combined;
        held.tasks.extend(tasks.drain(start..));
        self.state = ResponseHeadBarrierState::Holding(held);
        Ok(ResponseHeadBarrierOutput::Held)
    }

    /// Discard a held origin prefix and install one complete bounded
    /// replacement. The replacement is fully charged before the origin tasks
    /// are removed.
    pub(crate) fn replace(
        &mut self,
        tasks: &mut Vec<HttpTask>,
        start: usize,
        replacement: ResponseHeadReplacement,
    ) -> ResponseHeadBarrierResult<ResponseHeadBarrierOutput> {
        self.replace_inner(tasks, start, replacement, true)
    }

    pub(crate) fn replace_after_boundary(
        &mut self,
        tasks: &mut Vec<HttpTask>,
        start: usize,
        replacement: ResponseHeadReplacement,
    ) -> ResponseHeadBarrierResult<ResponseHeadBarrierOutput> {
        self.replace_inner(tasks, start, replacement, false)
    }

    fn replace_inner(
        &mut self,
        tasks: &mut Vec<HttpTask>,
        start: usize,
        replacement: ResponseHeadReplacement,
        enforce_deadline: bool,
    ) -> ResponseHeadBarrierResult<ResponseHeadBarrierOutput> {
        if start > tasks.len() {
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::ApplicationFail,
            ));
        }
        let state = std::mem::replace(&mut self.state, ResponseHeadBarrierState::Aborted);
        let ResponseHeadBarrierState::Holding(held) = state else {
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::ApplicationFail,
            ));
        };
        if enforce_deadline && Instant::now() >= held.deadline {
            tasks.truncate(start);
            self.state = ResponseHeadBarrierState::Holding(held);
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::Timeout,
            ));
        }

        let (header, body) = replacement.into_parts();
        if header.status.is_informational() {
            self.state = ResponseHeadBarrierState::Holding(held);
            return Err(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::Unsupported,
            ));
        }
        let mut replacement_tasks = Vec::with_capacity(body.len().saturating_add(1));
        let body_is_empty = body.is_empty();
        replacement_tasks.push(HttpTask::Header(header, body_is_empty));
        let last = body.len().saturating_sub(1);
        for (index, chunk) in body.into_iter().enumerate() {
            replacement_tasks.push(HttpTask::Body(Some(chunk), index == last));
        }
        let replacement_usage = match ResponseHeadRetentionUsage::for_tasks(&replacement_tasks) {
            Ok(usage) => usage,
            Err(failure) => {
                self.state = ResponseHeadBarrierState::Holding(held);
                return Err(failure);
            }
        };
        if let Err(failure) = replacement_usage.ensure_fits(held.limits) {
            self.state = ResponseHeadBarrierState::Holding(held);
            return Err(failure);
        }

        tasks.truncate(start);
        tasks.extend(replacement_tasks);
        let mut held = held;
        held.usage.output_bytes = replacement_usage.output_bytes;
        held.usage.nonempty_chunks = replacement_usage.nonempty_chunks;
        held.usage.events = replacement_usage.events;
        held.usage.metadata_bytes = replacement_usage.metadata_bytes;
        let usage = held.public_usage();
        self.state = ResponseHeadBarrierState::Replaced(Some(usage));
        Ok(ResponseHeadBarrierOutput::PrepareReplacementFrom(start))
    }

    pub(crate) fn boundary_usage(&self) -> Option<ResponseHeadUsage> {
        match &self.state {
            ResponseHeadBarrierState::Holding(held) => Some(held.public_usage()),
            _ => None,
        }
    }

    pub(crate) fn take_resolved_usage(&mut self) -> Option<ResponseHeadUsage> {
        match &mut self.state {
            ResponseHeadBarrierState::Released(usage)
            | ResponseHeadBarrierState::Replaced(usage) => usage.take(),
            _ => None,
        }
    }

    /// Abort and drop any retained prefix before propagating an application
    /// failure.
    pub(crate) fn abort(&mut self) {
        self.state = ResponseHeadBarrierState::Aborted;
    }

    #[cfg(test)]
    fn retained_usage(&self) -> Option<ResponseHeadRetentionUsage> {
        match &self.state {
            ResponseHeadBarrierState::Holding(held) => Some(held.usage),
            _ => None,
        }
    }
}

impl ResponseHeadRetentionUsage {
    fn for_tasks(tasks: &[HttpTask]) -> ResponseHeadBarrierResult<Self> {
        let mut usage = Self::default();
        for task in tasks {
            usage.events =
                usage
                    .events
                    .checked_add(1)
                    .ok_or(ResponseHeadBarrierFailure::Boundary(
                        ResponseHeadBoundary::EventLimit,
                    ))?;

            match task {
                HttpTask::Header(header, _) => {
                    usage.metadata_bytes = checked_metadata_add(
                        usage.metadata_bytes,
                        HEAD_METADATA_BASE_COST,
                        &header.headers,
                    )?;
                }
                HttpTask::Body(Some(body), _) | HttpTask::UpgradedBody(Some(body), _) => {
                    usage.output_bytes = usage.output_bytes.checked_add(body.len()).ok_or(
                        ResponseHeadBarrierFailure::Boundary(ResponseHeadBoundary::OutputLimit),
                    )?;
                    if !body.is_empty() {
                        usage.nonempty_chunks = usage.nonempty_chunks.checked_add(1).ok_or(
                            ResponseHeadBarrierFailure::Boundary(ResponseHeadBoundary::ChunkLimit),
                        )?;
                    }
                }
                HttpTask::Body(None, _) | HttpTask::UpgradedBody(None, _) | HttpTask::Done => {}
                HttpTask::Trailer(Some(trailers)) => {
                    usage.metadata_bytes = checked_metadata_add(
                        usage.metadata_bytes,
                        TRAILER_METADATA_BASE_COST,
                        trailers,
                    )?;
                }
                HttpTask::Trailer(None) => {
                    usage.metadata_bytes = usage
                        .metadata_bytes
                        .checked_add(TRAILER_METADATA_BASE_COST)
                        .ok_or_else(|| {
                            ResponseHeadBarrierFailure::Boundary(
                                ResponseHeadBoundary::MetadataLimit,
                            )
                        })?;
                }
                HttpTask::Failed(_) => {
                    return Err(ResponseHeadBarrierFailure::Boundary(
                        ResponseHeadBoundary::SourceFailed,
                    ));
                }
            }
        }
        Ok(usage)
    }

    fn checked_add(self, other: Self) -> ResponseHeadBarrierResult<Self> {
        Ok(Self {
            input_bytes: self.input_bytes,
            output_bytes: self.output_bytes.checked_add(other.output_bytes).ok_or(
                ResponseHeadBarrierFailure::Boundary(ResponseHeadBoundary::OutputLimit),
            )?,
            nonempty_chunks: self
                .nonempty_chunks
                .checked_add(other.nonempty_chunks)
                .ok_or(ResponseHeadBarrierFailure::Boundary(
                    ResponseHeadBoundary::ChunkLimit,
                ))?,
            events: self.events.checked_add(other.events).ok_or(
                ResponseHeadBarrierFailure::Boundary(ResponseHeadBoundary::EventLimit),
            )?,
            metadata_bytes: self
                .metadata_bytes
                .checked_add(other.metadata_bytes)
                .ok_or(ResponseHeadBarrierFailure::Boundary(
                    ResponseHeadBoundary::MetadataLimit,
                ))?,
            work_units: self.work_units,
        })
    }

    fn ensure_fits(self, limits: ResponseHeadHoldLimits) -> ResponseHeadBarrierResult<()> {
        let exceeded = if self.output_bytes > limits.max_output_bytes {
            Some(ResponseHeadBoundary::OutputLimit)
        } else if self.nonempty_chunks > limits.max_nonempty_chunks {
            Some(ResponseHeadBoundary::ChunkLimit)
        } else if self.events > limits.max_events {
            Some(ResponseHeadBoundary::EventLimit)
        } else if self.metadata_bytes > limits.max_metadata_bytes {
            Some(ResponseHeadBoundary::MetadataLimit)
        } else {
            None
        };

        if let Some(boundary) = exceeded {
            return Err(ResponseHeadBarrierFailure::Boundary(boundary));
        }
        Ok(())
    }
}

impl HeldResponseHead {
    fn public_usage(&self) -> ResponseHeadUsage {
        self.public_usage_with(self.usage)
    }

    fn public_usage_with(&self, usage: ResponseHeadRetentionUsage) -> ResponseHeadUsage {
        ResponseHeadUsage::new(
            usage.input_bytes,
            usage.output_bytes,
            usage.nonempty_chunks,
            usage.events,
            usage.metadata_bytes,
            usage.work_units,
            self.activated_at.elapsed(),
        )
    }
}

fn checked_metadata_add(
    current: usize,
    base: usize,
    headers: &http::HeaderMap,
) -> ResponseHeadBarrierResult<usize> {
    let mut total = current
        .checked_add(base)
        .ok_or(ResponseHeadBarrierFailure::Boundary(
            ResponseHeadBoundary::MetadataLimit,
        ))?;
    for (name, value) in headers {
        let field = name
            .as_str()
            .len()
            .checked_add(value.as_bytes().len())
            .and_then(|size| size.checked_add(HEADER_FIELD_METADATA_OVERHEAD))
            .ok_or(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::MetadataLimit,
            ))?;
        total = total
            .checked_add(field)
            .ok_or(ResponseHeadBarrierFailure::Boundary(
                ResponseHeadBoundary::MetadataLimit,
            ))?;
    }
    Ok(total)
}

#[cfg(test)]
#[path = "response_head_barrier_tests.rs"]
mod tests;
