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

//! Protocol-neutral response-task transformation.
//!
//! The duplex pumps retain ownership of transport reads, writes, cancellation,
//! and connection reuse. This module owns the shared semantic sequence between
//! receiving an [`HttpTask`] and handing a prepared task batch to a writer.

#[path = "response_terminal.rs"]
mod response_terminal;

pub(crate) use response_terminal::{normalize_trailers, TerminalBodyDispatch};

use crate::proxy_cache::{self, range_filter::RangeBodyFilter, ServeFromCache};
use crate::response_body_sink::ResponseHeadDecision;
use crate::response_cache_relay::{drain_emitted_chunks, drain_emitted_chunks_before};
use crate::response_head_barrier::{
    ResponseHeadBarrier, ResponseHeadBarrierFailure, ResponseHeadBarrierOutput,
};
use crate::{
    abort_cache_after_response_source_failure, custom, downstream_response_body_forbidden,
    is_downstream_followup, reconcile_terminal_cache_header, reconcile_terminal_response_tasks,
    reject_mismatched_h1_upgrade_101, HttpProxy, ProxyHttp, ResponseBodySink, ResponseHeadBoundary,
    ResponseHeadBoundaryAction, ResponseHeadCommitPlan, ResponseHeadOutcome, ResponseHeadSource,
    Session, UpstreamResponseBodyEvent,
};
use http::version::Version;
use log::trace;
use pingora_core::protocols::http::HttpTask;
use pingora_error::{Error, ErrorType::InternalError, Result};
use pingora_http::ResponseHeader;
use std::future::Future;
use tokio::time;

/// Protocol-specific response behavior retained at the shared pipeline seam.
///
/// Most variants are wire/framing policy. The custom conditional-filter gate
/// is an explicitly temporary compatibility difference tracked in
/// `edgion-changes/pending-issues/custom-conditional-filter-gate.md`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseProtocol {
    H1,
    H2,
    Custom,
}

/// Result of transforming one drained origin task batch. Origin abandonment is
/// distinct from application stream termination: a bounded replacement is a
/// complete downstream response while only the selected origin source must be
/// cancelled or made non-reusable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseTaskBatchOutcome {
    Progress { source_done: bool, terminated: bool },
    OriginAbandoned,
}

/// State whose lifetime is exactly one response transformation pipeline.
///
/// Session, application context, cache state, and protocol writer deliberately
/// remain outside this object: they have different ownership and lifetimes.
pub(crate) struct ResponsePipelineState {
    pub(crate) range_body_filter: RangeBodyFilter,
    pub(crate) suppress_downstream_body: bool,
    pub(crate) filtered_terminal_header: Option<Box<ResponseHeader>>,
    pub(crate) upstream_reusable: bool,
    pub(crate) sink: ResponseBodySink,
    pub(crate) terminal_body: TerminalBodyDispatch,
    pub(crate) head_barrier: ResponseHeadBarrier,
    pub(crate) origin_abandoned: bool,
    pending_head_boundary: Option<ResponseHeadBoundary>,
}

impl Default for ResponsePipelineState {
    fn default() -> Self {
        Self {
            range_body_filter: RangeBodyFilter::new(),
            suppress_downstream_body: false,
            filtered_terminal_header: None,
            upstream_reusable: true,
            sink: ResponseBodySink::new(),
            terminal_body: TerminalBodyDispatch::default(),
            head_barrier: ResponseHeadBarrier::default(),
            origin_abandoned: false,
            pending_head_boundary: None,
        }
    }
}

impl ResponsePipelineState {
    pub(crate) fn response_head_deadline(&self) -> Option<time::Instant> {
        self.head_barrier.deadline()
    }

    fn finish_head_deadline_wait<T>(
        &mut self,
        waited: std::result::Result<Result<T>, time::error::Elapsed>,
    ) -> Result<T> {
        match waited {
            Ok(result) => result,
            Err(_) => {
                self.pending_head_boundary = Some(ResponseHeadBoundary::Timeout);
                self.upstream_reusable = false;
                Error::e_explain(
                    InternalError,
                    "response head barrier callback exceeded its absolute deadline",
                )
            }
        }
    }

    pub(crate) async fn wait_with_response_head_deadline<T>(
        &mut self,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let deadline = self.response_head_deadline();
        let waited = await_response_head_deadline(deadline, future).await;
        self.finish_head_deadline_wait(waited)
    }

    fn take_pending_head_boundary(&mut self) -> Option<ResponseHeadBoundary> {
        self.pending_head_boundary.take().or_else(|| {
            self.sink
                .response_head_work_limit_exceeded()
                .then_some(ResponseHeadBoundary::WorkLimit)
        })
    }
}

async fn await_response_head_deadline<T>(
    deadline: Option<time::Instant>,
    future: impl Future<Output = Result<T>>,
) -> std::result::Result<Result<T>, time::error::Elapsed> {
    match deadline {
        Some(deadline) => time::timeout_at(deadline, future).await,
        None => Ok(future.await),
    }
}

impl ResponseProtocol {
    fn validates_upgrade_before_upstream_filter(self) -> bool {
        self == Self::Custom
    }

    fn validates_upgrade_after_upstream_filter(self) -> bool {
        self == Self::H1
    }

    fn preserves_custom_conditional_filter_gate(self) -> bool {
        // Preserve the custom transport's existing behavior during this
        // structural refactor. Aligning this known drift is a behavior change
        // and should be handled with its own regression evidence.
        self == Self::Custom
    }

    fn supports_upgraded_body(self) -> bool {
        self != Self::H2
    }
}

impl<SV, C> HttpProxy<SV, C>
where
    SV: ProxyHttp,
    C: custom::Connector,
{
    async fn downstream_response_filter_tasks_in_order(
        &self,
        session: &mut Session,
        tasks: &mut [HttpTask],
        ctx: &mut SV::CTX,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        for task in tasks {
            if let HttpTask::Trailer(trailers) = task {
                let buffer = match trailers.as_mut() {
                    Some(trailers) => {
                        self.inner
                            .response_trailer_filter(session, trailers, ctx)
                            .await?
                    }
                    None => None,
                };
                *task = if let Some(buffer) = buffer {
                    HttpTask::Body(Some(buffer), true)
                } else {
                    HttpTask::Trailer(normalize_trailers(std::mem::take(trailers)))
                };
            }
            self.downstream_response_body_filter_tasks(session, std::slice::from_mut(task), ctx)
                .await?;
        }
        Ok(())
    }

    pub(crate) fn resolve_response_head_boundary(
        &self,
        session: &Session,
        boundary: ResponseHeadBoundary,
        ctx: &mut SV::CTX,
        state: &mut ResponsePipelineState,
        out_tasks: &mut Vec<HttpTask>,
        start: usize,
    ) -> Result<ResponseHeadBarrierOutput> {
        let usage = state.head_barrier.boundary_usage().unwrap_or_default();
        let action = self
            .inner
            .response_head_hold_boundary(session, boundary, ctx);
        match action {
            ResponseHeadBoundaryAction::Replace(replacement) => {
                state.upstream_reusable = false;
                state.origin_abandoned = true;
                if session.req_header().method == http::Method::CONNECT
                    && replacement.header().status.is_success()
                {
                    state.head_barrier.abort();
                    state.sink.disarm_response_head_release();
                    self.inner.response_head_hold_outcome(
                        session,
                        ResponseHeadOutcome::Failed(ResponseHeadBoundary::Unsupported),
                        usage,
                        ctx,
                    );
                    return Error::e_explain(
                        InternalError,
                        "successful CONNECT is not a supported response-head replacement",
                    );
                }
                let output =
                    match state
                        .head_barrier
                        .replace_after_boundary(out_tasks, start, replacement)
                    {
                        Ok(output) => output,
                        Err(ResponseHeadBarrierFailure::Boundary(replacement_boundary)) => {
                            state.head_barrier.abort();
                            state.sink.disarm_response_head_release();
                            self.inner.response_head_hold_outcome(
                                session,
                                ResponseHeadOutcome::Failed(replacement_boundary),
                                usage,
                                ctx,
                            );
                            return Error::e_explain(
                                InternalError,
                                format!(
                                    "response head boundary replacement reached {}",
                                    replacement_boundary.as_str()
                                ),
                            );
                        }
                        Err(ResponseHeadBarrierFailure::Source(error)) => return Err(error),
                    };
                let usage = state.head_barrier.take_resolved_usage().unwrap_or(usage);
                state.sink.disarm_response_head_release();
                self.inner.response_head_hold_outcome(
                    session,
                    ResponseHeadOutcome::Replaced,
                    usage,
                    ctx,
                );
                Ok(output)
            }
            ResponseHeadBoundaryAction::Fail(mut error) => {
                error.set_retry(false);
                state.upstream_reusable = false;
                state.head_barrier.abort();
                out_tasks.truncate(start);
                state.sink.disarm_response_head_release();
                self.inner.response_head_hold_outcome(
                    session,
                    ResponseHeadOutcome::Failed(boundary),
                    usage,
                    ctx,
                );
                Err(error)
            }
        }
    }

    fn resolve_response_head_fail_only(
        &self,
        session: &Session,
        boundary: ResponseHeadBoundary,
        ctx: &mut SV::CTX,
        state: &mut ResponsePipelineState,
    ) -> pingora_error::BError {
        let usage = state.head_barrier.boundary_usage().unwrap_or_default();
        let action = self
            .inner
            .response_head_hold_boundary(session, boundary, ctx);
        state.upstream_reusable = false;
        state.head_barrier.abort();
        state.sink.disarm_response_head_release();
        self.inner.response_head_hold_outcome(
            session,
            ResponseHeadOutcome::Failed(boundary),
            usage,
            ctx,
        );
        match action {
            ResponseHeadBoundaryAction::Fail(mut error) => {
                error.set_retry(false);
                error
            }
            ResponseHeadBoundaryAction::Replace(_) => {
                let mut error = Error::explain(
                    InternalError,
                    format!(
                        "response head {} boundary permits Fail only in v1",
                        boundary.as_str()
                    ),
                );
                error.set_retry(false);
                error
            }
        }
    }

    pub(crate) fn resolve_response_head_wait_error(
        &self,
        session: &Session,
        ctx: &mut SV::CTX,
        state: &mut ResponsePipelineState,
        error: pingora_error::BError,
    ) -> pingora_error::BError {
        match state.take_pending_head_boundary() {
            Some(boundary) => self.resolve_response_head_fail_only(session, boundary, ctx, state),
            None => {
                if state.head_barrier.is_holding() {
                    let usage = state.head_barrier.claim_boundary().unwrap_or_default();
                    state.sink.disarm_response_head_release();
                    state.upstream_reusable = false;
                    self.inner.response_head_hold_outcome(
                        session,
                        ResponseHeadOutcome::Cancelled,
                        usage,
                        ctx,
                    );
                }
                error
            }
        }
    }

    pub(crate) fn resolve_response_head_idle_timeout(
        &self,
        session: &Session,
        ctx: &mut SV::CTX,
        state: &mut ResponsePipelineState,
    ) -> pingora_error::BError {
        self.resolve_response_head_fail_only(session, ResponseHeadBoundary::Timeout, ctx, state)
    }

    pub(crate) fn cancel_response_head_hold(
        &self,
        session: &Session,
        ctx: &mut SV::CTX,
        state: &mut ResponsePipelineState,
    ) {
        let Some(usage) = state.head_barrier.claim_boundary() else {
            return;
        };
        state.sink.disarm_response_head_release();
        state.upstream_reusable = false;
        self.inner
            .response_head_hold_outcome(session, ResponseHeadOutcome::Cancelled, usage, ctx);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enforce_response_head_source_input(
        &self,
        session: &mut Session,
        task: &HttpTask,
        ctx: &mut SV::CTX,
        state: &mut ResponsePipelineState,
        out_tasks: &mut Vec<HttpTask>,
    ) -> Result<bool>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let boundary = match state.head_barrier.observe_input(task) {
            Ok(()) => return Ok(false),
            Err(ResponseHeadBarrierFailure::Boundary(boundary)) => boundary,
            Err(ResponseHeadBarrierFailure::Source(error)) => return Err(error),
        };
        let start = out_tasks.len();
        let output =
            self.resolve_response_head_boundary(session, boundary, ctx, state, out_tasks, start)?;
        self.prepare_response_head_output(session, ctx, state, out_tasks, output, true, true)
            .await?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_response_head_output(
        &self,
        session: &mut Session,
        ctx: &mut SV::CTX,
        state: &mut ResponsePipelineState,
        out_tasks: &mut Vec<HttpTask>,
        barrier_output: ResponseHeadBarrierOutput,
        terminal_header: bool,
        filter_downstream_body: bool,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        match barrier_output {
            ResponseHeadBarrierOutput::PrepareFrom(start)
            | ResponseHeadBarrierOutput::PrepareReplacementFrom(start) => {
                let replacement = matches!(
                    barrier_output,
                    ResponseHeadBarrierOutput::PrepareReplacementFrom(_)
                );
                if terminal_header || replacement {
                    let downstream_body_forbidden = match &out_tasks[start] {
                        HttpTask::Header(header, _) => {
                            downstream_response_body_forbidden(session, header)
                        }
                        _ => unreachable!("terminal response must start with a header"),
                    };
                    if !downstream_body_forbidden {
                        self.downstream_response_filter_tasks_in_order(
                            session,
                            &mut out_tasks[start..],
                            ctx,
                        )
                        .await?;
                    }
                    reconcile_terminal_response_tasks(out_tasks, start, downstream_body_forbidden)?;
                    if replacement {
                        state.suppress_downstream_body = true;
                    }
                } else if filter_downstream_body {
                    self.downstream_response_filter_tasks_in_order(
                        session,
                        &mut out_tasks[start..],
                        ctx,
                    )
                    .await?;
                }
                if let Some(header) = out_tasks[start..].iter().find_map(|task| match task {
                    HttpTask::Header(header, _)
                        if !header.status.is_informational()
                            || header.status == http::StatusCode::SWITCHING_PROTOCOLS =>
                    {
                        Some(header.as_ref())
                    }
                    _ => None,
                }) {
                    self.inner.response_head_will_commit(session, header, ctx)?;
                    session.mark_response_head_writer_handoff();
                }
                session
                    .prepare_response_headers(&mut out_tasks[start..])
                    .await?;
            }
            ResponseHeadBarrierOutput::Held => {}
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn response_task_pipeline(
        &self,
        protocol: ResponseProtocol,
        session: &mut Session,
        task: HttpTask,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
        from_cache: bool,
        state: &mut ResponsePipelineState,
        out_tasks: &mut Vec<HttpTask>,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let start = out_tasks.len();
        if state.head_barrier.is_holding() {
            if let HttpTask::Failed(error) = task {
                let usage = state.head_barrier.claim_boundary().unwrap_or_default();
                state.sink.disarm_response_head_release();
                state.upstream_reusable = false;
                abort_cache_after_response_source_failure(session, from_cache);
                self.inner.response_head_hold_outcome(
                    session,
                    ResponseHeadOutcome::Failed(ResponseHeadBoundary::SourceFailed),
                    usage,
                    ctx,
                );
                return Err(error);
            }
        }
        let result = self
            .response_task_pipeline_inner(
                protocol,
                session,
                task,
                ctx,
                serve_from_cache,
                from_cache,
                state,
                out_tasks,
            )
            .await;
        let Err(error) = result else {
            return result;
        };
        let Some(boundary) = state.take_pending_head_boundary() else {
            if state.head_barrier.is_holding() {
                let usage = state.head_barrier.claim_boundary().unwrap_or_default();
                state.sink.disarm_response_head_release();
                state.upstream_reusable = false;
                self.inner.response_head_hold_outcome(
                    session,
                    ResponseHeadOutcome::Cancelled,
                    usage,
                    ctx,
                );
            }
            return Err(error);
        };
        out_tasks.truncate(start);
        if boundary == ResponseHeadBoundary::Timeout {
            return Err(self.resolve_response_head_fail_only(session, boundary, ctx, state));
        }
        let output =
            self.resolve_response_head_boundary(session, boundary, ctx, state, out_tasks, start)?;
        self.prepare_response_head_output(session, ctx, state, out_tasks, output, true, true)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn response_task_pipeline_inner(
        &self,
        protocol: ResponseProtocol,
        session: &mut Session,
        mut task: HttpTask,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
        from_cache: bool,
        state: &mut ResponsePipelineState,
        out_tasks: &mut Vec<HttpTask>,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        if state.origin_abandoned {
            return Ok(());
        }
        let source_failed = matches!(&task, HttpTask::Failed(_));
        let source_clean_terminal = task.is_end() && !source_failed;
        let terminal_header = !from_cache
            && matches!(
                &task,
                HttpTask::Header(header, true) if !header.status.is_informational()
            );
        let filter_downstream_body = terminal_header
            || matches!(&task, HttpTask::Body(..))
            || (protocol.supports_upgraded_body() && matches!(&task, HttpTask::UpgradedBody(..)))
            || (from_cache && matches!(&task, HttpTask::Done));
        let mut terminal_cacheability = None;
        let mut terminal_event = None;

        if state.head_barrier.is_holding() {
            let boundary = if state.sink.reserve_response_head_work(1).is_err()
                || state.sink.response_head_work_limit_exceeded()
            {
                Some(ResponseHeadBoundary::WorkLimit)
            } else {
                state
                    .head_barrier
                    .set_work_usage(state.sink.response_head_work_units().unwrap_or_default())
                    .err()
                    .and_then(|failure| match failure {
                        ResponseHeadBarrierFailure::Boundary(boundary) => Some(boundary),
                        ResponseHeadBarrierFailure::Source(_) => None,
                    })
            };
            if let Some(boundary) = boundary {
                let start = out_tasks.len();
                let output = self.resolve_response_head_boundary(
                    session, boundary, ctx, state, out_tasks, start,
                )?;
                self.prepare_response_head_output(
                    session, ctx, state, out_tasks, output, true, true,
                )
                .await?;
                return Ok(());
            }
        }

        if !from_cache {
            if protocol.validates_upgrade_before_upstream_filter() {
                if let HttpTask::Header(header, _) = &task {
                    reject_mismatched_h1_upgrade_101(session, header, "custom_upstream_filter")
                        .map_err(|e| e.into_up())?;
                }
            }

            let deadline = state.response_head_deadline();
            let filtered = await_response_head_deadline(
                deadline,
                self.upstream_filter(session, &mut task, &mut state.sink, ctx),
            )
            .await;
            if let Some(duration) = state.finish_head_deadline_wait(filtered)? {
                trace!("delaying upstream response for {duration:?}");
                let deadline = state.response_head_deadline();
                let slept = await_response_head_deadline(deadline, async {
                    time::sleep(duration).await;
                    Ok(())
                })
                .await;
                state.finish_head_deadline_wait(slept)?;
            }

            if protocol.validates_upgrade_after_upstream_filter() {
                if let HttpTask::Header(header, _) = &task {
                    reject_mismatched_h1_upgrade_101(session, header, "h1_upstream_filter")
                        .map_err(|e| e.into_up())?;
                }
            }

            terminal_event = state.terminal_body.claim_for(&task);
            if let Some(event) = terminal_event {
                let deadline = state.response_head_deadline();
                let filtered = await_response_head_deadline(
                    deadline,
                    self.terminal_upstream_body_filter(session, event, &mut state.sink, ctx),
                )
                .await;
                if let Some(duration) = state.finish_head_deadline_wait(filtered)? {
                    trace!("delaying terminal upstream response for {duration:?}");
                    let deadline = state.response_head_deadline();
                    let slept = await_response_head_deadline(deadline, async {
                        time::sleep(duration).await;
                        Ok(())
                    })
                    .await;
                    state.finish_head_deadline_wait(slept)?;
                }
            }
            if let HttpTask::Trailer(Some(trailers)) = &mut task {
                let deadline = state.response_head_deadline();
                let filtered = await_response_head_deadline(
                    deadline,
                    self.inner
                        .upstream_response_trailer_filter(session, trailers, ctx),
                )
                .await;
                state.finish_head_deadline_wait(filtered)?;
            }

            if terminal_header {
                let HttpTask::Header(header, _) = &task else {
                    unreachable!("terminal task must be a header")
                };
                terminal_cacheability =
                    self.response_cacheability_before_downstream_filter(session, header, ctx)?;
            } else {
                if terminal_event.is_some() {
                    self.cache_task_and_emitted_chunks_before(
                        session,
                        &task,
                        &state.sink,
                        state.terminal_body.is_upgraded(),
                        ctx,
                        serve_from_cache,
                    )
                    .await?;
                } else {
                    self.cache_task_and_emitted_chunks(
                        session,
                        &task,
                        &state.sink,
                        ctx,
                        serve_from_cache,
                    )
                    .await?;
                }
                self.track_predicted_uncacheable_response(session, &task, &state.sink);
            }

            if !terminal_header && !serve_from_cache.should_send_to_downstream() {
                state.sink.take_extra();
                if let HttpTask::Failed(error) = task {
                    abort_cache_after_response_source_failure(session, false);
                    return Err(error);
                }
                out_tasks.push(task);
                return Ok(());
            }
        }

        if state.suppress_downstream_body && is_downstream_followup(&task) {
            state.sink.take_extra();
            if matches!(task, HttpTask::Failed(_)) {
                state.upstream_reusable = false;
                abort_cache_after_response_source_failure(session, from_cache);
            }
            return Ok(());
        }

        let task = match task {
            HttpTask::Header(mut header, eos) => {
                let cache_header = terminal_header.then(|| header.clone());
                if !from_cache {
                    proxy_cache::strip_terminal_synthetic_wire_marker(&mut header);
                }
                let terminal_synthetic_entity = proxy_cache::is_terminal_synthetic_entity(&header);
                let substituted = if from_cache {
                    state
                        .filtered_terminal_header
                        .take()
                        .map(|filtered_header| header = filtered_header)
                        .is_some()
                } else {
                    false
                };
                if !substituted {
                    let apply_conditional = if protocol.preserves_custom_conditional_filter_gate() {
                        session.cache.enabled()
                    } else {
                        session.upstream_headers_mutated_for_cache()
                    };
                    if apply_conditional {
                        self.downstream_response_conditional_filter(
                            serve_from_cache,
                            session,
                            &mut header,
                            ctx,
                        );
                        let skip_range = if from_cache {
                            terminal_synthetic_entity
                        } else {
                            terminal_header
                        };
                        if !skip_range && !session.ignore_downstream_range {
                            let range_type =
                                self.inner.range_header_filter(session, &mut header, ctx);
                            state.range_body_filter.set(range_type);
                        }
                    }
                    let deadline = state.response_head_deadline();
                    let filtered = await_response_head_deadline(
                        deadline,
                        self.inner.response_filter(session, &mut header, ctx),
                    )
                    .await;
                    state.finish_head_deadline_wait(filtered)?;
                }

                let final_header = !header.status.is_informational()
                    || header.status == http::StatusCode::SWITCHING_PROTOCOLS;
                if final_header && state.head_barrier.is_awaiting_final_head() {
                    let source = if from_cache {
                        ResponseHeadSource::Cache
                    } else {
                        ResponseHeadSource::Origin
                    };
                    let plan = self
                        .inner
                        .response_head_commit_plan(session, source, &header, ctx)?;
                    let hold_selected = matches!(&plan, ResponseHeadCommitPlan::Hold(_));
                    state.head_barrier.select(plan)?;
                    if state.head_barrier.is_holding() {
                        let work_limit = state
                            .head_barrier
                            .work_limit()
                            .expect("Holding must expose its work limit");
                        if !state
                            .sink
                            .arm_response_head_release_with_work_limit(work_limit)
                        {
                            return Error::e_explain(
                                InternalError,
                                "response-head release latch was already armed",
                            );
                        }
                        session.mark_response_head_attempt_selected();
                        let unsupported = from_cache
                            || protocol == ResponseProtocol::Custom
                            || serve_from_cache.is_on()
                            || session.cache.enabled()
                            || header.status == http::StatusCode::SWITCHING_PROTOCOLS
                            || downstream_response_body_forbidden(session, &header)
                            || session.req_header().method == http::Method::CONNECT
                            || session.as_downstream().is_upgrade_req();
                        let boundary = if unsupported {
                            Some(ResponseHeadBoundary::Unsupported)
                        } else if state.sink.reserve_response_head_work(1).is_err() {
                            Some(ResponseHeadBoundary::WorkLimit)
                        } else {
                            state
                                .head_barrier
                                .set_work_usage(
                                    state.sink.response_head_work_units().unwrap_or_default(),
                                )
                                .err()
                                .and_then(|failure| match failure {
                                    ResponseHeadBarrierFailure::Boundary(boundary) => {
                                        Some(boundary)
                                    }
                                    ResponseHeadBarrierFailure::Source(_) => None,
                                })
                        };
                        if let Some(boundary) = boundary {
                            let start = out_tasks.len();
                            let output = self.resolve_response_head_boundary(
                                session, boundary, ctx, state, out_tasks, start,
                            )?;
                            self.prepare_response_head_output(
                                session, ctx, state, out_tasks, output, true, true,
                            )
                            .await?;
                            return Ok(());
                        }
                    } else {
                        debug_assert!(!hold_selected);
                        self.inner.response_head_hold_outcome(
                            session,
                            ResponseHeadOutcome::Immediate,
                            Default::default(),
                            ctx,
                        );
                    }
                }

                if protocol != ResponseProtocol::H1
                    && !from_cache
                    && session.as_downstream().is_upgrade_req()
                    && header.status == http::StatusCode::SWITCHING_PROTOCOLS
                {
                    state.terminal_body.mark_upgraded();
                }

                if terminal_header {
                    let deadline = state.response_head_deadline();
                    let filtered = await_response_head_deadline(
                        deadline,
                        self.terminal_upstream_body_filter(
                            session,
                            UpstreamResponseBodyEvent::TerminalWithoutTrailers,
                            &mut state.sink,
                            ctx,
                        ),
                    )
                    .await;
                    if let Some(duration) = state.finish_head_deadline_wait(filtered)? {
                        trace!("delaying terminal upstream response for {duration:?}");
                        let deadline = state.response_head_deadline();
                        let slept = await_response_head_deadline(deadline, async {
                            time::sleep(duration).await;
                            Ok(())
                        })
                        .await;
                        state.finish_head_deadline_wait(slept)?;
                    }
                    let mut cache_header =
                        cache_header.expect("terminal header must retain its cache representation");
                    reconcile_terminal_cache_header(&mut cache_header, &state.sink);
                    reconcile_terminal_cache_header(&mut header, &state.sink);
                    proxy_cache::mark_terminal_synthetic_entity(&mut cache_header);
                    state.filtered_terminal_header = Some(header.clone());
                    let cache_task = HttpTask::Header(cache_header, true);
                    self.track_predicted_uncacheable_response(session, &cache_task, &state.sink);
                    self.cache_task_and_emitted_chunks_with_decision(
                        session,
                        &cache_task,
                        &state.sink,
                        terminal_cacheability,
                        ctx,
                        serve_from_cache,
                    )
                    .await?;
                    if !serve_from_cache.should_send_to_downstream() {
                        state.sink.take_extra();
                        return Ok(());
                    }
                }

                if downstream_response_body_forbidden(session, &header) {
                    state.sink.take_extra();
                    header.remove_header(&http::header::TRANSFER_ENCODING);
                    if header.status.is_informational() || header.status.as_u16() == 204 {
                        header.remove_header(&http::header::CONTENT_LENGTH);
                    }
                }
                if !header.status.is_informational() {
                    state.suppress_downstream_body =
                        terminal_header || downstream_response_body_forbidden(session, &header);
                }

                header.set_version(Version::HTTP_11);
                match protocol {
                    ResponseProtocol::H1 => {
                        if !state.suppress_downstream_body
                            && header
                                .headers
                                .get(http::header::TRANSFER_ENCODING)
                                .is_none()
                            && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                            && (!eos || !state.sink.peek_extra().is_empty())
                        {
                            header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                        }
                        if !from_cache {
                            reject_mismatched_h1_upgrade_101(
                                session,
                                &header,
                                "h1_response_filter",
                            )
                            .map_err(|e| e.into_in())?;
                        }
                    }
                    ResponseProtocol::H2 => {
                        if !downstream_response_body_forbidden(session, &header)
                            && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                        {
                            header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                        }
                    }
                    ResponseProtocol::Custom => {
                        if !from_cache {
                            reject_mismatched_h1_upgrade_101(
                                session,
                                &header,
                                "custom_response_filter",
                            )
                            .map_err(|e| e.into_in())?;
                        }
                        if !downstream_response_body_forbidden(session, &header)
                            && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                        {
                            header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                        }
                    }
                }
                HttpTask::Header(header, eos || state.suppress_downstream_body)
            }
            HttpTask::Body(data, eos) => {
                HttpTask::Body(state.range_body_filter.filter_body(data), eos)
            }
            HttpTask::UpgradedBody(data, eos) if protocol.supports_upgraded_body() => {
                HttpTask::UpgradedBody(data, eos)
            }
            HttpTask::UpgradedBody(..) => {
                panic!("Unexpected UpgradedBody task while proxy h2")
            }
            HttpTask::Trailer(trailers) => HttpTask::Trailer(trailers),
            HttpTask::Done if from_cache => HttpTask::Body(None, true),
            task @ (HttpTask::Done | HttpTask::Failed(_)) => task,
        };

        let start = out_tasks.len();
        if from_cache {
            out_tasks.push(task);
        } else if terminal_event.is_some() {
            drain_emitted_chunks_before(
                task,
                &mut state.sink,
                state.terminal_body.is_upgraded(),
                out_tasks,
            );
        } else {
            drain_emitted_chunks(task, &mut state.sink, out_tasks);
        }

        if state.head_barrier.is_holding() {
            let boundary = if state.sink.response_head_work_limit_exceeded() {
                Some(ResponseHeadBoundary::WorkLimit)
            } else if let Some(work_units) = state.sink.response_head_work_units() {
                state
                    .head_barrier
                    .set_work_usage(work_units)
                    .err()
                    .and_then(|failure| match failure {
                        ResponseHeadBarrierFailure::Boundary(boundary) => Some(boundary),
                        ResponseHeadBarrierFailure::Source(_) => None,
                    })
            } else {
                None
            };
            if let Some(boundary) = boundary {
                let output = self.resolve_response_head_boundary(
                    session, boundary, ctx, state, out_tasks, start,
                )?;
                return self
                    .prepare_response_head_output(
                        session, ctx, state, out_tasks, output, true, true,
                    )
                    .await;
            }
        }
        let decision = state.sink.take_response_head_decision();
        let release_requested = matches!(decision, Some(ResponseHeadDecision::Release));
        let unresolved_terminate =
            state.head_barrier.is_holding() && state.sink.is_terminated() && !release_requested;
        let barrier_output = match decision {
            Some(ResponseHeadDecision::Release) | None if unresolved_terminate => Err(
                ResponseHeadBarrierFailure::Boundary(ResponseHeadBoundary::ApplicationTerminate),
            ),
            Some(ResponseHeadDecision::Release) | None => state.head_barrier.capture_or_release(
                out_tasks,
                start,
                release_requested,
                source_clean_terminal,
            ),
            Some(ResponseHeadDecision::Replace(replacement)) => {
                state.upstream_reusable = false;
                state.origin_abandoned = true;
                if session.req_header().method == http::Method::CONNECT
                    && replacement.header().status.is_success()
                {
                    Err(ResponseHeadBarrierFailure::Boundary(
                        ResponseHeadBoundary::Unsupported,
                    ))
                } else {
                    state.head_barrier.replace(out_tasks, start, replacement)
                }
            }
            Some(ResponseHeadDecision::Fail(mut error)) => {
                error.set_retry(false);
                state.upstream_reusable = false;
                let usage = state.head_barrier.boundary_usage().unwrap_or_default();
                state.head_barrier.abort();
                out_tasks.truncate(start);
                state.sink.disarm_response_head_release();
                self.inner.response_head_hold_outcome(
                    session,
                    ResponseHeadOutcome::Failed(ResponseHeadBoundary::ApplicationFail),
                    usage,
                    ctx,
                );
                return Err(error);
            }
        };
        let barrier_output = match barrier_output {
            Ok(output) => output,
            Err(ResponseHeadBarrierFailure::Source(error)) => {
                let usage = state.head_barrier.claim_boundary().unwrap_or_default();
                state.sink.disarm_response_head_release();
                state.upstream_reusable = false;
                self.inner.response_head_hold_outcome(
                    session,
                    ResponseHeadOutcome::Failed(ResponseHeadBoundary::SourceFailed),
                    usage,
                    ctx,
                );
                return Err(error);
            }
            Err(ResponseHeadBarrierFailure::Boundary(boundary)) => self
                .resolve_response_head_boundary(session, boundary, ctx, state, out_tasks, start)?,
        };
        if let Some(usage) = state.head_barrier.take_resolved_usage() {
            let outcome = match barrier_output {
                ResponseHeadBarrierOutput::PrepareReplacementFrom(_) => {
                    ResponseHeadOutcome::Replaced
                }
                ResponseHeadBarrierOutput::PrepareFrom(_) => ResponseHeadOutcome::Released,
                ResponseHeadBarrierOutput::Held => unreachable!(),
            };
            state.sink.disarm_response_head_release();
            self.inner
                .response_head_hold_outcome(session, outcome, usage, ctx);
        }
        self.prepare_response_head_output(
            session,
            ctx,
            state,
            out_tasks,
            barrier_output,
            terminal_header,
            filter_downstream_body || terminal_event.is_some(),
        )
        .await
    }
}

#[cfg(test)]
#[path = "response_pipeline_tests.rs"]
mod tests;
