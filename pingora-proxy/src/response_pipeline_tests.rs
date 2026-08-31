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
use crate::{
    ResponseHeadHoldLimits, ResponseHeadHoldPlan, ResponseHeadReplacement, ResponseHeadUsage,
};
use async_trait::async_trait;
use bytes::Bytes;
use pingora_core::{server::configuration::ServerConf, upstreams::peer::HttpPeer};
use pingora_error::{Error, ErrorType::InternalError};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;

struct ParityProxy;
struct PipelineBenchProxy;

struct HoldReleaseCtx {
    response_filter_complete: bool,
    plan_calls: AtomicUsize,
    will_commit_calls: AtomicUsize,
    body_calls: usize,
    downstream_body_calls: usize,
    downstream_trailer_calls: usize,
    release_on_body_call: Option<usize>,
    emit_extra_on_body_call: Option<usize>,
    stall_on_body_call: Option<usize>,
    replace_on_body_call: Option<usize>,
    empty_replace_on_body_call: Option<usize>,
    fail_on_body_call: Option<usize>,
    terminate_on_body_call: Option<usize>,
    reserve_work_on_body_call: Option<(usize, u64)>,
    boundary_replacement: Option<ResponseHeadBoundary>,
    max_input_bytes: usize,
    max_output_bytes: usize,
    max_work_units: u64,
    hold_timeout: std::time::Duration,
    boundaries: Vec<ResponseHeadBoundary>,
    outcomes: Vec<(ResponseHeadOutcome, ResponseHeadUsage)>,
}

impl Default for HoldReleaseCtx {
    fn default() -> Self {
        Self {
            response_filter_complete: false,
            plan_calls: AtomicUsize::new(0),
            will_commit_calls: AtomicUsize::new(0),
            body_calls: 0,
            downstream_body_calls: 0,
            downstream_trailer_calls: 0,
            release_on_body_call: None,
            emit_extra_on_body_call: None,
            stall_on_body_call: None,
            replace_on_body_call: None,
            empty_replace_on_body_call: None,
            fail_on_body_call: None,
            terminate_on_body_call: None,
            reserve_work_on_body_call: None,
            boundary_replacement: None,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            max_work_units: 32,
            hold_timeout: std::time::Duration::from_secs(30),
            boundaries: Vec::new(),
            outcomes: Vec::new(),
        }
    }
}

struct HoldReleaseProxy;

#[async_trait]
impl ProxyHttp for HoldReleaseProxy {
    type CTX = HoldReleaseCtx;

    fn new_ctx(&self) -> Self::CTX {
        HoldReleaseCtx::default()
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("hold tests call only the response pipeline")
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        _header: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.response_filter_complete = true;
        Ok(())
    }

    fn response_head_commit_plan(
        &self,
        _session: &Session,
        _source: ResponseHeadSource,
        _response: &ResponseHeader,
        ctx: &Self::CTX,
    ) -> Result<ResponseHeadCommitPlan> {
        assert!(
            ctx.response_filter_complete,
            "head plan must run after the final response filter"
        );
        ctx.plan_calls.fetch_add(1, Ordering::Relaxed);
        Ok(ResponseHeadCommitPlan::Hold(
            ResponseHeadHoldPlan::new_for_test(ResponseHeadHoldLimits::new_full_for_test(
                ctx.max_input_bytes,
                ctx.max_output_bytes,
                16,
                32,
                4096,
                ctx.max_work_units,
                ctx.hold_timeout,
            )),
        ))
    }

    fn response_head_will_commit(
        &self,
        _session: &Session,
        chosen_header: &ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        assert!(!chosen_header.status.is_informational());
        ctx.will_commit_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn response_head_hold_boundary(
        &self,
        _session: &Session,
        boundary: ResponseHeadBoundary,
        ctx: &mut Self::CTX,
    ) -> ResponseHeadBoundaryAction {
        ctx.boundaries.push(boundary);
        if ctx.boundary_replacement == Some(boundary) {
            return ResponseHeadBoundaryAction::Replace(ResponseHeadReplacement::new(
                Box::new(ResponseHeader::build(429, None).unwrap()),
                vec![Bytes::from_static(b"bounded")],
            ));
        }
        let mut error = Error::explain(
            InternalError,
            format!("test response head boundary: {}", boundary.as_str()),
        );
        error.set_retry(false);
        ResponseHeadBoundaryAction::Fail(error)
    }

    fn response_head_hold_outcome(
        &self,
        _session: &Session,
        outcome: ResponseHeadOutcome,
        usage: ResponseHeadUsage,
        ctx: &mut Self::CTX,
    ) {
        ctx.outcomes.push((outcome, usage));
    }

    async fn upstream_response_body_filter_event(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _event: UpstreamResponseBodyEvent,
        sink: &mut ResponseBodySink,
        ctx: &mut Self::CTX,
    ) -> Result<Option<std::time::Duration>> {
        ctx.body_calls += 1;
        if ctx.stall_on_body_call == Some(ctx.body_calls) {
            std::future::pending::<()>().await;
        }
        if ctx.emit_extra_on_body_call == Some(ctx.body_calls) {
            sink.push(Bytes::from_static(b"extra"))?;
        }
        if ctx.replace_on_body_call == Some(ctx.body_calls) {
            sink.replace_response_head(ResponseHeadReplacement::new(
                Box::new(ResponseHeader::build(403, None).unwrap()),
                vec![Bytes::from_static(b"blocked")],
            ))?;
        }
        if ctx.empty_replace_on_body_call == Some(ctx.body_calls) {
            sink.replace_response_head(ResponseHeadReplacement::new(
                Box::new(ResponseHeader::build(403, None).unwrap()),
                Vec::new(),
            ))?;
        }
        if ctx.fail_on_body_call == Some(ctx.body_calls) {
            sink.fail_response_head(Error::explain(
                InternalError,
                "application head failure marker",
            ))?;
        }
        if ctx.terminate_on_body_call == Some(ctx.body_calls) {
            sink.terminate();
        }
        if let Some((call, units)) = ctx.reserve_work_on_body_call {
            if call == ctx.body_calls {
                let _ = sink.reserve_response_head_work(units);
            }
        }
        if ctx.release_on_body_call == Some(ctx.body_calls) {
            assert!(sink.release_response_head());
        }
        Ok(None)
    }

    fn response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<std::time::Duration>> {
        ctx.downstream_body_calls += 1;
        Ok(None)
    }

    async fn response_trailer_filter(
        &self,
        _session: &mut Session,
        _trailers: &mut http::HeaderMap,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Bytes>> {
        ctx.downstream_trailer_calls += 1;
        Ok(None)
    }
}

#[async_trait]
impl ProxyHttp for PipelineBenchProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("benchmark calls only the response pipeline")
    }
}

#[async_trait]
impl ProxyHttp for ParityProxy {
    type CTX = Vec<&'static str>;

    fn new_ctx(&self) -> Self::CTX {
        Vec::new()
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("parity test calls only the response pipeline")
    }

    async fn upstream_response_header_filter_event(
        &self,
        _session: &mut Session,
        _header: &mut ResponseHeader,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.push("upstream-header");
        Ok(())
    }

    async fn upstream_response_body_filter_event(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        event: UpstreamResponseBodyEvent,
        sink: &mut ResponseBodySink,
        ctx: &mut Self::CTX,
    ) -> Result<Option<std::time::Duration>> {
        match event {
            UpstreamResponseBodyEvent::Data { .. } => {
                ctx.push("upstream-body");
                sink.push(Bytes::from_static(b"extra"))?;
            }
            UpstreamResponseBodyEvent::TerminalBeforeTrailers => {
                ctx.push("upstream-terminal-before-trailers");
                *body = Some(Bytes::from_static(b"released"));
            }
            UpstreamResponseBodyEvent::TerminalWithoutTrailers => {
                ctx.push("upstream-terminal-without-trailers");
            }
        }
        Ok(None)
    }

    async fn upstream_response_trailer_filter(
        &self,
        _session: &mut Session,
        _trailers: &mut http::HeaderMap,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.push("upstream-trailer");
        Ok(())
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        _header: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.push("downstream-header");
        Ok(())
    }

    fn response_head_will_commit(
        &self,
        _session: &Session,
        _chosen_header: &ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.push("will-commit");
        Ok(())
    }

    fn response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<std::time::Duration>> {
        ctx.push("downstream-body");
        Ok(None)
    }

    async fn response_trailer_filter(
        &self,
        _session: &mut Session,
        _trailers: &mut http::HeaderMap,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Bytes>> {
        ctx.push("downstream-trailer");
        Ok(None)
    }
}

async fn request_session() -> Session {
    let (mut client, server) = tokio::io::duplex(1024);
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .expect("test request should be written");
    let mut session = Session::new_h1(Box::new(server) as pingora_core::protocols::Stream);
    session
        .read_request()
        .await
        .expect("test request should parse");
    session
}

fn live_tasks() -> Vec<HttpTask> {
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-parity", http::HeaderValue::from_static("yes"));
    vec![
        HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
        HttpTask::Body(Some(Bytes::from_static(b"body")), false),
        HttpTask::Trailer(Some(Box::new(trailers))),
        HttpTask::Done,
        HttpTask::Failed(Error::explain(InternalError, "parity failure")),
    ]
}

fn cached_tasks() -> Vec<HttpTask> {
    vec![
        HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
        HttpTask::Body(Some(Bytes::from_static(b"cached")), false),
        HttpTask::Done,
    ]
}

fn output_body_values(tasks: &[HttpTask]) -> Vec<&[u8]> {
    tasks
        .iter()
        .filter_map(|task| match task {
            HttpTask::Body(Some(body), _) => Some(body.as_ref()),
            _ => None,
        })
        .collect()
}

async fn run_pipeline(
    protocol: ResponseProtocol,
    from_cache: bool,
    tasks: Vec<HttpTask>,
) -> (String, Vec<&'static str>, bool, bool) {
    let proxy = HttpProxy::new(ParityProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = Vec::new();
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    for task in tasks {
        proxy
            .response_task_pipeline(
                protocol,
                &mut session,
                task,
                &mut ctx,
                &mut serve_from_cache,
                from_cache,
                &mut state,
                &mut out,
            )
            .await
            .unwrap();
    }

    (
        format!("{out:?}"),
        ctx,
        state.suppress_downstream_body,
        state.upstream_reusable,
    )
}

#[test]
fn protocol_adapter_table_contains_only_documented_compatibility_differences() {
    struct Row {
        protocol: ResponseProtocol,
        validate_before: bool,
        validate_after: bool,
        cache_enabled_gate: bool,
        upgraded_body: bool,
    }

    let rows = [
        Row {
            protocol: ResponseProtocol::H1,
            validate_before: false,
            validate_after: true,
            cache_enabled_gate: false,
            upgraded_body: true,
        },
        Row {
            protocol: ResponseProtocol::H2,
            validate_before: false,
            validate_after: false,
            cache_enabled_gate: false,
            upgraded_body: false,
        },
        Row {
            protocol: ResponseProtocol::Custom,
            validate_before: true,
            validate_after: false,
            cache_enabled_gate: true,
            upgraded_body: true,
        },
    ];

    for row in rows {
        assert_eq!(
            row.protocol.validates_upgrade_before_upstream_filter(),
            row.validate_before,
            "{:?} pre-filter 101 policy drifted",
            row.protocol
        );
        assert_eq!(
            row.protocol.validates_upgrade_after_upstream_filter(),
            row.validate_after,
            "{:?} post-filter 101 policy drifted",
            row.protocol
        );
        assert_eq!(
            row.protocol.preserves_custom_conditional_filter_gate(),
            row.cache_enabled_gate,
            "{:?} conditional-filter gate drifted",
            row.protocol
        );
        assert_eq!(
            row.protocol.supports_upgraded_body(),
            row.upgraded_body,
            "{:?} upgraded-body policy drifted",
            row.protocol
        );
    }
}

#[test]
fn response_pipeline_state_starts_with_one_response_scope() {
    let state = ResponsePipelineState::default();
    assert!(!state.suppress_downstream_body);
    assert!(state.filtered_terminal_header.is_none());
    assert!(state.upstream_reusable);
    assert!(!state.sink.is_terminated());
    assert!(!state.terminal_body.is_upgraded());
}

#[tokio::test]
async fn live_header_body_trailer_done_failed_semantics_match_all_protocols() {
    let h1 = run_pipeline(ResponseProtocol::H1, false, live_tasks()).await;
    for protocol in [ResponseProtocol::H2, ResponseProtocol::Custom] {
        assert_eq!(
            run_pipeline(protocol, false, live_tasks()).await,
            h1,
            "{protocol:?} diverged from the shared live response semantics"
        );
    }
    assert_eq!(
        h1.1,
        vec![
            "upstream-header",
            "downstream-header",
            "will-commit",
            "upstream-body",
            "downstream-body",
            "downstream-body",
            "upstream-terminal-before-trailers",
            "upstream-trailer",
            "downstream-body",
            "downstream-trailer",
        ]
    );
    assert!(h1.0.contains("released"));
    assert!(h1.0.contains("x-parity"));
    assert!(h1.0.contains("parity failure"));
}

#[tokio::test]
async fn cache_hit_header_body_done_semantics_match_all_protocols() {
    let h1 = run_pipeline(ResponseProtocol::H1, true, cached_tasks()).await;
    for protocol in [ResponseProtocol::H2, ResponseProtocol::Custom] {
        assert_eq!(
            run_pipeline(protocol, true, cached_tasks()).await,
            h1,
            "{protocol:?} diverged from the shared cache-hit semantics"
        );
    }
    assert_eq!(
        h1.1,
        vec![
            "downstream-header",
            "will-commit",
            "downstream-body",
            "downstream-body"
        ]
    );
}

#[tokio::test]
async fn held_head_and_body_cross_batches_until_release() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        release_on_body_call: Some(2),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    assert!(
        out.is_empty(),
        "a held final head must not reach the writer batch"
    );
    assert!(state.head_barrier.is_holding());
    assert!(session.response_head_retry_closed());
    assert_eq!(ctx.plan_calls.load(Ordering::Relaxed), 1);
    assert_eq!(session.prepared_response_headers, 0);
    assert_eq!(ctx.will_commit_calls.load(Ordering::Relaxed), 0);

    state.sink.reset_batch();
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"one")), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    assert!(out.is_empty(), "a pre-decision body must remain held");
    assert_eq!(
        ctx.downstream_body_calls, 0,
        "downstream-only filters must not observe a held prefix"
    );

    state.sink.reset_batch();
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"two")), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();

    assert!(matches!(out.first(), Some(HttpTask::Header(..))));
    assert_eq!(
        output_body_values(&out),
        vec![b"one".as_slice(), b"two".as_slice()]
    );
    assert!(!state.head_barrier.is_holding());
    assert_eq!(session.prepared_response_headers, 1);
    assert_eq!(ctx.will_commit_calls.load(Ordering::Relaxed), 1);
    assert_eq!(ctx.downstream_body_calls, 2);
    assert_eq!(ctx.outcomes.len(), 1);
    assert_eq!(ctx.outcomes[0].0, ResponseHeadOutcome::Released);
    assert_eq!(ctx.outcomes[0].1.output_bytes(), 6);
    assert_eq!(ctx.outcomes[0].1.work_units(), 3);
}

#[tokio::test]
async fn cancelling_a_pump_reports_hold_outcome_once() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx::default();
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    proxy.cancel_response_head_hold(&session, &mut ctx, &mut state);
    proxy.cancel_response_head_hold(&session, &mut ctx, &mut state);
    assert_eq!(ctx.outcomes.len(), 1);
    assert_eq!(ctx.outcomes[0].0, ResponseHeadOutcome::Cancelled);
    assert!(!state.head_barrier.is_holding());
    assert!(!state.upstream_reusable);
    assert!(out.is_empty());
}

#[tokio::test]
async fn input_output_and_work_limits_report_distinct_boundaries() {
    for (mut ctx, expected, body) in [
        (
            HoldReleaseCtx {
                max_input_bytes: 3,
                ..HoldReleaseCtx::default()
            },
            ResponseHeadBoundary::InputLimit,
            Bytes::from_static(b"input"),
        ),
        (
            HoldReleaseCtx {
                max_output_bytes: 3,
                ..HoldReleaseCtx::default()
            },
            ResponseHeadBoundary::OutputLimit,
            Bytes::from_static(b"output"),
        ),
        (
            HoldReleaseCtx {
                max_work_units: 1,
                ..HoldReleaseCtx::default()
            },
            ResponseHeadBoundary::WorkLimit,
            Bytes::from_static(b"work"),
        ),
    ] {
        let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
        let mut session = request_session().await;
        let mut serve_from_cache = ServeFromCache::new();
        let mut state = ResponsePipelineState::default();
        let mut out = Vec::new();
        proxy
            .response_task_pipeline(
                ResponseProtocol::H1,
                &mut session,
                HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
                &mut ctx,
                &mut serve_from_cache,
                false,
                &mut state,
                &mut out,
            )
            .await
            .unwrap();

        let task = HttpTask::Body(Some(body), false);
        let result = if expected == ResponseHeadBoundary::InputLimit {
            proxy
                .enforce_response_head_source_input(
                    &mut session,
                    &task,
                    &mut ctx,
                    &mut state,
                    &mut out,
                )
                .await
                .map(|_| ())
        } else {
            proxy
                .response_task_pipeline(
                    ResponseProtocol::H1,
                    &mut session,
                    task,
                    &mut ctx,
                    &mut serve_from_cache,
                    false,
                    &mut state,
                    &mut out,
                )
                .await
        };
        let error = result.unwrap_err();
        assert!(!error.retry());
        assert_eq!(ctx.boundaries, vec![expected]);
        assert_eq!(ctx.outcomes.len(), 1);
        assert_eq!(ctx.outcomes[0].0, ResponseHeadOutcome::Failed(expected));
        assert!(out.is_empty());
    }
}

#[tokio::test]
async fn swallowed_work_reservation_error_still_fails_as_work_limit() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        max_work_units: 3,
        reserve_work_on_body_call: Some((1, 2)),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    let error = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"body")), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap_err();
    assert!(!error.retry());
    assert_eq!(ctx.boundaries, vec![ResponseHeadBoundary::WorkLimit]);
    assert_eq!(
        ctx.outcomes[0].0,
        ResponseHeadOutcome::Failed(ResponseHeadBoundary::WorkLimit)
    );
}

#[tokio::test]
async fn a_boundary_replacement_abandons_only_the_origin_source() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        max_input_bytes: 3,
        boundary_replacement: Some(ResponseHeadBoundary::InputLimit),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    let task = HttpTask::Body(Some(Bytes::from_static(b"oversized")), false);
    assert!(proxy
        .enforce_response_head_source_input(&mut session, &task, &mut ctx, &mut state, &mut out,)
        .await
        .unwrap());
    assert!(state.origin_abandoned);
    assert!(!state.upstream_reusable);
    assert!(matches!(
        out.first(),
        Some(HttpTask::Header(header, false)) if header.status == http::StatusCode::TOO_MANY_REQUESTS
    ));
    assert_eq!(output_body_values(&out), vec![b"bounded".as_slice()]);
    assert_eq!(ctx.boundaries, vec![ResponseHeadBoundary::InputLimit]);
    assert_eq!(ctx.outcomes[0].0, ResponseHeadOutcome::Replaced);
}

#[tokio::test]
async fn terminate_without_a_head_decision_is_typed_application_terminate() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        terminate_on_body_call: Some(1),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();
    for task in [
        HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
        HttpTask::Body(Some(Bytes::from_static(b"body")), false),
    ] {
        let result = proxy
            .response_task_pipeline(
                ResponseProtocol::H1,
                &mut session,
                task,
                &mut ctx,
                &mut serve_from_cache,
                false,
                &mut state,
                &mut out,
            )
            .await;
        if result.is_err() {
            break;
        }
    }
    assert_eq!(
        ctx.boundaries,
        vec![ResponseHeadBoundary::ApplicationTerminate]
    );
    assert_eq!(
        ctx.outcomes[0].0,
        ResponseHeadOutcome::Failed(ResponseHeadBoundary::ApplicationTerminate)
    );
    assert!(out.is_empty());
}

#[tokio::test(start_paused = true)]
async fn held_body_callback_is_cancelled_at_the_absolute_deadline() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        stall_on_body_call: Some(1),
        hold_timeout: std::time::Duration::from_millis(25),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();

    let error = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"stalled")), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("timeout"));
    assert_eq!(ctx.boundaries, vec![ResponseHeadBoundary::Timeout]);
    assert_eq!(
        ctx.outcomes[0].0,
        ResponseHeadOutcome::Failed(ResponseHeadBoundary::Timeout)
    );
    assert!(out.is_empty());
    assert!(!state.upstream_reusable);
    assert!(!state.sink.release_response_head());
    assert_eq!(ctx.downstream_body_calls, 0);
}

#[tokio::test(start_paused = true)]
async fn callback_timeout_rejects_a_boundary_replacement_in_v1() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        stall_on_body_call: Some(1),
        boundary_replacement: Some(ResponseHeadBoundary::Timeout),
        hold_timeout: std::time::Duration::from_millis(25),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    let error = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"stalled")), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("permits Fail only in v1"));
    assert!(out.is_empty());
    assert_eq!(ctx.boundaries, vec![ResponseHeadBoundary::Timeout]);
    assert_eq!(
        ctx.outcomes[0].0,
        ResponseHeadOutcome::Failed(ResponseHeadBoundary::Timeout)
    );
}

#[tokio::test]
async fn source_failure_precedes_an_exhausted_core_work_budget() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        max_work_units: 1,
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    let error = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Failed(Error::explain(InternalError, "source marker")),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("source marker"));
    assert!(ctx.boundaries.is_empty());
    assert_eq!(
        ctx.outcomes[0].0,
        ResponseHeadOutcome::Failed(ResponseHeadBoundary::SourceFailed)
    );
    assert!(out.is_empty());
}

#[tokio::test]
async fn replace_discards_the_origin_prefix_and_commits_one_bounded_response() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        replace_on_body_call: Some(1),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"origin")), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();

    assert!(
        matches!(out.first(), Some(HttpTask::Header(header, false)) if header.status == http::StatusCode::FORBIDDEN)
    );
    assert_eq!(output_body_values(&out), vec![b"blocked".as_slice()]);
    assert!(!format!("{out:?}").contains("origin"));
    assert!(state.suppress_downstream_body);
    assert!(!state.upstream_reusable);
    assert_eq!(ctx.downstream_body_calls, 1);
    assert_eq!(ctx.will_commit_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn empty_replacement_keeps_header_eos_for_h1_and_h2_pipelines() {
    for protocol in [ResponseProtocol::H1, ResponseProtocol::H2] {
        let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
        let mut session = request_session().await;
        let mut ctx = HoldReleaseCtx {
            empty_replace_on_body_call: Some(1),
            ..HoldReleaseCtx::default()
        };
        let mut serve_from_cache = ServeFromCache::new();
        let mut state = ResponsePipelineState::default();
        let mut out = Vec::new();
        for task in [
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            HttpTask::Body(Some(Bytes::from_static(b"origin")), false),
        ] {
            proxy
                .response_task_pipeline(
                    protocol,
                    &mut session,
                    task,
                    &mut ctx,
                    &mut serve_from_cache,
                    false,
                    &mut state,
                    &mut out,
                )
                .await
                .unwrap();
        }
        assert_eq!(out.len(), 1);
        assert!(matches!(
            out.first(),
            Some(HttpTask::Header(header, true))
                if header.status == http::StatusCode::FORBIDDEN
                    && !header.headers.contains_key(http::header::TRANSFER_ENCODING)
        ));
        assert!(state.origin_abandoned);
        assert_eq!(ctx.outcomes[0].0, ResponseHeadOutcome::Replaced);
    }
}

#[tokio::test]
async fn application_fail_discards_the_prefix_and_preserves_the_exact_error() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        fail_on_body_call: Some(1),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    let error = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"origin")), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("application head failure marker"));
    assert!(out.is_empty());
    assert!(!state.upstream_reusable);
    assert_eq!(ctx.downstream_body_calls, 0);
    assert_eq!(ctx.will_commit_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn held_terminal_without_release_fails_before_output() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx::default();
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    let error = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(None, true),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("clean-terminal-without-decision"));
    assert_eq!(
        ctx.boundaries,
        vec![ResponseHeadBoundary::CleanTerminalWithoutDecision]
    );
    assert!(out.is_empty());
    assert_eq!(session.prepared_response_headers, 0);
    assert_eq!(ctx.downstream_trailer_calls, 0);
}

#[tokio::test]
async fn trailer_filter_runs_only_after_the_terminal_callback_releases() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        release_on_body_call: Some(1),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    assert_eq!(ctx.downstream_trailer_calls, 0);

    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-finish", http::HeaderValue::from_static("yes"));
    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Trailer(Some(Box::new(trailers))),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();

    assert_eq!(ctx.body_calls, 1);
    assert_eq!(ctx.downstream_trailer_calls, 1);
    assert!(matches!(out.first(), Some(HttpTask::Header(..))));
    assert!(matches!(out.last(), Some(HttpTask::Trailer(Some(_)))));
}

#[tokio::test]
async fn informational_head_does_not_freeze_the_final_head_plan() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        release_on_body_call: Some(1),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(
                Box::new(ResponseHeader::build(http::StatusCode::EARLY_HINTS, None).unwrap()),
                false,
            ),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    assert_eq!(ctx.plan_calls.load(Ordering::Relaxed), 0);
    assert!(state.head_barrier.is_awaiting_final_head());
    assert!(
        matches!(out.as_slice(), [HttpTask::Header(header, false)] if header.status == http::StatusCode::EARLY_HINTS)
    );

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    assert_eq!(ctx.plan_calls.load(Ordering::Relaxed), 1);
    assert!(state.head_barrier.is_holding());
    assert_eq!(
        out.len(),
        1,
        "the final head must remain behind the barrier"
    );

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"release")), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();
    assert_eq!(ctx.plan_calls.load(Ordering::Relaxed), 1);
    assert!(
        matches!(out.get(1), Some(HttpTask::Header(header, false)) if header.status == http::StatusCode::OK)
    );
}

#[tokio::test]
async fn terminal_header_callback_can_release_the_held_head() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        release_on_body_call: Some(1),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), true),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();

    assert_eq!(ctx.body_calls, 1);
    assert_eq!(ctx.plan_calls.load(Ordering::Relaxed), 1);
    assert!(!state.head_barrier.is_holding());
    assert_eq!(session.prepared_response_headers, 1);
    assert!(
        matches!(out.first(), Some(HttpTask::Header(header, false)) if header.status == http::StatusCode::OK)
    );
    assert!(matches!(out.last(), Some(HttpTask::Body(None, true))));
}

#[tokio::test]
async fn source_failure_drops_the_held_prefix_and_preserves_the_error() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx::default();
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap();

    let marker = "held response source failure marker";
    let error = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            HttpTask::Failed(Error::explain(InternalError, marker)),
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut state,
            &mut out,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains(marker));
    assert!(
        out.is_empty(),
        "neither the held prefix nor Failed may escape"
    );
    assert_eq!(session.prepared_response_headers, 0);
}

#[tokio::test]
async fn release_preserves_held_current_and_sink_extra_order() {
    let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = HoldReleaseCtx {
        release_on_body_call: Some(2),
        emit_extra_on_body_call: Some(1),
        ..HoldReleaseCtx::default()
    };
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::new();

    for task in [
        HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
        HttpTask::Body(Some(Bytes::from_static(b"one")), false),
        HttpTask::Body(Some(Bytes::from_static(b"two")), false),
    ] {
        state.sink.reset_batch();
        proxy
            .response_task_pipeline(
                ResponseProtocol::H1,
                &mut session,
                task,
                &mut ctx,
                &mut serve_from_cache,
                false,
                &mut state,
                &mut out,
            )
            .await
            .unwrap();
    }

    assert_eq!(
        output_body_values(&out),
        vec![b"one".as_slice(), b"extra".as_slice(), b"two".as_slice()]
    );
}

#[tokio::test]
async fn cache_and_custom_hold_are_rejected_before_output() {
    for (protocol, from_cache) in [
        (ResponseProtocol::H1, true),
        (ResponseProtocol::Custom, false),
    ] {
        let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
        let mut session = request_session().await;
        let mut ctx = HoldReleaseCtx::default();
        let mut serve_from_cache = ServeFromCache::new();
        let mut state = ResponsePipelineState::default();
        let mut out = Vec::new();

        let error = proxy
            .response_task_pipeline(
                protocol,
                &mut session,
                HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), false),
                &mut ctx,
                &mut serve_from_cache,
                from_cache,
                &mut state,
                &mut out,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unsupported"));
        assert_eq!(ctx.boundaries, vec![ResponseHeadBoundary::Unsupported]);
        assert!(out.is_empty());
        assert_eq!(session.prepared_response_headers, 0);
    }
}

#[tokio::test]
async fn bodyless_status_hold_is_rejected_before_output() {
    for status in [http::StatusCode::NO_CONTENT, http::StatusCode::NOT_MODIFIED] {
        let proxy = HttpProxy::new(HoldReleaseProxy, Arc::new(ServerConf::default()));
        let mut session = request_session().await;
        let mut ctx = HoldReleaseCtx::default();
        let mut serve_from_cache = ServeFromCache::new();
        let mut state = ResponsePipelineState::default();
        let mut out = Vec::new();

        let error = proxy
            .response_task_pipeline(
                ResponseProtocol::H1,
                &mut session,
                HttpTask::Header(Box::new(ResponseHeader::build(status, None).unwrap()), true),
                &mut ctx,
                &mut serve_from_cache,
                false,
                &mut state,
                &mut out,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unsupported"));
        assert_eq!(ctx.boundaries, vec![ResponseHeadBoundary::Unsupported]);
        assert!(out.is_empty());
        assert_eq!(session.prepared_response_headers, 0);
    }
}

/// Microbenchmark the actual shared H1 per-body-task path.
///
/// Kept ignored because wall-clock output is evidence for a review, not a
/// pass/fail unit-test contract. Run in isolation with:
/// `cargo test -p pingora-proxy --release --lib response_pipeline::tests::benchmark_response_task_pipeline -- --ignored --exact --nocapture`
#[tokio::test(flavor = "current_thread")]
#[ignore = "manual response pipeline benchmark"]
async fn benchmark_response_task_pipeline() {
    const WARMUP: usize = 2_000;
    const ITERATIONS: usize = 100_000;

    let proxy = HttpProxy::new(PipelineBenchProxy, Arc::new(ServerConf::default()));
    let mut session = request_session().await;
    let mut ctx = ();
    let mut serve_from_cache = ServeFromCache::new();
    let mut state = ResponsePipelineState::default();
    let mut out = Vec::with_capacity(1);

    for _ in 0..WARMUP {
        state.sink.reset_batch();
        out.clear();
        proxy
            .response_task_pipeline(
                ResponseProtocol::H1,
                &mut session,
                HttpTask::Body(Some(Bytes::from_static(b"benchmark")), false),
                &mut ctx,
                &mut serve_from_cache,
                false,
                &mut state,
                &mut out,
            )
            .await
            .unwrap();
        black_box(out.len());
    }

    let started = Instant::now();
    for _ in 0..ITERATIONS {
        state.sink.reset_batch();
        out.clear();
        proxy
            .response_task_pipeline(
                ResponseProtocol::H1,
                &mut session,
                HttpTask::Body(Some(Bytes::from_static(b"benchmark")), false),
                &mut ctx,
                &mut serve_from_cache,
                false,
                &mut state,
                &mut out,
            )
            .await
            .unwrap();
        black_box(out.len());
    }
    let elapsed = started.elapsed();

    crate::test_allocator::start_counting();
    for _ in 0..ITERATIONS {
        state.sink.reset_batch();
        out.clear();
        proxy
            .response_task_pipeline(
                ResponseProtocol::H1,
                &mut session,
                HttpTask::Body(Some(Bytes::from_static(b"benchmark")), false),
                &mut ctx,
                &mut serve_from_cache,
                false,
                &mut state,
                &mut out,
            )
            .await
            .unwrap();
        black_box(out.len());
    }
    let allocations = crate::test_allocator::stop_counting();

    println!(
        "response_task_pipeline: {:.2} ns/task, {:.4} allocations/task",
        elapsed.as_nanos() as f64 / ITERATIONS as f64,
        allocations as f64 / ITERATIONS as f64,
    );
    assert_eq!(out.len(), 1, "one body task should remain one body task");
}
