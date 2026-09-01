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
use async_trait::async_trait;
use bytes::Bytes;
use pingora_core::{server::configuration::ServerConf, upstreams::peer::HttpPeer};
use pingora_error::{Error, ErrorType::InternalError};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;

struct ParityProxy;
struct PipelineBenchProxy;

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
            "upstream-body",
            "downstream-body",
            "downstream-body",
            "upstream-terminal-before-trailers",
            "upstream-trailer",
            "downstream-trailer",
            "downstream-body",
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
        vec!["downstream-header", "downstream-body", "downstream-body"]
    );
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
