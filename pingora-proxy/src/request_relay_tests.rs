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
use crate::{RequestRelayRetryState, UpstreamRequestBodyDisposition};
use async_trait::async_trait;
use pingora_core::modules::http::{HttpModule, HttpModuleBuilder, HttpModules, Module};
use pingora_core::protocols::http::InMemoryRequestBodyBuffer;
use pingora_core::protocols::Stream;
use pingora_core::server::configuration::ServerConf;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::ErrorType::InternalError;
use std::any::Any;
use std::future::pending;
use std::hint::black_box;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HookBehavior {
    Continue,
    Terminate,
    Error,
    Pending,
}

struct RelayProbe {
    body: HookBehavior,
    trailer: HookBehavior,
    log: Arc<Mutex<Vec<&'static str>>>,
}

struct RelayBenchmarkProxy;

#[async_trait]
impl ProxyHttp for RelayBenchmarkProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("request relay benchmark does not select an upstream")
    }
}

#[async_trait]
impl ProxyHttp for RelayProbe {
    type CTX = Vec<RequestBodyEvent>;

    fn new_ctx(&self) -> Self::CTX {
        Vec::new()
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("request relay tests do not select an upstream")
    }

    async fn request_body_filter_action(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        event: RequestBodyEvent,
        ctx: &mut Self::CTX,
    ) -> Result<RequestBodyAction> {
        self.log.lock().unwrap().push("application");
        ctx.push(event);
        if body.as_deref() == Some(b"module") {
            *body = Some(Bytes::from_static(b"application"));
        }
        match self.body {
            HookBehavior::Continue => Ok(RequestBodyAction::Continue),
            HookBehavior::Terminate => Ok(RequestBodyAction::Terminate),
            HookBehavior::Error => Error::e_explain(InternalError, "body hook failed"),
            HookBehavior::Pending => pending().await,
        }
    }

    async fn request_trailer_filter(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<RequestBodyAction> {
        self.log.lock().unwrap().push("trailer");
        match self.trailer {
            HookBehavior::Continue => Ok(RequestBodyAction::Continue),
            HookBehavior::Terminate => Ok(RequestBodyAction::Terminate),
            HookBehavior::Error => Error::e_explain(InternalError, "trailer hook failed"),
            HookBehavior::Pending => pending().await,
        }
    }
}

struct MutatingModule {
    log: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl HttpModule for MutatingModule {
    async fn request_body_filter(
        &mut self,
        body: &mut Option<Bytes>,
        _event: RequestBodyEvent,
    ) -> Result<()> {
        self.log.lock().unwrap().push("module");
        *body = Some(Bytes::from_static(b"module"));
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct MutatingModuleBuilder {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl HttpModuleBuilder for MutatingModuleBuilder {
    fn init(&self) -> Module {
        Box::new(MutatingModule {
            log: Arc::clone(&self.log),
        })
    }
}

fn make_proxy(
    body: HookBehavior,
    trailer: HookBehavior,
    log: Arc<Mutex<Vec<&'static str>>>,
) -> HttpProxy<RelayProbe> {
    HttpProxy::new(
        RelayProbe { body, trailer, log },
        Arc::new(ServerConf::default()),
    )
}

async fn request_session(raw: &'static [u8]) -> (Session, tokio::io::DuplexStream) {
    let (mut client, server) = tokio::io::duplex(4096);
    client.write_all(raw).await.unwrap();
    let mut session = Session::new_h1(Box::new(server) as Stream);
    assert!(session.read_request().await.unwrap());
    (session, client)
}

async fn empty_request_session() -> (Session, tokio::io::DuplexStream) {
    request_session(b"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 0\r\n\r\n").await
}

async fn trailer_request_session() -> (Session, tokio::io::DuplexStream) {
    let (mut session, client) = request_session(
        b"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n\
          0\r\nX-Test-Trailer: present\r\n\r\n",
    )
    .await;
    while session.read_request_body().await.unwrap().is_some() {}
    assert_eq!(
        session.downstream_session.request_trailers_present(),
        Some(true)
    );
    (session, client)
}

#[tokio::test]
async fn normalizes_source_eof_and_preserves_protocol_neutral_events() {
    for protocol in [
        RequestRelayProtocol::H1,
        RequestRelayProtocol::H2,
        RequestRelayProtocol::Custom,
    ] {
        let log = Arc::new(Mutex::new(Vec::new()));
        let proxy = make_proxy(HookBehavior::Continue, HookBehavior::Continue, log);
        let (mut session, _client) = empty_request_session().await;
        let mut ctx = Vec::new();

        let outcome = proxy
            .request_relay_event(
                protocol,
                &mut session,
                None,
                RequestBodyEvent::Data,
                &mut ctx,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome,
            RequestRelayOutcome::Continue(PreparedRequestEvent {
                body: None,
                event: RequestBodyEvent::Complete,
            })
        );
        assert_eq!(ctx, vec![RequestBodyEvent::Complete]);
    }
}

#[tokio::test]
async fn keeps_module_before_application_and_returns_mutated_bytes() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let proxy = make_proxy(
        HookBehavior::Continue,
        HookBehavior::Continue,
        Arc::clone(&log),
    );
    let (mut session, _client) = empty_request_session().await;
    let mut modules = HttpModules::new();
    modules.add_module(Box::new(MutatingModuleBuilder {
        log: Arc::clone(&log),
    }));
    session.downstream_modules_ctx = modules.build_ctx();

    let original = Bytes::from_static(b"original");
    let mut ctx = Vec::new();
    let outcome = proxy
        .request_relay_event(
            RequestRelayProtocol::H1,
            &mut session,
            Some(original),
            RequestBodyEvent::Data,
            &mut ctx,
        )
        .await
        .unwrap();

    assert_eq!(&*log.lock().unwrap(), &["module", "application"]);
    assert_eq!(
        outcome,
        RequestRelayOutcome::Continue(PreparedRequestEvent {
            body: Some(Bytes::from_static(b"application")),
            event: RequestBodyEvent::Data,
        })
    );
}

#[tokio::test]
async fn default_path_moves_the_same_bytes_without_copying_payload() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let proxy = make_proxy(HookBehavior::Continue, HookBehavior::Continue, log);
    let (mut session, _client) = empty_request_session().await;
    let body = Bytes::from_static(b"identity");
    let ptr = body.as_ptr();
    let mut ctx = Vec::new();

    let RequestRelayOutcome::Continue(prepared) = proxy
        .request_relay_event(
            RequestRelayProtocol::H2,
            &mut session,
            Some(body),
            RequestBodyEvent::Data,
            &mut ctx,
        )
        .await
        .unwrap()
    else {
        panic!("default hook must continue")
    };

    assert_eq!(prepared.body.as_ref().unwrap().as_ptr(), ptr);
}

#[tokio::test]
async fn h1_and_h2_return_typed_body_termination_but_custom_fails_closed() {
    for protocol in [RequestRelayProtocol::H1, RequestRelayProtocol::H2] {
        let log = Arc::new(Mutex::new(Vec::new()));
        let proxy = make_proxy(HookBehavior::Terminate, HookBehavior::Continue, log);
        let (mut session, _client) = empty_request_session().await;
        let outcome = proxy
            .request_relay_event(
                protocol,
                &mut session,
                Some(Bytes::from_static(b"body")),
                RequestBodyEvent::Data,
                &mut Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RequestRelayOutcome::Terminate(RequestTerminationOrigin::BodyFilter)
        );
    }

    let log = Arc::new(Mutex::new(Vec::new()));
    let proxy = make_proxy(HookBehavior::Terminate, HookBehavior::Continue, log);
    let (mut session, _client) = empty_request_session().await;
    let error = proxy
        .request_relay_event(
            RequestRelayProtocol::Custom,
            &mut session,
            Some(Bytes::from_static(b"body")),
            RequestBodyEvent::Data,
            &mut Vec::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.etype, InternalError);
    assert!(error.to_string().contains("terminate is not supported"));
}

#[tokio::test]
async fn body_hook_error_propagates_for_every_protocol() {
    for protocol in [
        RequestRelayProtocol::H1,
        RequestRelayProtocol::H2,
        RequestRelayProtocol::Custom,
    ] {
        let log = Arc::new(Mutex::new(Vec::new()));
        let proxy = make_proxy(HookBehavior::Error, HookBehavior::Continue, log);
        let (mut session, _client) = empty_request_session().await;
        let error = proxy
            .request_relay_event(
                protocol,
                &mut session,
                Some(Bytes::from_static(b"body")),
                RequestBodyEvent::Data,
                &mut Vec::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.etype, InternalError);
        assert!(error.to_string().contains("body hook failed"));
    }
}

#[tokio::test]
async fn trailer_success_latches_after_hook_and_precedes_body_filter() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let proxy = make_proxy(
        HookBehavior::Continue,
        HookBehavior::Continue,
        Arc::clone(&log),
    );
    let (mut session, _client) = trailer_request_session().await;
    let mut ctx = Vec::new();

    for _ in 0..2 {
        let outcome = proxy
            .request_relay_event(
                RequestRelayProtocol::H1,
                &mut session,
                None,
                RequestBodyEvent::Complete,
                &mut ctx,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, RequestRelayOutcome::Continue(_)));
    }

    assert!(session.request_trailer_filter_fired);
    assert_eq!(
        &*log.lock().unwrap(),
        &["trailer", "application", "application"]
    );
}

#[tokio::test]
async fn trailer_terminate_skips_body_filter_and_custom_preserves_no_trailer_hook() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let proxy = make_proxy(
        HookBehavior::Continue,
        HookBehavior::Terminate,
        Arc::clone(&log),
    );
    let (mut session, _client) = trailer_request_session().await;
    let outcome = proxy
        .request_relay_event(
            RequestRelayProtocol::H2,
            &mut session,
            None,
            RequestBodyEvent::Complete,
            &mut Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RequestRelayOutcome::Terminate(RequestTerminationOrigin::TrailerFilter)
    );
    assert_eq!(&*log.lock().unwrap(), &["trailer"]);

    let log = Arc::new(Mutex::new(Vec::new()));
    let proxy = make_proxy(
        HookBehavior::Continue,
        HookBehavior::Terminate,
        Arc::clone(&log),
    );
    let (mut session, _client) = trailer_request_session().await;
    let outcome = proxy
        .request_relay_event(
            RequestRelayProtocol::Custom,
            &mut session,
            None,
            RequestBodyEvent::Complete,
            &mut Vec::new(),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, RequestRelayOutcome::Continue(_)));
    assert!(!session.request_trailer_filter_fired);
    assert_eq!(&*log.lock().unwrap(), &["application"]);
}

#[tokio::test]
async fn trailer_error_or_cancellation_does_not_commit_the_latch() {
    for behavior in [HookBehavior::Error, HookBehavior::Pending] {
        let log = Arc::new(Mutex::new(Vec::new()));
        let proxy = make_proxy(HookBehavior::Continue, behavior, log);
        let (mut session, _client) = trailer_request_session().await;
        let mut ctx = Vec::new();
        let future = proxy.request_relay_event(
            RequestRelayProtocol::H1,
            &mut session,
            None,
            RequestBodyEvent::Complete,
            &mut ctx,
        );

        if behavior == HookBehavior::Pending {
            assert!(tokio::time::timeout(Duration::from_millis(10), future)
                .await
                .is_err());
        } else {
            assert!(future.await.is_err());
        }
        assert!(!session.request_trailer_filter_fired);
    }
}

#[tokio::test]
async fn abandoned_remains_distinct_from_clean_completion() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let proxy = make_proxy(HookBehavior::Continue, HookBehavior::Continue, log);
    let (mut session, _client) = empty_request_session().await;
    let mut ctx = Vec::new();
    let outcome = proxy
        .request_relay_event(
            RequestRelayProtocol::H1,
            &mut session,
            None,
            RequestBodyEvent::Abandoned,
            &mut ctx,
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RequestRelayOutcome::Continue(PreparedRequestEvent {
            body: None,
            event: RequestBodyEvent::Abandoned,
        })
    );
    assert_eq!(ctx, vec![RequestBodyEvent::Abandoned]);
}

#[tokio::test]
async fn frozen_plan_locks_source_and_reports_registered_replay() {
    let (mut session, _client) =
        request_session(b"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 1\r\n\r\nx")
            .await;
    session
        .downstream_session
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap();
    while session.read_request_body().await.unwrap().is_some() {}

    session
        .freeze_request_relay_plan(RequestRelayPlan::ordinary())
        .unwrap();
    assert_eq!(
        session.request_relay_retry_state(),
        RequestRelayRetryState::RegisteredReplay
    );
    assert_eq!(
        session.request_relay_plan(),
        Some(RequestRelayPlan::ordinary())
    );

    let error = session
        .downstream_session
        .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
        .unwrap_err();
    assert!(error.to_string().contains("configuration is frozen"));
    assert!(session
        .freeze_request_relay_plan(RequestRelayPlan::ordinary())
        .is_err());
}

#[tokio::test]
async fn frozen_replay_policy_controls_native_backing_state() {
    let (mut session, _client) =
        request_session(b"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 1\r\n\r\nx")
            .await;
    session
        .freeze_request_relay_plan(RequestRelayPlan::ordinary())
        .unwrap();
    assert_eq!(
        session.request_relay_retry_state(),
        RequestRelayRetryState::LiveUnread
    );
    session.enable_request_relay_retry_buffer();
    assert_eq!(
        session.request_relay_retry_state(),
        RequestRelayRetryState::NativeCapturing
    );

    let (mut streamed, _client) =
        request_session(b"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 1\r\n\r\nx")
            .await;
    streamed
        .freeze_request_relay_plan(RequestRelayPlan::streamed())
        .unwrap();
    streamed.enable_request_relay_retry_buffer();
    assert_eq!(
        streamed.request_relay_retry_state(),
        RequestRelayRetryState::Disabled
    );
    assert!(streamed.downstream_session.get_retry_buffer().is_none());
}

#[tokio::test]
async fn streamed_plan_cannot_claim_replayability() {
    let (mut session, _client) = empty_request_session().await;
    let error = session
        .freeze_request_relay_plan(RequestRelayPlan {
            disposition: UpstreamRequestBodyDisposition::Streamed,
            replay: RequestReplayPolicy::Replayable,
        })
        .unwrap_err();
    assert!(error.to_string().contains("must disable replay"));
}

#[test]
fn request_attempt_ids_are_one_based_and_monotonic() {
    let io = tokio_test::io::Builder::new().build();
    let mut session = Session::new_h1(Box::new(io));
    assert_eq!(session.request_attempt_id(), None);
    session.begin_request_relay_attempt(1);
    assert_eq!(session.request_attempt_id().unwrap().get(), 1);
    session.begin_request_relay_attempt(2);
    assert_eq!(session.request_attempt_id().unwrap().get(), 2);
}

#[test]
fn unsupported_custom_downstream_never_calls_retry_buffer_placeholders() {
    let modules = HttpModules::new();
    let downstream = pingora_core::protocols::http::ServerSession::new_custom(Box::new(()));
    let mut session = Session::new(
        downstream,
        &modules,
        #[cfg(feature = "upstream_modules")]
        &HttpModules::new(),
        Arc::new(AtomicBool::new(false)),
    );
    session
        .freeze_request_relay_plan(RequestRelayPlan::ordinary())
        .unwrap();
    session.enable_request_relay_retry_buffer();

    assert_eq!(
        session.request_relay_retry_state(),
        RequestRelayRetryState::Unsupported
    );
    assert!(session.request_relay_retry_buffer().is_none());
}

async fn legacy_data_event(
    proxy: &HttpProxy<RelayBenchmarkProxy>,
    session: &mut Session,
    mut body: Option<Bytes>,
    ctx: &mut (),
) -> Result<Option<Bytes>> {
    session
        .downstream_modules_ctx
        .request_body_filter(&mut body, RequestBodyEvent::Data)
        .await?;
    let action = proxy
        .inner
        .request_body_filter_action(session, &mut body, RequestBodyEvent::Data, ctx)
        .await?;
    assert_eq!(action, RequestBodyAction::Continue);
    Ok(body)
}

/// Compare the extracted relay with the exact pre-extraction Data-event
/// sequence. Kept ignored because wall-clock output is review evidence, not a
/// stable unit-test contract. Run in isolation with:
/// `cargo test -p pingora-proxy --release --lib request_relay::tests::benchmark_request_relay_data_event -- --ignored --exact --nocapture`
#[tokio::test(flavor = "current_thread")]
#[ignore = "manual request relay benchmark"]
async fn benchmark_request_relay_data_event() {
    const WARMUP: usize = 2_000;
    const ITERATIONS: usize = 100_000;

    let proxy = HttpProxy::new(RelayBenchmarkProxy, Arc::new(ServerConf::default()));
    let (mut legacy_session, _legacy_client) = empty_request_session().await;
    let (mut relay_session, _relay_client) = empty_request_session().await;
    let mut ctx = ();

    for _ in 0..WARMUP {
        black_box(
            legacy_data_event(
                &proxy,
                &mut legacy_session,
                Some(Bytes::from_static(b"benchmark")),
                &mut ctx,
            )
            .await
            .unwrap(),
        );
        black_box(
            proxy
                .request_relay_event(
                    RequestRelayProtocol::H1,
                    &mut relay_session,
                    Some(Bytes::from_static(b"benchmark")),
                    RequestBodyEvent::Data,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }

    let legacy_started = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(
            legacy_data_event(
                &proxy,
                &mut legacy_session,
                Some(Bytes::from_static(b"benchmark")),
                &mut ctx,
            )
            .await
            .unwrap(),
        );
    }
    let legacy_elapsed = legacy_started.elapsed();

    let relay_started = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(
            proxy
                .request_relay_event(
                    RequestRelayProtocol::H1,
                    &mut relay_session,
                    Some(Bytes::from_static(b"benchmark")),
                    RequestBodyEvent::Data,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }
    let relay_elapsed = relay_started.elapsed();

    crate::test_allocator::start_counting();
    for _ in 0..ITERATIONS {
        black_box(
            legacy_data_event(
                &proxy,
                &mut legacy_session,
                Some(Bytes::from_static(b"benchmark")),
                &mut ctx,
            )
            .await
            .unwrap(),
        );
    }
    let legacy_allocations = crate::test_allocator::stop_counting();

    crate::test_allocator::start_counting();
    for _ in 0..ITERATIONS {
        black_box(
            proxy
                .request_relay_event(
                    RequestRelayProtocol::H1,
                    &mut relay_session,
                    Some(Bytes::from_static(b"benchmark")),
                    RequestBodyEvent::Data,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }
    let relay_allocations = crate::test_allocator::stop_counting();

    println!(
        "request Data event: legacy {:.2} ns/event and {:.4} allocations/event; \
         relay {:.2} ns/event and {:.4} allocations/event",
        legacy_elapsed.as_nanos() as f64 / ITERATIONS as f64,
        legacy_allocations as f64 / ITERATIONS as f64,
        relay_elapsed.as_nanos() as f64 / ITERATIONS as f64,
        relay_allocations as f64 / ITERATIONS as f64,
    );
    assert_eq!(
        relay_allocations, legacy_allocations,
        "the relay must not add per-event allocation"
    );
}
