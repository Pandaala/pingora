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
use pingora_cache::lock::{CacheLock, LockWaitOutcome};
use pingora_cache::trace::SpanHandle;
use pingora_cache::{
    CacheKey, CacheMeta, CachePhase, HitHandler, MemCache, MissHandler, PurgeOutcome, PurgeTarget,
    PurgeType, RespCacheable, Storage,
};
use std::any::Any;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::SystemTime;
use tokio::io::AsyncWriteExt;

struct ResponseFilter101;

struct TerminateBodyFilter;

struct NonStreamingMemCache(MemCache);

static CACHE_STORAGE: LazyLock<MemCache> = LazyLock::new(MemCache::new);
static NON_STREAMING_CACHE_STORAGE: LazyLock<NonStreamingMemCache> =
    LazyLock::new(|| NonStreamingMemCache(MemCache::new()));
static CACHE_LOCK: LazyLock<CacheLock> = LazyLock::new(|| CacheLock::new(Duration::from_secs(2)));

#[async_trait]
impl Storage for NonStreamingMemCache {
    async fn lookup(
        &'static self,
        key: &CacheKey,
        trace: &SpanHandle,
    ) -> Result<Option<(CacheMeta, HitHandler)>> {
        self.0.lookup(key, trace).await
    }

    async fn get_miss_handler(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        trace: &SpanHandle,
    ) -> Result<MissHandler> {
        self.0.get_miss_handler(key, meta, trace).await
    }

    async fn purge(
        &'static self,
        target: PurgeTarget<'_>,
        purge_type: PurgeType,
        trace: &SpanHandle,
    ) -> Result<PurgeOutcome> {
        self.0.purge(target, purge_type, trace).await
    }

    async fn update_meta(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        trace: &SpanHandle,
    ) -> Result<bool> {
        self.0.update_meta(key, meta, trace).await
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static) {
        self
    }
}

#[async_trait]
impl ProxyHttp for ResponseFilter101 {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("test calls h1_response_filter directly")
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        response.set_status(http::StatusCode::SWITCHING_PROTOCOLS)?;
        response.set_version(Version::HTTP_11);
        Ok(())
    }
}

#[async_trait]
impl ProxyHttp for TerminateBodyFilter {
    type CTX = usize;

    fn new_ctx(&self) -> Self::CTX {
        0
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("test calls process_upstream_tasks directly")
    }

    async fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        sink: &mut ResponseBodySink,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        if body.is_some() {
            *ctx += 1;
            sink.terminate();
        }
        Ok(None)
    }

    fn response_cache_filter(
        &self,
        _session: &Session,
        response: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        let now = SystemTime::now();
        Ok(RespCacheable::Cacheable(CacheMeta::new(
            now + Duration::from_secs(60),
            now,
            0,
            0,
            response.clone(),
        )))
    }
}

async fn upgrade_request_session() -> Session {
    let (mut client, server) = tokio::io::duplex(1024);
    client
        .write_all(
            b"GET / HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
        )
        .await
        .expect("test request should be written");

    let mut session = Session::new_h1(Box::new(server) as pingora_core::protocols::Stream);
    session
        .read_request()
        .await
        .expect("test request should parse");
    session
}

async fn request_session() -> (Session, tokio::io::DuplexStream) {
    let (mut client, server) = tokio::io::duplex(4096);
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .expect("test request should be written");

    let mut session = Session::new_h1(Box::new(server) as pingora_core::protocols::Stream);
    session
        .read_request()
        .await
        .expect("test request should parse");
    (session, client)
}

async fn response_body_session() -> (Session, tokio::io::DuplexStream) {
    let (mut session, client) = request_session().await;
    let mut response = ResponseHeader::build(200, None).unwrap();
    response
        .insert_header(header::TRANSFER_ENCODING, "chunked")
        .unwrap();
    session
        .write_response_header(Box::new(response), false)
        .await
        .unwrap();
    (session, client)
}

async fn run_terminating_batch(
    trailing_tasks: Vec<HttpTask>,
    cache_enabled: bool,
) -> (bool, bool, bool, CachePhase, usize) {
    let proxy = HttpProxy::new(TerminateBodyFilter, Arc::new(ServerConf::default()));
    let (mut session, _client) = response_body_session().await;
    if cache_enabled {
        session
            .cache
            .enable(&*CACHE_STORAGE, None, None, None, None);
        session
            .cache
            .set_cache_key(CacheKey::new("h1-terminating-batch", ""));
        session.cache.bypass();
    }
    let (tx, mut rx) = mpsc::channel(TASK_BUFFER_SIZE);
    for task in trailing_tasks {
        tx.try_send(task).unwrap();
    }

    let mut ctx = 0;
    let mut serve_from_cache = ServeFromCache::new();
    let mut response_state = ResponseStateMachine::new();
    let mut response_pipeline = ResponsePipelineState::default();
    let outcome = proxy
        .process_upstream_tasks(
            &mut session,
            &mut ctx,
            HttpTask::Body(Some(Bytes::from_static(b"body")), false),
            &mut rx,
            &mut serve_from_cache,
            &mut response_state,
            &mut response_pipeline,
        )
        .await
        .unwrap()
        .unwrap();
    let ResponseTaskBatchOutcome::Progress {
        source_done,
        terminated,
    } = outcome
    else {
        panic!("terminate test must not abandon the origin")
    };

    (
        source_done,
        terminated,
        response_pipeline.upstream_reusable,
        session.cache.phase(),
        ctx,
    )
}

#[tokio::test]
async fn h1_terminate_ignores_unprocessed_trailer_and_done_for_completion() {
    let (source_done, terminated, upstream_reusable, _, filter_calls) =
        run_terminating_batch(vec![HttpTask::Trailer(None), HttpTask::Done], false).await;

    assert!(
        !source_done,
        "unprocessed terminal tasks are not completion"
    );
    assert!(
        terminated,
        "the pump must return the typed terminate outcome"
    );
    assert!(upstream_reusable, "no upstream failure was discarded");
    assert_eq!(
        filter_calls, 1,
        "the trailer and Done must stay unprocessed"
    );
}

#[tokio::test]
async fn h1_terminate_aborts_cache_and_reuse_for_unprocessed_failure() {
    let failure = Error::explain(ReadError, "queued upstream failure").into_up();
    let (source_done, terminated, upstream_reusable, cache_phase, filter_calls) =
        run_terminating_batch(vec![HttpTask::Failed(failure)], true).await;

    assert!(!source_done, "an unprocessed failure is not completion");
    assert!(
        terminated,
        "the pump must return the typed terminate outcome"
    );
    assert!(!upstream_reusable, "a failed H1 upstream cannot be pooled");
    assert!(matches!(
        cache_phase,
        CachePhase::Disabled(NoCacheReason::UpstreamError)
    ));
    assert_eq!(filter_calls, 1);
}

#[tokio::test]
async fn h1_batched_terminate_releases_real_cache_miss_lock() {
    let proxy = HttpProxy::new(TerminateBodyFilter, Arc::new(ServerConf::default()));
    let (mut session, _client) = request_session().await;
    let key = CacheKey::new("h1-batched-terminate-real-lock", "");
    session.cache.enable(
        &*NON_STREAMING_CACHE_STORAGE,
        None,
        None,
        Some(&*CACHE_LOCK),
        None,
    );
    session.cache.set_cache_key(key.clone());
    assert!(session.cache.cache_lookup().await.unwrap().is_none());
    assert!(
        !session.cache.is_cache_locked(),
        "first lookup owns the write lock"
    );
    session.cache.cache_miss();

    let mut reader = pingora_cache::HttpCache::new();
    reader.enable(
        &*NON_STREAMING_CACHE_STORAGE,
        None,
        None,
        Some(&*CACHE_LOCK),
        None,
    );
    reader.set_cache_key(key.clone());
    assert!(reader.cache_lookup().await.unwrap().is_none());
    assert!(
        reader.is_cache_locked(),
        "second lookup must wait for the writer"
    );
    let waiting = tokio::spawn(async move { reader.cache_lock_wait().await });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished(), "reader should still be waiting");

    let (request_tx, _request_rx) = mpsc::channel(TASK_BUFFER_SIZE);
    let (response_tx, response_rx) = mpsc::channel(TASK_BUFFER_SIZE);
    let response = ResponseHeader::build(200, None).unwrap();
    response_tx
        .try_send(HttpTask::Header(Box::new(response), false))
        .unwrap();
    response_tx
        .try_send(HttpTask::Body(Some(Bytes::from_static(b"partial")), false))
        .unwrap();
    response_tx.try_send(HttpTask::Trailer(None)).unwrap();
    response_tx.try_send(HttpTask::Done).unwrap();

    let mut ctx = 0;
    let mut custom_writer = None;
    let mut custom_reader = None;
    let mut response_pipeline = ResponsePipelineState::default();
    let outcome = proxy
        .proxy_handle_downstream(
            &mut session,
            request_tx,
            response_rx,
            &mut ctx,
            &mut custom_writer,
            &mut custom_reader,
            Arc::new(AtomicU8::new(PipeState::Active as u8)),
            UpstreamRequestBodyDisposition::Ordinary,
            &mut response_pipeline,
        )
        .await
        .unwrap();
    assert_eq!(outcome, DownstreamRequestOutcome::Terminate);

    release_cache_on_terminate(&mut session);
    assert_eq!(
        session.cache.phase(),
        CachePhase::Disabled(NoCacheReason::InternalError)
    );
    assert_eq!(waiting.await.unwrap(), LockWaitOutcome::TransientError);

    let mut next = pingora_cache::HttpCache::new();
    next.enable(
        &*NON_STREAMING_CACHE_STORAGE,
        None,
        None,
        Some(&*CACHE_LOCK),
        None,
    );
    next.set_cache_key(key);
    assert!(
        next.cache_lookup().await.unwrap().is_none(),
        "the partial response must not become a cache hit"
    );
    next.disable(NoCacheReason::InternalError);
}

#[tokio::test]
async fn h1_response_filter_rejects_filter_created_101_with_upgrade_mismatch() {
    let proxy = HttpProxy::new(ResponseFilter101, Arc::new(ServerConf::default()));
    let mut session = upgrade_request_session().await;
    session.h1_upgrade_request_status = H1UpgradeRequestStatus {
        upstream: Some(false),
    };

    let mut ctx = ();
    let mut serve_from_cache = ServeFromCache::new();
    let mut pipeline = ResponsePipelineState::default();
    let mut out_tasks = Vec::new();
    let task = HttpTask::Header(Box::new(ResponseHeader::build(200, Some(0)).unwrap()), true);

    let err = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            task,
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut pipeline,
            &mut out_tasks,
        )
        .await
        .unwrap_err();

    assert_eq!(err.etype(), &InvalidHTTPHeader);
    assert_eq!(err.esource(), &ErrorSource::Internal);
}

#[tokio::test]
async fn h1_response_filter_rejects_upstream_101_upgrade_mismatch_as_upstream() {
    let proxy = HttpProxy::new(ResponseFilter101, Arc::new(ServerConf::default()));
    let mut session = upgrade_request_session().await;
    session.h1_upgrade_request_status = H1UpgradeRequestStatus {
        upstream: Some(false),
    };

    let mut ctx = ();
    let mut serve_from_cache = ServeFromCache::new();
    let mut pipeline = ResponsePipelineState::default();
    let mut out_tasks = Vec::new();
    let mut response =
        ResponseHeader::build(http::StatusCode::SWITCHING_PROTOCOLS, Some(0)).unwrap();
    response.set_version(Version::HTTP_11);
    let task = HttpTask::Header(Box::new(response), true);

    let err = proxy
        .response_task_pipeline(
            ResponseProtocol::H1,
            &mut session,
            task,
            &mut ctx,
            &mut serve_from_cache,
            false,
            &mut pipeline,
            &mut out_tasks,
        )
        .await
        .unwrap_err();

    assert_eq!(err.etype(), &InvalidHTTPHeader);
    assert_eq!(err.esource(), &ErrorSource::Upstream);
}

fn request_with_framing() -> RequestHeader {
    let mut request = RequestHeader::build("POST", b"/", None).unwrap();
    request.insert_header(header::CONTENT_LENGTH, "12").unwrap();
    request
        .insert_header(header::TRANSFER_ENCODING, "gzip")
        .unwrap();
    request
}

#[test]
fn streamed_disposition_uses_h1_chunked_framing() {
    let mut request = request_with_framing();
    apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed)
        .unwrap();

    assert!(request.headers.get(header::CONTENT_LENGTH).is_none());
    // `gzip` is a content coding applied to the bytes being forwarded and
    // must survive the re-framing; only `chunked` is re-derived.
    assert_eq!(
        request.headers.get(header::TRANSFER_ENCODING).unwrap(),
        "gzip, chunked"
    );
}

#[test]
fn streamed_disposition_preserves_non_chunked_transfer_codings() {
    // Already `gzip, chunked`: must round-trip unchanged, not collapse to
    // bare `chunked` (which would erase the gzip coding while the body
    // bytes stay gzip-coded).
    let mut request = RequestHeader::build("POST", b"/", None).unwrap();
    request
        .insert_header(header::TRANSFER_ENCODING, "gzip, chunked")
        .unwrap();
    apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed)
        .unwrap();
    assert_eq!(
        request.headers.get(header::TRANSFER_ENCODING).unwrap(),
        "gzip, chunked"
    );

    // Multiple header lines, mixed case, and a redundant `chunked`.
    let mut request = RequestHeader::build("POST", b"/", None).unwrap();
    request
        .append_header(header::TRANSFER_ENCODING, "deflate")
        .unwrap();
    request
        .append_header(header::TRANSFER_ENCODING, "gzip, Chunked")
        .unwrap();
    apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed)
        .unwrap();
    assert_eq!(
        request.headers.get(header::TRANSFER_ENCODING).unwrap(),
        "deflate, gzip, chunked"
    );

    // Nothing to preserve: bare `chunked`.
    let mut request = RequestHeader::build("POST", b"/", None).unwrap();
    request.insert_header(header::CONTENT_LENGTH, "12").unwrap();
    apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed)
        .unwrap();
    assert_eq!(
        request.headers.get(header::TRANSFER_ENCODING).unwrap(),
        "chunked"
    );
}

#[test]
fn bodyless_disposition_removes_request_framing() {
    let mut request = request_with_framing();
    apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Bodyless)
        .unwrap();

    assert!(request.headers.get(header::CONTENT_LENGTH).is_none());
    assert!(request.headers.get(header::TRANSFER_ENCODING).is_none());
}

#[test]
fn ordinary_disposition_preserves_request_framing() {
    let mut request = request_with_framing();
    apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Ordinary)
        .unwrap();

    assert_eq!(request.headers.get(header::CONTENT_LENGTH).unwrap(), "12");
    assert_eq!(
        request.headers.get(header::TRANSFER_ENCODING).unwrap(),
        "gzip"
    );
}
