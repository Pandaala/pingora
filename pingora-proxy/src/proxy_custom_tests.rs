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
use pingora_cache::{
    predictor::{CacheablePredictor, Predictor},
    CacheKey, MemCache,
};
use std::sync::{Arc, LazyLock};
use tokio::io::AsyncWriteExt;

static CACHE_STORAGE: LazyLock<MemCache> = LazyLock::new(MemCache::new);
static CACHE_PREDICTOR: LazyLock<Predictor<1>> = LazyLock::new(|| Predictor::new(10, None));

struct TestProxy;

#[async_trait]
impl ProxyHttp for TestProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("test calls custom_response_filter directly")
    }
}

async fn predicted_too_large_session(key: CacheKey, max_file_size: usize) -> Session {
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
        .cache
        .enable(&*CACHE_STORAGE, None, Some(&*CACHE_PREDICTOR), None, None);
    session.cache.set_cache_key(key.clone());
    session.cache.set_max_file_size_bytes(max_file_size);
    CACHE_PREDICTOR.mark_uncacheable(&key, NoCacheReason::OriginNotCache);
    session
        .cache
        .disable(NoCacheReason::PredictedResponseTooLarge);
    session
}

async fn filter_task(session: &mut Session, task: HttpTask) {
    let proxy = HttpProxy::new(TestProxy, Arc::new(ServerConf::default()));
    let mut pipeline = ResponsePipelineState::default();
    let mut out_tasks = Vec::new();
    proxy
        .response_task_pipeline(
            ResponseProtocol::Custom,
            session,
            task,
            &mut (),
            &mut ServeFromCache::new(),
            false,
            &mut pipeline,
            &mut out_tasks,
        )
        .await
        .expect("response task should pass filters");
}

#[tokio::test]
async fn completed_response_under_limit_clears_predictor() {
    let key = CacheKey::new("/custom-under-limit", "");
    let mut session = predicted_too_large_session(key.clone(), 10).await;

    filter_task(
        &mut session,
        HttpTask::Body(Some(Bytes::from_static(b"small")), true),
    )
    .await;

    assert!(CACHE_PREDICTOR.cacheable_prediction(&key));
}

#[tokio::test]
async fn completed_response_over_limit_keeps_predictor() {
    let key = CacheKey::new("/custom-over-limit", "");
    let mut session = predicted_too_large_session(key.clone(), 4).await;

    filter_task(
        &mut session,
        HttpTask::Body(Some(Bytes::from_static(b"large")), true),
    )
    .await;

    assert!(!CACHE_PREDICTOR.cacheable_prediction(&key));
}

#[tokio::test]
async fn failed_response_keeps_predictor() {
    let key = CacheKey::new("/custom-failed", "");
    let mut session = predicted_too_large_session(key.clone(), 10).await;

    filter_task(
        &mut session,
        HttpTask::Body(Some(Bytes::from_static(b"small")), false),
    )
    .await;
    filter_task(
        &mut session,
        HttpTask::Failed(Error::explain(InternalError, "test failure")),
    )
    .await;

    assert!(!CACHE_PREDICTOR.cacheable_prediction(&key));
}
