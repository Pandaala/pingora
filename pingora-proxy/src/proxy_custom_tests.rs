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

struct RejectedReplayWriter;

#[async_trait]
impl BodyWrite for RejectedReplayWriter {
    async fn write_all_buf(&mut self, data: &mut Bytes) -> Result<()> {
        assert_eq!(&data[..], b"u");
        Error::e_explain(WriteError, "custom upstream rejected replay")
    }

    async fn finish(&mut self) -> Result<()> {
        panic!("failed writer must not finish")
    }

    async fn cleanup(&mut self) -> Result<()> {
        panic!("failed writer must not be cleaned up")
    }

    fn upgrade_body_writer(&mut self) {}
}

#[tokio::test]
async fn custom_retry_upload_failure_preserves_only_complete_responses() {
    use tokio::io::AsyncReadExt;
    let proxy = HttpProxy::new(TestProxy, Arc::new(ServerConf::default()));
    for complete in [false, true] {
        let (mut client, server) = tokio::io::duplex(4096);
        client
            .write_all(b"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 1\r\n\r\nu")
            .await
            .unwrap();
        let mut session = Session::new_h1(Box::new(server));
        session.read_request().await.unwrap();
        session
            .freeze_request_relay_plan(RequestRelayPlan::ordinary())
            .unwrap();
        session.enable_request_relay_retry_buffer();
        assert_eq!(
            session.read_request_body().await.unwrap(),
            Some(Bytes::from_static(b"u"))
        );

        let (tx, rx) = mpsc::channel(TASK_BUFFER_SIZE);
        let mut header = ResponseHeader::build(413, None).unwrap();
        header.insert_header("content-length", "2").unwrap();
        tx.send(HttpTask::Header(Box::new(header), false))
            .await
            .unwrap();
        if complete {
            tx.send(HttpTask::Body(Some(Bytes::from_static(b"no")), true))
                .await
                .unwrap();
            tx.send(HttpTask::Done).await.unwrap();
        }
        drop(tx);

        let upstream_messages = mpsc::channel(1).1;
        let downstream_messages = mpsc::channel(1).1;
        let mut error = None;
        let mut writer: Box<dyn BodyWrite> = Box::new(RejectedReplayWriter);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            proxy.custom_bidirection_down_to_up(
                &mut session,
                &mut writer,
                rx,
                &mut (),
                upstream_messages,
                downstream_messages,
                true,
                oneshot::channel().0,
                Arc::new(AtomicU8::new(PipeState::Active as u8)),
                &mut error,
            ),
        )
        .await
        .expect("closed response pipe must resolve a failed replay attempt");
        if complete {
            assert!(matches!(
                result,
                Ok(DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(_))
            ));
            assert!(error.is_none());
            drop(session);
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert!(response.starts_with(b"HTTP/1.1 413 "));
            assert!(response.ends_with(b"\r\n\r\nno"));
        } else {
            assert!(result.is_err() || error.is_some());
            let error = error.or_else(|| result.err()).unwrap();
            assert!(
                error
                    .to_string()
                    .contains("custom upstream rejected replay"),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn complete_downstream_framing_cannot_finish_an_active_custom_cache_fill() {
    use pingora_cache::CacheMeta;
    use std::time::SystemTime;

    let (mut client, server) = tokio::io::duplex(1024);
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .unwrap();
    let mut session = Session::new_h1(Box::new(server));
    session.read_request().await.unwrap();
    let mut header = ResponseHeader::build(200, None).unwrap();
    header.insert_header("content-length", "0").unwrap();
    session
        .write_response_header(Box::new(header.clone()), true)
        .await
        .unwrap();

    session
        .cache
        .enable(&*CACHE_STORAGE, None, None, None, None);
    session
        .cache
        .set_cache_key(CacheKey::new("/custom-framed-fill", ""));
    session.cache.cache_miss();
    assert!(upload_response_complete(&mut session, false, false, false));

    let now = SystemTime::now();
    session.cache.set_cache_meta(CacheMeta::new(
        now + Duration::from_secs(60),
        now,
        0,
        0,
        header,
    ));
    session.cache.set_miss_handler().await.unwrap();
    assert!(!upload_response_complete(&mut session, false, false, false));
    assert!(upload_response_complete(&mut session, true, false, false));
}
