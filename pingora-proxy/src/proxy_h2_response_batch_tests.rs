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
use pingora_cache::{CacheKey, CachePhase, MemCache};
use std::sync::{Arc, LazyLock};
use tokio::io::AsyncWriteExt;

struct TerminateBodyFilter;

static CACHE_STORAGE: LazyLock<MemCache> = LazyLock::new(MemCache::new);

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
        unreachable!("test calls process_upstream_tasks_h2 directly")
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
}

async fn response_body_session() -> (Session, tokio::io::DuplexStream) {
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
    let mut response = ResponseHeader::build(200, None).unwrap();
    response
        .insert_header(http::header::TRANSFER_ENCODING, "chunked")
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
) -> (bool, bool, CachePhase, usize) {
    let proxy = HttpProxy::new(TerminateBodyFilter, Arc::new(ServerConf::default()));
    let (mut session, _client) = response_body_session().await;
    if cache_enabled {
        session
            .cache
            .enable(&*CACHE_STORAGE, None, None, None, None);
        session
            .cache
            .set_cache_key(CacheKey::new("h2-terminating-batch", ""));
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
        .process_upstream_tasks_h2(
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

    (source_done, terminated, session.cache.phase(), ctx)
}

#[tokio::test]
async fn h2_terminate_ignores_unprocessed_trailer_and_done_for_completion() {
    let (source_done, terminated, _, filter_calls) =
        run_terminating_batch(vec![HttpTask::Trailer(None), HttpTask::Done], false).await;

    assert!(
        !source_done,
        "unprocessed terminal tasks are not completion"
    );
    assert!(
        terminated,
        "the pump must return the typed terminate outcome"
    );
    assert_eq!(
        filter_calls, 1,
        "the trailer and Done must stay unprocessed"
    );
}

#[tokio::test]
async fn h2_terminate_aborts_cache_for_unprocessed_failure() {
    let failure = Error::explain(ReadError, "queued upstream failure").into_up();
    let (source_done, terminated, cache_phase, filter_calls) =
        run_terminating_batch(vec![HttpTask::Failed(failure)], true).await;

    assert!(!source_done, "an unprocessed failure is not completion");
    assert!(
        terminated,
        "the pump must return the typed terminate outcome"
    );
    assert!(matches!(
        cache_phase,
        CachePhase::Disabled(NoCacheReason::UpstreamError)
    ));
    assert_eq!(filter_calls, 1);
}
