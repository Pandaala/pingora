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

struct MayHoldCacheProxy;

static MAY_HOLD_CACHE_KEY_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy)]
enum CacheHeadMode {
    Immediate,
    ReplaceUnsupportedHold,
}

struct CacheHeadProxy {
    mode: CacheHeadMode,
}

type CacheHeadEvents = Arc<Mutex<Vec<String>>>;

#[async_trait]
impl ProxyHttp for MayHoldCacheProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("cache request phase is driven directly")
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        session
            .cache
            .enable(&*CACHE_STORAGE, None, None, None, None);
        Ok(())
    }

    fn response_head_may_hold(&self, _session: &Session, _ctx: &Self::CTX) -> bool {
        true
    }

    fn cache_key_callback(&self, _session: &Session, _ctx: &mut Self::CTX) -> Result<CacheKey> {
        MAY_HOLD_CACHE_KEY_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(CacheKey::new("/must-not-look-up", ""))
    }
}

#[async_trait]
impl ProxyHttp for CacheHeadProxy {
    type CTX = CacheHeadEvents;

    fn new_ctx(&self) -> Self::CTX {
        Arc::new(Mutex::new(Vec::new()))
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("test drives a fully populated cache hit directly")
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        response.insert_header("x-cache-head-filtered", "true")?;
        ctx.lock().unwrap().push("filter".into());
        Ok(())
    }

    fn response_head_commit_plan(
        &self,
        _session: &Session,
        source: ResponseHeadSource,
        response: &ResponseHeader,
        ctx: &Self::CTX,
    ) -> Result<ResponseHeadCommitPlan> {
        assert_eq!(source, ResponseHeadSource::Cache);
        assert_eq!(response.headers["x-cache-head-filtered"], "true");
        ctx.lock().unwrap().push("plan:cache".into());
        Ok(match self.mode {
            CacheHeadMode::Immediate => ResponseHeadCommitPlan::Immediate,
            CacheHeadMode::ReplaceUnsupportedHold => ResponseHeadCommitPlan::Hold(
                ResponseHeadHoldPlan::new_for_test(ResponseHeadHoldLimits::new_full_for_test(
                    1024,
                    1024,
                    8,
                    16,
                    4096,
                    16,
                    Duration::from_secs(1),
                )),
            ),
        })
    }

    fn response_head_hold_boundary(
        &self,
        _session: &Session,
        boundary: ResponseHeadBoundary,
        ctx: &mut Self::CTX,
    ) -> ResponseHeadBoundaryAction {
        assert_eq!(boundary, ResponseHeadBoundary::Unsupported);
        ctx.lock().unwrap().push("boundary:unsupported".into());
        ResponseHeadBoundaryAction::Replace(ResponseHeadReplacement::new(
            Box::new(
                ResponseHeader::build(StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS, None).unwrap(),
            ),
            vec![Bytes::from_static(b"cache-head-replacement")],
        ))
    }

    fn response_head_hold_outcome(
        &self,
        _session: &Session,
        outcome: ResponseHeadOutcome,
        _usage: ResponseHeadUsage,
        ctx: &mut Self::CTX,
    ) {
        let event = match outcome {
            ResponseHeadOutcome::Immediate => "outcome:immediate",
            ResponseHeadOutcome::Replaced => "outcome:replaced",
            other => panic!("unexpected cache head outcome: {other:?}"),
        };
        ctx.lock().unwrap().push(event.into());
    }

    fn response_head_will_commit(
        &self,
        _session: &Session,
        chosen_header: &ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.lock()
            .unwrap()
            .push(format!("will-commit:{}", chosen_header.status.as_u16()));
        Ok(())
    }
}

async fn populated_cache_hit_session(
    key: CacheKey,
    body: &'static [u8],
) -> (Session, tokio::io::DuplexStream) {
    let (mut client, server) = tokio::io::duplex(4096);
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .unwrap();
    let mut session = Session::new_h1(Box::new(server) as pingora_core::protocols::Stream);
    session.read_request().await.unwrap();
    session
        .cache
        .enable(&*CACHE_STORAGE, None, None, None, None);
    session.cache.set_cache_key(key.clone());

    let now = SystemTime::now();
    let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response
        .insert_header(http::header::CONTENT_LENGTH, body.len().to_string())
        .unwrap();
    let meta = CacheMeta::new(now + Duration::from_secs(60), now, 0, 0, response);
    let trace = pingora_cache::trace::Span::inactive().handle();
    let mut miss = CACHE_STORAGE
        .get_miss_handler(&key, &meta, &trace)
        .await
        .unwrap();
    miss.write_body(Bytes::from_static(body), false)
        .await
        .unwrap();
    miss.finish().await.unwrap();
    let (meta, hit) = CACHE_STORAGE.lookup(&key, &trace).await.unwrap().unwrap();
    session.cache.cache_found(meta, hit, HitStatus::Fresh);
    (session, client)
}

#[tokio::test]
async fn may_hold_disables_cache_before_key_generation_or_lookup() {
    MAY_HOLD_CACHE_KEY_CALLS.store(0, Ordering::SeqCst);
    let (mut client, server) = tokio::io::duplex(1024);
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .unwrap();
    let mut session = Session::new_h1(Box::new(server) as pingora_core::protocols::Stream);
    session.read_request().await.unwrap();
    let proxy = Arc::new(HttpProxy::new(
        MayHoldCacheProxy,
        Arc::new(ServerConf::default()),
    ));

    assert!(proxy.proxy_cache(&mut session, &mut ()).await.is_none());
    assert_eq!(MAY_HOLD_CACHE_KEY_CALLS.load(Ordering::SeqCst), 0);
    assert_eq!(
        session.cache.phase(),
        CachePhase::Disabled(NoCacheReason::Custom("ResponseHeadMayHold"))
    );
}

async fn run_direct_cache_head_mode(
    key: CacheKey,
    mode: CacheHeadMode,
) -> (Vec<String>, String) {
    let (mut session, mut client) = populated_cache_hit_session(key, b"cached-origin").await;
    let proxy = HttpProxy::new(CacheHeadProxy { mode }, Arc::new(ServerConf::default()));
    let mut ctx = proxy.inner.new_ctx();
    let (reusable, error) = proxy.proxy_cache_hit(&mut session, &mut ctx).await;
    assert!(
        reusable,
        "complete cache hit should preserve downstream reuse"
    );
    assert!(error.is_none(), "cache hit failed: {error:?}");
    drop(session);

    let mut wire = Vec::new();
    client.read_to_end(&mut wire).await.unwrap();
    let events = ctx.lock().unwrap().clone();
    (events, String::from_utf8(wire).unwrap())
}

#[tokio::test]
async fn direct_cache_hit_runs_immediate_head_hooks_before_writer_handoff() {
    let (events, wire) = run_direct_cache_head_mode(
        CacheKey::new("/direct-cache-head-immediate", ""),
        CacheHeadMode::Immediate,
    )
    .await;

    assert_eq!(
        events,
        [
            "filter",
            "plan:cache",
            "outcome:immediate",
            "will-commit:200"
        ]
    );
    assert!(wire.starts_with("HTTP/1.1 200"), "{wire}");
    assert!(wire.ends_with("cached-origin"), "{wire}");
}

#[tokio::test]
async fn direct_cache_hit_resolves_hold_as_unsupported_replacement() {
    let (events, wire) = run_direct_cache_head_mode(
        CacheKey::new("/direct-cache-head-replace", ""),
        CacheHeadMode::ReplaceUnsupportedHold,
    )
    .await;

    assert_eq!(
        events,
        [
            "filter",
            "plan:cache",
            "boundary:unsupported",
            "outcome:replaced",
            "will-commit:451",
        ]
    );
    assert!(wire.starts_with("HTTP/1.1 451"), "{wire}");
    assert!(wire.contains("\r\ncache-head-replacement\r\n"), "{wire}");
    assert!(!wire.contains("cached-origin"), "{wire}");
}

