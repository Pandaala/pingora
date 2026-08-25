//! Everything the seam tests need in order to make a claim about the proxy:
//! the proxy services themselves, the scripted upstreams and their event
//! [`Recorder`], and the raw downstream clients.
//!
//! Nothing here asserts anything about the proxy on its own. The scenario
//! bodies in [`super::scenarios`] and the single-combination tests in
//! [`super::single`] do that.

use async_trait::async_trait;
use bytes::Bytes;
use h2::Reason;
use http::Response;
use once_cell::sync::Lazy;
use pingora_cache::lock::{CacheKeyLockImpl, CacheLock};
use pingora_cache::{CacheKey, MemCache};
use pingora_core::modules::http::compression::ResponseCompressionBuilder;
use pingora_core::modules::http::{HttpModule, HttpModuleBuilder, HttpModules, Module};
use pingora_core::server::configuration::ServerConf;
use pingora_core::server::Server;
use pingora_core::services::ServiceWithDependents;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, RequestBodyEvent, Session};
use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{oneshot, Notify};

pub static RT: Lazy<Runtime> =
    Lazy::new(|| Builder::new_multi_thread().enable_all().build().unwrap());

/// Counts `debug_assert!(false, "Dangling cache lock started!")` panics raised
/// inside the proxy's own connection tasks, which tokio would otherwise
/// swallow. Matching on the message keeps unrelated panics (e.g. another
/// test's failed assertion) from being miscounted. See
/// `terminate_with_cache_enabled_does_not_leave_a_dangling_lock`.
pub static DANGLING_CACHE_LOCKS: AtomicUsize = AtomicUsize::new(0);
const DANGLING_LOCK_MESSAGE: &str = "Dangling cache lock started!";

static CACHE_BACKEND: Lazy<MemCache> = Lazy::new(MemCache::new);
static CACHE_LOCK: Lazy<Box<CacheKeyLockImpl>> =
    Lazy::new(|| CacheLock::new_boxed(Duration::from_secs(2)));

/// What `ProxyHttp::logging` saw when the proxy FINISHED one observed request.
///
/// `logging` is the only hook that runs strictly after the pump completed and
/// released the request. A request whose pump parks forever never gets here
/// even though the client may already hold a complete response, so a fulfilled
/// record is the observable for "the proxy finished the request" -- and its
/// absence, within a deadline, for a leak.
pub struct CompletionRecord {
    /// The instant `logging` ran. Paired with a recorded upstream event this
    /// bounds how long the proxy took to finish AFTER the upstream demonstrably
    /// held the request -- the per-request upper half of a promptness claim,
    /// usable even where the downstream connection stays open (H2).
    pub finished_at: Instant,
    /// `ctx.eos_events` as of `logging`, i.e. the FINAL count. The
    /// `x-eos-events` response header cannot serve here: `response_filter`
    /// stamps it while the upstream response headers pass through, which is
    /// necessarily before the pump could deliver a late end-of-stream event.
    pub eos_events: usize,
    /// `ctx.abandoned_events` as of `logging`.
    pub abandoned_events: usize,
    /// Every [`RequestBodyEvent`], in order, that
    /// `ProxyHttp::request_body_filter_action` was handed for this request.
    ///
    /// The counters above are lossy on purpose (`eos_events` MERGES `Complete`
    /// and `Abandoned`), which is precisely what lets a mislabelled terminal
    /// event pass unnoticed. The sequence is not lossy, so a test can state the
    /// exact shape it expects -- e.g. "all `Data` and then one `Complete`".
    pub events: Vec<RequestBodyEvent>,
    /// The same sequence as observed by the downstream [`BodyEventProbe`]
    /// module, read back out of `session.downstream_modules_ctx` in `logging`.
    ///
    /// The pump hands ONE event to both `HttpModuleCtx::request_body_filter`
    /// and `ProxyHttp::request_body_filter_action`; that the two sides see the
    /// same thing is an explicit promise of the typed-event design, and
    /// comparing these two fields is the only place a real pump proves it.
    pub module_events: Vec<RequestBodyEvent>,
    /// The final error's `retry` flag: `-1` = no error, `0`/`1` = the
    /// `RetryType::Decided` value, `2` = the undecided `ReusedOnly`.
    ///
    /// This is the ONLY observable for the two forcing points that sit next to
    /// `error_while_proxy` and `fail_to_connect`: the retry LOOP would refuse
    /// the retry anyway (it re-checks the predicate), so the loop's own
    /// behaviour cannot tell those two lines apart from the loop's check. What
    /// the application sees on the error it is handed can.
    pub retry_flag: i8,
}

impl CompletionRecord {
    /// How many events of exactly `kind` `request_body_filter_action` saw.
    pub fn count(&self, kind: RequestBodyEvent) -> usize {
        self.events.iter().filter(|e| **e == kind).count()
    }

    /// Require that the downstream module and the `ProxyHttp` hook were handed
    /// the same request-body events, in the same order.
    ///
    /// The two hooks are fed from a single `event` variable inside
    /// `send_body_to_pipe`, so nothing about their agreement is structurally
    /// enforced: a future change that recomputes the event for one of the two
    /// call sites (or reorders them around a mutation of `event`) would leave
    /// the module -- e.g. a mirroring or gRPC-bridge module -- with a different
    /// story about the same body than the application has, and every
    /// application-side assertion in this suite would stay green.
    #[track_caller]
    pub fn assert_hooks_agree(&self, what: &str) {
        assert_eq!(
            self.events, self.module_events,
            "the pump must hand a downstream HttpModule exactly the request-body \
             events it hands ProxyHttp::request_body_filter_action ({what}): the \
             application saw {:?} while the module saw {:?}",
            self.events, self.module_events
        );
    }
}

/// The rendezvous between a test and `logging`, keyed by the id the request
/// carries in `x-observe-completion`.
///
/// Only the rendezvous is global (the proxy under test is process-wide, so
/// SOME shared meeting point is unavoidable). Every entry is owned by exactly
/// one test and attributed to exactly one request: it is created with the
/// test's [`CompletionHandle`], fulfilled by the one request carrying its id,
/// and removed at fulfilment or when the handle drops. That per-request
/// ownership is what the shared per-transport statics this replaced could not
/// provide -- two requests multiplexed onto one downstream connection each get
/// their own record instead of racing for a slot.
static COMPLETION_OBSERVATIONS: Lazy<Mutex<HashMap<u64, oneshot::Sender<CompletionRecord>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
/// How many requests the proxy has seen carrying each observation id, counted
/// in `request_filter` -- i.e. BEFORE any pump could park.
///
/// Two things need this, both of them failure modes the rendezvous alone
/// cannot tell apart from the one it names:
/// - a `wait` that times out because the id never reached the proxy at all (a
///   header typo, a request answered before `request_filter`) would otherwise
///   be reported as "the pump is still parked", i.e. as a proxy defect;
/// - two requests carrying ONE id: the first to reach `logging` takes the
///   record and the second is silently mis-attributed. [`CompletionId`] makes
///   that a compile error for a well-formed caller, and this count catches the
///   hand-written header the type system cannot see.
static COMPLETION_SEEN: Lazy<Mutex<HashMap<u64, usize>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static COMPLETION_IDS: AtomicU64 = AtomicU64::new(0);

/// A test's handle on ONE request's completion. Held by the test for its own
/// request; dropping it deregisters the observation, so an abandoned handle
/// can never be fulfilled by (or leak into) a later request.
pub struct CompletionHandle {
    id: u64,
    rx: Option<oneshot::Receiver<CompletionRecord>>,
}

/// The id of one registered observation, spendable exactly once.
///
/// Neither `Copy` nor `Clone`, and the only way to read it is
/// [`Self::into_header`], which consumes it: putting one id on two requests is
/// a compile error rather than a silent mis-attribution (the record goes to
/// whichever request reaches `logging` first, and the other request's facts
/// are simply lost).
#[must_use]
pub struct CompletionId(u64);

impl CompletionId {
    /// Spend this id as the request's `x-observe-completion` header value.
    pub fn into_header(self) -> String {
        self.0.to_string()
    }
}

/// Register a completion observation: send the returned id as the request's
/// `x-observe-completion` header and await the handle.
pub fn observe_completion() -> (CompletionId, CompletionHandle) {
    let id = COMPLETION_IDS.fetch_add(1, Ordering::SeqCst);
    let (tx, rx) = oneshot::channel();
    COMPLETION_OBSERVATIONS.lock().unwrap().insert(id, tx);
    (CompletionId(id), CompletionHandle { id, rx: Some(rx) })
}

/// How many requests carrying `id` the proxy has admitted so far.
fn completion_requests_seen(id: u64) -> usize {
    COMPLETION_SEEN
        .lock()
        .unwrap()
        .get(&id)
        .copied()
        .unwrap_or(0)
}

impl CompletionHandle {
    /// Wait for the proxy to finish the observed request; panic with
    /// `on_timeout` if it has not finished within `timeout`.
    ///
    /// `on_timeout` is only used for the timeout that it actually describes --
    /// the proxy admitted the request and never finished it. A deadline that
    /// expires without the proxy ever having SEEN the id is a defect in the
    /// test, not in the proxy, and says so.
    pub async fn wait(mut self, timeout: Duration, on_timeout: &str) -> CompletionRecord {
        let id = self.id;
        let rx = self.rx.take().expect("wait consumes the handle");
        let record = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(record)) => record,
            Ok(Err(_)) => panic!("the completion observation was dropped unfulfilled"),
            Err(_) => {
                let seen = completion_requests_seen(id);
                assert!(
                    seen > 0,
                    "no request carrying observation id {id} ever reached the proxy, so \
                     nothing could fulfil this observation -- the `x-observe-completion` \
                     header is missing or malformed. (The claim this wait was written for \
                     was: {on_timeout})"
                );
                panic!("{on_timeout}");
            }
        };
        let seen = completion_requests_seen(id);
        assert_eq!(
            seen, 1,
            "observation id {id} was carried by {seen} requests: the record just \
             received belongs to whichever of them reached `logging` first, so it \
             cannot be attributed"
        );
        record
    }
}

impl Drop for CompletionHandle {
    fn drop(&mut self) {
        COMPLETION_OBSERVATIONS.lock().unwrap().remove(&self.id);
    }
}

/// A downstream [`HttpModule`] whose only job is to record the request-body
/// events the pump hands the MODULE chain, so that they can be compared with
/// the ones it hands `ProxyHttp::request_body_filter_action`.
///
/// A module cannot see `SeamCtx`, and this deliberately does not try to: the
/// events stay inside the module instance, which lives in the per-request
/// `session.downstream_modules_ctx` and is read back by `logging` through
/// `HttpModuleCtx::get::<BodyEventProbe>()`. That keeps the observation
/// per-request -- the same property [`CompletionRecord`] exists for -- where a
/// process-global counter keyed by the observation id would be shared by every
/// concurrently running test in this binary.
#[derive(Default)]
pub struct BodyEventProbe {
    events: Vec<RequestBodyEvent>,
}

#[async_trait]
impl HttpModule for BodyEventProbe {
    async fn request_body_filter(
        &mut self,
        _body: &mut Option<Bytes>,
        event: RequestBodyEvent,
    ) -> Result<()> {
        self.events.push(event);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct BodyEventProbeBuilder;

impl HttpModuleBuilder for BodyEventProbeBuilder {
    fn init(&self) -> Module {
        Box::new(BodyEventProbe::default())
    }
}

#[derive(Default)]
pub struct SeamCtx {
    body_bytes_seen: usize,
    /// How many terminal request-body events the filter observed. Echoed back
    /// as `x-eos-events` so existing tests can assert exactly one terminal
    /// notification regardless of its cause.
    eos_events: usize,
    /// How many terminal events reported that the proxy abandoned an
    /// incomplete downstream body.
    abandoned_events: usize,
    /// Every event, in order. See [`CompletionRecord::events`].
    events: Vec<RequestBodyEvent>,
    /// How many times `request_trailer_filter` was invoked. Echoed back as
    /// `x-trailer-hook-calls`; the hook's contract is at most one call per
    /// downstream request, including across retry attempts.
    trailer_hook_calls: usize,
}

pub struct SeamProxy {}

#[async_trait]
impl ProxyHttp for SeamProxy {
    type CTX = SeamCtx;
    fn new_ctx(&self) -> Self::CTX {
        SeamCtx::default()
    }

    /// Register [`BodyEventProbe`] alongside the module the trait default would
    /// have added on its own.
    ///
    /// The disabled `ResponseCompressionBuilder` is repeated here on purpose:
    /// overriding this hook REPLACES the default, and several tests in
    /// `tests/utils` rely on that module existing at all. Keeping it means the
    /// only thing the probe changes about this proxy is that an extra module
    /// observes the request body.
    fn init_downstream_modules(&self, modules: &mut HttpModules) {
        modules.add_module(ResponseCompressionBuilder::enable(0));
        modules.add_module(Box::new(BodyEventProbeBuilder));
    }

    /// Only bookkeeping: record that the proxy ADMITTED a request carrying an
    /// observation id, which is what lets [`CompletionHandle::wait`] tell "the
    /// pump never finished" apart from "this id never got here". Returning
    /// `false` proxies the request exactly as before.
    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool> {
        if let Some(id) = session
            .req_header()
            .headers
            .get("x-observe-completion")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
        {
            *COMPLETION_SEEN.lock().unwrap().entry(id).or_insert(0) += 1;
        }
        Ok(false)
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let port = session
            .req_header()
            .headers
            .get("x-port")
            .and_then(|v| v.to_str().ok())
            .expect("tests must set x-port")
            .to_string();
        let mut peer = Box::new(HttpPeer::new(
            format!("127.0.0.1:{port}"),
            false,
            "".to_string(),
        ));
        if session.req_header().headers.get("x-h2").is_some() {
            // default is 1, 1
            peer.options.set_http_version(2, 2);
        }
        // How many concurrent h2 streams this peer's connections may carry.
        //
        // The default (1) is what every other test wants, and it also hides the
        // reused-connection half of the h2 connector: with one stream per
        // connection a pooled connection is never picked up while a stream is
        // still in flight, so `ConnectionRef::spawn_stream` is only ever
        // reached on connections nothing else is using. A test that needs the
        // GOAWAY-on-a-busy-connection path opts in per request -- and must send
        // the same value on every request of the exchange, because
        // `max_h2_streams` is part of the peer's `reuse_hash` and a different
        // value is a different connection pool.
        if let Some(max) = session
            .req_header()
            .headers
            .get("x-max-h2-streams")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
        {
            peer.options.max_h2_streams = max;
        }
        Ok(peer)
    }

    fn request_retry_allowed(&self, session: &Session, _ctx: &Self::CTX) -> bool {
        session.req_header().headers.get("x-no-retry").is_none()
    }

    /// Marking a connect failure retryable is this hook's documented purpose.
    /// Without it no reachable connect error is retryable at all (`l4.rs`
    /// resolves them all to `Decided(false)` for a fresh dial), so the guard
    /// that forces the retry predicate onto this error would have nothing to
    /// act on and could not be tested.
    fn fail_to_connect(
        &self,
        session: &mut Session,
        _peer: &HttpPeer,
        _ctx: &mut Self::CTX,
        mut e: Box<pingora_error::Error>,
    ) -> Box<pingora_error::Error> {
        if session
            .req_header()
            .headers
            .get("x-connect-retryable")
            .is_some()
        {
            e.retry = true.into();
        }
        e
    }

    fn upstream_request_body_disposition(
        &self,
        session: &pingora_proxy::Session,
        _ctx: &Self::CTX,
    ) -> pingora_proxy::UpstreamRequestBodyDisposition {
        use pingora_proxy::UpstreamRequestBodyDisposition as D;
        match session
            .req_header()
            .headers
            .get("x-disposition")
            .and_then(|v| v.to_str().ok())
        {
            Some("streamed") => D::Streamed,
            Some("bodyless") => D::Bodyless,
            _ => D::Ordinary,
        }
    }

    async fn upstream_request_filter(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        if session
            .req_header()
            .headers
            .get("x-no-header-eos")
            .is_some()
        {
            // Mirrors what the gRPC-web bridge does: gRPC requires a bodyless
            // request stream to be closed by an empty DATA frame carrying
            // END_STREAM, never by END_STREAM on HEADERS.
            upstream_request.set_send_end_stream(false);
        }
        Ok(())
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        if session.req_header().headers.get("x-enable-cache").is_some() {
            session
                .cache
                .enable(&*CACHE_BACKEND, None, None, Some(CACHE_LOCK.as_ref()), None);
        }
        Ok(())
    }

    fn cache_key_callback(&self, session: &Session, _ctx: &mut Self::CTX) -> Result<CacheKey> {
        let req = session.req_header();
        Ok(CacheKey::new(
            "",
            format!("{}", req.uri),
            format!("{:?}", req.headers.get("x-cache-key")),
        ))
    }

    async fn request_body_filter_action(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        event: RequestBodyEvent,
        ctx: &mut Self::CTX,
    ) -> Result<pingora_proxy::RequestBodyAction> {
        if event.is_terminal() {
            ctx.eos_events += 1;
        }
        if event == RequestBodyEvent::Abandoned {
            ctx.abandoned_events += 1;
        }
        ctx.events.push(event);
        ctx.body_bytes_seen += body.as_ref().map_or(0, |b| b.len());

        // Write a chunked response header + body but do NOT end the stream,
        // then terminate. The terminate arms return before the pump's
        // `finish_body()`, and `HttpProxy::finish` skips
        // `downstream_session.finish()` because a terminated request never
        // reports reuse -- so without a defensive `finish_body()` the
        // terminating `0\r\n\r\n` chunk is never written and the client sees a
        // truncated response body it cannot distinguish from a broken
        // connection.
        if session
            .req_header()
            .headers
            .get("x-terminate-unflushed")
            .is_some()
            && event.is_terminal()
        {
            let mut resp = ResponseHeader::build(403, None).unwrap();
            resp.insert_header("transfer-encoding", "chunked")?;
            session.write_response_header(Box::new(resp), false).await?;
            session
                .write_response_body(Some(Bytes::from_static(b"denied")), false)
                .await?;
            return Ok(pingora_proxy::RequestBodyAction::Terminate);
        }

        let threshold: Option<usize> = session
            .req_header()
            .headers
            .get("x-terminate-after-bytes")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        if let Some(threshold) = threshold {
            if ctx.body_bytes_seen >= threshold {
                let mut resp = ResponseHeader::build(403, None).unwrap();
                resp.insert_header("content-length", "6")?;
                session.write_response_header(Box::new(resp), false).await?;
                session
                    .write_response_body(Some(Bytes::from_static(b"denied")), true)
                    .await?;
                return Ok(pingora_proxy::RequestBodyAction::Terminate);
            }
        }
        Ok(pingora_proxy::RequestBodyAction::Continue)
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_response.insert_header("x-eos-events", ctx.eos_events.to_string())?;
        upstream_response.insert_header("x-body-bytes", ctx.body_bytes_seen.to_string())?;
        upstream_response
            .insert_header("x-trailer-hook-calls", ctx.trailer_hook_calls.to_string())?;
        // Whether the native retry buffer holds this request's body, i.e.
        // whether `enable_retry_buffering()` ran. This is the observable for the
        // retry predicate's FIRST consumption point, which the retry loop's own
        // check masks entirely.
        let buffered = usize::from(session.as_mut().get_retry_buffer().is_some());
        upstream_response.insert_header("x-retry-buffer", buffered.to_string())?;
        Ok(())
    }

    async fn logging(
        &self,
        session: &mut Session,
        e: Option<&pingora_error::Error>,
        ctx: &mut Self::CTX,
    ) {
        let Some(id) = session
            .req_header()
            .headers
            .get("x-observe-completion")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
        else {
            return;
        };
        let retry_flag = match e.map(|e| e.retry) {
            Some(pingora_error::RetryType::Decided(b)) => i8::from(b),
            Some(pingora_error::RetryType::ReusedOnly) => 2,
            None => -1,
        };
        // The module side of the same request, read out of the per-request
        // module ctx. `logging` is the earliest phase that is guaranteed to run
        // after the LAST body event (`response_filter` runs while the upstream
        // response headers pass through, i.e. possibly before a late terminal
        // event), and the session -- module ctx included -- is still alive here.
        let module_events = session
            .downstream_modules_ctx
            .get::<BodyEventProbe>()
            .expect("SeamProxy::init_downstream_modules registers BodyEventProbe")
            .events
            .clone();
        // The send can only fail if the test already dropped its handle (e.g.
        // its own deadline fired first); there is nothing to tell it then.
        if let Some(tx) = COMPLETION_OBSERVATIONS.lock().unwrap().remove(&id) {
            let _ = tx.send(CompletionRecord {
                finished_at: Instant::now(),
                eos_events: ctx.eos_events,
                abandoned_events: ctx.abandoned_events,
                events: ctx.events.clone(),
                module_events,
                retry_flag,
            });
        }
    }

    async fn request_trailer_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<pingora_proxy::RequestBodyAction> {
        ctx.trailer_hook_calls += 1;
        if session
            .req_header()
            .headers
            .get("x-reject-trailers")
            .is_some()
        {
            let mut resp = ResponseHeader::build(400, None).unwrap();
            resp.insert_header("content-length", "0")?;
            session.write_response_header(Box::new(resp), true).await?;
            return Ok(pingora_proxy::RequestBodyAction::Terminate);
        }
        Ok(pingora_proxy::RequestBodyAction::Continue)
    }
}

/// A proxy that overrides ONLY the legacy [`ProxyHttp::request_body_filter`],
/// so that a request through it exercises the default
/// `request_body_filter_action` delegation inside a real pump.
pub struct LegacyHookProxy {}

#[derive(Default)]
pub struct LegacyCtx {
    body_bytes_seen: usize,
    calls: usize,
}

#[async_trait]
impl ProxyHttp for LegacyHookProxy {
    type CTX = LegacyCtx;
    fn new_ctx(&self) -> Self::CTX {
        LegacyCtx::default()
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let port = session
            .req_header()
            .headers
            .get("x-port")
            .and_then(|v| v.to_str().ok())
            .expect("tests must set x-port")
            .to_string();
        Ok(Box::new(HttpPeer::new(
            format!("127.0.0.1:{port}"),
            false,
            "".to_string(),
        )))
    }

    /// The legacy hook. Nothing in this impl mentions
    /// `request_body_filter_action`: if the trait default stopped delegating
    /// here, `x-legacy-calls` would come back as `0`.
    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _event: RequestBodyEvent,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.calls += 1;
        ctx.body_bytes_seen += body.as_ref().map_or(0, |b| b.len());
        Ok(())
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_response.insert_header("x-legacy-calls", ctx.calls.to_string())?;
        upstream_response.insert_header("x-legacy-bytes", ctx.body_bytes_seen.to_string())?;
        Ok(())
    }
}

/// The three proxy listeners this file drives, on EPHEMERAL ports.
///
/// Fixed ports are not merely inconvenient here, they are unsound: two
/// concurrent runs of this binary (two checkouts, a `cargo test` racing a
/// watcher, CI sharding) would have one instance's requests answered by the
/// OTHER process's proxy, and almost every test in this file would still pass --
/// against code that is not the code under test. A reviewer reproduced exactly
/// that with 21 of 22 tests silently green.
pub struct SeamPorts {
    /// Plain HTTP/1.1 downstream.
    h1: u16,
    /// h2c (prior-knowledge HTTP/2) downstream.
    h2c: u16,
    /// HTTP/1.1 downstream in front of [`LegacyHookProxy`].
    legacy: u16,
}

impl SeamPorts {
    pub fn h1_addr(&self) -> String {
        format!("127.0.0.1:{}", self.h1)
    }
    pub fn h2c_addr(&self) -> String {
        format!("127.0.0.1:{}", self.h2c)
    }
    pub fn legacy_addr(&self) -> String {
        format!("127.0.0.1:{}", self.legacy)
    }
}

/// Reserve a free localhost port by binding it and immediately releasing it.
///
/// The window between release and pingora's own bind is why the readiness poll
/// in `start_seam_server` panics loudly rather than sleeping: if anything else
/// takes the port in between, pingora's bind fails and the tests must say so
/// instead of silently talking to a stranger.
pub fn reserve_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("cannot reserve a local port");
    listener.local_addr().unwrap().port()
}

fn start_seam_server() -> SeamPorts {
    // Panics inside the proxy's connection tasks are swallowed by tokio; watch
    // for the dangling-cache-lock one (see the cache-terminate test).
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let message = payload
            .downcast_ref::<String>()
            .map(|s| s.as_str())
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("");
        if message.contains(DANGLING_LOCK_MESSAGE) {
            DANGLING_CACHE_LOCKS.fetch_add(1, Ordering::SeqCst);
        }
        previous_hook(info);
    }));

    let ports = SeamPorts {
        h1: reserve_port(),
        h2c: reserve_port(),
        legacy: reserve_port(),
    };
    let (h1_addr, h2c_addr, legacy_addr) = (ports.h1_addr(), ports.h2c_addr(), ports.legacy_addr());
    let addrs = [h1_addr.clone(), h2c_addr.clone(), legacy_addr.clone()];

    thread::spawn(move || {
        let conf = Arc::new(ServerConf {
            max_retries: 2,
            ..Default::default()
        });

        let mut server = Server::new(None).unwrap();
        server.bootstrap();

        let mut h1 = pingora_proxy::http_proxy_service(&conf, SeamProxy {});
        h1.add_tcp(&h1_addr);

        let mut h2c = pingora_proxy::http_proxy_service(&conf, SeamProxy {});
        let logic = h2c.app_logic_mut().unwrap();
        let mut opts = pingora_core::apps::HttpServerOptions::default();
        opts.h2c = true;
        // CONNECT is 405-rejected unless the application opts in (the guard
        // in `lib.rs`), and this listener is the only one where a CONNECT
        // request is constructible at all: plain H2 CONNECT parses
        // featurelessly, while an H1 authority-form request line needs the
        // `patched_http1` feature, which does not compile in this fork (it
        // requires a patched httparse this workspace does not carry). Opting
        // in lets the CONNECT half of the disposition coercion
        // (`safe_disposition`, `is_connect`) be pinned end-to-end.
        //
        // Setting it for the whole listener, for one test's benefit, is safe
        // and this is the proof rather than the belief: the field is read at
        // exactly one place in the workspace, `pingora-proxy/src/lib.rs:266`
        // (`grep -rn allow_connect_method_proxying`), and that read is
        // `&&`-joined with `req_header().method == Method::CONNECT`. A
        // non-CONNECT request therefore takes the same branch either way --
        // there is no other consumer to scope it away from.
        opts.allow_connect_method_proxying = true;
        logic.server_options = Some(opts);
        h2c.add_tcp(&h2c_addr);

        let mut legacy = pingora_proxy::http_proxy_service(&conf, LegacyHookProxy {});
        legacy.add_tcp(&legacy_addr);

        let services: Vec<Box<dyn ServiceWithDependents>> =
            vec![Box::new(h1), Box::new(h2c), Box::new(legacy)];
        server.add_services(services);
        server.run_forever();
    });

    // Poll for readiness instead of sleeping: a bind that lost the race must
    // fail the run loudly rather than let every test talk to whatever else is
    // listening.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    for addr in addrs {
        loop {
            if std::net::TcpStream::connect(&addr).is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the seam proxy never started listening on {addr}; its bind most likely \
                 lost the race for the port"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    ports
}

static SEAM_SERVER: Lazy<SeamPorts> = Lazy::new(start_seam_server);

pub fn init() -> &'static SeamPorts {
    Lazy::force(&SEAM_SERVER)
}

// ---------------------------------------------------------------------------
// Upstream event recorder
// ---------------------------------------------------------------------------

/// One thing a scripted upstream observed the proxy do on the wire.
///
/// This is the WHOLE vocabulary; nothing here is speculative. Anything a test
/// wants to claim about the proxy's upstream behaviour has to be expressible
/// as a statement about this log, which is what keeps the claims honest: an
/// echoed response header proves what the APPLICATION saw, a shared
/// `AtomicUsize` proves a request was counted, and neither is the same
/// statement as "the proxy sent RST_STREAM(CANCEL)" or "the proxy reused the
/// pooled connection".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpEvent {
    /// `listener.accept()` returned. `conn` is this upstream's connection
    /// ordinal, and it is the whole point of the recorder: the request
    /// counters this file used to rely on count REQUESTS, so "the pooled
    /// connection was reused" and "a fresh connection was opened because the
    /// old one was poisoned" were both unassertable.
    ConnAccepted {
        conn: u32,
    },
    ReqHeaders {
        conn: u32,
        stream: u32,
        /// H2: END_STREAM on the HEADERS frame. H1: the request declared
        /// neither a body length nor a transfer coding, so no body can follow.
        ///
        /// ONE-WAY on H2. `true` is a wire fact -- the HEADERS frame carried
        /// END_STREAM. `false` is not: h2 reports
        /// `RecvStream::is_end_stream()` as "END_STREAM received AND nothing
        /// else is pending on this stream" (`Recv::is_end_stream`), so any
        /// frame that lands between the HEADERS frame and the scripted step
        /// reading the flag -- in practice the proxy's own RST_STREAM, ~30us
        /// later -- makes an ended stream read as `false`.
        ///
        /// Measured, because a test here once branched on the `false` reading
        /// as if it were the framing: in
        /// `bodyless_with_a_real_body_fails_closed` the proxy provably puts
        /// END_STREAM on HEADERS in every run (`send_header_eos == true`,
        /// probed in a scratch copy) and this field still reads `false` in
        /// 40/40 runs -- until the proxy's reset is delayed by 300ms, at which
        /// point it reads `true` in both H2 cells.
        ///
        /// So: assert `headers_eos == true` freely; assert `false` only where
        /// nothing can reset the stream inside the same run, and prefer a
        /// positive wire fact (body bytes arriving) where one exists.
        headers_eos: bool,
        content_length: Option<u64>,
        transfer_encoding: Option<String>,
    },
    ReqData {
        conn: u32,
        stream: u32,
        len: usize,
        end_stream: bool,
    },
    ReqTrailers {
        conn: u32,
        stream: u32,
    },
    /// RST_STREAM the PROXY sent to this upstream.
    PeerReset {
        conn: u32,
        stream: u32,
        code: Reason,
    },
    /// GOAWAY the PROXY sent to this upstream.
    PeerGoaway {
        conn: u32,
        code: Reason,
    },
    /// H1: FIN on the request half of the connection.
    PeerHalfClose {
        conn: u32,
    },
    /// H1: RST on the connection (a read failed with `ECONNRESET`).
    ///
    /// The connection-level sibling of [`Self::PeerReset`]. Whether an
    /// abandoned H1 leg surfaces as this event or as a clean
    /// [`Self::PeerHalfClose`] is an implementation accident (it depends on
    /// whether the proxy drained its receive buffer before closing), so
    /// scenarios accept either -- what matters is that ONE of them appears,
    /// which is what "the proxy ended this connection" means on a transport
    /// with no per-request cancel. Before this variant an `ECONNRESET` fell
    /// into the eventless `ReadMore::Failed` arm, so an RST teardown was
    /// indistinguishable from the recording window running out.
    ///
    /// This comment used to say the observed teardown was "FIN on macOS,
    /// consistently". Disproved by measurement: over 20 isolated runs each of
    /// `bodyless_with_a_real_body_fails_closed`, `h1_to_h1` gave 15 FIN / 5
    /// RST and `h2c_to_h1` gave 19 FIN / 1 RST. (A reviewer measuring the
    /// same H1 cell got 16 RST / 4 FIN -- the opposite ratio, on the same
    /// platform.) So neither form is "the" teardown, and this variant is not
    /// a nicety: without it the RST runs record no teardown event at all,
    /// which is indistinguishable from the proxy having kept the connection.
    PeerConnReset {
        conn: u32,
    },
    ConnClosed {
        conn: u32,
    },
}

impl UpEvent {
    /// Whether this event forecloses waiting for anything else on the stream
    /// or connection it belongs to. See [`Recorder::wait_for`].
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            UpEvent::ConnClosed { .. }
                | UpEvent::ReqData {
                    end_stream: true,
                    ..
                }
        )
    }

    /// Whether this event STARTS something, i.e. un-forecloses a wait that a
    /// preceding terminal event would otherwise have ended. A retry opens a
    /// second connection after the first one closed, and a wait for the retry
    /// must not fast-fail on the first connection's close.
    fn is_opening(&self) -> bool {
        matches!(
            self,
            UpEvent::ConnAccepted { .. } | UpEvent::ReqHeaders { .. }
        )
    }
}

#[derive(Clone, Debug)]
struct Recorded {
    seq: u64,
    at: Instant,
    event: UpEvent,
}

/// The append-only event log of one scripted upstream.
///
/// Every scripted upstream gets its OWN recorder, so the tests in this file --
/// which share one proxy and run concurrently -- can never read each other's
/// events.
#[derive(Clone)]
pub struct Recorder {
    log: Arc<Mutex<Vec<Recorded>>>,
    seq: Arc<AtomicU64>,
    conns: Arc<AtomicU32>,
    origin: Instant,
}

impl Recorder {
    pub fn new() -> Self {
        Recorder {
            log: Arc::new(Mutex::new(Vec::new())),
            seq: Arc::new(AtomicU64::new(0)),
            conns: Arc::new(AtomicU32::new(0)),
            origin: Instant::now(),
        }
    }

    /// Claim the next connection ordinal and record the accept. Called exactly
    /// where `listener.accept()` succeeded.
    fn accept_conn(&self) -> u32 {
        let conn = self.conns.fetch_add(1, Ordering::SeqCst);
        self.push(UpEvent::ConnAccepted { conn });
        conn
    }

    fn push(&self, event: UpEvent) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        self.log.lock().unwrap().push(Recorded {
            seq,
            at: Instant::now(),
            event,
        });
    }

    /// How many connections this upstream ever accepted.
    pub fn connections(&self) -> usize {
        self.conns.load(Ordering::SeqCst) as usize
    }

    /// Total request body bytes this upstream ever recorded, across every
    /// connection and stream.
    pub fn body_bytes(&self) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|r| match r.event {
                UpEvent::ReqData { len, .. } => len,
                _ => 0,
            })
            .sum()
    }

    pub fn count(&self, pred: impl Fn(&UpEvent) -> bool) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|r| pred(&r.event))
            .count()
    }

    /// When the FIRST event matching `pred` was recorded. The anchor for
    /// promptness claims: "the upstream demonstrably had the request at T" is
    /// the lower bound a latency assertion needs in order to be able to fail.
    pub fn first_seen(&self, pred: impl Fn(&UpEvent) -> bool) -> Option<Instant> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .find(|r| pred(&r.event))
            .map(|r| r.at)
    }

    /// The whole recorded log, for failure messages. A bare "timed out" says
    /// nothing about WHY; the log usually says it outright.
    pub fn dump(&self) -> String {
        let log = self.log.lock().unwrap();
        let mut out = format!("recorded upstream events ({}):\n", log.len());
        for r in log.iter() {
            out.push_str(&format!(
                "  #{:<3} +{:>9.3}ms  {:?}\n",
                r.seq,
                (r.at - self.origin).as_secs_f64() * 1000.0,
                r.event
            ));
        }
        out
    }

    /// Wait until an event matching `pred` has been recorded, scanning the
    /// events recorded BEFORE this call as well.
    ///
    /// Two behaviours worth the extra code:
    /// - it fast-fails when the stream or connection it is watching has ended
    ///   (`ConnClosed`, or an end-of-stream `ReqData`) without the awaited
    ///   event, instead of burning the whole timeout on something that can
    ///   never arrive. A later `ConnAccepted`/`ReqHeaders` re-opens the wait,
    ///   so a retry's fresh connection is still awaitable.
    /// - the failure carries the full event log.
    ///
    /// Note that the fast-fail rule makes this unsuitable for waiting on H1
    /// TRAILERS, which the wire order puts after the terminal chunk; no test
    /// here needs that.
    #[must_use = "an ignored wait asserts nothing"]
    pub async fn wait_for(
        &self,
        what: &str,
        timeout: Duration,
        pred: impl Fn(&UpEvent) -> bool,
    ) -> std::result::Result<UpEvent, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut cursor = 0usize;
        let mut foreclosed = false;
        loop {
            {
                let log = self.log.lock().unwrap();
                while cursor < log.len() {
                    let event = log[cursor].event.clone();
                    cursor += 1;
                    if pred(&event) {
                        return Ok(event);
                    }
                    if event.is_terminal() {
                        foreclosed = true;
                    } else if event.is_opening() {
                        foreclosed = false;
                    }
                }
            }
            if foreclosed {
                return Err(format!(
                    "stream finished while waiting for {what}\n{}",
                    self.dump()
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "timed out after {timeout:?} waiting for {what}\n{}",
                    self.dump()
                ));
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Require that NO event matching `pred` is recorded for `window`, from
    /// this call onwards.
    ///
    /// This is what replaces `counter.load(..) <= 1`: that form is also
    /// satisfied by a counter that never moved, so it passed whether or not
    /// the thing it named ever happened.
    #[must_use = "an ignored wait asserts nothing"]
    pub async fn expect_none(
        &self,
        what: &str,
        window: Duration,
        pred: impl Fn(&UpEvent) -> bool,
    ) -> std::result::Result<(), String> {
        let cursor = self.log.lock().unwrap().len();
        let deadline = tokio::time::Instant::now() + window;
        loop {
            let offender = {
                let log = self.log.lock().unwrap();
                log[cursor..]
                    .iter()
                    .find(|r| pred(&r.event))
                    .map(|r| r.event.clone())
            };
            if let Some(offender) = offender {
                return Err(format!(
                    "{what} must not have happened, but did: {offender:?}\n{}",
                    self.dump()
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// Unwrap a recorder result, printing the (multi-line) failure text as text
/// rather than as an escaped `Debug` string.
#[track_caller]
pub fn expect_ok<T>(result: std::result::Result<T, String>) -> T {
    match result {
        Ok(value) => value,
        Err(message) => panic!("{message}"),
    }
}

/// A spawned scripted upstream, plus the guard that makes a test which never
/// reached it fail loudly.
///
/// Several tests in this file assert that something did NOT happen. Those
/// assertions all hold trivially if the request never left the proxy at all,
/// so the upstream having been exercised is a precondition of the test meaning
/// anything. A test whose upstream is deliberately unused must say so with
/// [`Self::expect_unused`].
pub struct ExercisedUpstream {
    port: u16,
    /// How many requests the script has served, i.e. the script cursor.
    requests: Arc<AtomicUsize>,
    rec: Recorder,
    skipped: Cell<bool>,
}

impl ExercisedUpstream {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn rec(&self) -> &Recorder {
        &self.rec
    }

    #[allow(dead_code)]
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// This upstream is expected never to be reached; suppress the vacuity
    /// guard. The test must then assert the absence itself (typically
    /// `rec().connections() == 0`).
    pub fn expect_unused(&self) -> &Self {
        self.skipped.set(true);
        self
    }
}

// ---------------------------------------------------------------------------
// Matrix opt-out bookkeeping
// ---------------------------------------------------------------------------

thread_local! {
    /// Set by `skip_combo!`, cleared by [`enter_test`].
    ///
    /// A skipped combination returns before it can drive its upstream, so the
    /// [`ExercisedUpstream`] vacuity guard would fire on any upstream the body
    /// had already spawned -- turning a deliberate, announced opt-out into a
    /// failure. The flag is per-thread and cleared at the START of every
    /// generated test rather than at the end of a skip, because libtest runs
    /// serial (`--test-threads=1`) tests on one shared thread: a flag only ever
    /// set would leak into the next test and silently disarm ITS guard.
    static COMBINATION_SKIPPED: Cell<bool> = const { Cell::new(false) };
}

/// Clear the opt-out flag. Every generated matrix test calls this first.
pub fn enter_test() {
    COMBINATION_SKIPPED.with(|flag| flag.set(false));
}

/// Record that this combination opted out. Called by `skip_combo!`.
pub fn note_combination_skipped() {
    COMBINATION_SKIPPED.with(|flag| flag.set(true));
}

fn combination_skipped() -> bool {
    COMBINATION_SKIPPED.with(|flag| flag.get())
}

impl Drop for ExercisedUpstream {
    fn drop(&mut self) {
        if std::thread::panicking() || self.skipped.get() || combination_skipped() {
            return;
        }
        // The upstream side of an exchange is not synchronised with the
        // downstream reply, so a test that asserted only downstream facts may
        // legitimately finish a hair before the request lands. Grace-poll
        // rather than flake.
        let received = || self.rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. }));
        let deadline = Instant::now() + Duration::from_secs(3);
        while received() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            received() > 0,
            "test finished without the upstream ever receiving a request -- its \
             assertions are vacuous. Event log:\n{}",
            self.rec.dump()
        );
    }
}

/// A scripted H2 upstream.
#[derive(Clone)]
pub enum H2UpstreamStep {
    /// Respond 200 with a small body, ending the stream normally.
    Ok200,
    /// Respond 200 with a small body, then keep the REQUEST stream alive.
    ///
    /// Dropping h2's `RecvStream` while the peer has not ended its request
    /// stream makes h2 emit RST_STREAM(NO_ERROR) -- the RFC 9113 section 8.1
    /// "stop sending the request body" signal. Pingora's h2 client only treats
    /// that as a clean end of body once the response is PROVABLY complete, and
    /// this step's response declares no `content-length`, so it lands in the
    /// documented residual gap and still surfaces as an upstream error. That
    /// would mask any test about the DOWNSTREAM read side, so tests that
    /// deliberately leave the request stream open use this step to keep the
    /// upstream from resetting at all.
    Ok200Linger,
    /// Send response HEADERS without ending the stream, send a partial body,
    /// then wait to be RELEASED before resetting the stream.
    ///
    /// The rendezvous replaces a 50ms sleep. A sleep only makes the reset
    /// *probably* land after the client observed the partial body; the test
    /// signals the gate once it has actually read that body, so the ordering
    /// the test depends on is established rather than hoped for.
    HeaderThenReset(Arc<Notify>),
    /// Accept the request stream, read and record whatever body arrives (there
    /// may be none, and it may end in a peer RST_STREAM), then never respond
    /// and park forever.
    Hang,
    /// Drain the request body, then respond 200 with `x-headers-eos` set to
    /// whether the request's HEADERS frame carried END_STREAM. Lets a test
    /// observe the exact upstream request framing the proxy chose.
    EchoRequestEos,
    /// Like [`Self::EchoRequestEos`], but reports the FULL upstream request
    /// framing: `x-headers-eos`, `x-req-content-length` (`none` when absent),
    /// `x-req-transfer-encoding` (`none` when absent) and `x-req-body-len`.
    EchoRequestFraming,
    /// Send a graceful GOAWAY (`NO_ERROR`) while this stream is in flight,
    /// announce it on `applied`, and HOLD the stream open until `release`
    /// fires before responding 200.
    ///
    /// The GOAWAY is applied to the connection BEFORE the response frames are
    /// queued (the step waits for the accept loop's acknowledgement), so the
    /// wire order is GOAWAY first, response second: the proxy provably learns
    /// the connection is going away while its stream is still in flight, and
    /// RFC 9113 says that stream (id <= the GOAWAY's last-stream-id) still
    /// completes.
    ///
    /// Holding is what makes the GOAWAY testable as a POOL fact rather than
    /// only as a framing one. `graceful_shutdown` closes the connection once
    /// the last in-flight stream finishes, so with [`Self::GoawayThenOk200`]
    /// the origin's own FIN is what makes the next request dial again: the
    /// proxy's shutdown gates (`ConnectionRef::more_streams_allowed`,
    /// `spawn_stream`'s GOAWAY branch) are never even reached, and removing
    /// either of them leaves such a test green.
    ///
    /// While this step holds its stream:
    /// - the origin cannot close the connection (the h2 server keeps it alive
    ///   for the open stream), so a follow-up request that lands on a fresh
    ///   connection did so because the PROXY decided to;
    /// - the proxy's h2 client connection stays alive too (h2 only tears down
    ///   a GOAWAY'd connection once it has no streams left), so the pooled
    ///   `ConnectionRef` is neither closed nor evicted, and the follow-up
    ///   request reaches `spawn_stream` on a connection whose peer has gone
    ///   away -- which is the branch under test.
    GoawayThenHoldThenOk200 {
        /// Signalled once the GOAWAY is queued on the connection.
        applied: Arc<Notify>,
        /// Awaited before the response is sent; the test fires it when it is
        /// done making claims about the still-open connection.
        release: Arc<Notify>,
    },
    /// Kill the whole connection with an error GOAWAY carrying this code
    /// while the stream is in flight, and never respond.
    ///
    /// The abrupt shape of [`Self::GoawayThenHoldThenOk200`]:
    /// `abrupt_shutdown` puts
    /// GOAWAY(code) on the wire and tears the connection down, so the
    /// in-flight stream can never complete. What the proxy owes its client
    /// then is the scenario's to assert.
    AbruptGoaway(Reason),
}

/// Records the request side of ONE h2 stream while the script consumes it.
struct H2StreamRecorder {
    rec: Recorder,
    conn: u32,
    stream: u32,
}

impl H2StreamRecorder {
    /// One `RecvStream::data()` step, recording the DATA frame or the peer's
    /// RST_STREAM.
    async fn next(
        &self,
        body: &mut h2::RecvStream,
    ) -> Option<std::result::Result<Bytes, h2::Error>> {
        let item = body.data().await;
        match &item {
            Some(Ok(chunk)) => self.rec.push(UpEvent::ReqData {
                conn: self.conn,
                stream: self.stream,
                len: chunk.len(),
                end_stream: body.is_end_stream(),
            }),
            Some(Err(e)) => {
                if let Some(code) = e.reason() {
                    self.rec.push(UpEvent::PeerReset {
                        conn: self.conn,
                        stream: self.stream,
                        code,
                    });
                }
            }
            None => {}
        }
        item
    }

    /// Consume the whole request stream, recording every frame. Returns the
    /// number of body bytes that actually arrived.
    async fn drain(&self, body: &mut h2::RecvStream) -> usize {
        let mut total = 0usize;
        loop {
            match self.next(body).await {
                Some(Ok(chunk)) => total += chunk.len(),
                // Reset: already recorded, and nothing more will arrive.
                Some(Err(_)) => return total,
                None => break,
            }
        }
        if let Ok(Some(_)) = body.trailers().await {
            self.rec.push(UpEvent::ReqTrailers {
                conn: self.conn,
                stream: self.stream,
            });
        }
        total
    }
}

/// It accepts TCP connections and, for each one, performs an h2 server
/// handshake and serves streams sequentially; the k-th stream overall
/// (0-based, across connections) is answered with `script[k]`.
pub fn spawn_scripted_h2_upstream(script: Vec<H2UpstreamStep>) -> ExercisedUpstream {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_ret = counter.clone();
    let rec = Recorder::new();
    let rec_ret = rec.clone();
    let listener = RT.block_on(async { TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let port = listener.local_addr().unwrap().port();
    let script = Arc::new(script);

    RT.spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };
            let conn = rec.accept_conn();
            let counter = counter.clone();
            let script = script.clone();
            let rec = rec.clone();
            tokio::spawn(async move {
                let mut connection = match h2::server::handshake(stream).await {
                    Ok(c) => c,
                    Err(_) => {
                        rec.push(UpEvent::ConnClosed { conn });
                        return;
                    }
                };
                // GOAWAY emission channel. Only the accept loop holds the
                // `Connection`, but the shutdown APIs live there and the
                // GOAWAY steps run in per-stream tasks -- so a step sends a
                // command and waits for the loop's acknowledgement, which is
                // what sequences the GOAWAY into the frame queue AHEAD of
                // anything the step writes afterwards.
                let (goaway_tx, mut goaway_rx) = tokio::sync::mpsc::unbounded_channel::<(
                    Option<Reason>,
                    tokio::sync::oneshot::Sender<()>,
                )>();
                // Spawn a task per stream so the outer accept() loop keeps
                // driving (and flushing) the shared connection while a
                // stream's response handling (e.g. a gated reset) is in
                // flight, rather than blocking connection I/O on it.
                loop {
                    let accepted = tokio::select! {
                        result = connection.accept() => result,
                        Some((code, applied)) = goaway_rx.recv() => {
                            match code {
                                None => connection.graceful_shutdown(),
                                Some(code) => connection.abrupt_shutdown(code),
                            }
                            let _ = applied.send(());
                            continue;
                        }
                    };
                    let Some(result) = accepted else {
                        break;
                    };
                    let (request, mut send_response) = match result {
                        Ok(r) => r,
                        Err(e) => {
                            if e.is_go_away() {
                                rec.push(UpEvent::PeerGoaway {
                                    conn,
                                    code: e.reason().unwrap_or(Reason::NO_ERROR),
                                });
                            }
                            break;
                        }
                    };
                    let stream_id: u32 = send_response.stream_id().into();
                    let header = |name: &str| {
                        request
                            .headers()
                            .get(name)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v.to_string())
                    };
                    let content_length = header("content-length");
                    let transfer_encoding = header("transfer-encoding");
                    let mut body = request.into_parts().1;
                    let headers_eos = body.is_end_stream();
                    rec.push(UpEvent::ReqHeaders {
                        conn,
                        stream: stream_id,
                        headers_eos,
                        content_length: content_length.as_deref().and_then(|v| v.parse().ok()),
                        transfer_encoding: transfer_encoding.clone(),
                    });

                    let idx = counter.fetch_add(1, Ordering::SeqCst);
                    let step = script.get(idx).cloned();
                    let sr = H2StreamRecorder {
                        rec: rec.clone(),
                        conn,
                        stream: stream_id,
                    };
                    let goaway = goaway_tx.clone();
                    tokio::spawn(async move {
                        match step {
                            Some(H2UpstreamStep::Ok200) => {
                                let response = Response::builder().status(200).body(()).unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    let _ = send_stream.send_data(Bytes::from_static(b"ok"), true);
                                }
                            }
                            Some(H2UpstreamStep::Ok200Linger) => {
                                let response = Response::builder().status(200).body(()).unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    let _ = send_stream.send_data(Bytes::from_static(b"ok"), true);
                                }
                                // Hold the request stream so no RST_STREAM is
                                // sent for the still-open request half.
                                let _body = body;
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                            }
                            Some(H2UpstreamStep::HeaderThenReset(gate)) => {
                                let response = Response::builder().status(200).body(()).unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    send_stream.send_data(Bytes::from_static(b"pa"), false).ok();
                                    // Released once the test has read that
                                    // partial body downstream. The timeout is
                                    // only so a regression surfaces as the
                                    // test's own assertion rather than a hang.
                                    let _ = tokio::time::timeout(
                                        Duration::from_secs(10),
                                        gate.notified(),
                                    )
                                    .await;
                                    send_stream.send_reset(Reason::INTERNAL_ERROR);
                                }
                            }
                            Some(H2UpstreamStep::EchoRequestEos) => {
                                let _ = sr.drain(&mut body).await;
                                let response = Response::builder()
                                    .status(200)
                                    .header("x-headers-eos", if headers_eos { "1" } else { "0" })
                                    .body(())
                                    .unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    let _ = send_stream.send_data(Bytes::from_static(b"ok"), true);
                                }
                            }
                            Some(H2UpstreamStep::EchoRequestFraming) => {
                                let body_len = sr.drain(&mut body).await;
                                let response = Response::builder()
                                    .status(200)
                                    .header("x-headers-eos", if headers_eos { "1" } else { "0" })
                                    .header(
                                        "x-req-content-length",
                                        content_length.as_deref().unwrap_or("none"),
                                    )
                                    .header(
                                        "x-req-transfer-encoding",
                                        transfer_encoding.as_deref().unwrap_or("none"),
                                    )
                                    .header("x-req-body-len", body_len.to_string())
                                    .body(())
                                    .unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    let _ = send_stream.send_data(Bytes::from_static(b"ok"), true);
                                }
                            }
                            Some(H2UpstreamStep::Hang) => {
                                // Read and record whatever arrives (a prompt
                                // downstream terminate may mean nothing but a
                                // RST_STREAM does), then park without
                                // responding.
                                let _ = sr.drain(&mut body).await;
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                            }
                            Some(H2UpstreamStep::GoawayThenHoldThenOk200 { applied, release }) => {
                                let (applied_tx, applied_rx) = tokio::sync::oneshot::channel();
                                let _ = goaway.send((None, applied_tx));
                                // Once the acknowledgement arrives the GOAWAY
                                // is in the connection's frame queue; the
                                // response frames below are queued after it,
                                // so the wire order is fixed.
                                let _ = applied_rx.await;
                                // Announced only AFTER the accept loop
                                // acknowledged the shutdown call, so a test
                                // waiting on this knows the GOAWAY is ahead of
                                // everything this step writes later.
                                applied.notify_one();
                                let _ = sr.drain(&mut body).await;
                                // Park with the stream open. The timeout is
                                // only so a test that panicked before
                                // releasing does not leak this task for the
                                // rest of the run.
                                let _ = tokio::time::timeout(
                                    Duration::from_secs(30),
                                    release.notified(),
                                )
                                .await;
                                let response = Response::builder().status(200).body(()).unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    let _ = send_stream.send_data(Bytes::from_static(b"ok"), true);
                                }
                            }
                            Some(H2UpstreamStep::AbruptGoaway(code)) => {
                                let (applied_tx, applied_rx) = tokio::sync::oneshot::channel();
                                let _ = goaway.send((Some(code), applied_tx));
                                let _ = applied_rx.await;
                                // Never respond: the error GOAWAY takes the
                                // whole connection with it.
                            }
                            None => {}
                        }
                    });
                }
                rec.push(UpEvent::ConnClosed { conn });
            });
        }
    });
    ExercisedUpstream {
        port,
        requests: counter_ret,
        rec: rec_ret,
        skipped: Cell::new(false),
    }
}

/// A scripted H1 upstream.
pub enum UpstreamStep {
    /// Read one request, write this exact byte string, keep the connection.
    Respond(&'static [u8]),
    /// Read one request, then close without writing anything.
    CloseWithoutResponse,
    /// Read one request, write this exact byte string, wait to be RELEASED, then
    /// CLOSE the connection -- WITHOUT ever consuming the request body.
    ///
    /// [`Self::Respond`] keeps the connection and drains the body, so the
    /// proxy's request-body writes keep succeeding for as long as the client
    /// uploads. This step is the only way to make the proxy's NEXT upstream body
    /// write fail while the response it already holds is complete, which is what
    /// drives `proxy_handle_upstream` to set both `response_done` and
    /// `request_done`, return, and drop the receiving end of the body pipe. The
    /// downstream half of the duplex loop then observes `tx.is_closed()`, and
    /// what it owes the application there is
    /// `single::h1_upstream_gone_mid_upload_reports_exactly_one_abandoned_event`'s
    /// claim.
    ///
    /// The gate is not a nicety, it is what makes that test deterministic.
    /// Closing a socket that still holds unread request-body bytes makes the
    /// kernel send RST, and an RST DISCARDS whatever the peer had not yet read --
    /// including this very response. Ungated, the proxy would sometimes see a
    /// read error instead of the complete response and take the upstream-error
    /// path, which delivers no `Abandoned` event at all. The test releases the
    /// gate only after its own client has read the whole response, which
    /// (response tasks are forwarded downstream only after `response_done` is
    /// set) proves the proxy is past that point.
    RespondThenClose(&'static [u8], Arc<Notify>),
    /// Read one request (headers only), then hang forever.
    Hang,
    /// Read one request (headers only), then never respond -- but keep READING
    /// until the proxy closes its half of the connection.
    ///
    /// The H1 manifestation of "the proxy cancelled the upstream leg" is the
    /// connection going away, and a [`Self::Hang`] upstream that is parked in a
    /// `sleep` never notices it: no `PeerHalfClose`, no `ConnClosed`, nothing to
    /// wait for. This step is what lets an H1 upstream make the same claim the
    /// H2 upstream makes with `PeerReset`.
    HangObservingClose,
    /// Read one request, consume its whole body, and only THEN write this
    /// response.
    ///
    /// [`Self::Respond`] answers off the request line alone, which is right for
    /// tests about the response but useless for a test about what the proxy put
    /// on the request wire: the proxy may be finished downstream before the
    /// upstream ever read the body. Deferring the response makes the recorded
    /// body a precondition of the client seeing its 200.
    RespondAfterBody(&'static [u8]),
    /// Write this response immediately, then keep reading raw bytes for a short
    /// grace window, recording each read as a `ReqData` event.
    ///
    /// For a request the proxy declared bodyless (no `Content-Length`, no
    /// `Transfer-Encoding`) there is no body framing to parse, so
    /// `consume_h1_request_body` returns without reading and stray body bytes
    /// would be silently mis-parsed as the next request's headers. This step
    /// makes "not one request body byte reached the upstream" a statement about
    /// recorded events rather than about a raw byte capture.
    RespondThenRecordExtra(&'static [u8]),
}

/// For each accepted connection it serves requests sequentially; the k-th
/// request overall (0-based, across connections) is answered with
/// `script[k]`, then the behavior in the script entry runs.
pub fn spawn_scripted_upstream(script: Vec<UpstreamStep>) -> ExercisedUpstream {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_ret = counter.clone();
    let rec = Recorder::new();
    let rec_ret = rec.clone();
    let listener = RT.block_on(async { TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let port = listener.local_addr().unwrap().port();
    let script = Arc::new(script);

    RT.spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };
            let conn = rec.accept_conn();
            let counter = counter.clone();
            let script = script.clone();
            let rec = rec.clone();
            tokio::spawn(async move {
                serve_scripted_h1_connection(&mut stream, conn, &rec, &counter, &script).await;
                rec.push(UpEvent::ConnClosed { conn });
            });
        }
    });
    ExercisedUpstream {
        port,
        requests: counter_ret,
        rec: rec_ret,
        skipped: Cell::new(false),
    }
}

/// Serve one H1 connection: for every request, record its framing, run the
/// script entry, then consume that request's body (recording it) so the
/// connection is left at a message boundary for the next one.
///
/// The script entry runs BEFORE the body is consumed, deliberately: a
/// `Respond` upstream answers as soon as it has the request line, exactly as
/// it did before the recorder existed, so no test's timing changed.
/// [`UpstreamStep::RespondAfterBody`] opts out of that ordering.
async fn serve_scripted_h1_connection(
    stream: &mut TcpStream,
    conn: u32,
    rec: &Recorder,
    counter: &AtomicUsize,
    script: &[UpstreamStep],
) {
    let mut pending: Vec<u8> = Vec::new();
    let mut stream_no = 0u32;
    loop {
        // Read until the end of the request headers.
        let header_end = loop {
            if let Some(at) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
            match read_more(stream, &mut pending).await {
                ReadMore::Data => {}
                ReadMore::Eof => {
                    rec.push(UpEvent::PeerHalfClose { conn });
                    return;
                }
                ReadMore::Reset => {
                    rec.push(UpEvent::PeerConnReset { conn });
                    return;
                }
                ReadMore::Failed => return,
            }
        };
        let head = String::from_utf8_lossy(&pending[..header_end]).to_string();
        pending.drain(..header_end);
        let content_length = h1_header_value(&head, "content-length").and_then(|v| v.parse().ok());
        let transfer_encoding = h1_header_value(&head, "transfer-encoding");
        let stream_id = stream_no;
        stream_no += 1;
        rec.push(UpEvent::ReqHeaders {
            conn,
            stream: stream_id,
            // No length and no transfer coding: nothing may follow.
            headers_eos: content_length.unwrap_or(0) == 0 && transfer_encoding.is_none(),
            content_length,
            transfer_encoding: transfer_encoding.clone(),
        });

        let idx = counter.fetch_add(1, Ordering::SeqCst);
        let mut deferred: Option<&'static [u8]> = None;
        match script.get(idx) {
            Some(UpstreamStep::Respond(bytes)) => {
                if stream.write_all(bytes).await.is_err() {
                    return;
                }
            }
            Some(UpstreamStep::CloseWithoutResponse) => return,
            Some(UpstreamStep::RespondThenClose(bytes, gate)) => {
                let _ = stream.write_all(bytes).await;
                // The timeout is only so a test that panicked before releasing
                // the gate does not leak this task for the rest of the run.
                let _ = tokio::time::timeout(Duration::from_secs(30), gate.notified()).await;
                // Returning drops the socket, so the peer's next write to it
                // fails. The body is deliberately left unconsumed.
                return;
            }
            Some(UpstreamStep::Hang) => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                return;
            }
            Some(UpstreamStep::HangObservingClose) => {
                let mut scratch: Vec<u8> = Vec::new();
                loop {
                    match read_more(stream, &mut scratch).await {
                        ReadMore::Data => scratch.clear(),
                        ReadMore::Eof => {
                            rec.push(UpEvent::PeerHalfClose { conn });
                            return;
                        }
                        ReadMore::Reset => {
                            rec.push(UpEvent::PeerConnReset { conn });
                            return;
                        }
                        ReadMore::Failed => return,
                    }
                }
            }
            Some(UpstreamStep::RespondAfterBody(bytes)) => deferred = Some(bytes),
            Some(UpstreamStep::RespondThenRecordExtra(bytes)) => {
                if stream.write_all(bytes).await.is_err() {
                    return;
                }
                let mut scratch = vec![0u8; 16384];
                let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
                loop {
                    match tokio::time::timeout_at(deadline, stream.read(&mut scratch)).await {
                        Ok(Ok(0)) => {
                            rec.push(UpEvent::PeerHalfClose { conn });
                            return;
                        }
                        Ok(Ok(n)) => rec.push(UpEvent::ReqData {
                            conn,
                            stream: stream_id,
                            len: n,
                            end_stream: false,
                        }),
                        Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                            rec.push(UpEvent::PeerConnReset { conn });
                            return;
                        }
                        Ok(Err(_)) => return,
                        Err(_) => return,
                    }
                }
            }
            None => return,
        }

        if !consume_h1_request_body(
            stream,
            &mut pending,
            conn,
            stream_id,
            rec,
            content_length,
            transfer_encoding.as_deref(),
        )
        .await
        {
            return;
        }

        if let Some(bytes) = deferred {
            if stream.write_all(bytes).await.is_err() {
                return;
            }
        }
    }
}

enum ReadMore {
    Data,
    /// The peer half-closed (FIN).
    Eof,
    /// The peer reset the connection (`ECONNRESET`).
    ///
    /// Split out of [`Self::Failed`] because RST vs FIN is a wire fact the
    /// bodyless scenarios assert: the proxy abandoning an H1 leg resets, and a
    /// harness that folds that into a generic failure cannot distinguish the
    /// teardown from the upstream's own recording window running out.
    Reset,
    Failed,
}

async fn read_more(stream: &mut TcpStream, pending: &mut Vec<u8>) -> ReadMore {
    let mut buf = [0u8; 16384];
    match stream.read(&mut buf).await {
        Ok(0) => ReadMore::Eof,
        Ok(n) => {
            pending.extend_from_slice(&buf[..n]);
            ReadMore::Data
        }
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => ReadMore::Reset,
        Err(_) => ReadMore::Failed,
    }
}

/// The value of `name` in a raw H1 header block, lowercased and trimmed.
fn h1_header_value(head: &str, name: &str) -> Option<String> {
    head.lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.trim().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim().to_lowercase())
}

/// Consume exactly one request body off the connection, recording every piece
/// of it. Returns false if the connection died first.
async fn consume_h1_request_body(
    stream: &mut TcpStream,
    pending: &mut Vec<u8>,
    conn: u32,
    stream_id: u32,
    rec: &Recorder,
    content_length: Option<u64>,
    transfer_encoding: Option<&str>,
) -> bool {
    if transfer_encoding.is_some_and(|te| te.contains("chunked")) {
        loop {
            let Some(size_line) = read_h1_line(stream, pending, conn, rec).await else {
                return false;
            };
            let size = usize::from_str_radix(size_line.split(';').next().unwrap_or("").trim(), 16)
                .unwrap_or(0);
            if size == 0 {
                rec.push(UpEvent::ReqData {
                    conn,
                    stream: stream_id,
                    len: 0,
                    end_stream: true,
                });
                // Trailer section: fields, then a blank line.
                let mut trailers = 0usize;
                loop {
                    match read_h1_line(stream, pending, conn, rec).await {
                        Some(line) if line.is_empty() => break,
                        Some(_) => trailers += 1,
                        None => return false,
                    }
                }
                if trailers > 0 {
                    rec.push(UpEvent::ReqTrailers {
                        conn,
                        stream: stream_id,
                    });
                }
                return true;
            }
            // The chunk data plus its trailing CRLF.
            while pending.len() < size + 2 {
                match read_more(stream, pending).await {
                    ReadMore::Data => {}
                    ReadMore::Eof => {
                        rec.push(UpEvent::PeerHalfClose { conn });
                        return false;
                    }
                    ReadMore::Reset => {
                        rec.push(UpEvent::PeerConnReset { conn });
                        return false;
                    }
                    ReadMore::Failed => return false,
                }
            }
            pending.drain(..size + 2);
            rec.push(UpEvent::ReqData {
                conn,
                stream: stream_id,
                len: size,
                end_stream: false,
            });
        }
    }

    let Some(length) = content_length.filter(|l| *l > 0) else {
        return true;
    };
    let mut remaining = length as usize;
    while remaining > 0 {
        if pending.is_empty() {
            match read_more(stream, pending).await {
                ReadMore::Data => {}
                ReadMore::Eof => {
                    rec.push(UpEvent::PeerHalfClose { conn });
                    return false;
                }
                ReadMore::Reset => {
                    rec.push(UpEvent::PeerConnReset { conn });
                    return false;
                }
                ReadMore::Failed => return false,
            }
        }
        let take = remaining.min(pending.len());
        pending.drain(..take);
        remaining -= take;
        rec.push(UpEvent::ReqData {
            conn,
            stream: stream_id,
            len: take,
            end_stream: remaining == 0,
        });
    }
    true
}

/// Read one CRLF-terminated line, returning it without the terminator.
async fn read_h1_line(
    stream: &mut TcpStream,
    pending: &mut Vec<u8>,
    conn: u32,
    rec: &Recorder,
) -> Option<String> {
    loop {
        if let Some(at) = pending.windows(2).position(|w| w == b"\r\n") {
            let line = String::from_utf8_lossy(&pending[..at]).to_string();
            pending.drain(..at + 2);
            return Some(line);
        }
        match read_more(stream, pending).await {
            ReadMore::Data => {}
            ReadMore::Eof => {
                rec.push(UpEvent::PeerHalfClose { conn });
                return None;
            }
            ReadMore::Reset => {
                rec.push(UpEvent::PeerConnReset { conn });
                return None;
            }
            ReadMore::Failed => return None,
        }
    }
}

pub const OK_KEEPALIVE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";

/// Where a complete HTTP/1.1 response ends inside `buf`, or `None` if more
/// bytes are needed. Understands the two framings this file's proxy ever
/// produces downstream: `Content-Length` and chunked. A response with neither
/// is close-delimited and therefore ends at the header block as far as this
/// helper is concerned.
fn h1_response_end(buf: &[u8]) -> Option<usize> {
    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    let headers = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();

    if headers.contains("transfer-encoding: chunked") {
        // The terminating chunk (plus its -- here always empty -- trailer
        // section) closes the message.
        let terminator = buf[header_end..]
            .windows(5)
            .position(|w| w == b"0\r\n\r\n")?;
        return Some(header_end + terminator + 5);
    }

    let content_length = headers.split("content-length:").nth(1).map(|rest| {
        rest.split("\r\n")
            .next()
            .unwrap_or("")
            .trim()
            .parse::<usize>()
            .expect("content-length must parse")
    });
    match content_length {
        Some(len) if buf.len() >= header_end + len => Some(header_end + len),
        Some(_) => None,
        None => Some(header_end),
    }
}

/// Read exactly ONE complete HTTP/1.1 response off `stream`, consuming
/// whatever is already sitting in `pending` first and leaving any surplus
/// there for the next call.
///
/// Reading to a *marker* instead (as `raw_h1_roundtrip` does) is what makes a
/// follow-up read on the same connection meaningless: response #1's body is
/// still in the socket, so the "next response" a test reads is really the tail
/// of the previous one. Every keepalive probe in this file must go through
/// this function.
pub async fn read_one_h1_response(stream: &mut TcpStream, pending: &mut Vec<u8>) -> String {
    let mut buf = vec![0u8; 16384];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(end) = h1_response_end(pending) {
            let response = String::from_utf8_lossy(&pending[..end]).to_string();
            pending.drain(..end);
            return response;
        }
        let n = tokio::time::timeout_at(deadline, stream.read(&mut buf))
            .await
            .expect("timed out waiting for a complete response")
            .unwrap();
        assert!(
            n > 0,
            "connection closed before a complete response arrived: {:?}",
            String::from_utf8_lossy(pending)
        );
        pending.extend_from_slice(&buf[..n]);
    }
}

/// Write `request` and read one COMPLETE response back, so the connection is
/// left exactly at a message boundary and a follow-up request on it is a real
/// keepalive probe.
pub async fn h1_request_response(
    stream: &mut TcpStream,
    pending: &mut Vec<u8>,
    request: &str,
) -> String {
    stream.write_all(request.as_bytes()).await.unwrap();
    read_one_h1_response(stream, pending).await
}

/// A raw H1 client on one connection: connect, write `first`, then read
/// (with a 10s deadline) until the bytes read so far contain `expect`.
/// Returns the still-open connection and everything read from it, so the
/// caller can keep driving the connection (e.g. to probe reuse).
///
/// NOTE: this stops at a marker, so the connection is generally NOT left at a
/// message boundary. It is fine for tests that only inspect the response (or
/// that expect the connection to be closed next), but a test that issues a
/// SECOND request on the returned connection must use `read_one_h1_response`
/// instead -- see `h1_request_response`.
pub async fn raw_h1_roundtrip(addr: &str, first: &[u8], expect: &[u8]) -> (TcpStream, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(first).await.unwrap();
    let mut collected = Vec::new();
    let mut buf = vec![0u8; 16384];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !collected.windows(expect.len()).any(|w| w == expect) {
        let n = tokio::time::timeout_at(deadline, stream.read(&mut buf))
            .await
            .expect("timed out waiting for expected response bytes")
            .unwrap();
        assert!(n > 0, "connection closed before expected bytes arrived");
        collected.extend_from_slice(&buf[..n]);
    }
    (stream, collected)
}

/// How long after the upstream demonstrably held the request the terminate may
/// take to be complete downstream.
///
/// This is the UPPER half of a promptness claim; the lower half is the recorded
/// `ReqHeaders` instant it is measured from. Without that anchor the assertion
/// cannot fail: an elapsed time measured from an event that never happened is
/// not a latency at all.
pub const TERMINATE_BUDGET: Duration = Duration::from_secs(2);

/// How long the waits that lead up to the [`TERMINATE_BUDGET`] assertion are
/// allowed to run.
///
/// It is the budget plus one second of slack, and it exists because a generous
/// outer wait quietly takes the budget's job away: with a 10s wait, a
/// regression that finished in 4s failed the RUN with "the connection neither
/// closed nor errored" or with the completion wait's own message, and a
/// regression that finished in 3s was the only kind the 2s budget itself ever
/// caught. Deriving the outer waits from the budget puts the failure back
/// where the claim is, and the 1s of slack is what still lets a late-but-
/// finished pump report its measured elapsed time instead of a bare timeout.
pub const TERMINATE_WAIT: Duration = Duration::from_secs(3);

/// Wait until the scripted upstream has the request, then read one complete
/// downstream response, require the connection to reach EOF, and bound how long
/// the whole thing took from the moment the upstream had the request.
///
/// Time-to-EOF is the promptness property that is actually observable for a
/// terminate. The arrival of the local reply is NOT: the application flushes its
/// 403 from inside the hook, so the client holds the complete response whether
/// or not the pump dropped its sibling upstream future -- an assertion on the
/// reply's latency stays green even with the cancellation removed. The
/// connection, by contrast, only closes once the pump actually finished, which
/// it cannot do while awaiting a hung upstream.
///
/// Reading through `read_one_h1_response` (rather than to a marker) also leaves
/// the connection at a message boundary, so `pending` being empty afterwards is
/// a real assertion that nothing -- e.g. a generic 502 -- followed the reply.
pub async fn terminate_reply_and_eof(addr: &str, request: &str, rec: &Recorder) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();

    // Lower bound: the upstream really did receive this request, so everything
    // measured from here is measured against a hung upstream that exists.
    expect_ok(
        rec.wait_for(
            "the upstream to receive the request headers",
            Duration::from_secs(10),
            |e| matches!(e, UpEvent::ReqHeaders { .. }),
        )
        .await,
    );
    let upstream_had_it = rec
        .first_seen(|e| matches!(e, UpEvent::ReqHeaders { .. }))
        .expect("the wait above returned, so the event is recorded");

    let mut pending = Vec::new();
    let text = read_one_h1_response(&mut stream, &mut pending).await;
    assert!(
        pending.is_empty(),
        "unexpected bytes after the terminate reply: {:?}",
        String::from_utf8_lossy(&pending)
    );

    let mut buf = vec![0u8; 4096];
    match tokio::time::timeout(TERMINATE_WAIT, stream.read(&mut buf)).await {
        Ok(Ok(0)) => {}  // clean close: the pump finished
        Ok(Err(_)) => {} // reset: also acceptable
        Ok(Ok(n)) => panic!(
            "a second response followed the terminate: {:?}",
            String::from_utf8_lossy(&buf[..n])
        ),
        Err(_) => panic!(
            "the connection neither closed nor errored within {TERMINATE_WAIT:?}, which \
             is already past the {TERMINATE_BUDGET:?} terminate budget: the terminate is \
             still waiting for the hung upstream"
        ),
    }

    // Upper bound.
    let elapsed = upstream_had_it.elapsed();
    assert!(
        elapsed < TERMINATE_BUDGET,
        "the terminate took {elapsed:?} to complete downstream after the upstream \
         had the request, which is longer than the {TERMINATE_BUDGET:?} budget: it \
         is waiting on the upstream rather than cancelling it.\n{}",
        rec.dump()
    );
    text
}

/// Send `POST /` with `content-length: 0` over the h2c listener WITHOUT
/// END_STREAM on HEADERS, then close the request stream with an empty
/// END_STREAM DATA frame. Returns (status, `x-eos-events`, `x-headers-eos`,
/// [`CompletionRecord`]).
///
/// The record is what carries the NEGATIVE half of the end-of-stream claim: the
/// echoed `x-eos-events` header merges `Complete` and `Abandoned`, so it stays
/// `1` even if this normally-ended body were reported as abandoned.
pub async fn h2_cl0_no_end_stream_request(
    upstream_port: u16,
    h2_upstream: bool,
) -> (u16, String, Option<String>, CompletionRecord) {
    let ports = init();
    let (id, completion) = observe_completion();
    let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
    let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut h2 = h2.ready().await.unwrap();

    let mut builder = http::Request::builder()
        .method("POST")
        .uri("http://t/")
        .header("x-port", upstream_port.to_string())
        .header("x-observe-completion", id.into_header())
        .header("content-length", "0");
    if h2_upstream {
        builder = builder.header("x-h2", "1");
    }
    let req = builder.body(()).unwrap();

    // `false`: HEADERS without END_STREAM despite `content-length: 0`.
    let (response, mut body) = h2.send_request(req, false).unwrap();
    // The real transport EOS, as an empty DATA frame.
    body.send_data(Bytes::new(), true).unwrap();

    let response = tokio::time::timeout(Duration::from_secs(10), response)
        .await
        .expect("timed out waiting for the response")
        .unwrap();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
    };
    let status = response.status().as_u16();
    let eos_events = header("x-eos-events").expect("response_filter always sets x-eos-events");
    let echoed = header("x-headers-eos");
    let record = completion
        .wait(
            Duration::from_secs(10),
            "the request never reached ProxyHttp::logging even though the client held a \
             complete response: the pump is still parked on the downstream read side",
        )
        .await;
    (status, eos_events, echoed, record)
}

pub fn res_status(res: std::result::Result<reqwest::Response, reqwest::Error>) -> u16 {
    match res {
        Ok(r) => r.status().as_u16(),
        // A connection-level failure with no response counts as 0.
        Err(_) => 0,
    }
}

/// Captures the exact bytes the proxy puts on the wire for ONE upstream
/// connection. Once `respond_after` appears in the capture the listener keeps
/// reading for a short grace period -- so anything the proxy sends afterwards
/// (e.g. forwarded trailer fields) still lands in the capture -- then answers
/// 200 and closes.
pub fn spawn_recording_upstream(
    respond_after: &'static [u8],
) -> (u16, Arc<std::sync::Mutex<Vec<u8>>>) {
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured_ret = captured.clone();
    let listener = RT.block_on(async { TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let port = listener.local_addr().unwrap().port();

    RT.spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 65536];
        loop {
            let n = match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            let seen = {
                let mut guard = captured.lock().unwrap();
                guard.extend_from_slice(&buf[..n]);
                guard
                    .windows(respond_after.len())
                    .any(|w| w == respond_after)
            };
            if seen {
                break;
            }
        }
        // Grace period: collect whatever else is already in flight, so that a
        // regression which DID forward trailers would be visible.
        loop {
            match tokio::time::timeout(Duration::from_millis(300), stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => captured.lock().unwrap().extend_from_slice(&buf[..n]),
                _ => break,
            }
        }
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await;
    });
    (port, captured_ret)
}
