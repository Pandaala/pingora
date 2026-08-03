// Self-contained integration tests for the canonical request-body transport
// interface. Deliberately does NOT use tests/utils: that harness needs a
// local openresty. Every upstream here is a scripted tokio TCP listener.

use async_trait::async_trait;
use bytes::Bytes;
use h2::Reason;
use http::Response;
use once_cell::sync::Lazy;
use pingora_cache::lock::{CacheKeyLockImpl, CacheLock};
use pingora_cache::{CacheKey, MemCache};
use pingora_core::server::configuration::ServerConf;
use pingora_core::server::Server;
use pingora_core::services::ServiceWithDependents;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, Session};
use std::sync::atomic::{AtomicI8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};

pub static RT: Lazy<Runtime> =
    Lazy::new(|| Builder::new_multi_thread().enable_all().build().unwrap());

/// Counts `debug_assert!(false, "Dangling cache lock started!")` panics raised
/// inside the proxy's own connection tasks, which tokio would otherwise
/// swallow. Matching on the message keeps unrelated panics (e.g. another
/// test's failed assertion) from being miscounted. See
/// `terminate_with_cache_enabled_does_not_leave_a_dangling_lock`.
static DANGLING_CACHE_LOCKS: AtomicUsize = AtomicUsize::new(0);
const DANGLING_LOCK_MESSAGE: &str = "Dangling cache lock started!";

static CACHE_BACKEND: Lazy<MemCache> = Lazy::new(MemCache::new);
static CACHE_LOCK: Lazy<Box<CacheKeyLockImpl>> =
    Lazy::new(|| CacheLock::new_boxed(Duration::from_secs(2)));

/// Counts requests that reached `ProxyHttp::logging`, i.e. that the proxy
/// actually FINISHED. A request whose pump parks forever never gets here even
/// though the client may already hold a complete response, so this is the only
/// way to observe the leak in `h2_cl0_never_ending_request_completes`.
static COMPLETED_H1_UPSTREAM: AtomicUsize = AtomicUsize::new(0);
static COMPLETED_H2_UPSTREAM: AtomicUsize = AtomicUsize::new(0);

/// `ctx.eos_events` as of `ProxyHttp::logging`, i.e. the FINAL count. The
/// `x-eos-events` response header cannot serve here: `response_filter` stamps
/// it while the upstream response headers pass through, which is necessarily
/// before the pump could deliver a late end-of-stream event. Written before
/// the matching `COMPLETED_*` counter is bumped, so a reader that observed the
/// bump also observes this.
static FINAL_EOS_H1_UPSTREAM: AtomicUsize = AtomicUsize::new(0);
static FINAL_EOS_H2_UPSTREAM: AtomicUsize = AtomicUsize::new(0);

/// The final error's `retry` flag as `ProxyHttp::logging` sees it, for requests
/// carrying `x-observe-retry`. `-1` = no observation yet, `0`/`1` = the
/// `RetryType::Decided` value, `2` = the undecided `ReusedOnly`. The count is
/// bumped AFTER the flag is written, so a reader that saw the bump also sees the
/// flag.
///
/// This is the ONLY observable for the two forcing points that sit next to
/// `error_while_proxy` and `fail_to_connect`: the retry LOOP would refuse the
/// retry anyway (it re-checks the predicate), so the loop's own behaviour cannot
/// tell those two lines apart from the loop's check. What the application sees
/// on the error it is handed can.
///
/// One slot per test, selected by the header VALUE: the tests in this file run
/// concurrently, and a shared slot would let them read each other's observation.
struct RetryObservation {
    flag: AtomicI8,
    requests: AtomicUsize,
}
static OBSERVED_RETRY_PROXY: RetryObservation = RetryObservation {
    flag: AtomicI8::new(-1),
    requests: AtomicUsize::new(0),
};
static OBSERVED_RETRY_CONNECT: RetryObservation = RetryObservation {
    flag: AtomicI8::new(-1),
    requests: AtomicUsize::new(0),
};

fn retry_observation(slot: &str) -> Option<&'static RetryObservation> {
    match slot {
        "proxy" => Some(&OBSERVED_RETRY_PROXY),
        "connect" => Some(&OBSERVED_RETRY_CONNECT),
        _ => None,
    }
}

#[derive(Default)]
pub struct SeamCtx {
    body_bytes_seen: usize,
    /// How many times the request body filter was invoked with
    /// `end_of_stream`. Echoed back as `x-eos-events` by `response_filter`
    /// so tests can assert the application sees exactly one EOS.
    eos_events: usize,
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
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<pingora_proxy::RequestBodyAction> {
        if end_of_stream {
            ctx.eos_events += 1;
        }
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
            && end_of_stream
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
        _e: Option<&pingora_error::Error>,
        ctx: &mut Self::CTX,
    ) {
        if let Some(observation) = session
            .req_header()
            .headers
            .get("x-observe-retry")
            .and_then(|v| v.to_str().ok())
            .and_then(retry_observation)
        {
            let flag = match _e.map(|e| e.retry) {
                Some(pingora_error::RetryType::Decided(b)) => i8::from(b),
                Some(pingora_error::RetryType::ReusedOnly) => 2,
                None => -1,
            };
            observation.flag.store(flag, Ordering::SeqCst);
            observation.requests.fetch_add(1, Ordering::SeqCst);
        }

        let (eos, completed) = match session
            .req_header()
            .headers
            .get("x-count-completion")
            .and_then(|v| v.to_str().ok())
        {
            Some("h1") => (&FINAL_EOS_H1_UPSTREAM, &COMPLETED_H1_UPSTREAM),
            Some("h2") => (&FINAL_EOS_H2_UPSTREAM, &COMPLETED_H2_UPSTREAM),
            _ => return,
        };
        eos.store(ctx.eos_events, Ordering::SeqCst);
        // Bumped last: the test keys on this and then reads the count above.
        completed.fetch_add(1, Ordering::SeqCst);
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
        _end_of_stream: bool,
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
    fn h1_addr(&self) -> String {
        format!("127.0.0.1:{}", self.h1)
    }
    fn h2c_addr(&self) -> String {
        format!("127.0.0.1:{}", self.h2c)
    }
    fn legacy_addr(&self) -> String {
        format!("127.0.0.1:{}", self.legacy)
    }
}

/// Reserve a free localhost port by binding it and immediately releasing it.
///
/// The window between release and pingora's own bind is why the readiness poll
/// in `start_seam_server` panics loudly rather than sleeping: if anything else
/// takes the port in between, pingora's bind fails and the tests must say so
/// instead of silently talking to a stranger.
fn reserve_port() -> u16 {
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
    /// Send response HEADERS without ending the stream, then reset it.
    HeaderThenReset,
    /// Accept the request stream, read and discard whatever body arrives
    /// (there may be none), then never respond and park forever.
    Hang,
    /// Like [`Self::Hang`], but keep reading the request stream and bump the
    /// counter if the peer RESETS it with `CANCEL`.
    ///
    /// This is what makes the proxy's `client_body.send_reset(CANCEL)` on a
    /// terminate directly observable: without it a test can only see that the
    /// application's own local reply arrived, which it would regardless.
    HangObservingCancel(Arc<AtomicUsize>),
    /// Drain the request body, then respond 200 with `x-headers-eos` set to
    /// whether the request's HEADERS frame carried END_STREAM. Lets a test
    /// observe the exact upstream request framing the proxy chose.
    EchoRequestEos,
    /// Like [`Self::EchoRequestEos`], but reports the FULL upstream request
    /// framing: `x-headers-eos`, `x-req-content-length` (`none` when absent),
    /// `x-req-transfer-encoding` (`none` when absent) and `x-req-body-len`.
    EchoRequestFraming,
    /// Drain the request body into the shared counter, then respond 200.
    ///
    /// Same as [`Self::EchoRequestEos`] except that the number of request body
    /// bytes that actually reached the upstream is observable even when the
    /// proxy's response never makes it back to the client (as on the
    /// `Bodyless` fail-close path).
    CountRequestBody(Arc<AtomicUsize>),
}

/// It accepts TCP connections and, for each one, performs an h2 server
/// handshake and serves streams sequentially; the k-th stream overall
/// (0-based, across connections) is answered with `script[k]`. Returns
/// (port, served-request counter).
pub fn spawn_scripted_h2_upstream(script: Vec<H2UpstreamStep>) -> (u16, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_ret = counter.clone();
    let listener = RT.block_on(async { TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let port = listener.local_addr().unwrap().port();
    let script = Arc::new(script);

    RT.spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };
            let counter = counter.clone();
            let script = script.clone();
            tokio::spawn(async move {
                let mut connection = match h2::server::handshake(stream).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                // Spawn a task per stream so the outer accept() loop keeps
                // driving (and flushing) the shared connection while a
                // stream's response handling (e.g. a delayed reset) is in
                // flight, rather than blocking connection I/O on it.
                while let Some(result) = connection.accept().await {
                    let (request, mut send_response) = match result {
                        Ok(r) => r,
                        Err(_) => return,
                    };
                    let idx = counter.fetch_add(1, Ordering::SeqCst);
                    let step = script.get(idx).cloned();
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
                                let _body = request.into_body();
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                            }
                            Some(H2UpstreamStep::HeaderThenReset) => {
                                let response = Response::builder().status(200).body(()).unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    send_stream.send_data(Bytes::from_static(b"pa"), false).ok();
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                    send_stream.send_reset(Reason::INTERNAL_ERROR);
                                }
                            }
                            Some(H2UpstreamStep::EchoRequestEos) => {
                                let mut body = request.into_body();
                                let headers_eos = body.is_end_stream();
                                while let Some(chunk) = body.data().await {
                                    if chunk.is_err() {
                                        return;
                                    }
                                }
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
                                let header = |name: &str| {
                                    request
                                        .headers()
                                        .get(name)
                                        .and_then(|v| v.to_str().ok())
                                        .unwrap_or("none")
                                        .to_string()
                                };
                                let cl = header("content-length");
                                let te = header("transfer-encoding");
                                let mut body = request.into_body();
                                let headers_eos = body.is_end_stream();
                                let mut body_len = 0usize;
                                while let Some(chunk) = body.data().await {
                                    match chunk {
                                        Ok(chunk) => body_len += chunk.len(),
                                        Err(_) => return,
                                    }
                                }
                                let response = Response::builder()
                                    .status(200)
                                    .header("x-headers-eos", if headers_eos { "1" } else { "0" })
                                    .header("x-req-content-length", cl)
                                    .header("x-req-transfer-encoding", te)
                                    .header("x-req-body-len", body_len.to_string())
                                    .body(())
                                    .unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    let _ = send_stream.send_data(Bytes::from_static(b"ok"), true);
                                }
                            }
                            Some(H2UpstreamStep::CountRequestBody(seen)) => {
                                let mut body = request.into_body();
                                while let Some(chunk) = body.data().await {
                                    match chunk {
                                        Ok(chunk) => {
                                            seen.fetch_add(chunk.len(), Ordering::SeqCst);
                                        }
                                        Err(_) => return,
                                    }
                                }
                                let response = Response::builder().status(200).body(()).unwrap();
                                if let Ok(mut send_stream) =
                                    send_response.send_response(response, false)
                                {
                                    let _ = send_stream.send_data(Bytes::from_static(b"ok"), true);
                                }
                            }
                            Some(H2UpstreamStep::Hang) => {
                                // Read and discard whatever body arrives (a
                                // prompt downstream terminate may mean none
                                // ever does), then park without responding.
                                let mut body = request.into_body();
                                let _ = body.data().await;
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                            }
                            Some(H2UpstreamStep::HangObservingCancel(cancels)) => {
                                let mut body = request.into_body();
                                loop {
                                    match body.data().await {
                                        Some(Ok(_)) => continue,
                                        Some(Err(e)) => {
                                            if e.reason() == Some(Reason::CANCEL) {
                                                cancels.fetch_add(1, Ordering::SeqCst);
                                            }
                                            break;
                                        }
                                        None => break,
                                    }
                                }
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                            }
                            None => {}
                        }
                    });
                }
            });
        }
    });
    (port, counter_ret)
}

#[test]
fn h2_error_no_retry_after_header_sent_on_reused_conn() {
    let ports = init();
    // Request 1 succeeds over h2 and pools the upstream h2 connection.
    // Request 2 reuses it as a new stream; the upstream sends response
    // HEADERS (committing the downstream response, status forwarded to the
    // client), then resets the stream while sending the body.
    //
    // This is a structural invariant pin, not a regression test for a live
    // hole: as classified in pingora-core today, this h2 body-read error
    // (`ReadError`/"while read h2 response body", see
    // `Http2Session::read_response_body` in
    // pingora-core/src/protocols/http/v2/client.rs) is constructed
    // `RetryType::Decided(false)` regardless of connection reuse --
    // retryable classifications (`RetryType::ReusedOnly` or unconditional
    // `true`) are only ever produced while reading a response's own
    // headers, which by definition happens before that response could be
    // committed downstream. So no reachable error path today is retryable
    // after a final response is committed, on either H1 or H2; the guard in
    // the retry loop currently changes no observable behavior. This test
    // asserts the invariant end-to-end anyway, so that if a future change
    // (e.g. an upstream merge) alters error classification and makes a
    // post-commit error retryable, this test -- not just the guard's own
    // unit tests -- catches the regression.
    let (port, counter) = spawn_scripted_h2_upstream(vec![
        H2UpstreamStep::Ok200,
        H2UpstreamStep::HeaderThenReset,
        // If the guard is broken, a third upstream request arrives here.
        H2UpstreamStep::Ok200,
    ]);

    RT.block_on(async {
        let client = reqwest::Client::new();
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(res.text().await.unwrap(), "ok");

        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        // The reset stream must surface as a body read error, not as a
        // silently concatenated retry response.
        assert!(res.text().await.is_err());
    });

    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "a retry after response commit must not reach the upstream"
    );
}

/// An upstream that captures everything it receives for one connection,
/// answers 200 once a complete chunked body (terminated by "0\r\n\r\n") or
/// Content-Length body arrives, then closes.
pub fn spawn_capturing_upstream() -> (u16, Arc<std::sync::Mutex<Vec<u8>>>) {
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
            let done = {
                let mut guard = captured.lock().unwrap();
                guard.extend_from_slice(&buf[..n]);
                guard.windows(5).any(|w| w == b"0\r\n\r\n")
            };
            if done {
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await;
                return;
            }
        }
    });
    (port, captured_ret)
}

#[test]
fn streamed_disposition_rewrites_h1_upstream_framing() {
    let ports = init();
    let (port, captured) = spawn_capturing_upstream();

    RT.block_on(async {
        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-disposition", "streamed")
            .body("hello world!") // reqwest sends Content-Length: 12
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    });

    let bytes = captured.lock().unwrap().clone();
    let text = String::from_utf8_lossy(&bytes).to_lowercase();
    assert!(
        text.contains("transfer-encoding: chunked"),
        "upstream request must be chunked: {text}"
    );
    assert!(
        !text.contains("content-length"),
        "stale client content-length must be removed: {text}"
    );
    assert!(
        text.contains("hello world!"),
        "body bytes must arrive intact: {text}"
    );
}

/// A scripted H1 upstream.
pub enum UpstreamStep {
    /// Read one request, write this exact byte string, keep the connection.
    Respond(&'static [u8]),
    /// Read one request, then close without writing anything.
    CloseWithoutResponse,
    /// Read one request (headers only), then hang forever.
    Hang,
}

/// For each accepted connection it serves requests sequentially; the k-th
/// request overall (0-based, across connections) is answered with
/// `script[k]`, then the behavior in the script entry runs. Returns (port,
/// served-request counter).
pub fn spawn_scripted_upstream(script: Vec<UpstreamStep>) -> (u16, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_ret = counter.clone();
    let listener = RT.block_on(async { TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let port = listener.local_addr().unwrap().port();
    let script = Arc::new(script);

    RT.spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };
            let counter = counter.clone();
            let script = script.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                let mut pending: Vec<u8> = Vec::new();
                loop {
                    // Read until end of request headers.
                    while !contains_header_end(&pending) {
                        let n = match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        pending.extend_from_slice(&buf[..n]);
                    }
                    // This scripted upstream only receives GET requests or
                    // requests whose body it is allowed to ignore; drain
                    // whatever arrived alongside the headers.
                    pending.clear();

                    let idx = counter.fetch_add(1, Ordering::SeqCst);
                    match script.get(idx) {
                        Some(UpstreamStep::Respond(bytes)) => {
                            if stream.write_all(bytes).await.is_err() {
                                return;
                            }
                        }
                        Some(UpstreamStep::CloseWithoutResponse) => return,
                        Some(UpstreamStep::Hang) => {
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                            return;
                        }
                        None => return,
                    }
                }
            });
        }
    });
    (port, counter_ret)
}

fn contains_header_end(buf: &[u8]) -> bool {
    buf.windows(4).any(|w| w == b"\r\n\r\n")
}

const OK_KEEPALIVE: &[u8] =
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
async fn read_one_h1_response(stream: &mut TcpStream, pending: &mut Vec<u8>) -> String {
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
async fn h1_request_response(
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
async fn raw_h1_roundtrip(addr: &str, first: &[u8], expect: &[u8]) -> (TcpStream, Vec<u8>) {
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

/// Read one complete response and then require the connection to reach EOF.
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
async fn terminate_reply_and_eof(addr: &str, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut pending = Vec::new();
    let text = read_one_h1_response(&mut stream, &mut pending).await;
    assert!(
        pending.is_empty(),
        "unexpected bytes after the terminate reply: {:?}",
        String::from_utf8_lossy(&pending)
    );

    let mut buf = vec![0u8; 4096];
    match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
        Ok(Ok(0)) => {}  // clean close: the pump finished
        Ok(Err(_)) => {} // reset: also acceptable
        Ok(Ok(n)) => panic!(
            "a second response followed the terminate: {:?}",
            String::from_utf8_lossy(&buf[..n])
        ),
        Err(_) => panic!(
            "the connection neither closed nor errored within 5s: the terminate is \
             still waiting for the hung upstream"
        ),
    }
    text
}

#[test]
fn h1_terminate_is_prompt_and_skips_generic_errors() {
    let ports = init();
    // The upstream reads the request and then hangs forever. Without
    // select!-driven sibling cancellation the request could not finish
    // until an upstream read timeout (none is configured here).
    let (port, counter) = spawn_scripted_upstream(vec![UpstreamStep::Hang]);

    let text = RT.block_on(async {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             x-terminate-after-bytes: 1\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n"
        );
        terminate_reply_and_eof(&ports.h1_addr(), &request).await
    });

    assert!(
        text.starts_with("HTTP/1.1 403"),
        "local reply expected: {text}"
    );
    assert!(
        !text.contains("502") && !text.contains("500"),
        "terminate must not produce a generic proxy error response: {text}"
    );
    // The upstream's accept-counter increment isn't synchronized with the
    // downstream reply, so it may legitimately still read 0 here; the
    // invariant this test protects is "no second attempt".
    assert!(
        counter.load(Ordering::SeqCst) <= 1,
        "no second upstream attempt"
    );
}

#[test]
fn h1_terminated_connection_is_not_reused() {
    let ports = init();
    let (port, _counter) = spawn_scripted_upstream(vec![UpstreamStep::Hang]);

    RT.block_on(async {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             x-terminate-after-bytes: 1\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n"
        );
        let (mut stream, _collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"denied").await;

        // The request body was never completed; the proxy must have marked
        // this connection non-reusable. A pipelined second request must not
        // be answered: expect clean EOF (or an error), never a response.
        let second = b"GET / HTTP/1.1\r\nHost: t\r\n\r\n";
        let _ = stream.write_all(second).await;
        let mut buf = vec![0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
            Ok(Ok(0)) => {}  // clean close: correct
            Ok(Err(_)) => {} // reset: also acceptable
            Ok(Ok(n)) => panic!(
                "terminated H1 connection served a second request: {:?}",
                String::from_utf8_lossy(&buf[..n])
            ),
            Err(_) => panic!("connection neither closed nor errored within 5s"),
        }
    });
}

#[test]
fn h1_trailer_bearing_request_can_be_rejected() {
    let ports = init();
    let (port, counter) = spawn_scripted_upstream(vec![UpstreamStep::Hang]);

    let collected = RT.block_on(async {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             x-reject-trailers: 1\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n0\r\nx-checksum: ok\r\n\r\n"
        );
        let (mut stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"HTTP/1.1 400").await;

        // A terminate at the TRAILER point must also close the downstream
        // connection. This is the discriminating half: at that point the body
        // is already done, so the H1 session's default
        // `close_on_response_before_downstream_finish` safety net does NOT
        // fire -- only the pump's own `set_keepalive(None)` does.
        let second = b"GET / HTTP/1.1\r\nHost: t\r\n\r\n";
        let _ = stream.write_all(second).await;
        let mut buf = vec![0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
            Ok(Ok(0)) => {}  // clean close: correct
            Ok(Err(_)) => {} // reset: also acceptable
            Ok(Ok(n)) => panic!(
                "connection terminated at the trailer point served a second request: {:?}",
                String::from_utf8_lossy(&buf[..n])
            ),
            Err(_) => panic!("connection neither closed nor errored within 5s"),
        }
        collected
    });

    let text = String::from_utf8_lossy(&collected);
    assert!(
        text.starts_with("HTTP/1.1 400"),
        "trailer rejection expected: {text}"
    );
    // The trailer fact arrives with body EOF; the single hung upstream
    // attempt may or may not have received earlier body bytes, but no
    // SECOND attempt may exist.
    assert!(counter.load(Ordering::SeqCst) <= 1);
}

/// This exercises terminate over an H2 DOWNSTREAM session (the h2c listener):
/// mid-body termination on one stream produces the local 403 on that stream
/// only, and a later stream on the same downstream connection still works.
/// Neither request sets `x-h2`, so the proxy talks H1 to the upstream(s) --
/// this does NOT drive the `proxy_h2` upstream pump's terminate arms; see
/// `h2_upstream_terminate_resets_stream` for that.
#[test]
fn h2c_downstream_terminate_keeps_connection() {
    let ports = init();
    // Stream A and stream B each get their own scripted upstream (separate
    // ports), so B's success can never race against / accidentally draw
    // A's `Hang` script entry.
    let (port_a, _counter_a) = spawn_scripted_upstream(vec![UpstreamStep::Hang]);
    let (port_b, _counter_b) = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);

    RT.block_on(async {
        let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
        let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        // Stream A: terminated mid-body by the application. Its upstream
        // is left hanging so this test cannot pass by accident if the
        // terminate path silently waited on the upstream.
        let req = http::Request::builder()
            .method("POST")
            .uri("http://t/")
            .header("x-port", port_a.to_string())
            .header("x-terminate-after-bytes", "1")
            .body(())
            .unwrap();
        let (response_a, mut body_a) = h2.send_request(req, false).unwrap();
        body_a
            .send_data(Bytes::from_static(b"hello"), false)
            .unwrap();
        let resp_a = response_a.await.unwrap();
        assert_eq!(resp_a.status(), 403);

        // Stream B on the SAME downstream connection, against its own
        // upstream, must still work.
        let mut h2 = h2.ready().await.unwrap();
        let req = http::Request::builder()
            .method("GET")
            .uri("http://t/")
            .header("x-port", port_b.to_string())
            .body(())
            .unwrap();
        let (response_b, _) = h2.send_request(req, true).unwrap();
        let resp_b = response_b.await.unwrap();
        assert_eq!(
            resp_b.status(),
            200,
            "the downstream H2 connection must remain usable after a terminate"
        );
    });
}

/// This drives the `proxy_h2` pump's terminate arms directly: the
/// DOWNSTREAM session is H1 (matching the other H1 tests in this file), but the
/// request carries `x-h2: 1` so the proxy selects an H2 UPSTREAM peer (see
/// `SeamProxy::upstream_peer`), which routes proxying through
/// `proxy_h2::bidirection_down_to_up` / `send_body_to2` instead of the H1
/// upstream pump.
///
/// Three separable things are asserted, each with its own observable:
/// - the RESET this test is named for is observed AT THE UPSTREAM: the scripted
///   step counts an inbound RST_STREAM(CANCEL) on its request stream. Note that
///   the explicit `client_body.send_reset(CANCEL)` in `proxy_down_to_up`'s
///   result match and simply DROPPING the `SendStream` put the identical frame
///   on the wire, so this pins the observable contract ("the upstream request
///   stream is reset, and the upstream is not left working on a request nobody
///   will read") rather than that one line;
/// - the pump actually FINISHED (rather than parking on the hung upstream) is
///   observed as time-to-EOF on the downstream connection, not as the arrival of
///   the local reply -- the application flushes that reply itself, so it arrives
///   either way;
/// - no generic proxy error follows it, checked over a complete message with the
///   connection left at a message boundary.
#[test]
fn h2_upstream_terminate_resets_stream() {
    let ports = init();
    let cancels = Arc::new(AtomicUsize::new(0));
    let (port, counter) =
        spawn_scripted_h2_upstream(vec![H2UpstreamStep::HangObservingCancel(cancels.clone())]);

    let text = RT.block_on(async {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\nx-h2: 1\r\n\
             x-terminate-after-bytes: 1\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n"
        );
        let text = terminate_reply_and_eof(&ports.h1_addr(), &request).await;

        // The reset travels to the upstream asynchronously, so poll for it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while cancels.load(Ordering::SeqCst) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the upstream request stream was never reset with CANCEL"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        text
    });

    assert!(
        text.starts_with("HTTP/1.1 403"),
        "local reply expected: {text}"
    );
    assert!(
        !text.contains("502") && !text.contains("500"),
        "terminate must not produce a generic proxy error response: {text}"
    );
    // The scripted upstream's accept-counter increment isn't synchronized
    // with the downstream reply, so it may legitimately still read 0 here;
    // what matters is that a SECOND stream is never opened.
    assert!(
        counter.load(Ordering::SeqCst) <= 1,
        "no second upstream stream"
    );
}

/// Downstream hygiene for the H1-downstream -> H2-upstream shape. The pump
/// is selected by the UPSTREAM protocol (`x-h2: 1` routes through
/// `proxy_h2`), but the connection that must not be reused is the H1
/// DOWNSTREAM one: the application refused its request body, so keepalive
/// reuse would first have to drain bytes it deliberately did not read.
/// Mirrors `h1_terminated_connection_is_not_reused`.
///
/// The terminate is driven from `request_trailer_filter`, i.e. AFTER the
/// downstream body reached EOF. That is what makes this test discriminating:
/// a mid-body terminate is already covered by the H1 session's default
/// `close_on_response_before_downstream_finish`, which clears keepalive
/// whenever a response is written before the body is done. At the trailer
/// point that safety net does not fire, so the only thing standing between
/// the client and a reused connection is the pump's own reuse verdict: with
/// `proxy_h2` reporting reuse the follow-up request on the same connection
/// is answered 200, and it must not be.
#[test]
fn h1_downstream_h2_upstream_terminate_is_not_reused() {
    let ports = init();
    let (h2_port, _counter) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::Hang]);
    // The follow-up request targets a working H1 upstream, so it being
    // answered would be entirely the proxy's doing.
    let (h1_port, _counter_b) = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);

    RT.block_on(async {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {h2_port}\r\nx-h2: 1\r\n\
             x-reject-trailers: 1\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n0\r\nx-checksum: ok\r\n\r\n"
        );
        let (mut stream, _collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"HTTP/1.1 400").await;

        let second = format!("GET / HTTP/1.1\r\nHost: t\r\nx-port: {h1_port}\r\n\r\n");
        let _ = stream.write_all(second.as_bytes()).await;

        let mut buf = vec![0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
            Ok(Ok(0)) => {}  // clean close: correct
            Ok(Err(_)) => {} // reset: also acceptable
            Ok(Ok(n)) => panic!(
                "terminated H1 downstream connection served a second request: {:?}",
                String::from_utf8_lossy(&buf[..n])
            ),
            Err(_) => panic!("connection neither closed nor errored within 5s"),
        }
    });
}

/// An H2 downstream request that declares `Content-Length: 0` but does NOT
/// carry END_STREAM on HEADERS is not bodyless (design 4.3): the tightened
/// `is_body_done()` reports the transport fact, while `is_body_empty()`
/// still infers emptiness from the header. Each pump must send exactly one
/// upstream EOS and deliver exactly one EOS event to the application.
///
/// H1-upstream leg: the bodyless prelude used to fire an immediate
/// `(None, end)` event and the duplex loop fired a second one on the
/// client's real EOS, so `request_body_filter` saw TWO end-of-stream events.
#[test]
fn h2_cl0_without_end_stream_h1_upstream_sends_one_eos() {
    init();
    let (port, _counter) = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);

    RT.block_on(async {
        let (status, eos_events, _headers_eos) = h2_cl0_no_end_stream_request(port, false).await;
        assert_eq!(status, 200);
        assert_eq!(
            eos_events, "1",
            "the application must see exactly one end-of-stream event"
        );
    });
}

/// H2-upstream leg of the same shape. The upstream framing is the direct
/// observable, and the two decisions are deliberately split (see
/// `proxy_down_to_up`):
/// - the UPSTREAM framing follows the request's own declaration, so
///   `Content-Length: 0` is forwarded as END_STREAM on the HEADERS frame. An
///   origin that does not answer until it sees the end of the request would
///   otherwise deadlock, and the futile-read rule cannot rescue it because that
///   rule needs a complete response first.
/// - the DOWNSTREAM read keeps the strict transport fact, so the client's real
///   end of stream is still read and still produces exactly ONE application
///   end-of-stream event. The second, standalone END_STREAM that would once
///   have followed it (an h2 `UserError` costing the downstream its
///   reusability) is suppressed by `upstream_body_closed`.
#[test]
fn h2_cl0_without_end_stream_h2_upstream_sends_one_eos() {
    init();
    let (port, _counter) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestEos]);

    RT.block_on(async {
        let (status, eos_events, headers_eos) = h2_cl0_no_end_stream_request(port, true).await;
        assert_eq!(status, 200);
        assert_eq!(
            eos_events, "1",
            "the application must see exactly one end-of-stream event"
        );
        assert_eq!(
            headers_eos.as_deref(),
            Some("1"),
            "the `Content-Length: 0` declaration must be forwarded upstream as \
             END_STREAM on HEADERS"
        );
    });
}

/// The deadlock P6 is about, end to end: an H2 downstream that declares
/// `content-length: 0`, never sends END_STREAM, and an H2 upstream that does not
/// respond until it has seen the end of the request stream.
///
/// With the upstream framing keyed on the strict transport fact instead of the
/// declaration, nothing ever closes the upstream request stream: the origin
/// waits for END_STREAM, the pump waits for the origin, and the futile-read rule
/// cannot fire because it requires a complete response. The request hangs until
/// the client gives up.
#[test]
fn h2_cl0_never_ending_request_reaches_an_upstream_that_waits_for_eos() {
    init();
    // `EchoRequestEos` drains the request body to its end BEFORE responding, so
    // it answers only once END_STREAM has arrived.
    let (port, _counter) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestEos]);
    let ports = init();

    RT.block_on(async {
        let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
        let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        let req = http::Request::builder()
            .method("POST")
            .uri("http://t/")
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .header("content-length", "0")
            .body(())
            .unwrap();
        // No END_STREAM on HEADERS, and `_body` is held for the rest of the
        // block: this client never ends its request stream.
        let (response, _body) = h2.send_request(req, false).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .expect(
                "the upstream never saw END_STREAM: the `Content-Length: 0` declaration was \
                 not forwarded as upstream request framing",
            )
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("x-headers-eos")
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
    });
}

/// Send `POST /` with `content-length: 0` over the h2c listener WITHOUT
/// END_STREAM on HEADERS, then close the request stream with an empty
/// END_STREAM DATA frame. Returns (status, `x-eos-events`, `x-headers-eos`).
async fn h2_cl0_no_end_stream_request(
    upstream_port: u16,
    h2_upstream: bool,
) -> (u16, String, Option<String>) {
    let ports = init();
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
    (
        response.status().as_u16(),
        header("x-eos-events").expect("response_filter always sets x-eos-events"),
        header("x-headers-eos"),
    )
}

/// An H2 downstream request that declares `content-length: 0` WITHOUT
/// END_STREAM and whose client never sends the end-of-stream: the pump must
/// still finish once the upstream exchange is complete.
///
/// `is_body_done()` is the pure transport fact, so this request's read side
/// stays open, and the only live branch left is a body read that can never
/// yield -- no downstream request-body idle timeout exists. The pump, its
/// task, the downstream stream and the upstream stream stay pinned forever.
///
/// The client is NOT the observable for the hang: it receives its complete
/// response either way. `ProxyHttp::logging` is, because it only runs once the
/// proxy finished the request.
///
/// The response headers ARE the observable for invariant B: abandoning the
/// read must still deliver the application its single end-of-stream event
/// (`x-eos-events: 1`). Simply finishing the downstream state instead would
/// silently skip an application that finalizes its inspection at EOS -- a
/// client-reachable zero-EOS cell.
#[test]
fn h2_cl0_never_ending_request_completes() {
    let ports = init();
    let (h1_port, _c1) = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);
    // `Ok200Linger`, not `Ok200`: this test deliberately never ends its
    // downstream request stream, so an upstream that dropped the request half
    // would reset it and the pump would take the upstream-error path instead
    // of the futile-read path under test.
    let (h2_port, _c2) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::Ok200Linger]);

    for (tag, port, h2_upstream, counter, final_eos) in [
        (
            "h1",
            h1_port,
            false,
            &COMPLETED_H1_UPSTREAM,
            &FINAL_EOS_H1_UPSTREAM,
        ),
        (
            "h2",
            h2_port,
            true,
            &COMPLETED_H2_UPSTREAM,
            &FINAL_EOS_H2_UPSTREAM,
        ),
    ] {
        let before = counter.load(Ordering::SeqCst);
        RT.block_on(async {
            let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
            let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            let mut builder = http::Request::builder()
                .method("POST")
                .uri("http://t/")
                .header("x-port", port.to_string())
                .header("x-count-completion", tag)
                .header("content-length", "0");
            if h2_upstream {
                builder = builder.header("x-h2", "1");
            }
            // No END_STREAM on HEADERS, and `_body` is deliberately held (and
            // never written to) for the rest of this block: the client keeps
            // its request stream open forever.
            let (response, _body) = h2.send_request(builder.body(()).unwrap(), false).unwrap();
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .expect("timed out waiting for the response")
                .unwrap();
            assert_eq!(response.status(), 200, "{tag} upstream");

            // The request must actually FINISH, not merely answer.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while counter.load(Ordering::SeqCst) == before {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the {tag}-upstream pump never finished the request: it is still \
                     parked on a downstream body read that can never yield"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // ... and abandoning the read must not cost the application its
            // single end-of-stream event (invariant B).
            assert_eq!(
                final_eos.load(Ordering::SeqCst),
                1,
                "the application must still see exactly one end-of-stream event \
                 when the {tag}-upstream pump abandons the downstream read"
            );
        });
    }
}

/// A terminate must not leave the application's local reply unfinished.
///
/// The application writes a chunked 403 header and its body with
/// `end_of_stream = false`, then terminates. The terminate arms return before
/// the pump's `finish_body()`, and `HttpProxy::finish` skips
/// `downstream_session.finish()` because a terminated request never reports
/// reuse -- so the terminating `0\r\n\r\n` chunk was never written. The client
/// then cannot tell the response from a connection that died mid-body, and
/// `warn_terminate_without_response` stayed silent about it because a response
/// header WAS written.
///
/// `reqwest` is the client here on purpose: it validates the chunked framing,
/// so `text()` fails on the truncated form and succeeds on the complete one.
fn terminate_unflushed_body(port: u16, h2_upstream: bool) {
    let ports = init();
    RT.block_on(async {
        let client = reqwest::Client::new();
        let mut req = client
            .post(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-terminate-unflushed", "1")
            .body("hello");
        if h2_upstream {
            req = req.header("x-h2", "1");
        }
        let res = tokio::time::timeout(Duration::from_secs(10), req.send())
            .await
            .expect("the application's local reply must reach the client")
            .unwrap();
        assert_eq!(res.status(), 403);
        let body = tokio::time::timeout(Duration::from_secs(10), res.text())
            .await
            .expect("reading the local reply body must not hang")
            .expect("the local reply body must be completely framed");
        assert_eq!(body, "denied");
    });
}

#[test]
fn terminate_finishes_an_unfinished_local_reply() {
    init();
    let (port, _counter) = spawn_scripted_upstream(vec![UpstreamStep::Hang]);
    terminate_unflushed_body(port, false);
}

/// The same contract on the H2-upstream pump, whose terminate arms are
/// separate code.
#[test]
fn terminate_finishes_an_unfinished_local_reply_h2_upstream() {
    init();
    let (port, _counter) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::Hang]);
    terminate_unflushed_body(port, true);
}

#[test]
fn retry_predicate_gates_reused_connection_retry() {
    let ports = init();
    // Upstream: request 1 succeeds (pools the connection); request 2 closes
    // without a response (retryable, pre-commit); request 3 would be the
    // retry attempt and succeeds.
    let run = |no_retry: bool| -> (usize, u16) {
        let (port, counter) = spawn_scripted_upstream(vec![
            UpstreamStep::Respond(OK_KEEPALIVE),
            UpstreamStep::CloseWithoutResponse,
            UpstreamStep::Respond(OK_KEEPALIVE),
        ]);
        let status = RT.block_on(async {
            let client = reqwest::Client::new();
            let res = client
                .get(format!("http://{}/", ports.h1_addr()))
                .header("x-port", port.to_string())
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200);

            let mut req = client
                .get(format!("http://{}/", ports.h1_addr()))
                .header("x-port", port.to_string());
            if no_retry {
                req = req.header("x-no-retry", "1");
            }
            res_status(req.send().await)
        });
        (counter.load(Ordering::SeqCst), status)
    };

    // Control: retry allowed — the second client request is retried on a
    // fresh connection and succeeds; three upstream requests total.
    let (attempts, status) = run(false);
    assert_eq!(status, 200);
    assert_eq!(attempts, 3);

    // Predicate false: exactly one failed attempt, surfaced as 502.
    let (attempts, status) = run(true);
    assert_eq!(status, 502);
    assert_eq!(attempts, 2);
}

fn res_status(res: std::result::Result<reqwest::Response, reqwest::Error>) -> u16 {
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

/// `Bodyless` contradicted by a REAL downstream body, on an H1 upstream.
///
/// `Bodyless` is a guarantee that no upstream request body will follow, and the
/// pump acts on it irreversibly before reading any: the upstream request loses
/// both `Content-Length` and `Transfer-Encoding`, so its zero-length body
/// writer would swallow whatever the client sent. Forwarding the request
/// anyway would have the upstream act on a body-less `POST` while the client is
/// told it succeeded, so the proxy fails closed instead: no body bytes on the
/// upstream wire, and a 500 to the client.
///
/// The body is real on purpose: a bodyless request would be coerced back to
/// `Ordinary` by `safe_disposition`, so `Bodyless` would not be exercised at
/// all.
#[test]
fn bodyless_with_a_real_body_h1_upstream_fails_closed() {
    let ports = init();
    let (port, captured) = spawn_recording_upstream(b"\r\n\r\n");

    RT.block_on(async {
        let mut stream = TcpStream::connect(ports.h1_addr()).await.unwrap();
        let mut pending = Vec::new();
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             x-disposition: bodyless\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n"
        );
        // A COMPLETE response: the error path must produce a well-framed
        // response, not a bare connection close, and `read_one_h1_response`
        // fails the test on a hang (10s deadline) or a truncated one.
        let response = h1_request_response(&mut stream, &mut pending, &request)
            .await
            .to_lowercase();
        assert!(
            response.starts_with("http/1.1 500"),
            "a Bodyless declaration contradicted by a real request body must \
             fail the request closed with a 500: {response}"
        );
        assert!(
            !response.contains("x-eos-events"),
            "the upstream response filter must never have run: {response}"
        );
        // Exactly ONE response on the connection: `read_one_h1_response`
        // consumed the 500 in full, so anything left here would be a second
        // response written after the error had already been answered.
        assert!(
            pending.is_empty(),
            "the fail-closed error must produce exactly one downstream \
             response: {pending:?}"
        );
    });

    let bytes = captured.lock().unwrap().clone();
    let text = String::from_utf8_lossy(&bytes).to_lowercase();
    assert!(
        !text.contains("content-length"),
        "bodyless must not declare a content length upstream: {text}"
    );
    assert!(
        !text.contains("transfer-encoding"),
        "bodyless must not declare a transfer encoding upstream: {text}"
    );
    assert!(
        !text.contains("hello") && !text.contains("world"),
        "bodyless must not put any request body bytes on the upstream wire: {text}"
    );
}

/// The H2-upstream half of the same contract.
///
/// `Bodyless` closes the upstream request stream at header time (END_STREAM on
/// HEADERS), so every body chunk the pump reads afterwards can only be dropped.
/// It must fail the request closed instead, symmetrically with the H1 pump:
/// zero request body bytes at the upstream, a 500 at the client, no hang.
#[test]
fn bodyless_with_a_real_body_h2_upstream_fails_closed() {
    let ports = init();
    let upstream_body_bytes = Arc::new(AtomicUsize::new(0));
    let (port, _counter) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::CountRequestBody(
        upstream_body_bytes.clone(),
    )]);

    RT.block_on(async {
        let mut stream = TcpStream::connect(ports.h1_addr()).await.unwrap();
        let mut pending = Vec::new();
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\nx-h2: 1\r\n\
             x-disposition: bodyless\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n"
        );
        let response = h1_request_response(&mut stream, &mut pending, &request)
            .await
            .to_lowercase();
        assert!(
            response.starts_with("http/1.1 500"),
            "a Bodyless declaration contradicted by a real request body must \
             fail the request closed with a 500 on the H2 pump too: {response}"
        );
        assert!(
            !response.contains("x-eos-events"),
            "the upstream response filter must never have run: {response}"
        );
        // Exactly ONE response on the connection: `read_one_h1_response`
        // consumed the 500 in full, so anything left here would be a second
        // response written after the error had already been answered.
        assert!(
            pending.is_empty(),
            "the fail-closed error must produce exactly one downstream \
             response: {pending:?}"
        );
    });

    // The upstream drains its request stream and only then answers; give it a
    // moment to have done so, so a regression that DID forward the body is
    // visible here rather than racing past.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        upstream_body_bytes.load(Ordering::SeqCst),
        0,
        "bodyless must not put any request body bytes on the upstream stream"
    );
}

/// `Streamed` must NOT re-frame a request that has no body.
///
/// Rewriting a plain `GET` to `Transfer-Encoding: chunked` puts a `0\r\n\r\n`
/// terminator on a POOLED upstream connection. An origin or WAF that ignores
/// bodies on bodyless methods leaves those five bytes in the stream, which
/// desynchronises every later request on that connection -- a request
/// smuggling primitive. `GET` + `Transfer-Encoding: chunked` is also a shape
/// many WAFs reject outright.
#[test]
fn streamed_does_not_reframe_a_bodyless_request() {
    let ports = init();
    let (port, captured) = spawn_recording_upstream(b"\r\n\r\n");

    RT.block_on(async {
        let request = format!(
            "GET / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             x-disposition: streamed\r\n\r\n"
        );
        let (_stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"HTTP/1.1 200").await;
        assert!(String::from_utf8_lossy(&collected).starts_with("HTTP/1.1 200"));
    });

    let bytes = captured.lock().unwrap().clone();
    let text = String::from_utf8_lossy(&bytes).to_lowercase();
    assert!(
        !text.contains("transfer-encoding"),
        "a bodyless request must not be re-framed as chunked: {text}"
    );
    let header_end = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("the upstream request headers must have been captured");
    assert!(
        bytes[header_end + 4..].is_empty(),
        "nothing may follow the headers of a bodyless request (a chunked \
         terminator here is a smuggling primitive on a pooled connection): {:?}",
        String::from_utf8_lossy(&bytes[header_end + 4..])
    );
}

/// The trailer hook returning `Continue` must let the request through -- and
/// the trailer FIELDS must not reach the upstream, because Pingora does not
/// expose or forward them.
#[test]
fn trailer_continue_completes_without_forwarding_trailers() {
    let ports = init();
    // `\r\n0\r\n` is the start of the terminal chunk: it matches whether or
    // not trailer fields follow it, so the upstream answers either way and a
    // regression shows up as captured trailer bytes rather than a hang.
    let (port, captured) = spawn_recording_upstream(b"\r\n0\r\n");

    RT.block_on(async {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n0\r\nx-checksum: ok\r\n\r\n"
        );
        let (_stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"HTTP/1.1 200").await;
        let text = String::from_utf8_lossy(&collected);
        assert!(text.starts_with("HTTP/1.1 200"), "expected 200: {text}");
        assert!(
            text.to_lowercase().contains("x-trailer-hook-calls: 1"),
            "the trailer hook must have run exactly once: {text}"
        );
    });

    let bytes = captured.lock().unwrap().clone();
    let text = String::from_utf8_lossy(&bytes).to_lowercase();
    assert!(
        text.contains("hello"),
        "the body must still be forwarded: {text}"
    );
    assert!(
        !text.contains("x-checksum"),
        "trailer fields must not be forwarded upstream: {text}"
    );
}

/// `request_trailer_filter` fires AT MOST ONCE per downstream request.
///
/// The upstream script pools a connection, then closes the reused connection
/// without a response (retryable, nothing committed downstream), so the retry
/// runs the whole downstream pump again: its retry-buffer prelude replays the
/// same EOF (`data == None`) while the trailer fact is still true. Without the
/// latch the hook is invoked a second time and the echoed count is 2.
#[test]
fn trailer_hook_fires_at_most_once_across_retries() {
    let ports = init();
    let (port, counter) = spawn_scripted_upstream(vec![
        UpstreamStep::Respond(OK_KEEPALIVE),
        UpstreamStep::CloseWithoutResponse,
        UpstreamStep::Respond(OK_KEEPALIVE),
    ]);

    RT.block_on(async {
        // Prime the upstream connection pool so the failing attempt below is
        // on a REUSED connection, which is what makes it retryable.
        let client = reqwest::Client::new();
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        // An empty chunked body with trailers: the retry attempt's prelude
        // path is exactly the one that used to re-fire the hook.
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             0\r\nx-checksum: ok\r\n\r\n"
        );
        let (_stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"HTTP/1.1 200").await;
        let text = String::from_utf8_lossy(&collected).to_lowercase();
        assert!(
            text.contains("x-trailer-hook-calls: 1"),
            "request_trailer_filter must fire exactly once across retries: {text}"
        );
    });

    // prime + failed attempt + retry. A `>=` because the shared proxy serves
    // other tests concurrently and an extra upstream attempt would only give
    // the trailer hook MORE chances to re-fire, never fewer.
    assert!(
        counter.load(Ordering::SeqCst) >= 3,
        "the retry must actually have happened (prime + failed attempt + retry)"
    );
}

/// The truth table of the header-time end-of-stream decision, end to end.
///
/// For EVERY disposition on a request with NO body, through an H2 upstream:
/// exactly one upstream END_STREAM must reach the wire and exactly one
/// end-of-stream event must reach the application. Two independent holes made
/// this fail before:
/// - the H2 pump had no bodyless prelude at all (the H1 pump did), so a
///   bodyless request delivered ZERO body events to the application and
///   `Terminate` before any body event was unreachable;
/// - adding that prelude naively would emit a second, standalone END_STREAM on
///   a stream the HEADERS frame had already closed.
///
/// `x-headers-eos` is echoed by the scripted upstream (so a missing upstream
/// EOS shows up as a hang, and a doubled one as an h2 `UserError`);
/// `x-eos-events` is echoed by the application.
#[test]
fn bodyless_request_emits_exactly_one_eos_for_every_disposition() {
    let ports = init();
    // (disposition, send_end_stream opt-out, expected x-headers-eos)
    //
    // `safe_disposition` coerces every non-`Ordinary` disposition back to
    // `Ordinary` on a request with no body, so all three rows agree -- which
    // is exactly the point: the choice cannot change the framing of a request
    // that has nothing to frame.
    let cases = [
        ("ordinary", false, "1"),
        ("ordinary", true, "0"),
        ("bodyless", false, "1"),
        ("bodyless", true, "0"),
        ("streamed", false, "1"),
        ("streamed", true, "0"),
    ];

    for (disposition, no_header_eos, expected_headers_eos) in cases {
        let (port, _counter) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestEos]);
        RT.block_on(async {
            let client = reqwest::Client::new();
            let mut req = client
                .get(format!("http://{}/", ports.h1_addr()))
                .header("x-port", port.to_string())
                .header("x-h2", "1")
                .header("x-disposition", disposition);
            if no_header_eos {
                // Mirrors the gRPC-web bridge's `set_send_end_stream(false)`.
                req = req.header("x-no-header-eos", "1");
            }
            let res = tokio::time::timeout(Duration::from_secs(10), req.send())
                .await
                .unwrap_or_else(|_| panic!("{disposition} (opt-out {no_header_eos}) hung"))
                .unwrap();

            assert_eq!(res.status(), 200, "{disposition} opt-out={no_header_eos}");
            assert_eq!(
                res.headers().get("x-headers-eos").unwrap(),
                expected_headers_eos,
                "upstream END_STREAM placement for {disposition} opt-out={no_header_eos}"
            );
            assert_eq!(
                res.headers().get("x-eos-events").unwrap(),
                "1",
                "the application must see exactly one end-of-stream event for \
                 {disposition} opt-out={no_header_eos}"
            );
        });
    }
}

/// A terminate on a cache-enabled request must release the cache lock.
///
/// Terminate reports `error = None`, so the `final_error` branch in `lib.rs`
/// that disables the cache never runs for it. A cache-enabled MISS holding a
/// write lock would then reach `WritePermit::Drop` unfinished, which trips
/// `debug_assert!(false, "Dangling cache lock started!")` inside the proxy's
/// connection task. tokio swallows that panic, so the assertion here is on the
/// message-matching panic counter installed by `start_seam_server`.
#[test]
fn terminate_with_cache_enabled_does_not_leave_a_dangling_lock() {
    let ports = init();
    let (port, _counter) = spawn_scripted_upstream(vec![UpstreamStep::Hang]);
    let before = DANGLING_CACHE_LOCKS.load(Ordering::SeqCst);

    RT.block_on(async {
        let request = format!(
            "POST /cache-terminate HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             x-enable-cache: 1\r\n\
             x-terminate-after-bytes: 1\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n"
        );
        let (_stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"denied").await;
        assert!(String::from_utf8_lossy(&collected).starts_with("HTTP/1.1 403"));
    });

    // The lock is released when the session is dropped, shortly after the
    // response was flushed.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        DANGLING_CACHE_LOCKS.load(Ordering::SeqCst),
        before,
        "a cache-enabled terminate must not leave a dangling cache lock"
    );
}

/// The trait default for `request_body_filter_action` must delegate to the
/// legacy `request_body_filter` through a real pump. `LegacyHookProxy` (behind
/// the legacy listener) overrides ONLY the legacy hook.
#[test]
fn legacy_request_body_filter_is_delegated_through_the_h1_pump() {
    let ports = init();
    let (port, _counter) = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);

    RT.block_on(async {
        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/", ports.legacy_addr()))
            .header("x-port", port.to_string())
            .body("hello world!")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get("x-legacy-bytes").unwrap(),
            "12",
            "the legacy request_body_filter must have seen the request body"
        );
        assert_ne!(
            res.headers().get("x-legacy-calls").unwrap(),
            "0",
            "the legacy request_body_filter must have been invoked"
        );
    });
}

/// The feature's primary use case, end to end: `Streamed` through an H2
/// upstream with a REAL request body.
///
/// Every other `Streamed` test in this file either goes to an H1 upstream or
/// uses a bodyless request (which `safe_disposition` coerces back to
/// `Ordinary`), so this is the only one that exercises the H2 pump's rewrite at
/// the call site: both length headers removed and END_STREAM kept OFF the
/// HEADERS frame, so the stream stays open for the body that follows.
#[test]
fn streamed_disposition_with_a_body_through_an_h2_upstream() {
    let ports = init();
    let (port, _counter) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestFraming]);

    RT.block_on(async {
        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .header("x-disposition", "streamed")
            .body("hello world!") // reqwest sends Content-Length: 12
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        let header = |name: &str| {
            res.headers()
                .get(name)
                .map(|v| v.to_str().unwrap().to_string())
        };
        assert_eq!(
            header("x-headers-eos").as_deref(),
            Some("0"),
            "Streamed must keep END_STREAM off the upstream HEADERS frame"
        );
        assert_eq!(
            header("x-req-content-length").as_deref(),
            Some("none"),
            "the stale downstream Content-Length must be removed"
        );
        assert_eq!(
            header("x-req-transfer-encoding").as_deref(),
            Some("none"),
            "Transfer-Encoding has no meaning on HTTP/2 and must be removed"
        );
        assert_eq!(
            header("x-req-body-len").as_deref(),
            Some("12"),
            "the body must arrive at the upstream intact"
        );
    });
}

/// Retry predicate, consumption point 1: the NATIVE RETRY BUFFER.
///
/// `request_retry_allowed() == false` must also stop the pumps from buffering
/// the request body for a replay that can never happen -- an unbounded-ish
/// per-request memory cost paid for nothing. The retry loop's own check cannot
/// catch a regression here: it only decides whether to re-dial.
#[test]
fn retry_predicate_gates_the_request_body_retry_buffer() {
    let ports = init();
    let (port, _counter) = spawn_scripted_upstream(vec![
        UpstreamStep::Respond(OK_KEEPALIVE),
        UpstreamStep::Respond(OK_KEEPALIVE),
    ]);

    RT.block_on(async {
        let client = reqwest::Client::new();
        let buffered = |res: &reqwest::Response| {
            res.headers()
                .get("x-retry-buffer")
                .map(|v| v.to_str().unwrap().to_string())
        };

        // Control: retries allowed, so the body IS buffered.
        let res = client
            .post(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .body("hello world!")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            buffered(&res).as_deref(),
            Some("1"),
            "with retries allowed the request body must be buffered for replay"
        );

        // The predicate says no: nothing may be buffered.
        let res = client
            .post(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-no-retry", "1")
            .body("hello world!")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            buffered(&res).as_deref(),
            Some("0"),
            "a request that can never be retried must not have its body buffered"
        );
    });
}

/// Block until the slot's request count moves past `before`, then return the
/// flag `ProxyHttp::logging` saw.
async fn await_observed_retry_flag(observation: &RetryObservation, before: usize) -> i8 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while observation.requests.load(Ordering::SeqCst) == before {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the request never reached ProxyHttp::logging"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    observation.flag.load(Ordering::SeqCst)
}

/// Retry predicate, consumption point 2: the error handed BACK from
/// `error_while_proxy`.
///
/// The upstream closes a REUSED connection without responding, which
/// `error_while_proxy` decides is retryable. With the predicate saying no, the
/// error the application finally receives must say so too -- otherwise
/// `fail_to_proxy`/`logging` are told the request was retryable when the proxy
/// had already ruled that out. The retry loop's own check cannot catch a
/// regression here: it refuses the retry either way and never touches the error.
#[test]
fn retry_predicate_forces_the_error_from_error_while_proxy() {
    let ports = init();
    let (port, counter) = spawn_scripted_upstream(vec![
        UpstreamStep::Respond(OK_KEEPALIVE),
        UpstreamStep::CloseWithoutResponse,
    ]);

    RT.block_on(async {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        // Attempt 1 succeeds and pools the upstream connection.
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        let before = OBSERVED_RETRY_PROXY.requests.load(Ordering::SeqCst);
        // Attempt 2 reuses it and gets closed without a response.
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-no-retry", "1")
            .header("x-observe-retry", "proxy")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 502);

        assert_eq!(
            await_observed_retry_flag(&OBSERVED_RETRY_PROXY, before).await,
            0,
            "the error handed to the application must be marked non-retryable"
        );
    });
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "the refused retry must not have reached the upstream"
    );
}

/// Retry predicate, consumption point 3: the error handed back from
/// `fail_to_connect`.
///
/// The application's `fail_to_connect` marks the connect failure retryable --
/// the hook's documented purpose, and the only way any connect error becomes
/// retryable at all (`connectors::l4` resolves every fresh-dial connect error to
/// `Decided(false)`). With the retry predicate saying no, the error the
/// application finally receives must be marked non-retryable again.
///
/// No connect-failure test existed at all, so this also pins that a request
/// whose upstream cannot be dialled ends as a 502 rather than, say, a hang.
#[test]
fn retry_predicate_forces_the_error_from_fail_to_connect() {
    let ports = init();
    // A port that was bound and released: nothing is listening on it.
    let dead_port = reserve_port();

    let before = OBSERVED_RETRY_CONNECT.requests.load(Ordering::SeqCst);
    RT.block_on(async {
        let client = reqwest::Client::new();
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", dead_port.to_string())
            .header("x-no-retry", "1")
            .header("x-connect-retryable", "1")
            .header("x-observe-retry", "connect")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 502);

        assert_eq!(
            await_observed_retry_flag(&OBSERVED_RETRY_CONNECT, before).await,
            0,
            "a connect failure that may not be retried must be marked non-retryable"
        );
    });
}

/// `Streamed` must never close the upstream request stream early, whatever the
/// downstream request DECLARED.
///
/// The shape: an H2 downstream declaring `content-length: 0` that has not sent
/// END_STREAM. Its declaration is what the `Ordinary` upstream framing is built
/// from (see `h2_cl0_without_end_stream_h2_upstream_sends_one_eos`), and feeding
/// that same declaration to `Streamed` revives
/// `upstream_empty_data_end_stream`'s otherwise-unreachable `Streamed` arm: a
/// standalone empty DATA/END_STREAM right after the headers. That sets
/// `upstream_body_closed`, so every byte the application would go on to stream
/// in through `request_body_filter_action` -- the entire point of `Streamed` --
/// is refused by the suppressed-write branch, and on `Ordinary`/`Streamed` that
/// refusal is absorbed into `to_errored()`: the origin acts on a request whose
/// body was silently removed while the client is told it succeeded.
///
/// The observable is the ORDER of two events, which is what makes it
/// discriminating rather than a timing guess: the scripted upstream drains the
/// request body BEFORE responding, so it can only answer once it has seen an end
/// of stream. If the proxy answers before this client sends one, the early EOS
/// was sent.
#[test]
fn streamed_does_not_close_the_upstream_stream_for_a_cl0_request() {
    let ports = init();
    let (port, _counter) = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestFraming]);

    RT.block_on(async {
        let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
        let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        let req = http::Request::builder()
            .method("POST")
            .uri("http://t/")
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .header("x-disposition", "streamed")
            .header("content-length", "0")
            .body(())
            .unwrap();
        // No END_STREAM on HEADERS.
        let (response, mut body) = h2.send_request(req, false).unwrap();
        tokio::pin!(response);

        tokio::select! {
            _ = &mut response => panic!(
                "the upstream answered before the client ended its request stream: \
                 Streamed sent an early upstream END_STREAM"
            ),
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }

        // Now end it for real; the upstream may answer from here on.
        body.send_data(Bytes::new(), true).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .expect("timed out waiting for the response")
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("x-headers-eos")
                .and_then(|v| v.to_str().ok()),
            Some("0"),
            "Streamed must keep END_STREAM off the upstream HEADERS frame"
        );
        assert_eq!(
            response
                .headers()
                .get("x-eos-events")
                .and_then(|v| v.to_str().ok()),
            Some("1"),
            "the application must still see exactly one end-of-stream event"
        );
    });
}
