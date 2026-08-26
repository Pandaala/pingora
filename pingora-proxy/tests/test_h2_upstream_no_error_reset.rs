// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end coverage for RFC 9113 section 8.1 against an H2 upstream: a
//! complete response followed by RST_STREAM(NO_ERROR) while the proxy is still
//! uploading the request body.

mod utils;

use bytes::Bytes;
use http::{Response, StatusCode};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use utils::server_utils::init_without_mock_origin;

/// The upload has to be big enough that the proxy is provably still writing
/// request body when the reset lands: the origin never reads the request body,
/// so everything past one h2 flow-control window sits blocked in the pump.
const UPLOAD_LEN: usize = 1024 * 1024;

const RESPONSE_BODY: &str = "the complete response body";

/// Spawn a cleartext-h2 origin that answers ONE request with a complete
/// response and then resets the stream with NO_ERROR while the peer is still
/// uploading.
///
/// `end_stream` picks which half of the discrimination this origin plays:
/// - `true`: the response carries END_STREAM before the reset, i.e. the
///   complete response of RFC 9113 section 8.1. The client must see it.
/// - `false`: the body is TRUNCATED -- no END_STREAM ever appears -- and the
///   very same reset must still fail the exchange.
async fn spawn_reset_while_uploading_origin(end_stream: bool) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        send_stream
            .send_data(Bytes::from(RESPONSE_BODY), end_stream)
            .unwrap();

        // Flush what was just queued before resetting: h2's `send_reset` clears
        // the stream's pending send queue, so an immediate reset would drop the
        // very frames this test is about.
        //
        // The upload cannot finish in the meantime: this origin never reads the
        // request body, so the proxy is stuck on the stream's flow-control
        // window for as long as the test cares to wait. The reset therefore
        // always lands mid-upload, which is the premise of the shape.
        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        // RFC 9113 section 8.1: stop uploading, the response is done.
        send_stream.send_reset(h2::Reason::NO_ERROR);
        while conn.accept().await.is_some() {}
    });
    port
}

async fn post_through_proxy(port: u16) -> reqwest::Result<reqwest::Response> {
    let client = reqwest::Client::new();
    client
        .post("http://127.0.0.1:6147/upload")
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .body("x".repeat(UPLOAD_LEN))
        .timeout(Duration::from_secs(10))
        .send()
        .await
}

async fn spawn_h2_header_only_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let response = Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(())
            .unwrap();
        send_resp.send_response(response, true).unwrap();
        while conn.accept().await.is_some() {}
    });
    port
}

async fn spawn_h2_declared_empty_but_open_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let response = Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(http::header::CONTENT_LENGTH, "0")
            .body(())
            .unwrap();
        let mut body = send_resp.send_response(response, false).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        body.send_data(Bytes::new(), true).unwrap();
        while conn.accept().await.is_some() {}
    });
    port
}

#[tokio::test]
async fn h2_header_end_stream_runs_terminal_body_hook() {
    init_without_mock_origin();
    let port = spawn_h2_header_only_origin().await;
    let response = reqwest::Client::new()
        .get("http://127.0.0.1:6147/bodyless")
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-bodyless-replace", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.text().await.unwrap(), "generated-extra");
}

#[tokio::test]
async fn h2_content_length_zero_does_not_replace_the_real_end_stream_bit() {
    init_without_mock_origin();
    let port = spawn_h2_declared_empty_but_open_origin().await;
    let start = Instant::now();
    let response = reqwest::Client::new()
        .get("http://127.0.0.1:6147/bodyless")
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-bodyless-replace", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.text().await.unwrap(), "");
    assert!(
        start.elapsed() >= Duration::from_millis(40),
        "Content-Length: 0 incorrectly finalized an H2 stream before END_STREAM"
    );
}

/// The end-to-end outcome the read-half fix alone does not pin down: the
/// upstream write fails (that is the premise of the shape -- the origin resets
/// precisely because this side is still uploading), and the exchange must
/// nevertheless deliver the complete response the proxy already holds.
#[tokio::test]
async fn h2_upstream_complete_response_then_no_error_reset_delivers_the_response() {
    init_without_mock_origin();
    let port = spawn_reset_while_uploading_origin(true).await;
    let res = post_through_proxy(port)
        .await
        .expect("the complete response must be delivered, not turned into an error");
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), RESPONSE_BODY);
}

/// Spawn a cleartext-h2 origin that answers the request in full, WITHOUT
/// consuming the request body, and resets the stream once `reset_after` has
/// elapsed. One request only.
async fn spawn_ok_then_reset_origin(reset_after: Duration) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        send_stream
            .send_data(Bytes::from(RESPONSE_BODY), true)
            .unwrap();
        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = tokio::time::sleep(reset_after) => {}
        }
        send_stream.send_reset(h2::Reason::NO_ERROR);
        while conn.accept().await.is_some() {}
    });
    port
}

/// Spawn a one-shot HTTP/1.1 origin, for the follow-up request that proves the
/// downstream connection survived.
async fn spawn_h1_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = io.read(&mut buf).await.unwrap();
        io.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
        io.flush().await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    port
}

async fn read_until(io: &mut TcpStream, marker: &str) -> String {
    let mut seen = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(10), io.read(&mut buf))
            .await
            .expect("timed out waiting for the proxy")
            .expect("read failed");
        assert!(n > 0, "the proxy closed the connection early: {seen:?}");
        seen.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&seen).to_string();
        if text.contains(marker) {
            return text;
        }
    }
}

/// The write half of the same shape, pinned end to end.
///
/// The client uploads a CHUNKED body and holds back its terminating chunk until
/// the upstream's RST_STREAM(NO_ERROR) has landed, so the pump's very next
/// write -- the standalone END_STREAM it owes the upstream -- is guaranteed to
/// fail on a stream whose response is already complete. That failure must cost
/// the exchange nothing: the response is delivered AND the downstream
/// connection stays usable, which the follow-up request on the same socket is
/// what actually proves.
///
/// Before the write half consulted the wire-level END_STREAM record, the pump
/// booked that failure as a downstream error, and the connection was closed
/// under the client instead.
#[tokio::test]
async fn h2_upstream_no_error_reset_does_not_cost_the_downstream_connection() {
    init_without_mock_origin();
    let h2_port = spawn_ok_then_reset_origin(Duration::from_millis(100)).await;
    let h1_port = spawn_h1_origin().await;

    let mut io = TcpStream::connect("127.0.0.1:6147").await.unwrap();
    io.write_all(
        format!(
            "POST /upload HTTP/1.1\r\nHost: 127.0.0.1:6147\r\nx-h2: true\r\nx-port: {h2_port}\r\n\
             Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();

    // The whole response, terminating chunk included, arrives while the request
    // body is still open.
    let response = read_until(&mut io, "0\r\n\r\n").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains(RESPONSE_BODY), "{response}");

    // Let the upstream's RST_STREAM(NO_ERROR) land, then end the request body:
    // the END_STREAM the pump now owes upstream can no longer be written.
    tokio::time::sleep(Duration::from_millis(600)).await;
    io.write_all(b"0\r\n\r\n").await.unwrap();

    // Let the first exchange finish before the next request goes out: pingora
    // refuses to reuse a connection whose next request it had to overread
    // (it does not implement pipelining), which would confuse this test's
    // subject with a different reason for closing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The exchange succeeded, so the connection is still good for the next
    // request.
    io.write_all(
        format!("GET /second HTTP/1.1\r\nHost: 127.0.0.1:6147\r\nx-port: {h1_port}\r\n\r\n")
            .as_bytes(),
    )
    .await
    .unwrap();
    let second = read_until(&mut io, "ok").await;
    assert!(
        second.starts_with("HTTP/1.1 200 OK"),
        "the downstream connection must survive the upstream's stop-uploading reset: {second}"
    );
}

/// The direction that must never regress: the same reset on a TRUNCATED
/// response body. The wire never carried END_STREAM, so nothing may launder
/// this into a complete 200.
#[tokio::test]
async fn h2_upstream_truncated_response_then_no_error_reset_is_an_error() {
    init_without_mock_origin();
    let port = spawn_reset_while_uploading_origin(false).await;
    match post_through_proxy(port).await {
        // The proxy has already written 200 + partial body downstream when the
        // truncation is discovered, so the failure surfaces as an incomplete
        // H1 response body rather than as a 502.
        Ok(res) => {
            assert_eq!(res.status(), StatusCode::OK);
            let body = res.text().await;
            assert!(
                body.is_err(),
                "a truncated upstream body must not read as a complete response body: {body:?}"
            );
        }
        Err(_) => { /* connection error is an acceptable shape of the same failure */ }
    }
}

// ---------------------------------------------------------------------------
// A second, self-contained proxy, on its own port, whose only job is to make
// the application-visible effects of the pump observable. `tests/utils`'s
// example proxy implements neither `request_body_filter_action` nor
// `request_trailer_filter`, so nothing there can see the terminal body event
// this section is about.
// ---------------------------------------------------------------------------

mod observing {
    use async_trait::async_trait;
    use bytes::Bytes;
    use once_cell::sync::Lazy;
    use pingora_core::server::configuration::ServerConf;
    use pingora_core::server::Server;
    use pingora_core::services::ServiceWithDependents;
    use pingora_core::upstreams::peer::HttpPeer;
    use pingora_error::Result;
    use pingora_proxy::{ProxyHttp, RequestBodyAction, RequestBodyEvent, Session};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    pub const PROXY_ADDR: &str = "127.0.0.1:6161";

    /// How many terminal request-body events the
    /// application received. The pump owes the application EXACTLY one per
    /// request, and the RFC 9113 section 8.1 handling must not spend it.
    pub static TERMINAL_EVENTS: AtomicUsize = AtomicUsize::new(0);
    /// How many terminal events identified an abandoned downstream body.
    pub static ABANDONED_EVENTS: AtomicUsize = AtomicUsize::new(0);
    /// How many times the exchange reached `logging`, i.e. finished. Bumped
    /// AFTER `TERMINAL_EVENTS`, so a reader that sees this also sees that.
    pub static COMPLETED: AtomicUsize = AtomicUsize::new(0);

    /// Serializes the tests that drive THIS proxy.
    ///
    /// The three counters above are process-global and per-transport, not
    /// per-request, so every claim about them is a claim about a DELTA across
    /// one exchange. Two tests uploading through this proxy at the same time --
    /// libtest runs the tests of one binary on parallel threads -- would each
    /// see the other's increments, and both would fail (or, worse, pass for the
    /// wrong reason). The tests using `tests/utils`'s proxy on 6147 are
    /// unaffected and stay parallel.
    pub static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    pub struct ObservingProxy {}

    #[async_trait]
    impl ProxyHttp for ObservingProxy {
        type CTX = ();
        fn new_ctx(&self) {}

        async fn upstream_peer(
            &self,
            session: &mut Session,
            _ctx: &mut (),
        ) -> Result<Box<HttpPeer>> {
            let port = session
                .req_header()
                .headers
                .get("x-port")
                .map_or("8000", |v| v.to_str().unwrap())
                .to_string();
            let mut peer = Box::new(HttpPeer::new(
                format!("127.0.0.1:{port}"),
                false,
                String::new(),
            ));
            peer.options.set_http_version(2, 2);
            Ok(peer)
        }

        async fn request_body_filter_action(
            &self,
            _session: &mut Session,
            _body: &mut Option<Bytes>,
            event: RequestBodyEvent,
            _ctx: &mut (),
        ) -> Result<RequestBodyAction> {
            if event.is_terminal() {
                TERMINAL_EVENTS.fetch_add(1, Ordering::SeqCst);
            }
            if event == RequestBodyEvent::Abandoned {
                ABANDONED_EVENTS.fetch_add(1, Ordering::SeqCst);
            }
            Ok(RequestBodyAction::Continue)
        }

        async fn logging(
            &self,
            _session: &mut Session,
            _e: Option<&pingora_error::Error>,
            _ctx: &mut (),
        ) {
            COMPLETED.fetch_add(1, Ordering::SeqCst);
        }
    }

    static PROXY: Lazy<()> = Lazy::new(|| {
        thread::spawn(|| {
            let conf = std::sync::Arc::new(ServerConf {
                client_bind_to_ipv4: vec![],
                client_bind_to_ipv6: vec![],
                upstream_keepalive_pool_size: 10,
                ..Default::default()
            });
            let mut server = Server::new(None).unwrap();
            server.bootstrap();
            let mut svc = pingora_proxy::http_proxy_service(&conf, ObservingProxy {});
            svc.add_tcp(PROXY_ADDR);
            let services: Vec<Box<dyn ServiceWithDependents>> = vec![Box::new(svc)];
            server.add_services(services);
            server.run_forever();
        });

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if std::net::TcpStream::connect(PROXY_ADDR).is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the observing proxy never started listening on {PROXY_ADDR}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    });

    pub fn init() {
        Lazy::force(&PROXY);
    }
}

/// I1: the application's single terminal request-body event must survive
/// the RFC 9113 section 8.1 handling.
///
/// The canonical shape fails a MID-body write (`data = Some(chunk)`,
/// `end_of_body = false`): the origin resets precisely BECAUSE this side is
/// still uploading. Handling that by finishing the downstream read side also
/// disables `downstream_body_read_is_futile` (which requires `is_reading()`),
/// so without the explicit `Abandoned` event the hooks never learn that the
/// body was cut short -- while the request completes 200 and logs as a success.
///
/// The client here holds its chunked body open past the reset and then simply
/// stops, so the pump never reads a downstream EOF of its own: if the handler
/// does not deliver the event, nothing else will.
#[tokio::test]
async fn h2_upstream_no_error_reset_reports_abandoned_request_body() {
    observing::init();
    // See `observing::SERIAL`: the counters below are process-global, so this
    // exchange must be the only one going through that proxy.
    let _serial = observing::SERIAL.lock().await;
    let port = spawn_ok_then_reset_origin(Duration::from_millis(100)).await;

    let before_terminal = observing::TERMINAL_EVENTS.load(std::sync::atomic::Ordering::SeqCst);
    let before_abandoned = observing::ABANDONED_EVENTS.load(std::sync::atomic::Ordering::SeqCst);
    let before_done = observing::COMPLETED.load(std::sync::atomic::Ordering::SeqCst);

    let mut io = TcpStream::connect(observing::PROXY_ADDR).await.unwrap();
    io.write_all(
        format!(
            "POST /upload HTTP/1.1\r\nHost: {}\r\nx-port: {port}\r\n\
             Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n",
            observing::PROXY_ADDR
        )
        .as_bytes(),
    )
    .await
    .unwrap();

    // The whole response arrives while the request body is still open.
    let response = read_until(&mut io, "0\r\n\r\n").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains(RESPONSE_BODY), "{response}");

    // Now push one more chunk, AFTER the upstream's RST_STREAM(NO_ERROR) has
    // landed. That write is the one that fails mid-body.
    tokio::time::sleep(Duration::from_millis(600)).await;
    io.write_all(b"5\r\nworld\r\n").await.unwrap();

    // Wait for the exchange to finish. The client never sends its terminating
    // chunk, so the only terminal notification can be the pump's explicit
    // `Abandoned` event.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while observing::COMPLETED.load(std::sync::atomic::Ordering::SeqCst) == before_done {
        assert!(
            std::time::Instant::now() < deadline,
            "the exchange never finished"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        observing::TERMINAL_EVENTS.load(std::sync::atomic::Ordering::SeqCst),
        before_terminal + 1,
        "the application is owed exactly one terminal request-body event, and the \
         upstream's stop-uploading reset must not spend it"
    );
    assert_eq!(
        observing::ABANDONED_EVENTS.load(std::sync::atomic::Ordering::SeqCst),
        before_abandoned + 1,
        "the synthetic terminal event must identify the incomplete downstream body as abandoned"
    );
    drop(io);
}

/// Spawn a cleartext-h2 origin that DRAINS the whole request body and only then
/// answers 200 with a short body. One request only.
///
/// Draining first is what makes the 200 evidence that the proxy really forwarded
/// the complete request body, rather than a race against it.
async fn spawn_h2_drain_then_ok_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let mut body = req.into_parts().1;
        // The drain runs in its OWN task: only `conn.accept()` drives the
        // connection's I/O, so draining inline would stop the flow-control
        // updates below from ever reaching the peer -- and the upload, which is
        // far larger than the initial window, would stall forever.
        tokio::spawn(async move {
            while let Some(chunk) = body.data().await {
                let Ok(chunk) = chunk else { break };
                // Reopen the window, or the peer stops after one window's worth.
                let _ = body.flow_control().release_capacity(chunk.len());
            }
            let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
            if let Ok(mut send_stream) = send_resp.send_response(resp, false) {
                let _ = send_stream.send_data(Bytes::from(RESPONSE_BODY), true);
            }
        });
        while conn.accept().await.is_some() {}
    });
    port
}

/// The NEGATIVE direction of I1, on the same observing proxy: an upload the
/// client really finishes must be reported as a completion and NOT as an
/// abandonment.
///
/// `TERMINAL_EVENTS` alone cannot state this. It is bumped on
/// `event.is_terminal()`, which is true for `Abandoned` as well as for
/// `Complete`, so a pump that labelled every normal end-of-stream `Abandoned`
/// (the one-character regression of writing `is_terminal()` where the code means
/// `is_complete()`, or of reusing the `Abandoned` constant as "the terminal
/// one") would keep the assertion in
/// `h2_upstream_no_error_reset_reports_abandoned_request_body` -- and every
/// `x-eos-events: 1` assertion in the seam suite -- exactly as green as it is
/// now. What it would break is everything downstream of the distinction: a
/// filter that cancels its mirror on `Abandoned` would cancel every request, and
/// a bridge that only sets `end_of_stream = true` on `Complete` would never set
/// it at all.
///
/// So the load-bearing assertion here is that `ABANDONED_EVENTS` did NOT move.
#[tokio::test]
async fn h2_upstream_completed_upload_is_not_reported_as_abandoned() {
    observing::init();
    // See `observing::SERIAL`: the counters below are process-global, so this
    // exchange must be the only one going through that proxy.
    let _serial = observing::SERIAL.lock().await;
    let port = spawn_h2_drain_then_ok_origin().await;

    let before_terminal = observing::TERMINAL_EVENTS.load(std::sync::atomic::Ordering::SeqCst);
    let before_abandoned = observing::ABANDONED_EVENTS.load(std::sync::atomic::Ordering::SeqCst);
    let before_done = observing::COMPLETED.load(std::sync::atomic::Ordering::SeqCst);

    // A real, finished body: `reqwest` sends it with a `Content-Length`, so the
    // downstream read side observes the transport's own end of the body.
    let res = reqwest::Client::new()
        .post(format!("http://{}/upload", observing::PROXY_ADDR))
        .header("x-port", port.to_string())
        .body("x".repeat(UPLOAD_LEN))
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("the upload must succeed against an origin that drains it");
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), RESPONSE_BODY);

    // `COMPLETED` is bumped after the body events, so waiting on it is what makes
    // the two reads below final rather than a snapshot of a request in flight.
    let deadline = Instant::now() + Duration::from_secs(10);
    while observing::COMPLETED.load(std::sync::atomic::Ordering::SeqCst) == before_done {
        assert!(Instant::now() < deadline, "the exchange never finished");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        observing::ABANDONED_EVENTS.load(std::sync::atomic::Ordering::SeqCst),
        before_abandoned,
        "a downstream body the client really ended must never be reported as \
         abandoned: `Abandoned` means the delivered bytes are only a prefix, and \
         nothing here cut the body short"
    );
    assert_eq!(
        observing::TERMINAL_EVENTS.load(std::sync::atomic::Ordering::SeqCst),
        before_terminal + 1,
        "the completed body still owes the application exactly one terminal \
         request-body event"
    );
}

/// The response the test below streams, as many small DATA frames. The COUNT
/// is what matters, not the size: each frame becomes one `HttpTask` on the
/// 4-slot pipe between the two halves of the pump, so it is the number of
/// separate turns the response arm needs -- and therefore the number of
/// chances the downstream read arm gets while the response is still going out.
const FRAGMENT_COUNT: usize = 200;
const FRAGMENT: &str = "0123456789";

/// Spawn a cleartext-h2 origin that answers in many small frames and then
/// resets with NO_ERROR. It never reads the request body, so the proxy is
/// blocked on the stream's flow-control window for the whole exchange.
async fn spawn_fragmented_ok_then_reset_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        for i in 0..FRAGMENT_COUNT {
            send_stream
                .send_data(Bytes::from(FRAGMENT), i + 1 == FRAGMENT_COUNT)
                .unwrap();
        }

        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        send_stream.send_reset(h2::Reason::NO_ERROR);
        while conn.accept().await.is_some() {}
    });
    port
}

/// M1: the `!upstream_stopped_receiving` select guard is CONDITIONAL, and this
/// pins the condition.
///
/// Once the upstream stops receiving, `downstream_state` says "finished
/// reading" -- but the loop keeps running for as long as the RESPONSE still has
/// to go out. In that window the read branch would still be polled, and
/// `read_body_or_idle(no_body = true)` answers a client that is still uploading
/// with `ConnectError("Sent data after end of body")`, which the loop turns
/// into `Err(into_down())`: the connection is dropped mid-response and the
/// client loses a response the section 8.1 handling had just rescued.
///
/// Both halves of the window are forced here, and neither is incidental:
///
/// - The origin never reads the request body, so the pump spends the whole
///   exchange blocked inside `write_body` on the stream's flow-control window.
///   The response arm therefore does not run at all before the reset lands, and
///   `response_state` is provably not done at the moment the write fails.
/// - The response arrives as [`FRAGMENT_COUNT`] separate frames, so draining it
///   afterwards takes dozens of turns of the `select!`. A guard that is missing
///   is then observed with near-certainty rather than with the coin flip a
///   one-frame response would give.
///
/// The other tests in this file leave this window unentered or enter it only by
/// accident; do not treat their passing as coverage for this line.
#[tokio::test]
async fn h2_upstream_no_error_reset_keeps_streaming_while_the_client_uploads() {
    init_without_mock_origin();
    let port = spawn_fragmented_ok_then_reset_origin().await;
    let res = post_through_proxy(port)
        .await
        .expect("the complete response must be delivered, not turned into an error");
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.text().await.expect(
            "the client's continuing upload must not be turned into a downstream error \
             while the response is still being written"
        ),
        FRAGMENT.repeat(FRAGMENT_COUNT)
    );
}
