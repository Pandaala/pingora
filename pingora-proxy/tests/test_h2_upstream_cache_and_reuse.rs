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

//! End-to-end consumers for the H2 cache/reuse observability in
//! `tests/utils/server_utils.rs`.
//!
//! The contracts under test are the ones the §8.1 / GOAWAY / writer-capacity
//! work depends on but that downstream-keepalive assertions cannot see:
//!
//! 1. A complete upstream exchange admits a COMPLETE cache entry, and the next
//!    miss to the same origin travels on the SAME upstream socket -- stream 1
//!    then stream 3 of one h2 connection. They are SEQUENTIAL, not concurrent:
//!    `PeerOptions::new()` sets `max_h2_streams: 1` (and `HttpPeer::new` uses
//!    it), so the connection is returned to the idle pool only once its stream
//!    count drops to zero.
//! 2. Every failed or ambiguous exchange leaves NO complete cache entry, so a
//!    second request has to reach the origin again.
//!
//! Assertions are made on origin socket identity (`x-upstream-client-addr`,
//! which is the proxy's local address on the upstream connection, so equality
//! means one connection) and on cache state read through the cache's own
//! public interfaces ([`cache_entry_state`]). Downstream keepalive is
//! deliberately NOT used as evidence: it survives failures that matter here.

mod utils;

use bytes::Bytes;
use http::{Response, StatusCode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use utils::server_utils::{cache_entry_state, init_without_mock_origin, CacheEntryState};

/// The cache proxy's downstream address, and therefore the `Host` half of the
/// cache key `ExampleProxyCache::cache_key_callback` builds.
const CACHE_PROXY_HOST: &str = "127.0.0.1:6148";

const ORIGIN_BODY: &str = "cache-and-reuse origin body";

/// What an origin observed, so a test can assert that a second request really
/// reached it rather than being served from cache.
#[derive(Default)]
struct OriginStats {
    connections: AtomicUsize,
    requests: AtomicUsize,
}

impl OriginStats {
    fn connections(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }
}

/// Build a cache-key path unique to this process AND this test, so concurrently
/// running tests in this binary cannot collide in the shared `MemCache`.
fn unique_path(test: &str) -> String {
    format!("/h2-cache-reuse/{}/{}", std::process::id(), test)
}

/// Spawn a cleartext-h2 origin that serves EVERY stream on EVERY connection
/// with the same cacheable 200. It stays up for the life of the test, which is
/// what makes upstream connection reuse possible in the first place.
async fn spawn_reusable_origin() -> (u16, Arc<OriginStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let stats = Arc::new(OriginStats::default());
    let origin_stats = stats.clone();
    tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            origin_stats.connections.fetch_add(1, Ordering::Relaxed);
            let conn_stats = origin_stats.clone();
            tokio::spawn(async move {
                let mut conn = h2::server::handshake(io).await.unwrap();
                while let Some(Ok((_req, mut send_resp))) = conn.accept().await {
                    conn_stats.requests.fetch_add(1, Ordering::Relaxed);
                    let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
                    let mut body = send_resp.send_response(response, false).unwrap();
                    body.send_data(Bytes::from(ORIGIN_BODY), true).unwrap();
                }
            });
        }
    });
    (port, stats)
}

/// GET `path` through the cache proxy against a cleartext-h2 origin on `port`.
fn cache_proxy_get(port: u16, path: &str) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .get(format!("http://{CACHE_PROXY_HOST}{path}"))
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .timeout(Duration::from_secs(10))
}

fn upstream_socket(res: &reqwest::Response) -> String {
    res.headers()
        .get("x-upstream-client-addr")
        .expect("the cache proxy reports the upstream socket it used")
        .to_str()
        .unwrap()
        .to_string()
}

/// The positive baseline every failure case below is measured against: a
/// complete exchange admits a complete entry, and the NEXT miss to the same
/// origin reuses the connection the first one opened.
///
/// The two requests use different paths on purpose. A repeat of the first path
/// would be a cache hit and would never touch the upstream at all, so it could
/// not say anything about reuse; distinct paths force a second upstream
/// exchange, which is stream 3 on the socket stream 1 already used -- issued
/// after stream 1 closed, not alongside it.
///
/// Because `max_h2_streams` is 1, the connection reaches the idle pool only
/// after the first `Http2Session` is dropped and its stream slot released, so
/// the socket-identity assertion also pins that release.
#[tokio::test]
async fn h2_complete_exchange_caches_and_reuses_the_upstream_connection() {
    init_without_mock_origin();
    let (port, stats) = spawn_reusable_origin().await;
    let first = unique_path("reuse-1");
    let second = unique_path("reuse-2");

    let res = cache_proxy_get(port, &first).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("x-cache-status").unwrap(), "miss");
    let stream_1_socket = upstream_socket(&res);
    assert_eq!(res.text().await.unwrap(), ORIGIN_BODY);

    assert_eq!(
        cache_entry_state(CACHE_PROXY_HOST, &first).await,
        CacheEntryState::Complete,
        "a complete upstream exchange must admit a complete cache entry"
    );

    let res = cache_proxy_get(port, &second).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("x-cache-status").unwrap(), "miss");
    let stream_3_socket = upstream_socket(&res);
    assert!(
        res.headers().get("x-conn-reuse").is_some(),
        "the second upstream exchange must run on a pooled connection"
    );
    assert_eq!(res.text().await.unwrap(), ORIGIN_BODY);

    assert_eq!(
        stream_1_socket, stream_3_socket,
        "both exchanges must run on one upstream socket"
    );
    assert_eq!(
        stats.connections(),
        1,
        "reuse means the origin accepted exactly one connection"
    );
    assert_eq!(stats.requests(), 2, "both misses must reach the origin");

    // The first entry is still complete and serves a hit without the origin.
    //
    // `x-force-fresh` is robustness, not weakening: `CACHE_DEFAULT` grants only
    // a 1s freshness window, which a loaded CI run can outlast between the
    // first response and this request. The load-bearing assertion is the
    // request counter below -- the entry was admitted complete and answers
    // without the origin -- and that is unaffected by whether the clock has
    // rolled past the freshness horizon.
    let res = cache_proxy_get(port, &first)
        .header("x-force-fresh", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(res.headers().get("x-cache-status").unwrap(), "hit");
    assert_eq!(res.text().await.unwrap(), ORIGIN_BODY);
    assert_eq!(stats.requests(), 2, "a hit must not reach the origin");
}

// ---------------------------------------------------------------------------
// Failure cases. Each one asserts the same two things:
//
//   * `cache_entry_state` is not `Complete` -- a failed exchange must never
//     leave behind an entry a later request could be served from. `None` and
//     `Partial` are both acceptable outcomes; what must never happen is a
//     complete entry holding a body the origin never finished sending.
//   * the origin's request counter advances on the SECOND request, which is
//     the operational form of the same statement: the proxy had to go back to
//     the origin because nothing servable was cached.
// ---------------------------------------------------------------------------

/// Spawn a cleartext-h2 origin that answers every stream with 200 + a body that
/// NEVER carries END_STREAM, and then resets the stream with NO_ERROR. The
/// response is truncated on the wire no matter how benign the reset code is.
async fn spawn_truncating_origin() -> (u16, Arc<OriginStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let stats = Arc::new(OriginStats::default());
    let origin_stats = stats.clone();
    tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            origin_stats.connections.fetch_add(1, Ordering::Relaxed);
            let conn_stats = origin_stats.clone();
            tokio::spawn(async move {
                let mut conn = h2::server::handshake(io).await.unwrap();
                while let Some(Ok((_req, mut send_resp))) = conn.accept().await {
                    conn_stats.requests.fetch_add(1, Ordering::Relaxed);
                    let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
                    let mut body = send_resp.send_response(response, false).unwrap();
                    body.send_data(Bytes::from(ORIGIN_BODY), false).unwrap();
                    // The response frames have to reach the wire BEFORE the
                    // reset, which discards the stream's pending send queue --
                    // otherwise this degenerates into a header-only failure and
                    // the scenario stops testing truncation at all.
                    //
                    // The wait therefore runs in its own task: an `h2`
                    // connection only progresses while something polls it, so
                    // sleeping here would park the connection and guarantee the
                    // frames are NOT flushed.
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        body.send_reset(h2::Reason::NO_ERROR);
                    });
                }
            });
        }
    });
    (port, stats)
}

/// A body that stops without END_STREAM must not be laundered into a cache
/// entry, however benign the reset code that follows it is.
///
/// NO_ERROR is the interesting reset here: RFC 9113 section 8.1 makes it a
/// legitimate "stop uploading, the response is done" signal, and the proxy
/// honours it -- but only for a response the wire actually terminated. Without
/// the END_STREAM record, the very same reset must stay an error, and cache
/// admission is where a mistake would become durable.
#[tokio::test]
async fn h2_truncated_response_admits_no_cache_entry() {
    init_without_mock_origin();
    let (port, stats) = spawn_truncating_origin().await;
    let path = unique_path("truncated");

    let first = cache_proxy_get(port, &path).send().await;
    // The truncation may surface either as a proxy-generated error status or as
    // a broken downstream body; both are correct, and neither may cache.
    if let Ok(res) = first {
        let status = res.status();
        let body = res.text().await;
        assert!(
            status != StatusCode::OK || body.is_err() || body.unwrap() != ORIGIN_BODY,
            "a truncated upstream response must not be delivered as a complete 200"
        );
    }

    assert_ne!(
        cache_entry_state(CACHE_PROXY_HOST, &path).await,
        CacheEntryState::Complete,
        "a truncated exchange must not admit a complete cache entry"
    );

    let requests_after_first = stats.requests();
    let _ = cache_proxy_get(port, &path).send().await;
    assert_eq!(
        stats.requests(),
        requests_after_first + 1,
        "the second request must reach the origin, i.e. miss cache"
    );
}

/// A local response-body filter failure fails the exchange on THIS side, with
/// the origin behaving perfectly. The cache entry must be abandoned all the
/// same: what the client never received must not become what the next client
/// is served.
#[tokio::test]
async fn h2_local_response_body_failure_admits_no_cache_entry() {
    init_without_mock_origin();
    let (port, stats) = spawn_reusable_origin().await;
    let path = unique_path("local-filter-failure");

    let first = cache_proxy_get(port, &path)
        .header("x-test-local-response-body-failure", "true")
        .send()
        .await;
    if let Ok(res) = first {
        let status = res.status();
        let body = res.text().await;
        assert!(
            status != StatusCode::OK || body.is_err() || body.unwrap() != ORIGIN_BODY,
            "a locally failed response body must not be delivered as a complete 200"
        );
    }

    assert_ne!(
        cache_entry_state(CACHE_PROXY_HOST, &path).await,
        CacheEntryState::Complete,
        "a locally failed exchange must not admit a complete cache entry"
    );

    let requests_after_first = stats.requests();
    let _ = cache_proxy_get(port, &path)
        .header("x-test-local-response-body-failure", "true")
        .send()
        .await;
    assert_eq!(
        stats.requests(),
        requests_after_first + 1,
        "the second request must reach the origin, i.e. miss cache"
    );
}

// ---------------------------------------------------------------------------
// The invalid-trailer case needs a RAW-WIRE origin.
//
// A trailer block carrying a response pseudo-header is illegal, and that is
// exactly what makes it unrepresentable in `h2::server`'s API: `:status` is not
// a valid `http::HeaderName`, so `send_trailers` cannot express this shape at
// all. The frames below are the wire form the `pingora-core` contract
// `h2_watched_invalid_trailers_reset_is_not_a_clean_eof` pins, driven end to
// end through the proxy for the first time here.
// ---------------------------------------------------------------------------

const FRAME_DATA: u8 = 0x0;
const FRAME_HEADERS: u8 = 0x1;
const FRAME_SETTINGS: u8 = 0x4;
const FRAME_PING: u8 = 0x6;
const FLAG_ACK: u8 = 0x1;
const FLAG_END_STREAM: u8 = 0x1;
const FLAG_END_HEADERS: u8 = 0x4;

/// HPACK static table index 8 is `:status: 200`. Legal in a response header
/// block, illegal in a trailer block -- the whole point of this origin.
const HPACK_STATUS_200: u8 = 0x88;

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn raw_frame(frame_type: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut frame = vec![
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        frame_type,
        flags,
    ];
    frame.extend_from_slice(&stream_id.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Read one h2 frame header plus its payload. Returns `None` at EOF.
async fn read_frame(io: &mut tokio::net::TcpStream) -> Option<(u8, u8, u32, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let mut header = [0u8; 9];
    io.read_exact(&mut header).await.ok()?;
    let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let frame_type = header[3];
    let flags = header[4];
    let stream_id = u32::from_be_bytes([header[5] & 0x7f, header[6], header[7], header[8]]);
    let mut payload = vec![0u8; len];
    io.read_exact(&mut payload).await.ok()?;
    Some((frame_type, flags, stream_id, payload))
}

/// Spawn a raw-wire cleartext-h2 origin that answers each request with
/// `200` + DATA + a TRAILERS block containing a response pseudo-header.
///
/// It speaks only as much of the protocol as the exchange needs: the preface,
/// a SETTINGS exchange, SETTINGS/PING acknowledgements, and the response
/// frames. Everything else on the wire is ignored, which is safe because the
/// proxy is the only peer and the exchange is a single short GET.
async fn spawn_invalid_trailer_origin() -> (u16, Arc<OriginStats>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let stats = Arc::new(OriginStats::default());
    let origin_stats = stats.clone();
    tokio::spawn(async move {
        while let Ok((mut io, _)) = listener.accept().await {
            origin_stats.connections.fetch_add(1, Ordering::Relaxed);
            let conn_stats = origin_stats.clone();
            tokio::spawn(async move {
                let mut preface = [0u8; 24];
                if io.read_exact(&mut preface).await.is_err() || preface != H2_PREFACE[..] {
                    return;
                }
                if io
                    .write_all(&raw_frame(FRAME_SETTINGS, 0, 0, &[]))
                    .await
                    .is_err()
                {
                    return;
                }
                while let Some((frame_type, flags, stream_id, payload)) = read_frame(&mut io).await
                {
                    let reply = match frame_type {
                        FRAME_SETTINGS if flags & FLAG_ACK == 0 => {
                            raw_frame(FRAME_SETTINGS, FLAG_ACK, 0, &[])
                        }
                        FRAME_PING if flags & FLAG_ACK == 0 => {
                            raw_frame(FRAME_PING, FLAG_ACK, 0, &payload)
                        }
                        FRAME_HEADERS => {
                            conn_stats.requests.fetch_add(1, Ordering::Relaxed);
                            let mut frames = raw_frame(
                                FRAME_HEADERS,
                                FLAG_END_HEADERS,
                                stream_id,
                                &[HPACK_STATUS_200],
                            );
                            frames.extend_from_slice(&raw_frame(
                                FRAME_DATA,
                                0,
                                stream_id,
                                ORIGIN_BODY.as_bytes(),
                            ));
                            // The illegal terminal block: a trailer HEADERS
                            // frame whose only field is `:status: 200`.
                            frames.extend_from_slice(&raw_frame(
                                FRAME_HEADERS,
                                FLAG_END_HEADERS | FLAG_END_STREAM,
                                stream_id,
                                &[HPACK_STATUS_200],
                            ));
                            frames
                        }
                        _ => continue,
                    };
                    if io.write_all(&reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (port, stats)
}

/// An END_STREAM the codec REJECTED is not completion evidence, so the body it
/// terminates must not be admitted to cache.
///
/// This is the ambiguous half of the acceptance criteria: unlike the truncation
/// case, every byte of the body did arrive and a terminal block did carry
/// END_STREAM. Only the block's validity separates this from a complete
/// exchange -- which is precisely why cache state, rather than downstream
/// keepalive, has to be the assertion.
#[tokio::test]
async fn h2_invalid_trailers_admit_no_cache_entry() {
    init_without_mock_origin();
    let (port, stats) = spawn_invalid_trailer_origin().await;
    let path = unique_path("invalid-trailers");

    let first = cache_proxy_get(port, &path).send().await;
    if let Ok(res) = first {
        let status = res.status();
        let body = res.text().await;
        assert!(
            status != StatusCode::OK || body.is_err() || body.unwrap() != ORIGIN_BODY,
            "a rejected terminal block must not be delivered as a complete 200"
        );
    }

    assert_ne!(
        cache_entry_state(CACHE_PROXY_HOST, &path).await,
        CacheEntryState::Complete,
        "rejected trailers must not admit a complete cache entry"
    );

    let requests_after_first = stats.requests();
    let _ = cache_proxy_get(port, &path).send().await;
    assert_eq!(
        stats.requests(),
        requests_after_first + 1,
        "the second request must reach the origin, i.e. miss cache"
    );
}

// ---------------------------------------------------------------------------
// Flow control.
//
// `x-h2-stream-window-size` sets the window the proxy advertises for RECEIVING
// the response, so a small value is what makes the stall point deterministic:
// the origin cannot dump the whole body into the proxy's buffers in one go, and
// the exchange is provably still mid-flow-control when the interesting thing
// happens. Without the injection these shapes race against buffer sizes.
//
// Both directions share one upload constant: the origins here never read the
// request body, so everything past one window sits blocked in the request pump
// for the whole exchange. That is the writer-capacity condition under test.
// ---------------------------------------------------------------------------

/// Injected on the PROXY'S RECEIVE side. Small enough that a
/// [`flow_controlled_body`]-sized response needs several WINDOW_UPDATE round
/// trips to cross it.
const INJECTED_STREAM_WINDOW: usize = 4096;

/// Advertised by the origin, so it caps the PROXY'S WRITER capacity: the pump
/// may place this many bytes of the upload and must then park. This is the knob
/// the writer-capacity contract is about; the injected receive window above
/// cannot express it, because it governs the other direction.
const ORIGIN_ADVERTISED_WINDOW: u32 = 8192;

/// Comfortably larger than [`INJECTED_STREAM_WINDOW`], which is all this needs
/// to be. The baseline it has to beat is the INJECTED window, not the default
/// one: pingora's un-injected upstream stream window is
/// `H2_WINDOW_SIZE = 1 << 23` (8 MiB, `pingora-core/src/connectors/http/v2.rs`),
/// not h2's 65535, so any body of a sane test size crosses in one shot without
/// the injection.
const FLOW_CONTROLLED_BODY_LEN: usize = INJECTED_STREAM_WINDOW * 8;

/// Big enough that the request pump is provably still writing when the response
/// side finishes: the origins below never read it, so everything past one
/// origin-advertised window stays blocked.
const UPLOAD_LEN: usize = 1024 * 1024;

fn flow_controlled_body() -> Bytes {
    Bytes::from(vec![b'z'; FLOW_CONTROLLED_BODY_LEN])
}

/// POST `path` through the cache proxy with an upload the origin will never
/// drain, so the request pump spends the exchange blocked on the origin's
/// window.
fn stalled_cache_proxy_post(port: u16, path: &str) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .post(format!("http://{CACHE_PROXY_HOST}{path}"))
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .body("x".repeat(UPLOAD_LEN))
        .timeout(Duration::from_secs(10))
}

/// The same upload, plus a small injected RECEIVE window so the response side
/// is provably still crossing flow control when the origin walks away.
fn windowed_cache_proxy_post(port: u16, path: &str) -> reqwest::RequestBuilder {
    stalled_cache_proxy_post(port, path).header(
        "x-h2-stream-window-size",
        INJECTED_STREAM_WINDOW.to_string(),
    )
}

/// Spawn a cleartext-h2 origin that never reads the request body and answers
/// every stream with a SHORT body finished by END_STREAM, then resets the
/// stream with NO_ERROR -- the RFC 9113 section 8.1 shape.
///
/// The flow control in this shape is entirely on the WRITE side: the origin
/// advertises [`ORIGIN_ADVERTISED_WINDOW`], which is what caps the proxy's
/// writer. The response body is deliberately short (see the inline comment
/// below).
///
/// The response work runs in its own task so that the accept loop keeps
/// DRIVING the connection while it waits. An `h2` connection only makes
/// progress while something polls it, so sleeping inside the accept loop would
/// leave the queued frames unflushed and `send_reset` -- which discards the
/// stream's pending send queue -- would then delete the very response this
/// shape is about.
async fn spawn_flow_controlled_complete_origin() -> (u16, Arc<OriginStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let stats = Arc::new(OriginStats::default());
    let origin_stats = stats.clone();
    tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            origin_stats.connections.fetch_add(1, Ordering::Relaxed);
            let conn_stats = origin_stats.clone();
            tokio::spawn(async move {
                let mut conn = h2::server::Builder::new()
                    .initial_window_size(ORIGIN_ADVERTISED_WINDOW)
                    .handshake(io)
                    .await
                    .unwrap();
                while let Some(Ok((_req, mut send_resp))) = conn.accept().await {
                    conn_stats.requests.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
                        let mut body = send_resp.send_response(response, false).unwrap();
                        body.send_data(Bytes::from(ORIGIN_BODY), true).unwrap();
                        // Let the accept loop flush the response before the
                        // reset discards what is still queued. The body is kept
                        // short on purpose: an abandoned upload costs the
                        // downstream connection its keepalive, so a large
                        // response would race that close instead of testing
                        // the writer window.
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        // RFC 9113 section 8.1: stop uploading, the response is done.
                        body.send_reset(h2::Reason::NO_ERROR);
                    });
                }
            });
        }
    });
    (port, stats)
}

/// Spawn an origin whose first stream withholds both request-body capacity and
/// response END_STREAM. After the proxy's H2 write floor resets that stream,
/// the same connection serves subsequent streams normally.
async fn spawn_unterminated_stall_then_recover_origin() -> (u16, Arc<OriginStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let stats = Arc::new(OriginStats::default());
    let origin_stats = stats.clone();
    tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            origin_stats.connections.fetch_add(1, Ordering::Relaxed);
            let conn_stats = origin_stats.clone();
            tokio::spawn(async move {
                let mut conn = h2::server::Builder::new()
                    .initial_window_size(ORIGIN_ADVERTISED_WINDOW)
                    .handshake(io)
                    .await
                    .unwrap();
                while let Some(Ok((req, mut send_resp))) = conn.accept().await {
                    let request_number = conn_stats.requests.fetch_add(1, Ordering::Relaxed) + 1;
                    let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
                    let mut body = send_resp.send_response(response, false).unwrap();
                    if request_number == 1 {
                        body.send_data(Bytes::from_static(b"unterminated"), false)
                            .unwrap();
                        // Retain the RecvStream without polling it so no upload
                        // WINDOW_UPDATE is generated. Retain the response
                        // SendStream so this side also never supplies EOS.
                        tokio::spawn(async move {
                            std::future::pending::<()>().await;
                            drop((req, body));
                        });
                    } else {
                        drop(req);
                        body.send_data(Bytes::from(ORIGIN_BODY), true).unwrap();
                    }
                }
            });
        }
    });
    (port, stats)
}

/// Spawn a cleartext-h2 origin that never reads the request body, enqueues a
/// flow-controlled body WITHOUT END_STREAM, lets a large prefix of it reach the
/// wire, and then drops the connection mid-body.
///
/// The `select!` is what makes "a prefix really arrived" true rather than
/// hopeful: it drives the connection for the flush window instead of parking
/// it, so the proxy is provably still opening its receive window when the
/// connection dies.
async fn spawn_flow_controlled_cut_short_origin() -> (u16, Arc<OriginStats>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let stats = Arc::new(OriginStats::default());
    let origin_stats = stats.clone();
    tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            origin_stats.connections.fetch_add(1, Ordering::Relaxed);
            let conn_stats = origin_stats.clone();
            tokio::spawn(async move {
                let mut conn = h2::server::handshake(io).await.unwrap();
                if let Some(Ok((_req, mut send_resp))) = conn.accept().await {
                    conn_stats.requests.fetch_add(1, Ordering::Relaxed);
                    let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
                    let mut body = send_resp.send_response(response, false).unwrap();
                    body.send_data(flow_controlled_body(), false).unwrap();
                    tokio::select! {
                        _ = async { while conn.accept().await.is_some() {} } => {}
                        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
                    }
                }
                // Dropping `conn` here kills the connection with the response
                // body unfinished.
            });
        }
    });
    (port, stats)
}

/// A response cut short while the proxy is still opening its receive window
/// must not be admitted, even though a large prefix of the body did arrive.
///
/// This is the scenario that consumes `x-h2-stream-window-size`, and the
/// injection genuinely shapes it: pingora's default upstream stream window is
/// 8 MiB (`H2_WINDOW_SIZE = 1 << 23`), so without the header this body would
/// land in a single shot and the flow-control loop would never be entered. At
/// [`INJECTED_STREAM_WINDOW`] it crosses in steps, so the loop is provably
/// still running when the connection dies.
///
/// Honest scope note: shaping is not asserting. Both assertions below would
/// still hold under the default window, because the origin never sends
/// END_STREAM either way. Pinning the window VALUE would need an observable
/// this harness does not expose today.
#[tokio::test]
async fn h2_flow_controlled_body_cut_short_admits_no_cache_entry() {
    init_without_mock_origin();
    let (port, stats) = spawn_flow_controlled_cut_short_origin().await;
    let path = unique_path("flow-control-cut-short");

    let first = windowed_cache_proxy_post(port, &path).send().await;
    if let Ok(res) = first {
        let status = res.status();
        let body = res.text().await;
        assert!(
            status != StatusCode::OK
                || body.is_err()
                || body.unwrap().len() != FLOW_CONTROLLED_BODY_LEN,
            "a body cut short under flow control must not be delivered as a complete 200"
        );
    }

    assert_ne!(
        cache_entry_state(CACHE_PROXY_HOST, &path).await,
        CacheEntryState::Complete,
        "a flow-controlled exchange cut short must not admit a complete cache entry"
    );

    let requests_after_first = stats.requests();
    let _ = windowed_cache_proxy_post(port, &path).send().await;
    assert_eq!(
        stats.requests(),
        requests_after_first + 1,
        "the second request must reach the origin, i.e. miss cache"
    );
}

/// The positive direction: a request-body write that spends the whole exchange
/// blocked on the origin's flow-control window must cost the exchange nothing.
/// The complete response is delivered and cached, and the NEXT request reuses
/// the same upstream socket.
///
/// Scope note -- what the reuse assertion does and does not prove. There is no
/// in-process handle on a tokio task, so this does not observe a pump task
/// being dropped. What it does establish is the observable consequence: the
/// stream and the writer capacity it held were released, because a leaked pump
/// still parked on the write half could not be followed by a successful second
/// exchange on that very connection. Read it as "capacity released", not as
/// "task reaped".
#[tokio::test]
async fn h2_flow_controlled_stall_releases_capacity_and_reuses_the_connection() {
    init_without_mock_origin();
    let (port, stats) = spawn_flow_controlled_complete_origin().await;
    let first = unique_path("flow-control-stall-1");
    let second = unique_path("flow-control-stall-2");

    let res = stalled_cache_proxy_post(port, &first).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let stalled_socket = upstream_socket(&res);
    assert_eq!(
        res.text().await.unwrap(),
        ORIGIN_BODY,
        "the complete response must survive the blocked request-body write"
    );

    assert_eq!(
        cache_entry_state(CACHE_PROXY_HOST, &first).await,
        CacheEntryState::Complete,
        "a complete response must be cached even when the upload never finished"
    );

    let res = stalled_cache_proxy_post(port, &second)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        upstream_socket(&res),
        stalled_socket,
        "the stalled writer must have released the connection for reuse"
    );
    assert_eq!(
        stats.connections(),
        1,
        "reuse means the origin accepted exactly one connection"
    );
}

/// Without response END_STREAM, the H2 write floor must fail the first
/// exchange, abandon any partial cache entry, avoid retrying the failed upload,
/// and release the stream slot so the connection can be reused. The generic
/// committed-final-response retry guard is covered separately in proxy unit
/// tests.
#[tokio::test]
async fn h2_unterminated_stall_fails_without_cache_or_capacity_leak() {
    init_without_mock_origin();
    let (port, stats) = spawn_unterminated_stall_then_recover_origin().await;
    let path = unique_path("unterminated-stall");

    let start = Instant::now();
    let first = reqwest::Client::new()
        .post(format!("http://{CACHE_PROXY_HOST}{path}"))
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .body("x".repeat(UPLOAD_LEN))
        .timeout(Duration::from_secs(30))
        .send()
        .await;
    if let Ok(res) = first {
        let status = res.status();
        let body = res.text().await;
        assert!(
            status.is_server_error() || body.is_err(),
            "the unterminated response must fail: status={status}, body={body:?}"
        );
    }
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "the H2 write floor, not the client's timeout, must end the first exchange"
    );

    assert_eq!(stats.requests(), 1, "the failed upload must not be retried");
    assert_ne!(
        cache_entry_state(CACHE_PROXY_HOST, &path).await,
        CacheEntryState::Complete,
        "an unterminated response must not produce a complete cache entry"
    );

    let res = cache_proxy_get(port, &path).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("x-cache-status").unwrap(), "miss");
    assert_eq!(res.text().await.unwrap(), ORIGIN_BODY);
    assert_eq!(stats.requests(), 2, "the failed entry must miss cache");
    assert_eq!(
        stats.connections(),
        1,
        "the timed-out stream must release capacity for reuse on the same H2 connection"
    );
}

// ---------------------------------------------------------------------------
// The two peer window options, asserted where they are actually expressible:
// on the wire.
//
// They are NOT interchangeable and they are not even carried the same way, so
// one raw-wire origin can tell them apart without ambiguity:
//
//   * the stream window is a SETTINGS parameter -- `SETTINGS_INITIAL_WINDOW_SIZE`
//     (0x4) -- whose value is the configured number itself;
//   * the connection window has no SETTINGS parameter at all. HTTP/2 fixes the
//     initial connection window at [`H2_DEFAULT_CONNECTION_WINDOW`], and the
//     only way to change it is a WINDOW_UPDATE on stream 0, so the configured
//     value shows up as an INCREMENT above that default.
//
// Scope note: this asserts that each option is plumbed through to the h2
// handshake, which is what the harness knob exists for. It does not assert the
// flow-control BEHAVIOUR that follows from it -- nothing here observes that.
// ---------------------------------------------------------------------------

const FRAME_WINDOW_UPDATE: u8 = 0x8;
const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x4;

/// HTTP/2's fixed initial connection-level flow-control window (RFC 9113
/// section 6.9.2). A configured connection window is expressed relative to it.
const H2_DEFAULT_CONNECTION_WINDOW: u32 = 65535;

/// What pingora advertises for BOTH windows when neither option is set:
/// `H2_WINDOW_SIZE` in `pingora-core/src/connectors/http/v2.rs`.
const PINGORA_DEFAULT_H2_WINDOW: u32 = 1 << 23;

/// What the proxy put on the wire during the connection handshake, i.e. before
/// it opened its first stream.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct HandshakeWindows {
    /// The `SETTINGS_INITIAL_WINDOW_SIZE` the proxy advertised, if it sent one.
    stream_window: Option<u32>,
    /// The increment the proxy sent on stream 0 DURING THE HANDSHAKE. `None`
    /// means it never raised the connection window -- which is what happens for
    /// any configured value at or below [`H2_DEFAULT_CONNECTION_WINDOW`], since
    /// WINDOW_UPDATE can only raise a window, never lower it.
    connection_window_raise: Option<u32>,
}

/// Spawn a raw-wire cleartext-h2 origin that records the proxy's handshake
/// windows and then answers the request normally.
///
/// Recording genuinely stops at the first HEADERS this origin sends a response
/// to, which is what keeps the observation about the HANDSHAKE alone. Two
/// reasons it has to:
///
/// - A `None` must mean "never sent", not "not yet". `h2` flushes a pending
///   connection-level WINDOW_UPDATE ahead of any stream frame in the same write
///   pass, so by the time HEADERS is in hand the handshake raise has provably
///   already arrived (or was never coming).
/// - Later stream-0 updates must not contaminate the value. Once the response
///   body grows past h2's re-update threshold the peer emits further
///   connection-level updates, and accumulating those into the same field would
///   silently break the equality assertions for a reason that has nothing to do
///   with the configured window.
async fn spawn_window_recording_origin() -> (u16, Arc<std::sync::Mutex<HandshakeWindows>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let observed = Arc::new(std::sync::Mutex::new(HandshakeWindows::default()));
    let origin_observed = observed.clone();
    tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut preface = [0u8; 24];
        if io.read_exact(&mut preface).await.is_err() || preface != H2_PREFACE[..] {
            return;
        }
        if io
            .write_all(&raw_frame(FRAME_SETTINGS, 0, 0, &[]))
            .await
            .is_err()
        {
            return;
        }
        let mut handshake_done = false;
        while let Some((frame_type, flags, stream_id, payload)) = read_frame(&mut io).await {
            let reply = match frame_type {
                FRAME_SETTINGS if flags & FLAG_ACK == 0 => {
                    for param in payload.chunks(6) {
                        if param.len() == 6
                            && u16::from_be_bytes([param[0], param[1]])
                                == SETTINGS_INITIAL_WINDOW_SIZE
                        {
                            let value =
                                u32::from_be_bytes([param[2], param[3], param[4], param[5]]);
                            origin_observed.lock().unwrap().stream_window = Some(value);
                        }
                    }
                    raw_frame(FRAME_SETTINGS, FLAG_ACK, 0, &[])
                }
                FRAME_WINDOW_UPDATE if stream_id == 0 && payload.len() == 4 && !handshake_done => {
                    // The reserved bit is masked off; the rest is the increment.
                    let increment =
                        u32::from_be_bytes([payload[0] & 0x7f, payload[1], payload[2], payload[3]]);
                    let mut observed = origin_observed.lock().unwrap();
                    observed.connection_window_raise =
                        Some(observed.connection_window_raise.unwrap_or(0) + increment);
                    continue;
                }
                FRAME_PING if flags & FLAG_ACK == 0 => raw_frame(FRAME_PING, FLAG_ACK, 0, &payload),
                FRAME_HEADERS => {
                    handshake_done = true;
                    let mut frames = raw_frame(
                        FRAME_HEADERS,
                        FLAG_END_HEADERS,
                        stream_id,
                        &[HPACK_STATUS_200],
                    );
                    frames.extend_from_slice(&raw_frame(
                        FRAME_DATA,
                        FLAG_END_STREAM,
                        stream_id,
                        ORIGIN_BODY.as_bytes(),
                    ));
                    frames
                }
                _ => continue,
            };
            if io.write_all(&reply).await.is_err() {
                return;
            }
        }
    });
    (port, observed)
}

/// Drive one exchange through the cache proxy with the given window headers and
/// report what the proxy advertised during the handshake.
async fn observe_handshake_windows(headers: &[(&str, &str)], path: &str) -> HandshakeWindows {
    let (port, observed) = spawn_window_recording_origin().await;
    let mut request = cache_proxy_get(port, path);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let res = request.send().await.unwrap();
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "the probe exchange itself must succeed"
    );
    assert_eq!(res.text().await.unwrap(), ORIGIN_BODY);
    let windows = *observed.lock().unwrap();
    windows
}

/// Both `x-h2-stream-window-size` and `x-h2-connection-window-size` must reach
/// the upstream h2 handshake, and must reach the half of it that belongs to
/// them.
///
/// The un-injected control case is what keeps the other three honest: without
/// it, an assertion that "the stream window is 4096" would still pass if the
/// option were ignored and 4096 happened to be the default.
///
/// The fourth case pins a trap rather than a feature. A connection window at or
/// below [`H2_DEFAULT_CONNECTION_WINDOW`] is a NO-OP, because WINDOW_UPDATE can
/// only raise a window: the proxy simply sends nothing and the connection window
/// stays at the protocol default. Anyone writing "inject a small connection
/// window and assert the flow is throttled" would be writing a test that cannot
/// fail.
#[tokio::test]
async fn h2_peer_window_options_reach_the_upstream_handshake() {
    init_without_mock_origin();

    let default_raise = PINGORA_DEFAULT_H2_WINDOW - H2_DEFAULT_CONNECTION_WINDOW;

    let control = observe_handshake_windows(&[], &unique_path("windows-control")).await;
    assert_eq!(
        control,
        HandshakeWindows {
            stream_window: Some(PINGORA_DEFAULT_H2_WINDOW),
            connection_window_raise: Some(default_raise),
        },
        "with neither option set, both windows must be pingora's H2_WINDOW_SIZE"
    );

    let stream_injected = observe_handshake_windows(
        &[("x-h2-stream-window-size", "4096")],
        &unique_path("windows-stream"),
    )
    .await;
    assert_eq!(
        stream_injected,
        HandshakeWindows {
            stream_window: Some(4096),
            connection_window_raise: Some(default_raise),
        },
        "the stream option must move SETTINGS_INITIAL_WINDOW_SIZE and nothing else"
    );

    let connection_injected = observe_handshake_windows(
        &[("x-h2-connection-window-size", "1048576")],
        &unique_path("windows-connection"),
    )
    .await;
    assert_eq!(
        connection_injected,
        HandshakeWindows {
            stream_window: Some(PINGORA_DEFAULT_H2_WINDOW),
            connection_window_raise: Some(1048576 - H2_DEFAULT_CONNECTION_WINDOW),
        },
        "the connection option must move the stream-0 WINDOW_UPDATE and nothing else"
    );

    let below_default = observe_handshake_windows(
        &[("x-h2-connection-window-size", "4096")],
        &unique_path("windows-connection-below-default"),
    )
    .await;
    assert_eq!(
        below_default,
        HandshakeWindows {
            stream_window: Some(PINGORA_DEFAULT_H2_WINDOW),
            connection_window_raise: None,
        },
        "a connection window at or below 65535 must be a no-op, not a narrowing"
    );
}
