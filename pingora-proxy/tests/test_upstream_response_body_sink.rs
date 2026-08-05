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

//! Self-contained integration tests for delivering response-body chunks
//! emitted through `ResponseBodySink` (see `pingora-proxy/src/response_body_sink.rs`
//! and the `upstream_filter` plumbing in `pingora-proxy/src/lib.rs`).
//!
//! Deliberately does NOT use `tests/utils`: that harness needs a local
//! openresty mock origin (see `tests/test_request_body_seam.rs` for the same
//! rationale, and `tests/seam/harness.rs` for the pattern this file follows).
//! The origin here is a minimal scripted TCP listener that always answers
//! `hello world`, so these tests run without any external process and are not
//! affected by whether openresty happens to be installed.

use async_trait::async_trait;
use bytes::Bytes;
use once_cell::sync::Lazy;
use pingora_cache::{CacheKey, CacheMeta, MemCache, RespCacheable};
use pingora_core::server::Server;
use pingora_core::services::ServiceWithDependents;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, ResponseBodySink, Session, RESPONSE_BODY_EMIT_BUDGET};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ORIGIN_BODY: &[u8] = b"hello world";

/// What every client of a `terminate`-exercising path must see if (and only
/// if) the pump honors `ResponseBodySink::terminate` -- the first of the two
/// chunks `serve_two_delayed_chunks` sends.
const TERMINATE_FIRST_CHUNK: &[u8] = b"hel[cut]";

/// The second chunk `serve_two_delayed_chunks` sends, after a delay long
/// enough to land in a separate upstream read from the first. A pump that
/// keeps consuming upstream (and keeps calling the response-body filter)
/// after `terminate()` fires would let this leak into the client-visible
/// body; a pump that correctly stops reading upstream the moment terminate
/// fires never even receives it. This is what makes
/// `terminate_after_commit_ends_the_body_cleanly` discriminating: an earlier
/// version of this test used an origin body no longer than the terminated
/// prefix, so it passed even with the pump's terminate handling deleted
/// entirely (the test's own filter fully replaced the body regardless).
const TERMINATE_LEAKED_CHUNK: &[u8] = b"LEAKED_POST_TERMINATE_BYTES";

/// Sent by the two terminate-exercising tests to opt into
/// `serve_two_delayed_chunks` instead of the fixed single-write body every
/// other test in this file uses.
const DELAY_SECOND_CHUNK_HEADER: (&str, &str) = ("x-delay-second-chunk", "1");

/// Sent by `terminate_mid_batch_drops_only_the_leaked_tasks` to opt into
/// `serve_same_batch_chunks`.
const SAME_BATCH_HEADER: (&str, &str) = ("x-same-batch-chunks", "1");

/// What `terminate_mid_batch_drops_only_the_leaked_tasks`'s client must see:
/// the first chunk `serve_same_batch_chunks` sends, on which the filter
/// calls `sink.terminate()`.
const SAME_BATCH_FIRST_CHUNK: &[u8] = b"safe0";

/// The three chunks `serve_same_batch_chunks` sends immediately after
/// `SAME_BATCH_FIRST_CHUNK`, with no delay at all, so pingora's upstream
/// reader can parse and enqueue all four as one burst and the pump's own
/// greedy `now_or_never()` drain (in the `task = rx.recv()` arm) has a
/// realistic chance to pull all four into the *same* `tasks` batch before
/// the filter ever runs on any of them -- unlike `TERMINATE_LEAKED_CHUNK`,
/// which is deliberately delayed to land in a separate batch instead. These
/// three must never reach the client: they are upstream data pulled into the
/// same batch as, but filtered *after*, the task that called `terminate()`.
const SAME_BATCH_LEAK_CHUNKS: [&[u8]; 3] = [b"LEAK1", b"LEAK2", b"LEAK3"];

/// Sent by `terminate_flushes_a_content_length_framed_body` to opt into
/// `serve_cl_framed_delayed`.
const CL_FRAMED_HEADER: (&str, &str) = ("x-cl-framed-delay", "1");

/// What `terminate_flushes_a_content_length_framed_body`'s client must see:
/// the first write `serve_cl_framed_delayed` sends, on which the filter
/// calls `sink.terminate()`.
const CL_FRAMED_FIRST_WRITE: &[u8] = b"cl-safe";

/// The second write `serve_cl_framed_delayed` sends, after a delay -- must
/// never reach the client, same rationale as `TERMINATE_LEAKED_CHUNK`.
const CL_FRAMED_LEAKED_WRITE: &[u8] = b"-cl-leak";

/// A scripted origin that always answers the same fixed body, regardless of
/// the request path: the path alone drives which sink behavior the proxy's
/// filter exercises, so the origin does not need to know about it -- with
/// deliberate exceptions, gated on request headers rather than the path
/// (which by the time it reaches the origin has already been normalized to
/// `/` by `upstream_request_filter`): see `DELAY_SECOND_CHUNK_HEADER`,
/// `SAME_BATCH_HEADER`, and `CL_FRAMED_HEADER`.
async fn serve_origin_connection(mut stream: TcpStream) {
    let mut pending: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        // Read until the end of the request headers. These are simple GETs
        // with no body, so nothing past the header block needs consuming.
        let header_end = loop {
            if let Some(at) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
            match stream.read(&mut chunk).await {
                Ok(0) => return, // client closed the connection
                Ok(n) => pending.extend_from_slice(&chunk[..n]),
                Err(_) => return,
            }
        };
        let request_head = pending[..header_end].to_vec();
        pending.drain(..header_end);

        let has_header = |marker: (&str, &str)| {
            let line = format!("{}: {}", marker.0, marker.1);
            request_head
                .windows(line.len())
                .any(|w| w == line.as_bytes())
        };

        let result = if has_header(DELAY_SECOND_CHUNK_HEADER) {
            serve_two_delayed_chunks(&mut stream).await
        } else if has_header(SAME_BATCH_HEADER) {
            serve_same_batch_chunks(&mut stream).await
        } else if has_header(CL_FRAMED_HEADER) {
            serve_cl_framed_delayed(&mut stream).await
        } else {
            serve_fixed_body(&mut stream).await
        };
        if result.is_err() {
            return;
        }
        // Loop back for the next request on this keep-alive connection.
    }
}

async fn serve_fixed_body(stream: &mut TcpStream) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        ORIGIN_BODY.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(ORIGIN_BODY).await
}

/// Answers with `TERMINATE_FIRST_CHUNK`, then -- after a delay long enough to
/// force a separate upstream read -- `TERMINATE_LEAKED_CHUNK`, both
/// chunk-transfer-encoded so a client that keeps reading can observe the
/// second one. See `TERMINATE_LEAKED_CHUNK`'s doc comment for why the delay
/// matters.
async fn serve_two_delayed_chunks(stream: &mut TcpStream) -> std::io::Result<()> {
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
        )
        .await?;
    write_chunk(stream, TERMINATE_FIRST_CHUNK).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    write_chunk(stream, TERMINATE_LEAKED_CHUNK).await?;
    stream.write_all(b"0\r\n\r\n").await
}

/// Answers with `SAME_BATCH_FIRST_CHUNK` followed immediately (no delay
/// anywhere in this function) by each of `SAME_BATCH_LEAK_CHUNKS`, all
/// chunk-transfer-encoded. See `SAME_BATCH_LEAK_CHUNKS`'s doc comment for why
/// the absence of a delay matters here, unlike `serve_two_delayed_chunks`.
async fn serve_same_batch_chunks(stream: &mut TcpStream) -> std::io::Result<()> {
    // Built into one buffer and sent with a single `write_all` call
    // (unlike `write_chunk`'s three separate ones) so the whole response
    // -- headers and all four chunks -- has the best chance of landing in
    // one TCP segment, and so pingora's upstream reader has the best chance
    // of parsing and enqueueing all four `HttpTask`s before yielding back to
    // the executor. See `SAME_BATCH_LEAK_CHUNKS`'s doc comment.
    let mut buf = Vec::new();
    buf.extend_from_slice(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
    );
    encode_chunk(&mut buf, SAME_BATCH_FIRST_CHUNK);
    for leak in SAME_BATCH_LEAK_CHUNKS {
        encode_chunk(&mut buf, leak);
    }
    buf.extend_from_slice(b"0\r\n\r\n");
    stream.write_all(&buf).await
}

fn encode_chunk(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

/// Answers with a `Content-Length`-framed body (not chunked, unlike every
/// other non-default origin behavior in this file): declares the combined
/// length of `CL_FRAMED_FIRST_WRITE` and `CL_FRAMED_LEAKED_WRITE` up front
/// (this is the origin's own, upstream-facing framing -- valid and
/// unrelated to what the proxy ends up sending downstream), writes
/// `CL_FRAMED_FIRST_WRITE`, then -- after a delay long enough to force a
/// separate upstream read, so the task that calls `sink.terminate()` has a
/// real `end_of_stream = false` and the downstream `Content-Length`-framed
/// body is genuinely left unfinished at the point of termination, not
/// closed out by a coincident real end-of-stream -- writes
/// `CL_FRAMED_LEAKED_WRITE`.
async fn serve_cl_framed_delayed(stream: &mut TcpStream) -> std::io::Result<()> {
    let total_len = CL_FRAMED_FIRST_WRITE.len() + CL_FRAMED_LEAKED_WRITE.len();
    let response =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {total_len}\r\nConnection: keep-alive\r\n\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(CL_FRAMED_FIRST_WRITE).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream.write_all(CL_FRAMED_LEAKED_WRITE).await
}

async fn write_chunk(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    stream
        .write_all(format!("{:x}\r\n", data.len()).as_bytes())
        .await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await
}

fn spawn_origin() -> u16 {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap().port()).unwrap();
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                tokio::spawn(serve_origin_connection(stream));
            }
        });
    });
    rx.recv().expect("origin failed to start")
}

/// Reserve a free localhost port by binding it and immediately releasing it.
/// Mirrors `tests/seam/harness.rs::reserve_port`.
fn reserve_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("cannot reserve a local port");
    listener.local_addr().unwrap().port()
}

/// Backs the cache-enabled [`EmitProxy`] instance only; the plain
/// delivery-order and budget-overflow tests never touch it.
static CACHE_BACKEND: Lazy<MemCache> = Lazy::new(MemCache::new);

pub struct EmitProxy {
    origin_port: u16,
    /// When true, every request runs through a real (in-memory) cache: the
    /// first hit admits the response (including whatever the sink emitted),
    /// the second is served back out of it. See
    /// `emitted_chunks_are_byte_identical_from_cache_and_from_the_live_response`.
    cache: bool,
    /// Gates `/cache/terminate_once`: only the first request through this
    /// path terminates mid-body. Every later request must be a genuine,
    /// complete re-fetch, which is the whole point of
    /// `terminate_under_cache_streaming_readback_fails_closed` -- a second
    /// request that also terminated would prove nothing about cache
    /// admission.
    terminate_once_fired: AtomicBool,
}

/// Per-request decision, made once in `upstream_peer` -- the earliest hook
/// that runs on every request -- and read back by
/// `upstream_response_body_filter`. Deciding it in `upstream_peer` rather
/// than, say, inline in the body filter based on the path is not load-bearing
/// today (nothing else in this fixture also needs the decision), but it is
/// the one hook guaranteed to run before the live body filter regardless of
/// how a cache backend orders its admission/readback passes relative to each
/// other -- see the git history of this file for a version that decided it
/// in `response_filter` instead, and broke when a streaming-partial-write
/// cache backend ran that hook after the body filter had already read
/// `ctx.will_terminate`.
#[derive(Default)]
pub struct EmitCtx {
    will_terminate: bool,
}

#[async_trait]
impl ProxyHttp for EmitProxy {
    type CTX = EmitCtx;
    fn new_ctx(&self) -> Self::CTX {
        EmitCtx::default()
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        if self.cache {
            session
                .cache
                .enable(&*CACHE_BACKEND, None, None, None, None);
        }
        Ok(())
    }

    fn cache_key_callback(&self, session: &Session, _ctx: &mut Self::CTX) -> Result<CacheKey> {
        // Not production ready (ignores Vary, scheme, ...) -- fine for a test
        // fixture with one path per case. See `ProxyHttp::cache_key_callback`
        // for what a real implementation needs to consider.
        Ok(CacheKey::new(
            String::new(),
            session.req_header().uri.path().to_string(),
            String::new(),
        ))
    }

    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        // Unconditionally cacheable for long enough that the test's second
        // request is provably a hit, not a lucky re-fetch.
        Ok(RespCacheable::Cacheable(CacheMeta::new(
            SystemTime::now() + Duration::from_secs(60),
            SystemTime::now(),
            0,
            0,
            resp.clone(),
        )))
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Exposes which cache phase served this response, so the test can
        // prove its second request was a genuine cache hit rather than
        // asserting equal bytes that might both have come from a live fetch.
        if session.cache.enabled() {
            let status = match session.cache.phase() {
                pingora_cache::CachePhase::Hit => "hit",
                pingora_cache::CachePhase::Miss => "miss",
                _ => "other",
            };
            upstream_response.insert_header("x-cache-status", status)?;
        }
        if session.req_header().uri.path() == "/terminate_content_length" {
            // The origin's own `Content-Length` (declared to
            // `serve_cl_framed_delayed`'s upstream-facing framing, covering
            // both the safe and the leaked write) must not leak downstream
            // unchanged: the whole point of this path is to exercise
            // `Content-Length` framing on the *downstream* side of a
            // response-body terminate, so the declared length here has to
            // match what the filter actually intends to deliver.
            upstream_response
                .insert_header("content-length", CL_FRAMED_FIRST_WRITE.len().to_string())?;
        }
        Ok(())
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // The single decision point for this request -- see `EmitCtx`'s doc
        // comment for why it cannot be made in either downstream-facing
        // filter instead.
        match session.req_header().uri.path() {
            "/terminate_midstream" | "/terminate_same_batch" | "/terminate_content_length" => {
                ctx.will_terminate = true
            }
            "/cache/terminate_once" => {
                ctx.will_terminate = !self.terminate_once_fired.swap(true, Ordering::SeqCst);
            }
            _ => {}
        }
        Ok(Box::new(HttpPeer::new(
            format!("127.0.0.1:{}", self.origin_port),
            false,
            "".to_string(),
        )))
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        // The scripted origin answers the same fixed body no matter the path;
        // normalizing the upstream-side URI keeps its request parsing trivial
        // while the downstream-visible path (still readable via
        // `session.req_header()`) is what selects the filter behavior below.
        upstream_request.set_uri(http::Uri::from_static("/"));
        Ok(())
    }

    async fn upstream_response_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        sink: &mut ResponseBodySink,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        match session.req_header().uri.path() {
            "/split_body" => {
                // Split the single upstream chunk in two: keep only its first
                // byte in `body`, emit the remainder through the sink. The
                // client must still see the bytes concatenated in the same
                // order, proving 1-in-N-out delivery.
                if let Some(data) = body.as_mut() {
                    if data.len() > 1 {
                        let rest = data.split_off(1);
                        sink.push(rest)?;
                    }
                }
            }
            "/emit_overflow" => {
                // Push a chunk larger than the batch budget: `push` must
                // reject it, and that rejection must surface as a failed
                // request, never as a silently truncated body.
                sink.push(Bytes::from(vec![0u8; RESPONSE_BODY_EMIT_BUDGET + 1]))?;
            }
            _ => {}
        }

        // Deliberately no body rewriting and no compensation on a later call
        // once already terminated: the pump itself -- not this application
        // code -- must be what stops `TERMINATE_LEAKED_CHUNK` from ever
        // reaching the client. Suppressing it here (e.g. `*body = None` once
        // `sink.is_terminated()`) would mask a pump regression instead of a
        // test catching it.
        if ctx.will_terminate && !sink.is_terminated() {
            sink.terminate();
        }
        Ok(None)
    }
}

struct Harness {
    proxy_port: u16,
    cache_proxy_port: u16,
}

impl Harness {
    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.proxy_port)
    }

    /// The cache-enabled `EmitProxy` instance: same filter behavior, but
    /// `session.cache` is turned on, so a request here actually exercises
    /// `cache_task_and_emitted_chunks`/`drain_emitted_chunks` together.
    fn cache_base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.cache_proxy_port)
    }
}

fn start_harness() -> Harness {
    let origin_port = spawn_origin();
    let proxy_port = reserve_port();
    let cache_proxy_port = reserve_port();
    let proxy_addr = format!("127.0.0.1:{proxy_port}");
    let cache_proxy_addr = format!("127.0.0.1:{cache_proxy_port}");
    let listen_addr = proxy_addr.clone();
    let cache_listen_addr = cache_proxy_addr.clone();

    thread::spawn(move || {
        let mut server = Server::new(None).unwrap();
        server.bootstrap();

        let mut proxy_service = pingora_proxy::http_proxy_service(
            &server.configuration,
            EmitProxy {
                origin_port,
                cache: false,
                terminate_once_fired: AtomicBool::new(false),
            },
        );
        proxy_service.add_tcp(&listen_addr);

        let mut cache_proxy_service = pingora_proxy::http_proxy_service(
            &server.configuration,
            EmitProxy {
                origin_port,
                cache: true,
                terminate_once_fired: AtomicBool::new(false),
            },
        );
        cache_proxy_service.add_tcp(&cache_listen_addr);

        let services: Vec<Box<dyn ServiceWithDependents>> =
            vec![Box::new(proxy_service), Box::new(cache_proxy_service)];
        server.add_services(services);
        server.run_forever();
    });

    // Poll for readiness instead of sleeping, matching `tests/seam/harness.rs`.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    for addr in [&proxy_addr, &cache_proxy_addr] {
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the emit-sink test proxy never started listening on {addr}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    Harness {
        proxy_port,
        cache_proxy_port,
    }
}

static HARNESS: Lazy<Harness> = Lazy::new(start_harness);

fn init() -> &'static Harness {
    Lazy::force(&HARNESS)
}

#[tokio::test]
async fn emitted_chunks_reach_the_client_in_order() {
    let harness = init();
    let res = reqwest::get(format!("{}/split_body", harness.base_url()))
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body = res.text().await.unwrap();
    assert_eq!(body, "hello world");
}

/// What the client actually received for `/emit_overflow`, collapsed to the
/// one shape the test cares about: `Some(body)` only for a clean 200 whose
/// body was read to completion, `None` for every other outcome (a
/// non-200 status, a connection-level error, or a body read that itself
/// failed midway).
async fn completed_200_body(url: &str) -> Option<String> {
    let res = reqwest::get(url).await.ok()?;
    if res.status() != reqwest::StatusCode::OK {
        return None;
    }
    res.text().await.ok()
}

#[tokio::test]
async fn exceeding_the_emit_budget_fails_the_response() {
    let harness = init();
    // Single, unconditional, positive assertion -- no arm of `completed_200_body`
    // can make this pass by doing nothing: pushing past the budget must
    // surface as a failed request (a non-200, a dropped connection, or an
    // incomplete body read), never as a client-visible clean 200 that
    // silently reproduces the pristine 11-byte origin body.
    let observed = completed_200_body(&format!("{}/emit_overflow", harness.base_url())).await;
    assert_ne!(
        observed,
        Some("hello world".to_string()),
        "the client must never receive a complete, pristine body after the emit budget was exceeded"
    );
}

#[tokio::test]
async fn emitted_chunks_are_byte_identical_from_cache_and_from_the_live_response() {
    let harness = init();
    let url = format!("{}/split_body", harness.cache_base_url());

    // First request: a live fetch that both delivers the split chunks to the
    // client AND admits them to cache (`cache_task_and_emitted_chunks`).
    // Pre-fix (Critical 1) this call panics the connection task the moment
    // caching reaches the second (already end-of-stream-tagged) chunk, since
    // the original task was already cached with `end_stream = true` and the
    // cache's `MissHandler` was already taken by `finish_miss_handler`.
    let first = reqwest::get(&url).await.unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    assert_eq!(
        first.headers()["x-cache-status"],
        "miss",
        "the first request must be a genuine cache miss (live fetch + admission)"
    );
    let first_body = first.text().await.unwrap();
    assert_eq!(first_body, "hello world");

    // Second request: must be served entirely out of the cache
    // (`drain_emitted_chunks` never runs -- `from_cache = true` takes the
    // `out_tasks.push(task)` branch exclusively). Proven a real hit via
    // `x-cache-status`, not merely inferred from matching bytes: a second
    // live fetch would produce the same bytes too, and would not tell
    // Critical 1/2 apart from a working implementation.
    let second = reqwest::get(&url).await.unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    assert_eq!(
        second.headers()["x-cache-status"],
        "hit",
        "the second request must be served from cache, not fetched live again"
    );
    let second_body = second.text().await.unwrap();

    // The whole point: what the cache stored is byte-for-byte what the
    // client received on the live request that admitted it.
    assert_eq!(
        second_body, first_body,
        "the cache hit must reproduce exactly what the live response delivered"
    );
    assert_eq!(second_body, "hello world");
}

/// `completed_200_body`, generalized to take a prebuilt request (needed for
/// the `DELAY_SECOND_CHUNK_HEADER` these tests send): `Some(body)` only for
/// a clean 200 read to completion, `None` for anything else (a non-200
/// status, a connection-level error, or a body read that itself failed
/// midway).
async fn completed_200_body_for(builder: reqwest::RequestBuilder) -> Option<String> {
    let res = builder.send().await.ok()?;
    if res.status() != reqwest::StatusCode::OK {
        return None;
    }
    res.text().await.ok()
}

// Terminate on an already-committed response: headers sent and one body
// chunk delivered, then the filter ends it -- while the origin still has a
// second chunk (`TERMINATE_LEAKED_CHUNK`) queued up behind a delay. The
// client must see a well-formed, complete-looking response containing
// exactly the bytes delivered before the terminate -- not a connection
// error, not a status change, and critically not the leaked second chunk:
// only a pump that actually stops reading upstream once `sink.terminate()`
// fires can avoid it. (An earlier version of this test used an origin body
// no longer than the terminated prefix; it passed even with the pump's
// entire terminate-handling block deleted, because the filter itself fully
// replaced the body regardless of what the pump did afterward.)
#[tokio::test]
async fn terminate_after_commit_ends_the_body_cleanly() {
    let harness = init();
    let client = reqwest::Client::new();
    let res = client
        .get(format!("{}/terminate_midstream", harness.base_url()))
        .header(DELAY_SECOND_CHUNK_HEADER.0, DELAY_SECOND_CHUNK_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        reqwest::StatusCode::OK,
        "status cannot change after commit"
    );
    let body = res.text().await.unwrap();
    assert_eq!(
        body, "hel[cut]",
        "the client must see exactly the bytes delivered before terminate -- a leaked \
         LEAKED_POST_TERMINATE_BYTES here would mean the pump kept reading upstream (and kept \
         calling the response-body filter) after terminate() fired"
    );
}

// `ResponseBodySink::terminate` while a cache streaming readback is (or
// would be) in progress is unsupported and must fail closed rather than risk
// either defect it can produce (see the task's design notes): the client
// must never see a clean success, and the failed attempt must not leave a
// servable entry in the cache for the next request to hit.
//
// `sink.terminate()` fires on `TERMINATE_FIRST_CHUNK`, which is genuinely
// not the final chunk here (the origin still has `TERMINATE_LEAKED_CHUNK`
// queued, unlike `terminate_after_commit_ends_the_body_cleanly`'s
// non-cached counterpart): this keeps the admission this request creates
// from ever being marked complete for caching purposes either (see
// `migrate_end_of_stream` in `proxy_cache.rs`, unchanged by this task -- an
// end-of-stream flag only ever migrates onto a chunk when the leading task's
// own flag was already true), so the second request below is a clean,
// unambiguous miss. See the task report for the other boundary case this
// deliberately avoids: a filter that terminates exactly on a chunk that
// *is* the real final one lets that admission commit normally, which is a
// separate, accepted case, not exercised here.
#[tokio::test]
async fn terminate_under_cache_streaming_readback_fails_closed() {
    let harness = init();
    let client = reqwest::Client::new();
    let url = format!("{}/cache/terminate_once", harness.cache_base_url());

    let first_request = client
        .get(&url)
        .header(DELAY_SECOND_CHUNK_HEADER.0, DELAY_SECOND_CHUNK_HEADER.1);
    let observed = completed_200_body_for(first_request).await;
    // A single, unconditional, positive assertion -- matching the discipline
    // `exceeding_the_emit_budget_fails_the_response` above argues for. Two
    // `assert_ne!`s only rule out the two specific leaked bodies this task
    // happened to trip over; a clean 200 carrying any THIRD body would sail
    // right through both and still be wrong, because the design says this
    // combination fails closed, full stop -- not "fails closed, unless the
    // body looks different from these two examples".
    assert!(
        observed.is_none(),
        "a terminate during a streaming cache readback must fail closed -- the first request \
         must never complete as a clean 200 body at all (got {observed:?}, which would include \
         both the pre-fix truncated \"hel[cut]\" and the fully leaked \
         \"hel[cut]LEAKED_POST_TERMINATE_BYTES\")"
    );

    // `terminate_once_fired` has already flipped, so this second request
    // takes the ordinary, non-terminating path: a genuine live fetch that
    // must see the real, complete origin response and a genuine cache miss,
    // proving the failed first attempt left no servable entry behind.
    let second = client
        .get(&url)
        .header(DELAY_SECOND_CHUNK_HEADER.0, DELAY_SECOND_CHUNK_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    assert_eq!(
        second.headers()["x-cache-status"],
        "miss",
        "the failed first attempt must not have left a servable cache entry"
    );
    let second_body = second.text().await.unwrap();
    assert_eq!(second_body, "hel[cut]LEAKED_POST_TERMINATE_BYTES");
}

// The non-cached analog of the Critical `terminate_under_cache_streaming_readback_fails_closed`
// exercises, but for the pump's *own* per-batch task loop rather than the
// cache readback: `serve_same_batch_chunks` sends `SAME_BATCH_FIRST_CHUNK`
// immediately followed by `SAME_BATCH_LEAK_CHUNKS`, all without any delay,
// so pingora's upstream reader can enqueue all four before the pump's greedy
// `now_or_never()` drain (in the `task = rx.recv()` arm) ever runs -- landing
// all four in one `tasks` batch. The filter terminates on the first one; the
// other three are upstream data that arrived (or was pulled) *after* that
// decision and must never reach the client, even though they are in the same
// batch as -- and would, pre-fix, be written downstream in the same
// `write_response_tasks` call as -- the task that terminated.
//
// This is deliberately the mirror image of
// `terminate_after_commit_ends_the_body_cleanly`: that test forces the
// leaked chunk into a *separate* batch (via a real delay) and would not have
// caught this defect, because the fix for it (returning `Terminate` right
// after the batch write) never got a chance to matter -- the leaked chunk
// was already sitting in `filtered_tasks`, written in the very call the fix
// reacts to, before the fix's own check ever runs.
//
// Whether tasks 2..N actually land in the same batch as the terminating
// task is not something this black-box test can force or directly observe:
// it comes down to a race between the origin's single `write_all` and the
// pump's own `now_or_never()` drain (see `serve_same_batch_chunks`'s doc
// comment), decided by OS/scheduler timing this test does not control. It
// was measured 35/35 on the development machine across two separate
// verification runs (see the round-2 fix report), but that is evidence for
// one machine's scheduler, not a portability guarantee -- and critically,
// this test *cannot fail* on a run where the race goes the other way: it
// would just silently duplicate `terminate_after_commit_ends_the_body_cleanly`
// while reporting green, covering nothing extra. There is no way to observe
// the batch composition from outside the pump without either a source
// change (out of scope for this fix) or a flaky timing-based assertion on
// wall-clock duration (worse than what this does instead): repeat the
// request enough times that, even on a scheduler far less favorable than
// the development machine's, at least one iteration is overwhelmingly
// likely to land multiple tasks in one batch -- and since every iteration
// runs the identical assertion, any single one of them catching a
// regression fails the whole test.
#[tokio::test]
async fn terminate_mid_batch_drops_only_the_leaked_tasks() {
    let harness = init();
    let client = reqwest::Client::new();
    for attempt in 0..20 {
        let res = client
            .get(format!("{}/terminate_same_batch", harness.base_url()))
            .header(SAME_BATCH_HEADER.0, SAME_BATCH_HEADER.1)
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            reqwest::StatusCode::OK,
            "status cannot change after commit (attempt {attempt})"
        );
        let body = res.text().await.unwrap();
        assert_eq!(
            body, "safe0",
            "the client must see only the task that called terminate() -- any of \
             LEAK1/LEAK2/LEAK3 here means later tasks in the same drained batch still reached \
             the client (attempt {attempt})"
        );
    }
}

// Exercises `terminate()` under `Content-Length` framing end to end: every
// other terminate-exercising origin in this file uses chunked encoding
// instead (see `TERMINATE_LEAKED_CHUNK`'s doc comment for why), so none of
// them touch the CL-framed body path `finish_terminated_response` exists for
// (see its doc comment in `proxy_common.rs`). `serve_cl_framed_delayed`
// declares a real `Content-Length` covering two delayed writes,
// `sink.terminate()` fires on the first (a genuine `end_of_stream = false`,
// not a coincident real end), and the downstream `Content-Length` header is
// set to match only that first write. The request is bounded by a timeout
// so a regression here fails the suite instead of hanging it.
//
// What this test does NOT prove: that `finish_terminated_response` itself is
// load-bearing for this specific fixture. Verified directly: stubbing
// `finish_terminated_response` to a no-op still passes this test, in the
// same ~0.03s, no timeout involved. Root cause:
// `BodyWriter::do_write_body` (`pingora-core/src/protocols/http/v1/body.rs:996-1010`)
// already flushes the stream itself whenever a write brings
// `written >= total` (the declared `Content-Length`) -- independent of the
// task's `end_of_stream` flag and independent of `finish_terminated_response`.
// Because a terminate response has to declare a downstream `Content-Length`
// that exactly matches what actually gets delivered (the only way the
// client sees a clean, non-error response under CL framing at all -- a
// mismatched, larger declared length was tried too, and produces the
// identical fast `IncompleteBody` client error whether
// `finish_terminated_response` runs or not, so it does not discriminate
// either), `written == total` becomes true on exactly the write that also
// calls `terminate()`, and `do_write_body`'s own auto-flush always gets
// there first in every scenario this fixture can construct. This is still
// real, non-trivial coverage of the CL-framed terminate combination as a
// whole; it is not proof that any single function within it is necessary.
#[tokio::test]
async fn terminate_flushes_a_content_length_framed_body() {
    let harness = init();
    let client = reqwest::Client::new();
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        client
            .get(format!("{}/terminate_content_length", harness.base_url()))
            .header(CL_FRAMED_HEADER.0, CL_FRAMED_HEADER.1)
            .send(),
    )
    .await
    .expect(
        "request must complete within the timeout -- this bounds the CL-framed terminate path \
         so a regression fails the suite instead of hanging it, but per the doc comment above a \
         hang here would not by itself implicate finish_terminated_response specifically",
    )
    .unwrap();
    assert_eq!(
        res.status(),
        reqwest::StatusCode::OK,
        "status cannot change after commit"
    );
    let body = tokio::time::timeout(Duration::from_secs(5), res.text())
        .await
        .expect("body read must not hang")
        .unwrap();
    assert_eq!(
        body, "cl-safe",
        "the client must see exactly the pre-terminate bytes under Content-Length framing, and \
         no more"
    );
}
