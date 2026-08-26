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
use futures::Stream as FuturesStream;
use http::HeaderMap;
use once_cell::sync::Lazy;
use pingora_cache::{
    storage::{HitHandler, MissHandler, PurgeType, Storage},
    trace::{Span, SpanHandle},
    CacheKey, CacheMeta, MemCache, PurgeOutcome, PurgeTarget, RespCacheable,
};
use pingora_core::server::Server;
use pingora_core::services::ServiceWithDependents;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::{
    connectors::http::custom::{Connection as CustomConnection, Connector as CustomConnector},
    protocols::{
        http::custom::{client::Session as CustomSession, BodyWrite, CustomMessageWrite},
        l4::socket::SocketAddr,
        tls::{CustomALPN, ALPN},
        Digest, Stream, UniqueIDType,
    },
    server::ShutdownWatch,
    upstreams::peer::Peer,
};
use pingora_error::Result;
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{
    ProcessCustomSession, ProxyHttp, ProxyServiceBuilder, RangeType, ResponseBodySink, Session,
    RESPONSE_BODY_EMIT_BUDGET,
};
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
const SUPPRESSION_BATCH_HEADER: (&str, &str) = ("x-suppression-batch-chunks", "1");

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

const BODYLESS_ORIGIN_HEADER: (&str, &str) = ("x-bodyless-origin", "1");
const BODYLESS_OK_ORIGIN_HEADER: (&str, &str) = ("x-bodyless-ok-origin", "1");
const HTTP10_BODYLESS_OK_ORIGIN_HEADER: (&str, &str) = ("x-http10-bodyless-ok-origin", "1");
const BODYLESS_NO_CONTENT_HEADER: (&str, &str) = ("x-bodyless-no-content", "1");
const BODYLESS_NOT_MODIFIED_HEADER: (&str, &str) = ("x-bodyless-not-modified", "1");
const MANY_CHUNK_HEADER: (&str, &str) = ("x-many-chunks", "1");
const CUSTOM_POST_TERMINAL_FAILURE_HEADER: (&str, &str) = ("x-custom-post-terminal-failure", "1");
const SUPPRESS_DOWNSTREAM_BODY_HEADER: (&str, &str) = ("x-suppress-downstream-body", "1");
const RESPONSE_FILTER_CALLS_HEADER: &str = "x-test-response-filter-calls";
const SUPPRESSED_BODY_EXTRA: &[u8] = b"+";
const SUPPRESSION_ORIGIN_BODY_SIZE: usize = 256 * 1024;
/// Body chunks the scripted custom origin streams before its trailers.
const CUSTOM_TRAILERED_CHUNKS: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
const CUSTOM_TRAILERED_PATH: &str = "/custom_trailered";
/// Selects the trailered script in the custom session. A request HEADER, not
/// the path: the upstream request URI is rewritten to "/" before it reaches
/// `write_request_header`.
const CUSTOM_TRAILERED_HEADER: (&str, &str) = ("x-custom-trailered", "1");
const CUSTOM_EMPTY_UPGRADE_PATH: &str = "/bodyless_empty_upgrade";
const CUSTOM_EMPTY_UPGRADE_HEADER: (&str, &str) = ("x-custom-empty-upgrade", "1");
/// Selects the custom session that reports a completed Upgrade as a single
/// `Header(101, true)`: a final 101 whose connection already reached clean EOF,
/// with no `UpgradedBody` task behind it.
const CUSTOM_TERMINAL_UPGRADE_PATH: &str = "/bodyless_terminal_upgrade";
const CUSTOM_TERMINAL_UPGRADE_HEADER: (&str, &str) = ("x-custom-terminal-upgrade", "1");
/// Same scripted `Header(101, true)` session, but the downstream request is a
/// plain GET: the 101 is naked and must not upgrade the downstream response.
const CUSTOM_NAKED_TERMINAL_UPGRADE_PATH: &str = "/bodyless_naked_terminal_upgrade";
/// Same scripted session again, with `response_filter` rewriting the 101 to a
/// non-upgrade status before it reaches the downstream writer.
const CUSTOM_REWRITTEN_TERMINAL_UPGRADE_PATH: &str = "/bodyless_rewritten_terminal_upgrade";

const BODYLESS_CURRENT: &[u8] = b"generated";
const BODYLESS_EXTRA: &[u8] = b"-extra";

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

        let result = if has_header(HTTP10_BODYLESS_OK_ORIGIN_HEADER) {
            serve_http10_bodyless_ok(&mut stream).await
        } else if has_header(BODYLESS_OK_ORIGIN_HEADER) {
            serve_bodyless(&mut stream, 200, "OK").await
        } else if has_header(BODYLESS_NO_CONTENT_HEADER) {
            serve_bodyless(&mut stream, 204, "No Content").await
        } else if has_header(BODYLESS_NOT_MODIFIED_HEADER) {
            serve_bodyless(&mut stream, 304, "Not Modified").await
        } else if has_header(BODYLESS_ORIGIN_HEADER) {
            serve_bodyless(&mut stream, 503, "Service Unavailable").await
        } else if has_header(DELAY_SECOND_CHUNK_HEADER) {
            serve_two_delayed_chunks(&mut stream).await
        } else if has_header(SAME_BATCH_HEADER) {
            serve_same_batch_chunks(&mut stream).await
        } else if has_header(SUPPRESSION_BATCH_HEADER) {
            serve_suppression_batch_chunks(&mut stream).await
        } else if has_header(CL_FRAMED_HEADER) {
            serve_cl_framed_delayed(&mut stream).await
        } else if has_header(MANY_CHUNK_HEADER) {
            serve_many_chunks(&mut stream).await
        } else {
            serve_fixed_body(&mut stream).await
        };
        if result.is_err() {
            return;
        }
        // Loop back for the next request on this keep-alive connection.
    }
}

async fn serve_bodyless(stream: &mut TcpStream, status: u16, reason: &str) -> std::io::Result<()> {
    stream
        .write_all(
            format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nETag: \"terminal-v1\"\r\nConnection: keep-alive\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
}

async fn serve_http10_bodyless_ok(stream: &mut TcpStream) -> std::io::Result<()> {
    stream
        .write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
        .await
}

async fn serve_fixed_body(stream: &mut TcpStream) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        ORIGIN_BODY.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(ORIGIN_BODY).await
}

async fn serve_many_chunks(stream: &mut TcpStream) -> std::io::Result<()> {
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
        )
        .await?;
    for _ in 0..24 {
        write_chunk(stream, b"x").await?;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    stream.write_all(b"0\r\n\r\n").await
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

async fn serve_suppression_batch_chunks(stream: &mut TcpStream) -> std::io::Result<()> {
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let body = vec![b'x'; SUPPRESSION_ORIGIN_BODY_SIZE];
    write_chunk(stream, &body).await?;
    // The first large chunk is parsed into multiple body tasks in one pump
    // batch. This delay guarantees that the second chunk reaches a later
    // batch, after `ResponseBodySink::reset_batch()` has run again.
    tokio::time::sleep(Duration::from_millis(350)).await;
    write_chunk(stream, &body).await?;
    stream.write_all(b"0\r\n\r\n").await
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
static NON_STREAMING_CACHE_BACKEND: Lazy<NonStreamingMemCache> =
    Lazy::new(NonStreamingMemCache::new);
static SUPPRESSED_CACHE_EXPECTED_BODY: Lazy<Mutex<Vec<u8>>> = Lazy::new(|| Mutex::new(Vec::new()));
static SUPPRESSION_SHARED_BATCH_SEEN: AtomicBool = AtomicBool::new(false);
static SUPPRESSION_BATCH_STARTS: AtomicUsize = AtomicUsize::new(0);
static TERMINAL_HOOK_RAN_AFTER_HEADER_FILTER_ERROR: AtomicBool = AtomicBool::new(false);
static FAILED_CUSTOM_SESSION_RELEASED: AtomicBool = AtomicBool::new(false);
static PRE_WRITE_CUSTOM_SESSION_RELEASED: AtomicBool = AtomicBool::new(false);
static CUSTOM_POST_TERMINAL_FAILURE_EMITTED: AtomicBool = AtomicBool::new(false);
static TERMINAL_CUSTOM_RANGE_FILTER_CALLS: AtomicUsize = AtomicUsize::new(0);
static SPOOF_CUSTOM_RANGE_FILTER_CALLS: AtomicUsize = AtomicUsize::new(0);
static TERMINAL_CACHED_HEAD_BODY_FILTER_CALLS: AtomicUsize = AtomicUsize::new(0);
static TERMINAL_CACHED_304_BODY_FILTER_CALLS: AtomicUsize = AtomicUsize::new(0);
/// End-of-stream `upstream_response_body_filter` calls seen on each of the
/// scripted `Header(101, true)` paths. Keyed by path because the three tests
/// share one proxy instance and may run concurrently.
static TERMINAL_UPGRADE_EOS_CALLS: Lazy<Mutex<std::collections::HashMap<String, usize>>> =
    Lazy::new(|| Mutex::new(std::collections::HashMap::new()));
/// Set if the pump reads ANY scripted `Header(101, true)` session again after
/// it reported end-of-stream. Nothing may follow that header on the wire, so
/// neither `read_response_body` nor `read_trailers` may be called.
///
/// Deliberately not keyed by path: the scripted session only sees the upstream
/// request, whose URI has already been normalized to `/`. Correct behavior
/// never sets this flag for any of the three terminal-upgrade paths, so every
/// one of them asserts on it and the assertion message stays path-agnostic.
static TERMINAL_UPGRADE_SESSION_READ_AFTER_EOS: AtomicBool = AtomicBool::new(false);

fn assert_no_terminal_upgrade_read_after_eos() {
    assert!(
        !TERMINAL_UPGRADE_SESSION_READ_AFTER_EOS.load(Ordering::SeqCst),
        "the pump read a scripted terminal-upgrade session after it reported \
         end-of-stream (the flag is shared by all three terminal-upgrade tests)"
    );
}

struct NonStreamingMemCache {
    inner: MemCache,
}

impl NonStreamingMemCache {
    fn new() -> Self {
        Self {
            inner: MemCache::new(),
        }
    }
}

#[async_trait]
impl Storage for NonStreamingMemCache {
    async fn lookup(
        &'static self,
        key: &CacheKey,
        trace: &SpanHandle,
    ) -> Result<Option<(CacheMeta, HitHandler)>> {
        self.inner.lookup(key, trace).await
    }

    async fn get_miss_handler(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        trace: &SpanHandle,
    ) -> Result<MissHandler> {
        self.inner.get_miss_handler(key, meta, trace).await
    }

    async fn purge(
        &'static self,
        target: PurgeTarget<'_>,
        purge_type: PurgeType,
        trace: &SpanHandle,
    ) -> Result<PurgeOutcome> {
        self.inner.purge(target, purge_type, trace).await
    }

    async fn update_meta(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        trace: &SpanHandle,
    ) -> Result<bool> {
        self.inner.update_meta(key, meta, trace).await
    }

    /// Deliberately do not delegate to `MemCache`: returning `false` keeps
    /// cache miss streaming disabled, which exercises the downstream-body
    /// suppression path this fixture exists to cover.
    fn support_streaming_partial_write(&self) -> bool {
        false
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static) {
        self
    }
}

pub struct EmitProxy {
    origin_port: u16,
    custom: bool,
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
    /// Response bytes withheld by the `CUSTOM_TRAILERED_PATH` processor.
    withheld_body: Vec<u8>,
    will_terminate: bool,
    response_filter_seen: bool,
    response_cache_filter_seen: bool,
    response_filter_calls: usize,
}

#[derive(Clone, Copy)]
struct HeaderOnlyCustomConnector;

struct HeaderOnlyCustomSession {
    response: ResponseHeader,
    request_header_written: bool,
    fail_after_terminal: bool,
    /// Body chunks still to hand to the pump. Non-empty only for
    /// `CUSTOM_TRAILERED_PATH`, which scripts the custom-pump analogue of an
    /// H2 trailered response: body chunks that never carry end-of-stream,
    /// followed by trailers that do.
    pending_body: std::collections::VecDeque<Bytes>,
    pending_trailers: bool,
    upgraded: bool,
    body_eof_observed: bool,
    /// Scripts `Header(101, true)`: the connector already saw the upgraded
    /// connection reach clean EOF, so the whole response is complete at the
    /// header and the session must never be read again.
    terminal_upgrade: bool,
}

struct NoopBodyWriter;

#[async_trait]
impl BodyWrite for NoopBodyWriter {
    async fn write_all_buf(&mut self, data: &mut Bytes) -> Result<()> {
        data.clear();
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<()> {
        Ok(())
    }

    fn upgrade_body_writer(&mut self) {}
}

impl HeaderOnlyCustomSession {
    fn new() -> Self {
        Self {
            response: ResponseHeader::build(503, None).unwrap(),
            request_header_written: false,
            fail_after_terminal: false,
            pending_body: std::collections::VecDeque::new(),
            pending_trailers: false,
            upgraded: false,
            body_eof_observed: false,
            terminal_upgrade: false,
        }
    }
}

#[async_trait]
impl CustomConnector for HeaderOnlyCustomConnector {
    type Session = HeaderOnlyCustomSession;

    async fn get_http_session<P: Peer + Send + Sync + 'static>(
        &self,
        _peer: &P,
    ) -> Result<(CustomConnection<Self::Session>, bool)> {
        Ok((
            CustomConnection::Session(HeaderOnlyCustomSession::new()),
            false,
        ))
    }

    async fn reused_http_session<P: Peer + Send + Sync + 'static>(
        &self,
        _peer: &P,
    ) -> Option<Self::Session> {
        None
    }

    async fn release_http_session<P: Peer + Send + Sync + 'static>(
        &self,
        session: Self::Session,
        _peer: &P,
        _idle_timeout: Option<Duration>,
    ) {
        if session.fail_after_terminal {
            FAILED_CUSTOM_SESSION_RELEASED.store(true, Ordering::SeqCst);
        }
        if !session.request_header_written {
            PRE_WRITE_CUSTOM_SESSION_RELEASED.store(true, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl CustomSession for HeaderOnlyCustomSession {
    async fn write_request_header(&mut self, req: Box<RequestHeader>, _end: bool) -> Result<()> {
        self.request_header_written = true;
        self.fail_after_terminal = req
            .headers
            .get(CUSTOM_POST_TERMINAL_FAILURE_HEADER.0)
            .is_some();
        if req.headers.get(CUSTOM_TRAILERED_HEADER.0).is_some() {
            self.response = ResponseHeader::build(200, None).unwrap();
            self.pending_body = CUSTOM_TRAILERED_CHUNKS
                .iter()
                .map(|c| Bytes::from_static(c))
                .collect();
            self.pending_trailers = true;
        }
        if req.headers.get(CUSTOM_EMPTY_UPGRADE_HEADER.0).is_some() {
            self.response =
                ResponseHeader::build(http::StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
            self.response
                .insert_header(http::header::CONNECTION, "upgrade")?;
            self.response
                .insert_header(http::header::UPGRADE, "websocket")?;
            self.upgraded = true;
        }
        if req.headers.get(CUSTOM_TERMINAL_UPGRADE_HEADER.0).is_some() {
            self.response =
                ResponseHeader::build(http::StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
            self.response
                .insert_header(http::header::CONNECTION, "upgrade")?;
            self.response
                .insert_header(http::header::UPGRADE, "websocket")?;
            self.upgraded = true;
            self.terminal_upgrade = true;
        }
        Ok(())
    }

    async fn write_request_body(&mut self, _data: Bytes, _end: bool) -> Result<()> {
        Ok(())
    }

    async fn finish_request_body(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_read_timeout(&mut self, _timeout: Option<Duration>) {}

    fn set_write_timeout(&mut self, _timeout: Option<Duration>) {}

    async fn read_response_header(&mut self) -> Result<()> {
        Ok(())
    }

    async fn read_response_body(&mut self) -> Result<Option<Bytes>> {
        if self.terminal_upgrade {
            TERMINAL_UPGRADE_SESSION_READ_AFTER_EOS.store(true, Ordering::SeqCst);
            return Ok(None);
        }
        if let Some(chunk) = self.pending_body.pop_front() {
            return Ok(Some(chunk));
        }
        if self.fail_after_terminal {
            CUSTOM_POST_TERMINAL_FAILURE_EMITTED.store(true, Ordering::SeqCst);
            return pingora_error::Error::e_explain(
                pingora_error::ErrorType::ReadError,
                "scripted failure after terminal custom response header",
            );
        }
        self.body_eof_observed = true;
        Ok(None)
    }

    fn response_finished(&self) -> bool {
        self.pending_body.is_empty()
            && !self.pending_trailers
            && (!self.upgraded || self.terminal_upgrade || self.body_eof_observed)
    }

    async fn shutdown(&mut self, _code: u32, _ctx: &str) {}

    fn response_header(&self) -> Option<&ResponseHeader> {
        Some(&self.response)
    }

    fn was_upgraded(&self) -> bool {
        self.upgraded
    }

    fn digest(&self) -> Option<&Digest> {
        None
    }

    fn digest_mut(&mut self) -> Option<&mut Digest> {
        None
    }

    fn server_addr(&self) -> Option<&SocketAddr> {
        None
    }

    fn client_addr(&self) -> Option<&SocketAddr> {
        None
    }

    async fn read_trailers(&mut self) -> Result<Option<HeaderMap>> {
        if self.terminal_upgrade {
            TERMINAL_UPGRADE_SESSION_READ_AFTER_EOS.store(true, Ordering::SeqCst);
            return Ok(None);
        }
        if self.pending_trailers {
            self.pending_trailers = false;
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            return Ok(Some(trailers));
        }
        Ok(None)
    }

    fn fd(&self) -> UniqueIDType {
        0
    }

    async fn check_response_end_or_error(&mut self, _headers: bool) -> Result<bool> {
        // Mirrors `pingora_core::protocols::http::v2::client`: while trailers
        // are still pending the per-chunk end-of-stream predicate is false, so
        // no body task ever carries `eos = true`.
        Ok(self.response_finished())
    }

    fn take_request_body_writer(&mut self) -> Option<Box<dyn BodyWrite>> {
        Some(Box::new(NoopBodyWriter))
    }

    async fn finish_custom(&mut self) -> Result<()> {
        Ok(())
    }

    fn take_custom_message_reader(
        &mut self,
    ) -> Option<Box<dyn FuturesStream<Item = Result<Bytes>> + Unpin + Send + Sync + 'static>> {
        Some(Box::new(futures::stream::empty()))
    }

    async fn drain_custom_messages(&mut self) -> Result<()> {
        Ok(())
    }

    fn take_custom_message_writer(&mut self) -> Option<Box<dyn CustomMessageWrite>> {
        Some(Box::new(()))
    }
}

#[async_trait]
impl ProxyHttp for EmitProxy {
    type CTX = EmitCtx;

    fn new_ctx(&self) -> Self::CTX {
        EmitCtx::default()
    }

    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        if self.cache {
            let path = session.req_header().uri.path();
            let storage: &'static (dyn Storage + Sync) = if path
                .starts_with("/cache/suppressed_sink_extras/")
                || path.starts_with("/cache/non_streaming_range/")
            {
                &*NON_STREAMING_CACHE_BACKEND
            } else {
                &*CACHE_BACKEND
            };
            session.cache.enable(storage, None, None, None, None);
        }
        Ok(())
    }

    fn cache_key_callback(&self, session: &Session, _ctx: &mut Self::CTX) -> Result<CacheKey> {
        // Not production ready (ignores Vary, scheme, ...) -- fine for a test
        // fixture with one path per case. See `ProxyHttp::cache_key_callback`
        // for what a real implementation needs to consider.
        Ok(CacheKey::new(
            session.req_header().uri.path().to_string(),
            String::new(),
        ))
    }

    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        ctx.response_cache_filter_seen = true;
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
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.response_filter_seen = true;
        match session.req_header().uri.path() {
            "/cache/bodyless_replace" | "/cache/bodyful_response_filter_once" => {
                ctx.response_filter_calls += 1;
                upstream_response.insert_header(
                    RESPONSE_FILTER_CALLS_HEADER,
                    ctx.response_filter_calls.to_string(),
                )?;
            }
            _ => {}
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_cache_order")
            && !ctx.response_cache_filter_seen
        {
            return pingora_error::Error::e_explain(
                pingora_error::ErrorType::InternalError,
                "response_cache_filter ran after response_filter",
            );
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_response_filter_error")
        {
            return pingora_error::Error::e_explain(
                pingora_error::ErrorType::InternalError,
                "test response filter rejection",
            );
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_late_204")
        {
            upstream_response.set_status(http::StatusCode::NO_CONTENT)?;
        }
        if session.req_header().uri.path() == CUSTOM_REWRITTEN_TERMINAL_UPGRADE_PATH {
            // The FILTERED header is what decides the downstream handshake, so
            // a 101 rewritten away here must leave the response un-upgraded.
            upstream_response.set_status(http::StatusCode::OK)?;
            upstream_response.remove_header(&http::header::CONNECTION);
            upstream_response.remove_header(&http::header::UPGRADE);
        }
        if session
            .req_header()
            .uri
            .path()
            .starts_with("/cache/suppressed_sink_extras/")
            && session
                .req_header()
                .headers
                .get(SUPPRESS_DOWNSTREAM_BODY_HEADER.0)
                .is_some()
        {
            upstream_response.set_status(http::StatusCode::NO_CONTENT)?;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/late_204_bodyful")
        {
            upstream_response.set_status(http::StatusCode::NO_CONTENT)?;
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_late_200")
        {
            upstream_response.set_status(http::StatusCode::OK)?;
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_content_length")
        {
            upstream_response.insert_header(
                http::header::CONTENT_LENGTH,
                (BODYLESS_CURRENT.len() + BODYLESS_EXTRA.len()).to_string(),
            )?;
        }
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

    async fn upstream_response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        let path = session.req_header().uri.path();
        if path == "/cache/custom_range/spoof_bodyful" {
            upstream_response.insert_header("x-pingora-internal-terminal-synthetic", "spoof")?;
        } else if path.ends_with("/bodyless_cl_exact") {
            upstream_response.insert_header(
                http::header::CONTENT_LENGTH,
                (BODYLESS_CURRENT.len() + BODYLESS_EXTRA.len()).to_string(),
            )?;
        } else if path.contains("bodyless")
            && !path.ends_with("/bodyless_cl0")
            && !path.ends_with("/bodyless_empty_cl0")
        {
            upstream_response.remove_header("content-length");
        }
        Ok(())
    }

    fn range_header_filter(
        &self,
        session: &mut Session,
        response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> RangeType {
        if session
            .req_header()
            .uri
            .path()
            .starts_with("/cache/custom_range/")
        {
            let calls = if session.req_header().uri.path().ends_with("/spoof_bodyful") {
                &SPOOF_CUSTOM_RANGE_FILTER_CALLS
            } else {
                &TERMINAL_CUSTOM_RANGE_FILTER_CALLS
            };
            calls.fetch_add(1, Ordering::SeqCst);
            response
                .set_status(http::StatusCode::RANGE_NOT_SATISFIABLE)
                .unwrap();
            response
                .insert_header(http::header::CONTENT_LENGTH, "0")
                .unwrap();
            response
                .insert_header("x-custom-range-filter", "called")
                .unwrap();
            RangeType::Invalid
        } else {
            pingora_proxy::range_header_filter(session.req_header(), response, Some(200))
        }
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
        let mut peer = Box::new(HttpPeer::new(
            format!("127.0.0.1:{}", self.origin_port),
            false,
            "".to_string(),
        ));
        if self.custom {
            peer.options.alpn = ALPN::Custom(CustomALPN::new(b"test-custom".to_vec()));
        }
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        if session.req_header().uri.path() == "/custom_pre_write_rejection" {
            return pingora_error::Error::e_explain(
                pingora_error::ErrorType::InternalError,
                "scripted custom request rejection before the upstream write",
            );
        }
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
        end_of_stream: bool,
        sink: &mut ResponseBodySink,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        if end_of_stream {
            let path = session.req_header().uri.path();
            if matches!(
                path,
                CUSTOM_TERMINAL_UPGRADE_PATH
                    | CUSTOM_NAKED_TERMINAL_UPGRADE_PATH
                    | CUSTOM_REWRITTEN_TERMINAL_UPGRADE_PATH
            ) {
                *TERMINAL_UPGRADE_EOS_CALLS
                    .lock()
                    .unwrap()
                    .entry(path.to_string())
                    .or_default() += 1;
            }
        }
        match session.req_header().uri.path() {
            path if path.ends_with("/bodyless_no_upstream_output")
                || path.ends_with("/bodyless_empty_cl0") => {}
            path if path.contains("bodyless") && end_of_stream && body.is_none() => {
                if path.ends_with("/bodyless_response_filter_error") {
                    TERMINAL_HOOK_RAN_AFTER_HEADER_FILTER_ERROR.store(true, Ordering::SeqCst);
                }
                if path.ends_with("/bodyless_hook_order") && !ctx.response_filter_seen {
                    return pingora_error::Error::e_explain(
                        pingora_error::ErrorType::InternalError,
                        "terminal body hook ran before response_filter",
                    );
                }
                *body = Some(Bytes::from_static(BODYLESS_CURRENT));
                sink.push(Bytes::from_static(BODYLESS_EXTRA))?;
                // This is redundant at a natural header EOS and must not burn
                // either connection or fail the custom pump.
                sink.terminate();
            }
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
            path if path.starts_with("/cache/suppressed_sink_extras/") && body.is_some() => {
                if sink.remaining_budget() == RESPONSE_BODY_EMIT_BUDGET {
                    SUPPRESSION_BATCH_STARTS.fetch_add(1, Ordering::SeqCst);
                } else {
                    SUPPRESSION_SHARED_BATCH_SEEN.store(true, Ordering::SeqCst);
                }
                let mut expected = SUPPRESSED_CACHE_EXPECTED_BODY.lock().unwrap();
                expected.extend_from_slice(body.as_ref().unwrap());
                expected.extend_from_slice(SUPPRESSED_BODY_EXTRA);
                sink.push(Bytes::from_static(SUPPRESSED_BODY_EXTRA))?;
            }
            CUSTOM_TRAILERED_PATH => {
                // The canonical withholding processor: take every chunk and
                // release the whole body only at end-of-stream. Without the
                // terminal dispatch on `Trailer`/`Done` this never fires and
                // the client sees an empty 200. The `|eos` marker is written by
                // the terminal callback itself, so the client-visible body
                // doubles as the callback count.
                if let Some(bytes) = body.take() {
                    ctx.withheld_body.extend_from_slice(&bytes);
                }
                if end_of_stream {
                    let mut released = std::mem::take(&mut ctx.withheld_body);
                    released.extend_from_slice(b"|eos");
                    *body = Some(Bytes::from(released));
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
        Ok((session.req_header().uri.path() == "/bodyless_delay")
            .then_some(Duration::from_millis(50)))
    }

    fn response_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        match session.req_header().uri.path() {
            "/cache/terminal_head/bodyless" => {
                TERMINAL_CACHED_HEAD_BODY_FILTER_CALLS.fetch_add(1, Ordering::SeqCst);
            }
            "/cache/terminal_conditional/bodyless" => {
                TERMINAL_CACHED_304_BODY_FILTER_CALLS.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_late_204")
        {
            return pingora_error::Error::e_explain(
                pingora_error::ErrorType::InternalError,
                "downstream body filter ran for a body-forbidden response",
            );
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_filtered")
        {
            if let Some(data) = body {
                *data = Bytes::copy_from_slice(data.to_ascii_uppercase().as_slice());
            }
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_eos_filtered")
            && end_of_stream
        {
            match body {
                Some(data) => {
                    let mut bytes = data.to_vec();
                    bytes.extend_from_slice(b"-eos");
                    *data = Bytes::from(bytes);
                }
                None => *body = Some(Bytes::from_static(b"-eos")),
            }
        }
        if session
            .req_header()
            .uri
            .path()
            .ends_with("/bodyless_no_upstream_output")
            && end_of_stream
        {
            assert!(body.is_none(), "upstream hook unexpectedly generated bytes");
            *body = Some(Bytes::from_static(b"eos-only"));
        }
        Ok(None)
    }
}

struct Harness {
    proxy_port: u16,
    cache_proxy_port: u16,
    custom_proxy_port: u16,
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

    fn custom_base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.custom_proxy_port)
    }
}

fn start_harness() -> Harness {
    let origin_port = spawn_origin();
    let proxy_port = reserve_port();
    let cache_proxy_port = reserve_port();
    let custom_proxy_port = reserve_port();
    let proxy_addr = format!("127.0.0.1:{proxy_port}");
    let cache_proxy_addr = format!("127.0.0.1:{cache_proxy_port}");
    let custom_proxy_addr = format!("127.0.0.1:{custom_proxy_port}");
    let listen_addr = proxy_addr.clone();
    let cache_listen_addr = cache_proxy_addr.clone();
    let custom_listen_addr = custom_proxy_addr.clone();

    thread::spawn(move || {
        let mut server = Server::new(None).unwrap();
        server.bootstrap();

        let mut proxy_service = pingora_proxy::http_proxy_service(
            &server.configuration,
            EmitProxy {
                origin_port,
                custom: false,
                cache: false,
                terminate_once_fired: AtomicBool::new(false),
            },
        );
        proxy_service.add_tcp(&listen_addr);

        let mut cache_proxy_service = pingora_proxy::http_proxy_service(
            &server.configuration,
            EmitProxy {
                origin_port,
                custom: false,
                cache: true,
                terminate_once_fired: AtomicBool::new(false),
            },
        );
        cache_proxy_service.add_tcp(&cache_listen_addr);

        let custom_handler: ProcessCustomSession<EmitProxy, HeaderOnlyCustomConnector> =
            Arc::new(|_proxy, _stream: Stream, _shutdown: &ShutdownWatch| Box::pin(async { None }));
        let mut custom_proxy_service = ProxyServiceBuilder::new(
            &server.configuration,
            EmitProxy {
                origin_port,
                custom: true,
                cache: false,
                terminate_once_fired: AtomicBool::new(false),
            },
        )
        .custom(HeaderOnlyCustomConnector, custom_handler)
        .build();
        custom_proxy_service.add_tcp(&custom_listen_addr);

        let services: Vec<Box<dyn ServiceWithDependents>> = vec![
            Box::new(proxy_service),
            Box::new(cache_proxy_service),
            Box::new(custom_proxy_service),
        ];
        server.add_services(services);
        server.run_forever();
    });

    // Poll for readiness instead of sleeping, matching `tests/seam/harness.rs`.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    for addr in [&proxy_addr, &cache_proxy_addr, &custom_proxy_addr] {
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
        custom_proxy_port,
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

#[tokio::test]
async fn h1_header_eos_runs_body_hook_and_keeps_downstream_reusable() {
    let harness = init();
    let mut io = TcpStream::connect(("127.0.0.1", harness.proxy_port))
        .await
        .unwrap();

    for path in ["/bodyless_replace", "/bodyless_replace_again"] {
        io.write_all(
            format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\n{}: {}\r\nConnection: keep-alive\r\n\r\n",
                BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let response = read_http1_response(&mut io).await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains("transfer-encoding: chunked"),
            "a generated body must use reusable framing: {response}"
        );
        assert!(
            response.contains("F\r\ngenerated-extra\r\n0\r\n\r\n"),
            "{response}"
        );
    }
}

async fn read_http1_response(io: &mut TcpStream) -> String {
    let mut response = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(10), io.read(&mut buf))
            .await
            .expect("timed out waiting for proxy response")
            .expect("failed to read proxy response");
        assert!(
            n > 0,
            "proxy closed before completing the response: {:?}",
            String::from_utf8_lossy(&response)
        );
        response.extend_from_slice(&buf[..n]);
        let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let body_start = header_end + 4;
        let head = String::from_utf8_lossy(&response[..body_start]).to_ascii_lowercase();
        let status_has_no_body = head.starts_with("http/1.1 204")
            || head.starts_with("http/1.1 304")
            || head.starts_with("http/1.1 1");
        if status_has_no_body {
            return String::from_utf8(response).expect("test response must be UTF-8");
        }
        if head.contains("transfer-encoding: chunked")
            && response[body_start..]
                .windows(5)
                .any(|window| window == b"0\r\n\r\n")
        {
            return String::from_utf8(response).expect("test response must be UTF-8");
        }
        if let Some(content_length) = head.lines().find_map(|line| {
            line.strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        }) {
            if response.len() >= body_start + content_length {
                return String::from_utf8(response).expect("test response must be UTF-8");
            }
        }
    }
}

#[tokio::test]
async fn header_eos_generated_body_is_identical_on_cache_miss_and_hit() {
    let harness = init();
    let client = reqwest::Client::new();
    let url = format!("{}/cache/bodyless_replace", harness.cache_base_url());

    let first = client
        .get(&url)
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(first.headers()["x-cache-status"], "miss");
    assert_eq!(first.headers()[RESPONSE_FILTER_CALLS_HEADER], "1");
    assert_eq!(first.text().await.unwrap(), "generated-extra");

    let second = client
        .get(&url)
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(second.headers()["x-cache-status"], "hit");
    assert_eq!(second.headers()[RESPONSE_FILTER_CALLS_HEADER], "1");
    assert_eq!(second.text().await.unwrap(), "generated-extra");
}

/// Control: the non-terminal header path was not affected by the terminal
/// header regression. It must continue to run `response_filter` once for
/// each cache miss and hit request.
#[tokio::test]
async fn bodyful_cache_path_control_runs_response_filter_once_per_request() {
    let harness = init();
    let client = reqwest::Client::new();
    let url = format!(
        "{}/cache/bodyful_response_filter_once",
        harness.cache_base_url()
    );

    let first = client.get(&url).send().await.unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    assert_eq!(first.headers()["x-cache-status"], "miss");
    assert_eq!(first.headers()[RESPONSE_FILTER_CALLS_HEADER], "1");
    assert_eq!(first.text().await.unwrap(), "hello world");

    let second = client.get(&url).send().await.unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    assert_eq!(second.headers()["x-cache-status"], "hit");
    assert_eq!(second.headers()[RESPONSE_FILTER_CALLS_HEADER], "1");
    assert_eq!(second.text().await.unwrap(), "hello world");
}

#[tokio::test]
async fn terminal_body_hook_runs_after_response_filter() {
    let harness = init();
    let response = reqwest::Client::new()
        .get(format!("{}/bodyless_hook_order", harness.base_url()))
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.text().await.unwrap(), "generated-extra");
}

#[tokio::test]
async fn terminal_body_hook_does_not_run_when_response_filter_rejects_the_header() {
    TERMINAL_HOOK_RAN_AFTER_HEADER_FILTER_ERROR.store(false, Ordering::SeqCst);
    let harness = init();
    let _ = reqwest::Client::new()
        .get(format!(
            "{}/bodyless_response_filter_error",
            harness.base_url()
        ))
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await;
    assert!(!TERMINAL_HOOK_RAN_AFTER_HEADER_FILTER_ERROR.load(Ordering::SeqCst));
}

#[tokio::test]
async fn synthetic_body_uses_the_same_downstream_filter_on_live_miss_and_hit() {
    let harness = init();
    let client = reqwest::Client::new();

    let live = client
        .get(format!("{}/bodyless_filtered", harness.base_url()))
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(live.text().await.unwrap(), "GENERATED-EXTRA");

    let url = format!("{}/cache/bodyless_filtered", harness.cache_base_url());
    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
            .send()
            .await
            .unwrap();
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert_eq!(response.text().await.unwrap(), "GENERATED-EXTRA");
    }
}

#[tokio::test]
async fn downstream_eos_filter_is_identical_on_live_streaming_miss_and_hit() {
    let harness = init();
    let client = reqwest::Client::new();

    let live = client
        .get(format!("{}/bodyless_eos_filtered", harness.base_url()))
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(live.text().await.unwrap(), "generated-extra-eos");

    let url = format!("{}/cache/bodyless_eos_filtered", harness.cache_base_url());
    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
            .send()
            .await
            .unwrap();
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert_eq!(response.text().await.unwrap(), "generated-extra-eos");
    }
}

#[tokio::test]
async fn empty_terminal_upstream_output_still_drives_downstream_eos_on_live_miss_and_hit() {
    let harness = init();
    let client = reqwest::Client::new();

    let live = client
        .get(format!(
            "{}/bodyless_no_upstream_output",
            harness.base_url()
        ))
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(live.text().await.unwrap(), "eos-only");

    let url = format!(
        "{}/cache/bodyless_no_upstream_output",
        harness.cache_base_url()
    );
    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
            .send()
            .await
            .unwrap();
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert_eq!(response.text().await.unwrap(), "eos-only");
    }
}

#[tokio::test]
async fn terminal_cache_callback_runs_before_downstream_response_filter() {
    let harness = init();
    let response = reqwest::Client::new()
        .get(format!(
            "{}/cache/bodyless_cache_order",
            harness.cache_base_url()
        ))
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.text().await.unwrap(), "generated-extra");
}

#[tokio::test]
async fn final_status_can_make_an_originally_bodyless_response_bodyful() {
    let harness = init();
    let response = reqwest::Client::new()
        .get(format!("{}/bodyless_late_200", harness.base_url()))
        .header(BODYLESS_NO_CONTENT_HEADER.0, BODYLESS_NO_CONTENT_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "generated-extra");
}

#[tokio::test]
async fn h1_framing_reconciles_content_length_after_terminal_body_generation() {
    let harness = init();
    let mut io = TcpStream::connect(("127.0.0.1", harness.proxy_port))
        .await
        .unwrap();
    io.write_all(
        format!(
            "GET /bodyless_content_length HTTP/1.1\r\nHost: localhost\r\n{}: {}\r\nConnection: keep-alive\r\n\r\n",
            BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let response = read_http1_response(&mut io).await;
    let lower = response.to_ascii_lowercase();
    assert!(lower.contains("content-length: 15"), "{response}");
    assert!(!lower.contains("transfer-encoding"), "{response}");
    assert!(response.ends_with("generated-extra"), "{response}");
}

#[tokio::test]
async fn h1_cache_miss_readback_normalizes_substituted_http10_terminal_header() {
    let harness = init();
    let mut io = TcpStream::connect(("127.0.0.1", harness.cache_proxy_port))
        .await
        .unwrap();
    io.write_all(
        format!(
            "GET /cache/http10/bodyless_cl_exact HTTP/1.1\r\nHost: localhost\r\n{}: {}\r\nConnection: keep-alive\r\n\r\n",
            HTTP10_BODYLESS_OK_ORIGIN_HEADER.0, HTTP10_BODYLESS_OK_ORIGIN_HEADER.1
        )
        .as_bytes(),
    )
    .await
    .unwrap();

    let response = read_http1_response(&mut io).await;
    let lower = response.to_ascii_lowercase();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(lower.contains("x-cache-status: miss"), "{response}");
    assert!(lower.contains("content-length: 15"), "{response}");
    assert!(!lower.contains("transfer-encoding"), "{response}");
    assert!(response.ends_with("generated-extra"), "{response}");
}

#[tokio::test]
async fn original_content_length_zero_is_reconciled_for_generated_body() {
    let harness = init();
    let mut io = TcpStream::connect(("127.0.0.1", harness.proxy_port))
        .await
        .unwrap();
    io.write_all(
        format!(
            "GET /bodyless_cl0 HTTP/1.1\r\nHost: localhost\r\n{}: {}\r\nConnection: keep-alive\r\n\r\n",
            BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let response = read_http1_response(&mut io).await;
    let lower = response.to_ascii_lowercase();
    assert!(!lower.contains("content-length: 0"), "{response}");
    assert!(lower.contains("transfer-encoding: chunked"), "{response}");
    assert!(response.contains("generated-extra"), "{response}");

    let client = reqwest::Client::new();
    let url = format!("{}/cache/bodyless_cl0", harness.cache_base_url());
    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
            .send()
            .await
            .unwrap();
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert_ne!(
            response
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
        assert_eq!(response.text().await.unwrap(), "generated-extra");
    }
}

#[tokio::test]
async fn terminal_generated_body_range_is_identical_on_cache_miss_and_hit() {
    let harness = init();
    let client = reqwest::Client::new();
    let url = format!("{}/cache/range/bodyless_cl0", harness.cache_base_url());

    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_OK_ORIGIN_HEADER.0, BODYLESS_OK_ORIGIN_HEADER.1)
            .header(reqwest::header::RANGE, "bytes=0-4")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert_eq!(response.text().await.unwrap(), "generated-extra");
    }
}

#[tokio::test]
async fn exact_length_terminal_body_is_not_ranged_on_direct_live_path() {
    let harness = init();
    let response = reqwest::Client::new()
        .get(format!(
            "{}/cache/non_streaming_range/bodyless_cl_exact",
            harness.cache_base_url()
        ))
        .header(BODYLESS_OK_ORIGIN_HEADER.0, BODYLESS_OK_ORIGIN_HEADER.1)
        .header(reqwest::header::RANGE, "bytes=0-4")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["x-cache-status"], "miss");
    assert_eq!(response.text().await.unwrap(), "generated-extra");
}

#[tokio::test]
async fn exact_length_terminal_body_is_not_ranged_on_cache_miss_or_hit() {
    let harness = init();
    let client = reqwest::Client::new();
    let url = format!("{}/cache/range/bodyless_cl_exact", harness.cache_base_url());

    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_OK_ORIGIN_HEADER.0, BODYLESS_OK_ORIGIN_HEADER.1)
            .header(reqwest::header::RANGE, "bytes=0-4")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert_eq!(response.text().await.unwrap(), "generated-extra");
    }
}

#[tokio::test]
async fn empty_terminal_entity_is_not_ranged_on_cache_miss_or_hit() {
    let harness = init();
    let client = reqwest::Client::new();
    let url = format!(
        "{}/cache/range/bodyless_empty_cl0",
        harness.cache_base_url()
    );

    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_OK_ORIGIN_HEADER.0, BODYLESS_OK_ORIGIN_HEADER.1)
            .header(reqwest::header::RANGE, "bytes=0-4")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert!(response.bytes().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn terminal_cache_marker_skips_custom_range_filter_and_never_reaches_client() {
    TERMINAL_CUSTOM_RANGE_FILTER_CALLS.store(0, Ordering::SeqCst);
    let harness = init();
    let client = reqwest::Client::new();
    let url = format!(
        "{}/cache/custom_range/bodyless_cl_exact",
        harness.cache_base_url()
    );

    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_OK_ORIGIN_HEADER.0, BODYLESS_OK_ORIGIN_HEADER.1)
            .header(reqwest::header::RANGE, "bytes=0-4")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert!(!response.headers().contains_key("x-custom-range-filter"));
        assert!(!response
            .headers()
            .contains_key("x-pingora-internal-terminal-synthetic"));
        assert_eq!(response.text().await.unwrap(), "generated-extra");
    }
    assert_eq!(TERMINAL_CUSTOM_RANGE_FILTER_CALLS.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn origin_cannot_spoof_the_terminal_cache_marker() {
    SPOOF_CUSTOM_RANGE_FILTER_CALLS.store(0, Ordering::SeqCst);
    let harness = init();
    let client = reqwest::Client::new();
    let url = format!(
        "{}/cache/custom_range/spoof_bodyful",
        harness.cache_base_url()
    );

    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(reqwest::header::RANGE, "bytes=0-4")
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::RANGE_NOT_SATISFIABLE
        );
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert!(!response
            .headers()
            .contains_key("x-pingora-internal-terminal-synthetic"));
        assert!(response.bytes().await.unwrap().is_empty());
    }
    assert_eq!(SPOOF_CUSTOM_RANGE_FILTER_CALLS.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn late_bodyless_status_drains_a_bodyful_origin_and_allows_reuse() {
    let harness = init();
    let mut io = TcpStream::connect(("127.0.0.1", harness.proxy_port))
        .await
        .unwrap();
    io.write_all(
        format!(
            "GET /late_204_bodyful HTTP/1.1\r\nHost: localhost\r\n{}: {}\r\nConnection: keep-alive\r\n\r\n",
            MANY_CHUNK_HEADER.0, MANY_CHUNK_HEADER.1
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let first = read_http1_response(&mut io).await;
    assert!(first.starts_with("HTTP/1.1 204"), "{first}");

    tokio::time::sleep(Duration::from_millis(100)).await;
    io.write_all(b"GET /plain HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
        .await
        .unwrap();
    let second = read_http1_response(&mut io).await;
    assert!(second.starts_with("HTTP/1.1 200"), "{second}");
    assert!(second.ends_with("hello world"), "{second}");
}

#[tokio::test]
async fn late_bodyless_cache_miss_finishes_admission_and_becomes_a_hit() {
    let harness = init();
    for expected_cache_status in ["miss", "hit"] {
        let mut io = TcpStream::connect(("127.0.0.1", harness.cache_proxy_port))
            .await
            .unwrap();
        io.write_all(
            format!(
                "GET /cache/late_204_bodyful HTTP/1.1\r\nHost: localhost\r\n{}: {}\r\nConnection: close\r\n\r\n",
                MANY_CHUNK_HEADER.0, MANY_CHUNK_HEADER.1
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let response = read_http1_response(&mut io).await;
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains(&format!("x-cache-status: {expected_cache_status}")),
            "{response}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_complete_cache_entry(path: &str) {
    let key = CacheKey::new(path.to_string(), String::new());
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let trace = Span::inactive().handle();
            if NON_STREAMING_CACHE_BACKEND
                .lookup(&key, &trace)
                .await
                .unwrap()
                .is_some_and(|(_, handler)| handler.can_seek())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cache admission did not complete within two seconds");
}

async fn assert_suppressed_sink_extras_are_cached_once(path: &str, use_head: bool) {
    let harness = init();
    let client = reqwest::Client::new();
    SUPPRESSED_CACHE_EXPECTED_BODY.lock().unwrap().clear();
    SUPPRESSION_SHARED_BATCH_SEEN.store(false, Ordering::SeqCst);
    SUPPRESSION_BATCH_STARTS.store(0, Ordering::SeqCst);
    let url = format!("{}{path}", harness.cache_base_url());

    let mut miss_request = if use_head {
        client.head(&url)
    } else {
        client.get(&url).header(
            SUPPRESS_DOWNSTREAM_BODY_HEADER.0,
            SUPPRESS_DOWNSTREAM_BODY_HEADER.1,
        )
    };
    miss_request = miss_request.header(SUPPRESSION_BATCH_HEADER.0, SUPPRESSION_BATCH_HEADER.1);
    let miss = miss_request.send().await.unwrap();
    assert_eq!(
        miss.status(),
        if use_head {
            reqwest::StatusCode::OK
        } else {
            reqwest::StatusCode::NO_CONTENT
        }
    );
    assert_eq!(miss.headers()["x-cache-status"], "miss");
    assert!(miss.bytes().await.unwrap().is_empty());

    wait_for_complete_cache_entry(path).await;
    assert!(
        SUPPRESSION_SHARED_BATCH_SEEN.load(Ordering::SeqCst),
        "fixture did not place multiple body callbacks in one pump batch"
    );
    assert!(
        SUPPRESSION_BATCH_STARTS.load(Ordering::SeqCst) >= 2,
        "fixture did not split suppressed body callbacks across pump batches"
    );
    let expected = SUPPRESSED_CACHE_EXPECTED_BODY.lock().unwrap().clone();

    if use_head {
        let head_hit = client.head(&url).send().await.unwrap();
        assert_eq!(head_hit.status(), reqwest::StatusCode::OK);
        assert_eq!(head_hit.headers()["x-cache-status"], "hit");
        assert!(head_hit.bytes().await.unwrap().is_empty());
    }

    let hit = client.get(&url).send().await.unwrap();
    assert_eq!(hit.status(), reqwest::StatusCode::OK);
    assert_eq!(hit.headers()["x-cache-status"], "hit");
    let actual = hit.bytes().await.unwrap();
    assert_eq!(
        actual.len(),
        expected.len(),
        "sink extras changed the cached body length"
    );
    assert_eq!(
        actual.as_ref(),
        expected.as_slice(),
        "sink extras were cached out of order"
    );
}

#[tokio::test]
async fn suppressed_sink_extras_are_cached_once_across_batches_for_status_and_head() {
    // Late status rewriting drives suppression through the final response
    // status; HEAD drives the same pump guard through the request method.
    assert_suppressed_sink_extras_are_cached_once(
        "/cache/suppressed_sink_extras/late_status",
        false,
    )
    .await;
    assert_suppressed_sink_extras_are_cached_once("/cache/suppressed_sink_extras/head", true).await;
}

#[tokio::test]
async fn late_bodyless_status_suppresses_cached_generated_body() {
    let harness = init();
    for (protocol, client, path) in [("h1", reqwest::Client::new(), "cache")] {
        let url = format!("{}/{path}/bodyless_late_204", harness.cache_base_url());
        for expected_cache_status in ["miss", "hit"] {
            let response = client
                .get(&url)
                .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
            assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
            assert!(
                !response
                    .headers()
                    .contains_key(reqwest::header::TRANSFER_ENCODING),
                "unexpected {protocol} framing on {expected_cache_status}: {:?}",
                response.headers()
            );
            assert_eq!(response.bytes().await.unwrap().len(), 0);
        }
    }
}

#[tokio::test]
async fn terminal_cached_head_is_header_only_on_miss_and_hit() {
    let harness = init();
    TERMINAL_CACHED_HEAD_BODY_FILTER_CALLS.store(0, Ordering::SeqCst);

    for expected_cache_status in ["miss", "hit"] {
        let mut io = TcpStream::connect(("127.0.0.1", harness.cache_proxy_port))
            .await
            .unwrap();
        io.write_all(
            format!(
                "HEAD /cache/terminal_head/bodyless HTTP/1.1\r\nHost: localhost\r\n{}: {}\r\nConnection: close\r\n\r\n",
                BODYLESS_OK_ORIGIN_HEADER.0, BODYLESS_OK_ORIGIN_HEADER.1
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), io.read_to_end(&mut response))
            .await
            .expect("terminal cached HEAD response did not complete")
            .unwrap();
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("terminal cached HEAD response had no header terminator")
            + 4;
        let head = String::from_utf8_lossy(&response[..header_end]).to_ascii_lowercase();
        assert!(head.starts_with("http/1.1 200"), "{head}");
        assert!(
            head.contains(&format!("x-cache-status: {expected_cache_status}")),
            "{head}"
        );
        assert!(head.contains("etag: \"terminal-v1\""), "{head}");
        assert!(
            response[header_end..].is_empty(),
            "terminal cached HEAD leaked body bytes: {:?}",
            &response[header_end..]
        );
    }
    assert_eq!(
        TERMINAL_CACHED_HEAD_BODY_FILTER_CALLS.load(Ordering::SeqCst),
        0,
        "cached HEAD follow-up body tasks reached the downstream body filter"
    );

    let get_hit = reqwest::get(format!(
        "{}/cache/terminal_head/bodyless",
        harness.cache_base_url()
    ))
    .await
    .unwrap();
    assert_eq!(get_hit.headers()["x-cache-status"], "hit");
    assert_eq!(get_hit.text().await.unwrap(), "generated-extra");
}

#[tokio::test]
async fn terminal_cached_if_none_match_is_304_on_miss_and_hit() {
    let harness = init();
    TERMINAL_CACHED_304_BODY_FILTER_CALLS.store(0, Ordering::SeqCst);
    let client = reqwest::Client::new();
    let url = format!(
        "{}/cache/terminal_conditional/bodyless",
        harness.cache_base_url()
    );

    for expected_cache_status in ["miss", "hit"] {
        let response = client
            .get(&url)
            .header(BODYLESS_OK_ORIGIN_HEADER.0, BODYLESS_OK_ORIGIN_HEADER.1)
            .header(reqwest::header::IF_NONE_MATCH, "\"terminal-v1\"")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers()["x-cache-status"], expected_cache_status);
        assert!(response.bytes().await.unwrap().is_empty());
    }
    assert_eq!(
        TERMINAL_CACHED_304_BODY_FILTER_CALLS.load(Ordering::SeqCst),
        0,
        "cached 304 follow-up body tasks reached the downstream body filter"
    );
}

#[tokio::test]
async fn custom_header_eos_runs_body_hook_without_terminate_failure() {
    let harness = init();
    let response = reqwest::get(format!("{}/bodyless", harness.custom_base_url()))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.text().await.unwrap(), "generated-extra");
}

/// The custom pump has the same terminal-dispatch hole as the H2 pump:
/// `upstream_filter` reaches the body filter only from a `Body` task, so a
/// response ending in `Trailer` -> `Done` delivered no end-of-stream and a
/// withholding processor dropped the entire body.
#[tokio::test]
async fn custom_trailered_response_releases_the_withheld_body() {
    let harness = init();
    let expected: Vec<u8> = CUSTOM_TRAILERED_CHUNKS.concat();
    let response = reqwest::Client::new()
        .get(format!(
            "{}{CUSTOM_TRAILERED_PATH}",
            harness.custom_base_url()
        ))
        .header(CUSTOM_TRAILERED_HEADER.0, CUSTOM_TRAILERED_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.text().await.unwrap(),
        format!("{}|eos", String::from_utf8(expected).unwrap())
    );
}

/// `Trailer` claims the termination; the `Done` behind it must not dispatch a
/// second callback, which would append a second `|eos` marker.
#[tokio::test]
async fn custom_trailered_response_dispatches_the_terminal_callback_once() {
    let harness = init();
    let body = reqwest::Client::new()
        .get(format!(
            "{}{CUSTOM_TRAILERED_PATH}",
            harness.custom_base_url()
        ))
        .header(CUSTOM_TRAILERED_HEADER.0, CUSTOM_TRAILERED_HEADER.1)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body.matches("|eos").count(), 1, "body was {body:?}");
}

/// A cleanly closed upgrade may yield no `UpgradedBody` task at all. Bytes
/// synthesized by the terminal `Done` callback must still use the upgraded
/// body variant; a plain `Body` would panic in the H1 downstream writer after
/// the 101 handshake has switched it to raw duplex mode.
#[tokio::test]
async fn custom_empty_upgrade_tags_terminal_output_as_upgraded_body() {
    let harness = init();
    let mut io = TcpStream::connect(("127.0.0.1", harness.custom_proxy_port))
        .await
        .unwrap();
    io.write_all(
        format!(
            "GET {CUSTOM_EMPTY_UPGRADE_PATH} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n{}: {}\r\n\r\n",
            CUSTOM_EMPTY_UPGRADE_HEADER.0, CUSTOM_EMPTY_UPGRADE_HEADER.1
        )
        .as_bytes(),
    )
    .await
    .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(2), async {
        let mut response = Vec::new();
        let mut chunk = [0; 1024];
        while !response.ends_with(b"generated-extra") {
            let read = io.read(&mut chunk).await.unwrap();
            assert_ne!(
                read, 0,
                "empty upgraded response closed before terminal output"
            );
            response.extend_from_slice(&chunk[..read]);
        }
        response
    })
    .await
    .expect("empty upgraded response did not arrive");
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 101"), "{response}");
    assert!(response.ends_with("generated-extra"), "{response}");
}

fn terminal_upgrade_eos_calls(path: &str) -> usize {
    TERMINAL_UPGRADE_EOS_CALLS
        .lock()
        .unwrap()
        .get(path)
        .copied()
        .unwrap_or(0)
}

/// Drive one request against the scripted `Header(101, true)` session and
/// return everything the proxy wrote back, reading until the connection closes.
///
/// A raw socket rather than an HTTP client: after a 101 the downstream session
/// switches to raw duplex mode, and the point of these tests is what lands on
/// the wire byte for byte.
async fn terminal_upgrade_response(path: &str, upgrade_request: bool) -> String {
    let harness = init();
    let mut io = TcpStream::connect(("127.0.0.1", harness.custom_proxy_port))
        .await
        .unwrap();
    let upgrade_headers = if upgrade_request {
        "Connection: Upgrade\r\nUpgrade: websocket\r\n"
    } else {
        ""
    };
    io.write_all(
        format!(
            "GET {path} HTTP/1.1\r\nHost: localhost\r\n{upgrade_headers}{}: {}\r\n\r\n",
            CUSTOM_TERMINAL_UPGRADE_HEADER.0, CUSTOM_TERMINAL_UPGRADE_HEADER.1
        )
        .as_bytes(),
    )
    .await
    .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), async {
        let mut response = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            let read = io.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            response.extend_from_slice(&chunk[..read]);
            // Raw upgraded writes end at the generated bytes; an ordinary
            // chunked response ends at its terminating chunk.
            if response.ends_with(b"generated-extra") || response.ends_with(b"0\r\n\r\n") {
                break;
            }
        }
        response
    })
    .await
    .expect("terminal upgrade response did not arrive");
    String::from_utf8(response).unwrap()
}

/// Concatenate the payloads of a chunked body, so an assertion on the bytes
/// does not depend on how many tasks the writer happened to coalesce.
fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    loop {
        let (size, tail) = rest
            .split_once("\r\n")
            .unwrap_or_else(|| panic!("truncated chunk size in {body:?}"));
        let size = usize::from_str_radix(size.trim(), 16)
            .unwrap_or_else(|e| panic!("bad chunk size {size:?} in {body:?}: {e}"));
        if size == 0 {
            return out;
        }
        out.push_str(&tail[..size]);
        rest = &tail[size + 2..];
    }
}

/// The defect: a custom connector may report a completed Upgrade as a single
/// `Header(101, true)`. That header used to miss the `terminal_header` branch
/// (101 satisfies `StatusCode::is_informational()`) while still claiming the
/// terminal-dispatch latch, so the response body hook never saw end-of-stream
/// and the generic header-EOS drain appended a plain `Body(None, true)` behind
/// the 101 -- which panics the H1 writer once the handshake has switched it to
/// raw duplex mode.
///
/// The arrival of the terminal bytes is itself the no-panic assertion: a plain
/// `Body` task after the 101 aborts the connection task in
/// `buffer_body_data`, so a regression yields a closed connection and an empty
/// read rather than a wrong string.
#[tokio::test]
async fn custom_terminal_upgrade_header_dispatches_eos_without_a_plain_body() {
    let response = terminal_upgrade_response(CUSTOM_TERMINAL_UPGRADE_PATH, true).await;

    assert!(response.starts_with("HTTP/1.1 101"), "{response}");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("no header terminator in {response:?}"));
    // Raw upgraded write path: the generated bytes reach the wire verbatim,
    // with none of the chunked framing a plain body task would have added.
    assert_eq!(body, "generated-extra", "{response}");
    assert!(
        !head.to_ascii_lowercase().contains("transfer-encoding"),
        "{response}"
    );
    assert_eq!(
        terminal_upgrade_eos_calls(CUSTOM_TERMINAL_UPGRADE_PATH),
        1,
        "terminal body hook must observe end-of-stream exactly once"
    );
    assert_no_terminal_upgrade_read_after_eos();
}

/// A naked upstream 101 -- the downstream request never asked to upgrade -- must
/// not mark the downstream response upgraded. The terminal bytes then travel as
/// ordinary body, and tagging them `UpgradedBody` would panic the H1 writer from
/// the other side of the same invariant.
#[tokio::test]
async fn custom_naked_terminal_upgrade_header_does_not_upgrade_downstream() {
    let response = terminal_upgrade_response(CUSTOM_NAKED_TERMINAL_UPGRADE_PATH, false).await;

    assert!(response.starts_with("HTTP/1.1 101"), "{response}");
    assert!(response.ends_with("generated-extra"), "{response}");
    assert_eq!(
        terminal_upgrade_eos_calls(CUSTOM_NAKED_TERMINAL_UPGRADE_PATH),
        1,
        "terminal body hook must observe end-of-stream exactly once"
    );
    assert_no_terminal_upgrade_read_after_eos();
}

/// `response_filter` rewriting the 101 to a non-upgrade status makes the
/// FILTERED header authoritative: no downstream handshake happens, so the
/// terminal bytes must be emitted as ordinary body and framed normally.
#[tokio::test]
async fn custom_rewritten_terminal_upgrade_header_does_not_upgrade_downstream() {
    let response = terminal_upgrade_response(CUSTOM_REWRITTEN_TERMINAL_UPGRADE_PATH, true).await;

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("no header terminator in {response:?}"));
    assert!(
        head.to_ascii_lowercase().contains("transfer-encoding"),
        "{response}"
    );
    assert_eq!(dechunk(body), "generated-extra", "{response}");
    assert_eq!(
        terminal_upgrade_eos_calls(CUSTOM_REWRITTEN_TERMINAL_UPGRADE_PATH),
        1,
        "terminal body hook must observe end-of-stream exactly once"
    );
    assert_no_terminal_upgrade_read_after_eos();
}

#[tokio::test]
async fn custom_header_eos_honors_body_hook_delay() {
    let harness = init();
    let start = std::time::Instant::now();
    let response = reqwest::get(format!("{}/bodyless_delay", harness.custom_base_url()))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.text().await.unwrap(), "generated-extra");
    assert!(
        start.elapsed() >= Duration::from_millis(40),
        "custom response ignored the body hook delay"
    );
}

#[tokio::test]
async fn custom_post_terminal_failure_keeps_downstream_and_drops_upstream() {
    let harness = init();
    FAILED_CUSTOM_SESSION_RELEASED.store(false, Ordering::SeqCst);
    CUSTOM_POST_TERMINAL_FAILURE_EMITTED.store(false, Ordering::SeqCst);
    let mut io = TcpStream::connect(("127.0.0.1", harness.custom_proxy_port))
        .await
        .unwrap();

    for path in ["/custom_post_terminal_failure", "/bodyless"] {
        let failure_header = if path == "/custom_post_terminal_failure" {
            format!(
                "{}: {}\r\n",
                CUSTOM_POST_TERMINAL_FAILURE_HEADER.0, CUSTOM_POST_TERMINAL_FAILURE_HEADER.1
            )
        } else {
            String::new()
        };
        io.write_all(
            format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\n{failure_header}Connection: keep-alive\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let response = read_http1_response(&mut io).await;
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        if path == "/bodyless" {
            assert!(response.contains("generated-extra"), "{response}");
        } else {
            assert!(response.ends_with("0\r\n\r\n"), "{response}");
        }
    }

    assert!(
        CUSTOM_POST_TERMINAL_FAILURE_EMITTED.load(Ordering::SeqCst),
        "the custom fixture did not emit its post-terminal failure"
    );
    assert!(
        !FAILED_CUSTOM_SESSION_RELEASED.load(Ordering::SeqCst),
        "the failed custom upstream session was returned to the connector"
    );
}

#[tokio::test]
async fn custom_pre_write_rejection_releases_pristine_upstream() {
    let harness = init();
    PRE_WRITE_CUSTOM_SESSION_RELEASED.store(false, Ordering::SeqCst);

    let response = reqwest::get(format!(
        "{}/custom_pre_write_rejection",
        harness.custom_base_url()
    ))
    .await
    .unwrap();

    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(
        PRE_WRITE_CUSTOM_SESSION_RELEASED.load(Ordering::SeqCst),
        "the pristine custom upstream session was not returned to the connector"
    );
}

#[tokio::test]
async fn method_and_status_body_prohibitions_discard_synthetic_output() {
    let harness = init();
    let client = reqwest::Client::new();

    let head = client
        .head(format!("{}/bodyless_head", harness.base_url()))
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(head.bytes().await.unwrap().len(), 0);

    let late_no_content = client
        .get(format!("{}/bodyless_late_204", harness.base_url()))
        .header(BODYLESS_ORIGIN_HEADER.0, BODYLESS_ORIGIN_HEADER.1)
        .send()
        .await
        .unwrap();
    assert_eq!(late_no_content.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(!late_no_content
        .headers()
        .contains_key(reqwest::header::TRANSFER_ENCODING));
    assert_eq!(late_no_content.bytes().await.unwrap().len(), 0);

    for (path, marker, expected) in [
        (
            "/bodyless_204",
            BODYLESS_NO_CONTENT_HEADER,
            reqwest::StatusCode::NO_CONTENT,
        ),
        (
            "/bodyless_304",
            BODYLESS_NOT_MODIFIED_HEADER,
            reqwest::StatusCode::NOT_MODIFIED,
        ),
    ] {
        let response = client
            .get(format!("{}{path}", harness.base_url()))
            .header(marker.0, marker.1)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(response.bytes().await.unwrap().len(), 0);
    }
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
