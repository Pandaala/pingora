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

//! HTTP/2 server session

use bytes::Bytes;
use futures::Future;
use h2::server;
use h2::server::SendResponse;
use h2::{RecvStream, SendStream};
use http::header::HeaderName;
use http::uri::PathAndQuery;
use http::{header, HeaderMap, Response, StatusCode};
use log::{debug, warn};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_timeout::timeout;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::ready;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

use crate::protocols::http::body_buffer::{
    FixedBuffer, RegisteredRequestBodyBuffer, RequestBodyBuffer,
};
use crate::protocols::http::date::get_cached_date;
use crate::protocols::http::v1::client::{
    http_req_header_to_wire, request_target_has_forbidden_byte,
};
use crate::protocols::http::v1::common::validate_content_length_without_transfer_encoding;
use crate::protocols::http::HttpTask;
use crate::protocols::{Digest, SocketAddr, Stream};
use crate::server::ShutdownWatch;
use crate::{Error, ErrorType, OrErr, Result};

const BODY_BUF_LIMIT: usize = 1024 * 64;

/// The default downstream request-body read timeout, matching the HTTP/1
/// server session's own default (`v1::server::HttpSession::new`).
///
/// A default is what makes the bound real: nothing in `pingora-core` or
/// `pingora-proxy` calls [`HttpSession::set_read_timeout`], so an opt-in bound
/// would leave every consumer that does not set one -- including pingora's own
/// server apps -- with an unbounded H2 body read, while the same application
/// over HTTP/1 is protected. Applications with legitimately idle uploads
/// (long-idle client-streaming gRPC) raise or clear it per request with
/// [`HttpSession::set_read_timeout`]; CONNECT tunnels are exempt by
/// construction, see [`HttpSession::body_read_timeout`].
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);

type H2Connection<S> = server::Connection<S, Bytes>;

pub use h2::server::Builder as H2Options;

// 64 KiB decoded header-list limit.
const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 64 * 1024;
const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 100;

// Per-connection lifetime budget for selected malformed downstream requests
// rejected during acceptance (currently ambiguous Content-Length framing and
// conflicting authority/Host fields). The count is NOT reset by valid streams,
// so a client cannot evade the bound by interleaving valid requests. A
// well-behaved client never sends these requests, so this is never tripped in
// practice; it bounds the total rejection work a misbehaving or malicious
// client can drive over the life of a single connection.
// TODO: expose this through HTTP/2 server configuration if deployments need a
// different tolerance for malformed stream rejections.
const MAX_MALFORMED_STREAMS_PER_CONN: usize = 32;

/// Build [`H2Options`] with bounded defaults for received requests.
///
/// Use this as the starting point when customizing options to retain the default
/// decoded header-list and concurrent-stream limits.
pub fn default_h2_options() -> H2Options {
    let mut options = H2Options::default();
    options.max_header_list_size(DEFAULT_MAX_HEADER_LIST_SIZE);
    options.max_concurrent_streams(DEFAULT_MAX_CONCURRENT_STREAMS);
    options
}

/// Perform HTTP/2 connection handshake with an established (TLS) connection.
///
/// The optional `options` allow to adjust certain HTTP/2 parameters and settings.
/// When `options` is [`None`], bounded defaults from [`default_h2_options`] are
/// used. See [`H2Options`] for more details.
pub async fn handshake(io: Stream, options: Option<H2Options>) -> Result<H2Connection<Stream>> {
    let options = options.unwrap_or_else(default_h2_options);
    let res = options.handshake(io).await;

    match res {
        Ok(connection) => {
            debug!("H2 handshake done.");
            Ok(connection)
        }
        Err(e) => Error::e_because(
            ErrorType::HandshakeError,
            "while h2 handshaking with client",
            e,
        ),
    }
}

/// Drive a server-side HTTP/2 connection's accept loop, dispatching each new
/// stream to `on_session` until the connection closes.
///
/// This loop ends when:
///   * the client closes the H2 connection cleanly ([`HttpSession::from_h2_conn`]
///     returns `Ok(None)` after the final GOAWAY is flushed),
///   * the codec hits a connection error, or
///   * the configured idle timeout expires while no streams are active, or
///   * the runtime-level `graceful_shutdown_timeout_seconds` ceiling fires and
///     force-kills the task driving this future.
///
/// On a shutdown signal:
///   1. [`h2::server::Connection::graceful_shutdown`] is called, which
///      enqueues a GOAWAY with the maximum possible last_stream_id per
///      RFC 9113 §6.8. The codec emits a second, real GOAWAY when the
///      connection finishes draining.
///   2. The loop continues calling [`HttpSession::from_h2_conn`] so that:
///      - streams whose HEADERS were buffered in the codec before the shutdown
///        signal arrived are still surfaced and dispatched,
///      - streams the client opens after observing GOAWAY(MAX) but below the
///        eventual last_stream_id are also dispatched, and
///      - the codec is driven to completion so the final GOAWAY can be
///        flushed and the connection closed cleanly.
///
/// `on_session` is invoked once per accepted stream, together with a
/// [`StreamGuard`]. Typical callers spawn a task to process the session so the
/// accept loop is not blocked, and move the guard into that task so its lifetime
/// matches the session.
///
/// Note: this function does not impose its own per-connection drain timeout
/// (after a shutdown signal). The runtime-level `graceful_shutdown_timeout_seconds`
/// is the only ceiling there, so a slow client can keep this future alive up to
/// that bound during drain.
// TODO: add a per-connection drain timeout to bound how long a single
// misbehaving client can keep this task alive after GOAWAY.
pub(crate) async fn accept_downstream_sessions<F>(
    mut conn: H2Connection<Stream>,
    digest: Arc<Digest>,
    mut shutdown: ShutdownWatch,
    idle_timeout: Option<Duration>,
    mut on_session: F,
) where
    F: FnMut(HttpSession, StreamGuard),
{
    let mut shutdown_initiated = false;
    // Per-connection budget for malformed streams (see MAX_MALFORMED_STREAMS_PER_CONN).
    let mut malformed_streams = 0usize;
    // In-flight sessions, decremented by the `StreamGuard` given to `on_session`.
    let active = Arc::new(ActiveSessions::new());
    loop {
        let h2_stream = if shutdown_initiated {
            HttpSession::from_h2_conn_with_malformed_budget(
                &mut conn,
                digest.clone(),
                &mut malformed_streams,
            )
            .await
        } else {
            tokio::select! {
                // Poll the shutdown signal first so a concurrent signal is
                // observed deterministically. `from_h2_conn` is cancel-safe
                // and is polled again on the next iteration.
                biased;
                _ = shutdown.changed() => {
                    conn.graceful_shutdown();
                    shutdown_initiated = true;
                    continue;
                }
                h2_stream = HttpSession::from_h2_conn_with_malformed_budget(
                    &mut conn,
                    digest.clone(),
                    &mut malformed_streams,
                ) => h2_stream,
                // Any accepted stream cancels this future. The next iteration
                // waits for all active streams to finish before starting a fresh
                // idle period.
                _ = wait_for_idle_timeout(&active, idle_timeout.unwrap_or_default()), if idle_timeout.is_some() => {
                    // Idle with nothing in flight: drop `conn` to close the
                    // socket now (no graceful GOAWAY wait that could hang on
                    // a dead peer).
                    return;
                }
            }
        };
        match h2_stream {
            Err(e) => {
                // It is common for the client to just disconnect TCP without
                // properly closing H2. So we don't log the errors here
                debug!("H2 error when accepting new stream {e}");
                return;
            }
            // None means the connection is ready to be closed
            Ok(None) => return,
            // The offending stream was already answered or reset; keep the
            // connection alive and continue accepting sibling streams.
            Ok(Some(H2Accept::Rejected)) => continue,
            Ok(Some(H2Accept::Session(session))) => {
                on_session(session, active.start_session());
            }
        }
    }
}

/// Tracks one in-flight downstream H2 session for [`accept_downstream_sessions`].
/// `on_session` receives it alongside each session; keep it alive for as long as
/// the session is being processed (e.g. move it into the spawned task) so the
/// accept loop's idle timeout can tell a busy connection from an idle one. It
/// decrements the in-flight counter when dropped.
pub(crate) struct StreamGuard(Arc<ActiveSessions>);

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.idle.notify_one();
        }
    }
}

struct ActiveSessions {
    count: AtomicUsize,
    idle: Notify,
}

impl ActiveSessions {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }

    fn start_session(self: &Arc<Self>) -> StreamGuard {
        self.count.fetch_add(1, Ordering::Relaxed);
        StreamGuard(self.clone())
    }

    async fn wait_until_idle(&self) {
        while self.count.load(Ordering::Relaxed) != 0 {
            self.idle.notified().await;
        }
    }
}

async fn wait_for_idle_timeout(active: &ActiveSessions, idle_timeout: Duration) {
    active.wait_until_idle().await;
    if !idle_timeout.is_zero() {
        pingora_timeout::sleep(idle_timeout).await;
    }
}

use futures::task::Context;
use futures::task::Poll;
use std::pin::Pin;
/// The future to poll for an idle session.
///
/// Calling `.await` in this object will not return until the client decides to close this stream.
pub struct Idle<'a>(&'a mut HttpSession);

impl Future for Idle<'_> {
    type Output = Result<h2::Reason>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(body_writer) = self.0.send_response_body.as_mut() {
            body_writer.poll_reset(cx)
        } else {
            self.0.send_response.poll_reset(cx)
        }
        .map_err(|e| Error::because(ErrorType::H2Error, "downstream error while idling", e))
    }
}

/// Whether an `h2` error observed *after* the request body already reached EOF
/// is a benign way for the client to end the stream rather than a read failure.
///
/// The request is complete at this point, so none of these mean the bytes we
/// read are suspect:
/// - GOAWAY with `NO_ERROR`: graceful connection shutdown.
/// - RST_STREAM with `NO_ERROR`: RFC 9113 §8.1's "stop sending the request
///   body without breaking the response" signal, sent by a client that has
///   already finished its body.
/// - RST_STREAM with `CANCEL`: the client no longer wants the response (a
///   browser navigating away is the common case). Surfacing it as a read error
///   would fail the request AND -- because both proxy pumps convert it with
///   `into_down()` -- close and re-dial an otherwise healthy pooled upstream
///   connection, once per client cancel. The upstream request has already been
///   sent by the time this runs, so nothing is saved by failing hard here; a
///   later write to the cancelled stream still fails and is classified on its
///   own merits.
///
/// Mirrors the client-side classification in
/// [`crate::protocols::http::v2::client::Http2Session::read_trailers`].
fn benign_post_eof_stream_end(e: &h2::Error) -> bool {
    (e.is_go_away() || e.is_reset())
        && e.is_remote()
        && matches!(
            e.reason(),
            Some(h2::Reason::NO_ERROR) | Some(h2::Reason::CANCEL)
        )
}

/// The non-zero `content-length` a message declares, if any.
///
/// `0` is deliberately mapped to `None`: on HTTP/2 `content-length: 0` promises
/// zero DATA payload bytes but says nothing about END_STREAM, so a request that
/// declares it may still legitimately close its stream later (design 4.3). Were
/// it treated as "fully received" the moment the message is created, every such
/// request would be classified as complete before the transport ever said so,
/// which is exactly the distinction the rest of this module preserves.
pub(crate) fn declared_body_length(headers: &http::HeaderMap) -> Option<usize> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        // Digits only, staying at least as strict as h2's own `parse_u64`,
        // which rejects `+5` and surrounding whitespace and kills such a stream
        // with PROTOCOL_ERROR before it reaches here. `usize::parse` is laxer,
        // so reject any non-digit byte first. An empty value has no digits and
        // yields `None`, exactly as before.
        .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|len| *len > 0)
}

/// HTTP/2 server session
pub struct HttpSession {
    request_header: RequestHeader,
    request_body_reader: RecvStream,
    request_headers_end_stream: bool,
    send_response: SendResponse<Bytes>,
    send_response_body: Option<SendStream<Bytes>>,
    // Remember what has been written
    response_written: Option<Box<ResponseHeader>>,
    // Indicate that whether a END_STREAM is already sent
    // in order to tell whether needs to send one extra FRAME when this response finishes
    ended: bool,
    // How many (application, not wire) request body bytes have been read so far.
    body_read: usize,
    // How many (application, not wire) response body bytes have been sent so far.
    body_sent: usize,
    // buffered request body for retry logic
    retry_buffer: Option<FixedBuffer>,
    // early body capture hook for upstream replay
    early_body_buffer: Option<RegisteredRequestBodyBuffer>,
    // Set right before awaiting an `early_body_buffer` capture/finish and cleared
    // once the await returns Ok. A body-read future dropped mid-await
    // (select!/timeout cancellation) leaves it set: the chunk was already
    // consumed from the stream, so the buffered body is incomplete and any
    // further body read or replay must fail closed.
    early_body_capture_poisoned: bool,
    // Set when `drain_request_body` discards a registered `early_body_buffer`.
    // The body bytes are gone from both the stream and the buffer, so a later
    // replay attempt must fail closed instead of silently forwarding a bodyless
    // request upstream.
    early_body_buffer_discarded: bool,
    // Set when the session drops a fully replayed `early_body_buffer` after the
    // response was committed downstream (no further retry is possible then).
    // A later replay attempt indicates a broken retry decision upstream of this
    // session and must fail closed instead of silently forwarding a bodyless
    // request.
    early_body_buffer_released: bool,
    // Whether actual request trailer fields were received. Set once the
    // request body reader reaches EOF and its trailers are polled.
    request_trailers_present: bool,
    // Latched once the TRANSPORT said the request body ended: END_STREAM on the
    // HEADERS frame, or `RecvStream::is_end_stream()` observed at any body poll.
    //
    // It is latched rather than recomputed because `h2` destroys the fact: a
    // RST_STREAM received after END_STREAM overwrites the stream state with
    // `Closed(Cause::Error(..))`, after which `is_end_stream()` reports `false`
    // again while the END_STREAM-bearing DATA frame is still queued for
    // delivery. Recomputing would therefore be non-monotonic -- a fact the
    // peer can retract -- and every consumer of "is the request body done"
    // would follow it back to `false`.
    request_body_eof: bool,
    // Source (iii) of the same end-of-body proof: a declared `content-length`
    // that has been fully received. Stored as the declared length (only when
    // non-zero, see `request_body_length_satisfied`) and compared against
    // `body_read`.
    //
    // This is the source that covers the NATURAL wire ordering: a peer that
    // flushes DATA(END_STREAM) and then RST_STREAM before we ever poll destroys
    // the `is_end_stream()` evidence before we can latch it, so sources (i) and
    // (ii) never fire and only the byte count can still prove the body is
    // whole.
    request_body_declared_len: Option<usize>,
    // Whether `trailers()` has already been awaited for this request. The
    // post-EOF branch of `read_body_bytes()` can be reached more than once
    // (e.g. an idling pump keeps polling); re-awaiting would report `None`
    // the second time and clear an already-established trailer fact.
    trailers_polled: bool,
    // digest to record underlying connection info
    digest: Arc<Digest>,
    /// The write timeout which will be applied to writing response body.
    /// The timeout is reset on every write. This is not a timeout on the overall duration of the
    /// response.
    pub write_timeout: Option<Duration>,
    // The read timeout applied to each downstream request-body read. The
    // timeout is reset on every received chunk, so it bounds a stalled upload
    // without limiting the overall duration of a progressing one (the same
    // per-read semantics as the HTTP/1 `read_timeout`).
    read_timeout: Option<Duration>,
    // Absolute deadline of the CURRENT request-body read, i.e. "the client has
    // been silent since this instant plus `read_timeout`".
    //
    // The bound is a DEADLINE and not a per-call duration on purpose. Every
    // caller of `read_body_bytes` in this repo polls it from a `tokio::select!`
    // branch (see `proxy_h1::bidirection_1to2` / `proxy_h2::bidirection_down_to_up`),
    // where any OTHER branch becoming ready -- an upstream response chunk, a
    // cache task, a custom message -- completes the select and DROPS this
    // future. A duration recomputed per call would restart from zero on the
    // next loop iteration, so a client that stalls forever against an upstream
    // that speaks even once per timeout period would never be released: the
    // exact DoS this bound exists to close. Carrying the deadline on the
    // session makes cancellation unable to rearm it.
    //
    // Cleared ONLY by transport progress (a non-empty DATA payload, or
    // END_STREAM), never by a cancelled read. See `read_body_bytes`.
    read_deadline: Option<Instant>,
    // How long to wait when draining (discarding) request body
    total_drain_timeout: Option<Duration>,
}

/// The outcome of accepting the next event on an HTTP/2 downstream connection.
///
/// Returned by [`HttpSession::from_h2_conn`] so the accept loop can react to a
/// rejected stream without tearing down the whole connection.
///
/// The session is stored inline to avoid a heap allocation for every accepted
/// HTTP/2 stream.
#[allow(clippy::large_enum_variant)]
pub enum H2Accept {
    /// A new request stream was established and is ready to be served.
    Session(HttpSession),
    /// The next stream was rejected during acceptance (for example, its request
    /// target contained a forbidden byte) and has already been answered or
    /// reset. Sibling streams and the connection are unaffected; the caller
    /// should continue accepting.
    Rejected,
}

// The replay tests below predate `H2Accept` and only create valid requests.
// Keep their focus on body-state behavior while still exercising the current
// accept API; an unexpected rejected stream fails loudly instead of being
// treated as a session.
#[cfg(test)]
impl std::ops::Deref for H2Accept {
    type Target = HttpSession;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Session(session) => session,
            Self::Rejected => panic!("test request was unexpectedly rejected"),
        }
    }
}

#[cfg(test)]
impl std::ops::DerefMut for H2Accept {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Session(session) => session,
            Self::Rejected => panic!("test request was unexpectedly rejected"),
        }
    }
}

fn authority_host_mismatch(request: &RequestHeader) -> bool {
    let Some(authority) = request.uri.authority() else {
        return false;
    };

    let mut hosts = request.headers.get_all(header::HOST).iter();
    match (hosts.next(), hosts.next()) {
        (Some(host), None) => host.as_bytes() != authority.as_str().as_bytes(),
        (Some(_), Some(_)) => true,
        (None, _) => false,
    }
}

fn account_malformed_stream(malformed_streams: &mut usize) -> Result<()> {
    *malformed_streams += 1;
    if *malformed_streams >= MAX_MALFORMED_STREAMS_PER_CONN {
        // Rare, connection-level abuse signal (at most once per torn-down
        // connection), so warn! is flood-safe here and useful for detecting
        // abuse in production.
        warn!(
            "tearing down downstream h2 connection after \
             {malformed_streams} malformed requests"
        );
        return Error::e_explain(
            ErrorType::H2Error,
            "too many malformed downstream requests on connection",
        );
    }
    Ok(())
}

impl HttpSession {
    /// Create a new [`HttpSession`] from the HTTP/2 connection.
    /// This function returns a new HTTP/2 session when the provided HTTP/2 connection, `conn`,
    /// establishes a new HTTP/2 stream to this server.
    ///
    /// A [`Digest`] from the IO stream is also stored in the resulting session, since the
    /// session doesn't have access to the underlying stream (and the stream itself isn't
    /// accessible from the `h2::server::Connection`).
    ///
    /// Note: in order to handle all **existing** and new HTTP/2 sessions, the server must call
    /// this function in a loop until the client decides to close the connection.
    ///
    /// The return value distinguishes three outcomes:
    /// * `Ok(Some(`[`H2Accept::Session`]`))` — a new stream is ready to serve.
    /// * `Ok(Some(`[`H2Accept::Rejected`]`))` — the stream was answered or reset
    ///   during acceptance; the caller should keep accepting sibling streams.
    /// * `Ok(None)` — the connection is closing, so the loop can exit.
    ///
    /// This convenience wrapper uses a fresh malformed-stream counter on every
    /// call. It preserves the public API, but does not enforce the
    /// [`MAX_MALFORMED_STREAMS_PER_CONN`] budget across repeated calls by an
    /// external accept loop. Pingora's built-in downstream accept loop uses the
    /// internal budgeted helper to share one counter for the connection lifetime.
    pub async fn from_h2_conn(
        conn: &mut H2Connection<Stream>,
        digest: Arc<Digest>,
    ) -> Result<Option<H2Accept>> {
        let mut malformed_streams = 0usize;
        Self::from_h2_conn_with_malformed_budget(conn, digest, &mut malformed_streams).await
    }

    /// Like [`Self::from_h2_conn`], but shares a malformed-stream counter across
    /// calls for the same connection.
    ///
    /// `malformed_streams` is a per-connection counter, owned by the caller and
    /// shared across every call for the same connection. It tracks the total
    /// number of malformed streams counted by these acceptance checks over the
    /// connection's lifetime. Valid streams do not reset it, so a client cannot
    /// evade the [`MAX_MALFORMED_STREAMS_PER_CONN`] bound by interleaving valid
    /// requests.
    /// Callers should initialize it to `0` once per connection.
    async fn from_h2_conn_with_malformed_budget(
        conn: &mut H2Connection<Stream>,
        digest: Arc<Digest>,
        malformed_streams: &mut usize,
    ) -> Result<Option<H2Accept>> {
        // NOTE: conn.accept().await is what drives the entire connection.
        let res = conn.accept().await.transpose().or_err(
            ErrorType::H2Error,
            "while accepting new downstream requests",
        )?;

        let Some((req, mut send_response)) = res else {
            return Ok(None);
        };

        let (request_header, request_body_reader) = req.into_parts();
        let request_headers_end_stream = request_body_reader.is_end_stream();
        let request_body_declared_len = declared_body_length(&request_header.headers);
        let request_header: RequestHeader = request_header.into();

        // Depending on how the request URI is parsed, control bytes
        // (including CR and LF) may be accepted in the `:path`
        // pseudo-header. Reject them here as defense-in-depth: these
        // bytes are not permitted in a URI, and they would be dangerous
        // if forwarded to an HTTP/1.1 upstream. Reset only the offending
        // stream so sibling streams on the connection are unaffected.
        if request_target_has_forbidden_byte(request_header.raw_path()) {
            debug!("Rejecting H2 request: forbidden delimiter byte in request target");
            send_response.send_reset(h2::Reason::PROTOCOL_ERROR);
            return Ok(Some(H2Accept::Rejected));
        }

        // Reject ambiguous Content-Length framing at the stream level.
        // Identical duplicates and comma-combined identical values are
        // reconciled (RFC 9110 section 8.6), but conflicting or unparseable
        // values are an unrecoverable error and are treated as a stream
        // error per RFC 9113 section 8.1.1. This keeps the request path
        // consistent with HTTP/1 and prevents ambiguous framing from being
        // forwarded (e.g. when downgraded to an HTTP/1 upstream).
        if let Err(e) = validate_content_length_without_transfer_encoding(&request_header.headers) {
            // debug, not warn: per-stream, client-influenced; avoids log
            // floods. Connection-level abuse is surfaced via the error below.
            debug!("rejecting downstream h2 request: {e}");
            send_response.send_reset(h2::Reason::PROTOCOL_ERROR);

            account_malformed_stream(malformed_streams)?;
            return Ok(Some(H2Accept::Rejected));
        }

        if authority_host_mismatch(&request_header) {
            // RFC 9113 section 8.3.1 says a server SHOULD treat a request as
            // malformed when Host does not match :authority after
            // normalization. Until shared authority normalization exists,
            // conservatively require identical field values:
            // https://www.rfc-editor.org/rfc/rfc9113.html#section-8.3.1
            //
            // RFC 9112 section 3.2 requires HTTP/1.1 servers to reject more
            // than one Host field. When :authority is present, reject
            // duplicates that cannot be compared unambiguously before a
            // possible H1 downgrade:
            // https://www.rfc-editor.org/rfc/rfc9112.html#section-3.2
            debug!("rejecting downstream h2 request: conflicting :authority and Host fields");
            let mut response = Response::new(());
            *response.status_mut() = StatusCode::BAD_REQUEST;
            // RFC 9113 section 8.1.1 requires malformed requests to use a
            // PROTOCOL_ERROR stream error and permits sending an HTTP response
            // first. h2 replaces the queued response when reset immediately,
            // so prioritize an observable 400. An unfinished request body can
            // still cause h2 to reset the stream when its handles are dropped:
            // https://www.rfc-editor.org/rfc/rfc9113.html#section-8.1.1
            if let Err(e) = send_response.send_response(response, true) {
                // The client can reset this stream before the rejection is
                // written. Keep that stream-local failure from closing the
                // connection and dropping sibling streams.
                debug!("failed to send downstream h2 authority rejection: {e}");
            }
            account_malformed_stream(malformed_streams)?;
            return Ok(Some(H2Accept::Rejected));
        }

        Ok(Some(H2Accept::Session(HttpSession {
            request_header,
            request_body_reader,
            send_response,
            send_response_body: None,
            response_written: None,
            ended: false,
            body_read: 0,
            body_sent: 0,
            retry_buffer: None,
            early_body_buffer: None,
            early_body_capture_poisoned: false,
            early_body_buffer_discarded: false,
            early_body_buffer_released: false,
            request_trailers_present: false,
            request_headers_end_stream,
            request_body_eof: request_headers_end_stream,
            request_body_declared_len,
            trailers_polled: false,
            digest,
            write_timeout: None,
            read_timeout: Some(DEFAULT_READ_TIMEOUT),
            read_deadline: None,
            total_drain_timeout: None,
        })))
    }

    /// The request sent from the client
    ///
    /// Different from its HTTP/1.X counterpart, this function never panics as the request is already
    /// read when established a new HTTP/2 stream.
    pub fn req_header(&self) -> &RequestHeader {
        &self.request_header
    }

    /// A mutable reference to request sent from the client
    ///
    /// Different from its HTTP/1.X counterpart, this function never panics as the request is already
    /// read when established a new HTTP/2 stream.
    pub fn req_header_mut(&mut self) -> &mut RequestHeader {
        &mut self.request_header
    }

    /// Whether the request body is provably complete, from ANY source the peer
    /// cannot retract.
    ///
    /// This is the guard on treating a stream end as benign rather than as a
    /// truncated read, and it is deliberately broader than
    /// [`Self::is_body_done`]:
    /// - (i) END_STREAM on the HEADERS frame and (ii) `is_end_stream()` observed
    ///   at a body poll are both latched into `request_body_eof`;
    /// - (iii) a declared, non-zero `content-length` whose bytes have all been
    ///   read is computed here.
    ///
    /// Source (iii) is what covers the natural wire ordering -- peer writes
    /// DATA(END_STREAM) then RST_STREAM, and we poll only afterwards -- in which
    /// `h2` hands over the final chunk with the END_STREAM evidence already
    /// overwritten by the reset, so (i) and (ii) can never fire.
    ///
    /// RESIDUAL GAP, deliberate and load-bearing: a request that declares NO
    /// `content-length` (or declares `content-length: 0`) and whose peer resets
    /// the stream before we poll the final DATA frame has no surviving proof
    /// that its body is whole, so the reset still surfaces as a read error. That
    /// is the correct failure direction -- the alternative would be to guess,
    /// and a wrong guess forwards a TRUNCATED request body upstream as if it
    /// were complete. Do NOT "fix" this by dropping the guard and classifying
    /// every benign-looking reset as EOF; the negative-direction tests
    /// (`test_mid_body_reset_is_still_a_read_error`) exist to keep that from
    /// happening quietly.
    fn request_body_complete(&self) -> bool {
        self.request_body_eof
            || self
                .request_body_declared_len
                .is_some_and(|len| self.body_read >= len)
    }

    /// The idle bound that applies to the next request-body chunk read, or
    /// `None` when this session's body reads are deliberately unbounded.
    ///
    /// CONNECT is exempt. For a tunnel the "request body" IS the client-to-peer
    /// uplink, and a long idle period on it is ordinary rather than abusive: an
    /// idle SSH session, or a WebSocket over extended CONNECT (RFC 8441, which
    /// also uses `:method = CONNECT`, so this one check covers both) whose
    /// traffic happens to run server-to-client only. Applying the bound would
    /// tear such tunnels down at exactly the moment they are behaving
    /// correctly. The same check guards the request-body buffer registrations
    /// below.
    fn body_read_timeout(&self) -> Option<Duration> {
        if self.request_header.method == http::Method::CONNECT {
            return None;
        }
        self.read_timeout
    }

    /// Read request body bytes. `None` when there is no more body to read.
    pub async fn read_body_bytes(&mut self) -> Result<Option<Bytes>> {
        if self.early_body_capture_poisoned {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body capture failed or was cancelled mid-chunk; buffered body is incomplete",
            );
        }
        let polled = match self.body_read_timeout() {
            Some(t) => {
                // Resume the deadline of the read this one continues; only
                // transport progress clears it (see `read_deadline`). A read
                // whose deadline already passed still gets one poll: the data
                // may already be buffered, and answering a ready chunk with a
                // timeout would be wrong.
                let now = Instant::now();
                let deadline = *self.read_deadline.get_or_insert(now + t);
                let remaining = deadline.saturating_duration_since(now);
                match timeout(remaining, self.request_body_reader.data()).await {
                    Ok(data) => data,
                    Err(_) => {
                        // A body the TRANSPORT has promised is empty
                        // (`Content-Length: 0`) has no bytes left to lose, so
                        // there is nothing for this timeout to protect: the
                        // client owes us only an END_STREAM that H2 does not
                        // require it to have sent yet (design 4.3). Failing the
                        // exchange here would 4xx a request that is otherwise
                        // complete -- and would do it for an entirely ordinary
                        // client whose upstream simply had nothing more to say.
                        // Finish the read side instead; it is the same
                        // conclusion `proxy_common::downstream_body_read_is_futile`
                        // reaches for this shape, just reached from the read
                        // side and without waiting on the response.
                        //
                        // No trailer poll and no capture finish here, unlike
                        // the EOF branch below: the data section has NOT ended,
                        // so `trailers()` would park forever, and a registered
                        // capture buffer cannot coexist with an empty body
                        // (`set_request_body_buffer` rejects it, and
                        // `is_body_empty()` is false while one is registered).
                        if self.is_body_empty() {
                            debug!(
                                "downstream request body read timed out after {t:?} on a body \
                                 declared empty; finishing the read side instead of failing the \
                                 request"
                            );
                            self.request_body_eof = true;
                            return Ok(None);
                        }
                        return Error::e_explain(
                            ErrorType::ReadTimedout,
                            format!("while reading downstream request body, timeout: {t:?}"),
                        );
                    }
                }
            }
            None => self.request_body_reader.data().await,
        };
        let data = match polled.transpose() {
            Ok(data) => data,
            Err(e) => {
                // Once the request body is complete, a client ending the
                // stream is not a read failure -- see
                // `benign_post_eof_stream_end`. h2 surfaces the RST_STREAM
                // here rather than from `trailers()` when it arrives before
                // the EOF is polled, so the classification belongs on both.
                if self.request_body_complete() && benign_post_eof_stream_end(&e) {
                    None
                } else {
                    return Err(e).or_err(
                        ErrorType::ReadError,
                        "while reading downstream request body",
                    );
                }
            }
        };
        if let Some(data) = data.as_ref() {
            // Rearm the idle bound only on real progress. An EMPTY DATA frame
            // without END_STREAM is legal, costs the peer 9 bytes, consumes
            // NO flow-control window (h2 `proto::streams::recv::recv_data`
            // credits the window back for a zero-length payload) and trips no
            // flood counter, so treating it as progress would hand an attacker
            // a free, unlimited rearm -- and `body_read` never advances, so no
            // byte-count body-size limit catches it either. END_STREAM is
            // progress even with a zero-length payload: it ends the body.
            if !data.is_empty() || self.request_body_reader.is_end_stream() {
                self.read_deadline = None;
            }
            self.body_read += data.len();
            if let Some(buffer) = self.retry_buffer.as_mut() {
                buffer.write_to_buffer(data);
            }
            // Release flow control before the (cancellable) capture await: the
            // bytes are already consumed from the stream either way, and doing
            // it here keeps a cancelled capture from leaking this chunk's
            // flow-control window.
            let _ = self
                .request_body_reader
                .flow_control()
                .release_capacity(data.len());
            if let Some(buffer) = self.early_body_buffer.as_mut() {
                // Poison across the await: if this future is dropped mid-await
                // the flag stays set and the session fails closed from then on.
                self.early_body_capture_poisoned = true;
                buffer.capture(data).await?;
                self.early_body_capture_poisoned = false;
            }
            if self.request_body_reader.is_end_stream() {
                self.request_body_eof = true;
                if let Some(buffer) = self.early_body_buffer.as_mut() {
                    self.early_body_capture_poisoned = true;
                    buffer.finish_capture().await?;
                    self.early_body_capture_poisoned = false;
                }
            }
        } else {
            self.read_deadline = None;
            self.request_body_eof = true;
            // Establish the trailer fact exactly once: it is never cleared
            // again within this request.
            if !self.trailers_polled {
                // Latched BEFORE the await, which is safe only because the await
                // cannot be cancelled mid-poll here: `data()` returns `None`
                // (this `else` branch) exclusively after END_STREAM was
                // observed, at which point the trailers -- or the stream error
                // -- are already queued in h2, so `trailers()` is immediately
                // Ready and completes within this poll. Were it ever pending,
                // cancellation between this store and the await completing would
                // lose the trailer fact forever. Re-verify this invariant if the
                // h2 dependency is upgraded; if it no longer holds, latch after a
                // successful await instead (as `send_body_to2` in proxy_h2.rs
                // does for the same hazard).
                self.trailers_polled = true;
                let trailers = match self.request_body_reader.trailers().await {
                    Ok(trailers) => trailers,
                    Err(e) => {
                        if benign_post_eof_stream_end(&e) {
                            None
                        } else {
                            return Err(e).or_err(
                                ErrorType::ReadError,
                                "while reading downstream request trailers",
                            );
                        }
                    }
                };
                self.request_trailers_present = trailers.is_some_and(|fields| !fields.is_empty());
            }
            if let Some(buffer) = self.early_body_buffer.as_mut() {
                self.early_body_capture_poisoned = true;
                buffer.finish_capture().await?;
                self.early_body_capture_poisoned = false;
            }
        }
        Ok(data)
    }

    // A `RequestBodyBuffer::write` is async and cannot run in a poll context.
    // Fail closed when capture/replay is registered instead of silently returning
    // bytes that bypass the buffer.
    //
    // NOTE: this does NOT apply `read_timeout` -- it is a plain poll with no
    // timer of its own, so a caller that drives the request body through here
    // is responsible for its own idle bound (`read_body_bytes` is the bounded
    // entry point). There is no in-tree consumer today; add the bound here
    // before adding one.
    #[doc(hidden)]
    pub fn poll_read_body_bytes(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, h2::Error>>> {
        if self.early_body_buffer.is_some() {
            return Poll::Ready(Some(Err(h2::Reason::INTERNAL_ERROR.into())));
        }
        let data = match ready!(self.request_body_reader.poll_data(cx)).transpose() {
            Ok(data) => data,
            Err(err) => return Poll::Ready(Some(Err(err))),
        };

        if let Some(data) = data {
            self.body_read += data.len();
            self.request_body_reader
                .flow_control()
                .release_capacity(data.len())?;
            // Latch source (ii) here as well: this poll consumes the same
            // evidence `read_body_bytes` does, and a later reset would
            // otherwise destroy it (see `request_body_eof`).
            if self.request_body_reader.is_end_stream() {
                self.request_body_eof = true;
            }
            return Poll::Ready(Some(Ok(data)));
        }

        self.request_body_eof = true;
        Poll::Ready(None)
    }

    async fn do_drain_request_body(&mut self) -> Result<()> {
        loop {
            match self.read_body_bytes().await {
                Ok(Some(_)) => { /* continue to drain */ }
                Ok(None) => return Ok(()), // done
                Err(e) => return Err(e),
            }
        }
    }

    /// Drain the request body. `Ok(())` when there is no (more) body to read.
    // NOTE for h2 it may be worth allowing cancellation of the stream via reset.
    pub async fn drain_request_body(&mut self) -> Result<()> {
        // `is_body_empty()` is checked here (and deliberately NOT in
        // `is_body_done()`, which must stay the pure transport fact): a
        // request declaring `Content-Length: 0` promises zero DATA bytes, so
        // there is nothing to drain even if END_STREAM has not arrived yet.
        // Without this bound, and with `total_drain_timeout` defaulting to
        // `None`, draining such a request would await forever.
        if self.is_body_done() || self.is_body_empty() {
            return Ok(());
        }
        // Draining discards the remaining body — incompatible with capture-for-replay,
        // so drop any early body buffer first and remember the discard so a later
        // replay attempt fails closed (see the v1 counterpart for rationale).
        if self.early_body_buffer.take().is_some() {
            self.early_body_buffer_discarded = true;
        }
        match self.total_drain_timeout {
            Some(t) => match timeout(t, self.do_drain_request_body()).await {
                Ok(res) => res,
                Err(_) => Error::e_explain(
                    ErrorType::ReadTimedout,
                    format!("draining body, timeout: {t:?}"),
                ),
            },
            None => self.do_drain_request_body().await,
        }
    }

    /// Sets the downstream read timeout. This will trigger if the next
    /// request-body chunk cannot be read within `timeout`.
    ///
    /// The bound is an INTER-CHUNK idle bound, not a bound on the overall
    /// upload: it is rearmed by every DATA payload that carries at least one
    /// byte (and by END_STREAM), so a slow but progressing upload is never
    /// limited in total duration, while a client that stops sending is
    /// released after one `timeout` of silence. An empty DATA frame is not
    /// progress and does not rearm it.
    ///
    /// Defaults to 60s, matching the HTTP/1 server session. CONNECT requests
    /// (including extended CONNECT) are exempt regardless of what is set here;
    /// their "body" is a tunnel uplink on which idling is normal. Pass `None`
    /// to make the reads unbounded.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.read_timeout = timeout;
    }

    /// Get the read timeout.
    pub fn get_read_timeout(&self) -> Option<Duration> {
        self.read_timeout
    }

    /// Sets the downstream write timeout. This will trigger if we're unable
    /// to write to the stream after `timeout`.
    pub fn set_write_timeout(&mut self, timeout: Option<Duration>) {
        self.write_timeout = timeout;
    }

    /// Get the write timeout.
    pub fn get_write_timeout(&self) -> Option<Duration> {
        self.write_timeout
    }

    /// Sets the total drain timeout. This `timeout` will be used while draining
    /// the request body.
    pub fn set_total_drain_timeout(&mut self, timeout: Option<Duration>) {
        self.total_drain_timeout = timeout;
    }

    /// Get the total drain timeout.
    pub fn get_total_drain_timeout(&self) -> Option<Duration> {
        self.total_drain_timeout
    }

    // the write_* don't have timeouts because the actual writing happens on the connection
    // not here.

    /// Write the response header to the client.
    /// # the `end` flag
    /// `end` marks the end of this session.
    /// If the `end` flag is set, no more header or body can be sent to the client.
    pub fn write_response_header(
        &mut self,
        mut header: Box<ResponseHeader>,
        end: bool,
    ) -> Result<()> {
        if self.ended {
            // TODO: error or warn?
            return Ok(());
        }

        if header.status.is_informational() {
            // ignore informational response 1xx header because send_response() can only be called once
            // https://github.com/hyperium/h2/issues/167
            debug!("ignoring informational headers");
            return Ok(());
        }

        if self.response_written.as_ref().is_some() {
            warn!("Response header is already sent, cannot send again");
            return Ok(());
        }

        /* update headers */
        header.insert_header(header::DATE, get_cached_date())?;

        // remove other h1 hop headers that cannot be present in H2
        // https://httpwg.org/specs/rfc7540.html#n-connection-specific-header-fields
        header.remove_header(&header::TRANSFER_ENCODING);
        header.remove_header(&header::CONNECTION);
        header.remove_header(&header::UPGRADE);
        header.remove_header(&HeaderName::from_static("keep-alive"));
        header.remove_header(&HeaderName::from_static("proxy-connection"));

        let resp = Response::from_parts(header.as_owned_parts(), ());

        let body_writer = self.send_response.send_response(resp, end).or_err(
            ErrorType::WriteError,
            "while writing h2 response to downstream",
        )?;

        self.response_written = Some(header);
        self.send_response_body = Some(body_writer);
        self.ended = self.ended || end;
        // Committing the response ends any possibility of an upstream retry; a
        // fully replayed body buffer is dead weight for the rest of the response
        // (which may be long-lived, e.g. SSE / gRPC streaming).
        self.maybe_release_early_body_buffer();
        Ok(())
    }

    /// Drop the registered early body buffer once it can no longer be needed:
    /// replay reached EOF AND the response header was committed downstream. Both
    /// conditions are required — before the response commits, a retry may still
    /// rewind and replay the buffer; before replay EOF, the current attempt is
    /// still reading it. Called from both places where either condition becomes
    /// true. The `early_body_buffer_released` flag makes any later replay
    /// attempt fail closed (see `begin_request_body_replay`). Unlike HTTP/1,
    /// `response_written` here is only ever a non-informational header (1xx are
    /// not sent on the h2 path), so its presence alone means committed.
    fn maybe_release_early_body_buffer(&mut self) {
        if self.response_written.is_some()
            && self
                .early_body_buffer
                .as_ref()
                .is_some_and(RegisteredRequestBodyBuffer::is_replay_done)
        {
            self.early_body_buffer = None;
            self.early_body_buffer_released = true;
        }
    }

    /// Write response body to the client. See [Self::write_response_header] for how to use `end`.
    pub async fn write_body(&mut self, data: Bytes, end: bool) -> Result<()> {
        match self.write_timeout {
            Some(t) => match timeout(t, self.do_write_body(data, end)).await {
                Ok(res) => res,
                Err(_) => Error::e_explain(
                    ErrorType::WriteTimedout,
                    format!("writing body, timeout: {t:?}"),
                ),
            },
            None => self.do_write_body(data, end).await,
        }
    }

    async fn do_write_body(&mut self, data: Bytes, end: bool) -> Result<()> {
        if self.ended {
            // NOTE: in h1, we also track to see if content-length matches the data
            // We have not tracked that in h2
            warn!("Try to write body after end of stream, dropping the extra data");
            return Ok(());
        }
        let Some(writer) = self.send_response_body.as_mut() else {
            return Err(Error::explain(
                ErrorType::H2Error,
                "try to send body before header is sent",
            ));
        };
        let data_len = data.len();
        super::write_body(writer, data, end, self.write_timeout)
            .await
            .map_err(|e| e.into_down())?;
        self.body_sent += data_len;
        self.ended = self.ended || end;
        Ok(())
    }

    /// Write response trailers to the client, this also closes the stream.
    pub fn write_trailers(&mut self, trailers: HeaderMap) -> Result<()> {
        if self.ended {
            warn!("Tried to write trailers after end of stream, dropping them");
            return Ok(());
        }
        let Some(writer) = self.send_response_body.as_mut() else {
            return Err(Error::explain(
                ErrorType::H2Error,
                "try to send trailers before header is sent",
            ));
        };
        writer.send_trailers(trailers).or_err(
            ErrorType::WriteError,
            "while writing h2 response trailers to downstream",
        )?;
        // sending trailers closes the stream
        self.ended = true;
        Ok(())
    }

    /// Similar to [Self::write_response_header], this function takes a reference instead
    pub fn write_response_header_ref(&mut self, header: &ResponseHeader, end: bool) -> Result<()> {
        self.write_response_header(Box::new(header.clone()), end)
    }

    // TODO: trailer

    /// Mark the session end. If no `end` flag is already set before this call, this call will
    /// signal the client. Otherwise this call does nothing.
    ///
    /// Dropping this object without sending `end` will cause an error to the client, which will cause
    /// the client to treat this session as bad or incomplete.
    pub fn finish(&mut self) -> Result<()> {
        if self.ended {
            // already ended the stream
            return Ok(());
        }
        if let Some(writer) = self.send_response_body.as_mut() {
            // use an empty data frame to signal the end
            writer.send_data("".into(), true).or_err(
                ErrorType::WriteError,
                "while writing h2 response body to downstream",
            )?;
            self.ended = true;
        };
        // else: the response header is not sent, do nothing now.
        // When send_response_body is dropped, an RST_STREAM will be sent

        Ok(())
    }

    pub async fn response_duplex_vec(&mut self, tasks: Vec<HttpTask>) -> Result<bool> {
        let mut end_stream = false;
        for task in tasks.into_iter() {
            end_stream = match task {
                HttpTask::Header(header, end) => {
                    self.write_response_header(header, end)
                        .map_err(|e| e.into_down())?;
                    end
                }
                HttpTask::Body(data, end) => match data {
                    Some(d) => {
                        if !d.is_empty() {
                            self.write_body(d, end).await.map_err(|e| e.into_down())?;
                        }
                        end
                    }
                    None => end,
                },
                HttpTask::UpgradedBody(..) => {
                    // Seeing an Upgraded body means that the upstream session
                    // was H1.1 that upgraded.
                    //
                    // While the downstream H2 session may encapsulate the opaque body bytes,
                    // this represents an undefined discrepancy and change between how
                    // the upstream and downstream sessions began intepreting the response body.
                    return Error::e_explain(
                        ErrorType::InternalError,
                        "upgraded body on h2 server session",
                    );
                }
                HttpTask::Trailer(Some(trailers)) => {
                    self.write_trailers(*trailers)?;
                    true
                }
                HttpTask::Trailer(None) => true,
                HttpTask::Done => true,
                HttpTask::Failed(e) => {
                    return Err(e);
                }
            } || end_stream // safe guard in case `end` in tasks flips from true to false
        }
        if end_stream {
            // no-op if finished already
            self.finish().map_err(|e| e.into_down())?;
        }
        Ok(end_stream)
    }

    /// Return a string `$METHOD $PATH, Host: $HOST`. Mostly for logging and debug purpose
    pub fn request_summary(&self) -> String {
        format!(
            "{} {}, Host: {}:{}",
            self.request_header.method,
            self.request_header
                .uri
                .path_and_query()
                .map(PathAndQuery::as_str)
                .unwrap_or_default(),
            self.request_header.uri.host().unwrap_or_default(),
            self.req_header()
                .uri
                .port()
                .as_ref()
                .map(|port| port.as_str())
                .unwrap_or_default()
        )
    }

    /// Return the written response header. `None` if it is not written yet.
    pub fn response_written(&self) -> Option<&ResponseHeader> {
        self.response_written.as_deref()
    }

    /// Give up the stream abruptly.
    ///
    /// This will send an `INTERNAL_ERROR` stream error to the client.
    pub fn shutdown(&mut self) {
        self.shutdown_with_reason(h2::Reason::INTERNAL_ERROR);
    }

    /// Give up the stream abruptly with a custom reason.
    ///
    /// This will send a `RST_STREAM` frame with the given reason to the client.
    ///
    /// Useful reasons include:
    /// - [`h2::Reason::HTTP_1_1_REQUIRED`] - Signal to the client that HTTP/1.1 should be used
    ///   instead. Per RFC 7540 §9.1.2, clients should retry the request over HTTP/1.1.
    /// - [`h2::Reason::CANCEL`] - Indicate the stream is no longer needed.
    /// - [`h2::Reason::REFUSED_STREAM`] - Indicate the stream was refused before processing.
    pub fn shutdown_with_reason(&mut self, reason: h2::Reason) {
        if !self.ended {
            self.send_response.send_reset(reason);
        }
    }

    #[doc(hidden)]
    pub fn take_response_body_writer(&mut self) -> Option<SendStream<Bytes>> {
        self.send_response_body.take()
    }

    // This is a hack for pingora-proxy to create subrequests from h2 server session
    // TODO: be able to convert from h2 to h1 subrequest
    pub fn pseudo_raw_h1_request_header(&self) -> Bytes {
        // `http_req_header_to_wire` returns `None` for an unsupported HTTP
        // version or a request target containing forbidden delimiter bytes.
        // Neither should happen here: H2 sessions always carry `HTTP_2`, and
        // forbidden targets are rejected when the session is created (see
        // `from_h2_conn`).
        http_req_header_to_wire(&self.request_header)
            .map(|buf| buf.freeze())
            .expect("http_req_header_to_wire should not fail for a validated h2 request")
    }

    /// Whether there is no more body to read
    ///
    /// Reports the LATCHED transport fact (`request_body_eof`, sources (i) and
    /// (ii)) rather than the live `is_end_stream()`, so that a peer resetting a
    /// stream it already ended cannot flip this back to `false`. The live value
    /// is still consulted so that an END_STREAM which arrived without us polling
    /// is picked up immediately.
    ///
    /// Deliberately does NOT consult source (iii) (`content-length` satisfied,
    /// see [`Self::request_body_complete`]): the callers of this function stop
    /// reading the request body once it returns `true`, and an H2 request may
    /// legally send TRAILERS after a complete, `content-length`-declared body.
    /// Ending the read there would silently drop those trailers and skip the
    /// trailer hook. Source (iii) exists to classify a stream END, which is a
    /// strictly later event, so nothing is lost by the narrower rule here.
    pub fn is_body_done(&self) -> bool {
        if self
            .early_body_buffer
            .as_ref()
            .is_some_and(RegisteredRequestBodyBuffer::is_replaying)
        {
            return false;
        }
        self.request_body_eof || self.request_body_reader.is_end_stream()
    }

    /// Whether there is any body to read. true means there no body in request.
    ///
    /// While an early request body buffer is registered, the effective body is whatever
    /// the buffer replays, which may be a non-empty rewrite of a zero-byte original
    /// (e.g. HEADERS without END_STREAM followed by an empty END_STREAM DATA frame).
    /// Report non-empty then, so upstream framing decisions (H2 END_STREAM on HEADERS)
    /// keep the stream open until replay reaches EOF.
    ///
    /// Like [`Self::is_body_done`] this reads the LATCHED end-of-stream fact,
    /// never the live `is_end_stream()`: a bodyless request (END_STREAM on
    /// HEADERS, e.g. a plain `GET`) whose client then resets the stream must not
    /// stop reporting itself as bodyless. A retractable answer here defeats the
    /// anti-smuggling coercion in `pingora-proxy`'s `safe_disposition`, which
    /// keys on "this request has no body at all".
    pub fn is_body_empty(&self) -> bool {
        if self.early_body_buffer.is_some() {
            return false;
        }
        self.body_read == 0
            && (self.request_body_eof
                || self.request_body_reader.is_end_stream()
                || self
                    .request_header
                    .headers
                    .get(header::CONTENT_LENGTH)
                    .is_some_and(|cl| cl.as_bytes() == b"0"))
    }

    /// Whether the initial HEADERS frame carried END_STREAM.
    pub fn request_headers_end_stream(&self) -> bool {
        self.request_headers_end_stream
    }

    /// Whether actual request trailer fields were received.
    ///
    /// This is meaningful after `read_body_bytes()` returns EOF.
    pub fn request_trailers_present(&self) -> bool {
        self.request_trailers_present
    }

    /// Whether the response body writer has already been ended (END_STREAM
    /// sent). `false` while a response is still being streamed.
    pub fn response_body_finished(&self) -> bool {
        self.ended
    }

    pub fn retry_buffer_truncated(&self) -> bool {
        self.retry_buffer
            .as_ref()
            .map_or_else(|| false, |r| r.is_truncated())
    }

    pub fn enable_retry_buffering(&mut self) {
        if self.retry_buffer.is_none() {
            self.retry_buffer = Some(FixedBuffer::new(BODY_BUF_LIMIT))
        }
    }

    pub fn get_retry_buffer(&self) -> Option<Bytes> {
        self.retry_buffer.as_ref().and_then(|b| {
            if b.is_truncated() {
                None
            } else {
                b.get_buffer()
            }
        })
    }

    /// See `v1::server::HttpSession::set_request_body_buffer`. Fails closed if the
    /// body has already started being read (would capture only the remainder) and
    /// for CONNECT requests, whose "body" is a bidirectional tunnel stream.
    pub fn set_request_body_buffer(&mut self, buffer: Box<dyn RequestBodyBuffer>) -> Result<()> {
        if self.early_body_buffer.is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer is already registered",
            );
        }
        // Double-send defense, see the v1 counterpart: with retry buffering
        // already enabled, the capturing reads would tee drained chunks into
        // the native retry buffer and the proxy would send that buffer AND
        // replay this one.
        if self.retry_buffer.is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer cannot be registered while retry buffering is enabled",
            );
        }
        // Extended CONNECT (RFC 8441) also uses :method = CONNECT, so this single
        // check covers both plain and extended CONNECT tunnels.
        if self.request_header.method == http::Method::CONNECT {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer cannot be registered for a CONNECT request",
            );
        }
        if self.is_body_empty() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer cannot be registered for an empty request body",
            );
        }
        if self.body_read > 0 {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer must be registered before the body is read",
            );
        }
        self.early_body_buffer = Some(RegisteredRequestBodyBuffer::new(buffer));
        Ok(())
    }

    /// Register an already-finalized replay source for a request whose
    /// downstream stream already ended with no body (END_STREAM on HEADERS, or
    /// an already-consumed zero-byte body). This performs no downstream capture
    /// and lets the upstream stream remain open for an application-injected
    /// body.
    pub fn set_bodyless_request_replay_buffer(
        &mut self,
        buffer: Box<dyn RequestBodyBuffer>,
    ) -> Result<()> {
        if self.early_body_buffer.is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer is already registered",
            );
        }
        // Same double-send defense as `set_request_body_buffer`: keep the two
        // mechanisms mutually exclusive by construction.
        if self.retry_buffer.is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body replay buffer cannot be registered while retry buffering is enabled",
            );
        }
        if self.request_header.method == http::Method::CONNECT {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body replay buffer cannot be registered for a CONNECT request",
            );
        }
        if !self.is_body_empty() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body replay buffer requires an empty downstream request body",
            );
        }
        // `is_body_empty()` alone is not enough: `Content-Length: 0` makes it
        // true even while the downstream stream is still open (HEADERS without
        // END_STREAM). Replay would then permanently shadow the live stream —
        // its empty DATA + END_STREAM, trailers, or content-length-violating
        // DATA would never be observed, and dropping the `RecvStream` at
        // request end could send RST_STREAM(CANCEL) to the client.
        if !self.is_body_done() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body replay buffer requires the downstream request stream to have ended",
            );
        }
        self.early_body_buffer = Some(RegisteredRequestBodyBuffer::ready(buffer));
        Ok(())
    }

    pub fn request_body_buffer_registered(&self) -> bool {
        self.early_body_buffer.is_some()
    }

    /// Whether a registered request body buffer is currently replaying, i.e.
    /// `read_body_or_idle` serves buffered chunks instead of reading the client
    /// stream, so its errors originate in the buffer, not the client.
    pub fn request_body_buffer_replaying(&self) -> bool {
        self.early_body_buffer
            .as_ref()
            .is_some_and(RegisteredRequestBodyBuffer::is_replaying)
    }

    /// Prepare the registered buffer as the active request-body source for one
    /// upstream attempt. Returns `false` when no buffer was registered.
    pub async fn begin_request_body_replay(&mut self) -> Result<bool> {
        if self.early_body_capture_poisoned {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body capture failed or was cancelled mid-chunk; refusing to replay incomplete buffered body",
            );
        }
        if self.early_body_buffer_discarded {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer was discarded by drain_request_body; the request body is gone and cannot be replayed",
            );
        }
        if self.early_body_buffer_released {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer was released after the response was committed downstream; no further replay is possible",
            );
        }
        let Some(registered) = self.early_body_buffer.as_mut() else {
            return Ok(false);
        };
        registered.begin_replay().await?;
        // Replay serves buffered chunks, so any deadline left over from a live
        // read that was cancelled before this attempt no longer describes the
        // client's silence: drop it so the first live read after the replay
        // starts a fresh bound instead of expiring immediately.
        self.read_deadline = None;
        Ok(true)
    }

    /// `async fn idle() -> Result<Reason, Error>;`
    /// This async fn will be pending forever until the client closes the stream/connection
    /// This function is used for watching client status so that the server is able to cancel
    /// its internal tasks as the client waiting for the tasks goes away
    pub fn idle(&mut self) -> Idle<'_> {
        Idle(self)
    }

    /// Similar to `read_body_bytes()` but will be pending after Ok(None) is returned,
    /// until the client closes the connection
    pub async fn read_body_or_idle(&mut self, no_body_expected: bool) -> Result<Option<Bytes>> {
        if let Some(registered) = self.early_body_buffer.as_mut() {
            if registered.is_replaying() {
                let chunk = registered.next_chunk().await?;
                if chunk.is_none() {
                    // Replay EOF. If the response was already committed downstream
                    // (upstream responded before replay finished), the buffer can
                    // never be needed again — drop it now.
                    self.maybe_release_early_body_buffer();
                }
                return Ok(chunk);
            }
        }
        if no_body_expected || self.is_body_done() {
            let reason = self.idle().await?;
            Error::e_explain(
                ErrorType::H2Error,
                format!("Client closed H2, reason: {reason}"),
            )
        } else {
            self.read_body_bytes().await
        }
    }

    /// Return how many response body bytes (application, not wire) already sent downstream
    pub fn body_bytes_sent(&self) -> usize {
        self.body_sent
    }

    /// Return how many request body bytes (application, not wire) already read from downstream
    pub fn body_bytes_read(&self) -> usize {
        self.body_read
    }

    /// Return the [Digest] of the connection.
    pub fn digest(&self) -> Option<&Digest> {
        Some(&self.digest)
    }

    /// Return a mutable [Digest] reference for the connection.
    pub fn digest_mut(&mut self) -> Option<&mut Digest> {
        Arc::get_mut(&mut self.digest)
    }

    /// Return the server (local) address recorded in the connection digest.
    pub fn server_addr(&self) -> Option<&SocketAddr> {
        self.digest.socket_digest.as_ref().map(|d| d.local_addr())?
    }

    /// Return the client (peer) address recorded in the connection digest.
    pub fn client_addr(&self) -> Option<&SocketAddr> {
        self.digest.socket_digest.as_ref().map(|d| d.peer_addr())?
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;
    use futures::SinkExt;
    use h2::frame::{Frame, Headers, Pseudo, Reset, Settings};
    use http::Uri;
    use http::{HeaderValue, Method, Request, StatusCode};
    use tokio::io::{duplex, AsyncWriteExt, DuplexStream};
    use tokio::sync::oneshot;
    use tokio_stream::StreamExt;

    async fn advertised_settings(options: Option<H2Options>) -> Settings {
        let (mut client, server) = duplex(65536);
        let handshake = tokio::spawn(async move { handshake(Box::new(server), options).await });

        client
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        let mut codec: h2::Codec<DuplexStream, Bytes> = h2::Codec::new(client);
        let settings = match codec.next().await.unwrap().unwrap() {
            Frame::Settings(settings) => settings,
            frame => panic!("expected SETTINGS frame, received {frame:?}"),
        };

        let _ = handshake.await.unwrap().unwrap();
        settings
    }

    #[test]
    fn test_authority_host_mismatch() {
        let request = |hosts: &[&str]| {
            let mut request = Request::builder()
                .uri("https://authority.example/test")
                .body(())
                .unwrap();
            for host in hosts {
                request
                    .headers_mut()
                    .append(header::HOST, HeaderValue::from_str(host).unwrap());
            }
            RequestHeader::from(request.into_parts().0)
        };

        assert!(!authority_host_mismatch(&request(&[])));
        assert!(!authority_host_mismatch(&request(&["authority.example"])));
        assert!(authority_host_mismatch(&request(&["other.example"])));
        assert!(authority_host_mismatch(&request(&["AUTHORITY.EXAMPLE"])));
        assert!(authority_host_mismatch(&request(&[
            "authority.example:443"
        ])));
        assert!(authority_host_mismatch(&request(&[
            "authority.example",
            "authority.example",
        ])));
        assert!(authority_host_mismatch(&request(&[
            "authority.example",
            "other.example",
        ])));

        let request = RequestHeader::from(
            Request::builder()
                .uri("/test")
                .header(header::HOST, "host.example")
                .body(())
                .unwrap()
                .into_parts()
                .0,
        );
        assert!(!authority_host_mismatch(&request));
    }

    #[tokio::test]
    async fn test_server_rejects_authority_host_mismatch_with_400() {
        let (client, server) = duplex(65536);

        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });

            let mut h2 = h2.ready().await.unwrap();
            let mismatched = Request::builder()
                .method(Method::GET)
                .uri("https://authority.example/test")
                .header(header::HOST, "other.example")
                .body(())
                .unwrap();
            let (response, request_body) = h2.send_request(mismatched, false).unwrap();

            let response = response.await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            drop(response);
            drop(request_body);

            let mut h2 = h2.ready().await.unwrap();
            let duplicate = Request::builder()
                .method(Method::GET)
                .uri("https://authority.example/test")
                .header(header::HOST, "authority.example")
                .header(header::HOST, "authority.example")
                .body(())
                .unwrap();
            let (response, _) = h2.send_request(duplicate, true).unwrap();

            let response = response.await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let mut body = response.into_body();
            assert!(body.data().await.is_none());

            let mut h2 = h2.ready().await.unwrap();
            let matching = Request::builder()
                .method(Method::GET)
                .uri("https://authority.example/test")
                .header(header::HOST, "authority.example")
                .body(())
                .unwrap();
            let (response, _) = h2.send_request(matching, true).unwrap();

            assert_eq!(response.await.unwrap().status(), StatusCode::NO_CONTENT);
        });

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let accepted = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap();
        assert!(
            matches!(accepted, Some(H2Accept::Rejected)),
            "mismatched authority reached the application"
        );

        let accepted = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap();
        assert!(
            matches!(accepted, Some(H2Accept::Rejected)),
            "duplicate Host fields reached the application"
        );

        let Some(H2Accept::Session(mut session)) =
            HttpSession::from_h2_conn(&mut connection, digest.clone())
                .await
                .unwrap()
        else {
            panic!("matching authority did not reach the application");
        };
        assert_eq!(
            session.req_header().headers[header::HOST],
            "authority.example"
        );
        session
            .write_response_header(
                Box::new(ResponseHeader::build(StatusCode::NO_CONTENT, Some(0)).unwrap()),
                true,
            )
            .unwrap();
        drop(session);

        let done = timeout(
            Duration::from_secs(1),
            HttpSession::from_h2_conn(&mut connection, digest),
        )
        .await
        .expect("connection did not finish after authority mismatch test")
        .expect("connection failed after authority mismatch test");
        assert!(done.is_none());

        client.await.unwrap();
    }

    #[tokio::test]
    async fn test_failed_authority_rejection_write_is_stream_local() {
        let (mut client, server) = duplex(65536);
        let (frames_sent, wait_for_frames) = oneshot::channel();

        let client = tokio::spawn(async move {
            client
                .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
                .await
                .unwrap();
            let mut codec: h2::Codec<DuplexStream, Bytes> = h2::Codec::new(client);
            codec.send(Settings::default().into()).await.unwrap();
            codec.send(Settings::ack().into()).await.unwrap();

            let mut fields = HeaderMap::new();
            fields.insert(header::HOST, HeaderValue::from_static("other.example"));
            let mut mismatched = Headers::new(
                1.into(),
                Pseudo::request(
                    Method::GET,
                    Uri::from_static("https://authority.example/test"),
                    None,
                ),
                fields,
            );
            mismatched.set_end_headers();
            codec.send(mismatched.into()).await.unwrap();
            codec
                .send(Reset::new(1.into(), h2::Reason::CANCEL).into())
                .await
                .unwrap();

            let mut fields = HeaderMap::new();
            fields.insert(header::HOST, HeaderValue::from_static("authority.example"));
            let mut matching = Headers::new(
                3.into(),
                Pseudo::request(
                    Method::GET,
                    Uri::from_static("https://authority.example/test"),
                    None,
                ),
                fields,
            );
            matching.set_end_headers();
            matching.set_end_stream();
            codec.send(matching.into()).await.unwrap();
            frames_sent.send(()).unwrap();

            timeout(Duration::from_secs(1), async {
                while let Some(frame) = codec.next().await {
                    if let Frame::Headers(headers) = frame.unwrap() {
                        if headers.stream_id() == 3u32 {
                            return;
                        }
                    }
                }
                panic!("connection closed before the sibling response");
            })
            .await
            .expect("timed out waiting for the sibling response");
        });

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        wait_for_frames.await.unwrap();
        let digest = Arc::new(Digest::default());

        let accepted = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap();
        assert!(
            matches!(accepted, Some(H2Accept::Rejected)),
            "reset authority mismatch reached the application"
        );

        let Some(H2Accept::Session(mut session)) =
            HttpSession::from_h2_conn(&mut connection, digest.clone())
                .await
                .unwrap()
        else {
            panic!("sibling stream was dropped after the failed 400 write");
        };
        session
            .write_response_header(
                Box::new(ResponseHeader::build(StatusCode::NO_CONTENT, Some(0)).unwrap()),
                true,
            )
            .unwrap();
        drop(session);

        let done = timeout(
            Duration::from_secs(1),
            HttpSession::from_h2_conn(&mut connection, digest),
        )
        .await
        .expect("connection did not finish after the sibling response")
        .expect("connection failed after the sibling response");
        assert!(done.is_none());
        client.await.unwrap();
    }

    #[tokio::test]
    async fn test_authority_mismatch_exhausts_malformed_stream_budget() {
        let (client, server) = duplex(65536);

        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });

            let mut h2 = h2.ready().await.unwrap();
            let mismatched = Request::builder()
                .method(Method::GET)
                .uri("https://authority.example/test")
                .header(header::HOST, "other.example")
                .body(())
                .unwrap();
            let (response, _) = h2.send_request(mismatched, true).unwrap();

            // The connection can close before the queued 400 is flushed when
            // this request exhausts the connection-level malformed budget.
            let _ = response.await;
        });

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let mut malformed_streams = MAX_MALFORMED_STREAMS_PER_CONN - 1;

        let err = match HttpSession::from_h2_conn_with_malformed_budget(
            &mut connection,
            digest,
            &mut malformed_streams,
        )
        .await
        {
            Ok(Some(_)) => panic!("authority mismatch must not surface as a session"),
            Ok(None) => panic!("connection ended before malformed budget was exhausted"),
            Err(err) => err,
        };

        assert_eq!(err.etype(), &ErrorType::H2Error);
        assert_eq!(malformed_streams, MAX_MALFORMED_STREAMS_PER_CONN);

        drop(connection);
        client.await.unwrap();
    }

    #[tokio::test]
    async fn test_server_handshake_uses_bounded_default_options() {
        let settings = advertised_settings(None).await;

        assert_eq!(
            settings.max_header_list_size(),
            Some(DEFAULT_MAX_HEADER_LIST_SIZE)
        );
        assert_eq!(
            settings.max_concurrent_streams(),
            Some(DEFAULT_MAX_CONCURRENT_STREAMS)
        );
    }

    #[tokio::test]
    async fn test_server_handshake_uses_caller_options() {
        let mut options = H2Options::default();
        options.max_header_list_size(1234);
        options.max_concurrent_streams(42);

        let settings = advertised_settings(Some(options)).await;

        assert_eq!(settings.max_header_list_size(), Some(1234));
        assert_eq!(settings.max_concurrent_streams(), Some(42));
    }

    #[tokio::test]
    async fn test_zero_idle_timeout_waits_for_active_session() {
        let active = Arc::new(ActiveSessions::new());
        let guard = active.start_session();
        let timeout_active = active.clone();
        let timeout = tokio::spawn(async move {
            wait_for_idle_timeout(&timeout_active, Duration::ZERO).await;
        });

        tokio::task::yield_now().await;
        assert!(
            !timeout.is_finished(),
            "zero timeout must not spin or expire while a session is active"
        );

        drop(guard);
        pingora_timeout::timeout(Duration::from_secs(1), timeout)
            .await
            .expect("zero timeout did not expire after the session finished")
            .expect("timeout task panicked");
    }

    #[tokio::test]
    async fn test_server_handshake_rejects_oversized_header_list_by_default() {
        let (client, server) = duplex(256 * 1024);

        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });

            let mut request = Request::builder()
                .method(Method::GET)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            for _ in 0..2000 {
                request
                    .headers_mut()
                    .append("a", HeaderValue::from_static(""));
            }

            let (response, _) = h2
                .ready()
                .await
                .unwrap()
                .send_request(request, true)
                .unwrap();
            assert_eq!(
                response.await.unwrap().status(),
                http::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
            );
        });

        let server = tokio::spawn(async move {
            let mut connection = handshake(Box::new(server), None).await.unwrap();
            let digest = Arc::new(Digest::default());
            let accepted = timeout(
                Duration::from_secs(1),
                HttpSession::from_h2_conn(&mut connection, digest),
            )
            .await;
            assert!(
                !matches!(accepted, Ok(Ok(Some(_)))),
                "oversized request reached the application"
            );
        });

        client.await.unwrap();
        server.await.unwrap();
    }

    #[cfg(feature = "patched_http1")]
    #[tokio::test]
    async fn test_server_rejects_forbidden_byte_in_request_target() {
        // Control bytes (CR/LF) may be accepted in the request path depending
        // on URI parsing. Ensure such a request target is rejected on ingest as
        // defense-in-depth, since these bytes are not permitted in a URI.
        let (client, server) = duplex(65536);

        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });

            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::GET)
                .uri("https://www.example.com/a\r\nX-Injected: 1")
                .body(())
                .unwrap();
            // Ensure CR/LF survived URI parsing, otherwise the test is a no-op.
            assert!(request.uri().path().contains('\n'));

            let (response, _) = h2.send_request(request, true).unwrap();
            // The stream must be rejected (reset), not answered.
            assert!(response.await.is_err());
        });

        let server = tokio::spawn(async move {
            let mut connection = handshake(Box::new(server), None).await.unwrap();
            let digest = Arc::new(Digest::default());
            let accepted = timeout(
                Duration::from_secs(1),
                HttpSession::from_h2_conn(&mut connection, digest),
            )
            .await
            .expect("from_h2_conn hung: the offending stream was not rejected")
            .expect("from_h2_conn returned an error");
            // The offending stream is reset during acceptance, so `from_h2_conn`
            // yields `Rejected` rather than a session built from the forbidden
            // request target. Sibling streams and the connection are unaffected.
            assert!(
                matches!(accepted, Some(H2Accept::Rejected)),
                "request with forbidden byte in target was not rejected"
            );
        });

        client.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_server_handshake_accept_request() {
        let (client, server) = duplex(65536);
        let client_body = "test client body";
        let server_body = "test server body";

        let mut expected_trailers = HeaderMap::new();
        expected_trailers.insert("test", HeaderValue::from_static("trailers"));
        let trailers = expected_trailers.clone();

        let mut handles = vec![];
        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::GET)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();

            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.reserve_capacity(client_body.len());
            req_body.send_data(client_body.into(), true).unwrap();

            let (head, mut body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
            let data = body.data().await.unwrap().unwrap();
            assert_eq!(data, server_body);
            let resp_trailers = body.trailers().await.unwrap().unwrap();
            assert_eq!(resp_trailers, expected_trailers);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(accepted) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let H2Accept::Session(mut http) = accepted else {
                continue;
            };
            let trailers = trailers.clone();
            handles.push(tokio::spawn(async move {
                let req = http.req_header();
                assert_eq!(req.method, Method::GET);
                assert_eq!(req.uri, "https://www.example.com/");

                http.enable_retry_buffering();

                assert!(!http.is_body_empty());
                assert!(!http.is_body_done());

                let body = http.read_body_or_idle(false).await.unwrap().unwrap();
                assert_eq!(body, client_body);
                assert!(http.is_body_done());
                assert_eq!(http.body_bytes_read(), 16);

                let retry_body = http.get_retry_buffer().unwrap();
                assert_eq!(retry_body, client_body);

                // test idling before response header is sent
                tokio::select! {
                    _ = http.idle() => {panic!("downstream should be idling")},
                    _= tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
                }

                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                assert!(http
                    .write_response_header(response_header.clone(), false)
                    .is_ok());
                // this write should be ignored otherwise we will error
                assert!(http.write_response_header(response_header, false).is_ok());

                // test idling after response header is sent
                tokio::select! {
                    _ = http.read_body_or_idle(false) => {panic!("downstream should be idling")},
                    _= tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
                }

                // end: false here to verify finish() closes the stream nicely
                http.write_body(server_body.into(), false).await.unwrap();
                assert_eq!(http.body_bytes_sent(), 16);

                http.write_trailers(trailers).unwrap();
                http.finish().unwrap();
            }));
        }
        for handle in handles {
            // ensure no panics
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_req_conflicting_content_length_rejected() {
        let (client, server) = duplex(65536);

        let client_task = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            // Conflicting duplicate Content-Length values: unrecoverable framing.
            let request = Request::builder()
                .method(Method::GET)
                .uri("https://www.example.com/")
                .header("content-length", "5")
                .header("content-length", "6")
                .body(())
                .unwrap();

            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            // Send a body matching the first Content-Length value so the h2
            // codec accepts the stream and Pingora's duplicate-CL validation is
            // what rejects the request.
            req_body.reserve_capacity(5);
            req_body.send_data("abcde".into(), true).unwrap();

            // The server must reject the stream with PROTOCOL_ERROR rather than
            // surface the request to the application.
            let err = response.await.unwrap_err();
            assert_eq!(err.reason(), Some(h2::Reason::PROTOCOL_ERROR));
            // Dropping `h2` lets the connection close so the server loop exits.
        });

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        // The malformed stream is reset during acceptance, so it is reported as
        // rejected rather than surfaced to the application as a session.
        let accepted = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap();
        assert!(
            matches!(accepted, Some(H2Accept::Rejected)),
            "malformed request must not surface as a session"
        );

        let done = timeout(
            Duration::from_secs(1),
            HttpSession::from_h2_conn(&mut connection, digest),
        )
        .await
        .expect("from_h2_conn hung after rejecting malformed request")
        .expect("from_h2_conn returned an error after rejecting malformed request");
        assert!(done.is_none(), "connection should close after rejection");

        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_req_malformed_stream_budget_exhausted() {
        let (client, server) = duplex(65536);

        let client_task = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::GET)
                .uri("https://www.example.com/")
                .header("content-length", "5")
                .header("content-length", "6")
                .body(())
                .unwrap();

            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.reserve_capacity(5);
            req_body.send_data("abcde".into(), true).unwrap();
            let err = response.await.unwrap_err();
            if let Some(reason) = err.reason() {
                assert_eq!(reason, h2::Reason::PROTOCOL_ERROR);
            }
        });

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let mut malformed_streams = MAX_MALFORMED_STREAMS_PER_CONN - 1;

        let err = match HttpSession::from_h2_conn_with_malformed_budget(
            &mut connection,
            digest,
            &mut malformed_streams,
        )
        .await
        {
            Ok(Some(_)) => panic!("malformed request must not surface as a session"),
            Ok(None) => panic!("connection ended before malformed budget was exhausted"),
            Err(err) => err,
        };

        assert_eq!(err.etype(), &ErrorType::H2Error);
        assert_eq!(malformed_streams, MAX_MALFORMED_STREAMS_PER_CONN);

        drop(connection);
        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_req_content_length_eq_0_and_no_header_eos() {
        let (client, server) = duplex(65536);

        let server_body = "test server body";

        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .header("content-length", "0") // explicitly set
                .body(())
                .unwrap();

            let (response, mut req_body) = h2.send_request(request, false).unwrap(); // no EOS

            let (head, mut body) = response.await.unwrap().into_parts();

            assert_eq!(head.status, 200);
            let data = body.data().await.unwrap().unwrap();
            assert_eq!(data, server_body);

            req_body.send_data("".into(), true).unwrap(); // set EOS after read the resp body
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(accepted) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let H2Accept::Session(mut http) = accepted else {
                continue;
            };
            handles.push(tokio::spawn(async move {
                let req = http.req_header();
                assert_eq!(req.method, Method::POST);
                assert_eq!(req.uri, "https://www.example.com/");

                // 1. Check body related methods
                http.enable_retry_buffering();
                assert!(http.is_body_empty());
                assert!(!http.request_headers_end_stream());
                assert!(!http.is_body_done());
                let retry_body = http.get_retry_buffer();
                assert!(retry_body.is_none());

                // 2. Send response
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                assert!(http
                    .write_response_header(response_header.clone(), false)
                    .is_ok());

                http.write_body(server_body.into(), false).await.unwrap();
                assert_eq!(http.body_bytes_sent(), 16);

                // 3. The client drops the response stream after sending its request EOS,
                // so this test observes the resulting reset.
                assert!(http.read_body_or_idle(false).await.is_err());
            }));
        }

        for handle in handles {
            // ensure no panics
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_req_header_no_eos_empty_data_with_eos() {
        let (client, server) = duplex(65536);

        let server_body = "test server body";

        let mut handles = vec![];
        // Keeps the client's response stream alive until the server has made
        // its observation: dropping it earlier puts a RST_STREAM on the wire
        // and the read under test would race that reset instead of observing
        // the empty END_STREAM DATA frame this test is named for.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });

            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();

            let (response, mut req_body) = h2.send_request(request, false).unwrap(); // no EOS

            let (head, mut body) = response.await.unwrap().into_parts();

            assert_eq!(head.status, 200);
            let data = body.data().await.unwrap().unwrap();
            assert_eq!(data, server_body);

            req_body.send_data("".into(), true).unwrap(); // set EOS after read the resp body
            let _ = done_rx.await;
            // Drain the response to EOS before dropping the stream. Newer h2
            // sends RST_STREAM(CANCEL) when a still-open recv stream is dropped,
            // which would race with the server reading the request EOS and turn
            // the server-side read into a stream-reset error.
            while let Some(chunk) = body.data().await {
                let chunk = chunk.expect("response body error");
                body.flow_control()
                    .release_capacity(chunk.len())
                    .expect("release capacity");
            }
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        let mut done_tx = Some(done_tx);
        while let Some(accepted) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let H2Accept::Session(mut http) = accepted else {
                continue;
            };
            let done_tx = done_tx.take().expect("exactly one stream");
            handles.push(tokio::spawn(async move {
                let req = http.req_header();
                assert_eq!(req.method, Method::POST);
                assert_eq!(req.uri, "https://www.example.com/");

                // 1. Check body related methods
                http.enable_retry_buffering();
                assert!(!http.request_headers_end_stream());
                assert!(!http.is_body_empty());
                assert!(!http.is_body_done());
                let retry_body = http.get_retry_buffer();
                assert!(retry_body.is_none());

                // 2. Send response
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                assert!(http
                    .write_response_header(response_header.clone(), false)
                    .is_ok());

                http.write_body(server_body.into(), false).await.unwrap();
                assert_eq!(http.body_bytes_sent(), 16);

                // 3. The client's end of stream arrives as an EMPTY DATA frame
                // carrying END_STREAM -- the shape this test is named for. `h2`
                // hands that over as a zero-length chunk (NOT as `None`), and
                // the request must be reported as done and still empty
                // afterwards.
                let chunk = http
                    .read_body_or_idle(false)
                    .await
                    .expect("an empty END_STREAM DATA frame is a clean end of body");
                assert_eq!(
                    chunk.as_deref(),
                    Some(&b""[..]),
                    "the empty END_STREAM DATA frame is delivered as a zero-length chunk"
                );
                assert!(http.is_body_done());
                assert!(http.is_body_empty());
                assert_eq!(http.body_bytes_read(), 0);

                // 4. Finish the response so the client can drain it to EOS and
                //    close the stream cleanly instead of cancelling it.
                http.finish().unwrap();
                let _ = done_tx.send(());
            }));
        }

        for handle in handles {
            // ensure no panics
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_early_body_buffer_captures_replays_and_rejects_late_set() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("abc".into(), true).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
                http.set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                    .unwrap();
                {
                    let waker = futures::task::noop_waker();
                    let mut poll_ctx = std::task::Context::from_waker(&waker);
                    assert!(matches!(
                        http.poll_read_body_bytes(&mut poll_ctx),
                        Poll::Ready(Some(Err(_)))
                    ));
                }
                let mut total = Vec::new();
                while let Some(chunk) = http.read_body_bytes().await.unwrap() {
                    total.extend_from_slice(&chunk);
                }
                assert_eq!(total, b"abc");
                // Rewindable: every upstream attempt reads the same body in chunks.
                for _ in 0..2 {
                    assert!(http.begin_request_body_replay().await.unwrap());
                    assert_eq!(
                        http.read_body_or_idle(false).await.unwrap().unwrap(),
                        b"abc".as_slice()
                    );
                    assert!(http.read_body_or_idle(false).await.unwrap().is_none());
                }
                // Registering after the body was read must fail closed.
                assert!(http
                    .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                    .is_err());
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    /// Trailers after a COMPLETE, `content-length`-declared body.
    ///
    /// The declared length is what pins the P1 split: source (iii) of
    /// `request_body_complete()` is satisfied the moment the seventh byte
    /// arrives, and folding that source into `is_body_done()` would make the
    /// assertion below it (`!is_body_done()` after the payload) fail. That is
    /// not a style preference -- both proxy pumps stop reading the downstream
    /// body when `is_body_done()` reports `true`, so folding (iii) in would
    /// silently drop these trailer fields and skip `request_trailer_filter`.
    #[tokio::test]
    async fn test_request_trailer_presence_is_transport_observed() {
        let (client, server) = duplex(65536);

        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .header("trailer", "x-checksum")
                .header("content-length", "7")
                .body(())
                .unwrap();
            let (response, mut body) = h2
                .ready()
                .await
                .unwrap()
                .send_request(request, false)
                .unwrap();
            body.send_data("payload".into(), false).unwrap();
            let mut trailers = HeaderMap::new();
            trailers.insert("x-checksum", HeaderValue::from_static("ok"));
            body.send_trailers(trailers).unwrap();
            assert_eq!(response.await.unwrap().status(), StatusCode::NO_CONTENT);
        });

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let mut handlers = Vec::new();
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handlers.push(tokio::spawn(async move {
                assert!(!http.request_headers_end_stream());
                let data = http.read_body_bytes().await.unwrap().unwrap();
                assert_eq!(data, "payload");
                // The declared `content-length` is now fully received, but the
                // TRANSPORT has not ended: the trailers are still to come.
                // `is_body_done()` must report the transport, or the pumps stop
                // reading here and the trailers below are never observed.
                assert!(
                    !http.is_body_done(),
                    "a satisfied content-length must not end the read before the trailers"
                );
                assert!(http.read_body_bytes().await.unwrap().is_none());
                assert!(http.is_body_done());
                assert!(http.request_trailers_present());

                let response =
                    Box::new(ResponseHeader::build(StatusCode::NO_CONTENT, None).unwrap());
                http.write_response_header(response, true).unwrap();
            }));
        }
        client.await.unwrap();
        for handler in handlers {
            handler.await.unwrap();
        }
    }

    /// A client that finishes its request body and THEN cancels the stream
    /// (dropping the response is what a browser navigating away does, and it
    /// puts RST_STREAM CANCEL on the wire) must not turn the already-complete
    /// request into a read error. The unclassified trailer poll used to make
    /// it one: both proxy pumps convert it with `into_down()`, which fails the
    /// request with a spurious 400 AND closes and re-dials an otherwise
    /// healthy pooled upstream connection, once per client cancel.
    #[tokio::test]
    async fn test_client_cancel_after_body_eof_is_not_a_read_error() {
        let (client, server) = duplex(65536);
        let (body_read_tx, body_read_rx) = tokio::sync::oneshot::channel::<()>();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel::<()>();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut body) = h2
                .ready()
                .await
                .unwrap()
                .send_request(request, false)
                .unwrap();
            // A complete request body, END_STREAM included.
            body.send_data("payload".into(), true).unwrap();

            // Only cancel once the server has the body, so the cancel is
            // unambiguously post-EOF rather than a torn upload.
            body_read_rx.await.unwrap();
            drop(response);
            drop(body);
            // Let the connection task put the RST_STREAM on the wire.
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancelled_tx.send(()).unwrap();
            // Keep the connection open until the server side has made its
            // observation, so the test never races the connection teardown.
            let _ = done_rx.await;
        });

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let mut handlers = Vec::new();
        let mut signals = Some((body_read_tx, cancelled_rx, done_tx));
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let (body_read_tx, cancelled_rx, done_tx) = signals.take().expect("exactly one stream");
            handlers.push(tokio::spawn(async move {
                let data = http.read_body_bytes().await.unwrap().unwrap();
                assert_eq!(data, "payload");
                assert!(http.is_body_done());

                body_read_tx.send(()).unwrap();
                cancelled_rx.await.unwrap();

                // The EOF read (which polls trailers) must stay Ok: the
                // request was received in full and the client's cancel says
                // nothing about the bytes we already have.
                assert!(
                    http.read_body_bytes().await.unwrap().is_none(),
                    "a post-EOF client cancel must not surface as a read error"
                );
                assert!(!http.request_trailers_present());
                let _ = done_tx.send(());
            }));
        }
        client.await.unwrap();
        for handler in handlers {
            handler.await.unwrap();
        }
    }

    /// Drive a client that sends `declared` as its `content-length`, puts
    /// `payload` on the wire (with END_STREAM iff `end_stream`) and then cancels
    /// the stream -- all BEFORE the server reads anything.
    ///
    /// This is the natural wire ordering, and it is the one the latch exists
    /// for: no oneshot forces the reset to happen after the reader observed EOF,
    /// so by the time the server polls, `h2` has already overwritten the stream
    /// state and `RecvStream::is_end_stream()` reports `false` even though the
    /// END_STREAM-bearing DATA frame is still queued for delivery.
    ///
    /// The sleep between the write and the cancel is required, not cosmetic:
    /// `h2`'s `send_reset` CLEARS the stream's pending send queue, so cancelling
    /// immediately would drop the DATA frame the test is about.
    async fn cancel_after_write_client(
        client: DuplexStream,
        declared: &'static str,
        payload: &'static str,
        end_stream: bool,
    ) {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .header("content-length", declared)
            .body(())
            .unwrap();
        let (response, mut body) = h2
            .ready()
            .await
            .unwrap()
            .send_request(request, false)
            .unwrap();
        body.send_data(payload.into(), end_stream).unwrap();
        // Let the connection task flush the DATA frame before the cancel
        // discards whatever is still queued.
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(response);
        drop(body);
        // Keep the connection alive long enough for the reset to be written and
        // for the server to make its observation.
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    /// A complete request body followed by a client cancel, in the NATURAL wire
    /// ordering: both frames land before the server polls, so the END_STREAM
    /// evidence is destroyed before it can be latched and only the declared
    /// `content-length` can still prove the body whole.
    ///
    /// Without that source the read fails, and both proxy pumps convert it with
    /// `into_down()`: a spurious 400 for a request the proxy received in full,
    /// plus a closed and re-dialled upstream connection, once per client cancel.
    #[tokio::test]
    async fn test_complete_body_then_reset_before_any_read_is_a_clean_eof() {
        let (client, server) = duplex(65536);
        let client = tokio::spawn(cancel_after_write_client(client, "7", "payload", true));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let mut handlers = Vec::new();
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handlers.push(tokio::spawn(async move {
                // Nothing is read until both the DATA and the RST_STREAM have
                // been processed by the connection above.
                tokio::time::sleep(Duration::from_millis(600)).await;

                let data = http.read_body_bytes().await.unwrap().unwrap();
                assert_eq!(data, "payload");
                assert!(
                    http.read_body_bytes().await.unwrap().is_none(),
                    "a fully received content-length body must read as a clean EOF \
                     even when the client cancelled before we polled"
                );
                assert!(http.is_body_done());
                assert_eq!(http.body_bytes_read(), 7);
            }));
        }
        client.await.unwrap();
        for handler in handlers {
            handler.await.unwrap();
        }
    }

    /// The mirror image, and the security-relevant half: the same cancel after a
    /// PARTIAL body must stay a read error. Classifying it as a clean EOF would
    /// let the pumps forward a truncated request body upstream as if the client
    /// had sent it in full.
    #[tokio::test]
    async fn test_mid_body_reset_is_still_a_read_error() {
        let (client, server) = duplex(65536);
        // 20 declared, 4 delivered: the body is provably incomplete.
        let client = tokio::spawn(cancel_after_write_client(client, "20", "payl", false));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let mut handlers = Vec::new();
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handlers.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(600)).await;

                let data = http.read_body_bytes().await.unwrap().unwrap();
                assert_eq!(data, "payl");
                assert!(!http.is_body_done());

                let err = http
                    .read_body_bytes()
                    .await
                    .expect_err("a truncated request body must not read as a clean EOF");
                assert_eq!(err.etype(), &ErrorType::ReadError);
                assert!(!http.is_body_done());
            }));
        }
        client.await.unwrap();
        for handler in handlers {
            handler.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_early_body_buffer_rejected_for_connect_h2() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            // Plain CONNECT: authority-form URI, no END_STREAM (the stream is a tunnel)
            let request = Request::builder()
                .method(Method::CONNECT)
                .uri("www.example.com:443")
                .body(())
                .unwrap();
            let (response, _req_body) = h2.send_request(request, false).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
                // The tunnel stream must never be captured: registration fails closed.
                assert!(http
                    .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                    .is_err());
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_early_body_buffer_rejected_when_retry_buffering_enabled_h2() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("abc".into(), true).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
                // Double-send defense: with the native retry buffer enabled the
                // drained body would be teed into it AND replayed by the app
                // buffer, so registration must fail closed.
                http.enable_retry_buffering();
                assert!(http
                    .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                    .is_err());
                while http.read_body_bytes().await.unwrap().is_some() {}
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_early_body_buffer_not_registered_h2() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("abc".into(), true).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                while http.read_body_bytes().await.unwrap().is_some() {}
                assert!(!http.begin_request_body_replay().await.unwrap());
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_early_body_buffer_rewind_from_mid_replay_h2() {
        use crate::protocols::http::body_buffer::RequestBodyBuffer;
        use async_trait::async_trait;
        use bytes::BytesMut;

        /// Like a disk-spill impl: awaits (yields) inside next_chunk, opening
        /// a cancellation window mid-replay. 2-byte replay chunks so a small
        /// test body still replays in multiple chunks, like a large body
        /// against the real 64 KiB replay chunk size.
        struct SmallChunkYieldingBuffer {
            buf: BytesMut,
            body: Option<Bytes>,
            offset: usize,
        }

        #[async_trait]
        impl RequestBodyBuffer for SmallChunkYieldingBuffer {
            async fn write(&mut self, data: &Bytes) -> Result<()> {
                self.buf.extend_from_slice(data);
                Ok(())
            }

            async fn finish(&mut self) -> Result<()> {
                if self.body.is_none() {
                    self.body = Some(self.buf.split().freeze());
                }
                Ok(())
            }

            async fn rewind(&mut self) -> Result<()> {
                self.offset = 0;
                Ok(())
            }

            async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
                tokio::task::yield_now().await;
                let Some(body) = self.body.as_ref() else {
                    return Ok(None);
                };
                if self.offset >= body.len() {
                    return Ok(None);
                }
                let end = self.offset.saturating_add(max_bytes.min(2)).min(body.len());
                Ok(Some(body.slice(self.offset..end)))
            }

            fn consume(&mut self, bytes: usize) {
                self.offset = self.offset.saturating_add(bytes);
            }
        }

        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("abcdef".into(), true).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                http.set_request_body_buffer(Box::new(SmallChunkYieldingBuffer {
                    buf: BytesMut::new(),
                    body: None,
                    offset: 0,
                }))
                .unwrap();
                let mut total = Vec::new();
                while let Some(chunk) = http.read_body_bytes().await.unwrap() {
                    total.extend_from_slice(&chunk);
                }
                assert_eq!(total, b"abcdef");

                // Attempt 1 fails mid-replay: one chunk was delivered (its
                // commit still pending) when the retry rewinds from the
                // Replaying state.
                assert!(http.begin_request_body_replay().await.unwrap());
                assert_eq!(
                    http.read_body_or_idle(false).await.unwrap().unwrap(),
                    b"ab".as_slice()
                );
                assert!(http.begin_request_body_replay().await.unwrap());
                let mut replayed = Vec::new();
                while let Some(chunk) = http.read_body_or_idle(false).await.unwrap() {
                    replayed.extend_from_slice(&chunk);
                }
                assert_eq!(replayed, b"abcdef");

                // Attempt 2 fails with a read in flight: the read is
                // cancelled mid-poll (losing select! branch) before the retry
                // rewinds from the Replaying state.
                assert!(http.begin_request_body_replay().await.unwrap());
                assert_eq!(
                    http.read_body_or_idle(false).await.unwrap().unwrap(),
                    b"ab".as_slice()
                );
                {
                    let mut fut = tokio_test::task::spawn(http.read_body_or_idle(false));
                    assert!(fut.poll().is_pending());
                }
                assert!(http.begin_request_body_replay().await.unwrap());
                let mut replayed = Vec::new();
                while let Some(chunk) = http.read_body_or_idle(false).await.unwrap() {
                    replayed.extend_from_slice(&chunk);
                }
                assert_eq!(replayed, b"abcdef");

                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    /// `Content-Length: 0` without END_STREAM on HEADERS: `is_body_done()` is
    /// false (the transport fact), but the request promises zero DATA bytes,
    /// so draining must return immediately instead of awaiting an EOS that
    /// the client only sends later (or never).
    #[tokio::test]
    async fn test_drain_returns_immediately_for_cl0_without_end_stream() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .header("content-length", "0")
                .body(())
                .unwrap();
            // No END_STREAM on HEADERS, and this client never sends one.
            let (response, _req_body) = h2.send_request(request, false).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                assert!(!http.is_body_done());
                assert!(http.is_body_empty());
                // No total_drain_timeout is set: an unbounded drain would hang here.
                timeout(Duration::from_secs(5), http.drain_request_body())
                    .await
                    .expect("drain of a CL:0 request must not await the transport EOS")
                    .unwrap();
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_drain_discards_buffer_and_poisons_replay_h2() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("abc".into(), true).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
                http.set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                    .unwrap();
                http.drain_request_body().await.unwrap();
                // The registered buffer was discarded with the body: replay must
                // fail closed rather than report "no buffer registered" and let
                // the proxy forward a request whose body is gone.
                assert!(http.begin_request_body_replay().await.is_err());
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    /// Delegates to `InMemoryRequestBodyBuffer` and reports its own drop, so
    /// tests can pin down exactly when the session releases the buffer.
    struct DropProbeBuffer {
        inner: crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl DropProbeBuffer {
        fn new() -> (Self, Arc<std::sync::atomic::AtomicBool>) {
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            (
                DropProbeBuffer {
                    inner: crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer::new(),
                    dropped: dropped.clone(),
                },
                dropped,
            )
        }
    }

    impl Drop for DropProbeBuffer {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl crate::protocols::http::body_buffer::RequestBodyBuffer for DropProbeBuffer {
        async fn write(&mut self, data: &Bytes) -> Result<()> {
            self.inner.write(data).await
        }

        async fn finish(&mut self) -> Result<()> {
            self.inner.finish().await
        }

        async fn rewind(&mut self) -> Result<()> {
            self.inner.rewind().await
        }

        async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
            self.inner.next_chunk(max_bytes).await
        }

        fn consume(&mut self, bytes: usize) {
            self.inner.consume(bytes)
        }
    }

    fn probe_dropped(flag: &Arc<std::sync::atomic::AtomicBool>) -> bool {
        flag.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test]
    async fn test_buffer_released_when_response_commits_after_replay_h2() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("abc".into(), true).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                let (probe, dropped) = DropProbeBuffer::new();
                http.set_request_body_buffer(Box::new(probe)).unwrap();
                while http.read_body_bytes().await.unwrap().is_some() {}
                assert!(http.begin_request_body_replay().await.unwrap());
                assert_eq!(
                    http.read_body_or_idle(false).await.unwrap().unwrap(),
                    b"abc".as_slice()
                );
                assert!(http.read_body_or_idle(false).await.unwrap().is_none());
                // Replay is done but no response committed yet: a retry could
                // still rewind and replay, so the buffer must survive.
                assert!(!probe_dropped(&dropped));
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
                // Committing the response releases the buffer immediately.
                assert!(probe_dropped(&dropped));
                assert!(!http.request_body_buffer_registered());
                // A replay attempt after release must fail closed, not silently
                // proxy a bodyless request.
                assert!(http.begin_request_body_replay().await.is_err());
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_buffer_released_at_replay_eof_when_response_committed_first_h2() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("abc".into(), true).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                let (probe, dropped) = DropProbeBuffer::new();
                http.set_request_body_buffer(Box::new(probe)).unwrap();
                while http.read_body_bytes().await.unwrap().is_some() {}
                assert!(http.begin_request_body_replay().await.unwrap());
                assert_eq!(
                    http.read_body_or_idle(false).await.unwrap().unwrap(),
                    b"abc".as_slice()
                );
                // Early upstream response: the header commits downstream while
                // replay is still in flight — the buffer must survive until
                // replay EOF.
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
                assert!(!probe_dropped(&dropped));
                assert!(http.read_body_or_idle(false).await.unwrap().is_none());
                assert!(probe_dropped(&dropped));
                assert!(http.begin_request_body_replay().await.is_err());
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_zero_payload_body_stays_non_empty_while_registered_h2() {
        use crate::protocols::http::body_buffer::RequestBodyBuffer;

        /// Ignores captured data and replays a fixed non-empty body, like an
        /// app rewriting a zero-byte payload.
        #[derive(Default)]
        struct RewriteToNonEmptyBuffer {
            offset: usize,
        }

        const REWRITTEN_BODY: &[u8] = b"rewritten";

        #[async_trait::async_trait]
        impl RequestBodyBuffer for RewriteToNonEmptyBuffer {
            async fn write(&mut self, _data: &Bytes) -> Result<()> {
                Ok(())
            }

            async fn finish(&mut self) -> Result<()> {
                Ok(())
            }

            async fn rewind(&mut self) -> Result<()> {
                self.offset = 0;
                Ok(())
            }

            async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
                if self.offset >= REWRITTEN_BODY.len() {
                    return Ok(None);
                }
                let end = self
                    .offset
                    .saturating_add(max_bytes)
                    .min(REWRITTEN_BODY.len());
                Ok(Some(Bytes::from_static(&REWRITTEN_BODY[self.offset..end])))
            }

            fn consume(&mut self, bytes: usize) {
                self.offset = self.offset.saturating_add(bytes);
            }
        }

        let (client, server) = duplex(65536);
        let mut handles = vec![];
        // Sequencing: the client sends its empty END_STREAM DATA frame only
        // after the server registered the buffer, so registration always sees
        // a stream whose emptiness is still unknown.
        let (registered_tx, registered_rx) = tokio::sync::oneshot::channel::<()>();

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap(); // no EOS
            registered_rx.await.unwrap();
            // A zero-byte payload whose framing permitted a body.
            req_body.send_data("".into(), true).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        let mut registered_tx = Some(registered_tx);
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let registered_tx = registered_tx.take().unwrap();
            handles.push(tokio::spawn(async move {
                http.set_request_body_buffer(Box::new(RewriteToNonEmptyBuffer::default()))
                    .unwrap();
                assert!(!http.is_body_empty());
                registered_tx.send(()).unwrap();
                // Drain: the client now ends the stream with an empty DATA
                // frame, so zero bytes are captured.
                while let Some(chunk) = http.read_body_bytes().await.unwrap() {
                    assert!(chunk.is_empty());
                }
                // Zero bytes were captured and END_STREAM was received, but the
                // registered buffer may rewrite the body: the emptiness
                // decision proxy_h2 derives END_STREAM-on-HEADERS from must
                // keep tracking the replay source instead of the original
                // payload.
                assert!(!http.is_body_empty());
                assert!(http.begin_request_body_replay().await.unwrap());
                assert!(!http.is_body_empty());
                assert_eq!(
                    http.read_body_or_idle(false).await.unwrap().unwrap(),
                    REWRITTEN_BODY
                );
                assert!(http.read_body_or_idle(false).await.unwrap().is_none());
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn finalized_replay_buffer_injects_body_into_empty_h2_request() {
        use crate::protocols::http::body_buffer::RequestBodyBuffer;

        #[derive(Default)]
        struct InjectedBody {
            offset: usize,
        }

        const BODY: &[u8] = b"injected";

        #[async_trait::async_trait]
        impl RequestBodyBuffer for InjectedBody {
            async fn write(&mut self, _data: &Bytes) -> Result<()> {
                Ok(())
            }

            async fn finish(&mut self) -> Result<()> {
                Ok(())
            }

            async fn rewind(&mut self) -> Result<()> {
                self.offset = 0;
                Ok(())
            }

            async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
                if self.offset >= BODY.len() {
                    return Ok(None);
                }
                let end = self.offset.saturating_add(max_bytes).min(BODY.len());
                Ok(Some(Bytes::from_static(&BODY[self.offset..end])))
            }

            fn consume(&mut self, bytes: usize) {
                self.offset = self.offset.saturating_add(bytes);
            }
        }

        let (client, server) = duplex(65536);
        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, _) = h2.send_request(request, true).unwrap();
            assert_eq!(response.await.unwrap().status(), http::StatusCode::OK);
        });

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let mut server_tasks = Vec::new();
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            server_tasks.push(tokio::spawn(async move {
                assert!(http.is_body_empty());
                http.set_bodyless_request_replay_buffer(Box::new(InjectedBody::default()))
                    .unwrap();
                assert!(!http.is_body_empty());
                assert!(http.begin_request_body_replay().await.unwrap());
                assert_eq!(http.read_body_or_idle(false).await.unwrap().unwrap(), BODY);
                assert!(http.read_body_or_idle(false).await.unwrap().is_none());
                let response = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response, true).unwrap();
            }));
        }

        client.await.unwrap();
        for task in server_tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn bodyless_replay_buffer_rejected_while_downstream_stream_open_h2() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();

            // Stream 1: `Content-Length: 0` but no END_STREAM on HEADERS — the
            // downstream stream is still open even though the body is declared
            // empty.
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .header("content-length", "0")
                .body(())
                .unwrap();
            let (response, _req_body) = h2.send_request(request, false).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);

            // Stream 2: no Content-Length and no END_STREAM — the body may be
            // non-empty.
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, _req_body2) = h2.send_request(request, false).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
                // Replay would permanently shadow the still-open downstream
                // stream (empty DATA + END_STREAM, trailers, or violating DATA
                // would never be observed), so registration fails closed.
                assert!(http
                    .set_bodyless_request_replay_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                    .is_err());
                assert!(!http.begin_request_body_replay().await.unwrap());
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_cancelled_capture_poisons_h2_session() {
        use crate::protocols::http::body_buffer::RequestBodyBuffer;
        use std::future::Future;
        use std::sync::atomic::{AtomicBool, Ordering};

        /// A capture impl that flags entry into `write()` and then never
        /// completes, opening a deterministic cancellation window after the
        /// chunk has been consumed from the stream.
        struct PendingCaptureBuffer {
            entered: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl RequestBodyBuffer for PendingCaptureBuffer {
            async fn write(&mut self, _data: &Bytes) -> Result<()> {
                self.entered.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok(())
            }

            async fn finish(&mut self) -> Result<()> {
                Ok(())
            }

            async fn rewind(&mut self) -> Result<()> {
                Ok(())
            }

            async fn next_chunk(&mut self, _max_bytes: usize) -> Result<Option<Bytes>> {
                Ok(None)
            }

            fn consume(&mut self, _bytes: usize) {}
        }

        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("abc".into(), true).unwrap();
            // The poisoned server session errors out instead of responding.
            assert!(response.await.is_err());
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                let entered = Arc::new(AtomicBool::new(false));
                http.set_request_body_buffer(Box::new(PendingCaptureBuffer {
                    entered: entered.clone(),
                }))
                .unwrap();
                // Manually poll read_body_bytes until it has consumed the chunk
                // and suspended inside the buffer's write(), then drop the
                // future — modeling a losing select!/timeout branch cancelling
                // the read mid-capture.
                {
                    let mut fut = Box::pin(http.read_body_bytes());
                    let waker = futures::task::noop_waker();
                    loop {
                        // Scope the Context to a single poll: it is not Send and
                        // must not live across the yield await below.
                        let pending = {
                            let mut poll_ctx = std::task::Context::from_waker(&waker);
                            fut.as_mut().poll(&mut poll_ctx).is_pending()
                        };
                        assert!(pending);
                        if entered.load(Ordering::SeqCst) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }
                // The chunk is gone from both the app's view and the buffer:
                // the session must fail closed on any further read or replay.
                assert!(http.read_body_bytes().await.is_err());
                assert!(http.begin_request_body_replay().await.is_err());
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    // NOTE on wall-clock in the read-timeout tests below: `tokio::time::pause()`
    // cannot drive them. The bound is armed with `pingora_timeout::timeout`,
    // whose fast timer runs on a dedicated OS thread against the real clock
    // (`pingora_timeout::fast_timeout::TIMER_MANAGER`), so paused/auto-advanced
    // tokio time never fires it and every one of these tests would hang. The
    // durations are therefore kept small, and every "must fire" assertion is
    // wrapped in an outer bound that is orders of magnitude larger than the
    // timeout under test, so load can slow these tests down without failing
    // them.
    const TEST_STALL_GRACE: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn test_read_body_timeout_releases_stalled_upload() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];
        // The client stalls until the server has observed the timeout, so the
        // release is provably the read timeout firing and not this task
        // dropping the stream -- without paying a fixed wall-clock sleep.
        let (released_tx, released_rx) = tokio::sync::oneshot::channel::<()>();

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                // The server errors the session on timeout, so the connection
                // future may resolve with an error; either way is fine here.
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (_response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("first chunk".into(), false).unwrap();
            // Stall: keep the request-body half open, never send another byte.
            let _ = released_rx.await;
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        let mut released_tx = Some(released_tx);
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let released_tx = released_tx.take().unwrap();
            handles.push(tokio::spawn(async move {
                http.set_read_timeout(Some(Duration::from_millis(300)));

                let body = http.read_body_or_idle(false).await.unwrap().unwrap();
                assert_eq!(body, "first chunk");

                // The stalled second read must be released by the read timeout
                // instead of pinning this task forever. The outer bound only
                // distinguishes "timeout fired" from "test hung": it is far
                // above the read timeout and never expires on a healthy run.
                let err = tokio::time::timeout(TEST_STALL_GRACE, http.read_body_or_idle(false))
                    .await
                    .expect("read_timeout should have released the stalled read")
                    .unwrap_err();
                assert_eq!(err.etype(), &ErrorType::ReadTimedout);
                let _ = released_tx.send(());
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    /// The idle bound must be a DEADLINE carried by the session, not a duration
    /// rebuilt per call.
    ///
    /// Every caller polls the body read from a `tokio::select!` branch, and any
    /// other ready branch -- an upstream response chunk, a cache task, a custom
    /// message -- cancels it. Here a ticker fires several times per read
    /// timeout, standing in for an upstream that emits at least one event per
    /// period (SSE, gRPC server-streaming, a slow multi-chunk 200). With a
    /// per-call duration the bound is rearmed by every cancellation and the
    /// stalled client pins this pump, its downstream stream and its upstream
    /// connection forever: the DoS the bound exists to close.
    #[tokio::test]
    async fn test_read_body_timeout_survives_select_cancellation() {
        const READ_TIMEOUT: Duration = Duration::from_millis(300);
        // Well under the read timeout: the read is cancelled and rebuilt
        // several times before the deadline is reached.
        const CHATTY_UPSTREAM_TICK: Duration = Duration::from_millis(60);

        let (client, server) = duplex(65536);
        let mut handles = vec![];
        let (released_tx, released_rx) = tokio::sync::oneshot::channel::<()>();

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (_response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("first chunk".into(), false).unwrap();
            // Stall forever; only the server's deadline can end this.
            let _ = released_rx.await;
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        let mut released_tx = Some(released_tx);
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let released_tx = released_tx.take().unwrap();
            handles.push(tokio::spawn(async move {
                http.set_read_timeout(Some(READ_TIMEOUT));

                let body = http.read_body_or_idle(false).await.unwrap().unwrap();
                assert_eq!(body, "first chunk");

                let mut cancellations = 0u32;
                let result = tokio::time::timeout(TEST_STALL_GRACE, async {
                    loop {
                        tokio::select! {
                            body = http.read_body_or_idle(false) => break body,
                            _ = tokio::time::sleep(CHATTY_UPSTREAM_TICK) => {
                                cancellations += 1;
                            }
                        }
                    }
                })
                .await
                .expect(
                    "an unrelated ready select! branch must not rearm the downstream \
                     request-body idle bound",
                );

                assert_eq!(result.unwrap_err().etype(), &ErrorType::ReadTimedout);
                assert!(
                    cancellations >= 2,
                    "the competing branch must have cancelled the read at least twice \
                     for this to test anything; saw {cancellations}"
                );
                let _ = released_tx.send(());
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    /// An empty DATA frame is not progress and must not rearm the bound.
    ///
    /// A zero-length DATA frame without END_STREAM is legal, costs the peer 9
    /// bytes, consumes no flow-control window and trips no h2 flood counter, so
    /// treating it as a received chunk would hand an attacker an unlimited
    /// rearm at ~zero cost -- and because `body_read` never advances, no
    /// byte-count body-size limit would catch it either.
    #[tokio::test]
    async fn test_empty_data_frames_do_not_rearm_read_timeout() {
        const READ_TIMEOUT: Duration = Duration::from_millis(300);
        const TRICKLE_GAP: Duration = Duration::from_millis(80);

        let (client, server) = duplex(65536);
        let mut handles = vec![];
        let (released_tx, mut released_rx) = tokio::sync::oneshot::channel::<()>();
        let empty_frames_sent = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let client_empty_frames_sent = empty_frames_sent.clone();

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (_response, mut req_body) = h2.send_request(request, false).unwrap();
            req_body.send_data("first chunk".into(), false).unwrap();
            // Then nothing but empty DATA frames, forever.
            loop {
                tokio::select! {
                    _ = &mut released_rx => break,
                    _ = tokio::time::sleep(TRICKLE_GAP) => {
                        if req_body.send_data(Bytes::new(), false).is_err() {
                            break;
                        }
                        client_empty_frames_sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        let mut released_tx = Some(released_tx);
        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let released_tx = released_tx.take().unwrap();
            let empty_frames_sent = empty_frames_sent.clone();
            handles.push(tokio::spawn(async move {
                http.set_read_timeout(Some(READ_TIMEOUT));

                let body = http.read_body_or_idle(false).await.unwrap().unwrap();
                assert_eq!(body, "first chunk");

                let err = tokio::time::timeout(TEST_STALL_GRACE, async {
                    loop {
                        match http.read_body_or_idle(false).await {
                            Ok(Some(chunk)) => {
                                assert!(chunk.is_empty(), "the client only sends empty frames");
                            }
                            Ok(None) => panic!("the client never ends the stream"),
                            Err(e) => break e,
                        }
                    }
                })
                .await
                .expect("empty DATA frames must not rearm the request-body idle bound");

                assert_eq!(err.etype(), &ErrorType::ReadTimedout);
                assert!(
                    empty_frames_sent.load(std::sync::atomic::Ordering::SeqCst) >= 2,
                    "the client must have sent at least two empty frames for this to test anything"
                );
                // The bound fired without a single body byte having been added.
                assert_eq!(http.body_bytes_read(), "first chunk".len());
                let _ = released_tx.send(());
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    /// A CONNECT tunnel's "request body" is its uplink, on which long idle
    /// periods are ordinary (an idle SSH session; a WebSocket over extended
    /// CONNECT whose traffic runs server-to-client only). The bound must not
    /// apply to it.
    #[tokio::test]
    async fn test_read_body_timeout_spares_connect_tunnel() {
        const READ_TIMEOUT: Duration = Duration::from_millis(100);
        // Several times the read timeout: enough for the bound to have fired.
        const IDLE_UPLINK: Duration = Duration::from_millis(500);

        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            // Plain CONNECT: authority-form URI, no END_STREAM (it is a tunnel).
            let request = Request::builder()
                .method(Method::CONNECT)
                .uri("www.example.com:443")
                .body(())
                .unwrap();
            let (response, _req_body) = h2.send_request(request, false).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                http.set_read_timeout(Some(READ_TIMEOUT));

                tokio::select! {
                    read = http.read_body_or_idle(false) => {
                        panic!(
                            "an idle CONNECT uplink must not be cut by the request-body \
                             read timeout, got {read:?}"
                        );
                    }
                    _ = tokio::time::sleep(IDLE_UPLINK) => {}
                }

                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    /// `Content-Length: 0` without END_STREAM is legal on H2 (design 4.3): the
    /// transport has promised there are no body bytes, but the client owes an
    /// END_STREAM it may take arbitrarily long to send. Failing such a request
    /// with a read timeout would discard an exchange that has nothing left to
    /// read, so the bound finishes the read side instead of erroring.
    #[tokio::test]
    async fn test_read_body_timeout_finishes_declared_empty_body() {
        const READ_TIMEOUT: Duration = Duration::from_millis(200);

        let (client, server) = duplex(65536);
        let mut handles = vec![];

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .header("content-length", "0")
                .body(())
                .unwrap();
            // No END_STREAM, and the client never sends one.
            let (response, _req_body) = h2.send_request(request, false).unwrap();
            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                // Parity with the HTTP/1 session: the bound is on by default,
                // because nothing in pingora-core or pingora-proxy sets one.
                assert_eq!(http.get_read_timeout(), Some(Duration::from_secs(60)));
                http.set_read_timeout(Some(READ_TIMEOUT));
                assert!(http.is_body_empty());
                assert!(!http.is_body_done());

                let read = tokio::time::timeout(TEST_STALL_GRACE, http.read_body_or_idle(false))
                    .await
                    .expect("the read side must be finished by the bound, not left pending")
                    .expect("a provably empty body must not be failed by the read timeout");
                assert!(read.is_none());
                // The read side is finished for good: no second timeout, and no
                // idle-path error on a later poll.
                assert!(http.is_body_done());

                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_read_body_timeout_spares_progressing_upload() {
        let (client, server) = duplex(65536);
        let mut handles = vec![];

        // Inter-chunk gaps stay well under the read timeout while the total
        // upload duration exceeds it: the bound must be rearmed per chunk.
        const CHUNKS: usize = 8;
        const GAP: Duration = Duration::from_millis(100);
        const READ_TIMEOUT: Duration = Duration::from_millis(500);

        handles.push(tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap();
            });
            let mut h2 = h2.ready().await.unwrap();

            let request = Request::builder()
                .method(Method::POST)
                .uri("https://www.example.com/")
                .body(())
                .unwrap();
            let (response, mut req_body) = h2.send_request(request, false).unwrap();
            for i in 0..CHUNKS {
                tokio::time::sleep(GAP).await;
                let eos = i == CHUNKS - 1;
                req_body.send_data("chunk".into(), eos).unwrap();
            }

            let (head, _body) = response.await.unwrap().into_parts();
            assert_eq!(head.status, 200);
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                http.set_read_timeout(Some(READ_TIMEOUT));

                // Stop on `is_body_done()` rather than on a `None` read: the
                // final chunk carries END_STREAM, so one more read after it
                // would take the idle path and pend until the client closes --
                // and the client is waiting for our response. The outer
                // per-read bound only distinguishes "spared" from "test hung";
                // it never expires on a healthy run.
                let mut total = 0;
                while !http.is_body_done() {
                    let chunk =
                        tokio::time::timeout(TEST_STALL_GRACE, http.read_body_or_idle(false))
                            .await
                            .expect("a progressing upload must not be starved by the read timeout")
                            .unwrap()
                            .unwrap();
                    total += chunk.len();
                }
                assert_eq!(total, CHUNKS * "chunk".len());

                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                http.write_response_header(response_header, true).unwrap();
            }));
        }

        for handle in handles {
            assert!(handle.await.is_ok());
        }
    }
}
