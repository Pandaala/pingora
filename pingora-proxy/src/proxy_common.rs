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

use crate::{Session, UpstreamRequestBodyDisposition, UpstreamResponseBodyEvent};
use bytes::Bytes;
use http::{
    header::{self, HeaderName},
    HeaderMap, Method,
};
use log::{debug, warn};
use pingora_cache::NoCacheReason;
use pingora_core::protocols::http::HttpTask;
use pingora_core::upstreams::peer::{H1UpgradePolicy, HttpUpstreamRequestPolicy};
use pingora_error::{
    BError, Error,
    ErrorType::{InternalError, InvalidHTTPHeader},
    Result,
};
use pingora_http::RequestHeader;

const MAX_CONNECTION_NOMINATIONS: usize = 10;
pub(crate) const KEEP_ALIVE: &str = "keep-alive";
pub(crate) const PROXY_CONNECTION: &str = "proxy-connection";
pub(crate) const HTTP2_SETTINGS: &str = "http2-settings";

/// Whether `byte` is a `tchar`, the character set of an HTTP `token` (RFC 9110 §5.6.2). Checked
/// here because `HeaderName::from_bytes` may accept bytes outside the `token` set.
fn is_tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn is_websocket_upgrade_request(req: &RequestHeader, downstream_is_http11: bool) -> bool {
    downstream_is_http11
        && req
            .headers
            .get(header::UPGRADE)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
}

struct ConnectionNominations {
    headers: [Option<HeaderName>; MAX_CONNECTION_NOMINATIONS],
    len: usize,
}

impl ConnectionNominations {
    fn parse(req: &RequestHeader, reject_malformed: bool) -> Result<Self> {
        let mut headers = std::array::from_fn(|_| None);
        let mut len = 0;
        let mut nomination_count = 0;

        // This is inspired by Envoy's defensive Connection-header sanitization checks. Bound the
        // amount of token processing so it cannot become a request-time DoS vector.
        for token in req
            .headers
            .get_all(header::CONNECTION)
            .iter()
            .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
            .map(|token| token.trim_ascii())
            .filter(|token| !token.is_empty())
        {
            nomination_count += 1;
            if nomination_count >= MAX_CONNECTION_NOMINATIONS {
                return Error::e_explain(
                    InvalidHTTPHeader,
                    "too many Connection header nominations",
                );
            }

            // `:`-prefixed tokens nominate pseudo-headers (e.g. `:authority`); rejected in both modes.
            if token.starts_with(b":") {
                return Error::e_explain(
                    InvalidHTTPHeader,
                    "protected header cannot be nominated by the Connection header",
                );
            }

            // A nomination is an HTTP `token` (RFC 9110 §5.6.2). We validate that ourselves rather
            // than trust `HeaderName::from_bytes`, which may accept non-`token` bytes and let a
            // decorated spelling like `Connection: "X-Forwarded-For"` slip past the protected-name
            // check below. The RFC lets a recipient reject or ignore a malformed option, so
            // `reject_malformed` is a policy choice: fail closed (default) or tolerate it.
            if reject_malformed && !token.iter().all(|&byte| is_tchar(byte)) {
                return Error::e_explain(
                    InvalidHTTPHeader,
                    "invalid token nominated by the Connection header",
                );
            }

            // `HeaderName` lowercases, so the protected-set check below cannot be evaded via casing.
            let name = match HeaderName::from_bytes(token) {
                Ok(name) => name,
                // Strict mode: every token is valid `tchar` and parses; a residual failure (e.g. a
                // length limit) still fails closed. Lenient mode: an unparsable token names
                // nothing, so ignore it.
                Err(_) if reject_malformed => {
                    return Error::e_explain(
                        InvalidHTTPHeader,
                        "invalid token nominated by the Connection header",
                    );
                }
                Err(_) => continue,
            };

            if matches!(
                name.as_str(),
                "host" | "x-forwarded-for" | "x-forwarded-host" | "x-forwarded-proto"
            ) {
                return Error::e_explain(
                    InvalidHTTPHeader,
                    "protected header cannot be nominated by the Connection header",
                );
            }

            headers[len] = Some(name);
            len += 1;
        }

        Ok(Self { headers, len })
    }

    fn remove_from(self, req: &mut RequestHeader) {
        for name in self.headers.into_iter().take(self.len).flatten() {
            req.remove_header(&name);
        }
    }
}

fn strip_standard_hop_by_hop_headers(req: &mut RequestHeader) {
    req.remove_header(KEEP_ALIVE);
    req.remove_header(PROXY_CONNECTION);
    req.remove_header(&header::PROXY_AUTHENTICATE);
    req.remove_header(&header::PROXY_AUTHORIZATION);
    req.remove_header(&header::TE);
    req.remove_header(&header::TRAILER);
    req.remove_header(&header::TRANSFER_ENCODING);
    req.remove_header(&header::CONNECTION);
    req.remove_header(&header::UPGRADE);
    req.remove_header(HTTP2_SETTINGS);
}

/// Apply automatic request policy before application upstream request filtering.
pub(crate) fn sanitize_h1_upstream_request(
    req: &mut RequestHeader,
    policy: HttpUpstreamRequestPolicy,
    downstream_is_http11: bool,
) -> Result<()> {
    if policy == HttpUpstreamRequestPolicy::preserve() {
        return Ok(());
    }

    let nominations = policy
        .strip_connection_nominated
        .then(|| ConnectionNominations::parse(req, policy.reject_malformed_connection_nominations))
        .transpose()?;

    if policy.h1_upgrade == H1UpgradePolicy::Preserve && req.headers.contains_key(header::UPGRADE) {
        // An arbitrary upgrade may require any of the connection-nominated fields. Preserve the
        // complete request metadata rather than forwarding a partial handshake.
        return Ok(());
    }

    let websocket_upgrade = policy.h1_upgrade == H1UpgradePolicy::WebSocketOnly
        && is_websocket_upgrade_request(req, downstream_is_http11);
    if let Some(nominations) = nominations {
        nominations.remove_from(req);
    }

    if policy.strip_hop_by_hop {
        strip_standard_hop_by_hop_headers(req);
    }

    match policy.h1_upgrade {
        H1UpgradePolicy::WebSocketOnly => {
            req.remove_header(&header::CONNECTION);
            req.remove_header(&header::UPGRADE);
            req.remove_header(HTTP2_SETTINGS);
            if websocket_upgrade {
                req.insert_header(header::CONNECTION, "Upgrade")?;
                req.insert_header(header::UPGRADE, "websocket")?;
            }
        }
        H1UpgradePolicy::Deny => {
            req.remove_header(&header::CONNECTION);
            req.remove_header(&header::UPGRADE);
            req.remove_header(HTTP2_SETTINGS);
        }
        H1UpgradePolicy::Preserve => {}
    }

    Ok(())
}

/// Frame a body-bearing HTTP/1 upstream request after application request filtering.
pub(crate) fn finalize_h1_upstream_request_framing(
    req: &mut RequestHeader,
    downstream_has_body: bool,
) -> Result<()> {
    if downstream_has_body
        && req.headers.get(header::CONTENT_LENGTH).is_none()
        && req.headers.get(header::TRANSFER_ENCODING).is_none()
    {
        req.insert_header(header::TRANSFER_ENCODING, "chunked")?;
    }
    Ok(())
}

/// Remove downstream connection-nominated fields before an HTTP/2 conversion.
pub(crate) fn sanitize_h2_upstream_request(
    req: &mut RequestHeader,
    policy: HttpUpstreamRequestPolicy,
) -> Result<()> {
    if policy.strip_connection_nominated {
        ConnectionNominations::parse(req, policy.reject_malformed_connection_nominations)?
            .remove_from(req);
    }
    if policy.strip_hop_by_hop {
        strip_standard_hop_by_hop_headers(req);
    }
    Ok(())
}

/// Possible downstream states during request multiplexing
#[derive(Debug, Clone, Copy)]
pub(crate) enum DownstreamStateMachine {
    /// more request (body) to read
    Reading,
    /// no more data to read
    ReadingFinished,
    /// downstream is already errored or closed
    Errored,
}

#[allow(clippy::wrong_self_convention)]
impl DownstreamStateMachine {
    pub fn new(finished: bool) -> Self {
        if finished {
            Self::ReadingFinished
        } else {
            Self::Reading
        }
    }

    // Can call read() to read more data or wait on closing
    pub fn can_poll(&self) -> bool {
        !matches!(self, Self::Errored)
    }

    pub fn is_reading(&self) -> bool {
        matches!(self, Self::Reading)
    }

    pub fn is_done(&self) -> bool {
        !matches!(self, Self::Reading)
    }

    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Errored)
    }

    /// Move the state machine to Finished state if `set` is true.
    ///
    /// No-op when the current state is [`Errored`](Self::Errored) — once errored the
    /// downstream connection must not be reused, and late upstream chunks arriving
    /// via `rx.recv()` must not overwrite that decision.
    pub fn maybe_finished(&mut self, set: bool) {
        if set && !self.is_errored() {
            *self = Self::ReadingFinished
        }
    }

    /// Reset to [`Reading`](Self::Reading) for upgraded connections when body mode changes.
    ///
    /// No-op when the current state is [`Errored`](Self::Errored).
    pub fn reset(&mut self) {
        if !self.is_errored() {
            *self = Self::Reading;
        }
    }

    /// Transition to [`Errored`](Self::Errored). This is a terminal state: once entered,
    /// no other state transition is permitted and the connection must not be reused.
    pub fn to_errored(&mut self) {
        *self = Self::Errored
    }
}

/// Possible upstream states during request multiplexing
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResponseStateMachine {
    upstream_response_done: bool,
    cached_response_done: bool,
}

impl ResponseStateMachine {
    pub fn new() -> Self {
        ResponseStateMachine {
            upstream_response_done: false,
            cached_response_done: true, // no cached response by default
        }
    }

    pub fn is_done(&self) -> bool {
        self.upstream_response_done && self.cached_response_done
    }

    pub fn upstream_done(&self) -> bool {
        self.upstream_response_done
    }

    pub fn cached_done(&self) -> bool {
        self.cached_response_done
    }

    pub fn enable_cached_response(&mut self) {
        self.cached_response_done = false;
    }

    pub fn maybe_set_upstream_done(&mut self, done: bool) {
        if done {
            self.upstream_response_done = true;
        }
    }

    pub fn maybe_set_cache_done(&mut self, done: bool) {
        if done {
            self.cached_response_done = true;
        }
    }
}

/// Shared signal from the downstream proxy half to the upstream half: set to
/// [`DownstreamComplete`](Self::DownstreamComplete) right before the downstream
/// half returns successfully, so the upstream half can tell an expected pipe
/// closure (downstream finished the response by its own framing) from an
/// unexpected one.
///
/// Stored in an `AtomicU8` shared via `Arc`: the downstream half stores with
/// [`Release`](std::sync::atomic::Ordering::Release) and the upstream half loads
/// with [`Acquire`](std::sync::atomic::Ordering::Acquire), comparing against
/// `PipeState::DownstreamComplete as u8`.
#[derive(Debug)]
#[repr(u8)]
pub(crate) enum PipeState {
    Active = 0,
    DownstreamComplete = 1,
}

impl PipeState {
    /// Whether `raw` — a value previously read from the shared `AtomicU8` — is
    /// [`DownstreamComplete`](Self::DownstreamComplete). Centralizes the `as u8`
    /// comparison the upstream halves perform on a task-pipe closure.
    pub(crate) fn is_downstream_complete(raw: u8) -> bool {
        raw == PipeState::DownstreamComplete as u8
    }
}

/// Tracks whether the single terminal response-body lifecycle event has
/// already been delivered for this response.
///
/// `HttpProxy::upstream_filter` reaches the body filter only from a
/// `Body`/`UpgradedBody` task, so a response that terminates with a `Trailer`
/// or a bare `Done` would otherwise never deliver end-of-stream at all: on H2
/// the `END_STREAM` flag rides the trailers HEADERS frame, so every DATA frame
/// of a trailered response is emitted with `eos = false`. A body filter that
/// withholds bytes across callbacks until end-of-stream then never releases
/// them.
///
/// `Trailer` and the `Done` that follows it are two observations of ONE
/// termination, and a bare `Done` is a third; exactly one of them may dispatch.
/// Terminations that already carry end-of-stream through the ordinary path
/// (`Header(_, true)` via `terminal_upstream_body_filter`, `Body(_, true)`)
/// claim the latch without dispatching, so the trailing `Done` cannot deliver a
/// second one.
///
/// `Failed` claims WITHOUT dispatching: the response aborted, and synthesizing
/// end-of-stream for it would tell a filter that a truncated body was complete.
/// Claiming (rather than ignoring) is what stops a `Done` following the error
/// from doing exactly that.
///
/// Protocol-neutral on purpose: the H1, H2, and custom pumps share it.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TerminalBodyDispatch {
    claimed: bool,
    upgraded: bool,
}

impl TerminalBodyDispatch {
    /// Whether this response has carried upgraded body tasks.
    ///
    /// Bytes released by the terminal callback must be re-emitted under the
    /// response's own body variant: `Session::write_response_tasks` picks the
    /// raw post-upgrade duplex write path off the `UpgradedBody` tag, so
    /// tagging released bytes as plain `Body` would misroute them on an
    /// upgraded (e.g. WebSocket) connection. The terminal task itself is a
    /// `Trailer`/`Done` and carries no variant, hence the latch remembers it.
    pub fn is_upgraded(&self) -> bool {
        self.upgraded
    }

    /// Record that the final filtered response header completed the downstream
    /// Upgrade handshake. This must be called from the header path after
    /// `response_filter`: the upstream status alone is insufficient because
    /// the downstream request may not be an Upgrade and the filter may rewrite
    /// the response status before it reaches the writer.
    pub fn mark_upgraded(&mut self) {
        self.upgraded = true;
    }

    /// Record `task` as this response's terminal observation if nothing has
    /// claimed that role yet, and return its typed terminal body event.
    ///
    /// Returns an event at most once per response.
    pub fn claim_for(&mut self, task: &HttpTask) -> Option<UpstreamResponseBodyEvent> {
        self.upgraded |= matches!(task, HttpTask::UpgradedBody(..));
        match task {
            // Already delivers end-of-stream through the ordinary path: the
            // `terminal_header` branch runs `terminal_upstream_body_filter`,
            // and a terminal body task carries `eos = true` into
            // `upstream_response_body_filter` itself.
            HttpTask::Header(_, true)
            | HttpTask::Body(_, true)
            | HttpTask::UpgradedBody(_, true)
            // Aborted: must never be given a synthetic end-of-stream.
            | HttpTask::Failed(_) => {
                self.claimed = true;
                None
            }
            HttpTask::Trailer(trailers) => {
                if self.claimed {
                    None
                } else {
                    self.claimed = true;
                    Some(if trailers.is_some() {
                        UpstreamResponseBodyEvent::TerminalBeforeTrailers
                    } else {
                        UpstreamResponseBodyEvent::TerminalWithoutTrailers
                    })
                }
            }
            HttpTask::Done => {
                if self.claimed {
                    None
                } else {
                    self.claimed = true;
                    Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers)
                }
            }
            _ => None,
        }
    }
}

/// Canonicalize an emptied trailer map to the transport's trailer-free
/// terminal event. This lets an async application hook remove every field
/// without causing an empty trailer block to be forwarded.
pub(crate) fn normalize_trailers(trailers: Option<Box<HeaderMap>>) -> Option<Box<HeaderMap>> {
    trailers.filter(|trailers| !trailers.is_empty())
}

/// The request shapes on which a non-`Ordinary` disposition must not be
/// honored. Collected from the union of the DOWNSTREAM session and the
/// UPSTREAM request header, because `upstream_request_filter` runs first and
/// may have turned an ordinary downstream request into an upgrade/CONNECT
/// upstream request (or the other way around), while the rewrite the
/// disposition drives targets the upstream request.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DispositionFacts {
    /// Either side carries an `Upgrade:` header.
    pub is_upgrade_req: bool,
    /// Either side uses the CONNECT method.
    pub is_connect: bool,
    /// The downstream request has no body at all: empty AND already ended on
    /// the transport. Both facts are required -- `is_body_empty()` alone still
    /// infers emptiness from `Content-Length: 0`, which on H2 does not mean
    /// the request stream has ended (design 4.3).
    ///
    /// This fact must be one the CLIENT CANNOT RETRACT, because
    /// [`safe_disposition`] uses it to refuse re-framing a bodyless request --
    /// the guard against writing `Transfer-Encoding: chunked` and a `0\r\n\r\n`
    /// terminator for a plain `GET` onto a pooled upstream connection. Both
    /// halves are therefore read from the H2 session's LATCHED end-of-stream
    /// fact rather than from `h2`'s live `RecvStream::is_end_stream()`, which a
    /// peer flips back to `false` merely by resetting a stream it already ended
    /// (see `is_body_done`/`is_body_empty` in
    /// `pingora_core::protocols::http::v2::server`).
    ///
    /// Keying on the `request_headers_end_stream()` snapshot instead would fix
    /// only this one call site while `is_body_empty()`/`is_body_done()` stayed
    /// retractable for every other consumer, would need a fallback for the
    /// session types that cannot report it (`None` for subrequest and custom
    /// sessions), and would ignore a registered request-body replay buffer --
    /// which deliberately rewrites the effective body the upstream framing is
    /// built from. Fixing the two facts at their source covers all of that.
    pub body_empty: bool,
    /// The upstream request is still versioned below HTTP/1.1 (H1 pump only;
    /// the H2 pump always sends HTTP/2).
    pub upstream_below_http11: bool,
}

impl DispositionFacts {
    /// Collect the facts from the downstream session and the (already
    /// filtered) upstream request header.
    pub fn collect(session: &mut Session, upstream_request: &RequestHeader) -> Self {
        Self::union(
            session.is_upgrade_req(),
            session.req_header().method == Method::CONNECT,
            session.as_mut().is_body_empty() && session.as_mut().is_body_done(),
            upstream_request,
        )
    }

    /// The union itself, split out so it is testable without a live session.
    ///
    /// Both sides are consulted because `upstream_request_filter` has already
    /// run: an application can synthesize an upgrade or CONNECT upstream
    /// request from an ordinary downstream one (the downstream facts alone
    /// would miss it) or strip `Upgrade` from an upstream request whose
    /// downstream twin still has it. The disposition drives a rewrite of the
    /// UPSTREAM request, so a tunnel on either side disqualifies it.
    fn union(
        downstream_upgrade: bool,
        downstream_connect: bool,
        body_empty: bool,
        upstream_request: &RequestHeader,
    ) -> Self {
        DispositionFacts {
            is_upgrade_req: downstream_upgrade
                || pingora_core::protocols::http::v1::common::is_upgrade_req(upstream_request),
            is_connect: downstream_connect || upstream_request.method == Method::CONNECT,
            body_empty,
            // Set by the caller; the H1 pump is the only one that can send a
            // request below HTTP/1.1.
            upstream_below_http11: false,
        }
    }
}

/// Coerce a non-`Ordinary` disposition back to `Ordinary` on requests whose
/// framing must not be rewritten.
///
/// - An upgrade request (`Upgrade:` header, e.g. WebSocket) and a CONNECT
///   request both negotiate a tunnel: their message framing is fixed by the
///   protocol and the successful response switches the connection into
///   byte-stream mode. Re-framing such a request as `Transfer-Encoding:
///   chunked` (`Streamed`) or declaring it bodyless would corrupt the tunnel
///   -- e.g. the H1 `Streamed` prelude writing a `0\r\n\r\n` terminator before
///   the 101 ever arrives.
/// - A request with NO body must keep its ordinary framing. `Streamed` would
///   otherwise put `Transfer-Encoding: chunked` and a `0\r\n\r\n` terminator
///   on e.g. a plain `GET` sent over a POOLED upstream connection: origins and
///   WAFs that ignore bodies on bodyless methods leave those five bytes in the
///   stream, which is a request-smuggling/desync primitive against every later
///   request on that connection (and `GET` + `Transfer-Encoding: chunked` is a
///   shape many WAFs reject outright). Coercing every non-`Ordinary`
///   disposition here also collapses the upstream end-of-stream decision for
///   bodyless requests to a single case.
/// - An HTTP/1.0 upstream request must not be given `Transfer-Encoding:
///   chunked`: chunked framing does not exist below HTTP/1.1.
pub(crate) fn safe_disposition(
    disposition: UpstreamRequestBodyDisposition,
    facts: DispositionFacts,
) -> UpstreamRequestBodyDisposition {
    if disposition == UpstreamRequestBodyDisposition::Ordinary {
        return disposition;
    }
    let reason = if facts.is_connect {
        "a CONNECT request"
    } else if facts.is_upgrade_req {
        "an upgrade request"
    } else if facts.body_empty {
        "a request with no body"
    } else if facts.upstream_below_http11 {
        "an upstream request below HTTP/1.1"
    } else {
        return disposition;
    };
    // Routine, and reachable by client-chosen request shapes alone (an
    // `Upgrade:` header or a bodyless method), so this must not be a `warn!`:
    // it is not an application-contract violation and one client could
    // otherwise emit a WARN line per request.
    debug!(
        "upstream_request_body_disposition returned {disposition:?} for {reason}; \
         coercing to Ordinary"
    );
    UpstreamRequestBodyDisposition::Ordinary
}

/// Resolve the upstream request body disposition, collecting
/// [`DispositionFacts`] only when there is something for [`safe_disposition`]
/// to possibly coerce.
///
/// `Ordinary` is the coercion's own fixed point: `safe_disposition` returns it
/// unchanged no matter what the facts say (see `safe_disposition_truth_table`),
/// so collecting facts in order to decide whether to coerce `Ordinary` to
/// `Ordinary` is pure waste -- two `Upgrade` header lookups (downstream
/// session and upstream request), a method comparison, and an
/// `is_body_empty()`/`is_body_done()` pair, paid by every request regardless
/// of whether it ever uses this feature. Skipping straight to `Ordinary` here
/// is observably identical to running the full collect-then-coerce path.
///
/// `upstream_below_http11` is threaded in rather than computed here because
/// only the H1 pump can produce it; the H2 pump always sends HTTP/2 and passes
/// `false`.
pub(crate) fn safe_upstream_disposition(
    disposition: UpstreamRequestBodyDisposition,
    session: &mut Session,
    upstream_request: &RequestHeader,
    upstream_below_http11: bool,
) -> UpstreamRequestBodyDisposition {
    if disposition == UpstreamRequestBodyDisposition::Ordinary {
        return disposition;
    }
    let mut facts = DispositionFacts::collect(session, upstream_request);
    facts.upstream_below_http11 = upstream_below_http11;
    safe_disposition(disposition, facts)
}

/// The contract [`UpstreamRequestBodyDisposition::Bodyless`] asks the
/// application to honor, and the message
/// [`bodyless_contract_violation`] names it by.
pub(crate) const BODYLESS_CONTRACT_VIOLATION: &str =
    "application selected Bodyless upstream request framing but the downstream \
     request carried a body";

/// Whether this request-body event contradicts a `Bodyless` declaration.
///
/// Only ACTUAL bytes do. Two benign shapes reach the same suppressed-write
/// plumbing and must not be mistaken for the violation: a request that
/// genuinely has no body (whose single end-of-stream event still flows through
/// here) and the final end-of-stream event of any request, which carries no
/// data.
///
/// Both pumps call this AFTER the request-body filters have run, so an
/// application that declares `Bodyless` and then removes the body itself in
/// [`ProxyHttp::request_body_filter_action`](crate::ProxyHttp::request_body_filter_action)
/// is consistent, not in violation.
pub(crate) fn violates_bodyless_contract(
    disposition: UpstreamRequestBodyDisposition,
    data: Option<&Bytes>,
) -> bool {
    disposition == UpstreamRequestBodyDisposition::Bodyless && data.is_some_and(|d| !d.is_empty())
}

/// Fail closed on a `Bodyless` declaration that the downstream body just
/// disproved.
///
/// `Bodyless` is a guarantee from the application that no upstream request body
/// will follow, and both pumps act on it irreversibly before any body byte is
/// read: the H2 pump puts END_STREAM on the HEADERS frame (or on an empty DATA
/// frame), the H1 pump strips `Content-Length` and `Transfer-Encoding`. Once
/// downstream body bytes arrive anyway the only options left are to write them
/// onto a stream that cannot accept them, or to drop them.
///
/// Dropping is the dangerous one, and is what both pumps used to do: the
/// upstream then acts on a request whose client-supplied body was silently
/// removed -- a `POST` becomes an empty `POST`, a signed or authenticated
/// payload disappears -- while the client is told the request succeeded. The
/// proxy cannot judge that substitution safe for any upstream, so the request
/// fails instead. This is the same fail-closed convention the disposition
/// already follows for a non-`Ordinary` selection on a custom-connector session
/// (see `proxy_custom`).
///
/// [`safe_disposition`] has already coerced `Bodyless` to `Ordinary` for every
/// request whose downstream body is empty-and-done, so reaching here with real
/// bytes proves the application's declaration wrong rather than merely unlucky.
///
/// DO NOT add a `debug_assert!`/`panic!` here. It is tempting -- the trigger is
/// an application-contract violation, which is normally exactly what assertions
/// are for -- but this one is DATA-PLANE REACHABLE: given a single
/// mis-declaring application route, any ordinary client request that carries a
/// body reaches it. An abort on a client-reachable path is a remote
/// connection-kill primitive in every debug or staging build. Assertions are
/// for conditions untrusted traffic cannot reach; everything else, including
/// this, gets handled gracefully. The typed error below already produces both
/// the loud signal (an `error!` line from `HttpProxy::proxy_request`'s
/// final-error path, naming this message) and the safe outcome (a 500), so an
/// abort would buy nothing.
pub(crate) fn bodyless_contract_violation() -> BError {
    Error::explain(InternalError, BODYLESS_CONTRACT_VIOLATION)
}

/// Whether a pump that has no end-of-stream handling of its own has nothing to
/// read from the downstream request body.
///
/// This is the pre-tightening meaning of `is_body_done()`: "ended OR declared
/// empty". The H1/H2 upstream pumps deliberately no longer use it -- they gained
/// a bodyless prelude and a futile-read guard so that they can tell an H2
/// request declaring `Content-Length: 0` (empty, but whose stream has NOT ended,
/// design 4.3) apart from a genuinely finished one, and they need the strict
/// transport fact for that.
///
/// The custom-connector pump and the subrequest pipe implement neither. They
/// also derive their UPSTREAM end-of-stream from the declaration
/// (`is_body_empty()`), so with the strict fact their two halves contradict each
/// other: the upstream request is closed at header time while the downstream
/// state machine still believes a body is coming, and -- because these two
/// session types carry no downstream request-body idle timeout of their own
/// (the H1 and H2 sessions do, 60s by default, but a custom session's
/// `set_read_timeout` is `unreachable!()` and the subrequest pipe has no
/// transport to bound) -- the pump parks forever on a read that can never
/// yield, pinning the task, both streams and the upstream session.
/// Restoring the union here makes the two halves agree again, which is exactly
/// how both pumps behaved before the tightening.
pub(crate) fn no_downstream_body_to_read(session: &mut Session) -> bool {
    session.as_mut().is_body_done() || session.as_mut().is_body_empty()
}

/// Whether polling the downstream request body again can only park forever.
///
/// This is reached by a request that declares an EMPTY body but whose
/// transport EOS never arrives -- legal on H2, where `Content-Length: 0`
/// promises zero DATA bytes yet says nothing about END_STREAM (design 4.3) --
/// after the upstream exchange has completed. At that point there is provably
/// no body data left to forward (the emptiness is the transport's own
/// promise) and nothing left to consume it (the response is fully written), so
/// continuing to poll would pin the request, its task, the downstream stream
/// and the upstream stream for as long as the client cares to keep the stream
/// open. Finishing the read side lets the request complete instead.
///
/// Do NOT delete this as redundant now that H1/H2 sessions carry a
/// request-body idle timeout (60s by default). This rule fires immediately
/// rather than one idle period later; it also covers the session types that
/// have no such bound at all (custom, subrequest), and the H2 CONNECT sessions
/// that are deliberately exempt from it. The read-side counterpart --
/// `v2::server::HttpSession::read_body_bytes` answering its own timeout with
/// `Ok(None)` for a provably empty body -- is the backstop for the case this
/// one cannot see, where the response never completes because the upstream is
/// silent.
pub(crate) fn downstream_body_read_is_futile(
    session: &mut Session,
    downstream_state: &DownstreamStateMachine,
    response_state: &ResponseStateMachine,
) -> bool {
    if !downstream_state.is_reading()
        || !response_state.is_done()
        || !session.as_mut().is_body_empty()
    {
        return false;
    }
    debug!(
        "the upstream exchange is complete and the downstream request body is declared \
         empty; finishing the downstream read side instead of waiting for an \
         end-of-stream that may never arrive"
    );
    true
}

/// Release cache state held by a request that ends via the typed terminate
/// outcome.
///
/// Terminate returns `error = None`, so the `final_error` path in `lib.rs`
/// (which disables the cache) never runs for it. A cache-enabled miss holding
/// a write lock would then reach `WritePermit`'s `Drop` unfinished, which
/// trips a `debug_assert!` ("Dangling cache lock started!") and leaves other
/// waiters on that lock stranded. Disabling here releases the cache lock and
/// miss handler exactly like the error path would.
pub(crate) fn release_cache_on_terminate(session: &mut Session) {
    if session.cache.enabled() {
        session.cache.disable(NoCacheReason::InternalError);
    }
}

/// The terminate contract requires the application to have finished the
/// downstream response before returning [`RequestBodyAction::Terminate`].
/// Pingora never writes one on this path, so a terminate with nothing written
/// leaves the client with a bare connection close. Warn once, at the site that
/// accepts the terminate, so the misuse is diagnosable in production.
///
/// An unfinished response body is diagnosed too: it is invisible to the
/// "nothing written" check (a header was written, so `response_written()` is
/// `Some`) yet an H1 `Content-Length`-framed body sits unflushed in the
/// session's write buffer until something finishes it. The pump does finish it
/// defensively (see [`finish_terminated_response`]), so this is a warning
/// about the contract, not about lost bytes.
pub(crate) fn warn_terminate_without_response(session: &Session, hook: &str) {
    if session.response_written().is_none() {
        warn!(
            "{hook} returned Terminate without a downstream response having been \
             written; the client will see a bare connection close"
        );
    } else if session.response_body_finished() == Some(false) {
        warn!(
            "{hook} returned Terminate with an unfinished downstream response body; \
             the application is expected to complete the response itself"
        );
    }
}

/// Response-body variant of [`warn_terminate_without_response`]: diagnoses
/// only the "nothing written" misuse, not an "unfinished body".
///
/// The two contracts differ. A request-body terminate (the callers of
/// [`warn_terminate_without_response`] above) requires the application to
/// have *finished* the downstream response itself before returning
/// `Terminate` -- an unfinished body there really is a contract violation.
/// A response-body terminate (`upstream_response_body_filter` returning via
/// [`ResponseBodySink::terminate`]) instead fires *from inside* the pump
/// while it is still mid-body: a header (and normally some body) has already
/// been written but is never expected to be finished by anything other than
/// the pump's own `finish_terminated_response`, called on the very next line
/// after this warning. So `response_body_finished() == Some(false)` is not a
/// symptom of misuse here -- it is true on *every* response-body terminate,
/// by construction, which would make the "unfinished body" warning
/// permanently and spuriously true and teach operators to ignore it.
pub(crate) fn warn_response_body_terminate_without_response(session: &Session, hook: &str) {
    if session.response_written().is_none() {
        warn!(
            "{hook} returned Terminate without a downstream response having been \
             written; the client will see a bare connection close"
        );
    }
}

/// Diagnoses [`ResponseBodySink::terminate`](crate::ResponseBodySink::terminate)
/// firing while the committed downstream response still declares
/// `content-length`.
///
/// The precondition a terminating processor must satisfy (design doc §3.3):
/// it already declares `changes_body_length() == true`, so
/// `enforce_stream_processor_framing` (Edgion-side) strips `content-length`
/// before the response header is written. Nothing on either side of the seam
/// enforced that declaration until this guard -- a processor that forgets it
/// commits a response that is about to end short of the promised length,
/// which h1's `write_body` framing turns into bytes-fewer-than-declared: the
/// client reads that as a broken connection, not a normal end of stream.
///
/// This is a diagnostic, not a refusal. Two things make refusing the
/// terminate here both structurally awkward and only partially effective:
/// [`ResponseBodySink::terminate`](crate::ResponseBodySink::terminate) is
/// deliberately sticky (`reset_batch` does not clear it -- see
/// `response_body_sink.rs`), so by the time this check runs the decision to
/// end the response is already the sink's permanent state, not a one-shot
/// signal that can be "un-set" for this batch alone; and any extra chunks the
/// same processor pushed into the sink this batch were already written
/// downstream by `write_response_tasks` before this check ever runs, so
/// refusing could not undo that half of the leak regardless. Terminate exists
/// specifically to stop paying for upstream bytes nobody wants (the AI quota
/// use case this shipped for), so silently keeping the stream open instead of
/// warning would trade a diagnosable protocol issue for an undiagnosed
/// ongoing cost, on a path this guard expects to be dead code in practice --
/// every processor shipped today already declares `changes_body_length()`
/// correctly.
pub(crate) fn warn_response_body_terminate_content_length_leak(session: &Session, hook: &str) {
    let Some(header) = session.response_written() else {
        return;
    };
    if header.headers.contains_key(http::header::CONTENT_LENGTH) {
        // Same data-plane-reachable rule as `bodyless_contract_violation` above
        // (proxy_common.rs:316-326): the trigger is an application-contract
        // violation, but a single mis-declaring processor puts any client
        // request that reaches the terminate condition (for this fork's
        // gateway consumer, any client that deliberately exhausts its own
        // quota) on this path, so no `debug_assert!`/`panic!` here -- do not
        // re-add one.
        warn!(
            "{hook} terminated a response whose committed headers still declare \
             content-length; the client will see fewer bytes than promised and read \
             it as a transport failure rather than a clean end of stream. The \
             terminating response-body processor must declare \
             changes_body_length() == true."
        );
    }
}

/// Flush and close whatever downstream response the application wrote before
/// returning [`RequestBodyAction::Terminate`].
///
/// Terminate returns from the pump before its normal `finish_body()` call, and
/// `HttpProxy::finish` skips `downstream_session.finish()` because a
/// terminated request never reports reuse. On H1 that means a
/// `Content-Length`-framed response written with `end_of_stream = false` is
/// still sitting in the session's write buffer: neither `write_response_header`
/// (which only flushes for 1xx, for a response without `Content-Length`, or
/// when the writer is already finished) nor `write_body` flushes it, so the
/// client would receive ZERO bytes. Finishing here is idempotent on both
/// transports (H1's `BodyWriter::finish` and H2's `finish()` both no-op once
/// the body is done).
pub(crate) async fn finish_terminated_response(session: &mut Session) {
    if let Err(e) = session.as_mut().finish_body().await {
        // Nothing left to salvage: the request is ending either way, and the
        // connection is already marked non-reusable.
        warn!("Error finishing the downstream response body on terminate: {e}");
    }
}

/// A downstream custom-message reader, taken out of the session for the
/// duration of a pump and put back on every exit path.
pub(crate) type CustomMessageReader =
    Box<dyn futures::Stream<Item = Result<Bytes>> + Unpin + Send + Sync + 'static>;

/// Put the custom-message reader back into the downstream session.
///
/// Both pumps take it out for the duration of the duplex loop and restore it
/// on normal exit; the terminate early-returns must do the same, or a Custom
/// downstream session is left asymmetric (its writer IS restored by the outer
/// function) and a later reuse would trip the
/// `"...should be empty"` expectation.
pub(crate) fn restore_custom_message_reader(
    session: &mut Session,
    reader: Option<CustomMessageReader>,
) {
    let (Some(custom_session), Some(reader)) = (session.downstream_session.as_custom_mut(), reader)
    else {
        return;
    };
    if let Err(e) = custom_session.restore_custom_message_reader(reader) {
        warn!("Error restoring the downstream custom message reader: {e}");
    }
}

/// The outcome of the downstream half of a proxy exchange.
///
/// This is the crate-internal half of the termination contract: an
/// application-selected terminal action travels as a typed outcome, never as
/// a generic `Error`, so it can bypass retry classification and
/// `fail_to_proxy` response generation. The future response-side streaming
/// hook reuses this same channel.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DownstreamRequestOutcome {
    /// Normal completion; the bool is downstream connection reusability.
    Complete(bool),
    /// The downstream response completed successfully, but the upstream
    /// response source failed after that terminal boundary. The bool is the
    /// downstream connection reusability; the upstream connection must not
    /// be reused.
    CompleteWithoutUpstreamReuse(bool),
    /// Application termination: proxying of this request stops here. The
    /// application has already finished the downstream response.
    Terminate,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_headers(headers: &[(&str, &str)]) -> RequestHeader {
        let mut request = RequestHeader::build("GET", b"/", Some(headers.len())).unwrap();
        request.set_version(http::Version::HTTP_11);
        for (name, value) in headers {
            request
                .append_header(
                    HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    http::HeaderValue::from_str(value).unwrap(),
                )
                .unwrap();
        }
        request
    }

    #[test]
    fn h2_upstream_removes_connection_nominated_fields_by_default() {
        let mut request = request_with_headers(&[
            ("Connection", "X-Private-Hop, HTTP2-Settings"),
            ("X-Private-Hop", "secret"),
            ("HTTP2-Settings", "settings"),
            ("Proxy-Authorization", "secret"),
            ("TE", "trailers"),
            ("Trailer", "X-Trailer"),
        ]);

        sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard()).unwrap();

        assert!(request.headers.get("x-private-hop").is_none());
        assert!(request.headers.get("http2-settings").is_none());
        assert!(request.headers.get("proxy-authorization").is_none());
        assert!(request.headers.get("te").is_none());
        assert!(request.headers.get("trailer").is_none());
    }

    #[test]
    fn h2_upstream_can_retain_connection_nominated_fields() {
        let mut request =
            request_with_headers(&[("Connection", "X-Private-Hop"), ("X-Private-Hop", "secret")]);
        let mut policy = HttpUpstreamRequestPolicy::standard();
        policy.strip_connection_nominated = false;

        sanitize_h2_upstream_request(&mut request, policy).unwrap();

        assert_eq!(request.headers["x-private-hop"], "secret");
    }

    #[test]
    fn h2_upstream_removes_nominations_after_connection_self_nomination() {
        let mut request = request_with_headers(&[
            ("Connection", "Connection, X-Private-Hop"),
            ("X-Private-Hop", "secret"),
        ]);

        sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard()).unwrap();

        assert!(request.headers.get("connection").is_none());
        assert!(request.headers.get("x-private-hop").is_none());
    }

    #[test]
    fn h2_upstream_rejects_excessive_unparseable_connection_nominations() {
        let mut request = request_with_headers(&[("Connection", "@, @, @, @, @, @, @, @, @, @")]);

        assert!(
            sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                .is_err()
        );
    }

    fn facts(
        is_upgrade_req: bool,
        is_connect: bool,
        body_empty: bool,
        below_11: bool,
    ) -> DispositionFacts {
        DispositionFacts {
            is_upgrade_req,
            is_connect,
            body_empty,
            upstream_below_http11: below_11,
        }
    }

    /// Full truth table of the disposition coercion: every combination of the
    /// four facts, for every disposition.
    #[test]
    fn safe_disposition_truth_table() {
        use UpstreamRequestBodyDisposition::*;

        for upgrade in [false, true] {
            for connect in [false, true] {
                for empty in [false, true] {
                    for below_11 in [false, true] {
                        let f = facts(upgrade, connect, empty, below_11);
                        // Ordinary is never touched.
                        assert_eq!(safe_disposition(Ordinary, f), Ordinary, "{f:?}");

                        let must_coerce = upgrade || connect || empty || below_11;
                        for selected in [Bodyless, Streamed] {
                            let expected = if must_coerce { Ordinary } else { selected };
                            assert_eq!(
                                safe_disposition(selected, f),
                                expected,
                                "{selected:?} with {f:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// The tunnel facts come from the UNION of both sides, because the
    /// rewrite the disposition drives targets the upstream request while the
    /// downstream request is what the client actually sent.
    #[test]
    fn disposition_facts_union_both_sides() {
        fn upstream(method: &str, upgrade: bool) -> RequestHeader {
            let mut req = RequestHeader::build(method, b"/", None).unwrap();
            if upgrade {
                req.insert_header(http::header::UPGRADE, "websocket")
                    .unwrap();
                req.insert_header(http::header::CONNECTION, "upgrade")
                    .unwrap();
            }
            req
        }

        // Neither side: nothing to protect.
        let plain = DispositionFacts::union(false, false, false, &upstream("POST", false));
        assert!(!plain.is_upgrade_req && !plain.is_connect);

        // Only the DOWNSTREAM request is a tunnel (the application stripped
        // `Upgrade` from the upstream request).
        let downstream_only = DispositionFacts::union(true, false, false, &upstream("POST", false));
        assert!(downstream_only.is_upgrade_req);
        let downstream_connect =
            DispositionFacts::union(false, true, false, &upstream("POST", false));
        assert!(downstream_connect.is_connect);

        // Only the UPSTREAM request is a tunnel (the application synthesized
        // it from an ordinary downstream request).
        let upstream_only = DispositionFacts::union(false, false, false, &upstream("GET", true));
        assert!(upstream_only.is_upgrade_req);
        let upstream_connect =
            DispositionFacts::union(false, false, false, &upstream("CONNECT", false));
        assert!(upstream_connect.is_connect);
    }

    /// The individually load-bearing rows, spelled out so a regression names
    /// the reason it broke.
    #[test]
    fn safe_disposition_named_rows() {
        use UpstreamRequestBodyDisposition::*;
        // Nothing special about the request: the application's choice stands.
        assert_eq!(
            safe_disposition(Streamed, facts(false, false, false, false)),
            Streamed
        );
        assert_eq!(
            safe_disposition(Bodyless, facts(false, false, false, false)),
            Bodyless
        );
        // Tunnels keep their protocol-fixed framing.
        assert_eq!(
            safe_disposition(Streamed, facts(true, false, false, false)),
            Ordinary
        );
        assert_eq!(
            safe_disposition(Streamed, facts(false, true, false, false)),
            Ordinary
        );
        // A request with no body must not be re-framed as chunked: the
        // `0\r\n\r\n` terminator on a pooled upstream connection is a
        // smuggling primitive.
        assert_eq!(
            safe_disposition(Streamed, facts(false, false, true, false)),
            Ordinary
        );
        // HTTP/1.0 peers must never be sent `Transfer-Encoding: chunked`.
        assert_eq!(
            safe_disposition(Streamed, facts(false, false, false, true)),
            Ordinary
        );
    }

    /// `DispositionFacts::collect` against a LIVE downstream session that a
    /// client has poisoned.
    ///
    /// The pure truth table above cannot see this failure at all: it takes
    /// `body_empty` as an input, and the bug was in producing that input. A
    /// plain bodyless `GET` (END_STREAM on HEADERS) whose client then RESETS the
    /// stream makes `h2` overwrite the stream state, after which the live
    /// `RecvStream::is_end_stream()` reports `false` -- so both
    /// `is_body_empty()` and `is_body_done()` used to flip back to `false` and
    /// `safe_disposition` stopped recognising a request with no body. `Streamed`
    /// then survived the coercion, and the pump proxied a bodyless `GET`
    /// upstream with `Transfer-Encoding: chunked` framing it can never
    /// terminate.
    ///
    /// The reset is delivered AFTER the stream was accepted, which is the window
    /// that matters: `proxy_down_to_up` collects these facts only after
    /// `upstream_peer`, the cache lookup and `upstream_request_filter` have all
    /// had their turn, so a reset has plenty of await points to land in. (A
    /// reset that `h2` processes in the SAME batch as the HEADERS frame is
    /// beyond recovery: it destroys the END_STREAM evidence before `accept()`
    /// ever returns the stream, so `request_headers_end_stream()` -- the
    /// snapshot itself -- is already `false`. Such a request necessarily fails on
    /// its first body read, so no upstream request can complete for it.)
    #[tokio::test]
    async fn collect_survives_a_client_reset_after_a_bodyless_request() {
        use pingora_core::modules::http::HttpModules;
        use pingora_core::protocols::http::v2::server::{handshake, HttpSession as SessionV2};
        use pingora_core::protocols::http::ServerSession;
        use pingora_core::protocols::Digest;
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let (client_io, server_io) = tokio::io::duplex(65536);
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel::<()>();

        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client_io).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/")
                .body(())
                .unwrap();
            // END_STREAM on HEADERS: a request with no body at all.
            let (response, body) = h2.send_request(req, true).unwrap();

            // Only reset once the server has the stream, so the END_STREAM fact
            // is established first and this test is about RETRACTING it.
            accepted_rx.await.unwrap();
            drop(response);
            drop(body);

            // A SECOND stream. Frames are processed in order on the connection,
            // so the server accepting this one PROVES the reset above has
            // already been handled -- no sleep, no race.
            let mut h2 = h2.ready().await.unwrap();
            let probe = http::Request::builder()
                .method("GET")
                .uri("https://example.com/probe")
                .body(())
                .unwrap();
            let (probe_response, _) = h2.send_request(probe, true).unwrap();
            let _ = probe_response.await;
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        });

        let mut connection = handshake(Box::new(server_io), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let accepted = SessionV2::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
            .expect("the first stream");
        let pingora_core::protocols::http::v2::server::H2Accept::Session(poisoned) = accepted
        else {
            panic!("the first stream was unexpectedly rejected");
        };
        assert!(
            poisoned.request_headers_end_stream(),
            "precondition: the END_STREAM fact was established before the reset"
        );
        accepted_tx.send(()).unwrap();
        assert!(
            SessionV2::from_h2_conn(&mut connection, digest)
                .await
                .unwrap()
                .is_some(),
            "the probe stream proves the reset has been processed"
        );

        let modules = HttpModules::new();
        let mut session = Session::new(
            Box::new(ServerSession::new_http2(poisoned)),
            &modules,
            Arc::new(AtomicBool::new(false)),
        );

        let upstream_request = RequestHeader::build("GET", b"/", None).unwrap();
        let facts = DispositionFacts::collect(&mut session, &upstream_request);
        assert!(
            facts.body_empty,
            "a bodyless request stays bodyless after the client resets the stream"
        );
        assert_eq!(
            safe_disposition(UpstreamRequestBodyDisposition::Streamed, facts),
            UpstreamRequestBodyDisposition::Ordinary,
            "a request with no body must never be re-framed as chunked"
        );

        drop(session);
        drop(connection);
        client.abort();
    }

    /// [`safe_upstream_disposition`]'s short-circuit must not change observable
    /// behavior: it must skip fact collection ONLY for `Ordinary`, never for a
    /// selection that actually needs coercing.
    ///
    /// Both assertions run against the SAME live, bodyless session -- a shape
    /// that WOULD trigger coercion if the facts were consulted -- so the first
    /// assertion cannot pass merely because there was nothing to coerce in the
    /// first place. `Ordinary` coming back unchanged here is exactly what the
    /// old collect-then-coerce path also produced (`safe_disposition_truth_table`
    /// proves `Ordinary` is `safe_disposition`'s fixed point for every fact
    /// combination), so the skip is observably a no-op. `Streamed` on the same
    /// session must still be coerced back to `Ordinary`, proving the
    /// short-circuit's `if` does not accidentally swallow the case it exists to
    /// let through.
    #[tokio::test]
    async fn safe_upstream_disposition_short_circuits_ordinary_only() {
        use pingora_core::modules::http::HttpModules;
        use pingora_core::protocols::http::v2::server::{handshake, HttpSession as SessionV2};
        use pingora_core::protocols::http::ServerSession;
        use pingora_core::protocols::Digest;
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let (client_io, server_io) = tokio::io::duplex(65536);
        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client_io).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let req = http::Request::builder()
                .method("GET")
                .uri("https://example.com/")
                .body(())
                .unwrap();
            // END_STREAM on HEADERS: a request with no body at all, the shape
            // `safe_disposition` coerces a non-`Ordinary` selection away from.
            let (response, _body) = h2.send_request(req, true).unwrap();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), response).await;
        });

        let mut connection = handshake(Box::new(server_io), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let accepted = SessionV2::from_h2_conn(&mut connection, digest)
            .await
            .unwrap()
            .expect("the request stream");
        let pingora_core::protocols::http::v2::server::H2Accept::Session(session_v2) = accepted
        else {
            panic!("the request stream was unexpectedly rejected");
        };

        let modules = HttpModules::new();
        let mut session = Session::new(
            Box::new(ServerSession::new_http2(session_v2)),
            &modules,
            Arc::new(AtomicBool::new(false)),
        );

        let upstream_request = RequestHeader::build("GET", b"/", None).unwrap();

        assert_eq!(
            safe_upstream_disposition(
                UpstreamRequestBodyDisposition::Ordinary,
                &mut session,
                &upstream_request,
                false,
            ),
            UpstreamRequestBodyDisposition::Ordinary,
            "Ordinary must pass through unchanged, exactly as the old \
             collect-then-coerce path also produced"
        );
        assert_eq!(
            safe_upstream_disposition(
                UpstreamRequestBodyDisposition::Streamed,
                &mut session,
                &upstream_request,
                false,
            ),
            UpstreamRequestBodyDisposition::Ordinary,
            "a bodyless request must still be coerced back to Ordinary"
        );

        drop(session);
        drop(connection);
        client.abort();
    }

    /// The fact both pumps that have no end-of-stream handling of their own
    /// depend on, against a LIVE session.
    ///
    /// An H2 request declaring `Content-Length: 0` WITHOUT END_STREAM is empty
    /// but not finished: `is_body_done()` is `false` and stays `false` until the
    /// client sends an end of stream it may never send. The custom-connector
    /// pump and the subrequest pipe derive their upstream end-of-stream from
    /// `is_body_empty()` and have neither a bodyless prelude nor a futile-read
    /// guard, so initialising their downstream state machine from the strict
    /// fact leaves the two halves contradicting each other and parks the pump
    /// forever on a read that can never yield -- there is no downstream
    /// request-body idle timeout to break it. `no_downstream_body_to_read` is
    /// the union that keeps them agreeing, exactly as they did before
    /// `is_body_done()` was tightened.
    #[tokio::test]
    async fn no_downstream_body_to_read_covers_a_declared_empty_body() {
        use pingora_core::modules::http::HttpModules;
        use pingora_core::protocols::http::v2::server::{handshake, HttpSession as SessionV2};
        use pingora_core::protocols::http::ServerSession;
        use pingora_core::protocols::Digest;
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let (client_io, server_io) = tokio::io::duplex(65536);
        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client_io).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let req = http::Request::builder()
                .method("POST")
                .uri("https://example.com/")
                .header("content-length", "0")
                .body(())
                .unwrap();
            // No END_STREAM, and this client never sends one.
            let (response, _body) = h2.send_request(req, false).unwrap();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), response).await;
        });

        let mut connection = handshake(Box::new(server_io), None).await.unwrap();
        let accepted = SessionV2::from_h2_conn(&mut connection, Arc::new(Digest::default()))
            .await
            .unwrap()
            .expect("the request stream");
        let pingora_core::protocols::http::v2::server::H2Accept::Session(session_v2) = accepted
        else {
            panic!("the request stream was unexpectedly rejected");
        };

        let modules = HttpModules::new();
        let mut session = Session::new(
            Box::new(ServerSession::new_http2(session_v2)),
            &modules,
            Arc::new(AtomicBool::new(false)),
        );

        assert!(
            session.as_mut().is_body_empty(),
            "`Content-Length: 0` promises zero body bytes"
        );
        assert!(
            !session.as_mut().is_body_done(),
            "precondition: the transport has NOT ended, which is what would park the pump"
        );
        assert!(
            no_downstream_body_to_read(&mut session),
            "a pump without end-of-stream handling has nothing to read here"
        );

        drop(session);
        drop(connection);
        client.abort();
    }

    /// Only real bytes under `Bodyless` are a contract violation. Every other
    /// cell of the grid is a shape that legitimately reaches the same
    /// suppressed-write plumbing.
    #[test]
    fn violates_bodyless_contract_only_on_real_bytes_under_bodyless() {
        use UpstreamRequestBodyDisposition::*;
        let events: [(&str, Option<Bytes>); 3] = [
            // The end-of-stream event of any request.
            ("end of stream", None),
            // A chunk the filters emptied, or a zero-length transport read.
            ("empty chunk", Some(Bytes::new())),
            ("real bytes", Some(Bytes::from_static(b"hello"))),
        ];
        for disposition in [Ordinary, Bodyless, Streamed] {
            for (name, data) in events.iter() {
                let expected = disposition == Bodyless && *name == "real bytes";
                assert_eq!(
                    violates_bodyless_contract(disposition, data.as_ref()),
                    expected,
                    "{disposition:?} with {name}"
                );
            }
        }
    }
}

#[cfg(test)]
mod terminal_body_dispatch_tests {
    use super::*;

    fn request_with_headers(headers: &[(&str, &str)]) -> RequestHeader {
        let mut request = RequestHeader::build("GET", b"/", Some(headers.len())).unwrap();
        request.set_version(http::Version::HTTP_11);
        for (name, value) in headers {
            request
                .append_header(
                    HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    http::HeaderValue::from_str(value).unwrap(),
                )
                .unwrap();
        }
        request
    }
    use pingora_error::ErrorType::InternalError;
    use pingora_http::ResponseHeader;

    fn header(eos: bool) -> HttpTask {
        HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), eos)
    }

    fn body(eos: bool) -> HttpTask {
        HttpTask::Body(Some(Bytes::from_static(b"chunk")), eos)
    }

    fn trailer() -> HttpTask {
        HttpTask::Trailer(Some(Box::default()))
    }

    fn failed() -> HttpTask {
        HttpTask::Failed(Error::explain(InternalError, "upstream aborted"))
    }

    /// Feed a whole response through one latch and collect, for each task,
    /// whether it dispatched the terminal callback.
    fn dispatches(tasks: &[HttpTask]) -> Vec<Option<UpstreamResponseBodyEvent>> {
        let mut latch = TerminalBodyDispatch::default();
        tasks.iter().map(|t| latch.claim_for(t)).collect()
    }

    /// The defect this latch exists for: H2 puts END_STREAM on the trailers
    /// HEADERS frame, so every DATA frame arrives with `eos = false`. The
    /// `Trailer` must dispatch, and the `Done` behind it must not repeat it.
    #[test]
    fn trailered_response_dispatches_once_on_the_trailer() {
        assert_eq!(
            dispatches(&[
                header(false),
                body(false),
                body(false),
                trailer(),
                HttpTask::Done
            ]),
            [
                None,
                None,
                None,
                Some(UpstreamResponseBodyEvent::TerminalBeforeTrailers),
                None
            ]
        );
    }

    #[test]
    fn connection_nomination_rejects_protected_header() {
        for token in [
            "Host",
            "x-forwarded-for",
            "X-Forwarded-For",
            "X-FORWARDED-HOST",
            "x-Forwarded-Proto",
        ] {
            let mut request = request_with_headers(&[("Connection", token)]);
            assert!(
                sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                    .is_err(),
                "protected nomination should be rejected regardless of casing: {token:?}"
            );
        }
    }

    #[test]
    fn connection_nomination_rejects_pseudo_header() {
        for token in [":authority", ":method", ":path"] {
            let mut request = request_with_headers(&[("Connection", token)]);
            assert!(
                sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                    .is_err(),
                "pseudo-header nomination should be rejected: {token:?}"
            );
        }
    }

    /// A nomination that is not a valid `token` is rejected outright instead of silently dropped.
    #[test]
    fn connection_nomination_rejects_malformed_token() {
        let mut request = request_with_headers(&[("Connection", "keep-alive, bad token")]);
        assert!(
            sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                .is_err()
        );
    }

    /// A protected name decorated with any non-`token` byte is rejected, independent of how
    /// permissive the header-name parser is.
    #[test]
    fn connection_nomination_rejects_decorated_protected_header() {
        for token in [
            "\"X-Forwarded-For\"",
            "(X-Forwarded-For",
            "X-Forwarded-For)",
            "X-Forwarded-For/",
            "X-Forwarded-For:",
            "X -Forwarded-For",
            "@X-Forwarded-For",
        ] {
            let mut request =
                request_with_headers(&[("Connection", token), ("X-Forwarded-For", "6.6.6.6")]);
            assert!(
                sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                    .is_err(),
                "decorated protected nomination should be rejected: {token:?}"
            );
        }
    }

    /// A protected name decorated with a valid `tchar` (e.g. `'X-Forwarded-For'`) is a well-formed
    /// nomination of a *distinct* header: accepted, but harmless — the real header is untouched.
    #[test]
    fn connection_nomination_allows_tchar_decorated_lookalike() {
        for token in ["'X-Forwarded-For'", "X-Forwarded-For.", "!X-Forwarded-For"] {
            let mut request =
                request_with_headers(&[("Connection", token), ("X-Forwarded-For", "6.6.6.6")]);
            assert!(
                sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                    .is_ok(),
                "tchar-decorated lookalike is a distinct header, not a protected match: {token:?}"
            );
            assert_eq!(request.headers["x-forwarded-for"], "6.6.6.6");
        }
    }

    /// A policy that tolerates malformed `Connection` nominations while still stripping them.
    fn lenient_policy() -> HttpUpstreamRequestPolicy {
        let mut policy = HttpUpstreamRequestPolicy::standard();
        policy.reject_malformed_connection_nominations = false;
        policy
    }

    /// In lenient mode a malformed nomination is tolerated: it targets a distinct field and leaves
    /// the real protected header intact.
    #[test]
    fn lenient_connection_nomination_tolerates_malformed_token() {
        for token in [
            "\"X-Forwarded-For\"",
            "(X-Forwarded-For",
            "@X-Forwarded-For",
            "X -Forwarded-For",
            "keep-alive, bad token",
        ] {
            let mut request =
                request_with_headers(&[("Connection", token), ("X-Forwarded-For", "6.6.6.6")]);
            assert!(
                sanitize_h2_upstream_request(&mut request, lenient_policy()).is_ok(),
                "malformed nomination should be tolerated in lenient mode: {token:?}"
            );
            assert_eq!(request.headers["x-forwarded-for"], "6.6.6.6");
        }
    }

    /// Even in lenient mode, an exact protected or pseudo-header nomination is still rejected.
    #[test]
    fn lenient_connection_nomination_still_rejects_exact_protected() {
        for token in ["x-forwarded-for", "X-Forwarded-For", "host", ":authority"] {
            let mut request = request_with_headers(&[("Connection", token)]);
            assert!(
                sanitize_h2_upstream_request(&mut request, lenient_policy()).is_err(),
                "exact protected/pseudo nomination must be rejected even in lenient mode: {token:?}"
            );
        }
    }

    #[test]
    fn normal_lifecycle() {
        let mut ds = DownstreamStateMachine::new(false);
        assert!(ds.is_reading());
        assert!(ds.can_poll());
        assert!(!ds.is_errored());

        ds.maybe_finished(true);
        assert!(!ds.is_reading());
        assert!(ds.is_done());
        assert!(ds.can_poll()); // ReadingFinished still allows polling (for idle)
        assert!(!ds.is_errored());
    }

    #[test]
    fn errored_is_terminal() {
        let mut ds = DownstreamStateMachine::new(false);
        ds.to_errored();
        assert!(ds.is_errored());
        assert!(!ds.can_poll());
        assert!(ds.is_done());
    }

    /// `maybe_finished(false)` is always a no-op regardless of state.
    #[test]
    fn maybe_finished_false_is_noop() {
        let mut ds = DownstreamStateMachine::new(false);
        ds.to_errored();
        ds.maybe_finished(false); // must not panic
        assert!(ds.is_errored());
        assert!(!ds.can_poll());
    }

    /// `maybe_finished(true)` on `Errored` is a no-op — `Errored` is terminal.
    #[test]
    fn maybe_finished_true_noop_on_errored() {
        let mut ds = DownstreamStateMachine::new(false);
        ds.to_errored();
        ds.maybe_finished(true); // must not overwrite Errored
        assert!(ds.is_errored());
        assert!(!ds.can_poll());
    }

    /// `reset()` on `Errored` is a no-op — `Errored` is terminal.
    #[test]
    fn reset_noop_on_errored() {
        let mut ds = DownstreamStateMachine::new(false);
        ds.to_errored();
        ds.reset(); // must not overwrite Errored
        assert!(ds.is_errored());
        assert!(!ds.can_poll());
    }

    #[test]
    fn bare_done_dispatches_when_nothing_claimed_the_termination() {
        assert_eq!(
            dispatches(&[header(false), body(false), HttpTask::Done]),
            [
                None,
                None,
                Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers)
            ]
        );
    }

    /// `Header(_, true)` is 204/304/HEAD/`CL:0`: the `terminal_header` branch
    /// already runs `terminal_upstream_body_filter` for it.
    #[test]
    fn terminal_header_claims_without_dispatching() {
        assert_eq!(dispatches(&[header(true), HttpTask::Done]), [None, None]);
    }

    /// `Body(_, true)` carries `eos = true` into the body filter itself.
    #[test]
    fn terminal_body_claims_without_dispatching() {
        assert_eq!(
            dispatches(&[header(false), body(false), body(true), HttpTask::Done]),
            [None, None, None, None]
        );
    }

    /// A trailer arriving after a body task already ended the stream is not a
    /// second termination.
    #[test]
    fn trailer_after_terminal_body_does_not_dispatch() {
        assert_eq!(
            dispatches(&[body(true), trailer(), HttpTask::Done]),
            [None, None, None]
        );
    }

    /// An aborted response must never be told its truncated body was complete,
    /// and the `Done` that may follow the error must not say it either.
    #[test]
    fn failed_never_dispatches_and_suppresses_a_following_done() {
        assert_eq!(
            dispatches(&[header(false), body(false), failed(), HttpTask::Done]),
            [None, None, None, None]
        );
    }

    /// `Trailer(None)` is still a termination observation: `upstream_filter`
    /// skips it (its match arm is `Trailer(Some(..))`), so if it were ignored
    /// here the following `Done` would dispatch a second time.
    #[test]
    fn empty_trailer_claims_the_termination() {
        assert_eq!(
            dispatches(&[body(false), HttpTask::Trailer(None), HttpTask::Done]),
            [
                None,
                Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers),
                None
            ]
        );
    }

    /// Released bytes must inherit the response's body variant so
    /// `write_response_tasks` keeps routing them down the post-upgrade duplex
    /// path. The terminal `Done` itself carries no variant.
    #[test]
    fn upgraded_body_is_remembered_for_the_terminal_dispatch() {
        let mut latch = TerminalBodyDispatch::default();
        assert!(!latch.is_upgraded());
        latch.claim_for(&HttpTask::UpgradedBody(
            Some(Bytes::from_static(b"frame")),
            false,
        ));
        assert!(latch.is_upgraded());
        assert_eq!(
            latch.claim_for(&HttpTask::Done),
            Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers)
        );
        assert!(latch.is_upgraded());
    }

    /// An upgraded response can close before yielding any body task. The final
    /// filtered handshake must therefore establish the body variant on its
    /// own, or bytes released by the later `Done` callback would be emitted as
    /// plain `Body` into an already-upgraded downstream session.
    #[test]
    fn filtered_upgrade_handshake_marks_response_upgraded() {
        let mut latch = TerminalBodyDispatch::default();
        latch.mark_upgraded();
        assert!(latch.is_upgraded());
        assert_eq!(
            latch.claim_for(&HttpTask::Done),
            Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers)
        );
    }

    #[test]
    fn plain_body_response_is_not_marked_upgraded() {
        let mut latch = TerminalBodyDispatch::default();
        latch.claim_for(&body(false));
        latch.claim_for(&trailer());
        assert!(!latch.is_upgraded());
    }

    /// The latch is per response, not per batch: a fresh one dispatches again.
    #[test]
    fn a_new_latch_dispatches_for_the_next_response() {
        let mut latch = TerminalBodyDispatch::default();
        assert!(latch.claim_for(&HttpTask::Done).is_some());
        assert!(latch.claim_for(&HttpTask::Done).is_none());
        assert!(TerminalBodyDispatch::default()
            .claim_for(&HttpTask::Done)
            .is_some());
    }

    #[test]
    fn emptied_trailer_map_normalizes_to_no_trailer() {
        assert!(normalize_trailers(Some(Box::default())).is_none());

        let mut trailers = HeaderMap::new();
        trailers.insert("x-test", "present".parse().unwrap());
        assert!(normalize_trailers(Some(Box::new(trailers))).is_some());
    }
}
