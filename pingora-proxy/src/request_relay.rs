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

//! Protocol-neutral request-body relay policy and event processing.
//!
//! The duplex pumps retain ownership of source reads, pipe capacity, protocol
//! writes, retries, early-response cleanup, and connection reuse. This module
//! owns the shared relay disposition and bodyless-contract policy, plus the
//! semantic sequence between receiving one downstream body event and handing
//! its filtered form back to the protocol writer.

use crate::{
    custom, HttpProxy, ProxyHttp, RequestAttemptId, RequestBodyAction, RequestBodyEvent,
    RequestRelayPlan, RequestRelayRetryState, RequestReplayPolicy, Session,
    UpstreamRequestBodyDisposition,
};
use bytes::Bytes;
use http::Method;
use log::debug;
use pingora_error::{BError, Error, ErrorType::InternalError, Result};
use pingora_http::RequestHeader;

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

    fn streamed_protocol_conflict(self) -> bool {
        self.is_upgrade_req || self.is_connect || self.upstream_below_http11
    }
}

pub(crate) const STREAMED_PROTOCOL_CONFLICT: &str =
    "the frozen streamed request relay plan conflicts with the final upstream protocol shape";

/// Reject an attempt-local protocol rewrite that invalidates a frozen streamed
/// relay plan. Bodyless requests remain eligible for the existing benign
/// coercion to `Ordinary`; tunnel and pre-HTTP/1.1 shapes cannot safely keep a
/// previously installed length-changing body processor alive under ordinary
/// framing.
pub(crate) fn validate_streamed_upstream_disposition(
    disposition: UpstreamRequestBodyDisposition,
    session: &mut Session,
    upstream_request: &RequestHeader,
    upstream_below_http11: bool,
) -> Result<()> {
    if disposition != UpstreamRequestBodyDisposition::Streamed {
        return Ok(());
    }
    let mut facts = DispositionFacts::collect(session, upstream_request);
    facts.upstream_below_http11 = upstream_below_http11;
    if facts.streamed_protocol_conflict() {
        return Error::e_explain(InternalError, STREAMED_PROTOCOL_CONFLICT);
    }
    Ok(())
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
        target: "pingora_proxy::proxy_common",
        "request_relay_plan selected {disposition:?} for {reason}; \
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

/// Core-derived, immutable request relay contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrozenRequestRelayPlan {
    pub(crate) requested: RequestRelayPlan,
    pub(crate) source: RequestRelaySource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestRelaySource {
    LiveDownstream,
    RegisteredReplay,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum NativeRetryBufferState {
    #[default]
    NotStarted,
    Enabled,
    Unsupported,
}

impl FrozenRequestRelayPlan {
    pub(crate) fn derive(requested: RequestRelayPlan, registered_replay: bool) -> Self {
        Self {
            requested,
            source: if registered_replay {
                RequestRelaySource::RegisteredReplay
            } else {
                RequestRelaySource::LiveDownstream
            },
        }
    }

    pub(crate) fn enables_native_retry_buffer(self) -> bool {
        self.requested.replay == RequestReplayPolicy::Replayable
            && self.source == RequestRelaySource::LiveDownstream
    }
}

impl Session {
    pub(super) fn freeze_request_relay_plan(&mut self, requested: RequestRelayPlan) -> Result<()> {
        if self.frozen_request_relay_plan.is_some() {
            return Error::e_explain(
                InternalError,
                "request relay plan was frozen more than once",
            );
        }
        if requested.disposition == UpstreamRequestBodyDisposition::Streamed
            && requested.replay != RequestReplayPolicy::Never
        {
            return Error::e_explain(
                InternalError,
                "a streamed request relay plan must disable replay",
            );
        }
        let registered = self.downstream_session.request_body_buffer_registered();
        self.downstream_session.freeze_request_body_configuration();
        self.frozen_request_relay_plan =
            Some(FrozenRequestRelayPlan::derive(requested, registered));
        Ok(())
    }

    pub(super) fn frozen_request_relay_plan(&self) -> FrozenRequestRelayPlan {
        self.frozen_request_relay_plan
            .expect("request relay plan must be frozen before an upstream attempt")
    }

    /// Return the request-scoped relay policy once it has been frozen.
    pub fn request_relay_plan(&self) -> Option<RequestRelayPlan> {
        self.frozen_request_relay_plan.map(|plan| plan.requested)
    }

    /// Return the current canonical upstream attempt identity.
    pub fn request_attempt_id(&self) -> Option<RequestAttemptId> {
        self.request_attempt_id
    }

    pub(super) fn begin_request_relay_attempt(&mut self, attempt: usize) {
        self.request_attempt_id = Some(RequestAttemptId::new(attempt));
    }

    pub(super) fn enable_request_relay_retry_buffer(&mut self) {
        let plan = self.frozen_request_relay_plan();
        if !plan.enables_native_retry_buffer()
            || self.native_retry_buffer_state != NativeRetryBufferState::NotStarted
        {
            return;
        }
        if self.downstream_session.retry_buffering_supported() {
            self.downstream_session.enable_retry_buffering();
            self.native_retry_buffer_state = NativeRetryBufferState::Enabled;
        } else {
            self.native_retry_buffer_state = NativeRetryBufferState::Unsupported;
        }
    }

    pub(super) fn request_relay_retry_buffer(&self) -> Option<Bytes> {
        (self.native_retry_buffer_state == NativeRetryBufferState::Enabled)
            .then(|| self.downstream_session.get_retry_buffer())
            .flatten()
    }

    /// Return the current body backing state used by the retry gate.
    pub fn request_relay_retry_state(&self) -> RequestRelayRetryState {
        let Some(plan) = self.frozen_request_relay_plan else {
            return RequestRelayRetryState::Disabled;
        };
        if plan.requested.replay == RequestReplayPolicy::Never {
            return RequestRelayRetryState::Disabled;
        }
        if plan.source == RequestRelaySource::RegisteredReplay {
            return if self
                .downstream_session
                .request_body_buffer_replay_available()
            {
                RequestRelayRetryState::RegisteredReplay
            } else {
                RequestRelayRetryState::RegisteredUnavailable
            };
        }
        match self.native_retry_buffer_state {
            NativeRetryBufferState::NotStarted => RequestRelayRetryState::LiveUnread,
            NativeRetryBufferState::Unsupported => RequestRelayRetryState::Unsupported,
            NativeRetryBufferState::Enabled => {
                if self.downstream_session.retry_buffer_truncated() {
                    RequestRelayRetryState::NativeTruncated
                } else {
                    RequestRelayRetryState::NativeCapturing
                }
            }
        }
    }
}

/// Protocol capabilities which differ at the request relay seam.
///
/// This is deliberately closed and crate-private. It records existing pump
/// behavior; extracting the relay must not silently add trailer or termination
/// support to custom connector sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestRelayProtocol {
    H1,
    H2,
    Custom,
}

impl RequestRelayProtocol {
    fn dispatches_trailer_hook(self) -> bool {
        self != Self::Custom
    }

    fn supports_termination(self) -> bool {
        self != Self::Custom
    }
}

/// One filtered event ready for protocol-specific disposition and writing.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PreparedRequestEvent {
    pub(crate) body: Option<Bytes>,
    pub(crate) event: RequestBodyEvent,
}

impl PreparedRequestEvent {
    pub(crate) fn is_terminal(&self) -> bool {
        self.event.is_terminal()
    }
}

/// The hook which selected request-body termination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestTerminationOrigin {
    TrailerFilter,
    BodyFilter,
}

impl RequestTerminationOrigin {
    pub(crate) fn hook_name(self) -> &'static str {
        match self {
            Self::TrailerFilter => "request_trailer_filter",
            Self::BodyFilter => "request_body_filter_action",
        }
    }
}

/// Semantic result of processing one request-body event.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RequestRelayOutcome {
    Continue(PreparedRequestEvent),
    Terminate(RequestTerminationOrigin),
}

impl<SV, C> HttpProxy<SV, C>
where
    SV: ProxyHttp,
    C: custom::Connector,
{
    /// Process one request-body event without performing protocol I/O.
    ///
    /// The caller must acquire any pipe/capacity permit before entering this
    /// function, preserving the pumps' existing backpressure ordering. Empty
    /// output suppression, `Bodyless` validation, and every actual write stay
    /// in the caller because their consequences remain protocol-specific.
    pub(crate) async fn request_relay_event(
        &self,
        protocol: RequestRelayProtocol,
        session: &mut Session,
        mut body: Option<Bytes>,
        mut event: RequestBodyEvent,
        ctx: &mut SV::CTX,
    ) -> Result<RequestRelayOutcome>
    where
        SV: Send + Sync,
        SV::CTX: Send + Sync,
    {
        // A source EOF is authoritative even when a custom downstream's
        // user-provided `is_body_done()` implementation reports stale state.
        // Without this normalization that source can spin forever on EOF.
        if body.is_none() && event == RequestBodyEvent::Data {
            event = RequestBodyEvent::Complete;
        }

        if protocol.dispatches_trailer_hook()
            && event.is_complete()
            && body.is_none()
            && !session.request_trailer_filter_fired
            && session
                .downstream_session
                .request_trailers_present()
                .unwrap_or(false)
        {
            let action = self.inner.request_trailer_filter(session, ctx).await?;

            // Commit the latch only after a successful hook return. The pump
            // future may be cancelled at the await above, and a retryable hook
            // error must be allowed to run again on the next attempt.
            session.request_trailer_filter_fired = true;
            if action == RequestBodyAction::Terminate {
                return Ok(RequestRelayOutcome::Terminate(
                    RequestTerminationOrigin::TrailerFilter,
                ));
            }
        }

        session
            .downstream_modules_ctx
            .request_body_filter(&mut body, event)
            .await?;

        if self
            .inner
            .request_body_filter_action(session, &mut body, event, ctx)
            .await?
            == RequestBodyAction::Terminate
        {
            if !protocol.supports_termination() {
                return Error::e_explain(
                    InternalError,
                    "request-body terminate is not supported on custom connector sessions",
                );
            }
            return Ok(RequestRelayOutcome::Terminate(
                RequestTerminationOrigin::BodyFilter,
            ));
        }

        Ok(RequestRelayOutcome::Continue(PreparedRequestEvent {
            body,
            event,
        }))
    }
}

#[cfg(test)]
#[path = "request_relay_tests.rs"]
mod tests;
