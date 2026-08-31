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

//! Shared duplex-pump termination outcomes, diagnostics, and cleanup.
//!
//! Protocol pumps retain ownership of their select loops, I/O, framing,
//! stream or connection cleanup, cache finalization, and reuse decisions.

use crate::proxy_common::{DownstreamStateMachine, ResponseStateMachine};
use crate::Session;
use log::{debug, warn};
use pingora_cache::NoCacheReason;
use pingora_error::{BError, Result};
use std::future::Future;

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
pub(super) fn downstream_body_read_is_futile(
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
        target: "pingora_proxy::proxy_common",
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
pub(super) fn release_cache_on_terminate(session: &mut Session) {
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
pub(super) fn warn_terminate_without_response(session: &Session, hook: &str) {
    if session.response_written().is_none() {
        warn!(
        target: "pingora_proxy::proxy_common",
            "{hook} returned Terminate without a downstream response having been \
             written; the client will see a bare connection close"
        );
    } else if session.response_body_finished() == Some(false) {
        warn!(
        target: "pingora_proxy::proxy_common",
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
pub(super) fn warn_response_body_terminate_without_response(session: &Session, hook: &str) {
    if session.response_written().is_none() {
        warn!(
        target: "pingora_proxy::proxy_common",
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
pub(super) fn warn_response_body_terminate_content_length_leak(session: &Session, hook: &str) {
    let Some(header) = session.response_written() else {
        return;
    };
    if header.headers.contains_key(http::header::CONTENT_LENGTH) {
        // Same data-plane-reachable rule as `bodyless_contract_violation` in
        // `request_relay.rs`: the trigger is an application-contract
        // violation, but a single mis-declaring processor puts any client
        // request that reaches the terminate condition (for this fork's
        // gateway consumer, any client that deliberately exhausts its own
        // quota) on this path, so no `debug_assert!`/`panic!` here -- do not
        // re-add one.
        warn!(
        target: "pingora_proxy::proxy_common",
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
pub(super) async fn finish_terminated_response(session: &mut Session) {
    if let Err(e) = session.as_mut().finish_body().await {
        // Nothing left to salvage: the request is ending either way, and the
        // connection is already marked non-reusable.
        warn!(target: "pingora_proxy::proxy_common", "Error finishing the downstream response body on terminate: {e}");
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
pub(super) enum DownstreamRequestOutcome {
    /// Normal completion; the bool is downstream connection reusability.
    Complete(bool),
    /// The downstream response completed successfully after the selected
    /// origin became unusable or irrelevant (for example, a source failure
    /// after the terminal boundary or a bounded local replacement). The bool
    /// is downstream connection reusability; the upstream connection/stream
    /// must not be reused.
    CompleteWithoutUpstreamReuse(bool),
    /// Application termination: proxying of this request stops here. The
    /// application has already finished the downstream response.
    Terminate,
}

/// Result of joining the two halves of a bidirectional proxy pump.
///
/// `OriginAbandoned` intentionally carries no upstream result: the sibling
/// future is dropped as soon as a complete replacement response makes the
/// selected origin irrelevant. Keeping this distinction here prevents both
/// the H1 and H2 transports from accidentally waiting for origin EOS before
/// performing their protocol-specific non-reuse/reset cleanup.
pub(super) enum DuplexPumpOutcome<T> {
    ApplicationTerminate {
        /// Present only when the upstream half had already completed before
        /// the downstream application selected termination.
        upstream: Option<T>,
    },
    Complete {
        downstream_can_reuse: bool,
        upstream: T,
    },
    OriginAbandoned {
        downstream_can_reuse: bool,
    },
    Failed(BError),
}

/// Join request/downstream processing with the upstream response reader.
///
/// Normal downstream completion still waits for the upstream half so request
/// writes and response reads settle naturally. Application termination and a
/// replacement response instead stop immediately and drop the sibling future.
pub(super) async fn join_bidirectional_pumps<D, U, T>(
    downstream: D,
    upstream: U,
) -> DuplexPumpOutcome<T>
where
    D: Future<Output = Result<DownstreamRequestOutcome>>,
    U: Future<Output = Result<T>>,
{
    tokio::pin!(downstream);
    tokio::pin!(upstream);

    tokio::select! {
        // If application termination or origin abandonment and an upstream
        // error become ready in the same poll, the typed downstream outcome
        // wins. It already owns the final downstream response semantics.
        biased;

        downstream_result = &mut downstream => {
            match downstream_result {
                Ok(DownstreamRequestOutcome::Terminate) => {
                    DuplexPumpOutcome::ApplicationTerminate { upstream: None }
                }
                Ok(DownstreamRequestOutcome::Complete(downstream_can_reuse)) => {
                    match upstream.await {
                        Ok(upstream) => DuplexPumpOutcome::Complete {
                            downstream_can_reuse,
                            upstream,
                        },
                        Err(error) => DuplexPumpOutcome::Failed(error),
                    }
                }
                Ok(DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(
                    downstream_can_reuse,
                )) => DuplexPumpOutcome::OriginAbandoned {
                    downstream_can_reuse,
                },
                Err(error) => DuplexPumpOutcome::Failed(error),
            }
        }
        upstream_result = &mut upstream => {
            match upstream_result {
                Ok(upstream) => match downstream.await {
                    Ok(DownstreamRequestOutcome::Terminate) => {
                        DuplexPumpOutcome::ApplicationTerminate {
                            upstream: Some(upstream),
                        }
                    }
                    Ok(DownstreamRequestOutcome::Complete(downstream_can_reuse)) => {
                        DuplexPumpOutcome::Complete {
                            downstream_can_reuse,
                            upstream,
                        }
                    }
                    Ok(DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(
                        downstream_can_reuse,
                    )) => DuplexPumpOutcome::OriginAbandoned {
                        downstream_can_reuse,
                    },
                    Err(error) => DuplexPumpOutcome::Failed(error),
                },
                Err(error) => DuplexPumpOutcome::Failed(error),
            }
        }
    }
}
