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

//! H2 upstream request-body framing, writing, and abandonment capability.

use bytes::Bytes;
use futures::future::OptionFuture;
use log::{debug, warn};
use pingora_core::connectors::http::custom;
use pingora_core::protocols::http::v2::{client::PeerEndStream, server::Idle, write_body};
use pingora_error::{
    Error,
    ErrorType::{H2Error, InternalError, WriteError, WriteTimedout},
    Result,
};
use pingora_http::RequestHeader;
use std::time::Duration;

use crate::proxy_common::{warn_terminate_without_response, DownstreamRequestOutcome};
use crate::request_relay::{
    bodyless_contract_violation, violates_bodyless_contract, RequestRelayOutcome,
    RequestRelayProtocol,
};
use crate::{HttpProxy, ProxyHttp, RequestBodyEvent, Session, UpstreamRequestBodyDisposition};

pub(super) fn apply_upstream_body_disposition(
    request: &mut RequestHeader,
    disposition: UpstreamRequestBodyDisposition,
) {
    match disposition {
        UpstreamRequestBodyDisposition::Ordinary => {}
        UpstreamRequestBodyDisposition::Bodyless | UpstreamRequestBodyDisposition::Streamed => {
            request.remove_header(&http::header::CONTENT_LENGTH);
            request.remove_header(&http::header::TRANSFER_ENCODING);
        }
    }
}

/// Whether END_STREAM rides on the upstream HEADERS frame.
///
/// `send_end_stream` is the application-controlled opt-out
/// (`RequestHeader::set_send_end_stream`): the gRPC-web bridge sets it to
/// `false` because gRPC MUST close a bodyless request stream with an empty
/// DATA frame carrying END_STREAM, not with END_STREAM on HEADERS. It
/// therefore has to be honored for `Bodyless` too; only `Streamed`, which by
/// definition cannot know the body is finished at header time, is
/// unconditional.
pub(super) fn upstream_headers_end_stream(
    disposition: UpstreamRequestBodyDisposition,
    send_end_stream: bool,
    body_empty: bool,
) -> bool {
    match disposition {
        UpstreamRequestBodyDisposition::Ordinary => send_end_stream && body_empty,
        UpstreamRequestBodyDisposition::Bodyless => send_end_stream,
        UpstreamRequestBodyDisposition::Streamed => false,
    }
}

/// Whether the request stream must be closed right after the headers with a
/// standalone empty DATA frame carrying END_STREAM.
///
/// Only consulted when [`upstream_headers_end_stream`] said `false`.
/// `body_empty` is whatever [`upstream_framing_body_empty`] selected for this
/// disposition -- NOT a single fact about the request, see that function.
pub(super) fn upstream_empty_data_end_stream(
    disposition: UpstreamRequestBodyDisposition,
    send_end_stream: bool,
    body_empty: bool,
) -> bool {
    match disposition {
        // Exactly the original behavior: the empty-body EOS that could not
        // ride on HEADERS.
        UpstreamRequestBodyDisposition::Ordinary => !send_end_stream && body_empty,
        // The headers deliberately did not carry EOS (`send_end_stream ==
        // false`), and no body will follow: close with the empty DATA frame
        // gRPC requires.
        UpstreamRequestBodyDisposition::Bodyless => true,
        // Nothing will ever be read from downstream, so the pump's normal path
        // would never send an EOS: close now. When a body does exist, the loop
        // sends EOS with (or after) the last DATA frame as usual.
        //
        // `upstream_framing_body_empty` pins this input to `false` for
        // `Streamed`, so this arm is unreachable from the pump. It is kept
        // because the primitive is meaningful on its own; do NOT "simplify" the
        // call site by feeding it the request's declaration, which is what
        // revives it -- see `upstream_framing_body_empty`.
        UpstreamRequestBodyDisposition::Streamed => body_empty,
    }
}

/// The `body_empty` input the two framing decisions above are made with.
///
/// There is no single right answer, which is exactly the trap: the two
/// dispositions want DIFFERENT facts, and feeding one fact to both is a bug in
/// either direction.
///
/// - `Ordinary` takes the request's own DECLARATION (`is_body_empty()`).
///   `Content-Length: 0` promises zero DATA payload bytes but says nothing about
///   END_STREAM (design 4.3), so an H2 request can declare it while its stream
///   is still open. Forwarding that promise upstream is right: an origin that
///   does not answer until it sees the end of the request would otherwise
///   deadlock, and the futile-read rule cannot rescue it because that rule
///   requires a complete response first. The second, standalone END_STREAM that
///   the client's real EOS would later produce is suppressed by
///   `upstream_body_closed` in `proxy_down_to_up`.
///
/// - `Streamed` must NEVER send an early EOS (design 4.4). The application is
///   about to stream a body in through `request_body_filter_action`; closing the
///   upstream request stream at header time would set `stream_closed`, and every
///   byte it streams would then be refused by the suppressed-write branch of
///   `send_body_to2`. `safe_disposition` has already coerced `Streamed` to
///   `Ordinary` for every request whose body is provably absent
///   (`facts.body_empty`), so the strict fact is `false` here by construction --
///   this returns it explicitly rather than relying on that.
///
/// - `Bodyless` does not consult this value at all (both framing functions
///   ignore it), so the choice is immaterial.
pub(super) fn upstream_framing_body_empty(
    disposition: UpstreamRequestBodyDisposition,
    body_empty_declared: bool,
) -> bool {
    match disposition {
        UpstreamRequestBodyDisposition::Ordinary => body_empty_declared,
        UpstreamRequestBodyDisposition::Bodyless => false,
        UpstreamRequestBodyDisposition::Streamed => false,
    }
}

/// What one downstream request-body event did to the upstream request stream.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum UpstreamBodyOutcome {
    /// The event was forwarded (or deliberately not written), and the pump's
    /// downstream read side is governed by the wrapped outcome as usual.
    Downstream(DownstreamRequestOutcome),
    /// The upstream refused the write on a stream whose response the peer had
    /// ALREADY flagged complete on the wire (RFC 9113 §8.1: "stop uploading, I
    /// have answered you"). Nothing more may be written, but nothing is lost
    /// either -- see [`upstream_write_error_outcome`].
    UpstreamDoneReceiving {
        /// Whether the event whose write failed was itself the end of the
        /// downstream body, i.e. whether the application's single
        /// terminal request-body event has already been delivered.
        ///
        /// The canonical §8.1 shape fails on a MID-body write, where it has
        /// not: the handler then owes the application that event before it
        /// takes the downstream read side out of the loop, because nothing else
        /// will ever deliver it. See the arms in `bidirection_down_to_up`.
        terminal_event_delivered: bool,
    },
}

/// How the pump may write the upstream request body on this attempt.
#[derive(Debug, Clone)]
pub(super) struct UpstreamBodyWrite {
    /// Per-write timeout from the peer options. `None` means the caller did
    /// not choose a timeout; the H2 pump still applies its protocol liveness
    /// floor through [`effective_upstream_write_timeout`].
    pub(super) timeout: Option<Duration>,
    /// The upstream request stream already carries its END_STREAM, so no
    /// request body byte may be written at all -- h2 answers a DATA frame on a
    /// locally half-closed stream with `UnexpectedFrameType`. See where this is
    /// computed in `proxy_down_to_up`.
    pub(super) stream_closed: bool,
    /// The disposition the application selected, AFTER `safe_disposition`
    /// coercion. Carried into the pump so that a `Bodyless` declaration
    /// contradicted by real downstream body bytes can be failed closed at the
    /// point of detection; see `violates_bodyless_contract`.
    pub(super) disposition: UpstreamRequestBodyDisposition,
    /// Whether a failure to put the terminating END_STREAM on the upstream
    /// request stream may be ignored.
    ///
    /// Set only by the futile-read branch, which by construction runs AFTER the
    /// upstream response is complete. The frame is still owed and still sent --
    /// dropping the `SendStream` instead would make h2 emit a gratuitous
    /// RST_STREAM(CANCEL) per request, inflating exactly the post-CVE-2023-44487
    /// abuse counters this file is careful about elsewhere -- but the peer may
    /// legitimately have closed the stream first with RFC 9113 §8.1's
    /// RST_STREAM(NO_ERROR) ("response complete, stop uploading"), which makes
    /// the write fail. That failure costs the exchange nothing: the response is
    /// already in hand. It is swallowed rather than classified because h2 does
    /// not expose a reason at the write site at all -- see the TODO in
    /// `Http2Session::read_trailers` for why `UserError::InactiveStreamId` and
    /// `poll_reset` cannot distinguish the cases.
    pub(super) eos_write_optional: bool,
    /// Source (iv) for the UPSTREAM stream: whether the peer flagged the end of
    /// its response on the wire before tearing the stream down.
    ///
    /// This is the one fact about a failed h2 write that the write site can
    /// establish, and it is what `upstream_write_error_outcome` classifies a
    /// write failure with. The response half is read concurrently in the other
    /// arm of `proxy_down_to_up`'s `select!`, so the session itself is
    /// unreachable from here; the flag is a cheap `Arc` handle sampled at the
    /// moment the write fails.
    pub(super) upstream_response_ended: PeerEndStream,
}

/// Classify a failed upstream request-body write.
///
/// # Why swallowing this cannot deliver a truncated response
///
/// The premise of RFC 9113 §8.1's shape is that this side is still uploading --
/// that is *why* the origin resets -- so the write failing is not evidence of
/// anything going wrong: with `upstream_response_ended` set, the peer had
/// already put END_STREAM on the wire before the frame that killed the stream.
/// Failing the exchange there would throw away a response the proxy already
/// holds because of an upload the origin explicitly said it no longer wants.
///
/// The swallow cannot launder a truncation, because it decides NOTHING about
/// the response:
///
/// - Response completeness is decided exclusively by the READ half
///   (`pipe_up_to_down_response` -> `Http2Session::read_response_body` /
///   `read_trailers`), which applies its own, stricter guard: the wire flag is
///   consulted only once a read has actually failed, only for a `NO_ERROR`
///   remote stream end, and only when the wire's DATA byte count for the stream
///   matches what was actually read. A truncated body fails at least one of
///   those (see `Http2Session::response_body_complete_at_stream_end`), so the
///   read half still errors, still emits `HttpTask::Failed`, and the exchange
///   still fails -- with or without this classification. Note that the flag
///   this function consults is the WEAK one and deliberately so: it is asking
///   "did the origin say it was done with me", not "is the response whole".
/// - Nothing here reports success downstream. It only stops the pump from
///   turning the failed WRITE into the error that ends the exchange.
///
/// For the same reason it is safe that the flag says nothing about the reset's
/// error code: a non-`NO_ERROR` reset after a complete response leaves the flag
/// set, but the read half rejects that reason and fails the exchange anyway.
/// Without the flag -- any other write failure, and every stream whose response
/// the peer never flagged complete -- the error is returned unchanged.
///
/// # Why the flag is not sufficient on its own
///
/// `upstream_response_ended` is set for EVERY upstream response the peer ended
/// cleanly, reset or not, and it stays set for the life of the exchange. Taken
/// alone it would therefore swallow every request-body write failure that can
/// happen after the response arrives, including ones that have nothing to do
/// with the peer having stopped listening -- an application body filter's own
/// error, the `Bodyless` contract violation, a cache failure. So the
/// classification also requires the failure SHAPE to be one the peer's
/// behavior explains. Exactly two shapes qualify:
///
/// - [`upstream_write_failed_because_stream_gone`] -- h2 will never take
///   another byte on this stream, because the stream is closed or the whole
///   connection is.
/// - [`upstream_write_stalled_after_response`] -- the stream is still there,
///   but the peer granted no flow-control capacity for a whole `write_timeout`
///   window after having already flagged its response complete.
///
/// # Why the stalled shape is accepted, having once been refused
///
/// The second shape used to be refused on the grounds that a locally
/// configured `write_timeout` is not a peer signal, and that swallowing it
/// would truncate the upstream request body and report a success. That
/// reasoning rests on a premise which holds for the reset shape and fails for
/// this one: that the response is already safely in hand. It is not. The pump
/// awaits the request-body write INLINE in the duplex loop's downstream arm
/// (see `bidirection_down_to_up`), so while that write is blocked the loop is
/// not draining `rx` either, and the upstream response tasks sit undelivered
/// in a `TASK_BUFFER_SIZE` channel. Failing the exchange therefore answers the
/// client with a 502 while a complete response is sitting in the proxy's own
/// buffer -- and with no `write_timeout` configured at all the write never
/// returns and the client is answered never (that is what the stall probe in
/// `send_body_to2` bounds).
///
/// Delivering the response the origin actually sent is the best of those three
/// outcomes, and it conceals nothing:
///
/// - The origin learns. The request half never receives its END_STREAM, so
///   dropping the `SendStream` at the end of the exchange makes h2 emit
///   RST_STREAM(CANCEL) -- the standard "upload aborted" signal. An origin
///   that really was still consuming the body sees a truncated request rather
///   than a whole one.
/// - The operator learns. The swallow is logged at `warn` with the failure
///   attached.
/// - The response is not laundered. As above, completeness is still decided
///   exclusively by the read half.
pub(super) fn upstream_write_error_outcome(
    e: Box<Error>,
    terminal_event_delivered: bool,
    body_write: &UpstreamBodyWrite,
) -> Result<UpstreamBodyOutcome> {
    if !body_write.upstream_response_ended.observed() {
        return Err(e.into_up());
    }
    if upstream_write_failed_because_stream_gone(&e) {
        warn!(
            target: "pingora_proxy::proxy_h2",
            "upstream stopped receiving the request body after flagging its response complete: {e}"
        );
    } else if upstream_write_stalled_after_response(&e) {
        warn!(
            target: "pingora_proxy::proxy_h2",
            "upstream granted no request-body capacity for a whole write window after flagging \
             its response complete; the upstream request body is truncated: {e}"
        );
    } else {
        return Err(e.into_up());
    }
    Ok(UpstreamBodyOutcome::UpstreamDoneReceiving {
        terminal_event_delivered,
    })
}

/// Release flow-control capacity requested by an upstream body write that the
/// pump has decided never to resume.
///
/// Dropping the capacity-wait future does not cancel its request: h2 keeps the
/// reservation on the `SendStream`, and capacity assigned later cannot be used
/// by sibling streams. Every successful upload-abandonment path must therefore
/// pass through this helper while the send handle is still alive.
pub(super) fn cancel_abandoned_upstream_body_capacity(client_body: &mut h2::SendStream<Bytes>) {
    client_body.reserve_capacity(0);
}

/// Whether a failed `write_body` means the upstream request stream is GONE, as
/// opposed to still being there and merely not cooperating.
///
/// `write_body` fails in exactly three shapes, and only two of them are the
/// peer telling us something:
pub(super) fn upstream_write_failed_because_stream_gone(e: &Error) -> bool {
    match e.etype {
        // `reserve_and_send`: `poll_capacity` answered `Ready(None)` ("cannot
        // reserve capacity"), or yielded the stream's own `h2::Error`. Both
        // mean h2 will never accept another byte on this stream -- it is closed
        // or the whole connection is.
        H2Error => true,
        // `SendStream::send_data` refused the frame, which for a stream h2 has
        // already closed is `UserError::InactiveStreamId`. Same conclusion.
        WriteError => true,
        // A LOCAL deadline, from `peer.options.write_timeout`. The stream may be
        // perfectly alive and the peer merely slow (or withholding flow-control
        // window), so it is NOT evidence that the stream is gone and the answer
        // here stays `false`. It is nonetheless swallowed when the peer had
        // already flagged its response complete -- but through the separate
        // [`upstream_write_stalled_after_response`] shape, which asks a
        // different question. See `upstream_write_error_outcome`.
        WriteTimedout => false,
        _ => false,
    }
}

/// Whether a failed `write_body` means the upstream request stream is STALLED
/// after having already answered, as opposed to being GONE.
///
/// Deliberately a separate question from
/// [`upstream_write_failed_because_stream_gone`]: that one asks whether h2 will
/// ever take another byte on this stream, and a local deadline genuinely does
/// not answer it. This one is only ever asked in conjunction with
/// `upstream_response_ended`, and the conjunction is what carries the meaning.
/// Neither half means anything alone -- a timeout without the wire flag is just
/// a slow origin and must keep failing the exchange, and the wire flag without
/// a timeout is the ordinary early-response shape that must keep uploading,
/// because RFC 9113 lets a server that has answered in full go on receiving the
/// request body.
pub(super) fn upstream_write_stalled_after_response(e: &Error) -> bool {
    // The one shape `write_body` gives a stall. Note that `write_timeout`
    // bounds ONE `reserve_and_send`, i.e. one capacity grant, and is re-armed
    // for each -- so this is "the peer granted nothing for a whole window", not
    // "the chunk was too large to finish in time". A peer that keeps granting
    // window, however slowly, never produces it.
    matches!(e.etype, WriteTimedout)
}

/// How long the downstream request body may be drained for once the pump has
/// stopped reading it. Generous enough that an ordinary in-flight upload
/// finishes and the connection stays reusable; finite so that one cannot hold
/// the connection and its task open indefinitely.
pub(super) const ABANDONED_BODY_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound the drain of a downstream request body the pump has stopped reading.
///
/// `UpstreamDoneReceiving` returns the exchange as a SUCCESS, so unlike the two
/// `Terminate` paths nothing here clears keepalive -- and for an H1 downstream
/// that means `finish()` will DRAIN whatever the client is still uploading in
/// order to reuse the socket, bounded only by `total_drain_timeout`, which
/// defaults to `None`. A client with a multi-gigabyte upload in flight would
/// otherwise hold the connection, and this task, for as long as it cared to
/// keep writing.
///
/// Bounding is chosen over the terminate paths' `set_keepalive(None)`, and the
/// difference is not cosmetic: those paths have no response left to protect,
/// whereas this one is in the middle of delivering a complete response the
/// section 8.1 handling just rescued. Closing an H1 socket that still has an
/// unread multi-megabyte upload sitting in its receive queue makes the kernel
/// send RST rather than FIN, and the RST discards whatever the client has not
/// yet read -- so clearing keepalive here trades an unbounded drain for
/// intermittently truncating the very response this code exists to deliver.
/// That was measured, not theorised: it cost
/// `h2_upstream_no_error_reset_keeps_streaming_while_the_client_uploads` about
/// one run in fifteen.
///
/// An application that has set its own drain timeout keeps it. A no-op when the
/// downstream body is already done, which is the common case.
pub(super) fn bound_undrained_downstream_body(session: &mut Session) {
    if session.as_mut().is_body_done() || session.as_mut().get_total_drain_timeout().is_some() {
        return;
    }
    debug!(
        target: "pingora_proxy::proxy_h2",
        "the upstream stopped receiving before the downstream request body ended; \
         bounding the drain at {ABANDONED_BODY_DRAIN_TIMEOUT:?}"
    );
    session
        .as_mut()
        .set_total_drain_timeout(Some(ABANDONED_BODY_DRAIN_TIMEOUT));
}

/// How often a request-body write that is blocked on upstream flow control
/// re-checks whether the upstream has already flagged its response complete.
///
/// This is a PROBE interval, not a deadline: its expiry decides nothing on its
/// own. It only creates the opportunity to sample `upstream_response_ended`,
/// which is the fact that makes abandoning the upload safe. With no such
/// evidence the write goes on waiting exactly as it did before.
///
/// Armed ONLY when the caller configured no `write_timeout` of its own. A
/// consumer that set one has already said how long a stalled write may last,
/// and a probe firing first would silently override that configuration; the
/// stalled case then arrives through [`upstream_write_stalled_after_response`]
/// instead, on the operator's schedule. So this is not a knob for how patient
/// the proxy should be. It is the bound that keeps an explicitly unbounded
/// configuration from meaning "wait forever" once the origin has answered in
/// full -- h2 has no signal for "this peer will never grant window again", so
/// without something like this the wait has no end at all.
///
/// Generous for a reason: unlike `write_timeout`, which `write_body` re-arms
/// around each capacity grant and which therefore measures a LACK OF PROGRESS,
/// this probe cannot see progress made within a chunk. It must stay far above
/// any plausible time a healthy origin takes to accept one chunk, so that the
/// only writes it ever ends are the ones that were never going to finish. One
/// chunk is one downstream read -- an h2 DATA frame, or an H1 read buffer --
/// so tens of kilobytes at most; ten seconds is three orders of magnitude more
/// than an origin that is still draining the upload needs for one of them.
pub(super) const UPSTREAM_STALL_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// H2 request-body write-progress floor when the caller supplied no timeout.
///
/// `h2::SendStream::poll_capacity` has no deadline of its own. Leaving a write
/// completely unbounded therefore lets a peer that withholds both flow-control
/// capacity and response END_STREAM retain the whole exchange forever. Keep
/// this local to the H2 request pump: changing `PeerOptions::write_timeout`
/// would also change H1 and custom-upstream defaults.
///
/// A configured timeout always wins, even when it is longer than this floor.
/// Like `write_body`'s normal timeout, this bounds one capacity wait rather
/// than the total upload, so a progressing upload can run for longer.
pub(super) const DEFAULT_H2_UPSTREAM_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

#[inline]
pub(super) fn effective_upstream_write_timeout(configured: Option<Duration>) -> Duration {
    configured.unwrap_or(DEFAULT_H2_UPSTREAM_WRITE_TIMEOUT)
}

/// How one request-body write ended.
pub(super) enum UpstreamBodyWriteEnd {
    /// `write_body` returned, successfully or not.
    Wrote(Result<()>),
    /// The DOWNSTREAM H2 stream closed while the write was in flight.
    DownstreamClosed(Result<h2::Reason>),
    /// The upstream granted no request-body capacity for a whole
    /// [`UPSTREAM_STALL_PROBE_INTERVAL`] on a stream whose response it had
    /// ALREADY flagged complete on the wire, with no `write_timeout` to bound
    /// the wait. Same conclusion as [`upstream_write_stalled_after_response`],
    /// reached without a configured deadline.
    StalledAfterResponse,
}

/// Write one request-body event upstream, watching for the two things that can
/// make the write pointless while it is blocked on upstream flow control.
///
/// Both watchers exist because a wait on `poll_capacity` has no end of its own:
/// h2 reports "the stream is closed" and "the stream was reset", but nothing
/// distinguishes a peer that is about to grant window from one that never will.
pub(super) async fn write_upstream_body_watching_stall(
    client_body: &mut h2::SendStream<Bytes>,
    data: Bytes,
    end: bool,
    body_write: &UpstreamBodyWrite,
    // `Idle` borrows the downstream session mutably and is `Unpin`, so the loop
    // re-polls it by reference rather than moving it into a branch.
    mut stream_close: Option<Idle<'_>>,
) -> UpstreamBodyWriteEnd {
    let write = write_body(
        client_body,
        data,
        end,
        Some(effective_upstream_write_timeout(body_write.timeout)),
    );
    tokio::pin!(write);
    // Only a write nobody else bounded needs the probe; see the constant.
    let probe_stall = body_write.timeout.is_none();

    loop {
        tokio::select! {
            biased;
            // `&mut write`, NOT `write`: a probe tick must not cancel the
            // write. `write_body` tracks how much of the chunk it has already
            // put on the wire in its own local state, so dropping the future
            // would lose that -- a partial chunk cannot be resumed without
            // re-sending bytes. Re-polling the same future continues exactly
            // where it stopped, which is what lets the probe tick harmlessly.
            res = &mut write => return UpstreamBodyWriteEnd::Wrote(res),
            // Disabled for non-H2 downstreams: `OptionFuture::from(None)` is
            // immediately `Ready(None)`, and the failed pattern match takes
            // this branch out of the select rather than resolving it.
            Some(closed) = OptionFuture::from(stream_close.as_mut()) => {
                return UpstreamBodyWriteEnd::DownstreamClosed(closed);
            }
            // `pingora_timeout`, the same timer `write_body` itself uses, and
            // not `tokio::time`. This is a per-body-chunk timer, so its cost
            // scales with request rate: `pingora_timeout` measures ~4ns per
            // create/cancel against Tokio's ~107ns, and deadlines rounded to
            // the same 10ms tick SHARE one timer, so concurrent probes mostly
            // subscribe to an existing one instead of allocating.
            //
            // A cancelled fast timer does leave its entry in `TimerManager`
            // until the deadline passes, which is why the crate falls back to
            // Tokio for long deadlines -- but its own threshold for "long" is
            // fifteen minutes, and the sharing bounds the residue at one entry
            // per 10ms tick of the interval per thread, independent of request
            // rate. Ten seconds is not in that territory.
            //
            // Nothing is created at all on the common path: `select!` does not
            // evaluate a branch's future expression while its precondition is
            // false, so a caller with a `write_timeout` pays nothing here.
            _ = pingora_timeout::sleep(UPSTREAM_STALL_PROBE_INTERVAL), if probe_stall => {
                if body_write.upstream_response_ended.observed() {
                    return UpstreamBodyWriteEnd::StalledAfterResponse;
                }
                // No wire evidence, so nothing has been learned: go on waiting,
                // exactly as this write did before the probe existed.
            }
        }
    }
}

impl<SV, C> HttpProxy<SV, C>
where
    C: custom::Connector,
{
    /// Deliver the downstream body's single `Abandoned` terminal event for a
    /// pump that is about to stop reading without having seen its end.
    ///
    /// Returns whether the application asked to terminate.
    ///
    /// `UpstreamDoneReceiving` on a MID-body write -- the canonical RFC 9113
    /// §8.1 shape, where the origin resets precisely because we are still
    /// uploading -- sets `downstream_state` to finished and disables the read
    /// branch. That also disables `downstream_body_read_is_futile`, which needs
    /// `is_reading()`, so nothing downstream of here would ever run the hooks
    /// with a terminal event: `request_body_filter_action` would
    /// never see the end of the body, `request_trailer_filter` would never
    /// fire, and the downstream body modules would be left mid-stream -- while
    /// the request completed 200 and logged as a success. This is the same
    /// invariant the futile-read branch protects, paid the same way.
    ///
    /// `stream_closed` is forced here (unlike in the futile-read branch): the
    /// upstream request stream is provably gone -- that is what
    /// `UpstreamDoneReceiving` means -- so the terminating END_STREAM is not
    /// owed and attempting it would only fail again.
    pub(super) async fn finish_downstream_body_side(
        &self,
        session: &mut Session,
        client_body: &mut h2::SendStream<bytes::Bytes>,
        ctx: &mut SV::CTX,
        body_write: &UpstreamBodyWrite,
    ) -> Result<bool>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let outcome = self
            .send_body_to2(
                session,
                None,
                RequestBodyEvent::Abandoned,
                client_body,
                ctx,
                &UpstreamBodyWrite {
                    stream_closed: true,
                    eos_write_optional: true,
                    ..body_write.clone()
                },
            )
            .await?;
        Ok(outcome == UpstreamBodyOutcome::Downstream(DownstreamRequestOutcome::Terminate))
    }

    pub(super) async fn send_body_to2(
        &self,
        session: &mut Session,
        data: Option<Bytes>,
        event: RequestBodyEvent,
        client_body: &mut h2::SendStream<bytes::Bytes>,
        ctx: &mut SV::CTX,
        body_write: &UpstreamBodyWrite,
    ) -> Result<UpstreamBodyOutcome>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let prepared = match self
            .request_relay_event(RequestRelayProtocol::H2, session, data, event, ctx)
            .await?
        {
            RequestRelayOutcome::Continue(prepared) => prepared,
            RequestRelayOutcome::Terminate(origin) => {
                warn_terminate_without_response(session, origin.hook_name());
                return Ok(UpstreamBodyOutcome::Downstream(
                    DownstreamRequestOutcome::Terminate,
                ));
            }
        };
        let end_of_body = prepared.is_terminal();
        let data = prepared.body;

        /* it is normal to get 0 bytes because of multi-chunk parsing or request_body_filter.
         * Although there is no harm writing empty byte to h2, unlike h1, we ignore it
         * for consistency */
        if !end_of_body && data.as_ref().is_some_and(|d| d.is_empty()) {
            return Ok(UpstreamBodyOutcome::Downstream(
                DownstreamRequestOutcome::Complete(false),
            ));
        }

        // Fail closed on a `Bodyless` declaration the downstream body has just
        // disproved. Checked here -- after the request-body filters, before the
        // suppressed-write branch below -- because the upstream request stream
        // already carries its END_STREAM, so these bytes would be dropped and
        // the client would be told the request succeeded. See
        // `bodyless_contract_violation`.
        if violates_bodyless_contract(body_write.disposition, data.as_ref()) {
            return Err(bodyless_contract_violation());
        }

        if body_write.stream_closed {
            // The upstream request stream already carries its END_STREAM, so
            // there is nothing left to write -- but the application hooks
            // above still ran, and the state machine still has to advance. See
            // `upstream_body_closed` in `proxy_down_to_up` for how this state
            // is reached; writing here would make h2 fail the stream with
            // `UnexpectedFrameType` and cost the DOWNSTREAM connection its
            // remaining body events, its end-of-stream event and its
            // keepalive.
            //
            // Real bytes here contradict the empty-body declaration the upstream
            // framing was built from. This is a DIAGNOSTIC for application
            // misuse, deliberately NOT the fail-closed contract that
            // `bodyless_contract_violation` implements above -- be precise about
            // the difference before "unifying" the two:
            //
            // - It is not reachable from wire traffic. `h2` enforces
            //   `content-length` on receive, so a client that declares
            //   `Content-Length: 0` and then sends a DATA frame has its stream
            //   killed with a protocol error before the bytes reach this
            //   function. Only an application that INJECTS bytes from
            //   `request_body_filter_action` can get here.
            // - The error does not fail the request under `Ordinary`/`Streamed`:
            //   the duplex loop's downstream arm absorbs it into `to_errored()`
            //   (only `Bodyless` is re-raised there, on purpose). So this
            //   produces a log line and a truncated upstream request body, not a
            //   500.
            //
            // Returning an error rather than silently dropping the bytes is
            // still the right call -- it marks the downstream non-reusable and
            // stops the pump reading more of a body it cannot forward -- but do
            // not read it as a security boundary.
            if data.as_ref().is_some_and(|d| !d.is_empty()) {
                return Error::e_explain(
                    InternalError,
                    "downstream request body bytes arrived after the upstream request stream \
                     was closed by the request's own empty-body declaration",
                );
            }
            debug!(target: "pingora_proxy::proxy_h2", "upstream request stream already closed; not writing the end of stream");
            return Ok(UpstreamBodyOutcome::Downstream(
                DownstreamRequestOutcome::Complete(end_of_body),
            ));
        }

        let (data, end, eos_write_optional) = match data {
            Some(data) => {
                debug!(target: "pingora_proxy::proxy_h2", "Write {} bytes body to h2 upstream", data.len());
                (data, end_of_body, false)
            }
            None => {
                debug!(target: "pingora_proxy::proxy_h2", "Read downstream body done");
                /* send a standalone END_STREAM flag */
                (Bytes::new(), true, body_write.eos_write_optional)
            }
        };

        /* For H2 downstreams, race the upstream write against downstream stream
         * closure. A write blocked on upstream flow control would otherwise keep the
         * downstream stream handles referenced while a downstream RST_STREAM goes
         * unobserved, pinning the downstream connection window credit until the
         * write completes. The same write is also watched for an upstream that has
         * answered in full and then stopped granting capacity; see
         * `write_upstream_body_watching_stall`. */
        // Bound with `let` rather than matched in place: the future borrows both
        // `client_body` and the downstream session, and a match scrutinee would
        // hold those borrows across the arms below.
        let write_end = write_upstream_body_watching_stall(
            client_body,
            data,
            end,
            body_write,
            session.downstream_session.watch_h2_stream_close(),
        )
        .await;

        let write_result = match write_end {
            UpstreamBodyWriteEnd::Wrote(res) => res,
            UpstreamBodyWriteEnd::DownstreamClosed(close_result) => {
                return match close_result {
                    Ok(reason) => Error::e_explain(
                        H2Error,
                        format!("downstream H2 stream closed (reason: {reason}) while writing body to upstream"),
                    ),
                    Err(e) => Err(e),
                }
                .map_err(|e| e.into_down());
            }
            UpstreamBodyWriteEnd::StalledAfterResponse => {
                // The write future is gone by now, so the capacity it was
                // holding out for can be handed back to the connection instead
                // of staying reserved for a stream nothing will write again.
                cancel_abandoned_upstream_body_capacity(client_body);
                warn!(
                    target: "pingora_proxy::proxy_h2",
                    "upstream granted no request-body capacity for \
                     {UPSTREAM_STALL_PROBE_INTERVAL:?} after flagging its response complete; \
                     abandoning the upload so the response can be delivered"
                );
                // `eos_write_optional` needs no arm of its own: it is only ever
                // set for an EMPTY write, which `write_body` completes without
                // waiting for capacity at all, so this outcome cannot arise for
                // one.
                return Ok(UpstreamBodyOutcome::UpstreamDoneReceiving {
                    terminal_event_delivered: end,
                });
            }
        };

        if let Err(e) = write_result {
            if eos_write_optional {
                debug!(target: "pingora_proxy::proxy_h2", "upstream request stream would not take the final END_STREAM: {e}");
            } else {
                let outcome = upstream_write_error_outcome(e, end, body_write)?;
                cancel_abandoned_upstream_body_capacity(client_body);
                return Ok(outcome);
            }
        }

        Ok(UpstreamBodyOutcome::Downstream(
            DownstreamRequestOutcome::Complete(end_of_body),
        ))
    }
}
