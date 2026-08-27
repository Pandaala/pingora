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

use futures::future::OptionFuture;
use futures::StreamExt;

use super::*;
use crate::proxy_cache::{
    drain_emitted_chunks, drain_emitted_chunks_before, range_filter::RangeBodyFilter,
    ServeFromCache,
};
use crate::proxy_common::*;
use http::{header::CONTENT_LENGTH, Method, StatusCode};
use pingora_core::protocols::http::custom::CUSTOM_MESSAGE_QUEUE_SIZE;
use pingora_core::protocols::http::v2::{
    client::{Http2Session, PeerEndStream},
    server::Idle,
    write_body,
};

fn apply_upstream_body_disposition(
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
fn upstream_headers_end_stream(
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
fn upstream_empty_data_end_stream(
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
fn upstream_framing_body_empty(
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
enum UpstreamBodyOutcome {
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
struct UpstreamBodyWrite {
    /// Per-write timeout from the peer options.
    timeout: Option<Duration>,
    /// The upstream request stream already carries its END_STREAM, so no
    /// request body byte may be written at all -- h2 answers a DATA frame on a
    /// locally half-closed stream with `UnexpectedFrameType`. See where this is
    /// computed in `proxy_down_to_up`.
    stream_closed: bool,
    /// The disposition the application selected, AFTER `safe_disposition`
    /// coercion. Carried into the pump so that a `Bodyless` declaration
    /// contradicted by real downstream body bytes can be failed closed at the
    /// point of detection; see `violates_bodyless_contract`.
    disposition: UpstreamRequestBodyDisposition,
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
    eos_write_optional: bool,
    /// Source (iv) for the UPSTREAM stream: whether the peer flagged the end of
    /// its response on the wire before tearing the stream down.
    ///
    /// This is the one fact about a failed h2 write that the write site can
    /// establish, and it is what `upstream_write_error_outcome` classifies a
    /// write failure with. The response half is read concurrently in the other
    /// arm of `proxy_down_to_up`'s `select!`, so the session itself is
    /// unreachable from here; the flag is a cheap `Arc` handle sampled at the
    /// moment the write fails.
    upstream_response_ended: PeerEndStream,
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
fn upstream_write_error_outcome(
    e: Box<Error>,
    terminal_event_delivered: bool,
    body_write: &UpstreamBodyWrite,
) -> Result<UpstreamBodyOutcome> {
    if !body_write.upstream_response_ended.observed() {
        return Err(e.into_up());
    }
    if upstream_write_failed_because_stream_gone(&e) {
        warn!(
            "upstream stopped receiving the request body after flagging its response complete: {e}"
        );
    } else if upstream_write_stalled_after_response(&e) {
        warn!(
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

/// Whether a failed `write_body` means the upstream request stream is GONE, as
/// opposed to still being there and merely not cooperating.
///
/// `write_body` fails in exactly three shapes, and only two of them are the
/// peer telling us something:
fn upstream_write_failed_because_stream_gone(e: &Error) -> bool {
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
fn upstream_write_stalled_after_response(e: &Error) -> bool {
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
const ABANDONED_BODY_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

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
fn bound_undrained_downstream_body(session: &mut Session) {
    if session.as_mut().is_body_done() || session.as_mut().get_total_drain_timeout().is_some() {
        return;
    }
    debug!(
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
const UPSTREAM_STALL_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// How one request-body write ended.
enum UpstreamBodyWriteEnd {
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
async fn write_upstream_body_watching_stall(
    client_body: &mut h2::SendStream<Bytes>,
    data: Bytes,
    end: bool,
    body_write: &UpstreamBodyWrite,
    // `Idle` borrows the downstream session mutably and is `Unpin`, so the loop
    // re-polls it by reference rather than moving it into a branch.
    mut stream_close: Option<Idle<'_>>,
) -> UpstreamBodyWriteEnd {
    let write = write_body(client_body, data, end, body_write.timeout);
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

// add scheme and authority as required by h2 lib
fn update_h2_scheme_authority(
    header: &mut http::request::Parts,
    raw_host: &[u8],
    tls: bool,
) -> Result<()> {
    let authority = if let Ok(s) = std::str::from_utf8(raw_host) {
        if s.starts_with('[') {
            // don't mess with ipv6 host
            s
        } else if let Some(colon) = s.find(':') {
            if s.len() == colon + 1 {
                // colon is the last char, ignore
                s
            } else if let Some(another_colon) = s[colon + 1..].find(':') {
                // try to get rid of extra port numbers
                &s[..colon + 1 + another_colon]
            } else {
                s
            }
        } else {
            s
        }
    } else {
        return Error::e_explain(
            InvalidHTTPHeader,
            format!("invalid authority from host {:?}", raw_host),
        );
    };

    let scheme = if tls { "https" } else { "http" };
    let uri = http::uri::Builder::new()
        .scheme(scheme)
        .authority(authority)
        .path_and_query(header.uri.path_and_query().as_ref().unwrap().as_str())
        .build();
    match uri {
        Ok(uri) => {
            header.uri = uri;
            Ok(())
        }
        Err(_) => Error::e_explain(
            InvalidHTTPHeader,
            format!("invalid authority from host {}", authority),
        ),
    }
}

impl<SV, C> HttpProxy<SV, C>
where
    C: custom::Connector,
{
    pub(crate) async fn proxy_down_to_up(
        &self,
        session: &mut Session,
        client_session: &mut Http2Session,
        peer: &HttpPeer,
        ctx: &mut SV::CTX,
    ) -> (bool, Option<Box<Error>>)
    // (reuse_server, error)
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let mut req = session.req_header().clone();

        if req.version != Version::HTTP_2 || session.downstream_session.is_custom() {
            if let Err(e) =
                sanitize_h2_upstream_request(&mut req, peer.options.http_upstream_request_policy)
            {
                return (false, Some(e.into_down()));
            }
            /* remove H1 specific headers */
            // https://github.com/hyperium/h2/blob/d3b9f1e36aadc1a7a6804e2f8e86d3fe4a244b4f/src/proto/streams/send.rs#L72
            req.remove_header(&http::header::TRANSFER_ENCODING);
            req.remove_header(&http::header::CONNECTION);
            req.remove_header(&http::header::UPGRADE);
            req.remove_header(KEEP_ALIVE);
            req.remove_header(PROXY_CONNECTION);
        }

        /* turn it into h2 */
        req.set_version(Version::HTTP_2);

        if session.cache.enabled() {
            pingora_cache::filters::upstream::request_filter(
                &mut req,
                session.cache.maybe_cache_meta(),
            );
            session.mark_upstream_headers_mutated_for_cache();
        }

        match self
            .inner
            .upstream_request_filter(session, &mut req, ctx)
            .await
        {
            Ok(_) => { /* continue */ }
            Err(e) => {
                return (false, Some(e));
            }
        }

        // The disposition is resolved AFTER `begin_request_body_replay()`,
        // because a registered replay buffer changes the "does this request
        // have a body" fact the coercion below depends on.
        if let Err(e) = session.as_mut().begin_request_body_replay().await {
            return (false, Some(e));
        }

        // TWO different "empty body" facts are needed here, and conflating them
        // is what an earlier revision of this file got wrong.
        //
        // `DispositionFacts::body_empty` (`is_body_empty() && is_body_done()`)
        // is "this request has NO body at all", a fact the client cannot
        // retract. That is the one the anti-smuggling coercion in
        // `safe_disposition` must key on -- collected (via
        // `safe_upstream_disposition`, below) only when `disposition` is
        // non-`Ordinary`, since `Ordinary` is that coercion's fixed point.
        //
        // The DECLARATION (`is_body_empty()` alone) is the other one, and which
        // of the two the upstream FRAMING is built from depends on the
        // disposition -- see `upstream_framing_body_empty`, which is the only
        // place that choice is made.
        //
        // The downstream READ side always keeps the strict transport fact (see
        // `DownstreamStateMachine::new` and the bodyless prelude in
        // `bidirection_down_to_up`), so the client's real end of stream is still
        // read and still produces exactly one application terminal event.
        let disposition = self.inner.upstream_request_body_disposition(session, ctx);
        let body_empty_declared = session.as_mut().is_body_empty();
        // The H2 pump always sends HTTP/2 upstream, so there is no below-1.1
        // case here (unlike the H1 pump).
        let body_disposition = safe_upstream_disposition(disposition, session, &req, false);
        let body_empty = upstream_framing_body_empty(body_disposition, body_empty_declared);
        apply_upstream_body_disposition(&mut req, body_disposition);

        // Remove H1 `Host` header, save it in order to add to :authority
        // We do this because certain H2 servers expect request not to have a host header.
        // The `Host` is removed after the upstream filters above for 2 reasons
        // 1. there is no API to change the :authority header
        // 2. the filter code needs to be aware of the host vs :authority across http versions otherwise
        let host = req.remove_header(&http::header::HOST);

        session.upstream_compression.request_filter(&req);

        // whether we support sending END_STREAM on HEADERS if body is empty
        let send_end_stream = req.send_end_stream().expect("req must be h2");

        let mut req: http::request::Parts = req.into();

        // H2 requires authority to be set, so copy that from H1 host if that is set
        if let Some(host) = host {
            if let Err(e) = update_h2_scheme_authority(&mut req, host.as_bytes(), peer.is_tls()) {
                return (false, Some(e));
            }
        }

        debug!("Request to h2: {req:?}");

        // send END_STREAM on HEADERS
        let send_header_eos =
            upstream_headers_end_stream(body_disposition, send_end_stream, body_empty);
        debug!("send END_STREAM on HEADERS: {send_header_eos}");

        let req = Box::new(RequestHeader::from(req));
        if let Err(e) = client_session.write_request_header(req, send_header_eos) {
            return (false, Some(e.into_up()));
        }

        let send_empty_data_eos = !send_header_eos
            && upstream_empty_data_end_stream(body_disposition, send_end_stream, body_empty);
        if send_empty_data_eos {
            // send END_STREAM on empty DATA frame
            match client_session.write_request_body(Bytes::new(), true).await {
                Ok(()) => debug!("sent empty DATA frame to h2"),
                Err(e) => {
                    return (false, Some(e.into_up()));
                }
            }
        }

        // The upstream request stream is already closed: every EOS decision
        // that fires here is final, so the pump below must never write another
        // byte of request body (h2 answers a DATA frame on a locally
        // half-closed stream with `UnexpectedFrameType`). Three shapes reach
        // this state with downstream body events still to come:
        // - `Bodyless` with a real downstream body -- the application declared
        //   there is no upstream body, so the body events still have to reach
        //   the application hooks while nothing goes on the wire. That is what
        //   the H1 pump gets for free from its zero-length body writer.
        // - a request with no body at all, whose EOS rode on the HEADERS frame
        //   while the prelude below still owes the application its single
        //   `Complete` event.
        // - a request that DECLARED an empty body (`Content-Length: 0`) whose
        //   downstream stream has not ended yet: the declaration was forwarded
        //   upstream, and the client's real end-of-stream still has to be read
        //   downstream. Suppressing the write is what keeps that from becoming
        //   a second, standalone END_STREAM.
        let upstream_body_closed = send_header_eos || send_empty_data_eos;

        client_session.read_timeout = peer.options.read_timeout;

        let mut downstream_custom_message_writer = session
            .downstream_session
            .as_custom_mut()
            .and_then(|c| c.take_custom_message_writer());
        // Keep the reader in this caller so it is restored even if retryable
        // upstream errors make try_join! cancel the downstream future.
        let mut downstream_custom_message_reader = match session
            .take_downstream_custom_message_reader(&mut downstream_custom_message_writer)
        {
            Ok(reader) => reader,
            Err(e) => return (false, Some(e)),
        };

        // take the body writer out of the client for easy duplex
        let mut client_body = client_session
            .take_request_body_writer()
            .expect("already send request header");

        // need to get the write_timeout here since we pass the h2 SendStream
        // directly to bidirection_down_to_up
        let write_timeout = peer.options.write_timeout;

        let (tx, rx) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);

        if self.inner.request_retry_allowed(session, ctx) {
            session.as_mut().enable_retry_buffering();
        }

        // Shared signal so the upstream half can distinguish an expected task-pipe
        // closure (the downstream half finished and dropped rx) from an unexpected one.
        let pipe_state = Arc::new(AtomicU8::new(PipeState::Active as u8));

        /* read downstream body and upstream response at the same time */

        let ret = {
            let downstream = self.bidirection_down_to_up(
                session,
                &mut client_body,
                rx,
                ctx,
                &mut downstream_custom_message_writer,
                &mut downstream_custom_message_reader,
                pipe_state.clone(),
                UpstreamBodyWrite {
                    timeout: write_timeout,
                    stream_closed: upstream_body_closed,
                    eos_write_optional: false,
                    disposition: body_disposition,
                    upstream_response_ended: client_session.peer_end_stream(),
                },
            );
            let upstream = pipe_up_to_down_response(client_session, tx, pipe_state);
            tokio::pin!(downstream);
            tokio::pin!(upstream);

            tokio::select! {
                // Deterministic preference for the typed terminate outcome: when a
                // downstream `Ok(Terminate)` (the application already wrote the
                // response) and an upstream `Err` become ready in the same poll,
                // random branch order would non-deterministically pick the generic
                // error path instead. Non-terminate orderings are unchanged because
                // the `Complete` arm still awaits the sibling.
                biased;

                downstream_result = &mut downstream => {
                    match downstream_result {
                        Ok(DownstreamRequestOutcome::Terminate) => {
                            // Dropping the sibling future immediately stops both upstream
                            // response reads and request-body writes.
                            None
                        }
                        Ok(outcome @ (DownstreamRequestOutcome::Complete(_)
                            | DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(_))) => {
                            Some(upstream.await.map(|upstream| (outcome, upstream)))
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                upstream_result = &mut upstream => {
                    Some(match upstream_result {
                        Ok(upstream) => downstream.await.map(|downstream| (downstream, upstream)),
                        Err(e) => Err(e),
                    })
                }
            }
        };

        if let Some(custom_session) = session.downstream_session.as_custom_mut() {
            if let Some(downstream_custom_message_writer) = downstream_custom_message_writer {
                match custom_session.restore_custom_message_writer(downstream_custom_message_writer)
                {
                    Ok(_) => { /* continue */ }
                    Err(e) => {
                        return (false, Some(e));
                    }
                }
            }
            if let Some(downstream_custom_message_reader) = downstream_custom_message_reader {
                match custom_session.restore_custom_message_reader(downstream_custom_message_reader)
                {
                    Ok(_) => { /* continue */ }
                    Err(e) => {
                        return (false, Some(e));
                    }
                }
            }
        }

        match ret {
            None => {
                // The sibling upstream future was dropped mid-flight, so the request
                // stream is still open: reset it to stop the upstream from working on
                // a request nobody will read.
                // A locally reset stream may no longer be judged by the wire
                // END_STREAM record: `h2` starts DROPPING the DATA it decodes,
                // while a peer RST_STREAM landing afterwards can still surface
                // as a remote NO_ERROR. Nothing reads this stream after this
                // point, so this is enforcement of an invariant rather than a
                // fix -- see `Http2Session::note_local_reset`, which also
                // explains why it has to run BEFORE the reset is queued.
                client_session.note_local_reset();
                client_body.send_reset(h2::Reason::CANCEL);
                release_cache_on_terminate(session);
                // Downstream hygiene is keyed by the DOWNSTREAM protocol, not by the
                // upstream one this pump was selected for: an H1 client proxied to an
                // H2 upstream lands here too, and its connection holds request bytes
                // the application refused to read. Reporting non-reuse is a no-op for
                // an H2 downstream (only this stream ended; the connection lives on,
                // see `h2c_downstream_terminate_keeps_connection`) and is what keeps
                // an H1 downstream from being drained-and-reused.
                (false, None)
            }
            Some(Ok((DownstreamRequestOutcome::Terminate, _))) => {
                // The upstream half completed cleanly here, so the stream already saw
                // END_STREAM. h2 would swallow a reset of a closed stream, but some
                // servers still count an RST_STREAM on the wire toward their
                // post-CVE-2023-44487 abuse heuristics; there is nothing to cancel, so
                // do not send one. Downstream hygiene applies exactly as above.
                release_cache_on_terminate(session);
                (false, None)
            }
            Some(Ok((DownstreamRequestOutcome::Complete(downstream_can_reuse), _))) => {
                (downstream_can_reuse, None)
            }
            Some(Ok((
                DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(downstream_can_reuse),
                _,
            ))) => {
                // Invalidate before resetting; see `note_local_reset`.
                client_session.note_local_reset();
                client_body.send_reset(h2::Reason::CANCEL);
                (downstream_can_reuse, None)
            }
            Some(Err(e)) => {
                let upstream_read_timeout =
                    e.esource == ErrorSource::Upstream && matches!(e.etype, ReadTimedout);
                let downstream_error = e.esource == ErrorSource::Downstream;
                // On application level upstream read timeouts, send RST_STREAM CANCEL,
                // we know we have not received END_STREAM at this point since we read timed out.
                // Also cancel the upstream stream when downstream goes away/resets so the
                // upstream peer can release the stream promptly.
                // TODO: implement for write timeouts?
                //
                // Whether or not the explicit reset below is sent, this arm
                // abandons the upstream request stream: `client_body` is
                // dropped on return and `h2` cancels a still-open stream when
                // its last handle goes away. Source (iv) is given up either
                // way, so record it unconditionally -- and ahead of the reset,
                // because a record published in between could no longer be
                // retracted. See `Http2Session::note_local_reset`.
                client_session.note_local_reset();
                if upstream_read_timeout || downstream_error {
                    client_body.send_reset(h2::Reason::CANCEL);
                    if upstream_read_timeout {
                        // Mark the underlying H2 connection for shutdown so it's not used
                        // for new streams in case it is hung.
                        client_session.conn.mark_shutdown();
                    }
                }
                (false, Some(e))
            }
        }
    }

    pub(crate) async fn proxy_to_h2_upstream(
        &self,
        session: &mut Session,
        client_session: &mut Http2Session,
        reused: bool,
        peer: &HttpPeer,
        ctx: &mut SV::CTX,
    ) -> (bool, Option<Box<Error>>)
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        #[cfg(windows)]
        let raw = client_session.fd() as std::os::windows::io::RawSocket;
        #[cfg(unix)]
        let raw = client_session.fd();

        if let Err(e) = self
            .inner
            .connected_to_upstream(session, reused, peer, raw, client_session.digest(), ctx)
            .await
        {
            return (false, Some(e));
        }

        let (server_session_reuse, error) = self
            .proxy_down_to_up(session, client_session, peer, ctx)
            .await;

        // Record upstream response body bytes received (HTTP/2 DATA payload).
        let upstream_bytes_total = client_session.body_bytes_received();
        session.set_upstream_body_bytes_received(upstream_bytes_total);

        // Note: upstream_write_pending_time is not tracked for HTTP/2 (multiplexed streams).

        (server_session_reuse, error)
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_upstream_tasks_h2(
        &self,
        session: &mut Session,
        ctx: &mut SV::CTX,
        initial_task: HttpTask,
        rx: &mut mpsc::Receiver<HttpTask>,
        serve_from_cache: &mut ServeFromCache,
        range_body_filter: &mut proxy_cache::range_filter::RangeBodyFilter,
        response_state: &mut ResponseStateMachine,
        suppress_downstream_body: &mut bool,
        filtered_terminal_header: &mut Option<Box<ResponseHeader>>,
        upstream_reusable: &mut bool,
        sink: &mut ResponseBodySink,
        terminal_body: &mut TerminalBodyDispatch,
    ) -> Result<Option<(bool, bool)>>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        if serve_from_cache.should_discard_upstream() {
            // Serving the cached response and discarding the upstream one; nothing
            // is written downstream this round, so return None and let the caller
            // continue.
            return Ok(None);
        }

        // Batch: pull as many tasks as we can from rx
        let mut tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
        tasks.push(initial_task);
        // tokio::task::unconstrained because now_or_never may yield None when the future is ready
        while let Some(maybe_task) = tokio::task::unconstrained(rx.recv()).now_or_never() {
            if let Some(t) = maybe_task {
                tasks.push(t);
            } else {
                break; // upstream closed
            }
        }
        let source_done = tasks.iter().any(HttpTask::is_end);

        /* run filters before sending to downstream */
        let mut filtered_tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
        sink.reset_batch();
        for mut t in tasks {
            if self.revalidate_or_stale(session, &mut t, ctx).await {
                serve_from_cache.enable();
                response_state.enable_cached_response();
                // skip downstream filtering entirely as the 304 will not be sent
                break;
            }
            #[cfg(feature = "upstream_modules")]
            if let HttpTask::Header(header, end_of_stream) = &t {
                self.inner
                    .adjust_upstream_modules(session, header, *end_of_stream, ctx)
                    .await?;
            }
            #[cfg(feature = "upstream_modules")]
            session.upstream_modules_filter_task(&mut t).await?;
            session.upstream_compression.response_filter(&mut t);
            self.h2_response_filter(
                session,
                t,
                ctx,
                serve_from_cache,
                range_body_filter,
                false,
                suppress_downstream_body,
                filtered_terminal_header,
                upstream_reusable,
                sink,
                terminal_body,
                &mut filtered_tasks,
            )
            .await?;
            if serve_from_cache.is_miss_header() {
                response_state.enable_cached_response();
            }
            if sink.is_terminated() {
                break;
            }
        }

        if serve_from_cache.is_on() && sink.is_terminated() {
            return Error::e_explain(
                InternalError,
                "response-body terminate is not supported while serving from a streaming cache readback",
            );
        }

        if !serve_from_cache.should_send_to_downstream() {
            // TODO: need to derive response_done from filtered_tasks in case downstream failed already
            return Ok(None);
        }

        session.write_response_tasks(filtered_tasks).await?;

        Ok(Some((source_done, sink.is_terminated() && !source_done)))
    }

    // returns whether server (downstream) session can be reused
    #[allow(clippy::too_many_arguments)]
    async fn bidirection_down_to_up(
        &self,
        session: &mut Session,
        client_body: &mut h2::SendStream<bytes::Bytes>,
        mut rx: mpsc::Receiver<HttpTask>,
        ctx: &mut SV::CTX,
        downstream_custom_message_writer: &mut Option<Box<dyn CustomMessageWrite>>,
        downstream_custom_message_reader: &mut Option<
            Box<dyn futures::Stream<Item = Result<Bytes>> + Unpin + Send + Sync + 'static>,
        >,
        pipe_state: Arc<AtomicU8>,
        body_write: UpstreamBodyWrite,
    ) -> Result<DownstreamRequestOutcome>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        // setup custom message forwarding, if downstream supports it
        let (
            mut downstream_custom_read,
            mut downstream_custom_write,
            downstream_custom_message_custom_forwarding,
            mut downstream_custom_message_inject_rx,
        ) = if downstream_custom_message_writer.is_some() {
            let (inject_tx, inject_rx) = mpsc::channel::<Bytes>(CUSTOM_MESSAGE_QUEUE_SIZE);
            (true, true, Some(inject_tx), Some(inject_rx))
        } else {
            (false, false, None, None)
        };

        if let Some(custom_forwarding) = downstream_custom_message_custom_forwarding {
            // Custom handles are owned by the caller so an early error here still
            // lets the caller restore them before retrying another upstream.
            self.inner
                .custom_forwarding(session, ctx, None, custom_forwarding)
                .await?;
        }

        let mut downstream_state = DownstreamStateMachine::new(session.as_mut().is_body_done());
        // Set once the upstream has stopped receiving the request body after
        // flagging its response complete (RFC 9113 §8.1). It takes the read side
        // out of the loop for good: the state machine says "finished reading",
        // which is what keeps the downstream connection out of the errored path,
        // and this flag is what stops the loop from polling a read side it has
        // just declared done -- `read_body_or_idle` answers such a poll with
        // "Sent data after end of body" as soon as the client sends its next
        // byte, and the loop would turn that into a downstream error, failing
        // the very exchange this is saving.
        //
        // The guard is CONDITIONAL: it only bites while the loop is still
        // running for the RESPONSE's sake, i.e. while `response_state` is not
        // yet done. That window is narrow and easy to miss -- most shapes of
        // this exchange have the response fully written downstream by the time
        // the request-body write fails, and then the loop exits at once and
        // this flag is dead code. Do not take the file's other tests as
        // coverage; the one that enters the window on purpose is
        // `h2_upstream_no_error_reset_keeps_streaming_while_the_client_uploads`,
        // and it is the only one that fails when this guard is deleted.
        //
        // Not folded into `DownstreamStateMachine`: that type is shared with the
        // H1 pump, whose `Errored`/`ReadingFinished` distinction means other
        // things there.
        let mut upstream_stopped_receiving = false;

        let buffer = session.as_mut().get_retry_buffer();
        // Native retry-buffer path. Registered app buffers are replayed through
        // `read_body_or_idle()` below, one bounded chunk at a time.
        //
        // The bodyless prelude is identical to the H1 pump's: it fires one
        // immediate `(None, end)` body event so that a request with no body
        // reaches `request_body_filter_action` / `request_trailer_filter`
        // exactly once, whichever upstream protocol was selected (design 4.4).
        // It must require the transport fact (`is_body_done()`) and not just
        // `is_body_empty()`, which still infers emptiness from
        // `Content-Length: 0`: an H2 downstream request declaring
        // `Content-Length: 0` without END_STREAM is not bodyless (design 4.3),
        // so the loop below reads on to the real EOS and would deliver a
        // SECOND terminal event. Requiring both facts delivers exactly
        // one. The upstream EOS for exactly this shape already rode on the
        // HEADERS frame (or on the empty DATA frame), which is why
        // `body_write.stream_closed` suppresses the write side here.
        if buffer.is_some() || (session.as_mut().is_body_empty() && session.as_mut().is_body_done())
        {
            let outcome = self
                .send_body_to2(
                    session,
                    buffer,
                    RequestBodyEvent::from(downstream_state.is_done()),
                    client_body,
                    ctx,
                    &body_write,
                )
                .await?;
            match outcome {
                UpstreamBodyOutcome::Downstream(DownstreamRequestOutcome::Terminate) => {
                    // No-op for an H2 downstream; required for an H1 downstream proxied
                    // to an H2 upstream, whose unread request bytes must not be drained
                    // and the connection reused.
                    session.set_keepalive(None);
                    finish_terminated_response(session).await;
                    restore_custom_message_reader(session, downstream_custom_message_reader.take());
                    return Ok(DownstreamRequestOutcome::Terminate);
                }
                // The replayed body could not be written because the upstream
                // had already answered in full and reset the stream. Failing the
                // request here would discard that response; the duplex loop
                // below still has to run to deliver it.
                UpstreamBodyOutcome::UpstreamDoneReceiving {
                    terminal_event_delivered,
                } => {
                    if !terminal_event_delivered
                        && self
                            .finish_downstream_body_side(session, client_body, ctx, &body_write)
                            .await?
                    {
                        session.set_keepalive(None);
                        finish_terminated_response(session).await;
                        restore_custom_message_reader(
                            session,
                            downstream_custom_message_reader.take(),
                        );
                        return Ok(DownstreamRequestOutcome::Terminate);
                    }
                    upstream_stopped_receiving = true;
                    downstream_state.maybe_finished(true);
                    bound_undrained_downstream_body(session);
                }
                UpstreamBodyOutcome::Downstream(
                    DownstreamRequestOutcome::Complete(_)
                    | DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(_),
                ) => {}
            }
        }

        let mut response_state = ResponseStateMachine::new();

        // these two below can be wrapped into an internal ctx
        // use cache when upstream revalidates (or TODO: error)
        let mut serve_from_cache = ServeFromCache::new();
        let mut range_body_filter = proxy_cache::range_filter::RangeBodyFilter::new();
        // Shared across every batch drained from upstream for this response;
        // the per-batch byte budget is reset at each batch boundary (see
        // `ResponseBodySink::reset_batch`), but a `terminate()` signal stays
        // sticky for the rest of this response.
        let mut sink = ResponseBodySink::new();
        // Also shared across every batch: `Trailer` and the `Done` behind it
        // can land in different batches, so the latch that keeps the terminal
        // body callback to exactly one delivery must outlive a single batch.
        let mut terminal_body = TerminalBodyDispatch::default();
        let mut suppress_downstream_body = false;
        let mut filtered_terminal_header = None;
        let mut upstream_reusable = true;

        let mut next_upstream_task: Option<HttpTask> = None;

        /* duplex mode
         * see the Same function for h1 for more comments
         */
        while !downstream_state.is_done()
            || !response_state.is_done()
            || downstream_custom_read && !downstream_state.is_errored()
            || downstream_custom_write
        {
            if downstream_body_read_is_futile(session, &downstream_state, &response_state) {
                // Abandoning the read must not cost the application its single
                // terminal event (invariant B): run the hooks with one
                // `Abandoned` event exactly once.
                //
                // `body_write` is passed through UNCHANGED on purpose. Forcing
                // `stream_closed: true` here would skip the terminating
                // END_STREAM in exactly the case where the upstream request
                // stream is genuinely still open (`stream_closed` is false
                // precisely because the pump still owes that frame), and
                // dropping `client_body` afterwards would make h2 emit a
                // gratuitous RST_STREAM(CANCEL) per request instead -- the
                // opposite of the abuse-counter hygiene documented at the
                // terminate arms above. When the stream really is already
                // closed the existing suppression still applies. The write may
                // fail because the upstream already sent RFC 9113 §8.1's
                // RST_STREAM(NO_ERROR); that costs nothing here (the response is
                // complete) and is ignored, see `eos_write_optional`.
                let outcome = self
                    .send_body_to2(
                        session,
                        None,
                        RequestBodyEvent::Abandoned,
                        client_body,
                        ctx,
                        &UpstreamBodyWrite {
                            eos_write_optional: true,
                            ..body_write.clone()
                        },
                    )
                    .await?;
                if outcome == UpstreamBodyOutcome::Downstream(DownstreamRequestOutcome::Terminate) {
                    session.set_keepalive(None);
                    finish_terminated_response(session).await;
                    restore_custom_message_reader(session, downstream_custom_message_reader.take());
                    return Ok(DownstreamRequestOutcome::Terminate);
                }
                // `UpstreamDoneReceiving` needs nothing extra here. The
                // terminal event this branch exists to deliver has just been
                // delivered (`Abandoned` above), so the only thing the arms in
                // the loop below add -- `finish_downstream_body_side` -- would
                // be a second delivery. The read side is already being
                // finished, and `eos_write_optional` has already swallowed the
                // same write failure on the normal path.
                downstream_state.maybe_finished(true);
                continue;
            }

            // Use optional futures to allow using optional channels in select branches
            let custom_inject_rx_recv: OptionFuture<_> = downstream_custom_message_inject_rx
                .as_mut()
                .map(|rx| rx.recv())
                .into();
            let custom_reader_next: OptionFuture<_> = downstream_custom_message_reader
                .as_mut()
                .map(|reader| reader.next())
                .into();

            // partial read support, this check will also be false if cache is disabled.
            let support_cache_partial_read =
                session.cache.support_streaming_partial_write() == Some(true);
            let upgraded = session.was_upgraded();

            // Similar logic in h1 need to reserve capacity first to avoid deadlock
            // But we don't need to do the same because the h2 client_body pipe is unbounded (never block)
            tokio::select! {
                // NOTE: cannot avoid this copy since h2 owns the buf
                body = session.downstream_session.read_body_or_idle(downstream_state.is_done()), if downstream_state.can_poll() && !upstream_stopped_receiving => {
                    debug!("downstream event");
                    let body = match body {
                        Ok(b) => b,
                        Err(e) => {
                            if session.downstream_session.request_body_buffer_replaying() {
                                // The error came from the registered request body buffer
                                // (replay path), not the client stream: a gateway-local
                                // failure that must not be booked as a client abort nor
                                // swallowed as an ignorable downstream error during caching.
                                return Err(e.into_in());
                            }
                            let wait_for_cache_fill = (!serve_from_cache.is_on() && support_cache_partial_read)
                                || serve_from_cache.is_miss();
                            if wait_for_cache_fill {
                                // ignore downstream error so that upstream can continue to write cache
                                downstream_state.to_errored();
                                if !self.inner.suppress_proxy_warn_log(
                                    session,
                                    ctx,
                                    &e,
                                    ProxyWarnLogContext::DownstreamCache,
                                ) {
                                    warn!(
                                        "Downstream Error ignored during caching: {}, {}",
                                        e,
                                        self.inner.request_summary(session, ctx)
                                    );
                                }
                                // This will not be treated as a final error, but we should signal to
                                // downstream session regardless
                                session.downstream_session.on_proxy_failure(e);
                                continue;
                           } else {
                                return Err(e.into_down());
                           }
                        }
                    };
                    let is_body_done = session.is_body_done();
                    match self
                        .send_body_to2(
                            session,
                            body,
                            RequestBodyEvent::from(is_body_done),
                            client_body,
                            ctx,
                            &body_write,
                        )
                        .await
                    {
                        Ok(UpstreamBodyOutcome::Downstream(
                            DownstreamRequestOutcome::Complete(request_done)
                            | DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(request_done),
                        )) =>  {
                            downstream_state.maybe_finished(request_done);
                        },
                        Err(e) if e.esource == ErrorSource::Downstream => {
                            // Downstream reset/errored while the upstream write was blocked
                            // (e.g. on upstream flow control). Same policy as the read error
                            // handling above: ignore the downstream error if the upstream
                            // response is being admitted to cache, otherwise fail so the
                            // downstream stream handles are dropped promptly.
                            let wait_for_cache_fill = (!serve_from_cache.is_on() && support_cache_partial_read)
                                || serve_from_cache.is_miss();
                            if !wait_for_cache_fill {
                                return Err(e);
                            }
                            // ignore downstream error so that upstream can continue to write cache
                            downstream_state.to_errored();
                            if !self.inner.suppress_proxy_warn_log(
                                session,
                                ctx,
                                &e,
                                ProxyWarnLogContext::DownstreamCache,
                            ) {
                                warn!(
                                    "Downstream Error ignored during caching: {}, {}",
                                    e,
                                    self.inner.request_summary(session, ctx)
                                );
                            }
                            // This will not be treated as a final error, but we should signal to
                            // downstream session anyway.
                            session.downstream_session.on_proxy_failure(e);
                        },
                        // The upstream answered in full and reset the stream while
                        // this side was still uploading (RFC 9113 §8.1). The
                        // exchange is NOT failed over it: the response is already
                        // in hand, and whether it is complete was decided by the
                        // read half. All that is left is to stop feeding a write
                        // half that is gone -- and, first, to pay the application
                        // the terminal event that taking the read side out of
                        // the loop would otherwise cost it.
                        Ok(UpstreamBodyOutcome::UpstreamDoneReceiving { terminal_event_delivered }) => {
                            if !terminal_event_delivered
                                && self.finish_downstream_body_side(session, client_body, ctx, &body_write).await?
                            {
                                session.set_keepalive(None);
                                finish_terminated_response(session).await;
                                restore_custom_message_reader(session, downstream_custom_message_reader.take());
                                return Ok(DownstreamRequestOutcome::Terminate);
                            }
                            upstream_stopped_receiving = true;
                            downstream_state.maybe_finished(true);
                            bound_undrained_downstream_body(session);
                        },
                        Ok(UpstreamBodyOutcome::Downstream(DownstreamRequestOutcome::Terminate)) => {
                            // See the prelude terminate above: hygiene follows the
                            // downstream protocol, which may be H1 here.
                            session.set_keepalive(None);
                            finish_terminated_response(session).await;
                            restore_custom_message_reader(session, downstream_custom_message_reader.take());
                            return Ok(DownstreamRequestOutcome::Terminate);
                        },
                        Err(e) => {
                            // Under `Bodyless` the upstream request stream is already
                            // closed before this loop starts, so nothing in
                            // `send_body_to2` can write to it: an error here is the
                            // application's -- the `Bodyless` contract violation, or one
                            // of its own body filters -- never an upstream write failure.
                            // Absorbing it as one would let the request finish 200 with
                            // the client's body silently dropped, which is exactly what
                            // failing closed exists to prevent.
                            if body_write.disposition == UpstreamRequestBodyDisposition::Bodyless {
                                return Err(e);
                            }
                            // mark request done, attempt to drain receive
                            warn!("Upstream h2 body send error: {e}");
                            // upstream is what actually errored but we don't want to continue
                            // polling the downstream body
                            downstream_state.to_errored();
                        }
                    };
                },

                // Handle buffered upstream task from previous iteration
                task = async { next_upstream_task.take() }, if next_upstream_task.is_some() => {
                    debug!("buffered upstream event: {:?}", task);
                    if let Some(t) = task {
                        let Some((response_done, terminated)) = self.process_upstream_tasks_h2(
                            session,
                            ctx,
                            t,
                            &mut rx,
                            &mut serve_from_cache,
                            &mut range_body_filter,
                            &mut response_state,
                            &mut suppress_downstream_body,
                            &mut filtered_terminal_header,
                            &mut upstream_reusable,
                            &mut sink,
                            &mut terminal_body,
                        ).await? else {
                            // nothing sent downstream e.g. serve_from_cache
                            continue;
                        };
                        if terminated {
                            session.set_keepalive(None);
                            warn_response_body_terminate_without_response(session, "upstream_response_body_filter");
                            warn_response_body_terminate_content_length_leak(session, "upstream_response_body_filter");
                            finish_terminated_response(session).await;
                            restore_custom_message_reader(session, downstream_custom_message_reader.take());
                            return Ok(DownstreamRequestOutcome::Terminate);
                        }
                        if session.was_upgraded() {
                            return Error::e_explain(H2Error, "upgraded while proxying to h2 session");
                        }
                        response_state.maybe_set_upstream_done(response_done);
                    } else {
                        debug!("empty upstream event");
                        response_state.maybe_set_upstream_done(true);
                    }
                },

                task = rx.recv(), if !response_state.upstream_done() && next_upstream_task.is_none() => {
                    debug!("upstream event: {:?}", task);
                    if let Some(t) = task {
                        let Some((response_done, terminated)) = self.process_upstream_tasks_h2(
                            session,
                            ctx,
                            t,
                            &mut rx,
                            &mut serve_from_cache,
                            &mut range_body_filter,
                            &mut response_state,
                            &mut suppress_downstream_body,
                            &mut filtered_terminal_header,
                            &mut upstream_reusable,
                            &mut sink,
                            &mut terminal_body,
                        ).await? else {
                            // nothing sent downstream e.g. serve_from_cache
                            continue;
                        };
                        if terminated {
                            session.set_keepalive(None);
                            warn_response_body_terminate_without_response(session, "upstream_response_body_filter");
                            warn_response_body_terminate_content_length_leak(session, "upstream_response_body_filter");
                            finish_terminated_response(session).await;
                            restore_custom_message_reader(session, downstream_custom_message_reader.take());
                            return Ok(DownstreamRequestOutcome::Terminate);
                        }
                        if session.was_upgraded() {
                            // it is very weird if the downstream session decides to upgrade
                            // since the client h2 session cannot, return an error on this case
                            return Error::e_explain(H2Error, "upgraded while proxying to h2 session");
                        }
                        response_state.maybe_set_upstream_done(response_done);
                    } else {
                        debug!("empty upstream event");
                        response_state.maybe_set_upstream_done(true);
                    }
                },

                task = serve_from_cache.next_http_task(&mut session.cache, &mut range_body_filter, upgraded),
                    if !response_state.cached_done()
                        && !downstream_state.is_errored()
                        && serve_from_cache.is_on()
                        && !session.has_pending_downstream_tasks() => { // backpressure: don't queue if pending writes

                    let task = task?;
                    let cache_source_done = task.is_end();
                    let mut cached_tasks = Vec::with_capacity(1);
                    self.h2_response_filter(session, task, ctx,
                        &mut serve_from_cache,
                        &mut range_body_filter, true,
                        &mut suppress_downstream_body,
                        &mut filtered_terminal_header,
                        &mut upstream_reusable,
                        &mut sink, &mut terminal_body, &mut cached_tasks).await?;
                    debug!("serve_from_cache task {cached_tasks:?}");

                    if session.downstream_session.supports_proxy_task_api() {
                        if cached_tasks.is_empty() {
                            response_state.maybe_set_cache_done(cache_source_done);
                        } else {
                            for task in cached_tasks {
                                session.send_downstream_proxy_task(task).await?;
                            }
                        }
                    } else {
                        match session.write_response_tasks(cached_tasks).await {
                            Ok(_) => response_state.maybe_set_cache_done(cache_source_done),
                            Err(e) => if serve_from_cache.is_miss() {
                                // give up writing to downstream but wait for upstream cache write to finish
                                downstream_state.to_errored();
                                response_state.maybe_set_cache_done(true);
                                if !self.inner.suppress_proxy_warn_log(
                                    session,
                                    ctx,
                                    &e,
                                    ProxyWarnLogContext::DownstreamCache,
                                ) {
                                    warn!(
                                        "Downstream Error ignored during caching: {}, {}",
                                        e,
                                        self.inner.request_summary(session, ctx)
                                    );
                                }
                                // This will not be treated as a final error, but we should signal to
                                // downstream session regardless
                                session.downstream_session.on_proxy_failure(e);
                                continue;
                            } else {
                                return Err(e);
                            }
                        }
                        // A storage error can disable cache between cached_done
                        // being set and here; see the same guard in proxy_h1.rs.
                        if response_state.cached_done() && session.cache.enabled() {
                            if let Err(e) = session.cache.finish_hit_handler().await {
                                warn!("Error during finish_hit_handler: {}", e);
                            }
                        }
                    }
                }

                // Write queued downstream proxy tasks while also polling for upstream tasks.
                // This allows cache writes to continue even when downstream is stalled.
                //
                // "Gate" branch: ready(()) resolves immediately, so the guard controls
                // whether we enter. This is not a busy-loop because every path through
                // the inner select either (a) drains all pending tasks via
                // write_downstream_proxy_tasks (making the guard false), (b) observes a
                // downstream write error (making downstream_state errored and the guard false),
                // (c) stores an upstream task in next_upstream_task (making the guard false), or
                // (d) blocks on real I/O inside the nested select.
                _ = std::future::ready(()),
                    if !downstream_state.is_errored()
                        && session.has_pending_downstream_tasks()
                        && next_upstream_task.is_none() => {
                    tokio::select! {
                        // Try to write downstream proxy tasks (cancel-safe)
                        write_result = session.write_downstream_proxy_tasks() => {
                            match write_result {
                                Ok(end) => {
                                    response_state.maybe_set_cache_done(end);
                                    // See disabled() guard comment above.
                                    // See enabled() guard comment above.
                                    if response_state.cached_done() && session.cache.enabled() {
                                        if let Err(e) = session.cache.finish_hit_handler().await {
                                            warn!("Error during finish_hit_handler: {}", e);
                                        }
                                    }
                                }
                                Err(e) => if serve_from_cache.is_miss() {
                                    // give up writing to downstream but wait for upstream cache write to finish
                                    downstream_state.to_errored();
                                    response_state.maybe_set_cache_done(true);
                                    if !self.inner.suppress_proxy_warn_log(
                                        session,
                                        ctx,
                                        &e,
                                        ProxyWarnLogContext::DownstreamCache,
                                    ) {
                                        warn!(
                                            "Downstream write error ignored during caching: {}, {}",
                                            e,
                                            self.inner.request_summary(session, ctx)
                                        );
                                    }
                                    session.downstream_session.on_proxy_failure(e);
                                } else {
                                    return Err(e);
                                }
                            }
                        }

                        // Also poll for upstream tasks - if we get one, cancel the write and handle it.
                        upstream_task = rx.recv(), if !response_state.upstream_done() && serve_from_cache.is_on() && next_upstream_task.is_none() => {
                            if let Some(t) = upstream_task {
                                next_upstream_task = Some(t);
                                continue;
                            } else {
                                response_state.maybe_set_upstream_done(true);
                            }
                        }
                    }
                }
                data = custom_reader_next, if downstream_custom_read && !downstream_state.is_errored()  => {
                    let Some(data) = data.flatten() else {

                        downstream_custom_read = false;
                        continue;
                    };

                    let data = match data {
                        Ok(data) => data,
                        Err(err) =>  {
                            warn!("downstream_custom_message_reader got error: {err}");
                            downstream_custom_read = false;
                            continue;
                        },
                    };

                    self.inner
                        .downstream_custom_message_proxy_filter(session, data, ctx, true) // true, because it's the last hop for downstream proxying
                        .await?;
                },

                data = custom_inject_rx_recv, if downstream_custom_write => {
                    match data.flatten() {
                        Some(data) => {
                            if let Some(ref mut custom_writer) = downstream_custom_message_writer {
                                custom_writer.write_custom_message(data).await?
                            }
                        },
                        None => {
                            downstream_custom_write = false;
                            if let Some(ref mut custom_writer) = downstream_custom_message_writer {
                                custom_writer.finish_custom().await?;
                            }
                        },
                    }
                },

                else => {
                    break;
                }
            }
        }

        restore_custom_message_reader(session, downstream_custom_message_reader.take());
        let mut reuse_downstream = !downstream_state.is_errored();
        if reuse_downstream {
            match session.as_mut().finish_body().await {
                Ok(_) => {
                    debug!("finished sending body to downstream");
                }
                Err(e) => {
                    error!("Error finish sending body to downstream: {}", e);
                    reuse_downstream = false;
                }
            }
        }
        // Signal the upstream half that the downstream half completed cleanly before
        // dropping rx, so a resulting task-pipe closure is treated as benign.
        pipe_state.store(PipeState::DownstreamComplete as u8, Ordering::Release);
        Ok(if upstream_reusable {
            DownstreamRequestOutcome::Complete(reuse_downstream)
        } else {
            DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(reuse_downstream)
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn h2_response_filter(
        &self,
        session: &mut Session,
        mut task: HttpTask,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
        range_body_filter: &mut RangeBodyFilter,
        from_cache: bool, // are the task from cache already
        suppress_downstream_body: &mut bool,
        filtered_terminal_header: &mut Option<Box<ResponseHeader>>,
        upstream_reusable: &mut bool,
        sink: &mut ResponseBodySink,
        terminal_body: &mut TerminalBodyDispatch,
        out_tasks: &mut Vec<HttpTask>,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let terminal_header = !from_cache
            && matches!(
                &task,
                HttpTask::Header(header, true) if !header.status.is_informational()
            );
        let filter_downstream_body = terminal_header
            || matches!(&task, HttpTask::Body(..))
            || (from_cache && matches!(&task, HttpTask::Done));
        let mut terminal_cacheability = None;
        // Whether this task must deliver the response's single terminal
        // `upstream_response_body_filter` callback. Only `Trailer`/`Done` ever
        // set it, and only once per response -- see `TerminalBodyDispatch`.
        let mut terminal_dispatch = false;

        if !from_cache {
            if let Some(duration) = self.upstream_filter(session, &mut task, sink, ctx).await? {
                trace!("delaying upstream response for {duration:?}");
                time::sleep(duration).await;
            }

            // `upstream_filter` reaches the body filter only from a
            // `Body`/`UpgradedBody` task, so a response terminating with a
            // trailer or a bare `Done` would never deliver end-of-stream. On H2
            // that is every trailered response, because `END_STREAM` rides the
            // trailers HEADERS frame and each DATA frame is emitted with
            // `eos = false`.
            terminal_dispatch = terminal_body.claim_for(&task);
            if terminal_dispatch {
                if let Some(duration) = self
                    .terminal_upstream_body_filter(session, sink, ctx)
                    .await?
                {
                    trace!("delaying terminal upstream response for {duration:?}");
                    time::sleep(duration).await;
                }
            }

            if terminal_header {
                let HttpTask::Header(header, _) = &task else {
                    unreachable!("terminal task must be a header")
                };
                terminal_cacheability =
                    self.response_cacheability_before_downstream_filter(session, header, ctx)?;
            }

            // Cache the original response (and anything the upstream body
            // filter queued in `sink` after it) before any downstream
            // transformation. Requests that bypassed cache still need to run
            // filters to see if the response has become cacheable.
            if !terminal_header {
                if terminal_dispatch {
                    // Released body bytes precede the terminating task on the
                    // wire, so the cached entity has to be admitted in that
                    // same order to stay byte-identical.
                    self.cache_task_and_emitted_chunks_before(
                        session,
                        &task,
                        sink,
                        terminal_body.is_upgraded(),
                        ctx,
                        serve_from_cache,
                    )
                    .await?;
                } else {
                    self.cache_task_and_emitted_chunks(session, &task, sink, ctx, serve_from_cache)
                        .await?;
                }
                self.track_predicted_uncacheable_response(session, &task, sink);
            }

            // skip the downstream filtering if these tasks are just for cache admission
            if !terminal_header && !serve_from_cache.should_send_to_downstream() {
                // The batch this task belongs to is discarded by the pump
                // below (`continue`, never `write_response_tasks`), so any
                // chunks this task's filter queued must be discarded here
                // too: left queued, they would either be mis-attributed to a
                // LATER task in the same batch (cached a second time, out of
                // place) once that task's own terminal drain runs, or leak
                // into the separate serve-from-cache arm, which reuses this
                // same `sink` for the rest of the response and must never
                // emit chunks it did not itself produce (see the `from_cache`
                // guard at the end of this function).
                sink.take_extra();
                if let HttpTask::Failed(error) = task {
                    abort_cache_after_response_source_failure(session, false);
                    return Err(error);
                }
                out_tasks.push(task);
                return Ok(());
            }
        } // else: cached/local response, no need to trigger upstream filters and caching

        if *suppress_downstream_body && is_downstream_followup(&task) {
            // Cache admission already observed this task's queued chunks.
            sink.take_extra();
            if matches!(task, HttpTask::Failed(_)) {
                *upstream_reusable = false;
                abort_cache_after_response_source_failure(session, from_cache);
            }
            return Ok(());
        }

        let res: Result<HttpTask> = match task {
            HttpTask::Header(mut header, eos) => {
                let cache_header = terminal_header.then(|| header.clone());
                if !from_cache {
                    proxy_cache::strip_terminal_synthetic_wire_marker(&mut header);
                }
                let terminal_synthetic_entity = proxy_cache::is_terminal_synthetic_entity(&header);
                let substituted = if from_cache {
                    filtered_terminal_header
                        .take()
                        .map(|filtered_header| header = filtered_header)
                        .is_some()
                } else {
                    false
                };
                if !substituted {
                    /* Downstream revalidation, only needed when cache is on because otherwise origin
                     * will handle it */
                    if session.upstream_headers_mutated_for_cache() {
                        self.downstream_response_conditional_filter(
                            serve_from_cache,
                            session,
                            &mut header,
                            ctx,
                        );
                        // A terminal header describes no upstream body, so its
                        // Content-Length cannot range the body generated below.
                        let skip_range = if from_cache {
                            terminal_synthetic_entity
                        } else {
                            terminal_header
                        };
                        if !skip_range && !session.ignore_downstream_range {
                            let range_type =
                                self.inner.range_header_filter(session, &mut header, ctx);
                            range_body_filter.set(range_type);
                        }
                    }
                    self.inner
                        .response_filter(session, &mut header, ctx)
                        .await?;
                }
                if !from_cache
                    && session.as_downstream().is_upgrade_req()
                    && header.status == StatusCode::SWITCHING_PROTOCOLS
                {
                    terminal_body.mark_upgraded();
                }
                if terminal_header {
                    if let Some(duration) = self
                        .terminal_upstream_body_filter(session, sink, ctx)
                        .await?
                    {
                        trace!("delaying terminal upstream response for {duration:?}");
                        time::sleep(duration).await;
                    }
                    let mut cache_header =
                        cache_header.expect("terminal header must retain its cache representation");
                    reconcile_terminal_cache_header(&mut cache_header, sink);
                    reconcile_terminal_cache_header(&mut header, sink);
                    proxy_cache::mark_terminal_synthetic_entity(&mut cache_header);
                    *filtered_terminal_header = Some(header.clone());
                    let cache_task = HttpTask::Header(cache_header, true);
                    self.track_predicted_uncacheable_response(session, &cache_task, sink);
                    self.cache_task_and_emitted_chunks_with_decision(
                        session,
                        &cache_task,
                        sink,
                        terminal_cacheability,
                        ctx,
                        serve_from_cache,
                    )
                    .await?;
                    if !serve_from_cache.should_send_to_downstream() {
                        sink.take_extra();
                        return Ok(());
                    }
                }
                if downstream_response_body_forbidden(session, &header) {
                    sink.take_extra();
                    header.remove_header(&http::header::TRANSFER_ENCODING);
                    if header.status.is_informational() || header.status.as_u16() == 204 {
                        header.remove_header(&http::header::CONTENT_LENGTH);
                    }
                }
                if !header.status.is_informational() {
                    *suppress_downstream_body =
                        terminal_header || downstream_response_body_forbidden(session, &header);
                }
                /* Downgrade the version so that write_response_header won't panic */
                header.set_version(Version::HTTP_11);

                // these status codes / method cannot have body, so no need to add chunked encoding
                /* Add chunked header to tell downstream to use chunked encoding
                 * during the absent of content-length in h2 */
                if !downstream_response_body_forbidden(session, &header)
                    && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                {
                    header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                }
                Ok(HttpTask::Header(header, eos || *suppress_downstream_body))
            }
            HttpTask::Body(data, eos) => {
                let data = range_body_filter.filter_body(data);
                Ok(HttpTask::Body(data, eos))
            }
            HttpTask::UpgradedBody(..) => {
                // An h2 session should not be able to send an h2 upgraded response body,
                // and logically that is impossible unless there is a bug in the client v2 session
                panic!("Unexpected UpgradedBody task while proxy h2");
            }
            HttpTask::Trailer(mut trailers) => {
                let trailer_buffer = match trailers.as_mut() {
                    Some(trailers) => {
                        debug!("Parsing response trailers..");
                        match self
                            .inner
                            .response_trailer_filter(session, trailers, ctx)
                            .await
                        {
                            Ok(buf) => buf,
                            Err(e) => {
                                error!(
                                    "Encountered error while filtering upstream trailers {:?}",
                                    e
                                );
                                None
                            }
                        }
                    }
                    _ => None,
                };
                // if we have a trailer buffer write it to the downstream response body
                if let Some(buffer) = trailer_buffer {
                    // write_body will not write additional bytes after reaching the content-length
                    // for gRPC H2 -> H1 this is not a problem but may be a problem for non gRPC code
                    // https://http2.github.io/http2-spec/#malformed
                    Ok(HttpTask::Body(Some(buffer), true))
                } else {
                    Ok(HttpTask::Trailer(trailers))
                }
            }
            HttpTask::Done if from_cache => Ok(HttpTask::Body(None, true)),
            HttpTask::Done => Ok(task),
            HttpTask::Failed(_) => Ok(task), // Do nothing just pass the error down
        };
        let task = res?;
        let start = out_tasks.len();
        if from_cache {
            // The cache-serving pump arm shares this `sink` with the
            // upstream-batch arm across the whole response, but never itself
            // runs the upstream body filter that fills it (`upstream_filter`
            // is only called above, inside `if !from_cache`). Anything still
            // queued here belongs to an earlier upstream-batch call within
            // this same response and must not be replayed into a cache-hit
            // task -- see the `sink.take_extra()` discard on the early-return
            // path above for where that would otherwise leak from.
            out_tasks.push(task);
        } else if terminal_dispatch {
            // The terminal callback releases body bytes the filter had been
            // withholding. They are body, so they must precede the trailer
            // that ends the response -- the opposite of the ordinary drain
            // below. `task` keeps its own end-of-stream meaning.
            drain_emitted_chunks_before(task, sink, terminal_body.is_upgraded(), out_tasks);
        } else {
            // Extra chunks emitted by the upstream body filter follow the
            // chunk they were emitted from, preserving order; `task`'s own
            // end-of-stream flag migrates onto the last of them when there
            // are any (see `drain_emitted_chunks`).
            drain_emitted_chunks(task, sink, out_tasks);
        }
        if terminal_header {
            let downstream_body_forbidden = match &out_tasks[start] {
                HttpTask::Header(header, _) => downstream_response_body_forbidden(session, header),
                _ => unreachable!("terminal response must start with a header"),
            };
            if !downstream_body_forbidden {
                self.downstream_response_body_filter_tasks(session, &mut out_tasks[start..], ctx)
                    .await?;
            }
            reconcile_terminal_response_tasks(out_tasks, start, downstream_body_forbidden)?;
            // A `Trailer` task is not a `Body` task, so released bytes would
            // otherwise skip the downstream body filter entirely.
        } else if filter_downstream_body || terminal_dispatch {
            self.downstream_response_body_filter_tasks(session, &mut out_tasks[start..], ctx)
                .await?;
        }
        Ok(())
    }

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
    async fn finish_downstream_body_side(
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

    async fn send_body_to2(
        &self,
        session: &mut Session,
        mut data: Option<Bytes>,
        mut event: RequestBodyEvent,
        client_body: &mut h2::SendStream<bytes::Bytes>,
        ctx: &mut SV::CTX,
        body_write: &UpstreamBodyWrite,
    ) -> Result<UpstreamBodyOutcome>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        // `data == None` IS the end of the downstream body, whatever the caller
        // computed from `is_body_done()`. Mirrors the H1 pump's
        // `send_body_to_pipe`, and it is load-bearing rather than cosmetic:
        // without it a `None` read paired with `is_body_done() == false` would
        // invoke the application hooks with `(None, end_of_stream = false)` --
        // violating their documented contract -- never deliver the single
        // `(None, true)` event, and, with `stream_closed` set, keep returning
        // `Complete(false)` so the duplex loop below would spin on an
        // already-finished read side at 100% CPU.
        //
        // The two facts cannot disagree on an H1 or H2 downstream any more (a
        // `None` read latches the end-of-stream fact in both session types), but
        // they CAN on a `SessionCustom` downstream, whose `is_body_done()` is
        // implemented by the user -- and this pump serves an H1/H2/custom
        // downstream depending only on which UPSTREAM protocol was selected.
        if data.is_none() && event == RequestBodyEvent::Data {
            event = RequestBodyEvent::Complete;
        }
        let end_of_body = event.is_terminal();

        if event.is_complete()
            && data.is_none()
            && !session.request_trailer_filter_fired
            && session
                .downstream_session
                .request_trailers_present()
                .unwrap_or(false)
        {
            let action = self.inner.request_trailer_filter(session, ctx).await?;
            // At most once per downstream request: a retry attempt replays the
            // same EOF (`data == None`) while the trailer fact stays true, and
            // the hook's contract is a single invocation.
            //
            // Latched only AFTER the hook returns: the pinned downstream
            // future can be dropped mid-hook (the `select!` upstream-error
            // arm) and the request then retried, and latching first would
            // suppress the hook forever -- zero completed invocations for a
            // trailer-bearing request.
            session.request_trailer_filter_fired = true;
            if action == RequestBodyAction::Terminate {
                warn_terminate_without_response(session, "request_trailer_filter");
                return Ok(UpstreamBodyOutcome::Downstream(
                    DownstreamRequestOutcome::Terminate,
                ));
            }
        }

        session
            .downstream_modules_ctx
            .request_body_filter(&mut data, event)
            .await?;

        if self
            .inner
            .request_body_filter_action(session, &mut data, event, ctx)
            .await?
            == RequestBodyAction::Terminate
        {
            warn_terminate_without_response(session, "request_body_filter_action");
            return Ok(UpstreamBodyOutcome::Downstream(
                DownstreamRequestOutcome::Terminate,
            ));
        }

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
            debug!("upstream request stream already closed; not writing the end of stream");
            return Ok(UpstreamBodyOutcome::Downstream(
                DownstreamRequestOutcome::Complete(end_of_body),
            ));
        }

        let (data, end, eos_write_optional) = match data {
            Some(data) => {
                debug!("Write {} bytes body to h2 upstream", data.len());
                (data, end_of_body, false)
            }
            None => {
                debug!("Read downstream body done");
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
                client_body.reserve_capacity(0);
                warn!(
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
                debug!("upstream request stream would not take the final END_STREAM: {e}");
            } else {
                return upstream_write_error_outcome(e, end, body_write);
            }
        }

        Ok(UpstreamBodyOutcome::Downstream(
            DownstreamRequestOutcome::Complete(end_of_body),
        ))
    }
}

/* Read response header, body and trailer from h2 upstream and send them to tx */
pub(crate) async fn pipe_up_to_down_response(
    client: &mut Http2Session,
    tx: mpsc::Sender<HttpTask>,
    pipe_state: Arc<AtomicU8>,
) -> Result<()> {
    client
        .read_response_header()
        .await
        .map_err(|e| e.into_up())?; // should we send the error as an HttpTask?

    let resp_header = Box::new(client.response_header().expect("just read").clone());

    match client.check_response_end_or_error() {
        Ok(eos) => {
            // XXX: the h2 crate won't check for content-length underflow
            // if a header frame with END_STREAM is sent without data frames
            // As stated by RFC, "204 or 304 responses contain no content,
            // as does the response to a HEAD request"
            // https://datatracker.ietf.org/doc/html/rfc9113#section-8.1.1
            let req_header = client.request_header().expect("must have sent req");
            if eos
                && req_header.method != Method::HEAD
                && resp_header.status != StatusCode::NO_CONTENT
                && resp_header.status != StatusCode::NOT_MODIFIED
                // RFC technically allows for leading zeroes
                // https://datatracker.ietf.org/doc/html/rfc9110#name-content-length
                && resp_header
                    .headers
                    .get(CONTENT_LENGTH)
                    .is_some_and(|cl| cl.as_bytes().iter().any(|b| *b != b'0'))
            {
                let _ = tx
                    .send(HttpTask::Failed(
                        Error::explain(H2Error, "non-zero content-length on EOS headers frame")
                            .into_up(),
                    ))
                    .await;
                return Ok(());
            }
            tx.send(HttpTask::Header(resp_header, eos))
                .await
                .or_err(InternalError, "sending h2 headers to pipe")?;
        }
        Err(e) => {
            // If upstream errored, then push error to downstream and then quit
            // Don't care if send fails (which means downstream already gone)
            // we were still able to retrieve the headers, so try sending
            let _ = tx.send(HttpTask::Header(resp_header, false)).await;
            let _ = tx.send(HttpTask::Failed(e.into_up())).await;
            return Ok(());
        }
    }

    // Read body from H2 upstream, racing each read against tx.closed().
    //
    // When proxying an H2 upstream response with Content-Length to an H1 downstream,
    // bidirection_down_to_up() may determine the response is complete (all Content-Length
    // bytes written) and exit before the H2 stream signals END_STREAM. This drops the
    // receiving end (rx) of the channel. Without this race, read_response_body() would
    // block until the H2 stream eventually ends (e.g. via trailers or read_timeout),
    // while the downstream side (which could be H1) is in theory already done.
    loop {
        let chunk = tokio::select! {
            biased;
            body = client.read_response_body() => {
                body.map_err(|e| e.into_up()).transpose()
            }
            _ = tx.closed() => None,
        };
        let Some(chunk) = chunk else {
            break;
        };
        let data = match chunk {
            Ok(d) => d,
            Err(e) => {
                // Push the error to downstream and then quit
                let _ = tx.send(HttpTask::Failed(e.into_up())).await;
                // Downstream should consume all remaining data and handle the error
                return Ok(());
            }
        };
        match client.check_response_end_or_error() {
            Ok(eos) => {
                let empty = data.is_empty();
                if empty && !eos {
                    /* it is normal to get 0 bytes because of multi-chunk
                     * don't write 0 bytes to downstream since it will be
                     * misread as the terminating chunk */
                    continue;
                }
                // A send failure is benign only when the downstream half signaled it
                // completed (e.g. an H1 downstream finished by Content-Length before the
                // H2 stream signaled end-of-stream): stop reading the upstream stream.
                // Otherwise the closure is unexpected, so surface the original error.
                let send_result = tx.send(HttpTask::Body(Some(data), eos)).await;
                if send_result.is_err()
                    && PipeState::is_downstream_complete(pipe_state.load(Ordering::Acquire))
                {
                    return Ok(());
                }
                send_result.or_err(InternalError, "sending h2 body to pipe")?;
            }
            Err(e) => {
                // Similar to above, push the error to downstream and then quit
                let _ = tx.send(HttpTask::Failed(e.into_up())).await;
                return Ok(());
            }
        }
    }

    // If the channel is already closed, the downstream half is finished. This
    // skips trailers/done, but the downstream half has already finished so there
    // is nothing more to send. Benign only if the downstream half signaled
    // completion; otherwise the closure is unexpected, so surface it.
    if tx.is_closed() {
        if PipeState::is_downstream_complete(pipe_state.load(Ordering::Acquire)) {
            return Ok(());
        }
        return Error::e_explain(
            InternalError,
            "h2 task pipe closed unexpectedly before trailers",
        );
    }

    // attempt to get trailers, racing against channel close
    let trailers = tokio::select! {
        biased;
        t = client.read_trailers() => {
            match t {
                Ok(t) => t,
                Err(e) => {
                    let _ = tx.send(HttpTask::Failed(e.into_up())).await;
                    return Ok(());
                }
            }
        }
        _ = tx.closed() => {
            // Benign only if the downstream half signaled completion; otherwise
            // the closure is unexpected, so surface it.
            if PipeState::is_downstream_complete(pipe_state.load(Ordering::Acquire)) {
                return Ok(());
            }
            return Error::e_explain(InternalError, "h2 task pipe closed unexpectedly while reading trailers");
        }
    };

    let trailers = trailers.map(Box::new);

    if trailers.is_some() {
        // Benign only if the downstream signaled completion, same as the body sends above.
        let send_result = tx.send(HttpTask::Trailer(trailers)).await;
        if send_result.is_err()
            && PipeState::is_downstream_complete(pipe_state.load(Ordering::Acquire))
        {
            return Ok(());
        }
        send_result.or_err(InternalError, "sending h2 trailer to pipe")?;
    }

    let send_result = tx.send(HttpTask::Done).await;
    if send_result.is_err() && PipeState::is_downstream_complete(pipe_state.load(Ordering::Acquire))
    {
        debug!("h2 to h1 channel closed!");
        return Ok(());
    }
    send_result.or_err(InternalError, "sending h2 done to pipe")?;

    Ok(())
}

#[test]
fn test_update_authority() {
    let mut parts = http::request::Builder::new()
        .body(())
        .unwrap()
        .into_parts()
        .0;
    update_h2_scheme_authority(&mut parts, b"example.com", true).unwrap();
    assert_eq!("example.com", parts.uri.authority().unwrap());
    update_h2_scheme_authority(&mut parts, b"example.com:456", true).unwrap();
    assert_eq!("example.com:456", parts.uri.authority().unwrap());
    update_h2_scheme_authority(&mut parts, b"example.com:", true).unwrap();
    assert_eq!("example.com:", parts.uri.authority().unwrap());
    update_h2_scheme_authority(&mut parts, b"example.com:123:345", true).unwrap();
    assert_eq!("example.com:123", parts.uri.authority().unwrap());
    update_h2_scheme_authority(&mut parts, b"[::1]", true).unwrap();
    assert_eq!("[::1]", parts.uri.authority().unwrap());

    // verify scheme
    update_h2_scheme_authority(&mut parts, b"example.com", true).unwrap();
    assert_eq!("https://example.com", parts.uri);
    update_h2_scheme_authority(&mut parts, b"example.com", false).unwrap();
    assert_eq!("http://example.com", parts.uri);
}

#[test]
fn test_streamed_disposition_removes_h2_framing_and_keeps_stream_open() {
    let mut request = RequestHeader::build("POST", b"/", None).unwrap();
    request.insert_header(CONTENT_LENGTH, "0").unwrap();
    request
        .insert_header(http::header::TRANSFER_ENCODING, "chunked")
        .unwrap();

    apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed);

    assert!(request.headers.get(CONTENT_LENGTH).is_none());
    assert!(request
        .headers
        .get(http::header::TRANSFER_ENCODING)
        .is_none());
    assert!(!upstream_headers_end_stream(
        UpstreamRequestBodyDisposition::Streamed,
        true,
        true
    ));
}

/// Full truth table of the upstream EOS decision, as the pump applies it:
/// [`upstream_empty_data_end_stream`] is only consulted when
/// [`upstream_headers_end_stream`] said `false`. Every row is
/// (disposition, send_end_stream, body_empty) -> (headers_eos, empty_data_eos),
/// and the pair must always produce AT MOST ONE upstream EOS -- exactly one
/// whenever no downstream body can still arrive.
///
/// This pins the PRIMITIVES over their whole input domain. Which `body_empty`
/// each disposition is actually handed is a separate decision made by
/// [`upstream_framing_body_empty`], and it pins the `Streamed` rows below to
/// `body_empty == false`; see
/// `test_streamed_never_takes_an_early_eos_from_the_call_site`.
#[test]
fn test_upstream_eos_truth_table() {
    use UpstreamRequestBodyDisposition::*;

    // (disposition, send_end_stream, body_empty, headers_eos, empty_data_eos)
    let table = [
        // Ordinary: unchanged legacy behavior. The EOS rides on HEADERS when
        // allowed, otherwise on an empty DATA frame; with a body, neither.
        (Ordinary, true, true, true, false),
        (Ordinary, true, false, false, false),
        (Ordinary, false, true, false, true),
        (Ordinary, false, false, false, false),
        // Bodyless: no upstream body will follow, so the stream closes here
        // either way. `send_end_stream == false` (the gRPC-web bridge) MUST
        // get the empty DATA frame, not END_STREAM on HEADERS.
        (Bodyless, true, true, true, false),
        (Bodyless, true, false, true, false),
        (Bodyless, false, true, false, true),
        (Bodyless, false, false, false, true),
        // Streamed: HEADERS never carry EOS (the length is unknown at header
        // time). With a downstream body already finished nothing will ever be
        // read, so close now; otherwise the pump sends the EOS with the body.
        (Streamed, true, true, false, true),
        (Streamed, true, false, false, false),
        (Streamed, false, true, false, true),
        (Streamed, false, false, false, false),
    ];

    for (disposition, send_end_stream, body_empty, headers_eos, data_eos) in table {
        let actual_headers_eos =
            upstream_headers_end_stream(disposition, send_end_stream, body_empty);
        assert_eq!(
            actual_headers_eos, headers_eos,
            "headers EOS for {disposition:?} send_end_stream={send_end_stream} body_empty={body_empty}"
        );
        // As the pump applies it: gated on the headers decision, so an
        // already-closed stream never gets a second, standalone END_STREAM.
        let actual_data_eos = !actual_headers_eos
            && upstream_empty_data_end_stream(disposition, send_end_stream, body_empty);
        assert_eq!(
            actual_data_eos, data_eos,
            "empty-DATA EOS for {disposition:?} send_end_stream={send_end_stream} body_empty={body_empty}"
        );
        // Whenever the downstream body is already finished, exactly one EOS
        // must have been emitted here; otherwise the pump still owns it.
        if body_empty {
            assert!(
                actual_headers_eos ^ actual_data_eos,
                "no single upstream EOS for {disposition:?} send_end_stream={send_end_stream}"
            );
        }
    }
}

/// The gRPC-web bridge calls `set_send_end_stream(false)` because gRPC
/// requires a bodyless request stream to be closed by an empty DATA frame
/// with END_STREAM. `Bodyless` must not override that.
#[test]
fn test_bodyless_honors_explicit_send_end_stream_false() {
    assert!(!upstream_headers_end_stream(
        UpstreamRequestBodyDisposition::Bodyless,
        false,
        false
    ));
    assert!(upstream_empty_data_end_stream(
        UpstreamRequestBodyDisposition::Bodyless,
        false,
        false
    ));
}

/// `Streamed` must NEVER get an early upstream EOS, whatever the request
/// declared (design 4.4).
///
/// This is asserted AT THE CALL SITE's own decision function, not at the
/// primitives: `upstream_empty_data_end_stream`'s `Streamed` arm does close the
/// stream when handed `body_empty == true`, and feeding it the request's
/// `Content-Length: 0` declaration is exactly the regression this pins. An early
/// EOS there sets `upstream_body_closed`, which makes the suppressed-write
/// branch of `send_body_to2` refuse every byte the application streams in
/// through `request_body_filter_action` -- the whole point of `Streamed`.
#[test]
fn test_streamed_never_takes_an_early_eos_from_the_call_site() {
    use UpstreamRequestBodyDisposition::*;
    for declared_empty in [false, true] {
        let body_empty = upstream_framing_body_empty(Streamed, declared_empty);
        assert!(
            !body_empty,
            "Streamed must not inherit the declaration (declared_empty={declared_empty})"
        );
        for send_end_stream in [true, false] {
            let headers_eos = upstream_headers_end_stream(Streamed, send_end_stream, body_empty);
            let data_eos = !headers_eos
                && upstream_empty_data_end_stream(Streamed, send_end_stream, body_empty);
            assert!(
                !headers_eos && !data_eos,
                "Streamed sent an early EOS (declared_empty={declared_empty} \
                 send_end_stream={send_end_stream})"
            );
        }
    }
}

/// The mirror row: `Ordinary` DOES take the declaration, which is what lets a
/// `Content-Length: 0` request reach an origin that will not answer until it has
/// seen the end of the request stream.
#[test]
fn test_ordinary_takes_the_declaration_for_upstream_framing() {
    use UpstreamRequestBodyDisposition::*;
    assert!(upstream_framing_body_empty(Ordinary, true));
    assert!(!upstream_framing_body_empty(Ordinary, false));
    // ...and exactly one EOS is emitted for it, wherever `send_end_stream` puts it.
    for send_end_stream in [true, false] {
        let body_empty = upstream_framing_body_empty(Ordinary, true);
        let headers_eos = upstream_headers_end_stream(Ordinary, send_end_stream, body_empty);
        let data_eos =
            !headers_eos && upstream_empty_data_end_stream(Ordinary, send_end_stream, body_empty);
        assert!(headers_eos ^ data_eos, "send_end_stream={send_end_stream}");
    }
}

/// `Bodyless` with a real downstream body closes the upstream stream at header
/// time under BOTH `send_end_stream` settings, which is exactly why the pump
/// has to suppress its body writes instead of letting h2 fail the stream.
#[test]
fn test_bodyless_with_a_real_body_always_closes_at_header_time() {
    use UpstreamRequestBodyDisposition::*;
    for send_end_stream in [true, false] {
        let headers_eos = upstream_headers_end_stream(Bodyless, send_end_stream, false);
        let data_eos =
            !headers_eos && upstream_empty_data_end_stream(Bodyless, send_end_stream, false);
        assert!(
            headers_eos ^ data_eos,
            "Bodyless send_end_stream={send_end_stream} must close the stream exactly once"
        );
    }
}

/// I2: the write-error swallow must be keyed on the failure SHAPE, not just on
/// the wire END_STREAM flag.
///
/// `upstream_response_ended` is set for every upstream response the peer ended
/// cleanly, and it stays set. If it were the whole condition, then after any
/// such response EVERY request-body write failure would be swallowed and the
/// exchange logged a success -- including an application body filter's own
/// error and the `Bodyless` contract violation, which have nothing to do with
/// the peer.
///
/// This function answers only "is the stream GONE". A `write_timeout` does not
/// make it gone and still answers `false` here; it reaches the swallow through
/// [`upstream_write_stalled_after_response`] instead, which asks a different
/// question. Keeping the two apart is the point -- see
/// `test_a_stalled_write_is_a_separate_swallowable_shape`.
#[test]
fn test_only_stream_gone_write_failures_may_be_swallowed() {
    // The two shapes `write_body` produces when h2 will never take another byte
    // on this stream.
    for (etype, context) in [
        (H2Error, "cannot reserve capacity"),
        (H2Error, "while waiting for capacity"),
        (WriteError, "while writing h2 request body"),
    ] {
        let e = Error::explain(etype, context.to_string());
        assert!(
            upstream_write_failed_because_stream_gone(&e),
            "{context} means the upstream stream is gone"
        );
    }

    // A local deadline is not a peer signal.
    let timed_out = Error::explain(
        WriteTimedout,
        "while writing h2 request body, timeout: 1s".to_string(),
    );
    assert!(
        !upstream_write_failed_because_stream_gone(&timed_out),
        "a locally configured write_timeout must still fail the exchange: swallowing \
         it truncates the upstream request body and reports success"
    );

    // And nothing else is a peer signal either -- the application's own body
    // filters, the `Bodyless` contract violation, the cache.
    for etype in [InternalError, ReadError, ReadTimedout, ConnectError] {
        let e = Error::explain(etype.clone(), "".to_string());
        assert!(
            !upstream_write_failed_because_stream_gone(&e),
            "{etype:?} is not the upstream closing its request stream"
        );
    }
}

/// The second swallowable shape: the stream is NOT gone, the peer simply
/// stopped granting request-body capacity after having answered in full.
///
/// Kept distinct from the stream-gone question on purpose. A `write_timeout`
/// is a LOCAL deadline and says nothing about the peer by itself, which is why
/// it must never widen [`upstream_write_failed_because_stream_gone`]; it only
/// carries meaning in conjunction with the wire END_STREAM flag, and
/// `upstream_write_error_outcome` is the only place that conjunction is formed.
#[test]
fn test_a_stalled_write_is_a_separate_swallowable_shape() {
    let timed_out = Error::explain(
        WriteTimedout,
        "while writing h2 request body, timeout: 1s".to_string(),
    );
    assert!(
        upstream_write_stalled_after_response(&timed_out),
        "an expired write window is the stalled shape"
    );
    assert!(
        !upstream_write_failed_because_stream_gone(&timed_out),
        "and it must NOT be laundered into the stream-gone shape"
    );

    // The stream-gone shapes are not stalls: they are answered by the other
    // predicate, and the two must not overlap.
    for (etype, context) in [
        (H2Error, "cannot reserve capacity"),
        (WriteError, "while writing h2 request body"),
    ] {
        let e = Error::explain(etype, context.to_string());
        assert!(
            !upstream_write_stalled_after_response(&e),
            "{context} means the stream is gone, not stalled"
        );
    }

    // Nothing else is a stall either.
    for etype in [InternalError, ReadError, ReadTimedout, ConnectError] {
        let e = Error::explain(etype.clone(), "".to_string());
        assert!(
            !upstream_write_stalled_after_response(&e),
            "{etype:?} is not the upstream withholding capacity"
        );
    }
}

/// Neither swallowable shape may fire without the wire END_STREAM flag.
///
/// The flag is the whole reason the exchange survives a failed request-body
/// write: it is what says the origin already answered. `PeerEndStream::default`
/// is the no-watch-installed case, where the flag can never be set -- and every
/// failure must then cost the exchange, exactly as it did before either shape
/// existed.
#[test]
fn test_no_write_failure_is_swallowed_without_wire_end_stream() {
    let body_write = UpstreamBodyWrite {
        timeout: None,
        stream_closed: false,
        disposition: UpstreamRequestBodyDisposition::Ordinary,
        eos_write_optional: false,
        upstream_response_ended: PeerEndStream::default(),
    };
    assert!(
        !body_write.upstream_response_ended.observed(),
        "a default PeerEndStream is the no-evidence case"
    );

    for (etype, context) in [
        (H2Error, "cannot reserve capacity"),
        (WriteError, "while writing h2 request body"),
        (WriteTimedout, "while writing h2 request body, timeout: 1s"),
    ] {
        let e = Error::explain(etype.clone(), context.to_string());
        assert!(
            upstream_write_error_outcome(e, true, &body_write).is_err(),
            "{etype:?} must fail the exchange with no wire END_STREAM evidence"
        );
    }
}
