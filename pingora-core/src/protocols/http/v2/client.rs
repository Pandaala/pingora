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

//! HTTP/2 client session and connection
// TODO: this module needs a refactor

use bytes::Bytes;
use futures::FutureExt;
use h2::client::{self, ResponseFuture, SendRequest};
use h2::{Reason, RecvStream, SendStream};
use http::HeaderMap;
use log::{debug, error, warn};
use pingora_error::{Error, ErrorType, ErrorType::*, OrErr, Result, RetryType};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_timeout::timeout;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

use crate::connectors::http::v2::ConnectionRef;
use crate::protocols::http::v1::common::validate_content_length_without_transfer_encoding;
use crate::protocols::http::v2::end_stream_watch::StreamRecord;
use crate::protocols::{Digest, SocketAddr, UniqueIDType};

pub const PING_TIMEDOUT: ErrorType = ErrorType::new("PingTimedout");

/// Validate response headers after HTTP/2 decoding but before Pingora accepts
/// them as an upstream response.
///
/// Reconciles `Content-Length` per RFC 9110 section 8.6 (hyper parity):
/// identical duplicates and comma-combined identical values are accepted while
/// differing or unparseable values are rejected, so an ambiguous H2 response is
/// not forwarded to a downstream H1 or custom session.
fn validate_response_header(header: &ResponseHeader) -> Result<()> {
    validate_content_length_without_transfer_encoding(&header.headers)
}

pub struct Http2Session {
    send_req: SendRequest<Bytes>,
    send_body: Option<SendStream<Bytes>>,
    resp_fut: Option<ResponseFuture>,
    req_sent: Option<Box<RequestHeader>>,
    response_header: Option<ResponseHeader>,
    response_body_reader: Option<RecvStream>,
    // Trailers validated while confirming body EOF. `Some(None)` records a
    // validated trailer-free end, while `Some(Some(..))` preserves fields for
    // the later public `read_trailers` call.
    response_trailers: Option<Option<HeaderMap>>,
    response_body_error: bool,
    // Set only after the final response headers pass Pingora validation and
    // h2 reports that those initial headers carried END_STREAM. This keeps a
    // wire-level terminal HEADERS observation from misclassifying a valid
    // header-only response as lost trailers.
    response_initial_end_stream: bool,
    /// The read timeout, which will be applied to both reading the header and the body.
    /// The timeout is reset on every read. This is not a timeout on the overall duration of the
    /// response.
    pub read_timeout: Option<Duration>,
    /// The write timeout which will be applied to writing request body.
    /// The timeout is reset on every write. This is not a timeout on the overall duration of the
    /// request.
    pub write_timeout: Option<Duration>,
    pub conn: ConnectionRef,
    // Indicate that whether a END_STREAM is already sent
    ended: bool,
    // Total DATA payload bytes received from upstream response
    body_recv: usize,
    // Latched once the TRANSPORT said the response body ended: END_STREAM on
    // the response HEADERS frame, or `RecvStream::is_end_stream()` observed at
    // any body poll.
    //
    // It is latched rather than recomputed because `h2` destroys the fact: a
    // RST_STREAM received after END_STREAM overwrites the stream state with
    // `Closed(Cause::Error(..))`, after which `RecvStream::is_end_stream()`
    // reports `false` and the two situations are indistinguishable.
    response_body_eof: bool,
    // Source (iii) of the same end-of-body proof: the non-zero `content-length`
    // the response declared, compared against `body_recv`. This is the source
    // that covers the NATURAL wire ordering -- upstream writes DATA(END_STREAM)
    // then RST_STREAM, and we poll only afterwards -- in which the END_STREAM
    // evidence is already overwritten by the time the final chunk is handed
    // over, so the two latched sources can never fire. See
    // [`Http2Session::response_body_complete`].
    response_body_declared_len: Option<usize>,
    // The h2 stream id, once the request has been sent.
    stream_id: Option<u32>,
    // Source (iv): set by [`super::end_stream_watch`] when the peer's END_STREAM
    // flag was seen on the wire BEFORE anything tore the stream down. This is
    // the only source that survives the RFC 9113 §8.1 shape -- a complete
    // response followed by RST_STREAM(NO_ERROR) while this side is still
    // uploading -- because `h2` overwrites the stream state on that reset. May
    // only be consulted once a read has already failed; see
    // [`Http2Session::response_body_complete_at_stream_end`].
    peer_end_stream: PeerEndStream,
}

/// A cheap, cloneable handle on source (iv) for ONE h2 stream: whether the peer
/// flagged END_STREAM on the wire before anything tore that stream down, and
/// how many DATA payload bytes it put on the wire on the way there.
///
/// Cloneable so that the fact can be sampled at the moment a read (or an
/// upstream request-body write, see `pingora-proxy`'s h2 pump) fails, rather
/// than while `&mut self` is tied up by the reader. The record is finalized at
/// most once and never retracted, so no further synchronization is needed.
///
/// It is NOT an end-of-body proof on its own: the record is made as the bytes
/// pass INTO `h2`, which can be before `h2` has decoded them and before they
/// have been read, so anything gating *whether to keep reading* must not use
/// it. See [`Http2Session::response_body_complete_at_stream_end`] for the only
/// conditions under which it may be consulted.
#[derive(Clone, Debug, Default)]
pub struct PeerEndStream(Option<Arc<StreamRecord>>);

impl PeerEndStream {
    /// Whether the peer flagged END_STREAM for this stream on the wire before
    /// anything tore it down. Always `false` when no watch is installed.
    ///
    /// This is the WEAK question, and the only one a caller that is not
    /// deciding response completeness may ask -- `pingora-proxy`'s h2 pump uses
    /// it to tell "the origin said it was done with me" from "my write broke"
    /// at a site that reports nothing about the response. Anything deciding
    /// whether a body is whole must use [`Self::vouches_for`], which also
    /// checks that `h2` did not drop bytes the wire carried.
    pub fn observed(&self) -> bool {
        self.0.as_ref().is_some_and(|r| r.end_stream_observed())
    }

    /// Whether the peer flagged END_STREAM *and* the wire carried exactly
    /// `body_recv` DATA payload bytes for this stream.
    fn vouches_for(&self, body_recv: usize) -> bool {
        self.0.as_ref().is_some_and(|r| r.vouches_for(body_recv))
    }

    /// The shared record behind this handle, for the callers that have to mark
    /// it rather than read it: `Http2Session::note_local_reset` and
    /// `Http2Session::drop`.
    fn record(&self) -> Option<&StreamRecord> {
        self.0.as_deref()
    }

    fn terminal_headers_observed(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|r| r.terminal_headers_observed())
    }
}

/// The end-of-body facts one in-flight read needs, sampled BEFORE the reader
/// borrows `&mut Http2Session`.
///
/// Every field is fixed for the duration of the read that sampled it: a read
/// that fails delivered no bytes, so `body_recv` cannot have moved, and the
/// wire record only ever goes from "undecided" to its final value.
struct EndOfBodyProof {
    /// [`Http2Session::response_body_complete`] -- sources (i)-(iii).
    body_complete: bool,
    /// Bytes handed to the caller so far.
    body_recv: usize,
    /// Whether a declared `content-length` has been reached. Source (iv) may
    /// not overrule it: `h2` can drop a rejected TRAILERS frame without
    /// dropping any DATA, which leaves the byte counts agreeing on a body that
    /// the response's own framing says is short.
    declared_len_satisfied: bool,
    /// Source (iv).
    peer_end_stream: PeerEndStream,
}

impl EndOfBodyProof {
    fn holds(&self) -> bool {
        self.body_complete
            || (self.declared_len_satisfied && self.peer_end_stream.vouches_for(self.body_recv))
    }
}

/// Whether an `h2` error observed *after* the response body already reached
/// EOF is a benign way for the peer to end the stream rather than a read
/// failure.
///
/// The response is complete at this point, so neither of these means the bytes
/// we read are suspect:
/// - GOAWAY with `NO_ERROR`: graceful connection shutdown.
/// - RST_STREAM with `NO_ERROR`: RFC 9113 §8.1's "a server MAY request that
///   the client abort transmission of a request without error by sending a
///   RST_STREAM with an error code of NO_ERROR after sending a complete
///   response".
///
/// Mirrors the server-side classification in
/// [`crate::protocols::http::v2::server`]. Deliberately narrower than the
/// server's: `CANCEL` is *not* accepted here. A downstream client cancelling a
/// response it no longer wants is routine; an upstream server cancelling a
/// response it already sent in full is not, and the existing trailer
/// classification this reuses has only ever accepted `NO_ERROR`.
///
/// The caller MUST have proven the EOF first. Without that guard this
/// predicate would map a mid-body reset -- a truncated response -- onto a
/// clean end of body and hand the truncation to the downstream client.
fn benign_post_eof_stream_end(e: &h2::Error) -> bool {
    (e.is_go_away() || e.is_reset()) && e.is_remote() && e.reason() == Some(Reason::NO_ERROR)
}

impl Drop for Http2Session {
    fn drop(&mut self) {
        if let (Some(watch), Some(id)) = (self.conn.end_stream_watch(), self.stream_id) {
            // Invalidate rather than merely forget. Dropping the session drops
            // `send_body` too, and `h2` cancels a still-open stream when its
            // last handle goes away -- the same local-reset shape
            // `note_local_reset` guards, minus the explicit call. Since
            // [`Self::peer_end_stream`] is public, a caller may still hold a
            // clone of the record at this point, and a map removal would not
            // reach it. This never retracts evidence published earlier, so a
            // session dropped after a clean response is unaffected.
            watch.invalidate(id, self.peer_end_stream.record());
        }
        self.conn.release_stream();
    }
}

impl Http2Session {
    pub(crate) fn new(send_req: SendRequest<Bytes>, conn: ConnectionRef) -> Self {
        Http2Session {
            send_req,
            send_body: None,
            resp_fut: None,
            req_sent: None,
            response_header: None,
            response_body_reader: None,
            response_trailers: None,
            response_body_error: false,
            response_initial_end_stream: false,
            read_timeout: None,
            write_timeout: None,
            conn,
            ended: false,
            body_recv: 0,
            response_body_eof: false,
            response_body_declared_len: None,
            stream_id: None,
            peer_end_stream: PeerEndStream::default(),
        }
    }

    /// Whether the response body is provably complete, from ANY source the peer
    /// cannot retract: END_STREAM on HEADERS, `is_end_stream()` observed at a
    /// body poll (both latched into `response_body_eof`), or a declared,
    /// non-zero `content-length` whose bytes have all been received.
    ///
    /// This is the guard on treating a stream end as a clean end of body rather
    /// than as a truncated response.
    ///
    /// Deliberately does NOT consult the wire-level END_STREAM record (source
    /// (iv), [`Self::peer_end_stream`]): that record is set as the bytes pass
    /// into `h2`, which can be BEFORE `h2` has decoded them and before we have
    /// read them, so anything that gates *whether to keep reading* must not use
    /// it. Only [`Self::response_body_complete_at_stream_end`] may.
    fn response_body_complete(&self) -> bool {
        self.response_body_eof
            || self
                .response_body_declared_len
                .is_some_and(|len| self.body_recv >= len)
    }

    /// Whether the response body is provably complete, given that a read has
    /// ALREADY failed with a stream-end error.
    ///
    /// This adds source (iv) to [`Self::response_body_complete`]: the peer's
    /// END_STREAM flag, seen on the wire by [`super::end_stream_watch`] before
    /// anything tore the stream down. It is the only source that survives the
    /// RFC 9113 §8.1 shape (complete response, then RST_STREAM(NO_ERROR),
    /// while this side is still uploading a request body), because `h2`
    /// overwrites the stream state on that reset and its public API then
    /// reports a complete and a truncated response identically.
    ///
    /// # Why a truncated body cannot pass
    ///
    /// The caller has an error in hand, which pins down three facts:
    ///
    /// 1. `h2` returns an error from `poll_data`/`poll_trailers` only after the
    ///    stream's receive queue is EMPTY (`Recv::poll_data` errors from
    ///    `schedule_recv`, which is reached only when `pending_recv.pop_front()`
    ///    yields `None`). So every DATA frame `h2` QUEUED has already been
    ///    handed to us and counted in `body_recv`.
    /// 2. That error was caused by a teardown frame -- RST_STREAM or GOAWAY --
    ///    that `h2` processed. `h2` processes frames in wire order.
    /// 3. The watch also sees frames in wire order, and freezes a stream's
    ///    record when it sees that stream's RST_STREAM (or a GOAWAY that
    ///    excludes it). So a set flag means END_STREAM appeared STRICTLY BEFORE
    ///    the teardown frame that produced the error, and the byte count the
    ///    record carries is the count as of that END_STREAM.
    ///
    /// (2) and (3) put the END_STREAM-bearing frame ahead of a frame `h2` has
    /// already processed, so `h2` decoded every DATA frame of the body.
    ///
    /// Decoded is not delivered, and that gap is the whole difficulty:
    /// `Recv::recv_data` has several paths that decode a frame and then throw
    /// the payload away instead of pushing it onto `pending_recv` -- a
    /// `content-length` overflow or underflow, a flow-control violation, and
    /// `is_ignoring_frame` on a stream `h2` itself reset. None of those is
    /// self-defeating: they all leave the state `Closed(Cause::Error(<local>))`
    /// with `is_pending_send` true whenever we are still uploading, which is
    /// the §8.1 shape BY CONSTRUCTION, and `State::recv_reset` then overwrites
    /// that local close with the peer's REMOTE `NO_ERROR`, which
    /// [`benign_post_eof_stream_end`] accepts.
    ///
    /// So the flag alone is not enough, and this predicate does not use it
    /// alone. The record vouches for a byte COUNT -- the DATA payload bytes the
    /// wire carried for this stream, padding excluded, exactly the quantity
    /// `h2` would have queued -- and source (iv) is accepted only when that
    /// count equals `body_recv`. Any frame `h2` decoded and dropped shows up
    /// immediately as wire count > bytes read, whichever internal path dropped
    /// it, and no future `h2` release can add a path this misses. (1) is what
    /// makes the comparison meaningful: at the moment of the error there is
    /// nothing left queued that a later read would still hand us.
    ///
    /// One drop path moves no DATA at all and so cannot be caught this way:
    /// `Recv::recv_trailers` rejects a TRAILERS frame whose stream still owes
    /// `content-length` bytes. The body IS short there, and the response's own
    /// framing says so, which is why `declared_len_satisfied` is required
    /// alongside -- source (iv) may extend the proof past a missing END_STREAM,
    /// never past a `content-length` the peer did not deliver.
    ///
    /// A genuinely truncated response is the contrapositive: either the peer
    /// never flagged END_STREAM before the reset (no flag), or it flagged it on
    /// a frame `h2` refused (counts disagree), or its `content-length` is
    /// unmet. The negative-direction tests
    /// (`h2_response_body_no_error_reset_before_eos_is_an_error`,
    /// `h2_watched_truncated_body_reset_while_uploading_is_an_error`,
    /// `h2_watched_content_length_underflow_reset_is_not_a_clean_eof`,
    /// `h2_watched_rejected_trailers_reset_is_not_a_clean_eof`,
    /// `h2_watched_flow_control_drop_reset_is_not_a_clean_eof`) exist to keep
    /// that from regressing quietly; the last two pin one half of the
    /// conjunction each.
    ///
    /// Independently of all of the above, a stream WE reset gives up source
    /// (iv) outright at the moment the reset is sent -- see
    /// [`Self::note_local_reset`].
    fn response_body_complete_at_stream_end(&self) -> bool {
        self.end_of_body_proof().holds()
    }

    /// Sample [`EndOfBodyProof`] before a reader takes `&mut self`.
    fn end_of_body_proof(&self) -> EndOfBodyProof {
        EndOfBodyProof {
            body_complete: self.response_body_complete(),
            body_recv: self.body_recv,
            declared_len_satisfied: self
                .response_body_declared_len
                .is_none_or(|len| self.body_recv >= len),
            peer_end_stream: self.peer_end_stream.clone(),
        }
    }

    /// A handle on source (iv) for this stream, sampled wherever `&mut self` is
    /// not available -- the request-body write half in particular, which runs
    /// concurrently with the response read half.
    ///
    /// Consulting it is subject to the same rule as
    /// [`Self::response_body_complete_at_stream_end`]: it proves nothing about
    /// what has been READ, only that the peer flagged the end of its response
    /// on the wire before tearing the stream down.
    pub fn peer_end_stream(&self) -> PeerEndStream {
        self.peer_end_stream.clone()
    }

    /// Record that THIS side reset the stream, and give up source (iv) for it.
    ///
    /// MUST be called by every site that EXPLICITLY resets this stream,
    /// including through a [`SendStream`] detached with
    /// [`Self::take_request_body_writer`].
    ///
    /// It deliberately does not cover the RST_STREAM(CANCEL) `h2` emits by
    /// itself when the last handle on an open stream is dropped
    /// (`proto::streams::streams::drop_stream_ref` -> `maybe_cancel`): that
    /// reset is not observable from here, and it is harmless because dropping
    /// the handles is also the end of reading -- the flag can no longer be
    /// consulted by anyone. Do not read the rule above as "no local reset can
    /// exist without this call"; read it as "no local reset may be followed by
    /// a read that trusts source (iv)".
    ///
    /// After a local reset `h2` DROPS the DATA frames it decodes
    /// (`Recv::recv_data`'s `is_ignoring_frame`), while a peer RST_STREAM
    /// arriving afterwards can still overwrite the local close
    /// (`State::recv_reset` when `is_pending_send`) and surface as a remote
    /// `NO_ERROR`. Source (iv) would then read as "the peer ended its response
    /// cleanly" for a body `h2` has been discarding -- a truncation laundered
    /// into a clean EOF, exactly what the guard exists to prevent.
    ///
    /// Invalidating the shared record makes that unreachable: a flag already
    /// set before the reset is still sound (the body was whole before we gave
    /// up on it), and one not yet set can no longer be set, because publication
    /// refuses an invalidated record under the same lock. What is left are
    /// sources (i)-(iii), which a local reset cannot corrupt.
    ///
    /// # Why this must run BEFORE the reset is sent
    ///
    /// The two are not interchangeable. Between a queued RST_STREAM and this
    /// call, the connection's read task can publish the peer's END_STREAM for
    /// the stream, which removes the pending entry AND sets the flag on the
    /// record every [`PeerEndStream`] clone already holds -- the session's own
    /// and the h2 pump's. Running afterwards would then find nothing to remove
    /// and have nothing to retract, so the local failure this side had already
    /// decided on would be overwritten by the peer's evidence. Every caller
    /// therefore invalidates first and resets second.
    ///
    /// The byte count source (iv) carries would catch the discarded frames on
    /// its own (they are counted on the wire and never delivered), so this is
    /// now belt and braces rather than the only line of defence -- but it is
    /// the cheaper and more obvious one, and it also covers the case where the
    /// reset happens between two whole frames and nothing is discarded at all.
    pub fn note_local_reset(&mut self) {
        if let (Some(watch), Some(id)) = (self.conn.end_stream_watch(), self.stream_id) {
            watch.invalidate(id, self.peer_end_stream.record());
        }
    }

    fn sanitize_request_header(req: &mut RequestHeader) -> Result<()> {
        req.set_version(http::Version::HTTP_2);
        if req.uri.authority().is_some() {
            return Ok(());
        }
        // use host header to populate :authority field
        let Some(authority) = req.headers.get(http::header::HOST).map(|v| v.as_bytes()) else {
            return Error::e_explain(InvalidHTTPHeader, "no authority header for h2");
        };
        let uri = http::uri::Builder::new()
            .scheme("https") // fixed for now
            .authority(authority)
            .path_and_query(req.uri.path_and_query().as_ref().unwrap().as_str())
            .build();
        match uri {
            Ok(uri) => {
                req.set_uri(uri);
                Ok(())
            }
            Err(_) => Error::e_explain(
                InvalidHTTPHeader,
                format!("invalid authority from host {authority:?}"),
            ),
        }
    }

    /// Write the request header to the server
    pub fn write_request_header(&mut self, mut req: Box<RequestHeader>, end: bool) -> Result<()> {
        if self.req_sent.is_some() {
            // cannot send again, TODO: warn
            return Ok(());
        }
        Self::sanitize_request_header(&mut req)?;
        let parts = req.as_owned_parts();
        let request = http::Request::from_parts(parts, ());
        // Hold the wire-level watch's registration lock across `send_request`.
        //
        // `send_request` allocates the stream id, queues the HEADERS frame AND
        // notifies the connection task before it returns, so on a
        // multi-threaded runtime the request can be flushed -- and a fast peer
        // (loopback, UDS) can answer with a complete response and a reset --
        // before the next statement here runs. Registering afterwards would
        // race that scan and lose the record whenever it lost: the flag would
        // stay `false` forever and RFC 9113 §8.1's reset would go back to being
        // a failed request, nondeterministically and precisely on the fastest
        // upstreams. Taking the lock FIRST makes the scan wait for the entry
        // instead. `send_request` never touches the watch, so nothing here can
        // deadlock on it.
        let watch = self.conn.end_stream_watch().cloned();
        let registration = watch.as_ref().map(|watch| watch.registration());
        // There is no write timeout for h2 because the actual write happens async from this fn
        let (resp_fut, send_body) = self
            .send_req
            .send_request(request, end)
            .or_err(H2Error, "while sending request")
            .map_err(|e| self.handle_err(e))?;
        let stream_id = u32::from(send_body.stream_id());
        self.stream_id = Some(stream_id);
        self.peer_end_stream =
            PeerEndStream(registration.map(|registration| registration.register(stream_id)));
        self.req_sent = Some(req);
        self.send_body = Some(send_body);
        self.resp_fut = Some(resp_fut);
        self.ended = self.ended || end;

        Ok(())
    }

    /// Write a request body chunk
    ///
    /// A peer that sends RFC 9113 §8.1's RST_STREAM(NO_ERROR) while this is in
    /// flight makes the write fail. That is deliberately *not* softened into a
    /// graceful "stop uploading" here -- see the TODO in [`Self::read_trailers`]
    /// for why the reason behind a failed h2 write cannot be established from
    /// the write half alone. A caller that must decide whether such a failure
    /// should cost the exchange has [`Self::peer_end_stream`] for it: the wire
    /// flag says whether the peer had already flagged the end of its response,
    /// which is the fact the write site itself cannot see.
    pub async fn write_request_body(&mut self, data: Bytes, end: bool) -> Result<()> {
        if self.ended {
            warn!("Try to write request body after end of stream, dropping the extra data");
            return Ok(());
        }

        let body_writer = self
            .send_body
            .as_mut()
            .expect("Try to write request body before sending request header");

        super::write_body(body_writer, data, end, self.write_timeout)
            .await
            .map_err(|e| self.handle_err(e))?;
        self.ended = self.ended || end;
        Ok(())
    }

    /// Signal that the request body has ended
    pub fn finish_request_body(&mut self) -> Result<()> {
        if self.ended {
            return Ok(());
        }

        let body_writer = self
            .send_body
            .as_mut()
            .expect("Try to finish request stream before sending request header");

        // Just send an empty data frame with end of stream set
        body_writer
            .send_data("".into(), true)
            .or_err(WriteError, "while writing empty h2 request body")
            .map_err(|e| self.handle_err(e))?;
        self.ended = true;
        Ok(())
    }

    /// Read the response header
    pub async fn read_response_header(&mut self) -> Result<()> {
        // TODO: how to read 1xx headers?
        // https://github.com/hyperium/h2/issues/167

        if self.response_header.is_some() {
            panic!("H2 response header is already read")
        }

        let read_timeout = self.read_timeout;
        let res = match read_timeout {
            Some(t) => timeout(t, std::future::poll_fn(|cx| self.poll_response_header(cx)))
                .await
                .map_err(|_| Error::explain(ReadTimedout, "while reading h2 response header"))
                .map_err(|e| self.handle_err(e))?,
            None => std::future::poll_fn(|cx| self.poll_response_header(cx)).await,
        };
        let (resp, body_reader) = res.map_err(handle_read_header_error)?.into_parts();
        self.response_body_declared_len = super::server::declared_body_length(&resp.headers);
        let response_header = ResponseHeader::from(resp);
        validate_response_header(&response_header)?;
        self.response_initial_end_stream = body_reader.is_end_stream();
        self.response_body_eof = self.response_initial_end_stream;
        self.response_header = Some(response_header);
        self.response_body_reader = Some(body_reader);

        Ok(())
    }

    #[doc(hidden)]
    pub fn poll_read_response_header(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), h2::Error>> {
        let res = match ready!(self.poll_response_header(cx)) {
            Ok(res) => res,
            Err(err) => return Poll::Ready(Err(err)),
        };

        let (resp, body_reader) = res.into_parts();
        self.response_body_declared_len = super::server::declared_body_length(&resp.headers);
        let response_header = ResponseHeader::from(resp);
        if let Err(e) = validate_response_header(&response_header) {
            warn!("invalid h2 response header: {e}");
            return Poll::Ready(Err(Reason::PROTOCOL_ERROR.into()));
        }

        self.response_initial_end_stream = body_reader.is_end_stream();
        self.response_body_eof = self.response_initial_end_stream;
        self.response_header = Some(response_header);
        self.response_body_reader = Some(body_reader);

        Poll::Ready(Ok(()))
    }

    fn poll_response_header(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<http::Response<RecvStream>, h2::Error>> {
        if self.response_header.is_some() {
            panic!("H2 response header is already read")
        }

        let Some(mut resp_fut) = self.resp_fut.take() else {
            panic!("Try to take response header, but it is already taken")
        };

        let res = match resp_fut.poll_unpin(cx) {
            Poll::Ready(Ok(res)) => res,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => {
                self.resp_fut = Some(resp_fut);
                return Poll::Pending;
            }
        };

        Poll::Ready(Ok(res))
    }

    /// Read the response body
    ///
    /// `None` means, no more body to read
    pub async fn read_response_body(&mut self) -> Result<Option<Bytes>> {
        // Sampled before the mutable borrow of the reader below. Source (iv) is
        // carried as a handle rather than evaluated here: the peer's END_STREAM
        // may only be seen DURING this very read, so it has to be read at the
        // moment the read fails. See `response_body_complete_at_stream_end`.
        let proof = self.end_of_body_proof();
        let Some(body_reader) = self.response_body_reader.as_mut() else {
            // req is not sent or response is already read
            // TODO: warn
            return Ok(None);
        };

        let fut = body_reader.data();
        let res = match self.read_timeout {
            Some(t) => timeout(t, fut)
                .await
                .map_err(|_| Error::explain(ReadTimedout, "while reading h2 response body"))?,
            None => fut.await,
        };
        let body = match res.transpose() {
            Ok(body) => body,
            // Only reachable once `response_body_eof` is latched, i.e. the
            // response body is provably complete. `h2` still surfaces the
            // peer's RST_STREAM/GOAWAY here because a reset received after
            // END_STREAM overwrites the stream state, so the *next* read of an
            // already-finished body fails instead of reporting a clean EOF.
            // Reporting the end of body is what the peer's `NO_ERROR` means;
            // failing would cost the exchange a complete response it already
            // holds. See `benign_post_eof_stream_end` for why the guard is not
            // optional.
            Err(e) if proof.holds() && benign_post_eof_stream_end(&e) => {
                debug!("h2 stream ended with NO_ERROR after response body EOF: {e}");
                None
            }
            Err(e) => {
                // cannot use handle_err() because of borrow checker
                let mut e = Error::because(ReadError, "while read h2 response body", e);
                if self.conn.ping_timedout() {
                    e.etype = PING_TIMEDOUT;
                }
                return Err(e);
            }
        };

        if let Some(data) = body.as_ref() {
            body_reader
                .flow_control()
                .release_capacity(data.len())
                .or_err(ReadError, "while releasing h2 response body capacity")?;
            self.body_recv = self.body_recv.saturating_add(data.len());
        } else {
            // `data()` yielding `None` can mean trailers are queued. Validate
            // them before publishing a clean EOF: a malformed trailer block
            // followed by RST_STREAM(NO_ERROR) must remain an error.
            let trailers = match self.read_timeout {
                Some(t) => match timeout(t, body_reader.trailers()).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Error::e_explain(
                            ReadTimedout,
                            "while validating h2 trailers at response EOF",
                        )
                    }
                },
                None => body_reader.trailers().await,
            };
            match trailers {
                Ok(trailers) => {
                    if trailers.as_ref().is_none_or(HeaderMap::is_empty)
                        && self.peer_end_stream.terminal_headers_observed()
                        && !self.response_initial_end_stream
                    {
                        self.response_body_error = true;
                        return Error::e_explain(
                            ReadError,
                            "h2 terminal trailers were not validated before stream reset",
                        );
                    }
                    self.response_trailers = Some(trailers);
                    self.response_body_eof = true;
                }
                Err(e) => {
                    self.response_body_error = true;
                    return Error::e_because(
                        ReadError,
                        "while validating h2 trailers at response EOF",
                        e,
                    );
                }
            }
        }

        Ok(body)
    }

    #[doc(hidden)]
    pub fn poll_read_response_body(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, h2::Error>>> {
        // Sampled before the reader borrows `self`, exactly as in
        // [`Self::read_response_body`]: this poll IS the read whose failure
        // licenses source (iv), so the record has to be readable at the moment
        // it fails.
        let proof = self.end_of_body_proof();
        let Some(body_reader) = self.response_body_reader.as_mut() else {
            // req is not sent or response is already read
            // TODO: warn
            return Poll::Ready(None);
        };

        let data = match ready!(body_reader.poll_data(cx)).transpose() {
            Ok(data) => data,
            // The same classification `read_response_body` applies, so that
            // this `#[doc(hidden)]` API cannot report a stream end that the
            // async API calls a clean end of body as a failure. The guard is
            // not optional: without the proven EOF this would turn a mid-body
            // reset -- a truncated response -- into a clean end of body.
            Err(err) if proof.holds() && benign_post_eof_stream_end(&err) => {
                debug!("h2 stream ended with NO_ERROR after response body EOF: {err}");
                self.response_body_eof = true;
                return Poll::Ready(None);
            }
            Err(err) => return Poll::Ready(Some(Err(err))),
        };

        if let Some(data) = data {
            body_reader.flow_control().release_capacity(data.len())?;
            // Counted exactly as in `read_response_body`. Without this the
            // `content-length` source and source (iv) -- which both compare
            // against `body_recv` -- would be dead on this API, and
            // `body_bytes_received()` would under-report for anyone driving the
            // session by poll.
            self.body_recv = self.body_recv.saturating_add(data.len());
            return Poll::Ready(Some(Ok(data)));
        }

        match ready!(body_reader.poll_trailers(cx)) {
            Ok(trailers) => {
                if trailers.as_ref().is_none_or(HeaderMap::is_empty)
                    && self.peer_end_stream.terminal_headers_observed()
                    && !self.response_initial_end_stream
                {
                    self.response_body_error = true;
                    return Poll::Ready(Some(Err(Reason::PROTOCOL_ERROR.into())));
                }
                self.response_trailers = Some(trailers);
                self.response_body_eof = true;
                Poll::Ready(None)
            }
            Err(err) => {
                // Keep the poll API's state machine aligned with
                // `read_response_body`: once trailer validation fails, a later
                // `read_trailers` call must not retry the consumed stream and
                // turn the response-body failure into a successful EOF.
                self.response_body_error = true;
                Poll::Ready(Some(Err(err)))
            }
        }
    }

    /// Whether the response has ended
    ///
    /// Reports the LATCHED transport fact so that a peer resetting a stream it
    /// already ended cannot flip this back to `false`; the live
    /// `is_end_stream()` is still consulted so that an END_STREAM which arrived
    /// without us polling is picked up immediately.
    ///
    /// Deliberately does NOT consult the `content-length` source of
    /// [`Self::response_body_complete`]: an H2 response may legally send
    /// TRAILERS after a complete, `content-length`-declared body, and this
    /// function's callers stop reading the response once it reports `true`.
    pub fn response_finished(&self) -> bool {
        // if response_body_reader doesn't exist, the response is not even read yet
        self.response_body_reader
            .as_ref()
            .is_some_and(|reader| self.response_body_eof || reader.is_end_stream())
    }

    /// Check whether stream finished with error.
    /// Like `response_finished`, but also attempts to poll the h2 stream for errors that may have
    /// caused the stream to terminate, and returns them as `H2Error`s.
    pub fn check_response_end_or_error(&mut self) -> Result<bool> {
        // Same latched fact as `response_finished()`, read before the mutable
        // borrow of the reader below.
        let ended = self.response_body_eof;
        let Some(reader) = self.response_body_reader.as_mut() else {
            // response is not even read
            return Ok(false);
        };

        if !ended && !reader.is_end_stream() {
            return Ok(false);
        }

        // https://github.com/hyperium/h2/issues/806
        // The fundamental issue is that h2::RecvStream may return `is_end_stream` true
        // when the stream was naturally closed via END_STREAM /OR/ if there was an error
        // while reading data frames that forced the closure.
        // The h2 API as-is makes it difficult to determine which situation is occurring.
        //
        // `poll_data` should be returning None after `is_end_stream`, if the stream
        // is truly expecting no more data to be sent.
        // https://docs.rs/h2/latest/h2/struct.RecvStream.html#method.is_end_stream
        // So poll the data once to check this condition. If an error is returned, that indicates
        // that the stream closed due to an error e.g. h2 protocol error.
        //
        // tokio::task::unconstrained because now_or_never may yield None when the future is ready
        match tokio::task::unconstrained(reader.data()).now_or_never() {
            Some(None) => Ok(true),
            Some(Some(Ok(_))) => Error::e_explain(H2Error, "unexpected data after end stream"),
            // The response body already ended (that is what `ended` records),
            // so a peer ending the stream benignly on top of it is not an
            // error: `h2` only surfaces it here because the reset overwrote the
            // stream state that would otherwise have reported a clean EOF.
            // NOTE: the wire-level END_STREAM record is deliberately NOT added
            // here. The arm above gates on `ended || reader.is_end_stream()`,
            // and when `ended` is false but `is_end_stream()` is true `h2`
            // always answers `data()` with `None`, never an error -- so this
            // arm is only ever reached with `ended` already true. Widening the
            // gate itself would be unsound: the record can be set before we
            // have drained the body, which would turn a still-buffered chunk
            // into "unexpected data after end stream".
            Some(Some(Err(e))) if ended && benign_post_eof_stream_end(&e) => {
                debug!("h2 stream ended benignly after response body EOF: {e}");
                Ok(true)
            }
            Some(Some(Err(e))) => Error::e_because(H2Error, "while checking end stream", e),
            None => {
                // RecvStream data() should be ready to poll after the stream ends,
                // this indicates an unexpected change in the h2 crate
                panic!("data() not ready after end stream")
            }
        }
    }

    /// Read the optional trailer headers
    pub async fn read_trailers(&mut self) -> Result<Option<HeaderMap>> {
        if self.response_body_error {
            return Error::e_explain(
                ReadError,
                "h2 response body previously failed trailer validation",
            );
        }
        if let Some(trailers) = self.response_trailers.take() {
            return Ok(trailers);
        }
        let Some(reader) = self.response_body_reader.as_mut() else {
            // response is not even read
            // TODO: warn
            return Ok(None);
        };
        let fut = reader.trailers();

        let res = match self.read_timeout {
            Some(t) => timeout(t, fut)
                .await
                .map_err(|_| Error::explain(ReadTimedout, "while reading h2 trailer"))
                .map_err(|e| self.handle_err(e))?,
            None => fut.await,
        };
        match res {
            Ok(t) => Ok(t),
            // GOAWAY with no error: this is graceful shutdown, continue as if no trailer
            // RESET_STREAM with no error: https://datatracker.ietf.org/doc/html/rfc9113#section-8.1:
            // this is to signal client to stop uploading request without breaking the response.
            //
            // The `response_body_eof` guard used to be an unwritten contract
            // on the caller (both in-tree callers only reach this after
            // `read_response_body()` returned `Ok(None)`, which is exactly the
            // proof the latch records). Making it explicit keeps a caller that
            // skips or aborts the body read from turning a broken stream into
            // "no trailers".
            //
            // TODO: should actually stop uploading. Do NOT "fix" this by
            // classifying the failure at the write site: `h2` reports a write
            // to a reset stream as `UserError::InactiveStreamId`, and
            // `SendStream::poll_capacity` as a bare `Ready(None)` -- neither
            // carries a reason or an initiator. The only reason source there,
            // `SendStream::poll_reset`, collapses `Closed(Cause::Error(Reset))`
            // and `Closed(Cause::Error(GoAway))` into one bare `Reason`, so
            // RFC 9113 §8.1's RST_STREAM(NO_ERROR) (response complete, safe to
            // stop uploading) is indistinguishable there from a
            // GOAWAY(NO_ERROR) whose `last_stream_id` excluded this stream
            // (never processed, MUST be retried, no response will ever come).
            // Stopping the upload is only sound once a complete response is in
            // hand, and pingora-proxy's h2 upload pump cannot see that: it
            // writes to a `SendStream` detached via
            // `take_request_body_writer()` while the response half is read
            // concurrently in the other arm of its `select!`.
            // TODO: should we try reading again?
            // https://github.com/hyperium/h2/issues/741 (still open, docs-only)
            Err(e)
                if self.response_body_complete_at_stream_end()
                    && benign_post_eof_stream_end(&e) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
        .or_err(ReadError, "while reading h2 trailers")
    }

    /// The request header if it is already sent
    pub fn request_header(&self) -> Option<&RequestHeader> {
        self.req_sent.as_deref()
    }

    /// The response header if it is already read
    pub fn response_header(&self) -> Option<&ResponseHeader> {
        self.response_header.as_ref()
    }

    /// Give up the http session abruptly.
    pub fn shutdown(&mut self) {
        if (!self.ended || !self.response_finished()) && self.send_body.is_some() {
            // A locally reset stream may no longer be judged by the wire flag,
            // and giving it up has to happen BEFORE the reset is queued -- see
            // `note_local_reset`.
            self.note_local_reset();
            if let Some(send_body) = self.send_body.as_mut() {
                send_body.send_reset(h2::Reason::INTERNAL_ERROR);
            }
        }
    }

    /// Drop everything in this h2 stream. Return the connection ref.
    /// After this function the underlying h2 connection should already notify the closure of this
    /// stream so that another stream can be created if needed.
    pub(crate) fn conn(&self) -> ConnectionRef {
        self.conn.clone()
    }

    /// Whether ping timeout occurred. After a ping timeout, the h2 connection will be terminated.
    /// Ongoing h2 streams will receive an stream/connection error. The streams should check this
    /// flag to tell whether the error is triggered by the timeout.
    pub(crate) fn ping_timedout(&self) -> bool {
        self.conn.ping_timedout()
    }

    /// Return the [Digest] of the connection
    ///
    /// For reused connection, the timing in the digest will reflect its initial handshakes
    /// The caller should check if the connection is reused to avoid misuse the timing field.
    pub fn digest(&self) -> Option<&Digest> {
        Some(self.conn.digest())
    }

    /// Return a mutable [Digest] reference for the connection
    ///
    /// Will return `None` if multiple H2 streams are open.
    pub fn digest_mut(&mut self) -> Option<&mut Digest> {
        self.conn.digest_mut()
    }

    /// Return the server (peer) address recorded in the connection digest.
    pub fn server_addr(&self) -> Option<&SocketAddr> {
        self.conn
            .digest()
            .socket_digest
            .as_ref()
            .map(|d| d.peer_addr())?
    }

    /// Return the client (local) address recorded in the connection digest.
    pub fn client_addr(&self) -> Option<&SocketAddr> {
        self.conn
            .digest()
            .socket_digest
            .as_ref()
            .map(|d| d.local_addr())?
    }

    /// the FD of the underlying connection
    pub fn fd(&self) -> UniqueIDType {
        self.conn.id()
    }

    /// Upstream response body bytes received (HTTP/2 DATA payload; excludes headers/framing).
    pub fn body_bytes_received(&self) -> usize {
        self.body_recv
    }

    /// take the body sender to another task to perform duplex read and write
    pub fn take_request_body_writer(&mut self) -> Option<SendStream<Bytes>> {
        self.send_body.take()
    }

    fn handle_err(&self, mut e: Box<Error>) -> Box<Error> {
        if self.ping_timedout() {
            e.etype = PING_TIMEDOUT;
        }

        // is_go_away: retry via another connection, this connection is being teardown
        // should retry
        if self.response_header.is_none() {
            if let Some(err) = e.root_cause().downcast_ref::<h2::Error>() {
                if err.is_go_away()
                    && err.is_remote()
                    && (err.reason() == Some(h2::Reason::NO_ERROR))
                {
                    e.retry = true.into();
                }
            }
        }
        e
    }
}

/* helper functions */

/* Types of errors during h2 header read
 1. peer requests to downgrade to h1, mostly IIS server for NTLM: we will downgrade and retry
 2. peer sends invalid h2 frames, usually sending h1 only header: we will downgrade and retry
 3. peer sends GO_AWAY(NO_ERROR) connection is being shut down: we will retry
 4. peer IO error on reused conn, usually firewall kills old conn: we will retry
 5. peer sends REFUSED_STREAM on RST_STREAM, this is safe to retry
 6. All other errors will terminate the request
*/
fn handle_read_header_error(e: h2::Error) -> Box<Error> {
    if e.is_remote() && (e.reason() == Some(h2::Reason::HTTP_1_1_REQUIRED)) {
        let mut err = Error::because(H2Downgrade, "while reading h2 header", e);
        err.retry = true.into();
        err
    } else if e.is_go_away() && e.is_library() && (e.reason() == Some(h2::Reason::PROTOCOL_ERROR)) {
        // remote send invalid H2 responses
        let mut err = Error::because(InvalidH2, "while reading h2 header", e);
        err.retry = true.into();
        err
    } else if e.is_go_away() && e.is_remote() && (e.reason() == Some(h2::Reason::NO_ERROR)) {
        // is_go_away: retry via another connection, this connection is being teardown
        let mut err = Error::because(H2Error, "while reading h2 header", e);
        err.retry = true.into();
        err
    } else if e.is_reset() && e.is_remote() && (e.reason() == Some(h2::Reason::REFUSED_STREAM)) {
        // The REFUSED_STREAM error code can be included in a RST_STREAM frame to indicate
        // that the stream is being closed prior to any processing having occurred.
        // Any request that was sent on the reset stream can be safely retried.
        // https://datatracker.ietf.org/doc/html/rfc9113#section-8.7
        let mut err = Error::because(H2Error, "while reading h2 header", e);
        err.retry = true.into();
        err
    } else if e.is_io() {
        // is_io: typical if a previously reused connection silently drops it
        // only retry if the connection is reused
        // safety: e.get_io() will always succeed if e.is_io() is true
        let io_err = e.get_io().expect("checked is io");

        // for h2 hyperium raw_os_error() will be None unless this is a new connection
        // where we handshake() and from_io() is called, check ErrorKind explicitly with true_io_error
        let true_io_error = io_err.raw_os_error().is_some()
            || matches!(
                io_err.kind(),
                ErrorKind::ConnectionReset | ErrorKind::TimedOut | ErrorKind::BrokenPipe
            );
        let mut err = Error::because(ReadError, "while reading h2 header", e);
        if true_io_error {
            err.retry = RetryType::ReusedOnly;
        } // else could be TLS error, which is unsafe to retry
        err
    } else {
        Error::because(H2Error, "while reading h2 header", e)
    }
}

use tokio::sync::oneshot;

pub async fn drive_connection<S>(
    mut c: client::Connection<S>,
    id: UniqueIDType,
    closed: watch::Sender<bool>,
    ping_interval: Option<Duration>,
    ping_timeout_occurred: Arc<AtomicBool>,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin,
{
    let interval = ping_interval.unwrap_or(Duration::ZERO);
    if !interval.is_zero() {
        // for ping to inform this fn to drop the connection
        let (tx, rx) = oneshot::channel::<()>();
        // for this fn to inform ping to give up when it is already dropped
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped2 = dropped.clone();

        if let Some(ping_pong) = c.ping_pong() {
            pingora_runtime::current_handle().spawn(async move {
                do_ping_pong(ping_pong, interval, tx, dropped2, id).await;
            });
        } else {
            warn!("Cannot get ping-pong handler from h2 connection");
        }

        tokio::select! {
            r = c => match r {
                Ok(_) => debug!("H2 connection finished fd: {id}"),
                Err(e) => debug!("H2 connection fd: {id} errored: {e:?}"),
            },
            r = rx => match r {
                Ok(_) => {
                    ping_timeout_occurred.store(true, Ordering::Relaxed);
                    warn!("H2 connection Ping timeout/Error fd: {id}, closing conn");
                },
                Err(e) => warn!("H2 connection Ping Rx error {e:?}"),
            },
        };

        dropped.store(true, Ordering::Relaxed);
    } else {
        match c.await {
            Ok(_) => debug!("H2 connection finished fd: {id}"),
            Err(e) => debug!("H2 connection fd: {id} errored: {e:?}"),
        }
    }
    let _ = closed.send(true);
}

const PING_TIMEOUT: Duration = Duration::from_secs(5);

async fn do_ping_pong(
    mut ping_pong: h2::PingPong,
    interval: Duration,
    tx: oneshot::Sender<()>,
    dropped: Arc<AtomicBool>,
    id: UniqueIDType,
) {
    // delay before sending the first ping, no need to race with the first request
    tokio::time::sleep(interval).await;
    loop {
        if dropped.load(Ordering::Relaxed) {
            break;
        }
        let ping_fut = ping_pong.ping(h2::Ping::opaque());
        debug!("H2 fd: {id} ping sent");
        match tokio::time::timeout(PING_TIMEOUT, ping_fut).await {
            Err(_) => {
                error!("H2 fd: {id} ping timeout");
                let _ = tx.send(());
                break;
            }
            Ok(r) => match r {
                Ok(_) => {
                    debug!("H2 fd: {} pong received", id);
                    tokio::time::sleep(interval).await;
                }
                Err(e) => {
                    if dropped.load(Ordering::Relaxed) {
                        // drive_connection() exits first, no need to error again
                        break;
                    }
                    error!("H2 fd: {id} ping error: {e}");
                    let _ = tx.send(());
                    break;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests_h2 {
    use super::*;
    use bytes::Bytes;
    use http::{Response, StatusCode};
    use tokio::io::duplex;
    use tokio::sync::oneshot;

    async fn session_with_delayed_response() -> (
        Http2Session,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (client_io, server_io) = duplex(65536);
        let (request_accepted_tx, request_accepted_rx) = oneshot::channel();
        let (release_response_tx, release_response_rx) = oneshot::channel();

        let server_task = tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_io).await.unwrap();
            if let Some(result) = conn.accept().await {
                let (req, mut send_resp) = result.unwrap();
                assert_eq!(req.method(), http::Method::GET);
                let _ = request_accepted_tx.send(());
                let _ = release_response_rx.await;

                let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
                send_resp.send_response(resp, true).unwrap();
                conn.graceful_shutdown();
            }
            while let Some(_result) = conn.accept().await {}
        });

        let (send_req, connection) = h2::client::handshake(client_io).await.unwrap();
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        let ping_timeout = Arc::new(AtomicBool::new(false));
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
            let _ = closed_tx.send(true);
        });

        let conn_ref = crate::connectors::http::v2::ConnectionRef::new(
            send_req.clone(),
            closed_rx,
            ping_timeout,
            0,
            1,
            Digest::default(),
        );
        let mut h2s = Http2Session::new(send_req, conn_ref);
        let mut req = RequestHeader::build("GET", b"/", None).unwrap();
        req.insert_header(http::header::HOST, "example.com")
            .unwrap();
        h2s.write_request_header(Box::new(req), true).unwrap();

        request_accepted_rx
            .await
            .expect("server should accept the request before the response-header read");

        (h2s, release_response_tx, server_task, connection_task)
    }

    #[tokio::test]
    async fn response_header_read_can_resume_after_read_timeout() {
        let (mut h2s, release_response, server_task, connection_task) =
            session_with_delayed_response().await;
        h2s.read_timeout = Some(Duration::from_millis(1));

        let err = h2s
            .read_response_header()
            .await
            .expect_err("delayed response header should hit the read timeout");
        assert!(
            matches!(err.etype, ReadTimedout),
            "unexpected first read error: {err:?}"
        );
        assert!(h2s.response_header().is_none());
        assert!(
            h2s.resp_fut.is_some(),
            "timing out must not drop the pending response future"
        );

        h2s.read_timeout = None;
        release_response.send(()).unwrap();
        h2s.read_response_header().await.unwrap();
        assert_eq!(h2s.response_header().unwrap().status, StatusCode::OK);

        server_task.abort();
        connection_task.abort();
    }

    #[tokio::test]
    async fn response_header_read_can_resume_after_external_cancellation() {
        let (mut h2s, release_response, server_task, connection_task) =
            session_with_delayed_response().await;

        let first_read =
            tokio::time::timeout(Duration::from_millis(1), h2s.read_response_header()).await;
        assert!(
            first_read.is_err(),
            "external timeout should cancel the pending header read"
        );
        assert!(h2s.response_header().is_none());
        assert!(
            h2s.resp_fut.is_some(),
            "cancelling the read must not drop the pending response future"
        );

        release_response.send(()).unwrap();
        h2s.read_response_header().await.unwrap();
        assert_eq!(h2s.response_header().unwrap().status, StatusCode::OK);

        server_task.abort();
        connection_task.abort();
    }

    #[tokio::test]
    async fn h2_body_bytes_received_multi_frames() {
        let (client_io, server_io) = duplex(65536);

        // Server: respond with two DATA frames "a" and "bc"
        tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_io).await.unwrap();
            if let Some(result) = conn.accept().await {
                let (req, mut send_resp) = result.unwrap();
                assert_eq!(req.method(), http::Method::GET);
                let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
                let mut send_stream = send_resp.send_response(resp, false).unwrap();
                send_stream.send_data(Bytes::from("a"), false).unwrap();
                send_stream.send_data(Bytes::from("bc"), true).unwrap();
                // Signal graceful shutdown so the accept loop can exit after the client finishes
                conn.graceful_shutdown();
            }
            // Drive the server connection until the client closes
            while let Some(_res) = conn.accept().await {}
        });

        // Client: build Http2Session and read response
        let (send_req, connection) = h2::client::handshake(client_io).await.unwrap();
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        let ping_timeout = Arc::new(AtomicBool::new(false));
        tokio::spawn(async move {
            let _ = connection.await;
            let _ = closed_tx.send(true);
        });

        let digest = Digest::default();
        let conn_ref = crate::connectors::http::v2::ConnectionRef::new(
            send_req.clone(),
            closed_rx,
            ping_timeout,
            0,
            1,
            digest,
        );
        let mut h2s = Http2Session::new(send_req, conn_ref);

        // minimal request
        let mut req = RequestHeader::build("GET", b"/", None).unwrap();
        req.insert_header(http::header::HOST, "example.com")
            .unwrap();
        h2s.write_request_header(Box::new(req), true).unwrap();
        h2s.read_response_header().await.unwrap();

        let mut total = 0;
        while let Some(chunk) = h2s.read_response_body().await.unwrap() {
            total += chunk.len();
        }
        assert_eq!(total, 3);
        assert_eq!(h2s.body_bytes_received(), 3);
    }

    #[test]
    fn h2_response_conflicting_content_length_rejected() {
        let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
        response
            .append_header(http::header::CONTENT_LENGTH, "5")
            .unwrap();
        response
            .append_header(http::header::CONTENT_LENGTH, "6")
            .unwrap();

        let err = validate_response_header(&response).unwrap_err();
        assert_eq!(err.etype(), &InvalidHTTPHeader);
    }

    #[test]
    fn h2_response_duplicate_identical_content_length_accepted() {
        // RFC 9110 section 8.6 / hyper: identical duplicate (or comma-combined
        // identical) Content-Length values are reconciled to a single value.
        let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
        response
            .append_header(http::header::CONTENT_LENGTH, "5")
            .unwrap();
        response
            .append_header(http::header::CONTENT_LENGTH, "5")
            .unwrap();
        assert!(validate_response_header(&response).is_ok());

        let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
        response
            .append_header(http::header::CONTENT_LENGTH, "5, 5")
            .unwrap();
        assert!(validate_response_header(&response).is_ok());
    }

    /// Build an [`Http2Session`] over `client_io`, wired the way
    /// [`crate::connectors::http::v2::handshake`] wires production sessions:
    /// with the wire-level END_STREAM watch in front of the socket.
    async fn watched_client_session(client_io: tokio::io::DuplexStream) -> Http2Session {
        watched_client_session_with(client_io, |b| b).await
    }

    /// [`watched_client_session`] with the `h2` client settings tuned, for the
    /// tests that need a receive window the peer can overrun.
    async fn watched_client_session_with(
        client_io: tokio::io::DuplexStream,
        tune: impl FnOnce(&mut client::Builder) -> &mut client::Builder,
    ) -> Http2Session {
        use super::super::end_stream_watch::{EndStreamWatch, EndStreamWatchStream};

        let watch = EndStreamWatch::new();
        let io = EndStreamWatchStream::new(client_io, watch.clone());
        let mut builder = client::Builder::new();
        let (send_req, connection) = tune(&mut builder).handshake(io).await.unwrap();
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        let ping_timeout = Arc::new(AtomicBool::new(false));
        tokio::spawn(async move {
            let _ = connection.await;
            let _ = closed_tx.send(true);
        });
        let conn_ref = crate::connectors::http::v2::ConnectionRef::new_with_end_stream_watch(
            send_req.clone(),
            closed_rx,
            ping_timeout,
            0,
            1,
            Digest::default(),
            Some(watch),
        );
        Http2Session::new(send_req, conn_ref)
    }

    /// Build an [`Http2Session`] over `client_io` and start its connection task.
    ///
    /// Deliberately WITHOUT the END_STREAM watch, so that the tests below keep
    /// exercising the end-of-body proofs they were written for (END_STREAM
    /// latched at a poll, and the declared `content-length`) rather than
    /// silently passing through source (iv).
    async fn client_session(client_io: tokio::io::DuplexStream) -> Http2Session {
        unwatched_client_session_with(client_io, |b| b).await
    }

    /// [`client_session`] with the `h2` client settings tuned. This is the
    /// baseline counterpart of [`watched_client_session_with`].
    async fn unwatched_client_session_with(
        client_io: tokio::io::DuplexStream,
        tune: impl FnOnce(&mut client::Builder) -> &mut client::Builder,
    ) -> Http2Session {
        let mut builder = client::Builder::new();
        let (send_req, connection) = tune(&mut builder).handshake(client_io).await.unwrap();
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        let ping_timeout = Arc::new(AtomicBool::new(false));
        tokio::spawn(async move {
            let _ = connection.await;
            let _ = closed_tx.send(true);
        });
        let conn_ref = crate::connectors::http::v2::ConnectionRef::new(
            send_req.clone(),
            closed_rx,
            ping_timeout,
            0,
            1,
            Digest::default(),
        );
        Http2Session::new(send_req, conn_ref)
    }

    /// Send request HEADERS *without* END_STREAM and read the response header.
    ///
    /// Leaving the request stream open is what makes these tests reproduce the
    /// RFC 9113 §8.1 shape at all: once this side has sent END_STREAM, an
    /// inbound RST_STREAM finds the stream in `Closed(Cause::EndStream)` and h2
    /// leaves the state alone, so no read ever fails.
    async fn send_open_request(h2s: &mut Http2Session) {
        let mut req = RequestHeader::build("POST", b"/", None).unwrap();
        req.insert_header(http::header::HOST, "example.com")
            .unwrap();
        h2s.write_request_header(Box::new(req), false).unwrap();
        h2s.read_response_header().await.unwrap();
    }

    /// Give the connection task a chance to process a peer reset.
    ///
    /// Newer `h2` releases may preserve an already-observed END_STREAM instead
    /// of overwriting it with a later reset, so the raw receive state is not a
    /// portable acknowledgement. The behavioral assertion after this grace
    /// period is the contract that matters: the completed response stays a
    /// clean EOF under either internal representation.
    async fn await_reset_processed(_h2s: &Http2Session) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // NOTE: there is deliberately no test here that races `write_request_header`
    // against a fast peer's response. One existed, and it was theatre: the gap
    // it claimed to cover is the handful of nanoseconds between `send_request`
    // returning and the next statement, against a round trip that takes
    // microseconds even over an in-memory duplex, so it passed 300/300 runs with
    // the registration lock removed. The invariant it was meant to protect --
    // "a scan that runs while a registration is being taken must block, not drop
    // the record" -- is pinned deterministically instead, by
    // `end_stream_watch::tests::a_scan_waits_for_an_in_progress_registration`.

    /// A complete response followed by RFC 9113 §8.1's RST_STREAM(NO_ERROR)
    /// -- the upstream asking us to stop uploading a request it no longer
    /// needs -- must read as a clean end of body, not as a `ReadError`. The
    /// response is already in hand; failing it would discard a complete
    /// response over a protocol-sanctioned signal.
    #[tokio::test]
    async fn h2_response_body_no_error_reset_after_eos_is_end_of_body() {
        let (client_io, server_io) = duplex(65536);
        let (reset_tx, reset_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_io).await.unwrap();
            let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
            let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
            let mut send_stream = send_resp.send_response(resp, false).unwrap();
            // A complete response body, END_STREAM included.
            send_stream.send_data(Bytes::from("hello"), true).unwrap();

            // Drive the connection until the client has the whole body, so the
            // reset is unambiguously post-EOF.
            tokio::select! {
                _ = async { while conn.accept().await.is_some() {} } => {}
                _ = reset_rx => {}
            }
            send_stream.send_reset(Reason::NO_ERROR);
            while conn.accept().await.is_some() {}
        });

        let mut h2s = client_session(client_io).await;
        send_open_request(&mut h2s).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert!(h2s.response_finished());

        reset_tx.send(()).unwrap();
        await_reset_processed(&h2s).await;

        assert!(
            h2s.read_response_body()
                .await
                .expect("a post-EOF NO_ERROR reset must not surface as a read error")
                .is_none(),
            "the response body already ended"
        );
        assert_eq!(h2s.body_bytes_received(), 5);
    }

    /// The same shape in the NATURAL wire ordering: the upstream flushes the
    /// complete response AND the RST_STREAM before this side polls the body at
    /// all. No oneshot forces the reset to happen after the reader observed EOF,
    /// which is what makes this the case real traffic produces -- and the case
    /// in which `h2` has already overwritten the stream state, so END_STREAM can
    /// never be latched and only the declared `content-length` still proves the
    /// body whole.
    ///
    /// The sleep before the reset is required, not cosmetic: `h2`'s
    /// `send_reset` CLEARS the stream's pending send queue, so resetting
    /// immediately would drop the DATA frame this test is about.
    #[tokio::test]
    async fn h2_response_body_reset_before_any_read_is_end_of_body() {
        let (client_io, server_io) = duplex(65536);

        tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_io).await.unwrap();
            let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
            let resp = Response::builder()
                .status(StatusCode::OK)
                .header("content-length", "5")
                .body(())
                .unwrap();
            let mut send_stream = send_resp.send_response(resp, false).unwrap();
            send_stream.send_data(Bytes::from("hello"), true).unwrap();

            // Drive the connection long enough to flush the DATA frame, then
            // reset without waiting for the peer to have read anything.
            tokio::select! {
                _ = async { while conn.accept().await.is_some() {} } => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
            send_stream.send_reset(Reason::NO_ERROR);
            while conn.accept().await.is_some() {}
        });

        let mut h2s = client_session(client_io).await;
        send_open_request(&mut h2s).await;
        // Nothing is read until both frames have been processed.
        tokio::time::sleep(Duration::from_millis(600)).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert!(
            h2s.read_response_body()
                .await
                .expect(
                    "a fully received content-length body must read as a clean EOF even when \
                     the peer reset before we polled"
                )
                .is_none(),
            "the response body already ended"
        );
        assert_eq!(h2s.body_bytes_received(), 5);
    }

    /// The mirror image: the same RST_STREAM(NO_ERROR) arriving *before*
    /// END_STREAM means the response body was truncated. Reporting it as a
    /// clean end of body would hand the truncation to the downstream client,
    /// so it must stay an error.
    #[tokio::test]
    async fn h2_response_body_no_error_reset_before_eos_is_an_error() {
        let (client_io, server_io) = duplex(65536);
        let (reset_tx, reset_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_io).await.unwrap();
            let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
            let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
            let mut send_stream = send_resp.send_response(resp, false).unwrap();
            // A partial body: no END_STREAM, then the stream is reset.
            send_stream.send_data(Bytes::from("hel"), false).unwrap();

            // Reset only once the client holds the partial body, so the test
            // exercises the body read rather than the header read.
            tokio::select! {
                _ = async { while conn.accept().await.is_some() {} } => {}
                _ = reset_rx => {}
            }
            send_stream.send_reset(Reason::NO_ERROR);
            while conn.accept().await.is_some() {}
        });

        let mut h2s = client_session(client_io).await;
        send_open_request(&mut h2s).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hel");
        assert!(!h2s.response_finished());
        reset_tx.send(()).unwrap();

        // Blocks until the reset lands, so no sleep is needed to make this
        // deterministic: the receive half is still open until then.
        let err = h2s
            .read_response_body()
            .await
            .expect_err("a truncated response body must not read as a clean EOF");
        assert_eq!(err.etype(), &ReadError);
    }

    /// The pre-existing trailer classification keeps working now that it is
    /// guarded by the same end-of-body proof: the guard must not be so strict
    /// that it re-breaks the case it was written for.
    #[tokio::test]
    async fn h2_trailers_no_error_reset_after_eos_is_benign() {
        let (client_io, server_io) = duplex(65536);
        let (reset_tx, reset_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_io).await.unwrap();
            let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
            let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
            let mut send_stream = send_resp.send_response(resp, false).unwrap();
            send_stream.send_data(Bytes::from("hello"), true).unwrap();

            tokio::select! {
                _ = async { while conn.accept().await.is_some() {} } => {}
                _ = reset_rx => {}
            }
            send_stream.send_reset(Reason::NO_ERROR);
            while conn.accept().await.is_some() {}
        });

        let mut h2s = client_session(client_io).await;
        send_open_request(&mut h2s).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        // Reach the end of body before the reset lands, which is the ordering
        // the trailer read has always relied on.
        assert!(h2s.read_response_body().await.unwrap().is_none());

        reset_tx.send(()).unwrap();
        await_reset_processed(&h2s).await;

        assert!(h2s
            .read_trailers()
            .await
            .expect("a post-EOF NO_ERROR reset must not surface as a trailer read error")
            .is_none());
    }

    /// Serve RFC 9113 §8.1's shape: a response with NO `content-length` whose
    /// body either does or does not carry END_STREAM, followed by
    /// RST_STREAM(NO_ERROR) once those frames have flushed.
    ///
    /// The sleep before the reset is required, not cosmetic: `h2`'s
    /// `send_reset` CLEARS the stream's pending send queue, so resetting
    /// immediately would drop the DATA frame these tests are about.
    fn serve_body_then_no_error_reset(
        server_io: tokio::io::DuplexStream,
        body: &'static str,
        end_stream: bool,
    ) {
        tokio::spawn(async move {
            let mut conn = h2::server::handshake(server_io).await.unwrap();
            let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
            let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
            let mut send_stream = send_resp.send_response(resp, false).unwrap();
            send_stream
                .send_data(Bytes::from(body), end_stream)
                .unwrap();

            tokio::select! {
                _ = async { while conn.accept().await.is_some() {} } => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
            send_stream.send_reset(Reason::NO_ERROR);
            while conn.accept().await.is_some() {}
        });
    }

    /// THE defect this watch exists for: an upstream that sends a complete
    /// response with no `content-length` and then RST_STREAM(NO_ERROR) to stop
    /// an upload it no longer needs. `h2` overwrites the stream state on that
    /// reset -- the local half is still `HalfClosedRemote(Streaming)` because
    /// the request body is still going out -- so every one of `h2`'s own
    /// end-of-body proofs is destroyed. Only the END_STREAM flag seen on the
    /// wire survives, and the complete response must be delivered.
    #[tokio::test]
    async fn h2_watched_complete_body_reset_while_uploading_is_end_of_body() {
        let (client_io, server_io) = duplex(65536);
        serve_body_then_no_error_reset(server_io, "hello", true);

        let mut h2s = watched_client_session(client_io).await;
        send_open_request(&mut h2s).await;
        // Nothing is read until both frames have been processed: the natural
        // wire ordering, and the one that destroys the evidence.
        tokio::time::sleep(Duration::from_millis(600)).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");

        // No declared length can prove completion; either the wire watch or an
        // `h2` release that preserves the already-observed END_STREAM must do so.
        assert!(
            h2s.response_body_declared_len.is_none(),
            "the response must not declare a content-length"
        );

        assert!(
            h2s.read_response_body()
                .await
                .expect(
                    "a complete response ended with RST_STREAM(NO_ERROR) while the request was \
                     still uploading must read as a clean EOF"
                )
                .is_none(),
            "the response body already ended"
        );
        assert_eq!(h2s.body_bytes_received(), 5);

        // The trailer read that follows a finished body must not undo it.
        assert!(h2s
            .read_trailers()
            .await
            .expect("the trailer read must not resurrect the reset as an error")
            .is_none());
    }

    /// The direction that matters: the SAME shape with the body truncated --
    /// the peer never flagged END_STREAM before resetting -- must still be an
    /// error. Handing this to the downstream client as a complete response is
    /// precisely what the guard exists to prevent, and the wire watch must not
    /// weaken it.
    #[tokio::test]
    async fn h2_watched_truncated_body_reset_while_uploading_is_an_error() {
        let (client_io, server_io) = duplex(65536);
        serve_body_then_no_error_reset(server_io, "hel", false);

        let mut h2s = watched_client_session(client_io).await;
        send_open_request(&mut h2s).await;
        tokio::time::sleep(Duration::from_millis(600)).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hel");
        assert!(!h2s.response_finished());

        let err = h2s
            .read_response_body()
            .await
            .expect_err("a truncated response body must not read as a clean EOF");
        assert_eq!(err.etype(), &ReadError);

        // And the trailer read must not launder it into "no trailers" either.
        h2s.read_trailers()
            .await
            .expect_err("a truncated response body must not read as a clean trailer EOF");
    }

    /// A raw HTTP/2 origin, for the shapes `h2`'s own server API refuses to
    /// produce (a `content-length` its body does not honour) and for the ONE
    /// wire timing that makes them dangerous.
    ///
    /// The timing is the whole trick and is not incidental: the response frames
    /// and the RST_STREAM have to reach `h2` in a SINGLE burst. `h2`'s
    /// connection loop flushes its send queue between frames
    /// (`Connection::poll2` calls `poll_ready` before every `recv_frame`), so a
    /// reset that arrives even one poll later finds the RST_STREAM `h2` queued
    /// for its own PROTOCOL_ERROR already gone, `is_pending_send` false, and
    /// `State::recv_reset` takes its `Closed(..) if !queued` no-op arm -- the
    /// error stays LOCAL and nothing can launder it. Arriving in the same burst
    /// it finds the queue non-empty and OVERWRITES the local close with a
    /// REMOTE `NO_ERROR`, which is exactly what
    /// [`benign_post_eof_stream_end`] accepts.
    fn serve_raw_burst_then_no_error_reset(
        mut server_io: tokio::io::DuplexStream,
        response_frames: Vec<u8>,
    ) -> tokio::sync::oneshot::Receiver<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (flushed_tx, flushed_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            // Empty SETTINGS, then the ACK of whatever the client sent.
            server_io
                .write_all(&raw_frame(0x4, 0, 0, b""))
                .await
                .unwrap();
            server_io
                .write_all(&raw_frame(0x4, 0x1, 0, b""))
                .await
                .unwrap();
            // Wait for a complete request HEADERS frame on stream 1 before
            // answering. Do not search arbitrary byte windows: SETTINGS
            // payloads may contain the HEADERS frame-type byte by chance.
            let mut seen = Vec::new();
            loop {
                let Ok(n) = server_io.read(&mut buf).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                seen.extend_from_slice(&buf[..n]);
                if contains_request_headers_on_stream_one(&seen) {
                    break;
                }
            }
            let mut burst = response_frames;
            burst.extend_from_slice(&raw_frame(0x3, 0, 1, &[0, 0, 0, 0]));
            server_io.write_all(&burst).await.unwrap();
            server_io.flush().await.unwrap();
            let _ = flushed_tx.send(());
            while server_io.read(&mut buf).await.unwrap_or(0) != 0 {}
        });
        flushed_rx
    }

    #[derive(Clone, Copy, Debug)]
    enum EndStreamObservation {
        Watched,
        Unwatched,
    }

    /// Open a request against a hand-written response whose complete frame
    /// sequence and following NO_ERROR reset are processed before the first
    /// body poll. This is the ordering that used to let h2's reset state hide
    /// the terminal-frame result.
    async fn raw_reset_session(
        response_frames: Vec<u8>,
        observation: EndStreamObservation,
    ) -> Http2Session {
        let (client_io, server_io) = duplex(65536);
        let flushed = serve_raw_burst_then_no_error_reset(server_io, response_frames);
        let mut h2s = match observation {
            EndStreamObservation::Watched => watched_client_session(client_io).await,
            EndStreamObservation::Unwatched => client_session(client_io).await,
        };
        send_open_request(&mut h2s).await;
        flushed
            .await
            .expect("the raw origin must flush the terminal response burst");
        h2s
    }

    const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

    fn contains_request_headers_on_stream_one(bytes: &[u8]) -> bool {
        if !bytes.starts_with(CLIENT_PREFACE) {
            return false;
        }
        let mut offset = CLIENT_PREFACE.len();
        while bytes.len() >= offset + 9 {
            let len = ((bytes[offset] as usize) << 16)
                | ((bytes[offset + 1] as usize) << 8)
                | bytes[offset + 2] as usize;
            let end = offset + 9 + len;
            if bytes.len() < end {
                return false;
            }
            let frame_type = bytes[offset + 3];
            let stream_id =
                u32::from_be_bytes(bytes[offset + 5..offset + 9].try_into().unwrap()) & 0x7fff_ffff;
            if frame_type == 0x1 && stream_id == 1 {
                return true;
            }
            offset = end;
        }
        false
    }

    /// An HTTP/2 frame, built by hand.
    fn raw_frame(frame_type: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u32;
        let id = stream_id.to_be_bytes();
        let mut v = vec![
            (len >> 16) as u8,
            (len >> 8) as u8,
            len as u8,
            frame_type,
            flags,
            id[0],
            id[1],
            id[2],
            id[3],
        ];
        v.extend_from_slice(payload);
        v
    }

    /// `:status: 200` (HPACK static index 8) plus `content-length: 10` as a
    /// literal field without indexing whose NAME is static index 28.
    const RESP_200_CONTENT_LENGTH_10: &[u8] = &[0x88, 0x0F, 0x0D, 0x02, b'1', b'0'];
    const RESP_200_CONTENT_LENGTH_5: &[u8] = &[0x88, 0x0F, 0x0D, 0x01, b'5'];

    /// HPACK static status-code entries used by the hand-written response
    /// sequences below. 100 and 103 have no static value and are literal
    /// values under the indexed `:status` name (static index 8).
    const RESP_200: &[u8] = &[0x88];
    const RESP_100: &[u8] = &[0x08, 0x03, b'1', b'0', b'0'];
    const RESP_103: &[u8] = &[0x08, 0x03, b'1', b'0', b'3'];

    /// `x-trailer: 1` as a literal field without indexing with a new name.
    const TRAILER_BLOCK: &[u8] = &[
        0x00, 0x09, b'x', b'-', b't', b'r', b'a', b'i', b'l', b'e', b'r', 0x01, b'1',
    ];

    fn response_with_trailers(body: &[u8], trailers: &[u8]) -> Vec<u8> {
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200);
        if !body.is_empty() {
            frames.extend_from_slice(&raw_frame(0x0, 0, 1, body));
        }
        frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, trailers));
        frames
    }

    async fn assert_clean_response(
        mut h2s: Http2Session,
        expected_body: Option<&'static [u8]>,
        expected_trailers: Option<HeaderMap>,
    ) {
        match expected_body {
            Some(expected) => {
                assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), expected)
            }
            None => assert!(h2s.read_response_body().await.unwrap().is_none()),
        }
        assert!(
            h2s.read_response_body().await.unwrap().is_none(),
            "the response must remain at clean EOF"
        );
        assert_eq!(h2s.read_trailers().await.unwrap(), expected_trailers);
    }

    /// Which side of the upstream fix the resolved `h2` release is on, for the
    /// unwatched baselines below.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum DependencyBaseline {
        /// `h2` hands the invalid terminal block back as a success, so a
        /// following reset can pass it off as a clean end of body.
        Laundered,
        /// `h2` rejects the invalid terminal block itself, and the rejection
        /// reaches the caller as an error that the fail-closed latch keeps.
        Rejected,
        /// `h2` rejects the block, but the same-burst NO_ERROR reset overwrites
        /// the local PROTOCOL_ERROR and a satisfied `content-length` lets
        /// `read_trailers()` answer "this response had no trailers".
        ///
        /// Deferred, not a regression: an unwatched session holds no evidence
        /// that a terminal block was ever sent, and even the watched direct
        /// read does not latch this yet -- see the `#[ignore]`d
        /// `h2_watched_direct_trailer_read_latches_invalid_terminal_headers`
        /// and H2-004.
        RejectedThenReportedAsNoTrailers,
    }

    /// Report, without failing, which `h2` behavior the unwatched baselines saw.
    ///
    /// These baselines characterize the DEPENDENCY, not a Pingora contract:
    /// `h2 = ">=0.4.16"` has an open upper bound, so an upstream fix must show
    /// up as a reported behavior change rather than as a product regression. The
    /// required contract -- that the adapter never launders an unvalidated
    /// terminal block into a completed response -- is asserted by the
    /// `h2_watched_*` siblings, and stays strict under every outcome below.
    fn report_dependency_baseline(scenario: &str, observed: DependencyBaseline) {
        eprintln!("h2 dependency baseline: {scenario} = {observed:?}");
    }

    async fn assert_clean_empty_trailers(
        mut h2s: Http2Session,
        expected_body: Option<&'static [u8]>,
    ) {
        match expected_body {
            Some(expected) => {
                assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), expected);
                assert!(h2s.read_response_body().await.unwrap().is_none());
            }
            None => assert!(h2s.read_response_body().await.unwrap().is_none()),
        }
        assert!(
            h2s.read_trailers()
                .await
                .unwrap()
                .is_some_and(|trailers| trailers.is_empty()),
            "an explicitly validated empty trailer block must remain distinguishable from None"
        );
    }

    /// Reading body EOF again must not re-poll h2 and discard an already
    /// validated empty trailer map. The narrowed client will short-circuit on
    /// its EOF/trailer latch before touching the reader again.
    #[tokio::test]
    #[ignore = "current repeated body EOF poll can consume validated empty trailers"]
    async fn h2_repeated_body_eof_preserves_validated_empty_trailers() {
        let mut h2s = raw_reset_session(
            response_with_trailers(&[], &[]),
            EndStreamObservation::Unwatched,
        )
        .await;
        assert!(h2s.read_response_body().await.unwrap().is_none());
        assert!(h2s.read_response_body().await.unwrap().is_none());
        assert!(h2s
            .read_trailers()
            .await
            .unwrap()
            .is_some_and(|trailers| trailers.is_empty()));
    }

    /// h2 itself accepts an empty terminal trailer block both with and without
    /// DATA. The adapter must not reinterpret `Some(empty)` as an invalid
    /// terminal block merely because DATA preceded it.
    #[tokio::test]
    async fn h2_unwatched_valid_empty_trailers_are_clean_with_or_without_data() {
        for body in [&b""[..], &b"hello"[..]] {
            let h2s = raw_reset_session(
                response_with_trailers(body, &[]),
                EndStreamObservation::Unwatched,
            )
            .await;
            assert_clean_empty_trailers(h2s, (!body.is_empty()).then_some(&b"hello"[..])).await;
        }
    }

    /// Target contract after h2 can distinguish a validated empty trailer map
    /// from a pseudo-header block whose forbidden fields were discarded. The
    /// current fail-closed adapter rejects both shapes, so this remains deferred.
    #[tokio::test]
    #[ignore = "current fail-closed adapter cannot accept validated empty trailers"]
    async fn h2_watched_valid_empty_trailers_are_clean_with_or_without_data() {
        for body in [&b""[..], &b"hello"[..]] {
            let h2s = raw_reset_session(
                response_with_trailers(body, &[]),
                EndStreamObservation::Watched,
            )
            .await;
            assert_clean_empty_trailers(h2s, (!body.is_empty()).then_some(&b"hello"[..])).await;
        }
    }

    #[tokio::test]
    async fn h2_valid_nonempty_trailers_survive_a_pre_poll_reset() {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", "1".parse().unwrap());
        for observation in [
            EndStreamObservation::Unwatched,
            EndStreamObservation::Watched,
        ] {
            let h2s =
                raw_reset_session(response_with_trailers(b"hello", TRAILER_BLOCK), observation)
                    .await;
            assert_clean_response(h2s, Some(b"hello"), Some(trailers.clone())).await;
        }
    }

    /// Terminal HEADERS must remain observable even when the response has no
    /// DATA, or h2's following reset can launder this codec error into clean EOF.
    #[tokio::test]
    async fn h2_watched_zero_data_invalid_trailers_remain_an_error() {
        let mut h2s = raw_reset_session(
            response_with_trailers(&[], RESP_200),
            EndStreamObservation::Watched,
        )
        .await;
        let err = h2s
            .read_response_body()
            .await
            .expect_err("a response pseudo-header in trailers must remain an error");
        assert_eq!(err.etype(), &ReadError);
        h2s.read_trailers()
            .await
            .expect_err("the invalid trailer result must stay latched");
    }

    #[tokio::test]
    async fn h2_watched_poll_zero_data_invalid_trailers_remain_an_error() {
        let mut h2s = raw_reset_session(
            response_with_trailers(&[], RESP_200),
            EndStreamObservation::Watched,
        )
        .await;
        let err = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
            .await
            .expect("the terminal body poll must report the trailer failure")
            .expect_err("a response pseudo-header in trailers must remain an error");
        assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
        assert!(h2s.response_body_error);
        h2s.read_trailers()
            .await
            .expect_err("the invalid trailer result must stay latched");
    }

    /// The h2-only baseline for why a terminal-HEADERS observation is still
    /// required after DATA accounting is removed: with no watcher, the reset
    /// overwrites the codec error and the invalid zero-DATA trailers can look
    /// exactly like a clean body end.
    ///
    /// This characterizes the dependency, so it accepts BOTH sides of the
    /// upstream fix and reports which one it saw. Requiring the laundering
    /// would turn the day `h2` starts rejecting these trailers into a red CI
    /// run that reads like a Pingora regression -- and the obvious way to
    /// silence that is to pin a known-vulnerable `h2` or delete the evidence
    /// for the watcher. What must not move is the fail-closed contract, so the
    /// rejecting arm still requires the trailer read to keep failing. It
    /// asserts that outcome rather than a mechanism: with no `content-length`
    /// and no DATA, `EndOfBodyProof::holds()` is false either way, so it does
    /// not matter whether the `response_body_error` latch or the proof guard
    /// in `read_trailers` is what holds the door shut.
    #[tokio::test]
    async fn h2_unwatched_zero_data_invalid_trailers_dependency_baseline() {
        let mut h2s = raw_reset_session(
            response_with_trailers(&[], RESP_200),
            EndStreamObservation::Unwatched,
        )
        .await;
        let observed = match h2s.read_response_body().await {
            Ok(None) => {
                assert!(
                    h2s.read_response_body().await.unwrap().is_none(),
                    "the laundered response must remain at clean EOF"
                );
                // Either laundered shape is the dependency's to pick: no
                // trailers at all, or the discarded block as an empty map.
                // Surfacing the illegal fields would be a different defect.
                match h2s.read_trailers().await.unwrap() {
                    None => {}
                    Some(trailers) => assert!(
                        trailers.is_empty(),
                        "a laundered pseudo-header block must not surface fields: {trailers:?}"
                    ),
                }
                DependencyBaseline::Laundered
            }
            Ok(Some(body)) => panic!("a zero-DATA response must not yield body bytes: {body:?}"),
            Err(_) => {
                h2s.read_trailers()
                    .await
                    .expect_err("a rejected terminal block must keep the trailer read failing");
                DependencyBaseline::Rejected
            }
        };
        report_dependency_baseline("unwatched zero-DATA invalid trailers", observed);
    }

    #[tokio::test]
    async fn h2_header_only_response_survives_a_pre_poll_reset() {
        let frames = raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200);
        for observation in [
            EndStreamObservation::Watched,
            EndStreamObservation::Unwatched,
        ] {
            let h2s = raw_reset_session(frames.clone(), observation).await;
            assert_clean_response(h2s, None, None).await;
        }
    }

    /// Delay the first response-future poll until after the origin has flushed
    /// both header-only EOS and its reset. h2 >= 0.4.16 preserves received EOS
    /// across that reset; the client must latch it when the queued final response
    /// is eventually accepted instead of mistaking the wire marker for trailers.
    #[tokio::test]
    async fn h2_header_only_response_latches_eos_after_delayed_header_poll() {
        let (client_io, server_io) = duplex(65536);
        let frames = raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200);
        let flushed = serve_raw_burst_then_no_error_reset(server_io, frames);
        let mut h2s = watched_client_session(client_io).await;

        let mut req = RequestHeader::build("POST", b"/", None).unwrap();
        req.insert_header(http::header::HOST, "example.com")
            .unwrap();
        h2s.write_request_header(Box::new(req), false).unwrap();
        flushed
            .await
            .expect("the raw origin must flush header-only EOS and reset");
        await_reset_processed(&h2s).await;

        h2s.read_response_header().await.unwrap();
        assert!(
            h2s.response_initial_end_stream,
            "received initial EOS must survive a reset before the first header poll"
        );
        assert_clean_response(h2s, None, None).await;
    }

    fn prepend_informational(prefixes: &[&[u8]], mut final_frames: Vec<u8>) -> Vec<u8> {
        let mut frames = Vec::new();
        for block in prefixes {
            frames.extend_from_slice(&raw_frame(0x1, 0x4, 1, block));
        }
        frames.append(&mut final_frames);
        frames
    }

    /// Informational responses are consumed by h2's response future and must
    /// not affect how the adapter classifies the final response. Exercise one
    /// and multiple prefixes across every currently unambiguous terminal shape.
    #[tokio::test]
    async fn h2_informational_prefixes_preserve_final_response_shapes() {
        let mut nonempty = HeaderMap::new();
        nonempty.insert("x-trailer", "1".parse().unwrap());

        let cases = [
            (
                vec![RESP_100],
                raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200),
                None,
                None,
            ),
            (
                vec![RESP_100, RESP_103],
                {
                    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200);
                    frames.extend_from_slice(&raw_frame(0x0, FLAG_END_STREAM_RAW, 1, b"hello"));
                    frames
                },
                Some(&b"hello"[..]),
                None,
            ),
            (
                vec![RESP_100, RESP_103],
                response_with_trailers(b"hello", TRAILER_BLOCK),
                Some(&b"hello"[..]),
                Some(nonempty),
            ),
        ];

        for observation in [
            EndStreamObservation::Watched,
            EndStreamObservation::Unwatched,
        ] {
            for (prefixes, final_frames, body, trailers) in &cases {
                let frames = prepend_informational(prefixes, final_frames.clone());
                let h2s = raw_reset_session(frames, observation).await;
                assert_clean_response(h2s, *body, trailers.clone()).await;
            }
        }
    }

    /// Deferred with the other valid-empty controls: the current h2 public API
    /// cannot distinguish this from a trailer pseudo-header block whose fields
    /// were discarded before a same-burst reset.
    #[tokio::test]
    #[ignore = "requires decoder-level rejection before valid empty trailers can be accepted"]
    async fn h2_watched_informational_prefix_preserves_valid_empty_trailers() {
        let frames = prepend_informational(&[RESP_103], response_with_trailers(&[], &[]));
        let h2s = raw_reset_session(frames, EndStreamObservation::Watched).await;
        assert_clean_empty_trailers(h2s, None).await;
    }

    /// An informational response cannot end the stream. This must fail while
    /// reading the response header; neither a terminal-frame observation nor a
    /// following NO_ERROR reset may turn it into a final response.
    #[tokio::test]
    async fn h2_informational_end_stream_is_a_header_error() {
        let frames = raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_100);
        for observation in [
            EndStreamObservation::Watched,
            EndStreamObservation::Unwatched,
        ] {
            let (client_io, server_io) = duplex(65536);
            let _ = serve_raw_burst_then_no_error_reset(server_io, frames.clone());
            let mut h2s = match observation {
                EndStreamObservation::Watched => watched_client_session(client_io).await,
                EndStreamObservation::Unwatched => client_session(client_io).await,
            };

            let mut req = RequestHeader::build("POST", b"/", None).unwrap();
            req.insert_header(http::header::HOST, "example.com")
                .unwrap();
            h2s.write_request_header(Box::new(req), false).unwrap();
            h2s.read_response_header().await.expect_err(
                "END_STREAM on an informational response must fail the response future",
            );
        }
    }

    /// C1: a response that declares `content-length: 10`, sends 5 bytes, then
    /// puts END_STREAM on a frame `h2` THROWS AWAY.
    ///
    /// `Recv::recv_data` checks `ensure_content_length_zero()` before pushing
    /// the payload onto `pending_recv`, so the last frame's bytes are decoded
    /// and discarded and `h2` closes the stream locally with PROTOCOL_ERROR.
    /// The peer's RST_STREAM(NO_ERROR), arriving in the same burst, overwrites
    /// that local close with a remote `NO_ERROR`.
    ///
    /// So every ingredient of "a clean end of body" is present: the wire
    /// carried END_STREAM (source (iv)'s flag is set), and the error is a
    /// remote `NO_ERROR` reset. Only the BYTE COUNT dissents -- the wire
    /// carried 7 payload bytes and the reader received 5 -- and only the
    /// unsatisfied `content-length` dissents alongside it. Without both, this
    /// delivers 5 of a declared 10 bytes as a complete 200 and admits it to
    /// cache.
    #[tokio::test]
    async fn h2_watched_content_length_underflow_reset_is_not_a_clean_eof() {
        let (client_io, server_io) = duplex(65536);
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_10);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(0x0, FLAG_END_STREAM_RAW, 1, b"xy"));
        let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

        let mut h2s = watched_client_session(client_io).await;
        send_open_request(&mut h2s).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert_eq!(h2s.body_bytes_received(), 5);

        // The premises. If either stops holding, this test has stopped
        // reproducing the defect and is passing for the wrong reason.
        assert_eq!(h2s.response_body_declared_len, Some(10));
        assert!(
            h2s.peer_end_stream.observed(),
            "the wire DID carry END_STREAM -- the flag alone would say 'complete'"
        );

        let err = h2s.read_response_body().await.expect_err(
            "5 of a declared 10 bytes must not read as a clean EOF just because the \
             frame h2 discarded happened to carry END_STREAM",
        );
        assert_eq!(err.etype(), &ReadError);
        // ... and the reset really was the remote NO_ERROR that
        // `benign_post_eof_stream_end` accepts, i.e. the flag was the only thing
        // standing between this response and a clean EOF.
        assert!(
            format!("{err:?}").contains("NO_ERROR, Remote"),
            "the underlying h2 error must be the remote NO_ERROR reset: {err:?}"
        );

        h2s.read_trailers()
            .await
            .expect_err("nor may the trailer read launder it into 'no trailers'");
    }

    /// The same laundering one drop path over: TRAILERS rejected because the
    /// stream still owes `content-length` bytes.
    ///
    /// `Recv::recv_trailers` errors out before queueing the trailers, again
    /// with a local PROTOCOL_ERROR that the burst's RST_STREAM(NO_ERROR)
    /// overwrites. No DATA is dropped here, so the byte counts AGREE (5 on the
    /// wire, 5 read) -- this is the shape a byte count alone would still
    /// launder, and the unsatisfied `content-length` is what rejects it. Both
    /// halves of `EndOfBodyProof` are load-bearing; this test and the one above
    /// pin one each.
    #[tokio::test]
    async fn h2_watched_rejected_trailers_reset_is_not_a_clean_eof() {
        let (client_io, server_io) = duplex(65536);
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_10);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, TRAILER_BLOCK));
        let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

        let mut h2s = watched_client_session(client_io).await;
        send_open_request(&mut h2s).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert_eq!(h2s.body_bytes_received(), 5);
        assert_eq!(h2s.response_body_declared_len, Some(10));
        assert!(
            !h2s.peer_end_stream.observed(),
            "unvalidated trailers must not publish clean-EOF evidence"
        );
        assert!(h2s.peer_end_stream.terminal_headers_observed());

        let err = h2s.read_response_body().await.expect_err(
            "5 of a declared 10 bytes must not read as a clean EOF just because the \
             rejected trailers carried END_STREAM",
        );
        assert_eq!(err.etype(), &ReadError);
        assert!(
            format!("{err:?}").contains("NO_ERROR, Remote"),
            "the underlying h2 error must be the remote NO_ERROR reset: {err:?}"
        );

        h2s.read_trailers()
            .await
            .expect_err("nor may the trailer read launder it into 'no trailers'");
    }

    /// A trailer block containing a response pseudo-header is invalid even
    /// when its HEADERS frame carries END_STREAM. A following remote NO_ERROR
    /// reset must not turn that codec rejection into a successful EOF.
    #[tokio::test]
    async fn h2_watched_invalid_trailers_reset_is_not_a_clean_eof() {
        let (client_io, server_io) = duplex(65536);
        let mut frames = raw_frame(0x1, 0x4, 1, &[0x88]);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        // HPACK static index 8 is `:status: 200`, which is illegal in trailers.
        frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, &[0x88]));
        let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

        let mut h2s = watched_client_session(client_io).await;
        send_open_request(&mut h2s).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert_eq!(h2s.body_bytes_received(), 5);
        assert!(h2s.response_body_declared_len.is_none());
        assert!(
            !h2s.peer_end_stream.observed(),
            "unvalidated trailer END_STREAM must not be completion evidence"
        );
        assert!(
            h2s.peer_end_stream.terminal_headers_observed(),
            "the wire watcher must retain the unvalidated terminal trailers"
        );

        let err = h2s
            .read_response_body()
            .await
            .expect_err("invalid trailers must remain a response-body error");
        assert_eq!(err.etype(), &ReadError);
        h2s.read_trailers()
            .await
            .expect_err("invalid trailers must remain a trailer-read error");
    }

    /// A caller may consume exactly the declared body bytes and then ask for
    /// trailers without issuing a body EOF read, and this pins what that API
    /// order yields for an illegal terminal pseudo-header block.
    ///
    /// Like the zero-DATA baseline above this characterizes the dependency, so
    /// it accepts every outcome the dependency may produce and reports which
    /// one it saw. All three are dependency shapes, not product verdicts:
    /// today's `h2` launders the block into an empty map, while an `h2` that
    /// rejects it may surface either an error or -- because the same-burst
    /// NO_ERROR reset overwrites the local PROTOCOL_ERROR and the declared
    /// `content-length` is satisfied -- a plain "no trailers". Closing that
    /// last gap needs the terminal-HEADERS state consulted from the direct
    /// read, which is H2-004's deferred work and is pinned by the `#[ignore]`d
    /// `h2_watched_direct_trailer_read_latches_invalid_terminal_headers`.
    /// Failing here instead would file that known gap as a fresh regression on
    /// the day the dependency gets safer. What stays asserted is that the
    /// fields of an illegal block never reach the caller.
    #[tokio::test]
    async fn h2_unwatched_direct_trailer_read_invalid_terminal_headers_dependency_baseline() {
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200));

        let mut h2s = raw_reset_session(frames.clone(), EndStreamObservation::Unwatched).await;
        let observed = match h2s.read_response_body().await {
            Ok(Some(body)) => {
                assert_eq!(body, "hello");
                match h2s.read_trailers().await {
                    Ok(Some(trailers)) => {
                        assert!(
                            trailers.is_empty(),
                            "a laundered pseudo-header block must not surface fields: {trailers:?}"
                        );
                        DependencyBaseline::Laundered
                    }
                    Ok(None) => {
                        // Corroborate that the DEPENDENCY is what rejected the
                        // block, so that weakening a guard in `read_trailers`
                        // cannot pass itself off as an upstream fix and be
                        // absorbed by this arm. Replaying the same frames in
                        // body-EOF order forces the rejection into the open:
                        // `read_response_body`'s EOF branch validates the
                        // trailers itself instead of consulting those guards.
                        let mut replay =
                            raw_reset_session(frames.clone(), EndStreamObservation::Unwatched)
                                .await;
                        assert_eq!(replay.read_response_body().await.unwrap().unwrap(), "hello");
                        replay.read_response_body().await.expect_err(
                            "'no trailers' here must come from h2 rejecting the block, which \
                             the body-EOF read order surfaces as an error",
                        );
                        DependencyBaseline::RejectedThenReportedAsNoTrailers
                    }
                    Err(_) => DependencyBaseline::Rejected,
                }
            }
            Ok(None) => panic!("the declared body bytes must still be delivered"),
            Err(_) => {
                h2s.read_trailers()
                    .await
                    .expect_err("a rejected terminal block must keep the trailer read failing");
                DependencyBaseline::Rejected
            }
        };
        report_dependency_baseline(
            "unwatched direct trailer read of invalid terminal headers",
            observed,
        );
    }

    /// This is the pseudo-only companion to the mixed-field decoder contract.
    /// It deliberately runs without the watcher first: the h2 source itself
    /// must reject trailer pseudo-headers before any reset can hide the error.
    #[tokio::test]
    #[ignore = "requires decoder-level rejection of pseudo-headers in trailers"]
    async fn h2_pseudo_only_trailers_never_complete() {
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200));

        for observation in [
            EndStreamObservation::Unwatched,
            EndStreamObservation::Watched,
        ] {
            let mut h2s = raw_reset_session(frames.clone(), observation).await;
            assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
            assert!(
                h2s.read_response_body().await.is_err(),
                "async body EOF must reject pseudo-only trailers for {observation:?}"
            );
            assert!(
                h2s.read_trailers().await.is_err(),
                "direct trailers must observe the same permanent error for {observation:?}"
            );
        }
    }

    /// This isolates the watched direct-trailer API from Pingora's body-EOF
    /// latch. h2 must not queue malformed trailers, and the final client latch
    /// must reject the resulting missing trailer event before it becomes EOF.
    #[tokio::test]
    #[ignore = "requires decoder-level rejection of pseudo-headers in trailers"]
    async fn h2_direct_trailer_first_rejects_pseudo_trailers() {
        let mut mixed = RESP_200.to_vec();
        mixed.extend_from_slice(TRAILER_BLOCK);

        for invalid_trailers in [RESP_200, mixed.as_slice()] {
            let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
            frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
            frames.extend_from_slice(&raw_frame(
                0x1,
                0x4 | FLAG_END_STREAM_RAW,
                1,
                invalid_trailers,
            ));

            let mut h2s = raw_reset_session(frames, EndStreamObservation::Watched).await;
            assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
            assert!(
                h2s.read_trailers().await.is_err(),
                "direct first trailer read must reject malformed trailers"
            );
        }
    }

    #[tokio::test]
    #[ignore = "current direct read_trailers() does not consult terminal HEADERS state"]
    async fn h2_watched_direct_trailer_read_latches_invalid_terminal_headers() {
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200));

        let mut h2s = raw_reset_session(frames, EndStreamObservation::Watched).await;
        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        for _ in 0..2 {
            let err = h2s
                .read_trailers()
                .await
                .expect_err("invalid terminal headers must stay permanently failed");
            assert_eq!(err.etype(), &ReadError);
        }
    }

    /// A pseudo-header followed by an ordinary trailer is more dangerous than
    /// the pseudo-only case: h2 can discard the pseudo field yet still expose
    /// the ordinary field as a nonempty map. Any terminal state machine that
    /// trusts `Some(nonempty)` without decoder-level pseudo-header rejection
    /// would accept and cache this malformed response.
    #[tokio::test]
    #[ignore = "requires decoder-level rejection of pseudo-headers in trailers"]
    async fn h2_mixed_pseudo_and_regular_trailers_never_complete() {
        let mut invalid_trailers = RESP_200.to_vec();
        invalid_trailers.extend_from_slice(TRAILER_BLOCK);
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(
            0x1,
            0x4 | FLAG_END_STREAM_RAW,
            1,
            &invalid_trailers,
        ));

        for observation in [
            EndStreamObservation::Unwatched,
            EndStreamObservation::Watched,
        ] {
            let mut h2s = raw_reset_session(frames.clone(), observation).await;
            assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
            assert!(
                h2s.read_response_body().await.is_err(),
                "async body EOF must reject mixed pseudo/regular trailers for {observation:?}"
            );
            assert!(
                h2s.read_trailers().await.is_err(),
                "direct trailers must observe the same permanent error for {observation:?}"
            );
        }
    }

    /// The decoder rejects the mixed pseudo form before it can become a
    /// nonempty map; the terminal-HEADERS marker then prevents the same-burst
    /// reset from turning its missing poll-trailer event into EOF.
    #[tokio::test]
    #[ignore = "requires decoder rejection and final terminal-HEADERS poll latch"]
    async fn h2_watched_poll_mixed_pseudo_and_regular_trailers_never_complete() {
        let mut invalid_trailers = RESP_200.to_vec();
        invalid_trailers.extend_from_slice(TRAILER_BLOCK);
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(
            0x1,
            0x4 | FLAG_END_STREAM_RAW,
            1,
            &invalid_trailers,
        ));

        let mut h2s = raw_reset_session(frames, EndStreamObservation::Watched).await;
        let first = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
            .await
            .expect("the first body poll must yield DATA")
            .expect("the first body poll must succeed");
        assert_eq!(first, "hello");

        let terminal = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
            .await
            .expect("the terminal body poll must report a trailer error");
        assert!(
            terminal.is_err(),
            "poll body EOF must reject mixed pseudo/regular trailers"
        );
        assert!(
            h2s.read_trailers().await.is_err(),
            "direct trailers must remain failed after poll error"
        );
    }

    /// The poll API must latch the same terminal trailer failure as the async
    /// API. Otherwise a caller can observe the body error and then retry
    /// `read_trailers`, after the failing h2 result has already been consumed.
    #[tokio::test]
    async fn h2_poll_body_invalid_trailers_latches_the_trailer_error() {
        let (client_io, server_io) = duplex(65536);
        let mut frames = raw_frame(0x1, 0x4, 1, &[0x88]);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, &[0x88]));
        let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

        let mut h2s = watched_client_session(client_io).await;
        send_open_request(&mut h2s).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let first = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
            .await
            .expect("the first body poll must yield DATA")
            .expect("the first body poll must succeed");
        assert_eq!(first, "hello");

        let err = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
            .await
            .expect("the terminal body poll must report the trailer failure")
            .expect_err("invalid trailers must remain a poll-body error");
        assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
        assert!(
            h2s.response_body_error,
            "the poll API must latch the trailer validation failure"
        );

        for _ in 0..2 {
            h2s.read_trailers()
                .await
                .expect_err("a latched trailer failure must remain permanent");
        }
    }

    /// The half of the guard the two tests above do NOT pin on their own: a
    /// response with no `content-length` at all, whose END_STREAM-bearing DATA
    /// frame `h2` drops for overrunning the STREAM receive window.
    ///
    /// `Recv::recv_data` raises `library_reset(FLOW_CONTROL_ERROR)` before
    /// `pending_recv.push_back`, so the payload never reaches the reader; the
    /// burst's RST_STREAM(NO_ERROR) then overwrites the local close. Nothing
    /// here declares a length, so `declared_len_satisfied` is vacuously true
    /// and only the byte count -- 2005 on the wire against 5 read -- can reject
    /// it.
    ///
    /// The connection window is left wide so that the STREAM check is the one
    /// that fires; overrunning the connection window is a GOAWAY, a different
    /// shape entirely.
    #[tokio::test]
    async fn h2_watched_flow_control_drop_reset_is_not_a_clean_eof() {
        let (client_io, server_io) = duplex(1 << 20);
        let mut frames = raw_frame(0x1, 0x4, 1, &[0x88]); // `:status: 200`, no content-length
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(0x0, FLAG_END_STREAM_RAW, 1, &[b'z'; 2000]));
        let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

        let mut h2s = watched_client_session_with(client_io, |b| {
            b.initial_window_size(1024)
                .initial_connection_window_size(1 << 20)
        })
        .await;
        send_open_request(&mut h2s).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert_eq!(h2s.body_bytes_received(), 5);
        assert!(
            h2s.response_body_declared_len.is_none(),
            "no content-length may be declared, or the other half of the guard \
             would be doing the work"
        );
        assert!(
            h2s.peer_end_stream.observed(),
            "the wire DID carry END_STREAM -- the flag alone would say 'complete'"
        );

        let err = h2s.read_response_body().await.expect_err(
            "a body h2 dropped for overrunning the receive window must not read as \
             a clean EOF just because the dropped frame carried END_STREAM",
        );
        assert_eq!(err.etype(), &ReadError);
        assert!(
            format!("{err:?}").contains("NO_ERROR, Remote"),
            "the underlying h2 error must be the remote NO_ERROR reset: {err:?}"
        );
    }

    /// END_STREAM's flag bit, for the hand-built frames above.
    const FLAG_END_STREAM_RAW: u8 = 0x1;

    // NOTE: the padded-DATA accounting has no end-to-end test here on purpose --
    // `h2`'s server API never emits padding, so there is no way to put a padded
    // frame on this wire without hand-rolling the whole server side. It is
    // pinned at the scanner instead, by
    // `end_stream_watch::tests::padding_is_not_counted_as_payload`, which is
    // where the arithmetic lives.
}
