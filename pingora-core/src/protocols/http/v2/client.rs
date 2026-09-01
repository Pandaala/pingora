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
    // It is latched rather than recomputed so initial-header EOS and a clean
    // body poll remain stable across later errors and dependency state-machine
    // changes. Supported h2 0.4.19 preserves received EOS across reset, but
    // Pingora's completion proof does not depend on that private representation.
    response_body_eof: bool,
    // Source (iii) of the same end-of-body proof: the non-zero `content-length`
    // the response declared, compared against `body_recv`. This independently
    // proves a fixed-length body once every declared byte was delivered,
    // including natural DATA(END_STREAM)-then-reset orderings. See
    // [`Http2Session::response_body_complete`].
    response_body_declared_len: Option<usize>,
    // The h2 stream id, once the request has been sent.
    stream_id: Option<u32>,
    // Source (iv): set by [`super::end_stream_watch`] when the peer's END_STREAM
    // flag was seen on the wire BEFORE anything tore the stream down. This is
    // an independent source for the RFC 9113 §8.1 shape -- a complete response
    // followed by RST_STREAM(NO_ERROR) while this side is still uploading --
    // and the only source that also verifies wire/delivered byte equality. May
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
    /// anything tore the stream down. Supported h2 preserves decoded
    /// END_STREAM in the RFC 9113 §8.1 shape (complete response, then
    /// RST_STREAM(NO_ERROR), while this side is still uploading a request
    /// body), but only the watch also proves wire/delivered byte equality and
    /// applies Pingora's local-reset and GOAWAY evidence boundaries.
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
            // Only reachable with an end-of-body proof already in hand. h2
            // 0.4.19's normal END_STREAM-then-reset path reports clean EOF;
            // this defensive fallback handles other benign reset/GOAWAY error
            // orderings without costing an exchange a response already proven
            // complete. See `benign_post_eof_stream_end` for why the proof and
            // error-code guards are both required.
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
            // so a benign terminal error shape on top of it is not an error.
            // h2 0.4.19's normal END_STREAM-then-reset path returns `None` via
            // `ErrorAfterEndStream`; this is a defensive fallback for other
            // benign dependency/error orderings, not an assertion that current
            // h2 overwrites received END_STREAM.
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
#[path = "client_tests_h2.rs"]
mod tests_h2;
