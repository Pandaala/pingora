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
use crate::protocols::{Digest, SocketAddr, UniqueIDType};

pub const PING_TIMEDOUT: ErrorType = ErrorType::new("PingTimedout");

pub struct Http2Session {
    send_req: SendRequest<Bytes>,
    send_body: Option<SendStream<Bytes>>,
    resp_fut: Option<ResponseFuture>,
    req_sent: Option<Box<RequestHeader>>,
    response_header: Option<ResponseHeader>,
    response_body_reader: Option<RecvStream>,
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
            read_timeout: None,
            write_timeout: None,
            conn,
            ended: false,
            body_recv: 0,
            response_body_eof: false,
            response_body_declared_len: None,
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
    /// RESIDUAL GAP, deliberate and load-bearing: a response that declares no
    /// USABLE `content-length` -- either none at all (the shape gRPC and every
    /// chunked-style H2 stream use) or `content-length: 0`, which
    /// [`super::server::declared_body_length`] maps to `None` because on H2 it
    /// says nothing about END_STREAM -- and whose peer resets the stream before
    /// we poll the final DATA frame has no surviving proof that its body is
    /// whole, so the reset still surfaces as a read error. That is the correct
    /// failure direction -- guessing wrong hands a TRUNCATED response to the
    /// downstream client as if it were complete. Do NOT "fix" this by dropping
    /// the guard; the negative-direction test
    /// (`h2_response_body_no_error_reset_before_eos_is_an_error`) exists to keep
    /// that from happening quietly.
    fn response_body_complete(&self) -> bool {
        self.response_body_eof
            || self
                .response_body_declared_len
                .is_some_and(|len| self.body_recv >= len)
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
        // There is no write timeout for h2 because the actual write happens async from this fn
        let (resp_fut, send_body) = self
            .send_req
            .send_request(request, end)
            .or_err(H2Error, "while sending request")
            .map_err(|e| self.handle_err(e))?;
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
    /// the write half alone.
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

        let Some(resp_fut) = self.resp_fut.take() else {
            panic!("Try to take response header, but it is already taken")
        };

        let res = match self.read_timeout {
            Some(t) => timeout(t, resp_fut)
                .await
                .map_err(|_| Error::explain(ReadTimedout, "while reading h2 response header"))
                .map_err(|e| self.handle_err(e))?,
            None => resp_fut.await,
        };
        let (resp, body_reader) = res.map_err(handle_read_header_error)?.into_parts();
        self.response_body_declared_len = super::server::declared_body_length(&resp.headers);
        self.response_header = Some(resp.into());
        // END_STREAM on the HEADERS frame: the response body is complete (and
        // empty) before a single body read happens.
        self.response_body_eof = body_reader.is_end_stream();
        self.response_body_reader = Some(body_reader);

        Ok(())
    }

    #[doc(hidden)]
    pub fn poll_read_response_header(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), h2::Error>> {
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

        let (resp, body_reader) = res.into_parts();
        self.response_body_declared_len = super::server::declared_body_length(&resp.headers);
        self.response_header = Some(resp.into());
        self.response_body_eof = body_reader.is_end_stream();
        self.response_body_reader = Some(body_reader);

        Poll::Ready(Ok(()))
    }

    /// Read the response body
    ///
    /// `None` means, no more body to read
    pub async fn read_response_body(&mut self) -> Result<Option<Bytes>> {
        // Read before the mutable borrow of the reader below; the value cannot
        // change while this call is in flight.
        let body_complete = self.response_body_complete();
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
            Err(e) if body_complete && benign_post_eof_stream_end(&e) => {
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
            // Latch at the earliest moment the transport can still prove it:
            // right after the DATA frame that carried END_STREAM was handed
            // over, while nothing is buffered behind it.
            if body_reader.is_end_stream() {
                self.response_body_eof = true;
            }
        } else {
            // `data()` yielding `None` means `h2` had either observed
            // END_STREAM or already queued the trailers frame; either way the
            // body is complete.
            self.response_body_eof = true;
        }

        Ok(body)
    }

    #[doc(hidden)]
    pub fn poll_read_response_body(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, h2::Error>>> {
        let Some(body_reader) = self.response_body_reader.as_mut() else {
            // req is not sent or response is already read
            // TODO: warn
            return Poll::Ready(None);
        };

        let data = match ready!(body_reader.poll_data(cx)).transpose() {
            Ok(data) => data,
            Err(err) => return Poll::Ready(Some(Err(err))),
        };

        if let Some(data) = data {
            body_reader.flow_control().release_capacity(data.len())?;
            if body_reader.is_end_stream() {
                self.response_body_eof = true;
            }
            return Poll::Ready(Some(Ok(data)));
        }

        self.response_body_eof = true;
        Poll::Ready(None)
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
            Err(e) if self.response_body_complete() && benign_post_eof_stream_end(&e) => Ok(None),
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
        if !self.ended || !self.response_finished() {
            if let Some(send_body) = self.send_body.as_mut() {
                send_body.send_reset(h2::Reason::INTERNAL_ERROR)
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

    /// Build an [`Http2Session`] over `client_io` and start its connection task.
    async fn client_session(client_io: tokio::io::DuplexStream) -> Http2Session {
        let (send_req, connection) = h2::client::handshake(client_io).await.unwrap();
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

    /// Block until this side has processed the peer's RST_STREAM.
    ///
    /// The reset overwrites the stream state, which flips the RAW
    /// `RecvStream::is_end_stream()` back to `false`. That raw value is exactly
    /// what the session no longer exposes (`response_finished()` is latched, and
    /// staying `true` across a reset is the point of the latch), so this reaches
    /// into the private reader: waiting on the fact rather than on a sleep is
    /// what keeps these tests from passing for the wrong reason when the reset
    /// has not landed yet.
    async fn await_reset_processed(h2s: &Http2Session) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while raw_receiver_ended(h2s) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("peer RST_STREAM was never processed");
    }

    /// The un-latched `h2` view of the receive half, for tests that need to
    /// observe the state the latch exists to preserve.
    fn raw_receiver_ended(h2s: &Http2Session) -> bool {
        h2s.response_body_reader
            .as_ref()
            .is_some_and(|r| r.is_end_stream())
    }

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
        // The precondition this test exists for: the reset already destroyed
        // the END_STREAM evidence. If this ever fails, the test has silently
        // degenerated into the easy ordering covered by the test above.
        assert!(
            !raw_receiver_ended(&h2s),
            "the peer reset must already have overwritten the stream state"
        );

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
}
