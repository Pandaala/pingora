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
use http::{header, HeaderMap, Response};
use log::{debug, warn};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_timeout::timeout;
use std::sync::Arc;
use std::task::ready;
use std::time::Duration;

use crate::protocols::http::body_buffer::{
    FixedBuffer, RegisteredRequestBodyBuffer, RequestBodyBuffer,
};
use crate::protocols::http::date::get_cached_date;
use crate::protocols::http::v1::client::http_req_header_to_wire;
use crate::protocols::http::HttpTask;
use crate::protocols::{Digest, SocketAddr, Stream};
use crate::{Error, ErrorType, OrErr, Result};

const BODY_BUF_LIMIT: usize = 1024 * 64;

type H2Connection<S> = server::Connection<S, Bytes>;

pub use h2::server::Builder as H2Options;

// 64 KiB decoded header-list limit.
const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 64 * 1024;
const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 100;

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

/// HTTP/2 server session
pub struct HttpSession {
    request_header: RequestHeader,
    request_body_reader: RecvStream,
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
    // digest to record underlying connection info
    digest: Arc<Digest>,
    /// The write timeout which will be applied to writing response body.
    /// The timeout is reset on every write. This is not a timeout on the overall duration of the
    /// response.
    pub write_timeout: Option<Duration>,
    // How long to wait when draining (discarding) request body
    total_drain_timeout: Option<Duration>,
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
    /// `None` will be returned when the connection is closing so that the loop can exit.
    ///
    pub async fn from_h2_conn(
        conn: &mut H2Connection<Stream>,
        digest: Arc<Digest>,
    ) -> Result<Option<Self>> {
        // NOTE: conn.accept().await is what drives the entire connection.
        let res = conn.accept().await.transpose().or_err(
            ErrorType::H2Error,
            "while accepting new downstream requests",
        )?;

        Ok(res.map(|(req, send_response)| {
            let (request_header, request_body_reader) = req.into_parts();
            HttpSession {
                request_header: request_header.into(),
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
                digest,
                write_timeout: None,
                total_drain_timeout: None,
            }
        }))
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

    /// Read request body bytes. `None` when there is no more body to read.
    pub async fn read_body_bytes(&mut self) -> Result<Option<Bytes>> {
        if self.early_body_capture_poisoned {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body capture failed or was cancelled mid-chunk; buffered body is incomplete",
            );
        }
        // TODO: timeout
        let data = self.request_body_reader.data().await.transpose().or_err(
            ErrorType::ReadError,
            "while reading downstream request body",
        )?;
        if let Some(data) = data.as_ref() {
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
                if let Some(buffer) = self.early_body_buffer.as_mut() {
                    self.early_body_capture_poisoned = true;
                    buffer.finish_capture().await?;
                    self.early_body_capture_poisoned = false;
                }
            }
        } else if let Some(buffer) = self.early_body_buffer.as_mut() {
            self.early_body_capture_poisoned = true;
            buffer.finish_capture().await?;
            self.early_body_capture_poisoned = false;
        }
        Ok(data)
    }

    // A `RequestBodyBuffer::write` is async and cannot run in a poll context.
    // Fail closed when capture/replay is registered instead of silently returning
    // bytes that bypass the buffer.
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
            return Poll::Ready(Some(Ok(data)));
        }

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
        if self.is_body_done() {
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
    /// This will send a `INTERNAL_ERROR` stream error to the client
    pub fn shutdown(&mut self) {
        if !self.ended {
            self.send_response.send_reset(h2::Reason::INTERNAL_ERROR);
        }
    }

    #[doc(hidden)]
    pub fn take_response_body_writer(&mut self) -> Option<SendStream<Bytes>> {
        self.send_response_body.take()
    }

    // This is a hack for pingora-proxy to create subrequests from h2 server session
    // TODO: be able to convert from h2 to h1 subrequest
    pub fn pseudo_raw_h1_request_header(&self) -> Bytes {
        let buf = http_req_header_to_wire(&self.request_header).unwrap(); // safe, None only when version unknown
        buf.freeze()
    }

    /// Whether there is no more body to read
    pub fn is_body_done(&self) -> bool {
        if self
            .early_body_buffer
            .as_ref()
            .is_some_and(RegisteredRequestBodyBuffer::is_replaying)
        {
            return false;
        }
        // Check no body in request
        // Also check we hit end of stream
        self.is_body_empty() || self.request_body_reader.is_end_stream()
    }

    /// Whether there is any body to read. true means there no body in request.
    ///
    /// While an early request body buffer is registered, the effective body is whatever
    /// the buffer replays, which may be a non-empty rewrite of a zero-byte original
    /// (e.g. HEADERS without END_STREAM followed by an empty END_STREAM DATA frame).
    /// Report non-empty then, so upstream framing decisions (H2 END_STREAM on HEADERS)
    /// keep the stream open until replay reaches EOF.
    pub fn is_body_empty(&self) -> bool {
        if self.early_body_buffer.is_some() {
            return false;
        }
        self.body_read == 0
            && (self.request_body_reader.is_end_stream()
                || self
                    .request_header
                    .headers
                    .get(header::CONTENT_LENGTH)
                    .is_some_and(|cl| cl.as_bytes() == b"0"))
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
    use h2::frame::{Frame, Settings};
    use http::{HeaderValue, Method, Request};
    use tokio::io::{duplex, AsyncWriteExt, DuplexStream};
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

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
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

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                let req = http.req_header();
                assert_eq!(req.method, Method::POST);
                assert_eq!(req.uri, "https://www.example.com/");

                // 1. Check body related methods
                http.enable_retry_buffering();
                assert!(http.is_body_empty());
                assert!(http.is_body_done());
                let retry_body = http.get_retry_buffer();
                assert!(retry_body.is_none());

                // 2. Send response
                let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
                assert!(http
                    .write_response_header(response_header.clone(), false)
                    .is_ok());

                http.write_body(server_body.into(), false).await.unwrap();
                assert_eq!(http.body_bytes_sent(), 16);

                // 3. Waiting for the reset from the client
                assert!(http.read_body_or_idle(http.is_body_done()).await.is_err());
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

            let (head, mut body) = response.await.unwrap().into_parts();

            assert_eq!(head.status, 200);
            let data = body.data().await.unwrap().unwrap();
            assert_eq!(data, server_body);

            req_body.send_data("".into(), true).unwrap(); // set EOS after read the resp body
        }));

        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());

        while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            handles.push(tokio::spawn(async move {
                let req = http.req_header();
                assert_eq!(req.method, Method::POST);
                assert_eq!(req.uri, "https://www.example.com/");

                // 1. Check body related methods
                http.enable_retry_buffering();
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

                // 3. Waiting for the client to close stream.
                http.read_body_or_idle(http.is_body_done()).await.unwrap();
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
}
