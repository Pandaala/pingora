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
use crate::proxy_cache::{range_filter::RangeBodyFilter, ServeFromCache};
use crate::proxy_common::*;
use http::{header::CONTENT_LENGTH, Method, StatusCode};
use pingora_cache::CachePhase;
use pingora_core::protocols::http::custom::CUSTOM_MESSAGE_QUEUE_SIZE;
use pingora_core::protocols::http::v2::{client::Http2Session, write_body};

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

/// How the pump may write the upstream request body on this attempt.
#[derive(Debug, Clone, Copy)]
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

        if req.version != Version::HTTP_2 {
            /* remove H1 specific headers */
            // https://github.com/hyperium/h2/blob/d3b9f1e36aadc1a7a6804e2f8e86d3fe4a244b4f/src/proto/streams/send.rs#L72
            req.remove_header(&http::header::TRANSFER_ENCODING);
            req.remove_header(&http::header::CONNECTION);
            req.remove_header(&http::header::UPGRADE);
            req.remove_header("keep-alive");
            req.remove_header("proxy-connection");
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
        // read and still produces exactly one application end-of-stream event.
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
        //   end-of-stream event.
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

        /* read downstream body and upstream response at the same time */

        let ret = {
            let downstream = self.bidirection_down_to_up(
                session,
                &mut client_body,
                rx,
                ctx,
                &mut downstream_custom_message_writer,
                UpstreamBodyWrite {
                    timeout: write_timeout,
                    stream_closed: upstream_body_closed,
                    eos_write_optional: false,
                    disposition: body_disposition,
                },
            );
            let upstream = pipe_up_to_down_response(client_session, tx);
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
                        Ok(DownstreamRequestOutcome::Complete(reuse)) => {
                            Some(upstream.await.map(|upstream| (DownstreamRequestOutcome::Complete(reuse), upstream)))
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
        }

        match ret {
            None => {
                // The sibling upstream future was dropped mid-flight, so the request
                // stream is still open: reset it to stop the upstream from working on
                // a request nobody will read.
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
            Some(Err(e)) => {
                // On application level upstream read timeouts, send RST_STREAM CANCEL,
                // we know we have not received END_STREAM at this point since we read timed out
                // TODO: implement for write timeouts?
                if e.esource == ErrorSource::Upstream && matches!(e.etype, ReadTimedout) {
                    client_body.send_reset(h2::Reason::CANCEL);
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

    // returns whether server (downstream) session can be reused
    async fn bidirection_down_to_up(
        &self,
        session: &mut Session,
        client_body: &mut h2::SendStream<bytes::Bytes>,
        mut rx: mpsc::Receiver<HttpTask>,
        ctx: &mut SV::CTX,
        downstream_custom_message_writer: &mut Option<Box<dyn CustomMessageWrite>>,
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
            mut downstream_custom_message_reader,
        ) = if downstream_custom_message_writer.is_some() {
            let reader = session.downstream_custom_message()?;
            let (inject_tx, inject_rx) = mpsc::channel::<Bytes>(CUSTOM_MESSAGE_QUEUE_SIZE);
            (true, true, Some(inject_tx), Some(inject_rx), reader)
        } else {
            (false, false, None, None, None)
        };

        if let Some(custom_forwarding) = downstream_custom_message_custom_forwarding {
            self.inner
                .custom_forwarding(session, ctx, None, custom_forwarding)
                .await?;
        }

        let mut downstream_state = DownstreamStateMachine::new(session.as_mut().is_body_done());

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
        // SECOND end-of-stream event. Requiring both facts delivers exactly
        // one. The upstream EOS for exactly this shape already rode on the
        // HEADERS frame (or on the empty DATA frame), which is why
        // `body_write.stream_closed` suppresses the write side here.
        if buffer.is_some() || (session.as_mut().is_body_empty() && session.as_mut().is_body_done())
        {
            let outcome = self
                .send_body_to2(
                    session,
                    buffer,
                    downstream_state.is_done(),
                    client_body,
                    ctx,
                    body_write,
                )
                .await?;
            if outcome == DownstreamRequestOutcome::Terminate {
                // No-op for an H2 downstream; required for an H1 downstream proxied
                // to an H2 upstream, whose unread request bytes must not be drained
                // and the connection reused.
                session.set_keepalive(None);
                finish_terminated_response(session).await;
                restore_custom_message_reader(session, downstream_custom_message_reader);
                return Ok(outcome);
            }
        }

        let mut response_state = ResponseStateMachine::new();

        // these two below can be wrapped into an internal ctx
        // use cache when upstream revalidates (or TODO: error)
        let mut serve_from_cache = ServeFromCache::new();
        let mut range_body_filter = proxy_cache::range_filter::RangeBodyFilter::new();

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
                // end-of-stream event (invariant B): run the hooks with
                // `(None, end_of_stream = true)` exactly once.
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
                        true,
                        client_body,
                        ctx,
                        UpstreamBodyWrite {
                            eos_write_optional: true,
                            ..body_write
                        },
                    )
                    .await?;
                if outcome == DownstreamRequestOutcome::Terminate {
                    session.set_keepalive(None);
                    finish_terminated_response(session).await;
                    restore_custom_message_reader(session, downstream_custom_message_reader.take());
                    return Ok(outcome);
                }
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
                body = session.downstream_session.read_body_or_idle(downstream_state.is_done()), if downstream_state.can_poll() => {
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
                                warn!(
                                    "Downstream Error ignored during caching: {}, {}",
                                    e,
                                    self.inner.request_summary(session, ctx)
                                );
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
                    match self.send_body_to2(session, body, is_body_done, client_body, ctx, body_write).await {
                        Ok(DownstreamRequestOutcome::Complete(request_done)) =>  {
                            downstream_state.maybe_finished(request_done);
                        },
                        Ok(DownstreamRequestOutcome::Terminate) => {
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

                task = rx.recv(), if !response_state.upstream_done() => {
                    if let Some(t) = task {
                        debug!("upstream event: {:?}", t);
                        if serve_from_cache.should_discard_upstream() {
                            // just drain, do we need to do anything else?
                           continue;
                        }
                        // pull as many tasks as we can
                        let mut tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
                        tasks.push(t);
                        // tokio::task::unconstrained because now_or_never may yield None when the future is ready
                        while let Some(maybe_task) = tokio::task::unconstrained(rx.recv()).now_or_never() {
                            if let Some(t) = maybe_task {
                                tasks.push(t);
                            } else {
                                break
                            }
                        }

                        /* run filters before sending to downstream */
                        let mut filtered_tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
                        for mut t in tasks {
                            if self.revalidate_or_stale(session, &mut t, ctx).await {
                                serve_from_cache.enable();
                                response_state.enable_cached_response();
                                // skip downstream filtering entirely as the 304 will not be sent
                                break;
                            }
                            session.upstream_compression.response_filter(&mut t);
                            // check error and abort
                            // otherwise the error is surfaced via write_response_tasks()
                            if !serve_from_cache.should_send_to_downstream() {
                                if let HttpTask::Failed(e) = t {
                                    return Err(e);
                                }
                            }
                            filtered_tasks.push(
                                self.h2_response_filter(session, t, ctx,
                                    &mut serve_from_cache,
                                    &mut range_body_filter, false).await?);
                            if serve_from_cache.is_miss_header() {
                                response_state.enable_cached_response();
                            }
                        }

                        if !serve_from_cache.should_send_to_downstream() {
                            // TODO: need to derive response_done from filtered_tasks in case downstream failed already
                            continue;
                        }

                        let response_done = session.write_response_tasks(filtered_tasks).await?;
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
                }

                task = serve_from_cache.next_http_task(&mut session.cache, &mut range_body_filter, upgraded),
                    if !response_state.cached_done() && !downstream_state.is_errored() && serve_from_cache.is_on() => {
                    let task = self.h2_response_filter(session, task?, ctx,
                        &mut serve_from_cache,
                        &mut range_body_filter, true).await?;
                    debug!("serve_from_cache task {task:?}");

                    match session.write_response_tasks(vec![task]).await {
                        Ok(b) => response_state.maybe_set_cache_done(b),
                        Err(e) => if serve_from_cache.is_miss() {
                            // give up writing to downstream but wait for upstream cache write to finish
                            downstream_state.to_errored();
                            response_state.maybe_set_cache_done(true);
                            warn!(
                                "Downstream Error ignored during caching: {}, {}",
                                e,
                                self.inner.request_summary(session, ctx)
                            );
                            // This will not be treated as a final error, but we should signal to
                            // downstream session regardless
                            session.downstream_session.on_proxy_failure(e);
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                    if response_state.cached_done() {
                        if let Err(e) = session.cache.finish_hit_handler().await {
                            warn!("Error during finish_hit_handler: {}", e);
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

        restore_custom_message_reader(session, downstream_custom_message_reader);

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
        Ok(DownstreamRequestOutcome::Complete(reuse_downstream))
    }

    async fn h2_response_filter(
        &self,
        session: &mut Session,
        mut task: HttpTask,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
        range_body_filter: &mut RangeBodyFilter,
        from_cache: bool, // are the task from cache already
    ) -> Result<HttpTask>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        if !from_cache {
            if let Some(duration) = self.upstream_filter(session, &mut task, ctx).await? {
                trace!("delaying upstream response for {duration:?}");
                time::sleep(duration).await;
            }

            // cache the original response before any downstream transformation
            // requests that bypassed cache still need to run filters to see if the response has become cacheable
            if session.cache.enabled() || session.cache.bypassing() {
                if let Err(e) = self
                    .cache_http_task(session, &task, ctx, serve_from_cache)
                    .await
                {
                    session.cache.disable(NoCacheReason::StorageError);
                    if serve_from_cache.is_miss_body() {
                        // if the response stream cache body during miss but write fails, it has to
                        // give up the entire request
                        return Err(e);
                    } else {
                        // otherwise, continue processing the response
                        warn!(
                            "Fail to cache response: {}, {}",
                            e,
                            self.inner.request_summary(session, ctx)
                        );
                    }
                }
            }
            // skip the downstream filtering if these tasks are just for cache admission
            if !serve_from_cache.should_send_to_downstream() {
                return Ok(task);
            }
        } // else: cached/local response, no need to trigger upstream filters and caching

        // normally max file size is tracked in cache_http_task filters (when cache enabled),
        // we will track it in these filters before sending to downstream on specific conditions
        // when cache is disabled
        let track_max_cache_size = matches!(
            session.cache.phase(),
            CachePhase::Disabled(NoCacheReason::PredictedResponseTooLarge)
        );

        let res = match task {
            HttpTask::Header(mut header, eos) => {
                /* Downstream revalidation, only needed when cache is on because otherwise origin
                 * will handle it */
                if session.upstream_headers_mutated_for_cache() {
                    self.downstream_response_conditional_filter(
                        serve_from_cache,
                        session,
                        &mut header,
                        ctx,
                    );
                    if !session.ignore_downstream_range {
                        let range_type = self.inner.range_header_filter(session, &mut header, ctx);
                        range_body_filter.set(range_type);
                    }
                }

                self.inner
                    .response_filter(session, &mut header, ctx)
                    .await?;
                /* Downgrade the version so that write_response_header won't panic */
                header.set_version(Version::HTTP_11);

                // these status codes / method cannot have body, so no need to add chunked encoding
                let no_body = session.req_header().method == "HEAD"
                    || matches!(header.status.as_u16(), 204 | 304);

                /* Add chunked header to tell downstream to use chunked encoding
                 * during the absent of content-length in h2 */
                if !no_body
                    && !header.status.is_informational()
                    && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                {
                    header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                }
                Ok(HttpTask::Header(header, eos))
            }
            HttpTask::Body(data, eos) => {
                if track_max_cache_size {
                    session
                        .cache
                        .track_body_bytes_for_max_file_size(data.as_ref().map_or(0, |d| d.len()));
                }

                let mut data = range_body_filter.filter_body(data);
                if let Some(duration) = self
                    .inner
                    .response_body_filter(session, &mut data, eos, ctx)?
                {
                    trace!("delaying downstream response for {duration:?}");
                    time::sleep(duration).await;
                }
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
            HttpTask::Done => Ok(task),
            HttpTask::Failed(_) => Ok(task), // Do nothing just pass the error down
        };
        // On end, check if the response (based on file size) can be considered cacheable again
        if let Ok(task) = res.as_ref() {
            if track_max_cache_size
                && task.is_end()
                && !matches!(task, HttpTask::Failed(_))
                && !session.cache.exceeded_max_file_size()
            {
                session.cache.response_became_cacheable();
            }
        }
        res
    }

    async fn send_body_to2(
        &self,
        session: &mut Session,
        mut data: Option<Bytes>,
        end_of_body: bool,
        client_body: &mut h2::SendStream<bytes::Bytes>,
        ctx: &mut SV::CTX,
        body_write: UpstreamBodyWrite,
    ) -> Result<DownstreamRequestOutcome>
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
        let end_of_body = end_of_body || data.is_none();

        if data.is_none()
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
                return Ok(DownstreamRequestOutcome::Terminate);
            }
        }

        session
            .downstream_modules_ctx
            .request_body_filter(&mut data, end_of_body)
            .await?;

        if self
            .inner
            .request_body_filter_action(session, &mut data, end_of_body, ctx)
            .await?
            == RequestBodyAction::Terminate
        {
            warn_terminate_without_response(session, "request_body_filter_action");
            return Ok(DownstreamRequestOutcome::Terminate);
        }

        /* it is normal to get 0 bytes because of multi-chunk parsing or request_body_filter.
         * Although there is no harm writing empty byte to h2, unlike h1, we ignore it
         * for consistency */
        if !end_of_body && data.as_ref().is_some_and(|d| d.is_empty()) {
            return Ok(DownstreamRequestOutcome::Complete(false));
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
            return Ok(DownstreamRequestOutcome::Complete(end_of_body));
        }

        if let Some(data) = data {
            debug!("Write {} bytes body to h2 upstream", data.len());
            write_body(client_body, data, end_of_body, body_write.timeout)
                .await
                .map_err(|e| e.into_up())?;
        } else {
            debug!("Read downstream body done");
            /* send a standalone END_STREAM flag */
            if let Err(e) = write_body(client_body, Bytes::new(), true, body_write.timeout).await {
                if body_write.eos_write_optional {
                    debug!("upstream request stream would not take the final END_STREAM: {e}");
                } else {
                    return Err(e.into_up());
                }
            }
        }

        Ok(DownstreamRequestOutcome::Complete(end_of_body))
    }
}

/* Read response header, body and trailer from h2 upstream and send them to tx */
pub(crate) async fn pipe_up_to_down_response(
    client: &mut Http2Session,
    tx: mpsc::Sender<HttpTask>,
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

    while let Some(chunk) = client
        .read_response_body()
        .await
        .map_err(|e| e.into_up())
        .transpose()
    {
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
                let sent = tx
                    .send(HttpTask::Body(Some(data), eos))
                    .await
                    .or_err(InternalError, "sending h2 body to pipe");
                // If the if the response with content-length is sent to an HTTP1 downstream,
                // bidirection_down_to_up() could decide that the body has finished and exit without
                // waiting for this function to signal the eos. In this case tx being closed is not
                // an sign of error. It should happen if the only thing left for the h2 to send is
                // an empty data frame with eos set.
                if sent.is_err() && eos && empty {
                    return Ok(());
                }
                sent?;
            }
            Err(e) => {
                // Similar to above, push the error to downstream and then quit
                let _ = tx.send(HttpTask::Failed(e.into_up())).await;
                return Ok(());
            }
        }
    }

    // attempt to get trailers
    let trailers = match client.read_trailers().await {
        Ok(t) => t,
        Err(e) => {
            // Similar to above, push the error to downstream and then quit
            let _ = tx.send(HttpTask::Failed(e.into_up())).await;
            return Ok(());
        }
    };

    let trailers = trailers.map(Box::new);

    if trailers.is_some() {
        tx.send(HttpTask::Trailer(trailers))
            .await
            .or_err(InternalError, "sending h2 trailer to pipe")?;
    }

    tx.send(HttpTask::Done)
        .await
        .unwrap_or_else(|_| debug!("h2 to h1 channel closed!"));

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
