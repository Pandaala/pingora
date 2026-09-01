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
use crate::proxy_cache::ServeFromCache;
use crate::proxy_common::*;
use crate::pump_termination::{
    bound_undrained_downstream_body, downstream_body_read_is_futile,
    finalize_preserved_response_downstream_reuse, finish_terminated_response,
    join_bidirectional_pumps, release_cache_on_terminate,
    warn_response_body_terminate_content_length_leak,
    warn_response_body_terminate_without_response, DownstreamRequestOutcome, DuplexPumpOutcome,
};
use crate::request_relay::{
    safe_upstream_disposition, validate_streamed_upstream_disposition, AbandonmentResponsePolicy,
};
use crate::response_pipeline::{ResponsePipelineState, ResponseProtocol, ResponseTaskBatchOutcome};

#[path = "proxy_h2_request_body.rs"]
mod request_body;
use http::{header::CONTENT_LENGTH, Method, StatusCode};
use pingora_core::protocols::http::custom::CUSTOM_MESSAGE_QUEUE_SIZE;
use pingora_core::protocols::http::v2::client::Http2Session;
#[cfg(test)]
use pingora_core::protocols::http::v2::client::PeerEndStream;
use request_body::{
    apply_upstream_body_disposition, upstream_empty_data_end_stream, upstream_framing_body_empty,
    upstream_headers_end_stream, UpstreamBodyOutcome, UpstreamBodyWrite,
};
#[cfg(test)]
use request_body::{
    cancel_abandoned_upstream_body_capacity, effective_upstream_write_timeout,
    upstream_write_error_outcome, upstream_write_failed_because_stream_gone,
    upstream_write_stalled_after_response, DEFAULT_H2_UPSTREAM_WRITE_TIMEOUT,
};

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
        let disposition = session.frozen_request_relay_plan().requested.disposition;
        let body_empty_declared = session.as_mut().is_body_empty();
        // The H2 pump always sends HTTP/2 upstream, so there is no below-1.1
        // case here (unlike the H1 pump).
        if let Err(e) = validate_streamed_upstream_disposition(disposition, session, &req, false) {
            return (false, Some(e));
        }
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

        session.enable_request_relay_retry_buffer();

        // Shared signal so the upstream half can distinguish an expected task-pipe
        // closure (the downstream half finished and dropped rx) from an unexpected one.
        let pipe_state = Arc::new(AtomicU8::new(PipeState::Active as u8));

        /* read downstream body and upstream response at the same time */

        let mut response_pipeline = ResponsePipelineState::default();
        let ret = join_bidirectional_pumps(
            self.bidirection_down_to_up(
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
                &mut response_pipeline,
            ),
            pipe_up_to_down_response(client_session, tx, pipe_state),
        )
        .await;
        self.cancel_response_head_hold(session, ctx, &mut response_pipeline);

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
            DuplexPumpOutcome::ApplicationTerminate { upstream: None } => {
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
            DuplexPumpOutcome::ApplicationTerminate { upstream: Some(()) } => {
                // The upstream half completed cleanly before application
                // termination, so the stream already saw END_STREAM. Avoid a
                // gratuitous reset that some origins count toward H2 abuse
                // heuristics.
                release_cache_on_terminate(session);
                (false, None)
            }
            DuplexPumpOutcome::Complete {
                downstream_can_reuse,
                upstream: (),
            } => (downstream_can_reuse, None),
            DuplexPumpOutcome::OriginAbandoned {
                downstream_can_reuse,
            } => {
                // Invalidate before resetting; see `note_local_reset`.
                client_session.note_local_reset();
                client_body.send_reset(h2::Reason::CANCEL);
                (downstream_can_reuse, None)
            }
            DuplexPumpOutcome::Failed(e) => {
                let upstream_read_timeout =
                    e.esource == ErrorSource::Upstream && matches!(e.etype, ReadTimedout);
                let upstream_write_timeout =
                    e.esource == ErrorSource::Upstream && matches!(e.etype, WriteTimedout);
                let downstream_error = e.esource == ErrorSource::Downstream;
                // On upstream read/write timeouts, send RST_STREAM CANCEL: a
                // timeout that reaches this arm has no qualified END_STREAM
                // evidence and the exchange is being failed.
                // Also cancel the upstream stream when downstream goes away/resets so the
                // upstream peer can release the stream promptly.
                // Whether or not the explicit reset below is sent, this arm
                // abandons the upstream request stream: `client_body` is
                // dropped on return and `h2` cancels a still-open stream when
                // its last handle goes away. Source (iv) is given up either
                // way, so record it unconditionally -- and ahead of the reset,
                // because a record published in between could no longer be
                // retracted. See `Http2Session::note_local_reset`.
                client_session.note_local_reset();
                if upstream_read_timeout || upstream_write_timeout || downstream_error {
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
        response_state: &mut ResponseStateMachine,
        pipeline: &mut ResponsePipelineState,
    ) -> Result<Option<ResponseTaskBatchOutcome>>
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
        /* run filters before sending to downstream */
        let mut filtered_tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
        let mut source_done = false;
        pipeline.sink.reset_batch();
        let mut tasks = tasks.into_iter();
        for mut t in tasks.by_ref() {
            let task_source_done = t.is_end();
            if self
                .enforce_response_head_source_input(session, &t, ctx, pipeline, &mut filtered_tasks)
                .await?
            {
                break;
            }
            let revalidated = pipeline
                .wait_with_response_head_deadline(async {
                    Ok(self.revalidate_or_stale(session, &mut t, ctx).await)
                })
                .await
                .map_err(|error| {
                    self.resolve_response_head_wait_error(session, ctx, pipeline, error)
                })?;
            if revalidated {
                serve_from_cache.enable();
                response_state.enable_cached_response();
                // skip downstream filtering entirely as the 304 will not be sent
                break;
            }
            #[cfg(feature = "upstream_modules")]
            if let HttpTask::Header(header, end_of_stream) = &t {
                pipeline
                    .wait_with_response_head_deadline(self.inner.adjust_upstream_modules(
                        session,
                        header,
                        *end_of_stream,
                        ctx,
                    ))
                    .await
                    .map_err(|error| {
                        self.resolve_response_head_wait_error(session, ctx, pipeline, error)
                    })?;
            }
            #[cfg(feature = "upstream_modules")]
            pipeline
                .wait_with_response_head_deadline(session.upstream_modules_filter_task(&mut t))
                .await
                .map_err(|error| {
                    self.resolve_response_head_wait_error(session, ctx, pipeline, error)
                })?;
            let compression_prefix = session
                .upstream_compression
                .response_filter_with_preceding(&mut t);
            if let Some(prefix) = compression_prefix {
                self.response_task_pipeline(
                    ResponseProtocol::H2,
                    session,
                    prefix,
                    ctx,
                    serve_from_cache,
                    false,
                    pipeline,
                    &mut filtered_tasks,
                )
                .await?;
                if pipeline.origin_abandoned {
                    break;
                }
            }
            if !pipeline.sink.is_terminated() {
                self.response_task_pipeline(
                    ResponseProtocol::H2,
                    session,
                    t,
                    ctx,
                    serve_from_cache,
                    false,
                    pipeline,
                    &mut filtered_tasks,
                )
                .await?;
            }
            if pipeline.origin_abandoned {
                break;
            }
            source_done |= task_source_done;
            if serve_from_cache.is_miss_header() {
                response_state.enable_cached_response();
            }
            if pipeline.sink.is_terminated() {
                break;
            }
        }

        if pipeline.sink.is_terminated() {
            for dropped in tasks {
                if let HttpTask::Failed(e) = dropped {
                    abort_cache_after_response_source_failure(session, false);
                    warn!("dropping upstream error after response terminate: {e}");
                }
            }
        }

        if serve_from_cache.is_on() && pipeline.sink.is_terminated() {
            return Error::e_explain(
                InternalError,
                "response-body terminate is not supported while serving from a streaming cache readback",
            );
        }

        if !serve_from_cache.should_send_to_downstream() {
            // TODO: need to derive response_done from filtered_tasks in case downstream failed already
            return Ok(None);
        }

        if !filtered_tasks.is_empty() {
            session.write_response_tasks(filtered_tasks).await?;
        }

        if pipeline.origin_abandoned {
            abort_cache_after_response_source_failure(session, false);
            Ok(Some(ResponseTaskBatchOutcome::OriginAbandoned))
        } else {
            Ok(Some(ResponseTaskBatchOutcome::Progress {
                source_done,
                terminated: pipeline.sink.is_terminated() && !source_done,
            }))
        }
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
        response_pipeline: &mut ResponsePipelineState,
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

        let buffer = session.request_relay_retry_buffer();
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
                    AbandonmentResponsePolicy::Abort,
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
                UpstreamBodyOutcome::Downstream(
                    DownstreamRequestOutcome::AbortSelectedResponse,
                ) => return Ok(DownstreamRequestOutcome::AbortSelectedResponse),
                // The replayed body could not be written because the upstream
                // had already answered in full and reset the stream. Failing the
                // request here would discard that response; the duplex loop
                // below still has to run to deliver it.
                UpstreamBodyOutcome::UpstreamDoneReceiving {
                    terminal_event_delivered,
                } => {
                    if !terminal_event_delivered {
                        match self
                            .finish_downstream_body_side(session, client_body, ctx, &body_write)
                            .await?
                        {
                            DownstreamRequestOutcome::Terminate => {
                                session.set_keepalive(None);
                                finish_terminated_response(session).await;
                                restore_custom_message_reader(
                                    session,
                                    downstream_custom_message_reader.take(),
                                );
                                return Ok(DownstreamRequestOutcome::Terminate);
                            }
                            DownstreamRequestOutcome::AbortSelectedResponse => {
                                return Ok(DownstreamRequestOutcome::AbortSelectedResponse);
                            }
                            DownstreamRequestOutcome::Complete(_)
                            | DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(_) => {}
                        }
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
        // Shared across every batch drained from upstream for this response;
        // the per-batch byte budget is reset at each batch boundary (see
        // `ResponseBodySink::reset_batch`), but a `terminate()` signal stays
        // sticky for the rest of this response.
        // Also shared across every batch: `Trailer` and the `Done` behind it
        // can land in different batches, so the latch that keeps the terminal
        // body callback to exactly one delivery must outlive a single batch.
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
                        AbandonmentResponsePolicy::PreserveSelected,
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
                if outcome
                    == UpstreamBodyOutcome::Downstream(
                        DownstreamRequestOutcome::AbortSelectedResponse,
                    )
                {
                    return Ok(DownstreamRequestOutcome::AbortSelectedResponse);
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
            let response_head_timeout: OptionFuture<_> = response_pipeline
                .response_head_deadline()
                .map(tokio::time::sleep_until)
                .into();

            // partial read support, this check will also be false if cache is disabled.
            let support_cache_partial_read =
                session.cache.support_streaming_partial_write() == Some(true);
            let upgraded = session.was_upgraded();

            // Similar logic in h1 need to reserve capacity first to avoid deadlock
            // But we don't need to do the same because the h2 client_body pipe is unbounded (never block)
            tokio::select! {
                Some(()) = response_head_timeout => {
                    return Err(self.resolve_response_head_idle_timeout(
                        session,
                        ctx,
                        response_pipeline,
                    ));
                },

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
                            AbandonmentResponsePolicy::Abort,
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
                            if !terminal_event_delivered {
                                match self
                                    .finish_downstream_body_side(
                                        session,
                                        client_body,
                                        ctx,
                                        &body_write,
                                    )
                                    .await?
                                {
                                    DownstreamRequestOutcome::Terminate => {
                                        session.set_keepalive(None);
                                        finish_terminated_response(session).await;
                                        restore_custom_message_reader(
                                            session,
                                            downstream_custom_message_reader.take(),
                                        );
                                        return Ok(DownstreamRequestOutcome::Terminate);
                                    }
                                    DownstreamRequestOutcome::AbortSelectedResponse => {
                                        return Ok(
                                            DownstreamRequestOutcome::AbortSelectedResponse,
                                        );
                                    }
                                    DownstreamRequestOutcome::Complete(_)
                                    | DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(_) => {}
                                }
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
                        Ok(UpstreamBodyOutcome::Downstream(
                            DownstreamRequestOutcome::AbortSelectedResponse,
                        )) => return Ok(DownstreamRequestOutcome::AbortSelectedResponse),
                        Err(e)
                            if e.esource == ErrorSource::Upstream
                                && matches!(e.etype, WriteTimedout) =>
                        {
                            // This is the bounded failure policy when the peer
                            // has NOT flagged its response complete. Waiting
                            // for the read half here would defeat the write
                            // deadline if that peer also withholds response
                            // END_STREAM.
                            //
                            // The qualified END_STREAM case never reaches this
                            // arm: `upstream_write_error_outcome` converts it
                            // to `UpstreamDoneReceiving` above so the complete
                            // response can still be delivered.
                            return Err(e);
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
                        let Some(batch_outcome) = self.process_upstream_tasks_h2(
                            session,
                            ctx,
                            t,
                            &mut rx,
                            &mut serve_from_cache,
                            &mut response_state,
                            response_pipeline,
                        ).await? else {
                            // nothing sent downstream e.g. serve_from_cache
                            continue;
                        };
                        let response_done = match batch_outcome {
                            ResponseTaskBatchOutcome::Progress { source_done, terminated } => {
                                if terminated {
                                    session.set_keepalive(None);
                                    warn_response_body_terminate_without_response(session, "upstream_response_body_filter");
                                    warn_response_body_terminate_content_length_leak(session, "upstream_response_body_filter");
                                    finish_terminated_response(session).await;
                                    restore_custom_message_reader(session, downstream_custom_message_reader.take());
                                    return Ok(DownstreamRequestOutcome::Terminate);
                                }
                                source_done
                            }
                            ResponseTaskBatchOutcome::OriginAbandoned => true,
                        };
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
                        let Some(batch_outcome) = self.process_upstream_tasks_h2(
                            session,
                            ctx,
                            t,
                            &mut rx,
                            &mut serve_from_cache,
                            &mut response_state,
                            response_pipeline,
                        ).await? else {
                            // nothing sent downstream e.g. serve_from_cache
                            continue;
                        };
                        let response_done = match batch_outcome {
                            ResponseTaskBatchOutcome::Progress { source_done, terminated } => {
                                if terminated {
                                    session.set_keepalive(None);
                                    warn_response_body_terminate_without_response(session, "upstream_response_body_filter");
                                    warn_response_body_terminate_content_length_leak(session, "upstream_response_body_filter");
                                    finish_terminated_response(session).await;
                                    restore_custom_message_reader(session, downstream_custom_message_reader.take());
                                    return Ok(DownstreamRequestOutcome::Terminate);
                                }
                                source_done
                            }
                            ResponseTaskBatchOutcome::OriginAbandoned => true,
                        };
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

                task = serve_from_cache.next_http_task(
                    &mut session.cache,
                    &mut response_pipeline.range_body_filter,
                    upgraded,
                ),
                    if !response_state.cached_done()
                        && !downstream_state.is_errored()
                        && serve_from_cache.is_on()
                        && !session.has_pending_downstream_tasks() => { // backpressure: don't queue if pending writes

                    let task = task?;
                    let cache_source_done = task.is_end();
                    let mut cached_tasks = Vec::with_capacity(1);
                    self.response_task_pipeline(ResponseProtocol::H2, session, task, ctx,
                        &mut serve_from_cache,
                        true, response_pipeline, &mut cached_tasks).await?;
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
        reuse_downstream =
            finalize_preserved_response_downstream_reuse(session, reuse_downstream).await;
        // Signal the upstream half that the downstream half completed cleanly before
        // dropping rx, so a resulting task-pipe closure is treated as benign.
        pipe_state.store(PipeState::DownstreamComplete as u8, Ordering::Release);
        Ok(if response_pipeline.upstream_reusable {
            DownstreamRequestOutcome::Complete(reuse_downstream)
        } else {
            DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(reuse_downstream)
        })
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

#[cfg(test)]
include!("proxy_h2_request_body_tests.rs");

#[cfg(test)]
#[path = "proxy_h2_response_batch_tests.rs"]
mod response_batch_tests;
