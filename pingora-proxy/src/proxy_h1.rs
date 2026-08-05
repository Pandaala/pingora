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
use crate::proxy_cache::{drain_emitted_chunks, range_filter::RangeBodyFilter, ServeFromCache};
use crate::proxy_common::*;
use pingora_cache::CachePhase;
use pingora_core::protocols::http::custom::CUSTOM_MESSAGE_QUEUE_SIZE;

fn apply_upstream_body_disposition(
    request: &mut RequestHeader,
    disposition: UpstreamRequestBodyDisposition,
) -> Result<()> {
    match disposition {
        UpstreamRequestBodyDisposition::Ordinary => {}
        UpstreamRequestBodyDisposition::Bodyless => {
            request.remove_header(&header::CONTENT_LENGTH);
            request.remove_header(&header::TRANSFER_ENCODING);
        }
        UpstreamRequestBodyDisposition::Streamed => {
            // An H1 request with neither Content-Length nor
            // Transfer-Encoding is a zero-length body, so removal alone
            // would be a correctness bug.
            let transfer_encoding =
                streamed_transfer_encoding(request.headers.get_all(&header::TRANSFER_ENCODING));
            request.remove_header(&header::CONTENT_LENGTH);
            request.remove_header(&header::TRANSFER_ENCODING);
            request.insert_header(header::TRANSFER_ENCODING, transfer_encoding)?;
        }
    }
    Ok(())
}

/// Build the `Transfer-Encoding` value for a `Streamed` upstream request.
///
/// Any content coding the client applied (`Transfer-Encoding: gzip, chunked`)
/// still describes the bytes the proxy is about to forward, so dropping it
/// while keeping the coded bytes would corrupt the message. Only the
/// `chunked` token is re-derived: it is re-appended last, as RFC 9112 §6.1
/// requires.
fn streamed_transfer_encoding<'a>(
    values: impl IntoIterator<Item = &'a http::HeaderValue>,
) -> String {
    let mut codings: Vec<&str> = Vec::new();
    for value in values {
        // A non-ASCII Transfer-Encoding value is malformed; there is no
        // coding to preserve from it.
        let Ok(value) = value.to_str() else {
            continue;
        };
        for token in value.split(',') {
            let token = token.trim();
            if token.is_empty() || token.eq_ignore_ascii_case("chunked") {
                continue;
            }
            codings.push(token);
        }
    }
    codings.push("chunked");
    codings.join(", ")
}

impl<SV, C> HttpProxy<SV, C>
where
    C: custom::Connector,
{
    pub(crate) async fn proxy_1to1(
        &self,
        session: &mut Session,
        client_session: &mut HttpSessionV1,
        peer: &HttpPeer,
        ctx: &mut SV::CTX,
    ) -> (bool, bool, Option<Box<Error>>)
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        client_session.read_timeout = peer.options.read_timeout;
        client_session.write_timeout = peer.options.write_timeout;

        // phase 2 send to upstream

        let mut req = session.req_header().clone();

        // Convert HTTP2 headers to H1
        if req.version == Version::HTTP_2 {
            req.set_version(Version::HTTP_11);
            // if client has body but has no content length, add chunked encoding
            // https://datatracker.ietf.org/doc/html/rfc9112#name-message-body
            // "The presence of a message body in a request is signaled by a Content-Length or Transfer-Encoding header field."
            if !session.is_body_empty() && session.get_header(header::CONTENT_LENGTH).is_none() {
                req.insert_header(header::TRANSFER_ENCODING, "chunked")
                    .unwrap();
            }
            if session.get_header(header::HOST).is_none() {
                // H2 is required to set :authority, but no necessarily header
                // most H1 server expect host header, so convert
                let host = req.uri.authority().map_or("", |a| a.as_str()).to_owned();
                req.insert_header(header::HOST, host).unwrap();
            }
            // TODO: Add keepalive header for connection reuse, but this is not required per RFC
        }

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
                return (false, true, Some(e));
            }
        }

        // The disposition is resolved AFTER `begin_request_body_replay()`,
        // because a registered replay buffer changes the "does this request
        // have a body" fact the coercion below depends on.
        if let Err(e) = session.as_mut().begin_request_body_replay().await {
            return (false, true, Some(e));
        }

        // Facts are collected inside `safe_upstream_disposition`, and only
        // when `disposition` is non-`Ordinary` -- see its doc comment for why
        // that is sound.
        let disposition = self.inner.upstream_request_body_disposition(session, ctx);
        // Only the H1 pump can end up sending a request below HTTP/1.1, which
        // has no chunked framing at all.
        let upstream_below_http11 = matches!(req.version, Version::HTTP_09 | Version::HTTP_10);
        let body_disposition =
            safe_upstream_disposition(disposition, session, &req, upstream_below_http11);
        if let Err(e) = apply_upstream_body_disposition(&mut req, body_disposition) {
            return (false, true, Some(e));
        }

        session.upstream_compression.request_filter(&req);

        debug!("Sending header to upstream {:?}", req);

        match client_session.write_request_header(Box::new(req)).await {
            Ok(_) => { /* Continue */ }
            Err(e) => {
                return (false, false, Some(e.into_up()));
            }
        }

        let mut downstream_custom_message_writer = session
            .downstream_session
            .as_custom_mut()
            .and_then(|c| c.take_custom_message_writer());

        let (tx_upstream, rx_upstream) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);
        let (tx_downstream, rx_downstream) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);

        if self.inner.request_retry_allowed(session, ctx) {
            session.as_mut().enable_retry_buffering();
        }

        // start bi-directional streaming
        let ret = {
            let downstream = self.proxy_handle_downstream(
                session,
                tx_downstream,
                rx_upstream,
                ctx,
                &mut downstream_custom_message_writer,
                body_disposition,
            );
            let upstream = self.proxy_handle_upstream(client_session, tx_upstream, rx_downstream);
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
                        return (false, false, Some(e));
                    }
                }
            }
        }

        match ret {
            None | Some(Ok((DownstreamRequestOutcome::Terminate, _))) => {
                release_cache_on_terminate(session);
                (false, false, None)
            }
            Some(Ok((DownstreamRequestOutcome::Complete(downstream_can_reuse), _upstream))) => {
                (downstream_can_reuse, true, None)
            }
            Some(Err(e)) => (false, false, Some(e)),
        }
    }

    pub(crate) async fn proxy_to_h1_upstream(
        &self,
        session: &mut Session,
        client_session: &mut HttpSessionV1,
        reused: bool,
        peer: &HttpPeer,
        ctx: &mut SV::CTX,
    ) -> (bool, bool, Option<Box<Error>>)
    // (reuse_server, reuse_client, error)
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        #[cfg(windows)]
        let raw = client_session.id() as std::os::windows::io::RawSocket;
        #[cfg(unix)]
        let raw = client_session.id();

        let initial_write_pending = client_session.stream().get_write_pending_time();

        if let Err(e) = self
            .inner
            .connected_to_upstream(
                session,
                reused,
                peer,
                raw,
                Some(client_session.digest()),
                ctx,
            )
            .await
        {
            return (false, false, Some(e));
        }

        let (server_session_reuse, client_session_reuse, error) =
            self.proxy_1to1(session, client_session, peer, ctx).await;

        // Record upstream response body bytes received (payload only) for logging consumers.
        let upstream_bytes_total = client_session.body_bytes_received();
        session.set_upstream_body_bytes_received(upstream_bytes_total);

        // Record upstream write pending time for this session only (delta from baseline).
        let current_write_pending = client_session.stream().get_write_pending_time();
        let upstream_write_pending = current_write_pending.saturating_sub(initial_write_pending);
        session.set_upstream_write_pending_time(upstream_write_pending);

        (server_session_reuse, client_session_reuse, error)
    }

    async fn proxy_handle_upstream(
        &self,
        client_session: &mut HttpSessionV1,
        tx: mpsc::Sender<HttpTask>,
        mut rx: mpsc::Receiver<HttpTask>,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let mut request_done = false;
        let mut response_done = false;
        let mut send_error = None;
        let mut upgraded = false;

        /* duplex mode, wait for either to complete */
        while !request_done || !response_done {
            tokio::select! {
                res = client_session.read_response_task(), if !response_done => {
                    match res {
                        Ok(task) => {
                            response_done = task.is_end();
                            if !upgraded && client_session.was_upgraded() {
                                // upgrade can only happen once
                                upgraded = true;
                                if send_error.is_none() {
                                    // continue receiving from downstream after body mode change
                                    request_done = false;
                                }
                            }
                            let type_str = task.type_str();
                            let result = tx.send(task)
                                .await.or_err_with(
                                    InternalError,
                                    || format!("Failed to send upstream task {type_str}{} to pipe",
                                        if response_done { " (end)" } else {""})
                                );
                            // If the request is upgraded, the downstream pipe can early exit
                            // when the downstream connection is closed.
                            // In that case, this function should ignore that the pipe is closed.
                            // So that this function could read the rest events from rx including
                            // the closure, then exit.
                            if result.is_err() && !client_session.was_upgraded() {
                                return result;
                            }
                        },
                        Err(e) => {
                            // Push the error to downstream and then quit
                            // Don't care if send fails: downstream already gone
                            let _ = tx.send(HttpTask::Failed(send_error.unwrap_or(e).into_up())).await;
                            // Downstream should consume all remaining data and handle the error
                            return Ok(())
                        }
                    }
                },

                body = rx.recv(), if !request_done => {
                    match send_body_to1(client_session, body).await {
                        Ok(send_done) => {
                            request_done = send_done;
                            // An upgraded request is terminated when either side is done
                            if request_done && client_session.was_upgraded() {
                                response_done = true;
                            }
                        },
                        Err(e) => {
                           warn!("send error, draining read buf: {e}");
                           request_done = true;

                           send_error = Some(e);
                           continue
                        }
                    }
                },

                else => {
                    // this shouldn't be reached as the while loop would already exit
                    break;
                }
            }
        }

        Ok(())
    }

    // todo use this function to replace bidirection_1to2()
    // returns whether this server (downstream) session can be reused
    async fn proxy_handle_downstream(
        &self,
        session: &mut Session,
        tx: mpsc::Sender<HttpTask>,
        mut rx: mpsc::Receiver<HttpTask>,
        ctx: &mut SV::CTX,
        downstream_custom_message_writer: &mut Option<Box<dyn CustomMessageWrite>>,
        body_disposition: UpstreamRequestBodyDisposition,
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

        let buffer = session.as_ref().get_retry_buffer();
        // Native retry-buffer path. Registered app buffers are replayed through
        // `read_body_or_idle()` below, one bounded chunk at a time.
        //
        // The bodyless prelude fires one immediate `(None, end)` body event. It
        // must therefore require the transport fact (`is_body_done()`) and not
        // just `is_body_empty()`, which still infers emptiness from
        // `Content-Length: 0`: an H2 downstream request declaring `Content-Length: 0`
        // without END_STREAM is not bodyless (design 4.3), so the loop below reads
        // on to the real EOS and would deliver a SECOND end-of-stream event to
        // `request_body_filter`. Requiring both facts delivers exactly one.
        if buffer.is_some() || (session.as_mut().is_body_empty() && session.as_mut().is_body_done())
        {
            let send_permit = tx
                .reserve()
                .await
                .or_err(InternalError, "reserving body pipe")?;
            let outcome = self
                .send_body_to_pipe(
                    session,
                    buffer,
                    downstream_state.is_done(),
                    Some(send_permit),
                    ctx,
                    body_disposition,
                )
                .await?;
            if outcome == DownstreamRequestOutcome::Terminate {
                session.set_keepalive(None);
                finish_terminated_response(session).await;
                restore_custom_message_reader(session, downstream_custom_message_reader);
                return Ok(outcome);
            }
        }

        let mut response_state = ResponseStateMachine::new();

        // these two below can be wrapped into an internal ctx
        // use cache when upstream revalidates (or TODO: error)
        let mut serve_from_cache = proxy_cache::ServeFromCache::new();
        let mut range_body_filter = proxy_cache::range_filter::RangeBodyFilter::new();
        // Shared across every batch drained from upstream for this response;
        // the per-batch byte budget is reset at each batch boundary (see
        // `ResponseBodySink::reset_batch`), but a `terminate()` signal stays
        // sticky for the rest of this response.
        let mut sink = ResponseBodySink::new();

        /* duplex mode without caching
         * Read body from downstream while reading response from upstream
         * If response is done, only read body from downstream
         * If request is done, read response from upstream while idling downstream (to close quickly)
         * If both are done, quit the loop
         *
         * With caching + but without partial read support
         * Similar to above, cache admission write happen when the data is write to downstream
         *
         * With caching + partial read support
         * A. Read upstream response and write to cache
         * B. Read data from cache and send to downstream
         * If B fails (usually downstream close), continue A.
         * If A fails, exit with error.
         * If both are done, quit the loop
         * Usually there is no request body to read for cacheable request
         */
        while !downstream_state.is_done()
            || !response_state.is_done()
            || downstream_custom_read && !downstream_state.is_errored()
            || downstream_custom_write
        {
            if downstream_body_read_is_futile(session, &downstream_state, &response_state) {
                // Abandoning the read must not cost the application its single
                // end-of-stream event (invariant B): run the hooks with
                // `(None, end_of_stream = true)` exactly once, with NO permit
                // so nothing reaches the upstream -- the upstream exchange is
                // already complete and owes no further request framing.
                let outcome = self
                    .send_body_to_pipe(session, None, true, None, ctx, body_disposition)
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

            // reserve tx capacity ahead to avoid deadlock, see below

            let send_permit = tx
                .try_reserve()
                .or_err(InternalError, "try_reserve() body pipe for upstream");

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

            tokio::select! {
                // only try to send to pipe if there is capacity to avoid deadlock
                // Otherwise deadlock could happen if both upstream and downstream are blocked
                // on sending to their corresponding pipes which are both full.
                body = session.downstream_session.read_body_or_idle(downstream_state.is_done()),
                    if downstream_state.can_poll() && send_permit.is_ok() => {

                    debug!("downstream event");
                    let body = match body {
                        Ok(b) => b,
                        Err(e) => {
                            if session.downstream_session.request_body_buffer_replaying() {
                                // The error came from the registered request body buffer
                                // (replay path), not the client connection: a gateway-local
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
                    // If the request is websocket, `None` body means the request is closed.
                    // Set the response to be done as well so that the request completes normally.
                    if body.is_none() && session.was_upgraded() {
                        response_state.maybe_set_upstream_done(true);
                    }
                    // TODO: consider just drain this if serve_from_cache is set
                    let is_body_done = session.is_body_done();
                    let outcome = self.send_body_to_pipe(
                        session,
                        body,
                        is_body_done,
                        Some(send_permit.unwrap()), // safe because we checked is_ok()
                        ctx,
                        body_disposition,
                    )
                    .await?;
                    if outcome == DownstreamRequestOutcome::Terminate {
                        session.set_keepalive(None);
                        finish_terminated_response(session).await;
                        restore_custom_message_reader(session, downstream_custom_message_reader.take());
                        return Ok(outcome);
                    }
                    let DownstreamRequestOutcome::Complete(request_done) = outcome else {
                        unreachable!("terminal request-body outcome returned above");
                    };
                    downstream_state.maybe_finished(request_done);
                },

                _ = tx.reserve(), if downstream_state.is_reading() && send_permit.is_err() => {
                    // If tx is closed, the upstream has already finished its job.
                    downstream_state.maybe_finished(tx.is_closed());
                    debug!("waiting for permit {send_permit:?}, upstream closed {}", tx.is_closed());
                    /* No permit, wait on more capacity to avoid starving.
                     * Otherwise this select only blocks on rx, which might send no data
                     * before the entire body is uploaded.
                     * once more capacity arrives we just loop back
                     */
                },

                task = rx.recv(), if !response_state.upstream_done() => {
                    debug!("upstream event: {:?}", task);
                    if let Some(t) = task {
                        if serve_from_cache.should_discard_upstream() {
                            // just drain, do we need to do anything else?
                           continue;
                        }
                        // pull as many tasks as we can
                        let mut tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
                        tasks.push(t);
                        // tokio::task::unconstrained because now_or_never may yield None when the future is ready
                        while let Some(maybe_task) = tokio::task::unconstrained(rx.recv()).now_or_never() {
                            debug!("upstream event now: {:?}", maybe_task);
                            if let Some(t) = maybe_task {
                                tasks.push(t);
                            } else {
                                break; // upstream closed
                            }
                        }

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
                            session.upstream_compression.response_filter(&mut t);
                            // check error and abort: the error is surfaced via write_response_tasks()
                            // otherwise; the `should_send_to_downstream() == false` && Failed case is
                            // handled inside h1_response_filter itself, on the same post-cache state.
                            self.h1_response_filter(session, t, ctx,
                                &mut serve_from_cache,
                                &mut range_body_filter, false,
                                &mut sink, &mut filtered_tasks).await?;
                            if serve_from_cache.is_miss_header() {
                                response_state.enable_cached_response();
                            }
                            if sink.is_terminated() {
                                // Stop draining this batch right here: any
                                // task still left in `tasks` is upstream data
                                // that arrived (or was pulled) after the
                                // filter decided to end the response, and
                                // must never reach `filtered_tasks` -- only
                                // chunks the filter itself queued into `sink`
                                // for the task that just ran may still be
                                // delivered, and those are already captured
                                // (`h1_response_filter` drains them into
                                // `filtered_tasks` before returning). Without
                                // this, a terminate on chunk 1 of a
                                // same-batch multi-chunk pull would still let
                                // chunks 2..N -- origin bytes the filter
                                // never saw -- through to the client below.
                                break;
                            }
                        }

                        if serve_from_cache.is_on() && sink.is_terminated() {
                            // `is_on()` here can only be a miss-admission state
                            // (a genuine cache hit never runs
                            // `upstream_response_body_filter`, the only place
                            // that can call `sink.terminate()`), meaning this
                            // response is about to be served back to the
                            // client through the `serve_from_cache` arm below
                            // rather than the direct write a few lines down --
                            // a streaming-partial-write cache backend (this
                            // fork's `MemCache` always is one) decouples that
                            // readback from this admission pass, so by the
                            // time this pump reacts to the terminate signal
                            // the readback may already have delivered bytes
                            // that arrived after the terminate point, or the
                            // cache admission may already have committed a
                            // truncated entry as if it were complete -- see
                            // the task's design notes for the full trace.
                            // Terminate's use case (ending a non-cacheable,
                            // streamed response early) has no legitimate
                            // reason to combine with a cache-streaming
                            // readback, so this combination is unsupported:
                            // fail closed instead of risking either defect.
                            return Error::e_explain(
                                InternalError,
                                "response-body terminate is not supported while serving from a streaming cache readback",
                            );
                        }

                        if !serve_from_cache.should_send_to_downstream() {
                            // TODO: need to derive response_done from filtered_tasks in case downstream failed already
                            continue;
                        }

                        // set to downstream
                        let upgraded = session.was_upgraded();
                        let response_done = session.write_response_tasks(filtered_tasks).await?;
                        if sink.is_terminated() {
                            // The filter ended the response. `serve_from_cache`
                            // is not "on" here (the check above returns before
                            // this point otherwise), so this batch is the one
                            // actually written downstream. Finish the body so
                            // an H1 Content-Length-framed response is flushed,
                            // then hand back the typed terminate outcome: the
                            // biased select drops the upstream future
                            // (stopping the read), and the Terminate arm
                            // releases the cache and marks both connections
                            // non-reusable. Mirrors the wrap-up the
                            // request-body terminate arms of this same loop
                            // already perform below.
                            session.set_keepalive(None);
                            warn_response_body_terminate_without_response(session, "upstream_response_body_filter");
                            warn_response_body_terminate_content_length_leak(session, "upstream_response_body_filter");
                            finish_terminated_response(session).await;
                            restore_custom_message_reader(
                                session,
                                downstream_custom_message_reader.take(),
                            );
                            return Ok(DownstreamRequestOutcome::Terminate);
                        }
                        if !upgraded && session.was_upgraded() && downstream_state.can_poll() {
                            // just upgraded, the downstream state should be reset to continue to
                            // poll body
                            trace!("reset downstream state on upgrade");
                            downstream_state.reset();
                        }
                        response_state.maybe_set_upstream_done(response_done);
                        // unsuccessful upgrade response (or end of upstream upgraded conn,
                        // which forces the body reader to complete) may force the request done
                        downstream_state.maybe_finished(session.is_body_done());
                    } else {
                        debug!("empty upstream event");
                        response_state.maybe_set_upstream_done(true);
                    }
                },

                task = serve_from_cache.next_http_task(&mut session.cache, &mut range_body_filter, upgraded),
                    if !response_state.cached_done() && !downstream_state.is_errored() && serve_from_cache.is_on() => {

                    let mut cached_tasks = Vec::with_capacity(1);
                    self.h1_response_filter(session, task?, ctx,
                        &mut serve_from_cache,
                        &mut range_body_filter, true,
                        &mut sink, &mut cached_tasks).await?;
                    debug!("serve_from_cache task {cached_tasks:?}");

                    match session.write_response_tasks(cached_tasks).await {
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

    async fn h1_response_filter(
        &self,
        session: &mut Session,
        mut task: HttpTask,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
        range_body_filter: &mut RangeBodyFilter,
        from_cache: bool, // are the task from cache already
        sink: &mut ResponseBodySink,
        out_tasks: &mut Vec<HttpTask>,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        // skip caching if already served from cache
        if !from_cache {
            if let Some(duration) = self.upstream_filter(session, &mut task, sink, ctx).await? {
                trace!("delaying upstream response for {duration:?}");
                time::sleep(duration).await;
            }

            // Cache the original response (and anything the upstream body
            // filter queued in `sink` after it) before any downstream
            // transformation. Requests that bypassed cache still need to run
            // filters to see if the response has become cacheable.
            self.cache_task_and_emitted_chunks(session, &task, sink, ctx, serve_from_cache)
                .await?;

            if !serve_from_cache.should_send_to_downstream() {
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
                // The pump used to check this same condition against the task
                // this call returned, right after the call; since the flag
                // cannot change again for a Failed task past this point (the
                // Failed arm below is a pure passthrough), checking it here,
                // on the same post-cache state, is equivalent and lets the
                // pump stay a simple `.await?` instead of inspecting what was
                // just pushed.
                if let HttpTask::Failed(e) = task {
                    return Err(e);
                }
                out_tasks.push(task);
                return Ok(());
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
            HttpTask::Header(mut header, end) => {
                /* Downstream revalidation/range, only needed when cache modified headers because otherwise origin
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

                // TODO: just set version to Version::HTTP_11 unconditionally here,
                // (with another todo being an option to faithfully proxy the <1.1 responses)
                // as we are already trying to mutate this for HTTP/1.1 downstream reuse

                /* Convert HTTP 1.0 style response to chunked encoding so that we don't
                 * have to close the downstream connection */
                // these status codes / method cannot have body, so no need to add chunked encoding
                let no_body = session.req_header().method == http::method::Method::HEAD
                    || matches!(header.status.as_u16(), 204 | 304);
                if !no_body
                    && !header.status.is_informational()
                    && header
                        .headers
                        .get(http::header::TRANSFER_ENCODING)
                        .is_none()
                    && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                    && !end
                {
                    // Upgrade the http version to 1.1 because 1.0/0.9 doesn't support chunked
                    header.set_version(Version::HTTP_11);
                    header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                }

                match self.inner.response_filter(session, &mut header, ctx).await {
                    Ok(_) => Ok(HttpTask::Header(header, end)),
                    Err(e) => Err(e),
                }
            }
            HttpTask::Body(data, end) => {
                if track_max_cache_size {
                    session
                        .cache
                        .track_body_bytes_for_max_file_size(data.as_ref().map_or(0, |d| d.len()));
                }

                // before it can mark it as cacheable again.
                let mut data = range_body_filter.filter_body(data);
                if let Some(duration) = self
                    .inner
                    .response_body_filter(session, &mut data, end, ctx)?
                {
                    trace!("delaying downstream response for {:?}", duration);
                    time::sleep(duration).await;
                }

                Ok(HttpTask::Body(data, end))
            }
            HttpTask::UpgradedBody(mut data, end) => {
                if track_max_cache_size {
                    session
                        .cache
                        .track_body_bytes_for_max_file_size(data.as_ref().map_or(0, |d| d.len()));
                }

                // range doesn't apply to upgraded body
                if let Some(duration) = self
                    .inner
                    .response_body_filter(session, &mut data, end, ctx)?
                {
                    trace!("delaying downstream upgraded response for {:?}", duration);
                    time::sleep(duration).await;
                }

                Ok(HttpTask::UpgradedBody(data, end))
            }
            HttpTask::Trailer(h) => Ok(HttpTask::Trailer(h)), // TODO: support trailers for h1
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
        let task = res?;
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
        } else {
            // Extra chunks emitted by the upstream body filter follow the
            // chunk they were emitted from, preserving order; `task`'s own
            // end-of-stream flag migrates onto the last of them when there
            // are any (see `drain_emitted_chunks`).
            drain_emitted_chunks(task, sink, out_tasks);
        }
        Ok(())
    }

    // TODO:: use this function to replace send_body_to2
    async fn send_body_to_pipe(
        &self,
        session: &mut Session,
        mut data: Option<Bytes>,
        end_of_body: bool,
        tx: Option<mpsc::Permit<'_, HttpTask>>,
        ctx: &mut SV::CTX,
        body_disposition: UpstreamRequestBodyDisposition,
    ) -> Result<DownstreamRequestOutcome>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        // None: end of body
        // this var is to signal if downstream finish sending the body, which shouldn't be
        // affected by the request_body_filter
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

        // TODO: request body filter to have info about upgraded status?
        // (can also check session.was_upgraded())
        if self
            .inner
            .request_body_filter_action(session, &mut data, end_of_body, ctx)
            .await?
            == RequestBodyAction::Terminate
        {
            warn_terminate_without_response(session, "request_body_filter_action");
            return Ok(DownstreamRequestOutcome::Terminate);
        }

        // the flag to signal to upstream
        let upstream_end_of_body = end_of_body || data.is_none();

        /* It is normal to get 0 bytes because of multi-chunk or request_body_filter decides not to
         * output anything yet.
         * Don't write 0 bytes to the network since it will be
         * treated as the terminating chunk */
        if !upstream_end_of_body && data.as_ref().is_some_and(|d| d.is_empty()) {
            return Ok(DownstreamRequestOutcome::Complete(false));
        }

        debug!(
            "Read {} bytes body from downstream",
            data.as_ref().map_or(-1, |d| d.len() as isize)
        );

        // Fail closed on a `Bodyless` declaration the downstream body has just
        // disproved. Checked here -- after the request-body filters, before
        // anything is handed to the upstream writer -- because the upstream
        // request no longer carries `Content-Length` or `Transfer-Encoding`, so
        // its zero-length body writer would swallow these bytes and the client
        // would be told the request succeeded. See
        // `bodyless_contract_violation`.
        if violates_bodyless_contract(body_disposition, data.as_ref()) {
            return Err(bodyless_contract_violation());
        }

        // No permit means this event is application-only: the upstream
        // exchange is already finished and must not be written to. See the
        // futile-read branch in `proxy_handle_downstream`.
        if let Some(tx) = tx {
            // upgraded body needs to be marked
            if session.was_upgraded() {
                tx.send(HttpTask::UpgradedBody(data, upstream_end_of_body));
            } else {
                tx.send(HttpTask::Body(data, upstream_end_of_body));
            }
        }

        Ok(DownstreamRequestOutcome::Complete(end_of_body))
    }
}

pub(crate) async fn send_body_to1(
    client_session: &mut HttpSessionV1,
    recv_task: Option<HttpTask>,
) -> Result<bool> {
    let body_done;

    if let Some(task) = recv_task {
        match task {
            HttpTask::Body(data, end) => {
                body_done = end;
                if let Some(d) = data {
                    let m = client_session.write_body(&d).await;
                    match m {
                        Ok(m) => match m {
                            Some(n) => {
                                debug!("Write {} bytes body to upstream", n);
                            }
                            None => {
                                warn!("Upstream body is already finished. Nothing to write");
                            }
                        },
                        Err(e) => {
                            return e.into_up().into_err();
                        }
                    }
                }
            }
            HttpTask::UpgradedBody(data, end) => {
                client_session.maybe_upgrade_body_writer();

                body_done = end;
                if let Some(d) = data {
                    let m = client_session.write_body(&d).await;
                    match m {
                        Ok(m) => {
                            match m {
                                Some(n) => {
                                    debug!("Write {} bytes upgraded body to upstream", n);
                                }
                                None => {
                                    warn!("Upstream upgraded body is already finished. Nothing to write");
                                }
                            }
                        }
                        Err(e) => {
                            return e.into_up().into_err();
                        }
                    }
                }
            }
            _ => {
                // should never happen, sender only sends body
                warn!("Unexpected task sent to upstream");
                body_done = true;
                // error here,
                // for client sessions that received upgrade but didn't
                // receive any UpgradedBody,
                // no more data is arriving so we should consider this
                // as downstream finalizing its upgrade payload
                client_session.maybe_upgrade_body_writer();
            }
        }
    } else {
        // sender dropped
        body_done = true;
        // for client sessions that received upgrade but didn't
        // receive any UpgradedBody,
        // no more data is arriving so we should consider this
        // as downstream finalizing its upgrade payload
        client_session.maybe_upgrade_body_writer();
    }

    if body_done {
        match client_session.finish_body().await {
            Ok(_) => {
                debug!("finish sending body to upstream");
                Ok(true)
            }
            Err(e) => e.into_up().into_err(),
        }
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_framing() -> RequestHeader {
        let mut request = RequestHeader::build("POST", b"/", None).unwrap();
        request.insert_header(header::CONTENT_LENGTH, "12").unwrap();
        request
            .insert_header(header::TRANSFER_ENCODING, "gzip")
            .unwrap();
        request
    }

    #[test]
    fn streamed_disposition_uses_h1_chunked_framing() {
        let mut request = request_with_framing();
        apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed)
            .unwrap();

        assert!(request.headers.get(header::CONTENT_LENGTH).is_none());
        // `gzip` is a content coding applied to the bytes being forwarded and
        // must survive the re-framing; only `chunked` is re-derived.
        assert_eq!(
            request.headers.get(header::TRANSFER_ENCODING).unwrap(),
            "gzip, chunked"
        );
    }

    #[test]
    fn streamed_disposition_preserves_non_chunked_transfer_codings() {
        // Already `gzip, chunked`: must round-trip unchanged, not collapse to
        // bare `chunked` (which would erase the gzip coding while the body
        // bytes stay gzip-coded).
        let mut request = RequestHeader::build("POST", b"/", None).unwrap();
        request
            .insert_header(header::TRANSFER_ENCODING, "gzip, chunked")
            .unwrap();
        apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed)
            .unwrap();
        assert_eq!(
            request.headers.get(header::TRANSFER_ENCODING).unwrap(),
            "gzip, chunked"
        );

        // Multiple header lines, mixed case, and a redundant `chunked`.
        let mut request = RequestHeader::build("POST", b"/", None).unwrap();
        request
            .append_header(header::TRANSFER_ENCODING, "deflate")
            .unwrap();
        request
            .append_header(header::TRANSFER_ENCODING, "gzip, Chunked")
            .unwrap();
        apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed)
            .unwrap();
        assert_eq!(
            request.headers.get(header::TRANSFER_ENCODING).unwrap(),
            "deflate, gzip, chunked"
        );

        // Nothing to preserve: bare `chunked`.
        let mut request = RequestHeader::build("POST", b"/", None).unwrap();
        request.insert_header(header::CONTENT_LENGTH, "12").unwrap();
        apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed)
            .unwrap();
        assert_eq!(
            request.headers.get(header::TRANSFER_ENCODING).unwrap(),
            "chunked"
        );
    }

    #[test]
    fn bodyless_disposition_removes_request_framing() {
        let mut request = request_with_framing();
        apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Bodyless)
            .unwrap();

        assert!(request.headers.get(header::CONTENT_LENGTH).is_none());
        assert!(request.headers.get(header::TRANSFER_ENCODING).is_none());
    }

    #[test]
    fn ordinary_disposition_preserves_request_framing() {
        let mut request = request_with_framing();
        apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Ordinary)
            .unwrap();

        assert_eq!(request.headers.get(header::CONTENT_LENGTH).unwrap(), "12");
        assert_eq!(
            request.headers.get(header::TRANSFER_ENCODING).unwrap(),
            "gzip"
        );
    }
}
