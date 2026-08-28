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

use futures::StreamExt;
use pingora_core::{
    protocols::http::{
        custom::{
            client::Session as CustomSession, is_informational_except_101, BodyWrite,
            CustomMessageWrite, CUSTOM_MESSAGE_QUEUE_SIZE,
        },
        v1::common::is_upgrade_req as is_h1_upgrade_req,
    },
    ImmutStr,
};
use proxy_cache::{
    drain_emitted_chunks, drain_emitted_chunks_before, range_filter::RangeBodyFilter,
    ServeFromCache,
};
use proxy_common::{
    no_downstream_body_to_read, normalize_trailers, release_cache_on_terminate,
    DownstreamRequestOutcome, DownstreamStateMachine, PipeState, ResponseStateMachine,
    TerminalBodyDispatch,
};
use tokio::sync::oneshot;

use super::*;

impl<SV, C> HttpProxy<SV, C>
where
    C: custom::Connector,
{
    /// Proxy to a custom protocol upstream.
    /// Returns (reuse_server, reuse_upstream, error)
    pub(crate) async fn proxy_to_custom_upstream(
        &self,
        session: &mut Session,
        client_session: &mut C::Session,
        reused: bool,
        peer: &HttpPeer,
        ctx: &mut SV::CTX,
    ) -> (bool, bool, Option<Box<Error>>)
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
            return (false, false, Some(e));
        }

        let (server_session_reuse, upstream_session_reuse, error) = self
            .custom_proxy_down_to_up(session, client_session, peer, ctx)
            .await;

        // Parity with H1/H2: custom upstreams don't report payload bytes; record 0.
        session.set_upstream_body_bytes_received(0);

        (server_session_reuse, upstream_session_reuse, error)
    }

    /// Handle custom protocol proxying from downstream to upstream.
    /// Returns (reuse_server, reuse_upstream, error)
    async fn custom_proxy_down_to_up(
        &self,
        session: &mut Session,
        client_session: &mut C::Session,
        peer: &HttpPeer,
        ctx: &mut SV::CTX,
    ) -> (bool, bool, Option<Box<Error>>)
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        client_session.set_read_timeout(peer.options.read_timeout);
        client_session.set_write_timeout(peer.options.write_timeout);

        let mut req = session.req_header().clone();

        if session.as_ref().request_body_buffer_registered() {
            return (
                false,
                true,
                Some(Error::explain(
                    InternalError,
                    "early request body replay not supported for custom upstreams",
                )),
            );
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

        session.set_upstream_h1_upgrade_request_status(is_h1_upgrade_req(&req));

        // The custom pump has no upstream-framing rewrite of its own (the
        // connector owns framing), so it cannot honor a non-ordinary
        // disposition. Fail closed instead of silently proxying with the
        // wrong contract -- same rationale as the terminate path below.
        let body_disposition = self.inner.upstream_request_body_disposition(session, ctx);
        if body_disposition != UpstreamRequestBodyDisposition::Ordinary {
            return (
                false,
                true,
                Some(
                    Error::explain(
                        InternalError,
                        "a non-ordinary upstream request body disposition is not supported on custom connector sessions",
                    ),
                ),
            );
        }

        session.upstream_compression.request_filter(&req);
        let body_empty = session.as_mut().is_body_empty();

        debug!("Request to custom: {req:?}");

        let req = Box::new(req);
        if let Err(e) = client_session.write_request_header(req, body_empty).await {
            return (false, false, Some(e.into_up()));
        }

        // take the body writer out of the client for easy duplex
        let mut client_body = client_session
            .take_request_body_writer()
            .expect("already send request header");

        let (tx, rx) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);

        if self.inner.request_retry_allowed(session, ctx) {
            session.as_mut().enable_retry_buffering();
        }

        // Custom message logic

        let Some(mut upstream_custom_message_reader) = client_session.take_custom_message_reader()
        else {
            return (
                false,
                false,
                Some(Error::explain(
                    ReadError,
                    "can't extract custom reader from upstream",
                )),
            );
        };

        let Some(mut upstream_custom_message_writer) = client_session.take_custom_message_writer()
        else {
            return (
                false,
                false,
                Some(Error::explain(
                    WriteError,
                    "custom upstream must have a custom message writer",
                )),
            );
        };

        // A channel to inject custom messages to upstream from server logic.
        let (upstream_custom_message_inject_tx, upstream_custom_message_inject_rx) =
            mpsc::channel(CUSTOM_MESSAGE_QUEUE_SIZE);

        // Downstream reader
        let mut downstream_custom_message_reader = match session.downstream_custom_message() {
            Ok(Some(rx)) => rx,
            Ok(None) => Box::new(futures::stream::empty::<Result<Bytes>>()),
            Err(err) => return (false, false, Some(err)),
        };

        // Downstream writer
        let (mut downstream_custom_message_writer, downstream_custom_final_hop): (
            Box<dyn CustomMessageWrite>,
            bool, // if this hop is final
        ) = if let Some(custom_session) = session.downstream_session.as_custom_mut() {
            (
                custom_session
                    .take_custom_message_writer()
                    .expect("custom downstream must have a custom message writer"),
                false,
            )
        } else {
            (Box::new(()), true)
        };

        // A channel to inject custom messages to downstream from server logic.
        let (downstream_custom_message_inject_tx, downstream_custom_message_inject_rx) =
            mpsc::channel(CUSTOM_MESSAGE_QUEUE_SIZE);

        // Filters for ProxyHttp trait
        let (upstream_custom_message_filter_tx, upstream_custom_message_filter_rx) =
            mpsc::channel(CUSTOM_MESSAGE_QUEUE_SIZE);
        let (downstream_custom_message_filter_tx, downstream_custom_message_filter_rx) =
            mpsc::channel(CUSTOM_MESSAGE_QUEUE_SIZE);

        // Cancellation channels for custom coroutines
        // The transmitters act as guards: when dropped, they signal the receivers to cancel.
        // `cancel_downstream_reader_tx` is held and later used to explicitly cancel.
        // `_cancel_upstream_reader_tx` is unused (prefixed with _) - it will be dropped at the
        // end of this scope, which automatically signals cancellation to the upstream reader.
        let (cancel_downstream_reader_tx, cancel_downstream_reader_rx) = oneshot::channel();
        let (_cancel_upstream_reader_tx, cancel_upstream_reader_rx) = oneshot::channel();

        if let Err(e) = self
            .inner
            .custom_forwarding(
                session,
                ctx,
                Some(upstream_custom_message_inject_tx),
                downstream_custom_message_inject_tx,
            )
            .await
        {
            if let Some(custom_session) = session.downstream_session.as_custom_mut() {
                if let Err(restore_error) =
                    custom_session.restore_custom_message_writer(downstream_custom_message_writer)
                {
                    return (false, false, Some(restore_error));
                }

                if let Err(restore_error) =
                    custom_session.restore_custom_message_reader(downstream_custom_message_reader)
                {
                    return (false, false, Some(restore_error));
                }
            }
            return (false, false, Some(e));
        }

        let upstream_custom_message_forwarder = CustomMessageForwarder {
            ctx: "down_to_up".into(),
            reader: &mut downstream_custom_message_reader,
            writer: &mut upstream_custom_message_writer,
            filter: upstream_custom_message_filter_tx,
            inject: upstream_custom_message_inject_rx,
            cancel: cancel_downstream_reader_rx,
        };

        let downstream_custom_message_forwarder = CustomMessageForwarder {
            ctx: "up_to_down".into(),
            reader: &mut upstream_custom_message_reader,
            writer: &mut downstream_custom_message_writer,
            filter: downstream_custom_message_filter_tx,
            inject: downstream_custom_message_inject_rx,
            cancel: cancel_upstream_reader_rx,
        };

        // Shared signal so the upstream half can distinguish an expected task-pipe
        // closure (the downstream half finished and dropped rx) from an unexpected one.
        let pipe_state = Arc::new(AtomicU8::new(PipeState::Active as u8));

        /* read downstream body and upstream response at the same time */
        let ret = tokio::try_join!(
            self.custom_bidirection_down_to_up(
                session,
                &mut client_body,
                rx,
                ctx,
                upstream_custom_message_filter_rx,
                downstream_custom_message_filter_rx,
                downstream_custom_final_hop,
                cancel_downstream_reader_tx,
                pipe_state.clone(),
            ),
            custom_pipe_up_to_down_response(client_session, tx, pipe_state),
            upstream_custom_message_forwarder.proxy(),
            downstream_custom_message_forwarder.proxy(),
        );

        if let Some(custom_session) = session.downstream_session.as_custom_mut() {
            custom_session
                .restore_custom_message_writer(downstream_custom_message_writer)
                .expect("downstream restore_custom_message_writer should be empty");

            custom_session
                .restore_custom_message_reader(downstream_custom_message_reader)
                .expect("downstream restore_custom_message_reader should be empty");
        }

        match ret {
            Ok((
                DownstreamRequestOutcome::Complete(downstream_can_reuse),
                _upstream,
                _custom_up_down,
                _custom_down_up,
            )) => (downstream_can_reuse, true, None),
            Ok((
                DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(downstream_can_reuse),
                _upstream,
                _custom_up_down,
                _custom_down_up,
            )) => (downstream_can_reuse, false, None),
            // Unreachable by construction: the custom pump fails closed on
            // terminate (see the error return in `custom_bidirection_down_to_up`)
            // rather than propagating this outcome. Keep the explicit arm so
            // the match remains exhaustive without a catch-all.
            Ok((DownstreamRequestOutcome::Terminate, _, _, _)) => {
                release_cache_on_terminate(session);
                (false, false, None)
            }
            Err(e) => (false, false, Some(e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_upstream_tasks_custom(
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
    ) -> Result<Option<bool>>
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
        while let Ok(task) = rx.try_recv() {
            tasks.push(task);
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
            // check error and abort
            // otherwise the error is surfaced via write_response_tasks()
            if !serve_from_cache.should_send_to_downstream() {
                if let HttpTask::Failed(e) = t {
                    return Err(e);
                }
            }
            self.custom_response_filter(
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

        if sink.is_terminated() {
            return Error::e_explain(
                InternalError,
                "response-body terminate is not supported on custom connector sessions",
            );
        }

        if !serve_from_cache.should_send_to_downstream() {
            // TODO: need to derive response_done from filtered_tasks in case downstream failed already
            return Ok(None);
        }

        session.write_response_tasks(filtered_tasks).await?;

        Ok(Some(source_done))
    }

    // TODO: pre-existing inconsistency with proxy_h1/proxy_h2 to address in a follow-up:
    // upstream task rx.recv() branch is missing
    // downstream_state.maybe_finished(session.is_body_done()) after processing. proxy_h1 has
    // this because upgrade responses can force the body done — since custom upstreams can
    // serve H1 downstreams that support upgrades, the same may be needed here.
    // Returns whether server (downstream) session can be reused
    // Returns the downstream completion and upstream reuse outcome separately.
    #[allow(clippy::too_many_arguments)]
    async fn custom_bidirection_down_to_up(
        &self,
        session: &mut Session,
        client_body: &mut Box<dyn BodyWrite>,
        mut rx: mpsc::Receiver<HttpTask>,
        ctx: &mut SV::CTX,
        mut upstream_custom_message_filter_rx: mpsc::Receiver<(
            Bytes,
            oneshot::Sender<Option<Bytes>>,
        )>,
        mut downstream_custom_message_filter_rx: mpsc::Receiver<(
            Bytes,
            oneshot::Sender<Option<Bytes>>,
        )>,
        downstream_custom_final_hop: bool,
        cancel_downstream_reader_tx: oneshot::Sender<()>,
        pipe_state: Arc<AtomicU8>,
    ) -> Result<DownstreamRequestOutcome>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let mut cancel_downstream_reader_tx = Some(cancel_downstream_reader_tx);

        // Must agree with the `is_body_empty()` the upstream end-of-stream was
        // derived from in `proxy_to_custom_upstream`; see
        // `no_downstream_body_to_read`.
        let mut downstream_state = DownstreamStateMachine::new(no_downstream_body_to_read(session));
        // `ReadingFinished` still owns an idle/disconnect watcher. Stop that
        // watcher only when the application abandons the upload, or when a
        // custom downstream violates the idle contract by returning success.
        let mut poll_downstream_body_or_idle = true;

        // retry, send buffer if it exists
        if let Some(buffer) = session.as_mut().get_retry_buffer() {
            self.send_body_to_custom(
                session,
                Some(buffer),
                RequestBodyEvent::from(downstream_state.is_done()),
                client_body,
                ctx,
            )
            .await?;
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

        let mut upstream_custom = true;
        let mut downstream_custom = true;

        /* duplex mode
         * see the Same function for h1 for more comments
         */
        while !downstream_state.is_done()
            || !response_state.is_done()
            || upstream_custom
            || downstream_custom
        {
            if response_state.is_done() && downstream_state.is_reading() {
                // The custom upstream has completed the whole response while
                // the downstream upload is still open. Nothing can consume
                // more request bytes, so pay the request-body contract its
                // single terminal event without manufacturing a clean EOS for
                // the custom writer.
                self.send_body_to_custom(
                    session,
                    None,
                    RequestBodyEvent::Abandoned,
                    client_body,
                    ctx,
                )
                .await?;
                downstream_state.maybe_finished(true);
                poll_downstream_body_or_idle = false;
                if let Some(cancel) = cancel_downstream_reader_tx.take() {
                    let _ = cancel.send(());
                }
                continue;
            }

            // partial read support, this check will also be false if cache is disabled.
            let support_cache_partial_read =
                session.cache.support_streaming_partial_write() == Some(true);
            let upgraded = session.was_upgraded();

            tokio::select! {
                body = session.downstream_session.read_body_or_idle(downstream_state.is_done()),
                    if downstream_state.can_poll() && poll_downstream_body_or_idle => {
                    let reading_body = downstream_state.is_reading();
                    let body = match body {
                        Ok(b) => b,
                        Err(e) => {
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
                                continue;
                           } else {
                                return Err(e.into_down());
                            }
                        }
                    };
                    if !reading_body {
                        // Built-in downstreams resolve an idle watch only with
                        // an error. A custom implementation may instead return
                        // success; do not manufacture another request-body
                        // terminal event or spin by polling it again.
                        poll_downstream_body_or_idle = false;
                        continue;
                    }
                    let is_body_done = session.is_body_done();
                    let event = RequestBodyEvent::from(is_body_done);

                    match self.send_body_to_custom(session, body, event, client_body, ctx).await {
                        Ok(request_done) =>  {
                            downstream_state.maybe_finished(request_done);
                        },
                        Err(e) => {
                            // mark request done, attempt to drain receive
                            warn!("body send error: {e}");

                            // upstream is what actually errored but we don't want to continue
                            // polling the downstream body
                            downstream_state.to_errored();

                            // downstream still trying to send something, but the upstream is already stooped
                            // cancel the custom downstream to upstream coroutine, because the proxy will not see EOS.
                            let _ = cancel_downstream_reader_tx.take().expect("cancel must be set and called once").send(());
                        }
                    };
                },

                // Handle buffered upstream task from previous iteration
                task = async { next_upstream_task.take() }, if next_upstream_task.is_some() => {
                    debug!("buffered upstream event: {:?}", task);
                    if let Some(t) = task {
                        let Some(response_done) = self.process_upstream_tasks_custom(
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
                        response_state.maybe_set_upstream_done(response_done);
                    } else {
                        debug!("empty upstream event");
                        response_state.maybe_set_upstream_done(true);
                    }
                },

                task = rx.recv(), if !response_state.upstream_done() && next_upstream_task.is_none() => {
                    debug!("upstream event: {:?}", task);
                    if let Some(t) = task {
                        let upgraded = session.was_upgraded();
                        let Some(response_done) = self.process_upstream_tasks_custom(
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
                        if !upgraded && session.was_upgraded() && downstream_state.can_poll() {
                            // just upgraded, the downstream state should be reset to continue to
                            // poll body
                            trace!("reset downstream state on upgrade");
                            downstream_state.reset();
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
                    self.custom_response_filter(
                        session,
                        task,
                        ctx,
                        &mut serve_from_cache,
                        &mut range_body_filter,
                        true,
                        &mut suppress_downstream_body,
                        &mut filtered_terminal_header,
                        &mut upstream_reusable,
                        &mut sink,
                        &mut terminal_body,
                        &mut cached_tasks,
                    ).await?;

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

                ret = upstream_custom_message_filter_rx.recv(), if upstream_custom => {
                    let Some(msg) = ret else {
                        debug!("upstream_custom_message_filter_rx: custom downstream to upstream exited on reading");
                        upstream_custom = false;
                        continue;
                    };

                    let (data, callback) = msg;

                    let new_msg = self.inner
                        .downstream_custom_message_proxy_filter(session, data, ctx, false)  // false because the upstream is custom
                        .await?;

                    if callback.send(new_msg).is_err() {
                        debug!("upstream_custom_message_incoming_rx: custom downstream to upstream exited on callback");
                        upstream_custom = false;
                        continue;
                    };
                },

                ret = downstream_custom_message_filter_rx.recv(), if downstream_custom => {
                    let Some(msg) = ret else {
                        debug!("downstream_custom_message_filter_rx: custom upstream to downstream exited on reading");
                        downstream_custom = false;
                        continue;
                    };

                    let (data, callback) = msg;

                    let new_msg = self.inner
                        .upstream_custom_message_proxy_filter(session, data, ctx, downstream_custom_final_hop)
                        .await?;

                    if callback.send(new_msg).is_err() {
                        debug!("downstream_custom_message_filter_rx: custom upstream to downstream exited on callback");
                        downstream_custom = false;
                        continue
                    };
                },

                else => {
                    break;
                }
            }
        }

        // Re-raise the error then the loop is finished. This returns without
        // signaling PipeState::DownstreamComplete, so the upstream half treats
        // the resulting task-pipe closure as unexpected and surfaces the error.
        if downstream_state.is_errored() {
            let err = Error::e_explain(WriteError, "downstream_state is_errored");
            error!("custom_bidirection_down_to_up: downstream_state.is_errored",);
            return err;
        }

        client_body.cleanup().await?;

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
    async fn custom_response_filter(
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
            || matches!(&task, HttpTask::Body(..) | HttpTask::UpgradedBody(..))
            || (from_cache && matches!(&task, HttpTask::Done));
        let mut terminal_cacheability = None;
        // Whether this task must deliver the response's single terminal
        // `upstream_response_body_filter` callback. Only `Trailer`/`Done` ever
        // set it, and only once per response -- see `TerminalBodyDispatch`.
        let mut terminal_event = None;

        if !from_cache {
            if let HttpTask::Header(header, _) = &task {
                reject_mismatched_h1_upgrade_101(session, header, "custom_upstream_filter")
                    .map_err(|e| e.into_up())?;
            }

            if let Some(duration) = self.upstream_filter(session, &mut task, sink, ctx).await? {
                trace!("delaying upstream response for {duration:?}");
                time::sleep(duration).await;
            }

            // `upstream_filter` reaches the body filter only from a
            // `Body`/`UpgradedBody` task, so a response terminating with a
            // trailer or a bare `Done` would never deliver end-of-stream to a
            // filter that withholds bytes until it.
            terminal_event = terminal_body.claim_for(&task);
            if let Some(event) = terminal_event {
                if let Some(duration) = self
                    .terminal_upstream_body_filter(session, event, sink, ctx)
                    .await?
                {
                    trace!("delaying terminal upstream response for {duration:?}");
                    time::sleep(duration).await;
                }
            }
            if let HttpTask::Trailer(Some(trailers)) = &mut task {
                self.inner
                    .upstream_response_trailer_filter(session, trailers, ctx)
                    .await?;
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
                if terminal_event.is_some() {
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
                    // TODO: if cache is disabled during response phase, we should still do the filter
                    if session.cache.enabled() {
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
                    && header.status == http::StatusCode::SWITCHING_PROTOCOLS
                {
                    terminal_body.mark_upgraded();
                }
                if terminal_header {
                    if let Some(duration) = self
                        .terminal_upstream_body_filter(
                            session,
                            UpstreamResponseBodyEvent::TerminalWithoutTrailers,
                            sink,
                            ctx,
                        )
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
                if !from_cache {
                    // Re-check after response_filter in case it changed the final status to 101.
                    reject_mismatched_h1_upgrade_101(session, &header, "custom_response_filter")
                        .map_err(|e| e.into_in())?;
                }

                // these status codes / method cannot have body, so no need to add chunked encoding
                /* Add chunked header to tell downstream to use chunked encoding
                 * during the absent of content-length */
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
            HttpTask::UpgradedBody(data, eos) => Ok(HttpTask::UpgradedBody(data, eos)),
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
                    Ok(HttpTask::Trailer(normalize_trailers(trailers)))
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
        } else if terminal_event.is_some() {
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
        } else if filter_downstream_body || terminal_event.is_some() {
            self.downstream_response_body_filter_tasks(session, &mut out_tasks[start..], ctx)
                .await?;
        }
        session
            .prepare_response_headers(&mut out_tasks[start..])
            .await?;
        Ok(())
    }

    async fn send_body_to_custom(
        &self,
        session: &mut Session,
        mut data: Option<Bytes>,
        mut event: RequestBodyEvent,
        client_body: &mut Box<dyn BodyWrite>,
        ctx: &mut SV::CTX,
    ) -> Result<bool>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        // `data == None` IS the end of the downstream body, whatever the caller
        // computed from `is_body_done()`. Mirrors `proxy_h1::send_body_to_pipe`
        // and `proxy_h2::send_body_to2`.
        //
        // Load-bearing here rather than defensive: this pump's downstream is a
        // `SessionCustom`, whose `is_body_done()` is IMPLEMENTED BY THE USER
        // (`protocols/http/custom/server.rs`). An implementation that reports
        // `false` after its reader reached EOF would otherwise invoke the
        // application hooks with `(None, end_of_stream = false)` -- violating
        // their documented contract -- never deliver the single `(None, true)`
        // event, and return `Ok(false)` forever, so the duplex loop would spin
        // on an already-finished read side at 100% CPU.
        if data.is_none() && event == RequestBodyEvent::Data {
            event = RequestBodyEvent::Complete;
        }
        let end_of_body = event.is_terminal();

        self.filter_custom_request_body(session, &mut data, event, ctx)
            .await?;

        // Abandonment is terminal for application/module state, but it is not
        // a clean request EOS at the custom transport. In particular, calling
        // BodyWrite::finish() here would misrepresent a deliberately truncated
        // upload as complete.
        if event == RequestBodyEvent::Abandoned {
            return Ok(true);
        }

        if session.was_upgraded() {
            client_body.upgrade_body_writer();
        }

        /* it is normal to get 0 bytes because of multi-chunk parsing or request_body_filter.
         * Although there is no harm writing empty byte to custom, unlike h1, we ignore it
         * for consistency */
        if !end_of_body && data.as_ref().is_some_and(|d| d.is_empty()) {
            return Ok(false);
        }

        if let Some(mut data) = data {
            if let Err(e) = client_body.write_all_buf(&mut data).await {
                if event == RequestBodyEvent::Data {
                    let mut abandoned_body = None;
                    self.filter_custom_request_body(
                        session,
                        &mut abandoned_body,
                        RequestBodyEvent::Abandoned,
                        ctx,
                    )
                    .await?;
                }
                return Err(e.into_up());
            }
            if end_of_body {
                client_body.finish().await.map_err(|e| e.into_up())?;
            }
        } else {
            debug!("Read downstream body done");
            client_body
                .finish()
                .await
                .map_err(|e| {
                    Error::because(WriteError, "while shutdown send data stream on no data", e)
                })
                .map_err(|e| e.into_up())?;
        }

        Ok(end_of_body)
    }

    async fn filter_custom_request_body(
        &self,
        session: &mut Session,
        data: &mut Option<Bytes>,
        event: RequestBodyEvent,
        ctx: &mut SV::CTX,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        session
            .downstream_modules_ctx
            .request_body_filter(data, event)
            .await?;

        if self
            .inner
            .request_body_filter_action(session, data, event, ctx)
            .await?
            == RequestBodyAction::Terminate
        {
            // The custom pump has its own join structure and implements no
            // terminate propagation; fail closed instead of diverging
            // silently.
            return Error::e_explain(
                InternalError,
                "request-body terminate is not supported on custom connector sessions",
            );
        }

        Ok(())
    }
}

/* Read response header, body and trailer from custom upstream and send them to tx */
async fn custom_pipe_up_to_down_response<S: CustomSession>(
    client: &mut S,
    tx: mpsc::Sender<HttpTask>,
    pipe_state: Arc<AtomicU8>,
) -> Result<()> {
    let mut is_informational = true;
    // Set when a final `101` arrived together with clean end-of-stream, i.e.
    // the whole response is complete at the header. See the normalization
    // below for why that end-of-stream cannot ride on the header task.
    let mut upgrade_ended = false;
    while is_informational {
        client
            .read_response_header()
            .await
            .map_err(|e| e.into_up())?;
        let resp_header = Box::new(client.response_header().expect("just read").clone());
        // `101 Switching Protocols` is a response to the http1 Upgrade header and it's final response.
        // The WebSocket Protocol https://datatracker.ietf.org/doc/html/rfc6455
        is_informational = is_informational_except_101(resp_header.status.as_u16() as u32);

        match client.check_response_end_or_error(true).await {
            Ok(eos) => {
                // A connector that already saw the upgraded connection reach
                // clean EOF reports the entire response as `Header(101, true)`.
                // Downstream, `Header(_, true)` means "terminal header": a
                // final response with an empty body, served by the
                // `terminal_header` branch of `custom_response_filter`. A `101`
                // can never take that branch -- it is informational by status,
                // and writing it switches the H1 downstream session into raw
                // upgraded mode, where that branch's plain-body framing is
                // invalid. Normalize it to the shape the rest of the pipeline
                // already handles, `Header(101, false)` followed by `Done`:
                // `Done` is what dispatches the response's single terminal
                // `upstream_response_body_filter` callback, and it releases the
                // bytes that filter withheld under the response's own upgraded
                // body variant.
                if eos && resp_header.status == http::StatusCode::SWITCHING_PROTOCOLS {
                    upgrade_ended = true;
                }
                tx.send(HttpTask::Header(resp_header, eos && !upgrade_ended))
                    .await
                    .or_err(InternalError, "sending custom headers to pipe")?;
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
    }

    if upgrade_ended {
        // The upgraded connection is already at clean EOF: no body and no
        // trailers can follow it, and the session must not be read again after
        // it reported end-of-stream.
        tx.send(HttpTask::Done)
            .await
            .unwrap_or_else(|_| debug!("custom channel closed!"));
        return Ok(());
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

        match client.check_response_end_or_error(false).await {
            Ok(eos) => {
                let empty = data.is_empty();
                if empty && !eos {
                    /* it is normal to get 0 bytes because of multi-chunk
                     * don't write 0 bytes to downstream since it will be
                     * misread as the terminating chunk */
                    continue;
                }
                let body_task = if client.was_upgraded() {
                    HttpTask::UpgradedBody(Some(data), eos)
                } else {
                    HttpTask::Body(Some(data), eos)
                };
                // A send failure is benign only when the downstream half signaled it
                // completed (it finished the response by its own framing and dropped the
                // task pipe): stop reading the upstream. Otherwise the closure is
                // unexpected, so surface the original error.
                let send_result = tx.send(body_task).await;
                if send_result.is_err()
                    && PipeState::is_downstream_complete(pipe_state.load(Ordering::Acquire))
                {
                    return Ok(());
                }
                send_result.or_err(InternalError, "sending custom body to pipe")?;
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
        // Benign only if the downstream signaled completion, same as the body sends above.
        let send_result = tx.send(HttpTask::Trailer(trailers)).await;
        if send_result.is_err()
            && PipeState::is_downstream_complete(pipe_state.load(Ordering::Acquire))
        {
            return Ok(());
        }
        send_result.or_err(InternalError, "sending custom trailer to pipe")?;
    }

    let send_result = tx.send(HttpTask::Done).await;
    if send_result.is_err() && PipeState::is_downstream_complete(pipe_state.load(Ordering::Acquire))
    {
        debug!("custom channel closed!");
        return Ok(());
    }
    send_result.or_err(InternalError, "sending custom done to pipe")?;

    Ok(())
}

struct CustomMessageForwarder<'a> {
    ctx: ImmutStr,
    writer: &'a mut Box<dyn CustomMessageWrite>,
    reader:
        &'a mut Box<dyn futures::Stream<Item = Result<Bytes, Box<Error>>> + Send + Sync + Unpin>,
    inject: mpsc::Receiver<Bytes>,
    filter: mpsc::Sender<(Bytes, oneshot::Sender<Option<Bytes>>)>,
    cancel: oneshot::Receiver<()>,
}

impl CustomMessageForwarder<'_> {
    async fn proxy(mut self) -> Result<()> {
        let forwarder = async {
            let mut injector_status = true;
            let mut reader_status = true;

            debug!("{}: CustomMessageForwarder: start", self.ctx);

            while injector_status || reader_status {
                let (data, proxied) = tokio::select! {
                    ret = self.inject.recv(), if injector_status => {
                        let Some(data) = ret else {
                            injector_status = false;
                            continue
                        };
                        (data, false)
                    },

                    ret = self.reader.next(), if reader_status  => {
                        let Some(data) = ret else {
                            reader_status = false;
                            continue
                        };

                        let data = match data {
                            Ok(data) => data,
                            Err(err) => {
                                reader_status = false;
                                warn!("{}: CustomMessageForwarder: reader returned err: {err:?}", self.ctx);
                                continue;
                            },
                        };
                        (data, true)
                    },
                };

                let (callback_tx, callback_rx) = oneshot::channel();

                // If data received from proxy send it to filter
                if proxied {
                    if self.filter.send((data, callback_tx)).await.is_err() {
                        debug!(
                            "{}: CustomMessageForwarder: filter receiver dropped",
                            self.ctx
                        );
                        return Error::e_explain(
                            WriteError,
                            "CustomMessageForwarder: main proxy thread exited on filter send",
                        );
                    };
                } else {
                    callback_tx
                        .send(Some(data))
                        .expect("sending from the same thread");
                }

                match callback_rx.await {
                    Ok(None) => continue, // message was filtered
                    Ok(Some(msg)) => {
                        self.writer.write_custom_message(msg).await?;
                    }
                    Err(err) => {
                        debug!(
                            "{}: CustomMessageForwarder: callback_rx return error: {err}",
                            self.ctx
                        );
                        return Error::e_because(
                            WriteError,
                            "CustomMessageForwarder: main proxy thread exited on callback_rx await",
                            err,
                        );
                    }
                };
            }

            debug!("{}: CustomMessageForwarder: exit loop", self.ctx);

            let ret = self.writer.finish_custom().await;
            if let Err(ref err) = ret {
                debug!(
                    "{}: CustomMessageForwarder: finish_custom return error: {err}",
                    self.ctx
                );
            };
            ret?;

            debug!(
                "{}: CustomMessageForwarder: exit loop successfully",
                self.ctx
            );

            Ok(())
        };

        tokio::select! {
            ret = &mut self.cancel => {
                debug!("{}: CustomMessageForwarder: canceled while waiting for new messages: {ret:?}", self.ctx);
                Ok(())
            },
            ret = forwarder => ret
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pingora_cache::{
        predictor::{CacheablePredictor, Predictor},
        CacheKey, MemCache,
    };
    use std::sync::{Arc, LazyLock};
    use tokio::io::AsyncWriteExt;

    static CACHE_STORAGE: LazyLock<MemCache> = LazyLock::new(MemCache::new);
    static CACHE_PREDICTOR: LazyLock<Predictor<1>> = LazyLock::new(|| Predictor::new(10, None));

    struct TestProxy;

    #[async_trait]
    impl ProxyHttp for TestProxy {
        type CTX = ();

        fn new_ctx(&self) -> Self::CTX {}

        async fn upstream_peer(
            &self,
            _session: &mut Session,
            _ctx: &mut Self::CTX,
        ) -> Result<Box<HttpPeer>> {
            unreachable!("test calls custom_response_filter directly")
        }
    }

    async fn predicted_too_large_session(key: CacheKey, max_file_size: usize) -> Session {
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
            .await
            .expect("test request should be written");

        let mut session = Session::new_h1(Box::new(server) as pingora_core::protocols::Stream);
        session
            .read_request()
            .await
            .expect("test request should parse");
        session
            .cache
            .enable(&*CACHE_STORAGE, None, Some(&*CACHE_PREDICTOR), None, None);
        session.cache.set_cache_key(key.clone());
        session.cache.set_max_file_size_bytes(max_file_size);
        CACHE_PREDICTOR.mark_uncacheable(&key, NoCacheReason::OriginNotCache);
        session
            .cache
            .disable(NoCacheReason::PredictedResponseTooLarge);
        session
    }

    async fn filter_task(session: &mut Session, task: HttpTask) {
        let proxy = HttpProxy::new(TestProxy, Arc::new(ServerConf::default()));
        let mut suppress_downstream_body = false;
        let mut filtered_terminal_header = None;
        let mut upstream_reusable = true;
        let mut sink = ResponseBodySink::new();
        let mut terminal_body = TerminalBodyDispatch::default();
        let mut out_tasks = Vec::new();
        proxy
            .custom_response_filter(
                session,
                task,
                &mut (),
                &mut ServeFromCache::new(),
                &mut RangeBodyFilter::new(),
                false,
                &mut suppress_downstream_body,
                &mut filtered_terminal_header,
                &mut upstream_reusable,
                &mut sink,
                &mut terminal_body,
                &mut out_tasks,
            )
            .await
            .expect("response task should pass filters");
    }

    #[tokio::test]
    async fn completed_response_under_limit_clears_predictor() {
        let key = CacheKey::new("/custom-under-limit", "");
        let mut session = predicted_too_large_session(key.clone(), 10).await;

        filter_task(
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"small")), true),
        )
        .await;

        assert!(CACHE_PREDICTOR.cacheable_prediction(&key));
    }

    #[tokio::test]
    async fn completed_response_over_limit_keeps_predictor() {
        let key = CacheKey::new("/custom-over-limit", "");
        let mut session = predicted_too_large_session(key.clone(), 4).await;

        filter_task(
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"large")), true),
        )
        .await;

        assert!(!CACHE_PREDICTOR.cacheable_prediction(&key));
    }

    #[tokio::test]
    async fn failed_response_keeps_predictor() {
        let key = CacheKey::new("/custom-failed", "");
        let mut session = predicted_too_large_session(key.clone(), 10).await;

        filter_task(
            &mut session,
            HttpTask::Body(Some(Bytes::from_static(b"small")), false),
        )
        .await;
        filter_task(
            &mut session,
            HttpTask::Failed(Error::explain(InternalError, "test failure")),
        )
        .await;

        assert!(!CACHE_PREDICTOR.cacheable_prediction(&key));
    }
}
