// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Protocol-neutral response-task transformation.
//!
//! The duplex pumps retain ownership of transport reads, writes, cancellation,
//! and connection reuse. This module owns the shared semantic sequence between
//! receiving an [`HttpTask`] and handing a prepared task batch to a writer.

use crate::proxy_cache::{
    self, drain_emitted_chunks, drain_emitted_chunks_before, range_filter::RangeBodyFilter,
    ServeFromCache,
};
use crate::proxy_common::{normalize_trailers, TerminalBodyDispatch};
use crate::{
    abort_cache_after_response_source_failure, custom, downstream_response_body_forbidden,
    is_downstream_followup, reconcile_terminal_cache_header, reconcile_terminal_response_tasks,
    reject_mismatched_h1_upgrade_101, HttpProxy, ProxyHttp, ResponseBodySink, Session,
    UpstreamResponseBodyEvent,
};
use http::version::Version;
use log::trace;
use pingora_core::protocols::http::HttpTask;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use tokio::time;

/// Protocol-specific response behavior retained at the shared pipeline seam.
///
/// Most variants are wire/framing policy. The custom conditional-filter gate
/// is an explicitly temporary compatibility difference tracked in
/// `edgion-changes/pending-issues/custom-conditional-filter-gate.md`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseProtocol {
    H1,
    H2,
    Custom,
}

/// State whose lifetime is exactly one response transformation pipeline.
///
/// Session, application context, cache state, and protocol writer deliberately
/// remain outside this object: they have different ownership and lifetimes.
pub(crate) struct ResponsePipelineState {
    pub(crate) range_body_filter: RangeBodyFilter,
    pub(crate) suppress_downstream_body: bool,
    pub(crate) filtered_terminal_header: Option<Box<ResponseHeader>>,
    pub(crate) upstream_reusable: bool,
    pub(crate) sink: ResponseBodySink,
    pub(crate) terminal_body: TerminalBodyDispatch,
}

impl Default for ResponsePipelineState {
    fn default() -> Self {
        Self {
            range_body_filter: RangeBodyFilter::new(),
            suppress_downstream_body: false,
            filtered_terminal_header: None,
            upstream_reusable: true,
            sink: ResponseBodySink::new(),
            terminal_body: TerminalBodyDispatch::default(),
        }
    }
}

impl ResponseProtocol {
    fn validates_upgrade_before_upstream_filter(self) -> bool {
        self == Self::Custom
    }

    fn validates_upgrade_after_upstream_filter(self) -> bool {
        self == Self::H1
    }

    fn preserves_custom_conditional_filter_gate(self) -> bool {
        // Preserve the custom transport's existing behavior during this
        // structural refactor. Aligning this known drift is a behavior change
        // and should be handled with its own regression evidence.
        self == Self::Custom
    }

    fn supports_upgraded_body(self) -> bool {
        self != Self::H2
    }
}

impl<SV, C> HttpProxy<SV, C>
where
    SV: ProxyHttp,
    C: custom::Connector,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn response_task_pipeline(
        &self,
        protocol: ResponseProtocol,
        session: &mut Session,
        mut task: HttpTask,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
        from_cache: bool,
        state: &mut ResponsePipelineState,
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
            || (protocol.supports_upgraded_body() && matches!(&task, HttpTask::UpgradedBody(..)))
            || (from_cache && matches!(&task, HttpTask::Done));
        let mut terminal_cacheability = None;
        let mut terminal_event = None;

        if !from_cache {
            if protocol.validates_upgrade_before_upstream_filter() {
                if let HttpTask::Header(header, _) = &task {
                    reject_mismatched_h1_upgrade_101(session, header, "custom_upstream_filter")
                        .map_err(|e| e.into_up())?;
                }
            }

            if let Some(duration) = self
                .upstream_filter(session, &mut task, &mut state.sink, ctx)
                .await?
            {
                trace!("delaying upstream response for {duration:?}");
                time::sleep(duration).await;
            }

            if protocol.validates_upgrade_after_upstream_filter() {
                if let HttpTask::Header(header, _) = &task {
                    reject_mismatched_h1_upgrade_101(session, header, "h1_upstream_filter")
                        .map_err(|e| e.into_up())?;
                }
            }

            terminal_event = state.terminal_body.claim_for(&task);
            if let Some(event) = terminal_event {
                if let Some(duration) = self
                    .terminal_upstream_body_filter(session, event, &mut state.sink, ctx)
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
            } else {
                if terminal_event.is_some() {
                    self.cache_task_and_emitted_chunks_before(
                        session,
                        &task,
                        &mut state.sink,
                        state.terminal_body.is_upgraded(),
                        ctx,
                        serve_from_cache,
                    )
                    .await?;
                } else {
                    self.cache_task_and_emitted_chunks(
                        session,
                        &task,
                        &mut state.sink,
                        ctx,
                        serve_from_cache,
                    )
                    .await?;
                }
                self.track_predicted_uncacheable_response(session, &task, &state.sink);
            }

            if !terminal_header && !serve_from_cache.should_send_to_downstream() {
                state.sink.take_extra();
                if let HttpTask::Failed(error) = task {
                    abort_cache_after_response_source_failure(session, false);
                    return Err(error);
                }
                out_tasks.push(task);
                return Ok(());
            }
        }

        if state.suppress_downstream_body && is_downstream_followup(&task) {
            state.sink.take_extra();
            if matches!(task, HttpTask::Failed(_)) {
                state.upstream_reusable = false;
                abort_cache_after_response_source_failure(session, from_cache);
            }
            return Ok(());
        }

        let task = match task {
            HttpTask::Header(mut header, eos) => {
                let cache_header = terminal_header.then(|| header.clone());
                if !from_cache {
                    proxy_cache::strip_terminal_synthetic_wire_marker(&mut header);
                }
                let terminal_synthetic_entity = proxy_cache::is_terminal_synthetic_entity(&header);
                let substituted = if from_cache {
                    state
                        .filtered_terminal_header
                        .take()
                        .map(|filtered_header| header = filtered_header)
                        .is_some()
                } else {
                    false
                };
                if !substituted {
                    let apply_conditional = if protocol.preserves_custom_conditional_filter_gate() {
                        session.cache.enabled()
                    } else {
                        session.upstream_headers_mutated_for_cache()
                    };
                    if apply_conditional {
                        self.downstream_response_conditional_filter(
                            serve_from_cache,
                            session,
                            &mut header,
                            ctx,
                        );
                        let skip_range = if from_cache {
                            terminal_synthetic_entity
                        } else {
                            terminal_header
                        };
                        if !skip_range && !session.ignore_downstream_range {
                            let range_type =
                                self.inner.range_header_filter(session, &mut header, ctx);
                            state.range_body_filter.set(range_type);
                        }
                    }
                    self.inner
                        .response_filter(session, &mut header, ctx)
                        .await?;
                }

                if protocol != ResponseProtocol::H1
                    && !from_cache
                    && session.as_downstream().is_upgrade_req()
                    && header.status == http::StatusCode::SWITCHING_PROTOCOLS
                {
                    state.terminal_body.mark_upgraded();
                }

                if terminal_header {
                    if let Some(duration) = self
                        .terminal_upstream_body_filter(
                            session,
                            UpstreamResponseBodyEvent::TerminalWithoutTrailers,
                            &mut state.sink,
                            ctx,
                        )
                        .await?
                    {
                        trace!("delaying terminal upstream response for {duration:?}");
                        time::sleep(duration).await;
                    }
                    let mut cache_header =
                        cache_header.expect("terminal header must retain its cache representation");
                    reconcile_terminal_cache_header(&mut cache_header, &state.sink);
                    reconcile_terminal_cache_header(&mut header, &state.sink);
                    proxy_cache::mark_terminal_synthetic_entity(&mut cache_header);
                    state.filtered_terminal_header = Some(header.clone());
                    let cache_task = HttpTask::Header(cache_header, true);
                    self.track_predicted_uncacheable_response(session, &cache_task, &state.sink);
                    self.cache_task_and_emitted_chunks_with_decision(
                        session,
                        &cache_task,
                        &mut state.sink,
                        terminal_cacheability,
                        ctx,
                        serve_from_cache,
                    )
                    .await?;
                    if !serve_from_cache.should_send_to_downstream() {
                        state.sink.take_extra();
                        return Ok(());
                    }
                }

                if downstream_response_body_forbidden(session, &header) {
                    state.sink.take_extra();
                    header.remove_header(&http::header::TRANSFER_ENCODING);
                    if header.status.is_informational() || header.status.as_u16() == 204 {
                        header.remove_header(&http::header::CONTENT_LENGTH);
                    }
                }
                if !header.status.is_informational() {
                    state.suppress_downstream_body =
                        terminal_header || downstream_response_body_forbidden(session, &header);
                }

                header.set_version(Version::HTTP_11);
                match protocol {
                    ResponseProtocol::H1 => {
                        if !state.suppress_downstream_body
                            && header
                                .headers
                                .get(http::header::TRANSFER_ENCODING)
                                .is_none()
                            && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                            && (!eos || !state.sink.peek_extra().is_empty())
                        {
                            header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                        }
                        if !from_cache {
                            reject_mismatched_h1_upgrade_101(
                                session,
                                &header,
                                "h1_response_filter",
                            )
                            .map_err(|e| e.into_in())?;
                        }
                    }
                    ResponseProtocol::H2 => {
                        if !downstream_response_body_forbidden(session, &header)
                            && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                        {
                            header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                        }
                    }
                    ResponseProtocol::Custom => {
                        if !from_cache {
                            reject_mismatched_h1_upgrade_101(
                                session,
                                &header,
                                "custom_response_filter",
                            )
                            .map_err(|e| e.into_in())?;
                        }
                        if !downstream_response_body_forbidden(session, &header)
                            && header.headers.get(http::header::CONTENT_LENGTH).is_none()
                        {
                            header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
                        }
                    }
                }
                HttpTask::Header(header, eos || state.suppress_downstream_body)
            }
            HttpTask::Body(data, eos) => {
                HttpTask::Body(state.range_body_filter.filter_body(data), eos)
            }
            HttpTask::UpgradedBody(data, eos) if protocol.supports_upgraded_body() => {
                HttpTask::UpgradedBody(data, eos)
            }
            HttpTask::UpgradedBody(..) => {
                panic!("Unexpected UpgradedBody task while proxy h2")
            }
            HttpTask::Trailer(mut trailers) => {
                let trailer_buffer = match trailers.as_mut() {
                    Some(trailers) => {
                        self.inner
                            .response_trailer_filter(session, trailers, ctx)
                            .await?
                    }
                    None => None,
                };
                if let Some(buffer) = trailer_buffer {
                    HttpTask::Body(Some(buffer), true)
                } else {
                    HttpTask::Trailer(normalize_trailers(trailers))
                }
            }
            HttpTask::Done if from_cache => HttpTask::Body(None, true),
            task @ (HttpTask::Done | HttpTask::Failed(_)) => task,
        };

        let start = out_tasks.len();
        if from_cache {
            out_tasks.push(task);
        } else if terminal_event.is_some() {
            drain_emitted_chunks_before(
                task,
                &mut state.sink,
                state.terminal_body.is_upgraded(),
                out_tasks,
            );
        } else {
            drain_emitted_chunks(task, &mut state.sink, out_tasks);
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
        } else if filter_downstream_body || terminal_event.is_some() {
            self.downstream_response_body_filter_tasks(session, &mut out_tasks[start..], ctx)
                .await?;
        }
        session
            .prepare_response_headers(&mut out_tasks[start..])
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "response_pipeline_tests.rs"]
mod tests;
