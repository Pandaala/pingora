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

use crate::proxy_cache::ServeFromCache;
use crate::{HttpProxy, ProxyHttp, ResponseBodySink, Session};
use bytes::Bytes;
use log::warn;
#[allow(unused_imports)]
use pingora_cache::{CachePhase, HttpCache, NoCacheReason, RespCacheable};
use pingora_core::connectors::http::custom;
use pingora_core::protocols::http::HttpTask;
use pingora_error::Result;
use pingora_http::ResponseHeader;

impl<SV, C> HttpProxy<SV, C>
where
    C: custom::Connector,
{
    pub(super) fn track_predicted_uncacheable_response(
        &self,
        session: &mut Session,
        task: &HttpTask,
        sink: &ResponseBodySink,
    ) {
        if !matches!(
            session.cache.phase(),
            CachePhase::Disabled(NoCacheReason::PredictedResponseTooLarge)
        ) {
            return;
        }

        let task_bytes = match task {
            HttpTask::Body(Some(data), _) | HttpTask::UpgradedBody(Some(data), _) => data.len(),
            _ => 0,
        };
        let emitted_bytes = sink.peek_extra().iter().map(Bytes::len).sum::<usize>();
        session
            .cache
            .track_body_bytes_for_max_file_size(task_bytes + emitted_bytes);
        if task.is_end()
            && !matches!(task, HttpTask::Failed(_))
            && !session.cache.exceeded_max_file_size()
        {
            session.cache.response_became_cacheable();
        }
    }

    /// Feed `task` and everything the upstream body filter queued in `sink`
    /// (via [`ResponseBodySink::push`]) to the cache, in the same order
    /// [`super::proxy_h1::HttpProxy::h1_response_filter`] (and its h2/custom
    /// siblings) hand them to the downstream writer -- see
    /// `drain_emitted_chunks` below for that side.
    ///
    /// `task`'s own end-of-stream flag is migrated onto the LAST queued
    /// chunk instead of staying on `task`, whenever there is at least one
    /// queued chunk. Caching `task` unmodified AND every emitted chunk would
    /// tell the cache the response ended twice: [`HttpCache::miss_handler`]
    /// (see its `// this will panic ... should be impossible in real world`
    /// comment above) takes the [`pingora_cache::MissHandler`] the first
    /// time it sees `end_stream`, and the very next `Body` write after that
    /// unwraps `None` and panics. A `Body(Some(data), true)` /
    /// `UpgradedBody(Some(data), true)` keeps `data` but drops its own
    /// end-of-stream flag; a bare `Body(None, true)` / `UpgradedBody(None,
    /// true)` end marker carries no payload of its own and is skipped
    /// entirely -- the last chunk now carries its meaning instead. See
    /// `migrate_end_of_stream`.
    ///
    /// Every cache write here (the migrated `task` and each chunk) follows
    /// the same failure policy as the original single-task path: on error,
    /// disable the cache; if [`ServeFromCache::is_miss_body`] is set, the
    /// response is already streaming into the cache during a miss, so a
    /// write failure must give up the entire request (a partial cache write
    /// would otherwise leave a stream promising bytes it no longer has)
    /// rather than let the client silently receive fewer bytes than the
    /// cache now expects; otherwise, warn and stop feeding the cache instead
    /// of retrying against a cache already disabled.
    pub(super) async fn cache_task_and_emitted_chunks(
        &self,
        session: &mut Session,
        task: &HttpTask,
        sink: &ResponseBodySink,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        self.cache_task_and_emitted_chunks_with_decision(
            session,
            task,
            sink,
            None,
            ctx,
            serve_from_cache,
        )
        .await
    }

    pub(super) fn response_cacheability_before_downstream_filter(
        &self,
        session: &Session,
        header: &ResponseHeader,
        ctx: &mut SV::CTX,
    ) -> Result<Option<RespCacheable>>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        if !(session.cache.enabled() || session.cache.bypassing()) {
            return Ok(None);
        }
        Ok(Some(
            self.inner.response_cache_filter(session, header, ctx)?,
        ))
    }

    pub(super) async fn cache_task_and_emitted_chunks_with_decision(
        &self,
        session: &mut Session,
        task: &HttpTask,
        sink: &ResponseBodySink,
        response_cacheability: Option<RespCacheable>,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        if !(session.cache.enabled() || session.cache.bypassing()) {
            return Ok(());
        }

        let extra = sink.peek_extra();
        let is_upgraded = matches!(task, HttpTask::UpgradedBody(..));
        let leading = if !extra.is_empty() || matches!(task, HttpTask::Header(_, true)) {
            migrate_end_of_stream(task)
        } else {
            LeadingTask::Unchanged
        };

        let mut response_cacheability = response_cacheability;
        let leading_result = match &leading {
            LeadingTask::Unchanged => {
                self.cache_http_task(
                    session,
                    task,
                    response_cacheability.take(),
                    ctx,
                    serve_from_cache,
                )
                .await
            }
            LeadingTask::Substitute(substitute) => {
                self.cache_http_task(
                    session,
                    substitute,
                    response_cacheability.take(),
                    ctx,
                    serve_from_cache,
                )
                .await
            }
            LeadingTask::Drop => Ok(()),
        };
        if let Err(e) = leading_result {
            session.cache.disable(NoCacheReason::StorageError);
            if serve_from_cache.is_miss_body() {
                // if the response stream cache body during miss but write fails, it has to
                // give up the entire request
                return Err(e);
            }
            warn!(
                target: "pingora_proxy::proxy_cache",
                "Fail to cache response: {}, {}",
                e,
                self.inner.request_summary(session, ctx)
            );
            // The cache is already disabled: cache_http_task would no-op on
            // every remaining chunk anyway, so stop here instead of
            // repeating the same warning per chunk.
            return Ok(());
        }

        if extra.is_empty() {
            if matches!(task, HttpTask::Header(_, true)) {
                self.cache_http_task(
                    session,
                    &HttpTask::Body(None, true),
                    None,
                    ctx,
                    serve_from_cache,
                )
                .await?;
            }
            return Ok(());
        }
        let last = extra.len() - 1;
        let last_chunk_is_end_of_stream = !matches!(leading, LeadingTask::Unchanged);
        for (i, chunk) in extra.iter().enumerate() {
            let end = last_chunk_is_end_of_stream && i == last;
            let extra_task = if is_upgraded {
                HttpTask::UpgradedBody(Some(chunk.clone()), end)
            } else {
                HttpTask::Body(Some(chunk.clone()), end)
            };
            if let Err(e) = self
                .cache_http_task(session, &extra_task, None, ctx, serve_from_cache)
                .await
            {
                session.cache.disable(NoCacheReason::StorageError);
                if serve_from_cache.is_miss_body() {
                    return Err(e);
                }
                warn!(
                    target: "pingora_proxy::proxy_cache",
                    "Fail to cache emitted response chunk: {}, {}",
                    e,
                    self.inner.request_summary(session, ctx)
                );
                break;
            }
        }
        Ok(())
    }

    /// Cache the chunks `sink` queued BEFORE `task`, the admission-side mirror
    /// of [`drain_emitted_chunks_before`].
    ///
    /// The cached entity must be byte-identical to what the client receives, so
    /// a terminal `Trailer`/`Done` that released withheld body bytes has to feed
    /// those bytes to the cache ahead of the terminal task, exactly as they are
    /// written downstream.
    ///
    /// Load-bearing for the bare-`Done` dispatch specifically: `Done` runs
    /// `finish_miss_handler()` in [`Self::cache_http_task`], so admitting the
    /// released bytes after it would `write_body` into an already finished miss
    /// handler -- the failure the "this will panic if more data is sent after we
    /// see end_stream" note on the `Body` arm warns about. `Trailer` is a cache
    /// no-op, so only the ordering around `Done` changes the stored entity.
    ///
    /// No end-of-stream migration, for the same reason as the downstream drain:
    /// `Trailer`/`Done` already carry the response's single completion.
    pub(super) async fn cache_task_and_emitted_chunks_before(
        &self,
        session: &mut Session,
        task: &HttpTask,
        sink: &ResponseBodySink,
        is_upgraded: bool,
        ctx: &mut SV::CTX,
        serve_from_cache: &mut ServeFromCache,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        if !(session.cache.enabled() || session.cache.bypassing()) {
            return Ok(());
        }

        for chunk in sink.peek_extra() {
            let extra_task = if is_upgraded {
                HttpTask::UpgradedBody(Some(chunk.clone()), false)
            } else {
                HttpTask::Body(Some(chunk.clone()), false)
            };
            if let Err(e) = self
                .cache_http_task(session, &extra_task, None, ctx, serve_from_cache)
                .await
            {
                session.cache.disable(NoCacheReason::StorageError);
                if serve_from_cache.is_miss_body() {
                    // The miss body is already short: giving up the request is
                    // the only way to avoid storing a truncated entity.
                    return Err(e);
                }
                warn!(
                    target: "pingora_proxy::proxy_cache",
                    "Fail to cache released response chunk: {}, {}",
                    e,
                    self.inner.request_summary(session, ctx)
                );
                // The cache is disabled now; the terminal task below would
                // no-op anyway.
                return Ok(());
            }
        }

        if let Err(e) = self
            .cache_http_task(session, task, None, ctx, serve_from_cache)
            .await
        {
            session.cache.disable(NoCacheReason::StorageError);
            if serve_from_cache.is_miss_body() {
                return Err(e);
            }
            warn!(
                target: "pingora_proxy::proxy_cache",
                "Fail to cache response: {}, {}",
                e,
                self.inner.request_summary(session, ctx)
            );
        }
        Ok(())
    }
}

/// What migrating `task`'s end-of-stream flag onto a queued
/// [`ResponseBodySink`] chunk means for `task` itself. Only meaningful when
/// there is at least one queued chunk to migrate it onto -- callers skip
/// calling [`migrate_end_of_stream`] entirely otherwise and use
/// [`LeadingTask::Unchanged`] directly for that hot, sink-empty path.
enum LeadingTask {
    /// `task` did not carry an end-of-stream flag that can move to an emitted
    /// body chunk: feed `task` exactly as-is.
    Unchanged,
    /// `task` carried a payload; feed this de-asserted (`end = false`)
    /// substitute in its place, then the queued chunks -- the LAST of which
    /// now carries `end = true` instead of `task`.
    Substitute(HttpTask),
    /// `task` was a bare end-of-stream marker with no payload of its own
    /// (`Body(None, true)` / `UpgradedBody(None, true)`): drop it entirely
    /// and let the last queued chunk carry its meaning instead of also
    /// emitting a now-redundant marker.
    Drop,
}

/// Decide how `task`'s own end-of-stream flag must move to make room for the
/// chunks a response-body filter queued after it. Only called by
/// [`HttpProxy::cache_task_and_emitted_chunks`] and `drain_emitted_chunks`
/// when the sink has at least one chunk queued; see their doc comments for
/// why the flag cannot simply be duplicated onto both `task` and the chunks.
fn migrate_end_of_stream(task: &HttpTask) -> LeadingTask {
    match task {
        HttpTask::Header(header, true) => {
            LeadingTask::Substitute(HttpTask::Header(header.clone(), false))
        }
        HttpTask::Body(Some(data), true) => {
            LeadingTask::Substitute(HttpTask::Body(Some(data.clone()), false))
        }
        HttpTask::Body(None, true) => LeadingTask::Drop,
        HttpTask::UpgradedBody(Some(data), true) => {
            LeadingTask::Substitute(HttpTask::UpgradedBody(Some(data.clone()), false))
        }
        HttpTask::UpgradedBody(None, true) => LeadingTask::Drop,
        _ => LeadingTask::Unchanged,
    }
}

/// Drain the chunks `sink` queued and append them to `out_tasks` right after
/// `task`, migrating `task`'s own end-of-stream flag onto the last of them
/// exactly as [`HttpProxy::cache_task_and_emitted_chunks`] does for the
/// cache -- see that function's doc comment for the failure this prevents.
/// Cheap and branch-free when nothing was queued (the hot path: almost every
/// call has an empty sink).
///
/// Chunks are re-emitted under `task`'s own variant (`Body` stays `Body`,
/// `UpgradedBody` stays `UpgradedBody`): `Session::write_response_tasks`
/// tracks `seen_upgraded` off this tag to pick the raw post-upgrade duplex
/// write path over the normal framed one, so mistagging a chunk here would
/// misroute its bytes on an upgraded (e.g. WebSocket) connection.
pub(super) fn drain_emitted_chunks(
    task: HttpTask,
    sink: &mut ResponseBodySink,
    out_tasks: &mut Vec<HttpTask>,
) {
    let extra = sink.take_extra();
    if extra.is_empty() {
        if let HttpTask::Header(header, true) = task {
            out_tasks.push(HttpTask::Header(header, false));
            out_tasks.push(HttpTask::Body(None, true));
        } else {
            out_tasks.push(task);
        }
        return;
    }

    let is_upgraded = matches!(task, HttpTask::UpgradedBody(..));
    let leading = migrate_end_of_stream(&task);
    let last_chunk_is_end_of_stream = !matches!(leading, LeadingTask::Unchanged);
    match leading {
        LeadingTask::Unchanged => out_tasks.push(task),
        LeadingTask::Substitute(substitute) => out_tasks.push(substitute),
        LeadingTask::Drop => {}
    }

    let last = extra.len() - 1;
    for (i, chunk) in extra.into_iter().enumerate() {
        let end = last_chunk_is_end_of_stream && i == last;
        out_tasks.push(if is_upgraded {
            HttpTask::UpgradedBody(Some(chunk), end)
        } else {
            HttpTask::Body(Some(chunk), end)
        });
    }
}

/// Drain the chunks `sink` queued and append them to `out_tasks` BEFORE
/// `task`, the mirror image of [`drain_emitted_chunks`].
///
/// Used only for a terminal `Trailer`/`Done` that dispatched the terminal
/// `upstream_response_body_filter` callback (see
/// `response_pipeline::TerminalBodyDispatch`). The chunks queued there are response
/// BODY the filter had been withholding, so they must reach the wire before the
/// trailer that terminates the response, not after it.
///
/// No end-of-stream migration happens here, unlike [`drain_emitted_chunks`]:
/// `Trailer` and `Done` are intrinsically `HttpTask::is_end()`, so `task`
/// already carries the response's single completion and the released chunks are
/// always emitted with `end = false`. Migrating would either duplicate the
/// completion or, because [`migrate_end_of_stream`] reports `Unchanged` for
/// both variants, strand the released bytes after a terminal marker.
///
/// `is_upgraded` comes from the latch rather than from `task`, which is a
/// `Trailer`/`Done` and carries no body variant of its own -- see
/// `response_pipeline::TerminalBodyDispatch::is_upgraded` for why the tag must be preserved.
pub(super) fn drain_emitted_chunks_before(
    task: HttpTask,
    sink: &mut ResponseBodySink,
    is_upgraded: bool,
    out_tasks: &mut Vec<HttpTask>,
) {
    for chunk in sink.take_extra() {
        out_tasks.push(if is_upgraded {
            HttpTask::UpgradedBody(Some(chunk), false)
        } else {
            HttpTask::Body(Some(chunk), false)
        });
    }
    out_tasks.push(task);
}
