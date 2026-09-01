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

use super::{ExampleProxyHttp, CACHE_BACKEND, CTX};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use once_cell::sync::Lazy;
use pingora_cache::storage::{HandleHit, Storage};
use pingora_cache::CacheKey;
use pingora_core::protocols::{l4::socket::SocketAddr, Digest};
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::{Error, ErrorType::InternalError, Result};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{
    ProxyHttp, ResponseBodySink, ResponseHeadCommitPlan, ResponseHeadHoldLimits,
    ResponseHeadReplacement, Session, UpstreamResponseBodyEvent, RESPONSE_BODY_EMIT_CHUNK_BUDGET,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Default)]
pub(super) struct HttpResponseFeatureState {
    withheld_body: Vec<u8>,
    terminal_before_trailers_seen: bool,
    emitted_chunk_limit: bool,
    response_head_decided: bool,
}

#[derive(Default)]
pub(super) struct CacheResponseFeatureState {
    conn_reused: bool,
    upstream_client_addr: Option<SocketAddr>,
    upstream_server_addr: Option<SocketAddr>,
    withheld_body: Vec<u8>,
}

/// Terminal (`end_of_stream`) `upstream_response_body_filter` dispatches,
/// counted per `x-eos-probe` request header value.
///
/// A test that must prove the terminal callback was *not* dispatched cannot
/// read that off the client-visible body: when the exchange fails mid-body the
/// HTTP client discards what it already received, so an accidental dispatch
/// looks exactly like a correct skip. This map is written from inside the
/// filter, so it survives a failed body collection.
pub(super) static EOS_PROBES: Lazy<Mutex<HashMap<String, usize>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Downstream `response_trailer_filter` invocations, keyed by the request's
/// `x-downstream-trailer-probe` value. This is separate from client-visible
/// bytes so failure tests can prove the hook ran even though the response is
/// intentionally aborted before its trailers are written.
pub(super) static DOWNSTREAM_TRAILER_PROBES: Lazy<Mutex<HashMap<String, usize>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
pub(super) static DOWNSTREAM_TRAILER_LOG_ERRORS: Lazy<Mutex<HashMap<String, String>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
pub(super) static EMIT_CHUNK_LIMIT_LOG_ERRORS: Lazy<Mutex<HashMap<String, String>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Remove and return the terminal dispatch count recorded for `probe`.
///
/// Removing on read keeps the map bounded and keeps a probe id from leaking
/// into another test.
pub fn take_eos_dispatches(probe: &str) -> usize {
    EOS_PROBES.lock().unwrap().remove(probe).unwrap_or(0)
}

pub fn take_downstream_trailer_filter_calls(probe: &str) -> usize {
    DOWNSTREAM_TRAILER_PROBES
        .lock()
        .unwrap()
        .remove(probe)
        .unwrap_or(0)
}

pub fn take_downstream_trailer_logging_error(probe: &str) -> Option<String> {
    DOWNSTREAM_TRAILER_LOG_ERRORS.lock().unwrap().remove(probe)
}

pub fn take_emit_chunk_limit_logging_error(probe: &str) -> Option<String> {
    EMIT_CHUNK_LIMIT_LOG_ERRORS.lock().unwrap().remove(probe)
}

pub(super) fn record_downstream_trailer_filter(session: &Session) -> Result<()> {
    let probe = session.get_header_bytes("x-downstream-trailer-probe");
    if !probe.is_empty() {
        let probe = String::from_utf8_lossy(probe).into_owned();
        *DOWNSTREAM_TRAILER_PROBES
            .lock()
            .unwrap()
            .entry(probe)
            .or_insert(0) += 1;
    }
    if session.get_header_bytes("x-downstream-trailer-error") == b"true" {
        return Error::e_explain(
            InternalError,
            "scripted downstream response trailer rejection",
        );
    }
    Ok(())
}

/// Count one terminal dispatch for the request's probe id, if it carries one.
pub(super) fn record_eos_dispatch(session: &Session) {
    let probe = session.get_header_bytes("x-eos-probe");
    if probe.is_empty() {
        return;
    }
    let probe = String::from_utf8_lossy(probe).into_owned();
    *EOS_PROBES.lock().unwrap().entry(probe).or_insert(0) += 1;
}

pub(super) fn http_response_head_commit_plan(session: &Session) -> Result<ResponseHeadCommitPlan> {
    if session.get_header_bytes("x-response-head-hold").is_empty() {
        return Ok(ResponseHeadCommitPlan::Immediate);
    }
    Ok(ResponseHeadCommitPlan::hold(ResponseHeadHoldLimits::new(
        64 * 1024,
        64 * 1024,
        64,
        128,
        64 * 1024,
        128,
        Duration::from_secs(2),
    )))
}

pub(super) async fn http_upstream_response_body_filter(
    session: &mut Session,
    body: &mut Option<Bytes>,
    end_of_stream: bool,
    sink: &mut ResponseBodySink,
    state: &mut HttpResponseFeatureState,
) -> Result<Option<Duration>> {
    if !state.response_head_decided {
        match session.get_header_bytes("x-response-head-hold") {
            b"release" => {
                assert!(sink.release_response_head());
                state.response_head_decided = true;
            }
            b"replace" => {
                let mut replacement = ResponseHeader::build(403, Some(3))?;
                replacement.insert_header("content-type", "text/plain")?;
                replacement.insert_header("content-length", "7")?;
                replacement.insert_header("x-response-head-replacement", "true")?;
                sink.replace_response_head(ResponseHeadReplacement::new(
                    Box::new(replacement),
                    vec![Bytes::from_static(b"blocked")],
                ))?;
                state.response_head_decided = true;
            }
            _ => {}
        }
    }
    if end_of_stream {
        record_eos_dispatch(session);
    }
    if let Some(mode) = session.req_header().headers.get("x-emit-chunk-limit") {
        *body = None;
        if !state.emitted_chunk_limit {
            state.emitted_chunk_limit = true;
            for _ in 0..RESPONSE_BODY_EMIT_CHUNK_BUDGET {
                sink.push(Bytes::from_static(b"x"))?;
            }
            if mode == "overflow" {
                sink.push(Bytes::from_static(b"y"))?;
            }
        }
    }
    if session.get_header_bytes("x-bodyless-replace") == b"true" && end_of_stream && body.is_none()
    {
        *body = Some(Bytes::from_static(b"generated"));
        sink.push(Bytes::from_static(b"-extra"))?;
        sink.terminate();
    }
    retain_until_eos(session, body, end_of_stream, &mut state.withheld_body);
    Ok(None)
}

pub(super) async fn http_upstream_response_body_filter_event(
    session: &mut Session,
    body: &mut Option<Bytes>,
    event: UpstreamResponseBodyEvent,
    sink: &mut ResponseBodySink,
    state: &mut HttpResponseFeatureState,
) -> Result<Option<Duration>> {
    if event == UpstreamResponseBodyEvent::TerminalBeforeTrailers {
        state.terminal_before_trailers_seen = true;
    }
    let end_of_stream = !matches!(
        event,
        UpstreamResponseBodyEvent::Data {
            end_of_stream: false
        }
    );
    http_upstream_response_body_filter(session, body, end_of_stream, sink, state).await
}

pub(super) async fn http_upstream_response_trailer_filter(
    session: &mut Session,
    trailers: &mut HeaderMap,
    state: &mut HttpResponseFeatureState,
) -> Result<()> {
    if session.get_header_bytes("x-assert-trailer-order") == b"true"
        && !state.terminal_before_trailers_seen
    {
        return Error::e_explain(
            InternalError,
            "trailer hook ran before typed terminal event",
        );
    }
    if let Ok(delay_ms) = std::str::from_utf8(session.get_header_bytes("x-trailer-delay-ms"))
        .unwrap_or_default()
        .parse::<u64>()
    {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    if session.get_header_bytes("x-trailer-error") == b"true" {
        return Error::e_explain(InternalError, "scripted upstream trailer rejection");
    }
    match session.get_header_bytes("x-assert-trailer-capability") {
        b"true" if !session.downstream_session.response_trailers_supported() => {
            return Error::e_explain(
                InternalError,
                "expected downstream response trailers to be supported",
            );
        }
        b"false" if session.downstream_session.response_trailers_supported() => {
            return Error::e_explain(
                InternalError,
                "expected downstream response trailers to be unsupported",
            );
        }
        _ => {}
    }
    if session.get_header_bytes("x-assert-response-uncommitted") == b"true"
        && session.response_written().is_some()
    {
        return Error::e_explain(
            InternalError,
            "expected same-batch trailer hook before response commit",
        );
    }
    if session.get_header_bytes("x-trailer-mutate") == b"true" {
        trailers.insert("x-filtered-trailer", HeaderValue::from_static("yes"));
    }
    if session.get_header_bytes("x-trailer-clear") == b"true" {
        trailers.clear();
    }
    Ok(())
}

pub(super) async fn http_response_trailer_filter(
    session: &mut Session,
    _trailers: &mut HeaderMap,
) -> Result<Option<Bytes>> {
    record_downstream_trailer_filter(session)?;
    Ok(None)
}

pub(super) async fn http_logging(session: &mut Session, error: Option<&Error>) {
    let trailer_probe = session.get_header_bytes("x-downstream-trailer-probe");
    if !trailer_probe.is_empty() {
        if let Some(error) = error {
            DOWNSTREAM_TRAILER_LOG_ERRORS.lock().unwrap().insert(
                String::from_utf8_lossy(trailer_probe).into_owned(),
                format!("{:?}|{:?}|{error}", error.etype(), error.esource()),
            );
        }
    }
    let emit_probe = session.get_header_bytes("x-emit-chunk-limit-probe");
    if !emit_probe.is_empty() {
        if let Some(error) = error {
            EMIT_CHUNK_LIMIT_LOG_ERRORS.lock().unwrap().insert(
                String::from_utf8_lossy(emit_probe).into_owned(),
                format!("{:?}|{:?}|{error}", error.etype(), error.esource()),
            );
        }
    }
}

pub(super) async fn http_upstream_response_header_filter_event(
    proxy: &ExampleProxyHttp,
    session: &mut Session,
    upstream_response: &mut ResponseHeader,
    end_of_stream: bool,
    ctx: &mut CTX,
) -> Result<()> {
    upstream_response.insert_header(
        "x-upstream-header-eos",
        if end_of_stream { "true" } else { "false" },
    )?;
    <ExampleProxyHttp as ProxyHttp>::upstream_response_filter(
        proxy,
        session,
        upstream_response,
        ctx,
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheEntryState {
    None,
    Partial,
    Complete,
}

/// Test-only inspection using public cache interfaces. A complete memory hit
/// is seekable, while a streaming partial hit is intentionally not.
pub async fn cache_entry_state(host: &str, path_and_query: &str) -> CacheEntryState {
    let key = CacheKey::new(format!("{host}{path_and_query}"), String::new());
    let trace = pingora_cache::trace::Span::inactive().handle();
    match CACHE_BACKEND.lookup(&key, &trace).await.unwrap() {
        None => CacheEntryState::None,
        Some((_meta, hit)) if hit.can_seek() => CacheEntryState::Complete,
        Some(_) => CacheEntryState::Partial,
    }
}

pub(super) fn configure_cache_upstream_compression(session: &mut Session) {
    if session
        .req_header()
        .headers
        .get("x-upstream-compression")
        .is_some()
    {
        session.upstream_compression.adjust_level(6);
    }
}

pub(super) fn configure_cache_h2_windows(request: &RequestHeader, peer: &mut HttpPeer) {
    if let Some(window) = request.headers.get("x-h2-stream-window-size") {
        peer.options.h2_stream_window_size = Some(window.to_str().unwrap().parse().unwrap());
    }
    if let Some(window) = request.headers.get("x-h2-connection-window-size") {
        peer.options.h2_connection_window_size = Some(window.to_str().unwrap().parse().unwrap());
    }
}

pub(super) async fn cache_upstream_response_body_filter(
    session: &mut Session,
    body: &mut Option<Bytes>,
    end_of_stream: bool,
    _sink: &mut ResponseBodySink,
    state: &mut CacheResponseFeatureState,
) -> Result<Option<Duration>> {
    if session.get_header_bytes("x-test-local-response-body-failure") == b"true"
        && body.as_ref().is_some_and(|body| !body.is_empty())
    {
        return Error::e_explain(InternalError, "test local response body filter failure");
    }
    retain_until_eos(session, body, end_of_stream, &mut state.withheld_body);
    Ok(None)
}

pub(super) async fn cache_upstream_response_body_filter_event(
    session: &mut Session,
    body: &mut Option<Bytes>,
    event: UpstreamResponseBodyEvent,
    sink: &mut ResponseBodySink,
    state: &mut CacheResponseFeatureState,
) -> Result<Option<Duration>> {
    let end_of_stream = !matches!(
        event,
        UpstreamResponseBodyEvent::Data {
            end_of_stream: false
        }
    );
    cache_upstream_response_body_filter(session, body, end_of_stream, sink, state).await
}

pub(super) fn add_cache_connection_observation_headers(
    response: &mut ResponseHeader,
    state: &CacheResponseFeatureState,
) -> Result<()> {
    if state.conn_reused {
        response.insert_header("x-conn-reuse", "1")?;
    }
    response.insert_header(
        "x-upstream-client-addr",
        state
            .upstream_client_addr
            .as_ref()
            .map_or_else(|| "unset".into(), |addr| addr.to_string()),
    )?;
    response.insert_header(
        "x-upstream-server-addr",
        state
            .upstream_server_addr
            .as_ref()
            .map_or_else(|| "unset".into(), |addr| addr.to_string()),
    )?;
    Ok(())
}

pub(super) fn record_cache_upstream_connection(
    reused: bool,
    digest: Option<&Digest>,
    state: &mut CacheResponseFeatureState,
) -> Result<()> {
    state.conn_reused = reused;
    let socket_digest = digest
        .expect("upstream connector digest should be set for HTTP sessions")
        .socket_digest
        .as_ref()
        .expect("socket digest should be set for HTTP sessions");
    state.upstream_client_addr = socket_digest.local_addr().cloned();
    state.upstream_server_addr = socket_digest.peer_addr().cloned();
    Ok(())
}

pub(super) fn retain_until_eos(
    session: &Session,
    body: &mut Option<Bytes>,
    end_of_stream: bool,
    withheld_body: &mut Vec<u8>,
) {
    // A processor that withholds every chunk and releases the whole body
    // only at end-of-stream -- the shape that silently loses the entire
    // response when a termination never delivers `end_of_stream`.
    //
    // The `|eos` marker is appended by the terminal callback itself, so the
    // client-visible body doubles as the callback count: a second terminal
    // dispatch would append a second marker.
    if session.get_header_bytes("x-retain-until-eos") == b"true" {
        if let Some(bytes) = body.take() {
            withheld_body.extend_from_slice(&bytes);
        }
        if end_of_stream {
            let mut released = std::mem::take(withheld_body);
            released.extend_from_slice(b"|eos");
            *body = Some(Bytes::from(released));
        }
    }
}
