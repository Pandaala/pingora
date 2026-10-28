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

use super::*;
use crate::response_pipeline::{normalize_trailers, TerminalBodyDispatch};
use crate::UpstreamResponseBodyEvent;
use http::HeaderMap;
use pingora_core::protocols::http::HttpTask;

fn request_with_headers(headers: &[(&str, &str)]) -> RequestHeader {
    let mut request = RequestHeader::build("GET", b"/", Some(headers.len())).unwrap();
    request.set_version(http::Version::HTTP_11);
    for (name, value) in headers {
        request
            .append_header(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                http::HeaderValue::from_str(value).unwrap(),
            )
            .unwrap();
    }
    request
}
use pingora_error::ErrorType::InternalError;
use pingora_http::ResponseHeader;

fn header(eos: bool) -> HttpTask {
    HttpTask::Header(Box::new(ResponseHeader::build(200, None).unwrap()), eos)
}

fn body(eos: bool) -> HttpTask {
    HttpTask::Body(Some(Bytes::from_static(b"chunk")), eos)
}

fn trailer() -> HttpTask {
    HttpTask::Trailer(Some(Box::default()))
}

fn failed() -> HttpTask {
    HttpTask::Failed(Error::explain(InternalError, "upstream aborted"))
}

/// Feed a whole response through one latch and collect, for each task,
/// whether it dispatched the terminal callback.
fn dispatches(tasks: &[HttpTask]) -> Vec<Option<UpstreamResponseBodyEvent>> {
    let mut latch = TerminalBodyDispatch::default();
    tasks.iter().map(|t| latch.claim_for(t)).collect()
}

/// The defect this latch exists for: H2 puts END_STREAM on the trailers
/// HEADERS frame, so every DATA frame arrives with `eos = false`. The
/// `Trailer` must dispatch, and the `Done` behind it must not repeat it.
#[test]
fn trailered_response_dispatches_once_on_the_trailer() {
    assert_eq!(
        dispatches(&[
            header(false),
            body(false),
            body(false),
            trailer(),
            HttpTask::Done
        ]),
        [
            None,
            None,
            None,
            Some(UpstreamResponseBodyEvent::TerminalBeforeTrailers),
            None
        ]
    );
}

#[test]
fn connection_nomination_rejects_protected_header() {
    for token in [
        "Host",
        "x-forwarded-for",
        "X-Forwarded-For",
        "X-FORWARDED-HOST",
        "x-Forwarded-Proto",
    ] {
        let mut request = request_with_headers(&[("Connection", token)]);
        assert!(
            sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                .is_err(),
            "protected nomination should be rejected regardless of casing: {token:?}"
        );
    }
}

#[test]
fn connection_nomination_rejects_pseudo_header() {
    for token in [":authority", ":method", ":path"] {
        let mut request = request_with_headers(&[("Connection", token)]);
        assert!(
            sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                .is_err(),
            "pseudo-header nomination should be rejected: {token:?}"
        );
    }
}

/// A nomination that is not a valid `token` is rejected outright instead of silently dropped.
#[test]
fn connection_nomination_rejects_malformed_token() {
    let mut request = request_with_headers(&[("Connection", "keep-alive, bad token")]);
    assert!(
        sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard()).is_err()
    );
}

/// A protected name decorated with any non-`token` byte is rejected, independent of how
/// permissive the header-name parser is.
#[test]
fn connection_nomination_rejects_decorated_protected_header() {
    for token in [
        "\"X-Forwarded-For\"",
        "(X-Forwarded-For",
        "X-Forwarded-For)",
        "X-Forwarded-For/",
        "X-Forwarded-For:",
        "X -Forwarded-For",
        "@X-Forwarded-For",
    ] {
        let mut request =
            request_with_headers(&[("Connection", token), ("X-Forwarded-For", "6.6.6.6")]);
        assert!(
            sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                .is_err(),
            "decorated protected nomination should be rejected: {token:?}"
        );
    }
}

/// A protected name decorated with a valid `tchar` (e.g. `'X-Forwarded-For'`) is a well-formed
/// nomination of a *distinct* header: accepted, but harmless — the real header is untouched.
#[test]
fn connection_nomination_allows_tchar_decorated_lookalike() {
    for token in ["'X-Forwarded-For'", "X-Forwarded-For.", "!X-Forwarded-For"] {
        let mut request =
            request_with_headers(&[("Connection", token), ("X-Forwarded-For", "6.6.6.6")]);
        assert!(
            sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard())
                .is_ok(),
            "tchar-decorated lookalike is a distinct header, not a protected match: {token:?}"
        );
        assert_eq!(request.headers["x-forwarded-for"], "6.6.6.6");
    }
}

/// A policy that tolerates malformed `Connection` nominations while still stripping them.
fn lenient_policy() -> HttpUpstreamRequestPolicy {
    let mut policy = HttpUpstreamRequestPolicy::standard();
    policy.reject_malformed_connection_nominations = false;
    policy
}

/// In lenient mode a malformed nomination is tolerated: it targets a distinct field and leaves
/// the real protected header intact.
#[test]
fn lenient_connection_nomination_tolerates_malformed_token() {
    for token in [
        "\"X-Forwarded-For\"",
        "(X-Forwarded-For",
        "@X-Forwarded-For",
        "X -Forwarded-For",
        "keep-alive, bad token",
    ] {
        let mut request =
            request_with_headers(&[("Connection", token), ("X-Forwarded-For", "6.6.6.6")]);
        assert!(
            sanitize_h2_upstream_request(&mut request, lenient_policy()).is_ok(),
            "malformed nomination should be tolerated in lenient mode: {token:?}"
        );
        assert_eq!(request.headers["x-forwarded-for"], "6.6.6.6");
    }
}

/// Even in lenient mode, an exact protected or pseudo-header nomination is still rejected.
#[test]
fn lenient_connection_nomination_still_rejects_exact_protected() {
    for token in ["x-forwarded-for", "X-Forwarded-For", "host", ":authority"] {
        let mut request = request_with_headers(&[("Connection", token)]);
        assert!(
            sanitize_h2_upstream_request(&mut request, lenient_policy()).is_err(),
            "exact protected/pseudo nomination must be rejected even in lenient mode: {token:?}"
        );
    }
}

#[test]
fn normal_lifecycle() {
    let mut ds = DownstreamStateMachine::new(false);
    assert!(ds.is_reading());
    assert!(ds.can_poll());
    assert!(!ds.is_errored());

    ds.maybe_finished(true);
    assert!(!ds.is_reading());
    assert!(ds.is_done());
    assert!(ds.can_poll()); // ReadingFinished still allows polling (for idle)
    assert!(!ds.is_errored());
}

#[test]
fn errored_is_terminal() {
    let mut ds = DownstreamStateMachine::new(false);
    ds.to_errored();
    assert!(ds.is_errored());
    assert!(!ds.can_poll());
    assert!(ds.is_done());
}

/// `maybe_finished(false)` is always a no-op regardless of state.
#[test]
fn maybe_finished_false_is_noop() {
    let mut ds = DownstreamStateMachine::new(false);
    ds.to_errored();
    ds.maybe_finished(false); // must not panic
    assert!(ds.is_errored());
    assert!(!ds.can_poll());
}

/// `maybe_finished(true)` on `Errored` is a no-op — `Errored` is terminal.
#[test]
fn maybe_finished_true_noop_on_errored() {
    let mut ds = DownstreamStateMachine::new(false);
    ds.to_errored();
    ds.maybe_finished(true); // must not overwrite Errored
    assert!(ds.is_errored());
    assert!(!ds.can_poll());
}

/// `reset()` on `Errored` is a no-op — `Errored` is terminal.
#[test]
fn reset_noop_on_errored() {
    let mut ds = DownstreamStateMachine::new(false);
    ds.to_errored();
    ds.reset(); // must not overwrite Errored
    assert!(ds.is_errored());
    assert!(!ds.can_poll());
}

#[test]
fn bare_done_dispatches_when_nothing_claimed_the_termination() {
    assert_eq!(
        dispatches(&[header(false), body(false), HttpTask::Done]),
        [
            None,
            None,
            Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers)
        ]
    );
}

/// `Header(_, true)` is 204/304/HEAD/`CL:0`: the `terminal_header` branch
/// already runs `terminal_upstream_body_filter` for it.
#[test]
fn terminal_header_claims_without_dispatching() {
    assert_eq!(dispatches(&[header(true), HttpTask::Done]), [None, None]);
}

/// `Body(_, true)` carries `eos = true` into the body filter itself.
#[test]
fn terminal_body_claims_without_dispatching() {
    assert_eq!(
        dispatches(&[header(false), body(false), body(true), HttpTask::Done]),
        [None, None, None, None]
    );
}

/// A trailer arriving after a body task already ended the stream is not a
/// second termination.
#[test]
fn trailer_after_terminal_body_does_not_dispatch() {
    assert_eq!(
        dispatches(&[body(true), trailer(), HttpTask::Done]),
        [None, None, None]
    );
}

/// An aborted response must never be told its truncated body was complete,
/// and the `Done` that may follow the error must not say it either.
#[test]
fn failed_never_dispatches_and_suppresses_a_following_done() {
    assert_eq!(
        dispatches(&[header(false), body(false), failed(), HttpTask::Done]),
        [None, None, None, None]
    );
}

/// `Trailer(None)` is still a termination observation: `upstream_filter`
/// skips it (its match arm is `Trailer(Some(..))`), so if it were ignored
/// here the following `Done` would dispatch a second time.
#[test]
fn empty_trailer_claims_the_termination() {
    assert_eq!(
        dispatches(&[body(false), HttpTask::Trailer(None), HttpTask::Done]),
        [
            None,
            Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers),
            None
        ]
    );
}

/// Released bytes must inherit the response's body variant so
/// `write_response_tasks` keeps routing them down the post-upgrade duplex
/// path. The terminal `Done` itself carries no variant.
#[test]
fn upgraded_body_is_remembered_for_the_terminal_dispatch() {
    let mut latch = TerminalBodyDispatch::default();
    assert!(!latch.is_upgraded());
    latch.claim_for(&HttpTask::UpgradedBody(
        Some(Bytes::from_static(b"frame")),
        false,
    ));
    assert!(latch.is_upgraded());
    assert_eq!(
        latch.claim_for(&HttpTask::Done),
        Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers)
    );
    assert!(latch.is_upgraded());
}

/// An upgraded response can close before yielding any body task. The final
/// filtered handshake must therefore establish the body variant on its
/// own, or bytes released by the later `Done` callback would be emitted as
/// plain `Body` into an already-upgraded downstream session.
#[test]
fn filtered_upgrade_handshake_marks_response_upgraded() {
    let mut latch = TerminalBodyDispatch::default();
    latch.mark_upgraded();
    assert!(latch.is_upgraded());
    assert_eq!(
        latch.claim_for(&HttpTask::Done),
        Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers)
    );
}

#[test]
fn plain_body_response_is_not_marked_upgraded() {
    let mut latch = TerminalBodyDispatch::default();
    latch.claim_for(&body(false));
    latch.claim_for(&trailer());
    assert!(!latch.is_upgraded());
}

/// The latch is per response, not per batch: a fresh one dispatches again.
#[test]
fn a_new_latch_dispatches_for_the_next_response() {
    let mut latch = TerminalBodyDispatch::default();
    assert!(latch.claim_for(&HttpTask::Done).is_some());
    assert!(latch.claim_for(&HttpTask::Done).is_none());
    assert!(TerminalBodyDispatch::default()
        .claim_for(&HttpTask::Done)
        .is_some());
}

#[test]
fn emptied_trailer_map_normalizes_to_no_trailer() {
    assert!(normalize_trailers(Some(Box::default())).is_none());

    let mut trailers = HeaderMap::new();
    trailers.insert("x-test", "present".parse().unwrap());
    assert!(normalize_trailers(Some(Box::new(trailers))).is_some());
}
