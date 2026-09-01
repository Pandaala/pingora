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

use crate::Session;
use bytes::Bytes;
use http::header::{self, HeaderName};
use log::warn;
use pingora_core::upstreams::peer::{H1UpgradePolicy, HttpUpstreamRequestPolicy};
use pingora_error::{Error, ErrorType::InvalidHTTPHeader, Result};
use pingora_http::RequestHeader;

const MAX_CONNECTION_NOMINATIONS: usize = 10;
pub(crate) const KEEP_ALIVE: &str = "keep-alive";
pub(crate) const PROXY_CONNECTION: &str = "proxy-connection";
pub(crate) const HTTP2_SETTINGS: &str = "http2-settings";

/// Whether an externally received HTTP/1 request uses transfer coding the proxy can forward
/// without changing the payload semantics.
///
/// Pingora's HTTP/1 body reader removes chunk framing, but it does not decode transfer codings
/// that precede `chunked`. The proxy's normal hop-by-hop sanitization then removes the complete
/// `Transfer-Encoding` field. Accepting anything other than one `chunked` field would therefore
/// forward coded bytes under different metadata.
pub(crate) fn h1_transfer_encoding_is_forwardable(req: &RequestHeader) -> bool {
    let mut values = req.headers.get_all(header::TRANSFER_ENCODING).iter();
    let Some(value) = values.next() else {
        return true;
    };

    values.next().is_none()
        && value
            .as_bytes()
            .trim_ascii()
            .eq_ignore_ascii_case(b"chunked")
}

/// Whether `byte` is a `tchar`, the character set of an HTTP `token` (RFC 9110 §5.6.2). Checked
/// here because `HeaderName::from_bytes` may accept bytes outside the `token` set.
fn is_tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn is_websocket_upgrade_request(req: &RequestHeader, downstream_is_http11: bool) -> bool {
    downstream_is_http11
        && req
            .headers
            .get(header::UPGRADE)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
}

struct ConnectionNominations {
    headers: [Option<HeaderName>; MAX_CONNECTION_NOMINATIONS],
    len: usize,
}

impl ConnectionNominations {
    fn parse(req: &RequestHeader, reject_malformed: bool) -> Result<Self> {
        let mut headers = std::array::from_fn(|_| None);
        let mut len = 0;
        let mut nomination_count = 0;

        // This is inspired by Envoy's defensive Connection-header sanitization checks. Bound the
        // amount of token processing so it cannot become a request-time DoS vector.
        for token in req
            .headers
            .get_all(header::CONNECTION)
            .iter()
            .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
            .map(|token| token.trim_ascii())
            .filter(|token| !token.is_empty())
        {
            nomination_count += 1;
            if nomination_count >= MAX_CONNECTION_NOMINATIONS {
                return Error::e_explain(
                    InvalidHTTPHeader,
                    "too many Connection header nominations",
                );
            }

            // `:`-prefixed tokens nominate pseudo-headers (e.g. `:authority`); rejected in both modes.
            if token.starts_with(b":") {
                return Error::e_explain(
                    InvalidHTTPHeader,
                    "protected header cannot be nominated by the Connection header",
                );
            }

            // A nomination is an HTTP `token` (RFC 9110 §5.6.2). We validate that ourselves rather
            // than trust `HeaderName::from_bytes`, which may accept non-`token` bytes and let a
            // decorated spelling like `Connection: "X-Forwarded-For"` slip past the protected-name
            // check below. The RFC lets a recipient reject or ignore a malformed option, so
            // `reject_malformed` is a policy choice: fail closed (default) or tolerate it.
            if reject_malformed && !token.iter().all(|&byte| is_tchar(byte)) {
                return Error::e_explain(
                    InvalidHTTPHeader,
                    "invalid token nominated by the Connection header",
                );
            }

            // `HeaderName` lowercases, so the protected-set check below cannot be evaded via casing.
            let name = match HeaderName::from_bytes(token) {
                Ok(name) => name,
                // Strict mode: every token is valid `tchar` and parses; a residual failure (e.g. a
                // length limit) still fails closed. Lenient mode: an unparsable token names
                // nothing, so ignore it.
                Err(_) if reject_malformed => {
                    return Error::e_explain(
                        InvalidHTTPHeader,
                        "invalid token nominated by the Connection header",
                    );
                }
                Err(_) => continue,
            };

            if matches!(
                name.as_str(),
                "host" | "x-forwarded-for" | "x-forwarded-host" | "x-forwarded-proto"
            ) {
                return Error::e_explain(
                    InvalidHTTPHeader,
                    "protected header cannot be nominated by the Connection header",
                );
            }

            headers[len] = Some(name);
            len += 1;
        }

        Ok(Self { headers, len })
    }

    fn remove_from(self, req: &mut RequestHeader) {
        for name in self.headers.into_iter().take(self.len).flatten() {
            req.remove_header(&name);
        }
    }
}

fn strip_standard_hop_by_hop_headers(req: &mut RequestHeader) {
    req.remove_header(KEEP_ALIVE);
    req.remove_header(PROXY_CONNECTION);
    req.remove_header(&header::PROXY_AUTHENTICATE);
    req.remove_header(&header::PROXY_AUTHORIZATION);
    req.remove_header(&header::TE);
    req.remove_header(&header::TRAILER);
    req.remove_header(&header::TRANSFER_ENCODING);
    req.remove_header(&header::CONNECTION);
    req.remove_header(&header::UPGRADE);
    req.remove_header(HTTP2_SETTINGS);
}

/// Apply automatic request policy before application upstream request filtering.
pub(crate) fn sanitize_h1_upstream_request(
    req: &mut RequestHeader,
    policy: HttpUpstreamRequestPolicy,
    downstream_is_http11: bool,
) -> Result<()> {
    if policy == HttpUpstreamRequestPolicy::preserve() {
        return Ok(());
    }

    let nominations = policy
        .strip_connection_nominated
        .then(|| ConnectionNominations::parse(req, policy.reject_malformed_connection_nominations))
        .transpose()?;

    if policy.h1_upgrade == H1UpgradePolicy::Preserve && req.headers.contains_key(header::UPGRADE) {
        // An arbitrary upgrade may require any of the connection-nominated fields. Preserve the
        // complete request metadata rather than forwarding a partial handshake.
        return Ok(());
    }

    let websocket_upgrade = policy.h1_upgrade == H1UpgradePolicy::WebSocketOnly
        && is_websocket_upgrade_request(req, downstream_is_http11);
    if let Some(nominations) = nominations {
        nominations.remove_from(req);
    }

    if policy.strip_hop_by_hop {
        strip_standard_hop_by_hop_headers(req);
    }

    match policy.h1_upgrade {
        H1UpgradePolicy::WebSocketOnly => {
            req.remove_header(&header::CONNECTION);
            req.remove_header(&header::UPGRADE);
            req.remove_header(HTTP2_SETTINGS);
            if websocket_upgrade {
                req.insert_header(header::CONNECTION, "Upgrade")?;
                req.insert_header(header::UPGRADE, "websocket")?;
            }
        }
        H1UpgradePolicy::Deny => {
            req.remove_header(&header::CONNECTION);
            req.remove_header(&header::UPGRADE);
            req.remove_header(HTTP2_SETTINGS);
        }
        H1UpgradePolicy::Preserve => {}
    }

    Ok(())
}

/// Frame a body-bearing HTTP/1 upstream request after application request filtering.
pub(crate) fn finalize_h1_upstream_request_framing(
    req: &mut RequestHeader,
    downstream_has_body: bool,
) -> Result<()> {
    if downstream_has_body
        && req.headers.get(header::CONTENT_LENGTH).is_none()
        && req.headers.get(header::TRANSFER_ENCODING).is_none()
    {
        req.insert_header(header::TRANSFER_ENCODING, "chunked")?;
    }
    Ok(())
}

/// Remove downstream connection-nominated fields before an HTTP/2 conversion.
pub(crate) fn sanitize_h2_upstream_request(
    req: &mut RequestHeader,
    policy: HttpUpstreamRequestPolicy,
) -> Result<()> {
    if policy.strip_connection_nominated {
        ConnectionNominations::parse(req, policy.reject_malformed_connection_nominations)?
            .remove_from(req);
    }
    if policy.strip_hop_by_hop {
        strip_standard_hop_by_hop_headers(req);
    }
    Ok(())
}

/// Possible downstream states during request multiplexing
#[derive(Debug, Clone, Copy)]
pub(crate) enum DownstreamStateMachine {
    /// more request (body) to read
    Reading,
    /// no more data to read
    ReadingFinished,
    /// downstream is already errored or closed
    Errored,
}

#[allow(clippy::wrong_self_convention)]
impl DownstreamStateMachine {
    pub fn new(finished: bool) -> Self {
        if finished {
            Self::ReadingFinished
        } else {
            Self::Reading
        }
    }

    // Can call read() to read more data or wait on closing
    pub fn can_poll(&self) -> bool {
        !matches!(self, Self::Errored)
    }

    pub fn is_reading(&self) -> bool {
        matches!(self, Self::Reading)
    }

    pub fn is_done(&self) -> bool {
        !matches!(self, Self::Reading)
    }

    pub fn is_errored(&self) -> bool {
        matches!(self, Self::Errored)
    }

    /// Move the state machine to Finished state if `set` is true.
    ///
    /// No-op when the current state is [`Errored`](Self::Errored) — once errored the
    /// downstream connection must not be reused, and late upstream chunks arriving
    /// via `rx.recv()` must not overwrite that decision.
    pub fn maybe_finished(&mut self, set: bool) {
        if set && !self.is_errored() {
            *self = Self::ReadingFinished
        }
    }

    /// Reset to [`Reading`](Self::Reading) for upgraded connections when body mode changes.
    ///
    /// No-op when the current state is [`Errored`](Self::Errored).
    pub fn reset(&mut self) {
        if !self.is_errored() {
            *self = Self::Reading;
        }
    }

    /// Transition to [`Errored`](Self::Errored). This is a terminal state: once entered,
    /// no other state transition is permitted and the connection must not be reused.
    pub fn to_errored(&mut self) {
        *self = Self::Errored
    }
}

/// Possible upstream states during request multiplexing
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResponseStateMachine {
    upstream_response_done: bool,
    cached_response_done: bool,
}

impl ResponseStateMachine {
    pub fn new() -> Self {
        ResponseStateMachine {
            upstream_response_done: false,
            cached_response_done: true, // no cached response by default
        }
    }

    pub fn is_done(&self) -> bool {
        self.upstream_response_done && self.cached_response_done
    }

    pub fn upstream_done(&self) -> bool {
        self.upstream_response_done
    }

    pub fn cached_done(&self) -> bool {
        self.cached_response_done
    }

    pub fn enable_cached_response(&mut self) {
        self.cached_response_done = false;
    }

    pub fn maybe_set_upstream_done(&mut self, done: bool) {
        if done {
            self.upstream_response_done = true;
        }
    }

    pub fn maybe_set_cache_done(&mut self, done: bool) {
        if done {
            self.cached_response_done = true;
        }
    }
}

/// Shared signal from the downstream proxy half to the upstream half: set to
/// [`DownstreamComplete`](Self::DownstreamComplete) right before the downstream
/// half returns successfully, so the upstream half can tell an expected pipe
/// closure (downstream finished the response by its own framing) from an
/// unexpected one.
///
/// Stored in an `AtomicU8` shared via `Arc`: the downstream half stores with
/// [`Release`](std::sync::atomic::Ordering::Release) and the upstream half loads
/// with [`Acquire`](std::sync::atomic::Ordering::Acquire), comparing against
/// `PipeState::DownstreamComplete as u8`.
#[derive(Debug)]
#[repr(u8)]
pub(crate) enum PipeState {
    Active = 0,
    DownstreamComplete = 1,
}

impl PipeState {
    /// Whether `raw` — a value previously read from the shared `AtomicU8` — is
    /// [`DownstreamComplete`](Self::DownstreamComplete). Centralizes the `as u8`
    /// comparison the upstream halves perform on a task-pipe closure.
    pub(crate) fn is_downstream_complete(raw: u8) -> bool {
        raw == PipeState::DownstreamComplete as u8
    }
}

/// Whether a pump that has no end-of-stream handling of its own has nothing to
/// read from the downstream request body.
///
/// This is the pre-tightening meaning of `is_body_done()`: "ended OR declared
/// empty". The H1/H2 upstream pumps deliberately no longer use it -- they gained
/// a bodyless prelude and a futile-read guard so that they can tell an H2
/// request declaring `Content-Length: 0` (empty, but whose stream has NOT ended,
/// design 4.3) apart from a genuinely finished one, and they need the strict
/// transport fact for that.
///
/// The custom-connector pump and the subrequest pipe implement neither. They
/// also derive their UPSTREAM end-of-stream from the declaration
/// (`is_body_empty()`), so with the strict fact their two halves contradict each
/// other: the upstream request is closed at header time while the downstream
/// state machine still believes a body is coming, and -- because these two
/// session types carry no downstream request-body idle timeout of their own
/// (the H1 and H2 sessions do, 60s by default, but a custom session's
/// `set_read_timeout` is `unreachable!()` and the subrequest pipe has no
/// transport to bound) -- the pump parks forever on a read that can never
/// yield, pinning the task, both streams and the upstream session.
/// Restoring the union here makes the two halves agree again, which is exactly
/// how both pumps behaved before the tightening.
pub(crate) fn no_downstream_body_to_read(session: &mut Session) -> bool {
    session.as_mut().is_body_done() || session.as_mut().is_body_empty()
}

/// A downstream custom-message reader, taken out of the session for the
/// duration of a pump and put back on every exit path.
pub(crate) type CustomMessageReader =
    Box<dyn futures::Stream<Item = Result<Bytes>> + Unpin + Send + Sync + 'static>;

/// Put the custom-message reader back into the downstream session.
///
/// Both pumps take it out for the duration of the duplex loop and restore it
/// on normal exit; the terminate early-returns must do the same, or a Custom
/// downstream session is left asymmetric (its writer IS restored by the outer
/// function) and a later reuse would trip the
/// `"...should be empty"` expectation.
pub(crate) fn restore_custom_message_reader(
    session: &mut Session,
    reader: Option<CustomMessageReader>,
) {
    let (Some(custom_session), Some(reader)) = (session.downstream_session.as_custom_mut(), reader)
    else {
        return;
    };
    if let Err(e) = custom_session.restore_custom_message_reader(reader) {
        warn!("Error restoring the downstream custom message reader: {e}");
    }
}

#[cfg(test)]
#[path = "proxy_common_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "proxy_common_terminal_body_dispatch_tests.rs"]
mod terminal_body_dispatch_tests;
