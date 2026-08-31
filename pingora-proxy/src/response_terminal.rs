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

//! Response-terminal dispatch and trailer normalization.

use crate::UpstreamResponseBodyEvent;
use http::HeaderMap;
use pingora_core::protocols::http::HttpTask;

/// Tracks whether the single terminal response-body lifecycle event has
/// already been delivered for this response.
///
/// `HttpProxy::upstream_filter` reaches the body filter only from a
/// `Body`/`UpgradedBody` task, so a response that terminates with a `Trailer`
/// or a bare `Done` would otherwise never deliver end-of-stream at all: on H2
/// the `END_STREAM` flag rides the trailers HEADERS frame, so every DATA frame
/// of a trailered response is emitted with `eos = false`. A body filter that
/// withholds bytes across callbacks until end-of-stream then never releases
/// them.
///
/// `Trailer` and the `Done` that follows it are two observations of ONE
/// termination, and a bare `Done` is a third; exactly one of them may dispatch.
/// Terminations that already carry end-of-stream through the ordinary path
/// (`Header(_, true)` via `terminal_upstream_body_filter`, `Body(_, true)`)
/// claim the latch without dispatching, so the trailing `Done` cannot deliver a
/// second one.
///
/// `Failed` claims WITHOUT dispatching: the response aborted, and synthesizing
/// end-of-stream for it would tell a filter that a truncated body was complete.
/// Claiming (rather than ignoring) is what stops a `Done` following the error
/// from doing exactly that.
///
/// Protocol-neutral on purpose: the H1, H2, and custom pumps share it.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TerminalBodyDispatch {
    claimed: bool,
    upgraded: bool,
}

impl TerminalBodyDispatch {
    /// Whether this response has carried upgraded body tasks.
    ///
    /// Bytes released by the terminal callback must be re-emitted under the
    /// response's own body variant: `Session::write_response_tasks` picks the
    /// raw post-upgrade duplex write path off the `UpgradedBody` tag, so
    /// tagging released bytes as plain `Body` would misroute them on an
    /// upgraded (e.g. WebSocket) connection. The terminal task itself is a
    /// `Trailer`/`Done` and carries no variant, hence the latch remembers it.
    pub fn is_upgraded(&self) -> bool {
        self.upgraded
    }

    /// Record that the final filtered response header completed the downstream
    /// Upgrade handshake. This must be called from the header path after
    /// `response_filter`: the upstream status alone is insufficient because
    /// the downstream request may not be an Upgrade and the filter may rewrite
    /// the response status before it reaches the writer.
    pub fn mark_upgraded(&mut self) {
        self.upgraded = true;
    }

    /// Record `task` as this response's terminal observation if nothing has
    /// claimed that role yet, and return its typed terminal body event.
    ///
    /// Returns an event at most once per response.
    pub fn claim_for(&mut self, task: &HttpTask) -> Option<UpstreamResponseBodyEvent> {
        self.upgraded |= matches!(task, HttpTask::UpgradedBody(..));
        match task {
            // Already delivers end-of-stream through the ordinary path: the
            // `terminal_header` branch runs `terminal_upstream_body_filter`,
            // and a terminal body task carries `eos = true` into
            // `upstream_response_body_filter` itself.
            HttpTask::Header(_, true)
            | HttpTask::Body(_, true)
            | HttpTask::UpgradedBody(_, true)
            // Aborted: must never be given a synthetic end-of-stream.
            | HttpTask::Failed(_) => {
                self.claimed = true;
                None
            }
            HttpTask::Trailer(trailers) => {
                if self.claimed {
                    None
                } else {
                    self.claimed = true;
                    Some(if trailers.is_some() {
                        UpstreamResponseBodyEvent::TerminalBeforeTrailers
                    } else {
                        UpstreamResponseBodyEvent::TerminalWithoutTrailers
                    })
                }
            }
            HttpTask::Done => {
                if self.claimed {
                    None
                } else {
                    self.claimed = true;
                    Some(UpstreamResponseBodyEvent::TerminalWithoutTrailers)
                }
            }
            _ => None,
        }
    }
}

/// Canonicalize an emptied trailer map to the transport's trailer-free
/// terminal event. This lets an async application hook remove every field
/// without causing an empty trailer block to be forwarded.
pub(crate) fn normalize_trailers(trailers: Option<Box<HeaderMap>>) -> Option<Box<HeaderMap>> {
    trailers.filter(|trailers| !trailers.is_empty())
}
