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

//! Protocol-neutral request-body event processing.
//!
//! The duplex pumps retain ownership of source reads, pipe capacity, protocol
//! writes, retries, early-response cleanup, and connection reuse. This module
//! owns only the shared semantic sequence between receiving one downstream
//! body event and handing its filtered form back to the protocol writer.

use crate::{
    custom, HttpProxy, ProxyHttp, RequestBodyAction, RequestBodyEvent, RequestRelayPlan,
    RequestReplayPolicy, Session,
};
use bytes::Bytes;
use pingora_error::{Error, ErrorType::InternalError, Result};

/// Core-derived, immutable request relay contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrozenRequestRelayPlan {
    pub(crate) requested: RequestRelayPlan,
    pub(crate) source: RequestRelaySource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestRelaySource {
    LiveDownstream,
    RegisteredReplay,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum NativeRetryBufferState {
    #[default]
    NotStarted,
    Enabled,
    Unsupported,
}

impl FrozenRequestRelayPlan {
    pub(crate) fn derive(requested: RequestRelayPlan, registered_replay: bool) -> Self {
        Self {
            requested,
            source: if registered_replay {
                RequestRelaySource::RegisteredReplay
            } else {
                RequestRelaySource::LiveDownstream
            },
        }
    }

    pub(crate) fn enables_native_retry_buffer(self) -> bool {
        self.requested.replay == RequestReplayPolicy::Replayable
            && self.source == RequestRelaySource::LiveDownstream
    }
}

/// Protocol capabilities which differ at the request relay seam.
///
/// This is deliberately closed and crate-private. It records existing pump
/// behavior; extracting the relay must not silently add trailer or termination
/// support to custom connector sessions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestRelayProtocol {
    H1,
    H2,
    Custom,
}

impl RequestRelayProtocol {
    fn dispatches_trailer_hook(self) -> bool {
        self != Self::Custom
    }

    fn supports_termination(self) -> bool {
        self != Self::Custom
    }
}

/// One filtered event ready for protocol-specific disposition and writing.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PreparedRequestEvent {
    pub(crate) body: Option<Bytes>,
    pub(crate) event: RequestBodyEvent,
}

impl PreparedRequestEvent {
    pub(crate) fn is_terminal(&self) -> bool {
        self.event.is_terminal()
    }
}

/// The hook which selected request-body termination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestTerminationOrigin {
    TrailerFilter,
    BodyFilter,
}

impl RequestTerminationOrigin {
    pub(crate) fn hook_name(self) -> &'static str {
        match self {
            Self::TrailerFilter => "request_trailer_filter",
            Self::BodyFilter => "request_body_filter_action",
        }
    }
}

/// Semantic result of processing one request-body event.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RequestRelayOutcome {
    Continue(PreparedRequestEvent),
    Terminate(RequestTerminationOrigin),
}

impl<SV, C> HttpProxy<SV, C>
where
    SV: ProxyHttp,
    C: custom::Connector,
{
    /// Process one request-body event without performing protocol I/O.
    ///
    /// The caller must acquire any pipe/capacity permit before entering this
    /// function, preserving the pumps' existing backpressure ordering. Empty
    /// output suppression, `Bodyless` validation, and every actual write stay
    /// in the caller because their consequences remain protocol-specific.
    pub(crate) async fn request_relay_event(
        &self,
        protocol: RequestRelayProtocol,
        session: &mut Session,
        mut body: Option<Bytes>,
        mut event: RequestBodyEvent,
        ctx: &mut SV::CTX,
    ) -> Result<RequestRelayOutcome>
    where
        SV: Send + Sync,
        SV::CTX: Send + Sync,
    {
        // A source EOF is authoritative even when a custom downstream's
        // user-provided `is_body_done()` implementation reports stale state.
        // Without this normalization that source can spin forever on EOF.
        if body.is_none() && event == RequestBodyEvent::Data {
            event = RequestBodyEvent::Complete;
        }

        if protocol.dispatches_trailer_hook()
            && event.is_complete()
            && body.is_none()
            && !session.request_trailer_filter_fired
            && session
                .downstream_session
                .request_trailers_present()
                .unwrap_or(false)
        {
            let action = self.inner.request_trailer_filter(session, ctx).await?;

            // Commit the latch only after a successful hook return. The pump
            // future may be cancelled at the await above, and a retryable hook
            // error must be allowed to run again on the next attempt.
            session.request_trailer_filter_fired = true;
            if action == RequestBodyAction::Terminate {
                return Ok(RequestRelayOutcome::Terminate(
                    RequestTerminationOrigin::TrailerFilter,
                ));
            }
        }

        session
            .downstream_modules_ctx
            .request_body_filter(&mut body, event)
            .await?;

        if self
            .inner
            .request_body_filter_action(session, &mut body, event, ctx)
            .await?
            == RequestBodyAction::Terminate
        {
            if !protocol.supports_termination() {
                return Error::e_explain(
                    InternalError,
                    "request-body terminate is not supported on custom connector sessions",
                );
            }
            return Ok(RequestRelayOutcome::Terminate(
                RequestTerminationOrigin::BodyFilter,
            ));
        }

        Ok(RequestRelayOutcome::Continue(PreparedRequestEvent {
            body,
            event,
        }))
    }
}

#[cfg(test)]
#[path = "request_relay_tests.rs"]
mod tests;
