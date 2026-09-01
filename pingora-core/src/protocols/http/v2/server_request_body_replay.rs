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

use crate::protocols::http::body_buffer::{RegisteredRequestBodyBuffer, RequestBodyBuffer};
use crate::{Error, ErrorType, Result};

impl super::HttpSession {
    /// See `v1::server::HttpSession::set_request_body_buffer`. Fails closed if the
    /// body has already started being read (would capture only the remainder) and
    /// for CONNECT requests, whose "body" is a bidirectional tunnel stream.
    pub fn set_request_body_buffer(&mut self, buffer: Box<dyn RequestBodyBuffer>) -> Result<()> {
        if self.request_body_configuration_frozen {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body configuration is frozen for upstream proxying",
            );
        }
        if self.early_body_buffer.is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer is already registered",
            );
        }
        // Double-send defense, see the v1 counterpart: with retry buffering
        // already enabled, the capturing reads would tee drained chunks into
        // the native retry buffer and the proxy would send that buffer AND
        // replay this one.
        if self.retry_buffer.is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer cannot be registered while retry buffering is enabled",
            );
        }
        // Extended CONNECT (RFC 8441) also uses :method = CONNECT, so this single
        // check covers both plain and extended CONNECT tunnels.
        if self.request_header.method == http::Method::CONNECT {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer cannot be registered for a CONNECT request",
            );
        }
        if self.is_body_empty() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer cannot be registered for an empty request body",
            );
        }
        if self.body_read > 0 {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer must be registered before the body is read",
            );
        }
        self.early_body_buffer = Some(RegisteredRequestBodyBuffer::new(buffer));
        Ok(())
    }

    /// Register an already-finalized replay source for a request whose
    /// downstream stream already ended with no body (END_STREAM on HEADERS, or
    /// an already-consumed zero-byte body). This performs no downstream capture
    /// and lets the upstream stream remain open for an application-injected
    /// body.
    pub fn set_bodyless_request_replay_buffer(
        &mut self,
        buffer: Box<dyn RequestBodyBuffer>,
    ) -> Result<()> {
        if self.request_body_configuration_frozen {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body configuration is frozen for upstream proxying",
            );
        }
        if self.early_body_buffer.is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer is already registered",
            );
        }
        // Same double-send defense as `set_request_body_buffer`: keep the two
        // mechanisms mutually exclusive by construction.
        if self.retry_buffer.is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body replay buffer cannot be registered while retry buffering is enabled",
            );
        }
        if self.request_header.method == http::Method::CONNECT {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body replay buffer cannot be registered for a CONNECT request",
            );
        }
        if !self.is_body_empty() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body replay buffer requires an empty downstream request body",
            );
        }
        // `is_body_empty()` alone is not enough: `Content-Length: 0` makes it
        // true even while the downstream stream is still open (HEADERS without
        // END_STREAM). Replay would then permanently shadow the live stream —
        // its empty DATA + END_STREAM, trailers, or content-length-violating
        // DATA would never be observed, and dropping the `RecvStream` at
        // request end could send RST_STREAM(CANCEL) to the client.
        if !self.is_body_done() {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body replay buffer requires the downstream request stream to have ended",
            );
        }
        self.early_body_buffer = Some(RegisteredRequestBodyBuffer::ready(buffer));
        Ok(())
    }

    pub fn request_body_buffer_registered(&self) -> bool {
        self.early_body_buffer.is_some()
    }

    pub(crate) fn freeze_request_body_configuration(&mut self) {
        self.request_body_configuration_frozen = true;
    }

    pub(crate) fn request_body_buffer_replay_available(&self) -> bool {
        self.early_body_buffer.as_ref().is_some_and(|buffer| {
            !self.early_body_capture_poisoned
                && !self.early_body_buffer_discarded
                && !self.early_body_buffer_released
                && (buffer.is_ready_or_replay_done() || buffer.is_replaying())
        })
    }

    /// Whether a registered request body buffer is currently replaying, i.e.
    /// `read_body_or_idle` serves buffered chunks instead of reading the client
    /// stream, so its errors originate in the buffer, not the client.
    pub fn request_body_buffer_replaying(&self) -> bool {
        self.early_body_buffer
            .as_ref()
            .is_some_and(RegisteredRequestBodyBuffer::is_replaying)
    }

    /// Prepare the registered buffer as the active request-body source for one
    /// upstream attempt. Returns `false` when no buffer was registered.
    pub async fn begin_request_body_replay(&mut self) -> Result<bool> {
        if self.early_body_capture_poisoned {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body capture failed or was cancelled mid-chunk; refusing to replay incomplete buffered body",
            );
        }
        if self.early_body_buffer_discarded {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer was discarded by drain_request_body; the request body is gone and cannot be replayed",
            );
        }
        if self.early_body_buffer_released {
            return Error::e_explain(
                ErrorType::InternalError,
                "request body buffer was released after the response was committed downstream; no further replay is possible",
            );
        }
        let Some(registered) = self.early_body_buffer.as_mut() else {
            return Ok(false);
        };
        registered.begin_replay().await?;
        // Replay serves buffered chunks, so any deadline left over from a live
        // read that was cancelled before this attempt no longer describes the
        // client's silence: drop it so the first live read after the replay
        // starts a fresh bound instead of expiring immediately.
        self.read_deadline = None;
        Ok(true)
    }
}
