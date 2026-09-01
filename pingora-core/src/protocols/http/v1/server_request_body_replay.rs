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
use pingora_error::{Error, ErrorType::InternalError, Result};

impl super::HttpSession {
    /// Register an app-supplied buffer to capture the request body for early
    /// inspection / rewrite and upstream replay. Must be called BEFORE any body
    /// byte is read: registering after a partial read would capture only the
    /// remainder and silently replay a truncated body, so it fails closed then.
    /// Also fails closed for upgrade requests: their "body" is a bidirectional
    /// tunnel, so capture-until-EOF semantics do not apply. Also fails closed
    /// when the native retry buffer is already enabled: the capturing reads
    /// would tee every drained chunk into it, and the proxy would then send
    /// that buffer AND replay this one — the same body twice. (The proxy's own
    /// `enable_retry_buffering()` is not affected: it runs after the app has
    /// drained the body, and replayed chunks bypass the retry-buffer tee.)
    pub fn set_request_body_buffer(&mut self, buffer: Box<dyn RequestBodyBuffer>) -> Result<()> {
        if self.request_body_configuration_frozen {
            return Error::e_explain(
                InternalError,
                "request body configuration is frozen for upstream proxying",
            );
        }
        if self.early_body_buffer.is_some() {
            return Error::e_explain(InternalError, "request body buffer is already registered");
        }
        if self.retry_buffer.is_some() {
            return Error::e_explain(
                InternalError,
                "request body buffer cannot be registered while retry buffering is enabled",
            );
        }
        if self.is_upgrade_req() {
            return Error::e_explain(
                InternalError,
                "request body buffer cannot be registered for an upgrade request",
            );
        }
        if self.is_body_empty() {
            return Error::e_explain(
                InternalError,
                "request body buffer cannot be registered for an empty request body",
            );
        }
        if self.body_bytes_read > 0 {
            return Error::e_explain(
                InternalError,
                "request body buffer must be registered before the body is read",
            );
        }
        self.early_body_buffer = Some(RegisteredRequestBodyBuffer::new(buffer));
        Ok(())
    }

    /// Register an already-finalized replay source for a request whose
    /// downstream body is empty. This path performs no downstream capture and
    /// exists so an application can inject a body without weakening
    /// `set_request_body_buffer`'s register-before-read contract.
    pub fn set_bodyless_request_replay_buffer(
        &mut self,
        buffer: Box<dyn RequestBodyBuffer>,
    ) -> Result<()> {
        if self.request_body_configuration_frozen {
            return Error::e_explain(
                InternalError,
                "request body configuration is frozen for upstream proxying",
            );
        }
        if self.early_body_buffer.is_some() {
            return Error::e_explain(InternalError, "request body buffer is already registered");
        }
        // Same double-send defense as `set_request_body_buffer`. A bodyless
        // request has nothing to tee today, but rejecting keeps the two
        // mechanisms mutually exclusive by construction instead of by the
        // current send-path details.
        if self.retry_buffer.is_some() {
            return Error::e_explain(
                InternalError,
                "request body replay buffer cannot be registered while retry buffering is enabled",
            );
        }
        if self.is_upgrade_req() {
            return Error::e_explain(
                InternalError,
                "request body replay buffer cannot be registered for an upgrade request",
            );
        }
        if !self.is_body_empty() {
            return Error::e_explain(
                InternalError,
                "request body replay buffer requires an empty downstream request body",
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
    /// connection, so its errors originate in the buffer, not the client.
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
                InternalError,
                "request body capture failed or was cancelled mid-chunk; refusing to replay incomplete buffered body",
            );
        }
        if self.early_body_buffer_discarded {
            return Error::e_explain(
                InternalError,
                "request body buffer was discarded by drain_request_body; the request body is gone and cannot be replayed",
            );
        }
        if self.early_body_buffer_released {
            return Error::e_explain(
                InternalError,
                "request body buffer was released after the response was committed downstream; no further replay is possible",
            );
        }
        let Some(registered) = self.early_body_buffer.as_mut() else {
            return Ok(false);
        };
        registered.begin_replay().await?;
        // See the h2 counterpart: a deadline left over from a live read that
        // was cancelled before this attempt no longer describes the client's
        // silence once replay is serving buffered chunks.
        self.read_deadline = None;
        Ok(true)
    }
}
