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

//! Shared downstream response and terminal-task reconciliation.

use crate::{ResponseBodySink, Session};
use bytes::Bytes;
use http::version::Version;
use pingora_cache::NoCacheReason;
use pingora_core::protocols::http::v1::common::header_value_content_length;
use pingora_core::protocols::http::HttpTask;
use pingora_error::Result;
use pingora_http::ResponseHeader;

pub(super) fn downstream_response_body_forbidden(
    session: &Session,
    header: &ResponseHeader,
) -> bool {
    session.req_header().method == http::Method::HEAD
        || header.status.is_informational()
        || matches!(header.status.as_u16(), 204 | 304)
}

pub(super) fn is_downstream_followup(task: &HttpTask) -> bool {
    !matches!(task, HttpTask::Header(..))
}

pub(super) fn abort_cache_after_response_source_failure(session: &mut Session, from_cache: bool) {
    if session.cache.enabled() || session.cache.bypassing() {
        session.cache.disable(if from_cache {
            NoCacheReason::StorageError
        } else {
            NoCacheReason::UpstreamError
        });
    }
}

pub(super) fn reconcile_terminal_response_tasks(
    tasks: &mut Vec<HttpTask>,
    start: usize,
    downstream_body_forbidden: bool,
) -> Result<()> {
    if !matches!(tasks.get(start), Some(HttpTask::Header(..))) {
        return Ok(());
    }

    if downstream_body_forbidden {
        tasks.truncate(start + 1);
        let HttpTask::Header(header, eos) = &mut tasks[start] else {
            unreachable!("retained task must be a response header")
        };
        *eos = true;
        header.remove_header(&http::header::TRANSFER_ENCODING);
        if header.status.is_informational() || header.status.as_u16() == 204 {
            header.remove_header(&http::header::CONTENT_LENGTH);
        }
        return Ok(());
    }

    let body_len = tasks
        .iter()
        .skip(start + 1)
        .filter_map(|task| match task {
            HttpTask::Body(Some(data), _) | HttpTask::UpgradedBody(Some(data), _) => {
                Some(data.len())
            }
            _ => None,
        })
        .sum::<usize>();
    let has_followup = tasks.len() > start + 1;
    let HttpTask::Header(header, eos) = &mut tasks[start] else {
        unreachable!("located task must be a response header")
    };
    *eos = !has_followup;

    reconcile_content_length(header, body_len);
    if !has_followup {
        header.remove_header(&http::header::TRANSFER_ENCODING);
        return Ok(());
    }
    if header.headers.get(http::header::CONTENT_LENGTH).is_none()
        && header
            .headers
            .get(http::header::TRANSFER_ENCODING)
            .is_none()
    {
        header.set_version(Version::HTTP_11);
        header.insert_header(http::header::TRANSFER_ENCODING, "chunked")?;
    }
    Ok(())
}

fn reconcile_content_length(header: &mut ResponseHeader, body_len: usize) {
    let content_length_matches =
        header_value_content_length(header.headers.get(http::header::CONTENT_LENGTH))
            .is_some_and(|content_length| content_length == body_len);
    if header.headers.contains_key(http::header::CONTENT_LENGTH) && !content_length_matches {
        header.remove_header(&http::header::CONTENT_LENGTH);
    }
}

pub(super) fn reconcile_terminal_cache_header(
    header: &mut ResponseHeader,
    sink: &ResponseBodySink,
) {
    let body_len = sink.peek_extra().iter().map(Bytes::len).sum();
    reconcile_content_length(header, body_len);
}
