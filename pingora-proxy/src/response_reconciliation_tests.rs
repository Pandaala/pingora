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

use super::{is_downstream_followup, reconcile_terminal_response_tasks};
use bytes::Bytes;
use pingora_core::protocols::http::HttpTask;
use pingora_http::ResponseHeader;

#[test]
fn terminal_followups_are_dropped_instead_of_converted_to_done() {
    assert!(is_downstream_followup(&HttpTask::Body(None, true)));
    assert!(is_downstream_followup(&HttpTask::Trailer(None)));
    assert!(is_downstream_followup(&HttpTask::Done));
    assert!(!is_downstream_followup(&HttpTask::Header(
        Box::new(ResponseHeader::build(204, None).unwrap()),
        true,
    )));
}

#[test]
fn terminal_framing_removes_a_stale_content_length() {
    let mut header = ResponseHeader::build(503, None).unwrap();
    header
        .insert_header(http::header::CONTENT_LENGTH, "0")
        .unwrap();
    let mut tasks = vec![
        HttpTask::Header(Box::new(header), false),
        HttpTask::Body(Some(Bytes::from_static(b"generated")), true),
    ];
    reconcile_terminal_response_tasks(&mut tasks, 0, false).unwrap();
    let HttpTask::Header(header, false) = &tasks[0] else {
        panic!("unexpected header task")
    };
    assert!(header.headers.get(http::header::CONTENT_LENGTH).is_none());
    assert_eq!(
        header.headers.get(http::header::TRANSFER_ENCODING).unwrap(),
        "chunked"
    );
}
