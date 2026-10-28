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

#[test]
fn push_accumulates_and_take_drains() {
    let mut sink = ResponseBodySink::new();
    sink.push(Bytes::from_static(b"a")).unwrap();
    sink.push(Bytes::from_static(b"bc")).unwrap();
    let extra = sink.take_extra();
    assert_eq!(
        extra,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")]
    );
    assert!(sink.take_extra().is_empty(), "take must drain");
}

#[test]
fn budget_is_consumed_and_exhaustion_errors() {
    let mut sink = ResponseBodySink::new();
    assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET);
    sink.push(Bytes::from_static(b"1234")).unwrap();
    assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET - 4);

    let oversized = Bytes::from(vec![0u8; RESPONSE_BODY_EMIT_BUDGET]);
    assert!(sink.push(oversized).is_err(), "must reject, never truncate");
    // The rejected chunk must not be partially recorded.
    assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET - 4);
    assert_eq!(sink.take_extra().len(), 1);
}

#[test]
fn chunk_budget_accepts_limit_and_rejects_next_without_mutation() {
    let mut sink = ResponseBodySink::new();
    for _ in 0..RESPONSE_BODY_EMIT_CHUNK_BUDGET {
        sink.push(Bytes::from_static(b"x")).unwrap();
    }
    assert_eq!(sink.remaining_chunk_budget(), 0);
    assert_eq!(
        sink.remaining_budget(),
        RESPONSE_BODY_EMIT_BUDGET - RESPONSE_BODY_EMIT_CHUNK_BUDGET
    );

    let remaining_bytes = sink.remaining_budget();
    assert!(sink.push(Bytes::from_static(b"y")).is_err());
    assert_eq!(sink.remaining_budget(), remaining_bytes);
    assert_eq!(sink.remaining_chunk_budget(), 0);
    assert_eq!(sink.take_extra().len(), RESPONSE_BODY_EMIT_CHUNK_BUDGET);
}

#[test]
fn reset_batch_restores_budget_but_not_terminate() {
    let mut sink = ResponseBodySink::new();
    sink.push(Bytes::from_static(b"xyz")).unwrap();
    sink.terminate();
    sink.reset_batch();
    assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET);
    assert_eq!(
        sink.remaining_chunk_budget(),
        RESPONSE_BODY_EMIT_CHUNK_BUDGET
    );
    assert!(
        sink.take_extra().is_empty(),
        "reset_batch drops undelivered extras"
    );
    assert!(sink.is_terminated(), "terminate is sticky across batches");
}

#[test]
fn empty_push_is_free() {
    let mut sink = ResponseBodySink::new();
    sink.push(Bytes::new()).unwrap();
    assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET);
    assert_eq!(
        sink.remaining_chunk_budget(),
        RESPONSE_BODY_EMIT_CHUNK_BUDGET
    );
    assert!(
        sink.take_extra().is_empty(),
        "empty chunks are dropped, not queued"
    );
}

#[test]
fn prepend_current_keeps_current_chunk_before_extras_without_spending_budget() {
    let mut sink = ResponseBodySink::new();
    sink.push(Bytes::from_static(b"extra-a")).unwrap();
    sink.push(Bytes::from_static(b"extra-b")).unwrap();
    let remaining = sink.remaining_budget();
    let remaining_chunks = sink.remaining_chunk_budget();
    sink.prepend_current(Bytes::from_static(b"current"));
    assert_eq!(sink.remaining_budget(), remaining);
    assert_eq!(sink.remaining_chunk_budget(), remaining_chunks);
    assert_eq!(
        sink.take_extra(),
        vec![
            Bytes::from_static(b"current"),
            Bytes::from_static(b"extra-a"),
            Bytes::from_static(b"extra-b"),
        ]
    );
}

#[test]
fn synthetic_current_chunk_has_the_same_unbudgeted_semantics_as_body_mutation() {
    let mut sink = ResponseBodySink::new();
    sink.prepend_current(Bytes::from(vec![0; RESPONSE_BODY_EMIT_BUDGET + 1]));
    assert_eq!(sink.remaining_budget(), RESPONSE_BODY_EMIT_BUDGET);
    assert_eq!(sink.take_extra()[0].len(), RESPONSE_BODY_EMIT_BUDGET + 1);

    assert!(sink
        .push(Bytes::from(vec![0; RESPONSE_BODY_EMIT_BUDGET + 1]))
        .is_err());
}

#[test]
fn terminal_boundary_consumes_terminate() {
    let mut sink = ResponseBodySink::new();
    sink.terminate();
    sink.consume_terminate();
    assert!(!sink.is_terminated());
}
