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
use crate::{RESPONSE_BODY_EMIT_BUDGET, RESPONSE_BODY_EMIT_CHUNK_BUDGET};

fn sink_with(chunks: &[&'static [u8]]) -> ResponseBodySink {
    let mut sink = ResponseBodySink::new();
    for c in chunks {
        sink.push(Bytes::from_static(c)).unwrap();
    }
    sink
}

/// Nothing released: the terminal task is emitted alone, unchanged.
#[test]
fn before_drain_with_empty_sink_emits_only_the_task() {
    let mut sink = ResponseBodySink::new();
    let mut out = Vec::new();
    drain_emitted_chunks_before(HttpTask::Done, &mut sink, false, &mut out);
    assert!(matches!(out.as_slice(), [HttpTask::Done]));
}

/// The defect this helper exists for: released body bytes must reach the
/// wire BEFORE the trailer that terminates the response.
#[test]
fn before_drain_puts_released_bytes_ahead_of_the_trailer() {
    let mut sink = sink_with(&[b"held-a", b"held-b"]);
    let mut out = Vec::new();
    drain_emitted_chunks_before(
        HttpTask::Trailer(Some(Box::default())),
        &mut sink,
        false,
        &mut out,
    );
    assert!(matches!(
        out.as_slice(),
        [
            HttpTask::Body(Some(a), false),
            HttpTask::Body(Some(b), false),
            HttpTask::Trailer(Some(_)),
        ] if a.as_ref() == b"held-a" && b.as_ref() == b"held-b"
    ));
}

/// `Trailer`/`Done` are intrinsically `is_end()`, so the completion stays
/// on the terminal task and no released chunk claims it -- this is what
/// keeps the response finishing exactly once.
#[test]
fn before_drain_never_migrates_end_of_stream_onto_released_chunks() {
    for task in [HttpTask::Done, HttpTask::Trailer(Some(Box::default()))] {
        let mut sink = sink_with(&[b"a", b"b"]);
        let mut out = Vec::new();
        assert!(task.is_end());
        drain_emitted_chunks_before(task, &mut sink, false, &mut out);
        assert_eq!(out.len(), 3);
        assert!(out[..2].iter().all(|t| !t.is_end()));
        assert!(out[2].is_end());
    }
}

/// When `response_trailer_filter` converts the trailer into a body buffer
/// (`proxy_h2.rs`), the released bytes must still come first and the
/// converted buffer keeps its `end = true`.
#[test]
fn before_drain_keeps_released_bytes_ahead_of_a_converted_trailer_buffer() {
    let mut sink = sink_with(&[b"held"]);
    let mut out = Vec::new();
    drain_emitted_chunks_before(
        HttpTask::Body(Some(Bytes::from_static(b"trailer-buffer")), true),
        &mut sink,
        false,
        &mut out,
    );
    assert!(matches!(
        out.as_slice(),
        [
            HttpTask::Body(Some(held), false),
            HttpTask::Body(Some(buf), true),
        ] if held.as_ref() == b"held" && buf.as_ref() == b"trailer-buffer"
    ));
}

/// Released bytes inherit the response's body variant: mistagging them as
/// plain `Body` would misroute them off the post-upgrade duplex write path.
#[test]
fn before_drain_preserves_the_upgraded_body_tag() {
    let mut sink = sink_with(&[b"frame"]);
    let mut out = Vec::new();
    drain_emitted_chunks_before(HttpTask::Done, &mut sink, true, &mut out);
    assert!(matches!(
        out.as_slice(),
        [HttpTask::UpgradedBody(Some(d), false), HttpTask::Done] if d.as_ref() == b"frame"
    ));
}

/// The sink is drained, not peeked: a later batch must not replay them.
#[test]
fn before_drain_empties_the_sink() {
    let mut sink = sink_with(&[b"a"]);
    let mut out = Vec::new();
    drain_emitted_chunks_before(HttpTask::Done, &mut sink, false, &mut out);
    assert!(sink.peek_extra().is_empty());
}

#[test]
fn empty_sink_leaves_task_untouched() {
    let mut sink = ResponseBodySink::new();
    let mut out = Vec::new();
    drain_emitted_chunks(
        HttpTask::Body(Some(Bytes::from_static(b"hello")), true),
        &mut sink,
        &mut out,
    );
    assert!(matches!(out.as_slice(), [HttpTask::Body(Some(d), true)] if d.as_ref() == b"hello"));
}

#[test]
fn non_eos_body_leaves_every_chunk_non_eos() {
    let mut sink = sink_with(&[b"a", b"b"]);
    let mut out = Vec::new();
    drain_emitted_chunks(
        HttpTask::Body(Some(Bytes::from_static(b"x")), false),
        &mut sink,
        &mut out,
    );
    match out.as_slice() {
        [HttpTask::Body(Some(d0), false), HttpTask::Body(Some(d1), false), HttpTask::Body(Some(d2), false)] =>
        {
            assert_eq!(d0.as_ref(), b"x");
            assert_eq!(d1.as_ref(), b"a");
            assert_eq!(d2.as_ref(), b"b");
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
}

#[test]
fn fragmented_full_budget_has_a_bounded_downstream_operation_count() {
    let mut contiguous = ResponseBodySink::new();
    contiguous
        .push(Bytes::from(vec![0; RESPONSE_BODY_EMIT_BUDGET]))
        .unwrap();
    let mut contiguous_tasks = Vec::new();
    drain_emitted_chunks(
        HttpTask::Body(None, true),
        &mut contiguous,
        &mut contiguous_tasks,
    );

    let chunk_len = RESPONSE_BODY_EMIT_BUDGET / RESPONSE_BODY_EMIT_CHUNK_BUDGET;
    let fragment = Bytes::from(vec![0; chunk_len]);
    let mut fragmented = ResponseBodySink::new();
    for _ in 0..RESPONSE_BODY_EMIT_CHUNK_BUDGET {
        fragmented.push(fragment.clone()).unwrap();
    }
    let mut fragmented_tasks = Vec::new();
    drain_emitted_chunks(
        HttpTask::Body(None, true),
        &mut fragmented,
        &mut fragmented_tasks,
    );

    assert_eq!(contiguous_tasks.len(), 1);
    assert_eq!(fragmented_tasks.len(), RESPONSE_BODY_EMIT_CHUNK_BUDGET);
    assert!(fragmented_tasks[..fragmented_tasks.len() - 1]
        .iter()
        .all(|task| !task.is_end()));
    assert!(fragmented_tasks.last().unwrap().is_end());
    assert_eq!(
        fragmented_tasks
            .iter()
            .filter_map(|task| match task {
                HttpTask::Body(Some(bytes), _) => Some(bytes.len()),
                _ => None,
            })
            .sum::<usize>(),
        RESPONSE_BODY_EMIT_BUDGET
    );
}

#[test]
fn eos_body_with_payload_migrates_flag_to_last_chunk() {
    // This is Critical 1's exact shape: `Body(Some(data), true)` is the
    // normal single-read last-chunk shape on both H1 and H2, not an edge
    // case. The original must lose its `true` and gain a `false`, and
    // exactly one chunk -- the LAST one -- must carry `true` instead.
    let mut sink = sink_with(&[b"a", b"b"]);
    let mut out = Vec::new();
    drain_emitted_chunks(
        HttpTask::Body(Some(Bytes::from_static(b"x")), true),
        &mut sink,
        &mut out,
    );
    match out.as_slice() {
        [HttpTask::Body(Some(d0), false), HttpTask::Body(Some(d1), false), HttpTask::Body(Some(d2), true)] =>
        {
            assert_eq!(d0.as_ref(), b"x");
            assert_eq!(d1.as_ref(), b"a");
            assert_eq!(d2.as_ref(), b"b");
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
}

#[test]
fn bare_eos_marker_is_dropped_and_last_chunk_carries_its_meaning() {
    // `Body(None, true)` is a pure end-of-stream signal with no payload
    // of its own: emitting it AND a migrated last chunk would still be
    // two end-of-stream signals for the cache to choke on, so it must
    // not appear in the output at all.
    let mut sink = sink_with(&[b"a", b"b"]);
    let mut out = Vec::new();
    drain_emitted_chunks(HttpTask::Body(None, true), &mut sink, &mut out);
    match out.as_slice() {
        [HttpTask::Body(Some(d0), false), HttpTask::Body(Some(d1), true)] => {
            assert_eq!(d0.as_ref(), b"a");
            assert_eq!(d1.as_ref(), b"b");
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
}

#[test]
fn header_eos_migrates_to_the_last_emitted_body_chunk() {
    let mut sink = sink_with(&[b"current", b"extra"]);
    let mut out = Vec::new();
    let header = Box::new(ResponseHeader::build(503, None).unwrap());
    drain_emitted_chunks(HttpTask::Header(header, true), &mut sink, &mut out);
    match out.as_slice() {
        [HttpTask::Header(header, false), HttpTask::Body(Some(d0), false), HttpTask::Body(Some(d1), true)] =>
        {
            assert_eq!(header.status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(d0.as_ref(), b"current");
            assert_eq!(d1.as_ref(), b"extra");
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
}

#[test]
fn upgraded_body_chunks_stay_tagged_upgraded_body() {
    // `Session::write_response_tasks` picks the raw post-upgrade duplex
    // write path off this tag; mistagging a chunk as plain `Body` would
    // misroute its bytes on an upgraded (e.g. WebSocket) connection.
    let mut sink = sink_with(&[b"a"]);
    let mut out = Vec::new();
    drain_emitted_chunks(
        HttpTask::UpgradedBody(Some(Bytes::from_static(b"x")), true),
        &mut sink,
        &mut out,
    );
    match out.as_slice() {
        [HttpTask::UpgradedBody(Some(d0), false), HttpTask::UpgradedBody(Some(d1), true)] => {
            assert_eq!(d0.as_ref(), b"x");
            assert_eq!(d1.as_ref(), b"a");
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
}

#[test]
fn trailer_with_a_nonempty_sink_still_drains_every_chunk() {
    // Defensive: a trailer does not run the body hook, so it should see an
    // empty sink. If it somehow does not, deliver the chunks without
    // inventing another end-of-stream source.
    let mut sink = sink_with(&[b"a"]);
    let mut out = Vec::new();
    drain_emitted_chunks(HttpTask::Trailer(None), &mut sink, &mut out);
    match out.as_slice() {
        [HttpTask::Trailer(None), HttpTask::Body(Some(d), false)] => {
            assert_eq!(d.as_ref(), b"a");
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
}
