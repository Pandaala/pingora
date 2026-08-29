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
use std::hint::black_box;
use std::time::{Duration, Instant};

fn frame(frame_type: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let id = stream_id.to_be_bytes();
    let mut v = vec![
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        frame_type,
        flags,
        id[0],
        id[1],
        id[2],
        id[3],
    ];
    v.extend_from_slice(payload);
    v
}

/// One non-empty read followed by EOF, for pinning scanner state when the
/// transport closes in the middle of a frame.
struct ChunkThenEof(Option<Vec<u8>>);

impl AsyncRead for ChunkThenEof {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(bytes) = self.0.take() {
            buf.put_slice(&bytes);
        }
        Poll::Ready(Ok(()))
    }
}

async fn read_chunk_then_eof(io: &mut EndStreamWatchStream<ChunkThenEof>) {
    let mut storage = [0u8; 64];
    let mut first = ReadBuf::new(&mut storage);
    std::future::poll_fn(|cx| Pin::new(&mut *io).poll_read(cx, &mut first))
        .await
        .unwrap();
    assert!(
        !first.filled().is_empty(),
        "the fixture must deliver a chunk"
    );

    let mut storage = [0u8; 1];
    let mut eof = ReadBuf::new(&mut storage);
    std::future::poll_fn(|cx| Pin::new(&mut *io).poll_read(cx, &mut eof))
        .await
        .unwrap();
    assert!(eof.filled().is_empty(), "the second read must be EOF");
}

/// A padded DATA frame: `Pad Length` byte, then the payload, then `pad`
/// zero bytes. The frame length covers all three.
fn padded_data_frame(flags: u8, stream_id: u32, payload: &[u8], pad: u8) -> Vec<u8> {
    let mut body = vec![pad];
    body.extend_from_slice(payload);
    body.extend(std::iter::repeat_n(0u8, usize::from(pad)));
    frame(FRAME_TYPE_DATA, flags | FLAG_PADDED, stream_id, &body)
}

#[test]
fn end_stream_on_data_is_recorded() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(
        &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello"),
        &watch,
    );
    assert!(flag.end_stream_observed());
}

/// The record has to vouch for a byte COUNT, not just for the flag: that
/// count is what lets the reader detect DATA `h2` decoded and then dropped.
#[test]
fn data_payload_bytes_are_counted() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"hello");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"xy"));
    scanner.scan(&wire, &watch);
    assert!(record.vouches_for(7));
    assert!(
        !record.vouches_for(5),
        "a short read must not be vouched for"
    );
    assert!(!record.vouches_for(8));
}

/// Without END_STREAM there is nothing to vouch for, however many bytes
/// were seen.
#[test]
fn a_count_without_end_stream_vouches_for_nothing() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, b"hello"), &watch);
    assert!(!record.vouches_for(5));
}

/// `h2` hands the reader `Data::payload()`, which excludes both the Pad
/// Length field and the padding, so the count must exclude them too --
/// otherwise a padded response would never match and the §8.1 shape would
/// silently stop working for peers that pad.
#[test]
fn padding_is_not_counted_as_payload() {
    for split in [usize::MAX, 1, 2, 5, 9, 10, 11] {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = padded_data_frame(0, 1, b"hello", 4);
        wire.extend_from_slice(&padded_data_frame(FLAG_END_STREAM, 1, b"xy", 7));
        for chunk in wire.chunks(split.min(wire.len())) {
            scanner.scan(chunk, &watch);
        }
        assert!(record.end_stream_observed(), "split={split}");
        assert!(record.vouches_for(7), "split={split}");
    }
}

/// A padded DATA frame with no payload at all is malformed (`h2` answers it
/// with a protocol error). It must not leave the scanner waiting for a Pad
/// Length byte that never comes, nor claim any bytes.
#[test]
fn empty_padded_data_frame_counts_nothing() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, FLAG_PADDED, 1, b"");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"ab"));
    scanner.scan(&wire, &watch);
    assert!(record.vouches_for(2));
}

/// A PADDED DATA frame with a zero-length payload is malformed: the Pad
/// Length octet is missing, and h2 answers it with a connection error. Its
/// END_STREAM flag must therefore never be recorded as a real end of body,
/// or a peer could claim a complete body it never sent.
#[test]
fn empty_padded_data_frame_does_not_record_end_stream() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    let wire = frame(FRAME_TYPE_DATA, FLAG_PADDED | FLAG_END_STREAM, 1, b"");
    scanner.scan(&wire, &watch);
    assert!(
        !record.vouches_for(0),
        "a malformed padded frame must not vouch for an end of body"
    );

    // A well-formed empty final DATA frame still does.
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    let wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"");
    scanner.scan(&wire, &watch);
    assert!(record.vouches_for(0));
}

/// A trailer END_STREAM is not evidence until h2 validates its HPACK block.
/// The wire scanner cannot distinguish valid trailers from a pseudo-header
/// illegally sent in trailers, so it must leave the record unpublished.
#[test]
fn end_stream_on_headers_is_not_published_before_validation() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"hello");
    wire.extend_from_slice(&frame(
        FRAME_TYPE_HEADERS,
        FLAG_END_STREAM | 0x4,
        1,
        b"\x88",
    ));
    scanner.scan(&wire, &watch);
    assert!(!record.vouches_for(5));
    assert!(record.terminal_headers_observed());
}

/// Terminal HEADERS are relevant even when no response DATA preceded them:
/// malformed zero-DATA trailers can otherwise be hidden by a later reset.
#[test]
fn terminal_headers_are_recorded_without_data() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(
        &frame(FRAME_TYPE_HEADERS, FLAG_END_STREAM | 0x4, 1, b"\x88"),
        &watch,
    );
    assert!(record.terminal_headers_observed());
}

/// Once a stream's outcome is decided its record is frozen: a protocol
/// violation afterwards may neither add bytes nor set the flag.
#[test]
fn a_decided_record_is_frozen() {
    let watch = EndStreamWatch::default();
    let ended = watch.register(1);
    let torn_down = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello");
    wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, 3, &[0, 0, 0, 0]));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"more"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"late"));
    scanner.scan(&wire, &watch);
    assert!(ended.vouches_for(5));
    assert!(!ended.vouches_for(9));
    assert!(!torn_down.end_stream_observed());
    assert!(!torn_down.vouches_for(4));
}

#[test]
fn data_without_end_stream_is_not_recorded() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, b"hello"), &watch);
    assert!(!flag.end_stream_observed());
}

/// A payload that looks exactly like a frame header carrying END_STREAM
/// must not be mistaken for one.
#[test]
fn payload_bytes_are_skipped_not_parsed() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    let decoy = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"");
    scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, &decoy), &watch);
    assert!(!flag.end_stream_observed());
}

/// Frames are recorded correctly no matter how the reads are chopped up.
#[test]
fn split_reads_are_reassembled() {
    for split in 1..20 {
        let watch = EndStreamWatch::default();
        let flag = watch.register(3);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_HEADERS, 0x4, 3, b"\x88");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"abc"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"de"));
        for chunk in wire.chunks(split) {
            scanner.scan(chunk, &watch);
        }
        assert!(flag.end_stream_observed(), "split={split}");
    }
}

#[test]
fn reset_before_end_stream_wins() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_RST_STREAM, 0, 1, &[0, 0, 0, 0]);
    // A protocol-violating END_STREAM after the reset must not resurrect
    // the record: `h2` would never deliver that frame's payload.
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"x"));
    scanner.scan(&wire, &watch);
    assert!(!flag.end_stream_observed());
}

#[test]
fn reset_after_end_stream_does_not_retract() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello");
    wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, 1, &[0, 0, 0, 0]));
    scanner.scan(&wire, &watch);
    assert!(flag.end_stream_observed());
}

/// A GOAWAY payload naming `last_stream_id` and `error_code`.
fn goaway_payload(last_stream_id: u32) -> Vec<u8> {
    let mut v = last_stream_id.to_be_bytes().to_vec();
    v.extend_from_slice(&[0; 4]); // error code NO_ERROR
    v
}

fn short_goaway_payload(len: usize, last_stream_id: u32) -> Vec<u8> {
    let mut payload = goaway_payload(last_stream_id);
    payload.truncate(len);
    payload
}

#[test]
fn goaway_clears_pending_streams_only() {
    let watch = EndStreamWatch::default();
    let ended = watch.register(1);
    let pending = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hi");
    wire.extend_from_slice(&frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(0)));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hi"));
    scanner.scan(&wire, &watch);
    assert!(ended.end_stream_observed());
    assert!(!pending.end_stream_observed());
}

/// The graceful-shutdown pattern every mainstream server uses: an initial
/// `GOAWAY(NO_ERROR, 2^31-1)`, then the in-flight streams finish normally.
/// `h2` still delivers them, so their registrations must survive.
#[test]
fn goaway_keeps_streams_at_or_below_last_stream_id() {
    let watch = EndStreamWatch::default();
    let in_flight = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(0x7fff_ffff));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hi"));
    scanner.scan(&wire, &watch);
    assert!(in_flight.end_stream_observed());
}

/// Above the threshold `h2` errors the stream out at once and ignores every
/// later frame for it, so a late END_STREAM there is a false positive.
#[test]
fn goaway_clears_streams_above_last_stream_id() {
    let watch = EndStreamWatch::default();
    let kept = watch.register(3);
    let dropped = watch.register(5);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(3));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hi"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"hi"));
    scanner.scan(&wire, &watch);
    assert!(kept.end_stream_observed());
    assert!(!dropped.end_stream_observed());
    assert_eq!(watch.pending_len(), 0);
}

/// The identifier is read correctly however the payload is chopped up, and
/// its reserved high bit is ignored on receipt.
#[test]
fn goaway_last_stream_id_is_reassembled_and_masked() {
    for split in 1..24 {
        let watch = EndStreamWatch::default();
        let kept = watch.register(3);
        let dropped = watch.register(5);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(0x8000_0003));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hi"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"hi"));
        for chunk in wire.chunks(split) {
            scanner.scan(chunk, &watch);
        }
        assert!(kept.end_stream_observed(), "split={split}");
        assert!(!dropped.end_stream_observed(), "split={split}");
    }
}

/// A GOAWAY too short to carry `last_stream_id` names no stream, so the
/// connection is poisoned and nothing may be trusted afterwards.
#[test]
fn malformed_short_goaway_poisons_the_scanner() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &[0, 0]);
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hi"));
    scanner.scan(&wire, &watch);
    assert!(!flag.end_stream_observed());
    assert_eq!(watch.pending_len(), 0);
}

/// Current behavior for payloads too short to contain even the complete
/// `last_stream_id`: the scanner conservatively removes every stream.
#[test]
#[ignore = "characterizes unsafe pre-poison behavior; not a passing contract"]
fn goaway_declared_lengths_one_through_three_currently_clear_everything() {
    for len in 1..=3 {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &short_goaway_payload(len, 1));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"late"));
        scanner.scan(&wire, &watch);

        assert!(!record.end_stream_observed(), "declared length={len}");
        let later = watch.register(3);
        scanner.scan(
            &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"later"),
            &watch,
        );
        assert!(
            !later.end_stream_observed(),
            "declared length={len} must poison later registrations"
        );
        assert_eq!(watch.pending_len(), 0, "declared length={len}");
    }
}

/// Characterization of the unsafe pre-poison behavior. A GOAWAY must have
/// an eight-octet fixed payload, but the current scanner trusts a complete
/// four-octet `last_stream_id` even when the error code is truncated.
#[test]
#[ignore = "characterizes unsafe pre-poison behavior; not a passing contract"]
fn goaway_declared_lengths_four_through_seven_currently_trust_last_stream_id() {
    for len in 4..=7 {
        let watch = EndStreamWatch::default();
        let kept = watch.register(1);
        let excluded = watch.register(3);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &short_goaway_payload(len, 1));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"kept"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"excluded"));
        scanner.scan(&wire, &watch);

        assert!(kept.vouches_for(4), "declared length={len}");
        assert!(!excluded.end_stream_observed(), "declared length={len}");
    }
}

/// Final contract: every GOAWAY whose declared payload is shorter than the
/// fixed eight octets is a connection-level parse error, at every point the
/// reads happen to be split. No later frame may publish a stream record,
/// and no later registration may be entered into the map.
#[test]
fn final_contract_goaway_declared_lengths_zero_through_seven_poison_the_scanner() {
    for len in 0..=7 {
        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &short_goaway_payload(len, 1));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"late"));
        for split in 1..=wire.len() {
            let watch = EndStreamWatch::default();
            let record = watch.register(1);
            let mut scanner = FrameScanner::default();
            for chunk in wire.chunks(split) {
                scanner.scan(chunk, &watch);
            }

            let case = format!("declared length={len}, split={split}");
            assert!(!record.end_stream_observed(), "{case}");
            let later = watch.register(3);
            scanner.scan(
                &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"later"),
                &watch,
            );
            assert!(
                !later.end_stream_observed(),
                "{case} must poison later registrations"
            );
            assert_eq!(watch.pending_len(), 0, "{case}");
        }
    }
}

/// Characterization: the current scanner ignores the GOAWAY frame's own
/// stream id and applies its otherwise valid payload.
#[test]
#[ignore = "characterizes unsafe pre-poison behavior; not a passing contract"]
fn goaway_on_a_nonzero_stream_currently_applies_its_last_stream_id() {
    let watch = EndStreamWatch::default();
    let record = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 9, &goaway_payload(3));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"body"));
    scanner.scan(&wire, &watch);

    assert!(record.vouches_for(4));
}

/// Final contract: GOAWAY is a connection frame and a nonzero stream id
/// poisons the scanner permanently.
#[test]
fn final_contract_goaway_on_a_nonzero_stream_poisons_the_scanner() {
    let watch = EndStreamWatch::default();
    let record = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 9, &goaway_payload(3));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"late"));
    scanner.scan(&wire, &watch);

    assert!(!record.end_stream_observed());
    let later = watch.register(5);
    scanner.scan(
        &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"later"),
        &watch,
    );
    assert!(!later.end_stream_observed());
    assert_eq!(watch.pending_len(), 0);
}

/// A second, lower last-stream id is the normal graceful-drain sequence;
/// it narrows the surviving set and never retracts an already published
/// stream.
#[test]
fn decreasing_goaway_last_stream_id_narrows_the_surviving_set() {
    let watch = EndStreamWatch::default();
    let kept = watch.register(1);
    let excluded = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(3));
    wire.extend_from_slice(&frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1)));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"a"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"b"));
    scanner.scan(&wire, &watch);

    assert!(kept.vouches_for(1));
    assert!(!excluded.end_stream_observed());
}

/// Characterization: map removal makes an increasing second GOAWAY unable
/// to resurrect excluded streams, but the current scanner does not mark
/// the illegal sequence as a connection-level failure.
#[test]
#[ignore = "characterizes unsafe pre-poison behavior; not a passing contract"]
fn increasing_goaway_last_stream_id_currently_keeps_the_lower_survivor() {
    let watch = EndStreamWatch::default();
    let survivor = watch.register(1);
    let already_excluded = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1));
    wire.extend_from_slice(&frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(3)));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"a"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"b"));
    scanner.scan(&wire, &watch);

    assert!(survivor.vouches_for(1));
    assert!(!already_excluded.end_stream_observed());
}

/// Final contract: RFC 9113 forbids a later GOAWAY from increasing the
/// last-stream id, so the sequence poisons even streams below the old
/// threshold.
#[test]
fn final_contract_increasing_goaway_last_stream_id_poisons_the_scanner() {
    let watch = EndStreamWatch::default();
    let survivor = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1));
    wire.extend_from_slice(&frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(3)));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"late"));
    scanner.scan(&wire, &watch);

    assert!(!survivor.end_stream_observed());
    let later = watch.register(3);
    scanner.scan(
        &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"later"),
        &watch,
    );
    assert!(!later.end_stream_observed());
    assert_eq!(watch.pending_len(), 0);
}

/// Final contract: the GOAWAY ceiling outlives the frame that carried it.
/// A stream registered AFTER the GOAWAY is one `h2` errored out the moment
/// it processed that frame, so its END_STREAM must never be published --
/// while a stream at or below the ceiling keeps publishing normally.
#[test]
fn final_contract_goaway_ceiling_persists_for_later_registrations() {
    let watch = EndStreamWatch::default();
    let kept = watch.register(3);
    let mut scanner = FrameScanner::default();
    scanner.scan(&frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(3)), &watch);

    let later = watch.register(5);
    let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"later");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"kept"));
    scanner.scan(&wire, &watch);

    assert!(
        !later.end_stream_observed(),
        "a stream above the ceiling must not publish, however late it registers"
    );
    assert!(
        kept.vouches_for(4),
        "the ceiling must not retract survivors"
    );
}

/// The reserved bit of the GOAWAY frame's own stream identifier is ignored
/// on receipt (RFC 9113 §4.1), so it still names stream 0 and the frame
/// stays legal.
#[test]
fn goaway_frame_reserved_stream_id_bit_is_masked_and_stays_legal() {
    let watch = EndStreamWatch::default();
    let kept = watch.register(1);
    let excluded = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0x8000_0000, &goaway_payload(1));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"a"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"b"));
    scanner.scan(&wire, &watch);

    assert!(kept.vouches_for(1));
    assert!(!excluded.end_stream_observed());
}

/// The error code and any trailing debug data do not change which streams
/// a GOAWAY excludes: only `last_stream_id` does. A non-`NO_ERROR` GOAWAY
/// is still a well-formed frame at this layer.
#[test]
fn goaway_error_code_and_debug_data_variants_apply_the_same_ceiling() {
    // NO_ERROR, PROTOCOL_ERROR, ENHANCE_YOUR_CALM.
    for error_code in [0u32, 1, 11] {
        for debug_data in [&b""[..], &b"shutting down"[..]] {
            let watch = EndStreamWatch::default();
            let kept = watch.register(1);
            let excluded = watch.register(3);
            let mut scanner = FrameScanner::default();

            let mut payload = 1u32.to_be_bytes().to_vec();
            payload.extend_from_slice(&error_code.to_be_bytes());
            payload.extend_from_slice(debug_data);
            let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &payload);
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"a"));
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"b"));
            scanner.scan(&wire, &watch);

            let case = format!("error_code={error_code}, debug_data={}", debug_data.len());
            assert!(kept.vouches_for(1), "{case}");
            assert!(!excluded.end_stream_observed(), "{case}");
        }
    }
}

/// An empty GOAWAY payload must be decided at the header, not left waiting
/// for payload bytes that never come. It is shorter than the fixed eight
/// octets, so the decision is rejection.
#[test]
fn empty_goaway_payload_is_rejected_at_the_header() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, b"");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hi"));
    scanner.scan(&wire, &watch);
    assert!(!flag.end_stream_observed());
}

/// Characterization: EOF is not currently reported to `FrameScanner`, so
/// a staged partial frame header leaves the registration live.
#[tokio::test]
#[ignore = "characterizes unsafe pre-poison behavior; not a passing contract"]
async fn eof_in_a_partial_frame_header_currently_leaves_the_stream_pending() {
    let watch = EndStreamWatch::new();
    let record = watch.registration().register(1);
    let wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"body");
    let mut io = EndStreamWatchStream::new(ChunkThenEof(Some(wire[..5].to_vec())), watch.clone());

    read_chunk_then_eof(&mut io).await;

    assert!(!record.end_stream_observed());
    assert!(watch.has_pending(1));
}

/// Final contract: EOF with an incomplete frame header is a connection
/// parse failure and must retire every pending record.
#[tokio::test]
async fn final_contract_eof_in_a_partial_frame_header_poisons_the_scanner() {
    let wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"body");
    for split in 1..FRAME_HEADER_LEN {
        let watch = EndStreamWatch::new();
        let record = watch.registration().register(1);
        let mut io =
            EndStreamWatchStream::new(ChunkThenEof(Some(wire[..split].to_vec())), watch.clone());

        read_chunk_then_eof(&mut io).await;

        assert!(!record.end_stream_observed(), "split={split}");
        let later = watch.registration().register(3);
        let mut scanner = FrameScanner::default();
        scanner.scan(
            &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"later"),
            &watch,
        );
        assert!(!later.end_stream_observed(), "split={split}");
        assert_eq!(watch.pending_len(), 0, "split={split}");
    }
}

/// END_STREAM is not evidence until the complete DATA frame has reached
/// h2. This matters especially for padding-only terminal frames, whose
/// application byte count can still match the body delivered so far.
#[tokio::test]
async fn final_contract_eof_in_a_terminal_data_payload_never_publishes() {
    let cases = [
        ("plain", frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"body")),
        ("padding-only-after-body", {
            let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"body");
            wire.extend_from_slice(&padded_data_frame(FLAG_END_STREAM, 1, b"", 3));
            wire
        }),
    ];

    for (name, wire) in cases {
        let terminal_header = if name == "plain" {
            0
        } else {
            frame(FRAME_TYPE_DATA, 0, 1, b"body").len()
        };
        let payload_len = wire.len() - terminal_header - FRAME_HEADER_LEN;
        for payload_bytes in 0..payload_len {
            let watch = EndStreamWatch::new();
            let record = watch.registration().register(1);
            let split = terminal_header + FRAME_HEADER_LEN + payload_bytes;
            let mut io = EndStreamWatchStream::new(
                ChunkThenEof(Some(wire[..split].to_vec())),
                watch.clone(),
            );

            read_chunk_then_eof(&mut io).await;

            assert!(
                !record.end_stream_observed(),
                "case={name}, payload_bytes={payload_bytes}"
            );
            let later = watch.registration().register(3);
            let mut scanner = FrameScanner::default();
            scanner.scan(
                &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"later"),
                &watch,
            );
            assert!(
                !later.end_stream_observed(),
                "case={name}, payload_bytes={payload_bytes}"
            );
        }
    }
}

/// Characterization: after a complete GOAWAY header but only part of its
/// declared payload, EOF leaves the GOAWAY undispatched and registrations
/// untouched.
#[tokio::test]
#[ignore = "characterizes unsafe pre-poison behavior; not a passing contract"]
async fn eof_in_a_goaway_payload_currently_leaves_streams_pending() {
    let watch = EndStreamWatch::new();
    let first = watch.registration().register(1);
    let second = watch.registration().register(3);
    let wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1));
    let mut io = EndStreamWatchStream::new(
        ChunkThenEof(Some(wire[..FRAME_HEADER_LEN + 6].to_vec())),
        watch.clone(),
    );

    read_chunk_then_eof(&mut io).await;

    assert!(!first.end_stream_observed());
    assert!(!second.end_stream_observed());
    assert_eq!(watch.pending_len(), 2);
}

/// Final contract: EOF before all bytes in a declared GOAWAY payload have
/// arrived poisons the whole connection, irrespective of the partial
/// `last_stream_id` already staged.
#[tokio::test]
async fn final_contract_eof_in_a_goaway_payload_poisons_the_scanner() {
    let wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1));
    for payload_bytes in 0..goaway_payload(1).len() {
        let watch = EndStreamWatch::new();
        let first = watch.registration().register(1);
        let second = watch.registration().register(3);
        let mut io = EndStreamWatchStream::new(
            ChunkThenEof(Some(wire[..FRAME_HEADER_LEN + payload_bytes].to_vec())),
            watch.clone(),
        );

        read_chunk_then_eof(&mut io).await;

        assert!(
            !first.end_stream_observed(),
            "payload_bytes={payload_bytes}"
        );
        assert!(
            !second.end_stream_observed(),
            "payload_bytes={payload_bytes}"
        );
        let later = watch.registration().register(5);
        let mut scanner = FrameScanner::default();
        scanner.scan(
            &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"later"),
            &watch,
        );
        assert!(
            !later.end_stream_observed(),
            "payload_bytes={payload_bytes}"
        );
        assert!(watch.pending_len() == 0, "payload_bytes={payload_bytes}");
    }
}

/// The registration lock is what closes the window in which `h2` has
/// allocated a stream id and flushed its HEADERS -- so the peer's whole
/// response can already be scanned -- but the caller has not registered
/// yet. A scan that runs while a registration is being taken must BLOCK,
/// not silently drop the record.
#[test]
fn a_scan_waits_for_an_in_progress_registration() {
    use std::sync::atomic::AtomicUsize;

    let watch = Arc::new(EndStreamWatch::default());
    let registration = watch.registration();

    let scanned = Arc::new(AtomicUsize::new(0));
    let scanner_watch = watch.clone();
    let scanner_done = scanned.clone();
    let scanner = std::thread::spawn(move || {
        let mut scanner = FrameScanner::default();
        scanner.scan(
            &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello"),
            &scanner_watch,
        );
        scanner_done.store(1, Ordering::Release);
    });

    // The scan cannot have completed while the guard is held. This is a
    // one-way check: it can only fail if the lock is not being taken.
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        scanned.load(Ordering::Acquire),
        0,
        "the scan must block on the registration lock"
    );

    let flag = registration.register(1);
    scanner.join().unwrap();
    assert!(
        flag.end_stream_observed(),
        "the END_STREAM seen during registration must not be lost"
    );
}

/// Characterization of unsafe current behavior: bytes paired with a read
/// error do not reach h2, so treating their END_STREAM as evidence breaks
/// observer/decoder equivalence.
#[tokio::test]
#[ignore = "characterizes unsafe pre-poison behavior; not a passing contract"]
async fn bytes_delivered_with_an_error_are_scanned() {
    struct DataThenError(Option<Vec<u8>>);
    impl AsyncRead for DataThenError {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let bytes = self.0.take().expect("polled after error");
            buf.put_slice(&bytes);
            Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "reset")))
        }
    }

    let wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello");
    let watch = EndStreamWatch::new();
    let flag = watch.registration().register(1);
    let mut io = EndStreamWatchStream::new(DataThenError(Some(wire.clone())), watch);
    let mut buf = [0u8; 64];
    let mut read_buf = ReadBuf::new(&mut buf);
    let res = std::future::poll_fn(|cx| Pin::new(&mut io).poll_read(cx, &mut read_buf)).await;
    assert!(res.is_err());
    assert_eq!(read_buf.filled(), wire);
    assert!(flag.end_stream_observed());
}

/// Final contract: h2 discards bytes paired with `Ready(Err)`, so the
/// observer must poison without publishing them or admitting later facts.
#[tokio::test]
async fn final_contract_bytes_delivered_with_an_error_are_not_scanned() {
    struct DataThenError(Option<Vec<u8>>);
    impl AsyncRead for DataThenError {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            buf.put_slice(&self.0.take().expect("polled after error"));
            Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "reset")))
        }
    }

    let watch = EndStreamWatch::new();
    let first = watch.registration().register(1);
    let wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello");
    let mut io = EndStreamWatchStream::new(DataThenError(Some(wire)), watch.clone());
    let mut storage = [0u8; 64];
    let mut read_buf = ReadBuf::new(&mut storage);
    assert!(
        std::future::poll_fn(|cx| Pin::new(&mut io).poll_read(cx, &mut read_buf))
            .await
            .is_err()
    );
    assert!(!first.end_stream_observed());

    let later = watch.registration().register(3);
    let mut scanner = FrameScanner::default();
    scanner.scan(
        &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"later"),
        &watch,
    );
    assert!(!later.end_stream_observed());
    assert_eq!(watch.pending_len(), 0);
}

/// Only the registered stream's own END_STREAM counts.
#[test]
fn other_streams_do_not_set_the_flag() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(
        &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hello"),
        &watch,
    );
    assert!(!flag.end_stream_observed());
}

/// The reserved high bit of the stream identifier is ignored on receipt.
#[test]
fn reserved_bit_in_stream_id_is_masked() {
    let watch = EndStreamWatch::default();
    let flag = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(
        &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 0x8000_0001, b"hello"),
        &watch,
    );
    assert!(flag.end_stream_observed());
}

/// Deciding a stream's outcome must not leave anything behind: a pooled
/// connection sees an unbounded number of streams over its lifetime.
#[test]
fn decided_streams_are_evicted() {
    let watch = EndStreamWatch::default();
    let mut scanner = FrameScanner::default();
    for id in (1..100).step_by(2) {
        watch.register(id);
        scanner.scan(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, id, b"x"), &watch);
    }
    assert_eq!(watch.pending_len(), 0);
}

#[test]
fn forget_prevents_publication_with_a_warm_cache() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();

    let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"be");
    warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"fo"));
    warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"re"));
    scanner.scan(&warmup, &watch);
    watch.forget(1);
    scanner.scan(
        &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"after"),
        &watch,
    );

    assert!(!record.end_stream_observed());
    assert!(!record.vouches_for(11));
}

/// Forgetting an abandoned stream must not poison a distinct later stream
/// on the same H2 connection. Stream ids are monotonically increasing and
/// are never reused.
#[test]
fn local_forget_retires_the_old_record_without_poisoning_a_new_registration() {
    let watch = EndStreamWatch::default();
    let old = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, b"old"), &watch);

    watch.forget(1);
    let replacement = watch.register(3);
    scanner.scan(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"new"), &watch);

    assert!(!old.end_stream_observed());
    assert!(!old.vouches_for(6));
    assert!(replacement.vouches_for(3));
    assert_eq!(watch.pending_len(), 0);
}

/// H2-006's deterministic barrier: this side gives up on the stream FIRST,
/// and only then does the peer's terminal evidence -- a complete body
/// followed by RST_STREAM(NO_ERROR), the RFC 9113 section 8.1 shape --
/// reach the scanner. Every handle on the record, the session's and the h2
/// pump's alike, must still read as "no wire proof".
#[test]
fn a_local_invalidation_blocks_the_peers_later_terminal_evidence() {
    let watch = EndStreamWatch::default();
    let session_handle = watch.register(1);
    // The proxy's request-body pump samples its own clone of the same
    // record before the exchange starts.
    let pump_handle = session_handle.clone();
    let mut scanner = FrameScanner::default();
    scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, b"body"), &watch);

    watch.invalidate(1, Some(&session_handle));

    let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"!");
    wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, 1, &[0, 0, 0, 0]));
    scanner.scan(&wire, &watch);

    assert!(!session_handle.end_stream_observed());
    assert!(!pump_handle.end_stream_observed());
    assert!(!session_handle.vouches_for(5));
    assert!(!pump_handle.vouches_for(5));
    assert_eq!(watch.pending_len(), 0);
}

/// The invalidation flag carries the guarantee on its own, independently of
/// the map removal that accompanies it: a record still in the map does not
/// gain END_STREAM once it is marked. That is what makes the invariant hold
/// for the `Arc` clones a removal cannot reach.
#[test]
fn publication_refuses_an_invalidated_record_still_in_the_map() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    record.invalidated.store(true, Ordering::Release);
    let mut scanner = FrameScanner::default();

    scanner.scan(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"body"), &watch);

    assert!(!record.end_stream_observed());
    assert!(!record.vouches_for(4));
    assert_eq!(watch.pending_len(), 0);
}

#[test]
fn terminal_headers_are_refused_for_an_invalidated_record() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    record.invalidated.store(true, Ordering::Release);
    let mut scanner = FrameScanner::default();

    scanner.scan(
        &frame(FRAME_TYPE_HEADERS, FLAG_END_STREAM | 0x4, 1, b"\x88"),
        &watch,
    );

    assert!(!record.terminal_headers_observed());
    assert!(!record.end_stream_observed());
}

/// Evidence that was already published when the application gave up is NOT
/// retracted: the peer flagged the end of its body strictly before this
/// side decided to reset, so the body was whole before we walked away from
/// it. `note_local_reset` documents this as deliberate, and
/// `upstream_write_error_outcome` in `pingora-proxy` depends on it.
#[test]
fn invalidation_does_not_retract_evidence_published_before_it() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"body"), &watch);

    watch.invalidate(1, Some(&record));

    assert!(record.end_stream_observed());
    assert!(record.vouches_for(4));
}

/// Invalidating an abandoned stream must not cost a distinct later stream
/// its evidence, and the later stream must not inherit the abandoned one's.
#[test]
fn an_invalidated_stream_neither_poisons_nor_feeds_a_later_one() {
    let watch = EndStreamWatch::default();
    let abandoned = watch.register(1);
    let mut scanner = FrameScanner::default();
    scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, b"old"), &watch);

    watch.invalidate(1, Some(&abandoned));
    let replacement = watch.register(3);
    scanner.scan(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"new"), &watch);

    assert!(!abandoned.end_stream_observed());
    assert!(!abandoned.vouches_for(3));
    assert!(replacement.vouches_for(3));
    assert_eq!(watch.pending_len(), 0);
}

#[test]
fn a_cached_record_is_frozen_after_end_stream() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"b"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"x"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"c"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"invalid"));
    scanner.scan(&wire, &watch);

    assert!(record.vouches_for(4));
    assert!(!record.vouches_for(11));
}

#[test]
fn reset_prevents_publication_with_a_warm_cache() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"be");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"fo"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"re"));
    wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, 1, &[0, 0, 0, 0]));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"invalid"));
    scanner.scan(&wire, &watch);

    assert!(!record.end_stream_observed());
}

#[test]
fn end_stream_before_forget_remains_observed_with_a_warm_cache() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"b"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"c"));
    scanner.scan(&wire, &watch);
    watch.forget(1);

    assert!(record.vouches_for(3));
}

#[test]
fn goaway_prunes_only_excluded_cached_records() {
    let watch = EndStreamWatch::default();
    let kept = watch.register(1);
    let excluded = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"b"));
    wire.extend_from_slice(&frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1)));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"c"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"d"));
    scanner.scan(&wire, &watch);

    assert!(kept.vouches_for(2));
    assert!(!excluded.end_stream_observed());
}

#[test]
fn interleaved_streams_keep_independent_cached_counts() {
    let watch = EndStreamWatch::default();
    let first = watch.register(1);
    let second = watch.register(3);
    let mut scanner = FrameScanner::default();
    let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"bb"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"ccc"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b""));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b""));
    scanner.scan(&wire, &watch);

    assert!(first.vouches_for(4));
    assert!(second.vouches_for(2));
}

#[test]
fn scanner_cache_retains_at_most_two_forgotten_records() {
    let watch = EndStreamWatch::default();
    let mut scanner = FrameScanner::default();
    let mut records = Vec::new();

    for stream_id in (1..200).step_by(2) {
        let record = watch.register(stream_id);
        records.push(Arc::downgrade(&record));
        let mut wire = frame(FRAME_TYPE_DATA, 0, stream_id, b"a");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, stream_id, b"b"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, stream_id, b"cached"));
        scanner.scan(&wire, &watch);
        watch.forget(stream_id);
    }

    assert_eq!(watch.pending_len(), 0);
    assert!(
        records
            .iter()
            .filter(|record| record.upgrade().is_some())
            .count()
            <= 2
    );
}

#[test]
fn full_cache_reclaims_a_forgotten_record_for_a_new_stream() {
    let watch = EndStreamWatch::default();
    let first = watch.register(1);
    let second = watch.register(3);
    let first_weak = Arc::downgrade(&first);
    let second_weak = Arc::downgrade(&second);
    let mut scanner = FrameScanner::default();
    let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"a");
    warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"b"));
    scanner.scan(&warmup, &watch);
    watch.forget(1);
    watch.forget(3);
    drop(first);
    drop(second);

    let replacement = watch.register(5);
    scanner.scan(&frame(FRAME_TYPE_DATA, 0, 5, b"c"), &watch);
    assert!(scanner
        .data_records
        .iter()
        .flatten()
        .any(|cached| cached.stream_id == 5));
    assert_eq!(
        [first_weak, second_weak]
            .iter()
            .filter(|record| record.upgrade().is_some())
            .count(),
        0,
        "both forgotten slots should have been reclaimed"
    );

    scanner.scan(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"d"), &watch);
    assert!(replacement.vouches_for(2));
}

/// With more live streams than cache slots, the extra ones fall back to the
/// borrowed map lookup in `FrameScanner::note_data`'s full-cache branch.
/// That branch carries every DATA frame of every stream past the second on
/// a multiplexed connection, so its counts must stay exactly as separate as
/// the cached ones -- including for the stream that never gets a slot.
#[test]
fn a_full_cache_still_counts_the_uncached_stream_separately() {
    let watch = EndStreamWatch::default();
    let first = watch.register(1);
    let second = watch.register(3);
    let third = watch.register(5);
    let mut scanner = FrameScanner::default();

    // Streams 1 and 3 claim both slots, so every frame for stream 5 misses
    // a full cache and goes through `EndStreamWatch::note_data`.
    let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"bb"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 5, b"ccc"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 5, b"dddd"));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"e"));
    scanner.scan(&wire, &watch);

    let cached: Vec<u32> = scanner
        .data_records
        .iter()
        .flatten()
        .map(|cached| cached.stream_id)
        .collect();
    assert_eq!(cached, vec![1, 3], "both slots must be taken by 1 and 3");

    let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"f");
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b""));
    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b""));
    scanner.scan(&wire, &watch);

    assert!(first.vouches_for(2));
    assert!(second.vouches_for(2));
    assert!(third.vouches_for(8), "the uncached stream must count 3+4+1");
    assert!(!third.vouches_for(2));
}

/// The whole two-slot design rests on one claim: a terminal frame ALWAYS
/// re-consults the shared map, so an application `forget` wins even while
/// the scanner's cache is still warm for that stream. The generation check
/// runs once at the top of a read batch, so a `forget` landing after it
/// leaves the cache warm for the rest of that batch -- which is precisely
/// the window pinned down here.
///
/// `forget_prevents_publication_with_a_warm_cache` does NOT cover this: its
/// `forget` lands between two `scan` calls, so the next batch's generation
/// check has already emptied the slot before the terminal frame arrives.
#[test]
fn a_mid_batch_forget_still_blocks_a_warm_cache_publication() {
    let watch = EndStreamWatch::default();
    let record = watch.register(1);
    let mut scanner = FrameScanner::default();

    let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"be");
    warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"fore"));
    scanner.scan(&warmup, &watch);

    // Stand in for a batch whose generation check has already run: nothing
    // will invalidate the slot for the remainder of it.
    scanner.discard_forgotten_data_records(&watch);
    watch.forget(1);
    assert!(
        scanner
            .data_records
            .iter()
            .flatten()
            .any(|cached| cached.stream_id == 1),
        "the cache must still be warm for the window under test"
    );

    // Stale increments on a forgotten record are tolerated ...
    scanner.note_data_frame(1, 5, false, &watch);
    assert_eq!(
        record.data_bytes.load(Ordering::Relaxed),
        11,
        "the stale increment must have reached the record, or this test is \
         not exercising the warm-cache window it claims to"
    );

    // ... but the terminal frame must not publish it, and must not add its
    // own payload to it either: the map lookup is what fails, before any
    // counting.
    scanner.note_data_frame(1, 2, true, &watch);

    assert!(!record.end_stream_observed());
    assert_eq!(record.data_bytes.load(Ordering::Relaxed), 11);
}

/// `Http2Session` forgets on `note_local_reset` and again on `Drop`, so the
/// second call routinely finds nothing. It must not advance the generation,
/// or every locally reset stream would cost the scanner an extra locked
/// liveness sweep on its next read.
#[test]
fn forgetting_an_absent_stream_does_not_advance_the_generation() {
    let watch = EndStreamWatch::default();
    watch.register(1);
    watch.forget(1);
    let after_live_removal = watch.forget_generation.load(Ordering::Acquire);

    watch.forget(1);
    watch.forget(999);
    assert_eq!(
        watch.forget_generation.load(Ordering::Acquire),
        after_live_removal
    );
}

/// The cache prune lives in `finish_goaway`, which runs when the payload's
/// last byte is consumed -- possibly several reads after the header. A warm
/// cache must be pruned by the same `last_stream_id` rule whenever that
/// lands, not only when the whole frame arrives in one read.
#[test]
fn a_split_goaway_prunes_a_warm_cache() {
    for split in 1..16 {
        let watch = EndStreamWatch::default();
        let kept = watch.register(1);
        let excluded = watch.register(3);
        let mut scanner = FrameScanner::default();

        let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"a");
        warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"b"));
        scanner.scan(&warmup, &watch);

        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"c"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"d"));
        for chunk in wire.chunks(split) {
            scanner.scan(chunk, &watch);
        }

        assert!(kept.vouches_for(2), "split={split}");
        assert!(!excluded.end_stream_observed(), "split={split}");
        assert!(
            !scanner
                .data_records
                .iter()
                .flatten()
                .any(|cached| cached.stream_id == 3),
            "the excluded stream must not stay cached, split={split}"
        );
    }
}

/// A tiny deterministic PRNG, so the differential test below reproduces
/// exactly without adding a dev-dependency.
struct Xorshift64(u64);

impl Xorshift64 {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// One step of a replayable history. Chopping the wire into explicit chunks
/// and interleaving `forget` calls is what lets every scanner configuration
/// observe byte-for-byte the same sequence of events.
enum Step {
    Feed(Vec<u8>),
    Forget(u32),
}

/// Build a randomized but reproducible history: a mix of plain and padded
/// DATA (with and without END_STREAM), trailers, RST_STREAM, GOAWAY and an
/// ignored frame type, over a small pool of stream ids, split across reads
/// at arbitrary offsets with `forget` calls dropped in between.
///
/// Returns the steps and the ids to register.
fn random_scenario(seed: u64) -> (Vec<Step>, Vec<u32>) {
    // Mix before forcing the low bit: a bare `seed | 1` would collapse each
    // even seed onto its odd successor and halve the distinct scenarios.
    let mut rng = Xorshift64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let stream_ids: Vec<u32> = (1..=9u32).step_by(2).collect();

    let mut wire = Vec::new();
    // RFC 9113 §6.8 forbids a later GOAWAY from raising `last_stream_id`,
    // and the scanner now poisons when one does -- which would turn the
    // whole tail of a history into no-ops in all four configurations and
    // hollow out the differential. Keep the generated sequence monotone
    // non-increasing so the comparison keeps its reach; the illegal
    // direction has its own dedicated contract test.
    let mut goaway_ceiling = u32::MAX;
    for _ in 0..24 {
        let stream_id = stream_ids[rng.below(stream_ids.len())];
        let payload = vec![0x5a; rng.below(6)];
        match rng.below(10) {
            0..=5 => {
                let flags = if rng.below(4) == 0 {
                    FLAG_END_STREAM
                } else {
                    0
                };
                if rng.below(4) == 0 {
                    let pad = rng.below(4) as u8;
                    wire.extend_from_slice(&padded_data_frame(flags, stream_id, &payload, pad));
                } else {
                    wire.extend_from_slice(&frame(FRAME_TYPE_DATA, flags, stream_id, &payload));
                }
            }
            6 => wire.extend_from_slice(&frame(
                FRAME_TYPE_HEADERS,
                FLAG_END_STREAM | 0x4,
                stream_id,
                b"\x88",
            )),
            7 => wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, stream_id, &[0; 4])),
            8 => {
                goaway_ceiling = goaway_ceiling.min(stream_id);
                wire.extend_from_slice(&frame(
                    FRAME_TYPE_GOAWAY,
                    0,
                    0,
                    &goaway_payload(goaway_ceiling),
                ));
            }
            // CONTINUATION: a type the scanner must skip by length only.
            _ => wire.extend_from_slice(&frame(0x9, 0, stream_id, &payload)),
        }
    }

    let mut steps = Vec::new();
    let mut rest = wire.as_slice();
    while !rest.is_empty() {
        let take = (1 + rng.below(11)).min(rest.len());
        steps.push(Step::Feed(rest[..take].to_vec()));
        rest = &rest[take..];
        if rng.below(6) == 0 {
            steps.push(Step::Forget(stream_ids[rng.below(stream_ids.len())]));
        }
    }

    (steps, stream_ids)
}

/// Replay one history against a fresh watch, returning each registered
/// stream's `(end_stream_observed, data_bytes)`.
fn replay(steps: &[Step], stream_ids: &[u32], mut scanner: FrameScanner) -> Vec<(bool, usize)> {
    let watch = EndStreamWatch::default();
    let records: Vec<_> = stream_ids
        .iter()
        .map(|stream_id| watch.register(*stream_id))
        .collect();

    for step in steps {
        match step {
            Step::Feed(chunk) => scanner.scan(chunk, &watch),
            Step::Forget(stream_id) => watch.forget(*stream_id),
        }
    }

    records
        .iter()
        .map(|record| {
            (
                record.end_stream_observed(),
                record.data_bytes.load(Ordering::Relaxed),
            )
        })
        .collect()
}

/// The scanner cache and the combined terminal path are pure optimizations:
/// over ANY frame history they must reach the same verdict as the code they
/// replaced. The two benchmark A/B switches make all four configurations
/// reachable, so replay one randomized history through each and compare.
///
/// `data_bytes` is compared too, not just the published flag. A warm cached
/// record CAN legitimately outcount the uncached one, but only for a
/// `forget` that lands mid-batch -- and a replayed `Step::Forget` always
/// lands on a batch boundary, where the next `scan`'s generation check
/// empties the slot before another frame arrives. So the counts must agree
/// here, and requiring that is what lets this test see a broken
/// `discard_forgotten_data_records`. The genuinely mid-batch window has its
/// own test: `a_mid_batch_forget_still_blocks_a_warm_cache_publication`.
#[test]
fn the_cache_and_the_combined_terminal_path_are_observationally_equivalent() {
    for seed in 1..=200u64 {
        let (steps, stream_ids) = random_scenario(seed);
        let baseline = replay(
            &steps,
            &stream_ids,
            FrameScanner {
                data_cache_disabled: true,
                terminal_data_combining_disabled: true,
                ..FrameScanner::default()
            },
        );

        for (name, scanner) in [
            ("cached+combined", FrameScanner::default()),
            (
                "uncached+combined",
                FrameScanner {
                    data_cache_disabled: true,
                    ..FrameScanner::default()
                },
            ),
            (
                "cached+legacy-terminal",
                FrameScanner {
                    terminal_data_combining_disabled: true,
                    ..FrameScanner::default()
                },
            ),
        ] {
            let observed = replay(&steps, &stream_ids, scanner);
            for (index, stream_id) in stream_ids.iter().enumerate() {
                assert_eq!(
                    observed[index].0, baseline[index].0,
                    "{name}: END_STREAM differs for stream {stream_id} (seed={seed})"
                );
                assert_eq!(
                    observed[index].1, baseline[index].1,
                    "{name}: byte count differs for stream {stream_id} (seed={seed})"
                );
            }
        }
    }
}

/// Manual release-mode microbenchmark for the incremental scanner cost.
///
/// Run with:
/// `cargo test -p pingora-core --release benchmark_end_stream_watch -- \
///     --ignored --nocapture --test-threads=1`
///
/// This deliberately lives beside the private scanner instead of exposing
/// benchmark-only production API.
///
/// What is inside the timed region, so the numbers are read correctly:
///
/// - The single-stream workloads register one stream PER ITERATION, because
///   their wire ends in END_STREAM and publication evicts the entry. So one
///   `register` (a lock, a HashMap insert and the one `Arc<StreamRecord>`
///   allocation) is amortized over that workload's frames. `headers_end`
///   isolates that floor: it exercises neither DATA optimization, so all
///   variants there measure registration plus frame parsing and nothing
///   else. Subtract it to read the per-request DATA saving in absolute
///   terms.
/// - The multi-stream workloads (`alternating_*`, `round_robin_*`) carry no
///   terminal frame and register their streams ONCE, outside the timed
///   region. Registration there would scale with the stream count and
///   dilute exactly the difference being measured. They therefore report
///   steady-state per-frame cost with a warm cache, which is the regime the
///   two-slot cache exists for.
///
/// Each variant uses its own watch, but that watch is shared across the
/// samples of that variant; HashMap capacity growth is paid during warm-up.
#[test]
#[ignore = "manual performance measurement"]
fn benchmark_end_stream_watch() {
    const SAMPLES: usize = 7;

    fn report(name: &str, iterations: usize, mut run: impl FnMut()) {
        for _ in 0..(iterations / 20).max(1) {
            run();
        }

        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = Instant::now();
            for _ in 0..iterations {
                run();
            }
            samples.push(started.elapsed().as_secs_f64() * 1e9 / iterations as f64);
        }
        samples.sort_by(f64::total_cmp);
        println!(
            "end_stream_watch_bench {name}: median={:.2} ns/op min={:.2} ns/op iterations={iterations}",
            samples[SAMPLES / 2], samples[0]
        );
    }

    fn data_run(frame_count: usize, payload_len: usize) -> Vec<u8> {
        let payload = vec![0x5a; payload_len];
        let mut wire = Vec::with_capacity(frame_count * (FRAME_HEADER_LEN + payload_len));
        for frame_index in 0..frame_count {
            let flags = if frame_index + 1 == frame_count {
                FLAG_END_STREAM
            } else {
                0
            };
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, flags, 1, &payload));
        }
        wire
    }

    let headers_end = frame(FRAME_TYPE_HEADERS, FLAG_END_STREAM | 0x4, 1, b"\x88");
    let one_data_end = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello");
    let response_16k = data_run(64, 16 * 1024);
    let response_64k = data_run(16, 64 * 1024);
    let streaming = data_run(1024, 1);

    for (name, wire, iterations) in [
        ("headers_end", &headers_end, 200_000),
        ("one_data_end", &one_data_end, 200_000),
        ("1mib_16k_frames", &response_16k, 10_000),
        ("1mib_64k_frames", &response_64k, 20_000),
        ("streaming_1024_frames", &streaming, 2_000),
    ] {
        report(&format!("{name}/raw_slice_control"), iterations, || {
            black_box(black_box(wire).as_slice());
        });

        let empty_watch = EndStreamWatch::default();
        let mut empty_scanner = FrameScanner::default();
        report(&format!("{name}/unregistered"), iterations, || {
            empty_scanner.scan(black_box(wire), &empty_watch);
            black_box(&empty_scanner);
        });

        let watched = EndStreamWatch::default();
        let mut watched_scanner = FrameScanner::default();
        report(&format!("{name}/watched"), iterations, || {
            let record = watched.register(1);
            watched_scanner.scan(black_box(wire), &watched);
            black_box(record.end_stream_observed());
        });

        let no_cache_watch = EndStreamWatch::default();
        let mut no_cache_scanner = FrameScanner {
            data_cache_disabled: true,
            ..FrameScanner::default()
        };
        report(&format!("{name}/watched_no_cache"), iterations, || {
            let record = no_cache_watch.register(1);
            no_cache_scanner.scan(black_box(wire), &no_cache_watch);
            black_box(record.end_stream_observed());
        });

        let legacy_terminal_watch = EndStreamWatch::default();
        let mut legacy_terminal_scanner = FrameScanner {
            data_cache_disabled: true,
            terminal_data_combining_disabled: true,
            ..FrameScanner::default()
        };
        report(&format!("{name}/legacy_terminal"), iterations, || {
            let record = legacy_terminal_watch.register(1);
            legacy_terminal_scanner.scan(black_box(wire), &legacy_terminal_watch);
            black_box(record.end_stream_observed());
        });
    }

    // Steady-state multiplexed cost. These wires carry no terminal frame,
    // so the registrations survive every iteration and can be taken once,
    // outside the timed region: what is measured is the per-DATA-frame cost
    // with a warm cache, undiluted by a `register` that would scale with the
    // stream count.
    fn round_robin_wire(stream_count: u32) -> Vec<u8> {
        let mut wire = Vec::new();
        for frame_index in 0..1024u32 {
            let stream_id = 1 + 2 * (frame_index % stream_count);
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, stream_id, b"x"));
        }
        wire
    }

    for stream_count in [2u32, 3, 8, 32] {
        let wire = round_robin_wire(stream_count);
        let name = if stream_count == 2 {
            "alternating_1024_frames".to_string()
        } else {
            format!("round_robin_{stream_count}_streams")
        };

        let cached_watch = EndStreamWatch::default();
        let cached_records: Vec<_> = (0..stream_count)
            .map(|index| cached_watch.register(1 + 2 * index))
            .collect();
        let mut cached_scanner = FrameScanner::default();
        report(&format!("{name}/watched"), 1_000, || {
            cached_scanner.scan(black_box(&wire), &cached_watch);
            black_box(&cached_records);
        });

        let no_cache_watch = EndStreamWatch::default();
        let no_cache_records: Vec<_> = (0..stream_count)
            .map(|index| no_cache_watch.register(1 + 2 * index))
            .collect();
        let mut no_cache_scanner = FrameScanner {
            data_cache_disabled: true,
            ..FrameScanner::default()
        };
        report(&format!("{name}/watched_no_cache"), 1_000, || {
            no_cache_scanner.scan(black_box(&wire), &no_cache_watch);
            black_box(&no_cache_records);
        });
    }

    // Two live but idle streams pin both slots, while all traffic belongs to
    // a third. The cache can never fill for the hot stream, so this bounds
    // the worst case the fixed two-slot design admits (see
    // `FrameScanner::data_records`); it is the one shape where the cache is
    // pure overhead.
    let mut pinned = Vec::new();
    for _ in 0..1024 {
        pinned.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 5, b"x"));
    }
    let pinned_warmup = {
        let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"x");
        warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"x"));
        warmup
    };

    let pinned_watch = EndStreamWatch::default();
    let pinned_records = [1, 3, 5].map(|id| pinned_watch.register(id));
    let mut pinned_scanner = FrameScanner::default();
    pinned_scanner.scan(&pinned_warmup, &pinned_watch);
    report("pinned_slots_1024_frames/watched", 1_000, || {
        pinned_scanner.scan(black_box(&pinned), &pinned_watch);
        black_box(&pinned_records);
    });

    let pinned_no_cache_watch = EndStreamWatch::default();
    let pinned_no_cache_records = [1, 3, 5].map(|id| pinned_no_cache_watch.register(id));
    let mut pinned_no_cache_scanner = FrameScanner {
        data_cache_disabled: true,
        ..FrameScanner::default()
    };
    report("pinned_slots_1024_frames/watched_no_cache", 1_000, || {
        pinned_no_cache_scanner.scan(black_box(&pinned), &pinned_no_cache_watch);
        black_box(&pinned_no_cache_records);
    });

    // Lock contention is the cost this whole change exists to remove, so
    // both variants run against a thread churning registrations. Absolute
    // numbers here move with how much CPU the churn thread gets and are NOT
    // comparable across machines or runs; only the within-run ratio between
    // the two variants means anything.
    let mut streaming_open = Vec::new();
    for _ in 0..1024 {
        streaming_open.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"x"));
    }

    fn with_churn(watch: &Arc<EndStreamWatch>, mut run: impl FnMut()) {
        let keep_churning = Arc::new(AtomicBool::new(true));
        let churn_watch = watch.clone();
        let churn_flag = keep_churning.clone();
        let churner = std::thread::spawn(move || {
            let mut stream_id = 3u32;
            while churn_flag.load(Ordering::Acquire) {
                let record = churn_watch.register(stream_id);
                churn_watch.forget(stream_id);
                black_box(record);
                stream_id = stream_id.wrapping_add(2).max(3);
            }
        });
        run();
        keep_churning.store(false, Ordering::Release);
        churner.join().unwrap();
    }

    let contended_watch = Arc::new(EndStreamWatch::default());
    let contended_record = contended_watch.register(1);
    let mut contended_scanner = FrameScanner::default();
    with_churn(&contended_watch, || {
        report(
            "streaming_1024_frames/concurrent_register_forget",
            2_000,
            || {
                contended_scanner.scan(black_box(&streaming_open), &contended_watch);
                black_box(&contended_record);
            },
        );
    });

    let contended_no_cache_watch = Arc::new(EndStreamWatch::default());
    let contended_no_cache_record = contended_no_cache_watch.register(1);
    let mut contended_no_cache_scanner = FrameScanner {
        data_cache_disabled: true,
        ..FrameScanner::default()
    };
    with_churn(&contended_no_cache_watch, || {
        report(
            "streaming_1024_frames/concurrent_register_forget_no_cache",
            2_000,
            || {
                contended_no_cache_scanner
                    .scan(black_box(&streaming_open), &contended_no_cache_watch);
                black_box(&contended_no_cache_record);
            },
        );
    });
}
