---
name: h2-goaway-persistent-ceiling-fail-closed
description: Use when reviewing Pingora's upstream H2 END_STREAM wire observer around GOAWAY handling, stream eligibility after a connection teardown, or malformed/illegal GOAWAY frames.
status: implemented-pending-project-checks
finding_id: H2-005
closed: 2026-08-27
---

# H2 GOAWAY persistent ceiling and fail-closed rejection

## Conclusion

A GOAWAY's `last_stream_id` is a PERSISTENT connection ceiling, not a one-time
prune, and any GOAWAY the peer was not allowed to send poisons the observer
instead of contributing a guessed ceiling. Fix suggestions of the form "the
`retain` already removes the excluded streams, so the threshold does not need
to be stored" are **not accepted**: `retain` only reaches entries that were in
the map when the frame was read, and a stream registered afterwards is exactly
the one `h2` has already written off.

## Core rationale

**1. The ceiling must outlive the frame that carried it**

`EndStreamWatch::note_connection_torn_down` used to be a single
`pending.retain(|id, _| *id <= last_stream_id)`. Registration runs on
application threads under the same lock but consulted only the poison state, so
a stream allocated after the GOAWAY was inserted normally and a subsequent
legal END_STREAM published wire evidence for it. `h2` errors such a stream out
the moment it processes the GOAWAY and never delivers its body, so that
evidence is a false positive on the exact predicate
(`Http2Session::response_body_complete_at_stream_end`) that decides whether a
truncated upstream response is handed downstream as complete.

The ceiling therefore lives in `WatchState::Active`, next to the pending map and
under the same mutex as poisoning, and `Registration::register` refuses to
insert any `stream_id` above it. A refused registration still returns a blank
`Arc<StreamRecord>` so callers need no special case — the same shape H2-002
established for a poisoned connection.

**2. A frame the peer may not send names no trustworthy threshold**

Four cases are connection errors in `h2`, after which it delivers nothing
further, so none of them may be read as a usable `last_stream_id`:

- a declared payload shorter than the fixed eight octets (`last_stream_id` plus
  `error_code`). The old code trusted declared lengths 4..=7, because
  `LastStreamId::get()` only requires its own four bytes to be complete;
- a GOAWAY on a nonzero stream id — it is a connection-control frame;
- EOF before the complete declared payload arrived (already handled by H2-002's
  `has_partial_frame` poison, and depended on here);
- a later GOAWAY raising `last_stream_id`, which RFC 9113 §6.8 forbids.

Each now poisons. The first two are rejected at the frame header, before any
`LastStreamId` state is created, and `scan` returns immediately rather than
continuing to parse bytes it can no longer frame correctly.

**3. Poisoning is the conservative direction, not a behavior change**

Poison only withdraws wire evidence. The byte stream handed to `h2` is
untouched, no log or metric is emitted, and the affected response falls back to
the other three completion proofs (h2 end-stream state, latched EOF, satisfied
Content-Length) — i.e. back to being treated as a failed/retryable request
rather than a clean EOF. Streams at or below a legal ceiling keep publishing,
so the standard graceful-drain sequence `GOAWAY(NO_ERROR, 2^31-1)` followed by
a lower real id still works, and a decreasing second GOAWAY still only narrows
the surviving set.

## Fix suggestions not accepted

- "Clamp a malformed GOAWAY to `last_stream_id = 0` instead of poisoning" — that
  is what `unwrap_or(0)` did; it clears the map once but leaves later
  registrations publishable, which is the defect.
- "Trust a complete four-octet `last_stream_id` even when `error_code` is
  truncated" — the frame is a connection error either way and `h2` delivers
  nothing after it.
- "Track the ceiling in `FrameScanner` instead of `EndStreamWatch`" — the
  scanner is not consulted by `Registration::register`, and splitting the state
  across two owners reintroduces the registration race the shared lock exists
  to close.
- "Ignore an increasing second GOAWAY, since map removal already protects the
  excluded streams" — it protects the already-excluded ones but leaves streams
  below the old ceiling publishing on a connection whose frame sequence is
  provably illegal.

## Review rules

- Keep the GOAWAY ceiling under the same lock as stream registration,
  publication and poison state; a "ceiling known but not yet applied to
  registration" combination must stay unrepresentable.
- Apply a `last_stream_id` only after the frame's complete declared payload has
  been consumed.
- Reject, permanently, every GOAWAY that is malformed, carried on a nonzero
  stream, truncated by EOF, or raises a previous `last_stream_id`.
- Never fall back to a guessed threshold when the field cannot be trusted; fail
  closed.
- This rule shares H2-002's poison primitive but is a separate contract; do not
  merge the two when reviewing.

## Regression coverage

In `pingora-core/src/protocols/http/v2/end_stream_watch.rs`:

- `final_contract_goaway_declared_lengths_zero_through_seven_poison_the_scanner`
  — declared lengths 0..=7 at every read split point, each followed by a later
  registration and a legal END_STREAM that must not publish.
- `final_contract_goaway_on_a_nonzero_stream_poisons_the_scanner`
- `final_contract_increasing_goaway_last_stream_id_poisons_the_scanner`
- `final_contract_goaway_ceiling_persists_for_later_registrations` — the
  positive/negative pair: a stream above the ceiling never publishes however
  late it registers, while a survivor at or below it still does.
- `goaway_frame_reserved_stream_id_bit_is_masked_and_stays_legal`
- `goaway_error_code_and_debug_data_variants_apply_the_same_ceiling`
- Pre-existing and still passing: `decreasing_goaway_last_stream_id_narrows_the_surviving_set`,
  `goaway_keeps_streams_at_or_below_last_stream_id`,
  `goaway_last_stream_id_is_reassembled_and_masked`,
  `final_contract_eof_in_a_goaway_payload_poisons_the_scanner` (H2-002).

## Re-evaluation triggers

Re-open only if:

- `h2` starts delivering queued data for streams above a GOAWAY's
  `last_stream_id`, or stops treating short/misdirected GOAWAY frames as
  connection errors;
- the observer is ever shared across connections, or stream ids can repeat,
  which would break the "one ceiling per connection" premise;
- a legitimate peer is found to raise `last_stream_id`, which would make the
  poison a false positive rather than an RFC violation.

## Historical note

`end_stream_watch.rs` does not exist in `cloudflare/pingora` or in the fork's
`origin/main`; it was introduced whole by `682506d` (2026-08-25) on
`edgion_v3`, with the one-time `retain` present from that first version. This
is a fork-introduced fail-open defect on a fork-introduced evidence path, not
an upstream Pingora or `h2` bug — see
[h2-end-stream-observer-read-terminal-poison.md](h2-end-stream-observer-read-terminal-poison.md)
for the sibling read-terminal contract.

## Reference cases

- `../Edgion/tasks/todo/pingora-h2-end-stream-watch-simplification/issues/H2-005-goaway-persistent-poison.md`
- `edgion-changes/features/h2-end-stream.md` — "GOAWAY eligibility"
- [h2-end-stream-observer-read-terminal-poison.md](h2-end-stream-observer-read-terminal-poison.md) (H2-002)
