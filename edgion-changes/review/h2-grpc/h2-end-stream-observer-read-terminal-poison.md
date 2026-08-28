---
name: h2-end-stream-observer-read-terminal-poison
description: Use when reviewing Pingora's upstream H2 END_STREAM wire observer around read errors, partial EOF, or evidence publication before a complete DATA frame reaches h2.
status: implemented-pending-project-checks
finding_id: H2-002
---

# H2 END_STREAM observer read terminal poison

## Conclusion

The Pingora fork's wire observer must never retain more evidence than h2's
consumer accepts. Bytes placed in a `ReadBuf` together with `Ready(Err)` are not
advanced into h2's decode buffer, so the observer now poisons the connection
without scanning those bytes. EOF while a frame header or payload is incomplete
also poisons the observer.

Poisoning replaces `WatchState::Active(pending)` with the terminal
`WatchState::Poisoned` variant, dropping pending records and preventing every
later registration or publication. The state and pending map share one lock,
making registration, publication, and terminalization linearly ordered while
making a "poisoned but non-empty" combination unrepresentable. Already
published evidence from a complete frame read before the terminal event remains
immutable.

## Publication boundary

A DATA frame's END_STREAM flag is not published when only its header or Pad
Length byte has arrived. Publication waits for the complete declared payload,
matching the earliest point at which h2 can decode the frame. This is necessary
for padding-only terminal DATA: its application byte count can match the body
already delivered even when EOF truncates the remaining padding.

The wrapped byte stream remains transparent. Read results and `ReadBuf` contents
are returned unchanged; poison affects observer evidence only.

## Review rules

- Never scan bytes from `AsyncRead::Ready(Err)` unless h2's actual consuming
  stack is first changed and proven to retain those exact bytes.
- Keep poison state under the same lock as stream registration and publication.
- Do not publish DATA END_STREAM before the complete frame payload has arrived.
- A poisoned registration may return a blank record for API stability, but it
  must not insert that record into the publication map.
- Do not conflate this rule with H2-005's GOAWAY ceiling and malformed-GOAWAY
  state machine; they share a poison primitive but have separate contracts. See
  [h2-goaway-persistent-ceiling-fail-closed.md](h2-goaway-persistent-ceiling-fail-closed.md).

## Regression coverage

- `final_contract_bytes_delivered_with_an_error_are_not_scanned`
- `final_contract_eof_in_a_partial_frame_header_poisons_the_scanner`
- `final_contract_eof_in_a_terminal_data_payload_never_publishes`
- `final_contract_eof_in_a_goaway_payload_poisons_the_scanner`

The EOF tests cover every incomplete frame-header split, every incomplete
GOAWAY payload position, plain terminal DATA payload splits, and padding-only
terminal DATA after an already delivered body. Every case also verifies that a
later registration cannot publish evidence.

## Historical note

The local `edgion`, `edgion_v2`, `edgion_v3`, and `origin/edgion` branches all
shared the unsafe `res.is_ready()` scan and had no EOF poison. This is a fork
observer defect, not a behavior supplied by an older Edgion branch or repairable
by the separate upstream h2 trailer-decoder fix.
