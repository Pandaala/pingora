# Complete terminal HEADERS validation after upstream h2 fixes

Status: deferred; H2-004 blocked by H2-001 and upstream h2 trailer validation.

## Problem

The wire observer can record that a HEADERS frame carried END_STREAM, but it
cannot decode HPACK or decide whether the block is a valid empty trailer map.
The current h2 receive path can discard a forbidden trailer pseudo-field and
expose the result as `Some(empty)`, which is indistinguishable from a genuinely
valid empty trailer block through Pingora's public `RecvStream` API.

The narrowed fail-closed state therefore deliberately rejects terminal
`Some(empty)` after a non-header-only response. This protects response and cache
integrity, but it is a compatibility limitation for valid empty trailers.

## Required follow-up

After a normal upstream h2 release rejects every trailer pseudo-field and
oversized trailer block before publishing a trailer event:

1. Upgrade h2 through the normal dependency path and enable its decoder-level
   negative tests.
2. Treat `Some(empty)` and `Some(nonempty)` as validated trailer success while
   retaining `None` plus observed terminal HEADERS as a permanent error.
3. Apply the same permanent result latch to async body, poll body, and direct
   `read_trailers()` calls, including repeated reads.
4. Enable the existing pseudo-only, mixed pseudo/ordinary, valid-empty,
   same-burst reset, and cache-admission contracts at the minimum and resolved
   h2 versions.

## Safety constraint

Do not infer trailer validity from raw frame shape or wire END_STREAM. The wire
observer may detect a missing public validation result, but only h2's decoder
may validate the header block.

## Tracking references

- Edgion audit finding: `H2-004-trailer-api-permanent-latch.md`.
- Decoder prerequisite: `H2-001-decoder-trailer-validation.md`.
- Sibling fork record: [h2-trailer-validation.md](h2-trailer-validation.md).

## Upstream references

- The exact trailer pseudo-header/oversize handoff does not currently have a
  matching public Pingora or h2 issue. File one before attempting the upgrade.
- Current h2 `recv_trailers()` closes the stream and queues
  `frame.into_fields()` without checking the decoded pseudo fields or
  `is_over_size()`:
  https://github.com/hyperium/h2/blob/master/src/proto/streams/recv.rs
- Related response-completion discussion filed by Pingora contributor
  `eaufavor`: https://github.com/hyperium/h2/issues/806. Its fix was accepted
  in https://github.com/hyperium/h2/pull/810 and released in h2 0.4.10, but it
  does not fix trailer pseudo-header validation.
- The separate GOAWAY/public-API ambiguity discussion is
  https://github.com/hyperium/h2/issues/741; it remains open and has no linked
  implementation.
