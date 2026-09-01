# Upstream h2 trailer validation

Status: deferred for the upstream trailer fix. The separate fork-owned watcher
evidence work was resolved on 2026-08-29.

## Problem

The h2 trailer receive path can discard forbidden pseudo-fields while exposing
ordinary fields from the same block as a valid-looking trailer map. It also
does not reject oversized trailer blocks at that handoff. A following
`RST_STREAM(NO_ERROR)` can hide some decoder failures from Pingora's response
completion logic.

This is a pre-existing, low-frequency issue requiring a malformed response from
a buggy or malicious upstream. This fork intentionally does not vendor or
locally patch h2 for it.

## Resolved fork-owned dependency governance

[Finding 004](../review/findings/004-medium-end-stream-watch-is-not-pinned-to-an-audited-h2.md)
records the completed watcher-source audit and version matrix. The fork now
requires h2 0.4.19 or newer: 0.4.16 through 0.4.18 preserve received
END_STREAM, but fail the fork's large-window continuing-upload contract with
`too_many_data_frames`; 0.4.19 scales the automatic DATA-frame budget and
passes. The normal open upstream range is retained without an exact pin,
upper bound, or vendored dependency.

That closure does not change this pending issue. Receive-queue, GOAWAY,
buffer-lifetime, frame-order, trailer, and byte-delivery contracts still
prevent removing the watcher, and h2 0.4.19 still has the terminal trailer
validation limitation described above.

## Periodic review

When h2 or Pingora changes:

1. Record both the minimum supported h2 release and every resolved release
   used for verification. `Cargo.lock` is a resolution snapshot, not an exact
   dependency pin.
2. Re-audit the private h2 invariants documented by
   `end_stream_watch.rs`: reset-state preservation, receive-queue draining,
   GOAWAY handling, receive-buffer clearing, and wire-order processing. Update
   the watcher documentation to name the versions actually audited and any
   behavioral differences between them.
3. Check whether h2 rejects oversized trailer blocks and every pseudo-field in
   `recv_trailers()` before closing the receive side or queueing trailers.
4. Upgrade through the normal upstream dependency only. Keep the upstream
   version range unless a separate dependency-policy decision approves an
   exact pin, upper bound, or vendored fork.
5. Enable and run the pseudo-only, mixed pseudo/ordinary, oversized, fragmented
   CONTINUATION, and same-burst `RST_STREAM(NO_ERROR)` regression cases across
   async body, poll body, direct trailers, and cache admission, together with
   the DATA/HEADERS END_STREAM, reset ordering, GOAWAY, flow-control drop,
   content-length mismatch, and local-reset matrix.
6. Reconsider END_STREAM watcher simplification only after all contracts pass.

Until then, wire-level END_STREAM alone must not prove response validity or
authorize cache admission.
