# Upstream h2 trailer validation

Status: deferred for the upstream trailer fix. Fork-owned documentation and
dependency-governance follow-up is tracked separately below and does not wait
for upstream.

## Problem

The h2 trailer receive path can discard forbidden pseudo-fields while exposing
ordinary fields from the same block as a valid-looking trailer map. It also
does not reject oversized trailer blocks at that handoff. A following
`RST_STREAM(NO_ERROR)` can hide some decoder failures from Pingora's response
completion logic.

This is a pre-existing, low-frequency issue requiring a malformed response from
a buggy or malicious upstream. This fork intentionally does not vendor or
locally patch h2 for it.

## Fork-owned watcher evidence and dependency governance

The fork's watcher documentation still names h2 0.4.15 and incorrectly calls
the workspace's ignored `Cargo.lock` a version pin. Correcting that evidence is
fork-owned work and is not blocked on an upstream h2 release.

Current evidence is:

- upstream Pingora requires h2 0.4.16 or newer;
- the ignored local lockfile resolved h2 0.4.19 when the audit was performed;
- h2 0.4.16 and 0.4.19 preserve received END_STREAM across a later reset,
  unlike the behavior described by the current watcher documentation; and
- the remaining receive-queue, GOAWAY, buffer-lifetime, frame-order, trailer,
  and byte-delivery contracts still prevent removing the watcher on the
  strength of that reset-state improvement alone.

The formal Edgion task
`../Edgion/tasks/todo/pingora-h2-end-stream-watch-simplification/` owns the
minimum/current-version test matrix and dependency-policy decision. Its
`issues/H2-013-documentation-evidence-alignment.md` owns alignment of fork
documentation with executable evidence, and `06-decisions.md` records the
decision to retain the normal upstream version range rather than add an exact
pin, upper bound, tracked lockfile, or vendored h2.

## Periodic review

When h2 or Pingora changes:

1. Record both the minimum supported h2 release and every resolved release
   used for verification. The workspace's ignored `Cargo.lock` is local state,
   not a dependency pin or review artifact.
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
