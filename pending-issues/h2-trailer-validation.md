# Upstream h2 trailer validation

Status: deferred; waiting for upstream h2.

## Problem

The h2 trailer receive path can discard forbidden pseudo-fields while exposing
ordinary fields from the same block as a valid-looking trailer map. It also
does not reject oversized trailer blocks at that handoff. A following
`RST_STREAM(NO_ERROR)` can hide some decoder failures from Pingora's response
completion logic.

This is a pre-existing, low-frequency issue requiring a malformed response from
a buggy or malicious upstream. This fork intentionally does not vendor or
locally patch h2 for it.

## Periodic review

When h2 or Pingora changes:

1. Check whether h2 rejects oversized trailer blocks and every pseudo-field in
   `recv_trailers()` before closing the receive side or queueing trailers.
2. Upgrade through the normal upstream dependency only.
3. Enable and run the pseudo-only, mixed pseudo/ordinary, oversized, fragmented
   CONTINUATION, and same-burst `RST_STREAM(NO_ERROR)` regression cases across
   async body, poll body, direct trailers, and cache admission.
4. Reconsider END_STREAM watcher simplification only after all contracts pass.

Until then, wire-level END_STREAM alone must not prove response validity or
authorize cache admission.
