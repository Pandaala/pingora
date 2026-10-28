# Request prefix discard and downstream connection reuse

## Corrected boundary

Releasing or explicitly discarding a registered prefix source clears
`prefix_delivery_pending`. That flag represents bytes retained for first
delivery, not unread downstream transport bytes. H1 and H2 apply the transition
at the same two ownership boundaries: final-response release of a ready/done
store, and explicit drain taking the store.

Previously a three-byte H1 body captured with a four-byte window reached real
EOF, but a local 403 released its store while leaving delivery pending. Reuse
then tried to drain a body that was already complete; the public read guard
returned InternalError and prevented downstream keepalive reuse.

## Preserved invariants and guards

- A final response does not release an actively replaying store.
- `prefix_active` stays sticky: upstream retry and source re-registration remain
  forbidden after discard.
- Capture-pending and poisoned states are not cleared; draining a cancelled
  capture still fails closed.
- Discarded prefix and overrun bytes do not increment transport counters again;
  drain consumes only the live remainder.

The H1/H2 `test_prefix` suites cover release/discard, cancellation, counters,
and active replay. The H1 reuse regression actually reads the next request on
the returned connection. Re-evaluate these guards when changing source release,
drain, cancellation, or the distinction between transport EOF and delivery EOF.
