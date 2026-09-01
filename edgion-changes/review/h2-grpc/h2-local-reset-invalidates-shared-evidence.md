---
name: h2-local-reset-invalidates-shared-evidence
description: Use when reviewing Pingora's upstream H2 END_STREAM wire observer around locally reset streams, RST_STREAM ordering, or whether dropping a pending-map entry is enough to withdraw wire evidence.
status: fixed
finding_id: H2-006
closed: 2026-08-27
---

# H2 local reset invalidates shared evidence

## Conclusion

Withdrawing wire evidence for a stream THIS side resets is an irreversible mark
on the shared `StreamRecord`, made BEFORE the local RST_STREAM is queued, and
honored by publication under the same lock. Fix suggestions of the form "the
map entry is removed, so nothing can publish afterwards" are **not accepted**:
the record is shared, and a removal reaches neither the session's
`PeerEndStream` clone nor the scanner's bounded `data_records` cache.

## Core rationale

**1. `forget` cannot retract what publication already handed out**

`EndStreamWatch::forget` removes a `HashMap` entry. The `Arc<StreamRecord>`
behind it is already cloned into `Http2Session::peer_end_stream` and, in
`pingora-proxy`, into `UpstreamBodyWrite::upstream_response_ended`. If
`FrameScanner::publish` wins the race it removes the entry AND stores
`end_stream = true` on that shared record; the later `forget` finds no key and
retracts nothing. The mark therefore lives on the record itself.

**2. The mark must precede the reset, not follow it**

Every call site used to read `send_reset(...)` then `note_local_reset()`. In
between, the connection read task can publish the peer's END_STREAM. From the
moment the local reset is queued `h2` starts DROPPING the DATA it decodes
(`Recv::recv_data`'s `is_ignoring_frame`) while a peer RST_STREAM landing
afterwards still surfaces as a remote `NO_ERROR` (`State::recv_reset` when
`is_pending_send`), so evidence published in that window describes a body
nobody will receive. `upstream_write_error_outcome` would then classify the
guaranteed post-reset write failure as `UpstreamDoneReceiving` and report the
abandoned exchange a success, and
`response_body_complete_at_stream_end` could accept the body as whole — the
truncation-laundered-into-clean-EOF failure the watch exists to prevent, in
reverse.

All four sites now invalidate first and reset second: `Http2Session::shutdown`
and the three arms of `pingora-proxy`'s `proxy_down_to_up`.

**3. Publication holds the lock across its stores**

`publish` used to take the lock only for the `remove`; the guard was a
temporary in the `let` scrutinee, so `end_stream.store` executed outside it.
That is the window itself. The guard is now held across the stores, which is
what makes invalidation and publication linearly ordered instead of racing.

## Deliberate non-retraction

Evidence published strictly BEFORE the mark is KEPT. The peer flagged the end
of its body before this side decided to reset, so the body was whole at that
point. Retracting it would regress
`DownstreamRequestOutcome::CompleteWithoutUpstreamReuse` and every §8.1
exchange whose response is already in hand into a failure. Do not "simplify"
this by making `PeerEndStream::observed` / `vouches_for` consult the flag.

## Review rules

- Never withdraw wire evidence by map removal alone; mark the shared record.
- Never queue a local RST_STREAM before the invalidation.
- Keep the invalidation flag under the same lock as publication, and keep
  publication's stores inside that lock.
- Do not make readers of `PeerEndStream` consult the flag; it gates
  publication only.
- Do not conflate this with H2-002's read-error/EOF poison or H2-005's GOAWAY
  ceiling. All three withdraw evidence, but H2-006 is per-stream and
  application-initiated, not connection-wide and wire-initiated. See
  [h2-end-stream-observer-read-terminal-poison.md](h2-end-stream-observer-read-terminal-poison.md)
  and [h2-goaway-persistent-ceiling-fail-closed.md](h2-goaway-persistent-ceiling-fail-closed.md).

## Regression coverage

- `a_local_invalidation_blocks_the_peers_later_terminal_evidence`
- `publication_refuses_an_invalidated_record_still_in_the_map`
- `terminal_headers_are_refused_for_an_invalidated_record`
- `invalidation_does_not_retract_evidence_published_before_it`
- `an_invalidated_stream_neither_poisons_nor_feeds_a_later_one`

The middle three pin the flag gate independently of the map removal, so
re-introducing "removal is enough" fails them. The call-site ORDERING is not
covered by an executable test — it is inherently a race window — and is held by
the `note_local_reset` contract comment plus this entry.

## Closure evidence

Implemented in Pingora `a4a255a`. Final project verification passed the watcher
suite (54 passed, 8 intentionally ignored characterization cases),
`pingora-proxy --lib` (194 passed, 2 ignored), and the focused H2 reset, stall,
and cache/reuse targets (8, 4, and 8 passed). No H2-006-specific project check
remains; see the [verification matrix](../../verification/test-matrix.md).

## Provenance

Fork-only defect. `end_stream_watch.rs`, `PeerEndStream` and `note_local_reset`
do not exist in `upstream/main`; they arrived with fork commit `682506d`
("fix(http2): preserve end-stream evidence across resets"). Upstream Pingora is
not vulnerable because it has no source (iv) at all — it simply fails the §8.1
exchange. Do not file this against upstream or expect a rebase to resolve it.
