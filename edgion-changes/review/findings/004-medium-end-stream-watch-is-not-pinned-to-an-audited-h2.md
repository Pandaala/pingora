# END_STREAM watcher relies on an h2 implementation version it does not pin

Status: open

Severity: medium

Fork baseline: `upstream/main...edgion_v3`; the wire-level watcher and its
private-implementation proof are fork-only.

## Problem

`pingora-core/src/protocols/http/v2/end_stream_watch.rs:84-104` says its
correctness proof was verified against h2 0.4.15 and that every upgrade must
re-check several private implementation facts.

The dependency does not enforce that premise:

- Root `Cargo.toml:44` declares `h2 = ">=0.4.16"`, with no upper bound.
- The current `Cargo.lock` resolves the 0.4 line to h2 0.4.19, not 0.4.15.

This is not a cosmetic version mismatch. h2 0.4.19 changed the private reset
state machine: `State::recv_reset` now preserves the received-END_STREAM fact
and includes a `recv_reset_preserves_received_end_stream` test. The watcher
comment still describes reset as overwriting that evidence.

The watcher also relies on other unpublished details: when `pending_recv` is
drained relative to errors, which paths clear receive buffers, GOAWAY handling,
and strict frame processing order. SemVer does not promise those internals.

This issue is separate from the known trailer-decoder limitation documented in
`AGENTS.md`; it concerns the fork's own proof and dependency governance.

## Impact

- The documented proof does not match the dependency actually compiled.
- An ordinary `cargo update` can silently select another h2 implementation
  while leaving the security/cache-completeness argument apparently valid.
- A future internal change can make the watcher accept incomplete body evidence
  or reject valid responses without a source-level conflict in this repository.
- The watcher may now contain avoidable hot-path machinery because part of its
  original premise changed upstream; that cannot be simplified until the new
  behavior is audited end to end.

## Recommended fix

Audit every listed private invariant against the currently selected h2 version
and update the module documentation to name that exact version and behavior.

Then pin h2 to the audited exact version or a deliberately audited narrow
compatibility range. Make h2 upgrades explicit review events rather than
incidental lockfile updates. Re-evaluate whether the received-END_STREAM state
now exposed by h2 can safely replace any watcher paths; retain the watcher where
wire byte counts or other evidence remain necessary.

## Required tests and gates

- CI must assert that the resolved h2 0.4 version matches the audited version
  recorded beside the watcher.
- An h2 upgrade checklist must run the DATA/HEADERS END_STREAM, RST ordering,
  GOAWAY ceiling, flow-control drop, content-length mismatch, and local-reset
  matrix.
- Keep the existing black-box reset/cache tests, but do not treat them as proof
  of all private invariants without the source audit.
