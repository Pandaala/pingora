# Upstream synchronization

## Recommended order

1. Fetch upstream without moving the feature branch.
2. Record the current main SHA and preview with `git merge-tree` or a disposable
   worktree.
3. Rebase or rebuild the commits in feature order, running the focused tests
   after each feature.
4. Run the complete verification matrix before publishing the rewritten stack.

## Conflict hot spots

The expected shared-main conflict surface is intentionally concentrated:

- `pingora-core/src/listeners/{l4.rs,mod.rs}`: L4 buffer, pre-TLS callback and
  PROXY policy must all remain present and ordered.
- `pingora-core/src/protocols/http/v1/server.rs` and `v2/server.rs`: session body
  facts, replay lifecycle and upstream main's proxy-task API.
- `pingora-core/src/connectors/http/v2.rs`: keep configurable H2 windows while
  wrapping IO with the END_STREAM watch.
- `pingora-proxy/src/proxy_{h1,h2,custom}.rs`: preserve upstream batch helpers,
  pending-task backpressure, warning suppression and finish-hit guards while
  carrying Edgion filter state through them.
- `pingora-proxy/src/lib.rs`: preserve H1 transfer-coding admission before
  `on_connection_reuse` and all routing phases. Its forced-close policy must be
  applied after every mutable error hook and must directly veto returning a
  reusable downstream session; keepalive metadata alone is not that veto.
- `pingora-proxy/src/proxy_common.rs`: upstream hop-by-hop sanitization and the
  Edgion event/disposition state machines are independent and both required.

## Resolution rules

- Current main owns dependency versions, release metadata and unrelated
  refactors.
- Prefer adapting Edgion calls to current main helpers over restoring old
  inline loops.
- Preserve current main's security and protocol checks even when the older
  Edgion implementation has a larger conflicting block.
- Compare behavior and tests, not raw line counts. A smaller adapter around a
  current helper is preferable to copying an old function.
- Keep feature commits reviewable and single-purpose; follow-up fixes belong to
  the feature they correct rather than a generic final `fix` commit.

## Consumer cutover

The repository API is only half of a safe rollout. Before switching a consumer
lock or deployment to a new Edgion commit, validate its listener configuration
against the new binary, reject invalid trust-source input, and run the H1/H2
request and response matrices used by that consumer. Update the consumer lock
only after those checks pass; do not infer deployment from this branch's
existence.
