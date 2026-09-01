# Upstream synchronization

This is the canonical procedure for periodically adopting Cloudflare Pingora
`main` into the Edgion fork. A single replaceable "last synchronized commit" is
not sufficient provenance: every published synchronization must retain the old
fork head, identify the exact upstream target, and record the resulting fork
head and consumer verification.

## Ref and remote ownership

- `upstream` is the official Cloudflare Pingora remote. Only
  `upstream/main` is an upstream synchronization source.
- `origin` is the Edgion-maintained fork remote. `origin/main` is a fork mirror
  and must not be treated as proof of the current official upstream state.
- Local `main` tracks `upstream/main`, contains no Edgion commits, and moves
  only by fast-forward.
- `edgion_v3` is the published, consumer-facing fork integration branch. Keep
  it at the last validated head while a synchronization is in progress.
- Perform each synchronization on a temporary branch named
  `sync/upstream-YYYYMMDD-<upstream-short-sha>` or in a disposable worktree.
  Do not resolve conflicts directly on the published fork branch.

The original migration base in the knowledge index is historical provenance.
Do not overwrite it with the newest upstream SHA. Git ancestry, immutable
synchronization tags, and the append-only ledger below record later adoptions.

## Immutable synchronization points

Tag each validated published fork head with an annotated tag of the form:

```text
edgion-v3-sync-YYYYMMDD-upstream-<upstream-short-sha>
```

The annotation must name the full upstream target SHA, resulting fork SHA,
verification record, and Edgion consumer revision when one was tested. Before
the first history-rewriting synchronization, tag the existing published fork
head using the same scheme and its existing upstream base. Push the protective
tag before replacing a published branch history. Never move or reuse a
synchronization tag.

An old fork revision required by a supported consumer must remain reachable
from an immutable remote ref. Prefer an exact `rev` in the Edgion manifest for
released consumers; a branch name or nearby checkout is not a reproducibility
record.

## Preconditions

Before synchronization:

1. Finish or separately preserve every tracked and untracked worktree change.
   The source checkout and any disposable worktree must be clean.
2. Fetch `upstream` without moving `edgion_v3` and identify the exact full SHA
   of `upstream/main` that will be adopted. Do not use an unreviewed moving ref
   as the recorded target.
3. Record the old upstream base, old fork head, new upstream target, current
   sibling Edgion head, and the Pingora revision selected by Edgion's manifest
   and lockfile.
4. Preview the conflict surface with `git merge-tree` or a disposable
   worktree. Read the affected feature contracts and review records before
   resolving conflicts.

Fetching, rebasing, tagging, updating a consumer, publishing, and pushing are
separate operations. This procedure does not itself authorize a push or a
consumer change.

## Synchronization procedure

1. Create the temporary synchronization branch from the current validated fork
   head.
2. Rebase or rebuild the Edgion commits onto the frozen new upstream target in
   feature order. Do not merge a moving `upstream/main` into the published fork
   branch. For a direct rebase, the conceptual range is:

   ```text
   git rebase --onto <new-upstream-sha> <old-upstream-base> <sync-branch>
   ```

3. Resolve conflicts by contract, not by selecting whole sides or comparing
   line counts. Run focused tests after each feature or coherent conflict
   group. If upstream now supplies the feature, retain only the fork-owned
   policy or safety boundary that remains necessary and update its provenance.
   In a highly divergent file, start from the new upstream structure and
   reapply the smallest contract-complete fork delta instead of restoring the
   old fork version. Avoid carrying fork-only formatting, moves, helper copies,
   or compatibility layers that no longer protect an Edgion-owned behavior.
4. Compare the old and rebuilt stacks with `git range-diff`:

   ```text
   git range-diff <old-base>..<old-fork-tag> <new-base>..<sync-branch>
   ```

   Investigate every dropped, duplicated, reordered, or materially changed
   fork commit. A clean textual rebase is not behavioral evidence.
5. Run the complete current
   [verification matrix](../verification/test-matrix.md), then perform the
   required cross-repository checks against the actual Edgion consumer. Record
   exact commits and worktree state with the result.
6. Have the final synchronization diff independently reviewed before moving the
   published branch. Review both upstream changes in fork-owned hot spots and
   the complete range-diff of the fork stack.
7. After all gates pass, create the immutable synchronization tag, update the
   published fork branch, and only then update Edgion to the exact validated
   fork revision. If rewritten history must be published, use
   `--force-with-lease`, never bare `--force`.
8. Append a ledger entry. Do not relabel an earlier verification snapshot as
   current and do not replace the previous ledger row.

Major restructures may use a new versioned integration branch instead of
rewriting `edgion_v3`. Choose that path when old consumers must continue to
resolve a branch, when the feature stack is materially reorganized, or when a
safe lease-protected replacement cannot be coordinated.

## Synchronization ledger

Add one row only after the synchronized fork head and its consumer verification
are complete. Link the detailed test snapshot rather than copying its command
output here.

| Date | Old upstream base | New upstream target | Old fork head/tag | New fork head/tag | Edgion revision | Verification |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-09-01 (baseline audit; no upstream delta) | `09696b5` | `09696b5` | `2e2e674` | `edgion-v3-sync-20260901-upstream-09696b5` | `b9a73a8` | [snapshot](../verification/test-matrix.md#2026-09-01-upstream-baseline-audit-no-upstream-delta) |

The baseline provenance in the [knowledge index](../README.md) and
[feature inventory](../features.md) records the original migration and remains
unchanged. This ledger begins with subsequent periodic synchronization events.

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
  carrying Edgion filter state through them. The H2 parent retains duplex,
  response, cache, reset, allocation, and reuse ownership.
- `pingora-proxy/src/proxy_h2_request_body.rs`: preserve H2 request framing,
  the pinned write future, weak END_STREAM evidence, timeout precedence,
  capacity cancellation, and bounded abandonment as one capability.
- `pingora-proxy/src/response_terminal.rs`: preserve the exactly-once terminal
  latch, filtered-upgrade body tagging, and empty-trailer normalization as one
  private response-pipeline capability.
- `pingora-proxy/src/response_reconciliation.rs`: preserve downstream
  body-forbidden classification, terminal task/framing normalization, and
  response-source cache failure reconciliation as one shared private seam.
- `pingora-proxy/src/response_cache_relay.rs`: preserve pre-downstream-filter
  cacheability, cache/sink emitted-chunk ordering, single EOS migration, and
  upgraded-body tagging as one private response-pipeline capability.
- `pingora-proxy/src/lib.rs`: preserve H1 transfer-coding admission before
  `on_connection_reuse` and all routing phases. Its forced-close policy must be
  applied after every mutable error hook and must directly veto returning a
  reusable downstream session; keepalive metadata alone is not that veto.
  Preserve `Session` relay fields and initialization plus the plan-freeze,
  per-attempt, and retry-loop call sites at their lifecycle boundaries.
- `pingora-proxy/src/request_relay.rs`: preserve request disposition validation,
  safe coercion, the bodyless application contract, per-event relay semantics,
  and the `Session` plan, attempt, and native-retry backing transitions as one
  protocol-neutral policy station.
- `pingora-proxy/src/pump_termination.rs`: preserve typed duplex outcomes, the
  biased join and error priority, and shared termination diagnostics and
  cleanup as one capability.
- `pingora-proxy/src/proxy_common.rs`: preserve upstream hop-by-hop sanitation,
  pump state machines, and custom-reader restoration independently from relay
  and termination policy.

## Resolution rules

- Current main owns dependency versions, release metadata and unrelated
  refactors.
- Prefer adapting Edgion calls to current main helpers over restoring old
  inline loops.
- Preserve current main's security and protocol checks even when the older
  Edgion implementation has a larger conflicting block.
- Compare behavior and tests, not raw line counts. A smaller adapter around a
  current helper is preferable to copying an old function.
- For high-divergence conflicts, inspect the resolved file against the new
  upstream version as a separate diff. Minimize that downstream diff while
  preserving every documented fork contract; a smaller diff is a maintenance
  goal, not permission to weaken behavior or coverage.
- Keep feature commits reviewable and single-purpose; follow-up fixes belong to
  the feature they correct rather than a generic final `fix` commit.
- Do not treat a successful compile, a conflict-free rebase, or an unchanged
  public API as proof that proxy lifecycle behavior survived the sync.

## Consumer cutover

The repository API is only half of a safe rollout. Before switching a consumer
lock or deployment to a new Edgion commit, validate its listener configuration
against the new binary, reject invalid trust-source input, and run the H1/H2
request and response matrices used by that consumer. Update the consumer lock
only after those checks pass; do not infer deployment from this branch's
existence. Confirm after the cutover that the manifest and lockfile both select
the validated fork revision and that no local path patch masks the result.
