# Bounded response-head commit barrier

## Status

Open Phase 4 implementation. Design accepted; dormant Pingora infrastructure
for Hold/Release and absolute H1/H2 deadlines is in progress. Production Hold
remains intentionally unconstructible until the typed boundary, writer-claim,
Edgion claimant, and cross-repository verification slices are complete.
Severity: feature-safety and maintainability. Ownership: cross-repository.

Baseline: Pingora `60ce8e9c4828494973727679cd807cfba1ee5d66` and Edgion
`af83f684249186a25d2edecabab51baa76d60edf` plus its uncommitted Phase 2/3
consumer worktree.

## Canonical records

- [Accepted design](../review/response-head-commit-barrier-design.md)
- [Current Phase 1-3 body relay architecture](../architecture/body-relay.md)
- [Parent body-relay refactor](body-relay-refactor.md)

This file owns Phase 4 action and closure status. The design record owns API,
state-machine, capability, and rejection rationale. The current architecture
must not claim Phase 4 until the required evidence passes.

## v1 scope

Implement a post-onion, explicit, single-claimant, fail-close bounded head
barrier for ordinary origin responses on the H1/H2 upstream pumps. Default
responses remain Immediate. Hold bypasses cache. Custom, upgrades/tunnels,
cache-hit Hold, multiple claimants, and fail-open Hold remain unsupported.

## Work plan

1. Generic plan, limits, replacement, decision, and accounting types.
2. Shared pipeline Hold/Release before downstream header preparation.
3. H1/H2 absolute deadline wakeups and callback timeout.
4. Precommit Replace/Fail and typed origin abandonment.
5. Edgion selected/write-claimed split and post-onion plan.
6. Guardrail first-window claimant, config validation, and cache bypass.
7. Cross-repository test/performance matrix and current-architecture update.

## Implementation progress

- Completed locally: dormant plan/limit shapes, bounded cross-batch retention,
  explicit Release, independent byte/chunk/event/metadata accounting, empty
  writer-batch handling, retry closure at final-attempt selection, and deferred
  downstream body/trailer filtering.
- Completed locally: one absolute Hold deadline in H1/H2 pump waits and awaited
  response body/terminal/trailer callbacks. Timeout drops retained tasks,
  disables H1 upstream reuse, and follows the existing H2 per-stream
  cancellation path without marking the shared connection shut down.
- Completed locally: synchronous plan/will-commit/boundary/outcome hook shapes,
  a shared writer-handoff retry latch, and dormant callback-local
  Release/Replace/Fail decisions. Replace discards the origin prefix, applies
  normal downstream body/header preparation, and forbids upstream reuse; Fail
  preserves the exact application error. These decisions remain test-only
  until typed boundary mapping and origin-abandonment outcomes are complete.
- Still required before production opt-in: typed boundary mapping and outcomes,
  input/output/work accounting, typed early origin abandonment, direct cache
  Immediate plan/will-commit coverage, Edgion selected/write-claim separation,
  Guardrail integration, and the full cross-repository matrix.

These entries describe uncommitted implementation progress, not the current
released architecture. The current architecture remains Phase 1-3 until the
closure criteria below pass.

## Closure criteria

- Every accepted Hold resolves exactly once as Release, Replace, or Fail.
- No held final header reaches preparation, task queueing, or a writer early.
- Every configured limit and the absolute deadline is enforced across batches
  and callback awaits.
- Timeout/cancellation never releases partially processed bytes.
- Hold never enters v1 cache-hit/readback/fill, custom, 101, upgrade, or tunnel
  paths silently.
- H1 abandons origin reuse and H2 resets only the affected stream when needed.
- Retry remains closed from final attempt selection through writer handoff,
  including partial header-write failure.
- Guardrail holds only its first bounded semantic generation and long SSE
  streams do not accumulate barrier state after Release.
- Default Immediate performance retains the existing allocation count and has
  no material benchmark regression.
- Pingora and Edgion focused plus full library suites pass.

## Revisit-only capabilities

- fail-open Hold after cancellation-safe rollback handoff exists;
- multiple claimants after an explicit composition/arbitration contract;
- cache Hold after separate canonical-cache/downstream representations and
  policy-generation-aware cache semantics exist;
- custom/upgrade/tunnel Hold after typed cleanup and framing contracts exist;
  and
- replacement trailers after downstream protocol capability is explicit.
