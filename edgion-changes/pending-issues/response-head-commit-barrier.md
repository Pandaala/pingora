# Bounded response-head commit barrier

## Status

Resolved Phase 4 implementation and verification record. The generic Pingora
barrier and the default-off Edgion Guardrail claimant are implemented locally;
production opt-in is constructible through `holdFirstWindow: true`. The full
cross-repository protocol/cancellation/performance matrix passed, canonical
architecture and feature records were promoted, and two independent reviews
accepted the corrected implementation. The work remains uncommitted in both
repositories. Severity: feature-safety and maintainability. Ownership:
cross-repository.

Baseline: Pingora `48f603e9def49547d128d1efdfd6dd4c3af44184` and Edgion
`af83f684249186a25d2edecabab51baa76d60edf` plus the uncommitted Phase 1-4
consumer worktrees in both repositories.

## Canonical records

- [Accepted design](../review/response-head-commit-barrier-design.md)
- [Implemented feature contract](../features/response-head-commit-barrier.md)
- [Current Phase 1-4 body relay architecture](../architecture/body-relay.md)
- [Parent body-relay refactor](body-relay-refactor.md)

This file owns Phase 4 closure history and verification status. The feature
record owns the implemented contract; the design record owns rationale and
rejected alternatives.

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
  preserves the exact application error. Public bounded Hold construction is
  available to the verified Edgion claimant.
- Completed locally: independent input/output/work accounting, typed mapping
  for every accepted Hold boundary, content-free exactly-once outcomes,
  fail-only callback/idle timeout handling, and source-failure preservation.
  H1/H2 pumps now return a distinct origin-abandonment result for replacement;
  their shared typed join immediately drops a pending origin reader instead of
  waiting for origin EOS. H1 forbids origin reuse, while H2 cancels only the
  affected stream without shutting down the shared connection. Pump
  cancellation records one `Cancelled` outcome without exposing retained body
  content. Empty replacements preserve Header EOS, and informational, upgrade,
  tunnel-forming, or over-budget replacements fail closed. Independent review
  accepted the corrected lifecycle; focused tests pin both prompt abandonment
  and the unchanged normal-completion sibling wait.
- Completed locally: the direct full-cache-hit path now invokes the plan hook,
  maps `Cache + Hold` through `Unsupported`, and calls will-commit for Immediate
  before downstream header modules and writer handoff. In the sibling Edgion
  checkout, the ordinary response onion now selects a replaceable status and a
  dedicated will-commit hook performs the sole writer claim; direct local
  replies retain their immediate claim. A populated-memory-cache regression
  proves both the Immediate hook order and Unsupported replacement path, and
  proves that replacement never emits the cached origin body.
- Completed locally: Guardrail can explicitly opt into first-semantic-window
  Hold with `holdFirstWindow: true`; the default is false and the
  `holdFirstWindow + failOpen` combination is rejected. A durable request-stage
  claim disables cache before key generation or lookup, while activation waits
  until the semantic processor is installed successfully. The final post-onion
  response must still be a body-bearing identity-encoded 2xx canonical SSE
  response or the request fails closed before commit.
- Completed locally: first-window Pass/Replace stages Release; Reject stages a
  complete bounded 403 JSON replacement; dependency/resource failure stages a
  non-retryable Fail. A Release is only callback-local until the shared
  pipeline consumes it, so a later decision in the same callback may upgrade
  it to Replace or Fail. All precommit policy calls continue to charge the
  Hold work budget, accepted origin output is cleared on precommit replacement,
  and the driver does not translate a staged head decision into legacy stream
  termination. Later callbacks run in the established streaming mode after
  Release is consumed.
- Completed locally: Guardrail emits an independent content-free, exactly-once
  head summary containing outcome, aggregate usage, configured limits, and
  hold duration. The replacement is built from the final post-onion header,
  retains only selected gateway-owned security/trace fields, and rebuilds
  entity metadata for the local JSON body. Independent implementation review
  accepted the corrected same-callback decision and work-accounting state
  machine.
- Completed locally: the cross-repository protocol/cancellation/performance
  matrix passed, canonical architecture and feature inventory were promoted,
  and final independent closure review found no remaining implementation or
  test blocker after documentation status was reconciled.

These entries describe the verified current worktree architecture. They do not
claim that the baseline commits alone or any released revision contains the
feature; both repositories still have uncommitted changes.

## Current local implementation map

The local implementation has one generic mechanism and one product claimant;
it does not introduce a universal application-owned “relay object.”

```text
Edgion request plugin stage
  -> record a pending Guardrail Hold claim
  -> Pingora cache admission asks response_head_may_hold
       -> disable cache before key / lookup / fill
  -> install semantic processor successfully
       -> activate the claim

origin final response
  -> retry/status arbitration
  -> complete Edgion response plugin onion and framing repair
  -> select final candidate + freeze processor driver
  -> Pingora response_head_commit_plan(final post-onion head)
       Immediate (no active claim)
       Hold (active + eligible canonical SSE)
       Fail (active but final response is ineligible)
  -> shared ResponsePipelineState retains head/tasks under hard bounds
  -> first semantic decision through ResponseBodySink
       Release -> will_commit -> normal head/body writer handoff
       Replace -> discard origin prefix -> will_commit -> bounded local 403
       Fail    -> discard origin prefix -> non-retryable error path
```

The state boundaries are intentionally distinct:

- **pending claim** is a cache-admission fact and exists before cache lookup;
- **active claim** is a product-processor installation fact;
- **held head** is a Pingora pipeline fact under absolute resource/deadline
  bounds;
- **pending Release/Replace/Fail** is callback-local and remains upgradeable
  until the pipeline consumes it;
- **selected status** is an Edgion response-onion result;
- **writer claim** occurs only in `response_head_will_commit`, immediately
  before preparation/writer handoff; and
- **wire commit** and **response completion** remain later, separate facts.

Pingora owns plan validation, retention/accounting/deadline enforcement,
writer-handoff ordering, cache mechanics, and H1/H2 source abandonment.
Edgion owns claimant eligibility, Guardrail windows/callouts, terminal
arbitration, replacement presentation, configuration, and durable logging.

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
