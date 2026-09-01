# Body relay semantic-layer refactor

## Status

Completed parent refactor for phases 1-3; the overall relay program reached
phase 4 through the separately owned bounded response-head commit work in
[`response-head-commit-barrier.md`](response-head-commit-barrier.md) with an
independently reviewed design and is now resolved. This record retains
migration and closure history; it no longer owns actionable work.

Origin: body buffering/streaming architecture assessment on 2026-08-30.
Baseline: Pingora `44dbef281584f6ef4412fd44eea07dd56c5ae630` and Edgion
`90fef514df87907a584b17e369f5c4c80af3b900`, both with then-present worktree
changes described in the linked assessment.

## Problem

The gateway's conceptual client/middle/backend split is sound, but body relay
semantics do not have one owner. Request capture/replay, per-chunk processing,
retry/framing selection, response processing, semantic windows, header commit,
cache ordering, and terminal state are spread across core sessions, three
proxy pumps, public hooks, and Edgion request context.

The response side already has a shared semantic pipeline. The request side
still duplicates common processing in H1/H2/custom writers. Advanced
body-aware response policy also lacks an explicit bounded header commit
barrier.

This is not permission to rewrite protocol pumps or change existing behavior in
one patch.

## Record ownership

The implemented phase-1 through phase-3 flow, ownership, and invariant matrix
are canonicalized in
[`architecture/body-relay.md`](../architecture/body-relay.md).

The original rationale, rejected alternatives, target boundary, and migration
history are retained in
[`review/body-relay-architecture-assessment.md`](../review/body-relay-architecture-assessment.md).
The linked Phase 4 record owns its completed implementation and closure
history.

## Completed phase 1

The Pingora worktree based on `82ab02b50cd65204d47c7213c9b93fa74299c94c`
now has a private `request_relay` module used by the H1, H2, and custom upstream
pumps. It owns only:

- `Data(None) -> Complete` normalization;
- the existing H1/H2-only trailer hook and its post-success latch;
- downstream module then application body-hook ordering; and
- typed H1/H2 termination versus custom fail-closed capability.

The extraction deliberately leaves source/replay selection, pipe/capacity
reservation, empty-output suppression, post-filter `Bodyless` validation,
socket I/O, H2 timeout/reset, H1 reuse, custom writer cleanup, retry, and cache
in their existing owners. Subrequest piping is also outside this phase because
it intentionally runs modules without the `ProxyHttp` body-action hook.

Executable evidence:

- request relay unit cases: 14 passed / 1 ignored manual benchmark;
- `pingora-proxy` library: 140 passed / 2 ignored manual benchmarks;
- request-body seam: 61 passed across the H1/H2 four-cell matrix and the
  final-filtered Streamed/CONNECT fail-closed case;
- custom response/body harness: 57 passed, including request upload
  abandonment, writer failure, and reader/writer cleanup;
- H2 reset, stalled-upload, and cache/reuse targets: 8, 4, and 8 passed;
- core early body buffer/replay cases: 29 passed;
- Edgion request-body-filtered library cases: 143 passed;
- Edgion `cargo check -p edgion-gateway --lib`: passed against the local path
  patch and checkout `af83f684249186a25d2edecabab51baa76d60edf`; and
- same-process release comparison: former inline Data-event sequence 55.23
  ns/event and 2.0000 allocations/event, extracted relay 51.32 ns/event and
  2.0000 allocations/event. One timing sample is not a stable performance
  claim; the closure evidence is unchanged allocation count and no observed
  regression.

## Completed phase 2

The selected product boundary is request-scoped body policy with backendRef
body mutation prohibited. Edgion does not move backend selection or its plugin
chain: doing so would couple this refactor to sticky routing, internal jumps,
AI predispatch, retry budgets, load balancing and endpoint accounting. Instead,
the global/Gateway/route request-plugin boundary closes body configuration.
BackendRef request plugins retain their existing timing and may read a snapshot
captured earlier, but their first capture, mutation, observer/stream-handler
installation, request-mirror tap installation and WAF streaming requests are
refused by the runtime Body API.

Pingora replaced `upstream_request_body_disposition` and
`request_retry_allowed` with one synchronous `request_relay_plan` hook. The
plan freezes once after an accepted `proxy_upstream_filter` and before the retry
loop. Core derives the source and locks late H1/H2 replay registration. Retry
is now split as designed:

```text
request-stable structural replay policy
  AND per-attempt backing readiness (live/native/registered/poisoned/truncated)
  AND dynamic error.retry, deadline, budget, and response-commit policy
```

Application code must not declare the source itself. Registered replay versus
live/native buffering and rewind readiness are core/session facts. Effective
wire framing also remains per attempt because it depends on the filtered
request, selected upstream protocol, replay activation, upgrade/CONNECT, and
body facts.

Strictly bodyless requests keep the existing benign `Ordinary` coercion, and
`Bodyless` keeps its tunnel/legacy-version compatibility coercion. `Streamed`
now fails closed before the upstream header write if the final filtered request
is upgrade/CONNECT or the H1 request is below HTTP/1.1. This is an intentional
contract tightening: a frozen length-changing processor must not continue
under ordinary or tunnel framing after an attempt-local rewrite.

`RequestRelayRetryState` now exposes the combined structural/backing fact,
including unsupported custom sessions, native truncation, and registered replay
availability. Dynamic error, deadline, budget and commit decisions remain
outside the frozen plan. Pingora assigns a one-based request attempt identity;
Edgion body observers consume that canonical identity and retain the separate
AI/backend subattempt counter only for product accounting where one Pingora
attempt can internally try more than one AI target. The relay backing gate is
also checked before retry-budget or successor-selection side effects on connect
failure; current AI predispatch settlement still runs so its reservation is
released.

Phase-2 validation includes 14 active relay unit cases, the 61-case request
seam, the 140-pass/2-ignored Pingora library suite, the 57-case custom/response
harness, Edgion compilation, 143 request-body-filtered Edgion cases, 44
retry-filtered Edgion cases, and focused mirror-freeze tests. The full Edgion
library run reached 3293 passed / 2 ignored with one unrelated exact-layout
snapshot failure: the test pins `EdgionHttpContext` to 1104 bytes while the
pre-existing local Pingora path-patched checkout builds it at 1120 bytes; the
separate 1280-byte budget assertion passes, and this phase changes no context
field.

## Completed phase 3

The ownership review rejected storing Edgion processors in Pingora's
`ResponsePipelineState` at this stage: that state ends before downstream write
failure classification and universal logging, so its `Drop` cannot distinguish
ordinary pump exit from callback cancellation without broad H1/H2/custom
outcome plumbing.

Instead, Edgion now freezes the final response's AI, semantic, ordinary body,
and trailer processor set into one request-local `ResponseProcessorDriver`
after the response plugin onion and framing repair. Body and trailer callbacks
borrow separate execution leases from that driver. A normally completed lease
keeps the processor instances for later chunks and logging; cancellation drops
only the active group, synchronously and idempotently calling
`release_inflight`. The driver handle remains reachable from the context, so
logging can perform error-source-aware semantic/trailer fallback before shared
ExtProc and Guardrail summaries are emitted.

The current processor slot moved from request context into the callback-local
session adapter. Terminal arbitration itself remains request-scoped and
first-wins. Pingora's public hooks, task/cache ordering, and H1/H2/custom pumps
did not change. The no-processor freeze path allocates nothing.

The durable decision and revisit trigger are recorded in
[`review/response-processor-driver-ownership.md`](../review/response-processor-driver-ownership.md).

Phase-3 validation includes `cargo check -p edgion-gateway --tests`, 24 body
processor cases, 9 trailer cases, 9 logging cases, and the full Edgion library
suite at 3301 passed / 2 ignored.

## Phase 4 handoff

The bounded response-head commit barrier was delivered as a separate change
with an accepted design and resolved implementation record:

- [design](../review/response-head-commit-barrier-design.md)
- [implementation and closure](response-head-commit-barrier.md)

## Required closure evidence

- The explicit request plan remains the only owner of structural source/retry/
  framing selection; contradictory streamed/replayable plans fail closed.
- Existing request-body seam and phase-1 relay tests pass without weakened
  assertions.
- New plan cases cover pass-through, streamed, registered replay, retry attempt
  identity, mutation-derived length, upgrade/CONNECT, and unsupported custom
  combinations.
- H1 connection reuse, H2 stream-only reset, and custom reader/writer cleanup
  remain explicit and tested.
- The pass-through benchmark continues to show no new per-chunk allocation and
  no material throughput regression under the maintained comparison method.
- `../Edgion` builds against the local fork and its streamed request, full
  capture/replay, mutation/framing, WAF, observer, and mirror tests pass.
- The assessment is updated if implementation evidence changes its boundary or
  migration order.

Phase 4 is closed. Its concrete API and acceptance evidence live in the linked
feature, design, and closure records; this historical parent record carries no
remaining action.

## Revisit triggers

- a fourth upstream transport;
- another fork hook that changes request retry or framing independently;
- an Edgion feature that needs bounded response-prefix inspection before final
  header commit;
- response processor ownership/cancellation bugs; or
- evidence that the extraction cannot retain the pass-through fast path.
