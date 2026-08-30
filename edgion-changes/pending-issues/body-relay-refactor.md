# Body relay semantic-layer refactor

## Status

Open architecture/refactor work. Severity: maintainability and feature-safety,
not a presently reproduced wire defect. Ownership: cross-repository, with
generic mechanics in this fork and product policy in `../Edgion`.

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

## Canonical design

The rationale, current flow inventory, target boundary, rejected alternatives,
and full invariant matrix are owned by
[`review/body-relay-architecture-assessment.md`](../review/body-relay-architecture-assessment.md).
Do not copy them here.

## Next action

Prepare phase 1 and phase 2 as a behavior-preserving fork change:

1. add an executable request-event equivalence table covering H1/H2/custom;
2. define the minimal direction-specific relay vocabulary;
3. extract common request module/hook/trailer/terminal/bodyless processing from
   the protocol writers into `RequestRelay`;
4. leave socket I/O, H2 capacity/reset, H1 reuse, custom cleanup, and request
   storage implementations unchanged; and
5. benchmark the ordinary pass-through request path before proposing the next
   phase.

Do not begin the response-head commit barrier in the same change.

## Required closure evidence

- The request relay is used by H1, H2, and custom upstream paths.
- Existing request-body seam tests pass without weakened assertions.
- New equivalence cases cover Data, Complete, Abandoned, trailer delivery,
  terminate, bodyless violation, replay, early response, and hook error.
- H1 connection reuse, H2 stream-only reset, and custom reader/writer cleanup
  remain explicit and tested.
- The pass-through benchmark shows no new per-chunk allocation and no material
  throughput regression under the maintained comparison method.
- `../Edgion` builds against the local fork and its streamed request, full
  capture/replay, mutation/framing, WAF, observer, and mirror tests pass.
- The assessment is updated if implementation evidence changes its boundary or
  migration order.

Closing this first extraction does not close later relay-plan or response-head
barrier work. Split those into linked pending records when their concrete API
and acceptance tests are proposed.

## Revisit triggers

- a fourth upstream transport;
- another fork hook that changes request retry or framing independently;
- an Edgion feature that needs bounded response-prefix inspection before final
  header commit;
- response processor ownership/cancellation bugs; or
- evidence that the extraction cannot retain the pass-through fast path.
