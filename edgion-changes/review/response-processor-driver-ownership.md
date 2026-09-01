# Response processor driver ownership

## Status

Resolved fork/consumer architecture decision, implemented on 2026-08-30.
This is phase 3 of the body-relay refactor and does not implement a delayed
response-head commit barrier.

The complete implemented request/response flow and ownership map is maintained
in [the body relay architecture](../architecture/body-relay.md). This record
owns the narrower decision rationale for keeping the driver in Edgion.

## Decision

Writable response processor ownership stays in Edgion for this phase. The
final accepted response freezes AI, semantic, ordinary body processors, and
trailer companions into one request-local `ResponseProcessorDriver` after all
response plugins and framing repair. Cache hits may freeze the set for cleanup,
but do not run upstream body processors.

Each async body or trailer callback obtains a group-specific execution lease:

- normal completion disarms the lease and retains the same processor instances
  for later chunks, trailers, and logging;
- callback-future cancellation synchronously calls `release_inflight` once for
  the active group and closes that group against re-entry; and
- logging locks the durable driver handle before ExtProc/Guardrail summary
  collection, preserving ordinary error-source finalization without treating
  every pump error as callback cancellation.

The callback's `ResponseProcessorSlot` is adapter-local. The request context
continues to own terminal first-wins state, canonical semantic handoff,
framing reservations, attached shared owners, and product log state.

## Why the driver is not in Pingora yet

Pingora's shared response task pipeline ends before protocol-specific
downstream writes. Downstream write failure, duplex cancellation, cache-fill
continuation, and universal Edgion logging are classified outside its
`ResponsePipelineState`. If that state alone owned processor boxes, its `Drop`
would either misclassify ordinary errors as callback cancellation or discard
state needed by logging.

Moving an opaque driver into Pingora becomes reasonable only when a later
response lifecycle boundary spans task processing, downstream write outcome,
and explicit handback. That is a revisit trigger, not a prerequisite for the
current ownership cleanup.

## Preserved invariants

- AI, semantic, then ordinary installation order.
- Semantic withholding skips ordinary processors for that callback.
- Terminal claims remain first-wins and losers are preempted once.
- Failed transport outcomes do not synthesize clean EOS.
- Retry-discarded responses do not freeze the processor set; the final attempt
  does so after retry arbitration.
- Trailer companions remain serial, exactly once, and fail-close.
- Read-only body filters still run after writable processing under the existing
  hook contract.
- Cache and H1/H2/custom protocol ordering are unchanged.
- The empty processor path creates no `Arc`, mutex, or per-chunk allocation.

## Evidence

- `cargo check -p edgion-gateway --tests`: passed.
- Response body processor module: 24 passed.
- Response trailer companion module: 9 passed.
- Logging module: 9 passed.
- Full Edgion library suite: 3301 passed, 2 ignored.

## Revisit triggers

- A response-head commit barrier requires application state to survive across
  protocol writes.
- Pingora gains a shared response lifecycle owner that includes write outcome
  and explicit application handback.
- A fourth response transport repeats processor lifecycle plumbing.
- Evidence shows driver locking or the sparse-state size increase materially
  harms the measured fast path.
