# Bounded response-head commit barrier design

## Status and baseline

Accepted Phase 4 implementation design, 2026-08-30. This record owns the
design rationale, v1 capability boundary, state machine, API shape, ordering,
and rejected alternatives. Implementation and closure are tracked in
[`pending-issues/response-head-commit-barrier.md`](../pending-issues/response-head-commit-barrier.md).

The reviewed baseline is:

- Pingora `60ce8e9c4828494973727679cd807cfba1ee5d66`; and
- Edgion `af83f684249186a25d2edecabab51baa76d60edf` plus its uncommitted
  Phase 2/3 consumer worktree.

At design acceptance, the implemented Phase 1-3 architecture remained
canonical in [`architecture/body-relay.md`](../architecture/body-relay.md), and
nothing in this proposal could be promoted before executable evidence existed.
Phase 4 has since passed that gate; the current contract is now owned by
[`features/response-head-commit-barrier.md`](../features/response-head-commit-barrier.md).

## Problem

The final downstream response header is currently prepared and handed to a
protocol writer before a later body processor can inspect enough of the body to
make a policy decision. A later processor may stop the stream, but it cannot
safely change status or headers after commit. Same-batch Header+Body processing
is not a commit contract and does not solve the split-batch case.

The required feature is a bounded semantic prefix barrier:

```text
final post-onion header
  -> Immediate
  or
  -> Hold under independent hard limits
       -> Release original final head + ordered transformed prefix
       -> Replace with one complete bounded local response
       -> Fail before downstream head commit
```

This is not whole-response buffering, an entity-cache mode, a protocol-wire
buffer, or permission to wait indefinitely for a semantic decision.

## v1 decision

Phase 4 v1 is deliberately narrow.

Supported:

- final, non-informational origin responses;
- explicit opt-in after the complete Edgion response plugin onion;
- one request-local claimant;
- fail-close policy;
- H1 and H2 upstream pumps, while the real downstream protocol remains a
  `Session` fact;
- `Release`, complete bounded `Replace`, and precommit `Fail`;
- Header EOS, Body EOS, `Trailer(Some)`, `Trailer(None)`, `Done`, and `Failed`;
- independent input bytes, output bytes, nonempty chunks, task/event,
  metadata, work, and wall-clock bounds; and
- deterministic cleanup, logging, and cache abort/bypass.

Explicitly unsupported in v1:

- cache-hit or streaming cache-readback Hold;
- continuing entity-cache fill while a head is held;
- custom-upstream Hold;
- `101`, Upgrade, successful CONNECT tunnels, or `UpgradedBody` Hold;
- multiple claimants or limit merging;
- fail-open after a Hold callback is cancelled;
- replacement trailers; and
- replaying a processor set across a new upstream attempt.

Unsupported combinations fail before a downstream final header is queued.
They never degrade silently to Immediate.

## Ownership

### Pingora

Pingora owns generic mechanics only:

- plan validation and barrier activation;
- retained task ordering and accounting;
- the absolute deadline;
- writer-handoff and response-attempt latches;
- cache bypass/abort;
- H1/H2 wakeup, reset, reuse, and source-abandon outcomes; and
- pass-through performance and protocol matrix tests.

### Edgion

Edgion owns product policy:

- whether a request may need Hold before cache lookup;
- whether the final post-onion response actually requires Hold;
- claimant uniqueness and Guardrail eligibility;
- semantic `RawWindow`, provider decoding, dependency calls, replacement
  rendering, and product limits;
- response terminal arbitration and diagnostics; and
- processor/trailer finalization and access logging.

The `ResponseProcessorDriver` remains in Edgion. Pingora's barrier owns bytes
and tasks but not product processor boxes. The driver must outlive downstream
write failure and remain available to universal logging.

## Lifecycle facts

The implementation must keep these facts distinct:

```text
response candidate selected
  -> retry closed / processor set frozen
  -> optional Hold active
  -> Release or Replace validated
  -> writer handoff started
  -> downstream final head committed (or partial write failed)
  -> response completed / aborted
```

`Release` is permission to write, not proof of a successful wire commit.
Retry closes when the final response attempt is selected, before body policy
consumes state. Writer handoff closes the remaining double-response window even
if an H1 partial header write fails before `response_written()` is populated.

Edgion already has a candidate status and a write claim. The ordinary final
`response_filter` must stop claiming the writer directly: it selects the final
status and freezes the driver. A new will-commit seam performs the sole write
claim immediately before Pingora releases a final header to preparation/writer
handoff. Direct local replies keep their existing immediate claim.

The will-commit seam is not Hold-specific. Every accepted final header follows
one of these paths exactly once:

```text
shared pipeline Immediate -> will_commit -> prepare/queue/write
shared pipeline Release   -> will_commit -> prepare/queue/write
shared pipeline Replace   -> will_commit -> prepare/queue/write
direct full cache hit Immediate -> will_commit -> module filter/write
direct local reply -> existing direct claim/write (no plan hook)
```

Custom and cache paths still invoke the plan hook for a normal proxied final
header; v1 rejects a returned Hold before writer preparation. The direct full
cache-hit implementation, which does not enter `ResponsePipelineState`, must
call plan and will-commit in `proxy_cache_hit` explicitly. Removing the old
Edgion claim from `response_filter` before all these paths are wired is a STOP.

## Public plan and control shape

The intended additive Pingora API is structurally equivalent to:

```rust,ignore
pub enum ResponseHeadSource {
    Origin,
    Cache,
}

pub enum ResponseHeadCommitPlan {
    Immediate,
    Hold(ResponseHeadHoldLimits),
}

pub struct ResponseHeadHoldLimits {
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_nonempty_chunks: usize,
    pub max_events: usize,
    pub max_metadata_bytes: usize,
    pub max_work_units: u64,
    pub timeout: Duration,
}

pub struct ResponseHeadReplacement {
    pub header: Box<ResponseHeader>,
    pub body: Vec<Bytes>,
}

pub struct ResponseHeadUsage {
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub nonempty_chunks: usize,
    pub events: usize,
    pub metadata_bytes: usize,
    pub work_units: u64,
    pub held_for: Duration,
}

pub enum ResponseHeadBoundary {
    Unsupported,
    InputLimit,
    OutputLimit,
    ChunkLimit,
    EventLimit,
    MetadataLimit,
    WorkLimit,
    Timeout,
    CleanTerminalWithoutDecision,
    SourceFailed,
    ApplicationFail,
    ApplicationTerminate,
}

pub enum ResponseHeadBoundaryAction {
    Replace(ResponseHeadReplacement),
    Fail(BError),
}

pub enum ResponseHeadOutcome {
    Immediate,
    Released,
    Replaced,
    Failed(ResponseHeadBoundary),
    Cancelled,
}
```

`ProxyHttp` gains:

```rust,ignore
fn response_head_commit_plan(
    &self,
    session: &Session,
    source: ResponseHeadSource,
    final_header: &ResponseHeader,
    ctx: &Self::CTX,
) -> Result<ResponseHeadCommitPlan>;

fn response_head_will_commit(
    &self,
    session: &Session,
    chosen_header: &ResponseHeader,
    ctx: &mut Self::CTX,
) -> Result<()>;

fn response_head_hold_boundary(
    &self,
    session: &Session,
    boundary: ResponseHeadBoundary,
    ctx: &mut Self::CTX,
) -> ResponseHeadBoundaryAction;

fn response_head_hold_outcome(
    &self,
    session: &Session,
    outcome: ResponseHeadOutcome,
    usage: ResponseHeadUsage,
    ctx: &mut Self::CTX,
);
```

The default plan is `Immediate`; the plan and will-commit defaults are
synchronous and do not allocate. The default boundary action is a
non-retryable internal error and the default outcome hook is a no-op. The plan
hook runs exactly once after the final `response_filter` succeeds and the
Edgion driver is frozen, but before downstream module header preparation or
task queueing.

The boundary hook is also synchronous: it must remain usable after an async
processor future was cancelled at the absolute deadline. It converts every
generic timeout, limit, unsupported state, unresolved clean terminal, or
terminate-without-decision into either one bounded replacement or the exact
non-retryable `BError` propagated to `fail_to_proxy`. A source `HttpTask::Failed`
bypasses this mapper: Pingora preserves and propagates its original `BError`
including type, source, and context, while the outcome hook receives the typed
`SourceFailed` category. The barrier
claims a boundary once before calling the hook, so callback cancellation,
pump wakeup, and logging cannot resolve it twice. The outcome hook exposes
aggregate counters and terminal state without body content; it is notification,
not another decision point.

`ResponseBodySink` carries callback-local control while Hold is active:

```rust,ignore
sink.response_head_is_held();
sink.reserve_response_head_work(units);
sink.release_response_head();
sink.replace_response_head(replacement);
sink.fail_response_head(error);
```

Its existing per-batch emitted-byte/chunk limits remain separate. A head
decision is not cleared by `reset_batch`. The barrier consumes a decision only
after the complete writable and read-only body callback sequence returns.
Conflicting or repeated terminal decisions fail closed. With the v1 single
claimant, `Release` may be upgraded to `Replace` or `Fail` within the same
callback but is final once consumed by the barrier.

This sequencing gate was enforced while the target APIs were introduced slice
by slice: no public Hold constructor was exposed before H1/H2 idle and callback
deadlines were wired. The completed implementation now exposes bounded Hold to
verified application claimants; a dormant internal test seam by itself would
still not constitute a production capability.

## State machine

`ResponsePipelineState` stores no barrier allocation for Immediate. Hold alone
allocates the retained state:

```text
Immediate
  --final head--> will_commit -> WriterHandoff

Holding
  + retained final header
  + retained ordered tasks
  + input/output/chunk/event/metadata/work counters
  + absolute monotonic deadline
  + response protocol/source facts

Holding --Release--> ReleaseReady --will_commit--> WriterHandoff
Holding --Replace--> ReplaceReady --will_commit--> WriterHandoff
Holding --Fail-----> Failed
Holding --limit----> Failed
Holding --timeout--> Failed
Holding --Failed---> Failed
Holding --disconnect/reset--> Aborted
```

A clean terminal while still Holding is an invariant failure in v1: the
claimant promised a decision and did not produce one. It does not implicitly
release. A transport `Failed` task never invokes a clean-terminal fallback.

## Budget accounting

All arithmetic is checked or saturating and each dimension is independent:

| Budget | Counted material |
| --- | --- |
| Input bytes | Source body bytes presented while Hold is active, before writable mutation |
| Output bytes | Current transformed body plus sink extras retained for downstream |
| Nonempty chunks | Every retained nonempty body/sink chunk |
| Events | Every retained Header, Body, Trailer, Done, or replacement task envelope |
| Metadata | Final/replacement header and retained trailer names/values plus a fixed per-field charge |
| Work | One core unit per processed event plus explicit application reservations before bounded expensive work |
| Time | One absolute deadline from Hold activation through decision consumption |

An oversized task is checked before insertion into retained storage. A limit
failure never temporarily exceeds the configured retained bound. Replacement
material is charged before the origin prefix is discarded so an invalid
replacement cannot erase the only valid diagnostic path and then overflow.

The Edgion `RawWindow` continues to enforce resident bytes, handles, ranges,
lines, semantic events, buckets, fragments, snapshots, overlaps, and semantic
replacement limits. Those product limits complement rather than replace the
generic barrier budgets.

## Deadline semantics

Checking the clock only when another body task arrives is insufficient. The
same absolute Tokio deadline must cover:

- waiting for the first body task;
- waiting between tasks;
- an awaited writable body callback;
- terminal-body processing; and
- an awaited trailer companion.

H1 and H2 duplex selects therefore need a barrier deadline branch, and the
shared pipeline must wrap callback awaits with `timeout_at` while Holding.
Callback timeout cancels the Phase 3 execution lease and releases inflight
product resources exactly once. Because the current task may have been partly
processed and the processor window is released on cancellation, timeout can
only Fail in v1; it cannot fail-open Release.

Synchronous CPU cannot be preempted by Tokio. A processor must reserve bounded
work before performing expensive synchronous work, and outbound calls retain
their own dependency timeouts inside the overall head deadline.

## Task ordering

### Header

- Interim 1xx stays Immediate and does not create a plan.
- Final Header runs upstream processing, downstream conditional/range choice,
  the complete final response onion, driver freeze, then plan selection.
- Hold stores the final semantic header before downstream module preparation.
- Header EOS still executes its synthetic `TerminalWithoutTrailers` callback
  in the same pipeline call and must resolve or fail there.

### Body

```text
source task
  -> upstream modules/compression
  -> writable processors and terminal evidence
  -> read-only upstream body observers
  -> consume head decision
  -> downstream range transformation
  -> retain current output + sink extras
  -> on Release: downstream-only filters, prepare header, writer handoff
```

The body that triggers Release remains part of the released prefix. Same-batch
and split-batch Header/Body input must produce identical output.

### Trailer and Done

Compression footer body precedes `TerminalBeforeTrailers`. A real trailer
companion must succeed before a decision produced by that same
`TerminalBeforeTrailers` task is consumed; otherwise its fail-close would arrive
too late. A head released by an earlier body decision is already committed and
does not wait for a possible future trailer—doing so would become whole-response
buffering. Release preserves Header -> body/footer -> Trailer. Replace discards
origin trailers. A following duplicate `Done` remains inert.

### Failed

`Failed` produces no synthetic clean EOS. It discards the held prefix, aborts
cache state, marks the origin non-reusable, and enters the normal precommit
error renderer when the downstream is still available.

## Replace and Fail

A v1 replacement is one complete, bounded, trailer-free local response:

- origin head/body/trailer tasks are discarded;
- cache admission is aborted;
- range and suppression state are reset;
- the Edgion response onion is not rerun;
- body-forbidden and framing normalization are applied to the replacement;
- downstream header/body modules run exactly once;
- an empty body uses Header EOS; otherwise the final replacement Body owns EOS;
- origin H1 is non-reusable; an H2 origin stream is cancelled without poisoning
  the multiplexed connection; and
- downstream reuse remains a fact of its actual protocol and request state.

Replace is a clean downstream completion plus origin-source abandonment, not
`ResponseBodySink::terminate()`. Fail returns an error before writer handoff so
`fail_to_proxy` may render the product error; it must not write after a detected
downstream cancellation.

For the v1 Guardrail claimant, a first-window policy Reject changes behavior
only while the head is held: it stages `Replace`, claims/preempts the product
terminal owner, and closes processor state without setting
`ResponseBodySink::terminate()`. The driver recognizes that staged head
decision after the full callback chain and must not enter the ordinary
post-commit terminate path. After Release, later Guardrail rejection retains
the existing streamed terminal-event behavior because the head can no longer
be replaced.

The claimant mapping is fixed:

| First-window result while Holding | Head action |
| --- | --- |
| Pass | `Release` with the released original prefix |
| Semantic Replace | `Release` with the already transformed prefix |
| Policy Reject | Complete bounded head `Replace`; no `sink.terminate()` |
| Dependency, invariant, or rollback failure | Typed precommit `Fail` |

`ApplicationTerminate` while Holding without one of these head actions is a
typed boundary failure. An explicit `fail_response_head(BError)` preserves the
application error for `fail_to_proxy` and records `ApplicationFail`; it does not
route through the generic boundary mapper. After Release, the existing streamed
termination contract applies and cannot retroactively change the head.

## Cache contract

Hold and entity caching are mutually exclusive in v1:

- a request-stage `MayHold` marker bypasses cache lookup and admission before
  an async response plugin can install the claimant;
- `ResponseHeadSource::Cache + Hold` is an invariant error;
- selecting Hold on an origin response aborts any residual admission state
  idempotently;
- Release does not retroactively fill the cache; and
- Replace/Fail abort cache state again defensively.

The full cache-hit fast path runs `response_filter` and writes directly rather
than entering the shared pipeline. It must therefore invoke the plan hook after
`response_filter`, reject `Cache + Hold` through the typed boundary seam, call
will-commit for Immediate, and only then run downstream modules and the writer.

Future Hold+cache support requires separate canonical-cache and downstream-held
representations, policy-generation-aware keys, and an explicit cache-hit
processor contract. It is not part of Phase 4 v1. Wire END_STREAM alone remains
insufficient cache-completion evidence.

## Protocol behavior

### H1

No final head is queued during Hold. Selecting Hold closes retry for the chosen
attempt. Release/Replace starts writer handoff before the first task write; a
partial header write failure cannot reopen retry. Abandoning origin bytes makes
the upstream H1 connection non-reusable. Existing downstream body completion
and keepalive rules remain authoritative.

### H2

The barrier still owns semantic tasks, not encoded DATA frames or capacity.
Abandonment resets only the affected upstream stream with `CANCEL`; it does not
mark the shared connection shutdown. Downstream H2 flow control begins only
after Release/Replace reaches the writer.

### Custom and tunnels

Custom-upstream Hold and all upgrade/tunnel Hold combinations are rejected in
v1. Their opaque cleanup, post-101 task routing, and source-abandon contracts
must be specified and tested before capability is enabled.

## Observability

Edgion records one aggregate outcome, never per chunk:

```text
immediate | held-released | held-replaced | held-failed |
input-limit | output-limit | chunk-limit | event-limit |
metadata-limit | work-limit | timeout | cancelled | unsupported
```

Logs include configured limits, observed counters, hold duration, claimant,
and terminal cause without logging retained body content.

## Fast path

The default plan must preserve:

- no barrier `Box`, retained `Vec`, timer, or metadata scan;
- no extra per-chunk trait-object dispatch;
- the current ready-future no-op body hook;
- unchanged task ordering and cache behavior; and
- no additional allocation per response task in the maintained benchmark.

Only an accepted Hold plan pays for barrier state and deadline polling.

## Rejected alternatives

- sleeping in `response_filter` while no body can be pumped;
- relying on Header and Body appearing in one channel batch;
- buffering encoded H1 chunks or H2 frames in codecs;
- using entity-cache storage as a barrier buffer;
- reusing `ResponseBodySink` per-batch limits as cross-batch limits;
- waiting for full response EOS or an unbounded SSE stream;
- treating Edgion `RawWindow` as proof that the header was not written;
- queueing a held head with `send_downstream_proxy_task`;
- treating Release as a completed wire commit;
- using only `response_written()` as the retry/write-handoff latch;
- releasing partially processed bytes after callback timeout;
- allowing a cache hit to wait for an upstream processor hook that never runs;
- trusting wire END_STREAM as cache admission evidence; and
- moving product processor ownership into Pingora's shorter-lived pipeline.

## Implementation STOP conditions

Data-plane wiring must stop rather than weaken the contract if any of these is
true:

- a timeout, limit, unsupported state, source failure, unresolved terminal, or
  terminate event cannot reach Edgion through the typed boundary/outcome seam;
- the direct cache-hit or any Immediate final-header path can write without the
  exactly-once will-commit claim;
- Guardrail first-window Reject still depends on `sink.terminate()` instead of
  a staged bounded replacement;
- trailer correctness would require holding an earlier Release until full EOS;
- Hold is publicly constructible before idle and callback deadlines exist; or
- a retry can start after the selected attempt's processor state consumed body
  input.

## Acceptance matrix

Pingora evidence must cover:

- Immediate equivalence and allocation benchmark;
- same/split-batch Hold->Release;
- Header EOS, Body EOS, Trailer(Some/None), Done, Failed;
- every exact-limit and limit+1 case;
- idle and callback deadlines under paused Tokio time;
- duplicate/conflicting decisions and terminate-without-decision;
- H1 partial write/reuse, H2 stream reset/connection survival;
- cache Hold rejection and admission abort;
- upgrade/custom/bodyless rejection; and
- no origin/replacement double write.

Edgion evidence must cover:

- final post-onion eligibility after late status/content-type mutation;
- one Guardrail claimant and multiple-claimant refusal;
- first-window Pass, semantic Replace, policy Reject, and dependency failure;
- later windows unable to change the committed head;
- selected versus write-claimed retry/error paths;
- fail-open+Hold configuration rejection;
- RawWindow and barrier bounds composing without growth on long streams;
- callback cancellation and logging finalization exactly once;
- request-stage cache bypass; and
- the empty/default context and allocation fast path.

## Delivery slices

1. Add generic types, barrier accounting/state tests, default Immediate, and
   origin Hold->Release across batches.
2. Add absolute deadline wakeups and callback timeout behavior for H1/H2.
3. Add complete Replace/Fail plus typed origin-abandon cleanup.
4. Split Edgion selection/write claim, add post-onion plan and observability.
5. Add explicit fail-close Guardrail first-window opt-in and cache bypass.
6. Run cross-repository protocol, cache, cancellation, logging, and performance
   evidence; update the canonical current architecture only after it passes.
