# Body relay architecture assessment

## Status and baseline

Architecture assessment completed on 2026-08-30. The recommendation is an
accepted design direction. Its first behavior-preserving request-event
extraction, request-scoped relay plan, and Edgion response-processor ownership
are now implemented; bounded head commit is not. The canonical current flow,
ownership, and invariant matrix is
[`architecture/body-relay.md`](../architecture/body-relay.md). The remaining actionable work is tracked by
[`pending-issues/body-relay-refactor.md`](../pending-issues/body-relay-refactor.md).

The “Current architecture” sections below preserve the assessment-time
inventory and migration rationale. Where they differ from the canonical current
record, the canonical current record governs.

The source snapshot inspected was:

- Pingora fork `44dbef281584f6ef4412fd44eea07dd56c5ae630` plus the then-present
  documentation worktree changes;
- Edgion `90fef514df87907a584b17e369f5c4c80af3b900` plus the then-present
  request-body storage and policy worktree changes; and
- Edgion's local Cargo patch, which points the fork family at the sibling
  `../pingora` checkout.

Revalidate the consumer before implementation. Neither a dirty sibling checkout
nor a local patch proves what is deployed or eventually locked.

The first extraction was implemented and validated in a Pingora worktree based
on `82ab02b50cd65204d47c7213c9b93fa74299c94c`. Cross-repository validation was
repeated after the Edgion checkout advanced to
`af83f684249186a25d2edecabab51baa76d60edf`:
its local Cargo patch selected the sibling Pingora worktree, while its committed
lock baseline had selected `c50f93c0bf7adee4b1ed10a0696810968f042858`.
These are development and provenance facts, not a deployed-revision claim.

## Decision

Introduce a narrow, protocol-neutral **body relay** layer in
`pingora-proxy`, incrementally. It should sit between the duplex exchange
coordinator and application-owned processing/storage:

```text
downstream endpoint       exchange coordinator        upstream endpoint
(H1/H2/custom socket)  <->  request + response  <->  (H1/H2/custom socket)
                                  |
                           body relay semantics
                     (events, ordering, retention,
                      commit, terminal outcomes)
                                  |
                    application processors and stores
```

This is worth doing because the repository already contains the two difficult
halves of the abstraction:

- request capture/replay has an explicit stateful storage contract; and
- response transformation has a shared `response_pipeline` with typed terminal
  dispatch, bounded emission, cache ordering, and protocol-specific policy.

The missing piece is a coherent boundary and vocabulary across request and
response. The target is not one universal `Relay` trait. Use separate
`RequestRelay` and `ResponseRelay` state machines, sharing only primitives that
have identical semantics. Request and response have materially different legal
states, retry rules, and terminal actions.

Do not move socket reads/writes, H1 connection reuse, H2 flow control/reset,
custom transport cleanup, HTTP entity-cache policy, or Edgion AI semantics into
the relay. Hiding those behind a generic interface would make the architecture
shorter on paper and less auditable in practice.

## Terms

This record uses the following terms to avoid the overloaded word "cache":

- **endpoint**: a protocol implementation that reads or writes H1, H2, or a
  custom transport;
- **exchange coordinator**: the duplex pump that selects between request reads,
  response reads, capacity, cancellation, and errors;
- **body relay**: the semantic stream stage that converts one ordered body event
  into zero or more ordered output events plus control effects;
- **retention**: temporary ownership of body bytes for replay or a bounded
  decision window;
- **request capture store**: the full request body owned by an application
  implementation of `RequestBodyBuffer`;
- **semantic window**: a bounded response prefix or generation that must be
  explicitly released, replaced, or discarded;
- **entity cache**: Pingora's HTTP response cache. It is not a body retention
  strategy.

`BodyRelay` is preferred over `BodyBuffer` or `BodyCache`: pass-through is the
default mode and must not allocate or retain bytes.

## Current architecture

### Outer three-stage model

The proposed client/relay/backend mental model is accurate at the system level:

```text
client socket + codec
        -> gateway exchange and processing
        -> upstream socket + codec
```

The middle stage currently spans three implementation layers:

| Layer | Current owner | Responsibility |
| --- | --- | --- |
| Protocol endpoint | `pingora-core` H1/H2/custom sessions and clients | Parse/serialize, framing, flow control, timeouts, reset, connection state |
| Exchange coordinator | `pingora-proxy` H1/H2/custom pumps | Duplex scheduling, backpressure, retry, early response, cache source/fill, reuse outcomes |
| Application processing | `ProxyHttp` hooks and `../Edgion` | Body policy, inspection, mutation, storage, AI windows, local replies, product limits |

The architectural problem is not the existence of these layers. It is that the
semantic relay part crosses all three without one explicit owner.

### Request flow today

```text
downstream H1/H2 reader
  -> optional RequestBodyBuffer capture during request_filter
  -> Edgion BodyStorage (memory, anonymous spill file, logical snapshot)
  -> request-stage inspection/mutation of the completed snapshot
  -> live source for H1/H2/custom, or replay source for H1/H2 upstream
     (custom upstream currently rejects registered replay)
  -> protocol-specific request pump
  -> downstream modules
  -> ProxyHttp::request_body_filter_action
  -> Edgion streamed handlers -> WAF -> observers -> mirror tee
  -> protocol-specific upstream writer
```

Important current properties:

1. `RegisteredRequestBodyBuffer` already implements the core capture/replay
   state machine: `Capturing -> Ready -> Replaying -> ReplayDone`.
2. H1 and H2 sessions duplicate registration, capture poisoning, lifecycle,
   release, and replay integration around their different transport reads.
3. Edgion's `BodyStorage` correctly owns product storage policy: physical cap,
   memory/disk spill, I/O timeout, immutable snapshots, logical mutations, and
   replay reads.
4. Full capture and streamed processing are different execution models.
   Streamed handlers make retry unavailable and select streamed upstream
   framing; full capture makes replay possible and may retain a mutated length
   for retry framing.
5. Mode consequences now converge in a frozen `RequestRelayPlan`; core derives
   and locks the source, while `upstream_request_filter` still owns legitimate
   per-attempt framing repair for a mutated registered body.
6. Each H1/H2/custom pump independently performs its variant of the common
   semantic sequence: terminal normalization, request trailer dispatch where
   supported, downstream modules, application body action, bodyless validation,
   and writer handoff.

### Response flow today

```text
upstream H1/H2/custom reader or entity-cache source
  -> HttpTask batch
  -> upstream modules/compression
  -> shared response_task_pipeline
       upstream header/body/trailer hooks
       terminal latch
       entity-cache admission
       downstream transforms
       ResponseBodySink drain
       header preparation
  -> protocol-specific downstream writer
```

This side is closer to the target:

1. `response_pipeline.rs` owns the shared semantic ordering for all three
   pumps. `ResponseProtocol` keeps real framing and upgrade differences
   explicit.
2. The response hook plus `ResponseBodySink` support current-chunk mutation,
   bounded additional output, delay, and sticky termination.
3. `TerminalBodyDispatch` guarantees one clean terminal callback across Header
   EOS, Body EOS, Trailer, and Done; failures do not manufacture clean EOS.
4. Entity-cache admission happens before downstream-only transformation and
   shares the same emitted-byte ordering.
5. Edgion freezes AI, semantic guard, ordinary stream processors, and trailer
   companions into one request-local driver after final response-header
   processing. Separate body/trailer execution leases own callback cancellation;
   read-only filters, terminal arbitration, framing reservations, and
   `RawWindow` policy remain outside that ownership primitive.
6. Edgion's `RawWindow` is a sound bounded-retention primitive. It retains
   original slices under independent byte/handle/metadata/event limits and
   requires every generation to resolve as release, replace, or discard.

The shared response semantics still retain explicit capability differences.
For example, response-body termination currently fails closed for a custom
upstream connector rather than pretending it has the H1/H2 cleanup contract.

### Response-header commit today

There is no explicit body-aware response-header commit barrier. A pump drains
whatever `HttpTask`s are immediately ready, processes that batch, then writes
the result. If a body task happens to share the header batch, its processor may
run before the batch is written. If only the header is ready, the header is
written first. Application behavior must not depend on this scheduling
accident.

Current header hooks may await work before forwarding the header, but they
cannot consume a bounded body prefix and then choose whether to commit or
replace the response. Body-phase termination after the header commit can stop
the stream, but it cannot safely change the already-visible status and headers.

## What is already well abstracted

The following parts should be preserved, not replaced for naming symmetry:

- `RequestBodyBuffer` separates core replay mechanics from Edgion storage
  policy.
- `BodyStorage` and `BodyStorageView` separate physical capture from logical
  snapshots and mutations.
- `RawWindow` is policy-neutral bounded retention for semantic response
  decisions.
- `HttpTask` is a useful protocol-neutral response transport envelope.
- `response_task_pipeline` is the correct direction: shared semantics with a
  closed protocol policy enum, not dynamic transport dispatch.
- H1/H2/custom pumps explicitly own their different cancellation, flow-control,
  and reuse behavior.

The refactor should compose these components. Replacing them with a single
`Vec<Bytes>`-style buffer or one application trait would be a regression.

## Current design pressure

### 1. Policy selection is represented by scattered effects

There is no request-scoped plan that says, in one place, "capture and replay",
"stream and do not retry", or "pass through". The selected behavior emerges
from several mutations and monotonic latches. Correctness currently depends on
those sites staying synchronized.

Response behavior has the same issue at a larger scale: processor presence,
body-length changes, semantic guard ownership, terminal ownership, and
framing reservations live in separate fields.

### 2. Request semantic sequencing is still pump-owned

The response shared pipeline removed roughly three copies of the same semantic
transform. Request H1, H2, and custom writers still own their own variants.
Every new terminal or action rule must be checked in three places, including
early response, retry replay, bodyless framing, and abandonment.

### 3. Similar concepts use incompatible contracts

Request uses `RequestBodyEvent + RequestBodyAction`; response uses
`UpstreamResponseBodyEvent + ResponseBodySink + Option<Duration>`; transport
uses `HttpTask` plus booleans; Edgion adds `StreamingResponseEvent`,
`ChunkVerdict`, and another terminal state. The differences are partly real,
but the overlap has no common vocabulary for output order, retention, delay,
and terminal ownership.

### 4. Cancellation ownership leaked into application code (resolved)

Edgion temporarily moves processor sets out of the request context across
await points, then adds `release_inflight` and restore logic to survive
cancellation. Phase 3 replaced the two body/trailer run-set protocols with one
request-local driver and typed execution leases. Processor instances no longer
move through empty context slots for each async body/trailer callback, while
the context retains a durable handle for logging.

### 5. Header/body decisions are not first-class

Advanced response policy needs one explicit rule for whether the final header
is committed immediately or held behind a bounded decision. Current task
batching cannot be that rule.

### 6. "Cache" names hide different lifetimes

Request replay storage, AI semantic windows, sink-emitted chunks, channel
queues, native retry buffers, and HTTP entity cache all retain bytes, but for
different owners and lifetimes. Treating them as one cache abstraction would
lose the distinctions that make retries and cache admission safe.

## Target architecture

### Exchange coordinator remains protocol aware

The H1/H2/custom pumps remain responsible for:

- concurrent request upload and response download;
- channel/capacity reservation and socket backpressure;
- read/write timeouts and client disconnect observation;
- early upstream responses and upload abandonment;
- H1 connection reuse and H2 stream reset/connection health;
- upgrade/CONNECT handoff; and
- converting protocol errors into exchange outcomes.

The coordinator should call the relay with typed events and write the relay's
prepared output. It should not run application modules or interpret retention
policy itself.

### Separate request and response relays

`RequestRelay` should own:

- coordination of the endpoint-provided source: live downstream or completed
  replay where the selected upstream transport supports it;
- one normalized terminal event per body delivery sequence: complete or
  abandoned, without suppressing a retry attempt's replayed completion;
- ordered downstream modules and application processors;
- request trailer callback at most once;
- bodyless/streamed framing-contract validation;
- the semantic result handed to the protocol writer; and
- attempt identity for retry-visible processors.

`ResponseRelay` should evolve from `response_pipeline` and own:

- source identity: live upstream or entity-cache readback;
- header/body/trailer semantic ordering;
- upstream transforms, entity-cache admission, and downstream transforms;
- bounded extra output, delay, and typed termination;
- one terminal ledger; and
- optional bounded header commit control.

Do not force both directions through one event enum. A request can be abandoned
while an upstream response completes normally; a response can terminate before
trailers; request and response local termination have different wire effects.
Shared helpers should include only identical concepts such as emission budget,
attempt number, delay combination, and terminal claim mechanics.

### Plan behavior explicitly

Avoid a single exclusive mode enum. Real behavior composes. A conceptual plan
is:

```rust,ignore
struct RequestRelayPlan {
    processors: RequestProcessorSet,
    framing: RequestFramingPolicy,
    retry: RequestRetryPolicy,
}

struct DerivedRequestRelayState {
    source: RequestSource,           // core fact: Live or RegisteredReplay
    backing: ReplayBackingState,
    attempt: RequestAttemptId,
}

struct ResponseRelayPlan {
    head_commit: HeadCommitPolicy,   // Immediate or bounded barrier
    processors: ResponseProcessorSet,
    cache_tap: ResponseCachePolicy,
}
```

The implemented public plan deliberately contains only application intent;
source, live backing readiness and attempt identity are derived from core facts
and cannot be declared by Edgion. The important property is that retry and
framing consequences converge on this frozen intent plus derived runtime
state, rather than being independently declared by several callbacks.

### Pass-through fast path

The default plan must be recognizable without allocation or dynamic dispatch:

```text
no retention + no processor + immediate commit
    -> reuse input Bytes/HttpTask
    -> no Vec beyond the pump's existing batch
    -> no boxed per-chunk future
```

A body relay that makes the default proxy path pay for advanced AI features is
not acceptable. Maintain the existing response pipeline benchmark and add a
request relay equivalent.

### Full request capture

Full capture is request-only in Edgion and remains application-owned:

```text
live downstream -> capture store -> completed logical snapshot
                                  -> optional mutation
                                  -> replay source per upstream attempt
```

The protocol-neutral request-source adapter in core retains capture/replay state
and cancellation-safe reads; the relay coordinates which valid source feeds an
attempt. The store owns memory, disk, size limits, I/O deadlines, snapshot
consistency, and replay reads. The capture stage and replay stage must remain
distinct: complete-body plugins act on the snapshot, while ordinary request
body processors see the bytes selected for the upstream attempt. An unsupported
transport combination must fail during plan validation, not silently fall back
to a bodyless or live source.

### Bounded streaming window

A window is not full buffering:

```text
input generation -> retain under hard bounds
                 -> await one bounded decision
                 -> release | replace | discard
                 -> next generation
```

The relay should understand only the hold/release contract and backpressure.
Edgion keeps SSE decoding, provider translation, semantic bucket construction,
guardrail calls, and `RawWindow` policy. Every window needs independent hard
bounds for retained bytes, resident bytes/handles, metadata/work, and decision
time.

### Explicit response-head commit barrier

Add this only after the basic relay extraction is stable. The generic shape is:

```text
final upstream header
  -> response plan chooses Immediate or BoundedBarrier
  -> BoundedBarrier retains header + bounded body generations
  -> decision:
       commit original/modified header and release bytes
       replace with a complete local response
       fail before downstream commit
```

Required constraints:

- byte, chunk/event, metadata, and wall-clock bounds;
- deterministic overflow/timeout policy chosen before processing;
- no full-response wait for ordinary downloads or SSE;
- no retry after downstream final-header commit;
- no status/header replacement after commit;
- cache admission must follow the selected representation and validated
  completion, not merely the held prefix; and
- the barrier must not infer completion from wire END_STREAM alone.

This feature cannot be implemented by sleeping in a header hook or by relying
on `HttpTask` batch coalescing.

### Entity cache is a source and tap, not a retention mode

Keep the current semantic order:

```text
live upstream transform
  -> entity-cache admission tap
  -> downstream-only transform
  -> endpoint writer
```

A hit replaces the live source but still enters the downstream portion of the
response relay. Cache fill completion remains governed by validated response
completion and the known trailer limitations. Request capture storage and
semantic windows must never reuse entity-cache admission state.

## Ownership boundary

### Pingora fork

Owns generic mechanics:

- relay event ordering and terminal ledgers;
- request source switching between live capture and replay;
- common H1/H2/custom request processing before their writers;
- response pipeline and bounded output mechanics;
- head commit barrier capability;
- retry/commit/framing safety guards; and
- protocol matrix and performance characterization tests.

### Edgion

Owns product policy:

- when a request needs complete capture or streamed inspection;
- `BodyStorage`, physical limits, spill, snapshots, and mutation operations;
- plugin ordering, failure modes, local reply contents, and diagnostics;
- AI/provider decoding and translation;
- `RawWindow`, semantic guardrails, quota, and response terminal arbitration;
- configuration and per-request plan construction; and
- the policy that proxied upstream responses are never fully buffered.

If a generic relay API cannot express an Edgion requirement without importing
AI/plugin concepts into Pingora, the API boundary is wrong.

## Alternatives considered

### Keep adding hooks and latches

Rejected as the long-term direction. It minimizes each local change but keeps
retry, framing, terminal, and ownership consequences distributed. The recent
shared response pipeline demonstrates that centralizing semantics can preserve
protocol behavior without hiding the transport.

### One universal bidirectional relay trait

Rejected. It either exposes many invalid combinations or hides protocol facts
needed for H1 reuse and H2 reset/flow-control decisions. Direction-specific
state machines with small shared primitives give better type and review
boundaries.

### Move all buffering into Pingora

Rejected. Core cannot choose Edgion's physical cap, spill policy, I/O timeout,
semantic model, or failure response. `RequestBodyBuffer` is already the correct
storage inversion point.

### Implement only inside Edgion

Rejected for generic sequencing. Edgion cannot remove drift between the three
Pingora pumps or safely add a real response-head commit barrier from body hooks
alone. Product processors and stores still remain in Edgion.

### Rewrite the pumps around generic streams

Rejected for the first phases. The pumps encode hard-won protocol behavior for
capacity, reset, early response, cache, upgrade, and reuse. Extract semantic
steps around them first; reassess a deeper rewrite only after the relay has
stable tests and measurements.

## Recommended migration

1. **Freeze behavior and vocabulary.** Add request/response flow tables and
   characterization cases. No API change.
2. **Extract `RequestRelay` (implemented).** Consolidate the common request
   event processing formerly duplicated by H1/H2/custom `send_body` helpers.
   Protocol writes, empty-output suppression, `Bodyless` validation, retry,
   and transport outcomes remain outside it.
3. **Rename/evolve the response seed.** Treat `ResponsePipelineState` and
   `response_task_pipeline` as the initial `ResponseRelay`; avoid a mechanical
   rename unless it clarifies the new module boundary.
4. **Add an explicit relay plan (implemented).** Edgion chose request-scoped
   policy and closes body-affecting APIs after global/Gateway/route request
   plugins; backendRef plugins may read an existing snapshot but cannot change
   body policy. Pingora freezes application intent before the retry loop,
   derives and locks the source, retains per-attempt effective framing, and
   combines structural replay policy with live backing readiness at the final
   retry gate. The two obsolete independent hooks were removed without a
   compatibility facade.
5. **Move processor ownership into the driver (implemented in Edgion).** The
   final response freezes one request-local driver after response plugins and
   framing repair. Body and trailer leases distinguish normal return from
   callback cancellation, and logging retains the durable handle. The driver
   intentionally remains Edgion-owned: moving it into Pingora now would require
   unrelated pump outcome plumbing because downstream writes and logging occur
   outside `ResponsePipelineState`.
6. **Add bounded response-head commit.** Implement only with independent hard
   limits, deterministic failure policy, cache tests, and H1/H2/custom behavior
   defined explicitly.
7. **Delete redundant latches and hooks.** Do this last, after executable
   equivalence and Edgion integration evidence.

Each phase should be separately reviewable and behavior-preserving unless its
record explicitly names a contract change.

## Required invariants and test matrix

The relay work is incomplete until tests cover:

- downstream H1/H2/custom and upstream H1/H2/custom combinations;
- pass-through, complete request capture/replay, streamed request mutation, and
  bodyless requests;
- first attempt, retry, replay storage failure, cancellation during capture,
  cancellation during replay peek, and rewritten length;
- live response, entity-cache miss/fill/hit, downstream disconnect while
  filling, and cache abort;
- Header EOS, Body EOS, trailers plus Done, bare Done, Failed, and application
  termination;
- early upstream response during upload and exactly-once `Abandoned` delivery;
- delayed response head release, replacement, byte/chunk/time overflow, and
  cancellation before commit;
- H1 downstream/upstream reuse, H2 stream reset without connection poisoning,
  and custom reader/writer restoration;
- compression footer before trailers and emitted-byte ordering;
- pass-through allocation/throughput benchmarks for both directions; and
- the accepted upstream H2 trailer limitation without using wire END_STREAM as
  cache-admission proof.

## Final assessment

The abstraction has positive value if it is judged by two outcomes:

1. one place owns the semantic body sequence for each direction; and
2. advanced behavior is selected by an explicit plan while pass-through stays
   cheap.

It has negative value if it merely renames pumps, treats every retained byte as
one kind of cache, or hides transport lifecycle behind dynamic dispatch.

The recommended architecture is therefore a **thin semantic station with two
directional lanes**, not a new all-purpose proxy core.
