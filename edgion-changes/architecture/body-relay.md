# Body relay current architecture

## Status and scope

This is the canonical description of the body-relay architecture implemented
by the Edgion Pingora fork and its `../Edgion` consumer after phases 1-4 of the
relay refactor. It describes current ownership and executable behavior,
including the optional bounded response-head commit barrier.

Pingora `af9e1ac057c6` contains the generic phase 1-4 relay and response-head
barrier mechanisms. Edgion `feature-08-30` at `f31d0169da97` contains the
production consumer integration and selects the full Pingora commit from the
fork's `edgion_v3` git source. The current Edgion worktree adds configuration-
to-wire regression coverage and a shared test-only global-state lock; those
uncommitted test changes do not alter the production architecture described
here.

The architecture has two directional lanes rather than one universal relay:

- the **request relay** selects a request-scoped plan and applies one shared
  semantic event sequence before a protocol writer; and
- the **response relay** applies one shared `HttpTask` sequence, while Edgion's
  request-local processor driver owns product processors across body, trailer,
  cancellation, and logging callbacks.

Request and response share vocabulary—events, retention, terminal evidence,
and fail-closed boundaries—but not one state machine. Their retry, cache,
framing, and completion rules are materially different.

| Phase | State | Delivered boundary |
| --- | --- | --- |
| 1. Request event extraction | Implemented | One protocol-neutral semantic event sequence used by H1/H2/custom request pumps |
| 2. Request relay plan | Implemented | One request-scoped policy/source freeze, canonical attempt identity, backing-aware retry gate, per-attempt framing |
| 3. Response processor ownership | Implemented | One Edgion request-local driver with body/trailer execution leases and durable logging handle |
| 4. Response-head commit barrier | Implemented | Default-Immediate, explicitly claimed bounded Hold with precommit Release/Replace/Fail, cache bypass, writer claim, and H1/H2 cleanup |

## Three-stage system model

```text
client endpoint                 semantic station                 backend endpoint
(socket + H1/H2 codec)   <->   request lane / response lane  <-> (socket + H1/H2/custom codec)
                                  |                 |
                                  |                 +-- Edgion response processor driver
                                  +-- Edgion request body policy and storage
```

The apparent middle layer is deliberately split into three implementation
owners:

| Owner | Responsibility | Explicitly does not own |
| --- | --- | --- |
| `pingora-core` protocol sessions | Parse/serialize, framing primitives, flow control, H2 reset and END_STREAM evidence, H1 connection state, request capture/replay mechanics | Product policy, retry choice, AI semantics |
| `pingora-proxy` exchange coordinators and relays | Duplex scheduling, source selection, request plan freeze, retry gate, shared request-event and response-task sequencing, entity-cache ordering | Edgion storage limits, AI/Guardrail/ExtProc policy, protocol codec internals |
| `../Edgion/edgion-gateway` | Body API policy, request storage/mutation, mirror/WAF/observer composition, response processor composition, semantic windows, terminal product state, access-log finalization | Socket I/O, H1 reuse, H2 flow control/reset, HTTP entity-cache implementation |

This boundary is intentional. A relay returns semantic outcomes to the pump;
the pump still owns actual reads, writes, capacity, cancellation of its peer
future, and connection/stream cleanup.

## Terms and retention taxonomy

The word `cache` is not sufficient to describe the architecture:

| Term | Owner | Purpose | Lifetime |
| --- | --- | --- | --- |
| Registered request replay store | `pingora-core::RequestBodyBuffer`, implemented by Edgion `BodyStorage` | Complete capture, logical mutation, rewind, bounded replay | Request, possibly across attempts |
| Native retry prefix | Downstream transport session when it advertises retry buffering | Retain live bytes already consumed so a retry can replay the prefix | Request, until retry becomes impossible or request ends |
| Semantic response window | Edgion `RawWindow` users | Hold a bounded generation until release, replace, reject, or discard | One processor decision generation |
| `ResponseBodySink` extras | `pingora-proxy` | Bounded processor-generated output for one response pump batch | One pump batch; terminate latch is response-sticky |
| HTTP entity cache | Pingora cache subsystem | Store and serve validated response representations | Independent of request-local retention |

Pass-through does not mean “cache zero bytes in a special semantic buffer.” The
semantic relay itself retains nothing and forwards events, although an eligible
transport may independently activate its native retry prefix capture.

## Request lane

### 1. Product policy assembly

Edgion assembles request-body behavior during the global, Gateway, and route
request-plugin stages. These stages may:

- request a complete body snapshot through `BodyStorage`;
- mutate the logical snapshot;
- install streamed handlers, WAF streaming, observers, or a mirror tap; and
- choose ordinary versus streamed forwarding and structural replayability.

That configuration boundary closes before backend selection and Pingora's
retry loop. BackendRef-scoped plugins retain their existing execution timing
and may read an already-captured snapshot, but they cannot start capture,
mutate the body, or install new streamed consumers. This avoids making relay
policy depend on a backend candidate that can change across attempts.

### 2. One request-scoped plan freeze

After accepted pre-upstream request hooks and before the first upstream
attempt, Pingora calls `ProxyHttp::request_relay_plan` exactly once and freezes:

```text
RequestRelayPlan
  disposition: Ordinary | Bodyless | Streamed
  replay:      Replayable | Never
```

The application declares semantic intent, not the physical source. Core then
derives the immutable source:

```text
registered RequestBodyBuffer present -> RegisteredReplay
otherwise                            -> LiveDownstream
```

Invalid structural combinations fail immediately. In particular,
`Streamed + Replayable` is contradictory. Freezing also closes late H1/H2
request-buffer registration so source identity cannot change between attempts.

Strictly bodyless requests retain the benign compatibility coercion back to
`Ordinary`. A real `Bodyless` contract still fails closed if downstream bytes
arrive after the application promised there would be no upstream body.

There are two distinct request boundaries and they must not be collapsed:

```text
Edgion request-plugin freeze
  closes capture/mutation/stream-consumer registration
          |
          v
Pingora RequestRelayPlan freeze
  closes semantic disposition, structural replayability, and source identity
          |
          v
backend selection / retry attempts / per-attempt framing
```

The first is a product-policy boundary; the second is a transport-execution
boundary. Keeping both explicit prevents a backend retry from silently
changing the logical body or its delivery mode.

### 3. Attempt identity, backing, and retry

Every upstream attempt receives a canonical one-based `RequestAttemptId` from
Pingora. It is distinct from Edgion AI/backend subattempt accounting: one
Pingora attempt can internally evaluate more than one AI target before a wire
attempt is selected.

Structural replay permission and physical backing readiness remain different
facts. The runtime backing state is one of:

```text
Disabled
Unsupported
LiveUnread
NativeCapturing
NativeTruncated
RegisteredReplay
RegisteredUnavailable
```

A next attempt is allowed only when all independent gates agree:

```text
frozen structural replay policy
AND current source/backing is rewindable or still unread
AND Error::retry is true
AND deadline/request timeout permits retry
AND retry/AI budget permits retry
AND no final response has been committed
```

The backing gate is evaluated before retry-budget consumption or successor
selection side effects. Attempt-local AI reservation settlement still runs so
resources from the failed attempt are released.

A registered replay cursor advances only when its bytes have actually been
consumed by the delivery sequence. Cancellation cannot commit speculative
cursor progress or turn a partially delivered snapshot into replay EOF.

### 4. Attempt-local source and framing

The plan is request-stable; wire framing is not. Each H1/H2 attempt first runs
`upstream_request_filter`, activates registered replay when available, then
resolves effective framing from the filtered request, source, protocol,
upgrade/CONNECT state, version, and body facts.

- `Ordinary` preserves normal length/chunked/END_STREAM behavior.
- `Bodyless` removes upstream body framing or closes H2 request DATA, while
  downstream events still reach application hooks for validation and cleanup.
- `Streamed` means the final length is unknown and replay is forbidden.
- H1 below HTTP/1.1 and upgrade/CONNECT cannot safely carry `Streamed`; they
  fail before the upstream header write.
- The custom connector currently accepts only `Ordinary` because it owns an
  opaque framing contract and cannot honor the fork's non-ordinary semantics.

### 5. Shared request-event sequence

The H1, H2, and custom pumps retain source reads and writer operations, but all
ordinary request body events pass through `request_relay_event`:

```text
source read / replay read
  -> pump acquires any required pipe or H2 capacity permit
  -> normalize Data(None) to Complete
  -> H1/H2 trailer hook when trailers are present and not yet claimed
  -> downstream request modules
  -> ProxyHttp::request_body_filter_action
       Edgion: streamed handlers -> WAF -> observers -> mirror
  -> Continue(filtered body + typed event) | Terminate(origin)
  -> protocol-specific empty suppression/bodyless validation/write
```

The trailer latch commits only after the awaited hook succeeds. A cancelled or
retryable failed hook may therefore run on the next attempt; a successful hook
does not run twice merely because replay EOF is observed again.

Permit acquisition intentionally precedes application relay execution. Empty
chunk suppression, validation of a `Bodyless` promise, protocol serialization,
write completion, and permit cancellation remain pump-owned; a hook that
returns successfully is not evidence that bytes reached the backend socket.

Custom request termination fails closed because that pump does not have the
same typed termination cleanup contract as H1/H2.

### 6. Request termination and cleanup

`pingora-proxy/src/pump_termination.rs` owns the protocol-neutral typed duplex
outcomes, biased join policy, and shared termination diagnostics and cleanup.
The H1/H2/custom pumps still own protocol I/O, sibling cancellation effects,
stream or connection cleanup, cache finalization, and reuse decisions.

- Natural downstream completion produces one `Complete` event per delivery
  sequence.
- An early upstream response stops an unfinished live upload and delivers
  `Abandoned` once.
- Retry replay is a new application delivery sequence, but request trailers
  remain request-scoped and at most once.
- Capture/replay cancellation, poison, truncation, or release never becomes a
  fabricated complete replay.
- Accepting a final upstream response releases the request-body delivery view;
  logging may retain summaries, but cannot reopen relay ownership.
- H1 decides downstream and upstream connection reuse separately.
- H2 resets only the affected stream unless connection-level evidence requires
  broader poisoning, and outstanding capacity reservations are cancelled.
- Custom readers/writers are restored or joined by the custom pump even when
  its peer future ends first.

The request relay is the main client-to-upstream proxy path. Pingora subrequest
pipes do not currently enter this plan/event architecture, and legacy
`SavedBody`-style helpers are not a second supported replay contract. Any future
unification must first define subrequest capacity, cancellation, and body-mode
semantics rather than treating the existing relay as implicitly universal.

## Response lane

### 1. Sources and shared pipeline state

The response source is either a live H1/H2/custom upstream or the HTTP entity
cache. All three pumps call the shared `response_task_pipeline`; protocol
differences are selected by the closed `ResponseProtocol` enum.

`ResponsePipelineState` is scoped to one transformation pipeline and owns only
generic response semantics:

- range-body filtering;
- downstream body suppression;
- a filtered terminal header;
- upstream reuse eligibility;
- the bounded `ResponseBodySink`; and
- the exactly-once `TerminalBodyDispatch` latch, implemented in the private
  `pingora-proxy/src/response_terminal.rs` child module.

It deliberately does not own `Session`, application context, cache storage,
protocol writers, or Edgion processor boxes because those lifetimes extend to
different boundaries.

### 2. Live-task semantic order

All live tasks start with protocol-specific upgrade validation where required,
then upstream modules/compression and the matching Edgion upstream hook. Their
later order differs by task kind and is intentionally not flattened:

```text
nonterminal Header
  -> raw upstream header hook
  -> downstream conditional/range selection
  -> final Edgion response_filter/plugin onion (driver freeze on acceptance)
  -> prepare header -> protocol writer

final Header with EOS
  -> raw upstream header hook and pre-downstream cacheability decision
  -> downstream conditional selection
  -> final Edgion response_filter/plugin onion (driver freeze on acceptance)
  -> synthetic TerminalWithoutTrailers body callback
  -> reconcile and admit terminal cache representation
  -> prepare header/body output -> protocol writer

Body / UpgradedBody
  -> ordinary upstream body event hook (including its own EOS bit)
  -> claim terminal evidence without synthesizing a duplicate body callback
  -> admit body plus emitted chunks to entity cache
  -> downstream body/range filtering
  -> drain current body then ResponseBodySink extras
  -> protocol writer

Trailer / Done
  -> typed terminal body callback when still unclaimed
  -> real trailer hook only for Trailer(Some)
  -> admit released bytes and terminal evidence to entity cache
  -> downstream trailer processing / prepared terminal tasks
  -> protocol writer
```

Application-generated sink bytes are admitted to cache in the same order as
the mutated live representation and before downstream-only transformation.
Cache-hit tasks skip `upstream_response_*` hooks. They still run downstream
conditional/range processing and Edgion's final `response_filter` plugin onion,
which may freeze a driver for deterministic cleanup, before writer preparation.

### 3. Terminal evidence

Header EOS, Body EOS, `Trailer`, bare `Done`, `Failed`, decoded EOF,
content-length satisfaction, and wire END_STREAM are distinct facts.

- One clean response receives at most one typed terminal body event.
- A real trailer first emits `TerminalBeforeTrailers`, then runs the awaited
  trailer companions; the following `Done` is inert.
- Final Header EOS and trailer-free `Trailer(None)`/bare `Done` paths emit
  `TerminalWithoutTrailers`; Body EOS is carried by its ordinary `Data` event.
- A `Failed` task claims the terminal latch without synthesizing a clean EOS.
- Upstream compression finalizes before trailers; its footer body traverses
  the ordinary hook/cache/downstream path.
- The accepted upstream `h2` trailer limitation remains: wire END_STREAM alone
  is not sufficient cache-admission evidence.

The shared terminal latch maps task evidence as follows:

| Input evidence | Body dispatch | Public trailer hook | Completion meaning |
| --- | --- | --- | --- |
| Final header with EOS | Synthetic `TerminalWithoutTrailers` after final `response_filter` | None | Clean response with no body/trailers |
| Body with EOS | Ordinary `Data { end_of_stream: true }`; no synthetic terminal callback | None | Clean body completion |
| Real `Trailer(Some)` | `TerminalBeforeTrailers` first | Real trailer map | Clean completion after companions succeed |
| `Trailer(None)` | `TerminalWithoutTrailers` | None | Explicit clean no-trailer completion |
| Bare `Done` | `TerminalWithoutTrailers` only if still unclaimed | None | Decoded clean EOF fallback |
| `Failed` | No clean body terminal event; latch becomes claimed | None | Aborted response |
| Later `Done` after any claimed terminal | None | None | Inert duplicate evidence |

### 4. Sink, mutation, and cache ordering

`ResponseBodySink` distinguishes two output channels:

- mutation/replacement of the current chunk, bounded by the processor; and
- extra generated chunks, bounded per pump batch to 1 MiB and 2048 nonempty
  chunks.

Current bytes precede extra chunks. Overflow rejects without partial sink
mutation. Batch reset restores byte/chunk budgets but termination stays sticky
until consumed by the response lifecycle.

Entity-cache admission observes the post-upstream-processor representation but
pre-downstream-only transformation. A cache hit reproduces that representation
without rerunning upstream application processors. Termination during a
streaming cache readback fails closed because continuing could expose bytes
beyond the application's terminal point or admit a truncated entity.

When cache policy permits background fill after a downstream disconnect, the
upstream/cache side may continue even though the client writer is gone; this
does not transfer writer ownership to the shared pipeline. Conversely, a
queued `Failed` task that is discarded during pump teardown must still abort
cache admission and, for H1, prevent unsafe upstream connection reuse.

### 5. Edgion processor assembly and freeze

Before the final response is accepted, processors live in builder slots:

- AI source/translation/usage processor;
- semantic Guardrail processor;
- ordinary processors in installation order, including ExtProc wrappers; and
- response-trailer companions in response plugin order.

Retry-on-status arbitration happens in the upstream-response header hook. A
discarded response never freezes or consumes the request-owned processor set.
Header callbacks and both framing-repair passes also happen before freeze.

There is exactly one production freeze point: the successful tail of Edgion's
final `response_filter`, after the complete sync/async response plugin onion,
local-reply/denial handling, final stream classification, compression choice,
framing repair, and final response claim.

```text
builder slots in EdgionHttpContext
  -> freeze_response_processor_driver()
  -> Arc<ResponseProcessorDriver>
       Mutex {
         body: AI + semantic + ordinary
         trailers: ordered companions
         body_cancelled latch
         trailer_cancelled latch
       }
```

The empty set flips the inline freeze latch without allocating
`ResponseProcessingState`, `Arc`, or mutex. A body/trailer event that somehow
arrives before the final-header freeze is an invariant violation and fails
closed; it never silently freezes a partial builder.

Cache hits pass through the final response filter and may freeze the driver so
logging owns deterministic cleanup, but cached body/Done tasks do not execute
the upstream processor chain.

### 6. Body execution lease

Every writable body callback obtains a short-lived lease from the request-local
driver. The lease holds the driver mutex across the async processor callbacks,
which solves the former double mutable-borrow problem without moving boxes out
of context slots for each chunk.

Execution order is fixed:

```text
AI -> semantic -> ordinary[0..n]
```

- A terminal winner stops later processors and preempts every non-winner.
- If semantic Guardrail is withholding the current generation, ordinary
  processors do not receive a misleading empty callback.
- Canonical record handoff is same-callback and bounded; stale or oversized
  handoff fails closed.
- Read-only response body plugins run afterward under the existing hook
  contract, including when a writable processor has selected termination.

Calling `lease.complete()` means normal return: the same instances remain in
the driver for the next chunk and final logging. Dropping an incomplete lease
means the callback future itself was cancelled: `release_inflight()` runs once
for the body group, its cancellation latch closes, and later re-entry fails
closed.

### 7. Trailer execution lease

Trailer companions use a separate lease and cancellation latch because body
completion and trailer callback cancellation are different lifecycle facts.

- Companions run serially in installation/response-onion order.
- `None` means a real observation that no trailers were present, not a
  fabricated empty map.
- The trailer lifecycle claim makes real/no-trailer dispatch exactly once.
- Fail-close stops the chain, preempts later companions, and returns an
  internal error before the transport forwards trailers.
- Cancelling a trailer callback releases only trailer inflight resources; it
  does not retroactively cancel the completed body group.

### 8. Logging and finalization

The driver handle remains in `EdgionHttpContext` after processor execution, so
the universal logging boundary can distinguish normal pump errors from callback
cancellation.

```text
pump outcome / ErrorSource
  -> finalize request/downstream status and terminal owner
  -> finalize driver semantic/trailer incomplete state when still open
  -> terminalize shared ExtProc / Guardrail owners
  -> collect one summary line into stage logs
  -> build access log
```

An upstream, downstream/write, internal, or ordinary terminate outcome occurs
after a completed execution lease and therefore never triggers cancellation
release. Logging uses `ErrorSource` when one exists; an application terminate
may instead be classified by the request-scoped first-wins terminal cause and
the existing no-error fallback. Only dropping the callback future triggers
lease cancellation release. A cancelled group is not finalized a second time
by logging. Local replies and response-stage denials that finish before freeze
retain a builder-slot fallback for logging.

The current `ResponseProcessorSlot` is callback-local in
`PingoraSessionAdapter`; first-wins terminal state remains request-scoped.
Therefore cancellation cannot leave a stale “currently executing processor”
identity in the context.

## Protocol capability matrix

| Capability | H1 | H2 | Custom |
| --- | --- | --- | --- |
| Request shared event relay | Yes | Yes | Yes |
| Request trailer hook | Yes | Yes | No |
| Typed request termination | Yes | Yes | Fail closed |
| Non-ordinary request disposition | Yes, subject to version/tunnel checks | Yes, subject to tunnel checks | Fail closed |
| Registered request replay | Yes | Yes | Unsupported/fail closed where selected |
| Response shared task pipeline | Yes | Yes | Yes |
| Response processor termination | Cleanup/reuse policy in H1 pump | Stream reset/cleanup in H2 pump | Fail closed where equivalent cleanup is unavailable |
| Response trailers | H1 framing/HTTP version rules | H2 stream trailers | Connector capability rules |
| Connection reuse owner | H1 pump/session | H2 connection allocator plus stream state | Custom pump/connector |

Additional protocol normalizations are part of the implemented boundary:

- a truly empty request remains `Ordinary`; `Bodyless` is an explicit semantic
  promise, not the absence of currently buffered bytes;
- the benign strictly-bodyless compatibility coercion does not weaken the
  fail-close rule when body bytes later appear;
- custom-upstream `101` plus clean EOF is normalized so the terminal response
  is processed once rather than being mistaken for ordinary informational
  response flow;
- the custom transport keeps its historical conditional-filter gate; the
  shared pipeline records that difference instead of silently changing it; and
- H2 wire END_STREAM, decoded EOF, trailer delivery, application abandonment,
  and replay EOF remain separately tracked evidence.

The matrix is not a target for artificial symmetry. Capability gaps stay
explicit until the affected transport has a real cleanup/framing contract and
regression evidence.

## Ownership and lifetime map

| State | Created/frozen | Owner while active | Normal completion | Cancellation/error fallback |
| --- | --- | --- | --- | --- |
| `RequestRelayPlan` | Once before retry loop | Pingora `Session` | Dropped with request | Invalid/multiple freeze fails request |
| Registered replay state | Request capture registration | Core session + application buffer | Complete/replay done/release | Poisoned or unavailable; retry denied |
| Native retry prefix | First live attempt when eligible | Concrete downstream session | Reused or dropped | Truncation/unsupported denies retry |
| Request semantic event | Each source event | `request_relay_event` then pump | Returned to writer | Hook error/terminate returned to pump |
| `ResponsePipelineState` | One response pump | Pingora shared pipeline/pump | Prepared tasks written | Pump owns error/reuse/cache abort |
| `ResponseProcessorDriver` | Final response filter | Edgion context `Arc` | Finalized/dropped after logging | Group lease releases inflight once |
| Body/trailer lease | Each callback | Driver mutex guard | `complete()` preserves group | Drop closes only active group |
| `ResponseBodySink` batch output | One processed batch | Pipeline state | Drained to cache/downstream tasks | Overflow/error aborts without partial append |
| Semantic `RawWindow` generation | Processor policy decision | Edgion processor/shared owner | release/replace/discard | Bounded fail-close/fallback policy |
| Entity-cache fill | Cacheable response | Pingora cache | Validated completion commits | Failure/termination aborts admission |

## Fail-closed boundaries

| Invalid or unsupported state | Result |
| --- | --- |
| `Streamed + Replayable` request plan | Freeze error before any attempt |
| Late request body source/policy mutation | Edgion Body API refusal |
| Streamed H1 below 1.1 or upgrade/CONNECT | Error before upstream header write |
| Non-ordinary custom request disposition | Internal error |
| Custom request termination without cleanup contract | Internal error |
| Body bytes after a real `Bodyless` promise | Internal error; bytes not silently dropped as success |
| Replay source poisoned/truncated/unavailable | Retry denied |
| Response body/trailer callback before final freeze | Body terminate or trailer internal error |
| Re-entry after callback cancellation | Group-specific fail close |
| Sink extra byte/chunk overflow | Error without partial sink mutation |
| Custom response termination without equivalent cleanup | Fail closed |
| Streaming cache readback termination | Fail closed |
| `Failed` response task | Cache abort; no synthetic clean EOS |

## Fast-path properties

- Default request plan is `Ordinary + Replayable`; the shared event relay does
  not itself retain bytes.
- No response processors means final freeze creates no driver, `Arc`, or mutex.
- Default response body hooks retain their allocation-free ready-future path.
- `ResponseBodySink` allocates/grows only when an application actually emits
  extra chunks.
- Protocol selection uses closed enums and static control flow, not per-chunk
  transport trait-object dispatch.
- Large capture stores and semantic windows remain sparse product features;
  ordinary context construction stays within its existing allocation and size
  budgets.

These are comparative fast-path properties backed by focused allocation and
behavior tests, not a claim that an end-to-end proxied request performs zero
allocations.

## What remains protocol- or product-owned

The following must not be pulled into a generic relay merely to shorten call
graphs:

- socket reads/writes, H1 framing and connection reuse;
- H2 flow control, capacity timeout, reset, GOAWAY, and connection allocation;
- custom reader/writer extraction and restoration;
- entity-cache policy and storage;
- Edgion memory/disk spill limits and logical body mutation;
- AI provider translation, quota, usage, Guardrail/ExtProc policy;
- semantic window byte/handle/event limits; and
- access-log schema and product terminal causes.

Pingora owns generic transport and relay mechanics. Edgion owns product policy
and durable finalization. Cross-repository changes must update both sides only
for the behavior each side owns.

## Bounded response-head commit

The response relay now has an explicit optional precommit stage. The default
path is `Immediate`; an eligible single claimant may instead request a bounded
Hold from the final post-onion response head:

```text
Immediate
or
Hold {
  input/output bytes,
  chunks/events,
  metadata/work,
  absolute deadline
}
  -> Release(original head + ordered body)
  -> Replace(new head + bounded body)
  -> Fail closed
```

This remains a bounded prefix barrier, not permission to buffer an entire
response or unbounded SSE stream. Pingora retains protocol-neutral tasks and
enforces ordering, limits, deadline, cache exclusion, writer handoff, and H1/H2
origin cleanup. Edgion retains its processor driver and owns claimant policy,
semantic windows, dependency calls, replacement presentation, terminal
arbitration, and logging.

Edgion's Guardrail claimant records a pending request-stage claim before cache
admission and activates it only after the semantic processor installs. The
final response onion must still produce an eligible identity-encoded 2xx
canonical SSE response. With `holdFirstWindow` enabled, the first semantic
decision releases the selected response, replaces it with a complete bounded
403, or fails before commit. Release remains upgradeable within the same
callback until the shared pipeline consumes it; afterward the established
streaming response lane continues without head-barrier accumulation.

The implemented contract and source map are in
[the response-head commit barrier feature](../features/response-head-commit-barrier.md).
The accepted rationale and intentionally unsupported combinations remain in
[the design record](../review/response-head-commit-barrier-design.md), while
implementation and closure evidence are tracked in
[the Phase 4 record](../pending-issues/response-head-commit-barrier.md).

Moving an opaque application handle into the broader Pingora lifecycle remains
unjustified: the generic barrier owns retained tasks, while Edgion's driver
continues through downstream outcome and explicit finalization in logging.

## Source map

Pingora:

- `pingora-proxy/src/request_relay.rs` (request plan state, attempts, retry
  backing, policy, and event sequence)
- `pingora-proxy/src/proxy_trait.rs` (`RequestRelayPlan`, retry state, hooks)
- `pingora-proxy/src/lib.rs` (`Session` fields and initialization, plan-freeze
  and per-attempt call sites, retry loop)
- `pingora-proxy/src/proxy_{h1,h2,custom}.rs`
- `pingora-proxy/src/proxy_h2_request_body.rs` (H2 request framing, writes,
  liveness, and abandonment cleanup)
- `pingora-proxy/src/response_pipeline.rs`
- `pingora-proxy/src/response_terminal.rs`
- `pingora-proxy/src/response_body_sink.rs`
- `pingora-proxy/src/proxy_cache.rs`
- `pingora-proxy/src/pump_termination.rs`
- `pingora-proxy/src/proxy_common.rs`
- `pingora-core/src/protocols/http/body_buffer.rs`

Edgion:

- `edgion-gateway/src/ctx.rs`
- `edgion-gateway/src/response_processor_driver.rs`
- `edgion-gateway/src/plugins/runtime/session_adapter.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_request_filter.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_upstream_request_filter.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_response_filter.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_upstream_response_body_filter.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_upstream_response_trailer_filter.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_logging.rs`
- `edgion-gateway/src/request_body/`
- `edgion-gateway/src/sse/window.rs`

## Related records

- [Architecture assessment](../review/body-relay-architecture-assessment.md)
- [Request-body transport contract](../features/request-body-transport.md)
- [Response-body streaming contract](../features/response-body-streaming.md)
- [Response trailer contract](../features/response-trailers.md)
- [Response processor ownership decision](../review/response-processor-driver-ownership.md)
- [Remaining body-relay work](../pending-issues/body-relay-refactor.md)
- [Known upstream limitations](../review/upstream-limitations.md)
