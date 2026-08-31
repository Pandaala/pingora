# Bounded response-head commit barrier

## Contract

The fork provides an explicit, bounded precommit barrier for applications that
must inspect a response prefix before choosing the final downstream status and
headers. The ordinary path remains `Immediate`; Hold is opt-in, fail-close,
single-claimant, and limited to ordinary H1/H2 origin responses.

The barrier is a response-task relay stage, not whole-response buffering, an
entity-cache representation, or an owner of application processors:

```text
final post-filter response head
  -> Immediate
       -> will_commit -> prepare -> writer
  -> Hold(hard limits + absolute deadline)
       -> retain ordered origin tasks
       -> application body callback receives an armed ResponseBodySink
            Release -> original head + processed prefix
            Replace -> discard origin prefix + one bounded local response
            Fail    -> discard origin prefix + exact non-retryable error
       -> will_commit -> prepare -> writer
```

No final header is prepared, queued, or handed to a writer before the Hold
decision. Release is only permission to start writer handoff; it is not proof
that a header reached the wire.

## Generic Pingora mechanics

`ProxyHttp` exposes five narrow response-head seams:

- `response_head_may_hold` declares a request-stage possibility before cache
  key generation or lookup;
- `response_head_commit_plan` chooses `Immediate` or bounded `Hold` from the
  final post-application response;
- `response_head_will_commit` observes the one selected head immediately
  before downstream preparation and writer handoff;
- `response_head_hold_boundary` maps typed resource/lifecycle boundaries to a
  bounded replacement or an error; and
- `response_head_hold_outcome` receives one content-free terminal outcome and
  aggregate usage report.

`ResponseHeadHoldLimits` independently bounds:

- retained input bytes;
- replacement/output bytes;
- nonempty chunks;
- response tasks/events;
- retained metadata;
- application work units; and
- one absolute wall-clock deadline.

The deadline spans cross-batch retention, idle waits for more origin data, and
awaited body/terminal/trailer callbacks. The pipeline wakes at the deadline; it
does not depend on another origin event. Reservations and usage survive sink
batch resets. Limit overflow is atomic: the candidate that crosses a limit is
not partially retained.

`ResponseBodySink` is the callback-local decision surface. An armed Hold has no
decision yet. `Release`, `Replace`, and `Fail` remain pending until the shared
pipeline consumes them, so a Release may be upgraded to Replace or Fail later
in the same callback. Product drivers can distinguish an armed sink from a
staged decision and must not translate a staged precommit decision into legacy
post-commit termination.

## Cache boundary

A request for which `response_head_may_hold` is true is disabled from entity
cache before cache key generation, lookup, or fill. This early declaration is
intentionally conservative: the application may not know final response
eligibility yet.

The direct full-cache-hit path still invokes the final plan hook defensively.
`Cache + Hold` is `Unsupported` and is resolved through the boundary hook
before cached origin body bytes can be emitted. Custom-upstream Hold is handled
the same way. Immediate cache hits still execute `response_head_will_commit`
before downstream modules and writer handoff.

## Retry, writer, and origin lifecycle

Final-attempt selection closes retry before a body-dependent decision consumes
application state. `response_head_will_commit` closes the remaining
double-writer window even if the later header write fails partially.

A replacement abandons the origin response:

- H1 immediately drops a still-pending origin reader and marks the upstream
  connection non-reusable; and
- H2 resets/cancels only the affected stream while keeping the shared
  connection eligible for sibling and later streams.

Normal completion is unchanged and still waits for the sibling upload pump.
Cancellation of the response pump drops all retained tasks and publishes one
`Cancelled` outcome; it never releases partially processed bytes.

## Edgion Guardrail claimant

Guardrail is the v1 product claimant. Its request-stage lifecycle separates two
facts:

```text
pending claim
  = matching Guardrail may require Hold; sufficient to bypass cache

active claim
  = semantic response processor installed successfully; permits final Hold
```

After the complete response plugin onion and framing repair, an active claim
selects Hold only for a body-bearing, identity-encoded, 2xx canonical SSE
response. Final ineligibility fails closed before commit. The replacement
template is frozen from this final head and retains only selected
gateway-owned security/trace fields; status, content type, cache policy, and
content length are rebuilt for the fixed local JSON body.

`holdFirstWindow` is false by default. When true:

- `failOpen` is rejected during resource validation;
- `headHoldTimeoutMs` supplies the outer absolute precommit deadline;
- the first semantic Pass or semantic Replace stages Release;
- first-window Reject stages one complete 403 JSON replacement;
- dependency, source, bound, timeout, or work failure stages non-retryable
  Fail; and
- if multiple semantic windows are processed in one callback, every callout
  while the sink remains held consumes work budget, and a pending Release can
  still be upgraded to Replace or Fail.

After the pipeline consumes Release, later windows use the established bounded
streaming behavior and cannot change the committed response status.

## Observability

Pingora reports aggregate counters and elapsed hold time without body content.
Guardrail appends one independent, exactly-once head result to its request
summary: outcome, usage, configured limits, and duration. It never logs
semantic text, replacement text, dependency payloads, credentials, or retained
body bytes.

## Unsupported v1 combinations

The following fail before commit instead of degrading silently to Immediate:

- cache-hit/readback or cache-fill Hold;
- custom-upstream Hold;
- informational, 101, Upgrade, CONNECT tunnel, or `UpgradedBody` Hold;
- multiple claimants or merged limits;
- fail-open Hold;
- replacement trailers; and
- reusing a consumed processor set on another origin attempt.

## Verification ownership

Unit coverage pins every independent limit, cross-batch ordering, timeout and
cancellation, source failure, Release/Replace/Fail, same-callback decision
upgrade, direct cache-hit defense, request-stage cache bypass, writer-hook
ordering, and content-free outcomes.

Network tests exercise Release and Replace through both real H1 and H2 upstream
pumps. They also prove that replacement denies H1 reuse while a following H2
request reuses the same shared connection. The manual Immediate-path benchmark
is required because the default path must remain allocation-free.

## Source map

Pingora:

- `pingora-proxy/src/proxy_trait.rs`
- `pingora-proxy/src/response_head_barrier.rs`
- `pingora-proxy/src/response_body_sink.rs`
- `pingora-proxy/src/response_pipeline.rs`
- `pingora-proxy/src/response_terminal.rs`
- `pingora-proxy/src/response_reconciliation.rs`
- `pingora-proxy/src/pump_termination.rs`
- `pingora-proxy/src/proxy_common.rs`
- `pingora-proxy/src/proxy_h1.rs`
- `pingora-proxy/src/proxy_h2.rs`
- `pingora-proxy/src/proxy_cache.rs`

Edgion:

- `edgion-gateway/src/ctx.rs`
- `edgion-gateway/src/plugins/runtime/session_adapter.rs`
- `edgion-gateway/src/plugins/http/guardrail/{plugin,response,attached}.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_response_head.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_response_filter.rs`
- `edgion-gateway/src/routes/http/proxy_http/pg_upstream_response_body_filter.rs`
- `edgion-resources/src/resources/edgion_plugins/plugin_configs/guardrail.rs`

The design rationale and rejected alternatives remain in
[the accepted design](../review/response-head-commit-barrier-design.md). The
implementation/closure history is in
[the Phase 4 record](../pending-issues/response-head-commit-barrier.md).
