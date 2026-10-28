# Early request-body buffering

## Purpose

Applications can register a `RequestBodyBuffer` before consuming the request
body. The downstream session tees body bytes into that buffer, finalizes it at
the real transport end, and can rewind it before an upstream retry. Individual
replay chunks are bounded and work for both H1 and H2 downstream sessions.
Total capture size is a separate application policy.

## Public seam

- `RequestBodyBuffer` defines asynchronous `write`, `finish`, `rewind` and
  `next_chunk` operations plus explicit `consume` acknowledgement.
- `InMemoryRequestBodyBuffer` is the built-in implementation.
- `ServerSession::set_request_body_buffer` registers the buffer.
- `begin_request_body_replay` rewinds a completed capture for a new attempt.
- `request_body_buffer_replaying` distinguishes gateway-local replay failures
  from client transport failures.

The buffer is rejected after body consumption starts, for upgrade/CONNECT
shapes that change body semantics, or when Pingora's native retry buffer is
already active.

This contract is specific to the fork-owned `RequestBodyBuffer` seam. It does
not apply to upstream's alpha `pingora_proxy::subrequest::pipe::SavedBody`,
whose infallible conversion to `InputBody` does not preserve whether capture
was complete or truncated. A chained subrequest consumer must check
`SavedBody::is_body_complete()` before that conversion, or avoid saved-body
replay. See the
[recorded upstream limitation](../review/subrequest/incomplete-saved-body-replay.md).

## Bounded pre-forwarding prefix

`Session::capture_request_body_prefix(buffer, limit)` reuses the registered
application store. It reads before upstream selection, writes at most `limit`
bytes, and calls `finish` at that boundary or actual downstream EOF. `finish`
in this mode seals an inspection prefix; it does not assert transport EOF.
`request_body_prefix_transport_complete` reports the independent downstream
fact. `request_body_prefix_active` remains true for the whole request.

The first forwarding attempt rewinds the sealed store, drains its prefix, then
sends any overrun from the boundary-crossing transport chunk, then resumes
live reads. The store must replay the original bytes without modification;
core checks the replayed length. The store receives exact-sized owned slices
so a small inspection prefix cannot retain a large frame's backing allocation.
The overrun is one copied transport suffix, bounded by the maximum legal H2
DATA frame (2^24-1 bytes); H1 chunks are smaller. No complete-body or native
retry cache is created. Actual bytes read include that suffix once, and replay
never increments the transport counter. Applications enforce their physical
body cap against the transport counter both after capture and during live
continuation; their inspection limit is a separate policy.

Prefix requests are structurally non-retryable, including short bodies that
happen to reach real EOF. This conservative first version avoids confusing a
prefix store with complete replay backing. A second activation, registration
after prefix selection, capture after source freeze, or cancelled/failed
capture fails closed. Public body reads cannot bypass a pending prefix. A
final downstream response cannot release an actively replaying store before
its first attempt consumes it. The prefix store is dropped as soon as its EOF
and retained suffix have been delivered, even while live upload continues.

Discarding a prefix clears only its pending-delivery state: both explicit body
drain and final-response release restore the transport completion view. This
allows a rejected, fully received H1 request to retain downstream keepalive.
The sticky prefix retry veto and failed/cancelled capture state are not reset.
An actively replaying prefix remains retained across a final response until
its delivery finishes or the application explicitly abandons it.

Full-capture registrations retain their existing contract. A consumer that
needs a full snapshot must select full capture before prefix selection, rather
than trying to upgrade a sealed prefix after transport reads. `100-continue`
remains application-owned. Custom/subrequest sessions and tunnels reject this
API. The generic mechanism does not own WAF policy or the inspection window.

## Safety rules

- Cancellation during capture poisons the capture. The consumed transport
  bytes cannot be silently omitted on a later replay.
- Cancellation during `next_chunk` does not advance the replay cursor; only
  `consume` commits progress.
- Replay chunks are bounded. An implementation returning a larger chunk fails
  closed.
- `InMemoryRequestBodyBuffer` has no aggregate capture limit. It is a reference
  implementation, not a safe production default for client-controlled bodies.
  Production users need a per-request limit plus an aggregate admission budget
  (and commonly bounded memory with file spill) across concurrent captures.
- Draining an unread or partially read downstream body discards the registered
  buffer and prevents a later bodyless replay.
- Once capture completes, the buffer is released when a final response header
  commits if replay never started, or after replay reaches EOF if it did. Before
  those points a retry or the active attempt may still need it.
- `request_headers_end_stream` remains a transport fact. Registering a buffer
  may change the effective upstream body, but never rewrites what the client
  placed on the wire.
- Buffer operations execute in the request pump. Implementations must bound
  their own I/O latency; the API does not apply an independent timeout around
  `write`, `finish`, `rewind`, `next_chunk`, or `consume`. The upstream peer
  must also have a write timeout while replay is enabled, because downstream
  disconnect is not observed while the pump is serving captured chunks.
- Before early H1 capture, an application receiving `Expect: 100-continue`
  must explicitly send 100 or reject with a final response. If it sends 100,
  it must remove or otherwise handle the forwarded `Expect` header to avoid a
  second informational response. Registering a buffer does not do this work.

## Implementation concentration

- `pingora-core/src/protocols/http/body_buffer.rs`: compatibility facade and
  `FixedBuffer`.
- `pingora-core/src/protocols/http/request_body_buffer.rs`: registered-buffer
  contract, state, and built-in implementation.
- `pingora-core/src/protocols/http/server.rs`: protocol-neutral session API.
- `pingora-core/src/protocols/http/v1/server_request_body_replay.rs` and
  `v2/server_request_body_replay.rs`: registration, freeze, readiness, and
  replay-activation APIs.
- `pingora-core/src/protocols/http/v1/server.rs` and `v2/server.rs`: session
  fields plus transport capture, poison, drain, release, and replay reads.
- `pingora-proxy/src/proxy_h1.rs` and `proxy_h2.rs`: rewind before each attempt.

## Tests

- Core unit tests cover cancellation, bounded replay, release and rejection.
- `v1/server_test_prefix.rs` and `v2/server_test_prefix.rs` cover short/window/overrun/live continuation, exact bytes, and physical accounting.
- `pingora-proxy/tests/test_request_body_seam.rs` exercises retry and transport
  behavior across H1/H2 combinations.

The `prefix_verdict_before_forwarding` wire matrix covers all H1/H2 downstream
and upstream combinations: denied prefixes establish no upstream connection;
allowed short, exact-window, overrun and continued uploads arrive once with no
native retry cache. The relay unit test keeps retry disabled after delivery.
