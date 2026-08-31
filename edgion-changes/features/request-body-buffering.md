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

- `pingora-core/src/protocols/http/request_body_buffer.rs`: buffer contract and built-in implementation.
- `pingora-core/src/protocols/http/server.rs`: protocol-neutral session API.
- `pingora-core/src/protocols/http/v1/server.rs`: capture, replay and lifecycle.
- `pingora-core/src/protocols/http/v2/server.rs`: H2 capture, replay and lifecycle.
- `pingora-proxy/src/proxy_h1.rs` and `proxy_h2.rs`: rewind before each attempt.

## Tests

- Core unit tests cover cancellation, bounded replay, release and rejection.
- `pingora-proxy/tests/test_request_body_seam.rs` exercises retry and transport
  behavior across H1/H2 combinations.
