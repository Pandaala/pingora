# Early request-body buffering

## Purpose

Applications can register a `RequestBodyBuffer` before consuming the request
body. The downstream session tees body bytes into that buffer, finalizes it at
the real transport end, and can rewind it before an upstream retry. Replay is
bounded and works for both H1 and H2 downstream sessions.

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

## Safety rules

- Cancellation during capture poisons the capture. The consumed transport
  bytes cannot be silently omitted on a later replay.
- Cancellation during `next_chunk` does not advance the replay cursor; only
  `consume` commits progress.
- Replay chunks are bounded. An implementation returning a larger chunk fails
  closed.
- Draining an unread or partially read downstream body discards the registered
  buffer and prevents a later bodyless replay.
- The buffer is released only after replay reaches EOF and a final response
  header commits. Before that point a retry may still need it.
- `request_headers_end_stream` remains a transport fact. Registering a buffer
  may change the effective upstream body, but never rewrites what the client
  placed on the wire.

## Implementation concentration

- `pingora-core/src/protocols/http/body_buffer.rs`: buffer contract and built-in implementation.
- `pingora-core/src/protocols/http/server.rs`: protocol-neutral session API.
- `pingora-core/src/protocols/http/v1/server.rs`: capture, replay and lifecycle.
- `pingora-core/src/protocols/http/v2/server.rs`: H2 capture, replay and lifecycle.
- `pingora-proxy/src/proxy_h1.rs` and `proxy_h2.rs`: rewind before each attempt.

## Tests

- Core unit tests cover cancellation, bounded replay, release and rejection.
- `pingora-proxy/tests/test_request_body_seam.rs` exercises retry and transport
  behavior across H1/H2 combinations.
