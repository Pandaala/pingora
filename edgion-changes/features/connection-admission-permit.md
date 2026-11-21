# Connection admission permit

## Contract

With the `connection_filter` feature, `ConnectionFilter` has a defaulted
`admit(addr: Option<&SocketAddr>) -> ConnectionAdmission`:

- `ConnectionAdmission::Reject` drops the accepted socket inside
  `ListenerEndpoint::accept`, which continues with the next socket.
- `ConnectionAdmission::Accept(None)` accepts without a permit.
- `ConnectionAdmission::Accept(Some(ConnectionPermit))` accepts and stores the
  opaque permit on the accepted l4 `Stream` (private field
  `admission_permit`). Dropping the stream releases the permit exactly once.
  This covers socket-setting and PROXY parse failures as well as normal close.
  If an offloaded TLS handshake still owns the stream when the listener times
  out, all TLS backends request cancellation when the wait is dropped. The
  permit is released when the task actually drops the stream, which can be
  delayed by synchronous handshake work.
  See [TLS offload cancellation](downstream-tls-offload.md).

  This list follows from ownership: the field is private, only
  `ListenerEndpoint::accept` sets it, and every later layer (PROXY `apply`, TLS
  wrapping, the boxed application IO) takes the stream by value or `&mut`. The
  tests below cover the l4 accept and the PROXY handshake (success and
  failure); the later paths are not tested individually. A refactor that
  rebuilds a `Stream` from its raw socket would silently drop the permit early,
  because every `From` constructor starts with no permit.

`admit` is awaited inline by the task that accepted the connection. With the
default `listener_tasks_per_fd = 1`, a pending filter delays the next accept
on that socket. With multiple accept tasks, the others can continue accepting
connections while the filter is pending. The
`admit` future may also be cancelled when the listener shuts down, so a filter
must hold any reservation taken before an `.await` in a value that releases on
drop.

The permit lives as long as the socket, not as long as a request: an idle H1
keep-alive connection or an idle H2 connection still holds it, so a connection
cap built on permits also bounds idle connections until their idle timeout. The
permit's `Drop` may run on any thread, including a TLS offload thread, so it
must be cheap, non-blocking and must not panic.

The default `admit` returns `Accept(None)` when `should_accept` is true and
`Reject` otherwise, so filters that implement only `should_accept` keep their
decisions. `ListenerEndpoint::accept` now consults `admit` for every accepted
stream with the kernel peer address before PROXY parsing. It passes `None`
when the socket digest, peer address, or inet representation is unavailable.
The original loop skipped filtering when the digest or peer was missing. A
known Unix peer already reached the filter with `None`.

`ConnectionPermit::new(value)` wraps any `Send + Sync + 'static` value; consumers
implement `Drop` on the wrapped value to observe the end of the connection. The fork never
inspects the value. `Stream::set_admission_permit` is `pub(crate)`.

## Upstream ownership and the 0.9.1 extension

At official main `3f87e92`, `TransportStack::accept` attaches a listener tracer
for connect/disconnect notifications. Then `TransportStack::admit` reserves
endpoint capacity. The L4 stream owns the private connection permit until
close, while the uninitialized stream owns a separate reservation for the
pending handshake. The tracer has no admission result.
[Endpoint limits](https://github.com/cloudflare/pingora/commit/5104a3e) and
[listener tracing](https://github.com/cloudflare/pingora/commit/4a9d66d).

The 0.9.1 fork keeps the existing L4 filter call site and the same stream
ownership pattern. The extension lets the filter supply its own guard there.
When adopting upstream endpoint limits, keep each configured quota under one
owner and preserve both guards when both policies apply. Replacing a guard
already attached to the stream would release its quota early.

## Consumer

Edgion's listener filter (`edgion-gateway/src/runtime/server/downstream_connections.rs`,
`ListenerAdmissionFilter::admit`) returns a permit holding its process and
listener connection slots. It first reserves a slot under the process-wide
`server.max_downstream_connections` cap, then takes the listener slot and passes
the resulting count (including this connection) to the custom stream plugin
chain for `ConnectionLimit`. Denial or cancellation drops the local slots.
Success moves them together into the stream-owned guard.

## Tests

- `listeners::connection_filter::tests::test_default_admit_keeps_should_accept_decision_without_permit`
- `listeners::connection_filter::tests::test_permit_drops_wrapped_value_once`
- `listeners::l4::test::test_connection_filter_permit_lives_with_accepted_stream`
  (loopback: permit attached to the accepted stream, released when it drops,
  rejected stream never returned)
- `listeners::l4::test::test_connection_filter_admits_non_inet_peer_with_none`
  (unbound UDS client: admitted with `None`, permit released with the stream)
- `listeners::test::admission_permit::test_permit_survives_proxy_handshake_until_io_drops`
- `listeners::test::admission_permit::test_failed_proxy_handshake_releases_permit`

Upstream-main task 01, verified 2026-11-21 on fork `e5f4287` with the admission
patch and Edgion `e5c78eea5` using the local Cargo path override:

- `cargo test --locked -p pingora-core --features connection_filter --lib listeners::`: 29 passed.
- `cargo check --locked -p pingora-core --no-default-features`: passed.
- `cargo test --locked -p edgion-gateway --lib downstream_connections::`: 16 passed,
  including denial, cancellation during the filter chain, and retention of both
  slots until permit drop.
