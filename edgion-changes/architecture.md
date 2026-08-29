# Architecture and review map

## Workspace layers

The root Cargo workspace currently contains 22 crates.

| Layer | Crates and responsibility |
| --- | --- |
| Public facade | `pingora/` re-exports selected core, HTTP, proxy, cache, load-balancing, and timeout APIs |
| Service/protocol core | `pingora-core/` owns lifecycle, services, listeners, L4/TLS, H1/H2 sessions, clients/connectors, and protocol abstractions |
| Programmable proxy | `pingora-proxy/` owns `ProxyHttp`, orchestration, retry/cache/filter lifecycle, H1/H2/custom pumps, and subrequests |
| Primitives | `pingora-http`, `pingora-error`, `pingora-header-serde` |
| Runtime/connections | `pingora-runtime`, `pingora-timeout`, `pingora-pool` |
| Cache | `pingora-cache`, `pingora-memory-cache`, `pingora-lru`, `tinyufo` |
| Routing/policy | `pingora-load-balancing`, `pingora-ketama`, `pingora-limits` |
| TLS backends | `pingora-openssl`, `pingora-boringssl`, `pingora-rustls`, `pingora-s2n` |
| Operations | `pingora-prometheus`, `pingora-foundations` |

`pingora/src/lib.rs` is primarily a facade; it is not the main implementation
layer.

## Core implementation map

- `pingora-core/src/server/mod.rs`: `Server`, runtime, service dependencies,
  graceful shutdown, and upgrade lifecycle.
- `pingora-core/src/services/listening.rs`: endpoint accept loop and
  per-connection tasks.
- `pingora-core/src/listeners/` and `protocols/l4/`: socket acceptance,
  connection filters, PROXY protocol, and transport metadata.
- `pingora-core/src/apps/mod.rs`: H1/H2/custom dispatch, H2 stream tasks, and H1
  keepalive loop.
- `pingora-core/src/protocols/http/server.rs`: unified `ServerSession`; concrete
  behavior remains in `v1/server.rs` and `v2/server.rs`.
- `pingora-core/src/connectors/http/` and `protocols/http/v{1,2}/client.rs`:
  upstream acquisition, pooling, sessions, and response decoding.
- `pingora-core/src/upstreams/peer.rs`: peer and transport options.

Large protocol regression suites that need private implementation access live
in behavior-grouped sibling files such as `v1/server_test_*.rs`,
`v2/*_tests*.rs`, and `pingora-proxy/src/*_tests.rs`. Their parent production
module includes them with `#[cfg(test)]` plus `#[path]`, so tests retain parent
privacy access without widening production APIs or creating extra Cargo test
targets. Small tests that directly exercise one private detail remain inline.

## Proxy implementation map

- `pingora-proxy/src/proxy_trait.rs`: public `ProxyHttp` lifecycle contract.
- `pingora-proxy/src/lib.rs`: `HttpProxy`, `Session`, cache short-circuit,
  upstream selection/retry, errors, and pump dispatch.
- `proxy_h1.rs`, `proxy_h2.rs`, `proxy_custom.rs`: protocol-specific duplex
  request/response pumps, transport cancellation, framing, and reuse outcomes.
- `response_pipeline.rs`: shared response-task semantic stages: upstream and
  terminal hooks, cache admission, downstream transforms, sink drain, and
  prepared task batches. `ResponseProtocol` keeps the H1/H2/custom wire and
  upgrade differences explicit without dynamic dispatch.
- `proxy_common.rs`: shared event, completion, retry, and reuse decisions.
- `proxy_cache.rs`: cache lookup/fill/hit interleaved with body processing.
- `response_body_sink.rs`: fork response-body emission/termination surface.

## Request and response flow

```text
Server
  -> listening Service / accept
  -> transport stack (connection filter, PROXY, TLS)
  -> ServerApp H1/H2/custom dispatch
  -> HttpProxy::process_new_http / Session + CTX
  -> early hooks, modules, request_filter
  -> cache lookup or upstream path
  -> upstream_peer + Connector / pool
  -> H1, H2, or custom duplex pump
  -> hooks + cache admission/delivery
  -> failure/logging/finish
  -> stream or connection reuse decision
```

H1 loops requests on a connection when reuse is safe. H2 creates concurrent
stream tasks on a shared connection. Completion, reset, error, and reuse remain
protocol-specific even behind common abstractions.

## Review path matrix

Check each reachable combination:

- downstream H1/H2 and upstream H1/H2/custom;
- live response, cache miss/fill, and cache hit;
- ordinary request, retry/replay, early response, local termination,
  upgrade/CONNECT, and subrequest;
- Header-EOS, Body-EOS, trailers + Done, bare Done, reset, timeout, and failure;
- downstream reuse, upstream H1 reuse, H2 stream cleanup, and H2 connection
  allocation/reuse.

`HttpTask` in `pingora-core/src/protocols/http/mod.rs` is common, but the three
pumps interpret and batch tasks separately. Cache may short-circuit before
upstream work, while cache fill is embedded in response processing. Body and
terminal changes therefore require reading `proxy_cache.rs` too.

## Edgion consumer seam

- `../Edgion/Cargo.toml`: selected fork dependencies and local patches.
- `../Edgion/edgion-gateway/src/runtime/server/listener_builder.rs`: listener,
  h2c/TLS/keepalive, connection-filter, and PROXY setup.
- `../Edgion/edgion-gateway/src/routes/http/proxy_http/mod.rs`:
  `impl ProxyHttp for EdgionHttpProxy`.
- Its sibling `pg_*.rs`: body filters, retry/framing, failures, and local reply.

Review both repositories when public behavior or consumer assumptions change.
Pure internal refactors preserving an executable contract need not load the
consumer tree.

## Fork feature source index

- PROXY: `listeners/l4.rs` -> `listeners/mod.rs` ->
  `protocols/l4/proxy_protocol.rs` -> L4 stream/digest.
- Replay: `protocols/http/body_buffer.rs` -> unified server -> H1/H2 sessions ->
  proxy retry and pumps.
- H2 evidence: `protocols/http/v2/end_stream_watch.rs` -> H2 client -> connector
  -> proxy H2 and cache completion.
- Streaming: `proxy_trait.rs` and `response_body_sink.rs` ->
  `response_pipeline.rs` / `proxy_common.rs` -> all pumps -> `proxy_cache.rs`.

## Progressive review order

1. Read the feature contract and `review/README.md`; search existing decisions.
2. Read the diff and public contract.
3. For consumer-visible seams, inspect Edgion's use and selected revision.
4. Locate the lifecycle phase in `pingora-proxy/src/lib.rs`.
5. Read `proxy_common.rs`, each reachable pump, and cache path.
6. Descend through the core session to concrete protocol/connector/listener.
7. Select tests from [the verification matrix](verification/test-matrix.md).

## Core review invariants

- Transport EOF, declared empty body, abandonment, replay EOF, and trusted
  response completion are different states.
- Downstream/upstream reuse are separate; H2 stream reset is not automatically
  a connection failure.
- A committed final response cannot be followed by retry or a second response.
- Transforms must keep framing, task order, terminal state, and cache metadata
  consistent.
- Cache admission requires validated completion; wire END_STREAM is
  insufficient alone.
- Exactly-once terminal hooks and retry replay must be checked together.
- Local reply/termination must account for unread body, reuse, H1
  desynchronization, H2 reset scope, and error/log policy.
