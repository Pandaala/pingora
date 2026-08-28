# The default response-body path now creates an async-trait future per chunk

Status: open

Severity: low / performance investigation

Fork baseline: `upstream/main...edgion_v3`; introduced by `600ac49`.

## Problem

Upstream Pingora defines `ProxyHttp::upstream_response_body_filter` as a
synchronous hook. The fork changes it to `async fn` at
`pingora-proxy/src/proxy_trait.rs:708-720`, and awaits it for every
`Body`/`UpgradedBody` task at `pingora-proxy/src/lib.rs:536-560`.

`ProxyHttp` uses `async_trait`, so the API-level implementation constructs a
boxed future for each call. That path is taken even when the application uses
the default no-op body filter returning `Ok(None)`.

This is a universal response-body hot-path change: small upstream chunks and
high-throughput streaming responses maximize the call rate. The fork itself
documents `upstream_request_body_disposition` as synchronous specifically to
avoid async-trait per-call boxing, so this cost model is already relevant to
the design.

## Impact

The exact throughput impact has not yet been established in an end-to-end
benchmark, so this should not be treated as a correctness bug. The certain API
change is one boxed/dynamically-polled future per response body event, including
the default filter. Potential effects are allocator traffic, extra indirection,
and reduced small-chunk throughput.

The manual END_STREAM watcher microbenchmark did not show a material scanner
cost (roughly 6-10 ns per frame in its synthetic release benchmark), making
this per-chunk hook a more appropriate place for the next performance
measurement.

## Recommended investigation

Add an allocation and throughput benchmark for:

- upstream/main's synchronous no-op hook;
- the fork's default async no-op hook;
- an overridden async hook that actually yields;
- large chunks versus many small chunks.

Measure allocations per chunk and requests/bytes per second with and without
LTO. Use realistic H1 and multiplexed H2 response streams.

If the default-path regression is material, add a synchronous capability/fast
path that bypasses the async hook when unused, or split the existing sync hook
from an explicitly enabled async extension. Preserve the async feature for
applications that genuinely need to await without charging every default
response chunk.
