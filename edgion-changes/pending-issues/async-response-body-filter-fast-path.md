# Avoid async-trait allocation on the default response-body path

Status: open; split from review finding 007 after scope gate.

## Problem

The Edgion fork changed `ProxyHttp::upstream_response_body_filter` from a
synchronous upstream hook to an `async_trait` method. Every live upstream body
event therefore creates and dynamically polls a boxed future, including the
default no-op implementation. Small-chunk streaming responses maximize this
fork-owned hot-path cost.

The focused `pingora-proxy/benches/response_body_filter.rs` microbenchmark
compares the upstream-style synchronous no-op, the exact fork default hook,
and an async override that yields. It reports elapsed time, allocations, and
allocated bytes per hook call for 1 KiB and 64 KiB chunks.

## Required follow-up

1. Run the microbenchmark in representative release builds, with and without
   LTO, and record results for every supported target architecture.
2. Add end-to-end H1 and multiplexed H2 throughput measurements using both
   large chunks and many small chunks. The microbenchmark alone is not a proxy
   throughput result.
3. If the default-path regression is material, design a compatibility-safe
   synchronous capability or fast path. Existing external async overrides must
   not be silently skipped after an upgrade.
4. Apply the chosen design consistently to ordinary body events and synthetic
   terminal dispatch in the H1, H2, and custom-connector pumps.
5. Preserve the asynchronous extension for filters that genuinely need to
   yield, including their delay, bounded emission, and termination semantics.

## Benchmark commands

```text
cargo bench -p pingora-proxy --bench response_body_filter
CARGO_PROFILE_BENCH_LTO=true cargo bench -p pingora-proxy --bench response_body_filter
```

Use `PINGORA_BENCH_ITERS` and `PINGORA_BENCH_YIELD_ITERS` to change the default
iteration counts. Run on an otherwise idle host; allocation counts are process
global, so unrelated allocator activity would contaminate them.

## Initial local result

Measured on Apple M4 / arm64 with Rust 1.96.1. These are synthetic hook-call
costs, not end-to-end proxy throughput:

| Build | Hook | Allocation per call | Allocated bytes per call | Approx. time per call |
|---|---|---:|---:|---:|
| bench, no LTO | synchronous no-op | 0 | 0 | 0.3-0.6 ns |
| bench, no LTO | default async hook | 1 | 16 | 10-20 ns |
| bench, no LTO | yielding async hook | 1 | 32 | 13.3 us |
| bench, LTO | synchronous no-op | 0 | 0 | 0.25 ns |
| bench, LTO | default async hook | 1 | 16 | 10-16 ns |
| bench, LTO | yielding async hook | 1 | 32 | 13.0-13.3 us |

The 1 KiB and 64 KiB cases had the same allocation counts, confirming a fixed
per-event cost rather than a byte-volume cost. LTO reduced elapsed time but did
not eliminate the `async_trait` allocation. Allocation sampling runs in a
separate loop from elapsed-time measurement so the counter's atomic updates do
not inflate the recorded timings.

## Tracking references

- Review finding: `007-low-async-response-filter-adds-per-chunk-boxing.md`.
- Fork introduction: `600ac49` (`feat(proxy): add bidirectional body streaming controls`).
- Production hook: `pingora-proxy/src/proxy_trait.rs`.
