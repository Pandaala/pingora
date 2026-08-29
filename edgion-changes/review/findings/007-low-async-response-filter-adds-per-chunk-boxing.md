# Allocation-free default async response-body hooks

Status: resolved (2026-08-29)

Severity: low / fork-owned performance issue

Fork baseline: `upstream/main...edgion_v3`; introduced by `600ac49`, resolved
in the uncommitted working tree based on `bd89d47`.

## Original problem

The fork changed `ProxyHttp::upstream_response_body_filter` from upstream's
synchronous hook to an `async_trait` method, then added a typed
`upstream_response_body_filter_event` that delegates to it. The default path
therefore constructed and dynamically polled two nested boxed futures for each
ordinary or synthetic response-body event, even when the application used the
default no-op implementation.

The focused release benchmark isolated one allocation / 16 bytes and roughly
10-20 ns in the legacy default hook alone. The actual shared H1 body-task
pipeline showed two allocations per task, matching the typed and legacy
default layers.

## Resolution

The public methods keep the object-safe pinned boxed-future signature generated
by `async_trait`. The legacy default boxes a custom zero-sized future whose
`poll()` constructs `Ok(None)`; Rust does not request allocator storage when a
zero-sized value is boxed. The typed default maps the event to its legacy EOS
boolean and returns the legacy future directly, avoiding the previous second
async block and box. This avoids a capability method and therefore never tries
to infer whether an application overrode either hook.

Compatibility is deliberate:

- existing implementations can keep their `#[async_trait] async fn`
  `upstream_response_body_filter` or typed event override;
- the manually expanded trait signature matches the macro-generated pinned
  boxed future, so override source and `dyn ProxyHttp<CTX = ...>` compatibility
  remain unchanged;
- a real async override is called and awaited on every event exactly as before;
- the default typed hook still delegates all `Data`,
  `TerminalBeforeTrailers`, and `TerminalWithoutTrailers` events to the legacy
  hook with the established EOS mapping.

The explicit lifetime and `Self: Sync` bounds mirror `async_trait`'s expansion.
The implementation uses no language feature newer than the crate's Rust 1.85
MSRV. `cargo doc` succeeds and the trait remains object-compatible; production
services continue to use the existing generic path.

## Performance evidence

Measured on Apple M4 / arm64 with Rust 1.96.1, 100,000 default iterations and
5,000 yielding iterations. Times are synthetic and noisy; allocation counts
are the durable result.

| Build / path | Before | After |
|---|---:|---:|
| focused default call, no LTO | legacy hook: 1 allocation / 16 B / ~10.9 ns | full typed event: 0 allocations / 0 B / ~1.4 ns |
| focused default call, LTO | legacy hook: 1 allocation / 16 B / ~11-15 ns | full typed event: 0 allocations / 0 B / ~1.4-2.3 ns |
| yielding async override | 1 allocation / 32 B / ~12-15 us | 1 allocation / 32 B / ~13 us |
| actual shared H1 body-task pipeline | 2 allocations / ~73 ns | 0 allocations / ~54 ns |

The 1 KiB and 64 KiB cases have identical allocation results, confirming that
the removed cost was per event. Since the complete semantic pipeline lost both
allocations and roughly a quarter of its isolated runtime, no additional
network benchmark or permanent capability API is needed to justify this
localized change. Network throughput remains workload- and platform-specific;
the manual benchmark is not a pass/fail product contract.

## Cross-repository check

Edgion checkout `83408c1` implements the legacy hook as an
`#[async_trait] async fn`, the same source form compiled by multiple Pingora
unit/integration fixtures after this change. At that Edgion HEAD, the committed
lock selects fork `480bad2`. Edgion's dirty working-tree lock instead selects
`57f6183`; its dirty local path patch is unused because those locks expect fork
version 0.8.1 while this migrated checkout reports 0.8.0. No Edgion source
change is required; an exact sibling build must be repeated when Edgion
advances its selected fork revision/version.

## Verification contract

- Run `cargo bench -p pingora-proxy --bench response_body_filter` with and
  without `CARGO_PROFILE_BENCH_LTO=true` when changing either method shape.
- The default event must report zero allocations; a yielding override must
  still run, allocate its own async-trait future, and await successfully.
- Run proxy library, response sink, terminal dispatch, H2 reset/stall/cache,
  H1/H2/custom pump, cache hit/fill, and formatting checks.
- Recheck Edgion when its manifest/lock selects a revision containing this
  change.

## Revisit trigger

Reopen if a supported compiler no longer accepts async-trait implementations
against the manually expanded signature, if object compatibility regresses, or
if a real async override is skipped, polled differently, or changes terminal
semantics.
