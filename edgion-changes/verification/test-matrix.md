# Verification matrix

## Self-contained checks

Run from the repository root:

```text
cargo fmt --all -- --check
cargo check -p pingora-core -p pingora-proxy
cargo check -p pingora-core --features "connection_filter boringssl"
cargo test -p pingora-core --lib
cargo test -p pingora-core --lib --features connection_filter
cargo test -p pingora-core --lib --features boringssl
cargo test -p pingora-core --lib --features boringssl test_listen_tls_proxy_protocol
cargo test -p pingora-proxy --lib
cargo test -p pingora-proxy --test test_request_body_seam
cargo test -p pingora-proxy --test test_upstream_response_body_sink
cargo test -p pingora-proxy --test test_terminal_body_dispatch
cargo test -p pingora-proxy --test test_h2_upstream_no_error_reset
cargo test -p pingora-proxy --test test_h2_upstream_stalled_after_response
cargo test -p pingora-proxy --test test_h2_upstream_cache_and_reuse
```

The inherited core unit suites still include integration-style connector tests
against `1.1.1.1`, so the complete core commands require outbound networking
and are not hermetic. The local bind and no-total-timeout fixtures covered by
finding 008 no longer add external routing dependencies of their own.

`test_h2_upstream_cache_and_reuse` binds its upstream sockets through the
`client_bind_to_ipv4: 127.0.0.2` of `pingora-proxy/tests/pingora_conf.yaml`, as
every `pingora-proxy` integration target does. Linux carries all of `127.0.0.0/8`
on loopback; on macOS the alias has to be added first
(`sudo ifconfig lo0 alias 127.0.0.2 up`), otherwise every request fails to
connect and the target reports 502s rather than a real failure.

Expected feature coverage:

| Target | Coverage |
| --- | --- |
| `pingora-core --lib` | body buffers, H1/H2 sessions, END_STREAM watch, listener and PROXY parser |
| `pingora-core --lib --features boringssl` | complete TLS-backed core suite, including deterministic direct/high-level local source-bind classification and timeout-context separation |
| `pingora-core --lib --features boringssl test_listen_tls_proxy_protocol` | explicit PROXY-before-TLS rejection stages, successful handshake and address preservation |
| `pingora-proxy --lib` | 126 passed plus 1 ignored manual benchmark: H1 transfer-coding admission, disposition truth-table, H2 write-floor and abandoned-reservation cleanup, terminal-latch, object-compatible response hooks, shared response-pipeline parity, sink-budget, EOS-migration and retry-guard tests |
| `test_request_body_seam` | 60 H1/H2 request-pump, framing, retry, transfer-coding admission and termination scenarios |
| `test_upstream_response_body_sink` | 57 response streaming/cache/custom scenarios |
| `test_terminal_body_dispatch` | 26 self-contained terminal/trailer scenarios |
| `test_h2_upstream_no_error_reset` | 8 self-contained H2 reset/completion scenarios |
| `test_h2_upstream_stalled_after_response` | 4 H2 request-body stall, configured-deadline, default-floor and END_STREAM discrimination scenarios |
| `test_h2_upstream_cache_and_reuse` | 8 H2 cache-admission, upstream-connection-reuse, stalled-write cleanup and peer-window-handshake scenarios |

## h2 dependency audit

The workspace keeps a normal open upstream range and currently declares
`h2 >= 0.4.19`. On 2026-08-29 the ignored local lockfile was temporarily
resolved to each candidate release, the indicated contracts were run, and its
h2 resolution was then returned to 0.4.19. No pre-audit lockfile hash exists,
so this is not a byte-for-byte restoration claim. No exact pin, upper bound, or
vendored h2 was introduced.

| h2 | Result | Evidence |
| --- | --- | --- |
| 0.4.16 | incompatible | core 737 passed / 17 ignored; H2 stall 4 passed; cache/reuse 8 passed; reset 7 passed / 1 failed reproducibly with `too_many_data_frames` |
| 0.4.17 | incompatible | focused continuing-upload reset contract failed with `too_many_data_frames` |
| 0.4.18 | incompatible | focused continuing-upload reset contract failed with `too_many_data_frames` |
| 0.4.19 | supported minimum/current | focused contract and the complete current-version H2 matrix passed |

h2 0.4.19 is the first tested release whose automatic small-DATA-frame budget
scales with the configured connection window. This matters because the fork's
large-window continuing-upload case legitimately produces enough DATA framing
overhead to exhaust the fixed default in 0.4.16-0.4.18.

Every future h2 upgrade must test both the declared minimum and current
resolution and repeat the private handoff audit recorded in
`end_stream_watch.rs`: reset-state preservation, receive-queue draining,
GOAWAY/error handling, receive-buffer lifetime, strict frame order, and the
known trailer-decoder boundary. Temporary dependency resolutions must end with
the ignored local `Cargo.lock` resolving the intended current version; future
audits should record a pre-change hash if byte-for-byte restoration matters.

`cargo +1.85.0 check -p pingora-core -p pingora-proxy` also passed with h2
0.4.19. The dependency itself declares Rust 1.63, so raising the h2 minimum does
not raise this workspace's Rust 1.85 MSRV.

## Validation snapshot

Validated on 2026-08-30 against the H1 transfer-coding fail-close working tree
based on `2fbd195`:

- `cargo fmt --all -- --check`: passed.
- `cargo check -p pingora-core -p pingora-proxy`: passed.
- `cargo check -p pingora-core --features "connection_filter boringssl"`:
  passed.
- `cargo test -p pingora-core --lib`: 737 passed, 17 ignored.
- `cargo test -p pingora-core --lib --features connection_filter`: 742 passed,
  17 ignored.
- `cargo test -p pingora-core --lib --features boringssl`: macOS 769 passed / 17
  ignored; Linux arm64 container 781 passed / 18 ignored. Both direct L4 and
  high-level source-bind tests assert `InternalError -> BindError ->
  AddrInUse`; the no-total-timeout test uses a local plaintext listener instead
  of relying on TEST-NET routing. During the 2026-08-30 H1 admission rerun,
  launching this target concurrently with the connection-filter suite produced
  one `test_connect_uds` reuse-assertion failure; the complete boringssl target
  then passed in a dedicated serial rerun. Feature variants should not be run
  concurrently when they share process-external socket fixtures.
- `cargo test -p pingora-core --lib --features boringssl test_listen_tls_proxy_protocol`:
  2 passed.
- `cargo test -p pingora-proxy --lib`: 126 passed, 1 ignored manual response
  pipeline benchmark.
- `test_request_body_seam`: 60 passed, including rejected H1-to-H1/H2
  transfer-coding cases, their plain-chunked controls, and the reused-connection
  hook-ordering control plus the sequential same-socket renderer/logger
  force-close regression.
- `test_upstream_response_body_sink`: 57 passed.
- `test_terminal_body_dispatch`: 26 passed.
- `test_h2_upstream_no_error_reset`: 8 passed.
- `test_h2_upstream_stalled_after_response`: 4 passed.
- `test_h2_upstream_cache_and_reuse`: 8 passed on the full rerun. Its existing
  flow-control reuse case failed once during the first concurrent target run,
  then passed both in isolation and in the complete target rerun. The host had
  the required `127.0.0.2` loopback alias.
- `cargo clippy -p pingora-proxy --all-targets`: completed with only pre-existing
  warnings in `pingora-core::ServerSession`, `response_pipeline.rs`,
  `proxy_h2.rs`, and `test_upstream_response_body_sink.rs`; no changed H1
  admission or seam-test line produced a warning. Adding `--deny warnings` is
  currently blocked first by the baseline `large_enum_variant` warning in
  `pingora-core/src/protocols/http/server.rs`.
- `(cd ../Edgion && cargo test -p edgion-gateway --lib
  pg_early_request_filter::tests -- --nocapture)`: 4 passed, covering normal
  HTTP/1.1, exact-chunked plus content length, HTTP/1.0 default/explicit
  keepalive, and shutdown. `cargo check -p edgion-gateway --lib` also passed.
  `cargo clippy -p edgion-gateway --lib` completed with the repository's
  existing warning baseline and no warning in the changed early-filter file.
  This Edgion build resolved Pingora revision `57f6183c` at version `0.8.1`;
  Cargo warned that the sibling `0.8.0` path patch was unused, so the two
  repositories' verification claims remain separate.

All standalone targets in this snapshot ran on the macOS host. Its loopback
configuration already contained `127.0.0.2` through `127.0.0.7`; no network or
system configuration was changed for the run.

The 2026-08-29 private-test-module extraction preserved the complete unit-test
lists byte-for-byte after sorting:

- `pingora-core --lib`: 756 list-output lines, SHA1
  `9ed27e5e7edc992917dd7b9ad773efe8e9f66882`;
- `pingora-proxy --lib`: 126 list-output lines, SHA1
  `f3388b383452d8c0042298b7f39d0fa3256ee19d`.

The extraction added no Cargo test target, production visibility, or fixed-port
harness. The normal core/proxy unit runs retained 17 and 1 ignored tests,
respectively.

The manual response-body benchmark also covers the allocation-free default
hook. On the 2026-08-29 Apple M4 / arm64 run (Rust 1.96.1), the old direct
legacy default measured 1 allocation / 16 bytes / about 10.9 ns, while the new
full typed-to-legacy default event measured 0 allocations / 0 bytes / about
1.4 ns without LTO. With LTO it remained allocation-free at about 1.4-2.3 ns.
A genuinely yielding async override retained its one boxed
future and awaited behavior. The actual shared H1 body-task benchmark changed
from about 73 ns / 2 allocations to 54 ns / 0 allocations per task. These are
manual release measurements, not pass/fail throughput contracts.

`cargo check -p pingora-core -p pingora-proxy --all-features` currently fails
before compiling repository code because Cargo resolves `metrique 0.1.31`
against an incompatible `metrique-core 0.1.6`. The same failure reproduces in
a detached, unmodified `main` worktree, so it is a baseline dependency issue
rather than a regression in this feature stack.

## Review gates

- `git diff --check` is clean.
- Every fork-critical standalone integration target and feature-gated unit-test
  filter is listed explicitly above and mirrored by CI.
- Cargo.toml, crate-version, and dependency changes require an independent
  source audit plus minimum/current behavior evidence such as the h2 matrix
  above; an incidental resolution change is not sufficient.
- New integration tests remain standalone test targets.
- Substantial new harnesses follow the root `AGENTS.md` test-organization rule:
  behavior-grouped external files by default, without widening production APIs
  merely to move private-detail tests.
- The original `edgion` and `edgion_v2` refs still resolve to their recorded
  SHAs.
- A rebase/merge-tree preview against the target main is reviewed before the
  branch is moved.
