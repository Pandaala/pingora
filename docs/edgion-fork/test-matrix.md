# Verification matrix

## Self-contained checks

Run from the repository root:

```text
cargo fmt --all -- --check
cargo check -p pingora-core -p pingora-proxy
cargo check -p pingora-core --features "connection_filter boringssl"
cargo test -p pingora-core --lib
cargo test -p pingora-core --lib --features connection_filter
cargo test -p pingora-proxy --lib
cargo test -p pingora-proxy --test test_request_body_seam
cargo test -p pingora-proxy --test test_upstream_response_body_sink
cargo test -p pingora-proxy --test test_terminal_body_dispatch
cargo test -p pingora-proxy --test test_h2_upstream_no_error_reset
cargo test -p pingora-proxy --test test_h2_upstream_cache_and_reuse
```

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
| `pingora-proxy --lib` | disposition truth tables, terminal latch, sink budget, EOS migration, retry guards |
| `test_request_body_seam` | 54 H1/H2 request-pump, framing, retry and termination scenarios |
| `test_upstream_response_body_sink` | 43 response streaming/cache/custom scenarios |
| `test_terminal_body_dispatch` | 9 self-contained terminal/trailer scenarios |
| `test_h2_upstream_no_error_reset` | 8 self-contained H2 reset/completion scenarios |
| `test_h2_upstream_cache_and_reuse` | 7 H2 cache-admission, upstream-connection-reuse and peer-window-handshake scenarios |

## Validation snapshot

Validated on 2026-08-26 against `main` at `09696b5`:

- `cargo fmt --all -- --check`: passed.
- `cargo check -p pingora-core -p pingora-proxy`: passed.
- `cargo check -p pingora-core --features "connection_filter boringssl"`:
  passed.
- `cargo test -p pingora-core --lib`: 686 passed, 2 ignored.
- `cargo test -p pingora-core --lib --features connection_filter`: 691 passed,
  2 ignored.
- `cargo test -p pingora-proxy --lib`: 107 passed.
- `test_request_body_seam`: 54 passed.
- `test_upstream_response_body_sink`: 43 passed.
- `test_terminal_body_dispatch`: 9 passed.
- `test_h2_upstream_no_error_reset`: 8 passed.
- The standalone integration targets compile together with `--tests --no-run`.

`cargo check -p pingora-core -p pingora-proxy --all-features` currently fails
before compiling repository code because Cargo resolves `metrique 0.1.31`
against an incompatible `metrique-core 0.1.6`. The same failure reproduces in
a detached, unmodified `main` worktree, so it is a baseline dependency issue
rather than a regression in this feature stack.

Validated on 2026-08-28 against `edgion_v3` at `7f9c6c6`, for the target added
that day only. Recorded separately because the snapshot above pins a different
branch and commit, and merging the two would misattribute either result:

- `test_h2_upstream_cache_and_reuse`: 7 passed, over 8 consecutive runs with no
  flakes. Requires the `127.0.0.2` loopback alias described above.

## Review gates

- `git diff --check` is clean.
- No Cargo.toml, crate version or dependency change belongs to this feature
  stack unless a future feature explicitly needs one.
- New integration tests remain standalone test targets.
- The original `edgion` and `edgion_v2` refs still resolve to their recorded
  SHAs.
- A rebase/merge-tree preview against the target main is reviewed before the
  branch is moved.
