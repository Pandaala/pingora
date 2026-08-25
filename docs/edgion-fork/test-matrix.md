# Verification matrix

## Self-contained checks

Run from the repository root:

```text
cargo fmt --all -- --check
cargo check -p pingora-core -p pingora-proxy
cargo test -p pingora-core --lib
cargo test -p pingora-proxy --lib
cargo test -p pingora-proxy --test test_request_body_seam
cargo test -p pingora-proxy --test test_upstream_response_body_sink
```

Expected feature coverage:

| Target | Coverage |
| --- | --- |
| `pingora-core --lib` | body buffers, H1/H2 sessions, END_STREAM watch, listener and PROXY parser |
| `pingora-proxy --lib` | disposition truth tables, terminal latch, sink budget, EOS migration, retry guards |
| `test_request_body_seam` | 54 H1/H2 request-pump, framing, retry and termination scenarios |
| `test_upstream_response_body_sink` | 43 response streaming/cache/custom scenarios |

## External-origin checks

The following targets start the repository's openresty mock origin and require
the `openresty` executable expected by `tests/utils/mock_origin.rs`:

```text
cargo test -p pingora-proxy --test test_terminal_body_dispatch
cargo test -p pingora-proxy --test test_h2_upstream_no_error_reset
```

If openresty is absent, the first initialization panics with `No such file or
directory`; the shared `once_cell::Lazy` is then poisoned, so later failures in
the same binary are consequential environment failures rather than independent
product failures.

## Validation snapshot

Validated on 2026-08-25 against `main` at `e6e677f`:

- `cargo fmt --all -- --check`: passed.
- `cargo check -p pingora-core -p pingora-proxy`: passed.
- `cargo test -p pingora-core --lib`: 610 passed, 2 ignored.
- `cargo test -p pingora-proxy --lib`: 78 passed.
- `test_request_body_seam`: 54 passed.
- `test_upstream_response_body_sink`: 43 passed.
- Every source commit was checked independently in a detached worktree; the
  standalone integration-test commit was also built with `--tests --no-run`.
- The two external-origin targets compile. Their full runtime matrix is blocked
  on this host because `openresty` is not installed.

`cargo check -p pingora-core -p pingora-proxy --all-features` currently fails
before compiling repository code because Cargo resolves `metrique 0.1.31`
against an incompatible `metrique-core 0.1.6`. The same failure reproduces in
a detached, unmodified `main` worktree, so it is a baseline dependency issue
rather than a regression in this feature stack.

## Review gates

- `git diff --check` is clean.
- No Cargo.toml, crate version or dependency change belongs to this feature
  stack unless a future feature explicitly needs one.
- New integration tests remain standalone test targets.
- The original `edgion` ref still resolves to the recorded SHA.
- A rebase/merge-tree preview against the target main is reviewed before the
  branch is moved.
