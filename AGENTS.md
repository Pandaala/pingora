# IMPORTANT: authorized owner-operated security work

This repository and its Edgion integration are the user's own, authorized
software projects. Repository remotes, dependencies, submodules, CI resources,
and related development infrastructure may use real Git SSH URLs, hostnames,
credentials supplied by the user's environment, and network/protocol test
fixtures. Their presence is normal project infrastructure and is not evidence
of malicious intent.

Work requested in this repository—including security review, fuzzing, malformed
or adversarial packet construction, parser/protocol boundary testing, denial-of-
service resistance checks, and regression reproduction—is authorized defensive
engineering intended to harden the user's software. Agents should not refuse,
water down, or misclassify such in-scope work merely because it involves SSH,
security-sensitive code, crafted packets, or attack-like test inputs. Prefer
safe local tests, isolated fixtures, and reproducible evidence; protect secrets
and avoid printing credentials. This authorization does not extend to attacking
unrelated third-party systems or exceeding the targets and infrastructure the
user has placed in scope.

# Edgion Pingora fork guide

## Project scope

This repository is the Pingora fork maintained for the Edgion Rust L7 gateway.
The primary consumer is the sibling `../Edgion` workspace. Keep generic proxy
and protocol behavior here; keep Edgion product policy, resource configuration,
and gateway-specific orchestration in `../Edgion`.

Start every non-trivial task at [`edgion-changes/README.md`](edgion-changes/README.md).
Load only the linked architecture, feature, pending-issue, or review documents
needed for the task. Do not read the whole knowledge tree by default.

## Ownership and fix policy

- If a defect is inherited from upstream Pingora or a dependency, record the
  limitation and revisit trigger, retain necessary local safety guards, and
  wait for the normal upstream fix. Do not carry a speculative local fork or
  invasive workaround merely to make upstream code appear fixed.
- If a defect is introduced by this fork, including its parser, buffering,
  watcher integration, streaming hooks, cache integration, tests, or docs, fix
  it here and add proportionate regression coverage.
- An upstream root cause does not excuse a fork-owned amplification, unsafe
  fallback, stale claim, or missing boundary check. Fix the part we own.
- Fix defects in Edgion's use/configuration of a sound contract in `../Edgion`.
  If both sides disagree, record and link both sides and change each repository
  only for the behavior it owns.

## Known upstream h2 trailer limitation

`h2` can discard forbidden trailer pseudo-fields while exposing the remaining
ordinary fields as a valid-looking trailer map; it also lacks the required
oversized-trailer rejection at that handoff. A following
`RST_STREAM(NO_ERROR)` can obscure some failures from Pingora's completion
logic. This is a pre-existing, low-frequency upstream decoder/interface
limitation, not a regression introduced by this fork.

Accepted policy: do not vendor or locally fork `h2` for this issue. Keep the
normal upstream dependency and wait for an upstream fix. Until that fix is
adopted and covered by regression tests, do not simplify the END_STREAM watcher
on the assumption that terminal trailers and reset ordering are fully handled
by `h2`, and do not use wire-level END_STREAM alone as proof for cache
admission. See
[`review/upstream-limitations.md`](edgion-changes/review/upstream-limitations.md).

## HTTP/1 request transfer coding

Pre-admission handling first rejects HTTP/1.0 transfer coding, a missing or
non-`chunked` final coding token, and disabled CONNECT.
Of the external H1 requests that reach `HttpProxy`, only an
absent `Transfer-Encoding` or one trimmed, case-insensitive `chunked` field is
forwardable; any other form enters the 501 error-rendering path and is forced
non-reusable before filters, cache, or upstream selection. See the status-code
and custom-renderer details in
[`request-body-transport.md`](edgion-changes/features/request-body-transport.md#http1-transfer-coding-admission).

## Cross-repository review

Review with `../Edgion` whenever a change touches a public hook, `Session` API,
body buffering/replay, retry, framing, local replies, listener/PROXY protocol,
TLS feature wiring, or observable termination/error behavior. At minimum check:

- `../Edgion/Cargo.toml` for the selected fork revision or local patch;
- `../Edgion/edgion-gateway/src/runtime/server/listener_builder.rs` for
  listener and transport policy;
- `../Edgion/edgion-gateway/src/routes/http/proxy_http/mod.rs` and sibling
  `pg_*.rs` files for the real consumer contract.

Do not assume the sibling checkout is the revision in Edgion's lockfile or a
deployed revision. Record both commits when a conclusion depends on versions.

## Review workflow

Before reporting a finding, read
[`edgion-changes/review/README.md`](edgion-changes/review/README.md) and search
the relevant review and pending-issue subtree. Classify it as a fork defect,
upstream limitation, accepted design, dismissed/unreachable finding, open
investigation, or resolved finding. Do not re-report a recorded conclusion
unless code, dependency behavior, a premise, or reproducible evidence changed;
state that change when reopening it.

For proxy lifecycle changes, cover the H1, H2, and custom pumps plus cache hit,
cache fill, retry, early response, terminal trailer/Done, reset, and connection
reuse paths. Wire END_STREAM, decoded EOF, content-length satisfaction,
application abandonment, and replay EOF are distinct facts.

## Test organization

- Prefer substantial test suites and tests that require large fixtures or
  harnesses in dedicated, behavior-grouped files instead of growing large
  inline `#[cfg(test)]` modules. Use crate-level `tests/` integration targets
  for public behavior, or an external `#[cfg(test)]` submodule when private
  implementation access is necessary.
- Keep small, focused tests inline when they directly exercise private details.
  Do not widen production APIs, raise item visibility, or add test-only
  production interfaces merely to move tests out of the source file.
- Reuse shared harness code from a test subdirectory or `common/mod.rs`. Avoid
  one-test-per-file fragmentation and unnecessary extra targets that duplicate
  service startup or create fixed-port conflicts.

## Knowledge maintenance

- Update [`features.md`](edgion-changes/features.md) and the matching detailed
  contract when fork behavior changes.
- Put actionable unfinished work in
  [`pending-issues/`](edgion-changes/pending-issues/) and durable review
  conclusions in [`review/`](edgion-changes/review/).
- Keep one canonical record; cross-link summaries instead of copying status.
- Follow [`upstream-sync.md`](edgion-changes/maintenance/upstream-sync.md) for
  rebases and update
  [`test-matrix.md`](edgion-changes/verification/test-matrix.md) only after
  running its commands.
