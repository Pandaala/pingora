# Fork feature and malformed-input audit — 2026-08-28

This is a point-in-time review map, not a second status owner for the linked
issues. Each pending issue or durable review record remains canonical.

## Baseline and scope

- Fork: `dfa2c8cabef4529df930aa4436f88d5024f763c5` (`edgion_v3`).
- Upstream comparison: `09696b5` (`upstream/main`).
- Sibling Edgion checkout: `83408c11dedb81eab8504d85edfb0fcc061c9e7f`.
- The sibling manifest currently has local path patches, while its lockfile
  still records fork commit `57f6183`; local results are not deployment-version
  evidence.
- Reviewed fork features: inbound PROXY v1/v2, request capture/replay and
  request transport events, H1/H2/custom pumps, response sink/terminal
  dispatch, H2 completion evidence, cache admission, reset/GOAWAY/reuse, and
  the Edgion consumer seams required by `AGENTS.md`.

Malformed, truncated, oversized, slow, reset, premature-response, and
contradictory framing/termination shapes were treated as defensive test input.

## Finding correction resolved after this audit

- [Custom upstream early response misses request-body abandonment](custom/custom-upstream-early-response-abandonment.md)
  was medium for the public fork and currently unreachable in Edgion. The
  correction prevents body polling after `Abandoned` while retaining the
  natural-completion downstream idle watcher. Fresh independent review returned
  LGTM and the project verification matrix passed.

## Resolved after this audit

- [H2 upload stall without END_STREAM](h2-grpc/h2-writer-stall-without-end-stream-bounded.md)
  is bounded by the H2 writer's protocol-local progress floor when no explicit
  write timeout exists. Expiry without qualified response END_STREAM fails the
  exchange and releases stream capacity.
- [ResponseBodySink chunk-count amplification](findings/003-medium-response-body-sink-does-not-bound-chunk-count.md)
  is resolved by independent 1 MiB and 2048-nonempty-chunk per-batch budgets.
  Exact-limit and precisely observed overflow tests cover H1, H2, custom, and
  cache paths.
- [Async response filter fast path](findings/007-low-async-response-filter-adds-per-chunk-boxing.md)
  is resolved by an allocation-free zero-sized default future and direct typed
  delegation. Object compatibility and existing async overrides are retained.
- [Watcher dependency evidence](findings/004-medium-end-stream-watch-is-not-pinned-to-an-audited-h2.md)
  is resolved by an audited h2 0.4.19 handoff checklist and a raised minimum;
  0.4.16-0.4.18 reproducibly failed the large-window continuing-upload case.
- [TLS bind-test error classification](findings/008-low-tls-feature-bind-test-rejects-observed-bind-error.md)
  is resolved without changing production semantics. Local bind failure remains
  `InternalError -> BindError`; localhost-only fixtures remove this finding's
  routing-dependent failures on macOS and Linux.

## Confirmed existing open findings

- [H2 trailer validation](../pending-issues/h2-trailer-validation.md) and
  [terminal HEADERS completion](../pending-issues/h2-terminal-headers-completion.md)
  remain blocked/deferred at the accepted upstream `h2` decoder boundary.
- `../Edgion/tasks/todo/issue-compression-trailer-double-eos.md` remains the
  canonical record for the fork-owned compression + Trailer/Done double-EOS
  defect. Edgion currently does not enable upstream compression.
- `../Edgion/tasks/todo/proxy-protocol-trust-config/proxy-protocol-trust-config.md`
  remains the canonical P1 Edgion consumer finding: invalid or empty trusted
  CIDR configuration collapses into Pingora's intentional `None` mandatory
  mode and widens who may assert a client address. The Pingora parser contract
  itself is sound.

## No new finding in these areas

- Strict PROXY v1/v2 grammar, declared-length and truncation handling,
  split-read parsing, trust-before-peek, TLS ordering, and raw peer retention.
- Request capture cancellation poisoning, finalize/rewind/replay EOF, bounded
  replay chunks, H1/H2 bodyless violations, retry gates, and H1/H2 early
  response abandonment.
- H2 watcher read-error poisoning, partial-frame EOF, local reset evidence
  invalidation, persistent GOAWAY ceiling, connection allocation after
  shutdown, terminal Trailer/Done dispatch, and cache/reuse decisions.

The known `h2` trailer pseudo-field and oversized-trailer handoff remains an
upstream limitation and was not re-reported.

## Verification evidence

- `cargo test -p pingora-proxy --lib`: 114 passed.
- `cargo test -p pingora-proxy --test test_request_body_seam`: 54 passed.
- `cargo test -p pingora-core proxy_protocol`: 27 passed.
- `cargo test -p pingora-proxy --test test_terminal_body_dispatch`: 9 passed.
- `cargo test -p pingora-proxy --test test_h2_upstream_no_error_reset`: 8 passed.
- `cargo test -p pingora-proxy --test test_h2_upstream_cache_and_reuse`: 7 passed.
- `cargo test -p pingora-proxy --test test_h2_upstream_stalled_after_response`: 3 passed.
- H2 watcher unit matrix: 54 passed, 8 intentional upstream-limitation cases
  ignored; watched H2 client matrix: 8 passed, 4 upstream-blocked cases ignored.
- `test_upstream_response_body_sink`: one initial parallel run had one
  non-reproduced empty-body failure; the failing case passed alone, all 43
  passed serially, and ten subsequent parallel full runs passed. It is an
  observation, not a classified finding without reproducible evidence.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

These commands cover the executable local checkout only. They do not prove
the sibling lockfile revision or a deployed Edgion revision has the same
behavior.
