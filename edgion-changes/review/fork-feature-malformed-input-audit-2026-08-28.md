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

## Finding resolved after this audit

- [Custom upstream early response misses request-body abandonment](custom/custom-upstream-early-response-abandonment.md)
  was medium for the public fork and currently unreachable in Edgion. The
  correction prevents a custom downstream from being polled after `Abandoned`
  and passed fresh independent review and the project verification matrix.

## Confirmed existing open findings

- [ResponseBodySink chunk-count amplification](findings/003-medium-response-body-sink-does-not-bound-chunk-count.md)
  remains open. The byte budget does not bound the number of nonempty emitted
  chunks, so a filter can amplify one batch into roughly one million cache and
  downstream operations.
- [H2 upload stall without END_STREAM](../pending-issues/h2-request-body-stall-without-end-stream.md)
  remains an open policy issue when no write timeout is configured. Edgion's
  normal peer configuration supplies a bounded timeout.
- [H2 trailer validation](../pending-issues/h2-trailer-validation.md) and
  [terminal HEADERS completion](../pending-issues/h2-terminal-headers-completion.md)
  remain blocked/deferred at the accepted upstream `h2` decoder boundary.
- [Watcher dependency evidence](findings/004-medium-end-stream-watch-is-not-pinned-to-an-audited-h2.md)
  still needs its stale `h2 0.4.15` audit claims aligned with the supported
  version range. Its old exact-pin recommendation is superseded.
- [Async response filter fast path](../pending-issues/async-response-body-filter-fast-path.md)
  remains a measured performance investigation, not a correctness finding.
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
