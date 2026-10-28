---
name: custom-request-contract-coverage
description: Real custom-pump tests for request action, unsupported framing and stale source EOF.
status: fixed
finding_id: 03-fork-layer-coverage-for-unreachable-paths
---

# Custom request contracts require actual pump coverage

## Conclusion and scope

The custom connector owns its framing. Its request pump rejects non-ordinary
`RequestRelayPlan.disposition` values before writing upstream headers and rejects
ordinary request-body `Terminate` actions with `InternalError`. A source read
returning `None` is EOF even when a custom downstream reports
`is_body_done() == false`: the relay emits `Complete`, finishes the upstream
body writer once, and stops request-body reads. Idle disconnect monitoring after
natural completion is a separate operation and remains supported.

These are existing fork contracts, not new transport capabilities. The sibling
Edgion gateway does not configure a custom connector. The production code,
public hooks, and sibling manifest/lockfile are unchanged by this test work.

## Corrected historical premise

The Edgion task `03-fork-layer-coverage-for-unreachable-paths` originally claimed
that the harness had never constructed a custom session and that the pump had
only code-review coverage. At the selected dependency `0b0d8bb`,
`HeaderOnlyCustomConnector` and `ScriptedCustomDownstream` already provided real
custom-upstream and custom-downstream drivers. `request_relay_tests.rs` already
tested typed termination and source EOF in the shared relay. Those facts
disprove the broad claim but do not prove the missing whole-pump scenarios.

The added tests reuse those drivers in
`pingora-proxy/tests/upstream_response_body_sink/custom_request_contract.rs`:

- `ordinary_custom_upload_observes_real_writes_and_completion` is the positive
  control: two declared H1 data chunks are sent byte by byte, with every byte
  gated on cumulative actual forwarding. This forces partial chunk callbacks;
  all bytes reach the custom writer and the writer finishes once.
- `custom_request_terminate_fails_closed_without_forwarding_the_chunk` forwards
  a fragmented prefix first, explicitly arms termination, then sends only the
  first byte of the next declared chunk. Its next nonempty Data callback
  completes a local 403 response before returning `Terminate`. It observes the
  typed unsupported error and no forwarding of that fragment or clean upstream
  request completion, without requiring a full chunk to arrive.
- `custom_nonordinary_dispositions_fail_before_the_upstream_header_write`
  exercises `Bodyless` and `Streamed` separately. Both reach a real custom
  upstream, fail with the framing-capability error, and never write its headers,
  body, or request EOF.
- `custom_source_eof_finishes_once_despite_stale_body_done` drives a custom
  downstream that yields bytes, then `None`, while its done predicate stays
  false. It observes `Data, Complete`, two body reads, one writer finish and
  successful exchange logging. A third body read is recorded and parks instead
  of consuming a worker in a regression.

## Observation and limits

Per-script probes retain a whole event log out of band. The tests establish
upstream entry and actual forwarding before making negative claims, combine
bounded absence checks with exact whole-log counts, and require exchange
logging rather than treating an early local reply as successful proxy teardown.
Multiple notification waiters are registered before inspecting the log.

The termination response reader waits until the application has made its
decision; ordinary readers wait for request completion. This prevents a scripted
early response from abandoning the upload before the action under test occurs.
The EOF driver uses a separate listener in the same process-wide harness and
does not change existing early-response drivers' shared counters.

## CONNECT coverage and corrected historical limits

Item A's old task and test comment claimed that H1 authority-form parsing
required unavailable `patched_http1`, and that upstream serialization panicked
in `raw_path`. Current code and the locked `0b0d8bb` already disprove both:
`RequestHeader::set_raw_path` uses the request-target parser, `raw_path` retains
an authority fallback, and the core tests `read_authority_form_request_target`
and `test_absolute_form_and_connect_to_wire` exercise those boundaries.
The H1 seam listener now opts into the existing CONNECT admission option.

`seam::connect::bodyless_preserves_h1_connect_request_framing` observes actual
authority-form upstream request bytes for no length, Content-Length and chunked
requests. It verifies exact payloads and framing, decoding chunked data without
assuming callback/chunk boundaries. The shared recording origin collects extra
bytes during its bounded grace window before responding. This complements the
truth table and existing real H2 CONNECT tunnel test.

The old claim that honored H2 `Streamed` is wire-identical to coerced Ordinary
is also obsolete as a reason to omit current coverage. A frozen Streamed plan
now fails closed for CONNECT before writing an upstream header, via
`validate_streamed_upstream_disposition`; see the canonical
[request-body contract](../../features/request-body-transport.md).
`seam::connect::streamed_rejects_h1_connect_before_upstream_headers` verifies
that outcome for all three H1 shapes with bounded absence and exact header
counts. The existing H2 synthesized-CONNECT test covers the attempt-local
rewrite boundary separately.

The H1 tests cover request parsing, serialization and relay framing before the
successful response. They do not assert bidirectional H1 tunneling after a 200,
add Gateway API CONNECT routes, or claim a full downstream/upstream matrix.

## Verification and re-evaluation

On 2026-10-06, at fork `bd08429` plus these test changes, each of the six new
tests passed 10 isolated invocations with `--exact --test-threads=1`.
The response-body/custom integration target passed all 62 cases.

In a separate detached worktree, custom-only mutations of the request-action
condition and source-EOF condition failed their respective tests. Exempting
either `Bodyless` or `Streamed` from the framing guard failed the disposition
test. Clearing forwarded prefix bytes failed the ordinary positive control;
disabling its write recorder also failed that control. Removing CONNECT from
the coercion condition failed the H1 framing test, and removing it from the
Streamed conflict condition failed the pre-header rejection test. All eight
mutations failed and all mutated files were restored. These are observed
failures, not inferred discrimination.

Re-evaluate if relay dispatch bypasses the custom pump, custom framing or
termination capabilities change, or source EOF and idle monitoring are merged.
No evidence here establishes deployed coverage: Edgion still locks `0b0d8bb`
and the new test files are local, uncommitted work until separately published.
