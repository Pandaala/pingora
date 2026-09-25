# Default retry-policy unit tests fail on the current baseline

- ID: `default-retry-policy-test-baseline`
- Status: open investigation
- Severity: low (verification blocker; no runtime defect established)
- Ownership: fork tests / request-relay contract
- Origin: 2026-09-25 downstream response-body EOS verification
- Baseline: `0b0d8bba9609655bfd7db0fe0add6818bc0ee937`

## Reproduction and evidence

`cargo test -p pingora-proxy --lib default_retry_policy` fails all three cases:
`default_retry_policy_requires_an_idempotent_method`,
`default_retry_policy_requires_an_untruncated_body_buffer`, and
`default_retry_policy_resolves_reused_only`.

The failures reproduce in a clean `git archive HEAD` directory with the
original workspace's ignored `Cargo.lock` copied in and `--locked` enabled.
The response-body EOS diff is absent from that directory. Each failure expects
an eligible request to retry but observes false.

`DefaultRetryProxy` tests in `pingora-proxy/src/lib.rs` construct sessions and
invoke `ProxyHttp::error_while_proxy` directly. The current default hook in
`proxy_trait.rs` also consults
`session.request_relay_retry_state().can_start_next_attempt()`. Investigate
whether the test helpers need the production relay-plan/attempt lifecycle or
whether a real retry-policy regression exists; do not weaken the production
retry safety gate simply to satisfy these assertions.

## Next action and closure

Trace normal relay initialization and retry eligibility, correct the owning
test or runtime behavior with evidence, then run the three focused tests,
`cargo test -p pingora-proxy --lib`, and relevant request replay/retry coverage.
This is separate from
[downstream terminal observation](../review/h2-grpc/trailer-done-terminal-body-dispatch.md#downstream-terminal-observation)
and must not be hidden by skipping tests in a claimed green full-library run.
