# Response trailer filter error parity

Status: resolved

Severity: medium

Ownership: Pingora fork

Baseline: fork `bd89d47`, upstream merge base `09696b5`; resolved in the
working tree on 2026-08-29 without creating a commit by user request.

## Conclusion

The H1, H2, and custom response pumps now propagate the original
`ProxyHttp::response_trailer_filter` error through the common fatal response
path. H2 and custom no longer log and replace the error with `None` before
continuing trailer delivery.

The failure contract is downstream-specific: the rejected trailer, any body
buffer returned by the hook, the normal trailer terminator, and terminal sink
output are not written. The error type, source, and context reach application
logging unchanged. A failed H1/H2/custom exchange closes the H1 downstream
connection even when the client requested keepalive.

Upstream reuse remains protocol-specific. H1 uses its existing conservative
failure policy and discards the upstream session. H2 has already consumed the
complete stream and retains the healthy multiplexed connection. The custom
test connector has no session pool, so its coverage is limited to the hook,
error, terminal-output, and downstream-close contract.

## Cache boundary

Streaming-partial-write cache miss/readback serves the cached upstream body
representation and does not pass a real upstream `Trailer` through this
downstream hook. Non-streaming storage uses the inline downstream pump, but an
executable positive control proved that its H1 trailered-response admission is
already incomplete even when this hook succeeds. That separate defect is
canonical under
[non-streaming cache trailer completion](../pending-issues/non-streaming-cache-trailer-completion.md)
and must not be attributed to this resolved parity issue.

## Verification

Independent read-only subagent review completed with `LGTM` after cache-scope,
downstream keepalive, H1/H2 reuse, and stale-document findings were resolved.
The repository verification matrix passed:

- formatting and `pingora-core`/`pingora-proxy` checks, including
  `connection_filter` and `boringssl`;
- `pingora-core` library suites: 737 passed (17 ignored) by default and 742
  passed (17 ignored) with `connection_filter`; both targeted TLS/PROXY tests
  passed with `boringssl`;
- `pingora-proxy` library: 119 passed;
- request-body seam: 54 passed;
- response-body sink: 56 passed;
- terminal body dispatch: 26 passed;
- H2 reset, stalled-response, and cache/reuse suites: 8, 4, and 8 passed.

The Edgion consumer checkout at `83408c1` implements the upstream trailer hook
but does not implement this downstream hook and has no dependency on the old
H2/custom swallow-and-continue behavior.
