---
name: h2-configured-timeout-capacity-reservation
description: Use when reviewing H2 request-body capacity cleanup after a configured write timeout is converted into successful upload abandonment.
status: fixed
finding_id: h2-configured-timeout-keeps-capacity-reservation
closed: 2026-08-29
---

# Successful H2 upload abandonment cancels its capacity reservation

## Conclusion

Every H2 request-body write that becomes a successful
`UpstreamDoneReceiving` outcome cancels its outstanding `SendStream` capacity
request before response delivery continues. This applies both to the
protocol-local stall probe and to a configured `write_timeout` combined with
qualified response END_STREAM evidence.

The cancellation does not change whether an exchange succeeds. It is cleanup
required after that decision: once the write future is gone and the pump has
decided never to resume the upload, no later capacity assignment may remain
reserved for that stream.

## Root cause

`pingora-core::protocols::http::v2::write_body` calls
`reserve_capacity(remaining.len())` before awaiting `poll_capacity`. Its timeout
drops the wait future but retains the `SendStream`, so it does not cancel the
reservation attached to that handle.

The fork's protocol-local `StalledAfterResponse` path already called
`reserve_capacity(0)`. The configured-timeout path instead passed
`WriteTimedout` through `upstream_write_error_outcome`, converted it to
`UpstreamDoneReceiving`, and kept the same `SendStream` alive while the response
continued without cancelling the request. That fork-owned successful fallback
amplified an inherited cancellable wait into a multiplexing capacity stall.

## Impact boundary

The defect requires an upstream H2 connection with more than one allowed
stream. A later WINDOW_UPDATE can assign connection capacity to the abandoned
stream, and h2 will not reuse that capacity for siblings until it is explicitly
returned or the handle is dropped. Slow downstream response delivery extends
that interval.

`PeerOptions::max_h2_streams` defaults to one, and the reviewed Edgion checkout
at `83408c1` does not override it, so this is latent in Edgion's current normal
configuration. It remains a real fork defect for Pingora's supported
multiplexing configurations. Edgion's lockfile at that review selected the
historical fork commit `57f6183`; the sibling checkout is not treated as the
deployed revision.

## Fix

`cancel_abandoned_upstream_body_capacity` is the shared cleanup point for both
successful abandonment routes. It calls `reserve_capacity(0)` only after the
write future has ended and the outcome says no more request bytes will be
written. Errors that still fail the exchange return before this cleanup site.

No public API, timeout, response-completeness rule, cache-admission decision,
or default changed. The inherited core helper and the upstream h2 dependency
remain unchanged.

## Regression coverage

`test_abandoning_an_h2_upload_releases_capacity_to_a_live_sibling_stream`
creates an in-memory H2 connection, assigns its entire connection window to the
first stream, and verifies a second stream cannot obtain capacity. It then
cancels the first stream's reservation while keeping that send handle alive and
proves the sibling immediately receives capacity. Existing integration coverage
continues to assert that the configured-timeout path delivers a qualified
complete response and that timeout without END_STREAM fails.

## Re-evaluation triggers

- h2 changes `reserve_capacity(0)` cancellation or capacity-reclamation
  semantics;
- Pingora moves request-body ownership so a successful abandonment drops the
  `SendStream` before response delivery can continue;
- successful abandonment gains another outcome path that bypasses the shared
  cleanup helper.

## References

- `pingora-proxy/src/proxy_h2_request_body.rs` — successful
  write-abandonment classification and cleanup helper.
- `pingora-proxy/src/proxy_h2_request_body_tests.rs` — physical location of
  the capacity regression test, whose test identity remains under `proxy_h2`.
- [H2 writer stall after response](h2-writer-capacity-stall-after-response.md) —
  the successful-abandonment decision this cleanup preserves.
- [H2 writer stall without END_STREAM](h2-writer-stall-without-end-stream-bounded.md) —
  the failed-exchange mirror case.
- `tasks/issues/h2-configured-timeout-keeps-capacity-reservation.md` — original
  executable finding, removed after project checks pass.
