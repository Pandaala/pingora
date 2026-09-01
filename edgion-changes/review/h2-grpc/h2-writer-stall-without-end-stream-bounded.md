---
name: h2-writer-stall-without-end-stream-bounded
description: Use when reviewing an H2 upstream that withholds request-body flow-control capacity and response END_STREAM, especially when PeerOptions.write_timeout is None.
status: implemented
closed: 2026-08-29
---

# H2 request-body write stall without response END_STREAM

## Conclusion

Every H2 upstream request-body capacity wait has a finite progress bound. An
explicit `PeerOptions::write_timeout` wins unchanged; when it is `None`, the H2
proxy pump applies a ten-second protocol-local floor. The floor bounds one
capacity grant and is re-armed after progress, so it is not a whole-upload or
whole-exchange deadline.

Expiry without qualified response END_STREAM fails the exchange and sends
`RST_STREAM(CANCEL)`. It never returns `UpstreamDoneReceiving`, never invents
clean EOS, never admits a complete cache entry, and cannot retry after a final
response has already been committed. Dropping/resetting the failed stream
releases its reservation while leaving a healthy H2 connection reusable.

Qualified response END_STREAM remains a different case governed by H2-007:
the same write timeout then authorizes abandoning the unfinished upload and
delivering the response whose completeness is independently decided by the
read half.

## Policy choice

The floor belongs in the fork's H2 request pump, not in `PeerOptions::new()`:
the unbounded inline `poll_capacity` wait and its resource amplification are
generic H2 behavior, while changing the public peer default would also alter
H1 and custom-upstream semantics. Edgion's normal timeout fold still supplies
60 seconds and therefore bypasses the floor; explicit shorter and longer
finite values are also preserved. When Edgion disables every product deadline
with `0s`, the peer option stays unset but this protocol-level liveness floor
still applies.

Rejected alternatives:

- a whole-exchange deadline, which would affect legitimate long-lived reads,
  retries, backoff and every proxy pump;
- borrowing `read_timeout`, which conflates independent operations and still
  gives no answer when all peer deadlines are unset;
- changing only Edgion's timeout fold, which leaves other Pingora consumers
  vulnerable and assigns generic protocol liveness to the product layer;
- treating timeout without response END_STREAM as successful abandonment,
  which would launder an incomplete response.

## Evidence

- `proxy_h2::test_h2_write_timeout_floor_only_fills_an_unconfigured_timeout`
  pins the default and explicit shorter/longer precedence.
- `test_h2_upstream_stalled_after_response` covers bounded failure without
  response END_STREAM or configured deadlines and preserves both qualified
  END_STREAM success paths.
- `test_h2_upstream_cache_and_reuse::h2_unterminated_stall_fails_without_cache_or_capacity_leak`
  proves bounded failure without retry, no complete cache entry, a later cache
  miss, and reuse of the same H2 connection after stream capacity is released.
  The proxy unit test `retry_guard_tests::final_statuses_forbid_retry`
  independently pins the committed-final-response retry gate.

## Cross-repository consumption

The contract was reviewed against Edgion checkout `83408c11`. Its normal
timeout fold still resolves the omitted `backendRequest` through the 60-second
global default; the focused
`backend_bound_unset_falls_back_to_global` test passed. The checkout's
`Cargo.lock` currently selects Pingora `57f6183c`, not the local Pingora
checkout `ca45864d`, so that test confirms Edgion's configuration contract but
does not claim that the locked/deployed dependency already contains this fix.
The normal revision adoption workflow must move the selected fork revision
before deployment.

## Closure verification

The complete repository verification matrix passed on 2026-08-29 at Pingora
`ca45864d`, including formatting, both build configurations, all core/proxy unit
tests, the request-body seam, response sink, terminal dispatch, H2 reset,
writer-stall and cache/reuse integration targets, and `git diff --check`.
Independent read-only review returned LGTM after its two test/documentation
findings were corrected.

## Re-evaluation triggers

- Upstream `h2` exposes a reliable public signal that no future capacity will
  be granted.
- Upstream Pingora introduces an equivalent finite request-write policy.
- The proxy stops awaiting request-body writes inline, requiring the liveness
  and response-delivery interaction to be re-derived.

## References

- `pingora-proxy/src/proxy_h2_request_body.rs`
- `pingora-proxy/tests/test_h2_upstream_stalled_after_response.rs`
- `pingora-proxy/tests/test_h2_upstream_cache_and_reuse.rs`
- [`h2-writer-capacity-stall-after-response.md`](h2-writer-capacity-stall-after-response.md)
