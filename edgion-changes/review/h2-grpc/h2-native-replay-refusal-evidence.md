# H2 native replay preserves refusal evidence

## Scope and cause

On 2026-10-08, the Edgion `HTTPRoute/Transport` protocol-retry ceiling case
reproduced a non-retryable `cannot reserve capacity` failure on attempt two.
The selected consumer baseline was `0b0d8bb` with h2 0.4.19; the reproduction
also occurs on published `e18250e` with the H1 close-drain candidate.

The live H2 upload path stops uploading after a non-timeout upstream write
error and continues reading the response. The native replay prelude instead
used `await?`, immediately dropping the response reader. A remote refusal can
close the send half before that reader produces its qualified `REFUSED_STREAM`
error. The generic capacity error is inherited from core; this correction is
at the fork's replay seam and does not change h2 or classify that generic error
as retryable.

## Git provenance

This is an inherited replay error-arbitration gap, not a regression first
introduced by the latest fork commits. Source history shows:

- Upstream release `8797329` (Pingora 0.1.0, 2024-02-27) already returned a
  native replay send error before entering response processing. The later
  asynchronous implementation retained the same early-return structure.
- Upstream `aa7c2f1` (2025-04-24, "Flush already received data if upstream write
  errors") changed the live upload path to stop uploading and drain the
  response, but left the native replay prelude's `await?` unchanged.
- Upstream `8de791c` (authored 2025-09-18, committed 2025-09-26, "Retry on
  REFUSED_STREAM") added remote refusal retry qualification to
  `handle_read_header_error`; it did not reconcile that response-reader proof
  with native replay send errors. This commit is an ancestor of this checkout.
- The fork's current body-relay layer `5a6d22b` (authored 2026-09-02, committed
  2026-09-25) expanded the replay prelude and retained that early return. The
  subsequent barrier layer `c8b3ee8` retained it too. Both the selected
  `0b0d8bb` consumer and published `e18250e` reproduce the failure.

The correction belongs at the fork-owned replay seam: its expanded relay/retry
contract must preserve the response reader's qualified evidence. No dependency
decoder, generic capacity error, or ambiguous-request retry rule is changed.
These are source-history conclusions and current-baseline regression results;
they are not a runtime bisect of every historical dependency version.

Edgion added `transport/retry_policy.rs`, including the refusal ceiling case,
in `c6848316b` on 2026-10-05. Repeated refusals reach native replay; zero initial
stream credit in the new fork regression makes the send-side failure ordering
deterministic. A successful live-upload or single-refusal test does not prove
that this replay branch preserves refusal evidence.

## Correction and boundaries

Only upstream `H2Error` or `WriteError` from an ordinary/streamed replay write
continues to the existing response path. The request reader is marked errored
so no more downstream body is read. The existing response reader remains the
owner of remote refusal classification and its read timeout. Write timeout,
downstream error, application error and `Bodyless` violation fail immediately.
The final frozen replay readiness and downstream response-commit gates remain
unchanged. Ordinary ambiguous POST failures gain no retry permission.

## Fresh verification

The regression `refused_stream_during_native_replay_keeps_protocol_retry_evidence`
uses zero initial H2 stream credit to force the send side to observe each
refusal. Four refusals precede an accepted fifth attempt; only that last attempt
receives the 32-byte body. It fails on the original prelude after two attempts
and passes on the corrected prelude. Its dedicated proxy listener has six
attempts; existing seam listeners retain their two-attempt policy.

The Edgion candidate with this correction passes all 169 Transport cases,
including both non-idempotent POST refusal ceilings and ambiguous POST negative
controls. The final default consumer sweep passed 266 of 275 checks with zero failures
and nine existing skips, including all original dynamic failure groups. The
consumer also corrected its healthy-Redis fixture readiness, static DIU setup
and temporary certificate-directory mismatch. Publication remains pending;
the formal Edgion manifest and lockfile have not changed.

## Re-evaluation triggers

Revisit if request replay stops using the native prelude, response reader error
arbitration changes, or the inherited core capacity error acquires its own
qualified protocol cause. Do not replace protocol proof with a retry of every
closed send stream.
