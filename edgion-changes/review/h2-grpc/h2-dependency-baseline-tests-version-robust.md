---
name: h2-dependency-baseline-tests-version-robust
description: Use when reviewing the unwatched h2 "laundering baseline" tests in the Pingora fork, or any test that asserts a specific upstream h2 behavior; covers why a dependency characterization must accept both sides of an upstream fix and report the change instead of failing.
status: implemented
finding_id: H2-011
closed: 2026-08-28
---

# H2 dependency baselines accept both sides of an upstream fix

## Conclusion

The unwatched baselines in `pingora-core/src/protocols/http/v2/client.rs`
characterize the resolved `h2` release, not a Pingora contract. They accept
every outcome the dependency may produce — the laundered one (`h2` <= 0.4.19
hands an invalid terminal trailer block back as a success), an outright
rejection by an upstream `h2` that refuses it, and that rejection reaching the
direct trailer read as a plain "no trailers" — and print which one they saw via
`report_dependency_baseline`.
Fix suggestions of the form "keep asserting the laundering, it is what h2 does
today" and "just delete the assertion when it starts failing" are **not
accepted**.

## Core rationale

**1. An open dependency bound means the vulnerable behavior is not a contract**

The workspace declares `h2 = ">=0.4.19"` with no upper bound. Any
`cargo update` may resolve an `h2` that rejects trailer pseudo-headers at the
decoder (the fix H2-001 and H2-004 are waiting on). Two tests used to require
the vulnerable behavior:
`h2_unwatched_zero_data_invalid_trailers_are_laundered_baseline` asserted a
clean EOF through `assert_clean_response`, and
`h2_unwatched_direct_trailer_read_launders_invalid_terminal_headers_as_empty`
asserted `read_trailers()` returns `Some(empty)`.

That inverts the sign of CI: the day the dependency becomes safer, the suite
goes red and reads like a Pingora regression. The two obvious ways to clear a
red build then are both wrong — pinning a known-vulnerable `h2` in the default
job, or deleting the assertion and with it the evidence for why the wire
watcher exists at all.

**2. The product contract stays strict; only the characterization is widened**

Widening these baselines does not widen the security contract. The fail-closed
behavior is asserted by the `h2_watched_*` siblings
(`h2_watched_zero_data_invalid_trailers_remain_an_error`,
`h2_watched_invalid_trailers_reset_is_not_a_clean_eof`, and the poll variant),
which use zero-length or short bodies where `EndOfBodyProof::holds()` cannot be
satisfied, so they demand an error under either dependency.

Wherever a baseline observes a body-read error it still requires the trailer
read to keep failing, and in those scenarios it provably must: `holds()` is
false, so `read_trailers`'s `benign_post_eof_stream_end` arm cannot fire.
(Which mechanism closes it is version-specific — the `response_body_error`
latch when the failure came from trailer validation, the proof guard when it
came from `data()` — so the baselines assert the outcome, not the mechanism.)

The one outcome the baselines deliberately do NOT fail on is the direct
trailer read answering `Ok(None)` under a rejecting `h2`: there the declared
`content-length` is satisfied, the same-burst NO_ERROR reset has overwritten
the local PROTOCOL_ERROR, and an unwatched session holds no evidence that a
terminal block was ever sent. Latching that shape is H2-004's deferred work,
already pinned by the `#[ignore]`d
`h2_watched_direct_trailer_read_latches_invalid_terminal_headers`; the baseline
classifies it as `RejectedThenReportedAsNoTrailers` and reports it rather than
filing a known deferred gap as a fresh regression. That arm does not take the
dependency's word for it: it replays the same frames in body-EOF order and
requires the terminal `read_response_body()` to fail, so a Pingora-side
weakening of `read_trailers`'s guards cannot pass itself off as an upstream
fix. What stays asserted in every arm is that the illegal block's fields never
reach the caller.

**3. The behavior change must be visible, not silent**

`report_dependency_baseline` writes a line to stderr naming the scenario and
the observed outcome (`Laundered` / `Rejected` /
`RejectedThenReportedAsNoTrailers`). A migration to a fixed `h2` therefore
shows up in test output for design review — which is what lets H2-001 and
H2-004 be re-opened deliberately — rather than being hidden by a removed
assertion. Neither baseline is `#[ignore]`d, so both run in every job.

Caveat: the standard test harness captures stderr, so the line is printed only
under `--nocapture` or in a failing test's output. Making a CI job surface it
is a workflow change, which H2-009 placed outside this workflow's scope.

## Fix suggestions not accepted

- "Restore the hard laundering assertion, it documents current h2" — it
  documents a vulnerability as a requirement, and blocks the safe upgrade.
- "Delete the baselines; the watched tests already cover the contract" — the
  baselines are the evidence for WHY the watcher is required; without them a
  future reader cannot tell the watcher from dead weight.
- "Pin `h2` to an exact version instead" — the default job would then be
  required to build against a known-vulnerable dependency, which the H2 audit
  explicitly forbids.
- "Assert on the h2 version instead of the observed behavior" — Cargo exposes
  no dependency version to a dependent crate's test code, and behavior is the
  property that actually matters.
- "Make `assert_clean_response` itself tolerant" — it and
  `assert_clean_empty_trailers` are shared by the surrounding tests that assert
  genuine Pingora contracts; widening either would weaken all of them.

## Re-evaluation triggers

Re-open this decision only if:

- Upstream `h2` ships decoder-level rejection of trailer pseudo-headers, at
  which point the reported `Rejected` outcome should drive re-opening H2-001
  and H2-004 and possibly converting these baselines into hard contracts
  against a raised minimum.
- The workspace adopts a pinned or upper-bounded `h2` requirement, making the
  two-sided characterization unnecessary for the pinned leg.

## Reference cases

- The 2026-08-29 minimum/current audit raised the minimum from 0.4.16 to
  0.4.19 after 0.4.16-0.4.18 failed the fork's large-window continuing-upload
  contract. This does not change the two-sided trailer characterization: h2
  0.4.19 still exposes the known decoder limitation, while a future normal
  upstream release may fix it.

- H2-011, whole-change H2 audit 2026-08-26; fixed in the Pingora fork branch
  `edgion_v3` on 2026-08-28.
- `pingora-core/src/protocols/http/v2/client.rs` — `DependencyBaseline`,
  `report_dependency_baseline`, and the two `*_dependency_baseline` tests.
- `Http2Session::read_trailers` and `EndOfBodyProof::holds` in the same file —
  the guards that decide which outcome a rejecting `h2` produces.
- [h2-ci-contract-enforcement-out-of-scope.md](h2-ci-contract-enforcement-out-of-scope.md)
  — H2-009 established that the `h2` minimum/current CI matrix is a CI/CD
  change outside this workflow; this entry covers only the test code.
