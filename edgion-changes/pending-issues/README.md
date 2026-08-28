# Pending issue index

This directory owns actionable unfinished work. A durable review rationale may
live under `../review/`, but the pending issue is canonical for current status,
next action, and closure evidence.

## Current work

| Issue | Status | Ownership | Related review |
| --- | --- | --- | --- |
| [Async response filter fast path](async-response-body-filter-fast-path.md) | Open performance investigation | Fork | [Finding 007](../review/findings/007-low-async-response-filter-adds-per-chunk-boxing.md) |
| [Custom upstream early-response abandonment](custom-upstream-early-response-abandonment.md) | Open correctness issue | Fork; currently unreachable in Edgion | [2026-08-28 audit](../review/fork-feature-malformed-input-audit-2026-08-28.md) |
| [H2 upload stall without END_STREAM](h2-request-body-stall-without-end-stream.md) | Open policy/design issue | Fork at an upstream boundary | [H2-007](../review/h2-grpc/h2-writer-capacity-stall-after-response.md) |
| [Terminal HEADERS completion](h2-terminal-headers-completion.md) | Deferred/blocked | Upstream decoder, then fork integration | [Upstream limitation](../review/upstream-limitations.md) |
| [H2 trailer validation](h2-trailer-validation.md) | Deferred upstream plus fork evidence maintenance | Split upstream/fork | [Upstream limitation](../review/upstream-limitations.md) |
| [ResponseBodySink chunk count](../review/findings/003-medium-response-body-sink-does-not-bound-chunk-count.md) | Open review finding | Fork | No implementation task yet |
| [Watcher dependency evidence](../review/findings/004-medium-end-stream-watch-is-not-pinned-to-an-audited-h2.md) | Open; exact-pin proposal superseded | Fork docs/tests; accepted upstream range | [H2-011](../review/h2-grpc/h2-dependency-baseline-tests-version-robust.md) |
| [TLS bind-test classification](../review/findings/008-low-tls-feature-bind-test-rejects-observed-bind-error.md) | Open review finding | Fork | No implementation task yet |

Finding 007 and the async fast-path issue describe the same work. The pending
issue owns progress; the finding preserves discovery. Do not maintain two
independent status narratives.

## Required issue fields

New records should include:

- stable title/ID and `open`, `blocked`, `deferred`, `resolved`, or `wont-fix`;
- severity and `upstream`, `fork`, `Edgion`, or `cross-repository` ownership;
- origin/date and affected commit or dependency baseline;
- reproducible problem, impact, and evidence;
- decision or next action;
- required tests and closure evidence;
- revisit trigger for blocked/deferred/wont-fix work;
- source, commit, review record, and `../Edgion` links.

An ignored or merely written test is not closure evidence. Close only when the
fix/decision is recorded, relevant executable checks pass, and any required
cross-repository contract is verified.

## Triage rules

1. Search `../review/` and this directory first.
2. Separate upstream root cause from fork-owned mitigation, docs, tests, and
   unsafe amplification.
3. Keep one canonical action record and link historical findings.
4. On resolution, record commit/tests; preserve rationale in review only when
   it prevents future duplicate findings.
