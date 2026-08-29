# Pending issue index

This directory owns actionable unfinished work. A durable review rationale may
live under `../review/`, but the pending issue is canonical for current status,
next action, and closure evidence.

## Current work

| Issue | Status | Ownership | Related review |
| --- | --- | --- | --- |
| [Terminal HEADERS completion](h2-terminal-headers-completion.md) | Deferred/blocked | Upstream decoder, then fork integration | [Upstream limitation](../review/upstream-limitations.md) |
| [H2 trailer validation](h2-trailer-validation.md) | Deferred upstream | Upstream decoder, then fork integration | [Upstream limitation](../review/upstream-limitations.md) |
| [Non-streaming cache trailer completion](non-streaming-cache-trailer-completion.md) | Open | Fork | Discovered while closing [trailer filter parity](../review/response-trailer-filter-error-parity.md) |
| [Custom conditional-filter gate](custom-conditional-filter-gate.md) | Open investigation | Fork custom response pump | Preserved explicitly by the shared response pipeline |

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
