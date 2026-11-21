# Pending issue index

This directory owns actionable unfinished work. A durable review rationale may
live under `../review/`, but the pending issue is canonical for current status,
next action, and closure evidence.

## Current actionable work

| Issue | Status | Ownership | Related review |
| --- | --- | --- | --- |
| [Terminal HEADERS completion](h2-terminal-headers-completion.md) | Deferred/blocked | Upstream decoder, then fork integration | [Upstream limitation](../review/upstream-limitations.md) |
| [H2 trailer validation](h2-trailer-validation.md) | Deferred upstream | Upstream decoder, then fork integration | [Upstream limitation](../review/upstream-limitations.md) |
| [Non-streaming cache trailer completion](non-streaming-cache-trailer-completion.md) | Open | Fork | Discovered while closing [trailer filter parity](../review/response-trailer-filter-error-parity.md) |
| [Custom conditional-filter gate](custom-conditional-filter-gate.md) | Open investigation | Fork custom response pump | Preserved explicitly by the shared response pipeline |

## 与官方 main 的重叠项

2026-11-21 对照官方 `3f87e92`、本地 `edgion_v0.9.1` 的 `e5f4287` 及当时暂存的连接准入改动。工作分支为 `edgion_v0.9.1_main`。

| Ticket | 状态 | 归属 |
| --- | --- | --- |
| [03 路由前请求体缓冲与重试重放](upstream-main/03-request-body-replay.md) | open | cross-repository |
| [05 Custom 上传失败后的提前响应](upstream-main/05-custom-early-response.md) | open | fork |
| [06 Custom 未完成响应的会话清理](upstream-main/06-custom-session-cleanup.md) | open | fork |

官方 0.9.1 已带入的八项发布线改动无需再移植。上游缓存断开监测与本地缓存字节顺序属于不同契约，其与 custom 会话清理的交点见 06。

## Resolved history

| Issue | Closure | Ownership | Canonical records |
| --- | --- | --- | --- |
| [Default retry-policy test baseline](default-retry-policy-test-baseline.md) | Test fixtures corrected in local commit `9638a37`; production gates unchanged | Fork tests / request relay | [Verification](../verification/test-matrix.md) |
| [Bounded response-head commit barrier](response-head-commit-barrier.md) | Pingora mechanism committed as `af9e1ac`; Edgion production consumer committed as `f31d016`; recorded local-source matrix passed | Cross-repository | [Feature](../features/response-head-commit-barrier.md), [design](../review/response-head-commit-barrier-design.md), [current architecture](../architecture/body-relay.md) |

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
