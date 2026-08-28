---
name: negative-tests-need-out-of-band-observation
description: Use when reviewing a negative test that asserts something did NOT happen on a deliberately failing exchange; covers why the observation channel must not be the failing exchange itself.
status: fixed
finding_id: issue-abort-terminal-eos-test-false-positive
closed: 2026-08-24
---

# A negative test must observe through a channel the failure cannot erase — FIXED

## Conclusion

When a test asserts that some hook was **not** invoked during an exchange the
test itself forces to fail, the invocation must be recorded out of band — a
test-only counter written from inside the hook — not read back off the client's
view of that broken exchange. The test must also prove the exchange reached the
component under test, and must classify the failure it accepts.

Fix suggestions of the form "just replace `unwrap_or_default()` with `unwrap()`"
are **not accepted**: they change which false pass occurs, not that one can.

## Core Rationale

**1. A failing exchange erases the evidence the assertion depends on**

`pingora-proxy/tests/test_terminal_body_dispatch.rs` observes the terminal
`upstream_response_body_filter` dispatch through the client-visible body: the
`x-retain-until-eos` processor appends an `|eos` marker, so the body doubles as
a callback count. That works for the seven positive cases, whose exchanges
complete. It does not work for the aborted case, whose exchange must fail.

Measured on the real stack: an aborted upstream leaves the client with
`Ok((200, Err(Decode/Body/UnexpectedEof)))` — headers arrive, chunked body
collection fails. A regression that dispatched a synthetic end-of-stream would
release `partial|eos` downstream and then break the stream in exactly the same
place, and the client would discard the partial body just the same. Both the
correct implementation and the regression therefore yield no readable body. The
old `response.text().await.unwrap_or_default()` turned that into `""`, and
`assert!(!body.contains("|eos"))` held in both worlds. The test could not fail.

**2. `unwrap()` instead of `unwrap_or_default()` does not fix it**

`text()` returning `Err` is the *expected* outcome of the correct
implementation, so panicking on it turns the correct case red. The problem is
not the error handling; it is that the client's body is the wrong instrument for
this measurement. The fix is a second instrument: an `x-eos-probe` request
header keying a counter that the filter increments on `end_of_stream`, read back
after the request via `take_eos_dispatches()`. That counter survives a failed
body collection because nothing about the broken stream can reach it.

**3. "The exchange failed" is not evidence the code under test ran**

A negative test that accepts any failure also accepts failures that happen
before the component under test is reached. This was observed, not theorized:
the case passed in a run where sibling cases could not bind their configured
`127.0.0.2` source address and got 502 — the proxy never reached the aborting
origin, nothing about terminal dispatch was exercised, and the test was green.
Two guards close this: an `AtomicBool` the origin sets when it accepts the
proxied request (proving the request arrived), and `assert_eq!(status, OK)`
(the origin sends 200 before resetting, so a downstream 502 means the proxy gave
up earlier).

**4. An out-of-band probe needs a positive control, or it rots silently**

Every assertion the aborted case makes on the counter is `== 0`. That is the
same shape as the defect being fixed: if the probe stops recording — the
`record_eos_dispatch` call dropped from the filter, the header name drifting on
one side, the request routed through a proxy service with no probe
(`ExampleProxyCache` on :6148 has none) — the counter returns 0 and the test
passes forever. Mutating `claim_for` proves the wiring is live today, not that
it stays live.

So one *passing* case must also assert the counter is non-zero.
`trailered_response_dispatches_the_terminal_callback_exactly_once` sends a probe
id and asserts `take_eos_dispatches(&probe) == 1` alongside its existing
`|eos`-marker count, in the same request. The two channels then check each
other. Verified by removing the `record_eos_dispatch` call: the aborted case
still passed (0 == 0), the positive control failed (`left: 0, right: 1`).

Generalize: **a counter asserted only against zero is not a test, it is a
constant.** Pair it with a case that asserts the same counter is non-zero.

## Fix Suggestions Not Accepted

- "Replace `unwrap_or_default()` with `unwrap()`" — makes the correct
  implementation fail, because a broken body read is the expected outcome here.
- "Assert the client received exactly the empty string" — same false pass; the
  regression also produces an empty string.
- "Assert on the `Fail to proxy ... status: 502` log line" — that log reports
  pingora's internal disposition after downstream headers already went out; it
  appears in both the correct and the regressed run.
- "Drop the body assertion now that the counter exists" — the counter covers the
  filter contract, the body covers the client-facing half. Both are wanted.

## Scope this test does and does not cover

`claim_for` gives `HttpTask::Failed` two properties. The E2E test covers the
first (`Failed` must not dispatch). It cannot cover the second (`Failed` must
*claim*, so a following `Done` cannot dispatch instead), because the h2 pump
stops at the error and never emits that `Done`. Verified by mutation:

| Mutation | E2E test | `proxy_common` unit test |
|---|---|---|
| `Failed` dispatches (moved into the `Trailer`/`Done` arm) | **FAILS** (`left: 1, right: 0`) | fails |
| `Failed` no longer claims (removed from the claiming arm) | passes — unreachable | **FAILS** (`failed_never_dispatches_and_suppresses_a_following_done`) |

Do not report the second row as a test gap: it is covered at the unit level and
documented as a scope note on the test.

## Re-evaluation Triggers

Re-open this decision only if:

- The pumps start emitting a `Done` after a `Failed`, which would make the
  second mutation E2E-reachable and worth adding here.
- The `x-retain-until-eos` processor stops appending `|eos`, removing the
  client-visible half of the observation.

## Reference Cases

- `../Edgion/tasks/todo/issue-abort-terminal-eos-test-false-positive.md` (2026-08-24),
  from the deep review of `h2-trailer-eos-bypasses-response-body-filter`.
- Test: `pingora-proxy/tests/test_terminal_body_dispatch.rs`
  (`aborted_response_never_dispatches_a_terminal_callback`).
- Harness probe: `pingora-proxy/tests/utils/server_utils.rs`
  (`EOS_PROBES`, `take_eos_dispatches`, `record_eos_dispatch`).
- Parent finding: [../h2-grpc/trailer-done-terminal-body-dispatch.md](../h2-grpc/trailer-done-terminal-body-dispatch.md)
- Sibling rule: [ai-tests-must-discriminate-the-claimed-boundary.md](../../../../Edgion/skills/04-review/testing/ai-tests-must-discriminate-the-claimed-boundary.md)
