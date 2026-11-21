---
name: custom-writer-error-context
description: The custom upstream pump preserves the first request-body writer error while resolving early response delivery and joined-future teardown.
status: fixed
---

# Custom request-body writer error context

## Classification

Resolved fork-owned low-severity correctness and observability defect. The
affected public custom-connector path is not registered by the reviewed Edgion
checkout (`83408c11`); its lockfile selects fork revision `57f6183`, so the
sibling checkout is consumer-contract evidence rather than deployment
evidence.

## Root cause

`send_body_to_custom` classified a custom request-body writer failure as an
upstream error, but `custom_bidirection_down_to_up` reduced it to the
downstream state machine's generic errored state. The pump later returned an
unset-source `WriteError` with `downstream_state is_errored` context, losing the
writer's original type, source, and diagnostic marker.

The first correction stored the writer error only after the asynchronous
`Abandoned` hook returned. Independent review found that `tokio::try_join!`
could cancel that future while the hook was pending if a response or
custom-message sibling failed first, allowing the teardown error to replace the
writer root cause.

## Resolution

- The root request-body path error lives outside the joined futures. Issue 05
  adopts the completed-response exception from upstream `7de472d`: a failed
  upload writer can coexist with a successfully delivered response. A partial
  response or failed response reader still reports the saved upload error.
- A writer failure is converted to `Upstream` and placed in that durable slot
  before the next `await`. The rejection does not dispatch a new request-body
  event. `Complete` may already have been delivered before a failing terminal
  write or finish; an unfinished sequence can end with `Data`.
- A rejected writer receives no further writes, finish, or cleanup. The pump
  may still deliver the response. Ordinary downstream framing can establish
  completion without waiting for the custom reader to return from a stalled
  body or trailer read. Cache admission, upgrade half-close, and application
  termination have distinct completion conditions. A failed writer makes its
  upstream session ineligible for reuse even when delivery succeeds.
- A transport failure in `BodyWrite::finish` also rejects the upload. A
  request filter failure is an application failure and cannot be cleared by a
  successful response.

No public API changed.

## Regression and review evidence

For upstream-main issue 05, the custom transport tests exercise completed
responses after first/later writes and finish failures, truncated responses,
a blocked writer with a header-read failure, and upgraded half-close. They
check the delivered bytes, request-body events, logged outcome, and retirement
of the failed writer. The pump unit tests also exercise replay from the native
retry buffer and distinguish downstream framing from active cache admission.

### Historical verification

Before issue 05, `pingora-proxy/tests/test_upstream_response_body_sink.rs` covered first-write
and later-write rejection, an immediate secondary `Abandoned` hook failure, and
a deterministic cancellation race. The race held the hook pending, emitted a
separately marked custom response `ReadError`, and verified that final logging
observed the original
`WriteError`, `Upstream`, and writer context.

Reverse validation against the pre-fix path produced the old unset-source
`downstream_state is_errored` error. The new cancellation case also failed
before its final correction with the competing `ReadError`, then passed after
the error-slot write moved before the hook await. A fresh read-only subagent
reviewed both iterations, identified the cancellation window, and returned
LGTM after the correction.

The full verification matrix passed on 2026-08-29 at `bd89d47` plus the working
tree changes:

- `cargo fmt --all -- --check`, both required `cargo check` configurations,
  and `git diff --check`;
- `pingora-core --lib`: 737 passed, 17 ignored;
- `pingora-core --lib --features connection_filter`: 742 passed, 17 ignored;
- the boringssl PROXY-before-TLS tests: 2 passed;
- `pingora-proxy --lib`: 119 passed;
- `test_request_body_seam`: 54 passed;
- `test_upstream_response_body_sink`: 57 passed;
- `test_terminal_body_dispatch`: 26 passed;
- `test_h2_upstream_no_error_reset`: 8 passed;
- `test_h2_upstream_stalled_after_response`: 4 passed; and
- `test_h2_upstream_cache_and_reuse`: 8 passed.

## Revisit triggers

- Custom request-body work stops being coordinated by the current joined
  futures or gains a typed first-error container.
- Retry buffering for custom upstreams becomes supported through a different
  replay path.
- Edgion begins registering a custom connector and adds product-specific error
  handling that depends on this classification.
