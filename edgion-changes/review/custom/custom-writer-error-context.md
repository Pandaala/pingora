---
name: custom-writer-error-context
description: The custom upstream pump preserves the first request-body writer error across asynchronous abandonment and joined-future teardown.
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

- The root request-body path error lives outside the joined futures and is
  returned before any `try_join!` result.
- A writer failure is converted to `Upstream` and placed in that durable slot
  before the next `await`.
- The required `Abandoned` hook still runs exactly once. Its immediate failure
  is logged as secondary; if it remains pending and a sibling fails, future
  cancellation cannot erase the already stored writer error.
- A private upstream `WriteError` marker only stops the body state machine.
  All three private call sites, including retry-buffer replay, pass through the
  outer root-error priority check, so the marker cannot reach request logging.
- Non-writer filter, completion, finish, early-response, and custom-message
  errors keep their existing propagation behavior.

No public API changed.

## Regression and review evidence

`pingora-proxy/tests/test_upstream_response_body_sink.rs` now covers first-write
and later-write rejection, an immediate secondary `Abandoned` hook failure, and
a deterministic cancellation race. The race holds the hook pending, emits a
separately marked custom response `ReadError`, proves both secondary events
occurred, and still requires final logging to observe the original
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
