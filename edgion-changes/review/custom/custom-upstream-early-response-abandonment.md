---
name: custom-upstream-early-response-abandonment
description: The custom upstream pump stops unfinished uploads with exactly one Abandoned event after a final response or mid-upload writer rejection, without calling BodyWrite::finish for abandonment.
status: fixed
---

# Custom upstream early response abandons the request body

## Classification

Fork-owned correctness defect introduced by `600ac49`. Medium for public fork
users that install a custom connector; unreachable in the reviewed Edgion
checkout (`83408c11`), which registers no custom connector. The Edgion lockfile
still selects fork commit `57f6183`, so the sibling checkout is not deployment
evidence.

## Root cause

`custom_bidirection_down_to_up` kept its downstream read side alive after the
response state became complete. `send_body_to_custom` derived
`RequestBodyEvent` from a boolean and could therefore produce only `Data` or
`Complete`. A custom `BodyWrite` rejection moved the downstream state directly
to `Errored`; neither path could dispatch `Abandoned`.

This drift was specific to the fork's custom pump. The H1 and H2 pumps already
implemented the public contract that an intentionally unfinished downstream
upload receives exactly one terminal `Abandoned` event.

## Resolution

- `send_body_to_custom` accepts an explicit `RequestBodyEvent`.
- Once a final custom response is complete while the downstream state is still
  reading, the pump dispatches `Abandoned`, marks the read side finished, and
  cancels the no-longer-useful downstream custom-message reader.
- The request-body select arm is guarded by `is_reading()`, not the broader
  `can_poll()`: `ReadingFinished` permits protocol-idle polling elsewhere but
  must remain a terminal latch for request-body hook dispatch.
- Request-body hook dispatch is separate from custom writer operations.
  `Abandoned` never writes bytes and never calls `BodyWrite::finish()`.
- A non-terminal writer rejection dispatches `Abandoned` before following the
  existing error return path. Filter errors and failures after `Complete` are
  not mislabeled as abandonment.
- The existing downstream protocol owns cleanup: an H1 response committed
  before its request body completes disables connection reuse, while H2 ends
  only the affected stream. Custom writer cleanup and upstream reuse decisions
  remain on their existing paths.

The downstream state transition plus the request-body branch guard is the
exactly-once latch; no new shared state or public API was added, and the
H1/H2/custom pumps remain separate.

## Regression coverage

`pingora-proxy/tests/test_upstream_response_body_sink.rs` covers:

- an H1 chunked POST that sends one chunk without its terminal chunk, receives
  a final custom response within one second, records `Data, Abandoned`, reaches
  logging once, and closes the unreusable downstream connection;
- the same partial upload over H2, with a subsequent request proving the H2
  connection remains usable;
- a custom downstream whose next body read becomes ready only after the final
  response, while the opposite custom-message direction remains live; it
  records `Data, Abandoned`, one completed body read, zero writer `finish()`
  calls, and one normal writer cleanup;
- custom writers rejecting the first or a later body write, with exactly one
  `Abandoned` terminal event before the error path; and
- a completed-upload control that records `Data, Complete` and reuses its H1
  connection.

## Reopened review and closure evidence

A deeper independent review invalidated the initial closure evidence: the H1
and H2 downstream tests could not exercise the broader `can_poll()` behavior
of a custom downstream. The new custom-to-custom regression fails against the
old guard by retaining the request lifecycle past its one-second logging
deadline and passes with the corrected guard.

Fresh independent review initially found a low-risk timing window in the new
fixture. The custom-message stream was changed to wait deterministically for
the terminal response before its 100ms linger; the reviewer then returned
LGTM after checking the latest code and running the focused case five times.

Repository checks passed on 2026-08-28 at Pingora `849adea` plus the working
tree fix:

- `cargo fmt --all -- --check` and `git diff --check`;
- both required `cargo check` configurations;
- `pingora-core --lib`: 721 passed, 17 ignored;
- `pingora-core --lib --features connection_filter`: 726 passed, 17 ignored;
- the boringssl PROXY-before-TLS filter: 2 passed;
- `pingora-proxy --lib`: 114 passed;
- `test_request_body_seam`: 54 passed;
- `test_upstream_response_body_sink`: 48 passed;
- `test_terminal_body_dispatch`: 9 passed in the required isolated rerun after
  a parallel fixed-port collision;
- `test_h2_upstream_no_error_reset`: 8 passed;
- `test_h2_upstream_stalled_after_response`: 3 passed; and
- `test_h2_upstream_cache_and_reuse`: 7 passed.

The original writer error context is tracked separately by
`pending-issues/custom-writer-error-context.md`; it was not changed as part of
this terminal-latch correction.

## Superseded initial closure evidence

Independent review: LGTM, with all helper callers, state transitions, writer
failure paths, custom-message cleanup, and H1/H2 reuse semantics checked.

Repository checks passed on 2026-08-28 at Pingora `849adea` plus the working
tree fix:

- `cargo fmt --all -- --check` and `git diff --check`;
- both required `cargo check` configurations;
- `pingora-core --lib`: 721 passed, 17 ignored;
- `pingora-core --lib --features connection_filter`: 726 passed, 17 ignored;
- the boringssl PROXY-before-TLS filter: 2 passed;
- `pingora-proxy --lib`: 114 passed;
- `test_request_body_seam`: 54 passed;
- `test_upstream_response_body_sink`: 47 passed;
- `test_terminal_body_dispatch`: 9 passed;
- `test_h2_upstream_no_error_reset`: 8 passed;
- `test_h2_upstream_stalled_after_response`: 3 passed; and
- `test_h2_upstream_cache_and_reuse`: 7 passed.

## Revisit triggers

- The custom pump gains support for request-body `Terminate`, non-`Ordinary`
  dispositions, or replay that changes terminal-event ownership.
- `BodyWrite` gains a typed stopped-receiving outcome distinct from a generic
  write error.
- Custom-message lifetime becomes independent of the HTTP exchange and changes
  which reader cancellation is safe after a final response.
