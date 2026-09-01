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
- The request-body select arm retains `can_poll()` so natural
  `ReadingFinished` continues to watch downstream disconnects. A pump-local
  latch disables the arm after `Abandoned`; a successful custom idle return is
  ignored and disables the arm without dispatching another terminal event.
- Request-body hook dispatch is separate from custom writer operations.
  `Abandoned` never writes bytes and never calls `BodyWrite::finish()`.
- A non-terminal writer rejection dispatches `Abandoned` before following the
  existing error return path. Filter errors and failures after `Complete` are
  not mislabeled as abandonment.
- The existing downstream protocol owns cleanup: an H1 response committed
  before its request body completes disables connection reuse, while H2 ends
  only the affected stream. Custom writer cleanup and upstream reuse decisions
  remain on their existing paths.

The downstream state transition plus the pump-local polling latch enforces
exactly-once dispatch without removing natural-completion idle monitoring. No
shared state or public API was added, and the H1/H2/custom pumps remain
separate.

## Regression coverage

`pingora-proxy/tests/test_upstream_response_body_sink.rs` covers:

- an H1 chunked POST that sends one chunk without its terminal chunk, receives
  a final custom response within one second, records one or more `Data` events
  followed by `Abandoned`, reaches logging once, and closes the unreusable
  downstream connection;
- the same partial upload over H2, with a subsequent request proving the H2
  connection remains usable;
- a contract-faithful custom downstream whose pending read is cancelled by the
  final response while the opposite custom-message direction remains live; it
  records `Data, Abandoned`, zero post-response body polls, zero writer
  `finish()` calls, and one normal writer cleanup;
- completed H1 and H2 uploads followed by a stalled custom response and H1 FIN
  or H2 reset; both reach logging promptly with one `Complete`, while the H2
  connection remains usable;
- custom writers rejecting the first or a later body write, with exactly one
  `Abandoned` terminal event before the error path; and
- a completed-upload control that records one or more `Data` events followed by
  `Complete` and reuses its H1 connection. H1 parser boundaries may produce
  more than one `Data` callback; the tests intentionally assert the terminal
  invariant rather than an exact non-terminal callback count.

## Fourth reopened review and closure evidence

A fresh whole-stack review found that the H1 early-response and completed-upload
controls still hard-coded an exact `Data` callback count. The same legal empty
H1 callback that made the writer-rejection test flaky could therefore fail
either control. All request-body lifecycle cases now share one helper that
requires exactly one final terminal event of the expected kind and permits only
`Data` before it, without constraining parser-dependent `Data` cardinality.

A fresh read-only subagent reviewed the final protocol-specific data-count
expectations and returned LGTM. H1 chunked cases permit one or more standalone
`Data` callbacks; stable H2/custom early-response fixtures require exactly one;
completed-disconnect controls allow none because the final body bytes can be
delivered with `Complete` itself.

The formerly flaky H1 early-response and completed-upload controls each passed
30 consecutive runs, and writer rejection passed 20 consecutive runs. The full
50-case custom harness and repository verification matrix then passed at
Pingora `ff5f045` plus the working tree correction:

- `cargo fmt --all -- --check` and `git diff --check`;
- both required `cargo check` configurations;
- `pingora-core --lib`: 721 passed, 17 ignored;
- `pingora-core --lib --features connection_filter`: 726 passed, 17 ignored;
- the boringssl PROXY-before-TLS filter: 2 passed;
- `pingora-proxy --lib`: 114 passed;
- `test_request_body_seam`: 54 passed;
- `test_upstream_response_body_sink`: 50 passed;
- `test_terminal_body_dispatch`: 9 passed;
- `test_h2_upstream_no_error_reset`: 8 passed;
- `test_h2_upstream_stalled_after_response`: 3 passed; and
- `test_h2_upstream_cache_and_reuse`: 7 passed.

The four shared-config integration targets used the documented temporary
no-source-bind adaptation because this macOS host lacks the `127.0.0.2`
loopback alias. The tracked configuration was restored unchanged afterward.

## Correction chronology

- The initial closure at Pingora `849adea` used a logging deadline as evidence.
  A deeper review showed that this was not deterministic, so the custom-message
  fixture was changed to observe polling directly and to order the terminal
  response before its linger.
- A broad `is_reading()` guard stopped the unwanted post-response body poll,
  but also removed intentional idle watching after natural completion and for
  initially bodyless requests. The corrected local `can_poll()` latch retained
  idle monitoring without reopening body polling after `Abandoned`.
- The first corrected test treated request-body `Data` callback count as the
  number of writer attempts. H1 may emit a legal empty `Data` callback that the
  custom pump suppresses before the writer, so writer attempts gained a separate
  observation.
- The final correction at Pingora `ff5f045` plus `45d8375` moved all
  lifecycle cases to one terminal-event helper. It requires exactly one final
  event of the expected kind and permits only `Data` before it, while allowing
  parser-dependent H1 `Data` cardinality. The formerly flaky H1
  early-response and completed-upload controls passed 30 consecutive runs,
  writer rejection passed 20, and the 50-case custom harness plus the project
  matrix passed.

The fourth-review matrix above is the canonical closure evidence. Earlier full
matrices were superseded by these corrections and are intentionally not
repeated. The separately owned writer-root-cause preservation fix was committed
as Pingora `4dd9ce2` and remains documented in
[custom-writer-error-context.md](custom-writer-error-context.md).
## Revisit triggers

- The custom pump gains support for request-body `Terminate`, non-`Ordinary`
  dispositions, or replay that changes terminal-event ownership.
- `BodyWrite` gains a typed stopped-receiving outcome distinct from a generic
  write error.
- Custom-message lifetime becomes independent of the HTTP exchange and changes
  which reader cancellation is safe after a final response.
