# Request-body transport and bidirectional pumps

## Event model

`RequestBodyEvent` replaces a bare end-of-stream boolean:

- `Data`: a non-terminal body event.
- `Complete`: the downstream body ended normally.
- `Abandoned`: the proxy stopped reading because the upstream exchange ended
  or can no longer consume it.

Modules and application hooks see the same event classification. Every
request gets at most one terminal event, including bodyless requests and
upstreams that stop receiving mid-upload.

`RequestBodyAction::Terminate` stops the request as an application-selected
outcome. It bypasses retry classification and generic `fail_to_proxy` response
generation because the application owns the downstream response.

## Upstream framing

`UpstreamRequestBodyDisposition` controls framing after application request
header filtering:

- `Ordinary`: preserve normal inference.
- `Bodyless`: remove H1 body framing or close the H2 request stream. Real body
  bytes arriving later are an application-contract violation and fail closed.
- `Streamed`: the final size is unknown; H1 uses chunked framing and H2 keeps
  the stream open.

Unsafe combinations are coerced to `Ordinary`, including upgrades, CONNECT,
truly bodyless requests and HTTP versions that cannot represent the selected
framing.

## Pump rules

- H1 and H2 pumps read downstream uploads and upstream responses concurrently.
- The main branch's batch processing and downstream proxy-task backpressure
  remain authoritative; Edgion filtering state is passed through the shared
  batch helpers rather than duplicating inline loops.
- When an upstream response completes and the upstream stops receiving, the
  application still receives one `Abandoned` event.
- The custom pump dispatches that event independently from its `BodyWrite`:
  abandonment never calls `finish()`, because a deliberately truncated upload
  is not a clean custom-upstream request EOS. A mid-upload writer rejection
  dispatches the same terminal event before the error return path. The pump
  preserves that first upstream-classified writer error through joined-future
  teardown; an error raised while dispatching the subsequent `Abandoned` event
  is secondary and cannot replace the writer type, source, or context.
- Natural request-body completion retains the downstream idle/disconnect
  watcher while a custom upstream response is pending. Only application
  abandonment disables that watcher; an unexpected successful custom idle
  return never manufactures a second terminal event.
- H1 downstream connections with unread body state are not reused. H2 keeps
  the connection and ends only the affected stream.
- A final response already committed downstream disables retries even when an
  error would otherwise be retryable. `request_retry_allowed` is an additional
  application gate for retry buffering and attempts.

## Tests

`pingora-proxy/tests/test_request_body_seam.rs` is the primary matrix. It
covers H1 and H2 downstream/upstream combinations, framing, retry, GOAWAY,
termination, bodyless contract violations and connection reuse.

`pingora-proxy/tests/test_upstream_response_body_sink.rs` owns the custom
connector harness. It covers early final responses and writer rejection with
unfinished H1/H2 downstream uploads, exactly-once `Abandoned` delivery,
first-error classification/context preservation (including a simultaneous
`Abandoned` hook failure), protocol-specific cleanup, a completed-upload
`Complete` control, and a
custom-downstream case that keeps the opposite custom-message direction alive
to prove abandonment stops further request-body polling. Completed-upload H1
FIN and H2 reset controls prove natural completion still watches downstream
disconnects while the custom upstream response is stalled.
