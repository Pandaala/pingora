# Request-body transport and bidirectional pumps

## HTTP/1 transfer-coding admission

H1 transfer-coding is validated in two layers. Before `HttpProxy` exists, the
generic H1 reader rejects any HTTP/1.0 `Transfer-Encoding` and any HTTP/1.1 form
whose final coding token is missing or is not `chunked`; those cases receive
400 and the connection closes. Other unusual forms can pass this generic
framing check when their final token is `chunked` and are then governed by the
stricter proxy admission below. A request that otherwise passes generic framing
validation can also receive 405 and close at the default-disabled CONNECT gate.

Of the external HTTP/1 requests that reach `HttpProxy`, only these forms are
forwardable:

- no `Transfer-Encoding`; or
- one field whose value, after trimming optional whitespace, is exactly
  `chunked`, case-insensitively.

A core-accepted final-`chunked` form that is not that single exact field—for
example `gzip, chunked`, duplicate `chunked`, or repeated fields—is submitted
as `HTTPStatus(501)` to the normal `fail_to_proxy` error-rendering path. The
default renderer and the reviewed Edgion renderer write 501. A custom
`fail_to_proxy` implementation owns its emitted response, however, so the
framework contract is the submitted error type and forced close, not a
guarantee that every application writes status 501.

`Transfer-Encoding` combined with `Content-Length` is a separate generic H1
rule: the reader removes `Content-Length` and initially disables keepalive
before this admission check. If the remaining transfer coding is the single
accepted `chunked` field, the request continues through ordinary application
hooks rather than the unsupported-coding `ForceClose` path. The default
behavior leaves that connection closed. Edgion's early request hook likewise
treats its configured timeout only as a refinement of a session that Pingora
still considers reusable, preserving a core-disabled state such as this one or
HTTP/1.0 without explicit keepalive. A generic application retains its normal
mutable `Session` control and can explicitly change keepalive; there is no
framework-level final reassert or direct proxy reuse veto for this accepted
case. Any other transfer coding takes the applicable 400 or forced proxy error
path described above.

This check runs before the mutable `on_connection_reuse` hook,
`early_request_filter`, downstream modules, `request_filter`, cache lookup,
upstream selection, buffering, retry, and all upstream pumps. The rejected H1
connection is marked non-reusable both before the normal
`fail_to_proxy` path and finally after all mutable error hooks, including
`logging`, return. The proxy control flow also independently vetoes returning a
reusable downstream session rather than trusting a renderer's
`can_reuse_downstream` result. Neither a reuse hook, custom renderer, nor logger
can therefore hide the external field or re-enable a connection whose unread
coded bytes could be interpreted as a later request. H2, subrequest,
custom-session, and generic non-proxy H1 server contracts are unchanged.

The restriction is necessary because Pingora's downstream H1 reader removes
the final chunk framing but does not decode earlier transfer codings. Standard
proxy sanitization removes the complete hop-by-hop `Transfer-Encoding` field;
forwarding the remaining coded bytes would therefore change their declared
semantics. This fork follows Envoy's default validator strategy and fail-closed
principle instead of carrying a transfer-coding decoder or preserving unsafe
hop-by-hop metadata. The implementations are not claimed to be fully
behavior-equivalent: their parser layering, emitted status paths, and
`Transfer-Encoding` plus `Content-Length` handling differ.
Application-synthesized upstream metadata remains governed by the existing
`UpstreamRequestBodyDisposition` contract.

The durable ownership, baseline, Envoy comparison, Edgion consumer review, and
upstream revisit trigger are recorded in
[`h1-unsupported-request-transfer-coding-fail-closed.md`](../review/http1/h1-unsupported-request-transfer-coding-fail-closed.md).

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

Its H1 transfer-coding cases send a real gzip member under
`gzip, chunked`, assert a 501 and downstream close, and prove via independent
origin recorders that neither the rejected request nor a pipelined follow-up
dials the intended H1/H2 origin. Plain `chunked` controls prove both upstream
protocols still receive the body intact. A same-connection regression first
persists application context and then proves a mutable reuse hook cannot remove
the external field before admission. Another test gives both a custom error
renderer and logger mutable session access, makes both explicitly re-enable
keepalive while the renderer returns `can_reuse_downstream=true`, then sends a
same-socket follow-up only after the complete first response. That sequencing
distinguishes the proxy's forced-close decision from the generic H1 layer's
independent rejection of pre-buffered pipelining. The proxy-local table test
owns case/OWS, multiple-field, duplicate, empty, deflate, and unknown variants.

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
