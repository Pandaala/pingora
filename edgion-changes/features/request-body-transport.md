# Request-body transport and bidirectional pumps

For the request-scoped plan, source/backing separation, shared event sequence,
and cross-repository ownership surrounding this contract, see the canonical
[body relay architecture](../architecture/body-relay.md#request-lane).

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
live downstream delivery sequence gets at most one terminal event, including
bodyless requests and upstreams that stop receiving mid-upload. A retry can
replay a separate `Data`/`Complete` sequence through the same application hook;
that replay completion does not change the original downstream completion
cause. The request-trailer hook has a different lifetime and remains at most
once across all attempts for one downstream request.

`RequestBodyAction::Terminate` stops the request as an application-selected
outcome. It bypasses retry classification and generic `fail_to_proxy` response
generation because the application owns the downstream response.

## Upstream framing

`ProxyHttp::request_relay_plan` selects one request-scoped
`RequestRelayPlan` after `proxy_upstream_filter` accepts upstream proxying and
before the retry loop starts. The plan is frozen exactly once and combines an
`UpstreamRequestBodyDisposition` with `RequestReplayPolicy`:

- `Ordinary`: preserve normal inference.
- `Bodyless`: remove H1 body framing or close the H2 request stream. Real body
  bytes arriving later are an application-contract violation and fail closed.
- `Streamed`: the final size is unknown; H1 uses chunked framing and H2 keeps
  the stream open.

Strictly bodyless requests keep the benign coercion to `Ordinary`. `Bodyless`
also keeps the compatibility coercion for upgrade/CONNECT and pre-HTTP/1.1
requests. A frozen `Streamed` plan instead fails closed before writing an
upstream header when either side is upgrade/CONNECT or the final H1 request is
below HTTP/1.1: an installed length-changing processor must not remain active
under ordinary or tunnel framing after an attempt-local rewrite.

The application never selects the body source. Pingora derives live downstream
versus registered replay when the plan freezes, then locks H1/H2 request-body
configuration so an attempt-local `upstream_request_filter` cannot replace the
source. Effective framing remains per attempt and is resolved only after that
filter and registered replay activation. A `Streamed` plan must select
`RequestReplayPolicy::Never`; contradictory plans fail closed.

`RequestRelayRetryState` combines the frozen structural policy with live core
facts and distinguishes live-unread, native capture, native truncation,
registered replay, registered-unavailable, disabled, and unsupported states.
Only this state controls native-buffer allocation and the structural retry
gate. A particular error remains dynamic: `fail_to_connect`,
`error_while_proxy`, deadlines, retry budgets and response commit update or
constrain `Error::retry` independently. Pingora also assigns a canonical
one-based `RequestAttemptId` before every call to `upstream_peer`; Edgion uses
it to reset retry-visible body observers instead of its product-specific
backend/AI counter. Connect failures consult the relay gate before consuming an
ordinary retry budget or advancing AI successor selection, while still
settling the failed current AI predispatch reservation.

## Pump rules

- H1 and H2 pumps read downstream uploads and upstream responses concurrently.
- `pingora-proxy/src/request_relay.rs` owns the shared per-event semantic
  sequence: source EOF normalization, the capability-gated request-trailer
  hook, downstream modules, and the application body-action hook. It returns
  the same `Bytes` owner and typed action to the pump without performing I/O.
  H1/H2/custom keep their existing capability differences: custom does not
  dispatch the trailer hook and request-body termination remains fail-closed.
- Pipe/capacity reservation, empty-output suppression, post-filter `Bodyless`
  validation, task/frame construction, timeouts, reset, retry, early-response
  cleanup, and connection reuse remain in the protocol pumps. In particular,
  the H1 pump still acquires its permit before awaiting the relay, so the
  extraction does not weaken backpressure.
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
  error would otherwise be retryable. The frozen replay policy and current
  backing readiness are applied at the same final retry gate, and a veto is
  reflected on the final error handed to logging and `fail_to_proxy`.
- A custom downstream advertises native retry-buffer capability explicitly.
  Unsupported sessions never enter optional placeholder methods; connect or
  header failures may still retry while the live source is unread, but an
  attempt that reaches body pumping becomes structurally non-retryable.

## Tests

`pingora-proxy/tests/test_request_body_seam.rs` is the primary matrix. It
covers H1 and H2 downstream/upstream combinations, framing, retry, GOAWAY,
termination, bodyless contract violations and connection reuse.

`pingora-proxy/src/request_relay_tests.rs` directly characterizes the shared
semantic seam across H1/H2/custom: `Data(None)` normalization, `Abandoned`,
module-before-application ordering and mutation visibility, zero-copy `Bytes`
handoff, typed versus fail-closed termination, custom trailer capability,
trailer ordering/latching, hook error, cancellation before latch commit,
single-freeze/source locking, replay-state derivation, streamed-plan
validation, canonical attempt identity, and unsupported custom buffering.
Its ignored release microbenchmark compares the extracted Data-event path with
the exact former inline sequence in the same binary and allocation counter.

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
