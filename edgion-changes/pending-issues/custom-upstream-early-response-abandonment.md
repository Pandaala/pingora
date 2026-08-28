# Custom upstream early response misses request-body abandonment

Status: open.

Severity: medium for the public Pingora fork; currently unreachable in Edgion.

Ownership: fork. Introduced when `600ac49` connected the common
`RequestBodyEvent` contract to the pre-existing custom upstream pump without
adding the pump's early-response abandonment transition.

## Problem

When a custom upstream returns a complete response before the downstream
request body reaches EOS, the custom pump keeps reading and forwarding the
upload. It never delivers `RequestBodyEvent::Abandoned`.

The loop in `pingora-proxy/src/proxy_custom.rs` remains live while the
downstream state is incomplete even after the response state is complete. Its
body arm continues `read_body_or_idle(false)` and calls `send_body_to_custom`.
That helper derives the event only from a boolean and can therefore construct
`Data` or `Complete`, but never `Abandoned`; there is no other abandonment path
in the custom pump.

If the custom body writer accepts more data, a slow or non-terminating client
can retain the request task, context, and logging lifecycle until downstream
EOS or the downstream read timeout. If the writer rejects the post-response
upload, the pump marks the downstream state errored and returns a generic
`WriteError`, even though the final response may already be committed.

The H1 and H2 upstream pumps already stop futile uploads and deliver exactly
one `Abandoned` event for the equivalent lifecycle. This is therefore a
fork-owned cross-pump contract drift, not a generic upstream Pingora defect.

## Reachability and accepted boundaries

Any user that installs a Pingora custom connector can reach the defect. Edgion
currently installs no custom connector, so the active gateway is not affected.

This is not one of the accepted custom-pump capability boundaries. Request
body `Terminate`, non-`Ordinary` dispositions, and early replay are explicitly
unsupported. Ordinary body filtering is supported and already invokes the
public hook, whose `Complete`/`Abandoned` contract has no custom exception.

## Required outcome

1. On final custom response completion with an unfinished downstream upload,
   deliver one application/module `Abandoned` event and stop forwarding.
2. Separate terminal hook dispatch from `BodyWrite::finish()`; abandonment is
   not clean custom-upstream request EOS.
3. Deliver the same terminal event exactly once when the custom writer stops
   accepting a request mid-upload.
4. Preserve H1/H2 downstream cleanup, custom-message handle restoration, and
   custom upstream reuse decisions.

## Closure evidence

Add custom-connector early-response tests for an unfinished chunked POST and a
writer-rejection variant. Assert prompt response/logging completion,
`Data, Abandoned` with no `Complete`, exactly-once dispatch, and protocol-
specific downstream reuse. Run the request-body seam and custom connector
suites. Static inspection alone is not closure evidence.

Discovery baseline: Pingora `dfa2c8c`, Edgion checkout `83408c1`, 2026-08-28.
