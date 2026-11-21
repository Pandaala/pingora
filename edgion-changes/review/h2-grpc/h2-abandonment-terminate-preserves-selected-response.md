# H2 abandonment termination preserves a selected response — resolved

Status: resolved

Date: 2026-09-01

## Conclusion

An `Abandoned` request-body callback can run after Pingora's response pipeline
has selected the final origin response but before queued terminal response
tasks reach the downstream writer. Treating the callback's `Terminate` as
proof of an application-owned completed response let the H2 upstream pump call
`finish_terminated_response()` and exit. An H1 downstream could therefore see
a clean terminating chunk after only a response prefix.

The shared request relay now combines protocol-pump proof of response
completion with a snapshot of pipeline response ownership taken before awaiting
the application hook. Qualified `Abandoned + Terminate` with a preselected
pipeline response produces an internal `PreserveSelectedResponse` outcome.
H1, H2, and custom pumps stop request-side work without manufacturing a clean
request EOS or finalizing the response, and their existing response lanes keep
draining. Unqualified abandonment, nonterminal body-hook termination and
trailer-hook termination after response selection produce `AbortSelectedResponse`. H1 closes without a final chunk,
H2 resets the stream, and custom downstreams invoke their abort boundary.
Ordinary termination is also unchanged when no pipeline response was selected,
including a local response written by the application during the hook.

A historical follow-up applied the same abort helper to `Terminate` from a
custom writer-rejection callback. Upstream-main issue 05 removed that callback.
The [custom writer contract](../custom/custom-writer-error-context.md) governs
the response and error outcome after a rejected write or finish.

`Session::response_head_selected_by_pipeline()` exposes the ownership fact to
consumers; it is not response-completion proof. Edgion still returns
`Terminate` after WAF/handler/mirror cleanup, but writes a local rejection only
when the response pipeline does not already own the response.

## Preserved boundaries

- Retry remains closed from final-response selection.
- A delivered `Abandoned` terminates that request-body sequence.
- No clean upstream request EOS is inferred from abandonment.
- H1 unread-body non-reuse, H2 stream-local cleanup, cache finalization, and
  protocol-specific response draining remain pump-owned.
- Preserved H1 responses finish before a bounded request-body drain, then the
  downstream connection closes instead of entering the reusable pool.
- Normal `Data`/`Complete` termination and application-owned local replies keep
  the existing fail-closed behavior.

## Regression evidence

`test_h2_upstream_no_error_reset::h2_abandonment_terminate_preserves_the_selected_response`
uses an H1 downstream and an H2 origin. It commits the final response head
before the client upload starts, exhausts H2 request flow control, queues the
terminal response body, sends `RST_STREAM(NO_ERROR)`, returns `Terminate` from the
resulting `Abandoned` callback, and requires the complete chunked response.
Shared relay tests cover the selected and unselected ownership branches across
H1, H2, and custom capabilities. The protocol test also requires the response
body exactly once and closes the abandoned H1 connection after a bounded drain
instead of reusing it. Edgion unit coverage pins `Terminate` without a local
replacement for the selected-response case.

`terminate_explicitly_aborts_an_incomplete_selected_response` commits a
nonterminal H2-origin 200 head to an H1 client, terminates from a request `Data`
callback, and requires connection abort without a clean chunked terminator.
The shared relay negative case covers both unqualified `Abandoned` and `Data`
across H1, H2, and custom. Trailer-hook tests pin the same pre-hook ownership
snapshot and prove that a local response first written inside the hook remains
ordinary application-owned termination.

The historical `custom_writer_rejection_aborts_a_selected_incomplete_custom_response`
regression exercised the former writer-rejection callback and its explicit
custom downstream abort. Current issue 05 tests cover the independent writer
error policy.

## Re-evaluation triggers

Revisit this conclusion if response ownership moves out of `Session`, request
and response pumps stop sharing the current relay outcome, or an application is
allowed to replace a response after pipeline selection.
