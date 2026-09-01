---
name: trailer-done-terminal-body-dispatch
description: Use when reviewing response-body findings about end_of_stream never arriving on H2 trailered responses; records the single terminal-dispatch latch that closed the Trailer/Done hole.
status: fixed
finding_id: h2-trailer-eos-bypasses-response-body-filter
closed: 2026-08-24
---

# Trailer/Done Deliver the Single Terminal Response-Body Callback — FIXED

## Conclusion

`upstream_response_body_filter` now receives `end_of_stream = true` exactly once
for every response that terminates normally, including an H2 response that ends
with trailers and one that ends with a bare `HttpTask::Done`. A per-response
latch (`response_pipeline::TerminalBodyDispatch`) decides which task delivers it, and
released bytes are emitted ahead of the terminating task.

The H1 trailer re-evaluation trigger fired on 2026-08-28. The response-wide
latch is now shared by H1, H2, and custom pumps and returns a typed event:
`TerminalBeforeTrailers` for a real trailer map and
`TerminalWithoutTrailers` for an empty trailer or bare Done. H1 parsing and
writing use the same ordering and exactly-once contract; this remains a fixed
fork-owned finding rather than a new issue.

Fix suggestions of the form "just call `terminal_upstream_body_filter` from
`upstream_filter`", "add a `Trailer` arm to the `upstream_filter` dispatch
table", or "let both `Trailer` and `Done` fire the callback" are **not
accepted** — the first produces `Trailer` before the released `Body`, and the
last double-finishes the response.

## Core Rationale

**1. `end_of_stream` is a lifecycle event, not an error signal**

It is the only point at which a processor that withholds bytes across callbacks
may release them. `edgion-gateway/src/ai/anthropic/response.rs`
`process_non_streaming` is the canonical shape: it `take()`s every chunk,
accumulates, and emits only at EOS. When the signal never arrives the processor
returns normally, nothing errors, and the client receives `200` with an empty
body. Guardrail's `RawWindow` loses a final below-threshold window the same way.

**2. H2 puts END_STREAM on the trailers HEADERS frame**

`check_response_end_or_error` (`pingora-core/.../http/v2/client.rs`) reports
`response_body_eof || reader.is_end_stream()`; with trailers pending neither is
set, so `pipe_up_to_down_response` emits `Body(.., false)` for every DATA frame
and terminates with `Trailer` then `Done`. `HttpProxy::upstream_filter` reaches
the body filter only from `Body`/`UpgradedBody`, so nothing delivered EOS.

The fork had already closed one instance of this class: commit `83f3912` added
`terminal_upstream_body_filter` for `Header(_, true)`. `Trailer` and `Done` were
not covered.

**3. `Trailer` and the `Done` behind it are ONE termination**

Hence a latch rather than a per-task rule. `Header(_, true)`, `Body(_, true)`
and `Failed` claim it without dispatching; `Trailer`/`Done` dispatch only if
still unclaimed. `Failed` claiming is load-bearing: it stops a `Done` following
an error from telling a filter that a truncated body was complete.

The latch also records upgrade state from the final filtered response header
when it completes a downstream Upgrade request, not only from `UpgradedBody`.
A cleanly closed upgrade can reach `Done` without yielding any body task;
terminal output must still use `UpgradedBody` or the H1 downstream writer
rejects the ordinary-body/upgraded-session mismatch. A bare upstream 101 is not
enough because the request may not be an Upgrade and `response_filter` may
rewrite the status before the downstream writer sees it.

**4. Ordering is where the fix stops being a one-liner**

`drain_emitted_chunks` appends sink extras AFTER the leading task, and
`migrate_end_of_stream` reports `Unchanged` for `Trailer`/`Done`. Reusing it
would emit `Trailer(end)` then `Body(released, false)` — bytes after the
terminal marker. `drain_emitted_chunks_before` emits released bytes first and
performs no EOS migration, so the terminating task keeps the response's single
completion. `cache_task_and_emitted_chunks_before` mirrors it for admission;
that ordering is load-bearing for a bare `Done`, which runs
`finish_miss_handler()`.

## Fix Suggestions Not Accepted

- Call `terminal_upstream_body_filter` directly from `upstream_filter` — the
  ordinary drain then writes `Trailer` before the released `Body`.
- Add a `Trailer` arm to the `upstream_filter` body-filter dispatch — it fires
  again on the following `Done` and double-finishes the response.
- Synthesize EOS on `Failed` — presents a truncated body as complete.
- Migrate the end-of-stream flag onto the last released chunk — `Trailer`/`Done`
  are intrinsically `is_end()`; migrating either duplicates the completion or
  strands bytes after the terminal marker.
- Make the fix H2-specific — the latch and both drain helpers are
  protocol-neutral so H1 inherits them when H1 trailer parsing lands.

## Re-evaluation Triggers

Re-open only if:

- A response pump gains a termination shape outside the six enumerated ones
  (`Header`-EOS, `Body`-EOS, trailers, bare `Done`, `Failed`, H1 framings).
- `cache_http_task` stops treating `HttpTask::Trailer(_)` as a no-op, which
  would make the cache-ordering branch observable for trailered responses too.
  Note: this trigger was already pulled once, on the theory that a batch-ending
  `Trailer` strands the `Done` that finishes cache admission. That failure mode
  was measured and does not exist — see
  [trailer-batch-latch-cache-completion.md](trailer-batch-latch-cache-completion.md)
  before re-raising it.

## Reference Cases

- Fork: `pingora-proxy/src/response_terminal.rs` (`TerminalBodyDispatch`),
  `response_cache_relay.rs` (`drain_emitted_chunks_before`,
  `cache_task_and_emitted_chunks_before`), `proxy_h2.rs`, `proxy_custom.rs`.
- Regression tests: `pingora-proxy/tests/test_terminal_body_dispatch.rs` (H2),
  `test_upstream_response_body_sink.rs::custom_trailered_*` and
  `custom_empty_upgrade_tags_terminal_output_as_upgraded_body` (custom pump),
  plus latch and ordering unit tests in
  `proxy_common_terminal_body_dispatch_tests.rs` / `proxy_cache.rs`.
- Prior instance of the same class: commit `83f3912`
  (`terminal_upstream_body_filter` for `Header(_, true)`).
- Related: [abandoned-request-body-terminal-event.md](../../../../Edgion/skills/04-review/h2-grpc/abandoned-request-body-terminal-event.md)
  — the request-side analogue of "terminal event must be distinguishable".
- Adjacent, still open:
  `../Edgion/tasks/todo/downstream-response-body-filter-trailer-eos-gap.md` — the
  DOWNSTREAM `response_body_filter` has the same trailer-shaped hole. It is an
  observation gap, not byte loss: that hook is documented observation-only, so a
  "withheld bytes are lost there" re-report is not a defect. Priority P4, no
  consumer in the Edgion tree today.
