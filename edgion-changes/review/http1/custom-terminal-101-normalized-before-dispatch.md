---
name: custom-terminal-101-normalized-before-dispatch
description: Review rule for a custom upstream that reports a completed Upgrade as Header(101, true); the pump normalizes it to Header(101, false) -> Done so the terminal response-body hook still fires and the H1 upgraded writer never sees a plain Body.
status: implemented-locally
finding_id: gw-16-review-04
closed: 2026-08-24
---

# Source-Terminal 101 Is Normalized In The Custom Pump — FIXED

## Conclusion

`Header(_, true)` means "terminal header" to the response pipeline: a final response whose
body is empty, served by the `terminal_header` branch of `custom_response_filter`. A `101`
can never take that branch, so the custom pump must never hand one out in that shape.
`custom_pipe_up_to_down_response` normalizes a `101` that arrives together with clean
end-of-stream into `Header(101, false)` followed by `Done`, and returns without reading the
session again.

Fix suggestions of the form "include `101` in the `terminal_header` classification" or
"make `TerminalBodyDispatch::claim_for` dispatch on `Header(101, true)`" are **not
accepted**.

## Core Rationale

**1. `101` is a final response by status but an informational one by classification**

`custom_response_filter` derives `terminal_header` from
`HttpTask::Header(header, true) if !header.status.is_informational()`.
`StatusCode::SWITCHING_PROTOCOLS` satisfies `is_informational()`, so a `Header(101, true)`
misses that branch. `TerminalBodyDispatch::claim_for` nevertheless claims every
`Header(_, true)` WITHOUT dispatching, on the assumption that the `terminal_header` branch
already ran `terminal_upstream_body_filter`. The two rules are individually correct and
jointly lose the response's only end-of-stream: a body processor withholding bytes until
EOS never releases them.

**2. The generic drain then produces a task the upgraded writer cannot accept**

With nothing queued in the sink, `drain_emitted_chunks` rewrites an otherwise empty
`Header(_, true)` into `Header(101, false)` + `Body(None, true)`. Writing the `101` sets
`upgraded = true` on the H1 downstream session
(`pingora-core/src/protocols/http/v1/server.rs`), and the plain `Body` behind it violates
`buffer_body_data`'s body-variant invariant and panics the connection task.

**3. Normalizing at the producer keeps the cache machinery out of the upgrade path**

The `terminal_header` branch is entangled with cache admission — synthetic terminal entity
markers, `reconcile_terminal_cache_header`, Range suppression. An Upgrade response needs
none of it. Emitting `Header(101, false) -> Done` instead routes the response onto the path
that was ALREADY correct for a `101` whose connector reports end-of-stream separately:
`Done` claims the latch and dispatches the single terminal callback, and
`drain_emitted_chunks_before` re-emits the released bytes under the response's own body
variant, which `TerminalBodyDispatch::is_upgraded()` remembers.

**4. The filtered downstream header stays authoritative for the handshake**

`mark_upgraded()` is called after `response_filter`, gated on
`session.as_downstream().is_upgrade_req() && header.status == SWITCHING_PROTOCOLS`. The
normalization does not touch that gate, so a naked upstream `101` (the downstream request
never asked to upgrade) and a `response_filter` that rewrites the `101` away both leave the
response un-upgraded and let the terminal bytes travel as ordinary body — which is what the
un-upgraded H1 writer requires.

**5. An EOF'd session must not be read again**

The normalized path sends `Done` and returns instead of falling through the body and trailer
loops. A connector that reported end-of-stream at the header has nothing left to yield, and
calling `read_response_body`/`read_trailers` on it is outside the contract it just declared.

## Fix Suggestions Not Accepted

- Making `terminal_header` accept `101` — pulls an Upgrade response through cache terminal
  admission, Range suppression, and plain-body framing, none of which apply once the writer
  is in raw duplex mode.
- Making `claim_for` dispatch on `Header(101, true)` — dispatches the hook but leaves the
  `Header(_, true)` shape intact, so `drain_emitted_chunks` still appends the plain
  `Body(None, true)` that panics the writer.
- Deriving the upgraded body variant from the UPSTREAM status instead of the filtered
  downstream header — the downstream request may not be an Upgrade, and `response_filter`
  may rewrite the status before the writer sees it.
- Suppressing the terminal output because `101` satisfies `is_informational()` — discards
  legitimate bytes a processor was withholding.

## Re-evaluation Triggers

Re-open this decision only if:

- The H1 downstream writer stops switching to raw duplex mode on `101`, making a plain body
  task valid after the handshake.
- A custom connector gains a body or trailer phase that can legitimately follow a `101` it
  already reported as ended.
- `terminal_header` classification stops being status-derived.

## Regression Boundary

`pingora-proxy/tests/test_upstream_response_body_sink.rs` must keep covering, over the
custom pump:

- a real Upgrade request whose session reports `Header(101, true)` and no `UpgradedBody` —
  the hook observes EOS exactly once, the generated bytes reach the raw upgraded connection
  verbatim with no chunked framing, and the session is not read after it reported EOS;
- the pre-existing `Header(101, false) -> Done` shape
  (`custom_empty_upgrade_tags_terminal_output_as_upgraded_body`);
- a naked `101` answering a non-Upgrade request;
- a `response_filter` that rewrites the `101` to a non-upgrade status, whose terminal bytes
  must come back normally framed.

The first three assertions fail without the fix, and the first one panics the proxy service
thread in `buffer_body_data`.

## Reference Cases

- `../Edgion/tasks/todo/issue-custom-terminal-101-upgrade-panic.md` (2026-08-24), derived from
  the deep review of `h2-trailer-eos-bypasses-response-body-filter`.
- `pingora-proxy/src/proxy_custom.rs` — `custom_pipe_up_to_down_response`.
- `pingora-proxy/src/proxy_common.rs` — `TerminalBodyDispatch`.
- [bodyless-header-eos-terminal-body-hook.md](../../../../Edgion/skills/04-review/http1/bodyless-header-eos-terminal-body-hook.md) —
  the parent contract for terminal Header-EOS dispatch.
