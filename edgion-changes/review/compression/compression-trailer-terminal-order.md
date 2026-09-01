---
name: compression-trailer-terminal-order
description: Use when reviewing compression with response trailers, duplicate terminal body callbacks, or body tasks appearing after trailers.
status: fixed
finding_id: issue-compression-trailer-double-eos
closed: 2026-08-29
---

# Compression Finalizes Before Response Trailers — FIXED

## Conclusion

Upstream response compression now finalizes before a `Trailer` task in the H1,
H2, and custom pumps. Encoder footer bytes are processed as a non-terminal
`Body` immediately before the unchanged trailer. The compression context
records that it has finalized, so a following `Done`, including one arriving
in a later pump batch, remains a bookkeeping marker and cannot emit another
body task or body-hook EOS.

The original Pingora compression helper ignored trailers and finalized only by
rewriting `Done` to `Body(footer, true)`. That left a latent footer-after-
trailer ordering defect. Fork commit `600ac49` added the response-wide
`TerminalBodyDispatch`; its synthetic pre-trailer EOS made the inherited
ordering defect observable as a second EOS on the rewritten body. The
underlying behavior is upstream, while the public exactly-once amplification
and the seam correction are fork-owned.

## Required Ordering

```text
compressed data body (eos = false)
compression footer body (eos = false)
Trailer (single TerminalBeforeTrailers dispatch)
Done (inert)
```

Suppressing only the second callback is not a valid fix: it leaves footer bytes
after the protocol terminal marker and still produces a truncated compressed
representation. Moving termination from the trailer to the footer is also
invalid because the trailer is the transport's real terminal event.

`ResponseCompressionCtx::response_filter` keeps its existing public signature
and behavior for compatibility. Protocol pumps use the additive
`response_filter_with_preceding` API, which returns the footer task that must be
processed before the trailer. This avoids treating an internal compression
footer as application `ResponseBodySink` output and preserves the existing
filter and cache layers.

## Regression Coverage

- The core compression test joins the regular gzip output with the returned
  pre-trailer footer, decompresses it, and verifies that a later `Done` remains
  unchanged.
- `test_terminal_body_dispatch` covers decompressible H1 and H2 trailered gzip
  responses, one EOS observation, and byte-identical compressed H2 cache fill
  and hit bodies.
- `test_upstream_response_body_sink` covers the same complete representation
  and single EOS through the custom pump.
- Existing terminal, sink, cache/reuse, and failure suites retain coverage for
  same-batch/split-batch state, cache cleanup, aborts, and connection reuse.

## Revisit Trigger

Re-evaluate this seam if upstream changes `ResponseCompressionCtx` to model
trailers directly, removes the `Done` footer rewrite, or introduces a task-list
filter API that can atomically return body bytes plus the original trailer.
