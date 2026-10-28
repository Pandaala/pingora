# Non-streaming cache trailer completion

Status: open

Severity: medium

Ownership: Pingora fork

Origin: response trailer filter parity review at fork `bd89d47`, 2026-08-29.

## Problem

A cache backed by a `Storage` implementation whose
`support_streaming_partial_write()` returns `false` does not complete admission
for an H1 upstream response terminated by real trailers. The upstream body is
written to the miss handler, but `cache_http_task()` deliberately ignores
`HttpTask::Trailer`; the H1 response pump treats that trailer as terminal and
does not feed a following `Done` into cache completion. The entry therefore
remains unavailable even when all trailer hooks succeed.

This is independent of `response_trailer_filter` error propagation. The
executable characterization
`non_streaming_cache_does_not_admit_trailered_responses_with_or_without_hook_error`
shows both the error case and a successful-hook control remaining misses.

## Required action

1. Define a protocol-aware cache completion signal that is independent of the
   downstream-only trailer hook.
2. Do not blindly finish every cache entry on `HttpTask::Trailer`: the accepted
   upstream `h2` decoder/reset limitation means a terminal trailer handoff is
   not always sufficient cache-admission proof.
3. Cover H1, H2, and custom pumps with streaming and non-streaming storage,
   terminal trailers, reset/error ordering, and cache hit/readback checks.
4. Revisit this alongside the shared response-task pipeline refactor so the
   cache boundary has one owner.

## Links

- [Response trailer contract](../features/response-trailers.md)
- [Known upstream h2 limitations](../review/upstream-limitations.md)
- [Resolved trailer filter parity review](../review/response-trailer-filter-error-parity.md)
