---
name: trailer-batch-latch-cache-completion
description: Use when reviewing findings that claim a Trailer ending a pump batch strands the following Done and hangs a streaming cache miss; records why the hang is unreachable and what the real residual gap is.
status: wont-fix
finding_id: issue-trailer-done-cross-batch-cache-eof
closed: 2026-08-24
---

# A Batch-Ending `Trailer` Does NOT Strand `Done` on a Streaming Cache Miss — NOT-AN-ISSUE

## Conclusion

The reported failure — "a cacheable trailered response hangs forever because
`Trailer` latches `upstream_done` and the `Done` that runs
`finish_miss_handler()` is never read" — is **unreachable**. In every response
pump the batch handler takes an early `continue` **before**
`maybe_set_upstream_done(source_done)` whenever a cache readback is on, and a
streaming miss is exactly the state that turns it on. `Done` is therefore
always consumed on the path the finding requires, and the miss handler is
always finished.

Fix suggestions of the form "make `HttpTask::Trailer(_)` call
`finish_miss_handler()` in `cache_http_task` to stop the hang", or "compute
`source_done` without `Trailer` in the response pumps", are **not accepted** on
that rationale: they are justified by a failure mode that cannot occur.

## Core Rationale

**1. The latch the finding depends on is never reached during a streaming miss**

`pingora-proxy/src/proxy_h2.rs` (and the structurally identical
`proxy_custom.rs` / `proxy_h1.rs` sites):

```rust
if !serve_from_cache.should_send_to_downstream() {
    // TODO: need to derive response_done from filtered_tasks in case downstream failed already
    continue;
}
…
response_state.maybe_set_upstream_done(source_done);
```

`should_send_to_downstream()` is `!is_on()`. A miss with a streaming body
reader calls `serve_from_cache.enable_miss()` (`proxy_cache.rs`, guarded by
`session.cache.miss_body_reader().is_some()`), so `is_on()` is true and the
`continue` fires. `upstream_done` is then set only from the `rx` channel
closing — which happens after `Done` has been consumed.

**2. Measured, not reasoned**

Instrumenting the h2 pump with the finding's own scenario (origin holds its
body until the proxy is stalled inside its header filter, so the channel fills
with `Body, Body, Body, Trailer` and the producer blocks on `Done`) produces
exactly the batch layout the finding predicts — and `Done` is still delivered:

```
PROBE batch: ["H"]
PROBE batch: ["B", "B", "B", "T"]
PROBE batch: ["D"]
```

An end-to-end cache miss/hit test built on that forced layout passes
identically with and without the proposed `Trailer` arm. It cannot discriminate
the fix, which is the same thing as saying there is nothing there to fix.

**3. The residual gap is real but out of reach in this tree, and is not a hang**

If the cache storage does NOT support streaming partial write,
`miss_body_reader()` is `None`, `enable_miss()` is never called, `is_on()` is
false, the `continue` does not fire, and a batch-ending `Trailer` genuinely
latches `upstream_done` and strands `Done`. The consequence is **not** a hang:
with no miss body reader the downstream response is written directly from the
pump's `filtered_tasks`, so the client is served normally. The unfinished
`MissHandler` is dropped, and `pingora-cache`'s own contract ("if `self` is
dropped without calling this, the cache admission is considered incomplete and
should be cleaned up") means nothing is committed. The harm is a **silently
uncached response**, not a stall and not a truncated entity.

This fork's `MemCache` always supports streaming partial write, so no
in-tree configuration reaches it. It is a latent gap for a third-party
non-streaming storage backend only.

## Fix Suggestions Not Accepted

- "`Trailer` must run `finish_miss_handler()` to stop the streaming-miss hang" —
  the hang does not exist; the `continue` above keeps `Done` reachable.
- "Add an H2 test forcing `Trailer` and `Done` into separate batches to prove
  the hang" — such a test was written and forced the exact layout; it passes
  before and after the proposed change, so it asserts nothing.
- "Drop `Trailer` from the `HttpTask::is_end()` set used for `source_done`" —
  `Trailer` genuinely terminates the response; weakening `is_end()` to work
  around a non-existent hang would break the terminal-body latch and the
  upstream-reuse decisions that depend on it.
- "Treat this as P1" — no in-tree configuration can reach even the residual
  silently-uncached case.

## Re-evaluation Triggers

Re-open only if:

- The `if !serve_from_cache.should_send_to_downstream() { continue; }` early
  exit is moved, removed, or made conditional — its `TODO` comment marks it as
  provisional, and the finding's whole failure mode becomes reachable the moment
  `maybe_set_upstream_done(source_done)` runs on a streaming miss.
- A cache storage backend without streaming partial write is adopted, or
  `MemCache` gains a non-streaming mode. Then the residual
  silently-uncached-response gap becomes reachable and the `Trailer` arm is
  worth adding on its own merits (`finish_miss_handler` is already idempotent,
  so a same-batch `Done` would not double-finish).
- `HttpTask::Trailer` starts carrying cache-visible content (h1 trailer support
  landing, or trailer fields being stored in the entity), which would make the
  arm's behavior observable for reasons unrelated to completion.

## Reference Cases

- Finding source: `../Edgion/tasks/todo/issue-trailer-done-cross-batch-cache-eof.md`,
  raised by a deep review of `h2-trailer-eos-bypasses-response-body-filter`.
- Fork sources: `pingora-proxy/src/proxy_cache.rs` (`cache_http_task`,
  `ServeFromCache::should_send_to_downstream`), `proxy_h2.rs` / `proxy_custom.rs`
  / `proxy_h1.rs` batch handlers, `pingora-cache/src/lib.rs`
  (`finish_miss_handler`, already idempotent).
- Related: [trailer-done-terminal-body-dispatch.md](trailer-done-terminal-body-dispatch.md)
  — its "`cache_http_task` stops treating `HttpTask::Trailer(_)` as a no-op"
  re-evaluation trigger is what produced this finding; the trigger stands, but
  the cache-completion motivation for pulling it does not.
