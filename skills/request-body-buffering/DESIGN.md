# Design: Early request body buffering via a pluggable buffer trait

**Date:** 2026-06-20
**Status:** Approved direction; hardened over two 3–4 agent adversarial review rounds
(all findings folded in: this is **v4**). Pending final user sign-off → implementation plan.
**Branch:** `edgion` (carried as an `edgion:` fork patch).
**Relation to PR #816:** smaller, seam-only alternative keeping storage + rewrite policy
out of pingora. See `TODO.md`.

## Goal

Let an Edgion `ProxyHttp` impl, from within `request_filter` (before `upstream_peer`),
read the full request body for content-based routing / full-body auth / **body rewrite**,
and have a body (the original **or a rewritten one**) re-sent to upstream during
forwarding — with the **storage and rewrite policy supplied entirely by Edgion**
(memory, or memory-then-file spill; original or replacement bytes), not hardcoded in
pingora.

## Design principles (what makes this small)

- **Behavior-preserving, not literally "additive":** existing paths are not deleted, but
  two are *edited in place* (the v1 `read_body_bytes` body, and the proxy pre-loop body
  block, which gets wrapped in an `else`). The runtime behavior of the native path is
  preserved; the wording "untouched/additive" from earlier drafts was an overclaim.
- **All policy lives in the Edgion impl.** pingora supplies a capture hook + a replay
  source. Storage location (RAM/file), size budget, and **whether `get()` returns the
  captured original or a rewritten body** are 100% the impl's choice.
- **We do NOT replace `retry_buffer`.** `get_retry_buffer`/`enable_retry_buffering`/
  `retry_buffer_truncated` are methods on the public `ServerSession` trait
  (`custom/server.rs:93`) with many callers; making the shared sync `get_retry_buffer()`
  async would break that trait and every custom-protocol impl. (An earlier draft wrongly
  cited `v2/server.rs:743/829/901` as production H2 retry sites — they are `#[cfg(test)]`.
  The decision stands on the public-trait argument.)

## Trait (new, in `pingora-core/src/protocols/http/body_buffer.rs`)

```rust
#[async_trait]
pub trait RequestBodyBuffer: Send + Sync {
    /// Append one captured body chunk. Called once per chunk during capture.
    async fn write(&mut self, data: &Bytes) -> Result<()>;

    /// Return the body to forward upstream — the captured original OR a rewritten
    /// body, the impl's choice. Re-readable (callable multiple times, incl. on retry).
    /// `None` == the impl cannot produce a body (e.g. over budget) → the proxy fails
    /// the request (guard below). After the response is done, `write()` may be called
    /// again for post-response draining; the impl should treat that as a no-op.
    async fn get(&mut self) -> Result<Option<Bytes>>;
}
```

- `write`/`get` are **async** (user decision) to support memory-then-file spill during
  capture. Cancellation safety is bounded by the usage contract (capture only in
  `request_filter`, never in the forward `select!` loop).
- `get(&mut self)` (not `&self`): `&self` + the `Send + Sync` bound would force
  `tokio::sync::Mutex`-across-`.await` just to cache a materialized `Bytes`; `&mut self`
  is called by one proxy task on a non-shared `Session`, so it is re-readable with a plain
  field, no lock.
- **Rewrite needs no extra method:** because `get()` returns whatever the impl wants,
  returning rewritten bytes IS the rewrite mechanism. There is intentionally no
  exact-length / exact-original invariant.
- A small in-memory reference impl (over `BytesMut`) ships for tests/demo; the real
  memory-then-file (+rewrite) impl lives in Edgion.

## Session API (additive methods; private field)

- New private field `early_body_buffer: Option<Box<dyn RequestBodyBuffer>>` on the H1 and
  H2 server sessions.
- Methods surfaced on the `HttpSession` enum, forwarded to H1/H2, with inline no-op /
  `NotRegistered` arms for Subrequest+Custom (so the public `ServerSession` trait is NOT
  extended — this preserves the "no Custom-trait change" property):

```rust
pub fn set_request_body_buffer(&mut self, buffer: Box<dyn RequestBodyBuffer>);

/// Three-state replay accessor (fixes the v3 ambiguity where a single `Ok(None)`
/// could not distinguish "no buffer" from "buffer failed").
pub enum EarlyBodyReplay { NotRegistered, Body(Bytes), Unavailable }
pub async fn take_early_body_for_replay(&mut self) -> Result<EarlyBodyReplay>;
```

`take_early_body_for_replay` maps: field `None` → `NotRegistered`; field `Some` and
`get()` → `Some(b)` → `Body(b)`, `get()` → `None` → `Unavailable`; `get()` `Err` →
propagated `Err`. Reachable from the proxy via the existing `Session`/`as_mut()` deref
chain (same path `get_retry_buffer()` uses at `proxy_h1.rs:384`).

## Capture (read-triggered)

In `read_body_bytes()` (v1 and v2 server), after a chunk is read, add an async write to
the early buffer, separate from the existing sync `retry_buffer` write:

```rust
if let Some(eb) = self.early_body_buffer.as_mut() { eb.write(&bytes).await?; }
```

- **v1 restructure (verified):** the current v1 `read_body_bytes` (`v1/server.rs:542-552`)
  builds `bytes` inside a *sync* `.map(|b| …)` closure where `.await` is illegal. Rewrite
  it as `match`/`let-else`. The borrow is sound: `read_body()` returns
  `Result<Option<BufRef>>` and `BufRef` is two owned `usize`s (no `self` borrow);
  `get_body()` returns `&[u8]` but `copy_from_slice` copies, so the immutable `self`
  borrow ends at that statement, before the `&mut self.early_body_buffer` await. v2
  (`v2/server.rs:246-263`) already uses `if let`; no restructure.

## Replay (forwarding) + framing + guard

Wrap the existing pre-loop body block (`proxy_h1.rs:382-400`, `proxy_h2.rs:386-399`) in an
`else`, gated so the early branch is entered **only when a body is expected**:

```rust
// Only consult the early buffer for body-expected requests. For empty bodies
// (GET, Content-Length: 0, H2 END_STREAM on HEADERS) take the native path — which
// already END_STREAMs correctly — so we never do an illegal write-after-END_STREAM
// and never fail a legitimate empty request.
let replay = if session.is_body_empty() {
    EarlyBodyReplay::NotRegistered
} else {
    session.take_early_body_for_replay().await?
};
match replay {
    EarlyBodyReplay::Body(body) => {
        // acquire the same tx.reserve() permit the native H1 block uses, then send
        // `body` to the upstream pipe with end = session.is_body_done().
    }
    EarlyBodyReplay::Unavailable => return Err(/* InternalError: early body unavailable */),
    EarlyBodyReplay::NotRegistered => { /* existing get_retry_buffer()/network path */ }
}
```

- **Guard (B1):** `Unavailable` fails the request rather than silently sending an empty/
  short body (H1-chunked) or hanging an H2 stream with no END_STREAM. Verified: the `Err`
  propagates through `try_join!` → no upstream-connection reuse, **no retry with a
  consumed body**, clean downstream error. Caveat: it fires *after* the upstream request
  header is already on the wire (`proxy_h1.rs:88`/`proxy_h2.rs:153`), so the upstream sees
  a headers-then-abort; this is a loud backstop, not the primary defense (see contract).
- **Empty-body gate (C2):** mandatory — see the `is_body_empty()` check above.
- **Mutual exclusivity (verified):** the proxy force-enables retry buffering
  (`proxy_h1.rs:103`), but it stays empty in the fully-drained case (`FixedBuffer::
  get_buffer()` → `None` when empty), and the `else` makes the two branches structurally
  exclusive → body sent exactly once.
- **Framing — Edgion owns it when rewriting.** The upstream H1 header is a verbatim clone
  of the downstream header (`proxy_h1.rs:44`). Replaying bytes of the **same** length as
  the client's `Content-Length` is correct as-is. If Edgion's `get()` returns a body of a
  **different** length (rewrite), Edgion MUST fix `Content-Length` / switch to chunked in
  `upstream_request_filter`, else: CL over-length → excess silently dropped
  (`v1/body.rs:1198`); CL short → `PREMATURE_BODY_END`; chunked short → silently empty.
- `request_body_filter` still runs exactly once on the replayed body (invoked in
  `send_body_to_pipe`/`send_body_to2`), consistent with the retry path.

## Edgion usage contract (the sharp edges live here — all mandatory)

1. **Register before the first read, at most once.** `set_request_body_buffer` must be
   called before the first `read_request_body()`; registering after bytes are read yields
   a partial body. Calling it twice drops an already-filled buffer. (Impl/clear-cut: a
   debug-assert on `body_bytes_read > 0` or second registration is recommended.)
2. **Drain to completion in `request_filter`.** Read until `read_request_body()` returns
   `Ok(None)` so `is_body_done()` is true before `upstream_peer`. This is what makes the
   forward loop take the idle branch and not re-read the (now-EOF) body. A registered
   buffer present at forward entry with `!is_body_done()` is a usage error; the proxy
   should debug-assert/fail deterministically rather than silently truncate.
3. **Treat any `read_request_body()` error as fatal.** A mid-capture client disconnect
   returns `Err` (e.g. `ConnectionClosed … body remaining`, `v1/body.rs:355`), not a short
   `Ok(None)`. Propagate it; do not replay a partial body.
4. **Own the total read deadline + byte budget.** pingora's `read_timeout` is per-chunk
   and resets; `total_drain_timeout` does NOT cover a manual `read_request_body()` loop.
   Edgion MUST wrap its early-read loop in a wall-clock `tokio::time::timeout` and a total
   byte cap, or a slow-loris client holds the connection + buffer + temp file
   indefinitely.
5. **Enforce the size budget here and return 413 here.** Over-budget should be rejected in
   `request_filter` with `413` (before `upstream_peer`), the correct client signal. The
   proxy `Unavailable` guard is only a last-resort backstop (it yields a generic 500, and
   the upstream header is already sent by then).
6. **Keep the buffer registered through the whole retry loop.** Do NOT unregister in the
   forward path: the retry loop re-runs forwarding and re-calls `get()`. Unregistering
   between attempts routes the retry to the native empty/short-body path with no guard.
   The buffer is dropped naturally when the `Session` ends; the impl makes post-`get()`
   `write()` calls (post-response drain) no-ops.
7. **Reconcile framing on rewrite.** If `get()` changes the body length, fix
   `Content-Length`/chunked in `upstream_request_filter` (see Framing above).
8. **`Expect: 100-continue`:** pingora does not auto-send `100 Continue` before upstream
   contact; a client withholding its body will block `read_request_body()` forever. Edgion
   must send `100 Continue` (`write_continue_response`, `v1/server.rs:1433`) before
   early-reading, or skip buffering for such requests.
9. **Do not combine with caching on the same request** unless the body (or its digest) is
   folded into `cache_key_callback`/`cache_vary_filter`. The default cache key is
   header/URI-only, so a bodied request that is both cacheable and body-routed risks cache
   poisoning. (Also: on a cache hit the early read/spill is wasted — decide to buffer only
   for non-cacheable requests.)
10. **`get()` must be re-readable and return the SAME body across retries.** The proxy
    calls `get()` once per upstream attempt; the buffer stays registered (contract #6) and
    is re-consulted on retry. An impl that returns a different body — or `None` — on the
    retry call turns a *retryable* upstream failure into a non-retryable 500. The in-memory
    reference impl satisfies this by caching the materialized `Bytes`.

## Review-confirmed limits (second adversarial review round, 2026-06-20)

The implemented seam was re-reviewed on the final code. No correctness defect was found
(the guard does not false-positive — `is_body_done()` stays true on retry because the
body reader is only re-init'd for a new request; the native no-buffer path is byte-for-byte
unchanged). Three behaviors are documented limits, not bugs:

- **No body injection into an originally-empty request.** When the client sent no body
  (`is_body_empty()` true — GET / `Content-Length: 0` / H2 END_STREAM on HEADERS), the
  proxy routes to the native path and never consults the buffer, so a buffer that tries to
  *add* a body is ignored. This is required: on H2 the header END_STREAM is already on the
  wire by replay time, so a late body would be an illegal write-after-END_STREAM. **Rewrite
  is supported only for requests that already have a body.**
- **The guard catches "not fully drained", not "drained but captured nothing".** The Body
  arm fails when `!is_body_done()`. It does NOT detect a buffer that captured a short/empty
  body while the stream was fully drained — only possible if the body was drained through a
  path that bypasses `read_body_bytes` (i.e. the out-of-scope `poll_read_body_bytes`).
  Within the supported `read_request_body` capture path this cannot happen.
- **`EarlyBodyReplay::Unavailable` is the custom-impl over-budget signal.** It fires when an
  impl's `get()` returns `None`. The reference impl never returns `None` (an empty buffer
  yields `Some(empty)`), so `Unavailable` is exercised only by budget-enforcing impls.

## Scope limits

- **v1 = whole-`Bytes` replay.** `get()` returns one `Bytes`; a file-backed impl
  materializes into RAM on replay (amortized over multiple reads by cloning). Never-in-RAM
  streaming replay is out of scope (would change the proxy forward loop).
- **Request trailers out of scope.** H2/chunked request trailers are not captured or
  replayed (matches native pingora behavior — there is no downstream→upstream request
  trailer path today). Do not use early buffering where request trailers are semantically
  required.
- **`poll_read_body_bytes` (`v2/server.rs:266`) out of scope.** A `#[doc(hidden)]` sync H2
  poll path with no in-tree callers; it bypasses the buffers and cannot host an async hook.
- **Subrequest/Custom out of scope** (verified safe): subrequests run on a fresh session
  and do not inherit `early_body_buffer`; callers must re-supply a body to a subrequest.

## Known limits / caveats (not blockers, must be documented)

- **Memory bound is `spill_threshold + advertised H2 flow-control window` (× concurrent
  streams), not `spill_threshold`.** Reading the whole H2 body advertises the full window,
  so the h2 crate buffers up to the window in RAM before chunks reach the impl. Recommend
  capping the H2 stream/connection initial window when early-buffering is enabled. Reading
  the full body upfront also removes upstream-coupled backpressure (the gateway absorbs the
  whole upload before the upstream is even chosen) — an accepted trade-off; the mandatory
  total byte cap (contract #4/#5) bounds disk use.
- **Retry-reuse signal:** with only the early buffer in use, `retry_buffer_truncated()` is
  always false (`proxy_trait.rs:585`); replay-on-retry relies on the re-readable `get()`
  (contract #6).
- **Downstream module body filters** see one whole `Bytes` at replay, not streamed chunks;
  chunk-sensitive modules may differ.
- **Version invariant (DONE):** the `edgion` branch is bumped to `0.8.1` (all `pingora-*`
  versions + internal deps) so Edgion's `pingora-* = "0.8.1"` requirement is satisfied once
  `[patch.crates-io]` is wired. The `[patch]` wiring in `Edgion/Cargo.toml` is still
  pending (deferred). Verify by building **Edgion** after wiring it.

## Change surface

pingora-core: `body_buffer.rs` (trait + ref impl + `EarlyBodyReplay`); `v1/server.rs`
(field, let-else rewrite + capture hook, `set_request_body_buffer`,
`take_early_body_for_replay`); `v2/server.rs` (field, capture hook, accessors);
`server.rs` `HttpSession` enum (forward methods; `NotRegistered`/no-op for
Subrequest+Custom — no `ServerSession` trait change).
pingora-proxy: `proxy_h1.rs`, `proxy_h2.rs` (empty-body gate + `else`-wrapped guarded
replay branch + H1 permit).
Untouched: `retry_buffer`/`get_retry_buffer`/`enable_retry_buffering`/
`retry_buffer_truncated`, `custom/server.rs` `ServerSession` trait, `proxy_custom.rs`. No
new dependency (`async_trait` already in the workspace).

## Testing

- pingora-core: no buffer → unchanged (regression); fake impl verifies capture-on-read and
  replay-on-forward; `get()==None` → `Unavailable` → request fails (not a silent body);
  rewrite impl (`get()` returns different bytes) replays the new body; let-else keeps the
  retry-buffer path green.
- Integration (Edgion): full-drain POST forwarded correctly under `Content-Length` and
  chunked; multi-chunk; body read twice; rewrite with adjusted Content-Length; **empty
  body (GET / CL:0) with a buffer registered succeeds via the native path**; `Expect:
  100-continue` no deadlock; over-budget → 413 in request_filter; retry replays the
  buffer; opt-out identical to today; body-at-threshold ±1.

## References

- `TODO.md` (same dir) — decision history, A/B/C/#816 comparison.
- Issue #780, PR #816 — upstream's heavier early-buffering effort.
