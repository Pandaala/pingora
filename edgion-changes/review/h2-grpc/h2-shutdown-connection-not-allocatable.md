---
name: h2-shutdown-connection-not-allocatable
description: Use when reviewing Pingora's H2 connector around connection reuse, pool eligibility, `mark_shutdown()`, or claims that `more_streams_allowed()` already prevents new streams on a draining connection.
status: fixed
finding_id: H2-008
closed: 2026-08-27
---

# A connection marked for shutdown must be refused at allocation, not only at re-pooling

## Conclusion

`ConnectionRef::mark_shutdown()` means "no new stream on this connection", and
that has to be enforced where a stream is actually allocated
(`ConnectionRef::spawn_stream`) and where a connection is picked out of the pool
(`Connector::reused_http_session`). Fix suggestions of the form
"`more_streams_allowed()` already checks `is_shutting_down()`, so the flag is
handled" are **not accepted**: that check only decides whether the connection
goes BACK into the pool, which is a decision made after the stream in question
has already been handed out.

## Core Rationale

**1. The flag had two readers, and neither was on the allocation path**

Before the fix `shutting_down` was consulted in exactly two places, both of them
exits rather than entrances:

- `more_streams_allowed()` — gates re-insertion into the in-use pool after
  `spawn_stream()` has already returned a session;
- `release_http_session()` — drops the connection instead of pooling it, once
  its last stream is released.

`reused_http_session()` filtered pool candidates with `.filter(|c| !c.is_closed())`
only, and `spawn_stream()` compared `current_streams` against `max_streams` and
nothing else. So the flag could not stop the one thing it exists to stop.

**2. The window is not a hairline race**

Nothing removes a connection from the in-use pool at the moment it is marked;
that happens later, in `release_http_session()`, when the marking stream is
finally released. `pingora-proxy` raises the mark on an upstream read timeout
(`proxy_h2.rs`, `ReadTimedout` arm) precisely because the connection is suspected
hung — and then keeps unwinding that request. Every request arriving in between
used to be multiplexed onto the abandoned connection, and since the wire is
healthy in this case (a hung upstream sends no GOAWAY and closes nothing),
`new_stream()` succeeds and the new request goes on to burn its own full read
timeout. One hung upstream stream was therefore amplified into a run of them.

**3. The GOAWAY branch only looked safe by luck**

The `GOAWAY(NO_ERROR)` / `BrokenPipe` path in `spawn_stream()` appears to be
covered because `new_stream()` fails on its own and the error is converted to
`Ok(None)`. That is `h2` refusing, not the fork's shutdown state being honored.
The read-timeout mark has no such backstop, which is why the guard has to be
explicit.

**4. What the guarantee actually is: allocation, not the whole request**

`spawn_stream()` checks the flag twice, once before `new_stream()` and once
after its await. Be precise about what the second check buys, because the
obvious justification for it is false: `Stub::new_stream` awaits `ready()` on a
FRESH CLONE of `SendRequest`, `h2` sets `pending: None` on a clone, and
`poll_pending_open(cx, None)` therefore never returns `Poll::Pending`. The await
cannot suspend today, so the second check cannot fire. It is kept as defense in
depth against `Stub` no longer cloning — one relaxed load — and not because it
closes a window.

The guarantee this fix establishes is at the ALLOCATION boundary, which is what
H2-008 asked for: a connection marked for shutdown is never handed out by the
pool and never allocated a stream. It is NOT "no stream is ever opened on a
marked connection". `h2` allocates the stream id in `send_request()`, reached
from `Http2Session::write_request_header`, which the proxy calls well after
`spawn_stream()` returned — `upstream_request_filter` and the rest of the
request path run in between. A mark landing in that gap still results in a
stream opened on a marked connection, and no memory ordering can change that;
only a check at send time could.

That residual gap is deliberate, not an oversight. `mark_shutdown()` means "no
NEW stream", the same semantics RFC 9113 gives GOAWAY: work already committed to
the connection finishes. A request that has already been assigned a connection
is committed; failing it at `write_request_header` would turn a graceful drain
into a dropped request. What the fix removes is the case where a request was
assigned to a connection that had ALREADY been given up on — which is the
amplification described in rationale 2.

**5. Returning `Ok(None)` is the existing contract, not a new error path**

`connectors/http/mod.rs` treats `reused_http_session() == Ok(None)` as "no reuse
available" and falls through to `new_http_session()`. The refused connection is
also evicted as a side effect, because `InUsePool::get()` pops before the filter
runs. No error reaches the proxy and no request is failed by this change.

## Fix Suggestions Not Accepted

- "`more_streams_allowed()` already covers it" — it runs after the stream is
  allocated; it controls pooling, not allocation.
- "Filter at pool selection only" — the connection can be marked between
  selection and allocation; the enforcing check has to be inside `spawn_stream`.
- "Check inside `spawn_stream` only, drop the selection filter" — correct in
  outcome but wasteful: it pops, awaits and discards on a connection already
  given up on. The two layers are deliberately redundant, and the seam scenario
  `upstream_graceful_goaway_finishes_in_flight_and_is_not_reused` now fails only
  when both are removed (measured 2026-08-27; its doc comment records this).
- "Return an error for a shutdown connection so the caller knows" — the caller's
  correct action is to dial a fresh connection, which is what `Ok(None)` already
  means. An error would have to be papered over by the retry loop.
- "Remove the mark on read timeout instead, since the connection may recover" —
  a separate policy question (the disposition of malformed / timeout /
  stream-local failures), deliberately out of scope here and left as a follow-up.

## Re-evaluation Triggers

Re-open this decision only if:

- `shutting_down` gains a clearing path (a connection that can un-shutdown), at
  which point both guards must be re-examined for TOCTOU;
- stream ids start being allocated at `ready()` rather than `send_request()` in
  an upstream `h2` release, which would invalidate the "dropping `SendRequest`
  opens nothing" argument in rationale 4;
- `Stub::new_stream` stops awaiting a fresh clone of `SendRequest`, at which
  point the post-await check in `spawn_stream` becomes load-bearing rather than
  defensive;
- the residual `spawn_stream` → `send_request` gap in rationale 4 is judged
  unacceptable, e.g. because a mark starts carrying "this connection is unsafe"
  rather than "this connection is draining";
- the pool grows an eviction-on-mark path, which would make the selection filter
  genuinely redundant rather than defense-in-depth.

## Reference Cases

- H2-008, whole-change H2 audit 2026-08-26; fixed in pingora fork `edgion_v3`
  2026-08-27.
- `pingora-core/src/connectors/http/v2.rs` — `spawn_stream`,
  `reused_http_session`; unit tests
  `test_spawn_stream_rejects_shutdown_connection` and
  `test_reused_session_skips_shutdown_pooled_connection` (both carry their
  mutation-measured pinning notes).
- Related: [h2-goaway-persistent-ceiling-fail-closed.md](h2-goaway-persistent-ceiling-fail-closed.md) (H2-005) — the
  GOAWAY-side eligibility rule this one complements.
