---
name: h2-proxy-cache-reuse-harness-consumers
description: Use when reviewing findings that the Pingora fork's H2 proxy test harness (cache entry inspection, injected H2 windows, local response-body failure, upstream socket observability) has no end-to-end callers, or that H2 cache-admission and upstream-reuse contracts lack integration evidence.
status: fixed
finding_id: H2-012
closed: 2026-08-28
---

# H2 proxy cache/reuse harness must be driven by real end-to-end tests — FIXED

## Conclusion

H2-012 reported that the observability added to `pingora-proxy/tests/utils/server_utils.rs`
during the H2 audit had no consumers: the harness could report cache entry
state, inject H2 windows, fail a local response-body filter, and expose the
upstream socket, but nothing exercised any of it. Fixed by adding the standalone
target `pingora-proxy/tests/test_h2_upstream_cache_and_reuse.rs` (7 scenarios)
in the Pingora fork on branch `edgion_v3`.

Fix suggestions of the form "add a CI job for these tests" are **not accepted**
— the fork's `build.yml` already runs `cargo test --lib --bins --tests`, which
picks up every non-`#[ignore]`d target automatically, and CI/CD changes do not
go through the review-issue workflow anyway
([h2-ci-contract-enforcement-out-of-scope.md](h2-ci-contract-enforcement-out-of-scope.md)).
Suggestions to move this coverage into Edgion's `edgion-tests` are **not
accepted** either; see rationale 3.

## Core Rationale

**1. What the coverage asserts, and why keepalive could not**

The contracts that H2-005 / H2-007 / H2-008 and the RFC 9113 section 8.1
handling all depend on are invisible to a downstream-keepalive assertion: a
downstream connection survives plenty of exchanges that corrupted cache or
poisoned the upstream pool. The target therefore asserts on two things the
harness makes observable:

- **Cache state**, read through the cache's own public interfaces
  (`cache_entry_state`, which maps `MemCache::lookup` onto `None` / `Partial` /
  `Complete`; a complete memory hit is seekable, a streaming partial hit is
  deliberately not). `MemMissHandler::finish()` is what commits an object into
  `cached`, and its `Drop` removes the temp object, so a failed exchange that
  still produced a `Complete` entry is exactly the durable corruption to catch.
- **Origin socket identity** (`x-upstream-client-addr`, the proxy's local
  address on the upstream connection), so "stream 1 then stream 3 on one
  connection" is asserted directly rather than inferred. The two streams are
  SEQUENTIAL, not concurrent: `PeerOptions::new()` sets `max_h2_streams: 1`
  (`pingora-core/src/upstreams/peer.rs`), and `HttpPeer::new` uses it -- note
  there is no `HttpPeer::default()`/`PeerOptions::default()` in the tree -- so
  `release_http_session` only returns
  the connection to the idle pool once its stream count reaches zero. That makes
  the socket-identity assertion stronger than a bare reuse check -- it also pins
  the stream slot's release.

The seven scenarios: one positive baseline (complete exchange → `Complete` entry,
next miss carried on the same socket, then a hit that does not reach the
origin), four failure directions that must each leave no complete entry and send
the second request back to the origin (truncation, local response-body filter
failure, invalid trailers, a flow-controlled body cut short), and one
writer-capacity scenario (a request-body write parked on the origin's window for
the whole exchange must still deliver and cache the complete response, and
release the connection for reuse), and one that asserts both peer window
options on the wire (see rationale 2).

**2. Two knobs that look interchangeable and are not**

The `x-h2-stream-window-size` header sets the window the proxy advertises for
RECEIVING the response. The writer-capacity contract of H2-007 is about the
proxy SENDING, which is governed by the origin's advertised
`SETTINGS_INITIAL_WINDOW_SIZE`. Using the injected receive window to build a
writer stall deadlocks the shape rather than testing it: completing the response
requires WINDOW_UPDATEs from a proxy that is parked on the blocked write, so
END_STREAM never reaches the wire and only the origin's reset breaks the
standoff — which then reads as a truncation, not as the section 8.1 shape. The
stall scenario therefore constrains the writer via
`h2::server::Builder::initial_window_size` on the origin, and the injected
receive window is used where it genuinely shapes the failure (the cut-short
scenario).

Two residual points, recorded so a later reviewer does not have to rediscover
them:

- **The relevant default is 8 MiB, not 65535.** Pingora's un-injected upstream
  stream window is `H2_WINDOW_SIZE = 1 << 23`
  (`pingora-core/src/connectors/http/v2.rs`), and `h2_connection_window_size`
  defaults the same way -- the `h2` crate's 65535 is not the baseline on this
  path. Injecting 4096 therefore changes the regime for any body of a sane test
  size. Reasoning from 65535 produces wrong conclusions about whether a
  scenario's body is large enough to matter; that mistake was made once during
  this fix and corrected in review.
- **A connection window at or below 65535 is a NO-OP.** WINDOW_UPDATE can only
  raise a window, so for such a value the proxy sends nothing at all and the
  connection window stays at the protocol default. Measured, not inferred. A
  finding of the form "inject a small connection window and assert the flow is
  throttled" therefore describes a test that cannot fail, and is **not
  accepted**; `h2_peer_window_options_reach_the_upstream_handshake` pins the
  no-op instead.
- **Both window options ARE asserted, on the wire.** They are not carried the
  same way, which is what lets one raw-wire origin separate them: the stream
  window is the SETTINGS parameter `SETTINGS_INITIAL_WINDOW_SIZE` (0x4) and
  carries the configured value directly, while the connection window has no
  SETTINGS parameter at all -- HTTP/2 fixes the initial connection window at
  65535 (RFC 9113 section 6.9.2) and the only way to change it is a
  WINDOW_UPDATE on stream 0, so the configured value appears as an INCREMENT
  above that default. `h2_peer_window_options_reach_the_upstream_handshake`
  pins both against an un-injected control case. Verified by mutation: removing
  the option plumbing from `upstream_peer` turns the scenario red.
  What this does NOT assert is the flow-control BEHAVIOUR that follows from the
  setting; the cut-short scenario still only SHAPES its failure with the
  injected window rather than asserting it.

**3. Cache admission does not exist in Edgion, so this cannot live there**

The Edgion Gateway implements no HTTP response cache. Precisely: no Edgion
`Cargo.toml` declares `pingora-cache` (it appears in `Cargo.lock` only
transitively, as a dependency of `pingora-proxy`), and `edgion-gateway/src/`
contains zero references to `pingora_cache`, `session.cache`,
`request_cache_filter` or `cache_key_callback`. `edgion-gateway/src/cache/` is a
generic in-memory LRU/TinyUFO utility, unrelated to HTTP response caching. Cache
admission therefore never runs in Edgion, so the two acceptance criteria that
assert on cache state have no subject there — an absence of the thing under
test, not a cost question. State it this way rather than "no `pingora-cache`
dependency at all", which a lockfile grep falsifies. The reuse half would additionally require teaching the HTTP/h2 test
backend to echo the proxy's source address (today only the TCP/UDP echo paths
report `peer_addr`), and Edgion's black-box backends can express neither the
raw-wire invalid-trailer shape nor the injected windows. Edgion-side end-to-end
evidence for the H2 fixes remains a real and separate gap, tracked as its own
task.

**4. Test-harness pitfalls this target had to work around**

Both were introduced while writing it and are easy to reintroduce:

- **`h2` connection driver starvation.** An `h2` connection only progresses
  while something polls it. Sleeping inside the `conn.accept()` loop parks the
  whole connection, so queued frames never reach the wire — and `send_reset`,
  which discards the stream's pending send queue, then deletes the very
  response the scenario is about. Either drive the connection during the wait
  (`tokio::select!` over `conn.accept()`, the idiom in
  `test_h2_upstream_no_error_reset.rs`) or move the per-stream work into its own
  task and let the accept loop keep driving. A scenario that skips this can
  still pass, for the wrong reason: the truncation scenario shipped this way in
  the first draft of the target and was vacuous -- the proxy failed at
  `while reading h2 header` (a bare reset on a stream that never carried a
  response) rather than mid-body, so `assert_ne!(.., Complete)` held because
  nothing cacheable had ever existed. Independent review caught it. When
  checking such a scenario, read the proxy's own error context: `while read h2
  response body` is the truncation shape, `while reading h2 header` is not.
- **Abandoned uploads cost downstream keepalive.** When the proxy stops
  forwarding a request body, the downstream connection cannot be kept alive, so
  a large response body races that close. Scenarios whose subject is the writer
  window keep the response short; large flow-controlled bodies belong to the
  scenarios that are actually about the receive window.

## Fix Suggestions Not Accepted

- "Add these tests to a mandatory CI job" — the fork's `build.yml` runs
  `cargo test --verbose --lib --bins --tests --no-fail-fast` on its nightly and
  current-stable legs, so a new non-ignored target is already enforced; and
  CI/CD work is out of scope for this workflow.
- "Put the coverage in `edgion-tests` instead" — the cache contracts have no
  subject there (rationale 3).
- "Assert reuse from downstream keepalive / `x-cache-status` alone" — both
  survive the failures this target exists to catch; the acceptance criteria
  name origin socket identity and cache state for that reason.
- "Assert the connection window by building a scenario whose flow-control
  outcome depends on it" — the connection window is not on the causal path of
  any of the cache/reuse verdicts, and `max_h2_streams` is 1 so no two streams
  ever share it. It is asserted where it IS expressible (the handshake
  WINDOW_UPDATE), not by a behavioural scenario that would assert nothing. An
  earlier revision of this entry declined to cover the option at all; that was
  superseded once measurement showed the handshake is observable.
- "Claim the reuse assertion proves no pump task leaked" — it proves the stream
  and writer capacity were RELEASED (a pump still parked on the write half
  could not be followed by a successful exchange on that connection). There is
  no in-process handle on a tokio task; the test's doc comment states this
  boundary and must keep stating it.

## Re-evaluation Triggers

Re-open this decision only if:

- Edgion gains an HTTP response cache (a `pingora-cache` dependency), which
  would give the cache-admission contracts a subject in that repository;
- `MemCache`'s admission model changes such that `can_seek()` no longer
  separates a complete entry from a streaming partial one, which is what
  `cache_entry_state` relies on;
- `h2_stream_window_size` / `h2_connection_window_size` enter the peer reuse
  hash (they are excluded today, see `pingora-core/src/upstreams/peer.rs`),
  which would change how a window-injecting request shares the connection pool;
- the upstream `h2` decoder fix of H2-001 lands, which changes what a trailer
  block is allowed to be and therefore what the invalid-trailer scenario pins.

## Reference Cases

- H2-012, whole-change H2 audit 2026-08-26; closed `fixed` 2026-08-28. Source
  issue: `../Edgion/tasks/todo/pingora-h2-end-stream-watch-simplification/issues/H2-012-proxy-harness-end-to-end-coverage.md`.
- Fork-side change: `pingora-proxy/tests/test_h2_upstream_cache_and_reuse.rs`
  (new target) and `edgion-changes/verification/test-matrix.md` (target list), branch
  `edgion_v3`.
- Harness under test: `pingora-proxy/tests/utils/server_utils.rs` —
  `cache_entry_state` / `CacheEntryState`, `x-h2-stream-window-size`,
  `x-h2-connection-window-size`, `x-test-local-response-body-failure`, and the
  `CacheCTX` upstream socket fields surfaced as `x-conn-reuse` /
  `x-upstream-client-addr` / `x-upstream-server-addr`. All four additions named
  in H2-012's evidence section now have callers.
- Contracts this target guards end to end:
  [h2-goaway-persistent-ceiling-fail-closed.md](h2-goaway-persistent-ceiling-fail-closed.md) (H2-005),
  [h2-writer-capacity-stall-after-response.md](h2-writer-capacity-stall-after-response.md) (H2-007),
  [h2-shutdown-connection-not-allocatable.md](h2-shutdown-connection-not-allocatable.md) (H2-008).
- CI scope rule that governs the "add a CI job" suggestion:
  [h2-ci-contract-enforcement-out-of-scope.md](h2-ci-contract-enforcement-out-of-scope.md) (H2-009).
- Environment prerequisite shared by every `pingora-proxy` integration target:
  `client_bind_to_ipv4: 127.0.0.2` in `pingora-proxy/tests/pingora_conf.yaml`
  needs a loopback alias on macOS (`sudo ifconfig lo0 alias 127.0.0.2 up`);
  without it every request fails to connect and reports 502.
