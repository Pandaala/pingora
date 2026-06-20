# TODO / Decision: Early request body buffering (before `upstream_peer`)

**Status:** OPEN — deciding whether to adopt upstream PR #816's approach.
**Owner:** Edgion gateway.
**Last updated context:** fork retargeted to upstream `main` (`d9e6d7a`); `edgion` branch sits on top.

## Goal

Read / buffer the **full request body during `request_filter`** (i.e. *before*
`upstream_peer` selection) so Edgion can do:
- content-based routing (pick upstream by `tenant_id` / GraphQL operation / JSON field),
- auth signature verification that needs the whole body before deciding,
- body transformation before upstream selection.

The blocker today: pingora's `request_body_filter` and body proxying only run **after**
`upstream_peer`, so the body is not available when routing is decided.

## Approaches compared

| Approach | Early read | Forward when > limit | Re-inject to upstream | Correct (multi-chunk) | Bounded memory | Official |
|----------|:--:|:--:|:--:|:--:|:--:|:--:|
| sxhxliang fork hack (`request_body` field) | yes | no | no (not in send path) | **no — overwrites, keeps only last chunk** | no (2nd unbounded copy) | no |
| Raise/parameterize `BODY_BUF_LIMIT` (reuse `retry_buffer`) | yes | yes | yes (`get_retry_buffer`) | yes | partial (global fixed) | no |
| **PR #816 — early request body buffering** | yes | yes (or two-layer limit + 413) | yes (auto-forwarded) | yes | yes (per-session configurable) | **yes — maintainer-driven** |

Conclusion so far: **PR #816 is the right design.** Do NOT adopt the sxhxliang hack
(buggy on multi-chunk bodies, doesn't fix > 64K forwarding, reintroduces unbounded
memory). The `BODY_BUF_LIMIT` tweak is a viable stopgap but is superseded by #816.

## PR #816 — what it adds

`Resolves #780`. Opt-in, per-session. New API on `ProxyHttp` / `Session`:
- `early_request_body_buffer_limit() -> Option<usize>` — opt in with a size limit
  (default `None` = off, zero overhead, existing code unaffected).
- `early_request_body_filter()` — per-chunk callback during early buffering.
- `Session::get_buffered_body()` / `set_buffered_body()` — inspect / mutate in
  `request_filter`.
- Buffered body is **automatically forwarded** to upstream during the proxy phase
  (solves the re-injection problem cleanly).
- Size limit enforced in two layers (Content-Length pre-check + streaming accumulation);
  exceeding returns HTTP 413. Handles HTTP/2 requests without `Content-Length`.

## Status / risk (why this is still OPEN)

- **Not merged.** PR is open; maintainer **PiotrSikora** is actively reviewing.
- Maintainer wants design changes before merge — **API will likely change**:
  1. This step should only **pre-read + buffer**; body/header filter ordering in the
     current PR is wrong (body filters run before header filters).
  2. Preferred generic semantics = **buffer-then-stream**: buffer up to the limit for
     peek, then resume reading and forward the remainder after upstream connects —
     **instead of** rejecting large bodies with 413. Edgion's routing use case (small
     JSON bodies) is fine with the simpler full-buffer + 413, but the upstream design
     is converging on the more general behavior.
  3. HTTP/2 no-`Content-Length` handling still under review.
- **Port friction:** #816 is built on current `main`. We just moved `edgion` onto
  `main` (`d9e6d7a`), so a cherry-pick should apply far more cleanly now than it would
  have on the old `0.8.0` base.

## Decision options (pick one)

- [ ] **A — Wait for merge (cleanest).** Track #816; once merged upstream, bump the fork
      base to that version and drop any local patch. Cost: timing uncertain.
- [ ] **B — Cherry-pick #816 onto `edgion` now** as an `edgion:` patch; drop it once the
      PR merges upstream. Cost: PR is a moving target — may need re-porting when its API
      changes.
- [ ] **C — Implement a minimal version of the #816 design on `edgion`** (adopt the
      maintainer's semantics: pre-read + buffer only, expose via `get_buffered_body` in
      `request_filter`, auto-forward; on over-limit choose 413 or buffer-then-stream).
      Do NOT copy sxhxliang, do NOT copy the current #816 verbatim. Cost: we write/own
      the code; smallest if Edgion only needs small bodies.

Current lean: **C** if needed soon (small bodies), then replace with **A** long-term.

## Dependent follow-ups (not part of this decision, but required to ship)

- [ ] **Version alignment:** fork is `0.8.0`, Edgion requests `0.8.1`. `[patch.crates-io]`
      is ignored until aligned (`patch ... was not used`). Bump fork `pingora-*` to
      `0.8.1` or change Edgion's requirement.
- [ ] **Wire `[patch.crates-io]`** in `Edgion/Cargo.toml` → `../pingora/*` (see
      top-level `AGENTS.md`).
- [ ] After adopting any approach, verify via `cd ../Edgion && cargo check --all-targets`.

## References

- Issue #780 — Support Early Request Body Access for Dynamic Upstream Peer Selection:
  https://github.com/cloudflare/pingora/issues/780
- PR #816 — Support early request body buffering before upstream peer selection:
  https://github.com/cloudflare/pingora/pull/816
- Issue #575 — process request_body before upstream_peer (retry-buffering workaround):
  https://github.com/cloudflare/pingora/issues/575
- Issue #349 — forwarding body after reading in request_filter:
  https://github.com/cloudflare/pingora/issues/349
- Issue #692 (comment) — maintainer open to "read, buffer, filter upfront" as opt-in:
  https://github.com/cloudflare/pingora/issues/692#issuecomment-3287367952
