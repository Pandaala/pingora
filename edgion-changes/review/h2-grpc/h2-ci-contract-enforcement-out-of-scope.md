---
name: h2-ci-contract-enforcement-out-of-scope
description: Use when reviewing findings that ask CI to enforce the H2 security contracts or an h2 dependency-version matrix — covers why the remaining `#[ignore]` markers in the Pingora fork's H2 code are not a code defect and why the enforcement job is not built through the review-issue workflow.
status: wont-fix
finding_id: H2-009
closed: 2026-08-28
---

# Enforcing the H2 security contracts in CI is a CI/CD change, not a review fix

## Conclusion

H2-009 asked for a CI job that (a) proves no required H2 security contract is
still `#[ignore]`d, (b) runs the enabled core / watcher / proxy-reuse /
cache-admission suites against both the declared minimum and the current `h2`
resolution, and (c) prepares the loopback source address the proxy tests need.
Every part of that lands in workflow YAML and dependency-resolution
configuration — in the Pingora fork repository, not in Edgion — and touches no
production code. The maintainer's ruling at closure — and the rule this entry
establishes, since no earlier on-disk statement of it exists — is that CI/CD
changes are not made through the review-issue workflow, so the finding is closed
`wont-fix` at the review layer.

Fix suggestions of the form "just add the matrix job now" or "add a guard that
fails when any H2 test is `#[ignore]`d" are **not accepted** as review fixes.
The second one is also wrong on its own terms today; see rationale 2.

## Core Rationale

**1. The finding is CI-only, and its blast radius is another repository**

The three acceptance tests map onto: a GitHub Actions workflow, a `Cargo.toml`
dependency-resolution leg, and an `ip addr add` style pre-test step. The
contracts they would guard live in the Pingora fork
(`pingora-core/src/protocols/http/v2/`, branch `edgion_v3`), whose CI is
`.github/workflows/build.yml`. Edgion's own nine workflows contain no
H2 job and would not gain one. The review-issue workflow's close gate
(`cargo check --workspace`, `validate_agent_docs.py`, `check_ssa_force.py`) is
Edgion-specific and cannot verify a change made in the fork repository at all,
which is the structural reason this class of finding does not belong here.

**2. A blanket "no H2 contract may stay ignored" guard would be red by design**

Counted on 2026-08-28, `pingora-core/src/protocols/http/v2/` carries sixteen
`#[ignore]` markers in three semantically distinct groups, and a guard that
cannot tell them apart fails immediately:

- **eight in `client.rs`** — trailer contracts. Five name the upstream gap
  directly ("requires decoder-level rejection of pseudo-headers in trailers",
  "requires decoder-level rejection before valid empty trailers can be
  accepted", "requires decoder rejection and final terminal-HEADERS poll
  latch"); the other
  three describe the Pingora-side consequence instead ("current repeated body
  EOF poll can consume validated empty trailers", "current fail-closed adapter
  cannot accept validated empty trailers", "current direct `read_trailers()`
  does not consult terminal HEADERS state"). All eight resolve to the same
  blocker: H2-004 records that the empty-trailer ambiguity cannot be settled
  until the upstream `h2` decoder fix of H2-001 (P2, deferred pending upstream)
  lands. They MUST stay ignored until that fix is adopted.
- **seven in `end_stream_watch.rs`** — all marked "characterizes unsafe
  pre-poison behavior; not a passing contract". These are negative
  characterizations of the defect, kept as documentation of what the fix
  prevents. They must NEVER be enabled.
- **one more in `end_stream_watch.rs`** — a manual performance measurement,
  which is why a raw `grep -c` of that file reports eight, not seven.

So zero of the sixteen can be enabled today, and the source carries no
machine-readable marker distinguishing "required, waiting on upstream" from
"must never run". Building the guard therefore starts with designing a contract
manifest, not with writing a workflow step — which is a design task, not a
narrow review fix.

**3. The enforcement gate itself is not withdrawn, only its handling route**

H2-009's own text opens with "**When adopting an upstream fix**, add the focused
contracts to an appropriate existing or new CI job" — it was always defined as a
gate to be built at upstream-adoption time. `issues/README.md` for the H2 audit
states the standing rule directly: an issue is not closed merely because an
ignored contract exists; the contract must be enabled, pass at the declared `h2`
minimum and the current resolution, and receive independent review. That rule
survives this closure. What is closed is the attempt to satisfy H2-009 as a
review-issue code fix.

**4. What exists today instead, and what it does not cover**

`edgion-changes/verification/test-matrix.md` records the verification commands and
a validated snapshot (2026-08-26). It is a manually executed, manually
transcribed document: it lists `cargo test -p pingora-core --lib`,
`-p pingora-proxy --lib`, and the four standalone targets
(`test_request_body_seam`, `test_upstream_response_body_sink`,
`test_terminal_body_dispatch`, `test_h2_upstream_no_error_reset`). Read it as a
point-in-time record, not as evidence about the markers counted in rationale 2:
that snapshot targets `main` at `09696b5` and reports "686 passed, 2 ignored",
which predates the current ignore set on `edgion_v3`.

The fork's `build.yml` runs a generic `cargo test --lib --bins --tests` on two
of its three toolchain legs (the `1.85.0` MSRV leg is gated to fmt/check only),
which does execute the enabled contracts but pins nothing about `h2`. The
2026-08-29 manual dependency audit raised the declaration to
`h2 = ">=0.4.19"`; the minimum and current resolution are both 0.4.19 at this
snapshot. The CI job still resolves only one version, so this temporary
coincidence is not a minimum/current matrix and will stop covering both ends as
soon as the normal upstream resolution advances. That automation gap remains
real and deliberately out of scope here.

## Fix Suggestions Not Accepted

- "Add the h2 minimum/current dependency matrix job now" — a CI/CD change; out
  of scope for the review workflow regardless of its merit.
- "Add a CI check that fails when any H2 test is `#[ignore]`d" — would fail on
  every commit until the upstream `h2` decoder fix is adopted, and would also
  demand the deletion of the seven `end_stream_watch.rs` negative
  characterizations, which exist precisely to stay ignored.
- "Delete or enable the remaining `#[ignore]` markers so the guard passes" —
  inverts the contract: the markers record blocked and never-enable states, and
  removing them destroys the evidence H2-001/H2-004 depend on.
- "Move the enforcement into Edgion's CI instead, since that repo is in scope" —
  the tests do not live there; Edgion consumes the fork as a Git dependency and
  cannot run the fork's unit contracts.
- "Close H2-001/H2-004 too, since their contracts are ignored anyway" — barred
  by the H2 audit README's standing rule quoted in rationale 3.

## Re-evaluation Triggers

Re-open this decision only if:

- the upstream `h2` decoder-level trailer validation fix is adopted, which
  unblocks the eight `client.rs` contracts and makes the enforcement gate
  buildable as originally specified;
- the H2 contracts move into the Edgion repository, or the Pingora fork's CI
  starts being maintained through this workflow;
- the rule this entry establishes — CI/CD changes are not handled through the
  review-issue workflow — is lifted or superseded by a rule recorded elsewhere;
- `Cargo.toml` replaces the `h2 = ">=0.4.19"` range with an exact pin,
  which changes what a dependency matrix would even mean.

## Reference Cases

- H2-009, whole-change H2 audit 2026-08-26; closed `wont-fix` at the review
  layer 2026-08-28 on the CI/CD scope rule. Source issue:
  `../Edgion/tasks/todo/pingora-h2-end-stream-watch-simplification/issues/H2-009-ci-security-contract-enforcement.md`.
- Blocking dependencies: H2-001 (h2 trailer decoder rejection, deferred pending
  upstream) and H2-004 (trailer API terminal-error latch, blocked by H2-001).
- Fork-side state: `edgion-changes/verification/test-matrix.md` (manual
  verification matrix), `.github/workflows/build.yml` (toolchain matrix,
  no `h2` dependency legs), `edgion-changes/pending-issues/h2-trailer-validation.md`.
- Related: [pingora-fork-branch-lock-policy.md](../../../../Edgion/skills/04-review/supply-chain/pingora-fork-branch-lock-policy.md)
  — how the fork family is pinned and why `--locked` builds are the existing
  reproducibility guarantee.
- Sibling H2 fixes whose contracts this job would have guarded:
  [h2-goaway-persistent-ceiling-fail-closed.md](h2-goaway-persistent-ceiling-fail-closed.md) (H2-005),
  [h2-writer-capacity-stall-after-response.md](h2-writer-capacity-stall-after-response.md) (H2-007),
  [h2-shutdown-connection-not-allocatable.md](h2-shutdown-connection-not-allocatable.md) (H2-008).
