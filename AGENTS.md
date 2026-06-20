# Pingora Fork (Edgion) — AI Agent Guide

This repository is **Edgion's fork of [cloudflare/pingora](https://github.com/cloudflare/pingora)**.
It exists so the Edgion gateway can carry local patches to pingora that are not (yet)
upstream, while still tracking the official project.

## Remotes

| Remote     | URL                                          | Role                                  |
|------------|----------------------------------------------|---------------------------------------|
| `origin`   | `git@github.com:Pandaala/pingora.git`        | Our fork (push patches here)          |
| `upstream` | `https://github.com/cloudflare/pingora.git`  | Official pingora (read-only, sync in) |

## Branch model

| Branch     | Purpose                                                                              |
|------------|-------------------------------------------------------------------------------------|
| `main`     | Clean mirror of upstream. **Do NOT commit Edgion changes here.** Used only to track and pull official history. |
| `edgion`   | Long-lived patch branch. **All Edgion-specific changes live here.** This is the branch Edgion builds against. |

`edgion` is rebased on top of upstream when we pull new official commits, so its
history stays as "upstream + a small, reviewable stack of our patches".

> Note: `connection_filter` is an **upstream** feature, not one of our patches.
> Do not list it as a local change.

## How Edgion consumes this fork

Edgion lives next to this repo (`ws3/Edgion`, sibling of `ws3/pingora`) and redirects
the published `pingora-*` crates to this checkout via `[patch.crates-io]` in
`Edgion/Cargo.toml`, e.g.:

```toml
[patch.crates-io]
pingora-core           = { path = "../pingora/pingora-core" }
pingora-http           = { path = "../pingora/pingora-http" }
pingora-proxy          = { path = "../pingora/pingora-proxy" }
pingora-limits         = { path = "../pingora/pingora-limits" }
pingora-load-balancing = { path = "../pingora/pingora-load-balancing" }
pingora-ketama         = { path = "../pingora/pingora-ketama" }
```

`[patch]` redirects the **whole** dependency graph (including transitive deps such as
`pingora-proxy` → `pingora-core`) to this fork, so only one copy of each crate is
compiled. Using direct `path =` dependencies instead would compile two incompatible
copies of `pingora-core` and fail to build.

**Version invariant:** `[patch]` only takes effect when the crate version in this fork
**satisfies** the version Edgion requests. If Edgion asks for `pingora-core = "0.8.1"`
but this fork were `0.8.0`, Cargo would print `patch ... was not used` and silently use
crates.io instead. The `edgion` branch is therefore bumped to `0.8.1` (all `pingora-*`
crate versions + internal deps, lockstep) to match Edgion's requirement. `main` stays at
the clean upstream `0.8.0`; the bump is an `edgion`-branch customization.

## Knowledge base

Task-oriented guides live in `skills/`. Start at `skills/SKILL.md`.

- **Sync from upstream / apply patches / caveats** → `skills/upstream-sync/SKILL.md`

## Hard rules

- Never push Edgion changes to `main`; use `edgion`.
- Never commit directly to `upstream` (read-only).
- All content written to disk (code, comments, docs) is in **English**.
- After any rebase or patch, re-verify by building **Edgion** against this fork
  (`cargo check` in `ws3/Edgion`), not just this repo in isolation.
