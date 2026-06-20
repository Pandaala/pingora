---
name: upstream-sync
description: Use when pulling official pingora updates into this fork, rebasing the edgion patch branch, adding/maintaining a local patch, or debugging a "patch ... was not used" build failure in Edgion.
---

# Syncing Upstream & Maintaining Patches

This fork tracks `cloudflare/pingora` and carries Edgion-specific patches on top.
This skill is the canonical procedure for keeping the two in sync and for adding
new patches without breaking the Edgion build.

## Mental model

- `main` = clean mirror of `upstream/main`. No Edgion edits ever land here.
- `edgion` = upstream history **plus** a small, rebaseable stack of our patches.
  Edgion always builds against `edgion`.
- Edgion redirects `pingora-*` to this checkout via `[patch.crates-io]` (see top-level
  `AGENTS.md`). Anything you change here is picked up by Edgion on its next build.

```
upstream/main ──●──●──●──●          (official)
                       \
edgion          ────────●──●        (our patches, rebased onto upstream)
```

## One-time setup (already done, listed for reference)

```bash
git remote add upstream https://github.com/cloudflare/pingora.git   # read-only official
git switch -c edgion main                                            # long-lived patch branch
```

`origin` = `git@github.com:Pandaala/pingora.git` (our fork). Push patches to
`origin/edgion`; never push our work to `main`.

## Adding a new patch

1. `git switch edgion`
2. Make the change. Keep each logical change as **its own commit** with a clear,
   English message prefixed so it is easy to spot among upstream commits, e.g.
   `edgion: <what & why>`. Small, isolated commits rebase far more cleanly than one
   giant blob.
3. Verify against Edgion, not just this repo:
   ```bash
   cd ../Edgion && cargo check --all-targets
   ```
4. `git push origin edgion`

Prefer **additive, feature-gated** changes (new module / new Cargo `[features]` flag)
over editing existing upstream lines in place — gated additions almost never conflict
on rebase, in-place edits to hot files do.

## Syncing in new upstream commits

```bash
# 1. Get the latest official history
git fetch upstream

# 2. (optional) fast-forward our clean mirror
git switch main
git merge --ff-only upstream/main      # main has no local commits, so this is safe
git push origin main

# 3. Replay our patches on top of the new upstream
git switch edgion
git rebase upstream/main               # or rebase onto a specific tag, see below
```

Resolve any conflicts commit-by-commit (`git rebase --continue` after each), then:

```bash
# 4. Re-verify the FULL Edgion build — a clean rebase can still break compilation
cd ../Edgion && cargo check --all-targets

# 5. Force-push the rebased branch (history was rewritten)
git push --force-with-lease origin edgion
```

> Use `--force-with-lease`, never a bare `--force`: it refuses to overwrite if someone
> else pushed to `origin/edgion` in the meantime.

### Pin to a released tag instead of `main`

`upstream/main` is a moving target. To track a stable release, rebase onto a tag:

```bash
git fetch upstream --tags
git rebase v0.8.1            # replace with the target release tag
```

This is the safer choice when Edgion expects a specific published version (see the
version invariant below).

## Caveats (read before every sync)

1. **Version invariant — the #1 silent failure.**
   `[patch.crates-io]` is ignored unless this fork's crate version **satisfies** the
   version Edgion requests in `Edgion/Cargo.toml`. Mismatch ⇒ Cargo prints
   `warning: Patch ... was not used in the crate graph` and silently links the
   crates.io copy, so your patched code simply does not run.
   - Current state: the `edgion` branch is bumped to `0.8.1` (all `pingora-*` crate
     versions + internal deps, lockstep) to match Edgion's `pingora-* = "0.8.1"` request.
     `main` stays at the clean upstream `0.8.0`. When rebasing `edgion` onto a newer
     upstream, re-apply this bump (or carry it as a dedicated `edgion:` commit).
   - Fix by bumping every `pingora-*/Cargo.toml` `version` in this fork to match (and
     internal `pingora-*` dep version requirements that reference each other), or by
     aligning Edgion's requirement to this fork's version.
   - After any version change, run `cargo metadata` / a build in Edgion and grep the
     output for `was not used` to confirm the patch is live.

2. **`connection_filter` is upstream, not ours.** It ships in official pingora. Do not
   list it as a local patch and do not "re-add" it after a rebase.

3. **Verify through Edgion, always.** This repo compiling in isolation proves nothing —
   Edgion enables specific features (`connection_filter`, a TLS backend, etc.) and uses
   APIs this repo's own tests may not. The real gate is `cargo check --all-targets` in
   `../Edgion`.

4. **TLS backend features are mutually exclusive.** Edgion selects `boringssl` **or**
   `rustls` on `pingora-core` (never both — resolver 2 keeps them apart). If you touch
   `pingora-core`'s feature wiring, do not union the TLS backends.

5. **`path` vs `git` in Edgion's patch.** Local dev uses `path = "../pingora/..."`
   (instant rebuilds). For CI / reproducibility, switch the patch to
   `git = "...Pandaala/pingora.git", branch = "edgion"` (or pin a `rev`). A relative
   `path` does not exist on a CI runner.

6. **Rebase rewrites history.** `origin/edgion` will diverge after a rebase; always
   `--force-with-lease`. Never rebase or force-push `main`.

## Quick checklist

- [ ] `git fetch upstream`
- [ ] `main` fast-forwarded to `upstream/main` (optional)
- [ ] `edgion` rebased onto `upstream/main` (or a release tag)
- [ ] conflicts resolved, each patch commit still isolated
- [ ] version invariant satisfied (no `was not used` warning)
- [ ] `cd ../Edgion && cargo check --all-targets` passes
- [ ] `git push --force-with-lease origin edgion`
