---
name: pingora-fork-skills
description: Root navigation for the Edgion pingora-fork knowledge base. Read this first, then drill into the relevant skill.
---

# Pingora Fork — Skills Index

Task-oriented knowledge for this Edgion fork of pingora. Load on demand; do not read
everything at once.

| Topic | When to read | Path |
|-------|--------------|------|
| Sync from upstream, apply/maintain patches, version & build caveats | Pulling official updates, rebasing the `edgion` branch, debugging a `patch ... was not used` warning, or adding a new local patch | `upstream-sync/SKILL.md` |
| Early request body buffering (read body before `upstream_peer`) — open decision on adopting PR #816 | Deciding/implementing content-based routing or full-body auth that needs the request body before upstream selection | `request-body-buffering/TODO.md` |

For the high-level repo model (remotes, branch roles, how Edgion consumes the fork),
see the top-level `AGENTS.md`.
