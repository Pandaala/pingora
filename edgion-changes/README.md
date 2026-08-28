# Edgion fork knowledge index

This is the single maintenance area for behavior, architecture, unfinished
work, and durable review knowledge specific to the Edgion Pingora fork. Begin
here, then open only the smallest subtree relevant to the task.

## Choose a path

| Task | Read first | Then read |
| --- | --- | --- |
| Understand the workspace or trace a request | [architecture.md](architecture.md) | Only the source paths named for that flow |
| Review or change a fork feature | [features.md](features.md) | The matching file under [features/](features/) |
| Investigate unfinished work | [pending-issues/README.md](pending-issues/README.md) | The one issue and its linked review record |
| Perform code review | [review/README.md](review/README.md) | Matching protocol/category records; then `../Edgion` when required |
| Rebase onto upstream | [maintenance/upstream-sync.md](maintenance/upstream-sync.md) | [verification/test-matrix.md](verification/test-matrix.md) and affected contracts |
| Adopt a new `h2` release | [review/upstream-limitations.md](review/upstream-limitations.md) | H2 records, trailer issues, and the test matrix |

## Directory contract

- `architecture.md`: crates, data flow, cross-repository seams, and review path.
- `features.md`: authoritative fork feature inventory and commit provenance.
- `features/`: behavioral contracts that must survive upstream sync.
- `maintenance/`: upstream synchronization policy and conflict hot spots.
- `verification/`: commands, test ownership, prerequisites, and snapshots.
- `pending-issues/`: actionable open, blocked, or deferred work.
- `review/`: accepted designs, upstream limitations, dismissed findings,
  resolved findings, and review-originated open findings.

## Sources of truth

Implementation and executable tests outrank prose. Within this tree,
`features.md` owns the inventory, each detailed feature owns its contract, each
pending issue owns its action/status, and each review record owns its rationale.
Indexes summarize and link; they must not silently redefine those records.

The sibling `../Edgion` repository owns how the gateway configures and consumes
these APIs. This checkout owns the generic protocol/proxy implementation.
Always distinguish the sibling checkout from the revision selected by Edgion's
manifest and lockfile.

## Baseline provenance

- Original feature base: Pingora `0.8.1` at `719ef6c`.
- Original documented feature head: `edgion` at `57f6183`.
- Migration base: upstream `main` at `09696b5`.
- Migrated stack: `edgion_v3`, beginning with `64d2690` through `db3de91`,
  followed by review-driven corrections.
- The original `edgion` and `edgion_v2` refs are historical evidence; do not
  rewrite them while maintaining the current fork.
