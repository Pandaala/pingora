# Review knowledge index

Read this before reviewing fork changes. Records here capture decisions, fixed
defects, accepted trade-offs, upstream limitations, and rejected reports so a
later review does not rediscover them without new evidence.

## Classification rule

Classify a candidate as a fork defect, upstream limitation, accepted design,
dismissed/unreachable finding, open investigation, or resolved finding. It is
not new unless code/dependency baseline, a stated premise, or reproducible
evidence changed. A reopened report must name that change.

The `status` inside each record is authoritative. This index does not rewrite
`implemented-pending-project-checks` as `fixed`.

## Accepted boundaries and dismissed findings

- [Known upstream limitations](upstream-limitations.md): `h2` trailer gap,
  retained guards, and adoption triggers.
- [H2 CI enforcement](h2-grpc/h2-ci-contract-enforcement-out-of-scope.md)
  (`wont-fix` in the originating workflow): CI is a separate fork task and a
  blanket ban on ignored H2 tests is invalid.
- [Trailer batch/cache completion](h2-grpc/trailer-batch-latch-cache-completion.md)
  (`wont-fix`): the reported cross-batch hang is unreachable; read its residual
  gap before reopening.

## Implemented or resolved conclusions

- [Read terminal poisoning](h2-grpc/h2-end-stream-observer-read-terminal-poison.md)
  (H2-002): never scan error-returned bytes; poison partial EOF.
- [Persistent GOAWAY ceiling](h2-grpc/h2-goaway-persistent-ceiling-fail-closed.md)
  (H2-005): retain the ceiling and poison illegal GOAWAY.
- [Local reset invalidation](h2-grpc/h2-local-reset-invalidates-shared-evidence.md)
  (H2-006): invalidate shared evidence before local reset.
- [Writer stall after response](h2-grpc/h2-writer-capacity-stall-after-response.md)
  (H2-007): complete evidence plus stall permits upload abandonment; neither
  condition alone does.
- [Shutdown allocation](h2-grpc/h2-shutdown-connection-not-allocatable.md)
  (H2-008): enforce shutdown at allocation and pool selection.
- [Version-robust baselines](h2-grpc/h2-dependency-baseline-tests-version-robust.md)
  (H2-011): characterize both sides of an upstream fix without making unsafe
  behavior a product contract.
- [Cache/reuse harness consumers](h2-grpc/h2-proxy-cache-reuse-harness-consumers.md)
  (H2-012): drive harness capabilities through end-to-end tests.
- [Trailer/Done terminal dispatch](h2-grpc/trailer-done-terminal-body-dispatch.md):
  one latch dispatches EOS and orders released bytes before terminal tasks.
- [Custom terminal 101](http1/custom-terminal-101-normalized-before-dispatch.md):
  normalize `Header(101, true)` to `Header(101, false) -> Done` at the producer.
- [Negative-test observation](testing/negative-tests-need-out-of-band-observation.md):
  failure-path negative assertions need out-of-band evidence and a positive
  control.

## Open review findings

- [003: ResponseBodySink chunk-count bound](findings/003-medium-response-body-sink-does-not-bound-chunk-count.md)
- [004: watcher dependency evidence](findings/004-medium-end-stream-watch-is-not-pinned-to-an-audited-h2.md)
- [007: async response filter boxing](findings/007-low-async-response-filter-adds-per-chunk-boxing.md),
  tracked by [the pending task](../pending-issues/async-response-body-filter-fast-path.md)
- [008: TLS bind-test error classification](findings/008-low-tls-feature-bind-test-rejects-observed-bind-error.md)

Finding 004 predates the accepted normal `h2` version-range policy. Its stale
evidence/docs concern remains fork-owned; its exact-pin recommendation is not
accepted. Follow [upstream policy](upstream-limitations.md) and H2-011.

## Cross-repository rule

The protocol-specific records were moved from `../Edgion/skills/04-review/`
because their implementation lives here. Historical Edgion task references are
provenance, not current ownership. Review the sibling consumer when
reachability, configuration, public hooks, or end-to-end impact depends on it,
and record both revisions.

New durable fork conclusions belong here. Edgion-only product decisions remain
in `../Edgion`; link them instead of copying them.
