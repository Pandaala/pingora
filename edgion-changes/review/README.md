# Review knowledge index

Read this before reviewing fork changes. Records here capture decisions, fixed
defects, accepted trade-offs, upstream limitations, and rejected reports so a
later review does not rediscover them without new evidence.

## Classification rule

Classify a candidate as a fork defect, upstream limitation, accepted design,
dismissed/unreachable finding, open investigation, or resolved finding. It is
not new unless code/dependency baseline, a stated premise, or reproducible
evidence changed. A reopened report must name that change.

The `status` inside each record is authoritative.

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
- [Writer stall without response END_STREAM](h2-grpc/h2-writer-stall-without-end-stream-bounded.md):
  every H2 capacity wait has a finite progress bound; without qualified
  response END_STREAM, expiry fails the exchange and releases stream capacity.
- [Shutdown allocation](h2-grpc/h2-shutdown-connection-not-allocatable.md)
  (H2-008): enforce shutdown at allocation and pool selection.
- [Version-robust baselines](h2-grpc/h2-dependency-baseline-tests-version-robust.md)
  (H2-011): characterize both sides of an upstream fix without making unsafe
  behavior a product contract.
- [Cache/reuse harness consumers](h2-grpc/h2-proxy-cache-reuse-harness-consumers.md)
  (H2-012): drive harness capabilities through end-to-end tests.
- [Trailer/Done terminal dispatch](h2-grpc/trailer-done-terminal-body-dispatch.md):
  one latch dispatches EOS and orders released bytes before terminal tasks.
- [Compression/trailer terminal ordering](compression/compression-trailer-terminal-order.md):
  finalize compression before trailers and keep the following Done inert.
- [Custom terminal 101](http1/custom-terminal-101-normalized-before-dispatch.md):
  normalize `Header(101, true)` to `Header(101, false) -> Done` at the producer.
- [Custom early-response request abandonment](custom/custom-upstream-early-response-abandonment.md):
  abandonment stops body polling while natural completion retains downstream
  idle monitoring; the second correction is independently reviewed and fixed.
- [Custom writer error context](custom/custom-writer-error-context.md): preserve
  the first upstream writer root cause before asynchronous abandonment and
  joined-future teardown can replace it.
- [Negative-test observation](testing/negative-tests-need-out-of-band-observation.md):
  failure-path negative assertions need out-of-band evidence and a positive
  control.
- [Response trailer filter error parity](response-trailer-filter-error-parity.md):
  H1/H2/custom propagate downstream trailer hook failures consistently; cache
  capability and protocol-specific reuse boundaries are recorded explicitly.
- [ResponseBodySink chunk-count bound](findings/003-medium-response-body-sink-does-not-bound-chunk-count.md):
  independent byte and nonempty-chunk budgets cap downstream and cache fan-out
  per pump batch.
- [Shared response-task pipeline](response-task-pipeline-consolidation.md): H1,
  H2, and custom share hook/cache/terminal/sink semantics while retaining
  explicit protocol framing and upgrade policy.
- [Private protocol test extraction](test-module-extraction.md): twelve large
  H1/H2/proxy suites moved to behavior-grouped sibling modules without changing
  test identities, visibility, ignored coverage, or Cargo target topology.
- [Allocation-free default response body hooks](findings/007-low-async-response-filter-adds-per-chunk-boxing.md):
  a zero-sized boxed default plus direct typed delegation removes allocator
  traffic while preserving object compatibility and real async overrides.
- [H2 watcher dependency evidence](findings/004-medium-end-stream-watch-is-not-pinned-to-an-audited-h2.md):
  stale h2 0.4.15 premises were replaced by an audited 0.4.19 handoff checklist;
  the minimum was raised after 0.4.16-0.4.18 reproducibly failed the fork's
  continuing-upload contract.
- [TLS bind-test error classification](findings/008-low-tls-feature-bind-test-rejects-observed-bind-error.md):
  retain the inherited `InternalError -> BindError -> AddrInUse` production
  chain and use localhost-only deterministic bind/TLS failures in feature tests.

## Open review findings

- [2026-08-28 fork feature and malformed-input audit](fork-feature-malformed-input-audit-2026-08-28.md),
  including the discovery provenance for the resolved custom-upstream
  abandonment and idle-watch corrections.
- [Non-streaming cache trailer completion](../pending-issues/non-streaming-cache-trailer-completion.md):
  a real H1 trailer does not currently finish admission for storage without
  streaming partial-write support.

## Cross-repository rule

The protocol-specific records were moved from `../Edgion/skills/04-review/`
because their implementation lives here. Historical Edgion task references are
provenance, not current ownership. Review the sibling consumer when
reachability, configuration, public hooks, or end-to-end impact depends on it,
and record both revisions.

New durable fork conclusions belong here. Edgion-only product decisions remain
in `../Edgion`; link them instead of copying them.
