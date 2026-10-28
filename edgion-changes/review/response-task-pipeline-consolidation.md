# Shared H1/H2/custom response-task pipeline

## Status

Resolved fork maintenance finding on 2026-08-29. Implementation baseline:
working tree based on `bd89d47`.

## Decision

The protocol pumps retain transport reads/writes, cancellation, response-source
completion, and connection/stream reuse. Their duplicated response task
transformation now lives in `pingora-proxy/src/response_pipeline.rs`:

```text
upstream hook + terminal latch
  -> cache admission
  -> downstream header/body/trailer transforms
  -> sink drain and terminal reconciliation
  -> prepared task batch
```

`ResponsePipelineState` owns only the six values whose lifetime is one response
transformation: range state, body suppression, cached terminal-header
substitution, upstream reuse safety, emitted chunks, and terminal dispatch.
Session, application context, cache source state, and protocol writers remain
outside it.

`ResponseProtocol` is a closed enum rather than dynamic dispatch. It expresses
the retained H1 101 checks and framing, H2 upgraded-body rejection, and custom
framing/101 ordering. The existing custom conditional-filter predicate is not
accepted as a permanent protocol difference; it is preserved for a
behavior-neutral refactor and tracked by
[the canonical pending investigation](../pending-issues/custom-conditional-filter-gate.md).

## Equivalence evidence

- The three former 300-line `*_response_filter` functions were removed; every
  live, compression-prefix, and cache-hit call site now enters the same shared
  function.
- A table-driven executable comparison feeds live
  `Header/Body/Trailer/Done/Failed` and cached `Header/Body/Done` sequences
  through H1, H2, and custom policies, comparing hook order, emitted task
  order, suppression, and reuse state.
- Existing standalone suites cover terminal Header/Body/Trailer/Done, cache
  hit/fill, sink emission/terminate, custom writer and early response, H1 101,
  H2 reset, and reuse behavior.

## Performance evidence

The existing public-hook benchmark does not enter the pipeline, so an ignored
release microbenchmark now exercises the actual H1 per-body-task shared path.
The same workload and allocator counter were temporarily applied to the clean
`bd89d47` H1 implementation using the same lockfile and machine:

| implementation | ns/task | allocations/task |
| --- | ---: | ---: |
| `bd89d47` legacy H1 function | 79.37 | 2.0000 |
| shared pipeline | 73.05 | 2.0000 |

One wall-clock sample is not a stable performance claim; the useful closure
evidence is no allocation increase and no observed throughput regression. The
shared implementation adds no trait object, per-task container, or explicit
heap allocation. Run the maintained benchmark with the command documented on
`benchmark_response_task_pipeline`.

## Review and verification

An independent review first found that the adapter-only table and the existing
hook benchmark did not exercise the new pipeline. Both gaps were corrected;
the reviewer then re-reviewed the resulting implementation.

Validated suites and exact counts are maintained in
[`verification/test-matrix.md`](../verification/test-matrix.md).

## Revisit triggers

### Held response trailer capability

ISSUE-003 is a fork-owned ordering defect corrected on 2026-10-27
(regression guard). Hold released at `TerminalBeforeTrailers` previously ran
downstream trailer hooks before selected-header preparation. An H1 hook saw
an uninitialized planned capability as false even when the selected writer
would use chunked framing. This violates the existing
[same-batch planning contract](../features/response-trailers.md#same-batch-planning).

Ordinary released output now performs writer claim and header preparation
before downstream body/trailer filters. Undecided Hold remains unprepared;
header modules still run exactly once. Terminal-header and replacement output
retain filtering and length reconciliation before preparation: they have no
retained origin trailer sequence and must compute the final body length first.

`response_pipeline_trailer_capability_tests.rs` exercises H1/H2 origin policies
with an actual H1 downstream writer, Immediate, release on data, and release
on trailer, HTTP/1.0 and fixed-length negative cases, and header modules that
switch framing in both directions. It checks capability inside the hook,
exact trailer delivery, and one module invocation through writer handoff.
Its failure-path case verifies that a downstream trailer-hook error propagates
after preparation without committing the head or writing response bytes.

Verification on the corrected working tree: proxy library 233 passed, three
existing ignored tests; focused core H1 library 389 passed; the six proxy
integration suites (request seam, terminal dispatch, body sink, H2 reset,
H2 stalled response, H2 cache/reuse) passed 68/32/66/10/4/8 cases. The new
capability test fails on the original implementation with `false` instead of
`true` for trailer-triggered Release. Core/proxy all-target compilation and
Clippy passed (existing warnings remain); formatting and diff checks passed.
Three independent read-only reviewers checked ordering, protocols/cache, and
the Edgion consumer; none found a blocking issue.

The first integration attempt failed because this macOS host lacks the
fixture's `127.0.0.2` source-bind alias. Passing integration runs used the same
built test executables with a copied fixture directory whose source bind was
`127.0.0.1`; repository fixtures and host network configuration were unchanged.
These results do not claim a full core/TLS matrix or a rebuilt Edgion binary.

Consumer inspection used Pingora `6422c882` and Edgion `60699637`; Edgion's
lockfile selects Pingora `1aaa45cc`. Its downstream trailer hook does not query
capability; no deployed Edgion failure or consumer adoption is claimed.
This does not change the separately recorded upstream h2 decoder limitation.

Revisit when Release filtering, terminal reconciliation, or header-module
preparation moves, especially if replacement responses gain trailers.

### Other pipeline changes

- a fourth response transport or a new protocol-specific task variant;
- a change to cache admission, terminal task ordering, or sink drain ordering;
- enabling custom upstreams in Edgion, especially before resolving the
  conditional-filter gate;
- a compiler/profile change that materially changes the maintained pipeline
  benchmark.
