# H2 scanner payload-state consolidation

Status: implemented, verified, and independently approved by Astra on 2026-10-06.

## Scope and preserved contract

The 2026-10-06 refactor replaces the scanner's three mutually exclusive payload
options with `PayloadState`. Only one frame can be partially consumed at a time.
Pad Length consumption replaces padded state with terminal state or `Skip`;
terminal DATA publication and GOAWAY completion reset their state before acting.
Poison clears all partial state and cached handles. EOF still checks both staged
bytes and non-`Skip` state explicitly.

Byte count timing, complete terminal payload/padding, terminal HEADERS observation,
RST ordering, registration/publication locking, local invalidation, persistent
GOAWAY eligibility/poison, cache freeze/lifetime, and IO forwarding are preserved.
Client completion/trailer APIs, writer policy, cache admission, and dependency
sources are outside the production diff. The upstream trailer-validation
limitation and whole-watcher deletion remain deferred.

Baseline source: `e18250efb07a66439d04f0e03df73c3364c23af3`. The test-only
`end_stream_watch_reference.rs` freezes its parser and scanner helpers;
unchanged watch primitives/constants are shared. Differential snapshots compare
published flags, unpublished counts, HEADERS markers, invalidation, pending ids,
GOAWAY ceiling/poison, partial state, cache ids, generations, and strong ownership
counts after every feed or lifecycle event. They cover every split of the focused
frame matrix plus 200 deterministic randomized histories.

## Fresh evidence

- Original watcher: 54 passed, eight ignored; original client: 26 passed,
  eight ignored. Original watcher test identities are all retained (62 total);
  three new tests cover differential state and layout. Ignore reasons remain
  unchanged.
- Candidate watcher: 57 passed, eight ignored. Focused H2 core: 146 passed,
  16 ignored. Full feature/proxy gates are recorded in the verification matrix
  after completion; old ignored decoder contracts are not closure evidence.
- Twelve isolated mutations were killed by targeted assertion failures:
  byte equality, padding exclusion, full terminal payload boundary, terminal
  HEADERS marking, shared local invalidation, persistent GOAWAY ceiling,
  malformed GOAWAY, increasing GOAWAY, read-error poison, partial-EOF poison,
  terminal cache eviction, and reserved stream-id masking. Shared primitives
  are checked by explicit expected-result tests rather than by the oracle alone.

## Value and performance limits

On this macOS aarch64 build, production and test scanner layout both decrease
from 136 to 96 bytes (40 bytes, approximately 29%). Production-file total lines
are 1080 to 1079; nonempty lines excluding line comments are 563 to 575. This
is state consolidation, not completion of the original parser deletion target.
Tests and the frozen oracle are measured separately and do not count as savings.

Release-mode old/new measurements alternate order within one optimized test
build. Three repeat runs of unchanged candidate SHA-256
`dc9019eee7b718e5a81da9eb37fd15c1ba99a79da4866fe2da7311aa2e213f41`
gave the following median-of-run medians (nanoseconds per workload iteration):

| Workload | Original | Candidate |
| --- | ---: | ---: |
| One terminal DATA plus registration | 32.12 | 32.85 |
| 1024 one-byte DATA, terminal last frame | 4027.83 | 3674.40 |
| 1 MiB, 64 x 16 KiB DATA | 403.51 | 412.30 |
| One terminal DATA, read splits every three bytes | 43.87 | 45.79 |
| 1 MiB, 16 x 64 KiB DATA, splits every 4096 bytes | 877.62 | 875.42 |
| One terminal padded DATA, splits every three bytes | 54.00 | 61.89 |
| GOAWAY, splits every three bytes | 33.69 | 38.90 |
| 1024 nonterminal DATA, two alternating streams | 4575.17 | 4308.54 |
| 1024 nonterminal DATA, eight round-robin streams | 9695.42 | 8870.42 |

The terminal-padded and GOAWAY microcases have a repeatable approximately 5-8 ns
cost, while common streaming/multiplex cases improved and large split payloads
were comparable. These are local scanner microbenchmarks, not gateway throughput
measurements. The standalone diagnostic used optimized scanner code with debug
dependency artifacts and different frame shapes; it is not release acceptance
evidence. Earlier generalized dispatch shapes were discarded after regressions.

Reproduce with `cargo test -p pingora-core --release --lib
benchmark_end_stream_watch -- --ignored --nocapture --test-threads=1`. Preserve
the source/build and compare identical workloads when interpreting differences.

## Independent final review

A fresh Astra reviewer returned APPROVED with no must-fix finding. It verified
the actual diff and oracle provenance, preserved test identities/attributes,
exact isolated overlay, source hash, and twelve raw mutation assertion failures.
Its independent watcher rerun passed 57 tests with eight original ignores.
The documented layout/performance tradeoff was accepted within this limited
scope; neither consumer adoption nor upstream trailer closure was inferred.

## Re-evaluation boundary

Changing DATA count timing, publishing before full payload, sharing scanner state
between connections, relaxing any retained guard, or upgrading h2 requires a new
contract audit. Do not infer trailer validity or cache success from this enum or
from raw END_STREAM. See the feature contract and known upstream limitations.
