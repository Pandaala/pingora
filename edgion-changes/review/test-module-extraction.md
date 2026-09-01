# Private protocol test module extraction

Status: resolved fork maintainability issue (2026-08-29).

Baseline: Pingora fork `bd89d47`; Edgion sibling checkout `83408c1`. This was
an internal test-layout refactor and did not change a public or consumer-visible
contract, so no Edgion source change was required.

## Decision

Large regression matrices and network/state-machine harnesses that require
private implementation access belong in behavior-grouped sibling test files.
The production parent includes each file through `#[cfg(test)]` and `#[path]`.
This preserves Rust parent-module privacy without adding test-only production
interfaces or new Cargo integration targets. Small tests that exercise one
private implementation detail remain inline.

The extraction covered twelve modules:

- H1 server stream, proxy-task, pipelining, and early-body-buffer suites;
- H2 client, server, and END_STREAM watcher suites;
- proxy common decisions, terminal dispatch, H1, custom, and shared response
  pipeline suites.

The sibling files total 11,620 lines including license headers and module
scaffolding. File count and line count are descriptive maintenance evidence,
not a requirement to split future tests mechanically.

## Preserved boundaries

- No production item visibility changed.
- No Cargo test target, process startup, or fixed-port harness was added.
- Test module names and fully qualified test identities stayed unchanged.
- Feature conditions such as `patched_http1`, ignored attributes, and ignore
  reasons stayed attached to the same tests.
- The response-pipeline counting allocator remains unique at the unit-test
  crate level.

## Evidence

Sorted `cargo test -- --list` output was captured before migration and compared
with the final tree:

- core: 756 lines on both sides, SHA1
  `9ed27e5e7edc992917dd7b9ad773efe8e9f66882`;
- proxy: 126 lines on both sides, SHA1
  `f3388b383452d8c0042298b7f39d0fa3256ee19d`.

The normal unit suites passed with core 737 passed / 17 ignored and proxy 123
passed / 1 ignored. Formatting and diff-whitespace checks passed. An independent
review first identified three remaining H1 modules of the same size class;
those were extracted before the reviewer returned LGTM.

## Revisit trigger

Revisit only if a future extraction changes test identity, feature gating,
privacy, target/process topology, allocator ownership, or harness port behavior.
Do not split small inline tests solely to reduce production-file line counts.
