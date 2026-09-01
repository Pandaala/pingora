# TLS-feature bind test rejects an error shape produced by its own connector

Status: resolved

Severity: low

Classification: inherited upstream test defect; production error mapping accepted.

Origin baseline: `edgion_v3` at `65f217c`.

Closure baseline: uncommitted working tree based on `bd89d47461e2b5df399c853cd6e28f07e4ce5359`.

## Problem

`cargo test -p pingora-core --lib --features boringssl` consistently fails on
macOS in `connectors::tests::test_connector_bind_to`. The attempted connection
can fail as `BindError`, which `connectors::l4::connect` deliberately wraps as
`InternalError`, but the test accepts only `ConnectError`, `ConnectTimedout`,
or `ConnectNoRoute`.

The test therefore disagreed with the connector's current error mapping and
made broad TLS-feature test commands unreliable as verification gates.

## Evidence

The isolated command below reproduces the failure:

```text
cargo test -p pingora-core --lib --features boringssl \
  connectors::tests::test_connector_bind_to
```

Observed result: `unexpected error type: InternalError`.

The production path is internally consistent:

- `protocols/l4/ext.rs` classifies failure to bind a requested source socket as
  `BindError` and preserves the operating-system error;
- `connectors/l4.rs` wraps local `SocketError` and `BindError` failures in a
  top-level `InternalError` with `Fail to connect to {peer}` context;
- remote connection failures retain connection-class errors.

This keeps a caller-supplied local bind/configuration failure distinct from a
remote reachability failure. The mapping is inherited from upstream and no
fork-owned production contract justified changing it.

## Resolution

Both inherited bind tests now occupy a localhost source port and ask the direct
L4 connector and the higher-level `TransportConnector` to bind that exact port
before connecting to a second localhost listener. They assert the complete
error chain rather than accepting a wider set:

- top level `InternalError`;
- root type `BindError`;
- preserved `std::io::ErrorKind::AddrInUse`;
- both connector and source-bind context strings.

This removes the former dependencies on routing `240.0.0.1:80` from both
connector layers. While running the Linux broad gate, the adjacent inherited
`test_do_connect_without_total_timeout` also proved nondeterministic because
that container routed `192.0.2.1:79` successfully. It now connects to a local
plaintext listener with non-empty SNI, asserts `TLSHandshakeFailure`, and
verifies that no total-connection-timeout context was added. The timeout tests
that intentionally exercise network timeout classification still use the
TEST-NET address.

No production code or public connector contract changed, so no Edgion consumer
change is required.

## Closure evidence

Validated on 2026-08-29:

- macOS isolated bind and no-total-timeout tests: both passed; the bind test
  also passed five consecutive runs before broad validation;
- macOS `cargo test -p pingora-core --lib --features boringssl`: 769 passed,
  17 ignored;
- Linux arm64 (`rust:1.96.1-bookworm`) isolated bind test: passed;
- Linux arm64 full boringssl suite after all three deterministic corrections: 781
  passed, 18 ignored.

The Linux source checkout was mounted read-only and the target directory lived
inside the disposable container. The first pre-correction broad run produced
780 passed, one failed, and 18 ignored solely because
`test_do_connect_without_total_timeout` unexpectedly established the reserved
address connection; this observation is the evidence for the timeout-context
test correction, not a production connector failure.

The complete core suite still contains unrelated inherited integration-style
tests that connect to `1.1.1.1`. One macOS re-run was interrupted after its
external TLS case made no progress for more than three minutes; that case then
passed alone in 0.5 seconds and the following complete run passed. Finding 008
removes external routing from the three tests it closes, but does not claim the
entire inherited core suite is hermetic or independent of outbound networking.

## Reopen trigger

Reopen if the connector's public error taxonomy changes, a supported platform
does not report an occupied exact localhost source port as `AddrInUse`, or the
local TLS failure ceases to preserve the no-total-timeout distinction.
