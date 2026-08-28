# TLS-feature bind test rejects an error shape produced by its own connector

Status: open

Severity: low

Fork baseline: `edgion_v3` at `65f217c`.

## Problem

`cargo test -p pingora-core --lib --features boringssl` consistently fails on
macOS in `connectors::tests::test_connector_bind_to`. The attempted connection
can fail as `BindError`, which `connectors::l4::connect` deliberately wraps as
`InternalError`, but the test accepts only `ConnectError`, `ConnectTimedout`,
or `ConnectNoRoute`.

The test therefore disagrees with the connector's current error mapping and
makes broad TLS-feature test commands unreliable as verification gates.

## Evidence

The isolated command below reproduces the failure:

```text
cargo test -p pingora-core --lib --features boringssl \
  connectors::tests::test_connector_bind_to
```

Observed result: `unexpected error type: InternalError`.

## Recommended investigation

Determine whether a local bind failure should remain an `InternalError` or be
reported as a connection-class error. Then align the production mapping and
the cross-platform test without merely accepting every error type.
