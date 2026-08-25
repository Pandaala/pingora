# Edgion fork changes

This directory is the maintenance index for behavior carried by the Edgion
fork. It describes the contracts that must survive an upstream rebase, where
the implementation lives, and which tests pin each contract.

## Baseline

- Source branch: `edgion` at `57f6183c38b5efbf9182f6a7a51bc7597cea265e`.
- Edgion feature base: Pingora `0.8.1` at `719ef6cd54e40b530127751bab6c1afc5ae815a8`.
- Rework base: `main` at `e6e677fe9b58555140ab7bd14feff035392b3530`.
- The original `edgion` ref and worktree are not modified by this rework.

The feature delta is the net change from `719ef6c` through `57f6183`. The
earlier release, audit and CI commits on `edgion` are deliberately excluded;
current `main` remains authoritative for versions and upstream maintenance.

## Feature map

| Area | Contract | Main references |
| --- | --- | --- |
| Early request-body buffering | Capture once, rewind and replay safely across attempts | [request-body-buffering.md](request-body-buffering.md) |
| Request-body transport | One event model across H1/H2 and explicit upstream framing | [request-body-transport.md](request-body-transport.md) |
| Response-body streaming filters | Async filtering, bounded emitted chunks, terminate and terminal dispatch | [response-body-streaming.md](response-body-streaming.md) |
| Inbound PROXY protocol | Parse v1/v2 before TLS with an explicit trust boundary | [proxy-protocol.md](proxy-protocol.md) |
| H2 end-stream evidence | Preserve wire END_STREAM evidence across later resets | [h2-end-stream.md](h2-end-stream.md) |
| Verification | Test ownership, commands and external dependencies | [test-matrix.md](test-matrix.md) |
| Upstream maintenance | Rebase order and conflict hot spots | [upstream-sync.md](upstream-sync.md) |

## Cross-feature invariants

1. A downstream request-body terminal event is delivered at most once and is
   classified as `Complete` or `Abandoned`.
2. A registered request-body buffer never replays partial or cancelled
   capture as if it were complete.
3. A response-body terminal callback is delivered exactly once for a normal
   response, including Header-EOS, trailer and bare-Done endings; it is not
   synthesized for an aborted response.
4. Application termination is a typed outcome, not a retryable generic error.
5. Bytes emitted by a response filter are bounded per batch and keep the same
   order on live, cache-admission and cache-hit paths.
6. H1 connection reuse is rejected whenever unread or rewritten request state
   makes the next request ambiguous. H2 termination is scoped to the stream.
7. PROXY data is accepted only according to listener policy and is parsed
   before TLS consumes the ClientHello.

## Scope rule

Keep new tests in standalone integration targets when they exercise the proxy
contract. Small state-machine and parser tests may stay beside private code
when external tests cannot reach the state without exposing internals.
