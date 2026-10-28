# Known upstream limitations

## `h2` terminal trailer validation

The `h2` receive path can discard forbidden trailer pseudo-fields while
exposing remaining ordinary fields as a valid-looking trailer map. At the same
handoff it does not provide the oversized-trailer rejection Pingora needs. A
following `RST_STREAM(NO_ERROR)` can hide some failures from Pingora's public
completion view.

This is a pre-existing, low-frequency upstream decoder/interface limitation,
not a fork regression. It requires a malformed response from a buggy or
malicious origin.

### Maintenance policy

- Do not vendor or locally fork `h2` for this limitation.
- Keep the normal dependency range and adopt a fix normally.
- Never interpret raw terminal HEADERS or wire END_STREAM as proof that its
  HPACK block was valid, and never use wire END_STREAM alone for cache admission.
- Retain the watcher and fail-closed trailer handling until an upstream fix is
  adopted and the full contracts pass.
- Keep fixing fork-owned parsing, record lifetime, GOAWAY/reset ordering, byte
  accounting, API latches, tests, and documentation.

The wire observer answers whether the peer set END_STREAM; only the decoder can
validate trailer fields. Those are deliberately different responsibilities.

### Revisit trigger

Re-evaluate when a normal `h2` release rejects all trailer pseudo-fields and
oversized trailer blocks before publishing trailers. Then:

1. test minimum-supported and current dependency behavior;
2. enable pseudo-only, mixed, oversized, fragmented CONTINUATION, valid-empty,
   and same-burst reset contracts across body, trailers, and cache admission;
3. audit watcher assumptions around ordering, read errors, queues, GOAWAY,
   reset state, and buffer lifetime;
4. complete [terminal HEADERS work](../pending-issues/h2-terminal-headers-completion.md)
   and update [trailer validation](../pending-issues/h2-trailer-validation.md);
5. simplify only where executable evidence proves a watcher path redundant.

An upstream release preserving received END_STREAM across reset is not enough
on its own: byte delivery, terminal HEADERS validation, local reset ordering,
and GOAWAY/connection state remain separate contracts.
