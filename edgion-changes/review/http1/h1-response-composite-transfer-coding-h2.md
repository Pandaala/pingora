# Composite H1 response transfer coding is an upstream limitation

Status: accepted upstream limitation (2026-09-01)

Origin: upstream Pingora commit `8797329225018c4d0ab990166dd020338ae292dc`
(`Release Pingora version 0.1.0`, 2024-02-27), which contains both the H1
chunk-decoder selection and the H2 hop-by-hop header removal.

Reviewed baselines:

- Pingora fork: `ad15e4851dd10d8bd3ec233bfb3e48865a85097a`;
- local `upstream/main`: `09696b51bc59315353d96686355861604d0bb48c`;
- Edgion checkout: `5b84915bf4ad19a1c7454cc9e87799cc6cedf92e`;
- Edgion lockfile Pingora revision: `2e2e67416e4819d6b260ae1298324da221350541`.

## Conclusion

When an H1 origin sends a final-`chunked` composite transfer coding such as
`Transfer-Encoding: gzip, chunked`, Pingora removes the chunk framing but does
not decode the preceding coding. An H2 downstream cannot receive
`Transfer-Encoding`, so Pingora removes the field before the H2 wire handoff
while forwarding the still-coded bytes as DATA. The resulting response body no
longer has metadata that describes its transfer coding.

The complete behavior predates the Edgion fork and remains present in the
official upstream baseline. The fork's shared response-task pipeline preserves
the behavior of upstream's earlier protocol-specific H2 pipeline; it did not
introduce the mismatch. The generic cache can retain and later normalize the
same mismatched representation, but Edgion does not enable Pingora HTTP
response caching. No fork-owned or Edgion-owned amplification was established.

Edgion can reach the direct relay topology because its downstream listeners
support H2 and h2c while ordinary HTTP upstream peers can use H1. It does not
advertise a non-`chunked` transfer coding to origins. Practical exposure
therefore requires a buggy, noncompliant, or malicious origin to send a coding
the client did not request. Live H1 downstream relay retains the composite
field and reapplies chunk framing, so this particular metadata loss is specific
to an H2 downstream or a cache representation that discards the original
coding.

## Why there is no local code fix

The fork will not carry a transfer-coding decoder. Correct decoding would need
to support every accepted coding and extension, preserve streaming and retry
semantics, reconcile filters and content lengths, and define cache admission
and replay behavior. That would be an invasive local replacement for
upstream-owned protocol behavior.

A narrower cross-protocol rejection is possible, but it is not currently a
necessary fork safety guard: the root behavior and the H2/cache relay behavior
are both upstream-owned, Edgion does not enable the cache amplification, and
normal origins do not emit the unsupported coding without negotiation. The
accepted policy is to retain the upstream implementation and adopt a complete
upstream correction normally rather than add a speculative local production
fork.

Do not treat the current behavior as support for composite response transfer
codings. In particular, do not use successful H1 dechunking as evidence that
preceding codings were decoded, and do not admit such a representation to a
cross-protocol cache on the assumption that replacing the field with bare
`chunked` preserves its semantics.

## Revisit triggers

Re-evaluate this decision when any of the following changes:

1. upstream rejects unsupported response transfer codings before proxy/cache
   admission or decodes and accurately normalizes every removed coding;
2. Edgion enables Pingora HTTP response caching;
3. Edgion begins advertising or otherwise negotiating non-`chunked` transfer
   codings with ordinary HTTP origins;
4. the fork changes H1 response parsing, downstream header sanitization, cache
   metadata normalization, or the H1-to-H2 response pipeline;
5. reproducible evidence shows material exposure through compliant origins or
   a fork-owned consumer.

At adoption time, cover a real gzip member inside valid chunk framing, H2
downstream rejection or correct decoding, upstream connection non-reuse after
rejection, a sole-`chunked` positive control, and cache admission/replay where
cache support is enabled.

## References

- `pingora-core/src/protocols/http/v1/common.rs`: final transfer-coding token
  selection;
- `pingora-core/src/protocols/http/v1/client.rs`: H1 response body-reader
  selection and the multiple-transfer-encoding characterization test;
- `pingora-core/src/protocols/http/v2/server.rs`: H2 hop-by-hop header removal;
- `pingora-proxy/src/response_pipeline.rs`: current protocol-specific response
  header normalization;
- `pingora-proxy/src/proxy_cache.rs`: cache-hit response header normalization;
- originating finding:
  `tasks/review-pingora-client-security-20260901/001-h1-composite-transfer-coding-h2.md`.
