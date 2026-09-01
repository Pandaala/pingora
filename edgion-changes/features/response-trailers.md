# Response trailer lifecycle

For the relationship between Pingora's typed terminal/trailer sequence and
Edgion's trailer execution lease, see the canonical
[body relay architecture](../architecture/body-relay.md#7-trailer-execution-lease).

## Public API

`pingora-proxy` exports `UpstreamResponseBodyEvent` from the crate root and
prelude:

```rust
pub enum UpstreamResponseBodyEvent {
    Data { end_of_stream: bool },
    TerminalBeforeTrailers,
    TerminalWithoutTrailers,
}
```

`ProxyHttp` provides additive typed header and body hooks. Their defaults
delegate to the existing header/body hooks for ordinary data and both terminal
variants. `TerminalBeforeTrailers` delegates to the legacy body hook with EOS
to retain the fork's existing exactly-once compatibility; trailer-aware
implementations override the typed hook to distinguish it. The existing
`upstream_response_trailer_filter` hook is asynchronous and every protocol pump
awaits it.

The generic HTTP session exposes `prepare_response_header` and
`response_trailers_supported`. Native H2, subrequest, and custom transports
report support. H1 reports planned capability before final header commit and
the selected writer capability after commit.

## Canonical ordering

All H1, H2, and custom response pumps use this order:

```text
final response header
  -> Data events
  -> compression footer bytes, if trailers terminate an encoded response
  -> one typed terminal boundary
  -> awaited upstream_response_trailer_filter for a real trailer map
  -> cache handling for the validated upstream representation
  -> awaited downstream response_trailer_filter and trailer modules
  -> downstream filtering for bytes released at the boundary
  -> body bytes, then trailers on the wire
  -> Done without a second terminal event
```

Header EOS and a bare Done use `TerminalWithoutTrailers`. A terminal Body uses
`Data { end_of_stream: true }`. A real trailer map uses
`TerminalBeforeTrailers`; an absent or application-emptied map is normalized to
trailer-free completion. `Failed` claims the latch without dispatching a clean
terminal event. An upstream trailer-hook error stops before cache handling; a
downstream trailer-hook error stops downstream trailer processing and wire
delivery on paths that deliver a real trailer task. A streaming-partial-write
cache miss/readback serves the stored upstream body representation instead of
passing that upstream `Trailer` through the downstream pump, so it does not
invoke this downstream-only hook. Non-streaming storage follows the inline
downstream pump, but currently cannot complete admission for a trailered H1
response even when the hook succeeds; that separate limitation is tracked in
`pending-issues/non-streaming-cache-trailer-completion.md` and must not be
attributed to trailer-hook error propagation.

Upstream compression finalizes at the first trailer boundary, not at the
bookkeeping `Done` that follows it. Any encoder footer is exposed as a
non-terminal body task immediately before the unchanged trailer. The
compression context records finalization, so a split-batch `Done` cannot
finalize the encoder again or dispatch a second body-hook EOS.

## HTTP/1 behavior

The H1 chunked reader retains validated response trailers and preserves bytes
over-read into the next keepalive exchange. The reader and writer limit a
trailer section to 64 KiB and 256 field/value entries, preserve duplicate
values in arrival order, and reject malformed, incomplete, oversized, or
forbidden fields. The forbidden set is `content-length`, `transfer-encoding`,
`trailer`, `connection`, `keep-alive`, `proxy-authenticate`,
`proxy-authorization`, `te`, `upgrade`, and `host`.

The writer validates and serializes the complete block before its first byte is
written. The cancel-safe proxy task path retains partial write progress through
the existing persistent writer state. An empty map uses the ordinary
`0\r\n\r\n` terminator.

Trailers are representable only for an HTTP/1.1 downstream with the selected
chunked writer. Content-Length, HEAD, 204, 304, successful upgrade/101, and
HTTP/1.0 responses safely finish without forwarding the trailer block.
HTTP/1.0 responses remove a lone decoded `Transfer-Encoding: chunked`, use
close-delimited framing when a body-bearing final response has no
Content-Length, and reject composite or duplicate transfer codings before
committing the origin response. Ignored informational responses do not mutate
connection state; informational, HEAD, 204, and 304 responses keep their
self-delimited HTTP semantics without unnecessarily disabling keepalive.

## Same-batch planning

An upstream batch can contain the final header, body, and trailer before the H1
writer is selected. Downstream response-header modules therefore run once at
the planning boundary. A counter prevents the blocking and cancel-safe writer
paths from running those modules again. Informational responses do not replace
the final capability; status 101 is final. The latch resets on every keepalive
request, and debug builds assert that planned and actual capabilities agree.

## Source merge and retained fork strengths

The behavior was transplanted from the uncommitted response-trailer patch in
`/Volumes/ExtStore/ws4/pingora-ext-proc-trailers`, based on Pingora commit
`57f6183c38b5efbf9182f6a7a51bc7597cea265e`, into `edgion_v3` at baseline
`a220d92931c78396f8c7f84065a1642de68d684c`.

The merged implementation intentionally differs from that source patch:

- it embeds typed events in the newer response-wide latch, cache ordering, sink
  byte/chunk budgets, application termination, and request-abandonment logic;
- H1 trailer tasks use the newer cancel-safe proxy writer rather than a direct
  one-shot write;
- H1 parsing retains the newer zero-copy overread and pipelining behavior;
- planned header filtering covers both direct and queued downstream task APIs;
- the dedicated trailer suite binds portable loopback origins and runs on the
  current macOS host instead of depending on a `127.0.0.2` alias.
- unlike the source patch's pre-trailer default no-op, the typed hook preserves
  the existing fork contract by delegating that terminal event to legacy body
  filters with `end_of_stream = true`.

## Historical Edgion consumer handoff

The original migration inspection used Edgion
`83408c11dedb81eab8504d85edfb0fcc061c9e7f`, whose committed lockfile selected
Pingora `57f6183c...`, plus an uncommitted ws4 ExtProc worktree. An isolated
copy compiled against the then-current Pingora worktree with
`cargo check -p edgion-gateway --lib`, demonstrating that the typed trailer
consumer contract compiled at that historical boundary.

This is no longer an action list for the current consumer. Edgion later
committed the production relay integration as `f31d0169da97`, and its recorded
2026-08-31 lock selected Pingora `af9e1ac057c6`. Before making a current claim,
capture the live Edgion `HEAD`, tracked worktree state, manifest source, and
lock-resolved Pingora revision as required by the
[verification matrix](../verification/test-matrix.md).

The historical check exposed three independent migration items owned by
Edgion rather than this Pingora feature:

- this branch publishes the Pingora family as `0.8.0`, while the old Edgion
  lock selects `0.8.1`; Edgion must update the whole family atomically;
- this branch's BoringSSL adapter uses `boring`/`boring-sys` 5, while Edgion
  pins 4. The isolated check used 5 and omitted Edgion's obsolete
  `fips-link-precompiled` feature reference only for compatibility validation;
  the real FIPS wiring needs an Edgion-owned migration decision;
- `edgion-gateway/src/ai/anthropic/response.rs` directly assigns
  `ResponseHeader.status`; the isolated check changed that one line to
  `ResponseHeader::set_status`. This is unrelated to ExtProc trailers.

The Rustls-only check additionally reached an existing Edgion conditional-
compilation problem in `header_cert_auth`, whose imports assume the BoringSSL
TLS facade. No files or lock entries in the ws4 Edgion worktree were changed by
this validation.

## Verification ownership

- Core H1 parser, writer, framing, keepalive, and cancel-safety tests live in
  the H1 modules.
- `test_terminal_body_dispatch.rs` covers H1/H2 typed ordering, awaited hooks,
  mutation, planned capability, HTTP/1.0 downgrade, absent/cleared trailers,
  error suppression, cache identity, and abort behavior.
- `test_upstream_response_body_sink.rs` covers the custom pump and the existing
  sink/cache/termination matrix.
- The known upstream H2 decoder limitation remains governed by
  `review/upstream-limitations.md`; this feature does not treat wire
  END_STREAM as sufficient cache-admission evidence.
