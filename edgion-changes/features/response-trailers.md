# Response trailer lifecycle

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
  -> awaited application trailer filter for a real trailer map
  -> cache and downstream trailer modules
  -> downstream filtering for bytes released at the boundary
  -> body bytes, then trailers on the wire
  -> Done without a second terminal event
```

Header EOS and a bare Done use `TerminalWithoutTrailers`. A terminal Body uses
`Data { end_of_stream: true }`. A real trailer map uses
`TerminalBeforeTrailers`; an absent or application-emptied map is normalized to
trailer-free completion. `Failed` claims the latch without dispatching a clean
terminal event. Hook errors stop the trailer before cache, downstream modules,
or the wire.

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

## Edgion consumer handoff

The inspected Edgion checkout is commit
`83408c11dedb81eab8504d85edfb0fcc061c9e7f`. Its committed lockfile still
selects Pingora `57f6183c...`; the ws4 ExtProc worktree contains uncommitted
consumers of all typed hooks and `response_trailers_supported`. After this
Pingora work is committed, Edgion must pin the complete Pingora dependency
family to one immutable merged revision, remove temporary path patches,
regenerate `Cargo.lock`, and run its source-policy guard plus the ExtProc unit
and official-Go integration suites.

An isolated copy of that uncommitted Edgion worktree was compiled against this
working tree with `cargo check -p edgion-gateway --lib`; the ExtProc trailer
consumer contract compiled successfully. Reaching that check also exposed
three independent baseline-migration items that must be carried by Edgion, not
this Pingora feature:

- this branch publishes the Pingora family as `0.8.0`, while the old Edgion
  lock selects `0.8.1`; Edgion must update the whole family atomically;
- this branch's BoringSSL adapter uses `boring`/`boring-sys` 5, while Edgion
  pins 4. The isolated check used 5 and omitted Edgion's obsolete
  `fips-link-precompiled` feature reference only for compatibility validation;
  the real FIPS wiring needs an Edgion-owned migration decision;
- `edgion-gateway/src/ai/anthropic/response.rs` directly assigns
  `ResponseHeader.status`; the isolated check changed that one line to
  `ResponseHeader::set_status`. This is unrelated to ExtProc trailers.

The Rustls-only check additionally reaches an existing Edgion conditional-
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
