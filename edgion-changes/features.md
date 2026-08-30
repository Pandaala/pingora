# Fork feature inventory

This file records what the fork adds to upstream Pingora. Detailed files own
the full contracts and tests; this page is the provenance and dependency map.

## Commit provenance

| Layer | Original `edgion_v2` | Migrated `edgion_v3` | Purpose |
| --- | --- | --- | --- |
| Inbound transport | `1b33442` | `64d2690` | Parse trusted or mandatory PROXY protocol v1/v2 before TLS |
| Request lifecycle | `879bf3e` | `faf89f5` | Capture, finalize, rewind, and replay downstream request bodies |
| H2 response integrity | `b9ba47c` | `682506d` | Preserve qualified END_STREAM evidence across reset/error paths |
| Proxy streaming | `81aa6ac` | `600ac49` | Add request disposition/termination and async response streaming controls |
| Contract tests | `130a54d` | `605d306` | Cover streaming seams and H2 reset behavior |
| Maintenance docs | `2c60acf` | `db3de91` | Record behavior, verification, and upstream synchronization |

Later `edgion_v3` commits refine these features. Use `git log` and the review
records for current history; the migration commit alone is not the current
implementation.

## Feature map

| Feature | Contract | Implementation center | Detail |
| --- | --- | --- | --- |
| Inbound PROXY protocol | Strict v1/v2 parsing before TLS, explicit transport trust, preserved raw peer | core listeners, L4 parser, digest | [proxy-protocol.md](features/proxy-protocol.md) |
| Replayable request body | Capture once, fail closed on partial/cancelled capture, bounded replay chunks | core body buffer and H1/H2 server sessions | [request-body-buffering.md](features/request-body-buffering.md) |
| Request-body transport controls | H1 transfer-coding admission, consistent events, dispositions, termination, trailers, retry gates, cleanup | proxy entry, trait/common and pumps | [request-body-transport.md](features/request-body-transport.md) |
| Response-body streaming controls | Async filter/sink, allocation-free defaults, bounded emitted bytes and chunk count, typed termination, terminal dispatch, cache/live ordering | trait, shared pipeline, sink, pumps, cache | [response-body-streaming.md](features/response-body-streaming.md) |
| Response trailer lifecycle | Typed pre-trailer boundary, awaited application hook, H1 parsing/writing, planned framing capability, HTTP/1.0 downgrade | core H1, proxy trait and pumps | [response-trailers.md](features/response-trailers.md) |
| H2 END_STREAM evidence and upload liveness | Combine decoded state, EOF, content length, and qualified wire evidence; never trust wire flag alone; bound non-progressing request writes and release abandoned reservations | H2 watcher, client/connector, proxy H2 | [h2-end-stream.md](features/h2-end-stream.md) |

## Cross-feature invariants

1. A downstream request-body terminal event is delivered at most once as
   `Complete` or `Abandoned`; H1, H2, and custom upstream pumps all stop an
   unfinished upload with `Abandoned` after the response completes.
2. A fork-owned `RequestBodyBuffer` capture that is partial, cancelled,
   drained, or poisoned is never replayed as a complete request body. This
   guarantee does not cover upstream's alpha subrequest `SavedBody` API: its
   infallible conversion to `InputBody` erases incomplete/truncated state, so
   chained consumers must reject incomplete capture before conversion. See the
   [upstream limitation](review/subrequest/incomplete-saved-body-replay.md).
3. A normal response gets exactly one typed terminal event across Header-EOS,
   Body-EOS, Trailer, and Done; a real trailer receives
   `TerminalBeforeTrailers`, while an abort gets no synthetic clean terminal.
4. Application termination is typed and non-retryable. A committed final
   response cannot enter a second response or retry path.
5. Filtered bytes preserve order across live delivery, cache admission, and
   cache hit; compression finalizes before trailers, and framing metadata
   agrees with transformed body semantics.
6. H1 reuse is rejected when unread/rewritten state makes the next exchange
   ambiguous. H2 stream termination and connection health remain distinct.
7. PROXY source trust is transport trust. Consumers must reject invalid trust
   configuration rather than broaden it.
8. Wire END_STREAM, decoded EOF, content-length satisfaction, abandonment, and
   replay EOF are separate evidence. Cache admission never uses wire evidence
   alone.
9. An H2 upstream request-body capacity wait is always finite. A configured
   write timeout wins unchanged; otherwise the proxy applies its protocol-local
   progress floor. Expiry without qualified response END_STREAM fails and
   resets the stream rather than manufacturing clean completion. Every
   successful upload abandonment cancels its outstanding capacity request
   before response delivery continues.
10. H1 transfer-coding failures are terminal before routing: the generic H1
    layer closes framing-invalid requests, and proxy admission accepts only an
    absent `Transfer-Encoding` or one trimmed, case-insensitive `chunked`
    field among requests that reach `HttpProxy`. Every other core-accepted form
    is submitted to the normal `HTTPStatus(501)` error-rendering path and is
    independently denied downstream reuse before application filters, cache,
    upstream selection, buffering, or retry can observe it.

## Ownership boundary

The parser/trust wiring, request replay, watcher integration, proxy hooks, sink
bounds, terminal/cache behavior, tests, and docs are fork-owned and must be
fixed here when defective. Decoder behavior inside `h2` is upstream-owned; keep
the normal dependency and the boundary in
[`review/upstream-limitations.md`](review/upstream-limitations.md).
