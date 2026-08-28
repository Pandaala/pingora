# Response-body streaming filters

## API contract

`ProxyHttp::upstream_response_body_filter` is asynchronous. It receives the
current body slot, the terminal flag, a `ResponseBodySink`, and request context.
It may mutate or suppress the current chunk, emit extra chunks, request a
delay, or terminate the response.

`ResponseBodySink` has independent per-pump-batch byte and nonempty-chunk
budgets for additional chunks: 1 MiB and 2048 chunks. Empty output is free;
accepted output consumes both applicable budgets and an overflow fails the
response without partial mutation. Replacing the current chunk with a larger
chunk is outside these budgets and must be bounded by the filter. With the
current synchronous drain topology, a batch contains at most the initial task
plus the bounded channel's already queued tasks. `reset_batch` restores both
budgets but intentionally leaves termination sticky until the terminal
boundary consumes it.

## Ordering and terminal dispatch

- Mutated current bytes precede chunks pushed by the filter.
- A terminal Header, terminal Body, Trailer or bare Done claims one terminal
  callback for the response.
- Pump-batch completion is derived only from tasks actually processed. Tasks
  queued behind an early `ResponseBodySink::terminate()` are discarded rather
  than treated as completion evidence; a discarded failure still aborts cache
  state and prevents H1 upstream reuse.
- Trailer followed by Done still dispatches only once.
- Failed responses claim without dispatching; truncation is never presented as
  a clean terminal body.
- Upgrade responses retain `UpgradedBody` tagging for synthetic output.
- Filters that change body length must report `changes_body_length()`. This is
  required to prevent stale H1 Content-Length framing from clipping extra
  output and to keep H2 framing valid.

## Cache interaction

The original upstream representation and filter-emitted chunks are admitted
before downstream-only transformation. Synthetic terminal entities carry an
internal extension/marker so cache miss and hit paths reproduce the same body
without leaking the marker to clients.

Termination while a streaming cache readback is active fails closed. Allowing
it would race cache admission/readback and could expose bytes beyond the
application's terminal point or commit a truncated entity.

Body-forbidden responses (HEAD, informational, 204 and 304) discard synthetic
body output and reconcile Content-Length/Transfer-Encoding. Range processing
does not reinterpret a synthetic terminal entity using the origin's stale
length.

## Implementation concentration

- `pingora-proxy/src/response_body_sink.rs`: bounded output state.
- `pingora-proxy/src/proxy_common.rs`: terminal latch and shared outcomes.
- `pingora-proxy/src/proxy_cache.rs`: EOS migration and synthetic cache marker.
- `proxy_h1.rs`, `proxy_h2.rs`, `proxy_custom.rs`: protocol pumps.

## Tests

- `test_upstream_response_body_sink.rs`: live/cache ordering, framing, range,
  termination and custom connector behavior.
- `test_terminal_body_dispatch.rs`: self-contained end-shape and trailer
  handling against per-test origins.
- Library tests pin the sink budget, EOS migration and terminal latch without
  external processes.
