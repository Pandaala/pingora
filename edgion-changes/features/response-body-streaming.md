# Response-body streaming filters

For the shared task pipeline, cache ordering, Edgion request-local processor
driver, execution leases, and logging boundary surrounding this API, see the
canonical [body relay architecture](../architecture/body-relay.md#response-lane).

## API contract

`ProxyHttp::upstream_response_body_filter` is asynchronous. Its additive typed
variant, `upstream_response_body_filter_event`, distinguishes ordinary data,
the terminal boundary before real trailers, and a trailer-free terminal. The
legacy hook remains the default for data and both terminal variants. A
pre-trailer terminal delegates with `end_of_stream = true` to preserve the
fork's established exactly-once legacy contract; trailer-aware implementations
override the typed hook to retain the distinction.
Both hooks receive the current body slot, a `ResponseBodySink`, and request
context. They may mutate or suppress the current chunk, emit extra chunks,
request a delay, or terminate the response.

`ResponseBodySink` has independent per-pump-batch byte and nonempty-chunk
budgets for additional chunks: 1 MiB and 2048 chunks. Empty output is free;
accepted output consumes both applicable budgets and an overflow fails the
response without partial mutation. Replacing the current chunk with a larger
chunk is outside these budgets and must be bounded by the filter. With the
current synchronous drain topology, a batch contains at most the initial task
plus the bounded channel's already queued tasks. `reset_batch` restores both
budgets but intentionally leaves termination sticky until the terminal
boundary consumes it.

The default legacy hook returns the same object-safe boxed-future type emitted
by `async_trait`, but its concrete ready future is zero-sized and therefore
`Box::pin` does not request allocator storage. The typed default returns that
legacy future directly instead of wrapping it in another async block. Existing
`#[async_trait] async fn` overrides remain source-compatible, boxed, and
awaited; the proxy never skips an override through an inferred capability
flag.

## Ordering and terminal dispatch

- Mutated current bytes precede chunks pushed by the filter.
- A terminal Header, terminal Body, Trailer or bare Done claims one terminal
  event for the response. A real trailer dispatches `TerminalBeforeTrailers`
  before the awaited trailer hook; the following Done is inert.
- Pump-batch completion is derived only from tasks actually processed. Tasks
  queued behind an early `ResponseBodySink::terminate()` are discarded rather
  than treated as completion evidence; a discarded failure still aborts cache
  state and prevents H1 upstream reuse.
- Trailer followed by Done still dispatches only once.
- Upstream compression finalizes before Trailer. H1, H2, and custom pumps feed
  the returned non-terminal footer body through the ordinary filter/cache path
  before processing the trailer; a following Done cannot emit more body bytes.
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
- `pingora-proxy/src/response_terminal.rs`: terminal latch and empty-trailer
  normalization.
- `pingora-proxy/src/response_reconciliation.rs`: downstream body-forbidden,
  terminal task/framing, and response-source cache failure reconciliation.
- `pingora-proxy/src/pump_termination.rs`: shared pump outcomes and termination
  diagnostics and cleanup.
- `pingora-proxy/src/proxy_cache.rs`: EOS migration and synthetic cache marker.
- `proxy_h1.rs`, `proxy_h2.rs`, `proxy_custom.rs`: protocol pumps.
- `proxy_trait.rs`: allocation-free object-safe defaults and compatible async
  override surface.

## Tests

- `test_upstream_response_body_sink.rs`: live/cache ordering, framing, range,
  termination and custom connector behavior.
- `test_terminal_body_dispatch.rs`: self-contained end-shape and trailer
  handling against per-test origins.
- `response_reconciliation_tests.rs`: focused terminal followup suppression and
  framing reconciliation.
- Library tests pin the sink budget, EOS migration, terminal latch, and
  reconciliation without external processes.
