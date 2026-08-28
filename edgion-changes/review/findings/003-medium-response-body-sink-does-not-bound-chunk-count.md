# ResponseBodySink bounds payload bytes but not emitted chunk count

Status: open

Severity: medium

Fork baseline: `upstream/main...edgion_v3`; introduced with
`ResponseBodySink` in `600ac49`.

## Problem

`ResponseBodySink` is described as bounded, but its budget accounts only for
payload bytes:

- `pingora-proxy/src/response_body_sink.rs:37-45`
- `pingora-proxy/src/response_body_sink.rs:77-93`

The `extra: Vec<Bytes>` has no item-count limit. One callback can legally push
1,048,576 one-byte chunks while staying within the 1 MiB byte budget.

The fan-out continues after the sink:

- `pingora-proxy/src/proxy_cache.rs:1014-1038` writes each emitted chunk to the
  cache separately.
- `pingora-proxy/src/proxy_cache.rs:1452-1485` creates one `HttpTask` per
  emitted chunk for downstream delivery.

The per-batch input bound does not solve this: each input body task can create
a very large number of output tasks before the batch is drained.

## Impact

- One MiB of payload can require tens of MiB of `Bytes`, `HttpTask`, and Vec
  metadata.
- Downstream scheduling, framing, and cache admission become O(chunk count)
  rather than O(payload size).
- Cache admission performs up to roughly one million individual async writes
  for a single MiB.
- A response-driven splitting filter can turn an otherwise small upstream
  response into a memory/CPU/storage-call amplification vector.

The application owns the filter, but remote requests or origin data commonly
control the input that decides how it splits output.

## Decision and implementation

Keep byte volume and per-item work as independent budgets. The sink accepts at
most 1 MiB and 2048 nonempty `push()` calls per pump batch. The item limit is
equivalent to a 512-byte amortization target at the full byte budget, but it is
enforced independently: large chunks do not consume multiple item slots and
small chunks cannot turn unused byte budget into unbounded tasks.

Empty chunks are still dropped for free. `prepend_current()` retains the same
unbudgeted semantics as ordinary current-body mutation, so item accounting is
stored separately from `extra.len()`. `push()` checks both budgets before
mutating either counter or `extra`; `reset_batch()` restores both budgets while
leaving termination sticky.

The hard bound is sufficient for this finding. Coalescing remains an optional
future optimization and is not relied on for safety.

## Closure tests

- Sink unit tests accept exactly 2048 one-byte chunks, reject the next without
  partial state mutation, keep empty/current chunks free, and reset byte/item
  budgets without clearing terminate.
- Shared drain regression compares one 1 MiB emitted chunk with 2048 512-byte
  chunks carrying the same payload size and pins the maximum downstream task
  count plus EOS migration.
- `test_upstream_response_body_sink` sends the exact boundary through H1,
  custom, and cache miss/hit paths and rejects the next item on H1/custom.
- `test_terminal_body_dispatch` sends the exact boundary and overflow through
  a real H2 upstream pump.

Final command results and closure status are recorded only after the project
verification gate completes.
