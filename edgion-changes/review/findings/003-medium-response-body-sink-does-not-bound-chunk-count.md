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

## Recommended fix

Add a separate maximum emitted-chunk count per batch, or charge each chunk's
fixed metadata/task overhead against a combined budget. Reject the push before
mutating the sink when either limit would be exceeded.

Consider coalescing adjacent small chunks before building downstream/cache
tasks, while preserving ordering and EOS migration. Reserve Vec capacity only
from the bounded count.

## Required tests and benchmark

- Push one-byte chunks exactly to the item limit, then assert the next push
  returns a clear error and is not partially accepted.
- Verify that resetting a batch restores both byte and item budgets.
- Exercise EOS migration at the item boundary.
- Compare allocations and elapsed time for one 1 MiB chunk versus heavily
  fragmented output of the same byte size, including cache admission.
