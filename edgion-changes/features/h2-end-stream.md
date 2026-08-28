# HTTP/2 end-stream evidence

## Problem

An upstream may finish a response and then send `RST_STREAM(NO_ERROR)` because
it no longer wants the remaining request upload. Some `h2` versions overwrite
or otherwise hide the receive-side END_STREAM state after the reset. Treating
that as an incomplete response discards a response the proxy already received;
treating every reset as clean risks accepting truncation.

## Evidence model

`EndStreamWatchStream` observes frame headers before `h2` consumes them.
`EndStreamWatch` records, per stream, whether END_STREAM was observed and how
many DATA payload bytes preceded it. Registration is synchronized with frame
scanning so a fast response cannot publish and evict evidence before the
session knows its stream id.

The client session combines four independent proofs:

1. the ordinary `h2` end-stream state;
2. a latched EOF already returned to the caller;
3. a satisfied declared Content-Length;
4. wire evidence whose DATA byte count matches bytes actually delivered.

A reset before END_STREAM, an underflow, rejected trailers, flow-control loss,
GOAWAY exclusion or a local reset invalidates the corresponding proof. A
terminal HEADERS frame after DATA is only evidence that trailers must now be
validated; it is not published as clean EOF by the wire watcher. Observable
trailer rejection or an ambiguous terminal result fails closed where the
public `h2` API permits it. The upstream decoder can still discard forbidden
pseudo-fields or omit oversized-trailer rejection before exposing a
valid-looking map, so the fork cannot claim to detect every invalid trailer.
The wire flag alone never decides response success or cache admission; see
[`../review/upstream-limitations.md`](../review/upstream-limitations.md).

## GOAWAY eligibility

A GOAWAY's `last_stream_id` is retained as a connection ceiling, not applied
once. Streams above it can never publish wire evidence, whether they were
registered before the frame or after it, because `h2` errors such a stream out
as soon as it processes the GOAWAY and never delivers its body.

The ceiling is only taken from a frame the peer is allowed to send. A GOAWAY on
a nonzero stream id, one whose declared payload is shorter than the fixed eight
octets, one truncated by EOF, or a later GOAWAY that raises `last_stream_id`
are all connection errors after which `h2` delivers nothing further. None of
them names a threshold worth trusting, so each poisons the observer for the
rest of the connection instead of contributing a guessed ceiling. Poisoning
only withdraws wire evidence: affected responses fall back to the other three
proofs, and the byte stream handed to `h2` is unchanged either way.

## Local reset ordering

Giving up wire evidence for a stream this side resets is an irreversible mark
on the shared record, and it is made BEFORE the local RST_STREAM is queued.
Dropping the pending-map entry is not sufficient on its own: the session and
the request-body pump already hold `Arc` clones of that record, so a
publication winning the race would set END_STREAM on handles no later removal
can reach. Publication reads the mark under the same lock, which makes the two
orderings decidable rather than racy.

Evidence published strictly before the mark is kept. The peer flagged the end
of its body before this side decided to walk away, so the body was whole at
that point; retracting it would fail exchanges whose response was already in
hand.

## Request-body writes

The weak wire flag has one consumer outside response completion: the proxy's
h2 request-body pump, in `pingora-proxy/src/proxy_h2.rs`. It asks a different
question -- "did the origin say it was done with me" -- and it decides nothing
about the response, which is settled by the read half exactly as above.

A `poll_capacity` wait has no end of its own. `h2` reports that a stream closed
and that a stream was reset, but nothing distinguishes a peer that is about to
grant window from one that never will, so an origin that answers in full and
then stops granting request-body window leaves the write blocked forever. That
is not merely a leaked task: the pump awaits the write inline in its duplex
loop, so while it is blocked the loop is not delivering the upstream response
either, and a client can be left unanswered while a complete response sits in
the proxy's buffer.

Two failure shapes are therefore treated as "the origin has stopped receiving",
and both require the wire flag as well:

- the write failed because the stream is GONE (h2 will take no further byte),
  which is the RFC 9113 section 8.1 reset;
- the write made no progress for a whole `write_timeout` window. That deadline
  bounds ONE capacity grant and is re-armed for each, so it really does measure
  a lack of progress; a peer that keeps granting window, however slowly, never
  produces it.
- or, when no such deadline is configured, the write did not finish its chunk
  within a whole stall-probe interval. That is a coarser test: the probe is
  created once per chunk and cannot see progress made inside one, so its
  interval is set correspondingly high and it never preempts a configured
  `write_timeout`.

In both cases the upload is abandoned and the response delivered. Nothing is
concealed by that: the request half never receives its END_STREAM, so the
stream is reset when the exchange ends and the origin sees a truncated upload
rather than a whole one, and the swallow is logged at `warn`. Without the wire
flag every one of these failures still costs the exchange, so a response the
origin never flagged complete can neither be delivered nor admitted to cache on
the strength of a stalled write.

## Version-tolerant tests

Current `h2` releases may preserve END_STREAM after a later reset, while older
ones exposed the overwritten state that motivated the watch. Behavioral tests
therefore assert clean EOF or truncation, not a particular private receive
state. The frame-scanner unit tests independently prove record/reset ordering.
The workspace currently requires `h2 >= 0.4.16`; dependency upgrades must run
these contract tests because the implementation observes wire frames around a
library whose private reset behavior may change.

## Implementation concentration

- `pingora-core/src/protocols/http/v2/end_stream_watch.rs`.
- `pingora-core/src/connectors/http/v2.rs` wires the watch around production IO.
- `pingora-core/src/protocols/http/v2/client.rs` registers stream ids and
  classifies response completion.
- `pingora-core/src/protocols/http/v2/server.rs` tracks strict downstream body
  completion and timeout behavior.
- `pingora-proxy/src/proxy_h2.rs` classifies request-body write failures and
  bounds a write the upstream has stopped taking.
