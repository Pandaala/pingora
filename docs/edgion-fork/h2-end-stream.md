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
validated; it is not published as clean EOF by the wire watcher. Invalid
trailers latch a body error, so a later reset cannot launder them into success.
The wire flag alone never decides response success; it is consulted only in
the strict reset/error path.

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
