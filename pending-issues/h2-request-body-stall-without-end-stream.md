# Upstream h2 request-body stall with no END_STREAM evidence

Status: open; out of scope for H2-007, recorded during its review on 2026-08-27.

## Problem

H2-007 bounded the case where an upstream flags END_STREAM on its response and
then withholds request-body flow-control window. The bound comes from the
conjunction "wire END_STREAM observed" AND "the write stopped making progress":
the wire flag is what makes abandoning the upload safe, because it says the
origin already produced its complete answer.

The mirror case is still unbounded. An upstream that withholds request-body
window and NEVER flags END_STREAM, on a peer with no `peer.options.write_timeout`
configured, leaves `write_body` blocked forever exactly as before. The stall
probe fires, finds `upstream_response_ended.observed() == false`, and correctly
goes back to waiting -- there is no evidence that would justify abandoning the
upload, and abandoning it would truncate a request whose response has not even
arrived.

The exchange must fail in that case, but today it fails only when the client
gives up, holding a downstream connection, an upstream stream and its capacity
reservation until then.

## Why it was not fixed with H2-007

H2-007's premise is "origin sends complete DATA EOS", and its acceptance test
is written for that shape. The no-evidence case needs a different answer: there
is nothing to swallow and nothing to deliver, so the resolution is a failure
with a bound, not a success. Deciding what that bound should be -- a default
`write_timeout`, a whole-exchange deadline, or something the read half already
owns -- is a policy question wider than one review issue.

## Impact today

Not reachable on a deployment that configures `peer.options.write_timeout`,
which then fails the exchange on its own schedule. Edgion sets it from the
folded backend/route/gRPC deadlines (default 60s) in
`configure_peer_timeouts`, and only leaves it unset when every deadline is
explicitly disabled and no request body is being replayed.

## Reference

- `pingora-proxy/src/proxy_h2.rs` -- `write_upstream_body_watching_stall`,
  `UPSTREAM_STALL_PROBE_INTERVAL`.
- `docs/edgion-fork/h2-end-stream.md` -- "Request-body writes".
