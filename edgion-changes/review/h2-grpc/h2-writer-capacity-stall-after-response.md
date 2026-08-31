---
name: h2-writer-capacity-stall-after-response
description: Use when reviewing Pingora's h2 upstream request-body pump around a write blocked on flow control, a `write_timeout` that fires after the origin answered, or any claim that a stalled upload should fail the exchange.
status: implemented-pending-project-checks
finding_id: H2-007
closed: 2026-08-27
---

# H2 request-body write stalled after a complete response

## Conclusion

An upstream that flags END_STREAM on its response and then grants no further
request-body flow-control window -- without ever sending RST_STREAM -- must not
be allowed to block the request-body write indefinitely, and the bounded
outcome must DELIVER the response rather than fail the exchange. The wire
END_STREAM flag plus a stalled write is the conjunction that authorizes
abandoning the upload; neither half authorizes it alone.

Fix suggestions of the form "a `write_timeout` is a local deadline and must
always fail the exchange, because swallowing it truncates the request body and
reports success" are **not accepted** in the presence of the wire flag. That
reasoning was correct as written but rested on a premise this shape breaks; see
rationale 2.

## Core rationale

**1. The wait genuinely has no end without an added bound**

`reserve_and_send` (`pingora-core/src/protocols/http/v2/mod.rs`) waits on
`SendStream::poll_capacity`, which resolves only when the peer grants window,
when the stream closes, or on a stream error. An origin that answers in full
and then simply stops granting window does none of the three, and `h2` exposes
no signal distinguishing "about to grant" from "never will". `write_timeout`
defaults to `None` in both upstream Pingora and the fork, so the default
configuration waits forever. This is inherited from upstream cloudflare/pingora
(`reserve_and_send` and `write_body` are byte-identical to `upstream/main`, and
`try_join!` there hangs exactly as the fork's `select!` does); the fork is
merely the only place able to fix it, because the wire END_STREAM evidence the
fix needs does not exist upstream.

**2. The damage is an unanswered client, not a leaked task**

`bidirection_down_to_up` awaits the request-body write INLINE in its duplex
loop's downstream arm, so a blocked write also stops the loop draining `rx` --
the channel carrying the upstream response tasks downstream, `TASK_BUFFER_SIZE`
= 4 slots. A small response fits those slots, `pipe_up_to_down_response`
returns `Ok`, and the client is then answered never while the complete response
sits in the proxy's own buffer. A larger response blocks in `tx.send()` with
the same result.

This is what invalidates the previous position on `write_timeout`. That
position ("the response is already in hand, so failing costs nothing") holds
for the RFC 9113 section 8.1 reset shape, where the write FAILS and the loop
carries on. It does not hold here, where the write does not return at all.
Failing the exchange answers the client with a 502 while holding a complete
200; with no `write_timeout` configured it answers nothing at all.

**3. Abandoning the upload conceals nothing**

The request half never receives its END_STREAM, so `h2` emits
RST_STREAM(CANCEL) when the `SendStream` drops at the end of the exchange --
the standard "upload aborted" signal. An origin that really was still consuming
the body sees a truncated request, not a whole one. The swallow is logged at
`warn`, and response completeness is still decided exclusively by the read half
(`Http2Session::response_body_complete_at_stream_end`), so nothing about a
stalled write can admit an incomplete body to cache.

**4. The two failure shapes are kept as separate questions**

`upstream_write_failed_because_stream_gone` asks whether `h2` will ever take
another byte; a local deadline genuinely does not answer it, and that function
and its test are unchanged. `upstream_write_stalled_after_response` asks the
different question -- the stream is still there but stopped taking bytes after
answering -- and `upstream_write_error_outcome` is the only place either is
combined with `upstream_response_ended.observed()`.

**5. The added probe never overrides operator configuration**

The stall probe is armed ONLY when `body_write.timeout.is_none()`. A consumer
that configured a `write_timeout` has already stated how long a stalled write
may last, and that path reaches the same outcome through
`upstream_write_stalled_after_response`. Edgion always sets
`peer.options.write_timeout` (`pg_upstream_peer.rs::configure_peer_timeouts`,
default 60s), so on Edgion's normal configuration the probe is not armed at all
and no new timer is introduced; the probe exists for the library default and
for Edgion's all-deadlines-disabled corner (`backendRequestTimeout: "0s"`, no
route/gRPC deadline, no captured body, where `replay_write_floor` does not
apply).

## Fix suggestions not accepted

- "Fail the exchange when the write stalls" — answers the client 502 while the
  proxy holds the origin's complete 200, and with no `write_timeout` never
  answers at all.
- "Bail out as soon as peer END_STREAM is observed, no timer needed" — RFC 9113
  lets a server that has answered in full go on receiving the request body, so
  this truncates every upload to an early-answering origin. No timer-free
  discriminator exists; "about to grant window" and "never will" are identical
  on the wire.
- "Add a `Notify` to `StreamRecord` so the writer can be woken exactly on peer
  EOS" (the issue's own suggested shape) — adds a synchronization primitive to
  the file with the most delicate memory-ordering contract in the fork, to buy
  timeliness a periodic probe already provides. Rejected in favor of sampling
  the existing atomic from a probe tick.
- "Race the write against the timer with `select!` on the future by value" —
  a tick would then CANCEL `write_body`, discarding its internal record of how
  much of the chunk already reached the wire, which cannot be resumed without
  re-sending bytes. The probe arm must poll `&mut write`.
- "Make the probe interval a peer option" — pushes a liveness backstop into the
  configuration surface; the operator-facing knob is `write_timeout`, which
  already exists and takes precedence.
- "Use `tokio::time::sleep` for the probe rather than `pingora_timeout`" —
  rejected, and `pingora-proxy` gained a `pingora-timeout` dependency for it.
  The probe is per body chunk, so its cost scales with request rate:
  `pingora_timeout` benchmarks ~4ns per create/cancel against Tokio's ~107ns,
  and deadlines rounded to the same 10ms tick share one timer. The crate does
  keep a cancelled fast timer until its deadline, which is why it falls back to
  Tokio for long deadlines — but its own threshold for "long" is
  `DEFAULT_FAST_TIMEOUT_TO_TOKIO_THRESHOLD` = 15 minutes, and the sharing
  bounds the residue at one entry per 10ms tick per thread regardless of rate.
  Do not re-open this on the strength of that doc note alone.
- "Default `peer.options.write_timeout` to a non-`None` value instead" —
  changes a public default for every write, including those with no wire
  evidence, and still produces the 502 this decision rejects.

## Re-evaluation triggers

Re-open this decision only if:

- `h2` gains a public signal for "this peer will not grant further window",
  which would remove the need for any timer here.
- The duplex loop stops awaiting the request-body write inline, so that a
  blocked write no longer starves response delivery — rationale 2 would then
  need re-deriving.
- Upstream Pingora fixes the unbounded `poll_capacity` wait itself, in which
  case the fork should adopt the upstream shape rather than keep this one.
- The mirror case now has a separate bounded-failure policy: an upstream that
  withholds request-body window and never flags END_STREAM reaches the H2
  writer's protocol-local progress floor when no explicit `write_timeout`
  exists. That expiry fails the exchange; it does not widen this record's
  successful-abandonment rule. See
  [`h2-writer-stall-without-end-stream-bounded.md`](h2-writer-stall-without-end-stream-bounded.md).

## Reference cases

- H2 whole-change audit 2026-08-26, issue H2-007
  (`../Edgion/tasks/todo/pingora-h2-end-stream-watch-simplification/issues/`).
- `pingora-proxy/src/proxy_h2_request_body.rs` — `UPSTREAM_STALL_PROBE_INTERVAL`,
  `write_upstream_body_watching_stall`, `upstream_write_stalled_after_response`,
  `upstream_write_error_outcome`.
- `pingora-proxy/tests/test_h2_upstream_stalled_after_response.rs` — the
  bounded-completion and truncated-response-still-fails contracts.
- `edgion-changes/features/h2-end-stream.md` — "Request-body writes".
- Related: [h2-local-reset-invalidates-shared-evidence.md](h2-local-reset-invalidates-shared-evidence.md),
  [abandoned-request-body-terminal-event.md](../../../../Edgion/skills/04-review/h2-grpc/abandoned-request-body-terminal-event.md),
  [timeout-honored.md](../../../../Edgion/skills/04-review/h2-grpc/timeout-honored.md).
