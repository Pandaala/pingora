# END_STREAM watcher dependency evidence is aligned with the supported h2 range

Status: resolved on 2026-08-29

Severity: medium

Ownership: Pingora fork. The upstream trailer-decoder limitation remains
separate and deferred.

Fork baseline: `bd89d47461e2b5df399c853cd6e28f07e4ce5359` on
`edgion_v3`. Sibling Edgion checkout inspected at
`83408c11dedb81eab8504d85edfb0fcc061c9e7f` with unrelated local changes.

## Original problem

`end_stream_watch.rs` said its proof was verified against h2 0.4.15, called the
workspace lockfile a pin, and claimed every later reset erased received
END_STREAM. The manifest actually allowed h2 0.4.16 or newer and the working
resolution was 0.4.19. Since h2 0.4.16, `State::recv_reset` preserves received
END_STREAM as `Cause::ErrorAfterEndStream`, so the documented premise was
false even at the declared minimum.

The watcher nevertheless depends on private receive-queue, buffer-lifetime,
GOAWAY, and frame-order behavior. It also provides independent wire byte-count,
terminal-HEADERS, local-reset, and request-upload evidence that the preserved
h2 state cannot replace.

## Resolution

The private-source audit and minimum-version run found a second, executable
dependency problem:

- h2 0.4.16, 0.4.17, and 0.4.18 consistently fail
  `h2_upstream_no_error_reset_keeps_streaming_while_the_client_uploads` with a
  connection-level `too_many_data_frames` error;
- h2 0.4.19 passes because its automatic small-DATA-frame budget scales with
  the configured connection window; and
- the fork configures large H2 windows, so 0.4.19 is the first tested release
  satisfying the continuing-upload contract.

The workspace minimum is therefore raised to `h2 >= 0.4.19`. This is a normal
open upstream range, not an exact pin, upper bound, or vendored fork. The
watcher module now records the audited 0.4.19 handoff checklist and no longer
uses reset-state loss as its justification. `Cargo.lock` remains only the
checkout's resolved snapshot.

The retained private facts audited in h2 0.4.19 are:

1. received END_STREAM survives reset as `ErrorAfterEndStream`;
2. `Recv::poll_data` drains queued receive events before surfacing state errors;
3. GOAWAY/connection errors do not clear `pending_recv`, while buffer clearing
   happens after the receive handle or all stream references are gone;
4. the connection task applies each decoded frame before polling the next; and
5. terminal trailer validation is still not strong enough to let raw wire
   END_STREAM prove response validity or cache admission.

## Verification evidence

- h2 0.4.16: core unit suite 737 passed / 17 ignored; stall suite 4 passed;
  cache/reuse suite 8 passed; reset suite 7 passed / 1 failed reproducibly.
- h2 0.4.17 and 0.4.18: the same focused continuing-upload reset case failed
  reproducibly with `too_many_data_frames`.
- h2 0.4.19: the focused case passed; the complete current-version H2 and
  project matrix is recorded in `verification/test-matrix.md`.
- After the temporary version matrix, the ignored local `Cargo.lock` again
  resolves h2 0.4.19. No pre-audit hash exists, so this does not claim
  byte-for-byte lockfile identity; the reviewed dependency declaration change
  is the minimum in the root `Cargo.toml`.

## Edgion adoption boundary

The sibling checkout is not evidence that this change is already consumed:

- Edgion HEAD `83408c11dedb81eab8504d85edfb0fcc061c9e7f` has a committed
  `Cargo.lock` selecting fork commit
  `480bad2a8cb85e235079968955673d63749a61a4` and h2 0.4.15;
- its dirty working lock selects fork commit
  `57f6183c38b5efbf9182f6a7a51bc7597cea265e` and still h2 0.4.15; and
- the dirty local path patch is unused because that lock expects Pingora
  0.8.1, while this checkout currently declares 0.8.0.

When Edgion adopts this fork revision it must update the Pingora source and h2
resolution together and run its own locked build. Neither sibling lock describes
this working tree or a deployed revision.

Future h2 upgrades must repeat the minimum/current behavioral matrix and the
private handoff checklist. An upstream release preserving received END_STREAM
is not itself grounds to remove the watcher. Simplification still requires
wire/delivered byte parity, terminal trailer validation, local reset, GOAWAY,
cache admission, and request-upload contracts to remain proven.

Related durable records:

- [Known upstream limitations](../upstream-limitations.md)
- [Version-robust dependency baselines](../h2-grpc/h2-dependency-baseline-tests-version-robust.md)
- [H2 feature contract](../../features/h2-end-stream.md)
- [Manual verification matrix](../../verification/test-matrix.md)
