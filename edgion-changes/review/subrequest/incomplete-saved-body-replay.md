# Incomplete `SavedBody` replay is an upstream alpha-API limitation

Status: accepted upstream limitation (2026-08-30)

Origin: upstream Pingora commit `d3a3b1a4252a9a11b78fb2c3fe7a42eba7f6d073`
(`Pipe subrequests utility`, 2026-01-30)

Reviewed baselines:

- Pingora fork: `44dbef281584f6ef4412fd44eea07dd56c5ae630`;
- local `upstream/main`: `09696b51bc59315353d96686355861604d0bb48c`;
- Edgion checkout: `83408c11dedb81eab8504d85edfb0fcc061c9e7f`;
- Edgion lockfile Pingora revision: `57f6183c38b5efbf9182f6a7a51bc7597cea265e`.

## Conclusion

`pingora_proxy::subrequest::pipe::SavedBody` correctly records whether capture
completed and whether its byte limit was exceeded. Its public infallible
`From<SavedBody> for InputBody` implementation nevertheless moves only the
retained chunks. It can therefore turn an incomplete or truncated prefix into
an ordinary preset body; if the first chunk exceeded the limit, it instead
turns the invalid capture into `InputBody::NoBody`.

This behavior is inherited unchanged from upstream and is still present in the
reviewed fork. It is not a defect introduced or amplified by the Edgion fork.
The sibling Edgion checkout allows selected plugins to spawn subrequests, but
searching its Rust sources found no use of `pipe_subrequest`, `SavedBody`,
`InputBodyType`, or `InputBody`; there is no current Edgion chained saved-body
replay path to guard.

The earlier fork inventory stated too broadly that every partial, cancelled,
drained, or poisoned capture is never replayed as complete. That guarantee is
owned and enforced by the fork's separate `RequestBodyBuffer` implementation,
not by upstream's alpha subrequest pipe. The inventory and detailed buffering
contract now name that boundary explicitly.

## Why there is no local code fix

Removing `From<SavedBody>` or replacing it with `TryFrom` changes a public API.
Keeping the infallible conversion while adding a checked alternative would not
make the upstream API fail closed: callers could still select the unsafe path.
Because the API is explicitly alpha, the root behavior is upstream-owned, and
this fork has no production consumer, a local public-API fork would be a
speculative compatibility burden rather than a necessary safety guard.

Do not silently convert incomplete capture into `NoBody`, and do not treat
preset-reader exhaustion as independent evidence that the original capture
was complete. If a fork or Edgion consumer adopts this pipeline before an
upstream fix, it must first reject any `SavedBody` for which
`is_body_complete()` is false and add end-to-end overflow and early-cancellation
coverage.

## Revisit triggers

Re-evaluate this decision when any of the following changes:

1. upstream replaces or deprecates the infallible conversion with a checked
   API that preserves incomplete/truncated state;
2. the fork changes `SavedBody`, `InputBody`, preset EOF, or subrequest framing
   semantics;
3. Edgion or another in-scope fork feature begins chaining a captured
   `SavedBody` through `pipe_subrequest`;
4. reproducible evidence shows that an existing fork path reaches the unsafe
   conversion.

At adoption time, cover overflow after a retained prefix, first-chunk overflow,
early response/cancellation, complete chunked and fixed-length bodies, and a
bodyless control. Remove this record only after the upstream checked handoff is
selected and those contracts pass.

## Closure evidence

An independent read-only review returned LGTM after checking the upstream
origin, all in-tree symbols and callers, H1 preset framing/EOF behavior, the
absence of an Edgion consumer, and the preserved fork-owned
`RequestBodyBuffer` contract.

The complete verification matrix passed on 2026-08-30:

- `cargo fmt --all -- --check` and `git diff --check`;
- both required `cargo check` configurations;
- core unit tests: 737 passed / 17 ignored by default, 742 passed / 17
  ignored with `connection_filter`, and 769 passed / 17 ignored with
  `boringssl`;
- the focused boringssl listener/PROXY protocol filter: 2 passed;
- proxy unit tests: 126 passed / 1 ignored manual benchmark;
- `test_request_body_seam`: 60 passed;
- `test_upstream_response_body_sink`: 57 passed;
- `test_terminal_body_dispatch`: 26 passed;
- `test_h2_upstream_no_error_reset`: 8 passed;
- `test_h2_upstream_stalled_after_response`: 4 passed;
- `test_h2_upstream_cache_and_reuse`: its known flow-control fixture failed
  once with a connection decode error, then the focused case passed and the
  complete target passed 8/8 on the isolated rerun;
- `cargo clippy -p pingora-proxy --all-targets` completed with only the
  pre-existing warnings recorded by the project matrix.

No production code or public API changed, so no new runtime regression test is
needed for this accepted upstream limitation.

## References

- `pingora-proxy/src/subrequest/pipe.rs`: capture state, conversion, preset
  reader, and terminal task production;
- `pingora-core/src/protocols/http/subrequest/server.rs`: cloned H1 framing
  selection;
- `pingora-core/src/protocols/http/subrequest/body.rs`: explicit close-delimited
  completion;
- `edgion-changes/features.md` and
  `edgion-changes/features/request-body-buffering.md`: corrected fork-owned
  replay boundary;
- originating finding:
  `tasks/issues/pipeline-subrequest-incomplete-saved-body-replay.md`.
