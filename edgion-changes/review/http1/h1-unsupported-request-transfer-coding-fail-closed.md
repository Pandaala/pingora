# Unsupported H1 request transfer coding fails closed

Status: resolved finding

Classification: inherited upstream limitation with a fork-owned proxy admission guard

Resolved: 2026-08-30

## Conclusion

H1 transfer-coding fails closed in two layers. The generic H1 reader returns
400 and closes for any HTTP/1.0 `Transfer-Encoding` and for HTTP/1.1 forms whose
final coding token is missing or is not `chunked`. It does not comprehensively
validate every preceding list element; a form it accepts because its final
token is `chunked` is still subject to the stricter proxy admission below. An
otherwise valid request can instead return 405 and close at the
default-disabled CONNECT gate before proxy admission.

For external H1 requests that reach `HttpProxy`, `Transfer-Encoding` is
forwardable only when it is absent or is one header field whose trimmed value
is exactly `chunked`, case-insensitively. A core-accepted final-`chunked` form
that is not that single exact field exits through the normal error-rendering
and logging path with `ErrorType::HTTPStatus(501)`. The default renderer and
the reviewed Edgion renderer write 501, but a custom `fail_to_proxy` owns its
wire response; the framework guarantee is the submitted error type and forced
close, not an application-independent status code.

All of these rejection layers terminate before the mutable connection-reuse
hook, application filters, cache lookup, upstream selection, buffering, retry,
or an upstream pump can route the request. The proxy admission policy both
marks the H1 session non-reusable after every mutable error hook and directly
vetoes returning a reusable downstream session.

This is intentionally a proxy policy, not a change to Pingora's generic H1
parser. Non-proxy H1 applications, H2, subrequests, and custom sessions keep
their existing contracts. No public API was added.

## Root cause and ownership

Pingora accepts a final `chunked` transfer-coding sequence and its H1 body
reader removes that chunk framing. It does not decode preceding transfer
codings such as `gzip`. Standard proxy sanitization later removes the complete
hop-by-hop `Transfer-Encoding` field. The H1 upstream path can then add only
new chunk framing, while the H2 path sends the same still-coded bytes as DATA.
Both paths would give the origin bytes whose metadata no longer describes
them.

The sanitizer behavior and its old bug-locking integration assertion came
from upstream Pingora commit `28c18e6b6b04dd0bd937234e5950049e6d74cded`.
The root limitation is therefore inherited. The Edgion fork owns the proxy
boundary that amplifies it into silent request-body corruption, so it retains
the small admission guard while waiting for normal upstream support.

This follows RFC 9112 section 6.1: changing transfer codings requires the
message framing metadata to continue describing the body. It also follows the
fail-closed principle and validator strategy in Envoy's default HTTP/1 header
validator. At the fixed Envoy revision below, the validator accepts `chunked`
case-insensitively and rejects a nonempty different value as
`http1.invalid_transfer_encoding`; its source cites the expected 501 response,
and its focused test accepts `ChuNKeD` while rejecting `gzip`:

- <https://www.rfc-editor.org/rfc/rfc9112.html#section-6.1>
- <https://github.com/envoyproxy/envoy/blob/9c085ba4a27fddea1811a8ecc98fc094dbdd455c/source/extensions/http/header_validators/envoy_default/http1_header_validator.cc#L410-L427>
- <https://github.com/envoyproxy/envoy/blob/9c085ba4a27fddea1811a8ecc98fc094dbdd455c/test/extensions/http/header_validators/envoy_default/http1_header_validator_test.cc#L66-L76>

This is not a claim of complete behavioral equivalence. Pingora's generic
parser and proxy admission are separate layers, custom error renderers own the
wire status, and Pingora removes `Content-Length` while initially disabling
keepalive when it coexists with `Transfer-Encoding`. An accepted exact
`chunked` request then remains on the ordinary application-controlled path, so
custom hooks can change keepalive; it does not receive the unsupported-coding
path's final reassert or direct reuse veto. Envoy can apply different parser
and `Transfer-Encoding` plus `Content-Length` policy.

## Why the guard is at proxy admission

- Moving the additional restriction into the core parser would alter every
  Pingora H1 server and collapse the existing generic 400 framing boundary
  into the proxy's `HTTPStatus(501)` error-rendering path.
- An H1-pump-only check would leave H2 conversion and earlier cache/filter
  behavior inconsistent.
- Preserving the inbound field would also preserve hop-by-hop and
  connection-nominated metadata, reopening unrelated forwarding hazards.
- Decoding every registered and extension transfer coding would be a broad,
  speculative local fork of upstream protocol behavior.

At admission, the proxy still has the original H1 headers and no request body
has been consumed. Marking the connection non-reusable before invoking custom
`fail_to_proxy`, making the final keepalive write after every mutable error hook
(including `logging`) returns, and directly forcing the proxy's downstream
reuse decision false prevent a renderer, logger, or unread body bytes from
turning a suffix into a new exchange. Because rejection precedes all
request-filtering, cache, and upstream phases, buffered replay, native retry,
cache hit/fill, filter-owned local response, and all three pumps are
structurally unreachable; duplicating the predicate inside those paths would
weaken the single boundary rather than add coverage.

`UpstreamRequestBodyDisposition::Streamed` still preserves non-`chunked`
values synthesized by an application in `upstream_request_filter`. Those
values describe application-owned bytes after external request admission and
are not a bypass for client-supplied composite transfer coding.

## Regression evidence

The self-contained `pingora-proxy/tests/test_request_body_seam.rs` harness has
no OpenResty dependency and uses loopback scripted origins with independent
connection recorders.

- H1-to-H1 and H1-to-H2 cases send a valid gzip member inside chunk framing,
  followed by a pipelined request. Both receive 501, reach EOF/reset promptly,
  contain no follow-up 200, and record zero origin TCP connections.
- H1-to-H1 and H1-to-H2 positive controls send plain `chunked`, receive 200,
  and record exactly five payload bytes at the origin.
- A reused-connection control persists application context after a legal
  request. A legal same-connection positive control first proves that
  `on_connection_reuse` received that context and rewrote the selected origin;
  a subsequent composite-coded request still receives 501, closes, and records
  zero connections to its intended origin even though the hook would remove
  `Transfer-Encoding`, proving admission precedes the mutable hook.
- A custom error renderer writes the 501, deliberately calls
  `set_keepalive(Some(60))`, and returns `can_reuse_downstream=true`; the custom
  logger re-enables keepalive once more. Its negative control must send a
  same-socket follow-up only after reading the complete first response. A
  pre-buffered pipelined suffix is not valid proof here because the generic H1
  reuse layer can independently reject pipelining even if the proxy-level
  forced-close decision is removed.
- A proxy-local table test accepts absent, case-varied, and OWS-trimmed single
  `chunked`; it rejects gzip, deflate, unknown, duplicate, multiple-field,
  trailing-empty-token, and empty forms.
- The legacy OpenResty test that asserted silent `gzip` metadata loss as
  successful normalization was removed.

Zero origin connections are the out-of-band proof that no upstream attempt or
pump was entered; the guard's placement before cache and retry setup makes
those phases structurally unreachable. Downstream EOF/reset is the independent
proof that an unread rejected body cannot leave the H1 connection reusable.

## Edgion consumer check

Review used Pingora fork commit
`2fbd19589e415d7f6a8877c3427794a5ad554a89` as its implementation base and
the dirty sibling Edgion checkout at
`83408c11dedb81eab8504d85edfb0fcc061c9e7f`. The sibling's `Cargo.toml` selects
the `edgion` git branch but its development patch points `pingora-proxy` and
related crates at `../pingora`; its lockfile still records remote Pingora
`57f6183c38b5efbf9182f6a7a51bc7597cea265e`. These are development and
declared dependency states, not claims about a deployed revision.

Edgion constructs ordinary `HttpPeer` values without overriding
`http_upstream_request_policy`, so the inherited corruption path was reachable.
Its `fail_to_proxy` preserves `ErrorType::HTTPStatus(code)`, so this reviewed
Edgion implementation writes 501 without an Edgion change. That observation
does not strengthen the generic `ProxyHttp` contract for other custom
renderers. The guard runs before Edgion's request filter and upstream peer
selection.

The cross-repository review also found that Edgion's
`pg_early_request_filter.rs` unconditionally set a configured keepalive timeout
on every non-shutdown request. That consumer hook could turn Pingora's existing
H1 `keepalive=None` back into `Some`, including the core refusal for exact
`Transfer-Encoding: chunked` plus `Content-Length` and HTTP/1.0 without an
explicit keepalive request. The sibling fix now applies the configured timeout
only when `Session::get_keepalive()` shows that the transport still permits
reuse; a pre-existing `None` is preserved. This is an Edgion consumer fix to a
sound Pingora API contract and requires no new public fork interface. Its local
duplex-session tests cover ordinary HTTP/1.1, transfer encoding plus content
length, HTTP/1.0 default/explicit keepalive, and graceful shutdown. Those tests
and `cargo check -p edgion-gateway --lib` passed against Edgion's resolved
Pingora fork revision `57f6183c38b5efbf9182f6a7a51bc7597cea265e`; Cargo
reported that the sibling path patch was unused because its package version did
not match the resolved `0.8.1` graph. The Pingora change itself is therefore
validated by this repository's own test matrix, not inferred from that Edgion
build.

Edgion's prior durable decision
`skills/04-review/http1/streamed-transfer-encoding-preserves-undecoded-codings.md`
had accepted external composite final-`chunked` coding while naming a common
ingress policy as its re-evaluation trigger. This proxy admission satisfies that
trigger. The sibling record is now marked fixed/superseded, links back here as
the canonical transport contract, and retains only the separate rule for
application-synthesized metadata after external admission.

## Upstream revisit trigger

Do not vendor or locally fork a transfer-coding decoder. Revisit this guard
when upstream Pingora adopts and tests one of these complete contracts:

1. reject unsupported request transfer codings at an equivalent pre-routing
   boundary with a terminal error and non-reuse; or
2. decode every removed coding and accurately reframe both H1 and H2 upstream
   bodies.

Before removing the local guard, retain the negative H1/H2 origin evidence and
plain-chunked controls against the adopted upstream behavior.

## Independent review

A fresh independent subagent review found two mutable-hook ordering bypasses
before closure:

1. `on_connection_reuse` originally ran before admission and could remove the
   external field. Admission was moved before that hook and the same-connection
   regression above was added.
2. A custom `fail_to_proxy` could undo the initial keepalive disable, and the
   following mutable `logging` hook could do the same after the renderer. The
   admission error path now disables reuse one final time after both hooks,
   with the adversarial renderer/logger regression above.

Both findings were fixed without changing the generic H1 parser or ordinary
error-path reuse semantics. The reviewer rechecked the corrected boundary
before the originating finding was closed.

A later multi-perspective design review found no additional production bypass,
but identified two evidence and documentation gaps:

1. the renderer/logger regression's pre-buffered pipelined follow-up could be
   rejected by generic H1 pipelining policy even without the final proxy
   forced-close decision, so it required a sequential same-socket follow-up
   after the complete first response; and
2. the earlier prose incorrectly presented every unsupported form as a wire
   501, obscuring generic 400 framing failures, the 405 CONNECT gate, and the
   fact that a custom renderer owns its emitted status.

The production boundary was strengthened for clarity with a private named
reuse policy: `ForceClose` performs the final keepalive write and also directly
vetoes the proxy's `can_reuse_downstream` decision. The focused test execution
result is recorded only in the verification snapshot after it is actually run;
this review record does not infer a pass from the design change.

The next independent cross-repository review found one consumer-side contract
violation: Edgion's early request hook could reopen a keepalive state that the
Pingora H1 parser had already disabled. The fix belongs in Edgion, where the
configured timeout is now applied monotonically only to an already reusable
session. This preserves the generic Pingora API and also corrects the adjacent
HTTP/1.0 default-close case.

Two fresh reviewers then independently examined the combined Pingora and
Edgion candidate. The first found that the documentation overgeneralized the
generic H1 reader's 400 boundary as covering every malformed form; the text now
states the narrower final-token rule and the stricter proxy admission that
follows it. The second found that Edgion's older accepted-tradeoff record still
described composite external coding as allowed even though its stated common
ingress re-evaluation trigger had fired. That durable record and its closed
finding index now point to this canonical contract and distinguish external
admission from post-admission application-synthesized metadata. Both reviewers
rechecked their corrections and returned LGTM with no remaining blocker.
