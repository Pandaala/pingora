# H1 trailer writes use the session write bound

- Finding: ISSUE-001 (fork-owned omission).
- Status: implemented; independent code review PASS; terminal integration verification pending its environment prerequisite.
- Source baseline: Pingora `1aaa45cc70430bc782990bc20ddee3fef36d6b99`.

Direct trailer delivery previously awaited write and flush without the session
timeout; queued delivery passed `None` to the existing persistent HeaderWriter.
Both paths now use `HttpSession::write_timeout` with serialized trailer length.
The policy remains session-owned, and the queued timer retains the same
cancellation and lazy-start semantics as the existing writer. BodyWriter marks
completion only after successful flush; trailer bytes do not count as application
body bytes. The canonical contract is in
[response trailers](../../features/response-trailers.md#http1-behavior).

The repair is confined to supported nonempty H1 trailers. It does not replace
upstream header/finalization behavior, change no-timeout configuration, make
direct writes cancel-safe, or promise that cache fill stops when downstream
writing fails. The known upstream H2 trailer decoder limitation is unchanged.

Consumer source inspection at Edgion `bda681e4e6cf4bb93eb4eaec9e4dccff75d6d546`
found `apply_client_policy` configures the affected downstream write timeout.
Its manifest selects `edgion_0.9.0_v1` and lock selects the Pingora baseline
above. No Edgion source/configuration/dependency change is required; this source
observation is not deployment evidence.

Regression coverage lives in `v1/server_test_trailer_timeout.rs` and
`pingora-proxy/src/proxy_h1_tests.rs`: write/flush stalls, queued cancellation
and resumption, exact wire output, validation, byte accounting, no-bound and
minimum-send-rate behavior, plus actual H1 paired-pump failure/non-reuse after
decoded upstream trailers while the origin stays open. Each pump mode has a
writable positive control.

Fresh verification on the local fix: core H1 unit tests 389 passed; proxy
library 231 passed and two existing ignored tests; request-body seam 68 passed;
response-body sink 62 passed. Default core/proxy check, core
`connection_filter boringssl` check, all-target core/proxy Clippy, formatting
and diff whitespace checks passed. Clippy retains warnings in unchanged code.
The 32-case `test_terminal_body_dispatch` integration target could not pass
because the required `127.0.0.2` loopback alias is absent (OS error 49);
`sudo -n ifconfig lo0 alias 127.0.0.2 up` could not add it because sudo requires
a password. This is an unresolved environmental verification limitation,
not a passing integration run. No full Edgion test or deployed behavior is claimed.

Independent reviewer `/root/result_review` returned code PASS with no corrections.
Full closure remains pending `cargo test --locked -p pingora-proxy --test
test_terminal_body_dispatch` after the required loopback alias is supplied; the
source finding is preserved until that gate is resolved.
