# Edgion Pingora fork guidance

## Known upstream h2 trailer limitation

`h2` can discard forbidden trailer pseudo-fields while exposing the remaining
ordinary fields as a valid-looking trailer map; it also lacks the required
oversized-trailer rejection at that handoff. A following
`RST_STREAM(NO_ERROR)` can obscure some failures from Pingora's completion
logic. This is a pre-existing, low-frequency upstream decoder/interface
limitation, not a regression introduced by this fork.

Accepted policy: do not vendor or locally fork `h2` for this issue. Keep the
normal upstream dependency and wait for an upstream fix. Until that fix is
adopted and covered by regression tests, do not simplify the END_STREAM watcher
on the assumption that terminal trailers and reset ordering are fully handled
by `h2`, and do not use wire-level END_STREAM alone as proof for cache admission.
