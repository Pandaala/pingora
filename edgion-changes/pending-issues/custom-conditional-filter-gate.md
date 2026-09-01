# Custom response conditional-filter gate differs from H1/H2

## Status

Open investigation. Ownership: Pingora fork custom response pump.

Discovery baseline: fork `bd89d47`; recorded during the shared response-task
pipeline refactor on 2026-08-29.

## Observation

H1 and H2 run downstream conditional/range processing when
`Session::upstream_headers_mutated_for_cache()` is true. The custom upstream
path instead checks `session.cache.enabled()`. Cache can be disabled during the
response phase after headers were mutated, so these predicates are not
obviously equivalent.

The shared pipeline deliberately preserves this difference behind
`ResponseProtocol::Custom`. The refactor is behavior-neutral and must not turn
an unverified normalization into a hidden behavior change.

## Required investigation

1. Reproduce a custom-upstream response where cache processing mutates headers
   and then disables cache before downstream conditional/range filtering.
2. Compare H1, H2, and custom status, validators, range body, and cache outcome.
3. Inspect the Edgion consumer before changing the custom contract; the current
   checkout does not register a custom connector, but that premise can change.
4. If the predicates should be unified, change the shared policy and add an
   end-to-end regression that fails with the old custom gate.

## Revisit trigger

Revisit when custom upstreams are enabled by a consumer, when cache response
phase transitions change, or when a concrete divergent response is reproduced.
