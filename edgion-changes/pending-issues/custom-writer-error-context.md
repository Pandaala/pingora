# Custom request-body writer error context

Status: open low-priority correctness/observability follow-up.

Ownership: fork custom upstream pump.

When a custom request-body writer rejects a write, `send_body_to_custom`
returns the upstream-classified error, but the custom pump records only an
errored downstream state and later replaces it with a generic `WriteError`.
Existing regression coverage verifies that an error reaches logging, not that
the original classification and context survive.

Revisit by preserving the first writer error through pump teardown and adding
an assertion on its classification/context. Keep this separate from the
early-response terminal-latch fix because it changes error propagation rather
than request-body polling ownership.
