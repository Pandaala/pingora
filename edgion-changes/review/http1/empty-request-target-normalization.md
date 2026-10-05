# Empty request targets normalize before URI construction

Status: fixed locally; publication not asserted

## Scope and correction

`RequestHeader::build` and `set_raw_path` share `parse_request_target`.
After removing a fragment, an empty target must resolve to `/` with
`RawTarget::FromUri`, no query or authority, and no preserved raw bytes.
The parser now returns `Uri::default()` explicitly for that case.

Previously the parser sent the empty string to `Uri::builder().path_and_query`.
The existing root-fallback contract passed with http 1.5.0 but failed with
http 1.4.2 (`InvalidUri(Empty)`). The fork declares a dependency range that
allows the older version, so relying on its newer permissive behavior was a
fork-owned normalization defect, not a reason to weaken the tests or refresh
the lockfile until they passed.

This concerns the header construction/mutation API and fragment stripping.
It does not permit empty HTTP/1 request lines through the wire parser, change
Host/authority reconciliation, or alter CONNECT targets containing an
authority. Nonempty origin-, absolute-, asterisk-, query-, and opaque forms
keep their existing behavior. The empty branch adds no allocation.

## Regression boundary

`test_target_that_is_only_a_fragment_falls_back_to_root` covers empty input,
`#`, `#frag`, and `#/admin`. It checks both construction and replacement of
previous absolute-form or non-UTF-8 targets, including cleared query,
authority, and raw-target state. Existing asterisk/query-only assertions and
the complete header suite protect neighboring forms.

The same defect surfaced in core `validate_connect` and
`test_absolute_form_and_connect_to_wire`; their fragment-only/empty cases
failed during header construction. Ordinary CONNECT was not generally broken.
Keep the serializer's root-target assertions as an independent wire check.

Verification is recorded in `edgion-changes/verification/test-matrix.md`.
Re-evaluate when changing URI dependencies, fragment removal, raw-target
representation, or constructor/mutation normalization.

Reference: P090-002 follow-up, 2026-10-05, checkout `/Volumes/ExtStore/ws3/pingora`
based on `9638a37`.
