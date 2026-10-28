# HTTP/1.0 trailer planning must preserve self-delimited responses

Status: resolved fork defect (2026-08-28)

Response-trailer capability planning runs before an H1 response header is
committed so a same-batch trailer hook can query the selected framing. The
initial merge applied the HTTP/1.0 close-delimited downgrade before checking
whether an informational response would be ignored or whether the response was
self-delimited by method/status semantics.

That ordering could disable keepalive for an ignored 1xx, HEAD, 204, or 304
response without a Content-Length. The planner now returns without mutation for
ignored informational responses and applies `Connection: close` only to a
body-bearing final response that actually needs close delimiting. Regression
tests cover ignored 103 plus HEAD, 204, 304, and non-ignored informational
responses on an HTTP/1.0 keepalive request.

Re-open if response-framing planning moves again, or if the set of responses
treated as bodyless by `HttpSession::init_body_writer` changes without matching
the planning predicate.
