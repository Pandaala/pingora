// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests for HTTP/1.1 request pipelining support (RFC 9112 §9.3.2).
//!
//! Pipelining is an opt-in behavior: when enabled via
//! [`HttpSession::set_pipelining_enabled`], the session tolerates
//! overread bytes on reuse (they belong to the next request) and a new
//! session can have them fed in via [`HttpSession::set_pipelined_prefix`].
//!
//! When disabled (default), overread bytes cause [`HttpSession::reuse`]
//! to return `Ok(None)` so the connection closes — the historical
//! behavior preserved for callers that do not opt in.

use super::*;
use rstest::rstest;
use tokio_test::io::Builder;

fn init_log() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Default state: pipelining is off.
#[tokio::test]
async fn pipelining_disabled_by_default() {
    init_log();
    let mock_io = Builder::new().build();
    let s = HttpSession::new(Box::new(mock_io));
    assert!(!s.pipelining_enabled());
}

/// Toggling the pipelining flag is round-trippable.
#[tokio::test]
async fn set_pipelining_enabled_toggles() {
    init_log();
    let mock_io = Builder::new().build();
    let mut s = HttpSession::new(Box::new(mock_io));
    assert!(!s.pipelining_enabled());
    s.set_pipelining_enabled(true);
    assert!(s.pipelining_enabled());
    s.set_pipelining_enabled(false);
    assert!(!s.pipelining_enabled());
}

/// When pipelining is disabled (default), overread bytes must cause
/// reuse to return `None`. Pipelining opt-in must not regress that
/// compatibility behavior.
#[rstest]
#[case(true)] // pipelining explicitly off
#[case(false)] // pipelining flag never set
#[tokio::test]
async fn reuse_rejects_overread_when_pipelining_disabled(#[case] explicit_off: bool) {
    init_log();
    let request = b"GET / HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\npipelined_next";
    let mock_io = Builder::new().read(request).build();
    let mut s = HttpSession::new(Box::new(mock_io));
    if explicit_off {
        s.set_pipelining_enabled(false);
    }
    s.read_request().await.unwrap();
    // Overread is captured when body reading initializes — poll the
    // body to trigger the init_content_length path.
    let _ = s.read_body_bytes().await.unwrap();
    assert!(s.body_reader.has_bytes_overread());
    let reused = s.reuse().await.unwrap();
    assert!(
        reused.is_none(),
        "reuse must return None without pipelining"
    );
}

/// When pipelining is enabled and overread bytes are present,
/// reuse returns both the stream and the extracted prefix.
#[tokio::test]
async fn reuse_allows_overread_when_pipelining_enabled() {
    init_log();
    let request = b"GET / HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\npipelined_next";
    let mock_io = Builder::new().read(request).build();
    let mut s = HttpSession::new(Box::new(mock_io));
    s.set_pipelining_enabled(true);
    s.read_request().await.unwrap();
    let _ = s.read_body_bytes().await.unwrap();
    assert!(s.body_reader.has_bytes_overread());

    let reused = s.reuse().await.unwrap().expect("connection reusable");
    let (_stream, prefix) = reused.into_parts();
    let prefix = prefix.expect("overread must be returned as pipelined prefix");
    assert_eq!(prefix.as_ref(), b"pipelined_next");
}

/// Same-read pipelining with no prior body poll still extracts the prefix.
#[tokio::test]
async fn reuse_extracts_prefix_without_body_poll() {
    init_log();
    let req1 = b"GET /one HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mut combined = Vec::with_capacity(req1.len() + req2.len());
    combined.extend_from_slice(req1);
    combined.extend_from_slice(req2);

    let mock_io = Builder::new().read(&combined).build();
    let mut a = HttpSession::new(Box::new(mock_io));
    a.set_pipelining_enabled(true);
    a.read_request().await.unwrap();
    assert_eq!(a.req_header().uri.path(), "/one");

    let reused = a.reuse().await.unwrap().expect("connection reusable");
    let (stream, prefix) = reused.into_parts();
    let prefix = prefix.expect("pipelined prefix must be extracted during reuse");
    assert_eq!(prefix.as_ref(), req2);

    let mut b = HttpSession::new(stream);
    b.set_pipelining_enabled(true);
    b.set_pipelined_prefix(prefix);
    b.read_request()
        .await
        .unwrap()
        .expect("pipelined request must parse");
    assert_eq!(b.req_header().uri.path(), "/two");
}

/// Content-Length: 0 has the same extraction requirement as absent length.
#[tokio::test]
async fn reuse_extracts_content_length_zero_prefix_without_body_poll() {
    init_log();
    let req1 = b"GET /one HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    let mut combined = Vec::with_capacity(req1.len() + req2.len());
    combined.extend_from_slice(req1);
    combined.extend_from_slice(req2);

    let mock_io = Builder::new().read(&combined).build();
    let mut a = HttpSession::new(Box::new(mock_io));
    a.set_pipelining_enabled(true);
    a.read_request().await.unwrap();
    assert_eq!(a.req_header().uri.path(), "/one");

    let reused = a.reuse().await.unwrap().expect("connection reusable");
    let (_stream, prefix) = reused.into_parts();
    let prefix = prefix.expect("pipelined prefix must be extracted during reuse");
    assert_eq!(prefix.as_ref(), req2);
}

/// The new session parses the pipelined prefix as the start of a
/// request without issuing any stream read — the mock_io allows no
/// reads, so if read_request() tried to pull from the stream it would
/// panic. This is the essential pipelining property: a prefix that
/// already contains a complete request is parsed without waiting for
/// additional bytes.
#[tokio::test]
async fn read_request_consumes_complete_prefix_without_stream_read() {
    init_log();
    let prefix = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    // Mock IO that would panic on any read — ensures the parse is
    // wholly satisfied by the pipelined prefix.
    let mock_io = Builder::new().build();
    let mut s = HttpSession::new(Box::new(mock_io));
    s.set_pipelined_prefix(BytesMut::from(&prefix[..]));
    let n = s
        .read_request()
        .await
        .unwrap()
        .expect("request must parse from prefix alone");
    assert!(n > 0);
    assert_eq!(s.req_header().uri.path(), "/two");
}

/// When the prefix is only the beginning of a request, read_request()
/// continues to read from the stream to complete the header.
#[tokio::test]
async fn read_request_falls_through_to_stream_for_partial_prefix() {
    init_log();
    let prefix = b"GET /two HTTP/1.1\r\nHost: ";
    let rest = b"pingora.org\r\nContent-Length: 0\r\n\r\n";
    let mock_io = Builder::new().read(rest).build();
    let mut s = HttpSession::new(Box::new(mock_io));
    s.set_pipelined_prefix(BytesMut::from(&prefix[..]));
    let n = s
        .read_request()
        .await
        .unwrap()
        .expect("request must parse across prefix + stream");
    assert!(n > 0);
    assert_eq!(s.req_header().uri.path(), "/two");
}

/// Body-pump path: request 2's bytes arrive in a SEPARATE read
/// after request 1 has been fully consumed. The proxy's body-pump
/// loop polls the downstream socket via
/// [`HttpSession::read_body_or_idle`]`(true)` while request 1's
/// response is still being written. The idle branch at
/// `read_body_or_idle` currently raises
/// `ConnectError("Sent data after end of body")` when the idle
/// read returns > 0 bytes — which is exactly the shape pipelining
/// traffic takes when requests span TCP segment boundaries.
///
/// This covers the two-segment pipelining case: request 2's bytes
/// arrive during the proxy's idle poll, not during request 1's body
/// read. The reuse() overread path (already covered by the tests
/// above) never fires because request 2's bytes were never in
/// `body_buf_overread` to begin with.
///
/// When pipelining is enabled on the session, this branch must
/// NOT raise `ConnectError`. Instead, the byte(s) read by
/// `idle()` must be stashed so the reuse() path can hand them
/// to the next session via the standard `take_body_overread`
/// extractor. `idle()` uses a 1-byte probe buffer, so the
/// overread surface will typically hold 1 byte per idle poll —
/// the remaining bytes of request 2 stay on the underlying
/// stream and are read by the next session's `read_request`
/// (which seeds itself with the pipelined prefix and continues
/// reading from the stream to complete the header).
#[tokio::test]
async fn idle_read_stashes_bytes_when_pipelining_enabled() {
    init_log();
    let req1 = b"GET /one HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    // Only the first byte of req2 is queued — the idle-branch
    // read in `read_body_or_idle` uses a 1-byte probe buffer,
    // so that's all it will consume. The rest of req2 would
    // live on the kernel socket buffer in real traffic and be
    // drained by the next session.
    let req2_first = b"G";

    // No `.wait(...)` between the two reads — we want the
    // second read to be immediately available once the first
    // consumer polls. `tokio-test::io::Builder` delivers reads
    // one poll at a time regardless, which is what models a
    // TCP segment boundary for our purposes.
    let mock_io = Builder::new().read(&req1[..]).read(&req2_first[..]).build();

    let mut s = HttpSession::new(Box::new(mock_io));
    s.set_pipelining_enabled(true);

    // Consume request 1 fully. Body is zero-length so body_done
    // is true; no overread is captured in body_buf_overread
    // because req2's bytes were NOT in the same read as req1.
    s.read_request().await.unwrap();
    assert_eq!(s.req_header().uri.path(), "/one");
    let _ = s.read_body_bytes().await.unwrap();
    assert!(s.is_body_done());
    assert!(
        !s.body_reader.has_bytes_overread(),
        "precondition: req2 must arrive in a separate read, not as overread on req1"
    );

    // This is the proxy's body-pump poll. Post-fix, the idle
    // branch reads the byte, pushes it to the body reader's
    // overread surface, and stays pending — signaling the
    // body-pump `select!` loop that the downstream has no more
    // body activity to wait on (the loop exits via its other
    // branches when the upstream response completes).
    //
    // We assert the *causal* invariant, not a wall-clock one:
    // poll the future repeatedly, yielding between polls to
    // let the mock I/O stack drain, until either (a) it
    // resolves (which is a failure — it MUST stay pending) or
    // (b) we observe enough bookkeeping progress to know the
    // idle read has completed. The proxy_tasks channel via
    // `proxy_tasks_rx` isn't wired in this test, so "enough
    // progress" is signaled by tracking `poll_count` alone;
    // the actual overread presence is asserted after the
    // future is dropped.
    //
    // Scope the future in an async block so its borrow on `s`
    // ends when we exit the block — the body-reader check
    // needs a fresh borrow.
    {
        let fut = s.read_body_or_idle(true);
        tokio::pin!(fut);
        // Drive the future forward a bounded number of times.
        // Under the fix it will always stay Pending; a broken
        // fix resolves Ready in the first few polls.
        for _ in 0..10 {
            match futures::poll!(fut.as_mut()) {
                std::task::Poll::Pending => {
                    tokio::task::yield_now().await;
                }
                std::task::Poll::Ready(Err(e)) => panic!(
                    "read_body_or_idle(true) must not raise an error when \
                     pipelining is enabled and the idle read returns > 0 bytes \
                     (those bytes are the start of pipelined request 2, not \
                     illegal trailing body). Got error: {e:?}"
                ),
                std::task::Poll::Ready(Ok(body)) => panic!(
                    "read_body_or_idle(true) must stay pending after stashing \
                     pipelined bytes (the body-pump `select!` exits via its \
                     other branches). Got body: {body:?}"
                ),
            }
        }
        // Future still pending — exit the scope, which drops
        // `fut` and releases the mutable borrow on `s`.
    }

    // The byte must be extractable as overread, so the
    // standard reuse() + HttpPersistentSettings pipeline can
    // hand it to the next session.
    let overread = s
        .take_body_overread()
        .expect("pipelined request 2 byte must be retrievable as overread");
    assert_eq!(
        overread.as_ref(),
        req2_first,
        "stashed bytes must be the idle-read probe byte from request 2"
    );
}

/// Symmetric to the test above: pipelining OFF means the idle
/// branch still raises `ConnectError` as it did pre-patch. This
/// preserves upstream behavior for non-adopters.
#[tokio::test]
async fn idle_read_still_raises_when_pipelining_disabled() {
    init_log();
    let req1 = b"GET /one HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    // Single byte of req2 — idle-branch read uses a 1-byte probe
    // buffer, error path fires, mock is fully drained.
    let req2_first = b"G";

    let mock_io = Builder::new().read(&req1[..]).read(&req2_first[..]).build();

    let mut s = HttpSession::new(Box::new(mock_io));
    // Leave pipelining at the default (off).
    s.read_request().await.unwrap();
    let _ = s.read_body_bytes().await.unwrap();
    assert!(s.is_body_done());

    let err = s
        .read_body_or_idle(true)
        .await
        .expect_err("pipelining off: idle read > 0 must raise ConnectError");
    assert_eq!(
        *err.etype(),
        pingora_error::ErrorType::ConnectError,
        "non-adopter callers must still see ConnectError on surplus idle bytes"
    );
}

/// End-to-end: session A finishes with overread, bytes are extracted,
/// session B consumes them via set_pipelined_prefix and parses the
/// pipelined request without reading from the (empty) stream.
#[tokio::test]
async fn pipelined_request_chain_end_to_end() {
    init_log();

    // Session A: read request 1 with pipelined request 2 bytes appended.
    let req1 = b"GET /one HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    let mut combined = Vec::with_capacity(req1.len() + req2.len());
    combined.extend_from_slice(req1);
    combined.extend_from_slice(req2);

    let mock_io_a = Builder::new().read(&combined).build();
    let mut a = HttpSession::new(Box::new(mock_io_a));
    a.set_pipelining_enabled(true);
    a.read_request().await.unwrap();
    assert_eq!(a.req_header().uri.path(), "/one");
    // Poll the body to trigger init_content_length which captures
    // the bytes past Content-Length: 0 as overread.
    let _ = a.read_body_bytes().await.unwrap();
    assert!(a.body_reader.has_bytes_overread());

    let overread = a.take_body_overread().expect("overread present");

    // Session B: construct with an empty stream (pipelined prefix is
    // everything we need), feed the overread, parse the next request.
    let mock_io_b = Builder::new().build();
    let mut b = HttpSession::new(Box::new(mock_io_b));
    b.set_pipelining_enabled(true);
    b.set_pipelined_prefix(overread);
    b.read_request()
        .await
        .unwrap()
        .expect("pipelined request must parse");
    assert_eq!(b.req_header().uri.path(), "/two");
}

#[tokio::test]
async fn bodyless_pipeline_transfers_prefix_without_copying() {
    init_log();
    let req1 = b"GET /one HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mut prefix = BytesMut::with_capacity(req1.len() + req2.len());
    prefix.extend_from_slice(req1);
    prefix.extend_from_slice(req2);
    let base = prefix.as_ptr();

    let mock_io = Builder::new().build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.set_pipelining_enabled(true);
    session.set_pipelined_prefix(prefix);
    session.read_request().await.unwrap();

    let reused = session.reuse().await.unwrap().expect("connection reusable");
    let (_, prefix) = reused.into_parts();
    let prefix = prefix.expect("second request remains buffered");
    assert_eq!(prefix.as_ptr(), unsafe { base.add(req1.len()) });
    assert_eq!(prefix.as_ref(), req2);
}

#[tokio::test]
async fn content_length_pipeline_transfers_prefix_without_copying() {
    init_log();
    let req1 = b"POST /one HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 4\r\n\r\nbody";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mut prefix = BytesMut::with_capacity(req1.len() + req2.len());
    prefix.extend_from_slice(req1);
    prefix.extend_from_slice(req2);
    let base = prefix.as_ptr();

    let mock_io = Builder::new().build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.set_pipelining_enabled(true);
    session.set_pipelined_prefix(prefix);
    session.read_request().await.unwrap();

    let reused = session.reuse().await.unwrap().expect("connection reusable");
    let (_, prefix) = reused.into_parts();
    let prefix = prefix.expect("second request remains buffered");
    assert_eq!(prefix.as_ptr(), unsafe { base.add(req1.len()) });
    assert_eq!(prefix.as_ref(), req2);
}

#[tokio::test]
async fn chunked_pipeline_with_trailers_transfers_prefix_without_copying() {
    init_log();
    let req1 = b"POST /one HTTP/1.1\r\nHost: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\nX-Test: one\r\n\r\n";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mut prefix = BytesMut::with_capacity(req1.len() + req2.len());
    prefix.extend_from_slice(req1);
    prefix.extend_from_slice(req2);
    let base = prefix.as_ptr();

    let mock_io = Builder::new().build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.set_pipelining_enabled(true);
    session.set_pipelined_prefix(prefix);
    session.read_request().await.unwrap();

    let reused = session.reuse().await.unwrap().expect("connection reusable");
    let (_, prefix) = reused.into_parts();
    let prefix = prefix.expect("second request remains buffered");
    assert_eq!(prefix.as_ptr(), unsafe { base.add(req1.len()) });
    assert_eq!(prefix.as_ref(), req2);
}

#[tokio::test]
async fn escaped_uri_pipeline_does_not_copy_following_requests() {
    init_log();
    let req1 = b"GET /one?q=a b HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mut prefix = BytesMut::with_capacity(req1.len() + req2.len());
    prefix.extend_from_slice(req1);
    prefix.extend_from_slice(req2);
    let req2_ptr = unsafe { prefix.as_ptr().add(req1.len()) };

    let mock_io = Builder::new().build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.set_pipelining_enabled(true);
    session.set_pipelined_prefix(prefix);
    session.read_request().await.unwrap();
    assert_eq!(session.get_path(), b"/one?q=a%20b");

    let reused = session.reuse().await.unwrap().expect("connection reusable");
    let (_, prefix) = reused.into_parts();
    let prefix = prefix.expect("second request remains buffered");
    assert_eq!(prefix.as_ptr(), req2_ptr);
    assert_eq!(prefix.as_ref(), req2);
}

/// `do_read_chunked_body` splits the post-last-chunk tail to the front of the body buffer,
/// and `do_read_chunked_body_final` may then need several reads to find the end of the
/// trailer section. Only the first of those passes may reuse the split buffer; the later
/// ones reset it for fresh IO. This drives both passes with a pipelined request behind the
/// trailers to prove neither the trailers nor the queued request are lost.
#[tokio::test]
async fn chunked_trailers_across_reads_keep_pipelined_request() {
    init_log();
    let req1_head =
        b"POST /one HTTP/1.1\r\nHost: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n";
    // Ends mid trailer section: the final CRLF that terminates it is still on the wire.
    let req1_tail = b"1\r\na\r\n0\r\nX-Test: one\r\n";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mut rest = BytesMut::from(&b"\r\n"[..]);
    rest.extend_from_slice(req2);

    let mut prefix = BytesMut::with_capacity(req1_head.len() + req1_tail.len());
    prefix.extend_from_slice(req1_head);
    prefix.extend_from_slice(req1_tail);

    let mock_io = Builder::new().read(&rest).build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.set_pipelining_enabled(true);
    session.set_pipelined_prefix(prefix);
    session.read_request().await.unwrap();
    assert_eq!(session.req_header().uri.path(), "/one");

    let mut body = BytesMut::new();
    while let Some(chunk) = session.read_body_bytes().await.unwrap() {
        body.extend_from_slice(&chunk);
    }
    assert_eq!(body.as_ref(), b"a");

    let reused = session.reuse().await.unwrap().expect("connection reusable");
    let (stream, prefix) = reused.into_parts();
    let prefix = prefix.expect("second request remains buffered");
    assert_eq!(prefix.as_ref(), req2);

    let mut next = HttpSession::new(stream);
    next.set_pipelining_enabled(true);
    next.set_pipelined_prefix(prefix);
    next.read_request()
        .await
        .unwrap()
        .expect("pipelined request must parse");
    assert_eq!(next.req_header().uri.path(), "/two");
}

/// The URI-escape path detaches everything past the first CRLFCRLF, but httparse also
/// accepts bare LF line endings, so the real header can end earlier than that. The detached
/// suffix must be stitched back behind the leftover bytes, otherwise the queued request is
/// silently dropped (or, worse, mis-attributed) instead of being served.
#[tokio::test]
async fn escaped_uri_with_bare_lf_keeps_all_pipelined_bytes() {
    init_log();
    let req1 = b"GET /one?q=a b HTTP/1.1\nHost: pingora.org\n\n";
    let req2 = b"GET /two HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mut prefix = BytesMut::with_capacity(req1.len() + req2.len());
    prefix.extend_from_slice(req1);
    prefix.extend_from_slice(req2);

    let mock_io = Builder::new().build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.set_pipelining_enabled(true);
    session.set_pipelined_prefix(prefix);
    session.read_request().await.unwrap();
    assert_eq!(session.get_path(), b"/one?q=a%20b");

    let reused = session.reuse().await.unwrap().expect("connection reusable");
    let (stream, prefix) = reused.into_parts();
    let prefix = prefix.expect("second request remains buffered");
    assert_eq!(prefix.as_ref(), req2);

    let mut next = HttpSession::new(stream);
    next.set_pipelining_enabled(true);
    next.set_pipelined_prefix(prefix);
    next.read_request()
        .await
        .unwrap()
        .expect("pipelined request must parse");
    assert_eq!(next.req_header().uri.path(), "/two");
}

#[tokio::test]
async fn long_bodyless_pipeline_keeps_one_allocation() {
    init_log();
    const REQUESTS: usize = 256;
    let requests: Vec<_> = (0..REQUESTS)
        .map(|i| format!("GET /{i} HTTP/1.1\r\nHost: pingora.org\r\n\r\n"))
        .collect();
    let mut prefix = BytesMut::with_capacity(requests.iter().map(String::len).sum());
    for request in &requests {
        prefix.extend_from_slice(request.as_bytes());
    }
    let mut expected_ptr = prefix.as_ptr();
    let mock_io = Builder::new().build();
    let mut stream: Stream = Box::new(mock_io);

    for (i, request) in requests.iter().enumerate() {
        assert_eq!(prefix.as_ptr(), expected_ptr);
        let mut session = HttpSession::new(stream);
        session.set_pipelining_enabled(true);
        session.set_pipelined_prefix(prefix);
        session.read_request().await.unwrap();
        assert_eq!(session.req_header().uri.path(), format!("/{i}"));

        let reused = session.reuse().await.unwrap().expect("connection reusable");
        let (next_stream, next_prefix) = reused.into_parts();
        stream = next_stream;
        expected_ptr = unsafe { expected_ptr.add(request.len()) };
        prefix = next_prefix.unwrap_or_default();
    }

    assert!(prefix.is_empty());
}
