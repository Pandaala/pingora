//! Shared scenario bodies, each run once per transport combination.
//!
//! House rules for a body in this module:
//! - it takes a [`Combo`] and reads EVERY transport decision off it, so the
//!   same source line is what runs in all four cells;
//! - where the two transports manifest the same contract differently it
//!   BRANCHES (`match combo.up { .. }`) on the manifestation. It never weakens
//!   to the intersection of what both transports can show -- an assertion that
//!   holds on both by saying less is not the same test;
//! - a cell that genuinely cannot express the shape says so with
//!   `skip_combo!`, never by quietly passing.

use super::harness::*;
use super::{Combo, Down, Step, Up};
use crate::skip_combo;
use bytes::Bytes;
use h2::Reason;
use pingora_proxy::RequestBodyEvent;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;

/// The `x-h2` request header line (H1 wire form) this combination needs, or the
/// empty string. `SeamProxy::upstream_peer` keys the upstream protocol -- and
/// therefore which pump runs -- off it.
fn up_header_line(combo: Combo) -> &'static str {
    if combo.upstream_is_h2() {
        "x-h2: 1\r\n"
    } else {
        ""
    }
}

/// How long a "nothing else happened" window runs for.
const QUIET_WINDOW: Duration = Duration::from_millis(300);
/// How long to let the proxy's h2 connection task notice a frame the origin
/// has just queued (a GOAWAY), or hand a finished connection back to its pool.
///
/// An ordering aid for claims that only make sense afterwards; never itself a
/// claim. Every use is placed so that losing the race fails the test.
const GOAWAY_SETTLE: Duration = Duration::from_millis(200);
const WAIT: Duration = Duration::from_secs(10);

/// The cell's manifestation of "the proxy gave up on the upstream leg".
///
/// H1 has no per-request signal at all: abandoning a request means abandoning
/// the connection, so the observable is the connection going away. H2 resets
/// the one stream and keeps the connection. Asserting only the part they share
/// ("no second attempt") would be a strictly weaker test in both cells.
async fn expect_upstream_cancelled(combo: Combo, rec: &Recorder) {
    match combo.up {
        Up::H1 => expect_ok(
            rec.wait_for("the upstream connection to be closed", WAIT, |e| {
                matches!(e, UpEvent::ConnClosed { .. })
            })
            .await,
        ),
        Up::H2 => expect_ok(
            rec.wait_for(
                "RST_STREAM(CANCEL) on the upstream request stream",
                WAIT,
                |e| matches!(e, UpEvent::PeerReset { code, .. } if *code == Reason::CANCEL),
            )
            .await,
        ),
    };
}

/// A terminate must complete promptly and must not be dressed up as a proxy
/// error.
///
/// Merged from `h1_terminate_is_prompt_and_skips_generic_errors` and
/// `h2_upstream_terminate_resets_stream`, which asserted the same three things
/// (a prompt local 403, no generic 5xx behind it, exactly one upstream attempt)
/// about the two upstream pumps, plus one cell-specific cancellation
/// observable each. The upstream never answers, so nothing here can pass by the
/// request having completed normally.
///
/// Promptness needs both bounds. The lower is the recorded `ReqHeaders`
/// instant -- the upstream demonstrably held the request, without which the
/// upper bound cannot fail. The upper is "the pump FINISHED within
/// `TERMINATE_BUDGET` of it", and how "finished" is observed differs per
/// downstream:
/// - H1: time-to-EOF on the downstream connection, a wire fact -- the
///   connection can only close once the pump finished; see
///   `terminate_reply_and_eof`.
/// - H2c: the connection deliberately stays open across a terminate (its
///   contract -- see `single::h2c_downstream_terminate_keeps_connection`), and
///   the application flushes its complete 403 from inside the hook, so no
///   wire observable can distinguish a finished pump from one still parked on
///   the hung upstream. The per-request completion observation takes its
///   place: `ProxyHttp::logging` only runs once the proxy finished the
///   request, and `x-observe-completion` attributes that fact to this one
///   stream even on a shared connection.
pub fn terminate_is_prompt_and_cancels_the_upstream(combo: Combo) {
    let (port, rec, _upstream) = combo.spawn(&[Step::HangObservingCancel]);

    match combo.down {
        Down::H1 => {
            let up = up_header_line(combo);
            let text = RT.block_on(async {
                let request = format!(
                    "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n{up}\
                     x-terminate-after-bytes: 1\r\n\
                     Transfer-Encoding: chunked\r\n\r\n\
                     5\r\nhello\r\n"
                );
                // Asserts both promptness bounds; see `terminate_reply_and_eof`.
                let text = terminate_reply_and_eof(&combo.down_addr(), &request, &rec).await;

                expect_upstream_cancelled(combo, &rec).await;

                // "No second attempt" as an absence over a bounded window. A
                // `counter <= 1` form is also satisfied by a counter that never
                // moved, i.e. by the request never reaching the upstream at all.
                expect_ok(
                    rec.expect_none("a second upstream attempt", QUIET_WINDOW, |e| {
                        matches!(e, UpEvent::ReqHeaders { .. })
                    })
                    .await,
                );
                text
            });

            assert!(
                text.starts_with("HTTP/1.1 403"),
                "local reply expected: {text}"
            );
            assert!(
                !text.contains("502") && !text.contains("500"),
                "terminate must not produce a generic proxy error response: {text}"
            );
        }
        Down::H2c => RT.block_on(async {
            let (id, completion) = observe_completion();
            let tcp = tokio::net::TcpStream::connect(combo.down_addr())
                .await
                .unwrap();
            let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            let mut builder = http::Request::builder()
                .method("POST")
                .uri("http://t/")
                .header("x-port", port.to_string())
                .header("x-terminate-after-bytes", "1")
                .header("x-observe-completion", id.into_header());
            if combo.upstream_is_h2() {
                builder = builder.header("x-h2", "1");
            }
            // HEADERS without END_STREAM, then one DATA frame with the stream
            // held open for the rest of this block: the same mid-upload shape
            // as the H1 arm's unfinished chunked body.
            let (response, mut request_body) =
                h2.send_request(builder.body(()).unwrap(), false).unwrap();
            request_body
                .send_data(Bytes::from_static(b"hello"), false)
                .unwrap();

            // Lower bound: the upstream really did receive this request, so
            // everything measured from here is measured against a hung
            // upstream that exists.
            expect_ok(
                rec.wait_for(
                    "the upstream to receive the request headers",
                    Duration::from_secs(10),
                    |e| matches!(e, UpEvent::ReqHeaders { .. }),
                )
                .await,
            );
            let upstream_had_it = rec
                .first_seen(|e| matches!(e, UpEvent::ReqHeaders { .. }))
                .expect("the wait above returned, so the event is recorded");

            // On H2 a stream carries exactly one response, so "no generic 5xx
            // behind the local reply" is structural; the status IS the claim.
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .expect("the local reply must reach the client")
                .unwrap();
            assert_eq!(response.status(), 403, "local reply expected");
            // Drain the local reply until the stream is over. Two wire shapes
            // both mean that: END_STREAM on the last DATA frame, or the
            // RST_STREAM(NO_ERROR) the proxy sends because the client's
            // request half is still open -- and h2 may surface the latter as
            // a read error that discards buffered DATA, since the reset lands
            // while the client is still uploading (RFC 9113 section 8.1's own
            // premise). Any OTHER reset code would be the terminate dressed
            // up as a proxy error, which is exactly what this scenario
            // forbids.
            let mut body = response.into_body();
            loop {
                match tokio::time::timeout(Duration::from_secs(10), body.data())
                    .await
                    .expect("the local reply stream must end")
                {
                    Some(Ok(chunk)) => {
                        let _ = body.flow_control().release_capacity(chunk.len());
                    }
                    Some(Err(e)) => {
                        assert_eq!(
                            e.reason(),
                            Some(Reason::NO_ERROR),
                            "the terminated stream must end cleanly, not with a \
                             proxy-error reset: {e:?}"
                        );
                        break;
                    }
                    None => break,
                }
            }

            // Upper bound: the pump must actually FINISH -- release the
            // request rather than stay parked on the hung upstream -- within
            // the budget. `finished_at` is stamped proxy-side in `logging`,
            // so the measurement is immune to this test's own scheduling.
            // The wait is derived from the budget (see `TERMINATE_WAIT`): a
            // 10s wait here would mean the budget only ever caught a
            // regression that finished between 2s and 10s, and anything slower
            // failed on the wait's message instead of on the claim.
            let record = completion
                .wait(
                    TERMINATE_WAIT,
                    "the terminate never finished: the pump is still waiting on \
                     the hung upstream instead of cancelling it",
                )
                .await;
            let elapsed = record.finished_at.duration_since(upstream_had_it);
            assert!(
                elapsed < TERMINATE_BUDGET,
                "the terminate took {elapsed:?} to finish after the upstream had \
                 the request, which is longer than the {TERMINATE_BUDGET:?} \
                 budget: it is waiting on the upstream rather than cancelling \
                 it.\n{}",
                rec.dump()
            );

            expect_upstream_cancelled(combo, &rec).await;

            // "No second attempt" as an absence over a bounded window (see the
            // H1 arm).
            expect_ok(
                rec.expect_none("a second upstream attempt", QUIET_WINDOW, |e| {
                    matches!(e, UpEvent::ReqHeaders { .. })
                })
                .await,
            );
        }),
    }

    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        1,
        "exactly one upstream attempt:\n{}",
        rec.dump()
    );
    assert_eq!(
        rec.connections(),
        1,
        "exactly one upstream connection:\n{}",
        rec.dump()
    );
}

/// A downstream H1 connection whose request was terminated must not be reused.
///
/// `trigger` is the request header that makes the application terminate, and
/// `status` the local reply it writes. The two callers differ only in WHEN the
/// terminate happens, which is the whole point -- see their doc comments.
fn terminate_is_not_reused(combo: Combo, trigger: &str, status: &str, body: &str) {
    let (port, rec, _upstream) = combo.spawn(&[Step::HangObservingCancel]);
    // The follow-up request targets its OWN, working upstream, so it being
    // answered would be entirely the proxy's doing -- and that upstream must
    // never be dialled at all, which is why the vacuity guard is waived for it
    // explicitly rather than by accident.
    let (follow_port, follow_rec, _follow) = combo.spawn_unused_h1();
    let up = up_header_line(combo);

    RT.block_on(async {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n{up}\
             {trigger}\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             {body}"
        );
        let (mut stream, _collected) =
            raw_h1_roundtrip(&combo.down_addr(), request.as_bytes(), status.as_bytes()).await;

        // The proxy must have marked this downstream connection non-reusable.
        // A pipelined second request must not be answered: expect a clean EOF
        // (or an error), never a response.
        let second = format!("GET / HTTP/1.1\r\nHost: t\r\nx-port: {follow_port}\r\n\r\n");
        let _ = stream.write_all(second.as_bytes()).await;
        let mut buf = vec![0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
            Ok(Ok(0)) => {}  // clean close: correct
            Ok(Err(_)) => {} // reset: also acceptable
            Ok(Ok(n)) => panic!(
                "terminated H1 downstream connection served a second request: {:?}",
                String::from_utf8_lossy(&buf[..n])
            ),
            Err(_) => panic!("connection neither closed nor errored within 5s"),
        }

        // The upstream-side half. The first request really did reach the
        // upstream (so the terminate under test happened at all)...
        expect_ok(
            rec.wait_for(
                "the upstream to receive the terminated request",
                WAIT,
                |e| matches!(e, UpEvent::ReqHeaders { .. }),
            )
            .await,
        );
        // ... and the follow-up was never relayed anywhere. Downstream EOF
        // alone does not say that: the proxy could have forwarded the request
        // and only then dropped the client.
        expect_ok(
            follow_rec
                .expect_none(
                    "the follow-up request reaching an upstream",
                    QUIET_WINDOW,
                    |e| matches!(e, UpEvent::ConnAccepted { .. }),
                )
                .await,
        );
    });

    assert_eq!(
        rec.connections(),
        1,
        "exactly one upstream connection for the terminated request:\n{}",
        rec.dump()
    );
    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        1,
        "exactly one upstream request:\n{}",
        rec.dump()
    );
    assert_eq!(
        follow_rec.connections(),
        0,
        "the follow-up request's upstream must never have been dialled:\n{}",
        follow_rec.dump()
    );
}

/// Terminate MID-BODY: the application refused the request body, so keepalive
/// reuse would first have to drain bytes it deliberately did not read.
///
/// Merged from `h1_terminated_connection_is_not_reused`, which only ever ran
/// against an H1 upstream; the H1-downstream -> H2-upstream cell is new.
pub fn mid_body_terminate_is_not_reused(combo: Combo) {
    if combo.down == Down::H2c {
        skip_combo!(
            combo,
            "'the connection is not reused' is an H1 keepalive claim; an H2 \
             downstream is REQUIRED to survive a terminate on one stream -- see \
             single::h2c_downstream_terminate_keeps_connection"
        );
    }
    terminate_is_not_reused(
        combo,
        "x-terminate-after-bytes: 1",
        "denied",
        "5\r\nhello\r\n",
    );
}

/// Terminate at the TRAILER point, i.e. AFTER the downstream body reached EOF.
///
/// That is what makes this discriminating against
/// [`mid_body_terminate_is_not_reused`]: a mid-body terminate is already
/// covered by the H1 session's default
/// `close_on_response_before_downstream_finish`, which clears keepalive
/// whenever a response is written before the body is done. At the trailer point
/// that safety net does not fire, so the only thing standing between the client
/// and a reused connection is the pump's own reuse verdict.
///
/// Merged from `h1_trailer_bearing_request_can_be_rejected` (H1 upstream) and
/// `h1_downstream_h2_upstream_terminate_is_not_reused` (H2 upstream). The
/// H2-upstream version's stronger form -- a separate, working upstream for the
/// follow-up request, asserted never to have been dialled -- is what both cells
/// now use.
pub fn trailer_terminate_is_not_reused(combo: Combo) {
    if combo.down == Down::H2c {
        skip_combo!(
            combo,
            "'the connection is not reused' is an H1 keepalive claim; an H2 \
             downstream is REQUIRED to survive a terminate on one stream -- see \
             single::h2c_downstream_terminate_keeps_connection"
        );
    }
    terminate_is_not_reused(
        combo,
        "x-reject-trailers: 1",
        "HTTP/1.1 400",
        "5\r\nhello\r\n0\r\nx-checksum: ok\r\n\r\n",
    );
}

/// A terminate must not leave the application's local reply unfinished.
///
/// The application writes a chunked 403 header and its body with
/// `end_of_stream = false`, then terminates. The terminate arms return before
/// the pump's `finish_body()`, and `HttpProxy::finish` skips
/// `downstream_session.finish()` because a terminated request never reports
/// reuse -- so without a defensive `finish_body()` the terminating `0\r\n\r\n`
/// chunk (H1) / END_STREAM (H2) is never written, and the client cannot tell
/// the response from a connection that died mid-body.
/// `warn_terminate_without_response` stays silent about it because a response
/// header WAS written.
///
/// `reqwest` is the client here on purpose: it validates the response framing,
/// so `text()` fails on the truncated form and succeeds on the complete one.
/// (On an H2 downstream pingora strips the `transfer-encoding` header and the
/// same unfinished body shows up as a missing END_STREAM.)
///
/// Merged from `terminate_finishes_an_unfinished_local_reply` and
/// `terminate_finishes_an_unfinished_local_reply_h2_upstream`; both H2c
/// downstream cells are new.
pub fn terminate_finishes_an_unfinished_local_reply(combo: Combo) {
    let (port, _rec, _upstream) = combo.spawn(&[Step::HangObservingCancel]);

    RT.block_on(async {
        let client = combo.client();
        let mut req = client
            .post(format!("http://{}/", combo.down_addr()))
            .header("x-port", port.to_string())
            .header("x-terminate-unflushed", "1")
            .body("hello");
        if combo.upstream_is_h2() {
            req = req.header("x-h2", "1");
        }
        let res = tokio::time::timeout(Duration::from_secs(10), req.send())
            .await
            .expect("the application's local reply must reach the client")
            .unwrap();
        assert_eq!(res.status(), 403);
        let body = tokio::time::timeout(Duration::from_secs(10), res.text())
            .await
            .expect("reading the local reply body must not hang")
            .expect("the local reply body must be completely framed");
        assert_eq!(body, "denied");
    });
}

/// `Bodyless` contradicted by a REAL downstream body must fail closed.
///
/// `Bodyless` is a guarantee that no upstream request body will follow, and the
/// pump acts on it irreversibly before reading any: the upstream request loses
/// both `Content-Length` and `Transfer-Encoding` (H1) / is closed by END_STREAM
/// on HEADERS (H2), so every body byte the pump then reads could only be
/// dropped. Forwarding the request anyway would have the upstream act on a
/// body-less `POST` while the client is told it succeeded, so the proxy fails
/// the request closed instead: no request body bytes on the upstream wire, and
/// a 500 to the client, with no hang.
///
/// The body is real on purpose: a bodyless request would be coerced back to
/// `Ordinary` by `safe_disposition`, so `Bodyless` would not be exercised at
/// all.
///
/// Merged from `bodyless_with_a_real_body_h1_upstream_fails_closed` and
/// `bodyless_with_a_real_body_h2_upstream_fails_closed`. The H1-upstream half
/// used a raw byte capture to say "no body bytes"; it now says it with recorded
/// events, via `UpstreamStep::RespondThenRecordExtra` -- which records
/// unframed trailing bytes precisely because a bodyless upstream request has no
/// body framing to parse them with.
pub fn bodyless_with_a_real_body_fails_closed(combo: Combo) {
    let (port, rec, _upstream) = combo.spawn(&[Step::Ok200ThenRecordExtra]);
    let up = up_header_line(combo);

    RT.block_on(async {
        match combo.down {
            // Raw H1, so that "exactly ONE downstream response" is assertable:
            // `read_one_h1_response` consumes the 500 in full, and anything
            // left over would be a second response written after the error was
            // already answered.
            Down::H1 => {
                let mut stream = tokio::net::TcpStream::connect(combo.down_addr())
                    .await
                    .unwrap();
                let mut pending = Vec::new();
                let request = format!(
                    "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n{up}\
                     x-disposition: bodyless\r\n\
                     Transfer-Encoding: chunked\r\n\r\n\
                     5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n"
                );
                // A COMPLETE response: the error path must produce a
                // well-framed response, not a bare connection close, and
                // `read_one_h1_response` fails the test on a hang (10s
                // deadline) or a truncated one.
                let response = h1_request_response(&mut stream, &mut pending, &request)
                    .await
                    .to_lowercase();
                assert!(
                    response.starts_with("http/1.1 500"),
                    "a Bodyless declaration contradicted by a real request body must \
                     fail the request closed with a 500: {response}"
                );
                assert!(
                    !response.contains("x-eos-events"),
                    "the upstream response filter must never have run: {response}"
                );
                assert!(
                    pending.is_empty(),
                    "the fail-closed error must produce exactly one downstream \
                     response: {pending:?}"
                );
            }
            // h2 frames the response for us, so "exactly one response" is not a
            // claim that can fail here; the status and the absent
            // `x-eos-events` are.
            Down::H2c => {
                let client = combo.client();
                let mut req = client
                    .post(format!("http://{}/", combo.down_addr()))
                    .header("x-port", port.to_string())
                    .header("x-disposition", "bodyless")
                    .body("helloworld");
                if combo.upstream_is_h2() {
                    req = req.header("x-h2", "1");
                }
                let res = tokio::time::timeout(Duration::from_secs(10), req.send())
                    .await
                    .expect("the fail-closed error must reach the client")
                    .unwrap();
                assert_eq!(
                    res.status(),
                    500,
                    "a Bodyless declaration contradicted by a real request body must \
                     fail the request closed with a 500"
                );
                assert!(
                    res.headers().get("x-eos-events").is_none(),
                    "the upstream response filter must never have run: {:?}",
                    res.headers()
                );
            }
        }

        expect_ok(
            rec.wait_for("the upstream to receive the bodyless request", WAIT, |e| {
                matches!(e, UpEvent::ReqHeaders { .. })
            })
            .await,
        );
        expect_ok(
            rec.expect_none(
                "request body bytes on the upstream stream",
                QUIET_WINDOW,
                |e| matches!(e, UpEvent::ReqData { len, .. } if *len > 0),
            )
            .await,
        );
        // Failing closed must not leave the origin holding a connection the
        // proxy is done with. H2 says that as a per-stream fact; H1 has no
        // per-request signal, so its manifestation is the proxy ENDING the
        // connection -- observed as a FIN or an RST during the upstream's
        // 300ms recording window. Which of the two arrives is an
        // implementation accident, not the contract (it depends on whether the
        // pump drained the upstream's early 200 before the teardown closed the
        // socket), so pinning one wire form would assert the accident. Both
        // forms really do occur, and neither dominates: over 20 isolated runs
        // each, `h1_to_h1` gave 15 FIN / 5 RST and `h2c_to_h1` gave 19 FIN /
        // 1 RST on this machine. (A reviewer measuring the same cell reported
        // 16 RST / 4 FIN, i.e. the opposite ratio -- which is the point: the
        // wire form is a scheduling accident, and a wait for only one of them
        // would fail somewhere between 5% and 80% of runs.)
        //
        // Do NOT read the H1 wait as pinning the reuse VERDICT. This comment
        // used to claim that "a proxy that pooled the connection for reuse
        // leaves the window to expire silently", i.e. that the wait
        // discriminates the verdict. Disproved by mutating a scratch copy:
        // - reporting the failed upstream leg as reusable
        //   (`proxy_h1.rs:245` -> `(false, true, Some(e))`): teardown still
        //   observed, test still green;
        // - plus `HttpSession::reuse()` handing the stream back
        //   unconditionally, plus `test_reusable_stream()` returning `true`:
        //   STILL a teardown (RST in both H1 cells), because the connection
        //   carries the upstream's unread early 200 and `ConnectionPool::
        //   idle_poll` closes any pooled connection that has data readable on
        //   it ("Data received on idle client connection, close it").
        // The teardown is therefore over-determined by four independent
        // layers, and no single-condition mutation can flip it. What the wait
        // pins is the weaker, still-real claim it is worded as: the proxy does
        // not leave this connection open and idle. The reuse verdict itself
        // has no observable at this seam -- reaching it would need an upstream
        // that stays silent, which this scenario cannot have, since its early
        // 200 is what makes the "no body bytes on the wire" claim meaningful.
        match combo.up {
            Up::H1 => {
                expect_ok(
                    rec.wait_for(
                        "the proxy to end the upstream connection (FIN or RST)",
                        WAIT,
                        |e| {
                            matches!(
                                e,
                                UpEvent::PeerHalfClose { .. } | UpEvent::PeerConnReset { .. }
                            )
                        },
                    )
                    .await,
                );
            }
            Up::H2 => {
                // The pump explicitly asks h2 to reset the stream on the
                // fail-closed path. Current h2 releases may suppress that
                // frame once END_STREAM already rode on the request HEADERS;
                // older releases emitted the RST_STREAM. Either observation
                // proves the upstream request side was closed and cannot
                // receive the client body that violated the Bodyless contract.
                expect_ok(
                    rec.wait_for("a closed upstream request stream", WAIT, |e| {
                        matches!(
                            e,
                            UpEvent::PeerReset { .. }
                                | UpEvent::ReqHeaders {
                                    headers_eos: true,
                                    ..
                                }
                        )
                    })
                    .await,
                );
            }
        }
    });

    // The declaration the pump acted on, at the upstream.
    let headers = expect_ok(
        RT.block_on(rec.wait_for("the upstream request headers", WAIT, |e| {
            matches!(e, UpEvent::ReqHeaders { .. })
        })),
    );
    let UpEvent::ReqHeaders {
        content_length,
        transfer_encoding,
        ..
    } = headers
    else {
        unreachable!("the predicate above only matches ReqHeaders")
    };
    assert_eq!(
        content_length,
        None,
        "bodyless must not declare a content length upstream:\n{}",
        rec.dump()
    );
    assert_eq!(
        transfer_encoding,
        None,
        "bodyless must not declare a transfer encoding upstream:\n{}",
        rec.dump()
    );
    // `expect_none` only watches from the moment it is called, so it alone
    // would miss body frames that had already landed. The exact count over the
    // WHOLE log is what makes "no request body bytes, ever" the assertion.
    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqData { len, .. } if *len > 0)),
        0,
        "bodyless must not put any request body bytes on the upstream stream:\n{}",
        rec.dump()
    );
}

/// A downstream request that declares `Content-Length: 0` but has NOT ended its
/// request stream is not bodyless (design 4.3): the tightened `is_body_done()`
/// reports the transport fact, while `is_body_empty()` still infers emptiness
/// from the header. Each pump must send exactly one upstream end-of-stream and
/// deliver exactly one end-of-stream event to the application.
///
/// The H1-upstream leg's bodyless prelude used to fire an immediate
/// `(None, end)` event while the duplex loop fired a second one on the client's
/// real EOS, so `request_body_filter` saw TWO. On the H2-upstream leg the
/// second, standalone END_STREAM that would have followed (an h2 `UserError`
/// costing the downstream its reusability) is suppressed by
/// `upstream_body_closed`.
///
/// Merged from `h2_cl0_without_end_stream_h1_upstream_sends_one_eos` and
/// `h2_cl0_without_end_stream_h2_upstream_sends_one_eos`. Both legs now assert
/// the UPSTREAM framing off recorded events rather than only the H2 leg
/// asserting it off an echoed header.
pub fn cl0_without_end_stream_sends_one_eos(combo: Combo) {
    if combo.down == Down::H1 {
        skip_combo!(
            combo,
            "HTTP/1.1 has no separate transport end-of-stream: `Content-Length: 0` \
             IS the end of the request body, so a downstream that declares it and \
             has not finished is not a shape this transport can express"
        );
    }
    let (port, rec, _upstream) = combo.spawn(&[Step::DrainThenOk200]);

    RT.block_on(async {
        let (status, eos_events, _echoed, record) =
            h2_cl0_no_end_stream_request(port, combo.upstream_is_h2()).await;
        assert_eq!(status, 200);
        assert_eq!(
            eos_events, "1",
            "the application must see exactly one end-of-stream event"
        );
        // The NEGATIVE half of the same claim. This client ends its request
        // stream with a real END_STREAM DATA frame, so the one terminal event it
        // earns must be `Complete`. `x-eos-events` above cannot say that:
        // `is_terminal()` is true for `Abandoned` too, so a pump that labelled
        // this normal end-of-stream as "the proxy gave up reading" would keep
        // that header at `1` -- while every application built on the
        // distinction (a mirror that cancels, a digest that is discarded, a
        // protobuf `end_of_stream` flag that is never set) silently breaks.
        assert_eq!(
            record.abandoned_events, 0,
            "a downstream body that really ended must never be reported as abandoned"
        );
        assert_eq!(
            record.eos_events, 1,
            "the final count, after the pump released the request, must still be one \
             terminal event"
        );
        assert_eq!(
            record.events,
            vec![RequestBodyEvent::Complete],
            "a `Content-Length: 0` request that ends with an empty END_STREAM DATA \
             frame owes the application exactly one event, and it is the completion"
        );
        record.assert_hooks_agree("a Content-Length: 0 request ended by a real END_STREAM");

        // The upstream framing follows the request's own DECLARATION, so
        // `Content-Length: 0` closes the upstream request stream at header
        // time. An origin that does not answer until it has seen the end of the
        // request would otherwise deadlock, and the futile-read rule cannot
        // rescue it because that rule needs a complete response first (see
        // `single::h2_cl0_never_ending_request_reaches_an_upstream_that_waits_for_eos`).
        let headers = expect_ok(
            rec.wait_for("the upstream request headers", WAIT, |e| {
                matches!(e, UpEvent::ReqHeaders { .. })
            })
            .await,
        );
        let UpEvent::ReqHeaders { headers_eos, .. } = headers else {
            unreachable!("the predicate above only matches ReqHeaders")
        };
        assert!(
            headers_eos,
            "the `Content-Length: 0` declaration must close the upstream request \
             stream at header time:\n{}",
            rec.dump()
        );
        expect_ok(
            rec.expect_none(
                "request body bytes on the upstream stream",
                QUIET_WINDOW,
                |e| matches!(e, UpEvent::ReqData { len, .. } if *len > 0),
            )
            .await,
        );
    });

    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        1,
        "exactly one upstream request:\n{}",
        rec.dump()
    );
    assert_eq!(
        rec.body_bytes(),
        0,
        "a `Content-Length: 0` request has no body bytes to forward:\n{}",
        rec.dump()
    );
}

/// A graceful GOAWAY (`NO_ERROR`) from a busy upstream connection: the
/// in-flight stream must still complete to the client, and the PROXY -- not
/// the origin's socket teardown -- must stop putting new requests on that
/// connection.
///
/// The origin applies the GOAWAY BEFORE queueing its response (see
/// [`H2UpstreamStep::GoawayThenHoldThenOk200`]), so the proxy provably learns
/// the connection is draining while its stream is in flight -- RFC 9113's
/// "streams at or below last-stream-id still complete" case.
///
/// Then it HOLDS that stream open, and everything this test claims about
/// reuse is claimed while it is held. That is the whole design of the case,
/// and it is a correction of an earlier version that let the origin's
/// `graceful_shutdown` run to completion and asserted the same connection
/// counts. `graceful_shutdown` closes the socket as soon as the last stream
/// finishes, so the follow-up request there had to dial again NO MATTER WHAT
/// THE PROXY DECIDED. Measured during review of that version: the recorded
/// timeline was `ConnClosed{conn:0}` 0.2ms before `ConnAccepted{conn:1}`, and
/// two mutations -- deleting `!self.is_shutting_down()` from
/// `ConnectionRef::more_streams_allowed`, and deleting the
/// `GOAWAY(NO_ERROR) -> shutting_down` branch of `ConnectionRef::spawn_stream`
/// -- each left it green, i.e. the reuse half of its name was decoration.
///
/// With the stream held, all three requests below run against a connection
/// the origin has NOT closed and h2 has NOT torn down, so the proxy's own
/// gates are what is being observed. Both mutations above were re-run against
/// THIS version in a scratch copy, and both now fail it:
/// - the follow-up request reaches `spawn_stream` on the pooled, GOAWAY'd
///   connection, where `new_stream()` fails with
///   `GoAway(NO_ERROR, Remote)`. The fork turns that into `Ok(None)` -- "no
///   stream here, dial a fresh connection" -- instead of an error, so the
///   request is served on conn 1 with `x-no-retry` set, i.e. with the proxy's
///   retry loop unable to paper over it. With that branch disabled the
///   follow-up is a 502 (observed, both H2 cells).
/// - the THIRD request is what pins the pool hygiene half. On the correct
///   code conn 0 is not returned to the in-use pool (it is shutting down), so
///   the third request finds conn 1 and reuses it: two connections total.
///   Without the `!self.is_shutting_down()` guard, conn 0 goes back into the
///   pool ahead of conn 1 and the third request picks it up again, gets
///   `Ok(None)` a second time, and dials a THIRD connection (observed: the
///   connection-count assertion failing 3 != 2, both H2 cells).
///
/// Not pinned here: the same guard in `Connector::release_http_session`
/// (`conn.is_closed() || conn.is_shutting_down()`). It only acts when the
/// in-flight stream is finally released, and by then h2 has torn the GOAWAY'd
/// connection down anyway, so the idle pool would evict it in a race this
/// test cannot make deterministic.
///
/// The in-flight half of the claim (a stream at or below last-stream-id still
/// completes) is asserted at the end, and is NOT fork behavior: it is h2's
/// own `recv_go_away`, which only errors streams above the GOAWAY's
/// last-stream-id. There is no fork code to mutate for it; it is asserted
/// because the rest of the test would otherwise be free to break it.
pub fn upstream_graceful_goaway_finishes_in_flight_and_is_not_reused(combo: Combo) {
    if combo.up == Up::H1 {
        skip_combo!(
            combo,
            "GOAWAY is an HTTP/2 connection frame; an H1 upstream has no wire \
             form for 'finish what is in flight, then never use this connection \
             again'"
        );
    }
    let applied = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let upstream = spawn_scripted_h2_upstream(vec![
        H2UpstreamStep::GoawayThenHoldThenOk200 {
            applied: applied.clone(),
            release: release.clone(),
        },
        H2UpstreamStep::Ok200,
        H2UpstreamStep::Ok200,
    ]);
    let (port, rec) = (upstream.port(), upstream.rec().clone());

    RT.block_on(async {
        let client = combo.client();
        let addr = combo.down_addr();

        // The in-flight request, deliberately not awaited yet: its response is
        // held by the origin for the rest of this block, which is what keeps
        // the GOAWAY'd connection open and the proxy's pooled connection
        // alive.
        let in_flight = {
            let (client, addr) = (client.clone(), addr.clone());
            tokio::spawn(async move {
                client
                    .get(format!("http://{addr}/"))
                    .header("x-port", port.to_string())
                    .header("x-h2", "1")
                    .header("x-max-h2-streams", "2")
                    .send()
                    .await
            })
        };

        expect_ok(
            rec.wait_for("the upstream to receive the in-flight request", WAIT, |e| {
                matches!(e, UpEvent::ReqHeaders { conn: 0, .. })
            })
            .await,
        );
        tokio::time::timeout(WAIT, applied.notified())
            .await
            .expect("the origin must put its graceful GOAWAY on the connection");
        // The GOAWAY is queued ahead of anything the origin writes later; this
        // is the moment the proxy's h2 connection task needs in order to READ
        // it. It is an ordering aid, not a claim: a follow-up request that beat
        // the GOAWAY would be multiplexed onto conn 0 (the peer's
        // MAX_CONCURRENT_STREAMS allows it) and fail the counts below, never
        // pass wrongly.
        tokio::time::sleep(GOAWAY_SETTLE).await;

        for attempt in 0..2u32 {
            if attempt > 0 {
                // Let the proxy hand the fresh connection back to its pool, so
                // the second follow-up can reuse it. Same shape as above: a
                // connection that is not back in the pool yet costs a THIRD
                // connection and fails, it cannot pass wrongly.
                tokio::time::sleep(GOAWAY_SETTLE).await;
            }
            let req = client
                .get(format!("http://{addr}/"))
                .header("x-port", port.to_string())
                .header("x-h2", "1")
                .header("x-max-h2-streams", "2")
                // The redial the fork's GOAWAY branch performs is INSIDE the
                // connector. Forbidding the proxy's retry loop is what keeps
                // that from being confused with a retry: with this header a
                // 200 can only mean the connector itself found a usable
                // connection.
                .header("x-no-retry", "1");
            let res = tokio::time::timeout(Duration::from_secs(10), req.send())
                .await
                .expect("the follow-up request must not hang on a draining connection")
                .unwrap();
            assert_eq!(
                res.status(),
                200,
                "follow-up request {attempt}: a graceful GOAWAY on a pooled \
                 connection must cost no request its response -- the connector \
                 owes it a fresh connection, not an error:\n{}",
                rec.dump()
            );
            let body = tokio::time::timeout(Duration::from_secs(10), res.text())
                .await
                .expect("reading the response body must not hang")
                .expect("the response body must be completely framed");
            assert_eq!(
                body,
                "ok",
                "follow-up request {attempt}: the response must arrive complete:\n{}",
                rec.dump()
            );
        }

        // The negative claim -- the GOAWAY'd connection never carried another
        // request -- as an absence over a bounded window, backed by the exact
        // whole-log counts below. (`count <= 1` alone is also satisfied by a
        // request that reached no upstream at all.)
        expect_ok(
            rec.expect_none(
                "another request on the GOAWAY'd connection",
                QUIET_WINDOW,
                |e| matches!(e, UpEvent::ReqHeaders { conn: 0, .. }),
            )
            .await,
        );

        // All of the following is asserted while the in-flight stream is still
        // held, i.e. while conn 0 is provably still usable.
        assert_eq!(
            rec.count(|e| matches!(e, UpEvent::ConnClosed { conn: 0 })),
            0,
            "conn 0 must still be OPEN here: if the origin had closed it, the \
             follow-up requests would have had no choice but to dial again and \
             this test would be pinning the origin's teardown rather than the \
             proxy's decision:\n{}",
            rec.dump()
        );
        assert_eq!(
            rec.connections(),
            2,
            "exactly two upstream connections: the GOAWAY'd one, and the one \
             the connector dialled for the follow-ups (a third means a \
             shutting-down connection went back into the pool):\n{}",
            rec.dump()
        );
        assert_eq!(
            rec.count(|e| matches!(e, UpEvent::ReqHeaders { conn: 0, .. })),
            1,
            "exactly one request on the GOAWAY'd connection:\n{}",
            rec.dump()
        );
        assert_eq!(
            rec.count(|e| matches!(e, UpEvent::ReqHeaders { conn: 1, .. })),
            2,
            "both follow-up requests on the fresh connection -- the second one \
             is what shows the fresh connection was pooled and the GOAWAY'd one \
             was not:\n{}",
            rec.dump()
        );

        // Only now: let the origin answer the stream it has been holding since
        // before the GOAWAY.
        release.notify_one();
        let res = tokio::time::timeout(Duration::from_secs(10), in_flight)
            .await
            .expect("the in-flight request must not hang after the GOAWAY")
            .expect("the in-flight request task must not panic")
            .unwrap();
        assert_eq!(
            res.status(),
            200,
            "the in-flight stream must still complete after a graceful \
             GOAWAY:\n{}",
            rec.dump()
        );
        let body = tokio::time::timeout(Duration::from_secs(10), res.text())
            .await
            .expect("reading the in-flight response body must not hang")
            .expect("the in-flight response body must be completely framed");
        assert_eq!(
            body,
            "ok",
            "the in-flight response must arrive complete, not truncated by the \
             connection draining:\n{}",
            rec.dump()
        );
    });
}

/// An error GOAWAY (`INTERNAL_ERROR`) killing the connection mid-exchange:
/// the in-flight request can never complete, and the proxy must answer its
/// client with a proxy error rather than hanging -- without a silent second
/// attempt.
///
/// The second script step is a working `Ok200` on purpose: if the proxy DID
/// silently retry, the retry would succeed and this test would see a 200 plus
/// a second connection -- an observable wrong outcome instead of a vacuously
/// passing one.
pub fn upstream_error_goaway_fails_the_request_without_a_silent_retry(combo: Combo) {
    if combo.up == Up::H1 {
        skip_combo!(
            combo,
            "GOAWAY is an HTTP/2 connection frame; an H1 upstream cannot emit \
             one, and its connection-fatal analogue (a mid-response close) is \
             a different contract"
        );
    }
    let upstream = spawn_scripted_h2_upstream(vec![
        H2UpstreamStep::AbruptGoaway(Reason::INTERNAL_ERROR),
        H2UpstreamStep::Ok200,
    ]);
    let (port, rec) = (upstream.port(), upstream.rec().clone());

    RT.block_on(async {
        let client = combo.client();
        let req = client
            .get(format!("http://{}/", combo.down_addr()))
            .header("x-port", port.to_string())
            .header("x-h2", "1");
        let res = tokio::time::timeout(Duration::from_secs(10), req.send())
            .await
            .expect("the error GOAWAY must surface as a response, not a hang")
            .unwrap();
        assert_eq!(
            res.status(),
            502,
            "a connection killed by an error GOAWAY must fail the in-flight \
             request as a proxy error:\n{}",
            rec.dump()
        );

        // "No silent retry" as an absence over a bounded window; the exact
        // whole-log counts below make it a statement about the entire run.
        expect_ok(
            rec.expect_none("a second upstream attempt", QUIET_WINDOW, |e| {
                matches!(e, UpEvent::ConnAccepted { .. } | UpEvent::ReqHeaders { .. })
            })
            .await,
        );
    });

    assert_eq!(
        rec.connections(),
        1,
        "exactly one upstream connection:\n{}",
        rec.dump()
    );
    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        1,
        "exactly one upstream attempt:\n{}",
        rec.dump()
    );
}

/// `Streamed` must re-frame the upstream request so the body can be produced
/// incrementally, and must not carry the client's stale length across.
///
/// Merged from `streamed_disposition_rewrites_h1_upstream_framing` (which
/// inspected a raw byte capture of an H1 upstream) and
/// `streamed_disposition_with_a_body_through_an_h2_upstream` (which read
/// headers echoed by an H2 upstream). Both now read the same recorded events,
/// and the two H2c-downstream cells are new.
///
/// The rewrite MEANS different things on the two upstream transports, so the
/// assertion branches rather than settling for what they share: H1 must gain
/// `Transfer-Encoding: chunked` (its only incremental framing), H2 must gain
/// nothing at all -- `Transfer-Encoding` is meaningless there, and the
/// incremental framing is simply END_STREAM staying off the HEADERS frame.
pub fn streamed_disposition_rewrites_upstream_framing(combo: Combo) {
    let (port, rec, _upstream) = combo.spawn(&[Step::DrainThenOk200]);

    RT.block_on(async {
        let client = combo.client();
        let mut req = client
            .post(format!("http://{}/", combo.down_addr()))
            .header("x-port", port.to_string())
            .header("x-disposition", "streamed")
            .body("hello world!"); // the client sends Content-Length: 12
        if combo.upstream_is_h2() {
            req = req.header("x-h2", "1");
        }
        let res = tokio::time::timeout(Duration::from_secs(10), req.send())
            .await
            .expect("the streamed request must not hang")
            .unwrap();
        assert_eq!(res.status(), 200);

        // The upstream only answers once it has drained the request body, so
        // the 200 above already implies the end of stream arrived; wait for the
        // recorded fact anyway, since the recording is what the assertions
        // below read.
        expect_ok(
            rec.wait_for("the end of the upstream request body", WAIT, |e| {
                matches!(
                    e,
                    UpEvent::ReqData {
                        end_stream: true,
                        ..
                    }
                )
            })
            .await,
        );
    });

    let headers = expect_ok(
        RT.block_on(rec.wait_for("the upstream request headers", WAIT, |e| {
            matches!(e, UpEvent::ReqHeaders { .. })
        })),
    );
    let UpEvent::ReqHeaders {
        headers_eos,
        content_length,
        transfer_encoding,
        ..
    } = headers
    else {
        unreachable!("the predicate above only matches ReqHeaders")
    };

    assert_eq!(
        content_length,
        None,
        "the stale downstream Content-Length must be removed:\n{}",
        rec.dump()
    );
    assert!(
        !headers_eos,
        "Streamed must leave the upstream request stream open for the body that \
         follows:\n{}",
        rec.dump()
    );
    match combo.up {
        Up::H1 => assert_eq!(
            transfer_encoding.as_deref(),
            Some("chunked"),
            "an H1 upstream request must be re-framed as chunked:\n{}",
            rec.dump()
        ),
        Up::H2 => assert_eq!(
            transfer_encoding,
            None,
            "Transfer-Encoding has no meaning on HTTP/2 and must be removed:\n{}",
            rec.dump()
        ),
    }
    assert_eq!(
        rec.body_bytes(),
        12,
        "the body bytes must arrive at the upstream intact:\n{}",
        rec.dump()
    );
    // Keep the retry-buffer observable honest: `Streamed` is about the request
    // wire, and a silent extra attempt would double the recorded body.
    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        1,
        "exactly one upstream request:\n{}",
        rec.dump()
    );
}
