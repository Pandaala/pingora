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
use h2::Reason;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
/// Promptness is measured as time-to-EOF downstream from the moment the
/// upstream demonstrably held the request; see `terminate_reply_and_eof`.
pub fn terminate_is_prompt_and_cancels_the_upstream(combo: Combo) {
    if combo.down == Down::H2c {
        // The promptness half has no downstream observable here. The
        // application flushes its own complete 403, and an H2 session
        // deliberately stays open across a terminate (that is its contract --
        // see `single::h2c_downstream_terminate_keeps_connection`), so a pump
        // still parked on the hung upstream looks exactly like one that
        // finished.
        skip_combo!(
            combo,
            "an H2 downstream keeps its connection across a terminate, so \
             time-to-EOF -- the only observable that can distinguish a finished \
             pump from one still awaiting the hung upstream -- does not exist"
        );
    }
    let (port, rec, _upstream) = combo.spawn(&[Step::HangObservingCancel]);
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
        // `counter <= 1` form is also satisfied by a counter that never moved,
        // i.e. by the request never reaching the upstream at all.
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
        // Failing closed must not leave the origin working on a request nobody
        // will read. Only H2 can say that as a per-stream fact.
        match combo.up {
            // H1 tears the whole connection down, and it does so with a RESET
            // (there are unread request bytes in the proxy's receive buffer),
            // which is neither a `PeerHalfClose` -- that is a clean FIN -- nor
            // distinguishable from this upstream ending its own connection once
            // its recording window is up. The H1 cells' claim is carried
            // entirely by the framing and zero-body-bytes assertions below,
            // exactly as the test this was merged from made it.
            Up::H1 => {}
            Up::H2 => {
                expect_ok(
                    rec.wait_for("RST_STREAM on the upstream request stream", WAIT, |e| {
                        matches!(e, UpEvent::PeerReset { .. })
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
        let (status, eos_events, _echoed) =
            h2_cl0_no_end_stream_request(port, combo.upstream_is_h2()).await;
        assert_eq!(status, 200);
        assert_eq!(
            eos_events, "1",
            "the application must see exactly one end-of-stream event"
        );

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
