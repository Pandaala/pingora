//! Tests whose shape exists in exactly ONE transport combination, so running
//! them across the matrix would mean either four copies of the same run or
//! three announced skips. Each one says which cell it is and why that is the
//! only cell.
//!
//! Anything here that turns out to have a meaningful second cell belongs in
//! [`super::scenarios`] instead.

use super::harness::*;
use bytes::Bytes;
use pingora_proxy::RequestBodyEvent;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;

#[test]
fn h2_error_no_retry_after_header_sent_on_reused_conn() {
    let ports = init();
    // Request 1 succeeds over h2 and pools the upstream h2 connection.
    // Request 2 reuses it as a new stream; the upstream sends response
    // HEADERS (committing the downstream response, status forwarded to the
    // client), then resets the stream while sending the body.
    //
    // This is a structural invariant pin, not a regression test for a live
    // hole: as classified in pingora-core today, this h2 body-read error
    // (`ReadError`/"while read h2 response body", see
    // `Http2Session::read_response_body` in
    // pingora-core/src/protocols/http/v2/client.rs) is constructed
    // `RetryType::Decided(false)` regardless of connection reuse --
    // retryable classifications (`RetryType::ReusedOnly` or unconditional
    // `true`) are only ever produced while reading a response's own
    // headers, which by definition happens before that response could be
    // committed downstream. So no reachable error path today is retryable
    // after a final response is committed, on either H1 or H2; the guard in
    // the retry loop currently changes no observable behavior. This test
    // asserts the invariant end-to-end anyway, so that if a future change
    // (e.g. an upstream merge) alters error classification and makes a
    // post-commit error retryable, this test -- not just the guard's own
    // unit tests -- catches the regression.
    //
    // "on a reused connection" is half the claim, and the upstream side is the
    // only place it is visible: the recorder pins BOTH streams onto the same
    // accepted connection, so a regression that silently dialled a fresh
    // upstream connection (making the error non-retryable for an unrelated
    // reason) can no longer pass this test.
    let gate = Arc::new(Notify::new());
    let upstream = spawn_scripted_h2_upstream(vec![
        H2UpstreamStep::Ok200,
        H2UpstreamStep::HeaderThenReset(gate.clone()),
        // If the guard is broken, a third upstream request arrives here.
        H2UpstreamStep::Ok200,
    ]);
    let (port, rec) = (upstream.port(), upstream.rec());

    RT.block_on(async {
        let client = reqwest::Client::new();
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(res.text().await.unwrap(), "ok");

        let mut res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        // Read the partial body the upstream already sent, THEN release the
        // reset: that ordering is what makes this a post-commit error rather
        // than a race against the response headers.
        let partial = tokio::time::timeout(Duration::from_secs(10), res.chunk())
            .await
            .expect("the committed response's first body chunk must arrive")
            .expect("the partial body must be readable");
        assert_eq!(partial.as_deref(), Some(&b"pa"[..]));
        gate.notify_one();

        // The reset stream must surface as a body read error, not as a
        // silently concatenated retry response.
        let rest = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match res.chunk().await {
                    Ok(Some(_)) => continue,
                    other => return other,
                }
            }
        })
        .await
        .expect("reading the reset response body must not hang");
        assert!(
            rest.is_err(),
            "a reset after the response was committed must surface as a body \
             read error, not as a clean end of body"
        );

        expect_ok(
            rec.expect_none(
                "a retry after the response was committed",
                Duration::from_millis(300),
                |e| matches!(e, UpEvent::ReqHeaders { .. }),
            )
            .await,
        );
    });

    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        2,
        "a retry after response commit must not reach the upstream:\n{}",
        rec.dump()
    );
    assert_eq!(
        rec.connections(),
        1,
        "both requests must have gone over the SAME pooled upstream connection, \
         otherwise the second one was not a reused-connection error at all:\n{}",
        rec.dump()
    );
}

/// This exercises terminate over an H2 DOWNSTREAM session (the h2c listener):
/// mid-body termination on one stream produces the local 403 on that stream
/// only, and a later stream on the same downstream connection still works.
/// Neither request sets `x-h2`, so the proxy talks H1 to the upstream(s) --
/// this does NOT drive the `proxy_h2` upstream pump's terminate arms; see
/// `h2_upstream_terminate_resets_stream` for that.
#[test]
fn h2c_downstream_terminate_keeps_connection() {
    let ports = init();
    // Stream A and stream B each get their own scripted upstream (separate
    // ports), so B's success can never race against / accidentally draw
    // A's `Hang` script entry.
    let upstream_a = spawn_scripted_upstream(vec![UpstreamStep::Hang]);
    let upstream_b = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);
    let (port_a, port_b) = (upstream_a.port(), upstream_b.port());

    RT.block_on(async {
        let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
        let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        // Stream A: terminated mid-body by the application. Its upstream
        // is left hanging so this test cannot pass by accident if the
        // terminate path silently waited on the upstream.
        let req = http::Request::builder()
            .method("POST")
            .uri("http://t/")
            .header("x-port", port_a.to_string())
            .header("x-terminate-after-bytes", "1")
            .body(())
            .unwrap();
        let (response_a, mut body_a) = h2.send_request(req, false).unwrap();
        body_a
            .send_data(Bytes::from_static(b"hello"), false)
            .unwrap();
        let resp_a = response_a.await.unwrap();
        assert_eq!(resp_a.status(), 403);

        // Stream B on the SAME downstream connection, against its own
        // upstream, must still work.
        let mut h2 = h2.ready().await.unwrap();
        let req = http::Request::builder()
            .method("GET")
            .uri("http://t/")
            .header("x-port", port_b.to_string())
            .body(())
            .unwrap();
        let (response_b, _) = h2.send_request(req, true).unwrap();
        let resp_b = response_b.await.unwrap();
        assert_eq!(
            resp_b.status(),
            200,
            "the downstream H2 connection must remain usable after a terminate"
        );
    });
}

/// The deadlock P6 is about, end to end: an H2 downstream that declares
/// `content-length: 0`, never sends END_STREAM, and an H2 upstream that does not
/// respond until it has seen the end of the request stream.
///
/// With the upstream framing keyed on the strict transport fact instead of the
/// declaration, nothing ever closes the upstream request stream: the origin
/// waits for END_STREAM, the pump waits for the origin, and the futile-read rule
/// cannot fire because it requires a complete response. The request hangs until
/// the client gives up.
#[test]
fn h2_cl0_never_ending_request_reaches_an_upstream_that_waits_for_eos() {
    init();
    // `EchoRequestEos` drains the request body to its end BEFORE responding, so
    // it answers only once END_STREAM has arrived.
    let upstream = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestEos]);
    let port = upstream.port();
    let ports = init();

    RT.block_on(async {
        let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
        let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        let req = http::Request::builder()
            .method("POST")
            .uri("http://t/")
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .header("content-length", "0")
            .body(())
            .unwrap();
        // No END_STREAM on HEADERS, and `_body` is held for the rest of the
        // block: this client never ends its request stream.
        let (response, _body) = h2.send_request(req, false).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .expect(
                "the upstream never saw END_STREAM: the `Content-Length: 0` declaration was \
                 not forwarded as upstream request framing",
            )
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("x-headers-eos")
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
    });
}

/// An H2 downstream request that declares `content-length: 0` WITHOUT
/// END_STREAM and whose client never sends the end-of-stream: the pump must
/// still finish once the upstream exchange is complete.
///
/// `is_body_done()` is the pure transport fact, so this request's read side
/// stays open, and the only live branch left is a body read that can never
/// yield -- no downstream request-body idle timeout exists. The pump, its
/// task, the downstream stream and the upstream stream stay pinned forever.
///
/// The client is NOT the observable for the hang: it receives its complete
/// response either way. `ProxyHttp::logging` is, because it only runs once the
/// proxy finished the request.
///
/// The response headers ARE the observable for invariant B: abandoning the
/// read must still deliver the application one terminal event
/// (`x-eos-events: 1`), identified as `Abandoned`. Simply finishing the
/// downstream state instead would silently skip an application that finalizes
/// its inspection at termination.
#[test]
fn h2_cl0_never_ending_request_completes() {
    let ports = init();
    let h1_upstream = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);
    // `Ok200Linger`, not `Ok200`: this test deliberately never ends its
    // downstream request stream, so an upstream that dropped the request half
    // would reset it and the pump would take the upstream-error path instead
    // of the futile-read path under test.
    let h2_upstream = spawn_scripted_h2_upstream(vec![H2UpstreamStep::Ok200Linger]);
    let (h1_port, h2_port) = (h1_upstream.port(), h2_upstream.port());

    for (tag, port, h2_upstream) in [("h1", h1_port, false), ("h2", h2_port, true)] {
        RT.block_on(async {
            let (id, completion) = observe_completion();
            let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
            let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();

            let mut builder = http::Request::builder()
                .method("POST")
                .uri("http://t/")
                .header("x-port", port.to_string())
                .header("x-observe-completion", id.into_header())
                .header("content-length", "0");
            if h2_upstream {
                builder = builder.header("x-h2", "1");
            }
            // No END_STREAM on HEADERS, and `_body` is deliberately held (and
            // never written to) for the rest of this block: the client keeps
            // its request stream open forever.
            let (response, _body) = h2.send_request(builder.body(()).unwrap(), false).unwrap();
            let response = tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .expect("timed out waiting for the response")
                .unwrap();
            assert_eq!(response.status(), 200, "{tag} upstream");

            // The request must actually FINISH, not merely answer.
            let record = completion
                .wait(
                    Duration::from_secs(10),
                    &format!(
                        "the {tag}-upstream pump never finished the request: it is still \
                         parked on a downstream body read that can never yield"
                    ),
                )
                .await;

            // ... and abandoning the read must not cost the application its
            // single terminal event (invariant B).
            assert_eq!(
                record.eos_events, 1,
                "the application must still see exactly one terminal event \
                 when the {tag}-upstream pump abandons the downstream read"
            );
            assert_eq!(
                record.abandoned_events, 1,
                "the {tag}-upstream pump must distinguish its synthetic terminal event \
                 from a complete downstream body"
            );
            assert_eq!(
                record.events,
                vec![RequestBodyEvent::Abandoned],
                "the {tag}-upstream pump owes this request exactly one event and it is \
                 the abandonment: this client never sent a byte and never ended its \
                 stream, so there is nothing else to report"
            );
            // The module side of the SAME abandonment. A downstream module is
            // where a real gateway hangs its mirroring/inspection, and it is fed
            // from a different call site than the application hook; nothing else
            // in this suite proves a real pump gives both the same event.
            record.assert_hooks_agree(&format!(
                "the {tag}-upstream pump abandoning an unfinished downstream body"
            ));
        });
    }
}

/// The H1 duplex loop's OTHER way of giving up on the downstream read side: the
/// body pipe is closed because `proxy_handle_upstream` already returned.
///
/// This is not the futile-read branch, and no other test in this file can reach
/// it: that branch requires `is_body_empty()`, and this request declares a real
/// chunked body. The arm is
/// `_ = tx.reserve(), if downstream_state.is_reading() && send_permit.is_err()`,
/// and it used to call `downstream_state.maybe_finished(upstream_closed)` and
/// nothing else -- so the request's single terminal body event was spent on
/// silence: neither `Complete` nor `Abandoned` ever reached
/// `request_body_filter_action`, `request_body_filter` or the module chain, while
/// the request logged as a plain success. An application that finalizes at
/// termination (a digest, an audit record, a mirrored request) simply never
/// finalizes.
///
/// Getting here is deterministic, and every step is forced rather than hoped for:
/// - the client declares a chunked body and never terminates it, so
///   `is_body_empty()` is false (the futile branch cannot fire) and
///   `downstream_state.is_reading()` stays true;
/// - the origin sends a COMPLETE response and then closes its socket, so the
///   proxy's next upstream body write fails. That is the one place
///   `proxy_handle_upstream` sets `request_done = true` off a write error; with
///   `response_done` already true it returns and drops `rx`;
/// - `tx.try_reserve()` on a CLOSED channel returns `Err` whatever the capacity,
///   so the arm's guard needs no full pipe, and the read arm (guarded on
///   `send_permit.is_ok()`) is off for the same reason. Nothing else in the
///   `select!` is runnable.
///
/// The close is gated on this test having read the whole response; see
/// [`UpstreamStep::RespondThenClose`] for why an ungated close would race.
#[test]
fn h1_upstream_gone_mid_upload_reports_exactly_one_abandoned_event() {
    let ports = init();
    let close_upstream = Arc::new(Notify::new());
    let upstream = spawn_scripted_upstream(vec![UpstreamStep::RespondThenClose(
        OK_KEEPALIVE,
        close_upstream.clone(),
    )]);
    let port = upstream.port();

    RT.block_on(async {
        let (id, completion) = observe_completion();
        let mut stream = TcpStream::connect(ports.h1_addr()).await.unwrap();
        // `x-no-retry` keeps the native retry buffer out of the shape: a
        // buffered body could be replayed through the same hooks on a second
        // attempt, and the claim here is about ONE attempt's event count.
        stream
            .write_all(
                format!(
                    "POST /upload HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
                     x-no-retry: 1\r\nx-observe-completion: {}\r\n\
                     Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n",
                    id.into_header()
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        // The complete response, read to its last byte. Response tasks only
        // reach the downstream half after `response_done` was set, so this also
        // establishes that the upstream half will never poll its read arm again.
        let mut pending = Vec::new();
        let response = read_one_h1_response(&mut stream, &mut pending).await;
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "the origin's complete response must reach the client before it closes: \
             {response}"
        );

        // Only now may the origin's socket go away.
        close_upstream.notify_one();

        // Keep uploading: each chunk is another upstream write, and one of them
        // is the write that fails. The client never terminates the body, so no
        // downstream end-of-stream can be mistaken for the event under test.
        let uploader = tokio::spawn(async move {
            loop {
                if stream.write_all(b"5\r\nworld\r\n").await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let record = completion
            .wait(
                Duration::from_secs(10),
                "the pump never finished the request after its upstream leg went away",
            )
            .await;
        uploader.abort();

        assert_eq!(
            record.eos_events, 1,
            "the application is owed exactly one terminal request-body event, and a \
             closed body pipe must not spend it on silence"
        );
        assert_eq!(
            record.abandoned_events, 1,
            "the bytes delivered so far are only a prefix -- the client never ended its \
             body -- so the single terminal event must be `Abandoned`, not `Complete`"
        );
        assert_eq!(
            record.events.last(),
            Some(&RequestBodyEvent::Abandoned),
            "the abandonment must be the LAST event of the request, and the events are \
             {:?}",
            record.events
        );
        assert_eq!(
            record.count(RequestBodyEvent::Complete),
            0,
            "no end of stream was ever observed on this transport, so nothing may \
             report one: the events are {:?}",
            record.events
        );
        record.assert_hooks_agree("an H1 upstream leg that went away mid-upload");
    });
}

/// The direction nothing else in this suite states: a request whose client
/// REALLY ends its body must report zero abandonments.
///
/// Every other end-of-stream assertion here goes through `eos_events` /
/// `x-eos-events`, which is incremented on `event.is_terminal()` -- true for
/// `Abandoned` as well as for `Complete`. So a regression that mislabels a
/// normal end of stream keeps every one of those assertions green, while an
/// application built on the distinction breaks completely.
///
/// Run on BOTH downstream transports and BOTH pumps, because the four cells
/// have separate terminal-event call sites.
#[test]
fn a_completed_downstream_body_is_never_reported_as_abandoned() {
    let ports = init();

    for h2_upstream in [false, true] {
        let up_tag = if h2_upstream { "h2" } else { "h1" };
        for down_tag in ["h1", "h2c"] {
            // A fresh upstream per cell: the script cursor is per upstream, and
            // `RespondAfterBody`/`EchoRequestEos` answer only once the whole
            // request body is in, so the 200 the client reads is itself evidence
            // that the body really reached the origin.
            let upstream = if h2_upstream {
                spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestEos])
            } else {
                spawn_scripted_upstream(vec![UpstreamStep::RespondAfterBody(OK_KEEPALIVE)])
            };
            let port = upstream.port();
            let cell = format!("{down_tag} downstream -> {up_tag} upstream");

            RT.block_on(async {
                let (id, completion) = observe_completion();
                let record = if down_tag == "h1" {
                    let client = reqwest::Client::new();
                    let mut req = client
                        .post(format!("http://{}/", ports.h1_addr()))
                        .header("x-port", port.to_string())
                        .header("x-observe-completion", id.into_header())
                        .body("hello");
                    if h2_upstream {
                        req = req.header("x-h2", "1");
                    }
                    let res = tokio::time::timeout(Duration::from_secs(10), req.send())
                        .await
                        .unwrap_or_else(|_| panic!("{cell} hung"))
                        .unwrap();
                    assert_eq!(res.status(), 200, "{cell}");
                    completion
                        .wait(
                            Duration::from_secs(10),
                            &format!("the {cell} request never reached ProxyHttp::logging"),
                        )
                        .await
                } else {
                    let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
                    let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    let mut h2 = h2.ready().await.unwrap();
                    let mut builder = http::Request::builder()
                        .method("POST")
                        .uri("http://t/")
                        .header("x-port", port.to_string())
                        .header("x-observe-completion", id.into_header())
                        .header("content-length", "5");
                    if h2_upstream {
                        builder = builder.header("x-h2", "1");
                    }
                    let (response, mut body) =
                        h2.send_request(builder.body(()).unwrap(), false).unwrap();
                    // The real transport end-of-stream, on the DATA frame.
                    body.send_data(Bytes::from_static(b"hello"), true).unwrap();
                    let response = tokio::time::timeout(Duration::from_secs(10), response)
                        .await
                        .unwrap_or_else(|_| panic!("{cell} hung"))
                        .unwrap();
                    assert_eq!(response.status(), 200, "{cell}");
                    completion
                        .wait(
                            Duration::from_secs(10),
                            &format!("the {cell} request never reached ProxyHttp::logging"),
                        )
                        .await
                };

                assert_eq!(
                    record.abandoned_events, 0,
                    "{cell}: a downstream body the client really ended must never be \
                     reported as abandoned -- an application that cancels its mirror on \
                     `Abandoned` would cancel every single request"
                );
                assert_eq!(
                    record.eos_events, 1,
                    "{cell}: the completed body still owes the application exactly one \
                     terminal event"
                );
                assert_eq!(
                    record.count(RequestBodyEvent::Complete),
                    1,
                    "{cell}: exactly one of this request's events must be the completion, \
                     and the events are {:?}",
                    record.events
                );
                assert_eq!(
                    record.events.last(),
                    Some(&RequestBodyEvent::Complete),
                    "{cell}: the completion must be the LAST event -- nothing may follow \
                     the end of the body -- and the events are {:?}",
                    record.events
                );
                record.assert_hooks_agree(&cell);
            });
        }
    }
}

#[test]
fn retry_predicate_gates_reused_connection_retry() {
    let ports = init();
    // Upstream: request 1 succeeds (pools the connection); request 2 closes
    // without a response (retryable, pre-commit); request 3 would be the
    // retry attempt and succeeds.
    //
    // Connection IDENTITY is asserted alongside the attempt count, because the
    // attempt count alone does not distinguish the two shapes this test is
    // about: attempt 2 must have REUSED the pooled connection (that is what
    // makes its failure retryable at all), and the retry must have been
    // dialled on a FRESH one (the pooled one is poisoned).
    let run = |no_retry: bool| -> (usize, usize, u16) {
        let upstream = spawn_scripted_upstream(vec![
            UpstreamStep::Respond(OK_KEEPALIVE),
            UpstreamStep::CloseWithoutResponse,
            UpstreamStep::Respond(OK_KEEPALIVE),
        ]);
        let (port, rec) = (upstream.port(), upstream.rec());
        let status = RT.block_on(async {
            let client = reqwest::Client::new();
            let res = client
                .get(format!("http://{}/", ports.h1_addr()))
                .header("x-port", port.to_string())
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200);

            let mut req = client
                .get(format!("http://{}/", ports.h1_addr()))
                .header("x-port", port.to_string());
            if no_retry {
                req = req.header("x-no-retry", "1");
            }
            let status = res_status(req.send().await);
            // Nothing further may reach the upstream after the exchange the
            // client already saw the end of.
            expect_ok(
                rec.expect_none(
                    "a further upstream attempt",
                    Duration::from_millis(300),
                    |e| matches!(e, UpEvent::ReqHeaders { .. }),
                )
                .await,
            );
            status
        });
        (
            rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
            rec.connections(),
            status,
        )
    };

    // Control: retry allowed -- the second client request is retried on a
    // fresh connection and succeeds; three upstream requests total, over two
    // upstream connections (the pooled one, then the retry's fresh dial).
    let (attempts, connections, status) = run(false);
    assert_eq!(status, 200);
    assert_eq!(attempts, 3);
    assert_eq!(
        connections, 2,
        "the retry must have been dialled on a FRESH connection, and the two \
         attempts before it must have shared the pooled one"
    );

    // Predicate false: exactly one failed attempt, surfaced as 502, and no
    // second connection was ever dialled.
    let (attempts, connections, status) = run(true);
    assert_eq!(status, 502);
    assert_eq!(attempts, 2);
    assert_eq!(
        connections, 1,
        "a refused retry must not dial a replacement connection"
    );
}

/// `Streamed` must NOT re-frame a request that has no body.
///
/// Rewriting a plain `GET` to `Transfer-Encoding: chunked` puts a `0\r\n\r\n`
/// terminator on a POOLED upstream connection. An origin or WAF that ignores
/// bodies on bodyless methods leaves those five bytes in the stream, which
/// desynchronises every later request on that connection -- a request
/// smuggling primitive. `GET` + `Transfer-Encoding: chunked` is also a shape
/// many WAFs reject outright.
#[test]
fn streamed_does_not_reframe_a_bodyless_request() {
    let ports = init();
    let (port, captured) = spawn_recording_upstream(b"\r\n\r\n");

    RT.block_on(async {
        let request = format!(
            "GET / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             x-disposition: streamed\r\n\r\n"
        );
        let (_stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"HTTP/1.1 200").await;
        assert!(String::from_utf8_lossy(&collected).starts_with("HTTP/1.1 200"));
    });

    let bytes = captured.lock().unwrap().clone();
    let text = String::from_utf8_lossy(&bytes).to_lowercase();
    assert!(
        !text.contains("transfer-encoding"),
        "a bodyless request must not be re-framed as chunked: {text}"
    );
    let header_end = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("the upstream request headers must have been captured");
    assert!(
        bytes[header_end + 4..].is_empty(),
        "nothing may follow the headers of a bodyless request (a chunked \
         terminator here is a smuggling primitive on a pooled connection): {:?}",
        String::from_utf8_lossy(&bytes[header_end + 4..])
    );
}

/// A CONNECT tunnel must not be half-closed at header time by an application
/// disposition: `safe_disposition` (in `proxy_common.rs`) coerces a
/// non-`Ordinary` disposition back to `Ordinary` for CONNECT requests. An
/// honored `Bodyless` would put END_STREAM on the upstream HEADERS -- ending
/// the request half of the tunnel before a single tunnel byte could flow.
///
/// The observable is therefore the TUNNEL BYTES arriving upstream, and it is
/// asserted before anything else so that it is what fails. Two corrections
/// are folded into that ordering:
/// - the doc used to name `headers_eos == false` as the observable, but under
///   the mutation (coercion removed) the run failed at `resp.status() == 200`
///   several assertions earlier -- the named observable was not the one that
///   spoke;
/// - and it could not have spoken reliably anyway: `headers_eos` reads
///   `false` whenever anything else is pending on the stream, and an honored
///   `Bodyless` here also makes the proxy fail the request closed and reset
///   the stream. See the note on `UpEvent::ReqHeaders::headers_eos`. The
///   whole-log count is kept below as a corroborating check, not as the
///   claim.
///
/// The pure-function truth table (`safe_disposition_truth_table`) already
/// pins the DECISION for every fact combination; what this pins end-to-end is
/// the WIRING -- a pump consulting the coercion at all, with correctly
/// collected facts, for a real CONNECT request.
///
/// One cell (H2c downstream -> H2 upstream), not the matrix, because no other
/// cell is constructible in this fork:
/// - H1 downstream: an authority-form request-target needs the
///   `patched_http1` feature, which does not compile here (it requires a
///   patched httparse this workspace does not carry); without it the parse
///   rejects the request line (400 from `set_raw_path`).
/// - H1 upstream: `http_req_header_to_wire` serializes `raw_path()`, which
///   panics on an authority-form URI (no path-and-query, no raw-path
///   fallback), so CONNECT-to-an-H1-peer cannot even be driven.
///
/// Consequently the H1-specific hazard the coercion also guards (re-framing
/// the tunnel as `Transfer-Encoding: chunked`) is unreachable end-to-end in
/// this fork and stays pinned by the truth table alone. `Streamed` is not
/// separately driven either: on the H2 pump its honored rewrite is
/// wire-identical to the coerced `Ordinary` for a bare CONNECT, so no
/// end-to-end assertion could discriminate it.
///
/// Edgion cannot reach this path at all -- Gateway API has no CONNECT route
/// type -- which is exactly why the guard is pinned here in the fork
/// (`tasks/todo/important_test/03-fork-layer-coverage-for-unreachable-paths.md`
/// in the Edgion repository).
#[test]
fn bodyless_does_not_half_close_a_connect_tunnel() {
    let ports = init();
    let upstream = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestEos]);
    let (port, rec) = (upstream.port(), upstream.rec());

    RT.block_on(async {
        let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
        let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        // Plain CONNECT: authority-form URI, no END_STREAM on HEADERS -- the
        // stream IS the tunnel.
        let req = http::Request::builder()
            .method("CONNECT")
            .uri(format!("127.0.0.1:{port}"))
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .header("x-disposition", "bodyless")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(req, false).unwrap();

        // Tunnel bytes from the client, then end the request half so the
        // scripted upstream's drain completes and responds. An honored
        // `Bodyless` could not carry these bytes at all (its HEADERS already
        // closed the stream), so their arrival upstream is itself half the
        // claim.
        req_body
            .send_data(Bytes::from_static(b"tunnel-preamble"), true)
            .unwrap();

        // The claim, first: an honored `Bodyless` closes the upstream request
        // half at header time, so not one tunnel byte could arrive and this
        // wait is what times out.
        expect_ok(
            rec.wait_for(
                "the upstream to see the CONNECT request",
                Duration::from_secs(10),
                |e| matches!(e, UpEvent::ReqHeaders { .. }),
            )
            .await,
        );
        expect_ok(
            rec.wait_for(
                "the tunnel bytes to finish arriving upstream (an honored \
                 Bodyless disposition would have half-closed the tunnel at \
                 header time, so none could)",
                Duration::from_secs(10),
                |e| {
                    matches!(
                        e,
                        UpEvent::ReqData {
                            end_stream: true,
                            ..
                        }
                    )
                },
            )
            .await,
        );

        let resp = tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .expect("the tunnel response must not hang")
            .unwrap();
        assert_eq!(resp.status(), 200);
    });

    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        1,
        "exactly one upstream CONNECT request:\n{}",
        rec.dump()
    );
    // Corroboration, not the claim: a `false` reading of `headers_eos` is not
    // proof on its own (see its doc comment), but a `true` one would be proof
    // of the defect.
    assert_eq!(
        rec.count(|e| matches!(
            e,
            UpEvent::ReqHeaders {
                headers_eos: true,
                ..
            }
        )),
        0,
        "a CONNECT tunnel's HEADERS must not carry END_STREAM (an honored \
         Bodyless disposition would half-close the tunnel at header time):\n{}",
        rec.dump()
    );
    assert_eq!(
        rec.body_bytes(),
        b"tunnel-preamble".len(),
        "the client's tunnel bytes must reach the upstream exactly once:\n{}",
        rec.dump()
    );
}

/// The trailer hook returning `Continue` must let the request through -- and
/// the trailer FIELDS must not reach the upstream, because Pingora does not
/// expose or forward them.
#[test]
fn trailer_continue_completes_without_forwarding_trailers() {
    let ports = init();
    // `\r\n0\r\n` is the start of the terminal chunk: it matches whether or
    // not trailer fields follow it, so the upstream answers either way and a
    // regression shows up as captured trailer bytes rather than a hang.
    let (port, captured) = spawn_recording_upstream(b"\r\n0\r\n");

    RT.block_on(async {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n0\r\nx-checksum: ok\r\n\r\n"
        );
        let (_stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"HTTP/1.1 200").await;
        let text = String::from_utf8_lossy(&collected);
        assert!(text.starts_with("HTTP/1.1 200"), "expected 200: {text}");
        assert!(
            text.to_lowercase().contains("x-trailer-hook-calls: 1"),
            "the trailer hook must have run exactly once: {text}"
        );
    });

    let bytes = captured.lock().unwrap().clone();
    let text = String::from_utf8_lossy(&bytes).to_lowercase();
    assert!(
        text.contains("hello"),
        "the body must still be forwarded: {text}"
    );
    assert!(
        !text.contains("x-checksum"),
        "trailer fields must not be forwarded upstream: {text}"
    );
}

/// `request_trailer_filter` fires AT MOST ONCE per downstream request.
///
/// The upstream script pools a connection, then closes the reused connection
/// without a response (retryable, nothing committed downstream), so the retry
/// runs the whole downstream pump again: its retry-buffer prelude replays the
/// same EOF (`data == None`) while the trailer fact is still true. Without the
/// latch the hook is invoked a second time and the echoed count is 2.
#[test]
fn trailer_hook_fires_at_most_once_across_retries() {
    let ports = init();
    let upstream = spawn_scripted_upstream(vec![
        UpstreamStep::Respond(OK_KEEPALIVE),
        UpstreamStep::CloseWithoutResponse,
        UpstreamStep::Respond(OK_KEEPALIVE),
    ]);
    let (port, rec) = (upstream.port(), upstream.rec());

    RT.block_on(async {
        // Prime the upstream connection pool so the failing attempt below is
        // on a REUSED connection, which is what makes it retryable.
        let client = reqwest::Client::new();
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        // An empty chunked body with trailers: the retry attempt's prelude
        // path is exactly the one that used to re-fire the hook.
        let request = format!(
            "POST / HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             0\r\nx-checksum: ok\r\n\r\n"
        );
        let (_stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"HTTP/1.1 200").await;
        let text = String::from_utf8_lossy(&collected).to_lowercase();
        assert!(
            text.contains("x-trailer-hook-calls: 1"),
            "request_trailer_filter must fire exactly once across retries: {text}"
        );
    });

    // prime + failed attempt + retry. This upstream is private to this test,
    // so the count is exact rather than a lower bound.
    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        3,
        "the retry must actually have happened (prime + failed attempt + \
         retry):\n{}",
        rec.dump()
    );
    // ... and it must have been a RETRY of a reused connection: the prime and
    // the failed attempt share one connection, the retry dials a second.
    assert_eq!(
        rec.connections(),
        2,
        "the failed attempt must have reused the primed connection, and the \
         retry must have dialled a fresh one:\n{}",
        rec.dump()
    );
}

/// The truth table of the header-time end-of-stream decision, end to end.
///
/// For EVERY disposition on a request with NO body, through an H2 upstream:
/// exactly one upstream END_STREAM must reach the wire and exactly one
/// end-of-stream event must reach the application. Two independent holes made
/// this fail before:
/// - the H2 pump had no bodyless prelude at all (the H1 pump did), so a
///   bodyless request delivered ZERO body events to the application and
///   `Terminate` before any body event was unreachable;
/// - adding that prelude naively would emit a second, standalone END_STREAM on
///   a stream the HEADERS frame had already closed.
///
/// `x-headers-eos` is echoed by the scripted upstream (so a missing upstream
/// EOS shows up as a hang, and a doubled one as an h2 `UserError`);
/// `x-eos-events` is echoed by the application.
#[test]
fn bodyless_request_emits_exactly_one_eos_for_every_disposition() {
    let ports = init();
    // (disposition, send_end_stream opt-out, expected x-headers-eos)
    //
    // `safe_disposition` coerces every non-`Ordinary` disposition back to
    // `Ordinary` on a request with no body, so all three rows agree -- which
    // is exactly the point: the choice cannot change the framing of a request
    // that has nothing to frame.
    let cases = [
        ("ordinary", false, "1"),
        ("ordinary", true, "0"),
        ("bodyless", false, "1"),
        ("bodyless", true, "0"),
        ("streamed", false, "1"),
        ("streamed", true, "0"),
    ];

    for (disposition, no_header_eos, expected_headers_eos) in cases {
        let upstream = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestEos]);
        let port = upstream.port();
        RT.block_on(async {
            let client = reqwest::Client::new();
            let mut req = client
                .get(format!("http://{}/", ports.h1_addr()))
                .header("x-port", port.to_string())
                .header("x-h2", "1")
                .header("x-disposition", disposition);
            if no_header_eos {
                // Mirrors the gRPC-web bridge's `set_send_end_stream(false)`.
                req = req.header("x-no-header-eos", "1");
            }
            let res = tokio::time::timeout(Duration::from_secs(10), req.send())
                .await
                .unwrap_or_else(|_| panic!("{disposition} (opt-out {no_header_eos}) hung"))
                .unwrap();

            assert_eq!(res.status(), 200, "{disposition} opt-out={no_header_eos}");
            assert_eq!(
                res.headers().get("x-headers-eos").unwrap(),
                expected_headers_eos,
                "upstream END_STREAM placement for {disposition} opt-out={no_header_eos}"
            );
            assert_eq!(
                res.headers().get("x-eos-events").unwrap(),
                "1",
                "the application must see exactly one end-of-stream event for \
                 {disposition} opt-out={no_header_eos}"
            );
        });
    }
}

/// A terminate on a cache-enabled request must release the cache lock.
///
/// Terminate reports `error = None`, so the `final_error` branch in `lib.rs`
/// that disables the cache never runs for it. A cache-enabled MISS holding a
/// write lock would then reach `WritePermit::Drop` unfinished, which trips
/// `debug_assert!(false, "Dangling cache lock started!")` inside the proxy's
/// connection task. tokio swallows that panic, so the assertion here is on the
/// message-matching panic counter installed by `start_seam_server`.
#[test]
fn terminate_with_cache_enabled_does_not_leave_a_dangling_lock() {
    let ports = init();
    let upstream = spawn_scripted_upstream(vec![UpstreamStep::Hang]);
    let port = upstream.port();
    let before = DANGLING_CACHE_LOCKS.load(Ordering::SeqCst);

    RT.block_on(async {
        let request = format!(
            "POST /cache-terminate HTTP/1.1\r\nHost: t\r\nx-port: {port}\r\n\
             x-enable-cache: 1\r\n\
             x-terminate-after-bytes: 1\r\n\
             Transfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n"
        );
        let (_stream, collected) =
            raw_h1_roundtrip(&ports.h1_addr(), request.as_bytes(), b"denied").await;
        assert!(String::from_utf8_lossy(&collected).starts_with("HTTP/1.1 403"));
    });

    // The lock is released when the session is dropped, shortly after the
    // response was flushed.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        DANGLING_CACHE_LOCKS.load(Ordering::SeqCst),
        before,
        "a cache-enabled terminate must not leave a dangling cache lock"
    );
}

/// The trait default for `request_body_filter_action` must delegate to the
/// legacy `request_body_filter` through a real pump. `LegacyHookProxy` (behind
/// the legacy listener) overrides ONLY the legacy hook.
#[test]
fn legacy_request_body_filter_is_delegated_through_the_h1_pump() {
    let ports = init();
    let upstream = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);
    let port = upstream.port();

    RT.block_on(async {
        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/", ports.legacy_addr()))
            .header("x-port", port.to_string())
            .body("hello world!")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get("x-legacy-bytes").unwrap(),
            "12",
            "the legacy request_body_filter must have seen the request body"
        );
        assert_ne!(
            res.headers().get("x-legacy-calls").unwrap(),
            "0",
            "the legacy request_body_filter must have been invoked"
        );
    });
}

/// Retry predicate, consumption point 1: the NATIVE RETRY BUFFER.
///
/// `request_retry_allowed() == false` must also stop the pumps from buffering
/// the request body for a replay that can never happen -- an unbounded-ish
/// per-request memory cost paid for nothing. The retry loop's own check cannot
/// catch a regression here: it only decides whether to re-dial.
#[test]
fn retry_predicate_gates_the_request_body_retry_buffer() {
    let ports = init();
    let upstream = spawn_scripted_upstream(vec![
        UpstreamStep::Respond(OK_KEEPALIVE),
        UpstreamStep::Respond(OK_KEEPALIVE),
    ]);
    let port = upstream.port();

    RT.block_on(async {
        let client = reqwest::Client::new();
        let buffered = |res: &reqwest::Response| {
            res.headers()
                .get("x-retry-buffer")
                .map(|v| v.to_str().unwrap().to_string())
        };

        // Control: retries allowed, so the body IS buffered.
        let res = client
            .post(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .body("hello world!")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            buffered(&res).as_deref(),
            Some("1"),
            "with retries allowed the request body must be buffered for replay"
        );

        // The predicate says no: nothing may be buffered.
        let res = client
            .post(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-no-retry", "1")
            .body("hello world!")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            buffered(&res).as_deref(),
            Some("0"),
            "a request that can never be retried must not have its body buffered"
        );
    });
}

/// Retry predicate, consumption point 2: the error handed BACK from
/// `error_while_proxy`.
///
/// The upstream closes a REUSED connection without responding, which
/// `error_while_proxy` decides is retryable. With the predicate saying no, the
/// error the application finally receives must say so too -- otherwise
/// `fail_to_proxy`/`logging` are told the request was retryable when the proxy
/// had already ruled that out. The retry loop's own check cannot catch a
/// regression here: it refuses the retry either way and never touches the error.
#[test]
fn retry_predicate_forces_the_error_from_error_while_proxy() {
    let ports = init();
    let upstream = spawn_scripted_upstream(vec![
        UpstreamStep::Respond(OK_KEEPALIVE),
        UpstreamStep::CloseWithoutResponse,
    ]);
    let (port, rec) = (upstream.port(), upstream.rec());

    RT.block_on(async {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        // Attempt 1 succeeds and pools the upstream connection.
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        let (id, completion) = observe_completion();
        // Attempt 2 reuses it and gets closed without a response.
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", port.to_string())
            .header("x-no-retry", "1")
            .header("x-observe-completion", id.into_header())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 502);

        // The wire fact first: no third request was forwarded upstream.
        expect_ok(
            rec.expect_none(
                "the refused retry reaching the upstream",
                Duration::from_millis(300),
                |e| matches!(e, UpEvent::ReqHeaders { .. }),
            )
            .await,
        );
        let record = completion
            .wait(
                Duration::from_secs(10),
                "the request never reached ProxyHttp::logging",
            )
            .await;
        assert_eq!(
            record.retry_flag, 0,
            "the error handed to the application must be marked non-retryable"
        );
    });
    assert_eq!(
        rec.count(|e| matches!(e, UpEvent::ReqHeaders { .. })),
        2,
        "the refused retry must not have reached the upstream:\n{}",
        rec.dump()
    );
    // The failing attempt must have REUSED the pooled connection: that is what
    // makes `error_while_proxy` classify its failure as retryable in the first
    // place, so on a fresh dial this test would be asserting nothing.
    assert_eq!(
        rec.connections(),
        1,
        "both attempts must have gone over the same pooled connection:\n{}",
        rec.dump()
    );
}

/// Retry predicate, consumption point 3: the error handed back from
/// `fail_to_connect`.
///
/// The application's `fail_to_connect` marks the connect failure retryable --
/// the hook's documented purpose, and the only way any connect error becomes
/// retryable at all (`connectors::l4` resolves every fresh-dial connect error to
/// `Decided(false)`). With the retry predicate saying no, the error the
/// application finally receives must be marked non-retryable again.
///
/// No connect-failure test existed at all, so this also pins that a request
/// whose upstream cannot be dialled ends as a 502 rather than, say, a hang.
#[test]
fn retry_predicate_forces_the_error_from_fail_to_connect() {
    let ports = init();
    // A port that was bound and released: nothing is listening on it.
    let dead_port = reserve_port();

    RT.block_on(async {
        let (id, completion) = observe_completion();
        let client = reqwest::Client::new();
        let res = client
            .get(format!("http://{}/", ports.h1_addr()))
            .header("x-port", dead_port.to_string())
            .header("x-no-retry", "1")
            .header("x-connect-retryable", "1")
            .header("x-observe-completion", id.into_header())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 502);

        let record = completion
            .wait(
                Duration::from_secs(10),
                "the request never reached ProxyHttp::logging",
            )
            .await;
        assert_eq!(
            record.retry_flag, 0,
            "a connect failure that may not be retried must be marked non-retryable"
        );
    });
}

/// `Streamed` must never close the upstream request stream early, whatever the
/// downstream request DECLARED.
///
/// The shape: an H2 downstream declaring `content-length: 0` that has not sent
/// END_STREAM. Its declaration is what the `Ordinary` upstream framing is built
/// from (see `h2_cl0_without_end_stream_h2_upstream_sends_one_eos`), and feeding
/// that same declaration to `Streamed` revives
/// `upstream_empty_data_end_stream`'s otherwise-unreachable `Streamed` arm: a
/// standalone empty DATA/END_STREAM right after the headers. That sets
/// `upstream_body_closed`, so every byte the application would go on to stream
/// in through `request_body_filter_action` -- the entire point of `Streamed` --
/// is refused by the suppressed-write branch, and on `Ordinary`/`Streamed` that
/// refusal is absorbed into `to_errored()`: the origin acts on a request whose
/// body was silently removed while the client is told it succeeded.
///
/// The observable is the ORDER of two events, which is what makes it
/// discriminating rather than a timing guess: the scripted upstream drains the
/// request body BEFORE responding, so it can only answer once it has seen an end
/// of stream. If the proxy answers before this client sends one, the early EOS
/// was sent.
#[test]
fn streamed_does_not_close_the_upstream_stream_for_a_cl0_request() {
    let ports = init();
    let upstream = spawn_scripted_h2_upstream(vec![H2UpstreamStep::EchoRequestFraming]);
    let port = upstream.port();

    RT.block_on(async {
        let tcp = TcpStream::connect(ports.h2c_addr()).await.unwrap();
        let (h2, connection) = h2::client::handshake(tcp).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        let req = http::Request::builder()
            .method("POST")
            .uri("http://t/")
            .header("x-port", port.to_string())
            .header("x-h2", "1")
            .header("x-disposition", "streamed")
            .header("content-length", "0")
            .body(())
            .unwrap();
        // No END_STREAM on HEADERS.
        let (response, mut body) = h2.send_request(req, false).unwrap();
        tokio::pin!(response);

        tokio::select! {
            _ = &mut response => panic!(
                "the upstream answered before the client ended its request stream: \
                 Streamed sent an early upstream END_STREAM"
            ),
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }

        // Now end it for real; the upstream may answer from here on.
        body.send_data(Bytes::new(), true).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .expect("timed out waiting for the response")
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("x-headers-eos")
                .and_then(|v| v.to_str().ok()),
            Some("0"),
            "Streamed must keep END_STREAM off the upstream HEADERS frame"
        );
        assert_eq!(
            response
                .headers()
                .get("x-eos-events")
                .and_then(|v| v.to_str().ok()),
            Some("1"),
            "the application must still see exactly one end-of-stream event"
        );
    });
}
