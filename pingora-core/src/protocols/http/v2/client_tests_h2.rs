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

use super::*;
use bytes::Bytes;
use http::{Response, StatusCode};
use tokio::io::duplex;
use tokio::sync::oneshot;

async fn session_with_delayed_response() -> (
    Http2Session,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let (client_io, server_io) = duplex(65536);
    let (request_accepted_tx, request_accepted_rx) = oneshot::channel();
    let (release_response_tx, release_response_rx) = oneshot::channel();

    let server_task = tokio::spawn(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        if let Some(result) = conn.accept().await {
            let (req, mut send_resp) = result.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            let _ = request_accepted_tx.send(());
            let _ = release_response_rx.await;

            let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
            send_resp.send_response(resp, true).unwrap();
            conn.graceful_shutdown();
        }
        while let Some(_result) = conn.accept().await {}
    });

    let (send_req, connection) = h2::client::handshake(client_io).await.unwrap();
    let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
    let ping_timeout = Arc::new(AtomicBool::new(false));
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
        let _ = closed_tx.send(true);
    });

    let conn_ref = crate::connectors::http::v2::ConnectionRef::new(
        send_req.clone(),
        closed_rx,
        ping_timeout,
        0,
        1,
        Digest::default(),
    );
    let mut h2s = Http2Session::new(send_req, conn_ref);
    let mut req = RequestHeader::build("GET", b"/", None).unwrap();
    req.insert_header(http::header::HOST, "example.com")
        .unwrap();
    h2s.write_request_header(Box::new(req), true).unwrap();

    request_accepted_rx
        .await
        .expect("server should accept the request before the response-header read");

    (h2s, release_response_tx, server_task, connection_task)
}

#[tokio::test]
async fn response_header_read_can_resume_after_read_timeout() {
    let (mut h2s, release_response, server_task, connection_task) =
        session_with_delayed_response().await;
    h2s.read_timeout = Some(Duration::from_millis(1));

    let err = h2s
        .read_response_header()
        .await
        .expect_err("delayed response header should hit the read timeout");
    assert!(
        matches!(err.etype, ReadTimedout),
        "unexpected first read error: {err:?}"
    );
    assert!(h2s.response_header().is_none());
    assert!(
        h2s.resp_fut.is_some(),
        "timing out must not drop the pending response future"
    );

    h2s.read_timeout = None;
    release_response.send(()).unwrap();
    h2s.read_response_header().await.unwrap();
    assert_eq!(h2s.response_header().unwrap().status, StatusCode::OK);

    server_task.abort();
    connection_task.abort();
}

#[tokio::test]
async fn response_header_read_can_resume_after_external_cancellation() {
    let (mut h2s, release_response, server_task, connection_task) =
        session_with_delayed_response().await;

    let first_read =
        tokio::time::timeout(Duration::from_millis(1), h2s.read_response_header()).await;
    assert!(
        first_read.is_err(),
        "external timeout should cancel the pending header read"
    );
    assert!(h2s.response_header().is_none());
    assert!(
        h2s.resp_fut.is_some(),
        "cancelling the read must not drop the pending response future"
    );

    release_response.send(()).unwrap();
    h2s.read_response_header().await.unwrap();
    assert_eq!(h2s.response_header().unwrap().status, StatusCode::OK);

    server_task.abort();
    connection_task.abort();
}

#[tokio::test]
async fn h2_body_bytes_received_multi_frames() {
    let (client_io, server_io) = duplex(65536);

    // Server: respond with two DATA frames "a" and "bc"
    tokio::spawn(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        if let Some(result) = conn.accept().await {
            let (req, mut send_resp) = result.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
            let mut send_stream = send_resp.send_response(resp, false).unwrap();
            send_stream.send_data(Bytes::from("a"), false).unwrap();
            send_stream.send_data(Bytes::from("bc"), true).unwrap();
            // Signal graceful shutdown so the accept loop can exit after the client finishes
            conn.graceful_shutdown();
        }
        // Drive the server connection until the client closes
        while let Some(_res) = conn.accept().await {}
    });

    // Client: build Http2Session and read response
    let (send_req, connection) = h2::client::handshake(client_io).await.unwrap();
    let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
    let ping_timeout = Arc::new(AtomicBool::new(false));
    tokio::spawn(async move {
        let _ = connection.await;
        let _ = closed_tx.send(true);
    });

    let digest = Digest::default();
    let conn_ref = crate::connectors::http::v2::ConnectionRef::new(
        send_req.clone(),
        closed_rx,
        ping_timeout,
        0,
        1,
        digest,
    );
    let mut h2s = Http2Session::new(send_req, conn_ref);

    // minimal request
    let mut req = RequestHeader::build("GET", b"/", None).unwrap();
    req.insert_header(http::header::HOST, "example.com")
        .unwrap();
    h2s.write_request_header(Box::new(req), true).unwrap();
    h2s.read_response_header().await.unwrap();

    let mut total = 0;
    while let Some(chunk) = h2s.read_response_body().await.unwrap() {
        total += chunk.len();
    }
    assert_eq!(total, 3);
    assert_eq!(h2s.body_bytes_received(), 3);
}

#[test]
fn h2_response_conflicting_content_length_rejected() {
    let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response
        .append_header(http::header::CONTENT_LENGTH, "5")
        .unwrap();
    response
        .append_header(http::header::CONTENT_LENGTH, "6")
        .unwrap();

    let err = validate_response_header(&response).unwrap_err();
    assert_eq!(err.etype(), &InvalidHTTPHeader);
}

#[test]
fn h2_response_duplicate_identical_content_length_accepted() {
    // RFC 9110 section 8.6 / hyper: identical duplicate (or comma-combined
    // identical) Content-Length values are reconciled to a single value.
    let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response
        .append_header(http::header::CONTENT_LENGTH, "5")
        .unwrap();
    response
        .append_header(http::header::CONTENT_LENGTH, "5")
        .unwrap();
    assert!(validate_response_header(&response).is_ok());

    let mut response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response
        .append_header(http::header::CONTENT_LENGTH, "5, 5")
        .unwrap();
    assert!(validate_response_header(&response).is_ok());
}

/// Build an [`Http2Session`] over `client_io`, wired the way
/// [`crate::connectors::http::v2::handshake`] wires production sessions:
/// with the wire-level END_STREAM watch in front of the socket.
async fn watched_client_session(client_io: tokio::io::DuplexStream) -> Http2Session {
    watched_client_session_with(client_io, |b| b).await
}

/// [`watched_client_session`] with the `h2` client settings tuned, for the
/// tests that need a receive window the peer can overrun.
async fn watched_client_session_with(
    client_io: tokio::io::DuplexStream,
    tune: impl FnOnce(&mut client::Builder) -> &mut client::Builder,
) -> Http2Session {
    use super::super::end_stream_watch::{EndStreamWatch, EndStreamWatchStream};

    let watch = EndStreamWatch::new();
    let io = EndStreamWatchStream::new(client_io, watch.clone());
    let mut builder = client::Builder::new();
    let (send_req, connection) = tune(&mut builder).handshake(io).await.unwrap();
    let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
    let ping_timeout = Arc::new(AtomicBool::new(false));
    tokio::spawn(async move {
        let _ = connection.await;
        let _ = closed_tx.send(true);
    });
    let conn_ref = crate::connectors::http::v2::ConnectionRef::new_with_end_stream_watch(
        send_req.clone(),
        closed_rx,
        ping_timeout,
        0,
        1,
        Digest::default(),
        Some(watch),
    );
    Http2Session::new(send_req, conn_ref)
}

/// Build an [`Http2Session`] over `client_io` and start its connection task.
///
/// Deliberately WITHOUT the END_STREAM watch, so that the tests below keep
/// exercising the end-of-body proofs they were written for (END_STREAM
/// latched at a poll, and the declared `content-length`) rather than
/// silently passing through source (iv).
async fn client_session(client_io: tokio::io::DuplexStream) -> Http2Session {
    unwatched_client_session_with(client_io, |b| b).await
}

/// [`client_session`] with the `h2` client settings tuned. This is the
/// baseline counterpart of [`watched_client_session_with`].
async fn unwatched_client_session_with(
    client_io: tokio::io::DuplexStream,
    tune: impl FnOnce(&mut client::Builder) -> &mut client::Builder,
) -> Http2Session {
    let mut builder = client::Builder::new();
    let (send_req, connection) = tune(&mut builder).handshake(client_io).await.unwrap();
    let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
    let ping_timeout = Arc::new(AtomicBool::new(false));
    tokio::spawn(async move {
        let _ = connection.await;
        let _ = closed_tx.send(true);
    });
    let conn_ref = crate::connectors::http::v2::ConnectionRef::new(
        send_req.clone(),
        closed_rx,
        ping_timeout,
        0,
        1,
        Digest::default(),
    );
    Http2Session::new(send_req, conn_ref)
}

/// Send request HEADERS *without* END_STREAM and read the response header.
///
/// Leaving the request stream open is what makes these tests reproduce the
/// RFC 9113 §8.1 shape at all: once this side has sent END_STREAM, an
/// inbound RST_STREAM finds the stream in `Closed(Cause::EndStream)` and h2
/// leaves the state alone, so no read ever fails.
async fn send_open_request(h2s: &mut Http2Session) {
    let mut req = RequestHeader::build("POST", b"/", None).unwrap();
    req.insert_header(http::header::HOST, "example.com")
        .unwrap();
    h2s.write_request_header(Box::new(req), false).unwrap();
    h2s.read_response_header().await.unwrap();
}

/// Give the connection task a chance to process a peer reset.
///
/// Newer `h2` releases may preserve an already-observed END_STREAM instead
/// of overwriting it with a later reset, so the raw receive state is not a
/// portable acknowledgement. The behavioral assertion after this grace
/// period is the contract that matters: the completed response stays a
/// clean EOF under either internal representation.
async fn await_reset_processed(_h2s: &Http2Session) {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// NOTE: there is deliberately no test here that races `write_request_header`
// against a fast peer's response. One existed, and it was theatre: the gap
// it claimed to cover is the handful of nanoseconds between `send_request`
// returning and the next statement, against a round trip that takes
// microseconds even over an in-memory duplex, so it passed 300/300 runs with
// the registration lock removed. The invariant it was meant to protect --
// "a scan that runs while a registration is being taken must block, not drop
// the record" -- is pinned deterministically instead, by
// `end_stream_watch::tests::a_scan_waits_for_an_in_progress_registration`.

/// A complete response followed by RFC 9113 §8.1's RST_STREAM(NO_ERROR)
/// -- the upstream asking us to stop uploading a request it no longer
/// needs -- must read as a clean end of body, not as a `ReadError`. The
/// response is already in hand; failing it would discard a complete
/// response over a protocol-sanctioned signal.
#[tokio::test]
async fn h2_response_body_no_error_reset_after_eos_is_end_of_body() {
    let (client_io, server_io) = duplex(65536);
    let (reset_tx, reset_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        // A complete response body, END_STREAM included.
        send_stream.send_data(Bytes::from("hello"), true).unwrap();

        // Drive the connection until the client has the whole body, so the
        // reset is unambiguously post-EOF.
        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = reset_rx => {}
        }
        send_stream.send_reset(Reason::NO_ERROR);
        while conn.accept().await.is_some() {}
    });

    let mut h2s = client_session(client_io).await;
    send_open_request(&mut h2s).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
    assert!(h2s.response_finished());

    reset_tx.send(()).unwrap();
    await_reset_processed(&h2s).await;

    assert!(
        h2s.read_response_body()
            .await
            .expect("a post-EOF NO_ERROR reset must not surface as a read error")
            .is_none(),
        "the response body already ended"
    );
    assert_eq!(h2s.body_bytes_received(), 5);
}

/// The same shape in the NATURAL wire ordering: the upstream flushes the
/// complete response AND the RST_STREAM before this side polls the body at
/// all. No oneshot forces the reset to happen after the reader observed EOF,
/// which is what makes this the case real traffic produces -- and the case
/// in which older `h2` releases had already overwritten the stream state.
/// Supported h2 0.4.19 preserves received END_STREAM, while this regression
/// still verifies that Pingora classifies the body correctly without depending
/// on a particular private reset representation.
///
/// The sleep before the reset is required, not cosmetic: `h2`'s
/// `send_reset` CLEARS the stream's pending send queue, so resetting
/// immediately would drop the DATA frame this test is about.
#[tokio::test]
async fn h2_response_body_reset_before_any_read_is_end_of_body() {
    let (client_io, server_io) = duplex(65536);

    tokio::spawn(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder()
            .status(StatusCode::OK)
            .header("content-length", "5")
            .body(())
            .unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        send_stream.send_data(Bytes::from("hello"), true).unwrap();

        // Drive the connection long enough to flush the DATA frame, then
        // reset without waiting for the peer to have read anything.
        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        send_stream.send_reset(Reason::NO_ERROR);
        while conn.accept().await.is_some() {}
    });

    let mut h2s = client_session(client_io).await;
    send_open_request(&mut h2s).await;
    // Nothing is read until both frames have been processed.
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
    assert!(
        h2s.read_response_body()
            .await
            .expect(
                "a fully received content-length body must read as a clean EOF even when \
                 the peer reset before we polled"
            )
            .is_none(),
        "the response body already ended"
    );
    assert_eq!(h2s.body_bytes_received(), 5);
}

/// The mirror image: the same RST_STREAM(NO_ERROR) arriving *before*
/// END_STREAM means the response body was truncated. Reporting it as a
/// clean end of body would hand the truncation to the downstream client,
/// so it must stay an error.
#[tokio::test]
async fn h2_response_body_no_error_reset_before_eos_is_an_error() {
    let (client_io, server_io) = duplex(65536);
    let (reset_tx, reset_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        // A partial body: no END_STREAM, then the stream is reset.
        send_stream.send_data(Bytes::from("hel"), false).unwrap();

        // Reset only once the client holds the partial body, so the test
        // exercises the body read rather than the header read.
        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = reset_rx => {}
        }
        send_stream.send_reset(Reason::NO_ERROR);
        while conn.accept().await.is_some() {}
    });

    let mut h2s = client_session(client_io).await;
    send_open_request(&mut h2s).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hel");
    assert!(!h2s.response_finished());
    reset_tx.send(()).unwrap();

    // Blocks until the reset lands, so no sleep is needed to make this
    // deterministic: the receive half is still open until then.
    let err = h2s
        .read_response_body()
        .await
        .expect_err("a truncated response body must not read as a clean EOF");
    assert_eq!(err.etype(), &ReadError);
}

/// The pre-existing trailer classification keeps working now that it is
/// guarded by the same end-of-body proof: the guard must not be so strict
/// that it re-breaks the case it was written for.
#[tokio::test]
async fn h2_trailers_no_error_reset_after_eos_is_benign() {
    let (client_io, server_io) = duplex(65536);
    let (reset_tx, reset_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        send_stream.send_data(Bytes::from("hello"), true).unwrap();

        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = reset_rx => {}
        }
        send_stream.send_reset(Reason::NO_ERROR);
        while conn.accept().await.is_some() {}
    });

    let mut h2s = client_session(client_io).await;
    send_open_request(&mut h2s).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
    // Reach the end of body before the reset lands, which is the ordering
    // the trailer read has always relied on.
    assert!(h2s.read_response_body().await.unwrap().is_none());

    reset_tx.send(()).unwrap();
    await_reset_processed(&h2s).await;

    assert!(h2s
        .read_trailers()
        .await
        .expect("a post-EOF NO_ERROR reset must not surface as a trailer read error")
        .is_none());
}

/// Serve RFC 9113 §8.1's shape: a response with NO `content-length` whose
/// body either does or does not carry END_STREAM, followed by
/// RST_STREAM(NO_ERROR) once those frames have flushed.
///
/// The sleep before the reset is required, not cosmetic: `h2`'s
/// `send_reset` CLEARS the stream's pending send queue, so resetting
/// immediately would drop the DATA frame these tests are about.
fn serve_body_then_no_error_reset(
    server_io: tokio::io::DuplexStream,
    body: &'static str,
    end_stream: bool,
) {
    tokio::spawn(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        send_stream
            .send_data(Bytes::from(body), end_stream)
            .unwrap();

        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        send_stream.send_reset(Reason::NO_ERROR);
        while conn.accept().await.is_some() {}
    });
}

/// Historical motivation for the watch: before h2 0.4.16, an upstream that
/// sent a complete response with no `content-length` and then
/// RST_STREAM(NO_ERROR) could lose the decoded END_STREAM state while the
/// request upload was still open. Supported h2 0.4.19 preserves that state;
/// this contract now proves the fork still delivers the natural wire ordering
/// without depending on one private reset representation. The watcher remains
/// independently responsible for byte parity and other evidence boundaries.
#[tokio::test]
async fn h2_watched_complete_body_reset_while_uploading_is_end_of_body() {
    let (client_io, server_io) = duplex(65536);
    serve_body_then_no_error_reset(server_io, "hello", true);

    let mut h2s = watched_client_session(client_io).await;
    send_open_request(&mut h2s).await;
    // Nothing is read until both frames have been processed: the natural wire
    // ordering that must work under both historical and current h2 behavior.
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");

    // No declared length can prove completion; either the wire watch or an
    // `h2` release that preserves the already-observed END_STREAM must do so.
    assert!(
        h2s.response_body_declared_len.is_none(),
        "the response must not declare a content-length"
    );

    assert!(
        h2s.read_response_body()
            .await
            .expect(
                "a complete response ended with RST_STREAM(NO_ERROR) while the request was \
                 still uploading must read as a clean EOF"
            )
            .is_none(),
        "the response body already ended"
    );
    assert_eq!(h2s.body_bytes_received(), 5);

    // The trailer read that follows a finished body must not undo it.
    assert!(h2s
        .read_trailers()
        .await
        .expect("the trailer read must not resurrect the reset as an error")
        .is_none());
}

/// The direction that matters: the SAME shape with the body truncated --
/// the peer never flagged END_STREAM before resetting -- must still be an
/// error. Handing this to the downstream client as a complete response is
/// precisely what the guard exists to prevent, and the wire watch must not
/// weaken it.
#[tokio::test]
async fn h2_watched_truncated_body_reset_while_uploading_is_an_error() {
    let (client_io, server_io) = duplex(65536);
    serve_body_then_no_error_reset(server_io, "hel", false);

    let mut h2s = watched_client_session(client_io).await;
    send_open_request(&mut h2s).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hel");
    assert!(!h2s.response_finished());

    let err = h2s
        .read_response_body()
        .await
        .expect_err("a truncated response body must not read as a clean EOF");
    assert_eq!(err.etype(), &ReadError);

    // And the trailer read must not launder it into "no trailers" either.
    h2s.read_trailers()
        .await
        .expect_err("a truncated response body must not read as a clean trailer EOF");
}

/// A raw HTTP/2 origin, for the shapes `h2`'s own server API refuses to
/// produce (a `content-length` its body does not honour) and for the ONE
/// wire timing that makes them dangerous.
///
/// The timing is the whole trick and is not incidental: the response frames
/// and the RST_STREAM have to reach `h2` in a SINGLE burst. `h2`'s
/// connection loop flushes its send queue between frames
/// (`Connection::poll2` calls `poll_ready` before every `recv_frame`), so a
/// reset that arrives even one poll later finds the RST_STREAM `h2` queued
/// for its own PROTOCOL_ERROR already gone, `is_pending_send` false, and
/// `State::recv_reset` takes its `Closed(..) if !queued` no-op arm -- the
/// error stays LOCAL and nothing can launder it. Arriving in the same burst
/// it finds the queue non-empty and OVERWRITES the local close with a
/// REMOTE `NO_ERROR`, which is exactly what
/// [`benign_post_eof_stream_end`] accepts.
fn serve_raw_burst_then_no_error_reset(
    mut server_io: tokio::io::DuplexStream,
    response_frames: Vec<u8>,
) -> tokio::sync::oneshot::Receiver<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (flushed_tx, flushed_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        // Empty SETTINGS, then the ACK of whatever the client sent.
        server_io
            .write_all(&raw_frame(0x4, 0, 0, b""))
            .await
            .unwrap();
        server_io
            .write_all(&raw_frame(0x4, 0x1, 0, b""))
            .await
            .unwrap();
        // Wait for a complete request HEADERS frame on stream 1 before
        // answering. Do not search arbitrary byte windows: SETTINGS
        // payloads may contain the HEADERS frame-type byte by chance.
        let mut seen = Vec::new();
        loop {
            let Ok(n) = server_io.read(&mut buf).await else {
                return;
            };
            if n == 0 {
                return;
            }
            seen.extend_from_slice(&buf[..n]);
            if contains_request_headers_on_stream_one(&seen) {
                break;
            }
        }
        let mut burst = response_frames;
        burst.extend_from_slice(&raw_frame(0x3, 0, 1, &[0, 0, 0, 0]));
        server_io.write_all(&burst).await.unwrap();
        server_io.flush().await.unwrap();
        let _ = flushed_tx.send(());
        while server_io.read(&mut buf).await.unwrap_or(0) != 0 {}
    });
    flushed_rx
}

#[derive(Clone, Copy, Debug)]
enum EndStreamObservation {
    Watched,
    Unwatched,
}

/// Open a request against a hand-written response whose complete frame
/// sequence and following NO_ERROR reset are processed before the first
/// body poll. This is the ordering that used to let h2's reset state hide
/// the terminal-frame result.
async fn raw_reset_session(
    response_frames: Vec<u8>,
    observation: EndStreamObservation,
) -> Http2Session {
    let (client_io, server_io) = duplex(65536);
    let flushed = serve_raw_burst_then_no_error_reset(server_io, response_frames);
    let mut h2s = match observation {
        EndStreamObservation::Watched => watched_client_session(client_io).await,
        EndStreamObservation::Unwatched => client_session(client_io).await,
    };
    send_open_request(&mut h2s).await;
    flushed
        .await
        .expect("the raw origin must flush the terminal response burst");
    h2s
}

const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn contains_request_headers_on_stream_one(bytes: &[u8]) -> bool {
    if !bytes.starts_with(CLIENT_PREFACE) {
        return false;
    }
    let mut offset = CLIENT_PREFACE.len();
    while bytes.len() >= offset + 9 {
        let len = ((bytes[offset] as usize) << 16)
            | ((bytes[offset + 1] as usize) << 8)
            | bytes[offset + 2] as usize;
        let end = offset + 9 + len;
        if bytes.len() < end {
            return false;
        }
        let frame_type = bytes[offset + 3];
        let stream_id =
            u32::from_be_bytes(bytes[offset + 5..offset + 9].try_into().unwrap()) & 0x7fff_ffff;
        if frame_type == 0x1 && stream_id == 1 {
            return true;
        }
        offset = end;
    }
    false
}

/// An HTTP/2 frame, built by hand.
fn raw_frame(frame_type: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let id = stream_id.to_be_bytes();
    let mut v = vec![
        (len >> 16) as u8,
        (len >> 8) as u8,
        len as u8,
        frame_type,
        flags,
        id[0],
        id[1],
        id[2],
        id[3],
    ];
    v.extend_from_slice(payload);
    v
}

/// `:status: 200` (HPACK static index 8) plus `content-length: 10` as a
/// literal field without indexing whose NAME is static index 28.
const RESP_200_CONTENT_LENGTH_10: &[u8] = &[0x88, 0x0F, 0x0D, 0x02, b'1', b'0'];
const RESP_200_CONTENT_LENGTH_5: &[u8] = &[0x88, 0x0F, 0x0D, 0x01, b'5'];

/// HPACK static status-code entries used by the hand-written response
/// sequences below. 100 and 103 have no static value and are literal
/// values under the indexed `:status` name (static index 8).
const RESP_200: &[u8] = &[0x88];
const RESP_100: &[u8] = &[0x08, 0x03, b'1', b'0', b'0'];
const RESP_103: &[u8] = &[0x08, 0x03, b'1', b'0', b'3'];

/// `x-trailer: 1` as a literal field without indexing with a new name.
const TRAILER_BLOCK: &[u8] = &[
    0x00, 0x09, b'x', b'-', b't', b'r', b'a', b'i', b'l', b'e', b'r', 0x01, b'1',
];

fn response_with_trailers(body: &[u8], trailers: &[u8]) -> Vec<u8> {
    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200);
    if !body.is_empty() {
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, body));
    }
    frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, trailers));
    frames
}

async fn assert_clean_response(
    mut h2s: Http2Session,
    expected_body: Option<&'static [u8]>,
    expected_trailers: Option<HeaderMap>,
) {
    match expected_body {
        Some(expected) => {
            assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), expected)
        }
        None => assert!(h2s.read_response_body().await.unwrap().is_none()),
    }
    assert!(
        h2s.read_response_body().await.unwrap().is_none(),
        "the response must remain at clean EOF"
    );
    assert_eq!(h2s.read_trailers().await.unwrap(), expected_trailers);
}

/// Which side of the upstream fix the resolved `h2` release is on, for the
/// unwatched baselines below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DependencyBaseline {
    /// `h2` hands the invalid terminal block back as a success, so a
    /// following reset can pass it off as a clean end of body.
    Laundered,
    /// `h2` rejects the invalid terminal block itself, and the rejection
    /// reaches the caller as an error that the fail-closed latch keeps.
    Rejected,
    /// `h2` rejects the block, but the same-burst NO_ERROR reset overwrites
    /// the local PROTOCOL_ERROR and a satisfied `content-length` lets
    /// `read_trailers()` answer "this response had no trailers".
    ///
    /// Deferred, not a regression: an unwatched session holds no evidence
    /// that a terminal block was ever sent, and even the watched direct
    /// read does not latch this yet -- see the `#[ignore]`d
    /// `h2_watched_direct_trailer_read_latches_invalid_terminal_headers`
    /// and H2-004.
    RejectedThenReportedAsNoTrailers,
}

/// Report, without failing, which `h2` behavior the unwatched baselines saw.
///
/// These baselines characterize the DEPENDENCY, not a Pingora contract:
/// `h2 = ">=0.4.19"` has an open upper bound, so an upstream fix must show
/// up as a reported behavior change rather than as a product regression. The
/// required contract -- that the adapter never launders an unvalidated
/// terminal block into a completed response -- is asserted by the
/// `h2_watched_*` siblings, and stays strict under every outcome below.
fn report_dependency_baseline(scenario: &str, observed: DependencyBaseline) {
    eprintln!("h2 dependency baseline: {scenario} = {observed:?}");
}

async fn assert_clean_empty_trailers(mut h2s: Http2Session, expected_body: Option<&'static [u8]>) {
    match expected_body {
        Some(expected) => {
            assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), expected);
            assert!(h2s.read_response_body().await.unwrap().is_none());
        }
        None => assert!(h2s.read_response_body().await.unwrap().is_none()),
    }
    assert!(
        h2s.read_trailers()
            .await
            .unwrap()
            .is_some_and(|trailers| trailers.is_empty()),
        "an explicitly validated empty trailer block must remain distinguishable from None"
    );
}

/// Reading body EOF again must not re-poll h2 and discard an already
/// validated empty trailer map. The narrowed client will short-circuit on
/// its EOF/trailer latch before touching the reader again.
#[tokio::test]
#[ignore = "current repeated body EOF poll can consume validated empty trailers"]
async fn h2_repeated_body_eof_preserves_validated_empty_trailers() {
    let mut h2s = raw_reset_session(
        response_with_trailers(&[], &[]),
        EndStreamObservation::Unwatched,
    )
    .await;
    assert!(h2s.read_response_body().await.unwrap().is_none());
    assert!(h2s.read_response_body().await.unwrap().is_none());
    assert!(h2s
        .read_trailers()
        .await
        .unwrap()
        .is_some_and(|trailers| trailers.is_empty()));
}

/// h2 itself accepts an empty terminal trailer block both with and without
/// DATA. The adapter must not reinterpret `Some(empty)` as an invalid
/// terminal block merely because DATA preceded it.
#[tokio::test]
async fn h2_unwatched_valid_empty_trailers_are_clean_with_or_without_data() {
    for body in [&b""[..], &b"hello"[..]] {
        let h2s = raw_reset_session(
            response_with_trailers(body, &[]),
            EndStreamObservation::Unwatched,
        )
        .await;
        assert_clean_empty_trailers(h2s, (!body.is_empty()).then_some(&b"hello"[..])).await;
    }
}

/// Target contract after h2 can distinguish a validated empty trailer map
/// from a pseudo-header block whose forbidden fields were discarded. The
/// current fail-closed adapter rejects both shapes, so this remains deferred.
#[tokio::test]
#[ignore = "current fail-closed adapter cannot accept validated empty trailers"]
async fn h2_watched_valid_empty_trailers_are_clean_with_or_without_data() {
    for body in [&b""[..], &b"hello"[..]] {
        let h2s = raw_reset_session(
            response_with_trailers(body, &[]),
            EndStreamObservation::Watched,
        )
        .await;
        assert_clean_empty_trailers(h2s, (!body.is_empty()).then_some(&b"hello"[..])).await;
    }
}

#[tokio::test]
async fn h2_valid_nonempty_trailers_survive_a_pre_poll_reset() {
    let mut trailers = HeaderMap::new();
    trailers.insert("x-trailer", "1".parse().unwrap());
    for observation in [
        EndStreamObservation::Unwatched,
        EndStreamObservation::Watched,
    ] {
        let h2s =
            raw_reset_session(response_with_trailers(b"hello", TRAILER_BLOCK), observation).await;
        assert_clean_response(h2s, Some(b"hello"), Some(trailers.clone())).await;
    }
}

/// Terminal HEADERS must remain observable even when the response has no
/// DATA, or h2's following reset can launder this codec error into clean EOF.
#[tokio::test]
async fn h2_watched_zero_data_invalid_trailers_remain_an_error() {
    let mut h2s = raw_reset_session(
        response_with_trailers(&[], RESP_200),
        EndStreamObservation::Watched,
    )
    .await;
    let err = h2s
        .read_response_body()
        .await
        .expect_err("a response pseudo-header in trailers must remain an error");
    assert_eq!(err.etype(), &ReadError);
    h2s.read_trailers()
        .await
        .expect_err("the invalid trailer result must stay latched");
}

#[tokio::test]
async fn h2_watched_poll_zero_data_invalid_trailers_remain_an_error() {
    let mut h2s = raw_reset_session(
        response_with_trailers(&[], RESP_200),
        EndStreamObservation::Watched,
    )
    .await;
    let err = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
        .await
        .expect("the terminal body poll must report the trailer failure")
        .expect_err("a response pseudo-header in trailers must remain an error");
    assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
    assert!(h2s.response_body_error);
    h2s.read_trailers()
        .await
        .expect_err("the invalid trailer result must stay latched");
}

/// The h2-only baseline for why a terminal-HEADERS observation is still
/// required after DATA accounting is removed: with no watcher, the reset
/// overwrites the codec error and the invalid zero-DATA trailers can look
/// exactly like a clean body end.
///
/// This characterizes the dependency, so it accepts BOTH sides of the
/// upstream fix and reports which one it saw. Requiring the laundering
/// would turn the day `h2` starts rejecting these trailers into a red CI
/// run that reads like a Pingora regression -- and the obvious way to
/// silence that is to pin a known-vulnerable `h2` or delete the evidence
/// for the watcher. What must not move is the fail-closed contract, so the
/// rejecting arm still requires the trailer read to keep failing. It
/// asserts that outcome rather than a mechanism: with no `content-length`
/// and no DATA, `EndOfBodyProof::holds()` is false either way, so it does
/// not matter whether the `response_body_error` latch or the proof guard
/// in `read_trailers` is what holds the door shut.
#[tokio::test]
async fn h2_unwatched_zero_data_invalid_trailers_dependency_baseline() {
    let mut h2s = raw_reset_session(
        response_with_trailers(&[], RESP_200),
        EndStreamObservation::Unwatched,
    )
    .await;
    let observed = match h2s.read_response_body().await {
        Ok(None) => {
            assert!(
                h2s.read_response_body().await.unwrap().is_none(),
                "the laundered response must remain at clean EOF"
            );
            // Either laundered shape is the dependency's to pick: no
            // trailers at all, or the discarded block as an empty map.
            // Surfacing the illegal fields would be a different defect.
            match h2s.read_trailers().await.unwrap() {
                None => {}
                Some(trailers) => assert!(
                    trailers.is_empty(),
                    "a laundered pseudo-header block must not surface fields: {trailers:?}"
                ),
            }
            DependencyBaseline::Laundered
        }
        Ok(Some(body)) => panic!("a zero-DATA response must not yield body bytes: {body:?}"),
        Err(_) => {
            h2s.read_trailers()
                .await
                .expect_err("a rejected terminal block must keep the trailer read failing");
            DependencyBaseline::Rejected
        }
    };
    report_dependency_baseline("unwatched zero-DATA invalid trailers", observed);
}

#[tokio::test]
async fn h2_header_only_response_survives_a_pre_poll_reset() {
    let frames = raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200);
    for observation in [
        EndStreamObservation::Watched,
        EndStreamObservation::Unwatched,
    ] {
        let h2s = raw_reset_session(frames.clone(), observation).await;
        assert_clean_response(h2s, None, None).await;
    }
}

/// Delay the first response-future poll until after the origin has flushed
/// both header-only EOS and its reset. Supported h2 >= 0.4.19 preserves received EOS
/// across that reset; the client must latch it when the queued final response
/// is eventually accepted instead of mistaking the wire marker for trailers.
#[tokio::test]
async fn h2_header_only_response_latches_eos_after_delayed_header_poll() {
    let (client_io, server_io) = duplex(65536);
    let frames = raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200);
    let flushed = serve_raw_burst_then_no_error_reset(server_io, frames);
    let mut h2s = watched_client_session(client_io).await;

    let mut req = RequestHeader::build("POST", b"/", None).unwrap();
    req.insert_header(http::header::HOST, "example.com")
        .unwrap();
    h2s.write_request_header(Box::new(req), false).unwrap();
    flushed
        .await
        .expect("the raw origin must flush header-only EOS and reset");
    await_reset_processed(&h2s).await;

    h2s.read_response_header().await.unwrap();
    assert!(
        h2s.response_initial_end_stream,
        "received initial EOS must survive a reset before the first header poll"
    );
    assert_clean_response(h2s, None, None).await;
}

fn prepend_informational(prefixes: &[&[u8]], mut final_frames: Vec<u8>) -> Vec<u8> {
    let mut frames = Vec::new();
    for block in prefixes {
        frames.extend_from_slice(&raw_frame(0x1, 0x4, 1, block));
    }
    frames.append(&mut final_frames);
    frames
}

/// Informational responses are consumed by h2's response future and must
/// not affect how the adapter classifies the final response. Exercise one
/// and multiple prefixes across every currently unambiguous terminal shape.
#[tokio::test]
async fn h2_informational_prefixes_preserve_final_response_shapes() {
    let mut nonempty = HeaderMap::new();
    nonempty.insert("x-trailer", "1".parse().unwrap());

    let cases = [
        (
            vec![RESP_100],
            raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200),
            None,
            None,
        ),
        (
            vec![RESP_100, RESP_103],
            {
                let mut frames = raw_frame(0x1, 0x4, 1, RESP_200);
                frames.extend_from_slice(&raw_frame(0x0, FLAG_END_STREAM_RAW, 1, b"hello"));
                frames
            },
            Some(&b"hello"[..]),
            None,
        ),
        (
            vec![RESP_100, RESP_103],
            response_with_trailers(b"hello", TRAILER_BLOCK),
            Some(&b"hello"[..]),
            Some(nonempty),
        ),
    ];

    for observation in [
        EndStreamObservation::Watched,
        EndStreamObservation::Unwatched,
    ] {
        for (prefixes, final_frames, body, trailers) in &cases {
            let frames = prepend_informational(prefixes, final_frames.clone());
            let h2s = raw_reset_session(frames, observation).await;
            assert_clean_response(h2s, *body, trailers.clone()).await;
        }
    }
}

/// Deferred with the other valid-empty controls: the current h2 public API
/// cannot distinguish this from a trailer pseudo-header block whose fields
/// were discarded before a same-burst reset.
#[tokio::test]
#[ignore = "requires decoder-level rejection before valid empty trailers can be accepted"]
async fn h2_watched_informational_prefix_preserves_valid_empty_trailers() {
    let frames = prepend_informational(&[RESP_103], response_with_trailers(&[], &[]));
    let h2s = raw_reset_session(frames, EndStreamObservation::Watched).await;
    assert_clean_empty_trailers(h2s, None).await;
}

/// An informational response cannot end the stream. This must fail while
/// reading the response header; neither a terminal-frame observation nor a
/// following NO_ERROR reset may turn it into a final response.
#[tokio::test]
async fn h2_informational_end_stream_is_a_header_error() {
    let frames = raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_100);
    for observation in [
        EndStreamObservation::Watched,
        EndStreamObservation::Unwatched,
    ] {
        let (client_io, server_io) = duplex(65536);
        let _ = serve_raw_burst_then_no_error_reset(server_io, frames.clone());
        let mut h2s = match observation {
            EndStreamObservation::Watched => watched_client_session(client_io).await,
            EndStreamObservation::Unwatched => client_session(client_io).await,
        };

        let mut req = RequestHeader::build("POST", b"/", None).unwrap();
        req.insert_header(http::header::HOST, "example.com")
            .unwrap();
        h2s.write_request_header(Box::new(req), false).unwrap();
        h2s.read_response_header()
            .await
            .expect_err("END_STREAM on an informational response must fail the response future");
    }
}

/// C1: a response that declares `content-length: 10`, sends 5 bytes, then
/// puts END_STREAM on a frame `h2` THROWS AWAY.
///
/// `Recv::recv_data` checks `ensure_content_length_zero()` before pushing
/// the payload onto `pending_recv`, so the last frame's bytes are decoded
/// and discarded and `h2` closes the stream locally with PROTOCOL_ERROR.
/// The peer's RST_STREAM(NO_ERROR), arriving in the same burst, overwrites
/// that local close with a remote `NO_ERROR`.
///
/// So every ingredient of "a clean end of body" is present: the wire
/// carried END_STREAM (source (iv)'s flag is set), and the error is a
/// remote `NO_ERROR` reset. Only the BYTE COUNT dissents -- the wire
/// carried 7 payload bytes and the reader received 5 -- and only the
/// unsatisfied `content-length` dissents alongside it. Without both, this
/// delivers 5 of a declared 10 bytes as a complete 200 and admits it to
/// cache.
#[tokio::test]
async fn h2_watched_content_length_underflow_reset_is_not_a_clean_eof() {
    let (client_io, server_io) = duplex(65536);
    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_10);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(0x0, FLAG_END_STREAM_RAW, 1, b"xy"));
    let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

    let mut h2s = watched_client_session(client_io).await;
    send_open_request(&mut h2s).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
    assert_eq!(h2s.body_bytes_received(), 5);

    // The premises. If either stops holding, this test has stopped
    // reproducing the defect and is passing for the wrong reason.
    assert_eq!(h2s.response_body_declared_len, Some(10));
    assert!(
        h2s.peer_end_stream.observed(),
        "the wire DID carry END_STREAM -- the flag alone would say 'complete'"
    );

    let err = h2s.read_response_body().await.expect_err(
        "5 of a declared 10 bytes must not read as a clean EOF just because the \
         frame h2 discarded happened to carry END_STREAM",
    );
    assert_eq!(err.etype(), &ReadError);
    // ... and the reset really was the remote NO_ERROR that
    // `benign_post_eof_stream_end` accepts, i.e. the flag was the only thing
    // standing between this response and a clean EOF.
    assert!(
        format!("{err:?}").contains("NO_ERROR, Remote"),
        "the underlying h2 error must be the remote NO_ERROR reset: {err:?}"
    );

    h2s.read_trailers()
        .await
        .expect_err("nor may the trailer read launder it into 'no trailers'");
}

/// The same laundering one drop path over: TRAILERS rejected because the
/// stream still owes `content-length` bytes.
///
/// `Recv::recv_trailers` errors out before queueing the trailers, again
/// with a local PROTOCOL_ERROR that the burst's RST_STREAM(NO_ERROR)
/// overwrites. No DATA is dropped here, so the byte counts AGREE (5 on the
/// wire, 5 read) -- this is the shape a byte count alone would still
/// launder, and the unsatisfied `content-length` is what rejects it. Both
/// halves of `EndOfBodyProof` are load-bearing; this test and the one above
/// pin one each.
#[tokio::test]
async fn h2_watched_rejected_trailers_reset_is_not_a_clean_eof() {
    let (client_io, server_io) = duplex(65536);
    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_10);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, TRAILER_BLOCK));
    let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

    let mut h2s = watched_client_session(client_io).await;
    send_open_request(&mut h2s).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
    assert_eq!(h2s.body_bytes_received(), 5);
    assert_eq!(h2s.response_body_declared_len, Some(10));
    assert!(
        !h2s.peer_end_stream.observed(),
        "unvalidated trailers must not publish clean-EOF evidence"
    );
    assert!(h2s.peer_end_stream.terminal_headers_observed());

    let err = h2s.read_response_body().await.expect_err(
        "5 of a declared 10 bytes must not read as a clean EOF just because the \
         rejected trailers carried END_STREAM",
    );
    assert_eq!(err.etype(), &ReadError);
    assert!(
        format!("{err:?}").contains("NO_ERROR, Remote"),
        "the underlying h2 error must be the remote NO_ERROR reset: {err:?}"
    );

    h2s.read_trailers()
        .await
        .expect_err("nor may the trailer read launder it into 'no trailers'");
}

/// A trailer block containing a response pseudo-header is invalid even
/// when its HEADERS frame carries END_STREAM. A following remote NO_ERROR
/// reset must not turn that codec rejection into a successful EOF.
#[tokio::test]
async fn h2_watched_invalid_trailers_reset_is_not_a_clean_eof() {
    let (client_io, server_io) = duplex(65536);
    let mut frames = raw_frame(0x1, 0x4, 1, &[0x88]);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    // HPACK static index 8 is `:status: 200`, which is illegal in trailers.
    frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, &[0x88]));
    let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

    let mut h2s = watched_client_session(client_io).await;
    send_open_request(&mut h2s).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
    assert_eq!(h2s.body_bytes_received(), 5);
    assert!(h2s.response_body_declared_len.is_none());
    assert!(
        !h2s.peer_end_stream.observed(),
        "unvalidated trailer END_STREAM must not be completion evidence"
    );
    assert!(
        h2s.peer_end_stream.terminal_headers_observed(),
        "the wire watcher must retain the unvalidated terminal trailers"
    );

    let err = h2s
        .read_response_body()
        .await
        .expect_err("invalid trailers must remain a response-body error");
    assert_eq!(err.etype(), &ReadError);
    h2s.read_trailers()
        .await
        .expect_err("invalid trailers must remain a trailer-read error");
}

/// A caller may consume exactly the declared body bytes and then ask for
/// trailers without issuing a body EOF read, and this pins what that API
/// order yields for an illegal terminal pseudo-header block.
///
/// Like the zero-DATA baseline above this characterizes the dependency, so
/// it accepts every outcome the dependency may produce and reports which
/// one it saw. All three are dependency shapes, not product verdicts:
/// today's `h2` launders the block into an empty map, while an `h2` that
/// rejects it may surface either an error or -- because the same-burst
/// NO_ERROR reset overwrites the local PROTOCOL_ERROR and the declared
/// `content-length` is satisfied -- a plain "no trailers". Closing that
/// last gap needs the terminal-HEADERS state consulted from the direct
/// read, which is H2-004's deferred work and is pinned by the `#[ignore]`d
/// `h2_watched_direct_trailer_read_latches_invalid_terminal_headers`.
/// Failing here instead would file that known gap as a fresh regression on
/// the day the dependency gets safer. What stays asserted is that the
/// fields of an illegal block never reach the caller.
#[tokio::test]
async fn h2_unwatched_direct_trailer_read_invalid_terminal_headers_dependency_baseline() {
    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200));

    let mut h2s = raw_reset_session(frames.clone(), EndStreamObservation::Unwatched).await;
    let observed = match h2s.read_response_body().await {
        Ok(Some(body)) => {
            assert_eq!(body, "hello");
            match h2s.read_trailers().await {
                Ok(Some(trailers)) => {
                    assert!(
                        trailers.is_empty(),
                        "a laundered pseudo-header block must not surface fields: {trailers:?}"
                    );
                    DependencyBaseline::Laundered
                }
                Ok(None) => {
                    // Corroborate that the DEPENDENCY is what rejected the
                    // block, so that weakening a guard in `read_trailers`
                    // cannot pass itself off as an upstream fix and be
                    // absorbed by this arm. Replaying the same frames in
                    // body-EOF order forces the rejection into the open:
                    // `read_response_body`'s EOF branch validates the
                    // trailers itself instead of consulting those guards.
                    let mut replay =
                        raw_reset_session(frames.clone(), EndStreamObservation::Unwatched).await;
                    assert_eq!(replay.read_response_body().await.unwrap().unwrap(), "hello");
                    replay.read_response_body().await.expect_err(
                        "'no trailers' here must come from h2 rejecting the block, which \
                         the body-EOF read order surfaces as an error",
                    );
                    DependencyBaseline::RejectedThenReportedAsNoTrailers
                }
                Err(_) => DependencyBaseline::Rejected,
            }
        }
        Ok(None) => panic!("the declared body bytes must still be delivered"),
        Err(_) => {
            h2s.read_trailers()
                .await
                .expect_err("a rejected terminal block must keep the trailer read failing");
            DependencyBaseline::Rejected
        }
    };
    report_dependency_baseline(
        "unwatched direct trailer read of invalid terminal headers",
        observed,
    );
}

/// This is the pseudo-only companion to the mixed-field decoder contract.
/// It deliberately runs without the watcher first: the h2 source itself
/// must reject trailer pseudo-headers before any reset can hide the error.
#[tokio::test]
#[ignore = "requires decoder-level rejection of pseudo-headers in trailers"]
async fn h2_pseudo_only_trailers_never_complete() {
    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200));

    for observation in [
        EndStreamObservation::Unwatched,
        EndStreamObservation::Watched,
    ] {
        let mut h2s = raw_reset_session(frames.clone(), observation).await;
        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert!(
            h2s.read_response_body().await.is_err(),
            "async body EOF must reject pseudo-only trailers for {observation:?}"
        );
        assert!(
            h2s.read_trailers().await.is_err(),
            "direct trailers must observe the same permanent error for {observation:?}"
        );
    }
}

/// This isolates the watched direct-trailer API from Pingora's body-EOF
/// latch. h2 must not queue malformed trailers, and the final client latch
/// must reject the resulting missing trailer event before it becomes EOF.
#[tokio::test]
#[ignore = "requires decoder-level rejection of pseudo-headers in trailers"]
async fn h2_direct_trailer_first_rejects_pseudo_trailers() {
    let mut mixed = RESP_200.to_vec();
    mixed.extend_from_slice(TRAILER_BLOCK);

    for invalid_trailers in [RESP_200, mixed.as_slice()] {
        let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
        frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
        frames.extend_from_slice(&raw_frame(
            0x1,
            0x4 | FLAG_END_STREAM_RAW,
            1,
            invalid_trailers,
        ));

        let mut h2s = raw_reset_session(frames, EndStreamObservation::Watched).await;
        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert!(
            h2s.read_trailers().await.is_err(),
            "direct first trailer read must reject malformed trailers"
        );
    }
}

#[tokio::test]
#[ignore = "current direct read_trailers() does not consult terminal HEADERS state"]
async fn h2_watched_direct_trailer_read_latches_invalid_terminal_headers() {
    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, RESP_200));

    let mut h2s = raw_reset_session(frames, EndStreamObservation::Watched).await;
    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
    for _ in 0..2 {
        let err = h2s
            .read_trailers()
            .await
            .expect_err("invalid terminal headers must stay permanently failed");
        assert_eq!(err.etype(), &ReadError);
    }
}

/// A pseudo-header followed by an ordinary trailer is more dangerous than
/// the pseudo-only case: h2 can discard the pseudo field yet still expose
/// the ordinary field as a nonempty map. Any terminal state machine that
/// trusts `Some(nonempty)` without decoder-level pseudo-header rejection
/// would accept and cache this malformed response.
#[tokio::test]
#[ignore = "requires decoder-level rejection of pseudo-headers in trailers"]
async fn h2_mixed_pseudo_and_regular_trailers_never_complete() {
    let mut invalid_trailers = RESP_200.to_vec();
    invalid_trailers.extend_from_slice(TRAILER_BLOCK);
    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(
        0x1,
        0x4 | FLAG_END_STREAM_RAW,
        1,
        &invalid_trailers,
    ));

    for observation in [
        EndStreamObservation::Unwatched,
        EndStreamObservation::Watched,
    ] {
        let mut h2s = raw_reset_session(frames.clone(), observation).await;
        assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
        assert!(
            h2s.read_response_body().await.is_err(),
            "async body EOF must reject mixed pseudo/regular trailers for {observation:?}"
        );
        assert!(
            h2s.read_trailers().await.is_err(),
            "direct trailers must observe the same permanent error for {observation:?}"
        );
    }
}

/// The decoder rejects the mixed pseudo form before it can become a
/// nonempty map; the terminal-HEADERS marker then prevents the same-burst
/// reset from turning its missing poll-trailer event into EOF.
#[tokio::test]
#[ignore = "requires decoder rejection and final terminal-HEADERS poll latch"]
async fn h2_watched_poll_mixed_pseudo_and_regular_trailers_never_complete() {
    let mut invalid_trailers = RESP_200.to_vec();
    invalid_trailers.extend_from_slice(TRAILER_BLOCK);
    let mut frames = raw_frame(0x1, 0x4, 1, RESP_200_CONTENT_LENGTH_5);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(
        0x1,
        0x4 | FLAG_END_STREAM_RAW,
        1,
        &invalid_trailers,
    ));

    let mut h2s = raw_reset_session(frames, EndStreamObservation::Watched).await;
    let first = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
        .await
        .expect("the first body poll must yield DATA")
        .expect("the first body poll must succeed");
    assert_eq!(first, "hello");

    let terminal = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
        .await
        .expect("the terminal body poll must report a trailer error");
    assert!(
        terminal.is_err(),
        "poll body EOF must reject mixed pseudo/regular trailers"
    );
    assert!(
        h2s.read_trailers().await.is_err(),
        "direct trailers must remain failed after poll error"
    );
}

/// The poll API must latch the same terminal trailer failure as the async
/// API. Otherwise a caller can observe the body error and then retry
/// `read_trailers`, after the failing h2 result has already been consumed.
#[tokio::test]
async fn h2_poll_body_invalid_trailers_latches_the_trailer_error() {
    let (client_io, server_io) = duplex(65536);
    let mut frames = raw_frame(0x1, 0x4, 1, &[0x88]);
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(0x1, 0x4 | FLAG_END_STREAM_RAW, 1, &[0x88]));
    let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

    let mut h2s = watched_client_session(client_io).await;
    send_open_request(&mut h2s).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let first = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
        .await
        .expect("the first body poll must yield DATA")
        .expect("the first body poll must succeed");
    assert_eq!(first, "hello");

    let err = std::future::poll_fn(|cx| h2s.poll_read_response_body(cx))
        .await
        .expect("the terminal body poll must report the trailer failure")
        .expect_err("invalid trailers must remain a poll-body error");
    assert_eq!(err.reason(), Some(Reason::PROTOCOL_ERROR));
    assert!(
        h2s.response_body_error,
        "the poll API must latch the trailer validation failure"
    );

    for _ in 0..2 {
        h2s.read_trailers()
            .await
            .expect_err("a latched trailer failure must remain permanent");
    }
}

/// The half of the guard the two tests above do NOT pin on their own: a
/// response with no `content-length` at all, whose END_STREAM-bearing DATA
/// frame `h2` drops for overrunning the STREAM receive window.
///
/// `Recv::recv_data` raises `library_reset(FLOW_CONTROL_ERROR)` before
/// `pending_recv.push_back`, so the payload never reaches the reader; the
/// burst's RST_STREAM(NO_ERROR) then overwrites the local close. Nothing
/// here declares a length, so `declared_len_satisfied` is vacuously true
/// and only the byte count -- 2005 on the wire against 5 read -- can reject
/// it.
///
/// The connection window is left wide so that the STREAM check is the one
/// that fires; overrunning the connection window is a GOAWAY, a different
/// shape entirely.
#[tokio::test]
async fn h2_watched_flow_control_drop_reset_is_not_a_clean_eof() {
    let (client_io, server_io) = duplex(1 << 20);
    let mut frames = raw_frame(0x1, 0x4, 1, &[0x88]); // `:status: 200`, no content-length
    frames.extend_from_slice(&raw_frame(0x0, 0, 1, b"hello"));
    frames.extend_from_slice(&raw_frame(0x0, FLAG_END_STREAM_RAW, 1, &[b'z'; 2000]));
    let _ = serve_raw_burst_then_no_error_reset(server_io, frames);

    let mut h2s = watched_client_session_with(client_io, |b| {
        b.initial_window_size(1024)
            .initial_connection_window_size(1 << 20)
    })
    .await;
    send_open_request(&mut h2s).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(h2s.read_response_body().await.unwrap().unwrap(), "hello");
    assert_eq!(h2s.body_bytes_received(), 5);
    assert!(
        h2s.response_body_declared_len.is_none(),
        "no content-length may be declared, or the other half of the guard \
         would be doing the work"
    );
    assert!(
        h2s.peer_end_stream.observed(),
        "the wire DID carry END_STREAM -- the flag alone would say 'complete'"
    );

    let err = h2s.read_response_body().await.expect_err(
        "a body h2 dropped for overrunning the receive window must not read as \
         a clean EOF just because the dropped frame carried END_STREAM",
    );
    assert_eq!(err.etype(), &ReadError);
    assert!(
        format!("{err:?}").contains("NO_ERROR, Remote"),
        "the underlying h2 error must be the remote NO_ERROR reset: {err:?}"
    );
}

/// END_STREAM's flag bit, for the hand-built frames above.
const FLAG_END_STREAM_RAW: u8 = 0x1;

// NOTE: the padded-DATA accounting has no end-to-end test here on purpose --
// `h2`'s server API never emits padding, so there is no way to put a padded
// frame on this wire without hand-rolling the whole server side. It is
// pinned at the scanner instead, by
// `end_stream_watch::tests::padding_is_not_counted_as_payload`, which is
// where the arithmetic lives.
