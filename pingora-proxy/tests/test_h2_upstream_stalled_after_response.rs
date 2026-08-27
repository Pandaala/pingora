// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end coverage for an H2 upstream that answers in full and then simply
//! STOPS, without the RST_STREAM(NO_ERROR) of RFC 9113 section 8.1 and without
//! ever granting another byte of request-body flow-control window.
//!
//! `h2` reports "the stream closed" and "the stream was reset", but nothing
//! distinguishes a peer that is about to grant window from one that never will,
//! so a `poll_capacity` wait has no end of its own. The proxy awaits the
//! request-body write INLINE in its duplex loop, so that wait does not merely
//! leak a task: it stops the loop draining the upstream response tasks too, and
//! the client is answered never -- with a complete response sitting in the
//! proxy's own buffer.

mod utils;

use bytes::Bytes;
use http::{Response, StatusCode};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use utils::server_utils::init_without_mock_origin;

/// Big enough that the proxy is provably still writing request body: the origin
/// never reads it, so everything past one h2 flow-control window sits blocked
/// in the pump.
const UPLOAD_LEN: usize = 1024 * 1024;

const RESPONSE_BODY: &str = "the complete response body";

/// Spawn a cleartext-h2 origin that answers ONE request and then goes quiet:
/// it never reads the request body, never resets the stream, and keeps the
/// connection open. One request only.
///
/// `end_stream` picks which half of the discrimination this origin plays:
/// - `true`: the response is COMPLETE, so the wire END_STREAM flag is set and
///   abandoning the upload is safe.
/// - `false`: the response is TRUNCATED -- no END_STREAM ever appears -- so
///   there is no evidence, and the exchange must still fail.
async fn spawn_answer_then_withhold_window_origin(end_stream: bool) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        // Bound the request rather than dropping it: holding the `RecvStream`
        // without polling it is what keeps the stream's receive window shut.
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let resp = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut send_stream = send_resp.send_response(resp, false).unwrap();
        send_stream
            .send_data(Bytes::from(RESPONSE_BODY), end_stream)
            .unwrap();
        // Drive the connection so the response frames flush, but accept nothing
        // and read no body: no RST_STREAM, no WINDOW_UPDATE, ever.
        while conn.accept().await.is_some() {}
    });
    port
}

async fn post_through_proxy(
    port: u16,
    write_timeout_ms: Option<u64>,
    read_timeout_ms: Option<u64>,
) -> reqwest::Result<reqwest::Response> {
    let mut req = reqwest::Client::new()
        .post("http://127.0.0.1:6147/upload")
        .header("x-h2", "true")
        .header("x-port", port.to_string());
    if let Some(ms) = write_timeout_ms {
        req = req.header("x-write-timeout-ms", ms.to_string());
    }
    // Only the truncated case needs one: that origin never ends its response
    // body, so without an upstream read deadline the exchange would be bounded
    // only by the client's own timeout.
    if let Some(ms) = read_timeout_ms {
        req = req.header("x-read-timeout-ms", ms.to_string());
    }
    req.body("x".repeat(UPLOAD_LEN))
        .timeout(Duration::from_secs(60))
        .send()
        .await
}

/// The configured-deadline path, which is what a consumer that sets
/// `peer.options.write_timeout` gets. The expired write window plus the wire
/// END_STREAM flag is the conjunction that says "answered in full, and now
/// withholding capacity", and the complete response must be delivered rather
/// than turned into a 502.
#[tokio::test]
async fn h2_upstream_withholding_capacity_after_a_complete_response_delivers_it() {
    init_without_mock_origin();
    let port = spawn_answer_then_withhold_window_origin(true).await;
    let start = Instant::now();
    let res = post_through_proxy(port, Some(300), None)
        .await
        .expect("the complete response must be delivered, not turned into an error");
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), RESPONSE_BODY);
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "the exchange must complete on the configured write deadline, not hang"
    );
}

/// The same shape with NO deadline configured anywhere, which before the stall
/// probe existed was an unbounded wait: no `write_timeout`, no reset, no window
/// update, and h2 with nothing to report. Bounded completion is the whole
/// assertion -- the probe is what supplies the bound.
#[tokio::test]
async fn h2_upstream_withholding_capacity_is_bounded_without_a_write_timeout() {
    init_without_mock_origin();
    let port = spawn_answer_then_withhold_window_origin(true).await;
    let start = Instant::now();
    let res = post_through_proxy(port, None, None)
        .await
        .expect("an unbounded write must not hold the response hostage forever");
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), RESPONSE_BODY);
    assert!(
        start.elapsed() < Duration::from_secs(40),
        "the stall probe must bound the wait"
    );
}

/// The negative control, and the reason the wire flag is half of the
/// conjunction rather than the whole of it: the very same withheld window
/// against a TRUNCATED response must still fail the exchange. Nothing about a
/// stalled write may be read as evidence that the response is whole.
#[tokio::test]
async fn h2_upstream_withholding_capacity_still_fails_a_truncated_response() {
    init_without_mock_origin();
    let port = spawn_answer_then_withhold_window_origin(false).await;
    match post_through_proxy(port, Some(300), Some(1000)).await {
        // The proxy has already streamed 200 + partial body downstream by the
        // time the stalled write is classified, so the failure surfaces as an
        // incomplete response body rather than as a 502 -- the same shape as
        // `h2_upstream_truncated_response_then_no_error_reset_is_an_error`.
        Ok(res) => {
            assert_eq!(res.status(), StatusCode::OK);
            let body = res.text().await;
            assert!(
                body.is_err(),
                "a response the origin never flagged complete must not read as a \
                 complete body just because the request-body write stalled: {body:?}"
            );
        }
        Err(_) => { /* connection error is an acceptable shape of the same failure */ }
    }
}
