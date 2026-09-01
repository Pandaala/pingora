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
use crate::protocols::http::v1::body::{BodyMode, ParseState};
use http::{HeaderMap, StatusCode};
use pingora_error::ErrorType;
use rstest::rstest;
use std::str;
use tokio_test::io::Builder;

fn init_log() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[tokio::test]
async fn read_basic() {
    init_log();
    let input = b"GET / HTTP/1.1\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let res = http_stream.read_request().await;
    assert_eq!(input.len(), res.unwrap().unwrap());
    assert_eq!(0, http_stream.req_header().headers.len());
}

#[cfg(feature = "patched_http1")]
#[tokio::test]
async fn read_invalid_path() {
    init_log();
    let input = b"GET /\x01\xF0\x90\x80 HTTP/1.1\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let res = http_stream.read_request().await;
    assert_eq!(input.len(), res.unwrap().unwrap());
    assert_eq!(0, http_stream.req_header().headers.len());
    assert_eq!(b"/\x01\xF0\x90\x80", http_stream.get_path());
}

#[tokio::test]
async fn read_2_buf() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\n\r\n";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let res = http_stream.read_request().await;
    assert_eq!(input1.len() + input2.len(), res.unwrap().unwrap());
    assert_eq!(
        input1.len() + input2.len(),
        http_stream.raw_header.as_ref().unwrap().len()
    );
    assert_eq!(1, http_stream.req_header().headers.len());
    assert_eq!(Some(&Method::GET), http_stream.get_method());
    assert_eq!(b"/", http_stream.get_path());
    assert_eq!(Version::HTTP_11, http_stream.req_header().version);

    assert_eq!(b"pingora.org", http_stream.get_header_bytes("Host"));
}

#[tokio::test]
async fn headers_end_stream_is_a_stable_snapshot() {
    init_log();
    // A chunked request with an EMPTY body: framing did not end at the
    // header section, and reading the body to completion must not change
    // that fact (the live parser state would say "empty" afterwards).
    let input1 = b"POST / HTTP/1.1\r\nHost: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n";
    let input2 = b"0\r\n\r\n";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input1[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(!http_stream.request_headers_end_stream());
    assert!(http_stream.read_body_bytes().await.unwrap().is_none());
    assert!(http_stream.is_body_done());
    // `is_body_empty()` now reports true; the transport fact must not.
    assert!(http_stream.is_body_empty());
    assert!(
        !http_stream.request_headers_end_stream(),
        "the headers-end-stream fact must not flip once the body is read"
    );

    // The next request on this keepalive connection re-snapshots.
    http_stream.read_request().await.unwrap();
    assert!(!http_stream.request_headers_end_stream());
}

/// Read the request body to EOF and return it as one string.
async fn drain_h1_request_body(session: &mut HttpSession) -> String {
    let mut body = String::new();
    while let Some(chunk) = session.read_body_bytes().await.unwrap() {
        body.push_str(&String::from_utf8_lossy(&chunk));
    }
    body
}

/// `BodyReader::reinit()` must clear `trailers_present`, because the reader
/// is REUSED for the next request on a keepalive connection. Without that
/// line the second request inherits the first one's trailer fact, and both
/// proxy pumps drive `request_trailer_filter` off it: a plain request
/// pipelined behind a trailer-bearing one would fire the trailer hook it
/// never had.
#[tokio::test]
async fn trailer_fact_is_cleared_across_a_keepalive_request() {
    init_log();
    let headers = b"POST / HTTP/1.1\r\nHost: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n";
    // Request 1 ends with a real trailer field; request 2 does not.
    let body_with_trailers = b"5\r\nhello\r\n0\r\nx-checksum: ok\r\n\r\n";
    let body_without_trailers = b"5\r\nhello\r\n0\r\n\r\n";
    let mock_io = Builder::new()
        .read(&headers[..])
        .read(&body_with_trailers[..])
        .read(&headers[..])
        .read(&body_without_trailers[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));

    http_stream.read_request().await.unwrap();
    assert_eq!(
        drain_h1_request_body(&mut http_stream).await,
        "hello",
        "request 1 body"
    );
    assert!(http_stream.request_trailers_present());

    http_stream.read_request().await.unwrap();
    assert!(
        !http_stream.request_trailers_present(),
        "a new request must not inherit the previous request's trailer fact"
    );
    assert_eq!(
        drain_h1_request_body(&mut http_stream).await,
        "hello",
        "request 2 body"
    );
    assert!(
        !http_stream.request_trailers_present(),
        "this request ended with no trailer fields"
    );
}

#[tokio::test]
async fn headers_end_stream_true_for_content_length_zero() {
    init_log();
    // H1 framing is declarative: `Content-Length: 0` ends the request at
    // the header section (unlike H2, where END_STREAM is the only signal).
    let input = b"POST / HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 0\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.request_headers_end_stream());
}

#[tokio::test]
async fn read_with_body_content_length() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\n";
    let input3 = b"abc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, input3.as_slice());
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(3));
    assert_eq!(http_stream.body_bytes_read(), 3);
}

#[tokio::test]
#[should_panic(expected = "There is still data left to read.")]
async fn read_with_body_timeout() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\n";
    let input3 = b"abc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .wait(Duration::from_secs(2))
        .read(&input3[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_timeout = Some(Duration::from_secs(1));
    http_stream.read_request().await.unwrap();
    let res = http_stream.read_body_bytes().await;
    assert_eq!(http_stream.body_bytes_read(), 0);
    assert_eq!(res.unwrap_err().etype(), &ReadTimedout);
}

#[tokio::test]
async fn read_with_body_content_length_single_read() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, b"abc".as_slice());
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(3));
    assert_eq!(http_stream.body_bytes_read(), 3);
}

#[tokio::test]
#[should_panic(expected = "There is still data left to read.")]
async fn read_with_body_http10() {
    init_log();
    let input1 = b"GET / HTTP/1.0\r\n";
    let input2 = b"Host: pingora.org\r\n\r\n";
    let input3 = b"a"; // This should NOT be read as body
    let input4 = b""; // simulating close - should also NOT be reached
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .read(&input4[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let res = http_stream.read_body_bytes().await.unwrap();
    assert!(res.is_none());
    assert_eq!(http_stream.body_bytes_read(), 0);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(0));
}

#[tokio::test]
async fn read_with_body_http10_single_read() {
    init_log();
    // should have 0 body, even when data follows the headers
    let input1 = b"GET / HTTP/1.0\r\n";
    let input2 = b"Host: pingora.org\r\n\r\na";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let res = http_stream.read_body_bytes().await.unwrap();
    assert!(res.is_none());
    assert_eq!(http_stream.body_bytes_read(), 0);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(0));
    assert_eq!(http_stream.body_reader.get_body_overread().unwrap(), b"a");
}

#[tokio::test]
async fn read_http11_default_no_body() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\n\r\n";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let res = http_stream.read_body_bytes().await.unwrap();
    assert!(res.is_none());
    assert_eq!(http_stream.body_bytes_read(), 0);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(0));
}

#[tokio::test]
async fn read_http10_with_content_length() {
    init_log();
    let input1 = b"POST / HTTP/1.0\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\n";
    let input3 = b"abc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, input3.as_slice());
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(3));
    assert_eq!(http_stream.body_bytes_read(), 3);
}

#[tokio::test]
async fn read_with_body_chunked_0_incomplete() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n";
    let input3 = b"0\r\n";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_chunked_encoding());
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, b"".as_slice());
    let e = http_stream.read_body_bytes().await.unwrap_err();
    assert_eq!(*e.etype(), ErrorType::ConnectionClosed);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Done(0));
}

#[tokio::test]
async fn read_with_body_chunked_0_extra() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n";
    let input3 = b"0\r\n";
    let input4 = b"abc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .read(&input4[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_chunked_encoding());
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, b"".as_slice());
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, b"".as_slice());
    let e = http_stream.read_body_bytes().await.unwrap_err();
    assert_eq!(*e.etype(), ErrorType::ConnectionClosed);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Done(0));
}

#[tokio::test]
async fn read_with_body_chunked_single_read() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n";
    let input3 = b"0\r\n\r\n";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_chunked_encoding());
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, b"a".as_slice());
    assert_eq!(
        http_stream.body_reader.body_state,
        ParseState::Chunked(1, 0, 0, 0)
    );
    let res = http_stream.read_body_bytes().await.unwrap();
    assert!(res.is_none());
    assert_eq!(http_stream.body_bytes_read(), 1);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(1));
}

#[tokio::test]
async fn read_with_body_chunked_single_read_extra() {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n";
    let input3 = b"0\r\n\r\nabc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_chunked_encoding());
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, b"a".as_slice());
    assert_eq!(
        http_stream.body_reader.body_state,
        ParseState::Chunked(1, 0, 0, 0)
    );
    let res = http_stream.read_body_bytes().await.unwrap();
    assert!(res.is_none());
    assert_eq!(http_stream.body_bytes_read(), 1);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(1));
    assert_eq!(http_stream.body_reader.get_body_overread().unwrap(), b"abc");
}

#[rstest]
#[case(None, None)]
#[case(Some("transfer-encoding"), None)]
#[case(Some("transfer-encoding"), Some("CONTENT-LENGTH"))]
#[case(Some("TRANSFER-ENCODING"), Some("CONTENT-LENGTH"))]
#[case(Some("TRANSFER-ENCODING"), None)]
#[case(None, Some("CONTENT-LENGTH"))]
#[case(Some("TRANSFER-ENCODING"), Some("content-length"))]
#[case(None, Some("content-length"))]
#[tokio::test]
async fn transfer_encoding_and_content_length_disallowed(
    #[case] transfer_encoding_header: Option<&str>,
    #[case] content_length_header: Option<&str>,
) {
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let mut input2 = "Host: pingora.org\r\n".to_owned();

    if let Some(transfer_encoding) = transfer_encoding_header {
        input2 += &format!("{transfer_encoding}: chunked\r\n");
    }
    if let Some(content_length) = content_length_header {
        input2 += &format!("{content_length}: 4\r\n")
    }

    input2 += "\r\n3e\r\na\r\n";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(input2.as_bytes())
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let _ = http_stream.read_request().await.unwrap();

    match (content_length_header, transfer_encoding_header) {
        (Some(_) | None, Some(_)) => {
            assert!(http_stream.get_header(TRANSFER_ENCODING).is_some());
            assert!(http_stream.get_header(CONTENT_LENGTH).is_none());
        }
        (Some(_), None) => {
            assert!(http_stream.get_header(TRANSFER_ENCODING).is_none());
            assert!(http_stream.get_header(CONTENT_LENGTH).is_some());
        }
        _ => {
            assert!(http_stream.get_header(CONTENT_LENGTH).is_none());
            assert!(http_stream.get_header(TRANSFER_ENCODING).is_none());
        }
    }
}

#[rstest]
#[case::negative("-1")]
#[case::not_a_number("abc")]
#[case::float("1.5")]
#[case::empty("")]
#[case::spaces("  ")]
#[case::mixed("123abc")]
#[tokio::test]
async fn validate_request_rejects_invalid_content_length(#[case] invalid_value: &str) {
    init_log();
    let input = format!(
        "POST / HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: {}\r\n\r\n",
        invalid_value
    );
    let mock_io = Builder::new().read(input.as_bytes()).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    // read_request calls validate_request internally, so it should fail here
    let res = http_stream.read_request().await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().etype(), &InvalidHTTPHeader);
}

#[rstest]
#[case::valid_zero("0")]
#[case::valid_small("123")]
#[case::valid_large("999999")]
#[tokio::test]
async fn validate_request_accepts_valid_content_length(#[case] valid_value: &str) {
    init_log();
    let input = format!(
        "POST / HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: {}\r\n\r\n",
        valid_value
    );
    let mock_io = Builder::new().read(input.as_bytes()).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let res = http_stream.read_request().await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn validate_request_accepts_no_content_length() {
    init_log();
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let res = http_stream.read_request().await;
    assert!(res.is_ok());
}

#[tokio::test]
#[should_panic(expected = "There is still data left to read.")]
async fn read_invalid() {
    let input1 = b"GET / HTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\n\r\n";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let res = http_stream.read_request().await;
    assert_eq!(&InvalidHTTPHeader, res.unwrap_err().etype());
}

#[tokio::test]
async fn read_invalid_header_end() {
    let input = b"POST / HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 3\r\r\nConnection: keep-alive\r\n\r\nabc";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let res = http_stream.read_request().await;
    assert_eq!(&InvalidHTTPHeader, res.unwrap_err().etype());
}

async fn build_upgrade_req(upgrade: &str, conn: &str) -> HttpSession {
    let input = format!(
        "GET / HTTP/1.1\r\nHost: pingora.org\r\nUpgrade: {upgrade}\r\nConnection: {conn}\r\n\r\n"
    );
    let mock_io = Builder::new().read(input.as_bytes()).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream
}

#[tokio::test]
async fn read_upgrade_req() {
    // http 1.0
    let input =
        b"GET / HTTP/1.0\r\nHost: pingora.org\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(!http_stream.is_upgrade_req());

    // different method
    let input = b"POST / HTTP/1.1\r\nHost: pingora.org\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_upgrade_req());

    // missing upgrade header
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\nConnection: upgrade\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(!http_stream.is_upgrade_req());

    // no connection header
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\nUpgrade: WebSocket\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_upgrade_req());

    assert!(build_upgrade_req("websocket", "Upgrade")
        .await
        .is_upgrade_req());

    // mixed case
    assert!(build_upgrade_req("WebSocket", "Upgrade")
        .await
        .is_upgrade_req());
}

const POST_CL_UPGRADE_REQ: &[u8] = b"POST / HTTP/1.1\r\nHost: pingora.org\r\nUpgrade: websocket\r\nConnection: upgrade\r\nContent-Length: 10\r\n\r\n";
const POST_BODY_DATA: &[u8] = b"abcdefghij";
const POST_CHUNKED_UPGRADE_REQ: &[u8] = b"POST / HTTP/1.1\r\nHost: pingora.org\r\nUpgrade: websocket\r\nConnection: upgrade\r\nTransfer-Encoding: chunked\r\n\r\n";
const POST_BODY_DATA_CHUNKED: &[u8] = b"3\r\nabc\r\n7\r\ndefghij\r\n0\r\n\r\n";

#[rstest]
#[case::content_length(POST_CL_UPGRADE_REQ, POST_BODY_DATA, POST_BODY_DATA)]
#[case::chunked(POST_CHUNKED_UPGRADE_REQ, POST_BODY_DATA, POST_BODY_DATA_CHUNKED)]
#[tokio::test]
async fn read_upgrade_req_with_body(
    #[case] header: &[u8],
    #[case] body: &[u8],
    #[case] body_wire: &[u8],
) {
    let ws_data = b"data";
    let mock_io = Builder::new()
        .read(header)
        .read(body_wire)
        .write(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
        .read(&ws_data[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_upgrade_req());
    // request has body
    assert!(!http_stream.is_body_done());

    let mut buf = vec![];
    while let Some(b) = http_stream.read_body_bytes().await.unwrap() {
        buf.put_slice(&b);
    }
    assert_eq!(buf, body);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(10));
    assert_eq!(http_stream.body_bytes_read(), 10);

    assert!(http_stream.is_body_done());

    let mut response = ResponseHeader::build(StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
    response.set_version(http::Version::HTTP_11);
    http_stream
        .write_response_header(Box::new(response))
        .await
        .unwrap();
    // body reader type switches
    assert!(!http_stream.is_body_done());

    // now the ws data
    let buf = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(buf, ws_data.as_slice());
    assert!(!http_stream.is_body_done());

    // EOF ends body
    assert!(http_stream.read_body_bytes().await.unwrap().is_none());
    assert!(http_stream.is_body_done());
}

#[rstest]
#[case::content_length(POST_CL_UPGRADE_REQ, POST_BODY_DATA, POST_BODY_DATA)]
#[case::chunked(POST_CHUNKED_UPGRADE_REQ, POST_BODY_DATA, POST_BODY_DATA_CHUNKED)]
#[tokio::test]
async fn read_upgrade_req_with_body_extra(
    #[case] header: &[u8],
    #[case] body: &[u8],
    #[case] body_wire: &[u8],
) {
    let ws_data = b"data";
    let data_wire = [body_wire, ws_data.as_slice()].concat();
    let mock_io = Builder::new()
        .read(header)
        .read(&data_wire[..])
        .write(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_upgrade_req());
    // request has body
    assert!(!http_stream.is_body_done());

    let mut buf = vec![];
    while let Some(b) = http_stream.read_body_bytes().await.unwrap() {
        buf.put_slice(&b);
    }
    assert_eq!(buf, body);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(10));
    assert_eq!(http_stream.body_bytes_read(), 10);

    assert!(http_stream.is_body_done());

    let mut response = ResponseHeader::build(StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
    response.set_version(http::Version::HTTP_11);
    http_stream
        .write_response_header(Box::new(response))
        .await
        .unwrap();
    // body reader type switches
    assert!(!http_stream.is_body_done());

    // now the ws data
    let buf = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(buf, ws_data.as_slice());
    assert!(!http_stream.is_body_done());

    // EOF ends body
    assert!(http_stream.read_body_bytes().await.unwrap().is_none());
    assert!(http_stream.is_body_done());
}

#[rstest]
#[case::content_length(POST_CL_UPGRADE_REQ, POST_BODY_DATA, POST_BODY_DATA)]
#[case::chunked(POST_CHUNKED_UPGRADE_REQ, POST_BODY_DATA, POST_BODY_DATA_CHUNKED)]
#[tokio::test]
async fn read_upgrade_req_with_preread_body(
    #[case] header: &[u8],
    #[case] body: &[u8],
    #[case] body_wire: &[u8],
) {
    let ws_data = b"data";
    let data_wire = [header, body_wire, ws_data.as_slice()].concat();
    let mock_io = Builder::new()
        .read(&data_wire[..])
        .write(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_upgrade_req());
    // request has body
    assert!(!http_stream.is_body_done());

    let mut buf = vec![];
    while let Some(b) = http_stream.read_body_bytes().await.unwrap() {
        buf.put_slice(&b);
    }
    assert_eq!(buf, body);
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(10));
    assert_eq!(http_stream.body_bytes_read(), 10);

    assert!(http_stream.is_body_done());

    let mut response = ResponseHeader::build(StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
    response.set_version(http::Version::HTTP_11);
    http_stream
        .write_response_header(Box::new(response))
        .await
        .unwrap();
    // body reader type switches
    assert!(!http_stream.is_body_done());

    // now the ws data
    let buf = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(buf, ws_data.as_slice());
    assert!(!http_stream.is_body_done());

    // EOF ends body
    assert!(http_stream.read_body_bytes().await.unwrap().is_none());
    assert!(http_stream.is_body_done());
}

#[rstest]
#[case::content_length(POST_CL_UPGRADE_REQ, POST_BODY_DATA)]
#[case::chunked(POST_CHUNKED_UPGRADE_REQ, POST_BODY_DATA_CHUNKED)]
#[tokio::test]
async fn read_upgrade_req_with_preread_body_after_101(
    #[case] header: &[u8],
    #[case] body_wire: &[u8],
) {
    let ws_data = b"data";
    let data_wire = [header, body_wire, ws_data.as_slice()].concat();
    let mock_io = Builder::new()
        .read(&data_wire[..])
        .write(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_upgrade_req());
    // request has body
    assert!(!http_stream.is_body_done());

    let mut response = ResponseHeader::build(StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
    response.set_version(http::Version::HTTP_11);
    http_stream
        .write_response_header(Box::new(response))
        .await
        .unwrap();
    // body reader type switches to http10
    assert!(!http_stream.is_body_done());

    let mut buf = vec![];
    while let Some(b) = http_stream.read_body_bytes().await.unwrap() {
        buf.put_slice(&b);
    }
    let expected_body = [body_wire, ws_data.as_slice()].concat();
    assert_eq!(buf, expected_body.as_bytes());
    assert_eq!(http_stream.body_bytes_read(), expected_body.len());
    assert!(http_stream.is_body_done());
}

#[tokio::test]
async fn read_upgrade_req_with_1xx_response() {
    let input =
        b"GET / HTTP/1.1\r\nHost: pingora.org\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n";
    let mock_io = Builder::new()
        .read(&input[..])
        .write(b"HTTP/1.1 100 Continue\r\n\r\n")
        .write(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_upgrade_req());
    let mut response = ResponseHeader::build(StatusCode::CONTINUE, None).unwrap();
    response.set_version(http::Version::HTTP_11);
    http_stream
        .write_response_header(Box::new(response))
        .await
        .unwrap();
    // 100 won't affect body state
    // current GET request is done
    assert!(http_stream.is_body_done());

    let mut response = ResponseHeader::build(StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
    response.set_version(http::Version::HTTP_11);
    http_stream
        .write_response_header(Box::new(response))
        .await
        .unwrap();
    // body reader type switches
    assert!(!http_stream.is_body_done());
    // EOF ends body
    assert!(http_stream.read_body_bytes().await.unwrap().is_none());
    assert!(http_stream.is_body_done());
}

#[tokio::test]
async fn test_upgrade_without_content_length_with_ws_data() {
    let request =
        b"GET / HTTP/1.1\r\nHost: pingora.org\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n";
    let ws_data = b"websocket data";

    let mock_io = Builder::new()
        .read(request)
        .write(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
        .read(ws_data) // websocket data sent after 101
        .build();

    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert!(http_stream.is_upgrade_req());

    // When enabled (default), is_body_done() is called before the upgrade
    http_stream.set_close_on_response_before_downstream_finish(false);

    // Send 101 response - this is where the bug occurs
    let mut response = ResponseHeader::build(StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
    response.set_version(http::Version::HTTP_11);
    http_stream
        .write_response_header(Box::new(response))
        .await
        .unwrap();

    assert_eq!(
        http_stream.body_reader.body_state,
        ParseState::UntilClose(0),
        "Body reader should be in UntilClose mode after 101 for upgraded connections"
    );

    // Try to read websocket data
    let mut buf = vec![];
    while let Some(b) = http_stream.read_body_bytes().await.unwrap() {
        buf.put_slice(&b);
    }
    assert_eq!(buf, ws_data, "Expected to read websocket data after 101");
}

#[tokio::test]
async fn set_server_keepalive() {
    // close
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\nConnection: close\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    // verify close
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Off);
    http_stream.set_server_keepalive(Some(60));
    // verify no change on override
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Off);

    // explicit keep-alive
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\nConnection: keep-alive\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    // default is infinite for 1.1
    http_stream.read_request().await.unwrap();
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Infinite);
    http_stream.set_server_keepalive(Some(60));
    // override respected
    assert_eq!(
        http_stream.keepalive_timeout,
        KeepaliveStatus::Timeout(Duration::from_secs(60))
    );

    // not specified
    let input = b"GET / HTTP/1.1\r\nHost: pingora.org\r\n\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    // default is infinite for 1.1
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Infinite);
    http_stream.set_server_keepalive(Some(60));
    // override respected
    assert_eq!(
        http_stream.keepalive_timeout,
        KeepaliveStatus::Timeout(Duration::from_secs(60))
    );
}

#[tokio::test]
async fn write() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let write_expected = b"HTTP/1.1 200 OK\r\nFoo: Bar\r\n\r\n";
    let mock_io = Builder::new().read(read_wire).write(write_expected).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let mut new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    new_response.append_header("Foo", "Bar").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&new_response)
        .await
        .unwrap();
}

#[tokio::test]
async fn write_custom_reason() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let write_expected = b"HTTP/1.1 200 Just Fine\r\nFoo: Bar\r\n\r\n";
    let mock_io = Builder::new().read(read_wire).write(write_expected).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let mut new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    new_response.set_reason_phrase(Some("Just Fine")).unwrap();
    new_response.append_header("Foo", "Bar").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&new_response)
        .await
        .unwrap();
}

#[tokio::test]
async fn write_informational() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let write_expected = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nFoo: Bar\r\n\r\n";
    let mock_io = Builder::new().read(read_wire).write(write_expected).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let response_100 = ResponseHeader::build(StatusCode::CONTINUE, None).unwrap();
    http_stream
        .write_response_header_ref(&response_100)
        .await
        .unwrap();
    let mut response_200 = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response_200.append_header("Foo", "Bar").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&response_200)
        .await
        .unwrap();
}

#[tokio::test]
async fn write_informational_ignored() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let write_expected = b"HTTP/1.1 200 OK\r\nFoo: Bar\r\n\r\n";
    let mock_io = Builder::new().read(read_wire).write(write_expected).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    // ignore the 100 Continue
    http_stream.ignore_info_resp = true;
    http_stream.read_request().await.unwrap();
    let response_100 = ResponseHeader::build(StatusCode::CONTINUE, None).unwrap();
    http_stream
        .write_response_header_ref(&response_100)
        .await
        .unwrap();
    let mut response_200 = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response_200.append_header("Foo", "Bar").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&response_200)
        .await
        .unwrap();
}

#[tokio::test]
async fn write_informational_100_not_ignored_if_expect_continue() {
    let input = b"GET / HTTP/1.1\r\nExpect: 100-continue\r\n\r\n";
    let output = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nFoo: Bar\r\n\r\n";

    let mock_io = Builder::new().read(&input[..]).write(output).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream.ignore_info_resp = true;
    // 100 Continue is not ignored due to Expect: 100-continue on request
    let response_100 = ResponseHeader::build(StatusCode::CONTINUE, None).unwrap();
    http_stream
        .write_response_header_ref(&response_100)
        .await
        .unwrap();
    let mut response_200 = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response_200.append_header("Foo", "Bar").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&response_200)
        .await
        .unwrap();
}

#[tokio::test]
async fn write_informational_1xx_ignored_if_expect_continue() {
    let input = b"GET / HTTP/1.1\r\nExpect: 100-continue\r\n\r\n";
    let output = b"HTTP/1.1 200 OK\r\nFoo: Bar\r\n\r\n";

    let mock_io = Builder::new().read(&input[..]).write(output).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream.ignore_info_resp = true;
    // 102 Processing is ignored
    let response_102 = ResponseHeader::build(StatusCode::PROCESSING, None).unwrap();
    http_stream
        .write_response_header_ref(&response_102)
        .await
        .unwrap();
    let mut response_200 = ResponseHeader::build(StatusCode::OK, None).unwrap();
    response_200.append_header("Foo", "Bar").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&response_200)
        .await
        .unwrap();
}

#[tokio::test]
async fn write_101_switching_protocol() {
    let read_wire = b"GET / HTTP/1.1\r\nUpgrade: websocket\r\n\r\n";
    let wire = b"HTTP/1.1 101 Switching Protocols\r\nFoo: Bar\r\n\r\n";
    let wire_body = b"nPAYLOAD";
    let mock_io = Builder::new()
        .read(read_wire)
        .write(wire)
        .write(wire_body)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let mut response_101 = ResponseHeader::build(StatusCode::SWITCHING_PROTOCOLS, None).unwrap();
    response_101.append_header("Foo", "Bar").unwrap();
    http_stream
        .write_response_header_ref(&response_101)
        .await
        .unwrap();
    assert_eq!(http_stream.body_writer.body_mode, BodyMode::UntilClose(0));

    let n = http_stream.write_body(wire_body).await.unwrap().unwrap();
    assert_eq!(wire_body.len(), n);
    assert_eq!(http_stream.body_writer.body_mode, BodyMode::UntilClose(n));

    // this write should be ignored
    let response_502 = ResponseHeader::build(StatusCode::BAD_GATEWAY, None).unwrap();
    http_stream
        .write_response_header_ref(&response_502)
        .await
        .unwrap();
}

#[tokio::test]
async fn write_body_cl() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let wire_header = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n";
    let wire_body = b"a";
    let mock_io = Builder::new()
        .read(read_wire)
        .write(wire_header)
        .write(wire_body)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let mut new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    new_response.append_header("Content-Length", "1").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&new_response)
        .await
        .unwrap();
    assert_eq!(
        http_stream.body_writer.body_mode,
        BodyMode::ContentLength(1, 0)
    );
    let n = http_stream.write_body(wire_body).await.unwrap().unwrap();
    assert_eq!(wire_body.len(), n);
    let n = http_stream.finish_body().await.unwrap().unwrap();
    assert_eq!(wire_body.len(), n);
}

#[tokio::test]
async fn body_bytes_sent_excludes_response_header() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let wire_header = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
    let wire_body = b"hello";
    let mock_io = Builder::new()
        .read(read_wire)
        .write(wire_header)
        .write(wire_body)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let mut new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    new_response.append_header("Content-Length", "5").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header(Box::new(new_response))
        .await
        .unwrap();
    assert_eq!(http_stream.body_bytes_sent(), 0);
    http_stream.write_body(wire_body).await.unwrap();
    assert_eq!(http_stream.body_bytes_sent(), wire_body.len());
}

#[tokio::test]
async fn write_body_http10() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let wire_header = b"HTTP/1.1 200 OK\r\n\r\n";
    let wire_body = b"a";
    let mock_io = Builder::new()
        .read(read_wire)
        .write(wire_header)
        .write(wire_body)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&new_response)
        .await
        .unwrap();
    assert_eq!(http_stream.body_writer.body_mode, BodyMode::UntilClose(0));
    let n = http_stream.write_body(wire_body).await.unwrap().unwrap();
    assert_eq!(wire_body.len(), n);
    let n = http_stream.finish_body().await.unwrap().unwrap();
    assert_eq!(wire_body.len(), n);
}

#[tokio::test]
async fn write_body_chunk() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let wire_header = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    let wire_body = b"1\r\na\r\n";
    let wire_end = b"0\r\n\r\n";
    let mock_io = Builder::new()
        .read(read_wire)
        .write(wire_header)
        .write(wire_body)
        .write(wire_end)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let mut new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    new_response
        .append_header("Transfer-Encoding", "chunked")
        .unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&new_response)
        .await
        .unwrap();
    assert_eq!(
        http_stream.body_writer.body_mode,
        BodyMode::ChunkedEncoding(0)
    );
    let n = http_stream.write_body(b"a").await.unwrap().unwrap();
    assert_eq!(b"a".len(), n);
    let n = http_stream.finish_body().await.unwrap().unwrap();
    assert_eq!(b"a".len(), n);
}

#[tokio::test]
async fn read_with_illegal() {
    init_log();
    let input1 = b"GET /a?q=b c HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\n";
    let input3 = b"abc";
    let mock_io = Builder::new()
        .read(&input1[..])
        .read(&input2[..])
        .read(&input3[..])
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    assert_eq!(http_stream.get_path(), &b"/a?q=b%20c"[..]);
    let res = http_stream.read_body().await.unwrap().unwrap();
    assert_eq!(res, BufRef::new(0, 3));
    assert_eq!(http_stream.body_reader.body_state, ParseState::Complete(3));
    assert_eq!(input3, http_stream.get_body(&res));
}

#[test]
fn escape_illegal() {
    init_log();
    // in query string
    let input = BytesMut::from(
        &b"GET /a?q=<\"b c\"> HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 3\r\n\r\n"[..],
    );
    let output = escape_illegal_request_line(&input).unwrap();
    assert_eq!(
        &output,
        &b"GET /a?q=%3C%22b%20c%22%3E HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 3\r\n\r\n"[..]
    );

    // in path
    let input = BytesMut::from(
        &b"GET /a:\"bc\" HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 3\r\n\r\n"[..],
    );
    let output = escape_illegal_request_line(&input).unwrap();
    assert_eq!(
        &output,
        &b"GET /a:%22bc%22 HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 3\r\n\r\n"[..]
    );

    // empty uri, unable to parse
    let input =
        BytesMut::from(&b"GET  HTTP/1.1\r\nHost: pingora.org\r\nContent-Length: 3\r\n\r\n"[..]);
    assert!(escape_illegal_request_line(&input).is_none());
}

#[tokio::test]
async fn test_write_body_buf() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let write_expected = b"HTTP/1.1 200 OK\r\nFoo: Bar\r\n\r\n";
    let mock_io = Builder::new().read(read_wire).write(write_expected).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let mut new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    new_response.append_header("Foo", "Bar").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&new_response)
        .await
        .unwrap();
    let written = http_stream.write_body_buf().await.unwrap();
    assert!(written.is_none());
}

#[tokio::test]
async fn response_trailer_capability_tracks_planned_and_actual_framing() {
    let request = b"GET / HTTP/1.1\r\n\r\n";
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    let trailers = b"0\r\nx-test: yes\r\n\r\n";
    let mock_io = Builder::new()
        .read(request)
        .write(response)
        .write(trailers)
        .build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.read_request().await.unwrap();
    session.update_resp_headers = false;
    let mut header = ResponseHeader::build(StatusCode::OK, None).unwrap();
    header
        .insert_header(http::header::TRANSFER_ENCODING, "chunked")
        .unwrap();
    session.prepare_response_header(&mut header).unwrap();
    assert!(session.response_trailers_supported());
    session
        .write_response_header(Box::new(header))
        .await
        .unwrap();
    assert!(session.response_trailers_supported());
    let mut map = HeaderMap::new();
    map.insert("x-test", "yes".parse().unwrap());
    session.write_trailers(&map).await.unwrap();
}

#[tokio::test]
async fn http10_chunked_downgrade_closes_and_rejects_composite_codings() {
    let request = b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n";
    let mock_io = Builder::new().read(request).build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.read_request().await.unwrap();

    let mut chunked = ResponseHeader::build(StatusCode::OK, None).unwrap();
    chunked
        .insert_header(http::header::TRANSFER_ENCODING, "chunked")
        .unwrap();
    session.prepare_response_header(&mut chunked).unwrap();
    assert_eq!(chunked.version, Version::HTTP_10);
    assert!(!chunked
        .headers
        .contains_key(http::header::TRANSFER_ENCODING));
    assert_eq!(chunked.headers[http::header::CONNECTION], "close");
    assert!(!session.will_keepalive());
    assert!(!session.response_trailers_supported());

    let mut encoded = ResponseHeader::build(StatusCode::OK, None).unwrap();
    encoded
        .insert_header(http::header::TRANSFER_ENCODING, "gzip, chunked")
        .unwrap();
    assert!(session.prepare_response_header(&mut encoded).is_err());
    assert_eq!(
        encoded.headers[http::header::TRANSFER_ENCODING],
        "gzip, chunked"
    );
}

#[tokio::test]
async fn ignored_http10_informational_does_not_change_connection_state() {
    let request = b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n";
    let mock_io = Builder::new().read(request).build();
    let mut session = HttpSession::new(Box::new(mock_io));
    session.read_request().await.unwrap();
    session.ignore_info_resp = true;
    assert!(session.will_keepalive());

    let mut early_hints = ResponseHeader::build(StatusCode::EARLY_HINTS, None).unwrap();
    let original_version = early_hints.version;
    session.prepare_response_header(&mut early_hints).unwrap();

    assert_eq!(early_hints.version, original_version);
    assert!(!early_hints.headers.contains_key(header::CONNECTION));
    assert!(session.will_keepalive());
    assert!(session
        .prepare_response_header_for_write(&mut early_hints)
        .unwrap()
        .is_none());
    assert!(session.will_keepalive());
}

#[tokio::test]
async fn bodyless_http10_responses_do_not_require_close_delimiting() {
    for (method, status) in [
        ("HEAD", StatusCode::OK),
        ("GET", StatusCode::NO_CONTENT),
        ("GET", StatusCode::NOT_MODIFIED),
        ("GET", StatusCode::CONTINUE),
    ] {
        let request = format!("{method} / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
        let mock_io = Builder::new().read(request.as_bytes()).build();
        let mut session = HttpSession::new(Box::new(mock_io));
        session.read_request().await.unwrap();
        assert!(session.will_keepalive());

        let mut response = ResponseHeader::build(status, None).unwrap();
        session.prepare_response_header(&mut response).unwrap();

        assert_eq!(response.version, Version::HTTP_10, "{method} {status}");
        assert!(
            !response.headers.contains_key(header::CONNECTION),
            "{method} {status}"
        );
        assert!(session.will_keepalive(), "{method} {status}");
        assert!(!session.response_trailers_supported(), "{method} {status}");
    }
}

#[tokio::test]
#[should_panic(expected = "There is still data left to write.")]
async fn test_write_body_buf_write_timeout() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let wire1 = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n";
    let wire2 = b"abc";
    let mock_io = Builder::new()
        .read(read_wire)
        .write(wire1)
        .wait(Duration::from_millis(500))
        .write(wire2)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream.write_timeout = Some(Duration::from_millis(100));
    let mut new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    new_response.append_header("Content-Length", "3").unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header_ref(&new_response)
        .await
        .unwrap();
    http_stream.body_write_buf = BytesMut::from(&b"abc"[..]);
    let res = http_stream.write_body_buf().await;
    assert_eq!(res.unwrap_err().etype(), &WriteTimedout);
}

#[tokio::test]
async fn test_write_continue_resp() {
    let read_wire = b"GET / HTTP/1.1\r\n\r\n";
    let write_expected = b"HTTP/1.1 100 Continue\r\n\r\n";
    let mock_io = Builder::new().read(read_wire).write(write_expected).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream.write_continue_response().await.unwrap();
}

#[test]
fn test_get_write_timeout() {
    let mut http_stream = HttpSession::new(Box::new(Builder::new().build()));
    let expected = Duration::from_secs(5);

    http_stream.set_write_timeout(Some(expected));
    assert_eq!(Some(expected), http_stream.write_timeout(50));
}

#[test]
fn test_get_write_timeout_none() {
    let http_stream = HttpSession::new(Box::new(Builder::new().build()));
    assert!(http_stream.write_timeout(50).is_none());
}

#[test]
fn test_get_write_timeout_min_send_rate_zero() {
    let mut http_stream = HttpSession::new(Box::new(Builder::new().build()));
    http_stream.set_min_send_rate(Some(0));
    assert!(http_stream.write_timeout(50).is_none());

    let mut http_stream = HttpSession::new(Box::new(Builder::new().build()));
    http_stream.set_min_send_rate(None);
    assert!(http_stream.write_timeout(50).is_none());
}

#[test]
fn test_get_write_timeout_min_send_rate_overrides_write_timeout() {
    let mut http_stream = HttpSession::new(Box::new(Builder::new().build()));
    let expected = Duration::from_millis(29800);

    http_stream.set_write_timeout(Some(Duration::from_secs(60)));
    http_stream.set_min_send_rate(Some(5000));

    assert_eq!(Some(expected), http_stream.write_timeout(149000));
}

#[test]
fn test_get_write_timeout_min_send_rate_max_zero_buf() {
    let mut http_stream = HttpSession::new(Box::new(Builder::new().build()));
    let expected = Duration::from_secs(1);

    http_stream.set_min_send_rate(Some(1));
    assert_eq!(Some(expected), http_stream.write_timeout(0));
}

#[tokio::test]
async fn test_te_and_cl_disables_keepalive() {
    // When both Transfer-Encoding and Content-Length are present,
    // we must disable keepalive per RFC 9112 Section 6.1
    // https://datatracker.ietf.org/doc/html/rfc9112#section-6.1-15
    let input = b"POST / HTTP/1.1\r\n\
Host: pingora.org\r\n\
Transfer-Encoding: chunked\r\n\
Content-Length: 10\r\n\
\r\n\
5\r\n\
hello\r\n\
0\r\n\
\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();

    // Keepalive should be disabled
    assert_eq!(http_stream.keepalive_timeout, KeepaliveStatus::Off);

    // Content-Length header should have been removed
    assert!(!http_stream
        .req_header()
        .headers
        .contains_key(CONTENT_LENGTH));

    // Transfer-Encoding should still be present
    assert!(http_stream
        .req_header()
        .headers
        .contains_key(TRANSFER_ENCODING));
}

#[tokio::test]
async fn test_http10_request_with_transfer_encoding_rejected() {
    // HTTP/1.0 requests MUST NOT contain Transfer-Encoding
    let input = b"POST / HTTP/1.0\r\n\
Host: pingora.org\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
5\r\n\
hello\r\n\
0\r\n\
\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let result = http_stream.read_request().await;

    // Should be rejected with InvalidHTTPHeader error
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.etype(), &InvalidHTTPHeader);
    assert!(err.to_string().contains("Transfer-Encoding"));
}

#[tokio::test]
async fn test_http10_request_without_transfer_encoding_accepted() {
    // HTTP/1.0 requests without Transfer-Encoding should be accepted
    let input = b"POST / HTTP/1.0\r\n\
Host: pingora.org\r\n\
Content-Length: 5\r\n\
\r\n\
hello";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let result = http_stream.read_request().await;

    // Should succeed
    assert!(result.is_ok());
    assert_eq!(http_stream.req_header().version, http::Version::HTTP_10);
}

#[tokio::test]
async fn test_http11_request_with_transfer_encoding_accepted() {
    // HTTP/1.1 with Transfer-Encoding should be accepted (contrast with HTTP/1.0)
    let input = b"POST / HTTP/1.1\r\n\
Host: pingora.org\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
5\r\n\
hello\r\n\
0\r\n\
\r\n";
    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    let result = http_stream.read_request().await;

    // Should succeed
    assert!(result.is_ok());
    assert_eq!(http_stream.req_header().version, http::Version::HTTP_11);
}

#[tokio::test]
async fn test_request_multiple_transfer_encoding_headers() {
    init_log();
    // Multiple TE headers should be treated as comma-separated
    let input = b"POST / HTTP/1.1\r\n\
Host: pingora.org\r\n\
Transfer-Encoding: gzip\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
5\r\n\
hello\r\n\
0\r\n\
\r\n";

    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();

    // Should correctly identify chunked encoding from last header
    assert!(http_stream.is_chunked_encoding());

    // Verify body can be read correctly
    let body = http_stream.read_body_bytes().await.unwrap();
    assert_eq!(body.unwrap().as_ref(), b"hello");
}

#[tokio::test]
async fn test_request_multiple_te_headers_chunked_not_last() {
    init_log();
    // Chunked in first header but not last - should NOT be chunked
    // Only the final Transfer-Encoding determines if body is chunked
    let input = b"POST / HTTP/1.1\r\n\
Host: pingora.org\r\n\
Transfer-Encoding: chunked\r\n\
Transfer-Encoding: identity\r\n\
Content-Length: 5\r\n\
\r\n";

    let mock_io = Builder::new().read(&input[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    // should fail validation
    http_stream.read_request().await.unwrap_err();
}

#[tokio::test]
async fn test_no_more_reuses_explicitly_disables_reuse() {
    init_log();
    let wire_req = b"GET /test HTTP/1.1\r\n\r\n";
    let wire_header = b"HTTP/1.1 200 OK\r\n\r\n";
    let mock_io = Builder::new()
        .read(&wire_req[..])
        .write(wire_header)
        .build();
    let mut http_session = HttpSession::new(Box::new(mock_io));

    // Setting the number of keepalive reuses here overrides the keepalive
    // setting below
    http_session.set_keepalive_reuses_remaining(Some(0));

    http_session.read_request().await.unwrap();

    let new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    http_session.update_resp_headers = false;
    http_session
        .write_response_header(Box::new(new_response))
        .await
        .unwrap();

    assert_eq!(http_session.body_writer.body_mode, BodyMode::UntilClose(0));

    http_session.finish_body().await.unwrap().unwrap();

    http_session.set_keepalive(Some(100));
    let reused = http_session.reuse().await.unwrap();
    assert!(reused.is_none());
}

#[tokio::test]
async fn test_close_delimited_response_explicitly_disables_reuse() {
    init_log();
    let wire_req = b"GET /test HTTP/1.1\r\n\r\n";
    let wire_header = b"HTTP/1.1 200 OK\r\n\r\n";
    let mock_io = Builder::new()
        .read(&wire_req[..])
        .write(wire_header)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();

    let new_response = ResponseHeader::build(StatusCode::OK, None).unwrap();
    http_stream.update_resp_headers = false;
    http_stream
        .write_response_header(Box::new(new_response))
        .await
        .unwrap();

    assert_eq!(http_stream.body_writer.body_mode, BodyMode::UntilClose(0));

    http_stream.finish_body().await.unwrap().unwrap();

    let reused = http_stream.reuse().await.unwrap();
    assert!(reused.is_none());
}

#[test]
fn test_connection_user_context_set_and_take() {
    let mock_io = Builder::new().build();
    let mut session = HttpSession::new(Box::new(mock_io));

    // Initially no context
    assert!(session.take_connection_user_context().is_none());

    // Set a context
    session.set_connection_user_context(Some(Box::new(42u64)));

    // Take it back
    let ctx = session.take_connection_user_context();
    assert!(ctx.is_some());
    let val = ctx.unwrap().downcast::<u64>().unwrap();
    assert_eq!(*val, 42u64);

    // After take, it's gone
    assert!(session.take_connection_user_context().is_none());
}

#[test]
fn test_connection_user_context_set_none_clears() {
    let mock_io = Builder::new().build();
    let mut session = HttpSession::new(Box::new(mock_io));

    session.set_connection_user_context(Some(Box::new("hello".to_string())));
    session.set_connection_user_context(None);
    assert!(session.take_connection_user_context().is_none());
}
