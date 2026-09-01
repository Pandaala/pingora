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
use http::StatusCode;
use std::future::IntoFuture;
use tokio_test::io::{Builder, Mock};

fn init_log() {
    let _ = env_logger::builder().is_test(true).try_init();
}

// An upper limit for any read within any test to prevent tests from hanging forever if
// an internal read call never returns, etc.
const TEST_MAX_WAIT_FOR_READ: Duration = Duration::from_secs(3);

// The duration of 600 seconds is chosen to be "effectively forever" for the purpose of testing
const TEST_FOREVER_DURATION: Duration = Duration::from_secs(600);

// The read_timeout to use, when we want to test that a read operation times out
const TEST_READ_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct ReadBlockedForeverError;

// Returns a client stream that will "never" send any bytes / return from a read operation
fn mocked_blocking_headers_forever_stream() -> Box<Mock> {
    Box::new(Builder::new().wait(TEST_FOREVER_DURATION).build())
}

fn mocked_blocking_body_forever_stream() -> Box<Mock> {
    let http1 = b"GET / HTTP/1.1\r\n";
    let http2 = b"Host: pingora.example\r\nContent-Length: 3\r\n\r\n";
    Box::new(
        Builder::new()
            .read(&http1[..])
            .read(&http2[..])
            .wait(TEST_FOREVER_DURATION)
            .build(),
    )
}

// Helper function to test a read operation with a tokio timeout
// to prevent tests from hanging forever in case of a bug
async fn test_read_with_tokio_timeout<F, T>(
    read_future: F,
) -> Result<Result<T, Box<Error>>, ReadBlockedForeverError>
where
    F: IntoFuture<Output = Result<T, Box<Error>>>,
{
    let read_result = tokio::time::timeout(TEST_MAX_WAIT_FOR_READ, read_future).await;
    read_result.map_err(|_| ReadBlockedForeverError)
}

#[tokio::test]
async fn test_read_http_request_headers_timeout_for_read_request() {
    // confirm that a `read_timeout` of `None` would've waited "indefinitely"
    let mut http_stream = HttpSession::new(mocked_blocking_headers_forever_stream());
    http_stream.read_timeout = None;
    let res = test_read_with_tokio_timeout(http_stream.read_request()).await;
    assert!(res.is_err()); // test timeout occurred, and not any internal Pingora timeout

    // confirm that the `read_timeout` is respected
    let mut http_stream = HttpSession::new(mocked_blocking_headers_forever_stream());
    http_stream.read_timeout = Some(TEST_READ_TIMEOUT);
    let res = test_read_with_tokio_timeout(http_stream.read_request()).await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap().unwrap_err().etype(), &ReadTimedout);
}

#[tokio::test]
async fn test_read_http_body_timeout_for_read_body_bytes() {
    // confirm that a `read_timeout` of `None` would've waited "indefinitely"
    let mut http_stream = HttpSession::new(mocked_blocking_body_forever_stream());
    http_stream.read_timeout = None;
    http_stream.read_request().await.unwrap();
    let res = test_read_with_tokio_timeout(http_stream.read_body_bytes()).await;
    assert!(res.is_err()); // test timeout occurred, and not any internal Pingora timeout

    // confirm that the `read_timeout` is respected
    let mut http_stream = HttpSession::new(mocked_blocking_body_forever_stream());
    http_stream.read_timeout = Some(TEST_READ_TIMEOUT);
    http_stream.read_request().await.unwrap();
    let res = test_read_with_tokio_timeout(http_stream.read_body_bytes()).await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap().unwrap_err().etype(), &ReadTimedout);
}

#[tokio::test]
async fn test_send_proxy_task_and_write() {
    init_log();

    // We need to know exact bytes that will be written
    // "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"
    let expected_header = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
    let expected_body = b"hello";

    let mock_io = Builder::new()
        .write(expected_header)
        .write(expected_body)
        .build();

    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false; // Disable automatic headers

    // Queue header task
    let mut header = ResponseHeader::build(StatusCode::OK, Some(5)).unwrap();
    header.insert_header("Content-Length", "5").unwrap();
    http_stream.send_proxy_task(HttpTask::Header(Box::new(header), false));

    // Queue body task
    http_stream.send_proxy_task(HttpTask::Body(Some(Bytes::from("hello")), true));

    // Write all tasks
    let end_stream = http_stream.write_proxy_tasks().await.unwrap();
    assert!(end_stream);
}

#[tokio::test]
async fn test_proxy_task_with_timeout() {
    init_log();

    let expected_header = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
    let expected_body = b"hello";

    let mock_io = Builder::new()
        .write(expected_header)
        .write(expected_body)
        .build();

    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false;
    http_stream.write_timeout = Some(Duration::from_secs(1)); // Set write timeout

    // Queue tasks
    let mut header = ResponseHeader::build(StatusCode::OK, Some(5)).unwrap();
    header.insert_header("Content-Length", "5").unwrap();
    http_stream.send_proxy_task(HttpTask::Header(Box::new(header), false));
    http_stream.send_proxy_task(HttpTask::Body(Some(Bytes::from("hello")), true));

    // Verify initial state
    assert_eq!(
        http_stream.body_bytes_sent(),
        0,
        "Should start with 0 bytes sent"
    );

    // Write all tasks with timeout
    let end_stream = http_stream.write_proxy_tasks().await.unwrap();
    assert!(end_stream);

    // Verify body bytes were counted correctly (not double counted)
    assert_eq!(
        http_stream.body_bytes_sent(),
        5,
        "Should count exactly 5 bytes (application level), not double counted"
    );
}

// Test that write_proxy_tasks is cancel-safe: if the future is dropped mid-execution,
// unwritten tasks should remain in the queue.
#[tokio::test]
async fn test_proxy_task_cancel_safety() {
    init_log();

    let expected_header = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    // First chunk: "5\r\nhello\r\n"
    let expected_chunk1 = b"5\r\nhello\r\n";

    // Create a mock IO that will write the header and first chunk,
    // but will block indefinitely on the second chunk
    let mock_io = Builder::new()
        .write(expected_header)
        .write(expected_chunk1)
        .wait(Duration::from_secs(999)) // This will cause timeout
        .build();

    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false;
    http_stream.write_timeout = Some(Duration::from_millis(100));

    // Queue 3 tasks: header + 2 body chunks
    let mut header = ResponseHeader::build(StatusCode::OK, None).unwrap();
    header
        .insert_header("Transfer-Encoding", "chunked")
        .unwrap();
    http_stream.send_proxy_task(HttpTask::Header(Box::new(header), false));
    http_stream.send_proxy_task(HttpTask::Body(Some(Bytes::from("hello")), false));
    http_stream.send_proxy_task(HttpTask::Body(Some(Bytes::from("world")), true));

    // Verify we have 3 tasks queued
    assert_eq!(http_stream.proxy_task_state.tasks.len(), 3);

    // Try to write all tasks - this should timeout while writing the second body chunk
    let result = http_stream.write_proxy_tasks().await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().etype(), &WriteTimedout);

    // With the refactored cancel-safe design:
    // - First task (header) was written successfully and removed from queue
    // - Second task (first body "hello") was removed and sent to BodyWriter, write succeeded, state cleared
    // - Third task (second body "world") was removed and sent to BodyWriter, timed out mid-write
    // - The in-progress write state is tracked in current_writer, NOT in the queue
    assert_eq!(
        http_stream.proxy_task_state.tasks.len(),
        0,
        "Queue should be empty - tasks are owned by writers once sent"
    );

    // The task being written should be tracked in current_writer
    assert!(
        matches!(
            http_stream.proxy_task_state.current_writer,
            Some(ProxyTaskWriter::WritingBody(_))
        ),
        "Should be mid-write of body task - writer owns the 'world' task state"
    );

    // Verify body_bytes_sent only counts the successfully written "hello" (5 bytes)
    // not the timed-out "world"
    assert_eq!(
        http_stream.body_bytes_sent(),
        5,
        "Should only count the 5 bytes from 'hello', not the incomplete 'world' write"
    );

    // On next call to write_proxy_tasks(), Step 1 will resume the "world" write
}

use crate::protocols::http::v1::test_util::FlushTrackingMock;

// Test that write_continue_response can be called before write_proxy_tasks
// and both work correctly together.
#[tokio::test]
async fn test_continue_response_before_proxy_tasks() {
    init_log();

    // Expected bytes written:
    // 1. 100 Continue response
    // 2. 200 OK response header
    // 3. Body data
    let expected_continue = b"HTTP/1.1 100 Continue\r\n\r\n";
    let expected_header = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
    let expected_body = b"hello";

    let mock_io = Builder::new()
        .write(expected_continue)
        .write(expected_header)
        .write(expected_body)
        .build();

    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false; // Disable automatic headers

    // First, write the 100 Continue response
    http_stream.write_continue_response().await.unwrap();

    // Verify that 100 Continue was recorded
    assert!(
        http_stream.response_written().is_some(),
        "100 Continue should be recorded in response_written"
    );
    assert_eq!(
        http_stream.response_written().unwrap().status,
        StatusCode::CONTINUE,
        "Should have recorded 100 Continue"
    );

    // Now queue the actual response using proxy tasks
    let mut header = ResponseHeader::build(StatusCode::OK, Some(5)).unwrap();
    header.insert_header("Content-Length", "5").unwrap();
    http_stream.send_proxy_task(HttpTask::Header(Box::new(header), false));
    http_stream.send_proxy_task(HttpTask::Body(Some(Bytes::from("hello")), true));

    // Write all proxy tasks
    let end_stream = http_stream.write_proxy_tasks().await.unwrap();
    assert!(end_stream, "Should indicate end of stream");

    // Verify final response is 200 OK, not 100 Continue
    assert_eq!(
        http_stream.response_written().unwrap().status,
        StatusCode::OK,
        "Final response should be 200 OK, overwriting 100 Continue"
    );
}

#[tokio::test]
async fn test_head_response_with_content_length_flushes() {
    init_log();

    // HEAD request line + headers
    let request = b"HEAD / HTTP/1.1\r\nHost: example.com\r\n\r\n";
    let expected_header = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";

    let mock_io = Builder::new().read(request).write(expected_header).build();
    let (flush_mock, flush_count) = FlushTrackingMock::new(mock_io);
    let mut http_stream = HttpSession::new(Box::new(flush_mock));
    http_stream.update_resp_headers = false;

    // Read the HEAD request
    http_stream.read_request().await.unwrap();
    assert_eq!(http_stream.get_method(), Some(&Method::HEAD));

    // Queue header with Content-Length (body will be empty for HEAD)
    let mut header = ResponseHeader::build(StatusCode::OK, Some(2)).unwrap();
    header.insert_header("Content-Length", "100").unwrap();
    http_stream.send_proxy_task(HttpTask::Header(Box::new(header), true));

    let flush_before = FlushTrackingMock::flush_count(&flush_count);
    let end_stream = http_stream.write_proxy_tasks().await.unwrap();
    let flush_after = FlushTrackingMock::flush_count(&flush_count);

    assert!(end_stream, "HEAD response should be end of stream");
    assert!(
        flush_after > flush_before,
        "Should flush after writing HEAD response header with Content-Length \
         (body_writer.finished() is true). Got flush_before={flush_before}, \
         flush_after={flush_after}"
    );
}

#[tokio::test]
async fn test_204_response_with_content_length_flushes() {
    init_log();

    let request = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
    let expected_header = b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n";

    let mock_io = Builder::new().read(request).write(expected_header).build();
    let (flush_mock, flush_count) = FlushTrackingMock::new(mock_io);
    let mut http_stream = HttpSession::new(Box::new(flush_mock));
    http_stream.update_resp_headers = false;

    http_stream.read_request().await.unwrap();

    let mut header = ResponseHeader::build(StatusCode::NO_CONTENT, Some(2)).unwrap();
    header.insert_header("Content-Length", "0").unwrap();
    http_stream.send_proxy_task(HttpTask::Header(Box::new(header), true));

    let flush_before = FlushTrackingMock::flush_count(&flush_count);
    let end_stream = http_stream.write_proxy_tasks().await.unwrap();
    let flush_after = FlushTrackingMock::flush_count(&flush_count);

    assert!(end_stream, "204 response should be end of stream");
    assert!(
        flush_after > flush_before,
        "Should flush after writing 204 response header with Content-Length \
         (body_writer.finished() is true). Got flush_before={flush_before}, \
         flush_after={flush_after}"
    );
}

#[tokio::test]
#[should_panic(
    expected = "Unexpected UpgradedBody task received on un-upgraded downstream session"
)]
async fn test_upgraded_body_on_non_upgraded_session_panics() {
    init_log();

    let request = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
    let expected_header = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    // UpgradedBody on a non-upgraded session should panic before writing,
    // but if the bug exists, BodyWriter would encode it as a chunk:
    let expected_chunk = b"5\r\nhello\r\n";
    let expected_finish = b"0\r\n\r\n";

    let mock_io = Builder::new()
        .read(request)
        .write(expected_header)
        // If the panic check is missing, the body gets written as a chunk
        .write(expected_chunk)
        .write(expected_finish)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false;

    http_stream.read_request().await.unwrap();
    assert!(
        !http_stream.was_upgraded(),
        "Session should NOT be upgraded"
    );

    // Queue a normal header
    let mut header = ResponseHeader::build(StatusCode::OK, Some(2)).unwrap();
    header
        .insert_header("Transfer-Encoding", "chunked")
        .unwrap();
    http_stream.send_proxy_task(HttpTask::Header(Box::new(header), false));

    // Queue an UpgradedBody task on a non-upgraded session — should panic
    http_stream.send_proxy_task(HttpTask::UpgradedBody(Some(Bytes::from("hello")), true));

    // This should panic before/during the body write
    let _ = http_stream.write_proxy_tasks().await;
}

#[tokio::test]
#[should_panic(expected = "Unexpected Body task received on upgraded downstream session")]
async fn test_body_on_upgraded_session_panics() {
    init_log();

    // Upgrade request
    let request =
        b"GET / HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";
    // 101 Switching Protocols response
    let expected_header =
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";
    // If the panic check is missing, Body data would be written raw (close-delimited)
    let expected_body = b"hello";

    let mock_io = Builder::new()
        .read(request)
        .write(expected_header)
        .write(expected_body)
        .build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.update_resp_headers = false;

    http_stream.read_request().await.unwrap();

    // Queue 101 header to complete the upgrade
    let mut header = ResponseHeader::build(StatusCode::SWITCHING_PROTOCOLS, Some(3)).unwrap();
    header.insert_header("Upgrade", "websocket").unwrap();
    header.insert_header("Connection", "Upgrade").unwrap();
    http_stream.send_proxy_task(HttpTask::Header(Box::new(header), false));

    // Queue a regular Body task on what will be an upgraded session — should panic
    http_stream.send_proxy_task(HttpTask::Body(Some(Bytes::from("hello")), true));

    // This should panic (after writing the header, session becomes upgraded,
    // then the Body task should be rejected)
    let _ = http_stream.write_proxy_tasks().await;
}
/// The h1 counterpart of
/// `v2::server::test::test_read_body_timeout_survives_select_cancellation`.
///
/// The proxy polls the downstream body read from a `tokio::select!` branch,
/// so any other ready branch drops the read future. The bound must be a
/// deadline that survives that, or an upstream chattier than the timeout
/// rearms it forever and a stalled client pins the pump.
#[tokio::test]
async fn test_read_http_body_timeout_survives_select_cancellation() {
    /// Well under `TEST_READ_TIMEOUT`: the read is cancelled and rebuilt
    /// several times before the deadline is reached.
    const COMPETING_BRANCH_TICK: Duration = Duration::from_millis(100);

    let mut http_stream = HttpSession::new(mocked_blocking_body_forever_stream());
    http_stream.read_timeout = Some(TEST_READ_TIMEOUT);
    http_stream.read_request().await.unwrap();

    let mut cancellations = 0u32;
    let res = test_read_with_tokio_timeout(async {
        loop {
            tokio::select! {
                body = http_stream.read_body_bytes() => break body,
                _ = tokio::time::sleep(COMPETING_BRANCH_TICK) => {
                    cancellations += 1;
                }
            }
        }
    })
    .await;

    assert!(
        res.is_ok(),
        "an unrelated ready select! branch must not rearm the request-body idle bound"
    );
    assert_eq!(res.unwrap().unwrap_err().etype(), &ReadTimedout);
    assert!(
        cancellations >= 2,
        "the competing branch must have cancelled the read at least twice for this \
         to test anything; saw {cancellations}"
    );
}
