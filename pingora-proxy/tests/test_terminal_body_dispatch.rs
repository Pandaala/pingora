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

//! End-to-end coverage for the single terminal `upstream_response_body_filter`
//! callback (`proxy_common::TerminalBodyDispatch`).
//!
//! `HttpProxy::upstream_filter` reaches the body filter only from a
//! `Body`/`UpgradedBody` task. On HTTP/2 the `END_STREAM` flag rides the
//! trailers HEADERS frame, so a trailered response emits every DATA frame with
//! `eos = false` and terminates with `Trailer` then `Done` -- neither of which
//! used to reach the body filter. A processor that withholds bytes until
//! end-of-stream therefore never released them and the client saw an empty
//! body.
//!
//! The origins here are in-process `h2` servers, so these tests need no
//! external process (the `tests/utils` harness needs a local openresty mock
//! origin). `x-retain-until-eos` selects the withholding processor in
//! `tests/utils/server_utils.rs`; it appends a `|eos` marker from the terminal
//! callback itself, so the client-visible body doubles as the callback count.

mod utils;

use bytes::Bytes;
use http::{HeaderMap, Response, StatusCode};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, net::TcpStream};
use utils::server_utils::{
    init_without_mock_origin, take_downstream_trailer_filter_calls,
    take_downstream_trailer_logging_error, take_emit_chunk_limit_logging_error,
    take_eos_dispatches,
};

use pingora_proxy::RESPONSE_BODY_EMIT_CHUNK_BUDGET;

const CHUNKS: [&str; 3] = ["alpha", "beta", "gamma"];
const COMPRESSIBLE_BODY_LEN: usize = 4096;

fn whole_body() -> String {
    CHUNKS.concat()
}

/// How the origin ends the response body.
#[derive(Clone, Copy)]
enum Termination {
    /// DATA frames, then a trailers HEADERS frame carrying END_STREAM.
    /// Tasks: `Body(.., false)`… -> `Trailer(Some)` -> `Done`.
    Trailers,
    /// DATA frames, the last one carrying END_STREAM.
    /// Tasks: `Body(.., false)`… -> `Body(Some, true)`.
    EndStreamOnLastData,
    /// DATA frames, then an empty DATA frame carrying END_STREAM.
    /// Tasks: `Body(.., false)`… -> `Body(Some(empty), true)`.
    EndStreamOnEmptyData,
    /// No body at all: END_STREAM on the response HEADERS frame.
    /// Tasks: `Header(h, true)`.
    EndStreamOnHeaders,
}

/// Spawn a cleartext-h2 origin that answers ONE request with `CHUNKS` and ends
/// the response the way `how` says.
async fn spawn_origin(how: Termination) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let response = Response::builder().status(StatusCode::OK).body(()).unwrap();

        if matches!(how, Termination::EndStreamOnHeaders) {
            send_resp.send_response(response, true).unwrap();
            while conn.accept().await.is_some() {}
            return;
        }

        let mut body = send_resp.send_response(response, false).unwrap();
        let last = CHUNKS.len() - 1;
        for (i, chunk) in CHUNKS.iter().enumerate() {
            let end = matches!(how, Termination::EndStreamOnLastData) && i == last;
            body.send_data(Bytes::from_static(chunk.as_bytes()), end)
                .unwrap();
        }
        match how {
            Termination::Trailers => {
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                body.send_trailers(trailers).unwrap();
            }
            Termination::EndStreamOnEmptyData => {
                body.send_data(Bytes::new(), true).unwrap();
            }
            Termination::EndStreamOnLastData | Termination::EndStreamOnHeaders => {}
        }
        while conn.accept().await.is_some() {}
    });
    port
}

/// Spawn a cleartext-h2 origin that keeps accepting streams and connections.
/// The counters make connection reuse observable independently of proxy-added
/// response headers.
async fn spawn_reusable_trailered_origin() -> (u16, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let connections = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let origin_connections = connections.clone();
    let origin_requests = requests.clone();

    tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            origin_connections.fetch_add(1, Ordering::SeqCst);
            let requests = origin_requests.clone();
            tokio::spawn(async move {
                let Ok(mut conn) = h2::server::handshake(io).await else {
                    return;
                };
                while let Some(Ok((_req, mut send_resp))) = conn.accept().await {
                    requests.fetch_add(1, Ordering::SeqCst);
                    let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
                    let Ok(mut body) = send_resp.send_response(response, false) else {
                        continue;
                    };
                    for chunk in CHUNKS {
                        if body
                            .send_data(Bytes::from_static(chunk.as_bytes()), false)
                            .is_err()
                        {
                            break;
                        }
                    }
                    let mut trailers = HeaderMap::new();
                    trailers.insert("grpc-status", "0".parse().unwrap());
                    let _ = body.send_trailers(trailers);
                }
            });
        }
    });

    (port, connections, requests)
}

async fn spawn_reusable_h1_trailered_origin() -> (u16, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let connections = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let origin_connections = connections.clone();
    let origin_requests = requests.clone();

    tokio::spawn(async move {
        while let Ok((mut io, _)) = listener.accept().await {
            origin_connections.fetch_add(1, Ordering::SeqCst);
            let requests = origin_requests.clone();
            tokio::spawn(async move {
                let mut pending = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    while !pending.windows(4).any(|window| window == b"\r\n\r\n") {
                        match io.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => pending.extend_from_slice(&chunk[..read]),
                        }
                    }
                    let header_end = pending
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .unwrap()
                        + 4;
                    pending.drain(..header_end);
                    requests.fetch_add(1, Ordering::SeqCst);
                    if io
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                              5\r\nalpha\r\n4\r\nbeta\r\n5\r\ngamma\r\n\
                              0\r\ngrpc-status: 0\r\n\r\n",
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });

    (port, connections, requests)
}

/// Emit HEADERS, one DATA frame, and trailers without yielding so the proxy
/// can pull all three tasks into one channel batch.
async fn spawn_single_chunk_trailered_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut body = send_resp.send_response(response, false).unwrap();
        body.send_data(Bytes::from_static(b"alpha"), false).unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        body.send_trailers(trailers).unwrap();
        while conn.accept().await.is_some() {}
    });
    port
}

async fn spawn_content_length_trailered_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_LENGTH, whole_body().len())
            .body(())
            .unwrap();
        let mut body = send_resp.send_response(response, false).unwrap();
        for chunk in CHUNKS {
            body.send_data(Bytes::from_static(chunk.as_bytes()), false)
                .unwrap();
        }
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        body.send_trailers(trailers).unwrap();
        while conn.accept().await.is_some() {}
    });
    port
}

async fn spawn_h1_chunked_origin(with_trailers: bool) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = io.read(&mut chunk).await.unwrap();
            if read == 0 {
                return;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        let terminal = if with_trailers {
            "0\r\ngrpc-status: 0\r\nx-origin: h1\r\n\r\n"
        } else {
            "0\r\n\r\n"
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
             5\r\nalpha\r\n4\r\nbeta\r\n{terminal}"
        );
        io.write_all(response.as_bytes()).await.unwrap();
        io.shutdown().await.unwrap();
    });
    port
}

async fn spawn_h1_compressible_trailered_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = io.read(&mut chunk).await.unwrap();
            if read == 0 {
                return;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        let body = "a".repeat(COMPRESSIBLE_BODY_LEN);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\
             Connection: close\r\n\r\n{:X}\r\n{body}\r\n0\r\ngrpc-status: 0\r\n\r\n",
            body.len()
        );
        io.write_all(response.as_bytes()).await.unwrap();
        io.shutdown().await.unwrap();
    });
    port
}

async fn raw_h1_get(
    origin_port: u16,
    use_h2: bool,
    version: &str,
    extra_headers: &[(&str, &str)],
) -> (Duration, String) {
    let mut stream = TcpStream::connect("127.0.0.1:6147").await.unwrap();
    let mut request = format!(
        "GET /terminal-body {version}\r\nHost: 127.0.0.1\r\nx-port: {origin_port}\r\nConnection: close\r\n"
    );
    if use_h2 {
        request.push_str("x-h2: true\r\n");
    }
    for (name, value) in extra_headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    let started = tokio::time::Instant::now();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    (started.elapsed(), String::from_utf8(response).unwrap())
}

async fn raw_h1_get_and_probe_downstream_close(
    origin_port: u16,
    use_h2: bool,
    extra_headers: &[(&str, &str)],
) -> String {
    let mut stream = TcpStream::connect("127.0.0.1:6147").await.unwrap();
    let mut request =
        format!("GET /terminal-body HTTP/1.1\r\nHost: 127.0.0.1\r\nx-port: {origin_port}\r\n");
    if use_h2 {
        request.push_str("x-h2: true\r\n");
    }
    for (name, value) in extra_headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
        .await
        .expect("failed trailer-filter exchange left the downstream connection open")
        .unwrap();

    let second_request =
        b"GET /must-not-run HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    if stream.write_all(second_request).await.is_ok() {
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_millis(200), stream.read(&mut byte)).await;
        assert!(
            matches!(read, Ok(Ok(0)) | Ok(Err(_))),
            "the failed response's downstream connection accepted a second request"
        );
    }

    String::from_utf8(response).unwrap()
}

/// Spawn a cacheable trailered origin. Answers ONE request: the cache hit that
/// follows must be served without touching it.
async fn spawn_cacheable_trailered_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CACHE_CONTROL, "public, max-age=60")
            .body(())
            .unwrap();
        let mut body = send_resp.send_response(response, false).unwrap();
        for chunk in CHUNKS.iter() {
            body.send_data(Bytes::from_static(chunk.as_bytes()), false)
                .unwrap();
        }
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        body.send_trailers(trailers).unwrap();
        while conn.accept().await.is_some() {}
    });
    port
}

/// Spawn a one-request H2 origin whose body is large enough to exercise gzip
/// finalization at the trailer boundary. `cacheable` lets the same response
/// shape cover both live delivery and cache fill/hit byte identity.
async fn spawn_compressible_trailered_origin(cacheable: bool) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/plain");
        if cacheable {
            builder = builder.header(http::header::CACHE_CONTROL, "public, max-age=60");
        }
        let response = builder.body(()).unwrap();
        let mut body = send_resp.send_response(response, false).unwrap();
        body.send_data(Bytes::from(vec![b'a'; COMPRESSIBLE_BODY_LEN]), false)
            .unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        body.send_trailers(trailers).unwrap();
        while conn.accept().await.is_some() {}
    });
    port
}

/// Spawn a cleartext-h2 origin that sends part of the body and then resets the
/// stream with an error: `Failed`, never a normal termination.
///
/// The returned flag is set when this origin accepts the proxied request, which
/// happens before it sends anything the client could observe. It lets the test
/// tell an upstream reset apart from a request that never reached this origin
/// at all -- a bind failure, a refused connection, or a proxy that answered 502
/// on its own.
async fn spawn_aborting_origin() -> (u16, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let reached = Arc::new(AtomicBool::new(false));
    let origin_reached = reached.clone();
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(io).await.unwrap();
        let (_req, mut send_resp) = conn.accept().await.unwrap().unwrap();
        origin_reached.store(true, Ordering::SeqCst);
        let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let mut body = send_resp.send_response(response, false).unwrap();
        body.send_data(Bytes::from_static(b"partial"), false)
            .unwrap();
        // Let the DATA frame reach the wire: `send_reset` clears the stream's
        // pending send queue.
        tokio::select! {
            _ = async { while conn.accept().await.is_some() {} } => {}
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        body.send_reset(h2::Reason::INTERNAL_ERROR);
        while conn.accept().await.is_some() {}
    });
    (port, reached)
}

async fn get(port: u16, retain: bool) -> reqwest::Result<reqwest::Response> {
    let mut req = reqwest::Client::new()
        .get("http://127.0.0.1:6147/terminal-body")
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .timeout(Duration::from_secs(10));
    if retain {
        req = req.header("x-retain-until-eos", "true");
    }
    req.send().await
}

async fn get_with_head_hold(
    client: &reqwest::Client,
    port: u16,
    use_h2_upstream: bool,
    decision: &str,
) -> reqwest::Result<reqwest::Response> {
    let mut request = client
        .get("http://127.0.0.1:6147/terminal-body")
        .header("x-port", port.to_string())
        .header("x-response-head-hold", decision)
        .timeout(Duration::from_secs(10));
    if use_h2_upstream {
        request = request.header("x-h2", "true");
    }
    request.send().await
}

/// `get(port, true)` plus an `x-eos-probe` id, so the harness also counts the
/// terminal dispatch out of band (`take_eos_dispatches`).
async fn get_probed(port: u16, probe: &str) -> reqwest::Result<reqwest::Response> {
    reqwest::Client::new()
        .get("http://127.0.0.1:6147/terminal-body")
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-retain-until-eos", "true")
        .header("x-eos-probe", probe)
        .timeout(Duration::from_secs(10))
        .send()
        .await
}

async fn get_with_chunk_limit(
    port: u16,
    mode: &str,
    probe: Option<&str>,
) -> reqwest::Result<reqwest::Response> {
    let mut request = reqwest::Client::new()
        .get("http://127.0.0.1:6147/terminal-body")
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-emit-chunk-limit", mode)
        .timeout(Duration::from_secs(10));
    if let Some(probe) = probe {
        request = request.header("x-emit-chunk-limit-probe", probe);
    }
    request.send().await
}

async fn wait_for_emit_chunk_limit_error(probe: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if let Some(error) = take_emit_chunk_limit_logging_error(probe) {
            return error;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "chunk-limit request {probe} did not reach logging with an error"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn h2_upstream_drains_exactly_the_emitted_chunk_limit() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::EndStreamOnLastData).await;
    let response = get_with_chunk_limit(port, "exact", None).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await.unwrap();
    assert_eq!(body.len(), RESPONSE_BODY_EMIT_CHUNK_BUDGET);
    assert!(body.iter().all(|byte| *byte == b'x'));
}

#[tokio::test]
async fn h2_upstream_rejects_the_next_chunk_after_the_limit() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::EndStreamOnLastData).await;
    let probe = format!("h2-emit-chunk-overflow-{}", std::process::id());
    let response = get_with_chunk_limit(port, "overflow", Some(&probe)).await;
    let completed_ok = match response {
        Ok(response) if response.status() == StatusCode::OK => response.bytes().await.ok(),
        _ => None,
    };
    assert!(
        completed_ok.is_none(),
        "an H2 response that exceeds the emitted-chunk limit must fail closed"
    );
    let error = wait_for_emit_chunk_limit_error(&probe).await;
    let expected = format!(
        "response body emit chunk budget exhausted: maximum {} nonempty chunks per batch",
        RESPONSE_BODY_EMIT_CHUNK_BUDGET
    );
    assert!(
        error.starts_with("InternalError|") && error.contains(&expected),
        "H2 overflow failed for the wrong reason: {error}"
    );
}

#[tokio::test]
async fn response_head_hold_releases_or_replaces_on_h1_and_h2_upstream_pumps() {
    init_without_mock_origin();
    let client = reqwest::Client::new();

    let h1_release_origin = spawn_h1_chunked_origin(false).await;
    let response = get_with_head_hold(&client, h1_release_origin, false, "release")
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "alphabeta");

    let h1_replace_origin = spawn_h1_chunked_origin(false).await;
    let response = get_with_head_hold(&client, h1_replace_origin, false, "replace")
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.headers()["x-response-head-replacement"], "true");
    assert_eq!(response.text().await.unwrap(), "blocked");

    let h2_release_origin = spawn_origin(Termination::EndStreamOnLastData).await;
    let response = get_with_head_hold(&client, h2_release_origin, true, "release")
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), whole_body());

    let h2_replace_origin = spawn_origin(Termination::EndStreamOnLastData).await;
    let response = get_with_head_hold(&client, h2_replace_origin, true, "replace")
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.headers()["x-response-head-replacement"], "true");
    assert_eq!(response.text().await.unwrap(), "blocked");
}

#[tokio::test]
async fn response_head_replacement_drops_h1_reuse_but_preserves_the_h2_connection() {
    init_without_mock_origin();
    let client = reqwest::Client::new();

    let (h1_port, h1_connections, h1_requests) = spawn_reusable_h1_trailered_origin().await;
    let replaced = get_with_head_hold(&client, h1_port, false, "replace")
        .await
        .unwrap();
    assert_eq!(replaced.status(), StatusCode::FORBIDDEN);
    assert_eq!(replaced.text().await.unwrap(), "blocked");
    let released = get_with_head_hold(&client, h1_port, false, "release")
        .await
        .unwrap();
    assert_eq!(released.status(), StatusCode::OK);
    assert_eq!(released.text().await.unwrap(), whole_body());
    assert_eq!(h1_requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        h1_connections.load(Ordering::SeqCst),
        2,
        "a replaced H1 origin response must not return its connection to the pool"
    );

    let (h2_port, h2_connections, h2_requests) = spawn_reusable_trailered_origin().await;
    let replaced = get_with_head_hold(&client, h2_port, true, "replace")
        .await
        .unwrap();
    assert_eq!(replaced.status(), StatusCode::FORBIDDEN);
    assert_eq!(replaced.text().await.unwrap(), "blocked");
    let released = get_with_head_hold(&client, h2_port, true, "release")
        .await
        .unwrap();
    assert_eq!(released.status(), StatusCode::OK);
    assert_eq!(released.text().await.unwrap(), whole_body());
    assert_eq!(h2_requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        h2_connections.load(Ordering::SeqCst),
        1,
        "replacement must cancel only the H2 stream, not the shared connection"
    );
}

/// The defect: an H2 upstream ending in trailers used to deliver no
/// `end_of_stream` at all, so the withheld body was never released and the
/// client received an empty 200.
#[tokio::test]
async fn trailered_response_releases_the_withheld_body() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::Trailers).await;
    let response = get(port, true).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.unwrap(),
        format!("{}|eos", whole_body())
    );
}

#[tokio::test]
async fn header_event_exposes_real_end_of_stream_evidence() {
    init_without_mock_origin();
    let terminal = spawn_origin(Termination::EndStreamOnHeaders).await;
    let response = get(terminal, false).await.unwrap();
    assert_eq!(response.headers()["x-upstream-header-eos"], "true");

    let trailered = spawn_origin(Termination::Trailers).await;
    let response = get(trailered, false).await.unwrap();
    assert_eq!(response.headers()["x-upstream-header-eos"], "false");
}

#[tokio::test]
async fn async_trailer_hook_is_awaited_mutates_after_terminal_and_before_h1_write() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::Trailers).await;
    let (elapsed, wire) = raw_h1_get(
        port,
        true,
        "HTTP/1.1",
        &[
            ("x-retain-until-eos", "true"),
            ("x-trailer-delay-ms", "100"),
            ("x-trailer-mutate", "true"),
            ("x-assert-trailer-order", "true"),
            ("x-assert-trailer-capability", "true"),
        ],
    )
    .await;

    assert!(
        elapsed >= Duration::from_millis(100),
        "hook was not awaited: {elapsed:?}"
    );
    let released = wire.find("|eos").expect("typed terminal output missing");
    let trailer = wire
        .find("x-filtered-trailer: yes")
        .expect("mutated trailer missing from H1 wire");
    assert!(
        released < trailer,
        "terminal output followed trailers: {wire:?}"
    );
    assert!(wire.contains("grpc-status: 0"));
}

#[tokio::test]
async fn same_batch_header_data_and_trailer_use_planned_h1_capability() {
    init_without_mock_origin();
    let port = spawn_single_chunk_trailered_origin().await;
    let (_, wire) = raw_h1_get(
        port,
        true,
        "HTTP/1.1",
        &[
            ("x-assert-trailer-capability", "true"),
            ("x-assert-response-uncommitted", "true"),
        ],
    )
    .await;

    assert!(wire.contains("5\r\nalpha\r\n"), "wire was {wire:?}");
    assert!(wire.contains("grpc-status: 0"), "wire was {wire:?}");
}

#[tokio::test]
async fn h1_http10_drops_real_trailers_and_uses_close_delimited_framing() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::Trailers).await;
    let (_, wire) = raw_h1_get(
        port,
        true,
        "HTTP/1.0",
        &[("x-assert-trailer-capability", "false")],
    )
    .await;

    let lower = wire.to_ascii_lowercase();
    assert!(wire.starts_with("HTTP/1.0 200"), "wire was {wire:?}");
    assert!(!lower.contains("transfer-encoding"), "wire was {wire:?}");
    assert!(lower.contains("connection: close"), "wire was {wire:?}");
    assert!(!lower.contains("grpc-status"), "wire was {wire:?}");
    assert!(wire.ends_with(&whole_body()), "wire was {wire:?}");
}

#[tokio::test]
async fn cleared_trailer_map_uses_standard_chunked_terminator() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::Trailers).await;
    let (_, wire) = raw_h1_get(
        port,
        true,
        "HTTP/1.1",
        &[
            ("x-assert-trailer-capability", "true"),
            ("x-trailer-clear", "true"),
        ],
    )
    .await;

    assert!(!wire.contains("grpc-status"), "wire was {wire:?}");
    assert!(wire.ends_with("0\r\n\r\n"), "wire was {wire:?}");
}

#[tokio::test]
async fn content_length_response_drops_real_trailers_without_late_error() {
    init_without_mock_origin();
    let port = spawn_content_length_trailered_origin().await;
    let (_, wire) = raw_h1_get(
        port,
        true,
        "HTTP/1.1",
        &[("x-assert-trailer-capability", "false")],
    )
    .await;

    let lower = wire.to_ascii_lowercase();
    assert!(wire.starts_with("HTTP/1.1 200"), "wire was {wire:?}");
    assert!(
        lower.contains(&format!("content-length: {}", whole_body().len())),
        "wire was {wire:?}"
    );
    assert!(!lower.contains("grpc-status"), "wire was {wire:?}");
    assert!(wire.ends_with(&whole_body()), "wire was {wire:?}");
}

#[tokio::test]
async fn async_trailer_hook_error_prevents_trailer_forwarding() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::Trailers).await;
    let (_, wire) = raw_h1_get(port, true, "HTTP/1.1", &[("x-trailer-error", "true")]).await;

    assert!(wire.starts_with("HTTP/1.1 200"), "wire was {wire:?}");
    assert!(!wire.contains("grpc-status"), "wire was {wire:?}");
}

#[tokio::test]
async fn downstream_trailer_filter_error_aborts_h1_and_h2_upstream_responses() {
    init_without_mock_origin();

    for (protocol, use_h2, origin_port) in [
        ("h1", false, spawn_h1_chunked_origin(true).await),
        ("h2", true, spawn_origin(Termination::Trailers).await),
    ] {
        let probe = format!("downstream-trailer-error-{protocol}-{}", std::process::id());
        let wire = raw_h1_get_and_probe_downstream_close(
            origin_port,
            use_h2,
            &[
                ("x-downstream-trailer-error", "true"),
                ("x-downstream-trailer-probe", &probe),
                ("x-retain-until-eos", "true"),
            ],
        )
        .await;

        assert!(
            wire.starts_with("HTTP/1.1 500") || wire.starts_with("HTTP/1.1 200"),
            "{protocol} trailer rejection produced an unexpected response: {wire:?}"
        );
        assert_eq!(
            take_downstream_trailer_filter_calls(&probe),
            1,
            "{protocol} downstream trailer hook did not run exactly once"
        );
        assert!(
            !wire.contains("grpc-status"),
            "{protocol} rejected trailer reached the wire: {wire:?}"
        );
        assert!(
            !wire.contains("|eos"),
            "{protocol} terminal sink output leaked after trailer rejection: {wire:?}"
        );
        assert!(
            !wire.ends_with("0\r\n\r\n"),
            "{protocol} response completed cleanly after trailer rejection: {wire:?}"
        );
        let logged = take_downstream_trailer_logging_error(&probe)
            .unwrap_or_else(|| panic!("{protocol} trailer error did not reach logging"));
        assert!(
            logged.starts_with("InternalError|Unset|")
                && logged.contains("scripted downstream response trailer rejection"),
            "{protocol} logging lost the trailer error classification/context: {logged}"
        );
    }
}

#[tokio::test]
async fn downstream_trailer_filter_error_uses_existing_h1_upstream_discard_policy() {
    init_without_mock_origin();
    let (origin_port, connections, requests) = spawn_reusable_h1_trailered_origin().await;
    let probe = format!("downstream-trailer-h1-reuse-{}", std::process::id());

    let failed_wire = raw_h1_get_and_probe_downstream_close(
        origin_port,
        false,
        &[
            ("x-downstream-trailer-error", "true"),
            ("x-downstream-trailer-probe", &probe),
        ],
    )
    .await;
    assert_eq!(take_downstream_trailer_filter_calls(&probe), 1);
    assert!(!failed_wire.contains("grpc-status"));
    assert!(take_downstream_trailer_logging_error(&probe).is_some());

    let response = reqwest::Client::new()
        .get("http://127.0.0.1:6147/terminal-body")
        .header("x-port", origin_port.to_string())
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-conn-reuse")
            .and_then(|value| value.to_str().ok()),
        None,
        "the H1 failure path unexpectedly pooled the upstream session"
    );
    assert_eq!(response.text().await.unwrap(), whole_body());
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        connections.load(Ordering::SeqCst),
        2,
        "the H1 failure path did not use its conservative connection discard policy"
    );
}

#[tokio::test]
async fn downstream_trailer_filter_error_keeps_completed_h2_upstream_reusable() {
    init_without_mock_origin();
    let (origin_port, connections, requests) = spawn_reusable_trailered_origin().await;
    let probe = format!("downstream-trailer-reuse-{}", std::process::id());

    let (_, failed_wire) = raw_h1_get(
        origin_port,
        true,
        "HTTP/1.1",
        &[
            ("x-downstream-trailer-error", "true"),
            ("x-downstream-trailer-probe", &probe),
        ],
    )
    .await;
    assert_eq!(take_downstream_trailer_filter_calls(&probe), 1);
    assert!(!failed_wire.contains("grpc-status"));
    assert!(
        take_downstream_trailer_logging_error(&probe).is_some(),
        "trailer rejection did not finish before the reuse probe"
    );

    let response = get(origin_port, false).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-conn-reuse")
            .and_then(|v| v.to_str().ok()),
        Some("1"),
        "the next stream did not use Pingora's pooled H2 connection"
    );
    assert_eq!(response.text().await.unwrap(), whole_body());
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "the trailer-hook failure discarded a healthy H2 connection"
    );
}

#[tokio::test]
async fn h1_upstream_trailers_and_no_trailer_terminal_are_preserved_exactly_once() {
    init_without_mock_origin();
    let trailered = spawn_h1_chunked_origin(true).await;
    let (_, wire) = raw_h1_get(
        trailered,
        false,
        "HTTP/1.1",
        &[
            ("x-retain-until-eos", "true"),
            ("x-trailer-mutate", "true"),
            ("x-assert-trailer-order", "true"),
        ],
    )
    .await;
    assert_eq!(wire.matches("|eos").count(), 1, "wire was {wire:?}");
    assert!(wire.contains("grpc-status: 0"), "wire was {wire:?}");
    assert!(wire.contains("x-origin: h1"), "wire was {wire:?}");
    assert!(
        wire.contains("x-filtered-trailer: yes"),
        "wire was {wire:?}"
    );

    let no_trailers = spawn_h1_chunked_origin(false).await;
    let (_, wire) = raw_h1_get(
        no_trailers,
        false,
        "HTTP/1.1",
        &[("x-retain-until-eos", "true"), ("x-trailer-error", "true")],
    )
    .await;
    assert_eq!(wire.matches("|eos").count(), 1, "wire was {wire:?}");
    assert!(!wire.contains("grpc-status"), "wire was {wire:?}");
}

#[tokio::test]
async fn no_trailer_response_never_invokes_real_trailer_hook() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::EndStreamOnLastData).await;
    let response = reqwest::Client::new()
        .get("http://127.0.0.1:6147/terminal-body")
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-trailer-error", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), whole_body());
}

/// `Trailer` claims the termination and the `Done` behind it must not dispatch
/// again: a second callback would append a second `|eos` marker.
///
/// This case doubles as the positive control for the `x-eos-probe` counter.
/// `aborted_response_never_dispatches_a_terminal_callback` only ever asserts
/// that counter is 0, so a probe that silently stopped recording -- the call
/// dropped from the filter, the header name drifted, the request routed through
/// a proxy service that does not record -- would make it pass forever. Counting
/// a dispatch that must happen, in the same request that shows the `|eos`
/// marker, keeps the two observation channels checking each other.
#[tokio::test]
async fn trailered_response_dispatches_the_terminal_callback_exactly_once() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::Trailers).await;
    let probe = format!("trailered-{}-{port}", std::process::id());
    let body = get_probed(port, &probe)
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body.matches("|eos").count(), 1, "body was {body:?}");
    assert_eq!(
        take_eos_dispatches(&probe),
        1,
        "the x-eos-probe counter is not wired to the terminal callback"
    );
}

#[tokio::test]
async fn compressed_trailered_response_is_complete_and_dispatches_eos_once() {
    init_without_mock_origin();
    let port = spawn_compressible_trailered_origin(false).await;
    let probe = format!("compressed-trailered-{}-{port}", std::process::id());
    let response = reqwest::ClientBuilder::new()
        .gzip(true)
        .build()
        .unwrap()
        .get("http://127.0.0.1:6147/terminal-body")
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-upstream-compression", "true")
        .header(http::header::ACCEPT_ENCODING, "gzip")
        .header("x-eos-probe", &probe)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap(),
        Bytes::from(vec![b'a'; COMPRESSIBLE_BODY_LEN])
    );
    assert_eq!(take_eos_dispatches(&probe), 1);
}

#[tokio::test]
async fn h1_compressed_trailered_response_is_complete_and_dispatches_eos_once() {
    init_without_mock_origin();
    let port = spawn_h1_compressible_trailered_origin().await;
    let probe = format!("h1-compressed-trailered-{}-{port}", std::process::id());
    let response = reqwest::ClientBuilder::new()
        .gzip(true)
        .build()
        .unwrap()
        .get("http://127.0.0.1:6147/terminal-body")
        .header("x-port", port.to_string())
        .header(http::header::ACCEPT_ENCODING, "gzip")
        .header("x-eos-probe", &probe)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap(),
        Bytes::from(vec![b'a'; COMPRESSIBLE_BODY_LEN])
    );
    assert_eq!(take_eos_dispatches(&probe), 1);
}

/// Released bytes are body: they must reach the wire ahead of the trailer, not
/// after the task that terminates the response. A response whose bytes landed
/// after the terminal marker would arrive truncated.
#[tokio::test]
async fn released_bytes_precede_the_trailer() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::Trailers).await;
    let body = get(port, true).await.unwrap().text().await.unwrap();
    assert!(body.starts_with(&whole_body()), "body was {body:?}");
    assert!(body.ends_with("|eos"), "body was {body:?}");
}

/// A trailered response the proxy does not withhold must be byte-identical to
/// before the fix: the terminal callback releases nothing and adds nothing.
#[tokio::test]
async fn trailered_response_is_unchanged_without_a_withholding_filter() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::Trailers).await;
    let response = get(port, false).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), whole_body());
}

/// END_STREAM on the last DATA frame already delivered `eos = true` through the
/// ordinary path. It must claim the latch so the following `Done` cannot
/// dispatch a second callback.
#[tokio::test]
async fn end_stream_on_last_data_still_dispatches_exactly_once() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::EndStreamOnLastData).await;
    let body = get(port, true).await.unwrap().text().await.unwrap();
    assert_eq!(body, format!("{}|eos", whole_body()));
    assert_eq!(body.matches("|eos").count(), 1, "body was {body:?}");
}

/// Same, for the empty-DATA-frame framing.
#[tokio::test]
async fn end_stream_on_empty_data_dispatches_exactly_once() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::EndStreamOnEmptyData).await;
    let body = get(port, true).await.unwrap().text().await.unwrap();
    assert_eq!(body, format!("{}|eos", whole_body()));
    assert_eq!(body.matches("|eos").count(), 1, "body was {body:?}");
}

/// A bodyless response reaches the body filter through
/// `terminal_upstream_body_filter` on the `terminal_header` branch. It claims
/// the latch, so the `Done` behind it must not dispatch again.
#[tokio::test]
async fn end_stream_on_headers_dispatches_exactly_once() {
    init_without_mock_origin();
    let port = spawn_origin(Termination::EndStreamOnHeaders).await;
    let body = get(port, true).await.unwrap().text().await.unwrap();
    assert_eq!(body, "|eos", "body was {body:?}");
}

/// An aborted response must never be told its truncated body was complete: a
/// synthetic end-of-stream would make the processor release a partial body as
/// if it were the whole thing. The exchange has to fail instead.
///
/// Unlike the tests above, this one cannot read the callback count off the
/// client-visible body. The exchange it asserts on is the failing one, and a
/// client discards a partial body when collection fails -- so a regression that
/// released `partial|eos` and then broke the stream would leave the same empty
/// string a correct run leaves. The `x-eos-probe` counter is therefore the load-
/// bearing assertion; the body check only guards the client-facing half.
///
/// Scope note: this covers the `Failed` arm dispatching on its own. It does NOT
/// cover the other half of that arm -- `Failed` CLAIMING, so a `Done` behind it
/// cannot dispatch instead -- because the h2 pump stops at the error and never
/// emits that `Done`. The `failed_never_dispatches_and_suppresses_a_following_done`
/// unit test in `proxy_common` covers that mutation.
#[tokio::test]
async fn aborted_response_never_dispatches_a_terminal_callback() {
    init_without_mock_origin();
    let (port, origin_reached) = spawn_aborting_origin().await;
    let probe = format!("aborted-{}-{port}", std::process::id());

    let outcome = match get_probed(port, &probe).await {
        Ok(response) => {
            let status = response.status();
            Ok((status, response.text().await))
        }
        Err(e) => Err(e),
    };
    let dispatches = take_eos_dispatches(&probe);

    // Without this, every infrastructure failure short of a timeout -- an
    // unbound loopback address, an unreachable proxy, a refused origin
    // connection -- would satisfy the assertions below without ever exercising
    // the code under test.
    assert!(
        origin_reached.load(Ordering::SeqCst),
        "the request never reached the aborting origin: {outcome:?}"
    );

    match outcome {
        // The exchange must fail because the upstream reset it mid-body.
        Err(e) => assert!(
            !e.is_timeout() && !e.is_connect(),
            "expected an upstream reset, got {e:?}"
        ),
        Ok((status, body)) => {
            // The origin sends its 200 before resetting, so anything else --
            // 502 above all -- means the proxy gave up before the reset and
            // this is not the case under test.
            assert_eq!(status, StatusCode::OK, "body was {body:?}");
            if let Ok(body) = body {
                assert!(
                    !body.contains("|eos"),
                    "aborted response released a body as complete: {body:?}"
                );
            }
        }
    }

    assert_eq!(
        dispatches, 0,
        "aborted response dispatched the terminal body callback"
    );
}

/// The terminal dispatch must reach cache admission, not just the downstream
/// writer: the entity stored on the miss has to be the complete released body,
/// so the hit replays it byte-for-byte.
///
/// Scope note: this does NOT discriminate the cache ORDERING branch. `Trailer`
/// is a no-op in `cache_http_task`, so for a trailered response both orderings
/// store the same entity. Ordering only changes the stored entity for a bare
/// `Done` (which runs `finish_miss_handler`), a shape a well-behaved h2 origin
/// cannot be made to produce here; that path is covered by the
/// `drain_emitted_chunks_before` unit tests instead.
#[tokio::test]
async fn cached_body_matches_the_wire_body_for_a_trailered_response() {
    init_without_mock_origin();
    let port = spawn_cacheable_trailered_origin().await;
    let url = format!(
        "http://127.0.0.1:6148/terminal-body-cache-{}",
        std::process::id()
    );
    let client = reqwest::Client::new();

    let miss = client
        .get(&url)
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-retain-until-eos", "true")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(miss.headers().get("x-cache-status").unwrap(), "miss");
    let miss_body = miss.text().await.unwrap();
    assert_eq!(miss_body, format!("{}|eos", whole_body()));

    let hit = client
        .get(&url)
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-retain-until-eos", "true")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(hit.headers().get("x-cache-status").unwrap(), "hit");
    assert_eq!(hit.text().await.unwrap(), miss_body);
}

#[tokio::test]
async fn compressed_trailered_cache_hit_is_byte_identical_to_the_fill() {
    init_without_mock_origin();
    let port = spawn_compressible_trailered_origin(true).await;
    let url = format!(
        "http://127.0.0.1:6148/compressed-terminal-body-cache-{}",
        std::process::id()
    );
    let client = reqwest::ClientBuilder::new().gzip(false).build().unwrap();

    let miss = client
        .get(&url)
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-upstream-compression", "true")
        .header(http::header::ACCEPT_ENCODING, "gzip")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(miss.headers().get("x-cache-status").unwrap(), "miss");
    assert_eq!(
        miss.headers().get(http::header::CONTENT_ENCODING).unwrap(),
        "gzip"
    );
    let miss_body = miss.bytes().await.unwrap();

    let hit = client
        .get(&url)
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .header("x-upstream-compression", "true")
        .header(http::header::ACCEPT_ENCODING, "gzip")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    assert_eq!(hit.headers().get("x-cache-status").unwrap(), "hit");
    assert_eq!(
        hit.headers().get(http::header::CONTENT_ENCODING).unwrap(),
        "gzip"
    );
    assert_eq!(hit.bytes().await.unwrap(), miss_body);
}
