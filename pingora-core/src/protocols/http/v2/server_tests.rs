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
use futures::SinkExt;
use h2::frame::{Frame, Headers, Pseudo, Reset, Settings};
use http::Uri;
use http::{HeaderValue, Method, Request, StatusCode};
use tokio::io::{duplex, AsyncWriteExt, DuplexStream};
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

async fn advertised_settings(options: Option<H2Options>) -> Settings {
    let (mut client, server) = duplex(65536);
    let handshake = tokio::spawn(async move { handshake(Box::new(server), options).await });

    client
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    let mut codec: h2::Codec<DuplexStream, Bytes> = h2::Codec::new(client);
    let settings = match codec.next().await.unwrap().unwrap() {
        Frame::Settings(settings) => settings,
        frame => panic!("expected SETTINGS frame, received {frame:?}"),
    };

    let _ = handshake.await.unwrap().unwrap();
    settings
}

#[test]
fn test_authority_host_mismatch() {
    let request = |hosts: &[&str]| {
        let mut request = Request::builder()
            .uri("https://authority.example/test")
            .body(())
            .unwrap();
        for host in hosts {
            request
                .headers_mut()
                .append(header::HOST, HeaderValue::from_str(host).unwrap());
        }
        RequestHeader::from(request.into_parts().0)
    };

    assert!(!authority_host_mismatch(&request(&[])));
    assert!(!authority_host_mismatch(&request(&["authority.example"])));
    assert!(authority_host_mismatch(&request(&["other.example"])));
    assert!(authority_host_mismatch(&request(&["AUTHORITY.EXAMPLE"])));
    assert!(authority_host_mismatch(&request(&[
        "authority.example:443"
    ])));
    assert!(authority_host_mismatch(&request(&[
        "authority.example",
        "authority.example",
    ])));
    assert!(authority_host_mismatch(&request(&[
        "authority.example",
        "other.example",
    ])));

    let request = RequestHeader::from(
        Request::builder()
            .uri("/test")
            .header(header::HOST, "host.example")
            .body(())
            .unwrap()
            .into_parts()
            .0,
    );
    assert!(!authority_host_mismatch(&request));
}

#[tokio::test]
async fn test_server_rejects_authority_host_mismatch_with_400() {
    let (client, server) = duplex(65536);

    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let mut h2 = h2.ready().await.unwrap();
        let mismatched = Request::builder()
            .method(Method::GET)
            .uri("https://authority.example/test")
            .header(header::HOST, "other.example")
            .body(())
            .unwrap();
        let (response, request_body) = h2.send_request(mismatched, false).unwrap();

        let response = response.await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        drop(response);
        drop(request_body);

        let mut h2 = h2.ready().await.unwrap();
        let duplicate = Request::builder()
            .method(Method::GET)
            .uri("https://authority.example/test")
            .header(header::HOST, "authority.example")
            .header(header::HOST, "authority.example")
            .body(())
            .unwrap();
        let (response, _) = h2.send_request(duplicate, true).unwrap();

        let response = response.await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let mut body = response.into_body();
        assert!(body.data().await.is_none());

        let mut h2 = h2.ready().await.unwrap();
        let matching = Request::builder()
            .method(Method::GET)
            .uri("https://authority.example/test")
            .header(header::HOST, "authority.example")
            .body(())
            .unwrap();
        let (response, _) = h2.send_request(matching, true).unwrap();

        assert_eq!(response.await.unwrap().status(), StatusCode::NO_CONTENT);
    });

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let accepted = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap();
    assert!(
        matches!(accepted, Some(H2Accept::Rejected)),
        "mismatched authority reached the application"
    );

    let accepted = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap();
    assert!(
        matches!(accepted, Some(H2Accept::Rejected)),
        "duplicate Host fields reached the application"
    );

    let Some(H2Accept::Session(mut session)) =
        HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
    else {
        panic!("matching authority did not reach the application");
    };
    assert_eq!(
        session.req_header().headers[header::HOST],
        "authority.example"
    );
    session
        .write_response_header(
            Box::new(ResponseHeader::build(StatusCode::NO_CONTENT, Some(0)).unwrap()),
            true,
        )
        .unwrap();
    drop(session);

    let done = timeout(
        Duration::from_secs(1),
        HttpSession::from_h2_conn(&mut connection, digest),
    )
    .await
    .expect("connection did not finish after authority mismatch test")
    .expect("connection failed after authority mismatch test");
    assert!(done.is_none());

    client.await.unwrap();
}

#[tokio::test]
async fn test_failed_authority_rejection_write_is_stream_local() {
    let (mut client, server) = duplex(65536);
    let (frames_sent, wait_for_frames) = oneshot::channel();

    let client = tokio::spawn(async move {
        client
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        let mut codec: h2::Codec<DuplexStream, Bytes> = h2::Codec::new(client);
        codec.send(Settings::default().into()).await.unwrap();
        codec.send(Settings::ack().into()).await.unwrap();

        let mut fields = HeaderMap::new();
        fields.insert(header::HOST, HeaderValue::from_static("other.example"));
        let mut mismatched = Headers::new(
            1.into(),
            Pseudo::request(
                Method::GET,
                Uri::from_static("https://authority.example/test"),
                None,
            ),
            fields,
        );
        mismatched.set_end_headers();
        codec.send(mismatched.into()).await.unwrap();
        codec
            .send(Reset::new(1.into(), h2::Reason::CANCEL).into())
            .await
            .unwrap();

        let mut fields = HeaderMap::new();
        fields.insert(header::HOST, HeaderValue::from_static("authority.example"));
        let mut matching = Headers::new(
            3.into(),
            Pseudo::request(
                Method::GET,
                Uri::from_static("https://authority.example/test"),
                None,
            ),
            fields,
        );
        matching.set_end_headers();
        matching.set_end_stream();
        codec.send(matching.into()).await.unwrap();
        frames_sent.send(()).unwrap();

        timeout(Duration::from_secs(1), async {
            while let Some(frame) = codec.next().await {
                if let Frame::Headers(headers) = frame.unwrap() {
                    if headers.stream_id() == 3u32 {
                        return;
                    }
                }
            }
            panic!("connection closed before the sibling response");
        })
        .await
        .expect("timed out waiting for the sibling response");
    });

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    wait_for_frames.await.unwrap();
    let digest = Arc::new(Digest::default());

    let accepted = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap();
    assert!(
        matches!(accepted, Some(H2Accept::Rejected)),
        "reset authority mismatch reached the application"
    );

    let Some(H2Accept::Session(mut session)) =
        HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
    else {
        panic!("sibling stream was dropped after the failed 400 write");
    };
    session
        .write_response_header(
            Box::new(ResponseHeader::build(StatusCode::NO_CONTENT, Some(0)).unwrap()),
            true,
        )
        .unwrap();
    drop(session);

    let done = timeout(
        Duration::from_secs(1),
        HttpSession::from_h2_conn(&mut connection, digest),
    )
    .await
    .expect("connection did not finish after the sibling response")
    .expect("connection failed after the sibling response");
    assert!(done.is_none());
    client.await.unwrap();
}

#[tokio::test]
async fn test_authority_mismatch_exhausts_malformed_stream_budget() {
    let (client, server) = duplex(65536);

    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let mut h2 = h2.ready().await.unwrap();
        let mismatched = Request::builder()
            .method(Method::GET)
            .uri("https://authority.example/test")
            .header(header::HOST, "other.example")
            .body(())
            .unwrap();
        let (response, _) = h2.send_request(mismatched, true).unwrap();

        // The connection can close before the queued 400 is flushed when
        // this request exhausts the connection-level malformed budget.
        let _ = response.await;
    });

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let mut malformed_streams = MAX_MALFORMED_STREAMS_PER_CONN - 1;

    let err = match HttpSession::from_h2_conn_with_malformed_budget(
        &mut connection,
        digest,
        &mut malformed_streams,
    )
    .await
    {
        Ok(Some(_)) => panic!("authority mismatch must not surface as a session"),
        Ok(None) => panic!("connection ended before malformed budget was exhausted"),
        Err(err) => err,
    };

    assert_eq!(err.etype(), &ErrorType::H2Error);
    assert_eq!(malformed_streams, MAX_MALFORMED_STREAMS_PER_CONN);

    drop(connection);
    client.await.unwrap();
}

#[tokio::test]
async fn test_server_handshake_uses_bounded_default_options() {
    let settings = advertised_settings(None).await;

    assert_eq!(
        settings.max_header_list_size(),
        Some(DEFAULT_MAX_HEADER_LIST_SIZE)
    );
    assert_eq!(
        settings.max_concurrent_streams(),
        Some(DEFAULT_MAX_CONCURRENT_STREAMS)
    );
}

#[tokio::test]
async fn test_server_handshake_uses_caller_options() {
    let mut options = H2Options::default();
    options.max_header_list_size(1234);
    options.max_concurrent_streams(42);

    let settings = advertised_settings(Some(options)).await;

    assert_eq!(settings.max_header_list_size(), Some(1234));
    assert_eq!(settings.max_concurrent_streams(), Some(42));
}

#[tokio::test]
async fn test_zero_idle_timeout_waits_for_active_session() {
    let active = Arc::new(ActiveSessions::new());
    let guard = active.start_session();
    let timeout_active = active.clone();
    let timeout = tokio::spawn(async move {
        wait_for_idle_timeout(&timeout_active, Duration::ZERO).await;
    });

    tokio::task::yield_now().await;
    assert!(
        !timeout.is_finished(),
        "zero timeout must not spin or expire while a session is active"
    );

    drop(guard);
    pingora_timeout::timeout(Duration::from_secs(1), timeout)
        .await
        .expect("zero timeout did not expire after the session finished")
        .expect("timeout task panicked");
}

#[tokio::test]
async fn test_server_handshake_rejects_oversized_header_list_by_default() {
    let (client, server) = duplex(256 * 1024);

    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let mut request = Request::builder()
            .method(Method::GET)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        for _ in 0..2000 {
            request
                .headers_mut()
                .append("a", HeaderValue::from_static(""));
        }

        let (response, _) = h2
            .ready()
            .await
            .unwrap()
            .send_request(request, true)
            .unwrap();
        assert_eq!(
            response.await.unwrap().status(),
            http::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );
    });

    let server = tokio::spawn(async move {
        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let accepted = timeout(
            Duration::from_secs(1),
            HttpSession::from_h2_conn(&mut connection, digest),
        )
        .await;
        assert!(
            !matches!(accepted, Ok(Ok(Some(_)))),
            "oversized request reached the application"
        );
    });

    client.await.unwrap();
    server.await.unwrap();
}

#[cfg(feature = "patched_http1")]
#[tokio::test]
async fn test_server_rejects_forbidden_byte_in_request_target() {
    // Control bytes (CR/LF) may be accepted in the request path depending
    // on URI parsing. Ensure such a request target is rejected on ingest as
    // defense-in-depth, since these bytes are not permitted in a URI.
    let (client, server) = duplex(65536);

    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::GET)
            .uri("https://www.example.com/a\r\nX-Injected: 1")
            .body(())
            .unwrap();
        // Ensure CR/LF survived URI parsing, otherwise the test is a no-op.
        assert!(request.uri().path().contains('\n'));

        let (response, _) = h2.send_request(request, true).unwrap();
        // The stream must be rejected (reset), not answered.
        assert!(response.await.is_err());
    });

    let server = tokio::spawn(async move {
        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let accepted = timeout(
            Duration::from_secs(1),
            HttpSession::from_h2_conn(&mut connection, digest),
        )
        .await
        .expect("from_h2_conn hung: the offending stream was not rejected")
        .expect("from_h2_conn returned an error");
        // The offending stream is reset during acceptance, so `from_h2_conn`
        // yields `Rejected` rather than a session built from the forbidden
        // request target. Sibling streams and the connection are unaffected.
        assert!(
            matches!(accepted, Some(H2Accept::Rejected)),
            "request with forbidden byte in target was not rejected"
        );
    });

    client.await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn test_server_handshake_accept_request() {
    let (client, server) = duplex(65536);
    let client_body = "test client body";
    let server_body = "test server body";

    let mut expected_trailers = HeaderMap::new();
    expected_trailers.insert("test", HeaderValue::from_static("trailers"));
    let trailers = expected_trailers.clone();

    let mut handles = vec![];
    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });

        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::GET)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();

        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.reserve_capacity(client_body.len());
        req_body.send_data(client_body.into(), true).unwrap();

        let (head, mut body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
        let data = body.data().await.unwrap().unwrap();
        assert_eq!(data, server_body);
        let resp_trailers = body.trailers().await.unwrap().unwrap();
        assert_eq!(resp_trailers, expected_trailers);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(accepted) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        let H2Accept::Session(mut http) = accepted else {
            continue;
        };
        let trailers = trailers.clone();
        handles.push(tokio::spawn(async move {
            let req = http.req_header();
            assert_eq!(req.method, Method::GET);
            assert_eq!(req.uri, "https://www.example.com/");

            http.enable_retry_buffering();

            assert!(!http.is_body_empty());
            assert!(!http.is_body_done());

            let body = http.read_body_or_idle(false).await.unwrap().unwrap();
            assert_eq!(body, client_body);
            assert!(http.is_body_done());
            assert_eq!(http.body_bytes_read(), 16);

            let retry_body = http.get_retry_buffer().unwrap();
            assert_eq!(retry_body, client_body);

            // test idling before response header is sent
            tokio::select! {
                _ = http.idle() => {panic!("downstream should be idling")},
                _= tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
            }

            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            assert!(http
                .write_response_header(response_header.clone(), false)
                .is_ok());
            // this write should be ignored otherwise we will error
            assert!(http.write_response_header(response_header, false).is_ok());

            // test idling after response header is sent
            tokio::select! {
                _ = http.read_body_or_idle(false) => {panic!("downstream should be idling")},
                _= tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
            }

            // end: false here to verify finish() closes the stream nicely
            http.write_body(server_body.into(), false).await.unwrap();
            assert_eq!(http.body_bytes_sent(), 16);

            http.write_trailers(trailers).unwrap();
            http.finish().unwrap();
        }));
    }
    for handle in handles {
        // ensure no panics
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_req_conflicting_content_length_rejected() {
    let (client, server) = duplex(65536);

    let client_task = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        // Conflicting duplicate Content-Length values: unrecoverable framing.
        let request = Request::builder()
            .method(Method::GET)
            .uri("https://www.example.com/")
            .header("content-length", "5")
            .header("content-length", "6")
            .body(())
            .unwrap();

        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        // Send a body matching the first Content-Length value so the h2
        // codec accepts the stream and Pingora's duplicate-CL validation is
        // what rejects the request.
        req_body.reserve_capacity(5);
        req_body.send_data("abcde".into(), true).unwrap();

        // The server must reject the stream with PROTOCOL_ERROR rather than
        // surface the request to the application.
        let err = response.await.unwrap_err();
        assert_eq!(err.reason(), Some(h2::Reason::PROTOCOL_ERROR));
        // Dropping `h2` lets the connection close so the server loop exits.
    });

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    // The malformed stream is reset during acceptance, so it is reported as
    // rejected rather than surfaced to the application as a session.
    let accepted = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap();
    assert!(
        matches!(accepted, Some(H2Accept::Rejected)),
        "malformed request must not surface as a session"
    );

    let done = timeout(
        Duration::from_secs(1),
        HttpSession::from_h2_conn(&mut connection, digest),
    )
    .await
    .expect("from_h2_conn hung after rejecting malformed request")
    .expect("from_h2_conn returned an error after rejecting malformed request");
    assert!(done.is_none(), "connection should close after rejection");

    client_task.await.unwrap();
}

#[tokio::test]
async fn test_req_malformed_stream_budget_exhausted() {
    let (client, server) = duplex(65536);

    let client_task = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::GET)
            .uri("https://www.example.com/")
            .header("content-length", "5")
            .header("content-length", "6")
            .body(())
            .unwrap();

        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.reserve_capacity(5);
        req_body.send_data("abcde".into(), true).unwrap();
        let err = response.await.unwrap_err();
        if let Some(reason) = err.reason() {
            assert_eq!(reason, h2::Reason::PROTOCOL_ERROR);
        }
    });

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let mut malformed_streams = MAX_MALFORMED_STREAMS_PER_CONN - 1;

    let err = match HttpSession::from_h2_conn_with_malformed_budget(
        &mut connection,
        digest,
        &mut malformed_streams,
    )
    .await
    {
        Ok(Some(_)) => panic!("malformed request must not surface as a session"),
        Ok(None) => panic!("connection ended before malformed budget was exhausted"),
        Err(err) => err,
    };

    assert_eq!(err.etype(), &ErrorType::H2Error);
    assert_eq!(malformed_streams, MAX_MALFORMED_STREAMS_PER_CONN);

    drop(connection);
    client_task.await.unwrap();
}

#[tokio::test]
async fn test_req_content_length_eq_0_and_no_header_eos() {
    let (client, server) = duplex(65536);

    let server_body = "test server body";

    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });

        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .header("content-length", "0") // explicitly set
            .body(())
            .unwrap();

        let (response, mut req_body) = h2.send_request(request, false).unwrap(); // no EOS

        let (head, mut body) = response.await.unwrap().into_parts();

        assert_eq!(head.status, 200);
        let data = body.data().await.unwrap().unwrap();
        assert_eq!(data, server_body);

        req_body.send_data("".into(), true).unwrap(); // set EOS after read the resp body
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(accepted) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        let H2Accept::Session(mut http) = accepted else {
            continue;
        };
        handles.push(tokio::spawn(async move {
            let req = http.req_header();
            assert_eq!(req.method, Method::POST);
            assert_eq!(req.uri, "https://www.example.com/");

            // 1. Check body related methods
            http.enable_retry_buffering();
            assert!(http.is_body_empty());
            assert!(!http.request_headers_end_stream());
            assert!(!http.is_body_done());
            let retry_body = http.get_retry_buffer();
            assert!(retry_body.is_none());

            // 2. Send response
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            assert!(http
                .write_response_header(response_header.clone(), false)
                .is_ok());

            http.write_body(server_body.into(), false).await.unwrap();
            assert_eq!(http.body_bytes_sent(), 16);

            // 3. The client drops the response stream after sending its request EOS,
            // so this test observes the resulting reset.
            assert!(http.read_body_or_idle(false).await.is_err());
        }));
    }

    for handle in handles {
        // ensure no panics
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_req_header_no_eos_empty_data_with_eos() {
    let (client, server) = duplex(65536);

    let server_body = "test server body";

    let mut handles = vec![];
    // Keeps the client's response stream alive until the server has made
    // its observation: dropping it earlier puts a RST_STREAM on the wire
    // and the read under test would race that reset instead of observing
    // the empty END_STREAM DATA frame this test is named for.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();

        let (response, mut req_body) = h2.send_request(request, false).unwrap(); // no EOS

        let (head, mut body) = response.await.unwrap().into_parts();

        assert_eq!(head.status, 200);
        let data = body.data().await.unwrap().unwrap();
        assert_eq!(data, server_body);

        req_body.send_data("".into(), true).unwrap(); // set EOS after read the resp body
        let _ = done_rx.await;
        // Drain the response to EOS before dropping the stream. Newer h2
        // sends RST_STREAM(CANCEL) when a still-open recv stream is dropped,
        // which would race with the server reading the request EOS and turn
        // the server-side read into a stream-reset error.
        while let Some(chunk) = body.data().await {
            let chunk = chunk.expect("response body error");
            body.flow_control()
                .release_capacity(chunk.len())
                .expect("release capacity");
        }
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    let mut done_tx = Some(done_tx);
    while let Some(accepted) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        let H2Accept::Session(mut http) = accepted else {
            continue;
        };
        let done_tx = done_tx.take().expect("exactly one stream");
        handles.push(tokio::spawn(async move {
            let req = http.req_header();
            assert_eq!(req.method, Method::POST);
            assert_eq!(req.uri, "https://www.example.com/");

            // 1. Check body related methods
            http.enable_retry_buffering();
            assert!(!http.request_headers_end_stream());
            assert!(!http.is_body_empty());
            assert!(!http.is_body_done());
            let retry_body = http.get_retry_buffer();
            assert!(retry_body.is_none());

            // 2. Send response
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            assert!(http
                .write_response_header(response_header.clone(), false)
                .is_ok());

            http.write_body(server_body.into(), false).await.unwrap();
            assert_eq!(http.body_bytes_sent(), 16);

            // 3. The client's end of stream arrives as an EMPTY DATA frame
            // carrying END_STREAM -- the shape this test is named for. `h2`
            // hands that over as a zero-length chunk (NOT as `None`), and
            // the request must be reported as done and still empty
            // afterwards.
            let chunk = http
                .read_body_or_idle(false)
                .await
                .expect("an empty END_STREAM DATA frame is a clean end of body");
            assert_eq!(
                chunk.as_deref(),
                Some(&b""[..]),
                "the empty END_STREAM DATA frame is delivered as a zero-length chunk"
            );
            assert!(http.is_body_done());
            assert!(http.is_body_empty());
            assert_eq!(http.body_bytes_read(), 0);

            // 4. Finish the response so the client can drain it to EOS and
            //    close the stream cleanly instead of cancelling it.
            http.finish().unwrap();
            let _ = done_tx.send(());
        }));
    }

    for handle in handles {
        // ensure no panics
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_early_body_buffer_captures_replays_and_rejects_late_set() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abc".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
            http.set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                .unwrap();
            {
                let waker = futures::task::noop_waker();
                let mut poll_ctx = std::task::Context::from_waker(&waker);
                assert!(matches!(
                    http.poll_read_body_bytes(&mut poll_ctx),
                    Poll::Ready(Some(Err(_)))
                ));
            }
            let mut total = Vec::new();
            while let Some(chunk) = http.read_body_bytes().await.unwrap() {
                total.extend_from_slice(&chunk);
            }
            assert_eq!(total, b"abc");
            // Rewindable: every upstream attempt reads the same body in chunks.
            for _ in 0..2 {
                assert!(http.begin_request_body_replay().await.unwrap());
                assert_eq!(
                    http.read_body_or_idle(false).await.unwrap().unwrap(),
                    b"abc".as_slice()
                );
                assert!(http.read_body_or_idle(false).await.unwrap().is_none());
            }
            // Registering after the body was read must fail closed.
            assert!(http
                .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                .is_err());
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

/// Trailers after a COMPLETE, `content-length`-declared body.
///
/// The declared length is what pins the P1 split: source (iii) of
/// `request_body_complete()` is satisfied the moment the seventh byte
/// arrives, and folding that source into `is_body_done()` would make the
/// assertion below it (`!is_body_done()` after the payload) fail. That is
/// not a style preference -- both proxy pumps stop reading the downstream
/// body when `is_body_done()` reports `true`, so folding (iii) in would
/// silently drop these trailer fields and skip `request_trailer_filter`.
#[tokio::test]
async fn test_request_trailer_presence_is_transport_observed() {
    let (client, server) = duplex(65536);

    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .header("trailer", "x-checksum")
            .header("content-length", "7")
            .body(())
            .unwrap();
        let (response, mut body) = h2
            .ready()
            .await
            .unwrap()
            .send_request(request, false)
            .unwrap();
        body.send_data("payload".into(), false).unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", HeaderValue::from_static("ok"));
        body.send_trailers(trailers).unwrap();
        assert_eq!(response.await.unwrap().status(), StatusCode::NO_CONTENT);
    });

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let mut handlers = Vec::new();
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handlers.push(tokio::spawn(async move {
            assert!(!http.request_headers_end_stream());
            let data = http.read_body_bytes().await.unwrap().unwrap();
            assert_eq!(data, "payload");
            // The declared `content-length` is now fully received, but the
            // TRANSPORT has not ended: the trailers are still to come.
            // `is_body_done()` must report the transport, or the pumps stop
            // reading here and the trailers below are never observed.
            assert!(
                !http.is_body_done(),
                "a satisfied content-length must not end the read before the trailers"
            );
            assert!(http.read_body_bytes().await.unwrap().is_none());
            assert!(http.is_body_done());
            assert!(http.request_trailers_present());

            let response = Box::new(ResponseHeader::build(StatusCode::NO_CONTENT, None).unwrap());
            http.write_response_header(response, true).unwrap();
        }));
    }
    client.await.unwrap();
    for handler in handlers {
        handler.await.unwrap();
    }
}

/// A client that finishes its request body and THEN cancels the stream
/// (dropping the response is what a browser navigating away does, and it
/// puts RST_STREAM CANCEL on the wire) must not turn the already-complete
/// request into a read error. The unclassified trailer poll used to make
/// it one: both proxy pumps convert it with `into_down()`, which fails the
/// request with a spurious 400 AND closes and re-dials an otherwise
/// healthy pooled upstream connection, once per client cancel.
#[tokio::test]
async fn test_client_cancel_after_body_eof_is_not_a_read_error() {
    let (client, server) = duplex(65536);
    let (body_read_tx, body_read_rx) = tokio::sync::oneshot::channel::<()>();
    let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel::<()>();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut body) = h2
            .ready()
            .await
            .unwrap()
            .send_request(request, false)
            .unwrap();
        // A complete request body, END_STREAM included.
        body.send_data("payload".into(), true).unwrap();

        // Only cancel once the server has the body, so the cancel is
        // unambiguously post-EOF rather than a torn upload.
        body_read_rx.await.unwrap();
        drop(response);
        drop(body);
        // Let the connection task put the RST_STREAM on the wire.
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancelled_tx.send(()).unwrap();
        // Keep the connection open until the server side has made its
        // observation, so the test never races the connection teardown.
        let _ = done_rx.await;
    });

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let mut handlers = Vec::new();
    let mut signals = Some((body_read_tx, cancelled_rx, done_tx));
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        let (body_read_tx, cancelled_rx, done_tx) = signals.take().expect("exactly one stream");
        handlers.push(tokio::spawn(async move {
            let data = http.read_body_bytes().await.unwrap().unwrap();
            assert_eq!(data, "payload");
            assert!(http.is_body_done());

            body_read_tx.send(()).unwrap();
            cancelled_rx.await.unwrap();

            // The EOF read (which polls trailers) must stay Ok: the
            // request was received in full and the client's cancel says
            // nothing about the bytes we already have.
            assert!(
                http.read_body_bytes().await.unwrap().is_none(),
                "a post-EOF client cancel must not surface as a read error"
            );
            assert!(!http.request_trailers_present());
            let _ = done_tx.send(());
        }));
    }
    client.await.unwrap();
    for handler in handlers {
        handler.await.unwrap();
    }
}

/// Drive a client that sends `declared` as its `content-length`, puts
/// `payload` on the wire (with END_STREAM iff `end_stream`) and then cancels
/// the stream -- all BEFORE the server reads anything.
///
/// This is the natural wire ordering, and it is the one the latch exists
/// for: no oneshot forces the reset to happen after the reader observed EOF,
/// so by the time the server polls, both the END_STREAM-bearing DATA and reset
/// are already queued. Supported h2 0.4.19 preserves received END_STREAM; the
/// test deliberately asserts Pingora's body result rather than that private
/// state representation.
///
/// The sleep between the write and the cancel is required, not cosmetic:
/// `h2`'s `send_reset` CLEARS the stream's pending send queue, so cancelling
/// immediately would drop the DATA frame the test is about.
async fn cancel_after_write_client(
    client: DuplexStream,
    declared: &'static str,
    payload: &'static str,
    end_stream: bool,
) {
    let (h2, connection) = h2::client::handshake(client).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = Request::builder()
        .method(Method::POST)
        .uri("https://www.example.com/")
        .header("content-length", declared)
        .body(())
        .unwrap();
    let (response, mut body) = h2
        .ready()
        .await
        .unwrap()
        .send_request(request, false)
        .unwrap();
    body.send_data(payload.into(), end_stream).unwrap();
    // Let the connection task flush the DATA frame before the cancel
    // discards whatever is still queued.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(response);
    drop(body);
    // Keep the connection alive long enough for the reset to be written and
    // for the server to make its observation.
    tokio::time::sleep(Duration::from_secs(3)).await;
}

/// A complete request body followed by a client cancel, in the NATURAL wire
/// ordering: both frames land before the server polls, so the END_STREAM
/// evidence is destroyed before it can be latched and only the declared
/// `content-length` can still prove the body whole.
///
/// Without that source the read fails, and both proxy pumps convert it with
/// `into_down()`: a spurious 400 for a request the proxy received in full,
/// plus a closed and re-dialled upstream connection, once per client cancel.
#[tokio::test]
async fn test_complete_body_then_reset_before_any_read_is_a_clean_eof() {
    let (client, server) = duplex(65536);
    let client = tokio::spawn(cancel_after_write_client(client, "7", "payload", true));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let mut handlers = Vec::new();
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handlers.push(tokio::spawn(async move {
            // Nothing is read until both the DATA and the RST_STREAM have
            // been processed by the connection above.
            tokio::time::sleep(Duration::from_millis(600)).await;

            let data = http.read_body_bytes().await.unwrap().unwrap();
            assert_eq!(data, "payload");
            assert!(
                http.read_body_bytes().await.unwrap().is_none(),
                "a fully received content-length body must read as a clean EOF \
                 even when the client cancelled before we polled"
            );
            assert!(http.is_body_done());
            assert_eq!(http.body_bytes_read(), 7);
        }));
    }
    client.await.unwrap();
    for handler in handlers {
        handler.await.unwrap();
    }
}

/// The mirror image, and the security-relevant half: the same cancel after a
/// PARTIAL body must stay a read error. Classifying it as a clean EOF would
/// let the pumps forward a truncated request body upstream as if the client
/// had sent it in full.
#[tokio::test]
async fn test_mid_body_reset_is_still_a_read_error() {
    let (client, server) = duplex(65536);
    // 20 declared, 4 delivered: the body is provably incomplete.
    let client = tokio::spawn(cancel_after_write_client(client, "20", "payl", false));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let mut handlers = Vec::new();
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handlers.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;

            let data = http.read_body_bytes().await.unwrap().unwrap();
            assert_eq!(data, "payl");
            assert!(!http.is_body_done());

            let err = http
                .read_body_bytes()
                .await
                .expect_err("a truncated request body must not read as a clean EOF");
            assert_eq!(err.etype(), &ErrorType::ReadError);
            assert!(!http.is_body_done());
        }));
    }
    client.await.unwrap();
    for handler in handlers {
        handler.await.unwrap();
    }
}

#[tokio::test]
async fn test_early_body_buffer_rejected_for_connect_h2() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        // Plain CONNECT: authority-form URI, no END_STREAM (the stream is a tunnel)
        let request = Request::builder()
            .method(Method::CONNECT)
            .uri("www.example.com:443")
            .body(())
            .unwrap();
        let (response, _req_body) = h2.send_request(request, false).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
            // The tunnel stream must never be captured: registration fails closed.
            assert!(http
                .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                .is_err());
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_early_body_buffer_rejected_when_retry_buffering_enabled_h2() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abc".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
            // Double-send defense: with the native retry buffer enabled the
            // drained body would be teed into it AND replayed by the app
            // buffer, so registration must fail closed.
            http.enable_retry_buffering();
            assert!(http
                .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                .is_err());
            while http.read_body_bytes().await.unwrap().is_some() {}
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_early_body_buffer_not_registered_h2() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abc".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            while http.read_body_bytes().await.unwrap().is_some() {}
            assert!(!http.begin_request_body_replay().await.unwrap());
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_early_body_buffer_rewind_from_mid_replay_h2() {
    use crate::protocols::http::body_buffer::RequestBodyBuffer;
    use async_trait::async_trait;
    use bytes::BytesMut;

    /// Like a disk-spill impl: awaits (yields) inside next_chunk, opening
    /// a cancellation window mid-replay. 2-byte replay chunks so a small
    /// test body still replays in multiple chunks, like a large body
    /// against the real 64 KiB replay chunk size.
    struct SmallChunkYieldingBuffer {
        buf: BytesMut,
        body: Option<Bytes>,
        offset: usize,
    }

    #[async_trait]
    impl RequestBodyBuffer for SmallChunkYieldingBuffer {
        async fn write(&mut self, data: &Bytes) -> Result<()> {
            self.buf.extend_from_slice(data);
            Ok(())
        }

        async fn finish(&mut self) -> Result<()> {
            if self.body.is_none() {
                self.body = Some(self.buf.split().freeze());
            }
            Ok(())
        }

        async fn rewind(&mut self) -> Result<()> {
            self.offset = 0;
            Ok(())
        }

        async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
            tokio::task::yield_now().await;
            let Some(body) = self.body.as_ref() else {
                return Ok(None);
            };
            if self.offset >= body.len() {
                return Ok(None);
            }
            let end = self.offset.saturating_add(max_bytes.min(2)).min(body.len());
            Ok(Some(body.slice(self.offset..end)))
        }

        fn consume(&mut self, bytes: usize) {
            self.offset = self.offset.saturating_add(bytes);
        }
    }

    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abcdef".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            http.set_request_body_buffer(Box::new(SmallChunkYieldingBuffer {
                buf: BytesMut::new(),
                body: None,
                offset: 0,
            }))
            .unwrap();
            let mut total = Vec::new();
            while let Some(chunk) = http.read_body_bytes().await.unwrap() {
                total.extend_from_slice(&chunk);
            }
            assert_eq!(total, b"abcdef");

            // Attempt 1 fails mid-replay: one chunk was delivered (its
            // commit still pending) when the retry rewinds from the
            // Replaying state.
            assert!(http.begin_request_body_replay().await.unwrap());
            assert_eq!(
                http.read_body_or_idle(false).await.unwrap().unwrap(),
                b"ab".as_slice()
            );
            assert!(http.begin_request_body_replay().await.unwrap());
            let mut replayed = Vec::new();
            while let Some(chunk) = http.read_body_or_idle(false).await.unwrap() {
                replayed.extend_from_slice(&chunk);
            }
            assert_eq!(replayed, b"abcdef");

            // Attempt 2 fails with a read in flight: the read is
            // cancelled mid-poll (losing select! branch) before the retry
            // rewinds from the Replaying state.
            assert!(http.begin_request_body_replay().await.unwrap());
            assert_eq!(
                http.read_body_or_idle(false).await.unwrap().unwrap(),
                b"ab".as_slice()
            );
            {
                let mut fut = tokio_test::task::spawn(http.read_body_or_idle(false));
                assert!(fut.poll().is_pending());
            }
            assert!(http.begin_request_body_replay().await.unwrap());
            let mut replayed = Vec::new();
            while let Some(chunk) = http.read_body_or_idle(false).await.unwrap() {
                replayed.extend_from_slice(&chunk);
            }
            assert_eq!(replayed, b"abcdef");

            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

/// `Content-Length: 0` without END_STREAM on HEADERS: `is_body_done()` is
/// false (the transport fact), but the request promises zero DATA bytes,
/// so draining must return immediately instead of awaiting an EOS that
/// the client only sends later (or never).
#[tokio::test]
async fn test_drain_returns_immediately_for_cl0_without_end_stream() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .header("content-length", "0")
            .body(())
            .unwrap();
        // No END_STREAM on HEADERS, and this client never sends one.
        let (response, _req_body) = h2.send_request(request, false).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            assert!(!http.is_body_done());
            assert!(http.is_body_empty());
            // No total_drain_timeout is set: an unbounded drain would hang here.
            timeout(Duration::from_secs(5), http.drain_request_body())
                .await
                .expect("drain of a CL:0 request must not await the transport EOS")
                .unwrap();
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_drain_discards_buffer_and_poisons_replay_h2() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abc".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
            http.set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                .unwrap();
            http.drain_request_body().await.unwrap();
            // The registered buffer was discarded with the body: replay must
            // fail closed rather than report "no buffer registered" and let
            // the proxy forward a request whose body is gone.
            assert!(http.begin_request_body_replay().await.is_err());
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

/// Delegates to `InMemoryRequestBodyBuffer` and reports its own drop, so
/// tests can pin down exactly when the session releases the buffer.
struct DropProbeBuffer {
    inner: crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}

impl DropProbeBuffer {
    fn new() -> (Self, Arc<std::sync::atomic::AtomicBool>) {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        (
            DropProbeBuffer {
                inner: crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer::new(),
                dropped: dropped.clone(),
            },
            dropped,
        )
    }
}

impl Drop for DropProbeBuffer {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl crate::protocols::http::body_buffer::RequestBodyBuffer for DropProbeBuffer {
    async fn write(&mut self, data: &Bytes) -> Result<()> {
        self.inner.write(data).await
    }

    async fn finish(&mut self) -> Result<()> {
        self.inner.finish().await
    }

    async fn rewind(&mut self) -> Result<()> {
        self.inner.rewind().await
    }

    async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
        self.inner.next_chunk(max_bytes).await
    }

    fn consume(&mut self, bytes: usize) {
        self.inner.consume(bytes)
    }
}

fn probe_dropped(flag: &Arc<std::sync::atomic::AtomicBool>) -> bool {
    flag.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test]
async fn test_ready_buffer_released_when_response_commits_h2() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abc".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            let (probe, dropped) = DropProbeBuffer::new();
            http.set_request_body_buffer(Box::new(probe)).unwrap();
            while http.read_body_bytes().await.unwrap().is_some() {}
            assert!(!probe_dropped(&dropped));

            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
            assert!(probe_dropped(&dropped));
            assert!(!http.request_body_buffer_registered());
            assert!(http.begin_request_body_replay().await.is_err());
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_buffer_released_when_response_commits_after_replay_h2() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abc".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            let (probe, dropped) = DropProbeBuffer::new();
            http.set_request_body_buffer(Box::new(probe)).unwrap();
            while http.read_body_bytes().await.unwrap().is_some() {}
            assert!(http.begin_request_body_replay().await.unwrap());
            assert_eq!(
                http.read_body_or_idle(false).await.unwrap().unwrap(),
                b"abc".as_slice()
            );
            assert!(http.read_body_or_idle(false).await.unwrap().is_none());
            // Replay is done but no response committed yet: a retry could
            // still rewind and replay, so the buffer must survive.
            assert!(!probe_dropped(&dropped));
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
            // Committing the response releases the buffer immediately.
            assert!(probe_dropped(&dropped));
            assert!(!http.request_body_buffer_registered());
            // A replay attempt after release must fail closed, not silently
            // proxy a bodyless request.
            assert!(http.begin_request_body_replay().await.is_err());
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_buffer_released_at_replay_eof_when_response_committed_first_h2() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abc".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            let (probe, dropped) = DropProbeBuffer::new();
            http.set_request_body_buffer(Box::new(probe)).unwrap();
            while http.read_body_bytes().await.unwrap().is_some() {}
            assert!(http.begin_request_body_replay().await.unwrap());
            assert_eq!(
                http.read_body_or_idle(false).await.unwrap().unwrap(),
                b"abc".as_slice()
            );
            // Early upstream response: the header commits downstream while
            // replay is still in flight — the buffer must survive until
            // replay EOF.
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
            assert!(!probe_dropped(&dropped));
            assert!(http.read_body_or_idle(false).await.unwrap().is_none());
            assert!(probe_dropped(&dropped));
            assert!(http.begin_request_body_replay().await.is_err());
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_zero_payload_body_stays_non_empty_while_registered_h2() {
    use crate::protocols::http::body_buffer::RequestBodyBuffer;

    /// Ignores captured data and replays a fixed non-empty body, like an
    /// app rewriting a zero-byte payload.
    #[derive(Default)]
    struct RewriteToNonEmptyBuffer {
        offset: usize,
    }

    const REWRITTEN_BODY: &[u8] = b"rewritten";

    #[async_trait::async_trait]
    impl RequestBodyBuffer for RewriteToNonEmptyBuffer {
        async fn write(&mut self, _data: &Bytes) -> Result<()> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        async fn rewind(&mut self) -> Result<()> {
            self.offset = 0;
            Ok(())
        }

        async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
            if self.offset >= REWRITTEN_BODY.len() {
                return Ok(None);
            }
            let end = self
                .offset
                .saturating_add(max_bytes)
                .min(REWRITTEN_BODY.len());
            Ok(Some(Bytes::from_static(&REWRITTEN_BODY[self.offset..end])))
        }

        fn consume(&mut self, bytes: usize) {
            self.offset = self.offset.saturating_add(bytes);
        }
    }

    let (client, server) = duplex(65536);
    let mut handles = vec![];
    // Sequencing: the client sends its empty END_STREAM DATA frame only
    // after the server registered the buffer, so registration always sees
    // a stream whose emptiness is still unknown.
    let (registered_tx, registered_rx) = tokio::sync::oneshot::channel::<()>();

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap(); // no EOS
        registered_rx.await.unwrap();
        // A zero-byte payload whose framing permitted a body.
        req_body.send_data("".into(), true).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    let mut registered_tx = Some(registered_tx);
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        let registered_tx = registered_tx.take().unwrap();
        handles.push(tokio::spawn(async move {
            http.set_request_body_buffer(Box::new(RewriteToNonEmptyBuffer::default()))
                .unwrap();
            assert!(!http.is_body_empty());
            registered_tx.send(()).unwrap();
            // Drain: the client now ends the stream with an empty DATA
            // frame, so zero bytes are captured.
            while let Some(chunk) = http.read_body_bytes().await.unwrap() {
                assert!(chunk.is_empty());
            }
            // Zero bytes were captured and END_STREAM was received, but the
            // registered buffer may rewrite the body: the emptiness
            // decision proxy_h2 derives END_STREAM-on-HEADERS from must
            // keep tracking the replay source instead of the original
            // payload.
            assert!(!http.is_body_empty());
            assert!(http.begin_request_body_replay().await.unwrap());
            assert!(!http.is_body_empty());
            assert_eq!(
                http.read_body_or_idle(false).await.unwrap().unwrap(),
                REWRITTEN_BODY
            );
            assert!(http.read_body_or_idle(false).await.unwrap().is_none());
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn finalized_replay_buffer_injects_body_into_empty_h2_request() {
    use crate::protocols::http::body_buffer::RequestBodyBuffer;

    #[derive(Default)]
    struct InjectedBody {
        offset: usize,
    }

    const BODY: &[u8] = b"injected";

    #[async_trait::async_trait]
    impl RequestBodyBuffer for InjectedBody {
        async fn write(&mut self, _data: &Bytes) -> Result<()> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        async fn rewind(&mut self) -> Result<()> {
            self.offset = 0;
            Ok(())
        }

        async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
            if self.offset >= BODY.len() {
                return Ok(None);
            }
            let end = self.offset.saturating_add(max_bytes).min(BODY.len());
            Ok(Some(Bytes::from_static(&BODY[self.offset..end])))
        }

        fn consume(&mut self, bytes: usize) {
            self.offset = self.offset.saturating_add(bytes);
        }
    }

    let (client, server) = duplex(65536);
    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, _) = h2.send_request(request, true).unwrap();
        assert_eq!(response.await.unwrap().status(), http::StatusCode::OK);
    });

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let mut server_tasks = Vec::new();
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        server_tasks.push(tokio::spawn(async move {
            assert!(http.is_body_empty());
            http.set_bodyless_request_replay_buffer(Box::new(InjectedBody::default()))
                .unwrap();
            assert!(!http.is_body_empty());
            assert!(http.begin_request_body_replay().await.unwrap());
            assert_eq!(http.read_body_or_idle(false).await.unwrap().unwrap(), BODY);
            assert!(http.read_body_or_idle(false).await.unwrap().is_none());
            let response = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response, true).unwrap();
        }));
    }

    client.await.unwrap();
    for task in server_tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn bodyless_replay_buffer_rejected_while_downstream_stream_open_h2() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();

        // Stream 1: `Content-Length: 0` but no END_STREAM on HEADERS — the
        // downstream stream is still open even though the body is declared
        // empty.
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .header("content-length", "0")
            .body(())
            .unwrap();
        let (response, _req_body) = h2.send_request(request, false).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);

        // Stream 2: no Content-Length and no END_STREAM — the body may be
        // non-empty.
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, _req_body2) = h2.send_request(request, false).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
            // Replay would permanently shadow the still-open downstream
            // stream (empty DATA + END_STREAM, trailers, or violating DATA
            // would never be observed), so registration fails closed.
            assert!(http
                .set_bodyless_request_replay_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
                .is_err());
            assert!(!http.begin_request_body_replay().await.unwrap());
            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_cancelled_capture_poisons_h2_session() {
    use crate::protocols::http::body_buffer::RequestBodyBuffer;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A capture impl that flags entry into `write()` and then never
    /// completes, opening a deterministic cancellation window after the
    /// chunk has been consumed from the stream.
    struct PendingCaptureBuffer {
        entered: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl RequestBodyBuffer for PendingCaptureBuffer {
        async fn write(&mut self, _data: &Bytes) -> Result<()> {
            self.entered.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
            Ok(())
        }

        async fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        async fn rewind(&mut self) -> Result<()> {
            Ok(())
        }

        async fn next_chunk(&mut self, _max_bytes: usize) -> Result<Option<Bytes>> {
            Ok(None)
        }

        fn consume(&mut self, _bytes: usize) {}
    }

    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("abc".into(), true).unwrap();
        // The poisoned server session errors out instead of responding.
        assert!(response.await.is_err());
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            let entered = Arc::new(AtomicBool::new(false));
            http.set_request_body_buffer(Box::new(PendingCaptureBuffer {
                entered: entered.clone(),
            }))
            .unwrap();
            // Manually poll read_body_bytes until it has consumed the chunk
            // and suspended inside the buffer's write(), then drop the
            // future — modeling a losing select!/timeout branch cancelling
            // the read mid-capture.
            {
                let mut fut = Box::pin(http.read_body_bytes());
                let waker = futures::task::noop_waker();
                loop {
                    // Scope the Context to a single poll: it is not Send and
                    // must not live across the yield await below.
                    let pending = {
                        let mut poll_ctx = std::task::Context::from_waker(&waker);
                        fut.as_mut().poll(&mut poll_ctx).is_pending()
                    };
                    assert!(pending);
                    if entered.load(Ordering::SeqCst) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
            // The chunk is gone from both the app's view and the buffer:
            // the session must fail closed on any further read or replay.
            assert!(http.read_body_bytes().await.is_err());
            assert!(http.begin_request_body_replay().await.is_err());
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

// NOTE on wall-clock in the read-timeout tests below: `tokio::time::pause()`
// cannot drive them. The bound is armed with `pingora_timeout::timeout`,
// whose fast timer runs on a dedicated OS thread against the real clock
// (`pingora_timeout::fast_timeout::TIMER_MANAGER`), so paused/auto-advanced
// tokio time never fires it and every one of these tests would hang. The
// durations are therefore kept small, and every "must fire" assertion is
// wrapped in an outer bound that is orders of magnitude larger than the
// timeout under test, so load can slow these tests down without failing
// them.
const TEST_STALL_GRACE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn test_read_body_timeout_releases_stalled_upload() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];
    // The client stalls until the server has observed the timeout, so the
    // release is provably the read timeout firing and not this task
    // dropping the stream -- without paying a fixed wall-clock sleep.
    let (released_tx, released_rx) = tokio::sync::oneshot::channel::<()>();

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            // The server errors the session on timeout, so the connection
            // future may resolve with an error; either way is fine here.
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (_response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("first chunk".into(), false).unwrap();
        // Stall: keep the request-body half open, never send another byte.
        let _ = released_rx.await;
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    let mut released_tx = Some(released_tx);
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        let released_tx = released_tx.take().unwrap();
        handles.push(tokio::spawn(async move {
            http.set_read_timeout(Some(Duration::from_millis(300)));

            let body = http.read_body_or_idle(false).await.unwrap().unwrap();
            assert_eq!(body, "first chunk");

            // The stalled second read must be released by the read timeout
            // instead of pinning this task forever. The outer bound only
            // distinguishes "timeout fired" from "test hung": it is far
            // above the read timeout and never expires on a healthy run.
            let err = tokio::time::timeout(TEST_STALL_GRACE, http.read_body_or_idle(false))
                .await
                .expect("read_timeout should have released the stalled read")
                .unwrap_err();
            assert_eq!(err.etype(), &ErrorType::ReadTimedout);
            let _ = released_tx.send(());
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

/// The idle bound must be a DEADLINE carried by the session, not a duration
/// rebuilt per call.
///
/// Every caller polls the body read from a `tokio::select!` branch, and any
/// other ready branch -- an upstream response chunk, a cache task, a custom
/// message -- cancels it. Here a ticker fires several times per read
/// timeout, standing in for an upstream that emits at least one event per
/// period (SSE, gRPC server-streaming, a slow multi-chunk 200). With a
/// per-call duration the bound is rearmed by every cancellation and the
/// stalled client pins this pump, its downstream stream and its upstream
/// connection forever: the DoS the bound exists to close.
#[tokio::test]
async fn test_read_body_timeout_survives_select_cancellation() {
    const READ_TIMEOUT: Duration = Duration::from_millis(300);
    // Well under the read timeout: the read is cancelled and rebuilt
    // several times before the deadline is reached.
    const CHATTY_UPSTREAM_TICK: Duration = Duration::from_millis(60);

    let (client, server) = duplex(65536);
    let mut handles = vec![];
    let (released_tx, released_rx) = tokio::sync::oneshot::channel::<()>();

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (_response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("first chunk".into(), false).unwrap();
        // Stall forever; only the server's deadline can end this.
        let _ = released_rx.await;
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    let mut released_tx = Some(released_tx);
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        let released_tx = released_tx.take().unwrap();
        handles.push(tokio::spawn(async move {
            http.set_read_timeout(Some(READ_TIMEOUT));

            let body = http.read_body_or_idle(false).await.unwrap().unwrap();
            assert_eq!(body, "first chunk");

            let mut cancellations = 0u32;
            let result = tokio::time::timeout(TEST_STALL_GRACE, async {
                loop {
                    tokio::select! {
                        body = http.read_body_or_idle(false) => break body,
                        _ = tokio::time::sleep(CHATTY_UPSTREAM_TICK) => {
                            cancellations += 1;
                        }
                    }
                }
            })
            .await
            .expect(
                "an unrelated ready select! branch must not rearm the downstream \
                 request-body idle bound",
            );

            assert_eq!(result.unwrap_err().etype(), &ErrorType::ReadTimedout);
            assert!(
                cancellations >= 2,
                "the competing branch must have cancelled the read at least twice \
                 for this to test anything; saw {cancellations}"
            );
            let _ = released_tx.send(());
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

/// An empty DATA frame is not progress and must not rearm the bound.
///
/// A zero-length DATA frame without END_STREAM is legal, costs the peer 9
/// bytes, consumes no flow-control window and trips no h2 flood counter, so
/// treating it as a received chunk would hand an attacker an unlimited
/// rearm at ~zero cost -- and because `body_read` never advances, no
/// byte-count body-size limit would catch it either.
#[tokio::test]
async fn test_empty_data_frames_do_not_rearm_read_timeout() {
    const READ_TIMEOUT: Duration = Duration::from_millis(300);
    const TRICKLE_GAP: Duration = Duration::from_millis(80);

    let (client, server) = duplex(65536);
    let mut handles = vec![];
    let (released_tx, mut released_rx) = tokio::sync::oneshot::channel::<()>();
    let empty_frames_sent = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client_empty_frames_sent = empty_frames_sent.clone();

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (_response, mut req_body) = h2.send_request(request, false).unwrap();
        req_body.send_data("first chunk".into(), false).unwrap();
        // Then nothing but empty DATA frames, forever.
        loop {
            tokio::select! {
                _ = &mut released_rx => break,
                _ = tokio::time::sleep(TRICKLE_GAP) => {
                    if req_body.send_data(Bytes::new(), false).is_err() {
                        break;
                    }
                    client_empty_frames_sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    let mut released_tx = Some(released_tx);
    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        let released_tx = released_tx.take().unwrap();
        let empty_frames_sent = empty_frames_sent.clone();
        handles.push(tokio::spawn(async move {
            http.set_read_timeout(Some(READ_TIMEOUT));

            let body = http.read_body_or_idle(false).await.unwrap().unwrap();
            assert_eq!(body, "first chunk");

            let err = tokio::time::timeout(TEST_STALL_GRACE, async {
                loop {
                    match http.read_body_or_idle(false).await {
                        Ok(Some(chunk)) => {
                            assert!(chunk.is_empty(), "the client only sends empty frames");
                        }
                        Ok(None) => panic!("the client never ends the stream"),
                        Err(e) => break e,
                    }
                }
            })
            .await
            .expect("empty DATA frames must not rearm the request-body idle bound");

            assert_eq!(err.etype(), &ErrorType::ReadTimedout);
            assert!(
                empty_frames_sent.load(std::sync::atomic::Ordering::SeqCst) >= 2,
                "the client must have sent at least two empty frames for this to test anything"
            );
            // The bound fired without a single body byte having been added.
            assert_eq!(http.body_bytes_read(), "first chunk".len());
            let _ = released_tx.send(());
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

/// A CONNECT tunnel's "request body" is its uplink, on which long idle
/// periods are ordinary (an idle SSH session; a WebSocket over extended
/// CONNECT whose traffic runs server-to-client only). The bound must not
/// apply to it.
#[tokio::test]
async fn test_read_body_timeout_spares_connect_tunnel() {
    const READ_TIMEOUT: Duration = Duration::from_millis(100);
    // Several times the read timeout: enough for the bound to have fired.
    const IDLE_UPLINK: Duration = Duration::from_millis(500);

    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();
        // Plain CONNECT: authority-form URI, no END_STREAM (it is a tunnel).
        let request = Request::builder()
            .method(Method::CONNECT)
            .uri("www.example.com:443")
            .body(())
            .unwrap();
        let (response, _req_body) = h2.send_request(request, false).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            http.set_read_timeout(Some(READ_TIMEOUT));

            tokio::select! {
                read = http.read_body_or_idle(false) => {
                    panic!(
                        "an idle CONNECT uplink must not be cut by the request-body \
                         read timeout, got {read:?}"
                    );
                }
                _ = tokio::time::sleep(IDLE_UPLINK) => {}
            }

            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

/// `Content-Length: 0` without END_STREAM is legal on H2 (design 4.3): the
/// transport has promised there are no body bytes, but the client owes an
/// END_STREAM it may take arbitrarily long to send. Failing such a request
/// with a read timeout would discard an exchange that has nothing left to
/// read, so the bound finishes the read side instead of erroring.
#[tokio::test]
async fn test_read_body_timeout_finishes_declared_empty_body() {
    const READ_TIMEOUT: Duration = Duration::from_millis(200);

    let (client, server) = duplex(65536);
    let mut handles = vec![];

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .header("content-length", "0")
            .body(())
            .unwrap();
        // No END_STREAM, and the client never sends one.
        let (response, _req_body) = h2.send_request(request, false).unwrap();
        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            // Parity with the HTTP/1 session: the bound is on by default,
            // because nothing in pingora-core or pingora-proxy sets one.
            assert_eq!(http.get_read_timeout(), Some(Duration::from_secs(60)));
            http.set_read_timeout(Some(READ_TIMEOUT));
            assert!(http.is_body_empty());
            assert!(!http.is_body_done());

            let read = tokio::time::timeout(TEST_STALL_GRACE, http.read_body_or_idle(false))
                .await
                .expect("the read side must be finished by the bound, not left pending")
                .expect("a provably empty body must not be failed by the read timeout");
            assert!(read.is_none());
            // The read side is finished for good: no second timeout, and no
            // idle-path error on a later poll.
            assert!(http.is_body_done());

            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}

#[tokio::test]
async fn test_read_body_timeout_spares_progressing_upload() {
    let (client, server) = duplex(65536);
    let mut handles = vec![];

    // Inter-chunk gaps stay well under the read timeout while the total
    // upload duration exceeds it: the bound must be rearmed per chunk.
    const CHUNKS: usize = 8;
    const GAP: Duration = Duration::from_millis(100);
    const READ_TIMEOUT: Duration = Duration::from_millis(500);

    handles.push(tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            connection.await.unwrap();
        });
        let mut h2 = h2.ready().await.unwrap();

        let request = Request::builder()
            .method(Method::POST)
            .uri("https://www.example.com/")
            .body(())
            .unwrap();
        let (response, mut req_body) = h2.send_request(request, false).unwrap();
        for i in 0..CHUNKS {
            tokio::time::sleep(GAP).await;
            let eos = i == CHUNKS - 1;
            req_body.send_data("chunk".into(), eos).unwrap();
        }

        let (head, _body) = response.await.unwrap().into_parts();
        assert_eq!(head.status, 200);
    }));

    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());

    while let Some(mut http) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handles.push(tokio::spawn(async move {
            http.set_read_timeout(Some(READ_TIMEOUT));

            // Stop on `is_body_done()` rather than on a `None` read: the
            // final chunk carries END_STREAM, so one more read after it
            // would take the idle path and pend until the client closes --
            // and the client is waiting for our response. The outer
            // per-read bound only distinguishes "spared" from "test hung";
            // it never expires on a healthy run.
            let mut total = 0;
            while !http.is_body_done() {
                let chunk = tokio::time::timeout(TEST_STALL_GRACE, http.read_body_or_idle(false))
                    .await
                    .expect("a progressing upload must not be starved by the read timeout")
                    .unwrap()
                    .unwrap();
                total += chunk.len();
            }
            assert_eq!(total, CHUNKS * "chunk".len());

            let response_header = Box::new(ResponseHeader::build(200, None).unwrap());
            http.write_response_header(response_header, true).unwrap();
        }));
    }

    for handle in handles {
        assert!(handle.await.is_ok());
    }
}
