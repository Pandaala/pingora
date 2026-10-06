//! CONNECT request framing at the currently reachable H1 boundary. This does
//! not claim bidirectional H1 tunnel support after a successful response.

use super::harness::*;
use std::time::Duration;

const SHAPES: [(&str, &[u8], &[u8]); 3] = [
    ("", b"", b"\r\n\r\n"),
    ("Content-Length: 4\r\n", b"data", b"data"),
    (
        "Transfer-Encoding: chunked\r\n",
        b"4\r\ndata\r\n0\r\n\r\n",
        b"0\r\n\r\n",
    ),
];

fn decoded_chunked_body(mut wire: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    loop {
        let end = wire.windows(2).position(|w| w == b"\r\n").unwrap();
        let size = usize::from_str_radix(std::str::from_utf8(&wire[..end]).unwrap(), 16).unwrap();
        wire = &wire[end + 2..];
        if size == 0 {
            assert_eq!(wire, b"\r\n", "exactly one chunked EOF, no extra bytes");
            return body;
        }
        body.extend_from_slice(&wire[..size]);
        assert_eq!(&wire[size..size + 2], b"\r\n");
        wire = &wire[size + 2..];
    }
}

#[test]
fn bodyless_preserves_h1_connect_request_framing() {
    let ports = init();
    for (framing, body, marker) in SHAPES {
        let (port, captured) = spawn_recording_upstream(marker);
        RT.block_on(async {
            let mut request = format!(
                "CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
                 x-port: {port}\r\nx-disposition: bodyless\r\n{framing}\r\n"
            )
            .into_bytes();
            request.extend_from_slice(body);
            // The origin responds only after observing the selected shape and
            // its bounded extra-byte collection window. A local error cannot
            // satisfy the wire assertions below.
            let (_, response) = tokio::time::timeout(
                Duration::from_secs(2),
                raw_h1_roundtrip(&ports.h1_addr(), &request, b"\r\n\r\n"),
            )
            .await
            .expect("CONNECT framing exchange stalled");
            let wire = captured.lock().unwrap().clone();
            assert!(!wire.is_empty(), "upstream was never reached: {framing}");
            let header_end = wire.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            let headers = String::from_utf8_lossy(&wire[..header_end]);
            assert!(headers.starts_with(&format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\n")));
            if framing.starts_with("Transfer-Encoding") {
                assert_eq!(decoded_chunked_body(&wire[header_end..]), b"data");
            } else {
                assert_eq!(
                    &wire[header_end..],
                    body,
                    "forwarded body/framing: {headers}"
                );
            }
            let lower = headers.to_ascii_lowercase();
            assert_eq!(
                lower.contains("content-length: 4\r\n"),
                framing.starts_with("Content-Length")
            );
            assert_eq!(
                lower.contains("transfer-encoding: chunked\r\n"),
                framing.starts_with("Transfer-Encoding")
            );
            assert!(response.starts_with(b"HTTP/1.1 200"), "{response:?}");
        });
    }
}

#[test]
fn streamed_rejects_h1_connect_before_upstream_headers() {
    let ports = init();
    for (framing, body, _) in SHAPES {
        let upstream = spawn_scripted_upstream(vec![UpstreamStep::Respond(OK_KEEPALIVE)]);
        upstream.expect_unused();
        let port = upstream.port();
        let rec = upstream.rec();
        RT.block_on(async {
            let mut request = format!(
                "CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
                 x-port: {port}\r\nx-disposition: streamed\r\n{framing}\r\n"
            )
            .into_bytes();
            request.extend_from_slice(body);
            let (_, response) = tokio::time::timeout(
                Duration::from_secs(2),
                raw_h1_roundtrip(&ports.h1_addr(), &request, b"\r\n\r\n"),
            )
            .await
            .expect("CONNECT rejection stalled");
            assert!(response.starts_with(b"HTTP/1.1 500"), "{response:?}");
            expect_ok(
                rec.expect_none(
                    "headers for a rejected Streamed CONNECT",
                    Duration::from_millis(100),
                    |event| matches!(event, UpEvent::ReqHeaders { .. }),
                )
                .await,
            );
        });
        assert_eq!(
            rec.count(|event| matches!(event, UpEvent::ReqHeaders { .. })),
            0,
            "{}",
            rec.dump()
        );
    }
}
