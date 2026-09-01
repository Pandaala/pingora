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
use crate::pump_termination::{
    join_bidirectional_pumps, DownstreamRequestOutcome, DuplexPumpOutcome,
};
use crate::request_relay::{
    safe_disposition, safe_upstream_disposition, violates_bodyless_contract, DispositionFacts,
};
use crate::UpstreamRequestBodyDisposition;

fn request_with_headers(headers: &[(&str, &str)]) -> RequestHeader {
    let mut request = RequestHeader::build("GET", b"/", Some(headers.len())).unwrap();
    request.set_version(http::Version::HTTP_11);
    for (name, value) in headers {
        request
            .append_header(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                http::HeaderValue::from_str(value).unwrap(),
            )
            .unwrap();
    }
    request
}

#[test]
fn h1_transfer_encoding_forwardability_is_exactly_one_chunked_field() {
    for (headers, expected) in [
        (vec![], true),
        (vec![("Transfer-Encoding", "chunked")], true),
        (vec![("Transfer-Encoding", "CHUNKED")], true),
        (vec![("Transfer-Encoding", " \tchunked\t ")], true),
        (vec![("Transfer-Encoding", "gzip, chunked")], false),
        (vec![("Transfer-Encoding", "GZip , CHUNKED")], false),
        (vec![("Transfer-Encoding", "\tgzip,\tchunked ")], false),
        (vec![("Transfer-Encoding", "deflate, chunked")], false),
        (vec![("Transfer-Encoding", "unknown, chunked")], false),
        (vec![("Transfer-Encoding", "chunked, chunked")], false),
        (vec![("Transfer-Encoding", "chunked,")], false),
        (vec![("Transfer-Encoding", "")], false),
        (
            vec![
                ("Transfer-Encoding", "gzip"),
                ("Transfer-Encoding", "chunked"),
            ],
            false,
        ),
        (
            vec![
                ("Transfer-Encoding", "chunked"),
                ("Transfer-Encoding", "chunked"),
            ],
            false,
        ),
    ] {
        let request = request_with_headers(&headers);
        assert_eq!(
            h1_transfer_encoding_is_forwardable(&request),
            expected,
            "unexpected result for {headers:?}"
        );
    }
}

#[tokio::test]
async fn origin_abandonment_drops_a_pending_upstream_pump_immediately() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::Duration;

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let upstream_probe = DropProbe(dropped.clone());
    let upstream = async move {
        let _probe = upstream_probe;
        std::future::pending::<Result<()>>().await
    };

    let outcome = tokio::time::timeout(
        Duration::from_millis(100),
        join_bidirectional_pumps(
            async { Ok(DownstreamRequestOutcome::CompleteWithoutUpstreamReuse(true)) },
            upstream,
        ),
    )
    .await
    .expect("origin abandonment must not wait for upstream EOS");

    assert!(matches!(
        outcome,
        DuplexPumpOutcome::OriginAbandoned {
            downstream_can_reuse: true
        }
    ));
    assert!(
        dropped.load(Ordering::SeqCst),
        "the pending upstream sibling must be dropped before the join returns"
    );
}

#[tokio::test]
async fn normal_completion_still_awaits_the_upstream_pump() {
    use std::time::{Duration, Instant};

    let started = Instant::now();
    let outcome = join_bidirectional_pumps(
        async { Ok(DownstreamRequestOutcome::Complete(false)) },
        async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(7usize)
        },
    )
    .await;

    assert!(matches!(
        outcome,
        DuplexPumpOutcome::Complete {
            downstream_can_reuse: false,
            upstream: 7
        }
    ));
    assert!(
        started.elapsed() >= Duration::from_millis(15),
        "normal completion must preserve the sibling-settlement contract"
    );
}

#[test]
fn h2_upstream_removes_connection_nominated_fields_by_default() {
    let mut request = request_with_headers(&[
        ("Connection", "X-Private-Hop, HTTP2-Settings"),
        ("X-Private-Hop", "secret"),
        ("HTTP2-Settings", "settings"),
        ("Proxy-Authorization", "secret"),
        ("TE", "trailers"),
        ("Trailer", "X-Trailer"),
    ]);

    sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard()).unwrap();

    assert!(request.headers.get("x-private-hop").is_none());
    assert!(request.headers.get("http2-settings").is_none());
    assert!(request.headers.get("proxy-authorization").is_none());
    assert!(request.headers.get("te").is_none());
    assert!(request.headers.get("trailer").is_none());
}

#[test]
fn h2_upstream_can_retain_connection_nominated_fields() {
    let mut request =
        request_with_headers(&[("Connection", "X-Private-Hop"), ("X-Private-Hop", "secret")]);
    let mut policy = HttpUpstreamRequestPolicy::standard();
    policy.strip_connection_nominated = false;

    sanitize_h2_upstream_request(&mut request, policy).unwrap();

    assert_eq!(request.headers["x-private-hop"], "secret");
}

#[test]
fn h2_upstream_removes_nominations_after_connection_self_nomination() {
    let mut request = request_with_headers(&[
        ("Connection", "Connection, X-Private-Hop"),
        ("X-Private-Hop", "secret"),
    ]);

    sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard()).unwrap();

    assert!(request.headers.get("connection").is_none());
    assert!(request.headers.get("x-private-hop").is_none());
}

#[test]
fn h2_upstream_rejects_excessive_unparseable_connection_nominations() {
    let mut request = request_with_headers(&[("Connection", "@, @, @, @, @, @, @, @, @, @")]);

    assert!(
        sanitize_h2_upstream_request(&mut request, HttpUpstreamRequestPolicy::standard()).is_err()
    );
}

fn facts(
    is_upgrade_req: bool,
    is_connect: bool,
    body_empty: bool,
    below_11: bool,
) -> DispositionFacts {
    DispositionFacts {
        is_upgrade_req,
        is_connect,
        body_empty,
        upstream_below_http11: below_11,
    }
}

/// Full truth table of the disposition coercion: every combination of the
/// four facts, for every disposition.
#[test]
fn safe_disposition_truth_table() {
    use UpstreamRequestBodyDisposition::*;

    for upgrade in [false, true] {
        for connect in [false, true] {
            for empty in [false, true] {
                for below_11 in [false, true] {
                    let f = facts(upgrade, connect, empty, below_11);
                    // Ordinary is never touched.
                    assert_eq!(safe_disposition(Ordinary, f), Ordinary, "{f:?}");

                    let must_coerce = upgrade || connect || empty || below_11;
                    for selected in [Bodyless, Streamed] {
                        let expected = if must_coerce { Ordinary } else { selected };
                        assert_eq!(
                            safe_disposition(selected, f),
                            expected,
                            "{selected:?} with {f:?}"
                        );
                    }
                }
            }
        }
    }
}

/// The individually load-bearing rows, spelled out so a regression names
/// the reason it broke.
#[test]
fn safe_disposition_named_rows() {
    use UpstreamRequestBodyDisposition::*;
    // Nothing special about the request: the application's choice stands.
    assert_eq!(
        safe_disposition(Streamed, facts(false, false, false, false)),
        Streamed
    );
    assert_eq!(
        safe_disposition(Bodyless, facts(false, false, false, false)),
        Bodyless
    );
    // Tunnels keep their protocol-fixed framing.
    assert_eq!(
        safe_disposition(Streamed, facts(true, false, false, false)),
        Ordinary
    );
    assert_eq!(
        safe_disposition(Streamed, facts(false, true, false, false)),
        Ordinary
    );
    // A request with no body must not be re-framed as chunked: the
    // `0\r\n\r\n` terminator on a pooled upstream connection is a
    // smuggling primitive.
    assert_eq!(
        safe_disposition(Streamed, facts(false, false, true, false)),
        Ordinary
    );
    // HTTP/1.0 peers must never be sent `Transfer-Encoding: chunked`.
    assert_eq!(
        safe_disposition(Streamed, facts(false, false, false, true)),
        Ordinary
    );
}

/// `DispositionFacts::collect` against a LIVE downstream session that a
/// client has poisoned.
///
/// The pure truth table above cannot see this failure at all: it takes
/// `body_empty` as an input, and the bug was in producing that input. Before
/// h2 0.4.16, a plain bodyless `GET` (END_STREAM on HEADERS) whose
/// client then reset the stream could lose that decoded END_STREAM state.
/// `is_body_empty()` and `is_body_done()` could then flip back to `false`, so
/// `safe_disposition` stopped recognising a request with no body. `Streamed`
/// survived the coercion, and the pump could proxy a bodyless `GET` upstream
/// with `Transfer-Encoding: chunked` framing it can never terminate.
///
/// Supported h2 0.4.19 preserves received END_STREAM. The fork still snapshots
/// the accepted request-header fact explicitly, and this test keeps that public
/// disposition stable across a later reset without depending on h2's private
/// state representation.
///
/// The reset is delivered AFTER the stream was accepted, which is the window
/// that matters: `proxy_down_to_up` collects these facts only after
/// `upstream_peer`, the cache lookup and `upstream_request_filter` have all
/// had their turn, so a reset has plenty of await points to land in. The reset
/// is deliberately sent after acceptance to test stability of Pingora's
/// snapshot; supported h2 also preserves a same-batch received END_STREAM.
#[tokio::test]
async fn collect_survives_a_client_reset_after_a_bodyless_request() {
    use pingora_core::modules::http::HttpModules;
    use pingora_core::protocols::http::v2::server::{handshake, HttpSession as SessionV2};
    use pingora_core::protocols::http::ServerSession;
    use pingora_core::protocols::Digest;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    let (client_io, server_io) = tokio::io::duplex(65536);
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel::<()>();

    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();
        let req = http::Request::builder()
            .method("GET")
            .uri("https://example.com/")
            .body(())
            .unwrap();
        // END_STREAM on HEADERS: a request with no body at all.
        let (response, body) = h2.send_request(req, true).unwrap();

        // Only reset once the server has the stream, so the END_STREAM fact
        // is established first and this test is about RETRACTING it.
        accepted_rx.await.unwrap();
        drop(response);
        drop(body);

        // A SECOND stream. Frames are processed in order on the connection,
        // so the server accepting this one PROVES the reset above has
        // already been handled -- no sleep, no race.
        let mut h2 = h2.ready().await.unwrap();
        let probe = http::Request::builder()
            .method("GET")
            .uri("https://example.com/probe")
            .body(())
            .unwrap();
        let (probe_response, _) = h2.send_request(probe, true).unwrap();
        let _ = probe_response.await;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    });

    let mut connection = handshake(Box::new(server_io), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let accepted = SessionV2::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
        .expect("the first stream");
    let pingora_core::protocols::http::v2::server::H2Accept::Session(poisoned) = accepted else {
        panic!("the first stream was unexpectedly rejected");
    };
    assert!(
        poisoned.request_headers_end_stream(),
        "precondition: the END_STREAM fact was established before the reset"
    );
    accepted_tx.send(()).unwrap();
    assert!(
        SessionV2::from_h2_conn(&mut connection, digest)
            .await
            .unwrap()
            .is_some(),
        "the probe stream proves the reset has been processed"
    );

    let modules = HttpModules::new();
    let mut session = Session::new(
        Box::new(ServerSession::new_http2(poisoned)),
        &modules,
        Arc::new(AtomicBool::new(false)),
    );

    let upstream_request = RequestHeader::build("GET", b"/", None).unwrap();
    let facts = DispositionFacts::collect(&mut session, &upstream_request);
    assert!(
        facts.body_empty,
        "a bodyless request stays bodyless after the client resets the stream"
    );
    assert_eq!(
        safe_disposition(UpstreamRequestBodyDisposition::Streamed, facts),
        UpstreamRequestBodyDisposition::Ordinary,
        "a request with no body must never be re-framed as chunked"
    );

    drop(session);
    drop(connection);
    client.abort();
}

/// [`safe_upstream_disposition`]'s short-circuit must not change observable
/// behavior: it must skip fact collection ONLY for `Ordinary`, never for a
/// selection that actually needs coercing.
///
/// Both assertions run against the SAME live, bodyless session -- a shape
/// that WOULD trigger coercion if the facts were consulted -- so the first
/// assertion cannot pass merely because there was nothing to coerce in the
/// first place. `Ordinary` coming back unchanged here is exactly what the
/// old collect-then-coerce path also produced (`safe_disposition_truth_table`
/// proves `Ordinary` is `safe_disposition`'s fixed point for every fact
/// combination), so the skip is observably a no-op. `Streamed` on the same
/// session must still be coerced back to `Ordinary`, proving the
/// short-circuit's `if` does not accidentally swallow the case it exists to
/// let through.
#[tokio::test]
async fn safe_upstream_disposition_short_circuits_ordinary_only() {
    use pingora_core::modules::http::HttpModules;
    use pingora_core::protocols::http::v2::server::{handshake, HttpSession as SessionV2};
    use pingora_core::protocols::http::ServerSession;
    use pingora_core::protocols::Digest;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    let (client_io, server_io) = tokio::io::duplex(65536);
    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();
        let req = http::Request::builder()
            .method("GET")
            .uri("https://example.com/")
            .body(())
            .unwrap();
        // END_STREAM on HEADERS: a request with no body at all, the shape
        // `safe_disposition` coerces a non-`Ordinary` selection away from.
        let (response, _body) = h2.send_request(req, true).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), response).await;
    });

    let mut connection = handshake(Box::new(server_io), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let accepted = SessionV2::from_h2_conn(&mut connection, digest)
        .await
        .unwrap()
        .expect("the request stream");
    let pingora_core::protocols::http::v2::server::H2Accept::Session(session_v2) = accepted else {
        panic!("the request stream was unexpectedly rejected");
    };

    let modules = HttpModules::new();
    let mut session = Session::new(
        Box::new(ServerSession::new_http2(session_v2)),
        &modules,
        Arc::new(AtomicBool::new(false)),
    );

    let upstream_request = RequestHeader::build("GET", b"/", None).unwrap();

    assert_eq!(
        safe_upstream_disposition(
            UpstreamRequestBodyDisposition::Ordinary,
            &mut session,
            &upstream_request,
            false,
        ),
        UpstreamRequestBodyDisposition::Ordinary,
        "Ordinary must pass through unchanged, exactly as the old \
         collect-then-coerce path also produced"
    );
    assert_eq!(
        safe_upstream_disposition(
            UpstreamRequestBodyDisposition::Streamed,
            &mut session,
            &upstream_request,
            false,
        ),
        UpstreamRequestBodyDisposition::Ordinary,
        "a bodyless request must still be coerced back to Ordinary"
    );

    drop(session);
    drop(connection);
    client.abort();
}

/// The fact both pumps that have no end-of-stream handling of their own
/// depend on, against a LIVE session.
///
/// An H2 request declaring `Content-Length: 0` WITHOUT END_STREAM is empty
/// but not finished: `is_body_done()` is `false` and stays `false` until the
/// client sends an end of stream it may never send. The custom-connector
/// pump and the subrequest pipe derive their upstream end-of-stream from
/// `is_body_empty()` and have neither a bodyless prelude nor a futile-read
/// guard, so initialising their downstream state machine from the strict
/// fact leaves the two halves contradicting each other and parks the pump
/// forever on a read that can never yield -- there is no downstream
/// request-body idle timeout to break it. `no_downstream_body_to_read` is
/// the union that keeps them agreeing, exactly as they did before
/// `is_body_done()` was tightened.
#[tokio::test]
async fn no_downstream_body_to_read_covers_a_declared_empty_body() {
    use pingora_core::modules::http::HttpModules;
    use pingora_core::protocols::http::v2::server::{handshake, HttpSession as SessionV2};
    use pingora_core::protocols::http::ServerSession;
    use pingora_core::protocols::Digest;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    let (client_io, server_io) = tokio::io::duplex(65536);
    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client_io).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();
        let req = http::Request::builder()
            .method("POST")
            .uri("https://example.com/")
            .header("content-length", "0")
            .body(())
            .unwrap();
        // No END_STREAM, and this client never sends one.
        let (response, _body) = h2.send_request(req, false).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), response).await;
    });

    let mut connection = handshake(Box::new(server_io), None).await.unwrap();
    let accepted = SessionV2::from_h2_conn(&mut connection, Arc::new(Digest::default()))
        .await
        .unwrap()
        .expect("the request stream");
    let pingora_core::protocols::http::v2::server::H2Accept::Session(session_v2) = accepted else {
        panic!("the request stream was unexpectedly rejected");
    };

    let modules = HttpModules::new();
    let mut session = Session::new(
        Box::new(ServerSession::new_http2(session_v2)),
        &modules,
        Arc::new(AtomicBool::new(false)),
    );

    assert!(
        session.as_mut().is_body_empty(),
        "`Content-Length: 0` promises zero body bytes"
    );
    assert!(
        !session.as_mut().is_body_done(),
        "precondition: the transport has NOT ended, which is what would park the pump"
    );
    assert!(
        no_downstream_body_to_read(&mut session),
        "a pump without end-of-stream handling has nothing to read here"
    );

    drop(session);
    drop(connection);
    client.abort();
}

/// Only real bytes under `Bodyless` are a contract violation. Every other
/// cell of the grid is a shape that legitimately reaches the same
/// suppressed-write plumbing.
#[test]
fn violates_bodyless_contract_only_on_real_bytes_under_bodyless() {
    use UpstreamRequestBodyDisposition::*;
    let events: [(&str, Option<Bytes>); 3] = [
        // The end-of-stream event of any request.
        ("end of stream", None),
        // A chunk the filters emptied, or a zero-length transport read.
        ("empty chunk", Some(Bytes::new())),
        ("real bytes", Some(Bytes::from_static(b"hello"))),
    ];
    for disposition in [Ordinary, Bodyless, Streamed] {
        for (name, data) in events.iter() {
            let expected = disposition == Bodyless && *name == "real bytes";
            assert_eq!(
                violates_bodyless_contract(disposition, data.as_ref()),
                expected,
                "{disposition:?} with {name}"
            );
        }
    }
}
