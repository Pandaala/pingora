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

#[test]
fn test_streamed_disposition_removes_h2_framing_and_keeps_stream_open() {
    let mut request = RequestHeader::build("POST", b"/", None).unwrap();
    request.insert_header(CONTENT_LENGTH, "0").unwrap();
    request
        .insert_header(http::header::TRANSFER_ENCODING, "chunked")
        .unwrap();

    apply_upstream_body_disposition(&mut request, UpstreamRequestBodyDisposition::Streamed);

    assert!(request.headers.get(CONTENT_LENGTH).is_none());
    assert!(request
        .headers
        .get(http::header::TRANSFER_ENCODING)
        .is_none());
    assert!(!upstream_headers_end_stream(
        UpstreamRequestBodyDisposition::Streamed,
        true,
        true
    ));
}

/// Full truth table of the upstream EOS decision, as the pump applies it:
/// [`upstream_empty_data_end_stream`] is only consulted when
/// [`upstream_headers_end_stream`] said `false`. Every row is
/// (disposition, send_end_stream, body_empty) -> (headers_eos, empty_data_eos),
/// and the pair must always produce AT MOST ONE upstream EOS -- exactly one
/// whenever no downstream body can still arrive.
///
/// This pins the PRIMITIVES over their whole input domain. Which `body_empty`
/// each disposition is actually handed is a separate decision made by
/// [`upstream_framing_body_empty`], and it pins the `Streamed` rows below to
/// `body_empty == false`; see
/// `test_streamed_never_takes_an_early_eos_from_the_call_site`.
#[test]
fn test_upstream_eos_truth_table() {
    use UpstreamRequestBodyDisposition::*;

    // (disposition, send_end_stream, body_empty, headers_eos, empty_data_eos)
    let table = [
        // Ordinary: unchanged legacy behavior. The EOS rides on HEADERS when
        // allowed, otherwise on an empty DATA frame; with a body, neither.
        (Ordinary, true, true, true, false),
        (Ordinary, true, false, false, false),
        (Ordinary, false, true, false, true),
        (Ordinary, false, false, false, false),
        // Bodyless: no upstream body will follow, so the stream closes here
        // either way. `send_end_stream == false` (the gRPC-web bridge) MUST
        // get the empty DATA frame, not END_STREAM on HEADERS.
        (Bodyless, true, true, true, false),
        (Bodyless, true, false, true, false),
        (Bodyless, false, true, false, true),
        (Bodyless, false, false, false, true),
        // Streamed: HEADERS never carry EOS (the length is unknown at header
        // time). With a downstream body already finished nothing will ever be
        // read, so close now; otherwise the pump sends the EOS with the body.
        (Streamed, true, true, false, true),
        (Streamed, true, false, false, false),
        (Streamed, false, true, false, true),
        (Streamed, false, false, false, false),
    ];

    for (disposition, send_end_stream, body_empty, headers_eos, data_eos) in table {
        let actual_headers_eos =
            upstream_headers_end_stream(disposition, send_end_stream, body_empty);
        assert_eq!(
            actual_headers_eos, headers_eos,
            "headers EOS for {disposition:?} send_end_stream={send_end_stream} body_empty={body_empty}"
        );
        // As the pump applies it: gated on the headers decision, so an
        // already-closed stream never gets a second, standalone END_STREAM.
        let actual_data_eos = !actual_headers_eos
            && upstream_empty_data_end_stream(disposition, send_end_stream, body_empty);
        assert_eq!(
            actual_data_eos, data_eos,
            "empty-DATA EOS for {disposition:?} send_end_stream={send_end_stream} body_empty={body_empty}"
        );
        // Whenever the downstream body is already finished, exactly one EOS
        // must have been emitted here; otherwise the pump still owns it.
        if body_empty {
            assert!(
                actual_headers_eos ^ actual_data_eos,
                "no single upstream EOS for {disposition:?} send_end_stream={send_end_stream}"
            );
        }
    }
}

/// The gRPC-web bridge calls `set_send_end_stream(false)` because gRPC
/// requires a bodyless request stream to be closed by an empty DATA frame
/// with END_STREAM. `Bodyless` must not override that.
#[test]
fn test_bodyless_honors_explicit_send_end_stream_false() {
    assert!(!upstream_headers_end_stream(
        UpstreamRequestBodyDisposition::Bodyless,
        false,
        false
    ));
    assert!(upstream_empty_data_end_stream(
        UpstreamRequestBodyDisposition::Bodyless,
        false,
        false
    ));
}

/// `Streamed` must NEVER get an early upstream EOS, whatever the request
/// declared (design 4.4).
///
/// This is asserted AT THE CALL SITE's own decision function, not at the
/// primitives: `upstream_empty_data_end_stream`'s `Streamed` arm does close the
/// stream when handed `body_empty == true`, and feeding it the request's
/// `Content-Length: 0` declaration is exactly the regression this pins. An early
/// EOS there sets `upstream_body_closed`, which makes the suppressed-write
/// branch of `send_body_to2` refuse every byte the application streams in
/// through `request_body_filter_action` -- the whole point of `Streamed`.
#[test]
fn test_streamed_never_takes_an_early_eos_from_the_call_site() {
    use UpstreamRequestBodyDisposition::*;
    for declared_empty in [false, true] {
        let body_empty = upstream_framing_body_empty(Streamed, declared_empty);
        assert!(
            !body_empty,
            "Streamed must not inherit the declaration (declared_empty={declared_empty})"
        );
        for send_end_stream in [true, false] {
            let headers_eos = upstream_headers_end_stream(Streamed, send_end_stream, body_empty);
            let data_eos = !headers_eos
                && upstream_empty_data_end_stream(Streamed, send_end_stream, body_empty);
            assert!(
                !headers_eos && !data_eos,
                "Streamed sent an early EOS (declared_empty={declared_empty} \
                 send_end_stream={send_end_stream})"
            );
        }
    }
}

/// The mirror row: `Ordinary` DOES take the declaration, which is what lets a
/// `Content-Length: 0` request reach an origin that will not answer until it has
/// seen the end of the request stream.
#[test]
fn test_ordinary_takes_the_declaration_for_upstream_framing() {
    use UpstreamRequestBodyDisposition::*;
    assert!(upstream_framing_body_empty(Ordinary, true));
    assert!(!upstream_framing_body_empty(Ordinary, false));
    // ...and exactly one EOS is emitted for it, wherever `send_end_stream` puts it.
    for send_end_stream in [true, false] {
        let body_empty = upstream_framing_body_empty(Ordinary, true);
        let headers_eos = upstream_headers_end_stream(Ordinary, send_end_stream, body_empty);
        let data_eos =
            !headers_eos && upstream_empty_data_end_stream(Ordinary, send_end_stream, body_empty);
        assert!(headers_eos ^ data_eos, "send_end_stream={send_end_stream}");
    }
}

/// `Bodyless` with a real downstream body closes the upstream stream at header
/// time under BOTH `send_end_stream` settings, which is exactly why the pump
/// has to suppress its body writes instead of letting h2 fail the stream.
#[test]
fn test_bodyless_with_a_real_body_always_closes_at_header_time() {
    use UpstreamRequestBodyDisposition::*;
    for send_end_stream in [true, false] {
        let headers_eos = upstream_headers_end_stream(Bodyless, send_end_stream, false);
        let data_eos =
            !headers_eos && upstream_empty_data_end_stream(Bodyless, send_end_stream, false);
        assert!(
            headers_eos ^ data_eos,
            "Bodyless send_end_stream={send_end_stream} must close the stream exactly once"
        );
    }
}
/// I2: the write-error swallow must be keyed on the failure SHAPE, not just on
/// the wire END_STREAM flag.
///
/// `upstream_response_ended` is set for every upstream response the peer ended
/// cleanly, and it stays set. If it were the whole condition, then after any
/// such response EVERY request-body write failure would be swallowed and the
/// exchange logged a success -- including an application body filter's own
/// error and the `Bodyless` contract violation, which have nothing to do with
/// the peer.
///
/// This function answers only "is the stream GONE". A `write_timeout` does not
/// make it gone and still answers `false` here; it reaches the swallow through
/// [`upstream_write_stalled_after_response`] instead, which asks a different
/// question. Keeping the two apart is the point -- see
/// `test_a_stalled_write_is_a_separate_swallowable_shape`.
#[test]
fn test_only_stream_gone_write_failures_may_be_swallowed() {
    // The two shapes `write_body` produces when h2 will never take another byte
    // on this stream.
    for (etype, context) in [
        (H2Error, "cannot reserve capacity"),
        (H2Error, "while waiting for capacity"),
        (WriteError, "while writing h2 request body"),
    ] {
        let e = Error::explain(etype, context.to_string());
        assert!(
            upstream_write_failed_because_stream_gone(&e),
            "{context} means the upstream stream is gone"
        );
    }

    // A local deadline is not a peer signal.
    let timed_out = Error::explain(
        WriteTimedout,
        "while writing h2 request body, timeout: 1s".to_string(),
    );
    assert!(
        !upstream_write_failed_because_stream_gone(&timed_out),
        "a locally configured write_timeout must still fail the exchange: swallowing \
         it truncates the upstream request body and reports success"
    );

    // And nothing else is a peer signal either -- the application's own body
    // filters, the `Bodyless` contract violation, the cache.
    for etype in [InternalError, ReadError, ReadTimedout, ConnectError] {
        let e = Error::explain(etype.clone(), "".to_string());
        assert!(
            !upstream_write_failed_because_stream_gone(&e),
            "{etype:?} is not the upstream closing its request stream"
        );
    }
}

/// The second swallowable shape: the stream is NOT gone, the peer simply
/// stopped granting request-body capacity after having answered in full.
///
/// Kept distinct from the stream-gone question on purpose. A `write_timeout`
/// is a LOCAL deadline and says nothing about the peer by itself, which is why
/// it must never widen [`upstream_write_failed_because_stream_gone`]; it only
/// carries meaning in conjunction with the wire END_STREAM flag, and
/// `upstream_write_error_outcome` is the only place that conjunction is formed.
#[test]
fn test_a_stalled_write_is_a_separate_swallowable_shape() {
    let timed_out = Error::explain(
        WriteTimedout,
        "while writing h2 request body, timeout: 1s".to_string(),
    );
    assert!(
        upstream_write_stalled_after_response(&timed_out),
        "an expired write window is the stalled shape"
    );
    assert!(
        !upstream_write_failed_because_stream_gone(&timed_out),
        "and it must NOT be laundered into the stream-gone shape"
    );

    // The stream-gone shapes are not stalls: they are answered by the other
    // predicate, and the two must not overlap.
    for (etype, context) in [
        (H2Error, "cannot reserve capacity"),
        (WriteError, "while writing h2 request body"),
    ] {
        let e = Error::explain(etype, context.to_string());
        assert!(
            !upstream_write_stalled_after_response(&e),
            "{context} means the stream is gone, not stalled"
        );
    }

    // Nothing else is a stall either.
    for etype in [InternalError, ReadError, ReadTimedout, ConnectError] {
        let e = Error::explain(etype.clone(), "".to_string());
        assert!(
            !upstream_write_stalled_after_response(&e),
            "{etype:?} is not the upstream withholding capacity"
        );
    }
}

#[test]
fn test_h2_write_timeout_floor_only_fills_an_unconfigured_timeout() {
    let shorter = Duration::from_millis(250);
    let longer = DEFAULT_H2_UPSTREAM_WRITE_TIMEOUT + Duration::from_secs(30);

    assert_eq!(
        effective_upstream_write_timeout(None),
        DEFAULT_H2_UPSTREAM_WRITE_TIMEOUT,
        "an unconfigured H2 capacity wait must have a finite liveness floor"
    );
    assert_eq!(
        effective_upstream_write_timeout(Some(shorter)),
        shorter,
        "an explicit shorter write timeout must win"
    );
    assert_eq!(
        effective_upstream_write_timeout(Some(longer)),
        longer,
        "an explicit longer write timeout must not be clamped to the floor"
    );
}

/// A successful upload abandonment must release connection capacity before
/// the abandoned `SendStream` is dropped. Otherwise a slow response consumer
/// can keep that handle alive and starve sibling streams on the same H2
/// connection.
#[tokio::test]
async fn test_abandoning_an_h2_upload_releases_capacity_to_a_live_sibling_stream() {
    use std::future::poll_fn;
    use tokio::io::duplex;

    let (client_io, server_io) = duplex(64 * 1024);
    let server = tokio::spawn(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        let mut streams = Vec::new();
        while let Some(stream) = conn.accept().await {
            streams.push(stream.unwrap());
        }
        streams
    });

    let (mut send_request, connection) = h2::client::handshake(client_io).await.unwrap();
    let client = tokio::spawn(async move { connection.await });

    send_request = send_request.ready().await.unwrap();
    let first = http::Request::builder()
        .uri("https://example.test/first")
        .body(())
        .unwrap();
    let (_, mut first_body) = send_request.send_request(first, false).unwrap();

    send_request = send_request.ready().await.unwrap();
    let second = http::Request::builder()
        .uri("https://example.test/second")
        .body(())
        .unwrap();
    let (_, mut second_body) = send_request.send_request(second, false).unwrap();

    const CONNECTION_WINDOW: usize = 65_535;
    first_body.reserve_capacity(CONNECTION_WINDOW);
    let first_capacity = tokio::time::timeout(
        Duration::from_secs(1),
        poll_fn(|cx| first_body.poll_capacity(cx)),
    )
    .await
    .expect("the first stream must receive the connection window")
    .expect("the first stream must remain open")
    .expect("the first stream capacity request must succeed");
    assert_eq!(first_capacity, CONNECTION_WINDOW);

    second_body.reserve_capacity(1);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            poll_fn(|cx| second_body.poll_capacity(cx)),
        )
        .await
        .is_err(),
        "the first stream must initially hold all connection capacity"
    );

    cancel_abandoned_upstream_body_capacity(&mut first_body);

    let second_capacity = tokio::time::timeout(
        Duration::from_secs(1),
        poll_fn(|cx| second_body.poll_capacity(cx)),
    )
    .await
    .expect("cancelling the abandoned reservation must wake the sibling stream")
    .expect("the sibling stream must remain open")
    .expect("the sibling capacity request must succeed");
    assert_eq!(second_capacity, 1);
    assert_eq!(first_body.capacity(), 0);

    drop((first_body, second_body, send_request));
    client.abort();
    server.abort();
}

/// Neither swallowable shape may fire without the wire END_STREAM flag.
///
/// The flag is the whole reason the exchange survives a failed request-body
/// write: it is what says the origin already answered. `PeerEndStream::default`
/// is the no-watch-installed case, where the flag can never be set -- and every
/// failure must then cost the exchange, exactly as it did before either shape
/// existed.
#[test]
fn test_no_write_failure_is_swallowed_without_wire_end_stream() {
    let body_write = UpstreamBodyWrite {
        timeout: None,
        stream_closed: false,
        disposition: UpstreamRequestBodyDisposition::Ordinary,
        eos_write_optional: false,
        upstream_response_ended: PeerEndStream::default(),
    };
    assert!(
        !body_write.upstream_response_ended.observed(),
        "a default PeerEndStream is the no-evidence case"
    );

    for (etype, context) in [
        (H2Error, "cannot reserve capacity"),
        (WriteError, "while writing h2 request body"),
        (WriteTimedout, "while writing h2 request body, timeout: 1s"),
    ] {
        let e = Error::explain(etype.clone(), context.to_string());
        assert!(
            upstream_write_error_outcome(e, true, &body_write).is_err(),
            "{etype:?} must fail the exchange with no wire END_STREAM evidence"
        );
    }
}
