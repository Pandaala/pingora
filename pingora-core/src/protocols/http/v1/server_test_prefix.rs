use super::*;
use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
use tokio_test::io::Builder;

#[tokio::test]
async fn prefix_boundaries_and_live_continuation() {
    for len in [3, 4, 5, 20] {
        let data = (0..len).map(|i| b'a' + i as u8).collect::<Vec<_>>();
        let header = format!("POST / HTTP/1.1\r\nHost: test\r\nContent-Length: {len}\r\n\r\n");
        let split = len.min(7);
        let mut io = Builder::new();
        io.read(header.as_bytes()).read(&data[..split]);
        if split < len {
            io.read(&data[split..]);
        }
        let mut session = HttpSession::new(Box::new(io.build()));
        session.read_request().await.unwrap();
        session
            .capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
            .await
            .unwrap();
        assert_eq!(session.body_bytes_read(), split);
        assert_eq!(
            session.request_body_prefix_transport_complete(),
            split == len
        );
        assert!(!session.is_body_done());
        assert!(session.read_body_bytes().await.is_err());
        session.begin_request_body_replay().await.unwrap();
        let first = session.read_body_or_idle(false).await.unwrap().unwrap();
        assert_eq!(first.as_ref(), &data[..len.min(4)]);
        let mut forwarded = first.to_vec();
        while !session.is_body_done() {
            if let Some(chunk) = session.read_body_or_idle(false).await.unwrap() {
                forwarded.extend_from_slice(&chunk);
            }
        }
        assert_eq!(forwarded, data);
        assert_eq!(session.body_bytes_read(), len);
        assert!(session.begin_request_body_replay().await.is_err());
        assert!(session
            .set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()))
            .is_err());
    }
}

#[tokio::test]
async fn cancelled_prefix_transport_read_cannot_resume_or_forward() {
    let (mut client, server) = tokio::io::duplex(4096);
    use tokio::io::AsyncWriteExt;
    client
        .write_all(b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 8\r\n\r\nab")
        .await
        .unwrap();
    let mut session = HttpSession::new(Box::new(server));
    session.read_request().await.unwrap();
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(20),
        session.capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
    )
    .await
    .is_err());
    assert!(session.request_body_prefix_active());
    assert!(session.read_body_bytes().await.is_err());
    assert!(session.begin_request_body_replay().await.is_err());
    assert!(session
        .capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
        .await
        .is_err());
    assert!(session.drain_request_body().await.is_err());
    assert!(session.begin_request_body_replay().await.is_err());
}

#[tokio::test]
async fn rejected_complete_prefix_keeps_connection_reusable() {
    use tokio::io::AsyncWriteExt;
    let (mut client, server) = tokio::io::duplex(4096);
    client
        .write_all(b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 3\r\n\r\nabc")
        .await
        .unwrap();
    let mut session = HttpSession::new(Box::new(server));
    session.read_request().await.unwrap();
    session
        .capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
        .await
        .unwrap();
    let mut response = ResponseHeader::build(403, None).unwrap();
    response.insert_header("Content-Length", "0").unwrap();
    session
        .write_response_header(Box::new(response))
        .await
        .unwrap();
    assert!(session.is_body_done());
    assert!(session.will_keepalive());
    assert!(session.request_body_prefix_active());
    assert!(session.begin_request_body_replay().await.is_err());
    let (stream, prefix) = session.reuse().await.unwrap().unwrap().into_parts();
    assert!(prefix.is_none());
    client
        .write_all(b"GET /next HTTP/1.1\r\nHost: test\r\n\r\n")
        .await
        .unwrap();
    let mut next = HttpSession::new(stream);
    assert!(next.read_request().await.unwrap().is_some());
    assert_eq!(next.req_header().uri.path(), "/next");
}

#[tokio::test]
async fn explicit_prefix_discard_drains_only_the_live_remainder() {
    for len in [3, 20] {
        let data = vec![b'x'; len];
        let header = format!("POST / HTTP/1.1\r\nHost: test\r\nContent-Length: {len}\r\n\r\n");
        let split = len.min(7);
        let mut io = Builder::new();
        io.read(header.as_bytes()).read(&data[..split]);
        if split < len {
            io.read(&data[split..]);
        }
        let mut session = HttpSession::new(Box::new(io.build()));
        session.read_request().await.unwrap();
        session
            .capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
            .await
            .unwrap();
        assert_eq!(session.body_bytes_read(), split);
        session.drain_request_body().await.unwrap();
        assert!(session.is_body_done());
        assert_eq!(session.body_bytes_read(), len);
        assert!(session.request_body_prefix_active());
        assert!(session.begin_request_body_replay().await.is_err());
    }
}

#[tokio::test]
async fn final_response_preserves_an_active_prefix_replay() {
    let (mut client, server) = tokio::io::duplex(4096);
    use tokio::io::AsyncWriteExt;
    client
        .write_all(b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\nhello")
        .await
        .unwrap();
    let mut session = HttpSession::new(Box::new(server));
    session.read_request().await.unwrap();
    session
        .capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
        .await
        .unwrap();
    session.begin_request_body_replay().await.unwrap();
    session
        .write_response_header(Box::new(ResponseHeader::build(200, None).unwrap()))
        .await
        .unwrap();
    let mut body = Vec::new();
    while !session.is_body_done() {
        if let Some(chunk) = session.read_body_or_idle(false).await.unwrap() {
            body.extend_from_slice(&chunk);
        }
    }
    assert_eq!(body, b"hello");
    assert!(session.early_body_buffer.is_none());
}
