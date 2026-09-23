use super::*;
use crate::protocols::http::body_buffer::InMemoryRequestBodyBuffer;
use http::{Method, Request};
use tokio::io::duplex;

#[tokio::test]
async fn prefix_boundaries_and_live_continuation() {
    for len in [3, 4, 5, 20] {
        let data = (0..len).map(|i| b'a' + i as u8).collect::<Vec<_>>();
        let sent = data.clone();
        let (client, server) = duplex(65536);
        let client = tokio::spawn(async move {
            let (h2, connection) = h2::client::handshake(client).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut h2 = h2.ready().await.unwrap();
            let request = Request::builder()
                .method(Method::POST)
                .uri("https://example.com/")
                .header("content-length", len)
                .body(())
                .unwrap();
            let (response, mut body) = h2.send_request(request, false).unwrap();
            let split = len.min(7);
            body.send_data(Bytes::copy_from_slice(&sent[..split]), split == len)
                .unwrap();
            if split < len {
                body.send_data(Bytes::copy_from_slice(&sent[split..]), true)
                    .unwrap();
            }
            assert_eq!(response.await.unwrap().status(), 200);
        });
        let mut connection = handshake(Box::new(server), None).await.unwrap();
        let digest = Arc::new(Digest::default());
        let mut handlers = Vec::new();
        while let Some(mut session) = HttpSession::from_h2_conn(&mut connection, digest.clone())
            .await
            .unwrap()
        {
            let data = data.clone();
            handlers.push(tokio::spawn(async move {
                session
                    .capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
                    .await
                    .unwrap();
                assert_eq!(session.body_bytes_read(), len.min(7));
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
                session
                    .write_response_header(
                        Box::new(ResponseHeader::build(200, None).unwrap()),
                        true,
                    )
                    .unwrap();
            }));
        }
        client.await.unwrap();
        for handler in handlers {
            handler.await.unwrap();
        }
    }
}

#[tokio::test]
async fn cancelled_prefix_transport_read_cannot_resume_or_forward() {
    let (client, server) = duplex(65536);
    let client = tokio::spawn(async move {
        let (h2, connection) = h2::client::handshake(client).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut h2 = h2.ready().await.unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .uri("https://example.com/")
            .header("content-length", 8)
            .body(())
            .unwrap();
        let (response, mut body) = h2.send_request(request, false).unwrap();
        body.send_data(Bytes::from_static(b"ab"), false).unwrap();
        assert_eq!(response.await.unwrap().status(), 403);
    });
    let mut connection = handshake(Box::new(server), None).await.unwrap();
    let digest = Arc::new(Digest::default());
    let mut handlers = Vec::new();
    while let Some(mut session) = HttpSession::from_h2_conn(&mut connection, digest.clone())
        .await
        .unwrap()
    {
        handlers.push(tokio::spawn(async move {
            assert!(tokio::time::timeout(
                std::time::Duration::from_millis(20),
                session.capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
            )
            .await
            .is_err());
            assert!(session.request_body_prefix_active());
            assert!(session.read_body_bytes().await.is_err());
            assert!(session.begin_request_body_replay().await.is_err());
            assert!(session.drain_request_body().await.is_err());
            assert!(session.begin_request_body_replay().await.is_err());
            session
                .write_response_header(Box::new(ResponseHeader::build(403, None).unwrap()), true)
                .unwrap();
        }));
    }
    client.await.unwrap();
    for handler in handlers {
        handler.await.unwrap();
    }
}

#[tokio::test]
async fn discarded_prefix_restores_transport_completion() {
    for len in [3, 20] {
        for respond_first in [false, true] {
            let (client, server) = duplex(65536);
            let client = tokio::spawn(async move {
                let (h2, connection) = h2::client::handshake(client).await.unwrap();
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                let mut h2 = h2.ready().await.unwrap();
                let request = Request::builder()
                    .method(Method::POST)
                    .uri("https://example.com/")
                    .header("content-length", len)
                    .body(())
                    .unwrap();
                let (response, mut body) = h2.send_request(request, false).unwrap();
                let split = len.min(7);
                body.send_data(Bytes::from(vec![b'x'; split]), split == len)
                    .unwrap();
                if split < len {
                    body.send_data(Bytes::from(vec![b'x'; len - split]), true)
                        .unwrap();
                }
                assert_eq!(response.await.unwrap().status(), 403);
            });
            let mut connection = handshake(Box::new(server), None).await.unwrap();
            let mut handlers = Vec::new();
            while let Some(mut session) =
                HttpSession::from_h2_conn(&mut connection, Arc::new(Digest::default()))
                    .await
                    .unwrap()
            {
                handlers.push(tokio::spawn(async move {
                    session
                        .capture_request_body_prefix(Box::new(InMemoryRequestBodyBuffer::new()), 4)
                        .await
                        .unwrap();
                    assert_eq!(session.body_bytes_read(), len.min(7));
                    if respond_first {
                        session
                            .write_response_header(
                                Box::new(ResponseHeader::build(403, None).unwrap()),
                                true,
                            )
                            .unwrap();
                        assert_eq!(session.is_body_done(), len <= 7);
                    }
                    session.drain_request_body().await.unwrap();
                    assert!(session.is_body_done());
                    assert_eq!(session.body_bytes_read(), len);
                    assert!(session.request_body_prefix_active());
                    assert!(session.begin_request_body_replay().await.is_err());
                    if !respond_first {
                        session
                            .write_response_header(
                                Box::new(ResponseHeader::build(403, None).unwrap()),
                                true,
                            )
                            .unwrap();
                    }
                }));
            }
            client.await.unwrap();
            for handler in handlers {
                handler.await.unwrap();
            }
        }
    }
}
