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

use std::time::Duration;

#[tokio::test]
async fn test_listen_tcp_proxy_protocol_v2() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = "127.0.0.1:0";
    let mut listeners = Listeners::new();
    let sock_opt = TcpSocketOptions {
        proxy_protocol: true,
        ..Default::default()
    };
    listeners.add_tcp_with_settings(addr, sock_opt);
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let mut stream = stream.handshake().await.unwrap();
        let digest = stream.get_socket_digest().unwrap();
        assert_eq!(digest.peer_addr().unwrap().to_string(), "192.168.0.1:56324");
        assert_eq!(digest.local_addr().unwrap().to_string(), "10.0.0.2:443");
        // The LB's own address stays recoverable behind the override.
        let raw = digest.raw_peer_addr().unwrap().as_inet().unwrap();
        assert_eq!(raw.ip().to_string(), "127.0.0.1");
        assert_ne!(raw.to_string(), "192.168.0.1:56324");
        let mut buf = [0; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut header: Vec<u8> = vec![
        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, // sig
        0x21, // v2, PROXY
        0x11, // INET, STREAM
        0x00, 0x0C, // len 12
    ];
    header.extend_from_slice(&[192, 168, 0, 1, 10, 0, 0, 2, 0xdc, 0x04, 0x01, 0xbb]);
    header.extend_from_slice(b"hello");
    client.write_all(&header).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn test_listen_tcp_proxy_protocol_v1() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = "127.0.0.1:0";
    let mut listeners = Listeners::new();
    let sock_opt = TcpSocketOptions {
        proxy_protocol: true,
        ..Default::default()
    };
    listeners.add_tcp_with_settings(addr, sock_opt);
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let mut stream = stream.handshake().await.unwrap();
        let digest = stream.get_socket_digest().unwrap();
        assert_eq!(digest.peer_addr().unwrap().to_string(), "192.168.0.1:56324");
        let mut buf = [0; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"PROXY TCP4 192.168.0.1 10.0.0.2 56324 443\r\nhello")
        .await
        .unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn test_listen_tcp_proxy_protocol_missing_header() {
    use tokio::io::AsyncWriteExt;

    let addr = "127.0.0.1:0";
    let mut listeners = Listeners::new();
    let sock_opt = TcpSocketOptions {
        proxy_protocol: true,
        ..Default::default()
    };
    listeners.add_tcp_with_settings(addr, sock_opt);
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        // a connection without the header must fail the handshake
        assert!(stream.handshake().await.is_err());
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn test_listen_tcp_proxy_protocol_local() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = "127.0.0.1:0";
    let mut listeners = Listeners::new();
    let sock_opt = TcpSocketOptions {
        proxy_protocol: true,
        ..Default::default()
    };
    listeners.add_tcp_with_settings(addr, sock_opt);
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let mut stream = stream.handshake().await.unwrap();
        // LOCAL carries no addresses: the real socket peer is kept
        let digest = stream.get_socket_digest().unwrap();
        let peer = digest.peer_addr().unwrap().as_inet().unwrap();
        assert_eq!(peer.ip().to_string(), "127.0.0.1");
        let mut buf = [0; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    // v2 LOCAL, UNSPEC family, no payload: what an NLB health check sends
    client
        .write_all(&[
            0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, 0x20, 0x00,
            0x00, 0x00,
        ])
        .await
        .unwrap();
    client.write_all(b"ping").await.unwrap();
    server.await.unwrap();
}

/// The PROXY header must be consumed before the TLS handshake, since it
/// precedes the ClientHello on the wire. Both backends report their own
/// error type, so which stage rejected a connection is observable without
/// needing a TLS client here.
#[tokio::test]
#[cfg(feature = "any_tls")]
async fn test_listen_tls_proxy_protocol_runs_before_tls() {
    use pingora_error::ErrorType;
    use tokio::io::AsyncWriteExt;

    let addr = "127.0.0.1:0";
    let cert_path = format!("{}/tests/keys/server.crt", env!("CARGO_MANIFEST_DIR"));
    let key_path = format!("{}/tests/keys/key.pem", env!("CARGO_MANIFEST_DIR"));
    let mut listeners = Listeners::new();
    let sock_opt = TcpSocketOptions {
        proxy_protocol: true,
        ..Default::default()
    };
    listeners.add_tls_with_settings(
        addr,
        Some(sock_opt),
        TlsSettings::intermediate(&cert_path, &key_path).unwrap(),
    );
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        // a valid header followed by a bogus ClientHello gets past the
        // proxy protocol stage and fails inside TLS
        let stream = listener.accept().await.unwrap();
        let err = stream.handshake().await.unwrap_err();
        assert_eq!(err.etype(), &ErrorType::TLSHandshakeFailure);

        // the same bytes without a header never reach TLS
        let stream = listener.accept().await.unwrap();
        let err = stream.handshake().await.unwrap_err();
        assert_eq!(err.etype(), &ErrorType::HandshakeError);
    });
    let mut header: Vec<u8> = vec![
        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, 0x21, 0x11,
        0x00, 0x0C,
    ];
    header.extend_from_slice(&[192, 168, 0, 1, 10, 0, 0, 2, 0xdc, 0x04, 0x01, 0xbb]);
    header.extend_from_slice(b"not a client hello");
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&header).await.unwrap();
    drop(client);

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(b"not a client hello").await.unwrap();
    drop(client);

    server.await.unwrap();
}

/// A valid PROXY header must be removed before TLS, while its rewritten and
/// transport addresses remain available after a successful handshake.
#[tokio::test]
#[cfg(feature = "openssl_derived")]
async fn test_listen_tls_proxy_protocol_delivers_data_and_addresses() {
    use crate::protocols::tls::SslStream;
    use crate::tls::ssl;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let cert_path = format!("{}/tests/keys/server.crt", env!("CARGO_MANIFEST_DIR"));
    let key_path = format!("{}/tests/keys/key.pem", env!("CARGO_MANIFEST_DIR"));
    let mut listeners = Listeners::new();
    let sock_opt = TcpSocketOptions {
        proxy_protocol: true,
        ..Default::default()
    };
    listeners.add_tls_with_settings(
        "127.0.0.1:0",
        Some(sock_opt),
        TlsSettings::intermediate(&cert_path, &key_path).unwrap(),
    );
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let mut client = TcpStream::connect(addr).await.unwrap();
    let raw_peer_addr = client.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let mut stream = stream.handshake().await.unwrap();
        let digest = stream.get_socket_digest().unwrap();
        assert_eq!(digest.peer_addr().unwrap().to_string(), "192.168.0.1:56324");
        assert_eq!(digest.local_addr().unwrap().to_string(), "10.0.0.2:443");
        let raw = digest.raw_peer_addr().unwrap().as_inet().unwrap();
        assert_eq!(*raw, raw_peer_addr);

        let mut buf = [0; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    });

    client.write_all(&v2_inet_header()).await.unwrap();

    let ssl_context = ssl::SslContext::builder(ssl::SslMethod::tls())
        .unwrap()
        .build();
    let mut ssl = ssl::Ssl::new(&ssl_context).unwrap();
    ssl.set_hostname("localhost").unwrap();
    ssl.set_verify(ssl::SslVerifyMode::NONE);
    let mut client = SslStream::new(ssl, client).unwrap();
    client.connect().await.unwrap();
    client.write_all(b"ping").await.unwrap();

    server.await.unwrap();
}

#[derive(Debug)]
struct TrustNobody;

impl ProxyProtocolTrust for TrustNobody {
    fn is_trusted(&self, _addr: &std::net::SocketAddr) -> bool {
        false
    }
}

#[derive(Debug)]
struct TrustLoopback;

impl ProxyProtocolTrust for TrustLoopback {
    fn is_trusted(&self, addr: &std::net::SocketAddr) -> bool {
        addr.ip().is_loopback()
    }
}

fn conditional_opts(trust: Arc<dyn ProxyProtocolTrust>) -> TcpSocketOptions {
    TcpSocketOptions {
        proxy_protocol: true,
        proxy_protocol_trusted_sources: Some(trust),
        ..Default::default()
    }
}

fn v2_inet_header() -> Vec<u8> {
    let mut header: Vec<u8> = vec![
        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, 0x21, 0x11,
        0x00, 0x0C,
    ];
    header.extend_from_slice(&[192, 168, 0, 1, 10, 0, 0, 2, 0xdc, 0x04, 0x01, 0xbb]);
    header
}

/// THE security invariant of the whole feature: an untrusted peer's header
/// must be ignored entirely — neither honoured nor consumed — and the
/// connection served as an ordinary one.
#[tokio::test]
async fn test_untrusted_peer_forged_header_is_not_honoured() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = "127.0.0.1:0";
    let mut listeners = Listeners::new();
    listeners.add_tcp_with_settings(addr, conditional_opts(Arc::new(TrustNobody)));
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let mut stream = stream.handshake().await.unwrap();
        // The spoofed address must not have been adopted.
        let digest = stream.get_socket_digest().unwrap();
        let peer = digest.peer_addr().unwrap().as_inet().unwrap();
        assert_eq!(peer.ip().to_string(), "127.0.0.1");
        assert_ne!(peer.to_string(), "192.168.0.1:56324");
        // And the bytes must reach the application verbatim: an
        // implementation that consumes-but-ignores is also wrong. Bounded,
        // because an implementation that ate them would leave this read
        // blocked on a client that never closes — a CI timeout says far
        // less than an assertion does.
        let mut buf = [0u8; 12];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
            .await
            .expect("header bytes were consumed instead of being left for the application")
            .unwrap();
        assert_eq!(
            &buf,
            &[0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A]
        );
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&v2_inet_header()).await.unwrap();
    server.await.unwrap();
}

/// The trust check must come BEFORE the peek. Both orderings look alike for
/// a peer that sends plenty of bytes, so this one sends fewer than the
/// 12-byte probe wants and never closes: if the peek ran first the handshake
/// would block until the 60s downstream timeout, letting any untrusted peer
/// pin a connection.
#[tokio::test]
async fn test_untrusted_peer_is_not_probed_before_the_trust_check() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = "127.0.0.1:0";
    let mut listeners = Listeners::new();
    listeners.add_tcp_with_settings(addr, conditional_opts(Arc::new(TrustNobody)));
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let mut stream = stream.handshake().await.unwrap();
        let mut buf = [0u8; 3];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hey");
    });
    // Three bytes, connection held open. A trust check that runs first never
    // reads; a peek that runs first waits forever for 12.
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(b"hey").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("handshake blocked: the peek ran before the trust check")
        .unwrap();
}

/// The mixed-mode case: a trusted peer that sends no header is served as an
/// ordinary connection with its own address.
///
/// The probe waits for 12 bytes, so this sends a real request rather than a
/// token few bytes. That is not an artifact of the test: a client which
/// opens a connection and then says less than 12 bytes really is held until
/// the handshake timeout, which is why conditional mode does not suit
/// listeners fronting server-speaks-first protocols.
#[tokio::test]
async fn test_trusted_peer_without_header_is_served_directly() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = "127.0.0.1:0";
    let mut listeners = Listeners::new();
    listeners.add_tcp_with_settings(addr, conditional_opts(Arc::new(TrustLoopback)));
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let mut stream = stream.handshake().await.unwrap();
        let digest = stream.get_socket_digest().unwrap();
        assert_eq!(
            digest
                .peer_addr()
                .unwrap()
                .as_inet()
                .unwrap()
                .ip()
                .to_string(),
            "127.0.0.1"
        );
        let mut buf = [0u8; 18];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"GET / HTTP/1.1\r\n\r\n");
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    server.await.unwrap();
}

/// A trusted peer that does send one is honoured, as in mandatory mode.
#[tokio::test]
async fn test_trusted_peer_with_header_is_honoured() {
    use tokio::io::AsyncWriteExt;

    let addr = "127.0.0.1:0";
    let mut listeners = Listeners::new();
    listeners.add_tcp_with_settings(addr, conditional_opts(Arc::new(TrustLoopback)));
    let listener = listeners
        .build(
            #[cfg(unix)]
            None,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let addr = listener.l4.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let stream = stream.handshake().await.unwrap();
        let digest = stream.get_socket_digest().unwrap();
        assert_eq!(digest.peer_addr().unwrap().to_string(), "192.168.0.1:56324");
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(&v2_inet_header()).await.unwrap();
    server.await.unwrap();
}
