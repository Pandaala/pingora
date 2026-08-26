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

//! The listening endpoints (TCP and TLS) and their configurations.
//!
//! This module provides the infrastructure for setting up network listeners
//! that accept incoming connections. It supports TCP, Unix domain sockets,
//! and TLS endpoints.
//!
//! # Connection Filtering
//!
//! With the `connection_filter` feature enabled, this module also provides
//! early connection filtering capabilities through the [`ConnectionFilter`] trait.
//! This allows dropping unwanted connections at the TCP level before any
//! expensive operations like TLS handshakes.
//!
//! ## Example with Connection Filtering
//!
//! ```rust,no_run
//! # #[cfg(feature = "connection_filter")]
//! # {
//! use pingora_core::listeners::{Listeners, ConnectionFilter};
//! use std::sync::Arc;
//!
//! // Create a custom filter
//! let filter = Arc::new(MyCustomFilter::new());
//!
//! // Apply to listeners
//! let mut listeners = Listeners::new();
//! listeners.set_connection_filter(filter);
//! listeners.add_tcp("0.0.0.0:8080");
//! # }
//! ```

mod l4;

#[cfg(feature = "connection_filter")]
pub mod connection_filter;

#[cfg(feature = "connection_filter")]
pub use connection_filter::{AcceptAllFilter, ConnectionFilter};

#[cfg(not(feature = "connection_filter"))]
#[derive(Debug, Clone)]
pub struct AcceptAllFilter;

#[cfg(not(feature = "connection_filter"))]
pub trait ConnectionFilter: std::fmt::Debug + Send + Sync {
    fn should_accept(&self, _addr: &std::net::SocketAddr) -> bool {
        true
    }
}

#[cfg(not(feature = "connection_filter"))]
impl ConnectionFilter for AcceptAllFilter {
    fn should_accept(&self, _addr: &std::net::SocketAddr) -> bool {
        true
    }
}
#[cfg(feature = "any_tls")]
pub mod tls;

#[cfg(not(feature = "any_tls"))]
pub use crate::tls::listeners as tls;

use crate::protocols::l4::proxy_protocol::ProxyProtocolTrust;
use crate::protocols::{l4::proxy_protocol, l4::socket::SocketAddr, tls::TlsRef, Stream};

#[cfg(unix)]
use crate::server::ListenFds;

use async_trait::async_trait;
use pingora_error::Result;
use std::{any::Any, fs::Permissions, sync::Arc};

use l4::{ListenerEndpoint, Stream as L4Stream};
use tls::{Acceptor, TlsSettings};

pub use crate::protocols::l4::stream::{
    L4BufferSettings, DEFAULT_L4_READ_BUFFER_SIZE, DEFAULT_L4_WRITE_BUFFER_SIZE,
};
pub use crate::protocols::tls::ALPN;
use crate::protocols::GetSocketDigest;
pub use l4::{ServerAddress, TcpSocketOptions};

/// The APIs to customize things like certificate during TLS server side handshake
#[async_trait]
pub trait TlsAccept {
    // TODO: return error?
    /// This function is called in the middle of a TLS handshake. Structs who
    /// implement this function should provide tls certificate and key to the
    /// [TlsRef] via `ssl_use_certificate` and `ssl_use_private_key`.
    /// Note. This is only supported for openssl and boringssl
    async fn certificate_callback(&self, _ssl: &mut TlsRef) -> () {
        // does nothing by default
    }

    /// This function is called after the TLS handshake is complete.
    ///
    /// Any value returned from this function (other than `None`) will be stored in the
    /// `extension` field of `SslDigest`. This allows you to attach custom application-specific
    /// data to the TLS connection, which will be accessible from the HTTP layer via the
    /// `SslDigest` attached to the session digest.
    async fn handshake_complete_callback(
        &self,
        _ssl: &TlsRef,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
}

pub type TlsAcceptCallbacks = Box<dyn TlsAccept + Send + Sync>;
#[cfg(any(feature = "openssl_derived", feature = "rustls"))]
pub(crate) type SharedTlsAcceptCallbacks = Arc<dyn TlsAccept + Send + Sync>;

/// Callback for processing raw bytes before TLS handshake.
///
/// This trait allows applications to read and process data from the raw TCP stream
/// before the TLS handshake occurs. This is useful for protocols like HAProxy's
/// PROXY protocol, which sends client address information before TLS.
///
/// # Example
///
/// ```rust,ignore
/// use pingora_core::listeners::PreTlsProcess;
/// use pingora_core::protocols::l4::stream::Stream as L4Stream;
/// use async_trait::async_trait;
///
/// struct ProxyProtocolHandler;
///
/// #[async_trait]
/// impl PreTlsProcess for ProxyProtocolHandler {
///     async fn process(&self, stream: &mut L4Stream) -> pingora_error::Result<()> {
///         // Read PROXY protocol header, update socket digest, etc.
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait PreTlsProcess: Send + Sync {
    /// Process the raw stream before TLS handshake.
    ///
    /// The implementation can read bytes from the stream (e.g., PROXY protocol header)
    /// and update the stream's socket digest with parsed information such as the
    /// real client address.
    ///
    /// If this method returns an error, the connection will be dropped.
    async fn process(&self, stream: &mut L4Stream) -> Result<()>;
}

/// Type alias for a boxed pre-TLS processor.
pub type PreTlsCallback = Arc<dyn PreTlsProcess>;

struct TransportStackBuilder {
    l4: ServerAddress,
    tls: Option<TlsSettings>,
    l4_buffer: L4BufferSettings,
    #[cfg(feature = "connection_filter")]
    connection_filter: Option<Arc<dyn ConnectionFilter>>,
    pre_tls_callback: Option<PreTlsCallback>,
}

impl TransportStackBuilder {
    pub async fn build(
        &mut self,
        #[cfg(unix)] upgrade_listeners: Option<ListenFds>,
    ) -> Result<TransportStack> {
        let mut builder = ListenerEndpoint::builder();

        builder.listen_addr(self.l4.clone());

        #[cfg(feature = "connection_filter")]
        if let Some(filter) = &self.connection_filter {
            builder.connection_filter(filter.clone());
        }

        #[cfg(unix)]
        let l4 = builder.listen(upgrade_listeners).await?;

        #[cfg(windows)]
        let l4 = builder.listen().await?;

        Ok(TransportStack {
            l4,
            tls: self.tls.take().map(|tls| Arc::new(tls.build())),
            l4_buffer: self.l4_buffer,
            pre_tls_callback: self.pre_tls_callback.clone(),
            proxy_protocol: self
                .l4
                .tcp_sock_opts()
                .is_some_and(|opts| opts.proxy_protocol),
            proxy_protocol_trusted: self
                .l4
                .tcp_sock_opts()
                .and_then(|opts| opts.proxy_protocol_trusted_sources.clone()),
        })
    }
}

/// Configuration for one listening endpoint.
///
/// This configures the endpoint address and endpoint-specific transport
/// settings such as [`TcpSocketOptions`], [`TlsSettings`], and L4
/// [`BufStream`](tokio::io::BufStream) buffer sizes.
pub struct ListenerConfig {
    l4: ServerAddress,
    tls: Option<TlsSettings>,
    l4_buffer: L4BufferSettings,
}

impl ListenerConfig {
    /// Create a TCP listening endpoint config.
    pub fn tcp(addr: impl Into<String>) -> Self {
        Self {
            l4: ServerAddress::Tcp(addr.into(), None),
            tls: None,
            l4_buffer: L4BufferSettings::default(),
        }
    }

    /// Create a Unix domain socket listening endpoint config.
    #[cfg(unix)]
    pub fn uds(addr: impl Into<String>) -> Self {
        Self {
            l4: ServerAddress::Uds(addr.into(), None),
            tls: None,
            l4_buffer: L4BufferSettings::default(),
        }
    }

    /// Set TCP socket options for this endpoint.
    ///
    /// # Panics
    ///
    /// Panics if this endpoint is not TCP.
    #[track_caller]
    pub fn tcp_socket_options(mut self, options: TcpSocketOptions) -> Self {
        match &mut self.l4 {
            ServerAddress::Tcp(_, opt) => *opt = Some(options),
            #[cfg(unix)]
            ServerAddress::Uds(_, _) => {
                panic!("TCP socket options can only be set on TCP endpoints")
            }
        }
        self
    }

    /// Set Unix domain socket permissions for this endpoint.
    ///
    /// # Panics
    ///
    /// Panics if this endpoint is not a Unix domain socket.
    #[cfg(unix)]
    #[track_caller]
    pub fn permissions(mut self, permissions: Permissions) -> Self {
        match &mut self.l4 {
            ServerAddress::Uds(_, perm) => *perm = Some(permissions),
            ServerAddress::Tcp(_, _) => {
                panic!("Unix domain socket permissions can only be set on UDS endpoints")
            }
        }
        self
    }

    /// Set TLS settings for this endpoint.
    pub fn tls(mut self, settings: TlsSettings) -> Self {
        self.tls = Some(settings);
        self
    }

    /// Set L4 `BufStream` buffer sizes for this endpoint.
    pub fn l4_buffer(mut self, settings: L4BufferSettings) -> Self {
        self.l4_buffer = settings;
        self
    }
}

#[derive(Clone)]
pub(crate) struct TransportStack {
    l4: ListenerEndpoint,
    tls: Option<Arc<Acceptor>>,
    l4_buffer: L4BufferSettings,
    pre_tls_callback: Option<PreTlsCallback>,
    // expect a PROXY protocol header on every accepted connection
    proxy_protocol: bool,
    // when set, only these peers may speak PROXY protocol; others are served
    // as ordinary direct connections
    proxy_protocol_trusted: Option<Arc<dyn ProxyProtocolTrust>>,
}

impl TransportStack {
    pub fn as_str(&self) -> &str {
        self.l4.as_str()
    }

    pub async fn accept(&self) -> Result<UninitializedStream> {
        let stream = self.l4.accept().await?;
        Ok(UninitializedStream {
            l4: stream,
            tls: self.tls.clone(),
            l4_buffer: self.l4_buffer,
            pre_tls_callback: self.pre_tls_callback.clone(),
            proxy_protocol: self.proxy_protocol,
            proxy_protocol_trusted: self.proxy_protocol_trusted.clone(),
        })
    }

    pub fn cleanup(&mut self) {
        // placeholder
    }
}

pub(crate) struct UninitializedStream {
    l4: L4Stream,
    tls: Option<Arc<Acceptor>>,
    l4_buffer: L4BufferSettings,
    pre_tls_callback: Option<PreTlsCallback>,
    proxy_protocol: bool,
    proxy_protocol_trusted: Option<Arc<dyn ProxyProtocolTrust>>,
}

impl UninitializedStream {
    pub async fn handshake(mut self) -> Result<Stream> {
        self.l4.set_buffer(self.l4_buffer);
        // must happen before TLS: the PROXY header precedes the ClientHello
        proxy_protocol::apply(
            &mut self.l4,
            self.proxy_protocol,
            self.proxy_protocol_trusted.as_ref(),
        )
        .await?;
        if let Some(tls) = self.tls {
            // Process pre-TLS data if a callback is configured (e.g., PROXY protocol)
            if let Some(ref callback) = self.pre_tls_callback {
                callback.process(&mut self.l4).await?;
            }

            let tls_stream = tls.tls_handshake(self.l4).await?;
            Ok(Box::new(tls_stream))
        } else {
            Ok(Box::new(self.l4))
        }
    }

    /// Get the peer address of the connection if available
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.l4
            .get_socket_digest()
            .and_then(|d| d.peer_addr().cloned())
    }
}

/// The struct to hold one more multiple listening endpoints
pub struct Listeners {
    stacks: Vec<TransportStackBuilder>,
    #[cfg(feature = "connection_filter")]
    connection_filter: Option<Arc<dyn ConnectionFilter>>,
    pre_tls_callback: Option<PreTlsCallback>,
}

impl Listeners {
    /// Create a new [`Listeners`] with no listening endpoints.
    pub fn new() -> Self {
        Listeners {
            stacks: vec![],
            #[cfg(feature = "connection_filter")]
            connection_filter: None,
            pre_tls_callback: None,
        }
    }
    /// Create a new [`Listeners`] with a TCP server endpoint from the given string.
    pub fn tcp(addr: &str) -> Self {
        let mut listeners = Self::new();
        listeners.add_tcp(addr);
        listeners
    }

    /// Create a new [`Listeners`] with a Unix domain socket endpoint from the given string.
    #[cfg(unix)]
    pub fn uds(addr: &str, perm: Option<Permissions>) -> Self {
        let mut listeners = Self::new();
        listeners.add_uds(addr, perm);
        listeners
    }

    /// Create a new [`Listeners`] with a TLS (TCP) endpoint with the given address string,
    /// and path to the certificate/private key pairs.
    /// This endpoint will adopt the [Mozilla Intermediate](https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29)
    /// server side TLS settings.
    pub fn tls(addr: &str, cert_path: &str, key_path: &str) -> Result<Self> {
        let mut listeners = Self::new();
        listeners.add_tls(addr, cert_path, key_path)?;
        Ok(listeners)
    }

    /// Add a TCP endpoint to `self`.
    pub fn add_tcp(&mut self, addr: &str) {
        self.add_listener(ListenerConfig::tcp(addr));
    }

    /// Add a TCP endpoint to `self`, with the given [`TcpSocketOptions`].
    pub fn add_tcp_with_settings(&mut self, addr: &str, sock_opt: TcpSocketOptions) {
        self.add_listener(ListenerConfig::tcp(addr).tcp_socket_options(sock_opt));
    }

    /// Add a Unix domain socket endpoint to `self`.
    #[cfg(unix)]
    pub fn add_uds(&mut self, addr: &str, perm: Option<Permissions>) {
        let endpoint = perm.map_or_else(
            || ListenerConfig::uds(addr),
            |perm| ListenerConfig::uds(addr).permissions(perm),
        );
        self.add_listener(endpoint);
    }

    /// Add a TLS endpoint to `self` with the [Mozilla Intermediate](https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29)
    /// server side TLS settings.
    pub fn add_tls(&mut self, addr: &str, cert_path: &str, key_path: &str) -> Result<()> {
        self.add_tls_with_settings(addr, None, TlsSettings::intermediate(cert_path, key_path)?);
        Ok(())
    }

    /// Add a TLS endpoint to `self` with the given socket and server side TLS settings.
    /// See [`TlsSettings`] and [`TcpSocketOptions`] for more details.
    pub fn add_tls_with_settings(
        &mut self,
        addr: &str,
        sock_opt: Option<TcpSocketOptions>,
        settings: TlsSettings,
    ) {
        let mut endpoint = ListenerConfig::tcp(addr).tls(settings);
        if let Some(sock_opt) = sock_opt {
            endpoint = endpoint.tcp_socket_options(sock_opt);
        }
        self.add_listener(endpoint);
    }

    /// Add the given [`ServerAddress`] to `self`.
    pub fn add_address(&mut self, addr: ServerAddress) {
        self.add_endpoint(addr, None);
    }

    /// The configured bind addresses, using the keys expected by transferred listening fds.
    pub fn addresses(&self) -> Vec<String> {
        self.stacks
            .iter()
            .map(|stack| stack.l4.as_ref().to_string())
            .collect()
    }

    /// Set a connection filter for all endpoints in this listener collection
    #[cfg(feature = "connection_filter")]
    pub fn set_connection_filter(&mut self, filter: Arc<dyn ConnectionFilter>) {
        log::debug!("Setting connection filter on Listeners");

        // Store the filter for future endpoints
        self.connection_filter = Some(filter.clone());

        // Apply to existing stacks
        for stack in &mut self.stacks {
            stack.connection_filter = Some(filter.clone());
        }
    }

    /// Add the given listener endpoint to `self`.
    pub fn add_listener(&mut self, endpoint: ListenerConfig) {
        let ListenerConfig { l4, tls, l4_buffer } = endpoint;
        self.stacks.push(TransportStackBuilder {
            l4,
            tls,
            l4_buffer,
            #[cfg(feature = "connection_filter")]
            connection_filter: self.connection_filter.clone(),
            pre_tls_callback: self.pre_tls_callback.clone(),
        });
    }

    /// Set a pre-TLS callback for all endpoints in this listener collection.
    ///
    /// The callback will be invoked after TCP accept but before the TLS handshake,
    /// allowing the application to read and process data such as PROXY protocol
    /// headers that arrive before TLS.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use pingora_core::listeners::{Listeners, PreTlsProcess};
    /// use std::sync::Arc;
    ///
    /// let callback = Arc::new(MyProxyProtocolHandler::new());
    /// let mut listeners = Listeners::new();
    /// listeners.set_pre_tls_callback(callback);
    /// listeners.add_tls("0.0.0.0:443", "cert.pem", "key.pem")?;
    /// ```
    pub fn set_pre_tls_callback(&mut self, callback: PreTlsCallback) {
        log::debug!("Setting pre-TLS callback on Listeners");

        // Store the callback for future endpoints
        self.pre_tls_callback = Some(callback.clone());

        // Apply to existing stacks
        for stack in &mut self.stacks {
            stack.pre_tls_callback = Some(callback.clone());
        }
    }

    /// Add the given [`ServerAddress`] to `self` with the given [`TlsSettings`] if provided.
    pub fn add_endpoint(&mut self, l4: ServerAddress, tls: Option<TlsSettings>) {
        self.stacks.push(TransportStackBuilder {
            l4,
            tls,
            l4_buffer: L4BufferSettings::default(),
            #[cfg(feature = "connection_filter")]
            connection_filter: self.connection_filter.clone(),
            pre_tls_callback: self.pre_tls_callback.clone(),
        })
    }

    pub(crate) async fn build(
        &mut self,
        #[cfg(unix)] upgrade_listeners: Option<ListenFds>,
    ) -> Result<Vec<TransportStack>> {
        let mut stacks = Vec::with_capacity(self.stacks.len());

        for b in self.stacks.iter_mut() {
            let new_stack = b
                .build(
                    #[cfg(unix)]
                    upgrade_listeners.clone(),
                )
                .await?;

            stacks.push(new_stack);
        }

        Ok(stacks)
    }

    pub(crate) fn cleanup(&self) {
        // placeholder
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[cfg(feature = "connection_filter")]
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    #[cfg(feature = "any_tls")]
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_listen_tcp() {
        let mut listeners = Listeners::tcp("127.0.0.1:0");
        listeners.add_tcp("127.0.0.1:0");

        let listeners = listeners
            .build(
                #[cfg(unix)]
                None,
            )
            .await
            .unwrap();

        assert_eq!(listeners.len(), 2);
        let addrs: Vec<_> = listeners
            .iter()
            .map(|s| s.l4.local_addr().unwrap())
            .collect();
        for listener in listeners {
            tokio::spawn(async move {
                // just try to accept once
                let stream = listener.accept().await.unwrap();
                stream.handshake().await.unwrap();
            });
        }

        // The listeners are already bound (port resolved during build()),
        // so the kernel accepts connections into the backlog immediately.
        // No readiness wait needed — connect will succeed as soon as the
        // OS has completed the TCP handshake.
        TcpStream::connect(addrs[0]).await.unwrap();
        TcpStream::connect(addrs[1]).await.unwrap();
    }

    #[test]
    fn test_add_listener_config_tcp_l4_buffer() {
        let mut listeners = Listeners::new();
        let tcp_options = TcpSocketOptions {
            dscp: Some(10),
            ..Default::default()
        };
        let l4_buffer = L4BufferSettings {
            read: Some(0),
            write: None,
        };

        listeners.add_listener(
            ListenerConfig::tcp("127.0.0.1:7107")
                .tcp_socket_options(tcp_options)
                .l4_buffer(l4_buffer),
        );

        assert_eq!(listeners.stacks.len(), 1);
        assert_eq!(listeners.stacks[0].l4_buffer, l4_buffer);
        assert_eq!(listeners.stacks[0].l4_buffer.read_capacity(), 0);
        assert_eq!(
            listeners.stacks[0].l4_buffer.write_capacity(),
            DEFAULT_L4_WRITE_BUFFER_SIZE
        );

        match &listeners.stacks[0].l4 {
            ServerAddress::Tcp(addr, Some(options)) => {
                assert_eq!(addr, "127.0.0.1:7107");
                assert_eq!(options.dscp, Some(10));
            }
            other => panic!("unexpected listener address: {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_add_listener_config_uds_l4_buffer() {
        let mut listeners = Listeners::new();
        let l4_buffer = L4BufferSettings::unbuffered();

        listeners.add_listener(ListenerConfig::uds("/tmp/test_builder_uds").l4_buffer(l4_buffer));

        assert_eq!(listeners.stacks.len(), 1);
        assert_eq!(listeners.stacks[0].l4_buffer, l4_buffer);
        assert_eq!(listeners.stacks[0].l4_buffer.read_capacity(), 0);
        assert_eq!(listeners.stacks[0].l4_buffer.write_capacity(), 0);

        match &listeners.stacks[0].l4 {
            ServerAddress::Uds(addr, None) => assert_eq!(addr, "/tmp/test_builder_uds"),
            other => panic!("unexpected listener address: {other:?}"),
        }
    }

    #[test]
    fn test_l4_buffer_settings_defaults_per_direction() {
        let l4_buffer = L4BufferSettings {
            read: None,
            write: Some(0),
        };

        assert_eq!(l4_buffer.read_capacity(), DEFAULT_L4_READ_BUFFER_SIZE);
        assert_eq!(l4_buffer.write_capacity(), 0);
    }

    #[tokio::test]
    #[cfg(feature = "any_tls")]
    async fn test_listen_tls() {
        use tokio::io::AsyncReadExt;

        let addr = "127.0.0.1:7103";
        let cert_path = format!("{}/tests/keys/server.crt", env!("CARGO_MANIFEST_DIR"));
        let key_path = format!("{}/tests/keys/key.pem", env!("CARGO_MANIFEST_DIR"));
        let mut listeners = Listeners::tls(addr, &cert_path, &key_path).unwrap();
        let listener = listeners
            .build(
                #[cfg(unix)]
                None,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();

        tokio::spawn(async move {
            // just try to accept once
            let stream = listener.accept().await.unwrap();
            let mut stream = stream.handshake().await.unwrap();
            let mut buf = [0; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na")
                .await
                .unwrap();
        });
        // The listener is already bound, so the kernel accepts connections
        // into the backlog immediately. No readiness wait needed.
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        let res = client.get(format!("https://{addr}")).send().await.unwrap();
        assert_eq!(res.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    #[cfg(feature = "any_tls")]
    async fn test_listen_tls_with_offload() {
        use tokio::io::AsyncReadExt;

        const REQUESTS: usize = 8;

        let cert_path = format!("{}/tests/keys/server.crt", env!("CARGO_MANIFEST_DIR"));
        let key_path = format!("{}/tests/keys/key.pem", env!("CARGO_MANIFEST_DIR"));
        let mut tls_settings = TlsSettings::intermediate(&cert_path, &key_path).unwrap();
        let conf = crate::server::configuration::ServerConf {
            downstream_tls_offload_threadpools: Some(2),
            downstream_tls_offload_thread_per_pool: Some(2),
            ..Default::default()
        };
        tls_settings.set_offload_threadpool_from_server_conf(&conf);

        let mut listeners = Listeners::new();
        listeners.add_tls_with_settings("127.0.0.1:0", None, tls_settings);
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
            let mut streams = Vec::with_capacity(REQUESTS);
            for _ in 0..REQUESTS {
                streams.push(listener.accept().await.unwrap());
            }

            let mut responses = Vec::with_capacity(REQUESTS);
            for stream in streams {
                responses.push(tokio::spawn(async move {
                    let mut stream = stream.handshake().await.unwrap();
                    let mut buf = [0; 1024];
                    let _ = stream.read(&mut buf).await.unwrap();
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na")
                        .await
                        .unwrap();
                }));
            }

            for response in responses {
                response.await.unwrap();
            }
        });

        let url = format!("https://{addr}");
        let mut requests = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            let url = url.clone();
            requests.push(tokio::spawn(async move {
                let client = reqwest::Client::builder()
                    .danger_accept_invalid_certs(true)
                    .build()
                    .unwrap();
                client.get(url).send().await.unwrap().status()
            }));
        }

        for request in requests {
            assert_eq!(request.await.unwrap(), reqwest::StatusCode::OK);
        }
    }

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

    #[cfg(feature = "connection_filter")]
    #[test]
    fn test_connection_filter_inheritance() {
        #[derive(Debug, Clone)]
        struct TestFilter {
            counter: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl ConnectionFilter for TestFilter {
            async fn should_accept(&self, _addr: Option<&std::net::SocketAddr>) -> bool {
                self.counter.fetch_add(1, Ordering::SeqCst);
                true
            }
        }

        let mut listeners = Listeners::new();

        // Add an endpoint before setting filter
        listeners.add_tcp("127.0.0.1:7104");

        // Set the connection filter
        let filter = Arc::new(TestFilter {
            counter: Arc::new(AtomicUsize::new(0)),
        });
        listeners.set_connection_filter(filter.clone());

        // Add endpoints after setting filter
        listeners.add_tcp("127.0.0.1:7105");
        #[cfg(feature = "any_tls")]
        {
            // Only test TLS if the feature is enabled
            if let Ok(tls_settings) = TlsSettings::intermediate(
                &format!("{}/tests/keys/server.crt", env!("CARGO_MANIFEST_DIR")),
                &format!("{}/tests/keys/key.pem", env!("CARGO_MANIFEST_DIR")),
            ) {
                listeners.add_tls_with_settings("127.0.0.1:7106", None, tls_settings);
            }
        }

        // Verify all stacks have the filter (only when feature is enabled)
        for stack in &listeners.stacks {
            assert!(
                stack.connection_filter.is_some(),
                "All stacks should have the connection filter set"
            );
        }
    }
}
