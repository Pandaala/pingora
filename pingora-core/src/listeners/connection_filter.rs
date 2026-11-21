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

//! Connection filtering trait for early connection filtering
//!
//! This module provides the [`ConnectionFilter`] trait which allows filtering
//! incoming connections at the TCP level, before the TLS handshake occurs.
//!
//! # Feature Flag
//!
//! This functionality requires the `connection_filter` feature to be enabled:
//! ```toml
//! [dependencies]
//! pingora-core = { version = "0.5", features = ["connection_filter"] }
//! ```
//!
//! When the feature is disabled, a no-op implementation is provided for API compatibility.

use async_trait::async_trait;
use std::fmt::{self, Debug};
use std::net::SocketAddr;

/// Opaque value that an accepted downstream stream owns for its whole lifetime.
///
/// A filter returns a permit from [`ConnectionFilter::admit`]; the listener
/// attaches it to the accepted l4 stream, and it is dropped exactly once when
/// that stream is dropped. An offloaded TLS handshake that still owns the
/// stream after the caller times out also retains its permit.
/// Implement `Drop` on the wrapped value to
/// observe the end of the connection, for example to release a counter slot.
pub struct ConnectionPermit {
    _value: Box<dyn Send + Sync>,
}

impl ConnectionPermit {
    /// Wrap a value that must live exactly as long as the accepted stream.
    pub fn new<T: Send + Sync + 'static>(value: T) -> Self {
        Self {
            _value: Box::new(value),
        }
    }
}

impl Debug for ConnectionPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConnectionPermit")
    }
}

/// Admission decision returned by [`ConnectionFilter::admit`].
#[derive(Debug)]
pub enum ConnectionAdmission {
    /// Accept the connection, optionally attaching a permit to the stream.
    Accept(Option<ConnectionPermit>),
    /// Drop the connection immediately.
    Reject,
}

/// A trait for filtering incoming connections at the TCP level.
///
/// Implementations of this trait can inspect the peer address of incoming
/// connections and decide whether to accept or reject them before any
/// further processing (including TLS handshake) occurs.
///
/// # Example
///
/// ```rust,no_run
/// use async_trait::async_trait;
/// use pingora_core::listeners::ConnectionFilter;
/// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
///
/// #[derive(Debug)]
/// struct BlocklistFilter {
///     blocked_ips: Vec<IpAddr>,
/// }
///
/// #[async_trait]
/// impl ConnectionFilter for BlocklistFilter {
///     async fn should_accept(&self, addr: &SocketAddr) -> bool {
///         !self.blocked_ips.contains(&addr.ip())
///     }
/// }
/// ```
///
/// # Performance Considerations
///
/// This filter is called for every incoming connection, so implementations
/// should be efficient. Consider caching or pre-computing data structures
/// for IP filtering rather than doing expensive operations per connection.
#[async_trait]
pub trait ConnectionFilter: Debug + Send + Sync {
    /// Determines whether an incoming connection should be accepted.
    ///
    /// The default [`Self::admit`] calls this method after a TCP connection is
    /// accepted but before any further processing (including TLS handshake).
    /// A filter that overrides `admit` replaces this decision for listener
    /// admission.
    ///
    /// Note that this runs before any PROXY protocol header is parsed, so on
    /// a listener with [`crate::listeners::TcpSocketOptions::proxy_protocol`]
    /// enabled `addr` is the load balancer's address, never the real client's.
    /// Filtering clients by IP has to happen above this layer.
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address of the incoming connection
    ///
    /// # Returns
    ///
    /// * `true` - Accept the connection and continue processing
    /// * `false` - Drop the connection immediately
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// async fn should_accept(&self, addr: &SocketAddr) -> bool {
    ///     // Accept only connections from private IP ranges
    ///     match addr.ip() {
    ///         IpAddr::V4(ip) => ip.is_private(),
    ///         IpAddr::V6(_) => true,
    ///     }
    /// }
    ///
    async fn should_accept(&self, _addr: Option<&SocketAddr>) -> bool {
        true
    }

    /// Admit an incoming connection, optionally returning a [`ConnectionPermit`]
    /// that the accepted stream owns until it is dropped.
    ///
    /// The listener calls this method for every accepted stream, with `None`
    /// when the peer has no inet address. The default implementation keeps the
    /// [`Self::should_accept`] decision and attaches no permit. Implementations
    /// that override `admit` replace `should_accept` for listener admission.
    ///
    /// Each accept task awaits this method before accepting its next connection.
    /// With multiple accept tasks per listening socket, other tasks can continue
    /// accepting connections while this call is pending.
    ///
    /// The returned future may be dropped before it completes (listener
    /// shutdown cancels a pending accept). Any reservation taken before an
    /// `.await` must therefore release itself on drop (RAII) rather than rely on
    /// reaching the end of the method.
    async fn admit(&self, addr: Option<&SocketAddr>) -> ConnectionAdmission {
        if self.should_accept(addr).await {
            ConnectionAdmission::Accept(None)
        } else {
            ConnectionAdmission::Reject
        }
    }
}

/// Default implementation that accepts all connections.
///
/// This filter accepts all incoming connections without any filtering.
/// It's used as the default when no custom filter is specified.
#[derive(Debug, Clone)]
pub struct AcceptAllFilter;

#[async_trait]
impl ConnectionFilter for AcceptAllFilter {
    // Uses default implementation
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[derive(Debug, Clone)]
    struct BlockListFilter {
        blocked_ips: Vec<IpAddr>,
    }

    #[async_trait]
    impl ConnectionFilter for BlockListFilter {
        async fn should_accept(&self, addr_opt: Option<&SocketAddr>) -> bool {
            addr_opt
                .map(|addr| !self.blocked_ips.contains(&addr.ip()))
                .unwrap_or(true)
        }
    }

    #[tokio::test]
    async fn test_accept_all_filter() {
        let filter = AcceptAllFilter;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        assert!(filter.should_accept(Some(&addr)).await);
    }

    #[tokio::test]
    async fn test_blocklist_filter() {
        let filter = BlockListFilter {
            blocked_ips: vec![IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))],
        };

        let blocked_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
        let allowed_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)), 8080);

        assert!(!filter.should_accept(Some(&blocked_addr)).await);
        assert!(filter.should_accept(Some(&allowed_addr)).await);
    }

    #[tokio::test]
    async fn test_default_admit_keeps_should_accept_decision_without_permit() {
        let filter = BlockListFilter {
            blocked_ips: vec![IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))],
        };
        let blocked_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
        let allowed_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)), 8080);

        assert!(matches!(
            filter.admit(Some(&blocked_addr)).await,
            ConnectionAdmission::Reject
        ));
        assert!(matches!(
            filter.admit(Some(&allowed_addr)).await,
            ConnectionAdmission::Accept(None)
        ));
        assert!(matches!(
            filter.admit(None).await,
            ConnectionAdmission::Accept(None)
        ));
    }

    #[test]
    fn test_permit_drops_wrapped_value_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        struct Release(Arc<AtomicUsize>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let released = Arc::new(AtomicUsize::new(0));
        let permit = ConnectionPermit::new(Release(released.clone()));
        assert_eq!(format!("{permit:?}"), "ConnectionPermit");
        assert_eq!(released.load(Ordering::SeqCst), 0);
        drop(permit);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}
