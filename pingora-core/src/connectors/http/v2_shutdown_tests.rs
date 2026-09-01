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

/// A connection marked for shutdown must not allocate another stream, even
/// though the wire is perfectly healthy.
///
/// This is the case `test_spawn_stream_goaway_no_error_returns_none` and
/// `test_spawn_stream_broken_pipe_marks_shutdown` do NOT cover: there the
/// peer had already torn the connection down, so `new_stream()` failed on
/// its own and the shutdown flag was a consequence, not the cause. Here the
/// h2 connection is alive and `new_stream()` would happily succeed; only the
/// explicit `mark_shutdown()` (as raised by an upstream read timeout in
/// `pingora-proxy`) stands between the caller and a new stream.
///
/// Pinning, measured by mutation: `spawn_stream` checks the flag twice (once
/// before `new_stream()`, once after the await), and the two are redundant
/// for this test -- deleting either one alone leaves it green, deleting both
/// fails it. That redundancy is not an accident of the test: the second
/// check cannot fire at all as long as `Stub::new_stream` awaits a fresh
/// clone of `SendRequest` (see the comment there). It is defense in depth
/// against that changing, not a case this test declines to stage.
#[tokio::test]
async fn test_spawn_stream_rejects_shutdown_connection() {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = h2::client::handshake(client_io).await.unwrap();
    let (_closed_tx, closed_rx) = watch::channel(false);
    let ping_timeout = Arc::new(AtomicBool::new(false));
    let conn = ConnectionRef::new(send_req, closed_rx, ping_timeout, 0, 10, Digest::default());

    // Keep the h2 client connection task alive for the whole test so the
    // connection stays healthy and never reports closed.
    let conn_handle = tokio::spawn(connection);
    let server_handle = tokio::spawn(async move {
        let mut server_conn = h2::server::handshake(server_io).await.unwrap();
        while server_conn.accept().await.is_some() {}
    });

    // Sanity: on the healthy connection a stream IS handed out.
    let stream = conn
        .spawn_stream()
        .await
        .expect("healthy connection must allocate")
        .expect("healthy connection must allocate");
    assert!(!conn.is_idle());
    drop(stream);
    assert!(
        conn.is_idle(),
        "dropping the session must release the stream"
    );

    conn.mark_shutdown();

    // Same connection, same health, but now off limits.
    let result = conn
        .spawn_stream()
        .await
        .expect("a shutdown connection is not an error, just no stream");
    assert!(
        result.is_none(),
        "spawn_stream must refuse a connection marked for shutdown"
    );
    // The refused attempt must not leak the reservation it took while
    // checking, otherwise the connection would look busy forever.
    assert!(
        conn.is_idle(),
        "a refused spawn must not leave the stream counter incremented"
    );

    drop(conn);
    conn_handle.abort();
    server_handle.abort();
}

/// Pool selection must not hand out a connection that was marked for
/// shutdown after it was pooled.
///
/// The race this pins: a connection sits in the in-use pool with free stream
/// capacity, one of its streams hits an upstream read timeout and calls
/// `mark_shutdown()`, and nothing removes it from the pool until that stream
/// is finally released. A request arriving inside that window used to be
/// handed the abandoned connection. It is made deterministic here by driving
/// the pool directly instead of racing two requests.
///
/// Pinning, measured by mutation: the selection filter and `spawn_stream`'s
/// own checks are deliberately redundant, so no single deletion can be
/// isolated by this test -- removing the selection filter alone leaves it
/// green (`spawn_stream` refuses), and removing both of `spawn_stream`'s
/// checks alone leaves it green (the filter refuses). What it does pin is
/// the end-to-end guarantee the issue asks for -- a pooled connection
/// marked for shutdown is never handed out by the pool and never allocated
/// a stream -- and it fails once every layer is gone. (Allocation is the
/// boundary; a mark landing after `spawn_stream` returned is out of scope
/// by design, see H2-008's decision file.)
#[cfg(unix)]
#[tokio::test]
async fn test_reused_session_skips_shutdown_pooled_connection() {
    use crate::protocols::l4::socket::SocketAddr;
    use crate::upstreams::peer::Peer;
    use std::fmt::{Display, Formatter, Result as FmtResult};
    use std::os::unix::prelude::AsRawFd;

    #[derive(Clone)]
    struct PoolPeer(SocketAddr);

    impl Display for PoolPeer {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "{:?}", self.0)
        }
    }

    impl Peer for PoolPeer {
        fn address(&self) -> &SocketAddr {
            &self.0
        }

        fn tls(&self) -> bool {
            false
        }

        fn sni(&self) -> &str {
            ""
        }

        fn reuse_hash(&self) -> u64 {
            1234
        }

        fn matches_fd<V: AsRawFd>(&self, _fd: V) -> bool {
            true
        }
    }

    let peer = PoolPeer(SocketAddr::Inet("127.0.0.1:80".parse().unwrap()));
    let connector = Connector::new(None);

    let (client_io, server_io) = tokio::io::duplex(65536);
    let (send_req, connection) = h2::client::handshake(client_io).await.unwrap();
    let (_closed_tx, closed_rx) = watch::channel(false);
    let ping_timeout = Arc::new(AtomicBool::new(false));
    let conn = ConnectionRef::new(send_req, closed_rx, ping_timeout, 0, 10, Digest::default());

    let conn_handle = tokio::spawn(connection);
    let server_handle = tokio::spawn(async move {
        let mut server_conn = h2::server::handshake(server_io).await.unwrap();
        while server_conn.accept().await.is_some() {}
    });

    // The connection is pooled and healthy: reuse finds it.
    connector
        .in_use_pool
        .insert(peer.reuse_hash(), conn.clone());
    let reused = connector
        .reused_http_session(&peer)
        .await
        .expect("a healthy pooled connection is reusable");
    assert!(reused.is_some(), "healthy pooled connection must be reused");
    drop(reused);

    // That reuse put the connection back in the pool by itself: it still has
    // free stream capacity, so `more_streams_allowed()` re-inserted it.
    // Assert that before going on -- `more_streams_allowed()` also consults
    // the peer's advertised limit, so an unpooled connection here would make
    // every assertion below pass while testing nothing. `get()` pops, so put
    // it straight back.
    let pooled = connector
        .in_use_pool
        .get(peer.reuse_hash())
        .expect("a reused connection with free capacity is returned to the pool");
    connector.in_use_pool.insert(peer.reuse_hash(), pooled);

    // Now mark it for shutdown while it is sitting in the pool -- which is
    // exactly what a read timeout on a sibling stream does.
    conn.mark_shutdown();

    let reused = connector
        .reused_http_session(&peer)
        .await
        .expect("a shutdown pooled connection is not an error");
    assert!(
        reused.is_none(),
        "reuse must skip a connection marked for shutdown"
    );
    // No stream was opened on it, so it is still idle -- and it is gone from
    // the pool, so the next caller cannot find it either.
    assert!(conn.is_idle(), "no stream may be opened on it");
    assert!(
        connector.in_use_pool.get(peer.reuse_hash()).is_none(),
        "the refused connection must have been evicted from the pool"
    );

    drop(conn);
    conn_handle.abort();
    server_handle.abort();
}
