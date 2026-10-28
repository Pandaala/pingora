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

//! Listeners

use std::io;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;

use crate::protocols::digest::{GetSocketDigest, SocketDigest};
use crate::protocols::l4::socket::SocketAddr;
use crate::protocols::l4::stream::Stream;

fn initialize_peer_addrs(digest: &SocketDigest, addr: Option<SocketAddr>) {
    digest
        .peer_addr
        .set(addr.clone())
        .expect("newly created OnceCell must be empty");
    digest
        .raw_peer_addr
        .set(addr)
        .expect("newly created OnceCell must be empty");
}

/// The type for generic listener for both TCP and Unix domain socket
#[derive(Debug)]
pub enum Listener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(UnixListener),
}

impl From<TcpListener> for Listener {
    fn from(s: TcpListener) -> Self {
        Self::Tcp(s)
    }
}

#[cfg(unix)]
impl From<UnixListener> for Listener {
    fn from(s: UnixListener) -> Self {
        Self::Unix(s)
    }
}

#[cfg(unix)]
impl AsRawFd for Listener {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        match &self {
            Self::Tcp(l) => l.as_raw_fd(),
            Self::Unix(l) => l.as_raw_fd(),
        }
    }
}

#[cfg(windows)]
impl AsRawSocket for Listener {
    fn as_raw_socket(&self) -> std::os::windows::io::RawSocket {
        match &self {
            Self::Tcp(l) => l.as_raw_socket(),
        }
    }
}

impl Listener {
    /// Return the local address this listener is bound to.
    ///
    /// For TCP listeners this is the resolved address (including the
    /// OS-assigned port when the listener was bound to port 0).
    /// Returns `None` for non-TCP listeners (e.g. Unix domain sockets).
    #[cfg(test)]
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match self {
            Self::Tcp(l) => l.local_addr().ok(),
            #[cfg(unix)]
            Self::Unix(_) => None,
        }
    }

    /// Accept a connection from the listening endpoint
    pub async fn accept(&self) -> io::Result<Stream> {
        match &self {
            Self::Tcp(l) => l.accept().await.map(|(stream, peer_addr)| {
                let mut s: Stream = stream.into();
                #[cfg(unix)]
                let digest = SocketDigest::from_raw_fd(s.as_raw_fd());
                #[cfg(windows)]
                let digest = SocketDigest::from_raw_socket(s.as_raw_socket());
                initialize_peer_addrs(&digest, Some(peer_addr.into()));
                s.set_socket_digest(digest);
                // TODO: if listening on a specific bind address, we could save
                // an extra syscall looking up the local_addr later if we can pass
                // and init it in the socket digest here
                s
            }),
            #[cfg(unix)]
            Self::Unix(l) => l.accept().await.map(|(stream, peer_addr)| {
                let mut s: Stream = stream.into();
                let digest = SocketDigest::from_raw_fd(s.as_raw_fd());
                // note: if unnamed/abstract UDS, it will be `None`
                // (see TryFrom<tokio::net::unix::SocketAddr>)
                let addr = peer_addr.try_into().ok();
                initialize_peer_addrs(&digest, addr);
                s.set_socket_digest(digest);
                s
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn tcp_accept_eagerly_preserves_raw_peer_addr() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let listener = Listener::from(listener);
        let client = TcpStream::connect(listen_addr).await.unwrap();
        let expected_peer = client.local_addr().unwrap();
        let stream = listener.accept().await.unwrap();
        let digest = stream.get_socket_digest().unwrap();

        assert!(
            digest.raw_peer_addr.get().is_some(),
            "accept must initialize raw_peer_addr before any getter can query the socket"
        );
        assert_eq!(
            digest.raw_peer_addr().unwrap().as_inet(),
            Some(&expected_peer)
        );

        drop(stream);
        drop(client);
        assert_eq!(
            digest.raw_peer_addr().unwrap().as_inet(),
            Some(&expected_peer),
            "the retained digest must not consult the closed socket"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_accept_eagerly_initializes_raw_peer_addr() {
        let path = std::env::temp_dir().join(format!(
            "pingora-raw-peer-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = Listener::from(UnixListener::bind(&path).unwrap());
        let client = tokio::net::UnixStream::connect(&path).await.unwrap();
        let stream = listener.accept().await.unwrap();
        let digest = stream.get_socket_digest().unwrap();

        assert!(digest.peer_addr.get().is_some());
        assert!(
            digest.raw_peer_addr.get().is_some(),
            "even an unnamed UDS peer must be cached as None at accept time"
        );

        drop(stream);
        drop(client);
        assert!(digest.raw_peer_addr().is_none());
        std::fs::remove_file(path).unwrap();
    }
}
