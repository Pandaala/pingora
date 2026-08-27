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

//! PROXY protocol (v1 and v2) support for inbound connections.
//!
//! See <https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt>.
//!
//! When a listener sits behind an L4 load balancer (e.g. AWS NLB with proxy
//! protocol v2 enabled), the LB prepends a header carrying the original
//! client source/destination addresses at the start of each connection,
//! before any application bytes. This module parses that header and exposes
//! the addresses so the rest of the stack sees the real client address.
//!
//! Enable per listener via [`crate::listeners::TcpSocketOptions::proxy_protocol`].
//! Without a trusted-source policy, every connection must start with a valid
//! header (like HAProxy `accept-proxy`) or the downstream handshake is rejected.
//! With [`crate::listeners::TcpSocketOptions::proxy_protocol_trusted_sources`],
//! only trusted transport peers are inspected and the policy is opportunistic:
//! a trusted peer without a recognizable PROXY header is served as a normal
//! direct connection. Network controls must therefore ensure that trusted peers
//! inject the header instead of forwarding attacker-controlled leading bytes.
//!
//! # Trust model
//!
//! **The PROXY protocol header is not authenticated.** Any peer whose header is
//! accepted can claim an arbitrary source address, which defeats every IP-based
//! decision made downstream (ACLs, rate limits, logs). In mandatory mode this
//! means the listener must be reachable *exclusively* through trusted load
//! balancers, enforced with network controls (security groups, firewall rules,
//! or a private subnet). In conditional mode, keep the trusted-source policy as
//! narrow as possible and ensure each trusted peer injects the header rather
//! than forwarding attacker-controlled leading bytes.
//!
//! Note that with the `connection_filter` feature, `should_accept()` runs at
//! accept time, before this header is parsed, so it always observes the load
//! balancer's address rather than the real client's.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr as StdSockAddr};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;

use pingora_error::{Error, ErrorType::HandshakeError, OrErr, Result};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::socket::SocketAddr;
use super::stream::Stream;
use crate::protocols::{GetSocketDigest, Peek, SocketDigest};

/// The 12-byte signature that starts every proxy protocol v2 header
const V2_SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// A proxy protocol v1 header line is at most 107 bytes including CRLF
const V1_MAX_HEADER: usize = 107;

const V2_CMD_LOCAL: u8 = 0;
const V2_CMD_PROXY: u8 = 1;

const V2_FAM_UNSPEC: u8 = 0;
const V2_FAM_INET: u8 = 1;
const V2_FAM_INET6: u8 = 2;
const V2_FAM_UNIX: u8 = 3;

const V2_PROTO_STREAM: u8 = 1;
const V2_PROTO_DGRAM: u8 = 2;

const V2_INET_ADDR_LEN: usize = 12; // 4 + 4 + 2 + 2
const V2_INET6_ADDR_LEN: usize = 36; // 16 + 16 + 2 + 2
const V2_UNIX_ADDR_LEN: usize = 216; // 108 + 108

/// The addresses carried by a PROXY protocol header.
///
/// Both fields are `None` for headers that do not carry usable inet
/// addresses: v2 `LOCAL` command (LB health checks), v2 UNSPEC/UNIX address
/// families, and v1 `UNKNOWN`. In those cases the connection's real socket
/// addresses should be used as is.
#[derive(Debug, Clone, Default)]
pub struct ProxyHeader {
    /// The original source address (the real client) if present
    pub source: Option<StdSockAddr>,
    /// The original destination address (the address the client connected to
    /// on the load balancer) if present
    pub destination: Option<StdSockAddr>,
}

/// Decides which peers may speak PROXY protocol to a listener.
///
/// A PROXY header is unauthenticated, so it may only be honoured from peers the
/// deployment already trusts — normally the load balancer. Implementations come
/// from the embedding application, since pingora deliberately carries no CIDR
/// parser; this mirrors [`crate::listeners::ConnectionFilter`].
pub trait ProxyProtocolTrust: std::fmt::Debug + Send + Sync {
    /// Whether `addr`, the transport-level peer, may send a PROXY header.
    fn is_trusted(&self, addr: &StdSockAddr) -> bool;
}

/// Whether the stream begins with something that looks like a PROXY header,
/// without consuming it.
///
/// Only ever call this for peers that passed [`ProxyProtocolTrust`]. Peeking
/// confers no trust by itself; the reason to check trust first is that this
/// waits for 12 bytes, so probing an untrusted peer would let anyone hold a
/// connection open against the handshake budget.
///
/// A stream that ends before 12 bytes arrive is reported as "no header" rather
/// than an error, and the bytes it did send stay readable.
pub(crate) async fn peek_is_proxy_header(stream: &mut Stream) -> Result<bool> {
    let mut sig = [0u8; 12];
    match stream.try_peek(&mut sig).await {
        Ok(true) => Ok(sig == V2_SIGNATURE || sig.starts_with(b"PROXY ")),
        Ok(false) => Ok(false),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e).or_err(
            HandshakeError,
            "failed to probe for a proxy protocol header",
        ),
    }
}

/// Read and parse one PROXY protocol header (v1 or v2, auto-detected) from
/// the beginning of `stream`.
///
/// On success the stream is positioned exactly at the first byte after the
/// header. On error the stream position is undefined and the connection
/// should be dropped.
///
/// Pass a buffered reader: the parser issues several small reads and scans v1
/// headers one byte at a time, which would be one syscall per byte otherwise.
/// Listener streams are already buffered by the time this runs.
pub async fn read_proxy_header<S>(stream: &mut S) -> Result<ProxyHeader>
where
    S: AsyncRead + Unpin,
{
    let mut sig = [0u8; 12];
    stream
        .read_exact(&mut sig)
        .await
        .or_err(HandshakeError, "failed to read proxy protocol header")?;
    if sig == V2_SIGNATURE {
        read_v2(stream).await
    } else if sig.starts_with(b"PROXY ") {
        read_v1(stream, &sig).await
    } else {
        Error::e_explain(HandshakeError, "invalid proxy protocol signature")
    }
}

async fn read_v2<S>(stream: &mut S) -> Result<ProxyHeader>
where
    S: AsyncRead + Unpin,
{
    let mut hdr = [0u8; 4];
    stream
        .read_exact(&mut hdr)
        .await
        .or_err(HandshakeError, "failed to read proxy protocol v2 header")?;
    let version = hdr[0] >> 4;
    let command = hdr[0] & 0x0F;
    let family = hdr[1] >> 4;
    let protocol = hdr[1] & 0x0F;
    let len = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;

    if version != 2 {
        return Error::e_explain(HandshakeError, "invalid proxy protocol v2 version");
    }
    if command != V2_CMD_PROXY && command != V2_CMD_LOCAL {
        return Error::e_explain(HandshakeError, "invalid proxy protocol v2 command");
    }

    // LOCAL (e.g. load balancer health checks): the spec says the receiver must
    // accept the connection and the family/proto nibbles are ignored, so this
    // is handled before validating them — consume the header and keep the
    // connection's own socket addresses.
    if command == V2_CMD_LOCAL {
        discard(stream, len).await?;
        return Ok(ProxyHeader::default());
    }

    // From here the command is PROXY. Both nibbles of the family byte have
    // reserved values that the spec requires receivers to drop rather than
    // interpret.
    if family > V2_FAM_UNIX {
        return Error::e_explain(HandshakeError, "invalid proxy protocol v2 address family");
    }
    if protocol > V2_PROTO_DGRAM {
        return Error::e_explain(
            HandshakeError,
            "invalid proxy protocol v2 transport protocol",
        );
    }

    // A connection whose original addresses are unknown or were not carried
    // over a stream: consume the header and keep the connection's own socket
    // addresses. Falling back rather than rejecting matches HAProxy's handling
    // of address blocks it cannot use.
    if family == V2_FAM_UNSPEC || protocol != V2_PROTO_STREAM {
        discard(stream, len).await?;
        return Ok(ProxyHeader::default());
    }

    match family {
        V2_FAM_INET => {
            if len < V2_INET_ADDR_LEN {
                return Error::e_explain(HandshakeError, "proxy protocol v2 header too short");
            }
            let mut addr = [0u8; V2_INET_ADDR_LEN];
            stream
                .read_exact(&mut addr)
                .await
                .or_err(HandshakeError, "failed to read proxy protocol v2 addresses")?;
            discard(stream, len - V2_INET_ADDR_LEN).await?;
            let src_ip = Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3]);
            let dst_ip = Ipv4Addr::new(addr[4], addr[5], addr[6], addr[7]);
            let src_port = u16::from_be_bytes([addr[8], addr[9]]);
            let dst_port = u16::from_be_bytes([addr[10], addr[11]]);
            Ok(ProxyHeader {
                source: Some(StdSockAddr::new(IpAddr::V4(src_ip), src_port)),
                destination: Some(StdSockAddr::new(IpAddr::V4(dst_ip), dst_port)),
            })
        }
        V2_FAM_INET6 => {
            if len < V2_INET6_ADDR_LEN {
                return Error::e_explain(HandshakeError, "proxy protocol v2 header too short");
            }
            let mut addr = [0u8; V2_INET6_ADDR_LEN];
            stream
                .read_exact(&mut addr)
                .await
                .or_err(HandshakeError, "failed to read proxy protocol v2 addresses")?;
            discard(stream, len - V2_INET6_ADDR_LEN).await?;
            let mut ip = [0u8; 16];
            ip.copy_from_slice(&addr[0..16]);
            let src_ip = Ipv6Addr::from(ip);
            ip.copy_from_slice(&addr[16..32]);
            let dst_ip = Ipv6Addr::from(ip);
            let src_port = u16::from_be_bytes([addr[32], addr[33]]);
            let dst_port = u16::from_be_bytes([addr[34], addr[35]]);
            Ok(ProxyHeader {
                source: Some(StdSockAddr::new(IpAddr::V6(src_ip), src_port)),
                destination: Some(StdSockAddr::new(IpAddr::V6(dst_ip), dst_port)),
            })
        }
        V2_FAM_UNIX => {
            if len < V2_UNIX_ADDR_LEN {
                return Error::e_explain(HandshakeError, "proxy protocol v2 header too short");
            }
            // UNIX addresses are not mapped into the socket digest
            discard(stream, len).await?;
            Ok(ProxyHeader::default())
        }
        _ => Error::e_explain(HandshakeError, "invalid proxy protocol v2 address family"),
    }
}

async fn read_v1<S>(stream: &mut S, first12: &[u8; 12]) -> Result<ProxyHeader>
where
    S: AsyncRead + Unpin,
{
    let mut buf = [0u8; V1_MAX_HEADER];
    buf[..12].copy_from_slice(first12);
    let mut n = 12;
    while buf[n - 1] != b'\n' {
        if n >= V1_MAX_HEADER {
            return Error::e_explain(HandshakeError, "proxy protocol v1 header too long");
        }
        stream
            .read_exact(&mut buf[n..n + 1])
            .await
            .or_err(HandshakeError, "failed to read proxy protocol v1 header")?;
        n += 1;
    }
    let line = &buf[..n];
    if !line.ends_with(b"\r\n") {
        return Error::e_explain(HandshakeError, "proxy protocol v1 header missing CRLF");
    }
    let body = &line[..n - 2];

    // For UNKNOWN the spec requires the receiver to ignore everything between
    // the protocol and the CRLF, which is not guaranteed to be valid UTF-8, so
    // this has to be decided on the raw bytes.
    if let Some(rest) = body.strip_prefix(b"PROXY UNKNOWN") {
        if rest.is_empty() || rest[0] == b' ' {
            return Ok(ProxyHeader::default());
        }
    }

    let body =
        std::str::from_utf8(body).or_err(HandshakeError, "proxy protocol v1 header not ASCII")?;
    let mut parts = body.split(' ');
    parts.next(); // "PROXY", already validated by the signature check
    let proto = parts
        .next()
        .ok_or_else(|| Error::explain(HandshakeError, "proxy protocol v1 missing protocol"))?;
    if proto != "TCP4" && proto != "TCP6" {
        return Error::e_explain(HandshakeError, "invalid proxy protocol v1 protocol");
    }

    let mut next_field = |what: &'static str| {
        parts
            .next()
            .ok_or_else(|| Error::explain(HandshakeError, what))
    };
    let src_ip: IpAddr = next_field("proxy protocol v1 missing source address")?
        .parse()
        .or_err(HandshakeError, "invalid proxy protocol v1 source address")?;
    let dst_ip: IpAddr = next_field("proxy protocol v1 missing destination address")?
        .parse()
        .or_err(
            HandshakeError,
            "invalid proxy protocol v1 destination address",
        )?;
    let src_port = parse_v1_port(next_field("proxy protocol v1 missing source port")?)?;
    let dst_port = parse_v1_port(next_field("proxy protocol v1 missing destination port")?)?;
    // the v1 grammar is exact: nothing may follow the destination port
    if parts.next().is_some() {
        return Error::e_explain(HandshakeError, "proxy protocol v1 trailing data");
    }

    let want_v4 = proto == "TCP4";
    if src_ip.is_ipv4() != want_v4 || dst_ip.is_ipv4() != want_v4 {
        return Error::e_explain(HandshakeError, "proxy protocol v1 address family mismatch");
    }
    Ok(ProxyHeader {
        source: Some(StdSockAddr::new(src_ip, src_port)),
        destination: Some(StdSockAddr::new(dst_ip, dst_port)),
    })
}

/// Parse a v1 port, which the spec restricts to plain decimal digits with no
/// sign and no leading zero (`u16::from_str` alone would accept `+80`, `080`).
fn parse_v1_port(field: &str) -> Result<u16> {
    let valid = !field.is_empty()
        && field.bytes().all(|b| b.is_ascii_digit())
        && (field.len() == 1 || !field.starts_with('0'));
    if !valid {
        return Error::e_explain(HandshakeError, "invalid proxy protocol v1 port");
    }
    field
        .parse()
        .or_err(HandshakeError, "invalid proxy protocol v1 port")
}

/// Read and drop `len` bytes (TLVs, UNIX address blocks) without allocating.
async fn discard<S>(stream: &mut S, mut len: usize) -> Result<()>
where
    S: AsyncRead + Unpin,
{
    let mut scratch = [0u8; 256];
    while len > 0 {
        let n = len.min(scratch.len());
        stream
            .read_exact(&mut scratch[..n])
            .await
            .or_err(HandshakeError, "failed to read proxy protocol header")?;
        len -= n;
    }
    Ok(())
}

/// Apply inbound PROXY protocol to a freshly accepted stream.
///
/// * `enabled == false` — returns immediately, connection untouched.
/// * `trusted == None` — mandatory mode: every connection must begin with a
///   valid header (HAProxy `accept-proxy`).
/// * `trusted == Some(_)` — source-conditional mode: only a trusted peer is
///   examined, and then only if it actually sent a header. Untrusted peers are
///   served as ordinary direct connections and their bytes are never read, so a
///   forged header from one can never be honoured.
pub(crate) async fn apply(
    stream: &mut Stream,
    enabled: bool,
    trusted: Option<&std::sync::Arc<dyn ProxyProtocolTrust>>,
) -> Result<()> {
    if !enabled {
        return Ok(());
    }
    if let Some(trust) = trusted {
        // The transport peer, before any override: this is the load balancer.
        // `peer_addr` rather than `raw_peer_addr`: nothing has overridden it yet
        // at this point, and it is populated eagerly at accept, so reading it
        // costs no `getpeername`.
        let peer = stream
            .get_socket_digest()
            .and_then(|d| d.peer_addr().cloned())
            .and_then(|a| a.as_inet().copied());
        let Some(peer) = peer else {
            // The peer cannot be identified, so no trust decision is possible.
            // Reject rather than parse-and-honor the header: honoring it would
            // adopt a forged source address from a peer that cannot pass the
            // trust check. Unreachable today because only TCP listeners carry
            // `TcpSocketOptions`, but the shape must stay fail-closed for
            // whatever binds these options next.
            return Error::e_explain(
                HandshakeError,
                "proxy protocol: peer address unavailable in conditional-trust mode",
            );
        };
        if !trust.is_trusted(&peer) {
            return Ok(());
        }
        if !peek_is_proxy_header(stream).await? {
            return Ok(());
        }
    }
    apply_proxy_protocol(stream).await
}

/// Read one PROXY protocol header from the accepted `stream` and, if it
/// carries inet addresses, replace the stream's [`SocketDigest`] with one
/// whose peer address is the original client and whose local address is the
/// original destination on the load balancer.
///
/// Everything downstream that reads addresses through the socket digest
/// (HTTP sessions, logging, ACLs) then sees the real client address.
pub(crate) async fn apply_proxy_protocol(stream: &mut Stream) -> Result<()> {
    // Capture the transport peer from the current digest before it is replaced.
    // It was filled eagerly at accept time (so this costs no `getpeername`) and
    // nothing has overridden `peer_addr` yet at this point. Priming the new
    // digest's `raw_peer_addr` with it means that field never has to lazily
    // resolve the fd later, which could hit a recycled fd after the connection
    // closes.
    let raw_peer = stream
        .get_socket_digest()
        .and_then(|d| d.peer_addr().cloned());

    let header = read_proxy_header(stream).await?;
    let (Some(src), Some(dst)) = (header.source, header.destination) else {
        // LOCAL/UNKNOWN: keep the real socket addresses
        return Ok(());
    };
    #[cfg(unix)]
    let digest = SocketDigest::from_raw_fd(stream.as_raw_fd());
    #[cfg(windows)]
    let digest = SocketDigest::from_raw_socket(stream.as_raw_socket());
    if let Some(raw_peer) = raw_peer {
        digest
            .raw_peer_addr
            .set(Some(raw_peer))
            .expect("newly created OnceCell must be empty");
    }
    digest
        .peer_addr
        .set(Some(SocketAddr::Inet(src)))
        .expect("newly created OnceCell must be empty");
    digest
        .local_addr
        .set(Some(SocketAddr::Inet(dst)))
        .expect("newly created OnceCell must be empty");
    stream.set_socket_digest(digest);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn parse(mut input: &[u8]) -> Result<ProxyHeader> {
        read_proxy_header(&mut input).await
    }

    /// Build a v2 header with full control over every field, including
    /// deliberately inconsistent ones.
    fn v2_raw(ver_cmd: u8, fam_proto: u8, len: u16, payload: &[u8]) -> Vec<u8> {
        let mut buf = V2_SIGNATURE.to_vec();
        buf.push(ver_cmd);
        buf.push(fam_proto);
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    fn v2_header(cmd: u8, fam: u8, addr: &[u8], tlv: &[u8]) -> Vec<u8> {
        let mut payload = addr.to_vec();
        payload.extend_from_slice(tlv);
        v2_raw(
            0x20 | cmd,
            fam << 4 | V2_PROTO_STREAM,
            payload.len() as u16,
            &payload,
        )
    }

    const INET_ADDR: [u8; 12] = [192, 168, 0, 1, 10, 0, 0, 2, 0xdc, 0x04, 0x01, 0xbb];

    /// A real `Stream` fed `bytes` by a loopback peer that then closes.
    ///
    /// The probe is tested against this rather than a mock reader because the
    /// peek/rewind interaction with the stream's `BufStream` is the part that
    /// can actually break.
    async fn stream_serving(bytes: &[u8]) -> Stream {
        use tokio::io::AsyncWriteExt;
        let payload = bytes.to_vec();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            s.write_all(&payload).await.unwrap();
            drop(s);
        });
        tokio::net::TcpStream::connect(addr).await.unwrap().into()
    }

    #[tokio::test]
    async fn test_peek_detects_v2_and_leaves_the_header_readable() {
        let header = v2_header(V2_CMD_PROXY, V2_FAM_INET, &INET_ADDR, &[]);
        let mut stream = stream_serving(&header).await;
        assert!(peek_is_proxy_header(&mut stream).await.unwrap());
        // The probe must not consume anything: the parser still sees a full header.
        let parsed = read_proxy_header(&mut stream).await.unwrap();
        assert_eq!(parsed.source.unwrap().to_string(), "192.168.0.1:56324");
    }

    #[tokio::test]
    async fn test_peek_detects_v1() {
        let mut stream = stream_serving(b"PROXY TCP4 1.2.3.4 5.6.7.8 1 2\r\n").await;
        assert!(peek_is_proxy_header(&mut stream).await.unwrap());
        let parsed = read_proxy_header(&mut stream).await.unwrap();
        assert_eq!(parsed.source.unwrap().to_string(), "1.2.3.4:1");
    }

    #[tokio::test]
    async fn test_peek_rejects_plain_request_without_eating_it() {
        use tokio::io::AsyncReadExt;
        let mut stream = stream_serving(b"GET / HTTP/1.1\r\n\r\n").await;
        assert!(!peek_is_proxy_header(&mut stream).await.unwrap());
        // Critical: the request must survive so the connection can still be served.
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"GET / HTTP/1.1\r\n\r\n");
    }

    #[tokio::test]
    async fn test_peek_on_short_stream_is_not_an_error() {
        use tokio::io::AsyncReadExt;
        let mut stream = stream_serving(b"hi").await;
        assert!(!peek_is_proxy_header(&mut stream).await.unwrap());
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"hi");
    }

    /// An `AsyncRead` that yields at most `chunk` bytes per poll, to exercise
    /// headers arriving split across TCP segments.
    struct ChunkedReader {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
    }

    impl tokio::io::AsyncRead for ChunkedReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let me = &mut *self;
            let n = me
                .chunk
                .min(buf.remaining())
                .min(me.data.len().saturating_sub(me.pos));
            buf.put_slice(&me.data[me.pos..me.pos + n]);
            me.pos += n;
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_v2_inet() {
        let addr = [
            192, 168, 0, 1, // src ip
            10, 0, 0, 2, // dst ip
            0xdc, 0x04, // src port 56324
            0x01, 0xbb, // dst port 443
        ];
        let mut input = v2_header(V2_CMD_PROXY, V2_FAM_INET, &addr, &[]);
        input.extend_from_slice(b"hello");
        let mut input = input.as_slice();
        let header = read_proxy_header(&mut input).await.unwrap();
        assert_eq!(header.source.unwrap().to_string(), "192.168.0.1:56324");
        assert_eq!(header.destination.unwrap().to_string(), "10.0.0.2:443");
        // remaining payload untouched
        assert_eq!(input, b"hello");
    }

    #[tokio::test]
    async fn test_v2_inet_with_tlv() {
        let addr = [127, 0, 0, 1, 127, 0, 0, 2, 0, 80, 0, 81];
        let tlv = [0xEA, 0x00, 0x02, 0x01, 0x02]; // e.g. AWS PP2 TLV
        let header = parse(&v2_header(V2_CMD_PROXY, V2_FAM_INET, &addr, &tlv))
            .await
            .unwrap();
        assert_eq!(header.source.unwrap().to_string(), "127.0.0.1:80");
    }

    #[tokio::test]
    async fn test_v2_inet6() {
        let mut addr = Vec::new();
        addr.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
        addr.extend_from_slice(&Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1).octets());
        addr.extend_from_slice(&8080u16.to_be_bytes());
        addr.extend_from_slice(&443u16.to_be_bytes());
        let header = parse(&v2_header(V2_CMD_PROXY, V2_FAM_INET6, &addr, &[]))
            .await
            .unwrap();
        assert_eq!(header.source.unwrap().to_string(), "[::1]:8080");
        assert_eq!(header.destination.unwrap().to_string(), "[fd00::1]:443");
    }

    #[tokio::test]
    async fn test_v2_local() {
        let mut input = v2_header(V2_CMD_LOCAL, V2_FAM_UNSPEC, &[], &[1, 2, 3]);
        input.extend_from_slice(b"ping");
        let mut input = input.as_slice();
        let header = read_proxy_header(&mut input).await.unwrap();
        assert!(header.source.is_none());
        assert_eq!(input, b"ping");
    }

    /// The spec says the receiver must accept a LOCAL connection and ignore the
    /// address family, so the reserved-nibble check must not run before the
    /// LOCAL branch. A load balancer health check that sets a reserved family
    /// (or a non-stream protocol) must still be accepted, its address block
    /// consumed, and the connection's own socket addresses kept.
    #[tokio::test]
    async fn test_v2_local_ignores_reserved_family_and_protocol() {
        // 0x20 | LOCAL, with a reserved family nibble (4) and DGRAM protocol,
        // plus a 3-byte address block that must be discarded.
        let mut input = v2_raw(0x20 | V2_CMD_LOCAL, 4 << 4 | V2_PROTO_DGRAM, 3, &[7, 8, 9]);
        input.extend_from_slice(b"ping");
        let mut input = input.as_slice();
        let header = read_proxy_header(&mut input).await.unwrap();
        assert!(header.source.is_none(), "LOCAL carries no source address");
        assert!(header.destination.is_none());
        assert_eq!(input, b"ping", "the address block must be consumed");

        // The same reserved nibbles under the PROXY command stay rejected.
        assert!(
            parse(&v2_raw(0x21, 4 << 4 | V2_PROTO_STREAM, 0, &[]))
                .await
                .is_err(),
            "PROXY with a reserved family must still be rejected"
        );
    }

    #[tokio::test]
    async fn test_v2_truncated() {
        let addr = [127, 0, 0, 1, 127, 0, 0, 2, 0, 80]; // 2 bytes short
        assert!(parse(&v2_header(V2_CMD_PROXY, V2_FAM_INET, &addr, &[]))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_bad_signature() {
        assert!(parse(b"GET / HTTP/1.1\r\n\r\n").await.is_err());
    }

    #[tokio::test]
    async fn test_v1_tcp4() {
        let mut input: &[u8] = b"PROXY TCP4 192.168.0.1 10.0.0.2 56324 443\r\nhello";
        let header = read_proxy_header(&mut input).await.unwrap();
        assert_eq!(header.source.unwrap().to_string(), "192.168.0.1:56324");
        assert_eq!(header.destination.unwrap().to_string(), "10.0.0.2:443");
        assert_eq!(input, b"hello");
    }

    #[tokio::test]
    async fn test_v1_tcp6() {
        let header = parse(b"PROXY TCP6 ::1 fd00::1 8080 443\r\n").await.unwrap();
        assert_eq!(header.source.unwrap().to_string(), "[::1]:8080");
    }

    #[tokio::test]
    async fn test_v1_unknown() {
        let header = parse(b"PROXY UNKNOWN\r\n").await.unwrap();
        assert!(header.source.is_none());
    }

    #[tokio::test]
    async fn test_v2_dgram_falls_back_to_socket_addresses() {
        // DGRAM over a stream listener: consume the header, do not trust it
        let header = parse(&v2_raw(0x21, 1 << 4 | V2_PROTO_DGRAM, 12, &INET_ADDR))
            .await
            .unwrap();
        assert!(header.source.is_none());
        assert!(header.destination.is_none());
    }

    #[tokio::test]
    async fn test_v2_reserved_fields_rejected() {
        // undefined transport protocol
        assert!(parse(&v2_raw(0x21, 1 << 4 | 0x03, 12, &INET_ADDR))
            .await
            .is_err());
        // undefined address family
        assert!(parse(&v2_raw(0x21, 7 << 4 | 0x01, 12, &INET_ADDR))
            .await
            .is_err());
        // wrong version nibble
        assert!(parse(&v2_raw(0x31, 0x11, 12, &INET_ADDR)).await.is_err());
        // undefined command
        assert!(parse(&v2_raw(0x23, 0x11, 12, &INET_ADDR)).await.is_err());
    }

    #[tokio::test]
    async fn test_v2_unix_family() {
        // a well formed UNIX block is consumed, addresses are not mapped
        let header = parse(&v2_raw(0x21, 3 << 4 | 0x01, 216, &[0u8; 216]))
            .await
            .unwrap();
        assert!(header.source.is_none());
        // a short one is malformed
        assert!(parse(&v2_raw(0x21, 3 << 4 | 0x01, 100, &[0u8; 100]))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_v2_short_and_truncated() {
        // INET6 announced length below the address block
        assert!(parse(&v2_raw(0x21, 0x21, 20, &[0u8; 20])).await.is_err());
        // announced length exceeds the bytes actually sent
        assert!(parse(&v2_raw(0x21, 0x11, 12, &[0u8; 8])).await.is_err());
        // EOF right after the signature
        assert!(parse(&V2_SIGNATURE).await.is_err());
        // TLV announced but not sent
        assert!(parse(&v2_raw(0x21, 0x11, 20, &INET_ADDR)).await.is_err());
    }

    #[tokio::test]
    async fn test_header_split_across_reads() {
        // one byte per poll: read_exact must reassemble both versions
        let mut input = v2_header(V2_CMD_PROXY, V2_FAM_INET, &INET_ADDR, &[]);
        input.extend_from_slice(b"hello");
        let mut reader = ChunkedReader {
            data: input,
            pos: 0,
            chunk: 1,
        };
        let header = read_proxy_header(&mut reader).await.unwrap();
        assert_eq!(header.source.unwrap().to_string(), "192.168.0.1:56324");

        let mut reader = ChunkedReader {
            data: b"PROXY TCP4 192.168.0.1 10.0.0.2 56324 443\r\nhello".to_vec(),
            pos: 0,
            chunk: 3,
        };
        let header = read_proxy_header(&mut reader).await.unwrap();
        assert_eq!(header.source.unwrap().to_string(), "192.168.0.1:56324");
    }

    #[tokio::test]
    async fn test_v1_length_boundary() {
        // a line of exactly the 107 byte maximum (including CRLF) is legal
        let mut input = b"PROXY UNKNOWN ".to_vec();
        input.resize(105, b'x');
        input.extend_from_slice(b"\r\n");
        assert_eq!(input.len(), V1_MAX_HEADER);
        assert!(parse(&input).await.is_ok());

        // one byte over is not
        let mut input = b"PROXY UNKNOWN ".to_vec();
        input.resize(106, b'x');
        input.extend_from_slice(b"\r\n");
        assert!(parse(&input).await.is_err());
    }

    #[tokio::test]
    async fn test_v1_unknown_ignores_trailing_bytes() {
        // the spec requires anything between UNKNOWN and the CRLF to be
        // ignored, and does not require it to be valid UTF-8
        let mut input = b"PROXY UNKNOWN ".to_vec();
        input.extend_from_slice(&[0xFF, 0xFE, 0x80]);
        input.extend_from_slice(b"\r\nhello");
        let mut input = input.as_slice();
        let header = read_proxy_header(&mut input).await.unwrap();
        assert!(header.source.is_none());
        assert_eq!(input, b"hello");

        // but UNKNOWN must be a whole field
        assert!(parse(b"PROXY UNKNOWNXX\r\n").await.is_err());
    }

    #[tokio::test]
    async fn test_v1_strict_grammar() {
        // nothing may follow the destination port
        assert!(parse(b"PROXY TCP4 192.168.0.1 10.0.0.2 80 443 junk\r\n")
            .await
            .is_err());
        // ports carry no sign and no leading zero
        assert!(parse(b"PROXY TCP4 192.168.0.1 10.0.0.2 +80 443\r\n")
            .await
            .is_err());
        assert!(parse(b"PROXY TCP4 192.168.0.1 10.0.0.2 00080 443\r\n")
            .await
            .is_err());
        assert!(parse(b"PROXY TCP4 192.168.0.1 10.0.0.2 80 65536\r\n")
            .await
            .is_err());
        // but a bare zero port is still a number
        assert!(parse(b"PROXY TCP4 192.168.0.1 10.0.0.2 0 443\r\n")
            .await
            .is_ok());
        // missing fields
        assert!(parse(b"PROXY TCP4 192.168.0.1 10.0.0.2 80\r\n")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_v1_malformed() {
        assert!(parse(b"PROXY TCP4 not-an-ip 10.0.0.2 1 2\r\n")
            .await
            .is_err());
        assert!(parse(b"PROXY TCP4 ::1 fd00::1 1 2\r\n").await.is_err());
        // missing CR before LF
        assert!(parse(b"PROXY UNKNOWN\n\n\n\n\n\n\n\n\n\n\n").await.is_err());
        // no LF within the size limit
        let long = [b'a'; 200];
        let mut input = b"PROXY ".to_vec();
        input.extend_from_slice(&long);
        assert!(parse(&input).await.is_err());
    }
}
