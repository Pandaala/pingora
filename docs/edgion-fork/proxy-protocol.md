# Inbound PROXY protocol

## Listener policy

`TcpSocketOptions::proxy_protocol` enables v1/v2 auto-detection. The parsed
source and destination replace the socket peer/local addresses exposed through
the connection digest.

`proxy_protocol_trusted_sources` changes mandatory mode into conditional mode:

- trusted peers must provide a valid PROXY header;
- untrusted peers are served directly and their bytes are not parsed as PROXY;
- a broad trusted range lets every member claim an arbitrary source address,
  so exact load-balancer addresses are preferred;
- IPv4-mapped IPv6 addresses must be normalized by trust implementations on
  dual-stack listeners.

The header is unauthenticated. A trusted load balancer must be configured to
inject its own header; merely relaying client bytes lets a client forge one.

## Ordering

Accepted streams receive configured L4 buffer sizes first. The built-in PROXY
parser then runs before TLS because the PROXY header precedes ClientHello. The
main branch's generic pre-TLS callback remains available and runs afterwards.

## Parser rules

- v1 grammar and maximum length are strict.
- v2 validates signature, version, command, family/protocol and declared
  length; LOCAL and unsupported transport shapes fall back safely.
- Peek and rewind preserve bytes for direct connections and split reads.
- Short or malformed mandatory headers fail the downstream handshake.

## Implementation and tests

- `pingora-core/src/protocols/l4/proxy_protocol.rs`: parser and trust trait.
- `pingora-core/src/protocols/l4/stream.rs`: non-destructive peek/rewind.
- `pingora-core/src/listeners/l4.rs` and `listeners/mod.rs`: configuration and
  pre-TLS application.
- Parser unit tests and listener loopback tests cover v1, v2, trust and direct
  traffic.
