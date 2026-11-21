# Downstream TLS handshake offload cancellation

The BoringSSL/OpenSSL, rustls, and s2n acceptors can move a server-side TLS
handshake to dedicated current-thread runtimes when their `TlsSettings`
enable offload. Each built
acceptor owns a separate, lazily started `OffloadRuntime`; its configured
`shards * threads_per_shard` threads are not a process-global pool. This
mechanism does not limit the number of concurrent handshakes or reserve CPU.

The listener wraps `UninitializedStream::handshake()` in a 60-second timeout.
The offload wait must therefore own an abort-on-drop handle: when timeout or
caller cancellation drops the wait, the dispatched handshake task is aborted
and its socket is released. This cancels the async task; synchronous
cryptographic work already executing is not preemptible.

For upstream-main issue 02, all TLS backends use the cancellation mechanism from
official upstream commit `4487f7b2ab50f159e4a2cf4f6a6b813f61bb6e19`.
Upstream tests cover the abort-on-drop handle and client EOF after the caller
drops a pending handshake. The fork runs this client EOF check against each
selected TLS backend. OpenSSL-derived backends additionally cover a completed handshake
whose callback observes the dedicated thread and an incomplete handshake
held inside its certificate callback until the outer wait times out.
The test keeps the acceptor alive until the client observes closure; restoring
the old bare spawn makes the client-closure assertion time out. The tests use
a shorter controlled deadline rather than the listener's 60-second production
deadline. A separate test drops the owning runtime while a task is pending,
and another checks that two independent pools create and release all of their
threads.

Issue 02 verification on macOS used
`cargo test --locked -p pingora-core --lib --features <backend> offload`.
All selected tests passed for BoringSSL (13), OpenSSL (13), rustls (10), and
s2n (10). All four runs exercised listener handshakes, client EOF after
direct cancellation, and runtime teardown. The OpenSSL-derived runs also
covered timeout during the certificate callback.

Edgion owns process configuration, per-acceptor and process-total thread
budgets, and the choice of terminating listener types. Plaintext, passthrough,
upstream TLS, and conf-sync TLS are outside this contract.
