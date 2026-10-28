# Downstream TLS handshake offload cancellation

The BoringSSL acceptor can move a server-side TLS handshake to dedicated
current-thread runtimes when its `TlsSettings` enables offload. Each built
acceptor owns a separate, lazily started `OffloadRuntime`; its configured
`shards * threads_per_shard` threads are not a process-global pool. This
mechanism does not limit the number of concurrent handshakes or reserve CPU.

The listener wraps `UninitializedStream::handshake()` in a 60-second timeout.
The offload wait must therefore own an abort-on-drop handle: when timeout or
caller cancellation drops the wait, the dispatched handshake task is aborted
and its socket is released. This cancels the async task; synchronous
cryptographic work already executing is not preemptible.

This is a scoped adoption of the BoringSSL and offload-runtime behavior from
official upstream commit `4487f7b2ab50f159e4a2cf4f6a6b813f61bb6e19`.
It is not a full synchronization with upstream `main`. The fork's focused
tests exercise the abort-on-drop primitive, a completed BoringSSL handshake
whose callback observes the dedicated thread, and an incomplete BoringSSL
handshake held inside its certificate callback until the outer wait times out.
The test keeps the acceptor alive until the client observes closure; restoring
the old bare spawn makes the client-closure assertion time out. The tests use
a shorter controlled deadline rather than the listener's 60-second production
deadline. A separate test drops the owning runtime while a task is pending,
and another checks that two independent pools create and release all of their
threads.

Edgion owns process configuration, per-acceptor and process-total thread
budgets, and the choice of terminating listener types. Plaintext, passthrough,
upstream TLS, and conf-sync TLS are outside this contract.
