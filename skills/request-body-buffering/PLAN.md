# Early Request Body Buffering (pingora seam) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, pluggable `RequestBodyBuffer` seam to pingora so a proxy app can, in `request_filter`, capture the full request body (storage policy supplied by the app) and have it (original or rewritten) replayed to upstream during forwarding.

**Architecture:** A new trait + 3-state replay enum in `pingora-core`'s `body_buffer.rs`; a parallel `early_body_buffer` field on the H1/H2 server sessions with an async capture hook in `read_body_bytes` and a `take_early_body_for_replay` accessor; an empty-body-gated, guarded replay branch in `proxy_h1.rs`/`proxy_h2.rs`. The existing `retry_buffer` path and the public `ServerSession` trait are not modified (behavior-preserving).

**Tech Stack:** Rust, pingora-core / pingora-proxy, `async_trait`, `tokio`, `bytes`, `tokio_test::io` for mock-stream unit tests.

## Global Constraints

- **English only** in all on-disk content (code, comments, commit messages).
- **No new dependency.** `async_trait` is already a workspace dep of pingora-core (`pingora-core/Cargo.toml`).
- **Behavior-preserving:** when no buffer is registered, every existing code path behaves exactly as today. The `retry_buffer` / `get_retry_buffer` / `enable_retry_buffering` / `retry_buffer_truncated` methods and the `custom/server.rs` `ServerSession` trait are NOT changed.
- **Edgion side is out of scope** for this plan: the memory-then-file impl, the `request_filter` usage, size budget / 413, `Expect: 100-continue`, total read timeout, and framing reconciliation on rewrite are all Edgion obligations (see `DESIGN.md` "Edgion usage contract"). This plan delivers only the pingora seam + an in-memory reference impl.
- Branch: `edgion`. Verify finally by building **Edgion** against this fork.
- Spec: `skills/request-body-buffering/DESIGN.md` (v4).

---

## File structure

- `pingora-core/src/protocols/http/body_buffer.rs` — add `RequestBodyBuffer` trait, `EarlyBodyReplay` enum, `InMemoryRequestBodyBuffer` reference impl. (existing `FixedBuffer` untouched)
- `pingora-core/src/protocols/http/mod.rs` — `pub use` the new public items.
- `pingora-core/src/protocols/http/v1/server.rs` — `early_body_buffer` field + init; let-else rewrite of `read_body_bytes` + capture hook; `set_request_body_buffer` / `take_early_body_for_replay`.
- `pingora-core/src/protocols/http/v2/server.rs` — same field/init/hook/accessors (no `read_body_bytes` restructure needed).
- `pingora-core/src/protocols/http/server.rs` — `HttpSession` enum: dispatch the two new methods; no-op / `NotRegistered` for `Subrequest`+`Custom`.
- `pingora-proxy/src/proxy_h1.rs` — empty-body-gated guarded replay branch.
- `pingora-proxy/src/proxy_h2.rs` — same.

---

### Task 1: `RequestBodyBuffer` trait, `EarlyBodyReplay`, in-memory reference impl

**Files:**
- Modify: `pingora-core/src/protocols/http/body_buffer.rs`
- Modify: `pingora-core/src/protocols/http/mod.rs`
- Test: inline `#[cfg(test)]` in `pingora-core/src/protocols/http/body_buffer.rs`

**Interfaces:**
- Produces:
  - `pub trait RequestBodyBuffer: Send + Sync { async fn write(&mut self, data: &Bytes) -> Result<()>; async fn get(&mut self) -> Result<Option<Bytes>>; }`
  - `pub enum EarlyBodyReplay { NotRegistered, Body(Bytes), Unavailable }`
  - `pub struct InMemoryRequestBodyBuffer` implementing the trait.

- [ ] **Step 1: Write the failing test**

Append to `pingora-core/src/protocols/http/body_buffer.rs`:

```rust
#[cfg(test)]
mod early_buffer_tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_captures_and_is_rereadable() {
        let mut b = InMemoryRequestBodyBuffer::new();
        b.write(&Bytes::from_static(b"hello ")).await.unwrap();
        b.write(&Bytes::from_static(b"world")).await.unwrap();
        // get() returns the concatenation, and is callable more than once.
        assert_eq!(b.get().await.unwrap(), Some(Bytes::from_static(b"hello world")));
        assert_eq!(b.get().await.unwrap(), Some(Bytes::from_static(b"hello world")));
    }

    #[tokio::test]
    async fn in_memory_ignores_writes_after_first_get() {
        let mut b = InMemoryRequestBodyBuffer::new();
        b.write(&Bytes::from_static(b"abc")).await.unwrap();
        let _ = b.get().await.unwrap();
        // post-get write (e.g. post-response drain) must not change the captured body.
        b.write(&Bytes::from_static(b"def")).await.unwrap();
        assert_eq!(b.get().await.unwrap(), Some(Bytes::from_static(b"abc")));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pingora-core --lib protocols::http::body_buffer::early_buffer_tests`
Expected: FAIL — `cannot find type InMemoryRequestBodyBuffer` (not defined yet).

- [ ] **Step 3: Write minimal implementation**

At the top of `pingora-core/src/protocols/http/body_buffer.rs`, ensure imports include the error type and async_trait (add what is missing):

```rust
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use pingora_error::Result;
```

Then add (after the existing `FixedBuffer` definition):

```rust
/// A pluggable buffer for the full request body, supplied by the proxy app to capture
/// the body early (in `request_filter`) and replay it to upstream during forwarding.
/// Storage policy (memory / file) and whether `get()` returns the captured original or a
/// rewritten body are entirely the impl's choice.
#[async_trait]
pub trait RequestBodyBuffer: Send + Sync {
    /// Append one captured body chunk. Called once per chunk during capture.
    async fn write(&mut self, data: &Bytes) -> Result<()>;

    /// Return the body to forward upstream (captured original OR a rewritten body).
    /// Re-readable: callable multiple times, including on retry. `None` means the impl
    /// cannot produce a body (e.g. over budget); the proxy then fails the request.
    async fn get(&mut self) -> Result<Option<Bytes>>;
}

/// Result of consulting a session's early body buffer at replay time.
pub enum EarlyBodyReplay {
    /// No buffer registered — take the native forwarding path.
    NotRegistered,
    /// Forward these bytes to upstream.
    Body(Bytes),
    /// A buffer was registered but produced no body — the proxy must fail the request.
    Unavailable,
}

/// In-memory reference implementation of [`RequestBodyBuffer`]. Production apps
/// (e.g. with file spill) provide their own impl.
pub struct InMemoryRequestBodyBuffer {
    buf: BytesMut,
    cached: Option<Bytes>,
}

impl InMemoryRequestBodyBuffer {
    pub fn new() -> Self {
        InMemoryRequestBodyBuffer { buf: BytesMut::new(), cached: None }
    }
}

impl Default for InMemoryRequestBodyBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RequestBodyBuffer for InMemoryRequestBodyBuffer {
    async fn write(&mut self, data: &Bytes) -> Result<()> {
        // Once the body has been materialized for replay, ignore further writes
        // (e.g. post-response drain).
        if self.cached.is_none() {
            self.buf.extend_from_slice(data);
        }
        Ok(())
    }

    async fn get(&mut self) -> Result<Option<Bytes>> {
        if self.cached.is_none() {
            self.cached = Some(self.buf.split().freeze());
        }
        Ok(self.cached.clone())
    }
}
```

- [ ] **Step 4: Export the public items**

In `pingora-core/src/protocols/http/mod.rs`, add next to the other `pub use`/`pub mod` lines for `body_buffer`:

```rust
pub use body_buffer::{EarlyBodyReplay, InMemoryRequestBodyBuffer, RequestBodyBuffer};
```

(If `body_buffer` is declared `mod body_buffer;`, change it to `pub mod body_buffer;`.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p pingora-core --lib protocols::http::body_buffer::early_buffer_tests`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add pingora-core/src/protocols/http/body_buffer.rs pingora-core/src/protocols/http/mod.rs
git commit -m "edgion: add RequestBodyBuffer trait, EarlyBodyReplay, in-memory impl

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: v1 server — field, capture hook, accessors

**Files:**
- Modify: `pingora-core/src/protocols/http/v1/server.rs` (field decl near `:109`; constructor `retry_buffer: None` init; `read_body_bytes` `:542-552`; new accessors near `:1120`)
- Test: inline `#[cfg(test)]` in the same file (the existing test module, near `:1915`)

**Interfaces:**
- Consumes: `RequestBodyBuffer`, `EarlyBodyReplay`, `InMemoryRequestBodyBuffer` (Task 1).
- Produces (on the v1 `HttpSession`/`SessionV1`):
  - `pub fn set_request_body_buffer(&mut self, buffer: Box<dyn RequestBodyBuffer>)`
  - `pub async fn take_early_body_for_replay(&mut self) -> Result<EarlyBodyReplay>` (does NOT unregister — keeps the buffer for retry).

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module in `pingora-core/src/protocols/http/v1/server.rs` (mirrors `read_with_body_content_length_single_read` at `:1915`):

```rust
#[tokio::test]
async fn early_body_buffer_captures_and_replays() {
    use crate::protocols::http::body_buffer::{EarlyBodyReplay, InMemoryRequestBodyBuffer};
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    http_stream.set_request_body_buffer(Box::new(InMemoryRequestBodyBuffer::new()));
    let res = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert_eq!(res, b"abc".as_slice());
    match http_stream.take_early_body_for_replay().await.unwrap() {
        EarlyBodyReplay::Body(b) => assert_eq!(b, b"abc".as_slice()),
        _ => panic!("expected captured body"),
    }
}

#[tokio::test]
async fn early_body_buffer_not_registered() {
    use crate::protocols::http::body_buffer::EarlyBodyReplay;
    init_log();
    let input1 = b"GET / HTTP/1.1\r\n";
    let input2 = b"Host: pingora.org\r\nContent-Length: 3\r\n\r\nabc";
    let mock_io = Builder::new().read(&input1[..]).read(&input2[..]).build();
    let mut http_stream = HttpSession::new(Box::new(mock_io));
    http_stream.read_request().await.unwrap();
    let _ = http_stream.read_body_bytes().await.unwrap().unwrap();
    assert!(matches!(
        http_stream.take_early_body_for_replay().await.unwrap(),
        EarlyBodyReplay::NotRegistered
    ));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pingora-core --lib protocols::http::v1::server::early_body_buffer`
Expected: FAIL — `no method named set_request_body_buffer` / `no field early_body_buffer`.

- [ ] **Step 3: Add the field**

In `pingora-core/src/protocols/http/v1/server.rs`, after the `retry_buffer` field (`:109`):

```rust
    /// Optional app-supplied buffer that captures the full request body for early
    /// inspection / rewrite and replay to upstream. Parallel to `retry_buffer`.
    early_body_buffer: Option<Box<dyn RequestBodyBuffer>>,
```

In every constructor where `retry_buffer: None,` appears (e.g. `HttpSession::new`), add alongside it:

```rust
            early_body_buffer: None,
```

Ensure the import at the top of the file includes the trait (extend the existing `body_buffer` use):

```rust
use crate::protocols::http::body_buffer::{EarlyBodyReplay, RequestBodyBuffer};
```

- [ ] **Step 4: Add the capture hook (let-else rewrite of `read_body_bytes`)**

Replace `read_body_bytes` (`:542-552`) with:

```rust
    /// Read the request body. `Ok(None)` when there is no (more) body to read.
    pub async fn read_body_bytes(&mut self) -> Result<Option<Bytes>> {
        let Some(b) = self.read_body().await? else {
            return Ok(None);
        };
        let bytes = Bytes::copy_from_slice(self.get_body(&b));
        self.body_bytes_read += bytes.len();
        if let Some(buffer) = self.retry_buffer.as_mut() {
            buffer.write_to_buffer(&bytes);
        }
        if let Some(eb) = self.early_body_buffer.as_mut() {
            eb.write(&bytes).await?;
        }
        Ok(Some(bytes))
    }
```

- [ ] **Step 5: Add the accessors**

After `get_retry_buffer` (`:1126-1134`):

```rust
    pub fn set_request_body_buffer(&mut self, buffer: Box<dyn RequestBodyBuffer>) {
        self.early_body_buffer = Some(buffer);
    }

    /// Source the body to replay upstream from the registered early buffer. Does NOT
    /// unregister the buffer, so the proxy retry loop can call this again.
    pub async fn take_early_body_for_replay(&mut self) -> Result<EarlyBodyReplay> {
        match self.early_body_buffer.as_mut() {
            None => Ok(EarlyBodyReplay::NotRegistered),
            Some(b) => match b.get().await? {
                Some(body) => Ok(EarlyBodyReplay::Body(body)),
                None => Ok(EarlyBodyReplay::Unavailable),
            },
        }
    }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p pingora-core --lib protocols::http::v1::server::early_body_buffer`
Expected: PASS (2 tests). Also run the existing body tests to confirm the let-else restructure is behavior-preserving:
Run: `cargo test -p pingora-core --lib protocols::http::v1::server::read_with_body`
Expected: PASS (unchanged).

- [ ] **Step 7: Commit**

```bash
git add pingora-core/src/protocols/http/v1/server.rs
git commit -m "edgion: v1 early body buffer field, capture hook, accessors

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: v2 server — field, capture hook, accessors

**Files:**
- Modify: `pingora-core/src/protocols/http/v2/server.rs` (field next to `retry_buffer`; constructor `retry_buffer: None` init; `read_body_bytes` `:246-263`; new accessors next to its `get_retry_buffer`)

**Interfaces:**
- Consumes: `RequestBodyBuffer`, `EarlyBodyReplay` (Task 1).
- Produces (on v2 `HttpSession`/`SessionV2`): `set_request_body_buffer`, `take_early_body_for_replay` — same signatures as Task 2.

> Note on testing: a v2 session needs a live `h2` handshake, which the unit harness does not provide; v2 behavior is verified by the proxy/Edgion integration path (Task 7 + Edgion). This task is gated on compilation and mirrors Task 2 exactly.

- [ ] **Step 1: Add the field + import**

In `pingora-core/src/protocols/http/v2/server.rs`, extend the `body_buffer` import:

```rust
use crate::protocols::http::body_buffer::{EarlyBodyReplay, RequestBodyBuffer};
```

Add the field next to `retry_buffer`:

```rust
    early_body_buffer: Option<Box<dyn RequestBodyBuffer>>,
```

Add `early_body_buffer: None,` everywhere `retry_buffer: None,` appears in v2 constructors.

- [ ] **Step 2: Add the capture hook**

In `read_body_bytes` (`:246-263`), inside the existing `if let Some(data) = data.as_ref() {` block, after the `retry_buffer` write and before/after `release_capacity`:

```rust
        if let Some(data) = data.as_ref() {
            self.body_read += data.len();
            if let Some(buffer) = self.retry_buffer.as_mut() {
                buffer.write_to_buffer(data);
            }
            if let Some(eb) = self.early_body_buffer.as_mut() {
                eb.write(data).await?;
            }
            let _ = self
                .request_body_reader
                .flow_control()
                .release_capacity(data.len());
        }
```

- [ ] **Step 3: Add the accessors**

Next to v2's `get_retry_buffer`:

```rust
    pub fn set_request_body_buffer(&mut self, buffer: Box<dyn RequestBodyBuffer>) {
        self.early_body_buffer = Some(buffer);
    }

    pub async fn take_early_body_for_replay(&mut self) -> Result<EarlyBodyReplay> {
        match self.early_body_buffer.as_mut() {
            None => Ok(EarlyBodyReplay::NotRegistered),
            Some(b) => match b.get().await? {
                Some(body) => Ok(EarlyBodyReplay::Body(body)),
                None => Ok(EarlyBodyReplay::Unavailable),
            },
        }
    }
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p pingora-core`
Expected: builds with no errors.

- [ ] **Step 5: Commit**

```bash
git add pingora-core/src/protocols/http/v2/server.rs
git commit -m "edgion: v2 early body buffer field, capture hook, accessors

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `HttpSession` enum plumbing

**Files:**
- Modify: `pingora-core/src/protocols/http/server.rs` (near `get_retry_buffer` `:665-672`)

**Interfaces:**
- Consumes: v1/v2 `set_request_body_buffer` + `take_early_body_for_replay` (Tasks 2,3); `EarlyBodyReplay`, `RequestBodyBuffer` (Task 1).
- Produces (on the unified `HttpSession` enum that the proxy `Session` derefs to): `set_request_body_buffer`, `take_early_body_for_replay`.

- [ ] **Step 1: Add the dispatch methods**

Ensure the file imports the types (extend the existing `body_buffer` use):

```rust
use crate::protocols::http::body_buffer::{EarlyBodyReplay, RequestBodyBuffer};
```

After the `get_retry_buffer` enum method (`:665-672`):

```rust
    /// Register an app-supplied request body buffer (opt-in). Supported on the H1/H2
    /// data plane only; a no-op for subrequest/custom sessions.
    pub fn set_request_body_buffer(&mut self, buffer: Box<dyn RequestBodyBuffer>) {
        match self {
            Self::H1(s) => s.set_request_body_buffer(buffer),
            Self::H2(s) => s.set_request_body_buffer(buffer),
            Self::Subrequest(_) | Self::Custom(_) => {}
        }
    }

    /// Source the upstream body from a registered early buffer (see `EarlyBodyReplay`).
    pub async fn take_early_body_for_replay(&mut self) -> Result<EarlyBodyReplay> {
        match self {
            Self::H1(s) => s.take_early_body_for_replay().await,
            Self::H2(s) => s.take_early_body_for_replay().await,
            Self::Subrequest(_) | Self::Custom(_) => Ok(EarlyBodyReplay::NotRegistered),
        }
    }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p pingora-core`
Expected: builds with no errors.

- [ ] **Step 3: Commit**

```bash
git add pingora-core/src/protocols/http/server.rs
git commit -m "edgion: HttpSession enum plumbing for early body buffer

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: proxy_h1 — empty-body-gated guarded replay branch

**Files:**
- Modify: `pingora-proxy/src/proxy_h1.rs` (`:384-400`)

**Interfaces:**
- Consumes: `Session::is_body_empty()`, `Session::take_early_body_for_replay()`, `Session::get_retry_buffer()`, `EarlyBodyReplay`; `self.send_body_to_pipe(...)`.

- [ ] **Step 1: Add the import**

At the top of `pingora-proxy/src/proxy_h1.rs`, add to the `pingora_core::protocols::http` imports:

```rust
use pingora_core::protocols::http::EarlyBodyReplay;
```

- [ ] **Step 2: Replace the pre-loop body block**

Replace lines `:384-400` (the `let buffer = session.as_ref().get_retry_buffer();` block) with:

```rust
        // Early request body buffer (Edgion seam): if a body is expected and a buffer
        // was registered in request_filter, replay its body (original or rewritten).
        // Empty bodies take the native path, which terminates the request correctly.
        let early_replay = if session.as_mut().is_body_empty() {
            EarlyBodyReplay::NotRegistered
        } else {
            session.as_mut().take_early_body_for_replay().await?
        };
        match early_replay {
            EarlyBodyReplay::Body(body) => {
                let send_permit = tx
                    .reserve()
                    .await
                    .or_err(InternalError, "reserving body pipe")?;
                self.send_body_to_pipe(
                    session,
                    Some(body),
                    downstream_state.is_done(),
                    send_permit,
                    ctx,
                )
                .await?;
            }
            // Guard: a registered buffer that yields no body fails the request rather
            // than silently sending an empty/short body to upstream.
            EarlyBodyReplay::Unavailable => {
                return Error::e_explain(
                    InternalError,
                    "early request body buffer produced no body",
                );
            }
            // Native retry-buffer path, behavior-preserving.
            EarlyBodyReplay::NotRegistered => {
                let buffer = session.as_ref().get_retry_buffer();
                // retry, send buffer if it exists or body empty
                if buffer.is_some() || session.as_mut().is_body_empty() {
                    let send_permit = tx
                        .reserve()
                        .await
                        .or_err(InternalError, "reserving body pipe")?;
                    self.send_body_to_pipe(
                        session,
                        buffer,
                        downstream_state.is_done(),
                        send_permit,
                        ctx,
                    )
                    .await?;
                }
            }
        }
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p pingora-proxy`
Expected: builds with no errors. (If `Error`/`InternalError` are not already in scope at this point they are — they are used in the original block being replaced.)

- [ ] **Step 4: Commit**

```bash
git add pingora-proxy/src/proxy_h1.rs
git commit -m "edgion: proxy_h1 early body replay branch with empty-body gate + guard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: proxy_h2 — empty-body-gated guarded replay branch

**Files:**
- Modify: `pingora-proxy/src/proxy_h2.rs` (`:388-399`)

**Interfaces:**
- Consumes: `Session::is_body_empty()`, `Session::take_early_body_for_replay()`, `Session::get_retry_buffer()`, `EarlyBodyReplay`; `self.send_body_to2(...)`.

- [ ] **Step 1: Add the import**

At the top of `pingora-proxy/src/proxy_h2.rs`, add:

```rust
use pingora_core::protocols::http::EarlyBodyReplay;
```

- [ ] **Step 2: Replace the pre-loop body block**

Replace lines `:388-399` (the `if let Some(buffer) = session.as_mut().get_retry_buffer()` block) with:

```rust
        // Early request body buffer (Edgion seam) — see proxy_h1 for rationale.
        let early_replay = if session.as_mut().is_body_empty() {
            EarlyBodyReplay::NotRegistered
        } else {
            session.as_mut().take_early_body_for_replay().await?
        };
        match early_replay {
            EarlyBodyReplay::Body(body) => {
                self.send_body_to2(
                    session,
                    Some(body),
                    downstream_state.is_done(),
                    client_body,
                    ctx,
                    write_timeout,
                )
                .await?;
            }
            EarlyBodyReplay::Unavailable => {
                return Error::e_explain(
                    InternalError,
                    "early request body buffer produced no body",
                );
            }
            EarlyBodyReplay::NotRegistered => {
                // retry, send buffer if it exists
                if let Some(buffer) = session.as_mut().get_retry_buffer() {
                    self.send_body_to2(
                        session,
                        Some(buffer),
                        downstream_state.is_done(),
                        client_body,
                        ctx,
                        write_timeout,
                    )
                    .await?;
                }
            }
        }
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p pingora-proxy`
Expected: builds with no errors. (Confirm `Error` and `InternalError` are imported in `proxy_h2.rs`; they are used elsewhere in the file. If not, add `use pingora_error::{Error, ErrorType::InternalError, OrErr};` consistent with `proxy_h1.rs`.)

- [ ] **Step 4: Commit**

```bash
git add pingora-proxy/src/proxy_h2.rs
git commit -m "edgion: proxy_h2 early body replay branch with empty-body gate + guard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Full build / lint / test gate + Edgion verification

**Files:** none (verification only)

- [ ] **Step 1: Workspace build**

Run: `cargo build`
Expected: whole pingora workspace builds.

- [ ] **Step 2: Clippy**

Run: `cargo clippy -p pingora-core -p pingora-proxy --all-targets`
Expected: no new warnings from the changed files (the new trait, fields, branches).

- [ ] **Step 3: Run the affected unit tests**

Run: `cargo test -p pingora-core --lib protocols::http`
Expected: PASS, including the new `early_buffer_tests` and `early_body_buffer_*` tests and the unchanged v1 body tests.

- [ ] **Step 4: Build Edgion against this fork**

This is the real behavioral gate (the proxy/v2 replay paths are exercised by Edgion's integration suite, per `DESIGN.md`).

Run:
```bash
cd ../Edgion && cargo check --all-targets
```
Expected: Edgion compiles against the patched fork. (Requires the `[patch.crates-io]` wiring and version alignment — fork `0.8.0` vs Edgion's requested `pingora-* = "0.8.1"`; see `DESIGN.md` "Version invariant" and fork `AGENTS.md`. If `patch ... was not used` appears, align the versions first.)

- [ ] **Step 5: Final commit (if any lint/build fixups were needed)**

```bash
git add -A
git commit -m "edgion: build/lint fixups for early request body buffering

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-review notes (coverage vs DESIGN.md v4)

- Trait + `EarlyBodyReplay` (3-state, fixes C1) + ref impl → Task 1.
- Capture hook (read-triggered, async write) v1 let-else (C-m1) → Task 2; v2 → Task 3.
- Accessors keep the buffer registered for retry (contract #6) → Tasks 2/3.
- HttpSession plumbing without touching the `ServerSession` trait → Task 4.
- Empty-body gate (C2) + `else`-wrapped native path (no double-send) + `Unavailable` guard (B1) + H1 permit (M3) → Tasks 5/6.
- Behavior-preserving native path when not registered → Tasks 5/6 (`NotRegistered` arm reproduces the original block verbatim).
- Out of scope (Edgion contract, file spill, 413, timeouts, framing-on-rewrite, trailers, `poll_read_body_bytes`, caching exclusivity) → not in this plan by design; tracked in `DESIGN.md`.
