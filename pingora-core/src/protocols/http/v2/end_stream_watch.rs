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

//! Wire-level END_STREAM bookkeeping for h2 upstream connections.
//!
//! # Why this exists
//!
//! RFC 9113 §8.1 permits a server to send a complete response and then ask the
//! client to stop uploading with `RST_STREAM(NO_ERROR)`. Older `h2` releases
//! lost the receive-side END_STREAM state in that ordering. The supported
//! baseline, h2 0.4.19, preserves it as `Cause::ErrorAfterEndStream`, so this
//! module no longer relies on state loss as its justification.
//!
//! The public `h2` state is still not a complete response-integrity proof. It
//! does not say how many DATA bytes reached the application, cannot prove that
//! a terminal HEADERS block passed every validation Pingora needs, and cannot
//! express the fork's local-reset and GOAWAY evidence boundaries. A decoded
//! END_STREAM and a wire END_STREAM are useful but deliberately distinct
//! facts; neither alone authorizes cache admission.
//!
//! The evidence does exist on the wire: the END_STREAM flag on the response's
//! last DATA (or trailer HEADERS) frame. This module recovers it by watching
//! the bytes on their way into `h2` and recording, per stream, whether
//! END_STREAM was seen BEFORE the stream/connection was torn down -- together
//! with HOW MANY DATA payload bytes the peer put on the wire up to that point.
//! Only frame headers (and the one-byte Pad Length field of a padded DATA
//! frame) are inspected; the byte stream itself is passed through untouched.
//!
//! # What makes the record safe to trust
//!
//! The watcher sees frames in wire order, and so does `h2`. A recorded
//! END_STREAM therefore means "the peer flagged end-of-body at a frame that
//! precedes whatever tore the stream down". Since `h2` processes frames in
//! order, by the time it surfaces the teardown error it has already decoded
//! every DATA frame up to and including that one.
//!
//! Decoded is not the same as DELIVERED, though: `h2` has several paths on
//! which it decodes a DATA frame and then drops the payload instead of queueing
//! it (a `content-length` mismatch, a flow-control violation, a frame arriving
//! on a stream `h2` itself has already reset). Recording the flag alone would
//! turn those into "the peer ended its response cleanly" for a body that is
//! demonstrably short. That is why the record also carries the byte count: the
//! caller accepts it only when the bytes the WIRE carried equal the bytes it
//! actually READ, which is a direct check that nothing was dropped in between
//! and needs no knowledge of which internal path did the dropping. See
//! [`super::client::Http2Session::response_body_complete_at_stream_end`] for
//! the full argument, including why the caller may only consult this once a
//! read has actually failed.
//!
//! # Dependency on the HTTP/2 stream identifier rules
//!
//! [`FrameScanner`] caches a small, fixed number of resolved records keyed by
//! stream id so that a run of DATA frames does not have to re-lock the shared
//! map. That is only sound because RFC 9113 §5.1.1 forbids reusing a stream
//! identifier on a connection: an id names at most one stream for the lifetime
//! of that connection, so a cached `Arc<StreamRecord>` can never start
//! referring to a different stream than the one the scanner resolved it for.
//! Anything that would make ids repeat -- a per-connection id reset, a scanner
//! shared across connections -- invalidates the cache, not just its
//! performance.
//!
//! # Audited `h2` handoff
//!
//! Audited against h2 0.4.19 on 2026-08-29. This is both the declared minimum
//! and the version resolved by this checkout's `Cargo.lock` at the audit. The
//! lockfile is a resolution snapshot, not an exact dependency pin; every h2
//! upgrade must re-check these private handoff facts and run the minimum/current
//! matrix recorded in `edgion-changes/verification/test-matrix.md`:
//!
//! 1. `State::recv_reset` preserves a received END_STREAM as
//!    `Cause::ErrorAfterEndStream`. The watcher does not depend on reset
//!    erasing that state; its independent byte, terminal-HEADERS, local-reset,
//!    and GOAWAY evidence remains necessary.
//! 2. `Recv::poll_data` pops `pending_recv` before consulting stream state, so
//!    an error is surfaced only after every DATA event h2 queued has been
//!    handed to the reader. Frames decoded but rejected before queueing are
//!    caught by the wire/delivered byte-count equality.
//! 3. connection errors and `recv_go_away` change stream state without clearing
//!    `pending_recv`. Receive buffers are cleared when the receive handle is
//!    dropped or all stream references are gone, after the application can no
//!    longer read them.
//! 4. the connection task awaits one codec frame and applies `recv_frame`
//!    before polling the next, preserving the wire order observed here.
//! 5. h2 0.4.19 scales its automatic small-DATA-frame budget with the configured
//!    connection window. Versions 0.4.16 through 0.4.18 fail the fork's
//!    continuing-upload reset contract with `too_many_data_frames`; this is why
//!    the workspace minimum is 0.4.19 rather than the older reset-state fix.
//!
//! The known upstream terminal-trailer validation limitation is separate and
//! remains documented in `edgion-changes/review/upstream-limitations.md`.

use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const FRAME_HEADER_LEN: usize = 9;
const FRAME_TYPE_DATA: u8 = 0x0;
const FRAME_TYPE_HEADERS: u8 = 0x1;
const FRAME_TYPE_RST_STREAM: u8 = 0x3;
const FRAME_TYPE_GOAWAY: u8 = 0x7;
const FLAG_END_STREAM: u8 = 0x1;
const FLAG_PADDED: u8 = 0x8;
/// A GOAWAY payload is `last_stream_id` (4 octets) plus `error_code`
/// (4 octets), optionally followed by opaque debug data (RFC 9113 §6.8).
const GOAWAY_MIN_PAYLOAD_LEN: usize = 8;

/// What the wire watch recorded for ONE stream: whether the peer flagged
/// END_STREAM before anything tore that stream down, and how many DATA payload
/// bytes it vouched for on the way there.
///
/// Once `end_stream` becomes true, both fields stop changing. A scanner cache
/// may still add irrelevant bytes to a forgotten, unpublished record, but a
/// terminal event cannot publish that record after its pending-map entry is
/// gone. Publication is by the `Release` store on `end_stream`, which is the
/// only field a reader is allowed to consult first.
#[derive(Debug, Default)]
pub(crate) struct StreamRecord {
    /// Set at most once, never cleared.
    end_stream: AtomicBool,
    /// Total DATA payload bytes -- padding and the Pad Length field EXCLUDED,
    /// i.e. exactly what `h2` hands to the reader for a frame it queues --
    /// carried on the wire for this stream before `end_stream` was set.
    data_bytes: AtomicUsize,
    /// A HEADERS END_STREAM observed on the wire. This may be valid initial
    /// headers, valid trailers, or an invalid header block; only h2's decoded
    /// result can decide.
    terminal_headers: AtomicBool,
    /// Set once by the application, under the publication lock, when THIS side
    /// gives up on the stream -- before its local RST_STREAM reaches the wire.
    /// Never cleared.
    ///
    /// This lives on the record rather than being expressed by the map removal
    /// alone because the record is SHARED: the session and the scanner's
    /// bounded cache both hold `Arc` clones of it, and a removal cannot reach
    /// them. See [`EndStreamWatch::invalidate`].
    ///
    /// Only `end_stream` and `terminal_headers` are gated on it. `data_bytes`
    /// deliberately is NOT: it is readable only through [`Self::vouches_for`],
    /// which `end_stream` already blocks, so gating it would buy nothing and
    /// would put a branch on the per-frame counting path.
    invalidated: AtomicBool,
}

impl StreamRecord {
    /// Whether the peer flagged END_STREAM for this stream on the wire before
    /// anything tore it down.
    ///
    /// This says nothing about whether `h2` DELIVERED the bytes that came with
    /// it; anything deciding response completeness wants
    /// [`Self::vouches_for`] instead.
    pub fn end_stream_observed(&self) -> bool {
        self.end_stream.load(Ordering::Acquire)
    }

    /// Whether the peer flagged END_STREAM *and* the wire carried exactly
    /// `body_recv` DATA payload bytes for this stream.
    ///
    /// The equality is the whole point: `h2` can decode a DATA frame and then
    /// drop its payload, in which case the wire count exceeds what the reader
    /// received and this returns `false` -- no matter which internal path did
    /// the dropping.
    pub fn vouches_for(&self, body_recv: usize) -> bool {
        // `Acquire` pairs with the `Release` store in
        // `EndStreamWatch::publish`, which happens after every
        // `data_bytes` update for this stream and under the same lock that
        // removes the entry -- so once this load sees `true`, the count below
        // is final.
        self.end_stream.load(Ordering::Acquire)
            && self.data_bytes.load(Ordering::Relaxed) == body_recv
    }

    pub fn terminal_headers_observed(&self) -> bool {
        self.terminal_headers.load(Ordering::Acquire)
    }
}

/// Per-connection record of which streams the peer ended with END_STREAM
/// before tearing them down, and of how much body it vouched for.
///
/// A stream must register itself ([`registration`](Self::registration)) before
/// its request can be flushed; the returned record is finalized at most once
/// and is never retracted, so a reader can consult it without further
/// synchronization.
#[derive(Debug, Default)]
pub(crate) struct EndStreamWatch {
    /// The streams still in flight, the GOAWAY ceiling that bounds which of
    /// them may still be published, and the terminal poison state -- all under
    /// one lock, so registration, publication and terminalization are linearly
    /// ordered. Entries are removed as soon as their outcome is decided, which
    /// bounds the map to the streams actually open on the connection.
    state: Mutex<WatchState>,
    /// Changes whenever an application-side removal ([`Self::invalidate`]) or a
    /// connection-wide terminalization retires a live entry, so the scanner can
    /// lazily discard stale cache entries once per read batch.
    forget_generation: AtomicUsize,
}

#[derive(Debug)]
enum WatchState {
    Active(ActiveWatch),
    Poisoned,
}

impl Default for WatchState {
    fn default() -> Self {
        Self::Active(ActiveWatch::default())
    }
}

/// Everything a connection that has not been poisoned keeps, guarded by the one
/// lock that also orders registration, publication and poisoning.
///
/// The GOAWAY ceiling lives here rather than in the [`FrameScanner`] because a
/// registration arriving from an application thread has to be checked against
/// it too: the scanner's one-time prune only reaches entries that were already
/// in `pending` when the GOAWAY was read.
#[derive(Debug, Default)]
struct ActiveWatch {
    /// Streams that are still in flight, i.e. neither ended nor torn down.
    pending: HashMap<u32, Arc<StreamRecord>>,
    /// The `last_stream_id` of the lowest GOAWAY seen on this connection, once
    /// one has been seen. Streams above it can never be published, whether they
    /// were registered before the GOAWAY or after it.
    goaway_ceiling: Option<u32>,
}

impl EndStreamWatch {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Take the registration lock, so that a stream id can be allocated and
    /// registered as one atomic step.
    ///
    /// This is the ONLY safe way to register a stream. `h2`'s `send_request`
    /// allocates the stream id, queues the HEADERS frame AND notifies the
    /// connection task before it returns, so on a multi-threaded runtime the
    /// request can already be flushed -- and a fast peer's whole response
    /// observed by [`FrameScanner::scan`] -- before the caller gets to run its
    /// next statement. Registering after that returns loses the record: the
    /// scan found no entry for the id and dropped it, and the flag would stay
    /// `false` forever. Holding this guard across `send_request` closes the
    /// window instead of narrowing it: `scan` blocks on the same mutex until
    /// the insert has landed.
    ///
    /// Deadlock-free because `h2`'s send path never touches this watch: the
    /// only other lock holder is the scanner on the read side, which takes the
    /// lock, updates the map and releases it without calling back into `h2`.
    pub fn registration(&self) -> Registration<'_> {
        Registration {
            state: self.state.lock(),
        }
    }

    /// Start watching `stream_id`.
    ///
    /// Convenience for callers that do not need to serialize against the id
    /// allocation (tests). Production code must use [`Self::registration`].
    #[cfg(test)]
    pub fn register(&self, stream_id: u32) -> Arc<StreamRecord> {
        self.registration().register(stream_id)
    }

    /// Stop watching `stream_id` WITHOUT marking its record.
    ///
    /// Production code has no such site left: an application that stops
    /// watching a stream is always giving up on it, which is
    /// [`Self::invalidate`]'s job. This name survives because several tests
    /// below assert that a removal WITHOUT the mark behaves differently, and
    /// they only read as contracts while the two operations are named apart.
    /// It delegates rather than duplicating the body so the two cannot drift.
    #[cfg(test)]
    pub fn forget(&self, stream_id: u32) {
        self.invalidate(stream_id, None);
    }

    /// Irreversibly give up wire evidence for `stream_id`, because THIS side is
    /// about to reset it.
    ///
    /// MUST be called BEFORE the local RST_STREAM is handed to `h2`. From the
    /// moment that reset is queued, `h2` starts DROPPING the DATA it decodes
    /// for the stream while a peer RST_STREAM landing afterwards can still
    /// surface as a remote `NO_ERROR`, so any END_STREAM published from that
    /// point on describes a body nobody will ever receive. Calling this first
    /// is what makes the two orderings decidable: publication either happened
    /// strictly before this side gave up -- in which case the evidence predates
    /// the reset and stays sound -- or it is refused outright.
    ///
    /// `record` is the caller's own handle rather than a map lookup, and that
    /// is the whole point. Removing the map entry alone says nothing to the
    /// `Arc` clones the session and the scanner's bounded cache already hold;
    /// a publication that won the race would have left them reading
    /// `end_stream == true` with no way to retract it. Marking the
    /// shared record reaches every clone, including one whose map entry is
    /// already gone.
    ///
    /// The flag is stored under the same lock [`Self::publish`] takes, so the
    /// two are linearly ordered rather than racing.
    pub fn invalidate(&self, stream_id: u32, record: Option<&StreamRecord>) {
        let removed = {
            let mut state = self.state.lock();
            if let Some(record) = record {
                record.invalidated.store(true, Ordering::Release);
            }
            match &mut *state {
                WatchState::Active(active) => active.pending.remove(&stream_id).is_some(),
                WatchState::Poisoned => false,
            }
        };
        if removed {
            self.forget_generation.fetch_add(1, Ordering::Release);
        }
    }

    /// Permanently stop accepting or publishing evidence for this connection.
    ///
    /// The terminal state and stream map share the registration/publication
    /// lock, so a concurrent operation is ordered entirely before or after
    /// poisoning.
    fn poison(&self) {
        let newly_poisoned = Self::poison_locked(&mut self.state.lock());
        if newly_poisoned {
            self.forget_generation.fetch_add(1, Ordering::Release);
        }
    }

    /// Move `state` to [`WatchState::Poisoned`], reporting whether this call is
    /// the one that did it. The caller owns the lock and therefore also owns
    /// the `forget_generation` bump that has to follow a `true`.
    fn poison_locked(state: &mut WatchState) -> bool {
        match state {
            WatchState::Poisoned => false,
            WatchState::Active(_) => {
                *state = WatchState::Poisoned;
                true
            }
        }
    }

    /// The peer put `payload_bytes` of DATA payload on the wire for
    /// `stream_id`.
    ///
    /// `payload_bytes` is the frame's application payload only: the Pad Length
    /// field and the padding itself are excluded, because `h2` excludes them
    /// too (`frame::Data::payload()` is what ends up in `pending_recv`).
    ///
    /// This is the uncached path. A stream whose outcome is already decided has
    /// no entry left, so this path cannot inflate its count.
    fn note_data(&self, stream_id: u32, payload_bytes: usize) {
        if payload_bytes == 0 {
            return;
        }
        let state = self.state.lock();
        if let WatchState::Active(active) = &*state {
            if let Some(record) = active.pending.get(&stream_id) {
                record
                    .data_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
            }
        }
    }

    /// Resolve a live stream for the scanner's bounded DATA fast path.
    fn data_record(&self, stream_id: u32) -> Option<Arc<StreamRecord>> {
        match &*self.state.lock() {
            WatchState::Active(active) => active.pending.get(&stream_id).cloned(),
            WatchState::Poisoned => None,
        }
    }

    fn cached_streams_live(&self, stream_ids: [Option<u32>; 2]) -> [bool; 2] {
        let state = self.state.lock();
        stream_ids.map(|stream_id| {
            stream_id.is_some_and(|id| match &*state {
                WatchState::Active(active) => active.pending.contains_key(&id),
                WatchState::Poisoned => false,
            })
        })
    }

    /// Finalize `stream_id`: count `payload_bytes` from the terminal frame and
    /// publish END_STREAM, in one map critical section.
    ///
    /// Keeping the terminal frame together matters both for performance (one
    /// mutex acquisition and lookup instead of two) and publication ordering:
    /// the payload count is updated before the `Release` store makes
    /// END_STREAM observable to the application.
    ///
    /// The entry is removed as it is published, so a later teardown cannot
    /// retract the record -- and, symmetrically, a teardown seen FIRST removes
    /// the entry so that a later END_STREAM (which would be a protocol
    /// violation) cannot set it. The same removal is what makes an application
    /// `forget` that won the race win it for good.
    ///
    /// This is the ONLY place `end_stream` is ever stored. Removal alone no
    /// longer freezes `data_bytes`, so the returned [`TerminalFrameHandled`] must
    /// be handed to [`FrameScanner::drop_cache_after_publish`] before anything
    /// else touches the wire.
    ///
    /// The lock is held across the stores, not just across the removal. An
    /// application that is giving up on the stream marks the record under this
    /// same lock ([`Self::invalidate`]), and a store that escaped it could land
    /// after that mark and resurrect evidence for a stream this side has
    /// already reset.
    fn publish(&self, stream_id: u32, payload_bytes: usize) -> TerminalFrameHandled {
        let mut state = self.state.lock();
        let record = match &mut *state {
            WatchState::Active(active) => active.pending.remove(&stream_id),
            WatchState::Poisoned => None,
        };
        if let Some(record) = record {
            if !record.invalidated.load(Ordering::Acquire) {
                if payload_bytes != 0 {
                    record
                        .data_bytes
                        .fetch_add(payload_bytes, Ordering::Relaxed);
                }
                record.end_stream.store(true, Ordering::Release);
            }
        }
        drop(state);
        TerminalFrameHandled(stream_id)
    }

    /// The peer tore down `stream_id` without having flagged END_STREAM.
    fn note_stream_torn_down(&self, stream_id: u32) {
        if let WatchState::Active(active) = &mut *self.state.lock() {
            active.pending.remove(&stream_id);
        }
    }

    /// A HEADERS frame carrying END_STREAM was seen for `stream_id`.
    ///
    /// Gated on the invalidation flag for the same reason [`Self::publish`] is:
    /// a stream this side has given up on must not gain wire evidence of any
    /// kind afterwards.
    fn note_terminal_headers(&self, stream_id: u32) {
        let state = self.state.lock();
        if let WatchState::Active(active) = &*state {
            if let Some(record) = active.pending.get(&stream_id) {
                if !record.invalidated.load(Ordering::Acquire) {
                    record.terminal_headers.store(true, Ordering::Release);
                }
            }
        }
    }

    /// The peer tore down the whole connection (GOAWAY), naming
    /// `last_stream_id` as the highest stream it may have processed.
    ///
    /// Streams ABOVE that threshold lose their registration: `h2` errors them
    /// out as soon as it processes the GOAWAY and ignores every later frame for
    /// them, so an END_STREAM arriving afterwards would be a record of data
    /// that is never delivered -- a false positive.
    ///
    /// Streams at or below it keep theirs. `h2` will still deliver everything
    /// it has already queued for them (`recv_go_away` does not touch
    /// `pending_recv`, see the module docs), and the standard graceful-shutdown
    /// pattern -- `GOAWAY(NO_ERROR, 2^31-1)` first, the real last id only after
    /// the in-flight streams finish -- would otherwise clear the whole map on
    /// its first frame and send every in-flight stream back to the weaker
    /// end-of-body proofs.
    ///
    /// The threshold is RETAINED, not just applied once: a stream registered
    /// after this returns is checked against it too (see
    /// [`Registration::register`]). Pruning the map alone would leave the
    /// window RFC 9113 §6.8 closes on the peer's side wide open on ours -- a
    /// later registration above the ceiling could still publish END_STREAM for
    /// a stream `h2` errored out the moment it processed the GOAWAY.
    ///
    /// RFC 9113 §6.8 also forbids a later GOAWAY from RAISING `last_stream_id`.
    /// A peer that does it anyway is either broken or trying to re-admit a
    /// stream this connection has already written off, and there is no reading
    /// of the frame sequence under which the evidence stays trustworthy, so
    /// the whole connection is poisoned instead.
    ///
    /// Returns `false` when the GOAWAY was rejected and the connection
    /// poisoned, which the caller must treat as terminal.
    fn note_connection_torn_down(&self, last_stream_id: u32) -> bool {
        let mut state = self.state.lock();
        let active = match &mut *state {
            WatchState::Active(active) => active,
            // Nothing to apply and nothing to salvage; report the terminal
            // state rather than an accepted GOAWAY.
            WatchState::Poisoned => return false,
        };
        if active
            .goaway_ceiling
            .is_some_and(|ceiling| last_stream_id > ceiling)
        {
            let newly_poisoned = Self::poison_locked(&mut state);
            drop(state);
            if newly_poisoned {
                self.forget_generation.fetch_add(1, Ordering::Release);
            }
            return false;
        }
        active.goaway_ceiling = Some(last_stream_id);
        active.pending.retain(|id, _| *id <= last_stream_id);
        true
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        match &*self.state.lock() {
            WatchState::Active(active) => active.pending.len(),
            WatchState::Poisoned => 0,
        }
    }

    #[cfg(test)]
    fn has_pending(&self, stream_id: u32) -> bool {
        match &*self.state.lock() {
            WatchState::Active(active) => active.pending.contains_key(&stream_id),
            WatchState::Poisoned => false,
        }
    }
}

/// Evidence that [`EndStreamWatch::publish`] handled a terminal frame for a
/// stream, carrying the id whose scanner cache still has to be dropped.
///
/// Deliberately NOT "END_STREAM was published": `publish` hands this back even
/// when it lost the race to `forget`, a RST_STREAM or a GOAWAY and set nothing.
/// Dropping the cache entry is required either way -- a terminal frame that
/// lost the race must still evict a slot that would otherwise keep counting
/// into a record nobody will ever look at again.
///
/// Where publication DID happen, the eviction is what keeps the record frozen:
/// a cache entry outliving the `Release` store would let a later
/// (protocol-violating) frame move `data_bytes` after
/// [`StreamRecord::vouches_for`] may already have returned `true`. The
/// `#[must_use]` is a speed bump rather than a proof -- `let _ = ...` still
/// satisfies it -- but with publication reachable from exactly one place it is
/// enough to keep the pair from drifting apart unnoticed.
#[must_use = "handling a terminal frame must be paired with dropping the scanner cache for that stream"]
struct TerminalFrameHandled(u32);

/// Exclusive access to the registration map, held across `h2`'s stream id
/// allocation. See [`EndStreamWatch::registration`].
pub(crate) struct Registration<'a> {
    state: MutexGuard<'a, WatchState>,
}

impl Registration<'_> {
    /// Start watching `stream_id`, returning its (initially blank) record.
    ///
    /// A poisoned connection, or a stream above a GOAWAY's `last_stream_id`,
    /// still gets a record so that the caller needs no special case -- but it
    /// is never entered into the map, so nothing on the wire can ever publish
    /// it. Both are permanent: `h2` will not deliver such a stream's body, so a
    /// blank record is exactly the right answer for the rest of its life.
    pub fn register(mut self, stream_id: u32) -> Arc<StreamRecord> {
        let record = Arc::new(StreamRecord::default());
        if let WatchState::Active(active) = &mut *self.state {
            if active
                .goaway_ceiling
                .is_none_or(|ceiling| stream_id <= ceiling)
            {
                active.pending.insert(stream_id, record.clone());
            }
        }
        record
    }
}

/// Incremental HTTP/2 frame-header scanner.
///
/// The server-to-client direction of an HTTP/2 connection is a pure frame
/// stream from its first byte (only the client sends a connection preface), so
/// this can start parsing immediately. Payloads are skipped by length, which
/// makes the cost per read O(frames), not O(bytes).
#[derive(Debug, Default)]
struct FrameScanner {
    header: [u8; FRAME_HEADER_LEN],
    header_len: usize,
    payload_left: usize,
    /// Set while the payload of a GOAWAY frame is being skipped, to collect its
    /// `last_stream_id` (the first 4 payload bytes).
    goaway: Option<LastStreamId>,
    /// Set while a PADDED DATA frame's payload is being skipped, until its Pad
    /// Length field -- the first payload byte -- has been read. Only then is
    /// the frame's application payload size known.
    padded_data: Option<PaddedData>,
    /// A DATA END_STREAM whose header has been parsed but whose payload has not
    /// yet arrived in full. h2 cannot consume the frame before then, so neither
    /// may the observer publish it.
    terminal_data: Option<TerminalData>,
    /// The last two live streams resolved for non-terminal DATA frames. H2
    /// stream ids are never reused on a connection, so repeated frames for a
    /// cached id can update its record without consulting the shared map.
    ///
    /// Two is the smallest bound that avoids penalizing the basic multiplexed
    /// alternating-stream case. A concurrent application `forget` may leave
    /// an Arc alive and receive irrelevant byte increments, but every terminal
    /// event still consults the shared map before it can publish END_STREAM.
    ///
    /// Slots are claimed by liveness, not recency: a slot is released only when
    /// its stream ends, is reset, is excluded by a GOAWAY, or is forgotten.
    /// Two live but idle streams therefore pin both slots and every other
    /// stream keeps paying the borrowed map lookup for as long as they stay
    /// open -- plus four predictable branches for the slot scan. That worst
    /// case is deliberate: it is measured by the benchmark's
    /// `pinned_slots_1024_frames` pair, and evicting by recency instead would
    /// reintroduce the per-frame `Arc` clone/drop churn that the fixed bound
    /// exists to avoid.
    data_records: [Option<CachedRecord>; 2],
    forget_generation: usize,
    /// Benchmark-only A/B switch; never present in production builds.
    #[cfg(test)]
    data_cache_disabled: bool,
    /// Benchmark-only Candidate A switch; never present in production builds.
    #[cfg(test)]
    terminal_data_combining_disabled: bool,
}

#[derive(Debug)]
struct CachedRecord {
    stream_id: u32,
    record: Arc<StreamRecord>,
}

/// A PADDED DATA frame whose Pad Length field has not been seen yet.
#[derive(Debug, Clone, Copy)]
struct PaddedData {
    stream_id: u32,
    /// The frame's whole payload length, Pad Length field and padding
    /// included.
    payload_len: usize,
    /// Whether the frame also carried END_STREAM. Deferred with the rest: the
    /// byte count must land BEFORE the flag, because setting the flag evicts
    /// the entry the count is kept in.
    end_stream: bool,
}

#[derive(Debug, Clone, Copy)]
struct TerminalData {
    stream_id: u32,
    payload_bytes: usize,
}

/// The `last_stream_id` field of a GOAWAY frame, collected across however many
/// reads its payload happens to be split over.
#[derive(Debug, Default)]
struct LastStreamId {
    buf: [u8; 4],
    len: usize,
}

impl LastStreamId {
    fn feed(&mut self, bytes: &[u8]) {
        let taken = (4 - self.len).min(bytes.len());
        self.buf[self.len..self.len + taken].copy_from_slice(&bytes[..taken]);
        self.len += taken;
    }

    /// The identifier, or `None` if the frame's payload ended before the field
    /// was complete (a malformed GOAWAY, which `h2` answers with a connection
    /// error).
    fn get(&self) -> Option<u32> {
        // The high bit is reserved and ignored on receipt (RFC 9113 §4.1).
        (self.len == 4).then(|| u32::from_be_bytes(self.buf) & 0x7fff_ffff)
    }
}

impl FrameScanner {
    fn has_partial_frame(&self) -> bool {
        self.header_len != 0
            || self.payload_left != 0
            || self.goaway.is_some()
            || self.padded_data.is_some()
            || self.terminal_data.is_some()
    }

    fn poison(&mut self, watch: &EndStreamWatch) {
        watch.poison();
        self.reset_after_poison(watch);
    }

    /// Drop every piece of half-parsed wire state and every cached record, for
    /// a `watch` that is ALREADY poisoned. Nothing here can publish afterwards;
    /// the point is to stop the cache from counting into records nobody will
    /// look at again, and to leave the scanner in a defined state.
    fn reset_after_poison(&mut self, watch: &EndStreamWatch) {
        self.header_len = 0;
        self.payload_left = 0;
        self.goaway = None;
        self.padded_data = None;
        self.terminal_data = None;
        self.data_records = [None, None];
        self.forget_generation = watch.forget_generation.load(Ordering::Acquire);
    }

    fn discard_forgotten_data_records(&mut self, watch: &EndStreamWatch) {
        let generation = watch.forget_generation.load(Ordering::Acquire);
        if generation == self.forget_generation {
            return;
        }

        let stream_ids = self
            .data_records
            .each_ref()
            .map(|cached| cached.as_ref().map(|cached| cached.stream_id));
        let live = watch.cached_streams_live(stream_ids);
        for (cached, live) in self.data_records.iter_mut().zip(live) {
            if !live {
                *cached = None;
            }
        }
        self.forget_generation = generation;
    }

    /// Account for non-terminal DATA, using the bounded two-entry cache for
    /// repeated or alternating frames from recently active streams.
    fn note_data(&mut self, stream_id: u32, payload_bytes: usize, watch: &EndStreamWatch) {
        if payload_bytes == 0 {
            return;
        }

        #[cfg(test)]
        if self.data_cache_disabled {
            watch.note_data(stream_id, payload_bytes);
            return;
        }

        if let Some(cached) = self
            .data_records
            .iter()
            .flatten()
            .find(|cached| cached.stream_id == stream_id)
        {
            cached
                .record
                .data_bytes
                .fetch_add(payload_bytes, Ordering::Relaxed);
            return;
        }

        if let Some(slot) = self.data_records.iter().position(Option::is_none) {
            if let Some(record) = watch.data_record(stream_id) {
                self.data_records[slot] = Some(CachedRecord { stream_id, record });
                self.data_records[slot]
                    .as_ref()
                    .unwrap()
                    .record
                    .data_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
            }
        } else {
            // Do not churn Arcs when more streams are interleaved than the
            // fixed cache can hold. The uncached stream keeps the original
            // one-lock borrowed lookup path until a slot becomes available.
            watch.note_data(stream_id, payload_bytes);
        }
    }

    fn clear_data_state(&mut self, stream_id: u32) {
        for cached in &mut self.data_records {
            if cached
                .as_ref()
                .is_some_and(|cached| cached.stream_id == stream_id)
            {
                *cached = None;
            }
        }
    }

    /// Consume the evidence that a terminal frame was handled by dropping the
    /// cache entry for its stream. See [`TerminalFrameHandled`].
    fn drop_cache_after_publish(&mut self, published: TerminalFrameHandled) {
        self.clear_data_state(published.0);
    }

    /// Publish END_STREAM for `stream_id` -- counting `payload_bytes` from the
    /// same frame first -- and drop the scanner's cache for it.
    ///
    /// This is the only publication path. Publication still goes through the
    /// shared table, so an application `forget` or a wire teardown that won the
    /// race prevents it even when the cache is warm.
    fn publish_end_stream(&mut self, stream_id: u32, payload_bytes: usize, watch: &EndStreamWatch) {
        #[cfg(test)]
        if self.terminal_data_combining_disabled {
            // Candidate A's former two-lock sequence, kept for the benchmark's
            // A/B control only.
            watch.note_data(stream_id, payload_bytes);
            let published = watch.publish(stream_id, 0);
            self.drop_cache_after_publish(published);
            return;
        }

        let published = watch.publish(stream_id, payload_bytes);
        self.drop_cache_after_publish(published);
    }

    /// Route one DATA frame to the cached counting path or to publication.
    fn note_data_frame(
        &mut self,
        stream_id: u32,
        payload_bytes: usize,
        end_stream: bool,
        watch: &EndStreamWatch,
    ) {
        if end_stream {
            self.publish_end_stream(stream_id, payload_bytes, watch);
        } else {
            self.note_data(stream_id, payload_bytes, watch);
        }
    }

    fn scan(&mut self, mut bytes: &[u8], watch: &EndStreamWatch) {
        self.discard_forgotten_data_records(watch);
        while !bytes.is_empty() {
            if self.payload_left > 0 {
                let skip = self.payload_left.min(bytes.len());
                if let Some(goaway) = self.goaway.as_mut() {
                    goaway.feed(&bytes[..skip]);
                }
                if let Some(padded) = self.padded_data.take() {
                    // The Pad Length field is the FIRST payload byte, and this
                    // branch only runs with at least one payload byte in hand
                    // (`bytes` is non-empty and `payload_left > 0`).
                    let pad_len = usize::from(bytes[0]);
                    // A Pad Length that does not fit is a connection error in
                    // `h2` (it never delivers the frame), so saturating to zero
                    // is both safe and the conservative direction: undercounting
                    // makes the record fail the equality check, never pass it.
                    let data_len = padded.payload_len.saturating_sub(1 + pad_len);
                    if padded.end_stream {
                        self.terminal_data = Some(TerminalData {
                            stream_id: padded.stream_id,
                            payload_bytes: data_len,
                        });
                    } else {
                        self.note_data(padded.stream_id, data_len, watch);
                    }
                }
                self.payload_left -= skip;
                bytes = &bytes[skip..];
                if self.payload_left == 0 {
                    if let Some(terminal) = self.terminal_data.take() {
                        self.publish_end_stream(terminal.stream_id, terminal.payload_bytes, watch);
                    }
                    if !self.finish_goaway(watch) {
                        return;
                    }
                }
                continue;
            }

            // Fast path: the whole header is in this read, so it can be parsed
            // where it lies instead of being staged in `self.header` first.
            let header = if self.header_len == 0 && bytes.len() >= FRAME_HEADER_LEN {
                let (header, rest) = bytes.split_at(FRAME_HEADER_LEN);
                bytes = rest;
                header
            } else {
                let wanted = FRAME_HEADER_LEN - self.header_len;
                let taken = wanted.min(bytes.len());
                self.header[self.header_len..self.header_len + taken]
                    .copy_from_slice(&bytes[..taken]);
                self.header_len += taken;
                bytes = &bytes[taken..];

                if self.header_len < FRAME_HEADER_LEN {
                    // The header straddles this read; resume with the next one.
                    return;
                }
                self.header_len = 0;
                &self.header[..]
            };

            self.payload_left = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            let frame_type = header[3];
            let flags = header[4];
            // The high bit of the stream identifier is reserved and must be
            // ignored on receipt (RFC 9113 §4.1).
            let stream_id = u32::from_be_bytes([header[5] & 0x7f, header[6], header[7], header[8]]);

            match frame_type {
                FRAME_TYPE_DATA => {
                    let end_stream = flags & FLAG_END_STREAM != 0;
                    if flags & FLAG_PADDED != 0 {
                        if self.payload_left > 0 {
                            // Both the byte count and the flag have to wait for
                            // the Pad Length field in the payload.
                            self.padded_data = Some(PaddedData {
                                stream_id,
                                payload_len: self.payload_left,
                                end_stream,
                            });
                        }
                        // A PADDED DATA frame with a zero-length payload is
                        // malformed: the Pad Length octet is missing, so `h2`
                        // answers with a connection error and never delivers the
                        // frame. Its END_STREAM flag (if set) signals no valid
                        // end-of-body, so record neither the bytes nor the flag
                        // -- deliberately NOT falling through to
                        // `publish_end_stream`.
                    } else {
                        if end_stream && self.payload_left != 0 {
                            self.terminal_data = Some(TerminalData {
                                stream_id,
                                payload_bytes: self.payload_left,
                            });
                        } else {
                            self.note_data_frame(stream_id, self.payload_left, end_stream, watch);
                        }
                    }
                }
                // Do not publish END_STREAM from HEADERS here. At this wire
                // layer the HPACK block has not been decoded or validated, so
                // malformed trailers must not become evidence of a clean EOF.
                // Valid initial headers and trailers are latched through h2's
                // validated RecvStream API in the client session.
                FRAME_TYPE_HEADERS => {
                    if flags & FLAG_END_STREAM != 0 {
                        watch.note_terminal_headers(stream_id);
                    }
                }
                FRAME_TYPE_RST_STREAM => {
                    watch.note_stream_torn_down(stream_id);
                    self.clear_data_state(stream_id);
                }
                FRAME_TYPE_GOAWAY => {
                    // GOAWAY is a connection-control frame carrying a fixed
                    // eight-octet header (`last_stream_id` plus `error_code`)
                    // before any optional debug data. Either violation is a
                    // connection error in `h2`, after which it delivers
                    // nothing further -- so the frame names no trustworthy
                    // ceiling and everything already recorded on this
                    // connection has to be given up rather than trusted with a
                    // guessed threshold.
                    if stream_id != 0 || self.payload_left < GOAWAY_MIN_PAYLOAD_LEN {
                        self.poison(watch);
                        return;
                    }
                    self.goaway = Some(LastStreamId::default());
                }
                _ => {}
            }
        }
    }

    /// Dispatch the GOAWAY whose complete declared payload has just been
    /// skipped -- the earliest point at which `h2` can act on the frame, and
    /// therefore the earliest point at which its `last_stream_id` may be
    /// applied. An EOF before that poisons instead, via
    /// [`Self::has_partial_frame`].
    ///
    /// Returns `false` when the GOAWAY was rejected and the connection
    /// poisoned, meaning the caller must stop scanning.
    #[must_use]
    fn finish_goaway(&mut self, watch: &EndStreamWatch) -> bool {
        let Some(goaway) = self.goaway.take() else {
            return true;
        };
        let Some(last_stream_id) = goaway.get() else {
            // Unreachable: a declared payload shorter than eight octets was
            // already rejected at the frame header, and a payload that never
            // arrived in full poisons at EOF. Fail closed rather than fall back
            // to a guessed threshold if that ever stops holding.
            self.poison(watch);
            return false;
        };
        if !watch.note_connection_torn_down(last_stream_id) {
            // Already poisoned under the watch's lock; only the scanner's own
            // state is left to drop.
            self.reset_after_poison(watch);
            return false;
        }
        for cached in &mut self.data_records {
            if cached
                .as_ref()
                .is_some_and(|cached| cached.stream_id > last_stream_id)
            {
                *cached = None;
            }
        }
        true
    }
}

/// Wraps the upstream connection so that every byte `h2` reads is first
/// inspected by a [`FrameScanner`]. Writes and the byte stream itself are
/// untouched.
#[derive(Debug)]
pub(crate) struct EndStreamWatchStream<S> {
    inner: S,
    watch: Arc<EndStreamWatch>,
    scanner: FrameScanner,
}

impl<S> EndStreamWatchStream<S> {
    pub fn new(inner: S, watch: Arc<EndStreamWatch>) -> Self {
        EndStreamWatchStream {
            inner,
            watch,
            scanner: FrameScanner::default(),
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for EndStreamWatchStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let already_filled = buf.filled().len();
        let me = self.get_mut();
        let res = Pin::new(&mut me.inner).poll_read(cx, buf);
        match &res {
            Poll::Ready(Ok(())) => {
                let fresh = &buf.filled()[already_filled..];
                if fresh.is_empty() {
                    if me.scanner.has_partial_frame() {
                        me.scanner.poison(&me.watch);
                    }
                } else {
                    me.scanner.scan(fresh, &me.watch);
                }
            }
            // h2's read buffer does not retain bytes returned together with an
            // error, so observing them here would publish evidence h2 discards.
            Poll::Ready(Err(_)) => me.scanner.poison(&me.watch),
            Poll::Pending => {}
        }
        res
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for EndStreamWatchStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
#[path = "end_stream_watch_tests.rs"]
mod tests;
