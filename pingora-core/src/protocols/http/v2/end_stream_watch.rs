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
//! `h2` keeps one state machine per stream covering BOTH halves. When a
//! RST_STREAM is received the whole state is overwritten with
//! `Closed(Cause::Error(..))` unless the stream was already `Closed`
//! (`h2/src/proto/streams/state.rs`, `State::recv_reset`). A client that is
//! still uploading its request body sits in `HalfClosedRemote(Streaming)` once
//! the response ends, which is NOT `Closed`, so the peer's RST_STREAM erases
//! the record that END_STREAM was ever received. After that,
//! `RecvStream::is_end_stream()` reports `false` and the h2 public API offers
//! nothing else that differs:
//!
//! | observable after the reset      | complete response | truncated response |
//! |---------------------------------|-------------------|--------------------|
//! | `is_end_stream()`               | false             | false              |
//! | `flow_control().used_capacity()`| n                 | n                  |
//! | `data()`                        | `Err(NO_ERROR)`   | `Err(NO_ERROR)`    |
//! | `trailers()`                    | `Err(NO_ERROR)`   | `Err(NO_ERROR)`    |
//!
//! That is exactly RFC 9113 §8.1's shape -- "a server MAY request that the
//! client abort transmission of a request without error by sending a
//! RST_STREAM with an error code of NO_ERROR after sending a complete
//! response" -- and by construction the client IS still uploading, which is the
//! only reason the server sends it. So the case cannot be recovered from
//! `h2`'s API at all.
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
//! # Dependency on `h2` internals
//!
//! VERIFIED AGAINST h2 0.4.15 (the version this workspace pins in
//! `Cargo.lock`). The argument above is not a consequence of `h2`'s public API;
//! it rests on these internal facts, which an upgrade must re-check rather than
//! assume:
//!
//! 1. `proto::streams::state::State::recv_reset` overwrites any non-`Closed`
//!    state with `Closed(Cause::Error(..))`, which is what destroys the
//!    END_STREAM evidence in the first place (and, with `is_pending_send`, can
//!    overwrite a `Closed(Cause::Error(<local>))` too -- see
//!    `Http2Session::note_local_reset`).
//! 2. `proto::streams::recv::Recv::poll_data` only reaches its error path once
//!    `pending_recv` is empty, so an error means every DATA frame `h2` QUEUED
//!    has already been handed to the reader. (Frames it decoded but dropped are
//!    caught by the byte count instead, see above.)
//! 3. `Recv::recv_go_away` (via `Recv::handle_error`) does NOT drop
//!    `pending_recv`; the only `clear_recv_buffer` call sites require the
//!    `RecvStream` to have been dropped already. That is what makes it sound to
//!    keep the record for streams at or below a GOAWAY's `last_stream_id`.
//! 4. Frames are processed strictly in wire order by the connection task.

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
    /// Streams that are still in flight, i.e. neither ended nor torn down.
    /// Entries are removed as soon as their outcome is decided, which bounds
    /// this to the streams actually open on the connection.
    pending: Mutex<HashMap<u32, Arc<StreamRecord>>>,
    /// Changes whenever application-side `forget` removes a live entry, so the
    /// scanner can lazily discard stale cache entries once per read batch.
    forget_generation: AtomicUsize,
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
            pending: self.pending.lock(),
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

    /// Stop watching `stream_id`. Called when the session is dropped so that a
    /// long-lived connection does not accumulate entries for streams whose
    /// outcome was never decided on the wire.
    pub fn forget(&self, stream_id: u32) {
        if self.pending.lock().remove(&stream_id).is_some() {
            self.forget_generation.fetch_add(1, Ordering::Release);
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
        if let Some(record) = self.pending.lock().get(&stream_id) {
            record
                .data_bytes
                .fetch_add(payload_bytes, Ordering::Relaxed);
        }
    }

    /// Resolve a live stream for the scanner's bounded DATA fast path.
    fn data_record(&self, stream_id: u32) -> Option<Arc<StreamRecord>> {
        self.pending.lock().get(&stream_id).cloned()
    }

    fn cached_streams_live(&self, stream_ids: [Option<u32>; 2]) -> [bool; 2] {
        let pending = self.pending.lock();
        stream_ids.map(|stream_id| stream_id.is_some_and(|id| pending.contains_key(&id)))
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
    /// longer freezes `data_bytes`, so the returned [`EndStreamPublished`] must
    /// be handed to [`FrameScanner::drop_cache_after_publish`] before anything
    /// else touches the wire.
    fn publish(&self, stream_id: u32, payload_bytes: usize) -> EndStreamPublished {
        if let Some(record) = self.pending.lock().remove(&stream_id) {
            if payload_bytes != 0 {
                record
                    .data_bytes
                    .fetch_add(payload_bytes, Ordering::Relaxed);
            }
            record.end_stream.store(true, Ordering::Release);
        }
        EndStreamPublished(stream_id)
    }

    /// The peer tore down `stream_id` without having flagged END_STREAM.
    fn note_stream_torn_down(&self, stream_id: u32) {
        self.pending.lock().remove(&stream_id);
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
    fn note_connection_torn_down(&self, last_stream_id: u32) {
        self.pending.lock().retain(|id, _| *id <= last_stream_id);
    }
}

/// Evidence that [`EndStreamWatch::publish`] ran for a stream, carrying the id
/// whose scanner cache still has to be dropped.
///
/// Publication freezes a record for readers, but only for as long as nothing
/// keeps writing to it. A cache entry that outlived the `Release` store would
/// let a later (protocol-violating) frame move `data_bytes` after
/// [`StreamRecord::vouches_for`] may already have returned `true`. Making the
/// publication hand back a `#[must_use]` value that only
/// [`FrameScanner::drop_cache_after_publish`] consumes turns "remember to clear
/// the cache too" from a comment into something the compiler asks about.
#[must_use = "publishing END_STREAM must be paired with dropping the scanner cache for that stream"]
struct EndStreamPublished(u32);

/// Exclusive access to the registration map, held across `h2`'s stream id
/// allocation. See [`EndStreamWatch::registration`].
pub(crate) struct Registration<'a> {
    pending: MutexGuard<'a, HashMap<u32, Arc<StreamRecord>>>,
}

impl Registration<'_> {
    /// Start watching `stream_id`, returning its (initially blank) record.
    pub fn register(mut self, stream_id: u32) -> Arc<StreamRecord> {
        let record = Arc::new(StreamRecord::default());
        self.pending.insert(stream_id, record.clone());
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

    /// Consume the evidence that a stream was published by dropping the cache
    /// entry the publication froze. See [`EndStreamPublished`].
    fn drop_cache_after_publish(&mut self, published: EndStreamPublished) {
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
                    self.note_data_frame(padded.stream_id, data_len, padded.end_stream, watch);
                }
                self.payload_left -= skip;
                bytes = &bytes[skip..];
                if self.payload_left == 0 {
                    self.finish_goaway(watch);
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
                        self.note_data_frame(stream_id, self.payload_left, end_stream, watch);
                    }
                }
                FRAME_TYPE_HEADERS if flags & FLAG_END_STREAM != 0 => {
                    // Trailers ending the response. They carry no body bytes,
                    // so the count stands as the DATA frames left it.
                    self.publish_end_stream(stream_id, 0, watch);
                }
                FRAME_TYPE_RST_STREAM => {
                    watch.note_stream_torn_down(stream_id);
                    self.clear_data_state(stream_id);
                }
                FRAME_TYPE_GOAWAY => {
                    self.goaway = Some(LastStreamId::default());
                    if self.payload_left == 0 {
                        // Malformed (a GOAWAY payload is at least 8 bytes).
                        self.finish_goaway(watch);
                    }
                }
                _ => {}
            }
        }
    }

    /// Dispatch the GOAWAY whose payload has just been fully skipped.
    ///
    /// A payload too short to carry the field cannot be trusted to name any
    /// stream, so it clears every registration -- the conservative direction,
    /// and the same one the pre-`last_stream_id` version of this took for every
    /// GOAWAY.
    fn finish_goaway(&mut self, watch: &EndStreamWatch) {
        if let Some(goaway) = self.goaway.take() {
            let last_stream_id = goaway.get().unwrap_or(0);
            watch.note_connection_torn_down(last_stream_id);
            for cached in &mut self.data_records {
                if cached
                    .as_ref()
                    .is_some_and(|cached| cached.stream_id > last_stream_id)
                {
                    *cached = None;
                }
            }
        }
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
        // Scanned on `Ready(Err(..))` too: an `AsyncRead` may legitimately fill
        // the buffer and then report the error that ended the stream, and those
        // bytes are exactly the ones a teardown-shaped read carries. `h2` will
        // process them, so the watch must too. (`Pending` never fills the
        // buffer.)
        if res.is_ready() {
            let fresh = &buf.filled()[already_filled..];
            if !fresh.is_empty() {
                me.scanner.scan(fresh, &me.watch);
            }
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
mod tests {
    use super::*;
    use std::hint::black_box;
    use std::time::{Duration, Instant};

    fn frame(frame_type: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u32;
        let id = stream_id.to_be_bytes();
        let mut v = vec![
            (len >> 16) as u8,
            (len >> 8) as u8,
            len as u8,
            frame_type,
            flags,
            id[0],
            id[1],
            id[2],
            id[3],
        ];
        v.extend_from_slice(payload);
        v
    }

    /// A padded DATA frame: `Pad Length` byte, then the payload, then `pad`
    /// zero bytes. The frame length covers all three.
    fn padded_data_frame(flags: u8, stream_id: u32, payload: &[u8], pad: u8) -> Vec<u8> {
        let mut body = vec![pad];
        body.extend_from_slice(payload);
        body.extend(std::iter::repeat_n(0u8, usize::from(pad)));
        frame(FRAME_TYPE_DATA, flags | FLAG_PADDED, stream_id, &body)
    }

    #[test]
    fn end_stream_on_data_is_recorded() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        scanner.scan(
            &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello"),
            &watch,
        );
        assert!(flag.end_stream_observed());
    }

    /// The record has to vouch for a byte COUNT, not just for the flag: that
    /// count is what lets the reader detect DATA `h2` decoded and then dropped.
    #[test]
    fn data_payload_bytes_are_counted() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"hello");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"xy"));
        scanner.scan(&wire, &watch);
        assert!(record.vouches_for(7));
        assert!(
            !record.vouches_for(5),
            "a short read must not be vouched for"
        );
        assert!(!record.vouches_for(8));
    }

    /// Without END_STREAM there is nothing to vouch for, however many bytes
    /// were seen.
    #[test]
    fn a_count_without_end_stream_vouches_for_nothing() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, b"hello"), &watch);
        assert!(!record.vouches_for(5));
    }

    /// `h2` hands the reader `Data::payload()`, which excludes both the Pad
    /// Length field and the padding, so the count must exclude them too --
    /// otherwise a padded response would never match and the §8.1 shape would
    /// silently stop working for peers that pad.
    #[test]
    fn padding_is_not_counted_as_payload() {
        for split in [usize::MAX, 1, 2, 5, 9, 10, 11] {
            let watch = EndStreamWatch::default();
            let record = watch.register(1);
            let mut scanner = FrameScanner::default();
            let mut wire = padded_data_frame(0, 1, b"hello", 4);
            wire.extend_from_slice(&padded_data_frame(FLAG_END_STREAM, 1, b"xy", 7));
            for chunk in wire.chunks(split.min(wire.len())) {
                scanner.scan(chunk, &watch);
            }
            assert!(record.end_stream_observed(), "split={split}");
            assert!(record.vouches_for(7), "split={split}");
        }
    }

    /// A padded DATA frame with no payload at all is malformed (`h2` answers it
    /// with a protocol error). It must not leave the scanner waiting for a Pad
    /// Length byte that never comes, nor claim any bytes.
    #[test]
    fn empty_padded_data_frame_counts_nothing() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, FLAG_PADDED, 1, b"");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"ab"));
        scanner.scan(&wire, &watch);
        assert!(record.vouches_for(2));
    }

    /// A PADDED DATA frame with a zero-length payload is malformed: the Pad
    /// Length octet is missing, and h2 answers it with a connection error. Its
    /// END_STREAM flag must therefore never be recorded as a real end of body,
    /// or a peer could claim a complete body it never sent.
    #[test]
    fn empty_padded_data_frame_does_not_record_end_stream() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let wire = frame(FRAME_TYPE_DATA, FLAG_PADDED | FLAG_END_STREAM, 1, b"");
        scanner.scan(&wire, &watch);
        assert!(
            !record.vouches_for(0),
            "a malformed padded frame must not vouch for an end of body"
        );

        // A well-formed empty final DATA frame still does.
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"");
        scanner.scan(&wire, &watch);
        assert!(record.vouches_for(0));
    }

    /// Trailers carry the END_STREAM but no body bytes, so the count must be
    /// whatever the DATA frames left it at.
    #[test]
    fn end_stream_on_trailers_keeps_the_data_count() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"hello");
        wire.extend_from_slice(&frame(
            FRAME_TYPE_HEADERS,
            FLAG_END_STREAM | 0x4,
            1,
            b"\x88",
        ));
        scanner.scan(&wire, &watch);
        assert!(record.vouches_for(5));
    }

    /// Once a stream's outcome is decided its record is frozen: a protocol
    /// violation afterwards may neither add bytes nor set the flag.
    #[test]
    fn a_decided_record_is_frozen() {
        let watch = EndStreamWatch::default();
        let ended = watch.register(1);
        let torn_down = watch.register(3);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello");
        wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, 3, &[0, 0, 0, 0]));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"more"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"late"));
        scanner.scan(&wire, &watch);
        assert!(ended.vouches_for(5));
        assert!(!ended.vouches_for(9));
        assert!(!torn_down.end_stream_observed());
        assert!(!torn_down.vouches_for(4));
    }

    #[test]
    fn data_without_end_stream_is_not_recorded() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, b"hello"), &watch);
        assert!(!flag.end_stream_observed());
    }

    /// A payload that looks exactly like a frame header carrying END_STREAM
    /// must not be mistaken for one.
    #[test]
    fn payload_bytes_are_skipped_not_parsed() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        let decoy = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"");
        scanner.scan(&frame(FRAME_TYPE_DATA, 0, 1, &decoy), &watch);
        assert!(!flag.end_stream_observed());
    }

    /// Frames are recorded correctly no matter how the reads are chopped up.
    #[test]
    fn split_reads_are_reassembled() {
        for split in 1..20 {
            let watch = EndStreamWatch::default();
            let flag = watch.register(3);
            let mut scanner = FrameScanner::default();
            let mut wire = frame(FRAME_TYPE_HEADERS, 0x4, 3, b"\x88");
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"abc"));
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"de"));
            for chunk in wire.chunks(split) {
                scanner.scan(chunk, &watch);
            }
            assert!(flag.end_stream_observed(), "split={split}");
        }
    }

    #[test]
    fn reset_before_end_stream_wins() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_RST_STREAM, 0, 1, &[0, 0, 0, 0]);
        // A protocol-violating END_STREAM after the reset must not resurrect
        // the record: `h2` would never deliver that frame's payload.
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"x"));
        scanner.scan(&wire, &watch);
        assert!(!flag.end_stream_observed());
    }

    #[test]
    fn reset_after_end_stream_does_not_retract() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello");
        wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, 1, &[0, 0, 0, 0]));
        scanner.scan(&wire, &watch);
        assert!(flag.end_stream_observed());
    }

    /// A GOAWAY payload naming `last_stream_id` and `error_code`.
    fn goaway_payload(last_stream_id: u32) -> Vec<u8> {
        let mut v = last_stream_id.to_be_bytes().to_vec();
        v.extend_from_slice(&[0; 4]); // error code NO_ERROR
        v
    }

    #[test]
    fn goaway_clears_pending_streams_only() {
        let watch = EndStreamWatch::default();
        let ended = watch.register(1);
        let pending = watch.register(3);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hi");
        wire.extend_from_slice(&frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(0)));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hi"));
        scanner.scan(&wire, &watch);
        assert!(ended.end_stream_observed());
        assert!(!pending.end_stream_observed());
    }

    /// The graceful-shutdown pattern every mainstream server uses: an initial
    /// `GOAWAY(NO_ERROR, 2^31-1)`, then the in-flight streams finish normally.
    /// `h2` still delivers them, so their registrations must survive.
    #[test]
    fn goaway_keeps_streams_at_or_below_last_stream_id() {
        let watch = EndStreamWatch::default();
        let in_flight = watch.register(3);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(0x7fff_ffff));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hi"));
        scanner.scan(&wire, &watch);
        assert!(in_flight.end_stream_observed());
    }

    /// Above the threshold `h2` errors the stream out at once and ignores every
    /// later frame for it, so a late END_STREAM there is a false positive.
    #[test]
    fn goaway_clears_streams_above_last_stream_id() {
        let watch = EndStreamWatch::default();
        let kept = watch.register(3);
        let dropped = watch.register(5);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(3));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hi"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"hi"));
        scanner.scan(&wire, &watch);
        assert!(kept.end_stream_observed());
        assert!(!dropped.end_stream_observed());
        assert!(watch.pending.lock().is_empty());
    }

    /// The identifier is read correctly however the payload is chopped up, and
    /// its reserved high bit is ignored on receipt.
    #[test]
    fn goaway_last_stream_id_is_reassembled_and_masked() {
        for split in 1..24 {
            let watch = EndStreamWatch::default();
            let kept = watch.register(3);
            let dropped = watch.register(5);
            let mut scanner = FrameScanner::default();
            let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(0x8000_0003));
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hi"));
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"hi"));
            for chunk in wire.chunks(split) {
                scanner.scan(chunk, &watch);
            }
            assert!(kept.end_stream_observed(), "split={split}");
            assert!(!dropped.end_stream_observed(), "split={split}");
        }
    }

    /// A GOAWAY too short to carry `last_stream_id` names no stream, so nothing
    /// may be trusted afterwards.
    #[test]
    fn malformed_short_goaway_clears_everything() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &[0, 0]);
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hi"));
        scanner.scan(&wire, &watch);
        assert!(!flag.end_stream_observed());
        assert!(watch.pending.lock().is_empty());
    }

    /// An empty GOAWAY payload must be dispatched at the header, not left
    /// waiting for payload bytes that never come.
    #[test]
    fn empty_goaway_payload_is_dispatched_immediately() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, b"");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hi"));
        scanner.scan(&wire, &watch);
        assert!(!flag.end_stream_observed());
    }

    /// The registration lock is what closes the window in which `h2` has
    /// allocated a stream id and flushed its HEADERS -- so the peer's whole
    /// response can already be scanned -- but the caller has not registered
    /// yet. A scan that runs while a registration is being taken must BLOCK,
    /// not silently drop the record.
    #[test]
    fn a_scan_waits_for_an_in_progress_registration() {
        use std::sync::atomic::AtomicUsize;

        let watch = Arc::new(EndStreamWatch::default());
        let registration = watch.registration();

        let scanned = Arc::new(AtomicUsize::new(0));
        let scanner_watch = watch.clone();
        let scanner_done = scanned.clone();
        let scanner = std::thread::spawn(move || {
            let mut scanner = FrameScanner::default();
            scanner.scan(
                &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello"),
                &scanner_watch,
            );
            scanner_done.store(1, Ordering::Release);
        });

        // The scan cannot have completed while the guard is held. This is a
        // one-way check: it can only fail if the lock is not being taken.
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            scanned.load(Ordering::Acquire),
            0,
            "the scan must block on the registration lock"
        );

        let flag = registration.register(1);
        scanner.join().unwrap();
        assert!(
            flag.end_stream_observed(),
            "the END_STREAM seen during registration must not be lost"
        );
    }

    /// Bytes an `AsyncRead` delivered together with the error that ended the
    /// stream still reach `h2`, so they must still be scanned.
    #[tokio::test]
    async fn bytes_delivered_with_an_error_are_scanned() {
        struct DataThenError(Option<Vec<u8>>);
        impl AsyncRead for DataThenError {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                let bytes = self.0.take().expect("polled after error");
                buf.put_slice(&bytes);
                Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "reset")))
            }
        }

        let watch = EndStreamWatch::new();
        let flag = watch.registration().register(1);
        let mut io = EndStreamWatchStream::new(
            DataThenError(Some(frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello"))),
            watch,
        );
        let mut buf = [0u8; 64];
        let mut read_buf = ReadBuf::new(&mut buf);
        let res = std::future::poll_fn(|cx| Pin::new(&mut io).poll_read(cx, &mut read_buf)).await;
        assert!(res.is_err());
        assert!(flag.end_stream_observed());
    }

    /// Only the registered stream's own END_STREAM counts.
    #[test]
    fn other_streams_do_not_set_the_flag() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        scanner.scan(
            &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"hello"),
            &watch,
        );
        assert!(!flag.end_stream_observed());
    }

    /// The reserved high bit of the stream identifier is ignored on receipt.
    #[test]
    fn reserved_bit_in_stream_id_is_masked() {
        let watch = EndStreamWatch::default();
        let flag = watch.register(1);
        let mut scanner = FrameScanner::default();
        scanner.scan(
            &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 0x8000_0001, b"hello"),
            &watch,
        );
        assert!(flag.end_stream_observed());
    }

    /// Deciding a stream's outcome must not leave anything behind: a pooled
    /// connection sees an unbounded number of streams over its lifetime.
    #[test]
    fn decided_streams_are_evicted() {
        let watch = EndStreamWatch::default();
        let mut scanner = FrameScanner::default();
        for id in (1..100).step_by(2) {
            watch.register(id);
            scanner.scan(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, id, b"x"), &watch);
        }
        assert!(watch.pending.lock().is_empty());
    }

    #[test]
    fn forget_prevents_publication_with_a_warm_cache() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();

        let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"be");
        warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"fo"));
        warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"re"));
        scanner.scan(&warmup, &watch);
        watch.forget(1);
        scanner.scan(
            &frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"after"),
            &watch,
        );

        assert!(!record.end_stream_observed());
        assert!(!record.vouches_for(11));
    }

    #[test]
    fn a_cached_record_is_frozen_after_end_stream() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"b"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"x"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"c"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"invalid"));
        scanner.scan(&wire, &watch);

        assert!(record.vouches_for(4));
        assert!(!record.vouches_for(11));
    }

    #[test]
    fn reset_prevents_publication_with_a_warm_cache() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"be");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"fo"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"re"));
        wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, 1, &[0, 0, 0, 0]));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"invalid"));
        scanner.scan(&wire, &watch);

        assert!(!record.end_stream_observed());
    }

    #[test]
    fn end_stream_before_forget_remains_observed_with_a_warm_cache() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"b"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"c"));
        scanner.scan(&wire, &watch);
        watch.forget(1);

        assert!(record.vouches_for(3));
    }

    #[test]
    fn goaway_prunes_only_excluded_cached_records() {
        let watch = EndStreamWatch::default();
        let kept = watch.register(1);
        let excluded = watch.register(3);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"b"));
        wire.extend_from_slice(&frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1)));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"c"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"d"));
        scanner.scan(&wire, &watch);

        assert!(kept.vouches_for(2));
        assert!(!excluded.end_stream_observed());
    }

    #[test]
    fn interleaved_streams_keep_independent_cached_counts() {
        let watch = EndStreamWatch::default();
        let first = watch.register(1);
        let second = watch.register(3);
        let mut scanner = FrameScanner::default();
        let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"bb"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"ccc"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b""));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b""));
        scanner.scan(&wire, &watch);

        assert!(first.vouches_for(4));
        assert!(second.vouches_for(2));
    }

    #[test]
    fn scanner_cache_retains_at_most_two_forgotten_records() {
        let watch = EndStreamWatch::default();
        let mut scanner = FrameScanner::default();
        let mut records = Vec::new();

        for stream_id in (1..200).step_by(2) {
            let record = watch.register(stream_id);
            records.push(Arc::downgrade(&record));
            let mut wire = frame(FRAME_TYPE_DATA, 0, stream_id, b"a");
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, stream_id, b"b"));
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, stream_id, b"cached"));
            scanner.scan(&wire, &watch);
            watch.forget(stream_id);
        }

        assert!(watch.pending.lock().is_empty());
        assert!(
            records
                .iter()
                .filter(|record| record.upgrade().is_some())
                .count()
                <= 2
        );
    }

    #[test]
    fn full_cache_reclaims_a_forgotten_record_for_a_new_stream() {
        let watch = EndStreamWatch::default();
        let first = watch.register(1);
        let second = watch.register(3);
        let first_weak = Arc::downgrade(&first);
        let second_weak = Arc::downgrade(&second);
        let mut scanner = FrameScanner::default();
        let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"a");
        warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"b"));
        scanner.scan(&warmup, &watch);
        watch.forget(1);
        watch.forget(3);
        drop(first);
        drop(second);

        let replacement = watch.register(5);
        scanner.scan(&frame(FRAME_TYPE_DATA, 0, 5, b"c"), &watch);
        assert!(scanner
            .data_records
            .iter()
            .flatten()
            .any(|cached| cached.stream_id == 5));
        assert_eq!(
            [first_weak, second_weak]
                .iter()
                .filter(|record| record.upgrade().is_some())
                .count(),
            0,
            "both forgotten slots should have been reclaimed"
        );

        scanner.scan(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"d"), &watch);
        assert!(replacement.vouches_for(2));
    }

    /// With more live streams than cache slots, the extra ones fall back to the
    /// borrowed map lookup in `FrameScanner::note_data`'s full-cache branch.
    /// That branch carries every DATA frame of every stream past the second on
    /// a multiplexed connection, so its counts must stay exactly as separate as
    /// the cached ones -- including for the stream that never gets a slot.
    #[test]
    fn a_full_cache_still_counts_the_uncached_stream_separately() {
        let watch = EndStreamWatch::default();
        let first = watch.register(1);
        let second = watch.register(3);
        let third = watch.register(5);
        let mut scanner = FrameScanner::default();

        // Streams 1 and 3 claim both slots, so every frame for stream 5 misses
        // a full cache and goes through `EndStreamWatch::note_data`.
        let mut wire = frame(FRAME_TYPE_DATA, 0, 1, b"a");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"bb"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 5, b"ccc"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 5, b"dddd"));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"e"));
        scanner.scan(&wire, &watch);

        let cached: Vec<u32> = scanner
            .data_records
            .iter()
            .flatten()
            .map(|cached| cached.stream_id)
            .collect();
        assert_eq!(cached, vec![1, 3], "both slots must be taken by 1 and 3");

        let mut wire = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 5, b"f");
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b""));
        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b""));
        scanner.scan(&wire, &watch);

        assert!(first.vouches_for(2));
        assert!(second.vouches_for(2));
        assert!(third.vouches_for(8), "the uncached stream must count 3+4+1");
        assert!(!third.vouches_for(2));
    }

    /// The whole two-slot design rests on one claim: a terminal frame ALWAYS
    /// re-consults the shared map, so an application `forget` wins even while
    /// the scanner's cache is still warm for that stream. The generation check
    /// runs once at the top of a read batch, so a `forget` landing after it
    /// leaves the cache warm for the rest of that batch -- which is precisely
    /// the window pinned down here.
    ///
    /// `forget_prevents_publication_with_a_warm_cache` does NOT cover this: its
    /// `forget` lands between two `scan` calls, so the next batch's generation
    /// check has already emptied the slot before the terminal frame arrives.
    #[test]
    fn a_mid_batch_forget_still_blocks_a_warm_cache_publication() {
        let watch = EndStreamWatch::default();
        let record = watch.register(1);
        let mut scanner = FrameScanner::default();

        let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"be");
        warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"fore"));
        scanner.scan(&warmup, &watch);

        // Stand in for a batch whose generation check has already run: nothing
        // will invalidate the slot for the remainder of it.
        scanner.discard_forgotten_data_records(&watch);
        watch.forget(1);
        assert!(
            scanner
                .data_records
                .iter()
                .flatten()
                .any(|cached| cached.stream_id == 1),
            "the cache must still be warm for the window under test"
        );

        // Stale increments on a forgotten record are tolerated ...
        scanner.note_data_frame(1, 5, false, &watch);
        assert_eq!(
            record.data_bytes.load(Ordering::Relaxed),
            11,
            "the stale increment must have reached the record, or this test is \
             not exercising the warm-cache window it claims to"
        );

        // ... but the terminal frame must not publish it, and must not add its
        // own payload to it either: the map lookup is what fails, before any
        // counting.
        scanner.note_data_frame(1, 2, true, &watch);

        assert!(!record.end_stream_observed());
        assert_eq!(record.data_bytes.load(Ordering::Relaxed), 11);
    }

    /// `Http2Session` forgets on `note_local_reset` and again on `Drop`, so the
    /// second call routinely finds nothing. It must not advance the generation,
    /// or every locally reset stream would cost the scanner an extra locked
    /// liveness sweep on its next read.
    #[test]
    fn forgetting_an_absent_stream_does_not_advance_the_generation() {
        let watch = EndStreamWatch::default();
        watch.register(1);
        watch.forget(1);
        let after_live_removal = watch.forget_generation.load(Ordering::Acquire);

        watch.forget(1);
        watch.forget(999);
        assert_eq!(
            watch.forget_generation.load(Ordering::Acquire),
            after_live_removal
        );
    }

    /// The cache prune lives in `finish_goaway`, which runs when the payload's
    /// last byte is consumed -- possibly several reads after the header. A warm
    /// cache must be pruned by the same `last_stream_id` rule whenever that
    /// lands, not only when the whole frame arrives in one read.
    #[test]
    fn a_split_goaway_prunes_a_warm_cache() {
        for split in 1..16 {
            let watch = EndStreamWatch::default();
            let kept = watch.register(1);
            let excluded = watch.register(3);
            let mut scanner = FrameScanner::default();

            let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"a");
            warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"b"));
            scanner.scan(&warmup, &watch);

            let mut wire = frame(FRAME_TYPE_GOAWAY, 0, 0, &goaway_payload(1));
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"c"));
            wire.extend_from_slice(&frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 3, b"d"));
            for chunk in wire.chunks(split) {
                scanner.scan(chunk, &watch);
            }

            assert!(kept.vouches_for(2), "split={split}");
            assert!(!excluded.end_stream_observed(), "split={split}");
            assert!(
                !scanner
                    .data_records
                    .iter()
                    .flatten()
                    .any(|cached| cached.stream_id == 3),
                "the excluded stream must not stay cached, split={split}"
            );
        }
    }

    /// A tiny deterministic PRNG, so the differential test below reproduces
    /// exactly without adding a dev-dependency.
    struct Xorshift64(u64);

    impl Xorshift64 {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next_u64() % bound as u64) as usize
        }
    }

    /// One step of a replayable history. Chopping the wire into explicit chunks
    /// and interleaving `forget` calls is what lets every scanner configuration
    /// observe byte-for-byte the same sequence of events.
    enum Step {
        Feed(Vec<u8>),
        Forget(u32),
    }

    /// Build a randomized but reproducible history: a mix of plain and padded
    /// DATA (with and without END_STREAM), trailers, RST_STREAM, GOAWAY and an
    /// ignored frame type, over a small pool of stream ids, split across reads
    /// at arbitrary offsets with `forget` calls dropped in between.
    ///
    /// Returns the steps and the ids to register.
    fn random_scenario(seed: u64) -> (Vec<Step>, Vec<u32>) {
        // Mix before forcing the low bit: a bare `seed | 1` would collapse each
        // even seed onto its odd successor and halve the distinct scenarios.
        let mut rng = Xorshift64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let stream_ids: Vec<u32> = (1..=9u32).step_by(2).collect();

        let mut wire = Vec::new();
        for _ in 0..24 {
            let stream_id = stream_ids[rng.below(stream_ids.len())];
            let payload = vec![0x5a; rng.below(6)];
            match rng.below(10) {
                0..=5 => {
                    let flags = if rng.below(4) == 0 {
                        FLAG_END_STREAM
                    } else {
                        0
                    };
                    if rng.below(4) == 0 {
                        let pad = rng.below(4) as u8;
                        wire.extend_from_slice(&padded_data_frame(flags, stream_id, &payload, pad));
                    } else {
                        wire.extend_from_slice(&frame(FRAME_TYPE_DATA, flags, stream_id, &payload));
                    }
                }
                6 => wire.extend_from_slice(&frame(
                    FRAME_TYPE_HEADERS,
                    FLAG_END_STREAM | 0x4,
                    stream_id,
                    b"\x88",
                )),
                7 => wire.extend_from_slice(&frame(FRAME_TYPE_RST_STREAM, 0, stream_id, &[0; 4])),
                8 => wire.extend_from_slice(&frame(
                    FRAME_TYPE_GOAWAY,
                    0,
                    0,
                    &goaway_payload(stream_id),
                )),
                // CONTINUATION: a type the scanner must skip by length only.
                _ => wire.extend_from_slice(&frame(0x9, 0, stream_id, &payload)),
            }
        }

        let mut steps = Vec::new();
        let mut rest = wire.as_slice();
        while !rest.is_empty() {
            let take = (1 + rng.below(11)).min(rest.len());
            steps.push(Step::Feed(rest[..take].to_vec()));
            rest = &rest[take..];
            if rng.below(6) == 0 {
                steps.push(Step::Forget(stream_ids[rng.below(stream_ids.len())]));
            }
        }

        (steps, stream_ids)
    }

    /// Replay one history against a fresh watch, returning each registered
    /// stream's `(end_stream_observed, data_bytes)`.
    fn replay(steps: &[Step], stream_ids: &[u32], mut scanner: FrameScanner) -> Vec<(bool, usize)> {
        let watch = EndStreamWatch::default();
        let records: Vec<_> = stream_ids
            .iter()
            .map(|stream_id| watch.register(*stream_id))
            .collect();

        for step in steps {
            match step {
                Step::Feed(chunk) => scanner.scan(chunk, &watch),
                Step::Forget(stream_id) => watch.forget(*stream_id),
            }
        }

        records
            .iter()
            .map(|record| {
                (
                    record.end_stream_observed(),
                    record.data_bytes.load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// The scanner cache and the combined terminal path are pure optimizations:
    /// over ANY frame history they must reach the same verdict as the code they
    /// replaced. The two benchmark A/B switches make all four configurations
    /// reachable, so replay one randomized history through each and compare.
    ///
    /// `data_bytes` is compared too, not just the published flag. A warm cached
    /// record CAN legitimately outcount the uncached one, but only for a
    /// `forget` that lands mid-batch -- and a replayed `Step::Forget` always
    /// lands on a batch boundary, where the next `scan`'s generation check
    /// empties the slot before another frame arrives. So the counts must agree
    /// here, and requiring that is what lets this test see a broken
    /// `discard_forgotten_data_records`. The genuinely mid-batch window has its
    /// own test: `a_mid_batch_forget_still_blocks_a_warm_cache_publication`.
    #[test]
    fn the_cache_and_the_combined_terminal_path_are_observationally_equivalent() {
        for seed in 1..=200u64 {
            let (steps, stream_ids) = random_scenario(seed);
            let baseline = replay(
                &steps,
                &stream_ids,
                FrameScanner {
                    data_cache_disabled: true,
                    terminal_data_combining_disabled: true,
                    ..FrameScanner::default()
                },
            );

            for (name, scanner) in [
                ("cached+combined", FrameScanner::default()),
                (
                    "uncached+combined",
                    FrameScanner {
                        data_cache_disabled: true,
                        ..FrameScanner::default()
                    },
                ),
                (
                    "cached+legacy-terminal",
                    FrameScanner {
                        terminal_data_combining_disabled: true,
                        ..FrameScanner::default()
                    },
                ),
            ] {
                let observed = replay(&steps, &stream_ids, scanner);
                for (index, stream_id) in stream_ids.iter().enumerate() {
                    assert_eq!(
                        observed[index].0, baseline[index].0,
                        "{name}: END_STREAM differs for stream {stream_id} (seed={seed})"
                    );
                    assert_eq!(
                        observed[index].1, baseline[index].1,
                        "{name}: byte count differs for stream {stream_id} (seed={seed})"
                    );
                }
            }
        }
    }

    /// Manual release-mode microbenchmark for the incremental scanner cost.
    ///
    /// Run with:
    /// `cargo test -p pingora-core --release benchmark_end_stream_watch -- \
    ///     --ignored --nocapture --test-threads=1`
    ///
    /// This deliberately lives beside the private scanner instead of exposing
    /// benchmark-only production API.
    ///
    /// What is inside the timed region, so the numbers are read correctly:
    ///
    /// - The single-stream workloads register one stream PER ITERATION, because
    ///   their wire ends in END_STREAM and publication evicts the entry. So one
    ///   `register` (a lock, a HashMap insert and the one `Arc<StreamRecord>`
    ///   allocation) is amortized over that workload's frames. `headers_end`
    ///   isolates that floor: it exercises neither DATA optimization, so all
    ///   variants there measure registration plus frame parsing and nothing
    ///   else. Subtract it to read the per-request DATA saving in absolute
    ///   terms.
    /// - The multi-stream workloads (`alternating_*`, `round_robin_*`) carry no
    ///   terminal frame and register their streams ONCE, outside the timed
    ///   region. Registration there would scale with the stream count and
    ///   dilute exactly the difference being measured. They therefore report
    ///   steady-state per-frame cost with a warm cache, which is the regime the
    ///   two-slot cache exists for.
    ///
    /// Each variant uses its own watch, but that watch is shared across the
    /// samples of that variant; HashMap capacity growth is paid during warm-up.
    #[test]
    #[ignore = "manual performance measurement"]
    fn benchmark_end_stream_watch() {
        const SAMPLES: usize = 7;

        fn report(name: &str, iterations: usize, mut run: impl FnMut()) {
            for _ in 0..(iterations / 20).max(1) {
                run();
            }

            let mut samples = Vec::with_capacity(SAMPLES);
            for _ in 0..SAMPLES {
                let started = Instant::now();
                for _ in 0..iterations {
                    run();
                }
                samples.push(started.elapsed().as_secs_f64() * 1e9 / iterations as f64);
            }
            samples.sort_by(f64::total_cmp);
            println!(
                "end_stream_watch_bench {name}: median={:.2} ns/op min={:.2} ns/op iterations={iterations}",
                samples[SAMPLES / 2], samples[0]
            );
        }

        fn data_run(frame_count: usize, payload_len: usize) -> Vec<u8> {
            let payload = vec![0x5a; payload_len];
            let mut wire = Vec::with_capacity(frame_count * (FRAME_HEADER_LEN + payload_len));
            for frame_index in 0..frame_count {
                let flags = if frame_index + 1 == frame_count {
                    FLAG_END_STREAM
                } else {
                    0
                };
                wire.extend_from_slice(&frame(FRAME_TYPE_DATA, flags, 1, &payload));
            }
            wire
        }

        let headers_end = frame(FRAME_TYPE_HEADERS, FLAG_END_STREAM | 0x4, 1, b"\x88");
        let one_data_end = frame(FRAME_TYPE_DATA, FLAG_END_STREAM, 1, b"hello");
        let response_16k = data_run(64, 16 * 1024);
        let response_64k = data_run(16, 64 * 1024);
        let streaming = data_run(1024, 1);

        for (name, wire, iterations) in [
            ("headers_end", &headers_end, 200_000),
            ("one_data_end", &one_data_end, 200_000),
            ("1mib_16k_frames", &response_16k, 10_000),
            ("1mib_64k_frames", &response_64k, 20_000),
            ("streaming_1024_frames", &streaming, 2_000),
        ] {
            report(&format!("{name}/raw_slice_control"), iterations, || {
                black_box(black_box(wire).as_slice());
            });

            let empty_watch = EndStreamWatch::default();
            let mut empty_scanner = FrameScanner::default();
            report(&format!("{name}/unregistered"), iterations, || {
                empty_scanner.scan(black_box(wire), &empty_watch);
                black_box(&empty_scanner);
            });

            let watched = EndStreamWatch::default();
            let mut watched_scanner = FrameScanner::default();
            report(&format!("{name}/watched"), iterations, || {
                let record = watched.register(1);
                watched_scanner.scan(black_box(wire), &watched);
                black_box(record.end_stream_observed());
            });

            let no_cache_watch = EndStreamWatch::default();
            let mut no_cache_scanner = FrameScanner {
                data_cache_disabled: true,
                ..FrameScanner::default()
            };
            report(&format!("{name}/watched_no_cache"), iterations, || {
                let record = no_cache_watch.register(1);
                no_cache_scanner.scan(black_box(wire), &no_cache_watch);
                black_box(record.end_stream_observed());
            });

            let legacy_terminal_watch = EndStreamWatch::default();
            let mut legacy_terminal_scanner = FrameScanner {
                data_cache_disabled: true,
                terminal_data_combining_disabled: true,
                ..FrameScanner::default()
            };
            report(&format!("{name}/legacy_terminal"), iterations, || {
                let record = legacy_terminal_watch.register(1);
                legacy_terminal_scanner.scan(black_box(wire), &legacy_terminal_watch);
                black_box(record.end_stream_observed());
            });
        }

        // Steady-state multiplexed cost. These wires carry no terminal frame,
        // so the registrations survive every iteration and can be taken once,
        // outside the timed region: what is measured is the per-DATA-frame cost
        // with a warm cache, undiluted by a `register` that would scale with the
        // stream count.
        fn round_robin_wire(stream_count: u32) -> Vec<u8> {
            let mut wire = Vec::new();
            for frame_index in 0..1024u32 {
                let stream_id = 1 + 2 * (frame_index % stream_count);
                wire.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, stream_id, b"x"));
            }
            wire
        }

        for stream_count in [2u32, 3, 8, 32] {
            let wire = round_robin_wire(stream_count);
            let name = if stream_count == 2 {
                "alternating_1024_frames".to_string()
            } else {
                format!("round_robin_{stream_count}_streams")
            };

            let cached_watch = EndStreamWatch::default();
            let cached_records: Vec<_> = (0..stream_count)
                .map(|index| cached_watch.register(1 + 2 * index))
                .collect();
            let mut cached_scanner = FrameScanner::default();
            report(&format!("{name}/watched"), 1_000, || {
                cached_scanner.scan(black_box(&wire), &cached_watch);
                black_box(&cached_records);
            });

            let no_cache_watch = EndStreamWatch::default();
            let no_cache_records: Vec<_> = (0..stream_count)
                .map(|index| no_cache_watch.register(1 + 2 * index))
                .collect();
            let mut no_cache_scanner = FrameScanner {
                data_cache_disabled: true,
                ..FrameScanner::default()
            };
            report(&format!("{name}/watched_no_cache"), 1_000, || {
                no_cache_scanner.scan(black_box(&wire), &no_cache_watch);
                black_box(&no_cache_records);
            });
        }

        // Two live but idle streams pin both slots, while all traffic belongs to
        // a third. The cache can never fill for the hot stream, so this bounds
        // the worst case the fixed two-slot design admits (see
        // `FrameScanner::data_records`); it is the one shape where the cache is
        // pure overhead.
        let mut pinned = Vec::new();
        for _ in 0..1024 {
            pinned.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 5, b"x"));
        }
        let pinned_warmup = {
            let mut warmup = frame(FRAME_TYPE_DATA, 0, 1, b"x");
            warmup.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 3, b"x"));
            warmup
        };

        let pinned_watch = EndStreamWatch::default();
        let pinned_records = [1, 3, 5].map(|id| pinned_watch.register(id));
        let mut pinned_scanner = FrameScanner::default();
        pinned_scanner.scan(&pinned_warmup, &pinned_watch);
        report("pinned_slots_1024_frames/watched", 1_000, || {
            pinned_scanner.scan(black_box(&pinned), &pinned_watch);
            black_box(&pinned_records);
        });

        let pinned_no_cache_watch = EndStreamWatch::default();
        let pinned_no_cache_records = [1, 3, 5].map(|id| pinned_no_cache_watch.register(id));
        let mut pinned_no_cache_scanner = FrameScanner {
            data_cache_disabled: true,
            ..FrameScanner::default()
        };
        report("pinned_slots_1024_frames/watched_no_cache", 1_000, || {
            pinned_no_cache_scanner.scan(black_box(&pinned), &pinned_no_cache_watch);
            black_box(&pinned_no_cache_records);
        });

        // Lock contention is the cost this whole change exists to remove, so
        // both variants run against a thread churning registrations. Absolute
        // numbers here move with how much CPU the churn thread gets and are NOT
        // comparable across machines or runs; only the within-run ratio between
        // the two variants means anything.
        let mut streaming_open = Vec::new();
        for _ in 0..1024 {
            streaming_open.extend_from_slice(&frame(FRAME_TYPE_DATA, 0, 1, b"x"));
        }

        fn with_churn(watch: &Arc<EndStreamWatch>, mut run: impl FnMut()) {
            let keep_churning = Arc::new(AtomicBool::new(true));
            let churn_watch = watch.clone();
            let churn_flag = keep_churning.clone();
            let churner = std::thread::spawn(move || {
                let mut stream_id = 3u32;
                while churn_flag.load(Ordering::Acquire) {
                    let record = churn_watch.register(stream_id);
                    churn_watch.forget(stream_id);
                    black_box(record);
                    stream_id = stream_id.wrapping_add(2).max(3);
                }
            });
            run();
            keep_churning.store(false, Ordering::Release);
            churner.join().unwrap();
        }

        let contended_watch = Arc::new(EndStreamWatch::default());
        let contended_record = contended_watch.register(1);
        let mut contended_scanner = FrameScanner::default();
        with_churn(&contended_watch, || {
            report(
                "streaming_1024_frames/concurrent_register_forget",
                2_000,
                || {
                    contended_scanner.scan(black_box(&streaming_open), &contended_watch);
                    black_box(&contended_record);
                },
            );
        });

        let contended_no_cache_watch = Arc::new(EndStreamWatch::default());
        let contended_no_cache_record = contended_no_cache_watch.register(1);
        let mut contended_no_cache_scanner = FrameScanner {
            data_cache_disabled: true,
            ..FrameScanner::default()
        };
        with_churn(&contended_no_cache_watch, || {
            report(
                "streaming_1024_frames/concurrent_register_forget_no_cache",
                2_000,
                || {
                    contended_no_cache_scanner
                        .scan(black_box(&streaming_open), &contended_no_cache_watch);
                    black_box(&contended_no_cache_record);
                },
            );
        });
    }
}
