// Copyright 2026 Cloudflare, Inc.
// SPDX-License-Identifier: Apache-2.0

// Frozen scanner oracle from Pingora e18250efb07a66439d04f0e03df73c3364c23af3.
// Only test visibility and cached_ids were added. Keep parsing and scanner
// helpers independent of the production payload-state implementation.
// Watch primitives and constants are shared because this refactor leaves them unchanged.
use super::*;

/// Incremental HTTP/2 frame-header scanner.
///
/// The server-to-client direction of an HTTP/2 connection is a pure frame
/// stream from its first byte (only the client sends a connection preface), so
/// this can start parsing immediately. Payloads are skipped by length, which
/// makes the cost per read O(frames), not O(bytes).
#[derive(Debug, Default)]
pub(super) struct FrameScanner {
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
    pub(super) fn has_partial_frame(&self) -> bool {
        self.header_len != 0
            || self.payload_left != 0
            || self.goaway.is_some()
            || self.padded_data.is_some()
            || self.terminal_data.is_some()
    }

    pub(super) fn poison(&mut self, watch: &EndStreamWatch) {
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

    pub(super) fn scan(&mut self, mut bytes: &[u8], watch: &EndStreamWatch) {
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

impl FrameScanner {
    pub(super) fn cached_ids(&self) -> Vec<u32> {
        self.data_records
            .iter()
            .flatten()
            .map(|entry| entry.stream_id)
            .collect()
    }
}
