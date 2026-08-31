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

use async_trait::async_trait;
use pingora_error::Result;

    struct OversizedReplayBuffer;

    #[async_trait]
    impl RequestBodyBuffer for OversizedReplayBuffer {
        async fn write(&mut self, _data: &Bytes) -> Result<()> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        async fn rewind(&mut self) -> Result<()> {
            Ok(())
        }

        async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
            Ok(Some(Bytes::from(vec![0; max_bytes + 1])))
        }

        fn consume(&mut self, _bytes: usize) {}
    }

    #[tokio::test]
    async fn in_memory_captures_and_is_rewindable() {
        let mut b = InMemoryRequestBodyBuffer::new();
        b.write(&Bytes::from_static(b"hello ")).await.unwrap();
        b.write(&Bytes::from_static(b"world")).await.unwrap();
        b.finish().await.unwrap();
        let chunk = b
            .next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(chunk, Bytes::from_static(b"hello world"));
        // next_chunk is a pure peek: repeated calls re-serve the same chunk
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            Some(chunk.clone())
        );
        b.consume(chunk.len());
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            None
        );
        b.rewind().await.unwrap();
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            Some(Bytes::from_static(b"hello world"))
        );
    }

    #[tokio::test]
    async fn in_memory_unfinalized_replay_fails_closed() {
        // Data written but finish() never called: replay must error, not EOF.
        let mut b = InMemoryRequestBodyBuffer::new();
        b.write(&Bytes::from_static(b"payload")).await.unwrap();
        assert!(b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.is_err());
        // A brand-new buffer without finish() must also fail closed.
        let mut fresh = InMemoryRequestBodyBuffer::new();
        assert!(fresh
            .next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn in_memory_finalized_empty_body_replays_eof() {
        let mut b = InMemoryRequestBodyBuffer::new();
        b.finish().await.unwrap();
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn in_memory_replays_in_bounded_chunks() {
        let mut b = InMemoryRequestBodyBuffer::new();
        let body = Bytes::from(vec![b'x'; REQUEST_BODY_REPLAY_CHUNK_SIZE + 1]);
        b.write(&body).await.unwrap();
        b.finish().await.unwrap();
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
                .await
                .unwrap()
                .unwrap()
                .len(),
            REQUEST_BODY_REPLAY_CHUNK_SIZE
        );
        b.consume(REQUEST_BODY_REPLAY_CHUNK_SIZE);
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE)
                .await
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        b.consume(1);
        assert_eq!(
            b.next_chunk(REQUEST_BODY_REPLAY_CHUNK_SIZE).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn registered_buffer_rejects_oversized_replay_chunk() {
        let mut b = RegisteredRequestBodyBuffer::new(Box::new(OversizedReplayBuffer));
        b.finish_capture().await.unwrap();
        b.begin_replay().await.unwrap();
        assert!(b.next_chunk().await.is_err());
        // A replay error leaves the state as Replaying: the proxy pump relies
        // on this to attribute the surfaced error to the buffer (internal)
        // rather than the client connection (downstream).
        assert!(b.is_replaying());
    }

    /// A peek impl that awaits (yields) before reading at the cursor, like a
    /// disk-spill impl awaiting file I/O. Used to open a cancellation window
    /// inside next_chunk.
    struct YieldingReplayBuffer {
        body: Bytes,
        offset: usize,
    }

    #[async_trait]
    impl RequestBodyBuffer for YieldingReplayBuffer {
        async fn write(&mut self, _data: &Bytes) -> Result<()> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        async fn rewind(&mut self) -> Result<()> {
            self.offset = 0;
            Ok(())
        }

        async fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>> {
            tokio::task::yield_now().await;
            if self.offset >= self.body.len() {
                return Ok(None);
            }
            let end = self.offset.saturating_add(max_bytes).min(self.body.len());
            Ok(Some(self.body.slice(self.offset..end)))
        }

        fn consume(&mut self, bytes: usize) {
            self.offset = self.offset.saturating_add(bytes);
        }
    }

    fn replaying_yielding_buffer() -> RegisteredRequestBodyBuffer {
        RegisteredRequestBodyBuffer::new(Box::new(YieldingReplayBuffer {
            body: Bytes::from_static(b"chunk-1"),
            offset: 0,
        }))
    }

    #[tokio::test]
    async fn cancelled_next_chunk_re_serves_the_same_chunk() {
        let mut b = replaying_yielding_buffer();
        b.finish_capture().await.unwrap();
        b.begin_replay().await.unwrap();
        // Poll once (suspends at the impl's internal await), then drop the
        // future — exactly what a losing tokio::select! branch does.
        {
            let mut fut = tokio_test::task::spawn(b.next_chunk());
            assert!(fut.poll().is_pending());
        }
        // The cancelled call must not have consumed anything.
        assert_eq!(
            b.next_chunk().await.unwrap(),
            Some(Bytes::from_static(b"chunk-1"))
        );
        assert_eq!(b.next_chunk().await.unwrap(), None);
    }

    #[tokio::test]
    async fn cancellation_after_delivery_neither_duplicates_nor_skips() {
        let body = Bytes::from(vec![b'x'; REQUEST_BODY_REPLAY_CHUNK_SIZE + 3]);
        let mut b = RegisteredRequestBodyBuffer::new(Box::new(YieldingReplayBuffer {
            body: body.clone(),
            offset: 0,
        }));
        b.finish_capture().await.unwrap();
        b.begin_replay().await.unwrap();
        let c1 = b.next_chunk().await.unwrap().unwrap();
        assert_eq!(c1.len(), REQUEST_BODY_REPLAY_CHUNK_SIZE);
        // This call commits c1 at entry (synchronously) and is then cancelled
        // while peeking c2 — the commit must stick, the peek must not.
        {
            let mut fut = tokio_test::task::spawn(b.next_chunk());
            assert!(fut.poll().is_pending());
        }
        let c2 = b.next_chunk().await.unwrap().unwrap();
        assert_eq!(c2.len(), 3);
        assert_eq!([c1, c2].concat(), body);
        assert_eq!(b.next_chunk().await.unwrap(), None);
    }

    #[tokio::test]
    async fn rewind_discards_pending_commit() {
        let mut b = replaying_yielding_buffer();
        b.finish_capture().await.unwrap();
        b.begin_replay().await.unwrap();
        // Deliver the chunk; its consumption is now pending.
        assert_eq!(
            b.next_chunk().await.unwrap(),
            Some(Bytes::from_static(b"chunk-1"))
        );
        // A retry rewinds; the pending commit must be discarded or the retry
        // would skip the first chunk.
        b.begin_replay().await.unwrap();
        assert_eq!(
            b.next_chunk().await.unwrap(),
            Some(Bytes::from_static(b"chunk-1"))
        );
        assert_eq!(b.next_chunk().await.unwrap(), None);
    }
