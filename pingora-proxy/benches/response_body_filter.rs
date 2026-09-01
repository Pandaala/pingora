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

//! Microbenchmark for the per-chunk response-body event dispatch cost.
//!
//! Run with:
//! `cargo bench -p pingora-proxy --bench response_body_filter`
//!
//! Set `CARGO_PROFILE_BENCH_LTO=true` to repeat with LTO. Iteration counts can
//! be changed with `PINGORA_BENCH_ITERS` and `PINGORA_BENCH_YIELD_ITERS`.

use async_trait::async_trait;
use bytes::Bytes;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use pingora_proxy::{ProxyHttp, ResponseBodySink, Session, UpstreamResponseBodyEvent};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: the allocation is forwarded with the caller-provided layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: the pointer and layout came from the matching allocator call.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    allocations: u64,
    deallocations: u64,
    allocated_bytes: u64,
}

impl AllocationSnapshot {
    fn capture() -> Self {
        Self {
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            deallocations: DEALLOCATIONS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            allocations: self.allocations - earlier.allocations,
            deallocations: self.deallocations - earlier.deallocations,
            allocated_bytes: self.allocated_bytes - earlier.allocated_bytes,
        }
    }
}

struct DefaultsOnly;
struct YieldingFilter;

#[async_trait]
impl ProxyHttp for DefaultsOnly {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("the benchmark calls only the response-body hook")
    }
}

#[async_trait]
impl ProxyHttp for YieldingFilter {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        unreachable!("the benchmark calls only the response-body hook")
    }

    async fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _sink: &mut ResponseBodySink,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        tokio::task::yield_now().await;
        Ok(None)
    }
}

#[inline]
fn synchronous_noop(
    _session: &mut Session,
    _body: &mut Option<Bytes>,
    _end_of_stream: bool,
    _sink: &mut ResponseBodySink,
    _ctx: &mut (),
) -> Result<Option<Duration>> {
    Ok(None)
}

#[derive(Clone, Copy)]
struct Measurement {
    elapsed: Duration,
    allocations: AllocationSnapshot,
    iterations: usize,
}

impl Measurement {
    fn print(self, case: &str, chunk_size: usize) {
        let iterations = self.iterations as f64;
        println!(
            "{case:<14} {chunk_size:>10} {iterations:>12.0} {ns:>12.2} {allocs:>12.4} {bytes:>12.2} {deallocs:>12.4}",
            ns = self.elapsed.as_nanos() as f64 / iterations,
            allocs = self.allocations.allocations as f64 / iterations,
            bytes = self.allocations.allocated_bytes as f64 / iterations,
            deallocs = self.allocations.deallocations as f64 / iterations,
        );
    }
}

fn mock_session() -> Session {
    Session::new_h1(Box::new(tokio_test::io::Builder::new().build()))
}

fn measure_sync(iterations: usize, chunk_size: usize) -> Measurement {
    let mut session = mock_session();
    let mut body = Some(Bytes::from(vec![0; chunk_size]));
    let mut sink = ResponseBodySink::new();
    let mut ctx = ();

    for _ in 0..1_024 {
        black_box(synchronous_noop(&mut session, &mut body, false, &mut sink, &mut ctx).unwrap());
    }

    let started = Instant::now();
    for _ in 0..iterations {
        black_box(synchronous_noop(&mut session, &mut body, false, &mut sink, &mut ctx).unwrap());
    }
    let elapsed = started.elapsed();

    // Keep counter RMW operations out of the timing loop. The allocator wrapper
    // remains active, but does only one relaxed flag load while timing.
    let before = AllocationSnapshot::capture();
    COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
    for _ in 0..iterations {
        black_box(synchronous_noop(&mut session, &mut body, false, &mut sink, &mut ctx).unwrap());
    }
    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocations = AllocationSnapshot::capture().since(before);
    Measurement {
        elapsed,
        allocations,
        iterations,
    }
}

async fn measure_default_event(iterations: usize, chunk_size: usize) -> Measurement {
    let filter = DefaultsOnly;
    let mut session = mock_session();
    let mut body = Some(Bytes::from(vec![0; chunk_size]));
    let mut sink = ResponseBodySink::new();
    let mut ctx = ();

    for _ in 0..1_024 {
        black_box(
            filter
                .upstream_response_body_filter_event(
                    &mut session,
                    &mut body,
                    UpstreamResponseBodyEvent::Data {
                        end_of_stream: false,
                    },
                    &mut sink,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }

    let started = Instant::now();
    for _ in 0..iterations {
        black_box(
            filter
                .upstream_response_body_filter_event(
                    &mut session,
                    &mut body,
                    UpstreamResponseBodyEvent::Data {
                        end_of_stream: false,
                    },
                    &mut sink,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }
    let elapsed = started.elapsed();

    let before = AllocationSnapshot::capture();
    COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
    for _ in 0..iterations {
        black_box(
            filter
                .upstream_response_body_filter_event(
                    &mut session,
                    &mut body,
                    UpstreamResponseBodyEvent::Data {
                        end_of_stream: false,
                    },
                    &mut sink,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }
    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocations = AllocationSnapshot::capture().since(before);
    Measurement {
        elapsed,
        allocations,
        iterations,
    }
}

async fn measure_yielding(iterations: usize, chunk_size: usize) -> Measurement {
    let filter = YieldingFilter;
    let mut session = mock_session();
    let mut body = Some(Bytes::from(vec![0; chunk_size]));
    let mut sink = ResponseBodySink::new();
    let mut ctx = ();

    for _ in 0..128 {
        black_box(
            filter
                .upstream_response_body_filter_event(
                    &mut session,
                    &mut body,
                    UpstreamResponseBodyEvent::Data {
                        end_of_stream: false,
                    },
                    &mut sink,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }

    let started = Instant::now();
    for _ in 0..iterations {
        black_box(
            filter
                .upstream_response_body_filter_event(
                    &mut session,
                    &mut body,
                    UpstreamResponseBodyEvent::Data {
                        end_of_stream: false,
                    },
                    &mut sink,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }
    let elapsed = started.elapsed();

    let before = AllocationSnapshot::capture();
    COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
    for _ in 0..iterations {
        black_box(
            filter
                .upstream_response_body_filter_event(
                    &mut session,
                    &mut body,
                    UpstreamResponseBodyEvent::Data {
                        end_of_stream: false,
                    },
                    &mut sink,
                    &mut ctx,
                )
                .await
                .unwrap(),
        );
    }
    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocations = AllocationSnapshot::capture().since(before);
    Measurement {
        elapsed,
        allocations,
        iterations,
    }
}

fn configured_iterations(variable: &str, default: usize) -> usize {
    std::env::var(variable)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|iterations| *iterations > 0)
        .unwrap_or(default)
}

fn main() {
    let iterations = configured_iterations("PINGORA_BENCH_ITERS", 250_000);
    let yield_iterations = configured_iterations("PINGORA_BENCH_YIELD_ITERS", 25_000);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    println!(
        "{:<14} {:>10} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "case", "chunk", "iterations", "ns/call", "allocs/call", "bytes/call", "frees/call"
    );
    for chunk_size in [1_024, 64 * 1_024] {
        let sync = measure_sync(iterations, chunk_size);
        sync.print("sync_noop", chunk_size);
        assert_eq!(sync.allocations.allocations, 0);

        let default = runtime.block_on(measure_default_event(iterations, chunk_size));
        default.print("default_event", chunk_size);
        assert_eq!(
            default.allocations.allocations, 0,
            "the default typed response-body event must stay allocation-free"
        );
        assert_eq!(default.allocations.allocated_bytes, 0);

        let yielding = runtime.block_on(measure_yielding(yield_iterations, chunk_size));
        yielding.print("yielding_event", chunk_size);
        assert_eq!(
            yielding.allocations.allocations, yield_iterations as u64,
            "the real async override must still construct one future per event"
        );
    }
}
