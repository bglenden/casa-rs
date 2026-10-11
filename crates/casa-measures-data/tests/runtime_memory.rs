// SPDX-License-Identifier: LGPL-3.0-or-later
//! Loading the measures runtime's catalogs holds at most
//! [`MeasuresRuntime::LOAD_BYTES`] of heap, so a caller can admit it before
//! the first lookup loads them, and keeps at most the residency the runtime
//! reports for them.
//!
//! This binary's global allocator counts the live heap.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

use casa_measures_data::{MeasuresRuntime, RuntimePolicy};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their largest value.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: forwards to `System` with the caller's arguments and adds atomic
// bookkeeping only.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`.
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (old, new) = (layout.size() as u64, new_size as u64);
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if moved != pointer {
                // Old and new were held at once while the block moved.
                PEAK.fetch_max(LIVE.load(Ordering::Relaxed) + new, Ordering::Relaxed);
            }
            if new >= old {
                grow(new - old);
            } else {
                LIVE.fetch_sub(old - new, Ordering::Relaxed);
            }
        }
        moved
    }
}

#[test]
fn loading_every_catalog_holds_at_most_the_load_bytes() {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let runtime = MeasuresRuntime::open_discovered(RuntimePolicy::default()).expect("runtime");
    let state = runtime.prepare_bounded_state().expect("catalogs");
    let peak = PEAK.load(Ordering::Relaxed) - baseline;
    let held = LIVE.load(Ordering::Relaxed) - baseline;
    eprintln!(
        "peak {peak} bytes, held {held} bytes, reported residency {} bytes",
        state.retained_heap_bytes()
    );
    assert!(
        peak <= MeasuresRuntime::LOAD_BYTES as u64,
        "loading held {peak} bytes, more than {}",
        MeasuresRuntime::LOAD_BYTES
    );
    assert!(
        held <= MeasuresRuntime::LOAD_BYTES as u64,
        "the loaded runtime holds {held} bytes, more than {}",
        MeasuresRuntime::LOAD_BYTES
    );
}
