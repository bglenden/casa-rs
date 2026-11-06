// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cloning an immutable observation snapshot allocates no source copies.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;
struct CountingHeap;
static TRACK: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

fn count() {
    if TRACK.load(Ordering::Relaxed) {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
    }
}

// SAFETY: every operation delegates to System unchanged; counting uses
// atomics and cannot allocate.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() {
            count();
        }
        result
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc_zeroed(layout) };
        if !result.is_null() {
            count();
        }
        result
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            count();
        }
        result
    }
}

#[test]
fn cloning_a_snapshot_does_not_allocate() {
    let original = common::observation_snapshot(1);
    ALLOCS.store(0, Ordering::Relaxed);
    TRACK.store(true, Ordering::Relaxed);
    let cloned = original.clone();
    TRACK.store(false, Ordering::Relaxed);
    assert_eq!(
        ALLOCS.load(Ordering::Relaxed),
        0,
        "a snapshot clone allocated a source copy"
    );
    assert!(std::ptr::eq(
        original.sources().as_ptr(),
        cloned.sources().as_ptr()
    ));
}
