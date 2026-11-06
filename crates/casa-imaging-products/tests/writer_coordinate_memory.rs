// SPDX-License-Identifier: LGPL-3.0-or-later
//! Binding a second product family must not clone the first family's WCS.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use casa_coordinates::{CoordinateSystem, SpectralCoordinate};
use casa_imaging_model::ImageDomainRole;
use casa_imaging_products::CasaImageWriter;
use casa_types::measures::frequency::FrequencyRef;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

struct CountingHeap;
static TRACK: AtomicBool = AtomicBool::new(false);
static LARGE_ALLOCS: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
const LARGE: usize = 1 << 20;

fn count(size: usize) {
    if TRACK.load(Ordering::Relaxed) {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        if size >= LARGE {
            LARGE_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// SAFETY: allocation and deallocation are delegated unchanged to System;
// counting uses atomics and cannot allocate.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            count(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            count(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            count(size);
        }
        result
    }
}

#[test]
fn alternate_coordinates_do_not_clone_the_existing_coordinate_system() {
    let spectral = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1e9)
        .with_rest_frequencies(vec![1e9; 1 << 18]);
    let mut original = CoordinateSystem::new();
    original.add_coordinate(spectral);
    let writer = CasaImageWriter::new("mvc-memory-law".into(), original);
    let alternate = CoordinateSystem::new();
    let bindings = vec![(ImageDomainRole::Main, alternate)];
    LARGE_ALLOCS.store(0, Ordering::Relaxed);
    ALLOCS.store(0, Ordering::Relaxed);
    TRACK.store(true, Ordering::Relaxed);
    let rebound = writer.with_coordinates(bindings);
    TRACK.store(false, Ordering::Relaxed);
    assert_eq!(
        ALLOCS.load(Ordering::Relaxed),
        0,
        "rebinding allocated new output metadata"
    );
    assert_eq!(
        LARGE_ALLOCS.load(Ordering::Relaxed),
        0,
        "rebinding cloned the old coordinate system's large rest-frequency array"
    );
    drop(rebound);
    drop(writer);
}
