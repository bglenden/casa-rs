// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the gridder holds while it forms an image ([`TotalPowerGrid::new`],
//! [`TotalPowerGrid::grid`], [`TotalPowerGrid::finish`]) stays within what it
//! is admitted for ([`TotalPowerGrid::bytes`]).
//!
//! This binary's global allocator counts the live heap; the law measures the
//! most forming an image adds to it, for kernels of small and large support
//! (one pass over the channels and many), on one worker and on three. The
//! spectra, the kernel's table and the worker team are formed before the
//! measurement, as the caller forms them. A global allocator applies to this
//! test binary alone.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_runtime::Reservation;
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_singledish::{
    FlagMerge, GridFunction, GridShape, KernelTable, SpectrumBlock, TotalPowerGrid,
};
use ndarray::{Array2, Array3};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);
static BASELINE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

// SAFETY: every method forwards to `System` with the caller's arguments and
// only adds atomic bookkeeping, which allocates nothing.
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
                // Old and new bytes were both held while the block moved.
                observe(LIVE.load(Ordering::Relaxed) + new);
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

fn grow(bytes: u64) {
    observe(LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes);
}

fn observe(live: u64) {
    if MEASURING.load(Ordering::Relaxed) {
        PEAK.fetch_max(
            live.saturating_sub(BASELINE.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }
}

/// The most forming an image of `shape` from `spectra` spectra with
/// `function` on `workers` adds to the live heap, and what it is admitted
/// for.
fn measure(function: GridFunction, shape: GridShape, spectra: usize, workers: usize) -> (u64, u64) {
    let channels = shape.channels;
    let pixels: Vec<Option<[f64; 2]>> = (0..spectra)
        .map(|i| {
            let i = i as f64;
            Some([(i * 0.37) % shape.nx as f64, (i * 0.23) % shape.ny as f64])
        })
        .collect();
    let values = Array3::from_shape_fn((spectra, channels, 2), |(s, c, p)| {
        (s + c) as f32 * 0.01 + p as f32
    });
    let flags = Array3::from_shape_fn((spectra, channels, 2), |(s, c, _)| (s + c) % 17 == 0);
    let row_flags = vec![false; spectra];
    let weights = Array2::from_elem((spectra, channels), 1.5_f32);
    let channel_map: Vec<Option<usize>> = (0..channels).map(Some).collect();
    let block = SpectrumBlock {
        pixels: &pixels,
        values: values.view(),
        flags: flags.view(),
        row_flags: &row_flags,
        weights: weights.view(),
        channel_map: &channel_map,
        correlation_map: &[Some(0), Some(0)],
    };
    let kernel = KernelTable::new(function).expect("kernel");
    let admitted = TotalPowerGrid::bytes(shape, &kernel, channels);
    let team = WorkerTeam::new(workers).expect("team");
    PEAK.store(0, Ordering::Relaxed);
    BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let image = TotalPowerGrid::new(
        shape,
        kernel,
        FlagMerge::AnyCorrelation,
        Reservation::none(),
    )
    .and_then(|mut grid| {
        grid.grid(&block, &team)?;
        grid.grid(&block, &team)?;
        Ok(grid.finish().value)
    });
    MEASURING.store(false, Ordering::Relaxed);
    let image = image.expect("gridded");
    assert!(image.weight.iter().any(|&w| w > 0.0), "the spectra gridded");
    (PEAK.load(Ordering::Relaxed), admitted)
}

#[test]
fn forming_an_image_holds_at_most_what_it_is_admitted() {
    let shape = GridShape {
        nx: 97,
        ny: 80,
        planes: 1,
        channels: 5,
    };
    let mut breaches = Vec::new();
    for (name, function, spectra) in [
        (
            "SF of 6, one pass",
            GridFunction::Spheroidal { support: 6 },
            2_000,
        ),
        (
            "SF of 6, two passes",
            GridFunction::Spheroidal { support: 6 },
            4_000,
        ),
        (
            "GAUSS of 20, many passes",
            GridFunction::Gauss {
                hwhm: 6.0,
                truncate: 20.0,
            },
            1_500,
        ),
    ] {
        for workers in [1, 3] {
            let (measured, admitted) = measure(function, shape, spectra, workers);
            eprintln!("{name} on {workers}: {measured} B measured, {admitted} B admitted");
            if measured > admitted {
                breaches.push(format!(
                    "{name} on {workers}: {measured} B over {admitted} B"
                ));
            }
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
