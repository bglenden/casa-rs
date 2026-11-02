// SPDX-License-Identifier: LGPL-3.0-or-later
//! What one plane's automatic mask holds ([`auto_multithresh`]) stays
//! within what mask materialization admits for it
//! ([`auto_multithresh_bytes`]).
//!
//! This binary's global allocator counts the live heap; the law measures
//! the most one call adds to it, on a square plane with an ordinary beam
//! and with a beam wider than the plane, and on a long, thin plane with a
//! beam wider than it. There a square smoothing kernel of the beam's ±5σ,
//! or of the plane's longer axis, would hold many times the plane. A
//! global allocator applies to this test binary alone.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_deconvolution::{
    AutoMaskBeam, AutoMaskCycle, AutoMultithreshControls, NoiseEstimate, auto_multithresh,
    auto_multithresh_bytes,
};
use casa_imaging_model::{ReconstructionAlgorithm, ReconstructionBasis, ReconstructionControls};

#[path = "support/synthetic_pass.rs"]
mod synthetic_pass;
use synthetic_pass::Scene;

#[path = "support/problems.rs"]
mod problems;
use problems::{empty_final_model, model_lifecycle, reconstruction_problem_of_shape};

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

/// CASA's auto-multithresh defaults for 12 m data, pruning nothing so a
/// beam wider than the plane still leaves regions to smooth.
fn controls() -> AutoMultithreshControls {
    AutoMultithreshControls {
        sidelobe_factor: 2.0,
        noise_factor: 4.25,
        low_noise_factor: 1.5,
        negative_factor: 0.0,
        minimum_beam_fraction: 0.0,
        smooth_factor: 1.0,
        cut_threshold: 0.01,
        grow_iterations: 75,
        grow_prune: true,
        minimum_percent_change: -1.0,
        noise: NoiseEstimate::Fast,
        verbose: false,
    }
}

/// The most one automatic mask of a plane of `shape` adds to the heap, with
/// a beam of `fwhm` pixels, and the pixels it masked.
fn measure(shape: [usize; 2], fwhm: f64) -> (u64, usize) {
    let problem = reconstruction_problem_of_shape(
        1,
        shape,
        1,
        1,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Hogbom,
        ReconstructionControls::new(0, 1.0, 0.0),
    );
    let lifecycle = model_lifecycle(&problem);
    let centre = [shape[0] / 2, shape[1] / 2];
    let completion = Scene::new(&problem)
        .with_point(centre, &[1.0])
        .with_point([shape[0] / 4, shape[1] / 4], &[0.6])
        .with_noise(0.01)
        .initial(&problem, empty_final_model(&lifecycle));
    let coordinate = problem.geometry().domains()[0].direction();
    let valid = vec![true; shape[0] * shape[1]];
    let cycle = AutoMaskCycle {
        completed_major_cycles: 1,
        threshold_reached: false,
        stopped: false,
    };
    let beam = AutoMaskBeam::new(fwhm, 0.6 * fwhm, 0.3, 0.1);
    PEAK.store(0, Ordering::Relaxed);
    BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let outcome = auto_multithresh(
        coordinate,
        completion.normal_state(),
        None,
        &valid,
        cycle,
        controls(),
        beam,
    );
    MEASURING.store(false, Ordering::Relaxed);
    let (mask, _) = outcome.expect("the automatic mask");
    let masked = mask.support().iter().filter(|set| **set).count();
    (PEAK.load(Ordering::Relaxed), masked)
}

#[test]
fn an_automatic_mask_holds_at_most_what_it_is_admitted() {
    let mut breaches = Vec::new();
    for (shape, fwhm) in [([48, 40], 3.0), ([48, 40], 4000.0), ([8, 512], 4000.0)] {
        let (peak, masked) = measure(shape, fwhm);
        assert!(masked > 0, "{shape:?}, beam {fwhm} px: a mask was smoothed");
        let admitted = auto_multithresh_bytes(shape);
        if peak > admitted {
            breaches.push(format!(
                "{shape:?}, beam {fwhm} px: held {peak} B, admitted {admitted} B"
            ));
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
