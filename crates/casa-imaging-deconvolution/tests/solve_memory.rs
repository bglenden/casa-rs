// SPDX-License-Identifier: LGPL-3.0-or-later
//! What one plane's minor cycle holds ([`run_plane`]) stays within what the
//! minor cycle admits for it ([`solve_bytes`]).
//!
//! This binary's global allocator counts the live heap; the law measures
//! the most a solve adds to it, for every solver, on two plane shapes, over
//! a sky of points and broad sources on which multiscale and Taylor
//! components add flux to many pixels each. A global allocator applies to
//! this test binary alone.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_deconvolution::{
    Clark, CycleControls, Hogbom, MinorCycleView, Multiscale, PlaneShape, PlaneStatistics,
    PsfSummary, Solver, Support, Taylor, run_plane, solve_bytes,
};

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

/// One measurement at a time: the allocator is the process's.
static MEASUREMENT: Mutex<()> = Mutex::new(());

/// A Gaussian of `width` pixels and unit peak at `at` on `shape`.
fn gaussian(shape: PlaneShape, at: [f64; 2], width: f64) -> Vec<f64> {
    (0..shape.len())
        .map(|index| {
            let [x, y] = shape.pixel(index);
            let r2 = (x as f64 - at[0]).powi(2) + (y as f64 - at[1]).powi(2);
            (-r2 / (2.0 * width * width)).exp()
        })
        .collect()
}

/// A sky of two points and two broad sources seen through a beam of two
/// pixels: the residual, and the PSF centred on the plane.
fn sky(shape: PlaneShape) -> (Vec<f64>, Vec<f64>) {
    let (nx, ny) = (shape.nx as f64, shape.ny as f64);
    let mut residual = vec![0.0; shape.len()];
    for (at, width, flux) in [
        ([0.3 * nx, 0.4 * ny], 2.0, 1.0),
        ([0.7 * nx, 0.6 * ny], 2.0, 0.6),
        ([0.5 * nx, 0.3 * ny], 9.0, 0.8),
        ([0.35 * nx, 0.7 * ny], 14.0, 0.5),
    ] {
        for (value, source) in residual.iter_mut().zip(gaussian(shape, at, width)) {
            *value += flux * source;
        }
    }
    (residual, psf(shape, 0))
}

/// PSF term `term` of a Taylor plane, centred on the plane, with peaks
/// 1, 0.1 and 0.05, so that their Hessian inverts.
fn psf(shape: PlaneShape, term: usize) -> Vec<f64> {
    let centre = shape.centre().map(|centre| centre as f64);
    let peak = [1.0, 0.1, 0.05][term];
    gaussian(shape, centre, 2.0)
        .into_iter()
        .map(|value| value * peak)
        .collect()
}

/// The most `solver` adds to the live heap solving a plane of `shape` for
/// `iterations` components, and what the minor cycle admits for it.
fn measure<S: Solver>(
    solver: &S,
    shape: PlaneShape,
    terms: usize,
    iterations: usize,
) -> (u64, u64) {
    const TRACE: usize = 64;
    let (residual, _) = sky(shape);
    // A Taylor plane's higher residual terms: the residual weighted down.
    let residual = (0..terms)
        .map(|term| {
            residual
                .iter()
                .map(|value| value * 0.5_f64.powi(term as i32))
                .collect()
        })
        .collect::<Vec<Vec<f64>>>();
    let psf = (0..2 * terms - 1)
        .map(|term| psf(shape, term))
        .collect::<Vec<_>>();
    let summary = PsfSummary::new(&psf[0], shape).expect("a PSF summary");
    let support = Support::full(shape);
    let statistics = PlaneStatistics::measure(&residual[0], &support, &support, 0.0, false);
    let view = MinorCycleView {
        shape,
        residual: &residual,
        psf: &psf,
        summary: &summary,
        support: &support,
    };
    let cycle = CycleControls {
        iterations,
        threshold: 0.0,
        threshold_reached: true,
        gain: 0.1,
        nsigma: 0.0,
    };
    PEAK.store(0, Ordering::Relaxed);
    BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let outcome = run_plane(solver, &view, &cycle, &statistics, TRACE);
    MEASURING.store(false, Ordering::Relaxed);
    let outcome = outcome.expect("the plane solves");
    assert!(outcome.components > 0, "the solve cleaned");
    (
        PEAK.load(Ordering::Relaxed),
        solve_bytes(solver, shape, terms, iterations + 1, TRACE),
    )
}

#[test]
fn a_plane_solve_holds_at_most_what_it_is_admitted() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let mut breaches = Vec::new();
    for shape in [
        PlaneShape { nx: 97, ny: 80 },
        PlaneShape { nx: 256, ny: 256 },
    ] {
        let measured = [
            ("hogbom", measure(&Hogbom::new(false), shape, 1, 300)),
            ("clark", measure(&Clark::new(None), shape, 1, 300)),
            (
                "multiscale",
                measure(&Multiscale::new(vec![0.0, 6.0, 24.0], 0.6), shape, 1, 300),
            ),
            (
                "taylor",
                measure(&Taylor::new(2, vec![0.0], 0.6), shape, 2, 300),
            ),
            (
                "multiscale taylor",
                measure(&Taylor::new(2, vec![0.0, 6.0, 24.0], 0.6), shape, 2, 300),
            ),
        ];
        for (label, (held, admitted)) in measured {
            eprintln!(
                "{label} {}×{}: held {held} bytes, admitted {admitted}",
                shape.nx, shape.ny
            );
            if held > admitted {
                breaches.push(format!(
                    "{label} {}×{}: held {held} bytes, admitted {admitted}",
                    shape.nx, shape.ny
                ));
            }
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
