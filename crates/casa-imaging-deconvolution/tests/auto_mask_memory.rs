// SPDX-License-Identifier: LGPL-3.0-or-later
//! What one plane's automatic mask holds ([`auto_multithresh`]) stays
//! within what mask materialization admits for it
//! ([`auto_multithresh_bytes`]).
//!
//! This binary's global allocator counts the live heap; the law measures
//! the most one call adds to it, on a square plane with an ordinary beam
//! and with a beam wider than the plane, and on a long, thin plane with a
//! beam wider than it. There a square smoothing kernel of the beam's ±5σ,
//! or of the plane's longer axis, would hold many times the plane. It also
//! holds materializing a cube's mask, every channel's, to its charge
//! ([`ImageDomainReconstructionMaskPlans::materialize_bytes`]). A global
//! allocator applies to this test binary alone.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_deconvolution::{
    AutoMaskBeam, AutoMaskCycle, AutoMaskPlane, AutoMaskSource, AutoMultithreshControls,
    ImageDomainReconstructionMaskPlans, MaskError, MaskPlaneKey, NoiseEstimate, PlaneShape,
    PsfSummary, ReconstructionMaskPlan, auto_multithresh, auto_multithresh_bytes,
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
    // The residual in the units the minor cycle cleans, as it reads it.
    let normal = completion.normal_state();
    let read = normal
        .read_plane(0, normal.slab().core_range().start, 0)
        .expect("the plane");
    let sum_weight = read.sum_weight();
    let plane = AutoMaskPlane {
        residual: read
            .read_residual()
            .expect("the residual")
            .iter()
            .map(|value| value.re / sum_weight)
            .collect(),
        valid: vec![true; shape[0] * shape[1]],
    };
    let inherited = vec![false; shape[0] * shape[1]];
    let cycle = AutoMaskCycle {
        completed_major_cycles: 1,
        threshold_reached: false,
        stopped: false,
    };
    let beam = AutoMaskBeam::new(fwhm, 0.6 * fwhm, 0.3, 0.1);
    PEAK.store(0, Ordering::Relaxed);
    BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let outcome = auto_multithresh(&plane, &inherited, shape, cycle, &controls(), beam);
    MEASURING.store(false, Ordering::Relaxed);
    let (support, _) = outcome.expect("the automatic mask");
    let masked = support.iter().filter(|set| **set).count();
    (PEAK.load(Ordering::Relaxed), masked)
}

#[test]
fn an_automatic_mask_holds_at_most_what_it_is_admitted() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
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

/// One measurement at a time: the allocator is the process's.
static MEASUREMENT: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A Gaussian PSF of `fwhm` pixels on a plane of `shape`, x-major, peaked
/// at the plane's centre.
fn gaussian_psf(shape: [usize; 2], fwhm: f64) -> Vec<f64> {
    let sigma = fwhm / 2.354_820_045_030_949_3;
    let centre = [shape[0] / 2, shape[1] / 2].map(|c| c as f64);
    (0..shape[0] * shape[1])
        .map(|index| {
            let [x, y] = [(index / shape[1]) as f64, (index % shape[1]) as f64];
            let r2 = (x - centre[0]).powi(2) + (y - centre[1]).powi(2);
            (-0.5 * r2 / (sigma * sigma)).exp()
        })
        .collect()
}

/// Materializing an automatic mask of every channel of a cube holds at most
/// [`ImageDomainReconstructionMaskPlans::materialize_bytes`]: the new mask
/// and its rows, every plane's PSF measurements, and on each worker one
/// plane's read and working planes. Each channel writes its planes straight
/// into the new mask; a block of 240 channels of 16 by 16 pixels once
/// gathered every channel's planes beside it, a second mask uncharged. On
/// one worker and on four, at the first boundary and the next.
#[test]
fn materializing_every_channel_holds_at_most_its_charge() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let (width, channels) = (16, 240);
    let shape = [width, width];
    let problem = problems::reconstruction_problem(
        3,
        width,
        channels,
        ReconstructionBasis::ChannelLocal { channels },
        ReconstructionAlgorithm::Hogbom,
        ReconstructionControls::new(0, 1.0, 0.0),
    );
    let lifecycle = model_lifecycle(&problem);
    let completion = Scene::new(&problem)
        .with_point([6, 9], &vec![1.0; channels])
        .initial(&problem, empty_final_model(&lifecycle));
    let normal = completion.normal_state();
    let summary = PsfSummary::new(&gaussian_psf(shape, 3.0), PlaneShape::new(width, width))
        .expect("a fitted Gaussian PSF");
    // A point and faint noise on every plane, made as the plane is read.
    let read = |key: MaskPlaneKey| {
        let mut state = (key.channel as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let mut residual = (0..width * width)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((state >> 11) as f64 / (1_u64 << 53) as f64 - 0.5) * 0.02
            })
            .collect::<Vec<_>>();
        residual[6 * width + 9] += 1.0;
        Ok::<_, MaskError>(Some(AutoMaskPlane {
            residual,
            valid: vec![true; width * width],
        }))
    };
    let psf = |_: MaskPlaneKey| Some(summary);
    let mut breaches = Vec::new();
    for workers in [1, 4] {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .expect("a pool");
        let source = AutoMaskSource {
            read: &read,
            psf: &psf,
            workers,
        };
        let mut plans =
            ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::AutoMultithresh {
                coordinate: problem.geometry().domains()[0].direction(),
                controls: controls(),
                completed_major_cycles: 0,
                cycle_threshold_reached: false,
                previous: None,
                stopped_channels: Box::new([]),
            }])
            .expect("an automatic mask");
        for boundary in 0..2 {
            let admitted = plans.materialize_bytes(normal, workers);
            PEAK.store(0, Ordering::Relaxed);
            BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
            MEASURING.store(true, Ordering::Relaxed);
            let made =
                pool.install(|| plans.materialize(completion.final_model(), normal, Some(&source)));
            MEASURING.store(false, Ordering::Relaxed);
            let (masks, evidence) = made.expect("the masks").into_parts();
            let peak = PEAK.load(Ordering::Relaxed);
            if peak > admitted {
                breaches.push(format!(
                    "{workers} workers, boundary {boundary}: held {peak} B, admitted {admitted} B"
                ));
            }
            let mask = masks.primary();
            assert_eq!(mask.planes().count(), channels);
            for channel in 0..channels {
                assert!(
                    mask.plane(channel, 0).expect("a plane")[6 * width + 9],
                    "channel {channel} masks its point"
                );
            }
            plans = plans
                .next_cycle(&masks, &evidence, boundary + 1, false)
                .expect("the next boundary");
        }
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}
