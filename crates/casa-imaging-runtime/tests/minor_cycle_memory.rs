// SPDX-License-Identifier: LGPL-3.0-or-later
//! A minor cycle holds no more than it is admitted, however many planes it
//! cleans: its solves under [`run_bytes`], and each solved plane's terms
//! under the charge it admits as the plane finishes ([`result_bytes`]),
//! which the outcome hands on.
//!
//! This binary's global allocator counts the live heap; at every
//! allocation of a measured cycle it compares the bytes the cycle holds
//! beyond what was live when it started with the memory admitted since.
//! A cube of `n` channels keeps `n` planes' terms, so a charge that does
//! not grow with the planes fails here (Astra's probe on #700: 7,472 bytes
//! held at one channel and 522,792 at 512 against 21,184 admitted at
//! every count). A global allocator applies to this test binary alone.

#[path = "../../casa-imaging-deconvolution/tests/support/problems.rs"]
mod problems;
#[path = "../../casa-imaging-deconvolution/tests/support/synthetic_pass.rs"]
mod synthetic_pass;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use casa_imaging_deconvolution::{AutoMultithreshControls, CycleControls};
use casa_imaging_deconvolution::{
    ImageDomainReconstructionMaskPlans, MajorCycle, ReconstructionMaskPlan,
};
use casa_imaging_model::{
    HogbomIterationAccounting, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionControls,
};
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{
    Demand, HostResources, MinorCycleSetup, PsfCache, ResourcePolicy, admit, free_memory,
    prepare_bytes, prepare_minor_cycle, psf_cache_bytes, result_bytes, run_bytes, run_minor_cycle,
};
use problems::{empty_final_model, model_lifecycle, reconstruction_problem};
use synthetic_pass::{BLOCKS, SAMPLES, Scene};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);
/// `LIVE` when the measured cycle started.
static BASELINE: AtomicU64 = AtomicU64::new(0);
/// What the process held admitted when the measured cycle started.
static ADMITTED: AtomicU64 = AtomicU64::new(0);
/// The largest `LIVE − BASELINE − admitted since`.
static EXCESS: AtomicI64 = AtomicI64::new(i64::MIN);

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
    if !MEASURING.load(Ordering::Relaxed) {
        return;
    }
    let used = live as i64 - BASELINE.load(Ordering::Relaxed) as i64;
    let admitted = held() as i64 - ADMITTED.load(Ordering::Relaxed) as i64;
    EXCESS.fetch_max(used - admitted, Ordering::Relaxed);
}

/// A host and policy whose ceiling no cycle here reaches.
const HOST: HostResources = HostResources {
    threads: 8,
    performance_cores: 8,
    available_memory: 1 << 40,
    metal: false,
};
const POLICY: ResourcePolicy = ResourcePolicy::Explicit {
    workers: 1,
    memory: 1 << 40,
};

/// Bytes the process's live reservations hold.
fn held() -> u64 {
    HOST.available_memory - free_memory(&HOST, &POLICY)
}

/// One measurement at a time: the allocator and the reservations are the
/// process's.
static MEASUREMENT: Mutex<()> = Mutex::new(());

#[test]
fn a_minor_cycle_holds_at_most_its_admission_whatever_its_planes() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let mut charges = Vec::new();
    for channels in [1, 16, 64, 256] {
        let problem = reconstruction_problem(
            31,
            16,
            channels,
            ReconstructionBasis::ChannelLocal { channels },
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(100, 0.1, 0.0),
        );
        // A point on every channel, so every plane's residual is its PSF.
        let scene = Scene::new(&problem).with_point([8, 8], &vec![1.0; channels]);
        let lifecycle = model_lifecycle(&problem);
        let mut cycle = MajorCycle::initial(
            &problem,
            empty_final_model(&lifecycle),
            scene.resident_storage(),
        )
        .expect("major cycle");
        let (model, mut pass) = cycle.parts();
        pass.append(scene.pass_images(model, true)).expect("images");
        let completion = cycle.finish(SAMPLES, BLOCKS).expect("complete");
        let setup = MinorCycleSetup {
            algorithm: ReconstructionAlgorithm::Hogbom,
            accounting: HogbomIterationAccounting::Strict,
            response: None,
            nsigma: 0.0,
            noise: casa_imaging_deconvolution::NoiseEstimate::Fast,
            automask: false,
        };
        let masks = ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
            coordinate: problem.geometry().domains()[0].direction(),
        }])
        .expect("full-plane mask");
        let controls = CycleControls {
            iterations: 20,
            threshold: 0.0,
            threshold_reached: true,
            gain: 0.1,
            nsigma: 0.0,
        };
        let team = WorkerTeam::new(1).expect("team");
        let mut cache = PsfCache::default();
        let prepared =
            prepare_minor_cycle(&completion, &masks, &setup, &mut cache, &team).expect("prepare");
        ADMITTED.store(held(), Ordering::Relaxed);
        let solving = admit(
            &HOST,
            &POLICY,
            &Demand {
                phase: "minor cycle",
                memory: run_bytes(&completion, &setup, &controls, &cache, 1),
            },
        )
        .expect("admitted");
        EXCESS.store(i64::MIN, Ordering::Relaxed);
        BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let outcome = run_minor_cycle(
            prepared,
            &completion,
            &setup,
            &controls,
            &mut cache,
            &team,
            (&HOST, &POLICY),
        );
        MEASURING.store(false, Ordering::Relaxed);
        drop(solving);
        let outcome = outcome.expect("minor cycle");
        let excess = EXCESS.load(Ordering::Relaxed);
        assert!(
            excess <= 0,
            "{channels} channels: the cycle held {excess} bytes more than it was admitted"
        );
        assert_eq!(outcome.summary.stops.len(), channels);
        assert!(outcome.results.memory() >= result_bytes(outcome.terms.len()));
        charges.push(outcome.results.memory());
    }
    assert!(
        charges.windows(2).all(|pair| pair[1] > pair[0]),
        "the results' charge grows with the planes: {charges:?}"
    );
}

/// CASA's automatic mask of every channel (#980): preparing a minor cycle
/// measures every plane's PSF, masks each channel side by side on the team
/// and keeps the masks, a support and a summary row per plane. It holds no
/// more than [`prepare_bytes`], which charges the new masks, the planes
/// being masked and what it records of each plane, and the PSF summaries it
/// measures, which the run holds ([`psf_cache_bytes`]); the mask it evolves
/// from is the run's, outside the measurement. At the first boundary and the next, which grows each mask
/// from the one before, on one worker and on four: cubes of a few channels,
/// one block of 240 channels of 16 by 16 pixels, whose per-plane results a
/// block once gathered beside the mask, and a five-term Taylor family,
/// whose mask once read every term and PSF moment to keep the first.
#[test]
fn an_automatic_cube_mask_holds_at_most_its_admission() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let mtmfs = ReconstructionAlgorithm::Mtmfs {
        scales_px: vec![0.0],
        small_scale_bias: 0.0,
    };
    let cases = [
        (32, ReconstructionBasis::ChannelLocal { channels: 1 }, 1),
        (32, ReconstructionBasis::ChannelLocal { channels: 16 }, 1),
        (32, ReconstructionBasis::ChannelLocal { channels: 16 }, 4),
        (32, ReconstructionBasis::ChannelLocal { channels: 64 }, 4),
        (16, ReconstructionBasis::ChannelLocal { channels: 240 }, 1),
        (16, ReconstructionBasis::ChannelLocal { channels: 240 }, 4),
        (64, ReconstructionBasis::Taylor { terms: 5 }, 1),
    ];
    for (width, basis, workers) in cases {
        let (channels, planes, algorithm, fluxes) = match basis {
            ReconstructionBasis::ChannelLocal { channels } => (
                channels,
                channels,
                ReconstructionAlgorithm::Hogbom,
                vec![1.0; channels],
            ),
            ReconstructionBasis::Taylor { terms } => (
                1,
                1,
                mtmfs.clone(),
                (0..terms).map(|term| 0.5_f64.powi(term as i32)).collect(),
            ),
            ReconstructionBasis::Constant => unreachable!("not measured here"),
        };
        let label = format!("{basis:?} of {width} px on {workers} workers");
        let problem = reconstruction_problem(
            31,
            width,
            channels,
            basis,
            algorithm.clone(),
            ReconstructionControls::new(100, 0.1, 0.0),
        );
        let scene = Scene::new(&problem)
            .with_point([width * 3 / 8, width * 9 / 16], &fluxes)
            .with_noise(0.01);
        let lifecycle = model_lifecycle(&problem);
        let mut cycle = MajorCycle::initial(
            &problem,
            empty_final_model(&lifecycle),
            scene.resident_storage(),
        )
        .expect("major cycle");
        let (model, mut pass) = cycle.parts();
        pass.append(scene.pass_images(model, true)).expect("images");
        let completion = cycle.finish(SAMPLES, BLOCKS).expect("complete");
        let setup = MinorCycleSetup {
            algorithm,
            accounting: HogbomIterationAccounting::Strict,
            response: None,
            nsigma: 0.0,
            noise: casa_imaging_deconvolution::NoiseEstimate::Robust,
            automask: true,
        };
        let mut plans =
            ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::AutoMultithresh {
                coordinate: problem.geometry().domains()[0].direction(),
                controls: AutoMultithreshControls {
                    sidelobe_factor: 3.0,
                    noise_factor: 5.0,
                    low_noise_factor: 1.5,
                    negative_factor: 0.0,
                    minimum_beam_fraction: 0.3,
                    smooth_factor: 1.0,
                    cut_threshold: 0.01,
                    grow_iterations: 75,
                    grow_prune: true,
                    minimum_percent_change: -1.0,
                    noise: casa_imaging_deconvolution::NoiseEstimate::Robust,
                    verbose: false,
                },
                completed_major_cycles: 0,
                cycle_threshold_reached: false,
                previous: None,
                stopped_channels: Box::new([]),
            }])
            .expect("automatic mask");
        let team = WorkerTeam::new(workers).expect("team");
        let mut cache = PsfCache::default();
        for boundary in 0..2 {
            // The run holds the PSF summaries preparation measures with the
            // masks, outside preparation's own charge.
            let charge = prepare_bytes(&completion, &plans, &setup, &cache, workers)
                + psf_cache_bytes(&completion);
            let preparing = admit(
                &HOST,
                &POLICY,
                &Demand {
                    phase: "minor-cycle preparation",
                    memory: charge,
                },
            )
            .expect("admitted");
            ADMITTED.store(held() - charge, Ordering::Relaxed);
            EXCESS.store(i64::MIN, Ordering::Relaxed);
            BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
            MEASURING.store(true, Ordering::Relaxed);
            let prepared = prepare_minor_cycle(&completion, &plans, &setup, &mut cache, &team);
            MEASURING.store(false, Ordering::Relaxed);
            drop(preparing);
            let prepared = prepared.expect("prepared");
            let excess = EXCESS.load(Ordering::Relaxed);
            assert!(
                excess <= 0,
                "{label}, boundary {boundary}: preparation held {excess} bytes more than it \
                 was admitted"
            );
            let evidence = prepared.auto_masks()[0]
                .as_ref()
                .expect("automatic evidence");
            assert_eq!(evidence.planes().len(), planes, "{label}");
            assert_eq!(
                prepared.masks().primary().planes().count(),
                planes,
                "{label}: a mask plane per channel"
            );
            assert!(
                prepared.masks().primary().support().iter().any(|set| *set),
                "{label}: the point is masked"
            );
            plans = plans
                .next_cycle(prepared.masks(), prepared.auto_masks(), boundary + 1, false)
                .expect("next boundary");
        }
    }
}
