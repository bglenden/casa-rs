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
    Demand, HostResources, MinorCycleSetup, MinorLayout, PsfCache, ResourcePolicy, admit,
    free_memory, prepare_bytes, prepare_minor_cycle, psf_cache_bytes, result_bytes, run_bytes,
    run_bytes_for, run_minor_cycle,
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
static PLANE_BYTES: AtomicU64 = AtomicU64::new(0);
static PLANE_ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

// SAFETY: every method forwards to `System` with the caller's arguments and
// only adds atomic bookkeeping, which allocates nothing.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            count_plane(layout.size() as u64);
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            count_plane(layout.size() as u64);
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
            count_plane(new);
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

fn count_plane(bytes: u64) {
    if MEASURING.load(Ordering::Relaxed) && bytes == PLANE_BYTES.load(Ordering::Relaxed) {
        PLANE_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
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
            psf_cutoff: 0.35,
            psf_beams: None,
            mask_beams: None,
            algorithm: ReconstructionAlgorithm::Hogbom,
            accounting: HogbomIterationAccounting::Strict,
            response: None,
            saved_taylor_residual: false,
            nsigma: 0.0,
            noise: casa_imaging_deconvolution::NoiseEstimate::Fast,
            automask: false,
            primary_beam: None,
            residual_pixel_mask: false,
            pbmask: 0.0,
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
        // What the run keeps of the cycle: a row of every plane, within what
        // its record's admission planned.
        assert_eq!(outcome.summary.planes.len(), channels);
        let kept = std::mem::size_of_val(outcome.summary.planes.as_slice()) as u64;
        assert!(kept <= MinorLayout::of(completion.normal_state()).record_bytes());
        charges.push(outcome.results.memory());
    }
    assert!(
        charges.windows(2).all(|pair| pair[1] > pair[0]),
        "the results' charge grows with the planes: {charges:?}"
    );
}

/// A plane's peak over its pixels after its solve (#1068) reads the analytic
/// beam again where the residual's pixel mask is that beam: the solve holds
/// the beam and its supports under its admission too, which the load's
/// charge for the model samples it no longer holds covers. A plane this size
/// takes the fixed part of the admission out of the comparison.
#[test]
fn a_solve_measuring_through_a_beam_holds_at_most_its_admission() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let problem = reconstruction_problem(
        31,
        724,
        2,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Hogbom,
        ReconstructionControls::new(10, 0.1, 0.0),
    );
    let scene = Scene::new(&problem).with_point([362, 362], &[1.0, 1.0]);
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
    let centres = std::collections::BTreeMap::from([(
        casa_imaging_model::ImageDomainRole::Main,
        vec![[362.0, 362.0]],
    )]);
    let setup = MinorCycleSetup {
        psf_cutoff: 0.35,
        psf_beams: None,
        mask_beams: None,
        algorithm: ReconstructionAlgorithm::Hogbom,
        accounting: HogbomIterationAccounting::Strict,
        response: None,
        saved_taylor_residual: false,
        nsigma: 0.0,
        noise: casa_imaging_deconvolution::NoiseEstimate::Fast,
        automask: false,
        primary_beam: casa_imaging_products::AnalyticPrimaryBeam::new(
            &problem,
            casa_imaging_products::AnalyticPrimaryBeamModel::CasaEvlaCommon,
            Some(&centres),
        )
        .expect("L band"),
        residual_pixel_mask: true,
        pbmask: 0.0,
    };
    assert!(setup.primary_beam.is_some());
    let masks = ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
        coordinate: problem.geometry().domains()[0].direction(),
    }])
    .expect("full-plane mask");
    let controls = CycleControls {
        iterations: 5,
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
    outcome.expect("minor cycle");
    let excess = EXCESS.load(Ordering::Relaxed);
    assert!(
        excess <= 0,
        "the solve held {excess} bytes more than it was admitted"
    );
}

type AutomaticMemoryCase = (usize, ReconstructionBasis, usize, bool, bool);

fn automatic_memory_cases() -> impl Iterator<Item = (AutomaticMemoryCase, usize, bool)> {
    // The cases with a beam read an analytic `.pb` for the residual's
    // pixel mask and an `nsigma` noise inside it (#1078, #1079); the EVLA's
    // beam covers the cubes' 1.05 to 7.35 GHz.
    let cases = [
        (
            32,
            ReconstructionBasis::ChannelLocal { channels: 1 },
            1,
            false,
            false,
        ),
        (
            32,
            ReconstructionBasis::ChannelLocal { channels: 16 },
            1,
            false,
            false,
        ),
        (
            32,
            ReconstructionBasis::ChannelLocal { channels: 16 },
            4,
            false,
            false,
        ),
        (
            32,
            ReconstructionBasis::ChannelLocal { channels: 16 },
            4,
            true,
            false,
        ),
        (
            32,
            ReconstructionBasis::ChannelLocal { channels: 64 },
            4,
            false,
            false,
        ),
        (
            16,
            ReconstructionBasis::ChannelLocal { channels: 240 },
            1,
            false,
            false,
        ),
        (
            16,
            ReconstructionBasis::ChannelLocal { channels: 240 },
            4,
            false,
            false,
        ),
        (
            64,
            ReconstructionBasis::Taylor { terms: 5 },
            1,
            false,
            false,
        ),
        (64, ReconstructionBasis::Taylor { terms: 5 }, 1, true, false),
        // A Taylor family under a response: its beam is the principal
        // weight image's, read into an owned sensitivity plane.
        (64, ReconstructionBasis::Taylor { terms: 5 }, 1, true, true),
    ];
    let full_stokes = (
        32,
        ReconstructionBasis::ChannelLocal { channels: 16 },
        4,
        true,
        false,
    );
    cases
        .into_iter()
        .map(|case| (case, 1, false))
        .chain([(full_stokes, 4, false), (full_stokes, 4, true)])
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
    let cases = automatic_memory_cases();
    for ((width, basis, workers, with_beam, with_response), polarizations, empty_beam) in cases {
        let (channels, planes_per_polarization, algorithm, fluxes) = match basis {
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
        let planes = planes_per_polarization * polarizations;
        let label = format!(
            "{basis:?} of {width} px and {polarizations} polarizations on {workers} workers, beam {with_beam}, response {with_response}"
        );
        let problem = if polarizations == 4 {
            use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesQ, StokesU, StokesV};
            problems::reconstruction_problem_of_shape_and_polarizations(
                31,
                [width, width],
                channels,
                1,
                basis,
                algorithm.clone(),
                ReconstructionControls::new(100, 0.1, 0.0),
                vec![StokesI, StokesQ, StokesU, StokesV],
            )
        } else {
            reconstruction_problem(
                31,
                width,
                channels,
                basis,
                algorithm.clone(),
                ReconstructionControls::new(100, 0.1, 0.0),
            )
        };
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
        let mut images = scene.pass_images(model, true);
        if polarizations == 4 {
            images.polarizations = 4;
            let cells = width * width;
            images.psf = images.psf.take().map(|psf| {
                psf.chunks_exact(cells)
                    .flat_map(|plane| plane.repeat(4))
                    .collect()
            });
            images.residual = images
                .residual
                .chunks_exact(cells)
                .flat_map(|plane| plane.repeat(4))
                .collect();
            images.sum_weights = vec![1.0; channels * 4];
            images.published_sum_weights = vec![1.0; channels * 4];
        }
        pass.append(images).expect("images");
        let completion = cycle.finish(SAMPLES, BLOCKS).expect("complete");
        let centres = std::collections::BTreeMap::from([(
            casa_imaging_model::ImageDomainRole::Main,
            vec![[
                if empty_beam {
                    -1.0e5
                } else {
                    width as f64 / 2.0
                },
                width as f64 / 2.0,
            ]],
        )]);
        let mut setup = automask_setup(&problem, algorithm, &centres, (with_beam, with_response));
        if empty_beam {
            setup.pbmask = 0.2;
            let beam = setup.primary_beam.as_ref().expect("analytic beam");
            for channel in 0..channels {
                assert!(
                    beam.pb(&casa_imaging_model::ImageDomainRole::Main, channel)
                        .expect("channel beam")
                        .iter()
                        .all(|value| *value <= setup.pbmask as f32),
                    "{label}: channel {channel} must exercise the empty-domain fallback"
                );
            }
        }
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
            if empty_beam && boundary == 0 {
                assert!(
                    prepared.statistics.nsigma_threshold > 0.0,
                    "{label}: the first boundary must fall back to the valid pixels"
                );
            }
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
            assert_eq!(
                prepared.masks().primary().support().iter().any(|set| *set),
                !empty_beam,
                "{label}: the mask respects an empty beam"
            );
            plans = plans
                .next_cycle(prepared.masks(), prepared.auto_masks(), boundary + 1, false)
                .expect("next boundary");
        }
    }
}

/// How a beam-reading preparation reads the `.pb`: for the `nsigma` noise
/// and the residual's pixel mask at `pbmask` 0, also above a `pbmask` of
/// 0.3 with a mask cut to the beam there (`usemask='pb'`), or only for that
/// mask, without `nsigma` and with a negative `pblimit`.
#[derive(Clone, Copy, Debug)]
enum BeamReading {
    Statistics,
    PbMask,
    MaskOnly,
}

/// A minor cycle's preparation that reads the analytic `.pb` (#1078,
/// #1061): each plane loaded at once forms its beam (`f32`) and keeps its
/// supports beside the `nsigma` noise's copies, and a mask cut to the beam
/// forms one plane's beam at a time beside the mask it makes, a plane per
/// channel, all within [`prepare_bytes`]. A full-plane mask holds one
/// support, so the planes in flight are what the charge must cover: cubes
/// of 1 and 16 channels on one worker and on four, a single plane of 128 by
/// 128 pixels, under either noise estimate and each [`BeamReading`].
#[test]
fn a_preparation_that_reads_the_beam_holds_at_most_its_admission() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let cases = [(32, 1, 1), (32, 16, 1), (32, 16, 4), (128, 1, 1)];
    let readings = [
        BeamReading::Statistics,
        BeamReading::PbMask,
        BeamReading::MaskOnly,
    ];
    for (((width, channels, workers), noise), reading) in cases
        .into_iter()
        .flat_map(|case| {
            [
                casa_imaging_deconvolution::NoiseEstimate::Fast,
                casa_imaging_deconvolution::NoiseEstimate::Robust,
            ]
            .map(|noise| (case, noise))
        })
        .flat_map(|row| readings.map(|reading| (row, reading)))
    {
        let label = format!(
            "{channels} channels of {width} px on {workers} workers, {noise:?}, {reading:?}"
        );
        let basis = ReconstructionBasis::ChannelLocal { channels };
        let problem = reconstruction_problem(
            31,
            width,
            channels,
            basis,
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(100, 0.1, 0.0),
        );
        let scene = Scene::new(&problem)
            .with_point([width * 3 / 8, width * 9 / 16], &vec![1.0; channels])
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
        let centres = std::collections::BTreeMap::from([(
            casa_imaging_model::ImageDomainRole::Main,
            vec![[width as f64 / 2.0, width as f64 / 2.0]],
        )]);
        let mask_only = matches!(reading, BeamReading::MaskOnly);
        let pbmask = match reading {
            BeamReading::Statistics => 0.0,
            BeamReading::PbMask | BeamReading::MaskOnly => 0.3,
        };
        let setup = MinorCycleSetup {
            psf_cutoff: 0.35,
            psf_beams: None,
            mask_beams: None,
            algorithm: ReconstructionAlgorithm::Hogbom,
            accounting: HogbomIterationAccounting::Strict,
            response: None,
            saved_taylor_residual: false,
            nsigma: if mask_only { 0.0 } else { 3.0 },
            noise,
            automask: false,
            primary_beam: casa_imaging_products::AnalyticPrimaryBeam::new(
                &problem,
                casa_imaging_products::AnalyticPrimaryBeamModel::CasaEvlaCommon,
                Some(&centres),
            )
            .expect("L and S band"),
            residual_pixel_mask: !mask_only,
            pbmask,
        };
        let full = ReconstructionMaskPlan::FullPlane {
            coordinate: problem.geometry().domains()[0].direction(),
        };
        let plan = match reading {
            BeamReading::Statistics => full,
            BeamReading::PbMask | BeamReading::MaskOnly => {
                ReconstructionMaskPlan::within_primary_beam(full, pbmask).expect("a pb mask")
            }
        };
        let plans = ImageDomainReconstructionMaskPlans::new([plan]).expect("the mask");
        let team = WorkerTeam::new(workers).expect("team");
        let mut cache = PsfCache::default();
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
            "{label}: preparation held {excess} bytes more than it was admitted"
        );
        assert!(
            mask_only || prepared.statistics.nsigma_threshold > 0.0,
            "{label}"
        );
        let mask = prepared.masks().get(0).expect("the mask");
        assert_eq!(
            mask.planes().count(),
            if mask_only || matches!(reading, BeamReading::PbMask) {
                channels
            } else {
                1
            },
            "{label}"
        );
    }
}

/// The automatic-mask law's setup: reading the beam, with `nsigma`, when
/// `with_beam`, the weight image's under a flat-noise response when
/// `with_response`, else the EVLA's analytic beam centred on `centres`.
fn automask_setup<'a>(
    problem: &'a casa_imaging_model::CompiledProblem,
    algorithm: ReconstructionAlgorithm,
    centres: &'a casa_imaging_products::PrimaryBeamCentres,
    (with_beam, with_response): (bool, bool),
) -> MinorCycleSetup<'a> {
    MinorCycleSetup {
        psf_cutoff: 0.35,
        psf_beams: None,
        mask_beams: None,
        algorithm,
        accounting: HogbomIterationAccounting::Strict,
        response: with_response.then(|| {
            casa_imaging_products::MinorCycleImageResponse::new(
                casa_imaging_model::ProductNormalization::FlatNoise,
                problems::validity().primary_beam(),
            )
            .expect("flat-noise response")
        }),
        saved_taylor_residual: false,
        nsigma: if with_beam { 3.0 } else { 0.0 },
        noise: casa_imaging_deconvolution::NoiseEstimate::Robust,
        automask: true,
        primary_beam: (with_beam && !with_response)
            .then(|| {
                casa_imaging_products::AnalyticPrimaryBeam::new(
                    problem,
                    casa_imaging_products::AnalyticPrimaryBeamModel::CasaEvlaCommon,
                    Some(centres),
                )
                .expect("L to C band")
            })
            .flatten(),
        residual_pixel_mask: with_beam,
        pbmask: 0.0,
    }
}

/// A joint Clark solve holds all of a channel's polarization planes and
/// refreshes under its run admission (#1140).
#[test]
fn joint_clark_holds_at_most_its_admission() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    for (channels, workers) in [(1, 1), (4, 4)] {
        measure_clark(channels, workers, ReconstructionAlgorithm::Clark, false);
    }
}

/// Separate Clark polarization planes also stay within their admission.
#[test]
fn clarkstokes_holds_at_most_its_admission() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    for (channels, workers) in [(1, 1), (4, 4)] {
        measure_clark(
            channels,
            workers,
            ReconstructionAlgorithm::ClarkStokes,
            false,
        );
    }
}

/// A multi-plane clarkstokes run with only one nonblank polarization must
/// not retain an unadmitted single-plane refresh (#1126).
#[test]
fn clarkstokes_does_not_retain_a_refresh_for_one_surviving_plane() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    measure_clark(1, 1, ReconstructionAlgorithm::ClarkStokes, true);
}

fn measure_clark(
    channels: usize,
    workers: usize,
    algorithm: ReconstructionAlgorithm,
    one_nonblank: bool,
) {
    use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesQ, StokesU, StokesV};
    let width = 128;
    let problem = problems::reconstruction_problem_of_shape_and_polarizations(
        31,
        [width, width],
        channels,
        1,
        ReconstructionBasis::ChannelLocal { channels },
        algorithm.clone(),
        ReconstructionControls::new(300, 0.1, 0.0),
        vec![StokesI, StokesQ, StokesU, StokesV],
    );
    let scene = Scene::new(&problem).with_point([width / 2, width / 2], &vec![1.0; channels]);
    let lifecycle = model_lifecycle(&problem);
    let mut cycle = MajorCycle::initial(
        &problem,
        empty_final_model(&lifecycle),
        scene.resident_storage(),
    )
    .expect("major cycle");
    let (model, mut pass) = cycle.parts();
    let mut images = scene.pass_images(model, true);
    images.polarizations = 4;
    let psf = images.psf.take().unwrap();
    let residual = images.residual.clone();
    let cells = width * width;
    images.psf = Some(
        psf.chunks_exact(cells)
            .flat_map(|plane| plane.repeat(4))
            .collect(),
    );
    images.residual = residual
        .chunks_exact(cells)
        .flat_map(|plane| plane.repeat(4))
        .collect();
    images.sum_weights = vec![1.0; 4 * channels];
    images.published_sum_weights = vec![1.0; 4 * channels];
    if one_nonblank {
        images.sum_weights[1..].fill(0.0);
        images.published_sum_weights[1..].fill(0.0);
    }
    pass.append(images).expect("images");
    let completion = cycle.finish(SAMPLES, BLOCKS).expect("completion");
    let setup = MinorCycleSetup {
        psf_cutoff: 0.35,
        psf_beams: None,
        mask_beams: None,
        algorithm,
        accounting: HogbomIterationAccounting::Strict,
        response: None,
        saved_taylor_residual: false,
        nsigma: 0.0,
        noise: casa_imaging_deconvolution::NoiseEstimate::Fast,
        automask: false,
        primary_beam: None,
        residual_pixel_mask: false,
        pbmask: 0.0,
    };
    let layout = MinorLayout::of(completion.normal_state());
    let empty_budget = run_bytes_for(&layout, &setup, 0, 0, workers);
    let component_budget = run_bytes_for(&layout, &setup, 300, 0, workers);
    assert!(
        component_budget > empty_budget,
        "more possible components need a larger solve admission: {empty_budget} vs {component_budget}"
    );
    let masks = ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
        coordinate: problem.geometry().domains()[0].direction(),
    }])
    .expect("mask");
    let controls = CycleControls {
        iterations: 300,
        threshold: 0.0,
        threshold_reached: true,
        gain: 0.1,
        nsigma: 0.0,
    };
    let team = WorkerTeam::new(workers).expect("team");
    let mut cache = PsfCache::default();
    let prepared =
        prepare_minor_cycle(&completion, &masks, &setup, &mut cache, &team).expect("prepare");
    ADMITTED.store(held(), Ordering::Relaxed);
    let solving = admit(
        &HOST,
        &POLICY,
        &Demand {
            phase: "minor cycle",
            memory: run_bytes(&completion, &setup, &controls, &cache, workers),
        },
    )
    .expect("admit");
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
    assert!(outcome.summary.components > 0);
    if one_nonblank {
        assert_eq!(outcome.summary.planes.len(), 4, "one row per polarization");
        assert!(
            outcome
                .summary
                .trace
                .iter()
                .all(|entry| entry.cell.polarization() == 0),
            "blank polarizations must not acquire components"
        );
        assert_eq!(
            cache.refresh_bytes(&completion),
            0,
            "unadmitted retained refresh"
        );
    }
    let excess = EXCESS.load(Ordering::Relaxed);
    assert!(
        excess <= 0,
        "joint Clark with {channels} channels and {workers} workers held {excess} bytes more than admitted"
    );
}

/// Settlement stays within admission for a fresh off-centre PSF and for
/// a reused weak PSF whose solver plane is retained by preparation.
#[test]
fn settling_a_refused_psf_fits_its_admission() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    for (workers, weak) in [(1, false), (4, false), (1, true)] {
        let (width, channels) = if weak { (256, 1) } else { (128, 2) };
        let problem = reconstruction_problem(
            31,
            width,
            channels,
            ReconstructionBasis::ChannelLocal { channels },
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(10, 0.1, 0.0),
        );
        let scene = Scene::new(&problem);
        let lifecycle = model_lifecycle(&problem);
        let mut cycle = MajorCycle::initial(
            &problem,
            empty_final_model(&lifecycle),
            scene.resident_storage(),
        )
        .expect("major cycle");
        let (model, mut pass) = cycle.parts();
        let mut images = scene.pass_images(model, true);
        let psf = images.psf.as_mut().expect("initial PSF");
        psf.fill(0.0);
        if weak {
            let centre = (width / 2) * (width + 1);
            psf[centre] = 5e-8;
            psf[centre + width] = 4e-8;
        } else {
            for x in 0..width {
                for y in 0..width {
                    let dx = x as f64 - 64.0;
                    let dy = y as f64 - 64.0;
                    psf[x * width + y] =
                        (-4.0 * std::f64::consts::LN_2 * (dx * dx + dy * dy) / 3600.0).exp() as f32;
                }
            }
            psf[width * width + 31 * width + 64] = 1.0;
        }
        pass.append(images).expect("images");
        let completion = cycle.finish(SAMPLES, BLOCKS).expect("complete");
        let setup = MinorCycleSetup {
            psf_cutoff: 0.35,
            psf_beams: None,
            mask_beams: None,
            algorithm: ReconstructionAlgorithm::Hogbom,
            accounting: HogbomIterationAccounting::Strict,
            response: None,
            saved_taylor_residual: false,
            nsigma: 0.0,
            noise: casa_imaging_deconvolution::NoiseEstimate::Fast,
            automask: false,
            primary_beam: None,
            residual_pixel_mask: false,
            pbmask: 0.0,
        };
        let plans = ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
            coordinate: problem.geometry().domains()[0].direction(),
        }])
        .expect("full mask");
        let team = WorkerTeam::new(workers).expect("team");
        let mut cache = if weak {
            PsfCache::for_reused_single_psf()
        } else {
            PsfCache::default()
        };
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
        if weak {
            PLANE_BYTES.store((width * width * size_of::<f64>()) as u64, Ordering::Relaxed);
            PLANE_ALLOCATIONS.store(0, Ordering::Relaxed);
        }
        MEASURING.store(true, Ordering::Relaxed);
        let prepared = prepare_minor_cycle(&completion, &plans, &setup, &mut cache, &team);
        MEASURING.store(false, Ordering::Relaxed);
        PLANE_BYTES.store(0, Ordering::Relaxed);
        drop(preparing);
        prepared.expect("prepared");
        let excess = EXCESS.load(Ordering::Relaxed);
        assert!(
            excess <= 0,
            "{channels} channels with {workers} workers held {excess} bytes over admission"
        );
        let refused = cache.summary(0, channels - 1, 0).expect("settled PSF");
        assert!(refused.fit_refused() && !refused.needs_domain_beam());
        if weak {
            // Preparation allocates the retained residual and PSF. Settlement
            // should never allocate a second f64 PSF of the same size.
            assert_eq!(PLANE_ALLOCATIONS.load(Ordering::Relaxed), 2);
            assert_eq!(refused.sidelobe(), f64::from(4e-8_f32));
        } else {
            assert!(refused.sidelobe() < 1.0, "{}", refused.sidelobe());
        }
    }
}

#[test]
fn a_dense_mem_cycle_stays_within_solve_and_result_admissions() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    for channels in [1, 4] {
        let problem = reconstruction_problem(
            31,
            32,
            channels,
            ReconstructionBasis::ChannelLocal { channels },
            ReconstructionAlgorithm::Mem,
            ReconstructionControls::new(2, 0.1, 0.0),
        );
        let scene = Scene::new(&problem).with_point([16, 16], &vec![1.0; channels]);
        let lifecycle = model_lifecycle(&problem);
        let mut cycle = MajorCycle::initial(
            &problem,
            empty_final_model(&lifecycle),
            scene.resident_storage(),
        )
        .unwrap();
        let (model, mut pass) = cycle.parts();
        pass.append(scene.pass_images(model, true)).unwrap();
        let completion = cycle.finish(SAMPLES, BLOCKS).unwrap();
        let setup = MinorCycleSetup {
            psf_cutoff: 0.35,
            psf_beams: None,
            mask_beams: None,
            algorithm: ReconstructionAlgorithm::Mem,
            accounting: HogbomIterationAccounting::Strict,
            response: None,
            saved_taylor_residual: false,
            nsigma: 0.0,
            noise: casa_imaging_deconvolution::NoiseEstimate::Fast,
            automask: false,
            primary_beam: None,
            residual_pixel_mask: false,
            pbmask: 0.0,
        };
        let masks = ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
            coordinate: problem.geometry().domains()[0].direction(),
        }])
        .unwrap();
        let controls = CycleControls {
            iterations: 2,
            threshold: 0.0,
            threshold_reached: true,
            gain: 0.1,
            nsigma: 0.0,
        };
        let team = WorkerTeam::new(1).unwrap();
        let mut cache = PsfCache::default();
        let prepared = prepare_minor_cycle(&completion, &masks, &setup, &mut cache, &team).unwrap();
        let mut no_solver = setup.clone();
        no_solver.algorithm = ReconstructionAlgorithm::Dirty;
        let planned = run_bytes(&completion, &setup, &controls, &cache, 1);
        let base = run_bytes(&completion, &no_solver, &controls, &cache, 1);
        assert_eq!(
            planned - base,
            casa_imaging_deconvolution::mem_plane_bytes(
                casa_imaging_deconvolution::PlaneShape::new(32, 32)
            ),
            "the production planner admits the actual MEM plane owner"
        );
        ADMITTED.store(held(), Ordering::Relaxed);
        let solving = admit(
            &HOST,
            &POLICY,
            &Demand {
                phase: "minor cycle",
                memory: run_bytes(&completion, &setup, &controls, &cache, 1),
            },
        )
        .unwrap();
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
        let outcome = outcome.unwrap();
        let excess = EXCESS.load(Ordering::Relaxed);
        assert!(
            excess <= 0,
            "{channels} planes exceed their admission by {excess} bytes"
        );
        assert_eq!(outcome.dense.len(), channels);
        assert_eq!(
            outcome.results.memory(),
            MinorLayout::of(completion.normal_state()).result_bytes(&setup, controls.iterations)
        );
    }
}
