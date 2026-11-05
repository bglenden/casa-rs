// SPDX-License-Identifier: LGPL-3.0-or-later

//! Laws of the minor-cycle adapter (`prepare_minor_cycle`,
//! `run_minor_cycle`) on synthetic passes over a known sky: CASA's residual
//! normalisation under a direction-dependent response, the diagnostics the
//! task publishes, and results independent of the worker count. The
//! problems and passes are the reconstruction tests' shared fixtures.

#[path = "../../casa-imaging-deconvolution/tests/support/problems.rs"]
mod problems;
#[path = "../../casa-imaging-deconvolution/tests/support/synthetic_pass.rs"]
mod synthetic_pass;

use casa_imaging_deconvolution::CycleControls;
use casa_imaging_deconvolution::{
    ImageDomainReconstructionMaskPlans, MajorCycle, MajorCycleCompletion, PassImages,
    ReconstructionMaskPlan,
};
use casa_imaging_model::{
    CompiledProblem, HogbomIterationAccounting, ProductNormalization, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionControls,
};
use casa_imaging_products::MinorCycleImageResponse;
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{
    HostResources, MinorCycleOutcome, MinorCycleSetup, PsfCache, ResourcePolicy,
    prepare_minor_cycle, run_minor_cycle,
};
use problems::{empty_final_model, model_lifecycle, reconstruction_problem, validity};
use synthetic_pass::{BLOCKS, SAMPLES, Scene};

fn problem(
    width: usize,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
) -> CompiledProblem {
    let channels = match basis {
        ReconstructionBasis::ChannelLocal { channels } => channels,
        _ => 1,
    };
    reconstruction_problem(
        31,
        width,
        channels,
        basis,
        algorithm,
        ReconstructionControls::new(100, 0.1, 0.0),
    )
}

/// The completion of an initial pass of `scene` over the empty model, its
/// images edited by `edit` before they are assembled.
fn completion(
    problem: &CompiledProblem,
    scene: &Scene,
    edit: impl FnOnce(&mut PassImages),
) -> MajorCycleCompletion {
    let lifecycle = model_lifecycle(problem);
    let mut cycle = MajorCycle::initial(
        problem,
        empty_final_model(&lifecycle),
        scene.resident_storage(),
    )
    .expect("major cycle");
    let (model, mut pass) = cycle.parts();
    let mut images = scene.pass_images(model, true);
    edit(&mut images);
    pass.append(images).expect("images");
    cycle.finish(SAMPLES, BLOCKS).expect("complete")
}

fn setup(
    problem: &CompiledProblem,
    response: Option<MinorCycleImageResponse>,
) -> MinorCycleSetup<'static> {
    MinorCycleSetup {
        psf_cutoff: 0.35,
        psf_beams: None,
        mask_beams: None,
        algorithm: problem.reconstruction().algorithm().clone(),
        accounting: HogbomIterationAccounting::Strict,
        response,
        nsigma: 0.0,
        noise: casa_imaging_deconvolution::NoiseEstimate::Fast,
        automask: false,
        primary_beam: None,
        residual_pixel_mask: false,
        pbmask: 0.0,
    }
}

fn full_plane(problem: &CompiledProblem) -> ImageDomainReconstructionMaskPlans {
    ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
        coordinate: problem.geometry().domains()[0].direction(),
    }])
    .expect("full-plane mask")
}

fn controls(iterations: usize, gain: f64) -> CycleControls {
    CycleControls {
        iterations,
        threshold: 0.0,
        threshold_reached: true,
        gain,
        nsigma: 0.0,
    }
}

/// One minor cycle of `completion` on `workers` workers; the prepared
/// peak and the outcome.
fn minor_cycle(
    problem: &CompiledProblem,
    completion: &MajorCycleCompletion,
    setup: &MinorCycleSetup,
    controls: &CycleControls,
    workers: usize,
) -> (f64, MinorCycleOutcome) {
    let team = WorkerTeam::new(workers).expect("team");
    let mut cache = PsfCache::default();
    let prepared = prepare_minor_cycle(completion, &full_plane(problem), setup, &mut cache, &team)
        .expect("prepare");
    let peak = prepared.statistics.peak;
    let outcome = run_minor_cycle(
        prepared,
        completion,
        setup,
        controls,
        &mut cache,
        &team,
        (&HOST, &POLICY),
    )
    .expect("minor cycle");
    (peak, outcome)
}

/// A host and policy that admit every cycle here.
const HOST: HostResources = HostResources {
    threads: 8,
    performance_cores: 8,
    available_memory: 1 << 40,
    metal: false,
};
const POLICY: ResourcePolicy = ResourcePolicy::Explicit {
    workers: 8,
    memory: 1 << 40,
};

/// `SIImageStore::divideResidualByWeight` divides the residual by the
/// published `.sumwt` and the weight image by the PSF gridding's sum (the
/// same number unless the pass published another gridding's, #667); the
/// flat-noise peak the minor cycle cleans follows from both (#669).
#[test]
fn the_response_divides_the_residual_by_the_published_sum_and_the_weight_by_the_psf_sum() {
    let problem = problem(
        8,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Hogbom,
    );
    let scene = Scene::new(&problem);
    let psf = scene.unit_psf();
    let completion = completion(&problem, &scene, |images| {
        let eight = psf
            .iter()
            .map(|value| (8.0 * value) as f32)
            .collect::<Vec<_>>();
        images.sum_weights = vec![8.0];
        images.published_sum_weights = vec![3.0];
        images.weight = Some(vec![16.0; psf.len()]);
        images.residual.clone_from(&eight);
        images.psf = Some(eight);
    });
    let response =
        MinorCycleImageResponse::new(ProductNormalization::FlatNoise, validity().primary_beam())
            .expect("response");
    let (peak, outcome) = minor_cycle(
        &problem,
        &completion,
        &setup(&problem, Some(response)),
        &controls(1, 0.25),
        1,
    );
    // (8 / 3) / (√(16 / 8) · √(16 / 8)) = 4/3 Jy; one iteration at gain
    // 0.25 removes a third of a Jansky.
    assert!((peak - 4.0 / 3.0).abs() < 1e-6, "{peak}");
    assert!((outcome.summary.absolute_flux - 1.0 / 3.0).abs() < 1e-6);
}

/// The published final peak is a magnitude: a negative source cleaned once
/// at gain 0.1 leaves 0.9 Jy, not zero.
#[test]
fn a_negative_residual_publishes_its_peak_magnitude() {
    let problem = problem(
        16,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Hogbom,
    );
    let scene = Scene::new(&problem).with_point([8, 8], &[-1.0]);
    let completion = completion(&problem, &scene, |_| {});
    let (peak, outcome) = minor_cycle(
        &problem,
        &completion,
        &setup(&problem, None),
        &controls(1, 0.1),
        1,
    );
    assert!((peak - 1.0).abs() < 1e-6, "{peak}");
    assert!(
        (outcome.summary.peak - 0.9).abs() < 1e-6,
        "{}",
        outcome.summary.peak
    );
    assert!((outcome.summary.trace[0].flux + 0.1).abs() < 1e-6);
}

/// A plane whose PSF peaks off its centre has no beam of its own, and CASA
/// gives it its domain's largest (`SIImageStore::makeImageBeamSet`), which
/// sizes Clark's patch (`SDAlgorithmClarkClean2`, #1130). Here channel 0's PSF
/// peaks off-centre, and channel 1's beam is the narrower and channel 2's the
/// wider of two fitted ones: channel 0 takes channel 2's beam and patch, with
/// its own sidelobe, measured off its own PSF. A pixel-wide beam would give it
/// a patch of 13 pixels where channel 2's is about 60.
#[test]
fn an_off_centre_plane_takes_its_domains_largest_beam_and_clarks_patch() {
    const WIDTH: usize = 128;
    let problem = problem(
        WIDTH,
        ReconstructionBasis::ChannelLocal { channels: 3 },
        ReconstructionAlgorithm::Clark,
    );
    let scene = Scene::new(&problem);
    let cells = WIDTH * WIDTH;
    let cell = |x: usize, y: usize| x * WIDTH + y;
    let completion = completion(&problem, &scene, |images| {
        let psf = images.psf.as_mut().expect("the initial pass's PSF");
        psf.fill(0.0);
        psf[cell(10, 100)] = 1.0;
        psf[cell(64, 64)] = 0.99;
        for (plane, sigma) in [(1, 4.0), (2, 8.0)] {
            for x in 0..WIDTH {
                for y in 0..WIDTH {
                    let (dx, dy) = (x as f64 - 64.0, y as f64 - 64.0);
                    let value = (-(dx * dx + dy * dy) / (2.0_f64 * sigma * sigma)).exp();
                    psf[plane * cells + cell(x, y)] = value as f32;
                }
            }
        }
        images.residual.fill(0.0);
        images.residual[cell(40, 40)] = 1.0;
    });
    let team = WorkerTeam::new(1).expect("team");
    let mut cache = PsfCache::default();
    prepare_minor_cycle(
        &completion,
        &full_plane(&problem),
        &setup(&problem, None),
        &mut cache,
        &team,
    )
    .expect("prepare");
    let summary = |channel| cache.summary(0, channel, 0).expect("a measured plane");
    let (off, narrow, wide) = (summary(0), summary(1), summary(2));
    assert!(off.off_centre() && !narrow.off_centre() && !wide.off_centre());
    assert!(narrow.clark().size[0] < wide.clark().size[0]);
    assert_eq!(off.beam(), wide.beam());
    assert_eq!(off.clark().size, wide.clark().size);
    assert!(off.sidelobe() >= 1.0, "{}", off.sidelobe());
    assert!(off.clark().exterior_sidelobe >= 1.0);
}

/// CASA 6.7.6.14 subtracts the domain's fitted beam at the image centre
/// before choosing the largest sidelobe, including for an off-centre PSF.
#[test]
fn an_off_centre_plane_remeasures_sidelobes_after_its_domain_beam_is_chosen() {
    const WIDTH: usize = 64;
    for algorithm in [
        ReconstructionAlgorithm::Hogbom,
        ReconstructionAlgorithm::Clark,
    ] {
        let problem = problem(
            WIDTH,
            ReconstructionBasis::ChannelLocal { channels: 2 },
            algorithm,
        );
        let scene = Scene::new(&problem);
        let cells = WIDTH * WIDTH;
        let completion = completion(&problem, &scene, |images| {
            let psf = images.psf.as_mut().expect("initial PSF");
            psf.fill(0.0);
            for x in 0..WIDTH {
                for y in 0..WIDTH {
                    let dx = x as f64 - 32.0;
                    let dy = y as f64 - 32.0;
                    psf[x * WIDTH + y] =
                        (-4.0 * std::f64::consts::LN_2 * (dx * dx + dy * dy) / 1600.0).exp() as f32;
                }
            }
            psf[cells + 15 * WIDTH + 32] = 1.0;
            images.residual.fill(0.0);
            images.residual[32 * WIDTH + 32] = 1.0;
            images.residual[cells + 32 * WIDTH + 32] = 1.0;
        });
        let mut cache = PsfCache::default();
        let result = prepare_minor_cycle(
            &completion,
            &full_plane(&problem),
            &setup(&problem, None),
            &mut cache,
            &WorkerTeam::new(1).expect("team"),
        )
        .expect("prepare");
        let off = cache.summary(0, 1, 0).expect("off-centre summary");
        assert!(off.off_centre());
        assert!(
            (off.sidelobe() - 0.39395368).abs() < 2e-5,
            "{}",
            off.sidelobe()
        );
        assert!(
            (result.statistics.psf_sidelobe - 0.39395368).abs() < 2e-5,
            "{}",
            result.statistics.psf_sidelobe
        );
    }
}

fn automatic_plan(
    problem: &CompiledProblem,
    minimum_beam_fraction: f64,
) -> ImageDomainReconstructionMaskPlans {
    ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::AutoMultithresh {
        coordinate: problem.geometry().domains()[0].direction(),
        controls: casa_imaging_deconvolution::AutoMultithreshControls {
            sidelobe_factor: 2.0,
            noise_factor: 4.25,
            low_noise_factor: 1.5,
            negative_factor: 0.0,
            minimum_beam_fraction,
            smooth_factor: 1.0,
            cut_threshold: 0.01,
            grow_iterations: 0,
            grow_prune: false,
            minimum_percent_change: -1.0,
            noise: casa_imaging_deconvolution::NoiseEstimate::Fast,
            verbose: false,
        },
        completed_major_cycles: 0,
        cycle_threshold_reached: false,
        previous: None,
        stopped_channels: Box::new([]),
    }])
    .expect("automatic mask")
}

/// A non-Gaussian PSF has different fitted beams at different cutoffs.
/// Exercise both the ordinary path and the automatic-mask premeasurement.
fn cutoff_changes_the_measured_beam(automatic: bool) {
    const WIDTH: usize = 64;
    let problem = problem(
        WIDTH,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Clark,
    );
    let scene = Scene::new(&problem).with_point([32, 32], &[1.0]);
    let completion = completion(&problem, &scene, |images| {
        let psf = images.psf.as_mut().expect("PSF");
        for x in 0..WIDTH {
            for y in 0..WIDTH {
                let dx = x as f64 - 32.0;
                let dy = y as f64 - 32.0;
                let r2 = dx * dx + dy * dy;
                psf[x * WIDTH + y] = (0.7 * (-r2 / 8.0).exp() + 0.3 * (-r2 / 72.0).exp()) as f32;
            }
        }
    });
    let plans = if automatic {
        automatic_plan(&problem, 0.3)
    } else {
        full_plane(&problem)
    };
    let team = WorkerTeam::new(1).expect("team");
    let beam = |cutoff| {
        let mut setup = setup(&problem, None);
        setup.psf_cutoff = cutoff;
        setup.automask = automatic;
        let mut cache = PsfCache::default();
        prepare_minor_cycle(&completion, &plans, &setup, &mut cache, &team).expect("prepare");
        cache
            .summary(0, 0, 0)
            .expect("measured beam")
            .beam()
            .major_fwhm_rad()
    };
    let (low, high) = (beam(0.15), beam(0.65));
    assert!(low > high * 1.1, "low {low}, high {high}");
}

#[test]
fn a_minor_cycle_fits_its_psf_at_the_requested_cutoff() {
    cutoff_changes_the_measured_beam(false);
}

#[test]
fn an_automatic_mask_measures_its_psf_at_the_requested_cutoff() {
    cutoff_changes_the_measured_beam(true);
}

/// A saved beam is authoritative for a restart, whatever the new cutoff.
fn saved_beam_is_used_by_minor_preparation(automatic: bool) {
    let problem = problem(
        64,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Clark,
    );
    let scene = Scene::new(&problem).with_point([32, 32], &[1.0]);
    let completion = completion(&problem, &scene, |_| {});
    let beam = casa_imaging_deconvolution::RestoringBeam::new(10.0, 6.0, 0.2).unwrap();
    let mut setup = setup(&problem, None);
    setup.psf_cutoff = 0.65;
    setup.psf_beams = Some(vec![beam].into());
    setup.automask = automatic;
    let plans = if automatic {
        automatic_plan(&problem, 0.3)
    } else {
        full_plane(&problem)
    };
    let team = WorkerTeam::new(1).unwrap();
    let mut cache = PsfCache::default();
    prepare_minor_cycle(&completion, &plans, &setup, &mut cache, &team).expect("prepare");
    assert_eq!(cache.summary(0, 0, 0).expect("measured beam").beam(), beam);
}

#[test]
fn a_minor_cycle_reuses_its_saved_psf_beam() {
    saved_beam_is_used_by_minor_preparation(false);
}

#[test]
fn an_automatic_mask_reuses_its_saved_psf_beam() {
    saved_beam_is_used_by_minor_preparation(true);
}

/// A saved off-centre plane already has CASA's chosen beam; the other
/// channel's larger beam must not replace it on restart.
#[test]
fn a_saved_off_centre_beam_is_not_resettled() {
    const WIDTH: usize = 64;
    let problem = problem(
        WIDTH,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Clark,
    );
    let scene = Scene::new(&problem);
    let completion = completion(&problem, &scene, |images| {
        let psf = images.psf.as_mut().expect("PSF");
        psf[..WIDTH * WIDTH].fill(0.0);
        psf[8 * WIDTH + 50] = 1.0;
        psf[32 * WIDTH + 32] = 0.99;
    });
    let first = casa_imaging_deconvolution::RestoringBeam::new(10.0, 6.0, 0.0).unwrap();
    let second = casa_imaging_deconvolution::RestoringBeam::new(20.0, 8.0, 0.0).unwrap();
    let mut setup = setup(&problem, None);
    setup.psf_beams = Some(vec![first, second].into());
    let team = WorkerTeam::new(1).unwrap();
    let mut cache = PsfCache::default();
    prepare_minor_cycle(
        &completion,
        &full_plane(&problem),
        &setup,
        &mut cache,
        &team,
    )
    .expect("prepare");
    assert_eq!(cache.summary(0, 0, 0).unwrap().beam(), first);
}

/// An incomplete saved beam grid is rejected instead of silently refitting.
#[test]
fn an_incomplete_saved_psf_beam_grid_is_refused() {
    let problem = problem(
        64,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Clark,
    );
    let scene = Scene::new(&problem).with_point([32, 32], &[1.0]);
    let completion = completion(&problem, &scene, |_| {});
    let mut setup = setup(&problem, None);
    setup.psf_beams = Some(Vec::new().into());
    let team = WorkerTeam::new(1).unwrap();
    let mut cache = PsfCache::default();
    assert!(matches!(
        prepare_minor_cycle(
            &completion,
            &full_plane(&problem),
            &setup,
            &mut cache,
            &team
        ),
        Err(
            casa_imaging_runtime::MinorCycleRunError::SavedBeamGridLength {
                expected: 1,
                actual: 0
            }
        )
    ));
}

/// An incomplete residual beam grid is rejected before automatic masking.
#[test]
fn an_incomplete_saved_residual_beam_grid_is_refused() {
    let problem = problem(
        64,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Clark,
    );
    let scene = Scene::new(&problem).with_point([32, 32], &[1.0]);
    let completion = completion(&problem, &scene, |_| {});
    let mut setup = setup(&problem, None);
    setup.mask_beams = Some(Vec::new().into());
    let team = WorkerTeam::new(1).unwrap();
    let mut cache = PsfCache::default();
    assert!(matches!(
        prepare_minor_cycle(
            &completion,
            &full_plane(&problem),
            &setup,
            &mut cache,
            &team
        ),
        Err(
            casa_imaging_runtime::MinorCycleRunError::SavedBeamGridLength {
                expected: 1,
                actual: 0
            }
        )
    ));
}

/// A blank channel's saved beam participates in its mask block's common
/// beam, even if that beam is larger than the data channel's.
#[test]
fn a_blank_channels_saved_beam_controls_automatic_mask_pruning() {
    let problem = problem(
        64,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Hogbom,
    );
    let scene = Scene::new(&problem).with_weights(vec![1.0, 0.0]);
    let completion = completion(&problem, &scene, |images| {
        images.residual.fill(0.0);
        for x in 30..35 {
            for y in 30..35 {
                images.residual[x * 64 + y] = 10.0;
            }
        }
    });
    let narrow = casa_imaging_deconvolution::RestoringBeam::new(2.5, 2.5, 0.0).unwrap();
    let broad = casa_imaging_deconvolution::RestoringBeam::new(10.0, 8.0, 0.0).unwrap();
    let mut setup = setup(&problem, None);
    setup.automask = true;
    setup.psf_beams = Some(vec![narrow, broad].into());
    let team = WorkerTeam::new(1).unwrap();
    let mut cache = PsfCache::default();
    let result = prepare_minor_cycle(
        &completion,
        &automatic_plan(&problem, 0.5),
        &setup,
        &mut cache,
        &team,
    )
    .expect("prepare");
    let row = result.auto_masks()[0]
        .as_ref()
        .unwrap()
        .plane(0, 0)
        .unwrap();
    assert_eq!((row.regions, row.pruned_regions), (1, 1));
}

/// CASA's autoMask uses the residual beam when both images have beams.
/// Its choice must not change the PSF sidelobe used by minor-cycle control.
#[test]
fn automatic_mask_prefers_the_residual_beam() {
    let problem = problem(
        64,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Hogbom,
    );
    let scene = Scene::new(&problem).with_weights(vec![1.0, 0.0]);
    let completion = completion(&problem, &scene, |images| {
        images.residual.fill(0.0);
        for x in 30..35 {
            for y in 30..35 {
                images.residual[x * 64 + y] = 10.0;
            }
        }
    });
    let narrow = casa_imaging_deconvolution::RestoringBeam::new(2.5, 2.5, 0.0).unwrap();
    let broad = casa_imaging_deconvolution::RestoringBeam::new(10.0, 8.0, 0.0).unwrap();
    let team = WorkerTeam::new(1).unwrap();
    let mut narrow_psf_sidelobe = None;
    for (psf, residual, pruned) in [(narrow, broad, 1), (broad, narrow, 0), (narrow, narrow, 0)] {
        let mut setup = setup(&problem, None);
        setup.automask = true;
        setup.psf_beams = Some(vec![psf; 2].into());
        setup.mask_beams = Some(vec![residual; 2].into());
        let result = prepare_minor_cycle(
            &completion,
            &automatic_plan(&problem, 0.5),
            &setup,
            &mut PsfCache::default(),
            &team,
        )
        .expect("prepare");
        let row = result.auto_masks()[0]
            .as_ref()
            .unwrap()
            .plane(0, 0)
            .unwrap();
        assert_eq!((row.regions, row.pruned_regions), (1, pruned));
        if psf == narrow {
            if let Some(expected) = narrow_psf_sidelobe {
                assert_eq!(result.statistics.psf_sidelobe, expected);
            } else {
                narrow_psf_sidelobe = Some(result.statistics.psf_sidelobe);
            }
        }
    }
}

/// A multi-term component records each Taylor coefficient's flux, and the
/// absolute flux sums them: `[1, −0.5]` at gain 0.1 is `[0.1, −0.05]`, 0.15.
#[test]
fn a_multi_term_component_records_every_coefficient() {
    let problem = problem(
        16,
        ReconstructionBasis::Taylor { terms: 2 },
        ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
    );
    let scene = Scene::new(&problem).with_point([8, 8], &[1.0, -0.5]);
    let completion = completion(&problem, &scene, |_| {});
    let (_, outcome) = minor_cycle(
        &problem,
        &completion,
        &setup(&problem, None),
        &controls(1, 0.1),
        1,
    );
    let trace = &outcome.summary.trace;
    assert_eq!(trace.len(), 2);
    assert_eq!(
        (trace[0].cell.coefficient(), trace[1].cell.coefficient()),
        (0, 1)
    );
    assert!((trace[0].flux - 0.1).abs() < 1e-6, "{}", trace[0].flux);
    assert!((trace[1].flux + 0.05).abs() < 1e-6, "{}", trace[1].flux);
    assert!((outcome.summary.absolute_flux - 0.15).abs() < 1e-6);
}

/// Under a response the Taylor minor cycle solves with every PSF moment
/// divided by the zeroth moment's peak, as without one (`normPSF` before
/// `SDAlgorithmMSMFS::initializeDeconvolver`, #704). A mosaic's PSF does not
/// peak at its sum of weights: halving the PSF moments, with the residuals
/// and sums unchanged, leaves `[1, −0.5]` at gain 0.1 as `[0.1, −0.05]`;
/// dividing by the sum of weights instead doubles it.
#[test]
fn a_taylor_component_under_a_response_divides_the_psf_by_its_peak() {
    let problem = problem(
        16,
        ReconstructionBasis::Taylor { terms: 2 },
        ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
    );
    let scene = Scene::new(&problem).with_point([8, 8], &[1.0, -0.5]);
    let response =
        MinorCycleImageResponse::new(ProductNormalization::FlatNoise, validity().primary_beam())
            .expect("response");
    let setup = setup(&problem, Some(response));
    for scale in [1.0_f32, 0.5] {
        let completion = completion(&problem, &scene, |images| {
            let psf = images.psf.as_mut().expect("initial PSF moments");
            for value in psf {
                *value *= scale;
            }
        });
        let (_, outcome) = minor_cycle(&problem, &completion, &setup, &controls(1, 0.1), 1);
        let trace = &outcome.summary.trace;
        assert_eq!(trace.len(), 2, "PSF scale {scale}");
        assert!(
            (trace[0].flux - 0.1).abs() < 1e-6,
            "PSF scale {scale}: {}",
            trace[0].flux
        );
        assert!(
            (trace[1].flux + 0.05).abs() < 1e-6,
            "PSF scale {scale}: {}",
            trace[1].flux
        );
    }
}

/// The planes of a cube clean concurrently, one per worker, and their terms
/// merge in model order: the outcome does not depend on the worker count.
#[test]
fn a_cube_cleans_the_same_on_any_number_of_workers() {
    let problem = problem(
        16,
        ReconstructionBasis::ChannelLocal { channels: 3 },
        ReconstructionAlgorithm::Hogbom,
    );
    let scene = Scene::new(&problem)
        .with_point([5, 9], &[1.3, -0.7, 0.4])
        .with_point([11, 4], &[-0.2, 0.9, 1.1])
        .with_noise(0.01);
    let completion = completion(&problem, &scene, |_| {});
    let setup = setup(&problem, None);
    let run = |workers| minor_cycle(&problem, &completion, &setup, &controls(20, 0.1), workers);
    let (one_peak, one) = run(1);
    for workers in [2, 3] {
        let (peak, many) = run(workers);
        assert_eq!(peak, one_peak, "{workers} workers");
        assert_eq!(many.summary, one.summary, "{workers} workers");
        assert_eq!(many.terms, one.terms, "{workers} workers");
    }
    assert_eq!(one.summary.stops.len(), 3);
}

/// Flat-noise response weights of a two-channel cube whose channels have
/// sum weights `weights` and hold the same 1 Jy point, its pass images
/// edited by `edit`.
fn cube_with_response(
    weights: Vec<f64>,
    edit: impl FnOnce(&mut PassImages),
) -> (
    CompiledProblem,
    MajorCycleCompletion,
    MinorCycleSetup<'static>,
) {
    let problem = problem(
        16,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Hogbom,
    );
    let scene = Scene::new(&problem)
        .with_weights(weights)
        .with_point([8, 8], &[1.0, 1.0]);
    let completion = completion(&problem, &scene, edit);
    let response =
        MinorCycleImageResponse::new(ProductNormalization::FlatNoise, validity().primary_beam())
            .expect("response");
    let setup = setup(&problem, Some(response));
    (problem, completion, setup)
}

/// Each model term's channel and increment.
fn increments(outcome: &MinorCycleOutcome) -> Vec<(usize, f64)> {
    outcome
        .terms
        .iter()
        .map(|term| (term.cell().coefficient(), term.increment().value()))
        .collect()
}

/// `SIImageStore::divideResidualByWeight` loops over every (polarization,
/// channel) plane: each channel's residual is normalised by that channel's
/// sum of weights and weight image. The same 1 Jy point under sum weights 1
/// and 4 cleans as 1 Jy in both channels.
#[test]
fn a_cube_normalises_each_channel_by_its_own_weights() {
    let (problem, completion, setup) = cube_with_response(vec![1.0, 4.0], |_| {});
    let (_, outcome) = minor_cycle(&problem, &completion, &setup, &controls(1, 1.0), 1);
    let increments = increments(&outcome);
    assert_eq!(increments.len(), 2, "{increments:?}");
    for (channel, value) in increments {
        assert!((value - 1.0).abs() < 1e-6, "channel {channel}: {value}");
    }
}

/// Each channel's residual is divided by that channel's published `.sumwt`
/// and its weight image by that channel's PSF sum. A cube's sensitivity is
/// its plane's PSF sum, so the weight image normalises to one only when a
/// plane reads its own sensitivity and sum: under sum weights 1 and 4
/// published as 2 and 5, the same 1 Jy point cleans as 1/2 and 4/5 Jy.
#[test]
fn a_cube_channel_divides_by_its_own_published_sum_of_weights() {
    let weights = [1.0, 4.0];
    let published = [2.0, 5.0];
    let (problem, completion, setup) = cube_with_response(weights.to_vec(), |images| {
        images.published_sum_weights = published.to_vec();
    });
    let (_, outcome) = minor_cycle(&problem, &completion, &setup, &controls(1, 1.0), 1);
    let increments = increments(&outcome);
    assert_eq!(increments.len(), 2, "{increments:?}");
    for (channel, value) in increments {
        let expected = weights[channel] / published[channel];
        assert!(
            (value - expected).abs() < 1e-6,
            "channel {channel}: {value}, expected {expected}"
        );
    }
}

/// A mosaic cube keeps each channel's gridded weight image (#672, #659).
/// Under flat noise a channel's residual `R` normalises to `R / W` for a
/// constant weight image `W` (`(R / s) / (√(W / s) · √(W / s))`), so the
/// same point at sum weights 8 and 4 under weight images 16 and 36 cleans as
/// 8/16 and 4/36 Jy. Channel 0's image read for both channels would give
/// 4/16 Jy for channel 1, and each plane's sum weight alone 1 Jy for both.
#[test]
fn a_mosaic_cube_normalises_each_channel_by_its_own_weight_image() {
    let problem = problem(
        8,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Hogbom,
    );
    let scene = Scene::new(&problem);
    let psf = scene.unit_psf();
    let (sums, weights) = ([8.0, 4.0], [16.0_f32, 36.0]);
    let completion = completion(&problem, &scene, |images| {
        let planes = sums
            .iter()
            .flat_map(|&sum| psf.iter().map(move |value| (sum * value) as f32))
            .collect::<Vec<_>>();
        images.sum_weights = sums.to_vec();
        images.published_sum_weights = sums.to_vec();
        images.weight = Some(
            weights
                .iter()
                .flat_map(|&weight| std::iter::repeat_n(weight, psf.len()))
                .collect(),
        );
        images.residual.clone_from(&planes);
        images.psf = Some(planes);
    });
    let response =
        MinorCycleImageResponse::new(ProductNormalization::FlatNoise, validity().primary_beam())
            .expect("response");
    let (peak, outcome) = minor_cycle(
        &problem,
        &completion,
        &setup(&problem, Some(response)),
        &controls(1, 1.0),
        1,
    );
    assert!((peak - 0.5).abs() < 1e-6, "{peak}");
    let increments = increments(&outcome);
    assert_eq!(increments.len(), 2, "{increments:?}");
    for (channel, value) in increments {
        let expected = sums[channel] / f64::from(weights[channel]);
        assert!(
            (value - expected).abs() < 1e-6,
            "channel {channel}: {value}, expected {expected}"
        );
    }
}

/// The nsigma threshold `prepare_minor_cycle` measures, under `noise`, on
/// an 8×8 mosaic plane whose beam covers a 4×4 block holding the residuals
/// 0.01 to 0.16 Jy; the residual outside it is 0.5 Jy before
/// normalization.
fn mosaic_nsigma_threshold(noise: casa_imaging_deconvolution::NoiseEstimate) -> f64 {
    let problem = problem(
        8,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Hogbom,
    );
    let scene = Scene::new(&problem);
    let psf = scene.unit_psf();
    let inside = |index: usize| (2..6).contains(&(index % 8)) && (2..6).contains(&(index / 8));
    let completion = completion(&problem, &scene, |images| {
        images.sum_weights = vec![1.0];
        images.published_sum_weights = vec![1.0];
        images.weight = Some(
            (0..psf.len())
                .map(|index| if inside(index) { 1.0 } else { 0.0 })
                .collect(),
        );
        let mut next = 0.0_f32;
        images.residual = (0..psf.len())
            .map(|index| {
                if inside(index) {
                    next += 0.01;
                    next
                } else {
                    // `normalize_weighted_residual_sample` zeroes the
                    // residual where the beam is not above the cutoff.
                    0.5
                }
            })
            .collect();
        images.psf = Some(psf.iter().map(|value| *value as f32).collect());
    });
    let response =
        MinorCycleImageResponse::new(ProductNormalization::FlatNoise, validity().primary_beam())
            .expect("response");
    let mut setup = setup(&problem, Some(response));
    setup.nsigma = 3.0;
    setup.noise = noise;
    let team = WorkerTeam::new(1).expect("team");
    prepare_minor_cycle(
        &completion,
        &full_plane(&problem),
        &setup,
        &mut PsfCache::default(),
        &team,
    )
    .expect("prepare")
    .statistics
    .nsigma_threshold
}

/// `SIImageStore::calcRobustRMS` measures a plane's `nsigma` noise where its
/// primary beam exceeds tclean's `pbmask` of 0, and a mosaic's beam
/// (`makePBFromWeight`) is zero outside `pblimit`. The beam's residuals,
/// 0.01 to 0.16 Jy, have the median 0.085 and the median absolute
/// deviation 0.04: three sigma is 3 · 1.4826 · 0.04 Jy. Over the whole
/// plane, three quarters of it zero outside the beam, the deviation would
/// be zero and so the threshold.
#[test]
fn a_mosaic_measures_its_noise_inside_its_primary_beam() {
    let threshold = mosaic_nsigma_threshold(casa_imaging_deconvolution::NoiseEstimate::Fast);
    let expected = 3.0 * 1.4826 * 0.04;
    assert!(
        (threshold - expected).abs() < 1e-6,
        "{threshold}, expected {expected}"
    );
}

/// The same plane under `fastnoise=False`: `calcRobustImageStatistics`
/// takes the same primary-beam pixels, and Chauvenet's criterion at 16
/// samples (a z-score between 2 and 2.5, about 0.1 Jy either side of the
/// mean) keeps all of 0.01 to 0.16 Jy, so the threshold is the fast one;
/// over the whole plane the zeros would again leave no deviation.
#[test]
fn a_mosaic_measures_its_robust_noise_inside_its_primary_beam() {
    let threshold = mosaic_nsigma_threshold(casa_imaging_deconvolution::NoiseEstimate::Robust);
    let expected = 3.0 * 1.4826 * 0.04;
    assert!(
        (threshold - expected).abs() < 1e-6,
        "{threshold}, expected {expected}"
    );
}

/// A channel no sample reached (CASA's `test_cube_flagged_mosaic_hogbom`)
/// is skipped; the other channels still clean with their own weights.
#[test]
fn a_cube_with_an_empty_first_channel_cleans_the_others() {
    let (problem, completion, setup) = cube_with_response(vec![0.0, 1.0], |_| {});
    let (_, outcome) = minor_cycle(&problem, &completion, &setup, &controls(1, 1.0), 1);
    let increments = increments(&outcome);
    assert_eq!(increments.len(), 1, "{increments:?}");
    assert_eq!(increments[0].0, 1);
    assert!((increments[0].1 - 1.0).abs() < 1e-6, "{increments:?}");
}

/// Clark chooses one joint polarization component, rather than one per
/// plane, as CASA's two-position IV experiment does (#1140).
#[test]
fn clark_solves_a_channels_polarizations_as_one_component() {
    use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesV};
    let problem = problems::reconstruction_problem_of_shape_and_polarizations(
        31,
        [64, 64],
        1,
        1,
        ReconstructionBasis::ChannelLocal { channels: 1 },
        ReconstructionAlgorithm::Clark,
        ReconstructionControls::new(1, 0.17, 0.0),
        vec![StokesI, StokesV],
    );
    let scene = Scene::new(&problem);
    let completion = completion(&problem, &scene, |images| {
        let cells = 64 * 64;
        images.polarizations = 2;
        let psf = images.psf.as_ref().unwrap().clone();
        images.psf = Some([psf.as_slice(), psf.as_slice()].concat());
        images.sum_weights = vec![1.0, 1.0];
        images.published_sum_weights = vec![1.0, 1.0];
        images.residual = vec![0.0; 2 * cells];
        images.residual[20 * 64 + 20] = 3.0;
        images.residual[40 * 64 + 40] = 1.0;
        images.residual[cells + 40 * 64 + 40] = 3.0;
    });
    let (_, outcome) = minor_cycle(
        &problem,
        &completion,
        &setup(&problem, None),
        &controls(1, 0.17),
        1,
    );
    assert_eq!(outcome.summary.iterations, 1);
    assert_eq!(outcome.summary.planes.len(), 1);
    assert_eq!(outcome.summary.planes[0].polarizations, 2);
    let trace = &outcome.summary.trace;
    assert_eq!(trace.len(), 2);
    assert_eq!(trace[0].cell.pixel(), [40, 40]);
    assert_eq!(trace[1].cell.pixel(), [40, 40]);
    assert_eq!(trace[0].cell.polarization(), 0);
    assert_eq!(trace[1].cell.polarization(), 1);
    assert!((trace[0].flux - 0.17).abs() < 1e-6);
    assert!((trace[1].flux - 0.51).abs() < 1e-6);
}

/// CASA's clarkstokes gives each polarization its own peak and component
/// rather than Clark's joint polarization component (#1126).
#[test]
fn clarkstokes_solves_each_polarization_separately() {
    use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesV};
    let problem = problems::reconstruction_problem_of_shape_and_polarizations(
        31,
        [64, 64],
        1,
        1,
        ReconstructionBasis::ChannelLocal { channels: 1 },
        ReconstructionAlgorithm::ClarkStokes,
        ReconstructionControls::new(1, 0.17, 0.0),
        vec![StokesI, StokesV],
    );
    let scene = Scene::new(&problem);
    let completion = completion(&problem, &scene, |images| {
        let cells = 64 * 64;
        images.polarizations = 2;
        let psf = images.psf.as_ref().unwrap().clone();
        images.psf = Some([psf.as_slice(), psf.as_slice()].concat());
        images.sum_weights = vec![1.0, 1.0];
        images.published_sum_weights = vec![1.0, 1.0];
        images.residual = vec![0.0; 2 * cells];
        images.residual[20 * 64 + 20] = 3.0;
        images.residual[40 * 64 + 40] = 1.0;
        images.residual[cells + 40 * 64 + 40] = 3.0;
    });
    let (_, outcome) = minor_cycle(
        &problem,
        &completion,
        &setup(&problem, None),
        &controls(1, 0.17),
        1,
    );
    assert_eq!(outcome.summary.iterations, 2);
    assert_eq!(outcome.summary.planes.len(), 2);
    assert!(
        outcome
            .summary
            .planes
            .iter()
            .all(|row| row.polarizations == 1)
    );
    let trace = &outcome.summary.trace;
    assert_eq!(trace.len(), 2);
    assert_eq!(trace[0].cell.pixel(), [20, 20]);
    assert_eq!(trace[1].cell.pixel(), [40, 40]);
    assert_eq!(trace[0].cell.polarization(), 0);
    assert_eq!(trace[1].cell.polarization(), 1);
    assert!((trace[0].flux - 0.51).abs() < 1e-6);
    assert!((trace[1].flux - 0.51).abs() < 1e-6);
}

/// A channel with no samples does not disable joint Clark on a healthy
/// channel in the same slab (#1140).
#[test]
fn an_empty_channel_does_not_split_another_channels_clark_polarizations() {
    use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesV};
    let width = 64;
    let problem = problems::reconstruction_problem_of_shape_and_polarizations(
        31,
        [width, width],
        2,
        1,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Clark,
        ReconstructionControls::new(1, 0.17, 0.0),
        vec![StokesI, StokesV],
    );
    let scene = Scene::new(&problem);
    let completion = completion(&problem, &scene, |images| {
        let cells = width * width;
        images.polarizations = 2;
        let unit = scene
            .unit_psf()
            .into_iter()
            .map(|v| v as f32)
            .collect::<Vec<_>>();
        images.psf = Some(
            [
                unit.as_slice(),
                unit.as_slice(),
                &vec![0.0; cells],
                &vec![0.0; cells],
            ]
            .concat(),
        );
        images.sum_weights = vec![1.0, 1.0, 0.0, 0.0];
        images.published_sum_weights = images.sum_weights.clone();
        images.residual = vec![0.0; 4 * cells];
        images.residual[20 * width + 20] = 3.0;
        images.residual[40 * width + 40] = 1.0;
        images.residual[cells + 40 * width + 40] = 3.0;
    });
    let (_, outcome) = minor_cycle(
        &problem,
        &completion,
        &setup(&problem, None),
        &controls(1, 0.17),
        1,
    );
    assert_eq!(outcome.summary.iterations, 1);
    assert_eq!(outcome.summary.planes.len(), 2, "one row per channel");
    assert_eq!(outcome.summary.planes[1].channel, 1);
    assert_eq!(outcome.summary.planes[1].polarization, 0);
    assert_eq!(outcome.summary.trace.len(), 2);
    assert!(
        outcome
            .summary
            .trace
            .iter()
            .all(|c| c.cell.pixel() == [40, 40])
    );
}

/// CASA's joint IV Clark runs one channel even if either polarization has
/// no data, retaining a single iteration record (#1140).
#[test]
fn joint_clark_keeps_a_blank_polarization_in_its_channel() {
    use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesV};
    for blank in 0..2 {
        let width = 64;
        let problem = problems::reconstruction_problem_of_shape_and_polarizations(
            31,
            [width, width],
            1,
            1,
            ReconstructionBasis::ChannelLocal { channels: 1 },
            ReconstructionAlgorithm::Clark,
            ReconstructionControls::new(1, 0.17, 0.0),
            vec![StokesI, StokesV],
        );
        let scene = Scene::new(&problem);
        let completion = completion(&problem, &scene, |images| {
            let cells = width * width;
            images.polarizations = 2;
            let unit = scene
                .unit_psf()
                .into_iter()
                .map(|v| v as f32)
                .collect::<Vec<_>>();
            images.psf = Some(unit.repeat(2));
            images.psf.as_mut().unwrap()[blank * cells..(blank + 1) * cells].fill(0.0);
            images.sum_weights = vec![1.0, 1.0];
            images.sum_weights[blank] = 0.0;
            images.published_sum_weights = images.sum_weights.clone();
            images.residual = vec![0.0; 2 * cells];
            images.residual[(1 - blank) * cells + 40 * width + 40] = 3.0;
        });
        let (_, outcome) = minor_cycle(
            &problem,
            &completion,
            &setup(&problem, None),
            &controls(1, 0.17),
            1,
        );
        assert_eq!(outcome.summary.iterations, 1, "blank={blank}");
        assert_eq!(outcome.summary.planes.len(), 1, "blank={blank}");
        assert_eq!(outcome.terms.len(), 1, "blank={blank}");
        let changed = outcome
            .summary
            .trace
            .iter()
            .find(|c| c.flux != 0.0)
            .unwrap();
        assert_eq!(changed.cell.polarization(), 1 - blank);
        assert!((changed.flux - 0.51).abs() < 1e-6);
    }
}

/// CASA's Clark uses the first polarization's PSF even when it is blank;
/// a zero PSF leaves the other polarization's peak unchanged (#1140).
#[test]
fn joint_clark_uses_the_blank_first_polarizations_psf() {
    use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesV};
    let width = 64;
    let problem = problems::reconstruction_problem_of_shape_and_polarizations(
        31,
        [width, width],
        1,
        1,
        ReconstructionBasis::ChannelLocal { channels: 1 },
        ReconstructionAlgorithm::Clark,
        ReconstructionControls::new(20, 0.17, 0.0),
        vec![StokesI, StokesV],
    );
    let scene = Scene::new(&problem);
    let completion = completion(&problem, &scene, |images| {
        let cells = width * width;
        images.polarizations = 2;
        let unit = scene
            .unit_psf()
            .into_iter()
            .map(|v| v as f32)
            .collect::<Vec<_>>();
        images.psf = Some([vec![0.0; cells].as_slice(), unit.as_slice()].concat());
        images.sum_weights = vec![0.0, 1.0];
        images.published_sum_weights = images.sum_weights.clone();
        images.residual = vec![0.0; 2 * cells];
        images.residual[cells + 40 * width + 40] = 3.0;
    });
    let (_, outcome) = minor_cycle(
        &problem,
        &completion,
        &setup(&problem, None),
        &controls(20, 0.17),
        1,
    );
    assert_eq!(outcome.summary.iterations, 10);
    assert!((outcome.summary.peak - 3.0).abs() < 1e-6);
    assert!((outcome.summary.model_flux - 5.1).abs() < 1e-6);
}

/// CASA measures masks over both Stokes planes, but Clark reads the first
/// plane's component mask, even when only the second mask is nonempty (#1140).
#[test]
fn joint_clark_counts_all_masks_but_uses_the_first_for_components() {
    use casa_imaging_deconvolution::{MaskPlanes, PlaneStop};
    use casa_imaging_model::PolarizationCoordinate::{StokesI, StokesV};
    use std::sync::Arc;
    let width = 64;
    let problem = problems::reconstruction_problem_of_shape_and_polarizations(
        31,
        [width, width],
        1,
        1,
        ReconstructionBasis::ChannelLocal { channels: 1 },
        ReconstructionAlgorithm::Clark,
        ReconstructionControls::new(1, 0.17, 0.0),
        vec![StokesI, StokesV],
    );
    let scene = Scene::new(&problem);
    let completion = completion(&problem, &scene, |images| {
        let cells = width * width;
        images.polarizations = 2;
        let psf = images.psf.take().unwrap();
        images.psf = Some(psf.repeat(2));
        images.sum_weights = vec![1.0; 2];
        images.published_sum_weights = images.sum_weights.clone();
        images.residual = vec![0.0; 2 * cells];
        images.residual[cells + 40 * width + 40] = 3.0;
    });
    let cells = width * width;
    let mut support = vec![false; cells];
    support.extend(vec![true; cells]);
    let masks = ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::Reprojected {
        coordinate: problem.geometry().domains()[0].direction(),
        planes: MaskPlanes::new(1, 2).unwrap(),
        support: Arc::from(support),
    }])
    .unwrap();
    let team = WorkerTeam::new(1).unwrap();
    let mut cache = PsfCache::default();
    let configured = setup(&problem, None);
    let prepared =
        prepare_minor_cycle(&completion, &masks, &configured, &mut cache, &team).unwrap();
    let outcome = run_minor_cycle(
        prepared,
        &completion,
        &configured,
        &controls(1, 0.17),
        &mut cache,
        &team,
        (&HOST, &POLICY),
    )
    .unwrap();
    assert_eq!(outcome.summary.iterations, 0);
    assert!(outcome.terms.is_empty());
    assert_eq!(outcome.summary.planes.len(), 1);
    let row = outcome.summary.planes[0];
    assert_eq!(row.mask_sum, cells);
    assert!((row.start_peak - 3.0).abs() < 1e-6);
    assert_eq!(row.stop, PlaneStop::ZeroIterations);
}
