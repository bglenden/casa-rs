// SPDX-License-Identifier: LGPL-3.0-or-later

//! Laws of the minor-cycle adapter (`prepare_minor_cycle`,
//! `run_minor_cycle`) on synthetic passes over a known sky: CASA's residual
//! normalisation under a direction-dependent response, the diagnostics the
//! task publishes, and results independent of the worker count. The
//! problems and passes are the reconstruction tests' shared fixtures.

#[path = "../../casa-imaging-reconstruction/tests/support/problems.rs"]
mod problems;
#[path = "../../casa-imaging-reconstruction/tests/support/synthetic_pass.rs"]
mod synthetic_pass;

use casa_imaging_deconvolution::CycleControls;
use casa_imaging_model::{
    CompiledProblem, HogbomIterationAccounting, ProductNormalization, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionControls,
};
use casa_imaging_reconstruction::{
    ImageDomainReconstructionMaskPlans, MajorCycle, MajorCycleCompletion, MinorCycleImageResponse,
    PassImages, ReconstructionMaskPlan,
};
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{
    MinorCycleOutcome, MinorCycleSetup, PsfCache, prepare_minor_cycle, run_minor_cycle,
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

fn setup(problem: &CompiledProblem, response: Option<MinorCycleImageResponse>) -> MinorCycleSetup {
    MinorCycleSetup {
        algorithm: problem.reconstruction().algorithm().clone(),
        accounting: HogbomIterationAccounting::Strict,
        response,
        nsigma: 0.0,
        automask: false,
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
    let outcome = run_minor_cycle(prepared, completion, setup, controls, &mut cache, &team)
        .expect("minor cycle");
    (peak, outcome)
}

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
