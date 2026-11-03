// SPDX-License-Identifier: LGPL-3.0-or-later

//! The pixels a minor cycle's statistics read (#1078, #1079): tclean's
//! residual carries the `.pb`'s support as its pixel mask
//! (`SIImageStore::divideResidualByWeight`), so its peaks and automatic
//! mask read only the pixels inside the beam, and its `nsigma` noise
//! reads the pixels where the `.pb` exceeds `pbmask`, 0
//! (`SIImageStore::calcRobustRMS`), every pixel when none does. The beam
//! is the weight image's under a response and the telescope's analytic
//! beam otherwise; each law places the beam's edge across the plane and
//! compares with statistics computed here of the pixels on either side.

#[path = "../../casa-imaging-deconvolution/tests/support/problems.rs"]
mod problems;
#[path = "../../casa-imaging-deconvolution/tests/support/synthetic_pass.rs"]
mod synthetic_pass;

use std::collections::BTreeMap;

use casa_imaging_deconvolution::{
    AutoMultithreshControls, ImageDomainReconstructionMaskPlans, MajorCycle, MajorCycleCompletion,
    NoiseEstimate, PassImages, ReconstructionMaskPlan,
};
use casa_imaging_model::{
    CompiledProblem, HogbomIterationAccounting, ImageDomainRole, ProductNormalization,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionControls,
};
use casa_imaging_products::{
    AnalyticPrimaryBeam, AnalyticPrimaryBeamModel, MinorCycleImageResponse, PrimaryBeamCentres,
};
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{MinorCycleSetup, PreparedMinorCycle, PsfCache, prepare_minor_cycle};
use problems::{empty_final_model, model_lifecycle, reconstruction_problem, validity};
use synthetic_pass::{BLOCKS, SAMPLES, Scene};

/// The plane's width in pixels.
const WIDTH: usize = 16;

fn problem() -> CompiledProblem {
    reconstruction_problem(
        31,
        WIDTH,
        1,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Hogbom,
        ReconstructionControls::new(100, 0.1, 0.0),
    )
}

/// One pointing at `[x, (WIDTH − 1) / 2]`.
fn centres(x: f64) -> PrimaryBeamCentres {
    BTreeMap::from([(ImageDomainRole::Main, vec![[x, (WIDTH as f64 - 1.0) / 2.0]])])
}

/// The EVLA's L-band beam at 1.05 GHz, the problem's frequency, centred
/// left of the plane so far that its `pblimit` edge (0.2) crosses it: the
/// beam's support keeps between a quarter and three quarters of the
/// plane's pixels.
fn edge_centres(problem: &CompiledProblem) -> PrimaryBeamCentres {
    let kept = |x: f64| {
        let centres = centres(x);
        AnalyticPrimaryBeam::new(
            problem,
            AnalyticPrimaryBeamModel::CasaEvlaCommon,
            Some(&centres),
        )
        .expect("L band")
        .expect("an analytic beam")
        .support(&ImageDomainRole::Main, 0)
        .expect("the plane's support")
        .iter()
        .filter(|kept| **kept)
        .count()
    };
    let cells = WIDTH * WIDTH;
    let (mut near, mut far) = (0.0_f64, -1.0e5_f64);
    assert_eq!(kept(near), cells, "the beam covers the plane at its centre");
    assert_eq!(kept(far), 0, "the beam misses the plane far away");
    for _ in 0..200 {
        if (cells / 4..=3 * cells / 4).contains(&kept(near)) {
            break;
        }
        let middle = 0.5 * (near + far);
        if kept(middle) >= cells / 2 {
            near = middle;
        } else {
            far = middle;
        }
    }
    centres(near)
}

/// The support of `beam`, x-major.
fn support(beam: &AnalyticPrimaryBeam<'_>) -> Vec<bool> {
    beam.support(&ImageDomainRole::Main, 0)
        .expect("the plane's support")
}

/// The completion of an initial pass over the empty model whose residual
/// is `residual` (x-major, sum of weights one) and whose PSF is the
/// scene's, its images edited by `edit` before they are assembled.
fn completion(
    problem: &CompiledProblem,
    residual: &[f32],
    edit: impl FnOnce(&mut PassImages),
) -> MajorCycleCompletion {
    let scene = Scene::new(problem);
    let lifecycle = model_lifecycle(problem);
    let mut cycle = MajorCycle::initial(
        problem,
        empty_final_model(&lifecycle),
        scene.resident_storage(),
    )
    .expect("major cycle");
    let (model, mut pass) = cycle.parts();
    let mut images = scene.pass_images(model, true);
    images.sum_weights = vec![1.0];
    images.published_sum_weights = vec![1.0];
    images.residual = residual.to_vec();
    images.psf = Some(scene.unit_psf().iter().map(|value| *value as f32).collect());
    edit(&mut images);
    pass.append(images).expect("images");
    cycle.finish(SAMPLES, BLOCKS).expect("complete")
}

/// A residual of `inside` pixels from 0.01 Jy upwards in steps of
/// `step`, x-major, and `outside` elsewhere.
fn residual(inside: &[bool], step: f32, outside: f32) -> Vec<f32> {
    let mut next = 0.0_f32;
    inside
        .iter()
        .map(|inside| {
            if *inside {
                next += step;
                next
            } else {
                outside
            }
        })
        .collect()
}

/// The classical median and 1.4826 times the median absolute deviation of
/// `residual`'s pixels where `kept`.
fn classical(residual: &[f32], kept: &[bool]) -> (f64, f64) {
    let median = |values: &mut Vec<f64>| {
        values.sort_by(f64::total_cmp);
        let middle = values.len() / 2;
        if values.len().is_multiple_of(2) {
            0.5 * (values[middle - 1] + values[middle])
        } else {
            values[middle]
        }
    };
    let mut values = residual
        .iter()
        .zip(kept)
        .filter(|(_, kept)| **kept)
        .map(|(value, _)| f64::from(*value))
        .collect::<Vec<_>>();
    let centre = median(&mut values);
    let mut deviations = values.iter().map(|value| (value - centre).abs()).collect();
    (centre, 1.4826 * median(&mut deviations))
}

fn setup<'a>(beam: Option<AnalyticPrimaryBeam<'a>>) -> MinorCycleSetup<'a> {
    MinorCycleSetup {
        algorithm: ReconstructionAlgorithm::Hogbom,
        accounting: HogbomIterationAccounting::Strict,
        response: None,
        nsigma: 0.0,
        noise: NoiseEstimate::Fast,
        automask: false,
        primary_beam: beam,
        residual_pixel_mask: true,
    }
}

fn full_plane(problem: &CompiledProblem) -> ImageDomainReconstructionMaskPlans {
    ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::FullPlane {
        coordinate: problem.geometry().domains()[0].direction(),
    }])
    .expect("full-plane mask")
}

fn automask(problem: &CompiledProblem) -> ImageDomainReconstructionMaskPlans {
    ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::AutoMultithresh {
        coordinate: problem.geometry().domains()[0].direction(),
        controls: AutoMultithreshControls {
            sidelobe_factor: 3.0,
            noise_factor: 5.0,
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
        },
        completed_major_cycles: 0,
        cycle_threshold_reached: false,
        previous: None,
        stopped_channels: Box::new([]),
    }])
    .expect("automatic mask")
}

fn prepare(
    completion: &MajorCycleCompletion,
    plans: &ImageDomainReconstructionMaskPlans,
    setup: &MinorCycleSetup<'_>,
) -> PreparedMinorCycle {
    let team = WorkerTeam::new(1).expect("team");
    prepare_minor_cycle(completion, plans, setup, &mut PsfCache::default(), &team).expect("prepare")
}

fn close(actual: f64, expected: f64) -> bool {
    (actual - expected).abs() <= 1.0e-9 * expected.abs().max(1.0)
}

/// #1078: a standard-gridder plane's `nsigma` noise is `calcRobustRMS`'s
/// inside the analytic `.pb`, under either estimate. The residual outside
/// the beam, 0.5 Jy, is far from the ramp inside it, so over the whole
/// plane the noise would differ.
#[test]
fn a_plane_without_a_response_measures_its_noise_inside_its_analytic_beam() {
    let problem = problem();
    let centres = edge_centres(&problem);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    let inside = support(&beam);
    let residual = residual(&inside, 0.001, 0.5);
    let completion = completion(&problem, &residual, |_| {});
    let (_, rms) = classical(&residual, &inside);
    let (_, whole) = classical(&residual, &vec![true; inside.len()]);
    assert!(
        !close(rms, whole),
        "the law separates the two: {rms} {whole}"
    );
    for noise in [NoiseEstimate::Fast, NoiseEstimate::Robust] {
        let mut setup = setup(Some(beam));
        setup.nsigma = 3.0;
        setup.noise = noise;
        let threshold = prepare(&completion, &full_plane(&problem), &setup)
            .statistics
            .nsigma_threshold;
        assert!(
            close(threshold, 3.0 * rms),
            "{noise:?}: {threshold}, expected {}",
            3.0 * rms
        );
    }
}

/// CAS-13318: where no pixel of the `.pb` exceeds the cutoff (an outlier
/// beyond the beam), `calcRobustRMS` measures every pixel.
#[test]
fn a_plane_outside_its_beam_measures_its_noise_over_every_pixel() {
    let problem = problem();
    let centres = centres(-1.0e5);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    assert!(support(&beam).iter().all(|kept| !kept));
    let every = vec![true; WIDTH * WIDTH];
    let residual = residual(&every, 0.001, 0.0);
    let completion = completion(&problem, &residual, |_| {});
    let mut setup = setup(Some(beam));
    setup.nsigma = 3.0;
    let threshold = prepare(&completion, &full_plane(&problem), &setup)
        .statistics
        .nsigma_threshold;
    let (_, rms) = classical(&residual, &every);
    assert!(
        close(threshold, 3.0 * rms),
        "{threshold}, expected {}",
        3.0 * rms
    );
}

/// tclean's peak residuals, within the mask and without it, read the
/// residual's pixel mask (`getPeakResidualWithinMask`, `getPeakResidual`):
/// a peak outside the `.pb` counts only where `pblimit` is negative and
/// the residual carries no mask. The mask's size is the clean mask's
/// either way.
#[test]
fn a_plane_takes_its_peaks_inside_the_residual_pixel_mask() {
    let problem = problem();
    let centres = edge_centres(&problem);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    let inside = support(&beam);
    let mut residual = residual(&inside, 0.001, 0.0);
    let peak_inside = inside
        .iter()
        .position(|kept| *kept)
        .expect("a pixel inside");
    let peak_outside = inside
        .iter()
        .position(|kept| !kept)
        .expect("a pixel outside");
    residual[peak_inside] = 1.0;
    residual[peak_outside] = -2.0;
    let completion = completion(&problem, &residual, |_| {});
    for (masks_residual, expected) in [(true, 1.0), (false, 2.0)] {
        let mut setup = setup(Some(beam));
        setup.residual_pixel_mask = masks_residual;
        let statistics = prepare(&completion, &full_plane(&problem), &setup).statistics;
        assert_eq!(
            (
                statistics.peak,
                statistics.peak_no_mask,
                statistics.mask_sum
            ),
            (expected, expected, WIDTH * WIDTH),
            "pixel mask {masks_residual}"
        );
    }
}

/// #1079: a standard-gridder plane's automatic mask takes its median and
/// noise inside the analytic `.pb`, and masks nothing outside it
/// (`SDMaskHandler::autoMask` attaches the residual's mask). A 2 Jy point
/// inside the beam is masked; the 0.5 Jy outside it lies above the
/// threshold, so without the pixel mask the mask would take it too.
#[test]
fn an_automatic_mask_reads_inside_the_analytic_beam() {
    let problem = problem();
    let centres = edge_centres(&problem);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    let inside = support(&beam);
    let mut residual = residual(&inside, 0.001, 0.5);
    residual[inside
        .iter()
        .position(|kept| *kept)
        .expect("a pixel inside")] = 2.0;
    let completion = completion(&problem, &residual, |_| {});
    let mut setup = setup(Some(beam));
    setup.automask = true;
    let prepared = prepare(&completion, &automask(&problem), &setup);
    let row = prepared.auto_masks()[0]
        .as_ref()
        .expect("automatic evidence")
        .plane(0, 0)
        .copied()
        .expect("the plane's row");
    let (median, rms) = classical(&residual, &inside);
    assert!(
        close(row.median, median) && close(row.robust_rms, rms),
        "({}, {}), expected ({median}, {rms})",
        row.median,
        row.robust_rms
    );
    let mask = prepared.masks().get(0).expect("the mask");
    let masked = mask.plane(0, 0).expect("the plane");
    assert!(masked.iter().any(|set| *set), "the point is masked");
    assert!(
        masked
            .iter()
            .zip(&inside)
            .all(|(set, inside)| !set || *inside),
        "nothing outside the beam is masked"
    );
}

/// At a run's first boundary with `nsigma`, tclean's automatic mask takes
/// the median and noise `calcRobustRMS` measured inside the `.pb`, even
/// where `pblimit` is negative and the residual carries no mask; without
/// `nsigma` it measures every pixel the residual keeps.
#[test]
fn the_first_automatic_boundary_with_nsigma_reads_calc_robust_rms_pixels() {
    let problem = problem();
    let centres = edge_centres(&problem);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    let inside = support(&beam);
    let every = vec![true; inside.len()];
    let residual = residual(&inside, 0.001, 0.5);
    let completion = completion(&problem, &residual, |_| {});
    for (nsigma, kept) in [(3.0, &inside), (0.0, &every)] {
        let mut setup = setup(Some(beam));
        setup.automask = true;
        setup.residual_pixel_mask = false;
        setup.nsigma = nsigma;
        let prepared = prepare(&completion, &automask(&problem), &setup);
        let row = prepared.auto_masks()[0]
            .as_ref()
            .expect("automatic evidence")
            .plane(0, 0)
            .copied()
            .expect("the plane's row");
        let (median, rms) = classical(&residual, kept);
        assert!(
            close(row.median, median) && close(row.robust_rms, rms),
            "nsigma {nsigma}: ({}, {}), expected ({median}, {rms})",
            row.median,
            row.robust_rms
        );
    }
}

/// #1079: a mosaic plane's automatic mask takes its statistics inside its
/// weight image's beam (`makePBFromWeight`): its normalized residual is
/// zero outside, where the beam covers less than half the plane, so over
/// every pixel its median and deviation would be zero.
#[test]
fn an_automatic_mosaic_mask_reads_inside_its_primary_beam() {
    let problem = problem();
    let inside = (0..WIDTH * WIDTH)
        .map(|index| (4..12).contains(&(index % WIDTH)) && (4..12).contains(&(index / WIDTH)))
        .collect::<Vec<_>>();
    // `normalize_weighted_residual_sample` zeroes the residual where the
    // beam is not above the cutoff, whatever it was.
    let raw = residual(&inside, 0.001, 0.5);
    let completion = completion(&problem, &raw, |images| {
        images.weight = Some(
            inside
                .iter()
                .map(|inside| if *inside { 1.0 } else { 0.0 })
                .collect(),
        );
    });
    let response =
        MinorCycleImageResponse::new(ProductNormalization::FlatNoise, validity().primary_beam())
            .expect("response");
    let mut setup = setup(None);
    setup.response = Some(response);
    setup.automask = true;
    let prepared = prepare(&completion, &automask(&problem), &setup);
    let row = prepared.auto_masks()[0]
        .as_ref()
        .expect("automatic evidence")
        .plane(0, 0)
        .copied()
        .expect("the plane's row");
    let (median, rms) = classical(&raw, &inside);
    assert!(rms > 0.0);
    assert!(
        close(row.median, median) && close(row.robust_rms, rms),
        "({}, {}), expected ({median}, {rms})",
        row.median,
        row.robust_rms
    );
}

/// CAS-13318 is decided over the whole image: `calcRobustRMS` falls back
/// to every pixel only when no plane of the cube keeps a beam pixel. A
/// channel the beam leaves empty, its beam narrower at 1.15 GHz than at
/// 1.05 GHz, measures nothing, so the cube's `nsigma` threshold is the
/// other channel's; measured whole, its wide ramp would set it.
#[test]
fn a_cube_channel_outside_its_beam_measures_no_noise() {
    let problem = reconstruction_problem(
        31,
        WIDTH,
        2,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Hogbom,
        ReconstructionControls::new(100, 0.1, 0.0),
    );
    let centres = edge_centres(&problem);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    let inside = support(&beam);
    let outside = beam
        .support(&ImageDomainRole::Main, 1)
        .expect("channel 1's support");
    assert!(
        outside.iter().all(|kept| !kept),
        "channel 1's beam misses the plane"
    );
    let first = residual(&inside, 0.001, 0.5);
    let second = residual(&vec![true; inside.len()], 0.01, 0.0);
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
    images.sum_weights = vec![1.0; 2];
    images.published_sum_weights = vec![1.0; 2];
    images.residual = first.iter().chain(&second).copied().collect();
    let psf = scene.unit_psf();
    images.psf = Some(psf.iter().chain(&psf).map(|value| *value as f32).collect());
    pass.append(images).expect("images");
    let completion = cycle.finish(SAMPLES, BLOCKS).expect("complete");
    let mut setup = setup(Some(beam));
    setup.nsigma = 3.0;
    let threshold = prepare(&completion, &full_plane(&problem), &setup)
        .statistics
        .nsigma_threshold;
    let (_, rms) = classical(&first, &inside);
    assert!(
        close(threshold, 3.0 * rms),
        "{threshold}, expected {}",
        3.0 * rms
    );
}
