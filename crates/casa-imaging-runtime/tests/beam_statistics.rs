// SPDX-License-Identifier: LGPL-3.0-or-later

//! The pixels a minor cycle's statistics and masks read (#1078, #1079,
//! #1061): tclean's residual carries the `.pb`'s support as its pixel mask
//! (`SIImageStore::divideResidualByWeight`), so its peaks and automatic
//! mask read only the pixels inside the beam, and its `nsigma` noise
//! reads the pixels where the `.pb` exceeds `pbmask`
//! (`SIImageStore::calcRobustRMS`), every pixel when none does. A `pbmask`
//! above zero is an automatic mask's pixel mask
//! (`SDMaskHandler::autoMaskWithinPB`), and `usemask='pb'`, or a named
//! mask under it, keeps `pb > pbmask` in every plane
//! (`SDMaskHandler::makePBMask`). The beam is the weight image's under a
//! response and the telescope's analytic beam otherwise; each law places
//! the beam's edge, or a `pbmask` level, across the plane and compares
//! with statistics and masks computed here of the pixels on either side.

#[path = "../../casa-imaging-deconvolution/tests/support/problems.rs"]
mod problems;
#[path = "../../casa-imaging-deconvolution/tests/support/synthetic_pass.rs"]
mod synthetic_pass;

use std::collections::BTreeMap;

use casa_imaging_deconvolution::{
    AutoMultithreshControls, ImageDomainReconstructionMaskPlans, MajorCycle, MajorCycleCompletion,
    MaskBox, NoiseEstimate, PassImages, ReconstructionMaskPlan,
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
        pbmask: 0.0,
    }
}

/// The `.pb` of `beam` at `channel`, x-major, zero at or below the cutoff.
fn pb(beam: &AnalyticPrimaryBeam<'_>, channel: usize) -> Vec<f32> {
    beam.pb(&ImageDomainRole::Main, channel)
        .expect("the plane's beam")
}

/// The pixels of `pb` that exceed `level`, as CASA's Float `pb > pbmask`.
fn above(pb: &[f32], level: f64) -> Vec<bool> {
    pb.iter().map(|value| *value > level as f32).collect()
}

/// A `pbmask` that splits the beam's support on `pb`: the median of its
/// positive values, which some pixels exceed and others do not.
fn splitting_level(pb: &[f32]) -> f64 {
    let mut positive = pb
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .collect::<Vec<_>>();
    positive.sort_by(f32::total_cmp);
    let level = f64::from(positive[positive.len() / 2]);
    let kept = above(pb, level).iter().filter(|kept| **kept).count();
    assert!(
        0 < kept && kept < positive.len(),
        "the level splits the support"
    );
    level
}

/// `plan` cut to the primary beam at `level`.
fn within_beam(plan: ReconstructionMaskPlan, level: f64) -> ImageDomainReconstructionMaskPlans {
    ImageDomainReconstructionMaskPlans::new([ReconstructionMaskPlan::within_primary_beam(
        plan, level,
    )
    .expect("a mask cut to the beam")])
    .expect("the mask")
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

/// The completion of an initial pass over a two-channel cube whose
/// residual is 0.01 Jy everywhere.
fn cube_completion(problem: &CompiledProblem) -> MajorCycleCompletion {
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
    images.sum_weights = vec![1.0; 2];
    images.published_sum_weights = vec![1.0; 2];
    images.residual = vec![0.01; 2 * WIDTH * WIDTH];
    let psf = scene.unit_psf();
    images.psf = Some(psf.iter().chain(&psf).map(|value| *value as f32).collect());
    pass.append(images).expect("images");
    cycle.finish(SAMPLES, BLOCKS).expect("complete")
}

/// #1061: `usemask='pb'` keeps, in each channel of a cube, the pixels whose
/// own channel's `.pb` exceeds `pbmask` (`SDMaskHandler::makePBMask`); at
/// `pbmask` 0 that is the beam's support. The beam narrows from 1.05 to
/// 1.15 GHz, so the two channels' masks differ. The next boundary keeps
/// the mask it made, as tclean makes it once, and reads no beam to do so.
#[test]
fn usemask_pb_masks_each_channel_inside_its_own_beam() {
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
    let beams = [pb(&beam, 0), pb(&beam, 1)];
    let level = splitting_level(&beams[0]);
    let completion = cube_completion(&problem);
    let full = || ReconstructionMaskPlan::FullPlane {
        coordinate: problem.geometry().domains()[0].direction(),
    };
    for pbmask in [level, 0.0] {
        let plans = within_beam(full(), pbmask);
        let prepared = prepare(&completion, &plans, &setup(Some(beam)));
        let mask = prepared.masks().get(0).expect("the mask");
        let expected = beams.each_ref().map(|pb| above(pb, pbmask));
        for (channel, expected) in expected.iter().enumerate() {
            assert_eq!(
                mask.plane(channel, 0).expect("the channel's plane"),
                expected.as_slice(),
                "pbmask {pbmask}, channel {channel}"
            );
        }
        assert_ne!(expected[0], expected[1], "the channels' beams differ");
        assert!(expected[0].contains(&true), "pbmask {pbmask}");
        let next = plans
            .next_cycle(prepared.masks(), prepared.auto_masks(), 1, false)
            .expect("the next boundary");
        let mut without_beam = setup(None);
        without_beam.residual_pixel_mask = false;
        let kept = prepare(&completion, &next, &without_beam);
        assert!(std::ptr::eq(
            kept.masks().get(0).expect("the mask").support().as_ptr(),
            mask.support().as_ptr()
        ));
    }
}

/// #1061: a named mask under a `pbmask` above zero is kept where the
/// `.pb` exceeds it (`makePBMask(combinemask=True)`).
#[test]
fn a_named_mask_under_pbmask_is_kept_inside_the_beam() {
    let problem = problem();
    let centres = edge_centres(&problem);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    let pb = pb(&beam, 0);
    let level = splitting_level(&pb);
    let completion = completion(&problem, &vec![0.01; WIDTH * WIDTH], |_| {});
    // Rows y of 4 to 11, every column: x-major pixel (x, y) is x · ny + y.
    let boxes = ReconstructionMaskPlan::Boxes {
        coordinate: problem.geometry().domains()[0].direction(),
        boxes: vec![MaskBox::new([0, 4], [WIDTH as i64 - 1, 11]).expect("a box")],
    };
    let prepared = prepare(&completion, &within_beam(boxes, level), &setup(Some(beam)));
    let expected = above(&pb, level)
        .iter()
        .enumerate()
        .map(|(index, kept)| *kept && (4..12).contains(&(index % WIDTH)))
        .collect::<Vec<_>>();
    assert!(expected.contains(&true));
    assert_eq!(
        prepared
            .masks()
            .get(0)
            .expect("the mask")
            .plane(0, 0)
            .expect("the plane"),
        expected.as_slice()
    );
}

/// #1061: a mosaic's `usemask='pb'` mask is its weight image's Float beam
/// above `pbmask` (`makePBFromWeight`, then `makePBMask`). Weights of 1,
/// 0.25 and 0 make a beam of 1, 0.5 and 0, so a `pbmask` of 0.6 keeps the
/// core, and 0.4 or 0 the core and the ring.
#[test]
fn a_mosaic_masks_inside_its_weight_beam_above_pbmask() {
    let problem = problem();
    let square = |index: usize, half: usize| {
        let (x, y) = (index / WIDTH, index % WIDTH);
        x.abs_diff(WIDTH / 2) < half && y.abs_diff(WIDTH / 2) < half
    };
    let core = (0..WIDTH * WIDTH)
        .map(|index| square(index, 3))
        .collect::<Vec<_>>();
    let disc = (0..WIDTH * WIDTH)
        .map(|index| square(index, 6))
        .collect::<Vec<_>>();
    let completion = completion(&problem, &vec![0.01; WIDTH * WIDTH], |images| {
        images.weight = Some(
            core.iter()
                .zip(&disc)
                .map(|(core, disc)| match (core, disc) {
                    (true, _) => 1.0,
                    (false, true) => 0.25,
                    (false, false) => 0.0,
                })
                .collect(),
        );
    });
    let response =
        MinorCycleImageResponse::new(ProductNormalization::FlatNoise, validity().primary_beam())
            .expect("response");
    let mut setup = setup(None);
    setup.response = Some(response);
    for (pbmask, expected) in [(0.6, &core), (0.4, &disc), (0.0, &disc)] {
        let full = ReconstructionMaskPlan::FullPlane {
            coordinate: problem.geometry().domains()[0].direction(),
        };
        let prepared = prepare(&completion, &within_beam(full, pbmask), &setup);
        assert_eq!(
            prepared
                .masks()
                .get(0)
                .expect("the mask")
                .plane(0, 0)
                .expect("the plane"),
            expected.as_slice(),
            "pbmask {pbmask}"
        );
    }
}

/// #1061: under a `pbmask` above zero, an automatic mask's pixel mask is
/// `pb > pbmask`, whatever `pblimit`'s sign (`autoMaskWithinPB` attaches
/// it to the residual it masks): its median and noise read only those
/// pixels, and it masks nothing else. A 2 Jy point above the level is
/// masked; the 0.5 Jy inside the beam's support below it would be too.
#[test]
fn an_automatic_mask_under_pbmask_reads_and_masks_above_it() {
    let problem = problem();
    let centres = edge_centres(&problem);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    let pb = pb(&beam, 0);
    let level = splitting_level(&pb);
    let kept = above(&pb, level);
    let mut residual = residual(&kept, 0.001, 0.5);
    residual[kept.iter().position(|kept| *kept).expect("a pixel above")] = 2.0;
    let completion = completion(&problem, &residual, |_| {});
    let (median, rms) = classical(&residual, &kept);
    for masks_residual in [true, false] {
        let mut setup = setup(Some(beam));
        setup.automask = true;
        setup.pbmask = level;
        setup.residual_pixel_mask = masks_residual;
        let prepared = prepare(&completion, &automask(&problem), &setup);
        let row = prepared.auto_masks()[0]
            .as_ref()
            .expect("automatic evidence")
            .plane(0, 0)
            .copied()
            .expect("the plane's row");
        assert!(
            close(row.median, median) && close(row.robust_rms, rms),
            "pixel mask {masks_residual}: ({}, {}), expected ({median}, {rms})",
            row.median,
            row.robust_rms
        );
        let mask = prepared.masks().get(0).expect("the mask");
        let masked = mask.plane(0, 0).expect("the plane");
        assert!(masked.contains(&true), "the point is masked");
        assert!(
            masked.iter().zip(&kept).all(|(set, kept)| !set || *kept),
            "pixel mask {masks_residual}: nothing at or below pbmask is masked"
        );
    }
}

/// #1061: the `nsigma` noise reads `calcRobustRMS`'s pixels at `pbmask`,
/// those whose `.pb` exceeds it, under either estimate, or every pixel
/// where none does (CAS-13318); the peak still reads the residual's pixel
/// mask, the beam's support above `pblimit`.
#[test]
fn the_nsigma_noise_reads_above_pbmask_and_the_peak_above_pblimit() {
    let problem = problem();
    let centres = edge_centres(&problem);
    let beam = AnalyticPrimaryBeam::new(
        &problem,
        AnalyticPrimaryBeamModel::CasaEvlaCommon,
        Some(&centres),
    )
    .expect("L band")
    .expect("an analytic beam");
    let pb = pb(&beam, 0);
    let level = splitting_level(&pb);
    let kept = above(&pb, level);
    let inside = support(&beam);
    let every = vec![true; inside.len()];
    // A ramp over the whole plane, each part of it its own spread.
    let mut residual = residual(&every, 0.001, 0.0);
    let below = inside
        .iter()
        .zip(&kept)
        .position(|(inside, kept)| *inside && !kept)
        .expect("a pixel between pblimit and pbmask");
    residual[below] = 3.0;
    let outside = inside.iter().position(|inside| !inside).expect("outside");
    residual[outside] = 5.0;
    let completion = completion(&problem, &residual, |_| {});
    let rms = |measured: &[bool]| classical(&residual, measured).1;
    let distinct = [rms(&kept), rms(&inside), rms(&every)];
    assert!(
        !close(distinct[0], distinct[1])
            && !close(distinct[0], distinct[2])
            && !close(distinct[1], distinct[2]),
        "the law separates the three: {distinct:?}"
    );
    let highest = f64::from(pb.iter().copied().fold(0.0, f32::max));
    // Chauvenet's criterion would reject the peaks the whole plane holds.
    let estimates = [NoiseEstimate::Fast, NoiseEstimate::Robust];
    for (pbmask, measured, estimates) in [
        (level, &kept, &estimates[..]),
        (highest, &every, &estimates[..1]),
    ] {
        let rms = rms(measured);
        for &noise in estimates {
            let mut setup = setup(Some(beam));
            setup.nsigma = 3.0;
            setup.noise = noise;
            setup.pbmask = pbmask;
            let statistics = prepare(&completion, &full_plane(&problem), &setup).statistics;
            assert!(
                close(statistics.nsigma_threshold, 3.0 * rms),
                "pbmask {pbmask}, {noise:?}: {}, expected {}",
                statistics.nsigma_threshold,
                3.0 * rms
            );
            assert_eq!(statistics.peak_no_mask, 3.0, "pbmask {pbmask}");
        }
    }
}
