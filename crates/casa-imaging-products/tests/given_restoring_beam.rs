// SPDX-License-Identifier: LGPL-3.0-or-later

//! A restoring beam the request gives by its axes and angle (CASA's
//! `restoringbeam=['10arcsec','8arcsec','45deg']`), restored as
//! `SIImageStore::restore` restores it: the model convolved with the given
//! beam plus the residual brought to its resolution by
//! `rescaleResolution`, or left as it is when the PSF's fitted beam does not
//! deconvolve from the given one. The image records the given beam; the
//! residual keeps its fitted beam.

mod common;
use common::continuum::{SHAPE, continuum_problem_with_policy};
use common::synthetic_pass::{Scene, two_cycle_round};
use common::{GeneratedProducts, ImageOutput, full_window, retained_metadata_bytes};

use casa_imaging_model::{
    CompiledProblem, FixedRestoringBeam, ModelCell, ModelDeltaTerm, ModelValue, ProductKind,
    RestoringBeamPolicy,
};
use casa_imaging_products::{
    ContinuumProductControls, ContinuumProductInputs, PlannedContinuumGeneration, RestoringBeam,
    fft_convolve, gaussian_beam_image, produce_continuum_members, rescale_residual_to_beam,
};

const CENTRE: [usize; 2] = [SHAPE[0] / 2, SHAPE[1] / 2];

const PRODUCTS: [ProductKind; 6] = [
    ProductKind::Psf,
    ProductKind::Residual,
    ProductKind::Model,
    ProductKind::RestoredImage,
    ProductKind::SumWeights,
    ProductKind::Beam,
];

/// A unit point source at the centre, three quarters of it in the model.
fn generate(problem: &CompiledProblem) -> (GeneratedProducts, u64) {
    let scene = Scene::new(problem).with_point(0, CENTRE, &[1.0]);
    let join = two_cycle_round(
        problem,
        &scene,
        vec![ModelDeltaTerm::new(
            ModelCell::new(0, 0, 0, CENTRE),
            ModelValue::new(0.75).expect("model value"),
        )],
    );
    let inputs = ContinuumProductInputs::from_major_cycle(problem, &join);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned generation");
    let output = ImageOutput::for_problem(problem);
    let demand = planned
        .demand(&inputs, full_window(&planned), &output)
        .expect("product demand");
    let produced =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("product generation");
    assert_eq!(
        demand.retained_metadata_bytes(),
        retained_metadata_bytes(&produced),
        "the demand charges the generation's beams"
    );
    (
        GeneratedProducts::from_output(&planned, &produced, &output),
        demand.peak_residency_bytes(),
    )
}

fn payload<'a>(generated: &'a GeneratedProducts, name: &str) -> &'a [f32] {
    generated
        .members()
        .iter()
        .find(|member| member.name() == name)
        .unwrap_or_else(|| panic!("missing {name}"))
        .payload()
}

fn resolved_beam(generated: &GeneratedProducts, name: &str) -> Option<RestoringBeam> {
    generated
        .members()
        .iter()
        .find(|member| member.name() == name)
        .unwrap_or_else(|| panic!("missing {name}"))
        .resolved_beam()
        .copied()
}

/// The fixture's PSF beam, from a run that restores with it.
fn fitted_beam() -> RestoringBeam {
    let (generated, _) = generate(&continuum_problem_with_policy(
        121,
        &PRODUCTS,
        RestoringBeamPolicy::PerPlane,
    ));
    generated.fitted_beams()[0].expect("a fitted beam")
}

fn given(major: f64, minor: f64, angle: f64) -> (RestoringBeamPolicy, RestoringBeam) {
    (
        RestoringBeamPolicy::Fixed(FixedRestoringBeam::new(major, minor, angle).expect("a beam")),
        RestoringBeam::new(major, minor, angle).expect("a beam"),
    )
}

/// `conv(model, beam) + residual`, the restoration of the published parts.
fn restoration(model: &[f32], residual: &[f32], beam: &RestoringBeam) -> Vec<f32> {
    let kernel = gaussian_beam_image(SHAPE, beam, [1.0e-6; 2]);
    fft_convolve(model, kernel.as_slice().expect("contiguous"), SHAPE)
        .into_iter()
        .zip(residual)
        .map(|(model, residual)| model + residual)
        .collect()
}

fn assert_close(actual: &[f32], expected: &[f32], context: &str) {
    let error = actual
        .iter()
        .zip(expected)
        .map(|(actual, expected)| (actual - expected).abs())
        .fold(0.0_f32, f32::max);
    let scale = expected
        .iter()
        .map(|value| value.abs())
        .fold(0.0_f32, f32::max);
    assert!(
        error <= 1.0e-5 * scale.max(1.0),
        "{context}: off by {error}"
    );
}

#[test]
fn a_wider_given_beam_restores_with_it_and_rescales_the_residual() {
    let fitted = fitted_beam();
    let (policy, beam) = given(
        2.5 * fitted.major_fwhm_rad(),
        2.0 * fitted.major_fwhm_rad(),
        0.3,
    );
    let (generated, _) = generate(&continuum_problem_with_policy(121, &PRODUCTS, policy));
    assert_eq!(generated.fitted_beams(), [Some(fitted)]);
    assert_eq!(generated.restoring_beams(), [Some(beam)]);
    assert_eq!(resolved_beam(&generated, ".image"), Some(beam));
    assert_eq!(resolved_beam(&generated, ".residual"), Some(fitted));
    assert_eq!(resolved_beam(&generated, ".psf"), Some(fitted));

    let residual = payload(&generated, ".residual");
    let scaling = rescale_residual_to_beam(residual, SHAPE, [1.0e-6; 2], fitted, beam)
        .expect("a rescaled residual");
    assert!(scaling.deconvolved() && scaling.applied());
    assert!(scaling.area_ratio() > 4.0, "{}", scaling.area_ratio());
    let restored = payload(&generated, ".image");
    assert_close(
        restored,
        &restoration(payload(&generated, ".model"), scaling.values(), &beam),
        "conv(model, given) + rescaled residual",
    );
    // The residual is scaled, not added as it is: the old per-plane
    // restoration with the given kernel differs.
    let unscaled = restoration(payload(&generated, ".model"), residual, &beam);
    assert!(
        restored
            .iter()
            .zip(&unscaled)
            .any(|(restored, unscaled)| (restored - unscaled).abs() > 1.0e-3),
        "the residual was not rescaled"
    );
}

#[test]
fn a_narrower_given_beam_leaves_the_residual_as_casa_does() {
    let fitted = fitted_beam();
    let (policy, beam) = given(
        0.5 * fitted.major_fwhm_rad(),
        0.4 * fitted.minor_fwhm_rad(),
        0.0,
    );
    let (generated, _) = generate(&continuum_problem_with_policy(121, &PRODUCTS, policy));
    assert_eq!(resolved_beam(&generated, ".image"), Some(beam));
    assert_eq!(resolved_beam(&generated, ".residual"), Some(fitted));
    assert_close(
        payload(&generated, ".image"),
        &restoration(
            payload(&generated, ".model"),
            payload(&generated, ".residual"),
            &beam,
        ),
        "conv(model, given) + residual",
    );
}

/// A given beam holds no more than a common one: one beam per plane, one on
/// the restored image, and no enclosing-beam scratch.
#[test]
fn a_given_beam_is_charged_no_more_than_a_common_one() {
    let fitted = fitted_beam();
    let (policy, _) = given(2.0 * fitted.major_fwhm_rad(), fitted.minor_fwhm_rad(), 0.0);
    let (_, fixed) = generate(&continuum_problem_with_policy(121, &PRODUCTS, policy));
    let (_, common) = generate(&continuum_problem_with_policy(
        121,
        &PRODUCTS,
        RestoringBeamPolicy::Common,
    ));
    assert!(fixed <= common, "{fixed} > {common}");
}
