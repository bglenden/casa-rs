// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, MT-MFS with two Taylor terms and
//! with one.

use serde_json::json;

use super::fixture::{
    CELL_ARCSEC, Component, Geometry, Observation, ProductContract, Products, Setup,
    SpectralAxis, component_pixel, peak,
};

const CHANNELS: usize = 16;
const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 5.0,
    fwhm_px: None,
    spectral_index: -1.0,
};
/// Ten times the 0.5 mJy thermal noise of the 16-channel band.
const THRESHOLD: &str = "0.005Jy";
/// Above the Q-band PB at the field's corners (0.99974), below it at the
/// point.
const PBLIMIT: f64 = 0.9998;

#[test]
fn mtmfs_two_terms_recover_flux_and_spectral_index() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let (summary, products) = observation.image(
        "mtmfs",
        json!({
            "weighting": "natural",
            "deconvolver": "mtmfs",
            "nterms": 2,
            "niter": 1000,
            "threshold": THRESHOLD,
            "write_pb": true,
            "pblimit": PBLIMIT,
        }),
    );
    // CASA's MT-MFS set, with `.pb.tt0` alone: no pbcor touches pb.tt1.
    // The pblimit cuts the 3-arcsec field's edge, so the PB is zero and
    // masked there, as `SIImageStore::makePBImage` leaves it.
    observation.assert_product_contract(
        &products,
        &ProductContract {
            inventory: &[
                ".alpha",
                ".alpha.error",
                ".image.tt0",
                ".image.tt1",
                ".mask",
                ".model.tt0",
                ".model.tt1",
                ".pb.tt0",
                ".psf.tt0",
                ".psf.tt1",
                ".psf.tt2",
                ".residual.tt0",
                ".residual.tt1",
                ".sumwt.tt0",
                ".sumwt.tt1",
                ".sumwt.tt2",
            ],
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: PBLIMIT as f32,
            uncorrected_masks: true,
        },
    );
    let pb = products.get(".pb.tt0").values();
    let zeros = pb.iter().filter(|value| **value == 0.0).count();
    let lowest = pb
        .iter()
        .filter(|value| **value > 0.0)
        .fold(f32::MAX, |low, value| low.min(*value));
    assert!(
        zeros > 0 && zeros < pb.len(),
        "the pblimit cuts the field: {zeros} of {} PB pixels zero, lowest kept {lowest}",
        pb.len()
    );
    let tt0 = products.get(".image.tt0");
    observation.assert_image_wcs(tt0);
    let reference_hz = tt0.channel_frequency_hz(0);
    let pixel = component_pixel(POINT);
    let (peak_jy, peak_pixel, _) = peak(&tt0.pixels);
    let alpha = products.get(".alpha").pixels[pixel];
    let alpha_error = products.get(".alpha.error").pixels[pixel];
    eprintln!(
        "T1 mtmfs: tt0 peak {peak_jy:.5} Jy at {peak_pixel:?}, alpha {alpha:.4} ± \
         {alpha_error:.4}; {} minor iterations, {} major cycles",
        summary.actual_minor_iterations, summary.major_cycles
    );

    // Natural weighting of unit weights: the zeroth moment counts every
    // Stokes I sample.
    assert_eq!(
        f64::from(products.get(".sumwt.tt0").pixels[[0, 0]]),
        observation.stokes_i_samples() as f64
    );
    // tt0 is the flux at the reference frequency, to the clean threshold
    // and the 0.5 mJy noise.
    assert_eq!(peak_pixel, pixel);
    let expected_jy = POINT.flux_at(reference_hz);
    assert!(
        (f64::from(peak_jy) - expected_jy).abs() < 0.01 * expected_jy,
        "tt0 peak {peak_jy} Jy, S(ν_ref) {expected_jy} Jy"
    );
    // The band spans ±2.2% about the reference, so the tt1 noise is about
    // 40 mJy and the spectral-index error about 0.008 for 5 Jy.
    assert!(
        (f64::from(alpha) - POINT.spectral_index).abs() < 0.05,
        "alpha {alpha}, injected {}",
        POINT.spectral_index
    );
    assert!(
        alpha_error.is_finite() && alpha_error < 0.05,
        "alpha error {alpha_error}"
    );
}

/// `‖a − b‖ / ‖b‖` of product `a` of one run and product `b` of another.
fn nrms(actual: &Products, a: &str, expected: &Products, b: &str) -> f64 {
    let (a, b) = (actual.get(a).values(), expected.get(b).values());
    assert_eq!(a.shape(), b.shape());
    let error: f64 = a
        .iter()
        .zip(&b)
        .map(|(&a, &b)| f64::from(a - b).powi(2))
        .sum();
    let power: f64 = b.iter().map(|&b| f64::from(b).powi(2)).sum();
    (error / power).sqrt()
}

/// CASA's `mtmfs` with `nterms=1` (`test_onefield_mtmfs_nterms1`) is a
/// Taylor family of one term: `.tt0` products and no spectral index
/// (`calculateAlphaBeta` needs two terms). On the point scale alone its
/// minor cycle is Högbom's: one term's Hessian is the PSF's unit peak, so
/// each step takes the residual's peak and subtracts the gain-scaled PSF
/// there, within the cleaner's patch (`findBeamPatch`, 80 pixels), which
/// covers an 80-pixel image. Its major cycle grids what a constant basis
/// grids. So its PSF, sums of weights, PB, model, residual and restored
/// image are Högbom's, and its iterations and major cycles too.
#[test]
fn one_taylor_term_is_hogbom_where_its_patch_covers_the_image() {
    const FLAT: Component = Component {
        offset_px: [9, -6],
        flux_jy: 5.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise_setup(
        Setup {
            geometry: Geometry::square(80, CELL_ARCSEC),
            ..Setup::vla_q_band(CHANNELS)
        },
        &[("point", FLAT)],
    );
    let controls = |deconvolver: &str| {
        json!({
            "weighting": "natural",
            "deconvolver": deconvolver,
            "nterms": 1,
            "niter": 1000,
            "threshold": THRESHOLD,
            "write_pb": true,
            "pblimit": 0.2,
        })
    };
    let (taylor_run, taylor) = observation.image("one-term", controls("mtmfs"));
    let (hogbom_run, hogbom) = observation.image("hogbom", controls("hogbom"));
    observation.assert_product_contract(
        &taylor,
        &ProductContract {
            inventory: &[
                ".image.tt0",
                ".mask",
                ".model.tt0",
                ".pb.tt0",
                ".psf.tt0",
                ".residual.tt0",
                ".sumwt.tt0",
            ],
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: 0.2,
            uncorrected_masks: true,
        },
    );
    eprintln!(
        "T1 one-term mtmfs: {} minor iterations, {} major cycles; Högbom {} and {}",
        taylor_run.actual_minor_iterations,
        taylor_run.major_cycles,
        hogbom_run.actual_minor_iterations,
        hogbom_run.major_cycles
    );
    // Both stop on the threshold, before the iteration budget where
    // Högbom's inclusive loop would take one component more.
    assert!(taylor_run.actual_minor_iterations > 0);
    assert!(hogbom_run.actual_minor_iterations < 1000);
    assert_eq!(
        (taylor_run.actual_minor_iterations, taylor_run.major_cycles),
        (hogbom_run.actual_minor_iterations, hogbom_run.major_cycles)
    );
    for (term, single) in [
        (".psf.tt0", ".psf"),
        (".sumwt.tt0", ".sumwt"),
        (".pb.tt0", ".pb"),
        (".model.tt0", ".model"),
        (".residual.tt0", ".residual"),
        (".image.tt0", ".image"),
    ] {
        let error = nrms(&taylor, term, &hogbom, single);
        assert!(error < 1e-5, "{term} against {single}: {error}");
    }
    assert_eq!(
        taylor.get(".mask").values(),
        hogbom.get(".mask").values(),
        "one CLEAN mask"
    );
    // And the flat spectrum's flux, to the threshold and the noise.
    let (peak_jy, peak_pixel, _) = peak(&taylor.get(".image.tt0").pixels);
    assert_eq!(peak_pixel, observation.geometry().pixel(FLAT));
    assert!(
        (f64::from(peak_jy) - FLAT.flux_jy).abs() < 0.01 * FLAT.flux_jy,
        "tt0 peak {peak_jy} Jy, S {} Jy",
        FLAT.flux_jy
    );
}
