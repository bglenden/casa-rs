// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, MFS: Hogbom, Clark and multiscale.

use std::collections::BTreeSet;

use casars_imager::ImagerRunReport;
use serde_json::{Value, json};

use super::fixture::{
    CELL_ARCSEC, Component, Observation, Products, box_sum, component_pixel, fit_psf_main_lobe,
    off_source_rms, peak,
};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
const GAUSSIAN: Component = Component {
    offset_px: [-30, -26],
    flux_jy: 0.5,
    fwhm_px: Some(8.0),
    spectral_index: 0.0,
};
/// Five times the 1.0 mJy thermal image noise.
const THRESHOLD: &str = "0.005Jy";

fn controls(deconvolver: &str) -> Value {
    json!({
        "weighting": "natural",
        "deconvolver": deconvolver,
        "niter": 500,
        "threshold": THRESHOLD,
    })
}

/// Image the point-and-Gaussian sky and check the laws every deconvolver
/// must meet; returns the run for algorithm-specific checks.
pub(super) fn assert_recovers_the_point(
    name: &str,
    controls: Value,
) -> (Observation, ImagerRunReport, Products) {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let (summary, products) = observation.image(name, controls);
    let image = products.get(".image");
    let noise_jy = observation.image_noise_jy();
    let point_pixel = component_pixel(POINT);
    let (peak_jy, peak_pixel, peak_offset_px) = peak(&image.pixels);
    let point_model_jy = box_sum(&products.get(".model").pixels, point_pixel, 2);
    let beam = image.restoring_beam();
    let psf_fit = fit_psf_main_lobe(&products.get(".psf").pixels, 0.35);
    let residual_rms_jy = off_source_rms(
        &products.get(".residual").pixels,
        &[point_pixel, component_pixel(GAUSSIAN)],
        24.0,
    );
    eprintln!(
        "T1 {name}: row-channel samples {}, thermal {noise_jy:.3e} Jy; \
         peak {peak_jy:.5} Jy at {peak_pixel:?} offset {peak_offset_px:.3?} px; \
         point model {point_model_jy:.5} Jy; beam [{:.4e}, {:.4e}] rad, PSF fit \
         [{:.4e}, {:.4e}] rad; residual RMS {residual_rms_jy:.3e} Jy; \
         {} minor iterations, {} major cycles, stop {:?}",
        observation.row_channel_samples(),
        beam.major,
        beam.minor,
        psf_fit[0],
        psf_fit[1],
        summary.actual_minor_iterations,
        summary.major_cycles,
        summary.clean_stop_reason,
    );

    assert_eq!(
        products.suffixes(),
        BTreeSet::from(
            [".image", ".mask", ".model", ".psf", ".residual", ".sumwt"].map(String::from)
        )
    );
    observation.assert_image_wcs(image);
    // Natural weighting of unit-weight samples: sumwt counts every unflagged
    // parallel-hand sample that enters Stokes I.
    assert_eq!(
        f64::from(products.get(".sumwt").pixels[[0, 0]]),
        observation.stokes_i_samples() as f64
    );

    // The point sits on a pixel centre, 68 pixels (9-17 beams) from the
    // Gaussian and 0.35 arcsec from the phase centre, where the Q-band
    // primary beam attenuates by about 1e-4. Restored peak = clean flux +
    // residual, so its error is the 1 mJy thermal noise plus sidelobe
    // leakage from the Gaussian's uncleaned remainder (PSF sidelobes <= 0.18
    // times ~40 mJy, so <= 7 mJy). 1% covers both; the clean components at
    // the point differ from the injected flux by at most the 5 mJy threshold
    // plus noise.
    assert_eq!(peak_pixel, point_pixel);
    assert!(
        peak_offset_px.iter().all(|offset| offset.abs() < 0.1),
        "point-source sub-pixel offset {peak_offset_px:?} px"
    );
    for (product, flux_jy) in [
        ("restored peak", f64::from(peak_jy)),
        ("model", point_model_jy),
    ] {
        assert!(
            (flux_jy - POINT.flux_jy).abs() < 0.01 * POINT.flux_jy,
            "point {product} {flux_jy} Jy, injected {} Jy",
            POINT.flux_jy
        );
    }

    for (axis, (restored, fitted)) in [beam.major, beam.minor]
        .into_iter()
        .zip(psf_fit)
        .enumerate()
    {
        assert!(
            (restored / fitted - 1.0).abs() < 0.1,
            "restoring-beam axis {axis}: {restored} rad, PSF main-lobe fit {fitted} rad"
        );
    }

    // Away from the sources the residual is thermal noise plus sidelobes of
    // the emission left below the 5-sigma threshold; a noise-only dirty
    // image of this fixture measures 0.90-0.97 sigma. The lower bound keeps
    // the residual normalisation honest.
    assert!(
        (0.8 * noise_jy..1.5 * noise_jy).contains(&residual_rms_jy),
        "off-source residual RMS {residual_rms_jy} Jy, thermal {noise_jy} Jy"
    );
    (observation, summary, products)
}

#[test]
fn standard_mfs_hogbom_recovers_the_analytic_sky() {
    assert_recovers_the_point("standard-mfs-hogbom", controls("hogbom"));
}

#[test]
fn standard_mfs_clark_recovers_the_analytic_sky() {
    assert_recovers_the_point("standard-mfs-clark", controls("clark"));
}

#[test]
fn standard_mfs_multiscale_recovers_the_extended_flux() {
    let mut multiscale = controls("multiscale");
    multiscale["scales"] = json!("0,6,12");
    let (_, _, products) = assert_recovers_the_point("standard-mfs-multiscale", multiscale);
    // The 41-pixel box holds the Gaussian's 0.5 Jy to 1e-7. Cleaning stops
    // at 5 mJy/beam, so up to ~30 mJy (six beams of source at the threshold)
    // stays in the residual: the model holds less than the injected flux and
    // the restored image, which adds the residual back in clean-beam units,
    // more. Both stay within 10%, and they bracket it.
    let gaussian_model_jy = box_sum(
        &products.get(".model").pixels,
        component_pixel(GAUSSIAN),
        20,
    );
    let image = products.get(".image");
    let beam = image.restoring_beam();
    let cell_rad = CELL_ARCSEC.to_radians() / 3_600.0;
    let beam_area_px = std::f64::consts::PI * beam.major * beam.minor
        / (4.0 * std::f64::consts::LN_2 * cell_rad * cell_rad);
    let restored_jy = box_sum(&image.pixels, component_pixel(GAUSSIAN), 20) / beam_area_px;
    let residual_jy = box_sum(
        &products.get(".residual").pixels,
        component_pixel(GAUSSIAN),
        20,
    ) / beam_area_px;
    eprintln!(
        "T1 standard-mfs-multiscale: Gaussian model flux {gaussian_model_jy:.5} Jy, restored \
         {restored_jy:.5} Jy, residual {residual_jy:.5} Jy, beam area {beam_area_px:.2} px"
    );
    for (product, flux_jy) in [("model", gaussian_model_jy), ("restored", restored_jy)] {
        assert!(
            (flux_jy - GAUSSIAN.flux_jy).abs() < 0.1 * GAUSSIAN.flux_jy,
            "Gaussian {product} flux {flux_jy} Jy, injected {} Jy",
            GAUSSIAN.flux_jy
        );
    }
    assert!(
        gaussian_model_jy < GAUSSIAN.flux_jy && GAUSSIAN.flux_jy < restored_jy,
        "model {gaussian_model_jy} Jy and restored {restored_jy} Jy do not bracket {} Jy",
        GAUSSIAN.flux_jy
    );
}
