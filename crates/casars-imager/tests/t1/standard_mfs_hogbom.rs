// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, MFS, Hogbom.

use std::collections::BTreeSet;

use serde_json::json;

use super::fixture::{
    Component, Observation, box_sum, component_pixel, fit_psf_main_lobe, off_source_rms, peak,
};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
};
const GAUSSIAN: Component = Component {
    offset_px: [-30, -26],
    flux_jy: 0.5,
    fwhm_px: Some(8.0),
};
/// Five times the 1.0 mJy thermal image noise.
const THRESHOLD_JY: f32 = 0.005;

#[test]
fn standard_mfs_hogbom_recovers_the_analytic_sky() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let (summary, products) = observation.image(
        "standard-mfs-hogbom",
        json!({
            "weighting": { "kind": "natural" },
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold_jy": THRESHOLD_JY,
        }),
    );
    let noise_jy = observation.image_noise_jy();
    let point_pixel = component_pixel(POINT);
    let (peak_jy, peak_pixel, peak_offset_px) = peak(&products.image.pixels);
    let point_model_jy = box_sum(&products.model.pixels, point_pixel, 2);
    let beam = products.image.restoring_beam();
    let psf_fit = fit_psf_main_lobe(&products.psf.pixels, 0.35);
    let residual_rms_jy = off_source_rms(
        &products.residual.pixels,
        &[point_pixel, component_pixel(GAUSSIAN)],
        24.0,
    );
    eprintln!(
        "T1 standard-mfs-hogbom: row-channel samples {}, thermal {noise_jy:.3e} Jy; \
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
        products.suffixes,
        BTreeSet::from(
            [".image", ".mask", ".model", ".psf", ".residual", ".sumwt"].map(String::from)
        )
    );
    observation.assert_image_wcs(&products.image);
    // Natural weighting of unit-weight samples: sumwt counts every unflagged
    // parallel-hand sample that enters Stokes I.
    assert_eq!(
        f64::from(products.sumwt.pixels[[0, 0]]),
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
}
