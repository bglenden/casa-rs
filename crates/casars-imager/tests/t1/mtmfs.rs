// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, MT-MFS with two Taylor terms.

use serde_json::json;

use super::fixture::{Component, Observation, component_pixel, peak};

const CHANNELS: usize = 16;
const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 5.0,
    fwhm_px: None,
    spectral_index: -1.0,
};
/// Ten times the 0.5 mJy thermal noise of the 16-channel band.
const THRESHOLD_JY: f32 = 0.005;

#[test]
fn mtmfs_two_terms_recover_flux_and_spectral_index() {
    let observation = Observation::synthesise_band(CHANNELS, &[("point", POINT)]);
    let (summary, products) = observation.image(
        "mtmfs",
        json!({
            "weighting": { "kind": "natural" },
            "deconvolver": "mtmfs",
            "nterms": 2,
            "niter": 1000,
            "threshold_jy": THRESHOLD_JY,
        }),
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
