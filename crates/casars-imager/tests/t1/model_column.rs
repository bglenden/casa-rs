// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, MFS Hogbom with the final model
//! predicted into `MODEL_DATA`.

use serde_json::json;

use super::fixture::{Component, Observation, box_sum, component_pixel};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
/// Five times the 1.0 mJy thermal image noise.
const THRESHOLD_JY: f32 = 0.005;

#[test]
fn model_column_predicts_the_deconvolved_sky_at_every_sample() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let (_, products) = observation.image(
        "model-column",
        json!({
            "weighting": { "kind": "natural" },
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold_jy": THRESHOLD_JY,
            "save_model": "model_column",
        }),
    );
    let model_jy = box_sum(&products.get(".model").pixels, component_pixel(POINT), 2);
    let samples = observation.data_and_model();
    assert_eq!(samples.len(), observation.stokes_i_samples());

    // DATA is the true sky plus noise and MODEL_DATA the deconvolved sky,
    // so the least-squares gain g = Σ D·M*/Σ|M|² is the ratio of injected
    // to model flux, with no phase: any error of scale, phase convention or
    // conjugation in the prediction moves it. The noise term is
    // σ/(S·√N) ≈ 1e-3.
    let (cross_re, cross_im, power) =
        samples
            .iter()
            .fold((0.0, 0.0, 0.0), |(re, im, power), (data, model)| {
                let (d_re, d_im) = (f64::from(data.re), f64::from(data.im));
                let (m_re, m_im) = (f64::from(model.re), f64::from(model.im));
                (
                    re + d_re * m_re + d_im * m_im,
                    im + d_im * m_re - d_re * m_im,
                    power + m_re * m_re + m_im * m_im,
                )
            });
    let gain = [cross_re / power, cross_im / power];
    let expected = POINT.flux_jy / model_jy;
    eprintln!(
        "T1 model-column: {} samples, model flux {model_jy:.5} Jy, gain {:.5} + {:.5}i",
        samples.len(),
        gain[0],
        gain[1]
    );
    assert!(
        (gain[0] - expected).abs() < 5.0e-3,
        "gain {gain:?}, injected/model flux {expected}"
    );
    assert!(gain[1].abs() < 5.0e-3, "gain {gain:?} has a phase");
}
