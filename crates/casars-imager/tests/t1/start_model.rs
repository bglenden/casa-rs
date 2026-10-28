// SPDX-License-Identifier: LGPL-3.0-or-later

//! `startmodel` (#841 Phase 6b): CASA's start model is the model's first
//! generation, so the first major cycle subtracts its prediction and the
//! restored image includes it. A run started from another run's final model
//! with no minor cycle is that run's last major cycle again: the same model,
//! residual and restored image (CASA 6.7.6 on `refim_point` gives the deep
//! clean's residual exactly). Agreement with CASA's own runs is the T1.5
//! rows'.

use serde_json::json;

use super::fixture::{Component, Observation, Products};

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

/// The largest difference of `product` between two runs, relative to the
/// first's largest magnitude.
fn difference(left: &Products, right: &Products, product: &str) -> f64 {
    let left = &left.get(product).pixels;
    let right = &right.get(product).pixels;
    let peak = left.iter().fold(0.0_f64, |m, v| m.max(f64::from(v.abs())));
    let worst = left
        .iter()
        .zip(right)
        .fold(0.0_f64, |m, (a, b)| m.max(f64::from((a - b).abs())));
    worst / peak
}

#[test]
fn a_start_model_is_the_first_generation_of_the_model() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let (_, deep) = observation.image(
        "deep",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold": "0.005Jy",
        }),
    );
    // The deep run's products are under the observation's directory.
    let start = observation.scratch("deep.model");
    let (_, started) = observation.image(
        "started",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 0,
            "startmodel": start,
        }),
    );
    for product in [".model", ".residual", ".image", ".psf"] {
        let worst = difference(&deep, &started, product);
        eprintln!("T1 startmodel: {product} differs by {worst:.2e} of its peak");
        assert!(worst < 1.0e-5, "{product}: {worst:e}");
    }
    assert_eq!(
        deep.get(".sumwt").pixels,
        started.get(".sumwt").pixels,
        "the initial pass still sums the weights"
    );

    // Cleaning on from the start model goes deeper than it.
    let (_, cleaned) = observation.image(
        "cleaned-on",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 100,
            "threshold": "0.001Jy",
            "startmodel": start,
        }),
    );
    let rms = |products: &Products| {
        let residual = &products.get(".residual").pixels;
        (residual.iter().map(|v| f64::from(*v).powi(2)).sum::<f64>() / residual.len() as f64).sqrt()
    };
    assert!(
        rms(&cleaned) < rms(&started),
        "cleaning on: residual rms {} against {}",
        rms(&cleaned),
        rms(&started)
    );
}
