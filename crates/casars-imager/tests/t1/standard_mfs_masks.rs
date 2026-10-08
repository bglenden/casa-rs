// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, MFS Hogbom under a user box mask and
//! under CASA `auto-multithresh`.

use ndarray::Array2;
use serde_json::json;

use super::fixture::{Component, IMAGE_SIZE, Observation, box_sum, component_pixel, peak};

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
const THRESHOLD_JY: f32 = 0.005;

fn assert_model_inside(model: &Array2<f32>, mask: &Array2<f32>) {
    let outside = model
        .iter()
        .zip(mask.iter())
        .filter(|(model, mask)| **mask == 0.0 && **model != 0.0)
        .count();
    assert_eq!(outside, 0, "model pixels outside the mask");
}

fn assert_point_recovered(model: &Array2<f32>, image: &Array2<f32>) {
    let point_model_jy = box_sum(model, component_pixel(POINT), 2);
    let (peak_jy, peak_pixel, _) = peak(image);
    assert_eq!(peak_pixel, component_pixel(POINT));
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
}

#[test]
fn box_mask_confines_the_model_and_leaves_the_masked_out_source() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let [px, py] = component_pixel(POINT);
    let half_width = 6;
    let (_, products) = observation.image(
        "box-mask",
        json!({
            "weighting": { "kind": "natural" },
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold_jy": THRESHOLD_JY,
            "mask_boxes": [[px - half_width, py - half_width, px + half_width, py + half_width]],
        }),
    );
    let mask = &products.get(".mask").pixels;
    let model = &products.get(".model").pixels;
    // The mask product is the box, inclusive of its corners.
    for ((x, y), value) in mask.indexed_iter() {
        let inside = x.abs_diff(px) <= half_width && y.abs_diff(py) <= half_width;
        assert_eq!(*value, f32::from(inside), "mask pixel [{x}, {y}]");
    }
    assert_model_inside(model, mask);
    assert_point_recovered(model, &products.get(".image").pixels);
    // Cleaned, the Gaussian would leave under 5 mJy (five sigma) in the
    // residual; masked out, its dirty peak of about 0.15 Jy/beam remains.
    let gaussian = component_pixel(GAUSSIAN);
    let residual = &products.get(".residual").pixels;
    let gaussian_residual_jy = (gaussian[0] - 8..=gaussian[0] + 8)
        .flat_map(|x| (gaussian[1] - 8..=gaussian[1] + 8).map(move |y| [x, y]))
        .map(|[x, y]| residual[[x, y]])
        .fold(f32::NEG_INFINITY, f32::max);
    let noise_jy = observation.image_noise_jy() as f32;
    assert!(
        gaussian_residual_jy > 50.0 * noise_jy,
        "masked-out Gaussian residual peak {gaussian_residual_jy} Jy, noise {noise_jy} Jy"
    );
}

#[test]
fn auto_multithresh_masks_both_sources_and_confines_the_model() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let (summary, products) = observation.image(
        "auto-multithresh",
        json!({
            "weighting": { "kind": "natural" },
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold_jy": THRESHOLD_JY,
            "use_mask": "auto-multithresh",
        }),
    );
    let mask = &products.get(".mask").pixels;
    let model = &products.get(".model").pixels;
    let masked = mask.iter().filter(|value| **value != 0.0).count();
    eprintln!(
        "T1 auto-multithresh: {masked} masked pixels, {} minor iterations, {} major cycles",
        summary.actual_minor_iterations, summary.major_cycles
    );
    for component in [POINT, GAUSSIAN] {
        let [x, y] = component_pixel(component);
        assert_eq!(mask[[x, y]], 1.0, "{component:?} is not masked");
    }
    // Both sources span a few hundred pixels at most; a mask grown into the
    // noise or the sidelobes would cover far more of the image.
    assert!(
        masked < IMAGE_SIZE * IMAGE_SIZE / 50,
        "auto-multithresh masked {masked} pixels"
    );
    assert_model_inside(model, mask);
    assert_point_recovered(model, &products.get(".image").pixels);
}
