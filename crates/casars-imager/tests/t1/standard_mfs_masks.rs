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
const THRESHOLD: &str = "0.005Jy";

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
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold": THRESHOLD,
            "mask": format!(
                "{},{},{},{}",
                px - half_width,
                py - half_width,
                px + half_width,
                py + half_width
            ),
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
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold": THRESHOLD,
            "usemask": "auto-multithresh",
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

#[test]
fn an_explicit_empty_mask_returns_zero_mask_and_survives_restart() {
    use super::casa_return::json_run;
    use casa_images::PagedImage;
    use casa_provider_contracts::{CasaKey, CasaValue, TaskReply};

    let observation = Observation::synthesise(&[("point", POINT)]);
    let (_, dirty) = observation.image("empty-mask-source", json!({"niter": 0}));
    assert!(dirty.get(".model").pixels.iter().all(|&value| value == 0.0));
    let mask_path = observation.scratch("empty-mask-source.model");
    let directory = observation.scratch("empty-mask-run");
    std::fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": directory.join("image"),
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|cell| format!("{cell}arcsec")),
        "backend": "cpu", "niter": 10, "mask": mask_path,
    });
    for _ in 0..2 {
        let TaskReply::Succeeded {
            value: CasaValue::Dict { entries },
            ..
        } = json_run(&directory, values.clone())
        else {
            panic!("an empty mask must stop with CASA code7 rather than fail")
        };
        for (key, expected) in [("stopcode", 7), ("iterdone", 0), ("nmajordone", 1)] {
            let value = &entries
                .iter()
                .find(|entry| entry.key == CasaKey::String(key.into()))
                .unwrap()
                .value;
            assert_eq!(value, &CasaValue::Int { value: expected });
        }
        for suffix in ["model", "mask"] {
            let image = PagedImage::<f32>::open(directory.join(format!("image.{suffix}"))).unwrap();
            assert!(image.get().unwrap().iter().all(|&value| value == 0.0));
        }
        let residual = PagedImage::<f32>::open(directory.join("image.residual")).unwrap();
        assert!(
            residual
                .get()
                .unwrap()
                .iter()
                .any(|&value| value.abs() > 0.1)
        );
        // The second call must reuse the now-persisted empty CLEAN mask.
        values.as_object_mut().unwrap().remove("mask");
    }
    // A pre-existing nonzero model must also survive zero-mask continuation.
    let before = {
        let mut image = PagedImage::<f32>::open(directory.join("image.model")).unwrap();
        let mut pixels = image.get().unwrap();
        pixels[[geometry.image_size[0] / 2, geometry.image_size[1] / 2, 0, 0]] = 0.25;
        image.put_slice(&pixels, &[0; 4]).unwrap();
        image.save().unwrap();
        pixels
    };
    let TaskReply::Succeeded {
        value: CasaValue::Dict { entries },
        ..
    } = json_run(&directory, values)
    else {
        panic!("a seeded model must survive empty-mask continuation")
    };
    let iterations = &entries
        .iter()
        .find(|entry| entry.key == CasaKey::String("iterdone".into()))
        .unwrap()
        .value;
    assert_eq!(iterations, &CasaValue::Int { value: 0 });
    let after = PagedImage::<f32>::open(directory.join("image.model"))
        .unwrap()
        .get()
        .unwrap();
    assert_eq!(before, after);
}

#[test]
fn clipped_pixel_boxes_and_empty_regions_preserve_casa_mask_semantics() {
    use casars_imager::ImagerCleanStopReason;

    let observation = Observation::synthesise(&[("point", POINT)]);
    let [px, py] = component_pixel(POINT);
    let edge = IMAGE_SIZE + 10;
    let empty = observation.scratch("empty-region.crtf");
    std::fs::write(&empty, "#CRTF\n").unwrap();
    let boxes = [
        (
            "inside",
            format!(
                "box[[{}pix,{}pix],[{}pix,{}pix]]",
                px - 4,
                py - 4,
                px + 4,
                py + 4
            ),
            Some([px - 4, py - 4, px + 4, py + 4]),
        ),
        (
            "upper",
            format!("box[[{}pix,{}pix],[{edge}pix,{edge}pix]]", px - 4, py - 4),
            Some([px - 4, py - 4, IMAGE_SIZE - 1, IMAGE_SIZE - 1]),
        ),
        (
            "lower",
            format!("box[[-10pix,-20pix],[{}pix,{}pix]]", px + 4, py + 4),
            Some([0, 0, px + 4, py + 4]),
        ),
        (
            "reverse",
            format!(
                "box[[{}pix,{}pix],[{}pix,{}pix]]",
                px + 4,
                py + 4,
                px - 4,
                py - 4
            ),
            Some([px - 4, py - 4, px + 4, py + 4]),
        ),
        (
            "outside",
            format!(
                "box[[{edge}pix,{edge}pix],[{}pix,{}pix]]",
                edge + 10,
                edge + 10
            ),
            None,
        ),
        ("empty-file", empty.to_string_lossy().into_owned(), None),
    ];
    for pblimit in [0.2, -0.1] {
        for (label, mask, bounds) in &boxes {
            let (summary, products) = observation.image(
                &format!("integer-mask-{pblimit}-{label}"),
                json!({"mask": mask, "pblimit": pblimit, "niter": 10, "threshold": "0Jy"}),
            );
            let clean_mask = &products.get(".mask").pixels;
            for ((x, y), &value) in clean_mask.indexed_iter() {
                let included =
                    bounds.is_some_and(|[x0, y0, x1, y1]| x >= x0 && x <= x1 && y >= y0 && y <= y1);
                assert_eq!(value, f32::from(included), "{label} at [{x},{y}]");
            }
            let model = &products.get(".model").pixels;
            assert_model_inside(model, clean_mask);
            if bounds.is_none() {
                assert_eq!(
                    summary.clean_stop_reason,
                    Some(ImagerCleanStopReason::NoCleanablePixels)
                );
                assert_eq!(summary.actual_minor_iterations, 0);
                assert!(model.iter().all(|&value| value == 0.0));
                assert!(
                    products
                        .get(".residual")
                        .pixels
                        .iter()
                        .any(|value| value.abs() > 0.1)
                );
            } else {
                assert!(model.sum() > 0.0, "{label} must admit the point source");
            }
        }
        let (_, default) = observation.image(
            &format!("default-mask-{pblimit}"),
            json!({"pblimit": pblimit, "niter": 10}),
        );
        assert!(
            default.get(".model").pixels.sum() > 0.0,
            "absent mask still admits cleaning"
        );
    }
}
