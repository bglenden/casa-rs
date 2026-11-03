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
    assert_mask_holds_the_sources_only(mask);
    assert_model_inside(model, mask);
    assert_point_recovered(model, &products.get(".image").pixels);
}

/// The automatic mask covers both sources and stays about them. CASA grows
/// the inherited mask through the 1.5-sigma contour of each source and
/// smooths it by a beam (`autoMaskByMultiThreshold`), so it reaches a few
/// beams beyond the sources; a mask grown into the noise or the sidelobes
/// would reach across the image.
fn assert_mask_holds_the_sources_only(mask: &Array2<f32>) {
    let sources = [component_pixel(POINT), component_pixel(GAUSSIAN)];
    for [x, y] in sources {
        assert_eq!(mask[[x, y]], 1.0, "the source at [{x}, {y}] is not masked");
    }
    let masked = mask.iter().filter(|value| **value != 0.0).count();
    let farthest = mask
        .indexed_iter()
        .filter(|(_, value)| **value != 0.0)
        .map(|((x, y), _)| {
            sources
                .iter()
                .map(|[sx, sy]| x.abs_diff(*sx).pow(2) + y.abs_diff(*sy).pow(2))
                .min()
                .expect("two sources")
        })
        .max()
        .unwrap_or(0);
    eprintln!(
        "T1 auto-multithresh mask: {masked} pixels, the farthest {:.1} pixels from a source",
        (farthest as f64).sqrt()
    );
    assert!(
        farthest <= 32 * 32,
        "a masked pixel lies {} pixels from the nearest source",
        (farthest as f64).sqrt()
    );
    assert!(
        masked < IMAGE_SIZE * IMAGE_SIZE / 25,
        "auto-multithresh masked {masked} pixels"
    );
}

/// tclean's other auto-multithresh controls (contract 37): the robust noise
/// estimate (`fastnoise=False`, Chauvenet without a mask and the
/// statistics outside it after), wider smoothing, a higher cut, an unpruned
/// grown mask, the change-stop rule and the summary log still mask both
/// sources and confine the model; a negative cut keeps no pixel, so CASA's
/// zero-mask stop.
#[test]
fn auto_multithresh_controls_and_robust_noise_still_mask_both_sources() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold": THRESHOLD,
        "nsigma": 3.0,
        "usemask": "auto-multithresh",
        "fastnoise": false,
        "smoothfactor": 1.5,
        "cutthreshold": 0.05,
        "dogrowprune": false,
        "minpercentchange": 1.0,
        "verbose": true,
    });
    let (summary, products) = observation.image("auto-multithresh-controls", controls.clone());
    let mask = &products.get(".mask").pixels;
    let model = &products.get(".model").pixels;
    let masked = mask.iter().filter(|value| **value != 0.0).count();
    eprintln!(
        "T1 auto-multithresh controls: {masked} masked pixels, {} minor iterations, {} major \
         cycles",
        summary.actual_minor_iterations, summary.major_cycles
    );
    assert_mask_holds_the_sources_only(mask);
    assert_model_inside(model, mask);
    assert_point_recovered(model, &products.get(".image").pixels);

    let mut empty = controls;
    empty["cutthreshold"] = json!(-0.1);
    let (summary, products) = observation.image("auto-multithresh-negative-cut", empty);
    assert!(
        products
            .get(".mask")
            .pixels
            .iter()
            .all(|value| *value == 0.0)
    );
    assert!(
        products
            .get(".model")
            .pixels
            .iter()
            .all(|value| *value == 0.0)
    );
    assert_eq!(summary.actual_minor_iterations, 0);
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

/// A CRTF region limited to a spectral range (CASA's test_mask_2) masks an
/// MFS image when the range reaches its one channel, by casacore's
/// rounding of each end's pixel, and is left out when it misses it, which
/// here leaves an empty mask and no cleaning, as CASA's stop code 7. On a
/// cube it masks its channels only.
#[test]
fn a_crtf_range_masks_an_mfs_image_where_it_reaches_its_channel() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let [px, py] = component_pixel(POINT);
    let region = format!(
        "box[[{}pix,{}pix],[{}pix,{}pix]]",
        px - 6,
        py - 6,
        px + 6,
        py + 6
    );
    let controls = |mask: String| {
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 50,
            "threshold": THRESHOLD,
            "mask": mask,
        })
    };
    let (plain_summary, plain) = observation.image("range-plain", controls(region.clone()));
    let image = plain.get(".image");
    let (centre, width) = (
        image.channel_frequency_hz(0),
        image.channel_width_hz().abs(),
    );
    assert!(width > 0.0, "the MFS channel has a width");
    // Within a third of a channel of its centre: both ends round to it.
    let reaches = format!(
        "{region},range=[{}Hz,{}Hz]",
        centre - width / 3.0,
        centre + width / 3.0
    );
    let (summary, products) = observation.image("range-reaches", controls(reaches));
    assert_eq!(products.get(".mask").pixels, plain.get(".mask").pixels);
    assert!(plain_summary.actual_minor_iterations > 0);
    assert_eq!(
        summary.actual_minor_iterations,
        plain_summary.actual_minor_iterations
    );
    // From 0.6 of a channel above it: the low end rounds past the channel.
    let misses = format!(
        "{region},range=[{}Hz,{}Hz]",
        centre + 0.6 * width,
        centre + 2.0 * width
    );
    let (summary, products) = observation.image("range-misses", controls(misses));
    for product in [".mask", ".model"] {
        assert!(
            products
                .get(product)
                .pixels
                .iter()
                .all(|value| *value == 0.0),
            "{product}"
        );
    }
    assert_eq!(summary.actual_minor_iterations, 0);
    // On a cube the region masks only its channels: channel 1 of three is
    // masked and cleaned, the others hold no mask and no model.
    let cube = Observation::synthesise_band(3, &[("point", POINT)]);
    let (summary, products) = cube.image(
        "range-cube",
        json!({
            "specmode": "cube",
            "deconvolver": "hogbom",
            "niter": 50,
            "threshold": THRESHOLD,
            "mask": format!("{region},range=[1pix,1pix]"),
        }),
    );
    assert!(summary.actual_minor_iterations > 0);
    let (mask, model) = (products.get(".mask"), products.get(".model"));
    assert_eq!(mask.channels(), 3);
    let masked = |channel: usize| {
        mask.plane(channel)
            .iter()
            .filter(|value| **value != 0.0)
            .count()
    };
    assert_eq!(masked(1), 13 * 13, "the box on its channel");
    assert_eq!((masked(0), masked(2)), (0, 0), "no mask off its channel");
    for channel in [0, 2] {
        assert!(
            model.plane(channel).iter().all(|value| *value == 0.0),
            "channel {channel}"
        );
    }
    assert!(model.plane(1).iter().any(|value| *value != 0.0));
}
