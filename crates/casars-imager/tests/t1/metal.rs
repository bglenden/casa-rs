// SPDX-License-Identifier: LGPL-3.0-or-later

//! Metal variants: `backend = gpu` through the production route agrees with
//! the CPU route product by product, to 1e-4 of the restored peak. The MFS
//! case also meets the CPU route's analytic laws (flux, position, beam,
//! residual noise). The cubes include output channels twice the native
//! width. Mosaics, MFS and cube, run on Metal since #787. Skipped, saying
//! so, on a host without a Metal device.

use serde_json::{Value, json};

use super::fixture::{Component, Observation, Products};
use super::standard_mfs::assert_recovers_the_point;

/// The application's refusal on a host whose inventory has no Metal device.
const NO_METAL_DEVICE: &str = "the Metal backend needs a unified-memory Metal 3 device";
const TOLERANCE: f64 = 1.0e-4;
/// Spectral index and its error, absolute, where the restored image is
/// above a tenth of its peak: `tt1/tt0` from terms that agree to 1e-4.
const ALPHA_TOLERANCE: f64 = 1.0e-3;

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: -2.0,
};

fn on_metal(mut controls: Value) -> Value {
    controls["backend"] = json!("gpu");
    controls
}

/// `true` (saying so) when the route refused Metal: off macOS, where
/// availability rejects it (pinned in the application's availability tests),
/// or for want of a device.
fn no_device(outcome: &Result<(casars_imager::ImagerRunReport, Products), String>) -> bool {
    if !cfg!(target_os = "macos") {
        eprintln!("skipped: Metal is macOS only");
        return true;
    }
    let refused = outcome
        .as_ref()
        .is_err_and(|error| error.contains(NO_METAL_DEVICE));
    if refused {
        eprintln!("skipped: no Metal device");
    }
    refused
}

fn assert_products_agree(metal: &Products, cpu: &Products, label: &str) {
    assert_eq!(metal.suffixes(), cpu.suffixes(), "{label}");
    let peak = |values: &ndarray::ArrayD<f32>| {
        values
            .iter()
            .fold(0.0_f64, |peak, value| peak.max(f64::from(*value).abs()))
    };
    let restored = if cpu.suffixes().contains(".image") {
        ".image"
    } else {
        ".image.tt0"
    };
    let restored_image = cpu.get(restored).image.get().expect("read image");
    let image_peak = peak(&restored_image);
    assert!(image_peak > 0.0, "{label}: the restored image is empty");
    for suffix in cpu.suffixes() {
        let metal = metal.get(&suffix).image.get().expect("read Metal product");
        let cpu = cpu.get(&suffix).image.get().expect("read CPU product");
        assert_eq!(metal.shape(), cpu.shape(), "{label} {suffix}");
        if suffix.starts_with(".alpha") {
            // The spectral index and its error are cut where the image falls
            // below a threshold, so a pixel at the cut may land on either
            // side; they are compared where the image is bright, absolutely.
            let worst = metal
                .iter()
                .zip(&cpu)
                .zip(&restored_image)
                .filter(|(_, image)| f64::from(**image) > 0.1 * image_peak)
                .fold(0.0_f64, |worst, ((m, c), _)| {
                    worst.max((f64::from(*m) - f64::from(*c)).abs())
                });
            assert!(
                worst <= ALPHA_TOLERANCE,
                "{label} {suffix}: largest difference {worst:e} where the image is bright"
            );
            continue;
        }
        let scale = peak(&cpu).max(image_peak);
        let worst = metal.iter().zip(&cpu).fold(0.0_f64, |worst, (m, c)| {
            worst.max((f64::from(*m) - f64::from(*c)).abs())
        });
        assert!(
            worst <= TOLERANCE * scale,
            "{label} {suffix}: largest difference {worst:e} against a peak of {scale:e}"
        );
    }
}

/// #787: the mosaic gridder on Metal, MFS and a two-channel cube, matches
/// the CPU route product by product, `.weight` and `.pb` included. The
/// weight image is summed by contribution before it is gridded (#792), so
/// Metal's f32 grid keeps its precision.
#[test]
fn metal_mosaics_match_the_cpu() {
    for cube in [false, true] {
        let (observation, controls) = super::mosaic::mosaic_run(cube);
        let label = if cube { "mosaic-cube" } else { "mosaic-mfs" };
        let mut probe = controls.clone();
        probe["niter"] = json!(0);
        if no_device(&observation.try_image(&format!("probe-{label}"), on_metal(probe))) {
            return;
        }
        let (_, metal) = observation.image(&format!("metal-{label}"), on_metal(controls.clone()));
        let (_, cpu) = observation.image(&format!("cpu-{label}"), controls);
        assert_products_agree(&metal, &cpu, label);
    }
}

/// #841 Phase 6b: a non-square image of non-square cells on Metal matches
/// the CPU route product by product.
#[test]
fn metal_non_square_image_matches_the_cpu() {
    let (observation, controls) = super::non_square::non_square_run();
    let mut probe = controls.clone();
    probe["niter"] = json!(0);
    if no_device(&observation.try_image("probe-non-square", on_metal(probe))) {
        return;
    }
    let (_, metal) = observation.image("metal-non-square", on_metal(controls.clone()));
    let (_, cpu) = observation.image("cpu-non-square", controls);
    assert_products_agree(&metal, &cpu, "non-square");
}

#[test]
fn metal_standard_mfs_hogbom_recovers_the_analytic_sky_like_the_cpu() {
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold": "0.005Jy",
    });
    let probe = Observation::synthesise(&[("point", POINT)]);
    if no_device(&probe.try_image("probe", on_metal(json!({ "niter": 0 })))) {
        return;
    }
    let (observation, _, metal) =
        assert_recovers_the_point("metal-standard-mfs-hogbom", on_metal(controls.clone()));
    let (_, cpu) = observation.image("cpu-standard-mfs-hogbom", controls);
    assert_products_agree(&metal, &cpu, "standard MFS Hogbom");
}

#[test]
fn metal_cubes_with_native_and_coarse_output_channels_match_the_cpu() {
    let observation = Observation::synthesise_band(16, &[("point", POINT)]);
    for (label, controls) in [
        (
            "native",
            json!({
                "specmode": "cube",
                "channel_count": 16,
                "weighting": "briggs",
                "robust": 0.5,
                "perchanweightdensity": true,
                "deconvolver": "hogbom",
                "niter": 200,
                "threshold": "0.01Jy",
            }),
        ),
        (
            "coarse",
            json!({
                "specmode": "cube",
                "channel_count": 8,
                "width": "2",
                "weighting": "natural",
                "deconvolver": "hogbom",
                "niter": 200,
                "threshold": "0.01Jy",
            }),
        ),
    ] {
        let metal =
            observation.try_image(&format!("metal-cube-{label}"), on_metal(controls.clone()));
        if no_device(&metal) {
            return;
        }
        let (_, metal) = metal.expect("Metal cube");
        let (_, cpu) = observation.image(&format!("cpu-cube-{label}"), controls);
        assert_products_agree(&metal, &cpu, &format!("{label} cube"));
    }
}

/// Two Taylor terms on Metal: `backend = auto` sends MT-MFS to the GPU.
#[test]
fn metal_mtmfs_matches_the_cpu() {
    let observation = Observation::synthesise_band(8, &[("point", POINT)]);
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "mtmfs",
        "nterms": 2,
        "niter": 200,
        "threshold": "0.01Jy",
    });
    let metal = observation.try_image("metal-mtmfs", on_metal(controls.clone()));
    if no_device(&metal) {
        return;
    }
    let (_, metal) = metal.expect("Metal MT-MFS");
    let (_, cpu) = observation.image("cpu-mtmfs", controls);
    assert_products_agree(&metal, &cpu, "MT-MFS");
}

/// Facets on Metal, each facet a pass domain of its own (#664):
/// `backend = auto` sends faceted standard imaging to the GPU.
#[test]
fn metal_facets_match_the_cpu() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let controls = json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold": "0.005Jy",
        "facets": 2,
    });
    let metal = observation.try_image("metal-facets", on_metal(controls.clone()));
    if no_device(&metal) {
        return;
    }
    let (_, metal) = metal.expect("Metal facets");
    let (_, cpu) = observation.image("cpu-facets", controls);
    assert_products_agree(&metal, &cpu, "facets");
}
