// SPDX-License-Identifier: LGPL-3.0-or-later

//! Metal variants: `backend = metal` through the production route agrees with
//! the CPU route product by product, to 1e-4 of the restored peak. The MFS
//! case also meets the CPU route's analytic laws (flux, position, beam,
//! residual noise). The cubes include output channels twice the native
//! width. Skipped, saying so, on a host without a Metal device.

use serde_json::{Value, json};

use super::fixture::{Component, Observation, Products};
use super::standard_mfs::assert_recovers_the_point;

/// The application's refusal on a host whose inventory has no Metal device.
const NO_METAL_DEVICE: &str = "the Metal backend needs a unified-memory Metal 3 device";
const TOLERANCE: f64 = 1.0e-4;

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: -2.0,
};

fn on_metal(mut controls: Value) -> Value {
    controls["backend"] = json!("metal");
    controls
}

/// `true` (saying so) when the route refused Metal for want of a device.
fn no_device(outcome: &Result<(casars_imager::RunSummary, Products), String>) -> bool {
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
    let image_peak = peak(&cpu.get(".image").image.get().expect("read image"));
    for suffix in cpu.suffixes() {
        let metal = metal.get(&suffix).image.get().expect("read Metal product");
        let cpu = cpu.get(&suffix).image.get().expect("read CPU product");
        assert_eq!(metal.shape(), cpu.shape(), "{label} {suffix}");
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

#[test]
fn metal_standard_mfs_hogbom_recovers_the_analytic_sky_like_the_cpu() {
    let controls = json!({
        "weighting": { "kind": "natural" },
        "deconvolver": "hogbom",
        "niter": 500,
        "threshold_jy": 0.005,
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
                "spectral_mode": "cube",
                "channel_count": 16,
                "weighting": { "kind": "briggs", "robust": 0.5 },
                "per_channel_weight_density": true,
                "deconvolver": "hogbom",
                "niter": 200,
                "threshold_jy": 0.01,
            }),
        ),
        (
            "coarse",
            json!({
                "spectral_mode": "cube",
                "channel_count": 8,
                "cube_axis": { "width": { "kind": "channel", "channel": 2 } },
                "weighting": { "kind": "natural" },
                "deconvolver": "hogbom",
                "niter": 200,
                "threshold_jy": 0.01,
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
