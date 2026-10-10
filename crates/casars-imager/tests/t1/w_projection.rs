// SPDX-License-Identifier: LGPL-3.0-or-later

//! W projection, natural weighting, MFS: a 51′ field of the VLA-A track
//! at 150 MHz and declination −23°, where `π w θ²` reaches a few radians
//! at a far-off-axis point. The W-planes set recovers it in place at full
//! flux; the standard gridder, run on the same data as the control,
//! smears and displaces it (IF-3, #652).

use std::collections::BTreeSet;

use casa_ms::{SyntheticSpectralSetup, tutorial_vla_a_antennas};
use ndarray::Array2;
use serde_json::{Value, json};

use super::fixture::{Component, Geometry, Observation, Setup, box_sum, peak};

/// 150 MHz: the w term in turns scales with wavelength.
const FREQUENCY_HZ: f64 = 150.0e6;
/// 3″ cells, four per 11.5″ synthesised beam of the 36 km array at 2 m.
const GEOMETRY: Geometry = Geometry {
    image_size: 1024,
    cell_arcsec: 3.0,
};
/// Dishes shrunk to 6 m so the primary beam (20° FWHM at 2 m) is flat over
/// the field and the standard gridder's errors are the w term alone.
const DISH_DIAMETER_M: f64 = 6.0;
const CENTRE: Component = Component {
    offset_px: [0, 0],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
/// 494 cells (24.7′, 7.2 mrad) from the centre.
const FAR: Component = Component {
    offset_px: [-420, 260],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
const NOISE_JY: f32 = 0.65;
/// About five times the thermal image noise of the 1-hour track.
const THRESHOLD: &str = "0.01Jy";

fn setup() -> Setup {
    let mut antennas = tutorial_vla_a_antennas();
    for antenna in &mut antennas {
        antenna.dish_diameter_m = DISH_DIAMETER_M;
    }
    Setup {
        telescope: "VLA".to_string(),
        antennas,
        fields: Vec::new(),
        spectral: SyntheticSpectralSetup {
            name: "t1-150mhz".to_string(),
            start_frequency_hz: FREQUENCY_HZ,
            channel_width_hz: 1.0e6,
            channel_count: 1,
        },
        geometry: GEOMETRY,
        noise_jy: NOISE_JY,
    }
}

fn controls(gridder: &str) -> Value {
    json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 3000,
        "threshold": THRESHOLD,
        "gridder": gridder,
    })
}

/// The brightest pixel within `half` cells of `centre` and its parabolic
/// sub-pixel offset, as image coordinates.
fn local_peak(
    pixels: &Array2<f32>,
    centre: [usize; 2],
    half: usize,
) -> (f32, [usize; 2], [f64; 2]) {
    let window: Array2<f32> = pixels
        .slice(ndarray::s![
            centre[0] - half..=centre[0] + half,
            centre[1] - half..=centre[1] + half
        ])
        .to_owned();
    let (value, [x, y], offset) = peak(&window);
    (value, [centre[0] - half + x, centre[1] - half + y], offset)
}

/// Restored peak, pixel and sub-pixel offset of the far point, and its
/// clean flux, from one run.
struct FarPoint {
    peak_jy: f64,
    pixel: [usize; 2],
    offset_px: [f64; 2],
    model_jy: f64,
}

fn far_point(observation: &Observation, name: &str, controls: Value) -> FarPoint {
    let (summary, products) = observation.image(name, controls);
    let geometry = observation.geometry();
    let image = products.get(".image");
    let far_pixel = geometry.pixel(FAR);
    let (peak_jy, pixel, offset_px) = local_peak(&image.pixels, far_pixel, 12);
    let model_jy = box_sum(&products.get(".model").pixels, far_pixel, 12);
    let (centre_peak, centre_at, centre_offset) =
        local_peak(&image.pixels, geometry.pixel(CENTRE), 6);
    eprintln!(
        "T1 W {name}: products {:?}; far peak {peak_jy:.5} Jy at {pixel:?} (injected at \
         {far_pixel:?}) offset {offset_px:.3?}, far model {model_jy:.5} Jy; centre peak \
         {centre_peak:.5} Jy at {centre_at:?} offset {centre_offset:.3?}; {} minor iterations, \
         {} major cycles, stop {:?}",
        products.suffixes(),
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
    // `sumwt = Σ W·Re N` over unit-weight samples (`wprojgrid.f`); the W
    // planes' sampled real sums sit within a part in a thousand of one.
    let sumwt = f64::from(products.get(".sumwt").pixels[[0, 0]]);
    let samples = observation.stokes_i_samples() as f64;
    assert!(
        (sumwt - samples).abs() < 1.0e-3 * samples,
        "{name}: sumwt {sumwt}, samples {samples}"
    );
    // The centre point is w-free under both gridders.
    assert_eq!(centre_at, geometry.pixel(CENTRE));
    assert!(centre_offset.iter().all(|offset| offset.abs() < 0.1));
    assert!(
        (f64::from(centre_peak) - CENTRE.flux_jy).abs() < 0.02 * CENTRE.flux_jy,
        "{name}: centre peak {centre_peak} Jy"
    );
    FarPoint {
        peak_jy: f64::from(peak_jy),
        pixel,
        offset_px,
        model_jy,
    }
}

#[test]
fn w_projection_recovers_the_far_point_the_standard_gridder_smears() {
    let observation = Observation::synthesise_setup(setup(), &[("centre", CENTRE), ("far", FAR)]);
    let noise_jy = observation.image_noise_jy();
    eprintln!(
        "T1 W: samples {}, thermal {noise_jy:.3e} Jy",
        observation.row_channel_samples()
    );
    let projected = far_point(&observation, "w-projection", controls("wproject"));
    let standard = far_point(&observation, "w-standard", controls("standard"));

    let far_pixel = observation.geometry().pixel(FAR);
    assert_eq!(projected.pixel, far_pixel);
    assert!(
        projected.offset_px.iter().all(|offset| offset.abs() < 0.15),
        "W-projected far point offset {:?} px",
        projected.offset_px
    );
    for (product, flux_jy) in [
        ("restored peak", projected.peak_jy),
        ("model", projected.model_jy),
    ] {
        assert!(
            (flux_jy - FAR.flux_jy).abs() < 0.02 * FAR.flux_jy,
            "W-projected far {product} {flux_jy} Jy, injected {} Jy",
            FAR.flux_jy
        );
    }
    // The control: without the w term the far point is smeared or
    // displaced, so the test is sensitive to the planes it checks.
    let displaced =
        standard.pixel != far_pixel || standard.offset_px.iter().any(|offset| offset.abs() > 0.5);
    assert!(
        displaced || standard.peak_jy < 0.9 * projected.peak_jy,
        "standard gridder recovered the far point ({} Jy at {:?} offset {:?}) as well as W \
         projection ({} Jy)",
        standard.peak_jy,
        standard.pixel,
        standard.offset_px,
        projected.peak_jy
    );
}
