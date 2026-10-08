// SPDX-License-Identifier: LGPL-3.0-or-later

//! AW projection, natural weighting, MFS: a compact EVLA configuration at
//! L band imaging 2.8° (six primary beams) with a natively generated
//! catalog, a synthetic paraboloid surface through the ported BeamCalc.
//! The catalog is generated, indexed and gridded end to end: the
//! sensitivity image peaks at the pointing, the point at the pointing
//! comes back at its flux, and the point a third of a beam off axis comes
//! back in place and PB-corrected (IF-3, #652). The simulator's beam is an
//! unblocked 25 m Airy disc, the catalog's is BeamCalc's illumination of
//! the paraboloid, so off axis the two agree only to several per cent;
//! T1.5 `refim_mawproject` carries the CASA parity.

use std::collections::BTreeSet;

use casa_ms::{SyntheticAntenna, SyntheticSpectralSetup, tutorial_vla_a_antennas};
use ndarray::Array2;
use serde_json::json;

use super::fixture::{Component, Geometry, Observation, Setup, box_sum, peak};

/// L band, inside the native EVLA model's 0.9–8 GHz bands.
const FREQUENCY_HZ: f64 = 1.5e9;
/// The A-configuration baselines shrunk by this factor: a 1 km array with
/// a 41″ synthesised beam, so a 10″ cell images six 28′ primary beams in
/// 1024 cells and the 25 m aperture spans 25 uv cells of the catalog.
const CONFIGURATION_SHRINK: f64 = 35.0;
const GEOMETRY: Geometry = Geometry {
    image_size: 1024,
    cell_arcsec: 10.0,
};
const CENTRE: Component = Component {
    offset_px: [0, 0],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
/// 58 cells (9.7′, a third of the beam FWHM) off axis, where the beam
/// power is about 0.7.
const OFF_AXIS: Component = Component {
    offset_px: [50, -30],
    flux_jy: 0.7,
    fwhm_px: None,
    spectral_index: 0.0,
};
const NOISE_JY: f32 = 0.65;
/// About five times the thermal image noise of the 1-hour track.
const THRESHOLD_JY: f32 = 0.01;
/// CASA `tclean` freezes the EVLA cache at 32 W planes.
const W_PLANES: usize = 32;

/// A paraboloid dish surface in the EVLA radius/height/slope text format:
/// 12.5 m radius sampled every centimetre.
fn surface_text() -> String {
    (0..=1250)
        .map(|index| {
            let radius = f64::from(index) / 100.0;
            format!(
                "{radius:.2} {:.8} {:.8}\n",
                0.028 * radius * radius,
                0.056 * radius
            )
        })
        .collect()
}

/// The A-configuration pads drawn towards their centroid by
/// [`CONFIGURATION_SHRINK`].
fn compact_evla_antennas() -> Vec<SyntheticAntenna> {
    let mut antennas = tutorial_vla_a_antennas();
    let count = antennas.len() as f64;
    let centroid = antennas.iter().fold([0.0; 3], |sum, antenna| {
        [
            sum[0] + antenna.position_m[0] / count,
            sum[1] + antenna.position_m[1] / count,
            sum[2] + antenna.position_m[2] / count,
        ]
    });
    for antenna in &mut antennas {
        for axis in 0..3 {
            antenna.position_m[axis] =
                centroid[axis] + (antenna.position_m[axis] - centroid[axis]) / CONFIGURATION_SHRINK;
        }
    }
    antennas
}

fn setup() -> Setup {
    Setup {
        telescope: "EVLA".to_string(),
        antennas: compact_evla_antennas(),
        fields: Vec::new(),
        spectral: SyntheticSpectralSetup {
            name: "t1-lband".to_string(),
            start_frequency_hz: FREQUENCY_HZ,
            channel_width_hz: 8.0e6,
            channel_count: 1,
        },
        geometry: GEOMETRY,
        noise_jy: NOISE_JY,
    }
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

#[test]
fn aw_projection_generates_its_catalog_and_recovers_the_sky() {
    let observation =
        Observation::synthesise_setup(setup(), &[("centre", CENTRE), ("off-axis", OFF_AXIS)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let catalog = observation.scratch("native-cf");
    let (summary, products) = observation.image(
        "aw",
        json!({
            "weighting": { "kind": "natural" },
            "deconvolver": "hogbom",
            "niter": 3000,
            "threshold_jy": THRESHOLD_JY,
            "w_project_planes": W_PLANES,
            "aw_project": {
                "source": {
                    "kind": "native-evla",
                    "root": catalog,
                    "surface": surface,
                    "policy": "generate-missing",
                    "working_size": 2048,
                    "oversampling": 4,
                    "cache_bytes": 2_147_483_648_u64,
                    "maximum_cells": 128,
                },
                "cf_resident_mb": 512,
                "normalization": "flatnoise",
            },
            "pbcor": true,
            "write_pb": true,
        }),
    );
    let geometry = observation.geometry();
    let noise_jy = observation.image_noise_jy();
    let image = products.get(".image");
    let pb = products.get(".pb");
    let pbcor = products.get(".image.pbcor");
    let centre_pixel = geometry.pixel(CENTRE);
    let off_axis_pixel = geometry.pixel(OFF_AXIS);
    let (pb_peak, pb_at, _) = peak(&pb.pixels);
    let pb_off_axis = f64::from(pb.pixels[off_axis_pixel]);
    let (centre_peak, centre_at, centre_offset) = local_peak(&pbcor.pixels, centre_pixel, 6);
    let (off_axis_peak, off_axis_at, off_axis_offset) =
        local_peak(&pbcor.pixels, off_axis_pixel, 6);
    let off_axis_flat_noise = f64::from(image.pixels[off_axis_pixel]);
    let model_centre = box_sum(&products.get(".model").pixels, centre_pixel, 2);
    eprintln!(
        "T1 AW: samples {}, thermal {noise_jy:.3e} Jy; products {:?}; pb peak {pb_peak:.5} at \
         {pb_at:?}, off axis {pb_off_axis:.5}; pbcor centre {centre_peak:.5} Jy at {centre_at:?} \
         offset {centre_offset:.3?}, off axis {off_axis_peak:.5} Jy at {off_axis_at:?} offset \
         {off_axis_offset:.3?}; flat-noise off axis {off_axis_flat_noise:.5} Jy; model centre \
         {model_centre:.5} Jy; {} minor iterations, {} major cycles, stop {:?}",
        observation.row_channel_samples(),
        products.suffixes(),
        summary.actual_minor_iterations,
        summary.major_cycles,
        summary.clean_stop_reason,
    );

    assert_eq!(
        products.suffixes(),
        BTreeSet::from(
            [
                ".image",
                ".image.pbcor",
                ".mask",
                ".model",
                ".pb",
                ".psf",
                ".residual",
                ".sumwt",
                ".weight",
            ]
            .map(String::from)
        )
    );
    observation.assert_image_wcs(image);
    // The sensitivity image peaks at the pointing; a third of a beam off
    // axis it has fallen to about 0.7.
    assert_eq!(pb_at, centre_pixel);
    assert!(
        (f64::from(pb_peak) - 1.0).abs() < 1.0e-3,
        "pb peak {pb_peak}"
    );
    assert!(
        (0.55..0.85).contains(&pb_off_axis),
        "pb {pb_off_axis} at the off-axis point"
    );
    // Each point comes back at its pixel within a tenth of a cell. At the
    // pointing both beams are one, so the PB-corrected flux holds to the
    // thermal noise plus the other point's uncleaned sidelobes; off axis it
    // holds to the two beam models' disagreement.
    assert_eq!(centre_at, centre_pixel);
    assert_eq!(off_axis_at, off_axis_pixel);
    assert!(centre_offset.iter().all(|offset| offset.abs() < 0.1));
    assert!(off_axis_offset.iter().all(|offset| offset.abs() < 0.1));
    assert!(
        (f64::from(centre_peak) - CENTRE.flux_jy).abs() < 0.02 * CENTRE.flux_jy,
        "centre PB-corrected peak {centre_peak} Jy, injected {} Jy",
        CENTRE.flux_jy
    );
    assert!(
        (model_centre - CENTRE.flux_jy).abs() < 0.02 * CENTRE.flux_jy,
        "centre model {model_centre} Jy"
    );
    assert!(
        (f64::from(off_axis_peak) - OFF_AXIS.flux_jy).abs() < 0.1 * OFF_AXIS.flux_jy,
        "off-axis PB-corrected peak {off_axis_peak} Jy, injected {} Jy",
        OFF_AXIS.flux_jy
    );
    // The flat-noise image is the sky times `.pb`.
    assert!(
        (off_axis_flat_noise - f64::from(off_axis_peak) * pb_off_axis).abs()
            < 0.02 * OFF_AXIS.flux_jy,
        "off-axis flat-noise peak {off_axis_flat_noise} Jy, pbcor × pb {}",
        f64::from(off_axis_peak) * pb_off_axis
    );
}
