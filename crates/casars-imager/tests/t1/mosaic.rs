// SPDX-License-Identifier: LGPL-3.0-or-later

//! Mosaic gridder, natural weighting, MFS: three ALMA 12 m pointings along
//! right ascension observed round-robin, a point at the mosaic centre and
//! one at the eastern pointing's centre. The primary-beam image is the
//! root of the summed beam powers at the pointings, the flat-noise image
//! carries it, and the PB-corrected image recovers the sky (IF-3, #652).

use std::collections::BTreeSet;

use casa_ms::SyntheticAntenna;
use casa_numerics::AnnularApertureVoltageTable;
use ndarray::Array2;
use serde_json::json;

use super::fixture::{Component, Geometry, Observation, Setup, box_sum, peak};

/// 50 GHz: a 119″ beam (CASA's 10.7 m ALMA aperture) over 4″ resolution.
const FREQUENCY_HZ: f64 = 50.0e9;
/// 1″ cells, four per synthesised beam of the 290 m spiral; ±256″ field.
const GEOMETRY: Geometry = Geometry {
    image_size: 512,
    cell_arcsec: 1.0,
};
/// Pointing spacing in cells: 80″, two thirds of the beam FWHM.
const SPACING_PX: i32 = 80;
const CENTRE: Component = Component {
    offset_px: [0, 0],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
/// At the eastern pointing's centre (`l` grows east, opposite to `x`).
const EAST: Component = Component {
    offset_px: [-SPACING_PX, 0],
    flux_jy: 0.8,
    fwhm_px: None,
    spectral_index: 0.0,
};
const NOISE_JY: f32 = 0.1;
/// About five times the thermal image noise of the 1-hour track.
const THRESHOLD_JY: f32 = 0.005;
/// `HetArrayConvFunc::findAntennaSizes`: CASA evaluates every 12 m ALMA
/// dish as a 10.7 m aperture with 0.75 m blockage, tabulated to 150″ at
/// 100 GHz (`PBMath1DAiry`); the simulator applies the same aperture.
const ALMA_APERTURE_M: f64 = 10.7;
const ALMA_BLOCKAGE_M: f64 = 0.75;
const ALMA_TABLE_RADIUS_ARCMIN_GHZ: f64 = 150.0 / 60.0 * 100.0;

/// Ten 12 m dishes on a golden-angle spiral of 20 m to 146 m around the
/// ALMA site (`Observatories` WGS84 position), in ITRF.
fn alma_antennas() -> Vec<SyntheticAntenna> {
    let longitude = (-67.754_929_f64).to_radians();
    let latitude = (-23.022_886_f64).to_radians();
    let height_m = 5056.8;
    let a = 6_378_137.0_f64;
    let flattening = 1.0 / 298.257_223_563;
    let e2 = flattening * (2.0 - flattening);
    let n = a / (1.0 - e2 * latitude.sin().powi(2)).sqrt();
    let centre = [
        (n + height_m) * latitude.cos() * longitude.cos(),
        (n + height_m) * latitude.cos() * longitude.sin(),
        (n * (1.0 - e2) + height_m) * latitude.sin(),
    ];
    let east = [-longitude.sin(), longitude.cos(), 0.0];
    let north = [
        -latitude.sin() * longitude.cos(),
        -latitude.sin() * longitude.sin(),
        latitude.cos(),
    ];
    (0..10)
        .map(|index| {
            let radius_m = 20.0 + 14.0 * index as f64;
            let angle = index as f64 * 137.507_764_f64.to_radians();
            let (e, nn) = (radius_m * angle.cos(), radius_m * angle.sin());
            let position_m = [
                centre[0] + e * east[0] + nn * north[0],
                centre[1] + e * east[1] + nn * north[1],
                centre[2] + e * east[2] + nn * north[2],
            ];
            SyntheticAntenna {
                name: format!("DA{:02}", 41 + index),
                station: format!("A{index:03}"),
                position_m,
                dish_diameter_m: 12.0,
            }
        })
        .collect()
}

/// West, centre and east pointings [`SPACING_PX`] cells apart along `l`.
fn setup() -> Setup {
    let spacing_rad = f64::from(SPACING_PX) * GEOMETRY.cell_rad();
    Setup {
        telescope: "ALMA".to_string(),
        antennas: alma_antennas(),
        fields: vec![
            Setup::pointing([-spacing_rad, 0.0]),
            Setup::pointing([0.0, 0.0]),
            Setup::pointing([spacing_rad, 0.0]),
        ],
        spectral: casa_ms::SyntheticSpectralSetup {
            name: "t1-50ghz".to_string(),
            start_frequency_hz: FREQUENCY_HZ,
            channel_width_hz: 2.0e9,
            channel_count: 1,
        },
        geometry: GEOMETRY,
        noise_jy: NOISE_JY,
    }
}

/// The beam power `VP²` of one 12 m dish `radius_px` cells from its
/// pointing, through CASA's Airy table.
fn beam_power(radius_px: f64) -> f64 {
    let table = AnnularApertureVoltageTable::new(
        ALMA_APERTURE_M,
        ALMA_BLOCKAGE_M,
        ALMA_TABLE_RADIUS_ARCMIN_GHZ,
    );
    let radius = radius_px * GEOMETRY.cell_arcsec / 60.0 * FREQUENCY_HZ / 1.0e9;
    f64::from(table.evaluate(radius)).powi(2)
}

/// `Σ PB_f²` at `x` cells east of the mosaic centre, over the three
/// pointings: the mosaic weight image's law.
fn summed_beam_power(x_px: f64) -> f64 {
    [-SPACING_PX, 0, SPACING_PX]
        .iter()
        .map(|pointing| beam_power((x_px - f64::from(*pointing)).abs()).powi(2))
        .sum()
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
fn mosaic_recovers_the_sky_under_the_summed_beams() {
    let observation = Observation::synthesise_setup(setup(), &[("centre", CENTRE), ("east", EAST)]);
    let (summary, products) = observation.image(
        "mosaic",
        json!({
            "weighting": { "kind": "natural" },
            "deconvolver": "hogbom",
            "niter": 3000,
            "threshold_jy": THRESHOLD_JY,
            "field_ids": [0, 1, 2],
            "phasecenter_field": 1,
            "mosaic_gridder": true,
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
    let east_pixel = geometry.pixel(EAST);
    // `.pb` is the root of the weight image over its peak: at the centre
    // pointing by symmetry, and at the eastern pointing's centre the ratio
    // of the summed beam powers.
    let pb_centre = f64::from(pb.pixels[centre_pixel]);
    let pb_east = f64::from(pb.pixels[east_pixel]);
    let expected_pb_east =
        (summed_beam_power(f64::from(SPACING_PX)) / summed_beam_power(0.0)).sqrt();
    let (centre_peak, centre_at, centre_offset) = local_peak(&pbcor.pixels, centre_pixel, 6);
    let (east_peak, east_at, east_offset) = local_peak(&pbcor.pixels, east_pixel, 6);
    let east_flat_noise = f64::from(image.pixels[east_pixel]);
    let model_centre = box_sum(&products.get(".model").pixels, centre_pixel, 2);
    eprintln!(
        "T1 mosaic: samples {}, thermal {noise_jy:.3e} Jy; products {:?}; pb centre \
         {pb_centre:.5}, east {pb_east:.5} (law {expected_pb_east:.5}); pbcor centre \
         {centre_peak:.5} Jy at {centre_at:?} offset {centre_offset:.3?}, east {east_peak:.5} \
         Jy at {east_at:?} offset {east_offset:.3?}; flat-noise east {east_flat_noise:.5} Jy; \
         model centre {model_centre:.5} Jy; {} minor iterations, {} major cycles, stop {:?}",
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
    assert_eq!(
        f64::from(products.get(".sumwt").pixels[[0, 0]]),
        observation.stokes_i_samples() as f64
    );
    assert!(
        (pb_centre - 1.0).abs() < 1.0e-3,
        "pb at the mosaic centre {pb_centre}"
    );
    assert!(
        (pb_east - expected_pb_east).abs() < 0.03,
        "pb at the eastern pointing {pb_east}, summed-beam law {expected_pb_east}"
    );
    // The PB-corrected image is the sky: each point at its pixel within a
    // tenth of a cell, at its flux within the thermal noise amplified by
    // the beam and the sidelobes of the other point's residual.
    assert_eq!(centre_at, centre_pixel);
    assert_eq!(east_at, east_pixel);
    assert!(centre_offset.iter().all(|offset| offset.abs() < 0.1));
    assert!(east_offset.iter().all(|offset| offset.abs() < 0.1));
    assert!(
        (f64::from(centre_peak) - CENTRE.flux_jy).abs() < 0.02 * CENTRE.flux_jy,
        "centre PB-corrected peak {centre_peak} Jy, injected {} Jy",
        CENTRE.flux_jy
    );
    assert!(
        (f64::from(east_peak) - EAST.flux_jy).abs() < 0.04 * EAST.flux_jy,
        "east PB-corrected peak {east_peak} Jy, injected {} Jy",
        EAST.flux_jy
    );
    // The flat-noise image is the sky times `.pb`.
    assert!(
        (east_flat_noise - EAST.flux_jy * pb_east).abs() < 0.04 * EAST.flux_jy,
        "east flat-noise peak {east_flat_noise} Jy, sky × pb {}",
        EAST.flux_jy * pb_east
    );
}
