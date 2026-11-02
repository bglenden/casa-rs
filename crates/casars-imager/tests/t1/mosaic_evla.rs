// SPDX-License-Identifier: LGPL-3.0-or-later

//! Mosaic gridder on the EVLA (#980): three S-band pointings along right
//! ascension over a compact array of 25 m dishes at the VLA site, a point at
//! the mosaic centre and one at the eastern pointing's centre. CASA's tclean
//! builds the EVLA mosaic's beams from its common EVLA pattern
//! (`PBMath1DEVLA`, `HetArrayConvFunc` with `PBMathInterface::COMMONPB`),
//! which the synthesiser applies too: the primary-beam image is the root of
//! the summed beam powers at the pointings and the PB-corrected image
//! recovers the sky.

use casa_ms::SyntheticAntenna;
use casa_numerics::PrimaryBeam;
use serde_json::json;

use super::fixture::{
    Component, Geometry, Observation, ProductContract, Setup, SpectralAxis, peak,
};

/// 3 GHz: a 15′ beam over 34″ resolution of the 600 m array.
const FREQUENCY_HZ: f64 = 3.0e9;
/// 6″ cells, about six per synthesised beam; a 38′ field.
const GEOMETRY: Geometry = Geometry::square(384, 6.0);
/// Pointing spacing in cells: 7.5′, half the beam's FWHM.
const SPACING_PX: i32 = 75;
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
/// About five times the thermal image noise of the track.
const THRESHOLD: &str = "0.005Jy";

/// Ten 25 m dishes on a golden-angle spiral of 30 m to 300 m around the
/// VLA (`Observatories` position), in ITRF.
fn evla_antennas() -> Vec<SyntheticAntenna> {
    let longitude = (-107.617_728_f64).to_radians();
    let latitude = 34.078_749_f64.to_radians();
    let height_m = 2114.9;
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
            let radius_m = 30.0 + 30.0 * index as f64;
            let angle = index as f64 * 137.507_764_f64.to_radians();
            let (e, nn) = (radius_m * angle.cos(), radius_m * angle.sin());
            SyntheticAntenna {
                name: format!("ea{:02}", index + 1),
                station: format!("N{index:02}"),
                position_m: [0, 1, 2].map(|axis| centre[axis] + e * east[axis] + nn * north[axis]),
                dish_diameter_m: 25.0,
            }
        })
        .collect()
}

/// The beam power of an EVLA dish `radius_px` cells from its pointing at
/// [`FREQUENCY_HZ`]: CASA's polynomial fit of that frequency.
fn beam_power(radius_px: f64) -> f64 {
    let separation_rad = radius_px * GEOMETRY.cell_rad()[0];
    f64::from(
        PrimaryBeam::Evla
            .pattern()
            .power_at(separation_rad, FREQUENCY_HZ),
    )
}

/// `Σ PB²` at `x` cells east of the mosaic centre over the three
/// pointings: the mosaic weight image's law.
fn summed_beam_power(x_px: f64) -> f64 {
    [-SPACING_PX, 0, SPACING_PX]
        .iter()
        .map(|pointing| beam_power((x_px - f64::from(*pointing)).abs()).powi(2))
        .sum()
}

#[test]
fn an_evla_mosaic_recovers_the_sky_under_the_common_evla_beam() {
    let spacing_rad = f64::from(SPACING_PX) * GEOMETRY.cell_rad()[0];
    let setup = Setup {
        telescope: "EVLA".to_string(),
        antennas: evla_antennas(),
        fields: vec![
            Setup::pointing([-spacing_rad, 0.0]),
            Setup::pointing([0.0, 0.0]),
            Setup::pointing([spacing_rad, 0.0]),
        ],
        spectral_windows: vec![casa_ms::SyntheticSpectralSetup {
            name: "EVLA_S#A0C0#0".to_string(),
            start_frequency_hz: FREQUENCY_HZ,
            channel_width_hz: 128.0e6,
            channel_count: 1,
        }],
        geometry: GEOMETRY,
        noise_jy: NOISE_JY,
    };
    let observation = Observation::synthesise_setup(setup, &[("centre", CENTRE), ("east", EAST)]);
    let (summary, products) = observation.image(
        "evla-mosaic",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 3000,
            "threshold": THRESHOLD,
            "field": "0,1,2",
            "phasecenter": "1",
            "gridder": "mosaic",
            "pbcor": true,
            "write_pb": true,
        }),
    );
    let geometry = observation.geometry();
    let pb = products.get(".pb");
    let pbcor = products.get(".image.pbcor");
    let (centre_pixel, east_pixel) = (geometry.pixel(CENTRE), geometry.pixel(EAST));
    let pb_centre = f64::from(pb.pixels[centre_pixel]);
    // `.pb` along the row through the pointings, `x` cells east, and the
    // summed-beam law there.
    let pb_east = [37, SPACING_PX, 110].map(|x| {
        let pixel = [centre_pixel[0] - x as usize, centre_pixel[1]];
        let law = (summed_beam_power(f64::from(x)) / summed_beam_power(0.0)).sqrt();
        (x, f64::from(pb.pixels[pixel]), law)
    });
    let centre_peak = f64::from(pbcor.pixels[centre_pixel]);
    let east_peak = f64::from(pbcor.pixels[east_pixel]);
    let (_, peak_at, _) = peak(&pbcor.pixels);
    eprintln!(
        "T1 EVLA mosaic: products {:?}; pb centre {pb_centre:.5}, east (cells, pb, law) \
         {pb_east:.5?}; pbcor centre {centre_peak:.5} Jy, east {east_peak:.5} Jy, peak at \
         {peak_at:?}; {} minor iterations, {} major cycles, stop {:?}",
        products.suffixes(),
        summary.actual_minor_iterations,
        summary.major_cycles,
        summary.clean_stop_reason,
    );
    observation.assert_product_contract(
        &products,
        &ProductContract {
            inventory: &[
                ".image",
                ".image.pbcor",
                ".mask",
                ".model",
                ".pb",
                ".psf",
                ".residual",
                ".sumwt",
                ".weight",
            ],
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: 0.2,
            uncorrected_masks: true,
        },
    );
    assert!(
        (pb_centre - 1.0).abs() < 1.0e-3,
        "pb at the mosaic centre {pb_centre}"
    );
    for (x, pb, law) in pb_east {
        assert!(
            (pb - law).abs() < 0.01,
            "pb {x} cells east {pb}, summed EVLA-beam law {law}"
        );
    }
    assert_eq!(peak_at, centre_pixel);
    assert!(
        (centre_peak - CENTRE.flux_jy).abs() < 0.02 * CENTRE.flux_jy,
        "centre PB-corrected peak {centre_peak} Jy, injected {} Jy",
        CENTRE.flux_jy
    );
    assert!(
        (east_peak - EAST.flux_jy).abs() < 0.04 * EAST.flux_jy,
        "east PB-corrected peak {east_peak} Jy, injected {} Jy",
        EAST.flux_jy
    );
}
