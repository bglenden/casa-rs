// SPDX-License-Identifier: LGPL-3.0-or-later

//! Mosaic gridder, natural weighting, MFS: three ALMA 12 m pointings along
//! right ascension observed round-robin, a point at the mosaic centre and
//! one at the eastern pointing's centre. The primary-beam image is the
//! root of the summed beam powers at the pointings, the flat-noise image
//! carries it, and the PB-corrected image recovers the sky (IF-3, #652).

use casa_ms::SyntheticAntenna;
use casa_numerics::AnnularApertureVoltageTable;
use ndarray::{Array2, ShapeBuilder};
use serde_json::json;

use super::fixture::{
    Component, Geometry, Observation, PHASE_CENTER_RAD, ProductContract, Setup, SpectralAxis,
    box_sum, peak,
};

/// What a cleaned, PB-corrected mosaic run publishes.
const MOSAIC_INVENTORY: [&str; 9] = [
    ".image",
    ".image.pbcor",
    ".mask",
    ".model",
    ".pb",
    ".psf",
    ".residual",
    ".sumwt",
    ".weight",
];

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
const THRESHOLD: &str = "0.005Jy";
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
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 3000,
            "threshold": THRESHOLD,
            "field": "0,1,2",
            "phasecenter_field": "1",
            "gridder": "mosaic",
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

    observation.assert_product_contract(
        &products,
        &ProductContract {
            inventory: &MOSAIC_INVENTORY,
            spectral: SpectralAxis::BandCentre,
            natural: true,
            pblimit: 0.2,
            uncorrected_masks: true,
        },
    );
    observation.assert_image_wcs(image);
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

/// Plan section 5.7's flat-noise and flat-sky rows: a dirty flat-noise
/// residual divides by `pbmax·√weight`, a flat-sky one by the weight, so
/// flat noise is flat sky times the PB inside the PB's support, and both
/// are zero outside it. A negative pblimit keeps that support and leaves
/// `.image` and `.residual` unmasked.
#[test]
fn mosaic_flat_noise_is_flat_sky_times_the_pb() {
    let observation = Observation::synthesise_setup(setup(), &[("centre", CENTRE), ("east", EAST)]);
    let run = |name: &str, normtype: &str, pblimit: f64| {
        let (_, products) = observation.image(
            name,
            json!({
                "weighting": "natural",
                "niter": 0,
                "field": "0,1,2",
                "phasecenter_field": "1",
                "gridder": "mosaic",
                "normtype": normtype,
                "pblimit": pblimit,
                "write_pb": true,
            }),
        );
        observation.assert_product_contract(
            &products,
            &ProductContract {
                inventory: &[
                    ".image",
                    ".model",
                    ".pb",
                    ".psf",
                    ".residual",
                    ".sumwt",
                    ".weight",
                ],
                spectral: SpectralAxis::BandCentre,
                natural: true,
                pblimit: pblimit.abs() as f32,
                uncorrected_masks: pblimit >= 0.0,
            },
        );
        products
    };
    let flat_noise = run("mosaic-flatnoise", "flatnoise", 0.2);
    let flat_sky = run("mosaic-flatsky", "flatsky", -0.2);
    let pb = flat_noise.get(".pb").values();
    assert_eq!(pb, flat_sky.get(".pb").values(), "one PB for both");
    let (noise, sky) = (
        flat_noise.get(".residual").values(),
        flat_sky.get(".residual").values(),
    );
    let peak = noise
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.abs()));
    for ((noise, sky), pb) in noise.iter().zip(&sky).zip(&pb) {
        if *pb > 0.2 {
            assert!(
                (noise - sky * pb).abs() <= 1.0e-5 * peak,
                "flat noise {noise}, flat sky {sky} × PB {pb}"
            );
        } else {
            assert_eq!((*noise, *sky), (0.0, 0.0), "outside the PB support");
        }
    }
}

/// The standard gridder's analytic PB (`write_pb`) is CASA's: a beam on
/// each selected field's pointing, summed and divided by its peak, whatever
/// the image centre (`SIMapper::addPB`, `SIImageStore::makePBImage`; #702).
#[test]
fn standard_gridder_pb_follows_the_pointings() {
    let observation = Observation::synthesise_setup(setup(), &[("centre", CENTRE)]);
    let centre = format!(
        "J2000 {}rad {}rad",
        PHASE_CENTER_RAD[0], PHASE_CENTER_RAD[1]
    );
    let pb = |name: &str, fields: &str| {
        let (_, products) = observation.image(
            name,
            json!({
                "weighting": "natural",
                "niter": 0,
                "field": fields,
                "phasecenter": centre,
                "write_pb": true,
                // Below every beam value the sum compares, so nothing is
                // zeroed before it is summed.
                "pblimit": 1.0e-6,
            }),
        );
        products.get(".pb").pixels.clone()
    };
    let reference = GEOMETRY.image_size / 2;
    let spacing = SPACING_PX as usize;
    let fields = ["0", "1", "2"].map(|field| pb(&format!("standard-pb-{field}"), field));

    // The western field's beam is centred on its pointing, 80 cells west of
    // the image centre (+x is west), not on the image centre.
    let west = &fields[0];
    assert_eq!(
        west[[reference + spacing, reference]],
        1.0,
        "peak on the pointing"
    );
    for offset in [20, 40, spacing] {
        let (inside, outside) = (
            west[[reference + spacing - offset, reference]],
            west[[reference + spacing + offset, reference]],
        );
        assert!(
            (inside - outside).abs() < 1.0e-6,
            "symmetric about the pointing at {offset} cells: {inside} and {outside}"
        );
    }
    assert!(
        west[[reference, reference]] < 0.9,
        "the image centre is 80″ from the western beam's peak"
    );

    // All three fields: the beams summed, divided by the sum's peak.
    let all = pb("standard-pb-all", "0,1,2");
    let sum = &(&fields[0] + &fields[1]) + &fields[2];
    let peak = sum.iter().fold(0.0_f32, |peak, value| peak.max(*value));
    assert!(peak > 1.0, "overlapping beams sum above one");
    for (actual, summed) in all.iter().zip(&sum) {
        assert!(
            (actual - summed / peak).abs() < 1.0e-5,
            "PB {actual}, normalized sum {}",
            summed / peak
        );
    }
}

/// An outlier domain's analytic PB is the tail of the beam on the selected
/// pointing, evaluated on the outlier's own pixels (`SIMapper::addPB` on
/// every mapper, #702): an outlier centred 60″ east of the only pointing
/// carries the main image's PB at 60″ east on its reference pixel, not a
/// beam of its own peaking there.
#[test]
fn an_outlier_pb_is_the_main_beams_tail() {
    let observation = Observation::synthesise_setup(setup(), &[("centre", CENTRE)]);
    let offset_px = 60;
    let outlier_centre = Setup::pointing([f64::from(offset_px) * GEOMETRY.cell_rad(), 0.0]);
    let outlier = observation.scratch("outlier-pb");
    let outlier_file = observation.scratch("pb.outlier");
    std::fs::write(
        &outlier_file,
        format!(
            "imagename={}\nimsize=[64,64]\ncell=[{}arcsec,{}arcsec]\nphasecenter=J2000 {}rad {}rad\n",
            outlier.display(),
            GEOMETRY.cell_arcsec,
            GEOMETRY.cell_arcsec,
            outlier_centre[0],
            outlier_centre[1],
        ),
    )
    .expect("write the outlier file");
    let (_, products) = observation.image(
        "outlier-pb-main",
        json!({
            "weighting": "natural",
            "niter": 0,
            "field": "1",
            "phasecenter": format!("J2000 {}rad {}rad", PHASE_CENTER_RAD[0], PHASE_CENTER_RAD[1]),
            "write_pb": true,
            "pblimit": 1.0e-6,
            "outlierfile": outlier_file,
        }),
    );
    let reference = GEOMETRY.image_size / 2;
    let main = products.get(".pb").pixels[[reference - offset_px as usize, reference]];
    let outlier_pb = casa_images::PagedImage::<f32>::open(format!("{}.pb", outlier.display()))
        .expect("outlier .pb")
        .get()
        .expect("read outlier .pb");
    let tail = outlier_pb[[32, 32, 0, 0]];
    assert!(
        (0.2..0.8).contains(&main),
        "the main PB 60″ from the pointing is {main}"
    );
    assert!(
        (tail - main).abs() < 1.0e-4,
        "outlier PB at its centre {tail}, main PB at the same sky position {main}"
    );
}

/// A field whose `PHASE_DIR` polynomial has a rate is refused for an
/// analytic PB: CASA evaluates it at the field's first selected time
/// (`SIMapper::addPB`'s `phaseDirMeas`), casa-rs only reads its constant
/// term (#702).
#[test]
fn an_analytic_pb_refuses_a_time_varying_field_direction() {
    let observation = Observation::synthesise_setup(setup(), &[("centre", CENTRE)]);
    {
        let mut ms = casa_ms::MeasurementSet::open(observation.measurement_set()).expect("reopen");
        let pointing = Setup::pointing([0.0, 0.0]);
        // [2, NUM_POLY + 1] in Fortran order: the constant pointing, then
        // a right-ascension rate of 1e-9 rad/s.
        let polynomial = ndarray::ArrayD::from_shape_vec(
            ndarray::IxDyn(&[2, 2]).f(),
            vec![pointing[0], pointing[1], 1.0e-9, 0.0],
        )
        .expect("PHASE_DIR shape");
        let mut field = ms
            .subtable_mut(casa_ms::SubtableId::Field)
            .expect("FIELD")
            .row_accessor_mut();
        field
            .set_cell(
                1,
                "NUM_POLY",
                casa_types::Value::Scalar(casa_types::ScalarValue::Int32(1)),
            )
            .expect("NUM_POLY");
        field
            .set_cell(
                1,
                "PHASE_DIR",
                casa_types::Value::Array(casa_types::ArrayValue::Float64(polynomial)),
            )
            .expect("PHASE_DIR");
        ms.save().expect("save");
    }
    let Err(error) = observation.try_image(
        "polynomial-field-pb",
        json!({
            "weighting": "natural",
            "niter": 0,
            "field": "1",
            "write_pb": true,
        }),
    ) else {
        panic!("an analytic PB on a time-varying field is refused");
    };
    assert!(
        error.contains("FIELD 1 has a time-varying PHASE_DIR"),
        "{error}"
    );
}
