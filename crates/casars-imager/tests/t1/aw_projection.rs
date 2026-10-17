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
//! T1.5 `refim_mawproject` carries the CASA parity. A two-window track
//! checks that a native cache generated for another SPW selection is
//! regenerated, not reused (#735, #741).

use std::path::Path;
use std::sync::Arc;

use casa_logging::{CasaLogLayer, CasaPriority, MemorySink};
use casa_ms::{SyntheticAntenna, SyntheticSpectralSetup, tutorial_vla_a_antennas};
use casa_simulation_synthesis::{AiryPrimaryBeam, AiryVoltagePattern};
use ndarray::Array2;
use serde_json::{Value, json};
use tracing_subscriber::layer::SubscriberExt;

use super::fixture::{
    Component, Geometry, Observation, ProductContract, Products, Setup, SpectralAxis, box_sum, peak,
};

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
const THRESHOLD_JY: f64 = 0.01;
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
        for (position, centre) in antenna.position_m.iter_mut().zip(centroid) {
            *position = centre + (*position - centre) / CONFIGURATION_SHRINK;
        }
    }
    antennas
}

fn setup() -> Setup {
    Setup {
        telescope: "EVLA".to_string(),
        antennas: compact_evla_antennas(),
        fields: Vec::new(),
        spectral_windows: vec![lband_window("t1-lband", FREQUENCY_HZ)],
        geometry: GEOMETRY,
        noise_jy: NOISE_JY,
    }
}

/// One 8 MHz channel at `frequency_hz`.
fn lband_window(name: &str, frequency_hz: f64) -> SyntheticSpectralSetup {
    SyntheticSpectralSetup {
        name: name.to_string(),
        start_frequency_hz: frequency_hz,
        channel_width_hz: 8.0e6,
        channel_count: 1,
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

/// The off-axis point's flux as the simulator attenuates it: the injected
/// flux times the power of the unblocked 25 m Airy disc an `EVLA`
/// observation gets (`AiryVoltagePattern`). A standard-gridder control run
/// in this test's process measured the same value to 0.6 % (#741); the
/// analytic value saves that run.
fn attenuated_off_axis_jy() -> f64 {
    let [l_rad, m_rad] = OFF_AXIS.direction_cosines(GEOMETRY);
    let voltage = AiryVoltagePattern::new(AiryPrimaryBeam {
        dish_diameter_m: 25.0,
        blockage_diameter_m: 0.0,
    })
    .evaluate_offsets(l_rad, m_rad, FREQUENCY_HZ);
    OFF_AXIS.flux_jy * f64::from(voltage).powi(2)
}

#[test]
fn aw_projection_generates_its_catalog_and_recovers_the_sky() {
    let observation =
        Observation::synthesise_setup(setup(), &[("centre", CENTRE), ("off-axis", OFF_AXIS)]);
    let attenuated_off_axis_jy = attenuated_off_axis_jy();
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let catalog = observation.scratch("native-cf");
    let (summary, products) = observation.image(
        "aw",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 3000,
            "threshold": format!("{THRESHOLD_JY}Jy"),
            "gridder": "awproject",
            "wprojplanes": W_PLANES,
            "aw_cf_source": "native-evla",
            "native_cf_cache": catalog,
            "evla_surface": surface,
            "native_cf_policy": "generate-missing",
            "native_cf_working_size": 2048,
            "native_cf_oversampling": 4,
            "native_cf_cache_bytes": 2_147_483_648_u64,
            "native_cf_maximum_cells": 128,
            "cf_resident_mb": 512,
            "normtype": "flatnoise",
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
    let model = products.get(".model");
    let model_centre = box_sum(&model.pixels, centre_pixel, 2);
    let model_centre_wide = [6, 12].map(|half| box_sum(&model.pixels, centre_pixel, half));
    let model_off_axis = box_sum(&model.pixels, off_axis_pixel, 2);
    let residual = products.get(".residual");
    let residual_centre = f64::from(residual.pixels[centre_pixel]);
    let residual_off_axis = f64::from(residual.pixels[off_axis_pixel]);
    // The PSF at the other point's offset from the pointing: the dirty
    // beam's sidelobe there under the weight cell's PB² taper.
    let psf_at_offset = f64::from(products.get(".psf").pixels[off_axis_pixel]);
    eprintln!(
        "T1 AW: samples {}, thermal {noise_jy:.3e} Jy; simulator-attenuated off-axis flux \
         {attenuated_off_axis_jy:.5} Jy; products {:?}; pb peak {pb_peak:.5} at \
         {pb_at:?}, off axis {pb_off_axis:.5}; pbcor centre {centre_peak:.5} Jy at {centre_at:?} \
         offset {centre_offset:.3?}, off axis {off_axis_peak:.5} Jy at {off_axis_at:?} offset \
         {off_axis_offset:.3?}; flat-noise off axis {off_axis_flat_noise:.5} Jy; model centre \
         {model_centre:.5} Jy (13 and 25 px boxes {model_centre_wide:.5?}), off axis \
         {model_off_axis:.5} Jy; residual centre \
         {residual_centre:.5} Jy, off axis {residual_off_axis:.5} Jy; psf at the other point's \
         offset {psf_at_offset:.5}; {} minor iterations, {} major cycles, stop {:?}",
        observation.row_channel_samples(),
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
            natural: false,
            pblimit: 0.2,
            uncorrected_masks: true,
        },
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
    // pointing both beams are one. The PB-corrected flux there holds to
    // the thermal noise, the other point's uncleaned sidelobes, and CLEAN's
    // redistribution under CASA's PSF rule: `AWProjectFT::findConvFunction`
    // grids the PSF with the weight cell (`cfwts2_p`), whose transform
    // carries the PB² taper, while the flat-noise residual's response to a
    // point is the untapered dirty beam. With this snapshot's elongated
    // beam (a 0.13 tail eight cells along the major axis) the mismatch
    // moves about 2 % of the model off the centre cell before the true
    // residual falls under the threshold; the imaging-cell PSF, which
    // CASA does not use, gives 0.9997 Jy here. Off axis the flux holds to
    // the two beam models' disagreement.
    assert_eq!(centre_at, centre_pixel);
    assert_eq!(off_axis_at, off_axis_pixel);
    assert!(centre_offset.iter().all(|offset| offset.abs() < 0.1));
    assert!(off_axis_offset.iter().all(|offset| offset.abs() < 0.1));
    assert!(
        (f64::from(centre_peak) - CENTRE.flux_jy).abs() < 0.03 * CENTRE.flux_jy,
        "centre PB-corrected peak {centre_peak} Jy, injected {} Jy",
        CENTRE.flux_jy
    );
    assert!(
        (model_centre_wide[0] - CENTRE.flux_jy).abs() < 0.04 * CENTRE.flux_jy,
        "centre model {model_centre_wide:?} Jy in the 13 and 25 px boxes"
    );
    assert!(
        residual_centre.abs() < 2.0 * THRESHOLD_JY,
        "centre residual {residual_centre} Jy"
    );
    // Off axis the flat-noise image holds the attenuated flux the standard
    // gridder measures (the catalog's beam applied once by the adjoint and
    // divided out once by `sqrt(weight)`), and the PB-corrected image
    // divides it by the catalog's beam; the injected flux comes back to
    // within the two beam models' disagreement.
    assert!(
        (off_axis_flat_noise - attenuated_off_axis_jy).abs() < 0.03 * attenuated_off_axis_jy,
        "off-axis flat-noise peak {off_axis_flat_noise} Jy, standard gridder \
         {attenuated_off_axis_jy} Jy"
    );
    let expected_pbcor = attenuated_off_axis_jy / pb_off_axis;
    assert!(
        (f64::from(off_axis_peak) - expected_pbcor).abs() < 0.03 * expected_pbcor,
        "off-axis PB-corrected peak {off_axis_peak} Jy, attenuated / pb {expected_pbcor} Jy"
    );
    assert!(
        (f64::from(off_axis_peak) - OFF_AXIS.flux_jy).abs() < 0.1 * OFF_AXIS.flux_jy,
        "off-axis PB-corrected peak {off_axis_peak} Jy, injected {} Jy",
        OFF_AXIS.flux_jy
    );
}

/// The two-window track's geometry: the same 10″ cell over three primary
/// beams, half the side of [`GEOMETRY`].
const TWO_WINDOW_GEOMETRY: Geometry = Geometry {
    image_size: 512,
    cell_arcsec: 10.0,
};

/// [`setup`] observing two L-band windows, 1.4 and 1.6 GHz, on the
/// smaller [`TWO_WINDOW_GEOMETRY`].
fn two_window_setup() -> Setup {
    Setup {
        spectral_windows: vec![
            lband_window("t1-lband-low", 1.4e9),
            lband_window("t1-lband-high", 1.6e9),
        ],
        geometry: TWO_WINDOW_GEOMETRY,
        ..setup()
    }
}

/// A short native-AW Högbom clean of the SPWs `spw` with its cells in
/// `cache`, generated when it lacks them.
fn cached_clean(spw: &str, cache: &Path, surface: &Path) -> Value {
    json!({
        "spw": spw,
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 50,
        "threshold": format!("{THRESHOLD_JY}Jy"),
        "gridder": "awproject",
        "wprojplanes": W_PLANES,
        "aw_cf_source": "native-evla",
        "native_cf_cache": cache,
        "evla_surface": surface,
        "native_cf_policy": "generate-missing",
        "native_cf_working_size": 1024,
        "native_cf_oversampling": 4,
        "native_cf_cache_bytes": 2_147_483_648_u64,
        "native_cf_maximum_cells": 256,
        "cf_resident_mb": 512,
        "normtype": "flatnoise",
        "pbcor": true,
        "write_pb": true,
    })
}

/// The entries of a cache directory, sorted.
fn entries(cache: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(cache)
        .expect("read the cache directory")
        .map(|entry| {
            entry
                .expect("cache entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// `products` and `fresh` publish the same members with the same values,
/// to rounding.
fn assert_same_products(products: &Products, fresh: &Products) {
    assert_eq!(products.suffixes(), fresh.suffixes(), "inventory");
    for suffix in fresh.suffixes() {
        let (values, expected) = (products.get(&suffix).values(), fresh.get(&suffix).values());
        assert_eq!(values.shape(), expected.shape(), "{suffix} shape");
        let scale = expected
            .iter()
            .fold(0.0_f32, |peak, value| peak.max(value.abs()));
        let worst = values
            .iter()
            .zip(&expected)
            .fold(0.0_f32, |worst, (value, expected)| {
                worst.max((value - expected).abs())
            });
        assert!(
            worst <= 1.0e-6 * scale,
            "{suffix}: differs from a fresh cache's by {worst} (peak {scale})"
        );
    }
}

/// #735 end to end: a cell's conjugate beam and its lookup depend on the
/// whole selected frequency list, so a native cache generated for two SPWs
/// does not serve a run of one. Run A images both windows into the cache,
/// run B images the lower window with the same cache and `generate-missing`,
/// run C images it into a fresh cache. B warns that the cache belongs to
/// another request, regenerates it to exactly C's cells, and publishes C's
/// products.
#[test]
fn a_native_cache_reused_across_an_spw_selection_change_gives_a_fresh_caches_products() {
    let observation = Observation::synthesise_setup(
        two_window_setup(),
        &[("centre", CENTRE), ("off-axis", OFF_AXIS)],
    );
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let (shared, fresh_cache) = (
        observation.scratch("shared-cf"),
        observation.scratch("fresh-cf"),
    );

    let (_, wide) = observation.image("aw-wide", cached_clean("0,1", &shared, &surface));
    let wide_entries = entries(&shared);

    let warnings = Arc::new(MemorySink::new(CasaPriority::Warn));
    let subscriber = tracing_subscriber::registry().with(CasaLogLayer::filtered(warnings.clone()));
    let (_, reused) = tracing::subscriber::with_default(subscriber, || {
        observation.image("aw-reused", cached_clean("0", &shared, &surface))
    });
    let (_, fresh) = observation.image("aw-fresh", cached_clean("0", &fresh_cache, &surface));

    let regenerations = warnings
        .records()
        .expect("captured warnings")
        .into_iter()
        .filter(|record| {
            record.message.contains("does not belong to this request")
                && record.message.contains("regenerating it")
        })
        .count();
    assert_eq!(
        regenerations, 1,
        "B warns once that it regenerates the cache"
    );
    // A's cache held both windows' cells; B left exactly the cells a fresh
    // cache of the one window holds.
    assert_eq!(entries(&shared), entries(&fresh_cache));
    assert_ne!(wide_entries, entries(&fresh_cache), "A's cache was wider");
    assert_ne!(
        wide.get(".image").values(),
        fresh.get(".image").values(),
        "A images both windows"
    );
    assert_same_products(&reused, &fresh);
}
