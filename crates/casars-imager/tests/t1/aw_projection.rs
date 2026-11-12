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
//! regenerated, not reused (#735, #741), and that tclean's `conjbeams`,
//! `wbawp` and `wprojplanes` reach the cells (#980).

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
const GEOMETRY: Geometry = Geometry::square(1024, 10.0);
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
const TWO_WINDOW_GEOMETRY: Geometry = Geometry::square(512, 10.0);

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

/// A dirty run leaves raw higher Taylor weight moments. Recomputing the
/// PSF during CLEAN must preserve their scale and give the same result as
/// a fresh CLEAN. CASA can multiply those saved moments by the principal
/// sum of weights on restart (#1070).
#[test]
fn aw_mtmfs_restart_recomputes_taylor_weights() {
    let observation = Observation::synthesise_setup(two_window_setup(), &[("centre", CENTRE)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let cache = observation.scratch("cf");
    let mut controls = cached_clean("0,1", &cache, &surface);
    controls["deconvolver"] = json!("mtmfs");
    controls["nterms"] = json!(2);
    controls["scales"] = json!([0]);
    controls["reffreq"] = json!("1.45GHz");
    controls["threshold"] = json!("0Jy");
    controls["pbcor"] = json!(false);
    controls["niter"] = json!(0);
    let (_, dirty) = observation.image("continued", controls.clone());

    controls["niter"] = json!(10);
    let (continued, products) = observation.image("continued", controls.clone());
    let (fresh, fresh_products) = observation.image("fresh", controls);
    assert!(continued.minor_iterations > 0, "the restarted run cleans");
    assert_eq!(continued.minor_iterations, fresh.minor_iterations);
    assert_eq!(continued.major_cycles, fresh.major_cycles);
    for suffix in [".weight.tt1", ".weight.tt2"] {
        let before = dirty.get(suffix).values();
        let after = products.get(suffix).values();
        let peak = before
            .iter()
            .map(|value| value.abs())
            .fold(0.0_f32, f32::max);
        assert!(peak > 0.0, "{suffix} must exercise a nonzero weight moment");
        let difference = before
            .iter()
            .zip(&after)
            .map(|(before, after)| (before - after).abs())
            .fold(0.0_f32, f32::max);
        assert!(
            difference <= 1.0e-6 * peak,
            "{suffix} changed by {difference} from a dirty peak of {peak}"
        );
    }
    assert_same_products(&products, &fresh_products);
}

/// The saved AW Taylor PSF and weights stay on disk while the new
/// residual is formed and cleaned (#1070).
#[test]
fn aw_mtmfs_restart_uses_its_saved_psf_and_weights() {
    use super::restart::{assert_unchanged, snapshot};

    let source = Component {
        spectral_index: -0.7,
        ..CENTRE
    };
    let observation = Observation::synthesise_setup(two_window_setup(), &[("centre", source)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let cache = observation.scratch("cf");
    let mut controls = cached_clean("0,1", &cache, &surface);
    controls["deconvolver"] = json!("mtmfs");
    controls["nterms"] = json!(2);
    controls["scales"] = json!([0]);
    controls["reffreq"] = json!("1.45GHz");
    controls["threshold"] = json!("0Jy");
    controls["pbcor"] = json!(false);
    controls["niter"] = json!(0);
    let (_, dirty) = observation.image("continued", controls.clone());
    let [x, y] = observation.geometry().pixel(source);
    {
        let mut beam =
            casa_images::PagedImage::<f32>::open(observation.scratch("continued.pb.tt0")).unwrap();
        let mask_name = beam.default_mask_name().expect("the saved PB has a mask");
        let pixel = ndarray::Array4::from_elem((1, 1, 1, 1), false).into_dyn();
        assert!(dirty.get(".pb.tt0").mask().unwrap()[[x, y, 0, 0]]);
        beam.put_mask_slice(&mask_name, &pixel, &[x, y, 0, 0])
            .unwrap();
        beam.save().unwrap();
    }
    for term in [0, 1] {
        let mut residual = casa_images::PagedImage::<f32>::open(
            observation.scratch(&format!("continued.residual.tt{term}")),
        )
        .unwrap();
        residual.unset_default_mask().unwrap();
        residual.save().unwrap();
    }
    let saved_paths = dirty
        .suffixes()
        .into_iter()
        .filter(|name| {
            name.starts_with(".psf")
                || name.starts_with(".weight")
                || name.starts_with(".sumwt")
                || name == ".pb.tt0"
        })
        .map(|name| observation.scratch(&format!("continued{name}")))
        .collect::<Vec<_>>();
    assert_eq!(saved_paths.len(), 10);
    let saved = saved_paths
        .iter()
        .map(|path| snapshot(path))
        .collect::<Vec<_>>();

    controls["niter"] = json!(10);
    let (fresh, fresh_products) = observation.image("fresh", controls.clone());
    controls["calcpsf"] = json!(false);
    let missing = observation.scratch("continued.weight.tt1");
    let parked = observation.scratch("saved-weight-tt1");
    std::fs::rename(&missing, &parked).unwrap();
    let error = observation
        .try_image("continued", controls.clone())
        .err()
        .expect("missing weight must refuse");
    assert!(error.contains("continued.weight.tt1"), "{error}");
    std::fs::rename(parked, missing).unwrap();
    for (path, before) in saved_paths.iter().zip(&saved) {
        assert_unchanged(before, &snapshot(path));
    }

    let (continued, products) = observation.image("continued", controls);
    assert!(continued.minor_iterations > 0);
    assert_eq!(continued.minor_iterations, fresh.minor_iterations);
    assert_eq!(continued.major_cycles, fresh.major_cycles);
    assert!(products.suffixes().contains(".image.tt0"));
    assert_eq!(
        products.suffixes().len() + saved_paths.len(),
        fresh_products.suffixes().len()
    );
    for suffix in products.suffixes() {
        let actual = products.get(&suffix).values();
        let expected = fresh_products.get(&suffix).values();
        assert_eq!(actual.shape(), expected.shape(), "{suffix} shape");
        let reference_power = expected
            .iter()
            .map(|value| f64::from(*value).powi(2))
            .sum::<f64>();
        let difference_power = actual
            .iter()
            .zip(&expected)
            .map(|(actual, expected)| (f64::from(*actual) - f64::from(*expected)).powi(2))
            .sum::<f64>();
        assert!(
            difference_power <= 1.0e-12 * reference_power,
            "{suffix}: saved-PSF CLEAN differs from fresh CLEAN (error power {difference_power}, reference power {reference_power})"
        );
    }
    for suffix in [".image.tt0", ".residual.tt0"] {
        assert!(
            !products.get(suffix).mask().unwrap()[[x, y, 0, 0]],
            "{suffix}: use the saved PB mask"
        );
    }
    for (path, before) in saved_paths.iter().zip(&saved) {
        assert_unchanged(before, &snapshot(path));
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

/// #719: each plane of an AW cube is normalized by its own weights. On a grid
/// that drops more of the upper window's long baselines the planes' sums of
/// weights differ, and each plane's weight image peaks at the whole sample
/// weight over its own sum (`AWProjectWBFT::resampleCFToGrid`,
/// `SIImageStore::divideWeightBySumwt`). `divideResidualByWeight` divides by
/// the PB peak and the weight's root, so a point on axis of the same flux in
/// both planes reads its flux over that plane's own peak.
#[test]
fn each_plane_of_an_aw_cube_is_normalized_by_its_own_weights() {
    let observation = Observation::synthesise_setup(two_window_setup(), &[("centre", CENTRE)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let run = |name: &str, imsize: usize, cell: &str| {
        let cache = observation.scratch(&format!("{name}-cf"));
        let mut controls = cached_clean("0,1", &cache, &surface);
        let fields = controls.as_object_mut().expect("controls");
        fields.insert("niter".into(), json!(0));
        fields.insert("imsize".into(), json!([imsize, imsize]));
        fields.insert("cell".into(), json!([cell, cell]));
        fields.insert("specmode".into(), json!("cube"));
        fields.insert("nchan".into(), json!(2));
        fields.insert("start".into(), json!("1.4GHz"));
        fields.insert("width".into(), json!("200MHz"));
        fields.insert("interpolation".into(), json!("nearest"));
        observation.image(name, controls).1
    };
    let whole = run("aw-cube-holding", 1024, "10arcsec")
        .get(".sumwt")
        .channel_values();
    let coarse = run("aw-cube-coarse", 256, "40arcsec");
    let gridded = coarse.get(".sumwt").channel_values();
    let peaks = |suffix: &str| {
        [0, 1].map(|plane| {
            coarse
                .get(suffix)
                .plane(plane)
                .iter()
                .fold(0.0_f32, |m, v| m.max(*v))
        })
    };
    let expected = [whole[0] / gridded[0], whole[1] / gridded[1]];
    assert!(
        (expected[1] - expected[0]).abs() > 0.005,
        "the planes' weights must differ for this law: {expected:?}"
    );
    let (weight, residual) = (peaks(".weight"), peaks(".residual"));
    for plane in 0..2 {
        assert!(
            (weight[plane] - expected[plane]).abs() < 2.0e-3 * expected[plane],
            "plane {plane}'s weight image peaks at {weight:?}, not {expected:?}"
        );
    }
    let fluxes = [residual[0] * expected[0], residual[1] * expected[1]];
    assert!(
        (fluxes[0] - fluxes[1]).abs() < 0.02 * fluxes[0],
        "the point reads {fluxes:?} once each plane's own peak is taken out"
    );
}

/// Linear multi-channel AW cubes and tracked TOPO nearest cubes remain
/// refused naming their issue; a one-channel selection runs (#1121).
#[test]
fn an_aw_cube_of_a_window_of_several_channels_is_refused() {
    let setup = Setup {
        spectral_windows: vec![SyntheticSpectralSetup {
            channel_count: 2,
            ..lband_window("t1-lband-two", FREQUENCY_HZ)
        }],
        ..setup()
    };
    let observation = Observation::synthesise_setup(setup, &[("centre", CENTRE)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let controls = |spw: &str| {
        let mut controls = cached_clean(spw, &observation.scratch("two-cf"), &surface);
        let fields = controls.as_object_mut().expect("controls");
        fields.insert("niter".into(), json!(0));
        fields.insert("specmode".into(), json!("cube"));
        controls
    };
    let refused = observation
        .try_image("aw-two-channels", controls("0"))
        .err()
        .expect("a window of two channels");
    assert!(refused.contains("#1121"), "{refused}");
    let mut tracked = controls("0");
    let fields = tracked.as_object_mut().expect("controls");
    fields.insert("interpolation".into(), json!("nearest"));
    fields.insert("outframe".into(), json!("TOPO"));
    let refused = observation
        .try_image("aw-tracked-two-channels", tracked)
        .err()
        .expect("tracked TOPO AW cubes remain unsupported");
    assert!(refused.contains("#1121"), "{refused}");
    observation.image("aw-one-channel", controls("0:0"));
}

/// A multi-channel AW window with nearest interpolation places both native
/// channels on the cube, rather than refusing the run or losing a plane (#1121).
#[test]
fn an_aw_cube_nearest_samples_a_multichannel_window() {
    let setup = Setup {
        spectral_windows: vec![SyntheticSpectralSetup {
            channel_count: 2,
            ..lband_window("t1-lband-two-nearest", FREQUENCY_HZ)
        }],
        ..setup()
    };
    let observation = Observation::synthesise_setup(setup, &[("centre", CENTRE)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let mut controls = cached_clean("0", &observation.scratch("two-cf"), &surface);
    let fields = controls.as_object_mut().expect("controls");
    fields.insert("niter".into(), json!(0));
    fields.insert("specmode".into(), json!("cube"));
    fields.insert("nchan".into(), json!(2));
    fields.insert("interpolation".into(), json!("nearest"));
    let (_, products) = observation.image("aw-two-nearest", controls);
    let weights = products.get(".sumwt").channel_values();
    assert_eq!(weights.len(), 2);
    assert!(weights.iter().all(|weight| *weight > 0.0), "{weights:?}");
    for channel in 0..2 {
        let peak = products
            .get(".psf")
            .plane(channel)
            .iter()
            .copied()
            .fold(0.0_f32, f32::max);
        assert!((peak - 1.0).abs() < 1.0e-6, "channel {channel}: {peak}");
    }
}

/// A row off the grid still weighs in the sensitivity:
/// `AWProjectWBFT::resampleCFToGrid` puts every weight cell at the uv origin,
/// where `onGrid` holds, so the weight image's peak is the whole sample
/// weight over the sum of the rows that were gridded. On a grid too coarse
/// for the longest baselines the sum is lower than on one that holds them.
#[test]
fn a_row_off_the_grid_counts_in_the_aw_weight_image() {
    let observation = Observation::synthesise_setup(setup(), &[("centre", CENTRE)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let run = |name: &str, imsize: usize, cell: &str| {
        let mut controls = cached_clean("0", &observation.scratch(&format!("{name}-cf")), &surface);
        let fields = controls.as_object_mut().expect("controls");
        fields.insert("niter".into(), json!(0));
        fields.insert("imsize".into(), json!([imsize, imsize]));
        fields.insert("cell".into(), json!([cell, cell]));
        observation.image(name, controls).1
    };
    let holding = run("aw-holding", 1024, "10arcsec");
    let coarse = run("aw-coarse", 256, "40arcsec");
    let sum = |products: &Products| products.get(".sumwt").channel_values()[0];
    let (whole, gridded) = (sum(&holding), sum(&coarse));
    assert!(
        gridded < 0.99 * whole,
        "the coarse grid must drop rows: {gridded} of {whole}"
    );
    let weight_peak = coarse
        .get(".weight")
        .plane(0)
        .iter()
        .fold(0.0_f32, |m, v| m.max(*v));
    let expected = whole / gridded;
    assert!(
        (weight_peak - expected).abs() < 2.0e-3 * expected,
        "the weight image's peak is {weight_peak}, the whole weight over the gridded {expected}"
    );
}

/// tclean's AW terms reach the native cells and their lookup (#980). With
/// cells at each window's frequency, `conjbeams` grids each window with the
/// other's beam and weighs with the conjugate pair, so the products differ;
/// at the reference frequency alone (`wbawp=False`) every row takes the one
/// cell whatever `conjbeams` says (`AWConvFunc::makeFreqValList`), and the
/// products agree. A co-planar run (`wprojplanes=1`) makes one plane.
#[test]
fn aw_terms_select_the_native_cells_tclean_names() {
    let observation = Observation::synthesise_setup(two_window_setup(), &[("centre", CENTRE)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let dirty = |name: &str, terms: Value| {
        let mut controls =
            cached_clean("0,1", &observation.scratch(&format!("{name}-cf")), &surface);
        controls["niter"] = json!(0);
        for (term, value) in terms.as_object().expect("terms") {
            controls[term] = value.clone();
        }
        observation.image(name, controls).1
    };
    let conjugate = dirty("aw-conjugate", json!({ "conjbeams": true }));
    let direct = dirty("aw-direct", json!({ "conjbeams": false }));
    assert_ne!(
        conjugate.get(".pb").values(),
        direct.get(".pb").values(),
        "conjbeams changes the wideband weight cells"
    );
    let narrow = |name, conjbeams| dirty(name, json!({ "wbawp": false, "conjbeams": conjbeams }));
    assert_same_products(
        &narrow("aw-narrow-conjugate", true),
        &narrow("aw-narrow-direct", false),
    );
    let co_planar = observation.scratch("aw-co-planar-cf");
    let mut controls = cached_clean("0,1", &co_planar, &surface);
    controls["niter"] = json!(0);
    controls["wprojplanes"] = json!(1);
    observation.image("aw-co-planar", controls);
    // Two windows' cells, one plane, two Mueller elements, imaging and
    // weight images, beside the manifest and the lock.
    assert_eq!(
        entries(&co_planar)
            .iter()
            .filter(|name| name.starts_with("CFS_") || name.starts_with("WTCFS_"))
            .count(),
        2 * 2 * 2
    );
}

/// Under `usepointing` each row's antenna pointing and its AW pointing
/// pixel come from the POINTING table (one row an integration, as the
/// simulator writes for a stated field), over two windows whose epochs the
/// batches visit window by window, and whose two partitions share one
/// pointing query (#932 step 3b).
#[test]
fn aw_projection_images_on_its_pointing_table() {
    let setup = Setup {
        fields: vec![Setup::pointing([0.0, 0.0])],
        ..two_window_setup()
    };
    let observation = Observation::synthesise_setup(setup, &[("centre", CENTRE)]);
    let surface = observation.scratch("evla.surface");
    std::fs::write(&surface, surface_text()).expect("write the EVLA surface");
    let mut controls = cached_clean("0,1", &observation.scratch("cf"), &surface);
    controls["usepointing"] = json!(true);
    observation.image("aw-pointing-image", controls);
}
