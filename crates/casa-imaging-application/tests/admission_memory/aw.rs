// SPDX-License-Identifier: LGPL-3.0-or-later

//! The admission laws of AW projection: an MT-MFS image and a cube.

use std::path::{Path, PathBuf};

use casa_ms::{
    SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticObservationRequest,
    SyntheticSkyModel, SyntheticSpectralSetup, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};
use serde_json::json;

use super::{
    AMPLE, alone, check, evla_observation, evla_surface, imaging, mtmfs_controls, over_charge, warm,
};

/// A cube keeps each plane's weight image as a mosaic cube does, and its
/// admission covers it (#719). This catalog lies under the automatic bound's
/// floor, as in the MT-MFS law below, so it is not raised here.
#[test]
fn an_aw_cube_stays_within_its_admission() {
    if !alone("aw::an_aw_cube_stays_within_its_admission") {
        return;
    }
    check_aw_cube_admission("0:0", "linear");
}

/// #1121: a nearest-channel AW cube's live heap stays within its combined
/// admission when a selected window contains more than one channel.
#[test]
fn a_multichannel_nearest_aw_cube_stays_within_its_admission() {
    if !alone("aw::a_multichannel_nearest_aw_cube_stays_within_its_admission") {
        return;
    }
    check_aw_cube_admission("0", "nearest");
}

fn check_aw_cube_admission(spw: &str, interpolation: &str) {
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = evla_observation(root.path());
    let surface = evla_surface(root.path());
    warm(&measurement_set, root.path());
    let values = json!({
        "vis": measurement_set,
        "imagename": root.path().join("aw-cube"),
        "imsize": 1024,
        "cell": "10arcsec",
        "spw": spw,
        "specmode": "cube",
        "interpolation": interpolation,
        "deconvolver": "hogbom",
        "weighting": "natural",
        "niter": 200,
        "cycleniter": 50,
        "gain": 0.2,
        "threshold": "1mJy",
        "gridder": "awproject",
        "wprojplanes": 32,
        "aw_cf_source": "native-evla",
        "native_cf_cache": root.path().join("native-cf"),
        "evla_surface": surface,
        "native_cf_policy": "generate-missing",
        "native_cf_working_size": 2048,
        "native_cf_oversampling": 4,
        "native_cf_cache_bytes": 2_147_483_648_u64,
        "native_cf_maximum_cells": 128,
        "cf_resident_mb": "auto",
        "normtype": "flatnoise",
        "pbcor": true,
    });
    let mut breaches = Vec::new();
    check(
        &mut breaches,
        "aw cube",
        &imaging::request(values),
        1,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// The AW cube keeps its wideband kernels through Taylor reduction,
/// model projection and refresh without exceeding the combined admission.
#[test]
fn aw_mvc_clean_stays_within_its_held_admission() {
    if !alone("aw::aw_mvc_clean_stays_within_its_held_admission") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let ms = l_band_observation(root.path(), "EVLA", &[1.2e9, 1.5e9, 1.8e9], 1, false);
    let surface = evla_surface(root.path());
    warm(&ms, root.path());
    let values = json!({
        "vis": ms, "imagename": root.path().join("aw-mvc"),
        "imsize": 512, "cell": "10arcsec", "specmode": "mvc",
        "deconvolver": "mtmfs", "nterms": 2, "nchan": 3, "reffreq": "1.5GHz",
        "weighting": "natural", "gridder": "awproject", "wprojplanes": 1,
        "aw_cf_source": "native-evla", "native_cf_cache": root.path().join("native-cf"),
        "evla_surface": surface, "native_cf_policy": "generate-missing",
        "native_cf_working_size": 1024, "native_cf_oversampling": 4,
        "native_cf_cache_bytes": 268435456, "native_cf_maximum_cells": 32,
        "normtype": "flatnoise", "niter": 2, "cycleniter": 1, "pbcor": true,
    });
    let mut breaches = Vec::new();
    let measured = check(&mut breaches, "AW MVC", &imaging::request(values), 1, AMPLE);
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    let outcome = measured.outcome.unwrap();
    assert_eq!(outcome.total_actual_minor_iterations, 2);
    assert_eq!(outcome.major_cycle_count, 3);
}

/// #738: native A-projection MT-MFS at 1024², generating its catalog and
/// then reusing it, is admitted close to what it holds. Generation is
/// admitted before it starts: uncharged, it held 274 MB beyond the
/// admitted memory. A catalog whose groups all fit its bound is charged
/// those groups: charged the bound of `cf_resident_mb = 512`, the run
/// reserved 974 MB for a peak of 398 MB. An automatic bound
/// (`cf_resident_mb = auto`, #449) is admitted by the same path. This
/// catalog's 15.6 MB of cells lie under its 256 MiB floor, so the bound is
/// not raised here (#781 review): `aw::cache`'s laws pin the raise, and B3
/// measured it end to end.
#[test]
fn aw_mtmfs_is_admitted_close_to_what_it_holds() {
    if !alone("aw::aw_mtmfs_is_admitted_close_to_what_it_holds") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = evla_observation(root.path());
    let surface = evla_surface(root.path());
    warm(&measurement_set, root.path());
    let cache = root.path().join("native-cf");
    let mut breaches = Vec::new();
    for (label, resident) in [
        ("aw generating", json!(512)),
        ("aw reusing", json!(512)),
        ("aw automatic", json!("auto")),
    ] {
        let mut values = json!({
            "vis": measurement_set,
            "imagename": root.path().join(label.replace(' ', "-")),
            "imsize": 1024,
            "cell": "10arcsec",
            "weighting": "natural",
            "niter": 200,
            "cycleniter": 50,
            "gain": 0.2,
            "threshold": "1mJy",
            "gridder": "awproject",
            "wprojplanes": 32,
            "aw_cf_source": "native-evla",
            "native_cf_cache": cache,
            "evla_surface": surface,
            "native_cf_policy": "generate-missing",
            "native_cf_working_size": 2048,
            "native_cf_oversampling": 4,
            "native_cf_cache_bytes": 2_147_483_648_u64,
            "native_cf_maximum_cells": 128,
            "cf_resident_mb": resident,
            "normtype": "flatnoise",
            "pbcor": true,
        });
        values.as_object_mut().expect("AW controls").extend(
            mtmfs_controls()
                .as_object()
                .expect("MT-MFS controls")
                .clone(),
        );
        let measured = check(&mut breaches, label, &imaging::request(values), 1, AMPLE);
        breaches.extend(over_charge(label, &measured));
    }
    assert!(breaches.is_empty(), "{breaches:#?}");
}

/// The single-field AWPLPG operator admits its aperture scratch, tap
/// interpolation and all resident data and weight kernels.
#[test]
fn awp2_stays_within_its_admission() {
    if !alone("aw::awp2_stays_within_its_admission") {
        return;
    }
    check_awp2_admission("mfs", "hogbom", "I", &[1.5e9], 2, false);
}

/// AWP2 shares one admitted kernel set across the spectral cube planes.
#[test]
fn awp2_cube_stays_within_its_admission() {
    if !alone("aw::awp2_cube_stays_within_its_admission") {
        return;
    }
    check_awp2_admission("cube", "hogbom", "I", &[1.2e9, 1.5e9, 1.8e9], 1, false);
}

/// The nearest-cube route also admits a single native window of many channels.
#[test]
fn awp2_nearest_cube_stays_within_its_admission() {
    if !alone("aw::awp2_nearest_cube_stays_within_its_admission") {
        return;
    }
    check_awp2_admission("cube", "hogbom", "I", &[1.5e9], 3, false);
}

/// The shared aperture is charged beside four gridded polarizations.
#[test]
fn awp2_polarized_cube_stays_within_its_admission() {
    if !alone("aw::awp2_polarized_cube_stays_within_its_admission") {
        return;
    }
    check_awp2_admission("cube", "hogbom", "IQUV", &[1.5e9], 3, false);
}

/// The two-term MFS solve holds its Taylor products alongside three beam kernels.
#[test]
fn awp2_taylor_stays_within_its_admission() {
    if !alone("aw::awp2_taylor_stays_within_its_admission") {
        return;
    }
    check_awp2_admission("mfs", "mtmfs", "I", &[1.2e9, 1.5e9, 1.8e9], 1, false);
}

/// The cube kernels, Taylor solve, projected model and output families
/// stay within admission through a flat-noise AWP2 MVC run.
#[test]
fn awp2_mvc_stays_within_its_admission() {
    if !alone("aw::awp2_mvc_stays_within_its_admission") {
        return;
    }
    check_awp2_admission("mvc", "mtmfs", "I", &[1.2e9, 1.5e9, 1.8e9], 1, false);
}

/// Squinted Stokes I MVC retains PA geometry and W-plane kernels alongside
/// the cube and Taylor products.
#[test]
fn awp2_squinted_mvc_stays_within_its_admission() {
    if !alone("aw::awp2_squinted_mvc_stays_within_its_admission") {
        return;
    }
    check_awp2_admission("mvc", "mtmfs", "I", &[1.2e9], 16, true);
}

fn check_awp2_admission(
    mode: &str,
    deconvolver: &str,
    stokes: &str,
    frequencies: &[f64],
    channels_per_window: usize,
    squint: bool,
) {
    let root = tempfile::tempdir().expect("test root");
    let measurement_set =
        l_band_observation(root.path(), "VLA", frequencies, channels_per_window, squint);
    let mut ms = casa_ms::MeasurementSet::open(&measurement_set).unwrap();
    for spw in 0..frequencies.len() {
        ms.spectral_window_mut()
            .unwrap()
            .set_i32(
                spw,
                "MEAS_FREQ_REF",
                casa_types::measures::frequency::FrequencyRef::TOPO.casacore_code(),
            )
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);
    warm(&measurement_set, root.path());
    let mut values = json!({
        "vis": measurement_set,
        "imagename": root.path().join("awp2"),
        "gridder": "awp2",
        "specmode": mode,
        "deconvolver": deconvolver,
        "stokes": stokes,
        "imsize": 100,
        "cell": "8arcsec",
        "evla_surface": evla_surface(root.path()),
        "stage_timing": true,
    });
    if squint {
        values["computepastep"] = json!(90.0);
        values["wprojplanes"] = json!(2);
        values["weighting"] = json!("uniform");
    }
    let output_channels = if mode == "mvc" {
        values["nchan"] = json!(3);
        values["cycleniter"] = json!(2);
        values["pbcor"] = json!(true);
        3
    } else if mode == "cube" {
        let output_channels = frequencies.len() * channels_per_window;
        values["nchan"] = json!(output_channels);
        if channels_per_window == 1 {
            values["start"] = json!("1.2GHz");
            values["width"] = json!("300MHz");
        } else {
            values["interpolation"] = json!("nearest");
        }
        output_channels
    } else {
        1
    };
    if deconvolver == "mtmfs" {
        values["niter"] = json!(10);
    }
    if squint {
        values["perchanweightdensity"] = json!(true);
    }
    let request = imaging::request(values);
    let mut breaches = Vec::new();
    let measured = check(&mut breaches, "AWP2", &request, 1, AMPLE);
    assert!(breaches.is_empty(), "{breaches:#?}");
    let outcome = measured.outcome.unwrap();
    if mode == "mvc" {
        assert_eq!(outcome.problem.requires_parallactic_angles(), squint);
        assert!(
            outcome.major_cycle_count > 1,
            "MVC refreshes the cube model"
        );
        let products = outcome.product_names();
        assert!(products.iter().any(|name| name == ".weight"));
        assert!(!products.iter().any(|name| name.starts_with(".weight.tt")));
        assert!(products.iter().any(|name| name == ".image.tt1.pbcor"));
    } else if deconvolver == "mtmfs" {
        assert_eq!(
            outcome.problem.reconstruction().basis(),
            casa_imaging_model::ReconstructionBasis::Taylor { terms: 2 },
        );
    }
    assert_eq!(
        outcome.problem.geometry().spectral().output_channels(),
        output_channels,
        "AWP2 keeps every requested output channel",
    );
    assert_eq!(
        outcome
            .problem
            .reconstruction()
            .polarization()
            .coordinates()
            .len(),
        stokes.len(),
        "AWP2 keeps every requested Stokes plane",
    );
    if squint {
        let density = outcome.problem.weighting().density_scope();
        assert!(density.is_continuum());
        assert_eq!(density.fields().unwrap().count(), 2);
        let channel_centres = |outcome: &casa_imaging_application::ImagingOutcome| {
            (0..output_channels)
                .map(|channel| {
                    outcome
                        .problem
                        .geometry()
                        .spectral()
                        .channel_centre_hz(channel)
                })
                .collect::<Vec<_>>()
        };
        let both_fields = channel_centres(&outcome);
        drop(outcome);

        let mut first_field = request;
        first_field.vis[0].field = Some("0".to_string());
        first_field.imagename = root.path().join("awp2-field-zero");
        first_field.niter = 0;
        let first = check(
            &mut breaches,
            "AWP2 first selected field",
            &first_field,
            1,
            AMPLE,
        );
        assert!(breaches.is_empty(), "{breaches:#?}");
        assert_eq!(both_fields, channel_centres(&first.outcome.unwrap()));
    }
}

pub(super) fn l_band_observation(
    root: &Path,
    telescope: &str,
    frequencies_hz: &[f64],
    channels_per_window: usize,
    two_fields: bool,
) -> PathBuf {
    let measurement_set = root.join(format!("admission-{telescope}.ms"));
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
            *position = centre + (*position - centre) / 35.0;
        }
    }
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        antennas,
    );
    request.telescope_name = telescope.to_string();
    request.duration_seconds = 3_600.0;
    request.integration_seconds = 60.0;
    if two_fields {
        use casa_ms::simulation::SyntheticField;
        let [ra, dec] = request.phase_center_rad;
        request.fields = vec![
            SyntheticField {
                name: request.field_name.clone(),
                phase_center_rad: [ra, dec],
            },
            SyntheticField {
                name: "offset".to_string(),
                phase_center_rad: [ra + 0.0005, dec + 0.0005],
            },
        ];
    }
    request.spectral_windows = frequencies_hz
        .iter()
        .enumerate()
        .map(|(index, &frequency)| SyntheticSpectralSetup {
            name: format!("admission-lband-{index}"),
            start_frequency_hz: frequency,
            channel_width_hz: 8.0e6,
            channel_count: channels_per_window,
        })
        .collect();
    request.model = Some(SyntheticSkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("admission-aw".to_string()),
        components: vec![SyntheticAnalyticComponent::Point {
            name: Some("point".to_string()),
            l_rad: 0.0,
            m_rad: 0.0,
            spectrum: SyntheticAnalyticSpectrum {
                flux_jy: 1.0,
                spectral_index: -0.7,
                reference_frequency_hz: Some(1.5e9),
                line_peak_jy: 0.0,
                line_center_fraction: 0.5,
                line_sigma_fraction: 0.1,
                absorption_peak_jy: 0.0,
                absorption_center_fraction: 0.5,
                absorption_sigma_fraction: 0.1,
            },
        }],
    });
    generate_synthetic_observation_ms(&request).expect("synthesise the EVLA MS");
    measurement_set
}

/// The restart reads saved Taylor images and converts its nonempty model
/// before the first prediction. Both phases stay within admission (#1070).
#[test]
fn aw_mtmfs_saved_residual_clean_stays_within_its_admission() {
    if !alone("aw::aw_mtmfs_saved_residual_clean_stays_within_its_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = evla_observation(root.path());
    warm(&measurement_set, root.path());
    let mut values = json!({
        "vis": measurement_set,
        "imagename": root.path().join("aw"),
        "imsize": 512,
        "cell": "10arcsec",
        "gridder": "awproject",
        "wprojplanes": 1,
        "aw_cf_source": "native-evla",
        "native_cf_cache": root.path().join("native-cf"),
        "evla_surface": evla_surface(root.path()),
        "native_cf_policy": "generate-missing",
        "native_cf_working_size": 1024,
        "native_cf_oversampling": 4,
        "native_cf_cache_bytes": 2_147_483_648_u64,
        "native_cf_maximum_cells": 128,
        "cf_resident_mb": "auto",
        "normtype": "flatnoise",
        "pblimit": -0.01,
    });
    values
        .as_object_mut()
        .unwrap()
        .extend(mtmfs_controls().as_object().unwrap().clone());
    values["pbcor"] = json!(false);
    values["write_pb"] = json!(true);
    values["threshold"] = json!("0Jy");
    values["niter"] = json!(1);
    let mut breaches = Vec::new();
    let first = check(
        &mut breaches,
        "seed AW model",
        &imaging::request(values.clone()),
        1,
        AMPLE,
    );
    assert_eq!(first.outcome.as_ref().unwrap().total_minor_iterations, 1);
    drop(first);
    values["calcpsf"] = json!(false);
    values["calcres"] = json!(false);
    let restart = imaging::request(values).with_tclean_restart().unwrap();
    let resumed = check(&mut breaches, "saved AW Taylor CLEAN", &restart, 1, AMPLE);
    let outcome = resumed.outcome.as_ref().unwrap();
    assert_eq!(
        (outcome.total_minor_iterations, outcome.major_cycle_count),
        (1, 1)
    );
    assert!(breaches.is_empty(), "{breaches:#?}");
}
