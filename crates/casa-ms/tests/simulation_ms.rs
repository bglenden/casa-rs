// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native synthetic-observation MS writer tests.

use casa_ms::columns::main_ids;
use casa_ms::{
    MeasurementSet, SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticAntenna,
    SyntheticBandpassCorruption, SyntheticBandpassMode, SyntheticCorruptionConfig, SyntheticField,
    SyntheticGainCorruption, SyntheticGainMode, SyntheticNoiseCorruption, SyntheticNoiseMode,
    SyntheticObservationMode, SyntheticObservationRequest, SyntheticPointingCorruption,
    SyntheticPolarizationBasis, SyntheticPolarizationLeakageCorruption,
    SyntheticPolarizationLeakageMode, SyntheticPolarizationSetup, SyntheticSkyModel,
    SyntheticSpectralSetup, generate_synthetic_observation_ms, tutorial_vla_a_antennas,
};
use casa_test_support::{discover_casa_python, tutorial_dataset_path};
use casa_types::measures::position::MPosition;
use casa_types::{ArrayValue, ScalarValue, Value};
use std::process::Command;

fn antennas() -> Vec<SyntheticAntenna> {
    vec![
        SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
    ]
}

fn request(root: &std::path::Path) -> SyntheticObservationRequest {
    let model = root.join("ppdisk672_GHz_50pc.fits");
    write_test_fits_model(&model, 16, 16);
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        &model,
        root.join("ppdisk.synthetic.ms"),
        antennas(),
    );
    request.phase_center_rad = [1.23, -0.45];
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 25.0;
    request.integration_seconds = 10.0;
    request.spectral_setup = SyntheticSpectralSetup {
        name: "band1".to_string(),
        start_frequency_hz: 672.0e9,
        channel_width_hz: 2.0e6,
        channel_count: 4,
    };
    request
}

#[test]
fn generates_vla_ppdisk_synthetic_ms_skeleton() {
    let temp = tempfile::tempdir().unwrap();
    let request = request(temp.path());

    let report = generate_synthetic_observation_ms(&request).unwrap();
    assert_eq!(report.antenna_count, 3);
    assert_eq!(report.baseline_count, 3);
    assert_eq!(report.time_sample_count, 3);
    assert_eq!(report.main_row_count, 9);
    assert_eq!(report.channel_count, 4);
    assert!(report.nonzero_visibility_count > 0);
    assert!(report.applied_corruptions.is_empty());

    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    assert_eq!(ms.row_count(), 9);
    assert_eq!(ms.antenna().unwrap().row_count(), 3);
    assert_eq!(ms.antenna().unwrap().name(0).unwrap(), "VLA01");
    assert_eq!(ms.field().unwrap().name(0).unwrap(), "ppdisk");
    assert_eq!(ms.spectral_window().unwrap().num_chan(0).unwrap(), 4);
    let observation = ms.observation().unwrap();
    let telescope = observation
        .table()
        .cell_accessor(0, "TELESCOPE_NAME")
        .unwrap()
        .value()
        .unwrap();
    assert_eq!(
        telescope,
        Some(&Value::Scalar(ScalarValue::String("VLA".to_string())))
    );

    let data = ms
        .main_table()
        .cell_accessor(0, "DATA")
        .unwrap()
        .value()
        .unwrap();
    match data {
        Some(Value::Array(ArrayValue::Complex32(values))) => {
            assert_eq!(values.shape(), &[2, 4]);
            assert!(
                values
                    .iter()
                    .any(|value| value.re != 0.0 || value.im != 0.0)
            );
        }
        other => panic!("expected Complex32 DATA array, got {other:?}"),
    }

    let uvw = ms
        .main_table()
        .cell_accessor(0, "UVW")
        .unwrap()
        .value()
        .unwrap();
    match uvw {
        Some(Value::Array(ArrayValue::Float64(values))) => {
            assert_eq!(values.shape(), &[3]);
            assert!(values.iter().all(|value| value.is_finite()));
            assert!(values.iter().any(|value| value.abs() > 0.0));
        }
        other => panic!("expected Float64 UVW array, got {other:?}"),
    }
}

#[test]
fn analytic_component_model_generates_predicted_ms_data() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        temp.path().join("unused.fits"),
        temp.path().join("analytic.synthetic.ms"),
        antennas(),
    );
    request.model = Some(SyntheticSkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("inline-analytic".to_string()),
        components: vec![
            SyntheticAnalyticComponent::Point {
                name: Some("line-core".to_string()),
                l_rad: 0.0,
                m_rad: 0.0,
                spectrum: SyntheticAnalyticSpectrum {
                    flux_jy: 1.0,
                    spectral_index: 0.0,
                    reference_frequency_hz: None,
                    line_peak_jy: 3.0,
                    line_center_fraction: 0.0,
                    line_sigma_fraction: 0.08,
                    absorption_peak_jy: 0.0,
                    absorption_center_fraction: 0.5,
                    absorption_sigma_fraction: 0.1,
                },
            },
            SyntheticAnalyticComponent::Gaussian {
                name: Some("broad-extended".to_string()),
                l_rad: 4.0e-5,
                m_rad: -2.0e-5,
                major_fwhm_rad: 8.0e-5,
                minor_fwhm_rad: 4.0e-5,
                position_angle_rad: 0.3,
                spectrum: SyntheticAnalyticSpectrum {
                    flux_jy: 0.4,
                    spectral_index: -0.5,
                    reference_frequency_hz: None,
                    line_peak_jy: 0.0,
                    line_center_fraction: 0.5,
                    line_sigma_fraction: 0.1,
                    absorption_peak_jy: 0.0,
                    absorption_center_fraction: 0.5,
                    absorption_sigma_fraction: 0.1,
                },
            },
        ],
    });
    request.phase_center_rad = [1.23, -0.45];
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 10.0;
    request.integration_seconds = 10.0;
    request.spectral_setup = SyntheticSpectralSetup {
        name: "analytic-band".to_string(),
        start_frequency_hz: 44.0e9,
        channel_width_hz: 1.0e6,
        channel_count: 4,
    };

    let report = generate_synthetic_observation_ms(&request).unwrap();

    assert_eq!(report.model_kind, "analytic_components");
    assert!(report.nonzero_visibility_count > 0);
    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    let data = ms
        .main_table()
        .cell_accessor(0, "DATA")
        .unwrap()
        .value()
        .unwrap();
    match data {
        Some(Value::Array(ArrayValue::Complex32(values))) => {
            assert_eq!(values.shape(), &[2, 4]);
            assert_ne!(values[[0, 0]], values[[0, 3]]);
        }
        other => panic!("expected Complex32 DATA array, got {other:?}"),
    }
}

#[test]
fn total_power_mode_generates_autocorrelation_rows() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        temp.path().join("unused.fits"),
        temp.path().join("tp.synthetic.ms"),
        vec![SyntheticAntenna {
            name: "CM01".to_string(),
            station: "CM01".to_string(),
            position_m: [-2_223_990.194, -5_440_045.461, -2_481_682.086],
            dish_diameter_m: 7.0,
        }],
    );
    request.model = Some(SyntheticSkyModel::AnalyticComponents {
        path: None,
        schema_version: Some(1),
        name: Some("tp-point".to_string()),
        components: vec![SyntheticAnalyticComponent::Point {
            name: Some("center".to_string()),
            l_rad: 0.0,
            m_rad: 0.0,
            spectrum: SyntheticAnalyticSpectrum {
                flux_jy: 1.25,
                spectral_index: 0.0,
                reference_frequency_hz: None,
                line_peak_jy: 0.0,
                line_center_fraction: 0.5,
                line_sigma_fraction: 0.1,
                absorption_peak_jy: 0.0,
                absorption_center_fraction: 0.5,
                absorption_sigma_fraction: 0.1,
            },
        }],
    });
    request.telescope_name = "ACA".to_string();
    request.observation_mode = SyntheticObservationMode::TotalPower;
    request.polarization_setup =
        SyntheticPolarizationSetup::new(SyntheticPolarizationBasis::Linear, 2).unwrap();
    request.phase_center_rad = [0.011_028_274_098_247_087, -0.605_894_714_943_707_3];
    request.fields = vec![
        SyntheticField {
            name: "tp_0".to_string(),
            phase_center_rad: request.phase_center_rad,
        },
        SyntheticField {
            name: "tp_1".to_string(),
            phase_center_rad: [
                request.phase_center_rad[0] + 1.0e-5,
                request.phase_center_rad[1],
            ],
        },
    ];
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 30.0;
    request.integration_seconds = 10.0;
    request.allow_below_elevation_limit = true;
    request.elevation_limit_rad = (-89.0_f64).to_radians();
    request.spectral_setup = SyntheticSpectralSetup {
        name: "band3".to_string(),
        start_frequency_hz: 330.076e9,
        channel_width_hz: 50.0e6,
        channel_count: 1,
    };

    let report = generate_synthetic_observation_ms(&request).unwrap();

    assert_eq!(
        report.observation_mode,
        SyntheticObservationMode::TotalPower
    );
    assert_eq!(report.antenna_count, 1);
    assert_eq!(report.baseline_count, 1);
    assert_eq!(report.time_sample_count, 3);
    assert_eq!(report.main_row_count, 3);

    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    assert_eq!(ms.row_count(), 3);
    assert_eq!(ms.processor().unwrap().row_count(), 0);
    assert_eq!(ms.field().unwrap().row_count(), 2);
    assert_eq!(ms.pointing().unwrap().row_count(), 3);
    assert_eq!(
        ms.polarization().unwrap().corr_type(0).unwrap(),
        vec![9, 12]
    );
    assert_eq!(main_ids::antenna1(ms.main_table()).get(0).unwrap(), 0);
    assert_eq!(main_ids::antenna2(ms.main_table()).get(0).unwrap(), 0);
    assert_eq!(main_ids::field_id(ms.main_table()).get(0).unwrap(), 0);
    assert_eq!(main_ids::field_id(ms.main_table()).get(1).unwrap(), 1);
    assert_eq!(main_ids::field_id(ms.main_table()).get(2).unwrap(), 0);
    assert_eq!(main_ids::scan_number(ms.main_table()).get(0).unwrap(), 1);
    assert_eq!(main_ids::scan_number(ms.main_table()).get(1).unwrap(), 2);
    assert_eq!(main_ids::scan_number(ms.main_table()).get(2).unwrap(), 3);

    let data = ms
        .main_table()
        .cell_accessor(0, "DATA")
        .unwrap()
        .value()
        .unwrap();
    match data {
        Some(Value::Array(ArrayValue::Complex32(values))) => {
            assert_eq!(values.shape(), &[2, 1]);
            assert!(values.iter().all(|value| value.im.abs() < 1.0e-6));
            assert!(values.iter().all(|value| value.re > 0.0));
        }
        other => panic!("expected Complex32 DATA array, got {other:?}"),
    }

    let uvw = ms
        .main_table()
        .cell_accessor(0, "UVW")
        .unwrap()
        .value()
        .unwrap();
    match uvw {
        Some(Value::Array(ArrayValue::Float64(values))) => {
            assert_eq!(values.shape(), &[3]);
            assert!(values.iter().all(|value| *value == 0.0));
        }
        other => panic!("expected Float64 UVW array, got {other:?}"),
    }
}

#[test]
fn requested_four_correlations_drive_ms_metadata_and_main_shapes() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = request(temp.path());
    request.polarization_setup =
        SyntheticPolarizationSetup::new(SyntheticPolarizationBasis::Linear, 4).unwrap();

    let report = generate_synthetic_observation_ms(&request).unwrap();

    assert_eq!(report.correlation_count, 4);
    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    let polarization = ms.polarization().unwrap();
    assert_eq!(polarization.num_corr(0).unwrap(), 4);
    assert_eq!(polarization.corr_type(0).unwrap(), vec![9, 10, 11, 12]);

    let data = ms
        .main_table()
        .cell_accessor(0, "DATA")
        .unwrap()
        .value()
        .unwrap();
    match data {
        Some(Value::Array(ArrayValue::Complex32(values))) => {
            assert_eq!(values.shape(), &[4, 4]);
        }
        other => panic!("expected Complex32 DATA array, got {other:?}"),
    }
    let weight = ms
        .main_table()
        .cell_accessor(0, "WEIGHT")
        .unwrap()
        .value()
        .unwrap();
    match weight {
        Some(Value::Array(ArrayValue::Float32(values))) => {
            assert_eq!(values.shape(), &[4]);
        }
        other => panic!("expected Float32 WEIGHT array, got {other:?}"),
    }
}

#[test]
fn zenith_transit_schedule_writes_unflagged_vla_track() {
    let temp = tempfile::tempdir().unwrap();
    let model = temp.path().join("ppdisk672_GHz_50pc.fits");
    write_test_fits_model(&model, 16, 16);
    let measures = casa_measures_data::MeasuresRuntime::open_discovered(Default::default())
        .expect("test measures runtime");
    let vla = MPosition::from_observatory_name("VLA", &measures)
        .expect("catalog lookup")
        .expect("VLA position");
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        &model,
        temp.path().join("zenith.synthetic.ms"),
        tutorial_vla_a_antennas(),
    );
    request.phase_center_rad = [0.0, vla.latitude_rad()];
    request.start_time_mjd_seconds = 59_000.25 * 86_400.0;
    request.duration_seconds = 2.0 * 3_600.0;
    request.integration_seconds = 600.0;
    request.predict_model = false;
    request.corruption = None;

    let report = generate_synthetic_observation_ms(&request).unwrap();
    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    let flag_rows = ms.flag_row_column();
    let flags = ms.flag_column();
    let mut true_flag_rows = 0usize;
    let mut true_flags = 0usize;
    for row in 0..ms.row_count() {
        true_flag_rows += usize::from(flag_rows.get(row).unwrap());
        let ArrayValue::Bool(row_flags) = flags.get(row).unwrap() else {
            panic!("expected Bool FLAG row");
        };
        true_flags += row_flags.iter().filter(|flag| **flag).count();
    }

    assert_eq!(report.main_row_count, ms.row_count());
    assert_eq!(true_flag_rows, 0);
    assert_eq!(true_flags, 0);
}

#[test]
fn multi_field_synthetic_ms_cycles_field_ids_and_writes_pointings() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = request(temp.path());
    request.duration_seconds = 40.0;
    request.integration_seconds = 10.0;
    request.fields = vec![
        SyntheticField {
            name: "mosaic_0".to_string(),
            phase_center_rad: request.phase_center_rad,
        },
        SyntheticField {
            name: "mosaic_1".to_string(),
            phase_center_rad: [
                request.phase_center_rad[0] + 10.0_f64.to_radians() / 3600.0,
                request.phase_center_rad[1],
            ],
        },
    ];

    let report = generate_synthetic_observation_ms(&request).unwrap();
    assert_eq!(report.main_row_count, 12);

    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    assert_eq!(ms.field().unwrap().row_count(), 2);
    assert_eq!(ms.field().unwrap().name(0).unwrap(), "mosaic_0");
    assert_eq!(ms.field().unwrap().name(1).unwrap(), "mosaic_1");
    assert_eq!(ms.pointing().unwrap().row_count(), 12);

    assert_eq!(main_i32_cell(&ms, 0, "FIELD_ID"), 0);
    assert_eq!(main_i32_cell(&ms, 3, "FIELD_ID"), 1);
    assert_eq!(main_i32_cell(&ms, 6, "FIELD_ID"), 0);
    assert_eq!(main_i32_cell(&ms, 9, "FIELD_ID"), 1);
}

#[test]
fn tutorial_vla_a_configuration_matches_casa_guide_shape() {
    let antennas = tutorial_vla_a_antennas();
    assert_eq!(antennas.len(), 27);
    assert_eq!(antennas[0].name, "W08");
    assert_eq!(antennas[0].station, "W08");
    assert_eq!(
        antennas[0].position_m,
        [-1_601_614.061201, -5_042_001.676547, 3_554_652.455603]
    );
    assert_eq!(antennas[26].name, "N72");
    assert_eq!(antennas.len() * (antennas.len() - 1) / 2, 351);
}

#[test]
fn seeded_noise_and_gain_phase_corruptions_are_deterministic() {
    let temp = tempfile::tempdir().unwrap();
    let mut first = request(&temp.path().join("first"));
    std::fs::create_dir_all(temp.path().join("first")).unwrap();
    first.corruption = Some(SyntheticCorruptionConfig {
        seed: 42,
        noise: Some(SyntheticNoiseCorruption {
            mode: SyntheticNoiseMode::SimpleNoise,
            simplenoise_jy: 0.001,
        }),
        gain: Some(SyntheticGainCorruption {
            mode: SyntheticGainMode::Fbm,
            interval_seconds: 10.0,
            amplitude: [0.05, 0.02],
        }),
        bandpass: None,
        leakage: None,
        pointing: None,
    });
    let mut second = first.clone();
    second.output_ms = temp.path().join("second").join("ppdisk.synthetic.ms");
    second.model_image = temp.path().join("second").join("ppdisk672_GHz_50pc.fits");
    std::fs::create_dir_all(temp.path().join("second")).unwrap();
    write_test_fits_model(&second.model_image, 16, 16);

    let first_report = generate_synthetic_observation_ms(&first).unwrap();
    let second_report = generate_synthetic_observation_ms(&second).unwrap();
    assert_eq!(
        first_report.applied_corruptions,
        vec!["noise".to_string(), "gain".to_string()]
    );
    assert_eq!(
        second_report.applied_corruptions,
        first_report.applied_corruptions
    );

    let first_data = first_row_data(&first.output_ms);
    let second_data = first_row_data(&second.output_ms);
    assert_eq!(first_data, second_data);

    let uncorrupted_root = temp.path().join("uncorrupted");
    std::fs::create_dir_all(&uncorrupted_root).unwrap();
    let uncorrupted = request(&uncorrupted_root);
    generate_synthetic_observation_ms(&uncorrupted).unwrap();
    assert_ne!(first_data, first_row_data(&uncorrupted.output_ms));
}

#[test]
fn gain_phase_corruption_varies_by_time_sample() {
    let temp = tempfile::tempdir().unwrap();
    let mut corrupted = request(&temp.path().join("gain-time"));
    std::fs::create_dir_all(temp.path().join("gain-time")).unwrap();
    corrupted.corruption = Some(SyntheticCorruptionConfig {
        seed: 42,
        noise: None,
        gain: Some(SyntheticGainCorruption {
            mode: SyntheticGainMode::Fbm,
            interval_seconds: 10.0,
            amplitude: [0.05, 0.02],
        }),
        bandpass: None,
        leakage: None,
        pointing: None,
    });
    generate_synthetic_observation_ms(&corrupted).unwrap();

    let clean_root = temp.path().join("gain-time-clean");
    std::fs::create_dir_all(&clean_root).unwrap();
    let clean = request(&clean_root);
    generate_synthetic_observation_ms(&clean).unwrap();

    let first_ratio = complex_ratio(
        row_data(&corrupted.output_ms, 0)[0],
        row_data(&clean.output_ms, 0)[0],
    );
    let second_sample_ratio = complex_ratio(
        row_data(&corrupted.output_ms, 3)[0],
        row_data(&clean.output_ms, 3)[0],
    );
    assert_ne!(first_ratio, second_sample_ratio);
}

#[test]
fn common_bandpass_and_leakage_corruptions_are_reported_and_change_data() {
    let temp = tempfile::tempdir().unwrap();
    let mut corrupted = request(&temp.path().join("corrupted"));
    std::fs::create_dir_all(temp.path().join("corrupted")).unwrap();
    corrupted.spectral_setup.channel_count = 4;
    corrupted.corruption = Some(SyntheticCorruptionConfig {
        seed: 99,
        noise: None,
        gain: None,
        bandpass: Some(SyntheticBandpassCorruption {
            mode: SyntheticBandpassMode::Calculate,
            interval_seconds: 3600.0,
            amplitude: [0.08, 0.04],
        }),
        leakage: Some(SyntheticPolarizationLeakageCorruption {
            mode: SyntheticPolarizationLeakageMode::Constant,
            amplitude: [0.1, 0.0],
            offset: [0.0, 0.0],
        }),
        pointing: None,
    });
    let report = generate_synthetic_observation_ms(&corrupted).unwrap();
    assert_eq!(
        report.applied_corruptions,
        vec!["bandpass".to_string(), "leakage".to_string()]
    );

    let uncorrupted_root = temp.path().join("uncorrupted-common");
    std::fs::create_dir_all(&uncorrupted_root).unwrap();
    let mut uncorrupted = request(&uncorrupted_root);
    uncorrupted.spectral_setup.channel_count = 4;
    generate_synthetic_observation_ms(&uncorrupted).unwrap();

    let corrupted_data = first_row_data(&corrupted.output_ms);
    let uncorrupted_data = first_row_data(&uncorrupted.output_ms);
    assert_ne!(corrupted_data, uncorrupted_data);
    assert_ne!(corrupted_data[0], corrupted_data[1]);
}

#[test]
fn pointing_corruption_shifts_primary_beam_prediction_when_model_has_direction() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("pointing");
    std::fs::create_dir_all(&root).unwrap();
    let model = root.join("pointing.fits");
    let mut base =
        SyntheticObservationRequest::vla_ppdisk(&model, root.join("base.ms"), antennas());
    base.phase_center_rad = [1.23, -0.45];
    base.duration_seconds = 10.0;
    base.integration_seconds = 10.0;
    write_test_fits_model_with_center(&model, 16, 16, base.phase_center_rad);
    generate_synthetic_observation_ms(&base).unwrap();

    let mut shifted = base.clone();
    shifted.output_ms = root.join("shifted.ms");
    shifted.corruption = Some(SyntheticCorruptionConfig {
        seed: 5,
        noise: None,
        gain: None,
        bandpass: None,
        leakage: None,
        pointing: Some(SyntheticPointingCorruption {
            epjtablename: None,
            apply_pointing_offsets: true,
            do_pb_correction: false,
            offset_rad: [10.0_f64.to_radians() / 3600.0, 0.0],
        }),
    });
    let report = generate_synthetic_observation_ms(&shifted).unwrap();
    assert_eq!(report.applied_corruptions, vec!["pointing".to_string()]);
    assert_ne!(
        first_row_data(&base.output_ms),
        first_row_data(&shifted.output_ms)
    );
}

#[test]
fn invalid_corruption_parameters_fail_clearly() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = request(temp.path());
    request.corruption = Some(SyntheticCorruptionConfig {
        seed: 7,
        noise: Some(SyntheticNoiseCorruption {
            mode: SyntheticNoiseMode::SimpleNoise,
            simplenoise_jy: -1.0,
        }),
        gain: None,
        bandpass: None,
        leakage: None,
        pointing: None,
    });

    let error = generate_synthetic_observation_ms(&request)
        .unwrap_err()
        .to_string();
    assert!(error.contains("simplenoise_jy"));
    assert!(error.contains("non-negative"));
}

#[test]
fn missing_model_image_fails_clearly() {
    let temp = tempfile::tempdir().unwrap();
    let request = SyntheticObservationRequest::vla_ppdisk(
        temp.path().join("missing.fits"),
        temp.path().join("missing-model.ms"),
        antennas(),
    );

    let error = generate_synthetic_observation_ms(&request)
        .unwrap_err()
        .to_string();
    assert!(error.contains("model image"));
    assert!(error.contains("does not exist"));
}

#[test]
fn invalid_antenna_configuration_fails_clearly() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = request(temp.path());
    request.antennas.truncate(1);

    let error = generate_synthetic_observation_ms(&request)
        .unwrap_err()
        .to_string();
    assert!(error.contains("at least 2 antennas"));
    assert!(error.contains("interferometric"));
}

#[test]
fn unsupported_spectral_setup_fails_clearly() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = request(temp.path());
    request.spectral_setup.channel_count = 0;

    let error = generate_synthetic_observation_ms(&request)
        .unwrap_err()
        .to_string();
    assert!(error.contains("at least one channel"));
}

#[test]
fn tutorial_ppdisk_fits_model_generates_predicted_visibilities_when_available() {
    let Some(model) =
        tutorial_dataset_path("simulation/vla-ppdisk/model-fits").filter(|path| path.exists())
    else {
        eprintln!("skipping tutorial ppdisk simulation test: model FITS not staged");
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        &model,
        temp.path().join("ppdisk-tutorial.synthetic.ms"),
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = 10.0;
    request.integration_seconds = 2.0;
    request.spectral_setup.channel_count = 1;
    request.spectral_setup.start_frequency_hz = 44.0e9;
    request.spectral_setup.channel_width_hz = 128.0e6;

    let report = generate_synthetic_observation_ms(&request).unwrap();
    assert_eq!(report.antenna_count, 27);
    assert_eq!(report.baseline_count, 351);
    assert_eq!(report.time_sample_count, 5);
    assert_eq!(report.main_row_count, 1_755);
    assert!(
        report.nonzero_visibility_count > 0,
        "tutorial FITS model should predict non-zero visibility samples"
    );

    let ms = MeasurementSet::open(&request.output_ms).unwrap();
    assert!(ms.validate().unwrap().is_empty());
    let observation = ms.observation().unwrap();
    let telescope = observation
        .table()
        .cell_accessor(0, "TELESCOPE_NAME")
        .unwrap()
        .value()
        .unwrap();
    assert_eq!(
        telescope,
        Some(&Value::Scalar(ScalarValue::String("VLA".to_string())))
    );
}

#[test]
fn casa_can_open_generated_synthetic_ms_when_available() {
    let Some(casa) = discover_casa_python() else {
        eprintln!("skipping CASA synthetic MS read test: CASA Python not available");
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let request = request(temp.path());
    let report = generate_synthetic_observation_ms(&request).unwrap();

    let script = r#"
import json
import sys
from casatools import table

path = sys.argv[1]
tb = table()
tb.open(path)
try:
    data = tb.getcell("DATA", 0)
    result = {
        "rows": tb.nrows(),
        "nonzero": int((abs(data) > 0).sum()),
    }
finally:
    tb.close()
print(json.dumps(result, sort_keys=True))
"#;
    let output = Command::new(&casa.program)
        .arg("-c")
        .arg(script)
        .arg(&request.output_ms)
        .output()
        .expect("run CASA Python table reader");
    assert!(
        output.status.success(),
        "CASA table read failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!("\"rows\": {}", report.main_row_count)));
    assert!(stdout.contains("\"nonzero\": "));
    assert!(!stdout.contains("\"nonzero\": 0"));
}

fn write_test_fits_model(path: &std::path::Path, nx: usize, ny: usize) {
    write_test_fits_model_inner(path, nx, ny, None);
}

fn write_test_fits_model_with_center(
    path: &std::path::Path,
    nx: usize,
    ny: usize,
    center_rad: [f64; 2],
) {
    write_test_fits_model_inner(path, nx, ny, Some(center_rad));
}

fn write_test_fits_model_inner(
    path: &std::path::Path,
    nx: usize,
    ny: usize,
    center_rad: Option<[f64; 2]>,
) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut cards = vec![
        fits_card("SIMPLE", "T"),
        fits_card("BITPIX", "-32"),
        fits_card("NAXIS", "2"),
        fits_card("NAXIS1", &nx.to_string()),
        fits_card("NAXIS2", &ny.to_string()),
        fits_card("BSCALE", "1.0"),
        fits_card("BZERO", "0.0"),
        fits_card("CTYPE1", "'RA---SIN'"),
        fits_card("CDELT1", "1.0E-6"),
        fits_card("CUNIT1", "'rad'"),
        fits_card("CTYPE2", "'DEC--SIN'"),
        fits_card("CDELT2", "1.0E-6"),
        fits_card("CUNIT2", "'rad'"),
    ];
    if let Some(center_rad) = center_rad {
        cards.push(fits_card("CRVAL1", &format!("{:.16E}", center_rad[0])));
        cards.push(fits_card("CRVAL2", &format!("{:.16E}", center_rad[1])));
        cards.push(fits_card("CRPIX1", &(0.5 * nx as f64 + 1.0).to_string()));
        cards.push(fits_card("CRPIX2", &(0.5 * ny as f64 + 1.0).to_string()));
    }
    cards.push("END".to_string());
    let mut bytes = Vec::new();
    for card in cards.drain(..) {
        let mut padded = format!("{card:<80}").into_bytes();
        padded.truncate(80);
        bytes.extend(padded);
    }
    while bytes.len() % 2880 != 0 {
        bytes.push(b' ');
    }
    for y in 0..ny {
        for x in 0..nx {
            let value = if x == nx / 2 && y == ny / 2 {
                1.0f32
            } else {
                0.0
            };
            bytes.extend(value.to_bits().to_be_bytes());
        }
    }
    while bytes.len() % 2880 != 0 {
        bytes.push(0);
    }
    std::fs::write(path, bytes).unwrap();
}

fn first_row_data(path: &std::path::Path) -> Vec<(f32, f32)> {
    row_data(path, 0)
}

fn row_data(path: &std::path::Path, row: usize) -> Vec<(f32, f32)> {
    let ms = MeasurementSet::open(path).unwrap();
    let data = ms
        .main_table()
        .cell_accessor(row, "DATA")
        .unwrap()
        .value()
        .unwrap();
    match data {
        Some(Value::Array(ArrayValue::Complex32(values))) => values
            .iter()
            .map(|value| (value.re, value.im))
            .collect::<Vec<_>>(),
        other => panic!("expected Complex32 DATA array, got {other:?}"),
    }
}

fn main_i32_cell(ms: &MeasurementSet, row: usize, column: &str) -> i32 {
    let value = ms
        .main_table()
        .cell_accessor(row, column)
        .unwrap()
        .value()
        .unwrap();
    match value {
        Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
        other => panic!("expected Int32 {column} cell, got {other:?}"),
    }
}

fn complex_ratio(numerator: (f32, f32), denominator: (f32, f32)) -> (i32, i32) {
    let denom = denominator.0 * denominator.0 + denominator.1 * denominator.1;
    assert!(denom > 0.0);
    let real = (numerator.0 * denominator.0 + numerator.1 * denominator.1) / denom;
    let imag = (numerator.1 * denominator.0 - numerator.0 * denominator.1) / denom;
    ((real * 1.0e6).round() as i32, (imag * 1.0e6).round() as i32)
}

fn fits_card(key: &str, value: &str) -> String {
    format!("{key:<8}= {value:>20}")
}
