// SPDX-License-Identifier: LGPL-3.0-or-later

use super::{
    ListObsPlotKind, ListObsPlotPayload, ListObsPlotSpec, build_listobs_plot_payload_from_summary,
    build_listobs_uv_plot_payload, parse_on_off,
};
use casa_ms::{
    AntennaSummary, DataDescriptionSummary, FieldSummary, MeasurementSetInfo,
    MeasurementSetScanSummary, ObservationSummary, PolarizationSummary, SourceSummary,
    SpectralWindowSummary,
};
use casa_ms::{ListObsOptions, ListObsSummary, ListObsUvCoverage};
use casa_ms::{MeasurementSetUvPoint, MeasurementSetUvTrack};

#[test]
fn default_specs_can_round_trip_cli_assignments() {
    for kind in ListObsPlotKind::ALL {
        let spec = kind.default_spec();
        let rebuilt = ListObsPlotSpec::from_cli_assignments(kind, &spec.cli_assignments()).unwrap();
        assert_eq!(rebuilt, spec);
    }
}

#[test]
fn plot_kinds_parse_aliases_and_errors() {
    let aliases = [
        ("uv_coverage", ListObsPlotKind::UvCoverage),
        ("uv", ListObsPlotKind::UvCoverage),
        ("antenna_layout", ListObsPlotKind::AntennaLayout),
        ("antennas", ListObsPlotKind::AntennaLayout),
        ("scan_timeline", ListObsPlotKind::ScanTimeline),
        ("scans", ListObsPlotKind::ScanTimeline),
        (
            "spectral_window_coverage",
            ListObsPlotKind::SpectralWindowCoverage,
        ),
        ("spw_coverage", ListObsPlotKind::SpectralWindowCoverage),
        ("spws", ListObsPlotKind::SpectralWindowCoverage),
    ];
    for (value, expected) in aliases {
        assert_eq!(ListObsPlotKind::parse(value).unwrap(), expected);
        assert_eq!(expected.to_string(), expected.as_str());
        assert!(!expected.display_name().is_empty());
    }

    let error = ListObsPlotKind::parse("bogus").unwrap_err();
    assert!(error.contains("unsupported plot kind"));
}

#[test]
fn parse_on_off_accepts_boolean_tokens() {
    assert!(parse_on_off("on", "labels").unwrap());
    assert!(parse_on_off("true", "labels").unwrap());
    assert!(!parse_on_off("off", "labels").unwrap());
    assert!(!parse_on_off("false", "labels").unwrap());
    let error = parse_on_off("maybe", "labels").unwrap_err();
    assert!(error.contains("expected on/off"));
}

#[test]
fn plot_specs_validate_options_and_preserve_assignments() {
    let mut spec = ListObsPlotSpec::new(ListObsPlotKind::AntennaLayout);
    spec.set_option("labels", "id").unwrap();
    spec.set_option("coordinates", "absolute").unwrap();
    spec.set_option("size_by_diameter", "false").unwrap();
    assert_eq!(spec.option("labels"), Some("id"));
    assert_eq!(spec.option("coordinates"), Some("absolute"));
    assert_eq!(spec.option("size_by_diameter"), Some("false"));
    assert_eq!(
        spec.cli_assignments(),
        vec![
            "coordinates=absolute".to_string(),
            "labels=id".to_string(),
            "size_by_diameter=false".to_string(),
        ]
    );

    let error = spec.set_option("labels", "bogus").unwrap_err();
    assert!(error.contains("unsupported option"));

    let error = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::ScanTimeline,
        &["missing-separator".to_string()],
    )
    .unwrap_err();
    assert!(error.contains("expected key=value"));

    let error = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::SpectralWindowCoverage,
        &["labels=maybe".to_string()],
    )
    .unwrap_err();
    assert!(error.contains("expected on/off"));
}

#[test]
fn metadata_plot_payloads_build_for_each_metadata_kind() {
    let summary = synthetic_summary();
    for kind in [
        ListObsPlotKind::AntennaLayout,
        ListObsPlotKind::ScanTimeline,
        ListObsPlotKind::SpectralWindowCoverage,
    ] {
        let spec = kind.default_spec();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        assert_eq!(payload.kind(), kind);
    }
}

#[test]
fn uv_plot_payload_defaults_to_mirrored_points() {
    let coverage = synthetic_uv_coverage();
    let spec = ListObsPlotKind::UvCoverage.default_spec();
    let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
    let ListObsPlotPayload::UvCoverage(uv_payload) = &payload else {
        panic!("expected uv payload");
    };
    assert!(uv_payload.draw_points);
    assert!(uv_payload.mirror);
    assert_eq!(uv_payload.tracks.len(), coverage.tracks.len());
}

#[test]
fn uv_points_mode_and_single_sample_tracks_build() {
    let mut coverage = synthetic_uv_coverage();
    coverage.tracks[0].samples.truncate(1);
    let spec = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::UvCoverage,
        &["draw_mode=points".to_string(), "mirror=off".to_string()],
    )
    .unwrap();
    let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
    let ListObsPlotPayload::UvCoverage(payload) = payload else {
        panic!("expected uv payload");
    };
    assert!(payload.draw_points);
    assert!(!payload.mirror);
    assert_eq!(payload.tracks[0].points.len(), 1);
}

#[test]
fn uv_tracks_mode_remains_explicitly_available() {
    let coverage = synthetic_uv_coverage();
    let spec = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::UvCoverage,
        &["draw_mode=tracks".to_string()],
    )
    .unwrap();
    let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
    let ListObsPlotPayload::UvCoverage(payload) = payload else {
        panic!("expected uv payload");
    };
    assert!(!payload.draw_points);
}

#[test]
fn uv_payload_builder_covers_runtime_validation_paths() {
    let coverage = synthetic_uv_coverage();

    let error = build_listobs_plot_payload_from_summary(
        &synthetic_summary(),
        &ListObsPlotKind::UvCoverage.default_spec(),
    )
    .unwrap_err();
    assert!(error.contains("build_listobs_uv_plot_payload"));

    let error =
        build_listobs_uv_plot_payload(&coverage, &ListObsPlotKind::AntennaLayout.default_spec())
            .unwrap_err();
    assert!(error.contains("does not match UV coverage payload builder"));

    let mut spec = ListObsPlotKind::UvCoverage.default_spec();
    spec.options
        .insert("axis_extent".to_string(), "42.5".to_string());
    let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
    assert_eq!(payload.kind(), ListObsPlotKind::UvCoverage);
    let ListObsPlotPayload::UvCoverage(payload) = payload else {
        panic!("expected uv payload");
    };
    assert_eq!(payload.axis_extent_lambda, 42.5);

    let mut invalid_axis = ListObsPlotKind::UvCoverage.default_spec();
    invalid_axis
        .options
        .insert("axis_extent".to_string(), "oops".to_string());
    let error = build_listobs_uv_plot_payload(&coverage, &invalid_axis).unwrap_err();
    assert!(error.contains("invalid axis_extent"));

    let mut invalid_draw_mode = ListObsPlotKind::UvCoverage.default_spec();
    invalid_draw_mode
        .options
        .insert("draw_mode".to_string(), "bogus".to_string());
    let error = build_listobs_uv_plot_payload(&coverage, &invalid_draw_mode).unwrap_err();
    assert!(error.contains("invalid draw_mode"));
}

#[test]
fn antenna_layout_rejects_missing_coordinate_metadata() {
    let mut summary = synthetic_summary();
    for antenna in &mut summary.antennas {
        antenna.offset_from_observatory_m = [f64::NAN, f64::NAN, f64::NAN];
        antenna.position_m = [f64::NAN, f64::NAN, f64::NAN];
    }
    let spec = ListObsPlotKind::AntennaLayout.default_spec();
    let error = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap_err();
    assert!(error.contains("finite offset coordinates"));
}

#[test]
fn antenna_layout_falls_back_to_absolute_coordinates_when_offsets_are_all_zero() {
    let mut summary = synthetic_summary();
    for antenna in &mut summary.antennas {
        antenna.offset_from_observatory_m = [0.0, 0.0, 0.0];
    }
    let spec = ListObsPlotKind::AntennaLayout.default_spec();
    let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
    let ListObsPlotPayload::AntennaLayout(payload) = payload else {
        panic!("expected antenna layout payload");
    };
    assert_eq!(payload.x_label, "ITRF X (m)");
    assert_eq!(payload.y_label, "ITRF Y (m)");
    assert!(payload.summary.contains("using ITRF X/Y"));
    assert!(payload.antennas.iter().any(|point| point.x.abs() > 1.0));
}

#[test]
fn antenna_layout_supports_id_labels_absolute_coordinates_and_fixed_marker_sizes() {
    let summary = synthetic_summary();
    let spec = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::AntennaLayout,
        &[
            "labels=id".to_string(),
            "coordinates=absolute".to_string(),
            "size_by_diameter=off".to_string(),
        ],
    )
    .unwrap();
    let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
    let ListObsPlotPayload::AntennaLayout(payload) = payload else {
        panic!("expected antenna layout payload");
    };
    assert_eq!(payload.x_label, "ITRF X (m)");
    assert_eq!(payload.y_label, "ITRF Y (m)");
    assert!(payload.labels_enabled);
    assert!(
        payload
            .antennas
            .iter()
            .all(|point| point.marker_radius == 5 && point.label.parse::<usize>().is_ok())
    );
}

#[test]
fn spectral_window_plot_uses_true_band_edges_for_descending_windows() {
    let mut summary = synthetic_summary();
    summary.spectral_windows = vec![SpectralWindowSummary {
        spectral_window_id: 7,
        name: "DESC".to_string(),
        num_channels: 4,
        frame: Some("TOPO".to_string()),
        first_channel_frequency_hz: 10.0e9,
        channel_width_hz: -1.0e9,
        reference_frequency_hz: 9.0e9,
        center_frequency_hz: 8.5e9,
        min_frequency_hz: 7.0e9,
        max_frequency_hz: 10.5e9,
        total_bandwidth_hz: 4.0e9,
        data_description_ids: vec![0],
        polarization_ids: vec![0],
        correlation_types: vec!["XX".to_string()],
    }];

    let spec = ListObsPlotKind::SpectralWindowCoverage.default_spec();
    let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
    let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
        panic!("expected spectral window payload");
    };
    assert_eq!(payload.bars.len(), 1);
    let bar = &payload.bars[0];
    assert_eq!(bar.spectral_window_id, 7);
    assert_eq!(bar.start, 7.0);
    assert_eq!(bar.end, 10.5);
    assert!(bar.start < summary.spectral_windows[0].center_frequency_hz / 1.0e9);
    assert!(bar.end > summary.spectral_windows[0].center_frequency_hz / 1.0e9);
}

#[test]
fn spectral_window_lane_labels_use_real_spw_ids() {
    let mut summary = synthetic_summary();
    summary.spectral_windows = vec![
        SpectralWindowSummary {
            spectral_window_id: 1,
            name: "LOW".to_string(),
            num_channels: 16,
            frame: Some("TOPO".to_string()),
            first_channel_frequency_hz: 1.0e9,
            channel_width_hz: 1.0e6,
            reference_frequency_hz: 1.0e9,
            center_frequency_hz: 1.008e9,
            min_frequency_hz: 0.9995e9,
            max_frequency_hz: 1.0165e9,
            total_bandwidth_hz: 16.0e6,
            data_description_ids: vec![0],
            polarization_ids: vec![0],
            correlation_types: vec!["XX".to_string(), "YY".to_string()],
        },
        SpectralWindowSummary {
            spectral_window_id: 2,
            name: "HIGH".to_string(),
            num_channels: 32,
            frame: Some("TOPO".to_string()),
            first_channel_frequency_hz: 1.2e9,
            channel_width_hz: 5.0e5,
            reference_frequency_hz: 1.2e9,
            center_frequency_hz: 1.208e9,
            min_frequency_hz: 1.19975e9,
            max_frequency_hz: 1.21625e9,
            total_bandwidth_hz: 16.0e6,
            data_description_ids: vec![1],
            polarization_ids: vec![0],
            correlation_types: vec!["XX".to_string(), "YY".to_string()],
        },
    ];

    let spec = ListObsPlotKind::SpectralWindowCoverage.default_spec();
    let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
    let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
        panic!("expected spectral window payload");
    };

    let ids = payload
        .bars
        .iter()
        .map(|bar| bar.spectral_window_id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn spectral_window_polarization_color_group_uses_setup_ids() {
    let mut summary = synthetic_summary();
    summary.polarization_setups = vec![
        PolarizationSummary {
            polarization_id: 0,
            num_correlations: 2,
            correlation_types: vec!["XX".to_string(), "YY".to_string()],
        },
        PolarizationSummary {
            polarization_id: 1,
            num_correlations: 4,
            correlation_types: vec![
                "XX".to_string(),
                "YY".to_string(),
                "XY".to_string(),
                "YX".to_string(),
            ],
        },
    ];
    summary.data_descriptions = vec![
        DataDescriptionSummary {
            data_description_id: 0,
            spectral_window_id: 0,
            polarization_id: 0,
            flagged: false,
        },
        DataDescriptionSummary {
            data_description_id: 1,
            spectral_window_id: 1,
            polarization_id: 1,
            flagged: false,
        },
    ];
    summary.spectral_windows = vec![
        SpectralWindowSummary {
            spectral_window_id: 0,
            name: "LOW".to_string(),
            num_channels: 16,
            frame: Some("TOPO".to_string()),
            first_channel_frequency_hz: 1.0e9,
            channel_width_hz: 1.0e6,
            reference_frequency_hz: 1.0e9,
            center_frequency_hz: 1.008e9,
            min_frequency_hz: 0.9995e9,
            max_frequency_hz: 1.0165e9,
            total_bandwidth_hz: 16.0e6,
            data_description_ids: vec![0],
            polarization_ids: vec![0],
            correlation_types: vec!["XX".to_string(), "YY".to_string()],
        },
        SpectralWindowSummary {
            spectral_window_id: 1,
            name: "HIGH".to_string(),
            num_channels: 32,
            frame: Some("TOPO".to_string()),
            first_channel_frequency_hz: 1.2e9,
            channel_width_hz: 5.0e5,
            reference_frequency_hz: 1.2e9,
            center_frequency_hz: 1.208e9,
            min_frequency_hz: 1.19975e9,
            max_frequency_hz: 1.21625e9,
            total_bandwidth_hz: 16.0e6,
            data_description_ids: vec![1],
            polarization_ids: vec![1],
            correlation_types: vec![
                "XX".to_string(),
                "YY".to_string(),
                "XY".to_string(),
                "YX".to_string(),
            ],
        },
    ];

    let spec = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::SpectralWindowCoverage,
        &["color_by=polarization".to_string()],
    )
    .unwrap();
    let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
    let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
        panic!("expected spectral window payload");
    };

    let color_groups = payload
        .bars
        .iter()
        .map(|bar| bar.color_group.clone())
        .collect::<Vec<_>>();
    assert_eq!(color_groups, vec!["pol-0".to_string(), "pol-1".to_string()]);
}

#[test]
fn spectral_window_plot_supports_mhz_units_and_hidden_labels() {
    let summary = synthetic_summary();
    let spec = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::SpectralWindowCoverage,
        &["unit=mhz".to_string(), "labels=off".to_string()],
    )
    .unwrap();
    let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
    let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
        panic!("expected spectral window payload");
    };
    assert_eq!(payload.x_label, "Frequency (MHz)");
    assert!(payload.bars.iter().all(|bar| bar.label.is_empty()));
    assert!(payload.bars.iter().all(|bar| bar.start > 900.0));
}

#[test]
fn scan_timeline_plot_supports_field_lanes_and_hidden_labels() {
    let summary = synthetic_summary();
    let spec = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::ScanTimeline,
        &[
            "lanes=field".to_string(),
            "color_by=intent".to_string(),
            "labels=none".to_string(),
        ],
    )
    .unwrap();
    let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
    let ListObsPlotPayload::ScanTimeline(payload) = payload else {
        panic!("expected scan timeline payload");
    };
    assert_eq!(
        payload.lane_labels,
        vec!["FIELD_A".to_string(), "FIELD_B".to_string()]
    );
    assert!(
        payload
            .bars
            .iter()
            .all(|bar| bar.label.is_empty() && bar.color_group.contains("ON_SOURCE"))
    );
}

#[test]
fn scan_timeline_and_spectral_payloads_handle_empty_and_fallback_data() {
    let mut empty_summary = synthetic_summary();
    empty_summary.scans.clear();
    empty_summary.spectral_windows = vec![SpectralWindowSummary {
        spectral_window_id: 9,
        name: String::new(),
        num_channels: 8,
        frame: Some("TOPO".to_string()),
        first_channel_frequency_hz: 1.4e9,
        channel_width_hz: 1.0e6,
        reference_frequency_hz: 1.4e9,
        center_frequency_hz: 1.404e9,
        min_frequency_hz: 1.3995e9,
        max_frequency_hz: 1.4085e9,
        total_bandwidth_hz: 8.0e6,
        data_description_ids: vec![0],
        polarization_ids: vec![],
        correlation_types: vec!["XX".to_string()],
    }];

    let timeline = build_listobs_plot_payload_from_summary(
        &empty_summary,
        &ListObsPlotKind::ScanTimeline.default_spec(),
    )
    .unwrap();
    let ListObsPlotPayload::ScanTimeline(timeline) = timeline else {
        panic!("expected scan timeline payload");
    };
    assert!(timeline.bars.is_empty());
    assert!(timeline.lane_labels.is_empty());
    assert_eq!(timeline.start_mjd_seconds, 0.0);
    assert_eq!(timeline.end_mjd_seconds, 1.0);

    let spec = ListObsPlotSpec::from_cli_assignments(
        ListObsPlotKind::SpectralWindowCoverage,
        &["color_by=polarization".to_string()],
    )
    .unwrap();
    let payload = build_listobs_plot_payload_from_summary(&empty_summary, &spec).unwrap();
    let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
        panic!("expected spectral window payload");
    };
    assert_eq!(payload.bars.len(), 1);
    assert_eq!(payload.bars[0].label, "SPW 9");
    assert_eq!(payload.bars[0].color_group, "pol-none");
}

fn synthetic_summary() -> ListObsSummary {
    ListObsSummary {
        schema_version: 1,
        options: ListObsOptions::default(),
        measurement_set: MeasurementSetInfo {
            path: Some("fixture.ms".to_string()),
            ms_version: Some(2.0),
            row_count: 4,
            observation_count: 1,
            field_count: 2,
            spectral_window_count: 2,
            polarization_count: 1,
            data_description_count: 2,
            source_count: 1,
            antenna_count: 3,
            antenna_table_count: 3,
            time_reference: Some("UTC".to_string()),
            start_mjd_seconds: Some(100.0),
            end_mjd_seconds: Some(180.0),
            total_elapsed_seconds: Some(80.0),
        },
        observations: vec![ObservationSummary {
            observation_id: 0,
            telescope_name: "VLA".to_string(),
            observer: "Observer".to_string(),
            project: "Project".to_string(),
            release_date_mjd_seconds: 0.0,
            start_mjd_seconds: Some(100.0),
            end_mjd_seconds: Some(180.0),
        }],
        scans: vec![
            MeasurementSetScanSummary {
                observation_id: 0,
                array_id: 0,
                scan_number: 1,
                row_count: 2,
                field_id: 0,
                field_name: "FIELD_A".to_string(),
                field_ids: vec![0],
                field_names: vec!["FIELD_A".to_string()],
                data_description_ids: vec![0],
                spectral_window_ids: vec![0],
                state_ids: vec![0],
                scan_intents: vec!["CALIBRATE_PHASE.ON_SOURCE".to_string()],
                start_mjd_seconds: 100.0,
                end_mjd_seconds: 130.0,
                mean_interval_seconds: 15.0,
                mean_interval_seconds_by_spw: vec![15.0],
                unflagged_row_count: None,
            },
            MeasurementSetScanSummary {
                observation_id: 0,
                array_id: 0,
                scan_number: 2,
                row_count: 2,
                field_id: 1,
                field_name: "FIELD_B".to_string(),
                field_ids: vec![1],
                field_names: vec!["FIELD_B".to_string()],
                data_description_ids: vec![1],
                spectral_window_ids: vec![1],
                state_ids: vec![1],
                scan_intents: vec!["OBSERVE_TARGET.ON_SOURCE".to_string()],
                start_mjd_seconds: 140.0,
                end_mjd_seconds: 180.0,
                mean_interval_seconds: 20.0,
                mean_interval_seconds_by_spw: vec![20.0],
                unflagged_row_count: None,
            },
        ],
        fields: vec![
            FieldSummary {
                field_id: 0,
                name: "FIELD_A".to_string(),
                code: "".to_string(),
                source_id: 0,
                row_count: 2,
                unflagged_row_count: None,
                time_mjd_seconds: 100.0,
                direction_reference: Some("J2000".to_string()),
                phase_direction_radians: [0.0, 0.0],
            },
            FieldSummary {
                field_id: 1,
                name: "FIELD_B".to_string(),
                code: "".to_string(),
                source_id: 0,
                row_count: 2,
                unflagged_row_count: None,
                time_mjd_seconds: 140.0,
                direction_reference: Some("J2000".to_string()),
                phase_direction_radians: [0.1, 0.1],
            },
        ],
        polarization_setups: vec![PolarizationSummary {
            polarization_id: 0,
            num_correlations: 2,
            correlation_types: vec!["XX".to_string(), "YY".to_string()],
        }],
        data_descriptions: vec![
            DataDescriptionSummary {
                data_description_id: 0,
                spectral_window_id: 0,
                polarization_id: 0,
                flagged: false,
            },
            DataDescriptionSummary {
                data_description_id: 1,
                spectral_window_id: 1,
                polarization_id: 0,
                flagged: false,
            },
        ],
        spectral_windows: vec![
            SpectralWindowSummary {
                spectral_window_id: 0,
                name: "LOW".to_string(),
                num_channels: 16,
                frame: Some("TOPO".to_string()),
                first_channel_frequency_hz: 1.0e9,
                channel_width_hz: 1.0e6,
                reference_frequency_hz: 1.0e9,
                center_frequency_hz: 1.008e9,
                min_frequency_hz: 0.9995e9,
                max_frequency_hz: 1.0165e9,
                total_bandwidth_hz: 16.0e6,
                data_description_ids: vec![0],
                polarization_ids: vec![0],
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            },
            SpectralWindowSummary {
                spectral_window_id: 1,
                name: "HIGH".to_string(),
                num_channels: 32,
                frame: Some("TOPO".to_string()),
                first_channel_frequency_hz: 1.2e9,
                channel_width_hz: 5.0e5,
                reference_frequency_hz: 1.2e9,
                center_frequency_hz: 1.208e9,
                min_frequency_hz: 1.19975e9,
                max_frequency_hz: 1.21625e9,
                total_bandwidth_hz: 16.0e6,
                data_description_ids: vec![1],
                polarization_ids: vec![0],
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            },
        ],
        sources: vec![SourceSummary {
            source_id: 0,
            name: "SOURCE".to_string(),
            code: "".to_string(),
            spectral_window_id: -1,
            calibration_group: 0,
            num_lines: 0,
            rest_frequency_hz: None,
            system_velocity_m_s: None,
            time_mjd_seconds: 100.0,
            direction_radians: [0.0, 0.0],
        }],
        antennas: vec![
            AntennaSummary {
                antenna_id: 0,
                name: "ea01".to_string(),
                station: "A01".to_string(),
                antenna_type: "GROUND-BASED".to_string(),
                mount: "ALT-AZ".to_string(),
                dish_diameter_m: 25.0,
                longitude_radians: 0.0,
                latitude_radians: 0.0,
                offset_from_observatory_m: [0.0, 0.0, 0.0],
                position_m: [1000.0, 2000.0, 0.0],
            },
            AntennaSummary {
                antenna_id: 1,
                name: "ea02".to_string(),
                station: "A02".to_string(),
                antenna_type: "GROUND-BASED".to_string(),
                mount: "ALT-AZ".to_string(),
                dish_diameter_m: 25.0,
                longitude_radians: 0.0,
                latitude_radians: 0.0,
                offset_from_observatory_m: [120.0, 40.0, 0.0],
                position_m: [1120.0, 2040.0, 0.0],
            },
            AntennaSummary {
                antenna_id: 2,
                name: "ea03".to_string(),
                station: "A03".to_string(),
                antenna_type: "GROUND-BASED".to_string(),
                mount: "ALT-AZ".to_string(),
                dish_diameter_m: 18.0,
                longitude_radians: 0.0,
                latitude_radians: 0.0,
                offset_from_observatory_m: [-90.0, 150.0, 0.0],
                position_m: [910.0, 2150.0, 0.0],
            },
        ],
    }
}

fn synthetic_uv_coverage() -> ListObsUvCoverage {
    ListObsUvCoverage {
        schema_version: 1,
        options: ListObsOptions::default(),
        measurement_set_path: Some("fixture.ms".to_string()),
        axis_unit: "lambda".to_string(),
        mirrored_display: true,
        sample_count: 4,
        max_abs_uv_lambda: 2400.0,
        tracks: vec![
            MeasurementSetUvTrack {
                antenna1: 0,
                antenna2: 1,
                field_id: 0,
                spectral_window_id: 0,
                center_frequency_hz: 1.0e9,
                samples: vec![
                    MeasurementSetUvPoint {
                        row: 0,
                        time_mjd_seconds: 100.0,
                        u_lambda: 200.0,
                        v_lambda: 400.0,
                        w_lambda: 0.0,
                    },
                    MeasurementSetUvPoint {
                        row: 1,
                        time_mjd_seconds: 101.0,
                        u_lambda: 1200.0,
                        v_lambda: 1800.0,
                        w_lambda: 0.0,
                    },
                ],
            },
            MeasurementSetUvTrack {
                antenna1: 0,
                antenna2: 2,
                field_id: 1,
                spectral_window_id: 1,
                center_frequency_hz: 1.2e9,
                samples: vec![
                    MeasurementSetUvPoint {
                        row: 2,
                        time_mjd_seconds: 102.0,
                        u_lambda: -2400.0,
                        v_lambda: 1000.0,
                        w_lambda: 0.0,
                    },
                    MeasurementSetUvPoint {
                        row: 3,
                        time_mjd_seconds: 103.0,
                        u_lambda: -400.0,
                        v_lambda: -800.0,
                        w_lambda: 0.0,
                    },
                ],
            },
        ],
    }
}
