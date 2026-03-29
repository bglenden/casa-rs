// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use std::f64::consts::PI;
use std::path::Path;
use std::process::Command;

use casacore_ms::columns::frequency_columns::ChanFreqColumn;
use casacore_ms::derived::engine::MsCalEngine;
use casacore_ms::listobs::cli::{UiArgumentParser, UiValueKind};
use casacore_ms::msexplore::cli::command_schema;
use casacore_ms::subtables::SubTable;
use casacore_ms::{
    MeasurementSet, MsIterationAxis, MsPlotPayload, MsPlotPreset, MsSelectionSpec,
    build_msexplore_plot_payload,
};
use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::frame::MeasFrame;
use casacore_types::measures::frequency::FrequencyRef;
use common::{
    TIME_BASE_SECONDS, create_msexplore_fixture_ms, create_msexplore_fixture_ms_with_flags,
    create_msexplore_geometry_fixture_ms, create_msexplore_spectrum_fixture_ms,
};
use tempfile::tempdir;

#[test]
fn msexplore_help_mentions_plot_controls() {
    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .arg("--help")
        .output()
        .expect("run msexplore --help");
    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(stdout.contains("Usage:"));
    assert!(stdout.contains("--preset <PRESET>"));
    assert!(stdout.contains("--xaxis <AXIS>"));
    assert!(stdout.contains("--yaxis <AXIS>"));
    assert!(stdout.contains("--data-column <COLUMN>"));
    assert!(stdout.contains("--color-by <AXIS>"));
    assert!(stdout.contains("--avgchannel <N>"));
    assert!(stdout.contains("--freqframe <FRAME>"));
    assert!(stdout.contains("--restfreq <FREQ>"));
    assert!(stdout.contains("--veldef <DEF>"));
    assert!(stdout.contains("--iteraxis <AXIS>"));
    assert!(stdout.contains("--gridrows <N>"));
    assert!(stdout.contains("--gridcols <N>"));
    assert!(stdout.contains("--plot-output <PATH>"));
    assert!(stdout.contains("--plot-format <FORMAT>"));
    assert!(stdout.contains("--msselect <EXPR>"));
    assert!(stdout.contains("--ui-schema"));
}

#[test]
fn msexplore_ui_schema_round_trips_help() {
    let help = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .arg("--help")
        .output()
        .expect("run msexplore --help");
    assert!(help.status.success());

    let help_text = String::from_utf8(help.stdout).expect("utf8 help stdout");
    assert_eq!(command_schema("msexplore").render_help(), help_text);
}

#[test]
fn msexplore_ui_schema_describes_launcher_contract() {
    let schema = command_schema("msexplore");
    assert_eq!(schema.command_id, "msexplore");
    assert_eq!(schema.display_name, "MSExplore");
    assert_eq!(schema.category, "MeasurementSet");

    let preset = schema.argument("preset").expect("preset argument");
    assert_eq!(preset.value_kind, UiValueKind::Choice);

    let x_axis = schema.argument("x_axis").expect("x_axis argument");
    assert!(matches!(
        x_axis.parser,
        UiArgumentParser::Option { ref metavar, .. } if metavar == "AXIS"
    ));
    let iteraxis = schema.argument("iteraxis").expect("iteraxis argument");
    assert_eq!(iteraxis.value_kind, UiValueKind::Choice);

    let plot_format = schema
        .argument("plot_format")
        .expect("plot_format argument");
    assert_eq!(plot_format.value_kind, UiValueKind::Choice);

    let managed_output = schema.managed_output.expect("managed output");
    assert_eq!(managed_output.renderer, "listobs-summary-v1");
    assert_eq!(managed_output.stdout_format, "json");
}

#[test]
fn msexplore_preset_txt_export_emits_manifest_and_summary_json() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--preset",
            "amplitude_vs_time",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse summary json");
    assert_eq!(json["measurement_set"]["row_count"], 4);

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert!(manifest.starts_with("# msexplore-manifest-v1"));
    assert!(manifest.contains("series_key\tseries_label\tx\ty"));
}

#[test]
fn msexplore_time_manifest_emits_per_channel_and_correlation_points() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--preset", "amplitude_vs_time", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    let point_lines = manifest
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("series_key"))
        .count();
    assert_eq!(point_lines, 256);
}

#[test]
fn msexplore_avgchannel_bins_channel_plot_manifest() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-chan.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--preset",
            "amplitude_vs_channel",
            "--avgchannel",
            "4",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    let point_lines = manifest
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("series_key"))
        .count();
    assert_eq!(point_lines, 64);

    let x_values = manifest
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("series_key"))
        .map(|line| {
            line.split('\t')
                .nth(2)
                .expect("x value")
                .parse::<f64>()
                .expect("parse x")
                .round() as i32
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(x_values, [0, 1, 2, 3].into_iter().collect());
}

#[test]
fn msexplore_u_vs_v_manifest_uses_row_uvw_coordinates() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("u-v.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--xaxis", "u", "--yaxis", "v", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 4);

    let unique_x = rows
        .iter()
        .map(|(_, x, _)| *x as i32)
        .collect::<std::collections::BTreeSet<_>>();
    let unique_y = rows
        .iter()
        .map(|(_, _, y)| *y as i32)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique_x, [30, 31, 32, 33].into_iter().collect());
    assert_eq!(unique_y, [40].into_iter().collect());
}

#[test]
fn msexplore_amplitude_vs_w_manifest_uses_uvw_w_component() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-w.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--xaxis", "w", "--yaxis", "amplitude", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 256);
    assert!(rows.iter().all(|(_, x, _)| x.abs() < 1.0e-9));
}

#[test]
fn msexplore_velocity_manifest_uses_spw_center_as_default_rest_frequency() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-vel.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_velocity",
            "--scan",
            "1",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 64);

    let center_frequency_hz = (1.0e9 + 1.015e9) / 2.0;
    let mut expected = (0..16)
        .map(|chan| {
            let frequency_hz = 1.0e9 + chan as f64 * 1.0e6;
            299_792.458 * (1.0 - frequency_hz / center_frequency_hz)
        })
        .collect::<Vec<_>>();
    expected.sort_by(f64::total_cmp);
    let mut unique_x = rows.iter().map(|(_, x, _)| *x).collect::<Vec<_>>();
    unique_x.sort_by(f64::total_cmp);
    unique_x.dedup_by(|left, right| (*left - *right).abs() <= 1.0e-9);

    assert_eq!(unique_x.len(), expected.len());
    for (actual, expected) in unique_x.iter().zip(expected.iter()) {
        assert!((actual - expected).abs() < 1.0e-6);
    }
}

#[test]
fn msexplore_velocity_manifest_applies_freqframe_restfreq_and_veldef() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_geometry_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-vel-lsrk.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--xaxis",
            "velocity",
            "--yaxis",
            "amplitude",
            "--scan",
            "1",
            "--freqframe",
            "LSRK",
            "--restfreq",
            "1.0315GHz",
            "--veldef",
            "OPTICAL",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 64);

    let ms = MeasurementSet::open(&ms_path).expect("open geometry fixture");
    let engine = MsCalEngine::new(&ms).expect("engine");
    let spectral_window = ms.spectral_window().expect("spw");
    let chan_freq = ChanFreqColumn::new(spectral_window.table());
    let frequencies = chan_freq.get_frequencies(0).expect("channel frequencies");
    let frame = engine
        .spectral_frame_observatory(TIME_BASE_SECONDS, 0)
        .expect("spectral frame");
    let mut expected = frequencies
        .iter()
        .map(|frequency| {
            let converted = frequency
                .convert_to(FrequencyRef::LSRK, &frame)
                .expect("convert frequency");
            let doppler = MDoppler::new(converted.hz() / 1.0315e9, DopplerRef::RATIO)
                .convert_to(DopplerRef::Z, &MeasFrame::new())
                .expect("doppler");
            doppler.value() * 299_792.458
        })
        .collect::<Vec<_>>();
    expected.sort_by(f64::total_cmp);

    let mut unique_x = rows.iter().map(|(_, x, _)| *x).collect::<Vec<_>>();
    unique_x.sort_by(f64::total_cmp);
    unique_x.dedup_by(|left, right| (*left - *right).abs() <= 1.0e-9);

    assert_eq!(unique_x.len(), expected.len());
    for (actual, expected) in unique_x.iter().zip(expected.iter()) {
        assert!((actual - expected).abs() < 1.0e-6);
    }
}

#[test]
fn msexplore_msselect_filters_summary_rows() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--format", "json", "--msselect", "SCAN_NUMBER == 1"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse summary json");
    assert_eq!(json["measurement_set"]["row_count"], 1);
}

#[test]
fn msexplore_scan_iteration_manifest_groups_points_into_panels() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-time-scan-grid.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_time",
            "--iteraxis",
            "scan",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert!(manifest.contains("# iteraxis=scan"));
    assert_eq!(manifest_header_value(&manifest, "gridrows"), Some("2"));
    assert_eq!(manifest_header_value(&manifest, "gridcols"), Some("2"));

    let rows = iterated_manifest_rows(&plot_path);
    assert_eq!(rows.len(), 256);

    let panel_keys = rows
        .iter()
        .map(|(panel_key, _, _, _, _)| panel_key.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        panel_keys,
        ["scan-1", "scan-2", "scan-3", "scan-4"]
            .into_iter()
            .map(str::to_string)
            .collect()
    );
    for expected_panel in ["scan-1", "scan-2", "scan-3", "scan-4"] {
        assert_eq!(
            rows.iter()
                .filter(|(panel_key, _, _, _, _)| panel_key == expected_panel)
                .count(),
            64
        );
    }
}

#[test]
fn msexplore_scan_iteration_payload_resolves_grid_and_scaling() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = casacore_ms::MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.iteration.xselfscale = true;

    let payload = build_msexplore_plot_payload(&ms, &MsSelectionSpec::default(), &spec)
        .expect("build iterated payload");
    let MsPlotPayload::ScatterGrid(grid) = payload else {
        panic!("expected iterated scatter grid payload");
    };
    assert_eq!(grid.gridrows, 2);
    assert_eq!(grid.gridcols, 2);
    assert!(!grid.share_x_bounds);
    assert!(grid.share_y_bounds);
    assert_eq!(grid.panels.len(), 4);
}

#[test]
fn msexplore_weight_vs_time_manifest_emits_one_point_per_row_and_correlation() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("weight-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--xaxis", "time", "--yaxis", "weight", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 16);

    let y_values = rows
        .iter()
        .map(|(_, _, y)| *y as i32)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        y_values,
        [
            100, 101, 102, 103, 200, 201, 202, 203, 300, 301, 302, 303, 400, 401, 402, 403
        ]
        .into_iter()
        .collect()
    );
}

#[test]
fn msexplore_sigma_vs_time_manifest_emits_one_point_per_row_and_correlation() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("sigma-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--xaxis", "time", "--yaxis", "sigma", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 16);

    let y_values = rows
        .iter()
        .map(|(_, _, y)| (y * 10.0).round() as i32)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        y_values,
        [
            10, 11, 12, 13, 20, 21, 22, 23, 30, 31, 32, 33, 40, 41, 42, 43
        ]
        .into_iter()
        .collect()
    );
}

#[test]
fn msexplore_flag_vs_time_manifest_keeps_flagged_samples() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms_with_flags(temp.path(), &[(0, 1, 2), (2, 3, 15)]);
    let plot_path = temp.path().join("flag-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--xaxis", "time", "--yaxis", "flag", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 256);
    assert_eq!(
        rows.iter()
            .filter(|(_, _, y)| (*y - 1.0).abs() < 1e-9)
            .count(),
        2
    );
    assert_eq!(
        rows.iter()
            .filter(|(_, _, y)| (*y - 0.0).abs() < 1e-9)
            .count(),
        254
    );
}

#[test]
fn msexplore_weight_spectrum_vs_time_uses_channelized_values() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_spectrum_fixture_ms(temp.path(), true, &[]);
    let plot_path = temp.path().join("weight-spectrum-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--xaxis",
            "time",
            "--yaxis",
            "weight_spectrum",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 256);
    assert!(rows.iter().any(|(_, _, y)| (*y - 1000.0).abs() < 1e-9));
    assert!(rows.iter().any(|(_, _, y)| (*y - 4315.0).abs() < 1e-9));
}

#[test]
fn msexplore_sigma_spectrum_vs_time_falls_back_to_sigma_without_column() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("sigma-spectrum-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--xaxis",
            "time",
            "--yaxis",
            "sigma_spectrum",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 256);
    assert!(rows.iter().any(|(_, _, y)| (*y - 1.0).abs() < 1e-6));
    assert!(rows.iter().any(|(_, _, y)| (*y - 4.3).abs() < 1e-6));
}

#[test]
fn msexplore_flagrow_vs_time_repeats_row_flags_for_each_sample() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_spectrum_fixture_ms(temp.path(), false, &[1]);
    let plot_path = temp.path().join("flagrow-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--xaxis", "time", "--yaxis", "flagrow", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows.iter()
            .filter(|(_, _, y)| (*y - 1.0).abs() < 1e-9)
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter(|(_, _, y)| (*y - 0.0).abs() < 1e-9)
            .count(),
        3
    );
}

#[test]
fn msexplore_elevation_vs_time_matches_derived_geometry_degrees() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_geometry_fixture_ms(temp.path());
    let plot_path = temp.path().join("elevation-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--preset", "elevation_vs_time", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 4);

    let ms = MeasurementSet::open(&ms_path).expect("open geometry fixture");
    let engine = MsCalEngine::new(&ms).expect("engine");
    let expected = (0..4)
        .map(|row| {
            engine
                .azel(TIME_BASE_SECONDS + row as f64 * 3600.0, 0, 0)
                .expect("azel")
                .1
                .to_degrees()
        })
        .collect::<Vec<_>>();
    let actual = rows.iter().map(|(_, _, y)| *y).collect::<Vec<_>>();

    assert!(
        actual
            .iter()
            .zip(expected.iter())
            .all(|(actual, expected)| (*actual - *expected).abs() < 1.0e-9)
    );
}

#[test]
fn msexplore_hour_angle_vs_time_uses_hours() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_geometry_fixture_ms(temp.path());
    let plot_path = temp.path().join("hour-angle-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--preset", "hour_angle_vs_time", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 4);

    let ms = MeasurementSet::open(&ms_path).expect("open geometry fixture");
    let engine = MsCalEngine::new(&ms).expect("engine");
    let expected = (0..4)
        .map(|row| {
            engine
                .hour_angle(TIME_BASE_SECONDS + row as f64 * 3600.0, 0, 0)
                .expect("ha")
                * 12.0
                / PI
        })
        .collect::<Vec<_>>();
    let actual = rows.iter().map(|(_, _, y)| *y).collect::<Vec<_>>();

    assert!(
        actual
            .iter()
            .zip(expected.iter())
            .all(|(actual, expected)| (*actual - *expected).abs() < 1.0e-9)
    );
}

#[test]
fn msexplore_azimuth_vs_elevation_uses_geometry_axes_on_both_sides() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_geometry_fixture_ms(temp.path());
    let plot_path = temp.path().join("azimuth-elevation.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--preset", "azimuth_vs_elevation", "--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 4);

    let ms = MeasurementSet::open(&ms_path).expect("open geometry fixture");
    let engine = MsCalEngine::new(&ms).expect("engine");
    let mut expected = (0..4)
        .map(|row| {
            engine
                .azel(TIME_BASE_SECONDS + row as f64 * 3600.0, 0, 0)
                .expect("azel")
        })
        .collect::<Vec<_>>();
    expected.sort_by(|left, right| left.1.total_cmp(&right.1));

    assert!(
        rows.iter()
            .zip(expected.iter())
            .all(|((_, x, y), (azimuth, elevation))| {
                (*x - elevation.to_degrees()).abs() < 1.0e-6
                    && (*y - normalize_signed_degrees(azimuth.to_degrees())).abs() < 1.0e-6
            })
    );
}

fn normalize_signed_degrees(angle_degrees: f64) -> f64 {
    let wrapped = (angle_degrees + 180.0).rem_euclid(360.0) - 180.0;
    if wrapped == -180.0 { 180.0 } else { wrapped }
}

fn manifest_rows(path: &Path) -> Vec<(String, f64, f64)> {
    std::fs::read_to_string(path)
        .expect("read manifest")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("series_key"))
        .map(|line| {
            let mut parts = line.split('\t');
            let key = parts.next().expect("series key").to_string();
            let _label = parts.next().expect("series label");
            let x = parts
                .next()
                .expect("x value")
                .parse::<f64>()
                .expect("parse x");
            let y = parts
                .next()
                .expect("y value")
                .parse::<f64>()
                .expect("parse y");
            (key, x, y)
        })
        .collect()
}

fn iterated_manifest_rows(path: &Path) -> Vec<(String, String, String, f64, f64)> {
    std::fs::read_to_string(path)
        .expect("read iterated manifest")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("panel_key"))
        .map(|line| {
            let mut parts = line.split('\t');
            let panel_key = parts.next().expect("panel key").to_string();
            let panel_label = parts.next().expect("panel label").to_string();
            let series_key = parts.next().expect("series key").to_string();
            let _series_label = parts.next().expect("series label");
            let x = parts
                .next()
                .expect("x value")
                .parse::<f64>()
                .expect("parse x");
            let y = parts
                .next()
                .expect("y value")
                .parse::<f64>()
                .expect("parse y");
            (panel_key, panel_label, series_key, x, y)
        })
        .collect()
}

fn manifest_header_value<'a>(manifest: &'a str, key: &str) -> Option<&'a str> {
    manifest
        .lines()
        .find_map(|line| line.strip_prefix(&format!("# {key}=")))
}
