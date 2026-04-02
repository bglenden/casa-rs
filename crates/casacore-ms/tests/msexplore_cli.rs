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
    ListObsOutputFormat, MeasurementSet, MsAxis, MsColorAxis, MsDataColumn, MsExploreSpec,
    MsFlagAction, MsFlagEditSpec, MsFlagRegion, MsIterationAxis, MsLayoutSpec, MsLegendPosition,
    MsPageExportRange, MsPageHeaderItem, MsPlotPayload, MsPlotPreset, MsPlotSpec, MsSelectionSpec,
    apply_msexplore_flag_edit, build_msexplore_payload, build_msexplore_plot_payload,
    preview_msexplore_flag_edit, render_msexplore_plot_image,
};
use casacore_types::ArrayValue;
use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::frame::MeasFrame;
use casacore_types::measures::frequency::FrequencyRef;
use common::{
    TIME_BASE_SECONDS, create_msexplore_fixture_ms, create_msexplore_fixture_ms_with_flags,
    create_msexplore_geometry_fixture_ms, create_msexplore_spectrum_fixture_ms,
};
use ndarray::Ix2;
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
    assert!(stdout.contains("--yaxis2 <AXIS>"));
    assert!(stdout.contains("--data-column <COLUMN>"));
    assert!(stdout.contains("--color-by <AXIS>"));
    assert!(stdout.contains("--avgchannel <N>"));
    assert!(stdout.contains("--freqframe <FRAME>"));
    assert!(stdout.contains("--restfreq <FREQ>"));
    assert!(stdout.contains("--veldef <DEF>"));
    assert!(stdout.contains("--iteraxis <AXIS>"));
    assert!(stdout.contains("--gridrows <N>"));
    assert!(stdout.contains("--gridcols <N>"));
    assert!(stdout.contains("--showlegend"));
    assert!(stdout.contains("--legendposition <POSITION>"));
    assert!(stdout.contains("--headeritems <ITEMS>"));
    assert!(stdout.contains("--flag-action <ACTION>"));
    assert!(stdout.contains("--flag-xmin <VALUE>"));
    assert!(stdout.contains("--flag-xmax <VALUE>"));
    assert!(stdout.contains("--flag-ymin <VALUE>"));
    assert!(stdout.contains("--flag-ymax <VALUE>"));
    assert!(stdout.contains("--flag-panel <KEY>"));
    assert!(stdout.contains("--flag-extcorr"));
    assert!(stdout.contains("--flag-extchannel"));
    assert!(stdout.contains("--flag-apply"));
    assert!(stdout.contains("--flag-output <PATH>"));
    assert!(stdout.contains("--plot-output <PATH>"));
    assert!(stdout.contains("--plot-format <FORMAT>"));
    assert!(stdout.contains("--msselect <EXPR>"));
    assert!(stdout.contains("--page-spec <PATH>"));
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
    let page_spec = schema.argument("page_spec").expect("page_spec argument");
    assert_eq!(page_spec.value_kind, UiValueKind::Path);

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
    let legendposition = schema
        .argument("legendposition")
        .expect("legendposition argument");
    assert_eq!(legendposition.value_kind, UiValueKind::Choice);
    let flag_action = schema
        .argument("flag_action")
        .expect("flag_action argument");
    assert_eq!(flag_action.value_kind, UiValueKind::Choice);
    let flag_panel = schema.argument("flag_panel").expect("flag_panel argument");
    assert_eq!(flag_panel.value_kind, UiValueKind::String);
    let flag_xmin = schema.argument("flag_xmin").expect("flag_xmin argument");
    assert_eq!(flag_xmin.value_kind, UiValueKind::Float);

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
fn msexplore_dual_y_manifest_emits_axis_column_for_each_series() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-phase-time.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--xaxis",
            "time",
            "--yaxis",
            "amplitude",
            "--yaxis2",
            "phase",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(
        manifest_header_value(&manifest, "secondary_y_axis"),
        Some("phase")
    );
    assert!(manifest.contains("series_key\tseries_label\ty_axis\tx\ty"));

    let rows = dual_manifest_rows(&plot_path);
    assert_eq!(rows.len(), 512);
    assert_eq!(
        rows.iter()
            .filter(|(_, _, axis, _, _)| axis == "amplitude")
            .count(),
        256
    );
    assert_eq!(
        rows.iter()
            .filter(|(_, _, axis, _, _)| axis == "phase")
            .count(),
        256
    );
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
fn msexplore_dual_y_payload_tracks_secondary_axis_and_style_flags() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.y_axes.push(MsAxis::Phase);
    spec.style.showlegend = true;
    spec.style.legendposition = MsLegendPosition::LowerLeft;
    spec.style.showmajorgrid = true;
    spec.style.showminorgrid = true;

    let payload = build_msexplore_plot_payload(&ms, &MsSelectionSpec::default(), &spec)
        .expect("build dual-y payload");
    let MsPlotPayload::Scatter(scatter) = payload else {
        panic!("expected scatter payload");
    };
    assert_eq!(scatter.y_axis, MsAxis::Amplitude);
    assert_eq!(scatter.secondary_y_axis, Some(MsAxis::Phase));
    assert_eq!(scatter.secondary_y_label.as_deref(), Some("Phase (deg)"));
    assert!(scatter.showlegend);
    assert_eq!(scatter.legend_position, MsLegendPosition::LowerLeft);
    assert!(scatter.showmajorgrid);
    assert!(scatter.showminorgrid);
    let series_axes = scatter
        .series
        .iter()
        .map(|series| series.y_axis.as_str().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        series_axes,
        ["amplitude".to_string(), "phase".to_string()]
            .into_iter()
            .collect()
    );
}

#[test]
fn msexplore_single_plot_manifest_emits_header_lines_and_legend_position() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-time-header.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_time",
            "--showlegend",
            "--legendposition",
            "lowerLeft",
            "--headeritems",
            "filename,ycolumn",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(
        manifest_header_value(&manifest, "legendposition"),
        Some("lowerLeft")
    );
    assert!(manifest.contains("# header_line=Filename:"));
    assert!(manifest.contains("Y Column: Amplitude"));
}

#[test]
fn msexplore_payload_resolves_page_header_items() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let payload = build_msexplore_payload(
        &ms,
        &MsExploreSpec {
            ms_path,
            summary_format: ListObsOutputFormat::Text,
            selection: MsSelectionSpec::default(),
            header_items: vec![MsPageHeaderItem::Filename, MsPageHeaderItem::YColumn],
            page_title: Some("Amplitude".to_string()),
            exprange: MsPageExportRange::Current,
            plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime)],
        },
    )
    .expect("build payload");

    let MsPlotPayload::Scatter(scatter) = payload else {
        panic!("expected scatter payload");
    };
    assert!(!scatter.header_lines.is_empty());
    assert!(
        scatter
            .header_lines
            .iter()
            .any(|line| line.contains("Filename:"))
    );
    assert!(
        scatter
            .header_lines
            .iter()
            .any(|line| line.contains("Y Column: Amplitude"))
    );
}

#[test]
fn msexplore_stacked_time_preset_manifest_emits_two_plots_on_one_page() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-phase-stacked.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_phase_vs_time_stacked",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(manifest_header_value(&manifest, "gridrows"), Some("2"));
    assert_eq!(manifest_header_value(&manifest, "gridcols"), Some("1"));

    let rows = page_manifest_rows(&plot_path);
    assert_eq!(rows.len(), 512);
    assert_eq!(
        rows.iter()
            .filter(|(plotindex, _, _, _, _, _, _, _, _, _)| *plotindex == 0)
            .count(),
        256
    );
    assert_eq!(
        rows.iter()
            .filter(|(plotindex, _, _, _, _, _, _, _, _, _)| *plotindex == 1)
            .count(),
        256
    );
    assert!(rows.iter().any(|(_, _, _, title, _, y_axis, _, _, _, _)| {
        title == "Amplitude vs Time" && y_axis == "amplitude"
    }));
    assert!(rows.iter().any(|(_, _, _, title, _, y_axis, _, _, _, _)| {
        title == "Phase vs Time" && y_axis == "phase"
    }));
}

#[test]
fn msexplore_stacked_time_preset_builds_two_row_page_payload() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let payload = build_msexplore_plot_payload(
        &ms,
        &MsSelectionSpec::default(),
        &MsPlotSpec::from_preset(MsPlotPreset::AmplitudePhaseVsTimeStacked),
    )
    .expect("build stacked page payload");
    let MsPlotPayload::ScatterPage(page) = payload else {
        panic!("expected scatter page payload");
    };
    assert_eq!(page.gridrows, 2);
    assert_eq!(page.gridcols, 1);
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].plotindex, 0);
    assert_eq!(page.items[0].rowindex, 0);
    assert_eq!(page.items[0].plot.y_axis, MsAxis::Amplitude);
    assert_eq!(page.items[1].plotindex, 1);
    assert_eq!(page.items[1].rowindex, 1);
    assert_eq!(page.items[1].plot.y_axis, MsAxis::Phase);
}

#[test]
fn msexplore_generic_page_spec_builds_side_by_side_page_payload() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let mut amplitude = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    amplitude.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 2,
        rowindex: 0,
        colindex: 0,
        plotindex: 0,
    };
    amplitude.style.title = Some("Amplitude vs Time".to_string());

    let mut phase = MsPlotSpec::from_preset(MsPlotPreset::PhaseVsTime);
    phase.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 2,
        rowindex: 0,
        colindex: 1,
        plotindex: 1,
    };
    phase.style.title = Some("Phase vs Time".to_string());
    phase.color_by = MsColorAxis::Scan;
    phase.data_column = MsDataColumn::Data;

    let payload = build_msexplore_payload(
        &ms,
        &MsExploreSpec {
            ms_path,
            summary_format: ListObsOutputFormat::Text,
            selection: MsSelectionSpec::default(),
            header_items: Vec::new(),
            page_title: Some("Amplitude and Phase Side by Side".to_string()),
            exprange: MsPageExportRange::Current,
            plots: vec![amplitude, phase],
        },
    )
    .expect("build generic page payload");
    let MsPlotPayload::ScatterPage(page) = payload else {
        panic!("expected scatter page payload");
    };
    assert_eq!(page.title, "Amplitude and Phase Side by Side");
    assert_eq!(page.gridrows, 1);
    assert_eq!(page.gridcols, 2);
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].colindex, 0);
    assert_eq!(page.items[0].plot.y_axis, MsAxis::Amplitude);
    assert_eq!(page.items[1].colindex, 1);
    assert_eq!(page.items[1].plot.y_axis, MsAxis::Phase);
}

#[test]
fn msexplore_page_spec_manifest_emits_multiple_positions_on_one_page() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-phase-page.txt");
    let page_spec_path = temp.path().join("page.json");

    std::fs::write(
        &page_spec_path,
        serde_json::json!({
            "page_title": "Amplitude and Phase Side by Side",
            "gridrows": 1,
            "gridcols": 2,
            "plots": [
                {
                    "preset": "amplitude_vs_time",
                    "plotindex": 0,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude vs Time"
                },
                {
                    "preset": "phase_vs_time",
                    "plotindex": 1,
                    "rowindex": 0,
                    "colindex": 1,
                    "title": "Phase vs Time"
                }
            ]
        })
        .to_string(),
    )
    .expect("write page spec");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--page-spec"])
        .arg(&page_spec_path)
        .args(["--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(
        manifest_header_value(&manifest, "exprange"),
        Some("current")
    );
    assert_eq!(manifest_header_value(&manifest, "gridrows"), Some("1"));
    assert_eq!(manifest_header_value(&manifest, "gridcols"), Some("2"));

    let rows = page_manifest_rows(&plot_path);
    assert_eq!(rows.len(), 512);
    assert_eq!(
        rows.iter()
            .filter(
                |(plotindex, rowindex, colindex, title, _, y_axis, _, _, _, _)| {
                    *plotindex == 0
                        && *rowindex == 0
                        && *colindex == 0
                        && title == "Amplitude vs Time"
                        && y_axis == "amplitude"
                }
            )
            .count(),
        256
    );
    assert_eq!(
        rows.iter()
            .filter(
                |(plotindex, rowindex, colindex, title, _, y_axis, _, _, _, _)| {
                    *plotindex == 1
                        && *rowindex == 0
                        && *colindex == 1
                        && title == "Phase vs Time"
                        && y_axis == "phase"
                }
            )
            .count(),
        256
    );
}

#[test]
fn msexplore_generic_page_spec_allows_same_cell_overplot_render() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");

    let mut left = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    left.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 1,
        rowindex: 0,
        colindex: 0,
        plotindex: 0,
    };
    left.style.title = Some("Amplitude:vector".to_string());
    left.style.showlegend = true;

    let mut right = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    right.layout = MsLayoutSpec {
        gridrows: 1,
        gridcols: 1,
        rowindex: 0,
        colindex: 0,
        plotindex: 1,
    };
    right.averaging.scalar = true;
    right.style.title = Some("Amplitude:scalar".to_string());
    right.style.showlegend = true;

    let payload = build_msexplore_payload(
        &ms,
        &MsExploreSpec {
            ms_path,
            summary_format: ListObsOutputFormat::Text,
            selection: MsSelectionSpec::default(),
            header_items: Vec::new(),
            page_title: Some("Amplitude Overplot".to_string()),
            exprange: MsPageExportRange::Current,
            plots: vec![left, right],
        },
    )
    .expect("build overplot page payload");
    let MsPlotPayload::ScatterPage(page) = &payload else {
        panic!("expected scatter page payload");
    };
    assert_eq!(page.exprange, MsPageExportRange::Current);
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].rowindex, 0);
    assert_eq!(page.items[1].rowindex, 0);
    assert_eq!(page.items[0].colindex, 0);
    assert_eq!(page.items[1].colindex, 0);

    let image =
        render_msexplore_plot_image(&payload, casacore_ms::ListObsPlotTheme::light(), 1200, 800)
            .expect("render overplot image");
    assert_eq!(image.width(), 1200);
    assert_eq!(image.height(), 800);
}

#[test]
fn msexplore_page_spec_manifest_keeps_duplicate_cell_coordinates_for_overplot() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("amp-overplot.txt");
    let page_spec_path = temp.path().join("page-overplot.json");

    std::fs::write(
        &page_spec_path,
        serde_json::json!({
            "page_title": "Amplitude Overplot",
            "exprange": "all",
            "gridrows": 1,
            "gridcols": 1,
            "plots": [
                {
                    "preset": "amplitude_vs_time",
                    "plotindex": 0,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude:vector"
                },
                {
                    "preset": "amplitude_vs_time",
                    "scalar": true,
                    "plotindex": 1,
                    "rowindex": 0,
                    "colindex": 0,
                    "title": "Amplitude:scalar"
                }
            ]
        })
        .to_string(),
    )
    .expect("write overplot page spec");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--page-spec"])
        .arg(&page_spec_path)
        .args(["--plot-output"])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert_eq!(manifest_header_value(&manifest, "exprange"), Some("all"));
    assert_eq!(manifest_header_value(&manifest, "gridrows"), Some("1"));
    assert_eq!(manifest_header_value(&manifest, "gridcols"), Some("1"));

    let rows = page_manifest_rows(&plot_path);
    assert_eq!(rows.len(), 512);
    assert_eq!(
        rows.iter()
            .filter(|(plotindex, rowindex, colindex, title, _, _, _, _, _, _)| {
                *plotindex == 0 && *rowindex == 0 && *colindex == 0 && title == "Amplitude:vector"
            })
            .count(),
        256
    );
    assert_eq!(
        rows.iter()
            .filter(|(plotindex, rowindex, colindex, title, _, _, _, _, _, _)| {
                *plotindex == 1 && *rowindex == 0 && *colindex == 0 && title == "Amplitude:scalar"
            })
            .count(),
        256
    );
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

fn dual_manifest_rows(path: &Path) -> Vec<(String, String, String, f64, f64)> {
    std::fs::read_to_string(path)
        .expect("read dual manifest")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("series_key"))
        .map(|line| {
            let mut parts = line.split('\t');
            let key = parts.next().expect("series key").to_string();
            let label = parts.next().expect("series label").to_string();
            let axis = parts.next().expect("y axis").to_string();
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
            (key, label, axis, x, y)
        })
        .collect()
}

fn page_manifest_rows(
    path: &Path,
) -> Vec<(
    usize,
    usize,
    usize,
    String,
    String,
    String,
    String,
    String,
    f64,
    f64,
)> {
    std::fs::read_to_string(path)
        .expect("read manifest")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.starts_with("plotindex"))
        .map(|line| {
            let mut parts = line.split('\t');
            let plotindex = parts
                .next()
                .expect("plotindex")
                .parse()
                .expect("parse plotindex");
            let rowindex = parts
                .next()
                .expect("rowindex")
                .parse()
                .expect("parse rowindex");
            let colindex = parts
                .next()
                .expect("colindex")
                .parse()
                .expect("parse colindex");
            let plot_title = parts.next().expect("plot_title").to_string();
            let x_axis = parts.next().expect("x_axis").to_string();
            let y_axis = parts.next().expect("y_axis").to_string();
            let series_key = parts.next().expect("series_key").to_string();
            let series_label = parts.next().expect("series_label").to_string();
            let x = parts.next().expect("x").parse().expect("parse x");
            let y = parts.next().expect("y").parse().expect("parse y");
            (
                plotindex,
                rowindex,
                colindex,
                plot_title,
                x_axis,
                y_axis,
                series_key,
                series_label,
                x,
                y,
            )
        })
        .collect()
}

fn manifest_header_value<'a>(manifest: &'a str, key: &str) -> Option<&'a str> {
    manifest
        .lines()
        .find_map(|line| line.strip_prefix(&format!("# {key}=")))
}

#[test]
fn preview_flag_edit_selects_one_unique_sample() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        panel_key: None,
        extcorr: false,
        extchannel: false,
    });

    let preview =
        preview_msexplore_flag_edit(&ms, &MsSelectionSpec::default(), &spec).expect("preview");
    assert_eq!(preview.matched_points, 1);
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(preview.affected_samples, 1);
    assert_eq!(preview.sample_edits.len(), 1);
    assert_eq!(preview.sample_edits[0].row, 0);
    assert_eq!(preview.sample_edits[0].corr, 0);
    assert_eq!(preview.sample_edits[0].chan, 0);
    assert!(!preview.sample_edits[0].old_flag);
    assert!(preview.sample_edits[0].new_flag);
    assert_eq!(preview.row_edits[0].row, 0);
    assert_eq!(preview.row_edits[0].old_flag_row, false);
    assert_eq!(preview.row_edits[0].new_flag_row, false);
}

#[test]
fn preview_flag_edit_expands_across_correlation_and_channel() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        panel_key: None,
        extcorr: true,
        extchannel: true,
    });

    let preview =
        preview_msexplore_flag_edit(&ms, &MsSelectionSpec::default(), &spec).expect("preview");
    assert_eq!(preview.matched_points, 1);
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(
        preview.affected_samples,
        common::NUM_CORR * common::NUM_CHAN
    );
    assert_eq!(preview.row_edits[0].new_flag_row, true);
}

#[test]
fn apply_flag_edit_updates_flag_cells_and_flag_row() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        panel_key: None,
        extcorr: true,
        extchannel: true,
    });

    let preview =
        apply_msexplore_flag_edit(&mut ms, &MsSelectionSpec::default(), &spec).expect("apply");
    assert_eq!(
        preview.affected_samples,
        common::NUM_CORR * common::NUM_CHAN
    );
    assert!(ms.flag_row_column().get(0).expect("flag row"));
    let flags = row_flag_matrix(&ms, 0);
    assert!(flags.iter().all(|value| *value));
}

#[test]
fn cli_flag_preview_is_non_mutating_and_cli_apply_persists_changes() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let preview_path = temp.path().join("flag-preview.json");

    let preview_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--preset",
            "amplitude_vs_time",
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run preview");
    assert!(preview_output.status.success(), "{preview_output:?}");

    let preview_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&preview_path).expect("read preview json"))
            .expect("parse preview json");
    assert_eq!(preview_json["matched_points"], 1);
    assert_eq!(preview_json["affected_samples"], 1);
    let ms_after_preview = MeasurementSet::open(&ms_path).expect("reopen after preview");
    assert!(!row_flag_matrix(&ms_after_preview, 0)[(0, 0)]);

    let apply_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--overwrite",
            "--preset",
            "amplitude_vs_time",
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-apply",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run apply");
    assert!(apply_output.status.success(), "{apply_output:?}");

    let ms_after_apply = MeasurementSet::open(&ms_path).expect("reopen after apply");
    assert!(row_flag_matrix(&ms_after_apply, 0)[(0, 0)]);
}

#[test]
fn preview_flag_edit_on_iterated_scan_grid_requires_panel_key() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        panel_key: None,
        extcorr: false,
        extchannel: false,
    });

    let error =
        preview_msexplore_flag_edit(&ms, &MsSelectionSpec::default(), &spec).expect_err("error");
    assert!(error.contains("requires panel_key"));
    assert!(error.contains("scan-1"));
    assert!(error.contains("scan-4"));
}

#[test]
fn preview_flag_edit_on_iterated_scan_panel_reports_panel_metadata() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        panel_key: Some("scan-1".to_string()),
        extcorr: false,
        extchannel: false,
    });

    let preview =
        preview_msexplore_flag_edit(&ms, &MsSelectionSpec::default(), &spec).expect("preview");
    assert_eq!(preview.panel_key.as_deref(), Some("scan-1"));
    assert_eq!(preview.panel_label.as_deref(), Some("Scan 1"));
    assert_eq!(preview.matched_points, 1);
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(preview.affected_samples, 1);
    assert_eq!(preview.sample_edits[0].row, 0);
}

#[test]
fn apply_flag_edit_on_iterated_scan_panel_updates_only_target_panel_rows() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let mut ms = MeasurementSet::open(&ms_path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.flag_edit = Some(MsFlagEditSpec {
        action: MsFlagAction::Flag,
        region: MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        panel_key: Some("scan-1".to_string()),
        extcorr: false,
        extchannel: false,
    });

    let preview =
        apply_msexplore_flag_edit(&mut ms, &MsSelectionSpec::default(), &spec).expect("apply");
    assert_eq!(preview.panel_key.as_deref(), Some("scan-1"));
    assert_eq!(preview.affected_rows, 1);
    assert_eq!(preview.sample_edits[0].row, 0);
    assert!(row_flag_matrix(&ms, 0)[(0, 0)]);
    for row in 1..4 {
        assert!(
            !row_flag_matrix(&ms, row)[(0, 0)],
            "unexpected flag change on row {row}"
        );
    }
}

#[test]
fn cli_flag_preview_on_iterated_scan_panel_includes_panel_key_and_apply_is_selective() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let preview_path = temp.path().join("iterated-flag-preview.json");

    let preview_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--preset",
            "amplitude_vs_time",
            "--iteraxis",
            "scan",
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-panel",
            "scan-1",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run iterated preview");
    assert!(preview_output.status.success(), "{preview_output:?}");

    let preview_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&preview_path).expect("read preview json"))
            .expect("parse preview json");
    assert_eq!(preview_json["panel_key"], "scan-1");
    assert_eq!(preview_json["panel_label"], "Scan 1");
    assert_eq!(preview_json["matched_points"], 1);
    assert_eq!(preview_json["affected_samples"], 1);

    let apply_output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--format",
            "json",
            "--overwrite",
            "--preset",
            "amplitude_vs_time",
            "--iteraxis",
            "scan",
            "--flag-action",
            "flag",
            "--flag-xmin",
            &format!("{}", TIME_BASE_SECONDS - 0.1),
            "--flag-xmax",
            &format!("{}", TIME_BASE_SECONDS + 0.1),
            "--flag-ymin",
            "-0.1",
            "--flag-ymax",
            "0.1",
            "--flag-panel",
            "scan-1",
            "--flag-apply",
            "--flag-output",
        ])
        .arg(&preview_path)
        .arg(&ms_path)
        .output()
        .expect("run iterated apply");
    assert!(apply_output.status.success(), "{apply_output:?}");

    let ms_after_apply = MeasurementSet::open(&ms_path).expect("reopen after apply");
    assert!(row_flag_matrix(&ms_after_apply, 0)[(0, 0)]);
    for row in 1..4 {
        assert!(
            !row_flag_matrix(&ms_after_apply, row)[(0, 0)],
            "unexpected persisted flag change on row {row}"
        );
    }
}

fn row_flag_matrix(ms: &MeasurementSet, row: usize) -> ndarray::Array2<bool> {
    match ms.flag_column().get(row).expect("flag cell") {
        ArrayValue::Bool(values) => values
            .view()
            .into_dimensionality::<Ix2>()
            .expect("2d flag cell")
            .to_owned(),
        other => panic!("unexpected flag cell type {:?}", other.primitive_type()),
    }
}
