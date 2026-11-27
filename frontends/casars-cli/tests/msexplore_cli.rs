// SPDX-License-Identifier: LGPL-3.0-or-later

#[path = "../../../libs/casacore/casa-ms/tests/common/mod.rs"]
mod common;

use std::f64::consts::PI;
use std::path::Path;
use std::process::Command;

use casa_core::ArrayValue;
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::frequency::FrequencyRef;
use casa_ms::columns::frequency_columns::ChanFreqColumn;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::subtables::SubTable;
use casa_ms::{
    ListObsUvCoverage as MeasurementSetUvCoverage, MeasurementSet,
    MeasurementSetSummaryOutputFormat, MsSelection,
};
use casa_plot_data::msexplore::{
    DEFAULT_MAX_PLOT_POINTS, MsAxis, MsColorAxis, MsDataColumn, MsExploreSpec, MsFlagAction,
    MsFlagEditSpec, MsFlagRegion, MsIterationAxis, MsLayoutSpec, MsLegendPosition,
    MsPageExportRange, MsPageHeaderItem, MsPlotPayload, MsPlotPreset, MsPlotSpec,
    apply_msexplore_flag_edit, apply_msexplore_flag_edit_for_request, build_msexplore_payload,
    build_msexplore_payload_from_spec, build_msexplore_plot_payload, export_msexplore_plot,
    preview_msexplore_flag_edit, preview_msexplore_flag_edit_for_request,
};
use casa_plot_data::{
    MeasurementSetPlotKind, MeasurementSetPlotSpec,
    build_listobs_uv_plot_payload as build_measurement_set_uv_plot_payload,
};
use casa_tasks::ms::msexplore::task_contract::{
    MSEXPLORE_TASK_PROTOCOL_NAME, MsExploreFlagEditRequest, MsExploreRunTaskRequest,
};
use common::{
    TIME_BASE_SECONDS, create_msexplore_averaging_fixture_ms, create_msexplore_fixture_ms,
    create_msexplore_fixture_ms_with_flags, create_msexplore_geometry_fixture_ms,
    create_msexplore_spectrum_fixture_ms,
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
    for flag in [
        "--preset PRESET",
        "--xaxis AXIS",
        "--avgtime",
        "--flag-action",
        "--flag-output PATH",
        "--plot-output PATH",
        "--page-spec PATH",
        "--json-schema",
        "--protocol-info",
        "--json-run <SOURCE>",
    ] {
        assert!(stdout.contains(flag), "help omits {flag}:\n{stdout}");
    }
}

#[test]
fn msexplore_decimated_payload_samples_rows_before_large_array_reads() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let payload = build_msexplore_payload_from_spec(&MsExploreSpec {
        ms_path,
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: MsSelection::default(),
        header_items: Vec::new(),
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points: 1,
        plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsChannel)],
    })
    .expect("payload");

    let MsPlotPayload::Scatter(payload) = payload else {
        panic!("expected scatter payload");
    };
    assert!(payload.summary.contains("Sampled rows=1/"));
    assert_eq!(
        payload
            .series
            .iter()
            .map(|series| series.points.len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn a_value_msexplore_refuses_names_its_parameter() {
    // Astra's #962 F5: `--avgchannel -1` printed only `invalid numeric
    // value "-1"`; the old parser named the flag.
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let ms = ms_path.to_str().expect("UTF-8 MS path");
    let refused = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([ms, "--avgchannel", "-1", "--no-save-last"])
        .output()
        .expect("run msexplore");
    assert_eq!(refused.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("parameter \"avgchannel\""), "{stderr}");

    let request = temp.path().join("request.json");
    let values = serde_json::json!({
        "surface": "msexplore",
        "values": {"vis": ms, "avgchannel": -1},
    });
    std::fs::write(&request, values.to_string()).expect("write request");
    let replied = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(["--json-run", request.to_str().expect("UTF-8 path")])
        .output()
        .expect("run msexplore");
    assert_eq!(replied.status.code(), Some(2));
    let reply: serde_json::Value = serde_json::from_slice(&replied.stdout).expect("reply");
    assert_eq!(reply["kind"], "validation", "{reply}");
    assert_eq!(reply["parameter"], "avgchannel", "{reply}");
}

#[test]
fn msexplore_run_task_writes_summary_and_flag_preview_artifacts() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let summary_output = temp.path().join("summary.txt");
    let flag_output = temp.path().join("flag-preview.json");
    let request = MsExploreRunTaskRequest {
        spec: MsExploreSpec {
            ms_path,
            summary_format: MeasurementSetSummaryOutputFormat::Text,
            selection: MsSelection::default(),
            header_items: Vec::new(),
            page_title: None,
            exprange: MsPageExportRange::Current,
            max_plot_points: DEFAULT_MAX_PLOT_POINTS,
            plots: vec![MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime)],
        },
        summary_output_path: Some(summary_output.clone()),
        overwrite_outputs: false,
        flag_edit: Some(MsExploreFlagEditRequest {
            edit: MsFlagEditSpec {
                action: MsFlagAction::Flag,
                region: MsFlagRegion {
                    x_min: TIME_BASE_SECONDS - 1.0,
                    x_max: TIME_BASE_SECONDS + 100.0,
                    y_min: -10.0,
                    y_max: 10.0,
                },
                plot_index: None,
                panel_key: None,
                extcorr: true,
                extchannel: true,
            },
            apply: false,
            output_path: Some(flag_output.clone()),
        }),
        plot_export: None,
    };

    let result = request.execute().expect("run msexplore task request");
    assert_eq!(result.summary_output_path, Some(summary_output.clone()));
    assert_eq!(result.flag_edit_output_path, Some(flag_output.clone()));
    assert!(result.summary.measurement_set.row_count > 0);
    assert!(
        result
            .flag_edit_preview
            .as_ref()
            .expect("flag preview")
            .affected_samples
            > 0
    );
    assert!(
        std::fs::read_to_string(summary_output)
            .expect("summary output")
            .contains("MeasurementSet")
    );
    assert!(
        std::fs::read_to_string(flag_output)
            .expect("flag preview output")
            .contains("affected_samples")
    );
}

#[test]
fn each_hosted_surface_lists_every_parameter_in_its_help() {
    // The help is generated from the surface the first word selects, so
    // every parameter's own spelling appears in it.
    for surface in ["msexplore", "plotms"] {
        let help = Command::new(env!("CARGO_BIN_EXE_msexplore"))
            .args([surface, "--help"])
            .output()
            .expect("run msexplore --help");
        assert!(help.status.success());
        let help = String::from_utf8(help.stdout).expect("utf8 help stdout");
        assert!(
            help.contains(&format!("Parameters of {surface}:")),
            "{help}"
        );
        let bundle = casa_contracts::builtin_surface_bundle(surface).unwrap();
        for binding in bundle.surface.bindings() {
            let cli = binding.projections.cli.as_ref().expect("CLI projection");
            let spelling = match cli.flags.first() {
                Some(flag) => flag.clone(),
                None => binding.name.to_uppercase(),
            };
            assert!(help.contains(&spelling), "{surface} help omits {spelling}");
        }
    }
}

#[test]
fn msexplore_json_schema_emits_canonical_task_bundle() {
    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .arg("--json-schema")
        .output()
        .expect("run msexplore --json-schema");
    assert!(output.status.success());

    let schema = serde_json::from_slice::<serde_json::Value>(&output.stdout).expect("parse schema");
    assert_eq!(
        schema["protocol"]["protocol_name"],
        MSEXPLORE_TASK_PROTOCOL_NAME
    );
    assert_eq!(schema["protocol"]["protocol_version"], 2);
    assert_eq!(schema["protocol"]["surface_kind"], "task");
    assert_eq!(schema["semantic"]["operations"][0]["request_kind"], "run");
    assert_eq!(
        schema["parameter_surfaces"][0]["surface"]["id"],
        "msexplore"
    );
}

#[test]
fn msexplore_protocol_info_reports_task_surface() {
    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .arg("--protocol-info")
        .output()
        .expect("run msexplore --protocol-info");
    assert!(output.status.success());

    let info = serde_json::from_slice::<serde_json::Value>(&output.stdout).expect("parse info");
    assert_eq!(info["protocol_name"], MSEXPLORE_TASK_PROTOCOL_NAME);
    assert_eq!(info["protocol_version"], 2);
    assert_eq!(info["surface_kind"], "task");
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
fn msexplore_decimates_exports_that_exceed_max_points() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    let plot_path = temp.path().join("too-many-points.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_time",
            "--max-points",
            "10",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");
    let manifest = std::fs::read_to_string(&plot_path).expect("read manifest");
    assert!(manifest.contains("Downsampled plot"), "{manifest}");
    let plotted_rows = manifest
        .lines()
        .filter(|line| !line.is_empty())
        .filter(|line| !line.starts_with('#'))
        .filter(|line| !line.starts_with("series_key\t"))
        .count();
    assert_eq!(plotted_rows, 10, "{manifest}");
}

#[test]
fn msexplore_avgscan_requires_avgtime() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_averaging_fixture_ms(temp.path());
    let plot_path = temp.path().join("avgscan-requires-avgtime.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_time",
            "--avgscan",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(stderr.contains("avgscan"));
    assert!(stderr.contains("avgtime"));
}

#[test]
fn msexplore_rejects_mutually_exclusive_baseline_and_antenna_averaging() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_averaging_fixture_ms(temp.path());
    let plot_path = temp.path().join("avgbaseline-avgantenna-invalid.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_time",
            "--avgbaseline",
            "--avgantenna",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(stderr.contains("avgbaseline"));
    assert!(stderr.contains("avgantenna"));
}

#[test]
fn msexplore_avgtime_manifest_collapses_rows_within_time_bins() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_averaging_fixture_ms(temp.path());
    let plot_path = temp.path().join("avgtime.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_time",
            "--avgtime",
            "60",
            "--plot-output",
        ])
        .arg(&plot_path)
        .args(["--plot-format", "txt"])
        .arg(&ms_path)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");

    let rows = manifest_rows(&plot_path);
    assert_eq!(rows.len(), 384);
}

#[test]
fn msexplore_avgtime_with_scan_and_field_averaging_collapses_boundaries() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_averaging_fixture_ms(temp.path());
    let plot_path = temp.path().join("avgtime-scan-field.txt");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args([
            "--preset",
            "amplitude_vs_time",
            "--color-by",
            "none",
            "--avgtime",
            "60",
            "--avgscan",
            "--avgfield",
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
}

#[test]
fn msexplore_baseline_antenna_and_spw_averaging_reduce_point_counts() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_averaging_fixture_ms(temp.path());

    let run_manifest_count = |args: &[&str], output_name: &str| -> usize {
        let plot_path = temp.path().join(output_name);
        let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
            .args(args)
            .arg(&plot_path)
            .args(["--plot-format", "txt"])
            .arg(&ms_path)
            .output()
            .expect("run msexplore");
        assert!(output.status.success(), "{output:?}");
        manifest_rows(&plot_path).len()
    };

    let avgbaseline = run_manifest_count(
        &[
            "--preset",
            "amplitude_vs_time",
            "--avgbaseline",
            "--plot-output",
        ],
        "avgbaseline.txt",
    );
    let avgantenna = run_manifest_count(
        &[
            "--preset",
            "amplitude_vs_time",
            "--avgantenna",
            "--plot-output",
        ],
        "avgantenna.txt",
    );
    let avgspw = run_manifest_count(
        &["--preset", "amplitude_vs_time", "--avgspw", "--plot-output"],
        "avgspw.txt",
    );

    assert_eq!(avgbaseline, 384);
    assert_eq!(avgantenna, 896);
    assert_eq!(avgspw, 320);
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
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.iteration.iteraxis = Some(MsIterationAxis::Scan);
    spec.iteration.xselfscale = true;

    let payload = build_msexplore_plot_payload(&ms, &MsSelection::default(), &spec)
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
fn msexplore_direct_plot_builder_does_not_use_legacy_averaging_for_geometry() {
    let temp = tempdir().expect("tempdir");
    let path = create_msexplore_averaging_fixture_ms(temp.path());
    let ms = MeasurementSet::open(&path).expect("open fixture");
    let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
    spec.averaging.avgtime = Some(20.0);
    for axis in [MsAxis::Interval, MsAxis::UWave] {
        spec.x_axis = axis;
        let error = build_msexplore_plot_payload(&ms, &MsSelection::default(), &spec)
            .expect_err("time-averaged geometry requires the PlotMS iterator");
        assert!(error.contains("with a callibrary or direct msexplore are pending"));
    }
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

fn page_manifest_rows(path: &Path) -> Vec<PageManifestRow> {
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

type PageManifestRow = (
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
);

fn manifest_header_value<'a>(manifest: &'a str, key: &str) -> Option<&'a str> {
    manifest
        .lines()
        .find_map(|line| line.strip_prefix(&format!("# {key}=")))
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

#[path = "msexplore_cli_cases/flag_edits.rs"]
mod flag_edits;
#[path = "msexplore_cli_cases/pages.rs"]
mod pages;
