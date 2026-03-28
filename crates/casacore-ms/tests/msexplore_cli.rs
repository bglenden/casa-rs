// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use casacore_ms::listobs::cli::{UiArgumentParser, UiValueKind};
use casacore_ms::msexplore::cli::command_schema;
use casacore_ms::schema::main_table::OptionalMainColumn;
use casacore_ms::{MeasurementSet, MeasurementSetBuilder};
use casacore_types::{ArrayValue, ScalarValue, Value};
use common::{NUM_CHAN, NUM_CORR, TIME_BASE_SECONDS, make_vis_data, populate_subtables};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
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
    let ms_path = create_fixture_ms(temp.path());
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
    let ms_path = create_fixture_ms(temp.path());
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
    let ms_path = create_fixture_ms(temp.path());
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
fn msexplore_msselect_filters_summary_rows() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

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

fn create_fixture_ms(root: &Path) -> PathBuf {
    let ms_path = root.join("msexplore_fixture.ms");
    let mut ms = MeasurementSet::create(
        &ms_path,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create MS");
    populate_subtables(&mut ms);

    for row in 0..4usize {
        common::add_main_row(
            &mut ms,
            &[
                ("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                ("ANTENNA2", Value::Scalar(ScalarValue::Int32(1))),
                ("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS + row as f64)),
                ),
                (
                    "TIME_CENTROID",
                    Value::Scalar(ScalarValue::Float64(TIME_BASE_SECONDS + row as f64)),
                ),
                ("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0))),
                ("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0))),
                (
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(row as i32 + 1)),
                ),
                (
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![30.0 + row as f64, 40.0, 0.0])
                            .unwrap(),
                    )),
                ),
                ("DATA", Value::Array(make_vis_data(row))),
                (
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(
                            IxDyn(&[NUM_CORR, NUM_CHAN]).f(),
                            vec![false; NUM_CORR * NUM_CHAN],
                        )
                        .unwrap(),
                    )),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
    }
    ms.save().expect("save MS");
    ms_path
}
