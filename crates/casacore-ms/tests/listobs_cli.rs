// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};
use std::process::Command;

use casacore_ms::column_def::{ColumnDef, ColumnKind};
use casacore_ms::schema;
use casacore_ms::{MeasurementSet, MeasurementSetBuilder, SubtableId};
use casacore_types::{
    ArrayD, ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use tempfile::tempdir;

#[test]
fn listobs_help_mentions_core_options() {
    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .arg("--help")
        .output()
        .expect("run listobs --help");
    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(stdout.contains("Usage:"));
    assert!(stdout.contains("--format <FORMAT>"));
    assert!(stdout.contains("--output <PATH>"));
    assert!(stdout.contains("--listfile <PATH>"));
    assert!(stdout.contains("--field <EXPR>"));
    assert!(stdout.contains("--timerange <EXPR>"));
    assert!(stdout.contains("--uvrange <EXPR>"));
    assert!(stdout.contains("--correlation <EXPR>"));
    assert!(stdout.contains("--intent <EXPR>"));
    assert!(stdout.contains("--listunfl"));
    assert!(stdout.contains("--no-verbose"));
    assert!(stdout.contains("--overwrite"));
}

#[test]
fn listobs_json_matches_msinfo_alias() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let listobs = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(listobs.status.success(), "{listobs:?}");

    let msinfo = Command::new(env!("CARGO_BIN_EXE_msinfo"))
        .args(["--format", "json"])
        .arg(&ms_path)
        .output()
        .expect("run msinfo");
    assert!(msinfo.status.success(), "{msinfo:?}");
    assert_eq!(listobs.stdout, msinfo.stdout);

    let json: serde_json::Value = serde_json::from_slice(&listobs.stdout).expect("parse json");
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["measurement_set"]["row_count"], 2);
    assert_eq!(json["observations"][0]["telescope_name"], "VLA");
    assert_eq!(json["fields"][0]["name"], "3C286");
}

#[test]
fn listobs_no_verbose_uses_compact_antenna_listing() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .arg("--no-verbose")
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(output.status.success(), "{output:?}");

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(stdout.contains("Observation: VLA(2 antennas)"));
    assert!(stdout.contains("Antennas: 2 'name'='station'"));
    assert!(!stdout.contains("Sources:"));
    assert!(!stdout.contains("ObservationID ="));
}

#[test]
fn listobs_field_selection_filters_json_summary() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--field", "SECOND*"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(output.status.success(), "{output:?}");

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("parse json");
    assert_eq!(json["measurement_set"]["row_count"], 1);
    assert_eq!(json["fields"].as_array().unwrap().len(), 1);
    assert_eq!(json["fields"][0]["name"], "SECOND");
    assert_eq!(json["scans"][0]["scan_number"], 2);
}

#[test]
fn listobs_rejects_selection_when_selectdata_is_disabled() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--no-selectdata", "--field", "0"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(stderr.contains("selectdata"));
}

#[test]
fn listobs_correlation_selection_filters_json_summary() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--correlation", "XX"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(output.status.success(), "{output:?}");

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("parse json");
    assert_eq!(json["measurement_set"]["row_count"], 1);
    assert_eq!(json["data_descriptions"].as_array().unwrap().len(), 1);
    assert_eq!(json["data_descriptions"][0]["data_description_id"], 0);
}

#[test]
fn listobs_intent_selection_filters_json_summary() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--intent", "CALIBRATE*"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(output.status.success(), "{output:?}");

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("parse json");
    assert_eq!(json["measurement_set"]["row_count"], 1);
    assert_eq!(
        json["scans"][0]["scan_intents"][0],
        "CALIBRATE_PHASE.ON_SOURCE"
    );
}

#[test]
fn listobs_timerange_selection_filters_json_summary() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--timerange", "11:06:50"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(output.status.success(), "{output:?}");

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("parse json");
    assert_eq!(json["measurement_set"]["row_count"], 1);
    assert_eq!(json["scans"][0]["scan_number"], 2);
}

#[test]
fn listobs_uvrange_selection_filters_json_summary() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--uvrange", "0~100m"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(output.status.success(), "{output:?}");

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("parse json");
    assert_eq!(json["measurement_set"]["row_count"], 1);
    assert_eq!(json["scans"][0]["scan_number"], 1);
}

#[test]
fn listobs_listunfl_includes_unflagged_counts() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--listunfl"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(output.status.success(), "{output:?}");

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).expect("parse json");
    assert_eq!(json["scans"][0]["unflagged_row_count"], 0.75);
    assert_eq!(json["scans"][1]["unflagged_row_count"], 0.0);

    let text = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--listunfl"])
        .arg(&ms_path)
        .output()
        .expect("run text listobs");
    assert!(text.status.success(), "{text:?}");
    let stdout = String::from_utf8(text.stdout).expect("utf8 stdout");
    assert!(stdout.contains("nUnflRows"));
    assert!(stdout.contains("0.75"));
}

#[test]
fn listobs_output_file_respects_overwrite_flag() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());
    let output_path = temp.path().join("summary.json");

    let first = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--output"])
        .arg(&output_path)
        .arg(&ms_path)
        .output()
        .expect("first listobs run");
    assert!(first.status.success(), "{first:?}");
    assert!(output_path.is_file());

    let second = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--output"])
        .arg(&output_path)
        .arg(&ms_path)
        .output()
        .expect("second listobs run");
    assert!(!second.status.success());
    let stderr = String::from_utf8(second.stderr).expect("utf8 stderr");
    assert!(stderr.contains("--overwrite"));

    let third = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--format", "json", "--overwrite", "--output"])
        .arg(&output_path)
        .arg(&ms_path)
        .output()
        .expect("third listobs run");
    assert!(third.status.success(), "{third:?}");
}

#[test]
fn listobs_feed_selector_is_rejected_explicitly() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--feed", "0"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(stderr.contains("feed"));
    assert!(stderr.contains("not implemented"));
}

#[test]
fn listobs_cachesize_is_rejected_until_backed_by_real_cache_control() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_listobs"))
        .args(["--cachesize", "50"])
        .arg(&ms_path)
        .output()
        .expect("run listobs");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    assert!(stderr.contains("cachesize"));
    assert!(stderr.contains("cache"));
}

fn create_fixture_ms(root: &Path) -> PathBuf {
    let ms_path = root.join("listobs_fixture.ms");
    let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).expect("create MS");
    add_observation_row(&mut ms, 4_981_000_000.0, 4_981_000_030.0);
    add_field_row(&mut ms, "3C286", "C", 0, 4_981_000_000.0, [1.234, 0.456]);
    add_field_row(&mut ms, "SECOND", "S", 1, 4_981_000_015.0, [1.334, 0.556]);
    add_state_row(&mut ms, "CALIBRATE_PHASE.ON_SOURCE");
    add_state_row(&mut ms, "TARGET.ON_SOURCE");
    add_spectral_window_row(&mut ms, "SPW0", 1.4e9);
    add_spectral_window_row(&mut ms, "SPW1", 2.8e9);
    add_polarization_row(&mut ms, &[9, 12]);
    add_polarization_row(&mut ms, &[5, 8]);
    add_data_description_row(&mut ms, 0, 0);
    add_data_description_row(&mut ms, 1, 1);
    add_antenna_rows(&mut ms);
    add_main_row(&mut ms, 4_981_000_000.0, 1, 0, 1, 0, 0, [30.0, 40.0, 0.0]);
    add_main_row(&mut ms, 4_981_000_015.0, 0, 1, 2, 1, 1, [300.0, 400.0, 0.0]);
    set_main_row_flag_matrix(
        &mut ms,
        0,
        ArrayD::from_shape_vec(vec![2, 2], vec![false, false, false, true]).unwrap(),
    );
    set_main_row_flag_matrix(
        &mut ms,
        1,
        ArrayD::from_shape_vec(vec![2, 2], vec![true, true, true, true]).unwrap(),
    );
    ms.save().expect("save MS");
    ms_path
}

fn add_observation_row(ms: &mut MeasurementSet, start: f64, end: f64) {
    let table = ms
        .subtable_mut(SubtableId::Observation)
        .expect("OBSERVATION table");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "LOG",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["log".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "OBSERVER",
                Value::Scalar(ScalarValue::String("TESTER".to_string())),
            ),
            RecordField::new(
                "PROJECT",
                Value::Scalar(ScalarValue::String("CASA-RS".to_string())),
            ),
            RecordField::new("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(end))),
            RecordField::new(
                "SCHEDULE",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["default".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "SCHEDULE_TYPE",
                Value::Scalar(ScalarValue::String("standard".to_string())),
            ),
            RecordField::new(
                "TELESCOPE_NAME",
                Value::Scalar(ScalarValue::String("VLA".to_string())),
            ),
            RecordField::new(
                "TIME_RANGE",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![2], vec![start, end]).unwrap(),
                )),
            ),
        ]))
        .unwrap();
}

fn add_field_row(
    ms: &mut MeasurementSet,
    name: &str,
    code: &str,
    source_id: i32,
    time: f64,
    direction_pair: [f64; 2],
) {
    let table = ms.subtable_mut(SubtableId::Field).expect("FIELD table");
    let direction =
        ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], direction_pair.to_vec()).unwrap());
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("CODE", Value::Scalar(ScalarValue::String(code.to_string()))),
            RecordField::new("DELAY_DIR", Value::Array(direction.clone())),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
            RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("PHASE_DIR", Value::Array(direction.clone())),
            RecordField::new("REFERENCE_DIR", Value::Array(direction)),
            RecordField::new("SOURCE_ID", Value::Scalar(ScalarValue::Int32(source_id))),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
        ]))
        .unwrap();
}

fn add_spectral_window_row(ms: &mut MeasurementSet, name: &str, ref_frequency_hz: f64) {
    let table = ms
        .subtable_mut(SubtableId::SpectralWindow)
        .expect("SPECTRAL_WINDOW table");
    let freq = ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![1.4e9, 1.401e9]).unwrap());
    let width = ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap());
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("CHAN_FREQ", Value::Array(freq)),
            RecordField::new("CHAN_WIDTH", Value::Array(width.clone())),
            RecordField::new("EFFECTIVE_BW", Value::Array(width.clone())),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "FREQ_GROUP_NAME",
                Value::Scalar(ScalarValue::String("GROUP0".to_string())),
            ),
            RecordField::new("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
            RecordField::new("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new(
                "REF_FREQUENCY",
                Value::Scalar(ScalarValue::Float64(ref_frequency_hz)),
            ),
            RecordField::new("RESOLUTION", Value::Array(width.clone())),
            RecordField::new(
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(2.0e6)),
            ),
        ]))
        .unwrap();
}

fn add_polarization_row(ms: &mut MeasurementSet, corr_types: &[i32]) {
    let table = ms
        .subtable_mut(SubtableId::Polarization)
        .expect("POLARIZATION table");
    let corr_product = match corr_types.len() {
        2 => vec![0, 1, 0, 1],
        4 => vec![0, 0, 1, 1, 0, 1, 0, 1],
        len => vec![0; len * 2],
    };
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "CORR_PRODUCT",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![2, corr_types.len()], corr_product).unwrap(),
                )),
            ),
            RecordField::new(
                "CORR_TYPE",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![corr_types.len()], corr_types.to_vec()).unwrap(),
                )),
            ),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "NUM_CORR",
                Value::Scalar(ScalarValue::Int32(corr_types.len() as i32)),
            ),
        ]))
        .unwrap();
}

fn add_data_description_row(ms: &mut MeasurementSet, polarization_id: i32, spw_id: i32) {
    let table = ms
        .subtable_mut(SubtableId::DataDescription)
        .expect("DATA_DESCRIPTION table");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "POLARIZATION_ID",
                Value::Scalar(ScalarValue::Int32(polarization_id)),
            ),
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::Int32(spw_id)),
            ),
        ]))
        .unwrap();
}

fn add_state_row(ms: &mut MeasurementSet, obs_mode: &str) {
    let table = ms.subtable_mut(SubtableId::State).expect("STATE table");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("CAL", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("LOAD", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new(
                "OBS_MODE",
                Value::Scalar(ScalarValue::String(obs_mode.to_string())),
            ),
            RecordField::new("REF", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("SIG", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("SUB_SCAN", Value::Scalar(ScalarValue::Int32(0))),
        ]))
        .unwrap();
}

fn add_antenna_rows(ms: &mut MeasurementSet) {
    let mut antenna = ms.antenna_mut().expect("ANTENNA accessor");
    antenna
        .add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [0.0, 10.0, 20.0],
            [0.0, 0.0, 0.0],
            25.0,
        )
        .unwrap();
    antenna
        .add_antenna(
            "VLA02",
            "N02",
            "GROUND-BASED",
            "ALT-AZ",
            [1.0, 11.0, 21.0],
            [0.0, 0.0, 0.0],
            25.0,
        )
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
fn add_main_row(
    ms: &mut MeasurementSet,
    time: f64,
    antenna2: i32,
    field_id: i32,
    scan_number: i32,
    data_desc_id: i32,
    state_id: i32,
    uvw: [f64; 3],
) {
    let schema = ms.main_table().schema().unwrap().clone();
    let fields = schema
        .columns()
        .iter()
        .map(|column| match column.name() {
            "ANTENNA1" => RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
            "ANTENNA2" => RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2))),
            "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
            "DATA_DESC_ID" => RecordField::new(
                "DATA_DESC_ID",
                Value::Scalar(ScalarValue::Int32(data_desc_id)),
            ),
            "EXPOSURE" => RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(15.0))),
            "FIELD_ID" => RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id))),
            "INTERVAL" => RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(15.0))),
            "OBSERVATION_ID" => {
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
            }
            "SCAN_NUMBER" => RecordField::new(
                "SCAN_NUMBER",
                Value::Scalar(ScalarValue::Int32(scan_number)),
            ),
            "STATE_ID" => RecordField::new("STATE_ID", Value::Scalar(ScalarValue::Int32(state_id))),
            "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
            "TIME_CENTROID" => {
                RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
            }
            "UVW" => RecordField::new(
                "UVW",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], uvw.to_vec()).unwrap(),
                )),
            ),
            name => RecordField::new(name, default_value_for_def(main_column_def(name))),
        })
        .collect::<Vec<_>>();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();
}

fn set_main_row_flag_matrix(ms: &mut MeasurementSet, row: usize, flags: ArrayD<bool>) {
    ms.main_table_mut()
        .set_cell(row, "FLAG", Value::Array(ArrayValue::Bool(flags)))
        .unwrap();
}

fn main_column_def(name: &str) -> &'static ColumnDef {
    schema::main_table::REQUIRED_COLUMNS
        .iter()
        .find(|column| column.name == name)
        .expect("required main column definition")
}

fn default_value_for_def(column: &ColumnDef) -> Value {
    match column.column_kind {
        ColumnKind::Scalar => match column.data_type {
            PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            PrimitiveType::UInt8 => Value::Scalar(ScalarValue::UInt8(0)),
            PrimitiveType::UInt16 => Value::Scalar(ScalarValue::UInt16(0)),
            PrimitiveType::UInt32 => Value::Scalar(ScalarValue::UInt32(0)),
            PrimitiveType::Int16 => Value::Scalar(ScalarValue::Int16(0)),
            PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            PrimitiveType::Int64 => Value::Scalar(ScalarValue::Int64(0)),
            PrimitiveType::Float32 => Value::Scalar(ScalarValue::Float32(0.0)),
            PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            PrimitiveType::Complex32 => Value::Scalar(ScalarValue::Complex32(Default::default())),
            PrimitiveType::Complex64 => Value::Scalar(ScalarValue::Complex64(Default::default())),
            PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
        },
        ColumnKind::FixedArray { shape } => default_array_value(column.data_type, shape.to_vec()),
        ColumnKind::VariableArray { ndim } => default_array_value(column.data_type, vec![1; ndim]),
    }
}

fn default_array_value(data_type: PrimitiveType, shape: Vec<usize>) -> Value {
    let total = shape.iter().product();
    let array = match data_type {
        PrimitiveType::Bool => {
            ArrayValue::Bool(ArrayD::from_shape_vec(shape, vec![false; total]).unwrap())
        }
        PrimitiveType::UInt8 => {
            ArrayValue::UInt8(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::UInt16 => {
            ArrayValue::UInt16(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::UInt32 => {
            ArrayValue::UInt32(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::Int16 => {
            ArrayValue::Int16(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::Int32 => {
            ArrayValue::Int32(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::Int64 => {
            ArrayValue::Int64(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::Float32 => {
            ArrayValue::Float32(ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap())
        }
        PrimitiveType::Float64 => {
            ArrayValue::Float64(ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap())
        }
        PrimitiveType::Complex32 => ArrayValue::Complex32(
            ArrayD::from_shape_vec(shape, vec![Default::default(); total]).unwrap(),
        ),
        PrimitiveType::Complex64 => ArrayValue::Complex64(
            ArrayD::from_shape_vec(shape, vec![Default::default(); total]).unwrap(),
        ),
        PrimitiveType::String => {
            ArrayValue::String(ArrayD::from_shape_vec(shape, vec![String::new(); total]).unwrap())
        }
    };
    Value::Array(array)
}
