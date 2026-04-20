// SPDX-License-Identifier: LGPL-3.0-or-later

#![cfg(feature = "cpp-interop-tests")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use casa_ms::SubTable;
use casa_ms::ms::MeasurementSet;
use casa_tables::Table;
use casa_test_support::discover_casa_python;
use casa_types::{ArrayValue, Complex32, Complex64, RecordValue, ScalarValue, Value};
use casa_vla::{ImportVlaOptions, import_archive_files_to_measurement_set_from_options};
use tempfile::TempDir;

const DEFAULT_FREQUENCY_TOLERANCE_HZ: f64 = 150_000.0;
const FLOAT_TOLERANCE: f64 = 1.0e-5;
const SPW_FLOAT_TOLERANCE_HZ: f64 = 1.0e-2;

struct ImportedMeasurementSets {
    _tempdir: TempDir,
    rust: MeasurementSet,
    casa: MeasurementSet,
}

fn locate_archive(candidates: &[&'static str]) -> Option<PathBuf> {
    candidates
        .iter()
        .map(PathBuf::from)
        .find(|path| path.exists())
}

fn xp1_path() -> Option<PathBuf> {
    locate_archive(&[
        "/Volumes/home/casatestdata/unittest/importvla/AS758_C030425.xp1",
        "/Users/brianglendenning/SoftwareProjects/casatestdata/unittest/importvla/AS758_C030425.xp1",
        "/Volumes/home/casatestdata/other/AS758_C030425.xp1",
    ])
}

fn xp5_path() -> Option<PathBuf> {
    locate_archive(&[
        "/Volumes/home/casatestdata/unittest/importvla/AS758_C030426.xp5",
        "/Users/brianglendenning/SoftwareProjects/casatestdata/unittest/importvla/AS758_C030426.xp5",
        "/Volumes/home/casatestdata/other/AS758_C030426.xp5",
    ])
}

fn ag189_path() -> Option<PathBuf> {
    locate_archive(&[
        "/Users/brianglendenning/Desktop/AG189/observation.46325.2302894/AG189_1_46325.23029_46325.80807.exp",
        "/Users/brianglendenning/Desktop/AG189/observation.46182.7646759/AG189_1_46182.76468_46183.09488.exp",
        "/Users/brianglendenning/Desktop/AG189/observation.46673.4830671/AG189_1_46673.48307_46673.81374.exp",
    ])
}

fn get_f64_scalar(row: &casa_types::RecordValue, column: &str) -> f64 {
    match row.get(column).expect("missing scalar column") {
        Value::Scalar(ScalarValue::Float64(value)) => *value,
        other => panic!("{column} had unexpected value {other:?}"),
    }
}

fn get_i32_scalar(row: &casa_types::RecordValue, column: &str) -> i32 {
    match row.get(column).expect("missing scalar column") {
        Value::Scalar(ScalarValue::Int32(value)) => *value,
        other => panic!("{column} had unexpected value {other:?}"),
    }
}

fn get_i32_scalar_or(row: &casa_types::RecordValue, column: &str, default: i32) -> i32 {
    match row.get(column) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
        None => default,
        Some(other) => panic!("{column} had unexpected value {other:?}"),
    }
}

fn get_string_scalar(row: &casa_types::RecordValue, column: &str) -> String {
    match row.get(column).expect("missing scalar column") {
        Value::Scalar(ScalarValue::String(value)) => value.clone(),
        other => panic!("{column} had unexpected value {other:?}"),
    }
}

fn get_f64_array(row: &casa_types::RecordValue, column: &str) -> Vec<f64> {
    match row.get(column).expect("missing array column") {
        Value::Array(ArrayValue::Float64(values)) => values.iter().copied().collect(),
        other => panic!("{column} had unexpected value {other:?}"),
    }
}

fn assert_close(left: f64, right: f64, tolerance: f64, label: &str) {
    assert!(
        (left - right).abs() <= tolerance,
        "{label} mismatch: left={left} right={right} diff={}",
        left - right
    );
}

fn assert_array_close(left: &[f64], right: &[f64], tolerance: f64, label: &str) {
    assert_eq!(left.len(), right.len(), "{label} length mismatch");
    for (index, (&left_value, &right_value)) in left.iter().zip(right.iter()).enumerate() {
        assert_close(
            left_value,
            right_value,
            tolerance,
            &format!("{label}[{index}]"),
        );
    }
}

fn compare_table_rows(name: &str, rust: &Table, casa: &Table, columns: &[&str]) {
    assert_eq!(rust.row_count(), casa.row_count(), "{name} row count");
    for row_index in 0..rust.row_count() {
        let rust_row = rust.row(row_index).expect("read Rust row");
        let casa_row = casa.row(row_index).expect("read CASA row");
        compare_record_subset(name, row_index, rust_row, casa_row, columns);
    }
}

fn compare_record_subset(
    table_name: &str,
    row_index: usize,
    rust_row: &RecordValue,
    casa_row: &RecordValue,
    columns: &[&str],
) {
    for &column in columns {
        let rust_value = rust_row
            .get(column)
            .unwrap_or_else(|| panic!("{table_name}[{row_index}] missing Rust column {column}"));
        let casa_value = casa_row
            .get(column)
            .unwrap_or_else(|| panic!("{table_name}[{row_index}] missing CASA column {column}"));
        assert_value_close(
            &format!("{table_name}[{row_index}].{column}"),
            rust_value,
            casa_value,
            FLOAT_TOLERANCE,
        );
    }
}

fn assert_value_close(label: &str, rust: &Value, casa: &Value, tolerance: f64) {
    match (rust, casa) {
        (Value::Scalar(rust_scalar), Value::Scalar(casa_scalar)) => {
            assert_scalar_close(label, rust_scalar, casa_scalar, tolerance);
        }
        (Value::Array(rust_array), Value::Array(casa_array)) => {
            assert_general_array_close(label, rust_array, casa_array, tolerance);
        }
        _ => panic!("{label}: value kind mismatch: rust={rust:?} casa={casa:?}"),
    }
}

fn assert_scalar_close(label: &str, rust: &ScalarValue, casa: &ScalarValue, tolerance: f64) {
    let tolerance = tolerance_for_label(label, tolerance);
    match (rust, casa) {
        (ScalarValue::Bool(rust), ScalarValue::Bool(casa)) => assert_eq!(rust, casa, "{label}"),
        (ScalarValue::Int32(rust), ScalarValue::Int32(casa)) => assert_eq!(rust, casa, "{label}"),
        (ScalarValue::Float32(rust), ScalarValue::Float32(casa)) => {
            assert!(
                (f64::from(*rust) - f64::from(*casa)).abs() <= tolerance,
                "{label}: rust={rust} casa={casa}"
            );
        }
        (ScalarValue::Float64(rust), ScalarValue::Float64(casa)) => {
            assert!(
                (rust - casa).abs() <= tolerance,
                "{label}: rust={rust} casa={casa}"
            );
        }
        (ScalarValue::Complex32(rust), ScalarValue::Complex32(casa)) => {
            assert_complex32_close(label, *rust, *casa, tolerance);
        }
        (ScalarValue::Complex64(rust), ScalarValue::Complex64(casa)) => {
            assert_complex64_close(label, *rust, *casa, tolerance);
        }
        (ScalarValue::String(rust), ScalarValue::String(casa)) => assert_eq!(rust, casa, "{label}"),
        _ => panic!("{label}: scalar type mismatch: rust={rust:?} casa={casa:?}"),
    }
}

fn assert_general_array_close(label: &str, rust: &ArrayValue, casa: &ArrayValue, tolerance: f64) {
    let tolerance = tolerance_for_label(label, tolerance);
    match (rust, casa) {
        (ArrayValue::Bool(rust), ArrayValue::Bool(casa)) => {
            assert_eq!(rust.shape(), casa.shape(), "{label} shape");
            assert_eq!(
                rust.iter().copied().collect::<Vec<_>>(),
                casa.iter().copied().collect::<Vec<_>>(),
                "{label}"
            );
        }
        (ArrayValue::Int32(rust), ArrayValue::Int32(casa)) => {
            assert_eq!(rust.shape(), casa.shape(), "{label} shape");
            assert_eq!(
                rust.iter().copied().collect::<Vec<_>>(),
                casa.iter().copied().collect::<Vec<_>>(),
                "{label}"
            );
        }
        (ArrayValue::Float32(rust), ArrayValue::Float32(casa)) => {
            assert_eq!(rust.shape(), casa.shape(), "{label} shape");
            for (index, (rust_value, casa_value)) in rust.iter().zip(casa.iter()).enumerate() {
                assert!(
                    (f64::from(*rust_value) - f64::from(*casa_value)).abs() <= tolerance,
                    "{label}[{index}]: rust={rust_value} casa={casa_value}"
                );
            }
        }
        (ArrayValue::Float64(rust), ArrayValue::Float64(casa)) => {
            assert_eq!(rust.shape(), casa.shape(), "{label} shape");
            for (index, (rust_value, casa_value)) in rust.iter().zip(casa.iter()).enumerate() {
                assert!(
                    (rust_value - casa_value).abs() <= tolerance,
                    "{label}[{index}]: rust={rust_value} casa={casa_value}"
                );
            }
        }
        (ArrayValue::Complex32(rust), ArrayValue::Complex32(casa)) => {
            assert_eq!(rust.shape(), casa.shape(), "{label} shape");
            for (index, (rust_value, casa_value)) in rust.iter().zip(casa.iter()).enumerate() {
                assert_complex32_close(
                    &format!("{label}[{index}]"),
                    *rust_value,
                    *casa_value,
                    tolerance,
                );
            }
        }
        (ArrayValue::Complex64(rust), ArrayValue::Complex64(casa)) => {
            assert_eq!(rust.shape(), casa.shape(), "{label} shape");
            for (index, (rust_value, casa_value)) in rust.iter().zip(casa.iter()).enumerate() {
                assert_complex64_close(
                    &format!("{label}[{index}]"),
                    *rust_value,
                    *casa_value,
                    tolerance,
                );
            }
        }
        (ArrayValue::String(rust), ArrayValue::String(casa)) => {
            assert_eq!(rust.shape(), casa.shape(), "{label} shape");
            assert_eq!(
                rust.iter().cloned().collect::<Vec<_>>(),
                casa.iter().cloned().collect::<Vec<_>>(),
                "{label}"
            );
        }
        _ => panic!("{label}: array type mismatch: rust={rust:?} casa={casa:?}"),
    }
}

fn tolerance_for_label(label: &str, default_tolerance: f64) -> f64 {
    if label.starts_with("SPECTRAL_WINDOW[") {
        return SPW_FLOAT_TOLERANCE_HZ;
    }
    default_tolerance
}

fn assert_complex32_close(label: &str, rust: Complex32, casa: Complex32, tolerance: f64) {
    assert!(
        (f64::from(rust.re) - f64::from(casa.re)).abs() <= tolerance
            && (f64::from(rust.im) - f64::from(casa.im)).abs() <= tolerance,
        "{label}: rust={rust:?} casa={casa:?}"
    );
}

fn assert_complex64_close(label: &str, rust: Complex64, casa: Complex64, tolerance: f64) {
    assert!(
        (rust.re - casa.re).abs() <= tolerance && (rust.im - casa.im).abs() <= tolerance,
        "{label}: rust={rust:?} casa={casa:?}"
    );
}

fn measure_info_string(table: &Table, column: &str, field: &str) -> Option<String> {
    let keywords = table.column_keywords(column)?;
    let Value::Record(measinfo) = keywords.get("MEASINFO")? else {
        panic!("{column} MEASINFO keyword had unexpected shape");
    };
    let Value::Scalar(ScalarValue::String(value)) = measinfo.get(field)? else {
        panic!("{column} MEASINFO.{field} had unexpected shape");
    };
    Some(value.clone())
}

fn quantum_units(table: &Table, column: &str) -> Option<Vec<String>> {
    let keywords = table.column_keywords(column)?;
    let Value::Array(ArrayValue::String(units)) = keywords.get("QuantumUnits")? else {
        panic!("{column} QuantumUnits keyword had unexpected shape");
    };
    Some(units.iter().cloned().collect())
}

fn assert_measure_keyword_matches(
    table_name: &str,
    rust: &Table,
    casa: &Table,
    column: &str,
    measinfo_type: bool,
    measinfo_ref: bool,
    quantum_units_match: bool,
) {
    if measinfo_type {
        assert_eq!(
            measure_info_string(rust, column, "type"),
            measure_info_string(casa, column, "type"),
            "{table_name}.{column} MEASINFO.type"
        );
    }
    if measinfo_ref {
        assert_eq!(
            measure_info_string(rust, column, "Ref"),
            measure_info_string(casa, column, "Ref"),
            "{table_name}.{column} MEASINFO.Ref"
        );
    }
    if quantum_units_match {
        assert_eq!(
            quantum_units(rust, column),
            quantum_units(casa, column),
            "{table_name}.{column} QuantumUnits"
        );
    }
}

fn import_with_rust_and_casa(archive_path: &Path, label: &str) -> Option<ImportedMeasurementSets> {
    let Some(casa_python) = discover_casa_python() else {
        eprintln!("skipping {label}: CASA Python environment not found");
        return None;
    };

    let temp_root = PathBuf::from("target/real-importvla-parity-tmp");
    fs::create_dir_all(&temp_root).expect("create parity temp root");
    let tempdir = TempDir::new_in(&temp_root).expect("create tempdir");
    let rust_ms_path = tempdir.path().join(format!("{label}.rust.ms"));
    let casa_ms_path = tempdir.path().join(format!("{label}.casa.ms"));

    import_archive_files_to_measurement_set_from_options(&ImportVlaOptions {
        archivefiles: vec![archive_path.to_path_buf()],
        vis: Some(rust_ms_path.clone()),
        frequencytol_hz: DEFAULT_FREQUENCY_TOLERANCE_HZ,
        ..ImportVlaOptions::default()
    })
    .expect("import archive into Rust MeasurementSet");

    let output = Command::new(&casa_python.program)
        .arg("-c")
        .arg(
            r#"
from casatasks import importvla
import sys
importvla(archivefiles=[sys.argv[1]], vis=sys.argv[2], frequencytol=150000.0)
"#,
        )
        .arg(archive_path)
        .arg(&casa_ms_path)
        .output()
        .expect("run CASA importvla");
    assert!(
        output.status.success(),
        "CASA importvla failed for {}: status={:?}\nstdout={}\nstderr={}",
        archive_path.display(),
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let rust = MeasurementSet::open(&rust_ms_path).expect("open Rust MeasurementSet");
    let casa = MeasurementSet::open(&casa_ms_path).expect("open CASA MeasurementSet");
    Some(ImportedMeasurementSets {
        _tempdir: tempdir,
        rust,
        casa,
    })
}

fn assert_measurement_set_shape_matches(rust: &MeasurementSet, casa: &MeasurementSet, label: &str) {
    assert_eq!(rust.row_count(), casa.row_count(), "{label} main row count");
    assert_eq!(
        rust.field().expect("Rust FIELD").row_count(),
        casa.field().expect("CASA FIELD").row_count(),
        "{label} FIELD row count"
    );
    assert_eq!(
        rust.observation().expect("Rust OBSERVATION").row_count(),
        casa.observation().expect("CASA OBSERVATION").row_count(),
        "{label} OBSERVATION row count"
    );
    assert_eq!(
        rust.polarization().expect("Rust POLARIZATION").row_count(),
        casa.polarization().expect("CASA POLARIZATION").row_count(),
        "{label} POLARIZATION row count"
    );
    assert_eq!(
        rust.data_description()
            .expect("Rust DATA_DESCRIPTION")
            .row_count(),
        casa.data_description()
            .expect("CASA DATA_DESCRIPTION")
            .row_count(),
        "{label} DATA_DESCRIPTION row count"
    );
    assert_eq!(
        rust.spectral_window()
            .expect("Rust SPECTRAL_WINDOW")
            .row_count(),
        casa.spectral_window()
            .expect("CASA SPECTRAL_WINDOW")
            .row_count(),
        "{label} SPECTRAL_WINDOW row count"
    );

    let rust_source_rows = rust.source().map(|table| table.row_count()).unwrap_or(0);
    let casa_source_rows = casa.source().map(|table| table.row_count()).unwrap_or(0);
    assert_eq!(
        rust_source_rows, casa_source_rows,
        "{label} SOURCE row count"
    );

    let rust_doppler_rows = rust.doppler().map(|table| table.row_count()).unwrap_or(0);
    let casa_doppler_rows = casa.doppler().map(|table| table.row_count()).unwrap_or(0);
    assert_eq!(
        rust_doppler_rows, casa_doppler_rows,
        "{label} DOPPLER row count"
    );
}

fn assert_spectral_window_matches(rust: &MeasurementSet, casa: &MeasurementSet, label: &str) {
    let rust_spw_binding = rust.spectral_window().expect("Rust SPECTRAL_WINDOW");
    let rust_spw = rust_spw_binding.table();
    let casa_spw_binding = casa.spectral_window().expect("CASA SPECTRAL_WINDOW");
    let casa_spw = casa_spw_binding.table();

    assert_eq!(
        rust_spw.row_count(),
        casa_spw.row_count(),
        "{label} spw rows"
    );

    for row_index in 0..rust_spw.row_count() {
        let rust_row = rust_spw.row(row_index).expect("read Rust SPW row");
        let casa_row = casa_spw.row(row_index).expect("read CASA SPW row");
        assert_eq!(
            get_string_scalar(rust_row, "NAME"),
            get_string_scalar(casa_row, "NAME"),
            "{label} SPW[{row_index}] NAME"
        );
        assert_eq!(
            get_i32_scalar(rust_row, "NUM_CHAN"),
            get_i32_scalar(casa_row, "NUM_CHAN"),
            "{label} SPW[{row_index}] NUM_CHAN"
        );
        assert_eq!(
            get_i32_scalar(rust_row, "MEAS_FREQ_REF"),
            get_i32_scalar(casa_row, "MEAS_FREQ_REF"),
            "{label} SPW[{row_index}] MEAS_FREQ_REF"
        );
        assert_eq!(
            get_i32_scalar(rust_row, "IF_CONV_CHAIN"),
            get_i32_scalar(casa_row, "IF_CONV_CHAIN"),
            "{label} SPW[{row_index}] IF_CONV_CHAIN"
        );
        assert_eq!(
            get_i32_scalar_or(rust_row, "DOPPLER_ID", -1),
            get_i32_scalar_or(casa_row, "DOPPLER_ID", -1),
            "{label} SPW[{row_index}] DOPPLER_ID"
        );
        assert_close(
            get_f64_scalar(rust_row, "REF_FREQUENCY"),
            get_f64_scalar(casa_row, "REF_FREQUENCY"),
            SPW_FLOAT_TOLERANCE_HZ,
            &format!("{label} SPW[{row_index}] REF_FREQUENCY"),
        );
        assert_close(
            get_f64_scalar(rust_row, "TOTAL_BANDWIDTH"),
            get_f64_scalar(casa_row, "TOTAL_BANDWIDTH"),
            SPW_FLOAT_TOLERANCE_HZ,
            &format!("{label} SPW[{row_index}] TOTAL_BANDWIDTH"),
        );
        assert_array_close(
            &get_f64_array(rust_row, "CHAN_FREQ"),
            &get_f64_array(casa_row, "CHAN_FREQ"),
            SPW_FLOAT_TOLERANCE_HZ,
            &format!("{label} SPW[{row_index}] CHAN_FREQ"),
        );
        assert_array_close(
            &get_f64_array(rust_row, "CHAN_WIDTH"),
            &get_f64_array(casa_row, "CHAN_WIDTH"),
            SPW_FLOAT_TOLERANCE_HZ,
            &format!("{label} SPW[{row_index}] CHAN_WIDTH"),
        );
        assert_array_close(
            &get_f64_array(rust_row, "EFFECTIVE_BW"),
            &get_f64_array(casa_row, "EFFECTIVE_BW"),
            SPW_FLOAT_TOLERANCE_HZ,
            &format!("{label} SPW[{row_index}] EFFECTIVE_BW"),
        );
        assert_array_close(
            &get_f64_array(rust_row, "RESOLUTION"),
            &get_f64_array(casa_row, "RESOLUTION"),
            SPW_FLOAT_TOLERANCE_HZ,
            &format!("{label} SPW[{row_index}] RESOLUTION"),
        );
    }
}

fn assert_main_and_subtable_matches(rust: &MeasurementSet, casa: &MeasurementSet) {
    compare_table_rows(
        "MAIN",
        rust.main_table(),
        casa.main_table(),
        &[
            "ANTENNA1",
            "ANTENNA2",
            "ARRAY_ID",
            "DATA_DESC_ID",
            "DATA",
            "EXPOSURE",
            "FEED1",
            "FEED2",
            "FIELD_ID",
            "FLAG",
            "FLAG_ROW",
            "INTERVAL",
            "OBSERVATION_ID",
            "PROCESSOR_ID",
            "SCAN_NUMBER",
            "SIGMA",
            "STATE_ID",
            "TIME",
            "TIME_CENTROID",
            "UVW",
            "WEIGHT",
        ],
    );
    compare_table_rows(
        "ANTENNA",
        rust.antenna().expect("Rust ANTENNA").table(),
        casa.antenna().expect("CASA ANTENNA").table(),
        &[
            "NAME",
            "STATION",
            "TYPE",
            "MOUNT",
            "POSITION",
            "OFFSET",
            "DISH_DIAMETER",
        ],
    );
    compare_table_rows(
        "FEED",
        rust.feed().expect("Rust FEED").table(),
        casa.feed().expect("CASA FEED").table(),
        &[
            "ANTENNA_ID",
            "BEAM_ID",
            "BEAM_OFFSET",
            "FEED_ID",
            "INTERVAL",
            "NUM_RECEPTORS",
            "POL_RESPONSE",
            "POLARIZATION_TYPE",
            "POSITION",
            "RECEPTOR_ANGLE",
            "SPECTRAL_WINDOW_ID",
            "TIME",
        ],
    );
    compare_table_rows(
        "OBSERVATION",
        rust.observation().expect("Rust OBSERVATION").table(),
        casa.observation().expect("CASA OBSERVATION").table(),
        &[
            "FLAG_ROW",
            "LOG",
            "OBSERVER",
            "PROJECT",
            "RELEASE_DATE",
            "SCHEDULE",
            "SCHEDULE_TYPE",
            "TELESCOPE_NAME",
            "TIME_RANGE",
        ],
    );
    compare_table_rows(
        "FIELD",
        rust.field().expect("Rust FIELD").table(),
        casa.field().expect("CASA FIELD").table(),
        &[
            "CODE",
            "DELAY_DIR",
            "FLAG_ROW",
            "NAME",
            "NUM_POLY",
            "PHASE_DIR",
            "REFERENCE_DIR",
            "SOURCE_ID",
            "TIME",
        ],
    );
    compare_table_rows(
        "POLARIZATION",
        rust.polarization().expect("Rust POLARIZATION").table(),
        casa.polarization().expect("CASA POLARIZATION").table(),
        &["CORR_PRODUCT", "CORR_TYPE", "FLAG_ROW", "NUM_CORR"],
    );
    match (rust.source(), casa.source()) {
        (Ok(rust_source), Ok(casa_source)) => {
            compare_table_rows(
                "SOURCE",
                rust_source.table(),
                casa_source.table(),
                &[
                    "CALIBRATION_GROUP",
                    "CODE",
                    "DIRECTION",
                    "INTERVAL",
                    "NAME",
                    "NUM_LINES",
                    "PROPER_MOTION",
                    "SOURCE_ID",
                    "SPECTRAL_WINDOW_ID",
                    "TIME",
                ],
            );
        }
        (Err(_), Err(_)) => {}
        _ => panic!("SOURCE availability mismatch between Rust and CASA"),
    }
    compare_table_rows(
        "DATA_DESCRIPTION",
        rust.data_description()
            .expect("Rust DATA_DESCRIPTION")
            .table(),
        casa.data_description()
            .expect("CASA DATA_DESCRIPTION")
            .table(),
        &["SPECTRAL_WINDOW_ID", "POLARIZATION_ID", "FLAG_ROW"],
    );
    match (rust.doppler(), casa.doppler()) {
        (Ok(rust_doppler), Ok(casa_doppler)) => {
            compare_table_rows(
                "DOPPLER",
                rust_doppler.table(),
                casa_doppler.table(),
                &["DOPPLER_ID", "SOURCE_ID", "TRANSITION_ID", "VELDEF"],
            );
        }
        (Err(_), Err(_)) => {}
        _ => panic!("DOPPLER availability mismatch between Rust and CASA"),
    }
}

fn assert_metadata_matches(rust: &MeasurementSet, casa: &MeasurementSet) {
    let rust_ms_version = rust
        .main_table()
        .keywords()
        .get("MS_VERSION")
        .expect("Rust MS_VERSION keyword");
    let casa_ms_version = casa
        .main_table()
        .keywords()
        .get("MS_VERSION")
        .expect("CASA MS_VERSION keyword");
    assert_value_close(
        "MAIN.MS_VERSION",
        rust_ms_version,
        casa_ms_version,
        FLOAT_TOLERANCE,
    );

    let rust_main = rust.main_table();
    let casa_main = casa.main_table();
    for column in ["TIME", "TIME_CENTROID", "UVW"] {
        assert_measure_keyword_matches("MAIN", rust_main, casa_main, column, true, true, true);
    }

    let rust_observation = rust.observation().expect("Rust OBSERVATION");
    let casa_observation = casa.observation().expect("CASA OBSERVATION");
    for column in ["TIME_RANGE", "RELEASE_DATE"] {
        assert_measure_keyword_matches(
            "OBSERVATION",
            rust_observation.table(),
            casa_observation.table(),
            column,
            true,
            true,
            true,
        );
    }

    let rust_field = rust.field().expect("Rust FIELD");
    let casa_field = casa.field().expect("CASA FIELD");
    assert_measure_keyword_matches(
        "FIELD",
        rust_field.table(),
        casa_field.table(),
        "TIME",
        true,
        true,
        true,
    );
    for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
        assert_measure_keyword_matches(
            "FIELD",
            rust_field.table(),
            casa_field.table(),
            column,
            true,
            true,
            true,
        );
    }

    let rust_antenna = rust.antenna().expect("Rust ANTENNA");
    let casa_antenna = casa.antenna().expect("CASA ANTENNA");
    for column in ["POSITION", "OFFSET"] {
        assert_measure_keyword_matches(
            "ANTENNA",
            rust_antenna.table(),
            casa_antenna.table(),
            column,
            true,
            true,
            true,
        );
    }

    let rust_feed = rust.feed().expect("Rust FEED");
    let casa_feed = casa.feed().expect("CASA FEED");
    for column in ["TIME", "POSITION", "BEAM_OFFSET"] {
        assert_measure_keyword_matches(
            "FEED",
            rust_feed.table(),
            casa_feed.table(),
            column,
            true,
            true,
            true,
        );
    }

    let rust_spw = rust.spectral_window().expect("Rust SPECTRAL_WINDOW");
    let casa_spw = casa.spectral_window().expect("CASA SPECTRAL_WINDOW");
    for column in [
        "REF_FREQUENCY",
        "CHAN_FREQ",
        "CHAN_WIDTH",
        "EFFECTIVE_BW",
        "RESOLUTION",
        "TOTAL_BANDWIDTH",
    ] {
        assert_measure_keyword_matches(
            "SPECTRAL_WINDOW",
            rust_spw.table(),
            casa_spw.table(),
            column,
            true,
            false,
            true,
        );
    }

    match (rust.source(), casa.source()) {
        (Ok(rust_source), Ok(casa_source)) => {
            for column in ["TIME", "DIRECTION", "PROPER_MOTION"] {
                assert_measure_keyword_matches(
                    "SOURCE",
                    rust_source.table(),
                    casa_source.table(),
                    column,
                    true,
                    true,
                    true,
                );
            }
        }
        (Err(_), Err(_)) => {}
        _ => panic!("SOURCE metadata availability mismatch between Rust and CASA"),
    }
}

fn assert_real_import_parity(archive_path: &Path, label: &str) {
    let Some(imported) = import_with_rust_and_casa(archive_path, label) else {
        return;
    };
    assert_measurement_set_shape_matches(&imported.rust, &imported.casa, label);
    assert_main_and_subtable_matches(&imported.rust, &imported.casa);
    assert_metadata_matches(&imported.rust, &imported.casa);
    assert_spectral_window_matches(&imported.rust, &imported.casa, label);
}

#[test]
#[ignore = "requires CASA Python and real VLA export files"]
fn parity_as758_c030425_xp1() {
    let Some(path) = xp1_path() else {
        eprintln!("skipping: xp1 archive not available");
        return;
    };
    assert_real_import_parity(&path, "xp1");
}

#[test]
#[ignore = "requires CASA Python and real VLA export files"]
fn parity_as758_c030426_xp5() {
    let Some(path) = xp5_path() else {
        eprintln!("skipping: xp5 archive not available");
        return;
    };
    assert_real_import_parity(&path, "xp5");
}

#[test]
#[ignore = "requires CASA Python and real VLA export files"]
fn parity_ag189_revision_era_export() {
    let Some(path) = ag189_path() else {
        eprintln!("skipping: AG189 archive not available");
        return;
    };
    assert_real_import_parity(&path, "ag189");
}
