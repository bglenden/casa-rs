// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};
use std::process::Command;

use casa_ms::SubTable;
use casa_ms::ms::MeasurementSet;
use casa_tables::Table;
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier, discover_casa_python};
use casa_types::{ArrayValue, Complex32, Complex64, RecordValue, ScalarValue, Value};
use casa_vla::{ImportVlaOptions, import_archive_files_to_measurement_set_from_options};
use tempfile::TempDir;

const FLOAT_TOLERANCE: f64 = 1.0e-5;

pub fn run_importvla_parity_case(archive_path: &Path) {
    let Some(casa_python) = discover_casa_python() else {
        eprintln!("skipping: CASA Python environment not found");
        return;
    };

    let tempdir = TempDir::new().expect("create tempdir");
    let rust_ms_path = tempdir.path().join("rust-import.ms");
    let casa_ms_path = tempdir.path().join("casa-import.ms");

    import_archive_files_to_measurement_set_from_options(&ImportVlaOptions {
        archivefiles: vec![archive_path.to_path_buf()],
        vis: Some(rust_ms_path.clone()),
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
        "CASA importvla failed: status={:?}\nstdout={}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let rust = MeasurementSet::open(&rust_ms_path).expect("open Rust MeasurementSet");
    let casa = MeasurementSet::open(&casa_ms_path).expect("open CASA MeasurementSet");
    assert_measurement_sets_match_subset(&rust, &casa);
}

#[allow(dead_code)]
pub fn first_existing_path(candidates: &[&str]) -> Option<PathBuf> {
    candidates
        .iter()
        .map(PathBuf::from)
        .find(|path| path.exists())
}

#[allow(dead_code)]
pub fn slow_parity_archive(relative: &str) -> Option<PathBuf> {
    casatestdata_path_for_tier(CasaTestDataTier::SlowParity, relative).filter(|path| path.exists())
}

fn assert_measurement_sets_match_subset(rust: &MeasurementSet, cpp: &MeasurementSet) {
    assert_eq!(rust.row_count(), cpp.row_count(), "MAIN row count");

    compare_table_rows(
        "MAIN",
        rust.main_table(),
        cpp.main_table(),
        &[
            "ANTENNA1",
            "ANTENNA2",
            "ARRAY_ID",
            "DATA_DESC_ID",
            "DATA",
            "CORRECTED_DATA",
            "MODEL_DATA",
            "EXPOSURE",
            "FEED1",
            "FEED2",
            "FIELD_ID",
            "FLAG",
            "FLAG_CATEGORY",
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
        rust.antenna().expect("rust ANTENNA").table(),
        cpp.antenna().expect("cpp ANTENNA").table(),
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
        rust.feed().expect("rust FEED").table(),
        cpp.feed().expect("cpp FEED").table(),
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
        rust.observation().expect("rust OBSERVATION").table(),
        cpp.observation().expect("cpp OBSERVATION").table(),
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
        "SOURCE",
        rust.source().expect("rust SOURCE").table(),
        cpp.source().expect("cpp SOURCE").table(),
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
    compare_table_rows(
        "FIELD",
        rust.field().expect("rust FIELD").table(),
        cpp.field().expect("cpp FIELD").table(),
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
        rust.polarization().expect("rust POLARIZATION").table(),
        cpp.polarization().expect("cpp POLARIZATION").table(),
        &["CORR_PRODUCT", "CORR_TYPE", "FLAG_ROW", "NUM_CORR"],
    );
    compare_table_rows(
        "SPECTRAL_WINDOW",
        rust.spectral_window()
            .expect("rust SPECTRAL_WINDOW")
            .table(),
        cpp.spectral_window().expect("cpp SPECTRAL_WINDOW").table(),
        &[
            "NUM_CHAN",
            "NAME",
            "REF_FREQUENCY",
            "TOTAL_BANDWIDTH",
            "CHAN_FREQ",
            "CHAN_WIDTH",
            "EFFECTIVE_BW",
            "RESOLUTION",
            "MEAS_FREQ_REF",
            "NET_SIDEBAND",
            "FREQ_GROUP",
            "FREQ_GROUP_NAME",
            "IF_CONV_CHAIN",
            "FLAG_ROW",
        ],
    );
    compare_table_rows(
        "DATA_DESCRIPTION",
        rust.data_description()
            .expect("rust DATA_DESCRIPTION")
            .table(),
        cpp.data_description()
            .expect("cpp DATA_DESCRIPTION")
            .table(),
        &["SPECTRAL_WINDOW_ID", "POLARIZATION_ID", "FLAG_ROW"],
    );
    compare_table_rows(
        "DOPPLER",
        rust.doppler().expect("rust DOPPLER").table(),
        cpp.doppler().expect("cpp DOPPLER").table(),
        &["DOPPLER_ID", "SOURCE_ID", "TRANSITION_ID", "VELDEF"],
    );
    assert_measurement_set_metadata_matches(rust, cpp);
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
    cpp: &Table,
    column: &str,
    measinfo_type: bool,
    measinfo_ref: bool,
    quantum_units_match: bool,
) {
    if measinfo_type {
        assert_eq!(
            measure_info_string(rust, column, "type"),
            measure_info_string(cpp, column, "type"),
            "{table_name}.{column} MEASINFO.type"
        );
    }
    if measinfo_ref {
        assert_eq!(
            measure_info_string(rust, column, "Ref"),
            measure_info_string(cpp, column, "Ref"),
            "{table_name}.{column} MEASINFO.Ref"
        );
    }
    if quantum_units_match {
        assert_eq!(
            quantum_units(rust, column),
            quantum_units(cpp, column),
            "{table_name}.{column} QuantumUnits"
        );
    }
}

fn assert_measurement_set_metadata_matches(rust: &MeasurementSet, cpp: &MeasurementSet) {
    let rust_ms_version = rust
        .main_table()
        .keywords()
        .get("MS_VERSION")
        .expect("rust MS_VERSION keyword");
    let cpp_ms_version = cpp
        .main_table()
        .keywords()
        .get("MS_VERSION")
        .expect("cpp MS_VERSION keyword");
    assert_value_close(
        "MAIN.MS_VERSION",
        rust_ms_version,
        cpp_ms_version,
        FLOAT_TOLERANCE,
    );

    let rust_main = rust.main_table();
    let cpp_main = cpp.main_table();
    for column in ["TIME", "TIME_CENTROID", "UVW"] {
        assert_measure_keyword_matches("MAIN", rust_main, cpp_main, column, true, true, true);
    }

    let rust_observation = rust.observation().expect("rust OBSERVATION");
    let cpp_observation = cpp.observation().expect("cpp OBSERVATION");
    for column in ["TIME_RANGE", "RELEASE_DATE"] {
        assert_measure_keyword_matches(
            "OBSERVATION",
            rust_observation.table(),
            cpp_observation.table(),
            column,
            true,
            true,
            true,
        );
    }

    let rust_field = rust.field().expect("rust FIELD");
    let cpp_field = cpp.field().expect("cpp FIELD");
    assert_measure_keyword_matches(
        "FIELD",
        rust_field.table(),
        cpp_field.table(),
        "TIME",
        true,
        true,
        true,
    );
    for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
        assert_measure_keyword_matches(
            "FIELD",
            rust_field.table(),
            cpp_field.table(),
            column,
            true,
            true,
            true,
        );
    }

    let rust_antenna = rust.antenna().expect("rust ANTENNA");
    let cpp_antenna = cpp.antenna().expect("cpp ANTENNA");
    for column in ["POSITION", "OFFSET"] {
        assert_measure_keyword_matches(
            "ANTENNA",
            rust_antenna.table(),
            cpp_antenna.table(),
            column,
            true,
            true,
            true,
        );
    }

    let rust_feed = rust.feed().expect("rust FEED");
    let cpp_feed = cpp.feed().expect("cpp FEED");
    for column in ["TIME", "POSITION", "BEAM_OFFSET"] {
        assert_measure_keyword_matches(
            "FEED",
            rust_feed.table(),
            cpp_feed.table(),
            column,
            true,
            true,
            true,
        );
    }

    let rust_spw = rust.spectral_window().expect("rust SPECTRAL_WINDOW");
    let cpp_spw = cpp.spectral_window().expect("cpp SPECTRAL_WINDOW");
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
            cpp_spw.table(),
            column,
            matches!(column, "REF_FREQUENCY" | "CHAN_FREQ"),
            false,
            true,
        );
    }

    let rust_source = rust.source().expect("rust SOURCE");
    let cpp_source = cpp.source().expect("cpp SOURCE");
    for column in ["TIME", "DIRECTION", "PROPER_MOTION"] {
        assert_measure_keyword_matches(
            "SOURCE",
            rust_source.table(),
            cpp_source.table(),
            column,
            true,
            true,
            true,
        );
    }
}

fn compare_table_rows(name: &str, rust: &Table, cpp: &Table, columns: &[&str]) {
    assert_eq!(rust.row_count(), cpp.row_count(), "{name} row count");
    for row_index in 0..rust.row_count() {
        let rust_row = rust.row_accessor().row(row_index).expect("read rust row");
        let cpp_row = cpp.row_accessor().row(row_index).expect("read cpp row");
        compare_record_subset(name, row_index, rust_row, cpp_row, columns);
    }
}

fn compare_record_subset(
    table_name: &str,
    row_index: usize,
    rust_row: &RecordValue,
    cpp_row: &RecordValue,
    columns: &[&str],
) {
    for &column in columns {
        let rust_value = rust_row
            .get(column)
            .unwrap_or_else(|| panic!("{table_name}[{row_index}] missing Rust column {column}"));
        let cpp_value = cpp_row
            .get(column)
            .unwrap_or_else(|| panic!("{table_name}[{row_index}] missing C++ column {column}"));
        assert_value_close(
            &format!("{table_name}[{row_index}].{column}"),
            rust_value,
            cpp_value,
            FLOAT_TOLERANCE,
        );
    }
}

fn assert_value_close(label: &str, rust: &Value, cpp: &Value, tolerance: f64) {
    match (rust, cpp) {
        (Value::Scalar(rust_scalar), Value::Scalar(cpp_scalar)) => {
            assert_scalar_close(label, rust_scalar, cpp_scalar, tolerance);
        }
        (Value::Array(rust_array), Value::Array(cpp_array)) => {
            assert_array_close(label, rust_array, cpp_array, tolerance);
        }
        _ => panic!("{label}: value kind mismatch: rust={rust:?} cpp={cpp:?}"),
    }
}

fn assert_scalar_close(label: &str, rust: &ScalarValue, cpp: &ScalarValue, tolerance: f64) {
    let tolerance = tolerance_for_label(label, tolerance);
    match (rust, cpp) {
        (ScalarValue::Bool(rust), ScalarValue::Bool(cpp)) => assert_eq!(rust, cpp, "{label}"),
        (ScalarValue::Int32(rust), ScalarValue::Int32(cpp)) => assert_eq!(rust, cpp, "{label}"),
        (ScalarValue::Float32(rust), ScalarValue::Float32(cpp)) => {
            assert!(
                (f64::from(*rust) - f64::from(*cpp)).abs() <= tolerance,
                "{label}: rust={rust} cpp={cpp}"
            );
        }
        (ScalarValue::Float64(rust), ScalarValue::Float64(cpp)) => {
            assert!(
                (rust - cpp).abs() <= tolerance,
                "{label}: rust={rust} cpp={cpp}"
            );
        }
        (ScalarValue::Complex32(rust), ScalarValue::Complex32(cpp)) => {
            assert_complex32_close(label, *rust, *cpp, tolerance);
        }
        (ScalarValue::Complex64(rust), ScalarValue::Complex64(cpp)) => {
            assert_complex64_close(label, *rust, *cpp, tolerance);
        }
        (ScalarValue::String(rust), ScalarValue::String(cpp)) => assert_eq!(rust, cpp, "{label}"),
        _ => panic!("{label}: scalar type mismatch: rust={rust:?} cpp={cpp:?}"),
    }
}

fn assert_array_close(label: &str, rust: &ArrayValue, cpp: &ArrayValue, tolerance: f64) {
    let tolerance = tolerance_for_label(label, tolerance);
    match (rust, cpp) {
        (ArrayValue::Bool(rust), ArrayValue::Bool(cpp)) => {
            assert_eq!(rust.shape(), cpp.shape(), "{label} shape");
            assert_eq!(
                rust.iter().copied().collect::<Vec<_>>(),
                cpp.iter().copied().collect::<Vec<_>>(),
                "{label}"
            );
        }
        (ArrayValue::Int32(rust), ArrayValue::Int32(cpp)) => {
            assert_eq!(rust.shape(), cpp.shape(), "{label} shape");
            assert_eq!(
                rust.iter().copied().collect::<Vec<_>>(),
                cpp.iter().copied().collect::<Vec<_>>(),
                "{label}"
            );
        }
        (ArrayValue::Float32(rust), ArrayValue::Float32(cpp)) => {
            assert_eq!(rust.shape(), cpp.shape(), "{label} shape");
            for (index, (rust_value, cpp_value)) in rust.iter().zip(cpp.iter()).enumerate() {
                assert!(
                    (f64::from(*rust_value) - f64::from(*cpp_value)).abs() <= tolerance,
                    "{label}[{index}]: rust={rust_value} cpp={cpp_value}"
                );
            }
        }
        (ArrayValue::Float64(rust), ArrayValue::Float64(cpp)) => {
            assert_eq!(rust.shape(), cpp.shape(), "{label} shape");
            for (index, (rust_value, cpp_value)) in rust.iter().zip(cpp.iter()).enumerate() {
                assert!(
                    (rust_value - cpp_value).abs() <= tolerance,
                    "{label}[{index}]: rust={rust_value} cpp={cpp_value}"
                );
            }
        }
        (ArrayValue::Complex32(rust), ArrayValue::Complex32(cpp)) => {
            assert_eq!(rust.shape(), cpp.shape(), "{label} shape");
            for (index, (rust_value, cpp_value)) in rust.iter().zip(cpp.iter()).enumerate() {
                assert_complex32_close(
                    &format!("{label}[{index}]"),
                    *rust_value,
                    *cpp_value,
                    tolerance,
                );
            }
        }
        (ArrayValue::Complex64(rust), ArrayValue::Complex64(cpp)) => {
            assert_eq!(rust.shape(), cpp.shape(), "{label} shape");
            for (index, (rust_value, cpp_value)) in rust.iter().zip(cpp.iter()).enumerate() {
                assert_complex64_close(
                    &format!("{label}[{index}]"),
                    *rust_value,
                    *cpp_value,
                    tolerance,
                );
            }
        }
        (ArrayValue::String(rust), ArrayValue::String(cpp)) => {
            assert_eq!(rust.shape(), cpp.shape(), "{label} shape");
            assert_eq!(
                rust.iter().cloned().collect::<Vec<_>>(),
                cpp.iter().cloned().collect::<Vec<_>>(),
                "{label}"
            );
        }
        _ => panic!("{label}: array type mismatch: rust={rust:?} cpp={cpp:?}"),
    }
}

fn tolerance_for_label(label: &str, default_tolerance: f64) -> f64 {
    if label.starts_with("SPECTRAL_WINDOW[") {
        return 1.0e-2;
    }
    default_tolerance
}

fn assert_complex32_close(label: &str, rust: Complex32, cpp: Complex32, tolerance: f64) {
    assert!(
        (f64::from(rust.re) - f64::from(cpp.re)).abs() <= tolerance
            && (f64::from(rust.im) - f64::from(cpp.im)).abs() <= tolerance,
        "{label}: rust={rust:?} cpp={cpp:?}"
    );
}

fn assert_complex64_close(label: &str, rust: Complex64, cpp: Complex64, tolerance: f64) {
    assert!(
        (rust.re - cpp.re).abs() <= tolerance && (rust.im - cpp.im).abs() <= tolerance,
        "{label}: rust={rust:?} cpp={cpp:?}"
    );
}
