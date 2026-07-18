// SPDX-License-Identifier: LGPL-3.0-or-later
//! End-to-end TaQL interop for MEASINFO-backed columns.
//!
//! These tests exercise the remaining semantic gap between the current Rust
//! `meas.*` TaQL UDF surface and casacore's TaQL integration:
//!
//! - Rust currently consumes explicit raw values plus reference strings.
//! - C++ casacore TaQL can consume a `MEASINFO`-annotated column directly.
//!
//! The parity checks below compare those two calling models on the same
//! persisted fixture tables.
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use tempfile::TempDir;

use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_test_support::table_measures_interop::TableMeasuresOracle;
use casa_test_support::taql_interop::{
    CppTaqlQueryResult, TaqlOracle, TaqlQueryResult, rust_taql_query,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};

const RUST_EPOCH_QUERY: &str = "SELECT meas.epoch('TAI', TIME[1], 'UTC') AS tai";
const CPP_EPOCH_QUERY: &str = "using style python select meas.epoch('TAI', TIME)d as tai from $1";
const RUST_DIRECTION_QUERY: &str = "SELECT meas.galactic(DIR[1], DIR[2], 'J2000') AS gal";
const CPP_DIRECTION_QUERY: &str = "using style python select meas.galactic(DIR) as gal from $1";

fn build_epoch_measure_fixture() -> Table {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "TIME",
        PrimitiveType::Float64,
        vec![1],
    )])
    .unwrap();

    let mut table = Table::with_schema(schema);
    TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "UTC")
        .write(&mut table)
        .unwrap();

    for mjd in [51544.5_f64, 51545.0, 51546.5] {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "TIME",
                Value::Array(ArrayValue::from_f64_vec(vec![mjd])),
            )]))
            .unwrap();
    }

    table
}

fn build_direction_measure_fixture() -> Table {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "DIR",
        PrimitiveType::Float64,
        vec![2],
    )])
    .unwrap();

    let mut table = Table::with_schema(schema);
    TableMeasDesc::new_fixed("DIR", MeasureType::Direction, "J2000")
        .write(&mut table)
        .unwrap();

    for (lon, lat) in [(1.0_f64, 0.5_f64), (2.0, -0.3), (0.0, 1.5)] {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DIR",
                Value::Array(ArrayValue::from_f64_vec(vec![lon, lat])),
            )]))
            .unwrap();
    }

    table
}

fn assert_single_float_column_matches(
    rust_result: &TaqlQueryResult,
    cpp_result: &CppTaqlQueryResult,
    tolerance: f64,
) {
    assert_eq!(rust_result.rows.len(), cpp_result.grid.rows.len());
    assert_eq!(rust_result.rows.len(), 3);
    for (row_index, (rust_row, cpp_row)) in rust_result
        .rows
        .iter()
        .zip(cpp_result.grid.rows.iter())
        .enumerate()
    {
        assert_eq!(rust_row.len(), 1);
        assert_eq!(cpp_row.len(), 1);
        let rust_value = rust_row[0].parse::<f64>().unwrap();
        let cpp_value = cpp_row[0].parse::<f64>().unwrap();
        assert!(
            (rust_value - cpp_value).abs() <= tolerance,
            "row {row_index}: Rust={rust_value}, C++={cpp_value}"
        );
    }
}

fn parse_float_array_cell(cell: &str) -> Vec<f64> {
    cell.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| part.parse::<f64>().unwrap())
        .collect()
}

fn angle_diff_wrapped(lhs: f64, rhs: f64) -> f64 {
    let two_pi = std::f64::consts::TAU;
    let mut diff = (lhs - rhs).abs();
    while diff > two_pi {
        diff -= two_pi;
    }
    diff.min((two_pi - diff).abs())
}

fn assert_single_array_column_matches(
    rust_result: &TaqlQueryResult,
    cpp_result: &CppTaqlQueryResult,
    tolerance: f64,
) {
    assert_eq!(rust_result.rows.len(), cpp_result.grid.rows.len());
    for (row_index, (rust_row, cpp_row)) in rust_result
        .rows
        .iter()
        .zip(cpp_result.grid.rows.iter())
        .enumerate()
    {
        assert_eq!(rust_row.len(), 1);
        assert_eq!(cpp_row.len(), 1);
        let rust_values = parse_float_array_cell(&rust_row[0]);
        let cpp_values = parse_float_array_cell(&cpp_row[0]);
        assert_eq!(rust_values.len(), cpp_values.len());
        for (value_index, (rust_value, cpp_value)) in
            rust_values.iter().zip(cpp_values.iter()).enumerate()
        {
            let diff = if value_index == 0 {
                angle_diff_wrapped(*rust_value, *cpp_value)
            } else {
                (rust_value - cpp_value).abs()
            };
            assert!(
                diff <= tolerance,
                "row {row_index}, value {value_index}: Rust={rust_value}, C++={cpp_value}"
            );
        }
    }
}

#[test]
fn rc_epoch_meas_column_matches_cpp_taql() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("rc_epoch.tab");

    let mut rust_table = build_epoch_measure_fixture();
    let rust_result = rust_taql_query(&mut rust_table, RUST_EPOCH_QUERY).unwrap();
    rust_table.save(TableOptions::new(&path)).unwrap();

    let cpp_result = TaqlOracle::query(&path, CPP_EPOCH_QUERY).unwrap();
    assert_single_float_column_matches(&rust_result, &cpp_result, 1.0e-9);
}

#[test]
fn cr_epoch_meas_column_matches_cpp_taql() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("cr_epoch.tab");
    TableMeasuresOracle::create_epoch_fixed(path.to_str().unwrap()).unwrap();

    let cpp_result = TaqlOracle::query(&path, CPP_EPOCH_QUERY).unwrap();
    let mut rust_table = Table::open(TableOptions::new(&path)).unwrap();
    let rust_result = rust_taql_query(&mut rust_table, RUST_EPOCH_QUERY).unwrap();

    assert_single_float_column_matches(&rust_result, &cpp_result, 1.0e-9);
}

#[test]
fn rc_direction_meas_column_matches_cpp_taql() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("rc_direction.tab");

    let mut rust_table = build_direction_measure_fixture();
    let rust_result = rust_taql_query(&mut rust_table, RUST_DIRECTION_QUERY).unwrap();
    rust_table.save(TableOptions::new(&path)).unwrap();

    let cpp_result = TaqlOracle::query(&path, CPP_DIRECTION_QUERY).unwrap();
    assert_single_array_column_matches(&rust_result, &cpp_result, 5.0e-6);
}

#[test]
fn cr_direction_meas_column_matches_cpp_taql() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("cr_direction.tab");
    TableMeasuresOracle::create_direction_fixed(path.to_str().unwrap()).unwrap();

    let cpp_result = TaqlOracle::query(&path, CPP_DIRECTION_QUERY).unwrap();
    let mut rust_table = Table::open(TableOptions::new(&path)).unwrap();
    let rust_result = rust_taql_query(&mut rust_table, RUST_DIRECTION_QUERY).unwrap();

    assert_single_array_column_matches(&rust_result, &cpp_result, 5.0e-6);
}
