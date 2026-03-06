// SPDX-License-Identifier: LGPL-3.0-or-later
//! MS iteration and grouping demo.
//!
//! Creates an MS with multiple times, fields, and data descriptions, then
//! iterates with the canonical sort order (ARRAY_ID, FIELD_ID, DATA_DESC_ID,
//! TIME) and prints group keys and row counts.
//!
//! Also demonstrates custom grouping by FIELD_ID only.
//!
//! Cf. C++ `tMSIter`.

use casacore_ms::builder::MeasurementSetBuilder;
use casacore_ms::column_def::{ColumnDef, ColumnKind};
use casacore_ms::grouping;
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema;
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use num_complex::Complex32;

fn main() {
    println!("=== t_ms_iter: MeasurementSet iteration demo ===\n");

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).expect("create MS");

    // Build an MS with:
    //   2 fields (field 0 = calibrator, field 1 = target)
    //   2 data description IDs (different SPW/pol combos)
    //   3 time steps
    //   3 baselines (antennas 0-1, 0-2, 1-2)

    let integration = 10.0;
    let base_time = 59000.5 * 86400.0;
    let baselines: Vec<(i32, i32)> = vec![(0, 1), (0, 2), (1, 2)];

    // Insert rows: field 0 at times 0,1 then field 1 at times 0,1,2
    // with ddid 0 for field 0 and ddid 1 for field 1
    let scan_plan: Vec<(i32, i32, i32, usize)> = vec![
        // (field_id, ddid, scan, num_times)
        (0, 0, 1, 2), // calibrator: 2 time steps
        (1, 1, 2, 3), // target: 3 time steps
        (0, 0, 3, 1), // back to calibrator: 1 time step
    ];

    let mut total_rows = 0;
    for (field_id, ddid, scan, n_times) in &scan_plan {
        for t in 0..*n_times {
            let time = base_time
                + (total_rows as f64 / baselines.len() as f64) * integration
                + t as f64 * integration;
            for &(ant1, ant2) in &baselines {
                add_main_row(
                    &mut ms,
                    &[
                        ("ANTENNA1", Value::Scalar(ScalarValue::Int32(ant1))),
                        ("ANTENNA2", Value::Scalar(ScalarValue::Int32(ant2))),
                        ("FIELD_ID", Value::Scalar(ScalarValue::Int32(*field_id))),
                        ("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(*ddid))),
                        ("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(*scan))),
                        ("TIME", Value::Scalar(ScalarValue::Float64(time))),
                        ("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                    ],
                );
                total_rows += 1;
            }
        }
    }

    println!("Created MS with {total_rows} rows\n");

    // ---- Canonical iteration ----
    println!("--- Canonical iteration (ARRAY_ID, FIELD_ID, DATA_DESC_ID, TIME) ---");
    let groups: Vec<_> = grouping::iter_ms(&ms).expect("iter").collect();
    println!("Number of groups: {}\n", groups.len());

    println!(
        "{:<8} {:<10} {:<12} {:<6}",
        "ARRAY", "FIELD", "DDID", "ROWS"
    );
    println!("{}", "-".repeat(40));
    for g in &groups {
        println!(
            "{:<8} {:<10} {:<12} {:<6}",
            g.array_id.map_or("?".to_string(), |v| v.to_string()),
            g.field_id.map_or("?".to_string(), |v| v.to_string()),
            g.data_desc_id.map_or("?".to_string(), |v| v.to_string()),
            g.row_indices.len()
        );
    }

    // ---- Custom grouping by FIELD_ID only ----
    println!("\n--- Custom grouping by FIELD_ID only ---");
    let field_groups: Vec<_> = grouping::iter_ms_by(&ms, &["FIELD_ID"])
        .expect("iter by field")
        .collect();
    println!("Number of field groups: {}\n", field_groups.len());

    println!("{:<10} {:<6}", "FIELD", "ROWS");
    println!("{}", "-".repeat(20));
    for g in &field_groups {
        println!(
            "{:<10} {:<6}",
            g.field_id.map_or("?".to_string(), |v| v.to_string()),
            g.row_indices.len()
        );
    }

    // ---- Group by SCAN_NUMBER ----
    println!("\n--- Custom grouping by SCAN_NUMBER ---");
    let scan_groups: Vec<_> = grouping::iter_ms_by(&ms, &["SCAN_NUMBER"])
        .expect("iter by scan")
        .collect();
    println!("Number of scan groups: {}\n", scan_groups.len());

    println!("{:<10} {:<6}", "SCAN", "ROWS");
    println!("{}", "-".repeat(20));
    for g in &scan_groups {
        // MsGroup only extracts known fields; read SCAN_NUMBER from first row
        let first_row = g.row_indices[0];
        let scan = ms
            .main_table()
            .get_scalar_cell(first_row, "SCAN_NUMBER")
            .ok()
            .and_then(|v| match v {
                ScalarValue::Int32(s) => Some(*s),
                _ => None,
            })
            .unwrap_or(-1);
        println!("{scan:<10} {:<6}", g.row_indices.len());
    }

    println!("\n=== t_ms_iter complete ===");
}

fn add_main_row(ms: &mut MeasurementSet, overrides: &[(&str, Value)]) {
    let schema = ms.main_table().schema().unwrap().clone();
    let all_cols: Vec<&ColumnDef> = schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
        .collect();

    let fields: Vec<RecordField> = schema
        .columns()
        .iter()
        .map(|col| {
            if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == col.name()) {
                return RecordField::new(col.name(), v.clone());
            }
            if let Some(c) = all_cols.iter().find(|c| c.name == col.name()) {
                RecordField::new(col.name(), default_value_for_def(c))
            } else {
                RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(0)))
            }
        })
        .collect();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();
}

fn default_value_for_def(c: &ColumnDef) -> Value {
    match c.column_kind {
        ColumnKind::Scalar => match c.data_type {
            casacore_types::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            casacore_types::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            casacore_types::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            casacore_types::PrimitiveType::String => {
                Value::Scalar(ScalarValue::String(String::new()))
            }
            _ => Value::Scalar(ScalarValue::Float64(0.0)),
        },
        ColumnKind::FixedArray { shape } => {
            let total: usize = shape.iter().product();
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
            ))
        }
        ColumnKind::VariableArray { ndim } => {
            let shape: Vec<usize> = vec![1; ndim];
            let total: usize = shape.iter().product();
            match c.data_type {
                casacore_types::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                )),
                casacore_types::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
                casacore_types::PrimitiveType::Complex32 => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(shape, vec![Complex32::new(0.0, 0.0); total]).unwrap(),
                )),
                casacore_types::PrimitiveType::String => Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(shape, vec![String::new(); total]).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
            }
        }
    }
}
