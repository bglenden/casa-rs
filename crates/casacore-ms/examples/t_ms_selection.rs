// SPDX-License-Identifier: LGPL-3.0-or-later
//! MS selection and filtering demo.
//!
//! Demonstrates field selection, scan selection, antenna selection by name,
//! time range selection, and combined (AND) selection. Prints selected row
//! counts and the generated TaQL query strings.
//!
//! Cf. C++ `tMSSelection`.

use casacore_ms::builder::MeasurementSetBuilder;
use casacore_ms::column_def::{ColumnDef, ColumnKind};
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema;
use casacore_ms::selection::MsSelection;
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use num_complex::Complex32;

fn main() {
    println!("=== t_ms_selection: MeasurementSet selection demo ===\n");

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).expect("create MS");

    // Add 3 antennas
    {
        let mut ant = ms.antenna_mut().expect("antenna");
        for (name, station) in [("VLA01", "N01"), ("VLA02", "E02"), ("VLA03", "W03")] {
            ant.add_antenna(
                name,
                station,
                "GROUND-BASED",
                "ALT-AZ",
                [0.0; 3],
                [0.0; 3],
                25.0,
            )
            .expect("add antenna");
        }
    }

    // Build rows: 3 fields x 2 scans x 3 baselines x 2 times = 36 rows
    let base_time = 59000.0 * 86400.0;
    let integration = 10.0;
    let baselines: Vec<(i32, i32)> = vec![(0, 1), (0, 2), (1, 2)];

    for field_id in 0..3 {
        for scan in 1..=2 {
            for t in 0..2 {
                let time = base_time
                    + field_id as f64 * 1000.0
                    + scan as f64 * 100.0
                    + t as f64 * integration;
                for &(ant1, ant2) in &baselines {
                    add_main_row(
                        &mut ms,
                        &[
                            ("ANTENNA1", Value::Scalar(ScalarValue::Int32(ant1))),
                            ("ANTENNA2", Value::Scalar(ScalarValue::Int32(ant2))),
                            ("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id))),
                            ("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(scan))),
                            ("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0))),
                            ("TIME", Value::Scalar(ScalarValue::Float64(time))),
                        ],
                    );
                }
            }
        }
    }

    println!("Created MS with {} rows\n", ms.row_count());
    println!("  3 fields x 2 scans x 3 baselines x 2 times = 36 rows\n");

    // ---- Field selection ----
    {
        let sel = MsSelection::new().field(&[0, 1]);
        println!("1. Field selection: fields [0, 1]");
        println!("   TaQL: {}", sel.to_taql());
        let rows = sel.apply(&mut ms).expect("apply");
        println!("   Selected rows: {} (expected 24)\n", rows.len());
    }

    // ---- Scan selection ----
    {
        let sel = MsSelection::new().scan(&[2]);
        println!("2. Scan selection: scan [2]");
        println!("   TaQL: {}", sel.to_taql());
        let rows = sel.apply(&mut ms).expect("apply");
        println!("   Selected rows: {} (expected 18)\n", rows.len());
    }

    // ---- Antenna selection by name ----
    {
        let sel = MsSelection::new().antenna_name(&["VLA01"]);
        println!("3. Antenna selection by name: [VLA01]");
        let rows = sel.apply(&mut ms).expect("apply");
        println!(
            "   Selected rows: {} (expected 24, baselines 0-1 and 0-2)\n",
            rows.len()
        );
    }

    // ---- Antenna selection by ID ----
    {
        let sel = MsSelection::new().antenna(&[2]);
        println!("4. Antenna selection by ID: [2]");
        println!("   TaQL: {}", sel.to_taql());
        let rows = sel.apply(&mut ms).expect("apply");
        println!(
            "   Selected rows: {} (expected 24, baselines 0-2 and 1-2)\n",
            rows.len()
        );
    }

    // ---- Time range selection ----
    {
        // Select rows from field 0 only (time ~ base_time + 100..210)
        let t_start = base_time + 90.0;
        let t_end = base_time + 220.0;
        let sel = MsSelection::new().time_range(t_start, t_end);
        println!("5. Time range selection: [{t_start:.0}, {t_end:.0}]");
        println!("   TaQL: {}", sel.to_taql());
        let rows = sel.apply(&mut ms).expect("apply");
        println!("   Selected rows: {}\n", rows.len());
    }

    // ---- Combined selection (AND) ----
    {
        let sel = MsSelection::new().field(&[0]).scan(&[1]);
        println!("6. Combined selection: field=0 AND scan=1");
        println!("   TaQL: {}", sel.to_taql());
        let rows = sel.apply(&mut ms).expect("apply");
        println!("   Selected rows: {} (expected 6)\n", rows.len());
    }

    // ---- Baseline selection ----
    {
        let sel = MsSelection::new().baseline(&[(0, 1)]);
        println!("7. Baseline selection: [(0,1)]");
        println!("   TaQL: {}", sel.to_taql());
        let rows = sel.apply(&mut ms).expect("apply");
        println!("   Selected rows: {} (expected 12)\n", rows.len());
    }

    // ---- Raw TaQL expression ----
    {
        let sel = MsSelection::new().taql("FIELD_ID == 2 AND SCAN_NUMBER == 2");
        println!("8. Raw TaQL: FIELD_ID == 2 AND SCAN_NUMBER == 2");
        println!("   TaQL: {}", sel.to_taql());
        let rows = sel.apply(&mut ms).expect("apply");
        println!("   Selected rows: {} (expected 6)\n", rows.len());
    }

    println!("=== t_ms_selection complete ===");
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
