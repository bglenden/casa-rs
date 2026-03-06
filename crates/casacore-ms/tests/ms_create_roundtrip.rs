// SPDX-License-Identifier: LGPL-3.0-or-later
//! Integration test: create an MS, save, reopen, validate, and verify data.

use casacore_ms::builder::MeasurementSetBuilder;
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema::SubtableId;
use casacore_ms::selection::MsSelection;
use casacore_ms::selection_helpers;

use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

/// Helper to add a main-table row with overrides.
fn add_main_row(ms: &mut MeasurementSet, overrides: &[(&str, Value)]) {
    use casacore_ms::column_def::{ColumnDef, ColumnKind};
    use casacore_ms::schema::main_table;

    let schema = ms.main_table().schema().unwrap().clone();
    let all_cols: Vec<&ColumnDef> = main_table::REQUIRED_COLUMNS
        .iter()
        .chain(main_table::OPTIONAL_COLUMNS.iter())
        .collect();

    let fields: Vec<RecordField> = schema
        .columns()
        .iter()
        .map(|col| {
            if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == col.name()) {
                return RecordField::new(col.name(), v.clone());
            }
            if let Some(c) = all_cols.iter().find(|c| c.name == col.name()) {
                let val = match c.column_kind {
                    ColumnKind::Scalar => match c.data_type {
                        casacore_types::PrimitiveType::Int32 => {
                            Value::Scalar(ScalarValue::Int32(0))
                        }
                        casacore_types::PrimitiveType::Float64 => {
                            Value::Scalar(ScalarValue::Float64(0.0))
                        }
                        casacore_types::PrimitiveType::Bool => {
                            Value::Scalar(ScalarValue::Bool(false))
                        }
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
                            casacore_types::PrimitiveType::Float32 => {
                                Value::Array(ArrayValue::Float32(
                                    ArrayD::from_shape_vec(shape, vec![1.0; total]).unwrap(),
                                ))
                            }
                            _ => Value::Array(ArrayValue::Float64(
                                ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                            )),
                        }
                    }
                };
                RecordField::new(col.name(), val)
            } else {
                RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(0)))
            }
        })
        .collect();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();
}

#[test]
fn round_trip_create_save_open_validate() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("roundtrip.ms");

    // Create
    {
        let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();

        // Add antennas
        {
            let mut ant = ms.antenna_mut().unwrap();
            for i in 0..3 {
                ant.add_antenna(
                    &format!("ANT{i:02}"),
                    &format!("S{i:02}"),
                    "GROUND-BASED",
                    "ALT-AZ",
                    [1000.0 * i as f64, 2000.0, 3000.0],
                    [0.0; 3],
                    25.0,
                )
                .unwrap();
            }
        }

        // Add rows
        let i = |v: i32| Value::Scalar(ScalarValue::Int32(v));
        let f = |v: f64| Value::Scalar(ScalarValue::Float64(v));

        for &(ant1, ant2) in &[(0, 1), (0, 2), (1, 2)] {
            add_main_row(
                &mut ms,
                &[
                    ("ANTENNA1", i(ant1)),
                    ("ANTENNA2", i(ant2)),
                    ("FIELD_ID", i(0)),
                    ("DATA_DESC_ID", i(0)),
                    ("TIME", f(59000.0 * 86400.0)),
                ],
            );
        }

        ms.save().unwrap();
    }

    // Reopen and validate
    {
        let ms = MeasurementSet::open(&ms_path).unwrap();

        let issues = ms.validate().unwrap();
        assert!(issues.is_empty(), "Validation issues: {issues:?}");

        assert_eq!(ms.row_count(), 3);
        assert!(ms.ms_version().is_some());

        // Check subtables present
        assert!(ms.subtable(SubtableId::Antenna).is_some());
        assert!(ms.subtable(SubtableId::Field).is_some());
        assert!(ms.subtable(SubtableId::SpectralWindow).is_some());

        // Check antennas
        let ant = ms.antenna().unwrap();
        assert_eq!(ant.row_count(), 3);
        assert_eq!(ant.name(0).unwrap(), "ANT00");
        assert_eq!(ant.name(2).unwrap(), "ANT02");

        // Check unique antenna IDs
        let ids = selection_helpers::unique_antenna_ids(&ms).unwrap();
        assert_eq!(ids, vec![0, 1, 2]);
    }
}

#[test]
fn selection_round_trip() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

    let i = |v: i32| Value::Scalar(ScalarValue::Int32(v));
    let f = |v: f64| Value::Scalar(ScalarValue::Float64(v));

    // 5 rows with different fields and scans
    for &(field, scan) in &[(0, 1), (0, 2), (1, 1), (1, 2), (2, 3)] {
        add_main_row(
            &mut ms,
            &[
                ("FIELD_ID", i(field)),
                ("SCAN_NUMBER", i(scan)),
                ("TIME", f(59000.0 * 86400.0)),
            ],
        );
    }

    // Select field 0
    let sel = MsSelection::new().field(&[0]);
    let rows = sel.apply(&mut ms).unwrap();
    assert_eq!(rows.len(), 2);

    // Select scan 2
    let sel = MsSelection::new().scan(&[2]);
    let rows = sel.apply(&mut ms).unwrap();
    assert_eq!(rows.len(), 2);

    // Combined: field 1 AND scan 1
    let sel = MsSelection::new().field(&[1]).scan(&[1]);
    let rows = sel.apply(&mut ms).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn grouping_integration() {
    use casacore_ms::grouping;

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

    let i = |v: i32| Value::Scalar(ScalarValue::Int32(v));
    let f = |v: f64| Value::Scalar(ScalarValue::Float64(v));

    // 2 fields × 2 times
    let mut time_counter = 100.0_f64;
    for &field in &[0, 0, 1, 1] {
        time_counter += 100.0;
        let time = time_counter;
        add_main_row(
            &mut ms,
            &[
                ("FIELD_ID", i(field)),
                ("DATA_DESC_ID", i(0)),
                ("TIME", f(time)),
            ],
        );
    }

    let groups: Vec<_> = grouping::iter_ms(&ms).unwrap().collect();
    assert!(groups.len() >= 2, "Should have at least 2 groups");
}
