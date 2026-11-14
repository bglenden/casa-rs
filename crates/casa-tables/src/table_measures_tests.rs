// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;
use casa_types::measures::record;
use casa_types::*;

use crate::{ColumnSchema, TableSchema};

fn make_epoch_table(fixed_ref: &str) -> Table {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "TIME",
        PrimitiveType::Float64,
        vec![1],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let desc = TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, fixed_ref);
    desc.write(&mut table).unwrap();
    table
}

#[test]
fn rr_epoch_fixed_ref() {
    let mut table = make_epoch_table("UTC");
    let epochs = [
        MEpoch::from_mjd(51544.5, EpochRef::UTC),
        MEpoch::from_mjd(51545.0, EpochRef::UTC),
        MEpoch::from_mjd(51546.5, EpochRef::UTC),
    ];
    for e in &epochs {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "TIME",
                Value::Array(ArrayValue::from_f64_vec(vec![e.value().as_mjd()])),
            )]))
            .unwrap();
    }

    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
    for (i, expected) in epochs.iter().enumerate() {
        let got = col.get_epoch(i).unwrap();
        assert_eq!(got.refer(), expected.refer());
        assert!((got.value().as_mjd() - expected.value().as_mjd()).abs() < 1e-12);
    }
}

#[test]
fn rr_epoch_var_int_ref() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
        ColumnSchema::scalar("TimeRef", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let (types, codes) = default_epoch_ref_map();
    let desc = TableMeasDesc::new_variable_int("TIME", MeasureType::Epoch, "TimeRef", types, codes)
        .unwrap();
    desc.write(&mut table).unwrap();

    // Add rows with different refs
    let data = [
        (51544.5, EpochRef::UTC),
        (51545.0, EpochRef::TAI),
        (51546.5, EpochRef::TT),
    ];
    for &(mjd, refer) in &data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("TIME", Value::Array(ArrayValue::from_f64_vec(vec![mjd]))),
                RecordField::new(
                    "TimeRef",
                    Value::Scalar(ScalarValue::Int32(refer.casacore_code())),
                ),
            ]))
            .unwrap();
    }

    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
    for (i, &(mjd, refer)) in data.iter().enumerate() {
        let got = col.get_epoch(i).unwrap();
        assert_eq!(got.refer(), refer, "row {i}");
        assert!((got.value().as_mjd() - mjd).abs() < 1e-12);
    }
}

#[test]
fn undefined_frequency_with_default_integer_reference_map_round_trips() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("FREQ", PrimitiveType::Float64, vec![1]),
        ColumnSchema::scalar("FreqRef", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let (types, codes) = default_frequency_ref_map();
    let desc =
        TableMeasDesc::new_variable_int("FREQ", MeasureType::Frequency, "FreqRef", types, codes)
            .unwrap();
    desc.write(&mut table).unwrap();
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("FREQ", Value::Array(ArrayValue::from_f64_vec(vec![0.0]))),
            RecordField::new("FreqRef", Value::Scalar(ScalarValue::Int32(0))),
        ]))
        .unwrap();
    let frequency = MFrequency::new(1.4e9, FrequencyRef::Undefined);
    ScalarMeasColumnMut::new(&mut table, "FREQ")
        .unwrap()
        .put(0, &MeasureValue::Frequency(frequency))
        .unwrap();
    assert_eq!(
        table.cell_accessor(0, "FreqRef").unwrap().value().unwrap(),
        Some(&Value::Scalar(ScalarValue::Int32(64)))
    );
    let value = ScalarMeasColumn::new(&table, "FREQ")
        .unwrap()
        .get(0)
        .unwrap();
    let actual = value.as_frequency().unwrap();
    assert_eq!(actual.refer(), FrequencyRef::Undefined);
    assert_eq!(actual.hz(), 1.4e9);
}

#[test]
fn integer_frequency_reference_without_mapping_keywords_uses_canonical_codes() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("FREQ", PrimitiveType::Float64, vec![1]),
        ColumnSchema::scalar("FreqRef", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    // This writes only type and VarRefCol, as in older casacore tables.
    TableMeasDesc::new_variable_string("FREQ", MeasureType::Frequency, "FreqRef")
        .write(&mut table)
        .unwrap();
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("FREQ", Value::Array(ArrayValue::from_f64_vec(vec![1.4e9]))),
            RecordField::new("FreqRef", Value::Scalar(ScalarValue::Int32(3))),
        ]))
        .unwrap();
    assert_eq!(
        ScalarMeasColumn::new(&table, "FREQ")
            .unwrap()
            .get(0)
            .unwrap()
            .as_frequency()
            .unwrap()
            .refer(),
        FrequencyRef::BARY
    );
    ScalarMeasColumnMut::new(&mut table, "FREQ")
        .unwrap()
        .put(
            0,
            &MeasureValue::Frequency(MFrequency::new(1.5e9, FrequencyRef::Undefined)),
        )
        .unwrap();
    assert_eq!(
        table.cell_accessor(0, "FreqRef").unwrap().scalar().unwrap(),
        &ScalarValue::Int32(64)
    );
    assert_eq!(
        ScalarMeasColumn::new(&table, "FREQ")
            .unwrap()
            .get(0)
            .unwrap()
            .as_frequency()
            .unwrap()
            .refer(),
        FrequencyRef::Undefined
    );
}

#[test]
fn frequency_reference_mapping_rejects_noncanonical_table_type_names() {
    assert_eq!(
        frequency_ref_for_table_code(&["BARY".to_string()], &[40], 40),
        Some(FrequencyRef::BARY)
    );
    assert_eq!(
        frequency_ref_for_table_code(&["BARYCENTRIC".to_string()], &[40], 40),
        None
    );
    assert_eq!(
        frequency_ref_for_table_code(&["bary".to_string()], &[40], 40),
        None
    );
}

#[test]
fn integer_reference_writes_use_explicit_and_inferred_table_codes() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_variable("FREQ", PrimitiveType::Float64, Some(1)),
        ColumnSchema::scalar("FreqRef", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let desc = TableMeasDesc::new_variable_int(
        "FREQ",
        MeasureType::Frequency,
        "FreqRef",
        vec!["LSRK".to_string(), "Undefined".to_string()],
        vec![27, 9],
    )
    .unwrap();
    desc.write(&mut table).unwrap();
    for _ in 0..2 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("FREQ", Value::Array(ArrayValue::from_f64_vec(vec![1.0]))),
                RecordField::new("FreqRef", Value::Scalar(ScalarValue::Int32(27))),
            ]))
            .unwrap();
    }
    let undefined = MeasureValue::Frequency(MFrequency::new(1.4e9, FrequencyRef::Undefined));
    ScalarMeasColumnMut::new(&mut table, "FREQ")
        .unwrap()
        .put(0, &undefined)
        .unwrap();
    ArrayMeasColumnMut::new(&mut table, "FREQ")
        .unwrap()
        .put(1, &[undefined])
        .unwrap();
    for row in 0..2 {
        assert_eq!(
            table
                .cell_accessor(row, "FreqRef")
                .unwrap()
                .value()
                .unwrap(),
            Some(&Value::Scalar(ScalarValue::Int32(9)))
        );
        let value = ScalarMeasColumn::new(&table, "FREQ")
            .unwrap()
            .get(row)
            .unwrap();
        let frequency = value.as_frequency().unwrap();
        assert_eq!(frequency.refer(), FrequencyRef::Undefined);
        assert_eq!(frequency.hz(), 1.4e9);
    }
    let unmapped = MeasureValue::Frequency(MFrequency::new(2.0e9, FrequencyRef::BARY));
    ScalarMeasColumnMut::new(&mut table, "FREQ")
        .unwrap()
        .put(0, &unmapped)
        .unwrap();
    ArrayMeasColumnMut::new(&mut table, "FREQ")
        .unwrap()
        .put(1, &[unmapped])
        .unwrap();
    for row in 0..2 {
        assert_eq!(
            table
                .cell_accessor(row, "FreqRef")
                .unwrap()
                .value()
                .unwrap(),
            Some(&Value::Scalar(ScalarValue::Int32(30)))
        );
        let value = ScalarMeasColumn::new(&table, "FREQ")
            .unwrap()
            .get(row)
            .unwrap();
        let frequency = value.as_frequency().unwrap();
        assert_eq!(frequency.refer(), FrequencyRef::BARY);
        assert_eq!(frequency.hz(), 2.0e9);
    }
}

#[test]
fn remapped_epoch_reference_is_written_and_missing_mapping_keeps_values() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
        ColumnSchema::scalar("TimeRef", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let desc = TableMeasDesc::new_variable_int(
        "TIME",
        MeasureType::Epoch,
        "TimeRef",
        vec!["UTC".into(), "TAI".into()],
        vec![27, 9],
    )
    .unwrap();
    desc.write(&mut table).unwrap();
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("TIME", Value::Array(ArrayValue::from_f64_vec(vec![1.0]))),
            RecordField::new("TimeRef", Value::Scalar(ScalarValue::Int32(27))),
        ]))
        .unwrap();
    ScalarMeasColumnMut::new(&mut table, "TIME")
        .unwrap()
        .put_epoch(0, &MEpoch::from_mjd(51544.5, EpochRef::TAI))
        .unwrap();
    assert_eq!(
        table.cell_accessor(0, "TimeRef").unwrap().value().unwrap(),
        Some(&Value::Scalar(ScalarValue::Int32(9)))
    );
    assert!(
        ScalarMeasColumnMut::new(&mut table, "TIME")
            .unwrap()
            .put_epoch(0, &MEpoch::from_mjd(12345.0, EpochRef::TT))
            .is_err()
    );
    let value = ScalarMeasColumn::new(&table, "TIME")
        .unwrap()
        .get_epoch(0)
        .unwrap();
    assert_eq!(value.refer(), EpochRef::TAI);
    assert_eq!(value.value().as_mjd(), 51544.5);
}

#[test]
fn unrepresentable_inferred_reference_code_does_not_change_values() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_variable("FREQ", PrimitiveType::Float64, Some(1)),
        ColumnSchema::scalar("FreqRef", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let desc = TableMeasDesc::new_variable_int(
        "FREQ",
        MeasureType::Frequency,
        "FreqRef",
        vec!["LSRK".into()],
        vec![i32::MAX],
    )
    .unwrap();
    desc.write(&mut table).unwrap();
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("FREQ", Value::Array(ArrayValue::from_f64_vec(vec![1.4e9]))),
            RecordField::new("FreqRef", Value::Scalar(ScalarValue::Int32(i32::MAX))),
        ]))
        .unwrap();
    let undefined = MeasureValue::Frequency(MFrequency::new(2.0e9, FrequencyRef::Undefined));
    assert!(
        ScalarMeasColumnMut::new(&mut table, "FREQ")
            .unwrap()
            .put(0, &undefined)
            .is_err()
    );
    assert!(
        ArrayMeasColumnMut::new(&mut table, "FREQ")
            .unwrap()
            .put(0, &[undefined])
            .is_err()
    );
    let value = ScalarMeasColumn::new(&table, "FREQ")
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(value.as_frequency().unwrap().refer(), FrequencyRef::LSRK);
    assert_eq!(value.as_frequency().unwrap().hz(), 1.4e9);
}

#[test]
fn rr_epoch_var_str_ref() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
        ColumnSchema::scalar("TimeRefStr", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let desc = TableMeasDesc::new_variable_string("TIME", MeasureType::Epoch, "TimeRefStr");
    desc.write(&mut table).unwrap();

    let data = [(51544.5, "UTC"), (51545.0, "TAI"), (51546.5, "TT")];
    for &(mjd, refer_str) in &data {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("TIME", Value::Array(ArrayValue::from_f64_vec(vec![mjd]))),
                RecordField::new(
                    "TimeRefStr",
                    Value::Scalar(ScalarValue::String(refer_str.to_owned())),
                ),
            ]))
            .unwrap();
    }

    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
    for (i, &(mjd, refer_str)) in data.iter().enumerate() {
        let got = col.get_epoch(i).unwrap();
        assert_eq!(got.refer().as_str(), refer_str, "row {i}");
        assert!((got.value().as_mjd() - mjd).abs() < 1e-12);
    }
}

#[test]
fn rr_direction_fixed_ref() {
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "DIR",
        PrimitiveType::Float64,
        vec![2],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let desc = TableMeasDesc::new_fixed("DIR", MeasureType::Direction, "J2000");
    desc.write(&mut table).unwrap();

    let dirs = [
        MDirection::from_angles(1.0, 0.5, DirectionRef::J2000),
        MDirection::from_angles(2.0, -0.3, DirectionRef::J2000),
        MDirection::from_angles(0.0, 1.5, DirectionRef::J2000),
    ];
    for d in &dirs {
        let (lon, lat) = d.as_angles();
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DIR",
                Value::Array(ArrayValue::from_f64_vec(vec![lon, lat])),
            )]))
            .unwrap();
    }

    let col = ScalarMeasColumn::new(&table, "DIR").unwrap();
    for (i, expected) in dirs.iter().enumerate() {
        let got = col.get_direction(i).unwrap();
        assert_eq!(got.refer(), expected.refer());
        let (e_lon, e_lat) = expected.as_angles();
        let (g_lon, g_lat) = got.as_angles();
        assert!((e_lon - g_lon).abs() < 1e-12);
        assert!((e_lat - g_lat).abs() < 1e-12);
    }
}

#[test]
fn rr_direction_array() {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "DIRS",
        PrimitiveType::Float64,
        Some(1),
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let desc = TableMeasDesc::new_fixed("DIRS", MeasureType::Direction, "GALACTIC");
    desc.write(&mut table).unwrap();

    let dirs = vec![
        MDirection::from_angles(0.1, 0.2, DirectionRef::GALACTIC),
        MDirection::from_angles(0.3, 0.4, DirectionRef::GALACTIC),
    ];
    let mut values = Vec::new();
    for d in &dirs {
        let (lon, lat) = d.as_angles();
        values.push(lon);
        values.push(lat);
    }
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "DIRS",
            Value::Array(ArrayValue::from_f64_vec(values)),
        )]))
        .unwrap();

    let col = ArrayMeasColumn::new(&table, "DIRS").unwrap();
    let got = col.get(0).unwrap();
    assert_eq!(got.len(), 2);
    for (i, (expected, got_mv)) in dirs.iter().zip(got.iter()).enumerate() {
        let d = got_mv.as_direction().unwrap();
        let (e_lon, e_lat) = expected.as_angles();
        let (g_lon, g_lat) = d.as_angles();
        assert!((e_lon - g_lon).abs() < 1e-12, "dir {i} lon");
        assert!((e_lat - g_lat).abs() < 1e-12, "dir {i} lat");
    }
}

#[test]
fn rr_measinfo_reconstruct() {
    let table = make_epoch_table("TAI");
    let desc = TableMeasDesc::reconstruct(&table, "TIME").unwrap();
    assert_eq!(desc.measure_type(), MeasureType::Epoch);
    match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => assert_eq!(refer, "TAI"),
        _ => panic!("expected Fixed ref"),
    }
}

#[test]
fn rr_epoch_fixed_offset() {
    let mut table = make_epoch_table("TAI");
    // Overwrite with offset
    let offset_rec = record::epoch_to_record(&MEpoch::from_mjd(51544.0, EpochRef::UTC));
    let desc = TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "TAI")
        .with_fixed_offset(offset_rec.clone());
    desc.write(&mut table).unwrap();

    let reconstructed = TableMeasDesc::reconstruct(&table, "TIME").unwrap();
    assert!(reconstructed.offset_desc().is_some());
    match reconstructed.offset_desc().unwrap() {
        MeasOffsetDesc::Fixed { offset } => {
            let epoch = record::epoch_from_record(offset).unwrap();
            assert_eq!(epoch.refer(), EpochRef::UTC);
            assert!((epoch.value().as_mjd() - 51544.0).abs() < 1e-12);
        }
        _ => panic!("expected Fixed offset"),
    }
}

#[test]
fn rr_quantum_units_coexist() {
    use crate::table_quantum::TableQuantumDesc;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "TIME",
        PrimitiveType::Float64,
        vec![1],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    // Write quantum units
    TableQuantumDesc::with_unit("TIME", "d")
        .write(&mut table)
        .unwrap();
    // Write measinfo
    TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "UTC")
        .write(&mut table)
        .unwrap();

    // Both should be present
    assert!(TableQuantumDesc::has_quanta(&table, "TIME"));
    assert!(TableMeasDesc::has_measinfo(&table, "TIME"));
}

#[test]
fn rr_scalar_meas_column_mut() {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
        ColumnSchema::scalar("TimeRef", PrimitiveType::String),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let desc = TableMeasDesc::new_variable_string("TIME", MeasureType::Epoch, "TimeRef");
    desc.write(&mut table).unwrap();

    // Add empty rows
    for _ in 0..3 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("TIME", Value::Array(ArrayValue::from_f64_vec(vec![0.0]))),
                RecordField::new("TimeRef", Value::Scalar(ScalarValue::String(String::new()))),
            ]))
            .unwrap();
    }

    // Write measures
    {
        let mut col = ScalarMeasColumnMut::new(&mut table, "TIME").unwrap();
        col.put_epoch(0, &MEpoch::from_mjd(51544.5, EpochRef::UTC))
            .unwrap();
        col.put_epoch(1, &MEpoch::from_mjd(51545.0, EpochRef::TAI))
            .unwrap();
        col.put_epoch(2, &MEpoch::from_mjd(51546.5, EpochRef::TT))
            .unwrap();
    }

    // Read back
    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
    let e0 = col.get_epoch(0).unwrap();
    assert_eq!(e0.refer(), EpochRef::UTC);
    assert!((e0.value().as_mjd() - 51544.5).abs() < 1e-12);

    let e1 = col.get_epoch(1).unwrap();
    assert_eq!(e1.refer(), EpochRef::TAI);

    let e2 = col.get_epoch(2).unwrap();
    assert_eq!(e2.refer(), EpochRef::TT);
}

#[test]
fn measure_type_values_per_measure() {
    assert_eq!(MeasureType::Epoch.values_per_measure(), 1);
    assert_eq!(MeasureType::Direction.values_per_measure(), 2);
    assert_eq!(MeasureType::Position.values_per_measure(), 3);
    assert_eq!(MeasureType::Frequency.values_per_measure(), 1);
    assert_eq!(MeasureType::Doppler.values_per_measure(), 1);
    assert_eq!(MeasureType::RadialVelocity.values_per_measure(), 1);
}

#[test]
fn default_ref_maps() {
    let (types, codes) = default_epoch_ref_map();
    assert_eq!(types.len(), 12);
    assert_eq!(codes.len(), 12);
    assert_eq!(types[6], "UTC");
    assert_eq!(codes[6], 6);

    let (types, codes) = default_direction_ref_map();
    assert_eq!(types.len(), 22);
    for (name, code) in [("B1950_VLA", 5), ("BMEAN", 6), ("BTRUE", 7)] {
        let index = types.iter().position(|value| value == name).unwrap();
        assert_eq!(codes[index], code);
    }
    // J2000 should be code 0
    let j2000_idx = types.iter().position(|t| t == "J2000").unwrap();
    assert_eq!(codes[j2000_idx], 0);

    let (types, codes) = default_frequency_ref_map();
    assert_eq!(types.len(), 10);
    assert_eq!(types[0], "REST");
    assert_eq!(codes[0], 0);
    assert_eq!(types[9], "Undefined");
    assert_eq!(codes[9], 64);
}

#[test]
fn array_meas_put_rejects_mixed_references() {
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "DIRS",
        PrimitiveType::Float64,
        Some(1),
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    let desc = TableMeasDesc::new_fixed("DIRS", MeasureType::Direction, "J2000");
    desc.write(&mut table).unwrap();

    table.add_row(RecordValue::default()).unwrap();

    let measures = vec![
        MeasureValue::Direction(MDirection::from_angles(1.0, 0.5, DirectionRef::J2000)),
        MeasureValue::Direction(MDirection::from_angles(2.0, -0.3, DirectionRef::GALACTIC)),
    ];
    let mut col = ArrayMeasColumnMut::new(&mut table, "DIRS").unwrap();
    let err = col.put(0, &measures).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("measure[1]") && msg.contains("GALACTIC") && msg.contains("J2000"),
        "unexpected error: {msg}"
    );
}

#[test]
fn new_variable_int_rejects_mismatched_lengths() {
    let types = vec!["UTC".to_owned(), "TAI".to_owned()];
    let codes = vec![0]; // length mismatch
    let err = TableMeasDesc::new_variable_int("TIME", MeasureType::Epoch, "TimeRef", types, codes)
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("TabRefTypes length (2)") && msg.contains("TabRefCodes length (1)"),
        "unexpected error: {msg}"
    );
}

#[test]
fn resolve_ref_string_returns_error_for_corrupted_measinfo() {
    // Construct a table with a variable-int ref column but
    // with a manually corrupted TabRefTypes/TabRefCodes mapping
    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("TIME", PrimitiveType::Float64, vec![1]),
        ColumnSchema::scalar("TimeRef", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);

    // Use a valid ref map to write MEASINFO
    let (types, codes) = default_epoch_ref_map();
    let desc = TableMeasDesc::new_variable_int("TIME", MeasureType::Epoch, "TimeRef", types, codes)
        .unwrap();
    desc.write(&mut table).unwrap();

    // Add a row with a code that doesn't exist in the mapping
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "TIME",
                Value::Array(ArrayValue::from_f64_vec(vec![51544.5])),
            ),
            RecordField::new("TimeRef", Value::Scalar(ScalarValue::Int32(9999))),
        ]))
        .unwrap();

    let col = ScalarMeasColumn::new(&table, "TIME").unwrap();
    let err = col.get_epoch(0).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ref code 9999 not found"),
        "unexpected error: {msg}"
    );
}
