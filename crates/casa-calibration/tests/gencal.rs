// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_calibration::{GencalRequest, GencalType, gencal};
use casa_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

#[test]
fn gencal_antpos_writes_offsets_for_selected_antennas_and_zeroes_others() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let caltable_path = dir.path().join("antpos.cal");

    let report = gencal(&GencalRequest {
        measurement_set: ms_path,
        output_table: caltable_path.clone(),
        caltype: GencalType::Antpos,
        antenna: "ANT0,1".to_string(),
        spw: String::new(),
        parameter: vec![1.0, 2.0, 3.0, -4.0, -5.0, -6.0],
        gaincurve_table: None,
    })
    .expect("antpos gencal");

    assert_eq!(report.caltype, GencalType::Antpos);
    assert_eq!(report.table_subtype, "KAntPos Jones");
    assert_eq!(report.row_count, 3);
    assert_eq!(report.spectral_window_ids, vec![0]);
    assert_eq!(report.antenna_ids, vec![0, 1, 2]);

    let table = Table::open(TableOptions::new(&caltable_path)).expect("open antpos table");
    assert_eq!(table.row_count(), 3);
    assert_eq!(
        table.keywords().get("VisCal"),
        Some(&Value::Scalar(ScalarValue::String(
            "KAntPos Jones".to_string()
        )))
    );
    assert_fparam(&table, 0, &[1.0, 2.0, 3.0]);
    assert_fparam(&table, 1, &[-4.0, -5.0, -6.0]);
    assert_fparam(&table, 2, &[0.0, 0.0, 0.0]);
    assert!(caltable_path.join("ANTENNA").exists());
    assert!(caltable_path.join("SPECTRAL_WINDOW").exists());
}

#[test]
fn gencal_opacity_expands_selected_spws_across_antennas() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let caltable_path = dir.path().join("opacity.cal");

    let report = gencal(&GencalRequest {
        measurement_set: ms_path.clone(),
        output_table: caltable_path.clone(),
        caltype: GencalType::Opac,
        antenna: String::new(),
        spw: "0~1".to_string(),
        parameter: vec![0.11, 0.22],
        gaincurve_table: None,
    })
    .expect("opac gencal");

    assert_eq!(report.caltype, GencalType::Opac);
    assert_eq!(report.table_subtype, "TOpac");
    assert_eq!(report.row_count, 6);
    assert_eq!(report.spectral_window_ids, vec![0, 1]);
    assert_eq!(report.antenna_ids, vec![0, 1, 2]);

    let table = Table::open(TableOptions::new(&caltable_path)).expect("open opacity table");
    for row in 0..3 {
        assert_scalar_i32(&table, row, "SPECTRAL_WINDOW_ID", 0);
        assert_fparam(&table, row, &[0.11]);
    }
    for row in 3..6 {
        assert_scalar_i32(&table, row, "SPECTRAL_WINDOW_ID", 1);
        assert_fparam(&table, row, &[0.22]);
    }

    let err = gencal(&GencalRequest {
        measurement_set: ms_path,
        output_table: dir.path().join("bad-opacity.cal"),
        caltype: GencalType::Opac,
        antenna: String::new(),
        spw: "0,1".to_string(),
        parameter: vec![0.11],
        gaincurve_table: None,
    })
    .unwrap_err();
    assert!(err.to_string().contains("one parameter per selected SPW"));
}

#[test]
fn gencal_gceff_uses_gaincurve_rows_and_vla_efficiency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let gaincurve_path = dir.path().join("GainCurves");
    write_gaincurve_table(&gaincurve_path);
    let caltable_path = dir.path().join("gceff.cal");

    let report = gencal(&GencalRequest {
        measurement_set: ms_path,
        output_table: caltable_path.clone(),
        caltype: GencalType::Gceff,
        antenna: String::new(),
        spw: String::new(),
        parameter: vec![],
        gaincurve_table: Some(gaincurve_path),
    })
    .expect("gceff gencal");

    assert_eq!(report.caltype, GencalType::Gceff);
    assert_eq!(report.table_subtype, "EGainCurve");
    assert_eq!(report.row_count, 6);
    assert_eq!(report.spectral_window_ids, vec![0, 1]);
    assert_eq!(report.antenna_ids, vec![0, 1, 2]);

    let table = Table::open(TableOptions::new(&caltable_path)).expect("open gceff table");
    let row0 = fparam_vec(&table, 0);
    let row1 = fparam_vec(&table, 1);
    let row2 = fparam_vec(&table, 2);
    assert_eq!(row0, row1);
    assert!((row0[1] / row0[0] - 2.0).abs() < 1.0e-6);
    assert!((row2[0] / row0[0] - 3.0).abs() < 1.0e-6);
    assert!((row2[1] / row0[1] - 2.0).abs() < 1.0e-6);
    assert_scalar_i32(&table, 3, "SPECTRAL_WINDOW_ID", 1);
    assert_scalar_i32(&table, 0, "OBSERVATION_ID", -1);
}

#[test]
fn gencal_rejects_invalid_antpos_and_spw_contracts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);

    let err = gencal(&GencalRequest {
        measurement_set: ms_path.clone(),
        output_table: dir.path().join("missing-antpos.cal"),
        caltype: GencalType::Antpos,
        antenna: String::new(),
        spw: String::new(),
        parameter: vec![],
        gaincurve_table: None,
    })
    .unwrap_err();
    assert!(err.to_string().contains("requires explicit --antenna"));

    let err = gencal(&GencalRequest {
        measurement_set: ms_path,
        output_table: dir.path().join("bad-spw.cal"),
        caltype: GencalType::Opac,
        antenna: String::new(),
        spw: "9".to_string(),
        parameter: vec![0.1],
        gaincurve_table: None,
    })
    .unwrap_err();
    assert!(err.to_string().contains("outside 0.."));
}

fn write_gaincurve_table(path: &std::path::Path) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("BFREQ", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("EFREQ", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("BTIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("ETIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("ANTENNA", casa_types::PrimitiveType::String),
        ColumnSchema::array_variable("GAIN", casa_types::PrimitiveType::Float32, Some(2)),
    ])
    .expect("gaincurve schema");
    let mut table = Table::with_schema(schema);
    for (antenna, gain) in [("0", [1.0, 2.0]), ("2", [3.0, 4.0])] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("BFREQ", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new("EFREQ", Value::Scalar(ScalarValue::Float64(50.0e9))),
                RecordField::new("BTIME", Value::Scalar(ScalarValue::Float64(-1.0))),
                RecordField::new("ETIME", Value::Scalar(ScalarValue::Float64(1.0e12))),
                RecordField::new(
                    "ANTENNA",
                    Value::Scalar(ScalarValue::String(antenna.to_string())),
                ),
                RecordField::new(
                    "GAIN",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), gain.to_vec())
                            .expect("gain shape"),
                    )),
                ),
            ]))
            .expect("gaincurve row");
    }
    table
        .save(TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save gaincurve table");
}

fn assert_fparam(table: &Table, row: usize, expected: &[f32]) {
    let value = table
        .cell_accessor(row, "FPARAM")
        .and_then(|cell| cell.array())
        .expect("FPARAM");
    let ArrayValue::Float32(values) = value else {
        panic!("expected Float32 FPARAM");
    };
    assert_eq!(values.shape(), &[expected.len(), 1]);
    for (index, expected) in expected.iter().copied().enumerate() {
        assert!((values[[index, 0]] - expected).abs() < 1.0e-6);
    }
}

fn fparam_vec(table: &Table, row: usize) -> Vec<f32> {
    let value = table
        .cell_accessor(row, "FPARAM")
        .and_then(|cell| cell.array())
        .expect("FPARAM");
    let ArrayValue::Float32(values) = value else {
        panic!("expected Float32 FPARAM");
    };
    values.iter().copied().collect()
}

fn assert_scalar_i32(table: &Table, row: usize, column: &str, expected: i32) {
    assert_eq!(
        table
            .cell_accessor(row, column)
            .and_then(|cell| cell.scalar())
            .expect(column),
        &ScalarValue::Int32(expected)
    );
}
