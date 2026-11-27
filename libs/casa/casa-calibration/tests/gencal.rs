// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::common;

use casa_calibration::gencal::{GencalRequest, GencalType, gencal};
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
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
    // NewCalTable::flagAbsentSpws marks windows without parameter rows.
    let windows = Table::open(TableOptions::new(caltable_path.join("SPECTRAL_WINDOW"))).unwrap();
    for row in 0..windows.row_count() {
        assert_eq!(
            windows
                .cell_accessor(row, "FLAG_ROW")
                .unwrap()
                .value()
                .unwrap(),
            Some(&Value::Scalar(ScalarValue::Bool(row != 0)))
        );
    }
}

#[test]
fn gencal_refuses_a_non_table_destination_without_modifying_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let caltable_path = dir.path().join("existing.cal");
    std::fs::create_dir(&caltable_path).expect("create existing output");
    let sentinel = caltable_path.join("sentinel");
    std::fs::write(&sentinel, b"keep").expect("write sentinel");

    let error = gencal(&GencalRequest {
        measurement_set: ms_path,
        output_table: caltable_path,
        caltype: GencalType::Opac,
        antenna: String::new(),
        spw: "0".to_string(),
        parameter: vec![0.1],
        gaincurve_table: None,
    })
    .expect_err("existing output must be rejected");

    // Updating an existing calibration table is now valid; a plain directory is refused (#945).
    assert!(error.to_string().contains("not a Calibration"));
    assert_eq!(std::fs::read(sentinel).expect("sentinel remains"), b"keep");
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

    // One opacity holds for every selected window, as in CASA's specifycal.
    let one_path = dir.path().join("one-opacity.cal");
    gencal(&GencalRequest {
        measurement_set: ms_path.clone(),
        output_table: one_path.clone(),
        caltype: GencalType::Opac,
        antenna: String::new(),
        spw: "0,1".to_string(),
        parameter: vec![0.05],
        gaincurve_table: None,
    })
    .expect("one opacity for two windows");
    let table = Table::open(TableOptions::new(&one_path)).expect("open opacity table");
    for row in 0..6 {
        assert_fparam(&table, row, &[0.05]);
    }

    let err = gencal(&GencalRequest {
        measurement_set: ms_path,
        output_table: dir.path().join("bad-opacity.cal"),
        caltype: GencalType::Opac,
        antenna: String::new(),
        spw: "0,1".to_string(),
        parameter: vec![0.11, 0.22, 0.33],
        gaincurve_table: None,
    })
    .unwrap_err();
    assert!(err.to_string().contains("expected 1 or 2, got 3"), "{err}");
}

#[test]
fn gencal_gceff_uses_gaincurve_rows_and_vla_efficiency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path =
        common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let gaincurve_path = dir.path().join("GainCurves");
    write_gaincurve_table(&gaincurve_path);
    let caltable_path = dir.path().join("gceff.cal");

    let request = GencalRequest {
        measurement_set: ms_path,
        output_table: caltable_path.clone(),
        caltype: GencalType::Gceff,
        antenna: String::new(),
        spw: String::new(),
        parameter: vec![],
        gaincurve_table: Some(gaincurve_path),
    };
    let report = gencal(&request).expect("gceff gencal");

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
    // EGainCurve::setSpecify regenerates absolute coefficients (#945).
    let first = table.rows().unwrap().to_vec();
    drop(table);
    gencal(&request).unwrap();
    assert_eq!(
        Table::open(TableOptions::new(&caltable_path))
            .unwrap()
            .rows()
            .unwrap(),
        first.as_slice()
    );
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
    assert!(
        err.to_string()
            .contains("requires resolved antenna selections")
    );

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
    assert!(err.to_string().contains("No match found for 9"), "{err}");
}

fn write_gaincurve_table(path: &std::path::Path) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("BFREQ", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("EFREQ", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("BTIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("ETIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("ANTENNA", casa_core::PrimitiveType::String),
        ColumnSchema::array_variable("GAIN", casa_core::PrimitiveType::Float32, Some(2)),
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
