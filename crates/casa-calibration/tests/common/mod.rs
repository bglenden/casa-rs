// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

#[cfg(feature = "slow-tests")]
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(feature = "slow-tests")]
use std::process::Command;

use casa_ms::MeasurementSetBuilder;
use casa_ms::column_def::{ColumnDef, ColumnKind, build_table_schema};
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::schema::{self as ms_schema, SubtableId};
use casa_tables::{ColumnSchema, DataManagerKind, Table, TableInfo, TableOptions, TableSchema};
#[cfg(feature = "slow-tests")]
use casa_test_support::casatestdata_path;
use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
#[cfg(feature = "slow-tests")]
use serde_json::Value as JsonValue;

pub fn create_minimal_complex_caltable(root: &Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable("CPARAM", casa_types::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("PARAMERR", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_types::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("SNR", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("WEIGHT", casa_types::PrimitiveType::Float32, Some(1)),
    ])
    .expect("valid schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
    });
    table.keywords_mut().upsert(
        "ParType",
        Value::Scalar(ScalarValue::String("Complex".to_string())),
    );
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("G Jones".to_string())),
    );
    table.keywords_mut().upsert(
        "MSName",
        Value::Scalar(ScalarValue::String("synthetic.ms".to_string())),
    );
    table.keywords_mut().upsert(
        "PolBasis",
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        "CASA_Version",
        Value::Scalar(ScalarValue::String("test".to_string())),
    );
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        table
            .keywords_mut()
            .upsert(name, Value::table_ref(format!("././{name}")));
    }
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(1.0))),
            RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(3))),
            RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
            RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(7))),
            RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "CPARAM",
                Value::Array(ArrayValue::from_complex32_vec(vec![
                    Complex32 { re: 1.0, im: 0.0 },
                    Complex32 { re: 0.0, im: 1.0 },
                ])),
            ),
            RecordField::new(
                "PARAMERR",
                Value::Array(ArrayValue::from_f32_vec(vec![0.1, 0.1])),
            ),
            RecordField::new(
                "FLAG",
                Value::Array(ArrayValue::from_bool_vec(vec![false, false])),
            ),
            RecordField::new(
                "SNR",
                Value::Array(ArrayValue::from_f32_vec(vec![10.0, 11.0])),
            ),
            RecordField::new(
                "WEIGHT",
                Value::Array(ArrayValue::from_f32_vec(vec![1.0, 1.0])),
            ),
        ]))
        .expect("row insert");
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save synthetic caltable");

    let empty_schema = TableSchema::new(vec![]).expect("empty schema");
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        Table::with_schema(empty_schema.clone())
            .save(TableOptions::new(root.join(name)))
            .expect("save subtable");
    }
    root.to_path_buf()
}

pub fn assert_corrected_rows_are_unit_model(ms_path: &Path) {
    let ms = MeasurementSet::open(ms_path).expect("reopen measurement set");
    let corrected = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data column");
    for row in 0..ms.row_count() {
        let ArrayValue::Complex32(values) = corrected.get(row).expect("corrected row") else {
            panic!("expected complex corrected data");
        };
        for value in values.iter() {
            assert!(
                (value.re - 1.0).abs() <= 1.0e-3 && value.im.abs() <= 1.0e-3,
                "expected corrected value close to 1+0i, got ({:.6},{:.6})",
                value.re,
                value.im
            );
        }
    }
}

pub fn create_apply_fixture_ms(root: &Path, include_corrected_data: bool) -> PathBuf {
    create_apply_fixture_ms_with_options(root, include_corrected_data, false)
}

pub fn create_apply_fixture_ms_with_options(
    root: &Path,
    include_corrected_data: bool,
    include_weight_spectrum: bool,
) -> PathBuf {
    let ms_path = root.join("apply_fixture.ms");
    let mut builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    if include_corrected_data {
        builder = builder.with_main_column(OptionalMainColumn::CorrectedData);
    }
    if include_weight_spectrum {
        builder = builder.with_main_column(OptionalMainColumn::WeightSpectrum);
    }
    let mut ms = MeasurementSet::create(&ms_path, builder).expect("create apply fixture MS");

    populate_apply_fixture_subtables(&mut ms);
    add_apply_fixture_row(&mut ms, 0, 0, 0, 100.0);
    add_apply_fixture_row(&mut ms, 1, 1, 1, 200.0);

    ms.save().expect("save apply fixture MS");
    ms_path
}

pub fn create_apply_fixture_caltable(
    root: &Path,
    field_names: &[&str],
    field_ids: &[i32],
    spectral_window_ids: &[i32],
) -> PathBuf {
    assert_eq!(
        field_ids.len(),
        spectral_window_ids.len(),
        "each caltable row needs one field id and one spectral window id"
    );

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ARRAY_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("TIME_EXTRA_PREC", casa_types::PrimitiveType::Float64),
        ColumnSchema::array_variable("CPARAM", casa_types::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("PARAMERR", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_types::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("SNR", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("WEIGHT", casa_types::PrimitiveType::Float32, Some(1)),
    ])
    .expect("valid schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
    });
    table.keywords_mut().upsert(
        "ParType",
        Value::Scalar(ScalarValue::String("Complex".to_string())),
    );
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("G Jones".to_string())),
    );
    table.keywords_mut().upsert(
        "MSName",
        Value::Scalar(ScalarValue::String("apply_fixture.ms".to_string())),
    );
    table.keywords_mut().upsert(
        "PolBasis",
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        "CASA_Version",
        Value::Scalar(ScalarValue::String("test".to_string())),
    );
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    set_fixed_unit_keyword(&mut table, "TIME_EXTRA_PREC", &["s"]);
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        table
            .keywords_mut()
            .upsert(name, Value::table_ref(format!("././{name}")));
    }

    for (row_index, (&field_id, &spw_id)) in field_ids.iter().zip(spectral_window_ids).enumerate() {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(1_000.0 + row_index as f64)),
                ),
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id))),
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Scalar(ScalarValue::Int32(spw_id)),
                ),
                RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
                RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("TIME_EXTRA_PREC", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new(
                    "CPARAM",
                    Value::Array(ArrayValue::from_complex32_vec(vec![
                        Complex32 { re: 1.0, im: 0.0 },
                        Complex32 { re: 1.0, im: 0.0 },
                    ])),
                ),
                RecordField::new(
                    "PARAMERR",
                    Value::Array(ArrayValue::from_f32_vec(vec![0.1, 0.1])),
                ),
                RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::from_bool_vec(vec![false, false])),
                ),
                RecordField::new(
                    "SNR",
                    Value::Array(ArrayValue::from_f32_vec(vec![10.0, 11.0])),
                ),
                RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::from_f32_vec(vec![1.0, 1.0])),
                ),
            ]))
            .expect("insert calibration row");
    }

    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save apply caltable");

    write_caltable_field_subtable(root.join("FIELD"), field_names);
    write_caltable_spectral_window_subtable(root.join("SPECTRAL_WINDOW"), spectral_window_ids);
    write_minimal_observation_table(root.join("OBSERVATION"));
    write_minimal_antenna_table(root.join("ANTENNA"), 2);
    write_empty_table(root.join("HISTORY"));

    root.to_path_buf()
}

pub struct SyntheticGainSolutionRow {
    pub time_seconds: f64,
    pub field_id: i32,
    pub spectral_window_id: i32,
    pub antenna_id: i32,
    pub gains: Vec<Complex32>,
    pub flags: Vec<bool>,
}

pub struct SyntheticDelaySolutionRow {
    pub time_seconds: f64,
    pub field_id: i32,
    pub spectral_window_id: i32,
    pub antenna_id: i32,
    pub delays_ns: Vec<f32>,
    pub flags: Vec<bool>,
}

pub struct SyntheticBPolySolutionRow {
    pub time_seconds: f64,
    pub field_id: i32,
    pub spectral_window_id: i32,
    pub antenna_id: i32,
    pub scale_factor: Complex32,
    pub valid_domain_hz: [f64; 2],
    pub amp_coefficients: Vec<Vec<f64>>,
    pub phase_coefficients: Vec<Vec<f64>>,
    pub phase_units: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub enum SyntheticGainFixtureKind {
    G,
    T,
    GAmplitudePhase,
    TAmplitudePhase,
}

pub struct SyntheticGainTimeCluster {
    pub time_seconds: f64,
    pub scan_number: i32,
    pub gains: [[Complex32; 2]; 3],
}

pub struct SyntheticBandpassTimeCluster {
    pub field_id: i32,
    pub time_seconds: f64,
    pub scan_number: i32,
    pub prior_gains: [[Complex32; 2]; 3],
    pub bandpass_gains: [[[Complex32; 2]; 2]; 3],
}

#[cfg(feature = "slow-tests")]
pub struct CasaGaincalOptions<'a> {
    pub field: &'a str,
    pub spw: &'a str,
    pub refant: &'a str,
    pub gaintype: &'a str,
    pub calmode: &'a str,
    pub solint: &'a str,
    pub combine: &'a str,
    pub prior_gaintables: Vec<&'a Path>,
    pub parang: bool,
}

pub fn create_apply_gain_caltable(
    root: &Path,
    field_names: &[&str],
    rows: &[SyntheticGainSolutionRow],
) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ARRAY_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("TIME_EXTRA_PREC", casa_types::PrimitiveType::Float64),
        ColumnSchema::array_variable("CPARAM", casa_types::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("PARAMERR", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_types::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("SNR", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("WEIGHT", casa_types::PrimitiveType::Float32, Some(1)),
    ])
    .expect("valid gain caltable schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
    });
    table.keywords_mut().upsert(
        "ParType",
        Value::Scalar(ScalarValue::String("Complex".to_string())),
    );
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("G Jones".to_string())),
    );
    table.keywords_mut().upsert(
        "MSName",
        Value::Scalar(ScalarValue::String("apply_fixture.ms".to_string())),
    );
    table.keywords_mut().upsert(
        "PolBasis",
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        "CASA_Version",
        Value::Scalar(ScalarValue::String("test".to_string())),
    );
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    set_fixed_unit_keyword(&mut table, "TIME_EXTRA_PREC", &["s"]);
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        table
            .keywords_mut()
            .upsert(name, Value::table_ref(format!("././{name}")));
    }

    for row in rows {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(row.field_id))),
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Scalar(ScalarValue::Int32(row.spectral_window_id)),
                ),
                RecordField::new(
                    "ANTENNA1",
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
                RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("TIME_EXTRA_PREC", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new(
                    "CPARAM",
                    Value::Array(ArrayValue::from_complex32_vec(row.gains.clone())),
                ),
                RecordField::new(
                    "PARAMERR",
                    Value::Array(ArrayValue::from_f32_vec(vec![0.1; row.gains.len()])),
                ),
                RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::from_bool_vec(row.flags.clone())),
                ),
                RecordField::new(
                    "SNR",
                    Value::Array(ArrayValue::from_f32_vec(vec![10.0; row.gains.len()])),
                ),
                RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::from_f32_vec(vec![1.0; row.gains.len()])),
                ),
            ]))
            .expect("insert gain solution row");
    }

    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save gain caltable");

    let spectral_window_ids = rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect::<Vec<_>>();
    write_caltable_field_subtable(root.join("FIELD"), field_names);
    write_caltable_spectral_window_subtable(root.join("SPECTRAL_WINDOW"), &spectral_window_ids);
    write_minimal_observation_table(root.join("OBSERVATION"));
    let antenna_count = rows
        .iter()
        .map(|row| row.antenna_id)
        .max()
        .map_or(0usize, |max| usize::try_from(max + 1).unwrap_or(0));
    write_minimal_antenna_table(root.join("ANTENNA"), antenna_count.max(1));
    write_empty_table(root.join("HISTORY"));

    root.to_path_buf()
}

pub fn create_apply_delay_caltable(
    root: &Path,
    field_names: &[&str],
    rows: &[SyntheticDelaySolutionRow],
) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ARRAY_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("TIME_EXTRA_PREC", casa_types::PrimitiveType::Float64),
        ColumnSchema::array_variable("FPARAM", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_types::PrimitiveType::Bool, Some(1)),
    ])
    .expect("valid delay caltable schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "K Jones".to_string(),
    });
    table.keywords_mut().upsert(
        "ParType",
        Value::Scalar(ScalarValue::String("Float".to_string())),
    );
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("K Jones".to_string())),
    );
    table.keywords_mut().upsert(
        "MSName",
        Value::Scalar(ScalarValue::String("apply_fixture.ms".to_string())),
    );
    table.keywords_mut().upsert(
        "PolBasis",
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        "CASA_Version",
        Value::Scalar(ScalarValue::String("test".to_string())),
    );
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    set_fixed_unit_keyword(&mut table, "TIME_EXTRA_PREC", &["s"]);
    set_fixed_unit_keyword(&mut table, "FPARAM", &["ns"]);
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        table
            .keywords_mut()
            .upsert(name, Value::table_ref(format!("././{name}")));
    }

    for row in rows {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(row.field_id))),
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Scalar(ScalarValue::Int32(row.spectral_window_id)),
                ),
                RecordField::new(
                    "ANTENNA1",
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
                RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("TIME_EXTRA_PREC", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new(
                    "FPARAM",
                    Value::Array(ArrayValue::from_f32_vec(row.delays_ns.clone())),
                ),
                RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::from_bool_vec(row.flags.clone())),
                ),
            ]))
            .expect("insert delay solution row");
    }

    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save delay caltable");

    let spectral_window_ids = rows
        .iter()
        .map(|row| row.spectral_window_id)
        .collect::<Vec<_>>();
    write_caltable_field_subtable(root.join("FIELD"), field_names);
    write_delay_caltable_spectral_window_subtable(
        root.join("SPECTRAL_WINDOW"),
        &spectral_window_ids,
    );
    write_minimal_observation_table(root.join("OBSERVATION"));
    let antenna_count = rows
        .iter()
        .map(|row| row.antenna_id)
        .max()
        .map_or(0usize, |max| usize::try_from(max + 1).unwrap_or(0));
    write_minimal_antenna_table(root.join("ANTENNA"), antenna_count.max(1));
    write_empty_table(root.join("HISTORY"));

    root.to_path_buf()
}

pub fn create_apply_bpoly_caltable(root: &Path, rows: &[SyntheticBPolySolutionRow]) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("CAL_DESC_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SCALE_FACTOR", casa_types::PrimitiveType::Complex32),
        ColumnSchema::scalar("N_POLY_AMP", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("N_POLY_PHASE", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("PHASE_UNITS", casa_types::PrimitiveType::String),
        ColumnSchema::array_variable("VALID_DOMAIN", casa_types::PrimitiveType::Float64, Some(1)),
        ColumnSchema::array_variable(
            "POLY_COEFF_AMP",
            casa_types::PrimitiveType::Float64,
            Some(4),
        ),
        ColumnSchema::array_variable(
            "POLY_COEFF_PHASE",
            casa_types::PrimitiveType::Float64,
            Some(4),
        ),
    ])
    .expect("valid BPOLY schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "BPOLY".to_string(),
    });
    table
        .keywords_mut()
        .upsert("CAL_DESC", Value::table_ref("././CAL_DESC"));
    table
        .keywords_mut()
        .upsert("CAL_HISTORY", Value::table_ref("././CAL_HISTORY"));
    set_fixed_unit_keyword(&mut table, "TIME", &["s"]);
    set_measinfo_keyword(&mut table, "TIME", "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, "INTERVAL", &["s"]);
    set_fixed_unit_keyword(&mut table, "VALID_DOMAIN", &["Hz"]);

    let mut cal_desc_ids = std::collections::BTreeMap::new();
    for row in rows {
        let next_id = i32::try_from(cal_desc_ids.len()).expect("small synthetic CAL_DESC table");
        cal_desc_ids
            .entry(row.spectral_window_id)
            .or_insert(next_id);
    }

    for row in rows {
        assert_eq!(
            row.amp_coefficients.len(),
            row.phase_coefficients.len(),
            "BPOLY amp/phase receptor counts must match"
        );
        let receptor_count = row.amp_coefficients.len().max(1);
        let amp_degree = row
            .amp_coefficients
            .first()
            .map_or(0_i32, |coefficients| coefficients.len() as i32);
        let phase_degree = row
            .phase_coefficients
            .first()
            .map_or(0_i32, |coefficients| coefficients.len() as i32);
        assert!(
            row.amp_coefficients
                .iter()
                .all(|coefficients| coefficients.len() as i32 == amp_degree)
        );
        assert!(
            row.phase_coefficients
                .iter()
                .all(|coefficients| coefficients.len() as i32 == phase_degree)
        );
        let flat_amp = row
            .amp_coefficients
            .iter()
            .flat_map(|coefficients| coefficients.iter().copied())
            .collect::<Vec<_>>();
        let flat_phase = row
            .phase_coefficients
            .iter()
            .flat_map(|coefficients| coefficients.iter().copied())
            .collect::<Vec<_>>();
        let amp_array = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[1, 1, 1, flat_amp.len()]).f(), flat_amp).unwrap(),
        );
        let phase_array = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[1, 1, 1, flat_phase.len()]).f(), flat_phase).unwrap(),
        );
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(row.field_id))),
                RecordField::new(
                    "ANTENNA1",
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
                RecordField::new(
                    "CAL_DESC_ID",
                    Value::Scalar(ScalarValue::Int32(
                        *cal_desc_ids
                            .get(&row.spectral_window_id)
                            .expect("synthetic CAL_DESC entry"),
                    )),
                ),
                RecordField::new(
                    "SCALE_FACTOR",
                    Value::Scalar(ScalarValue::Complex32(row.scale_factor)),
                ),
                RecordField::new("N_POLY_AMP", Value::Scalar(ScalarValue::Int32(amp_degree))),
                RecordField::new(
                    "N_POLY_PHASE",
                    Value::Scalar(ScalarValue::Int32(phase_degree)),
                ),
                RecordField::new(
                    "PHASE_UNITS",
                    Value::Scalar(ScalarValue::String(row.phase_units.to_string())),
                ),
                RecordField::new(
                    "VALID_DOMAIN",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2]).f(),
                            vec![row.valid_domain_hz[0], row.valid_domain_hz[1]],
                        )
                        .unwrap(),
                    )),
                ),
                RecordField::new("POLY_COEFF_AMP", Value::Array(amp_array)),
                RecordField::new("POLY_COEFF_PHASE", Value::Array(phase_array)),
            ]))
            .expect("insert BPOLY solution row");
        assert!(receptor_count > 0);
    }

    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save BPOLY caltable");

    write_bpoly_cal_desc_subtable(root.join("CAL_DESC"), &cal_desc_ids, rows);
    write_empty_table(root.join("CAL_HISTORY"));

    root.to_path_buf()
}

pub fn set_ms_field_directions(path: &Path, directions: &[(usize, f64, f64)]) {
    set_field_directions_in_table(&path.join("FIELD"), directions);
}

pub fn set_caltable_field_directions(path: &Path, directions: &[(usize, f64, f64)]) {
    set_field_directions_in_table(&path.join("FIELD"), directions);
}

pub fn set_ms_antenna_mounts(path: &Path, mounts: &[(usize, &str)]) {
    set_antenna_mounts_in_table(&path.join("ANTENNA"), mounts);
}

#[cfg(feature = "slow-tests")]
pub fn copy_ms_subtables_into_caltable(ms_path: &Path, caltable_path: &Path, subtables: &[&str]) {
    for subtable in subtables {
        let source = ms_path.join(subtable);
        let destination = caltable_path.join(subtable);
        if destination.exists() {
            fs::remove_dir_all(&destination)
                .or_else(|_| fs::remove_file(&destination))
                .expect("remove existing caltable subtable");
        }
        copy_measurement_set(&source, &destination).expect("copy MS subtable into caltable");
    }
}

pub fn create_gain_solve_fixture_ms(root: &Path, kind: SyntheticGainFixtureKind) -> PathBuf {
    create_gain_solve_fixture_ms_from_clusters(
        root,
        &[
            SyntheticGainTimeCluster {
                time_seconds: 100.0,
                scan_number: 1,
                gains: gains_for_fixture_kind(kind),
            },
            SyntheticGainTimeCluster {
                time_seconds: 101.0,
                scan_number: 1,
                gains: gains_for_fixture_kind(kind),
            },
            SyntheticGainTimeCluster {
                time_seconds: 102.0,
                scan_number: 1,
                gains: gains_for_fixture_kind(kind),
            },
        ],
    )
}

pub fn create_gain_solve_fixture_ms_from_clusters(
    root: &Path,
    clusters: &[SyntheticGainTimeCluster],
) -> PathBuf {
    let ms_path = root.join("gain_solve_fixture.ms");
    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let mut ms = MeasurementSet::create(&ms_path, builder).expect("create gain solve fixture MS");

    populate_gain_solve_subtables(&mut ms);
    for cluster in clusters {
        add_custom_gain_solve_row(
            &mut ms,
            0,
            1,
            0,
            cluster.scan_number,
            cluster.time_seconds,
            &cluster.gains,
        );
        add_custom_gain_solve_row(
            &mut ms,
            0,
            2,
            0,
            cluster.scan_number,
            cluster.time_seconds,
            &cluster.gains,
        );
        add_custom_gain_solve_row(
            &mut ms,
            1,
            2,
            0,
            cluster.scan_number,
            cluster.time_seconds,
            &cluster.gains,
        );
    }

    ms.save().expect("save gain solve fixture MS");
    ms_path
}

pub fn append_gain_solve_cluster_for_field(
    ms: &mut MeasurementSet,
    field_id: i32,
    cluster: &SyntheticGainTimeCluster,
) {
    add_custom_gain_solve_row(
        ms,
        0,
        1,
        field_id,
        cluster.scan_number,
        cluster.time_seconds,
        &cluster.gains,
    );
    add_custom_gain_solve_row(
        ms,
        0,
        2,
        field_id,
        cluster.scan_number,
        cluster.time_seconds,
        &cluster.gains,
    );
    add_custom_gain_solve_row(
        ms,
        1,
        2,
        field_id,
        cluster.scan_number,
        cluster.time_seconds,
        &cluster.gains,
    );
}

pub fn create_bandpass_solve_fixture_ms(
    root: &Path,
    clusters: &[SyntheticBandpassTimeCluster],
) -> PathBuf {
    let ms_path = root.join("bandpass_solve_fixture.ms");
    let builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
    let mut ms = MeasurementSet::create(&ms_path, builder).expect("create bandpass fixture MS");

    populate_gain_solve_subtables(&mut ms);
    for cluster in clusters {
        add_custom_bandpass_solve_row(
            &mut ms,
            cluster.field_id,
            0,
            1,
            cluster.scan_number,
            cluster.time_seconds,
            &cluster.prior_gains,
            &cluster.bandpass_gains,
        );
        add_custom_bandpass_solve_row(
            &mut ms,
            cluster.field_id,
            0,
            2,
            cluster.scan_number,
            cluster.time_seconds,
            &cluster.prior_gains,
            &cluster.bandpass_gains,
        );
        add_custom_bandpass_solve_row(
            &mut ms,
            cluster.field_id,
            1,
            2,
            cluster.scan_number,
            cluster.time_seconds,
            &cluster.prior_gains,
            &cluster.bandpass_gains,
        );
    }

    ms.save().expect("save bandpass fixture MS");
    ms_path
}

fn populate_apply_fixture_subtables(ms: &mut MeasurementSet) {
    {
        let field_table = ms.subtable_mut(SubtableId::Field).expect("FIELD subtable");
        for (field_id, (name, ra, dec)) in
            [("TARGET0", 1.0_f64, 0.5_f64), ("TARGET1", 1.2_f64, 0.6_f64)]
                .into_iter()
                .enumerate()
        {
            let direction =
                ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![ra, dec]).unwrap());
            let row = make_subtable_row(
                ms_schema::field::REQUIRED_COLUMNS,
                &[
                    ("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
                    ("CODE", Value::Scalar(ScalarValue::String("T".to_string()))),
                    ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                    ("DELAY_DIR", Value::Array(direction.clone())),
                    ("PHASE_DIR", Value::Array(direction.clone())),
                    ("REFERENCE_DIR", Value::Array(direction)),
                    (
                        "SOURCE_ID",
                        Value::Scalar(ScalarValue::Int32(field_id as i32)),
                    ),
                    ("TIME", Value::Scalar(ScalarValue::Float64(100.0))),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            );
            field_table.add_row(row).expect("add FIELD row");
        }
    }

    {
        let pol_table = ms
            .subtable_mut(SubtableId::Polarization)
            .expect("POLARIZATION subtable");
        let row = make_subtable_row(
            ms_schema::polarization::REQUIRED_COLUMNS,
            &[
                ("NUM_CORR", Value::Scalar(ScalarValue::Int32(2))),
                (
                    "CORR_TYPE",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2], vec![5, 8]).unwrap(),
                    )),
                ),
                (
                    "CORR_PRODUCT",
                    Value::Array(ArrayValue::Int32(
                        ArrayD::from_shape_vec(vec![2, 2], vec![0, 0, 1, 1]).unwrap(),
                    )),
                ),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        pol_table.add_row(row).expect("add POLARIZATION row");
    }

    {
        let spw_table = ms
            .subtable_mut(SubtableId::SpectralWindow)
            .expect("SPECTRAL_WINDOW subtable");
        for (spw_id, base_frequency_hz) in [1.0e9_f64, 1.1e9_f64].into_iter().enumerate() {
            let row = make_subtable_row(
                ms_schema::spectral_window::REQUIRED_COLUMNS,
                &[
                    ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
                    (
                        "CHAN_FREQ",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![2],
                                vec![base_frequency_hz, base_frequency_hz + 1.0e6],
                            )
                            .unwrap(),
                        )),
                    ),
                    (
                        "CHAN_WIDTH",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                        )),
                    ),
                    (
                        "EFFECTIVE_BW",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                        )),
                    ),
                    (
                        "RESOLUTION",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                        )),
                    ),
                    (
                        "REF_FREQUENCY",
                        Value::Scalar(ScalarValue::Float64(base_frequency_hz)),
                    ),
                    (
                        "TOTAL_BANDWIDTH",
                        Value::Scalar(ScalarValue::Float64(2.0e6)),
                    ),
                    ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(format!("SPW{spw_id}"))),
                    ),
                    ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                    ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                    (
                        "FREQ_GROUP_NAME",
                        Value::Scalar(ScalarValue::String("GROUP".to_string())),
                    ),
                    ("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            );
            spw_table.add_row(row).expect("add SPECTRAL_WINDOW row");
        }
    }

    {
        let dd_table = ms
            .subtable_mut(SubtableId::DataDescription)
            .expect("DATA_DESCRIPTION subtable");
        for spw_id in [0_i32, 1_i32] {
            let row = make_subtable_row(
                ms_schema::data_description::REQUIRED_COLUMNS,
                &[
                    (
                        "SPECTRAL_WINDOW_ID",
                        Value::Scalar(ScalarValue::Int32(spw_id)),
                    ),
                    ("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            );
            dd_table.add_row(row).expect("add DATA_DESCRIPTION row");
        }
    }

    {
        let observation = ms
            .subtable_mut(SubtableId::Observation)
            .expect("OBSERVATION subtable");
        let row = make_subtable_row(
            ms_schema::observation::REQUIRED_COLUMNS,
            &[
                (
                    "TELESCOPE_NAME",
                    Value::Scalar(ScalarValue::String("TEST".to_string())),
                ),
                (
                    "TIME_RANGE",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![0.0, 1.0]).unwrap(),
                    )),
                ),
                (
                    "OBSERVER",
                    Value::Scalar(ScalarValue::String("tester".to_string())),
                ),
                (
                    "LOG",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(vec![0], Vec::<String>::new()).unwrap(),
                    )),
                ),
                (
                    "SCHEDULE_TYPE",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                (
                    "SCHEDULE",
                    Value::Array(ArrayValue::String(
                        ArrayD::from_shape_vec(vec![0], Vec::<String>::new()).unwrap(),
                    )),
                ),
                (
                    "PROJECT",
                    Value::Scalar(ScalarValue::String("project".to_string())),
                ),
                ("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(0.0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        observation.add_row(row).expect("add OBSERVATION row");
    }
}

fn populate_gain_solve_subtables(ms: &mut MeasurementSet) {
    {
        let antenna_table = ms
            .subtable_mut(SubtableId::Antenna)
            .expect("ANTENNA subtable");
        for antenna_id in 0..3 {
            let row = make_subtable_row(
                ms_schema::antenna::REQUIRED_COLUMNS,
                &[
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(format!("ANT{antenna_id}"))),
                    ),
                    (
                        "STATION",
                        Value::Scalar(ScalarValue::String(format!("PAD{antenna_id}"))),
                    ),
                    (
                        "TYPE",
                        Value::Scalar(ScalarValue::String("GROUND-BASED".to_string())),
                    ),
                    (
                        "MOUNT",
                        Value::Scalar(ScalarValue::String("ALT-AZ".to_string())),
                    ),
                    (
                        "POSITION",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![3],
                                vec![antenna_id as f64, antenna_id as f64 + 1.0, 0.0],
                            )
                            .unwrap(),
                        )),
                    ),
                    (
                        "OFFSET",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![3], vec![0.0, 0.0, 0.0]).unwrap(),
                        )),
                    ),
                    ("DISH_DIAMETER", Value::Scalar(ScalarValue::Float64(25.0))),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            );
            antenna_table.add_row(row).expect("add ANTENNA row");
        }
    }

    populate_apply_fixture_subtables(ms);
}

fn add_apply_fixture_row(
    ms: &mut MeasurementSet,
    data_desc_id: i32,
    field_id: i32,
    scan_number: i32,
    time: f64,
) {
    let schema = ms.main_table().schema().expect("main schema").clone();
    let has_weight_spectrum = schema.contains_column("WEIGHT_SPECTRUM");
    let row = RecordValue::new(
        schema
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                "ANTENNA2" => RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(1))),
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => RecordField::new(
                    "DATA_DESC_ID",
                    Value::Scalar(ScalarValue::Int32(data_desc_id)),
                ),
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "FIELD_ID" => {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "FLAG" => RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false; 4]).unwrap(),
                    )),
                ),
                "FLAG_ROW" => RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => RecordField::new(
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(scan_number)),
                ),
                "SIGMA" => RecordField::new(
                    "SIGMA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                "TIME_CENTROID" => {
                    RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
                }
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![0.0, 1.0, 2.0]).unwrap(),
                    )),
                ),
                "WEIGHT" => RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            vec![2],
                            if has_weight_spectrum {
                                vec![7.0, 11.0]
                            } else {
                                vec![1.0, 1.0]
                            },
                        )
                        .unwrap(),
                    )),
                ),
                "WEIGHT_SPECTRUM" => RecordField::new(
                    "WEIGHT_SPECTRUM",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![1.0, 2.0, 1.0, 2.0])
                            .unwrap(),
                    )),
                ),
                "DATA" | "CORRECTED_DATA" => RecordField::new(
                    column.name(),
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2, 2]).f(),
                            vec![
                                Complex32::new(1.0, 0.0),
                                Complex32::new(0.0, 1.0),
                                Complex32::new(2.0, 0.0),
                                Complex32::new(0.0, 2.0),
                            ],
                        )
                        .unwrap(),
                    )),
                ),
                name => {
                    RecordField::new(name, default_value_for_column_name(name, schema.columns()))
                }
            })
            .collect(),
    );
    ms.main_table_mut().add_row(row).expect("add MAIN row");
}

fn add_gain_solve_row(
    ms: &mut MeasurementSet,
    antenna1: i32,
    antenna2: i32,
    time: f64,
    kind: SyntheticGainFixtureKind,
) {
    let gains = gains_for_fixture_kind(kind);
    add_custom_gain_solve_row(ms, antenna1, antenna2, 0, 1, time, &gains);
}

fn gains_for_fixture_kind(kind: SyntheticGainFixtureKind) -> [[Complex32; 2]; 3] {
    match kind {
        SyntheticGainFixtureKind::G => [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [
                Complex32::new(0.9553365, 0.29552022),
                Complex32::new(0.921061, -0.38941833),
            ],
            [
                Complex32::new(0.9800666, -0.19866933),
                Complex32::new(0.87758255, 0.47942555),
            ],
        ],
        SyntheticGainFixtureKind::T => [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [
                Complex32::new(0.9553365, 0.29552022),
                Complex32::new(0.9553365, 0.29552022),
            ],
            [
                Complex32::new(0.9800666, -0.19866933),
                Complex32::new(0.9800666, -0.19866933),
            ],
        ],
        SyntheticGainFixtureKind::GAmplitudePhase => [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [
                Complex32::new(1.2419374, 0.38417628),
                Complex32::new(0.6907958, -0.29206374),
            ],
            [
                Complex32::new(0.83305657, -0.16873595),
                Complex32::new(1.3163738, 0.7191383),
            ],
        ],
        SyntheticGainFixtureKind::TAmplitudePhase => [
            [Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
            [
                Complex32::new(1.2419374, 0.38417628),
                Complex32::new(1.2419374, 0.38417628),
            ],
            [
                Complex32::new(0.83305657, -0.16873595),
                Complex32::new(0.83305657, -0.16873595),
            ],
        ],
    }
}

fn add_custom_gain_solve_row(
    ms: &mut MeasurementSet,
    antenna1: i32,
    antenna2: i32,
    field_id: i32,
    scan_number: i32,
    time: f64,
    gains: &[[Complex32; 2]; 3],
) {
    let g1 = gains[usize::try_from(antenna1).expect("antenna1 index")];
    let g2 = gains[usize::try_from(antenna2).expect("antenna2 index")];

    let row = RecordValue::new(
        ms.main_table()
            .schema()
            .expect("main schema")
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => {
                    RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(antenna1)))
                }
                "ANTENNA2" => {
                    RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2)))
                }
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => {
                    RecordField::new("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "FIELD_ID" => {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "FLAG" => RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false; 4]).unwrap(),
                    )),
                ),
                "FLAG_ROW" => RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => RecordField::new(
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(scan_number)),
                ),
                "SIGMA" => RecordField::new(
                    "SIGMA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                "TIME_CENTROID" => {
                    RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
                }
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![0.0, 1.0, 2.0]).unwrap(),
                    )),
                ),
                "WEIGHT" => RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "DATA" => {
                    let rr = g1[0] * g2[0].conj();
                    let ll = g1[1] * g2[1].conj();
                    RecordField::new(
                        "DATA",
                        Value::Array(ArrayValue::Complex32(
                            ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![rr, ll, rr, ll])
                                .unwrap(),
                        )),
                    )
                }
                name => RecordField::new(
                    name,
                    default_value_for_column_name(
                        name,
                        ms.main_table().schema().expect("main schema").columns(),
                    ),
                ),
            })
            .collect(),
    );
    ms.main_table_mut()
        .add_row(row)
        .expect("add gain solve row");
}

#[allow(clippy::too_many_arguments)]
fn add_custom_bandpass_solve_row(
    ms: &mut MeasurementSet,
    field_id: i32,
    antenna1: i32,
    antenna2: i32,
    scan_number: i32,
    time: f64,
    prior_gains: &[[Complex32; 2]; 3],
    bandpass_gains: &[[[Complex32; 2]; 2]; 3],
) {
    let g1 = prior_gains[usize::try_from(antenna1).expect("antenna1 index")];
    let g2 = prior_gains[usize::try_from(antenna2).expect("antenna2 index")];
    let b1 = bandpass_gains[usize::try_from(antenna1).expect("antenna1 index")];
    let b2 = bandpass_gains[usize::try_from(antenna2).expect("antenna2 index")];

    let rr0 = (g1[0] * b1[0][0]) * (g2[0] * b2[0][0]).conj();
    let ll0 = (g1[1] * b1[1][0]) * (g2[1] * b2[1][0]).conj();
    let rr1 = (g1[0] * b1[0][1]) * (g2[0] * b2[0][1]).conj();
    let ll1 = (g1[1] * b1[1][1]) * (g2[1] * b2[1][1]).conj();

    let row = RecordValue::new(
        ms.main_table()
            .schema()
            .expect("main schema")
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => {
                    RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(antenna1)))
                }
                "ANTENNA2" => {
                    RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2)))
                }
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => {
                    RecordField::new("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "FIELD_ID" => {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "FLAG" => RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false; 4]).unwrap(),
                    )),
                ),
                "FLAG_ROW" => RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(10.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => RecordField::new(
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(scan_number)),
                ),
                "SIGMA" => RecordField::new(
                    "SIGMA",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                "TIME_CENTROID" => {
                    RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
                }
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![0.0, 1.0, 2.0]).unwrap(),
                    )),
                ),
                "WEIGHT" => RecordField::new(
                    "WEIGHT",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![2], vec![1.0, 1.0]).unwrap(),
                    )),
                ),
                "DATA" => RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![rr0, ll0, rr1, ll1])
                            .unwrap(),
                    )),
                ),
                name => RecordField::new(
                    name,
                    default_value_for_column_name(
                        name,
                        ms.main_table().schema().expect("main schema").columns(),
                    ),
                ),
            })
            .collect(),
    );
    ms.main_table_mut()
        .add_row(row)
        .expect("add bandpass solve row");
}

fn set_field_directions_in_table(path: &Path, directions: &[(usize, f64, f64)]) {
    let mut table = Table::open(TableOptions::new(path)).expect("open FIELD table");
    for &(row, ra, dec) in directions {
        let direction =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![ra, dec]).unwrap());
        for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
            table
                .set_cell(row, column, Value::Array(direction.clone()))
                .expect("set FIELD direction");
        }
    }
    table
        .save(TableOptions::new(path))
        .expect("save FIELD table");
}

fn set_antenna_mounts_in_table(path: &Path, mounts: &[(usize, &str)]) {
    let mut table = Table::open(TableOptions::new(path)).expect("open ANTENNA table");
    for &(row, mount) in mounts {
        table
            .set_cell(
                row,
                "MOUNT",
                Value::Scalar(ScalarValue::String(mount.to_string())),
            )
            .expect("set ANTENNA mount");
    }
    table
        .save(TableOptions::new(path))
        .expect("save ANTENNA table");
}

fn write_caltable_field_subtable(path: PathBuf, field_names: &[&str]) {
    let schema = build_table_schema(ms_schema::field::REQUIRED_COLUMNS).expect("FIELD schema");
    let mut table = Table::with_schema(schema);
    for (field_id, name) in field_names.iter().enumerate() {
        let direction = ArrayValue::Float64(
            ArrayD::from_shape_vec(vec![2, 1], vec![1.0 + field_id as f64, 0.5]).unwrap(),
        );
        let row = make_subtable_row(
            ms_schema::field::REQUIRED_COLUMNS,
            &[
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String((*name).to_string())),
                ),
                ("CODE", Value::Scalar(ScalarValue::String("T".to_string()))),
                ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                ("DELAY_DIR", Value::Array(direction.clone())),
                ("PHASE_DIR", Value::Array(direction.clone())),
                ("REFERENCE_DIR", Value::Array(direction)),
                (
                    "SOURCE_ID",
                    Value::Scalar(ScalarValue::Int32(field_id as i32)),
                ),
                ("TIME", Value::Scalar(ScalarValue::Float64(100.0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        table.add_row(row).expect("add caltable FIELD row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save FIELD subtable");
}

fn write_caltable_spectral_window_subtable(path: PathBuf, spectral_window_ids: &[i32]) {
    let schema =
        build_table_schema(ms_schema::spectral_window::REQUIRED_COLUMNS).expect("SPW schema");
    let mut table = Table::with_schema(schema);
    let max_spw = spectral_window_ids.iter().copied().max().unwrap_or(-1);
    for spw_id in 0..=max_spw {
        let base_frequency_hz = 1.0e9_f64 + spw_id as f64 * 1.0e8;
        let row = make_subtable_row(
            ms_schema::spectral_window::REQUIRED_COLUMNS,
            &[
                ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
                (
                    "CHAN_FREQ",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            vec![2],
                            vec![base_frequency_hz, base_frequency_hz + 1.0e6],
                        )
                        .unwrap(),
                    )),
                ),
                (
                    "CHAN_WIDTH",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                (
                    "EFFECTIVE_BW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                (
                    "RESOLUTION",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap(),
                    )),
                ),
                (
                    "REF_FREQUENCY",
                    Value::Scalar(ScalarValue::Float64(base_frequency_hz)),
                ),
                (
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(2.0e6)),
                ),
                ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String(format!("CALSPW{spw_id}"))),
                ),
                ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String("GROUP".to_string())),
                ),
                ("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        table.add_row(row).expect("add caltable SPW row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save SPECTRAL_WINDOW subtable");
}

fn write_delay_caltable_spectral_window_subtable(path: PathBuf, spectral_window_ids: &[i32]) {
    let schema =
        build_table_schema(ms_schema::spectral_window::REQUIRED_COLUMNS).expect("SPW schema");
    let mut table = Table::with_schema(schema);
    let max_spw = spectral_window_ids.iter().copied().max().unwrap_or(-1);
    for spw_id in 0..=max_spw {
        let base_frequency_hz = 1.0e9_f64 + spw_id as f64 * 1.0e8;
        let row = make_subtable_row(
            ms_schema::spectral_window::REQUIRED_COLUMNS,
            &[
                ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(1))),
                (
                    "CHAN_FREQ",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![1], vec![base_frequency_hz]).unwrap(),
                    )),
                ),
                (
                    "CHAN_WIDTH",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![1], vec![1.0e6]).unwrap(),
                    )),
                ),
                (
                    "EFFECTIVE_BW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![1], vec![1.0e6]).unwrap(),
                    )),
                ),
                (
                    "RESOLUTION",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![1], vec![1.0e6]).unwrap(),
                    )),
                ),
                (
                    "REF_FREQUENCY",
                    Value::Scalar(ScalarValue::Float64(base_frequency_hz)),
                ),
                (
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(1.0e6)),
                ),
                ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String(format!("CALSPW{spw_id}"))),
                ),
                ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String("GROUP".to_string())),
                ),
                ("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        table.add_row(row).expect("add delay caltable SPW row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save delay SPECTRAL_WINDOW subtable");
}

fn write_bpoly_cal_desc_subtable(
    path: PathBuf,
    cal_desc_ids: &std::collections::BTreeMap<i32, i32>,
    rows: &[SyntheticBPolySolutionRow],
) {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("NUM_RECEPTORS", casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable(
            "SPECTRAL_WINDOW_ID",
            casa_types::PrimitiveType::Int32,
            Some(1),
        ),
    ])
    .expect("BPOLY CAL_DESC schema");
    let mut table = Table::with_schema(schema);
    for (spw_id, cal_desc_id) in cal_desc_ids {
        let receptor_count = rows
            .iter()
            .find(|row| row.spectral_window_id == *spw_id)
            .map(|row| row.amp_coefficients.len() as i32)
            .unwrap_or(1);
        while table.row_count() < usize::try_from(*cal_desc_id).expect("small CAL_DESC id") {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(1))),
                    RecordField::new(
                        "SPECTRAL_WINDOW_ID",
                        Value::Array(ArrayValue::from_i32_vec(vec![0])),
                    ),
                ]))
                .expect("pad CAL_DESC rows");
        }
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "NUM_RECEPTORS",
                    Value::Scalar(ScalarValue::Int32(receptor_count)),
                ),
                RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Array(ArrayValue::from_i32_vec(vec![*spw_id])),
                ),
            ]))
            .expect("add CAL_DESC row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save BPOLY CAL_DESC subtable");
}

fn write_empty_table(path: PathBuf) {
    let empty_schema = TableSchema::new(vec![]).expect("empty schema");
    Table::with_schema(empty_schema)
        .save(TableOptions::new(path))
        .expect("save empty subtable");
}

fn write_minimal_observation_table(path: PathBuf) {
    let schema =
        build_table_schema(ms_schema::observation::REQUIRED_COLUMNS).expect("OBSERVATION schema");
    let mut table = Table::with_schema(schema);
    let row = make_subtable_row(
        ms_schema::observation::REQUIRED_COLUMNS,
        &[
            (
                "TELESCOPE_NAME",
                Value::Scalar(ScalarValue::String("TEST".to_string())),
            ),
            (
                "TIME_RANGE",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![2], vec![0.0, 1.0]).unwrap(),
                )),
            ),
            (
                "OBSERVER",
                Value::Scalar(ScalarValue::String("tester".to_string())),
            ),
            (
                "LOG",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![0], Vec::<String>::new()).unwrap(),
                )),
            ),
            (
                "SCHEDULE_TYPE",
                Value::Scalar(ScalarValue::String(String::new())),
            ),
            (
                "SCHEDULE",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![0], Vec::<String>::new()).unwrap(),
                )),
            ),
            (
                "PROJECT",
                Value::Scalar(ScalarValue::String("project".to_string())),
            ),
            ("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(0.0))),
            ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        ],
    );
    table.add_row(row).expect("add OBSERVATION row");
    table
        .save(TableOptions::new(path))
        .expect("save OBSERVATION subtable");
}

fn write_minimal_antenna_table(path: PathBuf, antenna_count: usize) {
    let schema = build_table_schema(ms_schema::antenna::REQUIRED_COLUMNS).expect("ANTENNA schema");
    let mut table = Table::with_schema(schema);
    for antenna_id in 0..antenna_count {
        let row = make_subtable_row(
            ms_schema::antenna::REQUIRED_COLUMNS,
            &[
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String(format!("ANT{antenna_id}"))),
                ),
                (
                    "STATION",
                    Value::Scalar(ScalarValue::String(format!("PAD{antenna_id}"))),
                ),
                (
                    "TYPE",
                    Value::Scalar(ScalarValue::String("GROUND-BASED".to_string())),
                ),
                (
                    "MOUNT",
                    Value::Scalar(ScalarValue::String("ALT-AZ".to_string())),
                ),
                (
                    "POSITION",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(
                            vec![3],
                            vec![antenna_id as f64, antenna_id as f64 + 1.0, 0.0],
                        )
                        .unwrap(),
                    )),
                ),
                (
                    "OFFSET",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], vec![0.0, 0.0, 0.0]).unwrap(),
                    )),
                ),
                ("DISH_DIAMETER", Value::Scalar(ScalarValue::Float64(25.0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        table.add_row(row).expect("add ANTENNA row");
    }
    table
        .save(TableOptions::new(path))
        .expect("save ANTENNA subtable");
}

fn set_fixed_unit_keyword(table: &mut Table, column: &str, units: &[&str]) {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    keywords.upsert(
        "QuantumUnits",
        Value::Array(ArrayValue::from_string_vec(
            units.iter().map(|unit| (*unit).to_string()).collect(),
        )),
    );
    table.set_column_keywords(column, keywords);
}

fn set_measinfo_keyword(
    table: &mut Table,
    column: &str,
    measure_type: &str,
    measure_ref: Option<&str>,
) {
    let mut fields = vec![RecordField::new(
        "type",
        Value::Scalar(ScalarValue::String(measure_type.to_string())),
    )];
    if let Some(measure_ref) = measure_ref {
        fields.push(RecordField::new(
            "Ref",
            Value::Scalar(ScalarValue::String(measure_ref.to_string())),
        ));
    }
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    keywords.upsert("MEASINFO", Value::Record(RecordValue::new(fields)));
    table.set_column_keywords(column, keywords);
}

fn make_subtable_row(columns: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
    let fields = columns
        .iter()
        .map(|column| {
            let value = overrides
                .iter()
                .find_map(|(name, value)| (*name == column.name).then(|| value.clone()))
                .unwrap_or_else(|| default_value_for_column(column));
            RecordField::new(column.name, value)
        })
        .collect();
    RecordValue::new(fields)
}

fn default_value_for_column(column: &ColumnDef) -> Value {
    match column.column_kind {
        ColumnKind::Scalar => match column.data_type {
            casa_types::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            casa_types::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            casa_types::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            casa_types::PrimitiveType::Float32 => Value::Scalar(ScalarValue::Float32(0.0)),
            casa_types::PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
            _ => Value::Scalar(ScalarValue::Int32(0)),
        },
        ColumnKind::FixedArray { shape } => {
            let size: usize = shape.iter().product();
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; size]).unwrap(),
            ))
        }
        ColumnKind::VariableArray { ndim } => {
            let shape = vec![0; ndim];
            match column.data_type {
                casa_types::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<bool>::new()).unwrap(),
                )),
                casa_types::PrimitiveType::Int32 => Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<i32>::new()).unwrap(),
                )),
                casa_types::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<f32>::new()).unwrap(),
                )),
                casa_types::PrimitiveType::Float64 => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<f64>::new()).unwrap(),
                )),
                casa_types::PrimitiveType::String => Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<String>::new()).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<f64>::new()).unwrap(),
                )),
            }
        }
    }
}

fn default_value_for_column_name(name: &str, columns: &[casa_tables::ColumnSchema]) -> Value {
    let column = columns
        .iter()
        .find(|column| column.name() == name)
        .expect("column present in schema");
    match column.column_type() {
        casa_tables::ColumnType::Scalar => match column.data_type() {
            Some(casa_types::PrimitiveType::Bool) => Value::Scalar(ScalarValue::Bool(false)),
            Some(casa_types::PrimitiveType::Int32) => Value::Scalar(ScalarValue::Int32(0)),
            Some(casa_types::PrimitiveType::Float32) => Value::Scalar(ScalarValue::Float32(0.0)),
            Some(casa_types::PrimitiveType::Float64) => Value::Scalar(ScalarValue::Float64(0.0)),
            Some(casa_types::PrimitiveType::String) => {
                Value::Scalar(ScalarValue::String(String::new()))
            }
            _ => Value::Scalar(ScalarValue::Int32(0)),
        },
        casa_tables::ColumnType::Array(shape_contract) => {
            let shape = match shape_contract {
                casa_tables::ArrayShapeContract::Fixed { shape } => shape.clone(),
                casa_tables::ArrayShapeContract::Variable { ndim } => {
                    vec![0; ndim.unwrap_or(1)]
                }
            };
            match column.data_type() {
                Some(casa_types::PrimitiveType::Bool) => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![false; shape.iter().product()])
                        .unwrap(),
                )),
                Some(casa_types::PrimitiveType::Float32) => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        IxDyn(&shape).f(),
                        vec![0.0_f32; shape.iter().product()],
                    )
                    .unwrap(),
                )),
                Some(casa_types::PrimitiveType::Complex32) => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        IxDyn(&shape).f(),
                        vec![Complex32 { re: 0.0, im: 0.0 }; shape.iter().product()],
                    )
                    .unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(
                        IxDyn(&shape).f(),
                        vec![0.0_f64; shape.iter().product()],
                    )
                    .unwrap(),
                )),
            }
        }
        casa_tables::ColumnType::Record => Value::Record(RecordValue::new(vec![])),
    }
}

#[cfg(feature = "slow-tests")]
pub fn discover_casa_python() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    for key in ["CASA_RS_CASA_PYTHON", "CASA_PYTHON"] {
        if let Some(value) = std::env::var_os(key) {
            candidates.push(PathBuf::from(value));
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join("SoftwareProjects")
                .join("casa-build")
                .join("venv")
                .join("bin")
                .join("python"),
        );
    }
    candidates.push(PathBuf::from("python3"));
    candidates.push(PathBuf::from("python"));

    candidates.into_iter().find(|program| {
        Command::new(program)
            .arg("-c")
            .arg("import casatasks")
            .output()
            .is_ok_and(|output| output.status.success())
    })
}

#[cfg(feature = "slow-tests")]
pub fn ngc5921_ms_path() -> Option<PathBuf> {
    casatestdata_path("measurementset/vla/ngc5921.ms").filter(|path| path.exists())
}

#[cfg(feature = "slow-tests")]
pub fn casa_skip_reason() -> String {
    match (discover_casa_python(), ngc5921_ms_path()) {
        (None, _) => {
            "CASA calibration parity skipped: no CASA-capable python found via CASA_RS_CASA_PYTHON, CASA_PYTHON, python3, or python".to_string()
        }
        (_, None) => {
            "CASA calibration parity skipped: missing ngc5921.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata".to_string()
        }
        _ => "CASA calibration parity skipped".to_string(),
    }
}

#[cfg(feature = "slow-tests")]
pub fn generate_casa_exemplars(output_dir: &Path) -> Result<(PathBuf, PathBuf, PathBuf), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let ms_path = ngc5921_ms_path().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
from casatasks import bandpass, gaincal

vis = os.environ["CASA_RS_CAL_MS"]
out = os.environ["CASA_RS_CAL_OUT"]
phase = os.path.join(out, "phase.gcal")
tsolve = os.path.join(out, "t.gcal")
bp = os.path.join(out, "b.bcal")

gaincal(
    vis=vis,
    caltable=phase,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    calmode="p",
    minsnr=0.0,
)
gaincal(
    vis=vis,
    caltable=tsolve,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    calmode="p",
    gaintype="T",
    minsnr=0.0,
)
bandpass(
    vis=vis,
    caltable=bp,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    bandtype="B",
    gaintable=[phase],
    minsnr=0.0,
)
"#;
    let output = Command::new(&python)
        .env("CASA_RS_CAL_MS", &ms_path)
        .env("CASA_RS_CAL_OUT", output_dir)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA exemplar generation failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok((
        output_dir.join("phase.gcal"),
        output_dir.join("t.gcal"),
        output_dir.join("b.bcal"),
    ))
}

#[cfg(feature = "slow-tests")]
pub fn copy_measurement_set(source: &Path, destination: &Path) -> Result<(), String> {
    if source.is_dir() {
        fs::create_dir_all(destination).map_err(|error| {
            format!(
                "create destination directory {}: {error}",
                destination.display()
            )
        })?;
        for entry in fs::read_dir(source).map_err(|error| {
            format!(
                "read MeasurementSet directory {}: {error}",
                source.display()
            )
        })? {
            let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
            let child_source = entry.path();
            let child_destination = destination.join(entry.file_name());
            copy_measurement_set(&child_source, &child_destination)?;
        }
        return Ok(());
    }

    fs::copy(source, destination).map_err(|error| {
        format!(
            "copy MeasurementSet file {} -> {}: {error}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

#[cfg(feature = "slow-tests")]
pub fn generate_casa_phase_gain(
    output_dir: &Path,
    field: &str,
    spw: &str,
    refant: &str,
) -> Result<PathBuf, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let ms_path = ngc5921_ms_path().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
from casatasks import gaincal

gaincal(
    vis=os.environ["CASA_RS_CAL_MS"],
    caltable=os.environ["CASA_RS_CALTABLE"],
    field=os.environ["CASA_RS_CAL_FIELD"],
    spw=os.environ["CASA_RS_CAL_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_CAL_REFANT"],
    calmode="p",
    minsnr=0.0,
)
"#;
    let caltable = output_dir.join("phase.gcal");
    let output = Command::new(&python)
        .env("CASA_RS_CAL_MS", &ms_path)
        .env("CASA_RS_CALTABLE", &caltable)
        .env("CASA_RS_CAL_FIELD", field)
        .env("CASA_RS_CAL_SPW", spw)
        .env("CASA_RS_CAL_REFANT", refant)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA phase gain generation failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(caltable)
}

#[cfg(feature = "slow-tests")]
#[allow(clippy::too_many_arguments)]
pub fn run_casa_applycal(
    ms_path: &Path,
    caltable: &Path,
    field: &str,
    spw: &str,
    scan: Option<&str>,
    applymode: &str,
    calwt: bool,
    parang: bool,
) -> Result<(), String> {
    run_casa_applycal_chain(
        ms_path,
        &[caltable],
        field,
        spw,
        scan,
        applymode,
        calwt,
        parang,
    )
}

#[cfg(feature = "slow-tests")]
#[allow(clippy::too_many_arguments)]
pub fn run_casa_applycal_with_gainfield(
    ms_path: &Path,
    caltable: &Path,
    field: &str,
    spw: &str,
    gainfield: &str,
    applymode: &str,
    calwt: bool,
    parang: bool,
) -> Result<(), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
from casatasks import applycal

applycal(
    vis=os.environ["CASA_RS_APPLY_MS"],
    field=os.environ["CASA_RS_APPLY_FIELD"],
    spw=os.environ["CASA_RS_APPLY_SPW"],
    gaintable=[os.environ["CASA_RS_APPLY_GAINTABLE"]],
    gainfield=[os.environ["CASA_RS_APPLY_GAINFIELD"]],
    interp=["nearest"],
    calwt=os.environ["CASA_RS_APPLY_CALWT"] == "true",
    applymode=os.environ["CASA_RS_APPLY_MODE"],
    parang=os.environ["CASA_RS_APPLY_PARANG"] == "true",
    flagbackup=False,
)
"#;
    let output = Command::new(&python)
        .env("CASA_RS_APPLY_MS", ms_path)
        .env("CASA_RS_APPLY_FIELD", field)
        .env("CASA_RS_APPLY_SPW", spw)
        .env("CASA_RS_APPLY_GAINTABLE", caltable)
        .env("CASA_RS_APPLY_GAINFIELD", gainfield)
        .env("CASA_RS_APPLY_MODE", applymode)
        .env("CASA_RS_APPLY_CALWT", if calwt { "true" } else { "false" })
        .env(
            "CASA_RS_APPLY_PARANG",
            if parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA applycal with gainfield failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

#[cfg(feature = "slow-tests")]
pub fn run_casa_applycal_with_callib(
    ms_path: &Path,
    callib: &Path,
    field: &str,
    spw: &str,
    applymode: &str,
    parang: bool,
) -> Result<(), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
from casatasks import applycal

applycal(
    vis=os.environ["CASA_RS_APPLY_MS"],
    field=os.environ["CASA_RS_APPLY_FIELD"],
    spw=os.environ["CASA_RS_APPLY_SPW"],
    docallib=True,
    callib=os.environ["CASA_RS_APPLY_CALLIB"],
    applymode=os.environ["CASA_RS_APPLY_MODE"],
    parang=os.environ["CASA_RS_APPLY_PARANG"] == "true",
    flagbackup=False,
)
"#;
    let output = Command::new(&python)
        .env("CASA_RS_APPLY_MS", ms_path)
        .env("CASA_RS_APPLY_FIELD", field)
        .env("CASA_RS_APPLY_SPW", spw)
        .env("CASA_RS_APPLY_CALLIB", callib)
        .env("CASA_RS_APPLY_MODE", applymode)
        .env(
            "CASA_RS_APPLY_PARANG",
            if parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA applycal with callib failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

#[cfg(feature = "slow-tests")]
#[allow(clippy::too_many_arguments)]
pub fn run_casa_applycal_chain(
    ms_path: &Path,
    caltables: &[&Path],
    field: &str,
    spw: &str,
    scan: Option<&str>,
    applymode: &str,
    calwt: bool,
    parang: bool,
) -> Result<(), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import applycal

applycal(
    vis=os.environ["CASA_RS_APPLY_MS"],
    field=os.environ["CASA_RS_APPLY_FIELD"],
    spw=os.environ["CASA_RS_APPLY_SPW"],
    scan=os.environ.get("CASA_RS_APPLY_SCAN", ""),
    gaintable=json.loads(os.environ["CASA_RS_APPLY_GAINTABLES"]),
    interp=["nearest"],
    calwt=os.environ["CASA_RS_APPLY_CALWT"] == "true",
    applymode=os.environ["CASA_RS_APPLY_MODE"],
    parang=os.environ["CASA_RS_APPLY_PARANG"] == "true",
    flagbackup=False,
)
"#;
    let mut command = Command::new(&python);
    let gaintables_json = serde_json::to_string(
        &caltables
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    )
    .expect("serialize applycal gaintables");
    command
        .env("CASA_RS_APPLY_MS", ms_path)
        .env("CASA_RS_APPLY_GAINTABLES", gaintables_json)
        .env("CASA_RS_APPLY_FIELD", field)
        .env("CASA_RS_APPLY_SPW", spw)
        .env("CASA_RS_APPLY_MODE", applymode)
        .env("CASA_RS_APPLY_CALWT", if calwt { "true" } else { "false" })
        .env(
            "CASA_RS_APPLY_PARANG",
            if parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script);
    if let Some(scan) = scan
        && !scan.is_empty()
    {
        command.env("CASA_RS_APPLY_SCAN", scan);
    }
    let output = command
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA applycal failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

#[cfg(feature = "slow-tests")]
pub fn run_casa_gaincal(
    output_dir: &Path,
    ms_path: &Path,
    options: CasaGaincalOptions<'_>,
) -> Result<PathBuf, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import gaincal

gaincal(
    vis=os.environ["CASA_RS_GAIN_MS"],
    caltable=os.environ["CASA_RS_GAIN_CALTABLE"],
    field=os.environ["CASA_RS_GAIN_FIELD"],
    spw=os.environ["CASA_RS_GAIN_SPW"],
    solint=os.environ["CASA_RS_GAIN_SOLINT"],
    refant=os.environ["CASA_RS_GAIN_REFANT"],
    gaintype=os.environ["CASA_RS_GAIN_TYPE"],
    calmode=os.environ["CASA_RS_GAIN_MODE"],
    combine=os.environ["CASA_RS_GAIN_COMBINE"],
    gaintable=json.loads(os.environ["CASA_RS_GAIN_GAINTABLES"]),
    smodel=[1,0,0,0],
    minsnr=0.0,
    parang=os.environ["CASA_RS_GAIN_PARANG"] == "true",
)
"#;
    let suffix = match options.gaintype {
        "T" => "t.gcal",
        "K" => "k.gcal",
        _ => "g.gcal",
    };
    let caltable = output_dir.join(suffix);
    let gaintables_json = serde_json::to_string(
        &options
            .prior_gaintables
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    )
    .expect("serialize gaincal prior gaintables");
    let output = Command::new(&python)
        .env("CASA_RS_GAIN_MS", ms_path)
        .env("CASA_RS_GAIN_CALTABLE", &caltable)
        .env("CASA_RS_GAIN_FIELD", options.field)
        .env("CASA_RS_GAIN_SPW", options.spw)
        .env("CASA_RS_GAIN_SOLINT", options.solint)
        .env("CASA_RS_GAIN_REFANT", options.refant)
        .env("CASA_RS_GAIN_TYPE", options.gaintype)
        .env("CASA_RS_GAIN_MODE", options.calmode)
        .env("CASA_RS_GAIN_COMBINE", options.combine)
        .env("CASA_RS_GAIN_GAINTABLES", gaintables_json)
        .env(
            "CASA_RS_GAIN_PARANG",
            if options.parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA gaincal failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(caltable)
}

#[cfg(feature = "slow-tests")]
pub fn generate_casa_fluxscale_gain_fixture(
    output_dir: &Path,
    reference_field: &str,
    transfer_field: &str,
    spw: &str,
    refant: &str,
) -> Result<(PathBuf, PathBuf), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let source_ms = ngc5921_ms_path().ok_or_else(casa_skip_reason)?;
    let ms_copy = output_dir.join("fluxscale.ms");
    copy_measurement_set(&source_ms, &ms_copy)?;
    let gain_table = output_dir.join("fluxscale.gcal");
    let script = r#"
import os
from casatasks import gaincal, setjy

vis = os.environ["CASA_RS_FLUXSCALE_MS"]
reference_field = os.environ["CASA_RS_FLUXSCALE_REFERENCE"]
transfer_field = os.environ["CASA_RS_FLUXSCALE_TRANSFER"]

setjy(
    vis=vis,
    field=reference_field,
    standard="Perley-Taylor 99",
    scalebychan=False,
    usescratch=True,
)
gaincal(
    vis=vis,
    caltable=os.environ["CASA_RS_FLUXSCALE_GCAL"],
    field=f"{reference_field},{transfer_field}",
    spw=os.environ["CASA_RS_FLUXSCALE_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_FLUXSCALE_REFANT"],
    gaintype="G",
    calmode="ap",
    smodel=[1,0,0,0],
    minsnr=0.0,
)
"#;
    let output = Command::new(&python)
        .env("CASA_RS_FLUXSCALE_MS", &ms_copy)
        .env("CASA_RS_FLUXSCALE_GCAL", &gain_table)
        .env("CASA_RS_FLUXSCALE_REFERENCE", reference_field)
        .env("CASA_RS_FLUXSCALE_TRANSFER", transfer_field)
        .env("CASA_RS_FLUXSCALE_SPW", spw)
        .env("CASA_RS_FLUXSCALE_REFANT", refant)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA fluxscale gain fixture generation failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok((ms_copy, gain_table))
}

#[cfg(feature = "slow-tests")]
pub fn run_casa_fluxscale(
    ms_path: &Path,
    caltable: &Path,
    fluxtable: &Path,
    reference: &str,
    transfer: &str,
) -> Result<JsonValue, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import fluxscale

def normalize(value):
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if hasattr(value, "tolist"):
        return normalize(value.tolist())
    if hasattr(value, "item"):
        return normalize(value.item())
    return value

result = fluxscale(
    vis=os.environ["CASA_RS_FLUXSCALE_MS"],
    caltable=os.environ["CASA_RS_FLUXSCALE_INPUT"],
    fluxtable=os.environ["CASA_RS_FLUXSCALE_OUTPUT"],
    reference=os.environ["CASA_RS_FLUXSCALE_REFERENCE"],
    transfer=os.environ["CASA_RS_FLUXSCALE_TRANSFER"],
)
print(json.dumps(normalize(result), sort_keys=True))
"#;
    let output = Command::new(&python)
        .env("CASA_RS_FLUXSCALE_MS", ms_path)
        .env("CASA_RS_FLUXSCALE_INPUT", caltable)
        .env("CASA_RS_FLUXSCALE_OUTPUT", fluxtable)
        .env("CASA_RS_FLUXSCALE_REFERENCE", reference)
        .env("CASA_RS_FLUXSCALE_TRANSFER", transfer)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA fluxscale failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("parse CASA fluxscale json: {error}"))
}

#[cfg(feature = "slow-tests")]
#[allow(clippy::too_many_arguments)]
pub fn run_casa_bandpass(
    output_dir: &Path,
    ms_path: &Path,
    field: &str,
    spw: &str,
    refant: &str,
    prior_gaintables: &[&Path],
    combine: &str,
    solnorm: bool,
    parang: bool,
) -> Result<PathBuf, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"\
import os
import json
from casatasks import bandpass

bandpass(
    vis=os.environ["CASA_RS_BANDPASS_MS"],
    caltable=os.environ["CASA_RS_BANDPASS_CALTABLE"],
    field=os.environ["CASA_RS_BANDPASS_FIELD"],
    spw=os.environ["CASA_RS_BANDPASS_SPW"],
    solint="inf",
    combine=os.environ["CASA_RS_BANDPASS_COMBINE"],
    refant=os.environ["CASA_RS_BANDPASS_REFANT"],
    bandtype="B",
    gaintable=json.loads(os.environ["CASA_RS_BANDPASS_GAINTABLES"]),
    solnorm=os.environ["CASA_RS_BANDPASS_SOLNORM"] == "true",
    smodel=[1,0,0,0],
    minsnr=0.0,
    parang=os.environ["CASA_RS_BANDPASS_PARANG"] == "true",
)
"#;
    let caltable = output_dir.join("b.bcal");
    let gaintables_json = serde_json::to_string(
        &prior_gaintables
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    )
    .expect("serialize bandpass prior gaintables");
    let output = Command::new(&python)
        .env("CASA_RS_BANDPASS_MS", ms_path)
        .env("CASA_RS_BANDPASS_CALTABLE", &caltable)
        .env("CASA_RS_BANDPASS_FIELD", field)
        .env("CASA_RS_BANDPASS_SPW", spw)
        .env("CASA_RS_BANDPASS_COMBINE", combine)
        .env("CASA_RS_BANDPASS_REFANT", refant)
        .env("CASA_RS_BANDPASS_GAINTABLES", gaintables_json)
        .env(
            "CASA_RS_BANDPASS_SOLNORM",
            if solnorm { "true" } else { "false" },
        )
        .env(
            "CASA_RS_BANDPASS_PARANG",
            if parang { "true" } else { "false" },
        )
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA bandpass failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(caltable)
}

#[cfg(feature = "slow-tests")]
pub fn run_casa_bandpass_bpoly(
    output_dir: &Path,
    ms_path: &Path,
    field: &str,
    spw: &str,
    refant: &str,
    prior_gaintables: &[&Path],
) -> Result<PathBuf, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import bandpass

bandpass(
    vis=os.environ["CASA_RS_BPOLY_MS"],
    caltable=os.environ["CASA_RS_BPOLY_CALTABLE"],
    field=os.environ["CASA_RS_BPOLY_FIELD"],
    spw=os.environ["CASA_RS_BPOLY_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_BPOLY_REFANT"],
    bandtype="BPOLY",
    degamp=3,
    degphase=3,
    visnorm=False,
    solnorm=False,
    maskcenter=0,
    maskedge=0,
    gaintable=json.loads(os.environ["CASA_RS_BPOLY_GAINTABLES"]),
    smodel=[1,0,0,0],
    minsnr=0.0,
)
"#;
    let caltable = output_dir.join("bp.bpoly");
    let gaintables_json = serde_json::to_string(
        &prior_gaintables
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    )
    .expect("serialize BPOLY prior gaintables");
    let output = Command::new(&python)
        .env("CASA_RS_BPOLY_MS", ms_path)
        .env("CASA_RS_BPOLY_CALTABLE", &caltable)
        .env("CASA_RS_BPOLY_FIELD", field)
        .env("CASA_RS_BPOLY_SPW", spw)
        .env("CASA_RS_BPOLY_REFANT", refant)
        .env("CASA_RS_BPOLY_GAINTABLES", gaintables_json)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA BPOLY bandpass failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(caltable)
}

#[cfg(feature = "slow-tests")]
pub fn run_casa_calstat(
    caltable: &Path,
    axis: &str,
    datacolumn: &str,
) -> Result<JsonValue, String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
import json
from casatasks import calstat

def normalize(value):
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if hasattr(value, "item"):
        return normalize(value.item())
    return value

result = calstat(
    caltable=os.environ["CASA_RS_CALSTAT_TABLE"],
    axis=os.environ["CASA_RS_CALSTAT_AXIS"],
    datacolumn=os.environ["CASA_RS_CALSTAT_DATACOLUMN"],
)
print(json.dumps(normalize(result), sort_keys=True))
"#;
    let output = Command::new(&python)
        .env("CASA_RS_CALSTAT_TABLE", caltable)
        .env("CASA_RS_CALSTAT_AXIS", axis)
        .env("CASA_RS_CALSTAT_DATACOLUMN", datacolumn)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA calstat failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("parse CASA calstat json: {error}"))
}
