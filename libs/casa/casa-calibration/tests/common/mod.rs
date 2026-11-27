// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_ms::MeasurementSetBuilder;
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::schema::{self as ms_schema, SubtableId};
use casa_ms::{ColumnDef, ColumnKind, build_table_schema};
use casa_tables::{ColumnSchema, DataManagerKind, Table, TableInfo, TableOptions, TableSchema};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

pub fn create_minimal_complex_caltable(root: &Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::array_variable("CPARAM", casa_core::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("PARAMERR", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_core::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("SNR", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("WEIGHT", casa_core::PrimitiveType::Float32, Some(1)),
    ])
    .expect("valid schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
        readme: Vec::new(),
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
    assert_corrected_rows_are_unit_model_except(ms_path, &[]);
}

pub fn assert_corrected_rows_are_unit_model_except(ms_path: &Path, skipped_rows: &[usize]) {
    let ms = MeasurementSet::open(ms_path).expect("reopen measurement set");
    let corrected = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data column");
    for row in (0..ms.row_count()).filter(|row| !skipped_rows.contains(row)) {
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

/// Make MAIN row `row` an outlier flagged only by `FLAG_ROW`: its DATA is
/// scaled by `factor`, `FLAG_ROW` is set and every `FLAG` element is clear.
pub fn make_row_flag_only_outlier(ms_path: &Path, row: usize, factor: Complex32) {
    let mut ms = MeasurementSet::open(ms_path).expect("open measurement set");
    let ArrayValue::Complex32(data) = ms
        .main_table()
        .cell_accessor(row, "DATA")
        .and_then(|cell| cell.array())
        .expect("DATA cell")
    else {
        panic!("expected complex DATA");
    };
    let outlier = data.mapv(|value| value * factor);
    let flags = ArrayD::from_elem(outlier.raw_dim(), false);
    let table = ms.main_table_mut();
    table
        .column_accessor_mut("DATA")
        .and_then(|mut column| column.set(row, Value::Array(ArrayValue::Complex32(outlier))))
        .expect("write outlier DATA");
    table
        .column_accessor_mut("FLAG")
        .and_then(|mut column| column.set(row, Value::Array(ArrayValue::Bool(flags))))
        .expect("clear FLAG");
    table
        .column_accessor_mut("FLAG_ROW")
        .and_then(|mut column| column.set(row, Value::Scalar(ScalarValue::Bool(true))))
        .expect("set FLAG_ROW");
    table
        .prepare_write()
        .save_selected_rows(&["DATA", "FLAG", "FLAG_ROW"], &[row])
        .expect("save outlier row");
}

pub fn assert_corrected_rows_match_model_column(ms_path: &Path) {
    let ms = MeasurementSet::open(ms_path).expect("reopen measurement set");
    let corrected = ms
        .data_column(VisibilityDataColumn::CorrectedData)
        .expect("corrected data column");
    let model = ms
        .data_column(VisibilityDataColumn::ModelData)
        .expect("model data column");
    for row in 0..ms.row_count() {
        let ArrayValue::Complex32(corrected_values) = corrected.get(row).expect("corrected row")
        else {
            panic!("expected complex corrected data");
        };
        let ArrayValue::Complex32(model_values) = model.get(row).expect("model row") else {
            panic!("expected complex model data");
        };
        assert_eq!(corrected_values.shape(), model_values.shape());
        for (corrected, model) in corrected_values.iter().zip(model_values.iter()) {
            assert!(
                (corrected.re - model.re).abs() <= 1.0e-3
                    && (corrected.im - model.im).abs() <= 1.0e-3,
                "expected corrected value close to model ({:.6},{:.6}), got ({:.6},{:.6})",
                model.re,
                model.im,
                corrected.re,
                corrected.im
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
        ColumnSchema::scalar("TIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ARRAY_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("TIME_EXTRA_PREC", casa_core::PrimitiveType::Float64),
        ColumnSchema::array_variable("CPARAM", casa_core::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("PARAMERR", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_core::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("SNR", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("WEIGHT", casa_core::PrimitiveType::Float32, Some(1)),
    ])
    .expect("valid schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
        readme: Vec::new(),
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

pub fn create_apply_gain_caltable(
    root: &Path,
    field_names: &[&str],
    rows: &[SyntheticGainSolutionRow],
) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ARRAY_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("TIME_EXTRA_PREC", casa_core::PrimitiveType::Float64),
        ColumnSchema::array_variable("CPARAM", casa_core::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("PARAMERR", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_core::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("SNR", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("WEIGHT", casa_core::PrimitiveType::Float32, Some(1)),
    ])
    .expect("valid gain caltable schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
        readme: Vec::new(),
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
        ColumnSchema::scalar("TIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ARRAY_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("TIME_EXTRA_PREC", casa_core::PrimitiveType::Float64),
        ColumnSchema::array_variable("FPARAM", casa_core::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_core::PrimitiveType::Bool, Some(1)),
    ])
    .expect("valid delay caltable schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "K Jones".to_string(),
        readme: Vec::new(),
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
        ColumnSchema::scalar("TIME", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_core::PrimitiveType::Float64),
        ColumnSchema::scalar("CAL_DESC_ID", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("SCALE_FACTOR", casa_core::PrimitiveType::Complex32),
        ColumnSchema::scalar("N_POLY_AMP", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("N_POLY_PHASE", casa_core::PrimitiveType::Int32),
        ColumnSchema::scalar("PHASE_UNITS", casa_core::PrimitiveType::String),
        ColumnSchema::array_variable("VALID_DOMAIN", casa_core::PrimitiveType::Float64, Some(1)),
        ColumnSchema::array_variable("POLY_COEFF_AMP", casa_core::PrimitiveType::Float64, Some(4)),
        ColumnSchema::array_variable(
            "POLY_COEFF_PHASE",
            casa_core::PrimitiveType::Float64,
            Some(4),
        ),
    ])
    .expect("valid BPOLY schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "BPOLY".to_string(),
        readme: Vec::new(),
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

pub fn create_gain_solve_weight_spectrum_fixture_ms(root: &Path) -> PathBuf {
    let ms_path = root.join("gain_solve_weight_spectrum_fixture.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::WeightSpectrum)
        .with_main_column(OptionalMainColumn::SigmaSpectrum);
    let mut ms =
        MeasurementSet::create(&ms_path, builder).expect("create weight-spectrum gain solve MS");

    populate_gain_solve_subtables(&mut ms);
    let gains = gains_for_fixture_kind(SyntheticGainFixtureKind::G);
    for time_seconds in [100.0, 101.0, 102.0] {
        add_custom_gain_solve_row_with_weight_spectrum(&mut ms, 0, 1, time_seconds, &gains);
        add_custom_gain_solve_row_with_weight_spectrum(&mut ms, 0, 2, time_seconds, &gains);
        add_custom_gain_solve_row_with_weight_spectrum(&mut ms, 1, 2, time_seconds, &gains);
    }

    ms.save().expect("save weight-spectrum gain solve MS");
    ms_path
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

pub fn create_gain_solve_model_column_fixture_ms(
    root: &Path,
    model_values: [Complex32; 4],
) -> PathBuf {
    create_gain_solve_model_column_fixture_ms_for_kind(
        root,
        SyntheticGainFixtureKind::G,
        model_values,
    )
}

pub fn create_gain_solve_model_column_fixture_ms_for_kind(
    root: &Path,
    kind: SyntheticGainFixtureKind,
    model_values: [Complex32; 4],
) -> PathBuf {
    let ms_path = root.join("gain_solve_model_fixture.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::ModelData);
    let mut ms = MeasurementSet::create(&ms_path, builder).expect("create model-column MS");

    populate_gain_solve_subtables(&mut ms);
    let gains = gains_for_fixture_kind(kind);
    for time_seconds in [100.0, 101.0, 102.0] {
        add_custom_gain_solve_row_with_model(
            &mut ms,
            0,
            1,
            0,
            1,
            time_seconds,
            &gains,
            model_values,
        );
        add_custom_gain_solve_row_with_model(
            &mut ms,
            0,
            2,
            0,
            1,
            time_seconds,
            &gains,
            model_values,
        );
        add_custom_gain_solve_row_with_model(
            &mut ms,
            1,
            2,
            0,
            1,
            time_seconds,
            &gains,
            model_values,
        );
    }

    ms.save().expect("save model-column gain solve MS");
    ms_path
}

fn add_custom_gain_solve_row_with_weight_spectrum(
    ms: &mut MeasurementSet,
    antenna1: i32,
    antenna2: i32,
    time: f64,
    gains: &[[Complex32; 2]; 3],
) {
    let g1 = gains[usize::try_from(antenna1).expect("antenna1 index")];
    let g2 = gains[usize::try_from(antenna2).expect("antenna2 index")];
    let rr = g1[0] * g2[0].conj();
    let ll = g1[1] * g2[1].conj();
    let data_values = [rr, ll, -rr, -ll];

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
                "FIELD_ID" => RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
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
                "SCAN_NUMBER" => {
                    RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(1)))
                }
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
                "WEIGHT_SPECTRUM" => RecordField::new(
                    "WEIGHT_SPECTRUM",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![1.0, 1.0, 0.0, 0.0])
                            .unwrap(),
                    )),
                ),
                // CASA's solver weights come from SIGMA_SPECTRUM when both
                // spectrum columns exist; a zero sigma gives a zero weight.
                "SIGMA_SPECTRUM" => RecordField::new(
                    "SIGMA_SPECTRUM",
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![1.0, 1.0, 0.0, 0.0])
                            .unwrap(),
                    )),
                ),
                "DATA" => RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), data_values.to_vec()).unwrap(),
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
        .expect("add weight-spectrum gain solve row");
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
    add_custom_gain_solve_row_with_model(
        ms,
        antenna1,
        antenna2,
        field_id,
        scan_number,
        time,
        gains,
        [Complex32::new(1.0, 0.0); 4],
    );
}

#[allow(clippy::too_many_arguments)]
fn add_custom_gain_solve_row_with_model(
    ms: &mut MeasurementSet,
    antenna1: i32,
    antenna2: i32,
    field_id: i32,
    scan_number: i32,
    time: f64,
    gains: &[[Complex32; 2]; 3],
    model_values: [Complex32; 4],
) {
    let g1 = gains[usize::try_from(antenna1).expect("antenna1 index")];
    let g2 = gains[usize::try_from(antenna2).expect("antenna2 index")];
    let rr = g1[0] * g2[0].conj();
    let ll = g1[1] * g2[1].conj();
    let data_values = [
        rr * model_values[0],
        ll * model_values[1],
        rr * model_values[2],
        ll * model_values[3],
    ];

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
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), data_values.to_vec()).unwrap(),
                    )),
                ),
                "MODEL_DATA" => RecordField::new(
                    "MODEL_DATA",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), model_values.to_vec()).unwrap(),
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
                .cell_accessor_mut(row, column)
                .and_then(|mut cell| cell.set(Value::Array(direction.clone())))
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
            .cell_accessor_mut(row, "MOUNT")
            .and_then(|mut cell| cell.set(Value::Scalar(ScalarValue::String(mount.to_string()))))
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
        ColumnSchema::scalar("NUM_RECEPTORS", casa_core::PrimitiveType::Int32),
        ColumnSchema::array_variable(
            "SPECTRAL_WINDOW_ID",
            casa_core::PrimitiveType::Int32,
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

pub fn make_subtable_row(columns: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
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
            casa_core::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            casa_core::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            casa_core::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            casa_core::PrimitiveType::Float32 => Value::Scalar(ScalarValue::Float32(0.0)),
            casa_core::PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
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
                casa_core::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<bool>::new()).unwrap(),
                )),
                casa_core::PrimitiveType::Int32 => Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<i32>::new()).unwrap(),
                )),
                casa_core::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<f32>::new()).unwrap(),
                )),
                casa_core::PrimitiveType::Float64 => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), Vec::<f64>::new()).unwrap(),
                )),
                casa_core::PrimitiveType::String => Value::Array(ArrayValue::String(
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
            Some(casa_core::PrimitiveType::Bool) => Value::Scalar(ScalarValue::Bool(false)),
            Some(casa_core::PrimitiveType::Int32) => Value::Scalar(ScalarValue::Int32(0)),
            Some(casa_core::PrimitiveType::Float32) => Value::Scalar(ScalarValue::Float32(0.0)),
            Some(casa_core::PrimitiveType::Float64) => Value::Scalar(ScalarValue::Float64(0.0)),
            Some(casa_core::PrimitiveType::String) => {
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
                Some(casa_core::PrimitiveType::Bool) => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![false; shape.iter().product()])
                        .unwrap(),
                )),
                Some(casa_core::PrimitiveType::Float32) => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        IxDyn(&shape).f(),
                        vec![0.0_f32; shape.iter().product()],
                    )
                    .unwrap(),
                )),
                Some(casa_core::PrimitiveType::Complex32) => Value::Array(ArrayValue::Complex32(
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
