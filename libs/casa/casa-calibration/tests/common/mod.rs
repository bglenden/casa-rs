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

mod ms_rows;
pub mod synthetic_tables;

use ms_rows::*;
use synthetic_tables::*;

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
