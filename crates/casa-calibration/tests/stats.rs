// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use std::path::PathBuf;

use casa_tables::{ColumnSchema, DataManagerKind, Table, TableInfo, TableOptions, TableSchema};
use casa_types::Complex32;
#[cfg(feature = "slow-tests")]
use serde_json::Value as JsonValue;
use tempfile::TempDir;

use casa_calibration::{CalibrationStatsAxis, CalibrationStatsRequest, calibration_stats};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

fn create_misc_stats_table(root: &std::path::Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable("FLAG", casa_types::PrimitiveType::Int32, Some(1)),
        ColumnSchema::array_variable("ARRAY_I32", casa_types::PrimitiveType::Int32, Some(1)),
        ColumnSchema::array_variable("ARRAY_I64", casa_types::PrimitiveType::Int64, Some(1)),
        ColumnSchema::array_variable("ARRAY_F64", casa_types::PrimitiveType::Float64, Some(1)),
        ColumnSchema::scalar("SCALAR_BOOL", casa_types::PrimitiveType::Bool),
        ColumnSchema::scalar("SCALAR_I64", casa_types::PrimitiveType::Int64),
        ColumnSchema::scalar("SCALAR_F32", casa_types::PrimitiveType::Float32),
        ColumnSchema::scalar("SCALAR_STR", casa_types::PrimitiveType::String),
    ])
    .expect("valid misc stats schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
    });
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(7))),
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(8))),
            RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(9))),
            RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(10))),
            RecordField::new(
                "FLAG",
                Value::Array(ArrayValue::from_i32_vec(vec![1, 0, 1])),
            ),
            RecordField::new(
                "ARRAY_I32",
                Value::Array(ArrayValue::from_i32_vec(vec![2, 4, 6])),
            ),
            RecordField::new(
                "ARRAY_I64",
                Value::Array(ArrayValue::from_i64_vec(vec![3, 5, 7])),
            ),
            RecordField::new(
                "ARRAY_F64",
                Value::Array(ArrayValue::from_f64_vec(vec![1.5, 2.5, 3.5])),
            ),
            RecordField::new("SCALAR_BOOL", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("SCALAR_I64", Value::Scalar(ScalarValue::Int64(42))),
            RecordField::new("SCALAR_F32", Value::Scalar(ScalarValue::Float32(2.25))),
            RecordField::new(
                "SCALAR_STR",
                Value::Scalar(ScalarValue::String("bad".to_string())),
            ),
        ]))
        .expect("insert misc stats row");
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save misc stats table");
    root.to_path_buf()
}

fn create_grouping_fallback_stats_table(root: &std::path::Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int64),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("OBSERVATION_ID", casa_types::PrimitiveType::String),
        ColumnSchema::array_variable("CPARAM", casa_types::PrimitiveType::Complex32, Some(1)),
    ])
    .expect("valid grouping fallback schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
    });
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "FIELD_ID",
                Value::Scalar(ScalarValue::Int64(i64::from(i32::MAX) + 1)),
            ),
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::String("science".to_string())),
            ),
            RecordField::new(
                "OBSERVATION_ID",
                Value::Scalar(ScalarValue::String("obs".to_string())),
            ),
            RecordField::new(
                "CPARAM",
                Value::Array(ArrayValue::from_complex32_vec(vec![
                    Complex32::new(3.0, 4.0),
                    Complex32::new(5.0, 12.0),
                ])),
            ),
        ]))
        .expect("insert grouping fallback row");
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save grouping fallback table");
    root.to_path_buf()
}

#[test]
fn calibration_stats_axis_parse_and_display_names_cover_aliases() {
    assert_eq!(
        CalibrationStatsAxis::parse("amp"),
        CalibrationStatsAxis::Amplitude
    );
    assert_eq!(
        CalibrationStatsAxis::parse("amplitude"),
        CalibrationStatsAxis::Amplitude
    );
    assert_eq!(
        CalibrationStatsAxis::parse("phase"),
        CalibrationStatsAxis::Phase
    );
    assert_eq!(
        CalibrationStatsAxis::parse("real"),
        CalibrationStatsAxis::Real
    );
    assert_eq!(
        CalibrationStatsAxis::parse("imag"),
        CalibrationStatsAxis::Imaginary
    );
    assert_eq!(
        CalibrationStatsAxis::parse("weight_spectrum"),
        CalibrationStatsAxis::Column("WEIGHT_SPECTRUM".to_string())
    );

    assert_eq!(CalibrationStatsAxis::Amplitude.display_name(), "amplitude");
    assert_eq!(CalibrationStatsAxis::Phase.display_name(), "phase");
    assert_eq!(CalibrationStatsAxis::Real.display_name(), "real");
    assert_eq!(CalibrationStatsAxis::Imaginary.display_name(), "imaginary");
    assert_eq!(
        CalibrationStatsAxis::Column("COL".to_string()).display_name(),
        "COL"
    );
    assert_eq!(
        CalibrationStatsRequest::default().axis,
        CalibrationStatsAxis::Amplitude
    );
}

#[test]
fn calibration_stats_reports_global_and_grouped_amplitude_values() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_apply_gain_caltable(
        &dir.path().join("stats.gcal"),
        &["FIELD0", "FIELD1"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 10.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 20.0,
                field_id: 1,
                spectral_window_id: 1,
                antenna_id: 1,
                gains: vec![Complex32::new(3.0, 0.0), Complex32::new(4.0, 0.0)],
                flags: vec![true, false],
            },
        ],
    );

    let report = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: false,
        },
    )
    .expect("compute stats");

    assert_eq!(report.global.npts, 4);
    assert_eq!(report.global.flagged_npts, 1);
    assert_eq!(report.global.total_npts, 4);
    assert!((report.global.sum - 10.0).abs() <= 1.0e-9);
    assert!((report.global.mean - 2.5).abs() <= 1.0e-9);
    assert!((report.global.median - 2.5).abs() <= 1.0e-9);
    assert!((report.global.min - 1.0).abs() <= 1.0e-9);
    assert!((report.global.max - 4.0).abs() <= 1.0e-9);
    assert!((report.global.q1 - 1.5).abs() <= 1.0e-9);
    assert!((report.global.q3 - 3.5).abs() <= 1.0e-9);
    assert!((report.global.quartile - 2.0).abs() <= 1.0e-9);

    assert_eq!(report.by_field_id.len(), 2);
    assert_eq!(report.by_spectral_window_id.len(), 2);
    assert_eq!(report.by_antenna1_id.len(), 2);
    assert_eq!(report.by_observation_id.len(), 1);
    assert_eq!(report.by_field_id[0].key, 0);
    assert!((report.by_field_id[0].stats.sum - 3.0).abs() <= 1.0e-9);
    assert_eq!(report.by_field_id[1].key, 1);
    assert!((report.by_field_id[1].stats.sum - 7.0).abs() <= 1.0e-9);
}

#[test]
fn calibration_stats_can_exclude_flagged_values() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_apply_gain_caltable(
        &dir.path().join("stats-flags.gcal"),
        &["FIELD0"],
        &[common::SyntheticGainSolutionRow {
            time_seconds: 10.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            gains: vec![Complex32::new(1.0, 0.0), Complex32::new(10.0, 0.0)],
            flags: vec![false, true],
        }],
    );

    let report = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: true,
        },
    )
    .expect("compute stats");

    assert_eq!(report.global.total_npts, 2);
    assert_eq!(report.global.flagged_npts, 1);
    assert_eq!(report.global.npts, 1);
    assert!((report.global.sum - 1.0).abs() <= 1.0e-9);
}

#[test]
fn calibration_stats_supports_phase_real_imaginary_and_scalar_axes() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_apply_gain_caltable(
        &dir.path().join("axes.gcal"),
        &["FIELD0"],
        &[common::SyntheticGainSolutionRow {
            time_seconds: 12.5,
            field_id: 3,
            spectral_window_id: 4,
            antenna_id: 5,
            gains: vec![Complex32::new(1.0, 1.0), Complex32::new(-2.0, 0.5)],
            flags: vec![false, true],
        }],
    );

    let phase = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Phase,
            datacolumn: Some("gain".to_string()),
            use_flags: false,
        },
    )
    .expect("phase stats");
    assert_eq!(phase.datacolumn.as_deref(), Some("CPARAM"));
    assert_eq!(phase.global.npts, 2);
    assert!((phase.global.min - std::f64::consts::FRAC_PI_4).abs() <= 1.0e-9);
    assert!((phase.global.max - 2.896613990462929).abs() <= 1.0e-9);

    let real = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Real,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: false,
        },
    )
    .expect("real stats");
    assert!((real.global.sum + 1.0).abs() <= 1.0e-9);
    assert_eq!(real.by_field_id[0].key, 3);

    let imaginary = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Imaginary,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: false,
        },
    )
    .expect("imaginary stats");
    assert!((imaginary.global.sum - 1.5).abs() <= 1.0e-9);
    assert_eq!(imaginary.by_antenna1_id[0].key, 5);

    let scalar = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("FIELD_ID".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("scalar column stats");
    assert_eq!(scalar.datacolumn, None);
    assert_eq!(scalar.global.npts, 1);
    assert!((scalar.global.sum - 3.0).abs() <= 1.0e-9);
}

#[test]
fn calibration_stats_supports_real_array_and_scalar_columns() {
    let dir = TempDir::new().expect("tempdir");
    let misc_path = create_misc_stats_table(&dir.path().join("misc-stats.tbl"));
    let gain_path = common::create_apply_gain_caltable(
        &dir.path().join("real-arrays.gcal"),
        &["FIELD0"],
        &[common::SyntheticGainSolutionRow {
            time_seconds: 25.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            gains: vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)],
            flags: vec![true, false],
        }],
    );

    let bools = calibration_stats(
        &gain_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("FLAG".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("bool array stats");
    assert_eq!(bools.global.npts, 2);
    assert!((bools.global.sum - 1.0).abs() <= 1.0e-9);

    let errors = calibration_stats(
        &gain_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("PARAMERR".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("float32 array stats");
    assert_eq!(errors.global.npts, 2);
    assert!(errors.global.mean.is_finite());

    let times = calibration_stats(
        &gain_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("TIME".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("float64 scalar stats");
    assert!((times.global.sum - 25.0).abs() <= 1.0e-9);

    let ints = calibration_stats(
        &misc_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("ARRAY_I32".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("int32 array stats");
    assert!((ints.global.sum - 12.0).abs() <= 1.0e-9);

    let longs = calibration_stats(
        &misc_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("ARRAY_I64".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("int64 array stats");
    assert!((longs.global.sum - 15.0).abs() <= 1.0e-9);

    let doubles = calibration_stats(
        &misc_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("ARRAY_F64".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("float64 array stats");
    assert!((doubles.global.mean - 2.5).abs() <= 1.0e-9);

    let scalar_bool = calibration_stats(
        &misc_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("SCALAR_BOOL".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("bool scalar stats");
    assert!((scalar_bool.global.sum - 1.0).abs() <= 1.0e-9);

    let scalar_i64 = calibration_stats(
        &misc_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("SCALAR_I64".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("int64 scalar stats");
    assert!((scalar_i64.global.sum - 42.0).abs() <= 1.0e-9);

    let scalar_f32 = calibration_stats(
        &misc_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("SCALAR_F32".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect("float32 scalar stats");
    assert!((scalar_f32.global.sum - 2.25).abs() <= 1.0e-9);
}

#[test]
fn calibration_stats_reports_missing_unsupported_and_empty_value_errors() {
    let dir = TempDir::new().expect("tempdir");
    let gain_path = common::create_apply_gain_caltable(
        &dir.path().join("errors.gcal"),
        &["FIELD0"],
        &[common::SyntheticGainSolutionRow {
            time_seconds: 10.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            gains: vec![Complex32::new(1.0, 0.0), Complex32::new(10.0, 0.0)],
            flags: vec![true, true],
        }],
    );
    let delay_path = common::create_apply_delay_caltable(
        &dir.path().join("errors.kcal"),
        &["FIELD0"],
        &[common::SyntheticDelaySolutionRow {
            time_seconds: 10.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            delays_ns: vec![1.0, 2.0],
            flags: vec![false, false],
        }],
    );
    let misc_path = create_misc_stats_table(&dir.path().join("errors.tbl"));

    let missing_column = calibration_stats(
        &gain_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("does_not_exist".to_string()),
            use_flags: false,
        },
    )
    .expect_err("missing datacolumn");
    assert!(missing_column.to_string().contains("DOES_NOT_EXIST"));

    let wrong_complex_type = calibration_stats(
        &delay_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("float".to_string()),
            use_flags: false,
        },
    )
    .expect_err("float axis should reject FPARAM");
    assert!(
        wrong_complex_type
            .to_string()
            .contains("selected datacolumn is not complex-valued")
    );

    let scalar_as_complex = calibration_stats(
        &gain_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("FIELD_ID".to_string()),
            use_flags: false,
        },
    )
    .expect_err("scalar datacolumn cannot be read as a complex array");
    assert!(scalar_as_complex.to_string().contains("FIELD_ID"));

    let unsupported_array = calibration_stats(
        &gain_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("CPARAM".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect_err("complex array cannot be treated as real");
    assert!(
        unsupported_array
            .to_string()
            .contains("column is not real-valued")
    );

    let unsupported_scalar = calibration_stats(
        &misc_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("SCALAR_STR".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect_err("string scalar cannot be treated as real");
    assert!(
        unsupported_scalar
            .to_string()
            .contains("column is not real-valued")
    );

    let missing_column_via_column_axis = calibration_stats(
        &misc_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Column("DOES_NOT_EXIST".to_string()),
            datacolumn: None,
            use_flags: false,
        },
    )
    .expect_err("missing column should fail scalar fallback");
    assert!(
        missing_column_via_column_axis
            .to_string()
            .contains("DOES_NOT_EXIST")
    );

    let empty = calibration_stats(
        &gain_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: true,
        },
    )
    .expect_err("all flagged values should be filtered");
    assert!(empty.to_string().contains("no values available"));

    let open_error = calibration_stats(
        dir.path().join("missing.gcal"),
        &CalibrationStatsRequest::default(),
    )
    .expect_err("missing table should not open");
    assert!(
        open_error
            .to_string()
            .contains("failed to open calibration table")
    );
}

#[test]
fn calibration_stats_falls_back_when_grouping_columns_are_missing_or_invalid() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = create_grouping_fallback_stats_table(&dir.path().join("grouping.tbl"));

    let report = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: false,
        },
    )
    .expect("stats should still compute");

    assert_eq!(report.global.npts, 2);
    assert!((report.global.sum - 18.0).abs() <= 1.0e-9);
    assert_eq!(report.by_field_id[0].key, 0);
    assert_eq!(report.by_spectral_window_id[0].key, 0);
    assert_eq!(report.by_antenna1_id[0].key, 0);
    assert_eq!(report.by_observation_id[0].key, 0);
}

#[cfg(feature = "slow-tests")]
#[test]
fn calibration_stats_matches_casa_calstat_on_phase_gain_amplitudes() {
    let dir = TempDir::new().expect("tempdir");
    let phase_table = match common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15") {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let rust = calibration_stats(
        &phase_table,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: false,
        },
    )
    .expect("compute Rust stats");
    let casa = match common::run_casa_calstat(&phase_table, "amp", "CPARAM") {
        Ok(value) => value,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let casa_stats = casa
        .get("CPARAM")
        .expect("CPARAM key in CASA calstat output");
    assert_casa_stat_close(casa_stats, "npts", rust.global.npts as f64, 1.0e-9);
    assert_casa_stat_close(casa_stats, "sum", rust.global.sum, 1.0e-5);
    assert_casa_stat_close(casa_stats, "sumsq", rust.global.sumsq, 1.0e-4);
    assert_casa_stat_close(casa_stats, "mean", rust.global.mean, 1.0e-6);
    assert_casa_stat_close(casa_stats, "median", rust.global.median, 1.0e-6);
    assert_casa_stat_close(casa_stats, "medabsdevmed", rust.global.medabsdevmed, 1.0e-6);
    assert_casa_stat_close(casa_stats, "quartile", rust.global.quartile, 1.0e-6);
    assert_casa_stat_close(casa_stats, "min", rust.global.min, 1.0e-6);
    assert_casa_stat_close(casa_stats, "max", rust.global.max, 1.0e-6);
    assert_casa_stat_close(casa_stats, "var", rust.global.var, 1.0e-6);
    assert_casa_stat_close(casa_stats, "stddev", rust.global.stddev, 1.0e-6);
    assert_casa_stat_close(casa_stats, "rms", rust.global.rms, 1.0e-6);
}

#[cfg(feature = "slow-tests")]
fn assert_casa_stat_close(stats: &JsonValue, key: &str, rust_value: f64, tolerance: f64) {
    let casa_value = stats
        .get(key)
        .and_then(JsonValue::as_f64)
        .unwrap_or_else(|| panic!("missing CASA stat {key}"));
    let diff = (casa_value - rust_value).abs();
    assert!(
        diff <= tolerance,
        "CASA stat mismatch for {key}: casa={casa_value:.9} rust={rust_value:.9} tolerance={tolerance}"
    );
}
