// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use std::fs;
use std::path::PathBuf;

use casa_calibration::{CalibrationIssueSeverity, summarize_tables};
use casa_tables::{ColumnSchema, DataManagerKind, Table, TableInfo, TableOptions, TableSchema};
use casa_types::{Complex32, RecordField, RecordValue, ScalarValue, Value};
use tempfile::TempDir;

use casa_calibration::{CalibrationParameterFamily, summarize_table};

fn create_malformed_summary_table(root: &std::path::Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::String),
        ColumnSchema::scalar("INTERVAL", casa_types::PrimitiveType::String),
    ])
    .expect("valid malformed schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "NotCalibration".to_string(),
        sub_type: "X Jones".to_string(),
        readme: Vec::new(),
    });
    table.keywords_mut().upsert(
        "ANTENNA",
        Value::table_ref(root.join("broken-antenna").display().to_string()),
    );
    table
        .keywords_mut()
        .upsert("HISTORY", Value::table_ref("././missing-history"));
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "TIME",
                Value::Scalar(ScalarValue::String("soon".to_string())),
            ),
            RecordField::new(
                "FIELD_ID",
                Value::Scalar(ScalarValue::String("field-zero".to_string())),
            ),
            RecordField::new(
                "ANTENNA1",
                Value::Scalar(ScalarValue::String("ea01".to_string())),
            ),
            RecordField::new(
                "INTERVAL",
                Value::Scalar(ScalarValue::String("thirty".to_string())),
            ),
        ]))
        .expect("insert malformed row");
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save malformed table");
    fs::write(root.join("broken-antenna"), b"not a table").expect("write broken subtable path");
    root.to_path_buf()
}

fn create_sparse_unsupported_float_summary_table(root: &std::path::Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("OBSERVATION_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable("FPARAM", casa_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_types::PrimitiveType::Bool, Some(1)),
    ])
    .expect("valid sparse float schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "B Jones".to_string(),
        readme: Vec::new(),
    });
    table.keywords_mut().upsert(
        "ParType",
        Value::Scalar(ScalarValue::String("Float".to_string())),
    );
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("B Jones".to_string())),
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
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(3))),
            RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
            RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "FPARAM",
                Value::Array(casa_types::ArrayValue::from_f32_vec(vec![1.0, 2.0])),
            ),
            RecordField::new(
                "FLAG",
                Value::Array(casa_types::ArrayValue::from_bool_vec(vec![false, true])),
            ),
        ]))
        .expect("insert sparse float row");
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save sparse float table");

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

fn create_empty_inferred_complex_summary_table(root: &std::path::Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("OBSERVATION_ID", casa_types::PrimitiveType::Int32),
        ColumnSchema::array_variable("CPARAM", casa_types::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("FLAG", casa_types::PrimitiveType::Bool, Some(1)),
    ])
    .expect("valid inferred complex schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
        readme: Vec::new(),
    });
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
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save inferred complex table");

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

fn create_invalid_bpoly_summary_table(root: &std::path::Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casa_types::PrimitiveType::Int32),
    ])
    .expect("valid invalid-bpoly schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "NotCalibration".to_string(),
        sub_type: "BPOLY".to_string(),
        readme: Vec::new(),
    });
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(1.0))),
            RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
        ]))
        .expect("insert invalid bpoly row");
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save invalid bpoly table");
    fs::write(root.join("CAL_DESC"), b"not a table").expect("write invalid CAL_DESC");
    root.to_path_buf()
}

#[test]
fn summarize_synthetic_complex_caltable() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_minimal_complex_caltable(&dir.path().join("synthetic.gcal"));
    let summary = summarize_table(&table_path).expect("summary");

    assert_eq!(summary.table_type, "Calibration");
    assert_eq!(summary.table_subtype, "G Jones");
    assert_eq!(
        summary.parameter_family,
        CalibrationParameterFamily::Complex
    );
    assert_eq!(summary.row_count, 1);
    assert_eq!(summary.field_ids, vec![0]);
    assert_eq!(summary.spectral_window_ids, vec![3]);
    assert_eq!(summary.antenna1_ids, vec![1]);
    assert_eq!(summary.antenna2_ids, vec![-1]);
    assert_eq!(summary.observation_ids, vec![0]);
    assert_eq!(
        summary.parameter_column.parameter_column.as_deref(),
        Some("CPARAM")
    );
    assert!(summary.supported_for_v1_apply());
    assert!(summary.subtables.iter().all(|subtable| subtable.exists));
}

#[test]
fn summarize_delay_k_jones_table_supports_float_family() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_apply_delay_caltable(
        &dir.path().join("synthetic.kcal"),
        &["target"],
        &[common::SyntheticDelaySolutionRow {
            time_seconds: 123.5,
            field_id: 0,
            spectral_window_id: 5,
            antenna_id: 2,
            delays_ns: vec![1.25, -0.75],
            flags: vec![false, true],
        }],
    );
    let summary = summarize_table(&table_path).expect("delay summary");

    assert_eq!(summary.table_subtype, "K Jones");
    assert_eq!(summary.parameter_family, CalibrationParameterFamily::Float);
    assert_eq!(
        summary.parameter_column.parameter_column.as_deref(),
        Some("FPARAM")
    );
    assert_eq!(summary.spectral_window_ids, vec![5]);
    assert_eq!(summary.antenna1_ids, vec![2]);
    assert_eq!(
        summary
            .time_coverage
            .as_ref()
            .map(|coverage| coverage.min_time),
        Some(123.5)
    );
    assert!(summary.supported_for_v1_apply());
    assert!(
        summary
            .issues
            .iter()
            .all(|issue| issue.severity != CalibrationIssueSeverity::Error)
    );
}

#[test]
fn summarize_bpoly_table_reads_cal_desc_spw_ids() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_apply_bpoly_caltable(
        &dir.path().join("synthetic.bpoly"),
        &[
            common::SyntheticBPolySolutionRow {
                time_seconds: 42.0,
                field_id: 0,
                spectral_window_id: 3,
                antenna_id: 1,
                scale_factor: Complex32 { re: 1.0, im: 0.0 },
                valid_domain_hz: [1.0e9, 1.1e9],
                amp_coefficients: vec![vec![1.0, 0.0], vec![1.0, 0.0]],
                phase_coefficients: vec![vec![0.0, 0.0], vec![0.0, 0.0]],
                phase_units: "deg",
            },
            common::SyntheticBPolySolutionRow {
                time_seconds: 45.0,
                field_id: 0,
                spectral_window_id: 7,
                antenna_id: 2,
                scale_factor: Complex32 { re: 0.5, im: 0.5 },
                valid_domain_hz: [1.2e9, 1.3e9],
                amp_coefficients: vec![vec![0.8, 0.1], vec![0.8, 0.1]],
                phase_coefficients: vec![vec![0.0, 0.2], vec![0.0, 0.2]],
                phase_units: "deg",
            },
        ],
    );
    let summary = summarize_table(&table_path).expect("bpoly summary");

    assert_eq!(summary.table_subtype, "BPOLY");
    assert_eq!(
        summary.parameter_column.parameter_column.as_deref(),
        Some("POLY_COEFF_AMP")
    );
    assert_eq!(summary.spectral_window_ids, vec![3, 7]);
    assert!(summary.supported_for_v1_apply());
}

#[test]
fn summarize_malformed_table_reports_validation_and_type_warnings() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = create_malformed_summary_table(&dir.path().join("broken.gcal"));
    let summary = summarize_table(&table_path).expect("malformed summary");

    assert_eq!(summary.table_type, "NotCalibration");
    assert_eq!(
        summary.parameter_family,
        CalibrationParameterFamily::Unknown
    );
    assert_eq!(summary.parameter_column.parameter_column, None);
    assert!(summary.time_coverage.is_none());
    assert!(!summary.supported_for_v1_apply());

    let issue_codes = summary
        .issues
        .iter()
        .map(|issue| issue.code.as_str())
        .collect::<Vec<_>>();
    for expected in [
        "table-info-type",
        "missing-par-type",
        "missing-vis-cal",
        "missing-ms-name",
        "missing-pol-basis",
        "missing-casa-version",
        "missing-column-SPECTRAL_WINDOW_ID",
        "missing-column-ANTENNA2",
        "missing-column-CPARAM",
        "missing-column-FLAG",
        "missing-optional-column-OBSERVATION_ID",
        "missing-optional-column-PARAMERR",
        "missing-optional-column-SNR",
        "missing-optional-column-WEIGHT",
        "missing-observation-subtable",
        "failed-open-subtable-antenna",
        "missing-subtable-field",
        "missing-subtable-spectral_window",
        "dangling-subtable-history",
        "unknown-parameter-family",
        "unexpected-FIELD_ID-type",
        "unexpected-ANTENNA1-type",
        "time-column-type",
        "interval-column-type",
    ] {
        assert!(
            issue_codes.contains(&expected),
            "missing issue code {expected:?}: {issue_codes:?}"
        );
    }
}

#[test]
fn summarize_bpoly_missing_cal_desc_and_batch_open_errors() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_apply_bpoly_caltable(
        &dir.path().join("missing-cal-desc.bpoly"),
        &[common::SyntheticBPolySolutionRow {
            time_seconds: 1.0,
            field_id: 0,
            spectral_window_id: 2,
            antenna_id: 0,
            scale_factor: Complex32 { re: 1.0, im: 0.0 },
            valid_domain_hz: [9.0e8, 9.1e8],
            amp_coefficients: vec![vec![1.0]],
            phase_coefficients: vec![vec![0.0]],
            phase_units: "deg",
        }],
    );
    fs::remove_dir_all(table_path.join("CAL_DESC")).expect("remove CAL_DESC");

    let summary = summarize_table(&table_path).expect("bpoly summary without cal desc");
    let issue_codes = summary
        .issues
        .iter()
        .map(|issue| issue.code.as_str())
        .collect::<Vec<_>>();
    assert!(issue_codes.contains(&"missing-cal-desc"));
    assert!(issue_codes.contains(&"failed-open-cal-desc"));
    assert!(summary.spectral_window_ids.is_empty());

    let missing = dir.path().join("does-not-exist.gcal");
    let batch_error =
        summarize_tables([table_path.as_path(), missing.as_path()]).expect_err("batch open error");
    assert!(batch_error.to_string().contains("does-not-exist.gcal"));
}

#[test]
fn summarize_sparse_float_table_reports_read_failures_and_unsupported_family() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = create_sparse_unsupported_float_summary_table(&dir.path().join("float.bcal"));

    let summary = summarize_table(&table_path).expect("sparse float summary");
    let issue_codes = summary
        .issues
        .iter()
        .map(|issue| issue.code.as_str())
        .collect::<Vec<_>>();

    assert_eq!(summary.parameter_family, CalibrationParameterFamily::Float);
    assert_eq!(
        summary.parameter_column.parameter_column.as_deref(),
        Some("FPARAM")
    );
    let expected = "unsupported-float-family";
    assert!(
        issue_codes.contains(&expected),
        "missing issue code {expected:?}: {issue_codes:?}"
    );
}

#[test]
fn summarize_empty_table_infers_complex_family_from_cparam_column() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = create_empty_inferred_complex_summary_table(&dir.path().join("empty.gcal"));

    let summary = summarize_table(&table_path).expect("empty inferred summary");

    assert_eq!(
        summary.parameter_family,
        CalibrationParameterFamily::Complex
    );
    assert_eq!(
        summary.parameter_column.parameter_column.as_deref(),
        Some("CPARAM")
    );
    assert!(summary.parameter_column.first_cell_shape.is_none());
    assert_eq!(summary.row_count, 0);
}

#[test]
fn summarize_invalid_bpoly_table_reports_shape_and_cal_desc_errors() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = create_invalid_bpoly_summary_table(&dir.path().join("invalid.bpoly"));

    let summary = summarize_table(&table_path).expect("invalid bpoly summary");
    let issue_codes = summary
        .issues
        .iter()
        .map(|issue| issue.code.as_str())
        .collect::<Vec<_>>();

    assert_eq!(summary.table_subtype, "BPOLY");
    assert_eq!(summary.spectral_window_ids, Vec::<i32>::new());
    for expected in [
        "table-info-type",
        "missing-column-ANTENNA1",
        "failed-open-cal-desc",
    ] {
        assert!(
            issue_codes.contains(&expected),
            "missing issue code {expected:?}: {issue_codes:?}"
        );
    }
}
