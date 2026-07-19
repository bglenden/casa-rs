// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use std::path::{Path, PathBuf};

use casa_calibration::{
    CalibrationPlotPreset, CalibrationPlotRequest, build_calibration_plot_payload,
};
use casa_ms::{MsAxis, MsPlotPayload, MsSelection};
use casa_tables::{ColumnSchema, Table, TableInfo, TableOptions, TableSchema};
use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use tempfile::tempdir;

#[test]
fn gain_phase_plot_builds_time_scatter_from_caltable() {
    let dir = tempdir().expect("tempdir");
    let table_path = common::create_minimal_complex_caltable(&dir.path().join("phase.gcal"));

    let payload = build_calibration_plot_payload(
        &CalibrationPlotRequest {
            measurement_set_path: None,
            calibration_table_path: Some(table_path),
            selection: MsSelection::default(),
        },
        CalibrationPlotPreset::GainPhaseVsTime,
    )
    .expect("build gain-phase payload");

    let MsPlotPayload::Scatter(scatter) = payload else {
        panic!("expected scatter payload");
    };
    assert_eq!(scatter.x_axis, MsAxis::Time);
    assert_eq!(scatter.y_axis, MsAxis::Phase);
    assert!(!scatter.series.is_empty());
    assert_eq!(scatter.series[0].points.len(), 1);
}

#[test]
fn corrected_frequency_plot_uses_corrected_data_column() {
    let dir = tempdir().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), true);

    let payload = build_calibration_plot_payload(
        &CalibrationPlotRequest {
            measurement_set_path: Some(ms_path),
            calibration_table_path: None,
            selection: MsSelection::default(),
        },
        CalibrationPlotPreset::CorrectedAmplitudeVsFrequency,
    )
    .expect("build corrected-data payload");

    let MsPlotPayload::Scatter(scatter) = payload else {
        panic!("expected scatter payload");
    };
    assert_eq!(scatter.x_axis, MsAxis::Frequency);
    assert_eq!(scatter.y_axis, MsAxis::Amplitude);
    assert!(!scatter.series.is_empty());
}

#[test]
fn corrected_plot_requires_corrected_data_column() {
    let dir = tempdir().expect("tempdir");
    let ms_path = common::create_apply_fixture_ms(dir.path(), false);

    let error = build_calibration_plot_payload(
        &CalibrationPlotRequest {
            measurement_set_path: Some(ms_path),
            calibration_table_path: None,
            selection: MsSelection::default(),
        },
        CalibrationPlotPreset::CorrectedAmplitudeVsTime,
    )
    .expect_err("missing corrected data should fail");

    assert!(
        error
            .to_string()
            .contains("does not contain CORRECTED_DATA")
    );
}

#[test]
fn bandpass_frequency_plot_reads_channelized_cparam() {
    let dir = tempdir().expect("tempdir");
    let table_path = create_bandpass_fixture_caltable(&dir.path().join("test.bcal"));

    let payload = build_calibration_plot_payload(
        &CalibrationPlotRequest {
            measurement_set_path: None,
            calibration_table_path: Some(table_path),
            selection: MsSelection::default(),
        },
        CalibrationPlotPreset::BandpassAmplitudeVsFrequency,
    )
    .expect("build bandpass payload");

    let MsPlotPayload::Scatter(scatter) = payload else {
        panic!("expected scatter payload");
    };
    assert_eq!(scatter.x_axis, MsAxis::Frequency);
    assert_eq!(scatter.y_axis, MsAxis::Amplitude);
    assert_eq!(scatter.series.len(), 4);
    assert_eq!(scatter.series[0].points.len(), 3);
}

#[cfg(feature = "slow-tests")]
#[test]
fn casa_generated_gain_and_bandpass_tables_build_plot_payloads() {
    let dir = tempdir().expect("tempdir");
    let (phase_gcal, _t_gcal, bandpass) = match common::generate_casa_exemplars(dir.path()) {
        Ok(paths) => paths,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let gain_payload = build_calibration_plot_payload(
        &CalibrationPlotRequest {
            measurement_set_path: None,
            calibration_table_path: Some(phase_gcal),
            selection: MsSelection::default(),
        },
        CalibrationPlotPreset::GainPhaseVsTime,
    )
    .expect("build CASA gain plot");
    let bandpass_payload = build_calibration_plot_payload(
        &CalibrationPlotRequest {
            measurement_set_path: None,
            calibration_table_path: Some(bandpass),
            selection: MsSelection::default(),
        },
        CalibrationPlotPreset::BandpassAmplitudeVsFrequency,
    )
    .expect("build CASA bandpass plot");

    let MsPlotPayload::Scatter(gain) = gain_payload else {
        panic!("expected scatter payload");
    };
    let MsPlotPayload::Scatter(bp) = bandpass_payload else {
        panic!("expected scatter payload");
    };
    assert_eq!(gain.y_axis, MsAxis::Phase);
    assert_eq!(bp.x_axis, MsAxis::Frequency);
    assert!(!gain.series.is_empty());
    assert!(!bp.series.is_empty());
}

fn create_bandpass_fixture_caltable(root: &Path) -> PathBuf {
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
        ColumnSchema::array_variable("CPARAM", casa_types::PrimitiveType::Complex32, Some(2)),
        ColumnSchema::array_variable("FLAG", casa_types::PrimitiveType::Bool, Some(2)),
    ])
    .expect("valid schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "B Jones".to_string(),
        readme: Vec::new(),
    });
    for (key, value) in [
        ("ParType", "Complex"),
        ("VisCal", "B Jones"),
        ("MSName", "synthetic.ms"),
        ("PolBasis", "unknown"),
        ("CASA_Version", "test"),
    ] {
        table
            .keywords_mut()
            .upsert(key, Value::Scalar(ScalarValue::String(value.to_string())));
    }
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
    for antenna in [0, 1] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(10.0))),
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(antenna))),
                RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(60.0))),
                RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new("TIME_EXTRA_PREC", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new(
                    "CPARAM",
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2, 3]).f(),
                            vec![
                                Complex32 { re: 1.0, im: 0.0 },
                                Complex32 { re: 0.8, im: 0.1 },
                                Complex32 { re: 0.9, im: -0.1 },
                                Complex32 { re: 1.0, im: 0.0 },
                                Complex32 { re: 0.85, im: 0.05 },
                                Complex32 {
                                    re: 0.95,
                                    im: -0.05,
                                },
                            ],
                        )
                        .expect("shape"),
                    )),
                ),
                RecordField::new(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2, 3]).f(),
                            vec![false, false, false, false, false, false],
                        )
                        .expect("shape"),
                    )),
                ),
            ]))
            .expect("row insert");
    }
    table
        .save(TableOptions::new(root))
        .expect("save bandpass table");

    save_empty_subtable(&root.join("OBSERVATION"));
    save_empty_subtable(&root.join("ANTENNA"));
    save_field_subtable(&root.join("FIELD"));
    save_spectral_window_subtable(&root.join("SPECTRAL_WINDOW"));
    save_empty_subtable(&root.join("HISTORY"));
    root.to_path_buf()
}

fn save_empty_subtable(path: &Path) {
    Table::with_schema(TableSchema::new(vec![]).expect("empty schema"))
        .save(TableOptions::new(path))
        .expect("save empty subtable");
}

fn save_field_subtable(path: &Path) {
    let schema = TableSchema::new(vec![ColumnSchema::scalar(
        "NAME",
        casa_types::PrimitiveType::String,
    )])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "NAME",
            Value::Scalar(ScalarValue::String("field0".to_string())),
        )]))
        .expect("row");
    table.save(TableOptions::new(path)).expect("save field");
}

fn save_spectral_window_subtable(path: &Path) {
    let schema = TableSchema::new(vec![
        ColumnSchema::array_variable("CHAN_FREQ", casa_types::PrimitiveType::Float64, Some(1)),
        ColumnSchema::scalar("NUM_CHAN", casa_types::PrimitiveType::Int32),
    ])
    .expect("schema");
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "CHAN_FREQ",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(IxDyn(&[3]).f(), vec![1.0e9, 1.1e9, 1.2e9])
                        .expect("shape"),
                )),
            ),
            RecordField::new("NUM_CHAN", Value::Scalar(ScalarValue::Int32(3))),
        ]))
        .expect("row");
    table.save(TableOptions::new(path)).expect("save spw");
}
