// SPDX-License-Identifier: LGPL-3.0-or-later
//! Persist the legacy real-valued CalSet and its linked descriptions.

use std::collections::BTreeMap;

use casa_core::PrimitiveType::{Bool, Float32, Float64, Int32, String as Text};
use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::{MeasurementSet, MsResult};
use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::bandpass::bpoly::write_legacy_history;
use crate::writer::{CalibrationTableDescriptor, CalibrationTableWriter, set_fixed_unit_keyword};

use super::{LegacyEpSolver, invalid, solve::Slot};

pub(super) fn write(ms: &MeasurementSet, solver: &LegacyEpSolver, slots: &[Slot]) -> MsResult<()> {
    let mut writer = CalibrationTableWriter::with_mode(
        ms,
        CalibrationTableDescriptor {
            output: solver.output_path(),
            schema: main_schema(),
            subtype: "EP Jones",
            parameter_type: None,
            measurement_set_name: String::new(),
            include_polarization_basis: false,
            time_extra_precision_column: Some("TIME_EXTRA_PREC"),
        },
        solver.mode,
    )
    .map_err(invalid)?;
    writer.preserve_row_order();
    *writer.table_mut().keywords_mut() = RecordValue::new(vec![
        RecordField::new("CAL_DESC", Value::table_ref("././CAL_DESC")),
        int("CAL_DESC_ID", 0),
        RecordField::new("CAL_HISTORY", Value::table_ref("././CAL_HISTORY")),
        int("CAL_HISTORY_ID", 0),
    ]);
    let table = writer.table_mut();
    set_frequency_reference(table, "REF_FREQUENCY");
    set_fixed_unit_keyword(table, "REF_DIRECTION", &["rad", "rad"]);
    set_variable_reference(
        table,
        "REF_DIRECTION",
        "direction",
        "MEAS_DIR_REF",
        direction_refs(),
    );
    let mut by_window = BTreeMap::<_, Vec<_>>::new();
    for slot in slots {
        by_window.entry(slot.spw).or_default().push(slot);
    }
    for (description, slots) in by_window.values().enumerate() {
        for slot in slots {
            for (antenna, offsets) in slot.offsets.iter().enumerate() {
                let mut fields = vec![
                    number("TIME", ScalarValue::Float64(slot.time)),
                    number("TIME_EXTRA_PREC", ScalarValue::Float64(0.)),
                    number("INTERVAL", ScalarValue::Float64(solver.interval_seconds)),
                    int("ANTENNA1", antenna as i32),
                    int("FIELD_ID", slot.field),
                    int("CAL_DESC_ID", description as i32),
                    number("TOTAL_SOLUTION_OK", ScalarValue::Bool(true)),
                    number("TOTAL_FIT", ScalarValue::Float32(1.)),
                    number("TOTAL_FIT_WEIGHT", ScalarValue::Float32(1.)),
                    array(
                        "GAIN",
                        ArrayValue::Float32(shaped(&[4, 1], offsets.to_vec())),
                    ),
                    array(
                        "SOLUTION_OK",
                        ArrayValue::Bool(shaped(&[4, 1], vec![true; 4])),
                    ),
                    array("FIT", ArrayValue::Float32(shaped(&[1, 1], vec![1.]))),
                    array("FIT_WEIGHT", ArrayValue::Float32(shaped(&[1, 1], vec![1.]))),
                    array("FLAG", ArrayValue::Bool(shaped(&[4, 1], vec![false; 4]))),
                    array("SNR", ArrayValue::Float32(shaped(&[4, 1], vec![0.; 4]))),
                ];
                fields.extend(
                    [
                        "FEED1",
                        "ARRAY_ID",
                        "OBSERVATION_ID",
                        "SCAN_NUMBER",
                        "PROCESSOR_ID",
                        "STATE_ID",
                        "PHASE_ID",
                        "PULSAR_BIN",
                        "PULSAR_GATE_ID",
                        "FREQ_GROUP",
                        "CALIBRATION_GROUP",
                        "MEAS_FREQ_REF",
                        "MEAS_DIR_REF",
                        "CAL_HISTORY_ID",
                    ]
                    .map(|name| int(name, 0)),
                );
                fields.extend(
                    [
                        "FREQ_GROUP_NAME",
                        "FIELD_NAME",
                        "FIELD_CODE",
                        "SOURCE_NAME",
                        "SOURCE_CODE",
                    ]
                    .map(|name| number(name, ScalarValue::String(String::new()))),
                );
                writer.append(RecordValue::new(fields)).map_err(invalid)?;
            }
        }
    }
    let desc = description(by_window.into_keys())?;
    desc.save(TableOptions::new(writer.output_path().join("CAL_DESC")))?;
    write_legacy_history(&writer.output_path().join("CAL_HISTORY"))?;
    writer.save_main().map_err(invalid)
}

fn main_schema() -> TableSchema {
    let scalar = ColumnSchema::scalar;
    let array = |name, kind| ColumnSchema::array_variable(name, kind, None);
    let mut columns = vec![
        scalar("TIME", Float64),
        scalar("TIME_EXTRA_PREC", Float64),
        scalar("INTERVAL", Float64),
    ];
    columns.extend(
        [
            "ANTENNA1",
            "FEED1",
            "FIELD_ID",
            "ARRAY_ID",
            "OBSERVATION_ID",
            "SCAN_NUMBER",
            "PROCESSOR_ID",
            "STATE_ID",
            "PHASE_ID",
            "PULSAR_BIN",
            "PULSAR_GATE_ID",
            "FREQ_GROUP",
        ]
        .map(|name| scalar(name, Int32)),
    );
    columns.extend(
        [
            "FREQ_GROUP_NAME",
            "FIELD_NAME",
            "FIELD_CODE",
            "SOURCE_NAME",
            "SOURCE_CODE",
        ]
        .map(|name| scalar(name, Text)),
    );
    columns.extend([
        scalar("CALIBRATION_GROUP", Int32),
        array("GAIN", Float32),
        array("REF_ANT", Int32),
        array("REF_FEED", Int32),
        array("REF_RECEPTOR", Int32),
        array("REF_FREQUENCY", Float64),
        scalar("MEAS_FREQ_REF", Int32),
        array("REF_DIRECTION", Float64),
        scalar("MEAS_DIR_REF", Int32),
        scalar("CAL_DESC_ID", Int32),
        scalar("CAL_HISTORY_ID", Int32),
        scalar("TOTAL_SOLUTION_OK", Bool),
        scalar("TOTAL_FIT", Float32),
        scalar("TOTAL_FIT_WEIGHT", Float32),
        array("SOLUTION_OK", Bool),
        array("FIT", Float32),
        array("FIT_WEIGHT", Float32),
        array("FLAG", Bool),
        array("SNR", Float32),
    ]);
    TableSchema::new(columns).expect("legacy EP table schema")
}

fn description(windows: impl Iterator<Item = i32>) -> MsResult<Table> {
    let scalar = ColumnSchema::scalar;
    let array_column = |name, kind| ColumnSchema::array_variable(name, kind, None);
    let schema = TableSchema::new(vec![
        scalar("NUM_SPW", Int32),
        array_column("NUM_CHAN", Int32),
        scalar("NUM_RECEPTORS", Int32),
        scalar("N_JONES", Int32),
        array_column("SPECTRAL_WINDOW_ID", Int32),
        array_column("CHAN_FREQ", Float64),
        scalar("MEAS_FREQ_REF", Int32),
        array_column("CHAN_WIDTH", Float64),
        array_column("CHAN_RANGE", Int32),
        array_column("POLARIZATION_TYPE", Text),
        scalar("JONES_TYPE", Text),
        scalar("MS_NAME", Text),
    ])
    .expect("legacy EP CAL_DESC schema");
    let mut table = Table::with_schema(schema);
    set_frequency_reference(&mut table, "CHAN_FREQ");
    set_fixed_unit_keyword(&mut table, "CHAN_WIDTH", &["Hz"]);
    for spw in windows {
        table.add_row(RecordValue::new(vec![
            int("NUM_SPW", 1),
            array("NUM_CHAN", ArrayValue::from_i32_vec(vec![1])),
            int("NUM_RECEPTORS", 2),
            int("N_JONES", 2),
            array("SPECTRAL_WINDOW_ID", ArrayValue::from_i32_vec(vec![spw])),
            int("MEAS_FREQ_REF", 0),
            array("CHAN_FREQ", ArrayValue::Float64(shaped(&[1, 1], vec![0.]))),
            array("CHAN_WIDTH", ArrayValue::Float64(shaped(&[1, 1], vec![0.]))),
            array(
                "CHAN_RANGE",
                ArrayValue::Int32(shaped(&[2, 1, 1], vec![0, 0])),
            ),
            array(
                "POLARIZATION_TYPE",
                ArrayValue::String(shaped(&[1, 1], vec![String::new()])),
            ),
            number("JONES_TYPE", ScalarValue::String("full".into())),
            number("MS_NAME", ScalarValue::String(".".into())),
        ]))?;
    }
    Ok(table)
}

fn set_frequency_reference(table: &mut Table, name: &str) {
    set_fixed_unit_keyword(table, name, &["Hz"]);
    let refs = FrequencyRef::ALL
        .into_iter()
        .chain([FrequencyRef::Undefined])
        .map(|reference| (reference.casacore_code() as u32, reference.as_str()));
    set_variable_reference(table, name, "frequency", "MEAS_FREQ_REF", refs);
}

fn direction_refs() -> Vec<(u32, &'static str)> {
    let mut references = Vec::new();
    for code in (0..=21).chain(32..=42) {
        let reference = DirectionRef::from_casacore_code(code).expect("CASA direction reference");
        references.push((code as u32, reference.as_str()));
        if code == 11 {
            references.push((10, "AZELNE"));
        } else if code == 13 {
            references.push((12, "AZELNEGEO"));
        }
    }
    references
}

fn set_variable_reference(
    table: &mut Table,
    name: &str,
    kind: &str,
    reference_column: &str,
    references: impl IntoIterator<Item = (u32, &'static str)>,
) {
    let (codes, names) = references
        .into_iter()
        .map(|(code, name)| (code, name.to_owned()))
        .unzip();
    let mut keywords = table.column_keywords(name).cloned().unwrap_or_default();
    keywords.upsert(
        "MEASINFO",
        Value::Record(RecordValue::new(vec![
            array("TabRefCodes", ArrayValue::from_u32_vec(codes)),
            array("TabRefTypes", ArrayValue::from_string_vec(names)),
            number("VarRefCol", ScalarValue::String(reference_column.into())),
            number("type", ScalarValue::String(kind.into())),
        ])),
    );
    table.set_column_keywords(name, keywords);
}

fn shaped<T>(shape: &[usize], values: Vec<T>) -> ArrayD<T> {
    ArrayD::from_shape_vec(IxDyn(shape).f(), values).expect("legacy EP cell extent")
}

fn int(name: &str, value: i32) -> RecordField {
    number(name, ScalarValue::Int32(value))
}

fn number(name: &str, value: ScalarValue) -> RecordField {
    RecordField::new(name, Value::Scalar(value))
}

fn array(name: &str, value: ArrayValue) -> RecordField {
    RecordField::new(name, Value::Array(value))
}
