// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pulse-calibration NewCalTable publication.

use std::path::Path;

use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_tables::TableWriteLock;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::PccorError;
use crate::table_maintenance::prepare_empty_calibration_table;
use crate::writer::SingleChannelFrequency;

pub(super) struct Row {
    pub(super) antenna: i32,
    pub(super) window: i32,
    pub(super) time: f64,
    pub(super) field: i32,
    pub(super) scan: i32,
    pub(super) phase: [f64; 2],
    pub(super) delay: [f64; 2],
}

pub(super) fn write(
    ms: &MeasurementSet,
    destination: &Path,
    reference: i32,
    mut rows: Vec<Row>,
) -> Result<(), PccorError> {
    rows.sort_by(|first, second| {
        first
            .time
            .total_cmp(&second.time)
            .then_with(|| (first.window, first.antenna).cmp(&(second.window, second.antenna)))
    });
    let mut writer = prepare_empty_calibration_table(
        ms,
        destination,
        "Float",
        "Fringe Jones",
        Some(SingleChannelFrequency::MiddleChannel),
    )
    .map_err(PccorError::Execution)?;
    // PCCOR writes time, SPW, antenna order into the empty NewCalTable.
    writer.preserve_row_order();
    let scalar = |name, number| RecordField::new(name, Value::Scalar(ScalarValue::Int32(number)));
    let double = |name, number| RecordField::new(name, Value::Scalar(ScalarValue::Float64(number)));
    let array =
        |name, values| RecordField::new(name, Value::Array(ArrayValue::Float32(shaped(values))));
    for row in rows {
        let mut parameters = [0_f32; 8];
        for (hand, (phase, delay)) in row.phase.into_iter().zip(row.delay).enumerate() {
            parameters[4 * hand] = phase as f32;
            parameters[4 * hand + 1] = (delay * 1e9) as f32;
        }
        writer
            .append(RecordValue::new(vec![
                double("TIME", row.time),
                scalar("FIELD_ID", row.field),
                scalar("SPECTRAL_WINDOW_ID", row.window),
                scalar("ANTENNA1", row.antenna),
                scalar("ANTENNA2", reference),
                double("INTERVAL", 0.),
                scalar("SCAN_NUMBER", row.scan),
                scalar("OBSERVATION_ID", 0),
                array("FPARAM", parameters),
                RecordField::new("FLAG", Value::Array(ArrayValue::Bool(shaped([false; 8])))),
                array("SNR", [999.; 8]),
                array("WEIGHT", [1.; 8]),
            ]))
            .map_err(|error| PccorError::Execution(error.to_string()))?;
    }
    let _lock = destination
        .exists()
        .then(|| TableWriteLock::acquire_to_reread(destination, 1))
        .transpose()
        .map_err(|error| PccorError::Execution(error.to_string()))?;
    writer
        .save_main()
        .map_err(|error| PccorError::Execution(error.to_string()))
}

fn shaped<T>(values: [T; 8]) -> ArrayD<T> {
    ArrayD::from_shape_vec(IxDyn(&[8, 1]).f(), Vec::from(values)).expect("eight fringe parameters")
}
