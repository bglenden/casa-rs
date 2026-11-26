// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA calibration table output for known-source leakage fits.

use super::{LeakageSolution, LeakageSolveKind, LeakageSolveRequest, SelectedSolveRow};
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::{MeasurementSet, schema::SubtableId};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use std::collections::BTreeMap;

use crate::CalibrationOutputMode;
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriter, new_calibration_table_schema,
};

pub(super) fn write(
    ms: &MeasurementSet,
    request: &LeakageSolveRequest,
    selected_rows: &[SelectedSolveRow],
    solutions: &[LeakageSolution],
    channel_bin_widths: BTreeMap<i32, usize>,
) -> Result<usize, String> {
    if solutions.is_empty() {
        return Ok(0);
    }
    let complex = request.kind != LeakageSolveKind::PosAng;
    let mut writer = CalibrationTableWriter::with_mode(
        ms,
        CalibrationTableDescriptor {
            output: &request.output,
            schema: new_calibration_table_schema(complex),
            subtype: request.kind.subtype(),
            parameter_type: Some(if complex { "Complex" } else { "Float" }),
            measurement_set_name: ms
                .path()
                .map_or_else(|| "<memory>".into(), |path| path.display().to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        if request.append {
            CalibrationOutputMode::Append
        } else {
            CalibrationOutputMode::Replace
        },
    )
    .map_err(|error| error.to_string())?;
    let mut written = 0;
    let receptors = request.kind.parameter_count();
    for interval in solutions {
        let antennas = interval.values.first().map_or(0, Vec::len);
        let baseline = request.kind == LeakageSolveKind::XMueller;
        for antenna in 0..antennas {
            let seconds = if baseline { antenna..antennas } else { 0..1 };
            for second in seconds {
                let reference = if baseline {
                    second as i32
                } else {
                    interval.reference
                };
                writer
                    .append(solution_row(
                        interval, antenna, reference, receptors, complex,
                    ))
                    .map_err(|error| error.to_string())?;
                written += 1;
            }
        }
    }
    if request.kind.parallactic() {
        let windows = ms
            .spectral_window()
            .map_err(|error| error.to_string())?
            .row_count();
        let mut qu = ArrayD::zeros(IxDyn(&[2, windows]).f());
        for solution in solutions {
            if let Some(source) = solution.polarization {
                let spw = solution.spectral_window as usize;
                qu[[0, spw]] = source.re;
                qu[[1, spw]] = source.im;
            }
        }
        let record = RecordValue::new(vec![RecordField::new(
            "QU",
            Value::Array(ArrayValue::Float32(qu)),
        )]);
        writer
            .table_mut()
            .keywords_mut()
            .upsert("QU", Value::Record(record));
    }
    writer
        .copy_subtables(&[
            (SubtableId::Antenna, "ANTENNA"),
            (SubtableId::Field, "FIELD"),
            (SubtableId::History, "HISTORY"),
            (SubtableId::Observation, "OBSERVATION"),
            (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
        ])
        .map_err(|error| error.to_string())?;
    let windows = solutions
        .iter()
        .map(|interval| interval.spectral_window)
        .collect();
    crate::bandpass::write_solution_spectral_windows(
        &writer.output_path().join("SPECTRAL_WINDOW"),
        &windows,
        &channel_bin_widths,
        selected_rows,
        request.combine.spectral_windows,
    )
    .map_err(|error| error.to_string())?;
    writer.save_main().map_err(|error| error.to_string())?;
    Ok(written)
}

fn solution_row(
    interval: &LeakageSolution,
    antenna: usize,
    reference: i32,
    receptors: usize,
    complex: bool,
) -> RecordValue {
    let shape = IxDyn(&[receptors, interval.values.len()]).f();
    let parameter = |index: IxDyn| interval.values[index[1]][antenna].gain[index[0]];
    let (column, values) = if complex {
        (
            "CPARAM",
            ArrayValue::Complex32(ArrayD::from_shape_fn(shape.clone(), parameter)),
        )
    } else {
        (
            "FPARAM",
            ArrayValue::Float32(ArrayD::from_shape_fn(shape.clone(), |index| {
                parameter(index).re
            })),
        )
    };
    let flags = ArrayD::from_shape_fn(shape.clone(), |index| {
        !interval.values[index[1]][antenna].valid[index[0]]
    });
    let errors = ArrayD::from_shape_fn(shape.clone(), |index| {
        interval.values[index[1]][antenna].error[index[0]]
    });
    let snr = ArrayD::from_shape_fn(shape, |index| {
        interval.values[index[1]][antenna].snr[index[0]]
    });
    let integer = |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)));
    let float = |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Float64(value)));
    let array = |name, value| RecordField::new(name, Value::Array(value));
    RecordValue::new(vec![
        float("TIME", interval.time),
        float("INTERVAL", 0.0),
        integer("FIELD_ID", interval.field),
        integer("OBSERVATION_ID", interval.observation),
        integer("SPECTRAL_WINDOW_ID", interval.spectral_window),
        integer("SCAN_NUMBER", interval.scan),
        integer("ANTENNA1", antenna as i32),
        integer("ANTENNA2", reference),
        array(column, values),
        array("FLAG", ArrayValue::Bool(flags)),
        array("PARAMERR", ArrayValue::Float32(errors)),
        array("SNR", ArrayValue::Float32(snr)),
    ])
}
