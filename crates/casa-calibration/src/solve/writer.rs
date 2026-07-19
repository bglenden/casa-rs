// SPDX-License-Identifier: LGPL-3.0-or-later
//! On-disk caltable writing for limited `gaincal`.

use std::collections::BTreeSet;
use std::path::Path;

use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{GainSolveError, GainSolveReport, GainSolveRequest};
use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_ARRAY_ID, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
    COL_TIME_EXTRA_PREC, COL_WEIGHT,
};
use crate::solve::kernel::SolutionRow;
use crate::writer::{CalibrationTableDescriptor, CalibrationTableWriter};

pub(crate) fn write_gain_caltable(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    refant_id: i32,
    rows: &[SolutionRow],
) -> Result<GainSolveReport, GainSolveError> {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ARRAY_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_TIME_EXTRA_PREC, casa_types::PrimitiveType::Float64),
        ColumnSchema::array_variable(COL_CPARAM, casa_types::PrimitiveType::Complex32, Some(2)),
        ColumnSchema::array_variable(COL_PARAMERR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_FLAG, casa_types::PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable(COL_SNR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_WEIGHT, casa_types::PrimitiveType::Float32, Some(2)),
    ])
    .expect("valid gain caltable schema");
    let mut writer = CalibrationTableWriter::create(
        ms,
        CalibrationTableDescriptor {
            output: &request.output_table,
            schema,
            subtype: request.gain_type.vis_cal(),
            parameter_type: Some("Complex"),
            measurement_set_name: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: Some(COL_TIME_EXTRA_PREC),
        },
    )?;

    for row in rows {
        writer.append(RecordValue::new(vec![
            RecordField::new(
                COL_TIME,
                Value::Scalar(ScalarValue::Float64(row.time_seconds)),
            ),
            RecordField::new(
                COL_FIELD_ID,
                Value::Scalar(ScalarValue::Int32(row.field_id)),
            ),
            RecordField::new(
                COL_SPECTRAL_WINDOW_ID,
                Value::Scalar(ScalarValue::Int32(row.spw_id)),
            ),
            RecordField::new(
                COL_ANTENNA1,
                Value::Scalar(ScalarValue::Int32(row.antenna_id)),
            ),
            RecordField::new(
                COL_ANTENNA2,
                Value::Scalar(ScalarValue::Int32(row.refant_id)),
            ),
            RecordField::new(
                COL_INTERVAL,
                Value::Scalar(ScalarValue::Float64(row.interval_seconds)),
            ),
            RecordField::new(
                COL_SCAN_NUMBER,
                Value::Scalar(ScalarValue::Int32(row.scan_number)),
            ),
            RecordField::new(
                COL_OBSERVATION_ID,
                Value::Scalar(ScalarValue::Int32(row.observation_id)),
            ),
            RecordField::new(COL_ARRAY_ID, Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                COL_TIME_EXTRA_PREC,
                Value::Scalar(ScalarValue::Float64(0.0)),
            ),
            RecordField::new(
                COL_CPARAM,
                Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(IxDyn(&[row.gains.len(), 1]).f(), row.gains.clone())
                        .expect("gain vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_PARAMERR,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[row.gains.len(), 1]).f(),
                        row.param_errors.clone(),
                    )
                    .expect("paramerr vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_FLAG,
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&[row.flags.len(), 1]).f(), row.flags.clone())
                        .expect("flag vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_SNR,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[row.gains.len(), 1]).f(), row.snrs.clone())
                        .expect("snr vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_WEIGHT,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[row.gains.len(), 1]).f(), row.weights.clone())
                        .expect("weight vector should reshape to receptor x channel"),
                )),
            ),
        ]))?;
    }

    writer.copy_subtables(&[
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
    ])?;
    write_gain_spectral_window_subtable(ms, writer.output_path(), rows)?;
    writer.save_main()?;

    let field_ids = rows.iter().map(|row| row.field_id).collect::<BTreeSet<_>>();
    let spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    Ok(GainSolveReport {
        output_table: request.output_table.clone(),
        gain_type: request.gain_type,
        refant_antenna_id: refant_id,
        field_ids: field_ids.iter().copied().collect(),
        spectral_window_ids: spw_ids.iter().copied().collect(),
        solution_row_count: rows.len(),
    })
}

fn write_gain_spectral_window_subtable(
    ms: &MeasurementSet,
    output_root: &Path,
    rows: &[SolutionRow],
) -> Result<(), GainSolveError> {
    let destination = output_root.join("SPECTRAL_WINDOW");
    ms.subtable(SubtableId::SpectralWindow)
        .expect("required subtable available")
        .save(TableOptions::new(&destination))
        .map_err(|source| GainSolveError::CopySubtable {
            subtable: "SPECTRAL_WINDOW".to_string(),
            path: output_root.display().to_string(),
            source: Box::new(source),
        })?;

    let solved_spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    let mut table = Table::open(TableOptions::new(&destination)).map_err(|source| {
        GainSolveError::CopySubtable {
            subtable: "SPECTRAL_WINDOW".to_string(),
            path: output_root.display().to_string(),
            source: Box::new(source),
        }
    })?;

    for spw_id in solved_spw_ids {
        let row =
            usize::try_from(spw_id).map_err(|_| GainSolveError::UnsupportedParameterShape {
                path: "SPECTRAL_WINDOW row id".to_string(),
                shape: vec![spw_id as usize],
            })?;
        let chan_freq = get_f64_array(&table, row, "CHAN_FREQ")?;
        let chan_width = get_f64_array(&table, row, "CHAN_WIDTH")?;
        let effective_bw = get_f64_array(&table, row, "EFFECTIVE_BW")?;
        let resolution = get_f64_array(&table, row, "RESOLUTION")?;

        if chan_freq.len() <= 1 {
            continue;
        }

        let center_index = chan_freq.len() / 2;
        table
            .cell_accessor_mut(row, "NUM_CHAN")
            .and_then(|mut cell| cell.set(Value::Scalar(ScalarValue::Int32(1))))
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
        table
            .cell_accessor_mut(row, "CHAN_FREQ")
            .and_then(|mut cell| cell.set(Value::Array(f64_array(&[chan_freq[center_index]]))))
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
        table
            .cell_accessor_mut(row, "CHAN_WIDTH")
            .and_then(|mut cell| {
                cell.set(Value::Array(f64_array(&[chan_width.iter().copied().sum()])))
            })
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
        table
            .cell_accessor_mut(row, "EFFECTIVE_BW")
            .and_then(|mut cell| {
                cell.set(Value::Array(f64_array(&[effective_bw
                    .iter()
                    .copied()
                    .sum()])))
            })
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
        table
            .cell_accessor_mut(row, "RESOLUTION")
            .and_then(|mut cell| {
                cell.set(Value::Array(f64_array(&[resolution.iter().copied().sum()])))
            })
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
    }

    table
        .save(TableOptions::new(&destination))
        .map_err(|source| GainSolveError::CopySubtable {
            subtable: "SPECTRAL_WINDOW".to_string(),
            path: output_root.display().to_string(),
            source: Box::new(source),
        })?;

    Ok(())
}

fn get_f64_array(table: &Table, row: usize, column: &str) -> Result<Vec<f64>, GainSolveError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(MsError::from)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: column.to_string(),
            source,
        })? {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        other => Err(GainSolveError::UnsupportedParameterShape {
            path: format!("{column} array"),
            shape: other.shape().to_vec(),
        }),
    }
}

fn f64_array(values: &[f64]) -> ArrayValue {
    ArrayValue::Float64(
        ArrayD::from_shape_vec(IxDyn(&[values.len()]).f(), values.to_vec())
            .expect("f64 vector should reshape to 1-D array"),
    )
}
