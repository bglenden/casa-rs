// SPDX-License-Identifier: LGPL-3.0-or-later
//! Packed visibility and weight input batches shared by calibration solvers.

use casa_ms::MeasurementSet;
use casa_types::ArrayValue;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{GainSolveError, main_cell, ms_read_error};

/// Array payloads live for one batch, without filling the table's row cache.
pub(crate) struct SolveInputBatch {
    pub(super) columns: Vec<(&'static str, Vec<Option<ArrayValue>>)>,
    pub(super) spectral_weights: Vec<bool>,
}

impl SolveInputBatch {
    pub(super) fn polynomial_bin_weights(&self, index: usize) -> Option<&ArrayValue> {
        self.columns
            .iter()
            .find(|(column, _)| *column == "WEIGHT_SPECTRUM")
            .and_then(|(_, rows)| rows[index].as_ref())
    }
}

/// Read selected arrays together so headers and tiles are shared by a batch;
/// release the payloads after accumulation instead of retaining the whole MS.
pub(super) fn read_input_batch(
    ms: &MeasurementSet,
    rows: &[usize],
    (read_visibilities, read_model_flags): (bool, bool),
    read_model_column: bool,
    spectral_weights: Vec<bool>,
    read_polynomial_bin_weights: bool,
) -> Result<SolveInputBatch, GainSolveError> {
    let schema = ms.main_table().schema();
    let mut columns = Vec::new();
    if spectral_weights.contains(&true) {
        columns.push("SIGMA_SPECTRUM");
    }
    if spectral_weights.contains(&false) {
        columns.push("SIGMA");
    }
    if read_polynomial_bin_weights
        && schema.is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"))
    {
        columns.push("WEIGHT_SPECTRUM");
    }
    if read_visibilities {
        columns.push("DATA");
    }
    if read_visibilities || read_model_flags {
        columns.push("FLAG");
    }
    if read_model_column {
        columns.push("MODEL_DATA");
    }
    let mut inputs = SolveInputBatch {
        columns: Vec::with_capacity(columns.len()),
        spectral_weights,
    };
    for column in columns {
        let values = ms
            .main_table()
            .column_accessor(column)
            .and_then(|accessor| {
                read_column_inputs(
                    &accessor,
                    column,
                    schema
                        .and_then(|s| s.column(column))
                        .and_then(|c| c.data_type()),
                    rows,
                )
            })
            .map_err(|source| ms_read_error(ms, source))?;
        inputs.columns.push((column, values));
    }
    Ok(inputs)
}

/// Homogeneous cells use the typed selected reader, avoiding reconstruction
/// of complete tiled cubes. Mixed shapes and pending edits use the ordinary
/// owned-cell reader, preserving their existing validation and semantics.
fn read_column_inputs(
    column: &casa_tables::TableColumn<'_>,
    name: &str,
    kind: Option<casa_types::PrimitiveType>,
    rows: &[usize],
) -> Result<Vec<Option<ArrayValue>>, casa_tables::TableError> {
    if name != "SIGMA"
        && let Ok(shapes) = column.array_shapes_uncached(rows)
        && let Some(Some(shape)) = shapes.first()
        && shape.len() == 2
        && shape.iter().all(|n| *n > 0)
        && shapes.iter().all(|other| other.as_ref() == Some(shape))
    {
        use casa_tables::SelectedArray2DCellsMut;
        use casa_types::PrimitiveType;
        let values = match kind {
            Some(PrimitiveType::Complex32) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelComplex32(values),
                ArrayValue::Complex32,
            )?,
            Some(PrimitiveType::Bool) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelBool(values),
                ArrayValue::Bool,
            )?,
            Some(PrimitiveType::Float32) => read_row_packed(
                column,
                rows,
                shape[1],
                |values| SelectedArray2DCellsMut::RowChannelFloat32(values),
                ArrayValue::Float32,
            )?,
            _ => None,
        };
        if let Some(values) = values {
            return Ok(values);
        }
    }
    column.array_cells_owned_uncached(rows)
}

/// Request contiguous row cells directly, avoiding a channel/row transpose.
fn read_row_packed<T: Clone>(
    column: &casa_tables::TableColumn<'_>,
    rows: &[usize],
    channels: usize,
    destination: for<'a> fn(&'a mut Vec<T>) -> casa_tables::SelectedArray2DCellsMut<'a>,
    wrap: fn(ArrayD<T>) -> ArrayValue,
) -> Result<Option<Vec<Option<ArrayValue>>>, casa_tables::TableError> {
    let mut values = Vec::new();
    let Some(shape) = column.fill_array_cells_2d_channel_range_typed_uncached(
        rows,
        0,
        channels,
        destination(&mut values),
    )?
    else {
        return Ok(None);
    };
    let shape = [shape.axis0_count, shape.channel_count];
    let capacity = shape[0].checked_mul(shape[1]).ok_or_else(|| {
        casa_tables::TableError::Storage("solve input cell size overflows".to_string())
    })?;
    values
        .chunks_exact(capacity)
        .map(|row| {
            ArrayD::from_shape_vec(IxDyn(&shape).f(), row.to_vec())
                .map(|array| Some(wrap(array)))
                .map_err(|e| casa_tables::TableError::Storage(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(super) fn input_cell<'a>(
    ms: &'a MeasurementSet,
    row: usize,
    inputs: (&'a SolveInputBatch, usize),
    column: &str,
) -> Result<&'a ArrayValue, GainSolveError> {
    if let Some((_, values)) = inputs.0.columns.iter().find(|(name, _)| *name == column) {
        return values[inputs.1].as_ref().ok_or_else(|| {
            ms_read_error(
                ms,
                casa_tables::TableError::ColumnNotFound {
                    row_index: row,
                    column: column.to_string(),
                },
            )
        });
    }
    main_cell(ms, row, column)
}
