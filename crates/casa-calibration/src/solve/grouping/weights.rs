// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection of CASA's scalar and per-channel solve weights.

use std::collections::{HashMap, HashSet};

use casa_ms::MeasurementSet;
use casa_tables::{TableColumn, TableError};
use casa_types::ArrayValue;
use ndarray::{ArrayD, Axis};

use super::{
    BucketedRow, GainSolveError, SelectedSolveRow, SolveInputBatch, input_cell, ms_read_error,
    unsupported_shape,
};

/// The spectral columns are in use for a visibility buffer when both have
/// a defined cell in its first selected row (`resetWeightsUsingSigma`).
#[derive(Default)]
pub(super) struct SpectralWeights(HashSet<VisibilityBuffer>);

impl SpectralWeights {
    pub(super) fn new(
        ms: &MeasurementSet,
        rows: &[BucketedRow<'_>],
    ) -> Result<Self, GainSolveError> {
        let table = ms.main_table();
        let available = |name| {
            table
                .schema()
                .is_some_and(|schema| schema.contains_column(name))
        };
        if !available("WEIGHT_SPECTRUM") || !available("SIGMA_SPECTRUM") {
            return Ok(Self::default());
        }
        let weight = table
            .column_accessor("WEIGHT_SPECTRUM")
            .map_err(|source| ms_read_error(ms, source))?;
        let sigma = table
            .column_accessor("SIGMA_SPECTRUM")
            .map_err(|source| ms_read_error(ms, source))?;
        let mut first_rows = HashMap::<VisibilityBuffer, usize>::new();
        for (row, _, _) in rows {
            first_rows
                .entry((*row).into())
                .and_modify(|first| *first = (*first).min(row.row_index))
                .or_insert(row.row_index);
        }
        let mut first_rows = first_rows.into_iter().collect::<Vec<_>>();
        first_rows.sort_unstable_by_key(|&(_, row)| row);
        let mut spectral = HashSet::new();
        for block in first_rows.chunks(256) {
            let indices = block.iter().map(|&(_, row)| row).collect::<Vec<_>>();
            let weights =
                defined_cells(&weight, &indices).map_err(|source| ms_read_error(ms, source))?;
            let sigmas =
                defined_cells(&sigma, &indices).map_err(|source| ms_read_error(ms, source))?;
            spectral.extend(
                block
                    .iter()
                    .zip(weights.into_iter().zip(sigmas))
                    .filter_map(|(&(buffer, _), (weight, sigma))| {
                        (weight && sigma).then_some(buffer)
                    }),
            );
        }
        Ok(Self(spectral))
    }

    pub(super) fn contains(&self, row: &SelectedSolveRow) -> bool {
        self.0.contains(&row.into())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct VisibilityBuffer {
    observation: i32,
    field: i32,
    scan: i32,
    data_description: i32,
    time: u64,
}

impl From<&SelectedSolveRow> for VisibilityBuffer {
    fn from(row: &SelectedSolveRow) -> Self {
        Self {
            observation: row.observation_id,
            field: row.field_id,
            scan: row.scan_number,
            data_description: row.data_desc_id,
            time: row.time_seconds.to_bits(),
        }
    }
}

fn defined_cells(column: &TableColumn<'_>, rows: &[usize]) -> Result<Vec<bool>, TableError> {
    if let Ok(shapes) = column.array_shapes_uncached(rows) {
        return Ok(shapes.into_iter().map(|shape| shape.is_some()).collect());
    }
    Ok(column
        .array_cells_owned_uncached(rows)?
        .into_iter()
        .map(|cell| cell.is_some())
        .collect())
}

pub(super) fn solve_weights(
    ms: &MeasurementSet,
    row_index: usize,
    shape: &[usize],
    inputs: (&SolveInputBatch, usize),
) -> Result<ArrayD<f32>, GainSolveError> {
    let inverse_square = |sigma: f32| {
        if sigma > 0.0 {
            1.0 / (sigma * sigma)
        } else {
            0.0
        }
    };
    if inputs.0.spectral_weights[inputs.1] {
        let sigma = input_cell(ms, row_index, inputs, "SIGMA_SPECTRUM")?;
        return match sigma {
            ArrayValue::Float32(sigma) if sigma.shape() == shape => Ok(sigma.mapv(inverse_square)),
            other => Err(unsupported_shape("SIGMA_SPECTRUM", other)),
        };
    }
    let sigma = input_cell(ms, row_index, inputs, "SIGMA")?;
    let ArrayValue::Float32(sigma) = sigma else {
        return Err(unsupported_shape("SIGMA", sigma));
    };
    if sigma.ndim() != 1 || sigma.shape()[0] != shape[0] {
        return Err(GainSolveError::UnsupportedParameterShape {
            path: "<measurement-set SIGMA>".to_string(),
            shape: sigma.shape().to_vec(),
        });
    }
    Ok(sigma.mapv(inverse_square).insert_axis(Axis(1)))
}
