// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selection of CASA's scalar and per-channel solve weights.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use casa_core::ArrayValue;
use casa_ms::MeasurementSet;
use casa_tables::{TableColumn, TableError};
use ndarray::{ArrayD, Axis};

use super::{
    BucketedRow, GainSolveCombine, GainSolveError, SolveBucketKey, SolveInputBatch, input_cell,
    ms_read_error, unsupported_shape,
};

/// Spectral content in the CASA subchunks consumed by the solve.
#[derive(Default)]
pub(super) struct SpectralWeights {
    enabled: HashSet<VisibilityBuffer>,
    combine: GainSolveCombine,
}

impl SpectralWeights {
    pub(super) fn new(
        ms: &MeasurementSet,
        rows: &[BucketedRow<'_>],
        combine: GainSolveCombine,
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
        let weight_column = table
            .column_accessor("WEIGHT_SPECTRUM")
            .map_err(|source| ms_read_error(ms, source))?;
        let sigma_column = table
            .column_accessor("SIGMA_SPECTRUM")
            .map_err(|source| ms_read_error(ms, source))?;
        let mut first_rows = HashMap::<VisibilityBuffer, (usize, i32)>::new();
        for row in rows {
            let value = (row.0.row_index, row.0.data_spw_id);
            first_rows
                .entry(VisibilityBuffer::new(row, combine))
                .and_modify(|first| *first = (*first).min(value))
                .or_insert(value);
        }
        let mut first_rows = first_rows.into_iter().collect::<Vec<_>>();
        first_rows.sort_unstable_by_key(|&(buffer, _)| buffer);
        let first_chunk = first_rows.first().map(|(buffer, _)| buffer.chunk);
        let mut enabled = HashSet::new();
        let mut cache = SpectralColumnCache::default();
        for block in first_rows.chunks(256) {
            let indices = block.iter().map(|&(_, (row, _))| row).collect::<Vec<_>>();
            let weights = defined_cells(&weight_column, &indices)
                .map_err(|source| ms_read_error(ms, source))?;
            let sigmas = defined_cells(&sigma_column, &indices)
                .map_err(|source| ms_read_error(ms, source))?;
            for (&(buffer, (_, spw)), (weight, sigma)) in
                block.iter().zip(weights.into_iter().zip(sigmas))
            {
                cache.visit(
                    buffer.chunk,
                    spw,
                    Some(buffer.chunk) == first_chunk,
                    weight,
                    sigma,
                );
                if cache.weight_available && cache.sigma_available {
                    enabled.insert(buffer);
                }
            }
        }
        Ok(Self { enabled, combine })
    }

    pub(super) fn contains(&self, row: &BucketedRow<'_>) -> bool {
        !self.enabled.is_empty()
            && self
                .enabled
                .contains(&VisibilityBuffer::new(row, self.combine))
    }
}

/// CASA's `newSpectralWindow()` and `newMS()` flags are properties of the
/// outer chunk. Both availability probes use the current subchunk's first row.
#[derive(Default)]
struct SpectralColumnCache {
    chunk: Option<VisibilityChunk>,
    spw: Option<i32>,
    new_spw: bool,
    weight_available: bool,
    sigma_available: bool,
}

impl SpectralColumnCache {
    fn visit(&mut self, chunk: VisibilityChunk, spw: i32, new_ms: bool, weight: bool, sigma: bool) {
        if self.chunk != Some(chunk) {
            self.new_spw = self.spw != Some(spw);
            self.chunk = Some(chunk);
            self.spw = Some(spw);
        }
        if self.new_spw {
            self.weight_available = weight;
        }
        if new_ms && self.weight_available {
            self.sigma_available = sigma;
        }
    }
}

/// `CalSolVi2Organizer::deriveVI2Sort` orders outer chunks over these axes.
/// Array is always a boundary; combined observations/scans are omitted.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct VisibilityChunk {
    array: i32,
    observation: i32,
    scan: i32,
    field: i32,
    data_description: i32,
    time_bucket: SolveBucketKey,
    combined_field: i32,
    combined_data_description: i32,
}

/// The subchunk groups rows at the same exact time inside the outer chunk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::solve) struct VisibilityBuffer {
    chunk: VisibilityChunk,
    time: u64,
}

impl VisibilityBuffer {
    pub(in crate::solve) fn new(
        &(row, base, bucket): &BucketedRow<'_>,
        combine: GainSolveCombine,
    ) -> Self {
        Self {
            chunk: VisibilityChunk {
                array: row.array_id,
                observation: base.observation_id,
                scan: base.scan_number,
                field: base.field_id,
                data_description: if combine.spectral_windows {
                    0
                } else {
                    row.data_desc_id
                },
                time_bucket: bucket,
                combined_field: if combine.fields { row.field_id } else { 0 },
                combined_data_description: if combine.spectral_windows {
                    row.data_desc_id
                } else {
                    0
                },
            },
            time: row.time_seconds.to_bits(),
        }
    }
}

/// MSIter evaluates a moving FIELD direction at its chunk's first timestamp.
pub(super) fn chunk_phase_times(
    rows: &[BucketedRow<'_>],
    combine: GainSolveCombine,
    fields: &BTreeSet<i32>,
) -> BTreeMap<usize, f64> {
    if fields.is_empty() {
        return BTreeMap::new();
    }
    let mut first = HashMap::<VisibilityChunk, f64>::new();
    let changing = || {
        rows.iter()
            .filter(|(row, _, _)| fields.contains(&row.field_id))
    };
    for row in changing() {
        let time = row.0.time_seconds;
        first
            .entry(VisibilityBuffer::new(row, combine).chunk)
            .and_modify(|first| *first = first.min(time))
            .or_insert(time);
    }
    changing()
        .map(|row| {
            (
                row.0.row_index,
                first[&VisibilityBuffer::new(row, combine).chunk],
            )
        })
        .collect()
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
