// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded, channel-projected reads for the unweighted complex reduction.

use casa_tables::{SelectedArray2DCellsMut, SelectedArray2DShape, Table};
use num_complex::Complex32;

use super::cube::{CubeReduction, SampleAxes};
use super::reduction::Moments;
use super::rows::Row;
use super::spec::{Axis, Column, Weighting};
use casa_ms::{MsError, MsReadPlan, MsResult, MsSelectionIoBudget};

/// Return the number of integration buffers consumed, or `None` when
/// the storage/selection needs the general cell reader.
pub(super) fn reduce(
    reduction: &CubeReduction,
    table: &Table,
    rows: &[Row],
    axes: &SampleAxes,
    column: Column,
    first_dataset: usize,
    moments: &mut Moments,
) -> MsResult<Option<usize>> {
    let Column::Complex(name, component) = column else {
        return Ok(None);
    };
    if reduction.legacy || reduction.weighting != Weighting::Unweighted {
        return Ok(None);
    }
    let Some(&first_channel) = axes.channels.first() else {
        return Ok(None);
    };
    let channel_count = axes.channels.len();
    if !axes
        .channels
        .iter()
        .copied()
        .eq(first_channel..first_channel + channel_count)
    {
        return Ok(None);
    }
    let bytes_per_row = channel_count
        .saturating_mul(axes.stored_correlations)
        .saturating_mul(size_of::<Complex32>() + usize::from(reduction.use_flags))
        .saturating_add(size_of::<usize>());
    let budget = MsSelectionIoBudget::from_system_memory(1, bytes_per_row, None)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let plan = MsReadPlan::new(rows.len(), budget)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    if rows.len() > plan.rows_per_block {
        return Ok(None);
    }

    let physical: Vec<_> = rows.iter().map(|row| row.physical).collect();
    let mut data = Vec::new();
    let shape = table
        .column_accessor(name)?
        .fill_array_cells_2d_channel_range_typed_uncached(
            &physical,
            first_channel,
            channel_count,
            SelectedArray2DCellsMut::RowChannelComplex32(&mut data),
        )?;
    let expected_shape = SelectedArray2DShape {
        row_count: rows.len(),
        axis0_count: axes.stored_correlations,
        channel_count,
    };
    if shape != Some(expected_shape) {
        return Ok(None);
    }
    let mut flags = Vec::new();
    if reduction.use_flags {
        let flag_shape = table
            .column_accessor("FLAG")?
            .fill_array_cells_2d_channel_range_typed_uncached(
                &physical,
                first_channel,
                channel_count,
                SelectedArray2DCellsMut::RowChannelBool(&mut flags),
            )?;
        if flag_shape != Some(expected_shape) {
            return Ok(None);
        }
    }

    let values_per_row = channel_count * axes.stored_correlations;
    let expected_values = rows.len() * values_per_row;
    if data.len() != expected_values || (reduction.use_flags && flags.len() != expected_values) {
        return Err(MsError::InvalidInput(
            "invalid packed visibility shape".into(),
        ));
    }
    let mut row_offset = 0;
    let mut dataset = first_dataset;
    for group in rows.chunk_by(|left, right| left.compare(right, Axis::Integration).is_eq()) {
        let mut index = 0;
        for row in row_offset..row_offset + group.len() {
            let row_base = row * values_per_row;
            for channel in 0..channel_count {
                let channel_base = row_base + channel * axes.stored_correlations;
                for &correlation in &axes.correlations {
                    let offset = channel_base + correlation;
                    if !reduction.use_flags || !flags[offset] {
                        let value = f64::from(component.project(data[offset]));
                        moments.push(value, value * value, 1.0, dataset, index);
                    }
                    index += 1;
                }
            }
        }
        row_offset += group.len();
        dataset += 1;
    }
    Ok(Some(dataset - first_dataset))
}
