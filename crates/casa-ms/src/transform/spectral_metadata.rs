// SPDX-License-Identifier: LGPL-3.0-or-later
//! SPECTRAL_WINDOW metadata for channel selection and averaging.

use std::collections::BTreeMap;
use std::path::Path;

use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Array1;

use crate::SubtableId;

use super::{MsTransformError, TransformChannelSelection, all_channels_selected, f64_vector_cell};

pub(super) struct ChannelWindow {
    pub(super) first: usize,
    pub(super) last: usize,
    pub(super) dropped: bool,
}

struct SpectralGrid {
    centres: Vec<f64>,
    widths: Vec<f64>,
    resolution: Vec<f64>,
    reference: f64,
    bandwidth: f64,
}

pub(super) fn remap_associations(
    mut row: RecordValue,
    spw_map: &BTreeMap<i32, i32>,
) -> RecordValue {
    let Some(Value::Array(ArrayValue::Int32(ids))) = row.get("ASSOC_SPW_ID") else {
        return row;
    };
    let selected = ids
        .iter()
        .enumerate()
        .filter_map(|(index, id)| spw_map.get(id).map(|&id| (index, id)))
        .collect::<Vec<_>>();
    if let Some(Value::Array(ArrayValue::String(nature))) = row.get("ASSOC_NATURE") {
        let nature = nature.iter().collect::<Vec<_>>();
        let values = selected
            .iter()
            .map(|&(index, _)| {
                nature
                    .get(index)
                    .map_or_else(String::new, |&value| value.clone())
            })
            .collect();
        row.upsert(
            "ASSOC_NATURE",
            Value::Array(ArrayValue::from_string_vec(values)),
        );
    }
    row.upsert(
        "ASSOC_SPW_ID",
        Value::Array(ArrayValue::from_i32_vec(
            selected.into_iter().map(|(_, id)| id).collect(),
        )),
    );
    row
}

pub(super) fn write_channel_selection(
    output_ms: &Path,
    compact_ids: &BTreeMap<i32, i32>,
    selections: &BTreeMap<i32, TransformChannelSelection>,
    averaged: Option<&BTreeMap<i32, Vec<ChannelWindow>>>,
    split_widths: Option<&BTreeMap<i32, usize>>,
) -> Result<(), MsTransformError> {
    let path = output_ms.join(SubtableId::SpectralWindow.name());
    let error = |source: casa_tables::TableError| MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut table = Table::open(TableOptions::new(&path)).map_err(error)?;
    let mut changed = false;
    for (old_id, &new_id) in compact_ids {
        let row = new_id as usize;
        let selection = &selections[old_id];
        let freq = f64_vector_cell(&table, row, "CHAN_FREQ", &path)?;
        let averaged = averaged.map(|windows| windows[old_id].as_slice());
        let split_width = split_widths.map(|widths| widths[old_id]);
        if all_channels_selected(&selection.channels, freq.len())
            && averaged.is_none_or(|bins| bins.len() == freq.len())
            && split_width.is_none_or(|width| width == 1 || freq.len() == 1)
        {
            continue;
        }
        let widths = f64_vector_cell(&table, row, "CHAN_WIDTH", &path)?;
        let resolution = f64_vector_cell(&table, row, "RESOLUTION", &path)?;
        let planned;
        let windows = if let Some(width) = split_width {
            planned = split_windows(selection, width);
            &planned
        } else if let Some(windows) = averaged {
            windows
        } else {
            planned = selection_windows(selection);
            &planned
        };
        let bad_axis = || MsTransformError::SpectralMetadata {
            path: path.display().to_string(),
            reason: format!("SPW {old_id} channel metadata does not cover the selection"),
        };
        let mut grid =
            selected_windows(windows, &freq, &widths, &resolution).ok_or_else(bad_axis)?;
        let effective_bandwidth = if let Some(width) = split_width {
            let input = f64_vector_cell(&table, row, "EFFECTIVE_BW", &path)?;
            let stride = selection.ranges[0].step.max(1) as usize;
            let bandwidths =
                split_effective_bandwidths(windows, &input, stride).ok_or_else(bad_axis)?;
            grid.bandwidth = bandwidths.iter().sum();
            if width == 1 && stride == 1 {
                for (width, window) in grid.widths.iter_mut().zip(windows) {
                    *width = widths[window.first];
                }
            }
            bandwidths
        } else {
            grid.resolution.clone()
        };
        let channel_count = grid.centres.len();
        for (name, values) in [
            ("CHAN_FREQ", grid.centres),
            ("CHAN_WIDTH", grid.widths),
            ("EFFECTIVE_BW", effective_bandwidth),
            ("RESOLUTION", grid.resolution),
        ] {
            let value = Value::Array(ArrayValue::Float64(Array1::from_vec(values).into_dyn()));
            table
                .column_accessor_mut(name)
                .and_then(|mut column| column.set(row, value))
                .map_err(error)?;
        }
        for (name, value) in [
            ("NUM_CHAN", ScalarValue::Int32(channel_count as i32)),
            ("REF_FREQUENCY", ScalarValue::Float64(grid.reference)),
            ("TOTAL_BANDWIDTH", ScalarValue::Float64(grid.bandwidth)),
        ] {
            table
                .column_accessor_mut(name)
                .and_then(|mut column| column.set(row, Value::Scalar(value)))
                .map_err(error)?;
        }
        changed = true;
    }
    if changed {
        table
            .prepare_write()
            .save(TableOptions::new(&path))
            .map_err(error)?;
    }
    Ok(())
}

fn split_windows(selection: &TransformChannelSelection, width: usize) -> Vec<ChannelWindow> {
    let mut windows = selection.metadata_for_width(width);
    for window in &mut windows {
        window.dropped = false;
    }
    windows
}

fn split_effective_bandwidths(
    windows: &[ChannelWindow],
    input: &[f64],
    stride: usize,
) -> Option<Vec<f64>> {
    windows
        .iter()
        .map(|window| {
            (window.first..=window.last)
                .step_by(stride)
                .try_fold(0.0, |sum, channel| Some(sum + input.get(channel)?))
        })
        .collect()
}

fn selection_windows(selection: &TransformChannelSelection) -> Vec<ChannelWindow> {
    selection
        .channels
        .iter()
        .zip(&selection.metadata_ends)
        .map(|(&first, &last)| ChannelWindow {
            first,
            last,
            dropped: false,
        })
        .collect()
}

/// Use the existing copy-task channel geometry before sampling a new grid.
pub(super) fn selected_footprints(
    selection: &TransformChannelSelection,
    frequencies: &[f64],
    widths: &[f64],
) -> Option<(Vec<f64>, Vec<f64>)> {
    let grid = selected_windows(&selection_windows(selection), frequencies, widths, widths)?;
    Some((grid.centres, grid.widths))
}

fn selected_windows(
    windows: &[ChannelWindow],
    freq: &[f64],
    widths: &[f64],
    resolution: &[f64],
) -> Option<SpectralGrid> {
    let sign = if freq.last()? < freq.first()? {
        -1.0
    } else {
        1.0
    };
    let mut grid = SpectralGrid {
        centres: Vec::with_capacity(windows.len()),
        widths: Vec::with_capacity(windows.len()),
        resolution: Vec::with_capacity(windows.len()),
        reference: 0.0,
        bandwidth: 0.0,
    };
    // MSTransformDataHandler::fillSPWTable uses the stride's span for the
    // metadata even though the visibility iterator reads only stride samples.
    let centre =
        |window: &ChannelWindow| Some((freq.get(window.first)? + freq.get(window.last)?) * 0.5);
    grid.reference = centre(windows.first()?)?.min(centre(windows.last()?)?);
    let mut dropped_width = 0.0;
    for &ChannelWindow {
        first,
        last,
        dropped,
    } in windows
    {
        let separation = (freq.get(last)? - freq.get(first)?) * sign;
        let width = (separation + (widths.get(first)? + widths.get(last)?).abs() * 0.5) * sign;
        grid.bandwidth += width.abs();
        if dropped {
            dropped_width += width;
        } else {
            grid.centres.push((freq[first] + freq[last]) * 0.5);
            grid.widths.push(width);
            grid.resolution
                .push(separation + (resolution.get(first)? + resolution.get(last)?) * 0.5);
        }
    }
    grid.bandwidth -= dropped_width;
    Some(grid)
}
