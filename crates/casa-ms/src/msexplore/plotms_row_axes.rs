// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unaveraged identifiers from the selected MeasurementSet and its iterator.

use std::collections::HashMap;

use super::{MsAxis, MsPlotSpec, selected_i32_values};
use crate::subtables::get_string;
use crate::{MeasurementSet, SubtableId};

pub(super) struct RowAxes(Vec<(MsAxis, Vec<f64>)>);

impl RowAxes {
    pub(super) fn read(
        ms: &MeasurementSet,
        spec: &MsPlotSpec,
        rows: &[usize],
        selection_order: Option<&[usize]>,
        buffer_starts: &[bool],
        antennas: [&[i32]; 2],
    ) -> Result<Self, String> {
        let mut axes = Vec::new();
        let requested = std::iter::once(spec.x_axis).chain(spec.y_axes.iter().copied());
        for axis in requested.filter(|axis| axis.is_row_identifier()) {
            if axes.iter().any(|(loaded, _)| *loaded == axis) {
                continue;
            }
            let values = match axis {
                MsAxis::Row => row_numbers(rows, selection_order.expect("selected Row numbers")),
                MsAxis::Antenna1 => antennas[0].iter().copied().map(f64::from).collect(),
                MsAxis::Antenna2 => antennas[1].iter().copied().map(f64::from).collect(),
                MsAxis::Observation | MsAxis::Intent | MsAxis::Feed1 | MsAxis::Feed2 => {
                    buffer_values(ms, rows, buffer_starts, axis)?
                }
                _ => unreachable!("row identifier axis"),
            };
            axes.push((axis, values));
        }
        Ok(Self(axes))
    }

    pub(super) fn value(&self, axis: MsAxis, row_slot: usize) -> Option<f64> {
        self.0
            .iter()
            .find(|(loaded, _)| *loaded == axis)
            .map(|(_, values)| values[row_slot])
    }
}

fn row_numbers(rows: &[usize], selection_order: &[usize]) -> Vec<f64> {
    let index = selection_order
        .iter()
        .enumerate()
        .map(|(ordinal, &row)| (row, ordinal))
        .collect::<HashMap<_, _>>();
    rows.iter().map(|row| index[row] as f64).collect()
}

fn buffer_values(
    ms: &MeasurementSet,
    rows: &[usize],
    buffer_starts: &[bool],
    axis: MsAxis,
) -> Result<Vec<f64>, String> {
    let column = match axis {
        MsAxis::Observation => "OBSERVATION_ID",
        MsAxis::Intent => "STATE_ID",
        MsAxis::Feed1 => "FEED1",
        MsAxis::Feed2 => "FEED2",
        _ => unreachable!("visibility buffer identifier"),
    };
    let mut ids = selected_i32_values(ms.main_table(), column, rows)?;
    if axis == MsAxis::Intent {
        assign_intents(ms, &mut ids)?;
    }
    // PlotMS's degenerate-axis index selects the first value in each buffer.
    let mut first = 0;
    Ok(ids
        .into_iter()
        .zip(buffer_starts)
        .map(|(value, begins)| {
            if *begins {
                first = value;
            }
            f64::from(first)
        })
        .collect())
}

fn assign_intents(ms: &MeasurementSet, states: &mut [i32]) -> Result<(), String> {
    let Some(table) = ms.subtable(SubtableId::State) else {
        return Ok(());
    };
    if table.row_count() == 0 {
        return Ok(());
    }
    let mut unique_modes = HashMap::new();
    let intent_ids = (0..table.row_count())
        .map(|row| {
            let mode = get_string(table, row, "OBS_MODE").map_err(|error| error.to_string())?;
            let next_id = unique_modes.len() as i32;
            Ok(*unique_modes.entry(mode).or_insert(next_id))
        })
        .collect::<Result<Vec<_>, String>>()?;
    for state in states {
        if let Ok(id) = usize::try_from(*state) {
            *state = *intent_ids
                .get(id)
                .ok_or_else(|| format!("invalid STATE_ID {id} for intent axis"))?;
        }
    }
    Ok(())
}
