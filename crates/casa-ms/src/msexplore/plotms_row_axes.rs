// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unaveraged identifiers from the selected MeasurementSet and its iterator.

use std::collections::HashMap;

use super::plotms_ordinates::PointDimensions;
use super::{MsAxis, MsIterationAxis, MsPlotSpec, selected_i32_values, uses_extended_averaging};
use crate::subtables::get_string;
use crate::{MeasurementSet, SubtableId};

pub(super) fn requested(spec: &MsPlotSpec) -> bool {
    spec.x_axis.is_row_identifier() || spec.y_axes.iter().copied().any(MsAxis::is_row_identifier)
}

pub(super) fn metadata_pair(spec: &MsPlotSpec) -> Option<PointDimensions> {
    let &[y] = spec.y_axes.as_slice() else {
        return None;
    };
    (matches!(spec.x_axis, MsAxis::Channel | MsAxis::Time) && y.is_row_identifier()).then_some(
        PointDimensions {
            channel: spec.x_axis == MsAxis::Channel,
            baseline: matches!(y, MsAxis::Row | MsAxis::Antenna1 | MsAxis::Antenna2),
        },
    )
}

pub(super) fn uses_scan_row_iteration(spec: &MsPlotSpec) -> bool {
    spec.iteration.iteraxis == Some(MsIterationAxis::Scan)
        && spec.x_axis == MsAxis::Time
        && spec.y_axes == [MsAxis::Row]
}

pub(super) fn uses_mixed_row_geometry(spec: &MsPlotSpec) -> bool {
    spec.x_axis.is_visibility_math()
        && matches!(
            spec.y_axes.as_slice(),
            [MsAxis::Row, MsAxis::Elevation] | [MsAxis::Elevation, MsAxis::Row]
        )
}

pub(super) fn validate(spec: &MsPlotSpec) -> Result<(), String> {
    if !requested(spec) {
        return Ok(());
    }
    if uses_extended_averaging(spec) || spec.averaging.avgchannel.is_some_and(|width| width > 1) {
        return Err("averaged PlotMS row identifiers are pending (#1032)".into());
    }
    if metadata_pair(spec).is_some() {
        return if spec.iteration.iteraxis.is_some() && !uses_scan_row_iteration(spec) {
            Err("iterated PlotMS metadata-only row identifier plots are pending (#1032)".into())
        } else {
            Ok(())
        };
    }
    if spec.y_axes.iter().copied().any(|y| {
        (spec.x_axis.is_row_identifier() && !y.is_visibility_math())
            || (y.is_row_identifier() && !spec.x_axis.is_visibility_math())
    }) {
        return Err("PlotMS row identifiers without a visibility axis are pending (#1032)".into());
    }
    if spec
        .y_axes
        .iter()
        .copied()
        .any(MsAxis::uses_derived_geometry)
        && !uses_mixed_row_geometry(spec)
    {
        return Err("PlotMS row identifiers with geometry ordinates are pending (#1032)".into());
    }
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::super::{
        MsFlagAction, MsFlagEditSpec, MsFlagRegion, MsPlotPreset, preview_msexplore_flag_edit,
    };
    use super::*;
    use crate::{MeasurementSetBuilder, MsSelection};

    #[test]
    fn direct_row_identifier_flag_preview_is_refused() {
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        let mut spec = MsPlotSpec::from_preset(MsPlotPreset::AmplitudeVsTime);
        spec.preset = None;
        spec.x_axis = MsAxis::Row;
        spec.flag_edit = Some(MsFlagEditSpec {
            action: MsFlagAction::Flag,
            region: MsFlagRegion {
                x_min: 1.5,
                x_max: 2.5,
                y_min: 0.0,
                y_max: 10.0,
            },
            plot_index: None,
            panel_key: None,
            extcorr: false,
            extchannel: false,
        });
        let error = preview_msexplore_flag_edit(&ms, &MsSelection::new(), &spec).unwrap_err();
        assert_eq!(
            error,
            "MeasurementSet row identifier axes require the public plotms task"
        );
    }
}
