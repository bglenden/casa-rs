// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unaveraged identifiers and geometry on selected MeasurementSet buffers.

use std::collections::HashMap;

use casa_core::measures::position::MPosition;

use super::plotms_ordinates::PointDimensions;
use super::{
    MsAxis, MsDataColumn, MsIterationAxis, MsPlotSpec, normalize_signed_degrees, plotms_columns,
    selected_i32_values, uses_extended_averaging,
};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::observation_io::{Feeds, read_feeds};
use casa_ms::subtables::get_string;
use casa_ms::{MeasurementSet, SubtableId};

pub(super) fn averaged_uvw_cache_index(
    groups: &casa_ms::transform::PlotTimeGroups,
    group: usize,
    bins: usize,
    channel: usize,
) -> usize {
    let buffers = groups.buffers();
    let buffer = &buffers[buffers.partition_point(|buffer| buffer.end <= group)];
    (group - buffer.start) * bins + channel
}

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

pub fn array_horizontal_pair(x: MsAxis, y: MsAxis) -> bool {
    x == MsAxis::Channel && y.uses_derived_geometry()
}

pub fn metadata_coordinate_pair(x: MsAxis, y: MsAxis) -> Option<PointDimensions> {
    if x.uses_uvw() && y.uses_uvw() {
        return Some(PointDimensions {
            channel: x.uses_wavelengths() || y.uses_wavelengths(),
            baseline: true,
        });
    }
    let channel = match (x, y) {
        (MsAxis::Channel, MsAxis::Frequency) => true,
        (MsAxis::Elevation, MsAxis::Time) => false,
        _ => return None,
    };
    Some(PointDimensions {
        channel,
        baseline: false,
    })
}

pub(super) fn unaveraged_coordinate_mask(
    spec: &MsPlotSpec,
    raw_plotms: bool,
) -> Result<Option<PointDimensions>, String> {
    let [y] = spec.y_axes.as_slice() else {
        return Ok(None);
    };
    let mask = metadata_coordinate_pair(spec.x_axis, *y);
    if spec.x_axis.uses_uvw() && y.uses_uvw() {
        return Ok(mask.filter(|_| raw_plotms));
    }
    if mask.is_some()
        && (!raw_plotms
            || uses_extended_averaging(spec)
            || spec.averaging.avgchannel.is_some_and(|width| width > 1)
            || spec
                .iteration
                .iteraxis
                .is_some_and(|axis| axis != MsIterationAxis::Scan))
    {
        return Err("This PlotMS metadata coordinate combination is pending (#1032)".into());
    }
    Ok(mask)
}

pub(super) struct AntennaAxes {
    axis: MsAxis,
    count: usize,
    feeds: Option<Feeds>,
}

#[derive(Clone, Copy)]
pub(super) struct AntennaVisit {
    pub(super) antenna: usize,
    pub(super) buffer_start: usize,
    pub(super) first_for_row: bool,
}

impl AntennaAxes {
    pub(super) fn read(ms: &MeasurementSet, axis: MsAxis) -> Result<Self, String> {
        let antenna = ms.antenna().map_err(|error| error.to_string())?;
        let feeds = (axis == MsAxis::AntennaParallacticAngle)
            .then(|| read_feeds(ms))
            .transpose()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            axis,
            count: antenna.row_count(),
            feeds,
        })
    }

    pub(super) fn visits<'a>(
        &self,
        buffer_starts: &'a [bool],
    ) -> impl Iterator<Item = (usize, Option<AntennaVisit>)> + use<'a> {
        let starts = buffer_starts
            .iter()
            .enumerate()
            .filter_map(|(row, &start)| start.then_some(row));
        let ends = starts
            .clone()
            .skip(1)
            .chain(std::iter::once(buffer_starts.len()));
        let antenna_count = self.count;
        starts.zip(ends).flat_map(move |(start, end)| {
            let row_count = end - start;
            (0..antenna_count).map(move |antenna| {
                // The antenna axis spans the subtable, while export metadata
                // indexes the selected baseline rows cyclically in this buffer.
                (
                    start + antenna % row_count,
                    Some(AntennaVisit {
                        antenna,
                        buffer_start: start,
                        first_for_row: antenna < row_count,
                    }),
                )
            })
        })
    }

    pub(super) fn value(
        &self,
        engine: &MsCalEngine,
        antenna: usize,
        time: f64,
        field_id: i32,
        spw: i32,
    ) -> Result<f64, String> {
        let field = usize::try_from(field_id)
            .map_err(|_| format!("invalid FIELD_ID {field_id} for {} axis", self.axis))?;
        let value = match self.axis {
            MsAxis::AntennaAzimuth | MsAxis::AntennaElevation => engine
                .azel(time, field, antenna)
                .map(|(azimuth, elevation)| {
                    if self.axis == MsAxis::AntennaAzimuth {
                        normalize_signed_degrees(azimuth.to_degrees())
                    } else {
                        elevation.to_degrees()
                    }
                }),
            MsAxis::AntennaParallacticAngle => {
                let receptor = self
                    .feeds
                    .as_ref()
                    .and_then(|feeds| feeds.receptors(antenna as i32, 0, spw))
                    .and_then(|receptors| receptors.angles.first())
                    .map_or(0.0, |angle| angle.value() as f32);
                // The visibility iterator adds receptor zero in single precision.
                engine
                    .iterator_feed_parallactic_angle(time, field, antenna)
                    .map(|angle| f64::from(angle as f32 + receptor).to_degrees())
            }
            _ => unreachable!("only antenna horizontal axes use the antenna grid"),
        };
        value.map_err(|error| error.to_string())
    }
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

pub(super) fn uses_time_row_averaging(spec: &MsPlotSpec) -> bool {
    spec.x_axis == MsAxis::Row
        && matches!(spec.y_axes.as_slice(), [axis] if axis.is_visibility_math())
        && matches!(
            spec.data_column,
            MsDataColumn::Data | MsDataColumn::Corrected
        )
        && spec.iteration.iteraxis.is_none()
        && spec.transforms.freqframe.is_none()
        && !spec.averaging.scalar
        && spec.averaging.avgchannel.is_none_or(|width| width <= 1)
        && plotms_columns::uses_ordinary_time_averaging(spec)
}

pub(super) fn mean_antenna_position(ms: &MeasurementSet) -> Result<MPosition, String> {
    let antenna = ms.antenna().map_err(|error| error.to_string())?;
    let count = antenna.row_count();
    if count == 0 {
        return Err("PlotMS Elevation requires at least one antenna position".into());
    }
    // MSDerivedValues::setAntennas uses the Cartesian mean of the subtable.
    let mut sum = [0.0; 3];
    for row in 0..count {
        let position = antenna.position(row).map_err(|error| error.to_string())?;
        for (total, coordinate) in sum.iter_mut().zip(position) {
            *total += coordinate;
        }
    }
    let [x, y, z] = sum.map(|coordinate| coordinate / count as f64);
    Ok(MPosition::new_itrf(x, y, z))
}

pub(super) fn array_horizontal_value(
    engine: &MsCalEngine,
    axis: MsAxis,
    time: f64,
    field_id: i32,
    position: MPosition,
) -> Result<f64, String> {
    let field = usize::try_from(field_id)
        .map_err(|_| format!("invalid FIELD_ID {field_id} for {axis} axis"))?;
    let value =
        match axis {
            MsAxis::Azimuth | MsAxis::Elevation => engine
                .azel_at_position(time, field, position)
                .map(|(azimuth, elevation)| {
                    if axis == MsAxis::Azimuth {
                        normalize_signed_degrees(azimuth.to_degrees())
                    } else {
                        elevation.to_degrees()
                    }
                }),
            MsAxis::HourAngle => engine
                .hour_angle_at_position(time, field, position)
                .map(|angle| normalize_signed_degrees(angle.to_degrees()) / 15.0),
            MsAxis::ParallacticAngle => engine
                .array_feed_angle_at_position(time, field, position)
                .map(|angle| {
                    let degrees = f64::from(angle as f32).to_degrees();
                    if degrees < 0.0 {
                        degrees + 360.0
                    } else {
                        degrees
                    }
                }),
            _ => unreachable!("only array horizontal axes use the antenna-position mean"),
        };
    value.map_err(|error| error.to_string())
}

/// The requested ephemeris scalars sampled once per selected visibility buffer.
#[derive(Default)]
pub(super) struct BufferEphemeris {
    distance: Option<f64>,
    radial_velocity: Option<f64>,
}

impl BufferEphemeris {
    pub(super) fn read(
        engine: &MsCalEngine,
        axes: &[MsAxis],
        time: f64,
        field: i32,
    ) -> Result<Self, String> {
        let value = |axis| {
            axes.contains(&axis)
                .then(|| ephemeris_value(engine, axis, time, field))
                .transpose()
        };
        Ok(Self {
            distance: value(MsAxis::EphemerisDistance)?,
            radial_velocity: value(MsAxis::RadialVelocity)?,
        })
    }

    pub(super) fn get(&self, axis: MsAxis) -> Option<f64> {
        match axis {
            MsAxis::EphemerisDistance => self.distance,
            MsAxis::RadialVelocity => self.radial_velocity,
            _ => None,
        }
    }
}

fn ephemeris_value(
    engine: &MsCalEngine,
    axis: MsAxis,
    time: f64,
    field_id: i32,
) -> Result<f64, String> {
    let field = usize::try_from(field_id)
        .map_err(|_| format!("invalid FIELD_ID {field_id} for {axis} axis"))?;
    let value = engine
        .field_ephemeris_scalar(field, time, |ephemeris, time_utc| match axis {
            MsAxis::RadialVelocity => ephemeris.radial_velocity(time_utc).map(|value| value.ms()),
            MsAxis::EphemerisDistance => ephemeris.distance_metres(time_utc),
            _ => unreachable!("only ephemeris axes use attached ephemeris samples"),
        })
        .map_err(|error| error.to_string())?;
    // In an MS with an ephemeris, MSFieldColumns gives zero for a fixed field.
    Ok(value.unwrap_or_default() / 1000.0)
}

pub(super) fn validate(spec: &MsPlotSpec) -> Result<(), String> {
    if !requested(spec) {
        return Ok(());
    }
    if uses_extended_averaging(spec) || spec.averaging.avgchannel.is_some_and(|width| width > 1) {
        return if uses_time_row_averaging(spec) {
            Ok(())
        } else {
            Err("averaged PlotMS row identifiers are pending (#1032)".into())
        };
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
    use casa_ms::{MeasurementSetBuilder, MsSelection};

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
