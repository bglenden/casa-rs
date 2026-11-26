// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::{AddAssign, Div, Mul};

use casa_core::ArrayValue;
use casa_core::quanta::Quantity;
use casa_core::quanta::casacore::{casacore_unit, get_value};
use casa_tables::Table;
use ndarray::ArrayD;
use num_complex::Complex32;

use super::cube::{CubeInput, SampleAxes, WeightCells, read_cells, required};
use super::reduction::Moments;
use super::rows::Row;
use super::spec::{Axis, Column, Weighting};
use super::{CHUNK_AXES, ToolStatisticsOptions, same_group};
use crate::transform::time_average::{
    TimeChunk, TimeGroup, TimeSample, centered_time_and_interval, group_baselines,
};
use casa_ms::{MeasurementSet, MsError, MsResult};

pub(super) struct TimeAverage {
    pub(super) seconds: f64,
    max_uvw_distance: f64,
    mask: bool,
    weighting: Weighting,
}

impl TimeAverage {
    pub(super) fn new(
        ms: &MeasurementSet,
        options: &ToolStatisticsOptions,
        column: Column,
    ) -> MsResult<Self> {
        if !matches!(
            column,
            Column::Complex(_, _) | Column::FloatCube("FLOAT_DATA")
        ) {
            return Err(MsError::InvalidInput(format!(
                "Time averaging of '{}' is not supported",
                options.column.to_ascii_uppercase()
            )));
        }
        let time_bin = Quantity::read(&options.time_bin)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let seconds_unit =
            casacore_unit("s").map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let seconds = get_value(&time_bin, &seconds_unit);
        if !seconds.is_finite() {
            return Err(MsError::InvalidInput(format!(
                "invalid time averaging interval {}",
                options.time_bin
            )));
        }
        // CASA treats negative and zero width as one buffer per integration.
        let seconds = seconds.max(0.0);
        let weighting = if options.use_weights && matches!(column, Column::Complex("MODEL_DATA", _))
        {
            let schema = ms.main_table().schema();
            if schema.is_some_and(|schema| schema.column("CORRECTED_DATA").is_some()) {
                Weighting::VisibilityWeight
            } else if schema.is_some_and(|schema| schema.column("DATA").is_some()) {
                Weighting::InverseSigmaSquared
            } else {
                Weighting::Unweighted
            }
        } else {
            column.weighting(options.use_weights)
        };
        Ok(Self {
            seconds,
            max_uvw_distance: options.max_uvw_distance,
            mask: options.use_flags,
            weighting,
        })
    }

    pub(super) fn reporting_axes(axes: &mut Vec<Axis>, timespan: &str) {
        axes.retain(|&axis| axis != Axis::Integration);
        for (name, axis) in [("scan", Axis::Scan), ("state", Axis::Subscan)] {
            if !timespan.split(',').any(|span| span == name) && !axes.contains(&axis) {
                axes.push(axis);
            }
        }
        axes.push(Axis::Integration);
    }

    pub(super) fn report_chunks<'a>(&self, mut rows: &'a [Row], axes: &[Axis]) -> Vec<&'a [Row]> {
        let reporting = &axes[..axes.len() - 1];
        let mut chunks = Vec::new();
        while let Some(first) = rows.first() {
            let same_bin = |row: &Row| {
                let delta = row.time - first.time;
                if self.seconds == 0.0 {
                    delta == 0.0
                } else {
                    let anchor = first.time - 0.01;
                    ((row.time - anchor) / self.seconds).floor()
                        == ((first.time - anchor) / self.seconds).floor()
                }
            };
            let within_report = |row: &Row| same_group(row, first, reporting) && same_bin(row);
            let size = rows
                .iter()
                .take_while(|row| within_report(row) && same_group(row, first, &CHUNK_AXES[..3]))
                .count();
            let (chunk, rest) = rows.split_at(size);
            chunks.push(chunk);
            // A change only to a hidden array/field/DDID starts a new
            // averaging chunk but contributes to the same statistics record.
            if rest.first().is_none_or(|row| !within_report(row)) {
                break;
            }
            rows = rest;
        }
        chunks
    }

    pub(super) fn reduce(
        &self,
        input: &CubeInput<'_>,
        rows: &[Row],
        axes: &SampleAxes,
        column: Column,
        dataset: usize,
        moments: &mut Moments,
    ) -> MsResult<f64> {
        let name = match column {
            Column::Complex(name, _) | Column::FloatCube(name) => name,
            _ => unreachable!("only visibility cells are time averaged"),
        };
        let physical: Vec<_> = rows.iter().map(|row| row.physical).collect();
        let cells = input.read_data(name, &physical)?;
        let table = input.table;
        let flags = if self.mask {
            read_cells(table, "FLAG", &physical)?
        } else {
            Vec::new()
        };
        let weights = WeightCells::read(table, &physical, self.weighting)?;
        let groups = self.groups(table, rows, &physical)?;
        let first = groups
            .first()
            .ok_or_else(|| shape_error("baseline group"))?;
        let begin = &rows[first.first.row];
        let end_slot = first
            .rows
            .last()
            .ok_or_else(|| shape_error("baseline group"))?
            .physical;
        let end = &rows[end_slot];
        let (center, interval) =
            centered_time_and_interval((begin.time, begin.interval), (end.time, end.interval));
        let mut index = 0;
        for group in groups {
            let grids = GroupCells::read(&group, rows, &cells, &flags, name)?;
            for &channel in &axes.channels {
                for &correlation in &axes.correlations {
                    let position = [correlation, channel];
                    let sample_index = index;
                    index += 1;
                    let (value, weight) = match column {
                        Column::Complex(name, component) => {
                            let (value, _, _) =
                                average(&grids, position, &weights, |data| match data {
                                    ArrayValue::Complex32(data) => data.get(position).copied(),
                                    ArrayValue::Float32(data) if name == "FLOAT_DATA" => {
                                        data.get(position).map(|&value| Complex32::new(value, 0.0))
                                    }
                                    _ => None,
                                })?;
                            (component.project(value), 1.0)
                        }
                        Column::FloatCube(_) => {
                            let (mean, weight, flagged) =
                                average(&grids, position, &weights, |data| match data {
                                    ArrayValue::Float32(data) => data.get(position).copied(),
                                    _ => None,
                                })?;
                            if flagged && self.mask {
                                continue;
                            }
                            let weight = if self.weighting == Weighting::Unweighted {
                                1.0
                            } else {
                                weight
                            };
                            (mean, weight)
                        }
                        _ => unreachable!(),
                    };
                    let value = f64::from(value);
                    moments.push(
                        value,
                        value * value,
                        f64::from(weight),
                        dataset,
                        sample_index,
                    );
                }
            }
        }
        Ok(center - interval * 0.5)
    }

    fn groups(&self, table: &Table, rows: &[Row], physical: &[usize]) -> MsResult<Vec<TimeGroup>> {
        let uvw = if self.max_uvw_distance > 0.0 {
            read_cells(table, "UVW", physical)?
        } else {
            Vec::new()
        };
        let mut samples = Vec::with_capacity(rows.len());
        for (row, metadata) in rows.iter().enumerate() {
            let xyz = if uvw.is_empty() {
                [0.0; 3]
            } else {
                let ArrayValue::Float64(cell) = required(&uvw, row, metadata.physical, "UVW")?
                else {
                    return Err(shape_error("UVW"));
                };
                let Some(&[u, v, w]) = cell.as_slice() else {
                    return Err(shape_error("UVW"));
                };
                [u, v, w]
            };
            // Rows in this call already belong to one reporting bin and
            // one ARRAY/FIELD/DATA_DESC group.
            samples.push(TimeSample {
                row,
                description: 0,
                chunk: TimeChunk([0; 8]),
                time: metadata.time,
                antenna_pair: [metadata.antenna1, metadata.antenna2],
                uvw: xyz,
            });
        }
        Ok(group_baselines(
            samples,
            self.seconds,
            self.max_uvw_distance,
            &[0],
            None,
        ))
    }
}

struct GroupCells<'a> {
    slot: usize,
    value: &'a ArrayValue,
    flags: Option<&'a ArrayD<bool>>,
}

impl<'a> GroupCells<'a> {
    fn read(
        group: &TimeGroup,
        rows: &[Row],
        cells: &'a [Option<ArrayValue>],
        flags: &'a [Option<ArrayValue>],
        name: &str,
    ) -> MsResult<Vec<Self>> {
        group
            .rows
            .iter()
            .map(|row| {
                let slot = row.physical;
                let physical = rows[slot].physical;
                let value = required(cells, slot, physical, name)?;
                let flags = if flags.is_empty() {
                    None
                } else {
                    let ArrayValue::Bool(flags) = required(flags, slot, physical, "FLAG")? else {
                        return Err(shape_error("FLAG"));
                    };
                    Some(flags)
                };
                Ok(Self { slot, value, flags })
            })
            .collect()
    }
}

fn average<T>(
    group: &[GroupCells<'_>],
    position: [usize; 2],
    weights: &WeightCells,
    project: impl Fn(&ArrayValue) -> Option<T>,
) -> MsResult<(T, f32, bool)>
where
    T: AddAssign + Copy + Default + Mul<f32, Output = T> + Div<f32, Output = T>,
{
    let mut sum = T::default();
    let mut sum_weights = 0.0;
    let mut all_flagged = true;
    for cell in group {
        let flagged = match cell.flags {
            Some(flags) => *flags.get(position).ok_or_else(|| shape_error("FLAG"))?,
            None => false,
        };
        if !all_flagged && flagged {
            continue;
        }
        if all_flagged && !flagged {
            sum = T::default();
            sum_weights = 0.0;
            all_flagged = false;
        }
        let weight = weights.row(cell.slot).get(position[0], position[1])? as f32;
        let value = project(cell.value).ok_or_else(|| shape_error("visibility"))?;
        sum += value * weight;
        sum_weights += weight;
    }
    let mean = if sum_weights > 0.0 {
        sum / sum_weights
    } else {
        sum
    };
    Ok((mean, sum_weights, all_flagged))
}

fn shape_error(column: &str) -> MsError {
    MsError::InvalidInput(format!(
        "invalid {column} cell shape in statistics time averaging"
    ))
}
