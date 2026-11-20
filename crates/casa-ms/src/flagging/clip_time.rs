// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time-bin membership for clipping a selected MeasurementSet.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{AddAssign, DivAssign, Mul};

use casa_tables::Table;
use casa_types::{ArrayValue, ScalarValue, quanta::Quantity};
use ndarray::{ArrayD, Zip};

use super::clip::{ClipOptions, apply_clip_array};
use super::clip_average::{ChannelBins, ChannelReduction};
use super::clip_columns::{ClipColumns, subtract_model};
use super::correlation::CorrelationSelection;
use super::{
    ChangeSet, ChannelSelection, FLAG_COLUMN, FlagDataColumn, FlagSummary, FlaggingError,
    MeasurementSet, MsReadPlan, MsSelectionIoBudget, clip_channel_selection, load_i32_cells,
    write_touched_flag_updates,
};

// These are the visibility iterator's chunk keys, followed by the ordered
// baseline. A positive bin never crosses a chunk or an antenna-pair boundary.
const GROUP_COLUMNS: [&str; 7] = [
    "OBSERVATION_ID",
    "ARRAY_ID",
    "SCAN_NUMBER",
    "FIELD_ID",
    "DATA_DESC_ID",
    "ANTENNA1",
    "ANTENNA2",
];

pub(super) struct ClipTimedRow {
    pub(super) row: usize,
    group: [i32; GROUP_COLUMNS.len()],
    seconds: f64,
}

impl ClipTimedRow {
    pub(super) fn description(&self) -> i32 {
        self.group[4]
    }
}

pub(super) fn time_bin_seconds(text: &str) -> Option<f64> {
    // Like casaQuantity(...).get("s"), use the SI scaling of recognized
    // quantities. The task also accepts a unitless value as seconds.
    Quantity::read(text)
        .ok()
        .map(|value| value.get_si_value())
        .filter(|value| value.is_finite())
}

pub(super) fn read_clip_times(
    ms: &MeasurementSet,
    selected: &[usize],
    path: &str,
) -> Result<Vec<ClipTimedRow>, FlaggingError> {
    let fail = |reason: String| FlaggingError::MutateFlags {
        path: path.to_string(),
        reason,
    };
    let table = ms.main_table();
    let time = table
        .column_accessor("TIME")
        .map_err(|error| fail(error.to_string()))?;
    let bytes = std::mem::size_of::<ClipTimedRow>() + std::mem::size_of::<Option<ScalarValue>>();
    let budget = MsSelectionIoBudget::from_system_memory(2, bytes, None)
        .map_err(|error| fail(error.to_string()))?;
    let plan = MsReadPlan::new(selected.len(), budget).map_err(|error| fail(error.to_string()))?;
    let mut timed = Vec::with_capacity(selected.len());
    for rows in selected.chunks(plan.rows_per_block.max(1)) {
        let begin = timed.len();
        for (&row, value) in rows.iter().zip(
            time.scalar_cells_owned_for_rows(rows)
                .map_err(|error| fail(error.to_string()))?,
        ) {
            match value {
                Some(ScalarValue::Float64(seconds)) if seconds.is_finite() => {
                    timed.push(ClipTimedRow {
                        row,
                        group: [0; GROUP_COLUMNS.len()],
                        seconds,
                    });
                }
                _ => return Err(fail(format!("TIME row {row} must be a finite Double"))),
            }
        }
        for (slot, name) in GROUP_COLUMNS.into_iter().enumerate() {
            for (row, value) in timed[begin..]
                .iter_mut()
                .zip(load_i32_cells(table, rows, name, path)?)
            {
                row.group[slot] = value;
            }
        }
    }
    timed.sort_unstable_by(|left, right| {
        left.group
            .cmp(&right.group)
            .then_with(|| left.seconds.total_cmp(&right.seconds))
            .then_with(|| left.row.cmp(&right.row))
    });
    Ok(timed)
}

pub(super) fn clip_time_groups(
    mut rows: &[ClipTimedRow],
    interval: f64,
) -> impl Iterator<Item = &[ClipTimedRow]> {
    // CASA shrinks the interval slightly before comparing the integration
    // centers. It anchors the interval at the first sample of each baseline.
    let limit = interval * 0.999;
    std::iter::from_fn(move || {
        let first = rows.first()?;
        let count = 1 + rows[1..].partition_point(|row| {
            row.group == first.group && row.seconds - first.seconds <= limit
        });
        let (group, remaining) = rows.split_at(count);
        rows = remaining;
        Some(group)
    })
}

pub(super) struct TimeClip<'a> {
    pub(super) seconds: f64,
    pub(super) column: FlagDataColumn,
    pub(super) widths: Option<&'a [u32]>,
    pub(super) channels: Option<&'a BTreeMap<i32, ChannelSelection>>,
    pub(super) correlations: Option<&'a CorrelationSelection>,
    pub(super) options: ClipOptions<'a>,
    pub(super) path: &'a str,
}

impl TimeClip<'_> {
    pub(super) fn apply(
        &self,
        ms: &mut MeasurementSet,
        selected: &[usize],
        columns: &mut ClipColumns,
        ddid_to_spw: &BTreeMap<i32, i32>,
        read_plan: MsReadPlan,
    ) -> Result<ChangeSet, FlaggingError> {
        let timed = read_clip_times(ms, selected, self.path)?;
        let rows_per_block = read_plan.rows_per_block.max(1);
        let mut changes = ChangeSet::default();
        let mut counts = FlagSummary {
            flagged_samples: 0,
            total_samples: 0,
        };
        let mut matched = Vec::new();
        for members in clip_time_groups(&timed, self.seconds) {
            let (average, before) =
                collect_time_bin(ms, members, rows_per_block, columns, ddid_to_spw, self.path)?;
            counts.flagged_samples += before.flagged_samples;
            counts.total_samples += before.total_samples;
            let first = &members[0];
            let ddid = first.description();
            let projections = self
                .correlations
                .map(|correlations| correlations.clip_projections_for(ddid, self.path))
                .transpose()?;
            if projections.is_some_and(|projections| projections.is_empty()) {
                continue;
            }
            let (channels, width) = clip_channel_selection(
                Some(ddid),
                ddid_to_spw,
                self.channels,
                self.widths,
                average.flags.shape()[1],
                self.path,
            )?;
            matched.clear();
            let changes_in_group = self.match_group(
                average,
                first.row,
                channels.as_deref(),
                width,
                ClipOptions {
                    projections,
                    ..self.options
                },
                &mut matched,
            )?;
            if changes_in_group.is_empty() {
                continue;
            }
            let changed = apply_time_bin_flags(
                ms,
                members,
                rows_per_block,
                &changes_in_group,
                self.path,
                &mut changes,
            )?;
            counts.flagged_samples += changed;
            changes.changed_samples += changed;
        }
        changes.summary = Some(counts);
        Ok(changes)
    }

    fn match_group(
        &self,
        mut average: ClipTimeAccumulator,
        row: usize,
        channels: Option<&[usize]>,
        width: u32,
        options: ClipOptions<'_>,
        matched: &mut Vec<(usize, usize)>,
    ) -> Result<Vec<(usize, usize)>, FlaggingError> {
        let sum_weights = matches!(
            self.column,
            FlagDataColumn::Weight | FlagDataColumn::WeightSpectrum
        );
        average.normalize(sum_weights);
        let bins = channels.and_then(|indices| ChannelBins::new(width, indices, &average.flags));
        let mut data = Cow::Borrowed(&average.data);
        let mut model = average.model.as_ref().map(Cow::Borrowed);
        if let Some(bins) = &bins {
            let inverse_sigma = matches!(
                self.column,
                FlagDataColumn::Data | FlagDataColumn::FloatData | FlagDataColumn::ResidualData
            );
            let stats = ArrayValue::Float32(if inverse_sigma {
                average.mass.mapv(|weight| {
                    if weight > f32::MIN_POSITIVE {
                        1.0 / weight.sqrt()
                    } else {
                        -1.0
                    }
                })
            } else {
                average.mass
            });
            let reduction = if sum_weights {
                ChannelReduction::Sum
            } else if self.column == FlagDataColumn::ModelData {
                ChannelReduction::Mean
            } else {
                ChannelReduction::Weighted {
                    spectrum: &stats,
                    inverse_sigma,
                }
            };
            data = Cow::Owned(bins.average(&data, &average.flags, reduction));
            model = model.map(|model| {
                Cow::Owned(bins.average(&model, &average.flags, ChannelReduction::Mean))
            });
        }
        if let Some(model) = model {
            data = Cow::Owned(subtract_model(&data, &model, row, self.path)?);
        }
        let (flags, channels) = bins
            .as_ref()
            .map_or((&average.flags, channels), |bins| (bins.flags(), None));
        apply_clip_array(&data, flags, channels, options, self.path, row, matched)?;
        let original = matched.iter().flat_map(|(plane, channel)| {
            let channels = bins.as_ref().map_or(std::slice::from_ref(channel), |bins| {
                bins.original_channels(*channel)
            });
            channels.iter().map(move |&original| (*plane, original))
        });
        Ok(original.collect())
    }
}

fn apply_time_bin_flags(
    ms: &mut MeasurementSet,
    members: &[ClipTimedRow],
    block_size: usize,
    matched: &[(usize, usize)],
    path: &str,
    changes: &mut ChangeSet,
) -> Result<usize, FlaggingError> {
    let mut changed_samples = 0;
    for block in members.chunks(block_size) {
        let rows = block.iter().map(|entry| entry.row).collect::<Vec<_>>();
        let flags = read_flags(ms.main_table(), &rows, path)?;
        let mut updated = BTreeMap::new();
        let mut touched = BTreeSet::new();
        for (row, mut flags) in rows.into_iter().zip(flags) {
            let new = matched
                .iter()
                .filter(|&&(plane, channel)| !std::mem::replace(&mut flags[[plane, channel]], true))
                .count();
            if new != 0 {
                changed_samples += new;
                updated.insert(row, flags);
                touched.insert(row);
            }
        }
        write_touched_flag_updates(ms, updated, touched, false, changes)?;
    }
    Ok(changed_samples)
}

pub(super) fn collect_time_bin(
    ms: &MeasurementSet,
    members: &[ClipTimedRow],
    block_size: usize,
    columns: &mut ClipColumns,
    ddid_to_spw: &BTreeMap<i32, i32>,
    path: &str,
) -> Result<(ClipTimeAccumulator, FlagSummary), FlaggingError> {
    let mut result = None;
    let mut summary = FlagSummary {
        flagged_samples: 0,
        total_samples: 0,
    };
    let table = ms.main_table();
    for block in members.chunks(block_size) {
        let rows = block.iter().map(|entry| entry.row).collect::<Vec<_>>();
        let ddids = block
            .iter()
            .map(ClipTimedRow::description)
            .collect::<Vec<_>>();
        let flags = read_flags(table, &rows, path)?;
        let cells = columns.read(table, &rows, Some(&ddids), ddid_to_spw, path)?;
        for (slot, (&row, flags)) in rows.iter().zip(&flags).enumerate() {
            summary.total_samples += flags.len();
            summary.flagged_samples += flags.iter().filter(|&&flag| flag).count();
            let data = cells.measurements(slot, flags, row, path)?;
            let model = cells.model(slot, flags, row, path)?;
            let weights = cells.weights(slot, flags, row, path)?;
            let accumulator =
                result.get_or_insert_with(|| ClipTimeAccumulator::new(&data, model, flags));
            if !accumulator.add(&data, model, flags, weights.as_deref(), cells.uses_sigma()) {
                return Err(FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "clip time bin contains inconsistent visibility types or shapes at row {row}"
                    ),
                });
            }
        }
    }
    Ok((
        result.expect("time groups include their first row"),
        summary,
    ))
}

pub(super) fn read_flags(
    table: &Table,
    rows: &[usize],
    path: &str,
) -> Result<Vec<ArrayD<bool>>, FlaggingError> {
    table
        .column_accessor(FLAG_COLUMN)
        .and_then(|column| column.array_cells_owned(rows))
        .map_err(|error| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: error.to_string(),
        })?
        .into_iter()
        .zip(rows.iter())
        .map(|(value, row)| match value {
            Some(ArrayValue::Bool(flags)) if flags.ndim() == 2 => Ok(flags),
            _ => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!("FLAG row {row} must be a defined Boolean matrix"),
            }),
        })
        .collect()
}

/// An open baseline time bin. The number of integrations in it does not
/// change the amount of visibility data retained in memory.
pub(super) struct ClipTimeAccumulator {
    pub(super) flags: ArrayD<bool>,
    pub(super) data: ArrayValue,
    pub(super) model: Option<ArrayValue>,
    pub(super) mass: ArrayD<f32>,
}

impl ClipTimeAccumulator {
    pub(super) fn new(data: &ArrayValue, model: Option<&ArrayValue>, flags: &ArrayD<bool>) -> Self {
        Self {
            flags: ArrayD::from_elem(flags.raw_dim(), true),
            data: zero_samples(data),
            model: model.map(zero_samples),
            mass: ArrayD::zeros(flags.raw_dim()),
        }
    }

    pub(super) fn add(
        &mut self,
        data: &ArrayValue,
        model: Option<&ArrayValue>,
        flags: &ArrayD<bool>,
        weights: Option<&ArrayValue>,
        inverse_sigma: bool,
    ) -> bool {
        if self.flags.shape() != flags.shape() || self.model.is_some() != model.is_some() {
            return false;
        }
        let contributions = ArrayD::from_shape_fn(flags.raw_dim(), |index| {
            let flagged = flags[index.clone()];
            let averaged = &mut self.flags[index.clone()];
            if flagged && !*averaged {
                return None;
            }
            let factor = match weights {
                Some(ArrayValue::Float32(values)) => values[index.clone()],
                Some(ArrayValue::Float64(values)) => values[index.clone()] as f32,
                None => 1.0,
                _ => unreachable!("time averaging weights are floating-point arrays"),
            };
            let factor = if inverse_sigma {
                if factor > f32::MIN_POSITIVE {
                    1.0 / (factor * factor)
                } else {
                    0.0
                }
            } else {
                factor
            };
            let reset = *averaged && !flagged;
            *averaged = flagged;
            let normalization = &mut self.mass[index];
            if reset {
                *normalization = 0.0;
            }
            *normalization += factor;
            Some((factor, reset))
        });
        add_samples(&mut self.data, data, &contributions)
            && self
                .model
                .as_mut()
                .zip(model)
                .is_none_or(|(sum, model)| add_samples(sum, model, &contributions))
    }

    pub(super) fn normalize(&mut self, sum_weights: bool) {
        if !sum_weights {
            divide_samples(&mut self.data, &self.mass);
        }
        if let Some(model) = &mut self.model {
            divide_samples(model, &self.mass);
        }
    }
}

fn zero_samples(value: &ArrayValue) -> ArrayValue {
    match value {
        ArrayValue::Complex32(values) => ArrayValue::Complex32(ArrayD::zeros(values.raw_dim())),
        ArrayValue::Complex64(values) => ArrayValue::Complex64(ArrayD::zeros(values.raw_dim())),
        ArrayValue::Float32(values) => ArrayValue::Float32(ArrayD::zeros(values.raw_dim())),
        ArrayValue::Float64(values) => ArrayValue::Float64(ArrayD::zeros(values.raw_dim())),
        _ => unreachable!("time averaging uses checked numeric samples"),
    }
}

fn add_samples(
    sum: &mut ArrayValue,
    data: &ArrayValue,
    contributions: &ArrayD<Option<(f32, bool)>>,
) -> bool {
    match (sum, data) {
        (ArrayValue::Complex32(sum), ArrayValue::Complex32(data)) => {
            accumulate(sum, data, contributions, |weight| weight)
        }
        (ArrayValue::Complex64(sum), ArrayValue::Complex64(data)) => {
            accumulate(sum, data, contributions, f64::from)
        }
        (ArrayValue::Float32(sum), ArrayValue::Float32(data)) => {
            accumulate(sum, data, contributions, |weight| weight)
        }
        (ArrayValue::Float64(sum), ArrayValue::Float64(data)) => {
            accumulate(sum, data, contributions, f64::from)
        }
        _ => return false,
    }
    true
}

fn accumulate<T, F>(
    sum: &mut ArrayD<T>,
    data: &ArrayD<T>,
    contributions: &ArrayD<Option<(f32, bool)>>,
    convert: impl Fn(f32) -> F,
) where
    T: Copy + Default + AddAssign + Mul<F, Output = T>,
{
    Zip::from(sum)
        .and(data)
        .and(contributions)
        .for_each(|sum, &data, &contribution| {
            if let Some((weight, reset)) = contribution {
                if reset {
                    *sum = T::default();
                }
                *sum += data * convert(weight);
            }
        });
}

fn divide_samples(data: &mut ArrayValue, mass: &ArrayD<f32>) {
    match data {
        ArrayValue::Complex32(data) => divide(data, mass, |weight| weight),
        ArrayValue::Complex64(data) => divide(data, mass, f64::from),
        ArrayValue::Float32(data) => divide(data, mass, |weight| weight),
        ArrayValue::Float64(data) => divide(data, mass, f64::from),
        _ => unreachable!("time averaging uses checked numeric samples"),
    }
}

fn divide<T, F>(data: &mut ArrayD<T>, mass: &ArrayD<f32>, convert: impl Fn(f32) -> F)
where
    T: DivAssign<F>,
{
    Zip::from(data).and(mass).for_each(|data, &weight| {
        if weight > 0.0 {
            *data /= convert(weight);
        }
    });
}
