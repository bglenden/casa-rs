// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-iterator order and time-bin membership for a selected MeasurementSet.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{AddAssign, DivAssign, Mul};

use casa_tables::Table;
use casa_types::{
    ArrayValue, ScalarValue,
    quanta::{Quantity, Unit, casacore::convert},
};
use ndarray::{Array2, ArrayD, Ix2, Zip};

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
    chunk: usize,
    seconds: f64,
}

impl ClipTimedRow {
    #[cfg(test)]
    pub(super) fn integration_fixture_row(
        row: usize,
        seconds: f64,
        [first, second]: [i32; 2],
    ) -> Self {
        Self {
            row,
            group: [0, 0, 0, 0, 0, first, second],
            chunk: 0,
            seconds,
        }
    }

    pub(super) fn description(&self) -> i32 {
        self.group[4]
    }

    pub(super) fn field(&self) -> i32 {
        self.group[3]
    }

    pub(super) fn antennas(&self) -> [i32; 2] {
        [self.group[5], self.group[6]]
    }

    pub(super) fn time(&self) -> f64 {
        self.seconds
    }

    pub(super) fn combine_scans(&mut self) {
        self.group[2] = 0;
    }

    pub(super) fn baseline_key(&self) -> (([i32; 5], usize), [i32; 2]) {
        (self.chunk_key(), self.antennas())
    }

    pub(super) fn chunk_key(&self) -> ([i32; 5], usize) {
        let [observation, array, scan, field, window, _, _] = self.group;
        ([observation, array, scan, field, window], self.chunk)
    }

    pub(super) fn shares_integration(&self, other: &Self) -> bool {
        self.group[..5] == other.group[..5] && self.seconds == other.seconds
    }
}

pub(super) fn time_bin_seconds(text: &str) -> Option<f64> {
    let text = text.split_once('\0').map_or(text, |(head, _)| head);
    let seconds = Unit::new("s").expect("seconds are a defined unit");
    Quantity::read(text)
        .ok()
        .map(|value| {
            convert(&value, Some(&seconds))
                .map_or_else(|_| value.get_si_value(), |time| time.value())
        })
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
    let calibration = super::is_calibration_table(ms);
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
                        chunk: 0,
                        seconds,
                    });
                }
                _ => return Err(fail(format!("TIME row {row} must be a finite Double"))),
            }
        }
        for (slot, name) in GROUP_COLUMNS.into_iter().enumerate() {
            if calibration && name == "ARRAY_ID" {
                for row in &mut timed[begin..] {
                    row.group[slot] = -1;
                }
                continue;
            }
            let name = if calibration && name == "DATA_DESC_ID" {
                "SPECTRAL_WINDOW_ID"
            } else {
                name
            };
            for (row, value) in timed[begin..]
                .iter_mut()
                .zip(load_i32_cells(table, rows, name, path)?)
            {
                row.group[slot] = value;
            }
        }
    }
    Ok(timed)
}

pub(super) fn ordered_baseline_chunks(
    ms: &MeasurementSet,
    selected: &[usize],
    interval: Option<f64>,
    combine_scans: bool,
    path: &str,
) -> Result<Vec<ClipTimedRow>, FlaggingError> {
    let mut timed = read_clip_times(ms, selected, path)?;
    // The calibration iterator fixes its chunks by observation, scan,
    // field and window. MeasurementSet iterator controls apply to MSes.
    let calibration = super::is_calibration_table(ms);
    if !calibration && combine_scans {
        for row in &mut timed {
            row.combine_scans();
        }
    }
    if !calibration
        && let Some(seconds) = interval.filter(|seconds| *seconds > 0.0 && seconds.is_finite())
    {
        mark_time_chunks(&mut timed, seconds);
    }
    timed.sort_unstable_by(|left, right| {
        left.baseline_key()
            .cmp(&right.baseline_key())
            .then_with(|| left.time().total_cmp(&right.time()))
            .then_with(|| left.row.cmp(&right.row))
    });
    Ok(timed)
}

pub(super) fn sort_iterator_rows(rows: &mut [ClipTimedRow]) {
    rows.sort_unstable_by(|left, right| {
        left.group[..5]
            .cmp(&right.group[..5])
            .then_with(|| left.seconds.total_cmp(&right.seconds))
            .then_with(|| left.row.cmp(&right.row))
    });
}

pub(super) fn integration_blocks(
    mut rows: &[ClipTimedRow],
    block_rows: usize,
) -> impl Iterator<Item = &[ClipTimedRow]> {
    std::iter::from_fn(move || {
        if rows.is_empty() {
            return None;
        }
        let mut end = rows.len().min(block_rows.max(1));
        while end < rows.len() && rows[end - 1].shares_integration(&rows[end]) {
            end += 1;
        }
        let (block, rest) = rows.split_at(end);
        rows = rest;
        Some(block)
    })
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
            row.group == first.group
                && row.chunk == first.chunk
                && row.seconds - first.seconds <= limit
        });
        let (group, remaining) = rows.split_at(count);
        rows = remaining;
        Some(group)
    })
}

pub(super) fn mark_time_chunks(rows: &mut [ClipTimedRow], interval: f64) {
    let compare_group =
        |left: &ClipTimedRow, right: &ClipTimedRow| left.group[..5].cmp(&right.group[..5]);
    sort_iterator_rows(rows);
    let mut chunk = 0;
    while let Some(first) = rows.get(chunk) {
        // The input iterator starts each chunk at its actual first timestamp,
        // with MSInterval's centisecond offset to avoid boundary rounding.
        let offset = first.seconds - 0.01;
        let initial_interval = ((first.seconds - offset) / interval).floor();
        let count = 1 + rows[chunk + 1..].partition_point(|row| {
            compare_group(row, first).is_eq()
                && (row.seconds == first.seconds
                    || (interval >= 2.0 * f64::MIN_POSITIVE
                        && ((row.seconds - offset) / interval).floor() == initial_interval))
        });
        for row in &mut rows[chunk..chunk + count] {
            row.chunk = chunk;
        }
        chunk += count;
    }
}

#[derive(Clone, Copy)]
pub(super) struct TimeAveraging<'a> {
    pub(super) seconds: f64,
    pub(super) filter: Option<&'a super::TimeSelection>,
}

impl TimeAveraging<'_> {
    pub(super) fn groups(self, rows: &[ClipTimedRow]) -> impl Iterator<Item = &[ClipTimedRow]> {
        clip_time_groups(rows, self.seconds)
    }

    pub(super) fn holds(&self, rows: &[ClipTimedRow], last_unflagged: Option<f64>) -> bool {
        self.filter.is_none_or(|filter| {
            filter.holds_agent_time(averaged_output_time(rows, last_unflagged))
        })
    }
}

pub(super) fn averaged_output_time(rows: &[ClipTimedRow], last_unflagged: Option<f64>) -> f64 {
    // AveragingTvi2 anchors TIME at the first input center. Once any valid
    // sample arrives, it excludes wholly flagged integrations from the end.
    let start = rows[0].time();
    let end = last_unflagged.unwrap_or_else(|| rows[rows.len() - 1].time());
    start + (end - start) / 2.
}

pub(super) struct TimeClip<'a> {
    pub(super) averaging: TimeAveraging<'a>,
    pub(super) chunk_seconds: Option<f64>,
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
        let mut timed = read_clip_times(ms, selected, self.path)?;
        if columns.uses_weight_spectrum() {
            // Fallback belongs to the first selected row in each original
            // window. Baseline ordering is only for time-bin membership.
            let mut first = BTreeMap::new();
            for row in &timed {
                let description = row.description();
                let spw = ddid_to_spw.get(&description).copied().ok_or_else(|| {
                    FlaggingError::MutateFlags {
                        path: self.path.to_string(),
                        reason: format!("DATA_DESC_ID {description} has no SPW mapping"),
                    }
                })?;
                first.entry(spw).or_insert(row.row);
            }
            columns.prepare_spectrum_fallbacks(ms.main_table(), &first, self.path)?;
        }
        if let Some(seconds) = self
            .chunk_seconds
            .filter(|seconds| *seconds > 0.0 && seconds.is_finite())
        {
            mark_time_chunks(&mut timed, seconds);
        }
        timed.sort_unstable_by(|left, right| {
            left.group
                .cmp(&right.group)
                .then_with(|| left.chunk.cmp(&right.chunk))
                .then_with(|| left.seconds.total_cmp(&right.seconds))
                .then_with(|| left.row.cmp(&right.row))
        });
        let rows_per_block = read_plan.rows_per_block.max(1);
        let mut changes = ChangeSet::default();
        let mut counts = FlagSummary {
            flagged_samples: 0,
            total_samples: 0,
        };
        let mut matched = Vec::new();
        let mut selected = 0;
        for members in self.averaging.groups(&timed) {
            let (average, before, last_unflagged) =
                collect_time_bin(ms, members, rows_per_block, columns, ddid_to_spw, self.path)?;
            if !self.averaging.holds(members, last_unflagged) {
                continue;
            }
            selected += members.len();
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
        changes.selected_rows = self.averaging.filter.map(|_| selected);
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
        let averaged = average.reduced_visibility(self.column, channels, width, row, self.path)?;
        let (flags, channels) = averaged
            .bins
            .as_ref()
            .map_or((averaged.flags, channels), |bins| (bins.flags(), None));
        apply_clip_array(
            &averaged.data,
            flags,
            channels,
            options,
            self.path,
            row,
            matched,
        )?;
        let original = matched.iter().flat_map(|(plane, channel)| {
            let channels = averaged
                .bins
                .as_ref()
                .map_or(std::slice::from_ref(channel), |bins| {
                    bins.original_channels(*channel)
                });
            channels.iter().map(move |&original| (*plane, original))
        });
        Ok(original.collect())
    }
}

pub(super) struct TimeAveragedVisibility<'a> {
    pub(super) data: Cow<'a, ArrayValue>,
    pub(super) flags: &'a ArrayD<bool>,
    pub(super) bins: Option<ChannelBins<'a>>,
    pub(super) fourier_roundoff: Option<&'a ArrayD<f32>>,
}

impl ClipTimeAccumulator {
    pub(super) fn reduced_visibility<'a>(
        &'a mut self,
        column: FlagDataColumn,
        channels: Option<&'a [usize]>,
        width: u32,
        row: usize,
        path: &str,
    ) -> Result<TimeAveragedVisibility<'a>, FlaggingError> {
        let sum_weights = matches!(
            column,
            FlagDataColumn::Weight | FlagDataColumn::WeightSpectrum
        );
        self.normalize(sum_weights);
        let bins = channels.and_then(|indices| ChannelBins::new(width, indices, &self.flags));
        let mut data = Cow::Borrowed(&self.data);
        let mut model = self.model.as_ref().map(Cow::Borrowed);
        if let Some(bins) = &bins {
            let stats = ArrayValue::Float32(std::mem::take(&mut self.mass));
            let reduction = if sum_weights {
                ChannelReduction::Sum
            } else if column == FlagDataColumn::ModelData {
                ChannelReduction::Mean
            } else {
                ChannelReduction::Weighted {
                    spectrum: &stats,
                    inverse_sigma: false,
                }
            };
            data = Cow::Owned(bins.average(&data, &self.flags, reduction));
            model = model
                .map(|model| Cow::Owned(bins.average(&model, &self.flags, ChannelReduction::Mean)));
        }
        if let Some(model) = model {
            data = Cow::Owned(subtract_model(&data, &model, row, path)?);
        }
        Ok(TimeAveragedVisibility {
            data,
            flags: &self.flags,
            bins,
            fourier_roundoff: self.fourier_roundoff.as_ref(),
        })
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
        let mut partial = BTreeMap::new();
        let mut complete = BTreeMap::new();
        for (row, mut flags) in rows.into_iter().zip(flags) {
            let new = matched
                .iter()
                .filter(|&&(plane, channel)| !std::mem::replace(&mut flags[[plane, channel]], true))
                .count();
            if new != 0 {
                changed_samples += new;
                // The row flag covers the original MS axes, including all
                // samples outside the channels and planes used for clipping.
                if flags.iter().all(|flag| *flag) {
                    complete.insert(row, flags);
                } else {
                    partial.insert(row, flags);
                }
            }
        }
        for (updates, set_row_flag) in [(partial, false), (complete, true)] {
            if !updates.is_empty() {
                let touched: BTreeSet<_> = updates.keys().copied().collect();
                write_touched_flag_updates(ms, updates, touched, |_| set_row_flag, changes)?;
            }
        }
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
) -> Result<(ClipTimeAccumulator, FlagSummary, Option<f64>), FlaggingError> {
    let mut result = None;
    let mut last_unflagged = None;
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
        let cells = columns.read(ms, &rows, &flags, Some(&ddids), ddid_to_spw, path)?;
        for (slot, (&row, flags)) in rows.iter().zip(&flags).enumerate() {
            summary.total_samples += flags.len();
            let flagged = flags.iter().filter(|&&flag| flag).count();
            summary.flagged_samples += flagged;
            if flagged < flags.len() {
                last_unflagged = Some(block[slot].time());
            }
            let data = cells.measurements(slot, flags, row, path)?;
            let model = cells.model(slot, flags, row, path)?;
            let weights = cells.weights(slot, flags, row, path)?;
            let roundoff = cells.fourier_residual_roundoff(slot, flags, row, path)?;
            let accumulator =
                result.get_or_insert_with(|| ClipTimeAccumulator::new(&data, model, flags));
            if !accumulator.add(
                &data,
                model,
                flags,
                weights.as_deref(),
                roundoff.as_ref(),
                cells.uses_sigma(),
            ) {
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
        last_unflagged,
    ))
}

pub(super) fn read_flags(
    table: &Table,
    rows: &[usize],
    path: &str,
) -> Result<Vec<ArrayD<bool>>, FlaggingError> {
    read_flag_cells(table, rows, path, |column, rows| {
        column.array_cells_owned(rows)
    })
}

pub(super) fn read_flags_uncached(
    table: &Table,
    rows: &[usize],
    path: &str,
) -> Result<Vec<ArrayD<bool>>, FlaggingError> {
    read_flag_cells(table, rows, path, |column, rows| {
        column.array_cells_owned_uncached(rows)
    })
}

fn read_flag_cells(
    table: &Table,
    rows: &[usize],
    path: &str,
    read: impl FnOnce(
        &casa_tables::TableColumn<'_>,
        &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, casa_tables::TableError>,
) -> Result<Vec<ArrayD<bool>>, FlaggingError> {
    table
        .column_accessor(FLAG_COLUMN)
        .and_then(|column| read(&column, rows))
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
    fourier_roundoff: Option<ArrayD<f32>>,
    started: bool,
}

impl ClipTimeAccumulator {
    pub(super) fn new(data: &ArrayValue, model: Option<&ArrayValue>, flags: &ArrayD<bool>) -> Self {
        Self {
            flags: ArrayD::from_elem(flags.raw_dim(), true),
            data: zero_samples(data),
            model: model.map(zero_samples),
            mass: ArrayD::zeros(flags.raw_dim()),
            fourier_roundoff: None,
            started: false,
        }
    }

    pub(super) fn add(
        &mut self,
        data: &ArrayValue,
        model: Option<&ArrayValue>,
        flags: &ArrayD<bool>,
        weights: Option<&ArrayValue>,
        roundoff: Option<&ArrayD<f32>>,
        inverse_sigma: bool,
    ) -> bool {
        if self.flags.shape() != flags.shape()
            || self.model.is_some() != model.is_some()
            || roundoff.is_some_and(|bound| bound.shape() != flags.shape())
        {
            return false;
        }
        let Ok(flags) = flags.view().into_dimensionality::<Ix2>() else {
            return false;
        };
        let mut accumulated_flags = self
            .flags
            .view_mut()
            .into_dimensionality::<Ix2>()
            .expect("the time bin has the FLAG matrix shape");
        let mut mass = self
            .mass
            .view_mut()
            .into_dimensionality::<Ix2>()
            .expect("the time bin has the FLAG matrix shape");
        let weights = weights.and_then(|spectrum| {
            ChannelReduction::Weighted {
                spectrum,
                inverse_sigma,
            }
            .weights()
        });
        let first_row = !std::mem::replace(&mut self.started, true);
        let contributions = Array2::from_shape_fn(flags.dim(), |index| {
            let flagged = flags[index];
            let averaged = &mut accumulated_flags[index];
            if flagged && !*averaged {
                return None;
            }
            let factor = weights
                .as_ref()
                .map_or(1.0, |weights| weights.get(index) as f32);
            let reset = first_row || (*averaged && !flagged);
            *averaged = flagged;
            let normalization = &mut mass[index];
            if reset {
                *normalization = 0.0;
            }
            *normalization += factor;
            Some((factor, reset))
        })
        .into_dyn();
        if roundoff.is_some() && self.fourier_roundoff.is_none() {
            self.fourier_roundoff = Some(Array2::zeros(flags.raw_dim()).into_dyn());
        }
        if let Some(accumulated) = &mut self.fourier_roundoff {
            match roundoff {
                Some(source) => {
                    Zip::from(accumulated)
                        .and(source)
                        .and(&contributions)
                        .for_each(|acc, &value, &contribution| {
                            if let Some((weight, reset)) = contribution {
                                let bound = if weight > 0.0 { value } else { 0.0 };
                                if reset {
                                    *acc = bound;
                                } else {
                                    *acc = acc.max(bound);
                                }
                            }
                        });
                }
                None => {
                    Zip::from(accumulated)
                        .and(&contributions)
                        .for_each(|acc, &contribution| {
                            if let Some((_, true)) = contribution {
                                *acc = 0.0;
                            }
                        });
                }
            }
        }
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
    T: Copy + AddAssign + Mul<F, Output = T>,
{
    Zip::from(sum)
        .and(data)
        .and(contributions)
        .for_each(|sum, &data, &contribution| {
            if let Some((weight, reset)) = contribution {
                let data = data * convert(weight);
                if reset {
                    *sum = data;
                } else {
                    *sum += data;
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integration_blocks_keep_physical_order_across_reads() {
        let mut rows: Vec<_> = [
            (7, 20.0),
            (2, 10.0),
            (3, 20.0),
            (1, 30.0),
            (0, 10.0),
            (6, 20.0),
        ]
        .into_iter()
        .map(|(row, seconds)| ClipTimedRow {
            row,
            seconds,
            group: [0; GROUP_COLUMNS.len()],
            chunk: 0,
        })
        .collect();
        sort_iterator_rows(&mut rows);
        let physical_reads: Vec<_> = integration_blocks(&rows, 2)
            .map(|block| {
                block
                    .chunks(2)
                    .map(|batch| batch.iter().map(|row| row.row).collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            physical_reads,
            [vec![vec![0, 2]], vec![vec![3, 6], vec![7]], vec![vec![1]]]
        );
    }

    #[test]
    fn time_bin_seconds_truncates_at_nul() {
        assert_eq!(time_bin_seconds("20s\0ignored"), Some(20.0));
        assert_eq!(time_bin_seconds("0.5min\0ignored"), Some(30.0));
        assert_eq!(time_bin_seconds("garbage"), None);
    }

    #[test]
    fn fourier_roundoff_excludes_preflagged_and_zero_weight_members() {
        use casa_types::Complex32;
        let ones_data = |val: f32| {
            ArrayValue::Complex32(Array2::from_elem((1, 5), Complex32::new(val, 0.0)).into_dyn())
        };
        let flags_for =
            |flagged: [bool; 5]| Array2::from_shape_fn((1, 5), |(_, ch)| flagged[ch]).into_dyn();
        let roundoff_for =
            |vals: [f32; 5]| Array2::from_shape_fn((1, 5), |(_, ch)| vals[ch]).into_dyn();
        let sigma_for = |vals: [f32; 5]| {
            ArrayValue::Float32(Array2::from_shape_fn((1, 5), |(_, ch)| vals[ch]).into_dyn())
        };

        let d0 = ones_data(1.0);
        let f0 = flags_for([true, false, false, false, true]);
        let r0 = roundoff_for([1e6, 1e-5, 1e-5, 1e-5, 2e-5]);
        let s0 = sigma_for([1.0, 1.0, 1.0, 1.0, 1.0]);
        let mut acc = ClipTimeAccumulator::new(&d0, Some(&d0), &f0);
        assert!(acc.add(&d0, Some(&d0), &f0, Some(&s0), Some(&r0), true));

        let f1 = flags_for([false, true, false, false, true]);
        let r1 = roundoff_for([1e-5, 1e6, 3e-5, 1e6, 5e-5]);
        // Channel 3 gets zero inverse-variance weight because 1e20^2 overflows f32 / underflows to 0.
        let s1 = sigma_for([1.0, 1.0, 1.0, 1e20, 1.0]);
        assert!(acc.add(&d0, Some(&d0), &f1, Some(&s1), Some(&r1), true));

        let combined = acc
            .reduced_visibility(FlagDataColumn::ResidualData, None, 0, 0, "test")
            .expect("reduced visibility");
        let out = combined.fourier_roundoff.expect("fourier roundoff");
        // ch 0: flagged (1e6) then unflagged (1e-5) -> reset to 1e-5
        // ch 1: unflagged (1e-5) then flagged (1e6) -> flagged ignored, stays 1e-5
        // ch 2: unflagged (1e-5) then unflagged (3e-5) -> max 3e-5
        // ch 3: unflagged (1e-5, w=1) then unflagged (1e6, w=0) -> zero-weight masked, stays 1e-5
        // ch 4: all flagged -> max(2e-5, 5e-5) = 5e-5
        let expected = [1e-5, 1e-5, 3e-5, 1e-5, 5e-5];
        for (ch, want) in expected.into_iter().enumerate() {
            assert_eq!(out[[0, ch]], want, "channel {ch}");
        }
    }

    #[test]
    fn combined_time_and_channel_average_preserves_exact_inverse_variance_weights() {
        use casa_types::Complex32;
        let data = |c0: f32, c1: f32| {
            ArrayValue::Complex32(
                Array2::from_shape_fn((1, 2), |(_, ch)| {
                    Complex32::new(if ch == 0 { c0 } else { c1 }, 0.0)
                })
                .into_dyn(),
            )
        };
        let sigma = |s0: f32, s1: f32| {
            ArrayValue::Float32(
                Array2::from_shape_fn((1, 2), |(_, ch)| if ch == 0 { s0 } else { s1 }).into_dyn(),
            )
        };
        let unflagged = Array2::from_elem((1, 2), false).into_dyn();
        let mut acc = ClipTimeAccumulator::new(&data(0.0, 0.0), None, &unflagged);
        assert!(acc.add(
            &data(11.0, -1.0),
            None,
            &unflagged,
            Some(&sigma(1.0, 1.0)),
            None,
            true,
        ));
        assert!(acc.add(
            &data(1.0, -1.0),
            None,
            &unflagged,
            Some(&sigma(1.0, 0.5)),
            None,
            true,
        ));
        let combined = acc
            .reduced_visibility(FlagDataColumn::Data, Some(&[0, 1]), 2, 0, "test")
            .expect("reduced visibility");
        let ArrayValue::Complex32(values) = combined.data.as_ref() else {
            panic!("expected Complex32 output");
        };
        assert_eq!(values.as_slice(), Some(&[Complex32::new(1.0, 0.0)][..]));
    }
}
