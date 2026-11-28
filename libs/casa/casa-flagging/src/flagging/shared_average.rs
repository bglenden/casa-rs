// SPDX-License-Identifier: LGPL-3.0-or-later
//! The visibility iterator and source coordinates shared by averaged list agents.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use casa_ms::selection::{UvDistanceSelection, load_uvw_column};

use super::clip_average::{ChannelBins, bin_members, check_channel_bins};
use super::clip_time::{
    ClipTimedRow, averaged_output_time, clip_time_groups, ordered_baseline_chunks, read_flags,
    time_bin_seconds,
};
use super::*;

pub(super) struct SharedAveraging<'a> {
    pub(super) seconds: Option<f64>,
    pub(super) widths: Option<&'a [u32]>,
    uvw_weights: Option<UvwWeights>,
}

#[derive(Clone, Copy)]
enum UvwWeights {
    Unit,
    Weight,
    Sigma,
}

impl<'a> SharedAveraging<'a> {
    pub(super) fn from_requests(
        requests: impl DoubleEndedIterator<Item = &'a FlagDataRequest> + Clone,
    ) -> Option<Self> {
        let automatic = |request: &&FlagDataRequest| {
            matches!(
                request.mode,
                FlagDataMode::Clip | FlagDataMode::Tfcrop | FlagDataMode::Rflag
            )
        };
        if requests.clone().any(|request| {
            !request.casa_row_selection
                || (!automatic(&request)
                    && !matches!(request.mode, FlagDataMode::Extend | FlagDataMode::Antint))
        }) {
            return None;
        }
        if requests.clone().take(2).count() < 2 {
            return None;
        }
        let uvw_selected = requests.clone().any(|request| {
            request.selection.selectdata
                && request
                    .selection
                    .uvrange
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
        });
        let automatic = requests.filter(automatic);
        // Each enabled agent sets the corresponding layer on the shared
        // handler. The last enabled agent determines that layer's bin size.
        let seconds = automatic
            .clone()
            .rev()
            .find_map(|request| request.clip_time_bin.as_deref().and_then(time_bin_seconds));
        let widths = automatic
            .clone()
            .rev()
            .find_map(|request| request.clip_channel_bins.as_deref());
        let uvw_weights = (seconds.is_some() && uvw_selected).then(|| {
            let time_columns = automatic
                .filter(|request| {
                    request
                        .clip_time_bin
                        .as_deref()
                        .and_then(time_bin_seconds)
                        .is_some()
                })
                .map(|request| request.data_column);
            if time_columns.clone().any(|column| {
                matches!(
                    column,
                    FlagDataColumn::CorrectedData | FlagDataColumn::Residual
                )
            }) {
                UvwWeights::Weight
            } else if time_columns
                .clone()
                .any(|column| matches!(column, FlagDataColumn::Data | FlagDataColumn::FloatData))
            {
                UvwWeights::Sigma
            } else {
                UvwWeights::Unit
            }
        });
        (seconds.is_some() || widths.is_some()).then_some(Self {
            seconds,
            widths,
            uvw_weights,
        })
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct SharedAgentSelection<'a> {
    pub(super) rows: Option<&'a [usize]>,
    pub(super) channels: Option<&'a BTreeMap<i32, ChannelSelection>>,
    pub(super) correlations: Option<&'a CorrelationSelection>,
    pub(super) time: Option<&'a TimeSelection>,
    pub(super) uv: Option<&'a UvDistanceSelection>,
}

impl SharedAgentSelection<'_> {
    fn holds(&self, row: &SharedFlagRow) -> bool {
        self.rows
            .is_none_or(|rows| rows.binary_search(&row.row()).is_ok())
            && self
                .time
                .is_none_or(|filter| filter.holds_agent_time(row.time))
            && self.uv.is_none_or(|filter| {
                filter.holds(
                    row.uvw.expect("averaged UV coordinates have been prepared"),
                    Some(row.members[0].description()),
                )
            })
    }
}

pub(super) struct SharedFlagView {
    cancellation: Option<Cancellation>,
    input_rows: Vec<usize>,
    rows: Vec<SharedFlagRow>,
    flags: BTreeMap<usize, ArrayD<bool>>,
    ddid_to_spw: BTreeMap<i32, i32>,
    channel_layouts: BTreeMap<(i32, usize), Arc<[usize]>>,
    columns: Vec<(FlagDataColumn, Vec<SharedVisibility>)>,
    staged: BTreeMap<usize, ArrayD<bool>>,
    averaged_in_time: bool,
}

struct SharedVisibility {
    data: ArrayValue,
    roundoff: Option<ArrayD<f32>>,
}

struct SharedFlagRow {
    members: Vec<ClipTimedRow>,
    time: f64,
    uvw: Option<[f64; 3]>,
    spw: i32,
    width: u32,
    channels: Arc<[usize]>,
    original_flags: ArrayD<bool>,
    current_flags: ArrayD<bool>,
}

struct AveragedUvw {
    coordinates: Vec<[f64; 3]>,
    weights: Vec<f64>,
}

impl AveragedUvw {
    fn read(
        ms: &MeasurementSet,
        rows: &[usize],
        source: UvwWeights,
        path: &str,
    ) -> Result<Self, FlaggingError> {
        let table = ms.main_table();
        let error = |reason: String| FlaggingError::MutateFlags {
            path: path.to_owned(),
            reason,
        };
        let coordinates =
            load_uvw_column(table, rows).map_err(|source| error(source.to_string()))?;
        let weights = match source {
            UvwWeights::Unit => Vec::new(),
            UvwWeights::Weight | UvwWeights::Sigma => {
                let use_sigma = matches!(source, UvwWeights::Sigma);
                let name = if use_sigma { "SIGMA" } else { "WEIGHT" };
                table
                    .column_accessor(name)
                    .and_then(|column| column.array_cells_owned(rows))
                    .map_err(|source| error(source.to_string()))?
                    .into_iter()
                    .zip(rows)
                    .map(|(cell, row)| {
                        let Some(ArrayValue::Float32(cell)) = cell else {
                            return Err(error(format!("{name} row {row} must be a Float array")));
                        };
                        Ok(cell
                            .iter()
                            .copied()
                            .map(|value| {
                                f64::from(if use_sigma {
                                    if value > f32::MIN_POSITIVE {
                                        (1.0 / f64::from(value * value)) as f32
                                    } else {
                                        0.0
                                    }
                                } else {
                                    value
                                })
                            })
                            .sum())
                    })
                    .collect::<Result<_, _>>()?
            }
        };
        Ok(Self {
            coordinates,
            weights,
        })
    }

    fn reduce(&self, members: &[ClipTimedRow], flags: &BTreeMap<usize, ArrayD<bool>>) -> [f64; 3] {
        let valid = |row: usize| flags[&row].iter().any(|&flag| !flag);
        let all_flagged = !members.iter().any(|member| valid(member.row));
        let mut count = 0;
        let mut total = 0.0;
        let mut sum = [0.0; 3];
        for member in members {
            if all_flagged || valid(member.row) {
                let weight = if all_flagged {
                    1.0
                } else {
                    self.weights.get(member.row).copied().unwrap_or(1.0)
                };
                count += 1;
                total += weight;
                for (sum, coordinate) in sum.iter_mut().zip(self.coordinates[member.row]) {
                    *sum += coordinate * weight;
                }
            }
        }
        let normalization = if total == 0.0 {
            f64::from(count)
        } else {
            total
        };
        sum.map(|coordinate| coordinate / normalization)
    }
}

impl SharedFlagView {
    pub(super) fn new(
        ms: &MeasurementSet,
        config: SharedAveraging<'_>,
        time_chunk: Option<f64>,
        combinescans: bool,
        path: &str,
        cancellation: Option<&Cancellation>,
    ) -> Result<Self, FlaggingError> {
        if let Some(widths) = config.widths {
            check_channel_bins(ms, widths, path)?;
        }
        let selected = (0..ms.row_count()).collect::<Vec<_>>();
        let ddid_to_spw = data_description_spw_map(ms)?;
        let timed =
            ordered_baseline_chunks(ms, &selected, time_chunk, combinescans, path, cancellation)?;
        check_cancelled(cancellation)?;
        let flags = selected
            .iter()
            .copied()
            .zip(read_flags(ms.main_table(), &selected, path)?)
            .collect::<BTreeMap<_, _>>();
        let uvw = config
            .uvw_weights
            .map(|source| AveragedUvw::read(ms, &selected, source, path))
            .transpose()?;
        let mut view = Self {
            cancellation: cancellation.cloned(),
            input_rows: selected,
            rows: Vec::new(),
            flags,
            ddid_to_spw,
            channel_layouts: BTreeMap::new(),
            columns: Vec::new(),
            staged: BTreeMap::new(),
            averaged_in_time: config.seconds.is_some(),
        };
        if let Some(seconds) = config.seconds {
            for members in clip_time_groups(&timed, seconds) {
                check_cancelled(cancellation)?;
                view.add_row(members, config.widths, uvw.as_ref(), path)?;
            }
        } else {
            for row in &timed {
                check_cancelled(cancellation)?;
                view.add_row(std::slice::from_ref(row), config.widths, uvw.as_ref(), path)?;
            }
        }
        Ok(view)
    }

    pub(super) fn input_rows(&self) -> &[usize] {
        &self.input_rows
    }

    fn add_row(
        &mut self,
        members: &[ClipTimedRow],
        widths: Option<&[u32]>,
        uvw: Option<&AveragedUvw>,
        path: &str,
    ) -> Result<(), FlaggingError> {
        let first = &members[0];
        let ddid = first.description();
        let spw =
            self.ddid_to_spw
                .get(&ddid)
                .copied()
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: path.to_owned(),
                    reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
                })?;
        let [_, nchan] = *self.flags[&first.row].shape() else {
            return Err(FlaggingError::MutateFlags {
                path: path.to_owned(),
                reason: format!("FLAG row {} must be rank-2", first.row),
            });
        };
        let (_, width) =
            clip_channel_selection(Some(ddid), &self.ddid_to_spw, None, widths, nchan, path)?;
        let channels = self
            .channel_layouts
            .entry((spw, nchan))
            .or_insert_with(|| (0..nchan).collect::<Vec<_>>().into())
            .clone();
        let raw_flags = time_bin_flags(members, &self.flags, path)?;
        let original_flags = ChannelBins::new(width, &channels, &raw_flags)
            .map_or_else(|| raw_flags.clone(), |bins| bins.flags().clone());
        let last_unflagged = members
            .iter()
            .rfind(|entry| self.flags[&entry.row].iter().any(|&flag| !flag))
            .map(ClipTimedRow::time);
        let time = averaged_output_time(members, last_unflagged);
        self.rows.push(SharedFlagRow {
            members: members.to_vec(),
            time,
            uvw: uvw.map(|uvw| uvw.reduce(members, &self.flags)),
            spw,
            width,
            channels,
            current_flags: original_flags.clone(),
            original_flags,
        });
        Ok(())
    }
}

impl SharedFlagRow {
    fn row(&self) -> usize {
        self.members[0].row
    }

    fn physical_channels(&self, channel: usize) -> &[usize] {
        let width = match self.width {
            1 => self.channels.len(),
            0 => 1,
            width => width as usize,
        };
        bin_members(&self.channels, width, channel)
    }
}

impl SharedFlagView {
    fn averaged_column(
        &self,
        ms: &MeasurementSet,
        column: FlagDataColumn,
        columns: &mut ClipColumns,
        path: &str,
    ) -> Result<Vec<SharedVisibility>, FlaggingError> {
        use super::clip_time::ClipTimeAccumulator;

        let input = &self.flags;
        if columns.uses_weight_spectrum() {
            let mut first = BTreeMap::<i32, usize>::new();
            for group in &self.rows {
                for row in &group.members {
                    first
                        .entry(group.spw)
                        .and_modify(|value| *value = (*value).min(row.row))
                        .or_insert(row.row);
                }
            }
            columns.prepare_spectrum_fallbacks(ms.main_table(), &first, path)?;
        }
        let bytes = columns.bytes_per_sample(ms.main_table(), path)?;
        let capacity = flag_read_plan(ms, input.len(), bytes, true)?
            .rows_per_block
            .max(1);
        let mut output = Vec::with_capacity(self.rows.len());
        let mut groups = self.rows.as_slice();
        while let Some((first, tail)) = groups.split_first() {
            check_cancelled(self.cancellation.as_ref())?;
            let mut rows = first.members.len();
            let mut end = 1;
            for next in tail {
                if rows.saturating_add(next.members.len()) > capacity {
                    break;
                }
                rows += next.members.len();
                end += 1;
            }
            let (block, remaining) = groups.split_at(end);
            // Read selected operands in bounded batches. A group wider than
            // the budget is accumulated in pieces below.
            if rows > capacity {
                let mut accumulator = None;
                for members in first.members.chunks(capacity) {
                    check_cancelled(self.cancellation.as_ref())?;
                    let batch = self.input_batch(members.iter());
                    let values = columns.read(
                        ms,
                        &batch.rows,
                        &batch.flags,
                        Some(&batch.ddids),
                        &self.ddid_to_spw,
                        path,
                    )?;
                    add_averaged_values(&mut accumulator, &values, &batch, 0..members.len(), path)?;
                }
                output.push(first.reduce(
                    accumulator.expect("nonempty input group"),
                    column,
                    path,
                )?);
            } else {
                let batch = self.input_batch(block.iter().flat_map(|group| group.members.iter()));
                let values = columns.read(
                    ms,
                    &batch.rows,
                    &batch.flags,
                    Some(&batch.ddids),
                    &self.ddid_to_spw,
                    path,
                )?;
                let mut slot = 0;
                for group in block {
                    if self.averaged_in_time {
                        let end = slot + group.members.len();
                        let mut accumulator: Option<ClipTimeAccumulator> = None;
                        add_averaged_values(&mut accumulator, &values, &batch, slot..end, path)?;
                        output.push(group.reduce(
                            accumulator.expect("nonempty input group"),
                            column,
                            path,
                        )?);
                        slot = end;
                    } else {
                        let flags = &batch.flags[slot];
                        let bins = ChannelBins::new(group.width, &group.channels, flags);
                        let roundoff =
                            values.fourier_residual_roundoff(slot, flags, group.row(), path)?;
                        output.push(SharedVisibility {
                            data: values
                                .row(slot, flags, bins.as_ref(), group.row(), path)?
                                .into_owned(),
                            roundoff: reduce_roundoff(roundoff.as_ref(), flags, bins.as_ref()),
                        });
                        slot += 1;
                    }
                }
            }
            groups = remaining;
        }
        Ok(output)
    }

    fn input_batch<'a>(&self, members: impl Iterator<Item = &'a ClipTimedRow>) -> InputBatch {
        let mut batch = InputBatch {
            rows: Vec::new(),
            ddids: Vec::new(),
            flags: Vec::new(),
        };
        for member in members {
            batch.rows.push(member.row);
            batch.ddids.push(member.description());
            batch.flags.push(self.flags[&member.row].clone());
        }
        batch
    }
}

struct InputBatch {
    rows: Vec<usize>,
    ddids: Vec<i32>,
    flags: Vec<ArrayD<bool>>,
}

fn add_averaged_values(
    accumulator: &mut Option<super::clip_time::ClipTimeAccumulator>,
    values: &super::clip_columns::ClipColumnCells<'_>,
    batch: &InputBatch,
    slots: std::ops::Range<usize>,
    path: &str,
) -> Result<(), FlaggingError> {
    for slot in slots {
        let row = batch.rows[slot];
        let flags = &batch.flags[slot];
        let data = values.measurements(slot, flags, row, path)?;
        let model = values.model(slot, flags, row, path)?;
        let weights = values.weights(slot, flags, row, path)?;
        let roundoff = values.fourier_residual_roundoff(slot, flags, row, path)?;
        let acc = accumulator
            .get_or_insert_with(|| super::clip_time::ClipTimeAccumulator::new(&data, model, flags));
        if !acc.add(
            &data,
            model,
            flags,
            weights.as_deref(),
            roundoff.as_ref(),
            values.uses_sigma(),
        ) {
            return Err(FlaggingError::MutateFlags {
                path: path.to_owned(),
                reason: format!("inconsistent data shape in time bin at row {row}"),
            });
        }
    }
    Ok(())
}

impl SharedFlagRow {
    fn reduce(
        &self,
        mut accumulator: super::clip_time::ClipTimeAccumulator,
        column: FlagDataColumn,
        path: &str,
    ) -> Result<SharedVisibility, FlaggingError> {
        let reduced = accumulator.reduced_visibility(
            column,
            Some(&self.channels),
            self.width,
            self.row(),
            path,
        )?;
        Ok(SharedVisibility {
            data: reduced.data.into_owned(),
            roundoff: reduce_roundoff(
                reduced.fourier_roundoff,
                reduced.flags,
                reduced.bins.as_ref(),
            ),
        })
    }
}

fn reduce_roundoff(
    roundoff: Option<&ArrayD<f32>>,
    flags: &ArrayD<bool>,
    bins: Option<&ChannelBins<'_>>,
) -> Option<ArrayD<f32>> {
    let roundoff = roundoff?;
    let Some(bins) = bins else {
        return Some(roundoff.clone());
    };
    let averaged_flags = bins.flags();
    Some(ArrayD::from_shape_fn(averaged_flags.raw_dim(), |index| {
        let (corr, bin) = (index[0], index[1]);
        bins.original_channels(bin)
            .iter()
            .filter(|&&chan| flags[[corr, chan]] == averaged_flags[[corr, bin]])
            .map(|&chan| roundoff[[corr, chan]])
            .fold(0.0, f32::max)
    }))
}

impl SharedFlagView {
    pub(super) fn prepare_column(
        &mut self,
        ms: &MeasurementSet,
        column: FlagDataColumn,
        columns: &mut ClipColumns,
        path: &str,
    ) -> Result<(), FlaggingError> {
        if self.columns.iter().any(|(loaded, _)| *loaded == column) {
            return Ok(());
        }
        let values = self.averaged_column(ms, column, columns, path)?;
        self.columns.push((column, values));
        Ok(())
    }

    pub(super) fn load_samples(
        &self,
        selection: SharedAgentSelection<'_>,
        fit: Option<FlagDataColumn>,
        request: &FlagDataRequest,
        path: &str,
    ) -> Result<LoadedSamples, FlaggingError> {
        let values = fit.and_then(|column| self.columns.iter().find(|(key, _)| *key == column));
        let select_channels = request
            .clip_channel_bins
            .is_none()
            .then_some(selection.channels)
            .flatten();
        let mut samples = Vec::new();
        let mut model_roundoff = Vec::new();
        for (index, group) in self.rows.iter().enumerate() {
            check_cancelled(self.cancellation.as_ref())?;
            if !selection.holds(group) {
                continue;
            }
            let channels = group.select_channels(select_channels, path)?;
            let first = &group.members[0];
            let [ant1, ant2] = first.antennas();
            let metadata = RowSampleMetadata {
                row: index,
                field: first.field(),
                spw: group.spw,
                scan: first.chunk_key().0[2],
                ant1,
                ant2,
            };
            if let Some((column, values)) = values {
                let visibility = &values[index];
                let flags = if request.mode == FlagDataMode::Rflag {
                    &group.original_flags
                } else {
                    &group.current_flags
                };
                let first_sample = samples.len();
                push_row_samples(
                    &visibility.data,
                    flags,
                    &channels,
                    metadata,
                    path,
                    column.name(),
                    &mut samples,
                )?;
                if let Some(roundoff) = &visibility.roundoff {
                    model_roundoff.resize(first_sample, 0.0);
                    model_roundoff.extend(
                        samples[first_sample..]
                            .iter()
                            .map(|sample| roundoff[[sample.corr, sample.chan]]),
                    );
                }
            } else {
                push_flag_samples(
                    &group.current_flags,
                    &channels,
                    selection
                        .correlations
                        .map(|corr| corr.planes(first.description())),
                    metadata,
                    &mut samples,
                );
            }
        }
        Ok(LoadedSamples {
            samples,
            flags_by_row: BTreeMap::new(),
            model_roundoff,
            axes: AveragedSampleAxes::default(),
        })
    }

    pub(super) fn groups(&self, samples: &[Sample]) -> Vec<(AutoflagChunk, Vec<Sample>)> {
        let baseline_key = |sample: &Sample| self.rows[sample.row].members[0].baseline_key();
        samples
            .chunk_by(|a, b| baseline_key(a) == baseline_key(b))
            .map(|group| (baseline_key(&group[0]).0, group.to_vec()))
            .collect()
    }

    pub(super) fn averaged_in_time(&self) -> bool {
        self.averaged_in_time
    }

    pub(super) fn stage(&mut self, samples: &[Sample], matched: &[bool], flag: bool) -> ChangeSet {
        let mut changed = ChangeSet::default();
        let mut touched = BTreeSet::new();
        for (sample, _) in samples.iter().zip(matched).filter(|(_, matched)| **matched) {
            let row = &mut self.rows[sample.row];
            let target = &mut row.current_flags[[sample.corr, sample.chan]];
            if *target == flag {
                continue;
            }
            *target = flag;
            for member in &row.members {
                let flags = self
                    .staged
                    .entry(member.row)
                    .or_insert_with(|| self.flags[&member.row].clone());
                for &chan in row.physical_channels(sample.chan) {
                    let cell = &mut flags[[sample.corr, chan]];
                    if *cell != flag {
                        changed.changed_samples += 1;
                        *cell = flag;
                        touched.insert(member.row);
                    }
                }
            }
        }
        changed.changed_rows = touched.len();
        changed.changed_row_indices = touched.into_iter().collect();
        changed
    }

    pub(super) fn summarize(&self, selection: SharedAgentSelection<'_>) -> (usize, FlagSummary) {
        let rows = self.rows.iter().filter(|row| selection.holds(row));
        let mut selected_rows = 0;
        let mut counts = FlagSummary {
            flagged_samples: 0,
            total_samples: 0,
        };
        for member in rows.flat_map(|row| &row.members) {
            let flags = self
                .staged
                .get(&member.row)
                .unwrap_or(&self.flags[&member.row]);
            selected_rows += 1;
            counts.total_samples += flags.len();
            counts.flagged_samples += flags.iter().filter(|&&flag| flag).count();
        }
        (selected_rows, counts)
    }

    pub(super) fn flush(self, ms: &mut MeasurementSet) -> Result<ChangeSet, FlaggingError> {
        let full_rows = if self.averaged_in_time {
            self.rows
                .iter()
                .filter(|row| row.current_flags.iter().all(|&flag| flag))
                .flat_map(|row| row.members.iter().map(|member| member.row))
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };
        let staged = self
            .staged
            .into_iter()
            .filter(|(row, flags)| *flags != self.flags[row])
            .collect::<BTreeMap<_, _>>();
        let touched = staged.keys().copied().collect();
        let mut changes = ChangeSet::default();
        write_touched_flag_updates(
            ms,
            staged,
            touched,
            |row| full_rows.contains(&row),
            &mut changes,
        )?;
        Ok(changes)
    }
}

impl SharedFlagRow {
    fn select_channels(
        &self,
        selections: Option<&BTreeMap<i32, ChannelSelection>>,
        path: &str,
    ) -> Result<Vec<usize>, FlaggingError> {
        let count = self.current_flags.shape()[1];
        let Some(selection) = selections.and_then(|windows| windows.get(&self.spw)) else {
            return Ok((0..count).collect());
        };
        let mut selected =
            selection
                .indices(self.channels.len())
                .map_err(|error| FlaggingError::MutateFlags {
                    path: path.to_owned(),
                    reason: error.to_string(),
                })?;
        // FlagAgentBase's local chan selector indexes the output axis. It
        // ignores indices beyond that axis when another agent set chanbin.
        selected.retain(|&chan| chan < count);
        Ok(selected)
    }
}

impl SharedFlagView {
    pub(super) fn clip(
        &mut self,
        selection: SharedAgentSelection<'_>,
        request: &FlagDataRequest,
        options: ClipOptions<'_>,
        path: &str,
    ) -> Result<ChangeSet, FlaggingError> {
        let values = &self
            .columns
            .iter()
            .find(|(column, _)| *column == request.data_column)
            .expect("registered clipping column")
            .1;
        let selections = if request.clip_channel_bins.is_some() {
            None
        } else {
            selection.channels
        };
        let mut samples = Vec::new();
        let mut coordinates = Vec::new();
        for (index, group) in self.rows.iter().enumerate() {
            check_cancelled(self.cancellation.as_ref())?;
            if !selection.holds(group) {
                continue;
            }
            let first = &group.members[0];
            let projections = selection
                .correlations
                .map(|corr| corr.clip_projections_for(first.description(), path))
                .transpose()?;
            if projections.is_some_and(|products| products.is_empty()) {
                continue;
            }
            let channels = group.select_channels(selections, path)?;
            coordinates.clear();
            apply_clip_array(
                &values[index].data,
                &group.current_flags,
                Some(&channels),
                ClipOptions {
                    projections,
                    ..options
                },
                path,
                group.row(),
                &mut coordinates,
            )?;
            let [ant1, ant2] = first.antennas();
            let row = RowSampleMetadata {
                row: index,
                field: first.field(),
                spw: group.spw,
                scan: first.chunk_key().0[2],
                ant1,
                ant2,
            };
            for &(corr, chan) in &coordinates {
                push_sample(&mut samples, row, corr, chan, 0., 0., false);
            }
        }
        let matched = vec![true; samples.len()];
        Ok(self.stage(&samples, &matched, true))
    }
}

impl SharedFlagView {
    pub(super) fn extend_implicit(&mut self, path: &str) -> Result<ChangeSet, FlaggingError> {
        let samples = self
            .load_samples(
                SharedAgentSelection::default(),
                None,
                &FlagDataRequest::default(),
                path,
            )?
            .samples;
        let mut modified = vec![false; samples.len()];
        let baseline = |sample: &Sample| self.rows[sample.row].members[0].baseline_key();
        let groups = samples.chunk_by(|a, b| baseline(a) == baseline(b));
        extend_autoflag_grouped(groups, &samples, &mut modified);
        Ok(self.stage(&samples, &modified, true))
    }

    pub(super) fn extend(
        &mut self,
        selection: SharedAgentSelection<'_>,
        request: &FlagDataRequest,
        path: &str,
    ) -> Result<ChangeSet, FlaggingError> {
        if !extend::valid_request(request) {
            return Ok(ChangeSet {
                agent_skipped: true,
                ..ChangeSet::default()
            });
        }
        let loaded = self.load_samples(selection, None, request, path)?;
        let baseline = |sample: &Sample| self.rows[sample.row].members[0].baseline_key();
        let mut samples = loaded.samples;
        let mut grown = Vec::with_capacity(samples.len());
        for group in samples.chunk_by_mut(|a, b| baseline(a) == baseline(b)) {
            check_cancelled(self.cancellation.as_ref())?;
            for (index, sample) in group.iter_mut().enumerate() {
                sample.index = index;
            }
            grown.extend(extend::grow(group, request));
        }
        Ok(self.stage(&samples, &grown, request.action.value()))
    }

    pub(super) fn antint(
        &mut self,
        ms: &MeasurementSet,
        selection: SharedAgentSelection<'_>,
        request: &FlagDataRequest,
        path: &str,
    ) -> Result<ChangeSet, FlaggingError> {
        let loaded = self.load_samples(selection, None, request, path)?;
        let timed = self
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| row.members[0].projected(index, row.time))
            .collect::<Vec<_>>();
        let Some(affected) =
            antint::affected_average_rows(ms, timed, &request.antint, path, |row| {
                &self.rows[row].original_flags
            })?
        else {
            return Ok(ChangeSet {
                agent_skipped: true,
                ..ChangeSet::default()
            });
        };
        let mask = loaded
            .samples
            .iter()
            .map(|sample| affected.binary_search(&sample.row).is_ok())
            .collect::<Vec<_>>();
        Ok(self.stage(&loaded.samples, &mask, true))
    }
}
