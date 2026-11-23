// SPDX-License-Identifier: LGPL-3.0-or-later
//! The visibility iterator and source coordinates shared by averaged list agents.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::clip_average::{ChannelBins, bin_members, check_channel_bins};
use super::clip_time::{
    ClipTimedRow, averaged_output_time, clip_time_groups, ordered_baseline_chunks, read_flags,
    time_bin_seconds,
};
use super::*;

pub(super) struct SharedAveraging<'a> {
    pub(super) seconds: Option<f64>,
    pub(super) widths: Option<&'a [u32]>,
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
        let automatic = requests.filter(automatic);
        // Each enabled agent sets the corresponding layer on the shared
        // handler. The last enabled agent determines that layer's bin size.
        let seconds = automatic
            .clone()
            .rev()
            .find_map(|request| request.clip_time_bin.as_deref().and_then(time_bin_seconds));
        let widths = automatic
            .rev()
            .find_map(|request| request.clip_channel_bins.as_deref());
        (seconds.is_some() || widths.is_some()).then_some(Self { seconds, widths })
    }
}

#[derive(Clone, Copy)]
pub(super) struct SharedAgentSelection<'a> {
    pub(super) rows: &'a [usize],
    pub(super) channels: Option<&'a BTreeMap<i32, ChannelSelection>>,
    pub(super) correlations: Option<&'a CorrelationSelection>,
    pub(super) time: Option<&'a TimeSelection>,
}

pub(super) struct SharedFlagView {
    rows: Vec<SharedFlagRow>,
    flags: BTreeMap<usize, ArrayD<bool>>,
    ddid_to_spw: BTreeMap<i32, i32>,
    channel_layouts: BTreeMap<(i32, usize), Arc<[usize]>>,
    columns: Vec<(FlagDataColumn, Vec<ArrayValue>)>,
    staged: BTreeMap<usize, ArrayD<bool>>,
    averaged_in_time: bool,
}

struct SharedFlagRow {
    members: Vec<ClipTimedRow>,
    time: f64,
    spw: i32,
    width: u32,
    channels: Arc<[usize]>,
    original_flags: ArrayD<bool>,
    current_flags: ArrayD<bool>,
}

impl SharedFlagView {
    pub(super) fn new(
        ms: &MeasurementSet,
        config: SharedAveraging<'_>,
        time_chunk: Option<f64>,
        combinescans: bool,
        path: &str,
    ) -> Result<Self, FlaggingError> {
        if let Some(widths) = config.widths {
            check_channel_bins(ms, widths, path)?;
        }
        let selected = (0..ms.row_count()).collect::<Vec<_>>();
        let ddid_to_spw = data_description_spw_map(ms)?;
        let timed = ordered_baseline_chunks(ms, &selected, time_chunk, combinescans, path)?;
        let flags = selected
            .iter()
            .copied()
            .zip(read_flags(ms.main_table(), &selected, path)?)
            .collect::<BTreeMap<_, _>>();
        let mut view = Self {
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
                view.add_row(members, config.widths, path)?;
            }
        } else {
            for row in &timed {
                view.add_row(std::slice::from_ref(row), config.widths, path)?;
            }
        }
        Ok(view)
    }

    fn add_row(
        &mut self,
        members: &[ClipTimedRow],
        widths: Option<&[u32]>,
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
    ) -> Result<Vec<ArrayValue>, FlaggingError> {
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
                        output.push(
                            values
                                .row(slot, flags, bins.as_ref(), group.row(), path)?
                                .into_owned(),
                        );
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
    ) -> Result<ArrayValue, FlaggingError> {
        let reduced = accumulator.reduced_visibility(
            column,
            Some(&self.channels),
            self.width,
            self.row(),
            path,
        )?;
        Ok(reduced.data.into_owned())
    }
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
        let SharedAgentSelection {
            rows: selected,
            channels: channel_selections,
            correlations,
            time: filter,
        } = selection;
        let values = fit.and_then(|column| self.columns.iter().find(|(key, _)| *key == column));
        let select_channels = request
            .clip_channel_bins
            .is_none()
            .then_some(channel_selections)
            .flatten();
        let mut axes = AveragedSampleAxes::default();
        let mut samples = Vec::new();
        let mut projected = BTreeMap::new();
        for (index, group) in self.rows.iter().enumerate() {
            if selected.binary_search(&group.row()).is_err() {
                continue;
            }
            if filter.is_some_and(|filter| !filter.holds_agent_time(group.time)) {
                if !request.extendflags {
                    continue;
                }
                axes.rows_outside_time.insert(index);
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
                let flags = if request.mode == FlagDataMode::Rflag {
                    &group.original_flags
                } else {
                    &group.current_flags
                };
                push_row_samples(
                    &values[index],
                    flags,
                    &channels,
                    metadata,
                    path,
                    column.name(),
                    &mut samples,
                )?;
            } else {
                push_flag_samples(
                    &group.current_flags,
                    &channels,
                    correlations.map(|corr| corr.planes(first.description())),
                    metadata,
                    &mut samples,
                );
            }
            axes.rows_by_bin.insert(
                index,
                group.members.iter().map(|member| member.row).collect(),
            );
            projected.insert(index, group.current_flags.clone());
        }
        axes.flags_by_bin = Some(projected);
        Ok(LoadedSamples {
            samples,
            flags_by_row: BTreeMap::new(),
            model_roundoff: Vec::new(),
            axes,
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

    pub(super) fn add_modified_flags(&self, samples: &[Sample], mask: &mut [bool]) {
        for sample in samples {
            mask[sample.index] |= self.rows[sample.row].current_flags[[sample.corr, sample.chan]];
        }
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

    pub(super) fn summarize(
        &self,
        selected: &[usize],
        time: Option<&TimeSelection>,
    ) -> FlagSummary {
        let rows = self.rows.iter().filter(|row| {
            selected.binary_search(&row.row()).is_ok()
                && time.is_none_or(|filter| filter.holds_agent_time(row.time))
        });
        let mut counts = FlagSummary {
            flagged_samples: 0,
            total_samples: 0,
        };
        for member in rows.flat_map(|row| &row.members) {
            let flags = self
                .staged
                .get(&member.row)
                .unwrap_or(&self.flags[&member.row]);
            counts.total_samples += flags.len();
            counts.flagged_samples += flags.iter().filter(|&&flag| flag).count();
        }
        counts
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
        let SharedAgentSelection {
            rows: selected,
            channels,
            correlations,
            time: filter,
        } = selection;
        let values = &self
            .columns
            .iter()
            .find(|(column, _)| *column == request.data_column)
            .expect("registered clipping column")
            .1;
        let selections = if request.clip_channel_bins.is_some() {
            None
        } else {
            channels
        };
        let mut samples = Vec::new();
        let mut coordinates = Vec::new();
        let mut selected_rows = 0;
        for (index, group) in self.rows.iter().enumerate() {
            if selected.binary_search(&group.row()).is_err()
                || filter.is_some_and(|filter| !filter.holds_agent_time(group.time))
            {
                continue;
            }
            selected_rows += group.members.len();
            let first = &group.members[0];
            let projections = correlations
                .map(|corr| corr.clip_projections_for(first.description(), path))
                .transpose()?;
            if projections.is_some_and(|products| products.is_empty()) {
                continue;
            }
            let channels = group.select_channels(selections, path)?;
            coordinates.clear();
            apply_clip_array(
                &values[index],
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
        let mut changes = self.stage(&samples, &matched, true);
        changes.selected_rows = Some(selected_rows);
        Ok(changes)
    }
}

impl SharedFlagView {
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
            for (index, sample) in group.iter_mut().enumerate() {
                sample.index = index;
            }
            grown.extend(extend::grow(group, request));
        }
        let mut changed = self.stage(&samples, &grown, request.action.value());
        changed.selected_rows = loaded.axes.time_selected_rows(selection.time);
        Ok(changed)
    }

    pub(super) fn antint(
        &mut self,
        ms: &MeasurementSet,
        selection: SharedAgentSelection<'_>,
        request: &FlagDataRequest,
        path: &str,
    ) -> Result<ChangeSet, FlaggingError> {
        let loaded = self.load_samples(selection, None, request, path)?;
        let timed = loaded
            .axes
            .rows_by_bin
            .keys()
            .map(|&index| {
                let row = &self.rows[index];
                row.members[0].projected(index, row.time)
            })
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
        let mut changed = self.stage(&loaded.samples, &mask, true);
        changed.selected_rows = loaded.axes.time_selected_rows(selection.time);
        Ok(changed)
    }
}
