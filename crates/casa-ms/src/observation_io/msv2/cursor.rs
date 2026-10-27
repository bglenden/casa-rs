// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traversals of one MSv2 partition: whole times of its grid per batch,
//! the rows read by cell, the cells no row fills padded.

use std::ops::Range;
use std::sync::Arc;

use casa_observation::{
    AccessRequest, BatchArrays, BatchBuffers, BatchHeader, Catalogue, DataGroupEntry, IoCounters,
    ObservationError, PartitionEstimate, PartitionId, ResolvedRequest, Roles, Sharing, Step,
    VisibilityCursor, WeightRule, WeightSource, inverse_sigma_squared,
};
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, SelectedArray1DCellsMut,
    SelectedArray2DCellsMut, Table,
};
use num_complex::Complex32;

use super::index::{Grid, NO_ROW};
use super::{Msv2Observation, invalid, table_error};
use crate::observation_io::grouping::GroupIndex;

/// Reusable working storage: the present rows of a batch, their scalar
/// columns and, for a correlation subset, their cells before the delivered
/// products are gathered.
#[derive(Default)]
struct Scratch {
    /// MAIN rows of the batch's present cells, in cell order.
    rows: Vec<usize>,
    /// The sample of each present row.
    present: Vec<usize>,
    row_flags: Vec<bool>,
    centroid: Vec<f64>,
    interval: Vec<f64>,
    exposure: Vec<f64>,
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
}

/// A bound traversal of one MSv2 partition: whole times of its grid in
/// time order, as many per batch as the byte budget admits.
pub struct Msv2Cursor<'o> {
    observation: &'o Msv2Observation,
    partition: PartitionId,
    roles: Roles,
    channels: Range<usize>,
    resolved: ResolvedRequest,
    /// The next grid time to deliver, within `resolved.times`.
    next_time: usize,
    groups: Option<GroupIndex>,
    estimate: PartitionEstimate,
    counters: IoCounters,
    scratch: Scratch,
}

impl<'o> Msv2Cursor<'o> {
    pub(super) fn bind(
        observation: &'o Msv2Observation,
        request: &AccessRequest,
        estimate: PartitionEstimate,
        resolved: ResolvedRequest,
    ) -> Result<Self, ObservationError> {
        let (partition, _) = observation.partition(estimate.partition)?;
        let grid = &partition.grid;
        let groups = GroupIndex::new(
            estimate.partition,
            request.grouping,
            observation.options.group_closure,
            &grid.times_s,
            &grid.field_per_time,
            Some(&grid.scan_per_time),
            resolved.times.clone(),
        )?;
        Ok(Self {
            observation,
            partition: estimate.partition,
            roles: request.roles,
            channels: request.channels.clone(),
            next_time: resolved.times.start,
            resolved,
            groups,
            estimate,
            counters: IoCounters::default(),
            scratch: Scratch::default(),
        })
    }

    fn grid(&self) -> &'o Grid {
        &self.observation.partitions[self.partition.0 as usize].grid
    }

    /// Whether the delivered weights hold one value per channel: their
    /// source's, when the weights are read.
    fn weights_per_channel(&self) -> bool {
        self.estimate
            .weights
            .as_ref()
            .is_some_and(|source| source.per_channel)
    }

    fn header(&self, times: usize) -> BatchHeader {
        BatchHeader {
            partition: self.partition,
            data_group: self.resolved.data_group,
            times,
            first_channel: self.channels.start,
            channels: self.channels.len(),
            correlations: self.resolved.correlations.clone(),
            roles: self.roles,
            weights_per_channel: self.weights_per_channel(),
            interval: Sharing::PerSample,
            exposure: Sharing::PerSample,
        }
    }

    fn fill_batch(
        &mut self,
        buffers: &mut BatchBuffers,
        times: Range<usize>,
    ) -> Result<(), ObservationError> {
        let observation = self.observation;
        let main = observation.ms.main_table();
        let entry = observation.catalogue.partition(self.partition);
        let grid = self.grid();
        let header = self.header(times.len());
        let roles = self.roles;
        let channels = self.channels.clone();
        let capacity = buffers.capacity_bytes();
        let Self {
            scratch,
            counters,
            groups,
            resolved,
            estimate,
            ..
        } = self;
        let cells = grid.cells(times.clone());
        scratch.rows.clear();
        scratch.present.clear();
        for (sample, &row) in cells.iter().enumerate() {
            if row != NO_ROW {
                scratch.rows.push(row as usize);
                scratch.present.push(sample);
            }
        }
        let samples = cells.len();
        let mut fill = buffers.begin(header, &observation.catalogue)?;
        let arrays = fill.arrays();
        arrays
            .time_offsets_s
            .extend_from_slice(&grid.times_s[times.clone()]);
        arrays
            .field
            .extend_from_slice(&grid.field_per_time[times.clone()]);
        if roles.scan {
            arrays
                .scan
                .extend_from_slice(&grid.scan_per_time[times.clone()]);
        }
        read_scalars(main, roles, scratch)?;
        counters.read_calls += 1;
        for (wanted, source, target) in [
            (
                roles.time_centroid,
                &scratch.centroid,
                &mut arrays.centroid_offsets_s,
            ),
            (roles.interval, &scratch.interval, &mut arrays.interval_s),
            (roles.exposure, &scratch.exposure, &mut arrays.exposure_s),
        ] {
            if wanted {
                place(source, &scratch.present, samples, 1, f64::NAN, target);
                counters.copied_elements += source.len();
            }
        }
        let payload = Payload {
            main,
            group: &entry.data_groups[resolved.data_group],
            channels: &channels,
            roles,
            weights: estimate.weights.as_ref(),
            stored: entry.correlations.len(),
            resolved,
            samples,
        };
        payload.fill(scratch, arrays, counters)?;
        if let Some(groups) = groups.as_mut() {
            groups.fragments(times, grid.baselines.len(), &mut arrays.fragments);
        }
        counters.delivered_bytes += arrays.values.len() * size_of::<Complex32>()
            + arrays.flags.len()
            + arrays.weights.len() * size_of::<f32>()
            + (arrays.uvw.len()
                + arrays.centroid_offsets_s.len()
                + arrays.interval_s.len()
                + arrays.exposure_s.len())
                * size_of::<f64>();
        fill.finish()?;
        if buffers.capacity_bytes() > capacity {
            counters.buffer_growths += 1;
        }
        Ok(())
    }
}

/// MAIN's scalar columns for the present rows, in one call: `FLAG_ROW`
/// (folded into the flags afterwards) and the requested time auxiliaries.
fn read_scalars(main: &Table, roles: Roles, scratch: &mut Scratch) -> Result<(), ObservationError> {
    let mut destinations = Vec::new();
    for (wanted, column, values) in [
        (roles.time_centroid, "TIME_CENTROID", &mut scratch.centroid),
        (roles.interval, "INTERVAL", &mut scratch.interval),
        (roles.exposure, "EXPOSURE", &mut scratch.exposure),
    ] {
        values.clear();
        if wanted {
            destinations.push(RequiredScalarColumnDestination::new(
                column,
                RequiredScalarColumnValuesMut::Float64(values),
            ));
        }
    }
    scratch.row_flags.clear();
    if roles.flags {
        destinations.push(RequiredScalarColumnDestination::new(
            "FLAG_ROW",
            RequiredScalarColumnValuesMut::Bool(&mut scratch.row_flags),
        ));
    }
    if destinations.is_empty() {
        return Ok(());
    }
    main.required_scalar_columns_for_rows_into(&scratch.rows, &mut destinations)
        .map_err(table_error)
}

/// The payload roles of one batch and how to read them.
struct Payload<'a> {
    main: &'a Table,
    group: &'a DataGroupEntry,
    channels: &'a Range<usize>,
    roles: Roles,
    /// What the weights role reads, when it is read.
    weights: Option<&'a WeightSource>,
    /// Correlation products the store holds.
    stored: usize,
    resolved: &'a ResolvedRequest,
    samples: usize,
}

impl Payload<'_> {
    /// Read the payload roles of the present rows. With every product
    /// delivered, the typed fills write the batch's own arrays, and a batch
    /// with padding has its rows spread to their samples in place
    /// ([`spread`]). For a correlation subset, the whole stored axis is read
    /// into `scratch` and the delivered products gathered around the
    /// padding. `FLAG_ROW` is folded into the flags either way.
    fn fill(
        &self,
        scratch: &mut Scratch,
        arrays: &mut BatchArrays,
        counters: &mut IoCounters,
    ) -> Result<(), ObservationError> {
        let Scratch {
            rows,
            present,
            row_flags,
            values,
            flags,
            weights,
            ..
        } = scratch;
        let (main, roles, samples) = (self.main, self.roles, self.samples);
        let channels = self.channels.len();
        let keep = self.resolved.correlations.indices();
        let delivered = keep.map_or(self.stored, <[usize]>::len);
        let padded = rows.len() < samples;
        let cube = |column: &str, destination: SelectedArray2DCellsMut<'_>| {
            main.column_accessor(column)
                .map_err(table_error)?
                .fill_array_cells_2d_channel_range_typed_uncached(
                    rows,
                    self.channels.start,
                    channels,
                    destination,
                )
                .map_err(table_error)?
                .ok_or_else(|| invalid(format!("undefined {column} cells")))
                .map(|_| ())
        };
        let per_row = |column: &str, destination: SelectedArray1DCellsMut<'_>| {
            main.column_accessor(column)
                .map_err(table_error)?
                .fill_array_cells_1d_typed_uncached(rows, destination)
                .map_err(table_error)
        };
        let nan = Complex32::new(f32::NAN, f32::NAN);
        let stored = self.stored;
        if roles.values {
            counters.read_calls += 1;
            if let Some(keep) = keep {
                values.clear();
                cube(
                    &self.group.visibilities,
                    SelectedArray2DCellsMut::RowChannelComplex32(values),
                )?;
                gather(
                    values,
                    present,
                    [samples, channels, stored],
                    keep,
                    nan,
                    &mut arrays.values,
                );
                counters.copied_elements += arrays.values.len();
            } else {
                cube(
                    &self.group.visibilities,
                    SelectedArray2DCellsMut::RowChannelComplex32(&mut arrays.values),
                )?;
                if padded {
                    spread(&mut arrays.values, present, samples, channels * stored, nan);
                    counters.copied_elements += arrays.values.len();
                }
            }
        }
        if roles.flags {
            counters.read_calls += 1;
            if let Some(keep) = keep {
                flags.clear();
                cube(
                    &self.group.flags,
                    SelectedArray2DCellsMut::RowChannelBool(flags),
                )?;
                if self.resolved.flags_from_all {
                    gather_any(
                        flags,
                        present,
                        [samples, channels, stored],
                        delivered,
                        &mut arrays.flags,
                    );
                } else {
                    gather(
                        flags,
                        present,
                        [samples, channels, stored],
                        keep,
                        true,
                        &mut arrays.flags,
                    );
                }
                counters.copied_elements += arrays.flags.len();
            } else {
                cube(
                    &self.group.flags,
                    SelectedArray2DCellsMut::RowChannelBool(&mut arrays.flags),
                )?;
                if padded {
                    spread(&mut arrays.flags, present, samples, channels * stored, true);
                    counters.copied_elements += arrays.flags.len();
                }
            }
            // FLAG_ROW: the row's every value is flagged.
            let cell = channels * delivered;
            for (i, &sample) in present.iter().enumerate() {
                if row_flags[i] {
                    arrays.flags[sample * cell..(sample + 1) * cell].fill(true);
                }
            }
        }
        if let Some(source) = self.weights.filter(|_| roles.weights) {
            counters.read_calls += 1;
            let weight_channels = if source.per_channel { channels } else { 1 };
            // The present rows' values, made weights before padding is
            // spread, so a padded cell stays NaN.
            let read = |target: &mut Vec<f32>| {
                if source.per_channel {
                    cube(
                        &source.variable,
                        SelectedArray2DCellsMut::RowChannelFloat32(target),
                    )?;
                } else {
                    per_row(&source.variable, SelectedArray1DCellsMut::Float32(target))?;
                }
                if source.rule == WeightRule::InverseSigmaSquared {
                    for value in target.iter_mut() {
                        *value = inverse_sigma_squared(*value);
                    }
                }
                Ok::<(), ObservationError>(())
            };
            if let Some(keep) = keep {
                weights.clear();
                read(weights)?;
                gather(
                    weights,
                    present,
                    [samples, weight_channels, stored],
                    keep,
                    f32::NAN,
                    &mut arrays.weights,
                );
                counters.copied_elements += arrays.weights.len();
            } else {
                read(&mut arrays.weights)?;
                if padded {
                    spread(
                        &mut arrays.weights,
                        present,
                        samples,
                        weight_channels * stored,
                        f32::NAN,
                    );
                    counters.copied_elements += arrays.weights.len();
                }
            }
        }
        if roles.uvw {
            counters.read_calls += 1;
            per_row(
                &self.group.uvw,
                SelectedArray1DCellsMut::Float64(&mut arrays.uvw),
            )?;
            if padded {
                spread(&mut arrays.uvw, present, samples, 3, f64::NAN);
                counters.copied_elements += arrays.uvw.len();
            }
        }
        Ok(())
    }
}

/// `target` is `samples` cells of `cell` elements, `pad` where no row is,
/// and the present rows' cells of `source` at their samples.
fn place<T: Copy>(
    source: &[T],
    present: &[usize],
    samples: usize,
    cell: usize,
    pad: T,
    target: &mut Vec<T>,
) {
    target.clear();
    target.resize(samples * cell, pad);
    for (i, &sample) in present.iter().enumerate() {
        target[sample * cell..(sample + 1) * cell]
            .copy_from_slice(&source[i * cell..(i + 1) * cell]);
    }
}

/// Spread the cells of `present`'s rows, read in order to the front of
/// `target` (`cell` elements each), to their samples of `samples`, with
/// `pad` in every other: in place, the last run first, each run of
/// consecutive samples moved as one block. One move of the data at most,
/// and no copy of it elsewhere.
fn spread<T: Copy>(target: &mut Vec<T>, present: &[usize], samples: usize, cell: usize, pad: T) {
    target.resize(samples * cell, pad);
    // Cells at and after `end` are placed.
    let mut end = samples;
    let mut run_end = present.len();
    while run_end > 0 {
        let mut start = run_end - 1;
        while start > 0 && present[start - 1] + 1 == present[start] {
            start -= 1;
        }
        let first = present[start];
        // Rows before `run_end` lie before `run_end * cell <= last * cell`.
        let last = first + (run_end - start);
        target[last * cell..end * cell].fill(pad);
        target.copy_within(start * cell..run_end * cell, first * cell);
        end = first;
        run_end = start;
    }
    target[..end * cell].fill(pad);
}

/// [`place`] for cells of `[channels, stored]`, keeping products `keep` of
/// each.
fn gather<T: Copy>(
    source: &[T],
    present: &[usize],
    [samples, channels, stored]: [usize; 3],
    keep: &[usize],
    pad: T,
    target: &mut Vec<T>,
) {
    target.clear();
    target.resize(samples * channels * keep.len(), pad);
    for (i, &sample) in present.iter().enumerate() {
        for channel in 0..channels {
            let from = (i * channels + channel) * stored;
            let to = (sample * channels + channel) * keep.len();
            for (k, &product) in keep.iter().enumerate() {
                target[to + k] = source[from + product];
            }
        }
    }
}

/// [`gather`] for flags under [`FlagsFrom::All`]: each delivered flag is
/// the OR over every stored product of its cell.
///
/// [`FlagsFrom::All`]: casa_observation::FlagsFrom::All
fn gather_any(
    source: &[bool],
    present: &[usize],
    [samples, channels, stored]: [usize; 3],
    delivered: usize,
    target: &mut Vec<bool>,
) {
    target.clear();
    target.resize(samples * channels * delivered, true);
    for (i, &sample) in present.iter().enumerate() {
        for channel in 0..channels {
            let from = (i * channels + channel) * stored;
            let any = source[from..from + stored].iter().any(|flag| *flag);
            let to = (sample * channels + channel) * delivered;
            target[to..to + delivered].fill(any);
        }
    }
}

impl VisibilityCursor for Msv2Cursor<'_> {
    fn catalogue(&self) -> &Arc<Catalogue> {
        &self.observation.catalogue
    }

    fn estimate(&self) -> &PartitionEstimate {
        &self.estimate
    }

    fn step(&mut self) -> Step {
        if let Some(key) = self.groups.as_mut().and_then(GroupIndex::pop_end) {
            return Step::GroupEnd(key);
        }
        if self.next_time == self.resolved.times.end {
            Step::End
        } else {
            Step::Batch
        }
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<(), ObservationError> {
        let times = self.resolved.times.end;
        if self.next_time == times {
            return Err(ObservationError::InvalidRequest {
                reason: "fill after the end of the traversal".to_string(),
            });
        }
        let block = self.next_time..(self.next_time + self.estimate.times_per_batch).min(times);
        self.fill_batch(buffers, block.clone())?;
        self.counters.batches += 1;
        self.counters.decode_units += self.scratch.rows.len();
        self.next_time = block.end;
        Ok(())
    }

    fn counters(&self) -> IoCounters {
        self.counters
    }
}

#[cfg(test)]
mod tests {
    use super::{place, spread};

    /// Spreading in place gives what placing from a copy gives, wherever
    /// the padding falls.
    #[test]
    fn spreading_in_place_is_placing() {
        let samples = 9;
        let cases: [&[usize]; 7] = [
            &[0, 1, 2, 3, 4, 5, 6, 7, 8],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &[0, 1, 2, 3, 4, 5, 6, 7],
            &[0, 1, 3, 4, 7, 8],
            &[2, 6],
            &[4],
            &[],
        ];
        for present in cases {
            let source: Vec<i32> = (0..present.len() as i32 * 2).collect();
            let mut placed = Vec::new();
            place(&source, present, samples, 2, -1, &mut placed);
            let mut spread_in_place = source.clone();
            spread(&mut spread_in_place, present, samples, 2, -1);
            assert_eq!(spread_in_place, placed, "{present:?}");
        }
    }
}
