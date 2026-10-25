// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traversals of one MSv2 partition: whole times of its grid per batch,
//! the rows read by cell, the cells no row fills padded.

use std::ops::Range;

use casa_observation::{
    AccessRequest, BatchArrays, BatchBuffers, BatchHeader, Catalogue, DataGroupEntry, IoCounters,
    ObservationError, PartitionEstimate, PartitionId, ResolvedRequest, Roles, Sharing, Step,
    VisibilityCursor,
};
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, SelectedArray1DCellsMut,
    SelectedArray2DCellsMut, Table,
};
use num_complex::Complex32;

use super::index::{Grid, NO_ROW};
use super::{Msv2Observation, invalid, table_error};
use crate::observation_io::grouping::GroupIndex;

/// Reusable working storage: the present rows of a batch and, for a fill
/// that cannot read straight into the batch arrays (a padded time, or a
/// correlation subset), their cells before placement.
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
    uvw: Vec<f64>,
}

/// A bound traversal of one MSv2 partition: whole times of its grid in
/// time order, as many per batch as the byte budget admits.
pub struct Msv2Cursor<'o> {
    observation: &'o Msv2Observation,
    partition: PartitionId,
    roles: Roles,
    channels: Range<usize>,
    resolved: ResolvedRequest,
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
    ) -> Result<Self, ObservationError> {
        let (partition, entry) = observation.partition(estimate.partition)?;
        let resolved = request.resolve(entry)?;
        let grid = &partition.grid;
        let groups = GroupIndex::new(
            estimate.partition,
            request.grouping,
            observation.options.group_closure,
            &grid.times_s,
            &grid.field_per_time,
            &grid.scan_per_time,
        )?;
        Ok(Self {
            observation,
            partition: estimate.partition,
            roles: request.roles,
            channels: request.channels.clone(),
            resolved,
            next_time: 0,
            groups,
            estimate,
            counters: IoCounters::default(),
            scratch: Scratch::default(),
        })
    }

    fn grid(&self) -> &'o Grid {
        &self.observation.partitions[self.partition.0 as usize].grid
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
            weights_per_channel: self.observation.spectral_weights,
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
        let mut fill = buffers.begin(header);
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
            weights_per_channel: observation.spectral_weights,
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
        fill.finish(&observation.catalogue)?;
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
    weights_per_channel: bool,
    /// Correlation products the store holds.
    stored: usize,
    resolved: &'a ResolvedRequest,
    samples: usize,
}

impl Payload<'_> {
    /// Read the payload roles of the present rows: straight into `arrays`
    /// when every cell is present and every product delivered (the typed
    /// fills then write the batch's own storage), else into `scratch` and
    /// then placed: padded where no row is, the delivered products
    /// gathered. `FLAG_ROW` is folded into the flags either way.
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
            uvw,
            ..
        } = scratch;
        let (main, roles, samples) = (self.main, self.roles, self.samples);
        let channels = self.channels.len();
        let keep: Vec<usize> = match &self.resolved.correlation_indices {
            Some(indices) => indices.clone(),
            None => (0..self.stored).collect(),
        };
        let direct = rows.len() == samples && self.resolved.correlation_indices.is_none();
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
        if roles.values {
            counters.read_calls += 1;
            if direct {
                cube(
                    &self.group.visibilities,
                    SelectedArray2DCellsMut::RowChannelComplex32(&mut arrays.values),
                )?;
            } else {
                values.clear();
                cube(
                    &self.group.visibilities,
                    SelectedArray2DCellsMut::RowChannelComplex32(values),
                )?;
                gather(
                    values,
                    present,
                    samples,
                    channels,
                    self.stored,
                    &keep,
                    nan,
                    &mut arrays.values,
                );
                counters.copied_elements += arrays.values.len();
            }
        }
        if roles.flags {
            counters.read_calls += 1;
            if direct {
                cube(
                    &self.group.flags,
                    SelectedArray2DCellsMut::RowChannelBool(&mut arrays.flags),
                )?;
            } else {
                flags.clear();
                cube(
                    &self.group.flags,
                    SelectedArray2DCellsMut::RowChannelBool(flags),
                )?;
                if self.resolved.flags_from_all {
                    gather_any(
                        flags,
                        present,
                        samples,
                        channels,
                        self.stored,
                        keep.len(),
                        &mut arrays.flags,
                    );
                } else {
                    gather(
                        flags,
                        present,
                        samples,
                        channels,
                        self.stored,
                        &keep,
                        true,
                        &mut arrays.flags,
                    );
                }
                counters.copied_elements += arrays.flags.len();
            }
            // FLAG_ROW: the row's every value is flagged.
            let cell = channels * keep.len();
            for (i, &sample) in present.iter().enumerate() {
                if row_flags[i] {
                    arrays.flags[sample * cell..(sample + 1) * cell].fill(true);
                }
            }
        }
        if roles.weights {
            counters.read_calls += 1;
            if direct && self.weights_per_channel {
                cube(
                    &self.group.weights,
                    SelectedArray2DCellsMut::RowChannelFloat32(&mut arrays.weights),
                )?;
            } else if direct {
                per_row(
                    &self.group.weights,
                    SelectedArray1DCellsMut::Float32(&mut arrays.weights),
                )?;
            } else {
                weights.clear();
                let weight_channels = if self.weights_per_channel {
                    cube(
                        &self.group.weights,
                        SelectedArray2DCellsMut::RowChannelFloat32(weights),
                    )?;
                    channels
                } else {
                    per_row(
                        &self.group.weights,
                        SelectedArray1DCellsMut::Float32(weights),
                    )?;
                    1
                };
                gather(
                    weights,
                    present,
                    samples,
                    weight_channels,
                    self.stored,
                    &keep,
                    0.0,
                    &mut arrays.weights,
                );
                counters.copied_elements += arrays.weights.len();
            }
        }
        if roles.uvw {
            counters.read_calls += 1;
            if direct {
                per_row(
                    &self.group.uvw,
                    SelectedArray1DCellsMut::Float64(&mut arrays.uvw),
                )?;
            } else {
                uvw.clear();
                per_row(&self.group.uvw, SelectedArray1DCellsMut::Float64(uvw))?;
                place(uvw, present, samples, 3, f64::NAN, &mut arrays.uvw);
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

/// [`place`] for cells of `channels` × `stored` products, keeping products
/// `keep` of each.
#[expect(clippy::too_many_arguments, reason = "one placement, every axis named")]
fn gather<T: Copy>(
    source: &[T],
    present: &[usize],
    samples: usize,
    channels: usize,
    stored: usize,
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
    samples: usize,
    channels: usize,
    stored: usize,
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
    fn catalogue(&self) -> &Catalogue {
        &self.observation.catalogue
    }

    fn estimate(&self) -> &PartitionEstimate {
        &self.estimate
    }

    fn step(&mut self) -> Step {
        if let Some(key) = self.groups.as_mut().and_then(GroupIndex::pop_end) {
            return Step::GroupEnd(key);
        }
        if self.next_time == self.grid().times() {
            Step::End
        } else {
            Step::Batch
        }
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<(), ObservationError> {
        let times = self.grid().times();
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
