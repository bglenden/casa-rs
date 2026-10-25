// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traversals of an MSv4 dataset: whole times decoded straight into the
//! caller's buffers.

use std::ops::Range;
use std::sync::Arc;

use casa_observation::{
    AccessRequest, BatchArrays, BatchBuffers, BatchHeader, Catalogue, IoCounters, NaturalBlocks,
    ObservationError, PartitionEstimate, PartitionId, ResolvedRequest, Roles, Sharing, Step,
    VisibilityCursor, times_per_batch,
};
use num_complex::Complex32;
use zarrs::array::{
    Array, ArrayBytesDecodeIntoTarget, ArrayBytesFixedDisjointView, ArrayMetadata, ArraySubset,
    CodecOptions,
};
use zarrs::filesystem::FilesystemStore;

use super::metadata::{invalid, storage};
use super::read::{Dataset, GroupArrays, Msv4Observation};
use crate::observation_io::grouping::GroupIndex;

/// Reusable decode storage for what cannot be decoded in place: the flag
/// bytes before their conversion to `bool`, and the full correlation axis
/// when a subset is delivered.
#[derive(Default)]
struct Scratch {
    flag_bytes: Vec<u8>,
    values: Vec<Complex32>,
    weights: Vec<f32>,
}

/// A bound traversal of one MSv4 dataset: whole times in time order.
pub struct Msv4Cursor<'o> {
    observation: &'o Msv4Observation,
    dataset: &'o Dataset,
    arrays: &'o GroupArrays,
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

impl<'o> Msv4Cursor<'o> {
    pub(super) fn bind(
        observation: &'o Msv4Observation,
        request: &AccessRequest,
        estimate: PartitionEstimate,
    ) -> Result<Self, ObservationError> {
        let (dataset, entry) = observation.dataset(estimate.partition)?;
        let resolved = request.resolve(entry)?;
        let arrays = &dataset.groups[resolved.data_group];
        let groups = GroupIndex::new(
            estimate.partition,
            request.grouping,
            observation.open.group_closure,
            &dataset.times,
            &arrays.fields,
            dataset.scans.as_deref().unwrap_or(&[]),
        )?;
        Ok(Self {
            observation,
            dataset,
            arrays,
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
}

/// What a request reads: each requested variable and whether it spans
/// frequency, or is UVW (its last axis whole), or is `[time, baseline]`.
#[derive(Clone, Copy)]
enum Extent {
    Cube,
    Uvw,
    TimeBaseline,
}

fn requested<'a>(
    dataset: &'a Dataset,
    arrays: &'a GroupArrays,
    roles: Roles,
) -> Vec<(&'a Array<FilesystemStore>, Extent)> {
    let mut read = Vec::new();
    for (wanted, array, extent) in [
        (roles.values, Some(&arrays.values), Extent::Cube),
        (roles.flags, Some(&arrays.flags), Extent::Cube),
        (roles.weights, Some(&arrays.weights), Extent::Cube),
        (roles.uvw, Some(&arrays.uvw), Extent::Uvw),
        (
            roles.time_centroid,
            dataset.centroid.as_ref(),
            Extent::TimeBaseline,
        ),
        (
            roles.exposure,
            dataset.exposure.as_ref(),
            Extent::TimeBaseline,
        ),
    ] {
        if let (true, Some(array)) = (wanted, array) {
            read.push((array, extent));
        }
    }
    read
}

/// What `request` costs on partition `id`: the chunks the traversal
/// touches and the elements they hold, modelled from the chunk grid, and
/// the grid's shape.
///
/// # Errors
///
/// When the request names an unknown partition, data group, channel or
/// product, asks for a role the dataset does not hold (`TIME_CENTROID`,
/// `EFFECTIVE_INTEGRATION_TIME`, `time.integration_time` or `scan_name`),
/// or budgets less than one time.
pub(super) fn estimate(
    observation: &Msv4Observation,
    request: &AccessRequest,
    id: PartitionId,
) -> Result<PartitionEstimate, ObservationError> {
    let (dataset, entry) = observation.dataset(id)?;
    let resolved = request.resolve(entry)?;
    let arrays = &dataset.groups[resolved.data_group];
    let roles = request.roles;
    for (wanted, present, variable) in [
        (
            roles.time_centroid,
            dataset.centroid.is_some(),
            "TIME_CENTROID",
        ),
        (
            roles.exposure,
            dataset.exposure.is_some(),
            "EFFECTIVE_INTEGRATION_TIME",
        ),
        (
            roles.interval,
            dataset.interval_s.is_some(),
            "time.integration_time",
        ),
        (roles.scan, dataset.scans.is_some(), "scan_name"),
    ] {
        if wanted && !present {
            return Err(ObservationError::InvalidRequest {
                reason: format!("the dataset has no {variable}, which the request reads"),
            });
        }
    }
    let (times, baselines) = (dataset.times.len(), entry.baselines.len());
    let channels = request.channels.len();
    let scalars = [roles.time_centroid, roles.exposure]
        .into_iter()
        .filter(|wanted| *wanted)
        .count();
    let bytes_per_sample = roles.sample_bytes(channels, resolved.correlations.len(), true, scalars);
    // The cursor's scratch: the flag bytes before their conversion, and,
    // when a subset is delivered, the values and weights of every stored
    // product before they are gathered.
    let stored = channels * entry.correlations.len();
    let scratch_bytes_per_sample = usize::from(roles.flags) * stored
        + if resolved.correlation_indices.is_some() {
            stored * (usize::from(roles.values) * 8 + usize::from(roles.weights) * 4)
        } else {
            0
        };
    let times_per_batch = times_per_batch(
        request.batch_bytes,
        bytes_per_sample + scratch_bytes_per_sample,
        baselines,
        times,
    )?;
    let batches = if times_per_batch == 0 {
        0
    } else {
        times.div_ceil(times_per_batch)
    };
    let (mut decode_units, mut decoded, mut selected) = (0, 0, 0);
    let variables = requested(dataset, arrays, roles);
    for batch in 0..batches {
        let rows = batch * times_per_batch..((batch + 1) * times_per_batch).min(times);
        for &(array, extent) in &variables {
            let subset = subset(array, rows.clone(), &request.channels, extent);
            let (units, elements) = chunk_cost(array, &subset)?;
            decode_units += units;
            decoded += elements;
            selected += match (extent, &resolved.correlation_indices) {
                (Extent::Cube, Some(indices)) => {
                    subset.num_elements_usize() / entry.correlations.len() * indices.len()
                }
                _ => subset.num_elements_usize(),
            };
        }
    }
    let chunk = arrays.values.chunk_shape(&[0, 0, 0, 0]).map_err(storage)?;
    Ok(PartitionEstimate {
        partition: id,
        times,
        baselines,
        bytes_per_sample,
        scratch_bytes_per_sample,
        times_per_batch,
        batches,
        decode_units,
        decoded_elements: decoded,
        selected_elements: selected,
        natural_blocks: NaturalBlocks {
            times: chunk[0].get() as usize,
            channels: chunk[2].get() as usize,
        },
        padded_fraction: 0.0,
    })
}

/// The array subset of times `rows` and, for a cube, channels `channels`.
fn subset(
    array: &Array<FilesystemStore>,
    rows: Range<usize>,
    channels: &Range<usize>,
    extent: Extent,
) -> ArraySubset {
    let shape = array.shape();
    let rows = rows.start as u64..rows.end as u64;
    match extent {
        Extent::Cube => ArraySubset::new_with_ranges(&[
            rows,
            0..shape[1],
            channels.start as u64..channels.end as u64,
            0..shape[3],
        ]),
        Extent::Uvw => ArraySubset::new_with_ranges(&[rows, 0..shape[1], 0..3]),
        Extent::TimeBaseline => ArraySubset::new_with_ranges(&[rows, 0..shape[1]]),
    }
}

/// The chunks `subset` touches and the elements those chunks hold.
fn chunk_cost(
    array: &Array<FilesystemStore>,
    subset: &ArraySubset,
) -> Result<(usize, usize), ObservationError> {
    let chunks = array
        .chunks_in_array_subset(subset)
        .map_err(storage)?
        .ok_or_else(|| invalid("the chunk grid cannot be indexed"))?;
    let mut units = 0;
    let mut elements = 0;
    for indices in &chunks.indices() {
        let shape = array.chunk_shape(&indices).map_err(storage)?;
        units += 1;
        elements += shape.iter().map(|n| n.get() as usize).product::<usize>();
    }
    Ok((units, elements))
}

/// Decode `subset` of `array`, whose Zarr data type must be `data_type`,
/// straight into `destination`, whose length is the subset's element
/// count: zarrs writes each chunk's selected elements into place, with no
/// intermediate whole-subset copy. The decode copies bytes, so the type
/// check here (and the layout check at opening) is what makes the bytes
/// `T`s.
fn decode_into<T: bytemuck::Pod>(
    array: &Array<FilesystemStore>,
    data_type: &str,
    subset: &ArraySubset,
    destination: &mut [T],
    options: &CodecOptions,
) -> Result<(), ObservationError> {
    let ArrayMetadata::V3(metadata) = array.metadata() else {
        return Err(invalid("not a Zarr v3 array"));
    };
    if metadata.data_type.name() != data_type
        || array.data_type().fixed_size() != Some(size_of::<T>())
        || destination.len() != subset.num_elements_usize()
    {
        return Err(invalid(format!(
            "decode of {} into {data_type} elements",
            metadata.data_type.name()
        )));
    }
    let shape: Vec<u64> = subset.shape().to_vec();
    let bytes = bytemuck::cast_slice_mut::<T, u8>(destination);
    let cells = unsafe_cell_slice::UnsafeCellSlice::new(bytes);
    // SAFETY: the view covers the whole destination, which this function
    // borrows exclusively, so no other view of these bytes exists; the
    // element size is the array's, checked above.
    let mut view = unsafe {
        ArrayBytesFixedDisjointView::new(
            cells,
            size_of::<T>(),
            &shape,
            ArraySubset::new_with_shape(shape.clone()),
        )
    }
    .map_err(storage)?;
    array
        .retrieve_array_subset_into_opt(
            subset,
            ArrayBytesDecodeIntoTarget::Fixed(&mut view),
            options,
        )
        .map_err(storage)
}

/// Keep products `keep` of every `[channel][stored]` cell of `source`.
fn gather<T: Copy>(source: &[T], stored: usize, keep: &[usize], target: &mut Vec<T>) {
    target.clear();
    target.reserve(source.len() / stored * keep.len());
    for cell in source.chunks_exact(stored) {
        target.extend(keep.iter().map(|&product| cell[product]));
    }
}

impl Msv4Cursor<'_> {
    fn header(&self, times: usize) -> BatchHeader {
        BatchHeader {
            partition: self.partition,
            data_group: self.resolved.data_group,
            times,
            first_channel: self.channels.start,
            channels: self.channels.len(),
            correlations: self.resolved.correlations.clone(),
            roles: self.roles,
            weights_per_channel: true,
            interval: Sharing::Constant(self.dataset.interval_s.unwrap_or(f64::NAN)),
            exposure: Sharing::PerSample,
        }
    }

    fn fill_batch(
        &mut self,
        buffers: &mut BatchBuffers,
        rows: Range<usize>,
    ) -> Result<(), ObservationError> {
        let dataset = self.dataset;
        let arrays = self.arrays;
        let catalogue = &self.observation.catalogue;
        let capacity = buffers.capacity_bytes();
        let header = self.header(rows.len());
        let mut fill = buffers.begin(header);
        let batch = fill.arrays();
        batch
            .time_offsets_s
            .extend_from_slice(&dataset.times[rows.clone()]);
        batch.field.extend_from_slice(&arrays.fields[rows.clone()]);
        if self.roles.scan {
            let scans = dataset.scans.as_ref().expect("checked at binding");
            batch.scan.extend_from_slice(&scans[rows.clone()]);
        }
        self.decode_batch(batch, &rows)?;
        if let Some(groups) = self.groups.as_mut() {
            groups.fragments(
                rows,
                catalogue.partition(self.partition).baselines.len(),
                &mut batch.fragments,
            );
        }
        self.counters.delivered_bytes += batch.values.len() * size_of::<Complex32>()
            + batch.flags.len()
            + batch.weights.len() * size_of::<f32>()
            + (batch.uvw.len() + batch.centroid_offsets_s.len() + batch.exposure_s.len())
                * size_of::<f64>();
        fill.finish(catalogue)?;
        if buffers.capacity_bytes() > capacity {
            self.counters.buffer_growths += 1;
        }
        Ok(())
    }

    /// Decode each requested variable's subset of times `rows` into
    /// `batch`: in place when every product is delivered, else the whole
    /// correlation axis into scratch and the delivered products gathered.
    fn decode_batch(
        &mut self,
        batch: &mut BatchArrays,
        rows: &Range<usize>,
    ) -> Result<(), ObservationError> {
        let (dataset, arrays) = (self.dataset, self.arrays);
        let options = &self.observation.options;
        let roles = self.roles;
        let channels = &self.channels;
        let stored = arrays.values.shape()[3] as usize;
        let keep = self.resolved.correlation_indices.as_deref();
        let counters = &mut self.counters;
        let scratch = &mut self.scratch;
        let mut prepare = |wanted: bool,
                           array: Option<&Array<FilesystemStore>>,
                           extent: Extent|
         -> Result<Option<ArraySubset>, ObservationError> {
            let (true, Some(array)) = (wanted, array) else {
                return Ok(None);
            };
            let subset = subset(array, rows.clone(), channels, extent);
            counters.read_calls += 1;
            counters.decode_units += chunk_cost(array, &subset)?.0;
            Ok(Some(subset))
        };
        if let Some(subset) = prepare(roles.values, Some(&arrays.values), Extent::Cube)? {
            let target = match keep {
                None => &mut batch.values,
                Some(_) => &mut scratch.values,
            };
            target.resize(subset.num_elements_usize(), Complex32::default());
            decode_into(&arrays.values, "complex64", &subset, target, options)?;
            if let Some(keep) = keep {
                gather(&scratch.values, stored, keep, &mut batch.values);
                counters.copied_elements += batch.values.len();
            }
        }
        if let Some(subset) = prepare(roles.flags, Some(&arrays.flags), Extent::Cube)? {
            scratch.flag_bytes.resize(subset.num_elements_usize(), 0);
            decode_into(
                &arrays.flags,
                "bool",
                &subset,
                &mut scratch.flag_bytes,
                options,
            )?;
            let flag = |byte: u8| match byte {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(invalid(format!("FLAG byte {byte} is not a boolean"))),
            };
            match keep {
                None => {
                    for &byte in &scratch.flag_bytes {
                        batch.flags.push(flag(byte)?);
                    }
                }
                Some(keep) if self.resolved.flags_from_all => {
                    for cell in scratch.flag_bytes.chunks_exact(stored) {
                        let mut any = false;
                        for &byte in cell {
                            any |= flag(byte)?;
                        }
                        batch.flags.extend(std::iter::repeat_n(any, keep.len()));
                    }
                }
                Some(keep) => {
                    for cell in scratch.flag_bytes.chunks_exact(stored) {
                        for &product in keep {
                            batch.flags.push(flag(cell[product])?);
                        }
                    }
                }
            }
            counters.copied_elements += scratch.flag_bytes.len();
        }
        if let Some(subset) = prepare(roles.weights, Some(&arrays.weights), Extent::Cube)? {
            let target = match keep {
                None => &mut batch.weights,
                Some(_) => &mut scratch.weights,
            };
            target.resize(subset.num_elements_usize(), 0.0);
            decode_into(&arrays.weights, "float32", &subset, target, options)?;
            if let Some(keep) = keep {
                gather(&scratch.weights, stored, keep, &mut batch.weights);
                counters.copied_elements += batch.weights.len();
            }
        }
        for (wanted, array, extent, target) in [
            (roles.uvw, Some(&arrays.uvw), Extent::Uvw, &mut batch.uvw),
            (
                roles.time_centroid,
                dataset.centroid.as_ref(),
                Extent::TimeBaseline,
                &mut batch.centroid_offsets_s,
            ),
            (
                roles.exposure,
                dataset.exposure.as_ref(),
                Extent::TimeBaseline,
                &mut batch.exposure_s,
            ),
        ] {
            if let Some(subset) = prepare(wanted, array, extent)? {
                target.resize(subset.num_elements_usize(), 0.0);
                decode_into(
                    array.expect("prepared"),
                    "float64",
                    &subset,
                    target,
                    options,
                )?;
            }
        }
        Ok(())
    }
}

impl VisibilityCursor for Msv4Cursor<'_> {
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
        if self.next_time == self.dataset.times.len() {
            Step::End
        } else {
            Step::Batch
        }
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<(), ObservationError> {
        let times = self.dataset.times.len();
        if self.next_time == times {
            return Err(ObservationError::InvalidRequest {
                reason: "fill after the end of the traversal".to_string(),
            });
        }
        let rows = self.next_time..(self.next_time + self.estimate.times_per_batch).min(times);
        self.fill_batch(buffers, rows.clone())?;
        self.next_time = rows.end;
        self.counters.batches += 1;
        Ok(())
    }

    fn counters(&self) -> IoCounters {
        self.counters
    }
}
