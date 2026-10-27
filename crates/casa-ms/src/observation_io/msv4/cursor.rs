// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traversals of an MSv4 dataset: whole times decoded straight into the
//! caller's buffers.

use std::ops::Range;
use std::sync::Arc;

use casa_observation::{
    AccessRequest, BatchArrays, BatchBuffers, BatchHeader, Catalogue, IoCounters, NaturalBlocks,
    ObservationError, PartitionEstimate, PartitionId, ResolvedRequest, Roles, Sharing,
    SigmaFallback, Step, VisibilityCursor, WeightRequest, WeightRule, WeightSource,
    times_per_batch,
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
/// bytes before their conversion to `bool`, the stored weights the flags
/// read when weights are not delivered, and the full correlation axis when
/// a subset is delivered.
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
    /// The next dataset time to deliver, within `resolved.times`.
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
        resolved: ResolvedRequest,
    ) -> Result<Self, ObservationError> {
        let (dataset, _) = observation.dataset(estimate.partition)?;
        let arrays = &dataset.groups[resolved.data_group];
        let groups = GroupIndex::new(
            estimate.partition,
            request.grouping,
            observation.open.group_closure,
            &dataset.times,
            &arrays.fields,
            dataset.scans.as_deref(),
            resolved.times.clone(),
        )?;
        Ok(Self {
            observation,
            dataset,
            arrays,
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
}

/// What a request reads: each variable decoded, whether it spans
/// frequency, or is UVW (its last axis whole), or is `[time, baseline]`, and
/// whether it is delivered (`WEIGHT` is decoded for the flags alone when
/// flags are read without weights).
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
) -> Vec<(&'a Array<FilesystemStore>, Extent, bool)> {
    let mut read = Vec::new();
    for (decoded, delivered, array, extent) in [
        (roles.values, true, Some(&arrays.values), Extent::Cube),
        (roles.flags, true, Some(&arrays.flags), Extent::Cube),
        (
            roles.weights || roles.flags,
            roles.weights,
            Some(&arrays.weights),
            Extent::Cube,
        ),
        (roles.uvw, true, Some(&arrays.uvw), Extent::Uvw),
        (
            roles.time_centroid,
            true,
            dataset.centroid.as_ref(),
            Extent::TimeBaseline,
        ),
        (
            roles.exposure,
            true,
            dataset.exposure.as_ref(),
            Extent::TimeBaseline,
        ),
    ] {
        if let (true, Some(array)) = (decoded, array) {
            read.push((array, extent, delivered));
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
/// or budgets less than one time; unsupported when it asks for centroids
/// the dataset splits with `TIME_CENTROID_EXTRA_PRECISION`.
pub(super) fn estimate(
    observation: &Msv4Observation,
    request: &AccessRequest,
    id: PartitionId,
) -> Result<(PartitionEstimate, ResolvedRequest), ObservationError> {
    let (dataset, entry) = observation.dataset(id)?;
    let resolved = request.resolve(entry, &dataset.times)?;
    let arrays = &dataset.groups[resolved.data_group];
    let roles = request.roles;
    if roles.time_centroid && dataset.centroid_extra_precision {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 TIME_CENTROID split with TIME_CENTROID_EXTRA_PRECISION: \
                   the profile carries one f64 offset per centroid",
        });
    }
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
    // An MSv4 dataset holds weights and no sigmas: the sigma rule takes
    // its fallback, recorded, or is refused.
    let weights = match request.weights {
        WeightRequest::InverseSigmaSquared {
            fallback: SigmaFallback::Refuse,
        } if roles.weights => {
            return Err(ObservationError::InvalidRequest {
                reason: "an MSv4 dataset holds no sigmas for 1/sigma^2 weights, and the request \
                         names no fallback"
                    .to_string(),
            });
        }
        _ => WeightSource {
            rule: WeightRule::Stored,
            variable: entry.data_groups[resolved.data_group].weights.clone(),
            per_channel: true,
        },
    };
    let span = resolved.times.clone();
    let (times, baselines) = (span.len(), entry.baselines.len());
    let channels = request.channels.len();
    let scalars = [roles.time_centroid, roles.exposure]
        .into_iter()
        .filter(|wanted| *wanted)
        .count();
    let delivered = resolved.correlations.products(entry).len();
    let bytes_per_sample = roles.sample_bytes(channels, delivered, true, scalars);
    // The cursor's scratch: the flag bytes before their conversion; the
    // values of every stored product when a subset is delivered; and the
    // stored weights, unless they are decoded straight into the batch (the
    // flags read them too).
    let stored = channels * entry.correlations.len();
    let gathered = resolved.correlations.indices().is_some();
    let weights_in_scratch = (roles.flags && !roles.weights) || (roles.weights && gathered);
    let scratch_bytes_per_sample = stored
        * (usize::from(roles.flags)
            + usize::from(roles.values && gathered) * 8
            + usize::from(weights_in_scratch) * 4);
    // `time.integration_time` is one constant: no interval array.
    let bytes_per_time = roles.time_bytes(0);
    let times_per_batch = times_per_batch(
        request.batch_bytes,
        baselines * (bytes_per_sample + scratch_bytes_per_sample) + bytes_per_time,
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
        let rows = span.start + batch * times_per_batch
            ..span.start + ((batch + 1) * times_per_batch).min(times);
        for &(array, extent, delivered) in &variables {
            let subset = subset(array, rows.clone(), &request.channels, extent);
            let (units, elements) = chunk_cost(array, &subset)?;
            decode_units += units;
            decoded += elements;
            selected += match (delivered, extent, resolved.correlations.indices()) {
                (false, ..) => 0,
                (true, Extent::Cube, Some(indices)) => {
                    subset.num_elements_usize() / entry.correlations.len() * indices.len()
                }
                (true, ..) => subset.num_elements_usize(),
            };
        }
    }
    let chunk = arrays.values.chunk_shape(&[0, 0, 0, 0]).map_err(storage)?;
    let estimate = PartitionEstimate {
        partition: id,
        times,
        first_time: span.start,
        weights: roles.weights.then_some(weights),
        baselines,
        bytes_per_sample,
        bytes_per_time,
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
    };
    Ok((estimate, resolved))
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
        let mut fill = buffers.begin(header, catalogue)?;
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
        fill.finish()?;
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
        let keep = self.resolved.correlations.indices();
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
        // The stored weights come first whenever flags are read: XRADIO pads
        // a cell no row filled with NaN and leaves its FLAG false, so a
        // non-finite stored weight marks an absent acquisition, and every
        // delivered flag carries it.
        let weights_in_place = roles.weights && keep.is_none();
        if let Some(subset) = prepare(
            roles.weights || roles.flags,
            Some(&arrays.weights),
            Extent::Cube,
        )? {
            let target = if weights_in_place {
                &mut batch.weights
            } else {
                &mut scratch.weights
            };
            target.resize(subset.num_elements_usize(), 0.0);
            decode_into(&arrays.weights, "float32", &subset, target, options)?;
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
            let weights: &[f32] = if weights_in_place {
                &batch.weights
            } else {
                &scratch.weights
            };
            let flag = |byte: u8, weight: f32| match byte {
                0 => Ok(!weight.is_finite()),
                1 => Ok(true),
                _ => Err(invalid(format!("FLAG byte {byte} is not a boolean"))),
            };
            let cells = scratch
                .flag_bytes
                .chunks_exact(stored)
                .zip(weights.chunks_exact(stored));
            match keep {
                None => {
                    for (&byte, &weight) in scratch.flag_bytes.iter().zip(weights) {
                        batch.flags.push(flag(byte, weight)?);
                    }
                }
                Some(keep) if self.resolved.flags_from_all => {
                    for (bytes, weights) in cells {
                        let mut any = false;
                        for (&byte, &weight) in bytes.iter().zip(weights) {
                            any |= flag(byte, weight)?;
                        }
                        batch.flags.extend(std::iter::repeat_n(any, keep.len()));
                    }
                }
                Some(keep) => {
                    for (bytes, weights) in cells {
                        for &product in keep {
                            batch.flags.push(flag(bytes[product], weights[product])?);
                        }
                    }
                }
            }
            counters.copied_elements += scratch.flag_bytes.len();
        }
        if let (true, Some(keep)) = (roles.weights, keep) {
            gather(&scratch.weights, stored, keep, &mut batch.weights);
            counters.copied_elements += batch.weights.len();
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
