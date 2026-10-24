// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traversals of an MSv4 dataset: factored time × baseline batches decoded
//! straight into the caller's buffers.

use std::ops::Range;

use casa_observation::{
    AccessRequest, BatchArrays, BatchBuffers, BatchHeader, Fill, IdLimits, IoCounters, Layout,
    ObservationError, Occupancy, Precision, Realization, Sharing, TopologyShape, VisibilityCursor,
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

/// A bound traversal of one MSv4 dataset: whole times in time order.
pub struct Msv4Cursor<'o> {
    observation: &'o Msv4Observation,
    dataset: &'o Dataset,
    arrays: &'o GroupArrays,
    request: AccessRequest,
    times_per_batch: usize,
    next_time: usize,
    groups: Option<GroupIndex>,
    /// Decoded flag bytes, checked and converted to `bool`.
    flag_bytes: Vec<u8>,
    realization: Realization,
    counters: IoCounters,
}

impl<'o> Msv4Cursor<'o> {
    pub(super) fn bind(
        observation: &'o Msv4Observation,
        request: &AccessRequest,
    ) -> Result<Self, ObservationError> {
        let dataset = observation
            .datasets
            .get(request.partition.0 as usize)
            .ok_or_else(|| ObservationError::InvalidRequest {
                reason: format!("no partition {:?}", request.partition),
            })?;
        if request.layout != Layout::SampleMajor {
            return Err(ObservationError::Unsupported {
                what: "the channel-major layout",
            });
        }
        let arrays = dataset
            .groups
            .iter()
            .find(|group| group.record.name == request.data_group)
            .ok_or_else(|| ObservationError::InvalidRequest {
                reason: format!("no data group {}", request.data_group),
            })?;
        // The visibilities were checked to be complex64 at opening.
        let (Precision::Native | Precision::Complex32 { .. }) = request.precision;
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
        let channel_total = observation
            .catalogue
            .partition(request.partition)
            .frequencies_hz
            .len();
        if request.channels.is_empty() || request.channels.end > channel_total {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "channels {:?} outside the partition's {channel_total}",
                    request.channels
                ),
            });
        }
        let baselines = dataset.baselines.len();
        let times_per_batch = (request.max_samples / baselines.max(1)).min(dataset.times.len());
        if times_per_batch == 0 {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "one time holds {baselines} samples, more than the {} a batch admits",
                    request.max_samples
                ),
            });
        }
        let groups = GroupIndex::new(
            request.partition,
            request.grouping,
            observation.open.group_closure,
            &dataset.times,
            &arrays.fields,
        )?;
        let realization = realization(dataset, arrays, request, times_per_batch)?;
        Ok(Self {
            observation,
            dataset,
            arrays,
            request: request.clone(),
            times_per_batch,
            next_time: 0,
            groups,
            flag_bytes: Vec::new(),
            realization,
            counters: IoCounters::default(),
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
    request: &AccessRequest,
) -> Vec<(&'a Array<FilesystemStore>, Extent)> {
    let roles = request.roles;
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

/// The chunks a traversal touches and the elements they hold, modelled
/// from the chunk grid.
fn realization(
    dataset: &Dataset,
    arrays: &GroupArrays,
    request: &AccessRequest,
    times_per_batch: usize,
) -> Result<Realization, ObservationError> {
    let times = dataset.times.len();
    let baselines = dataset.baselines.len();
    let mut decode_units = 0;
    let mut decoded = 0;
    let mut selected = 0;
    let batches = times.div_ceil(times_per_batch);
    let variables = requested(dataset, arrays, request);
    for batch in 0..batches {
        let rows = batch * times_per_batch..((batch + 1) * times_per_batch).min(times);
        for &(array, extent) in &variables {
            let subset = subset(array, rows.clone(), &request.channels, extent);
            let (units, elements) = chunk_cost(array, &subset)?;
            decode_units += units;
            decoded += elements;
            selected += subset.num_elements_usize();
        }
    }
    let per_sample = request.channels.len() * arrays.values.shape()[3] as usize;
    Ok(Realization {
        layout: Layout::SampleMajor,
        factored: true,
        samples_per_batch: times_per_batch * baselines,
        batches,
        batch_payload_bytes: (times_per_batch * baselines)
            .saturating_mul(per_sample * (8 + 1 + 4) + 3 * 8 + 2 * 8),
        decode_units,
        decoded_elements: decoded,
        selected_elements: selected,
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

impl Msv4Cursor<'_> {
    fn header(&self, times: usize) -> BatchHeader {
        let dataset = self.dataset;
        let catalogue = &self.observation.catalogue;
        BatchHeader {
            partition: self.request.partition,
            topology: TopologyShape::TimeBaseline {
                times,
                occupancy: Occupancy::Unknown,
            },
            first_channel: self.request.channels.start,
            roles: self.request.roles,
            spectral_weights: true,
            row_flags: false,
            time_scale: dataset.time_scale,
            time_origin: dataset.time_origin,
            frequency_frame: catalogue.partition(self.request.partition).frequency_frame,
            uvw_frame: self.arrays.uvw_frame.effective,
            interval: Sharing::Constant(dataset.interval_s.unwrap_or(f64::NAN)),
            exposure: Sharing::PerSample,
            scan: Sharing::PerTime,
            limits: IdLimits {
                antennas: catalogue.antennas.len(),
                fields: catalogue.fields.len(),
                scans: catalogue.scans.len(),
            },
        }
    }

    fn fill_batch(
        &mut self,
        buffers: &mut BatchBuffers,
        rows: Range<usize>,
    ) -> Result<(), ObservationError> {
        let dataset = self.dataset;
        let arrays = self.arrays;
        let entry = self.observation.catalogue.partition(self.request.partition);
        let capacity = buffers.capacity_bytes();
        let header = self.header(rows.len());
        let mut fill = buffers.begin(header);
        let batch = fill.arrays();
        batch.group_name.clone_from(&self.request.data_group);
        let group_entry = entry
            .data_groups
            .iter()
            .find(|group| group.name == self.request.data_group)
            .expect("bound data group");
        batch.units.clone_from(&group_entry.units);
        batch.correlations.extend_from_slice(&entry.correlations);
        batch
            .frequency_hz
            .extend_from_slice(&entry.frequencies_hz[self.request.channels.clone()]);
        batch
            .time_offsets_s
            .extend_from_slice(&dataset.times[rows.clone()]);
        batch.field.extend_from_slice(&arrays.fields[rows.clone()]);
        batch.baselines.extend_from_slice(&dataset.baselines);
        if self.request.roles.scan {
            let scans = dataset.scans.as_ref().expect("checked at binding");
            batch.scan.extend_from_slice(&scans[rows.clone()]);
        }
        self.decode_batch(batch, &rows)?;
        if let Some(groups) = self.groups.as_mut() {
            groups.fragments(rows, dataset.baselines.len(), &mut batch.fragments);
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
    /// `batch`.
    fn decode_batch(
        &mut self,
        batch: &mut BatchArrays,
        rows: &Range<usize>,
    ) -> Result<(), ObservationError> {
        let (dataset, arrays) = (self.dataset, self.arrays);
        let options = &self.observation.options;
        let roles = self.request.roles;
        let channels = &self.request.channels;
        let counters = &mut self.counters;
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
            batch
                .values
                .resize(subset.num_elements_usize(), Complex32::default());
            decode_into(
                &arrays.values,
                "complex64",
                &subset,
                &mut batch.values,
                options,
            )?;
        }
        if let Some(subset) = prepare(roles.flags, Some(&arrays.flags), Extent::Cube)? {
            self.flag_bytes.resize(subset.num_elements_usize(), 0);
            decode_into(
                &arrays.flags,
                "bool",
                &subset,
                &mut self.flag_bytes,
                options,
            )?;
            for &byte in &self.flag_bytes {
                batch.flags.push(match byte {
                    0 => false,
                    1 => true,
                    _ => return Err(invalid(format!("FLAG byte {byte} is not a boolean"))),
                });
            }
            counters.copied_elements += self.flag_bytes.len();
        }
        let float64 = [
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
        ];
        if let Some(subset) = prepare(roles.weights, Some(&arrays.weights), Extent::Cube)? {
            batch.weights.resize(subset.num_elements_usize(), 0.0);
            decode_into(
                &arrays.weights,
                "float32",
                &subset,
                &mut batch.weights,
                options,
            )?;
        }
        for (wanted, array, extent, target) in float64 {
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
    fn realization(&self) -> &Realization {
        &self.realization
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<Option<Fill>, ObservationError> {
        if let Some(key) = self.groups.as_mut().and_then(GroupIndex::pop_end) {
            buffers.clear();
            return Ok(Some(Fill::GroupEnd(key)));
        }
        let times = self.dataset.times.len();
        if self.next_time == times {
            return Ok(None);
        }
        let rows = self.next_time..(self.next_time + self.times_per_batch).min(times);
        self.fill_batch(buffers, rows.clone())?;
        self.next_time = rows.end;
        self.counters.batches += 1;
        Ok(Some(Fill::Batch))
    }

    fn counters(&self) -> IoCounters {
        self.counters
    }
}
