// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reading an MSv4 processing set: each `VisibilityXds` dataset is a
//! partition, delivered as factored time × baseline batches.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use casa_observation::{
    AccessRequest, AntennaEntry, AntennaId, BatchBuffers, BatchShape, Catalogue, Correlation,
    DataGroupEntry, FieldEntry, Fill, IoCounters, Layout, ObservationError, Occupancy,
    OrderedBaseline, PartitionEntry, Precision, Realization, TopologyShape, VisibilityCursor,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MjdHighPrec};
use num_complex::Complex32;
use zarrs::array::{
    Array, ArrayBytesDecodeIntoTarget, ArrayBytesFixedDisjointView, ArraySubset, CodecOptions,
};
use zarrs::filesystem::FilesystemStore;
use zarrs::group::Group;

use super::metadata::{
    DataGroupRecord, SCHEMA_VERSION, data_groups, direction_frame, frequency_frame, invalid,
    numbers, open_array, storage, strings, text, time_reference, visibility_units,
};
use crate::observation_io::grouping::GroupIndex;

/// The payload variables of one data group.
struct GroupArrays {
    values: Array<FilesystemStore>,
    flags: Array<FilesystemStore>,
    weights: Array<FilesystemStore>,
    uvw: Array<FilesystemStore>,
}

/// One `VisibilityXds` dataset of the processing set.
struct Dataset {
    /// `time` coordinate: seconds from `time_origin`.
    times: Vec<f64>,
    time_scale: EpochRef,
    time_origin: MjdHighPrec,
    baselines: Vec<OrderedBaseline>,
    /// Field index at each time.
    fields: Vec<u32>,
    uvw_frame: DirectionRef,
    groups: Vec<(DataGroupRecord, GroupArrays)>,
}

/// An opened MSv4 processing set (XRADIO `VisibilityXds` schema 4.0.1 on
/// local Zarr v3) and its catalogue.
///
/// Each visibility dataset is one partition. Antennas are the union of the
/// datasets' `antenna_xds` names; fields of their `field_and_source` datasets.
/// The reader checks every coordinate unit against the schema's (Hz, s, m,
/// rad) and refuses others rather than rescaling them.
pub struct Msv4Observation {
    catalogue: Catalogue,
    datasets: Vec<Dataset>,
    options: CodecOptions,
}

impl Msv4Observation {
    /// Open the processing set at `path`.
    ///
    /// Decoding runs on the calling thread: the codec options fix zarrs'
    /// concurrency target at one rather than its default of the global rayon
    /// pool's size.
    ///
    /// # Errors
    ///
    /// When the store is not a processing set of the supported profile.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ObservationError> {
        let store = Arc::new(FilesystemStore::new(path.as_ref()).map_err(storage)?);
        let root = Group::open(store.clone(), "/").map_err(storage)?;
        if text(root.attributes(), "type", "the processing set")? != "processing_set" {
            return Err(invalid("the root group is not a processing_set"));
        }
        let mut names = std::fs::read_dir(path.as_ref())
            .map_err(storage)?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().join("zarr.json").is_file())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        let mut observation = Self {
            catalogue: Catalogue::default(),
            datasets: Vec::new(),
            options: CodecOptions::default().with_concurrent_target(1),
        };
        for name in names {
            observation.add_dataset(&store, &format!("/{name}"))?;
        }
        if observation.datasets.is_empty() {
            return Err(invalid("the processing set has no visibility datasets"));
        }
        Ok(observation)
    }

    /// The catalogue.
    #[must_use]
    pub fn catalogue(&self) -> &Catalogue {
        &self.catalogue
    }

    fn add_dataset(
        &mut self,
        store: &Arc<FilesystemStore>,
        path: &str,
    ) -> Result<(), ObservationError> {
        let group = Group::open(store.clone(), path).map_err(storage)?;
        let attributes = group.attributes();
        if text(attributes, "type", path)? != "visibility" {
            return Err(ObservationError::Unsupported {
                what: "an MSv4 dataset other than a visibility dataset",
            });
        }
        if text(attributes, "schema_version", path)? != SCHEMA_VERSION {
            return Err(ObservationError::Unsupported {
                what: "an MSv4 schema version other than 4.0.1",
            });
        }
        let antenna_names = strings(store, &format!("{path}/antenna_xds/antenna_name"))?;
        let stations = strings(store, &format!("{path}/antenna_xds/station_name"))?;
        let antenna_ids = antenna_names
            .iter()
            .zip(&stations)
            .map(|(name, station)| self.antenna(name, station))
            .collect::<Vec<_>>();
        let baseline_antenna = |which: &str| -> Result<Vec<AntennaId>, ObservationError> {
            strings(store, &format!("{path}/baseline_antenna{which}_name"))?
                .iter()
                .map(|name| {
                    antenna_names
                        .iter()
                        .position(|known| known == name)
                        .map(|index| antenna_ids[index])
                        .ok_or_else(|| {
                            invalid(format!("baseline antenna {name} is not in antenna_xds"))
                        })
                })
                .collect()
        };
        let baselines: Vec<OrderedBaseline> = baseline_antenna("1")?
            .into_iter()
            .zip(baseline_antenna("2")?)
            .map(|(antenna1, antenna2)| OrderedBaseline { antenna1, antenna2 })
            .collect();
        let (times, time_attributes) = numbers::<f64>(store, &format!("{path}/time"))?;
        let (time_scale, time_origin) = time_reference(&time_attributes, "time")?;
        let (frequencies_hz, frequency_attributes) =
            numbers::<f64>(store, &format!("{path}/frequency"))?;
        let frequency_frame = frequency_frame(&frequency_attributes, "frequency")?;
        let channel_width = frequency_attributes
            .get("channel_width")
            .and_then(|width| width.get("data"))
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| invalid("frequency has no channel_width"))?;
        let correlations = strings(store, &format!("{path}/polarization"))?
            .iter()
            .map(|label| {
                Correlation::from_label(label)
                    .ok_or_else(|| invalid(format!("unknown polarization {label}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let records = data_groups(attributes)?;
        let first = records.first().ok_or_else(|| invalid("no data group"))?;
        let field_names = strings(store, &format!("{path}/field_name"))?;
        let fields = self.fields(
            store,
            &format!("{path}/{}", first.field_and_source),
            &field_names,
        )?;
        let mut groups = Vec::new();
        let mut entries = Vec::new();
        let mut uvw_frame = None;
        for record in records {
            let arrays = GroupArrays {
                values: open_array(store, &format!("{path}/{}", record.correlated_data))?,
                flags: open_array(store, &format!("{path}/{}", record.flag))?,
                weights: open_array(store, &format!("{path}/{}", record.weight))?,
                uvw: open_array(store, &format!("{path}/{}", record.uvw))?,
            };
            check_payload(
                &arrays,
                times.len(),
                &baselines,
                frequencies_hz.len(),
                correlations.len(),
            )?;
            let uvw_attributes = arrays.uvw.attributes();
            super::metadata::require_units(uvw_attributes, "m", "UVW")?;
            uvw_frame = Some(direction_frame(uvw_attributes, "UVW")?);
            entries.push(DataGroupEntry {
                name: record.name.clone(),
                visibilities: record.correlated_data.clone(),
                flags: record.flag.clone(),
                weights: record.weight.clone(),
                uvw: record.uvw.clone(),
                units: visibility_units(arrays.values.attributes()),
            });
            groups.push((record, arrays));
        }
        self.catalogue.partitions.push(PartitionEntry {
            spectral_window: text(&frequency_attributes, "spectral_window_name", "frequency")?
                .to_string(),
            channel_widths_hz: vec![channel_width; frequencies_hz.len()],
            frequencies_hz,
            frequency_frame,
            correlations,
            data_groups: entries,
        });
        self.datasets.push(Dataset {
            times,
            time_scale,
            time_origin,
            baselines,
            fields,
            uvw_frame: uvw_frame.ok_or_else(|| invalid("no data group"))?,
            groups,
        });
        Ok(())
    }

    /// The catalogue antenna named `name` at `station`, added if new.
    fn antenna(&mut self, name: &str, station: &str) -> AntennaId {
        let antennas = &mut self.catalogue.antennas;
        let index = antennas
            .iter()
            .position(|known| known.name == name && known.station == station)
            .unwrap_or_else(|| {
                antennas.push(AntennaEntry {
                    name: name.to_string(),
                    station: station.to_string(),
                });
                antennas.len() - 1
            });
        AntennaId(u32::try_from(index).expect("few antennas"))
    }

    /// The catalogue field of each of `per_time`'s names, read from the
    /// `field_and_source` dataset at `path`.
    fn fields(
        &mut self,
        store: &Arc<FilesystemStore>,
        path: &str,
        per_time: &[String],
    ) -> Result<Vec<u32>, ObservationError> {
        let names = strings(store, &format!("{path}/field_name"))?;
        let (centres, attributes) =
            numbers::<f64>(store, &format!("{path}/FIELD_PHASE_CENTER_DIRECTION"))?;
        super::metadata::require_units(&attributes, "rad", "FIELD_PHASE_CENTER_DIRECTION")?;
        let frame = direction_frame(&attributes, "FIELD_PHASE_CENTER_DIRECTION")?;
        let mut indices = Vec::new();
        for (row, name) in names.iter().enumerate() {
            let entry = FieldEntry {
                name: name.clone(),
                phase_centre: MDirection::from_angles(
                    centres[2 * row],
                    centres[2 * row + 1],
                    frame,
                ),
            };
            let fields = &mut self.catalogue.fields;
            let index = fields
                .iter()
                .position(|known| known.name == *name)
                .unwrap_or_else(|| {
                    fields.push(entry);
                    fields.len() - 1
                });
            indices.push(u32::try_from(index).expect("few fields"));
        }
        per_time
            .iter()
            .map(|name| {
                names
                    .iter()
                    .position(|known| known == name)
                    .map(|row| indices[row])
                    .ok_or_else(|| invalid(format!("field {name} is not in {path}")))
            })
            .collect()
    }

    /// Bind a traversal of `request`: whole times per batch, as many as
    /// `request.max_samples` admits.
    ///
    /// # Errors
    ///
    /// When the request names an unknown partition, data group or channel,
    /// asks for the channel-major layout or a precision the store cannot
    /// deliver, or one time's baselines exceed `max_samples`.
    pub fn bind_read(&self, request: &AccessRequest) -> Result<Msv4Cursor<'_>, ObservationError> {
        let dataset = self
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
        let group = dataset
            .groups
            .iter()
            .position(|(record, _)| record.name == request.data_group)
            .ok_or_else(|| ObservationError::InvalidRequest {
                reason: format!("no data group {}", request.data_group),
            })?;
        let arrays = &dataset.groups[group].1;
        if arrays.values.data_type().fixed_size() != Some(size_of::<Complex32>()) {
            return Err(ObservationError::Unsupported {
                what: "MSv4 visibilities other than complex64",
            });
        }
        let (Precision::Native | Precision::Complex32 { .. }) = request.precision;
        let channel_total = self
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
        let times_per_batch = request.max_samples / baselines.max(1);
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
            request.closure,
            &dataset.times,
            &dataset.fields,
        )?;
        let realization = realization(
            arrays,
            request,
            dataset.times.len(),
            times_per_batch,
            baselines,
        )?;
        Ok(Msv4Cursor {
            observation: self,
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

/// Check that every payload variable has the dataset's dimensions.
fn check_payload(
    arrays: &GroupArrays,
    times: usize,
    baselines: &[OrderedBaseline],
    channels: usize,
    correlations: usize,
) -> Result<(), ObservationError> {
    let cube = [times, baselines.len(), channels, correlations].map(|n| n as u64);
    for (name, array, shape) in [
        ("visibilities", &arrays.values, &cube[..]),
        ("flags", &arrays.flags, &cube[..]),
        ("weights", &arrays.weights, &cube[..]),
        ("UVW", &arrays.uvw, &[cube[0], cube[1], 3][..]),
    ] {
        if array.shape() != shape {
            return Err(invalid(format!(
                "{name} has shape {:?}, not {shape:?}",
                array.shape()
            )));
        }
    }
    Ok(())
}

/// The chunks a traversal decodes and the elements they hold.
fn realization(
    arrays: &GroupArrays,
    request: &AccessRequest,
    times: usize,
    times_per_batch: usize,
    baselines: usize,
) -> Result<Realization, ObservationError> {
    let roles = request.roles;
    let mut decode_units = 0;
    let mut decoded = 0;
    let mut selected = 0;
    let batches = times.div_ceil(times_per_batch);
    for batch in 0..batches {
        let rows = batch * times_per_batch..((batch + 1) * times_per_batch).min(times);
        for (wanted, array, uvw) in [
            (roles.values, &arrays.values, false),
            (roles.flags, &arrays.flags, false),
            (roles.weights, &arrays.weights, false),
            (roles.uvw, &arrays.uvw, true),
        ] {
            if !wanted {
                continue;
            }
            let subset = subset(array, rows.clone(), &request.channels, uvw);
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
        batch_payload_bytes: times_per_batch * baselines * (per_sample * (8 + 1 + 4) + 3 * 8),
        decode_units,
        decoded_elements: decoded,
        selected_elements: selected,
    })
}

/// The array subset of times `rows` and channels `channels` (all of UVW's
/// last axis).
fn subset(
    array: &Array<FilesystemStore>,
    rows: Range<usize>,
    channels: &Range<usize>,
    uvw: bool,
) -> ArraySubset {
    let shape = array.shape();
    let rows = rows.start as u64..rows.end as u64;
    if uvw {
        ArraySubset::new_with_ranges(&[rows, 0..shape[1], 0..3])
    } else {
        ArraySubset::new_with_ranges(&[
            rows,
            0..shape[1],
            channels.start as u64..channels.end as u64,
            0..shape[3],
        ])
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

/// Decode `subset` of `array` straight into `destination`, whose length is
/// the subset's element count: zarrs writes each chunk's selected elements
/// into place, with no intermediate whole-subset copy.
fn decode_into<T: bytemuck::Pod>(
    array: &Array<FilesystemStore>,
    subset: &ArraySubset,
    destination: &mut [T],
    options: &CodecOptions,
) -> Result<(), ObservationError> {
    let shape: Vec<u64> = subset.shape().to_vec();
    let bytes = bytemuck::cast_slice_mut::<T, u8>(destination);
    let cells = unsafe_cell_slice::UnsafeCellSlice::new(bytes);
    // SAFETY: the view covers the whole destination, which this function
    // borrows exclusively, so no other view of these bytes exists.
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

impl Msv4Cursor<'_> {
    fn fill_batch(
        &mut self,
        buffers: &mut BatchBuffers,
        rows: Range<usize>,
    ) -> Result<BatchShape, ObservationError> {
        let dataset = self.dataset;
        let entry = self.observation.catalogue.partition(self.request.partition);
        let options = &self.observation.options;
        let capacity = buffers.capacity_bytes();
        buffers.clear();
        buffers.group_name.clone_from(&self.request.data_group);
        let group_entry = entry
            .data_groups
            .iter()
            .find(|group| group.name == self.request.data_group)
            .expect("bound data group");
        buffers.units = Some(group_entry.units.clone());
        buffers.correlations.extend_from_slice(&entry.correlations);
        buffers
            .frequency_hz
            .extend_from_slice(&entry.frequencies_hz[self.request.channels.clone()]);
        buffers
            .time_offsets_s
            .extend_from_slice(&dataset.times[rows.clone()]);
        buffers
            .field
            .extend_from_slice(&dataset.fields[rows.clone()]);
        buffers.baselines.extend_from_slice(&dataset.baselines);
        let roles = self.request.roles;
        let arrays = self.arrays;
        let channels = &self.request.channels;
        let mut decode = |wanted: bool,
                          array: &Array<FilesystemStore>,
                          uvw: bool|
         -> Result<Option<ArraySubset>, ObservationError> {
            if !wanted {
                return Ok(None);
            }
            let subset = subset(array, rows.clone(), channels, uvw);
            self.counters.read_calls += 1;
            self.counters.decode_units += chunk_cost(array, &subset)?.0;
            Ok(Some(subset))
        };
        if let Some(subset) = decode(roles.values, &arrays.values, false)? {
            buffers
                .values
                .resize(subset.num_elements_usize(), Complex32::default());
            decode_into(&arrays.values, &subset, &mut buffers.values, options)?;
        }
        if let Some(subset) = decode(roles.flags, &arrays.flags, false)? {
            self.flag_bytes.resize(subset.num_elements_usize(), 0);
            decode_into(&arrays.flags, &subset, &mut self.flag_bytes, options)?;
            for &byte in &self.flag_bytes {
                buffers.flags.push(match byte {
                    0 => false,
                    1 => true,
                    _ => return Err(invalid(format!("FLAG byte {byte} is not a boolean"))),
                });
            }
            self.counters.copied_elements += self.flag_bytes.len();
        }
        if let Some(subset) = decode(roles.weights, &arrays.weights, false)? {
            buffers.weights.resize(subset.num_elements_usize(), 0.0);
            decode_into(&arrays.weights, &subset, &mut buffers.weights, options)?;
        }
        if let Some(subset) = decode(roles.uvw, &arrays.uvw, true)? {
            buffers.uvw.resize(subset.num_elements_usize(), 0.0);
            decode_into(&arrays.uvw, &subset, &mut buffers.uvw, options)?;
        }
        if let Some(groups) = self.groups.as_mut() {
            groups.fragments(
                rows.clone(),
                dataset.baselines.len(),
                &mut buffers.fragments,
            );
        }
        self.counters.delivered_bytes += buffers.values.len() * size_of::<Complex32>()
            + buffers.flags.len()
            + buffers.weights.len() * size_of::<f32>()
            + buffers.uvw.len() * size_of::<f64>();
        if buffers.capacity_bytes() > capacity {
            self.counters.buffer_growths += 1;
        }
        Ok(BatchShape {
            partition: self.request.partition,
            topology: TopologyShape::TimeBaseline {
                times: rows.len(),
                occupancy: Occupancy::Unknown,
            },
            channels: self.request.channels.clone(),
            roles,
            spectral_weights: true,
            time_scale: dataset.time_scale,
            time_origin: dataset.time_origin,
            frequency_frame: entry.frequency_frame,
            uvw_frame: dataset.uvw_frame,
            row_flags: false,
        })
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
        let shape = self.fill_batch(buffers, rows.clone())?;
        self.next_time = rows.end;
        self.counters.batches += 1;
        Ok(Some(Fill::Batch(shape)))
    }

    fn counters(&self) -> IoCounters {
        self.counters
    }
}
