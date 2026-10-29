// SPDX-License-Identifier: LGPL-3.0-or-later
//! A narrow MSv4 writer: known-shape `VisibilityXds` datasets in a new
//! local processing set ([`Msv4ProcessingSetWriter`]), each filled from
//! batches ([`Msv4Writer`]) and the set published whole.
//!
//! What the dataset *is* comes from the common vocabulary: the source's
//! [`Catalogue`] (antennas, fields, scans, the partition and its data
//! group) and a [`TimeBaselineGrid`] (baselines, times, the field and scan
//! of each time, the interval). How it is stored comes from
//! [`Msv4StorageOptions`]: dataset name, provenance, chunks and codec.
//!
//! The trial profile writes what the reader admits (module docs): the main
//! dataset's coordinates and its data groups: the first declared holds
//! `VISIBILITY`, `FLAG`, `WEIGHT` and `UVW`, and each further one
//! (`corrected`, `model`) its own `VISIBILITY_<GROUP>` beside the first's
//! flags, weights and UVW, as XRADIO lays out a converted MSv2. Then
//! `TIME_CENTROID` and `EFFECTIVE_INTEGRATION_TIME`, an `antenna_xds` of the
//! antennas the dataset's baselines name (as XRADIO's converter writes it),
//! and a `field_and_source_<group>_xds` of the first group, which every
//! group names: the fields' phase centres, or, when they follow an
//! ephemeris, the ephemeris's samples and each time's phase centre
//! (`field_and_source_ephemeris`, `write/fields.rs`). It writes no
//! pointing, system-calibration, weather or phase-calibration datasets and
//! no `EFFECTIVE_CHANNEL_WIDTH`. Every time must be written exactly once,
//! from batches whose coordinates and context match the declaration
//! ([`Msv4Writer::write_batch`]); the payload variables' fill values are
//! the padding convention (flag true, values and weights NaN, as XRADIO
//! pads them), so a cell never delivered reads back as padding; the
//! processing set is built in a staging directory of its own and published
//! only by [`Msv4ProcessingSetWriter::finish`].

mod chunks;
mod fields;
mod payload;
mod pointing;
mod set;

pub(in crate::observation_io) use fields::kept_samples;
pub use set::Msv4ProcessingSetWriter;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use casa_observation::{
    AntennaEntry, AntennaId, Catalogue, DataGroupEntry, ObservationError, PartitionEntry,
    PartitionId, Receptors, ScanId, TimeBaselineGrid, VisibilityUnits,
};
use casa_types::measures::position::PositionRef;
use casa_types::measures::{EpochView, MjdHighPrec};
use casa_types::quanta::{Quantity, Unit};
use ndarray::ArrayView1;
use serde_json::{Value, json};
use zarrs::array::{
    Array, ArrayMetadata, ArrayMetadataOptions, ArraySubset, CodecOptions, Element,
};
use zarrs::filesystem::FilesystemStore;
use zarrs::group::{Group, GroupMetadata};
use zarrs::metadata::v3::{ArrayMetadataV3, GroupMetadataV3};

use super::metadata::{SCHEMA_VERSION, UNIX_EPOCH_MJD, storage};

/// The bytes-to-bytes codec of the payload variables. Coordinates always
/// use Zstandard, as XRADIO's converter does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Msv4Codec {
    /// No compression.
    None,
    /// Blosc with LZ4, level 5, no shuffle: XRADIO's default.
    BloscLz4,
    /// Zstandard, level 0.
    Zstd,
}

/// How a new dataset is stored: everything about the Zarr realization and
/// its provenance, nothing about the observation.
#[derive(Clone, Debug)]
pub struct Msv4StorageOptions {
    /// The dataset's name in the processing set.
    pub dataset_name: String,
    /// Software recorded as the creator.
    pub creator: String,
    /// Creation time, ISO 8601.
    pub creation_date: String,
    /// Chunk shape of the payload variables, `[time, baseline, frequency,
    /// polarization]`.
    pub chunks: [u64; 4],
    /// Bytes codec of the payload variables.
    pub codec: Msv4Codec,
}

/// What a writer did, for the evidence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Msv4WriteCounters {
    /// Region writes issued, over every variable.
    pub region_writes: usize,
    /// Payload elements encoded.
    pub elements: usize,
    /// Elements copied to reorder or broadcast a batch into the stored
    /// layout before encoding.
    pub packed_elements: usize,
    /// Elements held (copied) until their chunk was complete, because
    /// their batch covered that chunk only partly; none when every batch
    /// covers whole chunks of [`Msv4Writer::chunk_times`] times.
    pub held_elements: usize,
    /// Writes that covered a chunk only partly along time, counted for
    /// every variable written: zarrs decodes, merges and re-encodes such a
    /// chunk when its other times are written. Held times make this zero
    /// for batches in time order; batches out of order, or a dataset
    /// finished with times still held toward a chunk, count here.
    pub partial_chunks: usize,
}

/// What every batch written must agree with: the declaration, resolved
/// once at creation.
struct Destination {
    partition: PartitionId,
    /// The declared data groups; the first carries every role.
    groups: Vec<DataGroupEntry>,
    entry: PartitionEntry,
    grid: TimeBaselineGrid,
    /// The grid's times in Unix seconds, by bit pattern.
    times: HashMap<u64, usize>,
}

/// The dataset's payload variables.
struct Payload {
    /// Each declared group's visibilities, in declaration order.
    values: Vec<Array<FilesystemStore>>,
    flags: Array<FilesystemStore>,
    weights: Array<FilesystemStore>,
    uvw: Array<FilesystemStore>,
    centroid: Array<FilesystemStore>,
    exposure: Array<FilesystemStore>,
}

/// One dataset of a processing set being created
/// ([`Msv4ProcessingSetWriter::dataset`]).
pub struct Msv4Writer {
    /// The dataset's name in the processing set.
    dataset: String,
    store: Arc<FilesystemStore>,
    registry: Arc<set::Registry>,
    payload: Option<Payload>,
    target: Destination,
    shape: [usize; 4],
    written: Vec<bool>,
    chunk_times: usize,
    options: CodecOptions,
    scratch: payload::Scratch,
    /// Times held toward a chunk a batch covered only partly.
    held: chunks::Held,
    counters: Msv4WriteCounters,
}

/// The codec chain. The bytes codec always carries its endianness: zarrs
/// writes a configuration-less codec as a bare name, which zarr-python
/// 3.4.1 refuses to parse.
fn codecs(codec: Msv4Codec, type_size: usize) -> Value {
    let bytes = json!({"name": "bytes", "configuration": {"endian": "little"}});
    match codec {
        Msv4Codec::None => json!([bytes]),
        Msv4Codec::BloscLz4 => json!([bytes, {"name": "blosc", "configuration": {
            "typesize": type_size, "cname": "lz4", "clevel": 5, "shuffle": "noshuffle", "blocksize": 0
        }}]),
        Msv4Codec::Zstd => {
            json!([bytes, {"name": "zstd", "configuration": {"level": 0, "checksum": false}}])
        }
    }
}

/// A Zarr v3 array description.
struct ArraySpec<'a> {
    shape: &'a [u64],
    chunks: &'a [u64],
    data_type: Value,
    codecs: Value,
    fill: Value,
    dimensions: &'a [&'a str],
    attributes: Value,
}

fn create_array(
    store: &Arc<FilesystemStore>,
    path: &str,
    spec: ArraySpec<'_>,
) -> Result<Array<FilesystemStore>, ObservationError> {
    let metadata: ArrayMetadataV3 = serde_json::from_value(json!({
        "zarr_format": 3,
        "node_type": "array",
        "shape": spec.shape,
        "data_type": spec.data_type,
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": spec.chunks}},
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": spec.fill,
        "codecs": spec.codecs,
        "attributes": spec.attributes,
        "dimension_names": spec.dimensions,
    }))
    .map_err(storage)?;
    let array = Array::new_with_metadata(store.clone(), path, ArrayMetadata::V3(metadata))
        .map_err(storage)?;
    array
        .store_metadata_opt(&metadata_options())
        .map_err(storage)?;
    Ok(array)
}

/// Metadata as written, without the `_zarrs` provenance attribute zarrs adds
/// by default.
fn metadata_options() -> ArrayMetadataOptions {
    ArrayMetadataOptions::default().with_include_zarrs_metadata(false)
}

fn create_group(
    store: &Arc<FilesystemStore>,
    path: &str,
    attributes: Value,
) -> Result<(), ObservationError> {
    let metadata: GroupMetadataV3 = serde_json::from_value(json!({
        "zarr_format": 3,
        "node_type": "group",
        "attributes": attributes,
    }))
    .map_err(storage)?;
    Group::new_with_metadata(store.clone(), path, GroupMetadata::V3(metadata))
        .map_err(storage)?
        .store_metadata()
        .map_err(storage)
}

/// A one-dimensional coordinate written in one chunk.
fn coordinate<T: Element>(
    store: &Arc<FilesystemStore>,
    path: &str,
    data_type: Value,
    fill: Value,
    dimension: &str,
    values: &[T],
    attributes: Value,
) -> Result<(), ObservationError> {
    let shape = [values.len() as u64];
    let array = create_array(
        store,
        path,
        ArraySpec {
            shape: &shape,
            chunks: &[shape[0].max(1)],
            data_type,
            codecs: codecs(Msv4Codec::Zstd, 8),
            fill,
            dimensions: &[dimension],
            attributes,
        },
    )?;
    array
        .store_array_subset(&ArraySubset::new_with_shape(shape.to_vec()), values)
        .map_err(storage)
}

/// A string coordinate of any shape, as `fixed_length_utf32` sized to its
/// longest element, as numpy's `<U` arrays are stored.
fn strings(
    store: &Arc<FilesystemStore>,
    path: &str,
    shape: &[u64],
    dimensions: &[&str],
    values: &[String],
) -> Result<Array<FilesystemStore>, ObservationError> {
    let length = values
        .iter()
        .map(|value| value.chars().count())
        .max()
        .unwrap_or(0)
        .max(1);
    let chars: Vec<Vec<char>> = values
        .iter()
        .map(|value| {
            let mut chars: Vec<char> = value.chars().collect();
            chars.resize(length, '\0');
            chars
        })
        .collect();
    let array = create_array(
        store,
        path,
        ArraySpec {
            shape,
            chunks: shape,
            data_type: json!({"name": "fixed_length_utf32", "configuration": {"length_bytes": 4 * length}}),
            codecs: codecs(Msv4Codec::Zstd, 4),
            fill: json!(""),
            dimensions,
            attributes: json!({}),
        },
    )?;
    array
        .store_array_subset(&ArraySubset::new_with_shape(shape.to_vec()), &chars)
        .map_err(storage)?;
    Ok(array)
}

fn quantity(value: f64, units: &str) -> Value {
    json!({"data": value, "dims": [], "attrs": {"units": units, "type": "quantity"}})
}

/// `quantity` in `unit`.
fn value_in(quantity: &Quantity, unit: &str) -> Result<f64, ObservationError> {
    quantity
        .get_value_in(&Unit::new(unit).expect("unit parses"))
        .map_err(|_| ObservationError::InvalidRequest {
            reason: format!("{quantity:?} is not convertible to {unit}"),
        })
}

const PAYLOAD_COORDINATES: &str =
    "baseline_antenna1_name baseline_antenna2_name field_name scan_name";

fn request_error(reason: impl Into<String>) -> ObservationError {
    ObservationError::InvalidRequest {
        reason: reason.into(),
    }
}

/// The visibility variable of declared group `index` named `name`: the
/// first group's is `VISIBILITY`, any other's `VISIBILITY_<NAME>`
/// (XRADIO's `VISIBILITY_CORRECTED`, `VISIBILITY_MODEL`).
fn visibility_variable(index: usize, name: &str) -> String {
    if index == 0 {
        "VISIBILITY".to_string()
    } else {
        format!("VISIBILITY_{}", name.to_uppercase())
    }
}

/// Resolve and check the declaration: the partition, its data groups
/// (which must share the first's flags, weights and UVW), the grid's
/// identities against the catalogue, and the grid's times in Unix seconds.
fn destination(
    catalogue: &Catalogue,
    partition: PartitionId,
    data_groups: &[&str],
    grid: &TimeBaselineGrid,
) -> Result<(Destination, Vec<f64>), ObservationError> {
    let groups = data_groups
        .iter()
        .map(|name| {
            catalogue
                .data_group(partition, name)
                .cloned()
                .ok_or_else(|| request_error(format!("no data group {name} in {partition:?}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some(first) = groups.first() else {
        return Err(request_error("a dataset declares at least one data group"));
    };
    if groups.iter().any(|group| {
        group.flags != first.flags
            || group.weights != first.weights
            || group.uvw != first.uvw
            || group.uvw_frame != first.uvw_frame
    }) {
        return Err(ObservationError::Unsupported {
            what: "data groups that do not share the first group's flags, weights and UVW",
        });
    }
    let mut names: Vec<&str> = data_groups.to_vec();
    names.sort_unstable();
    names.dedup();
    if names.len() != data_groups.len() {
        return Err(request_error("a data group is declared twice"));
    }
    let entry = catalogue.partition(partition).clone();
    let times = grid.times_s.len();
    if grid.field_per_time.len() != times || grid.scan_per_time.len() != times {
        return Err(request_error("the grid has one field and scan per time"));
    }
    if grid.baselines.is_empty() || times == 0 {
        return Err(request_error(
            "the grid has at least one time and one baseline",
        ));
    }
    if !grid.times_s.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(request_error("the grid's times are strictly ascending"));
    }
    if grid
        .field_per_time
        .iter()
        .any(|f| f.0 as usize >= catalogue.fields.len())
        || grid
            .scan_per_time
            .iter()
            .any(|s| s.0 as usize >= catalogue.scans.len())
        || grid.baselines.iter().any(|b| {
            b.antenna1.0 as usize >= catalogue.antennas.len()
                || b.antenna2.0 as usize >= catalogue.antennas.len()
        })
    {
        return Err(request_error(
            "the grid names an identity the catalogue lacks",
        ));
    }
    if crate::observation_io::distinct(&grid.field_per_time) != entry.fields
        || crate::observation_io::distinct(&grid.scan_per_time) != entry.scans
    {
        return Err(request_error(
            "the grid's fields and scans are not the partition's",
        ));
    }
    if entry
        .channel_widths_hz
        .iter()
        .any(|width| *width != entry.channel_widths_hz[0])
    {
        return Err(ObservationError::Unsupported {
            what: "writing channels of unequal width",
        });
    }
    let view = EpochView::new(
        grid.time_scale,
        grid.time_origin,
        ArrayView1::from(&grid.times_s[..]),
    );
    let unix = MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
    let unix_times: Vec<f64> = (0..times).map(|t| view.seconds_since(t, unix)).collect();
    Ok((
        Destination {
            partition,
            groups,
            entry,
            times: unix_times
                .iter()
                .enumerate()
                .map(|(index, time)| (time.to_bits(), index))
                .collect(),
            grid: grid.clone(),
        },
        unix_times,
    ))
}

impl Msv4Writer {
    /// Write the dataset's coordinates and sub-datasets into `store` and
    /// create its payload variables ([`Msv4ProcessingSetWriter::dataset`]).
    fn create(
        store: &Arc<FilesystemStore>,
        registry: &Arc<set::Registry>,
        catalogue: &Catalogue,
        partition: PartitionId,
        data_groups: &[&str],
        grid: &TimeBaselineGrid,
        options: &Msv4StorageOptions,
    ) -> Result<Self, ObservationError> {
        let (target, unix_times) = destination(catalogue, partition, data_groups, grid)?;
        let chunk_times = usize::try_from(options.chunks[0].max(1))
            .map_err(|_| request_error("a chunk of more times than a usize holds"))?;
        let mut writer = Self {
            dataset: options.dataset_name.clone(),
            store: store.clone(),
            registry: registry.clone(),
            payload: None,
            shape: [
                grid.times_s.len(),
                grid.baselines.len(),
                target.entry.frequencies_hz.len(),
                target.entry.correlations.len(),
            ],
            written: vec![false; grid.times_s.len()],
            chunk_times,
            target,
            options: super::codec_options(),
            scratch: payload::Scratch::default(),
            held: chunks::Held::default(),
            counters: Msv4WriteCounters::default(),
        };
        let root = format!("/{}", options.dataset_name);
        let fields_xds = format!("field_and_source_{}_xds", data_groups[0]);
        let observation = catalogue.observation(writer.target.entry.observation);
        writer.write_main_group(store, &root, observation, options, &fields_xds)?;
        writer.write_coordinates(store, &root, catalogue, &unix_times)?;
        write_antennas(
            store,
            &format!("{root}/antenna_xds"),
            &dataset_antennas(catalogue, grid),
            &writer.target.entry.receptors,
            &observation.telescope,
        )?;
        fields::write_fields(
            store,
            &format!("{root}/{fields_xds}"),
            catalogue,
            &writer.target.grid,
            &unix_times,
            writer.target.groups[0].uvw_frame.effective,
        )?;
        writer.payload = Some(writer.create_payload(store, &root, options)?);
        Ok(writer)
    }

    fn write_main_group(
        &self,
        store: &Arc<FilesystemStore>,
        root: &str,
        observation: &casa_observation::ObservationEntry,
        options: &Msv4StorageOptions,
        fields_xds: &str,
    ) -> Result<(), ObservationError> {
        let mut groups = serde_json::Map::new();
        for (index, group) in self.target.groups.iter().enumerate() {
            groups.insert(
                group.name.clone(),
                json!({
                    "correlated_data": visibility_variable(index, &group.name),
                    "flag": "FLAG",
                "weight": "WEIGHT",
                "uvw": "UVW",
                "field_and_source": fields_xds,
                "description": "Data group written by the casa-rs MSv4 trial writer",
                "date": options.creation_date,
                }),
            );
        }
        let mut info = json!({
            "observer": observation.observers,
            "release_date": options.creation_date,
            "project_UID": observation.project,
        });
        for (key, uid) in [
            ("execution_block_UID", &observation.execution_block),
            ("session_reference_UID", &observation.session_reference),
            ("scheduling_block_UID", &observation.scheduling_block),
        ] {
            if let Some(uid) = uid {
                info[key] = uid.clone().into();
            }
        }
        create_group(
            store,
            root,
            json!({
                "schema_version": SCHEMA_VERSION,
                "creator": {"software_name": options.creator, "version": env!("CARGO_PKG_VERSION")},
                "creation_date": options.creation_date,
                "type": "visibility",
                "data_groups": groups,
                "observation_info": info,
                "processor_info": {"type": "", "sub_type": ""},
            }),
        )
    }

    fn write_coordinates(
        &self,
        store: &Arc<FilesystemStore>,
        root: &str,
        catalogue: &Catalogue,
        unix_times: &[f64],
    ) -> Result<(), ObservationError> {
        let grid = &self.target.grid;
        let entry = &self.target.entry;
        let mut time = super::metadata::time_attributes(grid.time_scale)?;
        time.insert("integration_time".into(), quantity(grid.interval_s, "s"));
        coordinate(
            store,
            &format!("{root}/time"),
            json!("float64"),
            json!("NaN"),
            "time",
            unix_times,
            Value::Object(time),
        )?;
        let ids: Vec<i64> = (0..grid.baselines.len() as i64).collect();
        coordinate(
            store,
            &format!("{root}/baseline_id"),
            json!("int64"),
            json!(0),
            "baseline_id",
            &ids,
            json!({}),
        )?;
        let baselines = grid.baselines.len() as u64;
        for (which, first) in [("1", true), ("2", false)] {
            let names: Vec<String> = grid
                .baselines
                .iter()
                .map(|baseline| {
                    let antenna = if first {
                        baseline.antenna1
                    } else {
                        baseline.antenna2
                    };
                    catalogue.antennas[antenna.0 as usize].name.clone()
                })
                .collect();
            strings(
                store,
                &format!("{root}/baseline_antenna{which}_name"),
                &[baselines],
                &["baseline_id"],
                &names,
            )?;
        }
        let observer = super::metadata::frequency_observer(entry.frequency_frame);
        let reference = entry.frequencies_hz[entry.frequencies_hz.len() / 2];
        coordinate(
            store,
            &format!("{root}/frequency"),
            json!("float64"),
            json!("NaN"),
            "frequency",
            &entry.frequencies_hz,
            json!({
                "type": "spectral_coord",
                "units": "Hz",
                "observer": observer,
                "spectral_window_name": entry.spectral_window,
                "spectral_window_intents": ["UNSPECIFIED"],
                "reference_frequency": {"data": reference, "dims": [], "attrs": {"units": "Hz", "observer": observer, "type": "spectral_coord"}},
                "channel_width": quantity(entry.channel_widths_hz[0], "Hz"),
            }),
        )?;
        let labels: Vec<String> = entry
            .correlations
            .iter()
            .map(|c| c.label().to_string())
            .collect();
        strings(
            store,
            &format!("{root}/polarization"),
            &[labels.len() as u64],
            &["polarization"],
            &labels,
        )?;
        strings(
            store,
            &format!("{root}/uvw_label"),
            &[3],
            &["uvw_label"],
            &["u", "v", "w"].map(String::from),
        )?;
        let times = grid.times_s.len() as u64;
        let fields: Vec<String> = grid
            .field_per_time
            .iter()
            .map(|field| catalogue.fields[field.0 as usize].name.clone())
            .collect();
        strings(
            store,
            &format!("{root}/field_name"),
            &[times],
            &["time"],
            &fields,
        )?;
        let scans: Vec<String> = grid
            .scan_per_time
            .iter()
            .map(|scan| catalogue.scans[scan.0 as usize].name.clone())
            .collect();
        let mut scan = strings(
            store,
            &format!("{root}/scan_name"),
            &[times],
            &["time"],
            &scans,
        )?;
        // The scan intents are the scan coordinate's attribute: the
        // partition's, then any further intent of a scan written, in order
        // of first use.
        let mut intents = entry.intents.clone();
        for ScanId(id) in &grid.scan_per_time {
            for intent in &catalogue.scans[*id as usize].intents {
                if !intents.contains(intent) {
                    intents.push(intent.clone());
                }
            }
        }
        scan.attributes_mut()
            .insert("scan_intents".into(), json!(intents));
        scan.store_metadata_opt(&metadata_options())
            .map_err(storage)
    }

    fn create_payload(
        &self,
        store: &Arc<FilesystemStore>,
        root: &str,
        options: &Msv4StorageOptions,
    ) -> Result<Payload, ObservationError> {
        let shape = self.shape.map(|n| n as u64);
        let dims = ["time", "baseline_id", "frequency", "polarization"];
        let chunks = options.chunks;
        // An undeclared unit is written as XRADIO's converter spells it, so
        // that no reader takes the data for calibrated janskys.
        let units = |group: &DataGroupEntry| match &group.units {
            VisibilityUnits::Jansky => "Jy".to_string(),
            VisibilityUnits::Declared(units) => units.clone(),
            VisibilityUnits::Undeclared => "unkown".to_string(),
        };
        let cube = |data_type: &str, size: usize, fill: Value, attributes: Value| ArraySpec {
            shape: &shape,
            chunks: &chunks,
            data_type: json!(data_type),
            codecs: codecs(options.codec, size),
            fill,
            dimensions: &dims,
            attributes,
        };
        let coordinates = json!(PAYLOAD_COORDINATES);
        // Fill values are the padding convention: a cell never delivered
        // reads back flagged, its values and weight NaN as XRADIO pads
        // them, so the non-finite weight marks it as padding.
        let values = self
            .target
            .groups
            .iter()
            .enumerate()
            .map(|(index, group)| {
                create_array(
                    store,
                    &format!("{root}/{}", visibility_variable(index, &group.name)),
                    cube(
                        "complex64",
                        8,
                        json!(["NaN", "NaN"]),
                        json!({"type": "quanta", "units": units(group), "coordinates": coordinates}),
                    ),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let flags = create_array(
            store,
            &format!("{root}/FLAG"),
            cube("bool", 1, json!(true), json!({"coordinates": coordinates})),
        )?;
        let weights = create_array(
            store,
            &format!("{root}/WEIGHT"),
            cube(
                "float32",
                4,
                json!("NaN"),
                json!({"coordinates": coordinates}),
            ),
        )?;
        let triple = [shape[0], shape[1], 3];
        let uvw = create_array(
            store,
            &format!("{root}/UVW"),
            ArraySpec {
                shape: &triple,
                chunks: &[chunks[0], chunks[1], 3],
                data_type: json!("float64"),
                codecs: codecs(options.codec, 8),
                fill: json!("NaN"),
                dimensions: &["time", "baseline_id", "uvw_label"],
                attributes: json!({
                    "type": "uvw", "units": "m",
                    "frame": super::metadata::direction_name(self.target.groups[0].uvw_frame.effective)?,
                    "coordinates": coordinates,
                }),
            },
        )?;
        let plane = [shape[0], shape[1]];
        let time_baseline = |name: &str, attributes: Value| {
            create_array(
                store,
                &format!("{root}/{name}"),
                ArraySpec {
                    shape: &plane,
                    chunks: &[chunks[0], chunks[1]],
                    data_type: json!("float64"),
                    codecs: codecs(options.codec, 8),
                    fill: json!("NaN"),
                    dimensions: &["time", "baseline_id"],
                    attributes,
                },
            )
        };
        let mut centroid = super::metadata::time_attributes(self.target.grid.time_scale)?;
        centroid.insert("coordinates".into(), coordinates.clone());
        Ok(Payload {
            values,
            flags,
            weights,
            uvw,
            centroid: time_baseline("TIME_CENTROID", Value::Object(centroid))?,
            exposure: time_baseline(
                "EFFECTIVE_INTEGRATION_TIME",
                json!({"type": "quantity", "units": "s", "coordinates": coordinates}),
            )?,
        })
    }

    /// Write the dataset's `pointing_xds`: `series`, the partition's
    /// pointing ([`casa_observation::Observation::pointing`], times in the
    /// partition's time system), for the antennas the dataset's baselines
    /// name. Nothing is written when none has a sample.
    ///
    /// # Errors
    ///
    /// [`ObservationError::Unsupported`] for a frame MSv4 does not name, or
    /// two samples of an antenna at one time; any error writing the store.
    pub fn write_pointing(
        &self,
        catalogue: &Catalogue,
        series: &casa_observation::PointingSeries,
    ) -> Result<(), ObservationError> {
        let grid = &self.target.grid;
        let antennas = dataset_antennas(catalogue, grid);
        pointing::write_pointing(
            &self.store,
            &format!("/{}/pointing_xds", self.dataset),
            &antennas,
            series,
            grid.time_scale,
            grid.time_origin,
        )
    }

    /// What the writer has done.
    #[must_use]
    pub fn counters(&self) -> Msv4WriteCounters {
        self.counters
    }

    /// Times per chunk ([`Msv4StorageOptions::chunks`]). Batches in time
    /// order write each chunk once whatever their size: the times of a
    /// chunk a batch covers only partly are held until the chunk is
    /// complete ([`Self::buffer_bytes`]). A producer whose batches hold a
    /// multiple of this many times, from a chunk boundary, makes the writer
    /// hold nothing and copy nothing more.
    #[must_use]
    pub fn chunk_times(&self) -> usize {
        self.chunk_times
    }
}

/// The antennas `grid`'s baselines name, in catalogue order: a dataset's
/// `antenna_xds` holds these alone, as XRADIO's converter writes the
/// antennas of a partition's baselines. The grid was checked against the
/// catalogue.
fn dataset_antennas<'a>(
    catalogue: &'a Catalogue,
    grid: &TimeBaselineGrid,
) -> Vec<(AntennaId, &'a AntennaEntry)> {
    let mut named = vec![false; catalogue.antennas.len()];
    for baseline in &grid.baselines {
        named[baseline.antenna1.0 as usize] = true;
        named[baseline.antenna2.0 as usize] = true;
    }
    catalogue
        .antennas
        .iter()
        .zip(named)
        .enumerate()
        .filter(|(_, (_, named))| *named)
        .map(|(id, (antenna, _))| (AntennaId(id as u32), antenna))
        .collect()
}

/// `antennas` as an `antenna_xds`, with their `receptors` in the
/// dataset's partition (none for an antenna the partition states none
/// for).
fn write_antennas(
    store: &Arc<FilesystemStore>,
    path: &str,
    antennas: &[(AntennaId, &AntennaEntry)],
    receptors: &BTreeMap<AntennaId, Receptors>,
    telescope: &str,
) -> Result<(), ObservationError> {
    let none = Receptors {
        types: Vec::new(),
        angles: Vec::new(),
    };
    let own: Vec<&Receptors> = antennas
        .iter()
        .map(|(id, _)| receptors.get(id).unwrap_or(&none))
        .collect();
    let antennas: Vec<&AntennaEntry> = antennas.iter().map(|(_, antenna)| *antenna).collect();
    create_group(
        store,
        path,
        json!({
            "type": "antenna",
            "overall_telescope_name": telescope,
            "relocatable_antennas": super::STATION_NAMED.contains(&telescope),
        }),
    )?;
    let count = antennas.len() as u64;
    let receptors = own.first().map_or(0, |own| own.types.len());
    if own
        .iter()
        .any(|own| own.types.len() != receptors || own.angles.len() != receptors)
    {
        return Err(ObservationError::Unsupported {
            what: "antennas with different receptor counts",
        });
    }
    if antennas
        .iter()
        .any(|antenna| antenna.position.refer() != PositionRef::ITRF)
    {
        return Err(ObservationError::Unsupported {
            what: "writing antenna positions in other than ITRF",
        });
    }
    let column = |f: fn(&AntennaEntry) -> String| {
        antennas
            .iter()
            .map(|antenna| f(antenna))
            .collect::<Vec<_>>()
    };
    for (name, values) in [
        ("antenna_name", column(|a| a.name.clone())),
        ("station_name", column(|a| a.station.clone())),
        ("mount", column(|a| a.mount.clone())),
        (
            "telescope_name",
            vec![telescope.to_string(); antennas.len()],
        ),
    ] {
        strings(
            store,
            &format!("{path}/{name}"),
            &[count],
            &["antenna_name"],
            &values,
        )?;
    }
    // Without receptors (FEED states none for the window), XRADIO writes no
    // receptor variables.
    if receptors > 0 {
        let labels: Vec<String> = (0..receptors).map(|r| format!("pol_{r}")).collect();
        strings(
            store,
            &format!("{path}/receptor_label"),
            &[receptors as u64],
            &["receptor_label"],
            &labels,
        )?;
        let types: Vec<String> = own
            .iter()
            .flat_map(|own| own.types.iter().cloned())
            .collect();
        strings(
            store,
            &format!("{path}/polarization_type"),
            &[count, receptors as u64],
            &["antenna_name", "receptor_label"],
            &types,
        )?;
    }
    strings(
        store,
        &format!("{path}/cartesian_pos_label"),
        &[3],
        &["cartesian_pos_label"],
        &["x", "y", "z"].map(String::from),
    )?;
    let small = |name: &str, shape: &[u64], dims: &[&str], values: &[f64], attributes: Value| {
        let array = create_array(
            store,
            &format!("{path}/{name}"),
            ArraySpec {
                shape,
                chunks: shape,
                data_type: json!("float64"),
                codecs: codecs(Msv4Codec::Zstd, 8),
                fill: json!("NaN"),
                dimensions: dims,
                attributes,
            },
        )?;
        array
            .store_array_subset(&ArraySubset::new_with_shape(shape.to_vec()), values)
            .map_err(storage)
    };
    let positions: Vec<f64> = antennas.iter().flat_map(|a| a.position.values()).collect();
    small(
        "ANTENNA_POSITION",
        &[count, 3],
        &["antenna_name", "cartesian_pos_label"],
        &positions,
        json!({
            "type": "location", "units": "m", "frame": "ITRS", "coordinate_system": "geocentric",
            "origin_object_name": "earth", "coordinates": "mount station_name telescope_name",
        }),
    )?;
    let diameters = antennas
        .iter()
        .map(|a| value_in(&a.dish_diameter, "m"))
        .collect::<Result<Vec<_>, _>>()?;
    small(
        "ANTENNA_DISH_DIAMETER",
        &[count],
        &["antenna_name"],
        &diameters,
        json!({"type": "quantity", "units": "m", "coordinates": "mount station_name telescope_name"}),
    )?;
    if receptors == 0 {
        return Ok(());
    }
    let mut angles = Vec::new();
    for own in &own {
        for angle in &own.angles {
            angles.push(value_in(angle, "rad")?);
        }
    }
    small(
        "ANTENNA_RECEPTOR_ANGLE",
        &[count, receptors as u64],
        &["antenna_name", "receptor_label"],
        &angles,
        json!({
            "type": "quantity", "units": "rad", "coordinates": "mount polarization_type station_name telescope_name",
        }),
    )
}

impl std::fmt::Debug for Msv4Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Msv4Writer")
            .field("dataset", &self.dataset)
            .field("shape", &self.shape)
            .finish_non_exhaustive()
    }
}
