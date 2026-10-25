// SPDX-License-Identifier: LGPL-3.0-or-later
//! A narrow MSv4 writer: one known-shape `VisibilityXds` dataset in a new
//! local processing set, filled from batches and published whole.
//!
//! What the dataset *is* comes from the common vocabulary: the source's
//! [`Catalogue`] (antennas, fields, scans, the partition and its data
//! group) and a [`TimeBaselineGrid`] (baselines, times, the field and scan
//! of each time, the interval). How it is stored comes from
//! [`Msv4StorageOptions`]: dataset name, provenance, chunks and codec.
//!
//! The trial profile writes what the reader admits (module docs): the main
//! dataset's coordinates and one data group (`VISIBILITY`, `FLAG`,
//! `WEIGHT`, `UVW`), `TIME_CENTROID` and `EFFECTIVE_INTEGRATION_TIME`, an
//! `antenna_xds` and a `field_and_source_<group>_xds`. It writes no
//! pointing, system-calibration, weather or phase-calibration datasets and
//! no `EFFECTIVE_CHANNEL_WIDTH`. Every time must be written exactly once,
//! from batches whose coordinates and context match the declaration
//! ([`Msv4Writer::write_batch`]); the payload variables' fill values are
//! the padding convention (NaN, flag true, weight 0), so a cell never
//! delivered reads back as padding; the store is built in a staging
//! directory of its own and published only by [`Msv4Writer::finish`].

mod payload;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_observation::{
    AntennaEntry, Catalogue, DataGroupEntry, FieldId, ObservationError, PartitionEntry,
    PartitionId, ScanId, TimeBaselineGrid, VisibilityUnits,
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
}

/// What every batch written must agree with: the declaration, resolved
/// once at creation.
struct Destination {
    partition: PartitionId,
    group: DataGroupEntry,
    entry: PartitionEntry,
    grid: TimeBaselineGrid,
    /// The grid's times in Unix seconds, by bit pattern.
    times: HashMap<u64, usize>,
}

/// The dataset's payload variables.
struct Payload {
    values: Array<FilesystemStore>,
    flags: Array<FilesystemStore>,
    weights: Array<FilesystemStore>,
    uvw: Array<FilesystemStore>,
    centroid: Array<FilesystemStore>,
    exposure: Array<FilesystemStore>,
}

/// A dataset being created.
pub struct Msv4Writer {
    /// The staging directory, until published or removed.
    staging: Option<PathBuf>,
    destination: PathBuf,
    payload: Option<Payload>,
    target: Destination,
    shape: [usize; 4],
    written: Vec<bool>,
    options: CodecOptions,
    scratch: payload::Scratch,
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

/// Resolve and check the declaration: the partition, its data group, the
/// grid's identities against the catalogue, and the grid's times in Unix
/// seconds.
fn destination(
    catalogue: &Catalogue,
    partition: PartitionId,
    data_group: &str,
    grid: &TimeBaselineGrid,
) -> Result<(Destination, Vec<f64>), ObservationError> {
    let group = catalogue
        .data_group(partition, data_group)
        .ok_or_else(|| request_error(format!("no data group {data_group} in {partition:?}")))?
        .clone();
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
            group,
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
    /// Create the dataset that `catalogue`'s partition `partition`, data
    /// group `data_group` and `grid` describe, in a new processing set at
    /// `processing_set`, stored as `options` say. Every coordinate and
    /// sub-dataset is written now, into a staging directory of this
    /// writer's own; the payload follows from [`Self::write_batch`], and
    /// nothing appears at `processing_set` until [`Self::finish`].
    ///
    /// # Errors
    ///
    /// When `processing_set` exists, the declaration is inconsistent, or a
    /// frame, scale or unit is outside the profile.
    pub fn create(
        processing_set: &Path,
        catalogue: &Catalogue,
        partition: PartitionId,
        data_group: &str,
        grid: &TimeBaselineGrid,
        options: &Msv4StorageOptions,
    ) -> Result<Self, ObservationError> {
        if processing_set.exists() {
            return Err(request_error(format!(
                "{} exists",
                processing_set.display()
            )));
        }
        let (target, unix_times) = destination(catalogue, partition, data_group, grid)?;
        let staging = payload::create_staging(processing_set)?;
        let mut writer = Self {
            staging: Some(staging.clone()),
            destination: processing_set.to_path_buf(),
            payload: None,
            shape: [
                grid.times_s.len(),
                grid.baselines.len(),
                target.entry.frequencies_hz.len(),
                target.entry.correlations.len(),
            ],
            written: vec![false; grid.times_s.len()],
            target,
            options: CodecOptions::default().with_concurrent_target(1),
            scratch: payload::Scratch::default(),
            counters: Msv4WriteCounters::default(),
        };
        // From here a failure drops the writer, which removes its staging.
        let store = Arc::new(FilesystemStore::new(&staging).map_err(storage)?);
        create_group(&store, "/", json!({"type": "processing_set"}))?;
        let root = format!("/{}", options.dataset_name);
        let fields_xds = format!("field_and_source_{data_group}_xds");
        let observation = catalogue.observation(writer.target.entry.observation);
        writer.write_main_group(&store, &root, observation, options, &fields_xds)?;
        writer.write_coordinates(&store, &root, catalogue, &unix_times)?;
        write_antennas(
            &store,
            &format!("{root}/antenna_xds"),
            catalogue,
            &observation.telescope,
        )?;
        writer.write_fields(&store, &format!("{root}/{fields_xds}"), catalogue)?;
        writer.payload = Some(writer.create_payload(&store, &root, options)?);
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
        let group = &self.target.group;
        let mut groups = serde_json::Map::new();
        groups.insert(
            group.name.clone(),
            json!({
                "correlated_data": "VISIBILITY",
                "flag": "FLAG",
                "weight": "WEIGHT",
                "uvw": "UVW",
                "field_and_source": fields_xds,
                "description": "Data group written by the casa-rs MSv4 trial writer",
                "date": options.creation_date,
            }),
        );
        create_group(
            store,
            root,
            json!({
                "schema_version": SCHEMA_VERSION,
                "creator": {"software_name": options.creator, "version": env!("CARGO_PKG_VERSION")},
                "creation_date": options.creation_date,
                "type": "visibility",
                "data_groups": groups,
                "observation_info": {
                    "observer": observation.observers,
                    "release_date": options.creation_date,
                    "project_UID": observation.project,
                },
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
        let units = match &self.target.group.units {
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
        // reads back as a flagged NaN of weight zero.
        let values = create_array(
            store,
            &format!("{root}/VISIBILITY"),
            cube(
                "complex64",
                8,
                json!(["NaN", "NaN"]),
                json!({"type": "quanta", "units": units, "coordinates": coordinates}),
            ),
        )?;
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
                json!(0.0),
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
                    "frame": super::metadata::direction_name(self.target.group.uvw_frame.effective)?,
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

    fn write_fields(
        &self,
        store: &Arc<FilesystemStore>,
        path: &str,
        catalogue: &Catalogue,
    ) -> Result<(), ObservationError> {
        create_group(store, path, json!({"type": "field_and_source"}))?;
        // The fields the grid uses, in catalogue order.
        let mut used: Vec<FieldId> = self.target.grid.field_per_time.clone();
        used.sort();
        used.dedup();
        let fields: Vec<_> = used
            .iter()
            .map(|field| &catalogue.fields[field.0 as usize])
            .collect();
        let frame = self.target.group.uvw_frame.effective;
        if fields
            .iter()
            .any(|field| field.phase_centre.refer() != frame)
        {
            return Err(ObservationError::Unsupported {
                what: "writing phase centres in another frame than the data group's UVW",
            });
        }
        let count = fields.len() as u64;
        let column = |f: &dyn Fn(&casa_observation::FieldEntry) -> String| {
            fields.iter().map(|field| f(field)).collect::<Vec<_>>()
        };
        strings(
            store,
            &format!("{path}/field_name"),
            &[count],
            &["field_name"],
            &column(&|f| f.name.clone()),
        )?;
        // XRADIO names an absent source "Unknown".
        strings(
            store,
            &format!("{path}/source_name"),
            &[count],
            &["field_name"],
            &column(&|f| {
                f.source_name
                    .clone()
                    .unwrap_or_else(|| "Unknown".to_string())
            }),
        )?;
        strings(
            store,
            &format!("{path}/sky_dir_label"),
            &[2],
            &["sky_dir_label"],
            &["ra", "dec"].map(String::from),
        )?;
        let centres: Vec<f64> = fields
            .iter()
            .flat_map(|field| {
                let (longitude, latitude) = field.phase_centre.as_angles();
                [longitude, latitude]
            })
            .collect();
        let shape = [count, 2];
        let array = create_array(
            store,
            &format!("{path}/FIELD_PHASE_CENTER_DIRECTION"),
            ArraySpec {
                shape: &shape,
                chunks: &shape,
                data_type: json!("float64"),
                codecs: codecs(Msv4Codec::Zstd, 8),
                fill: json!("NaN"),
                dimensions: &["field_name", "sky_dir_label"],
                attributes: json!({"type": "sky_coord", "units": "rad", "frame": super::metadata::direction_name(frame)?, "coordinates": "source_name"}),
            },
        )?;
        array
            .store_array_subset(&ArraySubset::new_with_shape(shape.to_vec()), &centres)
            .map_err(storage)
    }

    /// What the writer has done.
    #[must_use]
    pub fn counters(&self) -> Msv4WriteCounters {
        self.counters
    }
}

/// The catalogue's antennas as an `antenna_xds`.
fn write_antennas(
    store: &Arc<FilesystemStore>,
    path: &str,
    catalogue: &Catalogue,
    telescope: &str,
) -> Result<(), ObservationError> {
    create_group(
        store,
        path,
        json!({"type": "antenna", "overall_telescope_name": telescope, "relocatable_antennas": true}),
    )?;
    let antennas = &catalogue.antennas;
    let count = antennas.len() as u64;
    let receptors = antennas
        .first()
        .map_or(0, |antenna| antenna.receptors.len());
    if antennas.iter().any(|antenna| {
        antenna.receptors.len() != receptors || antenna.receptor_angles.len() != receptors
    }) {
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
    let column = |f: fn(&AntennaEntry) -> String| antennas.iter().map(f).collect::<Vec<_>>();
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
    let labels: Vec<String> = (0..receptors).map(|r| format!("pol_{r}")).collect();
    strings(
        store,
        &format!("{path}/receptor_label"),
        &[receptors as u64],
        &["receptor_label"],
        &labels,
    )?;
    let types: Vec<String> = antennas
        .iter()
        .flat_map(|a| a.receptors.iter().cloned())
        .collect();
    strings(
        store,
        &format!("{path}/polarization_type"),
        &[count, receptors as u64],
        &["antenna_name", "receptor_label"],
        &types,
    )?;
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
    let mut angles = Vec::new();
    for antenna in antennas {
        for angle in &antenna.receptor_angles {
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
            .field("staging", &self.staging)
            .field("destination", &self.destination)
            .field("shape", &self.shape)
            .finish_non_exhaustive()
    }
}
