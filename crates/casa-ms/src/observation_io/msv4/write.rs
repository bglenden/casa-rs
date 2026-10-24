// SPDX-License-Identifier: LGPL-3.0-or-later
//! A narrow MSv4 writer: one known-shape `VisibilityXds` dataset in a new
//! local processing set, filled from batches and published whole.
//!
//! The trial profile writes what the reader admits (module docs): the main
//! dataset's coordinates and its `base` data group (`VISIBILITY`, `FLAG`,
//! `WEIGHT`, `UVW`), an `antenna_xds` and a `field_and_source_base_xds`. It
//! writes no pointing, system-calibration, weather or phase-calibration
//! datasets, and none of the optional time and frequency auxiliaries
//! (`TIME_CENTROID`, `EFFECTIVE_INTEGRATION_TIME`, `EFFECTIVE_CHANNEL_WIDTH`),
//! because the trial batch does not carry them. Every time must be written
//! exactly once, by whole times; the store is built in a sibling staging
//! directory and renamed into place only when [`Msv4Writer::finish`] has
//! checked that.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_observation::{
    AntennaEntry, Correlation, FieldEntry, FieldId, ObservationError, OrderedBaseline,
    SampleTopology, VisibilityBatch, VisibilityUnits, WeightView,
};
use casa_types::measures::EpochRef;
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use num_complex::Complex32;
use serde_json::{Value, json};
use zarrs::array::{
    Array, ArrayMetadata, ArrayMetadataOptions, ArraySubset, CodecOptions, Element,
};
use zarrs::filesystem::FilesystemStore;
use zarrs::group::{Group, GroupMetadata};
use zarrs::metadata::v3::{ArrayMetadataV3, GroupMetadataV3};

use super::metadata::{SCHEMA_VERSION, UNIX_EPOCH_MJD, invalid, storage};

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

/// One dataset to create, fixed before any payload is written: the
/// catalogue facts, the axes and the storage layout. Nothing here is read
/// from an input store; a producer fills it from its own catalogue.
#[derive(Clone, Debug)]
pub struct Msv4DatasetSpec {
    /// The dataset's name in the processing set.
    pub name: String,
    /// Software recorded as the creator.
    pub creator: String,
    /// Creation time, ISO 8601.
    pub creation_date: String,
    /// Telescope name.
    pub telescope: String,
    /// Observers.
    pub observers: Vec<String>,
    /// Project code.
    pub project: String,
    /// Antennas, in `antenna_xds` order; baselines index them.
    pub antennas: Vec<AntennaEntry>,
    /// Fields, in `field_and_source` order.
    pub fields: Vec<FieldEntry>,
    /// The baselines, in `baseline_id` order.
    pub baselines: Vec<OrderedBaseline>,
    /// Times in Unix seconds, strictly ascending.
    pub times_unix_s: Vec<f64>,
    /// Their time scale.
    pub time_scale: EpochRef,
    /// Nominal integration time in seconds.
    pub integration_time_s: f64,
    /// Field of each time.
    pub field_per_time: Vec<FieldId>,
    /// Scan name of each time.
    pub scan_per_time: Vec<String>,
    /// The scans' intents.
    pub scan_intents: Vec<String>,
    /// The spectral window's name.
    pub spectral_window: String,
    /// Channel centres in hertz.
    pub frequencies_hz: Vec<f64>,
    /// Channel width in hertz.
    pub channel_width_hz: f64,
    /// Spectral frame.
    pub frequency_frame: FrequencyRef,
    /// Correlation products in stored order.
    pub correlations: Vec<Correlation>,
    /// The `base` data group's visibility units.
    pub units: VisibilityUnits,
    /// The UVW frame.
    pub uvw_frame: DirectionRef,
    /// Chunk shape of the payload variables, `[time, baseline, frequency,
    /// polarization]`.
    pub chunks: [u64; 4],
    /// Bytes codec of the payload variables.
    pub codec: Msv4Codec,
}

/// What a writer did, for the evidence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Msv4WriteCounters {
    /// Region writes issued, per variable.
    pub region_writes: usize,
    /// Payload elements encoded.
    pub elements: usize,
    /// Elements copied to reorder or broadcast a batch into the stored
    /// layout before encoding.
    pub packed_elements: usize,
}

/// A dataset being created.
pub struct Msv4Writer {
    staging: PathBuf,
    destination: PathBuf,
    values: Array<FilesystemStore>,
    flags: Array<FilesystemStore>,
    weights: Array<FilesystemStore>,
    uvw: Array<FilesystemStore>,
    shape: [usize; 4],
    times: HashMap<u64, usize>,
    baselines: HashMap<OrderedBaseline, usize>,
    written: Vec<bool>,
    options: CodecOptions,
    packed: Packed,
    counters: Msv4WriteCounters,
}

/// Reusable dense staging for batches that need reordering.
#[derive(Default)]
struct Packed {
    values: Vec<Complex32>,
    flags: Vec<bool>,
    weights: Vec<f32>,
    uvw: Vec<f64>,
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
) -> Result<(), ObservationError> {
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
        .map_err(storage)
}

fn quantity(value: f64, units: &str) -> Value {
    json!({"data": value, "dims": [], "attrs": {"units": units, "type": "quantity"}})
}

const PAYLOAD_COORDINATES: &str =
    "baseline_antenna1_name baseline_antenna2_name field_name scan_name";

impl Msv4Writer {
    /// Create `spec`'s dataset in a new processing set at `processing_set`,
    /// writing every coordinate and sub-dataset now and leaving the payload
    /// to [`Self::write_batch`]. Nothing appears at `processing_set` until
    /// [`Self::finish`].
    ///
    /// # Errors
    ///
    /// When `processing_set` exists, the spec is inconsistent, or a frame,
    /// scale or unit is outside the profile.
    pub fn create(processing_set: &Path, spec: &Msv4DatasetSpec) -> Result<Self, ObservationError> {
        if processing_set.exists() {
            return Err(ObservationError::InvalidRequest {
                reason: format!("{} exists", processing_set.display()),
            });
        }
        let check = |ok: bool, reason: &str| {
            if ok {
                Ok(())
            } else {
                Err(ObservationError::InvalidRequest {
                    reason: reason.to_string(),
                })
            }
        };
        let times = spec.times_unix_s.len();
        check(
            spec.field_per_time.len() == times && spec.scan_per_time.len() == times,
            "one field and scan per time",
        )?;
        check(
            spec.times_unix_s.windows(2).all(|pair| pair[0] < pair[1]),
            "times strictly ascending",
        )?;
        let staging = processing_set.with_extension(format!("staging-{}", std::process::id()));
        std::fs::create_dir_all(&staging).map_err(storage)?;
        let store = Arc::new(FilesystemStore::new(&staging).map_err(storage)?);
        create_group(&store, "/", json!({"type": "processing_set"}))?;
        let root = format!("/{}", spec.name);
        Self::write_main_group(&store, &root, spec)?;
        Self::write_coordinates(&store, &root, spec)?;
        Self::write_antennas(&store, &format!("{root}/antenna_xds"), spec)?;
        Self::write_fields(&store, &format!("{root}/field_and_source_base_xds"), spec)?;
        let cube = [
            times,
            spec.baselines.len(),
            spec.frequencies_hz.len(),
            spec.correlations.len(),
        ];
        let shape = cube.map(|n| n as u64);
        let dims = ["time", "baseline_id", "frequency", "polarization"];
        let chunks = spec.chunks;
        // An undeclared unit is written as XRADIO's converter spells it, so
        // that no reader takes the data for calibrated janskys.
        let units = match &spec.units {
            VisibilityUnits::Jansky => "Jy".to_string(),
            VisibilityUnits::Declared(units) => units.clone(),
            VisibilityUnits::Undeclared => "unkown".to_string(),
        };
        let payload = |data_type: &str, size: usize, fill: Value, attributes: Value| ArraySpec {
            shape: &shape,
            chunks: &chunks,
            data_type: json!(data_type),
            codecs: codecs(spec.codec, size),
            fill,
            dimensions: &dims,
            attributes,
        };
        let values = create_array(
            &store,
            &format!("{root}/VISIBILITY"),
            payload(
                "complex64",
                8,
                json!([0.0, 0.0]),
                json!({"type": "quanta", "units": units, "coordinates": PAYLOAD_COORDINATES}),
            ),
        )?;
        let flags = create_array(
            &store,
            &format!("{root}/FLAG"),
            payload(
                "bool",
                1,
                json!(false),
                json!({"coordinates": PAYLOAD_COORDINATES}),
            ),
        )?;
        let weights = create_array(
            &store,
            &format!("{root}/WEIGHT"),
            payload(
                "float32",
                4,
                json!("NaN"),
                json!({"coordinates": PAYLOAD_COORDINATES}),
            ),
        )?;
        let uvw_shape = [shape[0], shape[1], 3];
        let uvw = create_array(
            &store,
            &format!("{root}/UVW"),
            ArraySpec {
                shape: &uvw_shape,
                chunks: &[chunks[0], chunks[1], 3],
                data_type: json!("float64"),
                codecs: codecs(spec.codec, 8),
                fill: json!("NaN"),
                dimensions: &["time", "baseline_id", "uvw_label"],
                attributes: json!({"type": "uvw", "units": "m", "frame": super::metadata::direction_name(spec.uvw_frame)?, "coordinates": PAYLOAD_COORDINATES}),
            },
        )?;
        Ok(Self {
            staging,
            destination: processing_set.to_path_buf(),
            values,
            flags,
            weights,
            uvw,
            shape: cube,
            times: spec
                .times_unix_s
                .iter()
                .enumerate()
                .map(|(index, time)| (time.to_bits(), index))
                .collect(),
            baselines: spec
                .baselines
                .iter()
                .enumerate()
                .map(|(index, baseline)| (*baseline, index))
                .collect(),
            written: vec![false; times],
            options: CodecOptions::default().with_concurrent_target(1),
            packed: Packed::default(),
            counters: Msv4WriteCounters::default(),
        })
    }

    fn write_main_group(
        store: &Arc<FilesystemStore>,
        root: &str,
        spec: &Msv4DatasetSpec,
    ) -> Result<(), ObservationError> {
        create_group(
            store,
            root,
            json!({
                "schema_version": SCHEMA_VERSION,
                "creator": {"software_name": spec.creator, "version": env!("CARGO_PKG_VERSION")},
                "creation_date": spec.creation_date,
                "type": "visibility",
                "data_groups": {"base": {
                    "correlated_data": "VISIBILITY",
                    "flag": "FLAG",
                    "weight": "WEIGHT",
                    "uvw": "UVW",
                    "field_and_source": "field_and_source_base_xds",
                    "description": "Data group written by the casa-rs MSv4 trial writer",
                    "date": spec.creation_date,
                }},
                "observation_info": {
                    "observer": spec.observers,
                    "release_date": spec.creation_date,
                    "project_UID": spec.project,
                },
                "processor_info": {"type": "", "sub_type": ""},
            }),
        )
    }

    fn write_coordinates(
        store: &Arc<FilesystemStore>,
        root: &str,
        spec: &Msv4DatasetSpec,
    ) -> Result<(), ObservationError> {
        let mut time = super::metadata::time_attributes(spec.time_scale)?;
        time.insert(
            "integration_time".into(),
            quantity(spec.integration_time_s, "s"),
        );
        coordinate(
            store,
            &format!("{root}/time"),
            json!("float64"),
            json!("NaN"),
            "time",
            &spec.times_unix_s,
            Value::Object(time),
        )?;
        let ids: Vec<i64> = (0..spec.baselines.len() as i64).collect();
        coordinate(
            store,
            &format!("{root}/baseline_id"),
            json!("int64"),
            json!(0),
            "baseline_id",
            &ids,
            json!({}),
        )?;
        let baselines = spec.baselines.len() as u64;
        for (which, pick) in [("1", 0), ("2", 1)] {
            let names: Vec<String> = spec
                .baselines
                .iter()
                .map(|baseline| {
                    let antenna = if pick == 0 {
                        baseline.antenna1
                    } else {
                        baseline.antenna2
                    };
                    spec.antennas[antenna.0 as usize].name.clone()
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
        let observer = super::metadata::frequency_observer(spec.frequency_frame);
        let reference = spec.frequencies_hz[spec.frequencies_hz.len() / 2];
        coordinate(
            store,
            &format!("{root}/frequency"),
            json!("float64"),
            json!("NaN"),
            "frequency",
            &spec.frequencies_hz,
            json!({
                "type": "spectral_coord",
                "units": "Hz",
                "observer": observer,
                "spectral_window_name": spec.spectral_window,
                "spectral_window_intents": ["UNSPECIFIED"],
                "reference_frequency": {"data": reference, "dims": [], "attrs": {"units": "Hz", "observer": observer, "type": "spectral_coord"}},
                "channel_width": quantity(spec.channel_width_hz, "Hz"),
            }),
        )?;
        let labels: Vec<String> = spec
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
        let times = spec.times_unix_s.len() as u64;
        let fields: Vec<String> = spec
            .field_per_time
            .iter()
            .map(|field| spec.fields[field.0 as usize].name.clone())
            .collect();
        strings(
            store,
            &format!("{root}/field_name"),
            &[times],
            &["time"],
            &fields,
        )?;
        strings(
            store,
            &format!("{root}/scan_name"),
            &[times],
            &["time"],
            &spec.scan_per_time,
        )?;
        // The scan intents are the scan coordinate's attribute.
        let scan = Array::open(store.clone(), &format!("{root}/scan_name")).map_err(storage)?;
        let mut scan = scan;
        scan.attributes_mut()
            .insert("scan_intents".into(), json!(spec.scan_intents));
        scan.store_metadata_opt(&metadata_options())
            .map_err(storage)
    }

    fn write_antennas(
        store: &Arc<FilesystemStore>,
        path: &str,
        spec: &Msv4DatasetSpec,
    ) -> Result<(), ObservationError> {
        create_group(
            store,
            path,
            json!({"type": "antenna", "overall_telescope_name": spec.telescope, "relocatable_antennas": true}),
        )?;
        let antennas = &spec.antennas;
        let count = antennas.len() as u64;
        let receptors = antennas
            .first()
            .map_or(0, |antenna| antenna.receptors.len());
        if antennas.iter().any(|antenna| {
            antenna.receptors.len() != receptors || antenna.receptor_angles_rad.len() != receptors
        }) {
            return Err(ObservationError::Unsupported {
                what: "antennas with different receptor counts",
            });
        }
        let column = |f: fn(&AntennaEntry) -> String| antennas.iter().map(f).collect::<Vec<_>>();
        strings(
            store,
            &format!("{path}/antenna_name"),
            &[count],
            &["antenna_name"],
            &column(|a| a.name.clone()),
        )?;
        strings(
            store,
            &format!("{path}/station_name"),
            &[count],
            &["antenna_name"],
            &column(|a| a.station.clone()),
        )?;
        strings(
            store,
            &format!("{path}/mount"),
            &[count],
            &["antenna_name"],
            &column(|a| a.mount.clone()),
        )?;
        strings(
            store,
            &format!("{path}/telescope_name"),
            &[count],
            &["antenna_name"],
            &vec![spec.telescope.clone(); antennas.len()],
        )?;
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
        let small =
            |name: &str, shape: &[u64], dims: &[&str], values: &[f64], attributes: Value| {
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
        let positions: Vec<f64> = antennas.iter().flat_map(|a| a.position_m).collect();
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
        let diameters: Vec<f64> = antennas.iter().map(|a| a.dish_diameter_m).collect();
        small(
            "ANTENNA_DISH_DIAMETER",
            &[count],
            &["antenna_name"],
            &diameters,
            json!({
                "type": "quantity", "units": "m", "coordinates": "mount station_name telescope_name",
            }),
        )?;
        let angles: Vec<f64> = antennas
            .iter()
            .flat_map(|a| a.receptor_angles_rad.iter().copied())
            .collect();
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

    fn write_fields(
        store: &Arc<FilesystemStore>,
        path: &str,
        spec: &Msv4DatasetSpec,
    ) -> Result<(), ObservationError> {
        create_group(store, path, json!({"type": "field_and_source"}))?;
        let count = spec.fields.len() as u64;
        let frame = spec
            .fields
            .first()
            .map(|field| field.phase_centre.refer())
            .unwrap_or(DirectionRef::J2000);
        if spec
            .fields
            .iter()
            .any(|field| field.phase_centre.refer() != frame)
        {
            return Err(ObservationError::Unsupported {
                what: "fields with different phase-centre frames",
            });
        }
        strings(
            store,
            &format!("{path}/field_name"),
            &[count],
            &["field_name"],
            &spec
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<_>>(),
        )?;
        // XRADIO names an absent source "Unknown".
        strings(
            store,
            &format!("{path}/source_name"),
            &[count],
            &["field_name"],
            &spec
                .fields
                .iter()
                .map(|f| {
                    f.source_name
                        .clone()
                        .unwrap_or_else(|| "Unknown".to_string())
                })
                .collect::<Vec<_>>(),
        )?;
        strings(
            store,
            &format!("{path}/sky_dir_label"),
            &[2],
            &["sky_dir_label"],
            &["ra", "dec"].map(String::from),
        )?;
        let centres: Vec<f64> = spec
            .fields
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

    /// Write `batch`: every sample of whole times, each time's baselines
    /// complete, every channel. A factored batch in the dataset's baseline
    /// order is encoded straight from its views; a listed batch is first
    /// placed into dense time × baseline order, and a per-correlation weight
    /// is written for each channel (both counted as packed).
    ///
    /// # Errors
    ///
    /// When the batch's times or baselines are not the spec's, a time is
    /// written twice or incompletely, or the batch is a channel window.
    pub fn write_batch(&mut self, batch: &VisibilityBatch<'_>) -> Result<(), ObservationError> {
        let [_, baselines, channels, correlations] = self.shape;
        if batch.first_channel != 0
            || batch.channel_count() != channels
            || batch.correlations.len() != correlations
        {
            return Err(ObservationError::Unsupported {
                what: "writing a channel or correlation window",
            });
        }
        let time = batch.topology.time();
        let origin = casa_types::measures::MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
        let time_index = |sample_time: usize| -> Result<usize, ObservationError> {
            let unix = time.seconds_since(sample_time, origin);
            self.times
                .get(&unix.to_bits())
                .copied()
                .ok_or_else(|| invalid(format!("time {unix} is not in the dataset")))
        };
        let group = &batch.group;
        let (Some(values), Some(flags), Some(weights), Some(uvw)) =
            (group.values, group.flags, group.weights, group.uvw)
        else {
            return Err(ObservationError::InvalidRequest {
                reason: "a written batch carries every role".to_string(),
            });
        };
        let values = values.as_complex32().ok_or(ObservationError::Unsupported {
            what: "writing complex128",
        })?;
        let samples = batch.samples();
        let times = samples / baselines;
        let first_time = time_index(0)?;
        let rows = first_time..first_time + times;
        if rows.end > self.written.len() || self.written[rows.clone()].iter().any(|done| *done) {
            return Err(invalid("a time is written twice or beyond the dataset"));
        }
        let direct = match batch.topology {
            SampleTopology::TimeBaseline(topology) => {
                topology
                    .baselines
                    .iter()
                    .enumerate()
                    .all(|(index, baseline)| self.baselines.get(baseline) == Some(&index))
                    && (0..topology.time.len()).try_fold(true, |ok, t| {
                        Ok::<_, ObservationError>(ok && time_index(t)? == first_time + t)
                    })?
                    && matches!(weights, WeightView::Spectral(_))
                    && values.is_standard_layout()
                    && flags.is_standard_layout()
            }
            SampleTopology::Irregular(_) => false,
        };
        if !direct {
            self.pack(batch, first_time, times)?;
        }
        let cube = ArraySubset::new_with_ranges(&[
            rows.start as u64..rows.end as u64,
            0..baselines as u64,
            0..channels as u64,
            0..correlations as u64,
        ]);
        let triple = ArraySubset::new_with_ranges(&[
            rows.start as u64..rows.end as u64,
            0..baselines as u64,
            0..3,
        ]);
        let options = &self.options;
        let elements = samples * channels * correlations;
        if direct {
            let WeightView::Spectral(weights) = weights else {
                unreachable!("checked spectral")
            };
            self.values
                .store_array_subset_opt(&cube, values.as_slice().expect("standard layout"), options)
                .map_err(storage)?;
            self.flags
                .store_array_subset_opt(&cube, flags.as_slice().expect("standard layout"), options)
                .map_err(storage)?;
            self.weights
                .store_array_subset_opt(
                    &cube,
                    weights
                        .as_standard_layout()
                        .as_slice()
                        .expect("standard layout"),
                    options,
                )
                .map_err(storage)?;
            self.uvw
                .store_array_subset_opt(
                    &triple,
                    uvw.metres()
                        .as_standard_layout()
                        .as_slice()
                        .expect("standard layout"),
                    options,
                )
                .map_err(storage)?;
        } else {
            let packed = &self.packed;
            self.values
                .store_array_subset_opt(&cube, packed.values.as_slice(), options)
                .map_err(storage)?;
            self.flags
                .store_array_subset_opt(&cube, packed.flags.as_slice(), options)
                .map_err(storage)?;
            self.weights
                .store_array_subset_opt(&cube, packed.weights.as_slice(), options)
                .map_err(storage)?;
            self.uvw
                .store_array_subset_opt(&triple, packed.uvw.as_slice(), options)
                .map_err(storage)?;
        }
        self.counters.region_writes += 4;
        self.counters.elements += 3 * elements + 3 * samples;
        self.written[rows].iter_mut().for_each(|done| *done = true);
        Ok(())
    }

    /// Place `batch`'s samples into dense `[time, baseline]` order for
    /// `times` times from `first_time`.
    fn pack(
        &mut self,
        batch: &VisibilityBatch<'_>,
        first_time: usize,
        times: usize,
    ) -> Result<(), ObservationError> {
        let [_, baselines, channels, correlations] = self.shape;
        let cell = channels * correlations;
        let slots = times * baselines;
        let packed = &mut self.packed;
        packed.values.clear();
        packed
            .values
            .resize(slots * cell, Complex32::new(f32::NAN, f32::NAN));
        packed.flags.clear();
        packed.flags.resize(slots * cell, false);
        packed.weights.clear();
        packed.weights.resize(slots * cell, f32::NAN);
        packed.uvw.clear();
        packed.uvw.resize(slots * 3, f64::NAN);
        let mut filled = vec![false; slots];
        let group = &batch.group;
        let values = group
            .values
            .and_then(|v| v.as_complex32())
            .expect("checked");
        let flags = group.flags.expect("checked");
        let weights = group.weights.expect("checked");
        let uvw = group.uvw.expect("checked");
        let time = batch.topology.time();
        let origin = casa_types::measures::MjdHighPrec::new(UNIX_EPOCH_MJD, 0.0);
        for sample in 0..batch.samples() {
            let coordinate = batch.topology.sample(sample);
            let time_slot = match batch.topology {
                SampleTopology::Irregular(_) => sample,
                SampleTopology::TimeBaseline(topology) => sample / topology.baselines.len(),
            };
            let unix = time.seconds_since(time_slot, origin);
            let t = self
                .times
                .get(&unix.to_bits())
                .copied()
                .ok_or_else(|| invalid(format!("time {unix} is not in the dataset")))?;
            let b = *self
                .baselines
                .get(&coordinate.baseline)
                .ok_or_else(|| invalid("a baseline is not in the dataset"))?;
            if t < first_time || t >= first_time + times {
                return Err(invalid("a batch's times are not contiguous"));
            }
            let slot = (t - first_time) * baselines + b;
            if std::mem::replace(&mut filled[slot], true) {
                return Err(ObservationError::Unsupported {
                    what: "two acquisitions in one time/baseline slot",
                });
            }
            for channel in 0..channels {
                for correlation in 0..correlations {
                    let index = slot * cell + channel * correlations + correlation;
                    packed.values[index] = values[[sample, channel, correlation]];
                    packed.flags[index] = flags[[sample, channel, correlation]];
                    packed.weights[index] = weights.get(sample, channel, correlation);
                }
            }
            packed.uvw[slot * 3..slot * 3 + 3].copy_from_slice(&uvw.get(sample));
        }
        if filled.iter().any(|slot| !slot) {
            return Err(ObservationError::Unsupported {
                what: "writing an incomplete time: every baseline of a time is required",
            });
        }
        self.counters.packed_elements += 3 * slots * cell + 3 * slots;
        Ok(())
    }

    /// What the writer has done.
    #[must_use]
    pub fn counters(&self) -> Msv4WriteCounters {
        self.counters
    }

    /// Check every time was written and publish the processing set by
    /// renaming its staging directory into place. A writer dropped without
    /// finishing leaves only its staging directory.
    ///
    /// # Errors
    ///
    /// When a time was not written, or the rename fails.
    pub fn finish(self) -> Result<PathBuf, ObservationError> {
        if let Some(time) = self.written.iter().position(|done| !done) {
            return Err(invalid(format!("time {time} was never written")));
        }
        drop((self.values, self.flags, self.weights, self.uvw));
        std::fs::rename(&self.staging, &self.destination).map_err(storage)?;
        Ok(self.destination)
    }
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
