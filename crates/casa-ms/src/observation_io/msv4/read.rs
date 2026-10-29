// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opening an MSv4 processing set: each `VisibilityXds` dataset is a
//! partition, with its catalogue checked against the profile once.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_observation::{
    AccessRequest, AntennaEntry, AntennaId, BoundCursor, Catalogue, Correlation, DataGroupEntry,
    Estimate, FieldEntry, FieldId, FieldSelection, GroupKey, Grouping, Observation,
    ObservationEntry, ObservationError, ObservationId, OrderedBaseline, PartitionEntry,
    PartitionId, ProcessorEntry, ProcessorId, Receptors, ScanEntry, ScanId, TimeBaselineGrid,
    Traversal, UvwFrame, WindowEntry, WindowId,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::MFrequency;
use casa_types::measures::position::MPosition;
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::quanta::time::MvTime;
use zarrs::array::{Array, CodecOptions};
use zarrs::filesystem::FilesystemStore;
use zarrs::group::Group;

use super::cursor::Msv4Cursor;
use super::ephemeris;
use super::metadata::{
    DataGroupRecord, SCHEMA_VERSION, data_groups, direction_frame, frequency_frame, has_node,
    invalid, numbers, open_array, require_layout, require_units, storage, strings, text,
    time_reference, visibility_units,
};
use crate::observation_io::OpenOptions;
use crate::observation_io::msv2::{metres, radians};

/// The cube's dimensions and the UVW's.
pub(super) const CUBE: [&str; 4] = ["time", "baseline_id", "frequency", "polarization"];
pub(super) const UVW: [&str; 3] = ["time", "baseline_id", "uvw_label"];
pub(super) const TIME_BASELINE: [&str; 2] = ["time", "baseline_id"];

/// One data group: its payload variables and its own context, the field
/// of each time (from its `field_and_source` dataset) and its UVW frame.
pub(super) struct GroupArrays {
    pub(super) record: DataGroupRecord,
    pub(super) values: Array<FilesystemStore>,
    pub(super) flags: Array<FilesystemStore>,
    pub(super) weights: Array<FilesystemStore>,
    pub(super) uvw: Array<FilesystemStore>,
    /// Catalogue field at each time.
    pub(super) fields: Vec<FieldId>,
    pub(super) uvw_frame: UvwFrame,
}

/// One `VisibilityXds` dataset of the processing set: what its partition
/// entry does not already state.
pub(super) struct Dataset {
    /// The dataset's group in the processing set (`/name`).
    pub(super) path: String,
    /// `time` coordinate: seconds from the partition's origin.
    pub(super) times: Vec<f64>,
    /// `time.integration_time`, when stated.
    pub(super) interval_s: Option<f64>,
    /// `TIME_CENTROID`, when present.
    pub(super) centroid: Option<Array<FilesystemStore>>,
    /// Whether `TIME_CENTROID_EXTRA_PRECISION` is present: the profile
    /// carries one `f64` offset per centroid, so a request for centroids is
    /// refused rather than truncated to the primary component.
    pub(super) centroid_extra_precision: bool,
    /// `EFFECTIVE_INTEGRATION_TIME`, when present.
    pub(super) exposure: Option<Array<FilesystemStore>>,
    /// Catalogue scan at each time, when `scan_name` is present.
    pub(super) scans: Option<Vec<ScanId>>,
    pub(super) groups: Vec<GroupArrays>,
    /// The dataset's `antenna_xds` names, each with its catalogue id.
    pub(super) antennas: Vec<(String, AntennaId)>,
}

/// An opened MSv4 processing set (XRADIO `VisibilityXds` schema 4.0.1 on
/// local Zarr v3) and its catalogue.
///
/// Each visibility dataset is one partition, described by its content
/// (spectral window, correlations, the intents of its `scan_name`, its
/// `observation_info`). Antennas are the union of the datasets'
/// `antenna_xds` entries, one per name and station (one with two positions
/// is refused), in an order that keeps every dataset's: a dataset lists
/// only its baselines' antennas, in its MSv2's `ANTENNA` order when either
/// converter wrote it, so the order the datasets determine is that MSv2's.
/// Fields come from each data group's own `field_and_source` dataset and
/// are distinct unless name, phase centre and source all agree: two groups
/// with different phase centres keep their own fields and UVW frames, and a
/// traversal of either delivers its own. A `field_and_source_ephemeris`
/// dataset's fields follow its ephemeris, in either of XRADIO's layouts
/// (`msv4/ephemeris.rs`), and are one field across datasets when their
/// offsets agree and their samples agree where both hold them: the
/// field then follows the samples of both. An ephemeris that ends before
/// the TDB of a dataset's last time, as XRADIO's can, gains one sample
/// extrapolated to it ([`casa_observation::FieldEphemeris::extrapolated`]).
/// Opening checks every variable's data type and dimensions, and every
/// unit and frame, against the profile and refuses others rather than
/// reinterpreting or rescaling them.
pub struct Msv4Observation {
    /// The processing set's directory.
    pub(super) root: PathBuf,
    pub(super) store: Arc<FilesystemStore>,
    pub(super) catalogue: Arc<Catalogue>,
    pub(super) datasets: Vec<Dataset>,
    pub(super) options: CodecOptions,
    pub(super) open: OpenOptions,
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
        Self::open_with(path, &OpenOptions::default())
    }

    /// [`Self::open`] with `options`.
    ///
    /// # Errors
    ///
    /// As [`Self::open`].
    pub fn open_with(
        path: impl AsRef<Path>,
        options: &OpenOptions,
    ) -> Result<Self, ObservationError> {
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
            root: path.as_ref().to_path_buf(),
            store: store.clone(),
            catalogue: Arc::new(Catalogue::default()),
            datasets: Vec::new(),
            options: super::codec_options(),
            open: options.clone(),
        };
        let (antennas, per_dataset, receptors) = dataset_antennas(&store, &names)?;
        observation.catalogue_mut().antennas = antennas;
        for ((name, antennas), receptors) in names.iter().zip(&per_dataset).zip(receptors) {
            observation.add_dataset(
                &store,
                path.as_ref(),
                &format!("/{name}"),
                antennas,
                receptors,
            )?;
        }
        if observation.datasets.is_empty() {
            return Err(invalid("the processing set has no visibility datasets"));
        }
        let last_times: Vec<(Vec<FieldId>, f64)> = observation
            .datasets
            .iter()
            .zip(&observation.catalogue.partitions)
            .filter_map(|(dataset, entry)| {
                let origin = entry.time_origin.day() + entry.time_origin.frac();
                let last = dataset.times.last()?;
                Some((
                    entry.fields.clone(),
                    origin + last / ephemeris::SECONDS_PER_DAY,
                ))
            })
            .collect();
        ephemeris::reach_last_tdb(&mut observation.catalogue_mut().fields, &last_times)?;
        Ok(observation)
    }

    /// The catalogue.
    #[must_use]
    pub fn catalogue(&self) -> &Arc<Catalogue> {
        &self.catalogue
    }

    /// The processing set's directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// The bytes the opened processing set holds while it is open: the
    /// catalogue's ephemerides and, for each dataset, its times and scans
    /// and a kilobyte of array handles and metadata. What decoding a batch
    /// holds is the cursor's, counted in its estimate.
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        self.catalogue.ephemeris_bytes()
            + self
                .datasets
                .iter()
                .map(|dataset| {
                    dataset.times.len() * size_of::<f64>()
                        + dataset
                            .scans
                            .as_ref()
                            .map_or(0, |scans| scans.len() * size_of::<ScanId>())
                        + (dataset.groups.len() + 1) * 1024
                })
                .sum::<usize>()
    }

    /// The name of partition `id`'s dataset in the processing set.
    ///
    /// # Panics
    ///
    /// If `id` is not a partition of the observation.
    #[must_use]
    pub fn dataset_name(&self, id: PartitionId) -> &str {
        self.datasets[id.0 as usize].path.trim_start_matches('/')
    }

    /// The catalogue while the store is being opened, before any batch or
    /// cursor shares it.
    fn catalogue_mut(&mut self) -> &mut Catalogue {
        Arc::get_mut(&mut self.catalogue).expect("the catalogue is shared only after opening")
    }

    /// Add the dataset at `path`, whose `antenna_xds` names `antennas`
    /// (each with its catalogue id).
    fn add_dataset(
        &mut self,
        store: &Arc<FilesystemStore>,
        root: &Path,
        path: &str,
        antennas: &[(String, AntennaId)],
        receptors: BTreeMap<AntennaId, Receptors>,
    ) -> Result<(), ObservationError> {
        let group = visibility_group(store, path)?;
        let attributes = group.attributes();
        if exists(root, &format!("{path}/polarization_mixed")) {
            // The partition's correlations are one list for every baseline;
            // a per-baseline basis under `PP`, `PQ`, `QP`, `QQ` would be
            // lost (or misread as those labels' own types).
            return Err(ObservationError::Unsupported {
                what: "an MSv4 dataset whose polarization basis differs by baseline \
                       (polarization_mixed)",
            });
        }
        let baselines = baselines(store, path, antennas)?;
        let observation = self.observation_info(store, path, attributes)?;
        let (times, time_attributes) = numbers::<f64>(store, &format!("{path}/time"))?;
        let (time_scale, time_origin) = time_reference(&time_attributes, "time")?;
        let interval_s = time_attributes
            .get("integration_time")
            .map(|quantity| {
                let units = quantity
                    .get("attrs")
                    .and_then(|attrs| attrs.get("units"))
                    .and_then(serde_json::Value::as_str);
                match (
                    quantity.get("data").and_then(serde_json::Value::as_f64),
                    units,
                ) {
                    (Some(seconds), Some("s")) => Ok(seconds),
                    _ => Err(invalid("time.integration_time is not a quantity in s")),
                }
            })
            .transpose()?;
        let auxiliary = |name: &str, time_like: bool| -> Result<_, ObservationError> {
            let variable = format!("{path}/{name}");
            if !exists(root, &variable) {
                return Ok(None);
            }
            let array = open_array(store, &variable)?;
            require_layout(&array, &variable, "float64", &TIME_BASELINE)?;
            if array.shape() != [times.len() as u64, baselines.len() as u64] {
                return Err(invalid(format!("{variable} is not [time, baseline_id]")));
            }
            if time_like {
                if time_reference(array.attributes(), name)? != (time_scale, time_origin) {
                    return Err(ObservationError::Unsupported {
                        what: "an MSv4 TIME_CENTROID in another scale than time",
                    });
                }
            } else {
                require_units(array.attributes(), "s", name)?;
            }
            Ok(Some(array))
        };
        let centroid = auxiliary("TIME_CENTROID", true)?;
        let centroid_extra_precision =
            exists(root, &format!("{path}/TIME_CENTROID_EXTRA_PRECISION"));
        let exposure = auxiliary("EFFECTIVE_INTEGRATION_TIME", false)?;
        let (scans, intents) = self.scans(store, root, path)?;
        let (partition, window) = self.partition(store, path)?;
        let window = self.window(window);
        let processor = self.processor(attributes);
        let field_names = strings(store, &format!("{path}/field_name"))?;
        if field_names.len() != times.len()
            || scans
                .as_ref()
                .is_some_and(|scans| scans.len() != times.len())
        {
            return Err(invalid("field_name and scan_name are not one per time"));
        }
        let cube = [
            times.len(),
            baselines.len(),
            partition.frequencies_hz.len(),
            partition.correlations.len(),
        ];
        let mut groups = Vec::new();
        let mut entries = Vec::new();
        // Each field dataset's fields, by the dataset and its groups' UVW
        // frame.
        let mut contexts: Vec<((String, DirectionRef), Vec<FieldId>)> = Vec::new();
        for record in data_groups(attributes)? {
            let uvw = format!("{path}/{}", record.uvw);
            let uvw_frame = direction_frame(open_array(store, &uvw)?.attributes(), "UVW")?;
            let key = (record.field_and_source.clone(), uvw_frame);
            let fields = match contexts.iter().find(|(known, _)| *known == key) {
                Some((_, fields)) => fields.clone(),
                None => {
                    let fields = self.fields(
                        store,
                        &format!("{path}/{}", record.field_and_source),
                        (&times, &field_names),
                        (uvw_frame, window),
                    )?;
                    contexts.push((key, fields.clone()));
                    fields
                }
            };
            let arrays = open_group(store, path, record, cube, fields, uvw_frame)?;
            entries.push(DataGroupEntry {
                name: arrays.record.name.clone(),
                visibilities: arrays.record.correlated_data.clone(),
                flags: arrays.record.flag.clone(),
                weights: arrays.record.weight.clone(),
                uvw: arrays.record.uvw.clone(),
                units: visibility_units(arrays.values.attributes()),
                uvw_frame: arrays.uvw_frame,
            });
            groups.push(arrays);
        }
        if groups.is_empty() {
            return Err(invalid("no data group"));
        }
        let fields: Vec<FieldId> = groups
            .iter()
            .flat_map(|group| group.fields.iter().copied())
            .collect();
        self.catalogue_mut().partitions.push(PartitionEntry {
            intents,
            observation,
            feeds: None,
            fields: crate::observation_io::distinct(&fields),
            scans: scans
                .as_deref()
                .map(crate::observation_io::distinct)
                .unwrap_or_default(),
            baselines,
            time_scale,
            time_origin,
            data_groups: entries,
            receptors,
            window,
            // MSv4 states intents only.
            states: Vec::new(),
            processor,
            ..partition
        });
        self.datasets.push(Dataset {
            path: path.to_string(),
            times,
            interval_s,
            centroid,
            centroid_extra_precision,
            exposure,
            scans,
            groups,
            antennas: antennas.to_vec(),
        });
        Ok(())
    }

    /// The catalogue's observing session for this dataset's
    /// `observation_info` and telescope, added if new.
    fn observation_info(
        &mut self,
        store: &Arc<FilesystemStore>,
        path: &str,
        attributes: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<ObservationId, ObservationError> {
        let telescope = strings(store, &format!("{path}/antenna_xds/telescope_name"))?
            .into_iter()
            .next()
            .unwrap_or_default();
        let info = attributes.get("observation_info");
        let uid = |key: &str| {
            info.and_then(|info| info.get(key))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        let entry = ObservationEntry {
            telescope,
            execution_block: uid("execution_block_UID"),
            session_reference: uid("session_reference_UID"),
            scheduling_block: uid("scheduling_block_UID"),
            project: info
                .and_then(|info| info.get("project_UID"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            observers: info
                .and_then(|info| info.get("observer"))
                .and_then(serde_json::Value::as_array)
                .map(|observers| {
                    observers
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            // `observation_info` states no extent; one is never derived
            // from the samples.
            time_range: None,
            release_date: uid("release_date").as_deref().and_then(iso_utc),
            flagged: None,
        };
        let observations = &mut self.catalogue_mut().observations;
        let index = observations
            .iter()
            .position(|known| *known == entry)
            .unwrap_or_else(|| {
                observations.push(entry);
                observations.len() - 1
            });
        Ok(ObservationId(
            u32::try_from(index).expect("few observations"),
        ))
    }

    /// The dataset of partition `id` and its entry.
    pub(super) fn dataset(
        &self,
        id: PartitionId,
    ) -> Result<(&Dataset, &PartitionEntry), ObservationError> {
        match (
            self.datasets.get(id.0 as usize),
            self.catalogue.partitions.get(id.0 as usize),
        ) {
            (Some(dataset), Some(entry)) => Ok((dataset, entry)),
            _ => Err(ObservationError::InvalidRequest {
                reason: format!("no partition {id:?}"),
            }),
        }
    }

    /// The catalogue's window `entry` describes, added if new: one per
    /// spectral window name, as XRADIO names one per window.
    fn window(&mut self, entry: WindowEntry) -> WindowId {
        let windows = &mut self.catalogue_mut().windows;
        let index = windows
            .iter()
            .position(|known| known.name == entry.name)
            .unwrap_or_else(|| {
                windows.push(entry);
                windows.len() - 1
            });
        WindowId(u32::try_from(index).expect("few windows"))
    }

    /// The catalogue's processor for this dataset's `processor_info`,
    /// which states a type and a subtype, added if new; `None` without
    /// one.
    fn processor(
        &mut self,
        attributes: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<ProcessorId> {
        let info = attributes.get("processor_info")?;
        let text = |key: &str| {
            info.get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        let entry = ProcessorEntry {
            kind: text("type"),
            sub_type: text("sub_type"),
            ..ProcessorEntry::default()
        };
        let processors = &mut self.catalogue_mut().processors;
        let index = processors
            .iter()
            .position(|known| known.kind == entry.kind && known.sub_type == entry.sub_type)
            .unwrap_or_else(|| {
                processors.push(entry);
                processors.len() - 1
            });
        Some(ProcessorId(u32::try_from(index).expect("few processors")))
    }

    /// The partition's spectral window and correlations, and the window's
    /// entry; its data groups are added by the caller.
    fn partition(
        &self,
        store: &Arc<FilesystemStore>,
        path: &str,
    ) -> Result<(PartitionEntry, WindowEntry), ObservationError> {
        let (frequencies_hz, attributes) = numbers::<f64>(store, &format!("{path}/frequency"))?;
        let reference_frequency = attributes
            .get("reference_frequency")
            .map(|quantity| {
                let of = "frequency.reference_frequency";
                let frame = match quantity.get("attrs").and_then(serde_json::Value::as_object) {
                    Some(attrs) => frequency_frame(attrs, of)?,
                    None => return Err(invalid(format!("{of} has no attributes"))),
                };
                quantity
                    .get("data")
                    .and_then(serde_json::Value::as_f64)
                    .map(|hz| MFrequency::new(hz, frame))
                    .ok_or_else(|| invalid(format!("{of} has no value")))
            })
            .transpose()?;
        let frequency_frame = frequency_frame(&attributes, "frequency")?;
        let width = attributes
            .get("channel_width")
            .ok_or_else(|| invalid("frequency has no channel_width"))?;
        if width
            .get("attrs")
            .and_then(|attrs| attrs.get("units"))
            .and_then(serde_json::Value::as_str)
            != Some("Hz")
        {
            return Err(invalid("frequency.channel_width is not in Hz"));
        }
        let channel_width = width
            .get("data")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| invalid("frequency.channel_width has no value"))?;
        let correlations = strings(store, &format!("{path}/polarization"))?
            .iter()
            .map(|label| {
                Correlation::from_label(label)
                    .ok_or_else(|| invalid(format!("unknown polarization {label}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let name = text(&attributes, "spectral_window_name", "frequency")?.to_string();
        // XRADIO's frequency coordinate states the reference frequency; the
        // other members of an MSv2 window it does not carry.
        let window = WindowEntry {
            name: name.clone(),
            reference_frequency,
            ..WindowEntry::default()
        };
        let entry = PartitionEntry {
            spectral_window: name,
            channel_widths_hz: vec![channel_width; frequencies_hz.len()],
            frequencies_hz,
            frequency_frame,
            correlations,
            // The caller fills what the rest of the dataset states.
            intents: Vec::new(),
            observation: ObservationId(0),
            feeds: None,
            fields: Vec::new(),
            scans: Vec::new(),
            baselines: Vec::new(),
            time_scale: casa_types::measures::EpochRef::UTC,
            time_origin: casa_types::measures::MjdHighPrec::new(0.0, 0.0),
            data_groups: Vec::new(),
            receptors: BTreeMap::new(),
            window: WindowId(0),
            states: Vec::new(),
            processor: None,
        };
        Ok((entry, window))
    }

    /// The catalogue scan of each time, when the dataset has `scan_name`,
    /// and the dataset's intents (`scan_intents`, sorted).
    fn scans(
        &mut self,
        store: &Arc<FilesystemStore>,
        root: &Path,
        path: &str,
    ) -> Result<(Option<Vec<ScanId>>, Vec<String>), ObservationError> {
        let variable = format!("{path}/scan_name");
        if !exists(root, &variable) {
            return Ok((None, Vec::new()));
        }
        let names = strings(store, &variable)?;
        let mut intents: Vec<String> = open_array(store, &variable)?
            .attributes()
            .get("scan_intents")
            .and_then(serde_json::Value::as_array)
            .map(|intents| {
                intents
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        intents.sort();
        intents.dedup();
        let scans = &mut self.catalogue_mut().scans;
        let ids = names
            .iter()
            .map(|name| {
                let index = scans
                    .iter()
                    .position(|known| known.name == *name)
                    .unwrap_or_else(|| {
                        scans.push(ScanEntry {
                            name: name.clone(),
                            number: name.parse().ok(),
                            intents: Vec::new(),
                        });
                        scans.len() - 1
                    });
                for intent in &intents {
                    if !scans[index].intents.contains(intent) {
                        scans[index].intents.push(intent.clone());
                    }
                }
                ScanId(u32::try_from(index).expect("few scans"))
            })
            .collect();
        Ok((Some(ids), intents))
    }

    /// The catalogue field at each of the dataset's `times` (Unix seconds)
    /// of `per_time`'s names, read from the field dataset at `path` of a
    /// data group whose UVW is in `uvw_frame`, in spectral window `window`:
    /// a `field_and_source` dataset's phase centres, in that frame, and its
    /// sources' records for the window ([`super::sources`]), or a
    /// `field_and_source_ephemeris` dataset's moving fields
    /// ([`super::ephemeris`]).
    fn fields(
        &mut self,
        store: &Arc<FilesystemStore>,
        path: &str,
        (times, per_time): (&[f64], &[String]),
        (uvw_frame, window): (DirectionRef, WindowId),
    ) -> Result<Vec<FieldId>, ObservationError> {
        let group = Group::open(store.clone(), path).map_err(storage)?;
        if text(group.attributes(), "type", path)? == ephemeris::EPHEMERIS_DATASET {
            return self.moving_fields(store, path, (times, per_time), uvw_frame);
        }
        let names = strings(store, &format!("{path}/field_name"))?;
        let sources = strings(store, &format!("{path}/source_name"))?;
        let (centres, attributes) =
            numbers::<f64>(store, &format!("{path}/FIELD_PHASE_CENTER_DIRECTION"))?;
        require_units(&attributes, "rad", "FIELD_PHASE_CENTER_DIRECTION")?;
        let frame = direction_frame(&attributes, "FIELD_PHASE_CENTER_DIRECTION")?;
        if frame != uvw_frame {
            return Err(ObservationError::Unsupported {
                what: "an MSv4 data group whose UVW frame is not its phase centres' frame",
            });
        }
        if centres.len() != 2 * names.len() {
            return Err(invalid(format!("{path} has one phase centre per field")));
        }
        let records = super::sources::records(store, path, names.len(), window)?;
        let mut indices = Vec::new();
        for ((row, name), record) in names.iter().enumerate().zip(records) {
            // XRADIO's placeholder for an absent source is none.
            let source_name = sources
                .get(row)
                .filter(|name| !super::is_unknown_source(name))
                .cloned();
            let source = source_name.as_deref().map(|source| {
                super::sources::add(&mut self.catalogue_mut().sources, source, record)
            });
            let entry = FieldEntry {
                name: name.clone(),
                phase_centre: MDirection::from_angles(
                    centres[2 * row],
                    centres[2 * row + 1],
                    frame,
                ),
                // MSv4 phase centres are in celestial frames (fk5, icrs).
                reference_epoch: None,
                source_name,
                // The adapter does not read XRADIO's other field centres.
                source,
                code: None,
                delay_centre: None,
                reference_centre: None,
                flagged: None,
                ephemeris: None,
            };
            let fields = &mut self.catalogue_mut().fields;
            let index = match fields.iter().position(|known| same_field(known, &entry)) {
                Some(index) => {
                    // One name is one source, so a field met again names
                    // the source it named.
                    fields[index].source = fields[index].source.or(source);
                    index
                }
                None => {
                    fields.push(entry);
                    fields.len() - 1
                }
            };
            indices.push(FieldId(u32::try_from(index).expect("few fields")));
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

    /// [`Self::fields`] for a `field_and_source_ephemeris` dataset. A field
    /// is one with a catalogue field of its name, source and offset (to the
    /// store's resolution) whose ephemeris holds the same samples at the
    /// times both hold. Each dataset holds only the samples its own times
    /// need, so the body is the dataset's samples united with those of
    /// every ephemeris its matched fields follow ([`ephemeris::union`]):
    /// every catalogue field that follows any of them, the dataset's fields
    /// among them, then follows the union, as the fields of one MSv2
    /// `EPHEMERIS_ID` follow one table. The offset is labelled with the data
    /// group's UVW frame: casacore applies a moving field's offset in its
    /// ephemeris's frame whatever `PHASE_DIR`'s own
    /// (`MSFieldColumns::extractDirMeas`).
    fn moving_fields(
        &mut self,
        store: &Arc<FilesystemStore>,
        path: &str,
        (times, per_time): (&[f64], &[String]),
        uvw_frame: DirectionRef,
    ) -> Result<Vec<FieldId>, ObservationError> {
        let moving = ephemeris::read(store, path, times, per_time)?;
        let fields = &mut self.catalogue_mut().fields;
        let mut united = Arc::new(moving.ephemeris);
        let mut followed: Vec<Arc<casa_observation::FieldEphemeris>> = Vec::new();
        let mut matched = Vec::with_capacity(moving.fields.len());
        for field in &moving.fields {
            let source_name =
                (!super::is_unknown_source(&field.source_name)).then(|| field.source_name.clone());
            let found = fields.iter().position(|known| {
                known.name == field.name
                    && known.source_name == source_name
                    && known.phase_centre.refer() == uvw_frame
                    && ephemeris::same_angles(
                        known.phase_centre.as_angles().into(),
                        field.offset_rad,
                    )
                    && known.ephemeris.as_ref().is_some_and(|known| {
                        followed.iter().any(|seen| Arc::ptr_eq(seen, known))
                            || ephemeris::union(known, &united).is_some()
                    })
            });
            if let Some(index) = found {
                let known = fields[index].ephemeris.clone().expect("a moving field");
                if !followed.iter().any(|seen| Arc::ptr_eq(seen, &known)) {
                    united = ephemeris::union(&known, &united).expect("checked to unite");
                    followed.push(known);
                }
            }
            matched.push((found, source_name));
        }
        for field in fields.iter_mut() {
            if field
                .ephemeris
                .as_ref()
                .is_some_and(|known| followed.iter().any(|seen| Arc::ptr_eq(seen, known)))
            {
                field.ephemeris = Some(Arc::clone(&united));
            }
        }
        let mut indices = Vec::with_capacity(moving.fields.len());
        for (field, (found, source_name)) in moving.fields.iter().zip(matched) {
            let index = found.unwrap_or_else(|| {
                fields.push(FieldEntry {
                    name: field.name.clone(),
                    phase_centre: MDirection::from_angles(
                        field.offset_rad[0],
                        field.offset_rad[1],
                        uvw_frame,
                    ),
                    reference_epoch: None,
                    source_name,
                    // As for a fixed field (#976).
                    source: None,
                    code: None,
                    delay_centre: None,
                    reference_centre: None,
                    flagged: None,
                    ephemeris: Some(Arc::clone(&united)),
                });
                fields.len() - 1
            });
            indices.push(FieldId(u32::try_from(index).expect("few fields")));
        }
        Ok(moving
            .per_time
            .iter()
            .map(|field| indices[*field])
            .collect())
    }
}

/// Whether two fixed field entries are one field: the same name, phase
/// centre (frame and angles) and source. A shared display name alone is
/// not.
fn same_field(a: &FieldEntry, b: &FieldEntry) -> bool {
    a.ephemeris.is_none()
        && b.ephemeris.is_none()
        && a.name == b.name
        && a.source_name == b.source_name
        && a.phase_centre.refer() == b.phase_centre.refer()
        && a.phase_centre.as_angles() == b.phase_centre.as_angles()
}

/// Whether the processing set at `root` has node `variable`.
fn exists(root: &Path, variable: &str) -> bool {
    root.join(variable.trim_start_matches('/'))
        .join("zarr.json")
        .is_file()
}

/// Open data group `record`'s variables, checking their types, dimensions
/// and shapes; `fields` are its times' fields and `effective` its UVW
/// frame, checked against its fields' already.
fn open_group(
    store: &Arc<FilesystemStore>,
    path: &str,
    record: DataGroupRecord,
    cube: [usize; 4],
    fields: Vec<FieldId>,
    effective: DirectionRef,
) -> Result<GroupArrays, ObservationError> {
    let open = |name: &str, data_type: &str, dimensions: &[&str], shape: &[usize]| {
        let variable = format!("{path}/{name}");
        let array = open_array(store, &variable)?;
        require_layout(&array, &variable, data_type, dimensions)?;
        let expected: Vec<u64> = shape.iter().map(|n| *n as u64).collect();
        if array.shape() != expected {
            return Err(invalid(format!(
                "{variable} has shape {:?}, not {expected:?}",
                array.shape()
            )));
        }
        Ok(array)
    };
    let values = open(&record.correlated_data, "complex64", &CUBE, &cube)?;
    let flags = open(&record.flag, "bool", &CUBE, &cube)?;
    let weights = open(&record.weight, "float32", &CUBE, &cube)?;
    let uvw = open(&record.uvw, "float64", &UVW, &[cube[0], cube[1], 3])?;
    require_units(uvw.attributes(), "m", "UVW")?;
    Ok(GroupArrays {
        record,
        values,
        flags,
        weights,
        uvw,
        fields,
        uvw_frame: UvwFrame {
            effective,
            stored_label: Some(effective),
        },
    })
}

/// The group at `path`, refused unless it is a visibility dataset of the
/// profile's schema version.
fn visibility_group(
    store: &Arc<FilesystemStore>,
    path: &str,
) -> Result<Group<FilesystemStore>, ObservationError> {
    let group = Group::open(store.clone(), path).map_err(storage)?;
    if text(group.attributes(), "type", path)? != "visibility" {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 dataset other than a visibility dataset",
        });
    }
    if text(group.attributes(), "schema_version", path)? != SCHEMA_VERSION {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 schema version other than 4.0.1",
        });
    }
    Ok(group)
}

/// Each dataset's catalogue antennas (with their names), resolved from its
/// `antenna_xds`.
type DatasetAntennas = Vec<Vec<(String, AntennaId)>>;

/// Each dataset's antennas' receptors, from its `antenna_xds`.
type DatasetReceptors = Vec<BTreeMap<AntennaId, Receptors>>;

/// The catalogue's antennas and each of the datasets `names`' antennas,
/// reading each `antenna_xds` once. An antenna is one name and station
/// (one with two positions is refused, not merged); the catalogue orders
/// them so as to keep every dataset's order ([`merge_orders`]), so a store
/// either converter wrote from an MSv2 numbers its antennas as the MSv2
/// does wherever the datasets determine it, and no baseline's antennas
/// change places.
fn dataset_antennas(
    store: &Arc<FilesystemStore>,
    names: &[String],
) -> Result<(Vec<AntennaEntry>, DatasetAntennas, DatasetReceptors), ObservationError> {
    let mut known: HashMap<(String, String), usize> = HashMap::new();
    let mut first: Vec<AntennaEntry> = Vec::new();
    let mut lists: Vec<Vec<(String, usize)>> = Vec::with_capacity(names.len());
    let mut receptors: Vec<Vec<(usize, Receptors)>> = Vec::with_capacity(names.len());
    // A dataset's group is opened once, by `add_dataset` (an XRADIO
    // dataset's metadata is consolidated there, so a second open costs
    // about what its antennas do). An error here is reported as opening
    // would have: the first of the groups so far that is no visibility
    // dataset of the profile's schema is refused as such instead.
    let refused = |error: ObservationError, upto: usize| {
        names[..=upto]
            .iter()
            .find_map(|name| visibility_group(store, &format!("/{name}")).err())
            .unwrap_or(error)
    };
    for (index, name) in names.iter().enumerate() {
        let path = format!("/{name}");
        let read = read_antennas(store, &format!("{path}/antenna_xds"))
            .map_err(|error| refused(error, index))?;
        let mut list = Vec::new();
        let mut held = Vec::new();
        for (antenna, own) in read {
            let label = antenna.name.clone();
            let next = first.len();
            let rank = *known
                .entry((antenna.name.clone(), antenna.station.clone()))
                .or_insert(next);
            if rank == next {
                first.push(antenna);
            } else if first[rank].position.values() != antenna.position.values() {
                return Err(refused(
                    ObservationError::Unsupported {
                        what: "one MSv4 antenna name with two positions",
                    },
                    index,
                ));
            }
            list.push((label, rank));
            if !own.types.is_empty() {
                held.push((rank, own));
            }
        }
        lists.push(list);
        receptors.push(held);
    }
    let ranks = lists
        .iter()
        .map(|list| list.iter().map(|(_, rank)| *rank).collect())
        .collect();
    let order = merge_orders(ranks, |rank| *rank);
    let mut id = vec![AntennaId(0); first.len()];
    for (index, rank) in order.iter().enumerate() {
        id[*rank] = AntennaId(u32::try_from(index).expect("few antennas"));
    }
    let mut entries: Vec<Option<AntennaEntry>> = first.into_iter().map(Some).collect();
    let antennas = order
        .iter()
        .map(|rank| entries[*rank].take().expect("each antenna once"))
        .collect();
    let per_dataset = lists
        .into_iter()
        .map(|list| {
            list.into_iter()
                .map(|(label, rank)| (label, id[rank]))
                .collect()
        })
        .collect();
    let receptors = receptors
        .into_iter()
        .map(|held| {
            held.into_iter()
                .map(|(rank, own)| (id[rank], own))
                .collect()
        })
        .collect();
    Ok((antennas, per_dataset, receptors))
}

/// The baselines of the dataset at `path`, resolved by name through its
/// `antennas`.
fn baselines(
    store: &Arc<FilesystemStore>,
    path: &str,
    antennas: &[(String, AntennaId)],
) -> Result<Vec<OrderedBaseline>, ObservationError> {
    // By name, the first of a name kept: a scan per baseline would cost
    // baselines × antennas comparisons.
    let mut ids: HashMap<&str, AntennaId> = HashMap::with_capacity(antennas.len());
    for (name, id) in antennas {
        ids.entry(name.as_str()).or_insert(*id);
    }
    let baseline_antenna = |which: &str| -> Result<Vec<AntennaId>, ObservationError> {
        strings(store, &format!("{path}/baseline_antenna{which}_name"))?
            .iter()
            .map(|name| {
                ids.get(name.as_str()).copied().ok_or_else(|| {
                    invalid(format!("baseline antenna {name} is not in antenna_xds"))
                })
            })
            .collect()
    };
    Ok(baseline_antenna("1")?
        .into_iter()
        .zip(baseline_antenna("2")?)
        .map(|(antenna1, antenna2)| OrderedBaseline { antenna1, antenna2 })
        .collect())
}

/// The items of `lists`, each once (by `key`), in an order that keeps each
/// list's: a topological order of the lists' successions, the item first
/// seen first among those free to come next. Lists that disagree (neither
/// converter writes them) are reconciled the same way: when none is free,
/// the earliest seen item left goes next, which need not lie on the cycle
/// and may break more of the lists' order than the cycle requires.
fn merge_orders<T, K: Eq + std::hash::Hash>(lists: Vec<Vec<T>>, key: impl Fn(&T) -> K) -> Vec<T> {
    let mut index: HashMap<K, usize> = HashMap::new();
    let mut items: Vec<Option<T>> = Vec::new();
    let mut ranks: Vec<Vec<usize>> = Vec::with_capacity(lists.len());
    for list in lists {
        let mut ranked = Vec::with_capacity(list.len());
        for item in list {
            let next = items.len();
            let rank = *index.entry(key(&item)).or_insert(next);
            if rank == next {
                items.push(Some(item));
            }
            ranked.push(rank);
        }
        ranks.push(ranked);
    }
    let mut successors: Vec<HashSet<usize>> = vec![HashSet::new(); items.len()];
    let mut predecessors = vec![0_usize; items.len()];
    for ranked in &ranks {
        for pair in ranked.windows(2) {
            if pair[0] != pair[1] && successors[pair[0]].insert(pair[1]) {
                predecessors[pair[1]] += 1;
            }
        }
    }
    let mut free: BinaryHeap<Reverse<usize>> = (0..items.len())
        .filter(|item| predecessors[*item] == 0)
        .map(Reverse)
        .collect();
    let mut merged = Vec::with_capacity(items.len());
    while merged.len() < items.len() {
        let Reverse(item) = free.pop().unwrap_or_else(|| {
            // A cycle: the lists disagree; the earliest seen item left goes
            // next.
            let item = (0..items.len())
                .find(|item| items[*item].is_some() && predecessors[*item] > 0)
                .expect("an item is left");
            Reverse(item)
        });
        let Some(value) = items[item].take() else {
            continue;
        };
        predecessors[item] = 0;
        for next in std::mem::take(&mut successors[item]) {
            predecessors[next] = predecessors[next].saturating_sub(1);
            if predecessors[next] == 0 && items[next].is_some() {
                free.push(Reverse(next));
            }
        }
        merged.push(value);
    }
    merged
}

/// The antennas of the `antenna_xds` at `path`, with their receptors.
fn read_antennas(
    store: &Arc<FilesystemStore>,
    path: &str,
) -> Result<Vec<(AntennaEntry, Receptors)>, ObservationError> {
    let names = strings(store, &format!("{path}/antenna_name"))?;
    let stations = strings(store, &format!("{path}/station_name"))?;
    let mounts = strings(store, &format!("{path}/mount"))?;
    // XRADIO writes no receptor angles for a window FEED states none for,
    // though it may still write the polarization types; such antennas have
    // no receptors, as the MSv2 adapter reads that FEED.
    let types = if has_node(store, &format!("{path}/polarization_type"))?
        && has_node(store, &format!("{path}/ANTENNA_RECEPTOR_ANGLE"))?
    {
        strings(store, &format!("{path}/polarization_type"))?
    } else {
        Vec::new()
    };
    let (positions, attributes) = numbers::<f64>(store, &format!("{path}/ANTENNA_POSITION"))?;
    require_units(&attributes, "m", "ANTENNA_POSITION")?;
    if text(&attributes, "frame", "ANTENNA_POSITION")? != "ITRS" {
        return Err(ObservationError::Unsupported {
            what: "MSv4 antenna positions in other than ITRS",
        });
    }
    let (diameters, attributes) = numbers::<f64>(store, &format!("{path}/ANTENNA_DISH_DIAMETER"))?;
    require_units(&attributes, "m", "ANTENNA_DISH_DIAMETER")?;
    let angles = if types.is_empty() {
        Vec::new()
    } else {
        let (angles, attributes) =
            numbers::<f64>(store, &format!("{path}/ANTENNA_RECEPTOR_ANGLE"))?;
        require_units(&attributes, "rad", "ANTENNA_RECEPTOR_ANGLE")?;
        angles
    };
    let count = names.len();
    let receptors = types.len() / count.max(1);
    if [stations.len(), mounts.len(), diameters.len()] != [count; 3]
        || positions.len() != 3 * count
        || types.len() != receptors * count
        || angles.len() != receptors * count
    {
        return Err(invalid(format!(
            "{path} variables disagree on the antennas"
        )));
    }
    Ok((0..count)
        .map(|index| {
            (
                AntennaEntry {
                    name: names[index].clone(),
                    station: stations[index].clone(),
                    position: MPosition::new_itrf(
                        positions[3 * index],
                        positions[3 * index + 1],
                        positions[3 * index + 2],
                    ),
                    mount: mounts[index].clone(),
                    dish_diameter: metres(diameters[index]),
                    flagged: None,
                },
                Receptors {
                    types: types[receptors * index..receptors * (index + 1)].to_vec(),
                    angles: angles[receptors * index..receptors * (index + 1)]
                        .iter()
                        .map(|angle| radians(*angle))
                        .collect(),
                },
            )
        })
        .collect())
}

impl Observation for Msv4Observation {
    fn catalogue(&self) -> &Arc<Catalogue> {
        &self.catalogue
    }

    fn estimate(&self, request: &AccessRequest) -> Result<Estimate, ObservationError> {
        Msv4Observation::estimate(self, request)
    }

    fn bind_read(&self, request: &AccessRequest) -> Result<Traversal<'_>, ObservationError> {
        Msv4Observation::bind_read(self, request)
    }

    fn grid(
        &self,
        id: PartitionId,
        data_group: &str,
    ) -> Result<TimeBaselineGrid, ObservationError> {
        Msv4Observation::grid(self, id, data_group)
    }

    fn pointing(
        &self,
        request: &casa_observation::PointingRequest,
    ) -> Result<casa_observation::PointingSeries, ObservationError> {
        self.read_pointing(request)
    }

    fn pointing_bytes(&self) -> Result<usize, ObservationError> {
        self.pointing_bound()
    }

    fn group_keys(
        &self,
        id: PartitionId,
        data_group: &str,
        grouping: Grouping,
        fields: &FieldSelection,
    ) -> Result<Option<Vec<Option<GroupKey>>>, ObservationError> {
        let (dataset, entry) = self.dataset(id)?;
        let (index, _) =
            entry
                .data_group(data_group)
                .ok_or_else(|| ObservationError::InvalidRequest {
                    reason: format!("{id:?} has no data group {data_group}"),
                })?;
        super::super::grouping::group_keys(
            id,
            grouping,
            &dataset.times,
            &dataset.groups[index].fields,
            dataset.scans.as_deref(),
            fields,
        )
    }
}

impl Msv4Observation {
    /// What `request` costs ([`Observation::estimate`]), from the chunk
    /// grids alone.
    ///
    /// # Errors
    ///
    /// When the request names an unknown partition, data group, channel or
    /// correlation product, asks for a role a dataset does not hold
    /// (`TIME_CENTROID`, `EFFECTIVE_INTEGRATION_TIME`,
    /// `time.integration_time` or `scan_name`), or budgets less than one
    /// time. [`ObservationError::Unsupported`] when it asks for centroids
    /// that a dataset splits into `TIME_CENTROID` and
    /// `TIME_CENTROID_EXTRA_PRECISION`.
    pub fn estimate(&self, request: &AccessRequest) -> Result<Estimate, ObservationError> {
        Ok(Estimate {
            partitions: request
                .partitions
                .iter()
                .map(|&id| Ok(super::cursor::estimate(self, request, id)?.0))
                .collect::<Result<_, ObservationError>>()?,
        })
    }

    /// Bind a traversal of `request`: one cursor per dataset, whole times
    /// per batch as the byte budget admits.
    ///
    /// # Errors
    ///
    /// As [`Self::estimate`].
    pub fn bind_read(&self, request: &AccessRequest) -> Result<Traversal<'_>, ObservationError> {
        let bound = request
            .partitions
            .iter()
            .map(|&id| super::cursor::estimate(self, request, id))
            .collect::<Result<Vec<_>, _>>()?;
        let estimate = Estimate {
            partitions: bound.iter().map(|(estimate, _)| estimate.clone()).collect(),
        };
        let cursors = bound
            .into_iter()
            .map(|(estimate, resolved)| {
                Msv4Cursor::bind(self, request, estimate, resolved)
                    .map(|cursor| Box::new(cursor) as BoundCursor<'_>)
            })
            .collect::<Result<_, _>>()?;
        Ok(Traversal::new(cursors, estimate))
    }

    /// Partition `id`'s grid ([`Observation::grid`]): the dataset's times,
    /// `scan_name` and `time.integration_time`, and data group
    /// `data_group`'s fields.
    ///
    /// # Errors
    ///
    /// As [`Observation::grid`]; also when the dataset lacks `scan_name` or
    /// `time.integration_time`.
    pub fn grid(
        &self,
        id: PartitionId,
        data_group: &str,
    ) -> Result<TimeBaselineGrid, ObservationError> {
        let (dataset, entry) = self.dataset(id)?;
        let lacks = |what: &str| ObservationError::InvalidRequest {
            reason: format!("{id:?} has no {what}, which a grid needs"),
        };
        let (index, _) =
            entry
                .data_group(data_group)
                .ok_or_else(|| ObservationError::InvalidRequest {
                    reason: format!("no data group {data_group} in {id:?}"),
                })?;
        Ok(TimeBaselineGrid {
            baselines: entry.baselines.clone(),
            time_scale: entry.time_scale,
            time_origin: entry.time_origin,
            times_s: dataset.times.clone(),
            field_per_time: dataset.groups[index].fields.clone(),
            scan_per_time: dataset.scans.clone().ok_or_else(|| lacks("scan_name"))?,
            interval_s: dataset
                .interval_s
                .ok_or_else(|| lacks("time.integration_time"))?,
        })
    }
}

/// A UTC instant as XRADIO writes one (`2026-10-24T16:32:07.399771`, with
/// an optional `Z` or `+00:00`); `None` for any other form.
fn iso_utc(text: &str) -> Option<MEpoch> {
    let text = text
        .strip_suffix('Z')
        .or_else(|| text.strip_suffix("+00:00"))
        .unwrap_or(text);
    let (date, time) = text.split_once('T')?;
    let mut date = date.splitn(3, '-').map(str::parse::<u32>);
    let (year, month, day) = (date.next()?.ok()?, date.next()?.ok()?, date.next()?.ok()?);
    let mut time = time.splitn(3, ':');
    let (hour, minute) = (time.next()?.parse().ok()?, time.next()?.parse().ok()?);
    let second: f64 = time.next()?.parse().ok()?;
    let instant =
        MvTime::from_ymd_hms_utc(i32::try_from(year).ok()?, month, day, hour, minute, second)?;
    Some(MEpoch::from_mjd_seconds(
        instant.as_mjd_seconds(),
        EpochRef::UTC,
    ))
}

#[cfg(test)]
mod tests {
    use super::{iso_utc, merge_orders};

    #[test]
    fn xradio_instants_read_as_utc_and_others_as_none() {
        let mjd = |text: &str| iso_utc(text).map(|epoch| epoch.value().as_mjd());
        assert_eq!(mjd("1858-11-17T00:00:00.000000"), Some(0.0));
        let afternoon = mjd("2026-10-24T16:32:07.399771").expect("an instant");
        let expected = 61_337.0 + (16.0 * 3600.0 + 32.0 * 60.0 + 7.399_771) / 86_400.0;
        assert!(
            (afternoon - expected).abs() < 1e-9,
            "{afternoon} against {expected}"
        );
        assert_eq!(mjd("2026-10-24T16:32:07Z"), mjd("2026-10-24T16:32:07"));
        assert_eq!(mjd("2026-10-24T16:32:07+00:00"), mjd("2026-10-24T16:32:07"));
        assert_eq!(mjd("2026-10-24T16:32:07+02:00"), None);
        assert_eq!(mjd("24 Oct 2026"), None);
    }

    fn merged(lists: &[&[&str]]) -> Vec<String> {
        let lists = lists
            .iter()
            .map(|list| list.iter().map(ToString::to_string).collect())
            .collect();
        merge_orders(lists, Clone::clone)
    }

    #[test]
    fn merged_antennas_keep_every_datasets_order() {
        // The first dataset lacks a middle antenna the second places.
        assert_eq!(
            merged(&[&["W08", "W16", "W32"], &["W08", "W16", "W24", "W32"]]),
            ["W08", "W16", "W24", "W32"]
        );
        // Disjoint datasets keep the order they are seen in.
        assert_eq!(merged(&[&["A", "B"], &["C", "D"]]), ["A", "B", "C", "D"]);
        // Chains across datasets: B < C only through the third.
        assert_eq!(
            merged(&[&["A", "C"], &["B", "D"], &["A", "B", "C"]]),
            ["A", "B", "C", "D"]
        );
        // Disagreeing lists still give each item once.
        assert_eq!(merged(&[&["A", "B"], &["B", "A"]]), ["A", "B"]);
    }
}
