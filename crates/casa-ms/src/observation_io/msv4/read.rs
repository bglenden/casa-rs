// SPDX-License-Identifier: LGPL-3.0-or-later
//! Opening an MSv4 processing set: each `VisibilityXds` dataset is a
//! partition, with its catalogue checked against the profile once.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use casa_observation::{
    AccessRequest, AntennaEntry, AntennaId, BoundCursor, Catalogue, Correlation, DataGroupEntry,
    Estimate, FieldEntry, FieldId, Observation, ObservationEntry, ObservationError, ObservationId,
    OrderedBaseline, PartitionEntry, PartitionId, ScanEntry, ScanId, TimeBaselineGrid, Traversal,
    UvwFrame,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::position::MPosition;
use zarrs::array::{Array, CodecOptions};
use zarrs::filesystem::FilesystemStore;
use zarrs::group::Group;

use super::cursor::Msv4Cursor;
use super::metadata::{
    DataGroupRecord, SCHEMA_VERSION, data_groups, direction_frame, frequency_frame, invalid,
    numbers, open_array, require_layout, require_units, storage, strings, text, time_reference,
    visibility_units,
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
}

/// An opened MSv4 processing set (XRADIO `VisibilityXds` schema 4.0.1 on
/// local Zarr v3) and its catalogue.
///
/// Each visibility dataset is one partition, described by its content
/// (spectral window, correlations, the intents of its `scan_name`, its
/// `observation_info`). Antennas are the union of the datasets'
/// `antenna_xds` entries, one per name, station and position.
/// Fields come from each data group's own `field_and_source` dataset and
/// are distinct unless name, phase centre and source all agree: two groups
/// with different phase centres keep their own fields and UVW frames, and a
/// traversal of either delivers its own. Opening checks every variable's
/// data type and dimensions, and every unit and frame, against the profile
/// and refuses others rather than reinterpreting or rescaling them.
pub struct Msv4Observation {
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
            catalogue: Arc::new(Catalogue::default()),
            datasets: Vec::new(),
            options: CodecOptions::default().with_concurrent_target(1),
            open: options.clone(),
        };
        for name in names {
            observation.add_dataset(&store, path.as_ref(), &format!("/{name}"))?;
        }
        if observation.datasets.is_empty() {
            return Err(invalid("the processing set has no visibility datasets"));
        }
        Ok(observation)
    }

    /// The catalogue.
    #[must_use]
    pub fn catalogue(&self) -> &Arc<Catalogue> {
        &self.catalogue
    }

    /// The catalogue while the store is being opened, before any batch or
    /// cursor shares it.
    fn catalogue_mut(&mut self) -> &mut Catalogue {
        Arc::get_mut(&mut self.catalogue).expect("the catalogue is shared only after opening")
    }

    fn add_dataset(
        &mut self,
        store: &Arc<FilesystemStore>,
        root: &Path,
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
        if exists(root, &format!("{path}/polarization_mixed")) {
            // The partition's correlations are one list for every baseline;
            // a per-baseline basis under `PP`, `PQ`, `QP`, `QQ` would be
            // lost (or misread as those labels' own types).
            return Err(ObservationError::Unsupported {
                what: "an MSv4 dataset whose polarization basis differs by baseline \
                       (polarization_mixed)",
            });
        }
        let baselines = self.baselines(store, path)?;
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
        let partition = self.partition(store, path)?;
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
        let mut contexts = BTreeMap::<String, (Vec<FieldId>, DirectionRef)>::new();
        for record in data_groups(attributes)? {
            let context = match contexts.get(&record.field_and_source) {
                Some(context) => context.clone(),
                None => {
                    let context = self.fields(
                        store,
                        &format!("{path}/{}", record.field_and_source),
                        &field_names,
                    )?;
                    contexts.insert(record.field_and_source.clone(), context.clone());
                    context
                }
            };
            let arrays = open_group(store, path, record, cube, context)?;
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
            ..partition
        });
        self.datasets.push(Dataset {
            times,
            interval_s,
            centroid,
            centroid_extra_precision,
            exposure,
            scans,
            groups,
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

    /// The dataset's baselines, resolved through its `antenna_xds`.
    fn baselines(
        &mut self,
        store: &Arc<FilesystemStore>,
        path: &str,
    ) -> Result<Vec<OrderedBaseline>, ObservationError> {
        let antennas = read_antennas(store, &format!("{path}/antenna_xds"))?;
        let names: Vec<String> = antennas
            .iter()
            .map(|antenna| antenna.name.clone())
            .collect();
        let ids = antennas
            .into_iter()
            .map(|antenna| self.antenna(antenna))
            .collect::<Result<Vec<_>, _>>()?;
        let baseline_antenna = |which: &str| -> Result<Vec<AntennaId>, ObservationError> {
            strings(store, &format!("{path}/baseline_antenna{which}_name"))?
                .iter()
                .map(|name| {
                    names
                        .iter()
                        .position(|known| known == name)
                        .map(|index| ids[index])
                        .ok_or_else(|| {
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

    /// The partition's spectral window and correlations; its data groups
    /// are added by the caller.
    fn partition(
        &self,
        store: &Arc<FilesystemStore>,
        path: &str,
    ) -> Result<PartitionEntry, ObservationError> {
        let (frequencies_hz, attributes) = numbers::<f64>(store, &format!("{path}/frequency"))?;
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
        Ok(PartitionEntry {
            spectral_window: text(&attributes, "spectral_window_name", "frequency")?.to_string(),
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
        })
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

    /// The catalogue antenna `antenna` describes, added if new. One name
    /// and station with two positions is refused, not merged.
    fn antenna(&mut self, antenna: AntennaEntry) -> Result<AntennaId, ObservationError> {
        let antennas = &mut self.catalogue_mut().antennas;
        let index = match antennas
            .iter()
            .position(|known| known.name == antenna.name && known.station == antenna.station)
        {
            Some(index) if antennas[index].position.values() == antenna.position.values() => index,
            Some(_) => {
                return Err(ObservationError::Unsupported {
                    what: "one MSv4 antenna name with two positions",
                });
            }
            None => {
                antennas.push(antenna);
                antennas.len() - 1
            }
        };
        Ok(AntennaId(u32::try_from(index).expect("few antennas")))
    }

    /// The catalogue field of each of `per_time`'s names, read from the
    /// `field_and_source` dataset at `path`, and the phase centres' frame.
    fn fields(
        &mut self,
        store: &Arc<FilesystemStore>,
        path: &str,
        per_time: &[String],
    ) -> Result<(Vec<FieldId>, DirectionRef), ObservationError> {
        let names = strings(store, &format!("{path}/field_name"))?;
        let sources = strings(store, &format!("{path}/source_name"))?;
        let (centres, attributes) =
            numbers::<f64>(store, &format!("{path}/FIELD_PHASE_CENTER_DIRECTION"))?;
        require_units(&attributes, "rad", "FIELD_PHASE_CENTER_DIRECTION")?;
        let frame = direction_frame(&attributes, "FIELD_PHASE_CENTER_DIRECTION")?;
        if centres.len() != 2 * names.len() {
            return Err(invalid(format!("{path} has one phase centre per field")));
        }
        let mut indices = Vec::new();
        for (row, name) in names.iter().enumerate() {
            let entry = FieldEntry {
                name: name.clone(),
                phase_centre: MDirection::from_angles(
                    centres[2 * row],
                    centres[2 * row + 1],
                    frame,
                ),
                source_name: sources.get(row).cloned(),
            };
            let fields = &mut self.catalogue_mut().fields;
            let index = fields
                .iter()
                .position(|known| same_field(known, &entry))
                .unwrap_or_else(|| {
                    fields.push(entry);
                    fields.len() - 1
                });
            indices.push(FieldId(u32::try_from(index).expect("few fields")));
        }
        let per_time = per_time
            .iter()
            .map(|name| {
                names
                    .iter()
                    .position(|known| known == name)
                    .map(|row| indices[row])
                    .ok_or_else(|| invalid(format!("field {name} is not in {path}")))
            })
            .collect::<Result<_, _>>()?;
        Ok((per_time, frame))
    }
}

/// Whether two field entries are one field: the same name, phase centre
/// (frame and angles) and source. A shared display name alone is not.
fn same_field(a: &FieldEntry, b: &FieldEntry) -> bool {
    a.name == b.name
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
/// and shapes, and its UVW frame against its phase centres'.
fn open_group(
    store: &Arc<FilesystemStore>,
    path: &str,
    record: DataGroupRecord,
    cube: [usize; 4],
    (fields, phase_frame): (Vec<FieldId>, DirectionRef),
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
    let effective = direction_frame(uvw.attributes(), "UVW")?;
    if effective != phase_frame {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 data group whose UVW frame is not its phase centres' frame",
        });
    }
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

/// The antennas of the `antenna_xds` at `path`.
fn read_antennas(
    store: &Arc<FilesystemStore>,
    path: &str,
) -> Result<Vec<AntennaEntry>, ObservationError> {
    let names = strings(store, &format!("{path}/antenna_name"))?;
    let stations = strings(store, &format!("{path}/station_name"))?;
    let mounts = strings(store, &format!("{path}/mount"))?;
    let types = strings(store, &format!("{path}/polarization_type"))?;
    let (positions, attributes) = numbers::<f64>(store, &format!("{path}/ANTENNA_POSITION"))?;
    require_units(&attributes, "m", "ANTENNA_POSITION")?;
    if text(&attributes, "frame", "ANTENNA_POSITION")? != "ITRS" {
        return Err(ObservationError::Unsupported {
            what: "MSv4 antenna positions in other than ITRS",
        });
    }
    let (diameters, attributes) = numbers::<f64>(store, &format!("{path}/ANTENNA_DISH_DIAMETER"))?;
    require_units(&attributes, "m", "ANTENNA_DISH_DIAMETER")?;
    let (angles, attributes) = numbers::<f64>(store, &format!("{path}/ANTENNA_RECEPTOR_ANGLE"))?;
    require_units(&attributes, "rad", "ANTENNA_RECEPTOR_ANGLE")?;
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
        .map(|index| AntennaEntry {
            name: names[index].clone(),
            station: stations[index].clone(),
            position: MPosition::new_itrf(
                positions[3 * index],
                positions[3 * index + 1],
                positions[3 * index + 2],
            ),
            mount: mounts[index].clone(),
            dish_diameter: metres(diameters[index]),
            receptors: types[receptors * index..receptors * (index + 1)].to_vec(),
            receptor_angles: angles[receptors * index..receptors * (index + 1)]
                .iter()
                .map(|angle| radians(*angle))
                .collect(),
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
