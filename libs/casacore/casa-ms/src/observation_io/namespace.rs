// SPDX-License-Identifier: LGPL-3.0-or-later
//! The namespace a calibration table persists, which calibration's
//! `CalibrationBinding` binds a table to.
//!
//! A CASA calibration table numbers antennas, fields and spectral windows
//! by the rows of its own `ANTENNA`, `FIELD` and `SPECTRAL_WINDOW`
//! subtables, which CASA's `gaincal` copies from the MeasurementSet it
//! solved. Catalogue identities are opaque and scoped to one opened store,
//! so a caltable never holds them: [`persisted_namespace`] gives the
//! subtables a caltable solved on a store carries and the row each
//! catalogue entity has there, and [`NamespaceRows`] reads such subtables
//! back as content, from a caltable or from a store, for binding the one to
//! the other.
//!
//! An MSv2's namespace is its own subtables, so a caltable written through
//! it has the rows CASA's own would have and CASA applies it to that
//! MeasurementSet unchanged. An MSv4 has no such rows: its namespace is the
//! subtables an MSv2 converted from it would hold, built from its
//! catalogue as the converter builds them.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_core::measures::direction::MDirection;
use casa_core::measures::frequency::FrequencyRef;
use casa_core::{ArrayValue, ScalarValue};
use casa_observation::{Catalogue, ObservationError, PartitionId};
#[cfg(unix)]
use casa_observation::{FieldId, Observation, ScanId};
use casa_tables::{Table, TableOptions};

use super::msv2::catalogue::{measure_reference, phase_frames, require_units};
use super::update::storage;
use super::{Msv2Observation, OpenOptions};
use crate::columns::direction_columns::DirectionColumn;
use crate::selection::antenna_expr::StoredAntennaRow;
use crate::{MeasurementSet, MsError, SubtableId};

/// One `ANTENNA` row, as content.
#[derive(Clone, Debug, PartialEq)]
pub struct NamespaceAntenna {
    /// `NAME`.
    pub name: String,
    /// `STATION`.
    pub station: String,
    /// `POSITION`, geocentric ITRF metres.
    pub position_m: [f64; 3],
}

/// One `FIELD` row, as content.
#[derive(Clone, Debug)]
pub struct NamespaceField {
    /// `NAME`.
    pub name: String,
    /// `PHASE_DIR`'s constant term in its frame.
    pub phase_centre: MDirection,
}

/// One `SPECTRAL_WINDOW` row, as content.
#[derive(Clone, Debug, PartialEq)]
pub struct NamespaceWindow {
    /// `NAME`.
    pub name: String,
    /// `MEAS_FREQ_REF`.
    pub frame: FrequencyRef,
    /// `CHAN_FREQ`, hertz.
    pub frequencies_hz: Vec<f64>,
    /// `CHAN_WIDTH`, hertz.
    pub widths_hz: Vec<f64>,
}

/// The rows of a namespace's `ANTENNA`, `FIELD` and `SPECTRAL_WINDOW`
/// subtables, in row order.
#[derive(Clone, Debug)]
pub struct NamespaceRows {
    /// `ANTENNA` rows.
    pub antennas: Vec<NamespaceAntenna>,
    /// `FIELD` rows.
    pub fields: Vec<NamespaceField>,
    /// `SPECTRAL_WINDOW` rows.
    pub spectral_windows: Vec<NamespaceWindow>,
}

/// The namespace a calibration table solved on a store persists, and where
/// each entity of the store's catalogue, as one opening numbers it, is in
/// it.
pub struct PersistedNamespace {
    /// The store.
    pub source: PathBuf,
    /// The catalogue of the opening the namespace describes: the store
    /// opened with the same options numbers its entities alike.
    pub catalogue: Arc<Catalogue>,
    /// A MeasurementSet holding the subtables a caltable copies: an MSv2's
    /// own, or for an MSv4 one in memory holding no rows.
    pub subtables: MeasurementSet,
    /// Its `ANTENNA`, `FIELD` and `SPECTRAL_WINDOW` rows.
    pub rows: NamespaceRows,
    /// The `ANTENNA` row of each catalogue antenna.
    pub antennas: Vec<u32>,
    /// The `FIELD` row of each catalogue field.
    pub fields: Vec<u32>,
    /// The `SPECTRAL_WINDOW` row of each partition.
    pub spectral_windows: Vec<u32>,
    /// The `OBSERVATION` row of each observing session.
    pub observations: Vec<u32>,
}

impl PersistedNamespace {
    /// The `SPECTRAL_WINDOW` row of partition `id`.
    ///
    /// # Panics
    ///
    /// If `id` is not a partition of the store.
    #[must_use]
    pub fn spectral_window(&self, id: PartitionId) -> u32 {
        self.spectral_windows[id.0 as usize]
    }

    /// Whether `catalogue` numbers the store's entities as the namespace's
    /// opening does: the same antennas (station and position) and fields
    /// (phase centre) in order, as many sessions, and the same partitions in
    /// order, by content and session. A solve's identities translate
    /// through the namespace only then.
    #[must_use]
    pub fn describes(&self, catalogue: &Catalogue) -> bool {
        let ours = &self.catalogue;
        let same_antennas =
            ours.antennas.len() == catalogue.antennas.len()
                && ours.antennas.iter().zip(&catalogue.antennas).all(|(a, b)| {
                    a.station == b.station && a.position.values() == b.position.values()
                });
        let same_fields = ours.fields.len() == catalogue.fields.len()
            && ours.fields.iter().zip(&catalogue.fields).all(|(a, b)| {
                a.phase_centre.refer() == b.phase_centre.refer()
                    && a.phase_centre.as_angles() == b.phase_centre.as_angles()
            });
        same_antennas
            && same_fields
            && ours.observations.len() == catalogue.observations.len()
            && ours.partitions.len() == catalogue.partitions.len()
            && ours
                .partitions
                .iter()
                .zip(&catalogue.partitions)
                .all(|(a, b)| {
                    a.same_content(b) && a.observation == b.observation && a.fields == b.fields
                })
    }
}

/// A visibility partition's IDs for CASA calibration-table products and
/// solution boundaries. The store-local partition index is not an SPW ID.
#[cfg(unix)]
pub struct CalibrationPartition<'a> {
    /// The persisted `SPECTRAL_WINDOW` row.
    pub window: u32,
    /// The underlying polarization and SPW combination.
    pub data_description: u32,
    /// The partition's common MSv2 `ARRAY_ID`, or zero for MSv4. `None`
    /// means more than one array occurs in the partition.
    pub array: Option<i32>,
    /// Field and scan IDs by time for the base data group, when it has scans.
    /// The slices have the same length and represent only pairs in the grid.
    pub fields_and_scans: Option<(&'a [FieldId], &'a [ScanId])>,
}

/// The single observation opening used by a calibration solve and the
/// subtables it uses for CASA-format publication.
#[cfg(unix)]
pub struct CalibrationObservation<'a> {
    /// Store-neutral visibility traversal and catalogue.
    pub observation: &'a dyn Observation,
    /// The subtables that define the caltable's row numbers.
    pub subtables: &'a MeasurementSet,
    /// The legacy IDs used for solution grouping and publication, in
    /// catalogue partition order.
    pub partitions: &'a [CalibrationPartition<'a>],
    /// The observation budget remaining once its reader and subtables are held.
    pub available_bytes: usize,
}

/// Run a caltable consumer using one admitted observation opening. An MSv2
/// supplies its stored subtable row numbers. An MSv4 supplies the same
/// namespace an MSv2 conversion would persist.
/// `admit_msv2` may refuse an MSv2 from its retained table descriptions before
/// the observation indexes rows or reads subtables.
///
/// # Errors
/// When the observation is unreadable, the namespace cannot be represented
/// as CASA subtables, or the read would exceed the observation budget.
#[cfg(unix)]
pub fn with_calibration_observation<T>(
    source: &Path,
    admit_msv2: impl FnOnce(&MeasurementSet) -> Result<(), ObservationError>,
    query: impl FnOnce(CalibrationObservation<'_>) -> T,
) -> Result<T, ObservationError> {
    let budget = super::creation::default_memory_bytes()?;
    let mut grant = |required| {
        if required > budget {
            Err(ObservationError::OverBudget {
                what: "calibration observation opening and persisted namespace",
                required,
                budget,
            })
        } else {
            Ok(())
        }
    };
    if super::is_msv4_store(source) {
        #[cfg(feature = "msv4")]
        {
            let observation =
                super::Msv4Observation::open_granted(source, &OpenOptions::default(), &mut grant)?;
            let catalogue = observation.catalogue();
            let held = observation
                .held_bytes()
                .saturating_add(super::creation::subtables_bytes(catalogue))
                .saturating_add(calibration_partition_bytes(catalogue));
            let scan_bytes = casa_core::memory::btree_node_bytes::<i32, ()>(catalogue.scans.len());
            grant(held.saturating_add(scan_bytes))?;
            validate_calibration_scan_numbers(catalogue)?;
            let (subtables, indices) = super::creation::catalogue_subtables(&observation)?;
            let partitions = indices
                .into_iter()
                .enumerate()
                .map(|(index, (window, data_description))| CalibrationPartition {
                    window: window as u32,
                    data_description: data_description as u32,
                    array: Some(0),
                    fields_and_scans: observation
                        .calibration_fields_and_scans(PartitionId(index as u32)),
                })
                .collect::<Vec<_>>();
            return Ok(query(CalibrationObservation {
                observation: &observation,
                subtables: &subtables,
                partitions: &partitions,
                available_bytes: budget - held,
            }));
        }
        #[cfg(not(feature = "msv4"))]
        return Err(ObservationError::Unsupported {
            what: "an MSv4 store's namespace without casa-ms's msv4 feature",
        });
    }
    let described = Msv2Observation::opening_table_bytes(source);
    grant(described)?;
    let ms = MeasurementSet::open_retained_read(source).map_err(storage)?;
    admit_msv2(&ms)?;
    let catalogue_rows = super::msv2::held::read_bytes(&ms);
    grant(described.saturating_add(catalogue_rows.saturating_mul(2)))?;
    super::msv2::held::prefetch_catalogue_rows(&ms);
    let observation = Msv2Observation::open_granted_on(ms, &OpenOptions::default(), &mut grant)?;
    let held = observation
        .retained_bytes()
        .saturating_add(calibration_partition_bytes(observation.catalogue()));
    grant(held)?;
    let partitions = observation
        .catalogue()
        .partition_ids()
        .into_iter()
        .map(|partition| CalibrationPartition {
            window: observation.spectral_window_id(partition),
            data_description: observation.data_description_id(partition),
            array: observation.array_id(partition),
            fields_and_scans: Some(observation.calibration_fields_and_scans(partition)),
        })
        .collect::<Vec<_>>();
    Ok(query(CalibrationObservation {
        observation: &observation,
        subtables: observation.measurement_set(),
        partitions: &partitions,
        available_bytes: budget - held,
    }))
}

#[cfg(unix)]
fn calibration_partition_bytes(catalogue: &Catalogue) -> usize {
    catalogue.partitions.len() * (size_of::<CalibrationPartition<'_>>() + size_of::<PartitionId>())
}

#[cfg(all(unix, feature = "msv4"))]
fn validate_calibration_scan_numbers(catalogue: &Catalogue) -> Result<(), ObservationError> {
    let mut numbers = std::collections::BTreeSet::new();
    for scan in &catalogue.scans {
        let Some(number) = scan.number else {
            return Err(ObservationError::Unsupported {
                what: "MSv4 gaincal requires every scan_name to be a decimal scan number",
            });
        };
        if !numbers.insert(number) {
            return Err(ObservationError::Unsupported {
                what: "MSv4 gaincal cannot bind distinct scan_name values to the same CASA scan number",
            });
        }
    }
    Ok(())
}

/// Open the subtables for a calibration that uses metadata alone.
/// MSv2 retains its stored row numbers and columns. MSv4 supplies the
/// CASA namespace its MSv2 conversion would persist. Neither route reads
/// visibility values or requires the scan names to fit CASA scan numbers.
///
/// # Errors
///
/// When the store cannot be read, its namespace cannot be represented in
/// CASA subtables, or opening an MSv4 catalogue exceeds its memory grant.
pub fn calibration_subtables(source: &Path) -> Result<MeasurementSet, ObservationError> {
    if !super::is_msv4_store(source) {
        return MeasurementSet::open(source).map_err(storage);
    }
    #[cfg(all(feature = "msv4", unix))]
    {
        let budget = super::creation::default_memory_bytes()?;
        let mut grant = |required| {
            if required > budget {
                Err(ObservationError::OverBudget {
                    what: "calibration subtables and their observation catalogue",
                    required,
                    budget,
                })
            } else {
                Ok(())
            }
        };
        let observation =
            super::Msv4Observation::open_granted(source, &OpenOptions::default(), &mut grant)?;
        grant(
            observation
                .held_bytes()
                .saturating_add(super::creation::subtables_bytes(observation.catalogue())),
        )?;
        super::creation::catalogue_subtables(&observation).map(|(ms, _)| ms)
    }
    #[cfg(not(all(feature = "msv4", unix)))]
    Err(ObservationError::Unsupported {
        what: "an MSv4 calibration namespace without casa-ms's msv4 feature",
    })
}

/// The namespace of the store at `source` as [`super::open`] opens it with
/// `options`, which decide its partitions: an MSv2's own subtables, its
/// catalogue's antennas, fields and sessions being their rows and each
/// partition naming its data description's spectral window; or for an
/// MSv4 the subtables its MSv2 conversion would hold, in catalogue order.
/// The store is opened again; [`PersistedNamespace::describes`] tells
/// whether another opening numbers it alike.
///
/// # Errors
///
/// When the store cannot be opened, its subtables cannot be read as
/// content (a position in other than ITRF, a measure column with a variable
/// reference), or an MSv4's catalogue holds what an MSv2's subtables cannot
/// say; an MSv4 store without casa-ms's `msv4` feature.
pub fn persisted_namespace(
    source: &Path,
    options: &OpenOptions,
) -> Result<PersistedNamespace, ObservationError> {
    let (subtables, spectral_windows, catalogue) = if super::is_msv4_store(source) {
        msv4_subtables(source, options)?
    } else {
        let observation = Msv2Observation::open_with(source, options)?;
        let catalogue = Arc::clone(observation.catalogue());
        let windows = catalogue
            .partition_ids()
            .into_iter()
            .map(|id| observation.spectral_window_id(id))
            .collect();
        (observation.into_measurement_set(), windows, catalogue)
    };
    let rows = NamespaceRows::of(&subtables)?;
    let identity = |count: usize| (0..count as u32).collect::<Vec<_>>();
    Ok(PersistedNamespace {
        rows,
        antennas: identity(catalogue.antennas.len()),
        fields: identity(catalogue.fields.len()),
        spectral_windows,
        observations: identity(catalogue.observations.len()),
        catalogue,
        source: source.to_path_buf(),
        subtables,
    })
}

/// An MSv4's namespace: the subtables its MSv2 conversion would hold, each
/// partition's window row, and the catalogue.
#[cfg(all(feature = "msv4", unix))]
fn msv4_subtables(
    source: &Path,
    options: &OpenOptions,
) -> Result<(MeasurementSet, Vec<u32>, Arc<Catalogue>), ObservationError> {
    let observation = super::Msv4Observation::open_with(source, options)?;
    let (subtables, indices) = super::creation::catalogue_subtables(&observation)?;
    let windows = indices
        .into_iter()
        .map(|(window, _)| window as u32)
        .collect();
    Ok((subtables, windows, Arc::clone(observation.catalogue())))
}

#[cfg(not(all(feature = "msv4", unix)))]
fn msv4_subtables(
    _source: &Path,
    _options: &OpenOptions,
) -> Result<(MeasurementSet, Vec<u32>, Arc<Catalogue>), ObservationError> {
    Err(ObservationError::Unsupported {
        what: "an MSv4 store's namespace without casa-ms's msv4 feature",
    })
}

/// The antenna namespace of one store.
pub enum AntennaNamespace {
    /// Casacore ANTENNA rows in MSv2 order, with the original position
    /// values for selection in the table's own numbering.
    Msv2(Vec<StoredAntennaRow>),
    /// Antennas in MSv4 catalogue order, with positions in geocentric
    /// ITRF metres for matching against a persisted namespace.
    Msv4(Vec<NamespaceAntenna>),
}

/// Read the antenna part of a MeasurementSet's namespace. MSv2 uses the
/// ANTENNA subtable's row order and MSv4 uses catalogue order. Neither
/// opening reads or indexes visibility samples.
///
/// # Errors
/// When the store or its antenna metadata is unreadable or outside the
/// supported profile, or MSv4 support is disabled.
pub fn antenna_namespace(source: &Path) -> Result<AntennaNamespace, ObservationError> {
    if super::is_msv4_store(source) {
        #[cfg(feature = "msv4")]
        {
            let antennas = super::msv4::antenna_catalogue(source)?
                .into_iter()
                .map(|antenna| NamespaceAntenna {
                    name: antenna.name,
                    station: antenna.station,
                    position_m: antenna.position.as_itrf(),
                })
                .collect();
            return Ok(AntennaNamespace::Msv4(antennas));
        }
        #[cfg(not(feature = "msv4"))]
        return Err(ObservationError::Unsupported {
            what: "an MSv4 antenna namespace without casa-ms's msv4 feature",
        });
    }
    MeasurementSet::open(source)
        .and_then(|ms| ms.stored_antenna_rows())
        .map(AntennaNamespace::Msv2)
        .map_err(storage)
}

impl NamespaceRows {
    /// Read a persisted calibration table's antenna names and positions
    /// in its own row order, without reading the other subtables.
    ///
    /// # Errors
    /// When its ANTENNA subtable cannot be read or cannot represent its
    /// positions in geocentric ITRF metres.
    pub fn read_antennas(directory: &Path) -> Result<Vec<NamespaceAntenna>, ObservationError> {
        let table = Table::open(TableOptions::new(directory.join("ANTENNA")))
            .map_err(|error| storage(MsError::from(error)))?;
        antennas(&table)
    }

    /// The rows of the `ANTENNA`, `FIELD` and `SPECTRAL_WINDOW` subtables
    /// of the table at `directory`: a calibration table, or a
    /// MeasurementSet.
    ///
    /// # Errors
    ///
    /// When a subtable is missing or unreadable, or holds what the content
    /// cannot state (a position in other than ITRF, a variable reference).
    pub fn read(directory: &Path) -> Result<Self, ObservationError> {
        let open = |name: &str| {
            Table::open(TableOptions::new(directory.join(name)))
                .map_err(|error| storage(MsError::from(error)))
        };
        Self::of_tables(
            &open("ANTENNA")?,
            &open("FIELD")?,
            &open("SPECTRAL_WINDOW")?,
        )
    }

    fn of(ms: &MeasurementSet) -> Result<Self, ObservationError> {
        let subtable = |id: SubtableId| {
            ms.subtable(id)
                .ok_or_else(|| ObservationError::InvalidStore {
                    reason: format!("no {id:?} subtable"),
                })
        };
        Self::of_tables(
            subtable(SubtableId::Antenna)?,
            subtable(SubtableId::Field)?,
            subtable(SubtableId::SpectralWindow)?,
        )
    }

    fn of_tables(antenna: &Table, field: &Table, window: &Table) -> Result<Self, ObservationError> {
        Ok(Self {
            antennas: antennas(antenna)?,
            fields: fields(field)?,
            spectral_windows: windows(window)?,
        })
    }
}

fn invalid(reason: impl Into<String>) -> ObservationError {
    ObservationError::InvalidStore {
        reason: reason.into(),
    }
}

fn string(table: &Table, row: usize, column: &str) -> Result<String, ObservationError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.scalar().cloned())
        .map_err(|error| storage(MsError::from(error)))?
    {
        ScalarValue::String(value) => Ok(value),
        _ => Err(invalid(format!("{column} is not a string"))),
    }
}

fn floats(table: &Table, row: usize, column: &str) -> Result<Vec<f64>, ObservationError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|error| storage(MsError::from(error)))?
    {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        _ => Err(invalid(format!("{column} is not a Float64 array"))),
    }
}

fn antennas(table: &Table) -> Result<Vec<NamespaceAntenna>, ObservationError> {
    require_units(table, "POSITION", "m")?;
    match measure_reference(table, "POSITION")?.as_deref() {
        None | Some("ITRF") => {}
        Some(_) => {
            return Err(ObservationError::Unsupported {
                what: "antenna positions in other than ITRF",
            });
        }
    }
    (0..table.row_count())
        .map(|row| {
            let position = floats(table, row, "POSITION")?;
            let position_m = <[f64; 3]>::try_from(position.as_slice())
                .map_err(|_| invalid("an ANTENNA POSITION is not three values"))?;
            Ok(NamespaceAntenna {
                name: string(table, row, "NAME")?,
                station: string(table, row, "STATION")?,
                position_m,
            })
        })
        .collect()
}

fn fields(table: &Table) -> Result<Vec<NamespaceField>, ObservationError> {
    let frames = phase_frames(table)?;
    let phase_dir = DirectionColumn::phase_dir(table);
    (0..table.row_count())
        .map(|row| {
            let (longitude, latitude) = phase_dir.get_direction(row).map_err(storage)?.as_angles();
            Ok(NamespaceField {
                name: string(table, row, "NAME")?,
                phase_centre: MDirection::from_angles(longitude, latitude, frames[row]),
            })
        })
        .collect()
}

fn windows(table: &Table) -> Result<Vec<NamespaceWindow>, ObservationError> {
    require_units(table, "CHAN_FREQ", "Hz")?;
    require_units(table, "CHAN_WIDTH", "Hz")?;
    let frequency_refs = crate::columns::frequency_columns::ChanFreqColumn::new(table);
    (0..table.row_count())
        .map(|row| {
            Ok(NamespaceWindow {
                name: string(table, row, "NAME")?,
                frame: frequency_refs.frequency_reference(row).map_err(storage)?,
                frequencies_hz: floats(table, row, "CHAN_FREQ")?,
                widths_hz: floats(table, row, "CHAN_WIDTH")?,
            })
        })
        .collect()
}
