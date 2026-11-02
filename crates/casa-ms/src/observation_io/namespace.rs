// SPDX-License-Identifier: LGPL-3.0-or-later
//! The namespace a calibration table persists (`docs/measurement-set/
//! contracts.md` section 2, `CalibrationBinding`).
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

use casa_observation::{Catalogue, ObservationError, PartitionId};
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ArrayValue, ScalarValue};

use super::msv2::catalogue::{measure_reference, phase_frames, require_units};
use super::update::storage;
use super::{Msv2Observation, OpenOptions};
use crate::columns::direction_columns::DirectionColumn;
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
    let (subtables, spectral_windows, catalogue) = if source.join("zarr.json").is_file() {
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
    let (subtables, windows) = super::creation::catalogue_subtables(&observation)?;
    let windows = windows.into_iter().map(|row| row as u32).collect();
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

impl NamespaceRows {
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
    (0..table.row_count())
        .map(|row| {
            let code = match table
                .cell_accessor(row, "MEAS_FREQ_REF")
                .and_then(|cell| cell.scalar().cloned())
                .map_err(|error| storage(MsError::from(error)))?
            {
                ScalarValue::Int32(code) => code,
                _ => return Err(invalid("MEAS_FREQ_REF is not an Int32")),
            };
            Ok(NamespaceWindow {
                name: string(table, row, "NAME")?,
                frame: FrequencyRef::from_casacore_code(code)
                    .ok_or_else(|| invalid(format!("unknown MEAS_FREQ_REF {code}")))?,
                frequencies_hz: floats(table, row, "CHAN_FREQ")?,
                widths_hz: floats(table, row, "CHAN_WIDTH")?,
            })
        })
        .collect()
}
