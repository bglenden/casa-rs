// SPDX-License-Identifier: LGPL-3.0-or-later
//! High-level MeasurementSet type that manages main table + subtables as a unit.
//!
//! A [`MeasurementSet`] bundles the MS main table with its required (and
//! optional) subtables. It can be created from scratch via
//! [`MeasurementSet::create`], opened from disk via [`MeasurementSet::open`],
//! or assembled in memory for testing.
//!
//! Cf. C++ `MeasurementSet` class.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_measures::table_measures::{MeasureType, TableMeasDesc, default_frequency_ref_map};
use casa_tables::{
    ColumnBinding, ColumnOverrides, DataManagerKind, Table, TableError, TableInfo, TableOptions,
    TableWriteLock,
};
#[cfg(unix)]
use casa_tables::{LockMode, LockOptions};

use crate::builder::{MeasurementSetBuilder, MsSchemas};
use crate::column_def::ColumnDef;
use crate::error::{MsError, MsResult};
use crate::metadata::{measinfo_for, quantum_units_for};
use crate::schema::{SubtableId, main_table::VisibilityDataColumn};
use crate::subtable_overrides::SubtableColumnOverrides;
use crate::subtables::{
    MsAntenna, MsAntennaMut, MsDataDescription, MsDataDescriptionMut, MsDoppler, MsDopplerMut,
    MsFeed, MsFeedMut, MsField, MsFieldMut, MsFlagCmd, MsFlagCmdMut, MsFreqOffset, MsFreqOffsetMut,
    MsHistory, MsHistoryMut, MsObservation, MsObservationMut, MsPointing, MsPointingMut,
    MsPolarization, MsPolarizationMut, MsProcessor, MsProcessorMut, MsSource, MsSourceMut,
    MsSpectralWindow, MsSpectralWindowMut, MsState, MsStateMut, MsSysCal, MsSysCalMut, MsWeather,
    MsWeatherMut,
};
use crate::validate::{self, ValidationIssue};

pub mod linked_subtables;
mod memory;
mod optional_subtables;

mod main_storage;

pub use main_storage::casa_interferometer_count;
pub use main_storage::casa_uvw_tile_shape;
pub use main_storage::casa_visibility_tile_shape;
pub use main_storage::casa_weight_tile_shape;
pub(crate) use main_storage::measurement_set_main_data_manager_sequence;
pub use main_storage::measurement_set_main_table_bindings;
use main_storage::*;

/// The MS version number written by this crate.
pub const MS_VERSION: f32 = 2.0;
pub(crate) const CASACORE_MS_TABLE_TYPE: &str = "Measurement Set";

/// A MeasurementSet: main table + subtables.
///
/// This is the top-level type for working with MS data. It owns the main
/// table and all attached subtables (12 required + 5 optional).
///
/// When saved to disk, subtable links are emitted as true casacore `TpTable`
/// keywords, using the same relative-path encoding that C++ `MeasurementSet`
/// writes for sibling subtables.
///
/// # Example
///
/// ```rust
/// use casa_ms::MeasurementSet;
/// use casa_ms::MeasurementSetBuilder;
///
/// // Create an in-memory MS
/// let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new())
///     .expect("create MS");
/// assert_eq!(ms.row_count(), 0);
/// assert_eq!(ms.subtable_ids().len(), 12);
/// ```
pub struct MeasurementSet {
    main: Table,
    subtables: HashMap<SubtableId, Table>,
    subtable_paths: HashMap<SubtableId, PathBuf>,
    detached_keyword_tables: std::collections::BTreeMap<PathBuf, Table>,
    path: Option<PathBuf>,
}

impl MeasurementSet {
    /// Prepare an in-memory MeasurementSet that writes into a caller-owned private stage.
    #[doc(hidden)]
    pub fn create_staged(path: &Path, builder: MeasurementSetBuilder) -> MsResult<Self> {
        Self::from_schemas(builder.build_schemas()?, Some(path.to_owned()))
    }

    /// Create an MS on disk at the given path.
    ///
    /// Creates the main table directory and subtable subdirectories.
    pub fn create(path: impl AsRef<Path>, builder: MeasurementSetBuilder) -> MsResult<Self> {
        let path = path.as_ref().to_path_buf();
        let target = crate::write_session::MeasurementSetCreateTarget::prepare(&path, false)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        let schemas = builder.build_schemas()?;
        let mut ms = Self::from_schemas(schemas, Some(target.staging_path().to_path_buf()))?;
        ms.save()?;
        target
            .commit()
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        ms.path = Some(path.clone());
        ms.rebase_subtable_paths(&path);
        Ok(ms)
    }

    /// Open an existing MS from disk.
    ///
    /// Opens the main table and follows subtable keywords to open subtables.
    /// casacore-style `TpTable` keywords are preferred, but legacy string
    /// keywords written by older versions of this crate are also accepted.
    pub fn open(path: impl AsRef<Path>) -> MsResult<Self> {
        let path = path.as_ref().to_path_buf();
        Self::open_tables(path, None, |table_path| {
            Table::open(TableOptions::new(table_path))
        })
    }

    /// Open MAIN with an explicit set of resolved subtable paths. This is
    /// used by read-only views whose subtables are themselves concatenations.
    /// The supplied paths describe all the subtables held by the view.
    ///
    /// # Errors
    /// MAIN or a supplied subtable cannot be opened.
    pub fn open_with_subtable_paths(
        path: impl AsRef<Path>,
        subtables: &HashMap<SubtableId, PathBuf>,
    ) -> MsResult<Self> {
        Self::open_tables(path.as_ref().to_path_buf(), Some(subtables), |path| {
            Table::open(TableOptions::new(path))
        })
    }

    /// Open the requested subtables of an already opened MAIN table.
    pub fn with_open_main(
        main: Table,
        path: PathBuf,
        subtables: &HashMap<SubtableId, PathBuf>,
    ) -> MsResult<Self> {
        Self::open_subtables(main, path, Some(subtables), |path| {
            Table::open(TableOptions::new(path))
        })
    }

    /// Open only the descriptors used by the metadata phase of concatenation.
    pub fn open_concatenation_metadata(
        main: Table,
        links: Vec<(SubtableId, PathBuf)>,
        donor: bool,
    ) -> MsResult<Self> {
        use rayon::prelude::*;

        let path = main
            .path()
            .expect("captured MeasurementSet root")
            .to_path_buf();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let tables = links
            .into_par_iter()
            .map(|(id, path)| {
                tracing::dispatcher::with_default(&dispatch, || {
                    let table = if donor && id != SubtableId::History {
                        held_subtable_reader(&path)?
                    } else {
                        Table::open(TableOptions::new(&path))?
                    };
                    if !matches!(
                        id,
                        SubtableId::History | SubtableId::SysCal | SubtableId::Weather
                    ) {
                        table.rows()?;
                    }
                    Ok::<_, MsError>((id, path, table))
                })
            })
            .collect::<MsResult<Vec<_>>>()?;
        let mut subtables = HashMap::new();
        let mut subtable_paths = HashMap::new();
        for (id, path, table) in tables {
            subtables.insert(id, table);
            subtable_paths.insert(id, path);
        }
        Ok(Self {
            main,
            subtables,
            subtable_paths,
            detached_keyword_tables: Default::default(),
            path: Some(path),
        })
    }

    /// Open and retain read locks on MAIN and every present subtable.
    ///
    /// Tables are opened in canonical MAIN-then-subtable order. Row payloads
    /// remain lazy, so callers can combine this retained consistency capability
    /// with bounded selected-column reads. Dropping the returned MeasurementSet
    /// releases the locks. An observation opened on it
    /// ([`Msv2Observation::open_granted_on`]) keeps them.
    ///
    /// # Errors
    ///
    /// When MAIN or a present subtable cannot be opened or locked.
    ///
    /// [`Msv2Observation::open_granted_on`]: crate::observation_io::Msv2Observation::open_granted_on
    #[cfg(unix)]
    pub fn open_retained_read(path: impl AsRef<Path>) -> MsResult<Self> {
        let path = path.as_ref().to_path_buf();
        let lock_options = LockOptions::new(LockMode::AutoLocking);
        Self::open_tables(path, None, |table_path| {
            Table::open_with_lock(TableOptions::new(table_path), lock_options.clone())
        })
    }

    fn open_tables(
        path: PathBuf,
        linked: Option<&HashMap<SubtableId, PathBuf>>,
        mut open_table: impl FnMut(&Path) -> Result<Table, TableError>,
    ) -> MsResult<Self> {
        tracing::info!(path = %path.display(), "opening MeasurementSet");
        let main = open_table(&path)?;
        Self::open_subtables(main, path, linked, open_table)
    }

    fn open_subtables(
        main: Table,
        path: PathBuf,
        linked: Option<&HashMap<SubtableId, PathBuf>>,
        mut open_table: impl FnMut(&Path) -> Result<Table, TableError>,
    ) -> MsResult<Self> {
        let mut subtables = HashMap::new();
        let mut subtable_paths = HashMap::new();

        // Try to open each known subtable
        let all_ids: Vec<SubtableId> = SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL.iter())
            .copied()
            .collect();

        for id in all_ids {
            let subtable_path = match linked {
                Some(links) => links.get(&id).cloned(),
                None => subtable_path_from_main(&main, &path, id),
            };
            if let Some(subtable_path) = subtable_path {
                let table = open_table(&subtable_path)?;
                subtables.insert(id, table);
                subtable_paths.insert(id, subtable_path);
            }
        }

        let ms = Self {
            main,
            subtables,
            subtable_paths,
            detached_keyword_tables: Default::default(),
            path: Some(path),
        };
        tracing::info!(
            path = %ms.path.as_ref().expect("path set").display(),
            rows = ms.row_count(),
            subtables = ms.subtable_ids().len(),
            "opened MeasurementSet"
        );
        Ok(ms)
    }

    /// Save the MS to its path (must have been created with a path or opened from disk).
    ///
    /// The save step refreshes the main-table metadata and rewrites subtable
    /// keyword payloads using casacore's relative `././SUBTABLE` form.
    ///
    /// Every table rewritten in place is written under casacore's table write
    /// lock, taken before anything is written and released after the save;
    /// see [`TableWriteLock`]. A lock another process holds is waited for,
    /// as casacore waits, but never while this save holds the lock of
    /// another table of the MeasurementSet: the save releases those, waits
    /// for the held one alone and then takes the others again, so it cannot
    /// deadlock with a process that holds one table while it waits for
    /// another.
    ///
    /// # Errors
    ///
    /// [`MsError::Table`] with [`TableError::LockFailed`] when another handle
    /// in this process holds the write lock on MAIN or a subtable, when
    /// another process wrote such a table while the save waited for it or
    /// for another table, or when the wait would deadlock. A refused save
    /// writes nothing.
    pub fn save(&mut self) -> MsResult<()> {
        self.save_using(save_main_table_with_policy)
    }

    fn save_using(
        &mut self,
        save_main: impl FnOnce(&mut Table, &Path, Option<&TableWriteLock>) -> MsResult<()>,
    ) -> MsResult<()> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| MsError::VersionError("MS has no path; use save_as()".to_string()))?
            .clone();
        tracing::info!(
            path = %path.display(),
            rows = self.row_count(),
            subtables = self.subtable_ids().len(),
            "saving MeasurementSet"
        );

        self.refresh_subtable_paths(&path);
        self.sync_main_metadata(&path);
        let locks = self.lock_tables_for_in_place_save(&path, true)?;
        save_main(&mut self.main, &path, locks.for_table(None))?;

        for id in self.subtables.keys().copied().collect::<Vec<_>>() {
            let subtable_path = self.subtable_path(&path, id);
            self.subtables
                .get_mut(&id)
                .expect("known subtable")
                .prepare_write()
                .using_write_lock(locks.for_table(Some(id)))
                .save(measurement_set_table_options(&subtable_path))?;
        }
        locks.release()?;

        tracing::info!(path = %path.display(), rows = self.row_count(), "saved MeasurementSet");
        Ok(())
    }

    fn subtable_path(&self, path: &Path, id: SubtableId) -> PathBuf {
        self.subtable_paths
            .get(&id)
            .cloned()
            .unwrap_or_else(|| path.join(id.name()))
    }

    /// Take casacore's write lock on MAIN and, with `subtables`, on every
    /// subtable, in canonical order, for a save that rewrites them in place.
    ///
    /// A table whose directory holds no `table.dat` yet is being created, not
    /// changed in place, and is not locked. No write is recorded here: the
    /// save records each table just before it writes it (see
    /// [`InPlaceSaveLocks`]).
    ///
    /// A lock another process holds is waited for, but never while this save
    /// holds another table's lock: a process waiting for a lock the save holds
    /// may hold the one the save waits for. The kernel would refuse that wait
    /// as a deadlock and fail the save; releasing first lets the save complete
    /// once the other process is done. On the first table that is held, every
    /// lock taken so far is released, unrecorded, the save waits for that
    /// table alone and keeps it, and then tries the rest again. A released
    /// table's modify counter is read before its lock is released and
    /// compared once every lock is held again: if another process wrote the
    /// table meanwhile, the save would overwrite that write with its stale
    /// in-memory table, so it is refused before anything is written.
    fn lock_tables_for_in_place_save(
        &self,
        path: &Path,
        subtables: bool,
    ) -> MsResult<InPlaceSaveLocks> {
        let mut tables = vec![(None, path.to_path_buf())];
        if subtables {
            tables.extend(
                SubtableId::ALL_REQUIRED
                    .iter()
                    .chain(SubtableId::ALL_OPTIONAL.iter())
                    .filter(|id| self.subtables.contains_key(id))
                    .map(|id| (Some(*id), self.subtable_path(path, *id))),
            );
        }
        tables.retain(|(_, table_path)| table_path.join("table.dat").is_file());
        let mut held: Vec<Option<TableWriteLock>> = tables.iter().map(|_| None).collect();
        // The modify counter of each table whose lock was released to wait
        // for another, read while it was still held.
        let mut released_at: Vec<Option<Option<u32>>> = tables.iter().map(|_| None).collect();
        'acquire: loop {
            for (index, (_, table_path)) in tables.iter().enumerate() {
                if held[index].is_some() {
                    continue;
                }
                match TableWriteLock::acquire(table_path, 1) {
                    Ok(lock) => held[index] = Some(lock),
                    Err(TableError::LockFailed { .. }) => {
                        // Release before waiting; a holder in this process is
                        // refused again at once by the waiting request.
                        for (lock, baseline) in held.iter_mut().zip(released_at.iter_mut()) {
                            if let Some(lock) = lock.take()
                                && baseline.is_none()
                            {
                                *baseline = Some(lock.modify_counter()?);
                            }
                        }
                        held[index] = Some(TableWriteLock::acquire(table_path, 0)?);
                        continue 'acquire;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            break;
        }
        // A table released while the save waited for another may have been
        // written by another process meanwhile; the in-memory tables would
        // then overwrite that write, so the save is refused before anything
        // is written.
        for (index, (_, table_path)) in tables.iter().enumerate() {
            let Some(baseline) = released_at[index] else {
                continue;
            };
            let lock = held[index].as_ref().expect("every table is locked");
            if lock.modify_counter()? != baseline {
                return Err(TableError::LockFailed {
                    path: table_path.display().to_string(),
                    message: "another process wrote this table while the save waited for \
                              another table of the MeasurementSet; reopen the MeasurementSet \
                              and retry"
                        .to_string(),
                }
                .into());
            }
        }
        Ok(InPlaceSaveLocks {
            locks: tables
                .into_iter()
                .zip(held)
                .map(|((id, _), lock)| (id, lock.expect("every table is locked")))
                .collect(),
        })
    }

    /// Save MAIN and every subtable as [`save`](Self::save) does, writing
    /// the columns `main` and `subtables` override from the overrides
    /// instead of from rows held in memory.
    ///
    /// The overrides of a table follow
    /// [`TableWritePlan::save_with_column_overrides`](casa_tables::TableWritePlan::save_with_column_overrides):
    /// they may set its row count and give columns as materialized values,
    /// generated cells or, for MAIN, deferred columns another path installs.
    /// A subtable is written with the same storage managers as
    /// [`save`](Self::save) writes it, so overriding every column of a
    /// subtable with its cells writes the same files as holding them as
    /// rows. The in-memory tables are not changed: a subtable saved from
    /// generated cells keeps the rows it held.
    ///
    /// # Errors
    ///
    /// [`MsError::MissingSubtable`] when `subtables` names a subtable this
    /// MeasurementSet does not have, before anything is written; otherwise
    /// as [`save`](Self::save), and when an override does not fit its table.
    pub fn save_with_column_overrides(
        &mut self,
        main: &ColumnOverrides,
        subtables: &SubtableColumnOverrides,
    ) -> MsResult<()> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| MsError::VersionError("MS has no path; use save_as()".to_string()))?
            .clone();
        if let Some((missing, _)) = subtables
            .iter()
            .find(|(id, _)| !self.subtables.contains_key(id))
        {
            return Err(MsError::MissingSubtable(missing.name().to_string()));
        }

        self.refresh_subtable_paths(&path);
        self.sync_main_metadata(&path);
        let locks = self.lock_tables_for_in_place_save(&path, true)?;
        save_main_table_with_policy_and_column_overrides(
            &mut self.main,
            &path,
            main,
            locks.for_table(None),
        )?;

        let subtable_paths = self
            .subtables
            .keys()
            .map(|id| (*id, self.subtable_path(&path, *id)))
            .collect::<HashMap<_, _>>();
        for (id, table) in &mut self.subtables {
            let options = measurement_set_table_options(&subtable_paths[id]);
            let mut plan = table
                .prepare_write()
                .using_write_lock(locks.for_table(Some(*id)));
            match subtables.get(*id) {
                Some(overrides) => {
                    plan.save_with_column_overrides(options, &HashMap::new(), overrides)?
                }
                None => plan.save(options)?,
            }
        }
        locks.release()?;

        Ok(())
    }

    /// Save only the MS main table back to disk.
    ///
    /// This is intended for workflows that mutate only MAIN columns/keywords
    /// while leaving all subtables untouched, such as the initial
    /// `applycal`-class calibration executor. The method still refreshes
    /// main-table metadata and subtable-link keywords before writing the
    /// main table, but it skips rewriting the individual subtable directories.
    ///
    /// MAIN is written under casacore's table write lock, as in
    /// [`save`](Self::save).
    pub fn save_main_table_only(&mut self) -> MsResult<()> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| MsError::VersionError("MS has no path; use save_as()".to_string()))?
            .clone();

        self.refresh_subtable_paths(&path);
        self.sync_main_metadata(&path);
        let locks = self.lock_tables_for_in_place_save(&path, false)?;
        save_main_table_with_policy(&mut self.main, &path, locks.for_table(None))?;
        locks.release()?;
        Ok(())
    }

    /// Save the MS to a new path.
    ///
    /// All persisted subtable references are rebased so the copied MS remains
    /// self-consistent when reopened by Rust or C++ casacore tools.
    pub fn save_as(&mut self, path: impl AsRef<Path>) -> MsResult<()> {
        self.save_as_using(path.as_ref(), save_main_table_with_policy)
    }

    /// Save to a new path with StandardStMan for the MAIN columns.
    ///
    /// This is the casacore single-dish filler's layout. It packs irregular
    /// spectral shapes without padding each integration group to a tile and
    /// preserves explicitly defined empty flag-category cells.
    ///
    /// # Errors
    /// Returns the same destination and storage errors as [`Self::save_as`].
    pub fn save_as_standard(&mut self, path: impl AsRef<Path>) -> MsResult<()> {
        self.save_as_using(path.as_ref(), |main, path, lock| {
            main.prepare_write()
                .using_write_lock(lock)
                .save(measurement_set_table_options(path))?;
            Ok(())
        })
    }

    fn save_as_using(
        &mut self,
        path: &Path,
        save_main: impl FnOnce(&mut Table, &Path, Option<&TableWriteLock>) -> MsResult<()>,
    ) -> MsResult<()> {
        let path = path.to_path_buf();
        let target = crate::write_session::MeasurementSetCreateTarget::prepare(&path, true)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        self.path = Some(target.staging_path().to_path_buf());
        self.rebase_subtable_paths(target.staging_path());
        self.save_using(save_main)?;
        target
            .commit()
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        self.path = Some(path.clone());
        self.rebase_subtable_paths(&path);
        Ok(())
    }

    /// Validate the MS structure and required column metadata.
    ///
    /// This mirrors the casacore `MSTableImpl::validate` checks for required
    /// columns, `QuantumUnits`, and `MEASINFO.type` on the standard MS schema.
    pub fn validate(&self) -> MsResult<Vec<ValidationIssue>> {
        validate::validate_ms(&self.main, &self.subtables)
    }

    /// Check the MAIN identities verified by CASA when opening with `check=True`.
    pub fn check_referential_integrity(&self) -> MsResult<()> {
        validate::check_referential_integrity(self)
    }

    /// Build a structured MeasurementSet summary for this MeasurementSet.
    ///
    /// The returned summary is richer than CASA's returned `listobs` Python
    /// dictionary and is intended to drive both human-readable text output and
    /// machine-readable JSON without reparsing formatted text.
    pub fn summary(&self) -> MsResult<crate::MeasurementSetSummary> {
        crate::MeasurementSetSummary::from_ms(self)
    }

    /// Build a structured MeasurementSet summary using explicit task-style
    /// options.
    pub fn summary_with_options(
        &self,
        options: &crate::MeasurementSetSummaryOptions,
    ) -> MsResult<crate::MeasurementSetSummary> {
        crate::MeasurementSetSummary::from_ms_with_options(self, options)
    }

    /// Build lazy-load UV coverage for the current MeasurementSet selection.
    pub fn uv_coverage(&self) -> MsResult<crate::MeasurementSetUvCoverage> {
        crate::MeasurementSetUvCoverage::from_ms(self)
    }

    /// Build lazy-load UV coverage using explicit task-style options.
    pub fn uv_coverage_with_options(
        &self,
        options: &crate::MeasurementSetSummaryOptions,
    ) -> MsResult<crate::MeasurementSetUvCoverage> {
        crate::MeasurementSetUvCoverage::from_ms_with_options(self, options)
    }

    /// Number of rows in the main table.
    pub fn row_count(&self) -> usize {
        self.main.row_count()
    }

    /// MS version from the MS_VERSION keyword, or `None` if missing.
    pub fn ms_version(&self) -> Option<f64> {
        match self.main.keywords().get("MS_VERSION") {
            Some(Value::Scalar(ScalarValue::Float32(v))) => Some(*v as f64),
            Some(Value::Scalar(ScalarValue::Float64(v))) => Some(*v),
            _ => None,
        }
    }

    /// Reference to the main table.
    pub fn main_table(&self) -> &Table {
        &self.main
    }

    /// Mutable reference to the main table.
    pub fn main_table_mut(&mut self) -> &mut Table {
        &mut self.main
    }

    /// Reference to a subtable, or `None` if not present.
    pub fn subtable(&self, id: SubtableId) -> Option<&Table> {
        self.subtables.get(&id)
    }

    /// Mutable reference to a subtable, or `None` if not present.
    pub fn subtable_mut(&mut self, id: SubtableId) -> Option<&mut Table> {
        self.subtables.get_mut(&id)
    }

    /// All subtable IDs present in this MS.
    pub fn subtable_ids(&self) -> Vec<SubtableId> {
        self.subtables.keys().copied().collect()
    }

    /// The bytes retained by lazy cell payloads, or no estimate if a table
    /// has materialized all of its rows.
    #[doc(hidden)]
    pub fn retained_cell_cache_bytes(&self) -> Option<usize> {
        self.subtables
            .values()
            .chain(std::iter::once(&self.main))
            .try_fold(0usize, |bytes, table| {
                bytes.checked_add(table.retained_cell_cache_bytes()?)
            })
    }

    /// The filesystem path, if any.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    // ---- Typed subtable accessors ----

    /// Typed read-only accessor for the ANTENNA subtable.
    pub fn antenna(&self) -> MsResult<MsAntenna<'_>> {
        let table = self
            .subtable(SubtableId::Antenna)
            .ok_or_else(|| MsError::MissingSubtable("ANTENNA".to_string()))?;
        Ok(MsAntenna::new(table))
    }

    /// Typed mutable accessor for the ANTENNA subtable.
    pub fn antenna_mut(&mut self) -> MsResult<MsAntennaMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Antenna)
            .ok_or_else(|| MsError::MissingSubtable("ANTENNA".to_string()))?;
        Ok(MsAntennaMut::new(table))
    }

    /// Typed read-only accessor for the FIELD subtable.
    pub fn field(&self) -> MsResult<MsField<'_>> {
        let table = self
            .subtable(SubtableId::Field)
            .ok_or_else(|| MsError::MissingSubtable("FIELD".to_string()))?;
        Ok(MsField::new(table))
    }

    /// Typed read-only accessor for the SPECTRAL_WINDOW subtable.
    pub fn spectral_window(&self) -> MsResult<MsSpectralWindow<'_>> {
        let table = self
            .subtable(SubtableId::SpectralWindow)
            .ok_or_else(|| MsError::MissingSubtable("SPECTRAL_WINDOW".to_string()))?;
        Ok(MsSpectralWindow::new(table))
    }

    /// Typed read-only accessor for the POLARIZATION subtable.
    pub fn polarization(&self) -> MsResult<MsPolarization<'_>> {
        let table = self
            .subtable(SubtableId::Polarization)
            .ok_or_else(|| MsError::MissingSubtable("POLARIZATION".to_string()))?;
        Ok(MsPolarization::new(table))
    }

    /// Typed read-only accessor for the DATA_DESCRIPTION subtable.
    pub fn data_description(&self) -> MsResult<MsDataDescription<'_>> {
        let table = self
            .subtable(SubtableId::DataDescription)
            .ok_or_else(|| MsError::MissingSubtable("DATA_DESCRIPTION".to_string()))?;
        Ok(MsDataDescription::new(table))
    }

    /// Typed mutable accessor for the DATA_DESCRIPTION subtable.
    pub fn data_description_mut(&mut self) -> MsResult<MsDataDescriptionMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::DataDescription)
            .ok_or_else(|| MsError::MissingSubtable("DATA_DESCRIPTION".to_string()))?;
        Ok(MsDataDescriptionMut::new(table))
    }

    /// Typed read-only accessor for the FEED subtable.
    pub fn feed(&self) -> MsResult<MsFeed<'_>> {
        let table = self
            .subtable(SubtableId::Feed)
            .ok_or_else(|| MsError::MissingSubtable("FEED".to_string()))?;
        Ok(MsFeed::new(table))
    }

    /// Typed mutable accessor for the FEED subtable.
    pub fn feed_mut(&mut self) -> MsResult<MsFeedMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Feed)
            .ok_or_else(|| MsError::MissingSubtable("FEED".to_string()))?;
        Ok(MsFeedMut::new(table))
    }

    /// Typed read-only accessor for the FLAG_CMD subtable.
    pub fn flag_cmd(&self) -> MsResult<MsFlagCmd<'_>> {
        let table = self
            .subtable(SubtableId::FlagCmd)
            .ok_or_else(|| MsError::MissingSubtable("FLAG_CMD".to_string()))?;
        Ok(MsFlagCmd::new(table))
    }

    /// Typed mutable accessor for the FLAG_CMD subtable.
    pub fn flag_cmd_mut(&mut self) -> MsResult<MsFlagCmdMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::FlagCmd)
            .ok_or_else(|| MsError::MissingSubtable("FLAG_CMD".to_string()))?;
        Ok(MsFlagCmdMut::new(table))
    }

    /// Typed mutable accessor for the FIELD subtable.
    pub fn field_mut(&mut self) -> MsResult<MsFieldMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Field)
            .ok_or_else(|| MsError::MissingSubtable("FIELD".to_string()))?;
        Ok(MsFieldMut::new(table))
    }

    /// Typed read-only accessor for the HISTORY subtable.
    pub fn history(&self) -> MsResult<MsHistory<'_>> {
        let table = self
            .subtable(SubtableId::History)
            .ok_or_else(|| MsError::MissingSubtable("HISTORY".to_string()))?;
        Ok(MsHistory::new(table))
    }

    /// Typed mutable accessor for the HISTORY subtable.
    pub fn history_mut(&mut self) -> MsResult<MsHistoryMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::History)
            .ok_or_else(|| MsError::MissingSubtable("HISTORY".to_string()))?;
        Ok(MsHistoryMut::new(table))
    }

    /// Typed read-only accessor for the OBSERVATION subtable.
    pub fn observation(&self) -> MsResult<MsObservation<'_>> {
        let table = self
            .subtable(SubtableId::Observation)
            .ok_or_else(|| MsError::MissingSubtable("OBSERVATION".to_string()))?;
        Ok(MsObservation::new(table))
    }

    /// Typed mutable accessor for the OBSERVATION subtable.
    pub fn observation_mut(&mut self) -> MsResult<MsObservationMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Observation)
            .ok_or_else(|| MsError::MissingSubtable("OBSERVATION".to_string()))?;
        Ok(MsObservationMut::new(table))
    }

    /// Typed read-only accessor for the POINTING subtable.
    pub fn pointing(&self) -> MsResult<MsPointing<'_>> {
        let table = self
            .subtable(SubtableId::Pointing)
            .ok_or_else(|| MsError::MissingSubtable("POINTING".to_string()))?;
        Ok(MsPointing::new(table))
    }

    /// Typed mutable accessor for the POINTING subtable.
    pub fn pointing_mut(&mut self) -> MsResult<MsPointingMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Pointing)
            .ok_or_else(|| MsError::MissingSubtable("POINTING".to_string()))?;
        Ok(MsPointingMut::new(table))
    }

    /// Typed mutable accessor for the POLARIZATION subtable.
    pub fn polarization_mut(&mut self) -> MsResult<MsPolarizationMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Polarization)
            .ok_or_else(|| MsError::MissingSubtable("POLARIZATION".to_string()))?;
        Ok(MsPolarizationMut::new(table))
    }

    /// Typed read-only accessor for the PROCESSOR subtable.
    pub fn processor(&self) -> MsResult<MsProcessor<'_>> {
        let table = self
            .subtable(SubtableId::Processor)
            .ok_or_else(|| MsError::MissingSubtable("PROCESSOR".to_string()))?;
        Ok(MsProcessor::new(table))
    }

    /// Typed mutable accessor for the PROCESSOR subtable.
    pub fn processor_mut(&mut self) -> MsResult<MsProcessorMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Processor)
            .ok_or_else(|| MsError::MissingSubtable("PROCESSOR".to_string()))?;
        Ok(MsProcessorMut::new(table))
    }

    /// Typed mutable accessor for the SPECTRAL_WINDOW subtable.
    pub fn spectral_window_mut(&mut self) -> MsResult<MsSpectralWindowMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::SpectralWindow)
            .ok_or_else(|| MsError::MissingSubtable("SPECTRAL_WINDOW".to_string()))?;
        Ok(MsSpectralWindowMut::new(table))
    }

    /// Typed read-only accessor for the STATE subtable.
    pub fn state(&self) -> MsResult<MsState<'_>> {
        let table = self
            .subtable(SubtableId::State)
            .ok_or_else(|| MsError::MissingSubtable("STATE".to_string()))?;
        Ok(MsState::new(table))
    }

    /// Typed mutable accessor for the STATE subtable.
    pub fn state_mut(&mut self) -> MsResult<MsStateMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::State)
            .ok_or_else(|| MsError::MissingSubtable("STATE".to_string()))?;
        Ok(MsStateMut::new(table))
    }

    /// Typed read-only accessor for the DOPPLER subtable.
    pub fn doppler(&self) -> MsResult<MsDoppler<'_>> {
        let table = self
            .subtable(SubtableId::Doppler)
            .ok_or_else(|| MsError::MissingSubtable("DOPPLER".to_string()))?;
        Ok(MsDoppler::new(table))
    }

    /// Typed mutable accessor for the DOPPLER subtable.
    pub fn doppler_mut(&mut self) -> MsResult<MsDopplerMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Doppler)
            .ok_or_else(|| MsError::MissingSubtable("DOPPLER".to_string()))?;
        Ok(MsDopplerMut::new(table))
    }

    /// Typed read-only accessor for the FREQ_OFFSET subtable.
    pub fn freq_offset(&self) -> MsResult<MsFreqOffset<'_>> {
        let table = self
            .subtable(SubtableId::FreqOffset)
            .ok_or_else(|| MsError::MissingSubtable("FREQ_OFFSET".to_string()))?;
        Ok(MsFreqOffset::new(table))
    }

    /// Typed mutable accessor for the FREQ_OFFSET subtable.
    pub fn freq_offset_mut(&mut self) -> MsResult<MsFreqOffsetMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::FreqOffset)
            .ok_or_else(|| MsError::MissingSubtable("FREQ_OFFSET".to_string()))?;
        Ok(MsFreqOffsetMut::new(table))
    }

    /// Typed read-only accessor for the SOURCE subtable.
    pub fn source(&self) -> MsResult<MsSource<'_>> {
        let table = self
            .subtable(SubtableId::Source)
            .ok_or_else(|| MsError::MissingSubtable("SOURCE".to_string()))?;
        Ok(MsSource::new(table))
    }

    /// Typed mutable accessor for the SOURCE subtable.
    pub fn source_mut(&mut self) -> MsResult<MsSourceMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Source)
            .ok_or_else(|| MsError::MissingSubtable("SOURCE".to_string()))?;
        Ok(MsSourceMut::new(table))
    }

    /// Typed read-only accessor for the SYSCAL subtable.
    pub fn syscal(&self) -> MsResult<MsSysCal<'_>> {
        let table = self
            .subtable(SubtableId::SysCal)
            .ok_or_else(|| MsError::MissingSubtable("SYSCAL".to_string()))?;
        Ok(MsSysCal::new(table))
    }

    /// Typed mutable accessor for the SYSCAL subtable.
    pub fn syscal_mut(&mut self) -> MsResult<MsSysCalMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::SysCal)
            .ok_or_else(|| MsError::MissingSubtable("SYSCAL".to_string()))?;
        Ok(MsSysCalMut::new(table))
    }

    /// Typed read-only accessor for the WEATHER subtable.
    pub fn weather(&self) -> MsResult<MsWeather<'_>> {
        let table = self
            .subtable(SubtableId::Weather)
            .ok_or_else(|| MsError::MissingSubtable("WEATHER".to_string()))?;
        Ok(MsWeather::new(table))
    }

    /// Typed mutable accessor for the WEATHER subtable.
    pub fn weather_mut(&mut self) -> MsResult<MsWeatherMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Weather)
            .ok_or_else(|| MsError::MissingSubtable("WEATHER".to_string()))?;
        Ok(MsWeatherMut::new(table))
    }

    // ---- Main-table column accessors ----

    /// Typed read-only accessor for a complex visibility data column.
    ///
    /// Returns `MsError::ColumnNotPresent` if the selected column is absent.
    pub fn data_column(
        &self,
        column: VisibilityDataColumn,
    ) -> MsResult<crate::columns::data_columns::DataColumn<'_>> {
        match column {
            VisibilityDataColumn::Data => {
                crate::columns::data_columns::DataColumn::data(&self.main)
            }
            VisibilityDataColumn::CorrectedData => {
                crate::columns::data_columns::DataColumn::corrected_data(&self.main)
            }
            VisibilityDataColumn::ModelData => {
                crate::columns::data_columns::DataColumn::model_data(&self.main)
            }
        }
    }

    /// Typed mutable accessor for a complex visibility data column.
    ///
    /// Returns `MsError::ColumnNotPresent` if the selected column is absent.
    pub fn data_column_mut(
        &mut self,
        column: VisibilityDataColumn,
    ) -> MsResult<crate::columns::data_columns::DataColumnMut<'_>> {
        match column {
            VisibilityDataColumn::Data => {
                crate::columns::data_columns::DataColumnMut::data(&mut self.main)
            }
            VisibilityDataColumn::CorrectedData => {
                crate::columns::data_columns::DataColumnMut::corrected_data(&mut self.main)
            }
            VisibilityDataColumn::ModelData => {
                crate::columns::data_columns::DataColumnMut::model_data(&mut self.main)
            }
        }
    }

    /// Typed accessor for the FLAG column.
    pub fn flag_column(&self) -> crate::columns::flag_columns::FlagColumn<'_> {
        crate::columns::flag_columns::FlagColumn::new(&self.main)
    }

    /// Typed accessor for the FLAG_ROW column.
    pub fn flag_row_column(&self) -> crate::columns::flag_columns::FlagRowColumn<'_> {
        crate::columns::flag_columns::FlagRowColumn::new(&self.main)
    }

    /// Typed accessor for the WEIGHT column.
    pub fn weight_column(&self) -> crate::columns::weight_columns::WeightColumn<'_> {
        crate::columns::weight_columns::WeightColumn::new(&self.main)
    }

    /// Typed accessor for the SIGMA column.
    pub fn sigma_column(&self) -> crate::columns::weight_columns::SigmaColumn<'_> {
        crate::columns::weight_columns::SigmaColumn::new(&self.main)
    }

    // ---- Iteration convenience methods ----

    /// Iterate over the main table with canonical sort order
    /// (ARRAY_ID, FIELD_ID, DATA_DESC_ID, TIME).
    ///
    /// Convenience wrapper around [`grouping::iter_ms`](crate::grouping::iter_ms).
    pub fn iter(&self) -> MsResult<crate::grouping::MsIterator<'_>> {
        crate::grouping::iter_ms(self)
    }

    /// Iterate over the main table grouped by custom columns.
    ///
    /// Convenience wrapper around [`grouping::iter_ms_by`](crate::grouping::iter_ms_by).
    pub fn iter_by<'a>(&'a self, columns: &[&str]) -> MsResult<crate::grouping::MsIterator<'a>> {
        crate::grouping::iter_ms_by(self, columns)
    }

    // ---- Internal helpers ----

    fn from_schemas(schemas: MsSchemas, path: Option<PathBuf>) -> MsResult<Self> {
        let mut main = Table::with_schema(schemas.main);
        main.set_info(TableInfo {
            table_type: CASACORE_MS_TABLE_TYPE.to_string(),
            sub_type: String::new(),
            readme: Vec::new(),
        });
        apply_column_metadata(&mut main, crate::schema::main_table::REQUIRED_COLUMNS);
        apply_column_metadata(&mut main, crate::schema::main_table::OPTIONAL_COLUMNS);
        ensure_main_column_keywords(&mut main);

        // Set MS_VERSION keyword
        main.keywords_mut().push(RecordField::new(
            "MS_VERSION",
            Value::Scalar(ScalarValue::Float32(MS_VERSION)),
        ));

        // Set subtable reference keywords (as string paths)
        let mut subtables = HashMap::new();
        let mut subtable_paths = HashMap::new();
        for (id, schema) in schemas.subtables {
            let subtable_ref = id.name().to_string();
            main.keywords_mut()
                .push(RecordField::new(id.name(), Value::table_ref(subtable_ref)));

            let table = Table::with_schema(schema);
            let mut table = table;
            apply_column_metadata(&mut table, crate::schema::required_columns(id));
            apply_column_metadata(&mut table, crate::schema::optional_columns(id));
            if id == SubtableId::SpectralWindow {
                for column in ["CHAN_FREQ", "REF_FREQUENCY"] {
                    let (types, codes) = default_frequency_ref_map();
                    TableMeasDesc::new_variable_int(
                        column,
                        MeasureType::Frequency,
                        "MEAS_FREQ_REF",
                        types,
                        codes,
                    )?
                    .write(&mut table)?;
                }
            }
            if let Some(ref p) = path {
                subtable_paths.insert(id, p.join(id.name()));
            }
            subtables.insert(id, table);
        }

        Ok(Self {
            main,
            subtables,
            subtable_paths,
            detached_keyword_tables: Default::default(),
            path,
        })
    }

    fn refresh_subtable_paths(&mut self, base_path: &Path) {
        for id in self.subtables.keys().copied() {
            self.subtable_paths
                .entry(id)
                .or_insert_with(|| base_path.join(id.name()));
        }
    }

    fn rebase_subtable_paths(&mut self, base_path: &Path) {
        self.subtable_paths = self
            .subtables
            .keys()
            .copied()
            .map(|id| (id, base_path.join(id.name())))
            .collect();
    }

    fn sync_main_metadata(&mut self, base_path: &Path) {
        // The type is always a MeasurementSet; the writer's subtype and
        // readme (casacore's simulator writes "simulator") are kept.
        let info = self.main.info();
        self.main.set_info(TableInfo {
            table_type: CASACORE_MS_TABLE_TYPE.to_string(),
            sub_type: info.sub_type.clone(),
            readme: info.readme.clone(),
        });
        apply_column_metadata(&mut self.main, crate::schema::main_table::REQUIRED_COLUMNS);
        apply_column_metadata(&mut self.main, crate::schema::main_table::OPTIONAL_COLUMNS);
        ensure_main_column_keywords(&mut self.main);
        self.main.keywords_mut().upsert(
            "MS_VERSION",
            Value::Scalar(ScalarValue::Float32(MS_VERSION)),
        );

        for id in SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL.iter())
        {
            if let Some(subtable_path) = self.subtable_paths.get(id) {
                let keyword_path = casa_tables::strip_directory(subtable_path, base_path);
                self.main
                    .keywords_mut()
                    .upsert(id.name(), Value::table_ref(keyword_path));
            } else {
                self.main.keywords_mut().remove(id.name());
            }
        }
    }
}

#[doc(hidden)]
pub fn apply_column_metadata(table: &mut Table, defs: &[ColumnDef]) {
    for def in defs {
        let has_column = table
            .schema()
            .and_then(|schema| schema.column(def.name))
            .is_some();
        if !has_column {
            continue;
        }

        let mut keywords = table.column_keywords(def.name).cloned().unwrap_or_default();
        if let Some(units) = quantum_units_for(def) {
            keywords.upsert(
                "QuantumUnits",
                Value::Array(ArrayValue::from_string_vec(units)),
            );
        }
        if let Some(measinfo) = measinfo_for(def) {
            let merged = merge_measinfo_keywords(
                match keywords.get("MEASINFO") {
                    Some(Value::Record(existing)) => Some(existing),
                    _ => None,
                },
                &measinfo,
            );
            keywords.upsert("MEASINFO", Value::Record(merged));
        }
        table.set_column_keywords(def.name, keywords);
    }
}

/// Choose the standard storage manager for an MS table at the supplied path.
pub fn measurement_set_table_options(path: &Path) -> TableOptions {
    TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan)
}

/// Merge the schema's MEASINFO `defaults` into a column's `existing` record.
///
/// Only a new record takes the default `Ref`. An existing record keeps its
/// reference state: its `Ref` if it has one, and none if it has none.
/// casacore reads a missing `Ref` as the measure's reference code 0 (J2000
/// for UVW, which casa-rs wrote without a `Ref` before it defaulted to
/// ITRF), so filling one in on a save would relabel the stored values.
fn merge_measinfo_keywords(existing: Option<&RecordValue>, defaults: &RecordValue) -> RecordValue {
    let mut merged = defaults.clone();
    if let Some(existing) = existing {
        if existing.get("Ref").is_none() {
            merged.remove("Ref");
        }
        for field in existing.fields() {
            let preserve_existing = field.name == "Ref" || merged.get(&field.name).is_none();
            if preserve_existing {
                merged.upsert(field.name.clone(), field.value.clone());
            }
        }
    }
    merged
}

fn ensure_main_column_keywords(main: &mut Table) {
    let mut keywords = main
        .column_keywords("FLAG_CATEGORY")
        .cloned()
        .unwrap_or_default();
    if keywords.get("CATEGORY").is_none() {
        keywords.upsert(
            "CATEGORY",
            Value::Array(ArrayValue::from_string_vec(Vec::new())),
        );
        main.set_column_keywords("FLAG_CATEGORY", keywords);
    }
}

/// Resolve the filesystem path of a subtable from the MAIN table keywords.
pub fn subtable_path_from_main(main: &Table, base_path: &Path, id: SubtableId) -> Option<PathBuf> {
    if let Some(value) = main.keywords().get(id.name()) {
        let keyword_path = match value {
            Value::TableRef(path) => Some(path.as_str()),
            // Accept older Rust-written MS trees that stored string keywords.
            Value::Scalar(ScalarValue::String(path)) => Some(path.as_str()),
            _ => None,
        };
        if let Some(keyword_path) = keyword_path {
            let candidate = casa_tables::add_directory(keyword_path, base_path);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    let fallback = base_path.join(id.name());
    fallback.exists().then_some(fallback)
}

/// Open a subtable under a permanent read lock when the platform supports it.
pub fn held_subtable_reader(path: &Path) -> MsResult<Table> {
    let options = TableOptions::new(path);
    #[cfg(unix)]
    let table = Table::open_with_lock_attempts(
        options,
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLocking).read_only(true),
        1,
    )?;
    #[cfg(not(unix))]
    let table = Table::open(options)?;
    Ok(table)
}

#[cfg(test)]
mod tests;
