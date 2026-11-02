// SPDX-License-Identifier: LGPL-3.0-or-later
//! Writing back to a MeasurementSet v2 through production's bounded
//! selected-row mutation (`MeasurementSetWriteSession`).
//!
//! The session holds casacore's write lock on MAIN while any of its
//! updaters is live, so another process waits for a pass, not for each
//! batch, and it reopens its handle each time it takes the lock, so what
//! an updater reads and merges is never older than the lock it writes
//! under. Each batch is written by a mutation session of its own under that
//! lock; the session's updaters write one batch at a time whatever their
//! number. A batch's samples are their grid cells' rows; a padded cell has
//! no row and receives nothing. A cell the patch supplies whole (every
//! channel and product, and for flags every element set or cleared) is
//! written without reading it; any other is read and merged. Writes are in
//! place, as casacore's are: a failure leaves the rows already written.
//!
//! A `CORRECTED_DATA` the session creates starts as `DATA`'s copy and a
//! `MODEL_DATA` as CASA's initial model (1, 0 in the cross hands), as CASA
//! initializes them, so rows and channels a pass does not reach read as
//! uncorrected data and a unit point source.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use casa_tables::TableWriteLock;

use casa_observation::{
    AccessRequest, BatchAddress, Correlation, ObservationError, PartitionEntry, PatchValues,
    ResolvedTarget, TimeRuns, UpdateReport, Updater, VisibilityBatch, WeightChannels, WritableRole,
    check_values, refuse_payload_checks, uniform_over_channels,
};
use casa_types::ArrayValue;
use casa_types::measures::MjdHighPrec;
use ndarray::{ArrayD, ArrayView3, Axis, IxDyn, ShapeBuilder};
use num_complex::Complex32;

use super::index::NO_ROW;
use super::{Msv2Observation, OpenOptions};
use crate::column_def::build_table_schema;
use crate::observation_io::update::{Claim, Unit, request_error, storage};
use crate::schema::main_table::{VisibilityDataColumn, initial_model_visibility};
use crate::{
    MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession,
};

/// An MSv2 opened for update: the observation it reads and the
/// MeasurementSet handle its updaters write through.
pub(in crate::observation_io) struct Msv2Update {
    observation: Msv2Observation,
    path: PathBuf,
    /// MAIN's rows, each of which one grid cell addresses.
    rows: usize,
    writer: Mutex<Writer>,
}

/// The handle the updaters write through, and casacore's write lock on
/// MAIN while any of them is live.
struct Writer {
    ms: MeasurementSet,
    /// Taken for the first live updater and released, publishing the
    /// session's writes, when the last goes; taken again, with `ms`
    /// reopened under it, after a failed write released it.
    lock: Option<TableWriteLock>,
    live: usize,
}

impl Writer {
    /// Hold MAIN's write lock: take it, waiting as casacore does, when it
    /// is not held, and reopen the handle under it.
    fn hold(&mut self, path: &Path, rows: usize) -> Result<(), ObservationError> {
        if self.lock.is_some() {
            return Ok(());
        }
        let lock = TableWriteLock::acquire_to_reread(path, 0).map_err(storage)?;
        self.reopen(path, rows)?;
        self.lock = Some(lock);
        Ok(())
    }

    /// Open the handle afresh, refusing a MAIN whose rows the session's
    /// grids no longer address.
    fn reopen(&mut self, path: &Path, rows: usize) -> Result<(), ObservationError> {
        self.ms = open_addressed(path, rows)?;
        Ok(())
    }
}

/// The MeasurementSet at `path`, refused when MAIN no longer has the `rows`
/// the session's grids address.
fn open_addressed(path: &Path, rows: usize) -> Result<MeasurementSet, ObservationError> {
    let ms = MeasurementSet::open(path).map_err(storage)?;
    if ms.main_table().row_count() != rows {
        return Err(request_error(format!(
            "MAIN has {} rows and had {rows} when the session opened: another process \
             changed it, so the session's grids no longer address its rows",
            ms.main_table().row_count()
        )));
    }
    Ok(ms)
}

/// A live updater's share of the session's lock on MAIN: the last to go
/// releases it, publishing what the session wrote.
struct Lease<'s> {
    update: &'s Msv2Update,
    ended: bool,
}

impl Lease<'_> {
    /// Leave, releasing the lock if this was the last live updater.
    fn end(mut self) -> Result<(), ObservationError> {
        self.ended = true;
        self.update.leave()
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if !self.ended {
            let _ = self.update.leave();
        }
    }
}

/// A bound target: the role and the MAIN column it writes.
struct Column {
    role: WritableRole,
    name: String,
}

/// Writes one traversal through production's selected-row mutation, a
/// mutation session per batch under the update session's lock on MAIN.
struct Msv2Updater<'s> {
    update: &'s Msv2Update,
    _claim: Claim<'s>,
    lease: Lease<'s>,
    columns: Vec<Column>,
    /// The bound targets, which plan each batch's columns.
    targets: Vec<ResolvedTarget>,
    request: AccessRequest,
    /// Each requested partition's selected grid times.
    selections: Vec<TimeRuns>,
    report: UpdateReport,
}

/// One target's cells for a batch's present rows, in row order.
type Cells = Vec<ArrayValue>;

impl Msv2Update {
    #[cfg(unix)]
    pub(in crate::observation_io) fn open(
        path: &std::path::Path,
        options: &OpenOptions,
    ) -> Result<Self, ObservationError> {
        Self::over(Msv2Observation::open_with(path, options)?)
    }

    /// The update of `observation`, opened already: a second handle to its
    /// MeasurementSet for the updaters to write through, refused when MAIN
    /// no longer has the rows its grids address.
    pub(in crate::observation_io) fn over(
        observation: Msv2Observation,
    ) -> Result<Self, ObservationError> {
        let path = observation.path()?.to_path_buf();
        let rows = observation.main_rows();
        let writer = Writer {
            ms: open_addressed(&path, rows)?,
            lock: None,
            live: 0,
        };
        Ok(Self {
            observation,
            path,
            rows,
            writer: Mutex::new(writer),
        })
    }

    pub(in crate::observation_io) fn observation(&self) -> &Msv2Observation {
        &self.observation
    }

    /// Refuse, when the session opens and before anything is cleared or
    /// written, what its updaters could not carry out within their bound: a
    /// target whose column cannot be written in place in bounded memory
    /// ([`refuse_unbounded`]), or a `MODEL_DATA` to create whose rows the
    /// session's grids do not all place ([`require_placed`]).
    pub(in crate::observation_io) fn preflight(
        &self,
        targets: &[ResolvedTarget],
    ) -> Result<(), ObservationError> {
        let writer = self.writer();
        let main = writer.ms.main_table();
        refuse_unbounded(main, targets)?;
        if seeds_model(main, targets) {
            require_placed(&writer.ms, &self.observation)?;
        }
        Ok(())
    }

    fn writer(&self) -> MutexGuard<'_, Writer> {
        self.writer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hold MAIN's lock, clear what CASA clears before `targets` are
    /// written, and create and seed the columns the session creates;
    /// returns the cells seeded.
    fn prepare(
        &self,
        writer: &mut Writer,
        request: &AccessRequest,
        targets: &[ResolvedTarget],
    ) -> Result<usize, ObservationError> {
        let scratch_targets = targets
            .iter()
            .map(|target| scratch(&writer.ms, target).map(|column| column.is_some()))
            .collect::<Result<Vec<_>, _>>()?;
        if writer.lock.is_none() && scratch_targets.contains(&true) {
            // Cleared before MAIN's lock is taken, from a handle opened
            // now, so SOURCE's lock is waited for as casacore waits; under
            // the lock, below, there is then nothing left to clear. While
            // another updater is live the lock is held, and SOURCE's is
            // tried once, as while any holder of MAIN's lock clears.
            writer.reopen(&self.path, self.rows)?;
            // A column another writer has made unbounded since the session
            // opened is refused before the clear, as under the lock below.
            refuse_unbounded(writer.ms.main_table(), targets)?;
            for target in targets {
                clear(&mut writer.ms, target, None)?;
            }
        }
        writer.hold(&self.path, self.rows)?;
        let Writer { ms, lock, .. } = writer;
        // Again under MAIN's lock: another writer may have added a column
        // since the session opened. Its rows are those it opened with
        // (`hold`), which its grids place (`preflight`).
        refuse_unbounded(ms.main_table(), targets)?;
        let seeding = seeds_model(ms.main_table(), targets);
        for target in targets {
            clear(ms, target, lock.as_mut())?;
            add_column(ms, target)?;
        }
        // Install the columns the session creates, their storage and
        // values cloned from DATA, with a session of no rows; a MODEL_DATA
        // installed now is then seeded with CASA's initial model.
        mutate(
            ms,
            lock,
            &self.observation,
            request,
            targets,
            &[],
            |_, _| Ok(()),
        )?;
        if !seeding {
            return Ok(0);
        }
        seed_model(ms, lock, &self.observation, seed_bytes(request))
    }

    /// The most bytes an updater of `targets` over `request` holds while it
    /// writes a batch, besides the values it is given: for each present row
    /// of the largest batch, its cell of each target as many as
    /// [`CELL_COPIES`] times over and the row lists of the batch and its
    /// mutation, and two of each column's tiles; or, when it creates and
    /// seeds `MODEL_DATA`, a seeding batch ([`seed_bytes`]), whichever is
    /// more; and the staging every write holds ([`WRITE_STAGING_BYTES`]).
    pub(in crate::observation_io) fn write_bytes(
        &self,
        request: &AccessRequest,
        targets: &[&ResolvedTarget],
    ) -> Result<usize, ObservationError> {
        let rows = self
            .observation
            .estimate(request)?
            .partitions
            .iter()
            .map(casa_observation::PartitionEstimate::samples_per_batch)
            .max()
            .unwrap_or(0);
        let catalogue = self.observation.catalogue();
        let entries: Vec<&PartitionEntry> = request
            .partitions
            .iter()
            .map(|id| catalogue.partition(*id))
            .collect();
        let writer = self.writer();
        let main = writer.ms.main_table();
        let mut per_row = 6 * size_of::<usize>();
        let mut tiles = 0;
        let mut seeding = 0;
        for target in targets {
            let cell = column_plan(target, &entries, main).bytes_per_row;
            per_row += CELL_COPIES * (cell + size_of::<ArrayValue>());
            // A column the session creates is cloned from DATA's storage.
            let stored = if persisted(main, &target.variable) {
                target.variable.as_str()
            } else {
                "DATA"
            };
            let staged = 2 * main
                .array_column_tile_bytes(stored)
                .map_err(|error| super::storage(crate::MsError::from(error)))?
                .unwrap_or(0);
            tiles += staged;
            if target.variable == "MODEL_DATA" && creates(main, target) {
                let cell = catalogue
                    .partitions
                    .iter()
                    .map(|entry| {
                        entry.correlations.len()
                            * entry.frequencies_hz.len()
                            * size_of::<Complex32>()
                    })
                    .max()
                    .unwrap_or(0);
                // A seeding batch and the tiles its save stages.
                seeding = seed_bytes(request).max(seed_row_bytes(cell)) + staged;
            }
        }
        Ok((rows * per_row + tiles).max(seeding) + WRITE_STAGING_BYTES)
    }

    /// An updater has gone: the last releases MAIN's lock.
    fn leave(&self) -> Result<(), ObservationError> {
        let mut writer = self.writer();
        writer.live -= 1;
        match writer.lock.take() {
            Some(lock) if writer.live == 0 => lock.release().map_err(storage),
            held => {
                writer.lock = held;
                Ok(())
            }
        }
    }

    /// Claim `request`'s times of `targets`' columns, hold MAIN's lock,
    /// create and seed the columns the session creates, and bind an updater
    /// that writes each batch through production's selected-row mutation.
    ///
    /// What CASA clears before the targets' columns are written is cleared
    /// first ([`clear`]), before any batch: a write then refused
    /// leaves it cleared with nothing written, as CASA's imager clears it
    /// when it selects the data. The destination rule ([`refuse_unbounded`])
    /// is applied again on the handle the clear opens and under the lock,
    /// so a column another writer has changed since the session opened is
    /// refused.
    pub(in crate::observation_io) fn updater<'s>(
        &'s self,
        request: &AccessRequest,
        targets: &[&ResolvedTarget],
        claim: impl FnOnce(Vec<Unit>) -> Result<Claim<'s>, ObservationError>,
    ) -> Result<Box<dyn Updater + 's>, ObservationError> {
        let estimate = self.observation.estimate(request)?;
        let selections: Vec<TimeRuns> = estimate
            .partitions
            .into_iter()
            .map(|partition| partition.time_runs)
            .collect();
        let claim = claim(
            request
                .partitions
                .iter()
                .zip(&selections)
                .flat_map(|(partition, runs)| {
                    runs.runs().iter().flat_map(|times| {
                        targets.iter().map(|target| Unit {
                            partition: *partition,
                            variable: target.variable.clone(),
                            times: times.clone(),
                        })
                    })
                })
                .collect(),
        )?;
        let targets: Vec<ResolvedTarget> = targets.iter().map(|target| (*target).clone()).collect();
        let mut writer = self.writer();
        let seeded = match self.prepare(&mut writer, request, &targets) {
            Ok(seeded) => seeded,
            Err(error) => {
                if writer.live == 0 {
                    // Released, publishing what reached MAIN.
                    drop(writer.lock.take());
                }
                return Err(error);
            }
        };
        writer.live += 1;
        drop(writer);
        Ok(Box::new(Msv2Updater {
            update: self,
            _claim: claim,
            lease: Lease {
                update: self,
                ended: false,
            },
            columns: targets
                .iter()
                .map(|target| Column {
                    role: target.target.role,
                    name: target.variable.clone(),
                })
                .collect(),
            targets,
            request: request.clone(),
            selections,
            report: UpdateReport {
                units_written: seeded,
                ..UpdateReport::default()
            },
        }))
    }
}

/// One session of production's selected-row mutation of `targets`' columns
/// over MAIN rows `rows`, which `write` writes in the session's batches
/// ([`MeasurementSetWriteSession::next_mutation_rows`]), under `lock`,
/// casacore's write lock on MAIN, held: it is handed back once every row
/// is written, and released, publishing what was written, if the session
/// fails. Columns the session creates and MAIN lacks are installed as it
/// starts, so a session of no rows only installs them.
fn mutate(
    writer: &mut MeasurementSet,
    lock: &mut Option<TableWriteLock>,
    observation: &Msv2Observation,
    request: &AccessRequest,
    targets: &[ResolvedTarget],
    rows: &[usize],
    write: impl FnOnce(
        &mut MeasurementSetWriteSession,
        &mut MeasurementSet,
    ) -> Result<(), ObservationError>,
) -> Result<(), ObservationError> {
    let catalogue = observation.catalogue();
    let entries: Vec<&PartitionEntry> = request
        .partitions
        .iter()
        .map(|id| catalogue.partition(*id))
        .collect();
    let plans: Vec<MeasurementSetWriteColumnPlan> = targets
        .iter()
        .map(|target| column_plan(target, &entries, writer.main_table()))
        .collect();
    if rows.is_empty()
        && !plans
            .iter()
            .any(|plan| plan.mode == MeasurementSetColumnWriteMode::Create)
    {
        return Ok(());
    }
    let bytes_per_row: usize = plans.iter().map(|plan| plan.bytes_per_row).sum();
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        rows.to_vec(),
        plans,
        MeasurementSetWriteResources {
            available_bytes: rows.len().max(1).saturating_mul(bytes_per_row.max(1)),
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: casa_tables::STREAMING_TILED_COLUMN_BUFFER_BYTES,
        },
    )
    .map_err(storage)?;
    let held = lock.take().expect("MAIN's write lock is held");
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation_holding(writer, plan, held)
            .map_err(storage)?;
    write(&mut session, writer)?;
    let (_, held) = session.finish_mutation_holding().map_err(storage)?;
    *lock = Some(held);
    Ok(())
}

/// Whether the session creates `target`'s column: one it resolved as
/// created that no data manager stores yet.
fn creates(main: &casa_tables::Table, target: &ResolvedTarget) -> bool {
    target.created && !persisted(main, &target.variable)
}

/// Whether the session creates, and so seeds, `MODEL_DATA`.
fn seeds_model(main: &casa_tables::Table, targets: &[ResolvedTarget]) -> bool {
    targets
        .iter()
        .any(|target| target.variable == "MODEL_DATA" && creates(main, target))
}

/// Refuse a target whose column cannot be written in place in bounded
/// memory ([`crate::scratch_columns::unbounded_destination`]): the column must be
/// in a tiled manager, and a column the session creates clones `DATA`'s,
/// which must be one and hold `DATA` alone. A column in any other manager
/// is saved by rewriting every row of its storage group, once a batch.
fn refuse_unbounded(
    main: &casa_tables::Table,
    targets: &[ResolvedTarget],
) -> Result<(), ObservationError> {
    for target in targets {
        let created = creates(main, target);
        if let Some(reason) =
            crate::scratch_columns::unbounded_destination(main, &target.variable, created)
        {
            return Err(request_error(format!(
                "{} cannot be written in place: {reason}",
                target.variable
            )));
        }
    }
    Ok(())
}

/// Whether a data manager stores MAIN's `column`.
fn persisted(main: &casa_tables::Table, column: &str) -> bool {
    main.data_manager_info()
        .iter()
        .any(|manager| manager.columns.iter().any(|name| name == column))
}

/// Copies of a written cell, with its entry, a batch's write may hold at
/// once: the updater's (built, or read and merged), moved into the table's
/// row, and what reading a cell to merge decodes beside it.
/// `msv2_update_memory` measures about 2.2 for cells written whole and 2.9
/// for cells merged; the bound keeps a copy's headroom over that.
const CELL_COPIES: usize = 4;

/// Bytes every write holds whatever its batch: the write handle reopened
/// under MAIN's lock and the table descriptions a mutation session reads.
const WRITE_STAGING_BYTES: usize = 1 << 20;

/// The most bytes a seeding batch holds: what one of the traversal's
/// batches may (`request.batch_bytes`), up to 64 MiB.
fn seed_bytes(request: &AccessRequest) -> usize {
    request.batch_bytes.min(64 << 20)
}

/// Bytes one seeded row holds while its batch is written: its cell in the
/// batch and in the table's row, and their entries.
fn seed_row_bytes(cell_bytes: usize) -> usize {
    2 * (cell_bytes + size_of::<ArrayValue>()) + 3 * size_of::<usize>()
}

/// Refuse a MAIN whose rows the session's grids do not all place: a
/// `MODEL_DATA` the session creates is seeded in every row its grids place,
/// and a row outside them would keep the `DATA` it was cloned from.
fn require_placed(
    ms: &MeasurementSet,
    observation: &Msv2Observation,
) -> Result<(), ObservationError> {
    let placed: usize = observation
        .partitions
        .iter()
        .map(|partition| {
            partition
                .grid
                .cells
                .iter()
                .filter(|row| **row != NO_ROW)
                .count()
        })
        .sum();
    let main_rows = ms.main_table().row_count();
    if placed != main_rows {
        return Err(request_error(format!(
            "MAIN has {main_rows} rows and the session's grids place {placed}, so MODEL_DATA \
             cannot be seeded in every row"
        )));
    }
    Ok(())
}

/// Write CASA's initial model ([`initial_model_visibility`]) into every
/// row of a `MODEL_DATA` just installed, as CASA initializes the column
/// when it adds it; returns the cells written. The install cloned `DATA`'s
/// storage and values (the layout casacore is known to read, as applycal
/// installs `CORRECTED_DATA`) and every cell is overwritten here, so once
/// this returns no row of the model holds `DATA`; a pass then writes its
/// own values over the seed. Each partition's rows are seeded in batches
/// of at most `budget` bytes ([`seed_row_bytes`]), one mutation session a
/// batch, so the seeding holds no list of MAIN's rows. Every MAIN row lies
/// in one partition's grid ([`require_placed`], checked when the session
/// opens).
fn seed_model(
    ms: &mut MeasurementSet,
    lock: &mut Option<TableWriteLock>,
    observation: &Msv2Observation,
    budget: usize,
) -> Result<usize, ObservationError> {
    let catalogue = observation.catalogue();
    let mut seeded = 0;
    for (entry, partition) in catalogue.partitions.iter().zip(&observation.partitions) {
        let shape = [entry.correlations.len(), entry.frequencies_hz.len()];
        let cell_bytes = (shape[0] * shape[1] * size_of::<Complex32>()).max(1);
        let seed = ArrayValue::Complex32(ArrayD::from_shape_fn(IxDyn(&shape), |at| {
            initial_model_visibility(entry.correlations[at[0]].casacore_code())
        }));
        let rows_per_batch = (budget / seed_row_bytes(cell_bytes)).max(1);
        let mut rows = partition
            .grid
            .cells
            .iter()
            .filter(|row| **row != NO_ROW)
            .map(|row| *row as usize);
        loop {
            let batch: Vec<usize> = rows.by_ref().take(rows_per_batch).collect();
            if batch.is_empty() {
                break;
            }
            let plan = MeasurementSetWritePlan::selected_row_mutation(
                batch.clone(),
                vec![MeasurementSetWriteColumnPlan {
                    name: "MODEL_DATA".to_string(),
                    bytes_per_row: cell_bytes,
                    mode: MeasurementSetColumnWriteMode::Replace,
                    storage_manager: MeasurementSetColumnStorage::Persisted,
                    tile_shape: None,
                    create_source_column: None,
                }],
                MeasurementSetWriteResources {
                    available_bytes: batch.len() * cell_bytes,
                    maximum_live_batches: 1,
                    tiled_column_buffer_bytes: casa_tables::STREAMING_TILED_COLUMN_BUFFER_BYTES,
                },
            )
            .map_err(storage)?;
            let held = lock.take().expect("MAIN's write lock is held");
            let mut session =
                MeasurementSetWriteSession::start_selected_row_mutation_holding(ms, plan, held)
                    .map_err(storage)?;
            seeded += batch.len();
            session
                .write_mutation_batch(
                    ms,
                    MeasurementSetMutationBatch {
                        columns: vec![MeasurementSetMutationColumnBatch {
                            name: "MODEL_DATA".to_string(),
                            values: MeasurementSetMutationColumnValues::Arrays(vec![
                                seed.clone();
                                batch.len()
                            ]),
                        }],
                        row_indices: batch,
                    },
                )
                .map_err(storage)?;
            let (_, held) = session.finish_mutation_holding().map_err(storage)?;
            *lock = Some(held);
        }
    }
    Ok(seeded)
}

/// The scratch column `target` writes, if any, and whether the session
/// adds it to MAIN's schema.
fn scratch(
    ms: &MeasurementSet,
    target: &ResolvedTarget,
) -> Result<Option<(VisibilityDataColumn, bool)>, ObservationError> {
    let name = target.variable.as_str();
    let column = match name {
        "MODEL_DATA" => VisibilityDataColumn::ModelData,
        "CORRECTED_DATA" => VisibilityDataColumn::CorrectedData,
        _ if target.created => {
            return Err(request_error(format!(
                "{name} is not a column the session creates"
            )));
        }
        _ => return Ok(None),
    };
    let adds = target.created
        && !ms
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column(name));
    Ok(Some((column, adds)))
}

/// Clear what CASA clears before `target`'s column is written
/// ([`crate::scratch_columns`]): every field's virtual model before a
/// `MODEL_DATA` write, created or not, and the cached sort keywords before
/// a column is added; under `lock`, MAIN's write lock held, when given.
fn clear(
    ms: &mut MeasurementSet,
    target: &ResolvedTarget,
    lock: Option<&mut TableWriteLock>,
) -> Result<(), ObservationError> {
    let Some((column, adds)) = scratch(ms, target)? else {
        return Ok(());
    };
    if adds || column == VisibilityDataColumn::ModelData {
        match lock {
            Some(lock) => crate::scratch_columns::prepare_write_holding(ms, column, adds, lock),
            None => crate::scratch_columns::prepare_write(ms, column, adds),
        }
        .map_err(storage)?;
    }
    Ok(())
}

/// Add a created column MAIN's schema lacks, with `DATA`'s keywords, as
/// applycal adds `CORRECTED_DATA`. `CORRECTED_DATA`'s storage is installed
/// by the updater's mutation, cloned from `DATA` values included, as CASA
/// seeds it, so rows a pass skips read as uncorrected; `MODEL_DATA` is
/// installed and seeded with CASA's initial model by [`seed_model`].
fn add_column(ms: &mut MeasurementSet, target: &ResolvedTarget) -> Result<(), ObservationError> {
    let Some((column, true)) = scratch(ms, target)? else {
        return Ok(());
    };
    let name = target.variable.as_str();
    let schema = build_table_schema(&[*column.optional_column().column_def()]).map_err(storage)?;
    let definition = schema
        .column(name)
        .ok_or_else(|| request_error(format!("no schema for {name}")))?
        .clone();
    ms.main_table_mut()
        .add_column(definition, None)
        .map_err(storage)?;
    if let Some(keywords) = ms.main_table().column_keywords("DATA").cloned() {
        ms.main_table_mut().set_column_keywords(name, keywords);
    }
    Ok(())
}

/// Production's plan for `target`'s column: replace an existing one, or
/// create `CORRECTED_DATA` seeded from `DATA`, as applycal does.
fn column_plan(
    target: &ResolvedTarget,
    entries: &[&PartitionEntry],
    main: &casa_tables::Table,
) -> MeasurementSetWriteColumnPlan {
    let cell = entries
        .iter()
        .map(|entry| entry.correlations.len() * entry.frequencies_hz.len())
        .max()
        .unwrap_or(0);
    let correlations = entries
        .iter()
        .map(|entry| entry.correlations.len())
        .max()
        .unwrap_or(0);
    let bytes_per_row = match (target.target.role, target.variable.as_str()) {
        (WritableRole::Visibilities, _) => cell * 8,
        (WritableRole::Flags, _) => cell,
        (WritableRole::Weights, "WEIGHT") => correlations * 4,
        (WritableRole::Weights, _) => cell * 4,
    };
    let create = creates(main, target);
    MeasurementSetWriteColumnPlan {
        name: target.variable.clone(),
        bytes_per_row,
        mode: if create {
            MeasurementSetColumnWriteMode::Create
        } else {
            MeasurementSetColumnWriteMode::Replace
        },
        storage_manager: if create {
            MeasurementSetColumnStorage::TiledShape
        } else {
            MeasurementSetColumnStorage::Persisted
        },
        tile_shape: None,
        create_source_column: create.then(|| "DATA".to_string()),
    }
}

impl Updater for Msv2Updater<'_> {
    fn write(
        &mut self,
        batch: &VisibilityBatch<'_>,
        values: &[PatchValues<'_>],
    ) -> Result<(), ObservationError> {
        // The grid knows the padding, so no check reads the payload.
        self.write_address(&batch.address(), values)
    }

    fn write_at(
        &mut self,
        address: &BatchAddress,
        values: &[PatchValues<'_>],
    ) -> Result<(), ObservationError> {
        refuse_payload_checks(values)?;
        self.write_address(address, values)
    }

    fn finish(self: Box<Self>) -> Result<UpdateReport, ObservationError> {
        let report = self.report;
        self.lease.end()?;
        Ok(report)
    }
}

impl Msv2Updater<'_> {
    /// Write `values` for the samples of the batch at `address`: each row
    /// a patch writes, through one mutation session under MAIN's lock.
    fn write_address(
        &mut self,
        address: &BatchAddress,
        values: &[PatchValues<'_>],
    ) -> Result<(), ObservationError> {
        let roles: Vec<WritableRole> = self.columns.iter().map(|column| column.role).collect();
        check_values(address, &roles, values)?;
        let observation = &self.update.observation;
        if !Arc::ptr_eq(address.catalogue(), observation.catalogue()) {
            return Err(request_error(
                "the batch was not delivered by this session's observation",
            ));
        }
        let first = self.place(address)?;
        let grid = &observation.partitions[address.partition().0 as usize].grid;
        let cells = grid.cells(first..first + address.times());
        let entry = address.catalogue().partition(address.partition());
        let products = product_indices(address.correlations(), entry)?;
        let window = Window {
            first_channel: address.channels().start,
            channels: entry.frequencies_hz.len(),
            products: &products,
            correlations: entry.correlations.len(),
            whole: address.channels() == (0..entry.frequencies_hz.len())
                && products.len() == entry.correlations.len(),
        };
        for (column, value) in self.columns.iter().zip(values) {
            refuse_unaddressable(column, value, cells, &window)?;
        }
        // A row no patch writes (a sample a visibility mask leaves) is
        // neither read nor written.
        let present: Vec<(usize, usize)> = cells
            .iter()
            .enumerate()
            .filter(|(sample, row)| {
                **row != NO_ROW && values.iter().any(|value| value.writes_sample(*sample))
            })
            .map(|(sample, row)| (sample, *row as usize))
            .collect();
        let unaddressed = cells.iter().filter(|row| **row == NO_ROW).count();
        let mut writer = self.update.writer();
        writer.hold(&self.update.path, self.update.rows)?;
        let Writer { ms, lock, .. } = &mut *writer;
        let mut columns = Vec::with_capacity(self.columns.len());
        for (column, value) in self.columns.iter().zip(values) {
            let (cells, read) = column_cells(ms, column, value, &present, &window)?;
            self.report.units_read += read;
            columns.push(cells);
        }
        let rows: Vec<usize> = present.iter().map(|(_, row)| *row).collect();
        mutate(
            ms,
            lock,
            observation,
            &self.request,
            &self.targets,
            &rows,
            |session, writer| {
                let mut written = 0;
                while written < rows.len() {
                    let end = written + session.next_mutation_rows().map_err(storage)?.len();
                    let batch_columns = self
                        .columns
                        .iter()
                        .zip(&mut columns)
                        .map(|(column, cells)| MeasurementSetMutationColumnBatch {
                            name: column.name.clone(),
                            values: MeasurementSetMutationColumnValues::Arrays(
                                cells.drain(..end - written).collect(),
                            ),
                        })
                        .collect();
                    session
                        .write_mutation_batch(
                            writer,
                            MeasurementSetMutationBatch {
                                row_indices: rows[written..end].to_vec(),
                                columns: batch_columns,
                            },
                        )
                        .map_err(storage)?;
                    written = end;
                }
                Ok(())
            },
        )?;
        self.report.batches += 1;
        self.report.samples += present.len();
        self.report.unaddressed += unaddressed;
        self.report.units_written += present.len() * self.columns.len();
        Ok(())
    }

    /// The first grid time of the batch at `address`, after checking it is
    /// of the traversal: a requested partition, the traversal's channels,
    /// and times its selection holds. Batches come in any order.
    fn place(&self, address: &BatchAddress) -> Result<usize, ObservationError> {
        let index = self
            .request
            .partitions
            .iter()
            .position(|partition| *partition == address.partition())
            .ok_or_else(|| request_error("the batch's partition is not the traversal's"))?;
        if address.channels() != self.request.channels {
            return Err(request_error(
                "the batch's channels are not the traversal's the updater was bound to",
            ));
        }
        let grid = &self.update.observation.partitions[address.partition().0 as usize].grid;
        let time = address.time().seconds_since(0, MjdHighPrec::new(0.0, 0.0));
        let first = grid
            .times_s
            .binary_search_by(|known| known.total_cmp(&time))
            .map_err(|_| request_error("the batch's first time is not on the partition's grid"))?;
        if !self.selections[index].holds(first..first + address.times()) {
            return Err(request_error(
                "the batch's times are outside the traversal's time and field selection",
            ));
        }
        Ok(first)
    }
}

/// The delivered products' positions among the partition's.
fn product_indices(
    correlations: &[Correlation],
    entry: &PartitionEntry,
) -> Result<Vec<usize>, ObservationError> {
    correlations
        .iter()
        .map(|product| {
            entry
                .correlations
                .iter()
                .position(|known| known == product)
                .ok_or_else(|| request_error("a delivered product is not the partition's"))
        })
        .collect()
}

/// The part of a MAIN cell a batch covers.
struct Window<'a> {
    first_channel: usize,
    channels: usize,
    products: &'a [usize],
    correlations: usize,
    /// Every channel and product of the cell.
    whole: bool,
}

/// Refuse, before anything is written, a change the store cannot make:
/// clearing a flag where no row exists, or a weight shared by channels
/// changed for some channels only (`WEIGHT` without `WEIGHT_SPECTRUM`).
fn refuse_unaddressable(
    column: &Column,
    value: &PatchValues<'_>,
    cells: &[u32],
    window: &Window<'_>,
) -> Result<(), ObservationError> {
    match value {
        PatchValues::Flags {
            clear: Some(clear), ..
        } => {
            let padded_cleared = cells.iter().enumerate().any(|(sample, row)| {
                *row == NO_ROW && clear.slice(ndarray::s![sample, .., ..]).iter().any(|c| *c)
            });
            if padded_cleared {
                return Err(request_error(
                    "a flag cleared where the MeasurementSet holds no row (padding)",
                ));
            }
        }
        PatchValues::Weights {
            values,
            channels: WeightChannels::Delivered,
        } if column.name == "WEIGHT" => {
            let every_channel = window.first_channel == 0 && values.shape()[1] == window.channels;
            if !every_channel || !uniform_over_channels(values) {
                return Err(request_error(
                    "WEIGHT is one value per row and correlation that every channel reads: \
                     changing it for some channels changes the others too; give every \
                     channel one value, or change all channels (WeightChannels::All)",
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

/// `column`'s new cells for the `present` (sample, row) pairs, and how
/// many cells were read to merge into: those the patch covers partly, read
/// in one call past the table's row cache, which would otherwise load
/// every row of MAIN (ADR-0008).
fn column_cells(
    ms: &MeasurementSet,
    column: &Column,
    value: &PatchValues<'_>,
    present: &[(usize, usize)],
    window: &Window<'_>,
) -> Result<(Cells, usize), ObservationError> {
    let partly: Vec<bool> = present
        .iter()
        .map(|(sample, _)| !covers_whole(column, value, *sample, window))
        .collect();
    let rows: Vec<usize> = present
        .iter()
        .zip(&partly)
        .filter(|(_, partly)| **partly)
        .map(|((_, row), _)| *row)
        .collect();
    let mut stored = if rows.is_empty() {
        Vec::new()
    } else {
        ms.main_table()
            .column_accessor(&column.name)
            .and_then(|cells| cells.array_cells_owned_uncached(&rows))
            .map_err(|error| super::storage(crate::MsError::from(error)))?
    }
    .into_iter();
    let shape = [window.correlations, window.channels];
    let mut cells = Vec::with_capacity(present.len());
    for (&(sample, _), partly) in present.iter().zip(partly) {
        let stored = partly
            .then(|| {
                stored
                    .next()
                    .flatten()
                    .ok_or_else(|| super::invalid(format!("a {} cell is undefined", column.name)))
            })
            .transpose()?;
        let cell = match value {
            PatchValues::Visibilities { values, written } => {
                let sample_values = values.index_axis(Axis(0), sample);
                let contiguous = sample_values.as_slice();
                let cell = if stored.is_none()
                    && written.is_none()
                    && window.whole
                    && window.products.iter().copied().eq(0..window.correlations)
                    && let Some(values) = contiguous
                {
                    // A delivered [channel, correlation] sample is already
                    // in the MSv2 cell's Fortran [correlation, channel] order.
                    ArrayD::from_shape_vec(IxDyn(&shape).f(), values.to_vec())
                        .expect("the checked patch has the cell's shape")
                } else {
                    let mut cell = match stored {
                        Some(stored) => complex(stored)?,
                        None => ArrayD::from_elem(IxDyn(&shape), Complex32::default()),
                    };
                    overlay(&mut cell, values, written.as_ref(), sample, window);
                    cell
                };
                ArrayValue::Complex32(cell)
            }
            PatchValues::Flags { set, clear } => {
                let mut cell = match stored {
                    Some(stored) => boolean(stored)?,
                    None => ArrayD::from_elem(IxDyn(&shape), false),
                };
                change_flags(&mut cell, set.as_ref(), clear.as_ref(), sample, window);
                ArrayValue::Bool(cell)
            }
            PatchValues::Weights { values, channels } => ArrayValue::Float32(weight_cell(
                column,
                values,
                *channels,
                sample,
                window,
                stored.map(float).transpose()?,
            )),
        };
        cells.push(cell);
    }
    Ok((cells, rows.len()))
}

/// Whether `value` supplies `sample`'s whole `column` cell, which is then
/// written without being read: every channel and product, every element
/// a visibility mask or a flag change names, and for weights every product
/// (and channel, unless `WEIGHT` is one value per correlation).
fn covers_whole(
    column: &Column,
    value: &PatchValues<'_>,
    sample: usize,
    window: &Window<'_>,
) -> bool {
    let all_products = window.products.len() == window.correlations;
    match value {
        PatchValues::Visibilities { written, .. } => {
            window.whole
                && written
                    .as_ref()
                    .is_none_or(|written| written.index_axis(Axis(0), sample).iter().all(|on| *on))
        }
        // A change with neither mask assigns nothing: every flag keeps its
        // stored value.
        PatchValues::Flags { set, clear } => {
            window.whole
                && (set.is_some() || clear.is_some())
                && (0..values_len(set, clear, sample))
                    .all(|element| flag_at(set, sample, element) || flag_at(clear, sample, element))
        }
        PatchValues::Weights { channels, .. } => {
            all_products
                && (column.name == "WEIGHT" || *channels == WeightChannels::All || window.whole)
        }
    }
}

/// Elements of one sample in a flag change: channels × products.
fn values_len(
    set: &Option<ArrayView3<'_, bool>>,
    clear: &Option<ArrayView3<'_, bool>>,
    sample: usize,
) -> usize {
    set.as_ref()
        .or(clear.as_ref())
        .map_or(0, |mask| mask.slice(ndarray::s![sample, .., ..]).len())
}

/// Whether `mask` holds true at element `element` of `sample`'s
/// `[channel, product]` cell.
fn flag_at(mask: &Option<ArrayView3<'_, bool>>, sample: usize, element: usize) -> bool {
    mask.as_ref().is_some_and(|mask| {
        let products = mask.shape()[2];
        mask[[sample, element / products, element % products]]
    })
}

/// Write `values`' sample into the `[correlation, channel]` cell, where
/// `written` holds when given.
fn overlay<T: Copy>(
    cell: &mut ArrayD<T>,
    values: &ArrayView3<'_, T>,
    written: Option<&ArrayView3<'_, bool>>,
    sample: usize,
    window: &Window<'_>,
) {
    for (channel, row) in values
        .slice(ndarray::s![sample, .., ..])
        .outer_iter()
        .enumerate()
    {
        for (product, value) in row.iter().enumerate() {
            if written.is_none_or(|written| written[[sample, channel, product]]) {
                cell[[window.products[product], window.first_channel + channel]] = *value;
            }
        }
    }
}

/// Apply a flag change to the `[correlation, channel]` cell.
fn change_flags(
    cell: &mut ArrayD<bool>,
    set: Option<&ArrayView3<'_, bool>>,
    clear: Option<&ArrayView3<'_, bool>>,
    sample: usize,
    window: &Window<'_>,
) {
    for (mask, to) in [(set, true), (clear, false)] {
        let Some(mask) = mask else { continue };
        for (channel, row) in mask
            .slice(ndarray::s![sample, .., ..])
            .outer_iter()
            .enumerate()
        {
            for (product, on) in row.iter().enumerate() {
                if *on {
                    cell[[window.products[product], window.first_channel + channel]] = to;
                }
            }
        }
    }
}

/// The weight cell of `sample`: `WEIGHT` `[correlation]` takes each
/// product's one value; `WEIGHT_SPECTRUM` `[correlation, channel]` takes the
/// delivered channels, or every channel for [`WeightChannels::All`]. The
/// cell starts from `stored` when the patch covers it partly
/// ([`covers_whole`]).
fn weight_cell(
    column: &Column,
    values: &ArrayView3<'_, f32>,
    channels: WeightChannels,
    sample: usize,
    window: &Window<'_>,
    stored: Option<ArrayD<f32>>,
) -> ArrayD<f32> {
    if column.name == "WEIGHT" {
        let mut cell = stored.unwrap_or_else(|| ArrayD::zeros(IxDyn(&[window.correlations])));
        for (product, corr) in window.products.iter().enumerate() {
            cell[[*corr]] = values[[sample, 0, product]];
        }
        return cell;
    }
    let mut cell =
        stored.unwrap_or_else(|| ArrayD::zeros(IxDyn(&[window.correlations, window.channels])));
    if channels == WeightChannels::All {
        for (product, corr) in window.products.iter().enumerate() {
            let value = values[[sample, 0, product]];
            cell.slice_mut(ndarray::s![*corr, ..]).fill(value);
        }
    } else {
        overlay(&mut cell, values, None, sample, window);
    }
    cell
}

fn complex(value: ArrayValue) -> Result<ArrayD<Complex32>, ObservationError> {
    match value {
        ArrayValue::Complex32(cell) => Ok(cell),
        _ => Err(super::invalid("a visibility cell is not complex64")),
    }
}

fn boolean(value: ArrayValue) -> Result<ArrayD<bool>, ObservationError> {
    match value {
        ArrayValue::Bool(cell) => Ok(cell),
        _ => Err(super::invalid("a FLAG cell is not boolean")),
    }
}

fn float(value: ArrayValue) -> Result<ArrayD<f32>, ObservationError> {
    match value {
        ArrayValue::Float32(cell) => Ok(cell),
        _ => Err(super::invalid("a weight cell is not float32")),
    }
}
