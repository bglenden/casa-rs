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
    AccessRequest, ObservationError, PartitionEntry, PatchValues, ResolvedTarget, TimeRuns,
    UpdateReport, Updater, VisibilityBatch, WeightChannels, WritableRole, check_values,
    uniform_over_channels,
};
use casa_types::ArrayValue;
use casa_types::measures::MjdHighPrec;
use ndarray::{ArrayD, ArrayView3, IxDyn};
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
        let ms = MeasurementSet::open(path).map_err(storage)?;
        if ms.main_table().row_count() != rows {
            return Err(request_error(format!(
                "MAIN has {} rows and had {rows} when the session opened: another process \
                 changed it, so the session's grids no longer address its rows",
                ms.main_table().row_count()
            )));
        }
        self.ms = ms;
        Ok(())
    }
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

/// Where the traversal is: the request's partition being written and its
/// next grid time.
#[derive(Clone, Copy)]
struct Position {
    partition: usize,
    time: usize,
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
    position: Position,
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
        let observation = Msv2Observation::open_with(path, options)?;
        let ms = MeasurementSet::open(path).map_err(super::storage)?;
        Ok(Self {
            observation,
            path: path.to_path_buf(),
            rows: ms.main_table().row_count(),
            writer: Mutex::new(Writer {
                ms,
                lock: None,
                live: 0,
            }),
        })
    }

    pub(in crate::observation_io) fn observation(&self) -> &Msv2Observation {
        &self.observation
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
            for target in targets {
                clear(&mut writer.ms, target, None)?;
            }
        }
        writer.hold(&self.path, self.rows)?;
        let Writer { ms, lock, .. } = writer;
        let mut seeded = 0;
        for target in targets {
            clear(ms, target, lock.as_mut())?;
            add_column(ms, target)?;
            if target.created
                && target.variable == "MODEL_DATA"
                && !persisted(ms.main_table(), "MODEL_DATA")
            {
                seeded += seed_model(ms, lock, &self.observation)?;
            }
        }
        // Install the columns the session creates (CORRECTED_DATA cloned
        // from DATA) now, with a session of no rows.
        mutate(
            ms,
            lock,
            &self.observation,
            request,
            targets,
            &[],
            |_, _| Ok(()),
        )?;
        Ok(seeded)
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
    /// when it selects the data.
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
            position: Position {
                partition: 0,
                time: selections.first().and_then(TimeRuns::first).unwrap_or(0),
            },
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

/// Whether a data manager stores MAIN's `column`.
fn persisted(main: &casa_tables::Table, column: &str) -> bool {
    main.data_manager_info()
        .iter()
        .any(|manager| manager.columns.iter().any(|name| name == column))
}

/// The bytes one seeding batch may hold.
const SEED_BATCH_BYTES: usize = 64 << 20;

/// Install a `MODEL_DATA` just added to the schema and write CASA's
/// initial model ([`initial_model_visibility`]) into every row, as CASA initializes
/// the column when it adds it; returns the cells written. The storage is
/// cloned from `DATA`'s (the layout casacore is known to read, as applycal
/// installs `CORRECTED_DATA`) and every cell is then overwritten, so once
/// this returns no row of the model holds `DATA`; a pass then writes its
/// own values over the seed. Every MAIN row lies in one partition's grid.
fn seed_model(
    ms: &mut MeasurementSet,
    lock: &mut Option<TableWriteLock>,
    observation: &Msv2Observation,
) -> Result<usize, ObservationError> {
    let catalogue = observation.catalogue();
    let mut rows = Vec::new();
    let mut row_seeds = Vec::new();
    let mut seeds = Vec::with_capacity(catalogue.partitions.len());
    let mut cell_bytes = 0;
    for (index, entry) in catalogue.partitions.iter().enumerate() {
        let shape = [entry.correlations.len(), entry.frequencies_hz.len()];
        cell_bytes = cell_bytes.max(shape[0] * shape[1] * size_of::<Complex32>());
        seeds.push(ArrayValue::Complex32(ArrayD::from_shape_fn(
            IxDyn(&shape),
            |at| initial_model_visibility(entry.correlations[at[0]].casacore_code()),
        )));
        for row in observation.partitions[index].grid.cells.iter() {
            if *row != NO_ROW {
                rows.push(*row as usize);
                row_seeds.push(index);
            }
        }
    }
    let main_rows = ms.main_table().row_count();
    if rows.len() != main_rows {
        return Err(request_error(format!(
            "MAIN has {main_rows} rows and the session's grids place {}: it changed since the \
             session opened, so MODEL_DATA cannot be seeded in every row",
            rows.len()
        )));
    }
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        rows.clone(),
        vec![MeasurementSetWriteColumnPlan {
            name: "MODEL_DATA".to_string(),
            bytes_per_row: cell_bytes.max(1),
            mode: MeasurementSetColumnWriteMode::Create,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
            tile_shape: None,
            create_source_column: Some("DATA".to_string()),
        }],
        MeasurementSetWriteResources {
            available_bytes: SEED_BATCH_BYTES.max(cell_bytes),
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: casa_tables::STREAMING_TILED_COLUMN_BUFFER_BYTES,
        },
    )
    .map_err(storage)?;
    let batch_rows = plan.batch_rows().max(1);
    let held = lock.take().expect("MAIN's write lock is held");
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation_holding(ms, plan, held)
            .map_err(storage)?;
    for (batch, indices) in rows.chunks(batch_rows).zip(row_seeds.chunks(batch_rows)) {
        session
            .write_mutation_batch(
                ms,
                MeasurementSetMutationBatch {
                    row_indices: batch.to_vec(),
                    columns: vec![MeasurementSetMutationColumnBatch {
                        name: "MODEL_DATA".to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(
                            indices.iter().map(|index| seeds[*index].clone()).collect(),
                        ),
                    }],
                },
            )
            .map_err(storage)?;
    }
    let (_, held) = session.finish_mutation_holding().map_err(storage)?;
    *lock = Some(held);
    Ok(rows.len())
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
    let create = target.created && !persisted(main, &target.variable);
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
        let roles: Vec<WritableRole> = self.columns.iter().map(|column| column.role).collect();
        check_values(batch, &roles, values)?;
        let observation = &self.update.observation;
        if !Arc::ptr_eq(batch.catalogue, observation.catalogue()) {
            return Err(request_error(
                "the batch was not delivered by this session's observation",
            ));
        }
        let first = self.place(batch)?;
        let grid = &observation.partitions[batch.partition.0 as usize].grid;
        let cells = grid.cells(first..first + batch.times());
        let entry = batch.catalogue.partition(batch.partition);
        let products = product_indices(batch, entry)?;
        let window = Window {
            first_channel: batch.first_channel,
            channels: entry.frequencies_hz.len(),
            products: &products,
            correlations: entry.correlations.len(),
            whole: batch.channels() == (0..entry.frequencies_hz.len())
                && products.len() == entry.correlations.len(),
        };
        for (column, value) in self.columns.iter().zip(values) {
            refuse_unaddressable(column, value, cells, &window)?;
        }
        let present: Vec<(usize, usize)> = cells
            .iter()
            .enumerate()
            .filter(|(_, row)| **row != NO_ROW)
            .map(|(sample, row)| (sample, *row as usize))
            .collect();
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
        self.report.unaddressed += cells.len() - present.len();
        self.report.units_written += present.len() * self.columns.len();
        self.position.time = first + batch.times();
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<UpdateReport, ObservationError> {
        let unwritten = self
            .selections
            .iter()
            .enumerate()
            .any(
                |(partition, selection)| match partition.cmp(&self.position.partition) {
                    std::cmp::Ordering::Less => false,
                    std::cmp::Ordering::Equal => selection.next_from(self.position.time).is_some(),
                    std::cmp::Ordering::Greater => !selection.is_empty(),
                },
            );
        if unwritten {
            return Err(request_error(
                "the traversal was not written batch for batch: selected times remain unwritten",
            ));
        }
        let report = self.report;
        self.lease.end()?;
        Ok(report)
    }
}

impl Msv2Updater<'_> {
    /// `batch`'s first grid time, after checking it is the traversal's
    /// next batch: the same partition from the next time, or, once its
    /// selected times are all written, the next partition from its first
    /// selected time, passing partitions whose selection holds no time
    /// (they deliver no batch).
    fn place(&mut self, batch: &VisibilityBatch<'_>) -> Result<usize, ObservationError> {
        let partitions = &self.request.partitions;
        let observation = &self.update.observation;
        let selections = &self.selections;
        let mut position = self.position;
        let done = |position: Position| {
            selections[position.partition]
                .next_from(position.time)
                .is_none()
        };
        let passes = |partition: usize| {
            partitions[partition] != batch.partition && selections[partition].is_empty()
        };
        if partitions.get(position.partition) != Some(&batch.partition)
            || (done(position) && position.partition + 1 < partitions.len())
        {
            if !done(position) {
                return Err(request_error(
                    "the batch is not the traversal's next: an earlier partition's selected \
                     times are not all written",
                ));
            }
            let mut next = position.partition + 1;
            while next + 1 < partitions.len() && passes(next) {
                next += 1;
            }
            position = Position {
                partition: next,
                time: selections.get(next).and_then(TimeRuns::first).unwrap_or(0),
            };
        }
        if partitions.get(position.partition) != Some(&batch.partition)
            || batch.channels() != self.request.channels
        {
            return Err(request_error(
                "the batch is not the traversal's next: an updater is written batch for batch \
                 in delivery order",
            ));
        }
        let grid = &observation.partitions[batch.partition.0 as usize].grid;
        let time = batch.time.seconds_since(0, MjdHighPrec::new(0.0, 0.0));
        let first = grid
            .times_s
            .binary_search_by(|known| known.total_cmp(&time))
            .map_err(|_| request_error("the batch's first time is not on the partition's grid"))?;
        let selection = &selections[position.partition];
        if selection.next_from(position.time) != Some(first)
            || !selection.holds(first..first + batch.times())
        {
            return Err(request_error(
                "the batch is not the traversal's next: an updater is written batch for batch \
                 in delivery order",
            ));
        }
        self.position = position;
        Ok(first)
    }
}

/// The delivered products' positions among the partition's.
fn product_indices(
    batch: &VisibilityBatch<'_>,
    entry: &PartitionEntry,
) -> Result<Vec<usize>, ObservationError> {
    batch
        .correlations
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
/// many cells were read to merge into.
fn column_cells(
    ms: &MeasurementSet,
    column: &Column,
    value: &PatchValues<'_>,
    present: &[(usize, usize)],
    window: &Window<'_>,
) -> Result<(Cells, usize), ObservationError> {
    let mut read = 0;
    let mut stored = |row: usize| -> Result<ArrayValue, ObservationError> {
        read += 1;
        ms.main_table()
            .cell_accessor(row, &column.name)
            .and_then(|cell| cell.array())
            .cloned()
            .map_err(|error| super::storage(crate::MsError::from(error)))
    };
    let shape = [window.correlations, window.channels];
    let mut cells = Vec::with_capacity(present.len());
    for &(sample, row) in present {
        let cell = match value {
            PatchValues::Visibilities(values) => {
                let mut cell = if window.whole {
                    ArrayD::from_elem(IxDyn(&shape), Complex32::default())
                } else {
                    complex(stored(row)?)?
                };
                overlay(&mut cell, values, sample, window);
                ArrayValue::Complex32(cell)
            }
            PatchValues::Flags { set, clear } => {
                // A change with neither mask assigns nothing: every flag
                // keeps its stored value.
                let assigns_all = window.whole
                    && (set.is_some() || clear.is_some())
                    && (0..values_len(set, clear, sample)).all(|element| {
                        flag_at(set, sample, element) || flag_at(clear, sample, element)
                    });
                let mut cell = if assigns_all {
                    ArrayD::from_elem(IxDyn(&shape), false)
                } else {
                    boolean(stored(row)?)?
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
                || stored(row).and_then(float),
            )?),
        };
        cells.push(cell);
    }
    Ok((cells, read))
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

/// Write `values`' sample into the `[correlation, channel]` cell.
fn overlay<T: Copy>(
    cell: &mut ArrayD<T>,
    values: &ArrayView3<'_, T>,
    sample: usize,
    window: &Window<'_>,
) {
    for (channel, row) in values
        .slice(ndarray::s![sample, .., ..])
        .outer_iter()
        .enumerate()
    {
        for (product, value) in row.iter().enumerate() {
            cell[[window.products[product], window.first_channel + channel]] = *value;
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
/// delivered channels, or every channel for [`WeightChannels::All`].
fn weight_cell(
    column: &Column,
    values: &ArrayView3<'_, f32>,
    channels: WeightChannels,
    sample: usize,
    window: &Window<'_>,
    stored: impl FnOnce() -> Result<ArrayD<f32>, ObservationError>,
) -> Result<ArrayD<f32>, ObservationError> {
    let all_products = window.products.len() == window.correlations;
    if column.name == "WEIGHT" {
        let mut cell = if all_products {
            ArrayD::zeros(IxDyn(&[window.correlations]))
        } else {
            stored()?
        };
        for (product, corr) in window.products.iter().enumerate() {
            cell[[*corr]] = values[[sample, 0, product]];
        }
        return Ok(cell);
    }
    let every_channel = channels == WeightChannels::All || window.whole;
    let mut cell = if every_channel && all_products {
        ArrayD::zeros(IxDyn(&[window.correlations, window.channels]))
    } else {
        stored()?
    };
    if channels == WeightChannels::All {
        for (product, corr) in window.products.iter().enumerate() {
            let value = values[[sample, 0, product]];
            cell.slice_mut(ndarray::s![*corr, ..]).fill(value);
        }
    } else {
        overlay(&mut cell, values, sample, window);
    }
    Ok(cell)
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
