// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage calculated applycal rows before any MAIN or flag-version changes.

use casa_ms::MeasurementSetWriteError;

use super::*;

mod spool;
use spool::{ReplayRows, StagedRows};

/// A native task checks this before each batch. Keep the existing write
/// session's memory bound as well; either bound may require fewer rows.
pub(super) const CANCELLABLE_BATCH_ROWS: usize = 256;

pub(super) fn check(cancellation: Option<&Cancellation>) -> Result<(), ApplyExecutionError> {
    if cancellation.is_some_and(Cancellation::is_cancelled) {
        return Err(ApplyExecutionError::Interrupted);
    }
    Ok(())
}

pub(super) struct MainSnapshot {
    #[cfg(unix)]
    directory: File,
    generation: Option<u32>,
}

impl MainSnapshot {
    pub(super) fn capture(path: &Path) -> Result<Self, ApplyExecutionError> {
        Ok(Self {
            #[cfg(unix)]
            directory: File::open(path).map_err(|source| mutation_error(path, source))?,
            generation: TableWriteLock::read_modify_counter(path)
                .map_err(|source| mutation_error(path, source))?,
        })
    }

    pub(super) fn accept(
        &self,
        path: &Path,
        cancellation: &Cancellation,
    ) -> Result<TableWriteLock, ApplyExecutionError> {
        #[cfg(feature = "solve-test-hooks")]
        crate::solve_test_hooks::observe(crate::solve_test_hooks::Checkpoint::ApplyBeforeCommit);

        check(Some(cancellation))?;
        #[cfg(unix)]
        let lock = TableWriteLock::acquire_interruptibly(path, &|| cancellation.is_cancelled());
        #[cfg(not(unix))]
        let lock = TableWriteLock::acquire(path, 0);
        let lock = lock.map_err(|source| {
            if cancellation.is_cancelled() {
                ApplyExecutionError::Interrupted
            } else {
                mutation_error(path, source)
            }
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let current = std::fs::metadata(path).map_err(|source| mutation_error(path, source))?;
            let initial = self
                .directory
                .metadata()
                .map_err(|source| mutation_error(path, source))?;
            if (current.dev(), current.ino()) != (initial.dev(), initial.ino()) {
                return Err(mutation_error(
                    path,
                    "MAIN was replaced during applycal calculation",
                ));
            }
        }
        if lock
            .modify_counter()
            .map_err(|source| mutation_error(path, source))?
            != self.generation
        {
            return Err(mutation_error(
                path,
                "MAIN changed during applycal calculation",
            ));
        }
        check(Some(cancellation))?;
        Ok(lock)
    }
}

fn mutation_error(path: &Path, source: impl std::fmt::Display) -> ApplyExecutionError {
    ApplyExecutionError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: MsError::InvalidInput(source.to_string()),
    }
}

pub(super) struct ApplyStaging<'a> {
    cancellation: &'a Cancellation,
    mutations: Vec<StagedMutation>,
    initialize_corrected_data_on_error: bool,
}

struct StagedMutation {
    plan: MeasurementSetWritePlan,
    batch_rows: usize,
    rows: ReplayRows,
}

pub(super) enum ApplyMutation<'a, 'c> {
    Direct(Option<&'a mut TableWriteLock>),
    Staged(&'a mut ApplyStaging<'c>),
}

impl ApplyMutation<'_, '_> {
    pub(super) fn tool_holds_lock(&self) -> bool {
        matches!(self, Self::Direct(Some(_)))
    }

    pub(super) fn is_staged(&self) -> bool {
        matches!(self, Self::Staged(_))
    }

    pub(super) fn cancellation(&self) -> Option<&Cancellation> {
        match self {
            Self::Direct(_) => None,
            Self::Staged(stage) => Some(stage.cancellation),
        }
    }

    pub(super) fn write<T>(
        &mut self,
        ms: &mut MeasurementSet,
        plan: MeasurementSetWritePlan,
        write_error: impl Fn(MeasurementSetWriteError) -> ApplyExecutionError,
        calculate: impl FnOnce(
            &mut BatchOutput<'_>,
            &mut MeasurementSet,
        ) -> Result<T, ApplyExecutionError>,
    ) -> Result<T, ApplyExecutionError> {
        let batch_rows = plan.batch_rows().max(1);
        match self {
            Self::Direct(lock) => MeasurementSetWriteSession::mutate_selected_rows(
                ms,
                plan,
                lock.as_deref_mut(),
                &write_error,
                |writer, ms| {
                    calculate(
                        &mut BatchOutput::Direct {
                            writer,
                            batch_rows,
                            write_error: &write_error,
                        },
                        ms,
                    )
                },
            )
            .map(|(output, _)| output),
            Self::Staged(stage) => {
                stage.initialize_corrected_data_on_error |= plan.columns().iter().any(|column| {
                    column.name == VisibilityDataColumn::CorrectedData.name()
                        && column.mode == MeasurementSetColumnWriteMode::Create
                });
                let batch_rows = batch_rows.min(CANCELLABLE_BATCH_ROWS);
                let mut rows = StagedRows::default();
                let output = calculate(
                    &mut BatchOutput::Staged {
                        rows: &mut rows,
                        batch_rows,
                        cancellation: stage.cancellation,
                        _mutation: stage.mutations.len(),
                    },
                    ms,
                )?;
                let rows = rows
                    .finish()
                    .map_err(ApplyExecutionError::StageSelectedRows)?;
                stage.mutations.push(StagedMutation {
                    plan,
                    batch_rows,
                    rows,
                });
                Ok(output)
            }
        }
    }
}

pub(super) enum BatchOutput<'a> {
    Direct {
        writer: &'a mut MeasurementSetWriteSession,
        batch_rows: usize,
        write_error: &'a dyn Fn(MeasurementSetWriteError) -> ApplyExecutionError,
    },
    Staged {
        rows: &'a mut StagedRows,
        batch_rows: usize,
        cancellation: &'a Cancellation,
        _mutation: usize,
    },
}

impl BatchOutput<'_> {
    pub(super) fn batch_rows(&self) -> usize {
        match self {
            Self::Direct { batch_rows, .. } | Self::Staged { batch_rows, .. } => *batch_rows,
        }
    }

    pub(super) fn begin(&self, _batch_index: usize) -> Result<(), ApplyExecutionError> {
        if let Self::Staged {
            cancellation,
            _mutation,
            ..
        } = self
        {
            check(Some(cancellation))?;
            #[cfg(feature = "solve-test-hooks")]
            crate::solve_test_hooks::observe(
                crate::solve_test_hooks::Checkpoint::ApplyBatchStarted(*_mutation, _batch_index),
            );
        }
        Ok(())
    }

    pub(super) fn write(
        &mut self,
        ms: &mut MeasurementSet,
        batch: MeasurementSetMutationBatch,
        _batch_index: usize,
    ) -> Result<(), ApplyExecutionError> {
        match self {
            Self::Direct {
                writer,
                write_error,
                ..
            } => writer.write_mutation_batch(ms, batch).map_err(write_error),
            Self::Staged {
                rows, _mutation, ..
            } => {
                rows.push(batch)
                    .map_err(ApplyExecutionError::StageSelectedRows)?;
                #[cfg(feature = "solve-test-hooks")]
                crate::solve_test_hooks::observe(
                    crate::solve_test_hooks::Checkpoint::ApplyBatchFinished(
                        *_mutation,
                        _batch_index,
                    ),
                );
                Ok(())
            }
        }
    }
}

impl StagedMutation {
    fn publish(
        self,
        ms: &mut MeasurementSet,
        lock: &mut TableWriteLock,
    ) -> Result<(), ApplyExecutionError> {
        let Self {
            plan,
            batch_rows,
            mut rows,
        } = self;
        let path = lock.path().to_path_buf();
        let write_error = |source| mutation_error(&path, source);
        MeasurementSetWriteSession::mutate_selected_rows(
            ms,
            plan,
            Some(lock),
            write_error,
            |writer, ms| {
                loop {
                    let planned_rows = writer.next_mutation_rows().map_err(write_error)?;
                    if planned_rows.is_empty() {
                        return Ok(());
                    }
                    // Use the writer's resource bound and keep whole staging
                    // batches together. Repeated small table writes would
                    // otherwise repatch many of the same storage tiles.
                    let complete_batches = planned_rows.len() / batch_rows;
                    let count = if complete_batches == 0 {
                        planned_rows.len()
                    } else {
                        complete_batches * batch_rows
                    };
                    let batch = rows
                        .read(&planned_rows[..count], batch_rows)
                        .map_err(ApplyExecutionError::StageSelectedRows)?;
                    writer
                        .write_mutation_batch(ms, batch)
                        .map_err(write_error)?;
                }
            },
        )
        .map(|((), _)| ())
    }
}

/// Stage both normal corrections and strict flags before the same boundary.
/// Once accepted, finish backup, new-column setup and all MAIN mutations.
pub(super) fn execute(
    ms: &mut MeasurementSet,
    plan: ApplyPlan,
    input: ApplyInput<'_>,
    tracer: &mut CalibrationPerfTracer,
    options: ApplyExecutionOptions,
    (snapshot, cancellation): (MainSnapshot, &Cancellation),
) -> Result<(ApplyExecutionReport, ExecuteApplyPlanTraceSummary), ApplyExecutionError> {
    let path = ms
        .path()
        .expect("path apply uses a disk-backed MS")
        .to_path_buf();
    let mut stage = ApplyStaging {
        cancellation,
        mutations: Vec::new(),
        initialize_corrected_data_on_error: false,
    };
    let mode = plan.apply_mode;
    let result = execute_apply_plan(
        ms,
        plan,
        input,
        None,
        ApplyMutation::Staged(&mut stage),
        None,
    );
    let (mut report, mut trace) = match result {
        Ok(result) => result,
        Err(
            error @ (ApplyExecutionError::Interrupted | ApplyExecutionError::StageSelectedRows(_)),
        ) => return Err(error),
        Err(error) => {
            check(Some(cancellation))?;
            // CASA may already have backed up the flags and initialized the
            // new column before a calculation rejects the data. Keep those
            // setup effects, with no partially calculated rows.
            if stage.initialize_corrected_data_on_error
                || (options.flag_backup && mode.writes_flags())
            {
                let mut lock = snapshot.accept(&path, cancellation)?;
                save_apply_flag_backup(ms, mode, options)?;
                if stage.initialize_corrected_data_on_error {
                    initialization::corrected_data(ms, input.observed_column, Some(&mut lock))?;
                }
            }
            return Err(error);
        }
    };
    let save_started_at = Instant::now();
    {
        let mut lock = snapshot.accept(&path, cancellation)?;
        report.flag_backup_version = save_apply_flag_backup(ms, mode, options)?;
        if report.created_corrected_data_column {
            ensure_corrected_data_column_holding(ms, input.observed_column, Some(&mut lock))
                .map_err(|source| ApplyExecutionError::CreateCorrectedData {
                    path: display_ms_path(ms),
                    source,
                })?;
        }
        for mutation in stage.mutations {
            mutation.publish(ms, &mut lock)?;
        }
        lock.release()
            .map_err(|source| mutation_error(&path, source))?;
    }
    let save_ns = save_started_at.elapsed().as_nanos() as u64;
    report.timings.save_ns += save_ns;
    trace.save_ns += save_ns;
    trace.execute_apply_plan_ns += save_ns;
    tracer.emit_apply_plan_summary(&display_ms_path(ms), &report.plan, trace);
    #[cfg(feature = "solve-test-hooks")]
    crate::solve_test_hooks::observe(crate::solve_test_hooks::Checkpoint::ApplyMainCommitted);
    Ok((report, trace))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn write_main(path: &Path) {
        let schema = casa_tables::TableSchema::new(vec![ColumnSchema::scalar(
            "ID",
            casa_core::PrimitiveType::Int32,
        )])
        .unwrap();
        Table::with_schema(schema)
            .save(TableOptions::new(path))
            .unwrap();
    }

    #[test]
    fn staged_apply_refuses_a_generation_published_after_reading() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("MAIN");
        write_main(&path);
        let snapshot = MainSnapshot::capture(&path).unwrap();
        let mut writer = TableWriteLock::acquire(&path, 1).unwrap();
        writer.record_write();
        writer.release().unwrap();

        let Err(error) = snapshot.accept(&path, &Cancellation::new()) else {
            panic!("staged changes accepted against a different generation");
        };
        assert!(error.to_string().contains("MAIN changed during applycal"));
        MainSnapshot::capture(&path)
            .unwrap()
            .accept(&path, &Cancellation::new())
            .unwrap();
    }

    #[test]
    fn staged_apply_refuses_replacement_at_the_same_path() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("MAIN");
        write_main(&path);
        let snapshot = MainSnapshot::capture(&path).unwrap();
        std::fs::rename(&path, root.path().join("old")).unwrap();
        write_main(&path);
        let Err(error) = snapshot.accept(&path, &Cancellation::new()) else {
            panic!("staged changes accepted against a different table");
        };
        assert!(
            error
                .to_string()
                .contains("MAIN was replaced during applycal")
        );
    }
}
