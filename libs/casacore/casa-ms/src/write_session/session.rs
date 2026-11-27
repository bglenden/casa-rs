// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl MeasurementSetWriteSession {
    /// Start the physical column writers described by `plan`.
    pub fn start(
        output: impl AsRef<Path>,
        plan: MeasurementSetWritePlan,
    ) -> Result<Self, MeasurementSetWriteError> {
        if plan.operation != MeasurementSetWriteOperation::Create {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "background creation session requires a create plan".to_string(),
            ));
        }
        let output = output.as_ref().to_path_buf();
        if !plan.array_columns.is_empty()
            || (plan.correlation_count == 0 && plan.channel_count == 0)
        {
            let mut writers = HashMap::with_capacity(plan.array_columns.len());
            for column in &plan.array_columns {
                let mut writer = StreamingTiledShapeWriter::create(
                    output.join(format!(".casa-rs.{}.table.f.tmp", column.name)),
                    column.value_type,
                    column.layouts(),
                    plan.array_column_memory_budget_bytes,
                    false,
                )
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                if let Some(default) = plan
                    .columns
                    .iter()
                    .find(|plan| plan.name == column.name)
                    .and_then(|plan| plan.tile_shape.as_ref())
                {
                    writer
                        .set_default_tile_shape(default.clone())
                        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                }
                writers.insert(column.name.clone(), writer);
            }
            let scalar_writers =
                ScalarWriters::create(&output, plan.row_count, &plan.scalar_columns)?;
            return Ok(Self {
                plan,
                state: MeasurementSetWriteSessionState::VariableArrayCreation {
                    output,
                    writers,
                    scalar_writers,
                    started_at: Instant::now(),
                },
            });
        }
        let row_count = plan.row_count;
        let correlation_count = plan.correlation_count;
        let channel_count = plan.channel_count;
        let visibility_tile_shape = plan.visibility_tile_shape.clone();
        let create_data_writer = if plan.data_bands > 1 {
            StreamingTiledShapeComplex32Writer::create_overlapped
        } else {
            StreamingTiledShapeComplex32Writer::create
        };
        let data_writer = create_data_writer(
            output.join(".casa-rs.DATA.table.f.tmp"),
            row_count,
            vec![correlation_count, channel_count],
            visibility_tile_shape.clone(),
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let flag_writer = StreamingTiledPrimitiveWriter::create_shape(
            output.join(".casa-rs.FLAG.table.f.tmp"),
            row_count,
            vec![correlation_count, channel_count],
            visibility_tile_shape,
            StreamedTiledPrimitiveType::Bool,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let uvw_writer = StreamingTiledPrimitiveWriter::create_column(
            output.join(".casa-rs.UVW.table.f.tmp"),
            row_count,
            vec![3],
            plan.uvw_tile_shape.clone(),
            StreamedTiledPrimitiveType::Float64,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let weight_writer = StreamingTiledPrimitiveWriter::create_shape(
            output.join(".casa-rs.WEIGHT.table.f.tmp"),
            row_count,
            vec![correlation_count],
            plan.weight_tile_shape.clone(),
            StreamedTiledPrimitiveType::Float32,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let sigma_writer = StreamingTiledPrimitiveWriter::create_shape(
            output.join(".casa-rs.SIGMA.table.f.tmp"),
            row_count,
            vec![correlation_count],
            plan.weight_tile_shape.clone(),
            StreamedTiledPrimitiveType::Float32,
            plan.tiled_column_buffer_bytes,
            false,
        )
        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let (sender, receiver) = mpsc::sync_channel(plan.queue_capacity);
        let (returns, recycled) = mpsc::sync_channel(1);
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let handle = thread::spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                write_visibility_batches(
                    receiver,
                    &returns,
                    data_writer,
                    flag_writer,
                    uvw_writer,
                    weight_writer,
                    sigma_writer,
                    correlation_count,
                )
            })
        });
        let scalar_writers = ScalarWriters::create(&output, plan.row_count, &plan.scalar_columns)?;
        Ok(Self {
            plan,
            state: MeasurementSetWriteSessionState::Creation {
                output,
                sender,
                handle,
                sent_rows: AtomicUsize::new(0),
                scalar_writers,
                queue_wait_nanos: AtomicU64::new(0),
                started_at: Instant::now(),
                recycled,
            },
        })
    }

    /// Append one boolean variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_bool_row(
        &mut self,
        column: &str,
        cell_shape: &[usize],
        values: &[bool],
    ) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_bool_row(cell_shape, values)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one float32 variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_f32_row(
        &mut self,
        column: &str,
        cell_shape: &[usize],
        values: &[f32],
    ) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_f32_row(cell_shape, values)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one float64 variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_f64_row(
        &mut self,
        column: &str,
        cell_shape: &[usize],
        values: &[f64],
    ) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_f64_row(cell_shape, values)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one complex32 variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_complex32_row(
        &mut self,
        column: &str,
        cell_shape: &[usize],
        values: &[Complex32],
    ) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_complex32_row(cell_shape, values)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Append one undefined variable-array cell in output row order.
    #[doc(hidden)]
    pub fn push_undefined_row(&mut self, column: &str) -> Result<(), MeasurementSetWriteError> {
        self.variable_array_writer_mut(column)?
            .push_undefined_row()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))
    }

    /// Accept a typed batch, splitting it at the immutable plan boundary.
    pub(crate) fn send_batch(
        &self,
        batch: MeasurementSetWriteBatch,
    ) -> Result<(), MeasurementSetWriteError> {
        let MeasurementSetWriteSessionState::Creation {
            sender,
            sent_rows,
            queue_wait_nanos,
            ..
        } = &self.state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "visibility batches require a creation session".to_string(),
            ));
        };
        let sample_count = self
            .plan
            .correlation_count
            .checked_mul(self.plan.channel_count)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        match &batch {
            MeasurementSetWriteBatch::Columns(columns) => columns.validate(sample_count)?,
            #[cfg(test)]
            MeasurementSetWriteBatch::Repeated { data_row, .. } => {
                if data_row.len() != sample_count {
                    return Err(MeasurementSetWriteError::InvalidPlan(format!(
                        "repeated DATA row has {} samples; plan requires {sample_count}",
                        data_row.len()
                    )));
                }
            }
        }
        let batch_rows = batch.row_count();
        if batch_rows > self.plan.batch_rows {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "visibility batch has {batch_rows} rows; plan permits at most {}",
                self.plan.batch_rows
            )));
        }
        sent_rows
            .try_update(Ordering::AcqRel, Ordering::Acquire, |sent| {
                sent.checked_add(batch_rows)
                    .filter(|total| *total <= self.plan.row_count)
            })
            .map_err(|sent| {
                MeasurementSetWriteError::InvalidPlan(format!(
                    "write would exceed planned row count {} (already sent {sent}, batch {batch_rows})",
                    self.plan.row_count
                ))
            })?;
        let queue_wait_started = Instant::now();
        let result = sender.send(batch);
        queue_wait_nanos.fetch_add(
            duration_nanos_u64(queue_wait_started.elapsed()),
            Ordering::Relaxed,
        );
        result.map_err(|error| MeasurementSetWriteError::Background(error.to_string()))
    }

    /// Finalize and install all columns into an already-saved MAIN descriptor.
    pub fn finish(
        self,
        main: &casa_tables::Table,
    ) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        let MeasurementSetWriteSession { plan, state } = self;
        match state {
            MeasurementSetWriteSessionState::Creation {
                output,
                sender,
                handle,
                sent_rows,
                scalar_writers,
                queue_wait_nanos,
                started_at,
                recycled: _,
            } => {
                if !scalar_writers.is_empty() {
                    return Err(MeasurementSetWriteError::InvalidPlan(
                        "scalar creation columns must be finalized with the MAIN descriptor"
                            .to_string(),
                    ));
                }
                let producer_window_seconds = started_at.elapsed().as_secs_f64();
                let finalize_started = Instant::now();
                drop(sender);
                let streamed = handle.join().map_err(|_| {
                    MeasurementSetWriteError::Background("writer thread panicked".to_string())
                })??;
                let actual_rows = sent_rows.load(Ordering::Acquire);
                if actual_rows != plan.row_count {
                    return Err(MeasurementSetWriteError::InvalidPlan(format!(
                        "writer received {actual_rows} rows; plan requires {}",
                        plan.row_count
                    )));
                }
                let telemetry = telemetry(&streamed);
                install_streamed_columns(main, &output, &plan, streamed)?;
                Ok(complete_telemetry(
                    telemetry,
                    &plan,
                    producer_window_seconds,
                    queue_wait_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
                    finalize_started.elapsed().as_secs_f64(),
                ))
            }
            MeasurementSetWriteSessionState::VariableArrayCreation {
                output,
                mut writers,
                scalar_writers,
                started_at,
            } => {
                if !scalar_writers.is_empty() {
                    return Err(MeasurementSetWriteError::InvalidPlan(
                        "scalar creation columns must be finalized with the MAIN descriptor"
                            .to_string(),
                    ));
                }
                let producer_window_seconds = started_at.elapsed().as_secs_f64();
                let finalize_started = Instant::now();
                let mut columns = Vec::with_capacity(plan.array_columns.len());
                for column in &plan.array_columns {
                    let writer = writers.remove(&column.name).ok_or_else(|| {
                        MeasurementSetWriteError::InvalidPlan(format!(
                            "missing variable-array writer for {}",
                            column.name
                        ))
                    })?;
                    let streamed = writer
                        .finish()
                        .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                    let sequence =
                        crate::ms::measurement_set_main_data_manager_sequence(main, &column.name)
                            .ok_or_else(|| MeasurementSetWriteError::Install {
                            column: column.name.clone(),
                            reason: "data-manager sequence is absent".to_string(),
                        })?;
                    let column_telemetry = column_telemetry(
                        &column.name,
                        streamed.assemble_seconds(),
                        streamed.write_seconds(),
                        streamed.bytes_written(),
                    );
                    match column.storage_manager {
                        MeasurementSetColumnStorage::TiledColumn => {
                            install_streamed_tiled_column(&output, sequence, &column.name, streamed)
                        }
                        MeasurementSetColumnStorage::TiledShape => {
                            install_streamed_tiled_shape_column(
                                &output,
                                sequence,
                                &column.name,
                                streamed,
                            )
                        }
                        other => Err(casa_tables::StorageError::FormatMismatch(format!(
                            "array creation column {} has unsupported storage manager {other:?}",
                            column.name
                        ))),
                    }
                    .map_err(|error| install_error(&column.name, error))?;
                    columns.push(column_telemetry);
                }
                Ok(complete_telemetry(
                    MeasurementSetWriteTelemetry {
                        assemble_seconds: columns
                            .iter()
                            .map(|column| column.assemble_seconds)
                            .sum(),
                        write_seconds: columns.iter().map(|column| column.write_seconds).sum(),
                        bytes_written: columns.iter().map(|column| column.bytes_written).sum(),
                        columns,
                        rows_written: 0,
                        maximum_resident_bytes: 0,
                        producer_seconds: 0.0,
                        queue_wait_seconds: 0.0,
                        finalize_seconds: 0.0,
                    },
                    &plan,
                    producer_window_seconds,
                    0.0,
                    finalize_started.elapsed().as_secs_f64(),
                ))
            }
            MeasurementSetWriteSessionState::Mutation { .. } => {
                Err(MeasurementSetWriteError::InvalidPlan(
                    "creation finalization requires a creation session".to_string(),
                ))
            }
        }
    }

    /// Save a prepared descriptor and install all session-owned creation columns.
    #[doc(hidden)]
    pub fn save_and_finish(
        self,
        measurement_set: &mut MeasurementSet,
        column_overrides: &ColumnOverrides,
    ) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        self.save_and_finish_with_subtables(
            measurement_set,
            column_overrides,
            &SubtableColumnOverrides::new(),
        )
    }

    /// [`save_and_finish`](Self::save_and_finish), also writing the
    /// subtable columns `subtables` overrides, as
    /// [`MeasurementSet::save_with_column_overrides`] does: a creator's
    /// generated subtable, such as a simulator's POINTING, is written in the
    /// same save as MAIN without its rows held in memory.
    #[doc(hidden)]
    pub fn save_and_finish_with_subtables(
        mut self,
        measurement_set: &mut MeasurementSet,
        column_overrides: &ColumnOverrides,
        subtables: &SubtableColumnOverrides,
    ) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        let scalar_finalize_started = Instant::now();
        let scalar_columns = self.finish_scalar_columns(column_overrides)?;
        let scalar_finalize_seconds = scalar_finalize_started.elapsed().as_secs_f64();
        let save_started = Instant::now();
        measurement_set
            .save_with_column_overrides(&scalar_columns.overrides, subtables)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let save_seconds = save_started.elapsed().as_secs_f64();
        let telemetry = self.finish(measurement_set.main_table())?;
        complete_scalar_finalization(
            telemetry,
            scalar_columns,
            scalar_finalize_seconds,
            save_seconds,
        )
    }

    /// Flush staged scalar columns, save MAIN with its storage bindings and return the final writer telemetry.
    pub fn save_table_and_finish(
        mut self,
        main: &mut Table,
        options: TableOptions,
        bindings: &HashMap<String, ColumnBinding>,
        column_overrides: &ColumnOverrides,
    ) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        let scalar_finalize_started = Instant::now();
        let scalar_columns = self.finish_scalar_columns(column_overrides)?;
        let scalar_finalize_seconds = scalar_finalize_started.elapsed().as_secs_f64();
        let save_started = Instant::now();
        main.prepare_write()
            .save_with_column_overrides(options, bindings, &scalar_columns.overrides)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        let save_seconds = save_started.elapsed().as_secs_f64();
        let telemetry = self.finish(main)?;
        complete_scalar_finalization(
            telemetry,
            scalar_columns,
            scalar_finalize_seconds,
            save_seconds,
        )
    }

    fn finish_scalar_columns(
        &mut self,
        column_overrides: &ColumnOverrides,
    ) -> Result<ScalarColumnFinalization, MeasurementSetWriteError> {
        let mut overrides = column_overrides.clone();
        let scalar_writers = match &mut self.state {
            MeasurementSetWriteSessionState::Creation { scalar_writers, .. }
            | MeasurementSetWriteSessionState::VariableArrayCreation { scalar_writers, .. } => {
                scalar_writers
            }
            MeasurementSetWriteSessionState::Mutation { .. }
                if self.plan.scalar_columns.is_empty() =>
            {
                return Ok(ScalarColumnFinalization::empty(overrides));
            }
            MeasurementSetWriteSessionState::Mutation { .. } => {
                return Err(MeasurementSetWriteError::InvalidPlan(
                    "scalar columns require a creation session".to_string(),
                ));
            }
        };
        let mut writers = std::mem::take(scalar_writers);
        let mut paths = Vec::with_capacity(self.plan.scalar_columns.len());
        let mut telemetry = Vec::with_capacity(self.plan.scalar_columns.len());
        let mut bytes_written = 0usize;
        for column in &self.plan.scalar_columns {
            if overrides.contains_key(&column.name) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "scalar creation column {} also has an external override",
                    column.name
                )));
            }
            let streamed = writers.take(&column.name).ok_or_else(|| {
                MeasurementSetWriteError::InvalidPlan(format!(
                    "missing scalar writer for {}",
                    column.name
                ))
            })?;
            let streamed = streamed
                .finish()
                .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
            let column_bytes = self
                .plan
                .row_count
                .checked_mul(column.bytes_per_row())
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            bytes_written = bytes_written
                .checked_add(column_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            paths.push(streamed.path().to_path_buf());
            overrides.insert_streamed_scalar(&column.name, streamed);
            telemetry.push(column_telemetry(&column.name, 0.0, 0.0, column_bytes));
        }
        Ok(ScalarColumnFinalization {
            overrides,
            paths,
            telemetry,
            bytes_written,
        })
    }

    fn variable_array_writer_mut(
        &mut self,
        column: &str,
    ) -> Result<&mut StreamingTiledShapeWriter, MeasurementSetWriteError> {
        let MeasurementSetWriteSessionState::VariableArrayCreation { writers, .. } =
            &mut self.state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "variable-array cells require a variable-array creation session".to_string(),
            ));
        };
        writers.get_mut(column).ok_or_else(|| {
            MeasurementSetWriteError::InvalidPlan(format!(
                "column {column} is not owned by this variable-array session"
            ))
        })
    }

    /// Start a bounded selected-row mutation of `measurement_set` in place.
    ///
    /// The session takes casacore's write lock on MAIN before it changes
    /// anything, and holds it until [`finish_mutation`](Self::finish_mutation)
    /// or until the session is dropped. When another process holds a lock on
    /// MAIN, read or write, the session waits for it as casacore does,
    /// registered in the lock file's request list so that a casacore holder
    /// using `AutoLocking` releases it at its next inspection; the wait is
    /// logged. Another writer is excluded while the session lives: a casacore
    /// or casa-rs process waits, and another handle in this process and a
    /// [`MeasurementSet::save`] of any handle, this one included, are refused
    /// at once; the session persists its own batches. `measurement_set` must
    /// be opened without table locking ([`MeasurementSet::open`]).
    ///
    /// # Errors
    ///
    /// [`MeasurementSetWriteError::WriteLock`] when another handle in this
    /// process holds the lock, when another process wrote MAIN while the
    /// session waited for it, or when the lock cannot be taken.
    /// [`MeasurementSetWriteError::Install`] when a column to create, or the
    /// column it clones, is not in MAIN's schema, found before the lock is
    /// taken so nothing is published, or when an installation fails; MAIN is
    /// then published as persisted, with the columns installed before it.
    #[doc(hidden)]
    pub fn start_selected_row_mutation(
        measurement_set: &mut MeasurementSet,
        plan: MeasurementSetWritePlan,
    ) -> Result<Self, MeasurementSetWriteError> {
        Self::start_mutation(measurement_set, plan, None, None)
    }

    /// Start a selected-row mutation, as
    /// [`start_selected_row_mutation`](Self::start_selected_row_mutation)
    /// does, under `write_lock`, casacore's write lock on MAIN the caller
    /// already holds: a writer of several sessions in turn keeps other
    /// processes out between them. The caller takes the lock back with
    /// [`finish_mutation_holding`](Self::finish_mutation_holding); a session
    /// that fails or is dropped releases it, publishing what was written.
    ///
    /// # Errors
    ///
    /// As [`start_selected_row_mutation`](Self::start_selected_row_mutation),
    /// and [`MeasurementSetWriteError::InvalidPlan`] when `write_lock` is not
    /// on `measurement_set`'s MAIN.
    #[doc(hidden)]
    pub fn start_selected_row_mutation_holding(
        measurement_set: &mut MeasurementSet,
        plan: MeasurementSetWritePlan,
        write_lock: TableWriteLock,
    ) -> Result<Self, MeasurementSetWriteError> {
        Self::start_mutation(measurement_set, plan, Some(write_lock), None)
    }

    /// Use a write lock retained by a tool, and leave it held even on error.
    #[doc(hidden)]
    pub fn mutate_selected_rows_with_lock(
        measurement_set: &mut MeasurementSet,
        plan: MeasurementSetWritePlan,
        lock: &mut TableWriteLock,
        mutate: impl FnOnce(&mut Self, &mut MeasurementSet) -> Result<(), MeasurementSetWriteError>,
    ) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        Self::mutate_selected_rows(measurement_set, plan, Some(lock), |error| error, mutate)
            .map(|((), telemetry)| telemetry)
    }

    /// Write selected rows with a retained MAIN lock or acquire the lock for
    /// this session. The caller's lock remains held even if a batch fails.
    #[doc(hidden)]
    pub fn mutate_selected_rows<T, E>(
        measurement_set: &mut MeasurementSet,
        plan: MeasurementSetWritePlan,
        lock: Option<&mut TableWriteLock>,
        write_error: impl Fn(MeasurementSetWriteError) -> E,
        mutate: impl FnOnce(&mut Self, &mut MeasurementSet) -> Result<T, E>,
    ) -> Result<(T, MeasurementSetWriteTelemetry), E> {
        let retained = lock.is_some();
        let mut session =
            Self::start_mutation(measurement_set, plan, None, lock).map_err(&write_error)?;
        let output = mutate(&mut session, measurement_set)?;
        let telemetry = if retained {
            session.complete_mutation().map(|(telemetry, _)| telemetry)
        } else {
            session.finish_mutation()
        }
        .map_err(write_error)?;
        Ok((output, telemetry))
    }

    fn start_mutation(
        measurement_set: &mut MeasurementSet,
        plan: MeasurementSetWritePlan,
        held: Option<TableWriteLock>,
        borrowed: Option<&mut TableWriteLock>,
    ) -> Result<Self, MeasurementSetWriteError> {
        if plan.operation != MeasurementSetWriteOperation::SelectedRowMutation {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row session requires a mutation plan".to_string(),
            ));
        }
        if plan.selected_rows.len() != plan.row_count {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mapping does not match planned row count".to_string(),
            ));
        }
        if plan
            .selected_rows
            .iter()
            .any(|row| *row >= measurement_set.row_count())
        {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mapping contains a row outside MAIN".to_string(),
            ));
        }
        let Some(path) = measurement_set.path() else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation requires a disk-backed MeasurementSet".to_string(),
            ));
        };
        #[cfg(unix)]
        if measurement_set.main_table().lock_options().is_some() {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "selected-row mutation takes casacore's write lock itself and needs a \
                 MeasurementSet opened without table locking"
                    .to_string(),
            ));
        }
        // Check what can be checked before the lock is taken, so that a plan
        // whose columns cannot be installed publishes nothing.
        let main = measurement_set.main_table();
        let in_schema = |name: &str| {
            main.schema()
                .is_some_and(|schema| schema.contains_column(name))
        };
        for column in &plan.columns {
            let persisted = main
                .data_manager_info()
                .iter()
                .any(|manager| manager.columns.iter().any(|name| name == &column.name));
            if column.mode != MeasurementSetColumnWriteMode::Create || persisted {
                continue;
            }
            let missing = std::iter::once(column.name.as_str())
                .chain(column.create_source_column.as_deref())
                .find(|name| !in_schema(name));
            if let Some(missing) = missing {
                return Err(MeasurementSetWriteError::Install {
                    column: column.name.clone(),
                    reason: format!("column \"{missing}\" does not exist in schema"),
                });
            }
        }
        let target = canonical_measurement_set_path(path)?;
        let mut write_lock = held;
        if write_lock.is_none() && borrowed.is_none() {
            write_lock = Some(TableWriteLock::acquire(path, 0).map_err(|source| {
                MeasurementSetWriteError::WriteLock {
                    path: path.display().to_string(),
                    source,
                }
            })?);
        }
        let active_lock = borrowed
            .or(write_lock.as_mut())
            .expect("mutation holds MAIN");
        if canonical_measurement_set_path(active_lock.path())? != target {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "the write lock held on {} is not on MAIN of {}",
                active_lock.path().display(),
                path.display()
            )));
        }
        for column in &plan.columns {
            if column.mode == MeasurementSetColumnWriteMode::Create {
                let already_persisted = measurement_set
                    .main_table()
                    .data_manager_info()
                    .iter()
                    .any(|manager| manager.columns.iter().any(|name| name == &column.name));
                if !already_persisted {
                    // Each installation is persisted at once. Recording a
                    // write before each one means that when an installation
                    // fails, the lock still publishes the columns installed
                    // before it, and any part of this one, as persisted.
                    active_lock.record_write();
                    if let Some(source) = &column.create_source_column {
                        measurement_set
                            .main_table_mut()
                            .prepare_write()
                            .add_tiled_column_clone(
                                source,
                                &column.name,
                                &format!("Tiled{}", column.name),
                            )
                    } else {
                        measurement_set
                            .main_table_mut()
                            .prepare_write()
                            .add_tiled_shape_column(&column.name, &[], column.tile_shape.as_deref())
                    }
                    .map_err(|error| MeasurementSetWriteError::Install {
                        column: column.name.clone(),
                        reason: error.to_string(),
                    })?;
                }
            }
        }
        active_lock.record_write();
        Ok(Self {
            plan,
            state: MeasurementSetWriteSessionState::Mutation {
                write_lock,
                target,
                next_selected_row: 0,
                write_seconds: 0.0,
                bytes_written: 0,
                column_bytes_written: HashMap::new(),
                started_at: Instant::now(),
            },
        })
    }

    /// Persist one typed mutation batch and release it from the table cache.
    ///
    /// `measurement_set` must be the MeasurementSet the session was started
    /// on, the one whose write lock it holds: a handle on another
    /// MeasurementSet is refused with
    /// [`MeasurementSetWriteError::ForeignMeasurementSet`] before anything is
    /// written.
    #[doc(hidden)]
    pub fn write_mutation_batch(
        &mut self,
        measurement_set: &mut MeasurementSet,
        batch: MeasurementSetMutationBatch,
    ) -> Result<(), MeasurementSetWriteError> {
        let MeasurementSetWriteSessionState::Mutation {
            target,
            next_selected_row,
            write_seconds,
            bytes_written,
            column_bytes_written,
            ..
        } = &mut self.state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "mutation batches require a selected-row session".to_string(),
            ));
        };
        let given = measurement_set
            .path()
            .map(canonical_measurement_set_path)
            .transpose()?;
        if given.as_ref() != Some(target) {
            return Err(MeasurementSetWriteError::ForeignMeasurementSet {
                session: target.display().to_string(),
                batch: given.map_or_else(
                    || "without a path".to_string(),
                    |path| path.display().to_string(),
                ),
            });
        }
        if batch.row_indices.is_empty() {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "mutation batch must contain at least one row".to_string(),
            ));
        }
        if batch.row_indices.len() > self.plan.batch_rows {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation batch has {} rows; plan permits {}",
                batch.row_indices.len(),
                self.plan.batch_rows
            )));
        }
        let end = next_selected_row
            .checked_add(batch.row_indices.len())
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        if self.plan.selected_rows.get(*next_selected_row..end) != Some(&batch.row_indices) {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation batch rows do not match planned rows [{}..{end})",
                *next_selected_row
            )));
        }
        if batch.columns.len() != self.plan.columns.len() {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation batch has {} columns; plan requires {}",
                batch.columns.len(),
                self.plan.columns.len()
            )));
        }

        let plan_columns = self
            .plan
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column))
            .collect::<HashMap<_, _>>();
        let mut seen = HashSet::with_capacity(batch.columns.len());
        let mut batch_bytes = 0usize;
        let mut columns_bytes = Vec::with_capacity(batch.columns.len());
        for column in &batch.columns {
            if !seen.insert(column.name.as_str()) {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "mutation batch repeats column {}",
                    column.name
                )));
            }
            let plan_column = plan_columns.get(column.name.as_str()).ok_or_else(|| {
                MeasurementSetWriteError::InvalidPlan(format!(
                    "mutation batch column {} is absent from the plan",
                    column.name
                ))
            })?;
            if column.values.len() != batch.row_indices.len() {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "mutation column {} has {} values for {} rows",
                    column.name,
                    column.values.len(),
                    batch.row_indices.len()
                )));
            }
            let column_bytes = mutation_values_bytes(&column.values)?;
            let maximum_column_bytes = plan_column
                .bytes_per_row
                .checked_mul(batch.row_indices.len())
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            if column_bytes > maximum_column_bytes {
                return Err(MeasurementSetWriteError::InvalidPlan(format!(
                    "mutation column {} requires {column_bytes} bytes; batch plan permits {maximum_column_bytes}",
                    column.name
                )));
            }
            batch_bytes = batch_bytes
                .checked_add(column_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
            columns_bytes.push((column.name.clone(), column_bytes));
        }
        if batch_bytes > self.plan.batch_bytes {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation batch requires {batch_bytes} bytes; plan permits {}",
                self.plan.batch_bytes
            )));
        }

        let write_started = Instant::now();
        let column_names = columns_bytes
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        let mut writer = measurement_set
            .main_table_mut()
            .row_accessor_mut()
            .prepare(&column_names)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        // The batch's values move into the table's rows: a batch is written
        // once, so no cell is copied.
        for column in batch.columns {
            let slot = writer.column_index(&column.name).ok_or_else(|| {
                MeasurementSetWriteError::Column(format!(
                    "prepared mutation slot missing for {}",
                    column.name
                ))
            })?;
            match column.values {
                MeasurementSetMutationColumnValues::Scalars(values) => {
                    for (&row, value) in batch.row_indices.iter().zip(values) {
                        writer
                            .seek(row)
                            .and_then(|()| writer.set_value_at(slot, Value::Scalar(value)))
                            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                    }
                }
                MeasurementSetMutationColumnValues::Arrays(values) => {
                    for (&row, value) in batch.row_indices.iter().zip(values) {
                        writer
                            .seek(row)
                            .and_then(|()| writer.set_value_at(slot, Value::Array(value)))
                            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
                    }
                }
            }
        }
        writer
            .flush()
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        drop(writer);
        measurement_set
            .main_table_mut()
            .prepare_write()
            .save_selected_rows(&column_names, &batch.row_indices)
            .map_err(|error| MeasurementSetWriteError::Column(error.to_string()))?;
        measurement_set
            .main_table_mut()
            .discard_persisted_cell_updates(&column_names, &batch.row_indices);
        *write_seconds += write_started.elapsed().as_secs_f64();
        *bytes_written = bytes_written
            .checked_add(batch_bytes)
            .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        for (name, column_bytes) in columns_bytes {
            let total = column_bytes_written.entry(name).or_default();
            *total = total
                .checked_add(column_bytes)
                .ok_or(MeasurementSetWriteError::ByteOverflow)?;
        }
        *next_selected_row = end;
        Ok(())
    }

    /// Planned row slice for the next selected-row batch.
    #[doc(hidden)]
    pub fn next_mutation_rows(&self) -> Result<&[usize], MeasurementSetWriteError> {
        let MeasurementSetWriteSessionState::Mutation {
            next_selected_row, ..
        } = &self.state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "next mutation rows require a selected-row session".to_string(),
            ));
        };
        let end = next_selected_row
            .saturating_add(self.plan.batch_rows)
            .min(self.plan.selected_rows.len());
        Ok(&self.plan.selected_rows[*next_selected_row..end])
    }

    /// Complete a selected-row session after every planned row was written,
    /// and release casacore's MAIN write lock.
    #[doc(hidden)]
    pub fn finish_mutation(self) -> Result<MeasurementSetWriteTelemetry, MeasurementSetWriteError> {
        let finalize_started = Instant::now();
        let (mut telemetry, write_lock) = self.finish_mutation_holding()?;
        let path = write_lock.path().display().to_string();
        write_lock
            .release()
            .map_err(|source| MeasurementSetWriteError::WriteLock { path, source })?;
        telemetry.finalize_seconds = finalize_started.elapsed().as_secs_f64();
        Ok(telemetry)
    }

    /// Complete a selected-row session after every planned row was written,
    /// handing back casacore's MAIN write lock still held, with its write
    /// recorded: the caller's release publishes it.
    #[doc(hidden)]
    pub fn finish_mutation_holding(
        self,
    ) -> Result<(MeasurementSetWriteTelemetry, TableWriteLock), MeasurementSetWriteError> {
        let (telemetry, lock) = self.complete_mutation()?;
        let lock = lock.ok_or_else(|| {
            MeasurementSetWriteError::InvalidPlan(
                "borrowed mutation lock remains with caller".to_owned(),
            )
        })?;
        Ok((telemetry, lock))
    }

    fn complete_mutation(
        self,
    ) -> Result<(MeasurementSetWriteTelemetry, Option<TableWriteLock>), MeasurementSetWriteError>
    {
        let MeasurementSetWriteSession { plan, state } = self;
        let MeasurementSetWriteSessionState::Mutation {
            write_lock,
            target: _,
            next_selected_row,
            write_seconds,
            bytes_written,
            column_bytes_written,
            started_at,
        } = state
        else {
            return Err(MeasurementSetWriteError::InvalidPlan(
                "mutation finalization requires a selected-row session".to_string(),
            ));
        };
        if next_selected_row != plan.row_count {
            return Err(MeasurementSetWriteError::InvalidPlan(format!(
                "mutation session wrote {next_selected_row} rows; plan requires {}",
                plan.row_count
            )));
        }
        let producer_window_seconds = started_at.elapsed().as_secs_f64();
        let columns = plan
            .columns
            .iter()
            .map(|column| MeasurementSetColumnWriteTelemetry {
                column: column.name.clone(),
                assemble_seconds: 0.0,
                write_seconds,
                bytes_written: column_bytes_written.get(&column.name).copied().unwrap_or(0),
            })
            .collect();
        let telemetry = complete_telemetry(
            MeasurementSetWriteTelemetry {
                columns,
                assemble_seconds: 0.0,
                write_seconds,
                bytes_written,
                rows_written: 0,
                maximum_resident_bytes: 0,
                producer_seconds: 0.0,
                queue_wait_seconds: 0.0,
                finalize_seconds: 0.0,
            },
            &plan,
            producer_window_seconds,
            0.0,
            0.0,
        );
        Ok((telemetry, write_lock))
    }
}
