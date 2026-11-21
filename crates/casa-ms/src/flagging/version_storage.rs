// SPDX-License-Identifier: LGPL-3.0-or-later
//! Flag version snapshots and restoration for MeasurementSets and calibration tables.

use super::*;

fn calibration_table(ms: &MeasurementSet) -> bool {
    ms.main_table().info().table_type == "Calibration"
}

fn has_row_flags(ms: &MeasurementSet) -> bool {
    !calibration_table(ms)
        || ms
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column(FLAG_ROW_COLUMN))
}

pub(super) fn flag_version_schema(
    row_flags: bool,
) -> Result<TableSchema, casa_tables::SchemaError> {
    let mut columns = vec![ColumnSchema::array_variable(
        FLAG_COLUMN,
        PrimitiveType::Bool,
        Some(2),
    )];
    if row_flags {
        columns.push(ColumnSchema::scalar(FLAG_ROW_COLUMN, PrimitiveType::Bool));
    }
    TableSchema::new(columns)
}

/// Save current MAIN flags to a named flag version.
pub fn save_flag_version(
    ms: &MeasurementSet,
    versionname: &str,
    comment: &str,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    publish_flag_version(path, capture_flags(ms)?, versionname, comment, merge)
}

/// Snapshot the current flags from disk under a retained casacore read lock.
/// The source lock is released before the version store is updated.
pub fn save_disk_flag_version(
    ms: &MeasurementSet,
    versionname: &str,
    comment: &str,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    let captured = {
        #[cfg(unix)]
        let source = MeasurementSet::open_retained_read(path);
        #[cfg(not(unix))]
        let source = MeasurementSet::open(path);
        let source = source.map_err(|source| FlaggingError::OpenMeasurementSet {
            path: path.display().to_string(),
            source,
        })?;
        capture_flags(&source)?
    };
    publish_flag_version(path, captured, versionname, comment, merge)
}

fn capture_flags(ms: &MeasurementSet) -> Result<Table, FlaggingError> {
    let path = ms_path(ms)?;
    let row_flags = has_row_flags(ms);
    let mut table = Table::with_schema(flag_version_schema(row_flags).map_err(|source| {
        FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("build flag-version schema: {source}"),
        }
    })?);
    for row in 0..ms.row_count() {
        let mut fields = vec![RecordField::new(
            FLAG_COLUMN,
            Value::Array(ArrayValue::Bool(clone_flag_matrix(ms, row)?)),
        )];
        if row_flags {
            fields.push(RecordField::new(
                FLAG_ROW_COLUMN,
                Value::Scalar(ScalarValue::Bool(flag_row_value(ms, row)?)),
            ));
        }
        table
            .add_row(RecordValue::new(fields))
            .map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("add flag-version row: {source}"),
            })?;
    }
    Ok(table)
}

fn publish_flag_version(
    path: &Path,
    table: Table,
    versionname: &str,
    comment: &str,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    let directory = ensure_flagversions_dir(path)?;
    let version_path = flag_version_table_path(path, versionname);
    if version_path.exists() {
        merge_flags_into_existing_version(path, &table, &version_path, merge)?;
        if list_flag_versions_for_path(path)?
            .iter()
            .any(|entry| entry.name == versionname)
        {
            return update_flag_version_comment(path, versionname, comment);
        }
        return append_flag_version_entry(path, versionname, comment);
    }
    let failure = |reason: String| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("save flag version {versionname:?}: {reason}"),
    };
    let stage = tempfile::Builder::new()
        .prefix(".flag-version-")
        .tempdir_in(directory)
        .map_err(|error| failure(error.to_string()))?;
    let table_path = stage.path().join("flags");
    let bindings = HashMap::from([(
        FLAG_COLUMN.to_string(),
        ColumnBinding {
            data_manager: DataManagerKind::TiledShapeStMan,
            tile_shape: None,
        },
    )]);
    table
        .save_with_bindings(
            TableOptions::new(&table_path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .map_err(|error| failure(error.to_string()))?;
    fs::rename(table_path, version_path).map_err(|error| failure(error.to_string()))?;
    append_flag_version_entry(path, versionname, comment)
}

/// Restore a named flag version to MAIN.
pub fn restore_flag_version(
    ms: &mut MeasurementSet,
    versionname: &str,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?.to_path_buf();
    let version_path = flag_version_table_path(&path, versionname);
    // The version is read under its read lock, held until the restore ends.
    let table = open_flag_version_for_reading(&version_path).map_err(|source| {
        if matches!(version_path.try_exists(), Ok(false)) {
            return FlaggingError::MissingFlagVersion {
                version: versionname.to_owned(),
            };
        }
        FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("open flag version {versionname:?}: {source}"),
        }
    })?;
    if table.row_count() != ms.row_count() {
        return Err(FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!(
                "flag version {versionname:?} has {} rows; MAIN has {}",
                table.row_count(),
                ms.row_count()
            ),
        });
    }
    let row_flags = has_row_flags(ms);
    let plan = restore_plan(ms, &table, &path, row_flags)?;
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation(ms, plan).map_err(|source| {
            FlaggingError::MutateFlags {
                path: path.display().to_string(),
                reason: format!("start restored flags: {source}"),
            }
        })?;
    loop {
        let rows = session
            .next_mutation_rows()
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.display().to_string(),
                reason: format!("plan restored flag batch: {source}"),
            })?
            .to_vec();
        if rows.is_empty() {
            break;
        }
        let mut flags = Vec::with_capacity(rows.len());
        let mut flag_rows = Vec::new();
        for &row in &rows {
            let source = table_flag_matrix(&table, row, FLAG_COLUMN, Some(&path))?;
            let dest = clone_flag_matrix(ms, row)?;
            flags.push(ArrayValue::Bool(merge_bool_arrays(&source, &dest, merge)?));
            if row_flags {
                let source_flag_row = table_bool(&table, row, FLAG_ROW_COLUMN, Some(&path))?;
                let dest_flag_row = flag_row_value(ms, row)?;
                flag_rows.push(ScalarValue::Bool(merge_bool(
                    source_flag_row,
                    dest_flag_row,
                    merge,
                )));
            }
        }
        let mut columns = vec![MeasurementSetMutationColumnBatch {
            name: FLAG_COLUMN.to_string(),
            values: MeasurementSetMutationColumnValues::Arrays(flags),
        }];
        if row_flags {
            columns.push(MeasurementSetMutationColumnBatch {
                name: FLAG_ROW_COLUMN.to_string(),
                values: MeasurementSetMutationColumnValues::Scalars(flag_rows),
            });
        }
        session
            .write_mutation_batch(
                ms,
                MeasurementSetMutationBatch {
                    row_indices: rows,
                    columns,
                },
            )
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.display().to_string(),
                reason: format!("write restored flag batch: {source}"),
            })?;
    }
    session
        .finish_mutation()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.display().to_string(),
            reason: format!("finish restored flags: {source}"),
        })?;
    Ok(())
}

fn restore_plan(
    ms: &MeasurementSet,
    version: &Table,
    path: &Path,
    row_flags: bool,
) -> Result<MeasurementSetWritePlan, FlaggingError> {
    let failure = |reason: String| FlaggingError::MutateFlags {
        path: path.display().to_string(),
        reason: format!("plan restored flags: {reason}"),
    };
    let rows: Vec<_> = (0..ms.row_count()).collect();
    let maximum = if calibration_table(ms) {
        matching_calibration_flag_size(ms.main_table(), version, &rows).map_err(failure)?
    } else {
        maximum_visibility_cell_elements(ms).map_err(|error| failure(error.to_string()))?
    };
    let column = |name: &str, bytes_per_row| MeasurementSetWriteColumnPlan {
        name: name.to_string(),
        bytes_per_row,
        mode: MeasurementSetColumnWriteMode::Replace,
        storage_manager: MeasurementSetColumnStorage::Persisted,
        tile_shape: None,
        create_source_column: None,
    };
    let mut columns = vec![column(FLAG_COLUMN, maximum)];
    if row_flags {
        columns.push(column(FLAG_ROW_COLUMN, 1));
    }
    let resources = MeasurementSetWriteResources::from_system_memory(2)
        .map_err(|error| failure(error.to_string()))?;
    MeasurementSetWritePlan::selected_row_mutation(rows, columns, resources)
        .map_err(|error| failure(error.to_string()))
}

fn matching_calibration_flag_size(
    main: &Table,
    version: &Table,
    rows: &[usize],
) -> Result<usize, String> {
    let shapes = |table: &Table, batch: &[usize]| -> Result<Vec<Option<Vec<usize>>>, TableError> {
        let column = table.column_accessor(FLAG_COLUMN)?;
        column.array_shapes_uncached(batch).or_else(|_| {
            Ok(column
                .array_cells_owned_uncached(batch)?
                .into_iter()
                .map(|cell| cell.map(|array| array.shape().to_vec()))
                .collect())
        })
    };
    let mut maximum = 1;
    for batch in rows.chunks(256) {
        let dims = shapes(main, batch).map_err(|error| error.to_string())?;
        let saved = shapes(version, batch).map_err(|error| error.to_string())?;
        for ((&row, original), stored) in batch.iter().zip(dims).zip(saved) {
            if let (Some(shape), Some(stored)) = (original, stored)
                && shape == stored
                && let [receptors, channels] = shape.as_slice()
                && let Some(size) = receptors.checked_mul(*channels)
            {
                maximum = maximum.max(size);
            } else {
                return Err(format!("FLAG shape differs or is undefined at row {row}"));
            }
        }
    }
    Ok(maximum)
}

/// Take casacore's write lock on an existing flag-version table before it is
/// read and merged, waiting for another process as every in-place writer
/// does. The table is read only once the lock is held, so a write another
/// process published while this one waited is not stale here: the refusal
/// [`TableWriteLock`] gives for it is answered by taking the lock again.
fn lock_existing_flag_version(version_path: &Path) -> Result<TableWriteLock, TableError> {
    TableWriteLock::acquire(version_path, 0).or_else(|_| TableWriteLock::acquire(version_path, 0))
}

fn merge_flags_into_existing_version(
    path: &Path,
    snapshot: &Table,
    version_path: &Path,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    let mut lock =
        lock_existing_flag_version(version_path).map_err(|source| FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("lock existing flag version: {source}"),
        })?;
    let mut table = Table::open(TableOptions::new(version_path)).map_err(|source| {
        FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("open existing flag version: {source}"),
        }
    })?;
    let row_flags = snapshot
        .schema()
        .is_some_and(|schema| schema.contains_column(FLAG_ROW_COLUMN));
    if table.row_count() != snapshot.row_count() {
        return Err(FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!(
                "existing flag version has {} rows; MAIN has {}",
                table.row_count(),
                snapshot.row_count()
            ),
        });
    }
    for row in 0..snapshot.row_count() {
        let source = table_flag_matrix(snapshot, row, FLAG_COLUMN, Some(path))?;
        let dest = table_flag_matrix(&table, row, FLAG_COLUMN, Some(path))?;
        let merged = merge_bool_arrays(&source, &dest, merge)?;
        table
            .cell_accessor_mut(row, FLAG_COLUMN)
            .map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("open existing FLAG row {row}: {source}"),
            })?
            .set(Value::Array(ArrayValue::Bool(merged)))
            .map_err(|source| FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("write existing FLAG row {row}: {source}"),
            })?;
        if row_flags {
            let source_flag_row = table_bool(snapshot, row, FLAG_ROW_COLUMN, Some(path))?;
            let dest_flag_row = table_bool(&table, row, FLAG_ROW_COLUMN, Some(path))?;
            let merged_flag_row = merge_bool(source_flag_row, dest_flag_row, merge);
            table
                .cell_accessor_mut(row, FLAG_ROW_COLUMN)
                .map_err(|source| FlaggingError::FlagVersion {
                    path: path.display().to_string(),
                    reason: format!("open existing FLAG_ROW row {row}: {source}"),
                })?
                .set(Value::Scalar(ScalarValue::Bool(merged_flag_row)))
                .map_err(|source| FlaggingError::FlagVersion {
                    path: path.display().to_string(),
                    reason: format!("write existing FLAG_ROW row {row}: {source}"),
                })?;
        }
    }
    // Recorded before the save, so an interrupted save still tells other
    // processes to re-read the version.
    lock.record_write();
    table
        .save(TableOptions::new(version_path))
        .map_err(|source| FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("save existing flag version: {source}"),
        })?;
    lock.release().map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("release existing flag version: {source}"),
    })
}
