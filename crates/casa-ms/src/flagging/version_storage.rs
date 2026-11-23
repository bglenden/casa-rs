// SPDX-License-Identifier: LGPL-3.0-or-later
//! Flag version snapshots and restoration for MeasurementSets and calibration tables.

use super::initial_version::{read_flag_shapes, write_flag_snapshot};
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
    publish_flag_snapshot(path, versionname, comment, merge, |snapshot| {
        write_flag_snapshot(ms, snapshot, None, has_row_flags(ms))
    })
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
    snapshot_disk_flags(ms, versionname, comment, merge)
}

/// Save the raw agentflagger version, whose empty name produces `flags.`.
pub fn save_agent_disk_flag_version(
    ms: &MeasurementSet,
    versionname: &str,
    comment: &str,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    if !versionname.is_empty() {
        return save_disk_flag_version(ms, versionname, comment, merge);
    }
    snapshot_disk_flags(ms, versionname, comment, merge)
}

fn snapshot_disk_flags(
    ms: &MeasurementSet,
    versionname: &str,
    comment: &str,
    merge: FlagMerge,
) -> Result<(), FlaggingError> {
    let path = ms_path(ms)?;
    publish_flag_snapshot(path, versionname, comment, merge, |snapshot| {
        let source = open_flag_source(path)?;
        write_flag_snapshot(&source, snapshot, None, has_row_flags(&source))
    })
}

fn publish_flag_snapshot(
    path: &Path,
    versionname: &str,
    comment: &str,
    merge: FlagMerge,
    write: impl FnOnce(&Path) -> Result<(), FlaggingError>,
) -> Result<(), FlaggingError> {
    let version_path = flag_version_table_path(path, versionname);
    let existed = version_path.exists();
    let failure = |reason: String| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("save flag version {versionname:?}: {reason}"),
    };
    let stage = tempfile::Builder::new()
        .prefix(".flag-version-")
        .tempdir_in(ensure_flagversions_dir(path)?)
        .map_err(|error| failure(error.to_string()))?;
    let snapshot = stage.path().join("flags");
    write(&snapshot)?;
    if existed {
        let captured = open_flag_version_for_reading(&snapshot)
            .map_err(|source| failure(source.to_string()))?;
        merge_flags_into_existing_version(path, &captured, &version_path, merge)?;
        if list_flag_versions_for_path(path)?
            .iter()
            .any(|entry| entry.name == versionname)
        {
            return update_flag_version_comment(path, versionname, comment);
        }
    } else {
        casa_tables::publication::rename_without_replacing(&snapshot, &version_path)
            .map_err(|source| failure(source.to_string()))?;
    }
    append_flag_version_entry(path, versionname, comment)
}

fn open_flag_source(path: &Path) -> Result<MeasurementSet, FlaggingError> {
    #[cfg(unix)]
    let source = MeasurementSet::open_retained_read(path);
    #[cfg(not(unix))]
    let source = MeasurementSet::open(path);
    source.map_err(|source| FlaggingError::OpenMeasurementSet {
        path: path.display().to_string(),
        source,
    })
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
    let display_path = path.display().to_string();
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
        let main = ms.main_table();
        let saved = read_version_flags(&table, &rows, &display_path)?;
        let flags = restore_flag_arrays(main, &rows, &display_path, saved, merge)?;
        let mut columns = vec![MeasurementSetMutationColumnBatch {
            name: FLAG_COLUMN.to_string(),
            values: MeasurementSetMutationColumnValues::Arrays(flags),
        }];
        if row_flags {
            let mut saved = load_bool_cells(&table, &rows, FLAG_ROW_COLUMN, &display_path)?;
            if merge != FlagMerge::Replace {
                let current = load_bool_cells(main, &rows, FLAG_ROW_COLUMN, &display_path)?;
                for (source, dest) in saved.iter_mut().zip(current) {
                    *source = merge_bool(*source, dest, merge);
                }
            }
            let flag_rows = saved.into_iter().map(ScalarValue::Bool).collect();
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

fn read_version_flags(
    table: &Table,
    rows: &[usize],
    path: &str,
) -> Result<Vec<ArrayD<bool>>, FlaggingError> {
    let packed = table
        .column_accessor(FLAG_COLUMN)
        .and_then(|column| column.packed_2d_cells_uncached(rows))
        .map_err(|error| FlaggingError::MutateFlags {
            path: path.into(),
            reason: error.to_string(),
        })?;
    if let Some(ArrayValue::Bool(flags)) = packed {
        Ok(flags
            .axis_iter(ndarray::Axis(2))
            .map(|row| row.to_owned())
            .collect())
    } else {
        clip_time::read_flags(table, rows, path)
    }
}

fn restore_flag_arrays(
    main: &Table,
    rows: &[usize],
    path: &str,
    saved: Vec<ArrayD<bool>>,
    merge: FlagMerge,
) -> Result<Vec<ArrayValue>, FlaggingError> {
    if merge == FlagMerge::Replace {
        // Replacement needs the destination layout, but it does not use
        // the flag values. Tiled managers answer this from shape headers.
        let shapes = read_flag_shapes(main, rows).map_err(|error| FlaggingError::MutateFlags {
            path: path.into(),
            reason: error.to_string(),
        })?;
        for ((&row, source), dest) in rows.iter().zip(&saved).zip(shapes) {
            if dest.as_deref() != Some(source.shape()) {
                return Err(FlaggingError::MutateFlags {
                    path: path.into(),
                    reason: format!("FLAG shape differs or is undefined at row {row}"),
                });
            }
        }
        return Ok(saved.into_iter().map(ArrayValue::Bool).collect());
    }

    let current = clip_time::read_flags(main, rows, path)?;
    saved
        .into_iter()
        .zip(current)
        .map(|(source, dest)| merge_bool_arrays(&source, &dest, merge).map(ArrayValue::Bool))
        .collect()
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
    let resources = MeasurementSetWriteResources::from_system_memory(3)
        .map_err(|error| failure(error.to_string()))?;
    MeasurementSetWritePlan::selected_row_mutation(rows, columns, resources)
        .map_err(|error| failure(error.to_string()))
}

fn matching_calibration_flag_size(
    main: &Table,
    version: &Table,
    rows: &[usize],
) -> Result<usize, String> {
    let mut maximum = 1;
    for batch in rows.chunks(256) {
        let dims = read_flag_shapes(main, batch).map_err(|error| error.to_string())?;
        let saved = read_flag_shapes(version, batch).map_err(|error| error.to_string())?;
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
