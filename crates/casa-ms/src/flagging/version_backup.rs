// SPDX-License-Identifier: LGPL-3.0-or-later
//! Automatic flag versions for ordinary and partitioned MeasurementSets.

use super::*;

/// Save the MAIN flags to the next free `<task>_N` flag version.
pub fn save_next_flag_backup(ms: &MeasurementSet, task: &str) -> Result<String, FlaggingError> {
    let name = next_version(ms_path(ms)?, task)?;
    let comment = format!("{task} auto-backup");
    save_flag_version(ms, &name, &comment, FlagMerge::Replace)?;
    Ok(name)
}

fn next_version(path: &Path, task: &str) -> Result<String, FlaggingError> {
    if task.contains('/') {
        return Err(FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("illegal task flag version prefix {task:?}"),
        });
    }
    let versions = list_flag_versions_for_path(path)?;
    let index = (1..)
        .find(|i| {
            !versions
                .iter()
                .any(|entry| entry.name == format!("{task}_{i}"))
        })
        .expect("finite set of flag versions");
    Ok(format!("{task}_{index}"))
}

/// Back up the flags of a Multi-MS once, in the aggregate's row order.
/// Open each physical partition separately so only FLAG and FLAG_ROW are
/// collected. The aggregate's visibility columns are never materialized.
pub fn save_next_partition_flag_backup(
    path: &Path,
    partitions: &[PathBuf],
    task: &str,
) -> Result<String, FlaggingError> {
    let name = next_version(path, task)?;
    let comment = format!("{task} auto-backup");
    let failure = |reason| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason,
    };
    let mut table =
        Table::with_schema(flag_version_schema().map_err(|error| failure(error.to_string()))?);
    for part in partitions {
        let ms =
            MeasurementSet::open(part).map_err(|source| FlaggingError::OpenMeasurementSet {
                path: part.display().to_string(),
                source,
            })?;
        for row in 0..ms.row_count() {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new(
                        FLAG_COLUMN,
                        Value::Array(ArrayValue::Bool(clone_flag_matrix(&ms, row)?)),
                    ),
                    RecordField::new(
                        FLAG_ROW_COLUMN,
                        Value::Scalar(ScalarValue::Bool(flag_row_value(&ms, row)?)),
                    ),
                ]))
                .map_err(|error| failure(error.to_string()))?;
        }
    }
    ensure_flagversions_dir(path)?;
    let bindings = HashMap::from([(
        FLAG_COLUMN.to_string(),
        ColumnBinding {
            data_manager: DataManagerKind::TiledShapeStMan,
            tile_shape: None,
        },
    )]);
    table
        .save_with_bindings(
            TableOptions::new(&flag_version_table_path(path, &name))
                .with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .map_err(|error| failure(error.to_string()))?;
    append_flag_version_entry(path, &name, &comment)?;
    Ok(name)
}
