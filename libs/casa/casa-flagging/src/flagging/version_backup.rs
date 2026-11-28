// SPDX-License-Identifier: LGPL-3.0-or-later
//! Automatic flag versions for ordinary and partitioned MeasurementSets.

use super::initial_version::write_flag_snapshot;
use super::version_storage::{flag_version_schema, has_row_flags};
use super::*;

/// Save the MAIN flags to the next free `<task>_N` flag version.
pub fn save_next_flag_backup(ms: &MeasurementSet, task: &str) -> Result<String, FlaggingError> {
    save_next_flag_backup_with_comment(ms, task, &format!("{task} auto-backup"))
}

/// Save the MAIN flags as a new automatic version without acquiring the lock
/// of any existing version. A caller holding MAIN can use this to avoid
/// reversing the lock order of flag-version restore.
///
/// A version already present on disk is preserved, including one missing
/// from the version list after an incomplete earlier save.
///
/// # Errors
/// Returns an error for an invalid prefix or failure to save or list the backup.
pub fn save_next_flag_backup_without_replacing(
    ms: &MeasurementSet,
    task: &str,
) -> Result<String, FlaggingError> {
    let path = ms_path(ms)?;
    let names = unlisted_automatic_names(path, task)?;
    let failure = |error: std::io::Error| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("save {task} auto-backup: {error}"),
    };
    let stage = tempfile::Builder::new()
        .prefix(".flag-version-")
        .tempdir_in(ensure_flagversions_dir(path)?)
        .map_err(failure)?;
    let snapshot = stage.path().join("flags");
    write_flag_snapshot(ms, &snapshot, None, has_row_flags(ms))?;
    for name in names {
        let destination = flag_version_table_path(path, &name);
        match casa_tables::publication::rename_without_replacing(&snapshot, &destination) {
            Ok(()) => {
                append_flag_version_entry(path, &name, &format!("{task} auto-backup"))?;
                return Ok(name);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(failure(error)),
        }
    }
    unreachable!("finite set of flag versions")
}

/// Back up the working flags with the timestamp recorded by the CASA task.
pub fn save_next_casa_flag_backup(
    ms: &MeasurementSet,
    task: &str,
) -> Result<String, FlaggingError> {
    save_next_flag_backup_with_comment(ms, task, &autosave_comment())
}

fn save_next_flag_backup_with_comment(
    ms: &MeasurementSet,
    task: &str,
    comment: &str,
) -> Result<String, FlaggingError> {
    let name = next_version(ms_path(ms)?, task)?;
    save_flag_version(ms, &name, comment, FlagMerge::Replace)?;
    Ok(name)
}

fn next_version(path: &Path, task: &str) -> Result<String, FlaggingError> {
    Ok(unlisted_automatic_names(path, task)?
        .next()
        .expect("finite set of flag versions"))
}

fn unlisted_automatic_names(
    path: &Path,
    task: &str,
) -> Result<impl Iterator<Item = String>, FlaggingError> {
    if task.contains('/') {
        return Err(FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("illegal task flag version prefix {task:?}"),
        });
    }
    let versions = list_flag_versions_for_path(path)?;
    Ok((1..)
        .map(move |index| format!("{task}_{index}"))
        .filter(move |name| !versions.iter().any(|entry| &entry.name == name)))
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
    let comment = autosave_comment();
    let failure = |reason| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason,
    };
    let mut table =
        Table::with_schema(flag_version_schema(true).map_err(|error| failure(error.to_string()))?);
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
            TableOptions::new(flag_version_table_path(path, &name))
                .with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .map_err(|error| failure(error.to_string()))?;
    append_flag_version_entry(path, &name, &comment)?;
    Ok(name)
}

fn autosave_comment() -> String {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    #[cfg(unix)]
    if let Ok(seconds) = libc::time_t::try_from(unix) {
        let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
        // SAFETY: Both pointers reference live storage of the expected
        // types. localtime_r initializes local before returning success.
        let result = unsafe { libc::localtime_r(&seconds, local.as_mut_ptr()) };
        if !result.is_null() {
            // SAFETY: localtime_r reported successful initialization.
            let date = unsafe { local.assume_init() };
            return format!(
                "Flags autosave on {:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                date.tm_year + 1900,
                date.tm_mon + 1,
                date.tm_mday,
                date.tm_hour,
                date.tm_min,
                date.tm_sec,
            );
        }
    }
    let date = casa_core::quanta::MvTime::from_mjd_seconds(unix as f64 + 40_587.0 * 86_400.0);
    format!(
        "Flags autosave on {:04}-{:02}-{:02} {}",
        date.year(),
        date.month(),
        date.month_day(),
        date.format_time(0)
    )
}
