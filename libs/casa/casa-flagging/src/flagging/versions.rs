// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Resolve a flag store through a MeasurementSet alias.
///
/// An existing companion at the supplied path remains accessible for versions
/// saved by older casa-rs releases. New companions follow the physical MS.
pub fn flag_version_ms_path(vis: &Path) -> PathBuf {
    let path: PathBuf = vis.components().collect();
    if flagversions_dir(&path).exists() {
        path
    } else {
        path.canonicalize().unwrap_or(path)
    }
}

/// Initialize the companion catalog that flagmanager opens with the MeasurementSet.
pub fn initialize_flag_versions(ms: &MeasurementSet) -> Result<(), FlaggingError> {
    ensure_flagversions_dir(ms_path(ms)?).map(|_| ())
}

/// Return flag-version list entries.
pub fn list_flag_versions(ms: &MeasurementSet) -> Result<Vec<FlagVersionEntry>, FlaggingError> {
    let path = ms_path(ms)?;
    list_flag_versions_for_path(path)
}

/// Refuse to delete or rename the table of flag version `versionname` of
/// the MeasurementSet at `path` while a handle in this process or another
/// process has it open, as casacore's `Table::deleteTable` and
/// `Table::rename` refuse a table that is still open in this process or
/// `isMultiUsed` (#695). [`casa_tables::table_holder`] says what this can
/// and cannot see.
pub(super) fn ensure_version_not_open(
    path: &Path,
    versionname: &str,
    version_path: &Path,
) -> Result<(), FlaggingError> {
    #[cfg(unix)]
    {
        let holder = casa_tables::table_holder(version_path).map_err(|source| {
            FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("test whether flag version {versionname:?} is open: {source}"),
            }
        })?;
        if let Some(holder) = holder {
            return Err(FlaggingError::FlagVersionInUse {
                path: path.display().to_string(),
                version: versionname.to_string(),
                table: version_path.display().to_string(),
                holder,
            });
        }
    }
    #[cfg(not(unix))]
    let _ = (path, versionname, version_path);
    Ok(())
}

/// Delete a named flag version.
///
/// # Errors
///
/// [`FlaggingError::FlagVersionInUse`] while a handle in this process or
/// another process has the version's table open.
pub fn delete_flag_version(ms: &MeasurementSet, versionname: &str) -> Result<(), FlaggingError> {
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    let version_path = flag_version_table_path(path, versionname);
    if version_path.exists() {
        ensure_version_not_open(path, versionname, &version_path)?;
        fs::remove_dir_all(&version_path).map_err(|source| FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("delete flag version {versionname:?}: {source}"),
        })?;
    }
    remove_flag_version_entry(path, versionname)
}

/// Rename a flag version, giving it `comment`. Renaming a version to its
/// own name changes only its comment.
///
/// # Errors
///
/// [`FlaggingError::FlagVersionInUse`] while a handle in this process or
/// another process has the version's table open, and
/// [`FlaggingError::FlagVersion`] when a version named `versionname`
/// already exists.
pub fn rename_flag_version(
    ms: &MeasurementSet,
    oldname: &str,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, oldname)?;
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    let old_path = flag_version_table_path(path, oldname);
    let new_path = flag_version_table_path(path, versionname);
    if versionname != oldname
        && (new_path.exists()
            || list_flag_versions_for_path(path)?
                .iter()
                .any(|entry| entry.name == versionname))
    {
        return Err(FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!(
                "rename flag version {oldname:?}: flag version {versionname:?} already exists"
            ),
        });
    }
    ensure_version_not_open(path, oldname, &old_path)?;
    fs::rename(old_path, new_path).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("rename flag version {oldname:?}: {source}"),
    })?;
    rename_flag_version_entry(path, oldname, versionname, comment)
}

/// Rename a saved version through the CASA flagmanager task.
///
/// The task requires a saved source and a free destination, including when
/// the source and destination names are equal.
pub fn rename_flag_version_for_task(
    ms: &MeasurementSet,
    oldname: &str,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, oldname)?;
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    if !flag_version_table_path(path, oldname).is_dir() {
        return Err(FlaggingError::FlagVersionRenameArgument {
            parameter: "oldname",
            message: format!("No such flagversions: {oldname}"),
        });
    }
    if flag_version_table_path(path, versionname).exists() {
        return Err(FlaggingError::FlagVersionRenameArgument {
            parameter: "versionname",
            message: format!("Flagversions {versionname} already exists!"),
        });
    }
    rename_flag_version(ms, oldname, versionname, comment)
}

pub(super) fn list_flag_versions_for_path(
    path: &Path,
) -> Result<Vec<FlagVersionEntry>, FlaggingError> {
    let mut entries = vec![FlagVersionEntry {
        name: "main".to_string(),
        comment: "working copy in main table".to_string(),
    }];
    let list_path = flag_version_list_path(path);
    if !list_path.exists() {
        return Ok(entries);
    }
    let text = fs::read_to_string(&list_path).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("read FLAG_VERSION_LIST: {source}"),
    })?;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let (name, comment) = line
            .split_once(" : ")
            .map(|(name, comment)| (name.to_string(), comment.to_string()))
            .unwrap_or_else(|| (line.to_string(), String::new()));
        entries.push(FlagVersionEntry { name, comment });
    }
    Ok(entries)
}

pub(super) fn append_flag_version_entry(
    path: &Path,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |mut entries| {
        entries.push(FlagVersionEntry {
            name: versionname.to_string(),
            comment: comment.to_string(),
        });
        entries
    })
}

pub(super) fn update_flag_version_comment(
    path: &Path,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    if comment.is_empty() {
        return Ok(());
    }
    rewrite_flag_version_text(path, |mut entries| {
        for entry in &mut entries {
            if entry.name == versionname {
                entry.comment = comment.to_string();
            }
        }
        entries
    })
}

pub(super) fn remove_flag_version_entry(
    path: &Path,
    versionname: &str,
) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |entries| {
        entries
            .into_iter()
            .filter(|entry| entry.name != versionname)
            .collect()
    })
}

pub(super) fn rename_flag_version_entry(
    path: &Path,
    oldname: &str,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |mut entries| {
        for entry in &mut entries {
            if entry.name == oldname {
                entry.name = versionname.to_string();
                entry.comment = comment.to_string();
            }
        }
        entries
    })
}

pub(super) fn rewrite_flag_version_text<F>(path: &Path, edit: F) -> Result<(), FlaggingError>
where
    F: FnOnce(Vec<FlagVersionEntry>) -> Vec<FlagVersionEntry>,
{
    ensure_flagversions_dir(path)?;
    let entries = edit(
        list_flag_versions_for_path(path)?
            .into_iter()
            .skip(1)
            .collect(),
    );
    let text = entries
        .into_iter()
        .map(|entry| format!("{} : {}\n", entry.name, entry.comment))
        .collect::<String>();
    fs::write(flag_version_list_path(path), text).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("write FLAG_VERSION_LIST: {source}"),
    })
}

/// Open a flag-version table to read it, holding casacore's read lock until
/// the table is dropped. The open waits, as a casacore `AutoLocking` open
/// does, while another process (casacore's flagmanager or casa-rs) holds the
/// version's write lock, so a version being written is never read half
/// written; the table is read only once the lock is held.
pub(super) fn open_flag_version_for_reading(version_path: &Path) -> Result<Table, TableError> {
    #[cfg(unix)]
    {
        Table::open_with_lock(
            TableOptions::new(version_path),
            LockOptions::new(LockMode::AutoLocking),
        )
    }
    #[cfg(not(unix))]
    {
        Table::open(TableOptions::new(version_path))
    }
}

pub(super) fn merge_bool(source: bool, dest: bool, merge: FlagMerge) -> bool {
    match merge {
        FlagMerge::Replace => source,
        FlagMerge::Or => source || dest,
        FlagMerge::And => source && dest,
    }
}

pub(super) fn merge_bool_arrays(
    source: &ArrayD<bool>,
    dest: &ArrayD<bool>,
    merge: FlagMerge,
) -> Result<ArrayD<bool>, FlaggingError> {
    if source.shape() != dest.shape() {
        return Err(FlaggingError::MutateFlags {
            path: "<memory>".to_string(),
            reason: format!(
                "cannot merge flag arrays with shapes {:?} and {:?}",
                source.shape(),
                dest.shape()
            ),
        });
    }
    let values = source
        .iter()
        .zip(dest.iter())
        .map(|(source, dest)| match merge {
            FlagMerge::Replace => *source,
            FlagMerge::Or => *source || *dest,
            FlagMerge::And => *source && *dest,
        })
        .collect::<Vec<_>>();
    ArrayD::from_shape_vec(IxDyn(source.shape()), values).map_err(|source| {
        FlaggingError::MutateFlags {
            path: "<memory>".to_string(),
            reason: format!("build merged flag array: {source}"),
        }
    })
}

pub(super) fn ensure_flagversions_dir(path: &Path) -> Result<PathBuf, FlaggingError> {
    let dir = flagversions_dir(path);
    fs::create_dir_all(&dir).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("create flagversions directory: {source}"),
    })?;
    if let Err(source) = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(FLAG_VERSION_LIST))
        && source.kind() != std::io::ErrorKind::AlreadyExists
    {
        return Err(FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("create FLAG_VERSION_LIST: {source}"),
        });
    }
    Ok(dir)
}

use casa_ms::flagversions_dir;

pub(super) fn flag_version_list_path(path: &Path) -> PathBuf {
    flagversions_dir(path).join(FLAG_VERSION_LIST)
}

pub(super) fn flag_version_table_path(path: &Path, versionname: &str) -> PathBuf {
    flagversions_dir(path).join(format!("flags.{versionname}"))
}

pub(super) fn validate_version_name(
    ms: &MeasurementSet,
    versionname: &str,
) -> Result<(), FlaggingError> {
    if versionname.is_empty() || versionname == "main" || versionname.contains('/') {
        return Err(FlaggingError::FlagVersion {
            path: display_ms_path(ms),
            reason: format!("illegal flag version name {versionname:?}"),
        });
    }
    Ok(())
}

pub(super) fn clone_flag_matrix(
    ms: &MeasurementSet,
    row: usize,
) -> Result<ArrayD<bool>, FlaggingError> {
    table_flag_matrix(ms.main_table(), row, FLAG_COLUMN, ms.path())
}

pub(super) fn flag_row_value(ms: &MeasurementSet, row: usize) -> Result<bool, FlaggingError> {
    table_bool(ms.main_table(), row, FLAG_ROW_COLUMN, ms.path())
}
