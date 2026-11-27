// SPDX-License-Identifier: LGPL-3.0-or-later

//! Filesystem table locking and safe table-tree removal helpers for image lifecycle operations.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use casa_tables::{Table, TableError, TableOptions, TableUseLock, TableWriteLock};

use super::attachment::ExternalPathCheck;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::ImageHandle;

/// Yields ancestor directories of `path` that contain a casacore `table.dat` control file.
pub fn containing_tables(path: &Path) -> impl Iterator<Item = &Path> {
    path.parent()
        .into_iter()
        .flat_map(Path::ancestors)
        .filter(|ancestor| ancestor.join("table.dat").is_file())
}

/// Acquires write locks on any casacore tables containing `path`, checking parent writability.
pub fn lock_table_ancestors(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> ImageToolResult<()> {
    let parent = path.parent().ok_or_else(|| not_writable(path))?;
    if fs::metadata(parent)?.permissions().readonly() {
        return Err(not_writable(path));
    }
    for ancestor in containing_tables(path) {
        lock_table_for_deletion(ancestor, leases)?;
    }
    Ok(())
}

/// Acquires write locks on `path`, its sub-tables, and any containing ancestor tables, requiring writable directories.
pub fn lock_table_tree(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> ImageToolResult<()> {
    lock_tables_in_tree(path, leases, true)
}

/// Acquires write locks on tables inside `path` for moving read-only directories (e.g. MIRIAD/saved-expr).
pub fn lock_table_tree_for_read_only_move(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> ImageToolResult<()> {
    lock_tables_in_tree(path, leases, false)
}

/// Removes an existing native casacore table directory at `path` (or symlink `name`) after verifying locks.
pub fn remove_native_table(
    name: &Path,
    path: &Path,
    expected: Option<&TableUseLock>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<()> {
    Table::open_metadata_only(TableOptions::new(path))?;
    if is_external_open(path) {
        return Err(in_use(path, "this"));
    }
    let mut leases = BTreeMap::new();
    lock_table_tree(path, &mut leases)?;
    if expected.is_some_and(|guard| !guard.matches_directory(path)) {
        return Err(ImageToolError::new(format!(
            "Cannot delete file {} because the image at that path has changed",
            path.display()
        )));
    }
    let named_link = name
        .symlink_metadata()
        .is_ok_and(|entry| entry.is_symlink());
    if named_link
        && let Some(parent) = name.parent()
        && fs::metadata(parent)?.permissions().readonly()
    {
        return Err(ImageToolError::new(format!(
            "Cannot delete file {} because its directory is not writable",
            name.display()
        )));
    }
    fs::remove_dir_all(path)?;
    if named_link {
        // The table has been removed even if the alias's permissions changed
        // after the preflight check.
        let _ = fs::remove_file(name);
    }
    Ok(())
}

/// Removes an image file or directory from disk safely.
pub fn remove_image_path(path: &Path) -> ImageToolResult<()> {
    match path.symlink_metadata() {
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path)?,
        Ok(_) => fs::remove_file(path)?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn in_use(path: &Path, process: &str) -> ImageToolError {
    ImageToolError::new(format!(
        "Cannot delete file {} because table is still open in {process} process",
        path.display()
    ))
}

fn not_writable(name: &Path) -> ImageToolError {
    ImageToolError::new(format!(
        "Cannot delete file {} because table is not writable",
        name.display()
    ))
}

fn lock_tables_in_tree(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
    require_writable_directories: bool,
) -> ImageToolResult<()> {
    lock_table_ancestors(path, leases)?;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        if require_writable_directories && fs::metadata(&directory)?.permissions().readonly() {
            return Err(not_writable(&directory));
        }
        let control = directory.join("table.dat");
        if ImageHandle::has_native_control(&directory) {
            OpenOptions::new()
                .write(true)
                .open(control)
                .map_err(|_| not_writable(&directory))?;
            lock_table_for_deletion(&directory, leases)?;
        }
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Ok(())
}

fn lock_table_for_deletion(
    directory: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> ImageToolResult<()> {
    if leases.contains_key(directory) {
        return Ok(());
    }
    let lease = TableWriteLock::acquire(directory, 1).map_err(|error| match error {
        TableError::LockFailed { .. } => in_use(directory, "another"),
        _ => ImageToolError::from(error),
    })?;
    if lease.is_multi_used() {
        return Err(in_use(directory, "another"));
    }
    leases.insert(directory.to_path_buf(), lease);
    Ok(())
}
