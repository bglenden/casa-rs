// SPDX-License-Identifier: LGPL-3.0-or-later
//! Filesystem publication of staged casacore data products.

use std::collections::BTreeMap;
#[cfg(unix)]
use std::ffi::CString;
use std::path::{Path, PathBuf};

use crate::{TableError, TableWriteLock};

mod copy_tree;
pub use copy_tree::{
    copy_locked_external_tree, copy_locked_table_tree, copy_locked_table_tree_with_shared_files,
};

/// Replace a physical Int32 column in an exclusively owned private stage.
/// Returns `false` for unsupported layouts so the caller can use its general
/// table writer. StandardStMan retains the descriptor and bucket layout.
/// The caller must discard the stage on error and publish only on success.
/// # Errors
/// The descriptor or data buckets cannot be read or the payload cannot be
/// safely updated. A fresh Table handle must read the updated values.
#[doc(hidden)]
pub fn replace_private_int32_column(
    table: &Path,
    column: &str,
    values: &[i32],
) -> Result<bool, TableError> {
    crate::storage::row_changes::replace_private_int32_column(table, column, values)
        .map_err(Into::into)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod bound_file;
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub use bound_file::{
    StagedDirectoryFile, open_regular_directory_entry, open_symlink_directory_entry,
};

/// Keep a directory's filesystem identity alive without listing its entries.
///
/// # Errors
/// Returns an I/O error when the path cannot be opened as a directory.
#[cfg(unix)]
pub fn open_directory_identity(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    #[cfg(target_os = "linux")]
    let flags = libc::O_PATH | libc::O_DIRECTORY;
    #[cfg(target_os = "macos")]
    let flags = libc::O_SEARCH;
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let flags = libc::O_DIRECTORY;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(flags)
        .open(path)
}

/// Lock an existing casacore table and its physical subtables for replacement.
/// Keep the returned guards alive through publication.
///
/// # Errors
/// Returns an error for unreadable paths or any live table in the tree.
pub fn lock_table_tree_for_replacement(root: &Path) -> Result<Vec<TableWriteLock>, TableError> {
    lock_table_tree_for_replacement_with_held(root, &BTreeMap::new())
}

/// Lock a table tree while reusing the caller's verified write guards.
/// The caller must retain both its write guards and the returned guards
/// until replacement is complete.
pub fn lock_table_tree_for_replacement_with_held(
    root: &Path,
    held: &BTreeMap<PathBuf, TableWriteLock>,
) -> Result<Vec<TableWriteLock>, TableError> {
    let mut pending = vec![root.to_owned()];
    let mut locks = Vec::new();
    while let Some(directory) = pending.pop() {
        let io_error = |error: std::io::Error| TableError::LockIo {
            path: directory.display().to_string(),
            message: error.to_string(),
        };
        if directory == root || directory.join("table.dat").is_file() {
            let lock = if let Some(lock) = held.get(&directory) {
                if !lock.matches_table_path(&directory) {
                    return Err(TableError::LockFailed {
                        path: directory.display().to_string(),
                        message: "table or lock was replaced".into(),
                    });
                }
                lock
            } else {
                locks.push(TableWriteLock::acquire_to_reread(&directory, 1)?);
                locks.last().expect("replacement guard was just acquired")
            };
            if lock.is_multi_used() {
                return Err(TableError::LockFailed {
                    path: directory.display().to_string(),
                    message: "table is open in another process".into(),
                });
            }
        }
        for entry in directory.read_dir().map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if entry.file_type().map_err(io_error)?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Ok(locks)
}

/// Check write access to an existing publication path as the current process.
///
/// # Errors
/// Returns an I/O error if the path is absent or cannot be written.
#[cfg(unix)]
pub fn check_write_access(path: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("publication path contains NUL"))?;
    // SAFETY: the C string is valid for the duration of the access check.
    let status =
        unsafe { libc::faccessat(libc::AT_FDCWD, path.as_ptr(), libc::W_OK, libc::AT_EACCESS) };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

/// Check write access to an existing publication path.
///
/// # Errors
/// Returns an I/O error if the path is absent or read-only.
#[cfg(not(unix))]
pub fn check_write_access(path: &Path) -> std::io::Result<()> {
    if path.metadata()?.permissions().readonly() {
        Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
    } else {
        Ok(())
    }
}

/// Move a staged directory to a new path without replacing it on Linux or macOS.
///
/// # Errors
/// Returns an I/O error if either path is invalid or the destination exists.
#[cfg(target_os = "macos")]
pub fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let from = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let to = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("destination path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this call.
    let status = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

/// Move a staged directory to a new path without replacing it on Linux or macOS.
///
/// # Errors
/// Returns an I/O error if either path is invalid or the destination exists.
#[cfg(target_os = "linux")]
pub fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let from = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let to = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("destination path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this call.
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

/// Check that the destination is absent, then move a staged directory there.
///
/// # Errors
/// Returns an I/O error if either path is invalid or the destination exists.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
    if to.symlink_metadata().is_ok() {
        return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists));
    }
    std::fs::rename(from, to)
}

/// Exchange the directories at `staging` and `target` in one atomic call
/// (Linux `renameat2(RENAME_EXCHANGE)`, macOS `renamex_np(RENAME_SWAP)`):
/// afterwards each path names what the other did, and no moment exists at
/// which `target` is missing. Elsewhere it is `std::io::ErrorKind::Unsupported`.
///
/// # Errors
///
/// When either path does not exist, they are on different filesystems, or
/// the filesystem cannot exchange directories.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn exchange_directories(staging: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("target path contains NUL"))?;
    exchange_paths_at(libc::AT_FDCWD, &staging, &target)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn exchange_paths_at(
    directory: i32,
    staging: &std::ffi::CStr,
    target: &std::ffi::CStr,
) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    // SAFETY: both C strings and the directory descriptor stay valid.
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            staging.as_ptr(),
            directory,
            target.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    #[cfg(target_os = "macos")]
    // SAFETY: both C strings and the directory descriptor stay valid.
    let status = unsafe {
        libc::renameatx_np(
            directory,
            staging.as_ptr(),
            directory,
            target.as_ptr(),
            libc::RENAME_SWAP,
        )
    };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

/// Exchange the directories at `staging` and `target` in one atomic call
/// (Linux `renameat2(RENAME_EXCHANGE)`, macOS `renamex_np(RENAME_SWAP)`):
/// afterwards each path names what the other did, and no moment exists at
/// which `target` is missing. Elsewhere it is `std::io::ErrorKind::Unsupported`.
///
/// # Errors
///
/// When either path does not exist, they are on different filesystems, or
/// the filesystem cannot exchange directories.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn exchange_directories(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic non-empty directory exchange is unavailable",
    ))
}
