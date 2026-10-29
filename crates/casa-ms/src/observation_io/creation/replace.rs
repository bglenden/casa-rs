// SPDX-License-Identifier: LGPL-3.0-or-later
//! Replacing an existing MeasurementSet with a created one
//! ([`super::Destination::Replace`], r3 §8): how CASA's `concat` appends to
//! an existing `concatvis`, which is then the plan's first input.
//!
//! The plan takes casacore's write lock on the MeasurementSet's MAIN when it
//! is made, refusing one another process holds, and holds it through
//! publication. The new MeasurementSet is staged beside the old and the two
//! directories are exchanged in one call (`renamex_np` with `RENAME_SWAP` on
//! macOS, `renameat2` with `RENAME_EXCHANGE` on Linux), so an opening sees
//! either whole; the old one, then at the stage's path, is deleted. A
//! filesystem without the exchange (some network filesystems) refuses
//! `Replace` before anything is written. A failure before the exchange
//! removes the stage and leaves the MeasurementSet as it was; a failed
//! exchange changes nothing; an old copy that cannot be deleted is reported
//! ([`super::CreationReport::leftover`]). The same exchange publishes image
//! products (`casa-imaging-products`' writer); the two could share one
//! function if that crate's dependency policy (ADR-0016) took this one's.

#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::ffi::CString;
use std::fs;
use std::path::{Path, PathBuf};

use casa_observation::ObservationError;
use casa_tables::TableWriteLock;

use crate::observation_io::update::{request_error, storage};

/// A MeasurementSet a plan replaces: its path and the write lock held on
/// its MAIN.
pub(super) struct Replacing {
    pub(super) destination: PathBuf,
    lock: TableWriteLock,
}

impl Replacing {
    /// Take casacore's write lock on `destination`'s MAIN, once (a lock
    /// another process holds refuses it), after checking that it is a table
    /// and that its filesystem exchanges directories.
    pub(super) fn hold(destination: &Path) -> Result<Self, ObservationError> {
        if !destination.join("table.dat").is_file() {
            return Err(request_error(format!(
                "{} is not a MeasurementSet to replace",
                destination.display()
            )));
        }
        probe(destination)?;
        let lock = TableWriteLock::acquire(destination, 1).map_err(|error| {
            request_error(format!(
                "{} is held for writing elsewhere and cannot be replaced: {error}",
                destination.display()
            ))
        })?;
        Ok(Self {
            destination: destination.to_path_buf(),
            lock,
        })
    }

    /// Where the replacement is staged: beside the MeasurementSet.
    pub(super) fn staging(&self) -> Result<PathBuf, ObservationError> {
        beside(&self.destination, "casa-rs-replacing")
    }

    /// Publish `staging` in place of the MeasurementSet and delete the old
    /// one; the old copy's path when it could not be deleted.
    pub(super) fn publish(self, staging: &Path) -> Result<Option<PathBuf>, ObservationError> {
        exchange_directories(staging, &self.destination).map_err(storage)?;
        // The lock was taken on the old MAIN, now at the stage's path.
        let _ = self.lock.release();
        Ok(fs::remove_dir_all(staging)
            .is_err()
            .then(|| staging.to_path_buf()))
    }
}

/// A path beside `path` for this process's `purpose`.
fn beside(path: &Path, purpose: &str) -> Result<PathBuf, ObservationError> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| request_error("a MeasurementSet's path ends in a UTF-8 name"))?;
    Ok(path.with_file_name(format!(".{name}.{purpose}-{}", std::process::id())))
}

/// Refuse a filesystem that cannot exchange two directories, by exchanging
/// two empty ones beside `destination`.
fn probe(destination: &Path) -> Result<(), ObservationError> {
    let (first, second) = (
        beside(destination, "casa-rs-probe-a")?,
        beside(destination, "casa-rs-probe-b")?,
    );
    let created = fs::create_dir(&first).and_then(|()| fs::create_dir(&second));
    let exchanged = created.and_then(|()| exchange_directories(&first, &second));
    let _ = fs::remove_dir(&first);
    let _ = fs::remove_dir(&second);
    exchanged.map_err(|_| ObservationError::Unsupported {
        what: "replacing a MeasurementSet on a filesystem that cannot exchange directories",
    })
}

#[cfg(target_os = "macos")]
fn exchange_directories(staging: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("target path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this atomic call.
    let status = unsafe { libc::renamex_np(staging.as_ptr(), target.as_ptr(), libc::RENAME_SWAP) };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

#[cfg(target_os = "linux")]
fn exchange_directories(staging: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("target path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this atomic call.
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            staging.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn exchange_directories(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic non-empty directory exchange is unavailable",
    ))
}
