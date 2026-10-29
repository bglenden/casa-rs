// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publishing a created MeasurementSet: staged in a directory the plan
//! reserves beside its destination ([`Stage`]), then renamed into a new
//! path or exchanged with the MeasurementSet it replaces.
//!
//! The stage is a directory created atomically under a name of the plan's
//! own (another name is tried while one is taken), holding the MeasurementSet
//! being written, so a failure removes only what the plan made, never a
//! directory or file that was there before. A new MeasurementSet is renamed
//! into place without replacing anything that appeared there meanwhile.
//!
//! Replacing an existing MeasurementSet ([`super::Destination::Replace`],
//! r3 §8) is how CASA's `concat` appends to an existing `concatvis`, which
//! is then the plan's first input. The plan takes casacore's write lock on
//! the MeasurementSet's MAIN when it is made, refusing one another process
//! holds, and holds it through publication. The two directories are
//! exchanged in one call (`renamex_np` with `RENAME_SWAP` on macOS,
//! `renameat2` with `RENAME_EXCHANGE` on Linux), so an opening sees either
//! whole; the old one, then in the stage, is deleted. A filesystem without
//! the exchange (some network filesystems) refuses `Replace` before
//! anything is written. A failure before the exchange removes the stage and
//! leaves the MeasurementSet as it was; a failed exchange changes nothing; an
//! old copy that cannot be deleted is reported
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

    /// Publish the MeasurementSet `stage` holds in place of the one replaced
    /// and delete the old one, then in the stage; the old copy's path when
    /// it could not be deleted.
    pub(super) fn publish(self, stage: &Stage) -> Result<Option<PathBuf>, ObservationError> {
        exchange_directories(&stage.measurement_set, &self.destination).map_err(storage)?;
        // The lock was taken on the old MAIN, now in the stage.
        let _ = self.lock.release();
        Ok(fs::remove_dir_all(&stage.measurement_set)
            .is_err()
            .then(|| stage.measurement_set.clone()))
    }
}

/// A directory the plan reserved beside its destination, holding the
/// MeasurementSet being written (at the destination's own name).
pub(super) struct Stage {
    reserved: PathBuf,
    measurement_set: PathBuf,
}

impl Stage {
    /// Reserve a directory beside `destination` for `purpose`, created
    /// atomically: a name already taken, by anything, is passed over.
    pub(super) fn reserve(destination: &Path, purpose: &str) -> Result<Self, ObservationError> {
        let name = destination
            .file_name()
            .ok_or_else(|| request_error("a MeasurementSet's path ends in a name"))?;
        for attempt in 0..1000 {
            let reserved = beside(destination, &format!("{purpose}-{attempt}"))?;
            match fs::create_dir(&reserved) {
                Ok(()) => {
                    let measurement_set = reserved.join(name);
                    return Ok(Self {
                        reserved,
                        measurement_set,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(storage(error)),
            }
        }
        Err(request_error(format!(
            "no staging directory could be reserved beside {}",
            destination.display()
        )))
    }

    /// Where the MeasurementSet is written.
    pub(super) fn path(&self) -> &Path {
        &self.measurement_set
    }

    /// Publish the staged MeasurementSet at `destination`, a new path,
    /// without replacing anything that appeared there.
    pub(super) fn publish_new(&self, destination: &Path) -> Result<(), ObservationError> {
        rename_without_replacing(&self.measurement_set, destination).map_err(|error| {
            request_error(format!(
                "the MeasurementSet could not be published at {}: {error}",
                destination.display()
            ))
        })
    }

    /// Release the reservation once its MeasurementSet is published: the
    /// directory, then empty (a replaced copy that could not be deleted
    /// keeps it).
    pub(super) fn release(self) {
        let _ = fs::remove_dir(&self.reserved);
    }

    /// Remove the reservation and whatever was written in it.
    pub(super) fn remove(self) {
        let _ = fs::remove_dir_all(&self.reserved);
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
/// two empty ones reserved beside `destination`; only what this made is
/// removed.
fn probe(destination: &Path) -> Result<(), ObservationError> {
    let first = Stage::reserve(destination, "casa-rs-probe")?;
    let second = Stage::reserve(destination, "casa-rs-probe")?;
    let exchanged = exchange_directories(&first.reserved, &second.reserved);
    first.remove();
    second.remove();
    exchanged.map_err(|_| ObservationError::Unsupported {
        what: "replacing a MeasurementSet on a filesystem that cannot exchange directories",
    })
}

#[cfg(target_os = "macos")]
fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
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

#[cfg(target_os = "linux")]
fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
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

/// Elsewhere a destination that appeared is refused before the rename.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
    if to.symlink_metadata().is_ok() {
        return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists));
    }
    fs::rename(from, to)
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
