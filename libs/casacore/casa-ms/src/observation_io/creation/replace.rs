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

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use casa_observation::ObservationError;
use casa_tables::{
    TableWriteLock,
    publication::{exchange_directories, open_directory_identity, rename_without_replacing},
};

use crate::observation_io::update::{request_error, storage};

/// A MeasurementSet held for replacement: its path, and for a
/// MeasurementSet v2 the write lock held on its MAIN until publication.
///
/// What replaces it is written beside it, on the same filesystem, and the
/// two directories are exchanged in one atomic call ([`Replacement::publish`]),
/// so an opening sees either whole. A plan replaces this way
/// ([`super::Destination::Replace`]), and so may any other writer of a whole
/// store, an MSv4 as well.
#[derive(Debug)]
pub struct Replacement {
    destination: PathBuf,
    lock: Option<TableWriteLock>,
}

impl Replacement {
    /// Hold the MeasurementSet at `destination` for replacement. It must
    /// be a whole MeasurementSet ([`crate::observation_io::is_measurement_set`]),
    /// not a link to one, on a filesystem that exchanges directories (two
    /// empty ones reserved beside it are exchanged to find out); a
    /// MeasurementSet v2 must not be held for writing elsewhere, and its
    /// MAIN's casacore write lock is then held until [`publish`](Self::publish).
    ///
    /// # Errors
    ///
    /// [`ObservationError::InvalidRequest`] for what is no MeasurementSet, a
    /// link, or an MSv2 another process writes;
    /// [`ObservationError::Unsupported`] for a filesystem without the
    /// exchange.
    pub fn hold(destination: &Path) -> Result<Self, ObservationError> {
        refuse_unless_whole(destination)?;
        probe(destination)?;
        let lock = if crate::observation_io::is_msv4_store(destination) {
            None
        } else {
            Some(TableWriteLock::acquire(destination, 1).map_err(|error| {
                request_error(format!(
                    "{} is held for writing elsewhere and cannot be replaced: {error}",
                    destination.display()
                ))
            })?)
        };
        Ok(Self {
            destination: destination.to_path_buf(),
            lock,
        })
    }

    /// The MeasurementSet held.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.destination
    }

    /// Exchange the complete store at `replacement`, beside the one held, for
    /// it in one atomic call, then delete the old store, which `replacement`
    /// then names; its path when it could not be deleted. What is at the
    /// held path is checked again just before the exchange.
    ///
    /// # Errors
    ///
    /// When the held path no longer holds a whole MeasurementSet, or the
    /// exchange fails; either changes nothing.
    pub fn publish(self, replacement: &Path) -> Result<Option<PathBuf>, ObservationError> {
        refuse_unless_whole(&self.destination)?;
        exchange_directories(replacement, &self.destination).map_err(storage)?;
        // A lock was taken on the old MAIN, now at `replacement`.
        if let Some(lock) = self.lock {
            let _ = lock.release();
        }
        Ok(fs::remove_dir_all(replacement)
            .is_err()
            .then(|| replacement.to_path_buf()))
    }
}

/// A specific MSv4 directory held for atomic replacement. The open handle
/// keeps its identity from being reused if the pathname changes.
pub struct Msv4Replacement {
    destination: PathBuf,
    original: fs::File,
}

impl Msv4Replacement {
    /// Bind an existing MSv4 processing set before the new one is written.
    ///
    /// # Errors
    /// The destination is absent, is not a whole MSv4 processing set, or
    /// the filesystem cannot exchange directories.
    pub fn hold(destination: &Path) -> Result<Self, ObservationError> {
        let original = open_directory_identity(destination).map_err(storage)?;
        if !matches_msv4(destination, &original.metadata().map_err(storage)?) {
            return Err(request_error(format!(
                "{} is not an MSv4 processing set to replace",
                destination.display(),
            )));
        }
        probe(destination)?;
        Ok(Self {
            destination: destination.to_path_buf(),
            original,
        })
    }

    /// Exchange the staged product with the bound MSv4 store. Delete only
    /// the bound directory. Return its staged path if deletion fails.
    ///
    /// # Errors
    /// The destination or staged product changed, or exchange failed. If a
    /// foreign directory was displaced and rollback fails, preserve it at
    /// the reported staging path.
    pub fn publish(self, stage: Stage) -> Result<Option<PathBuf>, ObservationError> {
        self.publish_using(stage, exchange_directories)
    }

    fn publish_using(
        self,
        stage: Stage,
        mut exchange: impl FnMut(&Path, &Path) -> std::io::Result<()>,
    ) -> Result<Option<PathBuf>, ObservationError> {
        let exchanged = (|| {
            let original = self.original.metadata().map_err(storage)?;
            let candidate = open_directory_identity(stage.path()).map_err(storage)?;
            if !matches_msv4(&self.destination, &original) {
                return Err(self.changed());
            }
            exchange(stage.path(), &self.destination).map_err(storage)?;
            Ok((original, candidate))
        })();
        let (original, candidate) = match exchanged {
            Ok(pair) => pair,
            Err(error) => {
                stage.remove();
                return Err(error);
            }
        };
        if matches_msv4(stage.path(), &original) {
            let leftover = fs::remove_dir_all(stage.path())
                .is_err()
                .then(|| stage.path().to_path_buf());
            stage.release();
            return Ok(leftover);
        }
        // A change in the last window before exchange may have displaced
        // a foreign store. Roll back only while our new directory is still
        // at the destination. Never clean up an unverified staged directory.
        if let Ok(candidate) = candidate.metadata()
            && same_directory(&self.destination, &candidate)
            && exchange(stage.path(), &self.destination).is_ok()
            && same_directory(stage.path(), &candidate)
        {
            stage.remove();
            return Err(self.changed());
        }
        Err(ObservationError::Conflict {
            reason: format!(
                "{} changed during MSv4 replacement and could not be restored; any displaced directory was preserved at {}",
                self.destination.display(),
                stage.path().display(),
            ),
        })
    }

    fn changed(&self) -> ObservationError {
        ObservationError::Conflict {
            reason: format!(
                "{} changed during MSv4 replacement",
                self.destination.display(),
            ),
        }
    }
}

fn matches_msv4(path: &Path, expected: &fs::Metadata) -> bool {
    same_directory(path, expected)
        && crate::observation_io::is_msv4_store(path)
        && crate::observation_io::is_measurement_set(path)
}

fn same_directory(path: &Path, expected: &fs::Metadata) -> bool {
    fs::symlink_metadata(path).is_ok_and(|actual| {
        actual.is_dir() && actual.dev() == expected.dev() && actual.ino() == expected.ino()
    })
}

/// Refuse to replace what is at `destination` unless it is a whole
/// MeasurementSet, and not a link to one.
fn refuse_unless_whole(destination: &Path) -> Result<(), ObservationError> {
    let link = fs::symlink_metadata(destination).is_ok_and(|meta| meta.file_type().is_symlink());
    if link || !crate::observation_io::is_measurement_set(destination) {
        return Err(request_error(format!(
            "{} is not a MeasurementSet to replace{}",
            destination.display(),
            if link { ": it is a link" } else { "" }
        )));
    }
    Ok(())
}

/// A directory the plan reserved beside its destination, holding the
/// MeasurementSet being written (at the destination's own name).
pub struct Stage {
    reserved: PathBuf,
    measurement_set: PathBuf,
}

impl Stage {
    /// Reserve a directory beside `destination` for `purpose`, created
    /// atomically: a name already taken, by anything, is passed over.
    pub fn reserve(destination: &Path, purpose: &str) -> Result<Self, ObservationError> {
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
    pub fn path(&self) -> &Path {
        &self.measurement_set
    }

    /// Publish the staged MeasurementSet at `destination`, a new path,
    /// without replacing anything that appeared there.
    pub fn publish_new(&self, destination: &Path) -> Result<(), ObservationError> {
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
    pub fn release(self) {
        let _ = fs::remove_dir(&self.reserved);
    }

    /// Remove the reservation and whatever was written in it.
    pub fn remove(self) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn msv4(path: &Path, label: &str) {
        fs::create_dir(path).unwrap();
        fs::write(
            path.join("zarr.json"),
            r#"{"attributes":{"type":"processing_set"}}"#,
        )
        .unwrap();
        fs::write(path.join("marker"), label).unwrap();
    }

    #[test]
    fn exchange_checks_the_displaced_directory_and_preserves_it_if_rollback_fails() {
        for rollback_fails in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let destination = root.path().join("result.ps.zarr");
            let earlier = root.path().join("earlier.ps.zarr");
            msv4(&destination, "original");
            let replacement = Msv4Replacement::hold(&destination).unwrap();
            let stage = Stage::reserve(&destination, "test").unwrap();
            let staged = stage.path().to_owned();
            msv4(&staged, "candidate");
            let mut calls = 0;
            let error = replacement
                .publish_using(stage, |from, to| {
                    calls += 1;
                    if calls == 1 {
                        fs::rename(to, &earlier)?;
                        fs::create_dir(to)?;
                        fs::write(to.join("table.info"), "Type = Measurement Set\n")?;
                        fs::write(to.join("marker"), "late MSv2")?;
                    } else if rollback_fails {
                        return Err(std::io::Error::other("injected rollback failure"));
                    }
                    exchange_directories(from, to)
                })
                .unwrap_err();
            assert!(matches!(error, ObservationError::Conflict { .. }));
            assert_eq!(calls, 2);
            assert_eq!(
                fs::read_to_string(earlier.join("marker")).unwrap(),
                "original"
            );
            if rollback_fails {
                assert!(error.to_string().contains(&staged.display().to_string()));
                assert_eq!(
                    fs::read_to_string(staged.join("marker")).unwrap(),
                    "late MSv2"
                );
                assert_eq!(
                    fs::read_to_string(destination.join("marker")).unwrap(),
                    "candidate"
                );
            } else {
                assert_eq!(
                    fs::read_to_string(destination.join("marker")).unwrap(),
                    "late MSv2"
                );
                assert!(!staged.parent().unwrap().exists());
            }
        }
    }
}
