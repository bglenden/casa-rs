// SPDX-License-Identifier: LGPL-3.0-or-later
//! A new local processing set: built in a staging directory of its own,
//! filled with one dataset per partition ([`Msv4Writer`]), and published
//! whole once every dataset is complete.
//!
//! The datasets write disjoint parts of the staging store and share nothing
//! else, so a producer may write them one after another or from a thread
//! each. Publication renames the staging directory to the destination and
//! refuses to replace anything there; a writer dropped or failing before it
//! removes its staging directory, and a crash leaves it under a name no
//! later writer reuses.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use casa_observation::{Catalogue, ObservationError, PartitionId, TimeBaselineGrid};
use serde_json::json;
use zarrs::filesystem::FilesystemStore;

use super::{Msv4StorageOptions, Msv4Writer, create_group, request_error};
use crate::observation_io::msv4::metadata::storage;

/// The datasets a processing set declares, and which are complete.
#[derive(Debug, Default)]
pub(super) struct Registry {
    datasets: Mutex<Vec<(String, bool)>>,
}

impl Registry {
    /// Declare dataset `name`, which no other may have.
    fn declare(&self, name: &str) -> Result<(), ObservationError> {
        let mut datasets = self.datasets.lock().unwrap_or_else(PoisonError::into_inner);
        if datasets.iter().any(|(known, _)| known == name) {
            return Err(request_error(format!(
                "the processing set already has a dataset {name}"
            )));
        }
        datasets.push((name.to_string(), false));
        Ok(())
    }

    /// Record that dataset `name` is complete.
    pub(super) fn finished(&self, name: &str) {
        let mut datasets = self.datasets.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, done)) = datasets.iter_mut().find(|(known, _)| known == name) {
            *done = true;
        }
    }

    /// The first declared dataset not complete, if any.
    fn incomplete(&self) -> Option<String> {
        self.datasets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(_, done)| !done)
            .map(|(name, _)| name.clone())
    }
}

/// A processing set being created (module documentation).
pub struct Msv4ProcessingSetWriter {
    /// The staging directory, until published or removed.
    staging: Option<PathBuf>,
    destination: PathBuf,
    store: Arc<FilesystemStore>,
    registry: Arc<Registry>,
}

impl Msv4ProcessingSetWriter {
    /// Start a new processing set at `processing_set`, staged in a
    /// directory of this writer's own beside it.
    ///
    /// # Errors
    ///
    /// When `processing_set` exists or the staging store cannot be made.
    pub fn create(processing_set: &Path) -> Result<Self, ObservationError> {
        if processing_set.exists() {
            return Err(request_error(format!(
                "{} exists",
                processing_set.display()
            )));
        }
        let staging = create_staging(processing_set)?;
        let writer = Self {
            staging: Some(staging.clone()),
            destination: processing_set.to_path_buf(),
            store: Arc::new(FilesystemStore::new(&staging).map_err(storage)?),
            registry: Arc::default(),
        };
        // From here a failure drops the writer, which removes its staging.
        create_group(&writer.store, "/", json!({"type": "processing_set"}))?;
        Ok(writer)
    }

    /// Declare the dataset that `catalogue`'s partition `partition`, its
    /// data groups `data_groups` and `grid` describe, stored as `options`
    /// say, and return its writer ([`Msv4Writer`]). The first group carries
    /// every role; each further one only its visibilities, sharing the
    /// first's flags, weights and UVW (MSv2's `corrected` and `model`).
    /// Every coordinate and sub-dataset is written now; the payload follows
    /// from the writer's batches.
    ///
    /// # Errors
    ///
    /// When the processing set already has a dataset of that name, the
    /// declaration is inconsistent (a group missing, declared twice, or
    /// not sharing the first's flags, weights and UVW), or a frame, scale
    /// or unit is outside the profile.
    pub fn dataset(
        &self,
        catalogue: &Catalogue,
        partition: PartitionId,
        data_groups: &[&str],
        grid: &TimeBaselineGrid,
        options: &Msv4StorageOptions,
    ) -> Result<Msv4Writer, ObservationError> {
        self.registry.declare(&options.dataset_name)?;
        Msv4Writer::create(
            &self.store,
            &self.registry,
            catalogue,
            partition,
            data_groups,
            grid,
            options,
        )
    }

    /// Publish the processing set: every declared dataset must be complete
    /// ([`Msv4Writer::finish`]). The staging directory is renamed to the
    /// destination, which must still not exist: the rename refuses to
    /// replace it (Linux `renameat2(RENAME_NOREPLACE)`, macOS
    /// `renamex_np(RENAME_EXCL)`).
    ///
    /// # Errors
    ///
    /// When no dataset was declared or one is not complete, or the
    /// destination appeared meanwhile (another writer published first):
    /// the staging directory is then removed and nothing is replaced.
    pub fn finish(mut self) -> Result<PathBuf, ObservationError> {
        if let Some(name) = self.registry.incomplete() {
            return Err(request_error(format!("dataset {name} is not complete")));
        }
        if self
            .registry
            .datasets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
        {
            return Err(request_error("the processing set has no dataset"));
        }
        let staging = self.staging.take().expect("staging until finish");
        match rename_noreplace(&staging, &self.destination) {
            Ok(()) => Ok(std::mem::take(&mut self.destination)),
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging);
                if error.kind() == io::ErrorKind::AlreadyExists {
                    Err(request_error(format!(
                        "{} appeared while the processing set was written; not replaced",
                        self.destination.display()
                    )))
                } else {
                    Err(storage(error))
                }
            }
        }
    }
}

impl Drop for Msv4ProcessingSetWriter {
    fn drop(&mut self) {
        if let Some(staging) = self.staging.take() {
            let _ = std::fs::remove_dir_all(staging);
        }
    }
}

impl std::fmt::Debug for Msv4ProcessingSetWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Msv4ProcessingSetWriter")
            .field("staging", &self.staging)
            .field("destination", &self.destination)
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

/// A new staging directory beside `destination`, created exclusively under
/// a name unique to this process and call: two live writers, or a retry
/// after a crash, never share one, and an existing directory is never
/// adopted.
fn create_staging(destination: &Path) -> Result<PathBuf, ObservationError> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let name = destination
        .file_name()
        .ok_or_else(|| request_error(format!("{} has no file name", destination.display())))?
        .to_string_lossy()
        .into_owned();
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    loop {
        let candidate = parent.join(format!(
            ".{name}.staging-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(storage(error)),
        }
    }
}

/// Rename `from` to `to`, failing with `AlreadyExists` rather than replace
/// anything at `to`: `renameat2(RENAME_NOREPLACE)`, or the checked rename
/// where the filesystem does not offer it.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    let (c_from, c_to) = (c_path(from)?, c_path(to)?);
    // SAFETY: both paths are NUL-terminated C strings that outlive the
    // call; renameat2 reads them only.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            c_from.as_ptr(),
            libc::AT_FDCWD,
            c_to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) {
        checked_rename(from, to)
    } else {
        Err(error)
    }
}

/// [`rename_noreplace`] through `renamex_np(RENAME_EXCL)`.
#[cfg(target_os = "macos")]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    let (c_from, c_to) = (c_path(from)?, c_path(to)?);
    // SAFETY: as for renameat2: NUL-terminated paths, read only.
    let result = unsafe { libc::renamex_np(c_from.as_ptr(), c_to.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// [`rename_noreplace`] where no exclusive rename is available.
#[cfg(not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))))]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    checked_rename(from, to)
}

/// A check and a rename: a narrow race this trial accepts where the
/// platform or filesystem offers no exclusive rename.
#[cfg(not(target_os = "macos"))]
fn checked_rename(from: &Path, to: &Path) -> io::Result<()> {
    if to.exists() {
        return Err(io::Error::from(io::ErrorKind::AlreadyExists));
    }
    std::fs::rename(from, to)
}

/// `path` as a C string.
#[cfg(any(target_os = "macos", all(target_os = "linux", target_env = "gnu")))]
fn c_path(path: &Path) -> io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    Ok(std::ffi::CString::new(path.as_os_str().as_bytes())?)
}
