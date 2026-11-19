// SPDX-License-Identifier: LGPL-3.0-or-later

//! Share each open native image's pixels and table metadata across Python tools.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use casa_tables::{Table, TableError, TableOptions, TableUseLock, TableWriteLock};
use pyo3::prelude::*;

use super::handle::{ImageHandle, raised};

pub(super) struct SharedImage {
    handle: Mutex<ImageHandle>,
    use_lock: Option<TableUseLock>,
}

type OpenImages = BTreeMap<PathBuf, Weak<SharedImage>>;

static ATTACHED: Mutex<OpenImages> = Mutex::new(BTreeMap::new());

pub(super) fn registry() -> MutexGuard<'static, OpenImages> {
    ATTACHED.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(super) struct AttachedImage {
    image: Arc<SharedImage>,
    key: Option<PathBuf>,
}

impl AttachedImage {
    pub(super) fn temporary(image: ImageHandle) -> Self {
        Self {
            image: Arc::new(SharedImage {
                handle: Mutex::new(image),
                use_lock: None,
            }),
            key: None,
        }
    }

    pub(super) fn open(registry: &mut OpenImages, path: &Path) -> PyResult<Self> {
        let key = key(path)?;
        if let Some(image) = registry.get(&key).and_then(Weak::upgrade)
            && image
                .use_lock
                .as_ref()
                .is_none_or(|lease| lease.matches_directory(&key))
        {
            return Ok(Self {
                image,
                key: Some(key),
            });
        }
        let use_lock = key
            .join("table.dat")
            .is_file()
            .then(|| TableUseLock::acquire(&key))
            .transpose()
            .map_err(raised)?;
        let image = Arc::new(SharedImage {
            handle: Mutex::new(ImageHandle::open(path).map_err(raised)?),
            use_lock,
        });
        registry.insert(key.clone(), Arc::downgrade(&image));
        Ok(Self {
            image,
            key: Some(key),
        })
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, ImageHandle> {
        self.image
            .handle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn is_persistent(&self) -> bool {
        self.key.is_some()
    }

    pub(super) fn remove(&self) -> PyResult<String> {
        let Some(key) = &self.key else {
            return Err(raised(
                "Exception: This image tool is not associated with a persistent disk file. It cannot be deleted.",
            ));
        };
        let name = self.lock().path().unwrap_or(key).to_path_buf();
        let mut registry = registry();
        if conflicting_images(&registry, key, 1) {
            return Err(in_use(key, "this"));
        }
        remove_native_table(&name, key, self.image.use_lock.as_ref())?;
        registry.remove(key);
        Ok(name.to_string_lossy().into_owned())
    }
}

/// Deletes an unopened table by name. The image tool also accepts a
/// non-image casacore table here, as CASA's `removefile` does.
pub(super) fn remove_file(path: &Path) -> PyResult<bool> {
    if !path.is_dir() {
        return Ok(false);
    }
    let name = std::path::absolute(path).map_err(raised)?;
    let key = key(&name)?;
    let registry = registry();
    if conflicting_images(&registry, &key, 0) {
        return Err(in_use(&key, "this"));
    }
    remove_native_table(&name, &key, None)?;
    Ok(true)
}

fn conflicting_images(registry: &OpenImages, path: &Path, expected_at_path: usize) -> bool {
    registry.iter().any(|(open_path, image)| {
        if open_path == path {
            image.strong_count() > expected_at_path
        } else {
            (open_path.starts_with(path) || path.starts_with(open_path)) && image.strong_count() > 0
        }
    })
}

fn in_use(path: &Path, process: &str) -> PyErr {
    raised(format!(
        "Cannot delete file {} because table is still open in {process} process",
        path.display()
    ))
}

fn remove_native_table(name: &Path, path: &Path, expected: Option<&TableUseLock>) -> PyResult<()> {
    Table::open_metadata_only(TableOptions::new(path)).map_err(raised)?;
    if crate::tools::table::path_is_in_use(path) {
        return Err(in_use(path, "this"));
    }
    let _leases = deletion_locks(path)?;
    if expected.is_some_and(|guard| !guard.matches_directory(path)) {
        return Err(raised(format!(
            "Cannot delete file {} because the image at that path has changed",
            path.display()
        )));
    }
    let named_link = name
        .symlink_metadata()
        .is_ok_and(|entry| entry.is_symlink());
    if named_link
        && let Some(parent) = name.parent()
        && fs::metadata(parent)
            .map_err(raised)?
            .permissions()
            .readonly()
    {
        return Err(raised(format!(
            "Cannot delete file {} because its directory is not writable",
            name.display()
        )));
    }
    fs::remove_dir_all(path).map_err(raised)?;
    if named_link {
        // The table has been removed even if the alias's permissions changed
        // after the preflight check.
        let _ = fs::remove_file(name);
    }
    Ok(())
}

fn deletion_locks(path: &Path) -> PyResult<Vec<TableWriteLock>> {
    let not_writable = |name: &Path| {
        raised(format!(
            "Cannot delete file {} because table is not writable",
            name.display()
        ))
    };
    let parent = path.parent().ok_or_else(|| not_writable(path))?;
    if fs::metadata(parent)
        .map_err(raised)?
        .permissions()
        .readonly()
    {
        return Err(not_writable(path));
    }
    let mut leases = Vec::new();
    for ancestor in parent.ancestors() {
        if ancestor.join("table.dat").is_file() {
            leases.push(lock_table_for_deletion(ancestor)?);
        }
    }
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        if fs::metadata(&directory)
            .map_err(raised)?
            .permissions()
            .readonly()
        {
            return Err(not_writable(&directory));
        }
        let control = directory.join("table.dat");
        if control.is_file() {
            OpenOptions::new()
                .write(true)
                .open(control)
                .map_err(|_| not_writable(&directory))?;
            leases.push(lock_table_for_deletion(&directory)?);
        }
        for entry in fs::read_dir(&directory).map_err(raised)? {
            let entry = entry.map_err(raised)?;
            if entry.file_type().map_err(raised)?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Ok(leases)
}

fn lock_table_for_deletion(directory: &Path) -> PyResult<TableWriteLock> {
    let lease = TableWriteLock::acquire(directory, 1).map_err(|error| match error {
        TableError::LockFailed { .. } => in_use(directory, "another"),
        _ => raised(error),
    })?;
    if lease.is_multi_used() {
        return Err(in_use(directory, "another"));
    }
    Ok(lease)
}

impl Drop for AttachedImage {
    fn drop(&mut self) {
        if let Some(key) = &self.key {
            let mut registry = registry();
            if Arc::strong_count(&self.image) == 1
                && registry
                    .get(key)
                    .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&self.image)))
            {
                registry.remove(key);
            }
        }
    }
}

/// Registry keys resolve aliases to an existing native table directory.
pub(super) fn key(path: &Path) -> PyResult<PathBuf> {
    path.canonicalize().map_err(raised)
}
