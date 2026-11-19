// SPDX-License-Identifier: LGPL-3.0-or-later

//! Share each open native image's pixels and table metadata across Python tools.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{Table, TableError, TableOptions, TableUseLock, TableWriteLock};
use pyo3::prelude::*;

use super::handle::{ImageHandle, raised};
use super::stub;

pub(super) struct SharedImage {
    handle: Mutex<ImageHandle>,
    use_lock: Option<Arc<TableUseLock>>,
    shareable: bool,
}

type OpenImages = BTreeMap<PathBuf, Vec<Weak<SharedImage>>>;

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
                shareable: false,
            }),
            key: None,
        }
    }

    pub(super) fn open(path: &Path) -> PyResult<Self> {
        if path.is_file() {
            return ImageHandle::open(path).map(Self::temporary).map_err(raised);
        }
        let key = key(path)?;
        let shared = Self::shared(&registry(), &key);
        if let Some(shared) = shared {
            shared.require_lock(&mut shared.lock(), casa_tables::LockType::Read)?;
            return Ok(shared);
        }

        // The data lock may wait for a different process. Keep the registry
        // available to other image names while this open is in progress.
        let opened = Self::open_native(path, &key, Some(0), TableUseLock::acquire_waiting)?;
        let shared = {
            let mut registry = registry();
            if let Some(shared) = Self::shared(&registry, &key) {
                shared
            } else {
                opened.register(&mut registry);
                return Ok(opened);
            }
        };
        drop(opened);
        shared.require_lock(&mut shared.lock(), casa_tables::LockType::Read)?;
        Ok(shared)
    }

    pub(super) fn open_created(registry: &mut OpenImages, path: &Path) -> PyResult<Self> {
        let key = key(path)?;
        let opened = Self::open_native(path, &key, Some(1), TableUseLock::acquire)?;
        if !opened
            .lock()
            .lock_storage(casa_tables::LockType::Write, 1)?
        {
            return Err(raised("Could not acquire the new image write lock"));
        }
        opened.register(registry);
        Ok(opened)
    }

    fn shared(registry: &OpenImages, key: &Path) -> Option<Self> {
        registry
            .get(key)
            .into_iter()
            .flatten()
            .filter_map(Weak::upgrade)
            .find(|image| {
                image.shareable
                    && image
                        .use_lock
                        .as_ref()
                        .is_none_or(|lease| lease.matches_directory(key))
            })
            .map(|image| Self {
                image,
                key: Some(key.to_path_buf()),
            })
    }

    /// V1 Data callers get a fresh view on every open. The compatibility
    /// view is registered for safe replacement and reads the resolved path.
    pub(super) fn open_data(
        registry: &mut OpenImages,
        path: &Path,
        writable: bool,
    ) -> PyResult<Self> {
        let key = key(path)?;
        let acquire = if writable {
            TableUseLock::acquire
        } else {
            TableUseLock::acquire_readonly
        };
        let opened = Self::open_native(&key, &key, None, acquire)?;
        opened.register(registry);
        Ok(opened)
    }

    fn open_native(
        path: &Path,
        key: &Path,
        lock_attempts: Option<u32>,
        acquire: fn(&Path) -> Result<TableUseLock, TableError>,
    ) -> PyResult<Self> {
        let use_lock = key
            .join("table.dat")
            .is_file()
            .then(|| acquire(key).map(Arc::new))
            .transpose()
            .map_err(raised)?;
        let image = Arc::new(SharedImage {
            handle: Mutex::new(ImageHandle::open_native(path, lock_attempts).map_err(raised)?),
            use_lock,
            shareable: lock_attempts.is_some(),
        });
        let opened = Self {
            image,
            key: Some(key.to_path_buf()),
        };
        opened.check_bound_name(&opened.lock())?;
        Ok(opened)
    }

    fn register(&self, registry: &mut OpenImages) {
        if let Some(key) = &self.key {
            registry
                .entry(key.clone())
                .or_default()
                .push(Arc::downgrade(&self.image));
        }
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, ImageHandle> {
        self.image
            .handle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn is_persistent(&self) -> bool {
        self.key.is_some() || self.lock().fits().is_some()
    }

    pub(super) fn check_directory(&self) -> PyResult<()> {
        if let (Some(key), Some(lease)) = (&self.key, &self.image.use_lock)
            && !lease.matches_directory(key)
        {
            return Err(raised(format!(
                "The image at {} has changed",
                key.display()
            )));
        }
        Ok(())
    }

    pub(super) fn check_bound_name(&self, handle: &ImageHandle) -> PyResult<()> {
        self.check_directory()?;
        if let (Some(key), Some(path)) = (&self.key, handle.path())
            && path != key
            && !path.canonicalize().is_ok_and(|actual| &actual == key)
        {
            return Err(raised(format!(
                "The image at {} has changed",
                path.display()
            )));
        }
        Ok(())
    }

    pub(super) fn rename(&self, name: &str, overwrite: bool, history: bool) -> PyResult<Self> {
        if name.is_empty() {
            return Err(raised("Exception: new file name must be specified."));
        }
        let Some(source) = &self.key else {
            let fits_path = {
                let image = self.lock();
                image
                    .fits()
                    .and_then(|_| image.path())
                    .map(Path::to_path_buf)
            };
            if let Some(old_name) = fits_path {
                return super::fits_rename::rename(&old_name, name, overwrite, history);
            }
            return Err(raised(
                "Exception: This image tool is not associated with a persistent disk file. It cannot be renamed.",
            ));
        };
        let old_name = {
            let image = self.lock();
            self.check_bound_name(&image).map_err(|_| {
                raised(format!(
                    "Cannot rename image {} because the image at that path has changed",
                    source.display()
                ))
            })?;
            image.path().unwrap_or(source).to_path_buf()
        };
        let absolute = std::path::absolute(name).map_err(raised)?;
        let parent = absolute
            .parent()
            .ok_or_else(|| raised("Invalid output image path"))?;
        let directory = parent.canonicalize().map_err(|_| {
            let argument = Path::new(name).parent().unwrap_or(Path::new("."));
            raised(format!(
                "RegularFile::copy/move: target directory {} is not writable",
                argument.display()
            ))
        })?;
        let target = directory.join(
            absolute
                .file_name()
                .ok_or_else(|| raised("Invalid output image name"))?,
        );
        let mut registry = registry();
        let target_metadata = match target.symlink_metadata() {
            Ok(metadata) if overwrite => Some(metadata),
            Ok(_) => {
                return Err(raised(format!(
                    "Exception: File {name} exists, and the user does not want to remove it.."
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(raised(error)),
        };
        let log_rename = |image: &mut ImageHandle| {
            if history {
                image.append_history_at(
                    "::image::rename",
                    &[
                        "Ran ia.rename".into(),
                        format!("ia.rename(name={name:?}, overwrite={overwrite})"),
                        format!("original name was {}", old_name.display()),
                    ],
                )?;
            }
            Ok::<(), PyErr>(())
        };
        if std::ffi::OsStr::new(name) == old_name.as_os_str() {
            return Err(raised(
                "Exception: Specified output name is the same as the current image name.",
            ));
        }
        if source == &target {
            if self
                .image
                .use_lock
                .as_ref()
                .is_some_and(|lease| !lease.matches_directory(source))
            {
                return Err(raised(format!(
                    "Cannot rename image {} because the image at that path has changed",
                    source.display()
                )));
            }
            log_rename(&mut self.lock())?;
            return Ok(Self {
                image: Arc::clone(&self.image),
                key: Some(source.clone()),
            });
        }
        if old_name
            .symlink_metadata()
            .is_ok_and(|entry| entry.is_symlink())
            || target_metadata
                .as_ref()
                .is_some_and(|entry| entry.is_symlink())
            || target.starts_with(source)
            || source.starts_with(&target)
        {
            return Err(stub("rename").unwrap_err());
        }
        for (path, expected) in [(source.as_path(), 1), (target.as_path(), 0)] {
            if conflicting_images(&registry, path, expected)
                || crate::tools::table::path_is_in_use(path)
            {
                return Err(raised(format!(
                    "Cannot rename image because {} is still open in this process",
                    path.display()
                )));
            }
        }
        let mut locks = BTreeMap::new();
        {
            let mut image = self.lock();
            self.check_bound_name(&image).map_err(|_| {
                raised(format!(
                    "Cannot rename image {} because the image at that path has changed",
                    source.display()
                ))
            })?;
            image.unlock_storage()?;
        }
        lock_table_tree(source, &mut locks)?;
        if target_metadata.as_ref().is_some_and(|entry| entry.is_dir()) {
            lock_table_tree(&target, &mut locks)?;
        }
        if self
            .image
            .use_lock
            .as_ref()
            .is_some_and(|lease| !lease.matches_directory(source))
        {
            return Err(raised(format!(
                "Cannot rename image {} because the image at that path has changed",
                source.display()
            )));
        }
        let backup = move_native_table(source, &target, target_metadata.is_some())?;
        // Keep both root locks during reattachment, but release child
        // locks so the log-table writer can append the rename history.
        locks.retain(|path, _| path == source || path == &target);
        let reopen = || {
            let mut image = ImageHandle::open_native(&target, self.image.shareable.then_some(1))
                .map_err(raised)?;
            log_rename(&mut image)?;
            Ok::<_, PyErr>(image)
        };
        let image = match reopen() {
            Ok(image) => Arc::new(SharedImage {
                handle: Mutex::new(image),
                use_lock: self.image.use_lock.clone(),
                shareable: self.image.shareable,
            }),
            Err(error) => return restore_native_move(source, &target, backup, error),
        };
        registry.remove(source);
        registry.insert(target.clone(), vec![Arc::downgrade(&image)]);
        Ok(Self {
            image,
            key: Some(target),
        })
    }

    pub(super) fn remove(&self) -> PyResult<String> {
        let Some(key) = &self.key else {
            if let Some(path) = self.lock().path() {
                return Err(raised(format!(
                    "Cannot delete file {} because table is not writable",
                    path.display()
                )));
            }
            return Err(raised(
                "Exception: This image tool is not associated with a persistent disk file. It cannot be deleted.",
            ));
        };
        let name = self.lock().path().unwrap_or(key).to_path_buf();
        let mut registry = registry();
        if conflicting_images(&registry, key, 1) {
            return Err(in_use(key, "this"));
        }
        {
            let mut image = self.lock();
            self.check_bound_name(&image).map_err(|_| {
                raised(format!(
                    "Cannot delete file {} because the image at that path has changed",
                    key.display()
                ))
            })?;
            image.unlock_storage()?;
        }
        remove_native_table(&name, key, self.image.use_lock.as_deref())?;
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

pub(super) fn conflicting_images(
    registry: &OpenImages,
    path: &Path,
    expected_at_path: usize,
) -> bool {
    registry.iter().any(|(open_path, images)| {
        if open_path == path {
            images.iter().map(Weak::strong_count).sum::<usize>() > expected_at_path
        } else {
            (open_path.starts_with(path) || path.starts_with(open_path))
                && images.iter().any(|image| image.strong_count() > 0)
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
    let mut leases = BTreeMap::new();
    lock_table_tree(path, &mut leases)?;
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

pub(super) fn lock_table_tree(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> PyResult<()> {
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
    for ancestor in parent.ancestors() {
        if ancestor.join("table.dat").is_file() {
            lock_table_for_deletion(ancestor, leases)?;
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
            lock_table_for_deletion(&directory, leases)?;
        }
        for entry in fs::read_dir(&directory).map_err(raised)? {
            let entry = entry.map_err(raised)?;
            if entry.file_type().map_err(raised)?.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Ok(())
}

fn lock_table_for_deletion(
    directory: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> PyResult<()> {
    if leases.contains_key(directory) {
        return Ok(());
    }
    let lease = TableWriteLock::acquire(directory, 1).map_err(|error| match error {
        TableError::LockFailed { .. } => in_use(directory, "another"),
        _ => raised(error),
    })?;
    if lease.is_multi_used() {
        return Err(in_use(directory, "another"));
    }
    leases.insert(directory.to_path_buf(), lease);
    Ok(())
}

fn move_error(error: std::io::Error) -> PyErr {
    if error.kind() == std::io::ErrorKind::CrossesDevices {
        stub("rename").unwrap_err()
    } else {
        raised(error)
    }
}

fn move_native_table(
    source: &Path,
    target: &Path,
    replaces: bool,
) -> PyResult<Option<tempfile::TempDir>> {
    if !replaces {
        rename_without_replacing(source, target).map_err(move_error)?;
        return Ok(None);
    }
    let parent = source
        .parent()
        .ok_or_else(|| raised("Invalid source image path"))?;
    let backup = tempfile::tempdir_in(parent).map_err(raised)?;
    exchange_directories(source, target).map_err(move_error)?;
    if let Err(error) = rename_without_replacing(source, &backup.path().join("previous")) {
        if let Err(restore) = exchange_directories(source, target) {
            return Err(raised(format!(
                "Could not complete image rename ({error}) or restore by exchanging {} and {} ({restore})",
                source.display(),
                target.display()
            )));
        }
        return Err(move_error(error));
    }
    Ok(Some(backup))
}

fn restore_native_move<T>(
    source: &Path,
    target: &Path,
    backup: Option<tempfile::TempDir>,
    error: PyErr,
) -> PyResult<T> {
    let Some(backup) = backup else {
        rename_without_replacing(target, source).map_err(|restore| {
            raised(format!(
                "{error}. Could not restore image {}: {restore}",
                source.display()
            ))
        })?;
        return Err(error);
    };
    let previous = backup.path().join("previous");
    let restore = exchange_directories(target, &previous)
        .and_then(|()| rename_without_replacing(&previous, source));
    if let Err(restore) = restore {
        let recovery = backup.keep();
        return Err(raised(format!(
            "{error}. Could not restore image names: {restore}. Inspect {}",
            recovery.display()
        )));
    }
    Err(error)
}

impl Drop for AttachedImage {
    fn drop(&mut self) {
        if let Some(key) = &self.key {
            let mut registry = registry();
            if Arc::strong_count(&self.image) == 1
                && let Some(images) = registry.get_mut(key)
            {
                let current = Arc::downgrade(&self.image);
                images.retain(|image| !image.ptr_eq(&current) && image.strong_count() > 0);
                if images.is_empty() {
                    registry.remove(key);
                }
            }
        }
    }
}

/// Registry keys resolve aliases to an existing native table directory.
pub(super) fn key(path: &Path) -> PyResult<PathBuf> {
    path.canonicalize().map_err(raised)
}
