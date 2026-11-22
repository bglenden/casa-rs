// SPDX-License-Identifier: LGPL-3.0-or-later

//! Share each open native image's pixels and table metadata across Python tools.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_images::fits_read::FitsToolImport;
use casa_tables::publication::check_write_access;
use casa_tables::{Table, TableError, TableInfo, TableOptions, TableUseLock, TableWriteLock};
use casa_types::RecordValue;
use pyo3::prelude::*;

use crate::tools::coordsys::PyCoordsys;

use super::beam_view::BeamView;
use super::handle::{ImageHandle, Storage, raised, with_pixels};
use super::miriad_rename::MiriadDirectory;
use super::native_rename::{
    NativeTableMove, check_path_relationship, move_native_table, move_read_only_directory,
    native_destination,
};
use super::saved_expression::SavedExpressionDirectory;

pub(super) struct SharedImage {
    handle: Mutex<ImageHandle>,
    use_lock: Option<Arc<TableUseLock>>,
    miriad_directory: Option<MiriadDirectory>,
    saved_expression_directory: Option<SavedExpressionDirectory>,
    expression_operands: Vec<PathBuf>,
    shareable: bool,
    coordinates_replaced: AtomicBool,
}

impl SharedImage {
    pub(super) fn lock(&self) -> MutexGuard<'_, ImageHandle> {
        self.handle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub(super) type OpenImages = BTreeMap<PathBuf, Vec<Weak<SharedImage>>>;

static ATTACHED: Mutex<OpenImages> = Mutex::new(BTreeMap::new());

pub(super) fn registry() -> MutexGuard<'static, OpenImages> {
    ATTACHED.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(super) fn read_locked<T>(
    attachment: &AttachedImage,
    read: impl FnOnce(&ImageHandle) -> PyResult<T>,
) -> PyResult<T> {
    let mut handle = attachment.lock();
    attachment.require_pixel_read_lock(&mut handle)?;
    let staged = read(&handle);
    let release = attachment.release_requested_lock(&mut handle);
    let result = staged?;
    release?;
    Ok(result)
}

pub(super) struct AttachedImage {
    image: Arc<SharedImage>,
    key: Option<PathBuf>,
    native_name: Option<PathBuf>,
    registered: bool,
    beam_view: Mutex<BeamView>,
    retained_inputs: Vec<Self>,
    operand_paths: Vec<PathBuf>,
    live_coordinates: Option<LiveCoordinates>,
}

#[derive(Clone)]
struct LiveCoordinates {
    record: RecordValue,
    system: CoordinateSystem,
}

impl AttachedImage {
    pub(super) fn temporary(image: ImageHandle) -> Self {
        Self::without_table(image, None, None)
    }

    pub(super) fn with_saved_expression(
        image: ImageHandle,
        directory: SavedExpressionDirectory,
    ) -> Self {
        Self::without_table(image, None, Some(directory))
    }

    fn without_table(
        image: ImageHandle,
        miriad_directory: Option<MiriadDirectory>,
        saved_expression_directory: Option<SavedExpressionDirectory>,
    ) -> Self {
        let beam_view = BeamView::from_image(&image);
        let expression_operands = image.concat_expression_operands();
        Self {
            image: Arc::new(SharedImage {
                handle: Mutex::new(image),
                use_lock: None,
                miriad_directory,
                saved_expression_directory,
                expression_operands,
                shareable: false,
                coordinates_replaced: AtomicBool::new(false),
            }),
            key: None,
            native_name: None,
            registered: false,
            beam_view: Mutex::new(beam_view),
            retained_inputs: Vec::new(),
            operand_paths: Vec::new(),
            live_coordinates: None,
        }
    }

    pub(super) fn open(path: &Path) -> PyResult<Self> {
        Self::open_resolved(path, path, path)
    }

    pub(super) fn open_miriad(path: &Path) -> PyResult<Self> {
        let directory = MiriadDirectory::open(path)?;
        let image = ImageHandle::open_miriad(path).map_err(raised)?;
        directory.check(path)?;
        Ok(Self::without_table(image, Some(directory), None))
    }

    pub(super) fn open_resolved(path: &Path, probe: &Path, name: &Path) -> PyResult<Self> {
        Self::open_resolved_with_cache(path, probe, name, true)
    }

    pub(super) fn open_resolved_with_cache(
        path: &Path,
        probe: &Path,
        name: &Path,
        component_cache: bool,
    ) -> PyResult<Self> {
        Self::open_with_read_lock(path, probe, name, true, component_cache)
    }

    pub(super) fn open_for_ordered_read(path: &Path) -> PyResult<Self> {
        Self::open_with_read_lock(path, path, path, false, true)
    }

    pub(super) fn name_path<'a>(&'a self, image: &'a ImageHandle) -> Option<&'a Path> {
        self.native_name.as_deref().or_else(|| image.path())
    }

    pub(super) fn native_path(&self) -> Option<&Path> {
        self.key.as_deref()
    }

    pub(super) fn saved_expression_directory(&self) -> Option<&SavedExpressionDirectory> {
        self.image.saved_expression_directory.as_ref()
    }

    fn open_with_read_lock(
        path: &Path,
        probe: &Path,
        name: &Path,
        keep_read_lock: bool,
        component_cache: bool,
    ) -> PyResult<Self> {
        if probe.is_file() {
            let source =
                FitsToolImport::open_attachment_named(path, probe, name).map_err(raised)?;
            return ImageHandle::open_fits(path, source)
                .map(Self::temporary)
                .map_err(raised);
        }
        let unrecognized = || {
            raised(format!(
                "Exception: Unable to open image {}.",
                name.display()
            ))
        };
        if path != probe && (!probe.is_dir() || !path.is_dir()) {
            return Err(unrecognized());
        }
        if casa_images::concat_file::is_image_concat(probe) {
            return super::concatenate::virtual_image::open(path);
        }
        if casa_images::expr_file::is_image_expr(probe) {
            return super::saved_expression::open(path);
        }
        let key = key(path)?;
        let has_table_control = key.join("table.dat").is_file();
        let paged = has_table_control
            && fs::read_to_string(key.join("table.info"))
                .is_ok_and(|info| TableInfo::parse(&info).table_type == "Image");
        if !paged && !casa_images::ComponentListImage::recognizes_native(&key) {
            if ImageHandle::recognizes_miriad(probe, &key) {
                return Self::open_miriad(path);
            }
            return Err(unrecognized());
        }
        let name = std::path::absolute(path).map_err(raised)?;
        let shared = Self::shared(&registry(), &key, &name);
        if let Some(shared) = shared {
            if keep_read_lock {
                shared.read_current_beams()?;
            }
            return Ok(shared);
        }

        // The data lock may wait for a different process. Keep the registry
        // available to other image names while this open is in progress.
        let mut opened = Self::open_native(
            &name,
            &key,
            Some(0),
            TableUseLock::acquire_waiting_readonly,
            component_cache,
        )?;
        if !keep_read_lock {
            opened.lock().unlock_storage()?;
        }
        let shared = {
            let mut registry = registry();
            if let Some(shared) = Self::shared(&registry, &key, &name) {
                shared
            } else {
                opened.register(&mut registry);
                return Ok(opened);
            }
        };
        drop(opened);
        if keep_read_lock {
            shared.read_current_beams()?;
        }
        Ok(shared)
    }

    fn read_current_beams(&self) -> PyResult<()> {
        let mut handle = self.lock();
        self.require_lock(&mut handle, casa_tables::LockType::Read)?;
        *self.beam_view() = BeamView::from_image(&handle);
        Ok(())
    }

    pub(super) fn open_created(registry: &mut OpenImages, path: &Path) -> PyResult<Self> {
        let key = key(path)?;
        Self::open_created_as(registry, path, &key)
    }

    pub(super) fn open_created_as(
        registry: &mut OpenImages,
        name: &Path,
        key: &Path,
    ) -> PyResult<Self> {
        let mut opened = Self::open_native(name, key, Some(1), TableUseLock::acquire, true)?;
        if !opened
            .lock()
            .lock_storage(casa_tables::LockType::Write, 1)?
        {
            return Err(raised("Could not acquire the new image write lock"));
        }
        opened.register(registry);
        Ok(opened)
    }

    pub(super) fn with_virtual_concat(
        image: ImageHandle,
        name: &Path,
        registry: &mut OpenImages,
    ) -> PyResult<Self> {
        let mut attached = Self::temporary(image);
        attached.key = Some(key(name)?);
        attached.native_name = Some(std::path::absolute(name).map_err(raised)?);
        attached.check_bound_name(&attached.lock())?;
        attached.register(registry);
        Ok(attached)
    }

    fn shared(registry: &OpenImages, key: &Path, name: &Path) -> Option<Self> {
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
                native_name: Some(name.to_path_buf()),
                registered: true,
                beam_view: Mutex::default(),
                retained_inputs: Vec::new(),
                operand_paths: Vec::new(),
                live_coordinates: None,
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
        let mut opened = Self::open_native(&key, &key, None, acquire, true)?;
        opened.register(registry);
        Ok(opened)
    }

    fn open_native(
        path: &Path,
        key: &Path,
        lock_attempts: Option<u32>,
        acquire: fn(&Path) -> Result<TableUseLock, TableError>,
        component_cache: bool,
    ) -> PyResult<Self> {
        let use_lock = key
            .join("table.dat")
            .is_file()
            .then(|| acquire(key).map(Arc::new))
            .transpose()
            .map_err(raised)?;
        let handle = ImageHandle::open_native_with_cache(key, lock_attempts, component_cache)
            .map_err(raised)?;
        let shareable = lock_attempts.is_some() && handle.image_type_name() != "ComponentListImage";
        let beam_view = BeamView::from_image(&handle);
        let expression_operands = handle.concat_expression_operands();
        let image = Arc::new(SharedImage {
            handle: Mutex::new(handle),
            use_lock,
            miriad_directory: None,
            saved_expression_directory: None,
            expression_operands,
            shareable,
            coordinates_replaced: AtomicBool::new(false),
        });
        let opened = Self {
            image,
            key: Some(key.to_path_buf()),
            native_name: Some(std::path::absolute(path).map_err(raised)?),
            registered: false,
            beam_view: Mutex::new(beam_view),
            retained_inputs: Vec::new(),
            operand_paths: Vec::new(),
            live_coordinates: None,
        };
        opened.check_bound_name(&opened.lock())?;
        Ok(opened)
    }

    fn register(&mut self, registry: &mut OpenImages) {
        if let Some(key) = &self.key {
            registry
                .entry(key.clone())
                .or_default()
                .push(Arc::downgrade(&self.image));
            self.registered = true;
        }
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, ImageHandle> {
        self.image.lock()
    }

    pub(super) fn share_image(&self) -> Arc<SharedImage> {
        Arc::clone(&self.image)
    }

    pub(super) fn capture_live_coordinates(
        image: &ImageHandle,
    ) -> PyResult<Option<(RecordValue, CoordinateSystem)>> {
        let coordinates = image.coordinates();
        let has_samples =
            (0..coordinates.n_coordinates()).any(|index| match coordinates.coordinate(index) {
                CoordinateModel::Spectral(spectral) => !spectral.has_linear_mapping(),
                CoordinateModel::Tabular(_) => true,
                _ => false,
            });
        if !has_samples {
            return Ok(None);
        }
        let record = image.coordinate_record();
        let system = CoordinateSystem::from_live_record(&record).map_err(raised)?;
        Ok(Some((record, system)))
    }

    pub(super) fn keep_live_coordinates(&mut self, record: RecordValue, system: CoordinateSystem) {
        self.live_coordinates = Some(LiveCoordinates { record, system });
    }

    pub(super) fn coordinates_changed(&self) {
        self.image
            .coordinates_replaced
            .store(true, Ordering::Relaxed);
    }

    pub(super) fn coordinate_view<'a>(
        &'a self,
        image: &'a ImageHandle,
    ) -> (&'a CoordinateSystem, RecordValue) {
        let record = image.coordinate_record();
        let system = self
            .live_coordinates
            .as_ref()
            .filter(|live| {
                !self.image.coordinates_replaced.load(Ordering::Relaxed) && live.record == record
            })
            .map_or_else(|| image.coordinates(), |live| &live.system);
        (system, record)
    }

    pub(super) fn coordsys(&self, image: &ImageHandle) -> PyResult<PyCoordsys> {
        let (coordinates, record) = self.coordinate_view(image);
        PyCoordsys::from_image_coordinates(coordinates, record)
    }

    pub(super) fn beam_view(&self) -> MutexGuard<'_, BeamView> {
        self.beam_view
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn open_expression_inputs<'a>(
        paths: impl Iterator<Item = &'a Path>,
    ) -> PyResult<Vec<Self>> {
        paths
            .filter(|path| !path.is_file())
            .map(Self::open)
            .collect()
    }

    pub(super) fn open_concat_expression_operand(path: &Path) -> PyResult<Self> {
        if ImageHandle::recognizes_miriad(path, path) {
            return Self::open_miriad(path);
        }
        Self::open_native(
            path,
            path,
            Some(0),
            TableUseLock::acquire_waiting_readonly,
            true,
        )
    }

    pub(super) fn retain_expression_inputs(&mut self, inputs: Vec<Self>, paths: Vec<PathBuf>) {
        self.retained_inputs = inputs;
        self.operand_paths = paths;
    }

    pub(super) fn expression_inputs(&self) -> &[Self] {
        &self.retained_inputs
    }

    pub(super) fn operand_paths(&self) -> &[PathBuf] {
        &self.operand_paths
    }

    pub(super) fn is_persistent(&self) -> PyResult<bool> {
        let image = self.lock();
        if image.is_saved_expression() || image.image_type_name() == "ImageConcat" {
            image.check_single_precision("ispersistent")?;
        }
        Ok(self.key.is_some() || image.path().is_some())
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
        with_pixels!(handle, storage => {
            if let Storage::Temp { image, .. } = storage
                && let Some(source) = image.concat_source()
            {
                source.check_directory().map_err(raised)?;
            }
        });
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

    pub(super) fn rename(
        self,
        name: &str,
        expanded: PyResult<Cow<'_, str>>,
        overwrite: bool,
        history: bool,
    ) -> PyResult<Self> {
        if name.is_empty() {
            return Err(raised("Exception: new file name must be specified."));
        }
        let Some(source) = &self.key else {
            return self.rename_non_native(name, expanded, overwrite, history);
        };
        let source_changed = || {
            raised(format!(
                "Cannot rename image {} because the image at that path has changed",
                source.display()
            ))
        };
        let (old_name, virtual_concat) = {
            let image = self.lock();
            self.check_bound_name(&image)
                .map_err(|_| source_changed())?;
            (
                self.name_path(&image).unwrap_or(source).to_path_buf(),
                image.image_type_name() == "ImageConcat",
            )
        };
        let target = native_destination(expanded?.as_ref(), name)?;
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
                return Err(source_changed());
            }
            log_rename(&mut self.lock())?;
            return Ok(Self {
                image: Arc::clone(&self.image),
                key: Some(source.clone()),
                native_name: Some(target.clone()),
                registered: self.registered,
                beam_view: Mutex::new(self.beam_view().clone()),
                retained_inputs: Vec::new(),
                operand_paths: Vec::new(),
                live_coordinates: self.live_coordinates.clone(),
            });
        }
        check_path_relationship(source, &target, name)?;
        if virtual_concat {
            super::concatenate::virtual_image::check_relocation(source, &target)?;
        }
        let replaces_link = target_metadata
            .as_ref()
            .is_some_and(|entry| entry.is_symlink());
        if replaces_link && check_write_access(&target).is_err() {
            return Err(raised(format!(
                "RegularFile::copy/move: target file {} already exists and is not writable",
                target.display()
            )));
        }
        for (path, expected) in [(source.as_path(), 1), (target.as_path(), 0)] {
            let in_table_use = if replaces_link && path == target {
                // The link itself is replaced. Its underlying table and
                // any tools attached to that table retain their storage.
                containing_tables(path).any(crate::tools::table::path_is_in_use)
            } else {
                crate::tools::table::path_is_in_use(path)
            };
            if conflicting_images(&registry, path, expected)
                || in_table_use
                || (path == target && !replaces_link && in_use_by_expression(&registry, path))
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
            self.check_bound_name(&image)
                .map_err(|_| source_changed())?;
            image.unlock_storage()?;
        }
        lock_table_tree(source, &mut locks)?;
        if target_metadata.as_ref().is_some_and(|entry| entry.is_dir()) {
            lock_table_tree(&target, &mut locks)?;
        } else if replaces_link {
            lock_table_ancestors(&target, &mut locks)?;
        }
        let source_metadata = source.symlink_metadata().map_err(raised)?;
        let concat_matches = with_pixels!(&*self.lock(), storage => match storage {
            Storage::Temp { image, .. } => image.concat_source().is_none_or(|source|
                source.matches_directory_metadata(&source_metadata)),
            Storage::Paged(_) => true,
        });
        if !concat_matches
            || self
                .image
                .use_lock
                .as_ref()
                .is_some_and(|lease| !lease.matches_directory_metadata(&source_metadata))
        {
            return Err(source_changed());
        }
        let open = |path: &Path| {
            ImageHandle::open_native(path, self.image.shareable.then_some(1)).map_err(raised)
        };
        let moved = move_attached_directory(
            virtual_concat,
            source,
            &source_metadata,
            &target,
            target_metadata.as_ref(),
            |copied| {
                if history {
                    // Write only to the private copy. A competing rename of
                    // the public destination must not redirect this history.
                    log_rename(&mut open(copied)?)?;
                }
                Ok(())
            },
        )?;
        let copied_lock = moved.use_lock();
        // Keep both root locks during reattachment, but release child
        // locks so the log-table writer can append the rename history.
        locks.retain(|path, _| path == source || path == &target);
        let reopen = || {
            moved.check_target(&target)?;
            let mut image = open(&target)?;
            moved.check_target(&target)?;
            if copied_lock.is_none() {
                log_rename(&mut image)?;
            }
            Ok::<_, PyErr>(image)
        };
        let handle = match reopen() {
            Ok(image) => image,
            Err(error) => return moved.restore(source, &target, error),
        };
        moved.finish(&target)?;
        let beam_view = BeamView::from_image(&handle);
        let expression_operands = handle.concat_expression_operands();
        let image = Arc::new(SharedImage {
            handle: Mutex::new(handle),
            use_lock: copied_lock.or_else(|| self.image.use_lock.clone()),
            miriad_directory: None,
            saved_expression_directory: None,
            expression_operands,
            shareable: self.image.shareable,
            coordinates_replaced: AtomicBool::new(
                self.image.coordinates_replaced.load(Ordering::Relaxed),
            ),
        });
        registry.remove(source);
        registry.insert(target.clone(), vec![Arc::downgrade(&image)]);
        Ok(Self {
            image,
            key: Some(target.clone()),
            native_name: Some(target),
            registered: true,
            beam_view: Mutex::new(beam_view),
            retained_inputs: Vec::new(),
            operand_paths: Vec::new(),
            live_coordinates: self.live_coordinates.clone(),
        })
    }

    fn rename_non_native(
        mut self,
        name: &str,
        expanded: PyResult<Cow<'_, str>>,
        overwrite: bool,
        history: bool,
    ) -> PyResult<Self> {
        if let Some(directory) = &self.image.saved_expression_directory {
            let mut relocated =
                directory.rename(&self, name, expanded?.as_ref(), overwrite, history)?;
            relocated.retain_expression_inputs(
                std::mem::take(&mut self.retained_inputs),
                std::mem::take(&mut self.operand_paths),
            );
            return Ok(relocated);
        }
        if let Some(directory) = &self.image.miriad_directory {
            let old_name =
                self.lock().path().map(Path::to_path_buf).ok_or_else(|| {
                    raised("Exception: The image to be renamed no longer exists.")
                })?;
            return directory.rename(&old_name, name, expanded?.as_ref(), overwrite, history);
        }
        let fits_path = {
            let image = self.lock();
            image
                .fits()
                .and_then(|_| image.path())
                .map(Path::to_path_buf)
        };
        if let Some(old_name) = fits_path {
            return super::fits_rename::rename(
                &old_name,
                name,
                expanded?.as_ref(),
                overwrite,
                history,
            );
        }
        Err(raised(
            "Exception: This image tool is not associated with a persistent disk file. It cannot be renamed.",
        ))
    }

    pub(super) fn remove(&self) -> PyResult<String> {
        {
            let image = self.lock();
            if image.image_type_name() == "ImageConcat" {
                let name = self
                    .name_path(&image)
                    .expect("persistent concatenation name");
                return Err(raised(format!(
                    "Cannot delete file {} because table is not writable",
                    name.display()
                )));
            }
        }
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
        let name = self.name_path(&self.lock()).unwrap_or(key).to_path_buf();
        let mut registry = registry();
        if conflicting_images(&registry, key, 1) || in_use_by_expression(&registry, key) {
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
        let entry = if name.canonicalize().is_ok_and(|actual| actual == *key) {
            &name
        } else {
            key
        };
        remove_native_table(entry, key, self.image.use_lock.as_deref())?;
        registry.remove(key);
        Ok(name.to_string_lossy().into_owned())
    }
}

fn move_attached_directory(
    virtual_concat: bool,
    source: &Path,
    identity: &fs::Metadata,
    target: &Path,
    previous: Option<&fs::Metadata>,
    prepare_copy: impl FnOnce(&Path) -> PyResult<()>,
) -> PyResult<NativeTableMove> {
    if virtual_concat {
        // The virtual root is a manifest directory. Its history is rebuilt
        // on attachment, and it has no native table lock file.
        move_read_only_directory(source, identity, target, previous, |_| Ok(()))
    } else {
        move_native_table(source, identity, target, previous, prepare_copy)
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
    if conflicting_images(&registry, &key, 0) || in_use_by_expression(&registry, &key) {
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

pub(super) fn in_use_by_expression(registry: &OpenImages, path: &Path) -> bool {
    registry
        .values()
        .flatten()
        .filter_map(Weak::upgrade)
        .any(|image| {
            image
                .expression_operands
                .iter()
                .any(|operand| path.starts_with(operand) || operand.starts_with(path))
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

pub(super) fn containing_tables(path: &Path) -> impl Iterator<Item = &Path> {
    path.parent()
        .into_iter()
        .flat_map(Path::ancestors)
        .filter(|ancestor| ancestor.join("table.dat").is_file())
}

fn not_writable(name: &Path) -> PyErr {
    raised(format!(
        "Cannot delete file {} because table is not writable",
        name.display()
    ))
}

pub(super) fn lock_table_ancestors(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> PyResult<()> {
    let parent = path.parent().ok_or_else(|| not_writable(path))?;
    if fs::metadata(parent)
        .map_err(raised)?
        .permissions()
        .readonly()
    {
        return Err(not_writable(path));
    }
    for ancestor in containing_tables(path) {
        lock_table_for_deletion(ancestor, leases)?;
    }
    Ok(())
}

pub(super) fn lock_table_tree(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> PyResult<()> {
    lock_tables_in_tree(path, leases, true)
}

pub(super) fn lock_table_tree_for_read_only_move(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> PyResult<()> {
    lock_tables_in_tree(path, leases, false)
}

fn lock_tables_in_tree(
    path: &Path,
    leases: &mut BTreeMap<PathBuf, TableWriteLock>,
    require_writable_directories: bool,
) -> PyResult<()> {
    lock_table_ancestors(path, leases)?;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        if require_writable_directories
            && fs::metadata(&directory)
                .map_err(raised)?
                .permissions()
                .readonly()
        {
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

impl Drop for AttachedImage {
    fn drop(&mut self) {
        // A failed open can drop the candidate while publication holds the
        // registry lock. The candidate has no registry entry to remove.
        if self.registered
            && let Some(key) = &self.key
        {
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
