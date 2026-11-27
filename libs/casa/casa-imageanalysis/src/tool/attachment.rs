// SPDX-License-Identifier: LGPL-3.0-or-later

//! Process-wide open-image registry, shared attachment handles, locking, and file lifecycle.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_core::RecordValue;
use casa_images::fits_read::FitsToolImport;
use casa_tables::publication::check_write_access;
use casa_tables::{LockType, TableError, TableInfo, TableUseLock};

use super::beam_view::BeamView;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::fits_rename::FitsDirectory;
use super::handle::{ImageHandle, Storage, with_pixels};
use super::miriad_rename::MiriadDirectory;
use super::native_rename::{
    NativeTableMove, check_path_relationship, move_native_table, move_read_only_directory,
    native_destination,
};
use super::saved_expression::SavedExpressionDirectory;
pub use super::table_locks::{
    containing_tables, lock_table_ancestors, lock_table_tree, lock_table_tree_for_read_only_move,
    remove_image_path, remove_native_table,
};
pub use crate::analysis::ensure_outfile_does_not_alias_input;

/// Callback used to query whether a filesystem path is currently open in another subsystem
/// (e.g. an open Python `table` tool handle).
pub type ExternalPathCheck<'a> = &'a dyn Fn(&Path) -> bool;

/// Default [`ExternalPathCheck`] reporting that no external handle holds any path.
pub fn no_external_open(_: &Path) -> bool {
    false
}

/// Shared backing state for an open image handle and its table/directory leases.
pub struct SharedImage {
    handle: Mutex<ImageHandle>,
    use_lock: Option<Arc<TableUseLock>>,
    miriad_directory: Option<MiriadDirectory>,
    saved_expression_directory: Option<SavedExpressionDirectory>,
    expression_operands: Vec<PathBuf>,
    shareable: bool,
    coordinates_replaced: AtomicBool,
}

impl SharedImage {
    /// Locks and returns a guard to the underlying [`ImageHandle`].
    pub fn lock(&self) -> MutexGuard<'_, ImageHandle> {
        self.handle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Registry mapping canonical table paths to weak references of currently attached handles.
pub type OpenImages = BTreeMap<PathBuf, Vec<Weak<SharedImage>>>;

static ATTACHED: Mutex<OpenImages> = Mutex::new(BTreeMap::new());

/// Acquires the process-wide open-image registry mutex.
pub fn registry() -> MutexGuard<'static, OpenImages> {
    ATTACHED.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Executes `read` under an acquired pixel read lock, releasing any requested lock afterward.
pub fn read_locked<T>(
    attachment: &AttachedImage,
    read: impl FnOnce(&ImageHandle) -> ImageToolResult<T>,
) -> ImageToolResult<T> {
    let mut handle = attachment.lock();
    attachment.require_pixel_read_lock(&mut handle)?;
    let staged = read(&handle);
    let release = attachment.release_requested_lock(&mut handle);
    let result = staged?;
    release?;
    Ok(result)
}

#[derive(Clone)]
struct LiveCoordinates {
    record: RecordValue,
    system: CoordinateSystem,
}

/// Attached open-image handle coordinating shared tables, beams, live coordinates, and retained inputs.
pub struct AttachedImage {
    image: Arc<SharedImage>,
    key: Option<PathBuf>,
    native_name: Option<PathBuf>,
    registered: bool,
    beam_view: Mutex<BeamView>,
    retained_inputs: Vec<Self>,
    operand_paths: Vec<PathBuf>,
    live_coordinates: Option<LiveCoordinates>,
    advisory_lock: bool,
    tempclose: bool,
    read_only: Option<String>,
}

impl AttachedImage {
    /// Wraps a temporary or in-memory [`ImageHandle`] without a table registry entry.
    pub fn temporary(image: ImageHandle) -> Self {
        Self::without_table(image, None, None)
    }

    /// Wraps a handle together with explicit advisory/tempclose/read-only metadata.
    pub fn from_handle_with_options(
        image: ImageHandle,
        advisory_lock: bool,
        tempclose: bool,
        read_only: Option<String>,
    ) -> Self {
        let mut attached = Self::without_table(image, None, None);
        attached.advisory_lock = advisory_lock;
        attached.tempclose = tempclose;
        attached.read_only = read_only;
        attached
    }

    /// Wraps a saved-expression [`ImageHandle`] paired with its identity-bound directory.
    pub fn with_saved_expression(image: ImageHandle, directory: SavedExpressionDirectory) -> Self {
        Self::without_table(image, None, Some(directory))
    }

    fn without_table(
        image: ImageHandle,
        miriad_directory: Option<MiriadDirectory>,
        saved_expression_directory: Option<SavedExpressionDirectory>,
    ) -> Self {
        let beam_view = BeamView::from_image(&image);
        let expression_operands = image.concat_expression_operands();
        let read_only = image
            .read_only_file_type()
            .map(|kind| format!("{kind} images are read-only."));
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
            advisory_lock: false,
            tempclose: false,
            read_only,
        }
    }

    /// Opens an image from `path` on disk (FITS, MIRIAD, saved expression, virtual concat, or paged).
    pub fn open(path: &Path) -> ImageToolResult<Self> {
        Self::open_resolved(path, path, path)
    }

    /// Opens a MIRIAD image directory at `path` with directory-identity validation.
    pub fn open_miriad(path: &Path) -> ImageToolResult<Self> {
        let directory = MiriadDirectory::open(path)?;
        let image = ImageHandle::open_miriad(path)?;
        directory.check(path)?;
        Ok(Self::without_table(image, Some(directory), None))
    }

    /// Opens an image at resolved path `path` probing `probe` and displaying `name`.
    pub fn open_resolved(path: &Path, probe: &Path, name: &Path) -> ImageToolResult<Self> {
        Self::open_resolved_with_cache(path, probe, name, true)
    }

    /// Opens an image with explicit component-list pixel caching control.
    pub fn open_resolved_with_cache(
        path: &Path,
        probe: &Path,
        name: &Path,
        component_cache: bool,
    ) -> ImageToolResult<Self> {
        Self::open_with_read_lock(path, probe, name, true, component_cache)
    }

    /// Opens an image without holding a persistent read lock afterward.
    pub fn open_for_ordered_read(path: &Path) -> ImageToolResult<Self> {
        Self::open_with_read_lock(path, path, path, false, true)
    }

    /// Returns the display/user-supplied path of the attached image, if persistent.
    pub fn name_path<'a>(&'a self, image: &'a ImageHandle) -> Option<&'a Path> {
        self.native_name.as_deref().or_else(|| image.path())
    }

    /// Returns the canonical table key path for native/virtual-concat images.
    pub fn native_path(&self) -> Option<&Path> {
        self.key.as_deref()
    }

    /// Returns the bound saved-expression directory if this attachment is an `.imgexpr`.
    pub fn saved_expression_directory(&self) -> Option<&SavedExpressionDirectory> {
        self.image.saved_expression_directory.as_ref()
    }

    fn open_with_read_lock(
        path: &Path,
        probe: &Path,
        name: &Path,
        keep_read_lock: bool,
        component_cache: bool,
    ) -> ImageToolResult<Self> {
        if probe.is_file() {
            let source = FitsToolImport::open_attachment_named(path, probe, name)?;
            return Ok(ImageHandle::open_fits(path, source).map(Self::temporary)?);
        }
        let unrecognized = || {
            ImageToolError::new(format!(
                "Exception: Unable to open image {}.",
                name.display()
            ))
        };
        if path != probe && (!probe.is_dir() || !path.is_dir()) {
            return Err(unrecognized());
        }
        if casa_images::is_image_concat(probe) {
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
        if !paged && !crate::ComponentListImage::recognizes_native(&key) {
            if ImageHandle::recognizes_miriad(probe, &key) {
                return Self::open_miriad(path);
            }
            return Err(unrecognized());
        }
        let name = std::path::absolute(path)?;
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

    fn read_current_beams(&self) -> ImageToolResult<()> {
        let mut handle = self.lock();
        self.require_lock(&mut handle, LockType::Read)?;
        *self.beam_view() = BeamView::from_image(&handle);
        Ok(())
    }

    /// Opens and registers a newly created native image at `path` under `registry`.
    pub fn open_created(registry: &mut OpenImages, path: &Path) -> ImageToolResult<Self> {
        let key = key(path)?;
        Self::open_created_as(registry, path, &key)
    }

    /// Opens and registers a newly created native image referenced via `name` at canonical `key`.
    pub fn open_created_as(
        registry: &mut OpenImages,
        name: &Path,
        key: &Path,
    ) -> ImageToolResult<Self> {
        let mut opened = Self::open_native(name, key, Some(1), TableUseLock::acquire, true)?;
        if !opened.lock().lock_storage(LockType::Write, 1)? {
            return Err(ImageToolError::new(
                "Could not acquire the new image write lock",
            ));
        }
        opened.register(registry);
        Ok(opened)
    }

    /// Registers a freshly constructed virtual concatenation handle under `registry`.
    pub fn with_virtual_concat(
        image: ImageHandle,
        name: &Path,
        registry: &mut OpenImages,
    ) -> ImageToolResult<Self> {
        let mut attached = Self::temporary(image);
        attached.key = Some(key(name)?);
        attached.native_name = Some(std::path::absolute(name)?);
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
                advisory_lock: false,
                tempclose: false,
                read_only: None,
            })
    }

    /// Opens a fresh compatibility view for `casars.table` / V1 Data callers.
    pub fn open_data(
        registry: &mut OpenImages,
        path: &Path,
        writable: bool,
    ) -> ImageToolResult<Self> {
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
    ) -> ImageToolResult<Self> {
        let use_lock = key
            .join("table.dat")
            .is_file()
            .then(|| acquire(key).map(Arc::new))
            .transpose()?;
        let handle = ImageHandle::open_native_with_cache(key, lock_attempts, component_cache)?;
        let shareable = lock_attempts.is_some() && handle.image_type_name() != "ComponentListImage";
        let beam_view = BeamView::from_image(&handle);
        let expression_operands = handle.concat_expression_operands();
        let read_only = handle
            .read_only_file_type()
            .map(|kind| format!("{kind} images are read-only."));
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
            native_name: Some(std::path::absolute(path)?),
            registered: false,
            beam_view: Mutex::new(beam_view),
            retained_inputs: Vec::new(),
            operand_paths: Vec::new(),
            live_coordinates: None,
            advisory_lock: false,
            tempclose: false,
            read_only,
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

    /// Locks and returns a guard to the underlying [`ImageHandle`].
    pub fn lock(&self) -> MutexGuard<'_, ImageHandle> {
        self.image.lock()
    }

    /// Returns a cloned [`Arc<SharedImage>`] handle for cross-closure lazy sharing.
    pub fn share_image(&self) -> Arc<SharedImage> {
        Arc::clone(&self.image)
    }

    /// Captures sampled/tabular coordinate records for `live_coordinates` caching.
    pub fn capture_live_coordinates(
        image: &ImageHandle,
    ) -> ImageToolResult<Option<(RecordValue, CoordinateSystem)>> {
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
        let system = CoordinateSystem::from_live_record(&record)?;
        Ok(Some((record, system)))
    }

    /// Caches a live sampled/tabular coordinate system for this attachment.
    pub fn keep_live_coordinates(&mut self, record: RecordValue, system: CoordinateSystem) {
        self.live_coordinates = Some(LiveCoordinates { record, system });
    }

    /// Invalidates cached live coordinates across all shared views of this image.
    pub fn coordinates_changed(&self) {
        self.image
            .coordinates_replaced
            .store(true, Ordering::Relaxed);
    }

    /// Returns the effective coordinate system (live if unchanged, else stored) and record.
    pub fn coordinate_view<'a>(
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

    /// Locks and returns a guard to the attachment's cached [`BeamView`].
    pub fn beam_view(&self) -> MutexGuard<'_, BeamView> {
        self.beam_view
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Opens non-file expression operands through the registry.
    pub fn open_expression_inputs<'a>(
        paths: impl Iterator<Item = &'a Path>,
    ) -> ImageToolResult<Vec<Self>> {
        paths
            .filter(|path| !path.is_file())
            .map(Self::open)
            .collect()
    }

    /// Opens a table/MIRIAD operand directly without consulting or inserting into [`OpenImages`].
    pub fn open_unregistered_expression_operand(path: &Path) -> ImageToolResult<Self> {
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

    /// Retains open operand handles and path aliases while a lazy expression attachment lives.
    pub fn retain_expression_inputs(&mut self, inputs: Vec<Self>, paths: Vec<PathBuf>) {
        self.retained_inputs = inputs;
        self.operand_paths = paths;
    }

    /// Returns retained expression operand attachments.
    pub fn expression_inputs(&self) -> &[Self] {
        &self.retained_inputs
    }

    /// Returns paths of any expression or concatenation operands tracked by this image.
    pub fn operand_paths(&self) -> &[PathBuf] {
        &self.operand_paths
    }

    /// Returns whether this attachment is read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only.is_some()
    }

    /// Returns whether this attachment uses advisory locking.
    pub fn has_advisory_lock(&self) -> bool {
        self.advisory_lock
    }

    /// Returns whether this attachment temporarily closes storage after unlock.
    pub fn tempclose(&self) -> bool {
        self.tempclose
    }

    /// Returns whether this attachment is backed by a persistent disk image.
    pub fn is_persistent(&self) -> ImageToolResult<bool> {
        let image = self.lock();
        if image.is_saved_expression() || image.image_type_name() == "ImageConcat" {
            image.check_single_precision("ispersistent")?;
        }
        Ok(self.key.is_some() || image.path().is_some())
    }

    /// Checks whether the backing file on disk is still writable for native images.
    pub fn check_native_write(&self, image: &ImageHandle) -> ImageToolResult<()> {
        if let Some(reason) = &self.read_only {
            return Err(ImageToolError::new(reason.clone()));
        }
        if let Some(path) = image.path()
            && ImageHandle::has_native_control(path)
        {
            check_write_access(&path.join("table.dat"))?;
        }
        Ok(())
    }

    /// Verifies that the table directory lease still matches the on-disk directory.
    pub fn check_directory(&self) -> ImageToolResult<()> {
        if let (Some(key), Some(lease)) = (&self.key, &self.image.use_lock)
            && !lease.matches_directory(key)
        {
            return Err(ImageToolError::new(format!(
                "The image at {} has changed",
                key.display()
            )));
        }
        Ok(())
    }

    /// Verifies both directory lease and handle path match this attachment's bound path.
    pub fn check_bound_name(&self, handle: &ImageHandle) -> ImageToolResult<()> {
        self.check_directory()?;
        with_pixels!(handle, storage => {
            if let Storage::Temp { image, .. } = storage
                && let Some(source) = image.concat_source()
            {
                source.check_directory()?;
            }
        });
        if let (Some(key), Some(path)) = (&self.key, handle.path())
            && path != key
            && !path.canonicalize().is_ok_and(|actual| &actual == key)
        {
            return Err(ImageToolError::new(format!(
                "The image at {} has changed",
                path.display()
            )));
        }
        Ok(())
    }

    /// Renames the persistent backing file or directory (`ia.rename`), updating registry leases.
    pub fn rename_with(
        mut self,
        name: &str,
        expand: impl FnOnce() -> ImageToolResult<String>,
        overwrite: bool,
        history: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Self> {
        self.rename_impl(name, expand, overwrite, history, is_external_open)?;
        Ok(self)
    }

    fn rename_impl(
        &mut self,
        name: &str,
        expand: impl FnOnce() -> ImageToolResult<String>,
        overwrite: bool,
        history: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<(PathBuf, Vec<ToolLogLine>)> {
        if name.is_empty() {
            return Err(ImageToolError::new(
                "Exception: new file name must be specified.",
            ));
        }
        let Some(source) = self.key.clone() else {
            return self.rename_non_native(name, expand, overwrite, history, is_external_open);
        };
        let source_changed = || {
            ImageToolError::new(format!(
                "Cannot rename image {} because the image at that path has changed",
                source.display()
            ))
        };
        let (old_name, virtual_concat) = {
            let image = self.lock();
            self.check_bound_name(&image)
                .map_err(|_| source_changed())?;
            (
                self.name_path(&image).unwrap_or(&source).to_path_buf(),
                image.image_type_name() == "ImageConcat",
            )
        };
        let expanded = expand()?;
        let target = native_destination(&expanded, name)?;
        let mut registry = registry();
        let target_metadata = match target.symlink_metadata() {
            Ok(metadata) if overwrite => Some(metadata),
            Ok(_) => {
                return Err(ImageToolError::new(format!(
                    "Exception: File {name} exists, and the user does not want to remove it.."
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let log_rename = |image: &mut ImageHandle| -> ImageToolResult<()> {
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
            Ok(())
        };
        if std::ffi::OsStr::new(name) == old_name.as_os_str() {
            return Err(ImageToolError::new(
                "Exception: Specified output name is the same as the current image name.",
            ));
        }
        if source == target {
            if self
                .image
                .use_lock
                .as_ref()
                .is_some_and(|lease| !lease.matches_directory(&source))
            {
                return Err(source_changed());
            }
            log_rename(&mut self.lock())?;
            self.native_name = Some(target.clone());
            self.retained_inputs.clear();
            self.operand_paths.clear();
            return Ok((
                target.clone(),
                vec![ToolLogLine::info(format!(
                    "Renamed image {} to {}",
                    old_name.display(),
                    target.display()
                ))],
            ));
        }
        check_path_relationship(&source, &target, name)?;
        if virtual_concat {
            super::concatenate::virtual_image::check_relocation(&source, &target)?;
        }
        let replaces_link = target_metadata
            .as_ref()
            .is_some_and(|entry| entry.is_symlink());
        if replaces_link && check_write_access(&target).is_err() {
            return Err(ImageToolError::new(format!(
                "RegularFile::copy/move: target file {} already exists and is not writable",
                target.display()
            )));
        }
        for (path, expected) in [(source.as_path(), 1), (target.as_path(), 0)] {
            let in_table_use = if replaces_link && path == target {
                containing_tables(path).any(is_external_open)
            } else {
                is_external_open(path)
            };
            if conflicting_images(&registry, path, expected)
                || in_table_use
                || (path == target && !replaces_link && in_use_by_expression(&registry, path))
            {
                return Err(ImageToolError::new(format!(
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
        lock_table_tree(&source, &mut locks)?;
        if target_metadata.as_ref().is_some_and(|entry| entry.is_dir()) {
            lock_table_tree(&target, &mut locks)?;
        } else if replaces_link {
            lock_table_ancestors(&target, &mut locks)?;
        }
        let source_metadata = source.symlink_metadata()?;
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
        let open = |path: &Path| -> ImageToolResult<ImageHandle> {
            Ok(ImageHandle::open_native(
                path,
                self.image.shareable.then_some(1),
            )?)
        };
        let moved = move_attached_directory(
            virtual_concat,
            &source,
            &source_metadata,
            &target,
            target_metadata.as_ref(),
            |copied| {
                if history {
                    log_rename(&mut open(copied)?)?;
                }
                Ok(())
            },
        )?;
        let copied_lock = moved.use_lock();
        locks.retain(|path, _| path == &source || path == &target);
        let reopen = || -> ImageToolResult<ImageHandle> {
            moved.check_target(&target)?;
            let mut image = open(&target)?;
            moved.check_target(&target)?;
            if copied_lock.is_none() {
                log_rename(&mut image)?;
            }
            Ok(image)
        };
        let handle = match reopen() {
            Ok(image) => image,
            Err(error) => return moved.restore(&source, &target, error),
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
        registry.remove(&source);
        registry.insert(target.clone(), vec![Arc::downgrade(&image)]);
        self.image = image;
        self.key = Some(target.clone());
        self.native_name = Some(target.clone());
        self.registered = true;
        *self.beam_view() = beam_view;
        self.retained_inputs.clear();
        self.operand_paths.clear();
        Ok((
            target.clone(),
            vec![ToolLogLine::info(format!(
                "Renamed image {} to {}",
                old_name.display(),
                target.display()
            ))],
        ))
    }

    fn rename_non_native(
        &mut self,
        name: &str,
        expand: impl FnOnce() -> ImageToolResult<String>,
        overwrite: bool,
        history: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<(PathBuf, Vec<ToolLogLine>)> {
        let old_path = self.lock().path().map(Path::to_path_buf);
        if let Some(directory) = &self.image.saved_expression_directory {
            let expanded = expand()?;
            let mut relocated =
                directory.rename(self, name, &expanded, overwrite, history, is_external_open)?;
            relocated.retain_expression_inputs(
                std::mem::take(&mut self.retained_inputs),
                std::mem::take(&mut self.operand_paths),
            );
            let target = relocated
                .lock()
                .path()
                .map_or_else(|| PathBuf::from(&expanded), Path::to_path_buf);
            *self = relocated;
            let from = old_path.map_or_else(|| name.to_owned(), |p| p.display().to_string());
            return Ok((
                target.clone(),
                vec![ToolLogLine::info(format!(
                    "Renamed image {from} to {}",
                    target.display()
                ))],
            ));
        }
        if let Some(directory) = &self.image.miriad_directory {
            let old_name = old_path.ok_or_else(|| {
                ImageToolError::new("Exception: The image to be renamed no longer exists.")
            })?;
            let expanded = expand()?;
            let relocated = directory.rename(
                &old_name,
                name,
                &expanded,
                overwrite,
                history,
                is_external_open,
            )?;
            let target = relocated
                .lock()
                .path()
                .map_or_else(|| PathBuf::from(&expanded), Path::to_path_buf);
            *self = relocated;
            return Ok((
                target.clone(),
                vec![ToolLogLine::info(format!(
                    "Renamed image {} to {}",
                    old_name.display(),
                    target.display()
                ))],
            ));
        }
        let fits_path = {
            let image = self.lock();
            image
                .fits()
                .and_then(|_| image.path())
                .map(Path::to_path_buf)
        };
        if let Some(old_name) = fits_path {
            let expanded = expand()?;
            let relocated = FitsDirectory::rename(
                &old_name,
                name,
                &expanded,
                overwrite,
                history,
                is_external_open,
            )?;
            let target = relocated
                .lock()
                .path()
                .map_or_else(|| PathBuf::from(&expanded), Path::to_path_buf);
            *self = relocated;
            return Ok((
                target.clone(),
                vec![ToolLogLine::info(format!(
                    "Renamed image {} to {}",
                    old_name.display(),
                    target.display()
                ))],
            ));
        }
        Err(ImageToolError::new(
            "Exception: This image tool is not associated with a persistent disk file. It cannot be renamed.",
        ))
    }

    /// Re-attaching convenience wrapper for in-place `ia.rename(newname, overwrite)`.
    pub fn rename(
        &mut self,
        newname: &str,
        overwrite: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<(PathBuf, Vec<ToolLogLine>)> {
        self.rename_impl(
            newname,
            || Ok(newname.to_owned()),
            overwrite,
            false,
            is_external_open,
        )
    }

    /// Removes the persistent backing image from disk (`ia.remove` / `ia.done(remove=True)`),
    /// returning the removed path string.
    pub fn remove_registered(
        &self,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<String> {
        {
            let image = self.lock();
            if image.image_type_name() == "ImageConcat" {
                let name = self
                    .name_path(&image)
                    .expect("persistent concatenation name");
                return Err(ImageToolError::new(format!(
                    "Cannot delete file {} because table is not writable",
                    name.display()
                )));
            }
        }
        let Some(key) = &self.key else {
            if let Some(path) = self.lock().path() {
                return Err(ImageToolError::new(format!(
                    "Cannot delete file {} because table is not writable",
                    path.display()
                )));
            }
            return Err(ImageToolError::new(
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
                ImageToolError::new(format!(
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
        remove_native_table(entry, key, self.image.use_lock.as_deref(), is_external_open)?;
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
    prepare_copy: impl FnOnce(&Path) -> ImageToolResult<()>,
) -> ImageToolResult<NativeTableMove> {
    if virtual_concat {
        move_read_only_directory(source, identity, target, previous, |_| Ok(()))
    } else {
        move_native_table(source, identity, target, previous, prepare_copy)
    }
}

/// Deletes an unopened casacore table by `path` (`ia.removefile`).
pub fn remove_file(path: &Path, is_external_open: ExternalPathCheck<'_>) -> ImageToolResult<bool> {
    if !path.is_dir() {
        return Ok(false);
    }
    let name = std::path::absolute(path)?;
    let key = key(&name)?;
    let registry = registry();
    if conflicting_images(&registry, &key, 0) || in_use_by_expression(&registry, &key) {
        return Err(in_use(&key, "this"));
    }
    remove_native_table(&name, &key, None, is_external_open)?;
    Ok(true)
}

/// Returns whether any open image handles in `registry` overlap `path` beyond `expected_at_path`.
pub fn conflicting_images(registry: &OpenImages, path: &Path, expected_at_path: usize) -> bool {
    registry.iter().any(|(open_path, images)| {
        if open_path == path {
            images.iter().map(Weak::strong_count).sum::<usize>() > expected_at_path
        } else {
            (open_path.starts_with(path) || path.starts_with(open_path))
                && images.iter().any(|image| image.strong_count() > 0)
        }
    })
}

/// Returns whether any open expression or virtual concatenation references `path`.
pub fn in_use_by_expression(registry: &OpenImages, path: &Path) -> bool {
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

fn in_use(path: &Path, process: &str) -> ImageToolError {
    ImageToolError::new(format!(
        "Cannot delete file {} because table is still open in {process} process",
        path.display()
    ))
}

impl Drop for AttachedImage {
    fn drop(&mut self) {
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

/// Resolves aliases to a canonical path key for an existing image directory.
pub fn key(path: &Path) -> ImageToolResult<PathBuf> {
    Ok(path.canonicalize()?)
}
