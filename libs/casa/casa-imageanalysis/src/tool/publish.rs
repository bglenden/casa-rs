// SPDX-License-Identifier: LGPL-3.0-or-later

//! Staged atomic image directory publication, multi-output transactions, and naming checks.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_images::ImageError;
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{TableWriteLock, lexical_absolute};

use super::attachment::{
    self, AttachedImage, ExternalPathCheck, OpenImages, conflicting_images, in_use_by_expression,
};
use super::concatenate::owned_members::{MemberMoves, OwnedOperands};
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, same_file_identity};
use super::native_rename::publish_staged_directory;
use super::table_locks::lock_table_tree;

/// Validates `outfile` against `overwrite`, open tables, and mask operands, then stages a build.
///
/// When `outfile` is empty, `build(None)` produces an in-memory/temporary [`ImageHandle`].
/// When `outfile` is non-empty, `build(Some(stage_path))` writes a native image into a
/// temporary sibling directory, and `publish_image` atomically installs it onto `outfile`
/// and reopens the handle bound to the final path.
pub fn publish_image(
    outfile: &str,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    build: impl FnOnce(Option<&Path>) -> ImageToolResult<ImageHandle>,
) -> ImageToolResult<ImageHandle> {
    publish_staged_pair_core(
        StagedImage::allocate(outfile)?,
        None,
        overwrite,
        is_external_open,
        |target, _| Ok((build(target)?, None)),
        |installed, _| Ok(ImageHandle::open(&installed.stage.target)?),
        |handle| handle,
    )
}

/// Validates and publishes a directory-backed product (`.imgconcat`, `.imgexpr`, or component table)
/// built by `build(stage_path)` inside a staging directory.
pub fn publish_directory(
    outfile: &Path,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    build: impl FnOnce(&Path) -> ImageToolResult<()>,
) -> ImageToolResult<()> {
    validate_output_target(outfile, overwrite, is_external_open)?;
    let parent = output_parent(outfile);
    if !parent.exists() {
        std::fs::create_dir_all(parent)?;
    }
    let stage = tempfile::Builder::new()
        .prefix(".casars-dir-stage-")
        .tempdir_in(parent)?;
    build(stage.path())?;
    publish_staged_directory(stage, outfile)
}

/// Checks whether `outfile` may be created or overwritten safely.
pub fn validate_output_target(
    destination: &Path,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<()> {
    if destination.as_os_str().is_empty() {
        return Ok(());
    }
    if !overwrite && destination.symlink_metadata().is_ok() {
        return Err(ImageToolError::new(format!(
            "File {} exists, and the user does not want to remove it.",
            destination.display()
        )));
    }
    if is_external_open(destination) {
        return Err(ImageToolError::new(format!(
            "Cannot overwrite {}: file is currently open.",
            destination.display()
        )));
    }
    if overwrite
        && let Ok(meta) = destination.symlink_metadata()
        && !meta.is_dir()
    {
        return Err(ImageToolError::new(format!(
            "Cannot overwrite non-directory file {} with a CASA image.",
            destination.display()
        )));
    }
    Ok(())
}

/// Staged directory reservation for secondary output images (`fitcomponents`, `fft`, `moments`).
pub struct StagedImageOutput {
    destination: PathBuf,
    stage_dir: tempfile::TempDir,
    stage_path: PathBuf,
}

impl StagedImageOutput {
    /// Reserves a staging slot adjacent to `destination`, validating overwrite/external open state.
    pub fn prepare(
        destination: &Path,
        overwrite: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Self> {
        validate_output_target(destination, overwrite, is_external_open)?;
        let parent = output_parent(destination);
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
        let stage_dir = tempfile::Builder::new()
            .prefix(".casars-stage-")
            .tempdir_in(parent)?;
        let stage_path = stage_dir.path().join("product");
        Ok(Self {
            destination: destination.to_path_buf(),
            stage_dir,
            stage_path,
        })
    }

    /// Returns the temporary path into which the product image should be written.
    pub fn path(&self) -> &Path {
        &self.stage_path
    }

    /// Atomically publishes the staged image onto its final destination path.
    pub fn commit(self) -> ImageToolResult<PathBuf> {
        let parent = output_parent(&self.destination);
        let inner_stage = self.stage_dir.keep();
        let final_stage = tempfile::TempDir::new_in(parent)?;
        let final_stage_path = final_stage.path().to_path_buf();
        std::fs::remove_dir(&final_stage_path)?;
        std::fs::rename(&self.stage_path, &final_stage_path)?;
        let _ = std::fs::remove_dir_all(&inner_stage);
        publish_staged_directory(final_stage, &self.destination)?;
        Ok(self.destination)
    }
}

/// Converts an `ImageError` encountered during staged I/O into [`ImageToolError`].
pub fn staged_io_error(error: ImageError) -> ImageToolError {
    ImageToolError::from(error)
}

fn output_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn output_exists(name: &str) -> ImageToolError {
    ImageToolError::new(format!(
        "Exception: File {name} exists, and the user does not want to remove it.."
    ))
}

fn output_is_not_a_table(path: &Path) -> ImageToolError {
    ImageToolError::new(format!(
        "Table {} already exists (and is not a true table directory)",
        path.display()
    ))
}

/// Transaction stage for creating or overwriting one persistent image directory atomically.
pub struct StagedImage {
    /// Original user-requested output string.
    pub requested: String,
    /// Resolved target directory path.
    pub target: PathBuf,
    /// Optional symlink alias path when overwriting through a symlink.
    pub alias: Option<PathBuf>,
    /// Temporary parent directory holding [`Self::product`].
    pub directory: tempfile::TempDir,
    /// Path inside `directory` where the product image is built before swap/rename.
    pub product: PathBuf,
}

impl StagedImage {
    /// Allocates a staging directory when `name` is non-empty.
    pub fn allocate(name: &str) -> ImageToolResult<Option<Self>> {
        if name.is_empty() {
            return Ok(None);
        }
        Self::allocate_at(name, Path::new(name)).map(Some)
    }

    /// Allocates a staging directory with full CASA creation/alias/overwrite validation.
    pub fn allocate_creation(
        name: &str,
        expanded: &str,
        overwrite: bool,
    ) -> ImageToolResult<Option<Self>> {
        if name.is_empty() {
            return Ok(None);
        }
        let expanded = std::path::absolute(expanded)?;
        if expanded.file_name().is_none() {
            return Err(ImageToolError::new("Invalid output image name"));
        }
        if !overwrite && expanded.symlink_metadata().is_ok() {
            return Err(output_exists(name));
        }
        let target = lexical_absolute(&expanded);
        if !target.parent().is_some_and(Path::is_dir) {
            return Err(ImageToolError::new(format!(
                "Directory: {} does not exist and cannot be created",
                target.display()
            )));
        }
        if overwrite
            && target
                .symlink_metadata()
                .is_ok_and(|entry| entry.is_symlink())
        {
            let referent = target.canonicalize().map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    output_is_not_a_table(&target)
                } else {
                    ImageToolError::from(error)
                }
            })?;
            let mut stage = Self::allocate_at(name, &referent)?;
            stage.alias = Some(target);
            return Ok(Some(stage));
        }
        Self::allocate_at(name, &target).map(Some)
    }

    /// Allocates a staging directory adjacent to explicit `target`.
    pub fn allocate_at(name: &str, target: &Path) -> ImageToolResult<Self> {
        let target = std::path::absolute(target)?;
        let parent = target
            .parent()
            .ok_or_else(|| ImageToolError::new("Invalid output image path"))?;
        let directory = tempfile::tempdir_in(parent)?;
        let product = directory.path().join("image");
        Ok(Self {
            requested: name.to_owned(),
            target,
            alias: None,
            directory,
            product,
        })
    }

    /// Returns all output paths (resolved target and optional symlink alias).
    pub fn output_paths(&self) -> impl Iterator<Item = &Path> {
        std::iter::once(self.target.as_path()).chain(self.alias.as_deref())
    }

    fn check_alias(&self) -> ImageToolResult<()> {
        if let Some(alias) = &self.alias
            && !alias.canonicalize().is_ok_and(|path| path == self.target)
        {
            return Err(ImageToolError::new(format!(
                "The output alias {} changed during image creation",
                alias.display()
            )));
        }
        Ok(())
    }

    fn resolved_key(&self) -> ImageToolResult<PathBuf> {
        if self.target.symlink_metadata().is_ok() {
            attachment::key(&self.target)
        } else {
            entry_key(&self.target)
        }
    }

    fn check(
        &self,
        overwrite: bool,
        registry: &OpenImages,
        locks: &mut BTreeMap<PathBuf, TableWriteLock>,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<bool> {
        self.check_alias()?;
        let path = &self.target;
        match path.symlink_metadata() {
            Ok(_) if !overwrite => Err(output_exists(&self.requested)),
            Ok(_) => {
                let key = attachment::key(path)?;
                if conflicting_images(registry, &key, 0)
                    || in_use_by_expression(registry, &key)
                    || is_external_open(&key)
                    || key
                        .ancestors()
                        .filter(|parent| parent.join("table.dat").is_file())
                        .any(is_external_open)
                {
                    return Err(ImageToolError::new(format!(
                        "Invalid Table operation: SetupNewTable {} is already opened (is in the table cache)",
                        path.display()
                    )));
                }
                let empty = path
                    .read_dir()
                    .is_ok_and(|mut entries| entries.next().is_none());
                if !empty
                    && !(casa_images::is_image_concat(&self.product)
                        && casa_images::ConcatManifest::open(path).is_ok())
                    && casa_tables::Table::open_metadata_only(casa_tables::TableOptions::new(path))
                        .is_err()
                {
                    return Err(output_is_not_a_table(path));
                }
                lock_table_tree(&key, locks)?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn install(self, existed: bool) -> ImageToolResult<InstalledImage> {
        let publication = self.publish(existed)?;
        Ok(InstalledImage {
            stage: self,
            publication,
        })
    }

    fn publish(&self, existed: bool) -> ImageToolResult<ImagePublication> {
        self.check_alias()?;
        let virtual_concat = casa_images::is_image_concat(&self.product);
        #[cfg(unix)]
        let directory = std::fs::File::open(&self.product)?;
        if existed {
            exchange_directories(&self.product, &self.target)?;
        } else {
            rename_without_replacing(&self.product, &self.target)?;
        }
        Ok(ImagePublication {
            existed,
            virtual_concat,
            #[cfg(unix)]
            directory,
        })
    }
}

fn entry_key(path: &Path) -> ImageToolResult<PathBuf> {
    let parent = path.parent().and_then(|parent| parent.canonicalize().ok());
    parent
        .zip(path.file_name())
        .map(|(parent, name)| parent.join(name))
        .ok_or_else(|| ImageToolError::new("Invalid output image path"))
}

fn check_distinct(
    primary: Option<&StagedImage>,
    companion: Option<&StagedImage>,
) -> ImageToolResult<()> {
    let (Some(first), Some(second)) = (primary, companion) else {
        return Ok(());
    };
    let check = |container: &StagedImage, child: &StagedImage| -> ImageToolResult<bool> {
        let roots = [entry_key(&container.target)?, container.resolved_key()?];
        let child_entry = entry_key(&child.target)?;
        let child_resolved = child.resolved_key()?;
        Ok(roots.iter().any(|root| {
            child_entry.starts_with(root)
                || child_resolved.starts_with(root)
                || child.target.ancestors().skip(1).any(|ancestor| {
                    ancestor.canonicalize().is_ok_and(|path| path == *root)
                        || entry_key(ancestor).is_ok_and(|path| path == *root)
                })
        }))
    };
    if check(first, second)? || check(second, first)? {
        return Err(ImageToolError::new(
            "Output images must have separate destinations",
        ));
    }
    Ok(())
}

struct InstalledImage {
    stage: StagedImage,
    publication: ImagePublication,
}

struct ImagePublication {
    existed: bool,
    virtual_concat: bool,
    #[cfg(unix)]
    directory: std::fs::File,
}

impl InstalledImage {
    fn open(&self, registry: &mut OpenImages) -> ImageToolResult<AttachedImage> {
        self.publication.open(&self.stage, registry)
    }

    fn restore(self, original: ImageToolError) -> ImageToolError {
        match self.publication.restore(&self.stage) {
            Ok(()) => original,
            Err(error) => report_publication_recovery(self.stage, original, error),
        }
    }
}

impl ImagePublication {
    fn open(
        &self,
        stage: &StagedImage,
        registry: &mut OpenImages,
    ) -> ImageToolResult<AttachedImage> {
        if self.virtual_concat {
            stage.check_alias()?;
            if !self.still_at_destination(&stage.target) {
                return Err(changed_virtual_output());
            }
            let name = stage.alias.as_deref().unwrap_or(&stage.target);
            let image = super::concatenate::virtual_image::open_handle(&stage.target)?;
            if !self.still_at_destination(&stage.target) {
                return Err(changed_virtual_output());
            }
            return AttachedImage::with_virtual_concat(image, name, registry);
        }
        let Some(alias) = &stage.alias else {
            return AttachedImage::open_created(registry, &stage.target);
        };
        stage.check_alias()?;
        let image = AttachedImage::open_created_as(registry, alias, &stage.target)?;
        stage.check_alias()?;
        if !self.still_at_destination(&stage.target) {
            return Err(ImageToolError::new(format!(
                "The published image at {} changed during creation",
                stage.target.display()
            )));
        }
        Ok(image)
    }

    fn still_at_destination(&self, target: &Path) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            matches!(
                (self.directory.metadata(), target.symlink_metadata()),
                (Ok(opened), Ok(current)) if opened.dev() == current.dev() && opened.ino() == current.ino(),
            )
        }
        #[cfg(not(unix))]
        {
            target.is_dir()
        }
    }

    fn restore(&self, stage: &StagedImage) -> std::io::Result<()> {
        let staged = &stage.product;
        let target = &stage.target;
        if !self.still_at_destination(target) {
            Err(std::io::Error::other(
                "the destination changed after publication",
            ))
        } else if self.existed {
            exchange_directories(staged, target)
        } else {
            rename_without_replacing(target, staged)
        }
    }
}

fn changed_virtual_output() -> ImageToolError {
    ImageToolError::new("The published virtual concatenation changed during creation")
}

fn report_publication_recovery(
    stage: StagedImage,
    original: ImageToolError,
    error: std::io::Error,
) -> ImageToolError {
    let recovery = stage.directory.keep();
    ImageToolError::new(format!(
        "{original}. Image publication could not be rolled back. Inspect {} ({error})",
        recovery.display()
    ))
}

/// Builds and publishes a single image, returning a registered [`AttachedImage`].
pub fn publish_attached_image(
    outfile: &str,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    build: impl FnOnce(Option<&Path>) -> ImageToolResult<ImageHandle>,
) -> ImageToolResult<AttachedImage> {
    publish_image_pair(outfile, "", overwrite, is_external_open, |target, _| {
        Ok((build(target)?, None))
    })
}

/// Stages and publishes a `nomovevirtual` concatenation image as an [`AttachedImage`].
pub fn publish_virtual_concat(
    outfile: &str,
    expanded: &str,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    build: impl FnOnce(&Path, &Path) -> ImageToolResult<ImageHandle>,
) -> ImageToolResult<AttachedImage> {
    let stage = StagedImage::allocate_creation(outfile, expanded, overwrite)?
        .ok_or_else(|| ImageToolError::new("Virtual concatenation requires an output image"))?;
    let final_path = stage.target.clone();
    publish_staged_pair(Some(stage), None, overwrite, is_external_open, |path, _| {
        Ok((
            build(&final_path, path.expect("virtual output stage"))?,
            None,
        ))
    })
}

/// Stages and publishes an owned (`copyvirtual` / `movevirtual`) concatenation with rollback.
pub fn publish_owned_virtual_concat(
    outfile: &str,
    expanded: &str,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    prepare: impl FnOnce(&Path, &Path) -> ImageToolResult<OwnedOperands>,
) -> ImageToolResult<(AttachedImage, Vec<String>)> {
    let stage = StagedImage::allocate_creation(outfile, expanded, overwrite)?
        .ok_or_else(|| ImageToolError::new("Virtual concatenation requires an output image"))?;
    let members = prepare(&stage.target, &stage.product)?;
    install_owned_virtual_concat(
        stage,
        overwrite,
        is_external_open,
        members,
        ImagePublication::open,
    )
}

fn install_owned_virtual_concat(
    mut stage: StagedImage,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    members: OwnedOperands,
    open: impl FnOnce(
        &ImagePublication,
        &StagedImage,
        &mut OpenImages,
    ) -> ImageToolResult<AttachedImage>,
) -> ImageToolResult<(AttachedImage, Vec<String>)> {
    let mut registry = attachment::registry();
    let mut locks = BTreeMap::new();
    let existed = stage.check(overwrite, &registry, &mut locks, is_external_open)?;
    let previous = existed
        .then(|| stage.target.symlink_metadata())
        .transpose()?;
    stage.directory.disable_cleanup(true);
    let mut moves = MemberMoves::default();
    let mut recoverable_stage = true;
    let result = (|| {
        members.stage(
            &stage.target,
            &stage.product,
            &registry,
            &mut moves,
            is_external_open,
        )?;
        let publication = stage.publish(existed)?;
        let opened = if previous.as_ref().is_some_and(|previous| {
            !stage
                .product
                .symlink_metadata()
                .is_ok_and(|actual| same_file_identity(previous, &actual))
        }) {
            Err(ImageToolError::new(format!(
                "The previous output at {} changed",
                stage.target.display()
            )))
        } else {
            moves
                .verify_links(&stage.target)
                .and_then(|()| open(&publication, &stage, &mut registry))
        };
        match opened {
            Ok(opened) => Ok(opened),
            Err(error) => match publication.restore(&stage) {
                Ok(()) => Err(error),
                Err(restore) => {
                    recoverable_stage = false;
                    Err(ImageToolError::new(format!(
                        "{error}. Could not restore the previous output: {restore}"
                    )))
                }
            },
        }
    })();
    drop(registry);
    match result {
        Ok(opened) => {
            stage.directory.disable_cleanup(false);
            Ok((opened, moves.finish(&stage.target)))
        }
        Err(error) => {
            let (error, restored) = moves.rollback(error);
            if restored && recoverable_stage {
                stage.directory.disable_cleanup(false);
                Err(error)
            } else {
                Err(ImageToolError::new(format!(
                    "{error}. Concatenation recovery directory: {}",
                    stage.directory.path().display()
                )))
            }
        }
    }
}

/// Builds and publishes a primary image and optional companion image transactionally.
pub fn publish_image_pair(
    outfile: &str,
    companion: &str,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    build: impl FnOnce(
        Option<&Path>,
        Option<&Path>,
    ) -> ImageToolResult<(ImageHandle, Option<ImageHandle>)>,
) -> ImageToolResult<AttachedImage> {
    publish_staged_pair(
        StagedImage::allocate(outfile)?,
        StagedImage::allocate(companion)?,
        overwrite,
        is_external_open,
        build,
    )
}

/// Publishes a pre-allocated staged primary/companion pair transactionally.
pub fn publish_staged_pair(
    primary: Option<StagedImage>,
    companion: Option<StagedImage>,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    build: impl FnOnce(
        Option<&Path>,
        Option<&Path>,
    ) -> ImageToolResult<(ImageHandle, Option<ImageHandle>)>,
) -> ImageToolResult<AttachedImage> {
    publish_staged_pair_core(
        primary,
        companion,
        overwrite,
        is_external_open,
        build,
        InstalledImage::open,
        AttachedImage::temporary,
    )
}

fn publish_staged_pair_core<R>(
    primary: Option<StagedImage>,
    companion: Option<StagedImage>,
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    build: impl FnOnce(
        Option<&Path>,
        Option<&Path>,
    ) -> ImageToolResult<(ImageHandle, Option<ImageHandle>)>,
    open_primary: impl FnOnce(&InstalledImage, &mut OpenImages) -> ImageToolResult<R>,
    wrap_temporary: impl FnOnce(ImageHandle) -> R,
) -> ImageToolResult<R> {
    check_distinct(primary.as_ref(), companion.as_ref())?;
    let output_paths = primary
        .iter()
        .chain(companion.iter())
        .filter(|_| overwrite)
        .flat_map(StagedImage::output_paths);
    let (image, secondary) = crate::analysis::with_mask_output_paths(output_paths, || {
        build(
            primary.as_ref().map(|stage| stage.product.as_path()),
            companion.as_ref().map(|stage| stage.product.as_path()),
        )
    })?;
    drop(secondary);
    let temporary = if primary.is_none() {
        Some(wrap_temporary(image))
    } else {
        drop(image);
        None
    };
    if primary.is_none() && companion.is_none() {
        return Ok(temporary.expect("temporary result"));
    }
    let persistent = primary.is_some();
    let mut registry = attachment::registry();
    check_distinct(primary.as_ref(), companion.as_ref())?;
    let _companion_lock = companion
        .as_ref()
        .map(|stage| TableWriteLock::acquire(&stage.product, 1))
        .transpose()?;
    let mut locks = BTreeMap::new();
    let installed = install_images(
        [companion, primary]
            .into_iter()
            .flatten()
            .map(|stage| (stage, overwrite)),
        &registry,
        &mut locks,
        is_external_open,
    )?;

    match persistent
        .then(|| {
            open_primary(
                installed.last().expect("primary is installed last"),
                &mut registry,
            )
        })
        .transpose()
    {
        Ok(native) => Ok(native.or(temporary).expect("image result")),
        Err(error) => Err(installed
            .into_iter()
            .rev()
            .fold(error, |error, product| product.restore(error))),
    }
}

/// Stages and publishes a fixed set of named output images atomically.
pub fn publish_image_set<T, const N: usize>(
    outputs: [&str; N],
    overwrite: bool,
    is_external_open: ExternalPathCheck<'_>,
    build: impl FnOnce([Option<&Path>; N]) -> ImageToolResult<T>,
) -> ImageToolResult<T> {
    let stages = NamedImageStages::new(outputs, overwrite)?;
    let paths = stages.paths();
    let outputs = stages
        .stages
        .iter()
        .filter_map(Option::as_ref)
        .filter(|(_, overwrite)| *overwrite)
        .flat_map(|(stage, _)| stage.output_paths());
    let product = crate::analysis::with_mask_output_paths(outputs, || {
        build(std::array::from_fn(|index| paths[index]))
    })?;
    stages.publish(false, is_external_open)?;
    Ok(product)
}

/// Multi-output staging bundle supporting distinct overwrite policies and transactional rollback.
pub struct NamedImageStages {
    stages: Vec<Option<(StagedImage, bool)>>,
}

impl NamedImageStages {
    /// Reserves staging slots for every non-empty output name with uniform `overwrite`.
    pub fn new<'a>(
        outputs: impl IntoIterator<Item = &'a str>,
        overwrite: bool,
    ) -> ImageToolResult<Self> {
        Self::with_replacements(outputs.into_iter().map(|name| (name, overwrite)))
    }

    /// Reserves staging slots for `(name, overwrite)` pairs, validating distinctness and conflicts.
    pub fn with_replacements<'a>(
        outputs: impl IntoIterator<Item = (&'a str, bool)>,
    ) -> ImageToolResult<Self> {
        let stages = outputs
            .into_iter()
            .map(|(name, overwrite)| {
                StagedImage::allocate(name).map(|stage| stage.map(|stage| (stage, overwrite)))
            })
            .collect::<ImageToolResult<Vec<_>>>()?;
        let set = Self { stages };
        set.check_distinct()?;
        for (stage, overwrite) in set.stages.iter().flatten() {
            if !overwrite && stage.target.symlink_metadata().is_ok() {
                return Err(ImageToolError::new(format!(
                    "File {} already exists",
                    stage.requested
                )));
            }
        }
        Ok(set)
    }

    fn check_distinct(&self) -> ImageToolResult<()> {
        for (index, stage) in self.stages.iter().enumerate() {
            for previous in &self.stages[..index] {
                check_distinct(
                    stage.as_ref().map(|(stage, _)| stage),
                    previous.as_ref().map(|(stage, _)| stage),
                )?;
            }
        }
        Ok(())
    }

    /// Returns the temporary build paths corresponding to each slot.
    pub fn paths(&self) -> Vec<Option<&Path>> {
        self.stages
            .iter()
            .map(|stage| stage.as_ref().map(|(stage, _)| stage.product.as_path()))
            .collect()
    }

    /// Drops slot `index` from publication.
    pub fn omit(&mut self, index: usize) {
        self.stages[index] = None;
    }

    /// Atomically publishes all staged slots and opens slot 0 as an [`AttachedImage`].
    pub fn publish_and_open_primary(
        self,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<Option<AttachedImage>> {
        let primary = self
            .stages
            .first()
            .and_then(Option::as_ref)
            .map(|(stage, _)| stage.target.clone());
        self.publish_transaction(false, true, is_external_open, |registry| {
            primary
                .as_deref()
                .map(|path| AttachedImage::open_created(registry, path))
                .transpose()
        })
    }

    /// Atomically publishes all staged slots (`allow_missing` drops unbuilt slots).
    pub fn publish(
        self,
        allow_missing: bool,
        is_external_open: ExternalPathCheck<'_>,
    ) -> ImageToolResult<()> {
        self.publish_with(allow_missing, is_external_open, || Ok(()))
    }

    /// Atomically publishes all staged slots and runs `finish` while holding the registry lock.
    pub fn publish_with(
        self,
        allow_missing: bool,
        is_external_open: ExternalPathCheck<'_>,
        finish: impl FnOnce() -> ImageToolResult<()>,
    ) -> ImageToolResult<()> {
        self.publish_transaction(allow_missing, false, is_external_open, |_| finish())
    }

    fn publish_transaction<T>(
        mut self,
        allow_missing: bool,
        open_primary: bool,
        is_external_open: ExternalPathCheck<'_>,
        finish: impl FnOnce(&mut OpenImages) -> ImageToolResult<T>,
    ) -> ImageToolResult<T> {
        if allow_missing {
            for slot in &mut self.stages {
                if let Some((stage, _)) = slot
                    && let Err(error) = stage.product.symlink_metadata()
                {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        *slot = None;
                    } else {
                        return Err(error.into());
                    }
                }
            }
        }
        let mut registry = attachment::registry();
        self.check_distinct()?;
        let _product_locks = self
            .stages
            .iter()
            .skip(usize::from(open_primary))
            .flatten()
            .map(|(stage, _)| {
                TableWriteLock::acquire(&stage.product, 1).map_err(ImageToolError::from)
            })
            .collect::<ImageToolResult<Vec<_>>>()?;
        let mut locks = BTreeMap::new();
        let installed = install_images(
            self.stages.into_iter().flatten(),
            &registry,
            &mut locks,
            is_external_open,
        )?;
        finish(&mut registry).map_err(|error| {
            installed
                .into_iter()
                .rev()
                .fold(error, |error, image| image.restore(error))
        })
    }
}

fn install_images(
    stages: impl IntoIterator<Item = (StagedImage, bool)>,
    registry: &OpenImages,
    locks: &mut BTreeMap<PathBuf, TableWriteLock>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<Vec<InstalledImage>> {
    let mut prepared = Vec::new();
    for (stage, overwrite) in stages {
        let existed = stage.check(overwrite, registry, locks, is_external_open)?;
        prepared.push((stage, existed));
    }
    let mut installed: Vec<InstalledImage> = Vec::new();
    for (stage, existed) in prepared {
        match stage.install(existed) {
            Ok(product) => installed.push(product),
            Err(error) => {
                return Err(installed
                    .into_iter()
                    .rev()
                    .fold(error, |error, product| product.restore(error)));
            }
        }
    }
    Ok(installed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::attachment::no_external_open;
    use crate::tool::coords::default_record_for_shape;
    use crate::tool::handle::Storage;
    use casa_coordinates::CoordinateSystem;

    fn test_image(path: Option<&Path>, size: usize) -> ImageToolResult<ImageHandle> {
        let shape = vec![size];
        let coordinates = CoordinateSystem::from_record(&default_record_for_shape(&shape, false)?)?;
        let mut output = Storage::<f32>::create(shape, coordinates, path, None)?;
        output.save()?;
        Ok(ImageHandle::Float(output))
    }

    fn owned_move_fixture(
        root: &Path,
    ) -> (
        StagedImage,
        OwnedOperands,
        [PathBuf; 2],
        [std::fs::Metadata; 2],
    ) {
        let sources = [root.join("a.im"), root.join("b.im")];
        for source in &sources {
            test_image(Some(source), 2).unwrap();
        }
        let snapshots = sources.clone().map(|path| path.symlink_metadata().unwrap());
        let result = root.join("output.im");
        let stage = StagedImage::allocate_at("output.im", &result).unwrap();
        std::fs::create_dir(&stage.product).unwrap();
        let owned = OwnedOperands::inspect(&sources, &result, true).unwrap();
        (stage, owned, sources, snapshots)
    }

    #[test]
    fn publish_image_stages_and_replaces_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out.im");
        let coords =
            CoordinateSystem::from_record(&default_record_for_shape(&[4, 4], true).unwrap())
                .unwrap();

        let created = publish_image(target.to_str().unwrap(), false, &no_external_open, |path| {
            Ok(ImageHandle::Float(Storage::create(
                vec![4, 4],
                coords.clone(),
                path,
                None,
            )?))
        })
        .unwrap();
        assert_eq!(created.shape(), &[4, 4]);
        assert!(target.join("table.dat").is_file());
        drop(created);

        let err = publish_image(target.to_str().unwrap(), false, &no_external_open, |path| {
            Ok(ImageHandle::Float(Storage::create(
                vec![5, 5],
                coords.clone(),
                path,
                None,
            )?))
        })
        .err()
        .unwrap();
        assert!(err.to_string().contains("exists"));
        assert_eq!(ImageHandle::open(&target).unwrap().shape(), &[4, 4]);

        let replaced = publish_image(target.to_str().unwrap(), true, &no_external_open, |path| {
            Ok(ImageHandle::Float(Storage::create(
                vec![6, 6],
                coords.clone(),
                path,
                None,
            )?))
        })
        .unwrap();
        assert_eq!(replaced.shape(), &[6, 6]);
    }

    #[test]
    fn owned_virtual_move_rolls_back_when_later_member_is_open() {
        let root = tempfile::tempdir().unwrap();
        let (stage, members, sources, snapshots) = owned_move_fixture(root.path());
        let target = stage.target.clone();
        let staged_path = stage.directory.path().to_path_buf();
        let open_second = AttachedImage::open(&sources[1]).unwrap();
        let Err(error) =
            install_owned_virtual_concat(stage, false, &no_external_open, members, |_, _, _| {
                panic!("an input open elsewhere should prevent publication")
            })
        else {
            panic!("moving an open input succeeded");
        };
        assert!(error.to_string().contains("still open in this process"));
        drop(open_second);
        assert!(!target.exists());
        assert!(!staged_path.exists());
        for (source, identity) in sources.iter().zip(snapshots) {
            assert!(same_file_identity(
                &identity,
                &source.symlink_metadata().unwrap()
            ));
        }
    }

    #[test]
    fn owned_virtual_move_restores_existing_output_after_reopen_failure() {
        let root = tempfile::tempdir().unwrap();
        let (stage, members, sources, snapshots) = owned_move_fixture(root.path());
        let target = stage.target.clone();
        test_image(Some(&target), 7).unwrap();
        let previous = target.symlink_metadata().unwrap();
        let Err(error) =
            install_owned_virtual_concat(stage, true, &no_external_open, members, |_, _, _| {
                Err(ImageToolError::new("injected reopen failure"))
            })
        else {
            panic!("the injected failure succeeded");
        };
        assert!(error.to_string().contains("injected reopen failure"));
        assert!(same_file_identity(
            &previous,
            &target.symlink_metadata().unwrap()
        ));
        assert_eq!(ImageHandle::open(&target).unwrap().shape(), &[7]);
        for (source, identity) in sources.iter().zip(snapshots) {
            assert!(same_file_identity(
                &identity,
                &source.symlink_metadata().unwrap()
            ));
        }
    }

    #[test]
    fn owned_virtual_publication_cannot_open_an_unrelated_replacement() {
        let root = tempfile::tempdir().unwrap();
        let (stage, members, sources, snapshots) = owned_move_fixture(root.path());
        let target = stage.target.clone();
        let displaced = root.path().join("displaced.im");
        std::fs::write(stage.product.join("imageconcat.json"), "{}").unwrap();
        let Err(error) = install_owned_virtual_concat(
            stage,
            false,
            &no_external_open,
            members,
            |publication, stage, registry| {
                std::fs::rename(&stage.target, &displaced)?;
                test_image(Some(&stage.target), 7)?;
                publication.open(stage, registry)
            },
        ) else {
            panic!("opening the competing replacement succeeded");
        };
        assert!(
            error
                .to_string()
                .contains("published virtual concatenation changed")
        );
        assert_eq!(ImageHandle::open(&target).unwrap().shape(), &[7]);
        for (source, identity) in sources.iter().zip(snapshots) {
            let retained = displaced.join(source.file_name().unwrap());
            assert!(same_file_identity(
                &identity,
                &retained.symlink_metadata().unwrap()
            ));
        }
    }

    #[test]
    fn owned_virtual_move_retains_original_when_rollback_name_is_occupied() {
        let root = tempfile::tempdir().unwrap();
        let (stage, members, sources, snapshots) = owned_move_fixture(root.path());
        let target = stage.target.clone();
        let staged_path = stage.directory.path().to_path_buf();
        let Err(error) =
            install_owned_virtual_concat(stage, false, &no_external_open, members, |_, _, _| {
                test_image(Some(&sources[0]), 7)?;
                Err(ImageToolError::new("injected reopen failure"))
            })
        else {
            panic!("the injected failure succeeded");
        };
        let detail = error.to_string();
        assert!(detail.contains("injected reopen failure"));
        assert!(detail.contains(staged_path.to_str().unwrap()));
        assert!(!target.exists());
        assert_eq!(ImageHandle::open(&sources[0]).unwrap().shape(), &[7]);
        assert!(same_file_identity(
            &snapshots[1],
            &sources[1].symlink_metadata().unwrap()
        ));
        let product = staged_path.join("image");
        let retained = product
            .read_dir()
            .unwrap()
            .flatten()
            .find(|entry| {
                entry
                    .path()
                    .symlink_metadata()
                    .is_ok_and(|actual| same_file_identity(&snapshots[0], &actual))
            })
            .expect("the original is retained in the reported stage");
        assert_eq!(ImageHandle::open(&retained.path()).unwrap().shape(), &[2]);
    }

    #[test]
    fn paired_publication_restores_both_destinations_after_primary_failure() {
        for existed in [false, true] {
            for invalid_primary in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let residual = root.path().join("residual.im");
                let companion = root.path().join("model.im");
                if existed {
                    test_image(Some(&residual), 2).unwrap();
                    test_image(Some(&companion), 3).unwrap();
                }
                let result = publish_image_pair(
                    residual.to_str().unwrap(),
                    companion.to_str().unwrap(),
                    true,
                    &no_external_open,
                    |primary_path, companion_path| {
                        let model = test_image(companion_path, 4)?;
                        if invalid_primary {
                            std::fs::create_dir(primary_path.unwrap())?;
                        }
                        Ok((test_image(None, 5)?, Some(model)))
                    },
                );
                assert!(result.is_err());
                if existed {
                    assert_eq!(ImageHandle::open(&residual).unwrap().shape(), &[2]);
                    assert_eq!(ImageHandle::open(&companion).unwrap().shape(), &[3]);
                } else {
                    assert!(!residual.exists());
                    assert!(!companion.exists());
                }
                assert_eq!(
                    root.path().read_dir().unwrap().count(),
                    if existed { 2 } else { 0 }
                );
            }
        }
    }

    #[test]
    fn named_publication_restores_replacements_if_the_primary_cannot_open() {
        let root = tempfile::tempdir().unwrap();
        let primary = root.path().join("primary.im");
        let auxiliary = root.path().join("auxiliary.im");
        let new_output = root.path().join("new.im");
        test_image(Some(&primary), 2).unwrap();
        test_image(Some(&auxiliary), 3).unwrap();
        let names = [&primary, &auxiliary, &new_output].map(|path| path.to_str().unwrap());
        let stages = NamedImageStages::new(names, true).unwrap();
        let paths = stages.paths();
        for path in &paths[1..] {
            test_image(*path, 5).unwrap();
        }
        std::fs::create_dir(paths[0].unwrap()).unwrap();
        assert!(stages.publish_and_open_primary(&no_external_open).is_err());
        assert_eq!(ImageHandle::open(&primary).unwrap().shape(), &[2]);
        assert_eq!(ImageHandle::open(&auxiliary).unwrap().shape(), &[3]);
        assert!(!new_output.exists());
        assert_eq!(root.path().read_dir().unwrap().count(), 2);
    }
}
