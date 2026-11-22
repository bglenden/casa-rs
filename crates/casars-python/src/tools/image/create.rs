// SPDX-License-Identifier: LGPL-3.0-or-later

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_coordinates::CoordinateSystem;
use casa_images::{ComponentListImage, ImagePixelType};
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{TableWriteLock, lexical_absolute};
use casa_types::{RecordValue, Value};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::IntList;
use crate::tools::componentlist::component_image_creation_model;
use crate::tools::coordsys::validate_restored_linear_coordinates;
use crate::tools::table::casa_record;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::attachment::{self, AttachedImage};
use super::concatenate::owned_members::{MemberMoves, OwnedOperands};
use super::handle::{ImageHandle, Storage, raised, same_file_identity};
use super::lifecycle::resolve_image_path;
use super::pixels::Pixels;

enum CreationInput {
    Shape(Option<IntList>),
    Pixels(PyResult<Pixels>),
}

struct Create<'a> {
    method: &'static str,
    outfile: &'a str,
    shape: Vec<usize>,
    record: RecordValue,
    linear: bool,
    overwrite: bool,
    log: bool,
    kind: String,
}

struct CreationGeometry {
    record: RecordValue,
    coordinates: CoordinateSystem,
    pixel_type: ImagePixelType,
}

impl Create<'_> {
    fn geometry(&self, pixels: Option<&Pixels>) -> PyResult<CreationGeometry> {
        let record = if self.record.fields().is_empty() {
            super::default_record::for_shape(&self.shape, self.linear)?
        } else if let [field] = self.record.fields()
            && let Value::Record(record) = &field.value
        {
            record.clone()
        } else {
            self.record.clone()
        };
        let coordinates = CoordinateSystem::from_record(&record)
            .map_err(|_| raised("Exception: Invalid coordinate system record"))?;
        if !self.record.is_empty() {
            validate_restored_linear_coordinates(&coordinates, &record)?;
        }
        let kind = self.kind.to_ascii_lowercase();
        let pixel_type = match (kind.as_str(), pixels.is_some_and(Pixels::is_complex)) {
            ("f", false) => ImagePixelType::Float32,
            ("d", false) => ImagePixelType::Float64,
            ("c", _) | ("f", true) => ImagePixelType::Complex32,
            ("cd", _) | ("d", true) => ImagePixelType::Complex64,
            _ => {
                return Err(raised(
                    "Exception: Input parm type must be either 'f', 'c', 'd', or 'cd'",
                ));
            }
        };
        Ok(CreationGeometry {
            record,
            coordinates,
            pixel_type,
        })
    }

    fn build(
        &self,
        geometry: CreationGeometry,
        pixels: Option<Pixels>,
        path: Option<&Path>,
        history: bool,
    ) -> PyResult<ImageHandle> {
        let CreationGeometry {
            record,
            coordinates,
            pixel_type,
        } = geometry;
        if coordinates.n_pixel_axes() != self.shape.len() {
            return Err(raised(
                "Exception: Supplied CoordinateSystem and image shape are inconsistent.",
            ));
        }
        let shape = self.shape.clone();
        let mut image = match pixel_type {
            ImagePixelType::Float32 => ImageHandle::Float(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::f32).as_ref(),
                )
                .map_err(raised)?,
            ),
            ImagePixelType::Float64 => ImageHandle::Double(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::f64).as_ref(),
                )
                .map_err(raised)?,
            ),
            ImagePixelType::Complex32 => ImageHandle::Complex(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::c32).as_ref(),
                )
                .map_err(raised)?,
            ),
            ImagePixelType::Complex64 => ImageHandle::DComplex(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::c64).as_ref(),
                )
                .map_err(raised)?,
            ),
        };
        image.set_coordinate_record(record)?;
        self.history(&mut image, pixels.as_ref(), history)?;
        image.save().map_err(raised)?;
        Ok(image)
    }

    fn history(
        &self,
        image: &mut ImageHandle,
        pixels: Option<&Pixels>,
        enabled: bool,
    ) -> PyResult<()> {
        let (storage, filename) = if self.outfile.is_empty() {
            ("Temp", String::new())
        } else {
            ("Paged", format!("'{}'", self.outfile))
        };
        let pixel_type = match image {
            ImageHandle::Float(_) => "float",
            ImageHandle::Double(_) => "double",
            ImageHandle::Complex(_) => "Complex",
            ImageHandle::DComplex(_) => "DComplex",
        };
        let creation = format!(
            "Created {storage} image {filename} of shape {:?} with {pixel_type} valued pixels.",
            self.shape
        );
        image.append_history_at("::ImageFactory::createImage", &[creation])?;
        if enabled {
            let args = match pixels {
                Some(pixels) if self.method == "newimagefromarray" => {
                    format!("outfile={:?}, pixels={}", self.outfile, pixels.history())
                }
                Some(pixels) => format!("pixels={}", pixels.history()),
                None => format!(
                    "outfile={:?}, shape={}",
                    self.outfile,
                    format!("{:?}", self.shape).replace(' ', ""),
                ),
            };
            let kind = if self.method == "newimagefromarray" {
                String::new()
            } else {
                format!(", type={:?}", self.kind)
            };
            let call = format!(
                "ia.{}({args}, csys={{...}}, linear={}, overwrite={}, log={}{kind})",
                self.method, self.linear, self.overwrite, self.log
            );
            image.record_history(self.method, call)?;
        }
        Ok(())
    }

    fn image_shape(shape: Option<IntList>) -> PyResult<Vec<usize>> {
        let shape = shape.unwrap_or_else(|| IntList::of(&[0])).0;
        if shape.is_empty() {
            return Err(raised(
                "Exception: The shape must have more than zero elements.",
            ));
        }
        if shape.iter().any(|&length| length <= 0) {
            return Err(raised("Exception: All elements of shape must be positive."));
        }
        Ok(shape.into_iter().map(|length| length as usize).collect())
    }

    fn run(
        mut self,
        input: CreationInput,
        history: bool,
        expanded: PyResult<Cow<'_, str>>,
    ) -> PyResult<AttachedImage> {
        let pixels = match input {
            CreationInput::Shape(shape) => {
                self.shape = Self::image_shape(shape)?;
                None
            }
            CreationInput::Pixels(pixels) => {
                if !matches!(self.kind.to_ascii_lowercase().as_str(), "f" | "d") {
                    return Err(raised(format!(
                        "Exception: Unsupported value for type: {:?}. Supported values are \"d\" and \"f\"",
                        self.kind
                    )));
                }
                let pixels = pixels?;
                self.shape = pixels.shape().to_vec();
                Some(pixels)
            }
        };
        let geometry = self.geometry(pixels.as_ref())?;
        let primary =
            StagedImage::allocate_creation(self.outfile, expanded?.as_ref(), self.overwrite)?;
        publish_staged_pair(primary, None, self.overwrite, |path, _| {
            Ok((self.build(geometry, pixels, path, history)?, None))
        })
    }
}

fn output_exists(name: &str) -> PyErr {
    raised(format!(
        "Exception: File {name} exists, and the user does not want to remove it.."
    ))
}

fn output_is_not_a_table(path: &Path) -> PyErr {
    raised(format!(
        "Table {} already exists (and is not a true table directory)",
        path.display()
    ))
}

struct StagedImage {
    requested: String,
    target: PathBuf,
    alias: Option<PathBuf>,
    directory: tempfile::TempDir,
    product: PathBuf,
}

impl StagedImage {
    fn allocate(name: &str) -> PyResult<Option<Self>> {
        if name.is_empty() {
            return Ok(None);
        }
        Self::allocate_at(name, Path::new(name)).map(Some)
    }

    fn allocate_creation(name: &str, expanded: &str, overwrite: bool) -> PyResult<Option<Self>> {
        if name.is_empty() {
            return Ok(None);
        }
        let expanded = std::path::absolute(expanded).map_err(raised)?;
        if expanded.file_name().is_none() {
            return Err(raised("Invalid output image name"));
        }
        if !overwrite && expanded.symlink_metadata().is_ok() {
            return Err(output_exists(name));
        }
        let target = lexical_absolute(&expanded);
        if !target.parent().is_some_and(Path::is_dir) {
            return Err(raised(format!(
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
                    raised(error)
                }
            })?;
            let mut stage = Self::allocate_at(name, &referent)?;
            stage.alias = Some(target);
            return Ok(Some(stage));
        }
        Self::allocate_at(name, &target).map(Some)
    }

    fn allocate_at(name: &str, target: &Path) -> PyResult<Self> {
        let target = std::path::absolute(target).map_err(raised)?;
        let parent = target
            .parent()
            .ok_or_else(|| raised("Invalid output image path"))?;
        let directory = tempfile::tempdir_in(parent).map_err(raised)?;
        let product = directory.path().join("image");
        Ok(Self {
            requested: name.to_owned(),
            target,
            alias: None,
            directory,
            product,
        })
    }

    fn check_alias(&self) -> PyResult<()> {
        if let Some(alias) = &self.alias
            && !alias.canonicalize().is_ok_and(|path| path == self.target)
        {
            return Err(raised(format!(
                "The output alias {} changed during image creation",
                alias.display()
            )));
        }
        Ok(())
    }

    fn resolved_key(&self) -> PyResult<PathBuf> {
        if self.target.symlink_metadata().is_ok() {
            attachment::key(&self.target)
        } else {
            entry_key(&self.target)
        }
    }

    fn check(
        &self,
        overwrite: bool,
        registry: &attachment::OpenImages,
        locks: &mut BTreeMap<PathBuf, TableWriteLock>,
    ) -> PyResult<bool> {
        self.check_alias()?;
        let path = &self.target;
        match path.symlink_metadata() {
            Ok(_) if !overwrite => Err(output_exists(&self.requested)),
            Ok(_) => {
                let key = attachment::key(path)?;
                if attachment::conflicting_images(registry, &key, 0)
                    || attachment::in_use_by_expression(registry, &key)
                    || crate::tools::table::path_is_in_use(&key)
                    || key
                        .ancestors()
                        .filter(|parent| parent.join("table.dat").is_file())
                        .any(crate::tools::table::path_is_in_use)
                {
                    return Err(raised(format!(
                        "Invalid Table operation: SetupNewTable {} is already opened (is in the table cache)",
                        path.display()
                    )));
                }
                let empty = path
                    .read_dir()
                    .is_ok_and(|mut entries| entries.next().is_none());
                if !empty
                    && !(casa_images::concat_file::is_image_concat(&self.product)
                        && casa_images::concat_file::ConcatManifest::open(path).is_ok())
                    && casa_tables::Table::open_metadata_only(casa_tables::TableOptions::new(path))
                        .is_err()
                {
                    return Err(output_is_not_a_table(path));
                }
                attachment::lock_table_tree(&key, locks)?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(raised(error)),
        }
    }

    fn install(self, existed: bool) -> PyResult<InstalledImage> {
        let publication = self.publish(existed)?;
        Ok(InstalledImage {
            stage: self,
            publication,
        })
    }

    fn publish(&self, existed: bool) -> PyResult<ImagePublication> {
        self.check_alias()?;
        let virtual_concat = casa_images::concat_file::is_image_concat(&self.product);
        #[cfg(unix)]
        let directory = std::fs::File::open(&self.product).map_err(raised)?;
        if existed {
            exchange_directories(&self.product, &self.target).map_err(raised)?;
        } else {
            rename_without_replacing(&self.product, &self.target).map_err(raised)?;
        }
        Ok(ImagePublication {
            existed,
            virtual_concat,
            #[cfg(unix)]
            directory,
        })
    }
}

fn entry_key(path: &Path) -> PyResult<PathBuf> {
    let parent = path.parent().and_then(|parent| parent.canonicalize().ok());
    parent
        .zip(path.file_name())
        .map(|(parent, name)| parent.join(name))
        .ok_or_else(|| raised("Invalid output image path"))
}

fn check_distinct(primary: Option<&StagedImage>, companion: Option<&StagedImage>) -> PyResult<()> {
    let (Some(first), Some(second)) = (primary, companion) else {
        return Ok(());
    };
    let check = |container: &StagedImage, child: &StagedImage| -> PyResult<bool> {
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
        return Err(raised("Output images must have separate destinations"));
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
    fn open(&self, registry: &mut attachment::OpenImages) -> PyResult<AttachedImage> {
        self.publication.open(&self.stage, registry)
    }

    fn restore(self, original: PyErr) -> PyErr {
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
        registry: &mut attachment::OpenImages,
    ) -> PyResult<AttachedImage> {
        if self.virtual_concat {
            stage.check_alias()?;
            if !self.still_at_destination(&stage.target) {
                return Err(changed_virtual_output());
            }
            let name = stage.alias.as_deref().unwrap_or(&stage.target);
            let image =
                super::concatenate::virtual_image::open_handle(&stage.target).map_err(raised)?;
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
            return Err(raised(format!(
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

fn changed_virtual_output() -> PyErr {
    raised("The published virtual concatenation changed during creation")
}

fn report_publication_recovery(
    stage: StagedImage,
    original: PyErr,
    error: std::io::Error,
) -> PyErr {
    let recovery = stage.directory.keep();
    raised(format!(
        "{original}. Image publication could not be rolled back. Inspect {} ({error})",
        recovery.display()
    ))
}

pub(super) fn publish_image(
    outfile: &str,
    overwrite: bool,
    build: impl FnOnce(Option<&Path>) -> PyResult<ImageHandle>,
) -> PyResult<AttachedImage> {
    publish_image_pair(outfile, "", overwrite, |target, _| {
        Ok((build(target)?, None))
    })
}

pub(super) fn publish_virtual_concat(
    outfile: &str,
    expanded: &str,
    overwrite: bool,
    build: impl FnOnce(&Path, &Path) -> PyResult<ImageHandle>,
) -> PyResult<AttachedImage> {
    let stage = StagedImage::allocate_creation(outfile, expanded, overwrite)?
        .ok_or_else(|| raised("Virtual concatenation requires an output image"))?;
    let final_path = stage.target.clone();
    publish_staged_pair(Some(stage), None, overwrite, |path, _| {
        Ok((
            build(&final_path, path.expect("virtual output stage"))?,
            None,
        ))
    })
}

pub(super) fn publish_owned_virtual_concat(
    outfile: &str,
    expanded: &str,
    overwrite: bool,
    prepare: impl FnOnce(&Path, &Path) -> PyResult<OwnedOperands>,
) -> PyResult<(AttachedImage, Vec<String>)> {
    let stage = StagedImage::allocate_creation(outfile, expanded, overwrite)?
        .ok_or_else(|| raised("Virtual concatenation requires an output image"))?;
    let members = prepare(&stage.target, &stage.product)?;
    install_owned_virtual_concat(stage, overwrite, members, ImagePublication::open)
}

fn install_owned_virtual_concat(
    mut stage: StagedImage,
    overwrite: bool,
    members: OwnedOperands,
    open: impl FnOnce(
        &ImagePublication,
        &StagedImage,
        &mut attachment::OpenImages,
    ) -> PyResult<AttachedImage>,
) -> PyResult<(AttachedImage, Vec<String>)> {
    let mut registry = attachment::registry();
    let mut locks = BTreeMap::new();
    let existed = stage.check(overwrite, &registry, &mut locks)?;
    let previous = existed
        .then(|| stage.target.symlink_metadata().map_err(raised))
        .transpose()?;
    // The stage can hold the only copy of a moved member. Returning from a
    // failed transaction must never remove such a stage automatically.
    stage.directory.disable_cleanup(true);
    let mut moves = MemberMoves::default();
    let mut recoverable_stage = true;
    let result = (|| {
        members.stage(&stage.target, &stage.product, &registry, &mut moves)?;
        let publication = stage.publish(existed)?;
        let opened = if previous.as_ref().is_some_and(|previous| {
            !stage
                .product
                .symlink_metadata()
                .is_ok_and(|actual| same_file_identity(previous, &actual))
        }) {
            Err(raised(format!(
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
                    Err(raised(format!(
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
                Err(raised(format!(
                    "{error}. Concatenation recovery directory: {}",
                    stage.directory.path().display()
                )))
            }
        }
    }
}

pub(super) fn publish_image_pair(
    outfile: &str,
    companion: &str,
    overwrite: bool,
    build: impl FnOnce(Option<&Path>, Option<&Path>) -> PyResult<(ImageHandle, Option<ImageHandle>)>,
) -> PyResult<AttachedImage> {
    publish_staged_pair(
        StagedImage::allocate(outfile)?,
        StagedImage::allocate(companion)?,
        overwrite,
        build,
    )
}

fn publish_staged_pair(
    primary: Option<StagedImage>,
    companion: Option<StagedImage>,
    overwrite: bool,
    build: impl FnOnce(Option<&Path>, Option<&Path>) -> PyResult<(ImageHandle, Option<ImageHandle>)>,
) -> PyResult<AttachedImage> {
    check_distinct(primary.as_ref(), companion.as_ref())?;
    let (image, secondary) = build(
        primary.as_ref().map(|stage| stage.product.as_path()),
        companion.as_ref().map(|stage| stage.product.as_path()),
    )?;
    drop(secondary);
    let temporary = if primary.is_none() {
        Some(AttachedImage::temporary(image))
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
    // The companion becomes visible first. Keep its access lock across
    // installation of the primary and any rollback.
    let _companion_lock = companion
        .as_ref()
        .map(|stage| TableWriteLock::acquire(&stage.product, 1))
        .transpose()
        .map_err(raised)?;
    let mut locks = BTreeMap::new();
    let installed = install_images(
        [companion, primary]
            .into_iter()
            .flatten()
            .map(|stage| (stage, overwrite)),
        &registry,
        &mut locks,
    )?;

    match persistent
        .then(|| {
            installed
                .last()
                .expect("primary is installed last")
                .open(&mut registry)
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

pub(super) fn publish_image_set<T, const N: usize>(
    outputs: [&str; N],
    overwrite: bool,
    build: impl FnOnce([Option<&Path>; N]) -> PyResult<T>,
) -> PyResult<T> {
    let stages = NamedImageStages::new(outputs, overwrite)?;
    let paths = stages.paths();
    let product = build(std::array::from_fn(|index| paths[index]))?;
    stages.publish(false)?;
    Ok(product)
}

pub(super) struct NamedImageStages {
    stages: Vec<Option<(StagedImage, bool)>>,
}

impl NamedImageStages {
    pub(super) fn new<'a>(
        outputs: impl IntoIterator<Item = &'a str>,
        overwrite: bool,
    ) -> PyResult<Self> {
        Self::with_replacements(outputs.into_iter().map(|name| (name, overwrite)))
    }

    pub(super) fn with_replacements<'a>(
        outputs: impl IntoIterator<Item = (&'a str, bool)>,
    ) -> PyResult<Self> {
        let stages = outputs
            .into_iter()
            .map(|(name, overwrite)| {
                StagedImage::allocate(name).map(|stage| stage.map(|stage| (stage, overwrite)))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let set = Self { stages };
        set.check_distinct()?;
        for (stage, overwrite) in set.stages.iter().flatten() {
            if !overwrite && stage.target.symlink_metadata().is_ok() {
                return Err(raised(format!("File {} already exists", stage.requested)));
            }
        }
        Ok(set)
    }

    fn check_distinct(&self) -> PyResult<()> {
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

    pub(super) fn paths(&self) -> Vec<Option<&Path>> {
        self.stages
            .iter()
            .map(|stage| stage.as_ref().map(|(stage, _)| stage.product.as_path()))
            .collect()
    }

    pub(super) fn omit(&mut self, index: usize) {
        self.stages[index] = None;
    }

    pub(super) fn publish_and_open_primary(self) -> PyResult<Option<AttachedImage>> {
        let primary = self
            .stages
            .first()
            .and_then(Option::as_ref)
            .map(|(stage, _)| stage.target.clone());
        self.publish_transaction(false, true, |registry| {
            primary
                .as_deref()
                .map(|path| AttachedImage::open_created(registry, path))
                .transpose()
        })
    }

    pub(super) fn publish(self, allow_missing: bool) -> PyResult<()> {
        self.publish_with(allow_missing, || Ok(()))
    }

    pub(super) fn publish_with(
        self,
        allow_missing: bool,
        finish: impl FnOnce() -> PyResult<()>,
    ) -> PyResult<()> {
        self.publish_transaction(allow_missing, false, |_| finish())
    }

    fn publish_transaction<T>(
        mut self,
        allow_missing: bool,
        open_primary: bool,
        finish: impl FnOnce(&mut attachment::OpenImages) -> PyResult<T>,
    ) -> PyResult<T> {
        if allow_missing {
            for slot in &mut self.stages {
                if let Some((stage, _)) = slot
                    && let Err(error) = stage.product.symlink_metadata()
                {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        *slot = None;
                    } else {
                        return Err(raised(error));
                    }
                }
            }
        }
        let mut registry = attachment::registry();
        self.check_distinct()?;
        let _product_locks = self
            .stages
            .iter()
            // Opening the returned image takes its staged image lock itself,
            // just as the paired publisher does for its primary product.
            .skip(usize::from(open_primary))
            .flatten()
            .map(|(stage, _)| TableWriteLock::acquire(&stage.product, 1).map_err(raised))
            .collect::<PyResult<Vec<_>>>()?;
        let mut locks = BTreeMap::new();
        let installed = install_images(self.stages.into_iter().flatten(), &registry, &mut locks)?;
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
    registry: &attachment::OpenImages,
    locks: &mut BTreeMap<PathBuf, TableWriteLock>,
) -> PyResult<Vec<InstalledImage>> {
    let mut prepared = Vec::new();
    for (stage, overwrite) in stages {
        let existed = stage.check(overwrite, registry, locks)?;
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

impl PyImageTool {
    fn replace_from(
        &self,
        py: Python<'_>,
        request: Create<'_>,
        input: CreationInput,
    ) -> PyResult<bool> {
        let expanded = resolve_image_path(py, request.outfile);
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            state.image = Some(request.run(input, state.history, expanded)?);
            Ok(true)
        })
    }

    fn new_from(
        &self,
        py: Python<'_>,
        request: Create<'_>,
        input: CreationInput,
    ) -> PyResult<Py<Self>> {
        let expanded = resolve_image_path(py, request.outfile);
        let image = py.detach(|| {
            let history = self.state().history;
            request.run(input, history, expanded)
        })?;
        Py::new(py, Self::attached(image))
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyImageTool {
    #[pyo3(signature = (outfile = "", shape = None, cl = None, csys = None, overwrite = false, log = true, cache = true), text_signature = "($self, outfile='', shape=[], cl='', csys={}, overwrite=False, log=True, cache=True)")]
    fn fromcomplist(
        &self,
        py: Python<'_>,
        outfile: &str,
        shape: Option<IntList>,
        cl: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyDict>>,
        overwrite: bool,
        log: bool,
        cache: bool,
    ) -> PyResult<bool> {
        let input = component_list_input(py, cl);
        let expanded = resolve_image_path(py, outfile);
        let record = casa_record(py, csys)?;
        let shape = shape.map_or_else(Vec::new, |shape| shape.0);
        let messages = py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let expanded = expanded?;
            let input = input?;
            if !(2..=4).contains(&shape.len()) {
                return Err(raised("ComponentListImages must have 2, 3, or 4 dimensions"));
            }
            if shape.iter().any(|&length| length <= 0) {
                return Err(raised("TiledShape: shape has to be > 0"));
            }
            let shape: Vec<usize> = shape.into_iter().map(|length| length as usize).collect();
            let geometry = Create {
                method: "fromcomplist",
                outfile,
                shape: shape.clone(),
                record,
                linear: false,
                overwrite,
                log,
                kind: "f".into(),
            }
            .geometry(None)?;
            let stage = StagedImage::allocate_creation(outfile, &expanded, overwrite)?;
            let mut messages = Vec::new();
            let (components, spectral_records) = input.load(stage.is_some(), &mut messages)?;
            let message = format!("Created ComponentListImage {outfile} of shape {shape:?}.");
            let source = ComponentListImage::new(shape.clone(), components, geometry.coordinates, cache)
                .map_err(raised)?;
            let attachment = publish_staged_pair(stage, None, overwrite, |path, _| {
                if let Some(path) = path {
                    source.write_native(path, &geometry.record, &spectral_records).map_err(raised)?;
                }
                let mut image = ImageHandle::from_component(source, geometry.record).map_err(raised)?;
                if path.is_none() {
                    image.append_history_at(
                        "ImageFactory::createComponentListImage", std::slice::from_ref(&message),
                    )?;
                    if state.history {
                        image.record_history(
                            "fromcomplist",
                            format!("ia.fromcomplist(outfile={outfile:?}, shape={shape:?}, cl={{...}}, csys={{...}}, overwrite={overwrite}, log={log}, cache={cache})"),
                        )?;
                    }
                }
                Ok((image, None))
            })?;
            if !cache && !outfile.is_empty() {
                attachment.lock().disable_component_cache().map_err(raised)?;
            }
            state.image = Some(attachment);
            if log {
                messages.push(LogLine::info(message));
            }
            Ok::<_, PyErr>(messages)
        })?;
        emit(py, "casars.tools.image", &messages)?;
        Ok(true)
    }

    #[pyo3(signature = (outfile = "", shape = None, csys = None, linear = false, overwrite = false, log = true, r#type = "f"), text_signature = "($self, outfile='', shape=[0], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn fromshape(
        &self,
        py: Python<'_>,
        outfile: &str,
        shape: Option<IntList>,
        csys: Option<&Bound<'_, PyDict>>,
        linear: bool,
        overwrite: bool,
        log: bool,
        r#type: &str,
    ) -> PyResult<bool> {
        let request = Create {
            method: "fromshape",
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            outfile,
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        };
        self.replace_from(py, request, CreationInput::Shape(shape))
    }

    #[pyo3(signature = (outfile = "", pixels = None, csys = None, linear = false, overwrite = false, log = true, r#type = "f"), text_signature = "($self, outfile='', pixels=[], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn fromarray(
        &self,
        py: Python<'_>,
        outfile: &str,
        pixels: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyDict>>,
        linear: bool,
        overwrite: bool,
        log: bool,
        r#type: &str,
    ) -> PyResult<bool> {
        let request = Create {
            method: "fromarray",
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            outfile,
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        };
        self.replace_from(
            py,
            request,
            CreationInput::Pixels(Pixels::capture(py, pixels, true)),
        )
    }

    #[pyo3(signature = (outfile = "", shape = None, csys = None, linear = false, overwrite = false, log = true, r#type = "f"), text_signature = "($self, outfile='', shape=[0], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn newimagefromshape(
        &self,
        py: Python<'_>,
        outfile: &str,
        shape: Option<IntList>,
        csys: Option<&Bound<'_, PyDict>>,
        linear: bool,
        overwrite: bool,
        log: bool,
        r#type: &str,
    ) -> PyResult<Py<Self>> {
        let request = Create {
            method: "newimagefromshape",
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            outfile,
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        };
        self.new_from(py, request, CreationInput::Shape(shape))
    }

    #[pyo3(signature = (outfile = "", pixels = None, csys = None, linear = false, overwrite = false, log = true, r#type = "f"), text_signature = "($self, outfile='', pixels=[], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn newimagefromarray(
        &self,
        py: Python<'_>,
        outfile: &str,
        pixels: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyDict>>,
        linear: bool,
        overwrite: bool,
        log: bool,
        r#type: &str,
    ) -> PyResult<Py<Self>> {
        let request = Create {
            method: "newimagefromarray",
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            outfile,
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        };
        self.new_from(
            py,
            request,
            CreationInput::Pixels(Pixels::capture(py, pixels, true)),
        )
    }
}

enum ComponentListInput {
    Record(RecordValue),
    Table(PathBuf),
}

impl ComponentListInput {
    fn load(
        self,
        persistent: bool,
        logs: &mut Vec<LogLine>,
    ) -> PyResult<(ComponentList, BTreeMap<usize, RecordValue>)> {
        match self {
            Self::Record(record) => Ok(component_image_creation_model(&record, logs, persistent)?),
            Self::Table(path) => {
                // CASA opens the list, then constructs the image from its
                // public component records. Column-only metadata does not
                // pass through that conversion.
                let components = ComponentList::read_table(path).map_err(raised)?;
                let record = components.to_record().map_err(raised)?;
                Ok((
                    ComponentList::from_record(&record).map_err(raised)?,
                    BTreeMap::new(),
                ))
            }
        }
    }
}

fn component_list_input(
    py: Python<'_>,
    input: Option<&Bound<'_, PyAny>>,
) -> PyResult<ComponentListInput> {
    let Some(input) = input else {
        return Err(raised(
            "Exception: Component list table name cannot be empty.",
        ));
    };
    if let Ok(record) = input.cast::<PyDict>() {
        return casa_record(py, Some(record)).map(ComponentListInput::Record);
    }
    match input.extract::<String>() {
        Ok(name) if name.is_empty() => Err(raised(
            "Exception: Component list table name cannot be empty.",
        )),
        Ok(name) => resolve_image_path(py, &name)
            .map(|expanded| ComponentListInput::Table(PathBuf::from(expanded.as_ref()))),
        Err(_) => Err(raised("Unsupported type for parameter cl")),
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;

    fn test_image(path: Option<&Path>, size: usize) -> PyResult<ImageHandle> {
        let shape = vec![size];
        let coordinates =
            CoordinateSystem::from_record(&super::super::default_record::for_shape(&shape, false)?)
                .map_err(raised)?;
        let mut output = Storage::<f32>::create(shape, coordinates, path, None).map_err(raised)?;
        output.save().map_err(raised)?;
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
    fn owned_virtual_move_rolls_back_when_later_member_is_open() {
        Python::initialize();
        let root = tempfile::tempdir().unwrap();
        let (stage, members, sources, snapshots) = owned_move_fixture(root.path());
        let target = stage.target.clone();
        let staged_path = stage.directory.path().to_path_buf();
        let open_second = AttachedImage::open(&sources[1]).unwrap();
        let Err(error) = install_owned_virtual_concat(stage, false, members, |_, _, _| {
            panic!("an input open elsewhere should prevent publication")
        }) else {
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
        Python::initialize();
        let root = tempfile::tempdir().unwrap();
        let (stage, members, sources, snapshots) = owned_move_fixture(root.path());
        let target = stage.target.clone();
        test_image(Some(&target), 7).unwrap();
        let previous = target.symlink_metadata().unwrap();
        let Err(error) = install_owned_virtual_concat(stage, true, members, |_, _, _| {
            Err(raised("injected reopen failure"))
        }) else {
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
        Python::initialize();
        let root = tempfile::tempdir().unwrap();
        let (stage, members, sources, snapshots) = owned_move_fixture(root.path());
        let target = stage.target.clone();
        let displaced = root.path().join("displaced.im");
        std::fs::write(stage.product.join("imageconcat.json"), "{}").unwrap();
        let Err(error) =
            install_owned_virtual_concat(stage, false, members, |publication, stage, registry| {
                std::fs::rename(&stage.target, &displaced).map_err(raised)?;
                test_image(Some(&stage.target), 7)?;
                publication.open(stage, registry)
            })
        else {
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
        Python::initialize();
        let root = tempfile::tempdir().unwrap();
        let (stage, members, sources, snapshots) = owned_move_fixture(root.path());
        let target = stage.target.clone();
        let staged_path = stage.directory.path().to_path_buf();
        let Err(error) = install_owned_virtual_concat(stage, false, members, |_, _, _| {
            test_image(Some(&sources[0]), 7)?;
            Err(raised("injected reopen failure"))
        }) else {
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
        Python::initialize();
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
                    |primary_path, companion_path| {
                        let model = test_image(companion_path, 4)?;
                        if invalid_primary {
                            // Install a directory that fails native image reopen.
                            std::fs::create_dir(primary_path.unwrap()).map_err(raised)?;
                        }
                        // With no staged directory, primary installation fails.
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
        Python::initialize();
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
        // The directory can be installed. Opening it requires a CASA table.
        std::fs::create_dir(paths[0].unwrap()).unwrap();
        assert!(stages.publish_and_open_primary().is_err());
        assert_eq!(ImageHandle::open(&primary).unwrap().shape(), &[2]);
        assert_eq!(ImageHandle::open(&auxiliary).unwrap().shape(), &[3]);
        assert!(!new_output.exists());
        assert_eq!(root.path().read_dir().unwrap().count(), 2);
    }
}
