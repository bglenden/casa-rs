// SPDX-License-Identifier: LGPL-3.0-or-later

//! Concrete pixel storage handles and file-identity helpers for the image tool.

use std::cell::OnceCell;
use std::fs::Metadata;
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_core::{RecordValue, Value};
use casa_images::{
    AnyPagedImage, ImageError, ImageHistoryRow, ImageInterface, ImagePixel, ImagePixelType,
    MiriadImage, PagedImage, ScratchSpace, TempImage, TempStoragePolicy,
    fits_read::{FitsSectionReader, FitsToolImport, fits_section_image},
    image_history_rows,
};
use casa_tables::LockType;
use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};

use crate::ComponentListImage;

use super::error::{ImageToolError, ImageToolResult};

/// Backing storage for an open image of concrete pixel type `T`.
pub enum Storage<T: ImagePixel> {
    /// A persistent casacore paged image on disk.
    Paged(Box<PagedImage<T>>),
    /// An in-memory, scratch-paged, component, FITS, MIRIAD, or expression image.
    Temp {
        /// Underlying temporary/virtual image container.
        image: Box<TempImage<T>>,
        /// Preserved casacore coordinate record.
        record: RecordValue,
        /// Lazily opened FITS pixel reader when attached to a FITS file.
        fits: Option<FitsAttachment>,
    },
}

/// Lazily opened FITS pixel-section attachment paired with a metadata [`TempImage`].
pub struct FitsAttachment {
    path: PathBuf,
    hdu: usize,
    /// Full image shape.
    pub shape: Vec<usize>,
    /// Tile cursor shape reported for FITS sections.
    pub tile_shape: Vec<usize>,
    /// Whether the FITS image carries an implicit blank-value mask.
    pub has_mask: bool,
    pixels: OnceCell<FitsSectionReader>,
}

impl FitsAttachment {
    /// Builds an [`ImageInterface`] view over the FITS pixel stream and `metadata`.
    pub fn image_view<'a>(&'a self, metadata: &'a TempImage<f32>) -> impl ImageInterface<f32> + 'a {
        fits_section_image(&self.shape, metadata, &self.path, |start, count, stride| {
            self.read_pixels(start, count, stride)
        })
    }

    /// Reads a strided FITS section into a float array.
    pub fn read(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> ImageToolResult<ArrayD<f32>> {
        Ok(self.read_pixels(start, count, stride)?)
    }

    fn read_pixels(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, ImageError> {
        // CASA opens the pixel stream on the first read and retains it
        // across subsequent reads even if the filename is moved or removed.
        if let Some(reader) = self.pixels.get() {
            return reader.read(start, count, stride);
        }
        let reader = FitsSectionReader::open(&self.path, self.hdu, &self.shape)?;
        let section = reader.read(start, count, stride)?;
        let _ = self.pixels.set(reader);
        Ok(section)
    }
}

/// Type-erased open image handle across the four CASA pixel types.
pub enum ImageHandle {
    /// Single-precision real pixels (`f32`).
    Float(Storage<f32>),
    /// Double-precision real pixels (`f64`).
    Double(Storage<f64>),
    /// Single-precision complex pixels (`Complex32`).
    Complex(Storage<Complex32>),
    /// Double-precision complex pixels (`Complex64`).
    DComplex(Storage<Complex64>),
}

/// Dispatches over the concrete [`Storage`] inside an [`ImageHandle`].
#[macro_export]
macro_rules! with_image_handle_pixels {
    ($image:expr, $storage:ident => $work:expr) => {
        match $image {
            $crate::tool::ImageHandle::Float($storage) => $work,
            $crate::tool::ImageHandle::Double($storage) => $work,
            $crate::tool::ImageHandle::Complex($storage) => $work,
            $crate::tool::ImageHandle::DComplex($storage) => $work,
        }
    };
}

/// Dispatches over the concrete image (`PagedImage` or `TempImage`) inside a [`Storage`].
#[macro_export]
macro_rules! with_image_storage {
    ($storage:expr, $image:ident => $work:expr) => {
        match $storage {
            $crate::tool::Storage::Paged($image) => $work,
            $crate::tool::Storage::Temp { image: $image, .. } => $work,
        }
    };
}

pub use with_image_handle_pixels as with_pixels;
pub use with_image_storage as with_storage;

/// Returns whether two filesystem metadata snapshots refer to the same file object.
pub fn same_file_identity(expected: &Metadata, actual: &Metadata) -> bool {
    if expected.file_type() != actual.file_type() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        expected.dev() == actual.dev() && expected.ino() == actual.ino()
    }
    #[cfg(not(unix))]
    {
        expected.created().ok() == actual.created().ok()
    }
}

/// Returns whether two filesystem metadata snapshots share identity, size, and mtime.
pub fn same_file_snapshot(expected: &Metadata, actual: &Metadata) -> bool {
    same_file_identity(expected, actual)
        && expected.len() == actual.len()
        && expected.modified().ok() == actual.modified().ok()
}

impl ImageHandle {
    /// Opens an image from `path` (FITS, virtual concat, component list, or paged table).
    pub fn open(path: &Path) -> Result<Self, ImageError> {
        if path.is_file() {
            return Self::open_fits(path, FitsToolImport::open_attachment(path)?);
        }
        Self::open_native(path, Some(0))
    }

    /// Opens an image for read-only inspection without waiting on a table lock.
    pub fn open_read(path: &Path) -> Result<Self, ImageError> {
        if path.is_file() {
            return Self::open(path);
        }
        Self::open_native(path, Some(0))
    }

    /// Opens a native image directory with optional lock retry count.
    pub fn open_native(path: &Path, lock_attempts: Option<u32>) -> Result<Self, ImageError> {
        Self::open_native_with_cache(path, lock_attempts, true)
    }

    /// Opens a native image directory, configuring component-list pixel caching.
    pub fn open_native_with_cache(
        path: &Path,
        lock_attempts: Option<u32>,
        component_cache: bool,
    ) -> Result<Self, ImageError> {
        if casa_images::is_image_concat(path) {
            return super::concatenate::virtual_image::open_handle(path);
        }
        if super::saved_expression::recognizes(path) {
            return super::saved_expression::open_handle(path);
        }
        if Self::recognizes_miriad(path, path) {
            return Self::open_miriad(path);
        }
        if ComponentListImage::recognizes_native(path) {
            let (source, record) = ComponentListImage::open_native(path, component_cache)?;
            return Self::from_component(source, record);
        }
        #[cfg(unix)]
        if let Some(attempts) = lock_attempts {
            return AnyPagedImage::open_with_lock(path, attempts).map(Self::from_paged);
        }
        #[cfg(not(unix))]
        let _ = lock_attempts;
        AnyPagedImage::open(path).map(Self::from_paged)
    }

    /// Wraps a [`ComponentListImage`] and its coordinate record in a [`Self::Float`] handle.
    pub fn from_component(
        source: ComponentListImage,
        record: RecordValue,
    ) -> Result<Self, ImageError> {
        let image = TempImage::from_read_only_image(
            source,
            "ComponentListImage",
            "ComponentListImage::doPutSlice: pixel values cannot be modified in a ComponentListImage",
        )?;
        Ok(Self::Float(Storage::Temp {
            image: Box::new(image),
            record,
            fits: None,
        }))
    }

    /// Disables pixel caching on a [`ComponentListImage`]-backed handle.
    pub fn disable_component_cache(&mut self) -> Result<(), ImageError> {
        if let Self::Float(Storage::Temp { image, .. }) = self {
            image.set_source_cache(false)?;
        }
        Ok(())
    }

    fn from_paged(image: AnyPagedImage) -> Self {
        match image {
            AnyPagedImage::Float32(image) => Self::Float(Storage::Paged(Box::new(image))),
            AnyPagedImage::Float64(image) => Self::Double(Storage::Paged(Box::new(image))),
            AnyPagedImage::Complex32(image) => Self::Complex(Storage::Paged(Box::new(image))),
            AnyPagedImage::Complex64(image) => Self::DComplex(Storage::Paged(Box::new(image))),
        }
    }

    /// Opens a member image for virtual or paged concatenation.
    pub fn open_for_concatenation(path: &Path) -> Result<Self, ImageError> {
        if path.is_file() {
            return Self::open_fits(path, FitsToolImport::open_for_concatenation(path)?);
        }
        if Self::recognizes_miriad(path, path) {
            return Self::open_miriad(path);
        }
        Self::open_native(path, Some(0))
    }

    /// Wraps an already-parsed FITS import into a lazily-read [`Self::Float`] handle.
    pub fn open_fits(path: &Path, mut source: FitsToolImport) -> Result<Self, ImageError> {
        if source.is_tile_compressed() {
            return Err(ImageError::CasaSelection(format!(
                "No data in the zeroth or first extension of {}",
                path.display()
            )));
        }
        // Reuse temporary-image metadata operations. The attachment owns the
        // actual shape and pixel stream, so this needs only one storage cell.
        let mut image = TempImage::new(
            vec![1; source.shape().len()],
            source.coordinates().clone(),
            TempStoragePolicy::Memory,
        )?;
        let fits = FitsAttachment {
            path: path.to_path_buf(),
            hdu: source.hdu_index(),
            shape: source.shape().to_vec(),
            tile_shape: source.tile_shape(),
            has_mask: source.has_implicit_mask(),
            pixels: OnceCell::new(),
        };
        source.misc_info.remove("object");
        image.set_units(source.units)?;
        image.set_image_info_record(source.image_info)?;
        image.set_misc_info(source.misc_info)?;
        image.append_history_entries(&source.history)?;
        Ok(Self::Float(Storage::Temp {
            image: Box::new(image),
            record: source.coordinate_record,
            fits: Some(fits),
        }))
    }

    /// Returns the attached FITS reader when this handle wraps a FITS image.
    pub fn fits(&self) -> Option<&FitsAttachment> {
        match self {
            Self::Float(Storage::Temp { fits, .. }) => fits.as_ref(),
            _ => None,
        }
    }

    /// Returns whether `probe`/`resolved` looks like a MIRIAD image directory.
    pub fn recognizes_miriad(probe: &Path, resolved: &Path) -> bool {
        MiriadImage::recognizes_input(probe, resolved)
    }

    /// Returns whether `path` has a native casacore `table.dat` and is not MIRIAD.
    pub fn has_native_control(path: &Path) -> bool {
        path.join("table.dat").is_file() && !Self::recognizes_miriad(path, path)
    }

    /// Opens a read-only MIRIAD image wrapped in a [`Self::Float`] handle.
    pub fn open_miriad(path: &Path) -> Result<Self, ImageError> {
        let image = TempImage::from_read_only_image(
            MiriadImage::open(path)?,
            "MIRIADImage",
            "MIRIADImage::putSlice - is not possible yet as MIRIADImage is not writable",
        )?;
        Ok(Self::Float(Storage::Temp {
            record: image.coordinates().to_record(),
            image: Box::new(image),
            fits: None,
        }))
    }

    /// Returns the CASA image type string (`"PagedImage"`, `"TempImage"`, `"FITSImage"`, etc.).
    pub fn image_type_name(&self) -> &'static str {
        if self.fits().is_some() {
            "FITSImage"
        } else {
            with_pixels!(self, storage => with_storage!(storage, image => image.image_type_name()))
        }
    }

    /// Returns the type name if this image is a read-only external or saved-expression file.
    pub fn read_only_file_type(&self) -> Option<&'static str> {
        let kind = self.image_type_name();
        (matches!(kind, "FITSImage" | "MIRIADImage") || self.is_saved_expression()).then_some(kind)
    }

    /// Returns whether this handle is backed by a persistent saved expression directory.
    pub fn is_saved_expression(&self) -> bool {
        self.image_type_name() == "ImageExpr" && self.path().is_some()
    }

    /// Returns operand paths referenced by saved-expression members of a virtual concat.
    pub fn concat_expression_operands(&self) -> Vec<PathBuf> {
        with_pixels!(self, storage => match storage {
            Storage::Temp { image, .. } => image.concat_source()
                .map_or_else(Vec::new, |source| source.expression_operands().cloned().collect()),
            Storage::Paged(_) => Vec::new(),
        })
    }

    /// Returns all member paths of a virtual concatenation.
    pub fn concat_operands(&self) -> Vec<PathBuf> {
        with_pixels!(self, storage => match storage {
            Storage::Temp { image, .. } => image.concat_source()
                .map_or_else(Vec::new, |source| source.operand_paths().map(Path::to_path_buf).collect()),
            Storage::Paged(_) => Vec::new(),
        })
    }

    /// Returns the pixel data type.
    pub fn kind(&self) -> ImagePixelType {
        match self {
            Self::Float(_) => ImagePixelType::Float32,
            Self::Double(_) => ImagePixelType::Float64,
            Self::Complex(_) => ImagePixelType::Complex32,
            Self::DComplex(_) => ImagePixelType::Complex64,
        }
    }

    /// Returns the image shape.
    pub fn shape(&self) -> &[usize] {
        if let Some(fits) = self.fits() {
            return &fits.shape;
        }
        with_pixels!(self, storage => with_storage!(storage, image => image.shape()))
    }

    /// Returns the active [`CoordinateSystem`].
    pub fn coordinates(&self) -> &CoordinateSystem {
        with_pixels!(self, storage => with_storage!(storage, image => image.coordinates()))
    }

    /// Returns the backing file/directory path, if persistent.
    pub fn path(&self) -> Option<&Path> {
        if let Some(fits) = self.fits() {
            return Some(&fits.path);
        }
        with_pixels!(self, storage => with_storage!(storage, image => image.name()))
    }

    /// Flushes persistent changes to disk.
    pub fn save(&mut self) -> Result<(), ImageError> {
        with_pixels!(self, storage => storage.save())
    }

    /// Returns stored history rows.
    pub fn history_entries(&self) -> ImageToolResult<Vec<ImageHistoryRow>> {
        Ok(with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image_history_rows(image.name().expect("native image path"))?,
            Storage::Temp { image, .. } => image.history_rows()?.into_owned(),
        }))
    }

    /// Appends history rows to the image.
    pub fn append_history_entries(&mut self, entries: &[ImageHistoryRow]) -> ImageToolResult<()> {
        with_pixels!(self, storage => with_storage!(storage, image => image.append_history_entries(entries)))?;
        Ok(())
    }

    /// Appends log messages attributed to `location`.
    pub fn append_history_at(
        &mut self,
        location: &str,
        messages: &[String],
    ) -> ImageToolResult<()> {
        with_pixels!(self, storage => with_storage!(storage, image => image.append_history_at(location, messages)))?;
        Ok(())
    }

    /// Records a standard `ia.<method>` invocation in history.
    pub fn record_history(&mut self, method: &str, command: String) -> ImageToolResult<()> {
        self.append_history_at(
            &format!("::image::{method}"),
            &[format!("Ran ia.{method}"), command],
        )
    }

    /// Returns the preserved casacore coordinate record.
    pub fn coordinate_record(&self) -> RecordValue {
        with_pixels!(self, storage => match storage {
            Storage::Paged(image) => match image.table().keywords().get("coords") {
                Some(Value::Record(record)) => record.clone(),
                _ => image.coordinates().to_record(),
            },
            Storage::Temp { record, .. } => record.clone(),
        })
    }

    /// Replaces the coordinate system from a casacore coordinate `record`.
    pub fn set_coordinate_record(&mut self, mut record: RecordValue) -> ImageToolResult<()> {
        if let Self::Float(Storage::Temp {
            image,
            record: current,
            ..
        }) = self
            && image.image_type_name() == "ComponentListImage"
        {
            let coordinates = CoordinateSystem::from_record(&record)?;
            image.set_source_coordinates(coordinates, &record)?;
            *current = record;
            return Ok(());
        }
        with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.set_coordinate_record(record).map_err(Into::into),
            Storage::Temp { image, record: current, .. } => {
                let coordinates = CoordinateSystem::from_record(&record)?;
                coordinates.normalize_quality_records(&mut record)?;
                image.set_coordinates(coordinates)?;
                *current = record;
                Ok(())
            },
        })
    }

    /// Rejects double-precision pixel types with CASA's standard error message.
    pub fn check_single_precision(&self, method: &str) -> ImageToolResult<()> {
        let name = match self.kind() {
            ImagePixelType::Float64 => "double",
            ImagePixelType::Complex64 => "complex<double>",
            _ => return Ok(()),
        };
        Err(ImageToolError::new(format!(
            "Exception: {method} does not support images with {name} precision pixel values."
        )))
    }

    /// Returns the brightness unit string.
    pub fn units(&self) -> &str {
        with_pixels!(self, storage => with_storage!(storage, image => image.units()))
    }

    /// Sets the brightness unit string.
    pub fn set_units(&mut self, unit: impl Into<String>) -> ImageToolResult<()> {
        let unit = unit.into();
        with_pixels!(self, storage => with_storage!(storage, image => image.set_units(unit)))?;
        Ok(())
    }

    /// Returns the `miscinfo` record.
    pub fn misc_info(&self) -> RecordValue {
        with_pixels!(self, storage => with_storage!(storage, image => image.misc_info()))
    }

    /// Replaces the `miscinfo` record.
    pub fn set_misc_info(&mut self, record: RecordValue) -> ImageToolResult<()> {
        with_pixels!(self, storage => with_storage!(storage, image => image.set_misc_info(record)))?;
        Ok(())
    }

    /// Replaces the `imageinfo` record.
    pub fn set_image_info_record(&mut self, record: RecordValue) -> ImageToolResult<()> {
        with_pixels!(self, storage => with_storage!(storage, image => image.set_image_info_record(record)))?;
        Ok(())
    }

    /// Returns formatted history strings.
    pub fn history(&self) -> ImageToolResult<Vec<String>> {
        Ok(with_pixels!(self, storage => with_storage!(storage, image => image.history()))?)
    }

    /// Acquires a storage lock of `kind` with up to `attempts` retries.
    pub fn lock_storage(&mut self, kind: LockType, attempts: u32) -> ImageToolResult<bool> {
        #[cfg(unix)]
        return Ok(with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.lock(kind, attempts)?,
            Storage::Temp { image, .. } => image
                .concat_source()
                .map_or(Ok(true), |source| source.lock(kind, attempts))?,
        }));
        #[cfg(not(unix))]
        {
            let _ = (kind, attempts);
            Ok(true)
        }
    }

    /// Returns whether this handle currently holds a storage lock of `kind`.
    pub fn has_storage_lock(&self, kind: LockType) -> bool {
        #[cfg(unix)]
        return with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.has_lock(kind),
            Storage::Temp { image, .. } => image
                .concat_source()
                .is_none_or(|source| source.has_lock(kind)),
        });
        #[cfg(not(unix))]
        {
            let _ = kind;
            true
        }
    }

    /// Releases the handle's storage lock.
    pub fn unlock_storage(&mut self) -> ImageToolResult<()> {
        #[cfg(unix)]
        {
            with_pixels!(self, storage => match storage {
                Storage::Paged(image) => image.unlock()?,
                Storage::Temp { image, .. } => image
                    .concat_source()
                    .map_or(Ok(()), |source| source.unlock())?,
            });
        }
        Ok(())
    }

    /// Releases a storage lock if another process has requested it.
    pub fn release_requested_lock(&mut self) -> ImageToolResult<()> {
        #[cfg(unix)]
        {
            with_pixels!(self, storage => match storage {
                Storage::Paged(image) => image.release_requested_lock()?,
                Storage::Temp { image, .. } => image
                    .concat_source()
                    .map_or(Ok(()), |source| source.release_requested_lock())?,
            });
        }
        Ok(())
    }
}

impl<T: ImagePixel> Storage<T> {
    /// Creates a new paged (when `path` is `Some`) or temporary image storage.
    pub fn create(
        shape: Vec<usize>,
        coordinates: CoordinateSystem,
        path: Option<&Path>,
        data: Option<&ArrayD<T>>,
    ) -> Result<Self, ImageError> {
        let mut storage = match path {
            Some(path) => Self::Paged(Box::new(PagedImage::create(shape, coordinates, path)?)),
            None => Self::Temp {
                fits: None,
                record: coordinates.to_record(),
                image: Box::new(TempImage::new(
                    shape,
                    coordinates,
                    TempStoragePolicy::Auto {
                        memory_budget_bytes: 256 * 1024 * 1024,
                        scratch: ScratchSpace::SystemTemp,
                    },
                )?),
            },
        };
        if let Some(data) = data {
            let origin = vec![0; data.ndim()];
            with_storage!(&mut storage, image => image.put_slice(data, &origin))?;
        }
        Ok(storage)
    }

    /// Flushes persistent changes when backed by a [`PagedImage`].
    pub fn save(&mut self) -> Result<(), ImageError> {
        match self {
            Self::Paged(image) => image.save(),
            Self::Temp { .. } => Ok(()),
        }
    }
}

impl Storage<f32> {
    /// Calls `use_image` with a concrete [`ImageInterface<f32>`] view, handling FITS attachments.
    pub fn with_float_view<R>(&self, use_image: impl FnOnce(&dyn ImageInterface<f32>) -> R) -> R {
        match self {
            Self::Paged(image) => use_image(image.as_ref()),
            Self::Temp {
                image,
                fits: Some(fits),
                ..
            } => use_image(&fits.image_view(image)),
            Self::Temp { image, .. } => use_image(image.as_ref()),
        }
    }
}
