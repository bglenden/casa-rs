// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::OnceCell;
use std::fs::Metadata;
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_images::{
    AnyPagedImage, ImageError, ImagePixel, ImagePixelType, PagedImage, ScratchSpace, TempImage,
    TempStoragePolicy,
    fits_read::{FitsSectionReader, FitsToolImport, fits_section_image},
    miriad_read::MiriadImage,
};
use casa_types::{RecordValue, Value};
use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

pub(super) enum Storage<T: ImagePixel> {
    Paged(Box<PagedImage<T>>),
    Temp {
        image: Box<TempImage<T>>,
        record: RecordValue,
        fits: Option<FitsAttachment>,
    },
}

pub(super) struct FitsAttachment {
    path: PathBuf,
    hdu: usize,
    pub(super) shape: Vec<usize>,
    pub(super) tile_shape: Vec<usize>,
    pub(super) has_mask: bool,
    pixels: OnceCell<FitsSectionReader>,
}

impl FitsAttachment {
    pub(super) fn image_view<'a>(
        &'a self,
        metadata: &'a TempImage<f32>,
    ) -> impl casa_images::ImageInterface<f32> + 'a {
        fits_section_image(&self.shape, metadata, &self.path, |start, count, stride| {
            self.read_pixels(start, count, stride)
        })
    }

    pub(super) fn read(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> PyResult<ArrayD<f32>> {
        self.read_pixels(start, count, stride).map_err(raised)
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

pub(super) enum ImageHandle {
    Float(Storage<f32>),
    Double(Storage<f64>),
    Complex(Storage<Complex32>),
    DComplex(Storage<Complex64>),
}

// Each arm retains the concrete pixel type for calls into the image library.
macro_rules! with_pixels {
    ($image:expr, $storage:ident => $work:expr) => {
        match $image {
            ImageHandle::Float($storage) => $work,
            ImageHandle::Double($storage) => $work,
            ImageHandle::Complex($storage) => $work,
            ImageHandle::DComplex($storage) => $work,
        }
    };
}
pub(super) use with_pixels;

macro_rules! with_storage {
    ($storage:expr, $image:ident => $work:expr) => {
        match $storage {
            Storage::Paged($image) => $work,
            Storage::Temp { image: $image, .. } => $work,
        }
    };
}
pub(super) use with_storage;

pub(super) fn raised(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

pub(super) fn same_file_identity(expected: &Metadata, actual: &Metadata) -> bool {
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

pub(super) fn same_file_snapshot(expected: &Metadata, actual: &Metadata) -> bool {
    same_file_identity(expected, actual)
        && expected.len() == actual.len()
        && expected.modified().ok() == actual.modified().ok()
}

impl ImageHandle {
    pub(super) fn open(path: &Path) -> Result<Self, ImageError> {
        if path.is_file() {
            return Self::open_fits(path, FitsToolImport::open_attachment(path)?);
        }
        Self::open_native(path, None)
    }

    pub(super) fn open_read(path: &Path) -> Result<Self, ImageError> {
        if path.is_file() {
            return Self::open(path);
        }
        Self::open_native(path, Some(0))
    }

    pub(super) fn open_native(path: &Path, lock_attempts: Option<u32>) -> Result<Self, ImageError> {
        Self::open_native_with_cache(path, lock_attempts, true)
    }

    pub(super) fn open_native_with_cache(
        path: &Path,
        lock_attempts: Option<u32>,
        component_cache: bool,
    ) -> Result<Self, ImageError> {
        if casa_images::concat_file::is_image_concat(path) {
            return super::concatenate::virtual_image::open_handle(path);
        }
        if casa_images::ComponentListImage::recognizes_native(path) {
            let (source, record) =
                casa_images::ComponentListImage::open_native(path, component_cache)?;
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

    pub(super) fn from_component(
        source: casa_images::ComponentListImage,
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

    pub(super) fn disable_component_cache(&mut self) -> Result<(), ImageError> {
        if let Self::Float(Storage::Temp { image, .. }) = self {
            image.set_component_cache(false)?;
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

    pub(super) fn open_for_concatenation(path: &Path) -> Result<Self, ImageError> {
        if path.is_file() {
            return Self::open_fits(path, FitsToolImport::open_for_concatenation(path)?);
        }
        if Self::recognizes_miriad(path, path) {
            return Self::open_miriad(path);
        }
        Self::open_native(path, Some(0))
    }

    pub(super) fn open_fits(path: &Path, mut source: FitsToolImport) -> Result<Self, ImageError> {
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

    pub(super) fn fits(&self) -> Option<&FitsAttachment> {
        match self {
            Self::Float(Storage::Temp { fits, .. }) => fits.as_ref(),
            _ => None,
        }
    }

    pub(super) fn recognizes_miriad(probe: &Path, resolved: &Path) -> bool {
        MiriadImage::recognizes_input(probe, resolved)
    }

    pub(super) fn has_native_control(path: &Path) -> bool {
        path.join("table.dat").is_file() && !Self::recognizes_miriad(path, path)
    }

    pub(super) fn open_miriad(path: &Path) -> Result<Self, ImageError> {
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

    pub(super) fn image_type_name(&self) -> &'static str {
        if self.fits().is_some() {
            "FITSImage"
        } else {
            with_pixels!(self, storage => with_storage!(storage, image => image.image_type_name()))
        }
    }

    pub(super) fn read_only_file_type(&self) -> Option<&'static str> {
        let kind = self.image_type_name();
        (matches!(kind, "FITSImage" | "MIRIADImage") || self.is_saved_expression()).then_some(kind)
    }

    pub(super) fn is_saved_expression(&self) -> bool {
        self.image_type_name() == "ImageExpr" && self.path().is_some()
    }

    pub(super) fn concat_expression_operands(&self) -> Vec<PathBuf> {
        with_pixels!(self, storage => match storage {
            Storage::Temp { image, .. } => image.concat_source()
                .map_or_else(Vec::new, |source| source.expression_operands().cloned().collect()),
            Storage::Paged(_) => Vec::new(),
        })
    }

    pub(super) fn concat_operands(&self) -> Vec<PathBuf> {
        with_pixels!(self, storage => match storage {
            Storage::Temp { image, .. } => image.concat_source()
                .map_or_else(Vec::new, |source| source.operand_paths().map(Path::to_path_buf).collect()),
            Storage::Paged(_) => Vec::new(),
        })
    }

    pub(super) fn kind(&self) -> ImagePixelType {
        match self {
            Self::Float(_) => ImagePixelType::Float32,
            Self::Double(_) => ImagePixelType::Float64,
            Self::Complex(_) => ImagePixelType::Complex32,
            Self::DComplex(_) => ImagePixelType::Complex64,
        }
    }

    pub(super) fn shape(&self) -> &[usize] {
        if let Some(fits) = self.fits() {
            return &fits.shape;
        }
        with_pixels!(self, storage => with_storage!(storage, image => image.shape()))
    }

    pub(super) fn coordinates(&self) -> &CoordinateSystem {
        with_pixels!(self, storage => with_storage!(storage, image => image.coordinates()))
    }

    pub(super) fn path(&self) -> Option<&Path> {
        if let Some(fits) = self.fits() {
            return Some(&fits.path);
        }
        with_pixels!(self, storage => with_storage!(storage, image => image.name()))
    }

    pub(super) fn save(&mut self) -> Result<(), ImageError> {
        with_pixels!(self, storage => storage.save())
    }

    pub(super) fn history_entries(&self) -> PyResult<Vec<casa_images::image::ImageHistoryRow>> {
        with_pixels!(self, storage => match storage {
            Storage::Paged(image) => casa_images::image::image_history_rows(image.name().expect("native image path")).map_err(raised),
            Storage::Temp { image, .. } => Ok(image.history_rows().map_err(raised)?.into_owned()),
        })
    }

    pub(super) fn append_history_entries(
        &mut self,
        entries: &[casa_images::image::ImageHistoryRow],
    ) -> PyResult<()> {
        with_pixels!(self, storage => with_storage!(storage, image => image.append_history_entries(entries))).map_err(raised)
    }

    pub(super) fn append_history_at(
        &mut self,
        location: &str,
        messages: &[String],
    ) -> PyResult<()> {
        with_pixels!(self, storage => with_storage!(storage, image => image.append_history_at(location, messages)))
            .map_err(raised)
    }

    pub(super) fn record_history(&mut self, method: &str, command: String) -> PyResult<()> {
        self.append_history_at(
            &format!("::image::{method}"),
            &[format!("Ran ia.{method}"), command],
        )
    }

    pub(super) fn coordinate_record(&self) -> RecordValue {
        with_pixels!(self, storage => match storage {
            Storage::Paged(image) => match image.table().keywords().get("coords") {
                Some(Value::Record(record)) => record.clone(),
                _ => image.coordinates().to_record(),
            },
            Storage::Temp { record, .. } => record.clone(),
        })
    }

    pub(super) fn set_coordinate_record(&mut self, mut record: RecordValue) -> PyResult<()> {
        if let Self::Float(Storage::Temp {
            image,
            record: current,
            ..
        }) = self
            && image.image_type_name() == "ComponentListImage"
        {
            let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
            image
                .set_component_coordinates(coordinates, &record)
                .map_err(raised)?;
            *current = record;
            return Ok(());
        }
        with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.set_coordinate_record(record).map_err(raised),
            Storage::Temp { image, record: current, .. } => {
                let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
                coordinates.normalize_quality_records(&mut record).map_err(raised)?;
                image.set_coordinates(coordinates).map_err(raised)?;
                *current = record;
                Ok(())
            },
        })
    }

    pub(super) fn check_single_precision(&self, method: &str) -> PyResult<()> {
        let name = match self.kind() {
            ImagePixelType::Float64 => "double",
            ImagePixelType::Complex64 => "complex<double>",
            _ => return Ok(()),
        };
        Err(raised(format!(
            "Exception: {method} does not support images with {name} precision pixel values."
        )))
    }
}

impl<T: ImagePixel> Storage<T> {
    pub(super) fn create(
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

    pub(super) fn save(&mut self) -> Result<(), ImageError> {
        match self {
            Self::Paged(image) => image.save(),
            Self::Temp { .. } => Ok(()),
        }
    }
}

impl Storage<f32> {
    pub(super) fn with_float_view<R>(
        &self,
        use_image: impl FnOnce(&dyn casa_images::ImageInterface<f32>) -> R,
    ) -> R {
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
