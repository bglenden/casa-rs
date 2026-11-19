// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::OnceCell;
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_images::{
    AnyPagedImage, ImageError, ImagePixel, ImagePixelType, PagedImage, ScratchSpace, TempImage,
    TempStoragePolicy,
    fits_read::{FitsSectionReader, FitsToolImport, fits_section_image},
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

impl ImageHandle {
    pub(super) fn open(path: &Path) -> Result<Self, ImageError> {
        if path.is_file() {
            return Self::open_fits(path, FitsToolImport::open(path, 0, -1)?);
        }
        AnyPagedImage::open(path).map(Self::from_paged)
    }

    pub(super) fn open_native(path: &Path, lock_attempts: Option<u32>) -> Result<Self, ImageError> {
        #[cfg(unix)]
        if let Some(attempts) = lock_attempts {
            return AnyPagedImage::open_with_lock(path, attempts).map(Self::from_paged);
        }
        #[cfg(not(unix))]
        let _ = lock_attempts;
        Self::open(path)
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
        Self::open(path)
    }

    fn open_fits(path: &Path, mut source: FitsToolImport) -> Result<Self, ImageError> {
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

    pub(super) fn set_coordinate_record(&mut self, record: RecordValue) -> PyResult<()> {
        with_pixels!(self, storage => match storage {
            Storage::Paged(image) => image.set_coordinate_record(record).map_err(raised),
            Storage::Temp { image, record: current, .. } => {
                let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
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
