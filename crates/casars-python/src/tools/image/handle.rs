// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_images::{
    AnyPagedImage, ImageError, ImagePixel, ImagePixelType, PagedImage, ScratchSpace, TempImage,
    TempStoragePolicy,
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
    },
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
        Ok(match AnyPagedImage::open(path)? {
            AnyPagedImage::Float32(image) => Self::Float(Storage::Paged(Box::new(image))),
            AnyPagedImage::Float64(image) => Self::Double(Storage::Paged(Box::new(image))),
            AnyPagedImage::Complex32(image) => Self::Complex(Storage::Paged(Box::new(image))),
            AnyPagedImage::Complex64(image) => Self::DComplex(Storage::Paged(Box::new(image))),
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
        with_pixels!(self, storage => with_storage!(storage, image => image.shape()))
    }

    pub(super) fn path(&self) -> Option<&Path> {
        with_pixels!(self, storage => with_storage!(storage, image => image.name()))
    }

    pub(super) fn save(&mut self) -> Result<(), ImageError> {
        with_pixels!(self, storage => storage.save())
    }

    pub(super) fn add_history(&mut self, message: String) -> PyResult<()> {
        with_pixels!(self, storage => with_storage!(storage, image => image.add_history(message)))
            .map_err(raised)
    }

    pub(super) fn record_history(&mut self, method: &str, command: String) -> PyResult<()> {
        self.add_history(format!("Ran ia.{method}"))?;
        self.add_history(command)
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
            Storage::Temp { image, record: current } => {
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
