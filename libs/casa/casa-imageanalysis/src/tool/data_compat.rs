// SPDX-License-Identifier: LGPL-3.0-or-later

//! Native-width I/O helpers backing `PyImageTool` data compatibility accessors.

use std::path::Path;

use casa_coordinates::{CoordinateType, FitsHeader, fits::try_to_fits_header};
use casa_core::{ArrayD, ArrayValue, RecordValue};
use casa_images::{ImageInterface, ImagePixelType};

use super::attachment::{self, AttachedImage};
use super::beams::image_info;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, with_pixels, with_storage};

macro_rules! native_array {
    ($handle:expr, $image:ident => $read:expr) => {
        match $handle {
            ImageHandle::Float(storage) => {
                with_storage!(storage, $image => $read.map(ArrayValue::Float32))
            }
            ImageHandle::Double(storage) => {
                with_storage!(storage, $image => $read.map(ArrayValue::Float64))
            }
            ImageHandle::Complex(storage) => {
                with_storage!(storage, $image => $read.map(ArrayValue::Complex32))
            }
            ImageHandle::DComplex(storage) => {
                with_storage!(storage, $image => $read.map(ArrayValue::Complex64))
            }
        }
    };
}

/// Opens a directory-backed native image directly using the attachment registry (`_data_open`).
pub fn open_data_image(path: &Path, writable: bool) -> ImageToolResult<AttachedImage> {
    if !path.is_dir() {
        return Err(ImageToolError::new(format!(
            "Cannot open native image {}",
            path.display()
        )));
    }
    let path = std::path::absolute(path)?;
    AttachedImage::open_data(&mut attachment::registry(), &path, writable)
}

/// Returns the image shape under lock (`_data_shape`).
pub fn data_shape(attached: &AttachedImage) -> Vec<usize> {
    attached.lock().shape().to_vec()
}

/// Returns the pixel type under lock (`_data_pixel_type`).
pub fn data_pixel_type(attached: &AttachedImage) -> ImagePixelType {
    attached.lock().kind()
}

/// Returns brightness units (`_data_units`).
pub fn data_units(attached: &AttachedImage) -> String {
    let image = attached.lock();
    with_pixels!(&*image, storage => with_storage!(storage, img => img.units().into()))
}

/// Returns the `imageinfo` record (`_data_image_info`).
pub fn data_image_info(attached: &AttachedImage) -> ImageToolResult<RecordValue> {
    let image = attached.lock();
    Ok(image_info(&image)?.to_record())
}

/// Returns the `miscinfo` record (`_data_misc_info`).
pub fn data_misc_info(attached: &AttachedImage) -> RecordValue {
    let image = attached.lock();
    with_pixels!(&*image, storage => with_storage!(storage, img => img.misc_info()))
}

/// Returns the coordinate system record (`_data_coordinate_system`).
pub fn data_coordinate_system(attached: &AttachedImage) -> RecordValue {
    attached.lock().coordinates().to_record()
}

/// Computes direction WCS axes `(lon_axis, lat_axis)` and FITS header if the image has a direction axis matching pixel rank.
pub fn data_direction_wcs(
    attached: &AttachedImage,
) -> ImageToolResult<Option<(usize, usize, FitsHeader)>> {
    let image = attached.lock();
    let longitude = with_pixels!(
        &*image,
        storage => with_storage!(storage, img => img.find_axis(CoordinateType::Direction))
    );
    let coordinates = image.coordinates();
    let shape = image.shape();
    let Some(longitude) = longitude.filter(|_| coordinates.n_pixel_axes() == shape.len()) else {
        return Ok(None);
    };
    let header = try_to_fits_header(coordinates, shape)?;
    Ok(Some((longitude, longitude + 1, header)))
}

/// Returns all stored pixel mask names (`_data_mask_names`).
pub fn data_mask_names(attached: &AttachedImage) -> Vec<String> {
    let image = attached.lock();
    with_pixels!(&*image, storage => with_storage!(storage, img => img.mask_names()))
}

/// Returns the default pixel mask name if any (`_data_default_mask_name`).
pub fn data_default_mask_name(attached: &AttachedImage) -> Option<String> {
    let image = attached.lock();
    with_pixels!(&*image, storage => with_storage!(storage, img => img.default_mask_name()))
}

/// Reads a strided native-width pixel slice (`_data_slice`).
pub fn data_slice(
    attached: &AttachedImage,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> ImageToolResult<ArrayValue> {
    let image = attached.lock();
    Ok(native_array!(&*image, img => img.get_slice_with_stride(start, shape, stride))?)
}

/// Reads a single 2D/N-1D plane along `axis` at `index` (`_data_plane`).
pub fn data_plane(
    attached: &AttachedImage,
    axis: usize,
    index: usize,
) -> ImageToolResult<ArrayValue> {
    let image = attached.lock();
    Ok(native_array!(&*image, img => img.get_plane(axis, index))?)
}

/// Reads a strided boolean mask slice (`_data_mask_slice`).
pub fn data_mask_slice(
    attached: &AttachedImage,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> ImageToolResult<Option<ArrayD<bool>>> {
    let image = attached.lock();
    Ok(with_pixels!(
        &*image,
        storage => with_storage!(storage, img => ImageInterface::get_mask_slice(img.as_ref(), start, shape, stride))
    )?)
}

/// Writes a typed pixel array into `attached` starting at `start` and saves (`_data_put_slice`).
pub fn data_put_slice(
    attached: &AttachedImage,
    data: ArrayValue,
    start: &[usize],
) -> ImageToolResult<()> {
    attached.check_directory()?;
    let mut image = attached.lock();
    match (&mut *image, data) {
        (ImageHandle::Float(storage), ArrayValue::Float32(values)) => {
            with_storage!(storage, img => img.put_slice(&values, start))
        }
        (ImageHandle::Double(storage), ArrayValue::Float64(values)) => {
            with_storage!(storage, img => img.put_slice(&values, start))
        }
        (ImageHandle::Complex(storage), ArrayValue::Complex32(values)) => {
            with_storage!(storage, img => img.put_slice(&values, start))
        }
        (ImageHandle::DComplex(storage), ArrayValue::Complex64(values)) => {
            with_storage!(storage, img => img.put_slice(&values, start))
        }
        _ => {
            return Err(ImageToolError::new(
                "Image pixel type changed before the write",
            ));
        }
    }?;
    image.save()?;
    Ok(())
}

/// Normalizes an optional stride vector against `shape` rank and positivity rules.
pub fn normalize_stride(
    shape: &[usize],
    stride: Option<Vec<usize>>,
) -> ImageToolResult<Vec<usize>> {
    let stride = stride.unwrap_or_else(|| vec![1; shape.len()]);
    if stride.len() != shape.len() {
        return Err(ImageToolError::new(format!(
            "stride rank {} does not match shape rank {}",
            stride.len(),
            shape.len()
        )));
    }
    if stride.contains(&0) {
        return Err(ImageToolError::new("stride values must be >= 1"));
    }
    Ok(stride)
}
