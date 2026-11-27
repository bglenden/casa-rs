// SPDX-License-Identifier: LGPL-3.0-or-later

//! Single-pixel value and mask inspection (`ia.pixelvalue`).

use casa_core::{Complex32, Complex64};
use casa_images::{ImageInterface, ImagePixel};

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_storage};

/// Scalar pixel value variants returned by `ia.pixelvalue`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PixelScalar {
    /// Single-precision real scalar.
    Float(f32),
    /// Double-precision real scalar.
    Double(f64),
    /// Single-precision complex scalar.
    Complex(Complex32),
    /// Double-precision complex scalar.
    DComplex(Complex64),
}

/// Complete `ia.pixelvalue` report for a pixel coordinate.
#[derive(Debug, Clone)]
pub struct PixelValueResult {
    /// Valid (`true`) or masked (`false`) pixel flag.
    pub mask: bool,
    /// Scalar pixel value.
    pub value: PixelScalar,
    /// Brightness unit string.
    pub unit: String,
    /// Integer pixel coordinates actually sampled.
    pub pixel: Vec<i64>,
}

/// Resolves CASA's `pixel` selector against `coordinates` and `shape`, returning `None` when out of bounds.
pub fn pixel_position(
    coordinates: &casa_coordinates::CoordinateSystem,
    shape: &[usize],
    requested: &[i64],
) -> Option<Vec<usize>> {
    let reference = coordinates.reference_pixel();
    let requested = if requested == [-1] {
        &[][..]
    } else {
        requested
    };
    shape
        .iter()
        .enumerate()
        .map(|(axis, &size)| {
            let pos = requested
                .get(axis)
                .copied()
                .unwrap_or_else(|| (reference.get(axis).copied().unwrap_or(0.0) + 0.5) as i64);
            usize::try_from(pos).ok().filter(|&pos| pos < size)
        })
        .collect()
}

/// Reads a single `(pixel, mask)` sample at exact 0-based `pixel` coordinates.
pub fn pixel_sample<T: ImagePixel>(
    image: &(impl ImageInterface<T> + ?Sized),
    pixel: &[usize],
) -> ImageToolResult<(T, bool)> {
    let one = vec![1; pixel.len()];
    let mask = image
        .get_mask_slice(pixel, &one, &one)?
        .and_then(|mask| mask.first().copied())
        .unwrap_or(true);
    let pixel_value = image.get_at(pixel)?;
    Ok((pixel_value, mask))
}

/// Evaluates CASA's `ia.pixelvalue(pixel)` returning `Ok(None)` when `pixel` is out of bounds.
pub fn pixel_value_casa(
    attached: &AttachedImage,
    pixel: &[f64],
) -> ImageToolResult<Option<PixelValueResult>> {
    let mut image = attached.lock();
    image.check_single_precision("pixelvalue")?;
    if image.coordinates().n_world_axes() == 0 {
        return Err(ImageToolError::new(
            "WCBox: Failed AlwaysAssert itsCSys.nWorldAxes() > 0",
        ));
    }
    let requested: Vec<i64> = pixel.iter().map(|p| *p as i64).collect();
    if pixel_position(image.coordinates(), image.shape(), &requested).is_none() {
        return Ok(None);
    }
    attached.require_pixel_read_lock(&mut image)?;
    if let ImageHandle::Float(Storage::Temp {
        image,
        fits: Some(fits),
        ..
    }) = &*image
    {
        let Some(pos) = pixel_position(image.coordinates(), &fits.shape, &requested) else {
            return Ok(None);
        };
        let ones = vec![1; pos.len()];
        let values = fits.read(&pos, &ones, &ones)?;
        let value = *values.first().expect("one FITS pixel requested");
        return Ok(Some(PixelValueResult {
            mask: !value.is_nan(),
            value: PixelScalar::Float(value),
            unit: image.units().to_owned(),
            pixel: pos.into_iter().map(|v| v as i64).collect(),
        }));
    }
    let coords = image.coordinates();
    let shape = image.shape();
    let Some(pos) = pixel_position(coords, shape, &requested) else {
        return Ok(None);
    };
    let result = match &*image {
        ImageHandle::Float(storage) => with_storage!(storage, img => {
            let (v, m) = pixel_sample(img.as_ref(), &pos)?;
            PixelValueResult {
                mask: m,
                value: PixelScalar::Float(v),
                unit: img.units().to_owned(),
                pixel: pos.into_iter().map(|v| v as i64).collect(),
            }
        }),
        ImageHandle::Complex(storage) => with_storage!(storage, img => {
            let (v, m) = pixel_sample(img.as_ref(), &pos)?;
            PixelValueResult {
                mask: m,
                value: PixelScalar::Complex(v),
                unit: img.units().to_owned(),
                pixel: pos.into_iter().map(|v| v as i64).collect(),
            }
        }),
        _ => unreachable!("double precision was rejected"),
    };
    image.release_requested_lock()?;
    Ok(Some(result))
}

/// Error returned when constructing a constant float pixel array (`ia.makearray`).
#[derive(Debug, Clone)]
pub enum MakeArrayError {
    /// Invalid dimension values (negative or non-finite).
    InvalidShape(ImageToolError),
    /// Requested element count or allocation exceeds available memory.
    OutOfMemory,
}

/// Allocates a column-major constant float array of `dims` filled with `value` (`ia.makearray`).
pub fn make_constant_array(
    value: f64,
    dims: &[f64],
) -> Result<(Vec<usize>, Vec<f64>), MakeArrayError> {
    let shape = if dims.is_empty() {
        vec![1]
    } else {
        dims.iter()
            .map(|&size| {
                if size.is_finite() && size >= 0.0 {
                    Ok(size as usize)
                } else {
                    Err(MakeArrayError::InvalidShape(ImageToolError::new(
                        "image array dimensions must be non-negative",
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let size = shape
        .iter()
        .try_fold(1_usize, |acc, &axis| acc.checked_mul(axis))
        .ok_or(MakeArrayError::OutOfMemory)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(size)
        .map_err(|_| MakeArrayError::OutOfMemory)?;
    values.resize(size, value);
    Ok((shape, values))
}
