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

/// Evaluates `ia.pixelvalue` at `pixel` coordinates (defaulting to reference pixel when empty).
pub fn pixel_value(attached: &AttachedImage, pixel: &[isize]) -> ImageToolResult<PixelValueResult> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let shape = image.shape().to_vec();
    let unit = image.units().to_owned();
    let ndim = shape.len();
    let coords = image.coordinates();

    let resolved: Vec<usize> = if pixel.is_empty() {
        let mut ref_pix = vec![0usize; ndim];
        for c_idx in 0..coords.n_coordinates() {
            let coord = coords.coordinate(c_idx);
            let rp = coord.reference_pixel();
            for (local_i, ax_opt) in coords
                .coordinate_pixel_axes(c_idx)
                .unwrap_or_default()
                .into_iter()
                .enumerate()
            {
                if let Some(ax) = ax_opt {
                    let rounded = rp.get(local_i).copied().unwrap_or(0.0).round() as isize;
                    ref_pix[ax] = rounded.clamp(0, shape[ax] as isize - 1) as usize;
                }
            }
        }
        ref_pix
    } else {
        let mut pos = Vec::with_capacity(ndim);
        for (axis, &dim) in shape.iter().enumerate() {
            let p = pixel.get(axis).copied().unwrap_or(0);
            if p < 0 || (p as usize) >= dim {
                return Err(ImageToolError::new(format!(
                    "Pixel coordinate {p} out of bounds on axis {axis} (length {dim}).",
                )));
            }
            pos.push(p as usize);
        }
        pos
    };

    let ones = vec![1usize; ndim];
    let (value, mask) = match &*image {
        ImageHandle::Float(storage) => {
            if let Some(fits) = image.fits() {
                let metadata = match storage {
                    Storage::Temp { image, .. } => image.as_ref(),
                    Storage::Paged(_) => unreachable!(),
                };
                let view = fits.image_view(metadata);
                let (v, m) = read_one(&view, &resolved, &ones)?;
                (PixelScalar::Float(v), m)
            } else {
                with_storage!(storage, img => {
                    let (v, m) = read_one(img.as_ref(), &resolved, &ones)?;
                    (PixelScalar::Float(v), m)
                })
            }
        }
        ImageHandle::Double(storage) => with_storage!(storage, img => {
            let (v, m) = read_one(img.as_ref(), &resolved, &ones)?;
            (PixelScalar::Double(v), m)
        }),
        ImageHandle::Complex(storage) => with_storage!(storage, img => {
            let (v, m) = read_one(img.as_ref(), &resolved, &ones)?;
            (PixelScalar::Complex(v), m)
        }),
        ImageHandle::DComplex(storage) => with_storage!(storage, img => {
            let (v, m) = read_one(img.as_ref(), &resolved, &ones)?;
            (PixelScalar::DComplex(v), m)
        }),
    };

    Ok(PixelValueResult {
        mask,
        value,
        unit,
        pixel: resolved.into_iter().map(|v| v as i64).collect(),
    })
}

fn read_one<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    start: &[usize],
    ones: &[usize],
) -> ImageToolResult<(T, bool)> {
    let vals = image.get_slice(start, ones, ones)?;
    let mask = image
        .get_mask_slice(start, ones, ones)?
        .and_then(|m| m.iter().next().copied())
        .unwrap_or(true);
    let val = vals
        .iter()
        .next()
        .copied()
        .ok_or_else(|| ImageToolError::new("Failed to read single pixel value."))?;
    Ok((val, mask))
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
