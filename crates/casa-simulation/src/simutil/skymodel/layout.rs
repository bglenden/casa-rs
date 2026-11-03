// SPDX-License-Identifier: LGPL-3.0-or-later
//! What `simutil` reads of an image's layout without its pixels: `is4d`,
//! whether a sky model has CASA's four axes in order, and `spectral`, its
//! channel count and width.

use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_images::AnyPagedImage;

use super::{SkyModelError, pixel_axes};
use crate::simutil::python_float;

/// An image's shape and coordinate system: a CASA image (a directory) or a
/// FITS file (a file), as `ia.open` opens either.
fn layout(path: &Path) -> Result<(Vec<usize>, CoordinateSystem), SkyModelError> {
    if path.is_file() {
        // casa-images reads a FITS file whole; sky models are small.
        let fits = casa_images::read_fits_image(path)?;
        return Ok((fits.shape, fits.coordinates));
    }
    let image = AnyPagedImage::open(path)?;
    let coordinates = match &image {
        AnyPagedImage::Float32(image) => image.coordinates().clone(),
        AnyPagedImage::Float64(image) => image.coordinates().clone(),
        AnyPagedImage::Complex32(image) => image.coordinates().clone(),
        AnyPagedImage::Complex64(image) => image.coordinates().clone(),
    };
    Ok((image.shape().to_vec(), coordinates))
}

/// `simutil.is4d(image)`: whether the image has four axes, direction on
/// pixel axes 0 and 1, Stokes on 2 and spectral on 3.
///
/// # Errors
///
/// An image that cannot be opened.
pub fn is_4d(path: &Path) -> Result<bool, SkyModelError> {
    let (shape, coordinates) = layout(path)?;
    if shape.len() != 4 {
        return Ok(false);
    }
    let axes = |kind| pixel_axes(&coordinates, kind).map(|(_, axes)| axes);
    Ok(
        axes(CoordinateType::Direction).as_deref() == Some(&[0, 1][..])
            && axes(CoordinateType::Stokes).as_deref() == Some(&[2][..])
            && axes(CoordinateType::Spectral).as_deref() == Some(&[3][..]),
    )
}

/// What `simutil.spectral(image)` gives: the spectral axis's length and its
/// increment as CASA writes it, Python's `str` of the number then the unit
/// (`2000000.0Hz`).
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralSize {
    /// The number of channels.
    pub channels: usize,
    /// The channel width, as CASA's string.
    pub width: String,
}

/// `simutil.spectral(image)`: the image's spectral size, or `None` when it
/// has no spectral coordinate (CASA's `0.0`).
///
/// # Errors
///
/// An image that cannot be opened.
pub fn spectral_size(path: &Path) -> Result<Option<SpectralSize>, SkyModelError> {
    let (shape, coordinates) = layout(path)?;
    let Some((index, axes)) = pixel_axes(&coordinates, CoordinateType::Spectral) else {
        return Ok(None);
    };
    let coordinate = coordinates.coordinate(index);
    let increment = coordinate.increment()[0];
    let unit = coordinate.axis_units()[0].clone();
    Ok(Some(SpectralSize {
        channels: shape[axes[0]],
        width: format!("{}{unit}", python_float(increment)),
    }))
}
