// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel <-> world coordinate conversions for attached images (`ia.topixel`, `ia.toworld`, `ia.coordmeasures`).

use casa_images::{ImageInterface, ImagePixel};
use num_complex::Complex64;

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, with_storage};
use super::point::{PixelScalar, pixel_position, pixel_sample};

/// Intensity and mask sample returned alongside `ia.coordmeasures`.
#[derive(Debug, Clone)]
pub struct CoordMeasureIntensity {
    /// Pixel intensity scalar (`0.0` when `pixel` is out of bounds).
    pub value: PixelScalar,
    /// Brightness unit (`""` when `pixel` is out of bounds).
    pub unit: String,
    /// Optional pixel validity mask (`None` when `pixel` is out of bounds).
    pub mask: Option<bool>,
}

/// Samples single-precision intensity and mask for `ia.coordmeasures`.
pub fn coord_measures_intensity(
    attached: &AttachedImage,
    pixel: &[f64],
) -> ImageToolResult<CoordMeasureIntensity> {
    let image = attached.lock();
    image.check_single_precision("coordmeasures")?;
    let requested: Vec<i64> = pixel.iter().map(|&axis| axis as i64).collect();
    match &*image {
        ImageHandle::Float(storage) => {
            storage.with_float_view(|img| sample_intensity(img, requested, PixelScalar::Float))
        }
        ImageHandle::Complex(storage) => with_storage!(storage, img => {
            sample_intensity(img.as_ref(), requested, |v| {
                PixelScalar::DComplex(Complex64::new(f64::from(v.re), f64::from(v.im)))
            })
        }),
        _ => unreachable!("double precision was rejected"),
    }
}

fn sample_intensity<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    mut requested: Vec<i64>,
    wrap: impl Fn(T) -> PixelScalar,
) -> ImageToolResult<CoordMeasureIntensity> {
    if requested.is_empty() {
        requested = image
            .coordinates()
            .reference_pixel()
            .into_iter()
            .map(|pixel| pixel as i64)
            .collect();
    }
    match pixel_position(image.coordinates(), image.shape(), &requested) {
        Some(position) => {
            let (sample, mask) = pixel_sample(image, &position)?;
            Ok(CoordMeasureIntensity {
                value: wrap(sample),
                unit: image.units().to_owned(),
                mask: Some(mask),
            })
        }
        None => Ok(CoordMeasureIntensity {
            value: wrap(T::default_value()),
            unit: String::new(),
            mask: None,
        }),
    }
}

/// Converts pixel coordinates to world coordinates using the attached image's [`CoordinateSystem`].
pub fn to_world(
    attached: &AttachedImage,
    value: &[f64],
    _format: &str,
) -> ImageToolResult<Vec<f64>> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let coords = image.coordinates();
    let n_pix = coords.n_pixel_axes();
    let mut pix = if value.is_empty() {
        let mut refpix = vec![0.0; n_pix];
        for idx in 0..coords.n_coordinates() {
            let c = coords.coordinate(idx);
            let rp = c.reference_pixel();
            for (li, ax_opt) in coords
                .coordinate_pixel_axes(idx)
                .unwrap_or_default()
                .into_iter()
                .enumerate()
            {
                if let Some(ax) = ax_opt
                    && ax < n_pix
                {
                    refpix[ax] = rp.get(li).copied().unwrap_or(0.0);
                }
            }
        }
        refpix
    } else {
        if value.len() > n_pix {
            return Err(ImageToolError::new(format!(
                "Pixel vector length ({}) exceeds pixel axes ({n_pix}).",
                value.len()
            )));
        }
        let mut v = value.to_vec();
        v.resize(n_pix, 0.0);
        v
    };
    let _ = &mut pix;
    Ok(coords.to_world(&pix)?)
}

/// Converts world coordinates to pixel coordinates using the attached image's [`CoordinateSystem`].
pub fn to_pixel(attached: &AttachedImage, value: &[f64]) -> ImageToolResult<Vec<f64>> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let coords = image.coordinates();
    let n_world = coords.n_world_axes();
    if value.len() > n_world {
        return Err(ImageToolError::new(format!(
            "World vector length ({}) exceeds world axes ({n_world}).",
            value.len()
        )));
    }
    let mut world = value.to_vec();
    if world.len() < n_world {
        world.resize(n_world, 0.0);
    }
    Ok(coords.to_pixel(&world)?)
}
