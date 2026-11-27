// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel <-> world coordinate conversions for attached images (`ia.topixel`, `ia.toworld`).

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult};

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
