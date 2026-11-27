// SPDX-License-Identifier: LGPL-3.0-or-later

//! Samples along a polyline in an image's pixel plane.

use crate::resample::{Plane, RegridMethod, RegridPixel};
use crate::{ImageError, ImageInterface};

/// Samples, valid-mask flags and positions along a pixel-space polyline.
#[derive(Debug)]
pub struct PixelSlice<T> {
    /// Interpolated samples. Invalid samples have the pixel type's zero value.
    pub pixels: Vec<T>,
    /// True for samples whose interpolation stencil is valid.
    pub mask: Vec<bool>,
    /// Sample positions as `[x, y]`, rounded to the reference tool's Float precision.
    pub positions: Vec<[f32; 2]>,
    /// Cumulative straight-line distances between consecutive output positions.
    pub distance: Vec<f32>,
}

fn failed(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}

fn positions(x: &[f64], y: &[f64], npts: usize) -> Result<Vec<[f32; 2]>, ImageError> {
    if x.len() != y.len() {
        return Err(failed("Failed AlwaysAssert x.nelements() == y.nelements()"));
    }
    if x.len() < 2 {
        return Err(failed("Failed AlwaysAssert x.nelements() >= 2"));
    }

    let lengths: Vec<_> = x
        .windows(2)
        .zip(y.windows(2))
        .map(|(x, y)| (x[1] - x[0]).hypot(y[1] - y[0]))
        .collect();
    let length = lengths.iter().sum::<f64>();
    if !length.is_finite() || x.iter().chain(y).any(|value| !(*value as f32).is_finite()) {
        return Err(failed("Slice positions must be finite"));
    }
    let count = if npts == 0 {
        ((length + 0.1) as usize).saturating_add(1)
    } else {
        npts
    };
    if length == 0.0 {
        return if count == 1 {
            Ok(vec![[x[0] as f32, y[0] as f32]])
        } else {
            Err(failed("Failed AlwaysAssert np == npoints"))
        };
    }

    let mut output = Vec::new();
    output
        .try_reserve_exact(count)
        .map_err(|_| failed("Slice has too many sample positions"))?;
    let step = length / (count - 1) as f64;
    let mut segment = 0;
    let mut start = 0.0;
    for sample in 0..count {
        let at = if sample == 0 {
            0.0
        } else {
            step * sample as f64
        };
        while segment < lengths.len() - 1 && at >= start + lengths[segment] {
            start += lengths[segment];
            segment += 1;
        }
        let span = lengths[segment];
        let fraction = if span > 0.0 {
            ((at - start) / span).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let px = x[segment] + fraction * (x[segment + 1] - x[segment]);
        let py = y[segment] + fraction * (y[segment + 1] - y[segment]);
        output.push([px as f32, py as f32]);
    }
    Ok(output)
}

/// Read only the selected image plane and interpolate along its pixel polyline.
///
/// The interpolation stencils and mask semantics match `Interpolate2D`.
/// Plane storage follows ascending image-axis order, as in casacore's
/// degenerate-axis-dropping lattice slice. The endpoint domain checks use
/// the original order requested by the caller.
///
/// # Errors
///
/// Returns an error for an invalid path, axis, or fixed coordinate, or if
/// the source cannot provide the requested plane.
#[allow(private_bounds)]
pub fn get_slice<T: RegridPixel>(
    image: &(impl ImageInterface<T> + ?Sized),
    x: &[f64],
    y: &[f64],
    axes: [i64; 2],
    coord: &[i64],
    npts: usize,
    method: RegridMethod,
) -> Result<PixelSlice<T>, ImageError> {
    let positions = positions(x, y, npts)?;
    let shape = image.shape();
    let axis_index = |requested, name| {
        usize::try_from(requested)
            .ok()
            .filter(|&axis| axis < shape.len())
            .ok_or_else(|| failed(format!("Failed AlwaysAssert {name}<itsLatticePtr->ndim()")))
    };
    let axes = [axis_index(axes[0], "axis0")?, axis_index(axes[1], "axis1")?];
    if coord != [-1] && coord.len() != shape.len() {
        return Err(failed("coord must be of length number of image dimensions"));
    }
    let [first_x, first_y] = positions[0];
    if first_x < -0.5 || first_y < -0.5 {
        return Err(failed("x or y start of curve falls outside of lattice"));
    }
    let [last_x, last_y] = *positions.last().expect("at least one position");
    if f64::from(last_x) > shape[axes[0]] as f64 - 0.5
        || f64::from(last_y) > shape[axes[1]] as f64 - 0.5
    {
        return Err(failed("x or y end of curve falls outside of lattice"));
    }

    let mut start = vec![0; shape.len()];
    let mut count = vec![1; shape.len()];
    for (axis, &length) in shape.iter().enumerate() {
        if axes.contains(&axis) {
            count[axis] = length;
        } else if coord != [-1] {
            let requested = coord[axis];
            let requested = if requested < 0 {
                requested + length as i64
            } else {
                requested
            };
            start[axis] = usize::try_from(requested)
                .ok()
                .filter(|&index| index < length)
                .ok_or_else(|| {
                    failed(format!(
                        "slice coordinate on axis {axis} falls outside of lattice"
                    ))
                })?;
        }
    }
    let step = vec![1; shape.len()];
    let data = image.get_slice(&start, &count, &step)?;
    let mask = image
        .get_mask_slice(&start, &count, &step)?
        .map(|mask| mask.reversed_axes().iter().copied().collect::<Vec<_>>());

    let mut dimensions = count.into_iter().filter(|&length| length != 1);
    let shape = [
        dimensions.next().unwrap_or(1),
        dimensions.next().unwrap_or(1),
    ];
    let data = data.reversed_axes();
    let components: Vec<Vec<f64>> = (0..T::COMPONENTS)
        .map(|component| {
            data.iter()
                .map(|pixel| pixel.component(component))
                .collect()
        })
        .collect();
    let planes: Vec<_> = components
        .iter()
        .map(|data| Plane {
            data,
            mask: mask.as_deref(),
            shape,
        })
        .collect();

    let mut samples = Vec::with_capacity(positions.len());
    let mut valid = Vec::with_capacity(positions.len());
    for &position in &positions {
        let at = position.map(f64::from);
        let mut pixel = T::default_value();
        let mut good = true;
        for (component, plane) in planes.iter().enumerate() {
            let Some(value) = plane.sample(method, at) else {
                good = false;
                break;
            };
            pixel = pixel.with_component(component, value);
        }
        samples.push(pixel);
        valid.push(good);
    }
    let mut distance = vec![0.0; positions.len()];
    for (index, pair) in positions.windows(2).enumerate() {
        let dx = pair[1][0] - pair[0][0];
        let dy = pair[1][1] - pair[0][1];
        distance[index + 1] = distance[index] + (dx * dx + dy * dy).sqrt();
    }
    Ok(PixelSlice {
        pixels: samples,
        mask: valid,
        positions,
        distance,
    })
}
