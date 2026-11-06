// SPDX-License-Identifier: LGPL-3.0-or-later
//! The real-valued, mask-aware extrema needed by CASA's image header.

use std::path::Path;

use casa_lattices::{Lattice, StatsElement, TraversalCursorIter, TraversalSpec};
use ndarray::{ArrayD, Dimension};

use super::ImageExtrema;
use crate::{AnyPagedImage, ImageError, ImagePixel, PagedImage};

/// Read only bounded pixel and mask chunks to find an image's extrema.
///
/// CASA reports no extrema for complex images or wholly masked images.
/// The chunk buffers are capped at 1,048,576 pixels each; the table storage
/// manager can additionally retain a complete physical tile in its cache.
pub fn imhead_extrema(path: impl AsRef<Path>) -> Result<Option<ImageExtrema>, ImageError> {
    match AnyPagedImage::open(path)? {
        AnyPagedImage::Float32(image) => extrema(&image),
        AnyPagedImage::Float64(image) => extrema(&image),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Ok(None),
    }
}

#[derive(Clone)]
struct Located {
    value: f64,
    linear: usize,
    position: Vec<usize>,
}

fn fortran_slice<T>(array: &ArrayD<T>) -> Option<&[T]> {
    let mut stride = 1isize;
    for (&length, &actual) in array.shape().iter().zip(array.strides()) {
        if length > 1 && actual != stride {
            return None;
        }
        stride = stride.checked_mul(isize::try_from(length).ok()?)?;
    }
    array.as_slice_memory_order()
}

/// Find each chunk's first, minimum and maximum without constructing a
/// multidimensional index for every pixel. Slices use casacore axis order.
fn chunk_candidates<T: StatsElement>(
    values: &[T],
    mask: Option<&[bool]>,
) -> [Option<(f64, usize)>; 3] {
    let mut first = None;
    let mut minimum: Option<(f64, usize)> = None;
    let mut maximum: Option<(f64, usize)> = None;
    for (index, value) in values.iter().enumerate() {
        if mask.is_some_and(|mask| !mask[index]) {
            continue;
        }
        let value = value.to_f64_stats();
        if first.is_none() {
            first = Some((value, index));
        }
        if !value.is_nan() {
            if minimum.is_none_or(|(current, _)| value < current) {
                minimum = Some((value, index));
            }
            if maximum.is_none_or(|(current, _)| value > current) {
                maximum = Some((value, index));
            }
        }
    }
    [first, minimum, maximum]
}

fn locate(candidate: (f64, usize), start: &[usize], shape: &[usize], strides: &[usize]) -> Located {
    let (value, mut index) = candidate;
    let mut linear = 0;
    let mut position = Vec::with_capacity(shape.len());
    for ((&start, &length), &stride) in start.iter().zip(shape).zip(strides) {
        let coordinate = start + index % length;
        index /= length;
        linear += coordinate * stride;
        position.push(coordinate);
    }
    Located {
        value,
        linear,
        position,
    }
}

fn merge_candidates(
    candidates: [Option<Located>; 3],
    first: &mut Option<Located>,
    minimum: &mut Option<Located>,
    maximum: &mut Option<Located>,
) {
    let [new_first, new_min, new_max] = candidates;
    if let Some(value) = new_first
        && first.as_ref().is_none_or(|v| value.linear < v.linear)
    {
        *first = Some(value);
    }
    if let Some(value) = new_min
        && minimum.as_ref().is_none_or(|v| {
            value.value < v.value || (value.value == v.value && value.linear < v.linear)
        })
    {
        *minimum = Some(value);
    }
    if let Some(value) = new_max
        && maximum.as_ref().is_none_or(|v| {
            value.value > v.value || (value.value == v.value && value.linear < v.linear)
        })
    {
        *maximum = Some(value);
    }
}

fn extrema<T: ImagePixel + StatsElement>(
    image: &PagedImage<T>,
) -> Result<Option<ImageExtrema>, ImageError> {
    let shape = image.shape();
    let mut strides = Vec::with_capacity(shape.len());
    let mut product = 1usize;
    for &length in shape {
        strides.push(product);
        product = product.checked_mul(length).ok_or_else(|| {
            ImageError::InvalidMetadata("image shape overflows addressable pixels".into())
        })?;
    }
    let chunk_shape = bounded_cursor_shape(image.nice_cursor_shape(), shape, 1_048_576);
    let cursors = TraversalCursorIter::new(
        shape.to_vec(),
        chunk_shape.clone(),
        TraversalSpec::chunks(chunk_shape),
    );
    let mut first: Option<Located> = None;
    let mut minimum: Option<Located> = None;
    let mut maximum: Option<Located> = None;
    let step = vec![1; shape.len()];
    for cursor in cursors {
        let cursor = cursor?;
        let values = image.get_slice(&cursor.position, &cursor.shape)?;
        let mask = image.get_mask_slice(&cursor.position, &cursor.shape, &step)?;
        if let Some(pixels) = fortran_slice(&values) {
            let mask_slice = mask.as_ref().and_then(fortran_slice);
            if mask.is_none() || mask_slice.is_some() {
                let candidates = chunk_candidates(pixels, mask_slice).map(|candidate| {
                    candidate.map(|candidate| {
                        locate(candidate, &cursor.position, &cursor.shape, &strides)
                    })
                });
                merge_candidates(candidates, &mut first, &mut minimum, &mut maximum);
                continue;
            }
        }
        for (index, pixel) in values.indexed_iter() {
            if mask.as_ref().is_some_and(|mask| !mask[index.clone()]) {
                continue;
            }
            let linear = index
                .slice()
                .iter()
                .zip(&cursor.position)
                .zip(&strides)
                .map(|((&offset, &start), &stride)| (offset + start) * stride)
                .sum();
            let value = pixel.to_f64_stats();
            let make = || Located {
                value,
                linear,
                position: index
                    .slice()
                    .iter()
                    .zip(&cursor.position)
                    .map(|(&offset, &start)| offset + start)
                    .collect(),
            };
            if first.as_ref().is_none_or(|v| linear < v.linear) {
                first = Some(make());
            }
            if !value.is_nan() {
                if minimum
                    .as_ref()
                    .is_none_or(|v| value < v.value || (value == v.value && linear < v.linear))
                {
                    minimum = Some(make());
                }
                if maximum
                    .as_ref()
                    .is_none_or(|v| value > v.value || (value == v.value && linear < v.linear))
                {
                    maximum = Some(make());
                }
            }
        }
    }
    let Some(first) = first else {
        return Ok(None);
    };
    let (min, max) = if first.value.is_nan() {
        (first.clone(), first)
    } else {
        (
            minimum.expect("a non-NaN first pixel"),
            maximum.expect("a non-NaN first pixel"),
        )
    };
    let spectral_axis = image.first_spectral_pixel_axis()?;
    let min_world =
        super::world_header::format_position(image.coordinates(), &min.position, spectral_axis)?;
    let max_world =
        super::world_header::format_position(image.coordinates(), &max.position, spectral_axis)?;
    Ok(Some(ImageExtrema {
        min: min.value,
        max: max.value,
        min_position: min.position,
        max_position: max.position,
        min_world,
        max_world,
    }))
}

/// Bound our pixel and mask buffers even if the physical tile is enormous.
fn bounded_cursor_shape(tile: Vec<usize>, shape: &[usize], limit: usize) -> Vec<usize> {
    let mut remaining = limit;
    tile.into_iter()
        .zip(shape)
        .map(|(tile, &length)| {
            let extent = tile.min(length).min(remaining).max(1);
            remaining = (remaining / extent).max(1);
            extent
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::bounded_cursor_shape;

    #[test]
    fn extrema_pixel_and_mask_buffers_are_bounded_even_for_large_tiles() {
        for (tile, shape) in [
            (vec![16_384, 16_384], vec![16_384, 16_384]),
            (vec![64, 128, 4, 512], vec![64, 128, 4, 512]),
            (vec![256, 256], vec![64, 32]),
        ] {
            let cursor = bounded_cursor_shape(tile, &shape, 1_048_576);
            assert!(cursor.iter().product::<usize>() <= 1_048_576);
            assert!(
                cursor
                    .iter()
                    .zip(shape)
                    .all(|(&n, length)| n > 0 && n <= length)
            );
        }
    }
}
