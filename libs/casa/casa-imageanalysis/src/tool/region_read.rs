// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel and mask extraction from arbitrary CASA regions (`ia.getregion`).

use casa_images::{ImageInterface, ImagePixel};
use ndarray::{ArrayD, IxDyn};

use crate::analysis::evaluate_image_tool_mask_section;
use crate::analysis::imsubimage::{ImageRegionSection, SubimageRegion};

use super::attachment::{AttachedImage, read_locked};
use super::error::{ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_pixels, with_storage};
use super::pixels::{
    Chunk, ChunkData, ChunkRead, ChunkReadResult, PixelChunk, and_mask, collapse_axes,
};

/// Parameters for `ia.getregion`.
pub struct GetRegionRequest<'a> {
    /// Region specification (box, CRTF text, region file, or casacore record).
    pub region: Option<SubimageRegion<'a>>,
    /// Reduction / axis selection.
    pub axes: Option<Vec<usize>>,
    /// Optional LEL mask expression.
    pub mask: Option<&'a str>,
    /// Emit bounding box log line.
    pub list: bool,
    /// Drop degenerate singleton axes.
    pub dropdeg: bool,
    /// Return pixel mask array alongside pixels.
    pub getmask: bool,
    /// Stretch mask across degenerate axes.
    pub stretch: bool,
}

/// Reads pixels and combined mask from `attached` over `request.region` (`ia.getregion`).
pub fn get_region(
    attached: &AttachedImage,
    request: &GetRegionRequest<'_>,
) -> ImageToolResult<ChunkReadResult> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    let _ = &request.axes;

    let (pixels, mask, blc, trc) = match &*image {
        ImageHandle::Float(storage) => {
            if let Some(fits) = image.fits() {
                let metadata = match storage {
                    Storage::Temp { image, .. } => image.as_ref(),
                    Storage::Paged(_) => unreachable!(),
                };
                let view = fits.image_view(metadata);
                let (arr, m, blc, trc) = read_region_typed(&view, request)?;
                (PixelChunk::Float(arr), m, blc, trc)
            } else {
                with_storage!(storage, img => {
                    let (arr, m, blc, trc) = read_region_typed(img.as_ref(), request)?;
                    (PixelChunk::Float(arr), m, blc, trc)
                })
            }
        }
        ImageHandle::Double(storage) => with_storage!(storage, img => {
            let (arr, m, blc, trc) = read_region_typed(img.as_ref(), request)?;
            (PixelChunk::Double(arr), m, blc, trc)
        }),
        ImageHandle::Complex(storage) => with_storage!(storage, img => {
            let (arr, m, blc, trc) = read_region_typed(img.as_ref(), request)?;
            (PixelChunk::Complex(arr), m, blc, trc)
        }),
        ImageHandle::DComplex(storage) => with_storage!(storage, img => {
            let (arr, m, blc, trc) = read_region_typed(img.as_ref(), request)?;
            (PixelChunk::DComplex(arr), m, blc, trc)
        }),
    };

    let logs = if request.list {
        vec![ToolLogLine::info(format!(
            "getregion bounding box: {blc:?} to {trc:?}"
        ))]
    } else {
        Vec::new()
    };

    let pixels = if request.dropdeg {
        drop_deg_chunk(pixels)
    } else {
        pixels
    };
    let mask = if request.dropdeg {
        mask.map(drop_deg_array)
    } else {
        mask
    };

    Ok(ChunkReadResult { pixels, mask, logs })
}

type TypedRegionSlice<T> = (ArrayD<T>, Option<ArrayD<bool>>, Vec<usize>, Vec<usize>);

fn read_region_typed<T: ImagePixel>(
    view: &dyn ImageInterface<T>,
    request: &GetRegionRequest<'_>,
) -> ImageToolResult<TypedRegionSlice<T>> {
    let section = ImageRegionSection::resolve(view, request.region, true)?;
    let arr = view.get_slice(&section.start, &section.count, &section.stride)?;
    let mask = if request.getmask {
        let mut combined = view
            .get_mask_slice(&section.start, &section.count, &section.stride)?
            .unwrap_or_else(|| ArrayD::from_elem(IxDyn(arr.shape()), true));
        if let Some(membership) = &section.membership {
            combined &= membership;
        }
        if let Some(mask_expr) = request.mask
            && !mask_expr.is_empty()
        {
            combined &= &evaluate_image_tool_mask_section(
                mask_expr,
                view.shape(),
                view.coordinates(),
                (&section.start, &section.count, &section.stride),
                request.stretch,
            )?;
        }
        Some(combined)
    } else {
        None
    };
    let trc: Vec<usize> = section
        .start
        .iter()
        .zip(section.count.iter())
        .zip(section.stride.iter())
        .map(|((&s, &c), &st)| s + c.saturating_sub(1) * st)
        .collect();
    Ok((arr, mask, section.start, trc))
}

fn drop_deg_chunk(chunk: PixelChunk) -> PixelChunk {
    match chunk {
        PixelChunk::Float(a) => PixelChunk::Float(super::pixels::drop_degenerate_array(a)),
        PixelChunk::Double(a) => PixelChunk::Double(super::pixels::drop_degenerate_array(a)),
        PixelChunk::Complex(a) => PixelChunk::Complex(super::pixels::drop_degenerate_array(a)),
        PixelChunk::DComplex(a) => PixelChunk::DComplex(super::pixels::drop_degenerate_array(a)),
    }
}

fn drop_deg_array<A>(array: ArrayD<A>) -> ArrayD<A> {
    super::pixels::drop_degenerate_array(array)
}

/// Resolves region bounding box and optional per-pixel membership mask.
pub fn resolve_region_section(
    image: &ImageHandle,
    region: Option<SubimageRegion<'_>>,
    include_membership: bool,
) -> ImageToolResult<ImageRegionSection> {
    if let ImageHandle::Float(storage) = image {
        Ok(storage.with_float_view(|image| {
            ImageRegionSection::resolve(image, region, include_membership)
        })?)
    } else {
        Ok(
            with_pixels!(image, storage => with_storage!(storage, image =>
            ImageRegionSection::resolve(image.as_ref(), region, include_membership)))?,
        )
    }
}

/// Resolves a region section into a strided [`Chunk`] and optional membership mask.
pub fn read_region_section(
    image: &ImageHandle,
    region: Option<SubimageRegion<'_>>,
    include_membership: bool,
) -> ImageToolResult<(Chunk, Option<ArrayD<bool>>)> {
    let section = resolve_region_section(image, region, include_membership)?;
    Ok((
        Chunk {
            start: section.start,
            size: section.count,
            stride: section.stride,
        },
        section.membership,
    ))
}

/// Reads region pixel/mask arrays for CASA `ia.getregion`, including axis collapse and LEL mask.
#[allow(clippy::too_many_arguments)]
pub fn read_region_casa(
    attached: &AttachedImage,
    region: Option<SubimageRegion<'_>>,
    axes: &[i64],
    mask: &str,
    list: bool,
    dropdeg: bool,
    getmask: bool,
    stretch: bool,
) -> ImageToolResult<(ChunkData, Vec<ToolLogLine>)> {
    read_locked(attached, |image| {
        let shape = image.shape();
        let axes = collapse_axes(axes, shape.len())?;
        let (chunk, membership) = read_region_section(image, region, getmask || !axes.is_empty())?;
        let expression = if mask.is_empty() {
            None
        } else {
            Some(evaluate_image_tool_mask_section(
                mask,
                shape,
                image.coordinates(),
                (&chunk.start, &chunk.size, &chunk.stride),
                stretch,
            )?)
        };
        let data = chunk.read_from(
            image,
            &ChunkRead {
                axes: &axes,
                dropdeg,
                getmask,
            },
            and_mask(membership, expression),
        )?;
        let logs = list
            .then(|| {
                let end = chunk
                    .start
                    .iter()
                    .zip(&chunk.size)
                    .zip(&chunk.stride)
                    .map(|((&start, &count), &step)| start + (count - 1) * step)
                    .collect::<Vec<_>>();
                ToolLogLine::info(format!(
                    "Selected bounding box {:?} to {end:?}",
                    chunk.start
                ))
            })
            .into_iter()
            .collect();
        Ok((data, logs))
    })
}
