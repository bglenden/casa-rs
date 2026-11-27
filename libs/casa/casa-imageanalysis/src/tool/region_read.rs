// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel and mask extraction from arbitrary CASA regions (`ia.getregion`).

use ndarray::ArrayD;

use crate::analysis::evaluate_image_tool_mask_section;
use crate::analysis::imsubimage::{ImageRegionSection, SubimageRegion};

use super::attachment::{AttachedImage, read_locked};
use super::error::{ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, with_pixels, with_storage};
use super::pixels::{Chunk, ChunkData, ChunkRead, and_mask, collapse_axes};

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
