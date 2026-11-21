// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};

use crate::analysis::axes::{direction_plane_axes_for_coordinates, stokes_for_axis};
use crate::{ImageError, ImageInterface, ImagePixel};

use super::{
    ParsedRegion, PlaneSelection, QualifiedShape, SpatialShape, insert_region,
    parse_crtf_qualifiers, parse_crtf_shape, parse_crtf_spectral_range, shape_candidate_bounds,
    shape_contains, split_top_level,
};

#[derive(Clone, Copy)]
pub(in crate::analysis::imsubimage) struct TextRegionContext<'a> {
    pub(in crate::analysis::imsubimage) coordinates: Option<&'a CoordinateSystem>,
    pub(in crate::analysis::imsubimage) table: Option<&'a Path>,
    pub(in crate::analysis::imsubimage) shape: Option<&'a [usize]>,
}

// CASA discards CRTF entries whose source lattice has no selected pixel
// before converting the remaining world regions onto the copied image.
fn overlaps_image(shape: &SpatialShape, lattice: &[usize], plane: [usize; 2]) -> bool {
    let [Some(&nx), Some(&ny)] = plane.map(|axis| lattice.get(axis)) else {
        return false;
    };
    let (x0, y0, x1, y1) = shape_candidate_bounds(shape, nx, ny);
    (x0..=x1).any(|x| (y0..=y1).any(|y| shape_contains(shape, x, y)))
}

pub(super) fn looks_like_inline_crtf(text: &str, strict: bool) -> bool {
    let trimmed = text.trim_start();
    let shape_prefix = |prefix: &str| {
        let shape = prefix.trim_end();
        !shape.is_empty() && shape.bytes().all(|byte| byte.is_ascii_alphabetic())
    };
    if strict {
        return trimmed
            .split_once('[')
            .is_some_and(|(prefix, _)| shape_prefix(prefix));
    }
    trimmed.to_ascii_lowercase().starts_with("#crtf")
        || text.contains('\n')
        || trimmed.split_once('[').is_some_and(|(prefix, arguments)| {
            shape_prefix(prefix) && arguments.contains(',') && arguments.contains(']')
        })
}

pub(super) fn parse_crtf_document<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    text: &str,
    full_shape: &[usize],
    plane: [usize; 2],
    text_context: Option<TextRegionContext<'_>>,
) -> Result<ParsedRegion, ImageError> {
    let mut shapes = Vec::new();
    let mut defaults = PlaneSelection::default();
    let text_origin = text_context.and_then(|context| context.coordinates);
    let origin = text_origin.unwrap_or_else(|| image.coordinates());
    let origin_plane = text_origin
        .map(direction_plane_axes_for_coordinates)
        .transpose()?
        .unwrap_or(plane);
    let origin_shape = text_context.and_then(|context| context.shape);
    if origin_shape.is_some() && origin.find_coordinate(CoordinateType::Direction).is_none() {
        return Err(ImageError::CasaSelection(
            "RegionTextParser::RegionTextParser: Coordinate system has no direction coordinate"
                .into(),
        ));
    }
    let projection = text_origin
        .map(|origin| insert_region::Projection::new(origin, image.coordinates()))
        .transpose()?;
    let copy_text = origin_shape.is_some();
    for raw_stmt in text.split(|character| character == '\n' || (!copy_text && character == ';')) {
        let stmt = raw_stmt.trim();
        if stmt.is_empty() || stmt.starts_with('#') {
            continue;
        }
        let lower = stmt.to_ascii_lowercase();
        if lower.starts_with("ann ") || lower.starts_with("symbol") || lower.starts_with("text") {
            continue;
        }
        if lower.starts_with("global ") {
            parse_crtf_qualifiers(
                &split_top_level(stmt[7..].trim(), ','),
                image,
                full_shape,
                &mut defaults,
                text_origin,
            )?;
            continue;
        }
        let tokens = shape_tokens(stmt, copy_text)?;
        let Some(first) = tokens.first() else {
            continue;
        };
        let (first, excluded) = first
            .strip_prefix('-')
            .map_or((*first, false), |shape| (shape.trim_start(), true));
        let spatial = parse_crtf_shape(origin, first, origin_plane, copy_text)?;
        if let Some(shape) = origin_shape
            && !overlaps_image(&spatial, shape, origin_plane)
        {
            continue;
        }
        let project = |shape| match &projection {
            Some(projection) => projection.project_shape(shape),
            None => Ok(shape),
        };
        if excluded && shapes.is_empty() {
            // RegionTextList starts a leading difference from a single
            // origin pixel. Its stored upper corner repeats the lower corner.
            let spatial = project(SpatialShape::Box([0, 0, 0, 0]))?;
            let planes = origin_pixel_planes(image, text_origin, full_shape)?;
            shapes.push(QualifiedShape {
                spatial,
                planes,
                excluded: false,
            });
        }
        let spatial = project(spatial)?;
        let mut planes = defaults.clone();
        parse_crtf_qualifiers(&tokens[1..], image, full_shape, &mut planes, text_origin)?;
        shapes.push(QualifiedShape {
            spatial,
            planes,
            excluded,
        });
    }

    if shapes.is_empty() {
        let message = if origin_shape.is_some() {
            "Exception: No regions found.".into()
        } else {
            format!("No valid CRTF region definitions found in {text:?}")
        };
        return Err(ImageError::CasaSelection(message));
    }
    Ok(ParsedRegion {
        is_compound: false,
        is_crtf: true,
        shapes,
        saved_mask: None,
        saved_bounds: None,
        stride: None,
    })
}

fn shape_tokens(statement: &str, copy_text: bool) -> Result<Vec<&str>, ImageError> {
    if !copy_text {
        return Ok(split_top_level(statement, ','));
    }
    let sections = split_top_level(statement, ';');
    if let Some(extra) = sections
        .iter()
        .skip(1)
        .map(|text| text.trim())
        .find(|text| !text.is_empty() && !text.contains('='))
    {
        return Err(ImageError::CasaSelection(format!(
            "Illegal extra characters on line ({extra}). Did you forget a '='?"
        )));
    }
    Ok(sections
        .into_iter()
        .flat_map(|section| split_top_level(section, ','))
        .filter(|token| !token.trim().is_empty())
        .collect())
}

fn origin_pixel_planes<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    origin: Option<&CoordinateSystem>,
    shape: &[usize],
) -> Result<PlaneSelection, ImageError> {
    let channels = image
        .find_axis(CoordinateType::Spectral)
        .map(|axis| {
            parse_crtf_spectral_range(
                "[0pix,0pix]",
                image.coordinates(),
                axis,
                shape[axis],
                origin,
            )
        })
        .transpose()?;
    let stokes = if let Some(axis) = image.find_axis(CoordinateType::Stokes) {
        let origin = origin.unwrap_or_else(|| image.coordinates());
        let origin_axis = (0..origin.n_pixel_axes())
            .find(|&axis| origin.pixel_axis_type(axis) == Some(CoordinateType::Stokes));
        if let Some(origin_axis) = origin_axis {
            let labels = stokes_for_axis(origin, origin_axis)?;
            let selected = stokes_for_axis(image.coordinates(), axis)?
                .iter()
                .position(|label| Some(label) == labels.first())
                .into_iter()
                .collect();
            Some(selected)
        } else {
            None
        }
    } else {
        None
    };
    Ok(PlaneSelection { channels, stokes })
}
