// SPDX-License-Identifier: LGPL-3.0-or-later

use std::borrow::Cow;
use std::fs;
use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::RecordValue;

use crate::analysis::axes::{direction_plane_axes_for_coordinates, stokes_for_axis};
use crate::{ImageError, ImageInterface, ImagePixel};

use super::{
    ParsedRegion, PlaneSelection, QualifiedShape, SpatialShape, insert_region,
    parse_crtf_qualifiers, parse_crtf_shape, parse_crtf_spectral_range,
    saved::load_saved_region_record, saved_region_source, shape_candidate_bounds, shape_contains,
    split_top_level,
};

#[derive(Clone, Copy)]
pub(in crate::analysis::imsubimage) struct TextRegionContext<'a> {
    pub(in crate::analysis::imsubimage) coordinates: Option<&'a CoordinateSystem>,
    pub(in crate::analysis::imsubimage) shape: Option<&'a [usize]>,
}

pub(in crate::analysis::imsubimage) enum CopyTextInput<'a> {
    Document {
        text: Cow<'a, str>,
        file: Option<&'a Path>,
    },
    Record(RecordValue),
}

pub(in crate::analysis::imsubimage) fn copy_text_input<'a>(
    text: &'a str,
    table: Option<&Path>,
) -> Result<CopyTextInput<'a>, ImageError> {
    let path = Path::new(text);
    if path.is_file() {
        let bytes = fs::read(path)
            .map_err(|error| ImageError::Io(format!("read region file {text}: {error}")))?;
        if bytes.starts_with(&[0xbe; 4]) {
            return casa_tables::read_table_record_file(path)
                .map(CopyTextInput::Record)
                .map_err(|_| invalid_region_file(path));
        }
        if !bytes.starts_with(b"#CRTF") {
            return Err(invalid_region_file(path));
        }
        let text = String::from_utf8(bytes).map_err(|_| invalid_region_file(path))?;
        return Ok(CopyTextInput::Document {
            text: Cow::Owned(text),
            file: Some(path),
        });
    }
    if looks_like_inline_crtf(text, true) {
        return Ok(CopyTextInput::Document {
            text: Cow::Borrowed(text),
            file: None,
        });
    }
    let (table, name) =
        saved_region_source(text).map_or((table, text), |(path, name)| (Some(path), name));
    let missing = || {
        let table = table.map_or_else(
            || "Temporary_Image".into(),
            |path| path.display().to_string(),
        );
        ImageError::CasaSelection(format!(
            "Unable to open region file or region table description {name} in image {table}"
        ))
    };
    let path = table.ok_or_else(missing)?;
    load_saved_region_record(path, name)
        .map(CopyTextInput::Record)
        .map_err(|_| missing())
}

pub(in crate::analysis::imsubimage) fn invalid_region_file(path: &Path) -> ImageError {
    ImageError::CasaSelection(format!(
        "{} is neither a valid binary region file, nor a valid region text file.",
        path.display()
    ))
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
    let mut global_tokens = Vec::new();
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
    let copy_text = origin_shape.is_some();
    let projection = text_origin
        .map(|origin| insert_region::Projection::new(origin, image.coordinates()))
        .transpose()?;
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
            let tokens = split_top_level(stmt[7..].trim(), ',');
            if copy_text {
                global_tokens.extend(tokens);
            } else {
                parse_crtf_qualifiers(
                    &tokens,
                    image.coordinates(),
                    full_shape,
                    &mut defaults,
                    text_origin,
                )?;
            }
            continue;
        }
        let tokens = shape_tokens(stmt, copy_text)?;
        let Some(first) = tokens.first() else {
            continue;
        };
        let (first, excluded) = first
            .strip_prefix('-')
            .filter(|_| copy_text)
            .map_or((*first, false), |shape| (shape.trim_start(), true));
        let spatial = parse_crtf_shape(origin, first, origin_plane, copy_text)?;
        if !overlaps_image(&spatial, origin_shape.unwrap_or(full_shape), origin_plane) {
            continue;
        }
        let mut planes = defaults.clone();
        let row_tokens = &tokens[1..];
        if let Some(shape) = origin_shape {
            let effective = last_plane_qualifiers(&global_tokens, row_tokens, origin);
            if !planes_overlap(&effective, origin, shape)? {
                continue;
            }
            parse_crtf_qualifiers(
                &effective,
                image.coordinates(),
                full_shape,
                &mut planes,
                text_origin,
            )?;
        } else {
            if !plane_qualifiers_overlap(parse_crtf_qualifiers(
                row_tokens,
                image.coordinates(),
                full_shape,
                &mut planes,
                text_origin,
            ))? {
                continue;
            }
        }
        let project = |shape| match &projection {
            Some(projection) => projection.project_shape(shape, copy_text),
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
        shapes.push(QualifiedShape {
            spatial,
            planes,
            excluded,
        });
    }

    if shapes.is_empty() {
        return Err(ImageError::CasaSelection(
            "Exception: No regions found.".into(),
        ));
    }
    // CASA represents a spatial annotation with explicit plane selection
    // as a WCExtension, even when the selected planes cover the entire axis.
    let is_compound = shapes
        .iter()
        .any(|shape| shape.planes.channels.is_some() || shape.planes.stokes.is_some());
    Ok(ParsedRegion {
        is_compound,
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
    let tokens = split_top_level(statement, ';')
        .into_iter()
        .flat_map(|section| split_top_level(section, ','))
        .filter(|token| !token.trim().is_empty())
        .collect::<Vec<_>>();
    if let Some(extra) = tokens.iter().skip(1).find(|token| !token.contains('=')) {
        return Err(ImageError::CasaSelection(format!(
            "Illegal extra characters on line ({}). Did you forget a '='?",
            extra.trim()
        )));
    }
    Ok(tokens)
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

fn last_plane_qualifiers<'a>(
    global: &[&'a str],
    row: &[&'a str],
    origin: &CoordinateSystem,
) -> Vec<&'a str> {
    [
        ("range", CoordinateType::Spectral),
        ("corr", CoordinateType::Stokes),
    ]
    .into_iter()
    .filter(|&(_, kind)| {
        (0..origin.n_pixel_axes()).any(|axis| origin.pixel_axis_type(axis) == Some(kind))
    })
    .filter_map(|(key, _)| {
        global
            .iter()
            .chain(row)
            .rev()
            .find(|token| {
                token
                    .split_once('=')
                    .is_some_and(|(field, _)| field.trim().eq_ignore_ascii_case(key))
            })
            .copied()
    })
    .collect()
}

fn planes_overlap(
    qualifiers: &[&str],
    origin: &CoordinateSystem,
    shape: &[usize],
) -> Result<bool, ImageError> {
    plane_qualifiers_overlap(parse_crtf_qualifiers(
        qualifiers,
        origin,
        shape,
        &mut PlaneSelection::default(),
        None,
    ))
}

fn plane_qualifiers_overlap(result: Result<(), ImageError>) -> Result<bool, ImageError> {
    match result {
        Ok(()) => Ok(true),
        Err(ImageError::CasaSelection(message))
            if message.starts_with("CRTF spectral range")
                || (message.starts_with("Stokes ")
                    && message.ends_with("is not one of this image's Stokes planes")) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}
