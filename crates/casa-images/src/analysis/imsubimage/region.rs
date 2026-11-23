// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spatial (`box`, `region` CRTF and saved image regions), spectral (`chans`),
//! and Stokes (`stokes`) selection for `imsubimage`, matching CASA
//! `CasacRegionManager::fromBCS`.

use std::fs;
use std::path::Path;

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate, StokesType,
};
use casa_types::quanta::Quantity;
use casa_types::quanta::casacore::mvangle_radians;

use crate::saved_region::PolygonPixelPlane;

use crate::analysis::axes::{direction_plane_axes, stokes_for_axis};
use crate::analysis::{
    ChannelSpan, PixelRegion, parse_box_region, parse_stokes_names, parse_stokes_selection,
};
use crate::{ImageError, ImageInterface, ImagePixel};

use super::SubimageRegion;

mod channels;
mod insert_region;
pub(super) mod saved;
mod sections;
mod text_origin;
mod window_masks;

pub(super) use sections::StridedSection;

use channels::resolve_channels;
use saved::{load_saved_region, parse_saved_region};
pub(super) use text_origin::{
    CopyTextInput, TextRegionContext, copy_text_input, invalid_region_file,
};
use text_origin::{looks_like_inline_crtf, parse_crtf_document};

#[derive(Debug, Clone)]
pub(super) enum SpatialShape {
    Box([usize; 4]),
    CenterBox {
        cx: f64,
        cy: f64,
        half_w: f64,
        half_h: f64,
    },
    RotBox {
        cx: f64,
        cy: f64,
        half_w: f64,
        half_h: f64,
        cos_pa: f64,
        sin_pa: f64,
    },
    Circle {
        cx: f64,
        cy: f64,
        rx: f64,
        ry: f64,
    },
    Ellipse {
        cx: f64,
        cy: f64,
        semi_major: f64,
        semi_minor: f64,
        cos_pa: f64,
        sin_pa: f64,
    },
    Polygon(Vec<[f64; 2]>),
}

#[derive(Default, Clone)]
struct PlaneSelection {
    channels: Option<Vec<usize>>,
    stokes: Option<Vec<usize>>,
}

struct QualifiedShape {
    spatial: SpatialShape,
    planes: PlaneSelection,
    excluded: bool,
}

const EMPTY_SECTION: &str = "selected region contains no pixels inside the image";

fn empty_section() -> ImageError {
    ImageError::CasaSelection(EMPTY_SECTION.into())
}

impl From<SpatialShape> for QualifiedShape {
    fn from(spatial: SpatialShape) -> Self {
        Self {
            spatial,
            planes: PlaneSelection::default(),
            excluded: false,
        }
    }
}

pub(in crate::analysis) struct SubimageSelection {
    pub(in crate::analysis) start: Vec<usize>,
    pub(in crate::analysis) shape: Vec<usize>,
    pub(in crate::analysis) stride: Option<Vec<usize>>,
    pub(in crate::analysis) plane: [usize; 2],
    // Spatial shapes whose membership must be evaluated for a pixel window.
    spatial_shapes: Vec<SpatialShape>,
    qualified_shapes: Vec<QualifiedShape>,
    saved_mask: Option<SavedRegionMask>,
    pub(in crate::analysis) channel_axis: Option<usize>,
    pub(in crate::analysis) channel_spans: Vec<ChannelSpan>,
    pub(in crate::analysis) stokes_axis: Option<usize>,
    pub(in crate::analysis) stokes_indices: Option<Vec<usize>>,
    pub(in crate::analysis) selected_stokes: Option<Vec<StokesType>>,
    pub(in crate::analysis) has_multiple_regions: bool,
}

// CASA's implicit region selects a single pixel on two positional axes
// when a multidimensional image has no direction coordinate.
pub(super) fn implicit_positional_axes<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
) -> Option<[usize; 2]> {
    if image.find_axis(CoordinateType::Direction).is_some() {
        return None;
    }
    let spectral = image.find_axis(CoordinateType::Spectral);
    let stokes = image.find_axis(CoordinateType::Stokes);
    let mut axes = (0..image.ndim()).filter(|&axis| Some(axis) != spectral && Some(axis) != stokes);
    let leading = [axes.next()?, axes.next()?];
    axes.next().map(|_| leading)
}

impl SubimageSelection {
    pub(super) fn resolve_for_insert<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        target: &dyn ImageInterface<T>,
        region: Option<SubimageRegion<'_>>,
    ) -> Result<(Self, Vec<usize>), ImageError> {
        if let Some(SubimageRegion::Record(record)) = region
            && let Some(slicer) =
                saved::lc_slicer_section(record, image.coordinates(), image.shape())?
            && slicer.stride.iter().any(|&step| step != 1)
        {
            let mut selection = Self::resolve(image, None, None, None, None)?;
            selection.start = slicer.start;
            selection.shape = slicer.count;
            return Ok((selection, slicer.stride));
        }
        let text_context = TextRegionContext {
            coordinates: Some(target.coordinates()),
            shape: None,
        };
        let mut selection =
            Self::resolve_with_origin(image, None, region, None, None, Some(text_context))?;
        selection.shape = selection.packed_shape();
        let stride = selection
            .stride
            .take()
            .unwrap_or_else(|| vec![1; image.ndim()]);
        Ok((selection, stride))
    }

    pub(in crate::analysis) fn packed_shape(&self) -> Vec<usize> {
        match &self.stride {
            Some(stride) => self
                .shape
                .iter()
                .zip(stride)
                .map(|(&size, &step)| size.div_ceil(step))
                .collect(),
            None => self.shape.clone(),
        }
    }

    pub(in crate::analysis) fn resolve<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        box_arg: Option<&str>,
        region_arg: Option<SubimageRegion<'_>>,
        chans_arg: Option<&str>,
        stokes_arg: Option<&str>,
    ) -> Result<Self, ImageError> {
        let selection = Self::resolve_section(image, box_arg, region_arg, chans_arg, stokes_arg)?;
        if let Some(strides) = &selection.stride
            && let Some((axis, &step)) = strides.iter().enumerate().find(|(_, step)| **step != 1)
        {
            return Err(ImageError::InvalidMetadata(format!(
                "saved region has non-unit stride {step} on axis {axis}"
            )));
        }
        Ok(selection)
    }

    pub(in crate::analysis) fn resolve_section<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        box_arg: Option<&str>,
        region_arg: Option<SubimageRegion<'_>>,
        chans_arg: Option<&str>,
        stokes_arg: Option<&str>,
    ) -> Result<Self, ImageError> {
        Self::resolve_with_origin(image, box_arg, region_arg, chans_arg, stokes_arg, None)
    }

    pub(super) fn resolve_with_origin<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        box_arg: Option<&str>,
        region_arg: Option<SubimageRegion<'_>>,
        chans_arg: Option<&str>,
        stokes_arg: Option<&str>,
        text_context: Option<TextRegionContext<'_>>,
    ) -> Result<Self, ImageError> {
        let ndim = image.ndim();
        let full_shape = image.shape().to_vec();
        let box_arg = box_arg.map(str::trim).filter(|s| !s.is_empty());
        let plane = if box_arg.is_some() || region_arg.is_some() {
            direction_plane_axes(image)?
        } else {
            [0, 1]
        };

        let mut regions: Vec<QualifiedShape> = Vec::new();
        let mut saved_mask = None;
        let mut saved_bounds: Option<(Vec<usize>, Vec<usize>)> = None;
        let mut stride = None;

        if let Some(box_text) = box_arg {
            let PixelRegion::Boxes(boxes) = parse_box_region(box_text, &full_shape, plane)? else {
                unreachable!("parse_box_region returns Boxes")
            };
            for b in boxes {
                regions.push(SpatialShape::Box(b).into());
            }
        }

        let mut is_compound = false;
        let mut is_crtf = false;
        if let Some(region) = region_arg {
            let parsed = match region {
                SubimageRegion::Text(text)
                    if text_context.is_some_and(|context| context.shape.is_some()) =>
                {
                    parse_crtf_document(image, text, &full_shape, plane, text_context)?
                }
                SubimageRegion::Text(text) => {
                    parse_region_argument(image, text, &full_shape, plane, text_context)?
                }
                SubimageRegion::Record(record) => {
                    parse_saved_region("region", record, image.coordinates(), &full_shape, plane)?
                }
            };
            if parsed.saved_bounds.is_some()
                && (box_arg.is_some()
                    || [chans_arg, stokes_arg]
                        .into_iter()
                        .flatten()
                        .any(|v| !v.trim().is_empty()))
            {
                return Err(ImageError::CasaSelection(
                    "saved region cannot be combined with box, chans, or stokes".into(),
                ));
            }
            is_compound = parsed.is_compound;
            is_crtf = parsed.is_crtf;
            regions.extend(parsed.shapes);
            saved_mask = parsed.saved_mask;
            saved_bounds = parsed.saved_bounds;
            stride = parsed.stride;
        }

        let explicit = |selection: Option<&str>| selection.is_some_and(|s| !s.trim().is_empty());
        for region in &mut regions {
            if explicit(chans_arg) {
                region.planes.channels = None;
            }
            if explicit(stokes_arg) {
                region.planes.stokes = None;
            }
        }
        let channel_union = union_planes(&regions, |region| region.planes.channels.as_deref());
        let stokes_union = union_planes(&regions, |region| region.planes.stokes.as_deref());
        let spatial_shapes = regions
            .iter()
            .filter(|region| !region.excluded)
            .map(|region| &region.spatial)
            .collect::<Vec<_>>();
        let SpatialResolution {
            mut start,
            mut shape,
            spatial_shapes,
        } = resolve_spatial_and_saved_bounds(
            &full_shape,
            ndim,
            plane,
            &spatial_shapes,
            saved_bounds.as_ref(),
            saved_mask.is_some(),
        )?;

        let (channel_axis, channel_spans) = resolve_channels(
            image,
            &full_shape,
            chans_arg,
            channel_union.as_deref(),
            is_crtf,
            &mut start,
            &mut shape,
        )?;

        let StokesResolution {
            axis: stokes_axis,
            indices: stokes_indices,
            selected: selected_stokes,
        } = resolve_stokes(
            image,
            stokes_arg,
            stokes_union.as_deref(),
            &mut start,
            &mut shape,
        )?;

        let has_multiple_regions = is_compound
            || regions.len() > 1
            || !channel_spans.is_empty()
            || stokes_axis
                .zip(stokes_indices.as_ref())
                .is_some_and(|(a, v)| v.len() != shape[a]);
        Ok(Self {
            start,
            shape,
            stride,
            plane,
            spatial_shapes,
            qualified_shapes: if regions
                .iter()
                .any(|r| r.excluded || r.planes.channels.is_some() || r.planes.stokes.is_some())
            {
                regions
            } else {
                Vec::new()
            },
            saved_mask,
            channel_axis: channel_axis.or_else(|| image.find_axis(CoordinateType::Spectral)),
            channel_spans,
            stokes_axis: stokes_axis.or_else(|| image.find_axis(CoordinateType::Stokes)),
            stokes_indices,
            selected_stokes,
            has_multiple_regions,
        })
    }
}

fn union_planes(
    shapes: &[QualifiedShape],
    indices: impl Fn(&QualifiedShape) -> Option<&[usize]>,
) -> Option<Vec<usize>> {
    if shapes.is_empty() {
        return None;
    }
    let mut union = Vec::new();
    for shape in shapes.iter().filter(|shape| !shape.excluded) {
        union.extend_from_slice(indices(shape)?);
    }
    Some(union)
}

enum SavedRegionMask {
    Polygon {
        axes: [usize; 2],
        plane: PolygonPixelPlane,
    },
    Compound(saved::CompoundRegion),
}

struct ParsedRegion {
    is_compound: bool,
    is_crtf: bool,
    shapes: Vec<QualifiedShape>,
    saved_mask: Option<SavedRegionMask>,
    saved_bounds: Option<(Vec<usize>, Vec<usize>)>,
    stride: Option<Vec<usize>>,
}

fn saved_region_source(text: &str) -> Option<(&Path, &str)> {
    let (path, name) = text.rsplit_once(':')?;
    let name = name.trim();
    let path = Path::new(path);
    (!path.as_os_str().is_empty() && !name.is_empty() && path.is_dir()).then_some((path, name))
}

pub(super) fn external_region_path(text: &str) -> Option<&Path> {
    let text = text.trim();
    if let Some((path, _)) = saved_region_source(text) {
        return Some(path);
    }
    if text.starts_with("box:")
        || (text.starts_with("pixelbox(") && text.ends_with(')'))
        || looks_like_four_ints(text)
    {
        return None;
    }
    let path = Path::new(text);
    path.is_file().then_some(path)
}

fn parse_region_argument<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    text: &str,
    full_shape: &[usize],
    plane: [usize; 2],
    text_context: Option<TextRegionContext<'_>>,
) -> Result<ParsedRegion, ImageError> {
    if let Some((path, name)) = saved_region_source(text) {
        return load_saved_region(path, name, image.coordinates(), full_shape, plane);
    }

    let box_text = text
        .strip_prefix("box:")
        .or_else(|| {
            text.strip_prefix("pixelbox(")
                .and_then(|value| value.strip_suffix(')'))
        })
        .or_else(|| looks_like_four_ints(text).then_some(text));
    if let Some(box_text) = box_text {
        let b = parse_single_box(box_text.trim(), full_shape, plane)?;
        return Ok(ParsedRegion {
            is_compound: false,
            is_crtf: false,
            shapes: vec![SpatialShape::Box(b).into()],
            saved_mask: None,
            saved_bounds: None,
            stride: None,
        });
    }

    let reg_path = Path::new(text);
    let crtf_text = if reg_path.is_file() {
        let bytes = fs::read(reg_path)
            .map_err(|e| ImageError::Io(format!("read region file {}: {e}", reg_path.display())))?;
        if bytes.starts_with(&[0xbe; 4]) {
            let record = casa_tables::read_table_record_file(reg_path)
                .map_err(|e| ImageError::Io(format!("read saved region {text}: {e}")))?;
            return parse_saved_region(text, &record, image.coordinates(), full_shape, plane);
        }
        String::from_utf8(bytes)
            .map_err(|e| ImageError::Io(format!("read CRTF file {text}: {e}")))?
    } else if looks_like_inline_crtf(text, false) {
        text.to_string()
    } else if let Some(path) = image.name().filter(|path| path.is_dir()) {
        return load_saved_region(path, text, image.coordinates(), full_shape, plane);
    } else {
        return Err(ImageError::CasaSelection(format!(
            "Unable to open region file or region table description {text}."
        )));
    };

    parse_crtf_document(image, &crtf_text, full_shape, plane, text_context)
}

fn looks_like_four_ints(text: &str) -> bool {
    let parts: Vec<&str> = text.split(',').collect();
    parts.len() == 4 && parts.iter().all(|p| p.trim().parse::<usize>().is_ok())
}

fn parse_single_box(
    text: &str,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<[usize; 4], ImageError> {
    let PixelRegion::Boxes(boxes) = parse_box_region(text, full_shape, plane)? else {
        unreachable!("parse_box_region returns Boxes")
    };
    if boxes.len() != 1 {
        return Err(ImageError::InvalidMetadata(format!(
            "region box must hold 4 integers x0,y0,x1,y1: {text:?}"
        )));
    }
    Ok(boxes[0])
}

fn parse_crtf_qualifiers(
    tokens: &[&str],
    coordinates: &CoordinateSystem,
    full_shape: &[usize],
    planes: &mut PlaneSelection,
    text_origin: Option<&CoordinateSystem>,
) -> Result<(), ImageError> {
    let axis_for = |coordinates: &CoordinateSystem, kind| {
        (0..coordinates.n_pixel_axes())
            .find(|&axis| coordinates.pixel_axis_type(axis) == Some(kind))
    };
    for tok in tokens {
        let trimmed = tok.trim();
        let Some((key, val)) = trimmed.split_once('=') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let val = val.trim();
        if key == "corr" {
            let inner = val
                .strip_prefix('[')
                .and_then(|s| s.strip_suffix(']'))
                .unwrap_or(val);
            let joined = inner
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(",");
            if !joined.is_empty() {
                let selected = if let Some(origin) = text_origin {
                    let missing =
                        || ImageError::CasaSelection("Exception: No regions found.".into());
                    let axis = axis_for(origin, CoordinateType::Stokes).ok_or_else(missing)?;
                    let selected =
                        parse_stokes_names(origin, axis, &joined).map_err(|_| missing())?;
                    let origin_values = stokes_for_axis(origin, axis)?;
                    let axis = axis_for(coordinates, CoordinateType::Stokes).ok_or_else(missing)?;
                    let input_values = stokes_for_axis(coordinates, axis)?;
                    let matched = selected
                        .into_iter()
                        .filter_map(|index| {
                            input_values
                                .iter()
                                .position(|value| *value == origin_values[index])
                        })
                        .collect::<Vec<_>>();
                    if matched.is_empty() {
                        return Err(missing());
                    }
                    matched
                } else {
                    let axis = axis_for(coordinates, CoordinateType::Stokes).ok_or_else(|| {
                        ImageError::InvalidMetadata(
                            "Stokes selection requires a Stokes axis".into(),
                        )
                    })?;
                    parse_stokes_names(coordinates, axis, &joined)?
                };
                planes.stokes = Some(selected);
            }
        } else if key == "range" {
            let spec_ax = axis_for(coordinates, CoordinateType::Spectral).ok_or_else(|| {
                ImageError::InvalidMetadata("CRTF range requires a spectral axis".into())
            })?;
            let indices = parse_crtf_spectral_range(
                val,
                coordinates,
                spec_ax,
                full_shape[spec_ax],
                text_origin,
            )?;
            planes.channels = Some(indices);
        }
    }
    Ok(())
}

fn parse_crtf_spectral_range(
    val: &str,
    coords: &CoordinateSystem,
    spec_axis: usize,
    axis_len: usize,
    text_origin: Option<&CoordinateSystem>,
) -> Result<Vec<usize>, ImageError> {
    let [first, last] = crtf_spectral_bounds(val, coords, spec_axis, axis_len, text_origin)?;
    Ok((first..=last).collect())
}

fn crtf_spectral_bounds(
    val: &str,
    coords: &CoordinateSystem,
    spec_axis: usize,
    axis_len: usize,
    text_origin: Option<&CoordinateSystem>,
) -> Result<[usize; 2], ImageError> {
    let inner = val
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(val);
    let parts: Vec<&str> = inner
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if parts.len() != 2 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF spectral range [{inner}]"
        )));
    }
    let c0 = parse_spectral_endpoint(parts[0], coords, spec_axis, text_origin)?;
    let c1 = parse_spectral_endpoint(parts[1], coords, spec_axis, text_origin)?;
    let lo = c0.min(c1).max(0);
    let hi = c0.max(c1).min(axis_len as isize - 1);
    if hi < lo || lo >= axis_len as isize {
        return Err(ImageError::CasaSelection(format!(
            "CRTF spectral range [{inner}] selects no channels in 0..{axis_len}"
        )));
    }
    Ok([lo as usize, hi as usize])
}

fn parse_spectral_endpoint(
    text: &str,
    coords: &CoordinateSystem,
    spec_axis: usize,
    text_origin: Option<&CoordinateSystem>,
) -> Result<isize, ImageError> {
    let lower = text.trim().to_ascii_lowercase();
    let pixel =
        if let Some(ch_str) = lower
            .strip_suffix("chan")
            .or_else(|| lower.strip_suffix("pix"))
        {
            Some(ch_str.trim().parse::<f64>().map_err(|e| {
                ImageError::InvalidMetadata(format!("invalid channel {text:?}: {e}"))
            })?)
        } else {
            lower.parse::<isize>().ok().map(|channel| channel as f64)
        };
    if let Some(pixel) = pixel
        && text_origin.is_none()
    {
        return Ok(pixel.round() as isize);
    }
    if let Some(origin) = text_origin {
        let origin = spectral_coordinate(origin)?;
        let input = spectral_coordinate(coords)?;
        let hz = match pixel {
            Some(pixel) => origin.value_in_hz(origin.to_world(&[pixel])?[0])?,
            None => Quantity::read(text.trim())
                .map_err(|e| {
                    ImageError::InvalidMetadata(format!("invalid CRTF range {text:?}: {e}"))
                })?
                .get_value_in(&casa_types::quanta::Unit::new("Hz").expect("known unit"))
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?,
        };
        let world = input.value_from_hz(hz)?;
        return Ok(input.to_pixel(&[world])?[0].round() as isize);
    }
    let q = Quantity::read(text.trim())
        .map_err(|e| ImageError::InvalidMetadata(format!("invalid CRTF range {text:?}: {e}")))?;
    let (index, _) = coords
        .pixel_axis_owner(spec_axis)
        .ok_or_else(|| ImageError::InvalidMetadata("CRTF range requires a spectral axis".into()))?;
    let CoordinateModel::Spectral(spectral) = coords.coordinate(index) else {
        return Err(ImageError::InvalidMetadata(
            "CRTF range requires a spectral axis".into(),
        ));
    };
    let world = spectral.value_from_hz(q.get_si_value())?;
    Ok(spectral.to_pixel(&[world])?[0].round() as isize)
}

fn spectral_coordinate(coordinates: &CoordinateSystem) -> Result<&SpectralCoordinate, ImageError> {
    if let Some(index) = coordinates.find_coordinate(CoordinateType::Spectral)
        && let CoordinateModel::Spectral(coordinate) = coordinates.coordinate(index)
    {
        return Ok(coordinate);
    }
    Err(ImageError::InvalidMetadata(
        "CRTF range requires a spectral axis".into(),
    ))
}

fn parse_crtf_shape(
    coordinates: &CoordinateSystem,
    stmt: &str,
    plane: [usize; 2],
    copy_text: bool,
) -> Result<SpatialShape, ImageError> {
    let trimmed = stmt.trim();
    let Some(bracket) = trimmed.find('[') else {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF region syntax: {trimmed:?}"
        )));
    };
    let keyword = trimmed[..bracket].trim().to_ascii_lowercase();
    let geometry = &trimmed[bracket..];
    // CASA's coordinate-pair reader consumes one adjacent character.
    let geometry = if matches!(keyword.as_str(), "box" | "centerbox" | "poly" | "polygon") {
        geometry
            .strip_suffix(')')
            .filter(|body| body.ends_with(']'))
            .unwrap_or(geometry)
    } else {
        geometry
    };
    let body = geometry
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| {
            ImageError::InvalidMetadata(format!("unclosed CRTF brackets: {trimmed:?}"))
        })?;
    let args = split_top_level(body, ',');

    match keyword.as_str() {
        "box" => {
            if args.len() != 2 {
                return Err(ImageError::InvalidMetadata(format!(
                    "invalid CRTF box {trimmed:?}"
                )));
            }
            let [x0, y0] = crtf_point_to_pixel(coordinates, args[0], plane)?;
            let [x1, y1] = crtf_point_to_pixel(coordinates, args[1], plane)?;
            if copy_text {
                return Ok(SpatialShape::Polygon(vec![
                    [x0, y0],
                    [x1, y0],
                    [x1, y1],
                    [x0, y1],
                ]));
            }
            let lo_x = x0.min(x1).max(0.0).round() as usize;
            let hi_x = x0.max(x1).max(0.0).round() as usize;
            let lo_y = y0.min(y1).max(0.0).round() as usize;
            let hi_y = y0.max(y1).max(0.0).round() as usize;
            Ok(SpatialShape::Box([lo_x, lo_y, hi_x, hi_y]))
        }
        "centerbox" => {
            if args.len() != 2 {
                return Err(ImageError::InvalidMetadata(format!(
                    "invalid CRTF centerbox {trimmed:?}"
                )));
            }
            let [cx, cy] = crtf_point_to_pixel(coordinates, args[0], plane)?;
            let [wx, wy] = crtf_extent_pair_to_pixels(coordinates, args[1])?;
            Ok(SpatialShape::CenterBox {
                cx,
                cy,
                half_w: wx * 0.5,
                half_h: wy * 0.5,
            })
        }
        "rotbox" => {
            if args.len() != 3 {
                return Err(ImageError::InvalidMetadata(format!(
                    "invalid CRTF rotbox {trimmed:?}"
                )));
            }
            let [cx, cy] = crtf_point_to_pixel(coordinates, args[0], plane)?;
            let [wx, wy] = crtf_extent_pair_to_pixels(coordinates, args[1])?;
            let pa = parse_world_angle(args[2])?;
            Ok(SpatialShape::RotBox {
                cx,
                cy,
                half_w: wx * 0.5,
                half_h: wy * 0.5,
                cos_pa: pa.cos(),
                sin_pa: pa.sin(),
            })
        }
        "circle" => {
            if args.len() != 2 {
                return Err(ImageError::InvalidMetadata(format!(
                    "invalid CRTF circle {trimmed:?}"
                )));
            }
            let [cx, cy] = crtf_point_to_pixel(coordinates, args[0], plane)?;
            let [rx, ry] = crtf_radius_to_pixels(coordinates, args[1])?;
            if copy_text {
                if let Some(radius) = parse_crtf_pix(args[1])?
                    && let Some([sx, sy]) = direction_pixel_scales(coordinates)
                    && (sx / sy - 1.0).abs() > 1e-9
                {
                    // CASA's CRTF parser expresses a pixel-radius circle on a
                    // non-square grid as an axis-aligned ellipse.
                    return Ok(SpatialShape::Ellipse {
                        cx,
                        cy,
                        semi_major: radius * sx / sy,
                        semi_minor: radius * sy / sx,
                        cos_pa: 0.0,
                        sin_pa: 1.0,
                    });
                }
                let radius = if plane[0] < plane[1] { rx } else { ry };
                return Ok(SpatialShape::Circle {
                    cx,
                    cy,
                    rx: radius,
                    ry: radius,
                });
            }
            Ok(SpatialShape::Circle { cx, cy, rx, ry })
        }
        "ellipse" => {
            if args.len() != 3 {
                return Err(ImageError::InvalidMetadata(format!(
                    "invalid CRTF ellipse {trimmed:?}"
                )));
            }
            let [cx, cy] = crtf_point_to_pixel(coordinates, args[0], plane)?;
            let [a, b] = crtf_extent_pair_to_pixels(coordinates, args[1])?;
            let pa = parse_world_angle(args[2])?;
            Ok(SpatialShape::Ellipse {
                cx,
                cy,
                semi_major: a,
                semi_minor: b,
                cos_pa: pa.cos(),
                sin_pa: pa.sin(),
            })
        }
        "poly" | "polygon" => {
            if args.len() < 3 {
                return Err(ImageError::InvalidMetadata(format!(
                    "CRTF polygon requires at least 3 vertices: {trimmed:?}"
                )));
            }
            let mut verts = Vec::with_capacity(args.len());
            for arg in args {
                verts.push(crtf_point_to_pixel(coordinates, arg, plane)?);
            }
            Ok(SpatialShape::Polygon(verts))
        }
        _ => Err(ImageError::InvalidMetadata(format!(
            "unsupported CRTF region keyword {keyword:?}"
        ))),
    }
}

fn crtf_point_to_pixel(
    coordinates: &CoordinateSystem,
    pair_text: &str,
    plane: [usize; 2],
) -> Result<[f64; 2], ImageError> {
    let inner = pair_text
        .trim()
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| {
            ImageError::InvalidMetadata(format!(
                "expected [x, y] coordinate pair, got {pair_text:?}"
            ))
        })?;
    let parts = split_top_level(inner, ',');
    if parts.len() != 2 {
        return Err(ImageError::InvalidMetadata(format!(
            "expected [x, y] coordinate pair, got {pair_text:?}"
        )));
    }
    let px = parse_crtf_pix(parts[0])?;
    let py = parse_crtf_pix(parts[1])?;
    match (px, py) {
        (Some(x), Some(y)) => Ok([x, y]),
        (None, None) => {
            if coordinates.n_pixel_axes() < 2 || coordinates.n_world_axes() < 2 {
                return Err(ImageError::InvalidMetadata(
                    "world CRTF regions require at least 2 coordinate axes".into(),
                ));
            }
            let [ax, ay] = plane;
            let [longitude, latitude] =
                super::super::axes::world_axes_for_plane(coordinates, plane)?;
            let base = vec![0.0; coordinates.n_pixel_axes()];
            let mut world = coordinates.to_world(&base)?;
            world[longitude] = parse_world_angle(parts[0])?;
            world[latitude] = parse_world_angle(parts[1])?;
            let pix = coordinates.to_pixel(&world)?;
            Ok([pix[ax], pix[ay]])
        }
        _ => Err(ImageError::InvalidMetadata(
            "cannot mix pix and world coordinates in a CRTF point".into(),
        )),
    }
}

fn crtf_extent_pair_to_pixels(
    coordinates: &CoordinateSystem,
    pair_text: &str,
) -> Result<[f64; 2], ImageError> {
    let inner = pair_text
        .trim()
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| {
            ImageError::InvalidMetadata(format!("expected [wx, wy] pair, got {pair_text:?}"))
        })?;
    let parts = split_top_level(inner, ',');
    if parts.len() != 2 {
        return Err(ImageError::InvalidMetadata(format!(
            "expected [wx, wy] pair, got {pair_text:?}"
        )));
    }
    let scale = direction_pixel_scales(coordinates);
    Ok([
        parse_extent_single(parts[0], scale.map(|s| s[0]))?,
        parse_extent_single(parts[1], scale.map(|s| s[1]))?,
    ])
}

fn crtf_radius_to_pixels(
    coordinates: &CoordinateSystem,
    text: &str,
) -> Result<[f64; 2], ImageError> {
    if let Some(r) = parse_crtf_pix(text)? {
        return Ok([r, r]);
    }
    let rad = parse_world_angle(text)?;
    let [sx, sy] = direction_pixel_scales(coordinates).ok_or_else(|| {
        ImageError::InvalidMetadata(
            "angular CRTF radius requires a Direction or 2D linear coordinate".into(),
        )
    })?;
    Ok([rad / sx, rad / sy])
}

fn parse_extent_single(text: &str, scale_rad_per_px: Option<f64>) -> Result<f64, ImageError> {
    if let Some(px) = parse_crtf_pix(text)? {
        return Ok(px);
    }
    let rad = parse_world_angle(text)?;
    let scale = scale_rad_per_px.ok_or_else(|| {
        ImageError::InvalidMetadata(
            "angular CRTF size requires non-zero direction pixel scale".into(),
        )
    })?;
    Ok(rad / scale)
}

fn direction_pixel_scales(coords: &CoordinateSystem) -> Option<[f64; 2]> {
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        let np = coord.n_pixel_axes();
        if coord.coordinate_type() == CoordinateType::Direction && np >= 2 {
            let inc = coord.increment();
            let sx = inc[0].abs();
            let sy = inc[1].abs();
            if sx > 0.0 && sy > 0.0 {
                return Some([sx, sy]);
            }
        }
    }
    None
}

fn parse_crtf_pix(text: &str) -> Result<Option<f64>, ImageError> {
    let lower = text.trim().to_ascii_lowercase();
    let Some(num) = lower.strip_suffix("pix") else {
        return Ok(None);
    };
    let val = num.trim().parse::<f64>().map_err(|e| {
        ImageError::InvalidMetadata(format!("invalid CRTF pixel value {text:?}: {e}"))
    })?;
    if !val.is_finite() {
        return Err(ImageError::InvalidMetadata(format!(
            "non-finite CRTF pixel value {text:?}"
        )));
    }
    Ok(Some(val))
}

pub(in crate::analysis) fn parse_world_angle(text: &str) -> Result<f64, ImageError> {
    let trimmed = text.trim();
    if trimmed.contains(':') {
        return parse_colon_hms(trimmed);
    }
    if is_dot_dms(trimmed) {
        return parse_dot_dms(trimmed);
    }
    if let Ok(rad) = trimmed.parse::<f64>() {
        return if rad.is_finite() {
            Ok(rad)
        } else {
            Err(ImageError::InvalidMetadata(format!(
                "non-finite angle {text:?}"
            )))
        };
    }
    let q = Quantity::read(trimmed)
        .map_err(|e| ImageError::InvalidMetadata(format!("invalid CRTF angle {text:?}: {e}")))?;
    mvangle_radians(&q)
        .map_err(|e| ImageError::InvalidMetadata(format!("invalid CRTF angle {text:?}: {e}")))
}

fn parse_colon_hms(text: &str) -> Result<f64, ImageError> {
    let (sign, rest) = strip_sign(text);
    let parts: Vec<&str> = rest.split(':').collect();
    if !(2..=3).contains(&parts.len()) {
        return Err(ImageError::InvalidMetadata(format!("invalid HMS {text:?}")));
    }
    let h = parts[0]
        .parse::<f64>()
        .map_err(|e| ImageError::InvalidMetadata(format!("invalid HMS hours in {text:?}: {e}")))?;
    let m = parts[1].parse::<f64>().map_err(|e| {
        ImageError::InvalidMetadata(format!("invalid HMS minutes in {text:?}: {e}"))
    })?;
    let s = if parts.len() == 3 {
        parts[2].parse::<f64>().map_err(|e| {
            ImageError::InvalidMetadata(format!("invalid HMS seconds in {text:?}: {e}"))
        })?
    } else {
        0.0
    };
    let hours = sign * (h + m / 60.0 + s / 3600.0);
    Ok((hours * 15.0).to_radians())
}

fn is_dot_dms(text: &str) -> bool {
    let (_, rest) = strip_sign(text);
    rest.matches('.').count() >= 2 && rest.chars().all(|c| c.is_ascii_digit() || c == '.')
}

fn parse_dot_dms(text: &str) -> Result<f64, ImageError> {
    let (sign, rest) = strip_sign(text);
    let parts: Vec<&str> = rest.splitn(3, '.').collect();
    let d = parts[0].parse::<f64>().map_err(|e| {
        ImageError::InvalidMetadata(format!("invalid DMS degrees in {text:?}: {e}"))
    })?;
    let m = parts[1]
        .parse::<f64>()
        .map_err(|e| ImageError::InvalidMetadata(format!("invalid DMS arcmin in {text:?}: {e}")))?;
    let s = parts[2]
        .parse::<f64>()
        .map_err(|e| ImageError::InvalidMetadata(format!("invalid DMS arcsec in {text:?}: {e}")))?;
    let deg = sign * (d + m / 60.0 + s / 3600.0);
    Ok(deg.to_radians())
}

fn strip_sign(text: &str) -> (f64, &str) {
    let t = text.trim();
    if let Some(rest) = t.strip_prefix('-') {
        (-1.0, rest.trim())
    } else if let Some(rest) = t.strip_prefix('+') {
        (1.0, rest.trim())
    } else {
        (1.0, t)
    }
}

fn split_top_level(text: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (idx, ch) in text.char_indices() {
        match ch {
            '[' | '(' => depth += 1,
            ']' | ')' => depth = (depth - 1).max(0),
            c if c == sep && depth == 0 => {
                let piece = text[start..idx].trim();
                if !piece.is_empty() {
                    parts.push(piece);
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    let tail = text[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

struct SpatialResolution {
    start: Vec<usize>,
    shape: Vec<usize>,
    spatial_shapes: Vec<SpatialShape>,
}

fn resolve_spatial_and_saved_bounds(
    full_shape: &[usize],
    ndim: usize,
    plane: [usize; 2],
    shapes: &[&SpatialShape],
    saved_bounds: Option<&(Vec<usize>, Vec<usize>)>,
    has_saved_mask: bool,
) -> Result<SpatialResolution, ImageError> {
    let mut start = vec![0usize; ndim];
    let mut shape = full_shape.to_vec();

    if let Some((sb_start, sb_shape)) = saved_bounds {
        start = sb_start.clone();
        shape = sb_shape.clone();
    }

    if shapes.is_empty() {
        return Ok(SpatialResolution {
            start,
            shape,
            spatial_shapes: Vec::new(),
        });
    }

    if ndim < 2 {
        return Err(ImageError::ShapeMismatch {
            expected: full_shape.to_vec(),
            got: vec![2],
        });
    }
    let [px, py] = plane;
    let nx = full_shape[px];
    let ny = full_shape[py];

    // Fast path for single box with no saved WCPolygon.
    if shapes.len() == 1
        && !has_saved_mask
        && let SpatialShape::Box([x0, y0, x1, y1]) = *shapes[0]
    {
        if x1 >= nx || y1 >= ny || x1 < x0 || y1 < y0 {
            return Err(ImageError::CasaSelection(
                "selected box exceeds image bounds".into(),
            ));
        }
        start[px] = x0;
        start[py] = y0;
        shape[px] = x1 - x0 + 1;
        shape[py] = y1 - y0 + 1;
        return Ok(SpatialResolution {
            start,
            shape,
            spatial_shapes: Vec::new(),
        });
    }
    if saved_bounds.is_some() && !has_saved_mask {
        return Ok(SpatialResolution {
            start,
            shape,
            spatial_shapes: shapes.iter().copied().cloned().collect(),
        });
    }

    // Union bounding box + exact spatial mask over included integer pixels.
    let mut min_x = nx;
    let mut min_y = ny;
    let mut max_x = 0usize;
    let mut max_y = 0usize;
    let mut any_pixel = false;

    for s in shapes {
        let (x0, y0, x1, y1) = shape_candidate_bounds(s, nx, ny);
        if let SpatialShape::Box(_) = s {
            if x0 <= x1 && y0 <= y1 && shape_contains(s, x0, y0) {
                min_x = min_x.min(x0);
                min_y = min_y.min(y0);
                max_x = max_x.max(x1);
                max_y = max_y.max(y1);
                any_pixel = true;
            }
            continue;
        }
        let mut has_pixels = false;
        for x in x0..=x1 {
            for y in y0..=y1 {
                if shape_contains(s, x, y) {
                    has_pixels = true;
                    min_x = min_x.min(x);
                    min_y = min_y.min(y);
                    max_x = max_x.max(x);
                    max_y = max_y.max(y);
                }
            }
        }
        if has_pixels {
            any_pixel = true;
            // LCEllipsoid uses a square enclosing box for a rotated ellipse,
            // including rows and columns that its mask excludes entirely.
            if let SpatialShape::Ellipse {
                cx,
                cy,
                semi_major,
                semi_minor,
                cos_pa,
                sin_pa,
            } = s
                && cos_pa.abs() > 1e-7
                && sin_pa.abs() > 1e-7
            {
                let r = semi_major.max(*semi_minor);
                let lower = |c: f64| (c - r - 1e-5).ceil().max(0.0) as usize;
                let upper = |c: f64, n: usize| ((c + r + 1e-5).floor() as usize).min(n - 1);
                min_x = min_x.min(lower(*cx));
                min_y = min_y.min(lower(*cy));
                max_x = max_x.max(upper(*cx, nx));
                max_y = max_y.max(upper(*cy, ny));
            }
        }
    }

    if !any_pixel {
        return Err(empty_section());
    }

    if has_saved_mask {
        // When both box and saved WCPolygon exist, form union over their planes.
        min_x = min_x.min(start[px]);
        min_y = min_y.min(start[py]);
        max_x = max_x.max(start[px] + shape[px] - 1);
        max_y = max_y.max(start[py] + shape[py] - 1);
    }

    start[px] = min_x;
    start[py] = min_y;
    shape[px] = max_x - min_x + 1;
    shape[py] = max_y - min_y + 1;

    Ok(SpatialResolution {
        start,
        shape,
        spatial_shapes: shapes.iter().copied().cloned().collect(),
    })
}

fn shape_candidate_bounds(
    shape: &SpatialShape,
    nx: usize,
    ny: usize,
) -> (usize, usize, usize, usize) {
    let clamp_x = |v: f64| v.clamp(0.0, nx.saturating_sub(1) as f64) as usize;
    let clamp_y = |v: f64| v.clamp(0.0, ny.saturating_sub(1) as f64) as usize;
    match shape {
        SpatialShape::Box([x0, y0, x1, y1]) => (
            (*x0).min(nx.saturating_sub(1)),
            (*y0).min(ny.saturating_sub(1)),
            (*x1).min(nx.saturating_sub(1)),
            (*y1).min(ny.saturating_sub(1)),
        ),
        SpatialShape::CenterBox {
            cx,
            cy,
            half_w,
            half_h,
        } => (
            clamp_x((cx - half_w - 1.0).floor()),
            clamp_y((cy - half_h - 1.0).floor()),
            clamp_x((cx + half_w + 1.0).ceil()),
            clamp_y((cy + half_h + 1.0).ceil()),
        ),
        SpatialShape::RotBox {
            cx,
            cy,
            half_w,
            half_h,
            cos_pa,
            sin_pa,
        } => {
            let dx = half_w * cos_pa.abs() + half_h * sin_pa.abs() + 1.0;
            let dy = half_w * sin_pa.abs() + half_h * cos_pa.abs() + 1.0;
            (
                clamp_x((cx - dx).floor()),
                clamp_y((cy - dy).floor()),
                clamp_x((cx + dx).ceil()),
                clamp_y((cy + dy).ceil()),
            )
        }
        SpatialShape::Circle { cx, cy, rx, ry } => (
            clamp_x((cx - rx - 1.0).floor()),
            clamp_y((cy - ry - 1.0).floor()),
            clamp_x((cx + rx + 1.0).ceil()),
            clamp_y((cy + ry + 1.0).ceil()),
        ),
        SpatialShape::Ellipse {
            cx,
            cy,
            semi_major,
            semi_minor,
            ..
        } => {
            let r = semi_major.max(*semi_minor) + 1.0;
            (
                clamp_x((cx - r).floor()),
                clamp_y((cy - r).floor()),
                clamp_x((cx + r).ceil()),
                clamp_y((cy + r).ceil()),
            )
        }
        SpatialShape::Polygon(verts) => {
            let min_x = verts.iter().map(|v| v[0]).fold(f64::INFINITY, f64::min) - 1.0;
            let min_y = verts.iter().map(|v| v[1]).fold(f64::INFINITY, f64::min) - 1.0;
            let max_x = verts.iter().map(|v| v[0]).fold(f64::NEG_INFINITY, f64::max) + 1.0;
            let max_y = verts.iter().map(|v| v[1]).fold(f64::NEG_INFINITY, f64::max) + 1.0;
            (
                clamp_x(min_x.floor()),
                clamp_y(min_y.floor()),
                clamp_x(max_x.ceil()),
                clamp_y(max_y.ceil()),
            )
        }
    }
}

pub(super) fn shape_contains(shape: &SpatialShape, x: usize, y: usize) -> bool {
    let xf = x as f64;
    let yf = y as f64;
    match shape {
        SpatialShape::Box([x0, y0, x1, y1]) => (*x0..=*x1).contains(&x) && (*y0..=*y1).contains(&y),
        SpatialShape::CenterBox {
            cx,
            cy,
            half_w,
            half_h,
        } => (xf - cx).abs() <= half_w + 1e-9 && (yf - cy).abs() <= half_h + 1e-9,
        SpatialShape::RotBox {
            cx,
            cy,
            half_w,
            half_h,
            cos_pa,
            sin_pa,
        } => {
            let dx = xf - cx;
            let dy = yf - cy;
            let u = dx * cos_pa + dy * sin_pa;
            let v = -dx * sin_pa + dy * cos_pa;
            u.abs() <= half_w + 1e-9 && v.abs() <= half_h + 1e-9
        }
        SpatialShape::Circle { cx, cy, rx, ry } => {
            if *rx <= 0.0 || *ry <= 0.0 {
                return false;
            }
            ((xf - cx) / rx).powi(2) + ((yf - cy) / ry).powi(2) <= 1.0 + 1e-9
        }
        SpatialShape::Ellipse {
            cx,
            cy,
            semi_major,
            semi_minor,
            cos_pa,
            sin_pa,
        } => {
            if *semi_major <= 0.0 || *semi_minor <= 0.0 {
                return false;
            }
            let dx = xf - cx;
            let dy = yf - cy;
            let along_major = -dx * sin_pa + dy * cos_pa;
            let along_minor = dx * cos_pa + dy * sin_pa;
            (along_major / semi_major).powi(2) + (along_minor / semi_minor).powi(2) <= 1.0 + 1e-12
        }
        SpatialShape::Polygon(verts) => point_in_polygon_f64(xf, yf, verts),
    }
}

fn point_in_polygon_f64(x: f64, y: f64, vertices: &[[f64; 2]]) -> bool {
    let mut inside = false;
    let mut prev = vertices[vertices.len() - 1];
    for &cur in vertices {
        let (x0, y0) = (prev[0], prev[1]);
        let (x1, y1) = (cur[0], cur[1]);
        let cross = (x - x0) * (y1 - y0) - (y - y0) * (x1 - x0);
        if cross.abs() <= 1e-9
            && x >= x0.min(x1) - 1e-9
            && x <= x0.max(x1) + 1e-9
            && y >= y0.min(y1) - 1e-9
            && y <= y0.max(y1) + 1e-9
        {
            return true;
        }
        if (y0 > y) != (y1 > y) {
            let x_int = (x1 - x0) * (y - y0) / (y1 - y0) + x0;
            if x <= x_int {
                inside = !inside;
            }
        }
        prev = cur;
    }
    inside
}

struct StokesResolution {
    axis: Option<usize>,
    indices: Option<Vec<usize>>,
    selected: Option<Vec<StokesType>>,
}

fn resolve_stokes(
    image: &dyn ImageInterface<impl ImagePixel>,
    stokes_arg: Option<&str>,
    crtf_stokes: Option<&[usize]>,
    start: &mut [usize],
    shape: &mut [usize],
) -> Result<StokesResolution, ImageError> {
    let arg_trimmed = stokes_arg.map(str::trim).filter(|s| !s.is_empty());
    if arg_trimmed.is_none() && crtf_stokes.is_none() {
        return Ok(StokesResolution {
            axis: None,
            indices: None,
            selected: None,
        });
    }
    let axis = image.find_axis(CoordinateType::Stokes).ok_or_else(|| {
        ImageError::InvalidMetadata("Stokes selection requires a Stokes axis".into())
    })?;
    let mut indices = if let Some(selection) = arg_trimmed {
        parse_stokes_selection(image, selection)?
    } else {
        crtf_stokes.expect("at least one Stokes selection").to_vec()
    };
    indices.sort_unstable();
    indices.dedup();
    let prev_lo = start[axis];
    let prev_hi = start[axis] + shape[axis] - 1;
    let bounded: Vec<usize> = indices
        .into_iter()
        .filter(|&s| s >= prev_lo && s <= prev_hi)
        .collect();
    if bounded.is_empty() {
        return Err(ImageError::CasaSelection(
            "selected Stokes planes lie outside saved region".into(),
        ));
    }
    start[axis] = bounded[0];
    shape[axis] = bounded[bounded.len() - 1] - bounded[0] + 1;
    let full_stokes = stokes_for_axis(image.coordinates(), axis)?;
    let kept = full_stokes[bounded[0]..=bounded[bounded.len() - 1]].to_vec();
    Ok(StokesResolution {
        axis: Some(axis),
        indices: Some(bounded),
        selected: Some(kept),
    })
}

#[cfg(test)]
mod mapped_tests {
    use casa_coordinates::LinearCoordinate;
    use casa_types::measures::frequency::FrequencyRef;

    use super::{CoordinateSystem, SpectralCoordinate, parse_spectral_endpoint};

    #[test]
    fn spectral_selection_uses_the_owning_coordinate_with_independent_axis_orders() {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(LinearCoordinate::new(1, Vec::new(), Vec::new()));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::TOPO,
            1.4e9,
            2e6,
            0.0,
            0.0,
        ));
        coordinates.transpose_axes(&[0, 1], &[1, 0]).unwrap();
        assert_eq!(
            parse_spectral_endpoint("1.404GHz", &coordinates, 0, None).unwrap(),
            2
        );
    }
}
