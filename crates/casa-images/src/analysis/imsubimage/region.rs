// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spatial (`box`, `region` CRTF and saved image regions), spectral (`chans`),
//! and Stokes (`stokes`) selection for `imsubimage`, matching CASA
//! `CasacRegionManager::fromBCS`.

use std::fs;
use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType, StokesType};
use casa_types::ArrayD;
use casa_types::quanta::Quantity;
use casa_types::quanta::casacore::mvangle_radians;
use ndarray::{Array2, Axis, IxDyn, Zip};

use crate::analysis::axes::{direction_plane_axes, stokes_for_axis};
use crate::analysis::{
    PixelRegion, parse_box_region, parse_image_channel_selection, parse_stokes_selection,
};
use crate::{ImageError, ImageInterface, ImagePixel};

mod saved;

use saved::{load_saved_region, parse_saved_region};

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
}

impl From<SpatialShape> for QualifiedShape {
    fn from(spatial: SpatialShape) -> Self {
        Self {
            spatial,
            planes: PlaneSelection::default(),
        }
    }
}

pub(super) struct SubimageSelection {
    pub(super) start: Vec<usize>,
    pub(super) shape: Vec<usize>,
    pub(super) plane: [usize; 2],
    /// Precomputed spatial mask (shape `[shape[plane[0]], shape[plane[1]]]`) if
    /// the selection is non-rectangular or a union of multiple shapes.
    pub(super) spatial_mask: Option<Array2<bool>>,
    qualified_shapes: Vec<QualifiedShape>,
    /// Full-dimensional mask from saved `WCPolygon` / `WCUnion`, if any,
    /// covering the entire parent image (sliced lazily to `[start, shape]`).
    pub(super) saved_polygon_mask: Option<ArrayD<bool>>,
    pub(super) channel_axis: Option<usize>,
    pub(super) channel_indices: Option<Vec<usize>>,
    pub(super) stokes_axis: Option<usize>,
    pub(super) stokes_indices: Option<Vec<usize>>,
    pub(super) selected_stokes: Option<Vec<StokesType>>,
}

impl SubimageSelection {
    pub(super) fn resolve<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        box_arg: Option<&str>,
        region_arg: Option<&str>,
        chans_arg: Option<&str>,
        stokes_arg: Option<&str>,
    ) -> Result<Self, ImageError> {
        let ndim = image.ndim();
        let full_shape = image.shape().to_vec();
        let box_arg = box_arg.map(str::trim).filter(|s| !s.is_empty());
        let region_arg = region_arg.map(str::trim).filter(|s| !s.is_empty());
        let plane = if box_arg.is_some() || region_arg.is_some() {
            direction_plane_axes(image)?
        } else {
            [0, 1]
        };

        let mut regions: Vec<QualifiedShape> = Vec::new();
        let mut saved_polygon_mask: Option<ArrayD<bool>> = None;
        let mut saved_bounds: Option<(Vec<usize>, Vec<usize>)> = None;

        if let Some(box_text) = box_arg {
            let PixelRegion::Boxes(boxes) = parse_box_region(box_text, &full_shape, plane)? else {
                unreachable!("parse_box_region returns Boxes")
            };
            for b in boxes {
                regions.push(SpatialShape::Box(b).into());
            }
        }

        if let Some(reg_text) = region_arg {
            let parsed = parse_region_argument(image, reg_text, &full_shape, plane)?;
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
            regions.extend(parsed.shapes);
            saved_polygon_mask = parsed.saved_polygon_mask;
            saved_bounds = parsed.saved_bounds;
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
            .map(|region| &region.spatial)
            .collect::<Vec<_>>();
        let SpatialResolution {
            mut start,
            mut shape,
            spatial_mask,
        } = resolve_spatial_and_saved_bounds(
            &full_shape,
            ndim,
            plane,
            &spatial_shapes,
            saved_bounds.as_ref(),
            saved_polygon_mask.as_ref(),
        )?;

        let (channel_axis, channel_indices) = resolve_channels(
            image,
            &full_shape,
            chans_arg,
            channel_union.as_deref(),
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

        Ok(Self {
            start,
            shape,
            plane,
            spatial_mask,
            qualified_shapes: if regions
                .iter()
                .any(|r| r.planes.channels.is_some() || r.planes.stokes.is_some())
            {
                regions
            } else {
                Vec::new()
            },
            saved_polygon_mask,
            channel_axis: channel_axis.or_else(|| image.find_axis(CoordinateType::Spectral)),
            channel_indices,
            stokes_axis: stokes_axis.or_else(|| image.find_axis(CoordinateType::Stokes)),
            stokes_indices,
            selected_stokes,
        })
    }

    pub(super) fn apply_selection_masks(&self, mask: &mut Option<ArrayD<bool>>) {
        let [px, py] = self.plane;
        let box_mask = self.spatial_mask.as_ref().map(|spatial| {
            ArrayD::from_shape_fn(IxDyn(&self.shape), |idx| spatial[[idx[px], idx[py]]])
        });
        let poly_mask = self.saved_polygon_mask.as_ref().map(|saved_poly| {
            let slice_elems: Vec<ndarray::SliceInfoElem> = self
                .start
                .iter()
                .zip(self.shape.iter())
                .map(|(&s, &len)| ndarray::SliceInfoElem::Slice {
                    start: s as isize,
                    end: Some((s + len) as isize),
                    step: 1,
                })
                .collect();
            saved_poly.slice(slice_elems.as_slice()).to_owned()
        });

        match (box_mask, poly_mask) {
            (Some(mut bm), Some(pm)) => {
                // `box` and `region` combine by spatial union (CASA fromBCS).
                Zip::from(&mut bm).and(&pm).for_each(|b, &p| *b = *b || p);
                if !bm.iter().all(|&v| v) {
                    and_mask(mask, &bm);
                }
            }
            (Some(bm), None) => and_mask(mask, &bm),
            (None, Some(pm)) => {
                if !pm.iter().all(|&v| v) {
                    and_mask(mask, &pm);
                }
            }
            (None, None) => {}
        }

        if !self.qualified_shapes.is_empty() {
            let qualified = ArrayD::from_shape_fn(IxDyn(&self.shape), |idx| {
                let x = self.start[px] + idx[px];
                let y = self.start[py] + idx[py];
                self.qualified_shapes.iter().any(|region| {
                    let contains = |axis: Option<usize>, selected: Option<&[usize]>| {
                        axis.zip(selected).is_none_or(|(axis, selected)| {
                            selected.contains(&(self.start[axis] + idx[axis]))
                        })
                    };
                    contains(self.channel_axis, region.planes.channels.as_deref())
                        && contains(self.stokes_axis, region.planes.stokes.as_deref())
                        && shape_contains(&region.spatial, x, y)
                })
            });
            if qualified.iter().any(|&value| !value) {
                and_mask(mask, &qualified);
            }
        }

        if let (Some(ax), Some(indices)) = (self.channel_axis, &self.channel_indices)
            && indices.len() != self.shape[ax]
        {
            mask_skipped_axis(mask, &self.shape, ax, self.start[ax], indices);
        }

        if let (Some(ax), Some(indices)) = (self.stokes_axis, &self.stokes_indices)
            && indices.len() != self.shape[ax]
        {
            mask_skipped_axis(mask, &self.shape, ax, self.start[ax], indices);
        }
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
    for shape in shapes {
        union.extend_from_slice(indices(shape)?);
    }
    Some(union)
}

fn and_mask(dst: &mut Option<ArrayD<bool>>, rhs: &ArrayD<bool>) {
    *dst = Some(match dst.take() {
        Some(mut lhs) => {
            Zip::from(&mut lhs).and(rhs).for_each(|a, &b| *a = *a && b);
            lhs
        }
        None => rhs.clone(),
    });
}

fn mask_skipped_axis(
    mask: &mut Option<ArrayD<bool>>,
    shape: &[usize],
    axis: usize,
    start: usize,
    kept: &[usize],
) {
    let mut m = mask
        .take()
        .unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape), true));
    for (local, mut slice) in m.axis_iter_mut(Axis(axis)).enumerate() {
        if !kept.contains(&(start + local)) {
            slice.fill(false);
        }
    }
    *mask = Some(m);
}

struct ParsedRegion {
    shapes: Vec<QualifiedShape>,
    saved_polygon_mask: Option<ArrayD<bool>>,
    saved_bounds: Option<(Vec<usize>, Vec<usize>)>,
}

fn parse_region_argument<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    text: &str,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<ParsedRegion, ImageError> {
    if let Some((img_path_str, reg_name)) = text.rsplit_once(':')
        && !img_path_str.is_empty()
        && !reg_name.trim().is_empty()
        && Path::new(img_path_str).is_dir()
    {
        return load_saved_region(
            Path::new(img_path_str),
            reg_name.trim(),
            image.coordinates(),
            full_shape,
            plane,
        );
    }

    if let Some(box_text) = text.strip_prefix("box:") {
        let b = parse_single_box(box_text.trim(), full_shape, plane)?;
        return Ok(ParsedRegion {
            shapes: vec![SpatialShape::Box(b).into()],
            saved_polygon_mask: None,
            saved_bounds: None,
        });
    }
    if let Some(inner) = text
        .strip_prefix("pixelbox(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let b = parse_single_box(inner.trim(), full_shape, plane)?;
        return Ok(ParsedRegion {
            shapes: vec![SpatialShape::Box(b).into()],
            saved_polygon_mask: None,
            saved_bounds: None,
        });
    }
    if looks_like_four_ints(text) {
        let b = parse_single_box(text, full_shape, plane)?;
        return Ok(ParsedRegion {
            shapes: vec![SpatialShape::Box(b).into()],
            saved_polygon_mask: None,
            saved_bounds: None,
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
    } else if looks_like_inline_crtf(text) {
        text.to_string()
    } else {
        return Err(ImageError::CasaSelection(format!(
            "Unable to open region file or region table description {text}."
        )));
    };

    parse_crtf_document(image, &crtf_text, full_shape, plane)
}

fn looks_like_four_ints(text: &str) -> bool {
    let parts: Vec<&str> = text.split(',').collect();
    parts.len() == 4 && parts.iter().all(|p| p.trim().parse::<usize>().is_ok())
}

fn looks_like_inline_crtf(text: &str) -> bool {
    let lower = text.trim_start().to_ascii_lowercase();
    [
        "box",
        "centerbox",
        "rotbox",
        "circle",
        "ellipse",
        "poly",
        "#crtf",
    ]
    .iter()
    .any(|kw| lower.starts_with(kw))
        || text.contains('\n')
        || text.contains("[[")
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

fn parse_crtf_document<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    text: &str,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<ParsedRegion, ImageError> {
    let mut shapes = Vec::new();
    let mut defaults = PlaneSelection::default();

    for raw_stmt in text.split(['\n', ';']) {
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
            )?;
            continue;
        }
        let tokens = split_top_level(stmt, ',');
        let Some(first) = tokens.first() else {
            continue;
        };
        let spatial = parse_crtf_shape(image, first, plane)?;
        let mut planes = defaults.clone();
        parse_crtf_qualifiers(&tokens[1..], image, full_shape, &mut planes)?;
        shapes.push(QualifiedShape { spatial, planes });
    }

    if shapes.is_empty() {
        return Err(ImageError::CasaSelection(format!(
            "No valid CRTF region definitions found in {text:?}"
        )));
    }
    Ok(ParsedRegion {
        shapes,
        saved_polygon_mask: None,
        saved_bounds: None,
    })
}

fn parse_crtf_qualifiers<T: ImagePixel>(
    tokens: &[&str],
    image: &dyn ImageInterface<T>,
    full_shape: &[usize],
    planes: &mut PlaneSelection,
) -> Result<(), ImageError> {
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
                planes.stokes = Some(parse_stokes_selection(image, &joined)?);
            }
        } else if key == "range" {
            let spec_ax = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
                ImageError::InvalidMetadata("CRTF range requires a spectral axis".into())
            })?;
            let indices =
                parse_crtf_spectral_range(val, image.coordinates(), spec_ax, full_shape[spec_ax])?;
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
) -> Result<Vec<usize>, ImageError> {
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
    let c0 = parse_spectral_endpoint(parts[0], coords, spec_axis)?;
    let c1 = parse_spectral_endpoint(parts[1], coords, spec_axis)?;
    let lo = c0.min(c1).max(0);
    let hi = c0.max(c1).min(axis_len as isize - 1);
    if hi < lo || lo >= axis_len as isize {
        return Err(ImageError::CasaSelection(format!(
            "CRTF spectral range [{inner}] selects no channels in 0..{axis_len}"
        )));
    }
    Ok((lo as usize..=hi as usize).collect())
}

fn parse_spectral_endpoint(
    text: &str,
    coords: &CoordinateSystem,
    spec_axis: usize,
) -> Result<isize, ImageError> {
    let lower = text.trim().to_ascii_lowercase();
    if let Some(ch_str) = lower
        .strip_suffix("chan")
        .or_else(|| lower.strip_suffix("pix"))
    {
        return ch_str
            .trim()
            .parse::<f64>()
            .map(|v| v.round() as isize)
            .map_err(|e| ImageError::InvalidMetadata(format!("invalid channel {text:?}: {e}")));
    }
    if let Ok(ch) = lower.parse::<isize>() {
        return Ok(ch);
    }
    let q = Quantity::read(text.trim())
        .map_err(|e| ImageError::InvalidMetadata(format!("invalid CRTF range {text:?}: {e}")))?;
    let freq_hz = q.get_si_value();
    let mut world = coords.to_world(&vec![0.0; coords.n_pixel_axes()])?;
    world[spec_axis] = freq_hz;
    let pix = coords.to_pixel(&world)?;
    Ok(pix[spec_axis].round() as isize)
}

fn parse_crtf_shape<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    stmt: &str,
    plane: [usize; 2],
) -> Result<SpatialShape, ImageError> {
    let trimmed = stmt.trim();
    let Some(bracket) = trimmed.find('[') else {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF region syntax: {trimmed:?}"
        )));
    };
    let keyword = trimmed[..bracket].trim().to_ascii_lowercase();
    let body = trimmed[bracket..]
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
            let [x0, y0] = crtf_point_to_pixel(image, args[0], plane)?;
            let [x1, y1] = crtf_point_to_pixel(image, args[1], plane)?;
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
            let [cx, cy] = crtf_point_to_pixel(image, args[0], plane)?;
            let [wx, wy] = crtf_extent_pair_to_pixels(image, args[1], plane)?;
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
            let [cx, cy] = crtf_point_to_pixel(image, args[0], plane)?;
            let [wx, wy] = crtf_extent_pair_to_pixels(image, args[1], plane)?;
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
            let [cx, cy] = crtf_point_to_pixel(image, args[0], plane)?;
            let [rx, ry] = crtf_radius_to_pixels(image, args[1], plane)?;
            Ok(SpatialShape::Circle { cx, cy, rx, ry })
        }
        "ellipse" => {
            if args.len() != 3 {
                return Err(ImageError::InvalidMetadata(format!(
                    "invalid CRTF ellipse {trimmed:?}"
                )));
            }
            let [cx, cy] = crtf_point_to_pixel(image, args[0], plane)?;
            let [a, b] = crtf_extent_pair_to_pixels(image, args[1], plane)?;
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
                verts.push(crtf_point_to_pixel(image, arg, plane)?);
            }
            Ok(SpatialShape::Polygon(verts))
        }
        _ => Err(ImageError::InvalidMetadata(format!(
            "unsupported CRTF region keyword {keyword:?}"
        ))),
    }
}

fn crtf_point_to_pixel<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
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
            let coords = image.coordinates();
            if coords.n_pixel_axes() < 2 || coords.n_world_axes() < 2 {
                return Err(ImageError::InvalidMetadata(
                    "world CRTF regions require at least 2 coordinate axes".into(),
                ));
            }
            let [ax, ay] = plane;
            let base = vec![0.0; coords.n_pixel_axes()];
            let mut world = coords.to_world(&base)?;
            world[ax] = parse_world_angle(parts[0])?;
            world[ay] = parse_world_angle(parts[1])?;
            let pix = coords.to_pixel(&world)?;
            Ok([pix[ax], pix[ay]])
        }
        _ => Err(ImageError::InvalidMetadata(
            "cannot mix pix and world coordinates in a CRTF point".into(),
        )),
    }
}

fn crtf_extent_pair_to_pixels<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    pair_text: &str,
    plane: [usize; 2],
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
    let scale = direction_pixel_scales(image.coordinates(), plane);
    Ok([
        parse_extent_single(parts[0], scale.map(|s| s[0]))?,
        parse_extent_single(parts[1], scale.map(|s| s[1]))?,
    ])
}

fn crtf_radius_to_pixels<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    text: &str,
    plane: [usize; 2],
) -> Result<[f64; 2], ImageError> {
    if let Some(r) = parse_crtf_pix(text)? {
        return Ok([r, r]);
    }
    let rad = parse_world_angle(text)?;
    let [sx, sy] = direction_pixel_scales(image.coordinates(), plane).ok_or_else(|| {
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

fn direction_pixel_scales(coords: &CoordinateSystem, plane: [usize; 2]) -> Option<[f64; 2]> {
    let mut offset = 0usize;
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
        offset += np;
    }
    let _ = offset;
    let _ = plane;
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

fn parse_world_angle(text: &str) -> Result<f64, ImageError> {
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
    spatial_mask: Option<Array2<bool>>,
}

fn resolve_spatial_and_saved_bounds(
    full_shape: &[usize],
    ndim: usize,
    plane: [usize; 2],
    shapes: &[&SpatialShape],
    saved_bounds: Option<&(Vec<usize>, Vec<usize>)>,
    saved_polygon_mask: Option<&ArrayD<bool>>,
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
            spatial_mask: None,
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
        && saved_polygon_mask.is_none()
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
            spatial_mask: None,
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
        return Err(ImageError::CasaSelection(
            "selected region contains no pixels inside the image".into(),
        ));
    }

    if saved_polygon_mask.is_some() {
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

    let spatial = Array2::from_shape_fn((shape[px], shape[py]), |(lx, ly)| {
        let gx = min_x + lx;
        let gy = min_y + ly;
        shapes.iter().any(|s| shape_contains(s, gx, gy))
    });
    let spatial = (!spatial.iter().all(|&v| v)).then_some(spatial);
    Ok(SpatialResolution {
        start,
        shape,
        spatial_mask: spatial,
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

fn shape_contains(shape: &SpatialShape, x: usize, y: usize) -> bool {
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

fn resolve_channels<T: ImagePixel>(
    image: &dyn ImageInterface<T>,
    full_shape: &[usize],
    chans_arg: Option<&str>,
    crtf_chans: Option<&[usize]>,
    start: &mut [usize],
    shape: &mut [usize],
) -> Result<(Option<usize>, Option<Vec<usize>>), ImageError> {
    let arg_trimmed = chans_arg.map(str::trim).filter(|s| !s.is_empty());
    if arg_trimmed.is_none() && crtf_chans.is_none() {
        return Ok((None, None));
    }
    let axis = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
        ImageError::InvalidMetadata("channel selection requires a spectral axis".into())
    })?;
    let mut indices = match (arg_trimmed, crtf_chans) {
        (Some(text), None) => parse_image_channel_selection(text, full_shape[axis])?,
        (None, Some(c)) => c.to_vec(),
        (Some(text), Some(c)) => {
            let from_arg = parse_image_channel_selection(text, full_shape[axis])?;
            let intersected: Vec<usize> =
                from_arg.into_iter().filter(|idx| c.contains(idx)).collect();
            if intersected.is_empty() {
                return Err(ImageError::CasaSelection(
                    "channel selection and CRTF range have empty intersection".into(),
                ));
            }
            intersected
        }
        (None, None) => unreachable!(),
    };
    indices.sort_unstable();
    indices.dedup();
    // Intersect with any saved-region bounds already recorded on this axis.
    let prev_lo = start[axis];
    let prev_hi = start[axis] + shape[axis] - 1;
    let bounded: Vec<usize> = indices
        .into_iter()
        .filter(|&c| c >= prev_lo && c <= prev_hi)
        .collect();
    if bounded.is_empty() {
        return Err(ImageError::CasaSelection(
            "selected channels lie outside saved region".into(),
        ));
    }
    start[axis] = bounded[0];
    shape[axis] = bounded[bounded.len() - 1] - bounded[0] + 1;
    Ok((Some(axis), Some(bounded)))
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
