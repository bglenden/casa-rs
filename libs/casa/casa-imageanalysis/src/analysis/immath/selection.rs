// SPDX-License-Identifier: LGPL-3.0-or-later
//! Input loading (CASA tables and FITS), spatial/spectral/Stokes/mask selection,
//! and varname resolution for `immath`.

use std::any::Any;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use casa_coordinates::{CoordinateSystem, CoordinateType, StokesType};
use casa_core::{ArrayD, RecordValue, Value};
use casa_lattices::{Lattice, LatticeError};
use ndarray::{Axis, IxDyn, Slice, SliceInfoElem, Zip};

use casa_images::can_broadcast_to;
use casa_images::expr_parser::referenced_images;
use casa_images::expr_parser::{ExprValueConvert, HashMapResolver, parse_mask_expr};
use casa_images::{
    AnyPagedImage, ImageError, ImageInfo, ImageInterface, ImagePixel, PagedImage, TempImage,
    TempStoragePolicy, read_fits_image,
};
use casa_images::{ImageExprValue, MemorySnapshotImage};

use super::super::axes::{
    legacy_subimage_coordinates_with_stokes, stokes_for_axis, subimage_coordinates_with_stokes,
};
use super::super::imsubimage::{SubimagePlan, SubimageRegion, region::SubimageSelection};
use super::super::{
    PixelRegion, direction_plane_axes, mask_error_wording, parse_image_channel_selection,
    parse_stokes_selection, region_pixels,
};
use crate::ImmathRequest;

pub(super) trait RealMathPixel:
    ImagePixel + ImageExprValue + PartialOrd + ExprValueConvert + Clone + 'static
{
    fn from_f64_pixel(v: f64) -> Self;
}

impl RealMathPixel for f32 {
    fn from_f64_pixel(v: f64) -> Self {
        v as f32
    }
}

impl RealMathPixel for f64 {
    fn from_f64_pixel(v: f64) -> Self {
        v
    }
}

pub(super) enum RawImage<T: RealMathPixel> {
    Paged {
        image: Box<PagedImage<T>>,
        legacy_coords: Option<RecordValue>,
    },
    Memory(Box<TempImage<T>>),
}

impl<T: RealMathPixel> RawImage<T> {
    pub(super) fn as_interface(&self) -> &dyn ImageInterface<T> {
        match self {
            Self::Paged { image, .. } => image.as_ref(),
            Self::Memory(image) => image.as_ref(),
        }
    }

    pub(super) fn legacy_coords(&self) -> Option<&RecordValue> {
        match self {
            Self::Paged { legacy_coords, .. } => legacy_coords.as_ref(),
            Self::Memory(_) => None,
        }
    }
}

pub(super) fn open_raw_image<T: RealMathPixel>(path: &Path) -> Result<RawImage<T>, ImageError> {
    if path.is_file() {
        let fits = read_fits_image(path)?;
        let pixels = fits.pixels.mapv(|v| T::from_f64_pixel(f64::from(v)));
        let has_nan = fits.pixels.iter().any(|v| v.is_nan());
        let mut temp = TempImage::new(fits.shape, fits.coordinates, TempStoragePolicy::Memory)?;
        temp.set_units(fits.units)?;
        temp.set_image_info(&fits.image_info)?;
        temp.put_slice(&pixels, &vec![0; temp.ndim()])?;
        if has_nan {
            let mask = fits.pixels.mapv(|v| !v.is_nan());
            temp.put_mask("mask0", &mask)?;
            temp.set_default_mask("mask0")?;
        }
        return Ok(RawImage::Memory(Box::new(temp)));
    }

    match AnyPagedImage::open(path)? {
        AnyPagedImage::Float32(img) => cast_or_wrap_paged::<f32, T>(img, f64::from),
        AnyPagedImage::Float64(img) => cast_or_wrap_paged::<f64, T>(img, |v| v),
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => Err(
            ImageError::InvalidMetadata("immath requires real-valued input images".into()),
        ),
    }
}

fn cast_or_wrap_paged<S: ImagePixel + 'static, T: RealMathPixel>(
    img: PagedImage<S>,
    to_f64: impl Fn(S) -> f64,
) -> Result<RawImage<T>, ImageError> {
    let legacy_coords = img
        .table()
        .keywords()
        .get("coords")
        .cloned()
        .and_then(|v| match v {
            Value::Record(r) => Some(r),
            _ => None,
        });
    let boxed: Box<dyn std::any::Any> = Box::new(img);
    match boxed.downcast::<PagedImage<T>>() {
        Ok(typed) => Ok(RawImage::Paged {
            image: typed,
            legacy_coords,
        }),
        Err(original) => {
            let img = *original.downcast::<PagedImage<S>>().expect("original type");
            let data = img.get()?;
            let converted = data.mapv(|v| T::from_f64_pixel(to_f64(v)));
            let mask = img.default_mask()?;
            let mut temp = TempImage::new(
                img.shape().to_vec(),
                img.coordinates().clone(),
                TempStoragePolicy::Memory,
            )?;
            temp.set_units(img.units())?;
            temp.set_image_info(&img.image_info()?)?;
            temp.set_misc_info(img.misc_info())?;
            temp.put_slice(&converted, &vec![0; temp.ndim()])?;
            if let Some(m) = mask {
                temp.put_mask("mask0", &m)?;
                temp.set_default_mask("mask0")?;
            }
            Ok(RawImage::Memory(Box::new(temp)))
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct BoxSpec {
    pub(super) bounds: [usize; 4],
    pub(super) boxes: Vec<[usize; 4]>,
}

pub(super) fn parse_multi_box(text: &str) -> Result<BoxSpec, ImageError> {
    let values = text
        .split(',')
        .map(|part| {
            part.trim().parse::<usize>().map_err(|error| {
                ImageError::InvalidMetadata(format!("invalid box {text:?}: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.len() < 4 || values.len() % 4 != 0 {
        return Err(ImageError::InvalidMetadata(format!(
            "box must contain comma-separated x0,y0,x1,y1 quadruplets: {text:?}"
        )));
    }
    let mut boxes = Vec::with_capacity(values.len() / 4);
    let mut min_x0 = usize::MAX;
    let mut min_y0 = usize::MAX;
    let mut max_x1 = 0usize;
    let mut max_y1 = 0usize;
    for &[x0, y0, x1, y1] in values.as_chunks::<4>().0 {
        if x1 < x0 || y1 < y0 {
            return Err(ImageError::InvalidMetadata(format!(
                "box must have x0 <= x1 and y0 <= y1: {text:?}"
            )));
        }
        min_x0 = min_x0.min(x0);
        min_y0 = min_y0.min(y0);
        max_x1 = max_x1.max(x1);
        max_y1 = max_y1.max(y1);
        boxes.push([x0, y0, x1, y1]);
    }
    Ok(BoxSpec {
        bounds: [min_x0, min_y0, max_x1, max_y1],
        boxes,
    })
}

pub(super) fn parse_chans_extended(text: &str, axis_len: usize) -> Result<Vec<usize>, ImageError> {
    if !text.contains('<') && !text.contains('>') {
        return parse_image_channel_selection(text, axis_len);
    }
    let mut indices = Vec::new();
    for segment in text.split([',', ';']).filter(|s| !s.trim().is_empty()) {
        let seg = segment.trim();
        if let Some(rest) = seg.strip_prefix("<=") {
            let n = rest.trim().parse::<usize>().map_err(|e| {
                ImageError::InvalidMetadata(format!("invalid channel limit {seg:?}: {e}"))
            })?;
            indices.extend(0..=(n.min(axis_len.saturating_sub(1))));
        } else if let Some(rest) = seg.strip_prefix('<') {
            let n = rest.trim().parse::<usize>().map_err(|e| {
                ImageError::InvalidMetadata(format!("invalid channel limit {seg:?}: {e}"))
            })?;
            indices.extend(0..n.min(axis_len));
        } else if let Some(rest) = seg.strip_prefix(">=") {
            let n = rest.trim().parse::<usize>().map_err(|e| {
                ImageError::InvalidMetadata(format!("invalid channel limit {seg:?}: {e}"))
            })?;
            if n < axis_len {
                indices.extend(n..axis_len);
            }
        } else if let Some(rest) = seg.strip_prefix('>') {
            let n = rest.trim().parse::<usize>().map_err(|e| {
                ImageError::InvalidMetadata(format!("invalid channel limit {seg:?}: {e}"))
            })?;
            if n.saturating_add(1) < axis_len {
                indices.extend((n + 1)..axis_len);
            }
        } else {
            indices.extend(parse_image_channel_selection(seg, axis_len)?);
        }
    }
    if indices.is_empty() || indices.iter().any(|&i| i >= axis_len) {
        return Err(ImageError::ShapeMismatch {
            expected: vec![axis_len],
            got: indices,
        });
    }
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

#[derive(Clone)]
pub(super) struct ImmathSelection {
    pub(super) start: Vec<usize>,
    pub(super) shape: Vec<usize>,
    pub(super) plane: [usize; 2],
    pub(super) box_spec: Option<BoxSpec>,
    pub(super) region: Option<PixelRegion>,
    saved_mask: Option<ArrayD<bool>>,
    strided_plan: Option<(Arc<SubimagePlan>, Vec<usize>)>,
    pub(super) channel_axis: Option<usize>,
    pub(super) channel_indices: Option<Vec<usize>>,
    pub(super) stokes_axis: Option<usize>,
    pub(super) stokes_indices: Option<Vec<usize>>,
    pub(super) selected_stokes: Option<Vec<StokesType>>,
}

impl ImmathSelection {
    pub(super) fn from_raw<T: RealMathPixel>(
        image: &RawImage<T>,
        request: &ImmathRequest,
        ignore_stokes: bool,
    ) -> Result<Self, ImageError> {
        if let Some(path) = request
            .region
            .as_deref()
            .map(Path::new)
            .filter(|path| path.is_file())
            && let Ok(record) = casa_tables::read_table_record_file(path)
        {
            let saved = SubimageSelection::resolve_section(
                image.as_interface(),
                request.box_pixels.as_deref(),
                Some(SubimageRegion::Record(&record)),
                request.chans.as_deref(),
                if ignore_stokes {
                    None
                } else {
                    request.stokes.as_deref()
                },
            )?;
            if let Some(stride) = &saved.stride
                && stride.iter().any(|&step| step > 1)
            {
                if ignore_stokes {
                    return Err(ImageError::CasaSelection(
                        "strided saved region is unsupported for polarization modes".into(),
                    ));
                }
                let start = saved.start.clone();
                let shape = saved.shape.clone();
                let plane = saved.plane;
                let stride = stride.clone();
                let plan = SubimagePlan::from_selection(
                    image.as_interface(),
                    image.legacy_coords().cloned(),
                    saved,
                    false,
                    &[],
                    None,
                )?;
                return Ok(Self {
                    start,
                    shape,
                    plane,
                    box_spec: None,
                    region: None,
                    saved_mask: None,
                    strided_plan: Some((Arc::new(plan), stride)),
                    channel_axis: None,
                    channel_indices: None,
                    stokes_axis: None,
                    stokes_indices: None,
                    selected_stokes: None,
                });
            }
            let mut saved_mask = None;
            saved.apply_selection_masks(&mut saved_mask);
            return Ok(Self {
                start: saved.start,
                shape: saved.shape,
                plane: saved.plane,
                box_spec: None,
                region: None,
                saved_mask,
                strided_plan: None,
                channel_axis: saved.channel_axis,
                channel_indices: (!saved.channel_spans.is_empty()).then(|| {
                    let mut indices = saved
                        .channel_spans
                        .iter()
                        .copied()
                        .flat_map(crate::analysis::ChannelSpan::indices)
                        .collect::<Vec<_>>();
                    indices.sort_unstable();
                    indices.dedup();
                    indices
                }),
                stokes_axis: saved.stokes_axis,
                stokes_indices: saved.stokes_indices,
                selected_stokes: saved.selected_stokes,
            });
        }
        let ndim = image.ndim();
        let full_shape = image.shape();
        let mut start = vec![0usize; ndim];
        let mut shape = full_shape.to_vec();

        let box_spec = request
            .box_pixels
            .as_deref()
            .filter(|t| !t.trim().is_empty())
            .map(parse_multi_box)
            .transpose()?;

        let region = region_pixels(image, request.region.as_deref())?;
        let plane = if box_spec.is_some() || region.is_some() {
            direction_plane_axes(image)?
        } else {
            [0, 1]
        };

        if let Some(ref b) = box_spec {
            apply_box_bounds(&b.bounds, ndim, full_shape, plane, &mut start, &mut shape)?;
        }
        if let Some(ref reg) = region {
            let reg_bounds = reg.bounding_box();
            apply_intersect_bounds(
                &reg_bounds,
                ndim,
                full_shape,
                plane,
                &mut start,
                &mut shape,
                box_spec.is_some(),
            )?;
        }

        let (channel_axis, channel_indices) =
            if let Some(chans) = request.chans.as_deref().filter(|s| !s.trim().is_empty()) {
                let axis = image.find_axis(CoordinateType::Spectral).ok_or_else(|| {
                    ImageError::InvalidMetadata("channel selection requires a spectral axis".into())
                })?;
                let indices = parse_chans_extended(chans, full_shape[axis])?;
                start[axis] = indices[0];
                shape[axis] = indices[indices.len() - 1] - indices[0] + 1;
                (Some(axis), Some(indices))
            } else {
                (None, None)
            };

        let (stokes_axis, stokes_indices, selected_stokes) = if ignore_stokes {
            (None, None, None)
        } else if let Some(st) = request.stokes.as_deref().filter(|s| !s.trim().is_empty()) {
            let indices = parse_stokes_selection(image, st)?;
            let axis = image
                .find_axis(CoordinateType::Stokes)
                .expect("stokes axis exists");
            start[axis] = indices[0];
            shape[axis] = indices[indices.len() - 1] - indices[0] + 1;
            let stokes_list = stokes_for_axis(image.coordinates(), axis)?;
            let kept = stokes_list[indices[0]..=indices[indices.len() - 1]].to_vec();
            (Some(axis), Some(indices), Some(kept))
        } else {
            (None, None, None)
        };

        Ok(Self {
            start,
            shape,
            plane,
            box_spec,
            region,
            saved_mask: None,
            strided_plan: None,
            channel_axis,
            channel_indices,
            stokes_axis,
            stokes_indices,
            selected_stokes,
        })
    }

    pub(super) fn is_full_contiguous(&self, full_shape: &[usize]) -> bool {
        self.start.iter().all(|&s| s == 0)
            && self.shape == full_shape
            && self.saved_mask.is_none()
            && self.strided_plan.is_none()
            && self.box_spec.as_ref().is_none_or(|b| b.boxes.len() <= 1)
            && !matches!(self.region, Some(PixelRegion::Polygon(_)))
            && !matches!(self.region, Some(PixelRegion::Boxes(ref boxes)) if boxes.len() > 1)
            && !self.has_channel_gaps()
            && !self.has_stokes_gaps()
    }

    fn has_channel_gaps(&self) -> bool {
        match (self.channel_axis, &self.channel_indices) {
            (Some(ax), Some(indices)) => indices.len() != self.shape[ax],
            _ => false,
        }
    }

    fn has_stokes_gaps(&self) -> bool {
        match (self.stokes_axis, &self.stokes_indices) {
            (Some(ax), Some(indices)) => indices.len() != self.shape[ax],
            _ => false,
        }
    }
}

fn apply_box_bounds(
    bounds: &[usize; 4],
    ndim: usize,
    full_shape: &[usize],
    plane: [usize; 2],
    start: &mut [usize],
    shape: &mut [usize],
) -> Result<(), ImageError> {
    let [x0, y0, x1, y1] = *bounds;
    let [x, y] = plane;
    if ndim < 2 || x1 >= full_shape[x] || y1 >= full_shape[y] {
        return Err(ImageError::ShapeMismatch {
            expected: full_shape.to_vec(),
            got: vec![x0, y0, x1, y1],
        });
    }
    start[x] = x0;
    start[y] = y0;
    shape[x] = x1 - x0 + 1;
    shape[y] = y1 - y0 + 1;
    Ok(())
}

fn apply_intersect_bounds(
    bounds: &[usize; 4],
    ndim: usize,
    full_shape: &[usize],
    plane: [usize; 2],
    start: &mut [usize],
    shape: &mut [usize],
    intersect: bool,
) -> Result<(), ImageError> {
    let [x0, y0, x1, y1] = *bounds;
    let [x, y] = plane;
    if ndim < 2 || x1 >= full_shape[x] || y1 >= full_shape[y] {
        return Err(ImageError::ShapeMismatch {
            expected: full_shape.to_vec(),
            got: vec![x0, y0, x1, y1],
        });
    }
    if !intersect {
        start[x] = x0;
        start[y] = y0;
        shape[x] = x1 - x0 + 1;
        shape[y] = y1 - y0 + 1;
        return Ok(());
    }
    let sx = start[x].max(x0);
    let ex = (start[x] + shape[x] - 1).min(x1);
    let sy = start[y].max(y0);
    let ey = (start[y] + shape[y] - 1).min(y1);
    if ex < sx || ey < sy {
        return Err(ImageError::CasaSelection(
            "selected box and region do not overlap".into(),
        ));
    }
    start[x] = sx;
    start[y] = sy;
    shape[x] = ex - sx + 1;
    shape[y] = ey - sy + 1;
    Ok(())
}

pub(super) fn slice_array_subimage<A: Clone>(
    arr: &ArrayD<A>,
    start: &[usize],
    shape: &[usize],
) -> ArrayD<A> {
    let slices: Vec<SliceInfoElem> = start
        .iter()
        .zip(shape.iter())
        .map(|(&s, &len)| SliceInfoElem::Slice {
            start: s as isize,
            end: Some((s + len) as isize),
            step: 1,
        })
        .collect();
    arr.slice(slices.as_slice()).to_owned()
}

pub(super) fn selection_and_mask<T: RealMathPixel>(
    image: &dyn ImageInterface<T>,
    sel: &ImmathSelection,
    window_mask_override: Option<ArrayD<bool>>,
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let stride = vec![1; image.ndim()];
    let mut mask = if let Some(wm) = window_mask_override {
        if wm.shape() == sel.shape.as_slice() {
            Some(wm)
        } else {
            Some(slice_array_subimage(&wm, &sel.start, &sel.shape))
        }
    } else {
        None
    };

    if let Some(native_mask) = image.get_mask_slice(&sel.start, &sel.shape, &stride)? {
        and_mask(&mut mask, &native_mask);
    }
    if let Some(saved_mask) = &sel.saved_mask {
        and_mask(&mut mask, saved_mask);
    }

    if let Some(ref b) = sel.box_spec
        && b.boxes.len() > 1
    {
        let box_mask = boxes_union_mask(&b.boxes, &sel.start, &sel.shape, sel.plane);
        and_mask(&mut mask, &box_mask);
    }

    if let Some(ref reg) = sel.region {
        let reg_mask = immath_region_pixel_mask(reg, &sel.start, &sel.shape, sel.plane)?;
        if !reg_mask.iter().all(|&v| v) {
            and_mask(&mut mask, &reg_mask);
        }
    }

    if sel.has_channel_gaps()
        && let (Some(ax), Some(indices)) = (sel.channel_axis, &sel.channel_indices)
    {
        mask_skipped_axis_positions(&mut mask, &sel.shape, ax, sel.start[ax], indices);
    }

    if sel.has_stokes_gaps()
        && let (Some(ax), Some(indices)) = (sel.stokes_axis, &sel.stokes_indices)
    {
        mask_skipped_axis_positions(&mut mask, &sel.shape, ax, sel.start[ax], indices);
    }

    Ok(mask)
}

fn boxes_union_mask(
    boxes: &[[usize; 4]],
    start: &[usize],
    shape: &[usize],
    plane: [usize; 2],
) -> ArrayD<bool> {
    let [px, py] = plane;
    ArrayD::from_shape_fn(IxDyn(shape), |idx| {
        let gx = start[px] + idx[px];
        let gy = start[py] + idx[py];
        boxes
            .iter()
            .any(|&[x0, y0, x1, y1]| (x0..=x1).contains(&gx) && (y0..=y1).contains(&gy))
    })
}

fn immath_region_pixel_mask(
    region: &PixelRegion,
    start: &[usize],
    shape: &[usize],
    plane: [usize; 2],
) -> Result<ArrayD<bool>, ImageError> {
    match region {
        PixelRegion::Boxes(boxes) => {
            if boxes.len() < 2 {
                Ok(ArrayD::from_elem(IxDyn(shape), true))
            } else {
                Ok(boxes_union_mask(boxes, start, shape, plane))
            }
        }
        PixelRegion::Polygon(vertices) => {
            if vertices.len() < 3 {
                return Err(ImageError::InvalidMetadata(
                    "CRTF polygon regions require at least three vertices".to_string(),
                ));
            }
            let [x_axis, y_axis] = plane;
            Ok(ArrayD::from_shape_fn(IxDyn(shape), |idx| {
                let x = (start[x_axis] + idx[x_axis]) as f64;
                let y = (start[y_axis] + idx[y_axis]) as f64;
                point_in_polygon_or_boundary(x, y, vertices)
            }))
        }
    }
}

fn point_in_polygon_or_boundary(x: f64, y: f64, vertices: &[[usize; 2]]) -> bool {
    let mut inside = false;
    let mut previous = vertices[vertices.len() - 1];
    for &current in vertices {
        let x0 = previous[0] as f64;
        let y0 = previous[1] as f64;
        let x1 = current[0] as f64;
        let y1 = current[1] as f64;
        if point_on_segment(x, y, x0, y0, x1, y1) {
            return true;
        }
        let crosses = (y0 > y) != (y1 > y);
        if crosses {
            let x_intersection = (x1 - x0) * (y - y0) / (y1 - y0) + x0;
            if x <= x_intersection {
                inside = !inside;
            }
        }
        previous = current;
    }
    inside
}

fn point_on_segment(x: f64, y: f64, x0: f64, y0: f64, x1: f64, y1: f64) -> bool {
    let cross = (x - x0) * (y1 - y0) - (y - y0) * (x1 - x0);
    if cross.abs() > 1.0e-9 {
        return false;
    }
    x >= x0.min(x1) - 1.0e-9
        && x <= x0.max(x1) + 1.0e-9
        && y >= y0.min(y1) - 1.0e-9
        && y <= y0.max(y1) + 1.0e-9
}

fn mask_skipped_axis_positions(
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
        let global = start + local;
        if !kept.contains(&global) {
            slice.fill(false);
        }
    }
    *mask = Some(m);
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

/// Evaluates an optional `mask=` LEL string against `target_shape`, opening any
/// additional quoted images and binding known variable aliases. When `window`
/// (`(start, shape)`) is provided, only that slice is materialized.
pub(super) fn evaluate_immath_mask<T: RealMathPixel>(
    mask_expr: Option<&str>,
    target_shape: &[usize],
    aliases: &[(String, &dyn ImageInterface<T>)],
    stretch: bool,
    window: Option<(&[usize], &[usize])>,
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let Some(raw_expr) = mask_expr.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let alias_keys: Vec<String> = aliases.iter().map(|(k, _)| k.clone()).collect();
    let expr = quote_var_aliases(raw_expr, &alias_keys);
    let names = referenced_images(&expr).map_err(|error| {
        ImageError::CasaSelection(format!(
            "Input mask specification is incorrect: {}",
            mask_error_wording(error)
        ))
    })?;
    let mut extra_images: Vec<(String, RawImage<T>)> = Vec::new();
    for name in &names {
        if aliases.iter().any(|(k, _)| k == name) {
            continue;
        }
        let opened = open_raw_image::<T>(&super::operand_path(name)).map_err(|_| {
            ImageError::CasaSelection(format!(
                "Input mask specification is incorrect: ImageExprParse: '{name}' is an unknown lattice or image or it is an unqualified region"
            ))
        })?;
        extra_images.push((name.clone(), opened));
    }
    let mut map: HashMap<String, &dyn ImageInterface<T>> = HashMap::new();
    for (k, v) in aliases {
        map.insert(k.clone(), *v);
    }
    for (name, raw) in &extra_images {
        map.insert(name.clone(), raw.as_interface());
    }
    let resolver = HashMapResolver(map);
    let parsed = parse_mask_expr(&expr, &resolver).map_err(|error| {
        ImageError::CasaSelection(format!(
            "Input mask specification is incorrect: {}",
            mask_error_wording(error)
        ))
    })?;
    let mask_shape = parsed.shape();
    if mask_shape != target_shape && !mask_shape.is_empty() {
        if !can_broadcast_to(mask_shape, target_shape) {
            return Err(ImageError::CasaSelection(
                "Unable to extend mask: ExtendSpecifier - lengths of old axis mismatch".into(),
            ));
        }
        if !stretch {
            return Err(ImageError::CasaSelection(format!(
                "The input image shape ({target_shape:?}) and mask shape ({mask_shape:?}) are different and stretch is false"
            )));
        }
    }
    if let Some((w_start, w_shape)) = window {
        parsed
            .eval_window_for_shape(w_start, w_shape, target_shape)
            .map(Some)
    } else {
        parsed.eval_for_shape(target_shape).map(Some)
    }
}

/// Quotes `immath` variable aliases outside string literals so bare identifiers
/// adjacent to `/` (e.g. `IM0/1.579`) are not scanned by `expr_parser::lex_ident`
/// as filesystem paths, while leaving function calls untouched.
pub(super) fn quote_var_aliases(expr: &str, varnames: &[String]) -> String {
    let mut vars: Vec<&str> = varnames
        .iter()
        .map(String::as_str)
        .filter(|s| !s.is_empty())
        .collect();
    if vars.is_empty() {
        return expr.to_string();
    }
    vars.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| b.cmp(a)));
    vars.dedup();

    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = String::with_capacity(expr.len() + 16);
    let bytes = expr.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' || b == b'"' {
            let quote = b;
            out.push(quote as char);
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c as char);
                i += 1;
                if c == quote {
                    break;
                }
            }
            continue;
        }
        let rest = &expr[i..];
        let matched = vars.iter().copied().find(|v| {
            if !rest.starts_with(v) {
                return false;
            }
            let vb = v.as_bytes();
            if is_word(vb[0])
                && i > 0
                && (is_word(bytes[i - 1]) || matches!(bytes[i - 1], b'.' | b'/'))
            {
                return false;
            }
            let end = i + v.len();
            if is_word(vb[vb.len() - 1]) {
                if end < bytes.len() && (is_word(bytes[end]) || bytes[end] == b'.') {
                    return false;
                }
                let after_ws = bytes[end..]
                    .iter()
                    .copied()
                    .find(|c| !c.is_ascii_whitespace());
                if after_ws == Some(b'(') {
                    return false;
                }
            }
            true
        });
        if let Some(v) = matched {
            out.push('"');
            out.push_str(v);
            out.push('"');
            i += v.len();
            continue;
        }
        let ch = rest.chars().next().expect("non-empty");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

pub(super) struct PreparedInput<T: RealMathPixel> {
    pub(super) raw: RawImage<T>,
    pub(super) subimaged: Option<MemorySnapshotImage<T>>,
}

impl<T: RealMathPixel> PreparedInput<T> {
    pub(super) fn as_interface(&self) -> &dyn ImageInterface<T> {
        match &self.subimaged {
            Some(temp) => temp,
            None => self.raw.as_interface(),
        }
    }
}

impl<T: RealMathPixel> Lattice<T> for RawImage<T> {
    fn shape(&self) -> &[usize] {
        self.as_interface().shape()
    }
    fn is_writable(&self) -> bool {
        false
    }
    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        self.as_interface().get_at(position)
    }
    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        self.as_interface().get_slice(start, shape, stride)
    }
    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        self.as_interface().get()
    }
}

impl<T: RealMathPixel> ImageInterface<T> for RawImage<T> {
    fn as_any(&self) -> Option<&dyn Any> {
        self.as_interface().as_any()
    }
    fn coordinates(&self) -> &CoordinateSystem {
        self.as_interface().coordinates()
    }
    fn units(&self) -> &str {
        self.as_interface().units()
    }
    fn misc_info(&self) -> RecordValue {
        self.as_interface().misc_info()
    }
    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        self.as_interface().image_info()
    }
    fn name(&self) -> Option<&Path> {
        self.as_interface().name()
    }
    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        self.as_interface().default_mask()
    }
    fn default_mask_name(&self) -> Option<String> {
        self.as_interface().default_mask_name()
    }
    fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        self.as_interface().get_mask_slice(start, shape, stride)
    }
}

pub(super) fn apply_selection_to_raw<T: RealMathPixel>(
    raw: RawImage<T>,
    mask_override: Option<ArrayD<bool>>,
    sel: Option<&ImmathSelection>,
) -> Result<PreparedInput<T>, ImageError> {
    const SMALL_SNAPSHOT_MAX_ELEMENTS: usize = 2_097_152;
    let full_shape = raw.as_interface().shape().to_vec();
    let total_elems: usize = full_shape.iter().product();
    let default_sel;
    let sel_ref = match sel {
        Some(s) => s,
        None => {
            if mask_override.is_none() {
                if total_elems <= SMALL_SNAPSHOT_MAX_ELEMENTS
                    && let RawImage::Paged {
                        ref image,
                        ref legacy_coords,
                    } = raw
                {
                    let lc = legacy_coords.clone();
                    let snap = snapshot_from_interface(image, lc)?;
                    return Ok(PreparedInput {
                        raw,
                        subimaged: Some(snap),
                    });
                }
                return Ok(PreparedInput {
                    raw,
                    subimaged: None,
                });
            }
            default_sel = ImmathSelection {
                saved_mask: None,
                strided_plan: None,
                start: vec![0; full_shape.len()],
                shape: full_shape.clone(),
                plane: [0, 1],
                box_spec: None,
                region: None,
                channel_axis: None,
                channel_indices: None,
                stokes_axis: None,
                stokes_indices: None,
                selected_stokes: None,
            };
            &default_sel
        }
    };
    if sel_ref.is_full_contiguous(&full_shape) && mask_override.is_none() {
        if total_elems <= SMALL_SNAPSHOT_MAX_ELEMENTS
            && let RawImage::Paged {
                ref image,
                ref legacy_coords,
            } = raw
        {
            let lc = legacy_coords.clone();
            let snap = snapshot_from_interface(image, lc)?;
            return Ok(PreparedInput {
                raw,
                subimaged: Some(snap),
            });
        }
        return Ok(PreparedInput {
            raw,
            subimaged: None,
        });
    }
    materialize_subimage(raw, sel_ref, mask_override)
}

fn snapshot_from_interface<T: RealMathPixel>(
    image: &PagedImage<T>,
    legacy_coords: Option<RecordValue>,
) -> Result<MemorySnapshotImage<T>, ImageError> {
    let data = image.get()?;
    let mask = image
        .default_mask()?
        .filter(|m| m.iter().any(|&v| !v))
        .map(Arc::new);
    Ok(MemorySnapshotImage {
        data: Arc::new(data),
        mask,
        coordinates: image.coordinates().clone(),
        units: image.units().to_string(),
        image_info: image.image_info()?,
        misc_info: image.misc_info(),
        legacy_coords,
    })
}

fn materialize_subimage<T: RealMathPixel>(
    raw: RawImage<T>,
    sel: &ImmathSelection,
    window_mask_override: Option<ArrayD<bool>>,
) -> Result<PreparedInput<T>, ImageError> {
    let img = raw.as_interface();
    if let Some((plan, stride)) = &sel.strided_plan {
        let mut output = TempImage::new(
            plan.shape.clone(),
            plan.coordinates.clone(),
            TempStoragePolicy::Memory,
        )?;
        plan.copy_to(img, &mut output, None, false)?;
        let mut mask = output.default_mask()?;
        if let Some(override_mask) = window_mask_override {
            let sampled = override_mask
                .slice_each_axis(|axis| Slice::new(0, None, stride[axis.axis.index()] as isize));
            and_mask(&mut mask, &sampled.to_owned());
        }
        let snapshot = MemorySnapshotImage {
            data: Arc::new(output.get()?),
            mask: mask.map(Arc::new),
            coordinates: plan.coordinates.clone(),
            units: output.units().into(),
            image_info: plan.image_info().clone(),
            misc_info: output.misc_info(),
            legacy_coords: plan.coordinate_record.clone(),
        };
        return Ok(PreparedInput {
            raw,
            subimaged: Some(snapshot),
        });
    }
    let stride = vec![1; img.ndim()];
    let data = img.get_slice(&sel.start, &sel.shape, &stride)?;
    let mask = selection_and_mask(img, sel, window_mask_override)?;

    let out_coords = subimage_coordinates_with_stokes(
        img.coordinates(),
        &sel.start,
        &sel.shape,
        sel.selected_stokes.as_deref(),
    )?;
    let legacy_coords = match &raw {
        RawImage::Paged { image, .. } => legacy_subimage_coordinates_with_stokes(
            image,
            &sel.start,
            &sel.shape,
            sel.selected_stokes.as_deref(),
        ),
        RawImage::Memory(_) => None,
    };

    let snap = MemorySnapshotImage {
        data: Arc::new(data),
        mask: mask.filter(|m| m.iter().any(|&v| !v)).map(Arc::new),
        coordinates: out_coords,
        units: img.units().to_string(),
        image_info: img.image_info()?,
        misc_info: img.misc_info(),
        legacy_coords,
    };
    Ok(PreparedInput {
        raw,
        subimaged: Some(snap),
    })
}
