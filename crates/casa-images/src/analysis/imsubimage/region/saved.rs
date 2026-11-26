// SPDX-License-Identifier: LGPL-3.0-or-later
//! Decode the saved casacore region records accepted by imsubimage.

use std::path::Path;

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::analysis::world_axes_for_plane;
use crate::saved_region::map_world_region_axes;
use crate::{AnyPagedImage, ImageError, PolygonRegion};

mod compound;
pub(super) use compound::Region as CompoundRegion;
mod relative;

use super::{ParsedRegion, SavedRegionMask, SpatialShape, StridedSection};

pub(super) fn load_saved_region(
    image_path: &Path,
    region_name: &str,
    target_coords: &CoordinateSystem,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<ParsedRegion, ImageError> {
    let record = load_saved_region_record(image_path, region_name)?;
    parse_saved_region(region_name, &record, target_coords, full_shape, plane)
}

pub(super) fn load_saved_region_record(path: &Path, name: &str) -> Result<RecordValue, ImageError> {
    match AnyPagedImage::open(path)? {
        AnyPagedImage::Float32(img) => img.get_region_record(name),
        AnyPagedImage::Float64(img) => img.get_region_record(name),
        AnyPagedImage::Complex32(img) => img.get_region_record(name),
        AnyPagedImage::Complex64(img) => img.get_region_record(name),
    }
}

pub(super) fn parse_saved_region(
    region_name: &str,
    rec: &RecordValue,
    target_coords: &CoordinateSystem,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<ParsedRegion, ImageError> {
    let class_name = match rec.get("name") {
        Some(Value::Scalar(ScalarValue::String(name))) => name.as_str(),
        _ => "",
    };

    match class_name {
        "WCPolygon" if relative::mode(rec).is_some() => {
            relative::decode_polygon(region_name, rec, target_coords, full_shape)
        }
        "WCPolygon" => polygon_region(
            region_name,
            PolygonRegion::from_record(region_name, rec, target_coords)?,
            target_coords,
            full_shape,
        ),
        "WCUnion" | "WCIntersection" | "WCDifference" | "WCComplement" | "WCExtension"
        | "WCConcatenation" => compound::decode(region_name, rec, target_coords, full_shape, plane),
        "LCSlicer" | "LCBox" => {
            let (blc, trc, stride) = if class_name == "LCSlicer" {
                let section = decode_lc_slicer(region_name, rec, target_coords, full_shape)?;
                let stride = section
                    .stride
                    .iter()
                    .any(|&step| step != 1)
                    .then_some(section.stride);
                (section.lower, section.upper, stride)
            } else {
                let (lower, upper) = decode_lc_box(region_name, rec, full_shape)?;
                (lower, upper, None)
            };
            let shapes = if full_shape.len() >= 2 {
                vec![
                    SpatialShape::Box([blc[plane[0]], blc[plane[1]], trc[plane[0]], trc[plane[1]]])
                        .into(),
                ]
            } else {
                Vec::new()
            };
            let shape: Vec<usize> = blc
                .iter()
                .zip(trc.iter())
                .map(|(lo, hi)| hi - lo + 1)
                .collect();
            Ok(ParsedRegion {
                is_compound: false,
                is_crtf: false,
                shapes,
                saved_mask: None,
                saved_bounds: Some((blc, shape)),
                stride,
            })
        }
        "WCBox" => {
            let (blc, trc) = decode_wc_box(region_name, rec, target_coords, full_shape)?;
            let shapes = if full_shape.len() >= 2 {
                vec![
                    SpatialShape::Box([blc[plane[0]], blc[plane[1]], trc[plane[0]], trc[plane[1]]])
                        .into(),
                ]
            } else {
                Vec::new()
            };
            let shape: Vec<usize> = blc
                .iter()
                .zip(trc.iter())
                .map(|(lo, hi)| hi - lo + 1)
                .collect();
            Ok(ParsedRegion {
                is_compound: false,
                is_crtf: false,
                shapes,
                saved_mask: None,
                saved_bounds: Some((blc, shape)),
                stride: None,
            })
        }
        "WCEllipsoid" => {
            let ellipse = decode_wc_ellipsoid(region_name, rec, target_coords, full_shape, plane)?;
            let bounds = ellipsoid_bounds(&ellipse, full_shape, plane)?;
            Ok(ParsedRegion {
                is_compound: false,
                is_crtf: false,
                shapes: vec![ellipse.into()],
                saved_mask: None,
                saved_bounds: Some(bounds),
                stride: None,
            })
        }
        other => Err(ImageError::InvalidMetadata(format!(
            "unsupported saved region '{region_name}' of class {other:?}"
        ))),
    }
}

fn polygon_region(
    region_name: &str,
    poly: PolygonRegion,
    target_coords: &CoordinateSystem,
    full_shape: &[usize],
) -> Result<ParsedRegion, ImageError> {
    let plane = poly
        .lattice_plane(target_coords, full_shape)?
        .ok_or_else(|| {
            ImageError::CasaSelection(format!(
                "saved region '{region_name}' selects no pixels in image"
            ))
        })?;
    let axes = poly.pixel_axes;
    let bounds = plane.section(full_shape, axes);
    Ok(ParsedRegion {
        is_compound: false,
        is_crtf: false,
        shapes: Vec::new(),
        saved_mask: Some(SavedRegionMask::Polygon { axes, plane }),
        saved_bounds: Some(bounds),
        stride: None,
    })
}

pub(in crate::analysis::imsubimage) fn lattice_pixel_section(
    rec: &RecordValue,
    coords: &CoordinateSystem,
    shape: &[usize],
) -> Result<Option<StridedSection>, ImageError> {
    if matches!(rec.get("name"), Some(Value::Scalar(ScalarValue::String(name))) if name == "LCBox")
    {
        let stored_shape = read_numeric_vec(rec.get("shape"))
            .ok_or_else(|| ImageError::CasaSelection("LCBox region has no lattice shape".into()))?;
        if !stored_shape
            .iter()
            .copied()
            .eq(shape.iter().map(|&size| size as f64))
        {
            return Err(ImageError::CasaSelection(format!(
                "shape of lattice {shape:?} mismatches lattice shape in region {stored_shape:?}"
            )));
        }
        let (start, end) = decode_lc_box("region", rec, shape)?;
        let count = start.iter().zip(end).map(|(&lo, hi)| hi - lo + 1).collect();
        return Ok(Some(StridedSection {
            start,
            count,
            stride: vec![1; shape.len()],
        }));
    }
    lc_slicer_section(rec, coords, shape)
}

pub(super) fn lc_slicer_section(
    rec: &RecordValue,
    coords: &CoordinateSystem,
    shape: &[usize],
) -> Result<Option<StridedSection>, ImageError> {
    if !matches!(
        rec.get("name"),
        Some(Value::Scalar(ScalarValue::String(name))) if name == "LCSlicer"
    ) {
        return Ok(None);
    }
    let section = decode_lc_slicer("region", rec, coords, shape)?;
    let count = section
        .lower
        .iter()
        .zip(&section.upper)
        .zip(&section.stride)
        .map(|((&first, &last), &step)| (last - first) / step + 1)
        .collect();
    Ok(Some(StridedSection {
        start: section.lower,
        count,
        stride: section.stride,
    }))
}

pub(super) fn compound_section(
    record: &RecordValue,
    coords: &CoordinateSystem,
    shape: &[usize],
    plane: [usize; 2],
) -> Result<StridedSection, ImageError> {
    let (start, count) = compound::bounds("region", record, coords, shape, plane)?;
    Ok(StridedSection {
        start,
        count,
        stride: vec![1; shape.len()],
    })
}

pub(super) fn relative_polygon_section(
    record: &RecordValue,
    coords: &CoordinateSystem,
    shape: &[usize],
) -> Result<Option<StridedSection>, ImageError> {
    if relative::mode(record).is_none() {
        return Ok(None);
    }
    let (axes, plane) = relative::polygon_plane("region", record, coords, shape)?;
    let (start, count) = plane.section(shape, axes);
    Ok(Some(StridedSection {
        start,
        count,
        stride: vec![1; shape.len()],
    }))
}

struct SlicerSection {
    lower: Vec<usize>,
    upper: Vec<usize>,
    stride: Vec<usize>,
}

fn decode_lc_slicer(
    name: &str,
    rec: &RecordValue,
    coords: &CoordinateSystem,
    full_shape: &[usize],
) -> Result<SlicerSection, ImageError> {
    let numeric = |field| {
        read_numeric_vec(rec.get(field)).ok_or_else(|| {
            ImageError::InvalidMetadata(format!("saved region '{name}' missing numeric {field}"))
        })
    };
    let one_rel = match rec.get("oneRel") {
        Some(Value::Scalar(ScalarValue::Bool(value))) => *value,
        Some(Value::Scalar(ScalarValue::Int32(value))) => *value != 0,
        None => return Err(unknown_record_field("oneRel")),
        _ => {
            return Err(ImageError::InvalidMetadata(
                "RecordInterface::asBool - invalid data type".into(),
            ));
        }
    };
    let refpix = coords.reference_pixel();
    let [lower, upper, step] = [numeric("blc")?, numeric("trc")?, numeric("inc")?];
    let ndim = lower.len().max(upper.len()).max(step.len());
    if ndim > full_shape.len() {
        return Err(ImageError::ShapeMismatch {
            expected: vec![full_shape.len()],
            got: vec![ndim],
        });
    }
    let resolve = |values: &[f64], fraction_key, mode_key, low| {
        let fractions = slicer_flags(rec, fraction_key)?;
        let modes = numeric(mode_key)?;
        if fractions.len() != values.len() || modes.len() != values.len() {
            let fields = if low {
                "blc, fracBlc, and absRelBlc"
            } else {
                "trc, fracTrc, and absRelTrc"
            };
            return Err(ImageError::InvalidMetadata(format!(
                "LCSlicer::LCSLicer - {fields} have unequal lengths"
            )));
        }
        full_shape
            .iter()
            .enumerate()
            .map(|(axis, &len)| {
                let default = if low { 0 } else { len - 1 } as i64;
                let Some(&stored) = values.get(axis) else {
                    return Ok(default);
                };
                if is_mimic_source(stored) {
                    return Ok(default);
                }
                let fractional = fractions[axis];
                let mut pixel = stored as f32;
                if fractional {
                    pixel = pixel * len as f32 - if low { 0.0 } else { 1.0 };
                }
                pixel += match modes[axis] {
                    1.0 if one_rel && !fractional => -1.0,
                    1.0 => 0.0,
                    2.0 => *refpix.get(axis).ok_or_else(|| {
                        ImageError::InvalidMetadata(format!(
                            "saved region '{name}' has no reference pixel on axis {axis}"
                        ))
                    })? as f32,
                    3.0 => len as f32 / 2.0,
                    _ => {
                        return Err(ImageError::InvalidMetadata(format!(
                            "saved region '{name}' has invalid {mode_key} on axis {axis}"
                        )));
                    }
                };
                if !pixel.is_finite() {
                    return Err(ImageError::InvalidMetadata(format!(
                        "saved region '{name}' has non-finite bounds on axis {axis}"
                    )));
                }
                let pixel = (pixel + 0.5) as i64;
                Ok(if low {
                    pixel.max(0)
                } else {
                    pixel.min(default)
                })
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let blc = resolve(&lower, "fracblc", "arblc", true)?;
    let trc = resolve(&upper, "fractrc", "artrc", false)?;
    if blc.iter().zip(&trc).any(|(lo, hi)| lo > hi) {
        return Err(ImageError::CasaSelection(format!(
            "LCSlicer::toSlicer - blc {blc:?} must be <= trc {trc:?}"
        )));
    }
    let fractional_step = slicer_flags(rec, "fracinc")?;
    if fractional_step.len() != step.len() {
        return Err(ImageError::InvalidMetadata(
            "LCSlicer::LCSLicer - inc and fracInc have unequal lengths".into(),
        ));
    }
    let mut strides = vec![1; full_shape.len()];
    for (axis, (&stored, fractional)) in step.iter().zip(fractional_step).enumerate() {
        let stride = if is_mimic_source(stored) {
            1.0
        } else if fractional {
            stored as f32 * full_shape[axis] as f32
        } else {
            stored as f32
        };
        if !stride.is_finite() || stride < 0.5 {
            return Err(ImageError::InvalidMetadata(format!(
                "saved region '{name}' requires a positive stride on axis {axis}"
            )));
        }
        strides[axis] = (stride + 0.5) as usize;
    }
    let as_indices = |values: Vec<i64>| values.into_iter().map(|value| value as usize).collect();
    Ok(SlicerSection {
        lower: as_indices(blc),
        upper: as_indices(trc),
        stride: strides,
    })
}

fn unknown_record_field(field: &str) -> ImageError {
    ImageError::InvalidMetadata(format!("RecordInterface: field {field} is unknown"))
}

fn slicer_flags(record: &RecordValue, field: &str) -> Result<Vec<bool>, ImageError> {
    let flags = match record.get(field) {
        Some(Value::Array(ArrayValue::Bool(values))) if values.ndim() == 1 => {
            values.iter().copied().collect()
        }
        Some(Value::Array(ArrayValue::Int32(values))) if values.ndim() == 1 => {
            values.iter().map(|&value| value != 0).collect()
        }
        Some(Value::Scalar(ScalarValue::Bool(value))) => vec![*value],
        Some(Value::Scalar(ScalarValue::Int32(value))) => vec![*value != 0],
        None => return Err(unknown_record_field(field)),
        _ => {
            return Err(ImageError::InvalidMetadata(format!(
                "TableRecordRep::get_pointer - incorrect data type used for field {field}"
            )));
        }
    };
    Ok(flags)
}

fn is_mimic_source(value: f64) -> bool {
    // The integer sentinel is imprecise in the record's Float32 representation.
    value < -2_147_483_646.0 + 10.0
}

fn decode_lc_box(
    name: &str,
    rec: &RecordValue,
    full_shape: &[usize],
) -> Result<(Vec<usize>, Vec<usize>), ImageError> {
    let one_rel = matches!(
        rec.get("oneRel"),
        Some(Value::Scalar(ScalarValue::Bool(true)))
    );
    let blc_raw = read_numeric_vec(rec.get("blc")).ok_or_else(|| {
        ImageError::InvalidMetadata(format!("saved region '{name}' missing numeric blc"))
    })?;
    let trc_raw = read_numeric_vec(rec.get("trc")).ok_or_else(|| {
        ImageError::InvalidMetadata(format!("saved region '{name}' missing numeric trc"))
    })?;
    if let Some(inc) = read_numeric_vec(rec.get("inc"))
        && inc.iter().any(|&v| (v - 1.0).abs() > 1e-6)
    {
        return Err(ImageError::InvalidMetadata(format!(
            "saved region '{name}' has non-unit stride {inc:?}"
        )));
    }
    let ndim = full_shape.len();
    if blc_raw.len() != ndim || trc_raw.len() != ndim {
        return Err(ImageError::ShapeMismatch {
            expected: vec![ndim],
            got: vec![blc_raw.len()],
        });
    }
    let frac_blc = read_bool_vec(rec.get("fracblc"), ndim);
    let frac_trc = read_bool_vec(rec.get("fractrc"), ndim);
    let mut blc = Vec::with_capacity(ndim);
    let mut trc = Vec::with_capacity(ndim);
    for axis in 0..ndim {
        let len = full_shape[axis];
        let lo = if frac_blc[axis] {
            (blc_raw[axis] * (len.saturating_sub(1)) as f64).round()
        } else if one_rel {
            (blc_raw[axis] - 1.0).round()
        } else {
            blc_raw[axis].round()
        };
        let hi = if frac_trc[axis] {
            (trc_raw[axis] * (len.saturating_sub(1)) as f64).round()
        } else if one_rel {
            (trc_raw[axis] - 1.0).round()
        } else {
            trc_raw[axis].round()
        };
        if !lo.is_finite() || !hi.is_finite() || lo < 0.0 || hi < lo || (hi as usize) >= len {
            return Err(ImageError::CasaSelection(format!(
                "saved region '{name}' bounds [{lo}, {hi}] out of range for axis {axis} (len {len})"
            )));
        }
        blc.push(lo as usize);
        trc.push(hi as usize);
    }
    Ok((blc, trc))
}

fn decode_wc_box(
    name: &str,
    rec: &RecordValue,
    coords: &CoordinateSystem,
    full_shape: &[usize],
) -> Result<(Vec<usize>, Vec<usize>), ImageError> {
    decode_wc_box_axes(name, rec, coords, full_shape, |_| true)
}

fn decode_wc_box_axes(
    name: &str,
    rec: &RecordValue,
    coords: &CoordinateSystem,
    full_shape: &[usize],
    include: impl Fn(usize) -> bool,
) -> Result<(Vec<usize>, Vec<usize>), ImageError> {
    let (axes, origin) = wc_pixel_axes(name, "WCBox", rec, coords)?;
    let mut modes = read_numeric_vec(rec.get("absrel")).unwrap_or_default();
    if modes.is_empty() {
        modes.resize(axes.len(), 1.0);
    }
    let dimension_mismatch =
        || ImageError::InvalidMetadata(format!("WCBox {name:?} dimension mismatch"));
    if modes.len() != axes.len() {
        return Err(dimension_mismatch());
    }
    let selected_axes = axes
        .iter()
        .copied()
        .filter(|&axis| include(axis))
        .collect::<Vec<_>>();
    let modes = modes
        .into_iter()
        .zip(&axes)
        .filter_map(|(mode, &axis)| include(axis).then_some(mode))
        .collect::<Vec<_>>();
    let mut bounds = Vec::new();
    for (key, low) in [("blc", true), ("trc", false)] {
        let quantities = read_quantum_list(rec.get(key))?;
        if quantities.len() != axes.len() {
            return Err(dimension_mismatch());
        }
        let quantities = quantities
            .into_iter()
            .zip(&axes)
            .filter_map(|(quantity, &axis)| include(axis).then_some(quantity))
            .collect::<Vec<_>>();
        bounds.push(wc_box_corner(
            coords,
            full_shape,
            &selected_axes,
            &quantities,
            &modes,
            origin,
            low,
        )?);
    }
    let trc = bounds.pop().expect("upper corner");
    let blc = bounds.pop().expect("lower corner");
    if blc.iter().zip(&trc).any(|(lower, upper)| lower > upper) {
        return Err(ImageError::CasaSelection(format!(
            "WCBox {name:?} has reversed bounds"
        )));
    }
    Ok((blc, trc))
}

fn wc_pixel_axes(
    name: &str,
    class: &str,
    rec: &RecordValue,
    coords: &CoordinateSystem,
) -> Result<(Vec<usize>, usize), ImageError> {
    let origin = usize::from(matches!(
        rec.get("oneRel"),
        Some(Value::Scalar(ScalarValue::Bool(true)))
    ));
    let Some(Value::Record(source_record)) = rec.get("coordinates") else {
        return Err(ImageError::InvalidMetadata(format!(
            "{class} has no coordinate system"
        )));
    };
    let saved_coords = CoordinateSystem::from_record(source_record)?;
    let original_axes = read_numeric_vec(rec.get("pixelAxes"))
        .ok_or_else(|| ImageError::InvalidMetadata(format!("{class} has no pixelAxes")))?
        .into_iter()
        .map(|axis| {
            (axis as usize).checked_sub(origin).ok_or_else(|| {
                ImageError::InvalidMetadata(format!(
                    "{class} {name:?} has invalid pixel axis {axis}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let axes = map_world_region_axes(name, &saved_coords, coords, original_axes)?;
    Ok((axes, origin))
}

fn decode_wc_ellipsoid(
    name: &str,
    rec: &RecordValue,
    coords: &CoordinateSystem,
    shape: &[usize],
    plane: [usize; 2],
) -> Result<SpatialShape, ImageError> {
    let invalid =
        || ImageError::InvalidMetadata(format!("invalid direction-plane WCEllipsoid {name:?}"));
    let (axes, origin) = wc_pixel_axes(name, "WCEllipsoid", rec, coords)?;
    let mode = match rec.get("absrel") {
        Some(Value::Scalar(ScalarValue::Int32(mode @ (1..=3)))) => f64::from(*mode),
        _ => return Err(invalid()),
    };
    let [x, y] = plane;
    let reversed = mode != 1.0 && axes == [y, x];
    if axes != plane && !reversed {
        return Err(invalid());
    }
    let center = read_quantum_list(rec.get("center"))?;
    let [first, second] = center.as_slice() else {
        return Err(invalid());
    };
    let rad = Unit::new("rad").expect("radians are a built-in unit");
    let in_radians = |quantity: &Quantity| quantity.get_value_in(&rad).map_err(|_| invalid());
    let [cx, cy] = if mode != 1.0 {
        // Casacore indexes the world center and radii by lattice axis. It
        // applies pixel overrides in the record's order before returning the plane.
        let mapped = if reversed { [y, x] } else { plane };
        let mut projected = relative::project_points(
            coords,
            shape,
            world_axes_for_plane(coords, plane)?,
            mapped,
            [first.unit().name(), second.unit().name()],
            [[first.value(), second.value()]],
            &vec![mode; coords.n_world_axes()],
        )?[0];
        if reversed {
            projected.reverse();
        }
        projected
    } else if matches!(first.unit().name(), "pix" | "frac")
        && matches!(second.unit().name(), "pix" | "frac")
    {
        let pixel = |q: &Quantity, axis: usize| match q.unit().name() {
            "frac" => q.value() * shape[axis] as f64,
            _ => q.value() - origin as f64,
        };
        [pixel(first, x), pixel(second, y)]
    } else {
        let mut world = coords.reference_value();
        let [wx, wy] = world_axes_for_plane(coords, plane)?;
        world[wx] = in_radians(first)?;
        world[wy] = in_radians(second)?;
        let pixel = coords.to_pixel(&world)?;
        [pixel[x], pixel[y]]
    };
    let scales = super::direction_pixel_scales(coords).ok_or_else(invalid)?;
    let radius = |q: &Quantity, axis: usize| {
        let size = if q.unit().name() == "pix" {
            q.value()
        } else {
            in_radians(q)? / scales[axis]
        };
        if !size.is_finite() || size <= 0.0 {
            return Err(invalid());
        }
        Ok(size)
    };
    match rec.get("type") {
        Some(Value::Scalar(ScalarValue::Int32(1))) => {
            let Some(Value::Record(value)) = rec.get("radius") else {
                return Err(invalid());
            };
            let r = radius(&crate::beam::quantity_from_record(value)?, 0)?;
            Ok(SpatialShape::Circle {
                cx,
                cy,
                rx: r,
                ry: r,
            })
        }
        Some(Value::Scalar(ScalarValue::Int32(0 | 2))) => {
            let values = read_quantum_list(rec.get("radii"))?;
            let [major, minor] = values.as_slice() else {
                return Err(invalid());
            };
            let (a, b) = (radius(major, 0)?, radius(minor, 1)?);
            if matches!(rec.get("type"), Some(Value::Scalar(ScalarValue::Int32(0)))) {
                return Ok(SpatialShape::Circle {
                    cx,
                    cy,
                    rx: a,
                    ry: b,
                });
            }
            let Some(Value::Record(value)) = rec.get("theta") else {
                return Err(invalid());
            };
            let pa = in_radians(&crate::beam::quantity_from_record(value)?)?
                - std::f64::consts::FRAC_PI_2;
            Ok(SpatialShape::Ellipse {
                cx,
                cy,
                semi_major: a,
                semi_minor: b,
                cos_pa: pa.cos(),
                sin_pa: pa.sin(),
            })
        }
        _ => Err(invalid()),
    }
}

fn ellipsoid_bounds(
    ellipse: &SpatialShape,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<(Vec<usize>, Vec<usize>), ImageError> {
    let (center, radii) = match *ellipse {
        SpatialShape::Circle { cx, cy, rx, ry } => ([cx, cy], [rx, ry]),
        SpatialShape::Ellipse {
            cx,
            cy,
            semi_major,
            semi_minor,
            cos_pa,
            sin_pa,
        } => {
            let radii = if sin_pa.abs() <= 1e-7 {
                [semi_minor, semi_major]
            } else if cos_pa.abs() <= 1e-7 {
                [semi_major, semi_minor]
            } else {
                [semi_major.max(semi_minor); 2]
            };
            ([cx, cy], radii)
        }
        _ => unreachable!("WC ellipsoid decodes as a circle or ellipse"),
    };
    // LCEllipsoid sets its geometric box before rasterizing the mask, so the
    // crop can include outer rows or columns with no selected pixel centers.
    let mut start = vec![0; full_shape.len()];
    let mut shape = full_shape.to_vec();
    for ((axis, center), radius) in plane.into_iter().zip(center).zip(radii) {
        let (center, radius) = (center as f32, radius as f32);
        if !center.is_finite() || !radius.is_finite() || radius <= 0.0 {
            return Err(ImageError::InvalidMetadata(
                "invalid saved ellipsoid bounds".into(),
            ));
        }
        let epsilon = 10.0f32.powi((2.0 * f64::from(radius)).log10() as i32 - 5);
        let lo = ((center - radius + 1.0 - epsilon) as i64).max(0);
        let hi = ((center + radius + epsilon) as i64).min((full_shape[axis] - 1) as i64);
        if center + radius < 0.0 || center - radius > (full_shape[axis] - 1) as f32 || lo > hi {
            return Err(ImageError::CasaSelection(
                "saved ellipsoid does not contain pixels inside the image".into(),
            ));
        }
        start[axis] = lo as usize;
        shape[axis] = (hi - lo + 1) as usize;
    }
    Ok((start, shape))
}

fn wc_box_corner(
    coords: &CoordinateSystem,
    shape: &[usize],
    axes: &[usize],
    quantities: &[Quantity],
    modes: &[f64],
    origin: usize,
    low: bool,
) -> Result<Vec<usize>, ImageError> {
    let reference = coords.reference_value();
    let mut world = reference.clone();
    let reference_pixels = coords.reference_pixel();
    let units = coords.world_axis_units();
    let mut has_world_bounds = false;
    for (&axis, quantity) in axes.iter().zip(quantities) {
        if !matches!(quantity.unit().name(), "pix" | "frac" | "default") {
            has_world_bounds = true;
            let world_axis = coords.pixel_axis_to_world_axis(axis).ok_or_else(|| {
                ImageError::InvalidMetadata(format!("WCBox pixel axis {axis} lacks a world axis"))
            })?;
            let unit = Unit::new(&units[world_axis])
                .map_err(|err| ImageError::InvalidMetadata(err.to_string()))?;
            world[world_axis] = quantity
                .get_value_in(&unit)
                .map_err(|err| ImageError::InvalidMetadata(err.to_string()))?;
        }
    }
    if has_world_bounds && modes.iter().any(|&mode| mode != 1.0) {
        // WCBox passes its stored mode positions directly after mapping quantities.
        make_region_world_absolute(coords, shape, modes, &reference, &mut world)?;
    }
    let converted = coords.to_pixel(&world)?;
    let mut result = shape
        .iter()
        .map(|len| if low { 0 } else { len - 1 })
        .collect::<Vec<_>>();
    for (position, ((&axis, quantity), &mode)) in axes.iter().zip(quantities).zip(modes).enumerate()
    {
        let unit = quantity.unit().name();
        if unit == "default" {
            continue;
        }
        let limit = (shape[axis] - 1) as f64;
        let relative = match mode {
            1.0 => 0.0,
            // WCBox looks up the reference pixel by the quantity's position.
            2.0 => reference_pixels[position],
            3.0 => shape[axis] as f64 * 0.5,
            _ => return Err(ImageError::InvalidMetadata("invalid WCBox absrel".into())),
        };
        let pixel = match unit {
            "pix" => quantity.value() + relative - if mode == 1.0 { origin as f64 } else { 0.0 },
            "frac" => quantity.value() * shape[axis] as f64 + relative - f64::from(!low),
            _ => converted[axis],
        };
        // LCBox rounds both corners, then clips only the lower side of BLC
        // and the upper side of TRC. A disjoint box must stay disjoint.
        let pixel = (pixel + 0.5).trunc();
        if !pixel.is_finite() || (low && pixel > limit) || (!low && pixel < 0.0) {
            return Err(ImageError::CasaSelection(format!(
                "saved WCBox does not intersect the image on axis {axis}"
            )));
        }
        result[axis] = pixel.clamp(0.0, limit) as usize;
    }
    Ok(result)
}

fn make_region_world_absolute(
    coords: &CoordinateSystem,
    shape: &[usize],
    modes: &[f64],
    reference: &[f64],
    world: &mut [f64],
) -> Result<(), ImageError> {
    let offsets = world
        .iter()
        .enumerate()
        .map(|(axis, &value)| match modes.get(axis) {
            Some(2.0 | 3.0) => value,
            _ => 0.0,
        })
        .collect::<Vec<_>>();
    // WCRegion evaluates the image center even for reference-relative bounds.
    let center_pixel = shape
        .iter()
        .map(|&length| (length - 1) as f64 * 0.5)
        .collect::<Vec<_>>();
    let center = coords.to_world(&center_pixel)?;
    for (mode, anchor) in [(2.0, reference), (3.0, center.as_slice())] {
        if !modes.contains(&mode) {
            continue;
        }
        for i in 0..coords.n_coordinates() {
            let coord = coords.coordinate(i);
            let axes = coords
                .coordinate_world_axes(i)
                .expect("existing coordinate");
            if !axes
                .iter()
                .flatten()
                .any(|&axis| modes.get(axis) == Some(&mode))
            {
                continue;
            }
            let direction = if matches!(coord, CoordinateModel::Direction(_)) {
                let [Some(lon), Some(lat)] = axes.as_slice() else {
                    return Err(ImageError::InvalidMetadata(
                        "relative WCBox requires both direction axes in the image".into(),
                    ));
                };
                Some(offset_direction(
                    [anchor[*lon], anchor[*lat]],
                    [offsets[*lon], offsets[*lat]],
                ))
            } else {
                None
            };
            for (local_axis, axis) in axes.into_iter().enumerate() {
                if let Some(axis) = axis
                    && modes.get(axis) == Some(&mode)
                {
                    world[axis] = match direction {
                        Some(values) => values[local_axis],
                        // makeWorldAbsolute is a no-op for Stokes at the reference.
                        None if mode == 2.0 && matches!(coord, CoordinateModel::Stokes(_)) => {
                            offsets[axis]
                        }
                        None => anchor[axis] + offsets[axis],
                    };
                }
            }
        }
    }
    Ok(())
}

fn offset_direction(anchor: [f64; 2], offsets: [f64; 2]) -> [f64; 2] {
    // DirectionCoordinate::makeWorldAbsolute rotates the offset from the
    // equator to the anchor latitude. Relative longitude is scaled by cos(lat).
    let (sin_lat, cos_lat) = offsets[1].sin_cos();
    let (sin_lon, cos_lon) = (offsets[0] / cos_lat).sin_cos();
    let (sin_anchor, cos_anchor) = anchor[1].sin_cos();
    let x = cos_lat * cos_lon;
    let y = cos_lat * sin_lon;
    let equatorial = cos_anchor * x - sin_anchor * sin_lat;
    let polar = sin_anchor * x + cos_anchor * sin_lat;
    let longitude = anchor[0] + y.atan2(equatorial);
    [
        longitude.sin().atan2(longitude.cos()),
        polar.atan2(equatorial.hypot(y)),
    ]
}

fn read_quantum_list(val: Option<&Value>) -> Result<Vec<Quantity>, ImageError> {
    let Some(Value::Record(rec)) = val else {
        return Err(ImageError::InvalidMetadata(
            "saved region is missing a quantity list".into(),
        ));
    };
    rec.fields()
        .iter()
        .map(|field| match &field.value {
            Value::Record(value) => crate::beam::quantity_from_record(value),
            _ => Err(ImageError::InvalidMetadata(
                "invalid saved region quantity".into(),
            )),
        })
        .collect()
}

fn read_numeric_vec(val: Option<&Value>) -> Option<Vec<f64>> {
    match val? {
        Value::Array(ArrayValue::Float64(arr)) => Some(arr.iter().copied().collect()),
        Value::Array(ArrayValue::Float32(arr)) => Some(arr.iter().map(|&v| f64::from(v)).collect()),
        Value::Array(ArrayValue::Int32(arr)) => Some(arr.iter().map(|&v| f64::from(v)).collect()),
        Value::Array(ArrayValue::Int64(arr)) => Some(arr.iter().map(|&v| v as f64).collect()),
        _ => None,
    }
}

fn read_bool_vec(val: Option<&Value>, ndim: usize) -> Vec<bool> {
    if let Some(Value::Array(ArrayValue::Bool(arr))) = val
        && arr.len() == ndim
    {
        return arr.iter().copied().collect();
    }
    vec![false; ndim]
}
