// SPDX-License-Identifier: LGPL-3.0-or-later
//! Decode the saved casacore region records accepted by imsubimage.

use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Dimension;

use crate::{AnyPagedImage, ImageError, PolygonRegion};

use super::{ParsedRegion, SpatialShape};

pub(super) fn load_saved_region(
    image_path: &Path,
    region_name: &str,
    target_coords: &CoordinateSystem,
    full_shape: &[usize],
    plane: [usize; 2],
) -> Result<ParsedRegion, ImageError> {
    let rec = match AnyPagedImage::open(image_path)? {
        AnyPagedImage::Float32(img) => img.get_region_record(region_name)?,
        AnyPagedImage::Float64(img) => img.get_region_record(region_name)?,
        AnyPagedImage::Complex32(img) => img.get_region_record(region_name)?,
        AnyPagedImage::Complex64(img) => img.get_region_record(region_name)?,
    };
    parse_saved_region(region_name, &rec, target_coords, full_shape, plane)
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
        "WCPolygon" | "WCUnion" => {
            let poly = PolygonRegion::from_record(region_name, rec, target_coords)?;
            let mask = poly.mask(target_coords, full_shape)?;
            let mut min_pt = full_shape.to_vec();
            let mut max_pt = vec![0usize; full_shape.len()];
            let mut any_true = false;
            for (idx, &valid) in mask.indexed_iter() {
                if valid {
                    any_true = true;
                    for (ax, &pos) in idx.slice().iter().enumerate() {
                        min_pt[ax] = min_pt[ax].min(pos);
                        max_pt[ax] = max_pt[ax].max(pos);
                    }
                }
            }
            if !any_true {
                return Err(ImageError::CasaSelection(format!(
                    "saved region '{region_name}' selects no pixels in image"
                )));
            }
            let shape: Vec<usize> = min_pt
                .iter()
                .zip(max_pt.iter())
                .map(|(lo, hi)| hi - lo + 1)
                .collect();
            Ok(ParsedRegion {
                shapes: Vec::new(),
                saved_polygon_mask: Some(mask),
                saved_bounds: Some((min_pt, shape)),
            })
        }
        "LCSlicer" | "LCBox" => {
            let (blc, trc) = decode_lc_box_or_slicer(region_name, rec, full_shape)?;
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
                shapes,
                saved_polygon_mask: None,
                saved_bounds: Some((blc, shape)),
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
                shapes,
                saved_polygon_mask: None,
                saved_bounds: Some((blc, shape)),
            })
        }
        other => Err(ImageError::InvalidMetadata(format!(
            "unsupported saved region '{region_name}' of class {other:?}"
        ))),
    }
}

fn decode_lc_box_or_slicer(
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
    let origin = usize::from(matches!(
        rec.get("oneRel"),
        Some(Value::Scalar(ScalarValue::Bool(true)))
    ));
    let Some(Value::Record(source_record)) = rec.get("coordinates") else {
        return Err(ImageError::InvalidMetadata(
            "WCBox has no coordinate system".into(),
        ));
    };
    let saved_coords = CoordinateSystem::from_record(source_record)?;
    let names = |system: &CoordinateSystem| {
        (0..system.n_coordinates())
            .flat_map(|i| system.coordinate(i).axis_names())
            .collect::<Vec<_>>()
    };
    let saved_names = names(&saved_coords);
    let target_names = names(coords);
    let axes = read_numeric_vec(rec.get("pixelAxes"))
        .ok_or_else(|| ImageError::InvalidMetadata("WCBox has no pixelAxes".into()))?
        .into_iter()
        .map(|axis| {
            (axis as usize)
                .checked_sub(origin)
                .and_then(|i| saved_names.get(i))
                .and_then(|name| target_names.iter().position(|candidate| candidate == name))
                .ok_or_else(|| {
                    ImageError::InvalidMetadata(format!(
                        "WCBox {name:?} has an unmatched pixel axis {axis}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut modes = read_numeric_vec(rec.get("absrel")).unwrap_or_default();
    if modes.is_empty() {
        modes.resize(axes.len(), 1.0);
    }
    let mut bounds = Vec::new();
    for (key, low) in [("blc", true), ("trc", false)] {
        let quantities = read_quantum_list(rec.get(key))?;
        if quantities.len() != axes.len() || modes.len() != axes.len() {
            return Err(ImageError::InvalidMetadata(format!(
                "WCBox {name:?} dimension mismatch"
            )));
        }
        bounds.push(wc_box_corner(
            coords,
            full_shape,
            &axes,
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

fn wc_box_corner(
    coords: &CoordinateSystem,
    shape: &[usize],
    axes: &[usize],
    quantities: &[Quantity],
    modes: &[f64],
    origin: usize,
    low: bool,
) -> Result<Vec<usize>, ImageError> {
    let mut world = (0..coords.n_coordinates())
        .flat_map(|i| coords.coordinate(i).reference_value())
        .collect::<Vec<_>>();
    let reference_pixels = (0..coords.n_coordinates())
        .flat_map(|i| coords.coordinate(i).reference_pixel())
        .collect::<Vec<_>>();
    let units = (0..coords.n_coordinates())
        .flat_map(|i| coords.coordinate(i).axis_units())
        .collect::<Vec<_>>();
    for ((&axis, quantity), &mode) in axes.iter().zip(quantities).zip(modes) {
        if !matches!(quantity.unit().name(), "pix" | "frac" | "default") {
            if mode != 1.0 {
                return Err(ImageError::InvalidMetadata(
                    "relative WCBox world bounds are unsupported".into(),
                ));
            }
            let unit = Unit::new(&units[axis])
                .map_err(|err| ImageError::InvalidMetadata(err.to_string()))?;
            world[axis] = quantity
                .get_value_in(&unit)
                .map_err(|err| ImageError::InvalidMetadata(err.to_string()))?;
        }
    }
    let converted = coords.to_pixel(&world)?;
    let mut result = shape
        .iter()
        .map(|len| if low { 0 } else { len - 1 })
        .collect::<Vec<_>>();
    for ((&axis, quantity), &mode) in axes.iter().zip(quantities).zip(modes) {
        let unit = quantity.unit().name();
        if unit == "default" {
            continue;
        }
        let limit = (shape[axis] - 1) as f64;
        let relative = match mode {
            1.0 => 0.0,
            2.0 => reference_pixels[axis],
            3.0 => shape[axis] as f64 * 0.5,
            _ => return Err(ImageError::InvalidMetadata("invalid WCBox absrel".into())),
        };
        let pixel = match unit {
            "pix" => quantity.value() + relative - if mode == 1.0 { origin as f64 } else { 0.0 },
            "frac" => quantity.value() * shape[axis] as f64 + relative - f64::from(!low),
            _ => converted[axis],
        };
        result[axis] = pixel.round().clamp(0.0, limit) as usize;
    }
    Ok(result)
}

fn read_quantum_list(val: Option<&Value>) -> Result<Vec<Quantity>, ImageError> {
    let Some(Value::Record(rec)) = val else {
        return Err(ImageError::InvalidMetadata(
            "WCBox missing blc/trc record".into(),
        ));
    };
    rec.fields()
        .iter()
        .map(|field| match &field.value {
            Value::Record(value) => crate::beam::quantity_from_record(value),
            _ => Err(ImageError::InvalidMetadata(
                "invalid WCBox quantum value".into(),
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
