// SPDX-License-Identifier: LGPL-3.0-or-later
//! Decode the saved casacore region records accepted by imsubimage.

use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_types::quanta::Quantity;
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
                crtf_chans: None,
                crtf_stokes: None,
            })
        }
        "LCSlicer" | "LCBox" => {
            let (blc, trc) = decode_lc_box_or_slicer(region_name, rec, full_shape)?;
            let shapes = if full_shape.len() >= 2 {
                vec![SpatialShape::Box([
                    blc[plane[0]],
                    blc[plane[1]],
                    trc[plane[0]],
                    trc[plane[1]],
                ])]
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
                crtf_chans: None,
                crtf_stokes: None,
            })
        }
        "WCBox" => {
            let (blc, trc) = decode_wc_box(region_name, rec, target_coords, full_shape)?;
            let shapes = if full_shape.len() >= 2 {
                vec![SpatialShape::Box([
                    blc[plane[0]],
                    blc[plane[1]],
                    trc[plane[0]],
                    trc[plane[1]],
                ])]
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
                crtf_chans: None,
                crtf_stokes: None,
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
    let blc_world = read_quantum_list(rec.get("blc"))?;
    let trc_world = read_quantum_list(rec.get("trc"))?;
    if blc_world.len() > coords.n_world_axes() || trc_world.len() > coords.n_world_axes() {
        return Err(ImageError::InvalidMetadata(format!(
            "saved WCBox '{name}' world dimension mismatch"
        )));
    }
    let mut full_w0 = coords.to_world(&vec![0.0; coords.n_pixel_axes()])?;
    let mut full_w1 = full_w0.clone();
    for (i, v) in blc_world.iter().enumerate() {
        full_w0[i] = *v;
    }
    for (i, v) in trc_world.iter().enumerate() {
        full_w1[i] = *v;
    }
    let p0 = coords.to_pixel(&full_w0)?;
    let p1 = coords.to_pixel(&full_w1)?;
    let mut blc = Vec::with_capacity(full_shape.len());
    let mut trc = Vec::with_capacity(full_shape.len());
    for axis in 0..full_shape.len() {
        let len = full_shape[axis];
        let lo = p0[axis].min(p1[axis]).round().clamp(0.0, (len - 1) as f64) as usize;
        let hi = p0[axis].max(p1[axis]).round().clamp(0.0, (len - 1) as f64) as usize;
        blc.push(lo);
        trc.push(hi);
    }
    Ok((blc, trc))
}

fn read_quantum_list(val: Option<&Value>) -> Result<Vec<f64>, ImageError> {
    let Some(Value::Record(rec)) = val else {
        return Err(ImageError::InvalidMetadata(
            "WCBox missing blc/trc record".into(),
        ));
    };
    let mut out = Vec::new();
    for field in rec.fields() {
        if !field.name.starts_with('*') {
            continue;
        }
        if let Value::Record(qrec) = &field.value {
            let v = match qrec.get("value") {
                Some(Value::Scalar(ScalarValue::Float64(x))) => *x,
                Some(Value::Scalar(ScalarValue::Float32(x))) => f64::from(*x),
                Some(Value::Scalar(ScalarValue::Int32(x))) => f64::from(*x),
                _ => {
                    return Err(ImageError::InvalidMetadata(
                        "invalid WCBox quantum value".into(),
                    ));
                }
            };
            let u = match qrec.get("unit") {
                Some(Value::Scalar(ScalarValue::String(s))) => s.as_str(),
                _ => "",
            };
            if u.eq_ignore_ascii_case("pix") || u.is_empty() {
                out.push(v);
            } else {
                let q =
                    Quantity::new(v, u).map_err(|e| ImageError::InvalidMetadata(e.to_string()))?;
                out.push(q.get_si_value());
            }
        }
    }
    Ok(out)
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
