// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared LEL (Lattice Expression Language) mathematical, broadcasting,
//! coordinate, and statistical helpers for `image_expr` and `immath`.

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, StokesCoordinate, StokesType,
};
use casa_core::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use casa_lattices::LatticeError;
use ndarray::{Axis, Dimension, IxDyn, ShapeBuilder, Zip};

use crate::error::ImageError;
use crate::image_expr::ImageExprValue;

pub(crate) fn mask_coordinate_axes(
    system: &CoordinateSystem,
) -> Vec<(CoordinateType, usize, String)> {
    (0..system.n_pixel_axes())
        .map(|axis| {
            let (index, local) = system.pixel_axis_owner(axis).expect("dense pixel axes");
            let coordinate = system.coordinate(index);
            let name = match coordinate {
                CoordinateModel::Direction(direction) => {
                    direction.conversion_direction_ref().to_string()
                }
                CoordinateModel::Spectral(spectral) => spectral.world_frequency_ref().to_string(),
                _ => coordinate
                    .axis_names()
                    .get(local)
                    .cloned()
                    .unwrap_or_default(),
            };
            (coordinate.coordinate_type(), local, name)
        })
        .collect()
}

/// Returns the combined output shape of two LEL operands.
///
/// A scalar (`[]`) broadcasts to any shape. Two non-scalar operands conform
/// when, after right-padding the shorter shape with trailing size-1 axes, each
/// axis either matches or has size `1` on at least one operand.
pub(crate) fn broadcast_shapes(lhs: &[usize], rhs: &[usize]) -> Result<Vec<usize>, ImageError> {
    if lhs.is_empty() {
        return Ok(rhs.to_vec());
    }
    if rhs.is_empty() {
        return Ok(lhs.to_vec());
    }
    let ndim = lhs.len().max(rhs.len());
    let mut out = Vec::with_capacity(ndim);
    for axis in 0..ndim {
        let l = lhs.get(axis).copied().unwrap_or(1);
        let r = rhs.get(axis).copied().unwrap_or(1);
        if l == r {
            out.push(l);
        } else if l == 1 {
            out.push(r);
        } else if r == 1 {
            out.push(l);
        } else {
            return Err(ImageError::ShapeMismatch {
                expected: lhs.to_vec(),
                got: rhs.to_vec(),
            });
        }
    }
    Ok(out)
}

/// Returns whether `source` can be broadcast to `target` without changing `target`.
pub(crate) fn can_broadcast_to(source: &[usize], target: &[usize]) -> bool {
    if source == target {
        return true;
    }
    if source.is_empty() {
        return true;
    }
    if source.len() > target.len() {
        return source[target.len()..].iter().all(|&d| d == 1)
            && source[..target.len()]
                .iter()
                .zip(target.iter())
                .all(|(&s, &t)| s == t || s == 1);
    }
    source
        .iter()
        .zip(target.iter())
        .all(|(&s, &t)| s == t || s == 1)
}

/// Broadcasts `arr` to `out_shape` using casacore trailing-axis and size-1
/// degenerate-axis extension rules.
pub(crate) fn broadcast_array<A: Clone>(
    arr: ArrayD<A>,
    out_shape: &[usize],
) -> Result<ArrayD<A>, ImageError> {
    if arr.shape() == out_shape {
        return Ok(arr);
    }
    if arr.ndim() == 0 {
        let scalar = arr
            .iter()
            .next()
            .cloned()
            .ok_or_else(|| ImageError::InvalidMetadata("empty 0-D scalar array".into()))?;
        return Ok(ArrayD::from_elem(IxDyn(out_shape).f(), scalar));
    }
    if !can_broadcast_to(arr.shape(), out_shape) {
        return Err(ImageError::ShapeMismatch {
            expected: out_shape.to_vec(),
            got: arr.shape().to_vec(),
        });
    }
    if out_shape.contains(&0) {
        return ArrayD::from_shape_vec(IxDyn(out_shape).f(), Vec::new()).map_err(|_| {
            ImageError::ShapeMismatch {
                expected: out_shape.to_vec(),
                got: arr.shape().to_vec(),
            }
        });
    }

    // ndarray broadcasts leading axes, whereas casacore/CASA appends trailing size-1 axes.
    let mut padded = Vec::with_capacity(out_shape.len());
    for axis in 0..out_shape.len() {
        padded.push(arr.shape().get(axis).copied().unwrap_or(1));
    }
    let reshaped = arr
        .to_shape(IxDyn(&padded))
        .map_err(|_| ImageError::ShapeMismatch {
            expected: out_shape.to_vec(),
            got: padded.clone(),
        })?;
    let view = reshaped
        .broadcast(IxDyn(out_shape))
        .ok_or_else(|| ImageError::ShapeMismatch {
            expected: out_shape.to_vec(),
            got: padded,
        })?;
    let fill = view
        .iter()
        .next()
        .cloned()
        .ok_or_else(|| ImageError::InvalidMetadata("empty broadcast target".into()))?;
    let mut out = ArrayD::from_elem(IxDyn(out_shape).f(), fill);
    out.assign(&view);
    Ok(out)
}

/// Broadcasts `arr` to `out_shape`, mapping mismatches into `LatticeError`.
pub(crate) fn broadcast_lattice_array<A: Clone>(
    arr: ArrayD<A>,
    out_shape: &[usize],
) -> Result<ArrayD<A>, LatticeError> {
    if arr.shape() == out_shape {
        return Ok(arr);
    }
    let src_shape = arr.shape().to_vec();
    broadcast_array(arr, out_shape).map_err(|_| LatticeError::ShapeMismatch {
        expected: out_shape.to_vec(),
        got: src_shape,
    })
}

/// Combines two optional full-lattice masks using AND semantics and LEL broadcasting.
pub(crate) fn combine_optional_masks(
    lhs_mask: Option<ArrayD<bool>>,
    lhs_shape: &[usize],
    rhs_mask: Option<ArrayD<bool>>,
    rhs_shape: &[usize],
) -> Result<Option<ArrayD<bool>>, ImageError> {
    let out_shape = broadcast_shapes(lhs_shape, rhs_shape)?;
    match (lhs_mask, rhs_mask) {
        (None, None) => Ok(None),
        (Some(mask), None) => broadcast_array(mask, &out_shape).map(Some),
        (None, Some(mask)) => broadcast_array(mask, &out_shape).map(Some),
        (Some(lhs), Some(rhs)) => {
            let lhs = broadcast_array(lhs, &out_shape)?;
            let rhs = broadcast_array(rhs, &out_shape)?;
            Ok(Some(
                Zip::from(&lhs)
                    .and(&rhs)
                    .map_collect(|&lhs, &rhs| lhs && rhs),
            ))
        }
    }
}

/// Combines two optional masks already matching an output slice shape.
pub(crate) fn combine_slice_masks(
    lhs: Option<ArrayD<bool>>,
    rhs: Option<ArrayD<bool>>,
) -> Option<ArrayD<bool>> {
    match (lhs, rhs) {
        (None, None) => None,
        (Some(mask), None) | (None, Some(mask)) => Some(mask),
        (Some(lhs), Some(rhs)) => Some(
            Zip::from(&lhs)
                .and(&rhs)
                .map_collect(|&lhs, &rhs| lhs && rhs),
        ),
    }
}

/// Maps an output slice request `(start, shape, stride)` onto a narrower or
/// degenerate `child_shape`.
pub(crate) fn slice_for_broadcast_child(
    child_shape: &[usize],
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
    let mut child_start = Vec::with_capacity(child_shape.len());
    let mut child_shape_req = Vec::with_capacity(child_shape.len());
    let mut child_stride = Vec::with_capacity(child_shape.len());
    for (axis, &dim) in child_shape.iter().enumerate() {
        let req_len = shape.get(axis).copied().unwrap_or(1);
        if dim == 1 {
            child_start.push(0);
            child_shape_req.push(req_len.min(1));
            child_stride.push(1);
        } else {
            child_start.push(start.get(axis).copied().unwrap_or(0));
            child_shape_req.push(req_len);
            child_stride.push(stride.get(axis).copied().unwrap_or(1));
        }
    }
    (child_start, child_shape_req, child_stride)
}

/// Returns `true` when `rhs` should supply expression metadata (higher rank or
/// non-degenerate match for `out_shape`).
pub(crate) fn choose_rhs_meta(
    lhs_shape: &[usize],
    rhs_shape: &[usize],
    out_shape: &[usize],
) -> bool {
    if lhs_shape.is_empty() {
        return true;
    }
    if rhs_shape.is_empty() {
        return false;
    }
    lhs_shape.len() < rhs_shape.len()
        || (lhs_shape.len() == rhs_shape.len() && lhs_shape != out_shape && rhs_shape == out_shape)
}

/// Evaluates casacore LEL `indexin(axis, indices)` over a target `(start, shape, stride)`
/// slice with 0-based `axis` and 0-based `indices`.
pub(crate) fn eval_indexin(
    axis: i64,
    indices: &[i64],
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<bool>, LatticeError> {
    let ndim = shape.len();
    if axis < 0 || (axis as usize) >= ndim {
        return Err(LatticeError::InvalidTraversal(
            "Axis argument in INDEXIN function outside array; (note axis is 0-relative!)".into(),
        ));
    }
    let ax = axis as usize;
    let axis_hits: Vec<bool> = (0..shape[ax])
        .map(|k| {
            let coord = (start[ax] + k * stride[ax]) as i64;
            indices.contains(&coord)
        })
        .collect();
    if ndim == 1 {
        return ArrayD::from_shape_vec(IxDyn(shape), axis_hits)
            .map_err(|e| LatticeError::InvalidTraversal(e.to_string()));
    }
    let mut line_shape = vec![1; ndim];
    line_shape[ax] = shape[ax];
    let line = ArrayD::from_shape_vec(IxDyn(&line_shape), axis_hits)
        .map_err(|e| LatticeError::InvalidTraversal(e.to_string()))?;
    let view = line
        .broadcast(IxDyn(shape))
        .ok_or_else(|| LatticeError::InvalidTraversal("indexin broadcast failed".into()))?;
    Ok(view.to_owned())
}

/// Extracts the pixel axis index and per-channel frequencies in Hz from `coords`.
pub(crate) fn spectral_frequencies_hz(
    coords: &CoordinateSystem,
    image_shape: &[usize],
) -> Result<(usize, Vec<f64>), ImageError> {
    if let Some(index) = coords.find_coordinate(casa_coordinates::CoordinateType::Spectral)
        && let CoordinateModel::Spectral(spectral) = coords.coordinate(index)
        && let [Some(axis)] = coords
            .coordinate_pixel_axes(index)
            .expect("existing coordinate")
            .as_slice()
    {
        let &n_chan = image_shape.get(*axis).ok_or_else(|| {
            ImageError::InvalidMetadata(
                "spectral coordinate pixel axis exceeds image dimensions".into(),
            )
        })?;
        let freqs = (0..n_chan)
            .map(|ch| spectral.world_frequency_hz(ch as f64))
            .collect::<Result<_, _>>()?;
        return Ok((*axis, freqs));
    }
    Err(ImageError::InvalidMetadata(
        "spectralindex requires images with a spectral coordinate".into(),
    ))
}

#[inline]
fn spectral_index_scalar(s1: f64, s2: f64, f1: f64, f2: f64) -> f64 {
    if f1 == f2 {
        return 0.0;
    }
    if s1 == 0.0 || s2 == 0.0 {
        return 0.0;
    }
    let denom = (f1 / f2).ln();
    if denom == 0.0 {
        0.0
    } else {
        (s1 / s2).ln() / denom
    }
}

/// Evaluates `spectralindex(lhs, rhs) = ln(lhs / rhs) / ln(f1 / f2)` on an output slice.
#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_spectral_index<T: ImageExprValue>(
    lhs: ArrayD<T>,
    rhs: ArrayD<T>,
    spec_axis_a: usize,
    freqs_a: &[f64],
    spec_axis_b: usize,
    freqs_b: &[f64],
    start: &[usize],
    stride: &[usize],
) -> ArrayD<T> {
    if freqs_a.len() <= 1 && freqs_b.len() <= 1 {
        let f1 = freqs_a.first().copied().unwrap_or(0.0);
        let f2 = freqs_b.first().copied().unwrap_or(0.0);
        return Zip::from(&lhs).and(&rhs).map_collect(|&a, &b| {
            T::from_f64_lossy(spectral_index_scalar(
                a.to_f64_lossy(),
                b.to_f64_lossy(),
                f1,
                f2,
            ))
        });
    }

    if spec_axis_a == spec_axis_b
        && spec_axis_a < lhs.ndim()
        && lhs.shape().get(spec_axis_a) == rhs.shape().get(spec_axis_a)
    {
        let nchan = lhs.shape()[spec_axis_a];
        let inv_ln: Vec<f64> = (0..nchan)
            .map(|c| {
                let f1 = if freqs_a.len() <= 1 {
                    freqs_a.first().copied().unwrap_or(0.0)
                } else {
                    freqs_a[start[spec_axis_a] + c * stride[spec_axis_a]]
                };
                let f2 = if freqs_b.len() <= 1 {
                    freqs_b.first().copied().unwrap_or(0.0)
                } else {
                    freqs_b[start[spec_axis_b] + c * stride[spec_axis_b]]
                };
                if f1 == f2 {
                    0.0
                } else {
                    let denom = (f1 / f2).ln();
                    if denom == 0.0 { 0.0 } else { 1.0 / denom }
                }
            })
            .collect();
        let mut out = ArrayD::from_elem(IxDyn(lhs.shape()).f(), T::default_value());
        let ax = Axis(spec_axis_a);
        for (((lhs_c, rhs_c), mut out_c), &scale) in lhs
            .axis_iter(ax)
            .zip(rhs.axis_iter(ax))
            .zip(out.axis_iter_mut(ax))
            .zip(inv_ln.iter())
        {
            if scale != 0.0 {
                let t_scale = T::from_f64_lossy(scale);
                let zero = T::default_value();
                if lhs_c.strides() == out_c.strides()
                    && rhs_c.strides() == out_c.strides()
                    && let (Some(dst_s), Some(lhs_s), Some(rhs_s)) = (
                        out_c.as_slice_memory_order_mut(),
                        lhs_c.as_slice_memory_order(),
                        rhs_c.as_slice_memory_order(),
                    )
                {
                    for ((d, &a), &b) in dst_s.iter_mut().zip(lhs_s.iter()).zip(rhs_s.iter()) {
                        *d = if a.to_f64_lossy() == 0.0 || b.to_f64_lossy() == 0.0 {
                            zero
                        } else {
                            (a / b).expr_log() * t_scale
                        };
                    }
                } else {
                    Zip::from(&mut out_c)
                        .and(&lhs_c)
                        .and(&rhs_c)
                        .for_each(|dst, &a, &b| {
                            *dst = if a.to_f64_lossy() == 0.0 || b.to_f64_lossy() == 0.0 {
                                zero
                            } else {
                                (a / b).expr_log() * t_scale
                            };
                        });
                }
            }
        }
        return out;
    }

    let shape = lhs.shape().to_vec();
    let mut out = ArrayD::from_elem(IxDyn(&shape), T::default_value());
    for (((idx, &a), &b), dst) in lhs.indexed_iter().zip(rhs.iter()).zip(out.iter_mut()) {
        let idx_slice = idx.as_array_view();
        let f1 = if freqs_a.len() <= 1 {
            freqs_a.first().copied().unwrap_or(0.0)
        } else {
            let p = start[spec_axis_a] + idx_slice[spec_axis_a] * stride[spec_axis_a];
            freqs_a[p]
        };
        let f2 = if freqs_b.len() <= 1 {
            freqs_b.first().copied().unwrap_or(0.0)
        } else {
            let p = start[spec_axis_b] + idx_slice[spec_axis_b] * stride[spec_axis_b];
            freqs_b[p]
        };
        *dst = T::from_f64_lossy(spectral_index_scalar(
            a.to_f64_lossy(),
            b.to_f64_lossy(),
            f1,
            f2,
        ));
    }
    out
}

/// Evaluates `pa(u, q) = 0.5 * atan2(u, q) * 180 / pi` in degrees.
pub(crate) fn eval_pa<T: ImageExprValue>(u: &ArrayD<T>, q: &ArrayD<T>) -> ArrayD<T> {
    let half_deg_per_rad = T::from_f64_lossy(90.0 / std::f64::consts::PI);
    Zip::from(u)
        .and(q)
        .map_collect(|&u_val, &q_val| u_val.expr_atan2(q_val) * half_deg_per_rad)
}

/// Replaces any Stokes coordinate in `coords` with `[StokesType::Pangle]`.
pub(crate) fn replace_stokes_with_pangle(coords: &mut CoordinateSystem) {
    if let Some(idx) = coords.find_coordinate(CoordinateType::Stokes) {
        coords.replace_coordinate(idx, StokesCoordinate::new(vec![StokesType::Pangle]));
    }
}

/// Validates integer rebin factors and computes `(out_shape, padded_factors)`.
pub(crate) fn rebin_shape(
    in_shape: &[usize],
    factors: &[usize],
) -> Result<(Vec<usize>, Vec<usize>), ImageError> {
    if factors.len() != in_shape.len() {
        return Err(ImageError::InvalidMetadata(format!(
            "Binning vector and lattice must have same dimension (got {} for {}D image)",
            factors.len(),
            in_shape.len()
        )));
    }
    if factors.contains(&0) {
        return Err(ImageError::InvalidMetadata(
            "all rebin binning factors must be positive integers >= 1".into(),
        ));
    }
    let mut padded = Vec::with_capacity(in_shape.len());
    let mut out_shape = Vec::with_capacity(in_shape.len());
    for (axis, &dim) in in_shape.iter().enumerate() {
        let f = factors.get(axis).copied().unwrap_or(1);
        padded.push(f);
        out_shape.push(dim.div_ceil(f));
    }
    Ok((out_shape, padded))
}

fn record_axis_scalar(
    coord_record: &RecordValue,
    field_name: &str,
    local_axis: usize,
) -> Option<f64> {
    let read_from = |rec: &RecordValue| -> Option<f64> {
        match rec.get(field_name) {
            Some(Value::Scalar(ScalarValue::Float64(v))) if local_axis == 0 => Some(*v),
            Some(Value::Scalar(ScalarValue::Float32(v))) if local_axis == 0 => Some(f64::from(*v)),
            Some(Value::Array(ArrayValue::Float64(arr))) => arr.iter().nth(local_axis).copied(),
            Some(Value::Array(ArrayValue::Float32(arr))) => {
                arr.iter().nth(local_axis).map(|v| f64::from(*v))
            }
            _ => None,
        }
    };
    read_from(coord_record).or_else(|| {
        ["wcs", "tabular"].iter().find_map(|sub| {
            let Some(Value::Record(inner)) = coord_record.get(sub) else {
                return None;
            };
            read_from(inner)
        })
    })
}

/// Updates `CoordinateSystem` reference pixels and increments for integer rebin `factors`.
pub(crate) fn rebin_coordinates(
    coords: &mut CoordinateSystem,
    factors: &[usize],
) -> Result<(), ImageError> {
    if coords.n_pixel_axes() == 0 {
        return Ok(());
    }
    let mut record = coords.to_record();
    *coords = rebin_coordinate_record(&mut record, factors)?;
    Ok(())
}

pub(crate) fn rebin_coordinate_record(
    record: &mut RecordValue,
    factors: &[usize],
) -> Result<CoordinateSystem, ImageError> {
    let placed = CoordinateSystem::record_coordinates(record)?;
    for coord in placed {
        if coord.field.starts_with("stokes") {
            continue;
        }
        let Some(Value::Record(coord_record)) = record.get_mut(&coord.field) else {
            continue;
        };
        if let [Some(axis)] = coord.pixel_axes.as_slice()
            && let Some(&factor) = factors.get(*axis).filter(|&&factor| factor > 1)
        {
            let table = if coord.field.starts_with("tabular") {
                Some(&mut *coord_record)
            } else if let Some(Value::Record(table)) = coord_record.get_mut("tabular") {
                Some(table)
            } else {
                None
            };
            if let Some(table) = table
                && rebin_sampled_axis(table, factor)?
            {
                continue;
            }
        }
        let coord_record = coord_record.clone();
        for (local_axis, opt_pixel_axis) in coord.pixel_axes.iter().enumerate() {
            let Some(axis) = *opt_pixel_axis else {
                continue;
            };
            let f = factors.get(axis).copied().unwrap_or(1);
            if f <= 1 {
                continue;
            }
            let ff = f as f64;
            if let Some(old_crpix) = record_axis_scalar(&coord_record, "crpix", local_axis) {
                let new_crpix = (old_crpix - 0.5 * (ff - 1.0)) / ff;
                CoordinateSystem::set_reference_pixel_in_record(record, axis, new_crpix)?;
            }
            if let Some(old_cdelt) = record_axis_scalar(&coord_record, "cdelt", local_axis) {
                let new_cdelt = old_cdelt * ff;
                CoordinateSystem::set_increment_in_record(record, axis, new_cdelt)?;
            }
        }
    }
    CoordinateSystem::from_live_record(record).map_err(ImageError::from)
}

fn rebin_sampled_axis(table: &mut RecordValue, factor: usize) -> Result<bool, ImageError> {
    let reference = |field| {
        record_axis_scalar(table, field, 0)
            .ok_or_else(|| ImageError::InvalidMetadata(format!("sampled axis has no {field}")))
    };
    let Some(Value::Array(ArrayValue::Float64(worlds))) = table.get("worldvalues") else {
        return Ok(false);
    };
    if worlds.is_empty() {
        return Ok(false);
    }
    let (pixel, world, increment) = (
        reference("crpix")?,
        reference("crval")?,
        reference("cdelt")?,
    );
    let factor = factor as f64;
    let new_pixel = (pixel + 0.5) / factor - 0.5;
    let new_increment = increment * factor;
    // CASACORE changes the live reference pixel, then scales the samples
    // relative to the original reference world value.
    let shift = (pixel - new_pixel) * increment;
    let samples = worlds.mapv(|value| world + (value + shift - world) * factor);
    table.upsert("worldvalues", Value::Array(ArrayValue::Float64(samples)));
    for (field, value) in [("crpix", new_pixel), ("cdelt", new_increment)] {
        table.upsert(field, Value::Array(ArrayValue::from_f64_vec(vec![value])));
    }
    Ok(true)
}

/// Maps a requested rebinned slice `(start, shape, stride)` to the contiguous
/// child slice required to compute every output block.
pub(crate) fn rebin_child_slice(
    in_shape: &[usize],
    factors: &[usize],
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
    let ndim = in_shape.len();
    if shape.contains(&0) {
        return (vec![0; ndim], vec![0; ndim], vec![1; ndim]);
    }
    let mut c_start = Vec::with_capacity(ndim);
    let mut c_shape = Vec::with_capacity(ndim);
    let c_stride = vec![1; ndim];
    for axis in 0..ndim {
        let f = factors[axis];
        let lo = start[axis] * f;
        let last_out = start[axis] + (shape[axis] - 1) * stride[axis];
        let hi = ((last_out + 1) * f).min(in_shape[axis]);
        c_start.push(lo);
        c_shape.push(hi.saturating_sub(lo));
    }
    (c_start, c_shape, c_stride)
}

fn c_strides(shape: &[usize]) -> Vec<usize> {
    let mut strides = vec![1; shape.len()];
    for axis in (0..shape.len().saturating_sub(1)).rev() {
        strides[axis] = strides[axis + 1] * shape[axis + 1];
    }
    strides
}

#[allow(clippy::too_many_arguments)]
fn accumulate_rebin_block<T: ImageExprValue>(
    axis: usize,
    base: usize,
    los: &[usize],
    his: &[usize],
    in_strides: &[usize],
    values: &[T],
    mask: Option<&[bool]>,
    sum: &mut T,
    count: &mut usize,
) {
    if axis == 0 {
        let valid = mask.is_none_or(|m| m[base]);
        if valid {
            if *count == 0 {
                *sum = values[base];
            } else {
                *sum = *sum + values[base];
            }
            *count += 1;
        }
        return;
    }
    let axis = axis - 1;
    let stride = in_strides[axis];
    for coord in los[axis]..his[axis] {
        accumulate_rebin_block(
            axis,
            base + coord * stride,
            los,
            his,
            in_strides,
            values,
            mask,
            sum,
            count,
        );
    }
}

/// Evaluates rebinned averages for every block in `out_shape`, respecting
/// partial trailing bins and pixel masks.
pub(crate) fn rebin_reduce_slice<T: ImageExprValue>(
    in_values: &ArrayD<T>,
    in_mask: Option<&ArrayD<bool>>,
    factors: &[usize],
    out_shape: &[usize],
    out_stride: &[usize],
) -> (ArrayD<T>, Option<ArrayD<bool>>) {
    let ndim = out_shape.len();
    if out_shape.contains(&0) {
        let empty =
            ArrayD::from_shape_vec(IxDyn(out_shape), Vec::new()).expect("empty rebin slice");
        let empty_mask = in_mask.map(|_| {
            ArrayD::from_shape_vec(IxDyn(out_shape), Vec::new()).expect("empty rebin mask")
        });
        return (empty, empty_mask);
    }

    let in_shape = in_values.shape();
    let in_strides = c_strides(in_shape);
    let val_std = in_values.as_standard_layout();
    let val_slice = val_std.as_slice().expect("standard layout slice");
    let mask_std = in_mask.map(|m| m.as_standard_layout());
    let mask_slice = mask_std
        .as_ref()
        .map(|m| m.as_slice().expect("standard layout mask slice"));

    let out_len: usize = out_shape.iter().product();
    let mut out_vals = Vec::with_capacity(out_len);
    let mut out_mask = mask_slice.map(|_| Vec::with_capacity(out_len));
    let mut out_coord = vec![0usize; ndim];
    let mut los = vec![0usize; ndim];
    let mut his = vec![0usize; ndim];

    for _ in 0..out_len {
        for axis in 0..ndim {
            let f = factors[axis];
            let block_idx = out_coord[axis] * out_stride[axis];
            los[axis] = block_idx * f;
            his[axis] = ((block_idx + 1) * f).min(in_shape[axis]);
        }
        let mut sum = T::default_value();
        let mut count = 0usize;
        accumulate_rebin_block(
            ndim,
            0,
            &los,
            &his,
            &in_strides,
            val_slice,
            mask_slice,
            &mut sum,
            &mut count,
        );
        if count > 0 {
            out_vals.push(sum.div_by_real(count as f64));
            if let Some(ref mut m) = out_mask {
                m.push(true);
            }
        } else {
            out_vals.push(T::default_value());
            if let Some(ref mut m) = out_mask {
                m.push(false);
            }
        }

        for axis in (0..ndim).rev() {
            out_coord[axis] += 1;
            if out_coord[axis] < out_shape[axis] {
                break;
            }
            out_coord[axis] = 0;
        }
    }

    let values = ArrayD::from_shape_vec(IxDyn(out_shape), out_vals).expect("rebin output shape");
    let mask =
        out_mask.map(|m| ArrayD::from_shape_vec(IxDyn(out_shape), m).expect("rebin mask shape"));
    (values, mask)
}

fn block_has_valid_pixel(
    axis: usize,
    base: usize,
    los: &[usize],
    his: &[usize],
    in_strides: &[usize],
    mask: &[bool],
) -> bool {
    if axis == los.len() {
        return mask[base];
    }
    let stride = in_strides[axis];
    for coord in los[axis]..his[axis] {
        if block_has_valid_pixel(axis + 1, base + coord * stride, los, his, in_strides, mask) {
            return true;
        }
    }
    false
}

/// Evaluates rebinned output mask blocks: valid iff at least one input pixel in the block is valid.
pub(crate) fn rebin_mask_slice(
    in_mask: &ArrayD<bool>,
    factors: &[usize],
    out_shape: &[usize],
    out_stride: &[usize],
) -> ArrayD<bool> {
    let ndim = out_shape.len();
    if out_shape.contains(&0) {
        return ArrayD::from_shape_vec(IxDyn(out_shape), Vec::new()).expect("empty rebin mask");
    }
    let in_shape = in_mask.shape();
    let in_strides = c_strides(in_shape);
    let mask_std = in_mask.as_standard_layout();
    let mask_slice = mask_std.as_slice().expect("standard layout mask slice");
    let out_len: usize = out_shape.iter().product();
    let mut out_mask = Vec::with_capacity(out_len);
    let mut out_coord = vec![0usize; ndim];
    let mut los = vec![0usize; ndim];
    let mut his = vec![0usize; ndim];

    for _ in 0..out_len {
        for axis in 0..ndim {
            let f = factors[axis];
            let block_idx = out_coord[axis] * out_stride[axis];
            los[axis] = block_idx * f;
            his[axis] = ((block_idx + 1) * f).min(in_shape[axis]);
        }
        out_mask.push(block_has_valid_pixel(
            0,
            0,
            &los,
            &his,
            &in_strides,
            mask_slice,
        ));
        for axis in (0..ndim).rev() {
            out_coord[axis] += 1;
            if out_coord[axis] < out_shape[axis] {
                break;
            }
            out_coord[axis] = 0;
        }
    }
    ArrayD::from_shape_vec(IxDyn(out_shape), out_mask).expect("rebin mask shape")
}

/// Online numerically stable Welford variance accumulator across chunks.
#[derive(Default, Clone, Copy)]
pub(crate) struct WelfordAccum {
    pub(crate) count: usize,
    pub(crate) mean: f64,
    pub(crate) m2: f64,
}

impl WelfordAccum {
    pub(crate) fn update(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    pub(crate) fn variance(self) -> f64 {
        if self.count > 1 {
            self.m2 / (self.count - 1) as f64
        } else {
            0.0
        }
    }

    pub(crate) fn stddev(self) -> f64 {
        self.variance().sqrt()
    }
}

/// Computes the fractile element using linear-time partial selection,
/// matching casacore `LatticeFractile` (even-`N` median/`0.5` averages the two
/// middle elements; total IEEE ordering handles unmasked NaNs safely).
pub(crate) fn fractile_in_place<T: ImageExprValue>(values: &mut [T], fraction: f64) -> Option<T> {
    if values.is_empty() {
        return None;
    }
    let n = values.len();
    let cmp = |lhs: &T, rhs: &T| lhs.to_f64_lossy().total_cmp(&rhs.to_f64_lossy());
    if (fraction - 0.5).abs() <= f64::EPSILON && n.is_multiple_of(2) {
        let mid_hi = n / 2;
        let (left, hi, _) = values.select_nth_unstable_by(mid_hi, cmp);
        let lo = left
            .iter()
            .max_by(|a, b| cmp(a, b))
            .expect("even length >= 2");
        return Some(T::from_f64_lossy(
            0.5 * (lo.to_f64_lossy() + hi.to_f64_lossy()),
        ));
    }
    let index = ((fraction * n.saturating_sub(1) as f64).floor() as usize).min(n - 1);
    let (_, kth, _) = values.select_nth_unstable_by(index, cmp);
    Some(*kth)
}
