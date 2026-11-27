// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) fn patch_saved_coords_keyword(
    path: &Path,
    coords: casa_core::RecordValue,
) -> Result<(), ImageError> {
    let mut table = Table::open(TableOptions::new(path))?;
    table.keywords_mut().upsert("coords", Value::Record(coords));
    table.save_metadata_only(TableOptions::new(path))?;
    Ok(())
}

#[cfg(test)]
pub(super) fn all_indices(shape: &[usize]) -> Vec<Vec<usize>> {
    fn push_indices(
        shape: &[usize],
        axis: usize,
        current: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        if axis == shape.len() {
            out.push(current.clone());
            return;
        }
        for value in 0..shape[axis] {
            current.push(value);
            push_indices(shape, axis + 1, current, out);
            current.pop();
        }
    }
    let mut out = Vec::new();
    push_indices(shape, 0, &mut Vec::new(), &mut out);
    out
}

#[cfg(test)]
pub(super) fn collapse_moment_reference<T>(
    input: &ArrayD<T>,
    mask: Option<&ArrayD<bool>>,
    axis: usize,
    coords: &[f64],
    integrated_scale: f64,
    includepix: Option<[f64; 2]>,
    moment: i32,
) -> Result<(ArrayD<f32>, ArrayD<bool>), ImageError>
where
    T: Into<f64> + Copy,
{
    let mut out_shape = input.shape().to_vec();
    out_shape.remove(axis);
    let mut out = ArrayD::<f32>::zeros(IxDyn(&out_shape));
    let mut out_mask = ArrayD::<bool>::from_elem(IxDyn(&out_shape), false);
    for out_index in all_indices(&out_shape) {
        let mut samples = Vec::new();
        for (chan, coord) in coords.iter().enumerate().take(input.shape()[axis]) {
            let mut full_index = out_index.clone();
            full_index.insert(axis, chan);
            if let Some(mask) = mask
                && !mask[IxDyn(&full_index)]
            {
                continue;
            }
            let value = (*input.get(IxDyn(&full_index)).ok_or_else(|| {
                ImageError::InvalidMetadata("moment index out of bounds".to_string())
            })?)
            .into();
            if let Some([min, max]) = includepix
                && (value < min || value > max)
            {
                continue;
            }
            samples.push((value, *coord));
        }
        let npts = samples.len();
        let (result, valid) = if npts == 0 {
            (0.0, false)
        } else {
            match moment {
                -1 => (
                    samples.iter().map(|(value, _)| *value).sum::<f64>() / npts as f64,
                    true,
                ),
                0 => (
                    samples.iter().map(|(value, _)| *value).sum::<f64>() * integrated_scale,
                    true,
                ),
                1 | 2 => {
                    let s0 = samples.iter().map(|(value, _)| *value).sum::<f64>();
                    if s0 == 0.0 {
                        (0.0, false)
                    } else {
                        let mean = samples
                            .iter()
                            .map(|(value, coord)| value * coord)
                            .sum::<f64>()
                            / s0;
                        if moment == 1 {
                            (mean, true)
                        } else {
                            let variance = samples
                                .iter()
                                .map(|(value, coord)| value * (coord - mean).powi(2))
                                .sum::<f64>()
                                / s0;
                            if variance < 0.0 {
                                (0.0, false)
                            } else {
                                (variance.sqrt(), true)
                            }
                        }
                    }
                }
                3 => {
                    let mut values = samples.iter().map(|(value, _)| *value).collect::<Vec<_>>();
                    values.sort_by(|left, right| left.total_cmp(right));
                    if values.len() % 2 == 0 {
                        let upper = values.len() / 2;
                        ((values[upper - 1] + values[upper]) / 2.0, true)
                    } else {
                        (values[values.len() / 2], true)
                    }
                }
                _ => (0.0, false),
            }
        };
        out[IxDyn(&out_index)] = result as f32;
        out_mask[IxDyn(&out_index)] = valid;
    }
    Ok((
        out.insert_axis(ndarray::Axis(axis)).to_owned(),
        out_mask.insert_axis(ndarray::Axis(axis)).to_owned(),
    ))
}

pub(super) fn coordinate_for_axis(
    coords: &CoordinateSystem,
    axis: usize,
) -> Option<&casa_coordinates::CoordinateModel> {
    coords
        .pixel_axis_owner(axis)
        .map(|(index, _)| coords.coordinate(index))
}

pub(super) fn spectral_rest_frequency(coords: &CoordinateSystem) -> Option<f64> {
    for idx in 0..coords.n_coordinates() {
        let coord = coords.coordinate(idx);
        if let casa_coordinates::CoordinateModel::Spectral(spectral) = coord {
            return Some(spectral.rest_frequency());
        }
    }
    None
}

pub(super) fn image_pixel_type<T: ImagePixel>() -> ImagePixelType {
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f32>() {
        ImagePixelType::Float32
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f64>() {
        ImagePixelType::Float64
    } else {
        ImagePixelType::Complex32
    }
}
