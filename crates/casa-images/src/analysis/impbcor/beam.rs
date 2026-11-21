// SPDX-License-Identifier: LGPL-3.0-or-later
//! Full-rank and direction-plane primary beams for `impbcor`.

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
};
use casa_provider_contracts::{ParameterValue, parameter_array_to_casa};
use casa_types::{ArrayD, ArrayValue};
use ndarray::{Axis, Slice};

use crate::analysis::{axes::direction_plane_axes, imsubimage::region::SubimageSelection};
use crate::{ImageError, ImagePixel, PagedImage};

pub(super) struct SelectedBeam<T> {
    pub(super) pixels: ArrayD<T>,
    pub(super) mask: Option<ArrayD<bool>>,
    pub(super) input_shape: Vec<usize>,
}

pub(super) fn array<T: ImagePixel + From<f32>>(
    target: &PagedImage<T>,
    values: &[ParameterValue],
    selection: &SubimageSelection,
) -> Result<SelectedBeam<T>, ImageError> {
    direction(target.coordinates())?;
    let values = parameter_array_to_casa(values)
        .map_err(|message| ImageError::CasaSelection(message.into()))?;
    let ArrayValue::Float64(mut values) = values else {
        return Err(ImageError::CasaSelection(
            "pbimage pixels must be a floating-point array".into(),
        ));
    };
    let target_axes = direction_plane_axes(target)?;
    let input_shape = values.shape().to_vec();
    let plane = if input_shape == target.shape() {
        None
    } else if values.ndim() == 2 {
        if plane_shape(target.shape(), target_axes) != input_shape.as_slice() {
            return Err(ImageError::CasaSelection(
                "Array shape does not equal image direction plane shape".into(),
            ));
        }
        Some(([0, 1], target_axes))
    } else {
        return Err(ImageError::CasaSelection(format!(
            "Primary beam array is of wrong shape ({input_shape:?})"
        )));
    };
    let (start, shape) = window(selection, values.shape(), plane);
    for (axis, (&start, &len)) in start.iter().zip(&shape).enumerate() {
        values.slice_axis_inplace(Axis(axis), Slice::from(start..start + len));
    }
    let pixels = align(values.mapv(|v| T::from(v as f32)), plane, target.ndim());
    Ok(SelectedBeam {
        pixels,
        mask: None,
        input_shape,
    })
}

pub(super) fn image<T: ImagePixel>(
    target: &PagedImage<T>,
    beam: &PagedImage<T>,
    selection: &SubimageSelection,
) -> Result<SelectedBeam<T>, ImageError> {
    let target_axes = direction_plane_axes(target)?;
    let beam_axes = direction_plane_axes(beam)?;
    let same_shape = target.shape() == beam.shape();
    let target_direction = direction(target.coordinates())?;
    let beam_direction = direction(beam.coordinates())?;
    let plane = if same_shape && beam_axes == target_axes {
        None
    } else if plane_shape(beam.shape(), beam_axes)
        .into_iter()
        .zip(plane_shape(target.shape(), target_axes))
        .all(|(beam, target)| beam == 1 || beam == target)
        && beam
            .shape()
            .iter()
            .enumerate()
            .all(|(axis, &length)| beam_axes.contains(&axis) || length == 1)
    {
        Some((beam_axes, target_axes))
    } else {
        return Err(ImageError::CasaSelection("Input image and primary beam image have different shapes. Cannot do primary beam correction.".into()));
    };
    if !matching_direction(target_direction, beam_direction, same_shape) {
        return Err(ImageError::CasaSelection("Direction coordinates of input image and primary beam image are different. Cannot do primary beam correction.".into()));
    }
    let (start, shape) = window(selection, beam.shape(), plane);
    let pixels = align(beam.get_slice(&start, &shape)?, plane, target.ndim());
    let mask = beam
        .get_mask_slice(&start, &shape, &vec![1; shape.len()])?
        .map(|mask| align(mask, plane, target.ndim()));
    Ok(SelectedBeam {
        pixels,
        mask,
        input_shape: beam.shape().to_vec(),
    })
}

fn plane_shape(shape: &[usize], axes: [usize; 2]) -> [usize; 2] {
    axes.map(|axis| shape[axis])
}

fn window(
    selection: &SubimageSelection,
    beam_shape: &[usize],
    plane: Option<([usize; 2], [usize; 2])>,
) -> (Vec<usize>, Vec<usize>) {
    let Some((source, target)) = plane else {
        return (selection.start.clone(), selection.shape.clone());
    };
    let mut start = vec![0; beam_shape.len()];
    let mut shape = vec![1; beam_shape.len()];
    for (source, target) in source.into_iter().zip(target) {
        if beam_shape[source] != 1 {
            start[source] = selection.start[target];
            shape[source] = selection.shape[target];
        }
    }
    (start, shape)
}

fn align<T>(
    mut pixels: ArrayD<T>,
    plane: Option<([usize; 2], [usize; 2])>,
    target_rank: usize,
) -> ArrayD<T> {
    let Some((source, target)) = plane else {
        return pixels;
    };
    for axis in (0..pixels.ndim())
        .rev()
        .filter(|axis| !source.contains(axis))
    {
        pixels = pixels.index_axis_move(Axis(axis), 0);
    }
    if (source[0] < source[1]) != (target[0] < target[1]) {
        pixels.swap_axes(0, 1);
    }
    for axis in (0..target_rank).filter(|axis| !target.contains(axis)) {
        pixels = pixels.insert_axis(Axis(axis));
    }
    pixels
}

fn direction(system: &CoordinateSystem) -> Result<&DirectionCoordinate, ImageError> {
    match system
        .find_coordinate(CoordinateType::Direction)
        .map(|i| system.coordinate(i))
    {
        Some(CoordinateModel::Direction(direction)) if direction.n_pixel_axes() == 2 => {
            Ok(direction)
        }
        _ => Err(ImageError::CasaSelection(
            "image must have both direction axes".into(),
        )),
    }
}

fn matching_direction(
    left: &DirectionCoordinate,
    right: &DirectionCoordinate,
    same_shape: bool,
) -> bool {
    // ImagePrimaryBeamCorrector permits FITS-roundtrip reference-value
    // differences for full images. LinearXform::near uses its own tolerance.
    let reference_tolerance = if same_shape { 1e-11 } else { 1e-6 };
    let linear_tolerance = 1e-6;
    let near = |a: f64, b: f64, tol: f64| a == b || (a - b).abs() <= tol * a.abs().max(b.abs());
    let same = |a: &[f64], b: &[f64], tol: f64| {
        a.len() == b.len() && a.iter().zip(b).all(|(&a, &b)| near(a, b, tol))
    };
    left.direction_ref() == right.direction_ref()
        && left.conversion_direction_ref() == right.conversion_direction_ref()
        && left.projection().projection_type() == right.projection().projection_type()
        && same(
            left.projection().parameters(),
            right.projection().parameters(),
            reference_tolerance,
        )
        && left.axis_names() == right.axis_names()
        && left.world_units() == right.world_units()
        && near(left.longpole(), right.longpole(), 1e-13)
        && near(left.latpole(), right.latpole(), 1e-13)
        && same(
            &left.reference_value(),
            &right.reference_value(),
            reference_tolerance,
        )
        && same(
            &left.reference_pixel(),
            &right.reference_pixel(),
            linear_tolerance,
        )
        && same(&left.increment(), &right.increment(), linear_tolerance)
        && left
            .pc_matrix()
            .iter()
            .zip(right.pc_matrix())
            .all(|(&a, &b)| near(a, b, linear_tolerance))
}

#[cfg(test)]
mod tests {
    use ndarray::array;

    use super::align;

    #[test]
    fn primary_beam_matches_target_longitude_and_latitude_order() {
        let pixels = array![[1, 2, 3], [4, 5, 6]].into_dyn();
        let aligned = align(pixels, Some(([0, 1], [1, 0])), 3);
        assert_eq!(
            aligned,
            array![[[1], [4]], [[2], [5]], [[3], [6]]].into_dyn()
        );
    }
}
