// SPDX-License-Identifier: LGPL-3.0-or-later
//! Full-rank and direction-plane primary beams for `impbcor`.

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
};
use casa_core::ArrayD;
use ndarray::{Axis, Slice};

use crate::analysis::{
    axes::{StridedSection, direction_plane_axes},
    imsubimage::region::SubimageSelection,
};
use crate::{ImageError, ImageInterface, ImagePixel};

pub(super) struct SelectedBeam<T> {
    pub(super) pixels: ArrayD<T>,
    pub(super) mask: Option<ArrayD<bool>>,
    pub(super) input_shape: Vec<usize>,
}

pub(super) fn array<T: ImagePixel + From<f32>>(
    target: &dyn ImageInterface<T>,
    values: &ArrayD<f64>,
    selection: &SubimageSelection,
) -> Result<SelectedBeam<T>, ImageError> {
    let input_shape = values.shape().to_vec();
    let layout = BeamLayout::for_array(target, &input_shape)?;
    let stride = vec![1; target.ndim()];
    let section = StridedSection {
        start: &selection.start,
        shape: &selection.shape,
        stride: &stride,
    };
    let pixels = layout
        .read_array(values, &section)
        .mapv(|value| T::from(value as f32));
    Ok(SelectedBeam {
        pixels,
        mask: None,
        input_shape,
    })
}

pub(super) fn image<T: ImagePixel>(
    target: &dyn ImageInterface<T>,
    beam: &dyn ImageInterface<T>,
    selection: &SubimageSelection,
) -> Result<SelectedBeam<T>, ImageError> {
    let layout = BeamLayout::for_image(target, beam)?;
    let stride = vec![1; target.ndim()];
    let section = StridedSection {
        start: &selection.start,
        shape: &selection.shape,
        stride: &stride,
    };
    let (pixels, mask) = layout.read_image(beam, &section)?;
    Ok(SelectedBeam {
        pixels,
        mask,
        input_shape: beam.shape().to_vec(),
    })
}

pub(in crate::analysis) struct BeamLayout {
    plane: Option<([usize; 2], [usize; 2])>,
    target_rank: usize,
}

impl BeamLayout {
    pub(in crate::analysis) fn for_array<T: ImagePixel>(
        target: &dyn ImageInterface<T>,
        shape: &[usize],
    ) -> Result<Self, ImageError> {
        direction(target.coordinates())?;
        let target_axes = direction_plane_axes(target)?;
        let plane = if shape == target.shape() {
            None
        } else if shape.len() == 2 {
            if plane_shape(target.shape(), target_axes) != shape {
                return Err(ImageError::CasaSelection(
                    "Array shape does not equal image direction plane shape".into(),
                ));
            }
            Some(([0, 1], target_axes))
        } else {
            return Err(ImageError::CasaSelection(format!(
                "Primary beam array is of wrong shape ({shape:?})"
            )));
        };
        Ok(Self {
            plane,
            target_rank: target.ndim(),
        })
    }

    pub(in crate::analysis) fn for_image<T: ImagePixel>(
        target: &dyn ImageInterface<T>,
        beam: &dyn ImageInterface<T>,
    ) -> Result<Self, ImageError> {
        let target_axes = direction_plane_axes(target)?;
        let beam_axes = direction_plane_axes(beam)?;
        let same_shape = target.shape() == beam.shape();
        let target_direction = direction(target.coordinates())?;
        let beam_direction = direction(beam.coordinates())?;
        // CASA compares the local direction coordinates and then applies an
        // equal-shaped beam in stored pixel order.
        let plane = if same_shape {
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
        Ok(Self {
            plane,
            target_rank: target.ndim(),
        })
    }

    pub(in crate::analysis) fn read_array<T: Clone>(
        &self,
        beam: &ArrayD<T>,
        section: &StridedSection<'_>,
    ) -> ArrayD<T> {
        let (start, shape, stride) = self.window(section, beam.shape());
        let values = beam.slice_each_axis(|axis| {
            let axis = axis.axis.index();
            let stop = start[axis] + (shape[axis] - 1) * stride[axis] + 1;
            Slice::new(
                start[axis] as isize,
                Some(stop as isize),
                stride[axis] as isize,
            )
        });
        align(values.to_owned(), self.plane, self.target_rank)
    }

    pub(in crate::analysis) fn read_image<T: ImagePixel>(
        &self,
        beam: &dyn ImageInterface<T>,
        section: &StridedSection<'_>,
    ) -> Result<(ArrayD<T>, Option<ArrayD<bool>>), ImageError> {
        let (start, shape, stride) = self.window(section, beam.shape());
        let pixels = align(
            beam.get_slice(&start, &shape, &stride)?,
            self.plane,
            self.target_rank,
        );
        let mask = beam
            .get_mask_slice(&start, &shape, &stride)?
            .map(|mask| align(mask, self.plane, self.target_rank));
        Ok((pixels, mask))
    }

    fn window(
        &self,
        section: &StridedSection<'_>,
        beam_shape: &[usize],
    ) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
        let Some((source, target)) = self.plane else {
            let mut start = section.start.to_vec();
            let mut shape = section.shape.to_vec();
            let mut stride = section.stride.to_vec();
            for (axis, &length) in beam_shape.iter().enumerate() {
                if length == 1 {
                    start[axis] = 0;
                    shape[axis] = 1;
                    stride[axis] = 1;
                }
            }
            return (start, shape, stride);
        };
        let mut start = vec![0; beam_shape.len()];
        let mut shape = vec![1; beam_shape.len()];
        let mut stride = vec![1; beam_shape.len()];
        for (source, target) in source.into_iter().zip(target) {
            if beam_shape[source] != 1 {
                start[source] = section.start[target];
                shape[source] = section.shape[target];
                stride[source] = section.stride[target];
            }
        }
        (start, shape, stride)
    }
}

fn plane_shape(shape: &[usize], axes: [usize; 2]) -> [usize; 2] {
    axes.map(|axis| shape[axis])
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
