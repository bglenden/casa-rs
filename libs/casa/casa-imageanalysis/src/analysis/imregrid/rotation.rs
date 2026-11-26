// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use ndarray::arr2;

use casa_images::ImageError;

pub(super) fn rotated_grid(
    source: &CoordinateSystem,
    angle: f64,
    source_shape: &[usize],
    output_shape: &[usize],
) -> Result<(CoordinateSystem, [i64; 2]), ImageError> {
    let invalid = |message: &str| ImageError::CasaSelection(message.into());
    if !angle.is_finite() {
        return Err(invalid("rotation angle must be finite"));
    }
    let index = source
        .find_coordinate(CoordinateType::Direction)
        .or_else(|| source.find_coordinate(CoordinateType::Linear))
        .ok_or_else(|| invalid("Can only rotate a direction coordinate or a linear coordinate"))?;
    let axes = source
        .coordinate_pixel_axes(index)
        .expect("existing coordinate");
    let [Some(x), Some(y)] = axes.as_slice() else {
        return Err(invalid(
            "Can only rotate a coordinate with exactly two pixel axes",
        ));
    };
    let input_plane = [source_shape[*x], source_shape[*y]];
    let output_plane = [output_shape[*x], output_shape[*y]];
    let axes = [*x as i64, *y as i64];
    let linear = source.coordinate(index).coordinate_type() == CoordinateType::Linear;
    if linear && input_plane == [1, 1] && input_plane == output_plane {
        return Ok((source.clone(), axes));
    }
    if linear
        && input_plane
            .iter()
            .zip(output_plane)
            .any(|(&old, new)| new == 1 && old != new)
    {
        return Err(invalid(
            "an axis not being regridded must keep its input length",
        ));
    }
    let (sine, cosine) = angle.sin_cos();
    if input_plane.contains(&1) {
        if input_plane != output_plane || sine.abs() > 1e-12 || (cosine - 1.).abs() > 1e-12 {
            return Err(invalid("cannot rotate a plane with a singleton axis"));
        }
        return Ok((source.clone(), axes));
    }
    let rotation = arr2(&[[cosine, sine], [-sine, cosine]]);
    // The coordinate record stores the transpose of the public PC matrix.
    let rotated = match source.coordinate(index) {
        CoordinateModel::Direction(coordinate) => CoordinateModel::Direction(
            coordinate
                .clone()
                .with_normalized_pc_matrix(coordinate.pc_matrix().dot(&rotation))?,
        ),
        CoordinateModel::Linear(coordinate) if coordinate.pc_matrix().shape() == [2, 2] => {
            CoordinateModel::Linear(
                coordinate
                    .clone()
                    .with_pc_matrix(coordinate.pc_matrix().dot(&rotation)),
            )
        }
        _ => {
            return Err(invalid(
                "Can only rotate a linear coordinate with exactly two axes",
            ));
        }
    };
    let mut target = source.clone();
    target.replace_coordinate(index, rotated);
    Ok((target, axes))
}
