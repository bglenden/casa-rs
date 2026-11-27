// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// The unprojected casacore UVWMachine rotation between two directions in
/// the same reference frame.
pub struct UvwRotation {
    pub(super) uvw: [[f64; 3]; 3],
    pub(super) phase: [f64; 3],
}

impl UvwRotation {
    /// Build a UVW coordinate rotation from `source` to `target` in the same frame.
    pub fn new(source: &MDirection, target: &MDirection) -> Self {
        Self {
            uvw: uvw_rotation_matrix(source, target),
            phase: uvw_phase_rotation_vector(source, target),
        }
    }

    /// Return the rotated UVW and geometric path difference in metres.
    pub fn apply(&self, uvw: [f64; 3]) -> ([f64; 3], f64) {
        let rotated = row_vec3_mul_mat3(uvw, self.uvw);
        (rotated, dot3(self.phase, rotated))
    }
}

pub(super) fn uvw_rotation_matrix(
    source_direction: &MDirection,
    target_direction: &MDirection,
) -> [[f64; 3]; 3] {
    let (source_ra, source_dec) = source_direction.as_angles();
    let (target_ra, target_dec) = target_direction.as_angles();
    let rot1 = euler_rotation(&[
        (-(std::f64::consts::FRAC_PI_2 - source_ra), Axis::Z),
        (source_dec - std::f64::consts::FRAC_PI_2, Axis::X),
    ]);
    let rot2 = identity3();
    let rot3 = euler_rotation(&[
        (std::f64::consts::FRAC_PI_2 - target_dec, Axis::X),
        (-(target_ra - std::f64::consts::FRAC_PI_2), Axis::Z),
    ]);
    let uvrot = mat3_mul_mat3(mat3_mul_mat3(rot3, rot2), rot1);
    mat3_transpose(uvrot)
}

pub(super) fn mat3_mul_vec3(matrix: [[f64; 3]; 3], vector: [f64; 3]) -> [f64; 3] {
    [
        matrix[0][0] * vector[0] + matrix[0][1] * vector[1] + matrix[0][2] * vector[2],
        matrix[1][0] * vector[0] + matrix[1][1] * vector[1] + matrix[1][2] * vector[2],
        matrix[2][0] * vector[0] + matrix[2][1] * vector[1] + matrix[2][2] * vector[2],
    ]
}

pub(super) fn row_vec3_mul_mat3(vector: [f64; 3], matrix: [[f64; 3]; 3]) -> [f64; 3] {
    [
        vector[0] * matrix[0][0] + vector[1] * matrix[1][0] + vector[2] * matrix[2][0],
        vector[0] * matrix[0][1] + vector[1] * matrix[1][1] + vector[2] * matrix[2][1],
        vector[0] * matrix[0][2] + vector[1] * matrix[1][2] + vector[2] * matrix[2][2],
    ]
}

pub(super) fn uvw_phase_rotation_vector(
    source_direction: &MDirection,
    target_direction: &MDirection,
) -> [f64; 3] {
    let (target_ra, target_dec) = target_direction.as_angles();
    let rot3 = euler_rotation(&[
        (std::f64::consts::FRAC_PI_2 - target_dec, Axis::X),
        (-(target_ra - std::f64::consts::FRAC_PI_2), Axis::Z),
    ]);
    let target_cosines = target_direction.cosines();
    let source_cosines = source_direction.cosines();
    mat3_mul_vec3(
        rot3,
        [
            target_cosines[0] - source_cosines[0],
            target_cosines[1] - source_cosines[1],
            target_cosines[2] - source_cosines[2],
        ],
    )
}

pub(super) fn dot3(left: [f64; 3], right: [f64; 3]) -> f64 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

pub(super) fn identity3() -> [[f64; 3]; 3] {
    [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
}

pub(super) fn mat3_mul_mat3(left: [[f64; 3]; 3], right: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut out = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            out[row][col] = left[row][0] * right[0][col]
                + left[row][1] * right[1][col]
                + left[row][2] * right[2][col];
        }
    }
    out
}

pub(super) fn mat3_transpose(matrix: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    [
        [matrix[0][0], matrix[1][0], matrix[2][0]],
        [matrix[0][1], matrix[1][1], matrix[2][1]],
        [matrix[0][2], matrix[1][2], matrix[2][2]],
    ]
}

#[derive(Clone, Copy)]
pub(super) enum Axis {
    X,
    Z,
}

pub(super) fn euler_rotation(operations: &[(f64, Axis)]) -> [[f64; 3]; 3] {
    let mut matrix = identity3();
    for (angle, axis) in operations {
        matrix = mat3_mul_mat3(matrix, axis_rotation(*angle, *axis));
    }
    matrix
}

pub(super) fn axis_rotation(angle: f64, axis: Axis) -> [[f64; 3]; 3] {
    let (sin_angle, cos_angle) = angle.sin_cos();
    match axis {
        Axis::X => [
            [1.0, 0.0, 0.0],
            [0.0, cos_angle, -sin_angle],
            [0.0, sin_angle, cos_angle],
        ],
        Axis::Z => [
            [cos_angle, -sin_angle, 0.0],
            [sin_angle, cos_angle, 0.0],
            [0.0, 0.0, 1.0],
        ],
    }
}
