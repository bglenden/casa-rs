// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's `UVWMachine` between two J2000 directions: the rotation of
//! UVW coordinates from one phase centre to another, and the phase change
//! that goes with it.
//!
//! The arithmetic follows casacore's own statement by statement, Euler
//! rotations and matrix products included, because imaging and simulation
//! reproduce CASA to rounding: its `RotMatrix` products accumulate each term
//! as a separate statement, and its machine rotates even between two equal
//! directions, which leaves a rounding residue CASA's gridders depend on.

use super::direction::MDirection;

/// casacore `UVWMachine` for an explicit output direction, both directions
/// in J2000.
///
/// [`Self::convert`] is `UVWMachine::convertUVW(Double&, Vector<Double>&)`
/// with `project=false`: the rotated UVW and the phase change in metres
/// (`dphase`). [`Self::convert_projected`] is the `project=true` machine,
/// which also reprojects the rotated UV plane onto the input direction's
/// tangent plane, as CASA's faceted imaging builds it.
///
/// The UVW and the phase change are in casacore's sign convention, whose u
/// and v are the negatives of a MeasurementSet's.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct UvwMachine {
    rotation: [[f64; 3]; 3],
    input_plane_projection: [[f64; 3]; 3],
    phase_rotation: [f64; 3],
}

impl UvwMachine {
    /// The machine from UVW phased on `input` to UVW phased on `output`,
    /// both J2000 (the reference-frame conversion of casacore's machine is
    /// then the identity, and is not formed).
    #[must_use]
    pub fn new(output: &MDirection, input: &MDirection) -> Self {
        let [input_ra, input_dec] = angles(input);
        let [output_ra, output_dec] = angles(output);
        let rot1 = euler(&[
            (-(std::f64::consts::FRAC_PI_2 - input_ra), Axis::Z),
            (input_dec - std::f64::consts::FRAC_PI_2, Axis::X),
        ]);
        let rot3 = euler(&[
            (std::f64::consts::FRAC_PI_2 - output_dec, Axis::X),
            (-(output_ra - std::f64::consts::FRAC_PI_2), Axis::Z),
        ]);
        let rotation = transpose(product(product(rot3, IDENTITY), rot1));
        // The `project=true` reprojection of the rotated UV plane onto the
        // input direction's plane, with casacore's literal Euler rotation
        // and sparse assignment.
        let projection = euler(&[
            (-(std::f64::consts::FRAC_PI_2 - output_dec), Axis::X),
            (output_ra - input_ra, Axis::Z),
            (std::f64::consts::FRAC_PI_2 - input_dec, Axis::X),
        ]);
        let denominator = projection[2][2];
        let mut input_plane_projection = IDENTITY;
        input_plane_projection[0][0] = projection[1][1] / denominator;
        input_plane_projection[1][1] = projection[0][0] / denominator;
        input_plane_projection[0][1] = projection[1][0] / denominator;
        input_plane_projection[1][0] = projection[0][1] / denominator;
        let [output_cosines, input_cosines] = [output.cosines(), input.cosines()];
        let phase_rotation = matrix_vector(
            rot3,
            [
                output_cosines[0] - input_cosines[0],
                output_cosines[1] - input_cosines[1],
                output_cosines[2] - input_cosines[2],
            ],
        );
        Self {
            rotation,
            input_plane_projection,
            phase_rotation,
        }
    }

    /// `uvw_m` rotated to the output direction, and the phase change in
    /// metres.
    #[must_use]
    pub fn convert(&self, uvw_m: [f64; 3]) -> ([f64; 3], f64) {
        let rotated = vector_matrix(uvw_m, self.rotation);
        (rotated, dot(self.phase_rotation, rotated))
    }

    /// As [`Self::convert`], the rotated UV plane then reprojected onto the
    /// input direction's tangent plane (`project=true`); the phase change
    /// is that of the rotation alone.
    #[must_use]
    pub fn convert_projected(&self, uvw_m: [f64; 3]) -> ([f64; 3], f64) {
        let rotated = vector_matrix(uvw_m, self.rotation);
        let phase_m = dot(self.phase_rotation, rotated);
        (vector_matrix(rotated, self.input_plane_projection), phase_m)
    }

    /// The UVW rotation, applied as `uvw · rotation` (casacore `uvrot_p`).
    #[must_use]
    pub const fn rotation(&self) -> [[f64; 3]; 3] {
        self.rotation
    }

    /// The phase-change vector dotted with rotated UVW (casacore
    /// `phrot_p`).
    #[must_use]
    pub const fn phase_rotation(&self) -> [f64; 3] {
        self.phase_rotation
    }
}

const IDENTITY: [[f64; 3]; 3] = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]];

/// A direction's longitude and latitude as casacore's `MVDirection` forms
/// them from its cosines.
fn angles(direction: &MDirection) -> [f64; 2] {
    let cosines = direction.cosines();
    [cosines[1].atan2(cosines[0]), cosines[2].asin()]
}

#[derive(Clone, Copy)]
enum Axis {
    X,
    Z,
}

impl Axis {
    /// casacore's axis number, which `Euler` multiplies by the angle to
    /// skip a zero rotation.
    const fn number(self) -> f64 {
        match self {
            Self::X => 1.0,
            Self::Z => 3.0,
        }
    }

    const fn indices(self) -> (usize, usize) {
        match self {
            Self::X => (1, 2),
            Self::Z => (0, 1),
        }
    }
}

/// casacore `RotMatrix(Euler(...))`: the product of the axis rotations, in
/// order, a zero rotation skipped.
fn euler(operations: &[(f64, Axis)]) -> [[f64; 3]; 3] {
    let mut matrix = IDENTITY;
    for &(angle, axis) in operations {
        if angle * axis.number() == 0.0 {
            continue;
        }
        let mut rotation = IDENTITY;
        let (i, j) = axis.indices();
        let (sine, cosine) = (angle.sin(), angle.cos());
        rotation[i][i] = cosine;
        rotation[j][j] = cosine;
        rotation[j][i] = sine;
        rotation[i][j] = -sine;
        matrix = product(matrix, rotation);
    }
    matrix
}

/// casacore `RotMatrix::operator*=`, each multiply and addition a separate
/// statement: these construction-time cancellations differ observably from
/// fused multiply-adds.
fn product(left: [[f64; 3]; 3], right: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut output = IDENTITY;
    for row in 0..3 {
        let source = left[row];
        for column in 0..3 {
            let mut value = source[0] * right[0][column];
            value += source[1] * right[1][column];
            value += source[2] * right[2][column];
            output[row][column] = value;
        }
    }
    output
}

fn transpose(matrix: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    [
        [matrix[0][0], matrix[1][0], matrix[2][0]],
        [matrix[0][1], matrix[1][1], matrix[2][1]],
        [matrix[0][2], matrix[1][2], matrix[2][2]],
    ]
}

fn vector_matrix(vector: [f64; 3], matrix: [[f64; 3]; 3]) -> [f64; 3] {
    let mut output = [0.0; 3];
    for column in 0..3 {
        output[column] = vector[0] * matrix[0][column];
        output[column] += vector[1] * matrix[1][column];
        output[column] += vector[2] * matrix[2][column];
    }
    output
}

/// casacore's `RotMatrix * MVPosition`, accumulated as three statements.
fn matrix_vector(matrix: [[f64; 3]; 3], vector: [f64; 3]) -> [f64; 3] {
    let mut output = [0.0; 3];
    for row in 0..3 {
        output[row] += matrix[row][0] * vector[0];
        output[row] += matrix[row][1] * vector[1];
        output[row] += matrix[row][2] * vector[2];
    }
    output
}

fn dot(left: [f64; 3], right: [f64; 3]) -> f64 {
    let mut result = 0.0;
    for axis in 0..3 {
        result += left[axis] * right[axis];
    }
    result
}

#[cfg(test)]
mod tests {
    use super::UvwMachine;
    use crate::measures::direction::{DirectionRef, MDirection};

    /// The machine between a direction and itself keeps UVW to rounding
    /// with no phase change; between two directions it keeps the baseline
    /// length.
    #[test]
    fn rotation_keeps_length_and_equal_directions_keep_uvw() {
        let centre = MDirection::from_angles(
            2.617_993_877_991_494,
            -0.523_598_775_598_298_8,
            DirectionRef::J2000,
        );
        let other = MDirection::from_angles(2.618_001_1, -0.523_603_5, DirectionRef::J2000);
        let uvw = [120.5, -48.25, 3.5];
        let (same, phase) = UvwMachine::new(&centre, &centre).convert(uvw);
        for axis in 0..3 {
            assert!((same[axis] - uvw[axis]).abs() < 1e-10);
        }
        assert!(phase.abs() < 1e-10);
        let (rotated, _) = UvwMachine::new(&other, &centre).convert(uvw);
        let length = |v: [f64; 3]| v.iter().map(|x| x * x).sum::<f64>().sqrt();
        assert!((length(rotated) - length(uvw)).abs() < 1e-9);
    }
}
