// SPDX-License-Identifier: LGPL-3.0-or-later
//! FK4 precession/nutation needed by CASA simulator BMEAN/BTRUE images.
//! Coefficients follow casacore MeasTable and MeasTableMulSC1950.
use super::{ARCSEC_RAD, Matrix, Vector, euler_rotation, rotate, rotate_transposed};

/// MeasMath apply/deapplyPrecessionB1950, at the supplied TDB MJD.
pub(in crate::measures) fn precession(value: Vector, mjd: f64, inverse: bool) -> Vector {
    let fixed = (33_281.923_459_05 + 3242.29642) / 36_524.219_878_2;
    let t = (mjd + 3242.29642) / 36_524.219_878_2 - fixed;
    let angles: [f64; 3] = std::array::from_fn(|index| {
        let [a, b, c, d, e, f] = PRECESSION[index];
        let sign = if index == 1 { 1. } else { -1. };
        sign * t * ((a + fixed * (b + fixed * c)) + t * ((d + fixed * e) + t * f)) * ARCSEC_RAD
    });
    let rotation = euler_rotation(&[(angles[0], 3), (angles[1], 2), (angles[2], 3)]);
    apply(&rotation, value, inverse)
}

/// MeasMath apply/deapplyNutationB1950 (the 69-term FK4 series).
pub(in crate::measures) fn nutation(value: Vector, mjd: f64, inverse: bool) -> Vector {
    let t = (mjd - 15_019.5) / 36_525.;
    let fundamental = FUNDAMENTAL.map(|[a, b, c, d]| (a + t * (b + t * (c + t * d))) * ARCSEC_RAD);
    let (mut longitude, mut obliquity) = (0., 0.);
    for (index, args) in ARGUMENTS.iter().enumerate() {
        let phase: f64 = args
            .iter()
            .zip(&fundamental[1..])
            .map(|(&arg, &angle)| f64::from(arg) * angle)
            .sum();
        let (mut a, mut b) = (
            f64::from(COEFFICIENTS[index][0]) * 1e-4,
            f64::from(COEFFICIENTS[index][1]) * 1e-4,
        );
        if let Some(&[_, a0, a1, b0, b1]) = TIME_COEFFICIENTS
            .iter()
            .find(|row| row[0] as usize == index)
        {
            a = f64::from(a0) * 1e-4 + t * f64::from(a1) * 1e-5;
            b = f64::from(b0) * 1e-4 + t * f64::from(b1) * 1e-5;
        }
        longitude += a * ARCSEC_RAD * phase.sin();
        obliquity += b * ARCSEC_RAD * phase.cos();
    }
    let epsilon = fundamental[0];
    let rotation = euler_rotation(&[(epsilon, 1), (-longitude, 3), (-epsilon - obliquity, 1)]);
    apply(&rotation, value, inverse)
}

fn apply(rotation: &Matrix, value: Vector, inverse: bool) -> Vector {
    if inverse {
        rotate(rotation, &value)
    } else {
        rotate_transposed(rotation, &value)
    }
}

const PRECESSION: [[f64; 6]; 3] = [
    [2303.5545, 1.39720, 0.000060, 0.30240, -0.000270, 0.017995],
    [
        2005.1120, -0.85290, -0.00037, -0.42650, -0.000370, -0.041800,
    ],
    [2303.5545, 1.39720, 0.000060, 1.09480, 0.000390, 0.018325],
];

const FUNDAMENTAL: [[f64; 4]; 6] = [
    [84428.26, -46.846, -0.0059, 0.00181],
    [1065976.59, 1717915856.79, 33.09, 0.0518],
    [1290513.0, 129596579.1, -0.54, -0.012],
    [40503.2, 1739527290.54, -11.56, -0.0012],
    [1262654.95, 1602961611.18, -5.17, 0.0068],
    [933059.79, -6962911.23, 7.48, 0.008],
];

const ARGUMENTS: [[i8; 5]; 69] = [
    [0, 0, 0, 0, 1],
    [0, 0, 0, 0, 2],
    [-2, 0, 2, 0, 1],
    [2, 0, -2, 0, 0],
    [0, -2, 2, -2, 1],
    [-2, 0, 2, 0, 2],
    [1, -1, 0, -1, 0],
    [0, 0, 2, -2, 2],
    [0, 1, 0, 0, 0],
    [0, 1, 2, -2, 2],
    [0, -1, 2, -2, 2],
    [0, 0, 2, -2, 1],
    [2, 0, 0, -2, 0],
    [0, 0, 2, -2, 0],
    [0, 2, 0, 0, 0],
    [0, 1, 0, 0, 1],
    [0, 2, 2, -2, 2],
    [0, -1, 0, 0, 1],
    [-2, 0, 0, 2, 1],
    [0, -1, 2, -2, 1],
    [2, 0, 0, -2, 1],
    [0, 1, 2, -2, 1],
    [1, 0, 0, -1, 0],
    [0, 0, 2, 0, 2],
    [1, 0, 0, 0, 0],
    [0, 0, 2, 0, 1],
    [1, 0, 2, 0, 2],
    [1, 0, 0, -2, 0],
    [-1, 0, 2, 0, 2],
    [0, 0, 0, 2, 0],
    [1, 0, 0, 0, 1],
    [-1, 0, 0, 0, 1],
    [-1, 0, 2, 2, 2],
    [1, 0, 2, 0, 1],
    [0, 0, 2, 2, 2],
    [2, 0, 0, 0, 0],
    [1, 0, 2, -2, 2],
    [2, 0, 2, 0, 2],
    [0, 0, 2, 0, 0],
    [-1, 0, 2, 0, 1],
    [-1, 0, 0, 2, 1],
    [1, 0, 0, -2, 1],
    [-1, 0, 2, 2, 1],
    [1, 1, 0, -2, 0],
    [0, 1, 2, 0, 2],
    [1, 0, 0, 2, 0],
    [0, 0, 0, 2, 1],
    [0, -1, 2, 0, 2],
    [1, 0, 2, 2, 2],
    [2, 0, 2, -2, 2],
    [0, 0, 0, -2, 1],
    [0, 0, 2, 2, 1],
    [1, 0, 2, -2, 1],
    [0, 0, 0, 1, 0],
    [0, 1, 0, -2, 0],
    [1, -1, 0, 0, 0],
    [1, 0, -2, 0, 0],
    [2, 0, 2, 0, 1],
    [1, 0, 2, 0, 0],
    [1, 1, 0, 0, 0],
    [1, -1, 2, 0, 2],
    [-2, 0, 0, 0, 1],
    [-1, 0, 2, -2, 1],
    [2, 0, 0, 0, 1],
    [-1, -1, 2, 2, 2],
    [0, -1, 2, 2, 2],
    [1, 0, 0, 0, 2],
    [1, 1, 2, 0, 2],
    [3, 0, 2, 0, 2],
];

const TIME_COEFFICIENTS: [[i32; 5]; 13] = [
    [0, -172327, -1737, 92100, 91],
    [1, 2088, 2, -904, 4],
    [7, -12729, -13, 5522, -29],
    [8, 1261, -31, 0, 0],
    [9, -497, 12, 216, -6],
    [10, 214, -5, -93, 3],
    [11, 124, 1, -66, 0],
    [14, 16, -1, 0, 0],
    [16, -15, 1, 7, 0],
    [23, -2037, -2, 884, -5],
    [24, 675, 1, 0, 0],
    [25, -342, -4, 183, 0],
    [26, -261, 0, 113, -1],
];

const COEFFICIENTS: [[i16; 2]; 69] = [
    [0, 0],
    [0, 0],
    [45, -24],
    [10, 0],
    [-4, 2],
    [-3, 2],
    [-2, 0],
    [0, 0],
    [0, 0],
    [0, 0],
    [0, 0],
    [0, 0],
    [45, 0],
    [-21, 0],
    [0, 0],
    [-15, 8],
    [0, 0],
    [-10, 5],
    [-5, 3],
    [-5, 3],
    [4, -2],
    [3, -2],
    [-3, 0],
    [0, 0],
    [0, 0],
    [0, 0],
    [0, 0],
    [-149, 0],
    [114, -50],
    [60, 0],
    [58, -31],
    [-57, 30],
    [-52, 22],
    [-44, 23],
    [-32, 14],
    [28, 0],
    [26, -11],
    [-26, 11],
    [25, 0],
    [19, -10],
    [14, -7],
    [-13, 7],
    [-9, 5],
    [-7, 0],
    [7, -3],
    [6, 0],
    [-6, 3],
    [-6, 3],
    [-6, 3],
    [6, -2],
    [-5, 3],
    [-5, 3],
    [5, -3],
    [-4, 0],
    [-4, 0],
    [4, 0],
    [4, 0],
    [-4, 2],
    [3, 0],
    [-3, 0],
    [-3, 0],
    [-2, 0],
    [-2, 0],
    [2, 0],
    [-2, 0],
    [-2, 0],
    [-2, 0],
    [2, 0],
    [-2, 0],
];
