// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's celestial frame rotations: galactic, supergalactic, the
//! ecliptics, and FK4 B1950 with its E-terms.
//!
//! Each conversion follows the casacore routine named on it (`MeasData`,
//! `MeasTable` and `MeasMath`), constants and iteration included, because CASA
//! users see casacore's values (#850). The galactic and FK4 matrices are
//! casacore's 10-digit literals, so they are orthogonal only to about 1e-10;
//! results are renormalized, which leaves every angle unchanged.

use std::sync::LazyLock;

type Matrix = [[f64; 3]; 3];
type Vector = [f64; 3];

/// One arcsecond in radians (`C::arcsec`).
const ARCSEC_RAD: f64 = std::f64::consts::PI / (180.0 * 3600.0);

/// MJD of J2000.0 (`MeasData::MJD2000`).
const MJD2000: f64 = 51_544.5;

/// Days in a Julian century (`MeasData::JDCEN`).
const JULIAN_CENTURY_DAYS: f64 = 36_525.0;

/// `MeasData::GALtoJ2000`: v_J2000 = M · v_galactic. casacore's FK4-derived
/// matrix, not the Hipparcos galactic-to-ICRS one; they differ by up to 0.5″.
const GALACTIC_TO_J2000: Matrix = [
    [-0.054_877_762_1, 0.494_108_321_4, -0.867_666_639_8],
    [-0.873_436_959_1, -0.444_830_861_0, -0.198_074_187_1],
    [-0.483_835_002_6, 0.746_982_243_3, 0.455_983_791_9],
];

/// `MeasData::GALtoB1950`: v_B1950 = M · v_galactic.
const GALACTIC_TO_B1950: Matrix = [
    [-0.066_988_739_4, 0.492_728_466_1, -0.867_600_811_2],
    [-0.872_755_765_9, -0.450_346_958_0, -0.188_374_601_7],
    [-0.483_538_914_6, 0.744_584_633_3, 0.460_199_784_8],
];

/// `MeasData::MToB1950(4)` as listed: the FK5-to-FK4 frame rotation,
/// v_B1950 ≈ M · v_J2000 before the E-terms.
const J2000_TO_B1950: Matrix = [
    [0.999_925_678_1, 0.011_182_061_0, 0.004_857_947_9],
    [-0.011_182_061_0, 0.999_937_478_5, -0.000_027_176_5],
    [-0.004_857_947_7, -0.000_027_176_5, 0.999_988_199_8],
];

/// `MeasData::MToJ2000(0)` as listed: the FK4-to-FK5 frame rotation.
const B1950_TO_J2000: Matrix = [
    [0.999_925_678_2, -0.011_182_061_1, -0.004_857_947_7],
    [0.011_182_061_0, 0.999_937_478_4, -0.000_027_176_5],
    [0.004_857_947_9, -0.000_027_147_4, 0.999_988_199_7],
];

/// `MeasData::MToJ2000(2)` as listed: the FK4-to-FK5 rotation's rate, in
/// arcseconds per Julian century from J2000.
const B1950_TO_J2000_RATE: Matrix = [
    [-0.000_551, -0.238_565, 0.435_739],
    [0.238_514, -0.002_667, -0.008_541],
    [-0.435_623, 0.012_254, 0.002_117],
];

/// `MeasTable::AberETerm(0)`: the E-terms of aberration vector.
const E_TERMS: Vector = [-1.625_57e-6, -0.319_19e-6, -0.138_43e-6];

/// `MeasTable::AberETerm(1)`: its rate, scaled by (epoch + 0.5) arcseconds.
const E_TERMS_RATE: Vector = [1.245e-3, -1.580e-3, -0.659e-3];

/// Mean obliquity of the ecliptic, `MeasTable::fundArg(0)`, in arcseconds
/// per power of Julian centuries from J2000. Its T² term is casacore's
/// −0.0059″, where IAU 1976 (and SOFA's `obl80`) has −0.00059″.
const MEAN_OBLIQUITY_ARCSEC: [f64; 4] = [84_381.448, -46.8150, -0.0059, 0.001813];

/// `MeasTable::galToSupergal`: v_supergalactic = M · v_galactic.
static GALACTIC_TO_SUPERGALACTIC: LazyLock<Matrix> = LazyLock::new(|| {
    let degree = std::f64::consts::PI / 180.0;
    euler_rotation(&[
        (-90.0 * degree, 3),
        (-83.68 * degree, 2),
        (-47.37 * degree, 3),
    ])
});

/// casacore `RotMatrix(Euler)`: the product, in order, of the rotations by
/// each angle about each axis (1 = x, 2 = y, 3 = z), as `RotMatrix::
/// applySingle` builds them.
fn euler_rotation(rotations: &[(f64, usize)]) -> Matrix {
    let mut result = identity();
    for &(angle, axis) in rotations {
        if angle == 0.0 {
            continue;
        }
        let i = axis % 3;
        let j = (i + 1) % 3;
        let (sin, cos) = angle.sin_cos();
        let mut single = identity();
        single[i][i] = cos;
        single[j][j] = cos;
        single[j][i] = sin;
        single[i][j] = -sin;
        result = multiply(&result, &single);
    }
    result
}

fn identity() -> Matrix {
    [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
}

fn multiply(a: &Matrix, b: &Matrix) -> Matrix {
    std::array::from_fn(|row| {
        std::array::from_fn(|column| (0..3).map(|k| a[row][k] * b[k][column]).sum())
    })
}

/// M · v.
fn rotate(m: &Matrix, v: &Vector) -> Vector {
    std::array::from_fn(|row| (0..3).map(|k| m[row][k] * v[k]).sum())
}

/// Mᵀ · v, casacore's `in *= M`.
fn rotate_transposed(m: &Matrix, v: &Vector) -> Vector {
    std::array::from_fn(|column| (0..3).map(|k| m[k][column] * v[k]).sum())
}

fn dot(a: &Vector, b: &Vector) -> f64 {
    (0..3).map(|k| a[k] * b[k]).sum()
}

fn norm(v: &Vector) -> f64 {
    dot(v, v).sqrt()
}

fn normalized(v: Vector) -> Vector {
    let length = norm(&v);
    v.map(|component| component / length)
}

/// Galactic to J2000 (`MeasMath::applyGALtoJ2000`).
pub(super) fn galactic_to_j2000(galactic: Vector) -> Vector {
    normalized(rotate(&GALACTIC_TO_J2000, &galactic))
}

/// J2000 to galactic (`MeasMath::deapplyGALtoJ2000`).
pub(super) fn j2000_to_galactic(j2000: Vector) -> Vector {
    normalized(rotate_transposed(&GALACTIC_TO_J2000, &j2000))
}

/// Galactic to FK4 B1950 (`MeasMath::applyGALtoB1950`).
pub(super) fn galactic_to_b1950(galactic: Vector) -> Vector {
    normalized(rotate(&GALACTIC_TO_B1950, &galactic))
}

/// FK4 B1950 to galactic (`MeasMath::deapplyGALtoB1950`).
pub(super) fn b1950_to_galactic(b1950: Vector) -> Vector {
    normalized(rotate_transposed(&GALACTIC_TO_B1950, &b1950))
}

/// Galactic to supergalactic (`MeasMath::applyGALtoSUPERGAL`).
pub(super) fn galactic_to_supergalactic(galactic: Vector) -> Vector {
    rotate(&GALACTIC_TO_SUPERGALACTIC, &galactic)
}

/// Supergalactic to galactic (`MeasMath::deapplyGALtoSUPERGAL`).
pub(super) fn supergalactic_to_galactic(supergalactic: Vector) -> Vector {
    rotate_transposed(&GALACTIC_TO_SUPERGALACTIC, &supergalactic)
}

/// casacore's mean obliquity of the ecliptic at `tdb_mjd`, in radians
/// (`MeasTable::fundArg(0)`). At J2000 it is the ECLIPTIC frame's 84381.448″.
pub(super) fn mean_obliquity_rad(tdb_mjd: f64) -> f64 {
    let t = (tdb_mjd - MJD2000) / JULIAN_CENTURY_DAYS;
    let [c0, c1, c2, c3] = MEAN_OBLIQUITY_ARCSEC;
    (c0 + t * (c1 + t * (c2 + t * c3))) * ARCSEC_RAD
}

/// The ECLIPTIC frame's obliquity: casacore's mean obliquity at J2000.0, with
/// no frame bias (`MeasMath::applyECLIPtoJ2000`).
pub(super) fn j2000_obliquity_rad() -> f64 {
    MEAN_OBLIQUITY_ARCSEC[0] * ARCSEC_RAD
}

/// The B1950 conversions' epoch, in Julian centuries from J2000: the frame's
/// UT1 epoch when it has one, else casacore's default `measures.b1950.d_epoch`
/// of 2000.0, which is 0.
pub(super) fn b1950_epoch_centuries(ut1_mjd: Option<f64>) -> f64 {
    ut1_mjd.map_or(0.0, |mjd| (mjd - MJD2000) / JULIAN_CENTURY_DAYS)
}

/// The E-terms vector for an epoch (`MeasMath::applyETerms`).
fn e_terms(epoch_centuries: f64) -> Vector {
    let scale = (epoch_centuries + 0.5) * ARCSEC_RAD;
    std::array::from_fn(|k| E_TERMS[k] + scale * E_TERMS_RATE[k])
}

/// FK4 B1950 to J2000 (`MeasMath::deapplyJ2000toB1950`): add the E-terms,
/// rotate, and add the epoch-scaled rotation rate. `normalize` mirrors
/// casacore's `adjust()`, which normalizes an `MVDirection` and leaves the
/// `MVPosition` of the inverse's iteration as it is.
fn b1950_to_j2000_with(b1950: Vector, epoch_centuries: f64, normalize: bool) -> Vector {
    let e_terms = e_terms(epoch_centuries);
    let projection = dot(&b1950, &e_terms);
    let mut with_e_terms: Vector =
        std::array::from_fn(|k| b1950[k] + projection * b1950[k] - e_terms[k]);
    if normalize {
        with_e_terms = normalized(with_e_terms);
    }
    let rate = rotate(&B1950_TO_J2000_RATE, &with_e_terms);
    let rotated = rotate(&B1950_TO_J2000, &with_e_terms);
    let shift = epoch_centuries * ARCSEC_RAD;
    let j2000 = std::array::from_fn(|k| rotated[k] + shift * rate[k]);
    if normalize { normalized(j2000) } else { j2000 }
}

/// FK4 B1950 to J2000 (`MeasMath::deapplyJ2000toB1950`).
pub(super) fn b1950_to_j2000(b1950: Vector, epoch_centuries: f64) -> Vector {
    b1950_to_j2000_with(b1950, epoch_centuries, true)
}

/// J2000 to FK4 B1950 (`MeasMath::applyJ2000toB1950`): rotate, remove the
/// E-terms with casacore's one-step estimate, then correct by fixed-point
/// iteration on [`b1950_to_j2000`] until the J2000 residual is below 1e-12.
pub(super) fn j2000_to_b1950(j2000: Vector, epoch_centuries: f64) -> Vector {
    let rotated = normalized(rotate(&J2000_TO_B1950, &j2000));
    // `MeasMath::deapplyETerms`: its 1e-5 tolerance exceeds the E-terms'
    // 1.7e-6 size, so its loop always runs once.
    let e_terms = e_terms(epoch_centuries);
    let projection = dot(&rotated, &e_terms);
    let mut b1950 = normalized(std::array::from_fn(|k| {
        rotated[k] + e_terms[k] - projection * rotated[k]
    }));
    loop {
        let predicted = b1950_to_j2000_with(b1950, epoch_centuries, false);
        let residual: Vector = std::array::from_fn(|k| predicted[k] - j2000[k]);
        let correction = rotate(&J2000_TO_B1950, &residual);
        b1950 = std::array::from_fn(|k| b1950[k] - correction[k]);
        if norm(&residual) <= 1e-12 {
            break;
        }
    }
    normalized(b1950)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn separation(a: Vector, b: Vector) -> f64 {
        let cross = [
            a[1] * b[2] - a[2] * b[1],
            a[2] * b[0] - a[0] * b[2],
            a[0] * b[1] - a[1] * b[0],
        ];
        norm(&cross).atan2(dot(&a, &b))
    }

    fn unit(lon: f64, lat: f64) -> Vector {
        [lat.cos() * lon.cos(), lat.cos() * lon.sin(), lat.sin()]
    }

    #[test]
    fn euler_rotation_matches_casacore_apply_single() {
        // RotMatrix(Euler(a, 3)) is [[cos, -sin, 0], [sin, cos, 0], [0, 0, 1]].
        let m = euler_rotation(&[(0.3, 3)]);
        let (sin, cos) = 0.3_f64.sin_cos();
        assert_eq!(m, [[cos, -sin, 0.0], [sin, cos, 0.0], [0.0, 0.0, 1.0]]);
        let x = euler_rotation(&[(0.3, 1)]);
        assert_eq!(x, [[1.0, 0.0, 0.0], [0.0, cos, -sin], [0.0, sin, cos]]);
    }

    #[test]
    fn supergalactic_pole_is_casacore_s() {
        // The supergalactic north pole sits at galactic (47.37°, 6.32°).
        let pole = unit(47.37_f64.to_radians(), 6.32_f64.to_radians());
        let supergalactic = galactic_to_supergalactic(pole);
        assert!((supergalactic[2] - 1.0).abs() < 1e-15, "{supergalactic:?}");
    }

    #[test]
    fn galactic_and_b1950_rotations_invert() {
        // casacore's 10-digit matrices are orthogonal to about 7e-11; its
        // supergalactic matrix is built from Euler angles, so exactly.
        for (lon, lat) in [(0.0, 0.0), (1.0, 0.5), (4.0, -1.2), (2.2, 1.5)] {
            let v = unit(lon, lat);
            assert!(separation(j2000_to_galactic(galactic_to_j2000(v)), v) < 2e-10);
            assert!(separation(b1950_to_galactic(galactic_to_b1950(v)), v) < 2e-10);
            assert!(separation(supergalactic_to_galactic(galactic_to_supergalactic(v)), v) < 1e-15);
        }
    }

    #[test]
    fn fk4_conversion_inverts_to_its_iteration_tolerance() {
        for epoch in [0.0, 0.26, -0.5] {
            for (lon, lat) in [(0.0, 0.0), (1.0, 0.5), (4.0, -1.2), (2.2, 1.5)] {
                let j2000 = unit(lon, lat);
                let back = b1950_to_j2000(j2000_to_b1950(j2000, epoch), epoch);
                assert!(
                    separation(back, j2000) < 2e-12,
                    "epoch {epoch} ({lon}, {lat})"
                );
            }
        }
    }

    #[test]
    fn mean_obliquity_keeps_casacore_s_quadratic_term() {
        assert_eq!(j2000_obliquity_rad(), mean_obliquity_rad(MJD2000));
        let century = MJD2000 + JULIAN_CENTURY_DAYS;
        let expected = (84_381.448 - 46.8150 - 0.0059 + 0.001813) * ARCSEC_RAD;
        assert!((mean_obliquity_rad(century) - expected).abs() < 1e-15);
    }
}
