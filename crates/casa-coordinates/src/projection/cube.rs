// SPDX-License-Identifier: LGPL-3.0-or-later
//! The six-face WCS layout shared by TSC, CSC and QSC.
//!
//! Calabretta & Greisen (2002), §5.6. The COBE coefficients and the
//! single-precision evaluation follow WCSLIB 8.2.2 by Mark Calabretta.

use std::f64::consts::{FRAC_1_SQRT_2, FRAC_PI_4, PI};

use super::ProjectionType;
use crate::error::CoordinateError;

#[derive(Clone, Copy)]
enum Face {
    North,
    Front,
    East,
    Back,
    West,
    South,
}

impl Face {
    /// Select the largest outward component, with WCSLIB's tie order.
    fn from_sphere(longitude: f64, latitude: f64) -> (Self, [f64; 3]) {
        let (sin_lon, cos_lon) = longitude.sin_cos();
        let (north, cos_lat) = latitude.sin_cos();
        let front = cos_lat * cos_lon;
        let east = cos_lat * sin_lon;
        let faces = [
            (Self::North, [north, east, -front]),
            (Self::Front, [front, east, north]),
            (Self::East, [east, -front, north]),
            (Self::Back, [-front, -east, north]),
            (Self::West, [-east, front, north]),
            (Self::South, [-north, east, front]),
        ];
        faces
            .into_iter()
            .reduce(|selected, next| {
                if next.1[0] > selected.1[0] {
                    next
                } else {
                    selected
                }
            })
            .expect("six faces")
    }

    fn to_sphere(self, normal: f64, horizontal: f64, vertical: f64) -> (f64, f64) {
        let [front, east, north] = match self {
            Self::North => [-vertical, horizontal, normal],
            Self::Front => [normal, horizontal, vertical],
            Self::East => [-horizontal, normal, vertical],
            Self::Back => [-normal, -horizontal, vertical],
            Self::West => [horizontal, -normal, vertical],
            Self::South => [vertical, horizontal, -normal],
        };
        let longitude = if front == 0.0 && east == 0.0 {
            0.0
        } else {
            east.atan2(front)
        };
        (longitude, north.clamp(-1.0, 1.0).asin())
    }

    fn offset(self) -> (f64, f64) {
        match self {
            Self::North => (0.0, 2.0),
            Self::Front => (0.0, 0.0),
            Self::East => (2.0, 0.0),
            Self::Back => (4.0, 0.0),
            Self::West => (6.0, 0.0),
            Self::South => (0.0, -2.0),
        }
    }

    fn from_plane(kind: ProjectionType, x: f64, y: f64) -> Option<(Self, f64, f64)> {
        if x.abs() <= 1.0 {
            if y.abs() > 3.0 {
                return None;
            }
        } else if x.abs() > 7.0 || y.abs() > 1.0 {
            return None;
        }
        // The other three equatorial faces can also be read left of the
        // central face. Writes use the canonical positive strip.
        let x = if x < -1.0 {
            face_precision(kind, x + 8.0)
        } else {
            x
        };
        Some(if x > 5.0 {
            (Self::West, x - 6.0, y)
        } else if x > 3.0 {
            (Self::Back, x - 4.0, y)
        } else if x > 1.0 {
            (Self::East, x - 2.0, y)
        } else if y > 1.0 {
            (Self::North, x, y - 2.0)
        } else if y < -1.0 {
            (Self::South, x, y + 2.0)
        } else {
            (Self::Front, x, y)
        })
    }
}

pub(super) fn project(
    kind: ProjectionType,
    phi: f64,
    theta: f64,
) -> Result<(f64, f64), CoordinateError> {
    let (face, [normal, horizontal, vertical]) = Face::from_sphere(phi, theta);
    let (x, y) = match kind {
        ProjectionType::TSC => (horizontal / normal, vertical / normal),
        ProjectionType::CSC => {
            let x = (horizontal / normal) as f32;
            let y = (vertical / normal) as f32;
            (f64::from(cobe_forward(x, y)), f64::from(cobe_forward(y, x)))
        }
        ProjectionType::QSC => equal_area_forward(normal, horizontal, vertical),
        _ => unreachable!("cubed projection"),
    };
    let tolerance = if kind == ProjectionType::CSC {
        1e-7
    } else {
        1e-12
    };
    if x.abs() > 1.0 + tolerance || y.abs() > 1.0 + tolerance {
        return Err(boundary(kind));
    }
    let (x_offset, y_offset) = face.offset();
    let x = face_precision(kind, x.clamp(-1.0, 1.0) + x_offset);
    let y = face_precision(kind, y.clamp(-1.0, 1.0) + y_offset);
    Ok((FRAC_PI_4 * x, FRAC_PI_4 * y))
}

pub(super) fn deproject(
    kind: ProjectionType,
    x: f64,
    y: f64,
) -> Result<(f64, f64), CoordinateError> {
    let x = face_precision(kind, x / FRAC_PI_4);
    let y = face_precision(kind, y / FRAC_PI_4);
    if !x.is_finite() || !y.is_finite() {
        return Err(boundary(kind));
    }
    let (face, x, y) = Face::from_plane(kind, x, y).ok_or_else(|| boundary(kind))?;
    let [normal, horizontal, vertical] = match kind {
        ProjectionType::TSC => {
            let normal = (1.0 + x * x + y * y).sqrt().recip();
            [normal, x * normal, y * normal]
        }
        ProjectionType::CSC => {
            let horizontal = cobe_inverse(x as f32, y as f32);
            let vertical = cobe_inverse(y as f32, x as f32);
            let squared = f64::from(horizontal.mul_add(horizontal, vertical * vertical));
            let normal = (1.0 + squared).sqrt().recip();
            [
                normal,
                f64::from(horizontal) * normal,
                f64::from(vertical) * normal,
            ]
        }
        ProjectionType::QSC => equal_area_inverse(x, y),
        _ => unreachable!("cubed projection"),
    };
    Ok(face.to_sphere(normal, horizontal, vertical))
}

fn face_precision(kind: ProjectionType, value: f64) -> f64 {
    if kind == ProjectionType::CSC {
        f64::from(value as f32)
    } else {
        value
    }
}

fn boundary(kind: ProjectionType) -> CoordinateError {
    CoordinateError::ConversionFailed(format!(
        "{} point outside cube projection boundary",
        kind.name()
    ))
}

/// CSC employs separate polynomial approximations for each direction.
#[expect(
    clippy::excessive_precision,
    reason = "WCSLIB's published CSC coefficients"
)]
fn cobe_forward(x: f32, y: f32) -> f32 {
    let xx = x * x;
    let yy = y * y;
    let one_minus_xx = 1.0 - xx;
    let one_minus_yy = 1.0 - yy;
    let x4 = if xx > 1e-16 { xx * xx } else { 0.0 };
    let y4 = if yy > 1e-16 { yy * yy } else { 0.0 };
    let xxyy = if (x * y).abs() > 1e-16 { xx * yy } else { 0.0 };
    let cross = 0.0809701286525_f32.mul_add(xx, 0.141189631152);
    let cross = (-0.281528535557_f32).mul_add(yy, cross);
    let cross = 0.15384112876_f32.mul_add(xxyy, cross);
    let cross = (-0.178251207466_f32).mul_add(x4, cross);
    let cross = 0.106959469314_f32.mul_add(y4, cross);
    let angular = (-0.13161671474_f32).mul_add(one_minus_xx, 0.004869491981 * xx);
    let angular = one_minus_yy.mul_add(cross, angular);
    let correction = (-0.0217762490699_f32).mul_add(xx, 0.0759196200467);
    let correction = (-one_minus_xx).mul_add(correction, -0.159596235474);
    let blend = yy.mul_add(angular, 1.37484847732);
    let blend = xx.mul_add(correction, blend);
    x * one_minus_xx.mul_add(blend, xx)
}

#[expect(
    clippy::excessive_precision,
    reason = "WCSLIB's published CSC coefficients"
)]
fn cobe_inverse(x: f32, y: f32) -> f32 {
    const COEFFICIENTS: [&[f32]; 7] = [
        &[
            -0.27292696,
            -0.07629969,
            -0.22797056,
            0.54852384,
            -0.62930065,
            0.25795794,
            0.02584375,
        ],
        &[
            -0.02819452,
            -0.01471565,
            0.48051509,
            -1.74114454,
            1.71547508,
            -0.53022337,
        ],
        &[0.27058160, -0.56800938, 0.30803317, 0.98938102, -0.83180469],
        &[-0.60441560, 1.50880086, -0.93678576, 0.08693841],
        &[0.93412077, -1.41601920, 0.33887446],
        &[-0.63915306, 0.52032238],
        &[0.14381585],
    ];
    let xx = x * x;
    let yy = y * y;
    let mut correction = 0.0_f32;
    for row in COEFFICIENTS.into_iter().rev() {
        let mut coefficient = 0.0_f32;
        for value in row.iter().rev() {
            coefficient = coefficient.mul_add(xx, *value);
        }
        correction = correction.mul_add(yy, coefficient);
    }
    (x * (1.0 - xx)).mul_add(correction, x)
}

/// QSC uses the same wedge formula on either of the two major axes.
fn equal_area_forward(normal: f64, horizontal: f64, vertical: f64) -> (f64, f64) {
    let horizontal_major = horizontal.abs() > vertical.abs();
    let (major, minor) = if horizontal_major {
        (horizontal, vertical)
    } else {
        (vertical, horizontal)
    };
    if major == 0.0 {
        return (0.0, 0.0);
    }
    let ratio = minor / major;
    let tangent_squared = 1.0 + ratio * ratio;
    // This identity avoids cancellation in 1 - normal at the face centre.
    let distance = (horizontal * horizontal + vertical * vertical) / (1.0 + normal);
    let radial = (distance / (1.0 - (1.0 + tangent_squared).sqrt().recip()))
        .sqrt()
        .copysign(major);
    let azimuth = ratio.atan() - (ratio / (2.0 * tangent_squared).sqrt()).asin();
    let secondary = radial * azimuth / (PI / 12.0);
    if horizontal_major {
        (radial, secondary)
    } else {
        (secondary, radial)
    }
}

fn equal_area_inverse(x: f64, y: f64) -> [f64; 3] {
    let horizontal_major = x.abs() > y.abs();
    let (major, minor) = if horizontal_major { (x, y) } else { (y, x) };
    if major == 0.0 {
        return [1.0, 0.0, 0.0];
    }
    let angle = (PI / 12.0) * minor / major;
    let (sine, cosine) = angle.sin_cos();
    let ratio = sine / (cosine - FRAC_1_SQRT_2);
    let tangent_squared = 1.0 + ratio * ratio;
    let distance = major * major * (1.0 - (1.0 + tangent_squared).sqrt().recip());
    let dominant = (distance * (2.0 - distance) / tangent_squared)
        .sqrt()
        .copysign(major);
    let other = dominant * ratio;
    if horizontal_major {
        [1.0 - distance, dominant, other]
    } else {
        [1.0 - distance, other, dominant]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The unfolded map has no diagonal faces, and the left-hand alias
    /// names the same native direction as its canonical equatorial face.
    #[test]
    fn cube_layout_refuses_gaps_and_keeps_wrapped_faces() {
        for kind in [
            ProjectionType::TSC,
            ProjectionType::CSC,
            ProjectionType::QSC,
        ] {
            assert!(deproject(kind, 2.0 * FRAC_PI_4, 2.0 * FRAC_PI_4).is_err());
            assert!(deproject(kind, 0.0, 3.01 * FRAC_PI_4).is_err());
            let canonical = deproject(kind, 6.0 * FRAC_PI_4, 0.0).unwrap();
            let alias = deproject(kind, -2.0 * FRAC_PI_4, 0.0).unwrap();
            assert_eq!(canonical, alias, "{kind:?}");
        }
    }
}
