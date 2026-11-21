// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{FRAC_PI_2, PI};

use crate::CoordinateError;

use super::{deproject_sfl, project_sfl};

pub(super) fn latitude(parameters: &[f64]) -> Result<f64, CoordinateError> {
    let angle = parameters.first().copied().unwrap_or(f64::NAN);
    if angle.is_finite() {
        Ok(angle.to_radians())
    } else {
        Err(CoordinateError::ConversionFailed(
            "BON requires a finite conformal latitude".into(),
        ))
    }
}

// The Bonne projection uses the sinusoidal projection in the zero-latitude
// limit. The radius of each latitude arc is cot(theta1) + theta1 - theta.
pub(super) fn project(phi: f64, theta: f64, theta1: f64) -> Result<(f64, f64), CoordinateError> {
    if theta1 == 0.0 {
        return project_sfl(phi, theta);
    }
    let radius0 = 1.0 / theta1.tan() + theta1;
    let radius = radius0 - theta;
    if radius == 0.0 {
        return Ok((0.0, radius0));
    }
    let (sine, cosine) = (phi * theta.cos() / radius).sin_cos();
    Ok((radius * sine, radius0 - radius * cosine))
}

pub(super) fn deproject(x: f64, y: f64, theta1: f64) -> Result<(f64, f64), CoordinateError> {
    let (phi, theta) = if theta1 == 0.0 {
        deproject_sfl(x, y)?
    } else {
        let radius0 = 1.0 / theta1.tan() + theta1;
        let dy = radius0 - y;
        let radius = x.hypot(dy).copysign(theta1);
        let theta = radius0 - radius;
        let phi = if theta.abs() == FRAC_PI_2 || radius == 0.0 {
            0.0
        } else {
            (x / radius).atan2(dy / radius) * radius / theta.cos()
        };
        (phi, theta)
    };
    if phi.abs() <= PI + 2e-13 && theta.abs() <= FRAC_PI_2 + 2e-13 {
        Ok((phi.clamp(-PI, PI), theta.clamp(-FRAC_PI_2, FRAC_PI_2)))
    } else {
        Err(CoordinateError::ConversionFailed(
            "Pixel is outside the BON projection".into(),
        ))
    }
}
