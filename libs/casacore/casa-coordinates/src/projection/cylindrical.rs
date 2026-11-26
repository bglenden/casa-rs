// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{FRAC_PI_2, PI};

use crate::CoordinateError;

fn invalid(message: &str) -> CoordinateError {
    CoordinateError::ConversionFailed(message.into())
}

pub(super) fn cyp_parameters(parameters: &[f64]) -> Result<(f64, f64), CoordinateError> {
    let mu = parameters.first().copied().unwrap_or(1.0);
    let lambda = parameters.get(1).copied().unwrap_or(1.0);
    if !mu.is_finite() || !lambda.is_finite() || lambda == 0.0 || mu + lambda == 0.0 {
        return Err(invalid(
            "CYP requires finite parameters and nonzero lambda and mu + lambda",
        ));
    }
    Ok((mu, lambda))
}

pub(super) fn cea_scale(parameters: &[f64]) -> Result<f64, CoordinateError> {
    let lambda = parameters.first().copied().unwrap_or(1.0);
    if 0.0 < lambda && lambda <= 1.0 {
        Ok(lambda)
    } else {
        Err(invalid("CEA requires 0 < lambda <= 1"))
    }
}

fn native_angles(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    const TOLERANCE: f64 = 1.0e-13;
    if phi.abs() <= PI + TOLERANCE && theta.abs() <= FRAC_PI_2 + TOLERANCE {
        Ok((phi.clamp(-PI, PI), theta.clamp(-FRAC_PI_2, FRAC_PI_2)))
    } else {
        Err(invalid("Pixel is outside the cylindrical projection"))
    }
}

pub(super) fn project_cyp(
    parameters: &[f64],
    phi: f64,
    theta: f64,
) -> Result<(f64, f64), CoordinateError> {
    let (mu, lambda) = cyp_parameters(parameters)?;
    let cos_theta = if theta.abs() == FRAC_PI_2 {
        0.0
    } else {
        theta.cos()
    };
    if mu + cos_theta == 0.0 {
        return Err(invalid(
            "CYP cannot project a direction on its singular latitude",
        ));
    }
    Ok((lambda * phi, (mu + lambda) * theta.sin() / (mu + cos_theta)))
}

pub(super) fn deproject_cyp(
    parameters: &[f64],
    x: f64,
    y: f64,
) -> Result<(f64, f64), CoordinateError> {
    let (mu, lambda) = cyp_parameters(parameters)?;
    let eta = y / (mu + lambda);
    let theta = eta.atan() + (mu * (eta / eta.hypot(1.0))).asin();
    native_angles(x / lambda, theta)
}

pub(super) fn project_cea(
    parameters: &[f64],
    phi: f64,
    theta: f64,
) -> Result<(f64, f64), CoordinateError> {
    Ok((phi, theta.sin() / cea_scale(parameters)?))
}

pub(super) fn deproject_cea(
    parameters: &[f64],
    x: f64,
    y: f64,
) -> Result<(f64, f64), CoordinateError> {
    let sin_theta = y * cea_scale(parameters)?;
    if sin_theta.abs() > 1.0 + 1.0e-13 {
        return Err(invalid("Pixel is outside the CEA projection"));
    }
    native_angles(x, sin_theta.clamp(-1.0, 1.0).asin())
}
