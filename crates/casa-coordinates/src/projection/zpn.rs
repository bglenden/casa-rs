// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{FRAC_PI_2, PI};

use crate::CoordinateError;

/// Zenithal polynomial with the outer limit of its branch at the pole.
pub(super) struct ZenithalPolynomial<'a> {
    coefficients: &'a [f64],
    limit: f64,
}

impl<'a> ZenithalPolynomial<'a> {
    pub(super) fn new(parameters: &'a [f64]) -> Result<Self, CoordinateError> {
        let bad =
            || CoordinateError::ConversionFailed("Invalid ZPN polynomial coefficients".into());
        let coefficients = &parameters[..parameters.len().min(30)];
        let degree = coefficients
            .iter()
            .rposition(|value| *value != 0.0)
            .ok_or_else(bad)?;
        let coefficients = &coefficients[..=degree];
        if coefficients.iter().any(|value| !value.is_finite())
            || (degree >= 2 && coefficients[1] <= 0.0)
        {
            return Err(bad());
        }
        let mut result = Self {
            coefficients,
            limit: PI,
        };
        if degree >= 2
            && coefficients[2..]
                .iter()
                .any(|coefficient| *coefficient < 0.0)
        {
            result.limit = result.first_stationary_distance();
        }
        Ok(result)
    }

    fn radius(&self, distance: f64) -> f64 {
        self.coefficients
            .iter()
            .rev()
            .fold(0.0, |acc, coefficient| acc * distance + coefficient)
    }

    fn slope(&self, distance: f64) -> f64 {
        self.coefficients
            .iter()
            .enumerate()
            .skip(1)
            .rev()
            .fold(0.0, |acc, (degree, coefficient)| {
                acc * distance + coefficient * degree as f64
            })
    }

    fn first_stationary_distance(&self) -> f64 {
        let mut lower = 0.0_f64;
        // WCSLIB brackets the first stationary point in one-degree steps.
        for degree in 1..180 {
            let mut upper = f64::from(degree).to_radians();
            if self.slope(upper) <= 0.0 {
                for _ in 0..45 {
                    let middle = (lower + upper) / 2.0;
                    if self.slope(middle) > 0.0 {
                        lower = middle;
                    } else {
                        upper = middle;
                    }
                }
                return (lower + upper) / 2.0;
            }
            lower = upper;
        }
        PI
    }

    pub(super) fn project(&self, phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
        let distance = FRAC_PI_2 - theta;
        let radius = self.radius(distance);
        if distance > self.limit || !radius.is_finite() {
            return Err(CoordinateError::ConversionFailed(
                "Direction is outside the ZPN projection".into(),
            ));
        }
        let (sine, cosine) = phi.sin_cos();
        Ok((radius * sine, -radius * cosine))
    }

    pub(super) fn deproject(&self, x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
        const TOLERANCE: f64 = 1.0e-13;
        let invalid =
            || CoordinateError::ConversionFailed("Pixel is outside the ZPN projection".into());
        let dy = -y;
        let radius = x.hypot(dy);
        let distance = match self.coefficients {
            [_constant] => return Err(invalid()),
            [constant, linear] => (radius - constant) / linear,
            [constant, linear, quadratic] => {
                let c = constant - radius;
                let square = linear * linear - 4.0 * quadratic * c;
                if square < 0.0 {
                    return Err(invalid());
                }
                let denominator = -linear - square.sqrt();
                let roots = [denominator / (2.0 * quadratic), 2.0 * c / denominator];
                let nearest = roots[0].min(roots[1]);
                if nearest < -TOLERANCE {
                    roots[0].max(roots[1])
                } else {
                    nearest
                }
            }
            _ => {
                let maximum = self.radius(self.limit);
                if radius < self.coefficients[0] - TOLERANCE || maximum + TOLERANCE < radius {
                    return Err(invalid());
                }
                let (mut lower, mut upper) = (0.0, self.limit);
                for _ in 0..52 {
                    let middle = (lower + upper) / 2.0;
                    if self.radius(middle) < radius {
                        lower = middle;
                    } else {
                        upper = middle;
                    }
                }
                (lower + upper) / 2.0
            }
        };
        if !(-TOLERANCE..=PI + TOLERANCE).contains(&distance) {
            return Err(invalid());
        }
        let phi = if radius == 0.0 { 0.0 } else { x.atan2(dy) };
        Ok((phi, FRAC_PI_2 - distance.clamp(0.0, PI)))
    }
}
