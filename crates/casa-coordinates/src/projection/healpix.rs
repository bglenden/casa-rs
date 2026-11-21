// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{FRAC_PI_2, PI, TAU};

use crate::CoordinateError;

/// The equatorial band and polar facets in Calabretta & Roukema (2007).
pub(super) struct Healpix {
    facets: f64,
    layers: f64,
    half_width: f64,
}

impl Healpix {
    pub(super) fn new(parameters: &[f64]) -> Result<Self, CoordinateError> {
        let facets = parameters.first().copied().unwrap_or(4.0);
        let layers = parameters.get(1).copied().unwrap_or(3.0);
        if !(facets.is_finite() && layers.is_finite() && facets > 0.0 && layers > 0.0) {
            return Err(CoordinateError::ConversionFailed(
                "HPX needs positive, finite longitude and latitude facet counts".into(),
            ));
        }
        Ok(Self {
            facets,
            layers,
            half_width: PI / facets,
        })
    }

    fn facet_offset(&self, longitude: f64, southern: bool) -> f64 {
        let facet = ((longitude + PI) * self.facets / TAU).floor();
        let center = -PI + (2.0 * facet + 1.0) * self.half_width;
        let offset = longitude - center;
        if southern && self.layers.round() % 2.0 == 0.0 {
            let index = (longitude / self.half_width).floor() + self.facets.round() % 2.0;
            if index % 2.0 == 0.0 {
                offset + self.half_width
            } else {
                offset - self.half_width
            }
        } else {
            offset
        }
    }

    pub(super) fn project(&self, phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
        let sin_theta = theta.sin();
        let z = sin_theta.abs();
        if z <= (self.layers - 1.0) / self.layers {
            return Ok((phi, self.half_width * self.layers * sin_theta / 2.0));
        }
        let sigma = (self.layers * (1.0 - z)).sqrt();
        let offset = self.facet_offset(phi, theta < 0.0);
        let x = phi + offset * (sigma - 1.0);
        let y = self.half_width * ((self.layers + 1.0) / 2.0 - sigma);
        Ok((if x > PI { TAU - x } else { x }, y.copysign(theta)))
    }

    pub(super) fn deproject(&self, x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
        const TOLERANCE: f64 = 2.0e-14;
        let invalid =
            || CoordinateError::ConversionFailed("Pixel is outside the HPX projection".into());
        let outer = self.half_width * (self.layers + 1.0) / 2.0;
        if !x.is_finite() || !y.is_finite() || y.abs() > outer {
            return Err(invalid());
        }
        let (phi, theta) = if y.abs() <= self.half_width * (self.layers - 1.0) / 2.0 {
            (x, (2.0 * y / (self.half_width * self.layers)).asin())
        } else {
            let sigma = (self.layers + 1.0) / 2.0 - y.abs() / self.half_width;
            let offset = self.facet_offset(x, y < 0.0);
            let difference = if sigma == 0.0 {
                1.0e9 * offset
            } else {
                offset / sigma
            };
            let sin_theta = 1.0 - sigma * sigma / self.layers;
            if difference.abs() > self.half_width + TOLERANCE || sin_theta < -1.0 {
                return Err(invalid());
            }
            (x + difference - offset, sin_theta.asin().copysign(y))
        };
        if phi.abs() > PI + TOLERANCE || theta.abs() > FRAC_PI_2 + TOLERANCE {
            return Err(invalid());
        }
        Ok((phi.clamp(-PI, PI), theta))
    }
}
