// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA LimbDarkenedDiskShape::calcVis: Γ(η+1) (2/r)^η Jη(r), η=1+n/2.
use num_complex::Complex64;

use crate::ComponentError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct LimbDarkening {
    order: f64,
    log_gamma: f64,
}

fn refused(reason: impl ToString) -> ComponentError {
    ComponentError::Visibility {
        shape: "limb-darkened disk",
        reason: reason.to_string(),
    }
}

impl LimbDarkening {
    pub(super) fn new(exponent: f64) -> Result<Self, ComponentError> {
        let order = 1. + f64::from(exponent as f32) / 2.;
        if !order.is_finite() || order < 0. {
            return Err(refused(
                "the stored exponent must be finite and at least -2",
            ));
        }
        Ok(Self {
            order,
            log_gamma: libm::lgamma(order + 1.),
        })
    }

    pub(super) fn at_radius(self, radius: f64) -> Result<f64, ComponentError> {
        if !radius.is_finite() || radius < 0. {
            return Err(refused("the Fourier radius must be finite and nonnegative"));
        }
        // The normalized power series avoids multiplying an underflowed Jη
        // by an overflowing r^-η near zero. In this range its terms decrease.
        if radius <= 2. * (self.order + 1.).sqrt() {
            let mut sum = 1.;
            let mut term = 1.;
            for k in 1..=64 {
                let k = f64::from(k);
                term *= -(radius * radius / 4.) / (k * (self.order + k));
                sum += term;
                if term.abs() <= f64::EPSILON * sum.abs() {
                    break;
                }
            }
            return Ok(sum);
        }
        // AMOS 644 supplies real-order J without a native GSL dependency or
        // heap allocation. Log normalization avoids intermediate overflow.
        let bessel = complex_bessel::besselj(self.order, Complex64::new(radius, 0.))
            .map_err(refused)?
            .re;
        let log_scale = self.log_gamma + self.order * (2. / radius).ln();
        if bessel == 0. && log_scale > 0. {
            return Err(refused("Bessel underflow prevents normalized evaluation"));
        }
        Ok(bessel.signum() * (log_scale + bessel.abs().ln()).exp())
    }
}
