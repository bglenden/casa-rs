// SPDX-License-Identifier: LGPL-3.0-or-later
//! Estimate the cross-hand source multiplier before solving instrument leakage.

use num_complex::{Complex32, Complex64};

use crate::solve::kernel::complex32_to_64;

use super::standard::StandardSample;

#[derive(Clone, Copy)]
pub(super) enum SourceFitMode {
    Polarization,
    Angle,
}

impl SourceFitMode {
    pub(super) fn report(self, multiplier: Complex32, (field, spw): (i32, i32)) {
        let position_angle_degrees = 0.5 * multiplier.arg().to_degrees();
        match self {
            Self::Polarization => tracing::info!(
                field,
                spw,
                q = multiplier.re,
                u = multiplier.im,
                fraction = multiplier.norm(),
                position_angle_degrees,
                "Fractional polarization solution"
            ),
            Self::Angle => tracing::info!(
                field,
                spw,
                position_angle_degrees,
                "Position angle offset solution"
            ),
        }
    }
}

#[derive(Default)]
pub(super) struct SourceFit {
    weight: f64,
    power: f64,
    model: Complex64,
    observed: Complex64,
    product: Complex64,
    has_model: bool,
}

impl SourceFit {
    pub(super) fn has_model(&self) -> bool {
        self.has_model
    }

    pub(super) fn observe(&mut self, sample: &StandardSample) {
        self.has_model |= sample.model.iter().any(|&value| value != Complex32::ZERO);
        let [_, rl, lr, _] = sample.model;
        for ((model, observed), weight) in [rl, lr.conj()]
            .into_iter()
            .zip([sample.data[1], sample.data[2].conj()])
            .zip([sample.weights[1], sample.weights[2]])
        {
            if weight > 0.0 && model != Complex32::ZERO && observed != Complex32::ZERO {
                let weight = f64::from(weight);
                let model = complex32_to_64(model);
                let observed = complex32_to_64(observed);
                self.weight += weight;
                self.power += weight * model.norm_sqr();
                self.model += weight * model;
                self.observed += weight * observed;
                self.product += weight * model.conj() * observed;
            }
        }
    }

    pub(super) fn finish(self, mode: SourceFitMode) -> Option<Complex32> {
        if self.weight == 0.0 {
            // CASA's normal-equation fitter substitutes unit diagonals
            // for parameters with no observations. The resulting slope
            // is zero, giving zero QU or an identity angle correction.
            return Some(match mode {
                SourceFitMode::Polarization => Complex32::ZERO,
                SourceFitMode::Angle => Complex32::ONE,
            });
        }
        let (coefficients, rank) = casa_numerics::solve_complex_normal_equations(
            vec![
                self.weight.into(),
                self.model,
                self.model.conj(),
                self.power.into(),
            ],
            vec![self.observed, self.product],
        )?;
        if rank < 2 {
            return None;
        }
        let value = coefficients[1];
        let value = Complex32::new(value.re as f32, value.im as f32);
        Some(match mode {
            SourceFitMode::Polarization => value,
            SourceFitMode::Angle => Complex32::from_polar(1.0, value.arg()),
        })
    }
}
