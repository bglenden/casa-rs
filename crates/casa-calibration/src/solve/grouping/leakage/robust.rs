// SPDX-License-Identifier: LGPL-3.0-or-later
//! The leakage fit's per-pass L1 weights and residual rejection flags.

use num_complex::Complex32;

use super::standard::{LeakageAlgebra, StandardSample};

#[derive(Default)]
pub(super) struct RobustWeights {
    accepted: Vec<[bool; 4]>,
    working: Vec<StandardSample>,
}

impl RobustWeights {
    pub(super) fn working<'a>(
        &'a mut self,
        nominal: &[&StandardSample],
        gains: &[[Complex32; 2]],
        valid: &[[bool; 2]],
        algebra: LeakageAlgebra,
        clamp: f32,
    ) -> Vec<&'a StandardSample> {
        self.accepted.resize(nominal.len(), [true; 4]);
        self.working.clear();
        for (sample, flags) in nominal.iter().zip(&self.accepted) {
            let mut weights = sample.weights;
            let residual = (clamp > 0.0).then(|| sample.residual(gains, valid, algebra).0);
            for (index, value) in weights.iter_mut().enumerate() {
                if !flags[index] {
                    *value = 0.0;
                } else if let Some(residual) = residual {
                    let power = residual[index].norm_sqr();
                    *value = if power > 0.0 {
                        *value / (power + clamp).sqrt()
                    } else {
                        0.0
                    };
                }
            }
            self.working.push(StandardSample {
                weights,
                ..**sample
            });
        }
        self.working.iter().collect()
    }

    pub(super) fn reject(
        &mut self,
        nominal: &[&StandardSample],
        gains: &[[Complex32; 2]],
        valid: &[[bool; 2]],
        algebra: LeakageAlgebra,
        threshold: f32,
    ) {
        let mut weighted_power = [0.0f64; 4];
        let mut weight = [0.0f64; 4];
        let powers: Vec<_> = nominal
            .iter()
            .zip(&self.accepted)
            .map(|(sample, accepted)| {
                let powers = sample
                    .residual(gains, valid, algebra)
                    .0
                    .map(|value| value.norm_sqr());
                for index in 0..4 {
                    let nominal = sample.weights[index];
                    if accepted[index] && nominal > 0.0 {
                        weighted_power[index] += f64::from(nominal * powers[index]);
                        weight[index] += f64::from(nominal);
                    }
                }
                powers
            })
            .collect();
        let cutoff: [f32; 4] = std::array::from_fn(|index| {
            if weight[index] > 0.0 {
                threshold * (weighted_power[index] / weight[index]).sqrt() as f32
            } else {
                0.0
            }
        });
        // CASA restarts each rejection from the nominal flags.
        for (accepted, powers) in self.accepted.iter_mut().zip(powers) {
            *accepted = std::array::from_fn(|index| powers[index].sqrt() <= cutoff[index]);
        }
    }
}
