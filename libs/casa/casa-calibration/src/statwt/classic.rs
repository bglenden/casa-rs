// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample weights from the classical variances of the real and imaginary data.

use casa_core::Complex32;

#[derive(Clone, Copy, Default)]
pub(super) struct Moments {
    pub(super) count: usize,
    pub(super) total_weight: f64,
    pub(super) mean: f64,
    pub(super) squared_deviation: f64,
}

impl Moments {
    pub(super) fn add_repeated(&mut self, value: f64, repetitions: usize) {
        if repetitions > 0 {
            self.add_weighted(value, repetitions as f64);
            self.count += repetitions - 1;
        }
    }

    pub(super) fn add_weighted(&mut self, value: f64, weight: f64) {
        if weight <= 0.0 {
            return;
        }
        self.count += 1;
        self.total_weight += weight;
        let delta = value - self.mean;
        self.mean += delta * weight / self.total_weight;
        self.squared_deviation += weight * delta * (value - self.mean);
    }

    pub(super) fn mean(self) -> f64 {
        self.mean
    }

    pub(super) fn sample_variance(self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            self.squared_deviation / (self.count - 1) as f64
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct VisibilityMoments {
    real: Moments,
    imaginary: Moments,
}

impl VisibilityMoments {
    pub(super) fn add(&mut self, value: Complex32, exposure: f64) {
        self.real.add_weighted(f64::from(value.re), exposure);
        self.imaginary.add_weighted(f64::from(value.im), exposure);
    }

    pub(super) fn variance(self) -> f64 {
        // CASA's statwt uses the population variance of each component
        // to construct weights. The reported variance *of* the weights
        // below uses the sample variance instead.
        (self.real.squared_deviation + self.imaginary.squared_deviation)
            / (2.0 * self.real.count as f64)
    }
}
