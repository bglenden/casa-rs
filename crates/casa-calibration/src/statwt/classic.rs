// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample weights from the classical variances of the real and imaginary data.

use casa_types::Complex32;

#[derive(Clone, Copy, Default)]
pub(super) struct Moments {
    count: usize,
    mean: f64,
    squared_deviation: f64,
}

impl Moments {
    pub(super) fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        self.squared_deviation += delta * (value - self.mean);
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
    pub(super) fn add(&mut self, value: Complex32) {
        self.real.add(f64::from(value.re));
        self.imaginary.add(f64::from(value.im));
    }

    pub(super) fn weight(self, minimum_samples: usize) -> f32 {
        if self.real.count < minimum_samples {
            return 0.0;
        }
        // CASA's statwt uses the population variance of each component
        // to construct weights. The reported variance *of* the weights
        // below uses the sample variance instead.
        let variance_sum = self.real.squared_deviation + self.imaginary.squared_deviation;
        if variance_sum <= 0.0 || !variance_sum.is_finite() {
            0.0
        } else {
            (2.0 * self.real.count as f64 / variance_sum) as f32
        }
    }
}
