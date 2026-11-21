// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weighted cross-hand phase for the antenna-based X Jones family.

use num_complex::{Complex32, Complex64};

use crate::solve::kernel::complex32_to_64;

#[derive(Default)]
pub(super) struct CrossHandFit {
    saw_samples: bool,
    sum: Complex64,
}

impl CrossHandFit {
    pub(super) fn add(&mut self, data: [Complex32; 4], weights: [f32; 4]) {
        self.saw_samples = true;
        let weight = 0.5 * (weights[1] + weights[2]);
        self.sum += complex32_to_64(weight * data[1]) + complex32_to_64(weight * data[2]).conj();
    }

    pub(super) fn saw_samples(&self) -> bool {
        self.saw_samples
    }

    pub(super) fn phase(self) -> Option<Complex32> {
        let amplitude = self.sum.norm();
        if amplitude <= 0.0 || !amplitude.is_finite() {
            return None;
        }
        let unit = self.sum / amplitude;
        Some(Complex32::new(unit.re as f32, unit.im as f32))
    }
}
