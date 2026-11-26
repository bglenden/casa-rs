// SPDX-License-Identifier: LGPL-3.0-or-later
//! PBMath1DGauss::fillPBArray, preserving CASA's Float voltage samples.

#[derive(Clone)]
pub(super) struct GaussianVoltageTable {
    maximum_radius: f64,
    values: Box<[f32]>,
}

impl GaussianVoltageTable {
    pub(super) fn new(half_width: f64, maximum_radius: f64, is_voltage: bool) -> Self {
        let factor = (1. / std::f64::consts::LN_2).sqrt() * half_width;
        let factor_squared = factor * factor;
        let inverse_increment = 9999. / maximum_radius;
        let inverse_increment_squared = inverse_increment * inverse_increment;
        let values = (0..10000)
            .map(|index| {
                let index = f64::from(index);
                let value = (-(index * index / inverse_increment_squared / factor_squared)).exp();
                (if is_voltage { value } else { value.sqrt() }) as f32
            })
            .collect();
        Self {
            maximum_radius,
            values,
        }
    }

    pub(super) fn maximum_radius(&self) -> f64 {
        self.maximum_radius
    }

    pub(super) fn evaluate(&self, radius: f64) -> f32 {
        if !radius.is_finite() || radius < 0. || radius > self.maximum_radius {
            return 0.;
        }
        let index = (radius * 9999. / self.maximum_radius)
            .floor()
            .clamp(0., 9999.) as usize;
        self.values[index]
    }
}
