// SPDX-License-Identifier: LGPL-3.0-or-later
//! Saved radial PBMath curves on its 10,000-sample Float voltage grid.
use super::{BeamPattern, Lookup};
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct VoltageTable {
    pub(super) maximum_radius: f64,
    values: Arc<[f32]>,
}

impl VoltageTable {
    pub(super) fn evaluate(&self, radius: f64) -> f32 {
        if !radius.is_finite() || !(0. ..=self.maximum_radius).contains(&radius) {
            return 0.;
        }
        self.values[(radius * 9999. / self.maximum_radius)
            .floor()
            .clamp(0., 9999.) as usize]
    }
}

impl BeamPattern {
    /// Tabulate a radial voltage curve on PBMath1D's Float grid.
    /// Returns None for invalid support or any non-finite voltage sample.
    /// Samples share their storage when this pattern is cloned.
    pub fn tabulated_voltage(
        maximum_radius: f64,
        mut voltage: impl FnMut(f64) -> f32,
    ) -> Option<Self> {
        if !maximum_radius.is_finite() || maximum_radius <= 0. {
            return None;
        }
        let values: Arc<[f32]> = (0..10000)
            .map(|i| voltage(f64::from(i) * maximum_radius / 9999.))
            .collect();
        if values.iter().any(|v| !v.is_finite()) {
            return None;
        }
        Some(Self(Lookup::Tabulated(VoltageTable {
            maximum_radius,
            values,
        })))
    }

    /// PBMath1DPoly/IPoly: even powers of radius, optionally inverted,
    /// interpreting the supplied coefficients as voltage or power.
    #[must_use]
    pub fn polynomial(
        maximum_radius: f64,
        coefficients: &[f64],
        inverse: bool,
        is_voltage: bool,
    ) -> Option<Self> {
        Self::tabulated_voltage(maximum_radius, |radius| {
            let mut power = 1.;
            let mut value = 0.;
            for coefficient in coefficients {
                value += power * coefficient;
                power *= radius * radius;
            }
            if !is_voltage {
                value = value.sqrt();
            }
            if inverse && value != 0. {
                value = 1. / value;
            }
            value as f32
        })
    }

    /// PBMath1DCosPoly: sum c[j] cos(scale[j] r)^(j+1).
    #[must_use]
    pub fn cosine_polynomial(
        maximum_radius: f64,
        coefficients: &[f64],
        scales: &[f64],
        is_voltage: bool,
    ) -> Option<Self> {
        if coefficients.len() != scales.len() {
            return None;
        }
        Self::tabulated_voltage(maximum_radius, |radius| {
            let value: f64 = coefficients
                .iter()
                .zip(scales)
                .enumerate()
                .map(|(j, (coefficient, scale))| {
                    let cosine = (scale * radius).cos();
                    let mut power = 1.;
                    for _ in 0..=j {
                        power *= cosine;
                    }
                    coefficient * power
                })
                .sum();
            if is_voltage {
                value as f32
            } else {
                value.sqrt() as f32
            }
        })
    }

    /// PBMath1DNumeric: even extension, real Fourier transform, zero-pad
    /// to 20,000 samples and normalize the inverse at its centre. The
    /// finite cosine series evaluates this interpolation directly.
    #[must_use]
    pub fn numeric(maximum_radius: f64, samples: &[f32], is_voltage: bool) -> Option<Self> {
        if !(2..=10001).contains(&samples.len()) || samples.iter().any(|v| !v.is_finite()) {
            return None;
        }
        let half = samples.len() - 1;
        let coefficients: Vec<f64> = (0..=half)
            .map(|k| {
                let end = if k % 2 == 0 {
                    samples[half]
                } else {
                    -samples[half]
                };
                let interior: f64 = (1..half)
                    .map(|i| {
                        f64::from(samples[i])
                            * (std::f64::consts::PI * (k * i) as f64 / half as f64).cos()
                    })
                    .sum();
                f64::from((f64::from(samples[0] + end) + 2. * interior) as f32)
            })
            .collect();
        let sample = |index: usize| {
            (coefficients[0]
                + (1..=half)
                    .map(|k| {
                        let weight = if k == 10000 { 1. } else { 2. };
                        weight
                            * coefficients[k]
                            * (std::f64::consts::PI * (k * index) as f64 / 10000.).cos()
                    })
                    .sum::<f64>()) as f32
        };
        let centre = sample(0);
        if centre == 0. || !centre.is_finite() {
            return None;
        }
        let mut index = 0;
        Self::tabulated_voltage(maximum_radius, |_| {
            let value = sample(index) / centre;
            index += 1;
            if is_voltage {
                value
            } else {
                value.max(0.).sqrt()
            }
        })
    }
}
