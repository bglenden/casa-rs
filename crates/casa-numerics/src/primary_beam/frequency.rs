// SPDX-License-Identifier: LGPL-3.0-or-later
//! PBMath1DIPoly's coefficient columns and Float voltage interpolation.
use super::{BeamPattern, Lookup};
use std::sync::Arc;

pub(super) struct Curve {
    pub(super) maximum_radius: f64,
    coefficients: Box<[f64]>,
    frequencies: Box<[f64]>,
    per_fit: usize,
    is_voltage: bool,
}

impl BeamPattern {
    /// A saved inverse-polynomial matrix. Coefficients are fit-major, each
    /// column's even powers in ascending order; frequencies are in Hz.
    /// Input storage is shared by clones, without a dense table per fit.
    #[must_use]
    pub fn frequency_inverse_polynomial(
        maximum_radius: f64,
        coefficients: Vec<f64>,
        frequencies: Vec<f64>,
        is_voltage: bool,
    ) -> Option<Self> {
        if !maximum_radius.is_finite()
            || maximum_radius <= 0.
            || frequencies.is_empty()
            || coefficients.is_empty()
            || !coefficients.len().is_multiple_of(frequencies.len())
            || coefficients.iter().any(|v| !v.is_finite())
            || frequencies.iter().any(|v| !v.is_finite() || *v <= 0.)
        {
            return None;
        }
        Some(Self(Lookup::Frequency(Arc::new(Curve {
            maximum_radius,
            per_fit: coefficients.len() / frequencies.len(),
            coefficients: coefficients.into_boxed_slice(),
            frequencies: frequencies.into_boxed_slice(),
            is_voltage,
        }))))
    }
}

impl Curve {
    pub(super) fn at(&self, frequency: f64) -> Interpolated<'_> {
        let upper = self
            .frequencies
            .iter()
            .position(|f| frequency <= *f)
            .unwrap_or(self.frequencies.len());
        let high = upper.min(self.frequencies.len() - 1);
        let low = if upper == self.frequencies.len() {
            high
        } else {
            high.saturating_sub(1)
        };
        let column = |index| &self.coefficients[index * self.per_fit..(index + 1) * self.per_fit];
        Interpolated {
            low: column(low),
            high: column(high),
            weight: if low == high {
                0.
            } else {
                ((frequency - self.frequencies[low])
                    / (self.frequencies[high] - self.frequencies[low])) as f32
            },
            maximum_radius: self.maximum_radius,
            is_voltage: self.is_voltage,
        }
    }
}

pub(super) struct Interpolated<'a> {
    low: &'a [f64],
    high: &'a [f64],
    weight: f32,
    maximum_radius: f64,
    is_voltage: bool,
}

impl Interpolated<'_> {
    pub(super) fn voltage(&self, radius: f64) -> f32 {
        if !radius.is_finite() || !(0. ..=self.maximum_radius).contains(&radius) {
            return 0.;
        }
        let inverse_increment = 9999. / self.maximum_radius;
        let index = (radius * inverse_increment).floor();
        let x2 = (index / inverse_increment).powi(2);
        let sample = |coefficients: &[f64]| {
            let mut power = 1.;
            let mut taper = 0.;
            for c in coefficients {
                taper += power * c;
                power *= x2;
            }
            if self.is_voltage {
                if taper == 0. { 0. } else { (1. / taper) as f32 }
            } else if taper > 0. {
                (1. / taper.sqrt()) as f32
            } else {
                0.
            }
        };
        sample(self.low) * (1. - self.weight) + sample(self.high) * self.weight
    }
}
