// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polynomial and Chebyshev fits used for single-dish baseline subtraction.

use super::linear_fit::{Fit, LinearFit};

pub(super) const FIT_FAILED: &str = "sakura_LSQFitPolynomialFloat() failed -- NG";
pub(super) const INVALID_FIT_ARGUMENT: &str =
    "sakura_LSQFitPolynomialFloat() failed -- InvalidArgument";

#[derive(Clone, Copy)]
pub(super) enum Polynomial {
    Monomial,
    Chebyshev,
}

pub(super) struct PolynomialFit {
    family: Polynomial,
    linear: LinearFit,
}

impl PolynomialFit {
    pub(super) fn new(family: Polynomial, order: usize, channels: usize) -> Option<Self> {
        let terms = order.checked_add(1)?;
        if terms > channels {
            return None;
        }
        let basis = (0..channels)
            .map(|channel| family.basis(channel, channels, terms))
            .collect();
        Some(Self {
            family,
            linear: LinearFit {
                basis,
                terms,
                segment_ends: vec![channels],
                failure: FIT_FAILED,
            },
        })
    }

    pub(super) fn fit(
        &self,
        spectrum: &[f32],
        included: &[bool],
        clip_sigma: f64,
        max_fits: usize,
    ) -> Result<Option<Fit>, String> {
        let mut fit = self
            .linear
            .solve(spectrum, included, clip_sigma, max_fits)?;
        if let Some(fit) = &mut fit {
            self.scale_table_coefficients(&mut fit.coefficients);
        }
        Ok(fit)
    }

    fn scale_table_coefficients(&self, coefficients: &mut [f64]) {
        if matches!(self.family, Polynomial::Monomial) {
            let span = self.linear.basis.len().saturating_sub(1).max(1) as f64;
            let mut scale = 1.;
            for coefficient in coefficients {
                *coefficient /= scale;
                scale *= span;
            }
        }
    }
}

impl Polynomial {
    fn basis(self, channel: usize, channels: usize, terms: usize) -> Vec<f64> {
        let unit = channel as f64 / channels.saturating_sub(1).max(1) as f64;
        let coordinate = match self {
            Self::Monomial => unit,
            Self::Chebyshev => 2. * unit - 1.,
        };
        let mut row = Vec::with_capacity(terms);
        row.push(1.);
        if terms > 1 {
            row.push(coordinate);
        }
        for degree in 2..terms {
            let term = match self {
                Self::Monomial => coordinate * row[degree - 1],
                Self::Chebyshev => 2. * coordinate * row[degree - 1] - row[degree - 2],
            };
            row.push(term);
        }
        row
    }

    pub(super) fn subtract(self, spectrum: &mut [f32], coefficients: &[f64]) {
        let channels = spectrum.len();
        for (channel, sample) in spectrum.iter_mut().enumerate() {
            let baseline = match self {
                Self::Monomial => coefficients.iter().rev().fold(0., |value, coefficient| {
                    value * channel as f64 + coefficient
                }),
                Self::Chebyshev => self
                    .basis(channel, channels, coefficients.len())
                    .iter()
                    .zip(coefficients)
                    .map(|(value, coefficient)| value * coefficient)
                    .sum(),
            };
            *sample -= baseline as f32;
        }
    }
}
