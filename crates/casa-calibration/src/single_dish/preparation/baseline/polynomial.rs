// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polynomial and Chebyshev fits used for single-dish baseline subtraction.

use casa_numerics::solve_weighted_least_squares;

#[derive(Clone, Copy)]
pub(super) enum Polynomial {
    Monomial,
    Chebyshev,
}

pub(super) struct PolynomialFit {
    family: Polynomial,
    basis: Vec<Vec<f64>>,
    terms: usize,
}

pub(super) struct Fit {
    pub(super) coefficients: Vec<f64>,
    pub(super) residuals: Vec<f32>,
    pub(super) included: Vec<bool>,
    pub(super) rms: f32,
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
            basis,
            terms,
        })
    }

    pub(super) fn fit(
        &self,
        spectrum: &[f32],
        included: &[bool],
        clip_sigma: f64,
        max_fits: usize,
    ) -> Option<Fit> {
        if spectrum.len() != self.basis.len() || spectrum.len() != included.len() || max_fits == 0 {
            return None;
        }
        let mut included = included.to_vec();
        for iteration in 0..max_fits {
            let samples = self
                .basis
                .iter()
                .zip(spectrum)
                .zip(&included)
                .filter(|&(_, &use_channel)| use_channel)
                .map(|((terms, &value), _)| (terms.clone(), f64::from(value), 1.))
                .collect::<Vec<_>>();
            let coefficients = solve_weighted_least_squares(&samples, self.terms)?;
            let errors = self
                .basis
                .iter()
                .zip(spectrum)
                .map(|(basis, &value)| {
                    let fitted = basis
                        .iter()
                        .zip(&coefficients)
                        .map(|(value, coefficient)| value * coefficient)
                        .sum::<f64>() as f32;
                    value - fitted
                })
                .collect::<Vec<_>>();
            let power = errors
                .iter()
                .zip(&included)
                .filter(|&(_, &use_channel)| use_channel)
                .map(|(&value, _)| f64::from(value).powi(2))
                .sum::<f64>();
            let rms = (power / samples.len() as f64).sqrt();
            if iteration + 1 < max_fits {
                let threshold = clip_sigma * rms;
                let mut changed = false;
                for (&value, use_channel) in errors.iter().zip(&mut included) {
                    if *use_channel && f64::from(value.abs()) > threshold {
                        *use_channel = false;
                        changed = true;
                    }
                }
                if changed {
                    continue;
                }
            }
            return Some(Fit {
                coefficients: self.table_coefficients(coefficients),
                residuals: errors,
                included,
                rms: rms as f32,
            });
        }
        None
    }

    fn table_coefficients(&self, mut coefficients: Vec<f64>) -> Vec<f64> {
        if matches!(self.family, Polynomial::Monomial) {
            let span = self.basis.len().saturating_sub(1).max(1) as f64;
            let mut scale = 1.;
            for coefficient in &mut coefficients {
                *coefficient /= scale;
                scale *= span;
            }
        }
        coefficients
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
