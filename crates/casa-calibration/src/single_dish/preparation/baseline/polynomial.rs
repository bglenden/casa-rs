// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polynomial and Chebyshev fits used for single-dish baseline subtraction.

use casa_numerics::solve_weighted_least_squares;

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
    ) -> Result<Option<Fit>, String> {
        if spectrum.len() != self.basis.len() || spectrum.len() != included.len() {
            return Err(INVALID_FIT_ARGUMENT.into());
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
            if samples.len() < self.terms {
                return if iteration == 0 {
                    Ok(None)
                } else {
                    Err(FIT_FAILED.into())
                };
            }
            let coefficients =
                solve_weighted_least_squares(&samples, self.terms).ok_or(FIT_FAILED)?;
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
            let (sum, power) = errors
                .iter()
                .zip(&included)
                .filter(|&(_, &use_channel)| use_channel)
                .fold((0., 0.), |(sum, power), (&value, _)| {
                    let value = f64::from(value);
                    (sum + value, power + value * value)
                });
            let count = samples.len() as f64;
            let mean = sum / count;
            let rms = (power / count - mean * mean).abs().sqrt();
            if iteration + 1 < max_fits {
                let threshold = (clip_sigma * rms) as f32;
                let lower = (mean - f64::from(threshold)) as f32;
                let upper = (mean + f64::from(threshold)) as f32;
                let mut changed = false;
                // Libsakura does not clip the endpoint of each basis segment.
                // A polynomial uses one segment spanning the whole spectrum.
                for (&value, use_channel) in errors[..errors.len() - 1].iter().zip(&mut included) {
                    if *use_channel && (value - lower) * (upper - value) < 0. {
                        *use_channel = false;
                        changed = true;
                    }
                }
                if changed {
                    continue;
                }
            }
            return Ok(Some(Fit {
                coefficients: self.table_coefficients(coefficients),
                residuals: errors,
                included,
                rms: rms as f32,
            }));
        }
        Err(INVALID_FIT_ARGUMENT.into())
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
