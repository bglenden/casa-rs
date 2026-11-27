// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polynomial and Chebyshev fits used for single-dish baseline subtraction.

use casa_numerics::LeastSquaresDesign;

use super::linear_fit::{Fit, LinearFit};

pub(super) const FIT_FAILED: &str = "sakura_LSQFitPolynomialFloat() failed -- NG";
pub(super) const INVALID_FIT_ARGUMENT: &str =
    "sakura_LSQFitPolynomialFloat() failed -- InvalidArgument";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Polynomial {
    Monomial,
    Chebyshev,
}

pub(super) struct PolynomialFit {
    family: Polynomial,
    linear: LinearFit,
}

pub(super) struct PreparedPolynomial {
    basis: (Polynomial, usize, usize),
    included: Vec<bool>,
    design: Option<LeastSquaresDesign>,
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
        prepared: &mut Option<PreparedPolynomial>,
    ) -> Result<Option<Fit>, String> {
        let basis = (self.family, spectrum.len(), self.linear.terms);
        if prepared
            .as_ref()
            .is_none_or(|cached| cached.basis != basis || cached.included != included)
        {
            *prepared = Some(PreparedPolynomial {
                basis,
                included: included.to_vec(),
                design: None,
            });
        }
        let mut fit = self.linear.solve(
            spectrum,
            included,
            clip_sigma,
            max_fits,
            prepared.as_mut().map(|cached| &mut cached.design),
        )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use casa_numerics::solve_weighted_least_squares;

    #[test]
    fn nonfinite_observations_do_not_prepare_a_design() {
        let polynomial = PolynomialFit::new(Polynomial::Monomial, 1, 4).unwrap();
        let spectrum = [0., f32::NAN, 2., f32::INFINITY];
        let mut cached = None;
        assert_eq!(
            polynomial
                .fit(&spectrum, &[true; 4], 3., 1, &mut cached)
                .err()
                .as_deref(),
            Some(FIT_FAILED),
        );
        assert!(cached.as_ref().unwrap().design.is_none());
        assert!(
            polynomial
                .fit(&spectrum, &[false, true, false, false], 3., 1, &mut cached)
                .unwrap()
                .is_none()
        );
        assert!(cached.as_ref().unwrap().design.is_none());
        let fit = polynomial
            .fit(&spectrum, &[true, false, true, false], 3., 1, &mut cached)
            .unwrap()
            .unwrap();
        assert!((fit.coefficients[1] - 1.).abs() < 1e-12);
        assert!(cached.as_ref().unwrap().design.is_some());
    }

    #[test]
    fn prepared_polynomial_tracks_mask_and_basis() {
        let ordinary = PolynomialFit::new(Polynomial::Monomial, 2, 9).unwrap();
        let chebyshev = PolynomialFit::new(Polynomial::Chebyshev, 2, 9).unwrap();
        let lower_order = PolynomialFit::new(Polynomial::Chebyshev, 1, 9).unwrap();
        let all = [true; 9];
        let masked = [true, false, true, true, false, true, true, false, true];
        let first = [2., 1., 6., 4., 12., 5., 7., 9., 11.];
        let second = [5., 6., 4., 7., 2., 8., 9., 2., 10.];
        let mut cached = None;
        for (polynomial, mask, observations) in [
            (&ordinary, &all, &first),
            (&ordinary, &all, &second),
            (&ordinary, &masked, &second),
            (&chebyshev, &masked, &second),
            (&lower_order, &masked, &second),
            (&ordinary, &all, &first),
        ] {
            let samples: Vec<_> = polynomial
                .linear
                .basis
                .iter()
                .zip(observations)
                .zip(mask)
                .filter(|(_, include)| **include)
                .map(|((row, &value), _)| (row.clone(), f64::from(value), 1.))
                .collect();
            let mut expected =
                solve_weighted_least_squares(&samples, polynomial.linear.terms).unwrap();
            polynomial.scale_table_coefficients(&mut expected);
            let actual = polynomial
                .fit(observations, mask, 3., 1, &mut cached)
                .unwrap()
                .unwrap();
            for (value, reference) in actual.coefficients.iter().zip(expected) {
                assert!((value - reference).abs() <= 1e-12 * reference.abs().max(1.));
            }
        }
    }
}
