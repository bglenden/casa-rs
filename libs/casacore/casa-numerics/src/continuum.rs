// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weighted polynomial fits for one visibility row and correlation component.

use std::num::NonZeroUsize;

use crate::solve_weighted_least_squares;

/// Weight handling for the continuum fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowWeights {
    /// Use the caller's weights for one fit, as in the imaging continuum stage.
    Supplied,
    /// Normalize the initial weights and refit with CASA's residual weights
    /// for up to this many passes. One pass performs only the normalized fit.
    Reweighted(NonZeroUsize),
}

/// Fit a real polynomial to admitted channels of one visibility row.
///
/// Each sample is `(coordinate, observed, weight)`. Supply finite coordinates
/// on the caller's frequency basis and positive finite weights. The caller
/// selects usable channels and reduces `terms` when fewer channels are
/// available. Fit real and imaginary components independently.
///
/// Returns coefficients in increasing power order, or `None` if the input
/// cannot be fitted. Rank-deficient designs use the existing SVD cutoff.
#[must_use]
pub fn fit_polynomial_row(
    samples: &[(f64, f64, f64)],
    terms: usize,
    weights: RowWeights,
) -> Option<Vec<f64>> {
    if terms == 0
        || samples.len() < terms
        || samples.iter().any(|&(coordinate, observed, weight)| {
            !coordinate.is_finite() || !observed.is_finite() || !weight.is_finite() || weight <= 0.0
        })
    {
        return None;
    }

    let (weight_scale, passes) = match weights {
        RowWeights::Supplied => (1.0, 1),
        RowWeights::Reweighted(passes) => (
            samples
                .iter()
                .map(|&(_, _, weight)| weight)
                .fold(0.0, f64::max),
            passes.get(),
        ),
    };
    let mut design = samples
        .iter()
        .map(|&(coordinate, observed, weight)| {
            let basis = std::iter::successors(Some(1.0), |power| Some(power * coordinate))
                .take(terms)
                .collect::<Vec<_>>();
            (basis, observed, weight / weight_scale)
        })
        .collect::<Vec<_>>();

    let mut active = vec![true; samples.len()];
    let amplitude = samples
        .iter()
        .map(|&(_, observed, _)| observed.abs())
        .fold(0.0, f64::max);
    for _ in 1..passes {
        let coefficients = solve_weighted_least_squares(&design, terms)?;
        if !update_weights(&mut design, samples, &mut active, &coefficients, amplitude) {
            return Some(coefficients);
        }
    }
    solve_weighted_least_squares(&design, terms)
}

fn update_weights(
    design: &mut [(Vec<f64>, f64, f64)],
    samples: &[(f64, f64, f64)],
    active: &mut [bool],
    coefficients: &[f64],
    amplitude: f64,
) -> bool {
    let mut largest = 0.0_f64;
    for (((_, observed, weight), &(coordinate, _, _)), is_active) in
        design.iter_mut().zip(samples).zip(active.iter())
    {
        let residual = if *is_active {
            let prediction = coefficients
                .iter()
                .rev()
                .fold(0.0, |sum, value| sum * coordinate + value);
            (*observed - prediction).abs()
        } else {
            0.0
        };
        largest = largest.max(residual);
        *weight = residual;
    }
    if largest <= amplitude * 64.0 * f64::EPSILON {
        return false;
    }
    for ((_, _, weight), is_active) in design.iter_mut().zip(active.iter_mut()) {
        // The SVD solver requires strictly positive weights. Near-zero
        // weights retain the rows without moving a finite solution.
        if *is_active && *weight < largest {
            *weight = (largest - *weight) / largest;
        } else {
            *is_active = false;
            *weight = f64::MIN_POSITIVE;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_or_invalid_rows_fail_before_the_svd() {
        assert!(fit_polynomial_row(&[], 1, RowWeights::Supplied).is_none());
        assert!(fit_polynomial_row(&[(0.0, 2.0, 1.0)], 0, RowWeights::Supplied).is_none());
        for sample in [
            (f64::NAN, 2.0, 1.0),
            (0.0, f64::INFINITY, 1.0),
            (0.0, 2.0, 0.0),
        ] {
            assert!(fit_polynomial_row(&[sample], 1, RowWeights::Supplied).is_none());
        }
    }
}
