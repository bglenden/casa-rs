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
/// Each sample is `(coordinate, observed, weight)`. Supply a cloneable iterator
/// over the admitted channels, with finite coordinates on the caller's
/// frequency basis and positive finite weights. The caller selects usable
/// channels and reduces `terms` when fewer channels are available. Fit real and
/// imaginary components independently.
///
/// Returns coefficients in increasing power order, or `None` if the input
/// cannot be fitted. Rank-deficient designs use the existing SVD cutoff.
#[must_use]
pub fn fit_polynomial_row(
    samples: impl ExactSizeIterator<Item = (f64, f64, f64)> + Clone,
    terms: usize,
    weights: RowWeights,
) -> Option<Vec<f64>> {
    if terms == 0 || samples.len() < terms {
        return None;
    }

    let (weight_scale, passes) = match weights {
        RowWeights::Supplied => (1.0, 1),
        RowWeights::Reweighted(passes) => (
            samples
                .clone()
                .map(|(_, _, weight)| weight)
                .fold(0.0, f64::max),
            passes.get(),
        ),
    };
    let mut design = Vec::with_capacity(samples.len());
    for (coordinate, observed, weight) in samples.clone() {
        if !coordinate.is_finite() || !observed.is_finite() || !weight.is_finite() || weight <= 0.0
        {
            return None;
        }
        let mut basis = Vec::with_capacity(terms);
        let mut power = 1.0;
        for _ in 0..terms {
            basis.push(power);
            power *= coordinate;
        }
        design.push((basis, observed, weight / weight_scale));
    }

    if passes > 1 {
        let mut active = vec![true; samples.len()];
        let amplitude = design
            .iter()
            .map(|&(_, observed, _)| observed.abs())
            .fold(0.0, f64::max);
        for _ in 1..passes {
            let coefficients = solve_weighted_least_squares(&design, terms)?;
            if !update_weights(
                &mut design,
                samples.clone(),
                &mut active,
                &coefficients,
                amplitude,
            ) {
                return Some(coefficients);
            }
        }
    }
    solve_weighted_least_squares(&design, terms)
}

fn update_weights(
    design: &mut [(Vec<f64>, f64, f64)],
    samples: impl Iterator<Item = (f64, f64, f64)>,
    active: &mut [bool],
    coefficients: &[f64],
    amplitude: f64,
) -> bool {
    let mut largest = 0.0_f64;
    for (((_, observed, weight), (coordinate, _, _)), is_active) in
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
        assert!(fit_polynomial_row(std::iter::empty(), 1, RowWeights::Supplied).is_none());
        assert!(
            fit_polynomial_row(std::iter::once((0.0, 2.0, 1.0)), 0, RowWeights::Supplied).is_none()
        );
        for sample in [
            (f64::NAN, 2.0, 1.0),
            (0.0, f64::INFINITY, 1.0),
            (0.0, 2.0, 0.0),
        ] {
            assert!(fit_polynomial_row(std::iter::once(sample), 1, RowWeights::Supplied).is_none());
        }
    }
}
