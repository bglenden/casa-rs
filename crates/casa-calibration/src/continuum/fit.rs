// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fit a complex continuum to the time-averaged spectrum of each baseline.

use casa_numerics::solve_weighted_least_squares;
use casa_types::Complex32;
use ndarray::{Array2, ArrayView2};

pub(super) struct BaselineAverage {
    sums: Array2<Complex32>,
    weights: Array2<f32>,
    flagged: Option<Array2<bool>>,
}

impl BaselineAverage {
    pub(super) fn new(correlations: usize, channels: usize, old_iterator: bool) -> Self {
        let shape = (correlations, channels);
        Self {
            sums: Array2::zeros(shape),
            weights: Array2::zeros(shape),
            flagged: (!old_iterator).then(|| Array2::from_elem(shape, true)),
        }
    }

    pub(super) fn accumulate(
        &mut self,
        data: ArrayView2<'_, Complex32>,
        flags: ArrayView2<'_, bool>,
        weights: &[f32],
        spectrum: Option<ArrayView2<'_, f32>>,
        hands: &[usize],
    ) {
        for channel in 0..data.ncols() {
            for (hand, &correlation) in hands.iter().enumerate() {
                let input = (correlation, channel);
                let output = (hand, channel);
                let input_flagged = flags[input];
                if let Some(group_flags) = &mut self.flagged {
                    // VI2 averages flagged samples until an unflagged sample
                    // arrives. The MSSelector used by VI1 leaves these cells zero.
                    if input_flagged && !group_flags[output] {
                        continue;
                    }
                    if !input_flagged && group_flags[output] {
                        self.sums[output] = Complex32::ZERO;
                        self.weights[output] = 0.0;
                        group_flags[output] = false;
                    }
                } else if input_flagged {
                    continue;
                }
                let weight = spectrum.as_ref().map_or(weights[correlation], |w| w[input]);
                self.sums[output] += data[input] * weight;
                self.weights[output] += weight;
            }
        }
    }

    pub(super) fn model(self, fit: &FitGrid) -> Array2<Complex32> {
        let (hands, channels) = self.sums.dim();
        let mut result = Array2::zeros((hands, channels));
        for hand in 0..hands {
            let observed = fit.channels.iter().map(|&channel| {
                let weight = self.weights[[hand, channel]];
                if weight > 0.0 {
                    self.sums[[hand, channel]] / weight
                } else {
                    self.sums[[hand, channel]]
                }
            });
            fit.fit_hand(observed, result.row_mut(hand));
        }
        result
    }
}

pub(super) struct FitGrid {
    channels: Vec<usize>,
    apply: Vec<usize>,
    coordinates: Vec<f64>,
    terms: usize,
}

impl FitGrid {
    pub(super) fn new(order: usize, channels: Vec<usize>, apply: Vec<usize>, count: usize) -> Self {
        let span = count.saturating_sub(1).max(1) as f64;
        let requested_terms = order.saturating_add(1);
        Self {
            terms: if requested_terms <= channels.len() {
                requested_terms
            } else {
                0
            },
            channels,
            apply,
            // An affine change of x retains the polynomial space and avoids
            // ill-conditioned powers of a large integral channel index.
            coordinates: (0..count)
                .map(|channel| 2.0 * channel as f64 / span - 1.0)
                .collect(),
        }
    }

    fn fit_hand(
        &self,
        values: impl Iterator<Item = Complex32>,
        mut model: ndarray::ArrayViewMut1<'_, Complex32>,
    ) {
        let samples = self
            .channels
            .iter()
            .zip(values)
            .map(|(&channel, value)| (self.coordinates[channel], value, 1.0));
        let fit = ComplexPolynomial::fit(samples, self.terms);
        for &channel in &self.apply {
            model[channel] = fit.at(self.coordinates[channel]);
        }
    }
}

/// Two real least-squares fits on the same normalized coordinate axis.
#[derive(Default)]
pub(super) struct ComplexPolynomial {
    real: Vec<f64>,
    imaginary: Vec<f64>,
}

impl ComplexPolynomial {
    pub(super) fn fit(
        samples: impl Iterator<Item = (f64, Complex32, f64)>,
        max_terms: usize,
    ) -> Self {
        if max_terms == 0 {
            return Self::default();
        }
        if max_terms == 1 {
            return Self::constant(samples);
        }
        let samples = samples.collect::<Vec<_>>();
        let terms = max_terms.min(samples.len());
        if terms == 0 {
            return Self::default();
        }
        if terms == 1 {
            return Self::constant(samples.into_iter());
        }
        let weight_scale = samples
            .iter()
            .map(|&(_, _, weight)| weight)
            .fold(0.0, f64::max);
        let mut design = samples
            .iter()
            .map(|&(x, value, weight)| {
                let powers = std::iter::successors(Some(1.0), |power| Some(power * x))
                    .take(terms)
                    .collect::<Vec<_>>();
                (powers, f64::from(value.re), weight / weight_scale)
            })
            .collect::<Vec<_>>();
        let real = solve_weighted_least_squares(&design, terms).unwrap_or_default();
        for ((_, y, _), &(_, observed, _)) in design.iter_mut().zip(&samples) {
            *y = f64::from(observed.im);
        }
        let imaginary = solve_weighted_least_squares(&design, terms).unwrap_or_default();
        Self { real, imaginary }
    }

    fn constant(samples: impl Iterator<Item = (f64, Complex32, f64)>) -> Self {
        let (real, imaginary, weight) =
            samples.fold((0.0, 0.0, 0.0), |(re, im, total), (_, value, weight)| {
                (
                    re + f64::from(value.re) * weight,
                    im + f64::from(value.im) * weight,
                    total + weight,
                )
            });
        if weight == 0.0 {
            return Self::default();
        }
        Self {
            real: vec![real / weight],
            imaginary: vec![imaginary / weight],
        }
    }

    pub(super) fn at(&self, x: f64) -> Complex32 {
        let evaluate = |coefficients: &[f64]| {
            coefficients
                .iter()
                .rev()
                .fold(0.0, |sum, &coefficient| sum * x + coefficient) as f32
        };
        Complex32::new(evaluate(&self.real), evaluate(&self.imaginary))
    }

    pub(super) fn is_defined(&self) -> bool {
        !self.real.is_empty() && !self.imaginary.is_empty()
    }
}
