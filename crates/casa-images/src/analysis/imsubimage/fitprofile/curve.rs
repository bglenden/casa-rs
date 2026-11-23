// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{LN_2, PI};

use super::models::{LineEstimate, LineKind, ProfileModels, SpectralPolynomial};

/// Parameter values and references for a sum of line profiles and continua.
pub(super) struct Curve {
    pub(super) parameters: Vec<f64>,
    pub(super) fixed: Vec<bool>,
    pub(super) lines: Vec<Line>,
    pub(super) polynomial: Option<std::ops::Range<usize>>,
    pub(super) spectral: Option<(SpectralPolynomial, std::ops::Range<usize>)>,
}

#[derive(Clone, Copy)]
pub(super) struct Line {
    pub(super) kind: LineKind,
    values: [Binding; 3],
    pub(super) multiplet: Option<usize>,
}

#[derive(Clone, Copy)]
struct Binding {
    index: usize,
    factor: f64,
    delta: f64,
}

impl Binding {
    fn value(self, parameters: &[f64]) -> f64 {
        parameters[self.index] * self.factor + self.delta
    }
}

impl Curve {
    pub(super) fn new(
        models: &ProfileModels,
        automatic: &[LineEstimate],
        polynomial: Option<usize>,
    ) -> Self {
        let mut curve = Self {
            parameters: Vec::new(),
            fixed: Vec::new(),
            lines: Vec::new(),
            polynomial: None,
            spectral: None,
        };
        for (index, multiplet) in models.multiplets.iter().enumerate() {
            let head = curve.add_line(multiplet.components[0], Some(index), None);
            for (line, constraints) in multiplet.components[1..].iter().zip(&multiplet.constraints)
            {
                curve.add_line(*line, Some(index), Some((head, *constraints)));
            }
        }
        for &line in models.singlets.iter().chain(automatic) {
            curve.add_line(line, None, None);
        }
        if let Some(spectral) = &models.spectral_polynomial {
            let first = curve.parameters.len();
            curve.parameters.extend_from_slice(&spectral.values);
            curve.fixed.extend_from_slice(&spectral.fixed);
            curve.spectral = Some((spectral.kind, first..curve.parameters.len()));
        }
        if let Some(order) = polynomial {
            let first = curve.parameters.len();
            curve.parameters.resize(first + order + 1, 0.);
            curve.fixed.resize(first + order + 1, false);
            curve.polynomial = Some(first..curve.parameters.len());
        }
        curve
    }

    fn add_line(
        &mut self,
        estimate: LineEstimate,
        multiplet: Option<usize>,
        tied: Option<([Binding; 3], [f64; 3])>,
    ) -> [Binding; 3] {
        let values = std::array::from_fn(|index| {
            if let Some((parent, constraints)) = tied
                && constraints[index] != 0.
            {
                let mut binding = parent[index];
                if index == 1 {
                    binding.delta += constraints[index];
                } else {
                    binding.factor *= constraints[index];
                }
                return binding;
            }
            let binding = Binding {
                index: self.parameters.len(),
                factor: 1.,
                delta: 0.,
            };
            self.parameters.push(estimate.values[index]);
            self.fixed.push(estimate.fixed[index]);
            binding
        });
        self.lines.push(Line {
            kind: estimate.kind,
            values,
            multiplet,
        });
        values
    }

    pub(super) fn fitted_value(&self, parameters: &[f64], x: f64) -> f64 {
        let continuum = self
            .polynomial
            .clone()
            .map_or(0., |range| evaluate_polynomial(&parameters[range], x));
        let spectrum = self.spectral.as_ref().map_or(0., |(kind, range)| {
            let coefficients = &parameters[range.clone()];
            match kind {
                SpectralPolynomial::PowerLog => {
                    let log = x.ln();
                    coefficients[0] * (log * evaluate_polynomial(&coefficients[1..], log)).exp()
                }
                SpectralPolynomial::LogTransformed => evaluate_polynomial(coefficients, x),
            }
        });
        continuum
            + spectrum
            + self
                .lines
                .iter()
                .map(|line| line.value(parameters, x))
                .sum::<f64>()
    }

    /// Accumulate the slopes of the model with respect to the independent
    /// parameters. Contributions from tied lines share the same slope slot.
    pub(super) fn gradient(&self, parameters: &[f64], x: f64, slopes: &mut [f64]) {
        slopes.fill(0.);
        for line in &self.lines {
            let [amplitude, center, width] = line.parameters(parameters);
            let z = (x - center) / width;
            let shape = line.shape(z);
            let (center_slope, width_slope) = if shape == 0. {
                (0., 0.)
            } else {
                let center = match line.kind {
                    LineKind::Gaussian => amplitude * shape * (8. * LN_2) * z / width,
                    LineKind::Lorentzian => amplitude * shape * shape * 8. * z / width,
                };
                (center, center * z)
            };
            for (binding, part) in line.values.iter().zip([shape, center_slope, width_slope]) {
                slopes[binding.index] += part * binding.factor;
            }
        }
        if let Some(range) = &self.polynomial {
            polynomial_gradient(&mut slopes[range.clone()], 1., x);
        }
        if let Some((kind, range)) = &self.spectral {
            let coefficients = &parameters[range.clone()];
            let gradient = &mut slopes[range.clone()];
            match kind {
                SpectralPolynomial::PowerLog => {
                    let t = x.ln();
                    let normalization = (t * evaluate_polynomial(&coefficients[1..], t)).exp();
                    gradient[0] = normalization;
                    polynomial_gradient(&mut gradient[1..], coefficients[0] * normalization * t, t);
                }
                SpectralPolynomial::LogTransformed => polynomial_gradient(gradient, 1., x),
            }
        }
    }

    pub(super) fn is_linear(&self) -> bool {
        self.lines.is_empty() && !matches!(self.spectral, Some((SpectralPolynomial::PowerLog, _)))
    }

    pub(super) fn valid_widths(&self, parameters: &[f64]) -> bool {
        self.lines
            .iter()
            .all(|line| line.values[2].value(parameters).abs() > 0.)
    }
}

impl Line {
    pub(super) fn fixed(&self, fixed: &[bool]) -> [bool; 3] {
        self.values.map(|binding| fixed[binding.index])
    }

    pub(super) fn parameters(&self, values: &[f64]) -> [f64; 3] {
        self.values.map(|binding| binding.value(values))
    }

    pub(super) fn errors(&self, errors: &[f64]) -> [f64; 3] {
        self.values
            .map(|binding| errors[binding.index] * binding.factor.abs())
    }

    fn value(&self, parameters: &[f64], x: f64) -> f64 {
        let [peak, center, width] = self.parameters(parameters);
        peak * self.shape((x - center) / width)
    }

    fn shape(&self, z: f64) -> f64 {
        match self.kind {
            LineKind::Gaussian => (-4. * LN_2 * z * z).exp(),
            LineKind::Lorentzian => 1. / (1. + 4. * z * z),
        }
    }

    pub(super) fn integral(&self, amplitude: f64, width: f64) -> f64 {
        amplitude
            * width.abs()
            * match self.kind {
                LineKind::Gaussian => (PI / (4. * LN_2)).sqrt(),
                LineKind::Lorentzian => PI / 2.,
            }
    }
}

fn evaluate_polynomial(coefficients: &[f64], argument: f64) -> f64 {
    coefficients
        .iter()
        .rev()
        .fold(0., |value, &coefficient| value * argument + coefficient)
}

fn polynomial_gradient(gradient: &mut [f64], mut power: f64, x: f64) {
    for slope in gradient {
        *slope = power;
        power *= x;
    }
}
