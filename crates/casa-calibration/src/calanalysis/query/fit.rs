// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA polynomial fits of a selected calibration series.

use casa_provider_contracts::CasaValue;

use super::series::initial;

mod normal;
mod robust;

/// CASA's polynomial order, fitting method and error weighting controls.
#[derive(Debug, Clone, Copy)]
pub struct CalAnalysisFit<'a> {
    /// AVERAGE, LINEAR or QUADRATIC, or the corresponding initial.
    pub order: &'a str,
    /// LSQ or ROBUST, or the corresponding initial.
    pub method: &'a str,
    /// Weight by the inverse square of each solution uncertainty.
    pub weighted: bool,
}

#[derive(Clone, Copy)]
pub(super) struct FitOptions {
    degree: usize,
    robust: bool,
    weighted: bool,
}

impl FitOptions {
    pub(super) fn parse(options: CalAnalysisFit<'_>) -> Result<Self, String> {
        let degree = match initial(options.order, 'A') {
            'A' => 0,
            'L' => 1,
            'Q' => 2,
            _ => return Err("Invalid fit order".into()),
        };
        let robust = match initial(options.method, 'L') {
            'L' => false,
            'R' => true,
            _ => return Err("Invalid fit type".into()),
        };
        Ok(Self {
            degree,
            robust,
            weighted: options.weighted,
        })
    }

    pub(super) fn record(
        self,
        x: &[f64],
        values: &[f64],
        errors: &[f64],
        flags: &mut [bool],
    ) -> [(&'static str, CasaValue); 12] {
        let fit = self.run(x, values, errors, flags);
        let valid = fit.is_some();
        let fit = fit.unwrap_or_default();
        let order = ["AVERAGE", "LINEAR", "QUADRATIC"][self.degree];
        let method = if self.robust { "ROBUST" } else { "LSQ" };
        let weight = if self.weighted { "YES" } else { "NO" };
        [
            ("order", CasaValue::string(order)),
            ("type", CasaValue::string(method)),
            ("weight", CasaValue::string(weight)),
            ("validFit", CasaValue::Bool { value: valid }),
            ("pars", CasaValue::float64_vector(fit.coefficients)),
            ("vars", CasaValue::float64_vector(fit.variances)),
            ("covars", CasaValue::float64_vector(fit.covariances)),
            ("model", CasaValue::float64_vector(fit.model)),
            ("res", CasaValue::float64_vector(fit.residuals)),
            ("redChi2", CasaValue::float(fit.reduced_chi_square)),
            ("resMean", CasaValue::float(fit.residual_mean)),
            ("resVar", CasaValue::float(fit.residual_variance)),
        ]
    }

    fn run(self, x: &[f64], values: &[f64], errors: &[f64], flags: &mut [bool]) -> Option<Fit> {
        if self.robust {
            robust::trim(x, values, errors, flags, self.degree, self.weighted)?;
        }
        let active = x
            .iter()
            .zip(values)
            .zip(errors)
            .zip(flags.iter())
            .filter_map(|(((&x, &y), &sigma), &flag)| (!flag).then_some((x, y, sigma)))
            .collect::<Vec<_>>();
        let dof = active.len().checked_sub(self.degree)?;
        if dof == 0 {
            return None;
        }
        let x_scale = scale(active.iter().map(|&(x, _, _)| x))?;
        let y_scale = scale(active.iter().map(|&(_, y, _)| y))?;
        let mut system = normal::System::new(self.degree + 1);
        let samples = active
            .into_iter()
            .filter_map(|(x, y, error)| {
                let sigma = if self.weighted { error } else { 1. } / y_scale;
                (sigma != 0. && sigma != -1.).then_some((x / x_scale, y / y_scale, sigma))
            })
            .collect::<Vec<_>>();
        for &(x, y, sigma) in &samples {
            system.accumulate(x, y, sigma)?;
        }
        let solution = system.solve()?;
        // Form the small residuals directly. Subtracting the corresponding
        // normal-equation quadratic forms can lose their entire sum.
        let metric = samples
            .iter()
            .map(|&(x, y, sigma)| ((y - polynomial(&solution.coefficients, x)) / sigma).powi(2))
            .sum::<f64>()
            / dof as f64;
        let covariance_scale = if self.weighted { 1. } else { metric };
        let factors = [y_scale, y_scale / x_scale, y_scale / (x_scale * x_scale)];
        let coefficients = solution
            .coefficients
            .iter()
            .zip(factors)
            .map(|(&coefficient, factor)| coefficient * factor)
            .collect::<Vec<_>>();
        let mut variances = Vec::new();
        let mut covariances = Vec::new();
        for (row, values) in solution.covariance.iter().enumerate() {
            variances.push(values[row] * factors[row].powi(2) * covariance_scale);
            for column in row + 1..values.len() {
                covariances
                    .push(values[column] * factors[row] * factors[column] * covariance_scale);
            }
        }
        Some(Fit::new(
            x,
            values,
            coefficients,
            variances,
            covariances,
            if self.weighted { metric } else { 1. },
        ))
    }
}

fn scale(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for value in values {
        if !value.is_finite() {
            return None;
        }
        min = min.min(value);
        max = max.max(value);
    }
    let scale = min.abs() + max.abs();
    scale
        .is_finite()
        .then_some(if scale == 0. { 1. } else { scale })
}

#[derive(Default)]
struct Fit {
    coefficients: Vec<f64>,
    variances: Vec<f64>,
    covariances: Vec<f64>,
    model: Vec<f64>,
    residuals: Vec<f64>,
    reduced_chi_square: f64,
    residual_mean: f64,
    residual_variance: f64,
}

impl Fit {
    fn new(
        x: &[f64],
        values: &[f64],
        coefficients: Vec<f64>,
        variances: Vec<f64>,
        covariances: Vec<f64>,
        reduced_chi_square: f64,
    ) -> Self {
        let model = x
            .iter()
            .map(|&x| polynomial(&coefficients, x))
            .collect::<Vec<_>>();
        let residuals = values
            .iter()
            .zip(&model)
            .map(|(y, fitted)| y - fitted)
            .collect::<Vec<_>>();
        let residual_mean = residuals.iter().sum::<f64>() / residuals.len() as f64;
        let residual_variance = residuals
            .iter()
            .map(|value| (value - residual_mean).powi(2))
            .sum::<f64>()
            / residuals.len().saturating_sub(1).max(1) as f64;
        Self {
            coefficients,
            variances,
            covariances,
            model,
            residuals,
            reduced_chi_square,
            residual_mean,
            residual_variance,
        }
    }
}

fn polynomial(coefficients: &[f64], x: f64) -> f64 {
    coefficients
        .iter()
        .enumerate()
        .map(|(power, coefficient)| coefficient * x.powi(power as i32))
        .sum()
}
