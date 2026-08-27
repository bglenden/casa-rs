// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA-compatible visibility-domain continuum fitting and subtraction.

use casa_imaging_model::ContinuumChannelUse;
use casa_numerics::solve_weighted_least_squares;
use num_complex::Complex64;
use thiserror::Error;

/// One channel/correlation member supplied to the row-bounded transform.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContinuumSample {
    visibility: Complex64,
    flag: bool,
    weight: f64,
    use_role: ContinuumChannelUse,
}

impl ContinuumSample {
    /// Construct one native visibility sample and its compiled channel role.
    #[must_use]
    pub const fn new(
        visibility: Complex64,
        flag: bool,
        weight: f64,
        use_role: ContinuumChannelUse,
    ) -> Self {
        Self {
            visibility,
            flag,
            weight,
            use_role,
        }
    }

    /// Return the untransformed complex visibility.
    #[must_use]
    pub const fn visibility(self) -> Complex64 {
        self.visibility
    }

    /// Return the input flag. The transform never changes it.
    #[must_use]
    pub const fn flag(self) -> bool {
        self.flag
    }

    /// Return the input statistical weight. The transform never changes it.
    #[must_use]
    pub const fn weight(self) -> f64 {
        self.weight
    }

    /// Return the compiled fit/application role.
    #[must_use]
    pub const fn use_role(self) -> ContinuumChannelUse {
        self.use_role
    }

    /// Return this sample with its flag replaced.
    #[must_use]
    pub const fn with_flag(mut self, flag: bool) -> Self {
        self.flag = flag;
        self
    }
}

/// Borrowed input for one physical-row correlation fit.
#[derive(Debug, Clone, Copy)]
pub struct ContinuumRowInput<'a> {
    frequencies_hz: &'a [f64],
    samples: &'a [ContinuumSample],
    requested_order: u8,
}

impl<'a> ContinuumRowInput<'a> {
    /// Construct a row fit. Frequencies and samples must have equal lengths.
    #[must_use]
    pub const fn new(
        frequencies_hz: &'a [f64],
        samples: &'a [ContinuumSample],
        requested_order: u8,
    ) -> Self {
        Self {
            frequencies_hz,
            samples,
            requested_order,
        }
    }
}

/// Outcome of CASA's per-row/per-correlation valid-point policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuumFitStatus {
    /// A fit was evaluated, possibly after reducing the requested order.
    Fitted {
        /// Polynomial order actually used for this row/correlation.
        effective_order: u8,
    },
    /// No unflagged, positively weighted fit samples existed.
    NoValidFitSamples,
}

/// CASA-compatible continuum prediction and residual for one correlation.
#[derive(Debug, Clone, PartialEq)]
pub struct ContinuumRowResult {
    samples: Vec<ContinuumSample>,
    normalized_frequencies: Vec<f64>,
    fit_sample_indices: Vec<usize>,
    coefficients: Vec<Complex64>,
    prediction: Vec<Complex64>,
    residual: Vec<Complex64>,
    status: ContinuumFitStatus,
    chi_squared: [f64; 2],
}

impl ContinuumRowResult {
    /// Return the immutable source metadata retained for every channel.
    #[must_use]
    pub fn samples(&self) -> &[ContinuumSample] {
        &self.samples
    }

    /// Return CASA's complete-row min/max normalized frequency coordinates.
    ///
    /// This is retained as first-divergence instrumentation: a parity probe can
    /// compare the exact fit abscissa before examining solver coefficients.
    #[must_use]
    pub fn normalized_frequencies(&self) -> &[f64] {
        &self.normalized_frequencies
    }

    /// Return the ordered channel indices admitted to the weighted fit.
    ///
    /// The list is the discriminating mask after compiled roles, flags, and
    /// positive finite weights have been applied.
    #[must_use]
    pub fn fit_sample_indices(&self) -> &[usize] {
        &self.fit_sample_indices
    }

    /// Return complex polynomial coefficients in ascending monomial order.
    #[must_use]
    pub fn coefficients(&self) -> &[Complex64] {
        &self.coefficients
    }

    /// Return the fitted continuum at every supplied native frequency.
    #[must_use]
    pub fn prediction(&self) -> &[Complex64] {
        &self.prediction
    }

    /// Return input minus prediction at every supplied native frequency.
    #[must_use]
    pub fn residual(&self) -> &[Complex64] {
        &self.residual
    }

    /// Return the valid-point disposition and effective order.
    #[must_use]
    pub const fn status(&self) -> ContinuumFitStatus {
        self.status
    }

    /// Return weighted real and imaginary residual sums of squares on fit data.
    #[must_use]
    pub const fn chi_squared(&self) -> [f64; 2] {
        self.chi_squared
    }
}

/// Invalid row input or a singular continuum fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ContinuumFitError {
    /// Frequency and sample arrays were empty or had different lengths.
    #[error("continuum row frequencies and samples must be nonempty and equal-length")]
    InvalidShape,
    /// A frequency or visibility value was not finite.
    #[error("continuum row contains a non-finite frequency or visibility")]
    NonFiniteInput,
    /// The valid fit samples did not span the effective polynomial basis.
    #[error("continuum fit is singular for the effective polynomial order")]
    SingularFit,
}

/// Fit and subtract a CASA-compatible polynomial continuum from one correlation.
///
/// Frequencies are normalized over the complete supplied row domain with
/// `x = (frequency - midpoint) / (minimum - midpoint)`. Fit and application
/// roles are independent; predictions are returned for every channel while
/// callers decide which residual members enter line imaging.
pub fn fit_and_subtract_continuum(
    input: ContinuumRowInput<'_>,
) -> Result<ContinuumRowResult, ContinuumFitError> {
    if input.samples.is_empty() || input.samples.len() != input.frequencies_hz.len() {
        return Err(ContinuumFitError::InvalidShape);
    }
    if input.frequencies_hz.iter().any(|value| !value.is_finite())
        || input
            .samples
            .iter()
            .any(|sample| !sample.visibility.re.is_finite() || !sample.visibility.im.is_finite())
    {
        return Err(ContinuumFitError::NonFiniteInput);
    }

    let normalized = casa_normalized_frequency_axis(input.frequencies_hz);
    let valid_indices = input
        .samples
        .iter()
        .enumerate()
        .filter_map(|(index, sample)| {
            (sample.use_role.contributes_to_fit()
                && !sample.flag
                && sample.weight.is_finite()
                && sample.weight > 0.0)
                .then_some(index)
        })
        .collect::<Vec<_>>();
    if valid_indices.is_empty() {
        return Ok(ContinuumRowResult {
            samples: input.samples.to_vec(),
            normalized_frequencies: normalized,
            fit_sample_indices: valid_indices,
            coefficients: Vec::new(),
            prediction: vec![Complex64::new(0.0, 0.0); input.samples.len()],
            residual: input
                .samples
                .iter()
                .map(|sample| sample.visibility)
                .collect(),
            status: ContinuumFitStatus::NoValidFitSamples,
            chi_squared: [f64::INFINITY; 2],
        });
    }

    let effective_order = input
        .requested_order
        .min(u8::try_from(valid_indices.len() - 1).unwrap_or(u8::MAX));
    let coefficient_count = usize::from(effective_order) + 1;
    let rows = |component: fn(Complex64) -> f64| {
        valid_indices
            .iter()
            .map(|&index| {
                (
                    monomial_basis(normalized[index], coefficient_count),
                    component(input.samples[index].visibility),
                    input.samples[index].weight,
                )
            })
            .collect::<Vec<_>>()
    };
    let real = solve_weighted_least_squares(&rows(|value| value.re), coefficient_count)
        .ok_or(ContinuumFitError::SingularFit)?;
    let imaginary = solve_weighted_least_squares(&rows(|value| value.im), coefficient_count)
        .ok_or(ContinuumFitError::SingularFit)?;
    let coefficients = real
        .into_iter()
        .zip(imaginary)
        .map(|(real, imaginary)| Complex64::new(real, imaginary))
        .collect::<Vec<_>>();
    let prediction = normalized
        .iter()
        .map(|&x| evaluate_polynomial(&coefficients, x))
        .collect::<Vec<_>>();
    let residual = input
        .samples
        .iter()
        .zip(&prediction)
        .map(|(sample, model)| sample.visibility - model)
        .collect::<Vec<_>>();
    let mut chi_squared = [0.0_f64; 2];
    for &index in &valid_indices {
        chi_squared[0] += input.samples[index].weight * residual[index].re.powi(2);
        chi_squared[1] += input.samples[index].weight * residual[index].im.powi(2);
    }
    Ok(ContinuumRowResult {
        samples: input.samples.to_vec(),
        normalized_frequencies: normalized,
        fit_sample_indices: valid_indices,
        coefficients,
        prediction,
        residual,
        status: ContinuumFitStatus::Fitted { effective_order },
        chi_squared,
    })
}

fn casa_normalized_frequency_axis(frequencies_hz: &[f64]) -> Vec<f64> {
    let minimum = frequencies_hz
        .iter()
        .copied()
        .min_by(f64::total_cmp)
        .expect("validated nonempty frequencies");
    let maximum = frequencies_hz
        .iter()
        .copied()
        .max_by(f64::total_cmp)
        .expect("validated nonempty frequencies");
    let midpoint = 0.5 * (minimum + maximum);
    let denominator = minimum - midpoint;
    if denominator == 0.0 {
        vec![0.0; frequencies_hz.len()]
    } else {
        frequencies_hz
            .iter()
            .map(|frequency| (frequency - midpoint) / denominator)
            .collect()
    }
}

fn monomial_basis(x: f64, count: usize) -> Vec<f64> {
    let mut terms = Vec::with_capacity(count);
    let mut value = 1.0;
    for _ in 0..count {
        terms.push(value);
        value *= x;
    }
    terms
}

fn evaluate_polynomial(coefficients: &[Complex64], x: f64) -> Complex64 {
    coefficients
        .iter()
        .rev()
        .fold(Complex64::new(0.0, 0.0), |value, coefficient| {
            value * x + coefficient
        })
}
