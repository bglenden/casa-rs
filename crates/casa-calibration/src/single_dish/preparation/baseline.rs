// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fit single-dish baselines and apply saved spectra models.

mod auto_mask;
mod diagnostics;
mod linear_fit;
mod polynomial;
mod sinusoid;
mod spline;
mod table;
pub(crate) mod task;
mod variable;

use std::collections::{BTreeMap, btree_map::Entry};
use std::path::Path;

use casa_ms::MeasurementSet;
use casa_ms::transform::{
    MsTransformReport, MsTransformRequest, MsTransformTiling, SpectrumBaseline, SpectrumRow,
    SpectrumWeights, TransformTaskHistory, transform_baseline_spectra,
};
use casa_types::{ArrayValue, Complex32, RecordValue};
use ndarray::{Array1, ArrayD, Axis};

pub use auto_mask::SingleDishLineFinder;
use diagnostics::Diagnostics;
use linear_fit::Fit;
use polynomial::{Polynomial, PolynomialFit, PreparedPolynomial};
pub use sinusoid::SingleDishSinusoid;
use table::{AppliedBaseline, BaselineRow, BaselineTable, SpectrumModel, mask_ranges};
use variable::{StoredLineFinder, VariableParameters};

use super::{SingleDishSession, data_column};
use crate::output::CalibrationOutput;

/// The model basis for a fixed-family baseline fit.
pub enum SingleDishBaselineBasis {
    /// Polynomial or Chebyshev basis of the specified order.
    Polynomial {
        /// Degree of the highest term.
        order: usize,
        /// Use Chebyshev polynomials of the first kind.
        chebyshev: bool,
    },
    /// Continuous cubic spline with the specified number of pieces.
    CubicSpline {
        /// Number of spline pieces.
        pieces: usize,
    },
    /// Fourier components at fixed or FFT-selected waves.
    Sinusoid(SingleDishSinusoid),
}

impl SingleDishBaselineBasis {
    fn family(&self) -> BaselineFamily {
        match self {
            Self::Polynomial {
                chebyshev: false, ..
            } => BaselineFamily::Polynomial(Polynomial::Monomial),
            Self::Polynomial {
                chebyshev: true, ..
            } => BaselineFamily::Polynomial(Polynomial::Chebyshev),
            Self::CubicSpline { .. } => BaselineFamily::CubicSpline,
            Self::Sinusoid(_) => BaselineFamily::Sinusoid,
        }
    }
}

/// Fit or apply a single-dish baseline.
pub enum SingleDishBaselineMode<'a> {
    /// Estimate coefficients and optionally publish a baseline table.
    Fit {
        /// Model basis and its parameters.
        basis: SingleDishBaselineBasis,
        /// Sigma threshold for clipping residuals.
        clip_sigma: f64,
        /// Maximum number of fits, including the initial fit.
        max_fits: usize,
        /// Detect and exclude line channels before fitting.
        line_finder: Option<SingleDishLineFinder>,
        /// Optional CASA baseline-table destination.
        table: Option<&'a Path>,
        /// Optional coefficient and residual reports.
        diagnostics: SingleDishBaselineDiagnostics<'a>,
    },
    /// Use a baseline-parameter file keyed by selected row and polarization.
    Variable {
        /// File containing per-spectrum fit parameters.
        parameters: &'a Path,
        /// Log the parameters used for each eligible spectrum.
        verbose: bool,
        /// Optional CASA baseline-table destination.
        table: Option<&'a Path>,
        /// Optional coefficient and residual reports.
        diagnostics: SingleDishBaselineDiagnostics<'a>,
    },
    /// Apply saved CASA coefficients to matching integrations.
    Apply {
        /// An existing compatible baseline table.
        table: &'a Path,
    },
}

/// Optional destinations for the CASA baseline reports.
#[derive(Default)]
pub struct SingleDishBaselineDiagnostics<'a> {
    /// CSV file.
    pub csv: Option<&'a Path>,
    /// Human-readable report.
    pub text: Option<&'a Path>,
    /// Public task heading for the text report. Direct tool calls append report rows.
    pub heading: Option<&'a str>,
}

/// Selected baseline operation and its publication settings.
pub struct SingleDishBaseline<'a> {
    /// Fit a new baseline or apply a saved one.
    pub mode: SingleDishBaselineMode<'a>,
    /// Input visibility column.
    pub datacolumn: &'a str,
    /// Channel mask for fitting and noise calculation.
    pub mask: &'a str,
    /// Write a MeasurementSet when supplied.
    pub output: Option<&'a Path>,
    /// Populate the output MS with corrected spectra. A model-only tool
    /// operation can still request the empty MS structure.
    pub subtract: bool,
    /// Replace existing products after successful processing.
    pub overwrite: bool,
    /// Derive WEIGHT from the residuals.
    pub update_weight: bool,
    /// Use RMS or standard deviation for the resulting WEIGHT.
    pub sigma_value: &'a str,
    /// Task provenance. The direct tool leaves HISTORY unchanged.
    pub history: Option<TransformTaskHistory>,
}

impl SingleDishSession {
    /// Fit or apply baselines for the selected spectra.
    pub fn baseline(&self, request: SingleDishBaseline<'_>) -> Result<MsTransformReport, String> {
        let path = self.name()?;
        let input = MeasurementSet::open(path).map_err(error_text)?;
        let mask = masks(&input, request.mask)?;
        let noise = request
            .update_weight
            .then_some(if request.sigma_value == "stddev" {
                Noise::StandardDeviation
            } else {
                Noise::Rms
            });
        let destinations = match &request.mode {
            SingleDishBaselineMode::Fit {
                table, diagnostics, ..
            }
            | SingleDishBaselineMode::Variable {
                table, diagnostics, ..
            } => [request.output, *table, diagnostics.csv, diagnostics.text],
            SingleDishBaselineMode::Apply { .. } => [request.output, None, None, None],
        };
        validate_product_paths(path, &destinations)?;
        let (operation, table_path) = match request.mode {
            SingleDishBaselineMode::Fit {
                basis,
                clip_sigma,
                max_fits,
                line_finder,
                table,
                diagnostics,
            } => {
                let settings = FitSettings {
                    basis,
                    clip_sigma,
                    max_fits,
                };
                settings.validate()?;
                (
                    Operation::Fit(Box::new(FitOperation::new(
                        FitRequest::Fixed {
                            settings,
                            line_finder,
                        },
                        table.is_some(),
                        Diagnostics::prepare(diagnostics, request.overwrite)?,
                    ))),
                    table,
                )
            }
            SingleDishBaselineMode::Variable {
                parameters,
                verbose,
                table,
                diagnostics,
            } => {
                for destination in destinations.into_iter().flatten() {
                    if crate::paths::overlap(parameters, destination).map_err(error_text)? {
                        return Err(
                            "An output product overlaps the baseline parameter file.".into()
                        );
                    }
                }
                let parameters = VariableParameters::open(parameters)?;
                (
                    Operation::Fit(Box::new(FitOperation::new(
                        FitRequest::Variable {
                            input: &input,
                            parameters,
                            verbose,
                        },
                        table.is_some(),
                        Diagnostics::prepare(diagnostics, request.overwrite)?,
                    ))),
                    table,
                )
            }
            SingleDishBaselineMode::Apply { table } => {
                if table.as_os_str().is_empty() {
                    return Err("baseline table is not given.".into());
                }
                if let Some(output) = request.output
                    && crate::paths::overlap(table, output).map_err(error_text)?
                {
                    return Err(
                        "The output MeasurementSet overlaps the input baseline table.".into(),
                    );
                }
                (
                    Operation::Apply(Box::new(BaselineTable::open(table)?)),
                    None,
                )
            }
        };
        let output = request
            .output
            .map(|destination| {
                CalibrationOutput::prepare_measurement_set(destination, request.overwrite)
                    .map_err(error_text)
            })
            .transpose()?;
        let table = table_path
            .map(|destination| {
                CalibrationOutput::prepare_baseline_table(destination, request.overwrite)
                    .map_err(error_text)
            })
            .transpose()?;
        let scratch = output
            .is_none()
            .then(|| tempfile::tempdir().map_err(error_text))
            .transpose()?;
        let physical = match (&output, &scratch) {
            (Some(output), _) => output.staging.clone(),
            (_, Some(scratch)) => scratch.path().join("baseline.ms"),
            _ => return Err("No baseline staging path.".into()),
        };
        let mut worker = BaselineWorker {
            operation,
            masks: mask,
            noise,
            omit_spectral_weights: request.update_weight && request.history.is_some(),
            subtract: request.subtract,
        };
        let mut options = self.line_averaging.operation_options()?;
        options.history = request.history;
        options.tiling = Some(MsTransformTiling::Standard);
        let mut report = transform_baseline_spectra(
            &MsTransformRequest {
                input_ms: path.to_path_buf(),
                output_ms: physical,
                spw: self.selection.spw.clone().unwrap_or_default(),
                width: 1,
                data_column: data_column(request.datacolumn)?,
                selection: self.selection.clone(),
                keep_flags: true,
            },
            &options,
            self.reindex.unwrap_or(true),
            &mut worker,
        )
        .map_err(error_text)?;
        if let Operation::Fit(fitting) = worker.operation {
            if let Some(table) = &table {
                table::write(&table.staging, &input, fitting.records)?;
            }
            fitting.diagnostics.publish()?;
        }
        if let Some(table) = table {
            table.commit().map_err(error_text)?;
        }
        if let Some(output) = output {
            output.commit().map_err(error_text)?;
            report.output_ms = output.destination.clone();
        } else {
            report.output_ms.clear();
        }
        Ok(report)
    }
}

fn validate_product_paths(input: &Path, destinations: &[Option<&Path>]) -> Result<(), String> {
    for (index, destination) in destinations
        .iter()
        .enumerate()
        .filter_map(|(i, path)| path.map(|path| (i, path)))
    {
        if crate::paths::overlap(input, destination).map_err(error_text)? {
            return Err("An output product overlaps the input MeasurementSet.".into());
        }
        for other in destinations[..index].iter().flatten() {
            if crate::paths::overlap(other, destination).map_err(error_text)? {
                return Err("Baseline output products overlap.".into());
            }
        }
    }
    Ok(())
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[derive(Clone, Copy)]
enum Noise {
    Rms,
    StandardDeviation,
}

#[derive(Clone, Copy)]
enum BaselineFamily {
    Polynomial(Polynomial),
    CubicSpline,
    Sinusoid,
}

impl BaselineFamily {
    fn invalid_fit_argument(self) -> &'static str {
        match self {
            Self::Polynomial(_) => polynomial::INVALID_FIT_ARGUMENT,
            Self::CubicSpline => spline::INVALID_FIT_ARGUMENT,
            Self::Sinusoid => sinusoid::INVALID_FIT,
        }
    }
}

enum ModelBasis {
    Polynomial(Polynomial, usize),
    Spline {
        pieces: usize,
        boundaries: Vec<usize>,
    },
    Sinusoid(Vec<usize>),
}

impl ModelBasis {
    fn family(&self) -> BaselineFamily {
        match self {
            Self::Polynomial(polynomial, _) => BaselineFamily::Polynomial(*polynomial),
            Self::Spline { .. } => BaselineFamily::CubicSpline,
            Self::Sinusoid(_) => BaselineFamily::Sinusoid,
        }
    }

    fn subtract(&self, spectrum: &mut [f32], coefficients: &[f64]) -> Result<(), String> {
        match self {
            Self::Polynomial(polynomial, _) => {
                polynomial.subtract(spectrum, coefficients);
                Ok(())
            }
            Self::Spline { boundaries, .. } => spline::subtract(spectrum, coefficients, boundaries),
            Self::Sinusoid(waves) => sinusoid::subtract(spectrum, coefficients, waves),
        }
    }
}

struct FitSettings {
    basis: SingleDishBaselineBasis,
    clip_sigma: f64,
    max_fits: usize,
}

impl FitSettings {
    fn validate(&self) -> Result<(), String> {
        if self.max_fits == 0 {
            return Err("Number of fits must be positive.".into());
        }
        if matches!(
            self.basis,
            SingleDishBaselineBasis::CubicSpline { pieces: 0 }
        ) {
            return Err("npiece must be positive.".into());
        }
        if self.clip_sigma.is_nan() || self.clip_sigma <= 0. {
            return Err(self.basis.family().invalid_fit_argument().into());
        }
        Ok(())
    }
}

enum FitRequest<'a> {
    Fixed {
        settings: FitSettings,
        line_finder: Option<SingleDishLineFinder>,
    },
    Variable {
        input: &'a MeasurementSet,
        parameters: VariableParameters,
        verbose: bool,
    },
}

impl FitRequest<'_> {
    fn fixed_family(&self) -> Option<BaselineFamily> {
        match self {
            Self::Fixed { settings, .. } => Some(settings.basis.family()),
            Self::Variable { .. } => None,
        }
    }
}

struct FitOperation<'a> {
    request: FitRequest<'a>,
    bases: BTreeMap<(usize, bool, usize), PolynomialFit>,
    polynomial_design: Option<PreparedPolynomial>,
    records: Vec<RecordValue>,
    record_models: bool,
    diagnostics: Diagnostics,
}

enum Operation<'a> {
    Fit(Box<FitOperation<'a>>),
    Apply(Box<BaselineTable>),
}

struct BaselineWorker<'a> {
    operation: Operation<'a>,
    masks: WindowMasks,
    noise: Option<Noise>,
    omit_spectral_weights: bool,
    subtract: bool,
}

struct SpectrumFit {
    result: Fit,
    basis: ModelBasis,
    clip_threshold: f32,
    clip_iterations: u32,
    line_finder: StoredLineFinder,
}

impl<'a> FitOperation<'a> {
    fn new(request: FitRequest<'a>, record_models: bool, diagnostics: Diagnostics) -> Self {
        Self {
            request,
            bases: BTreeMap::new(),
            polynomial_design: None,
            records: Vec::new(),
            record_models,
            diagnostics,
        }
    }

    fn fit(
        &mut self,
        row: &SpectrumRow,
        receptor: usize,
        values: &[f32],
        mask: &mut [bool],
        has_unflagged_data: bool,
    ) -> Result<Option<SpectrumFit>, String> {
        let (settings, stored_line_finder) = match &self.request {
            FitRequest::Fixed {
                settings,
                line_finder,
            } => {
                if let Some(finder) = line_finder
                    && has_unflagged_data
                {
                    finder.exclude_lines(values, mask)?;
                }
                (settings, StoredLineFinder::default())
            }
            FitRequest::Variable {
                input,
                parameters,
                verbose,
            } => {
                let Some(entry) = parameters
                    .get(row.selection_row, receptor)
                    .filter(|_| has_unflagged_data)
                else {
                    return Ok(None);
                };
                if *verbose {
                    tracing::info!(target: "sdbaseline", row = row.selection_row, pol = receptor,
                        nchan = values.len(), mask = %entry.mask,
                        clip_iterations = entry.settings.max_fits,
                        clip_threshold = entry.settings.clip_sigma,
                        line_finder = ?entry.line_finder,
                        baseline = %entry.describe_basis(),
                        "Fitting Parameter");
                }
                parameters.validate_contexts(values.len())?;
                if !entry.mask.is_empty() {
                    let selector = format!("{}:{}", row.window, entry.mask);
                    let masks = masks(input, &selector)?;
                    let (local, _) = masks.for_window(row.window);
                    if let Some(local) = local {
                        for (channel, include) in mask.iter_mut().enumerate() {
                            *include &= local.get(channel).copied().unwrap_or(false);
                        }
                    }
                }
                if !mask.iter().any(|&included| included) {
                    return Ok(None);
                }
                entry.settings.validate()?;
                // The variable file's LF columns are recorded by CASA.
                (&entry.settings, entry.line_finder)
            }
        };
        let FitSettings {
            basis,
            clip_sigma,
            max_fits,
        } = settings;
        let (clip_sigma, max_fits) = (*clip_sigma, *max_fits);
        let fitted = match basis {
            SingleDishBaselineBasis::Polynomial { order, chebyshev } => {
                let family = if *chebyshev {
                    Polynomial::Chebyshev
                } else {
                    Polynomial::Monomial
                };
                let basis = match self.bases.entry((values.len(), *chebyshev, *order)) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let fit = PolynomialFit::new(family, *order, values.len())
                            .ok_or("sakura_CreateLSQFitContextFloat() failed -- InvalidArgument")?;
                        entry.insert(fit)
                    }
                };
                basis
                    .fit(
                        values,
                        mask,
                        clip_sigma,
                        max_fits,
                        &mut self.polynomial_design,
                    )?
                    .map(|fit| (fit, ModelBasis::Polynomial(family, *order)))
            }
            SingleDishBaselineBasis::CubicSpline { pieces } => {
                spline::fit(values, mask, *pieces, clip_sigma, max_fits)?.map(|mut fit| {
                    let boundaries = std::mem::take(&mut fit.boundaries);
                    (
                        fit,
                        ModelBasis::Spline {
                            pieces: *pieces,
                            boundaries,
                        },
                    )
                })
            }
            SingleDishBaselineBasis::Sinusoid(sinusoid) => sinusoid
                .fit(values, mask, clip_sigma, max_fits, has_unflagged_data)?
                .map(|(fit, waves)| (fit, ModelBasis::Sinusoid(waves))),
        };
        Ok(fitted.map(|(result, basis)| SpectrumFit {
            result,
            basis,
            clip_threshold: clip_sigma as f32,
            clip_iterations: max_fits.saturating_sub(1) as u32,
            line_finder: stored_line_finder,
        }))
    }
}

impl SpectrumBaseline for BaselineWorker<'_> {
    fn writes_rows(&self) -> bool {
        self.subtract
    }

    fn updates_weight(&self) -> bool {
        self.noise.is_some()
    }

    fn omit_spectral_weights(&self) -> bool {
        self.omit_spectral_weights
    }

    fn process(
        &mut self,
        row: &SpectrumRow,
        data: &mut ArrayValue,
        flags: &mut ArrayD<bool>,
        weights: SpectrumWeights<'_>,
    ) -> Result<Option<ArrayValue>, String> {
        let &[receptors, channels] = flags.shape() else {
            return Err("Expected a two-dimensional single-dish FLAG cell.".into());
        };
        if data.shape() != flags.shape() {
            return Err("Single-dish data and FLAG shapes disagree.".into());
        }
        let mut weights = self.noise.map(|_| {
            if let Some(ArrayValue::Float32(weights)) = weights.scalar {
                weights.iter().copied().collect::<Vec<_>>()
            } else {
                vec![1.; receptors]
            }
        });
        let mut models = Vec::new();
        let ids = row_ids(row)?;
        let (fit_mask, fallback) = self.masks.for_window(row.window);
        if fallback && fit_mask.is_some_and(|mask| mask.len() != channels) {
            if let Operation::Fit(fitting) = &self.operation {
                let error = fitting.request.fixed_family().map_or(
                    "The input mask does not match the spectrum channel count.",
                    BaselineFamily::invalid_fit_argument,
                );
                return Err(error.into());
            }
            if self.noise.is_some() {
                return Err("The input mask does not match the spectrum channel count.".into());
            }
        }
        let matching = if let Operation::Apply(table) = &self.operation {
            table.matching_row(
                ids.window,
                ids.antenna,
                ids.beam,
                ids.scan,
                row.time,
                row.interval,
            )
        } else {
            None
        };
        for receptor in 0..receptors {
            let mut values = real_spectrum(data, receptor)?;
            let mut flags = flags.index_axis_mut(Axis(0), receptor);
            let mut mask = (0..channels)
                .map(|channel| {
                    !flags[channel]
                        && fit_mask.is_none_or(|mask| mask.get(channel).copied().unwrap_or(false))
                })
                .collect::<Vec<_>>();
            let mut noise_mask = None;
            match &mut self.operation {
                Operation::Fit(fitting) => {
                    let has_unflagged_data = flags.iter().any(|&flag| !flag);
                    let fit = fitting.fit(row, receptor, &values, &mut mask, has_unflagged_data)?;
                    if let Some(SpectrumFit {
                        result:
                            Fit {
                                coefficients,
                                residuals,
                                included,
                                rms,
                                ..
                            },
                        basis,
                        clip_threshold,
                        clip_iterations,
                        line_finder,
                    }) = fit
                    {
                        values = residuals;
                        if fitting.record_models || fitting.diagnostics.enabled() {
                            let clipped = mask
                                .iter()
                                .zip(&included)
                                .filter(|(before, after)| **before && !**after)
                                .count();
                            models.push(Some(SpectrumModel {
                                basis,
                                coefficients,
                                mask: mask_ranges(&included),
                                rms,
                                clipped,
                                clip_threshold,
                                clip_iterations,
                                line_finder,
                            }));
                        }
                        // Variable baselines derive weights from their original fit mask,
                        // including residuals excluded by the clipping iterations.
                        noise_mask = Some(if fitting.request.fixed_family().is_none() {
                            mask
                        } else {
                            included
                        });
                    } else {
                        if fitting.record_models || fitting.diagnostics.enabled() {
                            models.push(None);
                        }
                        flags.fill(true);
                    }
                }
                Operation::Apply(table) => {
                    let model = matching
                        .map(|index| table.model(index, receptor))
                        .transpose()?
                        .flatten();
                    if let Some(AppliedBaseline {
                        basis,
                        coefficients,
                        included,
                    }) = model
                    {
                        if included.len() != channels {
                            return Err(
                                "Baseline table channel count differs from its spectrum.".into()
                            );
                        }
                        if flags.iter().any(|&flag| !flag) {
                            basis.subtract(&mut values, &coefficients)?;
                            noise_mask = Some(
                                mask.into_iter()
                                    .zip(included)
                                    .map(|(left, right)| left && right)
                                    .collect(),
                            );
                        }
                    } else {
                        flags.fill(true);
                    }
                }
            }
            if let (Some(noise), Some(weights), Some(mask)) = (self.noise, &mut weights, noise_mask)
            {
                let weight = weights
                    .get_mut(receptor)
                    .ok_or("Baseline weight and correlation axes disagree.")?;
                *weight = noise.weight(&values, &mask);
            }
            put_real_spectrum(data, receptor, values);
        }
        if let Operation::Fit(fitting) = &mut self.operation
            && models.iter().any(Option::is_some)
        {
            let row = BaselineRow {
                channels,
                models,
                ..ids
            };
            let fixed_family = fitting.request.fixed_family();
            if fitting.diagnostics.enabled() {
                fitting.diagnostics.report(&row, fixed_family.is_none())?;
            }
            if fitting.record_models {
                fitting.records.push(row.record(fixed_family));
            }
        }
        Ok(weights.map(|weights| ArrayValue::Float32(Array1::from_vec(weights).into_dyn())))
    }
}

fn row_ids(row: &SpectrumRow) -> Result<BaselineRow, String> {
    let id = |value| u32::try_from(value).map_err(|_| "Negative baseline row identifier.");
    Ok(BaselineRow {
        scan: id(row.scan)?,
        beam: id(row.feed)?,
        antenna: id(row.antenna)?,
        window: id(row.window)?,
        time: row.time,
        channels: 0,
        models: Vec::new(),
    })
}

fn real_spectrum(data: &ArrayValue, receptor: usize) -> Result<Vec<f32>, String> {
    match data {
        ArrayValue::Float32(values) => Ok(values
            .index_axis(Axis(0), receptor)
            .iter()
            .copied()
            .collect()),
        ArrayValue::Complex32(values) => Ok(values
            .index_axis(Axis(0), receptor)
            .iter()
            .map(|value| value.re)
            .collect()),
        _ => Err("Unexpected single-dish spectrum type.".into()),
    }
}

fn put_real_spectrum(data: &mut ArrayValue, receptor: usize, values: Vec<f32>) {
    match data {
        ArrayValue::Float32(data) => {
            for (output, value) in data
                .index_axis_mut(Axis(0), receptor)
                .iter_mut()
                .zip(values)
            {
                *output = value;
            }
        }
        ArrayValue::Complex32(data) => {
            for (output, value) in data
                .index_axis_mut(Axis(0), receptor)
                .iter_mut()
                .zip(values)
            {
                *output = Complex32::new(value, 0.);
            }
        }
        _ => unreachable!("real spectrum was already read"),
    }
}

impl Noise {
    fn weight(self, spectrum: &[f32], mask: &[bool]) -> f32 {
        let mut count = 0;
        let mut sum = 0.;
        let mut sum_squared = 0.;
        for (&sample, &included) in spectrum.iter().zip(mask) {
            if included {
                count += 1;
                sum += f64::from(sample);
                sum_squared += f64::from(sample * sample);
            }
        }
        if count == 0 {
            return 0.;
        }
        let fraction = f64::from(count).recip();
        let mean_squared = sum_squared * fraction;
        let variance = match self {
            Self::Rms => mean_squared,
            Self::StandardDeviation => mean_squared - (sum * fraction).powi(2),
        };
        if variance > 0. {
            variance.recip() as f32
        } else {
            0.
        }
    }
}

struct WindowMasks {
    windows: BTreeMap<i32, Vec<bool>>,
    first: Option<i32>,
}

impl WindowMasks {
    fn for_window(&self, window: i32) -> (Option<&[bool]>, bool) {
        if let Some(mask) = self.windows.get(&window) {
            return (Some(mask), false);
        }
        let mask = self.first.and_then(|first| self.windows.get(&first));
        (mask.map(Vec::as_slice), mask.is_some())
    }
}

fn masks(input: &MeasurementSet, selector: &str) -> Result<WindowMasks, String> {
    let selection = input
        .resolve_spw_selector(if selector.is_empty() { "*" } else { selector })
        .map_err(|error| error.to_string())?;
    let metadata = input.spectral_window().map_err(|error| error.to_string())?;
    let mut masks = BTreeMap::new();
    for &window in selection.spw_ids() {
        let count = metadata.num_chan(window).map_err(error_text)? as usize;
        let channels = selection.channels_of(window).map_err(error_text)?;
        let mut mask = vec![channels.is_none(); count];
        if let Some(channels) = channels {
            for channel in channels {
                if let Some(included) = mask.get_mut(channel) {
                    *included = true;
                }
            }
        }
        masks.insert(window as i32, mask);
    }
    Ok(WindowMasks {
        windows: masks,
        first: selection.spw_ids().first().map(|&window| window as i32),
    })
}
