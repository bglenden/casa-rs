// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage selected, convolved spectra with the common MeasurementSet writer.

use std::fs;
use std::path::Path;

use casa_core::ArrayValue;
use ndarray::{Array1, Array2, ArrayD, Axis};

use casa_tables::TableError;

use crate::MeasurementSet;
use crate::observation_io::creation::{Replacement, Stage};
use crate::selection::RowGrammar;

use super::baseline::SpectrumBaseline;
use super::channel_average::{AveragedStatistics, sigma_from_weight, weight_from_sigma};
use super::{MsTransformError, MsTransformReport, MsTransformRequest, MsTransformTaskOptions};

/// A single-dish visibility convolution and its noise propagation kernel.
pub trait SpectrumConvolution {
    /// Initialize kernels for the original channel counts of the selected windows.
    fn prepare(&mut self, channel_counts: &[usize]) -> Result<(), String>;

    /// Replace a selected visibility cell with its convolved spectrum.
    fn apply(&mut self, data: &mut ArrayValue, flags: &ArrayD<bool>) -> Result<(), String>;

    /// Coefficients used to propagate channel weights and errors.
    fn weight_kernel(&self) -> &[f32];
}

pub(super) enum SpectrumOperator<'a> {
    Convolution(&'a mut dyn SpectrumConvolution),
    Baseline(&'a mut dyn SpectrumBaseline),
}

pub(super) struct SpectralOperation<'a> {
    pub(super) kernel: SpectrumOperator<'a>,
    pub(super) reindex: bool,
}

pub(super) fn auto_correlations(
    input: &MeasurementSet,
    selector: Option<&str>,
) -> Result<String, MsTransformError> {
    let Some(selector) = selector.filter(|text| !text.is_empty()) else {
        return Ok("*&&&".into());
    };
    let ids = input
        .signed_antenna1_indices(selector)
        .map_err(|source| MsTransformError::Spectrum(source.to_string()))?;
    let rewritten = ids
        .iter()
        .map(|antenna| format!("{antenna}&&&"))
        .collect::<Vec<_>>()
        .join(";");
    if let Some(index) = rewritten.find('-') {
        return Err(MsTransformError::Spectrum(format!(
            "Antenna Expression: Parse error at or near '-'\n(near char. {index} in string \"{rewritten}\")"
        )));
    }
    if rewritten.is_empty() {
        return Ok("*&&&".into());
    }
    Ok(rewritten)
}

pub(super) fn statistics(
    weights: ArrayValue,
    sigmas: ArrayValue,
    flags: &ArrayD<bool>,
    kernel: &[f32],
    corrected: bool,
) -> Result<AveragedStatistics, TableError> {
    let input = if corrected { weights } else { sigmas };
    let ArrayValue::Float32(input) = input else {
        return Err(TableError::Schema(
            "Spectral weights must be Float32.".into(),
        ));
    };
    let &[correlations, channels] = flags.shape() else {
        return Err(TableError::Schema(
            "Spectral flags must have two axes.".into(),
        ));
    };
    let mut weight_spectrum = Array2::zeros((correlations, channels));
    let mut sigma_spectrum = Array2::zeros((correlations, channels));
    let mut weight = Array1::zeros(correlations);
    let mut sigma = Array1::zeros(correlations);
    let half = kernel.len() / 2;
    for (correlation, mask) in flags.axis_iter(Axis(0)).enumerate() {
        let incoming = input.index_axis(Axis(0), correlation);
        let values = incoming
            .iter()
            .map(|&value| {
                if corrected {
                    value
                } else {
                    weight_from_sigma(value)
                }
            })
            .collect::<Vec<_>>();
        let mut valid = 0;
        let mut weight_sum = 0.;
        let mut sigma_sum = 0.;
        for (channel, &original) in values.iter().enumerate() {
            let interior = half <= channel && channel < channels.saturating_sub(half);
            let (weighted, flag) = if interior {
                let from = channel - half;
                let mut inverse = 0.;
                let mut flagged = false;
                for (offset, &coefficient) in kernel.iter().enumerate() {
                    let value = values[from + offset];
                    if value > f32::MIN_POSITIVE {
                        inverse += coefficient * coefficient / value;
                        flagged |= mask[from + offset];
                    } else {
                        flagged = true;
                    }
                }
                (
                    if inverse > f32::MIN_POSITIVE {
                        inverse.recip()
                    } else {
                        inverse
                    },
                    flagged,
                )
            } else {
                (original, true)
            };
            let deviation = sigma_from_weight(weighted);
            weight_spectrum[[correlation, channel]] = weighted;
            sigma_spectrum[[correlation, channel]] = deviation;
            if !flag {
                weight_sum += weighted;
                sigma_sum += deviation;
                valid += 1;
            }
        }
        if valid == 0 {
            weight_sum = weight_spectrum.row(correlation).sum();
            sigma_sum = sigma_spectrum.row(correlation).sum();
            valid = channels;
        }
        weight[correlation] = weight_sum / valid as f32;
        sigma[correlation] = sigma_sum / valid as f32;
    }
    Ok(AveragedStatistics {
        weight: ArrayValue::Float32(weight.into_dyn()),
        sigma: ArrayValue::Float32(sigma.into_dyn()),
        weight_spectrum: ArrayValue::Float32(weight_spectrum.into_dyn()),
        sigma_spectrum: ArrayValue::Float32(sigma_spectrum.into_dyn()),
    })
}

/// Convolve the selected autocorrelation spectra and publish a complete MS.
///
/// An existing output is held for a recoverable exchange, leaving it readable
/// if the selection or convolution fails.
pub fn convolve_spectra(
    request: &MsTransformRequest,
    options: &MsTransformTaskOptions,
    reindex: bool,
    overwrite: bool,
    kernel: &mut dyn SpectrumConvolution,
) -> Result<MsTransformReport, MsTransformError> {
    stage_spectra(
        request,
        options,
        SpectralOperation {
            kernel: SpectrumOperator::Convolution(kernel),
            reindex,
        },
        overwrite,
    )
}

/// Fit or apply selected single-dish baselines using the staged MS writer.
///
/// The caller owns the private output path and publishes it with any
/// companion baseline table only after both products have been written.
pub fn transform_baseline_spectra(
    request: &MsTransformRequest,
    options: &MsTransformTaskOptions,
    reindex: bool,
    baseline: &mut dyn SpectrumBaseline,
) -> Result<MsTransformReport, MsTransformError> {
    super::transform_product(
        request,
        RowGrammar::Transform,
        options,
        None,
        Some(SpectralOperation {
            kernel: SpectrumOperator::Baseline(baseline),
            reindex,
        }),
    )
}

fn stage_spectra(
    request: &MsTransformRequest,
    options: &MsTransformTaskOptions,
    operation: SpectralOperation<'_>,
    overwrite: bool,
) -> Result<MsTransformReport, MsTransformError> {
    let destination = &request.output_ms;
    let path_error = |reason: String| MsTransformError::PrepareOutput {
        path: destination.display().to_string(),
        reason,
    };
    let name = destination
        .file_name()
        .ok_or_else(|| path_error("The output path must end in a name.".into()))?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).map_err(|error| path_error(error.to_string()))?;
    let final_path = parent.join(name);
    let input =
        fs::canonicalize(&request.input_ms).map_err(|error| path_error(error.to_string()))?;
    let existing = final_path.symlink_metadata().is_ok();
    let output = fs::canonicalize(&final_path).unwrap_or_else(|_| final_path.clone());
    if input.starts_with(&output) || output.starts_with(&input) {
        return Err(MsTransformError::SameInputOutput {
            path: destination.display().to_string(),
        });
    }
    if existing && !overwrite {
        return Err(MsTransformError::OutputExists {
            path: destination.display().to_string(),
        });
    }
    let replacement = existing
        .then(|| Replacement::hold(&final_path))
        .transpose()
        .map_err(|error| path_error(error.to_string()))?;
    let stage = Stage::reserve(&final_path, "casa-rs-spectral")
        .map_err(|error| path_error(error.to_string()))?;
    let mut physical = request.clone();
    physical.output_ms = stage.path().to_path_buf();
    let product = super::transform_product(
        &physical,
        RowGrammar::Transform,
        options,
        None,
        Some(operation),
    );
    let commit = product.and_then(|mut report| {
        if let Some(replacement) = replacement {
            replacement
                .publish(stage.path())
                .map_err(|error| path_error(error.to_string()))?;
        } else {
            stage
                .publish_new(&final_path)
                .map_err(|error| path_error(error.to_string()))?;
        }
        report.output_ms = destination.clone();
        Ok(report)
    });
    if commit.is_ok() {
        stage.release();
    } else {
        stage.remove();
    }
    commit
}
