// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-integration, per-correlation continuum fits on the selected input grid.

use std::collections::BTreeMap;

use casa_core::{ArrayValue, Complex32};
use casa_numerics::solve_weighted_least_squares;
use ndarray::{Array1, ArrayD, Ix2, s};

use super::{MainArrayBlock, MsTransformError, TransformChannelSelection, VisibilityColumns};
use crate::{MeasurementSet, OptionalMainColumn};

/// Weighted polynomial fitting for the public mstransform continuum stage.
#[derive(Debug, Clone)]
pub struct ContinuumFitOptions {
    /// Input SPWs and line-free channels. Empty fits all selected channels.
    pub fit_spw: String,
    /// Desired polynomial order, reduced when there are fewer usable channels.
    pub order: usize,
    /// Write the fitted continuum in place of the residuals.
    pub want_continuum: bool,
    /// Use CASA's iterative reweighting mode. The legacy fit uses one pass.
    pub reweight: bool,
    /// Number of least-squares passes when iterative reweighting is active.
    pub iterations: usize,
    /// Maximum number of distinct rows fitted concurrently.
    pub threads: usize,
}

pub(super) struct ContinuumFit {
    windows: BTreeMap<i32, FitWindow>,
    options: ContinuumFitOptions,
}

struct FitWindow {
    channels: Vec<usize>,
    frequencies: Vec<f64>,
    fit_channels: Vec<bool>,
}

impl ContinuumFit {
    pub(super) fn new(
        ms: &MeasurementSet,
        selected: &BTreeMap<i32, TransformChannelSelection>,
        options: &ContinuumFitOptions,
    ) -> Result<Self, MsTransformError> {
        let spec = options.fit_spw.trim();
        let mut windows = BTreeMap::new();
        if spec == "NONE" {
            return Ok(Self {
                windows,
                options: options.clone(),
            });
        }
        let fit_channels = (!spec.is_empty())
            .then(|| {
                let spws = ms.resolve_spw_selector(spec).map_err(|source| {
                    MsTransformError::Spectrum(format!("invalid fitspw {spec:?}: {source}"))
                })?;
                super::resolve_transform_channels(ms, &spws, spec).map_err(|source| {
                    MsTransformError::Spectrum(format!("invalid fitspw {spec:?}: {source}"))
                })
            })
            .transpose()?;
        let spectral_windows = ms.spectral_window().map_err(spectrum_error)?;
        for (&id, window) in selected {
            let admissible = if let Some(fit_channels) = &fit_channels {
                let Some(fit) = fit_channels.get(&id) else {
                    continue;
                };
                Some(
                    fit.channels
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>(),
                )
            } else {
                None
            };
            let full_frequencies = spectral_windows
                .chan_freq(id as usize)
                .map_err(spectrum_error)?;
            let frequencies = window
                .channels
                .iter()
                .map(|&channel| full_frequencies.get(channel).copied())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    MsTransformError::Spectrum(format!("SPW {id} has a truncated CHAN_FREQ array"))
                })?;
            let (minimum, maximum) = frequencies.iter().copied().fold(
                (f64::INFINITY, f64::NEG_INFINITY),
                |(low, high), frequency| (low.min(frequency), high.max(frequency)),
            );
            let midpoint = (minimum + maximum) * 0.5;
            let frequencies = frequencies
                .into_iter()
                .map(|frequency| {
                    if minimum == maximum {
                        0.0
                    } else {
                        (frequency - midpoint) / (minimum - midpoint)
                    }
                })
                .collect();
            windows.insert(
                id,
                FitWindow {
                    channels: window.channels.clone(),
                    frequencies,
                    fit_channels: window
                        .channels
                        .iter()
                        .map(|channel| admissible.as_ref().is_none_or(|fit| fit.contains(channel)))
                        .collect(),
                },
            );
        }
        Ok(Self {
            windows,
            options: options.clone(),
        })
    }

    pub(super) fn working_bytes_per_sample(&self) -> usize {
        let terms = self
            .windows
            .values()
            .map(|window| window.channels.len())
            .max()
            .unwrap_or(0)
            .min(self.options.order.saturating_add(1));
        // Per-channel bases, weighted design, SVD factors, and row vectors
        // can coexist. The SVD's square factor has at most this many terms.
        if terms == 0 {
            0
        } else {
            64_usize.saturating_add(48_usize.saturating_mul(terms))
        }
    }

    pub(super) fn apply(
        &self,
        block: &mut MainArrayBlock,
        visibility: &VisibilityColumns,
        spws: impl Iterator<Item = i32>,
        spectrum_names: &[&str],
        projected: bool,
    ) -> Result<(), MsTransformError> {
        if self.windows.is_empty() {
            return Ok(());
        }
        let windows = spws.map(|id| self.windows.get(&id)).collect::<Vec<_>>();
        let workers = self
            .options
            .threads
            .max(1)
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        for (copy, cells) in visibility.copies().iter().zip(&mut block.visibility) {
            if copy.source == OptionalMainColumn::LagData {
                continue;
            }
            let sigma = copy.source == OptionalMainColumn::Data;
            let (name, scalars) = if sigma {
                ("SIGMA_SPECTRUM", &block.sigmas)
            } else {
                ("WEIGHT_SPECTRUM", &block.weights)
            };
            let spectra = spectrum_names
                .iter()
                .position(|&candidate| candidate == name)
                .map(|slot| &block.spectra[slot]);
            let flags = &block.flags;
            let fit = |start: usize, cells: &mut [Option<ArrayValue>]| {
                for (offset, cell) in cells.iter_mut().enumerate() {
                    let row = start + offset;
                    if let (Some(window), Some(values), Some(flags)) =
                        (windows[row], cell, &flags[row])
                    {
                        let noise = Noise {
                            spectrum: spectra.and_then(|cells| cells[row].as_ref()),
                            scalar: scalars[row].as_ref(),
                            sigma,
                        };
                        window.apply(values, flags, noise, &self.options, projected)?;
                    }
                }
                Ok(())
            };
            if workers == 1 || cells.len() < 2 {
                fit(0, cells)?;
            } else {
                let size = cells.len().div_ceil(workers);
                std::thread::scope(|scope| {
                    let handles = cells
                        .chunks_mut(size)
                        .enumerate()
                        .map(|(index, cells)| scope.spawn(move || fit(index * size, cells)))
                        .collect::<Vec<_>>();
                    for handle in handles {
                        handle.join().map_err(|_| {
                            MsTransformError::Spectrum("continuum fit worker panicked".into())
                        })??;
                    }
                    Ok::<_, MsTransformError>(())
                })?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Noise<'a> {
    spectrum: Option<&'a ArrayValue>,
    scalar: Option<&'a ArrayValue>,
    sigma: bool,
}

impl Noise<'_> {
    fn weight(self, correlation: usize, channel: usize) -> f64 {
        let value = match self.spectrum {
            Some(ArrayValue::Float32(spectrum)) => spectrum.get([correlation, channel]),
            _ => match self.scalar {
                Some(ArrayValue::Float32(scalar)) => scalar.get([correlation]),
                _ => None,
            },
        }
        .copied()
        .unwrap_or(0.0);
        if self.sigma && value > 0.0 {
            f64::from(1.0 / (value * value))
        } else if !self.sigma {
            f64::from(value)
        } else {
            0.0
        }
    }
}

impl FitWindow {
    fn apply(
        &self,
        visibility: &mut ArrayValue,
        flags: &ArrayValue,
        noise: Noise<'_>,
        options: &ContinuumFitOptions,
        projected: bool,
    ) -> Result<(), MsTransformError> {
        let ArrayValue::Bool(flags) = flags else {
            return Err(MsTransformError::Spectrum(
                "continuum fitting requires a boolean FLAG array".into(),
            ));
        };
        match visibility {
            ArrayValue::Complex32(values) => self.fit_rows(
                values,
                flags,
                noise,
                options,
                projected,
                |v| v.re,
                |v| v.im,
                Complex32::new,
            ),
            ArrayValue::Float32(values) => self.fit_rows(
                values,
                flags,
                noise,
                options,
                projected,
                |v| v,
                |_| 0.0,
                |real, _| real,
            ),
            _ => Err(MsTransformError::Spectrum(
                "continuum fitting requires complex or float visibilities".into(),
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fit_rows<T: Copy>(
        &self,
        values: &mut ArrayD<T>,
        flags: &ArrayD<bool>,
        noise: Noise<'_>,
        options: &ContinuumFitOptions,
        projected: bool,
        real: impl Fn(T) -> f32,
        imaginary: impl Fn(T) -> f32,
        assemble: impl Fn(f32, f32) -> T,
    ) -> Result<(), MsTransformError> {
        let mut values = values
            .view_mut()
            .into_dimensionality::<Ix2>()
            .map_err(spectrum_error)?;
        let flags = flags
            .view()
            .into_dimensionality::<Ix2>()
            .map_err(spectrum_error)?;
        if values.shape() != flags.shape() {
            return Err(MsTransformError::Spectrum(
                "continuum visibility and FLAG shapes differ".into(),
            ));
        }
        let index = |position| {
            if projected {
                position
            } else {
                self.channels[position]
            }
        };
        if (0..self.channels.len()).any(|position| index(position) >= values.ncols()) {
            return Err(MsTransformError::Spectrum(
                "continuum channel is outside the visibility array".into(),
            ));
        }
        let iterations = if options.reweight {
            options.iterations
        } else {
            1
        };
        for correlation in 0..values.nrows() {
            let mut spectrum = values.slice_mut(s![correlation, ..]);
            let admissible = self
                .fit_channels
                .iter()
                .enumerate()
                .filter_map(|(position, &include)| {
                    let channel = index(position);
                    let weight = noise.weight(correlation, channel);
                    (include
                        && !flags[[correlation, channel]]
                        && weight.is_finite()
                        && weight > 0.0)
                        .then_some((position, weight))
                })
                .collect::<Vec<_>>();
            let terms = admissible.len().min(options.order.saturating_add(1));
            let fit = |component: &dyn Fn(T) -> f32| {
                let data = |position| f64::from(component(spectrum[index(position)]));
                polynomial_fit(&admissible, &self.frequencies, data, terms, iterations)
            };
            let real_fit = fit(&real);
            let imaginary_fit = fit(&imaginary);
            for (position, &frequency) in self.frequencies.iter().enumerate() {
                let sample = &mut spectrum[index(position)];
                let estimate = |coefficients: &[f64]| evaluate(coefficients, frequency) as f32;
                let re = estimate(&real_fit);
                let im = estimate(&imaginary_fit);
                *sample = if options.want_continuum {
                    assemble(re, im)
                } else {
                    assemble(real(*sample) - re, imaginary(*sample) - im)
                };
            }
        }
        Ok(())
    }
}

fn polynomial_fit(
    admitted: &[(usize, f64)],
    frequencies: &[f64],
    value: impl Fn(usize) -> f64,
    terms: usize,
    iterations: usize,
) -> Vec<f64> {
    if terms == 0 || iterations == 0 {
        return Vec::new();
    }
    // A common weight scale leaves the fit unchanged. Normalize before
    // applying the shared solver's absolute singular-value cutoff.
    let weight_scale = admitted
        .iter()
        .map(|&(_, weight)| weight)
        .fold(0.0, f64::max);
    let mut rows = admitted
        .iter()
        .map(|&(position, weight)| {
            let coordinate = frequencies[position];
            let basis = std::iter::successors(Some(1.0), move |power| Some(power * coordinate))
                .take(terms)
                .collect::<Vec<_>>();
            (basis, value(position), weight / weight_scale)
        })
        .collect::<Vec<_>>();
    let scale = rows
        .iter()
        .map(|(_, value, _)| value.abs())
        .fold(0.0, f64::max);
    let mut coefficients = Vec::new();
    for pass in 0..iterations {
        coefficients = solve_weighted_least_squares(&rows, terms).unwrap_or_default();
        if pass + 1 == iterations {
            break;
        }
        let mut largest = 0.0_f64;
        for ((_, observed, weight), &(position, _)) in rows.iter_mut().zip(admitted) {
            let residual = if *weight > f64::MIN_POSITIVE {
                (*observed - evaluate(&coefficients, frequencies[position])).abs()
            } else {
                0.0
            };
            largest = largest.max(residual);
            *weight = residual;
        }
        if largest <= scale * 64.0 * f64::EPSILON {
            break;
        }
        for (_, _, weight) in &mut rows {
            *weight = if *weight > 0.0 && *weight < largest {
                (largest - *weight) / largest
            } else {
                // The shared SVD solver requires a positive weight. Using
                // the smallest normal number retains the zero-weight rows
                // for rank-deficient fits without moving a finite solution.
                f64::MIN_POSITIVE
            };
        }
    }
    coefficients
}

pub(super) fn row_flag(flags: Option<&ArrayValue>, channels: Option<&[usize]>) -> bool {
    let Some(ArrayValue::Bool(flags)) = flags else {
        return false;
    };
    let Some(channels) = channels else {
        return flags.iter().all(|&flag| flag);
    };
    let Ok(flags) = flags.view().into_dimensionality::<Ix2>() else {
        return false;
    };
    flags
        .rows()
        .into_iter()
        .all(|row| channels.iter().all(|&channel| row[channel]))
}

/// FreqAxisTVI reports the mean of its spectrum over unflagged channels.
/// When an entire correlation is flagged, use the mean of all its channels.
pub(super) fn mean_noise(
    scalar: &mut ArrayValue,
    spectrum: Option<&ArrayValue>,
    flags: &ArrayValue,
    channels: Option<&[usize]>,
) {
    let (Some(ArrayValue::Float32(spectrum)), ArrayValue::Bool(flags)) = (spectrum, flags) else {
        return;
    };
    let (Ok(spectrum), Ok(flags)) = (
        spectrum.view().into_dimensionality::<Ix2>(),
        flags.view().into_dimensionality::<Ix2>(),
    ) else {
        return;
    };
    if spectrum.shape() != flags.shape() {
        return;
    }
    let channel_count = channels.map_or(spectrum.ncols(), <[usize]>::len);
    let means = Array1::from_shape_fn(spectrum.nrows(), |correlation| {
        let (mut full, mut good, mut count) = (0.0_f32, 0.0_f32, 0);
        for index in 0..channel_count {
            let channel = channels.map_or(index, |channels| channels[index]);
            let value = spectrum[[correlation, channel]];
            full += value;
            if !flags[[correlation, channel]] {
                good += value;
                count += 1;
            }
        }
        if count == 0 {
            full / channel_count.max(1) as f32
        } else {
            good / count as f32
        }
    });
    *scalar = ArrayValue::Float32(means.into_dyn());
}

fn evaluate(coefficients: &[f64], x: f64) -> f64 {
    coefficients
        .iter()
        .rev()
        .fold(0.0, |sum, coefficient| sum * x + coefficient)
}

fn spectrum_error(error: impl std::fmt::Display) -> MsTransformError {
    MsTransformError::Spectrum(error.to_string())
}
