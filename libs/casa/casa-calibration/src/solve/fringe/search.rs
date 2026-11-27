// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weighted delay/rate FFT search for reference-baseline fringe estimates.

use std::f64::consts::FRAC_PI_2;
use std::ops::Range;

use casa_core::Complex32;
use casa_fft::Fft2;
use casa_ms::MsSelectionIoBudget;

use super::{FringeParameters, FringeSample};
use crate::solve::GainSolveError;

#[derive(Clone, Copy)]
pub(super) struct GridSpec {
    pub(super) channels: usize,
    pub(super) timestamps: usize,
    pub(super) channel_spacing_hz: f64,
    pub(super) integration_seconds: f64,
    pub(super) center_hz: f64,
    pub(super) padding: usize,
}

impl GridSpec {
    pub(super) fn transform_shape(
        self,
        bands: usize,
    ) -> Result<([usize; 2], usize), GainSolveError> {
        if self.timestamps < 2 {
            return Err(error("Can't do a 2-dimensional FFT on a single timestep"));
        }
        let [Some(channels), Some(times)] =
            [self.channels, self.timestamps].map(|extent| extent.checked_mul(self.padding))
        else {
            return Err(error("fringe FFT dimensions overflow"));
        };
        let elements = channels
            .checked_mul(times)
            .ok_or_else(|| error("fringe FFT size overflow"))?;
        let bytes_per_band = elements
            .checked_mul(size_of::<Complex32>())
            .ok_or_else(|| error("fringe FFT size overflow"))?;
        let budget =
            MsSelectionIoBudget::from_system_memory(bands, bytes_per_band, None).map_err(error)?;
        if bytes_per_band
            .checked_mul(bands)
            .is_none_or(|bytes| bytes > budget.available_bytes)
        {
            return Err(error(
                "fringe FFT grid exceeds the system memory budget; select fewer channels or a shorter solution interval",
            ));
        }
        Ok(([channels, times], elements))
    }
}

pub(super) struct FringeSearch {
    grid: GridSpec,
    fft: Fft2<f32>,
    plane: Vec<Complex32>,
}

#[derive(Clone, Copy)]
pub(super) struct FringePeak {
    pub(super) parameters: FringeParameters,
    pub(super) snr: f32,
    pub(super) accepted: bool,
    pub(super) active: bool,
}

impl FringeSearch {
    pub(super) fn new(grid: GridSpec) -> Result<Self, GainSolveError> {
        let (shape, elements) = grid.transform_shape(1)?;
        let fft = Fft2::with_threads(shape, 1)
            .map_err(error)?
            .with_estimated_plan();
        let mut plane = Vec::new();
        plane.try_reserve_exact(elements).map_err(error)?;
        plane.resize(elements, Complex32::default());
        Ok(Self { grid, fft, plane })
    }

    pub(super) fn peak(
        &mut self,
        samples: &[FringeSample],
        sign: f64,
        delay_window: [f64; 2],
        rate_window: [f64; 2],
    ) -> Result<Option<FringePeak>, GainSolveError> {
        let [channels, times] = self.fft.shape();
        let bandwidth_ghz = channels as f64 * self.grid.channel_spacing_hz * 1e-9;
        let time_frequency = times as f64 * self.grid.integration_seconds * self.grid.center_hz;
        let Some(delays) = fft_bins(delay_window, sign, bandwidth_ghz, channels) else {
            return Ok(None);
        };
        let Some(rates) = fft_bins(rate_window, sign, time_frequency, times) else {
            return Ok(None);
        };

        self.plane.fill(Complex32::default());
        let mut weights = NoiseWeights::default();
        for sample in samples {
            self.plane[sample.channel * times + sample.integration] =
                sample.weighted_unit_visibility();
            weights.add(sample.weight);
        }
        if weights.count == 0 {
            return Ok(None);
        }
        self.fft.transform(&mut self.plane, false).map_err(error)?;
        let coefficient = |delay: isize, rate: isize| {
            let delay = delay.rem_euclid(channels as isize) as usize;
            let rate = rate.rem_euclid(times as isize) as usize;
            self.plane[delay * times + rate]
        };
        let (mut best_delay, mut best_rate, mut maximum) = (0, 0, -1.);
        for rate in rates {
            for delay in delays.clone() {
                let amplitude = coefficient(delay, rate).norm();
                if amplitude > maximum {
                    (best_delay, best_rate, maximum) = (delay, rate, amplitude);
                }
            }
        }
        let valid_peak = |left: f32, right: f32| {
            maximum > 0.
                && (left - 2. * maximum + right != 0. || left == maximum && right == maximum)
        };
        if !valid_peak(
            coefficient(best_delay - 1, best_rate).norm(),
            coefficient(best_delay + 1, best_rate).norm(),
        ) || !valid_peak(
            coefficient(best_delay, best_rate - 1).norm(),
            coefficient(best_delay, best_rate + 1).norm(),
        ) {
            return Ok(None);
        }

        let phase_rad = sign * f64::from(coefficient(best_delay, best_rate).arg());
        // CASA's concatenated FFT checks peak curvature but keeps the integer
        // grid indices for the parameters. The optional global stage refines them.
        let delay_ns = if self.grid.channels == 1 {
            0.
        } else {
            sign * folded_bin(best_delay, channels) / bandwidth_ghz
        };
        let rate = sign * folded_bin(best_rate, times) / time_frequency;
        Ok(Some(FringePeak {
            parameters: FringeParameters {
                phase_rad,
                delay_ns,
                rate,
                dispersion: 0.,
            },
            snr: weights.snr(maximum),
            accepted: true,
            active: true,
        }))
    }
}

fn folded_bin(bin: isize, size: usize) -> f64 {
    if size.is_multiple_of(2) && bin == -(size as isize / 2) {
        -bin as f64
    } else {
        bin as f64
    }
}

fn fft_bins(window: [f64; 2], sign: f64, units_to_bins: f64, size: usize) -> Option<Range<isize>> {
    if !units_to_bins.is_finite()
        || units_to_bins == 0.
        || window.iter().any(|value| !value.is_finite())
    {
        return None;
    }
    let [first, second] = window.map(|value| value * sign * units_to_bins);
    let half = size as f64 * 0.5;
    let low = first.min(second).max(-half);
    let high = first.max(second).min(half);
    if low > high {
        return None;
    }
    let start = low as isize;
    let end = (high as isize).max(start + 1);
    Some(start..end)
}

#[derive(Default)]
pub(super) struct NoiseWeights {
    count: usize,
    total: f64,
    squares: f64,
}

impl NoiseWeights {
    pub(super) fn add(&mut self, weight: f32) {
        if weight > 0. {
            self.count += 1;
            self.total += f64::from(weight);
            self.squares += f64::from(weight).powi(2);
        }
    }

    pub(super) fn snr(&self, peak: f32) -> f32 {
        let sum = self.total as f32;
        if sum.abs() < f32::EPSILON {
            return 0.;
        }
        let peak = peak.min(0.999 * sum);
        let angle = (FRAC_PI_2 * f64::from(peak) / f64::from(sum)) as f32;
        let weight = (sum / (self.squares as f32 / self.count as f32).sqrt()).sqrt();
        (f64::from(angle).tan().powf(1.163) * f64::from(weight)) as f32
    }
}

fn error(error: impl ToString) -> GainSolveError {
    GainSolveError::Delay(error.to_string())
}
