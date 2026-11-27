// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fringe search with an FFT per spectral window and coherent band combination.

use std::f64::consts::TAU;

use casa_core::Complex32;
use casa_fft::Fft2;

use super::search::{FringePeak, GridSpec, NoiseWeights};
use super::{FringeParameters, FringeSample, Window, error};
use crate::solve::GainSolveError;

const PADDING: usize = 4;

pub(super) struct MultibandSearch {
    fft: Fft2<f32>,
    bands: Vec<Band>,
    grid: GridSpec,
}

struct Band {
    description: i32,
    offset: f64,
    spectrum: Vec<Complex32>,
}

impl MultibandSearch {
    pub(super) fn new<'a>(
        mut grid: GridSpec,
        windows: impl Iterator<Item = (i32, &'a Window)>,
    ) -> Result<Self, GainSolveError> {
        let mut windows = windows.collect::<Vec<_>>();
        windows.sort_by_key(|(description, window)| (window.spw, *description));
        let first = windows[0].1.selected_hz[0] as f32;
        let last = &windows[windows.len() - 1].1.selected_hz;
        grid.padding = PADDING;
        grid.channels = windows
            .iter()
            .map(|(_, w)| w.selected_hz.len())
            .max()
            .unwrap_or(0);
        grid.channel_spacing_hz = last
            .get(1)
            .map_or(grid.channel_spacing_hz, |&second| second - last[0]);
        let (shape, elements) = grid.transform_shape(windows.len())?;
        let fft = Fft2::with_threads(shape, 1)
            .map_err(error)?
            .with_estimated_plan();
        let bandwidth = grid.channels as f64 * grid.channel_spacing_hz;
        let bands = windows
            .into_iter()
            .map(|(description, window)| {
                let mut spectrum = Vec::new();
                spectrum.try_reserve_exact(elements).map_err(error)?;
                spectrum.resize(elements, Complex32::default());
                let delta = f64::from(window.selected_hz[0] as f32) - f64::from(first);
                Ok(Band {
                    description,
                    offset: f64::from((delta / bandwidth) as f32),
                    spectrum,
                })
            })
            .collect::<Result<_, GainSolveError>>()?;
        Ok(Self { fft, bands, grid })
    }

    pub(super) fn peak(
        &mut self,
        samples: &[FringeSample],
        sign: f64,
    ) -> Result<Option<FringePeak>, GainSolveError> {
        let [channels, times] = self.fft.shape();
        let mut weights = NoiseWeights::default();
        for band in &mut self.bands {
            band.spectrum.fill(Complex32::default());
            for sample in samples
                .iter()
                .filter(|sample| sample.data_description == band.description)
            {
                band.spectrum[sample.local_channel * times + sample.integration] =
                    sample.weighted_unit_visibility();
                weights.add(sample.weight);
            }
            self.fft
                .transform(&mut band.spectrum, false)
                .map_err(error)?;
        }
        let [time, channel] = self.grid_peak(channels, times);
        let evaluate = |[time, channel]: [f64; 2]| {
            let time = if time < 0. { time + times as f64 } else { time };
            let channel = if channel < 0. {
                channel + channels as f64
            } else {
                channel
            };
            let time_weights = interpolation_weights(time, times);
            let channel_weights = interpolation_weights(channel, channels);
            self.bands
                .iter()
                .map(|band| {
                    let rotation = phasor(-TAU * band.offset * channel.fract());
                    let mut sum = Complex32::default();
                    for (channel, &channel_weight) in channel_weights.iter().enumerate() {
                        for (time, &time_weight) in time_weights.iter().enumerate() {
                            sum += band.spectrum[channel * times + time]
                                * ((rotation * time_weight) * channel_weight);
                        }
                    }
                    sum
                })
                .sum::<Complex32>()
        };
        let point = refine_peak([time, channel], |point| -evaluate(point).norm());
        let amplitude = evaluate(point);
        if amplitude.norm_sqr() == 0. {
            return Ok(None);
        }
        // CASA's separate-band search uses the entire FFT rather than the
        // supplied windows, and folds the padded indices on the original grid.
        let rate_period = self.grid.integration_seconds * self.grid.center_hz;
        let delay_period = self.grid.channel_spacing_hz * 1e-9;
        Ok(Some(FringePeak {
            parameters: FringeParameters {
                phase_rad: sign * f64::from(amplitude.arg()),
                delay_ns: sign * folded_bin(point[1], self.grid.channels) / delay_period,
                rate: sign * folded_bin(point[0], self.grid.timestamps) / rate_period,
                dispersion: 0.,
            },
            snr: weights.snr(amplitude.norm()),
            accepted: true,
            active: true,
        }))
    }

    fn grid_peak(&self, channels: usize, times: usize) -> [f64; 2] {
        let extent = self.bands.iter().map(|band| band.offset).fold(0., f64::max);
        let bins = (extent + 1.5) as usize;
        let mut best = [0., 0.];
        let mut largest = -1.;
        for bin in 0..bins {
            let fraction = (bin as f64 - (bins / 2) as f64) / bins as f64;
            let rotations = self
                .bands
                .iter()
                .map(|band| phasor(-TAU * band.offset * fraction))
                .collect::<Vec<_>>();
            for time in 0..times {
                for channel in 0..channels {
                    let index = channel * times + time;
                    let combined = self
                        .bands
                        .iter()
                        .zip(&rotations)
                        .map(|(band, &rotation)| band.spectrum[index] * rotation)
                        .sum::<Complex32>();
                    let norm = combined.norm();
                    if norm > largest {
                        largest = norm;
                        best = [time as f64, channel as f64 + fraction];
                    }
                }
            }
        }
        best
    }
}

fn phasor(phase: f64) -> Complex32 {
    let (sin, cos) = (phase as f32).sin_cos();
    Complex32::new(cos, sin)
}

fn interpolation_weights(position: f64, size: usize) -> Vec<Complex32> {
    (0..size)
        .map(|index| {
            let delta = position - index as f64;
            if position.fract().abs() < 1e-8 {
                Complex32::new(f32::from(delta == 0.), 0.)
            } else {
                let one = Complex32::new(1., 0.);
                (one - phasor(-TAU * delta))
                    / (one - phasor(-TAU * delta / size as f64))
                    / size as f32
            }
        })
        .collect()
}

fn folded_bin(index: f64, extent: usize) -> f64 {
    let fraction = index / extent as f64;
    if fraction > 0.5 {
        fraction - 1.
    } else {
        fraction
    }
}

fn refine_peak(start: [f64; 2], cost: impl Fn([f64; 2]) -> f32) -> [f64; 2] {
    // A short Nelder–Mead refinement of the fractional delay and rate bins.
    let mut points = [
        start,
        [start[0] + 0.01, start[1]],
        [start[0], start[1] + 0.01],
    ]
    .map(|point| (point, cost(point)));
    for _ in 0..5 {
        points.sort_by(|left, right| left.1.total_cmp(&right.1));
        let [best, second, worst] = points;
        let center = std::array::from_fn::<_, 2, _>(|i| (best.0[i] + second.0[i]) * 0.5);
        let trial = |scale, point: [f64; 2]| {
            let point = std::array::from_fn(|i| center[i] + scale * (point[i] - center[i]));
            (point, cost(point))
        };
        let reflected = trial(-1., worst.0);
        if reflected.1 < best.1 {
            let expanded = trial(-2., worst.0);
            points[2] = if expanded.1 < best.1 {
                expanded
            } else {
                reflected
            };
        } else if reflected.1 > second.1 {
            let closer = if reflected.1 < worst.1 {
                reflected
            } else {
                worst
            };
            let contracted = trial(0.5, closer.0);
            if contracted.1 <= closer.1 {
                points[2] = contracted;
            } else {
                for vertex in &mut points[1..] {
                    let midpoint = std::array::from_fn(|i| (best.0[i] + vertex.0[i]) * 0.5);
                    *vertex = (midpoint, cost(midpoint));
                }
            }
        } else {
            points[2] = reflected;
        }
    }
    points
        .into_iter()
        .min_by(|left, right| left.1.total_cmp(&right.1))
        .unwrap()
        .0
}
