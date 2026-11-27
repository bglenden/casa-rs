// SPDX-License-Identifier: LGPL-3.0-or-later
//! Single-dish convolution and the separate spectral weight kernel.

use std::collections::BTreeMap;
use std::f64::consts::{LN_2, TAU};

use casa_core::{ArrayValue, Complex32};
use casa_fft::Fft2;
use casa_mstransform::SpectrumConvolution;
use ndarray::{ArrayD, Axis};

#[derive(Clone, Copy)]
enum Kernel {
    Gaussian,
    Boxcar,
}

pub(super) struct Smoothing {
    kernel: Kernel,
    width: f32,
    convolution: BTreeMap<usize, Convolution>,
    weight_kernel: Vec<f32>,
}

impl Smoothing {
    pub(super) fn new(kernel: &str, width: f32) -> Result<Self, String> {
        let kind = match kernel.as_bytes().first().map(u8::to_ascii_uppercase) {
            Some(b'G') => Kernel::Gaussian,
            Some(b'B') => Kernel::Boxcar,
            Some(b'H') => {
                return Err(format!(
                    "Smoothing kernel type \"{kernel}\" is not supported yet."
                ));
            }
            _ => return Err(format!("Illegal kernel type {kernel}")),
        };
        if !width.is_finite() || width <= 0. {
            return Err("Zero or negative kernel width is not allowed.".into());
        }
        Ok(Self {
            kernel: kind,
            width,
            convolution: BTreeMap::new(),
            weight_kernel: Vec::new(),
        })
    }
}

impl SpectrumConvolution for Smoothing {
    fn prepare(&mut self, channels: &[usize]) -> Result<(), String> {
        let minimum = channels
            .iter()
            .copied()
            .min()
            .ok_or("No spectral windows are selected.")?;
        for (window, &count) in channels.iter().enumerate() {
            if count < 2 {
                return Err(format!("smooth: Failed due to wrong spw {window}"));
            }
            if let std::collections::btree_map::Entry::Vacant(entry) = self.convolution.entry(count)
            {
                entry.insert(Convolution::new(self.kernel.samples(self.width, count))?);
            }
        }
        let coefficients = self.kernel.samples(self.width, minimum);
        let window = (self.width.round() as usize | 1).min((minimum - 1) | 1);
        let peak = coefficients.iter().copied().fold(0., f32::max);
        let middle = coefficients
            .iter()
            .rposition(|&value| value == peak)
            .unwrap_or_default();
        let start = middle.saturating_sub(window / 2);
        let weights = (start..start + window)
            .map(|index| coefficients.get(index).copied().unwrap_or_default())
            .collect::<Vec<_>>();
        let total = weights.iter().sum::<f32>();
        self.weight_kernel = weights.into_iter().map(|value| value / total).collect();
        Ok(())
    }

    fn apply(&mut self, data: &mut ArrayValue, flags: &ArrayD<bool>) -> Result<(), String> {
        let [_, channels] = <[usize; 2]>::try_from(flags.shape())
            .map_err(|_| "Single-dish flags need two axes.")?;
        let convolution = self
            .convolution
            .get_mut(&channels)
            .ok_or("Failed to get convolver. Smoothing is not properly configured.")?;
        match data {
            ArrayValue::Float32(data) if data.shape() == flags.shape() => {
                for (mut values, mask) in data.axis_iter_mut(Axis(0)).zip(flags.axis_iter(Axis(0)))
                {
                    let input = values.iter().zip(mask).map(
                        |(&value, &flag)| {
                            if flag { 0. } else { value }
                        },
                    );
                    let smooth = convolution.apply(input)?;
                    for (value, smooth) in values.iter_mut().zip(smooth) {
                        *value = smooth;
                    }
                }
            }
            ArrayValue::Complex32(data) if data.shape() == flags.shape() => {
                for (mut values, mask) in data.axis_iter_mut(Axis(0)).zip(flags.axis_iter(Axis(0)))
                {
                    let input = values.iter().zip(mask).map(
                        |(value, &flag)| {
                            if flag { 0. } else { value.re }
                        },
                    );
                    let smooth = convolution.apply(input)?;
                    for (value, smooth) in values.iter_mut().zip(smooth) {
                        *value = Complex32::new(smooth, 0.);
                    }
                }
            }
            _ => return Err("Unexpected single-dish spectrum shape or type.".into()),
        }
        Ok(())
    }

    fn weight_kernel(&self) -> &[f32] {
        &self.weight_kernel
    }
}

impl Kernel {
    fn samples(self, width: f32, channels: usize) -> Vec<f32> {
        match self {
            Self::Gaussian => {
                let sigma = f64::from(width) / (8. * LN_2).sqrt();
                let center = channels as f64 / 2.;
                let norm = 1. / (sigma * TAU.sqrt());
                (0..channels)
                    .map(|channel| {
                        let distance = (channel as f64 - center) / sigma;
                        (norm * (-0.5 * distance * distance).exp()) as f32
                    })
                    .collect()
            }
            Self::Boxcar => {
                let span = width.round() as usize;
                let mut coefficients = vec![0.; channels];
                if (1..=channels).contains(&span) {
                    // CASA shifts the centered boxcar one channel to the right.
                    let start = (channels - span) / 2 + 1;
                    let end = (start + span).min(channels);
                    coefficients[start..end].fill(1. / span as f32);
                }
                coefficients
            }
        }
    }
}

enum Convolution {
    Direct(Vec<f32>),
    Fft {
        channels: usize,
        plan: Fft2<f32>,
        response: Vec<Complex32>,
        scratch: Vec<Complex32>,
    },
}

impl Convolution {
    fn new(kernel: Vec<f32>) -> Result<Self, String> {
        let channels = kernel.len();
        if channels <= 64 {
            return Ok(Self::Direct(kernel));
        }
        let size = channels
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or("Spectral convolution extent is too large.")?;
        let mut plan = Fft2::with_threads([size, 1], 1)
            .map_err(error_text)?
            .with_estimated_plan();
        let mut response = vec![Complex32::default(); size];
        for (slot, value) in response.iter_mut().zip(kernel) {
            slot.re = value;
        }
        plan.transform(&mut response, false).map_err(error_text)?;
        Ok(Self::Fft {
            channels,
            plan,
            response,
            scratch: vec![Complex32::default(); size],
        })
    }

    fn apply(&mut self, values: impl Iterator<Item = f32>) -> Result<Vec<f32>, String> {
        match self {
            Self::Direct(kernel) => {
                let data = values.collect::<Vec<_>>();
                let channels = kernel.len();
                let result = (0..channels)
                    .map(|channel| {
                        let middle = channel + channels / 2;
                        let start = middle.saturating_sub(channels - 1);
                        let end = middle.min(channels - 1);
                        (start..=end)
                            .map(|index| f64::from(data[index]) * f64::from(kernel[middle - index]))
                            .sum::<f64>() as f32
                    })
                    .collect();
                Ok(result)
            }
            Self::Fft {
                channels,
                plan,
                response,
                scratch,
            } => {
                scratch.fill(Complex32::default());
                for (output, input) in scratch.iter_mut().zip(values) {
                    output.re = input;
                }
                plan.transform(scratch, false).map_err(error_text)?;
                for (value, transfer) in scratch.iter_mut().zip(response) {
                    *value *= *transfer;
                }
                plan.transform(scratch, true).map_err(error_text)?;
                let scale = 1. / scratch.len() as f32;
                Ok(scratch[*channels / 2..*channels / 2 + *channels]
                    .iter()
                    .map(|value| value.re * scale)
                    .collect())
            }
        }
    }
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
