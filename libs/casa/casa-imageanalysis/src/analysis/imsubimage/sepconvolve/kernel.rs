// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{LN_2, TAU};

use casa_core::Complex64;
use casa_fft::Fft2;
use ndarray::ArrayViewMut1;

use casa_images::ImageError;

use super::invalid;

#[derive(Clone, Copy)]
pub(super) enum Kernel {
    Gaussian,
    Boxcar,
    Hanning,
}

impl Kernel {
    pub(super) fn parse(name: &str) -> Result<Self, ImageError> {
        match name.as_bytes().first().map(u8::to_ascii_lowercase) {
            Some(b'g') => Ok(Self::Gaussian),
            Some(b'b') => Ok(Self::Boxcar),
            Some(b'h') => Ok(Self::Hanning),
            _ => Err(invalid(format!("Unknown convolution kernel {name:?}"))),
        }
    }

    pub(super) fn weights(
        self,
        length: usize,
        width: f32,
        scale: f64,
    ) -> Result<Weights, ImageError> {
        if length < 2 {
            return Err(invalid("Kernel axis shape must be greater than one"));
        }
        let width = f64::from(width);
        if (!matches!(self, Self::Hanning) && !width.is_finite())
            || scale.is_nan()
            || scale > f64::from(f32::MAX)
        {
            return Err(invalid(
                "Kernel width and scale must be finite Float values",
            ));
        }
        let autoscale = scale < 0.0;
        let gain = if autoscale { 1.0 } else { scale as f32 };
        let weights = match self {
            Self::Gaussian => {
                if width <= 0.0 {
                    return Err(invalid("Gaussian width must be positive"));
                }
                let sigma = width / (8.0 * LN_2).sqrt();
                let kernel_length = ((5.0 * sigma).round() + 1.0) * 2.0;
                let size = checked_length(kernel_length)?.max(length);
                let origin = size / 2;
                let radius = (15.0 * sigma).ceil() as usize;
                let low = origin.saturating_sub(radius);
                let high = origin.saturating_add(radius).saturating_add(2).min(size);
                let norm = if autoscale {
                    1.0 / (TAU.sqrt() * sigma)
                } else {
                    1.0
                };
                let center = size as f64 / 2.0;
                Weights::from_support(low..high, origin, length, gain, |pixel| {
                    let offset = (pixel as f64 - center) / sigma;
                    norm * (-0.5 * offset * offset).exp()
                })
            }
            Self::Boxcar => {
                let samples = checked_length((width + 0.5).trunc())?;
                if samples == 0 {
                    return Ok(Weights {
                        data: Vec::new(),
                        first_offset: 0,
                    });
                }
                let size = length.max(samples + 1);
                let start = (size - samples) / 2;
                let norm = if autoscale { 1.0 / samples as f64 } else { 1.0 };
                Weights::from_support(start..start + samples, size / 2, length, gain, |_| norm)
            }
            Self::Hanning => {
                let middle = (length - 1) / 2;
                let radius = length.div_ceil(2);
                let norm = if autoscale {
                    1.0 / (length + 1) as f64
                } else {
                    0.5
                };
                Weights::from_support(0..2 * radius - 1, length / 2, length, gain, |pixel| {
                    let phase =
                        TAU * (radius - pixel.abs_diff(middle)) as f64 / (length + 1) as f64;
                    norm * (1.0 - phase.cos())
                })
            }
        };
        Ok(weights)
    }
}

fn checked_length(length: f64) -> Result<usize, ImageError> {
    if length >= 0.0 && length < isize::MAX as f64 {
        Ok(length as usize)
    } else {
        Err(invalid(
            "Convolution kernel width is outside the supported range",
        ))
    }
}

pub(super) struct Weights {
    data: Vec<f64>,
    first_offset: isize,
}

impl Weights {
    pub(super) fn halo(&self) -> [usize; 2] {
        let last = self.first_offset + self.data.len() as isize - 1;
        [last.max(0) as usize, (-self.first_offset).max(0) as usize]
    }

    fn from_support(
        support: std::ops::Range<usize>,
        center: usize,
        length: usize,
        scale: f32,
        coefficient: impl Fn(usize) -> f64,
    ) -> Self {
        let start = support.start.max(center.saturating_sub(length - 1));
        let end = support.end.min(center.saturating_add(length));
        let data = (start..end)
            .map(|pixel| f64::from(coefficient(pixel) as f32 * scale))
            .collect();
        Self {
            data,
            first_offset: start as isize - center as isize,
        }
    }
}

pub(super) struct LineConvolution<'a> {
    weights: &'a Weights,
    input: Vec<f64>,
    fft: Option<LineFft>,
}

impl<'a> LineConvolution<'a> {
    pub(super) fn new(length: usize, weights: &'a Weights) -> Result<Self, ImageError> {
        let fft = (weights.data.len() >= 32
            && length.saturating_mul(weights.data.len()) >= 1 << 16)
            .then(|| LineFft::new(length, weights))
            .transpose()?;
        Ok(Self {
            weights,
            input: vec![0.0; length],
            fft,
        })
    }

    pub(super) fn apply(&mut self, mut line: ArrayViewMut1<'_, f32>) -> Result<(), ImageError> {
        let mut finite = true;
        let mut nonzero = false;
        for (to, &value) in self.input.iter_mut().zip(line.iter()) {
            *to = f64::from(value);
            finite &= value.is_finite();
            nonzero |= value != 0.0;
        }
        if !finite {
            // The reference FFT spreads a nonfinite input across its profile.
            line.fill(f32::NAN);
            return Ok(());
        }
        if !nonzero {
            return Ok(());
        }
        if let Some(fft) = &mut self.fft {
            return fft.apply(&self.input, line, self.weights.first_offset);
        }
        for (pixel, value) in line.indexed_iter_mut() {
            let start = pixel as isize - self.weights.first_offset;
            let first = (start - self.input.len() as isize + 1).max(0) as usize;
            let end = self.weights.data.len().min((start + 1).max(0) as usize);
            *value = if first < end {
                let high = (start + 1) as usize;
                self.weights.data[first..end]
                    .iter()
                    .zip(self.input[high - end..high - first].iter().rev())
                    .map(|(weight, sample)| weight * sample)
                    .sum::<f64>() as f32
            } else {
                0.0
            };
        }
        Ok(())
    }
}

struct LineFft {
    transform: Fft2<f64>,
    transfer: Vec<Complex64>,
    buffer: Vec<Complex64>,
    convolution_length: usize,
}

impl LineFft {
    fn new(length: usize, weights: &Weights) -> Result<Self, ImageError> {
        let convolution_length = length
            .checked_add(weights.data.len() - 1)
            .ok_or_else(|| invalid("Convolution length overflows"))?;
        let extended = convolution_length
            .checked_next_power_of_two()
            .ok_or_else(|| invalid("Convolution transform length overflows"))?;
        let mut transform = Fft2::with_threads([1, extended], 1)
            .map_err(fft_error)?
            .with_estimated_plan();
        let mut transfer = vec![Complex64::default(); extended];
        for (target, &weight) in transfer.iter_mut().zip(&weights.data) {
            target.re = weight;
        }
        transform
            .transform(&mut transfer, false)
            .map_err(fft_error)?;
        Ok(Self {
            transform,
            transfer,
            buffer: vec![Complex64::default(); extended],
            convolution_length,
        })
    }

    fn apply(
        &mut self,
        input: &[f64],
        mut output: ArrayViewMut1<'_, f32>,
        first_offset: isize,
    ) -> Result<(), ImageError> {
        self.buffer.fill(Complex64::default());
        for (entry, &sample) in self.buffer.iter_mut().zip(input) {
            entry.re = sample;
        }
        self.transform
            .transform(&mut self.buffer, false)
            .map_err(fft_error)?;
        for (entry, &weight) in self.buffer.iter_mut().zip(&self.transfer) {
            *entry *= weight;
        }
        self.transform
            .transform(&mut self.buffer, true)
            .map_err(fft_error)?;
        let norm = self.buffer.len() as f64;
        for (pixel, value) in output.indexed_iter_mut() {
            let offset = (pixel as isize - first_offset) as usize;
            *value = if offset < self.convolution_length {
                (self.buffer[offset].re / norm) as f32
            } else {
                0.0
            };
        }
        Ok(())
    }
}

fn fft_error(error: casa_fft::FftError) -> ImageError {
    invalid(format!("Cannot convolve image profile: {error}"))
}
