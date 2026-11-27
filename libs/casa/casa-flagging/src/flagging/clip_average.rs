// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel averaging for clipping and propagation to the original samples.

use std::{
    cmp::Ordering,
    ops::{AddAssign, Div, Mul},
};

use casa_core::ArrayValue;
use ndarray::{Array2, ArrayD, ArrayView2, Ix2};

use super::{FlaggingError, MeasurementSet};

pub(super) fn check_channel_bins(
    ms: &MeasurementSet,
    widths: &[u32],
    path: &str,
) -> Result<(), FlaggingError> {
    // CASA configures the frequency iterator with all metadata windows, even
    // if row or channel selection leaves no MAIN rows for some of them.
    let spw_count = ms
        .spectral_window()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("open SPECTRAL_WINDOW: {source}"),
        })?
        .row_count();
    if !widths.is_empty() && (widths.len() == 1 || widths.len() == spw_count) {
        Ok(())
    } else {
        Err(FlaggingError::InvalidClipChannelBins)
    }
}

pub(super) struct ChannelBins<'a> {
    channels: &'a [usize],
    width: usize,
    flags: ArrayD<bool>,
}

impl<'a> ChannelBins<'a> {
    pub(super) fn new(width: u32, channels: &'a [usize], flags: &ArrayD<bool>) -> Option<Self> {
        let width = match width {
            0 => return None,
            1 => channels.len(),
            width => (width as usize).min(channels.len()),
        };
        if width <= 1 {
            return None;
        }
        let input = matrix(flags);
        let shape = (input.nrows(), channels.len().div_ceil(width));
        let averaged = Array2::from_shape_fn(shape, |(plane, bin)| {
            let start = bin * width;
            channels[start..(start + width).min(channels.len())]
                .iter()
                .all(|&channel| input[(plane, channel)])
        });
        Some(Self {
            channels,
            width,
            flags: averaged.into_dyn(),
        })
    }

    pub(super) fn flags(&self) -> &ArrayD<bool> {
        &self.flags
    }

    pub(super) fn original_channels(&self, bin: usize) -> &[usize] {
        bin_members(self.channels, self.width, bin)
    }

    pub(super) fn width(&self) -> usize {
        self.width
    }

    pub(super) fn average(
        &self,
        data: &ArrayValue,
        flags: &ArrayD<bool>,
        method: ChannelReduction<'_>,
    ) -> ArrayValue {
        match data {
            ArrayValue::Float32(values) => {
                ArrayValue::Float32(self.reduce(values, flags, method, |value| value as f32))
            }
            ArrayValue::Float64(values) => {
                ArrayValue::Float64(self.reduce(values, flags, method, |value| value))
            }
            ArrayValue::Complex32(values) => {
                ArrayValue::Complex32(self.reduce(values, flags, method, |value| value as f32))
            }
            ArrayValue::Complex64(values) => {
                ArrayValue::Complex64(self.reduce(values, flags, method, |value| value))
            }
            _ => unreachable!("clip inputs are checked numeric arrays"),
        }
    }

    fn reduce<T, F>(
        &self,
        values: &ArrayD<T>,
        flags: &ArrayD<bool>,
        method: ChannelReduction<'_>,
        precision: impl Fn(f64) -> F,
    ) -> ArrayD<T>
    where
        T: Copy + Default + AddAssign + Mul<F, Output = T> + Div<F, Output = T>,
        F: Copy + Default + PartialOrd + AddAssign,
    {
        let values = matrix(values);
        let flags = matrix(flags);
        let averaged = matrix(&self.flags);
        let measure = method.weights();
        let one = precision(1.0);
        Array2::from_shape_fn(averaged.dim(), |(plane, bin)| {
            let output_flag = averaged[(plane, bin)];
            let mut sum = T::default();
            let mut normalization = F::default();
            for &channel in self.original_channels(bin) {
                let input = (plane, channel);
                if flags[input] == output_flag {
                    match &measure {
                        Some(weights) => {
                            let weight = precision(weights.get(input));
                            sum += values[input] * weight;
                            normalization += weight;
                        }
                        None => {
                            sum += values[input];
                            normalization += one;
                        }
                    }
                }
            }
            if matches!(method, ChannelReduction::Sum) {
                sum
            } else if matches!(
                normalization.partial_cmp(&F::default()),
                Some(Ordering::Less | Ordering::Greater)
            ) {
                // Normalize by the real sum of weights. Squaring the sum
                // as a complex norm can underflow for valid positive weights.
                sum / normalization
            } else {
                T::default()
            }
        })
        .into_dyn()
    }
}

pub(super) fn bin_members(channels: &[usize], width: usize, bin: usize) -> &[usize] {
    let start = bin * width;
    &channels[start..(start + width).min(channels.len())]
}

#[derive(Clone, Copy)]
pub(super) enum ChannelReduction<'a> {
    Mean,
    Sum,
    Weighted {
        spectrum: &'a ArrayValue,
        inverse_sigma: bool,
    },
}

impl<'a> ChannelReduction<'a> {
    pub(super) fn weights(self) -> Option<SpectralWeights<'a>> {
        let Self::Weighted {
            spectrum,
            inverse_sigma,
        } = self
        else {
            return None;
        };
        Some(match spectrum {
            ArrayValue::Float32(weights) => SpectralWeights::Single(matrix(weights), inverse_sigma),
            ArrayValue::Float64(weights) => SpectralWeights::Double(matrix(weights), inverse_sigma),
            _ => unreachable!("weight spectra are checked floating-point arrays"),
        })
    }
}

fn matrix<T>(array: &ArrayD<T>) -> ArrayView2<'_, T> {
    array
        .view()
        .into_dimensionality::<Ix2>()
        .expect("clipping rows are rank two")
}

pub(super) enum SpectralWeights<'a> {
    Single(ArrayView2<'a, f32>, bool),
    Double(ArrayView2<'a, f64>, bool),
}

impl SpectralWeights<'_> {
    pub(super) fn get(&self, index: (usize, usize)) -> f64 {
        match self {
            Self::Single(weights, true) => {
                let sigma = weights[index];
                f64::from(if sigma > f32::MIN_POSITIVE {
                    1.0 / (sigma * sigma)
                } else {
                    0.0
                })
            }
            Self::Single(weights, false) => f64::from(weights[index]),
            Self::Double(weights, true) => {
                let sigma = weights[index];
                if sigma > f64::MIN_POSITIVE {
                    1.0 / (sigma * sigma)
                } else {
                    0.0
                }
            }
            Self::Double(weights, false) => weights[index],
        }
    }
}
