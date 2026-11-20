// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel averaging for clipping and propagation to the original samples.

use std::ops::{AddAssign, Div, Mul};

use casa_types::ArrayValue;
use ndarray::{ArrayD, IxDyn};

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
        let shape = [flags.shape()[0], channels.len().div_ceil(width)];
        let averaged = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
            let start = index[1] * width;
            channels[start..(start + width).min(channels.len())]
                .iter()
                .all(|&channel| flags[[index[0], channel]])
        });
        Some(Self {
            channels,
            width,
            flags: averaged,
        })
    }

    pub(super) fn flags(&self) -> &ArrayD<bool> {
        &self.flags
    }

    pub(super) fn original_channels(&self, bin: usize) -> &[usize] {
        let start = bin * self.width;
        &self.channels[start..(start + self.width).min(self.channels.len())]
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
        ArrayD::from_shape_fn(self.flags.raw_dim(), |index| {
            let plane = index[0];
            let output_flag = self.flags[index.clone()];
            let mut sum = T::default();
            let mut normalization = F::default();
            for &channel in self.original_channels(index[1]) {
                let input = [plane, channel];
                if flags[input] == output_flag {
                    let weight = precision(method.weight(input));
                    sum += values[input] * weight;
                    normalization += weight;
                }
            }
            if matches!(method, ChannelReduction::Sum) {
                sum
            } else if normalization > F::default() {
                sum / normalization
            } else {
                T::default()
            }
        })
    }
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

impl ChannelReduction<'_> {
    fn weight(self, index: [usize; 2]) -> f64 {
        let Self::Weighted {
            spectrum,
            inverse_sigma,
        } = self
        else {
            return 1.0;
        };
        match spectrum {
            ArrayValue::Float32(weights) if inverse_sigma => {
                let sigma = weights[index];
                f64::from(if sigma > f32::MIN_POSITIVE {
                    1.0 / (sigma * sigma)
                } else {
                    0.0
                })
            }
            ArrayValue::Float32(weights) => f64::from(weights[index]),
            ArrayValue::Float64(weights) if inverse_sigma => {
                let sigma = weights[index];
                if sigma > f64::MIN_POSITIVE {
                    1.0 / (sigma * sigma)
                } else {
                    0.0
                }
            }
            ArrayValue::Float64(weights) => weights[index],
            _ => unreachable!("weight spectra are checked floating-point arrays"),
        }
    }
}
