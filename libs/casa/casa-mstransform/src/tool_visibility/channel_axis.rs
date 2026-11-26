// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel coordinates selected on each row of the public MS visibility view.

use std::collections::BTreeSet;
use std::ops::{AddAssign, Div, Mul, Range};

use casa_core::{ArrayValue, RecordField, RecordValue, Value};
use ndarray::ArrayD;

use super::baseline_axis::{element_count, filled};
use super::{VisibilityReadOptions, input, shaped, vector};
use crate::transform::{mean_weight_preferring_unflagged, weight_from_sigma};
use casa_ms::{MeasurementSet, MsResult};

/// Data descriptions and labels retained by the legacy selector.
#[derive(Clone, Copy)]
pub struct LegacyVisibilityAxes<'a> {
    /// Data descriptions in selector initialization order.
    pub descriptions: &'a [i32],
    /// Labels supplied to selectpolarizationold, including an empty request.
    pub correlation_labels: Option<&'a [String]>,
}

/// Positive-width bins in the input channel axis of each selected window.
#[derive(Clone, Copy, Debug)]
pub struct VisibilityChannels {
    count: usize,
    first: usize,
    width: usize,
    stride: usize,
}

impl VisibilityChannels {
    /// Check that every bin lies in the input axis. An unknown, empty, or
    /// overflowing interval leaves the attachment's prior selection in place.
    pub fn new(
        count: i64,
        first: i64,
        width: i64,
        stride: i64,
        input_channels: usize,
    ) -> Option<Self> {
        let selection = Self {
            count: usize::try_from(count).ok()?,
            first: usize::try_from(first).ok()?,
            width: usize::try_from(width).ok()?,
            stride: usize::try_from(stride).ok()?,
        };
        if selection.width == 0 || selection.stride == 0 {
            return None;
        }
        let end = selection
            .count
            .checked_sub(1)?
            .checked_mul(selection.stride)?
            .checked_add(selection.first)?
            .checked_add(selection.width)?;
        (end <= input_channels).then_some(selection)
    }

    /// Number of requested channel bins.
    pub fn count(self) -> usize {
        self.count
    }

    /// Number of input channels that contribute to each bin.
    pub fn width(self) -> usize {
        self.width
    }

    /// Input channel ranges in output order. Ranges can overlap.
    pub fn ranges(self) -> impl Iterator<Item = Range<usize>> {
        (0..self.count).map(move |bin| {
            let start = self.first + bin * self.stride;
            start..start + self.width
        })
    }

    pub(super) fn coordinates(self, input: &[f64]) -> MsResult<Vec<f64>> {
        self.ranges()
            .map(|range| {
                input
                    .get(range)
                    .map(|values| values.iter().sum::<f64>() / values.len() as f64)
                    .ok_or_else(|| {
                        super::input("selected channels are outside the spectral window")
                    })
            })
            .collect()
    }

    pub(super) fn legacy_resolution(
        self,
        frequencies: &[f64],
        widths: &[f64],
    ) -> MsResult<Vec<f64>> {
        self.check_bounds(frequencies.len())?;
        self.check_bounds(widths.len())?;
        Ok(self
            .ranges()
            .map(|span| {
                let last = span.end - 1;
                if self.width == 1 {
                    widths[span.start]
                } else {
                    (widths[self.first] + widths[last]) * 0.5
                        + (frequencies[last] - frequencies[span.start]).abs()
                }
            })
            .collect())
    }

    pub(super) fn selects<T: Copy>(self, values: &ArrayD<T>) -> MsResult<ArrayD<T>> {
        self.map_bins(values, |_, samples, correlations, correlation, span| {
            samples[span.start * correlations + correlation]
        })
    }

    pub(super) fn flags(self, values: &ArrayD<bool>) -> MsResult<ArrayD<bool>> {
        self.map_bins(values, |_, samples, correlations, correlation, mut span| {
            span.all(|channel| samples[channel * correlations + correlation])
        })
    }

    pub(super) fn means<T>(
        self,
        values: &ArrayD<T>,
        flags: &ArrayD<bool>,
        weights: Option<&ChannelWeights>,
    ) -> MsResult<ArrayD<T>>
    where
        T: AddAssign + Copy + Default + Div<f32, Output = T> + Mul<f32, Output = T>,
    {
        let [correlations, channels, rows] = cube_shape(values)?;
        if flags.shape() != values.shape() {
            return Err(input("FLAG shape does not match channel selection data"));
        }
        let weights = weights
            .map(|weights| weights.view(correlations, channels, rows))
            .transpose()?;
        let flags = memory(flags)?;
        let plane = correlations * channels;
        self.map_bins(values, |row, samples, correlations, correlation, span| {
            let mut sum = T::default();
            let mut total = 0.0;
            for channel in span {
                let cell = channel * correlations + correlation;
                if !flags[row * plane + cell] {
                    let weight = weights
                        .as_ref()
                        .map_or(1.0, |weights| weights.at(row, cell, correlation));
                    sum += samples[cell] * weight;
                    total += weight;
                }
            }
            if total > 0.0 {
                sum / total
            } else {
                T::default()
            }
        })
    }

    pub(super) fn statistic(
        self,
        flags: &ArrayD<bool>,
        weights: &ChannelWeights,
        sigma: bool,
    ) -> MsResult<ArrayD<f32>> {
        let [correlations, channels, rows] = cube_shape(flags)?;
        self.check_bounds(channels)?;
        let weights = weights.view(correlations, channels, rows)?;
        let memory = memory(flags)?;
        let shape = [correlations, rows];
        let mut result = filled(element_count(&shape)?, 0.0)?;
        for (row, samples) in memory.chunks_exact(correlations * channels).enumerate() {
            for correlation in 0..correlations {
                let bins = self.ranges().map(|span| {
                    let mut sums = [0.0, 0.0];
                    let mut any_unflagged = false;
                    for channel in span {
                        let cell = channel * correlations + correlation;
                        let flagged = samples[cell];
                        sums[usize::from(flagged)] += weights.at(row, cell, correlation);
                        any_unflagged |= !flagged;
                    }
                    let flagged = !any_unflagged;
                    let weight = sums[usize::from(flagged)];
                    let value = if sigma {
                        crate::transform::sigma_from_weight(weight)
                    } else {
                        weight
                    };
                    (flagged, value)
                });
                result[row * correlations + correlation] = mean_weight_preferring_unflagged(bins);
            }
        }
        shaped(&shape, result)
    }

    fn check_bounds(self, channels: usize) -> MsResult<()> {
        let end = self.first + (self.count - 1) * self.stride + self.width;
        if end > channels {
            return Err(input("selected channels are outside the visibility cell"));
        }
        Ok(())
    }

    fn map_bins<T, R>(
        self,
        values: &ArrayD<T>,
        mut evaluate: impl FnMut(usize, &[T], usize, usize, Range<usize>) -> R,
    ) -> MsResult<ArrayD<R>> {
        let [correlations, channels, rows] = cube_shape(values)?;
        self.check_bounds(channels)?;
        let shape = [correlations, self.count, rows];
        let mut output = Vec::new();
        output
            .try_reserve_exact(element_count(&shape)?)
            .map_err(|_| input("cannot allocate channel-selected visibility array"))?;
        for (row, samples) in memory(values)?
            .chunks_exact(correlations * channels)
            .enumerate()
        {
            for span in self.ranges() {
                for correlation in 0..correlations {
                    output.push(evaluate(
                        row,
                        samples,
                        correlations,
                        correlation,
                        span.clone(),
                    ));
                }
            }
        }
        shaped(&shape, output)
    }
}

#[derive(Clone, Copy)]
pub(super) enum ChannelWeightKind {
    Observed,
    Corrected,
}

impl ChannelWeightKind {
    pub(super) fn spectrum(self) -> &'static str {
        match self {
            Self::Observed => "SIGMA_SPECTRUM",
            Self::Corrected => "WEIGHT_SPECTRUM",
        }
    }

    pub(super) fn fallback(self) -> &'static str {
        match self {
            Self::Observed => "SIGMA",
            Self::Corrected => "WEIGHT",
        }
    }
}

pub(super) struct ChannelWeights {
    values: ArrayD<f32>,
    spectral: bool,
}

impl ChannelWeights {
    pub(super) fn new(mut values: ArrayD<f32>, spectral: bool, kind: ChannelWeightKind) -> Self {
        if matches!(kind, ChannelWeightKind::Observed) {
            values.mapv_inplace(weight_from_sigma);
        }
        Self { values, spectral }
    }

    fn view(&self, correlations: usize, channels: usize, rows: usize) -> MsResult<WeightView<'_>> {
        let expected = if self.spectral {
            &[correlations, channels, rows][..]
        } else {
            &[correlations, rows][..]
        };
        if self.values.shape() != expected {
            return Err(input("channel weights do not match visibility dimensions"));
        }
        let channels = if self.spectral { channels } else { 1 };
        Ok(WeightView {
            values: memory(&self.values)?,
            width: correlations * channels,
            spectral: self.spectral,
        })
    }
}

struct WeightView<'a> {
    values: &'a [f32],
    width: usize,
    spectral: bool,
}

impl WeightView<'_> {
    fn at(&self, row: usize, cell: usize, correlation: usize) -> f32 {
        self.values[row * self.width + if self.spectral { cell } else { correlation }]
    }
}

fn cube_shape<T>(array: &ArrayD<T>) -> MsResult<[usize; 3]> {
    match array.shape() {
        &[correlations, channels, rows] if correlations > 0 && channels > 0 => {
            Ok([correlations, channels, rows])
        }
        _ => Err(input(
            "channel selection requires correlation, channel and row axes",
        )),
    }
}

fn memory<T>(array: &ArrayD<T>) -> MsResult<&[T]> {
    array
        .as_slice_memory_order()
        .ok_or_else(|| input("visibility block is not contiguous"))
}

pub(super) fn axis_info(
    ms: &MeasurementSet,
    ids: &[i32],
    first_row: usize,
    options: VisibilityReadOptions<'_>,
) -> MsResult<RecordValue> {
    use casa_coordinates::StokesType;

    let descriptions = options.legacy_axes.map_or(ids, |axes| axes.descriptions);
    let indices = descriptions
        .iter()
        .copied()
        .map(|id| usize::try_from(id).map_err(|_| input("negative DATA_DESC_ID")));
    let dd = ms.data_description()?;
    let first = descriptions
        .get(if options.legacy_axes.is_some() {
            0
        } else {
            first_row
        })
        .ok_or_else(|| input("no data descriptions in selection"))?;
    let labels = options.legacy_axes.and_then(|axes| axes.correlation_labels);
    let correlations = if let Some(labels) = labels {
        labels.to_vec()
    } else if let Some(polarization) = options.polarization {
        polarization
            .types(*first)?
            .iter()
            .map(|name| name.name().to_owned())
            .collect()
    } else {
        let first = usize::try_from(*first).map_err(|_| input("negative DATA_DESC_ID"))?;
        let pol_id = usize::try_from(dd.polarization_id(first)?)
            .map_err(|_| input("negative POLARIZATION_ID"))?;
        ms.polarization()?
            .corr_type(pol_id)?
            .into_iter()
            .map(|code| {
                StokesType::from_code(code)
                    .map_or("??", StokesType::name)
                    .to_owned()
            })
            .collect()
    };
    let descriptions = indices.collect::<MsResult<BTreeSet<_>>>()?;
    let windows = descriptions
        .into_iter()
        .map(|id| dd.spectral_window_id(id))
        .collect::<MsResult<Vec<_>>>()?;
    let spw = ms.spectral_window()?;
    let mut lengths = None;
    let mut frequencies = Vec::new();
    let mut resolution = Vec::new();
    for &window in &windows {
        let index = usize::try_from(window).map_err(|_| input("negative SPECTRAL_WINDOW_ID"))?;
        let mut freq = spw.chan_freq(index)?;
        let mut res = spw.resolution(index)?;
        if let Some(channels) = options.channels {
            res = if options.legacy_axes.is_some() {
                channels.legacy_resolution(&freq, &res)?
            } else {
                channels.coordinates(&res)?
            };
            freq = channels.coordinates(&freq)?;
        }
        if freq.len() != res.len() || lengths.is_some_and(|n| n != freq.len()) {
            return Err(input("spectral windows have different channel counts"));
        }
        lengths = Some(freq.len());
        frequencies.extend(freq);
        resolution.extend(res);
    }
    let shape = [lengths.unwrap_or(0), windows.len()];
    let freq = RecordValue::new(vec![
        RecordField::new(
            "chan_freq",
            Value::Array(ArrayValue::Float64(shaped(&shape, frequencies)?)),
        ),
        RecordField::new(
            "resolution",
            Value::Array(ArrayValue::Float64(shaped(&shape, resolution)?)),
        ),
    ]);
    Ok(RecordValue::new(vec![
        RecordField::new(
            "corr_axis",
            Value::Array(ArrayValue::String(vector(correlations))),
        ),
        RecordField::new("freq_axis", Value::Record(freq)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_and_overlap() {
        let view = VisibilityChannels::new(2, 0, 3, 1, 4).unwrap();
        assert_eq!(view.count(), 2);
        assert_eq!(view.width(), 3);
        assert_eq!(view.ranges().collect::<Vec<_>>(), [0..3, 1..4]);
        assert!(VisibilityChannels::new(3, 0, 1, 2, 4).is_none());
        assert!(VisibilityChannels::new(1, 4, 1, 1, 4).is_none());
        assert!(VisibilityChannels::new(0, 0, 1, 1, 4).is_none());
        assert!(VisibilityChannels::new(1, 0, 0, 1, 4).is_none());
        assert!(VisibilityChannels::new(i64::MAX, 0, 1, 4, 4).is_none());
    }
}
