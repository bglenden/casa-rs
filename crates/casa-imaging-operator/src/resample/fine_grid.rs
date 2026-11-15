// SPDX-License-Identifier: LGPL-3.0-or-later

use super::{Order, RowSpectrum, Sampling, SpectralAxis, SpectralResampler};

impl SpectralAxis {
    /// At an exact integral width ratio above one, use the fine-grid count
    /// from below. This resolves CASA's TOPO round-trip width discontinuity
    /// for the LSRK source grid without changing interpolation bounds.
    #[must_use]
    pub const fn with_exact_width_from_below(mut self) -> Self {
        self.exact_width_from_below = true;
        self
    }

    pub(super) fn fine_samples_per_plane(self, width: f64) -> usize {
        let whole = width.floor();
        if self.exact_width_from_below && width > 1.0 && width == whole {
            (whole as usize).saturating_sub(1)
        } else {
            (whole as usize).max(1)
        }
    }
}

/// CASA's fine frequency grid for linear interpolation: the output centres
/// when output channels are no wider than native ones, otherwise
/// `floor(width ratio)` points per output channel.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct FineGrid {
    start_hz: f64,
    pub(super) increment_hz: f64,
    per_output: usize,
    direct_centres: bool,
    output: SpectralAxis,
}

impl FineGrid {
    pub(super) fn compile(output: SpectralAxis, native_increment_hz: f64) -> Option<Self> {
        let output_increment_hz = output.increment_hz;
        if native_increment_hz == 0.0 || !native_increment_hz.is_finite() {
            return None;
        }
        let width = output_increment_hz.abs() / native_increment_hz.abs();
        if width <= 1.0 {
            let increment_hz = output_increment_hz.abs().copysign(native_increment_hz);
            let start_hz = if increment_hz.signum() == output_increment_hz.signum() {
                output.first_hz
            } else {
                output.last_hz()
            };
            return Some(Self {
                start_hz,
                increment_hz,
                per_output: 1,
                direct_centres: output.reporting_axis.is_some(),
                output,
            });
        }
        let per_output = output.fine_samples_per_plane(width);
        let fine_abs = output_increment_hz.abs() / per_output as f64;
        let first_edge = output.first_hz - output_increment_hz / 2.0;
        let last_edge = output.last_hz() + output_increment_hz / 2.0;
        let low_edge = first_edge.min(last_edge);
        let high_edge = first_edge.max(last_edge);
        let increment_hz = fine_abs.copysign(native_increment_hz);
        let start_hz = if increment_hz > 0.0 {
            low_edge + fine_abs / 2.0
        } else {
            high_edge - fine_abs / 2.0
        };
        Some(Self {
            start_hz,
            increment_hz,
            per_output,
            direct_centres: false,
            output,
        })
    }

    pub(super) fn count(self) -> usize {
        self.per_output * self.output.channels as usize
    }

    pub(super) fn frequency_hz(self, ordinal: usize) -> f64 {
        if self.direct_centres {
            self.output.centre_hz(self.output_channel(ordinal))
        } else {
            self.start_hz + ordinal as f64 * self.increment_hz
        }
    }

    pub(super) fn output_channel(self, ordinal: usize) -> u32 {
        let output_ordinal = (ordinal / self.per_output) as u32;
        if self.increment_hz.signum() == self.output.increment_hz.signum() {
            output_ordinal
        } else {
            self.output.channels - 1 - output_ordinal
        }
    }
}

impl SpectralResampler {
    /// The frequency list a mosaic uses to choose beam cells after CASA's
    /// `FTMachine::interpolateFrequencyTogrid`. This includes the padding
    /// on a fine grid even though those frequencies never reach an image
    /// plane. `native` already holds the buffer channels in the gridding
    /// frame, including the Doppler shift for a tracked cube.
    #[must_use]
    pub fn mosaic_buffer_frequencies(&self, native: Vec<f64>) -> Vec<f64> {
        let Sampling::Interpolated(output, order) = self.sampling else {
            return native;
        };
        if output.channels() <= 1 || native.len() <= 1 {
            return native;
        }
        let output = self.gridding_axis(output);
        let mut centres: Vec<_> = (0..output.channels())
            .map(|channel| output.centre_hz(channel))
            .collect();
        let output_step = output.increment_hz().abs();
        let native_step = native[1] - native[0];
        let width = output_step / native_step.abs();
        if width <= 1.0 || native_step == 0.0 {
            return centres;
        }
        let ends = |values: &[f64]| {
            let first = values[0];
            let last = values[values.len() - 1];
            [first.min(last), first.max(last)]
        };
        let [data_min, data_max] = ends(&native);
        let [image_min, image_max] = ends(&centres);
        if image_min - output_step / 2.0 > data_max || image_max + output_step / 2.0 < data_min {
            return centres;
        }

        let ascending = output.increment_hz() > 0.0;
        let bracket = |hz| {
            let index = centres
                .partition_point(|&centre| if ascending { centre < hz } else { centre > hz });
            centres.get(index).copied()
        };
        let low = if image_min < data_min {
            bracket(data_min).unwrap_or(image_min)
        } else {
            image_min
        };
        let high = if image_max > data_max {
            bracket(data_max).unwrap_or(image_max)
        } else {
            image_max
        };
        let fine = output_step / output.fine_samples_per_plane(width) as f64;
        let count = ((high - low + output_step) / fine).ceil() as usize + 2;
        let step = fine.copysign(native_step);
        let padding = match order {
            Order::Linear => step,
            Order::Cubic => 2.0 * step,
        };
        let edge = ((output_step - fine) / 2.0).copysign(step);
        let mut frequency = if step > 0.0 { low } else { high } - padding - edge;
        centres.clear();
        centres.reserve_exact(count);
        for _ in 0..count {
            centres.push(frequency);
            frequency += step;
        }
        centres
    }
}

/// The most fine-grid points a row of `spectrum` visits against an output
/// axis of `channels` channels `increment_hz` apart.
///
/// [`FineGrid`] takes `p = max(1, floor(|width| / first spacing))` points
/// per output channel at most, `|width| / p` apart. A row visits each point at
/// most once and only within its span: at most `p · channels` points and
/// `floor(span / spacing) + 1` of them, plus one for rounding. For evenly
/// spaced channels that is about one per channel or per output plane. A
/// one-channel axis or row maps like `nearest`, one sample per channel.
pub(super) fn linear_samples(increment_hz: f64, channels: usize, spectrum: RowSpectrum) -> usize {
    if channels <= 1 || spectrum.channels <= 1 {
        return spectrum.channels;
    }
    if spectrum.first_spacing_hz <= 0.0 {
        return spectrum.channels.max(channels) + 1;
    }
    let width = increment_hz.abs();
    let per_output = (width / spectrum.first_spacing_hz).floor().max(1.0);
    let within = (spectrum.span_hz * per_output / width).floor() + 2.0;
    // Saturating: a bound past the address space fails its allocation.
    (per_output * channels as f64).min(within) as usize
}

#[cfg(test)]
mod tests {
    use super::super::SpectralKernel;
    use super::*;

    #[test]
    fn mosaic_beam_frequencies_follow_the_buffer_and_interpolation_grid() {
        let native = vec![100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0];
        let axis = SpectralAxis::new(102.0, 2.0, 2).unwrap();
        let buffers = |kernel| {
            SpectralResampler::channel_local(axis, kernel).mosaic_buffer_frequencies(native.clone())
        };
        // FTMachine bypasses interpolation for nearest and, for wider
        // interpolated planes, includes the linear or cubic guard channels.
        assert_eq!(buffers(SpectralKernel::Nearest), native);
        assert_eq!(
            buffers(SpectralKernel::Linear),
            [100.5, 101.5, 102.5, 103.5, 104.5, 105.5],
        );
        assert_eq!(
            buffers(SpectralKernel::Cubic),
            [99.5, 100.5, 101.5, 102.5, 103.5, 104.5],
        );
        let narrower = SpectralAxis::new(102.25, 0.5, 3).unwrap();
        assert_eq!(
            SpectralResampler::channel_local(narrower, SpectralKernel::Linear)
                .mosaic_buffer_frequencies(native.clone()),
            [102.25, 102.75, 103.25],
        );
        let one_plane = SpectralAxis::new(102.0, 2.0, 1).unwrap();
        assert_eq!(
            SpectralResampler::channel_local(one_plane, SpectralKernel::Cubic)
                .mosaic_buffer_frequencies(native.clone()),
            native,
        );
        let descending = native.into_iter().rev().collect();
        assert_eq!(
            SpectralResampler::channel_local(axis, SpectralKernel::Linear)
                .mosaic_buffer_frequencies(descending),
            [105.5, 104.5, 103.5, 102.5, 101.5, 100.5],
        );
    }
}
