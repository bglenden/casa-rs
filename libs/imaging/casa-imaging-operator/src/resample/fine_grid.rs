// SPDX-License-Identifier: LGPL-3.0-or-later

use super::{Order, RowSpectrum, Sampling, SpectralAxis, SpectralResampler};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IntegralWidths {
    Floor,
    FromBelow,
    AtCoordinatePrecision,
}

impl SpectralAxis {
    /// At an exact integral width ratio above one, use the fine-grid count
    /// from below. This resolves CASA's TOPO round-trip width discontinuity
    /// for the LSRK source grid without changing interpolation bounds.
    #[must_use]
    pub const fn with_exact_width_from_below(mut self) -> Self {
        self.integral_widths = IntegralWidths::FromBelow;
        self
    }

    /// On a linear axis, use the corresponding integer fine-grid count
    /// when represented channel centres cannot distinguish the requested
    /// width from that many native intervals. Source-frame cubes use this
    /// to avoid changing the count when shifted channel centres are rounded.
    #[must_use]
    pub const fn with_integral_width_at_coordinate_precision(mut self) -> Self {
        if self.tabular.is_none() {
            self.integral_widths = IntegralWidths::AtCoordinatePrecision;
        }
        self
    }

    pub(super) fn fine_samples_per_plane(
        &self,
        width: f64,
        native_step: f64,
        native_pair: [f64; 2],
    ) -> usize {
        let whole = width.floor();
        match self.integral_widths {
            IntegralWidths::FromBelow if width > 1.0 && width == whole => {
                (whole as usize).saturating_sub(1)
            }
            IntegralWidths::AtCoordinatePrecision => {
                let integer = width.round();
                if integer > whole && integer > 1.0 {
                    // One representable step at each of the two native and
                    // two output centres. Compare in native-width units.
                    let native_precision =
                        coordinate_ulp(native_pair[0]) + coordinate_ulp(native_pair[1]);
                    let output_precision = coordinate_ulp(self.first_hz)
                        + coordinate_ulp(self.first_hz + self.increment_hz);
                    let nominal_step = self.increment_hz.abs() / integer;
                    if (nominal_step - native_step.abs()).abs()
                        <= native_precision + output_precision / integer
                    {
                        return integer as usize;
                    }
                }
                (whole as usize).max(1)
            }
            _ => (whole as usize).max(1),
        }
    }
}

fn coordinate_ulp(hz: f64) -> f64 {
    let magnitude = hz.abs();
    magnitude.next_up() - magnitude
}

/// CASA's fine frequency grid for linear interpolation: the output centres
/// when output channels are no wider than native ones, otherwise
/// `floor(width ratio)` points per output channel.
pub(super) struct FineGrid<'a> {
    start_hz: f64,
    pub(super) increment_hz: f64,
    per_output: usize,
    direct_centres: bool,
    output: &'a SpectralAxis,
    tabular_fine: Option<ClippedFineGrid>,
}

impl<'a> FineGrid<'a> {
    pub(super) fn compile(
        output: &'a SpectralAxis,
        native: &[f64],
        order: Order,
        selection_centres_hz: Option<[f64; 2]>,
    ) -> Option<Self> {
        let pair = selection_centres_hz.unwrap_or_else(|| [native[0], native[1]]);
        let native_increment_hz = pair[1] - pair[0];
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
                direct_centres: output.reporting_axis.is_some() || output.tabular.is_some(),
                output,
                tabular_fine: None,
            });
        }
        let per_output = output.fine_samples_per_plane(width, native_increment_hz, pair);
        let fine_abs = output_increment_hz.abs() / per_output as f64;
        if output.tabular.is_some() {
            let step = fine_abs.copysign(native_increment_hz);
            let fine = ClippedFineGrid::compile(output, native, step, order)?;
            return Some(Self {
                start_hz: fine.current_hz,
                increment_hz: step,
                per_output,
                direct_centres: false,
                output,
                tabular_fine: Some(fine),
            });
        }
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
            tabular_fine: None,
        })
    }

    pub(super) fn count(&self) -> usize {
        self.tabular_fine
            .as_ref()
            .map_or(self.per_output * self.output.channels as usize, |fine| {
                fine.count
            })
    }

    pub(super) fn frequency_hz(&mut self, ordinal: usize) -> f64 {
        if let Some(fine) = &mut self.tabular_fine {
            // CASA builds the frequencies by repeated addition.
            while fine.ordinal < ordinal {
                fine.current_hz += self.increment_hz;
                fine.ordinal += 1;
            }
            fine.current_hz
        } else if self.direct_centres {
            self.output.centre_hz(self.uniform_output_channel(ordinal))
        } else {
            self.start_hz + ordinal as f64 * self.increment_hz
        }
    }

    pub(super) fn output_channel(&self, ordinal: usize, frequency_hz: f64) -> Option<u32> {
        if self.tabular_fine.is_some() {
            return self.output.last_tabular_window(frequency_hz);
        }
        Some(self.uniform_output_channel(ordinal))
    }

    fn uniform_output_channel(&self, ordinal: usize) -> u32 {
        let output_ordinal = (ordinal / self.per_output) as u32;
        if self.increment_hz.signum() == self.output.increment_hz.signum() {
            output_ordinal
        } else {
            self.output.channels - 1 - output_ordinal
        }
    }
}

/// The row-local portion of CASA's finer interpolation grid. This uses
/// only its current point and visits the points in frequency order.
struct ClippedFineGrid {
    current_hz: f64,
    ordinal: usize,
    count: usize,
}

impl ClippedFineGrid {
    fn compile(output: &SpectralAxis, native: &[f64], step: f64, order: Order) -> Option<Self> {
        let first = native[0];
        let last = native[native.len() - 1];
        let [data_min, data_max] = [first.min(last), first.max(last)];
        let first = output.first_hz();
        let last = output.last_hz();
        let [image_min, image_max] = [first.min(last), first.max(last)];
        let image_step = output.increment_hz.abs();
        if image_min - image_step / 2.0 > data_max || image_max + image_step / 2.0 < data_min {
            return None;
        }
        let bracket = |hz| {
            let index = output.partition_centres(|centre| {
                if output.increment_hz > 0.0 {
                    centre < hz
                } else {
                    centre > hz
                }
            });
            (index < output.channels).then(|| output.centre_hz(index))
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
        let count = ((high - low + image_step) / step.abs()).ceil() as usize + 2;
        let padding = match order {
            Order::Linear => step,
            Order::Cubic => 2.0 * step,
        };
        let edge = (image_step / 2.0 - step.abs() / 2.0).copysign(step);
        let current_hz = if step > 0.0 { low } else { high } - padding - edge;
        Some(Self {
            current_hz,
            ordinal: 0,
            count,
        })
    }
}

impl SpectralResampler {
    /// The frequency list a mosaic uses to choose beam cells after CASA's
    /// `FTMachine::interpolateFrequencyTogrid`. This includes the padding
    /// on a fine grid even though those frequencies never reach an image
    /// plane. `native` already holds the buffer channels in the gridding
    /// frame, including the Doppler shift for a tracked cube. Pass the
    /// converted first selected pair if its centres lie outside the read.
    #[must_use]
    pub fn mosaic_buffer_frequencies(
        &self,
        native: Vec<f64>,
        selection_centres_hz: Option<[f64; 2]>,
    ) -> Vec<f64> {
        let Sampling::Interpolated(output, order) = &self.sampling else {
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
        let pair = selection_centres_hz.unwrap_or_else(|| [native[0], native[1]]);
        let native_step = pair[1] - pair[0];
        let width = output_step / native_step.abs();
        if width <= 1.0 || native_step == 0.0 {
            return centres;
        }
        let count = output.fine_samples_per_plane(width, native_step, pair);
        let step = (output_step / count as f64).copysign(native_step);
        let Some(fine) = ClippedFineGrid::compile(&output, &native, step, *order) else {
            return centres;
        };
        centres.clear();
        centres.reserve_exact(fine.count);
        let mut frequency = fine.current_hz;
        for _ in 0..fine.count {
            centres.push(frequency);
            frequency += step;
        }
        centres
    }
}

/// The most fine-grid points a row of `spectrum` visits against an output
/// axis of `channels` channels `increment_hz` apart.
///
/// A row visits each fine point at most once and only within its span.
impl SpectralAxis {
    /// A bound of `p · channels` points for `p` fine samples per plane,
    /// and `floor(span / spacing) + 1` points in the native span, plus one
    /// for rounding. One-channel axes and rows map like nearest.
    pub(super) fn linear_samples(&self, channels: usize, spectrum: RowSpectrum) -> usize {
        if channels <= 1 || spectrum.channels <= 1 {
            return spectrum.channels;
        }
        if spectrum.first_spacing_hz <= 0.0 {
            return spectrum.channels.max(channels) + 1;
        }
        let width = self.increment_hz.abs();
        let ratio = width / spectrum.first_spacing_hz;
        let per_output = if self.integral_widths == IntegralWidths::AtCoordinatePrecision {
            // A carried pair can lie outside the read span. Charge its
            // possible integer count without assuming its coordinate precision.
            ratio.ceil().max(1.0)
        } else {
            ratio.floor().max(1.0)
        };
        let within = (spectrum.span_hz * per_output / width).floor() + 2.0;
        // Saturating: a bound past the address space fails its allocation.
        (per_output * channels as f64).min(within) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::super::SpectralKernel;
    use super::*;

    #[test]
    fn a_restricted_wave_uses_the_precision_of_the_selected_pair() {
        let axis = SpectralAxis::new(1_065_741_824.0, 2_999_999.999_998_569_5, 16)
            .unwrap()
            .with_integral_width_at_coordinate_precision();
        let full = [1_041_741_824.0, 1_042_741_824.0];
        let wave = [1_081_741_824.0, 1_082_741_824.0];
        for order in [Order::Linear, Order::Cubic] {
            let grid = |native, pair| {
                FineGrid::compile(&axis, native, order, pair)
                    .unwrap()
                    .per_output
            };
            assert_eq!(grid(&full, None), 2);
            assert_eq!(grid(&wave, Some(full)), 2);
            assert_eq!(grid(&wave, None), 3);
        }
    }

    #[test]
    fn mosaic_beam_frequencies_follow_the_buffer_and_interpolation_grid() {
        let native = vec![100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0];
        let axis = SpectralAxis::new(102.0, 2.0, 2).unwrap();
        let buffers = |kernel| {
            SpectralResampler::channel_local(axis.clone(), kernel)
                .mosaic_buffer_frequencies(native.clone(), None)
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
                .mosaic_buffer_frequencies(native.clone(), None),
            [102.25, 102.75, 103.25],
        );
        let one_plane = SpectralAxis::new(102.0, 2.0, 1).unwrap();
        assert_eq!(
            SpectralResampler::channel_local(one_plane, SpectralKernel::Cubic)
                .mosaic_buffer_frequencies(native.clone(), None),
            native,
        );
        let descending = native.into_iter().rev().collect();
        assert_eq!(
            SpectralResampler::channel_local(axis, SpectralKernel::Linear)
                .mosaic_buffer_frequencies(descending, None),
            [105.5, 104.5, 103.5, 102.5, 101.5, 100.5],
        );
    }
    #[test]
    fn source_frame_bin_count_is_stable_at_channel_coordinate_precision() {
        let first_hz = 261_766_258_984.723_08;
        let bin_hz = 732_454.026_062_011_7;
        let first = 261_765_770_681.928_28_f64;
        let second = 261_766_014_833.270_3_f64;
        let centres = [first, second];
        let shifted = [first, second.next_up()];
        let axis = SpectralAxis::new(first_hz, bin_hz, 4).unwrap();
        let stable = axis.clone().with_integral_width_at_coordinate_precision();
        let row = RowSpectrum {
            channels: 12,
            first_spacing_hz: shifted[1] - shifted[0],
            span_hz: 11.0 * (shifted[1] - shifted[0]),
        };
        assert_eq!(axis.samples_per_row(row), 8);
        assert_eq!(stable.samples_per_row(row), 12);
        for pair in [centres, shifted] {
            let counts = |target: &SpectralAxis, order| {
                FineGrid::compile(target, &pair, order, None)
                    .unwrap()
                    .per_output
            };
            for order in [Order::Linear, Order::Cubic] {
                assert_eq!(counts(&axis, order), if pair == centres { 3 } else { 2 });
                assert_eq!(counts(&stable, order), 3);
                for (width, expected) in [(731_000.0, 2), (734_000.0, 3)] {
                    let control = SpectralAxis::new(first_hz, width, 4)
                        .unwrap()
                        .with_integral_width_at_coordinate_precision();
                    assert_eq!(counts(&control, order), expected);
                }
            }
        }
    }
}
