// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::Arc;

use super::{OperatorError, RowSpectrum, SpectralAxis};

#[derive(Clone, Debug, PartialEq)]
pub(super) struct TabularAxis {
    centres_hz: Arc<[f64]>,
    reference_increment_hz: f64,
    world_offset_hz: f64,
}

impl TabularAxis {
    pub(super) fn centre_hz(&self, channel: u32) -> f64 {
        let channel = channel as usize;
        let centres = &self.centres_hz;
        let last = centres.len() - 1;
        let centre = if last == 0 {
            centres[0]
        } else {
            centres.get(channel).copied().unwrap_or_else(|| {
                let step = centres[last] - centres[last - 1];
                centres[last] + (channel - last) as f64 * step
            })
        };
        centre + self.world_offset_hz
    }

    /// `TabularCoordinate::setReferencePixel` shifts the world value at
    /// each pixel by the nominal increment times the reference-pixel shift.
    /// The lookup's pixel indices stay in place.
    pub(super) fn shift_reference_pixel(&mut self, pixels: u32) {
        self.world_offset_hz -= f64::from(pixels) * self.reference_increment_hz;
    }

    pub(super) fn min_spacing_hz(&self) -> f64 {
        self.centres_hz
            .windows(2)
            .map(|pair| (pair[1] - pair[0]).abs())
            .fold(f64::INFINITY, f64::min)
    }

    pub(super) fn uniform_drift_hz(&self) -> f64 {
        let first = self.centres_hz[0];
        let step = self.centres_hz[1] - first;
        self.centres_hz
            .iter()
            .enumerate()
            .map(|(plane, centre)| (first + plane as f64 * step - centre).abs())
            .fold(0.0, f64::max)
    }
}

impl SpectralAxis {
    /// A tabular frequency coordinate with the evaluated centre of each
    /// image plane. CASA sets interpolation spacing from the first two
    /// centres. Its channel subimage rebuilds the table from the evaluated
    /// planes (`CoordinateSystem::subImageInSitu`), whose mean increment
    /// the density template uses when it shifts the reference pixel.
    pub fn tabular(centres_hz: &[f64], increment_hz: f64) -> Result<Self, OperatorError> {
        let channels =
            u32::try_from(centres_hz.len()).map_err(|_| OperatorError::SpectralAxis {
                reason: "the frequency table has too many channels",
            })?;
        let mut axis = Self::new(
            centres_hz.first().copied().unwrap_or_default(),
            increment_hz,
            channels,
        )?;
        if centres_hz.windows(2).any(|pair| {
            !pair[1].is_finite()
                || pair[1] <= 0.0
                || (pair[1] - pair[0]).signum() != increment_hz.signum()
                || pair[1] == pair[0]
        }) {
            return Err(OperatorError::SpectralAxis {
                reason: "tabular frequencies must be positive and strictly monotonic",
            });
        }
        let last = centres_hz.len() - 1;
        let reference_increment_hz = (centres_hz[last] - centres_hz[0]) / last.max(1) as f64;
        axis.tabular = Some(TabularAxis {
            centres_hz: centres_hz.into(),
            reference_increment_hz,
            world_offset_hz: 0.0,
        });
        Ok(axis)
    }

    /// The first plane for which `before` is false, including planes
    /// reached by extrapolating the original table.
    pub(super) fn partition_centres(&self, mut before: impl FnMut(f64) -> bool) -> u32 {
        let (mut first, mut past) = (0, self.channels);
        while first < past {
            let middle = first + (past - first) / 2;
            if before(self.centre_hz(middle)) {
                first = middle + 1;
            } else {
                past = middle;
            }
        }
        first
    }

    /// Invert the piecewise-linear axis, extrapolating along its end intervals.
    pub(super) fn tabular_pixel(&self, hz: f64) -> Option<f64> {
        let table = self.tabular.as_ref()?;
        let centres = &table.centres_hz;
        if centres.len() == 1 {
            return Some(0.0);
        }
        let hz = hz - table.world_offset_hz;
        let index = centres.partition_point(|&centre| {
            if self.increment_hz > 0.0 {
                centre < hz
            } else {
                centre > hz
            }
        });
        let left = index.saturating_sub(1).min(centres.len() - 2);
        let fraction = (hz - centres[left]) / (centres[left + 1] - centres[left]);
        Some(left as f64 + fraction)
    }

    /// `FTMachine::interpolateFrequencyTogrid` gives a fine point to the
    /// last image channel whose half-open window contains it. Every window
    /// has the width of the first frequency interval.
    pub(super) fn last_tabular_window(&self, hz: f64) -> Option<u32> {
        let half_step = self.increment_hz.abs() / 2.0;
        let end = self.partition_centres(|centre| {
            if self.increment_hz > 0.0 {
                centre - half_step <= hz
            } else {
                centre + half_step > hz
            }
        });
        let last = end.checked_sub(1)?;
        let centre = self.centre_hz(last);
        (centre - half_step <= hz && hz < centre + half_step).then_some(last)
    }

    /// Bound either CASA's direct centres or its fine-grid points within
    /// the native span. The table can have unequal adjacent intervals.
    pub(super) fn samples_per_row(&self, row: RowSpectrum) -> usize {
        let Some(table) = &self.tabular else {
            return self.linear_samples(self.channels as usize, row);
        };
        if self.channels <= 1 || row.channels <= 1 {
            return row.channels;
        }
        let width = self.increment_hz.abs();
        if width == 0.0 {
            // A padded constant coordinate repeats the same sample once
            // for each density plane when interpolation is active.
            return self.channels as usize;
        }
        let axis_span = (self.last_hz() - self.first_hz).abs();
        let direct = (self.channels as usize)
            .min(((row.span_hz / table.min_spacing_hz()).floor() + 2.0) as usize);
        if row.first_spacing_hz <= 0.0 {
            // An unnamed width uses the existing unmeasured-spectrum
            // convention. Measured native rows always report their spacing.
            return row.channels.max(self.channels as usize) + 1;
        }
        let points_per_plane = (width / row.first_spacing_hz).floor().max(1.0);
        let fine_spacing = width / points_per_plane;
        let within = (row.span_hz / fine_spacing).floor() + 2.0;
        let full = ((axis_span + width) / fine_spacing).ceil() + 2.0;
        direct.max(within.min(full) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{FineGrid, Order};
    use super::*;

    #[test]
    fn nonlinear_channel_mapping_inverts_the_table_and_uses_extrapolated_halo() {
        let axis = SpectralAxis::tabular(&[140.0, 120.0, 107.0, 100.0], -20.0).unwrap();
        assert_eq!(axis.tabular_pixel(109.0), Some(1.0 + 11.0 / 13.0));
        assert_eq!(axis.nearest_channel(109.0), Some(2));
        assert_eq!(axis.nearest_channel(97.0), Some(3));
        assert_eq!(axis.nearest_channel(96.0), None);
        assert_eq!(axis.centre_hz(4), 93.0);
        assert!(axis.in_halo(84.0, 5.0, Order::Linear));
        assert!(!axis.in_halo(82.0, 5.0, Order::Linear));
    }

    #[test]
    fn the_last_half_open_frequency_window_wins_in_either_order() {
        let descending = SpectralAxis::tabular(&[140.0, 120.0, 107.0, 100.0], -20.0).unwrap();
        assert_eq!(descending.last_tabular_window(115.0), Some(2));
        assert_eq!(descending.last_tabular_window(110.0), Some(2));
        assert_eq!(
            descending.last_tabular_window(110.0_f64.next_down()),
            Some(3)
        );
        let ascending = SpectralAxis::tabular(&[100.0, 120.0, 133.0, 140.0], 20.0).unwrap();
        assert_eq!(ascending.last_tabular_window(130.0), Some(3));
        assert_eq!(ascending.last_tabular_window(150.0), None);
    }

    #[test]
    fn interpolation_uses_centres_directly_or_clips_the_first_interval_fine_grid() {
        let axis = SpectralAxis::tabular(&[140.0, 120.0, 107.0, 100.0], -20.0).unwrap();
        let mut direct =
            FineGrid::compile(&axis, &[90.0, 115.0, 140.0], Order::Linear, None).unwrap();
        let doubles: Vec<_> = (0..direct.count())
            .map(|i| direct.frequency_hz(i))
            .collect();
        assert_eq!(doubles, [100.0, 107.0, 120.0, 140.0]);
        let mut fine =
            FineGrid::compile(&axis, &[108.0, 119.0, 130.0], Order::Linear, None).unwrap();
        let doubles: Vec<_> = (0..fine.count()).map(|i| fine.frequency_hz(i)).collect();
        assert_eq!(doubles, [87.0, 107.0, 127.0, 147.0]);
        assert!(FineGrid::compile(&axis, &[160.0, 170.0], Order::Linear, None).is_none());
    }

    #[test]
    fn fine_points_advance_by_casas_repeated_addition() {
        let centres = [
            1_000_000_000.002_3,
            1_001_000_000.042_3,
            1_001_800_000.047_3,
        ];
        let axis = SpectralAxis::tabular(&centres, centres[1] - centres[0]).unwrap();
        let native = [999_000_000.0, 999_300_000.0, 1_003_000_000.0];
        let mut fine = FineGrid::compile(&axis, &native, Order::Linear, None).unwrap();
        assert_eq!(fine.frequency_hz(7), 1_001_666_666.735_633_5);
    }

    #[test]
    fn a_tabular_wave_keeps_the_model_planes_reached_by_the_uniform_inverse() {
        use crate::{PlaneRange, SpectralKernel, SpectralResampler};
        let centres = [
            1600.0, 1500.0, 1450.0, 1400.0, 1350.0, 1300.0, 1250.0, 1200.0, 1150.0, 1100.0, 1050.0,
        ];
        let axis = SpectralAxis::tabular(&centres, -100.0).unwrap();
        let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Linear);
        let planes = resampler.model_planes(PlaneRange::new(10, 11), 40.0);
        // Output plane 10 samples around 1050 Hz. The two closest slots of
        // CASA's wider-bin prediction grid there are model planes 5 and 6.
        assert!(planes.contains(5) && planes.contains(6));
    }

    #[test]
    fn density_padding_shifts_world_by_the_evaluated_mean_and_extrapolates_the_tail() {
        let axis = SpectralAxis::tabular(&[140.0, 120.0, 107.0, 100.0], -20.0).unwrap();
        let padded = axis.padded(2).unwrap();
        assert_eq!(padded.channels(), 8);
        assert_eq!(padded.first_hz(), 140.0 + 80.0 / 3.0);
        assert_eq!(padded.increment_hz(), -20.0);
        assert_eq!(padded.centre_hz(7), 72.0 + 80.0 / 3.0);
        assert_eq!(padded.nearest_channel(120.0 + 80.0 / 3.0), Some(1));
    }

    #[test]
    fn a_constant_one_entry_table_maps_all_frequencies_to_its_only_plane() {
        let axis = SpectralAxis::tabular(&[120.0], 50.0).unwrap();
        assert_eq!(axis.nearest_channel(140.0), Some(0));
        assert_eq!(axis.nearest_channel(90.0), Some(0));
        assert_eq!(axis.centre_hz(1), 120.0);
    }
}
