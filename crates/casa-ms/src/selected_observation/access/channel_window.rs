// SPDX-License-Identifier: LGPL-3.0-or-later

//! The stored channels a wave reads: the window of a source's channels whose
//! samples, converted to the output frame row by row, reach the wave's
//! frequency bounds, with both linear-interpolation partners of each.

use casa_imaging_model::CompiledProblem;

use super::super::spectral_evaluation::prepare_row_frequency_conversion;
use super::{BoundObservationSource, BoundObservationSourceError, SelectedCoordinates};
use crate::spectral_selection::PreparedFrequencyFrameConversion;
use crate::{SelectedObservationRow, VisibilityChannelReadRange};

pub(super) fn selected_channel_window(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    coordinates: &SelectedCoordinates,
    rows: &[SelectedObservationRow],
    frequency_bounds_hz: [f64; 2],
) -> Result<Option<(VisibilityChannelReadRange, std::ops::Range<usize>)>, BoundObservationSourceError>
{
    let lower_bound = frequency_bounds_hz[0].min(frequency_bounds_hz[1]);
    let upper_bound = frequency_bounds_hz[0].max(frequency_bounds_hz[1]);
    if !lower_bound.is_finite() || !upper_bound.is_finite() || lower_bound == upper_bound {
        return Err(BoundObservationSourceError::SpectralContributionMismatch);
    }
    let output_frame = problem.geometry().spectral().output_frame();
    let source_frame = coordinates
        .channels
        .first()
        .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?
        .frame;
    let mut admitted_first: Option<usize> = None;
    let mut admitted_last: Option<usize> = None;
    let mut cached_conversion: Option<((i32, u64), PreparedFrequencyFrameConversion)> = None;
    for row in rows.iter().copied() {
        let normalized_time = row.time_mjd_seconds / 86_400.0 * 86_400.0;
        let context_key = (row.field_id, normalized_time.to_bits());
        let conversion = if let Some((cached_key, conversion)) = cached_conversion
            && cached_key == context_key
        {
            conversion
        } else {
            let conversion = prepare_row_frequency_conversion(
                source.geometry_engine(),
                row.field_id,
                normalized_time,
                source_frame,
                output_frame,
            )?;
            cached_conversion = Some((context_key, conversion));
            conversion
        };
        let mut row_window = SelectedChannelWindowSelection::new(lower_bound, upper_bound);
        for (ordinal, channel) in coordinates.channels.iter().copied().enumerate() {
            let centre = conversion.convert_hz(channel.centre_hz);
            if !centre.is_finite() || centre <= 0.0 {
                return Err(BoundObservationSourceError::SpectralContributionMismatch);
            }
            row_window.observe(ordinal, centre);
        }
        if let Some(window) = row_window.finish() {
            let first = window.start;
            let last = window.end - 1;
            admitted_first = Some(admitted_first.map_or(first, |current| current.min(first)));
            admitted_last = Some(admitted_last.map_or(last, |current| current.max(last)));
        }
    }
    let (Some(admitted_first), Some(admitted_last)) = (admitted_first, admitted_last) else {
        return Ok(None);
    };
    let mut physical_start = usize::MAX;
    let mut physical_end = 0_usize;
    for ordinal in admitted_first..=admitted_last {
        let channel = coordinates
            .channels
            .get(ordinal)
            .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?;
        let index = usize::try_from(channel.channel_index)
            .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
        physical_start = physical_start.min(index);
        physical_end = physical_end.max(
            index
                .checked_add(1)
                .ok_or(BoundObservationSourceError::MeasurementOverflow)?,
        );
    }
    Ok(Some((
        VisibilityChannelReadRange::new(physical_start, physical_end - physical_start),
        admitted_first..admitted_last + 1,
    )))
}

#[derive(Clone, Copy)]
struct SelectedChannelWindowSelection {
    lower_bound: f64,
    upper_bound: f64,
    first: Option<usize>,
    last: Option<usize>,
    previous: Option<(usize, f64)>,
}

impl SelectedChannelWindowSelection {
    const fn new(lower_bound: f64, upper_bound: f64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            first: None,
            last: None,
            previous: None,
        }
    }

    fn observe(&mut self, ordinal: usize, centre: f64) {
        if (self.lower_bound..=self.upper_bound).contains(&centre) {
            self.include(ordinal, ordinal);
        }
        if let Some((previous_ordinal, previous_centre)) = self.previous
            && previous_centre.min(centre) <= self.upper_bound
            && previous_centre.max(centre) >= self.lower_bound
        {
            self.include(previous_ordinal.min(ordinal), previous_ordinal.max(ordinal));
        }
        self.previous = Some((ordinal, centre));
    }

    fn include(&mut self, first: usize, last: usize) {
        self.first = Some(self.first.map_or(first, |current| current.min(first)));
        self.last = Some(self.last.map_or(last, |current| current.max(last)));
    }

    fn finish(self) -> Option<std::ops::Range<usize>> {
        let first = self.first?;
        let last = self.last?;
        Some(first..last + 1)
    }
}

#[cfg(test)]
mod selected_channel_window_tests {
    use super::SelectedChannelWindowSelection;

    fn select(centres: &[f64], bounds: [f64; 2]) -> Option<std::ops::Range<usize>> {
        let lower = bounds[0].min(bounds[1]);
        let upper = bounds[0].max(bounds[1]);
        let mut selection = SelectedChannelWindowSelection::new(lower, upper);
        for (ordinal, centre) in centres.iter().copied().enumerate() {
            selection.observe(ordinal, centre);
        }
        selection.finish()
    }

    #[test]
    fn adjacent_pair_support_handles_descending_and_gapped_centres() {
        assert_eq!(select(&[4.0, 3.0, 2.0, 1.0], [2.4, 3.6]), Some(0..3));
        assert_eq!(
            select(&[100.0, 111.0, 150.0, 220.0], [120.0, 145.0]),
            Some(1..3)
        );
    }

    #[test]
    fn selection_does_not_add_a_channel_beyond_the_intersecting_pair() {
        assert_eq!(select(&[0.0, 10.0, 20.0], [1.0, 9.0]), Some(0..2));
        assert_eq!(select(&[0.0, 10.0, 20.0], [9.0, 11.0]), Some(0..3));
        assert_eq!(select(&[0.0, 10.0, 20.0], [30.0, 40.0]), None);
        assert_eq!(select(&[10.0], [9.0, 11.0]), Some(0..1));
    }
}
