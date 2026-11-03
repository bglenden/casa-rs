// SPDX-License-Identifier: LGPL-3.0-or-later

//! A cube's per-time frequency selection: the bands CASA's visibility
//! iterator selects each window's channels by at each time
//! (`vi::FrequencySelectionUsingFrame`), and the channels they select.
//!
//! `SynthesisImagerVi2::selectData` states each channel range `spw` names
//! as a band in the selection's frame, tclean's `outframe`: the range's
//! lowest and highest channel centres converted at every epoch of the
//! window's samples ([`envelope::selection_band`]). A cube is gridded by
//! `CubeMajorCycleAlgorithm`'s own imager, which selects the same way and,
//! where `SynthesisImagerVi2::tuneSelectData` tunes the selection
//! ([`super::spectral`]), narrows each band that the image's LSRK band
//! (pixel −1.5 to `nchan + 0.5`) overlaps to their intersection, adding
//! the image's band to a window none of whose bands it overlaps
//! (`SynthesisImagerVi2::andFreqSelection`). At each time the iterator
//! converts the bands to the window's frame, towards the time's field, and
//! reads the channels whose extent meets one
//! (`VisibilityIteratorImpl2::makeChannelSelectorF`), so where the Doppler
//! shift across an observation is a sizeable part of a channel, the
//! channels a time reads move with it, and a channel range with a stride
//! is read whole.
//!
//! casa-rs departs from CASA in two ways, both recorded in
//! `docs/CASA (C++) bugs.md`: a tuned band is narrowed by the selection's
//! bands in LSRK, where CASA compares numbers in `outframe` with numbers in
//! LSRK; and the bands of one window's channel ranges do not narrow each
//! other where a Doppler shift makes them overlap, as CASA's do.

use std::collections::BTreeMap;

use casa_imaging_model::FrequencyBands;
use casa_ms::derived::engine::MsCalEngine;
use casa_observation::Catalogue;
use casa_types::measures::PreparedFrequencyConversion;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;

use super::PrepareError;
use super::envelope::{epoch_frame, selection_band};
use super::selection::SourceSpectralWindow;
use super::spectral::FrameContext;
use super::survey::Survey;

/// The bands of each surveyed window a cube's gridding selects by, in one
/// frame, before what they select at each epoch is known.
pub(super) struct CubeBands {
    frame: FrequencyRef,
    bands_hz: BTreeMap<usize, Vec<[f64; 2]>>,
}

/// The image's band in LSRK as `SynthesisImager::tuneSelectData` states
/// it: the axis from pixel −1.5 to pixel `channels + 0.5`, its first
/// channel centred at `first_hz` and its channels `increment_hz` apart in
/// frame `output`, converted to LSRK through the image's conversion layer
/// at its anchor (`setSpectralConversion("LSRK")`).
pub(super) fn image_band_lsrk(
    frame: &FrameContext<'_>,
    output: FrequencyRef,
    (first_hz, increment_hz, channels): (f64, f64, usize),
) -> Result<[f64; 2], PrepareError> {
    let factor = if output == FrequencyRef::LSRK {
        1.0
    } else {
        let reference = frame.direction.reference_direction();
        let direction = MDirection::from_angles(
            reference.longitude_rad(),
            reference.latitude_rad(),
            reference.frame().direction_ref(),
        );
        let anchor = frame
            .engine
            .spectral_frame_observatory_direction(frame.anchor_time_mjd_seconds, direction)?;
        PreparedFrequencyConversion::new(output, FrequencyRef::LSRK, Some(&anchor), Some(&anchor))?
            .convert_hz(1.0)
    };
    let ends = [
        first_hz - 1.5 * increment_hz,
        first_hz + (channels as f64 + 0.5) * increment_hz,
    ]
    .map(|hz| hz * factor);
    Ok([ends[0].min(ends[1]), ends[0].max(ends[1])])
}

/// The bands a cube's gridding selects each surveyed window's channels by:
/// those of the channel ranges `spw` names (a window it names without
/// channels is one range) in `outframe`, or, where `tuned_image_band` gives
/// the image's LSRK band, those ranges' bands in LSRK narrowed to it.
/// `None` where CASA selects by channel numbers alone, as for a selection
/// frame of `REST` (`ignoreframe`): the bands then select exactly the
/// named channels at every time.
pub(super) fn cube_bands(
    (catalogue, engine): (&Catalogue, &MsCalEngine),
    survey: &Survey,
    outframe: FrequencyRef,
    tuned_image_band: Option<[f64; 2]>,
) -> Result<Option<CubeBands>, PrepareError> {
    let frame = if tuned_image_band.is_some() {
        FrequencyRef::LSRK
    } else {
        outframe
    };
    if frame == FrequencyRef::REST {
        return Ok(None);
    }
    let mut bands_hz = BTreeMap::new();
    for window in &survey.spectral_windows {
        let whole = [[0, window.frequencies_hz.len().saturating_sub(1)]];
        let ranges = survey
            .spw_ranges
            .get(&window.spw_id)
            .map_or(&whole[..], Vec::as_slice);
        let mut bands = Vec::with_capacity(ranges.len() + 1);
        for range in ranges {
            if let Some(band) =
                selection_band((catalogue, engine), (survey, window), *range, frame)?
            {
                bands.push(band);
            }
        }
        if let Some(image) = tuned_image_band {
            and_band(&mut bands, image);
        }
        bands_hz.insert(window.spw_id, bands);
    }
    Ok(Some(CubeBands { frame, bands_hz }))
}

/// `SynthesisImagerVi2::andFreqSelection` of `[lower, upper]` on one
/// window's bands: each band it overlaps, ends excluded, narrows to their
/// intersection; with none, it is added.
fn and_band(bands: &mut Vec<[f64; 2]>, [lower, upper]: [f64; 2]) {
    let mut overlapped = false;
    for band in bands.iter_mut() {
        if lower < band[1] && upper > band[0] {
            overlapped = true;
            band[0] = band[0].max(lower);
            band[1] = band[1].min(upper);
        }
    }
    if !overlapped {
        bands.push([lower, upper]);
    }
}

/// What a window's bands select over the epochs of its samples: for each
/// channel, at how many epochs, of how many.
pub(super) struct EpochSelection {
    counts: Vec<u32>,
    epochs: u32,
}

impl EpochSelection {
    /// Whether some epoch selects `channel`.
    pub(super) fn ever(&self, channel: usize) -> bool {
        self.counts.get(channel).is_some_and(|count| *count > 0)
    }

    /// Whether every epoch selects `channel`.
    pub(super) fn always(&self, channel: usize) -> bool {
        self.counts
            .get(channel)
            .is_some_and(|count| *count == self.epochs)
    }
}

impl CubeBands {
    /// What `window`'s bands select at each epoch of its samples: at each,
    /// the bands converted to the window's frame towards the epoch's field
    /// ([`epoch_frame`]), as the visibility iterator converts them, select
    /// the channels whose extent meets one. `None` for a window without
    /// bands or epochs.
    pub(super) fn epoch_selection(
        &self,
        (catalogue, engine): (&Catalogue, &MsCalEngine),
        survey: &Survey,
        window: &SourceSpectralWindow,
    ) -> Result<Option<EpochSelection>, PrepareError> {
        let bands = self.window_bands(window.spw_id);
        let epochs = survey.window_epochs(window.spw_id);
        if bands.is_empty() || epochs.is_empty() {
            return Ok(None);
        }
        let source = window.frequency_reference;
        let mut counts = vec![0_u32; window.frequencies_hz.len()];
        for epoch in epochs {
            let factor = if source == self.frame || source == FrequencyRef::REST {
                1.0
            } else {
                let frame = epoch_frame((catalogue, engine), *epoch)?;
                PreparedFrequencyConversion::new(self.frame, source, Some(&frame), Some(&frame))?
                    .convert_hz(1.0)
            };
            for (count, (centre, width)) in counts.iter_mut().zip(
                window
                    .frequencies_hz
                    .iter()
                    .zip(&window.channel_widths_hz),
            ) {
                let half = width.abs() / 2.0;
                if bands
                    .iter()
                    .any(|[lower, upper]| centre + half >= lower * factor && centre - half <= upper * factor)
                {
                    *count += 1;
                }
            }
        }
        Ok(Some(EpochSelection {
            counts,
            epochs: u32::try_from(epochs.len()).expect("a window's epochs fit u32"),
        }))
    }

    /// The bands of window `spw_id`; none for a window CASA selects nothing
    /// of by frequency.
    fn window_bands(&self, spw_id: usize) -> &[[f64; 2]] {
        self.bands_hz.get(&spw_id).map_or(&[], Vec::as_slice)
    }

    /// The model's bands of window `spw_id`, for the gridding to select a
    /// time's channels by.
    pub(super) fn frequency_bands(
        &self,
        spw_id: usize,
    ) -> Result<Option<FrequencyBands>, PrepareError> {
        let frame = super::spectral::imaging_frequency_frame(self.frame)?;
        Ok(FrequencyBands::new(frame, self.window_bands(spw_id).to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_band_narrows_the_bands_it_overlaps_and_is_added_where_none() {
        let mut bands = vec![[1.0, 4.0], [6.0, 9.0]];
        and_band(&mut bands, [3.0, 7.0]);
        assert_eq!(bands, vec![[3.0, 4.0], [6.0, 7.0]]);
        let mut bands = vec![[1.0, 4.0]];
        // Touching at an end is no overlap (`andFreqSelection`'s strict
        // test), so the band is added.
        and_band(&mut bands, [4.0, 5.0]);
        assert_eq!(bands, vec![[1.0, 4.0], [4.0, 5.0]]);
    }
}
