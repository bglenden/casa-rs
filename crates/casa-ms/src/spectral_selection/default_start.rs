// SPDX-License-Identifier: LGPL-3.0-or-later
//! The data channels CASA lays a cube's grid over when `start` is unset and
//! `spw` selects channels.

use std::ops::RangeInclusive;

/// CASA's `datafstart` and `datafend`: the band tclean's `spw` selection
/// spans in the windows' own frame, from the lowest lower edge to the
/// highest upper edge of the channels from each window's first selected
/// channel to its last (`MSUtil::getFreqRangeInSpw` from the edges, as
/// `SynthesisParamsImage::buildCoordinateSystem` calls it with the
/// selection's first channel and channel count per window). Each window is
/// its `(CHAN_FREQ, CHAN_WIDTH)` and the channels `spw` names, every
/// channel when it names none. `None` when no window holds a channel.
pub fn casa_selected_band_hz<'a>(
    windows: impl IntoIterator<Item = (&'a [f64], &'a [f64], Option<&'a [usize]>)>,
) -> Option<[f64; 2]> {
    let mut band_hz: Option<[f64; 2]> = None;
    for (frequencies_hz, widths_hz, selected) in windows {
        let (first, last) = match selected {
            Some(selected) => (*selected.iter().min()?, *selected.iter().max()?),
            None => (0, frequencies_hz.len().checked_sub(1)?),
        };
        for channel in first..=last {
            let half_width_hz = widths_hz.get(channel)?.abs() / 2.0;
            let centre_hz = *frequencies_hz.get(channel)?;
            let [lower_hz, upper_hz] = band_hz.get_or_insert([f64::MAX, 0.0]);
            *lower_hz = lower_hz.min(centre_hz - half_width_hz);
            *upper_hz = upper_hz.max(centre_hz + half_width_hz);
        }
    }
    band_hz
}

/// The data channels `SynthesisParamsImage::buildCoordinateSystemCore`
/// keeps for a cube whose `start` is unset (`SynthesisUtilMethods.cc`
/// ~line 2986), or `None` when it keeps them all.
///
/// CASA cuts the data channels when `band_hz`, the selected band
/// ([`casa_selected_band_hz`]), begins above their lowest channel centre,
/// so that the default start is the first selected channel. The cut runs
/// from the channel whose centre lies within half its width above the
/// band's lower edge to the one within half its width below the upper
/// edge, the rest of the channels where none does. As in CASA, one channel
/// cannot bound both ends, and the test reads the signed `CHAN_WIDTH`, so a
/// window stored with negative widths is never cut.
pub(super) fn casa_default_start_channels(
    frequencies_hz: &[f64],
    widths_hz: &[f64],
    [band_start_hz, band_end_hz]: [f64; 2],
) -> Option<RangeInclusive<usize>> {
    if frequencies_hz.is_empty() || widths_hz.len() != frequencies_hz.len() {
        return None;
    }
    let lowest_hz = frequencies_hz.iter().copied().fold(f64::INFINITY, f64::min);
    if lowest_hz >= band_start_hz {
        return None;
    }
    let count = frequencies_hz.len();
    let ascending = frequencies_hz[count - 1] - frequencies_hz[0] > 0.0;
    let (mut low, mut high) = (0, count - 1);
    for (channel, (&frequency_hz, &width_hz)) in frequencies_hz.iter().zip(widths_hz).enumerate() {
        let above_start_hz = frequency_hz - band_start_hz;
        let below_end_hz = band_end_hz - frequency_hz;
        let bounds_start = above_start_hz > 0.0 && above_start_hz <= width_hz / 2.0;
        let bounds_end = below_end_hz > 0.0 && below_end_hz <= width_hz / 2.0;
        if ascending {
            if bounds_start {
                low = channel;
            } else if bounds_end {
                high = channel;
            }
        } else if bounds_end {
            high = channel;
        } else if bounds_start {
            low = channel;
        }
    }
    let cut = low.min(high)..=low.max(high);
    (cut != (0..=count - 1)).then_some(cut)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `refim_point.ms` spectral window 0: 20 channels of 50 MHz from 1 GHz.
    fn window() -> (Vec<f64>, Vec<f64>) {
        let frequencies_hz = (0..20)
            .map(|channel| 1.0e9 + f64::from(channel) * 5.0e7)
            .collect();
        (frequencies_hz, vec![5.0e7; 20])
    }

    /// The default-start cut of one window's selected channels.
    fn cut(
        frequencies_hz: &[f64],
        widths_hz: &[f64],
        selected: &[usize],
    ) -> Option<RangeInclusive<usize>> {
        let band_hz = casa_selected_band_hz([(frequencies_hz, widths_hz, Some(selected))])?;
        casa_default_start_channels(frequencies_hz, widths_hz, band_hz)
    }

    #[test]
    fn a_selection_above_the_lowest_channel_cuts_the_window_to_it() {
        // test_cube_23: spw='0:4~13'; CASA's cube starts at channel 4.
        let (frequencies_hz, widths_hz) = window();
        let selected = (4..=13).collect::<Vec<_>>();
        assert_eq!(cut(&frequencies_hz, &widths_hz, &selected), Some(4..=13));
        // A gap or a stride selects the same band.
        assert_eq!(
            cut(&frequencies_hz, &widths_hz, &[4, 5, 12, 13]),
            Some(4..=13)
        );
        // test_onefield_pcwdT_and_pcwdF: spw='0:1'. The one channel bounds
        // the start, so the cut runs to the window's end.
        assert_eq!(cut(&frequencies_hz, &widths_hz, &[1]), Some(1..=19));
    }

    #[test]
    fn a_selection_from_the_lowest_channel_or_with_negative_widths_keeps_the_window() {
        let (frequencies_hz, widths_hz) = window();
        assert_eq!(cut(&frequencies_hz, &widths_hz, &[0, 1, 2]), None);
        let mut descending_hz = frequencies_hz;
        descending_hz.reverse();
        let negative = vec![-5.0e7; 20];
        assert_eq!(cut(&descending_hz, &negative, &[4, 5, 6]), None);
        // With positive widths a descending window is cut, from the
        // selection's highest frequency.
        assert_eq!(cut(&descending_hz, &widths_hz, &[4, 5, 6]), Some(4..=6));
        assert_eq!(cut(&descending_hz, &negative, &[]), None);
    }

    /// Several windows: the band runs over every window's selection, a
    /// window without a channel selection whole, so the cut of the
    /// combined channels starts at the lowest selected channel of any.
    #[test]
    fn the_band_spans_every_selected_window() {
        let low = ([1.0e9, 1.1e9, 1.2e9], [1.0e8; 3]);
        let high = ([2.0e9, 2.1e9], [-1.0e8; 2]);
        let band_hz = casa_selected_band_hz([
            (&low.0[..], &low.1[..], Some(&[1, 2][..])),
            (&high.0[..], &high.1[..], None),
        ]);
        assert_eq!(band_hz, Some([1.05e9, 2.15e9]));
        let combined_hz = [1.0e9, 1.1e9, 1.2e9, 2.0e9, 2.1e9];
        assert_eq!(
            casa_default_start_channels(&combined_hz, &[1.0e8; 5], [1.05e9, 2.15e9]),
            Some(1..=4)
        );
        assert_eq!(casa_selected_band_hz([]), None);
    }
}
