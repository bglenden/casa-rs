// SPDX-License-Identifier: LGPL-3.0-or-later
//! The data channels CASA lays a cube's grid over when `start` is unset and
//! `spw` selects channels.

use std::ops::RangeInclusive;

/// The window channels `SynthesisParamsImage::buildCoordinateSystemCore`
/// keeps for a cube whose `start` is unset (`SynthesisUtilMethods.cc`
/// ~line 2986), or `None` when it keeps them all.
///
/// CASA cuts the window when the selected band begins above its lowest
/// channel centre, so that the default start is the first selected
/// channel. The band runs from the lowest lower edge to the highest upper
/// edge of the channels from the first to the last `selected`
/// (`MSUtil::getFreqRangeInSpw` with `fromEdge`); the cut runs from the
/// channel whose centre lies within half its width above the band's lower
/// edge to the one within half its width below the upper edge, the rest of
/// the window where none does. As in CASA, one channel cannot bound both
/// ends, and the test reads the signed `CHAN_WIDTH`, so a window stored
/// with negative widths is never cut.
pub(super) fn casa_default_start_channels(
    frequencies_hz: &[f64],
    widths_hz: &[f64],
    selected: &[usize],
) -> Option<RangeInclusive<usize>> {
    let first = *selected.iter().min()?;
    let last = *selected.iter().max()?;
    if last >= frequencies_hz.len() || widths_hz.len() != frequencies_hz.len() {
        return None;
    }
    let (mut band_start_hz, mut band_end_hz) = (f64::MAX, 0.0_f64);
    for channel in first..=last {
        let half_width_hz = widths_hz[channel].abs() / 2.0;
        band_start_hz = band_start_hz.min(frequencies_hz[channel] - half_width_hz);
        band_end_hz = band_end_hz.max(frequencies_hz[channel] + half_width_hz);
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

    #[test]
    fn a_selection_above_the_lowest_channel_cuts_the_window_to_it() {
        // test_cube_23: spw='0:4~13'; CASA's cube starts at channel 4.
        let (frequencies_hz, widths_hz) = window();
        let selected = (4..=13).collect::<Vec<_>>();
        assert_eq!(
            casa_default_start_channels(&frequencies_hz, &widths_hz, &selected),
            Some(4..=13)
        );
        // A gap or a stride selects the same band.
        assert_eq!(
            casa_default_start_channels(&frequencies_hz, &widths_hz, &[4, 5, 12, 13]),
            Some(4..=13)
        );
        // test_onefield_pcwdT_and_pcwdF: spw='0:1'. The one channel bounds
        // the start, so the cut runs to the window's end.
        assert_eq!(
            casa_default_start_channels(&frequencies_hz, &widths_hz, &[1]),
            Some(1..=19)
        );
    }

    #[test]
    fn a_selection_from_the_lowest_channel_or_with_negative_widths_keeps_the_window() {
        let (frequencies_hz, widths_hz) = window();
        assert_eq!(
            casa_default_start_channels(&frequencies_hz, &widths_hz, &[0, 1, 2]),
            None
        );
        let mut descending_hz = frequencies_hz;
        descending_hz.reverse();
        let negative = vec![-5.0e7; 20];
        assert_eq!(
            casa_default_start_channels(&descending_hz, &negative, &[4, 5, 6]),
            None
        );
        // With positive widths a descending window is cut, from the
        // selection's highest frequency.
        assert_eq!(
            casa_default_start_channels(&descending_hz, &widths_hz, &[4, 5, 6]),
            Some(4..=6)
        );
        assert_eq!(
            casa_default_start_channels(&descending_hz, &negative, &[]),
            None
        );
    }
}
