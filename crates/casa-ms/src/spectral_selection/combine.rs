// SPDX-License-Identifier: LGPL-3.0-or-later
//! The one channel axis CASA makes of several spectral windows before it
//! lays a cube's grid over them.

use crate::error::{MsError, MsResult};

/// The channels of several spectral windows, each given as its
/// `(CHAN_FREQ, CHAN_WIDTH)` in window-id order, merged into one axis
/// ascending in frequency with positive widths, as
/// `MSTransformRegridder::combineSpwsCore` merges them for
/// `SynthesisParamsImage::buildCoordinateSystemCore` (casatools
/// `mstransform/MSTransform/MSTransformRegridder.cc`, CASA 6.7.5, ~line 72).
///
/// The windows are taken in order of their lowest channel frequency, each
/// ascending. A window that begins above the merged channels' upper edge is
/// appended and one that ends below their lower edge prepended. Otherwise
/// its channels below the merged axis are prepended, the lowest of them
/// overlapping the axis's first channel becomes one channel spanning both,
/// the merged channels keep their frequencies, and its channels above the
/// axis are appended: the first of these, when it overlaps the axis's last
/// channel by more than 1% of its width, widens the channel at the index of
/// the axis's last channel instead (an index into the merged list, so
/// after prepended channels it is not the axis's last one, as in CASA), and
/// a smaller overlap leaves it a narrower channel from that channel's upper
/// edge. Windows that only touch, such as adjacent windows of equal widths,
/// are simply concatenated. The caller refuses windows in different frames,
/// which CASA cannot combine.
///
/// # Errors
///
/// Returns an error when no window is given, or a window is empty or its
/// frequencies and widths differ in length.
pub fn combine_spectral_windows(windows: &[(&[f64], &[f64])]) -> MsResult<(Vec<f64>, Vec<f64>)> {
    let mut ascending = windows
        .iter()
        .map(|&(frequencies_hz, widths_hz)| ascending_window(frequencies_hz, widths_hz))
        .collect::<MsResult<Vec<_>>>()?;
    // CASA sorts by the lowest channel frequency; the stable sort keeps
    // window-id order for windows starting together.
    ascending.sort_by(|a, b| a.0[0].total_cmp(&b.0[0]));
    let mut windows = ascending.into_iter();
    let (mut frequencies_hz, mut widths_hz) = windows.next().ok_or_else(|| {
        MsError::VersionError("combining spectral windows needs at least one".to_string())
    })?;
    for (window_hz, window_widths_hz) in windows {
        (frequencies_hz, widths_hz) = merge_window(
            (&frequencies_hz, &widths_hz),
            (&window_hz, &window_widths_hz),
        );
    }
    Ok((frequencies_hz, widths_hz))
}

/// A window's channels ascending in frequency, with positive widths.
fn ascending_window(frequencies_hz: &[f64], widths_hz: &[f64]) -> MsResult<(Vec<f64>, Vec<f64>)> {
    if frequencies_hz.is_empty() || frequencies_hz.len() != widths_hz.len() {
        return Err(MsError::VersionError(format!(
            "combining spectral windows needs matching non-empty frequency/width arrays, got {} \
             and {}",
            frequencies_hz.len(),
            widths_hz.len()
        )));
    }
    let mut frequencies_hz = frequencies_hz.to_vec();
    let mut widths_hz = widths_hz
        .iter()
        .map(|width| width.abs())
        .collect::<Vec<_>>();
    if frequencies_hz[0] > frequencies_hz[frequencies_hz.len() - 1] {
        frequencies_hz.reverse();
        widths_hz.reverse();
    }
    Ok((frequencies_hz, widths_hz))
}

/// One step of `combineSpwsCore`'s loop: window `i` merged into the axis.
fn merge_window(
    (axis_hz, axis_widths_hz): (&[f64], &[f64]),
    (window_hz, window_widths_hz): (&[f64], &[f64]),
) -> (Vec<f64>, Vec<f64>) {
    let n = axis_hz.len();
    let ni = window_hz.len();
    let mut merged_hz = Vec::with_capacity(n + ni);
    let mut merged_widths_hz = Vec::with_capacity(n + ni);
    let lower = |hz: &[f64], widths: &[f64], k: usize| hz[k] - widths[k] / 2.0;
    let upper = |hz: &[f64], widths: &[f64], k: usize| hz[k] + widths[k] / 2.0;
    if upper(axis_hz, axis_widths_hz, n - 1) < lower(window_hz, window_widths_hz, 0) {
        merged_hz.extend_from_slice(axis_hz);
        merged_hz.extend_from_slice(window_hz);
        merged_widths_hz.extend_from_slice(axis_widths_hz);
        merged_widths_hz.extend_from_slice(window_widths_hz);
        return (merged_hz, merged_widths_hz);
    }
    if lower(axis_hz, axis_widths_hz, 0) > upper(window_hz, window_widths_hz, ni - 1) {
        merged_hz.extend_from_slice(window_hz);
        merged_hz.extend_from_slice(axis_hz);
        merged_widths_hz.extend_from_slice(window_widths_hz);
        merged_widths_hz.extend_from_slice(axis_widths_hz);
        return (merged_hz, merged_widths_hz);
    }
    let mut axis_start = 0;
    // The window starts below the axis (CASA compares with its last
    // channel's lower edge).
    if lower(window_hz, window_widths_hz, 0) < lower(axis_hz, axis_widths_hz, n - 1) {
        let lower_0 = lower(axis_hz, axis_widths_hz, 0);
        let upper_0 = upper(axis_hz, axis_widths_hz, 0);
        let (mut lower_k, mut upper_k) = (0.0, 0.0);
        for k in 0..ni {
            upper_k = upper(window_hz, window_widths_hz, k);
            lower_k = lower(window_hz, window_widths_hz, k);
            if lower_0 < upper_k {
                break;
            }
            merged_hz.push(window_hz[k]);
            merged_widths_hz.push(window_widths_hz[k]);
        }
        if lower_0 < upper_k && lower_k < lower_0 {
            let width_hz = upper_0 - lower_k;
            merged_hz.push(lower_k + width_hz / 2.0);
            merged_widths_hz.push(width_hz);
            axis_start = 1;
        }
    }
    merged_hz.extend_from_slice(&axis_hz[axis_start..]);
    merged_widths_hz.extend_from_slice(&axis_widths_hz[axis_start..]);
    let upper_j = upper(axis_hz, axis_widths_hz, n - 1);
    if upper_j < upper(window_hz, window_widths_hz, ni - 1) {
        let lower_j = lower(axis_hz, axis_widths_hz, n - 1);
        // The overlap case holds a window channel starting at or below the
        // axis's upper edge.
        let mut k = (0..ni)
            .rev()
            .find(|&k| lower(window_hz, window_widths_hz, k) <= upper_j)
            .expect("an overlapping window starts at or below the axis's upper edge");
        let lower_k = lower(window_hz, window_widths_hz, k);
        let upper_k = upper(window_hz, window_widths_hz, k);
        if lower_k < upper_j && upper_j < upper_k {
            if (upper_j - lower_k) / window_widths_hz[k] > 0.01 {
                let j = n - 1;
                merged_hz[j] = (lower_j + upper_k) / 2.0;
                merged_widths_hz[j] = upper_k - lower_j;
            } else {
                merged_hz.push((upper_j + upper_k) / 2.0);
                merged_widths_hz.push(upper_k - upper_j);
            }
            k += 1;
        }
        merged_hz.extend_from_slice(&window_hz[k..]);
        merged_widths_hz.extend_from_slice(&window_widths_hz[k..]);
    }
    (merged_hz, merged_widths_hz)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn combined(windows: &[(Vec<f64>, Vec<f64>)]) -> (Vec<f64>, Vec<f64>) {
        let windows = windows
            .iter()
            .map(|(frequencies, widths)| (frequencies.as_slice(), widths.as_slice()))
            .collect::<Vec<_>>();
        combine_spectral_windows(&windows).expect("windows combine")
    }

    /// `refim_oneshiftpoint.mosaic.ms`: three one-channel windows of
    /// 300 MHz at 1.2, 1.5 and 1.8 GHz that touch; CASA's combined axis is
    /// the three channels.
    #[test]
    fn touching_windows_concatenate_in_frequency_order() {
        let window = |hz: f64| (vec![hz], vec![3.0e8]);
        assert_eq!(
            combined(&[window(1.8e9), window(1.2e9), window(1.5e9)]),
            (vec![1.2e9, 1.5e9, 1.8e9], vec![3.0e8; 3])
        );
        // Separated windows append or prepend; a descending window is
        // reversed and its negative widths made positive.
        assert_eq!(
            combined(&[
                (vec![3.0e9, 2.9e9], vec![-1.0e8, -1.0e8]),
                (vec![2.0e9], vec![2.0e8]),
            ]),
            (vec![2.0e9, 2.9e9, 3.0e9], vec![2.0e8, 1.0e8, 1.0e8])
        );
    }

    /// Overlapping windows: the window's channels below the axis are
    /// prepended and its lowest overlapping channel merged with the axis's
    /// first; above, a channel overlapping the axis's last by more than 1%
    /// widens it, and by less is cut to a narrower channel.
    #[test]
    fn overlapping_windows_merge_their_edge_channels() {
        // Axis 10..14 (centres 11, 13); the window 7..12 (centres 8, 10.5).
        let axis = (vec![11.0, 13.0], vec![2.0, 2.0]);
        let below = (vec![8.0, 10.5], vec![2.0, 3.0]);
        assert_eq!(
            combined(&[axis.clone(), below]),
            (vec![8.0, 10.5, 13.0], vec![2.0, 3.0, 2.0])
        );
        // The window lowest in frequency is the axis the others merge into:
        // here 8..10 and 11..13, whose last channel the window 10..14
        // overlaps by half its width, widening it to 11..14.
        let starts_below = (vec![9.0, 12.0], vec![2.0, 2.0]);
        assert_eq!(
            combined(&[axis.clone(), starts_below]),
            (vec![9.0, 12.5], vec![2.0, 3.0])
        );
        // A wider window starting above the axis's centre but below its
        // lower edge merges with its first channel (9.5..12), and that
        // channel is then widened again at the upper edge (10..13.5).
        assert_eq!(
            combined(&[(vec![11.0], vec![2.0]), (vec![11.5], vec![4.0])]),
            (vec![11.75], vec![3.5])
        );
        // Above: the window 13.5..15.5 overlaps the axis's last channel
        // (12..14) by 25% of its width, which widens it to 12..15.5.
        let above = (vec![14.5, 16.5], vec![2.0, 2.0]);
        assert_eq!(
            combined(&[axis.clone(), above]),
            (vec![11.0, 13.75, 16.5], vec![2.0, 3.5, 2.0])
        );
        // An overlap of 0.5% leaves a channel from the axis's upper edge.
        let barely = (vec![14.99, 17.0], vec![2.0, 2.0]);
        let (frequencies, widths) = combined(&[axis, barely]);
        assert_eq!(frequencies.len(), 4);
        assert_eq!(frequencies[2], (14.0 + 15.99) / 2.0);
        assert!((widths[2] - 1.99).abs() < 1e-12);
    }

    /// A channel-mode cube over combined windows with `nchan=-1` holds as
    /// many channels of the first channel's width as fit in the combined
    /// band (`regridChanBounds`), so windows with gaps between them give
    /// more planes than channels; each window reads its own channel.
    #[test]
    fn a_default_channel_cube_spans_the_combined_band() {
        use crate::spectral_selection::{CubeAxisConfig, CubeSpecMode, CubeSpectralSetup};

        let observatory = casa_types::measures::position::MPosition::new_itrf(
            -1_601_185.4,
            -5_041_977.5,
            3_554_875.9,
        );
        let direction = casa_types::measures::direction::MDirection::from_angles(
            0.0,
            std::f64::consts::FRAC_PI_4,
            casa_types::measures::direction::DirectionRef::J2000,
        );
        let engine = crate::derived::engine::MsCalEngine::from_parts(
            vec![observatory.clone()],
            vec![direction],
            observatory,
            casa_test_support::deterministic_measures_provider(),
        );
        let config = CubeAxisConfig {
            specmode: CubeSpecMode::Cubedata,
            ..CubeAxisConfig::default()
        };
        let time = 59_000.0 * 86_400.0;
        let axis = |windows: &[(Vec<f64>, Vec<f64>)]| {
            let (frequencies, widths) = combined(windows);
            CubeSpectralSetup::for_casa_cube_axis(
                casa_types::measures::frequency::FrequencyRef::TOPO,
                &frequencies,
                &widths,
                None,
                &config,
                time,
                0,
                None,
                &engine,
            )
            .expect("a channel-mode axis")
        };
        let touching = [1.2e9, 1.5e9, 1.8e9].map(|hz| (vec![hz], vec![3.0e8]));
        let setup = axis(&touching);
        assert_eq!(
            setup.output_channel_frequencies_hz,
            vec![1.2e9, 1.5e9, 1.8e9]
        );
        for (frequencies, widths) in &touching {
            let support = setup
                .source_channel_support(frequencies, widths, [time, time], 0, &engine)
                .expect("support");
            assert_eq!(support.indices, vec![0]);
        }
        let separated = [2.0e9, 2.5e9, 3.0e9].map(|hz| (vec![hz], vec![2.0e8]));
        let setup = axis(&separated);
        assert_eq!(setup.output_channel_frequencies_hz.len(), 6);
        assert_eq!(setup.output_channel_frequencies_hz[0], 2.0e9);
        assert_eq!(setup.output_channel_frequencies_hz[5], 3.0e9);
    }

    #[test]
    fn combining_needs_matching_non_empty_windows() {
        assert!(combine_spectral_windows(&[]).is_err());
        assert!(combine_spectral_windows(&[(&[1.0e9], &[])]).is_err());
    }
}
