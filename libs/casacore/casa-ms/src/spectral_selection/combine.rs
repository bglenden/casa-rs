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
/// The eight layouts of this module's `touching_windows_…` and
/// `overlapping_windows_…` tests (the latter's numbers taken as 1e8 Hz)
/// were combined by CASA 6.7.6.14 too, by `mstransform(combinespws=True)`,
/// whose log states the combined window's channel count, first and last
/// centres and last width before it regrids. Those agree on each: the
/// overlaps below and above the axis, the 1% rule both ways, descending
/// windows and the sort by lowest frequency. CASA states no interior
/// channel, so a widened channel's centre and width follow from the count
/// and the C++ alone.
///
/// # Errors
///
/// Returns an error when no window is given, or a window is empty or its
/// frequencies and widths differ in length.
pub fn combine_spectral_windows(windows: &[(&[f64], &[f64])]) -> MsResult<(Vec<f64>, Vec<f64>)> {
    Ok(combine_windows(windows, false)?
        .into_iter()
        .map(|channel| (channel.frequency, channel.width))
        .unzip())
}

/// One input channel contributing its fractional bandwidth to a combined channel.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WindowContribution {
    pub window: usize,
    pub channel: usize,
    pub fraction: f64,
}

/// A channel on the irregular grid before the task interpolates onto its final grid.
#[derive(Clone)]
pub struct CombinedChannel {
    /// Channel center frequency in hertz.
    pub frequency: f64,
    /// Channel bandwidth in hertz.
    pub width: f64,
    /// Contributing input channels and their fractional bandwidths.
    pub sources: Vec<WindowContribution>,
}

impl CombinedChannel {
    fn lower(&self) -> f64 {
        self.frequency - self.width / 2.0
    }

    fn upper(&self) -> f64 {
        self.frequency + self.width / 2.0
    }

    fn set_edges(&mut self, lower: f64, upper: f64) {
        self.width = upper - lower;
        self.frequency = (lower + upper) / 2.0;
    }
}

/// Merge spectral grids into ascending frequency order, optionally retaining input-channel contributions.
pub fn combine_windows(
    windows: &[(&[f64], &[f64])],
    track_sources: bool,
) -> MsResult<Vec<CombinedChannel>> {
    let mut ascending = windows
        .iter()
        .enumerate()
        .map(|(index, &(frequencies, widths))| {
            ascending_window(index, frequencies, widths, track_sources)
        })
        .collect::<MsResult<Vec<_>>>()?;
    // CASA sorts by the lowest channel frequency; the stable sort keeps
    // window-id order for windows starting together.
    ascending.sort_by(|a, b| a[0].frequency.total_cmp(&b[0].frequency));
    let mut windows = ascending.into_iter();
    let mut axis = windows.next().ok_or_else(|| {
        MsError::VersionError("combining spectral windows needs at least one".to_string())
    })?;
    for window in windows {
        axis = merge_window(axis, window, track_sources);
    }
    Ok(axis)
}

/// A window's channels ascending in frequency, with positive widths.
fn ascending_window(
    window: usize,
    frequencies_hz: &[f64],
    widths_hz: &[f64],
    track_sources: bool,
) -> MsResult<Vec<CombinedChannel>> {
    if frequencies_hz.is_empty() || frequencies_hz.len() != widths_hz.len() {
        return Err(MsError::VersionError(format!(
            "combining spectral windows needs matching non-empty frequency/width arrays, got {} \
             and {}",
            frequencies_hz.len(),
            widths_hz.len()
        )));
    }
    let mut channels = frequencies_hz
        .iter()
        .zip(widths_hz)
        .enumerate()
        .map(|(channel, (&frequency, &width))| CombinedChannel {
            frequency,
            width: width.abs(),
            sources: if track_sources {
                vec![WindowContribution {
                    window,
                    channel,
                    fraction: 1.0,
                }]
            } else {
                Vec::new()
            },
        })
        .collect::<Vec<_>>();
    if frequencies_hz[0] > frequencies_hz[frequencies_hz.len() - 1] {
        channels.reverse();
    }
    Ok(channels)
}

/// One step of `combineSpwsCore`'s loop: window `i` merged into the axis.
fn merge_window(
    mut axis: Vec<CombinedChannel>,
    mut window: Vec<CombinedChannel>,
    track_sources: bool,
) -> Vec<CombinedChannel> {
    let last_axis = axis.len() - 1;
    let last_window = window.len() - 1;
    let upper_axis = axis[last_axis].upper();
    let lower_axis = axis[last_axis].lower();
    if upper_axis < window[0].lower() {
        axis.append(&mut window);
        return axis;
    }
    if axis[0].lower() > window[last_window].upper() {
        window.append(&mut axis);
        return window;
    }
    let mut merged = Vec::with_capacity(axis.len() + window.len());
    let mut axis_start = 0;
    // The window starts below the axis (CASA compares with its last
    // channel's lower edge).
    if window[0].lower() < lower_axis {
        for incoming in &window {
            if axis[0].lower() < incoming.upper() {
                if incoming.lower() < axis[0].lower() {
                    let mut wide = incoming.clone();
                    wide.width = axis[0].upper() - incoming.lower();
                    wide.frequency = incoming.lower() + wide.width / 2.0;
                    wide.sources.append(&mut axis[0].sources);
                    merged.push(wide);
                    axis_start = 1;
                }
                break;
            }
            merged.push(incoming.clone());
        }
    }
    if track_sources {
        let half_max_width = window.iter().map(|c| c.width).fold(0.0, f64::max) / 2.0;
        for current in &mut axis[axis_start..] {
            let low = current.lower();
            let high = current.upper();
            let first = window.partition_point(|c| c.frequency <= low - half_max_width);
            for incoming in window[first..]
                .iter()
                .take_while(|c| c.frequency < high + half_max_width)
            {
                let overlap = high.min(incoming.upper()) - low.max(incoming.lower());
                if overlap > 0.0 {
                    let mut source = incoming.sources[0];
                    source.fraction = if low <= incoming.lower() && incoming.upper() <= high {
                        1.0
                    } else {
                        (overlap / incoming.width).min(1.0)
                    };
                    current.sources.push(source);
                }
            }
        }
    }
    merged.extend(axis.into_iter().skip(axis_start));
    if upper_axis < window[last_window].upper() {
        // The overlap case holds a window channel starting at or below the
        // axis's upper edge.
        let mut k = window
            .iter()
            .rposition(|incoming| incoming.lower() <= upper_axis)
            .expect("an overlapping window starts at or below the axis's upper edge");
        let incoming = &window[k];
        let lower_k = incoming.lower();
        let upper_k = incoming.upper();
        if lower_k < upper_axis && upper_axis < upper_k {
            if (upper_axis - lower_k) / incoming.width > 0.01 {
                let widened = &mut merged[last_axis];
                widened.set_edges(lower_axis, upper_k);
                if let Some(source) = widened.sources.last_mut() {
                    source.fraction = 1.0;
                }
            } else {
                let mut narrow = incoming.clone();
                narrow.set_edges(upper_axis, upper_k);
                merged.push(narrow);
            }
            k += 1;
        }
        merged.extend(window.into_iter().skip(k));
    }
    merged
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

    #[test]
    fn fractional_overlap_keeps_the_old_footprints_and_full_widened_edge_contributor() {
        let axis = combine_windows(
            &[(&[0., 1., 2.], &[1.; 3]), (&[1.5, 2.5, 3.5], &[1.; 3])],
            true,
        )
        .unwrap();
        assert_eq!(axis[2].frequency, 2.25);
        assert_eq!(axis[2].width, 1.5);
        assert_eq!(
            axis[2].sources,
            [
                WindowContribution {
                    window: 0,
                    channel: 2,
                    fraction: 1.0
                },
                WindowContribution {
                    window: 1,
                    channel: 0,
                    fraction: 0.5
                },
                WindowContribution {
                    window: 1,
                    channel: 1,
                    fraction: 1.0
                },
            ]
        );
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
        // reversed and its negative widths made positive, and sorts by its
        // lowest channel.
        assert_eq!(
            combined(&[
                (vec![3.0e9, 2.9e9], vec![-1.0e8, -1.0e8]),
                (vec![2.0e9, 2.2e9], vec![2.0e8, 2.0e8]),
            ]),
            (
                vec![2.0e9, 2.2e9, 2.9e9, 3.0e9],
                vec![2.0e8, 2.0e8, 1.0e8, 1.0e8]
            )
        );
        assert_eq!(
            combined(&[
                (vec![1.2e9, 1.3e9], vec![1.0e8, 1.0e8]),
                (vec![3.1e9, 3.0e9], vec![-1.0e8, -1.0e8]),
                (vec![2.0e9, 2.1e9], vec![1.0e8, 1.0e8]),
            ]),
            (
                vec![1.2e9, 1.3e9, 2.0e9, 2.1e9, 3.0e9, 3.1e9],
                vec![1.0e8; 6]
            )
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

        let observatory = casa_core::measures::position::MPosition::new_itrf(
            -1_601_185.4,
            -5_041_977.5,
            3_554_875.9,
        );
        let direction = casa_core::measures::direction::MDirection::from_angles(
            0.0,
            std::f64::consts::FRAC_PI_4,
            casa_core::measures::direction::DirectionRef::J2000,
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
                casa_core::measures::frequency::FrequencyRef::TOPO,
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
