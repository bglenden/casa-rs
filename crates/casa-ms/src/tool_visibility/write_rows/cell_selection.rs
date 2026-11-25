// SPDX-License-Identifier: LGPL-3.0-or-later
//! Merge only the recorded coordinates addressed by a visibility write.

use ndarray::{ArrayD, Axis, IxDyn};

use super::super::VisibilityChannels;

pub(super) fn patch_cell<T: Copy>(
    original: &mut ArrayD<T>,
    selected: &ArrayD<T>,
    correlations: Option<&[usize]>,
    channels: Option<VisibilityChannels>,
) {
    if let Some(channels) = channels {
        for (bin, span) in channels.ranges().enumerate() {
            let selected = selected.index_axis(Axis(1), bin);
            for channel in span {
                let mut original = original.index_axis_mut(Axis(1), channel);
                if let Some(indices) = correlations {
                    for (input, &output) in indices.iter().enumerate() {
                        original[IxDyn(&[output])] = selected[IxDyn(&[input])];
                    }
                } else {
                    original.assign(&selected);
                }
            }
        }
    } else if let Some(indices) = correlations {
        for (input, &output) in indices.iter().enumerate() {
            original
                .index_axis_mut(Axis(0), output)
                .assign(&selected.index_axis(Axis(0), input));
        }
    }
}

pub(super) fn patch_converted_flags(
    original: &mut ArrayD<bool>,
    selected: &ArrayD<bool>,
    dependencies: &[&[usize]],
    channels: Option<VisibilityChannels>,
) {
    let mut patch_bin = |bin, span: std::ops::Range<usize>| {
        for correlation in 0..original.shape()[0] {
            let mut flag = None;
            for (product, contributors) in dependencies.iter().enumerate() {
                if contributors.contains(&correlation) {
                    flag = Some(flag.unwrap_or(false) || selected[IxDyn(&[product, bin])]);
                }
            }
            if let Some(flag) = flag {
                for channel in span.clone() {
                    original[IxDyn(&[correlation, channel])] = flag;
                }
            }
        }
    };
    if let Some(channels) = channels {
        for (bin, span) in channels.ranges().enumerate() {
            patch_bin(bin, span);
        }
    } else {
        for channel in 0..selected.shape()[1] {
            patch_bin(channel, channel..channel + 1);
        }
    }
}
