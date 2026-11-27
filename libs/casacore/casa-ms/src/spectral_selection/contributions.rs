// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn nearest_channel_index(
    channel_frequencies_hz: &[f64],
    frequency_hz: f64,
) -> Option<usize> {
    let (best_index, best_diff) = channel_frequencies_hz
        .iter()
        .enumerate()
        .map(|(index, channel_frequency_hz)| (index, (channel_frequency_hz - frequency_hz).abs()))
        .min_by(|left, right| {
            left.1
                .partial_cmp(&right.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        })?;
    let max_allowed_diff = if channel_frequencies_hz.len() <= 1 {
        f64::INFINITY
    } else {
        let lower_spacing = best_index
            .checked_sub(1)
            .map(|index| (channel_frequencies_hz[best_index] - channel_frequencies_hz[index]).abs())
            .unwrap_or(f64::INFINITY);
        let upper_spacing = channel_frequencies_hz
            .get(best_index + 1)
            .map(|frequency| (frequency - channel_frequencies_hz[best_index]).abs())
            .unwrap_or(f64::INFINITY);
        0.5 * lower_spacing.min(upper_spacing)
    };
    if best_diff <= max_allowed_diff {
        Some(best_index)
    } else {
        None
    }
}

pub(super) fn spectral_interpolation_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    interpolation: CubeInterpolation,
    frequency_hz: f64,
) -> Vec<CubeChannelContribution> {
    match interpolation {
        CubeInterpolation::Nearest => {
            nearest_channel_index(source_channel_frequencies_hz, frequency_hz)
                .map(|index| {
                    vec![CubeChannelContribution {
                        source_channel: index,
                        source_frequency_hz: native_source_channel_frequencies_hz[index],
                        factor: 1.0,
                    }]
                })
                .unwrap_or_default()
        }
        CubeInterpolation::Linear | CubeInterpolation::Cubic => linear_channel_contributions(
            native_source_channel_frequencies_hz,
            source_channel_frequencies_hz,
            source_channel_widths_hz,
            frequency_hz,
        ),
    }
}

pub(super) fn build_output_channel_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_frequencies_for_interpolation: &[f64],
    source_channel_widths_for_interpolation: &[f64],
    output_frequencies_for_interpolation: &[f64],
    interpolation: CubeInterpolation,
) -> Vec<Vec<CubeChannelContribution>> {
    output_frequencies_for_interpolation
        .iter()
        .copied()
        .map(|output_frequency_hz| {
            spectral_interpolation_contributions(
                native_source_channel_frequencies_hz,
                source_frequencies_for_interpolation,
                source_channel_widths_for_interpolation,
                interpolation,
                output_frequency_hz,
            )
        })
        .collect()
}

pub(super) fn build_source_channel_output_map(
    source_channel_frequencies_hz: &[f64],
    output_channel_frequencies_hz: &[f64],
    output_channel_widths_hz: &[f64],
) -> Vec<Option<usize>> {
    if output_channel_frequencies_hz.is_empty() {
        return source_channel_frequencies_hz.iter().map(|_| None).collect();
    }
    if output_channel_frequencies_hz.len() == 1 {
        let center_hz = output_channel_frequencies_hz[0];
        let half_width_hz = output_channel_widths_hz
            .first()
            .copied()
            .map(|width| 0.5 * width.abs())
            .unwrap_or(f64::INFINITY);
        return source_channel_frequencies_hz
            .iter()
            .map(|frequency_hz| {
                (frequency_hz.is_finite()
                    && center_hz.is_finite()
                    && (*frequency_hz - center_hz).abs() <= half_width_hz)
                    .then_some(0)
            })
            .collect();
    }
    let first_hz = output_channel_frequencies_hz[0];
    let increment_hz = output_channel_frequencies_hz[1] - output_channel_frequencies_hz[0];
    if !(first_hz.is_finite() && increment_hz.is_finite() && increment_hz != 0.0) {
        return source_channel_frequencies_hz.iter().map(|_| None).collect();
    }
    let nchan = output_channel_frequencies_hz.len() as isize;
    source_channel_frequencies_hz
        .iter()
        .copied()
        .map(|frequency_hz| {
            if !frequency_hz.is_finite() {
                return None;
            }
            let pixel = ((frequency_hz - first_hz) / increment_hz).round() as isize;
            (pixel >= 0 && pixel < nchan).then_some(pixel as usize)
        })
        .collect()
}

pub(super) fn build_briggs_cube_weight_output_frequencies(
    output_channel_frequencies_hz: &[f64],
    output_channel_widths_hz: &[f64],
) -> Vec<f64> {
    if output_channel_frequencies_hz.is_empty() {
        return Vec::new();
    }
    let increment_hz = if output_channel_frequencies_hz.len() >= 2 {
        output_channel_frequencies_hz[1] - output_channel_frequencies_hz[0]
    } else {
        output_channel_widths_hz.first().copied().unwrap_or(0.0)
    };
    if !(increment_hz.is_finite() && increment_hz != 0.0) {
        return output_channel_frequencies_hz.to_vec();
    }
    let mut padded_frequencies_hz = Vec::with_capacity(output_channel_frequencies_hz.len() + 4);
    padded_frequencies_hz.push(output_channel_frequencies_hz[0] - 2.0 * increment_hz);
    padded_frequencies_hz.push(output_channel_frequencies_hz[0] - increment_hz);
    padded_frequencies_hz.extend_from_slice(output_channel_frequencies_hz);
    padded_frequencies_hz.push(
        output_channel_frequencies_hz[output_channel_frequencies_hz.len() - 1] + increment_hz,
    );
    padded_frequencies_hz.push(
        output_channel_frequencies_hz[output_channel_frequencies_hz.len() - 1] + 2.0 * increment_hz,
    );
    padded_frequencies_hz
}

pub(super) fn build_grid_channel_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    output_channel_frequencies_hz: &[f64],
    output_channel_widths_hz: &[f64],
    interpolation: CubeInterpolation,
) -> Vec<CubeGridChannelContributions> {
    let (grid_frequencies_hz, channel_map) = casa_grid_frequency_channel_map(
        source_channel_frequencies_hz,
        output_channel_frequencies_hz,
        output_channel_widths_hz,
        interpolation,
    );
    if interpolation == CubeInterpolation::Nearest
        && grid_frequencies_hz.len() == source_channel_frequencies_hz.len()
    {
        return grid_frequencies_hz
            .into_iter()
            .zip(channel_map)
            .enumerate()
            .filter_map(|(source_channel, (grid_frequency_hz, output_channel))| {
                let output_channel = output_channel?;
                let source_frequency_hz = native_source_channel_frequencies_hz
                    .get(source_channel)
                    .copied()?;
                Some(CubeGridChannelContributions {
                    output_channel,
                    grid_frequency_hz,
                    contributions: vec![CubeChannelContribution {
                        source_channel,
                        source_frequency_hz,
                        factor: 1.0,
                    }],
                })
            })
            .collect();
    }
    grid_frequencies_hz
        .into_iter()
        .zip(channel_map)
        .filter_map(|(grid_frequency_hz, output_channel)| {
            let output_channel = output_channel?;
            let contributions = spectral_interpolation_contributions(
                native_source_channel_frequencies_hz,
                source_channel_frequencies_hz,
                source_channel_widths_hz,
                interpolation,
                grid_frequency_hz,
            );
            (!contributions.is_empty()).then_some(CubeGridChannelContributions {
                output_channel,
                grid_frequency_hz,
                contributions,
            })
        })
        .collect()
}

pub(super) fn casa_grid_frequency_channel_map(
    source_channel_frequencies_hz: &[f64],
    output_channel_frequencies_hz: &[f64],
    output_channel_widths_hz: &[f64],
    interpolation: CubeInterpolation,
) -> (Vec<f64>, Vec<Option<usize>>) {
    if output_channel_frequencies_hz.is_empty() {
        return (Vec::new(), Vec::new());
    }
    if output_channel_frequencies_hz.len() == 1
        || interpolation == CubeInterpolation::Nearest
        || source_channel_frequencies_hz.len() == 1
    {
        let channel_map = build_source_channel_output_map(
            source_channel_frequencies_hz,
            output_channel_frequencies_hz,
            output_channel_widths_hz,
        );
        return (source_channel_frequencies_hz.to_vec(), channel_map);
    }

    let image_increment_hz = output_channel_frequencies_hz[1] - output_channel_frequencies_hz[0];
    let source_increment_hz = source_channel_frequencies_hz[1] - source_channel_frequencies_hz[0];
    if !(image_increment_hz.is_finite()
        && source_increment_hz.is_finite()
        && image_increment_hz != 0.0
        && source_increment_hz != 0.0)
    {
        return (
            output_channel_frequencies_hz.to_vec(),
            (0..output_channel_frequencies_hz.len()).map(Some).collect(),
        );
    }

    let width = image_increment_hz.abs() / source_increment_hz.abs();
    let direct_width_limit = match interpolation {
        CubeInterpolation::Linear => 2.0,
        CubeInterpolation::Cubic => 4.0,
        CubeInterpolation::Nearest => 1.0,
    };
    if width <= direct_width_limit {
        return (
            output_channel_frequencies_hz.to_vec(),
            (0..output_channel_frequencies_hz.len()).map(Some).collect(),
        );
    }

    let min_vis_hz = source_channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let max_vis_hz = source_channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let mut min_image_hz = output_channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let mut max_image_hz = output_channel_frequencies_hz
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let image_width_hz = image_increment_hz.abs();
    if !((min_image_hz - image_width_hz / 2.0) <= max_vis_hz
        && (max_image_hz + image_width_hz / 2.0) >= min_vis_hz)
    {
        return (
            output_channel_frequencies_hz.to_vec(),
            output_channel_frequencies_hz.iter().map(|_| None).collect(),
        );
    }

    if min_image_hz < min_vis_hz
        && let Some(value) =
            nearest_bracketed_output_frequency(output_channel_frequencies_hz, min_vis_hz)
    {
        min_image_hz = value;
    }
    if max_image_hz > max_vis_hz
        && let Some(value) =
            nearest_bracketed_output_frequency(output_channel_frequencies_hz, max_vis_hz)
    {
        max_image_hz = value;
    }

    let interp_width_hz = (image_width_hz / width.floor()).copysign(source_increment_hz);
    let ninterp = (((max_image_hz - min_image_hz).abs() + image_width_hz) / interp_width_hz.abs())
        .ceil() as usize
        + 2;
    let mut first = if interp_width_hz > 0.0 {
        min_image_hz
    } else {
        max_image_hz
    };
    if interpolation == CubeInterpolation::Linear {
        first -= interp_width_hz;
    } else if interpolation == CubeInterpolation::Cubic {
        first -= 2.0 * interp_width_hz;
    }
    let start_edge_hz = image_width_hz / 2.0 - interp_width_hz.abs() / 2.0;
    if interp_width_hz > 0.0 {
        first -= start_edge_hz;
    } else {
        first += start_edge_hz;
    }
    let grid_frequencies_hz = (0..ninterp)
        .map(|index| first + index as f64 * interp_width_hz)
        .collect::<Vec<_>>();
    let half_image_width_hz = image_width_hz / 2.0;
    let channel_map = grid_frequencies_hz
        .iter()
        .copied()
        .map(|frequency_hz| {
            output_channel_frequencies_hz.iter().position(|output_hz| {
                frequency_hz >= output_hz - half_image_width_hz
                    && frequency_hz < output_hz + half_image_width_hz
            })
        })
        .collect();
    (grid_frequencies_hz, channel_map)
}

pub(super) fn nearest_bracketed_output_frequency(
    output_channel_frequencies_hz: &[f64],
    frequency_hz: f64,
) -> Option<f64> {
    output_channel_frequencies_hz
        .iter()
        .copied()
        .min_by(|left, right| {
            (left - frequency_hz)
                .abs()
                .partial_cmp(&(right - frequency_hz).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

pub(super) fn linear_channel_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frequency_hz: f64,
) -> Vec<CubeChannelContribution> {
    linear_channel_contributions_impl(
        native_source_channel_frequencies_hz,
        source_channel_frequencies_hz,
        source_channel_widths_hz,
        frequency_hz,
        false,
    )
}

pub(super) fn linear_channel_model_contributions(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frequency_hz: f64,
) -> Vec<CubeChannelContribution> {
    linear_channel_contributions_impl(
        native_source_channel_frequencies_hz,
        source_channel_frequencies_hz,
        source_channel_widths_hz,
        frequency_hz,
        true,
    )
}

pub(super) fn linear_channel_contributions_impl(
    native_source_channel_frequencies_hz: &[f64],
    source_channel_frequencies_hz: &[f64],
    source_channel_widths_hz: &[f64],
    frequency_hz: f64,
    clip_to_edge_channel: bool,
) -> Vec<CubeChannelContribution> {
    let nchan = source_channel_frequencies_hz.len();
    if nchan == 0 {
        return Vec::new();
    }
    if native_source_channel_frequencies_hz.len() != nchan {
        return Vec::new();
    }
    if source_channel_widths_hz.len() != nchan {
        return Vec::new();
    }
    if nchan == 1 {
        let low_edge_hz =
            source_channel_frequencies_hz[0] - source_channel_widths_hz[0].abs() / 2.0;
        let high_edge_hz =
            source_channel_frequencies_hz[0] + source_channel_widths_hz[0].abs() / 2.0;
        if frequency_hz < low_edge_hz || frequency_hz > high_edge_hz {
            return Vec::new();
        }
        return Some(0)
            .map(|index| {
                vec![CubeChannelContribution {
                    source_channel: index,
                    source_frequency_hz: native_source_channel_frequencies_hz[index],
                    factor: 1.0,
                }]
            })
            .unwrap_or_default();
    }

    let min_spacing_hz = source_channel_frequencies_hz
        .windows(2)
        .map(|pair| (pair[1] - pair[0]).abs())
        .filter(|spacing| spacing.is_finite() && *spacing > 0.0)
        .fold(f64::INFINITY, f64::min);
    let center_tolerance_hz = if min_spacing_hz.is_finite() {
        min_spacing_hz * 1.0e-12
    } else {
        f64::EPSILON
    };

    if let Some(index) = source_channel_frequencies_hz
        .iter()
        .position(|center| (*center - frequency_hz).abs() <= center_tolerance_hz)
    {
        return vec![CubeChannelContribution {
            source_channel: index,
            source_frequency_hz: native_source_channel_frequencies_hz[index],
            factor: 1.0,
        }];
    }

    let ascending = source_channel_frequencies_hz[nchan - 1] >= source_channel_frequencies_hz[0];
    let first_center_hz = source_channel_frequencies_hz[0];
    let last_index = nchan - 1;
    let last_center_hz = source_channel_frequencies_hz[last_index];
    if clip_to_edge_channel {
        if ascending {
            let first_outer_edge_hz = first_center_hz - source_channel_widths_hz[0].abs() / 2.0;
            let last_outer_edge_hz =
                last_center_hz + source_channel_widths_hz[last_index].abs() / 2.0;
            if first_outer_edge_hz <= frequency_hz && frequency_hz < first_center_hz {
                return vec![CubeChannelContribution {
                    source_channel: 0,
                    source_frequency_hz: native_source_channel_frequencies_hz[0],
                    factor: 1.0,
                }];
            }
            if last_center_hz < frequency_hz && frequency_hz <= last_outer_edge_hz {
                return vec![CubeChannelContribution {
                    source_channel: last_index,
                    source_frequency_hz: native_source_channel_frequencies_hz[last_index],
                    factor: 1.0,
                }];
            }
        } else {
            let first_outer_edge_hz = first_center_hz + source_channel_widths_hz[0].abs() / 2.0;
            let last_outer_edge_hz =
                last_center_hz - source_channel_widths_hz[last_index].abs() / 2.0;
            if first_center_hz < frequency_hz && frequency_hz <= first_outer_edge_hz {
                return vec![CubeChannelContribution {
                    source_channel: 0,
                    source_frequency_hz: native_source_channel_frequencies_hz[0],
                    factor: 1.0,
                }];
            }
            if last_outer_edge_hz <= frequency_hz && frequency_hz < last_center_hz {
                return vec![CubeChannelContribution {
                    source_channel: last_index,
                    source_frequency_hz: native_source_channel_frequencies_hz[last_index],
                    factor: 1.0,
                }];
            }
        }
    }

    let interval = if ascending {
        source_channel_frequencies_hz
            .windows(2)
            .position(|pair| pair[0] <= frequency_hz && frequency_hz <= pair[1])
    } else {
        source_channel_frequencies_hz
            .windows(2)
            .position(|pair| pair[1] <= frequency_hz && frequency_hz <= pair[0])
    };
    let Some(lower_index) = interval else {
        return Vec::new();
    };

    let upper_index = lower_index + 1;
    let lower_freq = source_channel_frequencies_hz[lower_index];
    let upper_freq = source_channel_frequencies_hz[upper_index];
    let span = upper_freq - lower_freq;
    if !span.is_finite() || span == 0.0 {
        return vec![CubeChannelContribution {
            source_channel: lower_index,
            source_frequency_hz: native_source_channel_frequencies_hz[lower_index],
            factor: 1.0,
        }];
    }

    let upper_factor = ((frequency_hz - lower_freq) / span) as f32;
    let lower_factor = 1.0 - upper_factor;
    let mut contributions = Vec::with_capacity(2);
    if lower_factor > 0.0 {
        contributions.push(CubeChannelContribution {
            source_channel: lower_index,
            source_frequency_hz: native_source_channel_frequencies_hz[lower_index],
            factor: lower_factor,
        });
    }
    if upper_factor > 0.0 {
        contributions.push(CubeChannelContribution {
            source_channel: upper_index,
            source_frequency_hz: native_source_channel_frequencies_hz[upper_index],
            factor: upper_factor,
        });
    }
    contributions
}
