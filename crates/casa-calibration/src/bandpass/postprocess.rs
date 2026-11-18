// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reference phase, normalization and gap interpolation for B solution rows.

use casa_types::Complex32;

use super::{BandpassSolutionRow, BandpassSolveCuts, BandpassType};
use crate::solve::kernel::SolvedGraphs;

pub(super) fn bandpass_reference_phases(
    solved_channels: &[Option<SolvedGraphs>],
    refant_id: i32,
    cuts: Option<BandpassSolveCuts>,
    band_type: BandpassType,
) -> Vec<[Complex32; 2]> {
    solved_channels
        .iter()
        .map(|solved| {
            std::array::from_fn(|receptor| {
                let unity = Complex32::new(1.0, 0.0);
                if band_type == BandpassType::BPoly {
                    return unity;
                }
                solved
                    .as_ref()
                    .and_then(|solved| solved.receptors.get(receptor))
                    .filter(|fit| fit.solved.contains(&refant_id))
                    .filter(|fit| cuts.is_none_or(|cuts| fit.snr[&refant_id] > cuts.min_snr))
                    .map_or(unity, |fit| crate::solve::phase_only(fit.gains[&refant_id]))
            })
        })
        .collect()
}

pub(super) fn normalize_bandpass_rows(
    mut rows: Vec<BandpassSolutionRow>,
    channel_count: usize,
) -> Vec<BandpassSolutionRow> {
    for row in &mut rows {
        normalize_bandpass_row(row, channel_count);
    }
    rows
}

fn normalize_bandpass_row(row: &mut BandpassSolutionRow, channel_count: usize) {
    if channel_count == 0 {
        return;
    }
    for receptor in 0..2 {
        let mut coherent_sum = Complex32::new(0.0, 0.0);
        let mut power_sum = 0.0_f32;
        let mut good_count = 0usize;
        for channel in 0..channel_count {
            let index = receptor + 2 * channel;
            if row.flags.get(index).copied().unwrap_or(false) {
                continue;
            }
            let gain = row.gains[index];
            let amplitude = gain.norm();
            if amplitude <= f32::EPSILON {
                continue;
            }
            coherent_sum += gain / Complex32::new(amplitude, 0.0);
            power_sum += amplitude * amplitude;
            good_count += 1;
        }
        if good_count <= 1 {
            continue;
        }
        let amplitude_factor = (power_sum / good_count as f32).sqrt();
        if amplitude_factor <= f32::EPSILON {
            continue;
        }
        let phase_factor = if coherent_sum.norm() > f32::EPSILON {
            coherent_sum / Complex32::new(coherent_sum.norm(), 0.0)
        } else {
            Complex32::new(1.0, 0.0)
        };
        let normalization = phase_factor * Complex32::new(amplitude_factor, 0.0);
        for channel in 0..channel_count {
            let index = receptor + 2 * channel;
            row.gains[index] /= normalization;
        }
    }
}

pub(super) fn fill_bandpass_gaps(row: &mut BandpassSolutionRow, maximum: usize) {
    for receptor in 0..2 {
        let mut last_good = None;
        for channel in 0..row.channel_count {
            let right = receptor + 2 * channel;
            if row.flags[right] {
                continue;
            }
            if let Some(left) = last_good {
                let span = channel - left;
                if span > 1 && span - 1 <= maximum {
                    let lower = row.gains[receptor + 2 * left];
                    let upper = row.gains[right];
                    for gap in left + 1..channel {
                        let fraction = (gap - left) as f32 / span as f32;
                        let index = receptor + 2 * gap;
                        row.gains[index] =
                            crate::lookup::interpolate_gain_amplitude_phase(lower, upper, fraction);
                        row.flags[index] = false;
                    }
                }
            }
            last_good = Some(channel);
        }
    }
}
