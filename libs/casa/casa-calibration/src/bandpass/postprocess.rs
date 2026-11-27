// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reference phase, normalization and gap interpolation for B solution rows.

use std::collections::BTreeMap;

use casa_core::Complex32;

use super::{BandpassSolutionRow, BandpassSolveControls, BandpassSolveRequest};
use crate::solve::{GainNormalization, GainSolveMode, grouping::median_with_even_mean, phase_only};
use crate::solve_stats::SolveStatsBuilder;

/// Apply CASA's flex reference phase to the [receptor, channel] grid. A
/// reference must have a valid channel in every solved receptor in both
/// intervals, but its phase is removed only where it has a valid channel.
pub(super) fn reference_bandpass_rows(
    rows: &mut [BandpassSolutionRow],
    preferred: i32,
    alternates: &[i32],
    strict: bool,
    stats: &mut SolveStatsBuilder,
) {
    let mut intervals = BTreeMap::<_, Vec<usize>>::new();
    let mut receptor_counts = BTreeMap::<i32, usize>::new();
    for (index, row) in rows.iter().enumerate() {
        intervals
            .entry((row.spw_id, row.time_seconds.to_bits()))
            .or_default()
            .push(index);
        let count = receptor_counts.entry(row.spw_id).or_insert(1);
        if row.flags.iter().skip(1).step_by(2).any(|flag| !flag) {
            *count = 2;
        }
    }
    let mut choices = vec![preferred, preferred];
    choices.extend_from_slice(alternates);
    let mut previous = None::<Vec<usize>>;
    let mut previous_spw = None;
    for ((spw, _), current) in intervals {
        if previous_spw != Some(spw) {
            previous = None;
            previous_spw = Some(spw);
        }
        let prior = previous.as_deref().unwrap_or(&current);
        let receptor_count = receptor_counts[&spw];
        let candidates = if strict { &choices[..1] } else { &choices };
        let reference = candidates.iter().find_map(|&id| {
            let current_row = usable_reference(rows, &current, id, receptor_count)?;
            let previous_row = usable_reference(rows, prior, id, receptor_count)?;
            Some((id, current_row, previous_row))
        });
        let Some((refant, current_ref, previous_ref)) = reference else {
            for index in current {
                rows[index].flags.fill(true);
            }
            continue;
        };
        let phases = (0..rows[current_ref].gains.len())
            .map(|index| {
                let now = channel_phase(&rows[current_ref], index);
                if previous.is_none() {
                    now
                } else {
                    now / channel_phase(&rows[previous_ref], index)
                }
            })
            .collect::<Vec<_>>();
        choices[1] = refant;
        stats.used_as_refant(spw, refant);
        for &index in &current {
            let row = &mut rows[index];
            for (gain, phase) in row.gains.iter_mut().zip(&phases) {
                *gain /= *phase;
            }
            row.refant_id = refant;
        }
        previous = Some(current);
    }
}

fn usable_reference(
    rows: &[BandpassSolutionRow],
    indices: &[usize],
    antenna: i32,
    receptors: usize,
) -> Option<usize> {
    indices
        .iter()
        .copied()
        .find(|&index| rows[index].antenna_id == antenna)
        .filter(|&index| {
            (0..receptors).all(|receptor| {
                rows[index]
                    .flags
                    .iter()
                    .skip(receptor)
                    .step_by(2)
                    .any(|flag| !flag)
            })
        })
}

fn channel_phase(row: &BandpassSolutionRow, index: usize) -> Complex32 {
    if row.flags.get(index).copied().unwrap_or(true) {
        Complex32::new(1.0, 0.0)
    } else {
        phase_only(row.gains[index])
    }
}

pub(super) fn finish_bandpass_rows(
    rows: &mut [BandpassSolutionRow],
    request: &BandpassSolveRequest,
    controls: BandpassSolveControls,
) {
    for row in rows {
        if controls.solve_mode != GainSolveMode::AmplitudePhase {
            for (gain, flagged) in row.gains.iter_mut().zip(&row.flags) {
                if *flagged {
                    *gain = Complex32::new(1.0, 0.0);
                } else if controls.solve_mode == GainSolveMode::Amplitude {
                    *gain = Complex32::new(gain.norm(), 0.0);
                }
            }
        }
        if request.normalize_average_amplitude {
            normalize_bandpass_row(row, row.channel_count, controls.normalization);
        }
        if let Some(cuts) = request.cuts.filter(|cuts| cuts.fill_gaps > 0) {
            fill_bandpass_gaps(row, cuts.fill_gaps);
        }
    }
}

pub(super) fn normalize_bandpass_rows(rows: &mut [BandpassSolutionRow], channel_count: usize) {
    for row in rows {
        normalize_bandpass_row(row, channel_count, GainNormalization::Mean);
    }
}

fn normalize_bandpass_row(
    row: &mut BandpassSolutionRow,
    channel_count: usize,
    normalization: GainNormalization,
) {
    if channel_count == 0 {
        return;
    }
    for receptor in 0..2 {
        let mut coherent_sum = Complex32::new(0.0, 0.0);
        let mut power_sum = 0.0_f32;
        let mut powers = Vec::new();
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
            let power = amplitude * amplitude;
            match normalization {
                GainNormalization::Mean => power_sum += power,
                GainNormalization::Median => powers.push(power),
            }
            good_count += 1;
        }
        if good_count <= 1 {
            continue;
        }
        let amplitude_factor = match normalization {
            GainNormalization::Mean => (power_sum / good_count as f32).sqrt(),
            GainNormalization::Median => median_with_even_mean(&mut powers).sqrt(),
        };
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
