// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's extend mode (#931): existing flags grown across time, frequency
//! and correlation, as CASA 6.7.6.14's extension agent grows them
//! (`FlagAgentExtension.cc`, `computeAntennaPairFlags`).
//!
//! The agent works on one baseline of one visibility-iterator chunk at a time:
//! 1. Grow from existing flags to neighboring times and channels.
//! 2. Per correlation, a timestep whose flagged channels reach `growfreq`
//!    percent is flagged in every channel, and a channel whose flagged
//!    timesteps reach `growtime` percent is flagged at every timestep. Both
//!    counts are from the flags after neighbor growth.
//! 3. With `extendpols`, a channel and timestep flagged in one
//!    correlation is flagged in every correlation.
//!
//! The agent reads flags only, so the data column plays no part.

use std::collections::{BTreeMap, BTreeSet};

use super::clip_time::ordered_baseline_chunks;
use super::{
    ChangeSet, ChannelSelection, CorrelationSelection, FlagDataRequest, FlaggingError,
    MeasurementSet, Sample, SampleLoadOptions, load_samples, stage_flag_mask,
    write_touched_flag_updates,
};

pub(super) fn apply(
    ms: &mut MeasurementSet,
    selected: &[usize],
    channels: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    request: &FlagDataRequest,
    path: &str,
) -> Result<ChangeSet, FlaggingError> {
    let mut changed = ChangeSet::default();
    if !valid_request(request) {
        changed.agent_skipped = true;
        return Ok(changed);
    }
    let timed = ordered_baseline_chunks(
        ms,
        selected,
        request.time_chunk,
        request.combinescans,
        path,
        request.cancellation.as_ref(),
    )?;
    // Stage changed cells only. Later invalid rows must not leave a
    // caller with a partially updated MeasurementSet open in memory.
    let mut pending = BTreeMap::new();
    for baseline in timed.chunk_by(|a, b| a.baseline_key() == b.baseline_key()) {
        request.check_cancelled()?;
        let rows = baseline.iter().map(|row| row.row).collect::<Vec<_>>();
        let mut loaded = load_samples(
            ms,
            &rows,
            channels,
            None,
            correlations,
            SampleLoadOptions {
                retain_channel_overlaps: request.casa_row_selection,
                cancellation: request.cancellation.as_ref(),
                ..SampleLoadOptions::default()
            },
        )?;
        let grown = grow(&loaded.samples, request);
        let (touched, changed_samples) = stage_flag_mask(
            &loaded.samples,
            &grown,
            &mut loaded.flags_by_row,
            request.action.value(),
        );
        changed.changed_samples += changed_samples;
        loaded.flags_by_row.retain(|row, _| touched.contains(row));
        pending.append(&mut loaded.flags_by_row);
    }
    let touched = pending.keys().copied().collect();
    write_touched_flag_updates(
        ms,
        pending,
        touched,
        |_| !request.casa_row_selection,
        &mut changed,
    )?;
    Ok(changed)
}

pub(super) fn valid_request(request: &FlagDataRequest) -> bool {
    // The CASA constructor uses these comparisons directly. A NaN disables
    // its percentage test, so neighbor growth and polarization can still run.
    let invalid = if request.extension_enabled {
        [
            ("growtime", request.growtime),
            ("growfreq", request.growfreq),
        ]
        .into_iter()
        .find(|(_, value)| !value.is_nan() && !(0.0..=100.0).contains(value))
        .map(|(parameter, value)| {
            format!("Unsupported value for {parameter}:{value}. Allowed range is 0.0 through 100.0")
        })
    } else {
        Some("Parameter 'extendpols' must be of type 'bool'".to_owned())
    };
    if let Some(reason) = invalid {
        tracing::warn!("Cannot create agent Extend: {reason}");
        tracing::error!("There is no agent to run in list");
        return false;
    }
    true
}

/// Flag mask for one baseline's selected samples in time order.
pub(super) fn grow(samples: &[Sample], request: &FlagDataRequest) -> Vec<bool> {
    let mut flags = samples.iter().map(|sample| sample.flag).collect::<Vec<_>>();
    let value = request.action.value();
    if value && (request.growaround || request.flagneartime || request.flagnearfreq) {
        grow_neighbors(samples, request, &mut flags);
    }
    grow_across_time_and_frequency(samples, request, &mut flags);
    if request.extendpols {
        extend_across_correlations(samples, &mut flags, value);
    }
    if !value {
        for (flag, sample) in flags.iter_mut().zip(samples) {
            *flag = sample.flag && !*flag;
        }
    }
    flags
}

fn grow_neighbors(samples: &[Sample], request: &FlagDataRequest, flags: &mut [bool]) {
    // Channel slots follow the selection's order, including gaps and
    // repeated references to the same stored cell in overlapping ranges.
    let mut positions = Vec::with_capacity(samples.len());
    for (time, row) in samples.chunk_by(|a, b| a.row == b.row).enumerate() {
        for plane in row.chunk_by(|a, b| a.corr == b.corr) {
            positions.extend((0..plane.len()).map(|channel| (plane[0].corr, time, channel)));
        }
    }
    let original = samples
        .iter()
        .zip(&positions)
        .filter_map(|(sample, &position)| sample.flag.then_some(position))
        .collect::<BTreeSet<_>>();
    let physical_cell = |sample: &Sample| (sample.row, sample.corr, sample.chan);
    let mut grown = BTreeSet::new();
    for (sample, &(corr, time, channel)) in samples.iter().zip(&positions) {
        if sample.flag {
            continue;
        }
        let neighbor = |dt: isize, dc: isize| {
            time.checked_add_signed(dt)
                .zip(channel.checked_add_signed(dc))
                .is_some_and(|(t, c)| original.contains(&(corr, t, c)))
        };
        if (request.flagneartime && (neighbor(-1, 0) || neighbor(1, 0)))
            || (request.flagnearfreq && (neighbor(0, -1) || neighbor(0, 1)))
            || (request.growaround
                && (-1..=1)
                    .flat_map(|dt| (-1..=1).map(move |dc| (dt, dc)))
                    .filter(|&(dt, dc)| neighbor(dt, dc))
                    .count()
                    > 4)
        {
            grown.insert(physical_cell(sample));
        }
    }
    // All references to a flag see the same write before percentage growth.
    for sample in samples {
        flags[sample.index] |= grown.contains(&physical_cell(sample));
    }
}

/// Percentage growth reads the original counts after the neighbor pass.
fn grow_across_time_and_frequency(
    baseline: &[Sample],
    request: &FlagDataRequest,
    flags: &mut [bool],
) {
    if !(request.growtime > 0.0 || request.growfreq > 0.0) {
        return;
    }
    // The flagged counts per correlation and timestep, over its channels,
    // and per correlation and channel, over its timesteps.
    let mut by_timestep = BTreeMap::<(usize, usize), (usize, usize)>::new();
    let mut by_channel = BTreeMap::<(usize, usize), (usize, usize)>::new();
    for sample in baseline {
        for (counts, key) in [
            (&mut by_timestep, (sample.corr, sample.row)),
            (&mut by_channel, (sample.corr, sample.chan)),
        ] {
            let (flagged, total) = counts.entry(key).or_default();
            *flagged += usize::from(flags[sample.index]);
            *total += 1;
        }
    }
    let reaches = |(flagged, total): (usize, usize), percent: f64| {
        percent > 0.0 && 100.0 * flagged as f64 / total as f64 >= percent
    };
    for sample in baseline {
        if reaches(by_timestep[&(sample.corr, sample.row)], request.growfreq)
            || reaches(by_channel[&(sample.corr, sample.chan)], request.growtime)
        {
            flags[sample.index] = request.action.value();
        }
    }
}

/// Flag every correlation of a channel and timestep with any flagged correlation.
fn extend_across_correlations(baseline: &[Sample], flags: &mut [bool], value: bool) {
    let mut flagged = BTreeMap::<(usize, usize), bool>::new();
    for sample in baseline {
        *flagged.entry((sample.row, sample.chan)).or_default() |= flags[sample.index];
    }
    for sample in baseline {
        if flagged[&(sample.row, sample.chan)] {
            flags[sample.index] = value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two timesteps of one baseline, XX and YY by eight channels.
    fn cube(flagged: &[(usize, usize, usize)]) -> Vec<Sample> {
        let mut samples = Vec::new();
        for row in 0..2 {
            for corr in 0..2 {
                for chan in 0..8 {
                    samples.push(Sample {
                        index: samples.len(),
                        row,
                        corr,
                        chan,
                        amp: 0.0,
                        real: 0.0,
                        imag: 0.0,
                        flag: flagged.contains(&(row, corr, chan)),
                        field: 0,
                        spw: 0,
                        scan: 1,
                        ant1: 0,
                        ant2: 1,
                    });
                }
            }
        }
        samples
    }

    /// Flagged channels per correlation of `row`.
    fn per_correlation(samples: &[Sample], flags: &[bool], row: usize) -> [usize; 2] {
        let mut counts = [0; 2];
        for sample in samples.iter().filter(|sample| sample.row == row) {
            counts[sample.corr] += usize::from(flags[sample.index]);
        }
        counts
    }

    fn request(growtime: f64, growfreq: f64, extendpols: bool) -> FlagDataRequest {
        FlagDataRequest {
            growtime,
            growfreq,
            extendpols,
            ..FlagDataRequest::default()
        }
    }

    #[test]
    fn growth_counts_each_correlation_on_its_own_then_extends_polarizations() {
        // Astra's #997 F3: four of XX's eight channels flagged on row 0,
        // none of YY's. CASA 6.7.6.14 grows XX's row to [8, 0], and with
        // extendpols to [8, 8]; pooling the correlations gave [4, 0] and
        // extending before growing gave [4, 4].
        let samples = cube(&[(0, 0, 0), (0, 0, 1), (0, 0, 2), (0, 0, 3)]);
        let grown = grow(&samples, &request(0.0, 50.0, false));
        assert_eq!(per_correlation(&samples, &grown, 0), [8, 0]);
        assert_eq!(per_correlation(&samples, &grown, 1), [0, 0]);
        let grown = grow(&samples, &request(0.0, 50.0, true));
        assert_eq!(per_correlation(&samples, &grown, 0), [8, 8]);
        assert_eq!(per_correlation(&samples, &grown, 1), [0, 0]);
    }

    #[test]
    fn growth_in_time_counts_each_correlations_timesteps() {
        // Channel 5 of XX is flagged on one of the two timesteps: 50% of
        // XX's timesteps, but 25% of the channel's samples over both
        // correlations.
        let samples = cube(&[(0, 0, 5)]);
        let grown = grow(&samples, &request(50.0, 0.0, false));
        let flagged = |row, corr, chan| {
            let sample = samples
                .iter()
                .find(|sample| (sample.row, sample.corr, sample.chan) == (row, corr, chan))
                .unwrap();
            grown[sample.index]
        };
        assert!(flagged(1, 0, 5));
        assert!(!flagged(1, 1, 5) && !flagged(0, 1, 5));
        assert_eq!(grown.iter().filter(|flag| **flag).count(), 2);
    }

    #[test]
    fn both_growths_count_the_flags_before_either_grows() {
        // XX row 0 has 4 of 8 channels flagged, so growfreq=50 fills it;
        // channel 7 was flagged on no timestep before, so growtime=50 must
        // not then flag it on row 1.
        let samples = cube(&[(0, 0, 0), (0, 0, 1), (0, 0, 2), (0, 0, 3)]);
        let grown = grow(&samples, &request(50.0, 50.0, false));
        assert_eq!(per_correlation(&samples, &grown, 0), [8, 0]);
        // Channels 0 to 3 of XX reach 50% of timesteps and grow to row 1.
        assert_eq!(per_correlation(&samples, &grown, 1), [4, 0]);
    }
}
