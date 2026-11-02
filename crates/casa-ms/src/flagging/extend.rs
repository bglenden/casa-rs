// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's extend mode (#931): existing flags grown across time, frequency
//! and correlation, as CASA 6.7.6.14's extension agent grows them
//! (`FlagAgentExtension.cc`, `computeAntennaPairFlags`).
//!
//! The agent works on one baseline of one chunk at a time (here a field,
//! spectral window and scan), a cube of correlations by channels by
//! timesteps:
//! 1. per correlation, a timestep whose flagged channels reach `growfreq`
//!    percent is flagged in every channel, and a channel whose flagged
//!    timesteps reach `growtime` percent is flagged at every timestep. Both
//!    counts are of the flags before either growth;
//! 2. then, with `extendpols`, a channel and timestep flagged in one
//!    correlation is flagged in every correlation.
//!
//! The agent reads flags only, so the data column plays no part.

use std::collections::BTreeMap;

use super::{FlagDataRequest, Sample, group_samples};

/// The flags of `samples`, indexed by [`Sample::index`], after `request`'s
/// growth: each sample's own flag, or a flag the growth gives it.
pub(super) fn grow(samples: &[Sample], request: &FlagDataRequest) -> Vec<bool> {
    let mut flags = samples.iter().map(|sample| sample.flag).collect::<Vec<_>>();
    for baseline in group_samples(samples).values() {
        grow_across_time_and_frequency(baseline, request, &mut flags);
        if request.extendpols {
            extend_across_correlations(baseline, &mut flags);
        }
    }
    flags
}

/// Step 1, for one baseline's samples.
fn grow_across_time_and_frequency(
    baseline: &[Sample],
    request: &FlagDataRequest,
    flags: &mut [bool],
) {
    if request.growtime <= 0.0 && request.growfreq <= 0.0 {
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
            *flagged += usize::from(sample.flag);
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
            flags[sample.index] = true;
        }
    }
}

/// Step 2, for one baseline's samples: every correlation of a channel and
/// timestep any of them flags.
fn extend_across_correlations(baseline: &[Sample], flags: &mut [bool]) {
    let mut flagged = BTreeMap::<(usize, usize), bool>::new();
    for sample in baseline {
        *flagged.entry((sample.row, sample.chan)).or_default() |= flags[sample.index];
    }
    for sample in baseline {
        if flagged[&(sample.row, sample.chan)] {
            flags[sample.index] = true;
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
