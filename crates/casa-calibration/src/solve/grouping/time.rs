// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row order and time bins for gain solutions.

use std::collections::BTreeMap;

use super::{
    BucketedRow, GainSolveCombine, GainSolveInterval, SelectedSolveRow, SolveAccumulator,
    SolveBaseKey, SolveBucketKey, SolveSampleRow,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct AveragedBaseline {
    pub(super) antennas: (i32, i32),
    pub(super) field_id: i32,
    pub(super) data_desc_id: i32,
    pub(super) start_bits: u64,
}

impl AveragedBaseline {
    /// A common interval and centroid for all baselines in the old iterator.
    pub(super) fn time_bucket_key(self, shared: bool) -> Self {
        Self {
            antennas: if shared { (-1, -1) } else { self.antennas },
            ..self
        }
    }
}

/// The visibility input used by one solve family.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SolveInputRows {
    /// Use cross-correlations after reading solver metadata.
    #[default]
    Interferometry,
    /// Also retain the input buffer axes for a fringe FFT.
    Fringe,
    /// Keep the individual antennas' autocorrelations for normalization.
    Autocorrelations,
}

impl SolveInputRows {
    pub(super) fn receptors(
        self,
        correlation_type: i32,
        index: usize,
        count: usize,
    ) -> Option<(usize, usize)> {
        if self != Self::Autocorrelations {
            return super::correlation_receptors(correlation_type);
        }
        // AccorJones reads the first and last elements of a four-correlation
        // row and both elements of a two-correlation row in storage order.
        Some(match (count, index) {
            (_, 0) => (0, 0),
            (2, 1) | (4, 3) => (1, 1),
            _ => (0, 1),
        })
    }
}

/// A weighted mean of times.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct WeightedTime {
    origin: f64,
    weighted_offset: f64,
    weight: f64,
}

impl WeightedTime {
    pub(super) fn observe(&mut self, time: f64, weight: f64) {
        self.add(time, 0.0, weight);
    }

    pub(super) fn observe_mean(&mut self, centroid: Self, weight: f64) {
        if centroid.weight > 0.0 {
            self.add(
                centroid.origin,
                centroid.weighted_offset * (weight / centroid.weight),
                weight,
            );
        }
    }

    pub(super) fn mean(self) -> Option<f64> {
        (self.weight > 0.0).then(|| self.origin + self.weighted_offset / self.weight)
    }

    #[cfg(test)]
    pub(super) fn merge(&mut self, other: Self) {
        self.add(other.origin, other.weighted_offset, other.weight);
    }

    fn add(&mut self, origin: f64, weighted_offset: f64, weight: f64) {
        // Measure offsets before weighting. Summing absolute MS epochs
        // can split equal caltable times depending on baseline order.
        if self.weight == 0.0 {
            self.origin = origin;
        }
        self.weighted_offset += weighted_offset + weight * (origin - self.origin);
        self.weight += weight;
    }
}

impl SolveAccumulator {
    pub(super) fn cross_hand_centroid_weight(&mut self, weight: f64, input: SolveInputRows) -> f64 {
        if input == SolveInputRows::Autocorrelations {
            // CASA tests buffer viability before dropping cross-hand weights.
            self.attempted |= weight > 0.;
            return 0.;
        }
        weight
    }

    pub(in crate::solve) fn first_input_buffer(&self) -> Option<(i32, i32)> {
        self.first_t_buffer
            .map(|buffer| (buffer.field, buffer.data_desc_id))
    }

    pub(super) fn observe_metadata(&mut self, row: &SolveSampleRow, retain_fringe_buffers: bool) {
        if retain_fringe_buffers {
            self.observe_fringe_buffer(row);
        }
        self.min_time = self.min_time.min(row.time_seconds);
        self.max_time = self.max_time.max(row.time_seconds);
        if self.unique_time_bits.insert(row.time_seconds.to_bits()) {
            self.unique_time_sum += row.time_seconds;
            self.unique_time_count += 1;
        }
        self.total_interval += row.interval_seconds;
        self.sample_rows += 1;
        self.field_ids.insert(row.field_id);
        self.scan_numbers.insert(row.scan_number);
        self.antenna_ids.insert(row.antenna1);
        self.antenna_ids.insert(row.antenna2);
    }

    pub(super) fn observe_fringe_buffer(&mut self, row: &super::SolveSampleRow) {
        self.fringe_buffers
            .insert((row.data_desc_id, row.field_id, row.time_seconds.to_bits()));
    }

    pub(in crate::solve) fn integration_times(&self) -> impl Iterator<Item = f64> + '_ {
        self.unique_time_bits.iter().copied().map(f64::from_bits)
    }
}

/// Each selected row of `rows`, in solve order, with the group and
/// interval it is solved in.
///
/// CASA's iteration sorts by the axes `combine` keeps apart and then by
/// time, so the rows of a group come in time order whatever their fields
/// and scans, and a `solint` interval starts at its group's earliest time.
pub(super) fn bucketed_rows(
    rows: &[SelectedSolveRow],
    solve_interval: GainSolveInterval,
    combine: GainSolveCombine,
    include_autocorrelations: bool,
    legacy_intervals: bool,
) -> Vec<BucketedRow<'_>> {
    let mut keyed = rows
        .iter()
        .filter(|row| include_autocorrelations || row.antenna1 != row.antenna2)
        .map(|row| (row, SolveBaseKey::for_row(row, combine)))
        .collect::<Vec<_>>();
    keyed.sort_by(|(a, a_key), (b, b_key)| {
        a_key
            .cmp(b_key)
            .then(a.time_seconds.total_cmp(&b.time_seconds))
            .then(a.interval_seconds.total_cmp(&b.interval_seconds))
            .then(a.row_index.cmp(&b.row_index))
    });

    let mut bucket_starts = BTreeMap::<SolveBaseKey, (f64, u64)>::new();
    keyed
        .into_iter()
        .map(|(row, base_key)| {
            let bucket_key = match solve_interval {
                GainSolveInterval::Infinite => SolveBucketKey::Infinite,
                GainSolveInterval::Integration => SolveBucketKey::Integration {
                    time_bits: row.time_seconds.to_bits(),
                    interval_bits: if combine.spectral_windows {
                        0
                    } else {
                        row.interval_seconds.to_bits()
                    },
                },
                GainSolveInterval::Seconds(seconds) => {
                    let (start, number) = bucket_starts.entry(base_key).or_insert((
                        if legacy_intervals {
                            row.time_seconds
                        } else {
                            row.time_seconds - 0.01
                        },
                        0,
                    ));
                    let interval = f64::from(seconds as f32);
                    let elapsed = row.time_seconds - *start;
                    if legacy_intervals {
                        // Old MSIter restarts its interval at the first row
                        // after the previous interval's time limit.
                        if elapsed >= interval {
                            *start = row.time_seconds;
                            *number += 1;
                        }
                        SolveBucketKey::Seconds(*number)
                    } else {
                        // MSIter2 uses a fixed grid whose origin is 10 ms
                        // before the first timestamp.
                        SolveBucketKey::Seconds((elapsed / interval).floor().max(0.0) as u64)
                    }
                }
            };
            (row, base_key, bucket_key)
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FirstTBuffer {
    pub(super) field: i32,
    pub(super) data_desc_id: i32,
    pub(super) spw_id: i32,
    pub(super) observation_id: i32,
    pub(super) scan_number: i32,
    pub(super) time: f64,
    pub(super) max_span: Option<f64>,
    pub(super) skipped_baseline: (i32, i32),
}

impl FirstTBuffer {
    pub(super) fn contains(self, baseline: AveragedBaseline) -> bool {
        baseline.field_id == self.field
            && baseline.data_desc_id == self.data_desc_id
            && self
                .max_span
                .is_none_or(|span| f64::from_bits(baseline.start_bits) - self.time <= span)
    }
}

/// First buffer seen by `TJones::guessPar`. The CASA
/// initializer starts at row 1. The skipped row can have zero weight or be
/// an autocorrelation, so decide this using row metadata before reading DATA.
pub(super) fn first_t_buffers(
    rows: &[BucketedRow<'_>],
    max_span: Option<f64>,
) -> BTreeMap<(SolveBaseKey, SolveBucketKey), FirstTBuffer> {
    let mut first = BTreeMap::new();
    for &(row, base, bucket) in rows {
        let seed = first.entry((base, bucket)).or_insert(row);
        // BaselineIndex::configure uses the row order of the first input
        // buffer, including rows later ignored by the solver.
        if (
            row.field_id,
            row.data_desc_id,
            row.time_seconds,
            row.row_index,
        ) < (
            seed.field_id,
            seed.data_desc_id,
            seed.time_seconds,
            seed.row_index,
        ) {
            *seed = row;
        }
    }
    first
        .into_iter()
        .map(|(key, row)| {
            (
                key,
                FirstTBuffer {
                    field: row.field_id,
                    data_desc_id: row.data_desc_id,
                    spw_id: row.data_spw_id,
                    observation_id: row.observation_id,
                    scan_number: row.scan_number,
                    time: row.time_seconds,
                    max_span,
                    skipped_baseline: (row.antenna1, row.antenna2),
                },
            )
        })
        .collect()
}
