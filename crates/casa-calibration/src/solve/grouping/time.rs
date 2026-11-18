// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row order and time bins for gain solutions.

use std::collections::BTreeMap;

use super::{
    BucketedRow, GainSolveCombine, GainSolveInterval, SelectedSolveRow, SolveBaseKey,
    SolveBucketKey,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct AveragedBaseline {
    pub(super) antennas: (i32, i32),
    pub(super) field_id: i32,
    pub(super) data_desc_id: i32,
    pub(super) start_bits: u64,
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
) -> Vec<BucketedRow<'_>> {
    let mut keyed = rows
        .iter()
        .filter(|row| include_autocorrelations || row.antenna1 != row.antenna2)
        .map(|row| {
            let base_key = SolveBaseKey {
                field_id: if combine.fields { 0 } else { row.field_id },
                spw_id: if combine.spectral_windows {
                    0
                } else {
                    row.data_spw_id
                },
                data_desc_id: if combine.spectral_windows {
                    0
                } else {
                    row.data_desc_id
                },
                observation_id: if combine.observations {
                    0
                } else {
                    row.observation_id
                },
                scan_number: if combine.scans { 0 } else { row.scan_number },
            };
            (row, base_key)
        })
        .collect::<Vec<_>>();
    keyed.sort_by(|(a, a_key), (b, b_key)| {
        a_key
            .cmp(b_key)
            .then(a.time_seconds.total_cmp(&b.time_seconds))
            .then(a.interval_seconds.total_cmp(&b.interval_seconds))
            .then(a.row_index.cmp(&b.row_index))
    });

    let mut bucket_starts = BTreeMap::<SolveBaseKey, f64>::new();
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
                    // MSIter2 starts its time grid 10 ms before the first
                    // timestamp and receives solint as a Float.
                    let start = *bucket_starts
                        .entry(base_key)
                        .or_insert(row.time_seconds - 0.01);
                    let index = ((row.time_seconds - start) / f64::from(seconds as f32))
                        .floor()
                        .max(0.0);
                    SolveBucketKey::Seconds(index as u64)
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
                    time: row.time_seconds,
                    max_span,
                    skipped_baseline: (row.antenna1, row.antenna2),
                },
            )
        })
        .collect()
}
