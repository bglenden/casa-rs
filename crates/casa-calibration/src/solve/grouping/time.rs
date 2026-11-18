// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row order and time bins for gain solutions.

use std::collections::BTreeMap;

use super::{
    AveragedBaseline, BucketedRow, GainSolveCombine, GainSolveInterval, SelectedSolveRow,
    SolveBaseKey, SolveBucketKey,
};

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
                spw_id: row.data_spw_id,
                observation_id: row.observation_id,
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
                    interval_bits: row.interval_seconds.to_bits(),
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
    pub(super) time: f64,
    pub(super) max_span: Option<f64>,
    pub(super) skipped_baseline: (i32, i32),
}

impl FirstTBuffer {
    pub(super) fn contains(self, baseline: AveragedBaseline) -> bool {
        baseline.field_id == self.field
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
        if (row.field_id, row.time_seconds, row.row_index)
            < (seed.field_id, seed.time_seconds, seed.row_index)
        {
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
                    time: row.time_seconds,
                    max_span,
                    skipped_baseline: (row.antenna1, row.antenna2),
                },
            )
        })
        .collect()
}
