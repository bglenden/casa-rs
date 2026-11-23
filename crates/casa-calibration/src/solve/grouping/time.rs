// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row order and time bins for gain solutions.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    BucketedRow, GainSolveCombine, GainSolveError, GainSolveInterval, SelectedSolveRow,
    SolveAccumulator, SolveBaseKey, SolveBucketKey, SolveSampleRow,
};
use crate::solve::{correlation_receptors, stokes_name};

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
    /// Retain autocorrelations as fixed residuals in direct antenna solves.
    GainTool,
    /// Keep direct gather statistics and stop once fitting would be required.
    GatherOnlyTool { legacy: bool },
    /// Also retain the input buffer axes for a fringe FFT.
    Fringe,
    /// Retain input axes and raw gather records for a direct fringe solve.
    FringeTool { legacy: bool },
    /// Keep the individual antennas' autocorrelations for normalization.
    Autocorrelations,
    /// Gather every selected row before fitting antenna autocorrelations.
    AccorTool,
    /// Gather ACCOR rows using CASA's older interval boundaries.
    LegacyAccor,
    /// Use the original iterator's restarting time intervals for a delay fit.
    LegacyDelay,
    /// Retain all antenna pairs for a baseline Mueller solve.
    Baselines,
    /// Retain all antenna pairs and the direct tool's baseline gather counts.
    BaselineTool,
    /// Retain all baselines for CASA's old self-gathering Mueller solver.
    LegacyBaseline,
    /// Preserve all correlations and selected frequency channels for an A fit.
    LegacyAdditivePolynomial,
}

impl SolveInputRows {
    pub(crate) fn gain(direct: bool) -> Self {
        if direct {
            Self::GainTool
        } else {
            Self::Interferometry
        }
    }

    pub(crate) fn baseline(direct: bool, legacy: bool) -> Self {
        if direct && legacy {
            Self::LegacyBaseline
        } else if direct {
            Self::BaselineTool
        } else {
            Self::Baselines
        }
    }

    pub(super) fn is_baseline(self) -> bool {
        matches!(
            self,
            Self::Baselines
                | Self::BaselineTool
                | Self::LegacyBaseline
                | Self::LegacyAdditivePolynomial
        )
    }

    pub(super) fn tracks_gather(self) -> bool {
        matches!(
            self,
            Self::GainTool
                | Self::GatherOnlyTool { .. }
                | Self::BaselineTool
                | Self::AccorTool
                | Self::LegacyAccor
                | Self::FringeTool { .. }
        )
    }

    pub(super) fn is_fringe(self) -> bool {
        matches!(self, Self::Fringe | Self::FringeTool { .. })
    }

    pub(super) fn is_accor(self) -> bool {
        matches!(
            self,
            Self::Autocorrelations | Self::AccorTool | Self::LegacyAccor
        )
    }

    pub(super) fn receptors(
        self,
        correlations: &[i32],
        index: usize,
        data_desc_id: i32,
    ) -> Result<(usize, usize), GainSolveError> {
        if self.is_baseline() && !(correlations.len() == 4 && matches!(index, 1 | 2)) {
            let parameter = index % 2;
            return Ok((parameter, parameter));
        }
        if !self.is_accor() && !self.is_baseline() {
            return correlation_receptors(correlations[index]).ok_or_else(|| {
                GainSolveError::UnsupportedCorrelationLayout {
                    data_desc_id,
                    correlation_types: correlations
                        .iter()
                        .map(|code| stokes_name(*code).to_string())
                        .collect(),
                }
            });
        }
        // CASA reads the first and last elements of a four-correlation
        // row and both elements of a two-correlation row in storage order.
        Ok(match (correlations.len(), index) {
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
    pub(super) fn apply_bucket_identity(
        &mut self,
        row: &super::SelectedSolveRow,
        options: &super::SolveGroupOptions,
    ) {
        if options.combine.fields {
            // SDBList::aggregateFld uses the smallest field in a combined interval.
            self.field_id = self.field_id.min(row.field_id);
        }
        if matches!(
            options.input_rows,
            SolveInputRows::LegacyBaseline | SolveInputRows::LegacyAdditivePolynomial
        ) {
            // The old averaged VisBuffer reports the ending observation.
            self.observation_id = row.observation_id;
        }
    }

    pub(super) fn cross_hand_centroid_weight(&mut self, weight: f64, input: SolveInputRows) -> f64 {
        if input.is_accor() {
            // CASA tests buffer viability before dropping cross-hand weights.
            self.attempted |= weight > 0.;
            if input == SolveInputRows::Autocorrelations {
                return 0.;
            }
        }
        weight
    }

    pub(in crate::solve) fn first_input_buffer(&self) -> Option<(i32, i32)> {
        self.first_t_buffer
            .map(|buffer| (buffer.field, buffer.data_desc_id))
    }

    pub(in crate::solve) fn in_first_input_buffer(
        &self,
        (ddid, field, start): (i32, i32, u64),
    ) -> bool {
        self.first_t_buffer
            .is_some_and(|buffer| buffer.contains_start(field, ddid, start))
    }

    pub(crate) fn direct_gain_data(&self) -> Option<([BTreeSet<i32>; 2], bool)> {
        self.direct_gain
            .as_ref()
            .map(|gather| (gather.with_data.clone(), gather.attempted))
    }

    pub(crate) fn gather_attempted(&self) -> bool {
        self.direct_gain
            .as_ref()
            .is_some_and(|gather| gather.attempted)
    }

    pub(crate) fn expects_cross_correlation(&self) -> bool {
        self.direct_gain.is_none() || !self.antenna_ids.is_empty()
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
        if self.direct_gain.is_none() || row.antenna1 != row.antenna2 {
            self.antenna_ids.insert(row.antenna1);
            self.antenna_ids.insert(row.antenna2);
        }
    }

    pub(super) fn observe_fringe_buffer(&mut self, row: &super::SolveSampleRow) {
        self.fringe_buffers
            .insert((row.data_desc_id, row.field_id, row.time_seconds.to_bits()));
    }

    pub(in crate::solve) fn integration_times(&self) -> impl Iterator<Item = f64> + '_ {
        self.unique_time_bits.iter().copied().map(f64::from_bits)
    }
}

impl super::SolveGroupOptions {
    pub(super) fn legacy_intervals(&self) -> bool {
        self.legacy_chan_selection()
            || matches!(
                self.input_rows,
                SolveInputRows::LegacyAccor
                    | SolveInputRows::LegacyDelay
                    | SolveInputRows::GatherOnlyTool { legacy: true }
            )
    }

    pub(super) fn legacy_chan_selection(&self) -> bool {
        self.polynomial_bandpass
            || matches!(
                self.input_rows,
                SolveInputRows::LegacyBaseline
                    | SolveInputRows::LegacyAdditivePolynomial
                    | SolveInputRows::FringeTool { legacy: true }
            )
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
    baseline_solve: bool,
) -> Vec<BucketedRow<'_>> {
    let mut keyed = rows
        .iter()
        .filter(|row| include_autocorrelations || row.antenna1 != row.antenna2)
        .map(|row| (row, SolveBaseKey::for_row(row, combine)))
        .collect::<Vec<_>>();
    if baseline_solve && combine.scans && !combine.fields {
        assign_field_visits(&mut keyed);
    }
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

/// CASA's time-averaging iterator starts a new baseline solution when a
/// scan-combined sequence changes field. The next visit to the same field
/// starts its own time interval, even with `solint='inf'`.
fn assign_field_visits(rows: &mut [(&SelectedSolveRow, SolveBaseKey)]) {
    let Some((first, _)) = rows.first() else {
        return;
    };
    if rows.iter().all(|(row, _)| row.field_id == first.field_id) {
        return;
    }
    let scope = |key: &SolveBaseKey| {
        (
            key.array_id,
            key.observation_id,
            key.spw_id,
            key.data_desc_id,
        )
    };
    rows.sort_by(|(left, left_key), (right, right_key)| {
        scope(left_key)
            .cmp(&scope(right_key))
            .then(left.time_seconds.total_cmp(&right.time_seconds))
            .then(left.field_id.cmp(&right.field_id))
            .then(left.row_index.cmp(&right.row_index))
    });
    let mut visit = 0;
    let mut previous = None;
    for (row, key) in rows {
        let current = (scope(key), row.field_id);
        if previous.is_some_and(|previous| previous != current) {
            visit += 1;
        }
        key.field_visit = visit;
        previous = Some(current);
    }
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
        self.contains_start(
            baseline.field_id,
            baseline.data_desc_id,
            baseline.start_bits,
        )
    }

    fn contains_start(self, field: i32, ddid: i32, start: u64) -> bool {
        field == self.field
            && ddid == self.data_desc_id
            && self
                .max_span
                .is_none_or(|span| f64::from_bits(start) - self.time <= span)
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
