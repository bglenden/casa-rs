// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pure planning formulas for bounded parallel worker selection.
//!
//! Host discovery and policy remain frontend responsibilities. Imaging
//! kernels supply deterministic relative task weights and production-window
//! timing observations; this module combines those explicit inputs without
//! reading process or machine state.

use std::time::Duration;

/// Bounded production-window calibration requested by an application frontend.
///
/// The request is an internal execution-policy detail rather than a science
/// parameter. Candidate counts are generated from the resource slice assigned
/// by the frontend and are subsequently capped by the actual task set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParallelWorkerCalibrationRequest {
    coarse_candidates: Vec<usize>,
    hard_cap: usize,
    highest_capacity_class_boundary: Option<usize>,
    maximum_elapsed: Duration,
    reverse_calibration_order: bool,
}

impl ParallelWorkerCalibrationRequest {
    /// Constructs a validated bounded-calibration request.
    pub fn new(
        coarse_candidates: Vec<usize>,
        hard_cap: usize,
        highest_capacity_class_boundary: Option<usize>,
        maximum_elapsed: Duration,
    ) -> Result<Self, &'static str> {
        if hard_cap == 0 {
            return Err("parallel worker calibration hard cap must be positive");
        }
        if maximum_elapsed.is_zero() {
            return Err("parallel worker calibration duration must be positive");
        }
        if highest_capacity_class_boundary.is_some_and(|value| value == 0 || value > hard_cap) {
            return Err("parallel worker calibration capacity-class boundary exceeds the hard cap");
        }
        let mut coarse_candidates = coarse_candidates
            .into_iter()
            .filter(|value| *value > 0 && *value <= hard_cap)
            .collect::<Vec<_>>();
        coarse_candidates.sort_unstable();
        coarse_candidates.dedup();
        if coarse_candidates.is_empty() {
            return Err("parallel worker calibration has no admissible candidates");
        }
        Ok(Self {
            coarse_candidates,
            hard_cap,
            highest_capacity_class_boundary,
            maximum_elapsed,
            reverse_calibration_order: false,
        })
    }

    /// Reverses the counterbalanced production-window trial order.
    ///
    /// This remains an experiment-only validation control. It changes neither
    /// the admitted candidates nor the eventual execution policy.
    pub fn with_reversed_calibration_order(mut self, reverse: bool) -> Self {
        self.reverse_calibration_order = reverse;
        self
    }

    pub(crate) fn coarse_candidates(&self) -> &[usize] {
        &self.coarse_candidates
    }

    pub(crate) fn hard_cap(&self) -> usize {
        self.hard_cap
    }

    pub(crate) fn highest_capacity_class_boundary(&self) -> Option<usize> {
        self.highest_capacity_class_boundary
    }

    pub(crate) fn maximum_elapsed(&self) -> Duration {
        self.maximum_elapsed
    }

    pub(crate) fn reverse_calibration_order(&self) -> bool {
        self.reverse_calibration_order
    }
}

/// Generates at most four deterministic coarse worker candidates.
///
/// When heterogeneous capacity classes are known, the range begins at the
/// cumulative highest-capacity class boundary. Otherwise it spans the full
/// assigned range. Both endpoints are retained and integer results are
/// deduplicated.
pub fn topology_parallel_worker_candidates(
    assigned_parallelism: usize,
    highest_capacity_class_boundary: Option<usize>,
) -> Vec<usize> {
    let hard_cap = assigned_parallelism.max(1);
    let lower = highest_capacity_class_boundary
        .filter(|value| *value > 0 && *value <= hard_cap)
        .unwrap_or(1);
    let span = hard_cap - lower + 1;
    let candidate_count = span.min(4);
    if candidate_count == 1 {
        return vec![lower];
    }
    let denominator = candidate_count - 1;
    let distance = hard_cap - lower;
    let mut candidates = (0..candidate_count)
        .map(|index| lower + (index * distance + denominator / 2) / denominator)
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

pub(crate) fn modeled_lpt_makespan(weights: &[u64], workers: usize) -> Option<u128> {
    if weights.is_empty() || workers == 0 {
        return None;
    }
    let worker_count = workers.min(weights.len());
    let mut loads = vec![0u128; worker_count];
    let mut sorted_weights = weights.to_vec();
    sorted_weights.sort_unstable_by(|left, right| right.cmp(left));
    for weight in sorted_weights {
        let (worker, _) = loads
            .iter()
            .enumerate()
            .min_by_key(|(index, load)| (**load, *index))
            .expect("a positive worker count has one load");
        loads[worker] = loads[worker].saturating_add(u128::from(weight));
    }
    loads.into_iter().max()
}

/// Expands a sparse topology probe into the integer bracket around each probe.
///
/// Adjacent values make the production-window calibration sensitive to
/// scheduler plateaus without turning a large assigned CPU set into an
/// exhaustive sweep.
pub(crate) fn adjacent_parallel_worker_candidates(
    coarse_candidates: &[usize],
    hard_cap: usize,
) -> Vec<usize> {
    let hard_cap = hard_cap.max(1);
    let mut candidates = Vec::with_capacity(coarse_candidates.len().saturating_mul(3));
    for workers in coarse_candidates
        .iter()
        .copied()
        .filter(|workers| *workers > 0 && *workers <= hard_cap)
    {
        candidates.extend(
            [
                workers.saturating_sub(1).max(1),
                workers,
                workers.saturating_add(1).min(hard_cap),
            ]
            .into_iter(),
        );
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

/// Builds deterministic forward/reverse trial rounds.
pub(crate) fn counterbalanced_parallel_worker_sequence(
    candidates: &[usize],
    round_pairs: usize,
    reverse_first: bool,
) -> Vec<usize> {
    let mut ascending = candidates.to_vec();
    ascending.sort_unstable();
    ascending.dedup();
    let mut descending = ascending.clone();
    descending.reverse();
    let (first, second) = if reverse_first {
        (&descending, &ascending)
    } else {
        (&ascending, &descending)
    };
    let mut sequence = Vec::with_capacity(
        ascending
            .len()
            .saturating_mul(round_pairs.saturating_mul(2)),
    );
    for _ in 0..round_pairs {
        sequence.extend(first.iter().copied());
        sequence.extend(second.iter().copied());
    }
    sequence
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParallelWorkerTimingObservation {
    pub(crate) workers: usize,
    pub(crate) elapsed_ns: u128,
    pub(crate) work_units: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParallelWorkerCandidateScore {
    pub(crate) workers: usize,
    pub(crate) mean_elapsed_ns: u128,
    pub(crate) interval_min_score: u128,
    pub(crate) interval_max_score: u128,
    pub(crate) combined_score: u128,
}

fn student_t_95_critical(sample_count: usize) -> f64 {
    match sample_count {
        0 | 1 => 0.0,
        2 => 12.706,
        3 => 4.303,
        4 => 3.182,
        5 => 2.776,
        6 => 2.571,
        7 => 2.447,
        8 => 2.365,
        9 => 2.306,
        10 => 2.262,
        11..=15 => 2.201,
        16..=30 => 2.086,
        _ => 1.960,
    }
}

pub(crate) fn score_parallel_worker_candidates(
    observations: &[ParallelWorkerTimingObservation],
) -> Vec<ParallelWorkerCandidateScore> {
    const SCORE_WORK_UNITS: u128 = 1_000_000_000;
    let mut workers = observations
        .iter()
        .map(|observation| observation.workers)
        .collect::<Vec<_>>();
    workers.sort_unstable();
    workers.dedup();
    let mut scores = Vec::with_capacity(workers.len());
    for workers in workers {
        let samples = observations
            .iter()
            .filter(|observation| observation.workers == workers)
            .collect::<Vec<_>>();
        let elapsed_sum = samples
            .iter()
            .map(|observation| observation.elapsed_ns)
            .fold(0u128, u128::saturating_add);
        let mean_elapsed_ns = elapsed_sum / samples.len().max(1) as u128;
        let normalized_scores = samples
            .iter()
            .map(|observation| {
                observation.elapsed_ns.saturating_mul(SCORE_WORK_UNITS)
                    / observation.work_units.max(1)
            })
            .collect::<Vec<_>>();
        let combined_score = normalized_scores
            .iter()
            .copied()
            .fold(0u128, u128::saturating_add)
            / normalized_scores.len().max(1) as u128;
        let (interval_min_score, interval_max_score) = if normalized_scores.len() < 2 {
            (combined_score, combined_score)
        } else {
            let mean = combined_score as f64;
            let sample_variance = normalized_scores
                .iter()
                .map(|score| {
                    let delta = *score as f64 - mean;
                    delta * delta
                })
                .sum::<f64>()
                / (normalized_scores.len() - 1) as f64;
            let standard_error = (sample_variance / normalized_scores.len() as f64).sqrt();
            let margin = student_t_95_critical(normalized_scores.len()) * standard_error;
            (
                (mean - margin).max(0.0).floor() as u128,
                (mean + margin).ceil() as u128,
            )
        };
        scores.push(ParallelWorkerCandidateScore {
            workers,
            mean_elapsed_ns,
            interval_min_score,
            interval_max_score,
            combined_score,
        });
    }
    scores.sort_unstable_by_key(|score| (score.combined_score, score.workers));
    scores
}

pub(crate) fn choose_parallel_worker_with_topology_prior(
    scores: &[ParallelWorkerCandidateScore],
    highest_capacity_class_boundary: Option<usize>,
    hard_cap: usize,
) -> Option<(ParallelWorkerCandidateScore, Vec<usize>)> {
    let uncertainty_scores = parallel_worker_uncertainty_scores(scores);
    let mut uncertainty = uncertainty_scores
        .iter()
        .map(|score| score.workers)
        .collect::<Vec<_>>();
    uncertainty.sort_unstable();
    let empirical_best = scores.first().copied()?;
    let Some(boundary) = highest_capacity_class_boundary else {
        return Some((empirical_best, uncertainty));
    };
    let boundary = boundary.max(1).min(hard_cap.max(1));
    let efficiency_capacity = hard_cap.saturating_sub(boundary);
    let topology_prior = boundary.saturating_add(efficiency_capacity.div_ceil(2));
    let selected = uncertainty_scores
        .iter()
        .copied()
        .min_by_key(|score| {
            (
                score.workers.abs_diff(topology_prior),
                score.workers,
                score.combined_score,
            )
        })
        .unwrap_or(empirical_best);
    Some((selected, uncertainty))
}

pub(crate) fn parallel_worker_uncertainty_scores(
    scores: &[ParallelWorkerCandidateScore],
) -> Vec<ParallelWorkerCandidateScore> {
    let Some(best) = scores.first().copied() else {
        return Vec::new();
    };
    let mut uncertainty = scores
        .iter()
        .copied()
        .filter(|score| {
            score.interval_min_score <= best.interval_max_score
                && best.interval_min_score <= score.interval_max_score
        })
        .collect::<Vec<_>>();
    uncertainty.sort_unstable_by_key(|score| score.workers);
    uncertainty
}

pub(crate) fn conservative_parallel_worker_fallback(
    actual_weights: &[u64],
    candidates: &[usize],
    highest_capacity_class_boundary: Option<usize>,
) -> usize {
    if actual_weights.is_empty() {
        return 0;
    }
    let task_cap = actual_weights.len();
    if let Some(boundary) = highest_capacity_class_boundary {
        return boundary.max(1).min(task_cap);
    }
    candidates
        .iter()
        .copied()
        .filter(|workers| *workers > 0)
        .filter_map(|workers| {
            modeled_lpt_makespan(actual_weights, workers)
                .map(|makespan| (makespan, workers.min(task_cap)))
        })
        .min_by_key(|(makespan, workers)| (*makespan, *workers))
        .map_or(1, |(_, workers)| workers)
}

#[cfg(test)]
mod tests {
    use super::{
        ParallelWorkerTimingObservation, adjacent_parallel_worker_candidates,
        choose_parallel_worker_with_topology_prior, conservative_parallel_worker_fallback,
        counterbalanced_parallel_worker_sequence, modeled_lpt_makespan,
        score_parallel_worker_candidates, topology_parallel_worker_candidates,
    };

    #[test]
    fn heterogeneous_topology_generates_bounded_coarse_candidates() {
        assert_eq!(
            topology_parallel_worker_candidates(10, Some(4)),
            vec![4, 6, 8, 10]
        );
        assert_eq!(
            topology_parallel_worker_candidates(6, Some(4)),
            vec![4, 5, 6]
        );
        assert_eq!(
            topology_parallel_worker_candidates(8, None),
            vec![1, 3, 6, 8]
        );
        assert_eq!(topology_parallel_worker_candidates(1, Some(1)), vec![1]);
    }

    #[test]
    fn lpt_model_preserves_the_indivisible_heavy_task_bound() {
        let weights = [100, 10, 10, 10, 10, 10];
        assert_eq!(modeled_lpt_makespan(&weights, 1), Some(150));
        assert_eq!(modeled_lpt_makespan(&weights, 2), Some(100));
        assert_eq!(modeled_lpt_makespan(&weights, 6), Some(100));
        assert_eq!(modeled_lpt_makespan(&[], 4), None);
    }

    #[test]
    fn production_window_candidate_sequence_is_bounded_and_counterbalanced() {
        assert_eq!(
            adjacent_parallel_worker_candidates(&[4, 6, 8, 10], 10),
            vec![3, 4, 5, 6, 7, 8, 9, 10]
        );
        assert_eq!(
            counterbalanced_parallel_worker_sequence(&[4, 6, 8], 2, false),
            vec![4, 6, 8, 8, 6, 4, 4, 6, 8, 8, 6, 4]
        );
        assert_eq!(
            counterbalanced_parallel_worker_sequence(&[4, 6, 8], 1, true),
            vec![8, 6, 4, 4, 6, 8]
        );
    }

    #[test]
    fn normalized_production_windows_select_six_and_topology_breaks_overlap() {
        let observations = [
            ParallelWorkerTimingObservation {
                workers: 4,
                elapsed_ns: 800,
                work_units: 100,
            },
            ParallelWorkerTimingObservation {
                workers: 5,
                elapsed_ns: 660,
                work_units: 100,
            },
            ParallelWorkerTimingObservation {
                workers: 6,
                elapsed_ns: 500,
                work_units: 100,
            },
            ParallelWorkerTimingObservation {
                workers: 7,
                elapsed_ns: 560,
                work_units: 100,
            },
            ParallelWorkerTimingObservation {
                workers: 8,
                elapsed_ns: 620,
                work_units: 100,
            },
            ParallelWorkerTimingObservation {
                workers: 10,
                elapsed_ns: 700,
                work_units: 100,
            },
        ];
        let scores = score_parallel_worker_candidates(&observations);
        assert_eq!(scores.first().unwrap().workers, 6);

        let tied = [
            ParallelWorkerTimingObservation {
                workers: 5,
                elapsed_ns: 500,
                work_units: 100,
            },
            ParallelWorkerTimingObservation {
                workers: 6,
                elapsed_ns: 505,
                work_units: 100,
            },
            ParallelWorkerTimingObservation {
                workers: 5,
                elapsed_ns: 510,
                work_units: 100,
            },
            ParallelWorkerTimingObservation {
                workers: 6,
                elapsed_ns: 500,
                work_units: 100,
            },
        ];
        let tied_scores = score_parallel_worker_candidates(&tied);
        assert_eq!(
            choose_parallel_worker_with_topology_prior(&tied_scores, Some(4), 8)
                .unwrap()
                .0
                .workers,
            6
        );

        let broad_laptop_overlap = [5, 9, 6, 8, 10, 4, 7]
            .into_iter()
            .enumerate()
            .map(|(rank, workers)| super::ParallelWorkerCandidateScore {
                workers,
                mean_elapsed_ns: 700 + rank as u128,
                interval_min_score: 8_000,
                interval_max_score: 10_000,
                combined_score: 8_700 + rank as u128 * 50,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            choose_parallel_worker_with_topology_prior(&broad_laptop_overlap, Some(4), 10)
                .unwrap()
                .0
                .workers,
            7
        );

        let four_window_small_sample = [
            (5, 850),
            (5, 812),
            (5, 851),
            (5, 844),
            (7, 878),
            (7, 883),
            (7, 858),
            (7, 865),
        ]
        .map(|(workers, elapsed_ns)| ParallelWorkerTimingObservation {
            workers,
            elapsed_ns,
            work_units: 100,
        });
        let four_window_scores = score_parallel_worker_candidates(&four_window_small_sample);
        assert_eq!(four_window_scores.first().unwrap().workers, 5);
        assert_eq!(
            choose_parallel_worker_with_topology_prior(&four_window_scores, Some(4), 10)
                .unwrap()
                .0
                .workers,
            7
        );
    }

    #[test]
    fn conservative_fallback_uses_capacity_boundary_or_smallest_modeled_winner() {
        let weights = [100, 100, 100];
        assert_eq!(
            conservative_parallel_worker_fallback(&weights, &[1, 2, 3, 4], Some(4)),
            3
        );
        assert_eq!(
            conservative_parallel_worker_fallback(&weights, &[1, 2, 3, 4], None),
            3
        );
        assert_eq!(
            conservative_parallel_worker_fallback(&[], &[1, 2, 3], Some(2)),
            0
        );
    }
}
