// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pure planning formulas for bounded parallel worker selection.
//!
//! Host discovery and policy remain frontend responsibilities. Imaging
//! kernels supply deterministic relative task weights and exact-kernel timing
//! observations; this module combines those explicit inputs without reading
//! process or machine state.

use std::time::Duration;

/// Bounded exact-kernel calibration requested by an application frontend.
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
    score_tie_tolerance_ppm: u32,
}

impl ParallelWorkerCalibrationRequest {
    /// Constructs a validated bounded-calibration request.
    pub fn new(
        coarse_candidates: Vec<usize>,
        hard_cap: usize,
        highest_capacity_class_boundary: Option<usize>,
        maximum_elapsed: Duration,
        score_tie_tolerance_ppm: u32,
    ) -> Result<Self, &'static str> {
        if hard_cap == 0 {
            return Err("parallel worker calibration hard cap must be positive");
        }
        if maximum_elapsed.is_zero() {
            return Err("parallel worker calibration duration must be positive");
        }
        if score_tie_tolerance_ppm > 1_000_000 {
            return Err("parallel worker calibration tie tolerance exceeds one");
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
            score_tie_tolerance_ppm,
        })
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

    pub(crate) fn score_tie_tolerance_ppm(&self) -> u32 {
        self.score_tie_tolerance_ppm
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParallelWorkerTimingObservation {
    pub(crate) workers: usize,
    pub(crate) elapsed_ns: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParallelWorkerCandidateScore {
    pub(crate) workers: usize,
    pub(crate) modeled_actual_makespan: u128,
    pub(crate) modeled_calibration_makespan: u128,
    pub(crate) mean_elapsed_ns: u128,
    pub(crate) repeat_spread_ns: u128,
    pub(crate) combined_score: u128,
}

pub(crate) fn score_parallel_worker_candidates(
    actual_weights: &[u64],
    calibration_weights: &[u64],
    observations: &[ParallelWorkerTimingObservation],
) -> Vec<ParallelWorkerCandidateScore> {
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
            .map(|observation| observation.elapsed_ns)
            .collect::<Vec<_>>();
        let Some(modeled_actual_makespan) = modeled_lpt_makespan(actual_weights, workers) else {
            continue;
        };
        let Some(modeled_calibration_makespan) = modeled_lpt_makespan(calibration_weights, workers)
        else {
            continue;
        };
        let elapsed_sum = samples.iter().copied().fold(0u128, u128::saturating_add);
        let mean_elapsed_ns = elapsed_sum / samples.len().max(1) as u128;
        let repeat_spread_ns = samples
            .iter()
            .copied()
            .max()
            .unwrap_or_default()
            .saturating_sub(samples.iter().copied().min().unwrap_or_default());
        let combined_score = mean_elapsed_ns.saturating_mul(modeled_actual_makespan)
            / modeled_calibration_makespan.max(1);
        scores.push(ParallelWorkerCandidateScore {
            workers,
            modeled_actual_makespan,
            modeled_calibration_makespan,
            mean_elapsed_ns,
            repeat_spread_ns,
            combined_score,
        });
    }
    scores.sort_unstable_by_key(|score| (score.combined_score, score.workers));
    scores
}

pub(crate) fn choose_parallel_worker_score(
    scores: &[ParallelWorkerCandidateScore],
    tie_tolerance_ppm: u32,
) -> Option<ParallelWorkerCandidateScore> {
    let best = *scores.first()?;
    let tolerance = best
        .combined_score
        .saturating_mul(u128::from(tie_tolerance_ppm))
        / 1_000_000;
    scores
        .iter()
        .copied()
        .filter(|score| score.combined_score <= best.combined_score.saturating_add(tolerance))
        .min_by_key(|score| score.workers)
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
        ParallelWorkerTimingObservation, choose_parallel_worker_score,
        conservative_parallel_worker_fallback, modeled_lpt_makespan,
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
    fn injected_calibration_selects_six_and_smaller_noisy_ties() {
        let actual = vec![100; 120];
        let calibration = vec![100; 24];
        let observations = [
            ParallelWorkerTimingObservation {
                workers: 4,
                elapsed_ns: 800,
            },
            ParallelWorkerTimingObservation {
                workers: 5,
                elapsed_ns: 660,
            },
            ParallelWorkerTimingObservation {
                workers: 6,
                elapsed_ns: 500,
            },
            ParallelWorkerTimingObservation {
                workers: 7,
                elapsed_ns: 560,
            },
            ParallelWorkerTimingObservation {
                workers: 8,
                elapsed_ns: 620,
            },
            ParallelWorkerTimingObservation {
                workers: 10,
                elapsed_ns: 700,
            },
        ];
        let scores = score_parallel_worker_candidates(&actual, &calibration, &observations);
        assert_eq!(
            choose_parallel_worker_score(&scores, 20_000)
                .unwrap()
                .workers,
            6
        );

        let tied = [
            ParallelWorkerTimingObservation {
                workers: 5,
                elapsed_ns: 500,
            },
            ParallelWorkerTimingObservation {
                workers: 6,
                elapsed_ns: 495,
            },
        ];
        let tied_scores = score_parallel_worker_candidates(&actual, &calibration, &tied);
        assert_eq!(
            choose_parallel_worker_score(&tied_scores, 20_000)
                .unwrap()
                .workers,
            5
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
