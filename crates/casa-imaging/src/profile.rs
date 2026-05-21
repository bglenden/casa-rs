// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lightweight standard-MFS profiling helpers.

use std::{
    env,
    time::{Duration, Instant},
};

const STANDARD_MFS_PROFILE_DETAIL_ENV: &str = "CASA_RS_STANDARD_MFS_PROFILE_DETAIL";
const STANDARD_MFS_PROFILE_BLOCK_DETAIL_ENV: &str = "CASA_RS_STANDARD_MFS_PROFILE_BLOCK_DETAIL";

/// Return true when detailed standard-MFS profiling lines should be emitted.
pub(crate) fn standard_mfs_profile_detail_enabled() -> bool {
    env::var_os(STANDARD_MFS_PROFILE_DETAIL_ENV).is_some()
}

/// Return true when row-block level standard-MFS profiling lines should be emitted.
pub(crate) fn standard_mfs_profile_block_detail_enabled() -> bool {
    standard_mfs_profile_detail_enabled()
        && env::var_os(STANDARD_MFS_PROFILE_BLOCK_DETAIL_ENV).is_some()
}

/// Return a timestamp only when detailed profiling is enabled.
pub(crate) fn maybe_profile_now() -> Option<Instant> {
    standard_mfs_profile_detail_enabled().then(Instant::now)
}

/// Return elapsed time for an optional profiling timestamp.
pub(crate) fn elapsed_since(started_at: Option<Instant>) -> Duration {
    started_at.map_or(Duration::ZERO, |started_at| started_at.elapsed())
}

/// Return elapsed time in milliseconds.
pub(crate) fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

/// Return min/p50/max for a possibly empty `usize` sample.
pub(crate) fn min_p50_max_usize(values: &[usize]) -> (usize, usize, usize) {
    if values.is_empty() {
        return (0, 0, 0);
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    let mid = values.len() / 2;
    (values[0], values[mid], values[values.len() - 1])
}

/// Return min/p50/max for a possibly empty duration sample.
pub(crate) fn min_p50_max_duration(values: &[Duration]) -> (Duration, Duration, Duration) {
    if values.is_empty() {
        return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    let mid = values.len() / 2;
    (values[0], values[mid], values[values.len() - 1])
}

/// Profile summary for one parallel standard-MFS stage.
pub(crate) struct ParallelStageProfile<'a> {
    /// Stage name.
    pub(crate) stage: &'a str,
    /// Requested worker count before batch/sample and hardware caps.
    pub(crate) requested_threads: usize,
    /// Actual worker count used.
    pub(crate) actual_threads: usize,
    /// Chunking unit, for example `batch` or `planned_sample`.
    pub(crate) chunking: &'a str,
    /// Chunk length in chunking units.
    pub(crate) chunk_len: usize,
    /// Total sample count represented by all chunks.
    pub(crate) samples_total: usize,
    /// Samples assigned to each worker.
    pub(crate) samples_per_worker: Vec<usize>,
    /// Bytes of local grid storage per worker.
    pub(crate) local_grid_bytes_per_worker: usize,
    /// Number of local grids allocated per worker.
    pub(crate) local_grid_count: usize,
    /// Per-worker local allocation and zero-fill duration.
    pub(crate) local_alloc_zero_by_worker: Vec<Duration>,
    /// Per-worker compute duration.
    pub(crate) worker_compute_by_worker: Vec<Duration>,
    /// Time spent joining workers.
    pub(crate) join_duration: Duration,
    /// Time spent merging local grids or worker outputs.
    pub(crate) merge_duration: Duration,
    /// Total stage duration.
    pub(crate) stage_duration: Duration,
}

/// Emit one structured parallel-stage profiling line when enabled.
pub(crate) fn log_parallel_stage(profile: ParallelStageProfile<'_>) {
    if !standard_mfs_profile_detail_enabled() {
        return;
    }
    let (samples_min, samples_p50, samples_max) = min_p50_max_usize(&profile.samples_per_worker);
    let (alloc_min, alloc_p50, alloc_max) =
        min_p50_max_duration(&profile.local_alloc_zero_by_worker);
    let (compute_min, compute_p50, compute_max) =
        min_p50_max_duration(&profile.worker_compute_by_worker);
    let alloc_total = profile
        .local_alloc_zero_by_worker
        .iter()
        .copied()
        .sum::<Duration>();
    eprintln!(
        "standard_mfs_parallel_stage stage={} requested_threads={} actual_threads={} chunking={} chunk_len={} samples_total={} samples_per_worker_min={} samples_per_worker_p50={} samples_per_worker_max={} local_grid_bytes_per_worker={} local_grid_count={} local_alloc_zero_total_ms={:.3} local_alloc_zero_min_ms={:.3} local_alloc_zero_p50_ms={:.3} local_alloc_zero_max_ms={:.3} worker_compute_min_ms={:.3} worker_compute_p50_ms={:.3} worker_compute_max_ms={:.3} join_ms={:.3} merge_ms={:.3} stage_total_ms={:.3}",
        profile.stage,
        profile.requested_threads,
        profile.actual_threads,
        profile.chunking,
        profile.chunk_len,
        profile.samples_total,
        samples_min,
        samples_p50,
        samples_max,
        profile.local_grid_bytes_per_worker,
        profile.local_grid_count,
        millis(alloc_total),
        millis(alloc_min),
        millis(alloc_p50),
        millis(alloc_max),
        millis(compute_min),
        millis(compute_p50),
        millis(compute_max),
        millis(profile.join_duration),
        millis(profile.merge_duration),
        millis(profile.stage_duration),
    );
}

/// Profile summary for one worker inside a parallel standard-MFS stage.
pub(crate) struct ParallelWorkerProfile<'a> {
    /// Stage name.
    pub(crate) stage: &'a str,
    /// Stable worker index in chunk order.
    pub(crate) worker_index: usize,
    /// Raw samples assigned to this worker.
    pub(crate) samples: usize,
    /// Samples accepted for the stage's main useful work.
    pub(crate) accepted_samples: usize,
    /// Samples with finite visibilities.
    pub(crate) finite_visibility_samples: usize,
    /// Samples skipped because the visibility was not finite.
    pub(crate) nonfinite_visibility_samples: usize,
    /// Samples skipped because the row/channel was not gridable.
    pub(crate) skipped_not_gridable: usize,
    /// Samples skipped because the input weight was unusable.
    pub(crate) skipped_invalid_weight: usize,
    /// Samples skipped because the sum-weight factor was unusable.
    pub(crate) skipped_invalid_sumwt: usize,
    /// Samples skipped because the density value was unusable.
    pub(crate) skipped_invalid_density: usize,
    /// Samples skipped because the grid or density lookup was out of bounds.
    pub(crate) skipped_out_of_grid: usize,
    /// Estimated degrid tap visits.
    pub(crate) degrid_tap_visits: usize,
    /// Estimated grid tap visits.
    pub(crate) grid_tap_visits: usize,
    /// Density-grid cell updates performed by weighting stages.
    pub(crate) density_cell_hits: usize,
    /// Per-worker local allocation and zero-fill duration.
    pub(crate) local_alloc_zero: Duration,
    /// Per-worker compute duration.
    pub(crate) worker_compute: Duration,
}

/// Emit one structured per-worker profiling line when enabled.
pub(crate) fn log_parallel_worker(profile: ParallelWorkerProfile<'_>) {
    if !standard_mfs_profile_detail_enabled() {
        return;
    }
    eprintln!(
        "standard_mfs_parallel_worker stage={} worker_index={} samples={} accepted_samples={} finite_visibility_samples={} nonfinite_visibility_samples={} skipped_not_gridable={} skipped_invalid_weight={} skipped_invalid_sumwt={} skipped_invalid_density={} skipped_out_of_grid={} degrid_tap_visits={} grid_tap_visits={} density_cell_hits={} local_alloc_zero_ms={:.3} worker_compute_ms={:.3}",
        profile.stage,
        profile.worker_index,
        profile.samples,
        profile.accepted_samples,
        profile.finite_visibility_samples,
        profile.nonfinite_visibility_samples,
        profile.skipped_not_gridable,
        profile.skipped_invalid_weight,
        profile.skipped_invalid_sumwt,
        profile.skipped_invalid_density,
        profile.skipped_out_of_grid,
        profile.degrid_tap_visits,
        profile.grid_tap_visits,
        profile.density_cell_hits,
        millis(profile.local_alloc_zero),
        millis(profile.worker_compute),
    );
}

/// Profile summary for one serial standard-MFS attribution stage.
pub(crate) struct SerialStageProfile<'a> {
    /// Stage name.
    pub(crate) stage: &'a str,
    /// Total sample count seen by the stage.
    pub(crate) samples_total: usize,
    /// Samples with finite visibilities.
    pub(crate) finite_visibility_samples: usize,
    /// Samples skipped because the visibility was not finite.
    pub(crate) nonfinite_visibility_samples: usize,
    /// Accepted or planned samples that can visit gridder taps.
    pub(crate) planned_samples: usize,
    /// Samples processed with a model grid prediction.
    pub(crate) model_grid_present_samples: usize,
    /// Samples processed without a model grid prediction.
    pub(crate) model_grid_absent_samples: usize,
    /// Estimated degrid tap visits.
    pub(crate) degrid_tap_visits: usize,
    /// Estimated grid tap visits.
    pub(crate) grid_tap_visits: usize,
    /// Total stage duration.
    pub(crate) stage_duration: Duration,
}

/// Emit one structured serial attribution profiling line when enabled.
pub(crate) fn log_serial_stage(profile: SerialStageProfile<'_>) {
    if !standard_mfs_profile_detail_enabled() {
        return;
    }
    eprintln!(
        "standard_mfs_serial_stage stage={} samples_total={} finite_visibility_samples={} nonfinite_visibility_samples={} planned_samples={} model_grid_present_samples={} model_grid_absent_samples={} degrid_tap_visits={} grid_tap_visits={} stage_total_ms={:.3}",
        profile.stage,
        profile.samples_total,
        profile.finite_visibility_samples,
        profile.nonfinite_visibility_samples,
        profile.planned_samples,
        profile.model_grid_present_samples,
        profile.model_grid_absent_samples,
        profile.degrid_tap_visits,
        profile.grid_tap_visits,
        millis(profile.stage_duration),
    );
}
