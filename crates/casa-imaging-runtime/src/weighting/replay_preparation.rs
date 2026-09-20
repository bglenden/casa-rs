// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded independent source-run preparation, followed by ordered consumption.

use super::{
    ReconstructionWeightedBlock, ReconstructionWeightedSample, ReplayCallbackError,
    StreamingWeightPhase, WeightingBlockKernelError, widen_terminal_traversal_error,
};
use casa_imaging_model::CompiledProblem;
use casa_imaging_reconstruction::runtime_adapter::WeightingSpectralCache;
use casa_imaging_reconstruction::{WeightingError, WeightingPlan};
use casa_ms::{
    SelectedObservationBlock, SelectedObservationBlockConsumer, SelectedObservationBlockIndex,
    SelectedObservationBlockIndexPlan, SelectedObservationProjector,
    SelectedObservationTraversalError,
};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplayPreparationPlan {
    index: SelectedObservationBlockIndexPlan,
    workers: usize,
    samples_per_worker: usize,
    bytes: u64,
}

impl ReplayPreparationPlan {
    pub(crate) fn new(
        problem: &CompiledProblem,
        weighting: &WeightingPlan,
        workers: usize,
    ) -> Result<Self, WeightingError> {
        let correlations = problem
            .selected_observation()
            .read_set()
            .sources()
            .iter()
            .flat_map(|source| source.selection().correlations())
            .map(|selection| selection.products().len())
            .max()
            .unwrap_or(0);
        if workers == 0 || correlations == 0 {
            return Err(WeightingError::ResidencyOverflow);
        }
        let runs = weighting.limits().max_block_samples() / correlations;
        if runs == 0 {
            return Err(WeightingError::ResidencyOverflow);
        }
        let index = SelectedObservationBlockIndexPlan::new(problem, runs)
            .map_err(|_| WeightingError::ResidencyOverflow)?;
        let samples_per_worker = runs
            .div_ceil(workers)
            .checked_mul(correlations)
            .ok_or(WeightingError::ResidencyOverflow)?;
        let residency = weighting.planned_residency();
        let sample_bytes =
            residency.weighted_block_bytes() / weighting.limits().max_block_samples();
        let worker_bytes = samples_per_worker
            .checked_mul(sample_bytes)
            .and_then(|bytes| {
                bytes.checked_add(
                    residency
                        .spectral_cache_bytes()
                        .checked_sub(size_of::<WeightingSpectralCache<'_>>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    SelectedObservationProjector::required_bytes(problem)?
                        .checked_sub(size_of::<SelectedObservationProjector>())?,
                )
            })
            .and_then(|bytes| bytes.checked_add(size_of::<PreparationWorker<'_>>()))
            .ok_or(WeightingError::ResidencyOverflow)?;
        let bytes = workers
            .checked_mul(worker_bytes)
            .and_then(|bytes| {
                bytes.checked_add(
                    index
                        .capacity_bytes()
                        .checked_sub(size_of::<SelectedObservationBlockIndex>())?,
                )
            })
            .and_then(|bytes| bytes.checked_add(size_of::<ReplayPreparation<'_>>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(WeightingError::ResidencyOverflow)?;
        Ok(Self {
            index,
            workers,
            samples_per_worker,
            bytes,
        })
    }

    pub(crate) const fn workers(self) -> usize {
        self.workers
    }
    pub(crate) fn admitted_heap_bytes(self) -> Result<u64, WeightingError> {
        let stacks = if self.workers > 1 {
            self.workers as u64 * crate::bounded_stream::BOUNDED_WORKER_STACK_BYTES as u64
        } else {
            0
        };
        crate::bounded_stream::BoundedKernelPlan::new::<(), ()>(self.workers, 1, 0)
            .ok()
            .and_then(|plan| plan.capacity_bytes().checked_sub(stacks))
            .and_then(|bytes| bytes.checked_add(self.bytes))
            .ok_or(WeightingError::ResidencyOverflow)
    }
}

struct PreparationWorker<'a> {
    projector: SelectedObservationProjector,
    spectral: WeightingSpectralCache<'a>,
    prepared: Vec<ReconstructionWeightedSample>,
    samples: u64,
}

struct ActivePreparation<'a>(&'a AtomicUsize);
impl Drop for ActivePreparation<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) struct ReplayPreparation<'a> {
    plan: ReplayPreparationPlan,
    index: SelectedObservationBlockIndex,
    workers: Vec<PreparationWorker<'a>>,
    active: AtomicUsize,
    peak_active: AtomicUsize,
    runs: u64,
    index_payload_bytes: u64,
    preparation_nanos: u128,
    ordered_commit_nanos: u128,
}

impl<'a> ReplayPreparation<'a> {
    pub(super) fn new(
        problem: &'a CompiledProblem,
        plan: ReplayPreparationPlan,
    ) -> Result<Self, WeightingError> {
        let mut workers = Vec::with_capacity(plan.workers);
        for _ in 0..plan.workers {
            workers.push(PreparationWorker {
                projector: SelectedObservationProjector::new(problem),
                spectral: WeightingSpectralCache::new(problem)?,
                prepared: Vec::with_capacity(plan.samples_per_worker),
                samples: 0,
            });
        }
        Ok(Self {
            plan,
            index: plan.index.create_index(),
            workers,
            active: AtomicUsize::new(0),
            peak_active: AtomicUsize::new(0),
            runs: 0,
            index_payload_bytes: 0,
            preparation_nanos: 0,
            ordered_commit_nanos: 0,
        })
    }

    pub(super) fn sample_count(&self) -> u64 {
        self.workers.iter().map(|worker| worker.samples).sum()
    }

    pub(super) fn log(&self) {
        eprintln!(
            "imaging_replay_preparation_summary workers={} peak_active_jobs={} indexed_runs={} prepared_samples={} planned_workspace_bytes={} index_payload_bytes={} preparation_nanos={} ordered_commit_nanos={}",
            self.plan.workers,
            self.peak_active.load(Ordering::Relaxed),
            self.runs,
            self.sample_count(),
            self.plan.bytes,
            self.index_payload_bytes,
            self.preparation_nanos,
            self.ordered_commit_nanos
        );
    }

    pub(super) fn consume<W, F, E>(
        &mut self,
        problem: &CompiledProblem,
        consumer: &mut SelectedObservationBlockConsumer<'_>,
        weights: &mut W,
        storage: &SelectedObservationBlock,
        execution: crate::bounded_stream::BoundedExecution<'_>,
        emit: &mut F,
    ) -> Result<(), WeightingBlockKernelError<E>>
    where
        W: StreamingWeightPhase + Sync,
        F: FnMut(
            &ReconstructionWeightedBlock,
            crate::bounded_stream::BoundedExecution<'_>,
        ) -> Result<(), E>,
        E: Error + Send + 'static,
    {
        let count = storage.selected_run_count().map_err(|error| {
            WeightingBlockKernelError::Traversal(SelectedObservationTraversalError::Source(error))
        })?;
        let mut start = 0;
        while start < count {
            let range = self
                .plan
                .index
                .next_range(storage, start)
                .map_err(|error| {
                    WeightingBlockKernelError::Traversal(SelectedObservationTraversalError::Source(
                        error,
                    ))
                })?;
            let end = range.end;
            consumer
                .index_block_range(storage, start..end, &mut self.index)
                .map_err(|error| {
                    WeightingBlockKernelError::Traversal(widen_terminal_traversal_error(error))
                })?;
            self.runs += (end - start) as u64;
            self.index_payload_bytes +=
                (self.index.current_bytes().map_err(|_| {
                    WeightingBlockKernelError::Owner(WeightingError::ResidencyOverflow)
                })? - size_of::<SelectedObservationBlockIndex>()) as u64;
            let indexed = self.index.view(storage, problem).map_err(|error| {
                WeightingBlockKernelError::Traversal(SelectedObservationTraversalError::Source(
                    error,
                ))
            })?;
            let runs_per_worker = indexed.run_count().div_ceil(self.plan.workers);
            let weights = &mut *weights;
            let (active, peak_active) = (&self.active, &self.peak_active);
            let started = Instant::now();
            execution.for_each_mut(&mut self.workers, |ordinal, worker| {
                worker.prepared.clear();
                let first = (ordinal * runs_per_worker).min(indexed.run_count());
                let last = (first + runs_per_worker).min(indexed.run_count());
                if first == last {
                    return Ok(());
                }
                let active_jobs = active.fetch_add(1, Ordering::Relaxed) + 1;
                peak_active.fetch_max(active_jobs, Ordering::Relaxed);
                let _active = ActivePreparation(active);
                indexed
                    .visit_range(&mut worker.projector, first..last, |run| {
                        for reported in run.samples() {
                            if worker.prepared.len() == worker.prepared.capacity() {
                                return Err(ReplayCallbackError::Owner(
                                    WeightingError::ResidencyOverflow,
                                ));
                            }
                            let contributions = worker
                                .spectral
                                .compile(reported.selected(), reported.spectral_evaluation())
                                .map_err(ReplayCallbackError::Owner)?;
                            worker.prepared.push(
                                weights
                                    .prepare_sample(
                                        problem,
                                        reported.selected(),
                                        reported.spectral_evaluation().output_frame().centre_hz(),
                                        contributions,
                                    )
                                    .map_err(ReplayCallbackError::Owner)?,
                            );
                        }
                        Ok::<(), ReplayCallbackError<E>>(())
                    })
                    .map_err(WeightingBlockKernelError::Traversal)?;
                worker.samples += worker.prepared.len() as u64;
                Ok(())
            })?;
            self.preparation_nanos += started.elapsed().as_nanos();
            let started = Instant::now();
            for worker in &mut self.workers {
                for weighted in worker.prepared.drain(..) {
                    if let Some(block) = weights
                        .commit_prepared(problem, weighted)
                        .map_err(WeightingBlockKernelError::Owner)?
                    {
                        emit(&block, execution).map_err(WeightingBlockKernelError::Consumer)?;
                        weights
                            .reuse_emitted_block(block)
                            .map_err(WeightingBlockKernelError::Owner)?;
                    }
                }
            }
            self.ordered_commit_nanos += started.elapsed().as_nanos();
            start = end;
        }
        self.index.clear();
        Ok(())
    }
}
