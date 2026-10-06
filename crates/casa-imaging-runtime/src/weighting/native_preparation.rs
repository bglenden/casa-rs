// SPDX-License-Identifier: LGPL-3.0-or-later

//! Whole-row preparation on the admitted team, borrowing the source columns.
//! Native buffers survive source-block boundaries and go directly to the writer.

use super::*;
use casa_imaging_reconstruction::runtime_adapter::{
    NativeBlock, NativeLayout, NativePreparationWorker, NativeWeightingPreparation,
};
use casa_ms::SelectedObservationProjector;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativePreparationPlan {
    pub(super) workers: usize,
    pub(super) heap_bytes: u64,
    block_rows: usize,
    worker_rows: usize,
    channels: usize,
    correlations: usize,
}

impl NativePreparationPlan {
    pub(crate) fn new(
        problem: &CompiledProblem,
        weighting: &WeightingPlan,
        workers: usize,
        block_rows: usize,
    ) -> Result<Self, WeightingError> {
        let [source] = problem.selected_observation().read_set().sources() else {
            return Err(WeightingError::ProblemMismatch);
        };
        let ([spw], [pol]) = (
            source.selection().spectral_windows(),
            source.selection().correlations(),
        ) else {
            return Err(WeightingError::ProblemMismatch);
        };
        if workers == 0 || block_rows == 0 {
            return Err(WeightingError::ResidencyOverflow);
        }
        let channels = spw.channel_indices().len();
        let correlations = pol.products().len();
        let worker_rows = block_rows.div_ceil(workers);
        let per_worker = NativeWeightingPreparation::worker_required_bytes(
            problem,
            weighting,
            worker_rows,
            channels,
            correlations,
        )
        .map_err(|_| WeightingError::ResidencyOverflow)?
        .checked_add(weighting.planned_residency().spectral_cache_bytes())
        .and_then(|n| n.checked_add(SelectedObservationProjector::required_bytes(problem)?))
        .and_then(|n| n.checked_add(size_of::<Worker<'_>>() + size_of::<&NativeBlock>()))
        .ok_or(WeightingError::ResidencyOverflow)?;
        let owned = per_worker
            .checked_mul(workers)
            .and_then(|n| {
                n.checked_add(
                    NativeWeightingPreparation::coordinator_required_bytes(weighting).ok()?,
                )
            })
            .and_then(|n| {
                n.checked_add(size_of::<
                    NativeKernel<'_, fn(&[&NativeBlock], &NativeLayout) -> io::Result<()>>,
                >())
            })
            .ok_or(WeightingError::ResidencyOverflow)? as u64;
        let stacks = if workers > 1 {
            workers as u64 * crate::bounded_stream::BOUNDED_WORKER_STACK_BYTES as u64
        } else {
            0
        };
        let heap_bytes = crate::bounded_stream::BoundedKernelPlan::new::<(), ()>(workers, 1, 0)
            .ok()
            .and_then(|plan| plan.capacity_bytes().checked_sub(stacks))
            .and_then(|bytes| bytes.checked_add(owned))
            .ok_or(WeightingError::ResidencyOverflow)?;
        Ok(Self {
            workers,
            heap_bytes,
            block_rows,
            worker_rows,
            channels,
            correlations,
        })
    }
}

struct Worker<'a> {
    projector: SelectedObservationProjector,
    spectral: WeightingSpectralCache<'a>,
    native: Option<NativePreparationWorker>,
    rows: usize,
}

struct NativeKernel<'a, F> {
    problem: &'a CompiledProblem,
    weighting_plan: &'a WeightingPlan,
    consumer: SelectedObservationBlockConsumer<'a>,
    weights: NativeWeightingPreparation,
    workers: Vec<Worker<'a>>,
    plan: NativePreparationPlan,
    batch_rows: usize,
    emit: F,
    inspection_nanos: u128,
    preparation_nanos: u128,
    ordered_commit_nanos: u128,
}

impl<F> NativeKernel<'_, F>
where
    F: FnMut(&[&NativeBlock], &NativeLayout) -> io::Result<()>,
{
    fn flush(&mut self) -> Result<(), WeightingBlockKernelError<io::Error>> {
        if self.batch_rows == 0 {
            return Ok(());
        }
        let started = Instant::now();
        for worker in self.workers.iter_mut().filter(|worker| worker.rows != 0) {
            let native = worker.native.as_mut().expect("populated worker");
            native
                .finish_batch()
                .map_err(WeightingBlockKernelError::Consumer)?;
            self.weights
                .commit(native)
                .map_err(WeightingBlockKernelError::Consumer)?;
        }
        {
            let parts: Vec<_> = self
                .workers
                .iter()
                .filter(|worker| worker.rows != 0)
                .map(|worker| worker.native.as_ref().expect("populated worker").block())
                .collect();
            let layout = self.workers[0]
                .native
                .as_ref()
                .expect("first populated worker")
                .layout();
            (self.emit)(&parts, layout).map_err(WeightingBlockKernelError::Consumer)?;
        }
        for worker in &mut self.workers {
            worker.rows = 0;
        }
        self.batch_rows = 0;
        self.ordered_commit_nanos += started.elapsed().as_nanos();
        Ok(())
    }

    fn consume(
        &mut self,
        storage: &SelectedObservationBlock,
        execution: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<(), WeightingBlockKernelError<io::Error>> {
        let runs = storage.selected_run_count().map_err(|e| {
            WeightingBlockKernelError::Traversal(SelectedObservationTraversalError::Source(e))
        })?;
        let channels = storage.selected_channels_per_row().map_err(|e| {
            WeightingBlockKernelError::Traversal(SelectedObservationTraversalError::Source(e))
        })?;
        if channels != self.plan.channels || runs % channels != 0 {
            return Err(WeightingBlockKernelError::Consumer(io::Error::other(
                "native source row shape mismatch",
            )));
        }
        let started = Instant::now();
        self.consumer
            .inspect_block_range(storage, 0..runs)
            .map_err(|e| WeightingBlockKernelError::Traversal(widen_terminal_traversal_error(e)))?;
        self.inspection_nanos += started.elapsed().as_nanos();
        let rows = runs / channels;
        let mut first = 0;
        while first < rows {
            let take = (rows - first).min(self.plan.block_rows - self.batch_rows);
            let batch_first = self.batch_rows;
            let batch_end = batch_first + take;
            let plan = self.plan;
            let problem = self.problem;
            let weights = &self.weights;
            let weighting_plan = self.weighting_plan;
            let started = Instant::now();
            execution.for_each_mut(&mut self.workers, |ordinal, worker| {
                let lower = (ordinal * plan.worker_rows).max(batch_first);
                let upper = ((ordinal + 1) * plan.worker_rows).min(batch_end);
                if lower >= upper {
                    return Ok(());
                }
                let source_first = first + lower - batch_first;
                let source_last = source_first + upper - lower;
                if worker.rows == 0
                    && let Some(native) = &mut worker.native
                {
                    native
                        .begin_batch()
                        .map_err(WeightingBlockKernelError::Consumer)?;
                }
                worker
                    .projector
                    .visit_block_range(
                        problem,
                        storage,
                        source_first * channels..source_last * channels,
                        |run| {
                            if let Some(reported) = run.samples().next() {
                                if worker.native.is_none() {
                                    let selected =
                                        problem.selected_observation().read_set().sources()[0]
                                            .selection();
                                    let layout = NativeLayout::new(
                                        reported.selected().address(),
                                        selected.spectral_windows()[0].channel_indices().to_vec(),
                                        selected.correlations()[0]
                                            .products()
                                            .iter()
                                            .map(|c| (c.correlation_index(), c.correlation_type()))
                                            .collect(),
                                    )
                                    .map_err(ReplayCallbackError::Consumer)?;
                                    let mut native = weights
                                        .worker(
                                            problem,
                                            weighting_plan,
                                            layout,
                                            NativeBlock::new(
                                                plan.worker_rows,
                                                plan.channels,
                                                plan.correlations,
                                            )
                                            .map_err(ReplayCallbackError::Consumer)?,
                                            problem.numerics().finite_values(),
                                        )
                                        .map_err(ReplayCallbackError::Consumer)?;
                                    native
                                        .begin_batch()
                                        .map_err(ReplayCallbackError::Consumer)?;
                                    worker.native = Some(native);
                                }
                                let contributions = worker
                                    .spectral
                                    .compile(reported.selected(), reported.spectral_evaluation())
                                    .map_err(ReplayCallbackError::Owner)?;
                                worker
                                    .native
                                    .as_mut()
                                    .expect("initialized above")
                                    .consume_channel(
                                        run.row(),
                                        run.channel(),
                                        run.correlations(),
                                        reported.spectral_evaluation().row_geometry().ok_or(
                                            ReplayCallbackError::Owner(
                                                WeightingError::RowSpectralGeometryMismatch,
                                            ),
                                        )?,
                                        reported.spectral_evaluation().output_frame().centre_hz(),
                                        contributions,
                                    )
                                    .map_err(ReplayCallbackError::Consumer)?;
                            }
                            Ok::<_, ReplayCallbackError<io::Error>>(())
                        },
                    )
                    .map_err(WeightingBlockKernelError::Traversal)?;
                worker.rows += upper - lower;
                Ok(())
            })?;
            self.preparation_nanos += started.elapsed().as_nanos();
            first += take;
            self.batch_rows += take;
            if self.batch_rows == self.plan.block_rows {
                self.flush()?;
            }
        }
        Ok(())
    }
}

impl<'a, F> PartitionedKernel<SelectedObservationBlock> for NativeKernel<'a, F>
where
    F: FnMut(&[&NativeBlock], &NativeLayout) -> io::Result<()> + Send + Sync,
{
    type Partition = ();
    type Partial = ();
    type Completion =
        WeightingBlockKernelCompletion<'a, (WeightingAlgorithmState, WeightingReplaySummary)>;
    type Error = WeightingBlockKernelError<io::Error>;

    fn partition_count(
        &self,
        _: BlockIdentity,
        _: &SelectedObservationBlock,
    ) -> Result<usize, Self::Error> {
        Ok(1)
    }
    fn partition(
        &self,
        _: BlockIdentity,
        _: &SelectedObservationBlock,
        _: usize,
    ) -> Result<KernelPartition<()>, Self::Error> {
        Ok(KernelPartition::exclusive(0, 0, ()))
    }
    fn execute(
        &self,
        _: WorkIdentity,
        _: &SelectedObservationBlock,
        _: &(),
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn commit(
        &mut self,
        _: WorkIdentity,
        storage: &SelectedObservationBlock,
        _: (),
        execution: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<(), Self::Error> {
        self.consume(storage, execution)
    }
    fn complete(
        mut self,
        _: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<Self::Completion, Self::Error> {
        self.flush()?;
        let weights = self
            .weights
            .finish()
            .map_err(WeightingBlockKernelError::Consumer)?;
        let samples = weights.1.sample_count();
        eprintln!(
            "imaging_native_preparation_summary workers={} samples={samples} indexed_runs=0 index_payload_bytes=0 retained_weighted_samples=0 inspection_nanos={} preparation_nanos={} ordered_commit_nanos={} planned_heap_bytes={}",
            self.plan.workers,
            self.inspection_nanos,
            self.preparation_nanos,
            self.ordered_commit_nanos,
            self.plan.heap_bytes
        );
        Ok(WeightingBlockKernelCompletion {
            consumer: self.consumer,
            weights,
            continuum: None,
            spectral_support_sample_count: 0,
            prepared_samples: samples,
        })
    }
}

pub(super) fn execute<'a, F>(
    problem: &'a CompiledProblem,
    selected: BoundSelectedObservation,
    plan: BoundedStreamPlan,
    preparation: NativePreparationPlan,
    weighting: &'a WeightingPlan,
    emit: F,
) -> Result<
    CompletedWeightingBlockStream<'a, (WeightingAlgorithmState, WeightingReplaySummary)>,
    WeightingBlockStreamFailure<io::Error>,
>
where
    F: FnMut(&[&NativeBlock], &NativeLayout) -> io::Result<()> + Send + Sync,
{
    let (source, consumer) = selected.into_block_stream(problem).map_err(|e| {
        WeightingReplayError::Traversal(SelectedObservationTraversalError::Binding(e))
    })?;
    let workers = (0..preparation.workers)
        .map(|_| {
            Ok(Worker {
                projector: SelectedObservationProjector::new(problem),
                spectral: WeightingSpectralCache::new(problem)?,
                native: None,
                rows: 0,
            })
        })
        .collect::<Result<Vec<_>, WeightingError>>()
        .map_err(WeightingReplayError::Owner)?;
    let outcome = execute_bounded(
        plan,
        0,
        SelectedBlockSource { source },
        NativeKernel {
            problem,
            weighting_plan: weighting,
            consumer,
            weights: NativeWeightingPreparation::new(problem, weighting)
                .map_err(WeightingReplayError::Consumer)?,
            workers,
            plan: preparation,
            batch_rows: 0,
            emit,
            inspection_nanos: 0,
            preparation_nanos: 0,
            ordered_commit_nanos: 0,
        },
    )
    .map_err(|failure| WeightingBlockStreamFailure {
        error: Box::new(map_bounded_stream_error(*failure.cause)),
        measurements: failure.measurements,
    })?;
    let mut terminal = outcome.source_completion;
    terminal
        .record_runtime_residency(
            outcome.measurements.peak_live_source_blocks,
            outcome.measurements.peak_live_source_current_bytes,
            outcome.measurements.peak_live_source_capacity_bytes,
        )
        .map_err(|e| {
            WeightingReplayError::Traversal(SelectedObservationTraversalError::Source(e))
        })?;
    let WeightingBlockKernelCompletion {
        consumer,
        weights,
        prepared_samples,
        ..
    } = outcome.kernel_completion;
    let (selected, owner_completion) = consumer
        .complete(terminal)
        .map_err(|e| WeightingReplayError::Traversal(widen_terminal_traversal_error(e)))?;
    Ok(CompletedWeightingBlockStream {
        selected,
        owner_completion,
        weights,
        continuum: None,
        spectral_support_sample_count: 0,
        prepared_samples,
        measurements: outcome.measurements,
    })
}
