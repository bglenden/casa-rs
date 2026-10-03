// SPDX-License-Identifier: LGPL-3.0-or-later

//! Ordinary MFS preparation borrows MS columns and projects geometry once per
//! source block. The existing weighting phase and ordered replay own the math.

use super::*;
use casa_imaging_model::{
    SelectedInputWeightGroup, SelectedNumericRow, SelectedNumericVisibility,
    SelectedNumericWeights, SelectedObservationRunCorrelation, SelectedObservationSampleView,
    SelectedRowSpectralGeometry, SelectedSpectralContribution, SelectedSpectralContributions,
    SelectedVisibilitySample,
};

impl ReplayPreparation<'_> {
    pub(super) fn consume_numeric<W, F, E>(
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
            ) -> Result<(), E>
            + Send,
        E: Error + Send + 'static,
    {
        let geometry = self.geometry.as_mut().expect("numeric preparation plan");
        let started = Instant::now();
        crate::weighting::bulk_source::project_geometry(problem, storage, geometry, execution)
            .map_err(|error| {
                WeightingBlockKernelError::Traversal(SelectedObservationTraversalError::Source(
                    error,
                ))
            })?;
        self.preparation_nanos += started.elapsed().as_nanos();
        let channels = geometry.channels().len();
        let runs = geometry.row_count() * channels;
        let mut producer = NumericProducer {
            problem,
            storage,
            geometry,
            weights,
            workers: &mut self.workers,
            plan: self.plan,
            active: &self.active,
            peak_active: &self.peak_active,
            next_run: 0,
            next_worker: self.plan.workers,
            runs,
            preparation_nanos: 0,
            commit_nanos: 0,
        };
        let mut consumer_nanos = 0;
        consumer
            .consume_numeric(storage, geometry, || {
                let mut current = producer
                    .next_block(execution)
                    .map_err(ReplayCallbackError::Owner)?;
                while let Some(block) = current {
                    let next = if W::OVERLAP_PREPARATION && execution.is_parallel() {
                        let (next, consumed) = execution.join_pipeline(
                            || {
                                producer
                                    .next_block(execution)
                                    .map_err(ReplayCallbackError::Owner)
                            },
                            || {
                                let started = Instant::now();
                                let result =
                                    emit(&block, execution).map_err(ReplayCallbackError::Consumer);
                                consumer_nanos += started.elapsed().as_nanos();
                                result
                            },
                        );
                        finish_pipeline(next, consumed)?
                    } else {
                        let started = Instant::now();
                        emit(&block, execution).map_err(ReplayCallbackError::Consumer)?;
                        consumer_nanos += started.elapsed().as_nanos();
                        producer
                            .weights
                            .reuse_emitted_block(block)
                            .map_err(ReplayCallbackError::Owner)?;
                        current = producer
                            .next_block(execution)
                            .map_err(ReplayCallbackError::Owner)?;
                        continue;
                    };
                    producer
                        .weights
                        .reuse_emitted_block(block)
                        .map_err(ReplayCallbackError::Owner)?;
                    current = next;
                }
                Ok::<(), ReplayCallbackError<E>>(())
            })
            .map_err(WeightingBlockKernelError::Traversal)?;
        self.preparation_nanos += producer.preparation_nanos;
        // These are nested job durations when pipelined, not disjoint wall time.
        self.ordered_commit_nanos += producer.commit_nanos + consumer_nanos;
        self.runs += runs as u64;
        Ok(())
    }
}

struct NumericProducer<'a, 'p, W> {
    problem: &'a CompiledProblem,
    storage: &'a SelectedObservationBlock,
    geometry: &'a casa_ms::SelectedObservationNumericGeometry,
    weights: &'a mut W,
    workers: &'a mut [PreparationWorker<'p>],
    plan: ReplayPreparationPlan,
    active: &'a AtomicUsize,
    peak_active: &'a AtomicUsize,
    next_run: usize,
    next_worker: usize,
    runs: usize,
    preparation_nanos: u128,
    commit_nanos: u128,
}

impl<W: StreamingWeightPhase + Sync> NumericProducer<'_, '_, W> {
    fn next_block(
        &mut self,
        execution: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<Option<ReconstructionWeightedBlock>, WeightingError> {
        loop {
            while self.next_worker < self.workers.len() {
                let prepared = &mut self.workers[self.next_worker].prepared;
                if prepared.is_empty() {
                    self.next_worker += 1;
                    continue;
                }
                let started = Instant::now();
                let block = self.weights.commit_prepared(self.problem, prepared)?;
                self.commit_nanos += started.elapsed().as_nanos();
                if block.is_some() {
                    return Ok(block);
                }
            }
            if self.next_run == self.runs {
                return Ok(None);
            }
            let first = self.next_run;
            let last = (first + self.plan.runs_per_batch).min(self.runs);
            let runs_per_worker = (last - first).div_ceil(self.plan.workers);
            let channels = self.geometry.channels().len();
            let (problem, storage, geometry, weights) =
                (self.problem, self.storage, self.geometry, &*self.weights);
            let (active, peak_active) = (self.active, self.peak_active);
            let started = Instant::now();
            execution.for_each_mut(self.workers, |ordinal, worker| {
                worker.prepared.clear();
                let lower = (first + ordinal * runs_per_worker).min(last);
                let upper = (lower + runs_per_worker).min(last);
                if lower == upper {
                    return Ok::<(), WeightingError>(());
                }
                let live = active.fetch_add(1, Ordering::Relaxed) + 1;
                peak_active.fetch_max(live, Ordering::Relaxed);
                let _active = ActivePreparation(active);
                for row_index in lower / channels..upper.div_ceil(channels) {
                    let row = storage
                        .numeric_row(geometry, row_index)
                        .expect("source owner validated numeric rows");
                    let frequencies = &geometry.frequencies_hz()
                        [row_index * channels..(row_index + 1) * channels];
                    let row_geometry = spectral_geometry(
                        problem,
                        row,
                        frequencies,
                        geometry.original_pairs_hz()[row_index],
                    )?;
                    let begin = lower.saturating_sub(row_index * channels);
                    let end = (upper - row_index * channels).min(channels);
                    for channel in begin..end {
                        prepare_channel(
                            problem,
                            weights,
                            row,
                            channel,
                            frequencies[channel],
                            row_geometry,
                            &mut worker.prepared,
                            &mut worker.correlations,
                        )?;
                    }
                }
                worker.samples += worker.prepared.len() as u64;
                Ok(())
            })?;
            self.preparation_nanos += started.elapsed().as_nanos();
            self.next_run = last;
            self.next_worker = 0;
        }
    }
}

fn finish_pipeline<T, E: Error>(
    produced: std::thread::Result<Result<T, ReplayCallbackError<E>>>,
    consumed: std::thread::Result<Result<(), ReplayCallbackError<E>>>,
) -> Result<T, ReplayCallbackError<E>> {
    // Preserve the original typed consumer/I/O error. Both jobs are joined;
    // report a second failure rather than silently dropping it on early return.
    let describe_panic = |payload: &Box<dyn std::any::Any + Send>| {
        payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload")
            .to_owned()
    };
    match consumed {
        Ok(Err(error)) => {
            match produced {
                Ok(Err(sibling)) => eprintln!("weighting producer also failed: {sibling}"),
                Err(panic) => eprintln!(
                    "weighting producer also panicked: {}",
                    describe_panic(&panic)
                ),
                Ok(Ok(_)) => {}
            }
            Err(error)
        }
        Err(panic) => match produced {
            Ok(Err(error)) => {
                eprintln!(
                    "weighting consumer also panicked: {}",
                    describe_panic(&panic)
                );
                Err(error)
            }
            Err(sibling) => panic!(
                "weighting consumer panicked: {}; producer also panicked: {}",
                describe_panic(&panic),
                describe_panic(&sibling)
            ),
            Ok(Ok(_)) => std::panic::resume_unwind(panic),
        },
        Ok(Ok(())) => produced.unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_preserves_consumer_io_error_with_a_failed_or_panicked_producer() {
        for producer in [
            Ok(Err(ReplayCallbackError::Owner(
                WeightingError::ReturnedBlockMismatch,
            ))),
            Err(Box::new("producer panic") as Box<dyn std::any::Any + Send>),
        ] {
            let error = finish_pipeline::<(), std::io::Error>(
                producer,
                Ok(Err(ReplayCallbackError::Consumer(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "writer failure",
                )))),
            )
            .unwrap_err();
            let ReplayCallbackError::Consumer(error) = error else {
                panic!("lost I/O error")
            };
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            assert_eq!(error.to_string(), "writer failure");
        }
    }

    #[test]
    fn pipeline_cannot_succeed_after_a_producer_error_or_consumer_panic() {
        let error = finish_pipeline::<(), std::io::Error>(
            Ok(Err(ReplayCallbackError::Owner(
                WeightingError::ReturnedBlockMismatch,
            ))),
            Err(Box::new("consumer panic")),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ReplayCallbackError::Owner(WeightingError::ReturnedBlockMismatch)
        ));
        assert!(
            std::panic::catch_unwind(|| finish_pipeline::<(), std::io::Error>(
                Ok(Ok(())),
                Err(Box::new("consumer panic")),
            ))
            .is_err()
        );
    }
}

fn correlation(
    row: SelectedNumericRow<'_>,
    channel: usize,
    ordinal: usize,
) -> SelectedObservationRunCorrelation {
    let product = row.correlations[ordinal];
    let index = (row.channels[channel].channel_index - row.first_stored_channel) as usize
        * row.stored_correlations
        + product.correlation_index() as usize;
    SelectedObservationRunCorrelation {
        correlation_index: product.correlation_index(),
        correlation_type: product.correlation_type(),
        visibility: match row.visibility {
            SelectedNumericVisibility::Complex32(values) => {
                SelectedVisibilitySample::Complex32([values[index].re, values[index].im])
            }
            SelectedNumericVisibility::Float32(values) => {
                SelectedVisibilitySample::Float32(values[index])
            }
        },
        channel_flag: row.flags[index],
        parallel_hand_group_flag: false,
        input_weight: match row.weights {
            SelectedNumericWeights::PerRow(values) => values[product.correlation_index() as usize],
            SelectedNumericWeights::PerChannel(values) => values[index],
        },
    }
}

fn spectral_geometry(
    problem: &CompiledProblem,
    row: SelectedNumericRow<'_>,
    frequencies: &[f64],
    original_pair: [f64; 2],
) -> Result<SelectedRowSpectralGeometry, WeightingError> {
    let first = correlation(row, 0, 0);
    let sample = SelectedObservationSampleView::from_run(row.row, &row.channels[0], &first);
    let geometry = SelectedRowSpectralGeometry::new(
        sample,
        problem.geometry().spectral().output_frame(),
        row.channels.len(),
        (row.channels[0].channel_index, frequencies[0]),
        row.channels
            .get(1)
            .map(|channel| (channel.channel_index, frequencies[1])),
    )
    .ok_or(WeightingError::RowSpectralGeometryMismatch)?;
    if row.channels.len() > 1 {
        geometry
            .with_lattice_first_pair_hz(original_pair)
            .ok_or(WeightingError::RowSpectralGeometryMismatch)
    } else {
        Ok(geometry)
    }
}

fn prepare_channel<W: StreamingWeightPhase>(
    problem: &CompiledProblem,
    weights: &W,
    row: SelectedNumericRow<'_>,
    channel: usize,
    frequency: f64,
    geometry: SelectedRowSpectralGeometry,
    prepared: &mut Vec<ReconstructionWeightedSample>,
    correlations: &mut Vec<SelectedObservationRunCorrelation>,
) -> Result<(), WeightingError> {
    if row.correlations.len() > prepared.capacity() - prepared.len()
        || row.correlations.len() > correlations.capacity()
    {
        return Err(WeightingError::ResidencyOverflow);
    }
    correlations.clear();
    let mut group_flag = false;
    let mut parallel_flag = false;
    for ordinal in 0..row.correlations.len() {
        let value = correlation(row, channel, ordinal);
        group_flag |= value.channel_flag;
        parallel_flag |= value.channel_flag && value.correlation_type.contributes_to_stokes_i();
        correlations.push(value);
    }
    let first = correlations
        .first()
        .expect("validated nonempty source correlations");
    let last = correlations
        .last()
        .expect("validated nonempty source correlations");
    let group = SelectedInputWeightGroup::correlation_run(
        first.input_weight,
        last.input_weight,
        row.correlations.len(),
    )
    .with_imaging_flag(group_flag);
    // The existing constant-basis stencil is one unit contribution evaluated
    // at the source owner's converted channel frequency, regardless of flags.
    let contributions =
        SelectedSpectralContributions::new([SelectedSpectralContribution::new(0, 1.0, frequency)])
            .ok_or(WeightingError::RowSpectralGeometryMismatch)?;
    let first_prepared = prepared.len();
    for (ordinal, mut value) in correlations.iter().copied().enumerate() {
        let weighted = if ordinal == 0 {
            value.parallel_hand_group_flag = parallel_flag;
            let sample =
                SelectedObservationSampleView::from_run(row.row, &row.channels[channel], &value)
                    .with_input_weight_group(
                        group.with_terminal_member(row.correlations.len() == 1),
                    )
                    .with_row_spectral_geometry(Some(geometry));
            weights.prepare_sample(problem, sample, frequency, contributions.clone())?
        } else {
            prepared[first_prepared]
                .prepare_group_member(value, ordinal + 1 == row.correlations.len())
        };
        prepared.push(weighted);
    }
    Ok(())
}
