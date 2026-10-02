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
        ) -> Result<(), E>,
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
        let workers = &mut self.workers;
        let plan = self.plan;
        let (active, peak_active) = (&self.active, &self.peak_active);
        let preparation_nanos = &mut self.preparation_nanos;
        let ordered_commit_nanos = &mut self.ordered_commit_nanos;
        consumer
            .consume_numeric(storage, geometry, || {
                let mut first = 0;
                while first < runs {
                    let last = (first + plan.runs_per_batch).min(runs);
                    let runs_per_worker = (last - first).div_ceil(plan.workers);
                    let started = Instant::now();
                    execution.for_each_mut(workers, |ordinal, worker| {
                        worker.prepared.clear();
                        let lower = (first + ordinal * runs_per_worker).min(last);
                        let upper = (lower + runs_per_worker).min(last);
                        if lower == upper {
                            return Ok(());
                        }
                        let live = active.fetch_add(1, Ordering::Relaxed) + 1;
                        peak_active.fetch_max(live, Ordering::Relaxed);
                        let _active = ActivePreparation(active);
                        for row_index in lower / channels..upper.div_ceil(channels) {
                            // consume_numeric validated every immutable row before
                            // entering this callback; no storage I/O occurs here.
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
                            )
                            .map_err(ReplayCallbackError::Owner)?;
                            let begin = lower.saturating_sub(row_index * channels);
                            let end = (upper - row_index * channels).min(channels);
                            for channel in begin..end {
                                prepare_channel(
                                    problem,
                                    &*weights,
                                    row,
                                    channel,
                                    frequencies[channel],
                                    row_geometry,
                                    &mut worker.prepared,
                                )
                                .map_err(ReplayCallbackError::Owner)?;
                            }
                        }
                        worker.samples += worker.prepared.len() as u64;
                        Ok::<(), ReplayCallbackError<E>>(())
                    })?;
                    *preparation_nanos += started.elapsed().as_nanos();
                    let started = Instant::now();
                    for worker in workers.iter_mut() {
                        while !worker.prepared.is_empty() {
                            if let Some(block) = weights
                                .commit_prepared(problem, &mut worker.prepared)
                                .map_err(ReplayCallbackError::Owner)?
                            {
                                emit(&block, execution).map_err(ReplayCallbackError::Consumer)?;
                                weights
                                    .reuse_emitted_block(block)
                                    .map_err(ReplayCallbackError::Owner)?;
                            }
                        }
                    }
                    *ordered_commit_nanos += started.elapsed().as_nanos();
                    first = last;
                }
                Ok::<(), ReplayCallbackError<E>>(())
            })
            .map_err(WeightingBlockKernelError::Traversal)?;
        self.runs += runs as u64;
        Ok(())
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
) -> Result<(), WeightingError> {
    if row.correlations.len() > prepared.capacity() - prepared.len() {
        return Err(WeightingError::ResidencyOverflow);
    }
    let first = correlation(row, channel, 0);
    let last = correlation(row, channel, row.correlations.len() - 1);
    let mut group_flag = false;
    let mut parallel_flag = false;
    for ordinal in 0..row.correlations.len() {
        let value = correlation(row, channel, ordinal);
        group_flag |= value.channel_flag;
        parallel_flag |= value.channel_flag && value.correlation_type.contributes_to_stokes_i();
    }
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
    for ordinal in 0..row.correlations.len() {
        let mut value = correlation(row, channel, ordinal);
        value.parallel_hand_group_flag = parallel_flag;
        let sample =
            SelectedObservationSampleView::from_run(row.row, &row.channels[channel], &value)
                .with_input_weight_group(
                    group
                        .with_density_owner(ordinal == 0)
                        .with_terminal_member(ordinal + 1 == row.correlations.len()),
                )
                .with_row_spectral_geometry(Some(geometry));
        prepared.push(weights.prepare_sample(problem, sample, frequency, contributions.clone())?);
    }
    Ok(())
}
