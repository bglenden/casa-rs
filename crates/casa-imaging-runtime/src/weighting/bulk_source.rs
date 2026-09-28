// SPDX-License-Identifier: LGPL-3.0-or-later

//! Direct selected-MS ingress. The source owns visibility columns until every
//! numerical consumer has joined; only derived weights and row geometry live
//! here. A gather buffer exists only for Float data or non-dense selections.

use super::*;
use crate::bounded_stream::BoundedExecution;
use casa_imaging_model::{SelectedNumericVisibility, SelectedSampleAddress};
use casa_imaging_reconstruction::runtime_adapter::{
    BulkNaturalWeighting, NativeBlockView, NativeLayout, NaturalRowPreparation, RowMetadata,
};
use casa_ms::SelectedObservationNumericGeometry;
use num_complex::Complex32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

struct PreparedChunk<'a> {
    first_row: usize,
    metadata: &'a mut [RowMetadata],
    weights: &'a mut [f32],
    flags: &'a mut [bool],
    weight_flags: &'a mut [bool],
    gathered: Option<&'a mut [Complex32]>,
    sum: f64,
}

struct ActiveChunk<'a>(&'a AtomicUsize);

impl Drop for ActiveChunk<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn resize_gathered(buffer: &mut Vec<Complex32>, admitted_samples: usize, samples: usize) {
    if buffer.capacity() == 0 {
        buffer.reserve_exact(admitted_samples);
    }
    buffer.resize(samples, Complex32::new(0.0, 0.0));
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BulkInputPlan {
    pub(crate) rows: usize,
    pub(crate) channels: usize,
    pub(crate) correlations: usize,
    pub(crate) bytes: u64,
}

impl BulkInputPlan {
    pub(crate) fn new(problem: &CompiledProblem, rows: usize) -> io::Result<Self> {
        let [source] = problem.selected_observation().read_set().sources() else {
            return Err(io::Error::other("bulk input requires one source"));
        };
        let [spw] = source.selection().spectral_windows() else {
            return Err(io::Error::other("bulk input requires one channel layout"));
        };
        Self::for_window(problem, rows, 0..spw.channel_indices().len())
    }

    pub(crate) fn for_window(
        problem: &CompiledProblem,
        rows: usize,
        window: std::ops::Range<usize>,
    ) -> io::Result<Self> {
        let [source] = problem.selected_observation().read_set().sources() else {
            return Err(io::Error::other("bulk input requires one source"));
        };
        let ([spw], [pol]) = (
            source.selection().spectral_windows(),
            source.selection().correlations(),
        ) else {
            return Err(io::Error::other(
                "bulk input requires one channel/correlation layout",
            ));
        };
        let total_channels = spw.channel_indices().len();
        if window.start > window.end || window.end > total_channels {
            return Err(io::Error::other(
                "bulk input window exceeds selected channels",
            ));
        }
        // An empty halo owns no samples; the numeric geometry still needs its
        // one-channel construction minimum and will never consume a block.
        let channels = window.len().max(1);
        let correlations = pol.products().len();
        let projection =
            casa_imaging_model::SelectedImageDomainProjections::retained_heap_bytes_for_len(1)
                .ok_or_else(|| io::Error::other("bulk projection size overflow"))?;
        let spectral_bytes = (problem.geometry().spectral().output_channels() * 2 + 1)
            .checked_mul(size_of::<f64>())
            .and_then(|n| n.checked_add(size_of::<BulkNaturalWeighting>()))
            .ok_or_else(|| io::Error::other("bulk spectral scratch overflow"))?;
        let bytes = SelectedObservationNumericGeometry::required_bytes(rows, channels)
            .map_err(io::Error::other)?
            .checked_add(
                rows.checked_mul(projection + size_of::<RowMetadata>())
                    .ok_or_else(|| io::Error::other("bulk rows overflow"))?,
            )
            .and_then(|n| n.checked_add(spectral_bytes))
            .and_then(|n| {
                n.checked_add(
                    rows.checked_mul(channels)?
                        .checked_mul(correlations)?
                        .checked_mul(14)?,
                )
            })
            .and_then(|n| n.checked_add(channels.checked_mul(size_of::<u32>())?))
            .and_then(|n| {
                n.checked_add(
                    size_of::<NaturalRowPreparation>()
                        + size_of::<NativeLayout>()
                        + size_of::<Self>(),
                )
            })
            .and_then(|n| n.checked_add(rows.checked_mul(size_of::<PreparedChunk<'static>>())?))
            .ok_or_else(|| io::Error::other("bulk input size overflow"))?;
        Ok(Self {
            rows,
            channels,
            correlations,
            bytes: bytes as u64,
        })
    }
}

pub(crate) struct BulkInputCompletion {
    pub(crate) selected: BoundSelectedObservation,
    owner: BulkSourceCompletion,
    pub(crate) weighting: BulkNaturalWeighting,
    pub(crate) measurements: BoundedStreamMeasurements,
}

enum BulkSourceCompletion {
    Full(SelectedObservationCompletion),
    Window(casa_ms::SelectedObservationWindowCompletion),
}

/// One admitted set of numerical owners borrowing each source block together.
/// Completion joins FFT/output work before the source pass can be accepted.
pub(crate) trait BulkConsumer: Send + Sync {
    type Completion: Send;
    fn consume(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        selected_channels: std::ops::Range<usize>,
        execution: BoundedExecution<'_>,
    ) -> io::Result<()>;
    fn complete(self, execution: BoundedExecution<'_>) -> io::Result<Self::Completion>;
}

struct Kernel<'a, F> {
    problem: &'a CompiledProblem,
    consumer: SelectedObservationBlockConsumer<'a>,
    plan: BulkInputPlan,
    geometry: SelectedObservationNumericGeometry,
    metadata: Vec<RowMetadata>,
    derived: NaturalRowPreparation,
    gathered: Vec<Complex32>,
    weighting: BulkNaturalWeighting,
    initial: bool,
    selected_channels: std::ops::Range<usize>,
    emit: F,
    blocks: u64,
    copied_samples: u64,
    preparation_nanos: u128,
    inspection_and_kernel_nanos: u128,
    prepared_chunks: u64,
    peak_active_preparation: usize,
    projected_chunks: u64,
    peak_active_projection: usize,
}

impl<F> Kernel<'_, F>
where
    F: BulkConsumer,
{
    fn consume(
        &mut self,
        storage: &SelectedObservationBlock,
        execution: BoundedExecution<'_>,
    ) -> io::Result<()> {
        let started = Instant::now();
        let geometry_active = AtomicUsize::new(0);
        let geometry_peak = AtomicUsize::new(0);
        let geometry_chunk_rows = self
            .plan
            .rows
            .div_ceil(execution.worker_count().saturating_mul(4))
            .max(1);
        let mut projected_chunks = 0;
        storage
            .project_numeric_geometry_with(
                self.problem,
                &mut self.geometry,
                geometry_chunk_rows,
                |chunks| {
                    projected_chunks = chunks.len();
                    execution.for_each_mut(chunks, |_, chunk| {
                        let live = geometry_active.fetch_add(1, Ordering::Relaxed) + 1;
                        geometry_peak.fetch_max(live, Ordering::Relaxed);
                        let _active = ActiveChunk(&geometry_active);
                        chunk.project()
                    })
                },
            )
            .map_err(io::Error::other)?;
        self.projected_chunks += projected_chunks as u64;
        self.peak_active_projection = self
            .peak_active_projection
            .max(geometry_peak.load(Ordering::Relaxed));
        let first = storage
            .numeric_row(&self.geometry, 0)
            .map_err(io::Error::other)?;
        let channels = first.channels.len();
        let correlations = first.correlations.len();
        let rows = self.geometry.row_count();
        if rows > self.plan.rows
            || channels > self.plan.channels
            || correlations != self.plan.correlations
        {
            return Err(io::Error::other(
                "source block exceeds admitted bulk geometry",
            ));
        }
        let channel = first.channels[0];
        let correlation = first.correlations[0];
        let layout = NativeLayout::new(
            SelectedSampleAddress {
                measurement_set: first.row.measurement_set,
                physical_row: first.row.physical_row,
                data_description_id: first.row.data_description_id,
                spectral_window_id: first.row.spectral_window_id,
                polarization_id: first.row.polarization_id,
                channel_index: channel.channel_index,
                correlation_index: correlation.correlation_index(),
                correlation_type: correlation.correlation_type(),
                frequency_centre_hz: channel.frequency_centre_hz,
                frequency_lower_hz: channel.frequency_lower_hz,
                frequency_upper_hz: channel.frequency_upper_hz,
                channel_width_hz: channel.channel_width_hz,
                frequency_frame: channel.frequency_frame,
            },
            first.channels.iter().map(|c| c.channel_index).collect(),
            first
                .correlations
                .iter()
                .map(|c| (c.correlation_index(), c.correlation_type()))
                .collect(),
        )?;
        let columns = storage.numeric_block().map_err(io::Error::other)?.columns();
        let dense = first
            .channels
            .iter()
            .enumerate()
            .all(|(i, c)| c.channel_index as usize == columns.channel_range.start + i)
            && channels == columns.channel_range.count
            && correlations == columns.correlation_count
            && first
                .correlations
                .iter()
                .enumerate()
                .all(|(i, c)| c.correlation_index() as usize == i);
        let borrowed = match columns.visibility {
            SelectedNumericVisibility::Complex32(values) if dense => Some(values),
            _ => None,
        };
        let samples = rows * channels * correlations;
        self.metadata.resize(rows, RowMetadata::default());
        if borrowed.is_none() {
            resize_gathered(
                &mut self.gathered,
                self.plan.rows * self.plan.channels * self.plan.correlations,
                samples,
            );
        } else {
            self.gathered.clear();
        }
        let stride = channels * correlations;
        let chunk_rows = rows
            .div_ceil(execution.worker_count().saturating_mul(4))
            .max(1);
        let (weights, flags, weight_flags) = self.derived.buffers_mut();
        let (mut weights, mut flags, mut weight_flags) = (
            &mut weights[..samples],
            &mut flags[..samples],
            &mut weight_flags[..samples],
        );
        let mut metadata = &mut self.metadata[..rows];
        let mut gathered = &mut self.gathered[..];
        let mut chunks = Vec::with_capacity(rows.div_ceil(chunk_rows));
        let mut first_row = 0;
        while first_row < rows {
            let take = (rows - first_row).min(chunk_rows);
            let cells = take * stride;
            let (metadata_head, metadata_tail) = metadata.split_at_mut(take);
            let (weights_head, weights_tail) = weights.split_at_mut(cells);
            let (flags_head, flags_tail) = flags.split_at_mut(cells);
            let (weight_flags_head, weight_flags_tail) = weight_flags.split_at_mut(cells);
            let (gather_head, gather_tail) = if borrowed.is_some() {
                gathered.split_at_mut(0)
            } else {
                gathered.split_at_mut(cells)
            };
            chunks.push(PreparedChunk {
                first_row,
                metadata: metadata_head,
                weights: weights_head,
                flags: flags_head,
                weight_flags: weight_flags_head,
                gathered: borrowed.is_none().then_some(gather_head),
                sum: 0.0,
            });
            (metadata, weights, flags, weight_flags, gathered) = (
                metadata_tail,
                weights_tail,
                flags_tail,
                weight_flags_tail,
                gather_tail,
            );
            first_row += take;
        }
        let active = AtomicUsize::new(0);
        let peak_active = AtomicUsize::new(0);
        let geometry = &self.geometry;
        let weighting = &self.weighting;
        let initial = self.initial;
        execution.for_each_mut(&mut chunks, |_, chunk| -> io::Result<()> {
            let live = active.fetch_add(1, Ordering::Relaxed) + 1;
            peak_active.fetch_max(live, Ordering::Relaxed);
            let _active = ActiveChunk(&active);
            for local in 0..chunk.metadata.len() {
                let row = chunk.first_row + local;
                let numeric = storage
                    .numeric_row(geometry, row)
                    .map_err(io::Error::other)?;
                let projection = numeric
                    .row
                    .domain_projections
                    .get(0)
                    .ok_or_else(|| io::Error::other("bulk row lacks image projection"))?
                    .model();
                chunk.metadata[local] = RowMetadata {
                    physical_row: numeric.row.physical_row,
                    uvw_m: projection.transformed_uvw_m(),
                    phase_shift_m: projection.phase_shift_m(),
                    original_pair_hz: geometry.original_pairs_hz()[row],
                };
                let source_cells = row * channels..(row + 1) * channels;
                let output_cells = local * stride..(local + 1) * stride;
                chunk.sum += weighting
                    .prepare_row(
                        numeric,
                        &geometry.frequencies_hz()[source_cells.clone()],
                        &geometry.boundaries_hz()[source_cells],
                        &mut chunk.weights[output_cells.clone()],
                        &mut chunk.flags[output_cells.clone()],
                        &mut chunk.weight_flags[output_cells.clone()],
                        initial,
                    )
                    .map_err(io::Error::other)?;
                if let Some(gathered) = chunk.gathered.as_deref_mut() {
                    let mut destination = output_cells.start;
                    for channel in numeric.channels {
                        let start = (channel.channel_index - numeric.first_stored_channel) as usize
                            * numeric.stored_correlations;
                        for correlation in numeric.correlations {
                            let index = start + correlation.correlation_index() as usize;
                            gathered[destination] = match numeric.visibility {
                                SelectedNumericVisibility::Complex32(values) => values[index],
                                SelectedNumericVisibility::Float32(values) => {
                                    Complex32::new(values[index], 0.0)
                                }
                            };
                            destination += 1;
                        }
                    }
                }
            }
            Ok(())
        })?;
        self.prepared_chunks += chunks.len() as u64;
        self.peak_active_preparation = self
            .peak_active_preparation
            .max(peak_active.load(Ordering::Relaxed));
        if self.initial {
            for chunk in &chunks {
                self.weighting
                    .commit_prepared_chunk(
                        chunk.sum,
                        (chunk.metadata.len() * stride) as u64,
                        chunk.metadata.len() as u64,
                    )
                    .map_err(io::Error::other)?;
            }
        }
        drop(chunks);
        let view = NativeBlockView::new(
            &self.metadata,
            self.geometry.frequencies_hz(),
            borrowed.unwrap_or(&self.gathered),
            &self.derived.weights()[..samples],
            &self.derived.flags()[..samples],
            &self.derived.weight_flags()[..samples],
            channels,
            correlations,
        )?;
        self.preparation_nanos += started.elapsed().as_nanos();
        let started = Instant::now();
        let emit = &mut self.emit;
        self.consumer
            .consume_numeric(storage, &self.geometry, || {
                emit.consume(view, &layout, self.selected_channels.clone(), execution)
            })
            .map_err(io::Error::other)?;
        self.inspection_and_kernel_nanos += started.elapsed().as_nanos();
        self.blocks += 1;
        self.copied_samples += if borrowed.is_none() {
            samples as u64
        } else {
            0
        };
        Ok(())
    }
}

#[cfg(test)]
mod gather_tests {
    use super::*;

    #[test]
    fn bulk_gather_refills_reuse_the_admitted_capacity() {
        let admitted = 100;
        let mut buffer = Vec::new();
        let mut allocation = None;
        for samples in [60, 90, 100, 3, 99] {
            resize_gathered(&mut buffer, admitted, samples);
            assert_eq!(buffer.len(), samples);
            assert_eq!(buffer.capacity(), admitted);
            assert_eq!(*allocation.get_or_insert(buffer.as_ptr()), buffer.as_ptr());
            buffer.fill(Complex32::new(1.0, -1.0));
            buffer.clear();
        }
    }
}

impl<'a, F> PartitionedKernel<SelectedObservationBlock> for Kernel<'a, F>
where
    F: BulkConsumer,
{
    type Partition = ();
    type Partial = ();
    type Completion = (
        SelectedObservationBlockConsumer<'a>,
        BulkNaturalWeighting,
        F::Completion,
    );
    type Error = io::Error;
    fn partition_count(&self, _: BlockIdentity, _: &SelectedObservationBlock) -> io::Result<usize> {
        Ok(1)
    }
    fn partition(
        &self,
        _: BlockIdentity,
        _: &SelectedObservationBlock,
        _: usize,
    ) -> io::Result<KernelPartition<()>> {
        Ok(KernelPartition::exclusive(0, 0, ()))
    }
    fn execute(&self, _: WorkIdentity, _: &SelectedObservationBlock, _: &()) -> io::Result<()> {
        Ok(())
    }
    fn commit(
        &mut self,
        _: WorkIdentity,
        storage: &SelectedObservationBlock,
        _: (),
        execution: BoundedExecution<'_>,
    ) -> io::Result<()> {
        self.consume(storage, execution)
    }
    fn complete(self, execution: BoundedExecution<'_>) -> io::Result<Self::Completion> {
        eprintln!(
            "bulk_input blocks={} channels={} capacity_bytes={} copied_samples={} preparation_nanos={} inspection_and_kernel_nanos={} projected_chunks={} peak_active_projection={} prepared_chunks={} peak_active_preparation={}",
            self.blocks,
            self.plan.channels,
            self.plan.bytes,
            self.copied_samples,
            self.preparation_nanos,
            self.inspection_and_kernel_nanos,
            self.projected_chunks,
            self.peak_active_projection,
            self.prepared_chunks,
            self.peak_active_preparation
        );
        Ok((
            self.consumer,
            self.weighting,
            self.emit.complete(execution)?,
        ))
    }
}

pub(crate) fn execute<F>(
    problem: &CompiledProblem,
    selected: BoundSelectedObservation,
    stream: BoundedStreamPlan,
    input: BulkInputPlan,
    weighting: &WeightingPlan,
    initial: bool,
    channels: Option<std::ops::Range<usize>>,
    emit: F,
) -> io::Result<(BulkInputCompletion, F::Completion)>
where
    F: BulkConsumer,
{
    let (source, consumer) = match &channels {
        Some(channels) => selected.into_channel_window_block_stream(problem, channels.clone()),
        None => selected.into_block_stream(problem),
    }
    .map_err(io::Error::other)?;
    let rows = source.maximum_rows_per_block().min(input.rows);
    let admitted_bytes = input.bytes;
    let input = match &channels {
        Some(window) => BulkInputPlan::for_window(problem, rows, window.clone())?,
        None => BulkInputPlan::new(problem, rows)?,
    };
    if input.bytes > admitted_bytes {
        return Err(io::Error::other(
            "bulk source window exceeds admitted input capacity",
        ));
    }
    let outcome = execute_bounded(
        stream,
        0,
        SelectedBlockSource { source },
        Kernel {
            problem,
            consumer,
            plan: input,
            geometry: SelectedObservationNumericGeometry::new(input.rows, input.channels)
                .map_err(io::Error::other)?,
            metadata: Vec::with_capacity(input.rows),
            derived: NaturalRowPreparation::new(input.rows, input.channels, input.correlations)
                .map_err(io::Error::other)?,
            gathered: Vec::new(),
            weighting: BulkNaturalWeighting::new(problem, weighting, initial)
                .map_err(io::Error::other)?,
            initial,
            selected_channels: channels.clone().unwrap_or(0..input.channels),
            emit,
            blocks: 0,
            copied_samples: 0,
            preparation_nanos: 0,
            inspection_and_kernel_nanos: 0,
            prepared_chunks: 0,
            peak_active_preparation: 0,
            projected_chunks: 0,
            peak_active_projection: 0,
        },
    )
    .map_err(|failure| {
        io::Error::other(format!("bulk source traversal failed: {:?}", failure.cause))
    })?;
    let mut terminal = outcome.source_completion;
    terminal
        .record_runtime_residency(
            outcome.measurements.peak_live_source_blocks,
            outcome.measurements.peak_live_source_current_bytes,
            outcome.measurements.peak_live_source_capacity_bytes,
        )
        .map_err(io::Error::other)?;
    let (consumer, weighting, output) = outcome.kernel_completion;
    let (selected, owner) = if channels.is_some() {
        let (selected, owner) = consumer
            .complete_window(terminal)
            .map_err(io::Error::other)?;
        (selected, BulkSourceCompletion::Window(owner))
    } else {
        let (selected, owner) = consumer.complete(terminal).map_err(io::Error::other)?;
        (selected, BulkSourceCompletion::Full(owner))
    };
    Ok((
        BulkInputCompletion {
            selected,
            owner,
            weighting,
            measurements: outcome.measurements,
        },
        output,
    ))
}

impl WeightingExecutionState {
    pub(crate) fn traverse_bulk<F: BulkConsumer>(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        problem: &CompiledProblem,
        selected: BoundSelectedObservation,
        input: BulkInputPlan,
        channels: Option<std::ops::Range<usize>>,
        consumer: F,
    ) -> io::Result<F::Completion> {
        if !matches!(self.phase, WeightingExecutionPhase::Empty)
            || !matches!(
                fragment.replay_preparation,
                Some(PreparationPlan::Bulk { .. })
            )
        {
            return Err(io::Error::other(
                "bulk initial traversal has no admitted empty owner",
            ));
        }
        fragment
            .authorize_source_observation(context, problem, selected.residency_certificate())
            .map_err(io::Error::other)?;
        let initial = self.imported.is_none();
        if initial && channels.is_some() {
            return Err(io::Error::other(
                "initial bulk discovery cannot use a channel window",
            ));
        }
        let stream = fragment
            .bounded_stream_plan(context, initial)
            .map_err(io::Error::other)?;
        let (completed, output) = execute(
            problem,
            selected,
            stream,
            input,
            fragment.plan,
            initial,
            channels.clone(),
            consumer,
        )?;
        let BulkInputCompletion {
            selected,
            owner,
            weighting,
            measurements,
        } = completed;
        let (state, summary, proof, owner) = if let Some(artifact) = self.imported.take() {
            let proof = artifact
                .coverage_proof
                .ok_or_else(|| io::Error::other("bulk replay lacks frozen coverage"))?;
            let rows = problem
                .selected_observation()
                .read_set()
                .sources()
                .iter()
                .map(|s| s.selection().rows().selected_row_count())
                .sum();
            let summary = BulkNaturalWeighting::replay_summary(&artifact.state, proof, rows)
                .map_err(io::Error::other)?;
            let (source_generation, traversal) = match &owner {
                BulkSourceCompletion::Full(owner) => {
                    artifact
                        .validate_derived_completion(owner, None, &summary)
                        .map_err(io::Error::other)?;
                    (owner.generation_id(), *owner.measurements())
                }
                BulkSourceCompletion::Window(owner) => {
                    let window = channels.as_ref().ok_or_else(|| {
                        io::Error::other("bulk window lacks a declared channel range")
                    })?;
                    let expected_samples = rows
                        .checked_mul(window.len() as u64)
                        .and_then(|n| n.checked_mul(input.correlations as u64))
                        .ok_or_else(|| io::Error::other("bulk window sample count overflow"))?;
                    if owner.channel_ordinals().as_ref() != Some(window)
                        || owner.sample_count() != expected_samples
                    {
                        return Err(io::Error::other(
                            "first bulk window differs from its selected source",
                        ));
                    }
                    artifact
                        .validate_derived_window_completion(owner, &summary)
                        .map_err(io::Error::other)?;
                    (owner.generation_id(), *owner.measurements())
                }
            };
            self.latest_traversal_measurements = Some(traversal);
            self.latest_stream_measurements = Some(measurements);
            let binding = WeightingGenerationBinding {
                attempt_id: context.attempt_id(),
                owner_node: context.node().id.clone(),
                lease_epoch: context.lease_epoch(),
                source_generation,
                source_sample_count: summary.sample_count(),
            };
            self.retained_observation = Some(RetainedWeightingObservation {
                selected,
                attempt_id: context.attempt_id(),
                owner_node: context.node().id.clone(),
                lease_epoch: context.lease_epoch(),
            });
            self.phase = WeightingExecutionPhase::PendingReplay {
                frozen: FrozenWeightingGeneration {
                    artifact,
                    binding: WeightingGenerationBinding {
                        attempt_id: binding.attempt_id,
                        owner_node: binding.owner_node.clone(),
                        lease_epoch: binding.lease_epoch,
                        source_generation: binding.source_generation,
                        source_sample_count: binding.source_sample_count,
                    },
                },
                pending: Box::new(PendingWeightingReplay {
                    state: summary,
                    owner_completion: match owner {
                        BulkSourceCompletion::Full(owner) => ReplaySourceCompletion::Full(owner),
                        BulkSourceCompletion::Window(owner) => {
                            ReplaySourceCompletion::Window(owner)
                        }
                    },
                    binding,
                    continuum_transform: None,
                    spectral_support_sample_count: 0,
                }),
            };
            return Ok(output);
        } else {
            let BulkSourceCompletion::Full(owner) = owner else {
                return Err(io::Error::other("initial bulk traversal returned a window"));
            };
            let (state, summary, proof) = weighting
                .finish(owner.generation_id(), owner.sample_count())
                .map_err(io::Error::other)?;
            (state, summary, proof, owner)
        };
        let samples = summary.sample_count();
        self.accept_initial_stream_with_coverage::<io::Error>(
            context,
            fragment,
            problem,
            None,
            CompletedWeightingBlockStream {
                selected,
                owner_completion: owner,
                weights: (state, summary),
                continuum: None,
                spectral_support_sample_count: 0,
                prepared_samples: samples,
                measurements,
            },
            Some(proof),
        )
        .map_err(io::Error::other)?;
        Ok(output)
    }

    pub(crate) fn traverse_bulk_next<F: BulkConsumer>(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        problem: &CompiledProblem,
        input: BulkInputPlan,
        channels: std::ops::Range<usize>,
        consumer: F,
    ) -> io::Result<F::Completion> {
        let WeightingExecutionPhase::PendingReplay { frozen, .. } = &self.phase else {
            return Err(io::Error::other(
                "bulk additional wave lacks a pending source pass",
            ));
        };
        let retained = self
            .retained_observation
            .take()
            .ok_or_else(|| io::Error::other("bulk source owner was released"))?;
        if retained.attempt_id != context.attempt_id()
            || retained.lease_epoch != context.lease_epoch()
            || retained.owner_node != context.node().id
        {
            return Err(io::Error::other(
                "bulk source belongs to a different attempt",
            ));
        }
        let (completed, output) = execute(
            problem,
            retained.selected,
            fragment
                .bounded_stream_plan(context, false)
                .map_err(io::Error::other)?,
            input,
            fragment.plan,
            false,
            Some(channels.clone()),
            consumer,
        )?;
        let replay = match &self.phase {
            WeightingExecutionPhase::PendingReplay { pending, .. } => &pending.state,
            _ => unreachable!(),
        };
        let BulkSourceCompletion::Window(owner) = completed.owner else {
            return Err(io::Error::other(
                "additional bulk wave returned exhaustive coverage",
            ));
        };
        let expected_samples = problem.selected_observation().read_set().sources()[0]
            .selection()
            .rows()
            .selected_row_count()
            .checked_mul(channels.len() as u64)
            .and_then(|n| n.checked_mul(input.correlations as u64))
            .ok_or_else(|| io::Error::other("bulk window sample count overflow"))?;
        if owner.generation_id() != frozen.binding.source_generation
            || owner.channel_ordinals().as_ref() != Some(&channels)
            || owner.sample_count() != expected_samples
            || expected_samples > replay.sample_count()
        {
            return Err(io::Error::other(
                "bulk channel window differs from its source binding",
            ));
        }
        self.latest_traversal_measurements = Some(*owner.measurements());
        self.latest_stream_measurements = Some(completed.measurements);
        self.retained_observation = Some(RetainedWeightingObservation {
            selected: completed.selected,
            ..retained
        });
        Ok(output)
    }
}
