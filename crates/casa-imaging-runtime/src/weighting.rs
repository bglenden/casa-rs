// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime composition of reconstruction phases with opaque T17 traversal evidence.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    mem::align_of,
    sync::Arc,
};

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, MeasurementSetIdentity, SelectedObservationGenerationId,
    SelectedObservationSample, SelectedSpectralContribution, SelectedSpectralContributions,
    SelectedSpectralInterval, SequentialContinuumTransform,
};
use casa_imaging_reconstruction::runtime_adapter::WeightingReplayPhase;
use casa_imaging_reconstruction::{
    FusedWeightingPhase, WeightingAlgorithmState, WeightingDensityPhase, WeightingError,
    WeightingGenerationId, WeightingPlan, WeightingReplayChunk as ReconstructionWeightedBlock,
    WeightingReplayCoverageId, WeightingReplayId, WeightingReplaySummary, WeightingResidency,
    WeightingSampleValue as ReconstructionWeightedSample,
    WeightingSelectedSample as ReconstructionSelectedSample,
    WeightingSpectralValue as ReconstructionWeightedSpectralValue, begin_natural_weighting_stream,
    begin_weighting_generation, compile_spectral_stencil,
};
use casa_ms::{
    BoundObservationSourceError, BoundSelectedObservation, SelectedObservationBlock,
    SelectedObservationBlockConsumer, SelectedObservationBlockSource,
    SelectedObservationCompletion, SelectedObservationResidencyCertificate,
    SelectedObservationTerminal, SelectedObservationTraversalError,
    SelectedObservationTraversalMeasurements, SelectedObservationTraversalSample,
};

use crate::bounded_stream::{
    BlockIdentity, BoundedStreamError, BoundedStreamMeasurements, BoundedStreamPlan,
    KernelPartition, OrderedBlockSource, PartitionedKernel, SourcePoll, WorkIdentity,
    execute_bounded,
};
use crate::{
    AllocationAccess, AllocationId, AllocationLayout, AllocationLifetime, AllocationPurpose,
    AllocationUse, AlternativeId, AttemptBoundObservationCompletion, CacheDemand,
    CapabilityPredicate, CapacityDomainId, CapacityViewId, ClaimLifetime, CountDemand,
    DemandAlternative, DemandAlternatives, DemandEnvelope, ExecutionAttemptId, ExecutionDag,
    ExecutionDagSpecification, ExecutionError, FenceId, FenceKind, InitializationPolicy,
    IoBufferDemand, IoBufferKind, IoPrediction, LeaseResource, LogicalAllocation, MemoryDemand,
    ObservationCompletionBindingError, ObservationReadCompletionContext, PhysicalSlot,
    PhysicalSlotId, PhysicalWorkBinding, PhysicalWorkBindingError, PlanPrediction, QuiescencePoint,
    ResourceAuthority, ResourceClaim, ResourceError, ResourceHeadroom, ResourceLease,
    ResourcePolicy, RuntimeOverheadDemand, ScalingMetadata, SlotCompatibility, StagePrediction,
    StorageMode, WorkDependency, WorkDomain, WorkExecutionContext, WorkImplementationId, WorkKind,
    WorkNode, WorkNodeId,
};
use crate::{
    ContinuumTransformCompletion, ContinuumTransformError, ContinuumTransformStream,
    ContinuumTransformedSample, plan_continuum_transform_row,
};

#[derive(Clone, Copy, PartialEq)]
struct SpectralContributionKey {
    measurement_set: MeasurementSetIdentity,
    field_id: i32,
    spectral_window_id: u32,
    channel_index: u32,
    native: SelectedSpectralInterval,
    output_frame: SelectedSpectralInterval,
}

struct SpectralContributionCache {
    last: Option<(SpectralContributionKey, SelectedSpectralContributions)>,
}

impl SpectralContributionCache {
    const fn new() -> Self {
        Self { last: None }
    }

    fn compile(
        &mut self,
        problem: &CompiledProblem,
        reported: &SelectedObservationTraversalSample<'_>,
    ) -> Result<SelectedSpectralContributions, WeightingError> {
        let sample = reported.selected();
        let key = SpectralContributionKey {
            measurement_set: sample.address.measurement_set,
            field_id: sample.metadata.field_id,
            spectral_window_id: sample.address.spectral_window_id,
            channel_index: sample.address.channel_index,
            native: reported.spectral_evaluation().native(),
            output_frame: reported.spectral_evaluation().output_frame(),
        };
        if let Some((cached_key, contributions)) = &self.last
            && *cached_key == key
        {
            return Ok(contributions.clone());
        }
        let contributions =
            compile_spectral_stencil(problem, sample, reported.spectral_evaluation())?
                .contributions()
                .clone();
        self.last = Some((key, contributions.clone()));
        Ok(contributions)
    }
}

fn transformed_spectral_contributions(
    problem: &CompiledProblem,
    transformed: &ContinuumTransformedSample,
) -> Result<SelectedSpectralContributions, WeightingError> {
    if !transformed.use_role().contributes_to_output() {
        return Ok(SelectedSpectralContributions::empty());
    }
    Ok(compile_spectral_stencil(
        problem,
        transformed.selected(),
        transformed.spectral_evaluation(),
    )?
    .contributions()
    .clone())
}

fn density_spectral_contributions(
    cache: &mut SpectralContributionCache,
    problem: &CompiledProblem,
    reported: &SelectedObservationTraversalSample<'_>,
    continuum: &SequentialContinuumTransform,
) -> Result<SelectedSpectralContributions, ContinuumDensityCallbackError> {
    if let Some(rule) = continuum.rule(
        reported.selected().metadata.field_id,
        reported.selected().address.spectral_window_id,
    ) {
        let use_role = rule
            .channel_use(reported.selected().address.channel_index)
            .ok_or(ContinuumTransformError::UndeclaredChannel)?;
        if !use_role.contributes_to_output() {
            return Ok(SelectedSpectralContributions::empty());
        }
    }
    cache
        .compile(problem, reported)
        .map_err(ContinuumDensityCallbackError::Owner)
}

struct SelectedBlockSource<'a> {
    source: SelectedObservationBlockSource<'a>,
}

impl OrderedBlockSource for SelectedBlockSource<'_> {
    type Storage = SelectedObservationBlock;
    type Completion = SelectedObservationTerminal;
    type Error = BoundObservationSourceError;

    fn create_storage(&self, slot: usize) -> Self::Storage {
        self.source.create_storage(slot)
    }

    fn fill(
        &mut self,
        _block_ordinal: u64,
        storage: &mut Self::Storage,
        _cancellation: crate::bounded_stream::SourceFillCancellation<'_>,
    ) -> Result<SourcePoll, Self::Error> {
        let Some(source_ordinal) = self.source.fill_next(storage)? else {
            return Ok(SourcePoll::Exhausted);
        };
        Ok(SourcePoll::Ready {
            source_ordinal,
            logical_bytes: storage.logical_bytes(),
            source_read_operations: storage.source_read_operations(),
            resident_current_bytes: storage.resident_current_bytes()?,
            resident_capacity_bytes: storage.resident_capacity_bytes()?,
        })
    }

    fn complete(self) -> Result<Self::Completion, Self::Error> {
        self.source.complete()
    }
}

trait StreamingWeightPhase {
    type Finish;

    fn consume_sample(
        &mut self,
        problem: &CompiledProblem,
        sample: &SelectedObservationSample,
        contributions: SelectedSpectralContributions,
    ) -> Result<Option<ReconstructionWeightedBlock>, WeightingError>;

    fn reuse_emitted_block(
        &mut self,
        block: ReconstructionWeightedBlock,
    ) -> Result<(), WeightingError>;

    fn finish_phase(
        self,
    ) -> Result<(Option<ReconstructionWeightedBlock>, Self::Finish), WeightingError>;
}

impl StreamingWeightPhase for FusedWeightingPhase {
    type Finish = (WeightingAlgorithmState, WeightingReplaySummary);

    fn consume_sample(
        &mut self,
        problem: &CompiledProblem,
        sample: &SelectedObservationSample,
        contributions: SelectedSpectralContributions,
    ) -> Result<Option<ReconstructionWeightedBlock>, WeightingError> {
        self.consume(problem, sample, contributions)
    }

    fn reuse_emitted_block(
        &mut self,
        block: ReconstructionWeightedBlock,
    ) -> Result<(), WeightingError> {
        self.reuse_emitted_block(block)
    }

    fn finish_phase(
        self,
    ) -> Result<(Option<ReconstructionWeightedBlock>, Self::Finish), WeightingError> {
        let (block, state, summary) = self.finish()?;
        Ok((block, (state, summary)))
    }
}

impl StreamingWeightPhase for WeightingReplayPhase<'_> {
    type Finish = WeightingReplaySummary;

    fn consume_sample(
        &mut self,
        problem: &CompiledProblem,
        sample: &SelectedObservationSample,
        contributions: SelectedSpectralContributions,
    ) -> Result<Option<ReconstructionWeightedBlock>, WeightingError> {
        self.consume(problem, sample, contributions)
    }

    fn reuse_emitted_block(
        &mut self,
        block: ReconstructionWeightedBlock,
    ) -> Result<(), WeightingError> {
        self.reuse_emitted_block(block)
    }

    fn finish_phase(
        self,
    ) -> Result<(Option<ReconstructionWeightedBlock>, Self::Finish), WeightingError> {
        self.finish()
    }
}

#[derive(Debug)]
enum WeightingBlockKernelError<E> {
    Traversal(SelectedObservationTraversalError<ReplayCallbackError<E>>),
    Owner(WeightingError),
    Transform(ContinuumTransformError),
    Consumer(E),
}

impl<E: fmt::Display> fmt::Display for WeightingBlockKernelError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Traversal(error) => error.fmt(formatter),
            Self::Owner(error) => error.fmt(formatter),
            Self::Transform(error) => error.fmt(formatter),
            Self::Consumer(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for WeightingBlockKernelError<E> {}

struct WeightingBlockKernel<'a, W, F> {
    problem: &'a CompiledProblem,
    consumer: SelectedObservationBlockConsumer<'a>,
    weights: W,
    continuum: Option<ContinuumTransformStream<'a>>,
    spectral_support_sample_count: u64,
    spectral_contributions: SpectralContributionCache,
    emit: F,
}

struct WeightingBlockKernelCompletion<'a, T> {
    consumer: SelectedObservationBlockConsumer<'a>,
    weights: T,
    continuum: Option<ContinuumTransformStream<'a>>,
    spectral_support_sample_count: u64,
}

struct DensityBlockKernel<'a> {
    problem: &'a CompiledProblem,
    consumer: SelectedObservationBlockConsumer<'a>,
    density: WeightingDensityPhase,
    spectral_contributions: SpectralContributionCache,
}

struct DensityBlockKernelCompletion<'a> {
    consumer: SelectedObservationBlockConsumer<'a>,
    density: WeightingDensityPhase,
}

#[derive(Debug)]
enum DensityBlockKernelError {
    Traversal(SelectedObservationTraversalError<ContinuumDensityCallbackError>),
}

impl fmt::Display for DensityBlockKernelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Traversal(error) => error.fmt(formatter),
        }
    }
}

impl Error for DensityBlockKernelError {}

struct CompletedWeightingBlockStream<'a, T> {
    selected: BoundSelectedObservation,
    owner_completion: SelectedObservationCompletion,
    weights: T,
    continuum: Option<ContinuumTransformStream<'a>>,
    spectral_support_sample_count: u64,
    measurements: BoundedStreamMeasurements,
}

struct WeightingBlockStreamFailure<E> {
    error: Box<WeightingReplayError<E>>,
    measurements: Box<BoundedStreamMeasurements>,
}

impl<E> From<WeightingReplayError<E>> for WeightingBlockStreamFailure<E> {
    fn from(error: WeightingReplayError<E>) -> Self {
        Self {
            error: Box::new(error),
            measurements: Box::new(BoundedStreamMeasurements::default()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightingStreamRuntimeError {
    /// Exact diagnostic counters exceeded their numeric domain.
    MeasurementOverflow,
    /// Kernel partitions contradicted deterministic execution rules.
    InvalidKernelPlan,
    /// Live reusable source storage exceeded the immutable execution budget.
    ResidencyExceeded,
    /// The scoped source producer panicked.
    ProducerPanicked,
    /// The bounded source and consumer channels disconnected prematurely.
    ProducerDisconnected,
}

impl fmt::Display for WeightingStreamRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MeasurementOverflow => {
                formatter.write_str("bounded-stream measurements overflowed")
            }
            Self::InvalidKernelPlan => formatter.write_str("bounded-stream kernel plan is invalid"),
            Self::ResidencyExceeded => {
                formatter.write_str("bounded-stream source residency exceeded its plan")
            }
            Self::ProducerPanicked => {
                formatter.write_str("bounded-stream source producer panicked")
            }
            Self::ProducerDisconnected => {
                formatter.write_str("bounded-stream source producer disconnected")
            }
        }
    }
}

impl Error for WeightingStreamRuntimeError {}

fn consume_weighting_sample<W, F, E>(
    problem: &CompiledProblem,
    weights: &mut W,
    continuum: &mut Option<ContinuumTransformStream<'_>>,
    spectral_support_sample_count: &mut u64,
    spectral_contributions: &mut SpectralContributionCache,
    emit: &mut F,
    reported: SelectedObservationTraversalSample<'_>,
) -> Result<(), ReplayCallbackError<E>>
where
    W: StreamingWeightPhase,
    F: FnMut(&ReconstructionWeightedBlock) -> Result<(), E>,
{
    if let Some(transform) = continuum {
        let completed = transform
            .push(*reported.selected(), reported.spectral_evaluation())
            .map_err(ReplayCallbackError::Transform)?;
        for transformed in completed {
            let contributions = transformed_spectral_contributions(problem, &transformed)
                .map_err(ReplayCallbackError::Owner)?;
            if !contributions.is_empty() {
                *spectral_support_sample_count =
                    spectral_support_sample_count.checked_add(1).ok_or(
                        ReplayCallbackError::Owner(WeightingError::SampleCountOverflow),
                    )?;
            }
            if let Some(block) = weights
                .consume_sample(problem, transformed.selected(), contributions)
                .map_err(ReplayCallbackError::Owner)?
            {
                emit(&block).map_err(ReplayCallbackError::Consumer)?;
                weights
                    .reuse_emitted_block(block)
                    .map_err(ReplayCallbackError::Owner)?;
            }
        }
    } else {
        let contributions = spectral_contributions
            .compile(problem, &reported)
            .map_err(ReplayCallbackError::Owner)?;
        if let Some(block) = weights
            .consume_sample(problem, reported.selected(), contributions)
            .map_err(ReplayCallbackError::Owner)?
        {
            emit(&block).map_err(ReplayCallbackError::Consumer)?;
            weights
                .reuse_emitted_block(block)
                .map_err(ReplayCallbackError::Owner)?;
        }
    }
    Ok(())
}

impl<'a, W, F, E> PartitionedKernel<SelectedObservationBlock> for WeightingBlockKernel<'a, W, F>
where
    W: StreamingWeightPhase + Sync,
    F: FnMut(&ReconstructionWeightedBlock) -> Result<(), E> + Sync,
    E: Error + Send + 'static,
{
    type Partition = ();
    type Partial = ();
    type Completion = WeightingBlockKernelCompletion<'a, W::Finish>;
    type Error = WeightingBlockKernelError<E>;

    fn partition_count(
        &self,
        _block: BlockIdentity,
        _storage: &SelectedObservationBlock,
    ) -> Result<usize, Self::Error> {
        Ok(1)
    }

    fn partition(
        &self,
        _block: BlockIdentity,
        _storage: &SelectedObservationBlock,
        local_ordinal: usize,
    ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
        debug_assert_eq!(local_ordinal, 0);
        Ok(KernelPartition::exclusive(0, 0, ()))
    }

    fn execute(
        &self,
        _work: WorkIdentity,
        _storage: &SelectedObservationBlock,
        _partition: &Self::Partition,
    ) -> Result<Self::Partial, Self::Error> {
        Ok(())
    }

    fn commit(
        &mut self,
        _work: WorkIdentity,
        storage: &SelectedObservationBlock,
        (): Self::Partial,
    ) -> Result<(), Self::Error> {
        let problem = self.problem;
        let weights = &mut self.weights;
        let continuum = &mut self.continuum;
        let spectral_support_sample_count = &mut self.spectral_support_sample_count;
        let spectral_contributions = &mut self.spectral_contributions;
        let emit = &mut self.emit;
        self.consumer
            .consume(storage, |reported| {
                consume_weighting_sample(
                    problem,
                    weights,
                    continuum,
                    spectral_support_sample_count,
                    spectral_contributions,
                    emit,
                    reported,
                )
            })
            .map_err(WeightingBlockKernelError::Traversal)
    }

    fn complete(mut self) -> Result<Self::Completion, Self::Error> {
        if let Some(transform) = &mut self.continuum {
            for transformed in transform
                .finish_rows()
                .map_err(WeightingBlockKernelError::Transform)?
            {
                let contributions = transformed_spectral_contributions(self.problem, &transformed)
                    .map_err(WeightingBlockKernelError::Owner)?;
                if !contributions.is_empty() {
                    self.spectral_support_sample_count =
                        self.spectral_support_sample_count.checked_add(1).ok_or(
                            WeightingBlockKernelError::Owner(WeightingError::SampleCountOverflow),
                        )?;
                }
                if let Some(block) = self
                    .weights
                    .consume_sample(self.problem, transformed.selected(), contributions)
                    .map_err(WeightingBlockKernelError::Owner)?
                {
                    (self.emit)(&block).map_err(WeightingBlockKernelError::Consumer)?;
                    self.weights
                        .reuse_emitted_block(block)
                        .map_err(WeightingBlockKernelError::Owner)?;
                }
            }
        }
        let (final_block, weights) = self
            .weights
            .finish_phase()
            .map_err(WeightingBlockKernelError::Owner)?;
        if let Some(block) = final_block {
            (self.emit)(&block).map_err(WeightingBlockKernelError::Consumer)?;
        }
        Ok(WeightingBlockKernelCompletion {
            consumer: self.consumer,
            weights,
            continuum: self.continuum,
            spectral_support_sample_count: self.spectral_support_sample_count,
        })
    }
}

impl<'a> PartitionedKernel<SelectedObservationBlock> for DensityBlockKernel<'a> {
    type Partition = ();
    type Partial = ();
    type Completion = DensityBlockKernelCompletion<'a>;
    type Error = DensityBlockKernelError;

    fn partition_count(
        &self,
        _block: BlockIdentity,
        _storage: &SelectedObservationBlock,
    ) -> Result<usize, Self::Error> {
        Ok(1)
    }

    fn partition(
        &self,
        _block: BlockIdentity,
        _storage: &SelectedObservationBlock,
        local_ordinal: usize,
    ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
        debug_assert_eq!(local_ordinal, 0);
        Ok(KernelPartition::exclusive(0, 0, ()))
    }

    fn execute(
        &self,
        _work: WorkIdentity,
        _storage: &SelectedObservationBlock,
        _partition: &Self::Partition,
    ) -> Result<Self::Partial, Self::Error> {
        Ok(())
    }

    fn commit(
        &mut self,
        _work: WorkIdentity,
        storage: &SelectedObservationBlock,
        (): Self::Partial,
    ) -> Result<(), Self::Error> {
        let problem = self.problem;
        let continuum = problem.visibility_transform();
        let density = &mut self.density;
        let spectral_contributions = &mut self.spectral_contributions;
        self.consumer
            .consume(storage, |reported| {
                let contributions = match continuum {
                    Some(continuum) => density_spectral_contributions(
                        spectral_contributions,
                        problem,
                        &reported,
                        continuum,
                    )?,
                    None => spectral_contributions
                        .compile(problem, &reported)
                        .map_err(ContinuumDensityCallbackError::Owner)?,
                };
                density
                    .consume(problem, reported.selected(), contributions)
                    .map_err(ContinuumDensityCallbackError::Owner)
            })
            .map_err(DensityBlockKernelError::Traversal)
    }

    fn complete(self) -> Result<Self::Completion, Self::Error> {
        Ok(DensityBlockKernelCompletion {
            consumer: self.consumer,
            density: self.density,
        })
    }
}

fn begin_continuum_stream<E>(
    problem: &CompiledProblem,
) -> Result<Option<ContinuumTransformStream<'_>>, WeightingReplayError<E>> {
    let Some(contract) = problem.visibility_transform() else {
        return Ok(None);
    };
    let plan = plan_continuum_transform_row(problem)
        .map_err(WeightingReplayError::Transform)?
        .ok_or(WeightingReplayError::Evidence(WeightingEvidenceError))?;
    Ok(Some(
        ContinuumTransformStream::new(contract, plan).map_err(WeightingReplayError::Transform)?,
    ))
}

fn widen_terminal_traversal_error<E>(
    error: SelectedObservationTraversalError<std::convert::Infallible>,
) -> SelectedObservationTraversalError<ReplayCallbackError<E>> {
    match error {
        SelectedObservationTraversalError::Binding(error) => {
            SelectedObservationTraversalError::Binding(error)
        }
        SelectedObservationTraversalError::Source(error) => {
            SelectedObservationTraversalError::Source(error)
        }
        SelectedObservationTraversalError::Inspection(error) => {
            SelectedObservationTraversalError::Inspection(error)
        }
        SelectedObservationTraversalError::Consumer(error) => match error {},
        SelectedObservationTraversalError::TraversalIdentityExhausted => {
            SelectedObservationTraversalError::TraversalIdentityExhausted
        }
        SelectedObservationTraversalError::MeasurementOverflow => {
            SelectedObservationTraversalError::MeasurementOverflow
        }
    }
}

fn map_bounded_stream_error<E>(
    error: BoundedStreamError<BoundObservationSourceError, WeightingBlockKernelError<E>>,
) -> WeightingReplayError<E> {
    match error {
        BoundedStreamError::Source(error) => {
            WeightingReplayError::Traversal(SelectedObservationTraversalError::Source(error))
        }
        BoundedStreamError::Kernel(WeightingBlockKernelError::Traversal(error)) => {
            WeightingReplayError::Traversal(error)
        }
        BoundedStreamError::Kernel(WeightingBlockKernelError::Owner(error)) => {
            WeightingReplayError::Owner(error)
        }
        BoundedStreamError::Kernel(WeightingBlockKernelError::Transform(error)) => {
            WeightingReplayError::Transform(error)
        }
        BoundedStreamError::Kernel(WeightingBlockKernelError::Consumer(error)) => {
            WeightingReplayError::Consumer(error)
        }
        BoundedStreamError::MeasurementOverflow => {
            WeightingReplayError::Runtime(WeightingStreamRuntimeError::MeasurementOverflow)
        }
        BoundedStreamError::InvalidKernelPlan => {
            WeightingReplayError::Runtime(WeightingStreamRuntimeError::InvalidKernelPlan)
        }
        BoundedStreamError::ResidencyExceeded => {
            WeightingReplayError::Runtime(WeightingStreamRuntimeError::ResidencyExceeded)
        }
        BoundedStreamError::ProducerPanicked => {
            WeightingReplayError::Runtime(WeightingStreamRuntimeError::ProducerPanicked)
        }
        BoundedStreamError::ProducerDisconnected => {
            WeightingReplayError::Runtime(WeightingStreamRuntimeError::ProducerDisconnected)
        }
    }
}

fn execute_weighting_block_stream<'a, W, F, E>(
    problem: &'a CompiledProblem,
    selected: BoundSelectedObservation,
    plan: BoundedStreamPlan,
    weights: W,
    continuum: Option<ContinuumTransformStream<'a>>,
    emit: F,
) -> Result<CompletedWeightingBlockStream<'a, W::Finish>, WeightingBlockStreamFailure<E>>
where
    W: StreamingWeightPhase + Sync,
    F: FnMut(&ReconstructionWeightedBlock) -> Result<(), E> + Sync,
    E: Error + Send + 'static,
{
    let (source, consumer) = selected.into_block_stream(problem).map_err(|error| {
        WeightingReplayError::Traversal(SelectedObservationTraversalError::Binding(error))
    })?;
    let outcome = match execute_bounded(
        plan,
        0,
        SelectedBlockSource { source },
        WeightingBlockKernel {
            problem,
            consumer,
            weights,
            continuum,
            spectral_support_sample_count: 0,
            spectral_contributions: SpectralContributionCache::new(),
            emit,
        },
    ) {
        Ok(outcome) => outcome,
        Err(failure) => {
            return Err(WeightingBlockStreamFailure {
                error: Box::new(map_bounded_stream_error(*failure.cause)),
                measurements: failure.measurements,
            });
        }
    };
    let mut terminal = outcome.source_completion;
    terminal
        .record_runtime_residency(
            outcome.measurements.peak_live_source_blocks,
            outcome.measurements.peak_live_source_current_bytes,
            outcome.measurements.peak_live_source_capacity_bytes,
        )
        .map_err(|error| {
            WeightingReplayError::Traversal(SelectedObservationTraversalError::Source(error))
        })?;
    let WeightingBlockKernelCompletion {
        consumer,
        weights,
        continuum,
        spectral_support_sample_count,
    } = outcome.kernel_completion;
    let (selected, owner_completion) = consumer
        .complete(terminal)
        .map_err(|error| WeightingReplayError::Traversal(widen_terminal_traversal_error(error)))?;
    Ok(CompletedWeightingBlockStream {
        selected,
        owner_completion,
        weights,
        continuum,
        spectral_support_sample_count,
        measurements: outcome.measurements,
    })
}

/// Scientific phase occupied by one ordinary continuum reconstruction plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SpectralPassPhase {
    /// Normal state used to drive a minor cycle.
    InitialMajor,
    /// Mandatory reconciliation of the accepted final model.
    FinalMajor,
}

/// Stable phase and ordinal namespace for one continuum pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SpectralPassIdentity {
    phase: SpectralPassPhase,
    ordinal: u32,
}

impl SpectralPassIdentity {
    /// Construct one explicit pass namespace.
    #[must_use]
    pub const fn new(phase: SpectralPassPhase, ordinal: u32) -> Self {
        Self { phase, ordinal }
    }

    /// Return the semantic phase namespace.
    #[must_use]
    pub const fn phase(self) -> SpectralPassPhase {
        self.phase
    }

    /// Return the phase-local plan ordinal.
    #[must_use]
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }

    fn suffix(self) -> String {
        let phase = match self.phase {
            SpectralPassPhase::InitialMajor => "initial-major",
            SpectralPassPhase::FinalMajor => "final-major",
        };
        format!("{phase}-{}", self.ordinal)
    }
}

/// Exact source resources retained by one selected-observation weighting lifecycle.
///
/// This binds the selected-content budget to the logical source allocations and
/// queue permit that remain live through the fragment's explicit release node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedObservationSourceResources {
    residency: SelectedObservationResidencyCertificate,
    allocations: BTreeSet<AllocationId>,
    queue: LeaseResource,
}

impl SelectedObservationSourceResources {
    /// Bind owner-certified source residency to its exact logical allocations and queue.
    #[must_use]
    pub fn new(
        residency: SelectedObservationResidencyCertificate,
        allocations: BTreeSet<AllocationId>,
        queue: LeaseResource,
    ) -> Self {
        Self {
            residency,
            allocations,
            queue,
        }
    }
}

/// Production composition of one frozen global weighting generation and replay.
///
/// The fragment inserts the complete generation, replay, and release lifecycle
/// into an already validated observation transaction. It is the only supported
/// route for attaching reconstruction weighting residency to physical work.
pub struct WeightingPlanFragment<'a> {
    plan: &'a WeightingPlan,
    source_read: WorkNodeId,
    source_resources: SelectedObservationSourceResources,
    generation_implementation: WorkImplementationId,
    replay_implementation: WorkImplementationId,
    release_implementation: WorkImplementationId,
    ids: WeightingPlanIds,
    streaming: Option<WeightingStreamingMode>,
    continuum_row_bytes: Option<u64>,
}

/// Production selected-payload traversal shape for one continuum major pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightingStreamingMode {
    /// Natural weighting is generated and consumed in the transaction read.
    NaturalInitial,
    /// Density is generated in the transaction read and consumed in one terminal stream.
    DensityInitial,
    /// A later major consumes a previously frozen weighting state in the transaction read.
    Reuse,
}

impl<'a> WeightingPlanFragment<'a> {
    /// Bind a non-streaming integration fragment.
    ///
    /// Production continuum execution uses [`Self::streaming_for_pass`]; this
    /// constructor cannot enter the bounded spectral-cycle owner.
    #[must_use]
    pub fn new(
        plan: &'a WeightingPlan,
        source_read: WorkNodeId,
        source_resources: SelectedObservationSourceResources,
        generation_implementation: WorkImplementationId,
        replay_implementation: WorkImplementationId,
        release_implementation: WorkImplementationId,
    ) -> Self {
        Self::new_for_pass(
            plan,
            source_read,
            source_resources,
            generation_implementation,
            replay_implementation,
            release_implementation,
            SpectralPassIdentity::new(SpectralPassPhase::InitialMajor, 0),
        )
    }

    /// Bind one reconstruction plan in an explicit phase/ordinal namespace.
    #[must_use]
    pub fn new_for_pass(
        plan: &'a WeightingPlan,
        source_read: WorkNodeId,
        source_resources: SelectedObservationSourceResources,
        generation_implementation: WorkImplementationId,
        replay_implementation: WorkImplementationId,
        release_implementation: WorkImplementationId,
        pass: SpectralPassIdentity,
    ) -> Self {
        Self {
            plan,
            source_read,
            source_resources,
            generation_implementation,
            replay_implementation,
            release_implementation,
            ids: WeightingPlanIds::new(plan, pass),
            streaming: None,
            continuum_row_bytes: None,
        }
    }

    /// Bind the production streaming traversal shape for a continuum pass.
    #[must_use]
    pub fn streaming_for_pass(
        plan: &'a WeightingPlan,
        source_read: WorkNodeId,
        source_resources: SelectedObservationSourceResources,
        implementation: WorkImplementationId,
        pass: SpectralPassIdentity,
        mode: WeightingStreamingMode,
        continuum_row_bytes: Option<u64>,
    ) -> Self {
        Self {
            plan,
            source_read,
            source_resources,
            generation_implementation: implementation.clone(),
            replay_implementation: implementation.clone(),
            release_implementation: implementation,
            ids: WeightingPlanIds::new(plan, pass),
            streaming: Some(mode),
            continuum_row_bytes,
        }
    }

    /// Return the sole terminal weighted payload traversal node.
    #[must_use]
    pub const fn streaming_node(&self) -> &WorkNodeId {
        match self.streaming {
            Some(WeightingStreamingMode::DensityInitial) => &self.ids.generation_node,
            Some(WeightingStreamingMode::NaturalInitial | WeightingStreamingMode::Reuse) => {
                &self.source_read
            }
            None => &self.ids.replay_node,
        }
    }

    /// Return the selected-payload traversal shape owned by this fragment.
    #[must_use]
    pub const fn streaming_mode(&self) -> Option<WeightingStreamingMode> {
        self.streaming
    }

    /// Return the global density and sum-weight generation node.
    #[must_use]
    pub const fn generation_node(&self) -> &WorkNodeId {
        &self.ids.generation_node
    }

    /// Return the T17 source node whose retained owner enters this lifecycle.
    #[must_use]
    pub const fn source_read_node(&self) -> &WorkNodeId {
        &self.source_read
    }

    pub(crate) const fn source_queue(&self) -> &LeaseResource {
        &self.source_resources.queue
    }

    /// Return the bounded weighted replay node.
    #[must_use]
    pub const fn replay_node(&self) -> &WorkNodeId {
        &self.ids.replay_node
    }

    /// Return the explicit frozen-state release node.
    #[must_use]
    pub const fn release_node(&self) -> &WorkNodeId {
        &self.ids.release_node
    }

    /// Return the logical allocation retaining the frozen generation.
    #[must_use]
    pub const fn frozen_allocation(&self) -> &AllocationId {
        &self.ids.frozen_allocation
    }

    /// Compose the complete weighting lifecycle into existing physical work.
    pub fn compose(
        &self,
        base: &PhysicalWorkBinding,
    ) -> Result<PhysicalWorkBinding, WeightingPlanFragmentError> {
        if let Some(mode) = self.streaming {
            return self.compose_streaming(base, mode);
        }
        self.compose_legacy(base)
    }

    fn compose_legacy(
        &self,
        base: &PhysicalWorkBinding,
    ) -> Result<PhysicalWorkBinding, WeightingPlanFragmentError> {
        let source = base
            .execution_dag()
            .nodes()
            .get(&self.source_read)
            .ok_or_else(|| WeightingPlanFragmentError::MissingNode(self.source_read.clone()))?;
        if source.kind != WorkKind::ObservationRead {
            return Err(WeightingPlanFragmentError::InvalidSourceKind(
                self.source_read.clone(),
            ));
        }
        let reconciliation_id = base
            .observation_transaction()
            .post_replay_reconciliation()
            .expect("weighting composition requires reconstruction");
        let model_preparation_id = base
            .observation_transaction()
            .final_model_preparation()
            .cloned();
        let commit_id = base.observation_transaction().commit();
        let reconciliation = base
            .execution_dag()
            .nodes()
            .get(reconciliation_id)
            .ok_or_else(|| WeightingPlanFragmentError::MissingNode(reconciliation_id.clone()))?;
        if !base.execution_dag().nodes().contains_key(commit_id) {
            return Err(WeightingPlanFragmentError::MissingNode(commit_id.clone()));
        }

        let source_contract = SourceTraversalContract::from_source(
            base,
            source,
            &self.source_resources.residency,
            &self.source_resources.allocations,
            &self.source_resources.queue,
            &self.ids.release_node,
        )?;
        let io_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
        let read_claims = source_contract
            .traversal_claims
            .iter()
            .chain(&source_contract.retained_claims)
            .cloned()
            .collect::<Vec<_>>();
        let read_allocations = source_contract.allocations.clone();
        let generation = WorkNode {
            id: self.ids.generation_node.clone(),
            kind: WorkKind::ObservationRead,
            domain: WorkDomain::Io,
            implementation: self.generation_implementation.clone(),
            dependencies: terminal_events(source),
            claims: read_claims.clone(),
            allocations: read_allocations
                .iter()
                .cloned()
                .chain([
                    allocation_use(&self.ids.frozen_allocation, io_lifetime.clone()),
                    allocation_use(&self.ids.partial_allocation, io_lifetime.clone()),
                ])
                .collect(),
            fences: BTreeSet::from([FenceKind::Io]),
            quiescence_after: BTreeSet::new(),
        };
        let replay = WorkNode {
            id: self.ids.replay_node.clone(),
            kind: WorkKind::ObservationRead,
            domain: WorkDomain::Io,
            implementation: self.replay_implementation.clone(),
            dependencies: std::iter::once(WorkDependency::Fence(FenceId::new(
                self.ids.generation_node.clone(),
                FenceKind::Io,
            )))
            .chain(
                model_preparation_id
                    .iter()
                    .cloned()
                    .map(WorkDependency::Work),
            )
            .collect(),
            claims: read_claims.clone(),
            allocations: read_allocations
                .into_iter()
                .chain([
                    allocation_use(&self.ids.frozen_allocation, io_lifetime.clone()),
                    allocation_use(&self.ids.reduction_allocation, io_lifetime.clone()),
                    allocation_use(&self.ids.weighted_block_allocation, io_lifetime.clone()),
                ])
                .chain(self.continuum_row_bytes.map(|_| {
                    allocation_use(&self.ids.continuum_row_allocation, io_lifetime.clone())
                }))
                .collect(),
            fences: BTreeSet::from([FenceKind::Io]),
            quiescence_after: BTreeSet::new(),
        };
        let release_claims = std::iter::once(ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        })
        .chain(source_contract.retained_claims.iter().cloned())
        .chain(source_contract.release_buffer_claims.iter().cloned())
        .collect();
        let release = WorkNode {
            id: self.ids.release_node.clone(),
            kind: WorkKind::Release,
            domain: WorkDomain::Cpu,
            implementation: self.release_implementation.clone(),
            dependencies: terminal_events(reconciliation),
            claims: release_claims,
            allocations: std::iter::once(allocation_use(
                &self.ids.frozen_allocation,
                ClaimLifetime::Work,
            ))
            .chain(
                source_contract
                    .retained_allocations
                    .iter()
                    .map(|allocation| allocation_use(allocation, ClaimLifetime::Work)),
            )
            .collect(),
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        };

        let mut nodes = base
            .execution_dag()
            .nodes()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let source_node = nodes
            .iter_mut()
            .find(|node| node.id == self.source_read)
            .ok_or_else(|| WeightingPlanFragmentError::MissingNode(self.source_read.clone()))?;
        source_node.claims = read_claims.clone();
        source_node
            .allocations
            .push(allocation_use(&self.ids.frozen_allocation, io_lifetime));
        nodes
            .iter_mut()
            .find(|node| &node.id == reconciliation_id)
            .ok_or_else(|| WeightingPlanFragmentError::MissingNode(reconciliation_id.clone()))?
            .dependencies
            .insert(WorkDependency::Fence(FenceId::new(
                self.ids.replay_node.clone(),
                FenceKind::Io,
            )));
        nodes
            .iter_mut()
            .find(|node| &node.id == commit_id)
            .ok_or_else(|| WeightingPlanFragmentError::MissingNode(commit_id.clone()))?
            .dependencies
            .insert(WorkDependency::Work(self.ids.release_node.clone()));
        nodes.extend([generation.clone(), replay.clone(), release.clone()]);

        let allocation_specs = self.allocation_specs()?;
        let mut alternative = base.execution_dag().resource_alternative().clone();
        alternative.id = AlternativeId::new(format!(
            "{}-weighting-{}",
            alternative.id.as_str(),
            self.plan.commitment_id()
        ));
        alternative
            .demand
            .memory
            .extend(allocation_specs.iter().map(AllocationSpec::memory_demand));
        let dag = ExecutionDag::new(ExecutionDagSpecification {
            required_resource_capabilities: base
                .execution_dag()
                .required_resource_capabilities()
                .clone(),
            resource_alternative: alternative,
            nodes,
            logical_allocations: base
                .execution_dag()
                .logical_allocations()
                .values()
                .cloned()
                .map(|mut allocation| {
                    if source_contract
                        .retained_allocations
                        .contains(&allocation.id)
                    {
                        allocation.lifetime.release_after =
                            BTreeSet::from([WorkDependency::Work(self.ids.release_node.clone())]);
                    } else if source_contract.allocation_ids.contains(&allocation.id) {
                        allocation
                            .lifetime
                            .release_after
                            .insert(WorkDependency::Fence(FenceId::new(
                                self.ids.replay_node.clone(),
                                FenceKind::Io,
                            )));
                    }
                    allocation
                })
                .chain(
                    allocation_specs
                        .iter()
                        .map(AllocationSpec::logical_allocation),
                )
                .collect(),
            physical_slots: base
                .execution_dag()
                .physical_slots()
                .values()
                .cloned()
                .chain(allocation_specs.iter().map(AllocationSpec::physical_slot))
                .collect(),
            initial_knobs: base.execution_dag().initial_knobs().clone(),
            adaptations: base
                .execution_dag()
                .adaptations()
                .values()
                .cloned()
                .collect(),
        })?;

        let source_prediction = base
            .prediction()
            .stages()
            .get(&self.source_read)
            .ok_or_else(|| WeightingPlanFragmentError::MissingNode(self.source_read.clone()))?;
        let generation_prediction = scaled_prediction(source_prediction, generation.id, 2)?;
        let replay_prediction = scaled_prediction(source_prediction, replay.id, 1)?;
        let release_prediction =
            StagePrediction::new(release.id, 0).with_io(vec![IoPrediction::new(
                IoBufferKind::SourceReadAhead,
                u64::try_from(self.source_resources.residency.aggregate_resident_bytes())
                    .map_err(|_| WeightingPlanFragmentError::PredictionOverflow)?,
                1,
            )]);
        let extra_elapsed = generation_prediction
            .elapsed_nanos()
            .checked_add(replay_prediction.elapsed_nanos())
            .ok_or(WeightingPlanFragmentError::PredictionOverflow)?;
        let prediction = PlanPrediction::new(
            base.prediction()
                .elapsed_nanos()
                .checked_add(extra_elapsed)
                .ok_or(WeightingPlanFragmentError::PredictionOverflow)?,
            base.prediction().confidence(),
            base.prediction().uncertainty().to_vec(),
            base.prediction()
                .stages()
                .values()
                .cloned()
                .chain([generation_prediction, replay_prediction, release_prediction])
                .collect(),
        )?;
        Ok(PhysicalWorkBinding::with_implementation_contract(
            base.implementation_contract().for_execution_dag(&dag)?,
            dag,
            prediction,
            base.artifacts().to_vec(),
            base.observation_transaction().clone(),
            base.publication_layouts().clone(),
            base.product_publication_authority(),
        )?)
    }

    fn compose_streaming(
        &self,
        base: &PhysicalWorkBinding,
        mode: WeightingStreamingMode,
    ) -> Result<PhysicalWorkBinding, WeightingPlanFragmentError> {
        let legacy = self.compose_legacy(base)?;
        let terminal = self.streaming_node().clone();
        let removed = match mode {
            WeightingStreamingMode::DensityInitial => {
                BTreeSet::from([self.ids.replay_node.clone()])
            }
            WeightingStreamingMode::NaturalInitial | WeightingStreamingMode::Reuse => {
                BTreeSet::from([
                    self.ids.generation_node.clone(),
                    self.ids.replay_node.clone(),
                ])
            }
        };
        let extra_allocations = legacy
            .execution_dag()
            .nodes()
            .iter()
            .filter(|(id, _)| removed.contains(*id))
            .flat_map(|(_, node)| node.allocations.iter().cloned())
            .collect::<Vec<_>>();
        let model_preparation = base
            .observation_transaction()
            .final_model_preparation()
            .cloned();
        let reconciliation = base
            .observation_transaction()
            .post_replay_reconciliation()
            .expect("streaming weighting composition requires reconstruction");
        let mut nodes = legacy
            .execution_dag()
            .nodes()
            .values()
            .filter(|node| !removed.contains(&node.id))
            .cloned()
            .collect::<Vec<_>>();
        for node in &mut nodes {
            node.dependencies.retain(|dependency| match dependency {
                WorkDependency::Work(id) => !removed.contains(id),
                WorkDependency::Fence(fence) => !removed.contains(fence.node()),
            });
            if node.id == terminal {
                if let Some(preparation) = &model_preparation {
                    node.dependencies
                        .insert(WorkDependency::Work(preparation.clone()));
                }
                node.allocations.extend(extra_allocations.iter().cloned());
                node.allocations
                    .sort_by(|left, right| left.allocation.as_str().cmp(right.allocation.as_str()));
                node.allocations
                    .dedup_by(|left, right| left.allocation == right.allocation);
            }
            if &node.id == reconciliation {
                node.dependencies.insert(WorkDependency::Fence(FenceId::new(
                    terminal.clone(),
                    FenceKind::Io,
                )));
            }
        }
        let terminal_fence = WorkDependency::Fence(FenceId::new(terminal.clone(), FenceKind::Io));
        let allocations = legacy
            .execution_dag()
            .logical_allocations()
            .values()
            .cloned()
            .map(|mut allocation| {
                if removed.contains(&allocation.lifetime.acquire_at) {
                    allocation.lifetime.acquire_at = terminal.clone();
                }
                let mut release_after = BTreeSet::new();
                for dependency in allocation.lifetime.release_after {
                    let dependency = match dependency {
                        WorkDependency::Work(id) if removed.contains(&id) => terminal_fence.clone(),
                        WorkDependency::Fence(fence) if removed.contains(fence.node()) => {
                            terminal_fence.clone()
                        }
                        other => other,
                    };
                    release_after.insert(dependency);
                }
                allocation.lifetime.release_after = release_after;
                allocation
            })
            .collect();
        let dag = ExecutionDag::new(ExecutionDagSpecification {
            required_resource_capabilities: legacy
                .execution_dag()
                .required_resource_capabilities()
                .clone(),
            resource_alternative: legacy.execution_dag().resource_alternative().clone(),
            nodes,
            logical_allocations: allocations,
            physical_slots: legacy
                .execution_dag()
                .physical_slots()
                .values()
                .cloned()
                .collect(),
            initial_knobs: legacy.execution_dag().initial_knobs().clone(),
            adaptations: legacy
                .execution_dag()
                .adaptations()
                .values()
                .cloned()
                .collect(),
        })?;
        let stages = legacy
            .prediction()
            .stages()
            .values()
            .filter(|stage| !removed.contains(stage.node()))
            .cloned()
            .map(|stage| {
                if mode == WeightingStreamingMode::DensityInitial
                    && stage.node() == &self.ids.generation_node
                {
                    scaled_prediction(
                        base.prediction()
                            .stages()
                            .get(&self.source_read)
                            .expect("source stage"),
                        self.ids.generation_node.clone(),
                        1,
                    )
                    .expect("one-pass prediction")
                } else {
                    stage
                }
            })
            .collect::<Vec<_>>();
        let elapsed_nanos = stages
            .iter()
            .try_fold(0_u64, |total, stage| {
                total.checked_add(stage.elapsed_nanos())
            })
            .ok_or(WeightingPlanFragmentError::PredictionOverflow)?;
        let prediction = PlanPrediction::new(
            elapsed_nanos,
            legacy.prediction().confidence(),
            legacy.prediction().uncertainty().to_vec(),
            stages,
        )?;
        Ok(PhysicalWorkBinding::with_implementation_contract(
            legacy.implementation_contract().for_execution_dag(&dag)?,
            dag,
            prediction,
            legacy.artifacts().to_vec(),
            legacy.observation_transaction().clone(),
            legacy.publication_layouts().clone(),
            legacy.product_publication_authority(),
        )?)
    }

    fn allocation_specs(&self) -> Result<Vec<AllocationSpec>, WeightingPlanFragmentError> {
        let residency = self.plan.planned_residency();
        let frozen_bytes = checked_sum([
            residency.density_grid_bytes(),
            residency.robust_factor_bytes(),
            residency.sum_weight_bytes(),
        ])?;
        let generation_fence = BTreeSet::from([WorkDependency::Fence(FenceId::new(
            self.ids.generation_node.clone(),
            FenceKind::Io,
        ))]);
        let replay_fence = BTreeSet::from([WorkDependency::Fence(FenceId::new(
            self.ids.replay_node.clone(),
            FenceKind::Io,
        ))]);
        let mut specs = vec![
            AllocationSpec::new(
                self.ids.frozen_allocation.clone(),
                self.ids.frozen_slot.clone(),
                frozen_bytes,
                "weighting-frozen-generation",
                self.source_read.clone(),
                BTreeSet::from([WorkDependency::Work(self.ids.release_node.clone())]),
            )?,
            AllocationSpec::new(
                self.ids.partial_allocation.clone(),
                self.ids.partial_slot.clone(),
                residency.shared_density_accumulator_bytes(),
                "weighting-shared-density-accumulator",
                self.ids.generation_node.clone(),
                generation_fence.clone(),
            )?,
            AllocationSpec::new(
                self.ids.reduction_allocation.clone(),
                self.ids.reduction_slot.clone(),
                residency.sum_weight_accumulator_bytes(),
                "weighting-sum-weight-accumulator",
                self.ids.replay_node.clone(),
                replay_fence.clone(),
            )?,
            AllocationSpec::new(
                self.ids.weighted_block_allocation.clone(),
                self.ids.weighted_block_slot.clone(),
                residency.weighted_block_bytes(),
                "weighting-weighted-block",
                self.ids.replay_node.clone(),
                replay_fence.clone(),
            )?,
        ];
        if let Some(bytes) = self.continuum_row_bytes {
            specs.push(AllocationSpec::new(
                self.ids.continuum_row_allocation.clone(),
                self.ids.continuum_row_slot.clone(),
                usize::try_from(bytes)
                    .map_err(|_| WeightingPlanFragmentError::ResidencyOverflow)?,
                "continuum-transform-row",
                self.ids.replay_node.clone(),
                replay_fence,
            )?);
        }
        Ok(specs)
    }

    /// Validate one weighting traversal's complete lease and return its owner certificate.
    pub fn selected_observation_residency(
        &self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
    ) -> Result<&SelectedObservationResidencyCertificate, WeightingEvidenceError> {
        let specs = self
            .allocation_specs()
            .map_err(|_| WeightingEvidenceError)?;
        let (expected_node, mut expected) = if context.node().id == self.ids.generation_node {
            (&self.ids.generation_node, vec![&specs[0], &specs[1]])
        } else if context.node().id == self.ids.replay_node {
            (&self.ids.replay_node, vec![&specs[0], &specs[2], &specs[3]])
        } else {
            return Err(WeightingEvidenceError);
        };
        if self.continuum_row_bytes.is_some() {
            expected.push(&specs[4]);
        }
        validate_work_authority(
            context,
            expected_node,
            &expected,
            WeightingWorkContract::SelectedTraversal {
                problem,
                residency: &self.source_resources.residency,
                queue: &self.source_resources.queue,
                source_allocations: &self.source_resources.allocations,
            },
        )?;
        Ok(&self.source_resources.residency)
    }

    fn authorize_source_observation(
        &self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
        actual: &SelectedObservationResidencyCertificate,
    ) -> Result<(), WeightingEvidenceError> {
        if actual != &self.source_resources.residency || !actual.matches_problem(problem) {
            return Err(WeightingEvidenceError);
        }
        let specs = self
            .allocation_specs()
            .map_err(|_| WeightingEvidenceError)?;
        validate_work_authority(
            context,
            &self.source_read,
            &[&specs[0]],
            WeightingWorkContract::SelectedTraversal {
                problem,
                residency: actual,
                queue: &self.source_resources.queue,
                source_allocations: &self.source_resources.allocations,
            },
        )
    }

    fn bounded_stream_plan(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<BoundedStreamPlan, WeightingEvidenceError> {
        let mut workers = context
            .resources()
            .iter()
            .filter(|capability| capability.resource() == &LeaseResource::Workers);
        let worker_claim = workers.next().ok_or(WeightingEvidenceError)?;
        if worker_claim.amount() == 0 || workers.next().is_some() {
            return Err(WeightingEvidenceError);
        }
        // Delivery 1 has one scientific consumer. A second claimed worker may
        // belong to visibility writeback, not to weighting/gridding execution.
        BoundedStreamPlan::new::<(), ()>(
            self.source_resources.residency.peak_live_blocks(),
            1,
            u64::try_from(self.source_resources.residency.aggregate_resident_bytes())
                .map_err(|_| WeightingEvidenceError)?,
            1,
            0,
        )
        .map_err(|_| WeightingEvidenceError)
    }

    fn authorize_generation(
        &self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
    ) -> Result<WeightingGenerationBinding, WeightingEvidenceError> {
        let specs = self
            .allocation_specs()
            .map_err(|_| WeightingEvidenceError)?;
        let expected = [&specs[0], &specs[1]];
        validate_work_authority(
            context,
            &self.ids.generation_node,
            &expected,
            WeightingWorkContract::SelectedTraversal {
                problem,
                residency: &self.source_resources.residency,
                queue: &self.source_resources.queue,
                source_allocations: &self.source_resources.allocations,
            },
        )?;
        let predecessor = context
            .predecessor_observation_completion(&self.source_read)
            .ok_or(WeightingEvidenceError)?;
        let owner = predecessor.owner_completion();
        if predecessor.attempt_id() != context.attempt_id()
            || predecessor.owner_node() != &self.source_read
            || predecessor.lease_epoch() != context.lease_epoch()
            || owner.problem_id() != problem.problem_id()
            || owner.commitment_id() != problem.selected_observation().commitment_id()
        {
            return Err(WeightingEvidenceError);
        }
        Ok(WeightingGenerationBinding {
            attempt_id: context.attempt_id(),
            owner_node: self.ids.generation_node.clone(),
            lease_epoch: context.lease_epoch(),
            source_generation: owner.generation_id(),
            source_sample_count: owner.sample_count(),
        })
    }

    fn authorize_replay(
        &self,
        context: WorkExecutionContext<'_>,
        frozen: &FrozenWeightingGeneration,
        problem: &CompiledProblem,
    ) -> Result<WeightingGenerationBinding, WeightingEvidenceError> {
        let specs = self
            .allocation_specs()
            .map_err(|_| WeightingEvidenceError)?;
        let expected = [&specs[0], &specs[2], &specs[3]];
        validate_work_authority(
            context,
            &self.ids.replay_node,
            &expected,
            WeightingWorkContract::SelectedTraversal {
                problem,
                residency: &self.source_resources.residency,
                queue: &self.source_resources.queue,
                source_allocations: &self.source_resources.allocations,
            },
        )?;
        let predecessor = context
            .predecessor_observation_completion(&self.ids.generation_node)
            .ok_or(WeightingEvidenceError)?;
        if predecessor.attempt_id() != frozen.binding.attempt_id
            || predecessor.attempt_id() != context.attempt_id()
            || predecessor.owner_node() != &frozen.binding.owner_node
            || predecessor.lease_epoch() != frozen.binding.lease_epoch
            || predecessor.lease_epoch() != context.lease_epoch()
            || predecessor.owner_completion().generation_id() != frozen.artifact.source_generation
            || predecessor.owner_completion().sample_count() != frozen.artifact.source_sample_count
        {
            return Err(WeightingEvidenceError);
        }
        Ok(WeightingGenerationBinding {
            attempt_id: context.attempt_id(),
            owner_node: self.ids.replay_node.clone(),
            lease_epoch: context.lease_epoch(),
            source_generation: predecessor.owner_completion().generation_id(),
            source_sample_count: predecessor.owner_completion().sample_count(),
        })
    }

    fn authorize_release(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), WeightingEvidenceError> {
        let specs = self
            .allocation_specs()
            .map_err(|_| WeightingEvidenceError)?;
        validate_work_authority(
            context,
            &self.ids.release_node,
            &[&specs[0]],
            WeightingWorkContract::Release,
        )?;
        let expected_bytes =
            u64::try_from(self.source_resources.residency.aggregate_resident_bytes())
                .map_err(|_| WeightingEvidenceError)?;
        let selected = context
            .allocations()
            .iter()
            .filter(|capability| {
                self.source_resources
                    .allocations
                    .contains(capability.allocation())
            })
            .collect::<Vec<_>>();
        let selected_bytes = selected
            .iter()
            .try_fold(0_u64, |total, capability| {
                total.checked_add(capability.capacity_bytes())
            })
            .ok_or(WeightingEvidenceError)?;
        let read_buffer_bytes = context
            .resources()
            .iter()
            .filter(|capability| {
                capability.resource() == &LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
                    && capability.lifetime() == &ClaimLifetime::Work
            })
            .try_fold(0_u64, |total, capability| {
                total.checked_add(capability.amount())
            })
            .ok_or(WeightingEvidenceError)?;
        if selected.len() != self.source_resources.allocations.len()
            || selected_bytes != expected_bytes
            || read_buffer_bytes != expected_bytes
        {
            return Err(WeightingEvidenceError);
        }
        Ok(())
    }
}

/// Opaque adapter-owned state for one scheduler-planned weighting lifecycle.
///
/// This is the sole supported retention point across generation and replay
/// fences. The scheduler's explicit Release node consumes it on both success
/// and fail-closed drain paths, so pending traversal state cannot outlive its
/// planned allocation permit.
pub struct WeightingExecutionState {
    phase: WeightingExecutionPhase,
    retained_observation: Option<RetainedWeightingObservation>,
    density: Option<WeightingDensityPhase>,
    imported: Option<FrozenWeightingArtifact>,
    latest_traversal_measurements: Option<SelectedObservationTraversalMeasurements>,
    latest_stream_measurements: Option<BoundedStreamMeasurements>,
}

struct RetainedWeightingObservation {
    selected: BoundSelectedObservation,
    attempt_id: ExecutionAttemptId,
    owner_node: WorkNodeId,
    lease_epoch: u64,
}

impl fmt::Debug for WeightingExecutionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WeightingExecutionState")
            .field("phase", &self.phase)
            .field(
                "has_retained_observation",
                &self.retained_observation.is_some(),
            )
            .finish()
    }
}

impl Default for WeightingExecutionState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
enum WeightingExecutionPhase {
    #[default]
    Empty,
    PendingGeneration(Box<PendingWeightingGeneration>),
    Frozen(FrozenWeightingGeneration),
    PendingReplay {
        frozen: FrozenWeightingGeneration,
        pending: Box<PendingWeightingReplay>,
    },
    Replayed {
        frozen: FrozenWeightingGeneration,
        completion: Box<WeightingReplayCompletion>,
    },
}

impl WeightingExecutionState {
    /// Begin the T19 owner from the exact frozen generation and replay lease.
    pub fn begin_complete_data(
        &self,
        context: WorkExecutionContext<'_>,
        fragment: &crate::CompleteDataPlanFragment,
        problem: &CompiledProblem,
        prepared: crate::CompleteDataPreparedState,
    ) -> Result<crate::SpectralOperatorState, crate::CompleteDataPlanError> {
        let WeightingExecutionPhase::Frozen(frozen) = &self.phase else {
            return Err(crate::CompleteDataPlanError::MissingFrozenWeighting);
        };
        fragment.begin(context, problem, &frozen.artifact.state, prepared)
    }

    /// Construct an empty lifecycle before the generation node is dispatched.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            phase: WeightingExecutionPhase::Empty,
            retained_observation: None,
            density: None,
            imported: None,
            latest_traversal_measurements: None,
            latest_stream_measurements: None,
        }
    }

    /// Begin a later major with the immutable weighting generated initially.
    #[must_use]
    pub fn with_frozen_artifact(artifact: FrozenWeightingArtifact) -> Self {
        Self {
            phase: WeightingExecutionPhase::Empty,
            retained_observation: None,
            density: None,
            imported: Some(artifact),
            latest_traversal_measurements: None,
            latest_stream_measurements: None,
        }
    }

    /// Clone the immutable weighting artifact retained by a completed pass.
    #[must_use]
    pub fn frozen_artifact(&self) -> Option<FrozenWeightingArtifact> {
        match &self.phase {
            WeightingExecutionPhase::Replayed { frozen, .. } => Some(frozen.artifact.clone()),
            _ => None,
        }
    }

    /// Return measurements from the most recently completed owner traversal.
    #[must_use]
    pub(crate) const fn latest_traversal_measurements(
        &self,
    ) -> Option<&SelectedObservationTraversalMeasurements> {
        self.latest_traversal_measurements.as_ref()
    }

    pub(crate) const fn latest_stream_measurements(&self) -> Option<&BoundedStreamMeasurements> {
        self.latest_stream_measurements.as_ref()
    }

    fn begin_measurement_scope(&mut self) {
        self.latest_traversal_measurements = None;
        self.latest_stream_measurements = None;
    }

    /// Validate, traverse, and adopt the exact T17 source owner.
    ///
    /// Owner-certificate and scheduler authority are checked before the first
    /// sample can reach `consume`. The owner then remains inside this lifecycle
    /// across generation, replay, and reconciliation until the scheduler-issued
    /// Release node consumes it.
    pub fn traverse_and_retain_source<E>(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        mut selected: BoundSelectedObservation,
        problem: &CompiledProblem,
        consume: impl FnMut(SelectedObservationTraversalSample<'_>) -> Result<(), E>,
    ) -> Result<SelectedObservationCompletion, WeightingSourceTraversalError<E>>
    where
        E: Error + 'static,
    {
        if !matches!(self.phase, WeightingExecutionPhase::Empty)
            || self.retained_observation.is_some()
            || fragment.streaming.is_some()
            || context.node().id != fragment.source_read
            || !context.node().kind.reads_observation()
        {
            return Err(WeightingSourceTraversalError::Evidence(
                WeightingEvidenceError,
            ));
        }
        fragment
            .authorize_source_observation(context, problem, selected.residency_certificate())
            .map_err(WeightingSourceTraversalError::Evidence)?;
        let completion = selected
            .traverse(problem, consume)
            .map_err(WeightingSourceTraversalError::Traversal)?;
        self.latest_traversal_measurements = Some(*completion.measurements());
        if !selected.can_resume_after(&completion) {
            return Err(WeightingSourceTraversalError::Evidence(
                WeightingEvidenceError,
            ));
        }
        self.retained_observation = Some(RetainedWeightingObservation {
            selected,
            attempt_id: context.attempt_id(),
            owner_node: context.node().id.clone(),
            lease_epoch: context.lease_epoch(),
        });
        Ok(completion)
    }

    /// Run the sole density prepass for a density-dependent initial major.
    pub fn traverse_density_source(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        selected: BoundSelectedObservation,
        problem: &CompiledProblem,
    ) -> Result<SelectedObservationCompletion, ContinuumDensityTraversalError> {
        self.begin_measurement_scope();
        if !matches!(self.phase, WeightingExecutionPhase::Empty)
            || self.retained_observation.is_some()
            || self.density.is_some()
            || fragment.streaming != Some(WeightingStreamingMode::DensityInitial)
        {
            return Err(ContinuumDensityTraversalError::Evidence(
                WeightingEvidenceError,
            ));
        }
        fragment
            .authorize_source_observation(context, problem, selected.residency_certificate())
            .map_err(ContinuumDensityTraversalError::Evidence)?;
        let density = begin_weighting_generation(problem, fragment.plan)
            .map_err(ContinuumDensityTraversalError::Owner)?;
        let plan = fragment
            .bounded_stream_plan(context)
            .map_err(ContinuumDensityTraversalError::Evidence)?;
        let (source, consumer) = selected.into_block_stream(problem).map_err(|error| {
            ContinuumDensityTraversalError::Traversal(SelectedObservationTraversalError::Binding(
                error,
            ))
        })?;
        let outcome = match execute_bounded(
            plan,
            0,
            SelectedBlockSource { source },
            DensityBlockKernel {
                problem,
                consumer,
                density,
                spectral_contributions: SpectralContributionCache::new(),
            },
        ) {
            Ok(outcome) => outcome,
            Err(failure) => {
                self.latest_stream_measurements = Some(*failure.measurements);
                return Err(match *failure.cause {
                    BoundedStreamError::Source(error) => ContinuumDensityTraversalError::Traversal(
                        SelectedObservationTraversalError::Source(error),
                    ),
                    BoundedStreamError::Kernel(DensityBlockKernelError::Traversal(error)) => {
                        ContinuumDensityTraversalError::Traversal(error)
                    }
                    BoundedStreamError::MeasurementOverflow => {
                        ContinuumDensityTraversalError::Runtime(
                            WeightingStreamRuntimeError::MeasurementOverflow,
                        )
                    }
                    BoundedStreamError::InvalidKernelPlan => {
                        ContinuumDensityTraversalError::Runtime(
                            WeightingStreamRuntimeError::InvalidKernelPlan,
                        )
                    }
                    BoundedStreamError::ResidencyExceeded => {
                        ContinuumDensityTraversalError::Runtime(
                            WeightingStreamRuntimeError::ResidencyExceeded,
                        )
                    }
                    BoundedStreamError::ProducerPanicked => {
                        ContinuumDensityTraversalError::Runtime(
                            WeightingStreamRuntimeError::ProducerPanicked,
                        )
                    }
                    BoundedStreamError::ProducerDisconnected => {
                        ContinuumDensityTraversalError::Runtime(
                            WeightingStreamRuntimeError::ProducerDisconnected,
                        )
                    }
                });
            }
        };
        let mut terminal = outcome.source_completion;
        terminal
            .record_runtime_residency(
                outcome.measurements.peak_live_source_blocks,
                outcome.measurements.peak_live_source_current_bytes,
                outcome.measurements.peak_live_source_capacity_bytes,
            )
            .map_err(|error| {
                ContinuumDensityTraversalError::Traversal(
                    SelectedObservationTraversalError::Source(error),
                )
            })?;
        let DensityBlockKernelCompletion { consumer, density } = outcome.kernel_completion;
        let (selected, completion) = consumer.complete(terminal).map_err(|error| {
            ContinuumDensityTraversalError::Traversal(match error {
                SelectedObservationTraversalError::Binding(error) => {
                    SelectedObservationTraversalError::Binding(error)
                }
                SelectedObservationTraversalError::Source(error) => {
                    SelectedObservationTraversalError::Source(error)
                }
                SelectedObservationTraversalError::Inspection(error) => {
                    SelectedObservationTraversalError::Inspection(error)
                }
                SelectedObservationTraversalError::Consumer(error) => match error {},
                SelectedObservationTraversalError::TraversalIdentityExhausted => {
                    SelectedObservationTraversalError::TraversalIdentityExhausted
                }
                SelectedObservationTraversalError::MeasurementOverflow => {
                    SelectedObservationTraversalError::MeasurementOverflow
                }
            })
        })?;
        self.latest_traversal_measurements = Some(*completion.measurements());
        self.latest_stream_measurements = Some(outcome.measurements);
        self.density = Some(density);
        self.retained_observation = Some(RetainedWeightingObservation {
            selected,
            attempt_id: context.attempt_id(),
            owner_node: context.node().id.clone(),
            lease_epoch: context.lease_epoch(),
        });
        Ok(completion)
    }

    pub(crate) fn traverse_initial_bounded_stream<E, F>(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        problem: &CompiledProblem,
        selected: Option<BoundSelectedObservation>,
        emit: F,
    ) -> Result<(), WeightingReplayError<E>>
    where
        E: Error + Send + 'static,
        F: FnMut(&ReconstructionWeightedBlock) -> Result<(), E> + Sync,
    {
        self.begin_measurement_scope();
        if !matches!(self.phase, WeightingExecutionPhase::Empty) {
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        }
        let plan = fragment
            .bounded_stream_plan(context)
            .map_err(WeightingReplayError::Evidence)?;
        let (selected, stream, binding) = match fragment.streaming {
            Some(WeightingStreamingMode::NaturalInitial) => {
                let selected =
                    selected.ok_or(WeightingReplayError::Evidence(WeightingEvidenceError))?;
                fragment
                    .authorize_source_observation(
                        context,
                        problem,
                        selected.residency_certificate(),
                    )
                    .map_err(WeightingReplayError::Evidence)?;
                let stream = begin_natural_weighting_stream(problem, fragment.plan)
                    .map_err(WeightingReplayError::Owner)?;
                (selected, stream, None)
            }
            Some(WeightingStreamingMode::DensityInitial) => {
                if selected.is_some() {
                    return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
                }
                let binding = fragment
                    .authorize_generation(context, problem)
                    .map_err(WeightingReplayError::Evidence)?;
                let retained = self
                    .retained_observation
                    .take()
                    .ok_or(WeightingReplayError::Evidence(WeightingEvidenceError))?;
                if retained.attempt_id != context.attempt_id()
                    || retained.lease_epoch != context.lease_epoch()
                {
                    return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
                }
                let density = self
                    .density
                    .take()
                    .ok_or(WeightingReplayError::Evidence(WeightingEvidenceError))?;
                let stream = density
                    .finish_into_stream(problem, fragment.plan)
                    .map_err(WeightingReplayError::Owner)?;
                (retained.selected, stream, Some(binding))
            }
            Some(WeightingStreamingMode::Reuse) | None => {
                return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
            }
        };
        let completed = match execute_weighting_block_stream(
            problem,
            selected,
            plan,
            stream,
            begin_continuum_stream(problem)?,
            emit,
        ) {
            Ok(completed) => completed,
            Err(failure) => {
                self.latest_stream_measurements = Some(*failure.measurements);
                return Err(*failure.error);
            }
        };
        let CompletedWeightingBlockStream {
            selected,
            owner_completion,
            weights: (state, summary),
            continuum,
            spectral_support_sample_count,
            measurements,
        } = completed;
        self.latest_traversal_measurements = Some(*owner_completion.measurements());
        self.latest_stream_measurements = Some(measurements);
        let continuum_completion = continuum
            .map(|transform| transform.complete(owner_completion.generation_id()))
            .transpose()
            .map_err(WeightingReplayError::Transform)?;
        let binding = binding.unwrap_or_else(|| WeightingGenerationBinding {
            attempt_id: context.attempt_id(),
            owner_node: context.node().id.clone(),
            lease_epoch: context.lease_epoch(),
            source_generation: owner_completion.generation_id(),
            source_sample_count: owner_completion.sample_count(),
        });
        if state.sample_count() != owner_completion.sample_count()
            || owner_completion.generation_id() != binding.source_generation
            || owner_completion.sample_count() != binding.source_sample_count
        {
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        }
        let artifact = FrozenWeightingArtifact {
            state: Arc::new(state),
            source_generation: owner_completion.generation_id(),
            source_sample_count: owner_completion.sample_count(),
            continuum_transform: continuum_completion,
            cross_plan_reservation: None,
        };
        let frozen = FrozenWeightingGeneration {
            artifact,
            binding: WeightingGenerationBinding {
                attempt_id: binding.attempt_id,
                owner_node: binding.owner_node.clone(),
                lease_epoch: binding.lease_epoch,
                source_generation: binding.source_generation,
                source_sample_count: binding.source_sample_count,
            },
        };
        self.retained_observation = Some(RetainedWeightingObservation {
            selected,
            attempt_id: context.attempt_id(),
            owner_node: fragment.source_read.clone(),
            lease_epoch: context.lease_epoch(),
        });
        self.phase = WeightingExecutionPhase::PendingReplay {
            frozen,
            pending: Box::new(PendingWeightingReplay {
                state: summary,
                owner_completion,
                binding,
                continuum_transform: continuum_completion,
                spectral_support_sample_count,
            }),
        };
        Ok(())
    }

    pub(crate) fn traverse_reuse_bounded_stream<E, F>(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        selected: BoundSelectedObservation,
        problem: &CompiledProblem,
        emit: F,
    ) -> Result<(), WeightingReplayError<E>>
    where
        E: Error + Send + 'static,
        F: FnMut(&ReconstructionWeightedBlock) -> Result<(), E> + Sync,
    {
        self.begin_measurement_scope();
        if !matches!(self.phase, WeightingExecutionPhase::Empty)
            || self.retained_observation.is_some()
            || self.density.is_some()
            || fragment.streaming != Some(WeightingStreamingMode::Reuse)
        {
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        }
        fragment
            .authorize_source_observation(context, problem, selected.residency_certificate())
            .map_err(WeightingReplayError::Evidence)?;
        let plan = fragment
            .bounded_stream_plan(context)
            .map_err(WeightingReplayError::Evidence)?;
        let artifact = self
            .imported
            .take()
            .ok_or(WeightingReplayError::Evidence(WeightingEvidenceError))?;
        let replay = artifact
            .state
            .begin_replay(problem, fragment.plan)
            .map_err(WeightingReplayError::Owner)?;
        let completed = match execute_weighting_block_stream(
            problem,
            selected,
            plan,
            replay,
            begin_continuum_stream(problem)?,
            emit,
        ) {
            Ok(completed) => completed,
            Err(failure) => {
                self.latest_stream_measurements = Some(*failure.measurements);
                return Err(*failure.error);
            }
        };
        let CompletedWeightingBlockStream {
            selected,
            owner_completion,
            weights: summary,
            continuum,
            spectral_support_sample_count,
            measurements,
        } = completed;
        self.latest_traversal_measurements = Some(*owner_completion.measurements());
        self.latest_stream_measurements = Some(measurements);
        let continuum_completion = continuum
            .map(|transform| transform.complete(owner_completion.generation_id()))
            .transpose()
            .map_err(WeightingReplayError::Transform)?;
        if owner_completion.generation_id() != artifact.source_generation
            || owner_completion.sample_count() != artifact.source_sample_count
            || continuum_completion != artifact.continuum_transform
        {
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        }
        let binding = WeightingGenerationBinding {
            attempt_id: context.attempt_id(),
            owner_node: context.node().id.clone(),
            lease_epoch: context.lease_epoch(),
            source_generation: owner_completion.generation_id(),
            source_sample_count: owner_completion.sample_count(),
        };
        self.retained_observation = Some(RetainedWeightingObservation {
            selected,
            attempt_id: context.attempt_id(),
            owner_node: fragment.source_read.clone(),
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
                owner_completion,
                binding,
                continuum_transform: continuum_completion,
                spectral_support_sample_count,
            }),
        };
        Ok(())
    }

    /// Drive the two owner traversals under generation-node authority.
    pub fn traverse_generation(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        problem: &CompiledProblem,
    ) -> Result<(), WeightingGenerationError> {
        if !matches!(self.phase, WeightingExecutionPhase::Empty) || fragment.streaming.is_some() {
            return Err(WeightingGenerationError::Evidence(WeightingEvidenceError));
        }
        let retained = self
            .retained_observation
            .as_mut()
            .ok_or(WeightingGenerationError::Evidence(WeightingEvidenceError))?;
        if retained.attempt_id != context.attempt_id()
            || retained.owner_node != fragment.source_read
            || retained.lease_epoch != context.lease_epoch()
        {
            return Err(WeightingGenerationError::Evidence(WeightingEvidenceError));
        }
        self.phase = WeightingExecutionPhase::PendingGeneration(Box::new(
            traverse_weighting_generation(context, fragment, &mut retained.selected, problem)?,
        ));
        Ok(())
    }

    /// Bind a successfully settled generation fence and retain its frozen W.
    pub fn complete_generation(
        &mut self,
        context: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, WeightingGenerationCompletionError> {
        let phase = std::mem::take(&mut self.phase);
        let WeightingExecutionPhase::PendingGeneration(pending) = phase else {
            self.phase = phase;
            return Err(WeightingGenerationCompletionError::Evidence(
                WeightingEvidenceError,
            ));
        };
        let (frozen, completion) = complete_weighting_generation(*pending, context)?;
        self.phase = WeightingExecutionPhase::Frozen(frozen);
        Ok(completion)
    }

    /// Drive the third exhaustive traversal while retaining the frozen W.
    pub fn traverse_replay<E>(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        problem: &CompiledProblem,
        emit: impl FnMut(&WeightedObservationBlock) -> Result<(), E>,
    ) -> Result<(), WeightingReplayError<E>>
    where
        E: Error + 'static,
    {
        if fragment.streaming.is_some() {
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        }
        let phase = std::mem::take(&mut self.phase);
        let WeightingExecutionPhase::Frozen(frozen) = phase else {
            self.phase = phase;
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        };
        let Some(retained) = self.retained_observation.as_mut() else {
            self.phase = WeightingExecutionPhase::Frozen(frozen);
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        };
        if retained.attempt_id != context.attempt_id()
            || retained.owner_node != fragment.source_read
            || retained.lease_epoch != context.lease_epoch()
        {
            self.phase = WeightingExecutionPhase::Frozen(frozen);
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        }
        match frozen.replay(context, fragment, &mut retained.selected, problem, emit) {
            Ok(pending) => {
                self.phase = WeightingExecutionPhase::PendingReplay {
                    frozen,
                    pending: Box::new(pending),
                };
                Ok(())
            }
            Err(error) => {
                self.phase = WeightingExecutionPhase::Frozen(frozen);
                Err(error)
            }
        }
    }

    /// Bind a successfully settled replay fence and retain its terminal proof.
    pub fn complete_replay(
        &mut self,
        context: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, WeightingReplayCompletionError> {
        let phase = std::mem::take(&mut self.phase);
        let WeightingExecutionPhase::PendingReplay { frozen, pending } = phase else {
            self.phase = phase;
            return Err(WeightingReplayCompletionError::Evidence(
                WeightingEvidenceError,
            ));
        };
        match (*pending).bind(context) {
            Ok((completion, predecessor)) => {
                self.phase = WeightingExecutionPhase::Replayed {
                    frozen,
                    completion: Box::new(completion),
                };
                Ok(predecessor)
            }
            Err(error) => {
                self.phase = WeightingExecutionPhase::Frozen(frozen);
                Err(error)
            }
        }
    }

    /// Return the terminal replay proof retained for reconciliation.
    #[must_use]
    pub const fn replay_completion(&self) -> Option<&WeightingReplayCompletion> {
        match &self.phase {
            WeightingExecutionPhase::Replayed { completion, .. } => Some(completion),
            WeightingExecutionPhase::Empty
            | WeightingExecutionPhase::PendingGeneration(_)
            | WeightingExecutionPhase::Frozen(_)
            | WeightingExecutionPhase::PendingReplay { .. } => None,
        }
    }

    /// Drop every retained phase only from the scheduler-issued Release node.
    ///
    /// A success-path release requires a completed replay. A draining release
    /// accepts any phase, including the pending values held between work and
    /// fence completion, after validating its attempt, lease, and allocation.
    pub fn release(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
    ) -> Result<(), WeightingEvidenceError> {
        fragment.authorize_release(context)?;
        if !self.matches_attempt(context, fragment)
            || !context.is_cleanup()
                && !matches!(self.phase, WeightingExecutionPhase::Replayed { .. })
        {
            return Err(WeightingEvidenceError);
        }
        self.phase = WeightingExecutionPhase::Empty;
        self.retained_observation = None;
        self.density = None;
        self.imported = None;
        Ok(())
    }

    /// Return whether the planned release has consumed all externally retained state.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        matches!(self.phase, WeightingExecutionPhase::Empty)
            && self.retained_observation.is_none()
            && self.density.is_none()
            && self.imported.is_none()
    }

    /// Return whether the actual read-locked selected-observation owner is live.
    #[must_use]
    pub const fn has_retained_observation(&self) -> bool {
        self.retained_observation.is_some()
    }

    fn matches_attempt(
        &self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
    ) -> bool {
        if self.retained_observation.as_ref().is_some_and(|retained| {
            retained.attempt_id != context.attempt_id()
                || retained.owner_node != fragment.source_read
                || retained.lease_epoch != context.lease_epoch()
        }) {
            return false;
        }
        let valid_binding = |binding: &WeightingGenerationBinding, owner: &WorkNodeId| {
            binding.attempt_id == context.attempt_id()
                && binding.lease_epoch == context.lease_epoch()
                && &binding.owner_node == owner
        };
        let (generation_owner, replay_owner) = match fragment.streaming {
            Some(_) => (fragment.streaming_node(), fragment.streaming_node()),
            None => (fragment.generation_node(), fragment.replay_node()),
        };
        match &self.phase {
            WeightingExecutionPhase::Empty => context.is_cleanup(),
            WeightingExecutionPhase::PendingGeneration(pending) => {
                valid_binding(&pending.binding, fragment.generation_node())
                    && pending.state.commitment_id() == fragment.plan.commitment_id()
            }
            WeightingExecutionPhase::Frozen(frozen) => {
                valid_binding(&frozen.binding, generation_owner)
                    && frozen.artifact.state.commitment_id() == fragment.plan.commitment_id()
            }
            WeightingExecutionPhase::PendingReplay { frozen, pending } => {
                valid_binding(&frozen.binding, generation_owner)
                    && valid_binding(&pending.binding, replay_owner)
                    && frozen.artifact.state.commitment_id() == fragment.plan.commitment_id()
                    && pending.state.weighting_generation() == frozen.artifact.state.generation_id()
            }
            WeightingExecutionPhase::Replayed { frozen, completion } => {
                valid_binding(&frozen.binding, generation_owner)
                    && valid_binding(&completion.binding, replay_owner)
                    && frozen.artifact.state.commitment_id() == fragment.plan.commitment_id()
                    && completion.state.weighting_generation()
                        == frozen.artifact.state.generation_id()
            }
        }
    }
}

#[derive(Clone, Debug)]
struct WeightingPlanIds {
    generation_node: WorkNodeId,
    replay_node: WorkNodeId,
    release_node: WorkNodeId,
    frozen_allocation: AllocationId,
    partial_allocation: AllocationId,
    reduction_allocation: AllocationId,
    weighted_block_allocation: AllocationId,
    continuum_row_allocation: AllocationId,
    frozen_slot: PhysicalSlotId,
    partial_slot: PhysicalSlotId,
    reduction_slot: PhysicalSlotId,
    weighted_block_slot: PhysicalSlotId,
    continuum_row_slot: PhysicalSlotId,
}

impl WeightingPlanIds {
    fn new(plan: &WeightingPlan, pass: SpectralPassIdentity) -> Self {
        let suffix = format!("{}-{}", pass.suffix(), plan.commitment_id());
        Self {
            generation_node: WorkNodeId::new(format!("weighting-generation-{suffix}")),
            replay_node: WorkNodeId::new(format!("weighting-replay-{suffix}")),
            release_node: WorkNodeId::new(format!("weighting-release-{suffix}")),
            frozen_allocation: AllocationId::new(format!("weighting-frozen-{suffix}")),
            partial_allocation: AllocationId::new(format!(
                "weighting-shared-density-accumulator-{suffix}"
            )),
            reduction_allocation: AllocationId::new(format!(
                "weighting-sum-weight-accumulator-{suffix}"
            )),
            weighted_block_allocation: AllocationId::new(format!(
                "weighting-weighted-block-{suffix}"
            )),
            continuum_row_allocation: AllocationId::new(format!(
                "continuum-transform-row-{suffix}"
            )),
            frozen_slot: PhysicalSlotId::new(format!("weighting-frozen-slot-{suffix}")),
            partial_slot: PhysicalSlotId::new(format!(
                "weighting-shared-density-accumulator-slot-{suffix}"
            )),
            reduction_slot: PhysicalSlotId::new(format!(
                "weighting-sum-weight-accumulator-slot-{suffix}"
            )),
            weighted_block_slot: PhysicalSlotId::new(format!(
                "weighting-weighted-block-slot-{suffix}"
            )),
            continuum_row_slot: PhysicalSlotId::new(format!(
                "continuum-transform-row-slot-{suffix}"
            )),
        }
    }
}

#[derive(Clone)]
struct SourceTraversalContract {
    traversal_claims: Vec<ResourceClaim>,
    retained_claims: Vec<ResourceClaim>,
    release_buffer_claims: Vec<ResourceClaim>,
    allocations: Vec<AllocationUse>,
    allocation_ids: BTreeSet<AllocationId>,
    retained_allocations: BTreeSet<AllocationId>,
}

impl SourceTraversalContract {
    fn from_source(
        base: &PhysicalWorkBinding,
        source: &WorkNode,
        residency: &SelectedObservationResidencyCertificate,
        selected_content_allocations: &BTreeSet<AllocationId>,
        queue: &LeaseResource,
        release: &WorkNodeId,
    ) -> Result<Self, WeightingPlanFragmentError> {
        if residency.aggregate_resident_bytes() == 0
            || residency.peak_live_blocks() == 0
            || residency.maximum_pointing_polynomial_terms() == 0
        {
            return Err(WeightingPlanFragmentError::InvalidSourceAuthority {
                node: source.id.clone(),
                reason: "selected-content budget is empty",
            });
        }
        if !source
            .claims
            .iter()
            .any(|claim| matches!(claim.resource, LeaseResource::Workers) && claim.amount > 0)
        {
            return Err(WeightingPlanFragmentError::InvalidSourceAuthority {
                node: source.id.clone(),
                reason: "selected traversal has no worker claim",
            });
        }
        let required_blocks = u64::try_from(residency.peak_live_blocks())
            .map_err(|_| WeightingPlanFragmentError::ResidencyOverflow)?;
        let mut queue_claims = source
            .claims
            .iter()
            .filter(|claim| &claim.resource == queue);
        let queue_claim_covers = queue_claims
            .next()
            .is_some_and(|claim| claim.amount == required_blocks)
            && queue_claims.next().is_none();
        if !is_selected_content_queue(queue)
            || !queue_claim_covers
            || !queue_demand_covers(
                base.execution_dag().resource_alternative(),
                queue,
                required_blocks,
            )
        {
            return Err(WeightingPlanFragmentError::InvalidSourceAuthority {
                node: source.id.clone(),
                reason: "selected traversal lacks its exact planned queue identity and capacity",
            });
        }
        let expected_bytes = u64::try_from(residency.aggregate_resident_bytes())
            .map_err(|_| WeightingPlanFragmentError::ResidencyOverflow)?;
        let claimed_bytes = source.claims.iter().try_fold(0_u64, |total, claim| {
            if claim.resource == LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead) {
                total.checked_add(claim.amount)
            } else {
                Some(total)
            }
        });
        let retained_allocations = source
            .allocations
            .iter()
            .filter(|usage| selected_content_allocations.contains(&usage.allocation))
            .map(|usage| usage.allocation.clone())
            .collect::<BTreeSet<_>>();
        let allocated_bytes = retained_allocations.iter().try_fold(0_u64, |total, id| {
            let allocation = &base.execution_dag().logical_allocations()[id];
            if allocation.purpose == AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead) {
                total.checked_add(allocation.bytes)
            } else {
                None
            }
        });
        if selected_content_allocations.is_empty()
            || &retained_allocations != selected_content_allocations
            || claimed_bytes != Some(expected_bytes)
            || allocated_bytes != Some(expected_bytes)
        {
            return Err(WeightingPlanFragmentError::InvalidSourceAuthority {
                node: source.id.clone(),
                reason: "selected-content budget does not match its exact retained read-buffer claim and allocations",
            });
        }

        let io_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
        let (retained_claims, traversal_claims) = source
            .claims
            .iter()
            .cloned()
            .partition::<Vec<_>, _>(|claim| {
                matches!(
                    claim.resource,
                    LeaseResource::MeasurementSetLock { .. } | LeaseResource::FileDescriptors
                )
            });
        let retained_claims = retained_claims
            .into_iter()
            .map(|mut claim| {
                claim.lifetime = ClaimLifetime::retained_until(release.clone());
                claim
            })
            .collect();
        let traversal_claims = traversal_claims
            .into_iter()
            .map(|mut claim| {
                claim.lifetime = if matches!(
                    claim.resource,
                    LeaseResource::Workers
                        | LeaseResource::RuntimeOverhead(crate::RuntimeOverheadKind::ThreadStack)
                ) {
                    ClaimLifetime::Work
                } else {
                    io_lifetime.clone()
                };
                claim
            })
            .collect();
        let release_buffer_claims = source
            .claims
            .iter()
            .filter(|claim| {
                claim.resource == LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
            })
            .cloned()
            .map(|mut claim| {
                claim.lifetime = ClaimLifetime::Work;
                claim
            })
            .collect();
        let allocations = source
            .allocations
            .iter()
            .map(|usage| allocation_use(&usage.allocation, io_lifetime.clone()))
            .collect::<Vec<_>>();
        let allocation_ids = allocations
            .iter()
            .map(|usage| usage.allocation.clone())
            .collect();
        Ok(Self {
            traversal_claims,
            retained_claims,
            release_buffer_claims,
            allocations,
            allocation_ids,
            retained_allocations,
        })
    }
}

struct AllocationSpec {
    allocation: AllocationId,
    slot: PhysicalSlotId,
    bytes: u64,
    compatibility: SlotCompatibility,
    acquire_at: WorkNodeId,
    release_after: BTreeSet<WorkDependency>,
}

impl AllocationSpec {
    fn new(
        allocation: AllocationId,
        slot: PhysicalSlotId,
        bytes: usize,
        layout: &str,
        acquire_at: WorkNodeId,
        release_after: BTreeSet<WorkDependency>,
    ) -> Result<Self, WeightingPlanFragmentError> {
        Ok(Self {
            allocation,
            slot,
            bytes: u64::try_from(bytes)
                .map_err(|_| WeightingPlanFragmentError::ResidencyOverflow)?,
            compatibility: SlotCompatibility {
                memory_domain: CapacityDomainId::new("host-memory"),
                views: BTreeSet::from([CapacityViewId::new("host-memory")]),
                alignment_bytes: align_of::<usize>() as u64,
                storage_mode: StorageMode::Host,
                layout: AllocationLayout::new(layout),
                initialization: InitializationPolicy::OverwriteBeforeRead,
                access: AllocationAccess::ReadWrite,
            },
            acquire_at,
            release_after,
        })
    }

    fn memory_demand(&self) -> MemoryDemand {
        MemoryDemand {
            allocation_id: self.allocation.as_str().to_string(),
            hard_bytes: self.bytes,
            preferred_bytes: self.bytes,
            views: vec![CapacityViewId::new("host-memory")],
        }
    }

    fn logical_allocation(&self) -> LogicalAllocation {
        LogicalAllocation {
            id: self.allocation.clone(),
            bytes: self.bytes,
            purpose: AllocationPurpose::Data,
            compatibility: self.compatibility.clone(),
            physical_slot: self.slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: self.acquire_at.clone(),
                release_after: self.release_after.clone(),
            },
        }
    }

    fn physical_slot(&self) -> PhysicalSlot {
        PhysicalSlot {
            id: self.slot.clone(),
            lease_resource: LeaseResource::Memory {
                allocation_id: self.allocation.as_str().to_string(),
            },
            capacity_bytes: self.bytes,
            compatibility: self.compatibility.clone(),
        }
    }

    fn matches_capability(
        &self,
        capability: &crate::WorkAllocationCapability,
        lifetime: &ClaimLifetime,
    ) -> bool {
        capability.allocation() == &self.allocation
            && capability.physical_slot() == &self.slot
            && capability.capacity_bytes() == self.bytes
            && capability.lifetime() == lifetime
    }
}

enum WeightingWorkContract<'a> {
    SelectedTraversal {
        problem: &'a CompiledProblem,
        residency: &'a SelectedObservationResidencyCertificate,
        queue: &'a LeaseResource,
        source_allocations: &'a BTreeSet<AllocationId>,
    },
    Release,
}

fn validate_work_authority(
    context: WorkExecutionContext<'_>,
    expected_node: &WorkNodeId,
    expected_allocations: &[&AllocationSpec],
    contract: WeightingWorkContract<'_>,
) -> Result<(), WeightingEvidenceError> {
    let (expected_kind, expected_domain, problem, lifetime, selected_content_budget) =
        match contract {
            WeightingWorkContract::SelectedTraversal {
                problem,
                residency,
                queue,
                source_allocations,
            } => (
                WorkKind::ObservationRead,
                WorkDomain::Io,
                Some(problem),
                ClaimLifetime::through_fence(FenceKind::Io),
                Some((residency, queue, source_allocations)),
            ),
            WeightingWorkContract::Release => (
                WorkKind::Release,
                WorkDomain::Cpu,
                None,
                ClaimLifetime::Work,
                None,
            ),
        };
    let kind_matches = if expected_kind == WorkKind::ObservationRead {
        context.node().kind.reads_observation()
    } else {
        context.node().kind == expected_kind
    };
    if &context.node().id != expected_node
        || !kind_matches
        || context.node().domain != expected_domain
        || context.resources().len() != context.node().claims.len()
        || context.allocations().len() != context.node().allocations.len()
        || problem.is_some_and(|problem| {
            context.compiled().problem_id() != problem.problem_id()
                || context
                    .selected_observation()
                    .is_none_or(|selected| selected.problem_id() != problem.problem_id())
        })
        || context.node().claims.iter().any(|claim| {
            !context.resources().iter().any(|capability| {
                capability.resource() == &claim.resource
                    && capability.amount() == claim.amount
                    && capability.lifetime() == &claim.lifetime
            })
        })
        || context.node().allocations.iter().any(|usage| {
            !context.allocations().iter().any(|capability| {
                capability.allocation() == &usage.allocation
                    && capability.lifetime() == &usage.lifetime
            })
        })
        || expected_allocations.iter().any(|spec| {
            !context
                .allocations()
                .iter()
                .any(|capability| spec.matches_capability(capability, &lifetime))
        })
    {
        return Err(WeightingEvidenceError);
    }
    if let Some((residency, queue, source_allocations)) = selected_content_budget {
        if !residency
            .matches_problem(problem.expect("selected traversal always carries a compiled problem"))
        {
            return Err(WeightingEvidenceError);
        }
        let required_bytes = u64::try_from(residency.aggregate_resident_bytes())
            .map_err(|_| WeightingEvidenceError)?;
        let required_blocks =
            u64::try_from(residency.peak_live_blocks()).map_err(|_| WeightingEvidenceError)?;
        let source_capacity = context
            .allocations()
            .iter()
            .filter(|capability| source_allocations.contains(capability.allocation()))
            .try_fold(0_u64, |total, capability| {
                total.checked_add(capability.capacity_bytes())
            })
            .ok_or(WeightingEvidenceError)?;
        let read_buffer_bytes = context
            .resources()
            .iter()
            .filter(|capability| {
                capability.resource() == &LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
            })
            .try_fold(0_u64, |total, capability| {
                total.checked_add(capability.amount())
            })
            .ok_or(WeightingEvidenceError)?;
        let mut queue_capabilities = context
            .resources()
            .iter()
            .filter(|capability| capability.resource() == queue);
        let queue_capability_covers = queue_capabilities
            .next()
            .is_some_and(|capability| capability.amount() == required_blocks)
            && queue_capabilities.next().is_none();
        if source_allocations.is_empty()
            || context
                .allocations()
                .iter()
                .filter(|capability| source_allocations.contains(capability.allocation()))
                .count()
                != source_allocations.len()
            || read_buffer_bytes != required_bytes
            || source_capacity != required_bytes
            || !queue_capability_covers
            || !queue_demand_covers(context.resource_alternative(), queue, required_blocks)
            || !context.resources().iter().any(|capability| {
                capability.resource() == &LeaseResource::Workers && capability.amount() > 0
            })
        {
            return Err(WeightingEvidenceError);
        }
    }
    Ok(())
}

fn is_selected_content_queue(resource: &LeaseResource) -> bool {
    matches!(
        resource,
        LeaseResource::Queue { .. }
            | LeaseResource::StorageQueue { .. }
            | LeaseResource::TransferQueue { .. }
    )
}

fn queue_demand_covers(
    alternative: &crate::DemandAlternative,
    resource: &LeaseResource,
    required_slots: u64,
) -> bool {
    match resource {
        LeaseResource::Queue { demand_id } => {
            let mut demands = alternative
                .demand
                .queues
                .iter()
                .filter(|demand| &demand.demand_id == demand_id);
            demands
                .next()
                .is_some_and(|demand| demand.slots.hard() >= required_slots)
                && demands.next().is_none()
        }
        LeaseResource::StorageQueue { demand_id } => {
            let mut demands = alternative
                .demand
                .storage
                .iter()
                .filter(|demand| &demand.demand_id == demand_id);
            demands
                .next()
                .is_some_and(|demand| demand.queue_slots.hard() >= required_slots)
                && demands.next().is_none()
        }
        LeaseResource::TransferQueue { demand_id } => {
            let mut demands = alternative
                .demand
                .transfers
                .iter()
                .filter(|demand| &demand.demand_id == demand_id);
            demands
                .next()
                .is_some_and(|demand| demand.queue_slots.hard() >= required_slots)
                && demands.next().is_none()
        }
        _ => false,
    }
}

fn allocation_use(allocation: &AllocationId, lifetime: ClaimLifetime) -> AllocationUse {
    AllocationUse {
        allocation: allocation.clone(),
        lifetime,
    }
}

fn terminal_events(node: &WorkNode) -> BTreeSet<WorkDependency> {
    if node.fences.is_empty() {
        BTreeSet::from([WorkDependency::Work(node.id.clone())])
    } else {
        node.fences
            .iter()
            .map(|kind| WorkDependency::Fence(FenceId::new(node.id.clone(), *kind)))
            .collect()
    }
}

fn checked_sum(
    values: impl IntoIterator<Item = usize>,
) -> Result<usize, WeightingPlanFragmentError> {
    values.into_iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(value)
            .ok_or(WeightingPlanFragmentError::ResidencyOverflow)
    })
}

fn scaled_prediction(
    source: &StagePrediction,
    node: WorkNodeId,
    passes: u64,
) -> Result<StagePrediction, WeightingPlanFragmentError> {
    let io = source
        .io()
        .iter()
        .map(|prediction| {
            let bytes = prediction
                .bytes()
                .checked_mul(passes)
                .ok_or(WeightingPlanFragmentError::PredictionOverflow)?;
            let operations = prediction
                .operations()
                .checked_mul(passes)
                .ok_or(WeightingPlanFragmentError::PredictionOverflow)?;
            Ok(crate::IoPrediction::new(
                prediction.kind(),
                bytes,
                operations,
            ))
        })
        .collect::<Result<Vec<_>, WeightingPlanFragmentError>>()?;
    Ok(StagePrediction::new(
        node,
        source
            .elapsed_nanos()
            .checked_mul(passes)
            .ok_or(WeightingPlanFragmentError::PredictionOverflow)?,
    )
    .with_io(io))
}

/// Failure to compose a complete production weighting lifecycle.
#[derive(Debug)]
pub enum WeightingPlanFragmentError {
    /// A required transaction node is absent from the base physical work.
    MissingNode(WorkNodeId),
    /// The named predecessor is not a typed selected-observation read.
    InvalidSourceKind(WorkNodeId),
    /// The selected-observation source omits required bounded traversal authority.
    InvalidSourceAuthority {
        /// Source node with the incomplete resource contract.
        node: WorkNodeId,
        /// Stable contract defect.
        reason: &'static str,
    },
    /// A weighting byte projection exceeded the host integer domain.
    ResidencyOverflow,
    /// A plan prediction could not represent all selected traversal passes.
    PredictionOverflow,
    /// The composed execution DAG is invalid.
    Execution(ExecutionError),
    /// The complete physical binding is inconsistent.
    Binding(PhysicalWorkBindingError),
}

impl fmt::Display for WeightingPlanFragmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode(node) => write!(
                formatter,
                "weighting plan fragment requires missing node {}",
                node.as_str()
            ),
            Self::InvalidSourceKind(node) => write!(
                formatter,
                "weighting predecessor {} is not an ObservationRead node",
                node.as_str()
            ),
            Self::InvalidSourceAuthority { node, reason } => write!(
                formatter,
                "weighting predecessor {} has incomplete source authority: {reason}",
                node.as_str()
            ),
            Self::ResidencyOverflow => {
                formatter.write_str("weighting fragment residency overflowed")
            }
            Self::PredictionOverflow => {
                formatter.write_str("weighting fragment prediction overflowed")
            }
            Self::Execution(error) => error.fmt(formatter),
            Self::Binding(error) => error.fmt(formatter),
        }
    }
}

impl Error for WeightingPlanFragmentError {}

impl From<ExecutionError> for WeightingPlanFragmentError {
    fn from(error: ExecutionError) -> Self {
        Self::Execution(error)
    }
}

impl From<PhysicalWorkBindingError> for WeightingPlanFragmentError {
    fn from(error: PhysicalWorkBindingError) -> Self {
        Self::Binding(error)
    }
}

/// One runtime-authorized output contribution carrying the frozen W generation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightedSpectralValue {
    value: ReconstructionWeightedSpectralValue,
    generation: WeightingGenerationId,
}

impl WeightedSpectralValue {
    /// Return the storage-owner-reported output contribution.
    #[must_use]
    pub const fn contribution(self) -> SelectedSpectralContribution {
        self.value.contribution()
    }

    /// Return the final non-negative diagonal metric value.
    #[must_use]
    pub const fn imaging_weight(self) -> f64 {
        self.value.imaging_weight()
    }

    /// Return the sole frozen generation that supplied W.
    #[must_use]
    pub const fn weighting_generation(self) -> WeightingGenerationId {
        self.generation
    }
}

/// One runtime-authorized weighted sample carrying output-specific W values.
#[derive(Debug, Clone, Copy)]
pub struct WeightedObservationSample<'a> {
    sample: &'a ReconstructionWeightedSample,
    generation: WeightingGenerationId,
}

impl WeightedObservationSample<'_> {
    /// Return the selected sample validated by T17 traversal.
    #[must_use]
    pub const fn selected(&self) -> &ReconstructionSelectedSample {
        self.sample.selected()
    }

    /// Iterate over output contributions and their final W values.
    pub fn spectral_values(&self) -> impl Iterator<Item = WeightedSpectralValue> + '_ {
        self.sample
            .spectral_values()
            .map(|value| WeightedSpectralValue {
                value,
                generation: self.generation,
            })
    }

    /// Return the sole frozen generation that supplied W.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.generation
    }
}

/// One borrowed-consumption replay block branded only by runtime-held T17 evidence.
#[derive(Debug)]
pub struct WeightedObservationBlock {
    generation: WeightingGenerationId,
    block: ReconstructionWeightedBlock,
}

impl WeightedObservationBlock {
    fn authorize(generation: WeightingGenerationId, block: ReconstructionWeightedBlock) -> Self {
        Self { generation, block }
    }

    /// Return the frozen W generation authorizing every sample.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.generation
    }

    /// Return the zero-based replay block sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.block.sequence()
    }

    /// Iterate over weighted samples for synchronous bounded consumption.
    pub fn samples(&self) -> impl Iterator<Item = WeightedObservationSample<'_>> {
        self.block
            .samples()
            .iter()
            .map(|sample| WeightedObservationSample {
                sample,
                generation: self.generation,
            })
    }

    pub(crate) const fn reconstruction_block(&self) -> &ReconstructionWeightedBlock {
        &self.block
    }
}

/// A frozen W whose reconstruction state is backed by two opaque T17 completions.
#[derive(Debug)]
struct FrozenWeightingGeneration {
    artifact: FrozenWeightingArtifact,
    binding: WeightingGenerationBinding,
}

/// Immutable weighting values reusable by later model-dependent major passes.
#[derive(Clone, Debug)]
pub struct FrozenWeightingArtifact {
    state: Arc<WeightingAlgorithmState>,
    source_generation: SelectedObservationGenerationId,
    source_sample_count: u64,
    continuum_transform: Option<ContinuumTransformCompletion>,
    cross_plan_reservation: Option<Arc<FrozenWeightingReservation>>,
}

/// Resource Authority lease retaining frozen weighting bytes between major plans.
///
/// The ordinary per-plan allocation still accounts each plan's direct use. This
/// longer lease closes the interval between those plans and is shared by every
/// immutable artifact clone until the final owner drops it.
#[derive(Debug)]
pub struct FrozenWeightingReservation {
    _lease: ResourceLease,
    bytes: u64,
}

impl FrozenWeightingReservation {
    /// Reserve the exact frozen density, robust-factor, and sum-weight state.
    pub fn acquire(
        authority: &ResourceAuthority,
        policy: ResourcePolicy,
        residency: WeightingResidency,
    ) -> Result<Self, ResourceError> {
        let bytes = [
            residency.density_grid_bytes(),
            residency.robust_factor_bytes(),
            residency.sum_weight_bytes(),
        ]
        .into_iter()
        .try_fold(0_u64, |total, bytes| {
            total
                .checked_add(
                    u64::try_from(bytes)
                        .map_err(|_| ResourceError::Overflow("frozen weighting residency"))?,
                )
                .ok_or(ResourceError::Overflow("frozen weighting residency"))
        })?;
        let memory = MemoryDemand {
            allocation_id: "cross-plan-frozen-weighting".to_string(),
            hard_bytes: bytes,
            preferred_bytes: bytes,
            views: vec![CapacityViewId::new("host-memory")],
        };
        let alternative = DemandAlternative {
            id: AlternativeId::new("cross-plan-frozen-weighting"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: vec![memory],
                workers: CountDemand::zero(),
                overhead: RuntimeOverheadDemand::zero(),
                storage: vec![],
                rates: vec![],
                caches: CacheDemand::zero(),
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: vec![],
                transfers: vec![],
                accelerators: vec![],
                io_buffers: IoBufferDemand::zero(),
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 0,
                maximum_workers: 0,
                maximum_batch_size: 1,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::MajorCycle]),
        };
        let lease = authority.acquire(
            policy,
            DemandAlternatives {
                required_capabilities: BTreeSet::new(),
                alternatives: vec![alternative],
            },
        )?;
        Ok(Self {
            _lease: lease,
            bytes,
        })
    }

    /// Return the resident-byte ceiling held between plans.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl FrozenWeightingArtifact {
    pub(crate) fn with_cross_plan_reservation(
        mut self,
        reservation: Arc<FrozenWeightingReservation>,
    ) -> Self {
        self.cross_plan_reservation = Some(reservation);
        self
    }

    pub(crate) fn has_cross_plan_reservation(&self) -> bool {
        self.cross_plan_reservation.is_some()
    }
}

#[derive(Debug)]
struct WeightingGenerationBinding {
    attempt_id: ExecutionAttemptId,
    owner_node: WorkNodeId,
    lease_epoch: u64,
    source_generation: SelectedObservationGenerationId,
    source_sample_count: u64,
}

impl FrozenWeightingGeneration {
    fn generation_id(&self) -> WeightingGenerationId {
        self.artifact.state.generation_id()
    }

    fn replay<E>(
        &self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        selected: &mut BoundSelectedObservation,
        problem: &CompiledProblem,
        mut emit: impl FnMut(&WeightedObservationBlock) -> Result<(), E>,
    ) -> Result<PendingWeightingReplay, WeightingReplayError<E>>
    where
        E: Error + 'static,
    {
        let replay_binding = fragment
            .authorize_replay(context, self, problem)
            .map_err(WeightingReplayError::Evidence)?;
        let predecessor = context
            .predecessor_observation_completion(fragment.generation_node())
            .ok_or(WeightingReplayError::Evidence(WeightingEvidenceError))?;
        if !selected.can_resume_after(predecessor.owner_completion()) {
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        }
        let mut phase = self
            .artifact
            .state
            .begin_replay(problem, fragment.plan)
            .map_err(WeightingReplayError::Owner)?;
        let mut spectral_contributions = SpectralContributionCache::new();
        let owner_completion = selected
            .traverse(problem, |reported| {
                let contributions = spectral_contributions
                    .compile(problem, &reported)
                    .map_err(ReplayCallbackError::Owner)?;
                if let Some(block) = phase
                    .consume(problem, reported.selected(), contributions)
                    .map_err(ReplayCallbackError::Owner)?
                {
                    let block = WeightedObservationBlock::authorize(self.generation_id(), block);
                    emit(&block).map_err(ReplayCallbackError::Consumer)?;
                }
                Ok(())
            })
            .map_err(WeightingReplayError::Traversal)?;
        validate_replay_completion(
            self.artifact.source_generation,
            self.artifact.source_sample_count,
            predecessor.owner_completion(),
            &owner_completion,
            &self.artifact.state,
        )
        .map_err(WeightingReplayError::Evidence)?;
        let (final_block, state) = phase.finish().map_err(WeightingReplayError::Owner)?;
        if let Some(block) = final_block {
            let block = WeightedObservationBlock::authorize(self.generation_id(), block);
            emit(&block).map_err(WeightingReplayError::Consumer)?;
        }
        let spectral_support_sample_count = state.sample_count();
        Ok(PendingWeightingReplay {
            state,
            owner_completion,
            binding: replay_binding,
            continuum_transform: None,
            spectral_support_sample_count,
        })
    }
}

/// Unbranded result of two exhaustive owner traversals.
#[derive(Debug)]
struct PendingWeightingGeneration {
    state: WeightingAlgorithmState,
    density_completion: SelectedObservationCompletion,
    sum_weight_completion: SelectedObservationCompletion,
    binding: WeightingGenerationBinding,
}

fn traverse_weighting_generation(
    context: WorkExecutionContext<'_>,
    fragment: &WeightingPlanFragment<'_>,
    selected: &mut BoundSelectedObservation,
    problem: &CompiledProblem,
) -> Result<PendingWeightingGeneration, WeightingGenerationError> {
    let binding = fragment
        .authorize_generation(context, problem)
        .map_err(WeightingGenerationError::Evidence)?;
    let source_completion = context
        .predecessor_observation_completion(&fragment.source_read)
        .ok_or(WeightingGenerationError::Evidence(WeightingEvidenceError))?
        .owner_completion();
    if !selected.can_resume_after(source_completion) {
        return Err(WeightingGenerationError::Evidence(WeightingEvidenceError));
    }
    let mut density = begin_weighting_generation(problem, fragment.plan)
        .map_err(WeightingGenerationError::Owner)?;
    let mut spectral_contributions = SpectralContributionCache::new();
    let density_completion = selected
        .traverse(problem, |reported| {
            let contributions = spectral_contributions.compile(problem, &reported)?;
            density.consume(problem, reported.selected(), contributions)
        })
        .map_err(WeightingGenerationError::DensityTraversal)?;
    let sum_weight = density
        .finish(problem)
        .map_err(WeightingGenerationError::Owner)?;
    let mut sum_weight = sum_weight;
    let mut spectral_contributions = SpectralContributionCache::new();
    let sum_weight_completion = selected
        .traverse(problem, |reported| {
            let contributions = spectral_contributions.compile(problem, &reported)?;
            sum_weight.consume(problem, reported.selected(), contributions)
        })
        .map_err(WeightingGenerationError::SumWeightTraversal)?;
    let state = sum_weight
        .finish()
        .map_err(WeightingGenerationError::Owner)?;
    validate_generation_completions(&density_completion, &sum_weight_completion)
        .map_err(WeightingGenerationError::Evidence)?;
    if state.sample_count() != density_completion.sample_count() {
        return Err(WeightingGenerationError::Evidence(WeightingEvidenceError));
    }
    if !source_completion.precedes(&density_completion)
        || density_completion.generation_id() != binding.source_generation
        || density_completion.sample_count() != binding.source_sample_count
    {
        return Err(WeightingGenerationError::Evidence(WeightingEvidenceError));
    }
    Ok(PendingWeightingGeneration {
        state,
        density_completion,
        sum_weight_completion,
        binding,
    })
}

fn complete_weighting_generation(
    pending: PendingWeightingGeneration,
    context: ObservationReadCompletionContext,
) -> Result<
    (FrozenWeightingGeneration, AttemptBoundObservationCompletion),
    WeightingGenerationCompletionError,
> {
    validate_generation_completions(&pending.density_completion, &pending.sum_weight_completion)
        .map_err(WeightingGenerationCompletionError::Evidence)?;
    if pending.state.sample_count() != pending.density_completion.sample_count() {
        return Err(WeightingGenerationCompletionError::Evidence(
            WeightingEvidenceError,
        ));
    }
    if context.attempt_id() != pending.binding.attempt_id
        || context.owner_node() != &pending.binding.owner_node
        || context.lease_epoch() != pending.binding.lease_epoch
    {
        return Err(WeightingGenerationCompletionError::Evidence(
            WeightingEvidenceError,
        ));
    }
    let binding = pending.binding;
    let predecessor = context
        .bind(pending.sum_weight_completion)
        .map_err(WeightingGenerationCompletionError::Binding)?;
    Ok((
        FrozenWeightingGeneration {
            artifact: FrozenWeightingArtifact {
                state: Arc::new(pending.state),
                source_generation: pending.density_completion.generation_id(),
                source_sample_count: pending.density_completion.sample_count(),
                continuum_transform: None,
                cross_plan_reservation: None,
            },
            binding,
        },
        predecessor,
    ))
}

fn validate_generation_completions(
    density: &SelectedObservationCompletion,
    sum_weight: &SelectedObservationCompletion,
) -> Result<(), WeightingEvidenceError> {
    if !density.precedes(sum_weight)
        || density.problem_id() != sum_weight.problem_id()
        || density.commitment_id() != sum_weight.commitment_id()
        || density.generation_id() != sum_weight.generation_id()
        || density.sample_count() != sum_weight.sample_count()
    {
        return Err(WeightingEvidenceError);
    }
    Ok(())
}

fn validate_replay_completion(
    source_generation: SelectedObservationGenerationId,
    source_sample_count: u64,
    prior: &SelectedObservationCompletion,
    replay: &SelectedObservationCompletion,
    state: &WeightingAlgorithmState,
) -> Result<(), WeightingEvidenceError> {
    if prior.generation_id() != source_generation
        || prior.sample_count() != source_sample_count
        || !prior.precedes(replay)
        || replay.problem_id() != prior.problem_id()
        || replay.commitment_id() != prior.commitment_id()
        || replay.generation_id() != prior.generation_id()
        || replay.sample_count() != prior.sample_count()
        || replay.sample_count() != state.sample_count()
    {
        return Err(WeightingEvidenceError);
    }
    Ok(())
}

/// Replay algorithm result awaiting scheduler-issued attempt authority.
#[derive(Debug)]
struct PendingWeightingReplay {
    state: WeightingReplaySummary,
    owner_completion: SelectedObservationCompletion,
    binding: WeightingGenerationBinding,
    continuum_transform: Option<ContinuumTransformCompletion>,
    spectral_support_sample_count: u64,
}

impl PendingWeightingReplay {
    fn bind(
        self,
        context: ObservationReadCompletionContext,
    ) -> Result<
        (WeightingReplayCompletion, AttemptBoundObservationCompletion),
        WeightingReplayCompletionError,
    > {
        if context.attempt_id() != self.binding.attempt_id
            || context.owner_node() != &self.binding.owner_node
            || context.lease_epoch() != self.binding.lease_epoch
        {
            return Err(WeightingReplayCompletionError::Evidence(
                WeightingEvidenceError,
            ));
        }
        let selected_generation = self.owner_completion.generation_id();
        let problem = self.owner_completion.problem_id();
        let sample_count = self.owner_completion.sample_count();
        let owner_completion = context
            .bind(self.owner_completion)
            .map_err(WeightingReplayCompletionError::Binding)?;
        Ok((
            WeightingReplayCompletion {
                state: self.state,
                problem,
                selected_generation,
                sample_count,
                binding: self.binding,
                continuum_transform: self.continuum_transform,
                spectral_support_sample_count: self.spectral_support_sample_count,
            },
            owner_completion,
        ))
    }
}

/// Distinct terminal proof of a weighted replay and its exhaustive T17 traversal.
#[derive(Debug)]
pub struct WeightingReplayCompletion {
    state: WeightingReplaySummary,
    problem: CompiledProblemId,
    selected_generation: SelectedObservationGenerationId,
    sample_count: u64,
    binding: WeightingGenerationBinding,
    continuum_transform: Option<ContinuumTransformCompletion>,
    spectral_support_sample_count: u64,
}

impl WeightingReplayCompletion {
    pub(crate) const fn reconstruction_summary(&self) -> &WeightingReplaySummary {
        &self.state
    }

    /// Return the exact Compiled Problem whose T17 traversal produced this replay.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the unique replay identity.
    #[must_use]
    pub const fn replay_id(&self) -> WeightingReplayId {
        self.state.replay_id()
    }

    /// Return the frozen W carried by every emitted block.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.state.weighting_generation()
    }

    /// Return the independently traversed T17 content generation.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.selected_generation
    }

    /// Return sequential continuum-transform evidence when the replay used it.
    #[must_use]
    pub const fn continuum_transform(&self) -> Option<ContinuumTransformCompletion> {
        self.continuum_transform
    }

    /// Return samples whose compiled spectral stencil reached weighting.
    #[must_use]
    pub const fn spectral_support_sample_count(&self) -> u64 {
        self.spectral_support_sample_count
    }

    /// Return exact emitted weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.state.coverage()
    }

    /// Return the exhaustive emitted sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return emitted block count.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.state.block_count()
    }

    /// Return this generation's unique replay sequence.
    #[must_use]
    pub const fn replay_sequence(&self) -> u64 {
        self.state.replay_sequence()
    }

    /// Return actual bounded replay residency.
    #[must_use]
    pub const fn residency(&self) -> WeightingResidency {
        self.state.residency()
    }

    /// Return the execution attempt that authorized this replay before traversal.
    #[must_use]
    pub const fn attempt_id(&self) -> ExecutionAttemptId {
        self.binding.attempt_id
    }

    /// Return the planned replay node whose settled I/O fence minted this completion.
    #[must_use]
    pub const fn owner_node(&self) -> &WorkNodeId {
        &self.binding.owner_node
    }

    /// Return the Resource Authority lease epoch held through replay completion.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.binding.lease_epoch
    }
}

/// Initial selected-observation authority or traversal failed before retention.
#[derive(Debug)]
pub enum WeightingSourceTraversalError<E> {
    /// The owner certificate did not match the scheduler's complete source contract.
    Evidence(WeightingEvidenceError),
    /// The storage owner failed while producing the first exhaustive traversal.
    Traversal(SelectedObservationTraversalError<E>),
}

impl<E: fmt::Display> fmt::Display for WeightingSourceTraversalError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evidence(error) => error.fmt(formatter),
            Self::Traversal(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for WeightingSourceTraversalError<E> {}

/// Two T17 generation traversals or reconstruction reduction failed.
#[derive(Debug)]
pub enum WeightingGenerationError {
    /// Density traversal failed before opaque completion.
    DensityTraversal(SelectedObservationTraversalError<WeightingError>),
    /// Sum-weight traversal failed before opaque completion.
    SumWeightTraversal(SelectedObservationTraversalError<WeightingError>),
    /// Reconstruction rejected a plan, sample, or reduction.
    Owner(WeightingError),
    /// Opaque T17 completions did not prove the same ordered retained access.
    Evidence(WeightingEvidenceError),
}

/// Failure while resolving channel roles or accumulating the density prepass.
#[derive(Debug)]
pub enum ContinuumDensityCallbackError {
    /// The compiled transform did not cover a selected channel.
    Transform(ContinuumTransformError),
    /// Reconstruction rejected the spectral stencil or density sample.
    Owner(WeightingError),
}

impl From<ContinuumTransformError> for ContinuumDensityCallbackError {
    fn from(error: ContinuumTransformError) -> Self {
        Self::Transform(error)
    }
}

impl fmt::Display for ContinuumDensityCallbackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transform(error) => error.fmt(formatter),
            Self::Owner(error) => error.fmt(formatter),
        }
    }
}

impl Error for ContinuumDensityCallbackError {}

/// Transform-aware density traversal failure.
#[derive(Debug)]
pub enum ContinuumDensityTraversalError {
    /// Resource or phase evidence did not authorize the traversal.
    Evidence(WeightingEvidenceError),
    /// Reconstruction rejected density initialization.
    Owner(WeightingError),
    /// The exhaustive storage traversal or role callback failed.
    Traversal(SelectedObservationTraversalError<ContinuumDensityCallbackError>),
    /// The shared bounded executor failed before owner completion.
    Runtime(WeightingStreamRuntimeError),
}

impl fmt::Display for ContinuumDensityTraversalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evidence(error) => error.fmt(formatter),
            Self::Owner(error) => error.fmt(formatter),
            Self::Traversal(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
        }
    }
}

impl Error for ContinuumDensityTraversalError {}

impl fmt::Display for WeightingGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DensityTraversal(error) => {
                write!(formatter, "weighting density traversal failed: {error}")
            }
            Self::SumWeightTraversal(error) => {
                write!(formatter, "weighting sum-weight traversal failed: {error}")
            }
            Self::Owner(error) => error.fmt(formatter),
            Self::Evidence(error) => error.fmt(formatter),
        }
    }
}

impl Error for WeightingGenerationError {}

/// Scheduler binding of an owner-traversed weighting generation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightingGenerationCompletionError {
    /// Traversal evidence did not describe two ordered passes over one retained source.
    Evidence(WeightingEvidenceError),
    /// The scheduler completion context belongs to another compiled observation.
    Binding(ObservationCompletionBindingError),
}

impl fmt::Display for WeightingGenerationCompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evidence(error) => error.fmt(formatter),
            Self::Binding(error) => error.fmt(formatter),
        }
    }
}

impl Error for WeightingGenerationCompletionError {}

/// Scheduler binding of an owner-traversed weighting replay failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightingReplayCompletionError {
    /// The settled node did not match the attempt authorized before traversal.
    Evidence(WeightingEvidenceError),
    /// The scheduler completion context belongs to another compiled observation.
    Binding(ObservationCompletionBindingError),
}

impl fmt::Display for WeightingReplayCompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evidence(error) => error.fmt(formatter),
            Self::Binding(error) => error.fmt(formatter),
        }
    }
}

impl Error for WeightingReplayCompletionError {}

/// Weighted replay traversal, reconstruction, or consumer failure.
#[derive(Debug)]
pub enum WeightingReplayError<E> {
    /// The exhaustive T17 traversal or an in-traversal callback failed.
    Traversal(SelectedObservationTraversalError<ReplayCallbackError<E>>),
    /// Reconstruction rejected the replay.
    Owner(WeightingError),
    /// Sequential continuum transformation rejected a row or its evidence.
    Transform(ContinuumTransformError),
    /// Opaque replay completion did not follow the frozen generation passes.
    Evidence(WeightingEvidenceError),
    /// The consumer rejected the terminal partial block.
    Consumer(E),
    /// The shared bounded executor failed before owner completion.
    Runtime(WeightingStreamRuntimeError),
}

impl<E: fmt::Display> fmt::Display for WeightingReplayError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Traversal(error) => error.fmt(formatter),
            Self::Owner(error) => error.fmt(formatter),
            Self::Transform(error) => error.fmt(formatter),
            Self::Evidence(error) => error.fmt(formatter),
            Self::Consumer(error) => write!(formatter, "weighted replay consumer failed: {error}"),
            Self::Runtime(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for WeightingReplayError<E> {}

/// Error raised inside the T17 replay callback.
#[derive(Debug)]
pub enum ReplayCallbackError<E> {
    /// Reconstruction rejected a validated sample.
    Owner(WeightingError),
    /// Sequential continuum transformation rejected a row.
    Transform(ContinuumTransformError),
    /// The downstream block consumer failed.
    Consumer(E),
}

impl<E: fmt::Display> fmt::Display for ReplayCallbackError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(error) => error.fmt(formatter),
            Self::Transform(error) => error.fmt(formatter),
            Self::Consumer(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for ReplayCallbackError<E> {}

/// Opaque traversal evidence did not bind the required ordered passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightingEvidenceError;

impl fmt::Display for WeightingEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("weighting phases do not bind ordered exhaustive traversals of one retained selected observation")
    }
}

impl Error for WeightingEvidenceError {}
