// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime ownership boundary for the first complete-data continuum operator.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt, io,
    mem::{align_of, size_of},
};

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, ModelDeltaTerm, ModelSample,
    NumericsContractId, ReconstructionBasis, SelectedObservationGenerationId, SpectralKernel,
    WeightingCommitmentId,
};
use casa_imaging_reconstruction::{
    FinalNormalState, MajorCyclePreparation, SpectralOperatorError, SpectralOperatorPrimitives,
    SpectralOperatorSpecification, SpectralPrimitiveCatalog, WeightingAlgorithmState,
    WeightingGenerationId, WeightingReplayCoverageId, WeightingReplayId,
    runtime_adapter::{
        CompleteDataOwnerResult, CompleteDataOwnerState,
        GRIDDED_NORMAL_MAXIMUM_PARTITIONS_PER_BLOCK, GRIDDED_NORMAL_OPERATOR_RECORD_BYTES,
        GriddedNormalOperatorApply, GriddedNormalOperatorBlockMeasurements,
        GriddedNormalOperatorCompiler, GriddedNormalOperatorProgram,
        GriddedNormalPredictionPartial, GriddedNormalPredictionWork, PreparedSpectralOperator,
        SpectralOperatorPass, SpectralOperatorWorkload, prepare_spectral_operator,
        spectral_operator_workload,
    },
};

use crate::bounded_stream::{
    BlockIdentity, BoundedStreamError, BoundedStreamMeasurements, BoundedStreamPlan,
    KernelPartition, PartitionedKernel, WorkIdentity, execute_bounded,
};
use crate::{
    AllocationAccess, AllocationId, AllocationLayout, AllocationLifetime, AllocationPurpose,
    AllocationUse, AlternativeId, CapacityDomainId, CapacityViewId, ClaimLifetime,
    ExecutionAttemptId, ExecutionDag, ExecutionDagSpecification, ExecutionError, FenceId,
    FenceKind, InitializationPolicy, LeaseResource, LogicalAllocation, MemoryDemand, PhysicalSlot,
    PhysicalSlotId, PhysicalWorkBinding, PhysicalWorkBindingError, PlanPrediction, ResourceClaim,
    SlotCompatibility, StagePrediction, StorageMode, WeightedObservationBlock,
    WeightingReplayCompletion, WorkDependency, WorkDomain, WorkExecutionContext, WorkKind,
    WorkNode, WorkNodeId,
};

use crate::gridded_normal_artifact::{
    GriddedNormalArtifactBudget, GriddedNormalArtifactFrameStorage,
    GriddedNormalArtifactMeasurements, GriddedNormalArtifactWriter,
    GriddedNormalReplayArtifact as GriddedNormalSpillArtifact, GriddedNormalReplayStorage,
};

/// Opaque run-scoped normal-operator program and its checksummed spill storage.
///
/// The application may move this capability between major-cycle executors, but
/// cannot inspect records, reopen the selected observation, or apply science.
pub struct FrozenGriddedNormalReplay {
    program: GriddedNormalOperatorProgram,
    spill: GriddedNormalSpillArtifact,
    retention: Option<crate::gridded_normal_artifact::GriddedNormalArtifactRetention>,
    latest_read: Option<GriddedNormalArtifactMeasurements>,
    latest_stream: Option<BoundedStreamMeasurements>,
}

/// Plan-visible identity and sealed size of one private gridded-normal replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GriddedNormalReplayDescriptor {
    identity: crate::ArtifactIdentity,
    bytes: u64,
}

impl GriddedNormalReplayDescriptor {
    /// Exact reconstruction-minted identity of the encoded operator program.
    #[must_use]
    pub const fn identity(self) -> crate::ArtifactIdentity {
        self.identity
    }

    /// Exact sealed artifact size.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }
}

pub(crate) struct GriddedNormalReplayCompilation {
    compiler: GriddedNormalOperatorCompiler,
    writer: Option<GriddedNormalArtifactWriter>,
    spill: Option<GriddedNormalSpillArtifact>,
    compilation_measurements: GriddedNormalCompilationMeasurements,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GriddedNormalCompilationMeasurements {
    pub(crate) blocks: u64,
    pub(crate) source_group_vector_allocations: u64,
    pub(crate) source_group_capacity_growth_bytes: u64,
    pub(crate) reduction_map_entry_insertions: u64,
    pub(crate) multiplicity_vector_allocations: u64,
    pub(crate) multiplicity_capacity_growth_bytes: u64,
    pub(crate) encoded_buffer_allocations: u64,
    pub(crate) encoded_buffer_bytes: u64,
    pub(crate) descriptor_vector_allocations: u64,
    pub(crate) descriptor_capacity_growth_bytes: u64,
}

impl GriddedNormalCompilationMeasurements {
    fn add_block(&mut self, block: GriddedNormalOperatorBlockMeasurements) -> io::Result<()> {
        macro_rules! add {
            ($field:ident, $value:expr) => {
                self.$field = self.$field.checked_add($value).ok_or_else(|| {
                    io::Error::other("gridded-normal allocation measurement overflow")
                })?;
            };
        }
        add!(blocks, 1);
        add!(
            source_group_vector_allocations,
            block.source_group_vector_allocations
        );
        add!(
            source_group_capacity_growth_bytes,
            block.source_group_capacity_growth_bytes
        );
        add!(
            reduction_map_entry_insertions,
            block.reduction_map_entry_insertions
        );
        add!(
            multiplicity_vector_allocations,
            block.multiplicity_vector_allocations
        );
        add!(
            multiplicity_capacity_growth_bytes,
            block.multiplicity_capacity_growth_bytes
        );
        add!(encoded_buffer_allocations, block.encoded_buffer_allocations);
        add!(encoded_buffer_bytes, block.encoded_buffer_bytes);
        add!(
            descriptor_vector_allocations,
            block.descriptor_vector_allocations
        );
        add!(
            descriptor_capacity_growth_bytes,
            block.descriptor_capacity_growth_bytes
        );
        Ok(())
    }
}

impl GriddedNormalReplayCompilation {
    pub(crate) fn new(
        problem: &CompiledProblem,
        context: WorkExecutionContext<'_>,
        storage: &GriddedNormalReplayStorage,
        max_block_samples: usize,
    ) -> io::Result<Self> {
        let budget = project_gridded_normal_artifact_budget(problem, max_block_samples)?;
        validate_gridded_artifact_context(context, budget, crate::IoBufferKind::SpillWrite)?;
        Ok(Self {
            compiler: GriddedNormalOperatorCompiler::new(problem).map_err(io::Error::other)?,
            writer: Some(
                GriddedNormalArtifactWriter::create(storage, budget).map_err(io::Error::other)?,
            ),
            spill: None,
            compilation_measurements: GriddedNormalCompilationMeasurements::default(),
        })
    }

    pub(crate) fn consume_block(
        &mut self,
        block: &casa_imaging_reconstruction::WeightingReplayChunk,
    ) -> io::Result<()> {
        let compiled = self
            .compiler
            .compile_block(block)
            .map_err(io::Error::other)?;
        self.compilation_measurements
            .add_block(compiled.measurements())?;
        self.writer
            .as_mut()
            .ok_or_else(|| io::Error::other("gridded-normal writer already sealed"))?
            .append_frame(
                compiled.sequence(),
                compiled.record_count(),
                compiled.encoded_bytes(),
            )
            .map_err(io::Error::other)
    }

    pub(crate) fn write_measurements(&self) -> GriddedNormalArtifactMeasurements {
        self.writer.as_ref().map_or_else(
            || {
                self.spill
                    .as_ref()
                    .expect("sealed compilation retains its spill")
                    .write_measurements()
            },
            GriddedNormalArtifactWriter::measurements,
        )
    }

    pub(crate) const fn compilation_measurements(&self) -> GriddedNormalCompilationMeasurements {
        self.compilation_measurements
    }

    pub(crate) fn seal(&mut self) -> io::Result<()> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| io::Error::other("gridded-normal writer already sealed"))?;
        self.spill = Some(writer.seal().map_err(io::Error::other)?);
        Ok(())
    }

    pub(crate) fn complete(
        self,
        replay: &WeightingReplayCompletion,
    ) -> io::Result<FrozenGriddedNormalReplay> {
        let spill = self
            .spill
            .ok_or_else(|| io::Error::other("gridded-normal writer was not sealed"))?;
        let program = self
            .compiler
            .complete(
                replay.reconstruction_summary(),
                replay.selected_generation(),
                replay
                    .continuum_transform()
                    .map(|completion| completion.generation_id()),
            )
            .map_err(io::Error::other)?;
        let seal = spill.seal();
        if seal.frame_count() != program.block_count()
            || seal.record_count() != program.record_count()
        {
            return Err(io::Error::other(
                "sealed gridded-normal spill disagrees with reconstruction program",
            ));
        }
        Ok(FrozenGriddedNormalReplay {
            program,
            spill,
            retention: None,
            latest_read: None,
            latest_stream: None,
        })
    }
}

fn validate_gridded_artifact_context(
    context: WorkExecutionContext<'_>,
    budget: GriddedNormalArtifactBudget,
    io_kind: crate::IoBufferKind,
) -> io::Result<()> {
    let claims = &context.node().claims;
    let has_buffer = claims.iter().any(|claim| {
        claim.resource == LeaseResource::IoBuffer(io_kind)
            && claim.amount >= budget.io_buffer_bytes()
    });
    let has_storage = io_kind == crate::IoBufferKind::SpillRead
        || claims.iter().any(|claim| {
            matches!(
                claim.resource,
                LeaseResource::Storage {
                    use_kind: crate::StorageUseKind::Temporary,
                    ..
                }
            ) && claim.amount >= budget.maximum_artifact_bytes()
                && claim.lifetime == ClaimLifetime::Artifact
        });
    let has_file = claims
        .iter()
        .any(|claim| claim.resource == LeaseResource::FileDescriptors && claim.amount >= 1);
    let has_rate = claims
        .iter()
        .any(|claim| matches!(claim.resource, LeaseResource::Rate { .. }));
    let has_queue = claims
        .iter()
        .any(|claim| matches!(claim.resource, LeaseResource::Queue { .. }));
    if has_buffer && has_storage && has_file && has_rate && has_queue {
        Ok(())
    } else {
        Err(io::Error::other(
            "gridded-normal artifact work lacks its complete planned resource claims",
        ))
    }
}

impl FrozenGriddedNormalReplay {
    pub(crate) fn retain_plan_storage(
        &mut self,
        permit: crate::RetainedArtifactPermit,
        storage: &GriddedNormalReplayStorage,
        retained_bytes: u64,
    ) -> io::Result<()> {
        if self.retention.is_some() {
            return Err(io::Error::other(
                "gridded-normal artifact storage was retained more than once",
            ));
        }
        self.retention = Some(
            crate::gridded_normal_artifact::GriddedNormalArtifactRetention::bind(
                permit,
                storage,
                retained_bytes,
            )?,
        );
        Ok(())
    }

    pub(crate) fn validates_plan_storage(
        &self,
        storage: &GriddedNormalReplayStorage,
        retained_bytes: u64,
    ) -> bool {
        self.retention
            .as_ref()
            .is_some_and(|retention| retention.validates_bytes(storage, retained_bytes))
    }
    /// Return the immutable descriptor consumed by later-major planning.
    #[must_use]
    pub fn descriptor(&self) -> GriddedNormalReplayDescriptor {
        GriddedNormalReplayDescriptor {
            identity: crate::ArtifactIdentity::from_logical_identity(self.program.identity()),
            bytes: self.spill.seal().artifact_bytes(),
        }
    }

    pub(crate) const fn latest_read_measurements(
        &self,
    ) -> Option<GriddedNormalArtifactMeasurements> {
        self.latest_read
    }

    pub(crate) const fn latest_stream_measurements(&self) -> Option<&BoundedStreamMeasurements> {
        self.latest_stream.as_ref()
    }

    pub(crate) fn execute_bounded(
        &mut self,
        context: WorkExecutionContext<'_>,
        pass_ordinal: u32,
        state: GriddedNormalOperatorState,
    ) -> io::Result<CompleteDataOperatorResult> {
        let budget = self.spill.budget();
        validate_gridded_artifact_context(context, budget, crate::IoBufferKind::SpillRead)?;
        let source_capacity_bytes = context
            .node()
            .claims
            .iter()
            .find_map(|claim| {
                (claim.resource == LeaseResource::IoBuffer(crate::IoBufferKind::SpillRead))
                    .then_some(claim.amount)
            })
            .ok_or_else(|| io::Error::other("gridded-normal replay buffer claim missing"))?;
        let per_slot = budget.io_buffer_bytes();
        if source_capacity_bytes % per_slot != 0 {
            return Err(io::Error::other(
                "gridded-normal replay buffer claim is not an exact frame-slot multiple",
            ));
        }
        let source_slots = usize::try_from(source_capacity_bytes / per_slot)
            .map_err(|_| io::Error::other("gridded-normal replay slot count overflow"))?;
        let worker_claim = context
            .node()
            .claims
            .iter()
            .find_map(|claim| (claim.resource == LeaseResource::Workers).then_some(claim.amount))
            .ok_or_else(|| io::Error::other("gridded-normal worker claim missing"))?;
        if worker_claim != context.knobs().workers {
            return Err(io::Error::other(
                "gridded-normal worker claim disagrees with execution knobs",
            ));
        }
        let workers = usize::try_from(worker_claim)
            .map_err(|_| io::Error::other("gridded-normal worker count overflow"))?;
        let maximum_frame_records =
            budget.maximum_frame_payload_bytes() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES;
        let maximum_partitions =
            maximum_frame_records.clamp(1, GRIDDED_NORMAL_MAXIMUM_PARTITIONS_PER_BLOCK);
        let partial_bytes = maximum_frame_records
            .checked_mul(size_of::<num_complex::Complex64>())
            .ok_or_else(|| io::Error::other("gridded-normal partial residency overflow"))?;
        let dynamic_kernel_bytes = u64::try_from(partial_bytes)
            .map_err(|_| io::Error::other("gridded-normal kernel residency overflow"))?;
        let plan =
            BoundedStreamPlan::new::<GriddedNormalPredictionWork, GriddedNormalPredictionPartial>(
                source_slots,
                workers,
                source_capacity_bytes,
                maximum_partitions,
                dynamic_kernel_bytes,
            )
            .map_err(|_| io::Error::other("invalid gridded-normal bounded-stream plan"))?;
        let source = self.spill.block_source().map_err(io::Error::other)?;
        let outcome = execute_bounded(
            plan,
            pass_ordinal,
            source,
            GriddedNormalReplayKernel { state },
        )
        .map_err(|failure| match *failure.cause {
            BoundedStreamError::Source(error) => io::Error::other(error),
            BoundedStreamError::Kernel(error) => io::Error::other(error),
            BoundedStreamError::MeasurementOverflow => {
                io::Error::other("gridded-normal stream measurement overflow")
            }
            BoundedStreamError::InvalidKernelPlan => {
                io::Error::other("invalid gridded-normal kernel plan")
            }
            BoundedStreamError::ResidencyExceeded => {
                io::Error::other("gridded-normal stream exceeded planned residency")
            }
            BoundedStreamError::ProducerPanicked => {
                io::Error::other("gridded-normal source producer panicked")
            }
            BoundedStreamError::ProducerDisconnected => {
                io::Error::other("gridded-normal source producer disconnected")
            }
        })?;
        let completion = outcome.source_completion;
        if completion.seal() != self.spill.seal() {
            return Err(io::Error::other(
                "gridded-normal read completion changed the sealed artifact",
            ));
        }
        self.latest_read = Some(completion.measurements());
        self.latest_stream = Some(outcome.measurements);
        Ok(outcome.kernel_completion)
    }
}

struct GriddedNormalReplayKernel {
    state: GriddedNormalOperatorState,
}

impl PartitionedKernel<GriddedNormalArtifactFrameStorage> for GriddedNormalReplayKernel {
    type Partition = GriddedNormalPredictionWork;
    type Partial = GriddedNormalPredictionPartial;
    type Completion = CompleteDataOperatorResult;
    type Error = CompleteDataOperatorError;

    fn partition_count(
        &self,
        _block: BlockIdentity,
        storage: &GriddedNormalArtifactFrameStorage,
    ) -> Result<usize, Self::Error> {
        let records = u64::try_from(storage.payload().len() / GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
            .map_err(|_| CompleteDataOperatorError::ExecutionBinding)?;
        if storage.record_count() != records {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        self.state
            .state
            .prediction_partition_count(storage.sequence(), storage.payload())
            .map_err(CompleteDataOperatorError::Owner)
    }

    fn partition(
        &self,
        _block: BlockIdentity,
        storage: &GriddedNormalArtifactFrameStorage,
        local_ordinal: usize,
    ) -> Result<KernelPartition<Self::Partition>, Self::Error> {
        let partition = self
            .state
            .state
            .prediction_partition(storage.sequence(), storage.payload(), local_ordinal)
            .map_err(CompleteDataOperatorError::Owner)?;
        Ok(KernelPartition::ordered(
            partition.partition_key(),
            0,
            partition.partition_key(),
            partition,
        ))
    }

    fn execute(
        &self,
        _work: WorkIdentity,
        storage: &GriddedNormalArtifactFrameStorage,
        partition: &Self::Partition,
    ) -> Result<Self::Partial, Self::Error> {
        self.state
            .state
            .predict_partition(storage.payload(), *partition)
            .map_err(CompleteDataOperatorError::Owner)
    }

    fn partial_dynamic_capacity_bytes(&self, partial: &Self::Partial) -> u64 {
        partial.resident_bytes()
    }

    fn commit(
        &mut self,
        _work: WorkIdentity,
        storage: &GriddedNormalArtifactFrameStorage,
        partial: Self::Partial,
    ) -> Result<(), Self::Error> {
        self.state
            .state
            .commit_prediction(storage.payload(), partial)
            .map_err(CompleteDataOperatorError::Owner)
    }

    fn complete(self) -> Result<Self::Completion, Self::Error> {
        self.state.complete()
    }
}

pub(crate) fn project_gridded_normal_artifact_budget(
    problem: &CompiledProblem,
    max_block_samples: usize,
) -> io::Result<GriddedNormalArtifactBudget> {
    if max_block_samples == 0 {
        return Err(io::Error::other(
            "gridded-normal replay requires a positive block bound",
        ));
    }
    let maximum_samples = problem
        .inputs()
        .observation_snapshot()
        .sources()
        .iter()
        .try_fold(0_u64, |total, source| {
            let selection = source.selection();
            let channels = selection
                .spectral_windows()
                .iter()
                .map(|window| window.channel_indices().len())
                .max()
                .unwrap_or(0);
            let correlations = selection
                .correlations()
                .iter()
                .map(|selection| selection.products().len())
                .max()
                .unwrap_or(0);
            let per_row = u64::try_from(channels)
                .ok()
                .and_then(|channels| {
                    u64::try_from(correlations)
                        .ok()
                        .and_then(|correlations| channels.checked_mul(correlations))
                })
                .ok_or_else(|| io::Error::other("selected sample bound overflow"))?;
            let source_samples = selection
                .rows()
                .selected_row_count()
                .checked_mul(per_row)
                .ok_or_else(|| io::Error::other("selected sample bound overflow"))?;
            total
                .checked_add(source_samples)
                .ok_or_else(|| io::Error::other("selected sample bound overflow"))
        })?;
    let max_block = u64::try_from(max_block_samples)
        .map_err(|_| io::Error::other("gridded-normal block bound overflow"))?;
    let maximum_frames = maximum_samples
        .checked_add(max_block - 1)
        .and_then(|samples| samples.checked_div(max_block))
        .ok_or_else(|| io::Error::other("gridded-normal frame bound overflow"))?
        .max(1);
    let record_bytes = u64::try_from(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES)
        .map_err(|_| io::Error::other("gridded-normal record width overflow"))?;
    let maximum_contributions_per_sample = match problem.reconstruction().basis() {
        ReconstructionBasis::Constant => 1_u64,
        ReconstructionBasis::ChannelLocal { .. } => {
            let terms = match problem.science().spectral().sampling().kernel() {
                SpectralKernel::Identity | SpectralKernel::Nearest => 1,
                SpectralKernel::Linear => 2,
                SpectralKernel::Cubic => 4,
                SpectralKernel::ChannelIntegration { maximum_terms } => maximum_terms,
            };
            u64::try_from(terms)
                .map_err(|_| io::Error::other("spectral contribution bound overflow"))?
        }
        ReconstructionBasis::Taylor { .. } => {
            return Err(io::Error::other(
                "gridded-normal replay does not support Taylor reconstruction",
            ));
        }
    };
    let maximum_payload_bytes = maximum_samples
        .checked_mul(maximum_contributions_per_sample)
        .and_then(|contributions| contributions.checked_mul(record_bytes))
        .ok_or_else(|| io::Error::other("gridded-normal payload bound overflow"))?;
    let maximum_frame_samples = usize::try_from(maximum_samples.min(max_block))
        .map_err(|_| io::Error::other("gridded-normal frame sample bound overflow"))?
        .max(1);
    let maximum_frame_payload_bytes = maximum_frame_samples
        .checked_mul(
            usize::try_from(maximum_contributions_per_sample)
                .map_err(|_| io::Error::other("spectral contribution bound overflow"))?,
        )
        .and_then(|contributions| contributions.checked_mul(GRIDDED_NORMAL_OPERATOR_RECORD_BYTES))
        .ok_or_else(|| io::Error::other("gridded-normal frame payload overflow"))?;
    GriddedNormalArtifactBudget::for_bounded_stream(
        maximum_payload_bytes.max(
            u64::try_from(maximum_frame_payload_bytes)
                .map_err(|_| io::Error::other("gridded-normal frame payload overflow"))?,
        ),
        maximum_frame_payload_bytes,
        maximum_frames,
    )
    .map_err(io::Error::other)
}

/// Runtime-owned physical residency for one serial complete-data operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompleteDataResidency {
    grid_bytes: usize,
    convolution_cache_bytes: usize,
    fft_resident_bytes: usize,
    fft_planning_bytes: usize,
    forward_workspace_bytes: usize,
    primitive_output_bytes: usize,
    major_cycle_model_bytes: usize,
    peak_bytes: usize,
}

impl CompleteDataResidency {
    /// Bytes for pass-required accumulation and compensation grids.
    #[must_use]
    pub const fn grid_bytes(self) -> usize {
        self.grid_bytes
    }

    /// Bytes for normalized convolution taps and image-correction axes.
    #[must_use]
    pub const fn convolution_cache_bytes(self) -> usize {
        self.convolution_cache_bytes
    }

    /// Bytes retained by reusable FFT plans, lane, and library scratch.
    #[must_use]
    pub const fn fft_resident_bytes(self) -> usize {
        self.fft_resident_bytes
    }

    /// Transient bytes for RustFFT planner recipes and cache metadata.
    #[must_use]
    pub const fn fft_planning_bytes(self) -> usize {
        self.fft_planning_bytes
    }

    /// Bytes for one forward grid and one lending prediction buffer.
    #[must_use]
    pub const fn forward_workspace_bytes(self) -> usize {
        self.forward_workspace_bytes
    }

    /// Bytes covering retained prior state plus newly produced normal-state primitives.
    #[must_use]
    pub const fn primitive_output_bytes(self) -> usize {
        self.primitive_output_bytes
    }

    /// Bytes for the current/final model samples and bounded pending delta.
    #[must_use]
    pub const fn major_cycle_model_bytes(self) -> usize {
        self.major_cycle_model_bytes
    }

    /// Conservative peak of all runtime-owned T19 allocations.
    #[must_use]
    pub const fn peak_bytes(self) -> usize {
        self.peak_bytes
    }
}

/// Hard physical allocations and FFT preparation bound to one T18 replay.
#[derive(Debug, Clone)]
pub struct CompleteDataPlanFragment {
    specification: SpectralOperatorSpecification,
    workload: SpectralOperatorWorkload,
    residency: CompleteDataResidency,
    preparation_node: WorkNodeId,
    replay_node: WorkNodeId,
    reconciliation_node: Option<WorkNodeId>,
}

impl CompleteDataPlanFragment {
    /// Compile runtime resources for the exact problem and T18 replay block bound.
    pub fn new(
        problem: &CompiledProblem,
        max_replay_block_samples: usize,
        replay_node: WorkNodeId,
        pass: SpectralOperatorPass,
    ) -> Result<Self, CompleteDataPlanError> {
        let specification = SpectralOperatorSpecification::new(problem)?;
        let workload = spectral_operator_workload(&specification, max_replay_block_samples, pass)?;
        let shape = workload.grid_shape();
        let slab = specification.slab();
        let preparation_node = WorkNodeId::new(format!(
            "spectral-operator-fft-plan-{}x{}-ch{}-{}",
            shape[0],
            shape[1],
            slab.core_range().start,
            slab.core_range().end
        ));
        Self::from_specification(
            problem,
            max_replay_block_samples,
            replay_node,
            preparation_node,
            specification,
            pass,
        )
    }

    /// Compile runtime resources for one bounded channel-local core slab.
    pub fn for_slab(
        problem: &CompiledProblem,
        max_replay_block_samples: usize,
        replay_node: WorkNodeId,
        core_start: usize,
        core_depth: usize,
        pass: SpectralOperatorPass,
    ) -> Result<Self, CompleteDataPlanError> {
        let specification =
            SpectralOperatorSpecification::for_slab(problem, core_start, core_depth)?;
        let shape = specification.grid_shape();
        let slab = specification.slab();
        let preparation_node = WorkNodeId::new(format!(
            "spectral-operator-fft-plan-{}x{}-ch{}-{}",
            shape[0],
            shape[1],
            slab.core_range().start,
            slab.core_range().end
        ));
        Self::from_specification(
            problem,
            max_replay_block_samples,
            replay_node,
            preparation_node,
            specification,
            pass,
        )
    }

    /// Compile runtime resources with a caller-supplied plan-unique FFT node.
    pub fn new_with_preparation_node(
        problem: &CompiledProblem,
        max_replay_block_samples: usize,
        replay_node: WorkNodeId,
        preparation_node: WorkNodeId,
        pass: SpectralOperatorPass,
    ) -> Result<Self, CompleteDataPlanError> {
        let specification = SpectralOperatorSpecification::new(problem)?;
        Self::from_specification(
            problem,
            max_replay_block_samples,
            replay_node,
            preparation_node,
            specification,
            pass,
        )
    }

    fn from_specification(
        problem: &CompiledProblem,
        max_replay_block_samples: usize,
        replay_node: WorkNodeId,
        preparation_node: WorkNodeId,
        specification: SpectralOperatorSpecification,
        pass: SpectralOperatorPass,
    ) -> Result<Self, CompleteDataPlanError> {
        let workload = spectral_operator_workload(&specification, max_replay_block_samples, pass)?;
        let residency = project_residency(problem, workload)?;
        Ok(Self {
            specification,
            workload,
            residency,
            preparation_node,
            replay_node,
            reconciliation_node: None,
        })
    }

    /// Return the exact FFT-planning node inserted before replay.
    #[must_use]
    pub const fn preparation_node(&self) -> &WorkNodeId {
        &self.preparation_node
    }

    pub(crate) const fn replay_node(&self) -> &WorkNodeId {
        &self.replay_node
    }

    /// Return the final-reconciliation node after composition.
    #[must_use]
    pub fn reconciliation_node(&self) -> Option<&WorkNodeId> {
        self.reconciliation_node.as_ref()
    }

    /// Return the runtime-owned resident-byte projection.
    #[must_use]
    pub const fn residency(&self) -> CompleteDataResidency {
        self.residency
    }

    /// Return the exact core/halo channel slab projected by this plan.
    #[must_use]
    pub const fn slab(&self) -> casa_imaging_reconstruction::SpectralSlabPlan {
        self.specification.slab()
    }

    /// Prepare reusable FFT state only at the planned FFT node.
    pub fn prepare(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<CompleteDataPreparedState, CompleteDataPlanError> {
        if context.node().id != self.preparation_node
            || context.node().kind != WorkKind::FftPlanning
        {
            return Err(CompleteDataPlanError::WrongExecutionNode);
        }
        if context.compiled().problem_id() != self.specification.problem_id() {
            return Err(CompleteDataPlanError::PlanMismatch);
        }
        self.validate_fft_capability(context)?;
        Ok(CompleteDataPreparedState {
            owner: prepare_spectral_operator(self.specification.clone(), self.workload)?,
            problem: self.specification.problem_id(),
            attempt: context.attempt_id(),
            preparation_node: self.preparation_node.clone(),
            replay_node: self.replay_node.clone(),
            reconciliation_node: self.reconciliation_node.clone(),
            lease_epoch: context.lease_epoch(),
        })
    }

    /// Begin replay from the prepared FFT state and exact frozen T18 generation.
    pub fn begin(
        &self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
        weighting: &WeightingAlgorithmState,
        prepared: CompleteDataPreparedState,
    ) -> Result<SpectralOperatorState, CompleteDataPlanError> {
        let slab = self.specification.slab();
        let compiled_specification = SpectralOperatorSpecification::for_slab(
            problem,
            slab.core_range().start,
            slab.core_depth(),
        )?;
        if context.node().id != self.replay_node {
            return Err(CompleteDataPlanError::WrongExecutionNode);
        }
        if context.compiled().problem_id() != problem.problem_id()
            || compiled_specification != self.specification
            || weighting.max_replay_block_samples() != self.workload.max_replay_block_samples()
        {
            return Err(CompleteDataPlanError::PlanMismatch);
        }
        self.validate_allocations(context)?;
        prepared.begin(context, problem, weighting, self)
    }

    pub(crate) fn begin_gridded_replay(
        &self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
        preparation: &MajorCyclePreparation,
        prior: FinalNormalState,
        prepared: CompleteDataPreparedState,
        artifact: &FrozenGriddedNormalReplay,
    ) -> Result<GriddedNormalOperatorState, CompleteDataPlanError> {
        if context.node().id != self.replay_node
            || self.workload.pass() != SpectralOperatorPass::ResidualRefresh
            || context.compiled().problem_id() != problem.problem_id()
        {
            return Err(CompleteDataPlanError::PlanMismatch);
        }
        self.validate_allocations(context)?;
        prepared.begin_gridded(
            context,
            problem,
            preparation,
            prior,
            &artifact.program,
            self,
        )
    }

    fn validate_allocations(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), CompleteDataPlanError> {
        let suffix = operator_allocation_suffix(self.workload);
        let residency = self.residency;
        let required = [
            (
                format!("spectral-operator-grids-{suffix}"),
                residency.grid_bytes(),
            ),
            (
                format!("spectral-operator-convolution-cache-{suffix}"),
                residency.convolution_cache_bytes(),
            ),
            (
                format!("spectral-operator-fft-state-{suffix}"),
                residency.fft_resident_bytes(),
            ),
            (
                format!("spectral-operator-forward-workspace-{suffix}"),
                residency.forward_workspace_bytes(),
            ),
            (
                format!("spectral-operator-primitives-{suffix}"),
                residency.primitive_output_bytes(),
            ),
            (
                format!("spectral-operator-major-cycle-model-{suffix}"),
                residency.major_cycle_model_bytes(),
            ),
        ];
        for (allocation, bytes) in required {
            let bytes =
                u64::try_from(bytes).map_err(|_| CompleteDataPlanError::ResidencyOverflow)?;
            if context
                .allocations()
                .iter()
                .filter(|capability| {
                    capability.allocation().as_str() == allocation
                        && capability.capacity_bytes() == bytes
                })
                .count()
                != 1
            {
                return Err(CompleteDataPlanError::MissingAllocationCapability);
            }
        }
        Ok(())
    }

    fn validate_fft_capability(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), CompleteDataPlanError> {
        let amount = u64::try_from(self.residency.fft_planning_bytes())
            .map_err(|_| CompleteDataPlanError::ResidencyOverflow)?;
        if context
            .resources()
            .iter()
            .filter(|capability| {
                capability.resource()
                    == &LeaseResource::RuntimeOverhead(crate::RuntimeOverheadKind::FftWorkspace)
                    && capability.amount() == amount
                    && capability.lifetime() == &ClaimLifetime::Work
            })
            .count()
            != 1
        {
            return Err(CompleteDataPlanError::MissingFftCapability);
        }
        let suffix = operator_allocation_suffix(self.workload);
        let allocation = format!("spectral-operator-fft-state-{suffix}");
        let capacity = u64::try_from(self.residency.fft_resident_bytes())
            .map_err(|_| CompleteDataPlanError::ResidencyOverflow)?;
        if context
            .allocations()
            .iter()
            .filter(|capability| {
                capability.allocation().as_str() == allocation
                    && capability.capacity_bytes() == capacity
            })
            .count()
            != 1
        {
            return Err(CompleteDataPlanError::MissingAllocationCapability);
        }
        Ok(())
    }

    /// Add shared grids, FFT scratch, and primitive outputs to physical work.
    ///
    /// Composition also binds this fragment to the sealed observation
    /// transaction's post-replay reconciliation node: afterwards,
    /// reconciliation may execute only at that exact plan-authoritative Compute
    /// node.
    ///
    /// Returns the composed physical work together with this fragment bound to
    /// its authoritative reconciliation node.
    pub fn compose(
        mut self,
        base: &PhysicalWorkBinding,
    ) -> Result<(PhysicalWorkBinding, Self), CompleteDataPlanError> {
        let reconciliation = base
            .observation_transaction()
            .post_replay_reconciliation()
            .ok_or(CompleteDataPlanError::MissingReconciliationNode)?;
        if !base.execution_dag().nodes().contains_key(&self.replay_node) {
            return Err(CompleteDataPlanError::MissingReplayNode);
        }
        let specs = self.allocation_specs(reconciliation)?;
        let replay_fence = ClaimLifetime::through_fence(FenceKind::Io);
        let fft_planning_bytes = u64::try_from(self.residency.fft_planning_bytes())
            .map_err(|_| CompleteDataPlanError::ResidencyOverflow)?;
        let mut nodes = base
            .execution_dag()
            .nodes()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let replay = nodes
            .iter_mut()
            .find(|node| node.id == self.replay_node)
            .ok_or(CompleteDataPlanError::MissingReplayNode)?;
        if !replay.fences.contains(&FenceKind::Io) {
            return Err(CompleteDataPlanError::ReplayWithoutTerminalFence);
        }
        let preparation = WorkNode {
            id: self.preparation_node.clone(),
            kind: WorkKind::FftPlanning,
            domain: WorkDomain::Cpu,
            implementation: replay.implementation.clone(),
            dependencies: replay.dependencies.clone(),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: LeaseResource::RuntimeOverhead(
                        crate::RuntimeOverheadKind::FftWorkspace,
                    ),
                    amount: fft_planning_bytes,
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: [
                Some(specs[2].usage(ClaimLifetime::Work)),
                (self.workload.pass() == SpectralOperatorPass::ResidualRefresh)
                    .then(|| specs[4].usage(ClaimLifetime::Work)),
                Some(specs[5].usage(ClaimLifetime::Work)),
            ]
            .into_iter()
            .flatten()
            .collect(),
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        };
        replay
            .dependencies
            .insert(WorkDependency::Work(self.preparation_node.clone()));
        replay.allocations.extend([
            specs[0].usage(replay_fence.clone()),
            specs[1].usage(replay_fence.clone()),
            specs[2].usage(replay_fence.clone()),
            specs[3].usage(replay_fence.clone()),
            specs[4].usage(replay_fence.clone()),
            specs[5].usage(replay_fence),
        ]);
        nodes.push(preparation.clone());
        let planned_reconciliation = nodes
            .iter_mut()
            .find(|node| &node.id == reconciliation)
            .ok_or(CompleteDataPlanError::MissingReconciliationNode)?;
        if planned_reconciliation.kind != WorkKind::Compute {
            return Err(CompleteDataPlanError::MissingReconciliationNode);
        }
        planned_reconciliation.allocations.extend([
            specs[4].usage(ClaimLifetime::Work),
            specs[5].usage(ClaimLifetime::Work),
        ]);
        self.reconciliation_node = Some(reconciliation.clone());

        let mut alternative = base.execution_dag().resource_alternative().clone();
        alternative.id =
            AlternativeId::new(format!("{}-spectral-operator", alternative.id.as_str()));
        alternative
            .demand
            .memory
            .extend(specs.iter().map(CompleteDataAllocation::memory_demand));
        alternative.demand.overhead.fft_workspace_bytes = alternative
            .demand
            .overhead
            .fft_workspace_bytes
            .max(fft_planning_bytes);
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
                .chain(specs.iter().map(CompleteDataAllocation::logical_allocation))
                .collect(),
            physical_slots: base
                .execution_dag()
                .physical_slots()
                .values()
                .cloned()
                .chain(specs.iter().map(CompleteDataAllocation::physical_slot))
                .collect(),
            initial_knobs: base.execution_dag().initial_knobs().clone(),
            adaptations: base
                .execution_dag()
                .adaptations()
                .values()
                .cloned()
                .collect(),
        })?;
        let replay_prediction = base
            .prediction()
            .stages()
            .get(&self.replay_node)
            .ok_or(CompleteDataPlanError::MissingReplayPrediction)?;
        let preparation_prediction =
            StagePrediction::new(preparation.id, replay_prediction.elapsed_nanos());
        let prediction = PlanPrediction::new(
            base.prediction()
                .elapsed_nanos()
                .checked_add(preparation_prediction.elapsed_nanos())
                .ok_or(CompleteDataPlanError::ResidencyOverflow)?,
            base.prediction().confidence(),
            base.prediction().uncertainty().to_vec(),
            base.prediction()
                .stages()
                .values()
                .cloned()
                .chain([preparation_prediction])
                .collect(),
        )?;
        let physical = PhysicalWorkBinding::with_implementation_contract(
            base.implementation_contract().for_execution_dag(&dag)?,
            dag,
            prediction,
            base.artifacts().to_vec(),
            base.observation_transaction().clone(),
            base.publication_layouts().clone(),
            base.product_publication_authority(),
        )?;
        Ok((physical, self))
    }

    fn allocation_specs(
        &self,
        reconciliation: &WorkNodeId,
    ) -> Result<[CompleteDataAllocation; 6], CompleteDataPlanError> {
        let suffix = operator_allocation_suffix(self.workload);
        let residency = self.residency;
        let replay_done = BTreeSet::from([WorkDependency::Fence(FenceId::new(
            self.replay_node.clone(),
            FenceKind::Io,
        ))]);
        let reconciled = BTreeSet::from([WorkDependency::Work(reconciliation.clone())]);
        let residual_refresh = self.workload.pass() == SpectralOperatorPass::ResidualRefresh;
        Ok([
            CompleteDataAllocation::new(
                format!("spectral-operator-grids-{suffix}"),
                residency.grid_bytes(),
                if residual_refresh {
                    "spectral-operator-shared-residual-refresh-grid"
                } else {
                    "spectral-operator-shared-dirty-psf-residual-grids"
                },
                InitializationPolicy::ZeroBeforeRead,
                self.replay_node.clone(),
                replay_done.clone(),
            )?,
            CompleteDataAllocation::new(
                format!("spectral-operator-convolution-cache-{suffix}"),
                residency.convolution_cache_bytes(),
                "spectral-operator-convolution-taps-and-corrections",
                InitializationPolicy::OverwriteBeforeRead,
                self.replay_node.clone(),
                replay_done.clone(),
            )?,
            CompleteDataAllocation::new(
                format!("spectral-operator-fft-state-{suffix}"),
                residency.fft_resident_bytes(),
                "spectral-operator-rustfft-plans-lane-and-scratch",
                InitializationPolicy::OverwriteBeforeRead,
                self.preparation_node.clone(),
                replay_done.clone(),
            )?,
            CompleteDataAllocation::new(
                format!("spectral-operator-forward-workspace-{suffix}"),
                residency.forward_workspace_bytes(),
                "spectral-operator-forward-grid-and-bounded-predictions",
                InitializationPolicy::OverwriteBeforeRead,
                self.replay_node.clone(),
                replay_done,
            )?,
            CompleteDataAllocation::new(
                format!("spectral-operator-primitives-{suffix}"),
                residency.primitive_output_bytes(),
                "spectral-operator-unnormalized-dirty-psf-residual-primitives",
                InitializationPolicy::OverwriteBeforeRead,
                if residual_refresh {
                    self.preparation_node.clone()
                } else {
                    self.replay_node.clone()
                },
                reconciled,
            )?,
            CompleteDataAllocation::new(
                format!("spectral-operator-major-cycle-model-{suffix}"),
                residency.major_cycle_model_bytes(),
                "spectral-operator-current-final-model-and-pending-delta",
                InitializationPolicy::OverwriteBeforeRead,
                self.preparation_node.clone(),
                BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
            )?,
        ])
    }
}

fn operator_allocation_suffix(workload: SpectralOperatorWorkload) -> String {
    let shape = workload.grid_shape();
    let slab = workload.slab();
    format!(
        "{}-{}x{}-ch{}-{}",
        match workload.pass() {
            SpectralOperatorPass::InitialMajor => "initial",
            SpectralOperatorPass::ResidualRefresh => "residual-refresh",
        },
        shape[0],
        shape[1],
        slab.core_range().start,
        slab.core_range().end
    )
}

fn project_residency(
    problem: &CompiledProblem,
    workload: SpectralOperatorWorkload,
) -> Result<CompleteDataResidency, CompleteDataPlanError> {
    let complex_bytes = size_of::<num_complex::Complex64>();
    let grid_bytes = workload
        .grid_complex_values()
        .checked_mul(complex_bytes)
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    let convolution_cache_bytes = workload
        .convolution_f64_values()
        .checked_mul(size_of::<f64>())
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    let fft_resident_bytes = workload
        .fft_resident_complex_values()
        .checked_mul(complex_bytes)
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    let fft_planning_bytes = workload
        .fft_planning_words()
        .checked_mul(size_of::<usize>())
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    let forward_workspace_bytes = workload
        .forward_complex_values()
        .checked_mul(complex_bytes)
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    let primitive_output_bytes = workload
        .primitive_complex_values()
        .checked_mul(complex_bytes)
        .and_then(|bytes| {
            workload
                .primitive_f64_values()
                .checked_mul(size_of::<f64>())
                .and_then(|f64_bytes| bytes.checked_add(f64_bytes))
        })
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    let model = problem.model_lifecycle();
    let total_model_samples = model.target().sample_count();
    let slab = workload.slab();
    let model_samples = total_model_samples
        .checked_div(slab.total_channels())
        .and_then(|plane_samples| plane_samples.checked_mul(slab.resident_depth()))
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    let major_cycle_model_bytes = model_samples
        .checked_mul(size_of::<ModelSample>())
        .and_then(|bytes| {
            model
                .bounds()
                .max_delta_terms()
                .min(model_samples)
                .checked_mul(size_of::<ModelDeltaTerm>())
                .and_then(|delta_bytes| bytes.checked_add(delta_bytes))
        })
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    let peak_bytes = grid_bytes
        .checked_add(convolution_cache_bytes)
        .and_then(|bytes| bytes.checked_add(fft_resident_bytes))
        .and_then(|bytes| bytes.checked_add(fft_planning_bytes))
        .and_then(|bytes| bytes.checked_add(forward_workspace_bytes))
        .and_then(|bytes| bytes.checked_add(primitive_output_bytes))
        .and_then(|bytes| bytes.checked_add(major_cycle_model_bytes))
        .ok_or(CompleteDataPlanError::ResidencyOverflow)?;
    Ok(CompleteDataResidency {
        grid_bytes,
        convolution_cache_bytes,
        fft_resident_bytes,
        fft_planning_bytes,
        forward_workspace_bytes,
        primitive_output_bytes,
        major_cycle_model_bytes,
        peak_bytes,
    })
}

struct CompleteDataAllocation {
    allocation: AllocationId,
    slot: PhysicalSlotId,
    bytes: u64,
    compatibility: SlotCompatibility,
    acquire_at: WorkNodeId,
    release_after: BTreeSet<WorkDependency>,
}

impl CompleteDataAllocation {
    fn new(
        id: String,
        bytes: usize,
        layout: &str,
        initialization: InitializationPolicy,
        acquire_at: WorkNodeId,
        release_after: BTreeSet<WorkDependency>,
    ) -> Result<Self, CompleteDataPlanError> {
        let allocation = AllocationId::new(id);
        let slot = PhysicalSlotId::new(format!("{}-slot", allocation.as_str()));
        Ok(Self {
            allocation,
            slot,
            bytes: u64::try_from(bytes).map_err(|_| CompleteDataPlanError::ResidencyOverflow)?,
            compatibility: SlotCompatibility {
                memory_domain: CapacityDomainId::new("host-memory"),
                views: BTreeSet::from([CapacityViewId::new("host-memory")]),
                alignment_bytes: align_of::<usize>() as u64,
                storage_mode: StorageMode::Host,
                layout: AllocationLayout::new(layout),
                initialization,
                access: AllocationAccess::ReadWrite,
            },
            acquire_at,
            release_after,
        })
    }

    fn usage(&self, lifetime: ClaimLifetime) -> AllocationUse {
        AllocationUse {
            allocation: self.allocation.clone(),
            lifetime,
        }
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
}

/// Exact reason the T19 plan could not bind to T18 replay work.
#[derive(Debug)]
pub enum CompleteDataPlanError {
    /// The named T18 replay node is absent.
    MissingReplayNode,
    /// The observation transaction lacks final reconciliation.
    MissingReconciliationNode,
    /// T18 replay does not settle an I/O fence before completion.
    ReplayWithoutTerminalFence,
    /// The T18 replay lacks a cost prediction from which FFT preparation can project work.
    MissingReplayPrediction,
    /// Execution was attempted outside the plan-bound replay node.
    WrongExecutionNode,
    /// The runtime problem no longer matches the planned operator.
    PlanMismatch,
    /// T18 has not produced the frozen generation that owns this operator.
    MissingFrozenWeighting,
    /// A required shared grid, FFT, or output allocation capability is absent.
    MissingAllocationCapability,
    /// FFT preparation lacks its exact transient overhead or resident allocation capability.
    MissingFftCapability,
    /// A resident-byte projection exceeded the plan identity domain.
    ResidencyOverflow,
    /// The composed execution DAG is invalid.
    Execution(ExecutionError),
    /// The complete physical binding is inconsistent.
    Binding(PhysicalWorkBindingError),
    /// The reconstruction owner rejected the plan or compiled problem.
    Operator(CompleteDataOperatorError),
}

impl fmt::Display for CompleteDataPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingReplayNode => {
                formatter.write_str("T19 requires its exact T18 replay node")
            }
            Self::MissingReconciliationNode => {
                formatter.write_str("T19 requires final reconciliation")
            }
            Self::ReplayWithoutTerminalFence => {
                formatter.write_str("T19 requires terminal T18 replay proof")
            }
            Self::MissingReplayPrediction => {
                formatter.write_str("T19 requires a prediction for its T18 replay")
            }
            Self::WrongExecutionNode => {
                formatter.write_str("T19 can execute only at its planned replay node")
            }
            Self::PlanMismatch => {
                formatter.write_str("T19 execution problem does not match its physical plan")
            }
            Self::MissingFrozenWeighting => {
                formatter.write_str("T19 requires a frozen T18 weighting generation")
            }
            Self::MissingAllocationCapability => {
                formatter.write_str("T19 execution lacks an exact planned allocation capability")
            }
            Self::MissingFftCapability => {
                formatter.write_str("T19 FFT preparation lacks its exact planned capability")
            }
            Self::ResidencyOverflow => formatter.write_str("T19 residency overflowed"),
            Self::Execution(error) => error.fmt(formatter),
            Self::Binding(error) => error.fmt(formatter),
            Self::Operator(error) => error.fmt(formatter),
        }
    }
}

impl Error for CompleteDataPlanError {}

impl From<ExecutionError> for CompleteDataPlanError {
    fn from(error: ExecutionError) -> Self {
        Self::Execution(error)
    }
}

impl From<PhysicalWorkBindingError> for CompleteDataPlanError {
    fn from(error: PhysicalWorkBindingError) -> Self {
        Self::Binding(error)
    }
}

impl From<SpectralOperatorError> for CompleteDataPlanError {
    fn from(error: SpectralOperatorError) -> Self {
        Self::Operator(CompleteDataOperatorError::Owner(error))
    }
}

/// Opaque prepared FFT state retained from the explicit planning node to replay.
#[doc(hidden)]
#[derive(Debug)]
pub struct CompleteDataPreparedState {
    owner: PreparedSpectralOperator,
    problem: CompiledProblemId,
    attempt: ExecutionAttemptId,
    preparation_node: WorkNodeId,
    replay_node: WorkNodeId,
    reconciliation_node: Option<WorkNodeId>,
    lease_epoch: u64,
}

impl CompleteDataPreparedState {
    fn begin(
        self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
        weighting: &WeightingAlgorithmState,
        fragment: &CompleteDataPlanFragment,
    ) -> Result<SpectralOperatorState, CompleteDataPlanError> {
        if self.problem != problem.problem_id()
            || self.attempt != context.attempt_id()
            || self.preparation_node != fragment.preparation_node
            || self.replay_node != fragment.replay_node
            || self.reconciliation_node != fragment.reconciliation_node
            || self.replay_node != context.node().id
            || self.lease_epoch != context.lease_epoch()
        {
            return Err(CompleteDataPlanError::PlanMismatch);
        }
        let reconciliation_node = self
            .reconciliation_node
            .ok_or(CompleteDataPlanError::MissingReconciliationNode)?;
        let state = self.owner.begin(problem, weighting).map_err(|error| {
            CompleteDataPlanError::Operator(CompleteDataOperatorError::Owner(error))
        })?;
        Ok(SpectralOperatorState {
            state,
            binding: CompleteDataExecutionBinding {
                problem: problem.problem_id(),
                attempt: context.attempt_id(),
                replay_node: context.node().id.clone(),
                reconciliation_node,
                lease_epoch: context.lease_epoch(),
                observation_predecessor_required: true,
            },
        })
    }

    pub(crate) fn begin_streaming(
        self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
        fragment: &CompleteDataPlanFragment,
    ) -> Result<SpectralOperatorState, CompleteDataPlanError> {
        if self.problem != problem.problem_id()
            || self.attempt != context.attempt_id()
            || self.preparation_node != fragment.preparation_node
            || self.replay_node != fragment.replay_node
            || self.reconciliation_node != fragment.reconciliation_node
            || self.replay_node != context.node().id
            || self.lease_epoch != context.lease_epoch()
        {
            return Err(CompleteDataPlanError::PlanMismatch);
        }
        let reconciliation_node = self
            .reconciliation_node
            .ok_or(CompleteDataPlanError::MissingReconciliationNode)?;
        let state = self.owner.begin_streaming(problem).map_err(|error| {
            CompleteDataPlanError::Operator(CompleteDataOperatorError::Owner(error))
        })?;
        Ok(SpectralOperatorState {
            state,
            binding: CompleteDataExecutionBinding {
                problem: problem.problem_id(),
                attempt: context.attempt_id(),
                replay_node: context.node().id.clone(),
                reconciliation_node,
                lease_epoch: context.lease_epoch(),
                observation_predecessor_required: true,
            },
        })
    }

    fn begin_gridded(
        self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
        preparation: &MajorCyclePreparation,
        prior: FinalNormalState,
        program: &GriddedNormalOperatorProgram,
        fragment: &CompleteDataPlanFragment,
    ) -> Result<GriddedNormalOperatorState, CompleteDataPlanError> {
        if self.problem != problem.problem_id()
            || self.attempt != context.attempt_id()
            || self.preparation_node != fragment.preparation_node
            || self.replay_node != fragment.replay_node
            || self.reconciliation_node != fragment.reconciliation_node
            || self.replay_node != context.node().id
            || self.lease_epoch != context.lease_epoch()
        {
            return Err(CompleteDataPlanError::PlanMismatch);
        }
        let reconciliation_node = self
            .reconciliation_node
            .ok_or(CompleteDataPlanError::MissingReconciliationNode)?;
        let state = program
            .begin_apply(problem, preparation.final_model(), prior, self.owner)
            .map_err(|error| {
                CompleteDataPlanError::Operator(CompleteDataOperatorError::Owner(error))
            })?;
        Ok(GriddedNormalOperatorState {
            state,
            binding: CompleteDataExecutionBinding {
                problem: problem.problem_id(),
                attempt: context.attempt_id(),
                replay_node: context.node().id.clone(),
                reconciliation_node,
                lease_epoch: context.lease_epoch(),
                observation_predecessor_required: false,
            },
        })
    }
}

pub(crate) struct GriddedNormalOperatorState {
    state: GriddedNormalOperatorApply,
    binding: CompleteDataExecutionBinding,
}

impl GriddedNormalOperatorState {
    pub(crate) fn complete(self) -> Result<CompleteDataOperatorResult, CompleteDataOperatorError> {
        let evidence = self
            .state
            .finish()
            .map_err(CompleteDataOperatorError::Owner)?;
        if evidence.completion().problem_id() != self.binding.problem {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(CompleteDataOperatorResult {
            evidence,
            attempt: self.binding.attempt,
            replay_node: self.binding.replay_node,
            reconciliation_node: self.binding.reconciliation_node,
            lease_epoch: self.binding.lease_epoch,
            observation_predecessor_required: self.binding.observation_predecessor_required,
        })
    }
}

/// Runtime attempt-bound envelope around one owner-minted T19 complete-data
/// result.
///
/// The envelope pairs the reconstruction evidence inseparably with the exact
/// runtime attempt, lease epoch, settled replay node, and plan-authoritative
/// final-reconciliation node. It is deliberately not constructible from caller
/// digests or a generic scheduler completion: it is minted only by consuming a
/// [`SpectralOperatorState`] after that state has accepted the complete
/// ordered stream of [`WeightedObservationBlock`] values and the terminal
/// [`WeightingReplayCompletion`].
///
/// A caller cannot substitute a generic scheduler completion:
///
/// ```compile_fail
/// use casa_imaging_runtime::{
///     AttemptBoundObservationCompletion, SpectralOperatorState,
/// };
///
/// fn substitute(
///     state: SpectralOperatorState,
///     generic: &AttemptBoundObservationCompletion,
/// ) {
///     let _ = state.complete(generic);
/// }
/// ```
///
/// Nor can a caller construct completion evidence from its own digest:
///
/// ```compile_fail
/// use casa_imaging_runtime::CompleteDataOperatorResult;
///
/// let _ = CompleteDataOperatorResult {};
/// ```
#[derive(Debug)]
pub struct CompleteDataOperatorResult {
    evidence: CompleteDataOwnerResult,
    attempt: ExecutionAttemptId,
    replay_node: WorkNodeId,
    reconciliation_node: WorkNodeId,
    lease_epoch: u64,
    observation_predecessor_required: bool,
}

impl CompleteDataOperatorResult {
    /// Return reconstruction-owned unnormalized primitives.
    #[must_use]
    pub const fn primitives(&self) -> &SpectralOperatorPrimitives {
        self.evidence.primitives()
    }

    /// Return the exact Compiled Problem executed by this operator.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.evidence.completion().problem_id()
    }

    /// Return the compiled geometry/operator coordinate commitment.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.evidence.completion().geometry_id()
    }

    /// Return the exact numerical contract.
    #[must_use]
    pub const fn numerics_id(&self) -> NumericsContractId {
        self.evidence.completion().numerics_id()
    }

    /// Return the compiler-owned weighting commitment used by T18.
    #[must_use]
    pub const fn weighting_commitment_id(&self) -> WeightingCommitmentId {
        self.evidence.completion().weighting_commitment_id()
    }

    /// Return the frozen W generation carried by every accepted block.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.evidence.completion().weighting_generation()
    }

    /// Return the unique terminal replay identity.
    #[must_use]
    pub const fn replay_id(&self) -> WeightingReplayId {
        self.evidence.completion().replay_id()
    }

    /// Return the exact T17 selected-observation generation behind every sample.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.evidence.completion().selected_generation()
    }

    /// Return exact T18 weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.evidence.completion().coverage()
    }

    /// Return the versioned primitive set produced by the science owner.
    #[must_use]
    pub const fn primitive_catalog(&self) -> SpectralPrimitiveCatalog {
        self.evidence.completion().primitive_catalog()
    }

    /// Return the execution attempt that authorized this complete replay.
    #[must_use]
    pub const fn attempt_id(&self) -> ExecutionAttemptId {
        self.attempt
    }

    /// Return the exact replay node whose settled fence completed T19.
    #[must_use]
    pub const fn replay_node(&self) -> &WorkNodeId {
        &self.replay_node
    }

    /// Return the plan-authoritative final-reconciliation node bound at compose time.
    #[must_use]
    pub(crate) const fn reconciliation_node(&self) -> &WorkNodeId {
        &self.reconciliation_node
    }

    /// Return the Resource Authority lease epoch held through completion.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    pub(crate) const fn observation_predecessor_required(&self) -> bool {
        self.observation_predecessor_required
    }

    /// Return the exhaustive selected-sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.evidence.completion().sample_count()
    }

    /// Return the exhaustive replay block count.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.evidence.completion().block_count()
    }

    /// Consume the envelope into its intact reconstruction evidence for the
    /// Major-Cycle owner; the pairing is never split outside this crate.
    #[must_use]
    pub(crate) fn into_evidence(self) -> CompleteDataOwnerResult {
        self.evidence
    }
}

/// Streaming owner for one serial CPU constant-basis MFS execution.
///
/// This boundary exposes no raw weighting configuration. Its only data input is
/// the T18-branded weighted block, and completion requires T18's terminal replay
/// proof.
///
/// Raw selected samples are not accepted at this boundary:
///
/// ```compile_fail
/// use casa_imaging_model::SelectedObservationSample;
/// use casa_imaging_runtime::SpectralOperatorState;
///
/// fn bypass(mut state: SpectralOperatorState, raw: &SelectedObservationSample) {
///     let _ = state.consume_weighted_block(raw);
/// }
/// ```
#[derive(Debug)]
pub struct SpectralOperatorState {
    state: CompleteDataOwnerState,
    binding: CompleteDataExecutionBinding,
}

#[derive(Debug)]
struct CompleteDataExecutionBinding {
    problem: CompiledProblemId,
    attempt: ExecutionAttemptId,
    replay_node: WorkNodeId,
    reconciliation_node: WorkNodeId,
    lease_epoch: u64,
    observation_predecessor_required: bool,
}

impl SpectralOperatorState {
    pub(crate) fn authorize_derived_coverage(
        &mut self,
        proof: casa_imaging_reconstruction::FrozenWeightingCoverageProof,
    ) -> Result<(), CompleteDataOperatorError> {
        self.state
            .authorize_derived_coverage(proof)
            .map_err(CompleteDataOperatorError::Owner)
    }

    /// Request final selected visibility samples from this bounded replay.
    pub(crate) fn enable_final_visibility_samples(&mut self) {
        self.state.enable_final_visibility_samples();
    }

    /// Bind one validated final model before consuming the exhaustive replay.
    pub fn bind_major_cycle_model(
        &mut self,
        preparation: &MajorCyclePreparation,
        prior_normal_state: Option<FinalNormalState>,
    ) -> Result<(), CompleteDataOperatorError> {
        self.state
            .bind_major_cycle_model(preparation.final_model(), prior_normal_state)
            .map_err(CompleteDataOperatorError::Owner)
    }

    pub(crate) fn bind_selected_output_model(
        &mut self,
        model: &casa_imaging_reconstruction::ModelGeneration,
    ) -> Result<(), CompleteDataOperatorError> {
        self.state
            .bind_selected_output_model(model)
            .map_err(CompleteDataOperatorError::Owner)
    }

    /// Consume one ordered T18 weighted block synchronously.
    pub fn consume_weighted_block(
        &mut self,
        block: &WeightedObservationBlock,
    ) -> Result<
        &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
        CompleteDataOperatorError,
    > {
        if Some(block.weighting_generation()) != self.state.weighting_generation() {
            return Err(CompleteDataOperatorError::WeightingGeneration);
        }
        Ok(self.state.consume_block(block.reconstruction_block())?)
    }

    /// Predict one bounded T18 block through the same plan-authorized A operator.
    pub fn predict_weighted_block(
        &mut self,
        model: &[num_complex::Complex64],
        block: &WeightedObservationBlock,
    ) -> Result<&[num_complex::Complex64], CompleteDataOperatorError> {
        if Some(block.weighting_generation()) != self.state.weighting_generation() {
            return Err(CompleteDataOperatorError::WeightingGeneration);
        }
        Ok(self
            .state
            .predict_block(model, block.reconstruction_block())?)
    }

    pub(crate) fn consume_bounded_replay_chunk(
        &mut self,
        block: &casa_imaging_reconstruction::WeightingReplayChunk,
    ) -> Result<
        &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
        CompleteDataOperatorError,
    > {
        Ok(self.state.consume_block(block)?)
    }

    pub(crate) fn predict_final_visibility_chunk(
        &mut self,
        block: &casa_imaging_reconstruction::WeightingReplayChunk,
    ) -> Result<
        &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
        CompleteDataOperatorError,
    > {
        Ok(self.state.predict_final_visibility_block(block)?)
    }

    /// Consume terminal T18 proof and mint the runtime complete-data envelope.
    ///
    /// The reconstruction evidence stays inseparably paired inside the
    /// envelope together with the attempt, lease, replay node, and
    /// plan-authoritative reconciliation node that produced it.
    pub fn complete(
        self,
        replay: &WeightingReplayCompletion,
    ) -> Result<CompleteDataOperatorResult, CompleteDataOperatorError> {
        if self.binding.problem != replay.problem_id()
            || self.binding.attempt != replay.attempt_id()
            || self.binding.replay_node != *replay.owner_node()
            || self.binding.lease_epoch != replay.lease_epoch()
        {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        let evidence = self.state.complete(
            replay.reconstruction_summary(),
            replay.selected_generation(),
            replay
                .continuum_transform()
                .map(|completion| completion.generation_id()),
        )?;
        Ok(CompleteDataOperatorResult {
            evidence,
            attempt: self.binding.attempt,
            replay_node: self.binding.replay_node,
            reconciliation_node: self.binding.reconciliation_node,
            lease_epoch: self.binding.lease_epoch,
            observation_predecessor_required: self.binding.observation_predecessor_required,
        })
    }
}

/// Exact reason T19 rejected an operator problem, block, or terminal proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompleteDataOperatorError {
    /// Blocks or terminal proof disagree on the frozen W generation.
    WeightingGeneration,
    /// Terminal T18 proof does not match the plan capability that began T19.
    ExecutionBinding,
    /// Reconstruction rejected a numerical plan or weighted contribution.
    Owner(SpectralOperatorError),
}

impl fmt::Display for CompleteDataOperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WeightingGeneration => "weighted replay generations do not match",
            Self::ExecutionBinding => "weighted replay completion changed T19 execution authority",
            Self::Owner(error) => return error.fmt(formatter),
        })
    }
}

impl Error for CompleteDataOperatorError {}

impl From<SpectralOperatorError> for CompleteDataOperatorError {
    fn from(error: SpectralOperatorError) -> Self {
        Self::Owner(error)
    }
}
