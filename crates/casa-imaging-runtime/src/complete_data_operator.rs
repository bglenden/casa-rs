// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime ownership boundary for the first complete-data continuum operator.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    mem::{align_of, size_of},
};

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, ModelDeltaTerm, ModelSample,
    NumericsContractId, SelectedObservationGenerationId, WeightingCommitmentId,
};
use casa_imaging_reconstruction::{
    ContinuumPrimitiveCatalog, MajorCyclePreparation, SerialMfsError, SerialMfsPrimitives,
    SerialMfsSpecification, WeightingAlgorithmState, WeightingGenerationId,
    WeightingReplayCoverageId, WeightingReplayId,
    runtime_adapter::{
        CompleteDataOwnerResult, CompleteDataOwnerState, PreparedSerialMfsOperator,
        SerialMfsWorkload, prepare_serial_mfs_operator, serial_mfs_workload,
    },
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
    /// Bytes for dirty, PSF, and exact residual accumulation plus compensation grids.
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

    /// Bytes retained by dirty, PSF, exact residual, and sensitivity primitives.
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
    specification: SerialMfsSpecification,
    workload: SerialMfsWorkload,
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
    ) -> Result<Self, CompleteDataPlanError> {
        let specification = SerialMfsSpecification::new(problem)?;
        let workload = serial_mfs_workload(&specification, max_replay_block_samples)?;
        let residency = project_residency(problem, workload)?;
        let shape = workload.grid_shape();
        Ok(Self {
            specification,
            workload,
            residency,
            preparation_node: WorkNodeId::new(format!(
                "serial-mfs-fft-plan-{}x{}",
                shape[0], shape[1]
            )),
            replay_node,
            reconciliation_node: None,
        })
    }

    /// Return the exact FFT-planning node inserted before replay.
    #[must_use]
    pub const fn preparation_node(&self) -> &WorkNodeId {
        &self.preparation_node
    }

    /// Return the runtime-owned resident-byte projection.
    #[must_use]
    pub const fn residency(&self) -> CompleteDataResidency {
        self.residency
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
            owner: prepare_serial_mfs_operator(self.specification.clone(), self.workload)?,
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
    ) -> Result<SerialMfsOperatorState, CompleteDataPlanError> {
        if context.node().id != self.replay_node {
            return Err(CompleteDataPlanError::WrongExecutionNode);
        }
        if context.compiled().problem_id() != problem.problem_id()
            || SerialMfsSpecification::new(problem)? != self.specification
            || weighting.max_replay_block_samples() != self.workload.max_replay_block_samples()
        {
            return Err(CompleteDataPlanError::PlanMismatch);
        }
        self.validate_allocations(context)?;
        prepared.begin(context, problem, weighting, self)
    }

    fn validate_allocations(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), CompleteDataPlanError> {
        let shape = self.workload.grid_shape();
        let residency = self.residency;
        let required = [
            (
                format!("serial-mfs-grids-{}x{}", shape[0], shape[1]),
                residency.grid_bytes(),
            ),
            (
                format!("serial-mfs-convolution-cache-{}x{}", shape[0], shape[1]),
                residency.convolution_cache_bytes(),
            ),
            (
                format!("serial-mfs-fft-state-{}x{}", shape[0], shape[1]),
                residency.fft_resident_bytes(),
            ),
            (
                format!("serial-mfs-forward-workspace-{}x{}", shape[0], shape[1]),
                residency.forward_workspace_bytes(),
            ),
            (
                format!("serial-mfs-primitives-{}x{}", shape[0], shape[1]),
                residency.primitive_output_bytes(),
            ),
            (
                format!("serial-mfs-major-cycle-model-{}x{}", shape[0], shape[1]),
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
        let shape = self.workload.grid_shape();
        let allocation = format!("serial-mfs-fft-state-{}x{}", shape[0], shape[1]);
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
    /// transaction's final-reconciliation node: afterwards, reconciliation may
    /// execute only at that exact plan-authoritative Compute node.
    ///
    /// Returns the composed physical work together with this fragment bound to
    /// its authoritative reconciliation node.
    pub fn compose(
        mut self,
        base: &PhysicalWorkBinding,
    ) -> Result<(PhysicalWorkBinding, Self), CompleteDataPlanError> {
        let reconciliation = base.observation_transaction().final_reconciliation();
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
            allocations: vec![
                specs[2].usage(ClaimLifetime::Work),
                specs[5].usage(ClaimLifetime::Work),
            ],
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
            AlternativeId::new(format!("{}-serial-mfs-nterms1", alternative.id.as_str()));
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
        )?;
        Ok((physical, self))
    }

    fn allocation_specs(
        &self,
        reconciliation: &WorkNodeId,
    ) -> Result<[CompleteDataAllocation; 6], CompleteDataPlanError> {
        let suffix = self.workload.grid_shape();
        let residency = self.residency;
        let replay_done = BTreeSet::from([WorkDependency::Fence(FenceId::new(
            self.replay_node.clone(),
            FenceKind::Io,
        ))]);
        let reconciled = BTreeSet::from([WorkDependency::Work(reconciliation.clone())]);
        Ok([
            CompleteDataAllocation::new(
                format!("serial-mfs-grids-{}x{}", suffix[0], suffix[1]),
                residency.grid_bytes(),
                "serial-mfs-shared-dirty-psf-residual-grids",
                InitializationPolicy::ZeroBeforeRead,
                self.replay_node.clone(),
                replay_done.clone(),
            )?,
            CompleteDataAllocation::new(
                format!("serial-mfs-convolution-cache-{}x{}", suffix[0], suffix[1]),
                residency.convolution_cache_bytes(),
                "serial-mfs-convolution-taps-and-corrections",
                InitializationPolicy::OverwriteBeforeRead,
                self.replay_node.clone(),
                replay_done.clone(),
            )?,
            CompleteDataAllocation::new(
                format!("serial-mfs-fft-state-{}x{}", suffix[0], suffix[1]),
                residency.fft_resident_bytes(),
                "serial-mfs-rustfft-plans-lane-and-scratch",
                InitializationPolicy::OverwriteBeforeRead,
                self.preparation_node.clone(),
                replay_done.clone(),
            )?,
            CompleteDataAllocation::new(
                format!("serial-mfs-forward-workspace-{}x{}", suffix[0], suffix[1]),
                residency.forward_workspace_bytes(),
                "serial-mfs-forward-grid-and-bounded-predictions",
                InitializationPolicy::OverwriteBeforeRead,
                self.replay_node.clone(),
                replay_done,
            )?,
            CompleteDataAllocation::new(
                format!("serial-mfs-primitives-{}x{}", suffix[0], suffix[1]),
                residency.primitive_output_bytes(),
                "serial-mfs-unnormalized-dirty-psf-residual-primitives",
                InitializationPolicy::OverwriteBeforeRead,
                self.replay_node.clone(),
                reconciled,
            )?,
            CompleteDataAllocation::new(
                format!("serial-mfs-major-cycle-model-{}x{}", suffix[0], suffix[1]),
                residency.major_cycle_model_bytes(),
                "serial-mfs-current-final-model-and-pending-delta",
                InitializationPolicy::OverwriteBeforeRead,
                self.preparation_node.clone(),
                BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
            )?,
        ])
    }
}

fn project_residency(
    problem: &CompiledProblem,
    workload: SerialMfsWorkload,
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
    let model_samples = model.target().sample_count();
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

impl From<SerialMfsError> for CompleteDataPlanError {
    fn from(error: SerialMfsError) -> Self {
        Self::Operator(CompleteDataOperatorError::Owner(error))
    }
}

/// Opaque prepared FFT state retained from the explicit planning node to replay.
#[doc(hidden)]
#[derive(Debug)]
pub struct CompleteDataPreparedState {
    owner: PreparedSerialMfsOperator,
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
    ) -> Result<SerialMfsOperatorState, CompleteDataPlanError> {
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
        Ok(SerialMfsOperatorState {
            state,
            binding: CompleteDataExecutionBinding {
                problem: problem.problem_id(),
                attempt: context.attempt_id(),
                replay_node: context.node().id.clone(),
                reconciliation_node,
                lease_epoch: context.lease_epoch(),
            },
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
/// [`SerialMfsOperatorState`] after that state has accepted the complete
/// ordered stream of [`WeightedObservationBlock`] values and the terminal
/// [`WeightingReplayCompletion`].
///
/// A caller cannot substitute a generic scheduler completion:
///
/// ```compile_fail
/// use casa_imaging_runtime::{
///     AttemptBoundObservationCompletion, SerialMfsOperatorState,
/// };
///
/// fn substitute(
///     state: SerialMfsOperatorState,
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
}

impl CompleteDataOperatorResult {
    /// Return reconstruction-owned unnormalized primitives.
    #[must_use]
    pub const fn primitives(&self) -> &SerialMfsPrimitives {
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
    pub const fn primitive_catalog(&self) -> ContinuumPrimitiveCatalog {
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
/// use casa_imaging_runtime::SerialMfsOperatorState;
///
/// fn bypass(mut state: SerialMfsOperatorState, raw: &SelectedObservationSample) {
///     let _ = state.consume_weighted_block(raw);
/// }
/// ```
#[derive(Debug)]
pub struct SerialMfsOperatorState {
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
}

impl SerialMfsOperatorState {
    /// Bind one validated final model before consuming the exhaustive replay.
    pub fn bind_major_cycle_model(
        &mut self,
        preparation: &MajorCyclePreparation,
    ) -> Result<(), CompleteDataOperatorError> {
        self.state
            .bind_major_cycle_model(preparation.final_model())
            .map_err(CompleteDataOperatorError::Owner)
    }

    /// Consume one ordered T18 weighted block synchronously.
    pub fn consume_weighted_block(
        &mut self,
        block: &WeightedObservationBlock,
    ) -> Result<(), CompleteDataOperatorError> {
        if block.weighting_generation() != self.state.weighting_generation() {
            return Err(CompleteDataOperatorError::WeightingGeneration);
        }
        self.state.consume_block(block.reconstruction_block())?;
        Ok(())
    }

    /// Predict one bounded T18 block through the same plan-authorized A operator.
    pub fn predict_weighted_block(
        &mut self,
        model: &[num_complex::Complex64],
        block: &WeightedObservationBlock,
    ) -> Result<&[num_complex::Complex64], CompleteDataOperatorError> {
        if block.weighting_generation() != self.state.weighting_generation() {
            return Err(CompleteDataOperatorError::WeightingGeneration);
        }
        Ok(self
            .state
            .predict_block(model, block.reconstruction_block())?)
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
        )?;
        Ok(CompleteDataOperatorResult {
            evidence,
            attempt: self.binding.attempt,
            replay_node: self.binding.replay_node,
            reconciliation_node: self.binding.reconciliation_node,
            lease_epoch: self.binding.lease_epoch,
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
    Owner(SerialMfsError),
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

impl From<SerialMfsError> for CompleteDataOperatorError {
    fn from(error: SerialMfsError) -> Self {
        Self::Owner(error)
    }
}
