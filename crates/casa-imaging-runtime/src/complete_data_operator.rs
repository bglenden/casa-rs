// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime ownership boundary for the first complete-data continuum operator.

use std::{collections::BTreeSet, error::Error, fmt, mem::align_of};

use casa_imaging_model::{CompiledProblem, CorrelationType, SelectedVisibilitySample};
use casa_imaging_reconstruction::{
    CompleteDataResult, CompleteDataState, SerialMfsError, SerialMfsPlan, SerialMfsSample,
};

use crate::{
    AllocationAccess, AllocationId, AllocationLayout, AllocationLifetime, AllocationPurpose,
    AllocationUse, AlternativeId, CapacityDomainId, CapacityViewId, ClaimLifetime, ExecutionDag,
    ExecutionDagSpecification, ExecutionError, FenceId, FenceKind, InitializationPolicy,
    LeaseResource, LogicalAllocation, MemoryDemand, PhysicalSlot, PhysicalSlotId,
    PhysicalWorkBinding, PhysicalWorkBindingError, SlotCompatibility, StorageMode,
    WeightedObservationBlock, WeightingReplayCompletion, WorkDependency, WorkExecutionContext,
    WorkNodeId,
};

/// Hard physical allocations attached to the exact T18 replay node.
pub struct CompleteDataPlanFragment<'a> {
    plan: &'a SerialMfsPlan,
    replay_node: WorkNodeId,
}

impl<'a> CompleteDataPlanFragment<'a> {
    /// Bind the serial operator plan to the T18 replay that emits its only input.
    #[must_use]
    pub const fn new(plan: &'a SerialMfsPlan, replay_node: WorkNodeId) -> Self {
        Self { plan, replay_node }
    }

    /// Begin execution only from the exact replay node with every T19 allocation live.
    pub fn begin(
        &self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
    ) -> Result<SerialMfsOperatorState, CompleteDataPlanError> {
        if context.node().id != self.replay_node {
            return Err(CompleteDataPlanError::WrongExecutionNode);
        }
        if &SerialMfsPlan::new(problem)? != self.plan {
            return Err(CompleteDataPlanError::PlanMismatch);
        }
        let shape = self.plan.grid_shape();
        let residency = self.plan.residency();
        let required = [
            (
                format!("serial-mfs-grids-{}x{}", shape[0], shape[1]),
                residency.grid_bytes(),
            ),
            (
                format!("serial-mfs-fft-scratch-{}x{}", shape[0], shape[1]),
                residency.fft_scratch_bytes(),
            ),
            (
                format!("serial-mfs-primitives-{}x{}", shape[0], shape[1]),
                residency.primitive_output_bytes(),
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
        SerialMfsOperatorState::new(problem).map_err(CompleteDataPlanError::Operator)
    }

    /// Add shared grids, FFT scratch, and primitive outputs to physical work.
    pub fn compose(
        &self,
        base: &PhysicalWorkBinding,
    ) -> Result<PhysicalWorkBinding, CompleteDataPlanError> {
        let reconciliation = base.observation_transaction().final_reconciliation();
        if !base.execution_dag().nodes().contains_key(&self.replay_node) {
            return Err(CompleteDataPlanError::MissingReplayNode);
        }
        let specs = self.allocation_specs(reconciliation)?;
        let replay_fence = ClaimLifetime::through_fence(FenceKind::Io);
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
        replay.allocations.extend([
            specs[0].usage(replay_fence.clone()),
            specs[1].usage(replay_fence.clone()),
            specs[2].usage(replay_fence),
        ]);
        nodes
            .iter_mut()
            .find(|node| &node.id == reconciliation)
            .ok_or(CompleteDataPlanError::MissingReconciliationNode)?
            .allocations
            .push(specs[2].usage(ClaimLifetime::Work));

        let mut alternative = base.execution_dag().resource_alternative().clone();
        alternative.id =
            AlternativeId::new(format!("{}-serial-mfs-nterms1", alternative.id.as_str()));
        alternative
            .demand
            .memory
            .extend(specs.iter().map(CompleteDataAllocation::memory_demand));
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
        Ok(PhysicalWorkBinding::with_implementation_contract(
            base.implementation_contract().for_execution_dag(&dag)?,
            dag,
            base.prediction().clone(),
            base.artifacts().to_vec(),
            base.observation_transaction().clone(),
            base.publication_layouts().clone(),
        )?)
    }

    fn allocation_specs(
        &self,
        reconciliation: &WorkNodeId,
    ) -> Result<[CompleteDataAllocation; 3], CompleteDataPlanError> {
        let suffix = self.plan.grid_shape();
        let residency = self.plan.residency();
        let replay_done = BTreeSet::from([WorkDependency::Fence(FenceId::new(
            self.replay_node.clone(),
            FenceKind::Io,
        ))]);
        let reconciled = BTreeSet::from([WorkDependency::Work(reconciliation.clone())]);
        Ok([
            CompleteDataAllocation::new(
                format!("serial-mfs-grids-{}x{}", suffix[0], suffix[1]),
                residency.grid_bytes(),
                "serial-mfs-shared-dirty-psf-grids",
                InitializationPolicy::ZeroBeforeRead,
                self.replay_node.clone(),
                replay_done.clone(),
            )?,
            CompleteDataAllocation::new(
                format!("serial-mfs-fft-scratch-{}x{}", suffix[0], suffix[1]),
                residency.fft_scratch_bytes(),
                "serial-mfs-rustfft-scratch",
                InitializationPolicy::OverwriteBeforeRead,
                self.replay_node.clone(),
                replay_done,
            )?,
            CompleteDataAllocation::new(
                format!("serial-mfs-primitives-{}x{}", suffix[0], suffix[1]),
                residency.primitive_output_bytes(),
                "serial-mfs-unnormalized-primitives",
                InitializationPolicy::OverwriteBeforeRead,
                self.replay_node.clone(),
                reconciled,
            )?,
        ])
    }
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
    /// Execution was attempted outside the plan-bound replay node.
    WrongExecutionNode,
    /// The runtime problem no longer matches the planned operator.
    PlanMismatch,
    /// A required shared grid, FFT, or output allocation capability is absent.
    MissingAllocationCapability,
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
            Self::WrongExecutionNode => {
                formatter.write_str("T19 can execute only at its planned replay node")
            }
            Self::PlanMismatch => {
                formatter.write_str("T19 execution problem does not match its physical plan")
            }
            Self::MissingAllocationCapability => {
                formatter.write_str("T19 execution lacks an exact planned allocation capability")
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

/// Opaque owner-minted proof that one complete weighted replay reached the operator.
///
/// The completion is deliberately not constructible from caller digests or a
/// generic runtime completion. It is minted only by consuming a
/// [`SerialMfsOperatorState`] after that state has accepted the complete ordered
/// stream of [`WeightedObservationBlock`] values and the terminal
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
/// use casa_imaging_reconstruction::CompleteDataCompletion;
///
/// let _ = CompleteDataCompletion {};
/// ```
pub type CompleteDataOperatorResult = CompleteDataResult;

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
    state: CompleteDataState,
}

impl SerialMfsOperatorState {
    /// Start an operator only for the T19 single-field Stokes-I nterms=1 surface.
    fn new(problem: &CompiledProblem) -> Result<Self, CompleteDataOperatorError> {
        Ok(Self {
            state: CompleteDataState::new(problem)?,
        })
    }

    /// Consume one ordered T18 weighted block synchronously.
    pub fn consume_weighted_block(
        &mut self,
        block: &WeightedObservationBlock,
    ) -> Result<(), CompleteDataOperatorError> {
        let mut block_samples = 0_u64;
        let mut samples = Vec::new();
        for weighted in block.samples() {
            block_samples = block_samples
                .checked_add(1)
                .ok_or(CompleteDataOperatorError::CoverageOverflow)?;
            let selected = weighted.selected();
            if !stokes_i_parallel_hand(selected.address.correlation_type) {
                continue;
            }
            let visibility = match selected.visibility {
                SelectedVisibilitySample::Float32(value) => [f64::from(value), 0.0],
                SelectedVisibilitySample::Complex32([real, imaginary]) => {
                    [f64::from(real), f64::from(imaginary)]
                }
            };
            for spectral in weighted.spectral_values() {
                let contribution = spectral.contribution();
                samples.push(SerialMfsSample::new(
                    selected.coordinates.transformed_uvw_m,
                    selected.address.frequency_centre_hz,
                    selected.coordinates.phase_shift_m,
                    visibility,
                    spectral.imaging_weight(),
                    f64::from(contribution.factor()),
                )?);
            }
        }
        self.state.consume_block(
            block.weighting_generation(),
            block.sequence(),
            block_samples,
            samples,
        )?;
        Ok(())
    }

    /// Consume terminal T18 proof and mint the complete-data operator completion.
    pub fn complete(
        self,
        replay: &WeightingReplayCompletion,
    ) -> Result<CompleteDataOperatorResult, CompleteDataOperatorError> {
        Ok(self.state.complete(
            replay.reconstruction_summary(),
            replay.selected_generation(),
        )?)
    }
}

fn stokes_i_parallel_hand(correlation: CorrelationType) -> bool {
    matches!(
        correlation,
        CorrelationType::StokesI
            | CorrelationType::LinearXx
            | CorrelationType::LinearYy
            | CorrelationType::CircularRr
            | CorrelationType::CircularLl
    )
}

/// Exact reason T19 rejected an operator problem, block, or terminal proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompleteDataOperatorError {
    /// The Compiled Problem is outside single-field scalar Stokes-I nterms=1.
    UnsupportedProblem,
    /// Replay blocks were missing, repeated, or reordered.
    BlockSequence,
    /// Blocks or terminal proof disagree on the frozen W generation.
    WeightingGeneration,
    /// Sample or block counts exceeded the supported identity domain.
    CoverageOverflow,
    /// Terminal replay proof does not cover every block accepted by the operator.
    IncompleteCoverage,
    /// Reconstruction rejected a numerical plan or weighted contribution.
    Owner(SerialMfsError),
}

impl fmt::Display for CompleteDataOperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedProblem => {
                "serial MFS requires one scalar-response Stokes-I constant-basis domain"
            }
            Self::BlockSequence => "weighted replay blocks are not exhaustive and ordered",
            Self::WeightingGeneration => "weighted replay generations do not match",
            Self::CoverageOverflow => "weighted replay coverage overflowed",
            Self::IncompleteCoverage => "terminal replay proof does not match consumed coverage",
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
