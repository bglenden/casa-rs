// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime composition of reconstruction phases with opaque T17 traversal evidence.

use std::{collections::BTreeSet, error::Error, fmt, mem::align_of};

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, SelectedObservationGenerationId, SelectedObservationSample,
    SelectedSpectralContribution,
};
use casa_imaging_reconstruction::{
    WeightingAlgorithmState, WeightingError, WeightingGenerationId, WeightingPlan,
    WeightingReplayChunk as ReconstructionWeightedBlock, WeightingReplayCoverageId,
    WeightingReplayId, WeightingReplaySummary, WeightingResidency,
    WeightingSampleValue as ReconstructionWeightedSample,
    WeightingSpectralValue as ReconstructionWeightedSpectralValue, begin_weighting_generation,
};
use casa_ms::{
    BoundSelectedObservation, SelectedObservationCompletion,
    SelectedObservationResidencyCertificate, SelectedObservationTraversalError,
    SelectedObservationTraversalSample,
};

use crate::{
    AllocationAccess, AllocationId, AllocationLayout, AllocationLifetime, AllocationPurpose,
    AllocationUse, AlternativeId, AttemptBoundObservationCompletion, CapacityDomainId,
    CapacityViewId, ClaimLifetime, ExecutionAttemptId, ExecutionDag, ExecutionDagSpecification,
    ExecutionError, FenceId, FenceKind, InitializationPolicy, IoBufferKind, IoPrediction,
    LeaseResource, LogicalAllocation, MemoryDemand, ObservationCompletionBindingError,
    ObservationReadCompletionContext, PhysicalSlot, PhysicalSlotId, PhysicalWorkBinding,
    PhysicalWorkBindingError, PlanPrediction, ResourceClaim, SlotCompatibility, StagePrediction,
    StorageMode, WorkDependency, WorkDomain, WorkExecutionContext, WorkImplementationId, WorkKind,
    WorkNode, WorkNodeId,
};

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
}

impl<'a> WeightingPlanFragment<'a> {
    /// Bind one reconstruction plan to its direct T17 predecessor and selected implementations.
    #[must_use]
    pub fn new(
        plan: &'a WeightingPlan,
        source_read: WorkNodeId,
        source_resources: SelectedObservationSourceResources,
        generation_implementation: WorkImplementationId,
        replay_implementation: WorkImplementationId,
        release_implementation: WorkImplementationId,
    ) -> Self {
        Self {
            plan,
            source_read,
            source_resources,
            generation_implementation,
            replay_implementation,
            release_implementation,
            ids: WeightingPlanIds::new(plan),
        }
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
        let reconciliation_id = base.observation_transaction().final_reconciliation();
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
                    allocation_use(&self.ids.reduction_allocation, io_lifetime.clone()),
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
            dependencies: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                self.ids.generation_node.clone(),
                FenceKind::Io,
            ))]),
            claims: read_claims.clone(),
            allocations: read_allocations
                .into_iter()
                .chain([
                    allocation_use(&self.ids.frozen_allocation, io_lifetime.clone()),
                    allocation_use(&self.ids.replay_read_allocation, io_lifetime.clone()),
                    allocation_use(&self.ids.weighted_block_allocation, io_lifetime.clone()),
                ])
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

    fn allocation_specs(&self) -> Result<[AllocationSpec; 5], WeightingPlanFragmentError> {
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
        Ok([
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
                residency.deterministic_partial_bytes(),
                "weighting-density-partials",
                self.ids.generation_node.clone(),
                generation_fence.clone(),
            )?,
            AllocationSpec::new(
                self.ids.reduction_allocation.clone(),
                self.ids.reduction_slot.clone(),
                residency.reduction_scratch_bytes(),
                "weighting-exact-reduction",
                self.ids.generation_node.clone(),
                generation_fence,
            )?,
            AllocationSpec::new(
                self.ids.replay_read_allocation.clone(),
                self.ids.replay_read_slot.clone(),
                residency.replay_read_bytes(),
                "weighting-replay-read",
                self.ids.replay_node.clone(),
                replay_fence.clone(),
            )?,
            AllocationSpec::new(
                self.ids.weighted_block_allocation.clone(),
                self.ids.weighted_block_slot.clone(),
                residency.weighted_block_bytes(),
                "weighting-weighted-block",
                self.ids.replay_node.clone(),
                replay_fence,
            )?,
        ])
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
        let (expected_node, expected) = if context.node().id == self.ids.generation_node {
            (&self.ids.generation_node, [&specs[0], &specs[1], &specs[2]])
        } else if context.node().id == self.ids.replay_node {
            (&self.ids.replay_node, [&specs[0], &specs[3], &specs[4]])
        } else {
            return Err(WeightingEvidenceError);
        };
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

    fn authorize_generation(
        &self,
        context: WorkExecutionContext<'_>,
        problem: &CompiledProblem,
    ) -> Result<WeightingGenerationBinding, WeightingEvidenceError> {
        let specs = self
            .allocation_specs()
            .map_err(|_| WeightingEvidenceError)?;
        let expected = [&specs[0], &specs[1], &specs[2]];
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
        let expected = [&specs[0], &specs[3], &specs[4]];
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
            || validate_generation_completions(
                &frozen.density_completion,
                predecessor.owner_completion(),
            )
            .is_err()
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
    PendingGeneration(PendingWeightingGeneration),
    Frozen(FrozenWeightingGeneration),
    PendingReplay {
        frozen: FrozenWeightingGeneration,
        pending: PendingWeightingReplay,
    },
    Replayed {
        frozen: FrozenWeightingGeneration,
        completion: WeightingReplayCompletion,
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
    ) -> Result<crate::SerialMfsOperatorState, crate::CompleteDataPlanError> {
        let WeightingExecutionPhase::Frozen(frozen) = &self.phase else {
            return Err(crate::CompleteDataPlanError::MissingFrozenWeighting);
        };
        fragment.begin(context, problem, &frozen.state, prepared)
    }

    /// Construct an empty lifecycle before the generation node is dispatched.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            phase: WeightingExecutionPhase::Empty,
            retained_observation: None,
        }
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
        consume: impl FnMut(SelectedObservationTraversalSample) -> Result<(), E>,
    ) -> Result<SelectedObservationCompletion, WeightingSourceTraversalError<E>>
    where
        E: Error + 'static,
    {
        if !matches!(self.phase, WeightingExecutionPhase::Empty)
            || self.retained_observation.is_some()
            || context.node().id != fragment.source_read
            || context.node().kind != WorkKind::ObservationRead
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

    /// Drive the two owner traversals under generation-node authority.
    pub fn traverse_generation(
        &mut self,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        problem: &CompiledProblem,
    ) -> Result<(), WeightingGenerationError> {
        if !matches!(self.phase, WeightingExecutionPhase::Empty) {
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
        self.phase = WeightingExecutionPhase::PendingGeneration(traverse_weighting_generation(
            context,
            fragment,
            &mut retained.selected,
            problem,
        )?);
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
        let (frozen, completion) = complete_weighting_generation(pending, context)?;
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
                self.phase = WeightingExecutionPhase::PendingReplay { frozen, pending };
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
        match pending.bind(context) {
            Ok((completion, predecessor)) => {
                self.phase = WeightingExecutionPhase::Replayed { frozen, completion };
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
        Ok(())
    }

    /// Return whether the planned release has consumed all externally retained state.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        matches!(self.phase, WeightingExecutionPhase::Empty) && self.retained_observation.is_none()
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
        match &self.phase {
            WeightingExecutionPhase::Empty => context.is_cleanup(),
            WeightingExecutionPhase::PendingGeneration(pending) => {
                valid_binding(&pending.binding, fragment.generation_node())
                    && pending.state.commitment_id() == fragment.plan.commitment_id()
            }
            WeightingExecutionPhase::Frozen(frozen) => {
                valid_binding(&frozen.binding, fragment.generation_node())
                    && frozen.state.commitment_id() == fragment.plan.commitment_id()
            }
            WeightingExecutionPhase::PendingReplay { frozen, pending } => {
                valid_binding(&frozen.binding, fragment.generation_node())
                    && valid_binding(&pending.binding, fragment.replay_node())
                    && frozen.state.commitment_id() == fragment.plan.commitment_id()
                    && pending.state.weighting_generation() == frozen.state.generation_id()
            }
            WeightingExecutionPhase::Replayed { frozen, completion } => {
                valid_binding(&frozen.binding, fragment.generation_node())
                    && valid_binding(&completion.binding, fragment.replay_node())
                    && frozen.state.commitment_id() == fragment.plan.commitment_id()
                    && completion.state.weighting_generation() == frozen.state.generation_id()
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
    replay_read_allocation: AllocationId,
    weighted_block_allocation: AllocationId,
    frozen_slot: PhysicalSlotId,
    partial_slot: PhysicalSlotId,
    reduction_slot: PhysicalSlotId,
    replay_read_slot: PhysicalSlotId,
    weighted_block_slot: PhysicalSlotId,
}

impl WeightingPlanIds {
    fn new(plan: &WeightingPlan) -> Self {
        let suffix = plan.commitment_id().to_string();
        Self {
            generation_node: WorkNodeId::new(format!("weighting-generation-{suffix}")),
            replay_node: WorkNodeId::new(format!("weighting-replay-{suffix}")),
            release_node: WorkNodeId::new(format!("weighting-release-{suffix}")),
            frozen_allocation: AllocationId::new(format!("weighting-frozen-{suffix}")),
            partial_allocation: AllocationId::new(format!("weighting-partials-{suffix}")),
            reduction_allocation: AllocationId::new(format!("weighting-reduction-{suffix}")),
            replay_read_allocation: AllocationId::new(format!("weighting-replay-read-{suffix}")),
            weighted_block_allocation: AllocationId::new(format!(
                "weighting-weighted-block-{suffix}"
            )),
            frozen_slot: PhysicalSlotId::new(format!("weighting-frozen-slot-{suffix}")),
            partial_slot: PhysicalSlotId::new(format!("weighting-partials-slot-{suffix}")),
            reduction_slot: PhysicalSlotId::new(format!("weighting-reduction-slot-{suffix}")),
            replay_read_slot: PhysicalSlotId::new(format!("weighting-replay-read-slot-{suffix}")),
            weighted_block_slot: PhysicalSlotId::new(format!(
                "weighting-weighted-block-slot-{suffix}"
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
    if &context.node().id != expected_node
        || context.node().kind != expected_kind
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
    pub const fn selected(&self) -> &SelectedObservationSample {
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
    state: WeightingAlgorithmState,
    density_completion: SelectedObservationCompletion,
    binding: WeightingGenerationBinding,
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
    const fn generation_id(&self) -> WeightingGenerationId {
        self.state.generation_id()
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
            .state
            .begin_replay(problem, fragment.plan)
            .map_err(WeightingReplayError::Owner)?;
        let owner_completion = selected
            .traverse(problem, |reported| {
                if let Some(block) = phase
                    .consume(
                        problem,
                        *reported.selected(),
                        reported.spectral_contributions(),
                    )
                    .map_err(ReplayCallbackError::Owner)?
                {
                    let block = WeightedObservationBlock::authorize(self.generation_id(), block);
                    emit(&block).map_err(ReplayCallbackError::Consumer)?;
                }
                Ok(())
            })
            .map_err(WeightingReplayError::Traversal)?;
        validate_replay_completion(
            &self.density_completion,
            predecessor.owner_completion(),
            &owner_completion,
            &self.state,
        )
        .map_err(WeightingReplayError::Evidence)?;
        let (final_block, state) = phase.finish().map_err(WeightingReplayError::Owner)?;
        if let Some(block) = final_block {
            let block = WeightedObservationBlock::authorize(self.generation_id(), block);
            emit(&block).map_err(WeightingReplayError::Consumer)?;
        }
        Ok(PendingWeightingReplay {
            state,
            owner_completion,
            binding: replay_binding,
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
    let density_completion = selected
        .traverse(problem, |reported| {
            density.consume(
                problem,
                *reported.selected(),
                reported.spectral_contributions(),
            )
        })
        .map_err(WeightingGenerationError::DensityTraversal)?;
    let sum_weight = density
        .finish(problem)
        .map_err(WeightingGenerationError::Owner)?;
    let mut sum_weight = sum_weight;
    let sum_weight_completion = selected
        .traverse(problem, |reported| {
            sum_weight.consume(
                problem,
                *reported.selected(),
                reported.spectral_contributions(),
            )
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
            state: pending.state,
            density_completion: pending.density_completion,
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
    density: &SelectedObservationCompletion,
    prior: &SelectedObservationCompletion,
    replay: &SelectedObservationCompletion,
    state: &WeightingAlgorithmState,
) -> Result<(), WeightingEvidenceError> {
    if validate_generation_completions(density, prior).is_err()
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
    /// Opaque replay completion did not follow the frozen generation passes.
    Evidence(WeightingEvidenceError),
    /// The consumer rejected the terminal partial block.
    Consumer(E),
}

impl<E: fmt::Display> fmt::Display for WeightingReplayError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Traversal(error) => error.fmt(formatter),
            Self::Owner(error) => error.fmt(formatter),
            Self::Evidence(error) => error.fmt(formatter),
            Self::Consumer(error) => write!(formatter, "weighted replay consumer failed: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for WeightingReplayError<E> {}

/// Error raised inside the T17 replay callback.
#[derive(Debug)]
pub enum ReplayCallbackError<E> {
    /// Reconstruction rejected a validated sample.
    Owner(WeightingError),
    /// The downstream block consumer failed.
    Consumer(E),
}

impl<E: fmt::Display> fmt::Display for ReplayCallbackError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(error) => error.fmt(formatter),
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
