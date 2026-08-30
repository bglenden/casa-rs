// SPDX-License-Identifier: LGPL-3.0-or-later

//! Production planning for ordinary spectral cycle reconstruction passes.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use casa_imaging_model::CompiledProblem;
use casa_imaging_reconstruction::{
    WeightingExecutionLimits, WeightingPlan, plan_weighting, runtime_adapter::SpectralOperatorPass,
};
use casa_ms::{SelectedObservationResidencyCertificate, SelectedVisibilityStoragePlan};

use crate::*;
use crate::{
    bounded_stream::BOUNDED_WORKER_STACK_BYTES,
    spectral_cycle::{SelectedVisibilityCellWrite, VISIBILITY_WRITE_WORKER_STACK_BYTES},
};

const READ_NODE: &str = "transaction-read";
const CHECK_NODE: &str = "transaction-check";
const FINAL_MODEL_PREPARATION_NODE: &str = "final-model-preparation";
const POST_REPLAY_RECONCILIATION_NODE: &str = "post-replay-reconciliation";
const COMMIT_NODE: &str = "transaction-commit";
const MINOR_NODE: &str = "spectral-cycle-minor-cycle";
const SOURCE_READ_RATE_DEMAND: &str = "spectral-cycle-source-read-rate";
const OUTPUT_WRITE_RATE_DEMAND: &str = "spectral-cycle-output-write-rate";
const IO_QUEUE_DEMAND: &str = "spectral-cycle-storage-queue";
const OUTPUT_STORAGE_DEMAND: &str = "spectral-cycle-private-commit";
const GRIDDED_REPLAY_NODE: &str = "gridded-normal-replay";
const GRIDDED_SPILL_READ_RATE_DEMAND: &str = "gridded-normal-spill-read-rate";
const GRIDDED_SPILL_WRITE_RATE_DEMAND: &str = "gridded-normal-spill-write-rate";
const GRIDDED_SPILL_QUEUE_DEMAND: &str = "gridded-normal-spill-queue";
const GRIDDED_SPILL_STORAGE_DEMAND: &str = "gridded-normal-spill-storage";

fn bounded_worker_stack_bytes(workers: u64) -> Result<u64, SpectralCyclePlanError> {
    if workers == 1 {
        return Ok(0);
    }
    workers
        .checked_mul(BOUNDED_WORKER_STACK_BYTES as u64)
        .ok_or(SpectralCyclePlanError::Overflow)
}

/// Explicit non-scientific limits for one spectral cycle physical plan.
#[derive(Clone)]
pub struct SpectralCycleExecutionPolicy {
    implementation: WorkImplementationId,
    weighting_limits: WeightingExecutionLimits,
    selected_residency: SelectedObservationResidencyCertificate,
    storage_io: StorageIoResourceBinding,
    stage_nanos: u64,
    minor_cycle_bytes: u64,
    confidence_parts_per_million: u32,
    visibility_write: Option<SelectedVisibilityStoragePlan>,
    gridded_normal_storage: Option<GriddedNormalReplayStorage>,
    workers: u64,
    gridded_replay_working_set_bytes: Option<u64>,
}

impl SpectralCycleExecutionPolicy {
    /// Construct explicit execution limits; no machine estimate is inferred.
    #[must_use]
    pub fn new(
        implementation: WorkImplementationId,
        weighting_limits: WeightingExecutionLimits,
        selected_residency: SelectedObservationResidencyCertificate,
        storage_io: StorageIoResourceBinding,
        stage_nanos: u64,
        minor_cycle_bytes: u64,
        confidence_parts_per_million: u32,
    ) -> Self {
        Self {
            implementation,
            weighting_limits,
            selected_residency,
            storage_io,
            stage_nanos,
            minor_cycle_bytes,
            confidence_parts_per_million,
            visibility_write: None,
            gridded_normal_storage: None,
            workers: 1,
            gridded_replay_working_set_bytes: None,
        }
    }

    /// Bind the worker count currently available under the runtime authority.
    ///
    /// This keeps host detection and policy interpretation inside the runtime;
    /// applications only compose the authority and policy they already own.
    pub fn with_planned_workers(
        mut self,
        authority: &ResourceAuthority,
        policy: &ResourcePolicy,
    ) -> Result<Self, SpectralCyclePlanError> {
        self.workers = authority.projected_worker_capacity(policy)?;
        if self.workers == 0 {
            return Err(SpectralCyclePlanError::ZeroWorkers);
        }
        self.gridded_replay_working_set_bytes = match authority.topology().cpu_data_working_set {
            CpuDataWorkingSetCapacity::Known(bytes) => Some(bytes),
            CpuDataWorkingSetCapacity::Unknown => None,
        };
        Ok(self)
    }

    #[cfg(test)]
    pub(crate) const fn with_gridded_replay_working_set_bytes_for_test(
        mut self,
        bytes: u64,
    ) -> Self {
        self.gridded_replay_working_set_bytes = Some(bytes);
        self
    }

    /// Bind the runtime-private storage used by planned gridded-normal spill work.
    #[must_use]
    pub fn with_gridded_normal_storage(mut self, storage: GriddedNormalReplayStorage) -> Self {
        self.gridded_normal_storage = Some(storage);
        self
    }

    /// Add the storage-owner plan for a terminal in-place visibility write.
    #[must_use]
    pub const fn with_visibility_write(mut self, plan: SelectedVisibilityStoragePlan) -> Self {
        self.visibility_write = Some(plan);
        self
    }
}

/// One fully composed ordinary reconstruction physical plan.
pub struct SpectralCyclePlan {
    physical: PhysicalWorkBinding,
    weighting: WeightingPlan,
    complete_data: CompleteDataPlanFragment,
    source_resources: SelectedObservationSourceResources,
    pass: SpectralPassIdentity,
    minor_cycle_node: Option<WorkNodeId>,
    gridded_normal: Option<PlannedGriddedNormalBinding>,
}

/// One complete plan-issued capability for gridded-normal compilation or replay.
pub struct PlannedGriddedNormalBinding {
    kind: PlannedGriddedNormalKind,
}

enum PlannedGriddedNormalKind {
    Compilation {
        storage: GriddedNormalReplayStorage,
        maximum_bytes: u64,
    },
    Replay {
        descriptor: crate::GriddedNormalReplayDescriptor,
        replay: Box<crate::FrozenGriddedNormalReplay>,
    },
}

impl PlannedGriddedNormalBinding {
    fn compilation(storage: GriddedNormalReplayStorage, maximum_bytes: u64) -> Self {
        Self {
            kind: PlannedGriddedNormalKind::Compilation {
                storage,
                maximum_bytes,
            },
        }
    }

    fn replay(
        replay: crate::FrozenGriddedNormalReplay,
        storage: GriddedNormalReplayStorage,
    ) -> Result<Self, SpectralCyclePlanError> {
        let descriptor = replay.descriptor();
        let retained_bytes = descriptor.bytes();
        if !replay.validates_plan_storage(&storage, retained_bytes) {
            return Err(SpectralCyclePlanError::InvalidGriddedNormalReplay);
        }
        Ok(Self {
            kind: PlannedGriddedNormalKind::Replay {
                descriptor,
                replay: Box::new(replay),
            },
        })
    }

    pub(crate) fn into_compilation(self) -> Option<(GriddedNormalReplayStorage, u64)> {
        match self.kind {
            PlannedGriddedNormalKind::Compilation {
                storage,
                maximum_bytes,
            } => Some((storage, maximum_bytes)),
            PlannedGriddedNormalKind::Replay { .. } => None,
        }
    }

    pub(crate) fn into_replay(
        self,
    ) -> Option<(
        crate::GriddedNormalReplayDescriptor,
        crate::FrozenGriddedNormalReplay,
    )> {
        match self.kind {
            PlannedGriddedNormalKind::Replay { descriptor, replay } => Some((descriptor, *replay)),
            PlannedGriddedNormalKind::Compilation { .. } => None,
        }
    }
}

/// Named ownership transfer from one sealed spectral-cycle plan to its executor.
pub struct SpectralCyclePlanParts {
    /// Scheduler-ready physical work binding.
    pub physical: PhysicalWorkBinding,
    /// Weighting science plan owned by the executor.
    pub weighting: WeightingPlan,
    /// Complete-data operator plan owned by the executor.
    pub complete_data: CompleteDataPlanFragment,
    /// Selected-observation source resource contract.
    pub source_resources: SelectedObservationSourceResources,
    /// Exact spectral pass identity.
    pub pass: SpectralPassIdentity,
    /// Optional scheduler-accounted minor-cycle node.
    pub minor_cycle_node: Option<WorkNodeId>,
    /// Complete plan-issued gridded-normal capability, when this pass uses one.
    pub gridded_normal: Option<PlannedGriddedNormalBinding>,
}

impl SpectralCyclePlan {
    /// Plan one complete-data dirty pass without minor-cycle work.
    pub fn dirty<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SpectralCycleExecutionPolicy,
    ) -> Result<Self, SpectralCyclePlanError> {
        Self::build(
            problem,
            registry,
            policy,
            SpectralPassIdentity::new(SpectralPassPhase::InitialMajor, 0),
            false,
            None,
            None,
        )
    }

    /// Plan an initial major pass followed by one scheduler-accounted T21 node.
    pub fn initial<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SpectralCycleExecutionPolicy,
    ) -> Result<Self, SpectralCyclePlanError> {
        Self::build(
            problem,
            registry,
            policy,
            SpectralPassIdentity::new(SpectralPassPhase::InitialMajor, 0),
            true,
            None,
            None,
        )
    }

    /// Plan the mandatory final major pass as a separate ordinary plan.
    pub fn final_major<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SpectralCycleExecutionPolicy,
        input: &FinalMajorPhaseInput,
        gridded_replay: crate::FrozenGriddedNormalReplay,
    ) -> Result<Self, SpectralCyclePlanError> {
        Self::build(
            problem,
            registry,
            policy,
            SpectralPassIdentity::new(SpectralPassPhase::FinalMajor, 1),
            false,
            Some(input.identity()),
            Some(gridded_replay),
        )
    }

    /// Plan a reconciliating major pass followed by another bounded minor cycle.
    pub fn continuing_major<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SpectralCycleExecutionPolicy,
        input: &FinalMajorPhaseInput,
        ordinal: u32,
        gridded_replay: crate::FrozenGriddedNormalReplay,
    ) -> Result<Self, SpectralCyclePlanError> {
        Self::build(
            problem,
            registry,
            policy,
            SpectralPassIdentity::new(SpectralPassPhase::FinalMajor, ordinal),
            true,
            Some(input.identity()),
            Some(gridded_replay),
        )
    }

    /// Plan the terminal reconciliation at an explicit multi-cycle ordinal.
    pub fn final_major_at<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SpectralCycleExecutionPolicy,
        input: &FinalMajorPhaseInput,
        ordinal: u32,
        gridded_replay: crate::FrozenGriddedNormalReplay,
    ) -> Result<Self, SpectralCyclePlanError> {
        Self::build(
            problem,
            registry,
            policy,
            SpectralPassIdentity::new(SpectralPassPhase::FinalMajor, ordinal),
            false,
            Some(input.identity()),
            Some(gridded_replay),
        )
    }

    /// Plan the bounded selected-observation traversal used only for terminal
    /// visibility products and optional MODEL_DATA/CORRECTED_DATA writes.
    pub fn selected_output<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SpectralCycleExecutionPolicy,
        ordinal: u32,
    ) -> Result<Self, SpectralCyclePlanError> {
        let pass = SpectralPassIdentity::new(SpectralPassPhase::FinalMajor, ordinal);
        let weighting = plan_weighting(problem, policy.weighting_limits)?;
        let (base, source_resources) = base_physical(problem, registry, &policy, pass, None)?;
        let fragment = WeightingPlanFragment::streaming_for_pass(
            &weighting,
            pass_node(READ_NODE, pass),
            source_resources.clone(),
            policy.implementation.clone(),
            pass,
            WeightingStreamingMode::SelectedOutputOnly,
            crate::plan_continuum_transform_row(problem)?
                .map(|plan| u64::try_from(plan.bytes()))
                .transpose()
                .map_err(|_| SpectralCyclePlanError::Overflow)?,
        );
        let replay = fragment.streaming_node().clone();
        let physical = fragment.compose(&base)?;
        let complete_data = CompleteDataPlanFragment::new_with_preparation_node(
            problem,
            weighting.limits().max_block_samples(),
            replay.clone(),
            pass_node("spectral-output-fft-plan", pass),
            SpectralOperatorPass::ResidualRefresh,
        )?;
        let (mut physical, complete_data) = complete_data.compose(&physical)?;
        if let Some(bounds) = policy.visibility_write {
            if problem
                .observation_transaction()
                .write_set()
                .visibility_columns()
                .is_empty()
            {
                return Err(SpectralCyclePlanError::VisibilityWriteCount);
            }
            physical =
                append_visibility_write_resources(registry, physical, &policy, &replay, bounds)?;
        }
        Ok(Self {
            physical,
            weighting,
            complete_data,
            source_resources,
            pass,
            minor_cycle_node: None,
            gridded_normal: None,
        })
    }

    fn build<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SpectralCycleExecutionPolicy,
        pass: SpectralPassIdentity,
        include_minor: bool,
        phase_input: Option<ArtifactIdentity>,
        mut gridded_replay: Option<crate::FrozenGriddedNormalReplay>,
    ) -> Result<Self, SpectralCyclePlanError> {
        let weighting = plan_weighting(problem, policy.weighting_limits)?;
        let gridded_normal_storage = if include_minor || gridded_replay.is_some() {
            Some(
                policy
                    .gridded_normal_storage
                    .clone()
                    .ok_or(SpectralCyclePlanError::MissingGriddedNormalStorage)?,
            )
        } else {
            None
        };
        let artifact_budget =
            crate::complete_data_operator::project_gridded_normal_artifact_budget(
                problem,
                weighting.limits().max_block_samples(),
            )
            .map_err(|_| SpectralCyclePlanError::Overflow)?;
        let gridded_replay_descriptor = gridded_replay
            .as_ref()
            .map(crate::FrozenGriddedNormalReplay::descriptor);
        let retained_artifact_bytes =
            gridded_replay_descriptor.map(|descriptor| descriptor.bytes());
        let gridded_window_plan = gridded_replay
            .as_mut()
            .map(|replay| replay.plan_windows(policy.gridded_replay_working_set_bytes))
            .transpose()?;
        let (physical, source_resources, replay) = match pass.phase() {
            SpectralPassPhase::InitialMajor => {
                let (base, source_resources) =
                    base_physical(problem, registry, &policy, pass, phase_input)?;
                let weighting_mode = match problem.weighting().scheme() {
                    casa_imaging_model::WeightingScheme::Natural => {
                        WeightingStreamingMode::NaturalInitial
                    }
                    casa_imaging_model::WeightingScheme::Uniform
                    | casa_imaging_model::WeightingScheme::Briggs { .. }
                    | casa_imaging_model::WeightingScheme::BriggsBandwidthTaper { .. } => {
                        WeightingStreamingMode::DensityInitial
                    }
                };
                let fragment = WeightingPlanFragment::streaming_for_pass(
                    &weighting,
                    pass_node(READ_NODE, pass),
                    source_resources.clone(),
                    policy.implementation.clone(),
                    pass,
                    weighting_mode,
                    crate::plan_continuum_transform_row(problem)?
                        .map(|plan| u64::try_from(plan.bytes()))
                        .transpose()
                        .map_err(|_| SpectralCyclePlanError::Overflow)?,
                );
                let replay = fragment.streaming_node().clone();
                let mut physical = fragment.compose(&base)?;
                if include_minor {
                    physical = append_gridded_spill_resources(
                        registry,
                        physical,
                        &policy,
                        &replay,
                        pass,
                        artifact_budget,
                        GriddedSpillMode::Write,
                    )?;
                }
                (physical, source_resources, replay)
            }
            SpectralPassPhase::FinalMajor => {
                if policy.visibility_write.is_some() {
                    return Err(SpectralCyclePlanError::VisibilityWriteCount);
                }
                let replay = pass_node(GRIDDED_REPLAY_NODE, pass);
                let gridded_replay =
                    gridded_replay_descriptor.ok_or(SpectralCyclePlanError::Overflow)?;
                let (base, source_resources) = base_gridded_physical(
                    problem,
                    registry,
                    &policy,
                    pass,
                    phase_input,
                    &replay,
                    GriddedReplayPlanning {
                        descriptor: gridded_replay,
                        window: gridded_window_plan
                            .as_ref()
                            .ok_or(SpectralCyclePlanError::Overflow)?,
                    },
                )?;
                let physical = append_gridded_spill_resources(
                    registry,
                    base,
                    &policy,
                    &replay,
                    pass,
                    artifact_budget,
                    GriddedSpillMode::Read(
                        gridded_window_plan
                            .as_ref()
                            .ok_or(SpectralCyclePlanError::Overflow)?,
                    ),
                )?;
                (physical, source_resources, replay)
            }
        };
        let preparation_node = pass_node("spectral-operator-fft-plan", pass);
        let complete_data = match pass.phase() {
            SpectralPassPhase::InitialMajor => CompleteDataPlanFragment::new_with_preparation_node(
                problem,
                weighting.limits().max_block_samples(),
                replay.clone(),
                preparation_node,
                SpectralOperatorPass::InitialMajor,
            )?,
            SpectralPassPhase::FinalMajor => {
                CompleteDataPlanFragment::gridded_replay_with_preparation_node(
                    problem,
                    weighting.limits().max_block_samples(),
                    replay.clone(),
                    preparation_node,
                    gridded_window_plan
                        .as_ref()
                        .ok_or(SpectralCyclePlanError::Overflow)?,
                )?
            }
        };
        let (mut physical, complete_data) = complete_data.compose(&physical)?;
        if let Some(bounds) = policy.visibility_write {
            if problem
                .observation_transaction()
                .write_set()
                .visibility_columns()
                .is_empty()
            {
                return Err(SpectralCyclePlanError::VisibilityWriteCount);
            }
            physical =
                append_visibility_write_resources(registry, physical, &policy, &replay, bounds)?;
        }
        let minor_cycle_node = include_minor.then(|| WorkNodeId::new(MINOR_NODE));
        if let Some(minor) = &minor_cycle_node {
            physical = append_minor(registry, physical, &policy, minor)?;
        }
        let gridded_normal = match (gridded_normal_storage, gridded_replay) {
            (Some(storage), Some(replay)) => {
                Some(PlannedGriddedNormalBinding::replay(replay, storage)?)
            }
            (Some(storage), None) => Some(PlannedGriddedNormalBinding::compilation(
                storage,
                retained_artifact_bytes.unwrap_or(artifact_budget.maximum_artifact_bytes()),
            )),
            (None, None) => None,
            (None, Some(_)) => return Err(SpectralCyclePlanError::MissingGriddedNormalStorage),
        };
        Ok(Self {
            physical,
            weighting,
            complete_data,
            source_resources,
            pass,
            minor_cycle_node,
            gridded_normal,
        })
    }

    /// Return the complete physical work to the ordinary runtime planner.
    pub const fn physical_work(&self) -> &PhysicalWorkBinding {
        &self.physical
    }
    /// Return the optional resource-accounted T21 node.
    pub const fn minor_cycle_node(&self) -> Option<&WorkNodeId> {
        self.minor_cycle_node.as_ref()
    }
    /// Consume into executor construction parts.
    pub fn into_parts(self) -> SpectralCyclePlanParts {
        SpectralCyclePlanParts {
            physical: self.physical,
            weighting: self.weighting,
            complete_data: self.complete_data,
            source_resources: self.source_resources,
            pass: self.pass,
            minor_cycle_node: self.minor_cycle_node,
            gridded_normal: self.gridded_normal,
        }
    }
}

fn base_physical<R: ImplementationRegistry>(
    problem: &CompiledProblem,
    registry: &R,
    policy: &SpectralCycleExecutionPolicy,
    pass: SpectralPassIdentity,
    phase_input: Option<ArtifactIdentity>,
) -> Result<(PhysicalWorkBinding, SelectedObservationSourceResources), SpectralCyclePlanError> {
    let check = pass_node(CHECK_NODE, pass);
    let read = pass_node(READ_NODE, pass);
    let model_preparation = pass_node(FINAL_MODEL_PREPARATION_NODE, pass);
    let reconcile = pass_node(POST_REPLAY_RECONCILIATION_NODE, pass);
    let commit = pass_node(COMMIT_NODE, pass);
    let source_bytes = u64::try_from(policy.selected_residency.aggregate_resident_bytes())
        .map_err(|_| SpectralCyclePlanError::Overflow)?;
    let blocks = u64::try_from(policy.selected_residency.peak_live_blocks())
        .map_err(|_| SpectralCyclePlanError::Overflow)?;
    let sources = problem.observation_transaction().read_set().sources();
    let source_count =
        u64::try_from(sources.len()).map_err(|_| SpectralCyclePlanError::Overflow)?;
    let locks = sources
        .iter()
        .map(|source| source.measurement_set())
        .collect::<BTreeSet<_>>();
    let lock_count = u64::try_from(locks.len()).map_err(|_| SpectralCyclePlanError::Overflow)?;
    let source_allocation = AllocationId::new("spectral-cycle-selected-source");
    let source_slot = PhysicalSlotId::new("spectral-cycle-selected-source-slot");
    let commit_allocation = AllocationId::new("spectral-cycle-commit-buffer");
    let commit_slot = PhysicalSlotId::new("spectral-cycle-commit-slot");
    let queue_id = IO_QUEUE_DEMAND.to_string();
    let source_rate_id = SOURCE_READ_RATE_DEMAND.to_string();
    let output_rate_id = OUTPUT_WRITE_RATE_DEMAND.to_string();
    let output_storage_id = OUTPUT_STORAGE_DEMAND.to_string();
    let io_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let lock_claims = |lifetime: ClaimLifetime| {
        locks
            .iter()
            .copied()
            .map(|measurement_set| ResourceClaim {
                resource: LeaseResource::MeasurementSetLock { measurement_set },
                amount: 1,
                lifetime: lifetime.clone(),
            })
            .collect::<Vec<_>>()
    };
    let mut check_claims = vec![ResourceClaim {
        resource: LeaseResource::Workers,
        amount: 1,
        lifetime: ClaimLifetime::Work,
    }];
    check_claims.extend(lock_claims(ClaimLifetime::Work));
    let mut read_claims = vec![
        ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::Rate {
                demand_id: source_rate_id.clone(),
            },
            amount: 1,
            lifetime: io_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::Queue {
                demand_id: queue_id.clone(),
            },
            amount: blocks,
            lifetime: io_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead),
            amount: source_bytes,
            lifetime: io_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::FileDescriptors,
            amount: source_count,
            lifetime: io_lifetime.clone(),
        },
    ];
    read_claims.extend(lock_claims(io_lifetime.clone()));
    let mut commit_claims = vec![
        ResourceClaim {
            resource: LeaseResource::Storage {
                demand_id: output_storage_id.clone(),
                use_kind: StorageUseKind::StagedOutput,
            },
            amount: 1,
            lifetime: publication_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::Rate {
                demand_id: output_rate_id.clone(),
            },
            amount: 1,
            lifetime: publication_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::Queue {
                demand_id: queue_id.clone(),
            },
            amount: 1,
            lifetime: publication_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
            amount: 1,
            lifetime: publication_lifetime.clone(),
        },
    ];
    commit_claims.extend(lock_claims(publication_lifetime.clone()));
    let nodes = vec![
        WorkNode {
            id: check.clone(),
            kind: WorkKind::DataCensus,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::new(),
            claims: check_claims,
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: model_preparation.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(check.clone())]),
            claims: vec![ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }],
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: read.clone(),
            kind: WorkKind::ObservationRead,
            domain: WorkDomain::Io,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(check.clone())]),
            claims: read_claims,
            allocations: vec![AllocationUse {
                allocation: source_allocation.clone(),
                lifetime: io_lifetime.clone(),
            }],
            fences: BTreeSet::from([FenceKind::Io]),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: reconcile.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                read.clone(),
                FenceKind::Io,
            ))]),
            claims: vec![ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }],
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: commit.clone(),
            kind: WorkKind::Publication,
            domain: WorkDomain::Io,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconcile.clone())]),
            claims: commit_claims,
            allocations: vec![AllocationUse {
                allocation: commit_allocation.clone(),
                lifetime: publication_lifetime.clone(),
            }],
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
            quiescence_after: BTreeSet::new(),
        },
    ];
    let compatibility = |layout: &str| SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(layout),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let source_compat = compatibility("spectral-cycle-selected-source");
    let commit_compat = compatibility("spectral-cycle-commit-buffer");
    let allocations = vec![
        LogicalAllocation {
            id: source_allocation.clone(),
            bytes: source_bytes,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead),
            compatibility: source_compat.clone(),
            physical_slot: source_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: read.clone(),
                release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    read.clone(),
                    FenceKind::Io,
                ))]),
            },
        },
        LogicalAllocation {
            id: commit_allocation.clone(),
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
            compatibility: commit_compat.clone(),
            physical_slot: commit_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: commit.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
                ]),
            },
        },
    ];
    let slots = vec![
        PhysicalSlot {
            id: source_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "spectral-cycle-selected-source".to_string(),
            },
            capacity_bytes: source_bytes,
            compatibility: source_compat,
        },
        PhysicalSlot {
            id: commit_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "spectral-cycle-commit-buffer".to_string(),
            },
            capacity_bytes: 1,
            compatibility: commit_compat,
        },
    ];
    let alternative = DemandAlternative {
        id: AlternativeId::new("spectral-cycle-cpu"),
        capabilities: CapabilityPredicate::default(),
        demand: DemandEnvelope {
            host_memory_view: CapacityViewId::new("host-memory"),
            memory: vec![
                MemoryDemand {
                    allocation_id: "spectral-cycle-selected-source".to_string(),
                    hard_bytes: source_bytes,
                    preferred_bytes: source_bytes,
                    views: vec![CapacityViewId::new("host-memory")],
                },
                MemoryDemand {
                    allocation_id: "spectral-cycle-commit-buffer".to_string(),
                    hard_bytes: 1,
                    preferred_bytes: 1,
                    views: vec![CapacityViewId::new("host-memory")],
                },
            ],
            workers: CountDemand::new(1, 1),
            overhead: RuntimeOverheadDemand::zero(),
            storage: vec![StorageDemand {
                demand_id: output_storage_id,
                domain: policy.storage_io.domain().clone(),
                temporary_bytes: 0,
                staged_output_bytes: 1,
                final_output_bytes: 0,
                persistent_cache_bytes: 0,
                read_rate: CountDemand::zero(),
                write_rate: CountDemand::zero(),
                operations_rate: CountDemand::zero(),
                queue_slots: CountDemand::zero(),
            }],
            rates: vec![
                RateDemand {
                    demand_id: source_rate_id,
                    resource: policy.storage_io.read_rate().clone(),
                    amount: CountDemand::new(1, 1),
                },
                RateDemand {
                    demand_id: output_rate_id,
                    resource: policy.storage_io.write_rate().clone(),
                    amount: CountDemand::new(1, 1),
                },
            ],
            caches: CacheDemand::zero(),
            locks: CountDemand::new(lock_count, lock_count),
            file_descriptors: CountDemand::new(source_count, source_count),
            queues: vec![QueueDemand {
                demand_id: queue_id.clone(),
                resource: policy.storage_io.queue().clone(),
                slots: CountDemand::new(blocks, blocks),
            }],
            transfers: vec![],
            accelerators: vec![],
            io_buffers: IoBufferDemand {
                source_read_ahead_bytes: source_bytes,
                publication_bytes: 1,
                ..IoBufferDemand::zero()
            },
        },
        headroom: ResourceHeadroom::default(),
        scaling: ScalingMetadata {
            minimum_workers: 1,
            maximum_workers: 1,
            maximum_batch_size: blocks,
            maximum_tile_width: 1,
            maximum_tile_height: 1,
            maximum_slab_depth: 1,
            memory_bytes_per_worker: BTreeMap::new(),
        },
        quiescence_points: BTreeSet::from([
            QuiescencePoint::RunBoundary,
            QuiescencePoint::MajorCycle,
        ]),
    };
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: alternative,
        nodes,
        logical_allocations: allocations,
        physical_slots: slots,
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: vec![],
    })?;
    let predictions = dag
        .nodes()
        .keys()
        .map(|node| {
            let prediction = StagePrediction::new(node.clone(), policy.stage_nanos);
            if node == &read {
                prediction.with_io(vec![IoPrediction::new(
                    IoBufferKind::SourceReadAhead,
                    source_bytes,
                    1,
                )])
            } else if node == &commit {
                prediction.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 1, 1)])
            } else {
                prediction
            }
        })
        .collect::<Vec<_>>();
    let prediction = PlanPrediction::new(
        policy
            .stage_nanos
            .checked_mul(predictions.len() as u64)
            .ok_or(SpectralCyclePlanError::Overflow)?,
        PredictionConfidence::new(policy.confidence_parts_per_million)?,
        vec![],
        predictions,
    )?;
    let catalog =
        ImplementationContractCatalog::from_registry(registry, [policy.implementation.clone()])?;
    let artifacts = phase_input
        .map(|identity| {
            PlannedArtifact::new(
                identity,
                model_preparation.clone(),
                ArtifactRole::Input,
                None,
            )
        })
        .into_iter()
        .collect();
    let physical = PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        prediction,
        artifacts,
        ObservationTransactionWork::new_reconstruction(check, reconcile, commit)
            .with_final_model_preparation(model_preparation),
        PublicationLayoutLedger::empty(),
    )?;
    Ok((
        physical,
        SelectedObservationSourceResources::new(
            policy.selected_residency.clone(),
            BTreeSet::from([source_allocation]),
            LeaseResource::Queue {
                demand_id: queue_id,
            },
        ),
    ))
}

struct GriddedReplayPlanning<'a> {
    descriptor: crate::GriddedNormalReplayDescriptor,
    window: &'a crate::complete_data_operator::GriddedNormalReplayWindowPlan,
}

fn base_gridded_physical<R: ImplementationRegistry>(
    _problem: &CompiledProblem,
    registry: &R,
    policy: &SpectralCycleExecutionPolicy,
    pass: SpectralPassIdentity,
    phase_input: Option<ArtifactIdentity>,
    replay: &WorkNodeId,
    gridded: GriddedReplayPlanning<'_>,
) -> Result<(PhysicalWorkBinding, SelectedObservationSourceResources), SpectralCyclePlanError> {
    let check = pass_node(CHECK_NODE, pass);
    let model_preparation = pass_node(FINAL_MODEL_PREPARATION_NODE, pass);
    let reconcile = pass_node(POST_REPLAY_RECONCILIATION_NODE, pass);
    let commit = pass_node(COMMIT_NODE, pass);
    let commit_allocation = AllocationId::new(format!(
        "spectral-cycle-commit-buffer-final-major-{}",
        pass.ordinal()
    ));
    let commit_slot = PhysicalSlotId::new(format!("{}-slot", commit_allocation.as_str()));
    let output_rate_id = format!("{OUTPUT_WRITE_RATE_DEMAND}-final-major-{}", pass.ordinal());
    let output_queue_id = format!("{IO_QUEUE_DEMAND}-commit-final-major-{}", pass.ordinal());
    let output_storage_id = format!("{OUTPUT_STORAGE_DEMAND}-final-major-{}", pass.ordinal());
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let worker_stack_bytes = bounded_worker_stack_bytes(policy.workers)?;
    let mut replay_claims = vec![ResourceClaim {
        resource: LeaseResource::Workers,
        amount: policy.workers,
        lifetime: ClaimLifetime::Work,
    }];
    if worker_stack_bytes > 0 {
        replay_claims.push(ResourceClaim {
            resource: LeaseResource::RuntimeOverhead(RuntimeOverheadKind::ThreadStack),
            amount: worker_stack_bytes,
            lifetime: ClaimLifetime::Work,
        });
    }
    let nodes = vec![
        WorkNode {
            id: check.clone(),
            kind: WorkKind::DataCensus,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::new(),
            claims: vec![ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }],
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: model_preparation.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(check.clone())]),
            claims: vec![ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }],
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: replay.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([
                WorkDependency::Work(check.clone()),
                WorkDependency::Work(model_preparation.clone()),
            ]),
            claims: replay_claims,
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: reconcile.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(replay.clone())]),
            claims: vec![ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }],
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: commit.clone(),
            kind: WorkKind::Publication,
            domain: WorkDomain::Io,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconcile.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: output_storage_id.clone(),
                        use_kind: StorageUseKind::StagedOutput,
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: output_rate_id.clone(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: output_queue_id.clone(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: commit_allocation.clone(),
                lifetime: publication_lifetime.clone(),
            }],
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
            quiescence_after: BTreeSet::new(),
        },
    ];
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("spectral-cycle-commit-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let alternative = DemandAlternative {
        id: AlternativeId::new(format!("spectral-cycle-gridded-{}", pass.ordinal())),
        capabilities: CapabilityPredicate::default(),
        demand: DemandEnvelope {
            host_memory_view: CapacityViewId::new("host-memory"),
            memory: vec![MemoryDemand {
                allocation_id: commit_allocation.as_str().to_string(),
                hard_bytes: 1,
                preferred_bytes: 1,
                views: vec![CapacityViewId::new("host-memory")],
            }],
            workers: CountDemand::new(policy.workers, policy.workers),
            overhead: RuntimeOverheadDemand {
                thread_stack_bytes: worker_stack_bytes,
                ..RuntimeOverheadDemand::zero()
            },
            storage: vec![StorageDemand {
                demand_id: output_storage_id,
                domain: policy.storage_io.domain().clone(),
                temporary_bytes: 0,
                staged_output_bytes: 1,
                final_output_bytes: 0,
                persistent_cache_bytes: 0,
                read_rate: CountDemand::zero(),
                write_rate: CountDemand::zero(),
                operations_rate: CountDemand::zero(),
                queue_slots: CountDemand::zero(),
            }],
            rates: vec![RateDemand {
                demand_id: output_rate_id,
                resource: policy.storage_io.write_rate().clone(),
                amount: CountDemand::new(1, 1),
            }],
            caches: CacheDemand::zero(),
            locks: CountDemand::zero(),
            file_descriptors: CountDemand::zero(),
            queues: vec![QueueDemand {
                demand_id: output_queue_id,
                resource: policy.storage_io.queue().clone(),
                slots: CountDemand::new(1, 1),
            }],
            transfers: vec![],
            accelerators: vec![],
            io_buffers: IoBufferDemand {
                publication_bytes: 1,
                ..IoBufferDemand::zero()
            },
        },
        headroom: ResourceHeadroom::default(),
        scaling: ScalingMetadata {
            minimum_workers: policy.workers,
            maximum_workers: policy.workers,
            maximum_batch_size: u64::try_from(gridded.window.maximum_frames())
                .map_err(|_| SpectralCyclePlanError::Overflow)?,
            maximum_tile_width: 1,
            maximum_tile_height: 1,
            maximum_slab_depth: 1,
            memory_bytes_per_worker: BTreeMap::new(),
        },
        quiescence_points: BTreeSet::from([
            QuiescencePoint::RunBoundary,
            QuiescencePoint::MajorCycle,
        ]),
    };
    let mut initial_knobs = ExecutionKnobs::serial();
    initial_knobs.workers = policy.workers;
    initial_knobs.batch_size = u64::try_from(gridded.window.maximum_frames())
        .map_err(|_| SpectralCyclePlanError::Overflow)?;
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: alternative,
        nodes,
        logical_allocations: vec![LogicalAllocation {
            id: commit_allocation.clone(),
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
            compatibility: compatibility.clone(),
            physical_slot: commit_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: commit.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
                ]),
            },
        }],
        physical_slots: vec![PhysicalSlot {
            id: commit_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: commit_allocation.as_str().to_string(),
            },
            capacity_bytes: 1,
            compatibility,
        }],
        initial_knobs,
        adaptations: vec![],
    })?;
    let predictions = dag
        .nodes()
        .keys()
        .map(|node| {
            let prediction = StagePrediction::new(node.clone(), policy.stage_nanos);
            if node == &commit {
                prediction.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 1, 1)])
            } else {
                prediction
            }
        })
        .collect::<Vec<_>>();
    let prediction = PlanPrediction::new(
        policy
            .stage_nanos
            .checked_mul(predictions.len() as u64)
            .ok_or(SpectralCyclePlanError::Overflow)?,
        PredictionConfidence::new(policy.confidence_parts_per_million)?,
        vec![],
        predictions,
    )?;
    let catalog =
        ImplementationContractCatalog::from_registry(registry, [policy.implementation.clone()])?;
    let artifacts = phase_input
        .map(|identity| {
            PlannedArtifact::new(
                identity,
                model_preparation.clone(),
                ArtifactRole::Input,
                None,
            )
        })
        .into_iter()
        .chain([PlannedArtifact::new(
            gridded.descriptor.identity(),
            replay.clone(),
            ArtifactRole::Input,
            None,
        )])
        .collect();
    let physical = PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        prediction,
        artifacts,
        ObservationTransactionWork::new_source_free_reconstruction(check, reconcile, commit)
            .with_final_model_preparation(model_preparation),
        PublicationLayoutLedger::empty(),
    )?;
    Ok((
        physical,
        SelectedObservationSourceResources::new(
            policy.selected_residency.clone(),
            BTreeSet::new(),
            LeaseResource::Queue {
                demand_id: "unreachable-selected-source".to_string(),
            },
        ),
    ))
}

#[derive(Clone, Copy)]
enum GriddedSpillMode<'a> {
    Read(&'a crate::complete_data_operator::GriddedNormalReplayWindowPlan),
    Write,
}

fn append_gridded_spill_resources<R: ImplementationRegistry>(
    registry: &R,
    base: PhysicalWorkBinding,
    policy: &SpectralCycleExecutionPolicy,
    node: &WorkNodeId,
    pass: SpectralPassIdentity,
    budget: crate::gridded_normal_artifact::GriddedNormalArtifactBudget,
    mode: GriddedSpillMode<'_>,
) -> Result<PhysicalWorkBinding, SpectralCyclePlanError> {
    let storage = policy
        .gridded_normal_storage
        .as_ref()
        .ok_or(SpectralCyclePlanError::MissingGriddedNormalStorage)?;
    let suffix = format!(
        "{}-{}",
        pass.ordinal(),
        match mode {
            GriddedSpillMode::Read(_) => "read",
            GriddedSpillMode::Write => "write",
        }
    );
    let rate_id = format!(
        "{}-{suffix}",
        match mode {
            GriddedSpillMode::Read(_) => GRIDDED_SPILL_READ_RATE_DEMAND,
            GriddedSpillMode::Write => GRIDDED_SPILL_WRITE_RATE_DEMAND,
        }
    );
    let storage_id = format!("{GRIDDED_SPILL_STORAGE_DEMAND}-{suffix}");
    let queue_id = base
        .execution_dag()
        .resource_alternative()
        .demand
        .queues
        .iter()
        .find(|demand| &demand.resource == storage.resources().queue())
        .map(|demand| demand.demand_id.clone())
        .unwrap_or_else(|| format!("{GRIDDED_SPILL_QUEUE_DEMAND}-{suffix}"));
    let allocation = AllocationId::new(format!("gridded-normal-spill-buffer-{suffix}"));
    let slot = PhysicalSlotId::new(format!("{}-slot", allocation.as_str()));
    let io_kind = match mode {
        GriddedSpillMode::Read(_) => IoBufferKind::SpillRead,
        GriddedSpillMode::Write => IoBufferKind::SpillWrite,
    };
    let source_slots = match mode {
        GriddedSpillMode::Read(_) => 2,
        GriddedSpillMode::Write => 1,
    };
    let bytes_per_slot = match mode {
        GriddedSpillMode::Read(window) => window.source_slot_bytes(),
        GriddedSpillMode::Write => budget.io_buffer_bytes(),
    };
    let buffer_bytes = bytes_per_slot
        .checked_mul(source_slots)
        .ok_or(SpectralCyclePlanError::Overflow)?;
    let lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    if matches!(mode, GriddedSpillMode::Read(_)) {
        let owner = nodes
            .iter_mut()
            .find(|candidate| candidate.id == *node)
            .ok_or(SpectralCyclePlanError::Overflow)?;
        owner.kind = WorkKind::Prefetch;
        owner.domain = WorkDomain::Io;
        owner.fences = BTreeSet::from([FenceKind::Io]);
        let reconciliation = base
            .observation_transaction()
            .post_replay_reconciliation()
            .ok_or(SpectralCyclePlanError::Overflow)?;
        let reconciliation = nodes
            .iter_mut()
            .find(|candidate| candidate.id == *reconciliation)
            .ok_or(SpectralCyclePlanError::Overflow)?;
        reconciliation
            .dependencies
            .remove(&WorkDependency::Work(node.clone()));
        reconciliation
            .dependencies
            .insert(WorkDependency::Fence(FenceId::new(
                node.clone(),
                FenceKind::Io,
            )));
    }
    let owner = nodes
        .iter_mut()
        .find(|candidate| candidate.id == *node)
        .ok_or(SpectralCyclePlanError::Overflow)?;
    owner.claims.push(ResourceClaim {
        resource: LeaseResource::Rate {
            demand_id: rate_id.clone(),
        },
        amount: 1,
        lifetime: lifetime.clone(),
    });
    if !owner.claims.iter().any(|claim| {
        claim.resource
            == LeaseResource::Queue {
                demand_id: queue_id.clone(),
            }
            && claim.lifetime == lifetime
    }) {
        owner.claims.push(ResourceClaim {
            resource: LeaseResource::Queue {
                demand_id: queue_id.clone(),
            },
            amount: 1,
            lifetime: lifetime.clone(),
        });
    }
    owner.claims.push(ResourceClaim {
        resource: LeaseResource::IoBuffer(io_kind),
        amount: buffer_bytes,
        lifetime: lifetime.clone(),
    });
    if matches!(mode, GriddedSpillMode::Write) {
        owner.claims.push(ResourceClaim {
            resource: LeaseResource::Storage {
                demand_id: storage_id.clone(),
                use_kind: StorageUseKind::Temporary,
            },
            amount: budget.maximum_artifact_bytes(),
            lifetime: ClaimLifetime::Artifact,
        });
    }
    owner.claims.push(ResourceClaim {
        resource: LeaseResource::FileDescriptors,
        amount: 1,
        lifetime: lifetime.clone(),
    });
    owner.allocations.push(AllocationUse {
        allocation: allocation.clone(),
        lifetime: lifetime.clone(),
    });
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("gridded-normal-spill-frame"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut alternative = base.execution_dag().resource_alternative().clone();
    alternative.id = AlternativeId::new(format!("{}-gridded-{suffix}", alternative.id.as_str()));
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: allocation.as_str().to_string(),
        hard_bytes: buffer_bytes,
        preferred_bytes: buffer_bytes,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.storage.push(StorageDemand {
        demand_id: storage_id,
        domain: storage.resources().domain().clone(),
        temporary_bytes: if matches!(mode, GriddedSpillMode::Write) {
            budget.maximum_artifact_bytes()
        } else {
            0
        },
        staged_output_bytes: 0,
        final_output_bytes: 0,
        persistent_cache_bytes: 0,
        read_rate: CountDemand::zero(),
        write_rate: CountDemand::zero(),
        operations_rate: CountDemand::zero(),
        queue_slots: CountDemand::zero(),
    });
    alternative.demand.file_descriptors = CountDemand::new(
        alternative
            .demand
            .file_descriptors
            .hard()
            .checked_add(1)
            .ok_or(SpectralCyclePlanError::Overflow)?,
        alternative
            .demand
            .file_descriptors
            .preferred()
            .checked_add(1)
            .ok_or(SpectralCyclePlanError::Overflow)?,
    );
    alternative.demand.rates.push(RateDemand {
        demand_id: rate_id,
        resource: match mode {
            GriddedSpillMode::Read(_) => storage.resources().read_rate().clone(),
            GriddedSpillMode::Write => storage.resources().write_rate().clone(),
        },
        amount: CountDemand::new(1, 1),
    });
    if !alternative
        .demand
        .queues
        .iter()
        .any(|demand| demand.demand_id == queue_id)
    {
        alternative.demand.queues.push(QueueDemand {
            demand_id: queue_id,
            resource: storage.resources().queue().clone(),
            slots: CountDemand::new(1, 1),
        });
    }
    match mode {
        GriddedSpillMode::Read(_) => {
            alternative.demand.io_buffers.spill_read_bytes = buffer_bytes;
        }
        GriddedSpillMode::Write => {
            alternative.demand.io_buffers.spill_write_bytes = buffer_bytes;
        }
    }
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
            .chain([LogicalAllocation {
                id: allocation.clone(),
                bytes: buffer_bytes,
                purpose: AllocationPurpose::IoBuffer(io_kind),
                compatibility: compatibility.clone(),
                physical_slot: slot.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: node.clone(),
                    release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                        node.clone(),
                        FenceKind::Io,
                    ))]),
                },
            }])
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .chain([PhysicalSlot {
                id: slot,
                lease_resource: LeaseResource::Memory {
                    allocation_id: allocation.as_str().to_string(),
                },
                capacity_bytes: buffer_bytes,
                compatibility,
            }])
            .collect(),
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })?;
    let stages = base
        .prediction()
        .stages()
        .values()
        .cloned()
        .map(|stage| {
            if stage.node() == node {
                let mut io = stage.io().to_vec();
                io.push(IoPrediction::new(
                    io_kind,
                    budget.maximum_artifact_bytes(),
                    budget
                        .maximum_artifact_bytes()
                        .div_ceil(budget.io_buffer_bytes().max(1)),
                ));
                stage.with_io(io)
            } else {
                stage
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        base.prediction().elapsed_nanos(),
        base.prediction().confidence(),
        base.prediction().uncertainty().to_vec(),
        stages,
    )?;
    let catalog =
        ImplementationContractCatalog::from_registry(registry, [policy.implementation.clone()])?;
    Ok(PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        prediction,
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )?)
}

pub(crate) fn pass_node(base: &str, pass: SpectralPassIdentity) -> WorkNodeId {
    let phase = match pass.phase() {
        SpectralPassPhase::InitialMajor => "initial-major",
        SpectralPassPhase::FinalMajor => "final-major",
    };
    WorkNodeId::new(format!("{base}-{phase}-{}", pass.ordinal()))
}

fn append_visibility_write_resources<R: ImplementationRegistry>(
    registry: &R,
    base: PhysicalWorkBinding,
    policy: &SpectralCycleExecutionPolicy,
    replay: &WorkNodeId,
    storage_plan: SelectedVisibilityStoragePlan,
) -> Result<PhysicalWorkBinding, SpectralCyclePlanError> {
    let allocation = AllocationId::new("serial-visibility-write-cell-buffer");
    let slot = PhysicalSlotId::new("serial-visibility-write-cell-buffer-slot");
    let block_allocation = AllocationId::new("serial-visibility-write-replay-copy");
    let block_slot = PhysicalSlotId::new("serial-visibility-write-replay-copy-slot");
    let storage_id = "serial-visibility-write-column".to_string();
    let existing_rate = base
        .execution_dag()
        .resource_alternative()
        .demand
        .rates
        .iter()
        .find(|demand| &demand.resource == policy.storage_io.write_rate())
        .map(|demand| demand.demand_id.clone());
    let existing_queue = base
        .execution_dag()
        .resource_alternative()
        .demand
        .queues
        .iter()
        .find(|demand| &demand.resource == policy.storage_io.queue())
        .map(|demand| demand.demand_id.clone());
    let rate_id = existing_rate
        .clone()
        .unwrap_or_else(|| "serial-visibility-write-rate".to_string());
    let queue_id = existing_queue
        .clone()
        .unwrap_or_else(|| "serial-visibility-write-queue".to_string());
    let persistent_bytes = storage_plan.additional_persistent_bytes();
    let write_bytes = storage_plan.write_bytes().max(1);
    let cell_bytes = storage_plan.maximum_cell_bytes().max(1);
    let block_copy_bytes = u64::try_from(policy.weighting_limits.max_block_samples())
        .ok()
        .and_then(|samples| {
            samples.checked_mul(
                u64::try_from(std::mem::size_of::<SelectedVisibilityCellWrite>())
                    .expect("visibility write tuple size fits u64"),
            )
        })
        .ok_or(SpectralCyclePlanError::Overflow)?
        .max(1);
    let write_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let replay_node = nodes
        .iter_mut()
        .find(|node| node.id == *replay)
        .expect("complete-data replay node exists");
    replay_node.kind = WorkKind::ObservationReadWriteback;
    replay_node
        .claims
        .iter_mut()
        .find(|claim| claim.resource == LeaseResource::Workers)
        .expect("terminal replay has a worker claim")
        .amount = 2;
    replay_node.claims.extend([
        ResourceClaim {
            resource: LeaseResource::IoBuffer(IoBufferKind::Writeback),
            amount: cell_bytes,
            lifetime: write_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::RuntimeOverhead(RuntimeOverheadKind::ThreadStack),
            amount: VISIBILITY_WRITE_WORKER_STACK_BYTES as u64,
            lifetime: write_lifetime.clone(),
        },
    ]);
    if persistent_bytes > 0 {
        replay_node.claims.push(ResourceClaim {
            resource: LeaseResource::Storage {
                demand_id: storage_id.clone(),
                use_kind: StorageUseKind::FinalOutput,
            },
            amount: persistent_bytes,
            lifetime: write_lifetime.clone(),
        });
    }
    if existing_rate.is_none() {
        replay_node.claims.push(ResourceClaim {
            resource: LeaseResource::Rate {
                demand_id: rate_id.clone(),
            },
            amount: 1,
            lifetime: write_lifetime.clone(),
        });
    }
    if existing_queue.is_none() {
        replay_node.claims.push(ResourceClaim {
            resource: LeaseResource::Queue {
                demand_id: queue_id.clone(),
            },
            amount: 1,
            lifetime: write_lifetime.clone(),
        });
    }
    replay_node.allocations.push(AllocationUse {
        allocation: allocation.clone(),
        lifetime: write_lifetime.clone(),
    });
    replay_node.allocations.push(AllocationUse {
        allocation: block_allocation.clone(),
        lifetime: write_lifetime.clone(),
    });
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("serial-visibility-write-cell-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut alternative = base.execution_dag().resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "serial-visibility-write-cell-buffer".to_string(),
        hard_bytes: cell_bytes,
        preferred_bytes: cell_bytes,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "serial-visibility-write-replay-copy".to_string(),
        hard_bytes: block_copy_bytes,
        preferred_bytes: block_copy_bytes,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.workers = CountDemand::new(2, 2);
    alternative.scaling.minimum_workers = 2;
    alternative.scaling.maximum_workers = 2;
    alternative.demand.overhead.thread_stack_bytes = alternative
        .demand
        .overhead
        .thread_stack_bytes
        .checked_add(VISIBILITY_WRITE_WORKER_STACK_BYTES as u64)
        .ok_or(SpectralCyclePlanError::Overflow)?;
    if persistent_bytes > 0 {
        alternative.demand.storage.push(StorageDemand {
            demand_id: storage_id,
            domain: policy.storage_io.domain().clone(),
            temporary_bytes: 0,
            staged_output_bytes: 0,
            final_output_bytes: persistent_bytes,
            persistent_cache_bytes: 0,
            read_rate: CountDemand::zero(),
            write_rate: CountDemand::zero(),
            operations_rate: CountDemand::zero(),
            queue_slots: CountDemand::zero(),
        });
    }
    if existing_rate.is_none() {
        alternative.demand.rates.push(RateDemand {
            demand_id: rate_id,
            resource: policy.storage_io.write_rate().clone(),
            amount: CountDemand::new(1, 1),
        });
    }
    if existing_queue.is_none() {
        alternative.demand.queues.push(QueueDemand {
            demand_id: queue_id,
            resource: policy.storage_io.queue().clone(),
            slots: CountDemand::new(1, 1),
        });
    }
    alternative.demand.io_buffers.writeback_bytes = alternative
        .demand
        .io_buffers
        .writeback_bytes
        .max(cell_bytes);
    let mut initial_knobs = base.execution_dag().initial_knobs().clone();
    initial_knobs.workers = 2;
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
            .chain([
                LogicalAllocation {
                    id: allocation,
                    bytes: cell_bytes,
                    purpose: AllocationPurpose::IoBuffer(IoBufferKind::Writeback),
                    compatibility: compatibility.clone(),
                    physical_slot: slot.clone(),
                    lifetime: AllocationLifetime {
                        acquire_at: replay.clone(),
                        release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                            replay.clone(),
                            FenceKind::Io,
                        ))]),
                    },
                },
                LogicalAllocation {
                    id: block_allocation,
                    bytes: block_copy_bytes,
                    purpose: AllocationPurpose::Data,
                    compatibility: compatibility.clone(),
                    physical_slot: block_slot.clone(),
                    lifetime: AllocationLifetime {
                        acquire_at: replay.clone(),
                        release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                            replay.clone(),
                            FenceKind::Io,
                        ))]),
                    },
                },
            ])
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .chain([
                PhysicalSlot {
                    id: slot,
                    lease_resource: LeaseResource::Memory {
                        allocation_id: "serial-visibility-write-cell-buffer".to_string(),
                    },
                    capacity_bytes: cell_bytes,
                    compatibility: compatibility.clone(),
                },
                PhysicalSlot {
                    id: block_slot,
                    lease_resource: LeaseResource::Memory {
                        allocation_id: "serial-visibility-write-replay-copy".to_string(),
                    },
                    capacity_bytes: block_copy_bytes,
                    compatibility,
                },
            ])
            .collect(),
        initial_knobs,
        adaptations: vec![],
    })?;
    let stages = base
        .prediction()
        .stages()
        .values()
        .cloned()
        .map(|stage| {
            if stage.node() == replay {
                let mut io = stage.io().to_vec();
                io.push(IoPrediction::new(IoBufferKind::Writeback, write_bytes, 1));
                stage.with_io(io)
            } else {
                stage
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        base.prediction().elapsed_nanos(),
        base.prediction().confidence(),
        base.prediction().uncertainty().to_vec(),
        stages,
    )?;
    let catalog =
        ImplementationContractCatalog::from_registry(registry, [policy.implementation.clone()])?;
    let work = base
        .observation_transaction()
        .clone()
        .with_visibility_writeback(replay.clone());
    Ok(PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        prediction,
        base.artifacts().to_vec(),
        work,
        base.publication_layouts().clone(),
    )?)
}

fn append_minor<R: ImplementationRegistry>(
    registry: &R,
    base: PhysicalWorkBinding,
    policy: &SpectralCycleExecutionPolicy,
    minor: &WorkNodeId,
) -> Result<PhysicalWorkBinding, SpectralCyclePlanError> {
    let reconcile = base
        .observation_transaction()
        .post_replay_reconciliation()
        .expect("spectral cycle plan has post-replay reconciliation")
        .clone();
    let commit = base.observation_transaction().commit().clone();
    let allocation = AllocationId::new("spectral-cycle-minor-cycle");
    let slot = PhysicalSlotId::new("spectral-cycle-minor-cycle-slot");
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("spectral-cycle-minor-cycle"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    nodes
        .iter_mut()
        .find(|n| n.id == commit)
        .expect("commit exists")
        .dependencies
        .insert(WorkDependency::Work(minor.clone()));
    nodes.push(WorkNode {
        id: minor.clone(),
        kind: WorkKind::Compute,
        domain: WorkDomain::Cpu,
        implementation: policy.implementation.clone(),
        dependencies: BTreeSet::from([WorkDependency::Work(reconcile)]),
        claims: vec![ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }],
        allocations: vec![AllocationUse {
            allocation: allocation.clone(),
            lifetime: ClaimLifetime::Work,
        }],
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    });
    let mut alternative = base.execution_dag().resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "spectral-cycle-minor-cycle".to_string(),
        hard_bytes: policy.minor_cycle_bytes,
        preferred_bytes: policy.minor_cycle_bytes,
        views: vec![CapacityViewId::new("host-memory")],
    });
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
            .chain([LogicalAllocation {
                id: allocation,
                bytes: policy.minor_cycle_bytes,
                purpose: AllocationPurpose::Data,
                compatibility: compatibility.clone(),
                physical_slot: slot.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: minor.clone(),
                    release_after: BTreeSet::from([WorkDependency::Work(minor.clone())]),
                },
            }])
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .chain([PhysicalSlot {
                id: slot,
                lease_resource: LeaseResource::Memory {
                    allocation_id: "spectral-cycle-minor-cycle".to_string(),
                },
                capacity_bytes: policy.minor_cycle_bytes,
                compatibility,
            }])
            .collect(),
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: vec![],
    })?;
    let prediction = PlanPrediction::new(
        base.prediction()
            .elapsed_nanos()
            .checked_add(policy.stage_nanos)
            .ok_or(SpectralCyclePlanError::Overflow)?,
        base.prediction().confidence(),
        base.prediction().uncertainty().to_vec(),
        base.prediction()
            .stages()
            .values()
            .cloned()
            .chain([StagePrediction::new(minor.clone(), policy.stage_nanos)])
            .collect(),
    )?;
    let catalog =
        ImplementationContractCatalog::from_registry(registry, [policy.implementation.clone()])?;
    let work = base.observation_transaction().clone();
    Ok(PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        prediction,
        base.artifacts().to_vec(),
        work,
        PublicationLayoutLedger::empty(),
    )?)
}

#[derive(Debug)]
/// Failure to construct a complete spectral cycle physical plan.
pub enum SpectralCyclePlanError {
    /// The resource policy admitted no CPU worker for complete-data execution.
    ZeroWorkers,
    /// The runtime authority could not project the worker capacity.
    Resources(ResourceError),
    /// A clean initial-major plan omitted its runtime-private spill storage.
    MissingGriddedNormalStorage,
    /// A later-major plan received replay state outside its retained storage authority.
    InvalidGriddedNormalReplay,
    /// Visibility-write bounds were supplied without a logical destination.
    VisibilityWriteCount,
    /// A byte, row, or elapsed-time projection overflowed its identity domain.
    Overflow,
    /// Scientific weighting planning rejected the compiled problem or limits.
    Weighting(casa_imaging_reconstruction::WeightingError),
    /// T18 physical composition rejected the base transaction authority.
    WeightingFragment(WeightingPlanFragmentError),
    /// The compiled visibility transform could not derive a bounded row plan.
    ContinuumTransform(crate::ContinuumTransformError),
    /// T19/T20 physical composition rejected the weighting plan.
    Complete(CompleteDataPlanError),
    /// The composed execution DAG violated scheduler invariants.
    Execution(ExecutionError),
    /// Physical work, prediction, or transaction binding failed.
    Physical(PhysicalWorkBindingError),
}
impl fmt::Display for SpectralCyclePlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "spectral cycle planning failed: {self:?}")
    }
}
impl Error for SpectralCyclePlanError {}
impl From<casa_imaging_reconstruction::WeightingError> for SpectralCyclePlanError {
    fn from(v: casa_imaging_reconstruction::WeightingError) -> Self {
        Self::Weighting(v)
    }
}
impl From<WeightingPlanFragmentError> for SpectralCyclePlanError {
    fn from(v: WeightingPlanFragmentError) -> Self {
        Self::WeightingFragment(v)
    }
}
impl From<crate::ContinuumTransformError> for SpectralCyclePlanError {
    fn from(value: crate::ContinuumTransformError) -> Self {
        Self::ContinuumTransform(value)
    }
}
impl From<CompleteDataPlanError> for SpectralCyclePlanError {
    fn from(v: CompleteDataPlanError) -> Self {
        Self::Complete(v)
    }
}
impl From<ExecutionError> for SpectralCyclePlanError {
    fn from(v: ExecutionError) -> Self {
        Self::Execution(v)
    }
}
impl From<PhysicalWorkBindingError> for SpectralCyclePlanError {
    fn from(v: PhysicalWorkBindingError) -> Self {
        Self::Physical(v)
    }
}
impl From<ResourceError> for SpectralCyclePlanError {
    fn from(value: ResourceError) -> Self {
        Self::Resources(value)
    }
}
