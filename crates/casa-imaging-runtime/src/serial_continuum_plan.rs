// SPDX-License-Identifier: LGPL-3.0-or-later

//! Production planning for ordinary serial continuum reconstruction passes.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use casa_imaging_model::CompiledProblem;
use casa_imaging_reconstruction::{WeightingExecutionLimits, WeightingPlan, plan_weighting};
use casa_ms::{ModelColumnStorageBounds, SelectedObservationResidencyCertificate};

use crate::serial_continuum::MODEL_COLUMN_WORKER_STACK_BYTES;
use crate::*;

const READ_NODE: &str = "transaction-read";
const CHECK_NODE: &str = "transaction-check";
const RECONCILE_NODE: &str = "transaction-reconciliation";
const COMMIT_NODE: &str = "transaction-commit";
const MINOR_NODE: &str = "serial-continuum-minor-cycle";
const SOURCE_READ_RATE_DEMAND: &str = "serial-continuum-source-read-rate";
const OUTPUT_WRITE_RATE_DEMAND: &str = "serial-continuum-output-write-rate";
const IO_QUEUE_DEMAND: &str = "serial-continuum-storage-queue";
const OUTPUT_STORAGE_DEMAND: &str = "serial-continuum-private-commit";

/// Explicit non-scientific limits for one serial continuum physical plan.
#[derive(Clone)]
pub struct SerialContinuumExecutionPolicy {
    implementation: WorkImplementationId,
    weighting_limits: WeightingExecutionLimits,
    selected_residency: SelectedObservationResidencyCertificate,
    storage_io: StorageIoResourceBinding,
    stage_nanos: u64,
    minor_cycle_bytes: u64,
    confidence_parts_per_million: u32,
    model_data: Option<ModelColumnStorageBounds>,
}

impl SerialContinuumExecutionPolicy {
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
            model_data: None,
        }
    }

    /// Reserve the complete storage-owner MODEL_DATA replacement on a terminal pass.
    #[must_use]
    pub const fn with_model_data(mut self, bounds: ModelColumnStorageBounds) -> Self {
        self.model_data = Some(bounds);
        self
    }
}

/// One fully composed ordinary reconstruction physical plan.
pub struct SerialContinuumPlan {
    physical: PhysicalWorkBinding,
    weighting: WeightingPlan,
    complete_data: CompleteDataPlanFragment,
    source_resources: SelectedObservationSourceResources,
    pass: ContinuumPassIdentity,
    minor_cycle_node: Option<WorkNodeId>,
}

impl SerialContinuumPlan {
    /// Plan one complete-data dirty pass without minor-cycle work.
    pub fn dirty<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SerialContinuumExecutionPolicy,
    ) -> Result<Self, SerialContinuumPlanError> {
        Self::build(
            problem,
            registry,
            policy,
            ContinuumPassIdentity::new(ContinuumPassPhase::InitialMajor, 0),
            false,
            None,
        )
    }

    /// Plan an initial major pass followed by one scheduler-accounted T21 node.
    pub fn initial<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SerialContinuumExecutionPolicy,
    ) -> Result<Self, SerialContinuumPlanError> {
        Self::build(
            problem,
            registry,
            policy,
            ContinuumPassIdentity::new(ContinuumPassPhase::InitialMajor, 0),
            true,
            None,
        )
    }

    /// Plan the mandatory final major pass as a separate ordinary plan.
    pub fn final_major<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SerialContinuumExecutionPolicy,
        input: &FinalMajorPhaseInput,
    ) -> Result<Self, SerialContinuumPlanError> {
        Self::build(
            problem,
            registry,
            policy,
            ContinuumPassIdentity::new(ContinuumPassPhase::FinalMajor, 1),
            false,
            Some(input.identity()),
        )
    }

    /// Plan a reconciliating major pass followed by another bounded minor cycle.
    pub fn continuing_major<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SerialContinuumExecutionPolicy,
        input: &FinalMajorPhaseInput,
        ordinal: u32,
    ) -> Result<Self, SerialContinuumPlanError> {
        Self::build(
            problem,
            registry,
            policy,
            ContinuumPassIdentity::new(ContinuumPassPhase::FinalMajor, ordinal),
            true,
            Some(input.identity()),
        )
    }

    /// Plan the terminal reconciliation at an explicit multi-cycle ordinal.
    pub fn final_major_at<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SerialContinuumExecutionPolicy,
        input: &FinalMajorPhaseInput,
        ordinal: u32,
    ) -> Result<Self, SerialContinuumPlanError> {
        Self::build(
            problem,
            registry,
            policy,
            ContinuumPassIdentity::new(ContinuumPassPhase::FinalMajor, ordinal),
            false,
            Some(input.identity()),
        )
    }

    fn build<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        registry: &R,
        policy: SerialContinuumExecutionPolicy,
        pass: ContinuumPassIdentity,
        include_minor: bool,
        phase_input: Option<ArtifactIdentity>,
    ) -> Result<Self, SerialContinuumPlanError> {
        let (base, source_resources) =
            base_physical(problem, registry, &policy, pass, phase_input)?;
        let weighting = plan_weighting(problem, policy.weighting_limits)?;
        let fragment = WeightingPlanFragment::new_for_pass(
            &weighting,
            pass_node(READ_NODE, pass),
            source_resources.clone(),
            policy.implementation.clone(),
            policy.implementation.clone(),
            policy.implementation.clone(),
            pass,
        );
        let replay = fragment.replay_node().clone();
        let physical = fragment.compose(&base)?;
        let complete_data = CompleteDataPlanFragment::new_with_preparation_node(
            problem,
            weighting.limits().max_block_samples(),
            replay.clone(),
            pass_node("serial-mfs-fft-plan", pass),
        )?;
        let (mut physical, complete_data) = complete_data.compose(&physical)?;
        if let Some(bounds) = policy.model_data {
            let [_write] = problem
                .observation_transaction()
                .write_set()
                .model_columns()
            else {
                return Err(SerialContinuumPlanError::ModelColumnCount);
            };
            physical = append_model_data_resources(registry, physical, &policy, &replay, bounds)?;
        }
        let minor_cycle_node = include_minor.then(|| WorkNodeId::new(MINOR_NODE));
        if let Some(minor) = &minor_cycle_node {
            physical = append_minor(registry, physical, &policy, minor)?;
        }
        Ok(Self {
            physical,
            weighting,
            complete_data,
            source_resources,
            pass,
            minor_cycle_node,
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
    pub fn into_parts(
        self,
    ) -> (
        PhysicalWorkBinding,
        WeightingPlan,
        CompleteDataPlanFragment,
        SelectedObservationSourceResources,
        ContinuumPassIdentity,
        Option<WorkNodeId>,
    ) {
        (
            self.physical,
            self.weighting,
            self.complete_data,
            self.source_resources,
            self.pass,
            self.minor_cycle_node,
        )
    }
}

fn base_physical<R: ImplementationRegistry>(
    problem: &CompiledProblem,
    registry: &R,
    policy: &SerialContinuumExecutionPolicy,
    pass: ContinuumPassIdentity,
    phase_input: Option<ArtifactIdentity>,
) -> Result<(PhysicalWorkBinding, SelectedObservationSourceResources), SerialContinuumPlanError> {
    let check = pass_node(CHECK_NODE, pass);
    let read = pass_node(READ_NODE, pass);
    let reconcile = pass_node(RECONCILE_NODE, pass);
    let commit = pass_node(COMMIT_NODE, pass);
    let source_bytes = u64::try_from(policy.selected_residency.aggregate_resident_bytes())
        .map_err(|_| SerialContinuumPlanError::Overflow)?;
    let blocks = u64::try_from(policy.selected_residency.peak_live_blocks())
        .map_err(|_| SerialContinuumPlanError::Overflow)?;
    let sources = problem.observation_transaction().read_set().sources();
    let source_count =
        u64::try_from(sources.len()).map_err(|_| SerialContinuumPlanError::Overflow)?;
    let locks = sources
        .iter()
        .map(|source| source.measurement_set())
        .collect::<BTreeSet<_>>();
    let lock_count = u64::try_from(locks.len()).map_err(|_| SerialContinuumPlanError::Overflow)?;
    let source_allocation = AllocationId::new("serial-continuum-selected-source");
    let source_slot = PhysicalSlotId::new("serial-continuum-selected-source-slot");
    let commit_allocation = AllocationId::new("serial-continuum-commit-buffer");
    let commit_slot = PhysicalSlotId::new("serial-continuum-commit-slot");
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
    let source_compat = compatibility("serial-continuum-selected-source");
    let commit_compat = compatibility("serial-continuum-commit-buffer");
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
                allocation_id: "serial-continuum-selected-source".to_string(),
            },
            capacity_bytes: source_bytes,
            compatibility: source_compat,
        },
        PhysicalSlot {
            id: commit_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "serial-continuum-commit-buffer".to_string(),
            },
            capacity_bytes: 1,
            compatibility: commit_compat,
        },
    ];
    let alternative = DemandAlternative {
        id: AlternativeId::new("serial-continuum-cpu"),
        capabilities: CapabilityPredicate::default(),
        demand: DemandEnvelope {
            host_memory_view: CapacityViewId::new("host-memory"),
            memory: vec![
                MemoryDemand {
                    allocation_id: "serial-continuum-selected-source".to_string(),
                    hard_bytes: source_bytes,
                    preferred_bytes: source_bytes,
                    views: vec![CapacityViewId::new("host-memory")],
                },
                MemoryDemand {
                    allocation_id: "serial-continuum-commit-buffer".to_string(),
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
            .ok_or(SerialContinuumPlanError::Overflow)?,
        PredictionConfidence::new(policy.confidence_parts_per_million)?,
        vec![],
        predictions,
    )?;
    let catalog =
        ImplementationContractCatalog::from_registry(registry, [policy.implementation.clone()])?;
    let artifacts = phase_input
        .map(|identity| {
            PlannedArtifact::new(identity, reconcile.clone(), ArtifactRole::Input, None)
        })
        .into_iter()
        .collect();
    let physical = PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        prediction,
        artifacts,
        ObservationTransactionWork::new_reconstruction(check, reconcile, None, commit),
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

pub(crate) fn pass_node(base: &str, pass: ContinuumPassIdentity) -> WorkNodeId {
    let phase = match pass.phase() {
        ContinuumPassPhase::InitialMajor => "initial-major",
        ContinuumPassPhase::FinalMajor => "final-major",
    };
    WorkNodeId::new(format!("{base}-{phase}-{}", pass.ordinal()))
}

fn append_model_data_resources<R: ImplementationRegistry>(
    registry: &R,
    base: PhysicalWorkBinding,
    policy: &SerialContinuumExecutionPolicy,
    replay: &WorkNodeId,
    bounds: ModelColumnStorageBounds,
) -> Result<PhysicalWorkBinding, SerialContinuumPlanError> {
    let commit = base.observation_transaction().commit().clone();
    let allocation = AllocationId::new("serial-model-data-cell-buffer");
    let slot = PhysicalSlotId::new("serial-model-data-cell-buffer-slot");
    let block_allocation = AllocationId::new("serial-model-data-replay-copy");
    let block_slot = PhysicalSlotId::new("serial-model-data-replay-copy-slot");
    let storage_id = "serial-model-data-column".to_string();
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
        .unwrap_or_else(|| "serial-model-data-write-rate".to_string());
    let queue_id = existing_queue
        .clone()
        .unwrap_or_else(|| "serial-model-data-queue".to_string());
    let bytes = bounds.column_bytes().max(1);
    let cell_bytes = bounds.maximum_cell_bytes().max(1);
    let block_copy_bytes = u64::try_from(policy.weighting_limits.max_block_samples())
        .ok()
        .and_then(|samples| {
            samples.checked_mul(
                u64::try_from(std::mem::size_of::<(u64, u32, u32, num_complex::Complex32)>())
                    .expect("MODEL_DATA write tuple size fits u64"),
            )
        })
        .ok_or(SerialContinuumPlanError::Overflow)?
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
            resource: LeaseResource::Storage {
                demand_id: storage_id.clone(),
                use_kind: StorageUseKind::FinalOutput,
            },
            amount: bytes,
            lifetime: write_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::IoBuffer(IoBufferKind::Writeback),
            amount: cell_bytes,
            lifetime: write_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::RuntimeOverhead(RuntimeOverheadKind::ThreadStack),
            amount: MODEL_COLUMN_WORKER_STACK_BYTES as u64,
            lifetime: write_lifetime.clone(),
        },
    ]);
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
        layout: AllocationLayout::new("serial-model-data-cell-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut alternative = base.execution_dag().resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "serial-model-data-cell-buffer".to_string(),
        hard_bytes: cell_bytes,
        preferred_bytes: cell_bytes,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "serial-model-data-replay-copy".to_string(),
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
        .checked_add(MODEL_COLUMN_WORKER_STACK_BYTES as u64)
        .ok_or(SerialContinuumPlanError::Overflow)?;
    alternative.demand.storage.push(StorageDemand {
        demand_id: storage_id,
        domain: policy.storage_io.domain().clone(),
        temporary_bytes: 0,
        staged_output_bytes: 0,
        final_output_bytes: bytes,
        persistent_cache_bytes: 0,
        read_rate: CountDemand::zero(),
        write_rate: CountDemand::zero(),
        operations_rate: CountDemand::zero(),
        queue_slots: CountDemand::zero(),
    });
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
                        allocation_id: "serial-model-data-cell-buffer".to_string(),
                    },
                    capacity_bytes: cell_bytes,
                    compatibility: compatibility.clone(),
                },
                PhysicalSlot {
                    id: block_slot,
                    lease_resource: LeaseResource::Memory {
                        allocation_id: "serial-model-data-replay-copy".to_string(),
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
                io.push(IoPrediction::new(IoBufferKind::Writeback, bytes, 1));
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
        ObservationTransactionWork::new_reconstruction(
            base.observation_transaction()
                .initial_consistency_check()
                .clone(),
            base.observation_transaction()
                .final_reconciliation()
                .clone(),
            None,
            commit,
        ),
        base.publication_layouts().clone(),
    )?)
}

fn append_minor<R: ImplementationRegistry>(
    registry: &R,
    base: PhysicalWorkBinding,
    policy: &SerialContinuumExecutionPolicy,
    minor: &WorkNodeId,
) -> Result<PhysicalWorkBinding, SerialContinuumPlanError> {
    let reconcile = base
        .observation_transaction()
        .final_reconciliation()
        .clone();
    let commit = base.observation_transaction().commit().clone();
    let allocation = AllocationId::new("serial-continuum-minor-cycle");
    let slot = PhysicalSlotId::new("serial-continuum-minor-cycle-slot");
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("serial-continuum-minor-cycle"),
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
        allocation_id: "serial-continuum-minor-cycle".to_string(),
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
                    allocation_id: "serial-continuum-minor-cycle".to_string(),
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
            .ok_or(SerialContinuumPlanError::Overflow)?,
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
    Ok(PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        prediction,
        base.artifacts().to_vec(),
        ObservationTransactionWork::new_reconstruction(
            base.observation_transaction()
                .initial_consistency_check()
                .clone(),
            base.observation_transaction()
                .final_reconciliation()
                .clone(),
            None,
            commit,
        ),
        PublicationLayoutLedger::empty(),
    )?)
}

#[derive(Debug)]
/// Failure to construct a complete serial continuum physical plan.
pub enum SerialContinuumPlanError {
    /// MODEL_DATA bounds were supplied without one exact model-column write.
    ModelColumnCount,
    /// A byte, row, or elapsed-time projection overflowed its identity domain.
    Overflow,
    /// Scientific weighting planning rejected the compiled problem or limits.
    Weighting(casa_imaging_reconstruction::WeightingError),
    /// T18 physical composition rejected the base transaction authority.
    WeightingFragment(WeightingPlanFragmentError),
    /// T19/T20 physical composition rejected the weighting plan.
    Complete(CompleteDataPlanError),
    /// The composed execution DAG violated scheduler invariants.
    Execution(ExecutionError),
    /// Physical work, prediction, or transaction binding failed.
    Physical(PhysicalWorkBindingError),
}
impl fmt::Display for SerialContinuumPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "serial continuum planning failed: {self:?}")
    }
}
impl Error for SerialContinuumPlanError {}
impl From<casa_imaging_reconstruction::WeightingError> for SerialContinuumPlanError {
    fn from(v: casa_imaging_reconstruction::WeightingError) -> Self {
        Self::Weighting(v)
    }
}
impl From<WeightingPlanFragmentError> for SerialContinuumPlanError {
    fn from(v: WeightingPlanFragmentError) -> Self {
        Self::WeightingFragment(v)
    }
}
impl From<CompleteDataPlanError> for SerialContinuumPlanError {
    fn from(v: CompleteDataPlanError) -> Self {
        Self::Complete(v)
    }
}
impl From<ExecutionError> for SerialContinuumPlanError {
    fn from(v: ExecutionError) -> Self {
        Self::Execution(v)
    }
}
impl From<PhysicalWorkBindingError> for SerialContinuumPlanError {
    fn from(v: PhysicalWorkBindingError) -> Self {
        Self::Physical(v)
    }
}
