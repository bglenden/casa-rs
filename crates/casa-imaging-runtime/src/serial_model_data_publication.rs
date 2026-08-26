// SPDX-License-Identifier: LGPL-3.0-or-later

//! Planned, receipted publication of one independently atomic `MODEL_DATA` generation.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    sync::atomic::{AtomicBool, Ordering},
};

use casa_imaging_model::{CompiledProblem, MeasurementSetIdentity};
use casa_imaging_products::VisibilityProductCompletion;

use crate::*;

const CHECK: &str = "model-data-publication-check";
const RECONCILE: &str = "model-data-publication-reconciliation";
const STAGE: &str = "model-data-publication-stage";
const COMMIT: &str = "model-data-publication-commit";

/// Deployment-owned bounds for one private column replacement.
#[derive(Clone)]
pub struct SerialModelDataPublicationPolicy {
    implementation: WorkImplementationId,
    storage_io: StorageIoResourceBinding,
    staged_bytes: u64,
    validation_buffer_bytes: u64,
    stage_nanos: u64,
    confidence_parts_per_million: u32,
}

impl SerialModelDataPublicationPolicy {
    /// Bind the implementation and conservative storage/buffer bounds.
    #[must_use]
    pub fn new(
        implementation: WorkImplementationId,
        storage_io: StorageIoResourceBinding,
        staged_bytes: u64,
        validation_buffer_bytes: u64,
        stage_nanos: u64,
        confidence_parts_per_million: u32,
    ) -> Self {
        Self {
            implementation,
            storage_io,
            staged_bytes: staged_bytes.max(1),
            validation_buffer_bytes: validation_buffer_bytes.max(1),
            stage_nanos,
            confidence_parts_per_million,
        }
    }
}

/// Immutable physical plan for one model-column generation replacement.
pub struct SerialModelDataPublicationPlan {
    physical: PhysicalWorkBinding,
}

impl SerialModelDataPublicationPlan {
    /// Plan the exact lock, private storage, writeback buffer, and commit fence.
    pub fn new<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        completion: VisibilityProductCompletion,
        registry: &R,
        policy: SerialModelDataPublicationPolicy,
    ) -> Result<Self, SerialModelDataPublicationPlanError> {
        if completion.problem_id() != problem.problem_id() {
            return Err(SerialModelDataPublicationPlanError::WrongProblem);
        }
        let writes = problem
            .observation_transaction()
            .write_set()
            .model_columns();
        let [write] = writes else {
            return Err(SerialModelDataPublicationPlanError::ModelColumnCount(
                writes.len(),
            ));
        };
        let physical = build_physical(write.measurement_set(), completion, registry, &policy)?;
        Ok(Self { physical })
    }

    /// Consume the planned physical work.
    #[must_use]
    pub fn into_physical_work(self) -> PhysicalWorkBinding {
        self.physical
    }
}

fn build_physical<R: ImplementationRegistry>(
    measurement_set: MeasurementSetIdentity,
    completion: VisibilityProductCompletion,
    registry: &R,
    policy: &SerialModelDataPublicationPolicy,
) -> Result<PhysicalWorkBinding, SerialModelDataPublicationPlanError> {
    let check = WorkNodeId::new(CHECK);
    let reconcile = WorkNodeId::new(RECONCILE);
    let stage = WorkNodeId::new(STAGE);
    let commit = WorkNodeId::new(COMMIT);
    let writer_allocation = AllocationId::new("model-data-writeback-buffer");
    let writer_slot = PhysicalSlotId::new("model-data-writeback-slot");
    let commit_allocation = AllocationId::new("model-data-publication-buffer");
    let commit_slot = PhysicalSlotId::new("model-data-publication-slot");
    let storage_demand = "model-data-private-column".to_string();
    let write_rate_demand = "model-data-write-rate".to_string();
    let queue_demand = "model-data-storage-queue".to_string();
    let bytes = policy.staged_bytes;
    let validation_bytes = policy.validation_buffer_bytes;
    let lock = |lifetime| ResourceClaim {
        resource: LeaseResource::MeasurementSetLock { measurement_set },
        amount: 1,
        lifetime,
    };
    let writeback_lifetime = ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Writeback]);
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let nodes = vec![
        WorkNode {
            id: check.clone(),
            kind: WorkKind::DataCensus,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::new(),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                lock(ClaimLifetime::Work),
            ],
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: reconcile.clone(),
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
            id: stage.clone(),
            kind: WorkKind::Writeback,
            domain: WorkDomain::Io,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconcile.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: write_rate_demand.clone(),
                    },
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: queue_demand.clone(),
                    },
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: storage_demand.clone(),
                        use_kind: StorageUseKind::StagedOutput,
                    },
                    amount: bytes,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Writeback),
                    amount: validation_bytes,
                    lifetime: writeback_lifetime.clone(),
                },
                lock(writeback_lifetime.clone()),
            ],
            allocations: vec![AllocationUse {
                allocation: writer_allocation.clone(),
                lifetime: writeback_lifetime.clone(),
            }],
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Writeback]),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: commit.clone(),
            kind: WorkKind::Publication,
            domain: WorkDomain::Io,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(stage.clone(), FenceKind::Io)),
                WorkDependency::Fence(FenceId::new(stage.clone(), FenceKind::Writeback)),
            ]),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: write_rate_demand.clone(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: queue_demand.clone(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: storage_demand.clone(),
                        use_kind: StorageUseKind::StagedOutput,
                    },
                    amount: bytes,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: storage_demand.clone(),
                        use_kind: StorageUseKind::FinalOutput,
                    },
                    amount: bytes,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                lock(publication_lifetime.clone()),
            ],
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
    let writer_compat = compatibility("model-data-writeback");
    let commit_compat = compatibility("model-data-publication");
    let allocations = vec![
        LogicalAllocation {
            id: writer_allocation.clone(),
            bytes: validation_bytes,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Writeback),
            compatibility: writer_compat.clone(),
            physical_slot: writer_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: stage.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(stage.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(stage.clone(), FenceKind::Writeback)),
                ]),
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
            id: writer_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "model-data-writeback".to_string(),
            },
            capacity_bytes: validation_bytes,
            compatibility: writer_compat,
        },
        PhysicalSlot {
            id: commit_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "model-data-publication".to_string(),
            },
            capacity_bytes: 1,
            compatibility: commit_compat,
        },
    ];
    let alternative = DemandAlternative {
        id: AlternativeId::new("serial-model-data-publication"),
        capabilities: CapabilityPredicate::default(),
        demand: DemandEnvelope {
            host_memory_view: CapacityViewId::new("host-memory"),
            memory: vec![
                MemoryDemand {
                    allocation_id: "model-data-writeback".to_string(),
                    hard_bytes: validation_bytes,
                    preferred_bytes: validation_bytes,
                    views: vec![CapacityViewId::new("host-memory")],
                },
                MemoryDemand {
                    allocation_id: "model-data-publication".to_string(),
                    hard_bytes: 1,
                    preferred_bytes: 1,
                    views: vec![CapacityViewId::new("host-memory")],
                },
            ],
            workers: CountDemand::new(1, 1),
            overhead: RuntimeOverheadDemand::zero(),
            storage: vec![StorageDemand {
                demand_id: storage_demand,
                domain: policy.storage_io.domain().clone(),
                temporary_bytes: 0,
                staged_output_bytes: bytes,
                final_output_bytes: bytes,
                persistent_cache_bytes: 0,
                read_rate: CountDemand::zero(),
                write_rate: CountDemand::zero(),
                operations_rate: CountDemand::zero(),
                queue_slots: CountDemand::zero(),
            }],
            rates: vec![RateDemand {
                demand_id: write_rate_demand,
                resource: policy.storage_io.write_rate().clone(),
                amount: CountDemand::new(1, 1),
            }],
            caches: CacheDemand::zero(),
            locks: CountDemand::new(1, 1),
            file_descriptors: CountDemand::zero(),
            queues: vec![QueueDemand {
                demand_id: queue_demand,
                resource: policy.storage_io.queue().clone(),
                slots: CountDemand::new(1, 1),
            }],
            transfers: vec![],
            accelerators: vec![],
            io_buffers: IoBufferDemand {
                writeback_bytes: validation_bytes,
                publication_bytes: 1,
                ..IoBufferDemand::zero()
            },
        },
        headroom: ResourceHeadroom::default(),
        scaling: ScalingMetadata {
            minimum_workers: 1,
            maximum_workers: 1,
            maximum_batch_size: 1,
            maximum_tile_width: 1,
            maximum_tile_height: 1,
            maximum_slab_depth: 1,
            memory_bytes_per_worker: BTreeMap::new(),
        },
        quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
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
            if node == &stage {
                prediction.with_io(vec![IoPrediction::new(
                    IoBufferKind::Writeback,
                    validation_bytes,
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
            .ok_or(SerialModelDataPublicationPlanError::Overflow)?,
        PredictionConfidence::new(policy.confidence_parts_per_million)?,
        vec![],
        predictions,
    )?;
    let artifact = ArtifactIdentity::from_logical_identity(completion.model_product().identity());
    let layouts = PublicationLayoutLedger::new(vec![PublicationPhysicalLayout::new(
        PublicationParticipant::ModelData(measurement_set),
        artifact,
        PhysicalLayoutId::from_sha256(artifact.as_bytes()),
        PublicationStaging::new(
            stage.clone(),
            WorkDependency::Fence(FenceId::new(stage.clone(), FenceKind::Writeback)),
            IoBufferKind::Writeback,
            writer_allocation,
        )?,
        PublicationResourceBounds::new(bytes, bytes, validation_bytes, 0)?,
    )])?;
    let catalog =
        ImplementationContractCatalog::from_registry(registry, [policy.implementation.clone()])?;
    Ok(PhysicalWorkBinding::new_with_model_data_publication(
        catalog,
        dag,
        prediction,
        vec![PlannedArtifact::new(
            artifact,
            commit.clone(),
            ArtifactRole::Output,
            None,
        )],
        ObservationTransactionWork::new_model_data_publication(check, reconcile, stage, commit),
        layouts,
    )?)
}

/// Executor that publishes the already private column only at the plan's commit fence.
pub struct SerialModelDataPublicationExecutor {
    id: WorkImplementationId,
    staging: VisibilityProductStaging,
    artifact: ArtifactIdentity,
    bytes: u64,
    expected_samples: u64,
    prepared: AtomicBool,
}

impl SerialModelDataPublicationExecutor {
    /// Bind the staged private column to its planned artifact.
    #[must_use]
    pub fn new(
        id: WorkImplementationId,
        staging: VisibilityProductStaging,
        completion: VisibilityProductCompletion,
        bytes: u64,
    ) -> Self {
        Self {
            id,
            staging,
            artifact: ArtifactIdentity::from_logical_identity(
                completion.model_product().identity(),
            ),
            bytes: bytes.max(1),
            expected_samples: completion.sample_count(),
            prepared: AtomicBool::new(false),
        }
    }
}

impl WorkImplementation for SerialModelDataPublicationExecutor {
    type Error = io::Error;
    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }
    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        if context.node().id.as_str() == STAGE {
            self.staging.prepare_model_column(self.expected_samples)?;
            self.prepared.store(true, Ordering::Release);
        }
        let artifacts = (context.node().id.as_str() == COMMIT)
            .then(|| {
                ArtifactMeasurement::new(
                    self.artifact,
                    Some(self.artifact),
                    ArtifactDisposition::PublicationPrepared,
                    self.bytes,
                    None,
                )
                .expect("MODEL_DATA staging is adapter-owned")
            })
            .into_iter()
            .collect();
        let resources = context
            .node()
            .claims
            .iter()
            .map(|claim| {
                ResourceMeasurement::new(
                    claim.resource.clone(),
                    claim.lifetime.clone(),
                    claim.amount,
                )
            })
            .collect();
        let io = context
            .node()
            .claims
            .iter()
            .filter_map(|claim| match claim.resource {
                LeaseResource::IoBuffer(kind) => Some(IoMeasurement::new(kind, claim.amount, 1)),
                _ => None,
            })
            .collect();
        Ok(WorkMeasurements::new(resources, io, artifacts))
    }
    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        if context.node().id.as_str() != COMMIT {
            return Err(io::Error::other(
                "MODEL_DATA publication invoked outside its commit node",
            ));
        }
        if !self.prepared.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "MODEL_DATA publication ran before staged-state validation",
            ));
        }
        self.staging.commit_model_column()
    }
    fn failure_measurements<'a>(&'a self, _error: &'a Self::Error) -> Option<&'a WorkMeasurements> {
        None
    }
    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        _fence: FenceKind,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn complete_observation_read(
        &self,
        _completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        Err(io::Error::other(
            "MODEL_DATA publication has no observation-read completion",
        ))
    }
}

/// Single-implementation registry for the terminal storage transaction.
pub type SerialModelDataPublicationRegistry =
    crate::SerialContinuumRegistry<SerialModelDataPublicationExecutor>;

/// Failure to plan the independently atomic model-column replacement.
#[derive(Debug)]
pub enum SerialModelDataPublicationPlanError {
    /// Visibility evidence belongs to another compiled problem.
    WrongProblem,
    /// The logical transaction does not name exactly one model column.
    ModelColumnCount(usize),
    /// A physical byte or time bound overflowed.
    Overflow,
    /// The execution DAG was structurally invalid.
    Execution(ExecutionError),
    /// The physical binding violated a runtime contract.
    Physical(PhysicalWorkBindingError),
    /// The publication ledger was invalid.
    Layout(PublicationLayoutError),
    /// The staging producer or terminal event was invalid.
    Staging(PublicationStagingError),
    /// A required storage or buffer bound was zero.
    Bounds(PublicationResourceBoundsError),
}

impl fmt::Display for SerialModelDataPublicationPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MODEL_DATA publication planning failed: {self:?}")
    }
}
impl std::error::Error for SerialModelDataPublicationPlanError {}
impl From<ExecutionError> for SerialModelDataPublicationPlanError {
    fn from(v: ExecutionError) -> Self {
        Self::Execution(v)
    }
}
impl From<PhysicalWorkBindingError> for SerialModelDataPublicationPlanError {
    fn from(v: PhysicalWorkBindingError) -> Self {
        Self::Physical(v)
    }
}
impl From<PublicationLayoutError> for SerialModelDataPublicationPlanError {
    fn from(v: PublicationLayoutError) -> Self {
        Self::Layout(v)
    }
}
impl From<PublicationStagingError> for SerialModelDataPublicationPlanError {
    fn from(v: PublicationStagingError) -> Self {
        Self::Staging(v)
    }
}
impl From<PublicationResourceBoundsError> for SerialModelDataPublicationPlanError {
    fn from(v: PublicationResourceBoundsError) -> Self {
        Self::Bounds(v)
    }
}
