// SPDX-License-Identifier: LGPL-3.0-or-later

//! Production serial product staging and independently atomic publication.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Mutex,
};

use casa_imaging_model::CompiledProblem;
use casa_imaging_products::{
    PlannedContinuumGeneration, PublicationProjection, SealedContinuumGeneration, SealedMember,
};
use casa_ms::{
    BoundSelectedObservation, SelectedObservationCompletion,
    SelectedObservationResidencyCertificate,
};
use sha2::{Digest, Sha256};

use crate::*;

const CHECK: &str = "product-publication-check";
const READ: &str = "product-publication-read";
const RECONCILE: &str = "product-publication-reconciliation";
const STAGE: &str = "product-publication-stage";
const COMMIT: &str = "product-publication-commit";

/// Storage boundary used by the runtime publication owner.
///
/// `stage` writes only private, non-visible state. `promote` independently and
/// atomically replaces one conventional product. A later member failure does
/// not invalidate members already promoted from the same sealed generation.
pub trait SerialProductPublicationSink {
    /// Sink-specific failure.
    type Error: Error + 'static;

    /// Privately stage one exact sealed member.
    fn stage(
        &self,
        planned: ArtifactIdentity,
        observed: ArtifactIdentity,
        member: &SealedMember,
    ) -> Result<(), Self::Error>;

    /// Atomically activate one exact runtime-authorized member.
    ///
    /// Repeating the same planned/observed identity is idempotent and succeeds
    /// without changing visible content.
    fn promote(
        &self,
        entry: AuthorizedProductPublicationEntry,
    ) -> Result<(), MemberPromotionFailure<Self::Error>>;
}

/// Certainty of a failed per-member atomic replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberPromotionFailureKind {
    /// The sink proved that the prior member remains visible.
    Failed,
    /// The sink could not prove which member is visible.
    Uncertain,
}

/// Sink error retaining whether one member's visibility is known.
#[derive(Debug)]
pub struct MemberPromotionFailure<E> {
    kind: MemberPromotionFailureKind,
    source: E,
}

impl<E> MemberPromotionFailure<E> {
    /// Report a failure known to leave the prior member visible.
    #[must_use]
    pub const fn failed(source: E) -> Self {
        Self {
            kind: MemberPromotionFailureKind::Failed,
            source,
        }
    }

    /// Report a failure whose visibility outcome could not be proved.
    #[must_use]
    pub const fn uncertain(source: E) -> Self {
        Self {
            kind: MemberPromotionFailureKind::Uncertain,
            source,
        }
    }

    /// Return the visibility certainty.
    #[must_use]
    pub const fn kind(&self) -> MemberPromotionFailureKind {
        self.kind
    }

    /// Borrow the sink-specific failure.
    #[must_use]
    pub const fn source(&self) -> &E {
        &self.source
    }
}

/// Explicit physical policy for one serial product-publication plan.
#[derive(Clone)]
pub struct SerialProductPublicationPolicy {
    implementation: WorkImplementationId,
    selected_residency: SelectedObservationResidencyCertificate,
    storage_io: StorageIoResourceBinding,
    stage_nanos: u64,
    confidence_parts_per_million: u32,
}

impl SerialProductPublicationPolicy {
    /// Bind every deployment-owned resource and prediction input.
    #[must_use]
    pub fn new(
        implementation: WorkImplementationId,
        selected_residency: SelectedObservationResidencyCertificate,
        storage_io: StorageIoResourceBinding,
        stage_nanos: u64,
        confidence_parts_per_million: u32,
    ) -> Self {
        Self {
            implementation,
            selected_residency,
            storage_io,
            stage_nanos,
            confidence_parts_per_million,
        }
    }
}

/// One ordinary runtime physical plan for a preplanned continuum generation.
pub struct SerialProductPublicationPlan {
    physical: PhysicalWorkBinding,
    publication: ProductPublicationPlan,
}

impl SerialProductPublicationPlan {
    /// Build the exact product DAG before member production and sealing.
    pub fn new<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        planned: &PlannedContinuumGeneration,
        registry: &R,
        policy: SerialProductPublicationPolicy,
    ) -> Result<Self, SerialProductPublicationPlanError> {
        let publication = ProductPublicationPlan::bind(problem, planned)?;
        let physical = build_physical(problem, registry, &policy, &publication)?;
        Ok(Self {
            physical,
            publication,
        })
    }

    /// Return the ordinary physical candidate.
    #[must_use]
    pub const fn physical_work(&self) -> &PhysicalWorkBinding {
        &self.physical
    }

    /// Return the immutable pre-seal publication authority.
    #[must_use]
    pub const fn publication(&self) -> &ProductPublicationPlan {
        &self.publication
    }

    /// Consume into ordinary planning and execution construction parts.
    #[must_use]
    pub fn into_parts(self) -> (PhysicalWorkBinding, ProductPublicationPlan) {
        (self.physical, self.publication)
    }
}

fn build_physical<R: ImplementationRegistry>(
    problem: &CompiledProblem,
    registry: &R,
    policy: &SerialProductPublicationPolicy,
    publication: &ProductPublicationPlan,
) -> Result<PhysicalWorkBinding, SerialProductPublicationPlanError> {
    let check = WorkNodeId::new(CHECK);
    let read = WorkNodeId::new(READ);
    let reconcile = WorkNodeId::new(RECONCILE);
    let stage = WorkNodeId::new(STAGE);
    let commit = WorkNodeId::new(COMMIT);
    let source_allocation = AllocationId::new("product-publication-source-buffer");
    let source_slot = PhysicalSlotId::new("product-publication-source-slot");
    let writer_allocation = AllocationId::new("product-publication-writer-buffer");
    let writer_slot = PhysicalSlotId::new("product-publication-writer-slot");
    let commit_allocation = AllocationId::new("product-publication-commit-buffer");
    let commit_slot = PhysicalSlotId::new("product-publication-commit-slot");
    let source_bytes = u64::try_from(policy.selected_residency.aggregate_resident_bytes())
        .map_err(|_| SerialProductPublicationPlanError::Overflow)?;
    let blocks = u64::try_from(policy.selected_residency.peak_live_blocks())
        .map_err(|_| SerialProductPublicationPlanError::Overflow)?;
    let payload_bytes = publication
        .entries()
        .iter()
        .try_fold(0_u64, |total, entry| {
            total
                .checked_add(entry.payload_bytes())
                .ok_or(SerialProductPublicationPlanError::Overflow)
        })?;
    let sources = problem.observation_transaction().read_set().sources();
    let source_count =
        u64::try_from(sources.len()).map_err(|_| SerialProductPublicationPlanError::Overflow)?;
    let lock_claims = |lifetime: ClaimLifetime| {
        sources
            .iter()
            .map(|source| ResourceClaim {
                resource: LeaseResource::MeasurementSetLock {
                    measurement_set: source.measurement_set(),
                },
                amount: 1,
                lifetime: lifetime.clone(),
            })
            .collect::<Vec<_>>()
    };
    let io_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let source_rate_demand = "product-publication-source-read-rate".to_string();
    let output_rate_demand = "product-publication-output-write-rate".to_string();
    let queue_demand = "product-publication-storage-queue".to_string();
    let storage_demand = "product-publication-output".to_string();
    let mut check_claims = vec![claim(LeaseResource::Workers, 1, ClaimLifetime::Work)];
    check_claims.extend(lock_claims(ClaimLifetime::Work));
    let mut read_claims = vec![
        claim(
            LeaseResource::Rate {
                demand_id: source_rate_demand.clone(),
            },
            1,
            io_lifetime.clone(),
        ),
        claim(
            LeaseResource::Queue {
                demand_id: queue_demand.clone(),
            },
            blocks,
            io_lifetime.clone(),
        ),
        claim(
            LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead),
            source_bytes,
            io_lifetime.clone(),
        ),
        claim(
            LeaseResource::FileDescriptors,
            source_count,
            io_lifetime.clone(),
        ),
    ];
    read_claims.extend(lock_claims(io_lifetime.clone()));
    let mut commit_claims = vec![
        claim(
            LeaseResource::Rate {
                demand_id: output_rate_demand.clone(),
            },
            1,
            publication_lifetime.clone(),
        ),
        claim(
            LeaseResource::Queue {
                demand_id: queue_demand.clone(),
            },
            1,
            publication_lifetime.clone(),
        ),
        claim(
            LeaseResource::Storage {
                demand_id: storage_demand.clone(),
                use_kind: StorageUseKind::StagedOutput,
            },
            payload_bytes,
            publication_lifetime.clone(),
        ),
        claim(
            LeaseResource::Storage {
                demand_id: storage_demand.clone(),
                use_kind: StorageUseKind::FinalOutput,
            },
            payload_bytes,
            publication_lifetime.clone(),
        ),
        claim(
            LeaseResource::IoBuffer(IoBufferKind::Publication),
            1,
            publication_lifetime.clone(),
        ),
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
            claims: vec![claim(LeaseResource::Workers, 1, ClaimLifetime::Work)],
            allocations: vec![],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: stage.clone(),
            kind: WorkKind::Serialization,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconcile.clone())]),
            claims: vec![
                claim(LeaseResource::Workers, 1, ClaimLifetime::Work),
                claim(
                    LeaseResource::Storage {
                        demand_id: storage_demand.clone(),
                        use_kind: StorageUseKind::StagedOutput,
                    },
                    payload_bytes,
                    ClaimLifetime::Work,
                ),
                claim(
                    LeaseResource::IoBuffer(IoBufferKind::Serialization),
                    payload_bytes,
                    ClaimLifetime::Work,
                ),
            ],
            allocations: vec![AllocationUse {
                allocation: writer_allocation.clone(),
                lifetime: ClaimLifetime::Work,
            }],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: commit.clone(),
            kind: WorkKind::Publication,
            domain: WorkDomain::Io,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(stage.clone())]),
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
    let source_compat = compatibility("product-publication-source");
    let writer_compat = compatibility("product-publication-writer");
    let commit_compat = compatibility("product-publication-commit");
    let allocations = vec![
        allocation(
            source_allocation.clone(),
            source_bytes,
            AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead),
            source_compat.clone(),
            source_slot.clone(),
            read.clone(),
            WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io)),
        ),
        allocation(
            writer_allocation.clone(),
            payload_bytes,
            AllocationPurpose::IoBuffer(IoBufferKind::Serialization),
            writer_compat.clone(),
            writer_slot.clone(),
            stage.clone(),
            WorkDependency::Work(stage.clone()),
        ),
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
        slot(
            source_slot,
            LeaseResource::Memory {
                allocation_id: "product-publication-source".to_string(),
            },
            source_bytes,
            source_compat,
        ),
        slot(
            writer_slot,
            LeaseResource::Memory {
                allocation_id: "product-publication-writer".to_string(),
            },
            payload_bytes,
            writer_compat,
        ),
        slot(
            commit_slot,
            LeaseResource::Memory {
                allocation_id: "product-publication-commit".to_string(),
            },
            1,
            commit_compat,
        ),
    ];
    let alternative = DemandAlternative {
        id: AlternativeId::new("serial-product-publication"),
        capabilities: CapabilityPredicate::default(),
        demand: DemandEnvelope {
            host_memory_view: CapacityViewId::new("host-memory"),
            memory: vec![
                memory("product-publication-source", source_bytes),
                memory("product-publication-writer", payload_bytes),
                memory("product-publication-commit", 1),
            ],
            workers: CountDemand::new(1, 1),
            overhead: RuntimeOverheadDemand::zero(),
            storage: vec![StorageDemand {
                demand_id: storage_demand,
                domain: policy.storage_io.domain().clone(),
                temporary_bytes: 0,
                staged_output_bytes: payload_bytes,
                final_output_bytes: payload_bytes,
                persistent_cache_bytes: 0,
                read_rate: CountDemand::zero(),
                write_rate: CountDemand::zero(),
                operations_rate: CountDemand::zero(),
                queue_slots: CountDemand::zero(),
            }],
            rates: vec![
                RateDemand {
                    demand_id: source_rate_demand,
                    resource: policy.storage_io.read_rate().clone(),
                    amount: CountDemand::new(1, 1),
                },
                RateDemand {
                    demand_id: output_rate_demand,
                    resource: policy.storage_io.write_rate().clone(),
                    amount: CountDemand::new(1, 1),
                },
            ],
            caches: CacheDemand::zero(),
            locks: CountDemand::new(source_count, source_count),
            file_descriptors: CountDemand::new(source_count, source_count),
            queues: vec![QueueDemand {
                demand_id: queue_demand,
                resource: policy.storage_io.queue().clone(),
                slots: CountDemand::new(blocks, blocks),
            }],
            transfers: vec![],
            accelerators: vec![],
            io_buffers: IoBufferDemand {
                source_read_ahead_bytes: source_bytes,
                serialization_bytes: payload_bytes,
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
            if node == &read {
                prediction.with_io(vec![IoPrediction::new(
                    IoBufferKind::SourceReadAhead,
                    source_bytes,
                    1,
                )])
            } else if node == &stage {
                prediction.with_io(vec![IoPrediction::new(
                    IoBufferKind::Serialization,
                    payload_bytes,
                    u64::try_from(publication.entries().len()).unwrap_or(u64::MAX),
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
            .ok_or(SerialProductPublicationPlanError::Overflow)?,
        PredictionConfidence::new(policy.confidence_parts_per_million)?,
        vec![],
        predictions,
    )?;
    let artifacts = publication
        .entries()
        .iter()
        .map(|entry| {
            PlannedArtifact::new(entry.artifact(), commit.clone(), ArtifactRole::Output, None)
        })
        .collect();
    let layouts = PublicationLayoutLedger::new(
        publication
            .entries()
            .iter()
            .map(|entry| {
                PublicationPhysicalLayout::new(
                    PublicationParticipant::Product {
                        graph_id: publication.graph_id(),
                        node_id: entry.node(),
                    },
                    entry.artifact(),
                    layout_id(entry.artifact()),
                    PublicationStaging::new(
                        stage.clone(),
                        WorkDependency::Work(stage.clone()),
                        IoBufferKind::Serialization,
                        writer_allocation.clone(),
                    )
                    .expect("serial staging contract"),
                    PublicationResourceBounds::new(
                        entry.payload_bytes(),
                        entry.payload_bytes(),
                        entry.payload_bytes(),
                        0,
                    )
                    .expect("planned payload is nonzero"),
                )
            })
            .collect(),
    )?;
    let catalog =
        ImplementationContractCatalog::from_registry(registry, [policy.implementation.clone()])?;
    Ok(PhysicalWorkBinding::new_with_product_publication(
        catalog,
        dag,
        prediction,
        artifacts,
        ObservationTransactionWork::new_product_publication(check, reconcile, None, commit),
        layouts,
        publication,
    )?)
}

fn claim(resource: LeaseResource, amount: u64, lifetime: ClaimLifetime) -> ResourceClaim {
    ResourceClaim {
        resource,
        amount,
        lifetime,
    }
}
fn memory(id: &str, bytes: u64) -> MemoryDemand {
    MemoryDemand {
        allocation_id: id.to_string(),
        hard_bytes: bytes,
        preferred_bytes: bytes,
        views: vec![CapacityViewId::new("host-memory")],
    }
}
fn slot(
    id: PhysicalSlotId,
    lease_resource: LeaseResource,
    capacity_bytes: u64,
    compatibility: SlotCompatibility,
) -> PhysicalSlot {
    PhysicalSlot {
        id,
        lease_resource,
        capacity_bytes,
        compatibility,
    }
}
fn allocation(
    id: AllocationId,
    bytes: u64,
    purpose: AllocationPurpose,
    compatibility: SlotCompatibility,
    physical_slot: PhysicalSlotId,
    acquire_at: WorkNodeId,
    release_after: WorkDependency,
) -> LogicalAllocation {
    LogicalAllocation {
        id,
        bytes,
        purpose,
        compatibility,
        physical_slot,
        lifetime: AllocationLifetime {
            acquire_at,
            release_after: BTreeSet::from([release_after]),
        },
    }
}
fn layout_id(artifact: ArtifactIdentity) -> PhysicalLayoutId {
    let mut hash = Sha256::new();
    hash.update(b"casa-rs-serial-product-layout-v1");
    hash.update(artifact.as_bytes());
    PhysicalLayoutId::from_sha256(hash.finalize().into())
}

/// Planning failure for serial product publication.
#[derive(Debug)]
pub enum SerialProductPublicationPlanError {
    /// A resource or payload calculation overflowed.
    Overflow,
    /// Planned product authority rejected the generation.
    Publication(ProductPublicationError),
    /// Execution DAG validation failed.
    Execution(ExecutionError),
    /// Physical binding or prediction validation failed.
    Physical(PhysicalWorkBindingError),
    /// Publication layout validation failed.
    Layout(PublicationLayoutError),
}

impl fmt::Display for SerialProductPublicationPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "serial product publication planning failed: {self:?}")
    }
}
impl Error for SerialProductPublicationPlanError {}
impl From<ProductPublicationError> for SerialProductPublicationPlanError {
    fn from(v: ProductPublicationError) -> Self {
        Self::Publication(v)
    }
}
impl From<ExecutionError> for SerialProductPublicationPlanError {
    fn from(v: ExecutionError) -> Self {
        Self::Execution(v)
    }
}
impl From<PhysicalWorkBindingError> for SerialProductPublicationPlanError {
    fn from(v: PhysicalWorkBindingError) -> Self {
        Self::Physical(v)
    }
}
impl From<PublicationLayoutError> for SerialProductPublicationPlanError {
    fn from(v: PublicationLayoutError) -> Self {
        Self::Layout(v)
    }
}

/// Stateful serial staging and atomic-publication implementation.
pub struct SerialProductPublicationExecutor<S> {
    id: WorkImplementationId,
    problem: CompiledProblem,
    publication: ProductPublicationPlan,
    projection: PublicationProjection,
    selected: Mutex<Option<BoundSelectedObservation>>,
    selected_completion: Mutex<Option<SelectedObservationCompletion>>,
    sealed: Mutex<Option<SealedContinuumGeneration>>,
    staged_measurements: Mutex<Option<Vec<ArtifactMeasurement>>>,
    sink: S,
}

impl<S: SerialProductPublicationSink> SerialProductPublicationExecutor<S> {
    /// Bind completed sealed members to their immutable pre-seal plan and sink.
    pub fn new(
        id: WorkImplementationId,
        problem: CompiledProblem,
        publication: ProductPublicationPlan,
        sealed: SealedContinuumGeneration,
        selected: BoundSelectedObservation,
        sink: S,
    ) -> Result<Self, SerialProductPublicationExecutionError<S::Error>> {
        let projection = PublicationProjection::from_sealed(&sealed)
            .map_err(SerialProductPublicationExecutionError::Products)?;
        publication
            .authorize(&projection)
            .map_err(SerialProductPublicationExecutionError::Publication)?;
        Ok(Self {
            id,
            problem,
            publication,
            projection,
            selected: Mutex::new(Some(selected)),
            selected_completion: Mutex::new(None),
            sealed: Mutex::new(Some(sealed)),
            staged_measurements: Mutex::new(None),
            sink,
        })
    }

    /// Borrow the application/storage sink.
    #[must_use]
    pub const fn sink(&self) -> &S {
        &self.sink
    }

    /// Consume the sealed generation after a successful ordinary publication run.
    pub fn take_sealed_generation(&self) -> Option<SealedContinuumGeneration> {
        self.sealed.lock().ok()?.take()
    }
}

impl<S: SerialProductPublicationSink> WorkImplementation for SerialProductPublicationExecutor<S> {
    type Error = SerialProductPublicationExecutionError<S::Error>;
    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }
    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        if context.node().id.as_str() == READ {
            let mut selected = self
                .selected
                .lock()
                .map_err(|_| SerialProductPublicationExecutionError::State)?;
            let completion = selected
                .as_mut()
                .ok_or(SerialProductPublicationExecutionError::State)?
                .traverse(&self.problem, |_| Ok::<_, std::convert::Infallible>(()))
                .map_err(|error| {
                    SerialProductPublicationExecutionError::Observation(error.to_string())
                })?;
            *self
                .selected_completion
                .lock()
                .map_err(|_| SerialProductPublicationExecutionError::State)? = Some(completion);
        }
        let mut artifact_measurements = Vec::new();
        if context.node().id.as_str() == STAGE {
            let sealed = self
                .sealed
                .lock()
                .map_err(|_| SerialProductPublicationExecutionError::State)?;
            let sealed = sealed
                .as_ref()
                .ok_or(SerialProductPublicationExecutionError::State)?;
            let authorization = self
                .publication
                .authorize(&self.projection)
                .map_err(SerialProductPublicationExecutionError::Publication)?;
            for entry in authorization.entries() {
                let member = sealed
                    .members()
                    .iter()
                    .find(|member| member.node() == entry.node())
                    .ok_or(SerialProductPublicationExecutionError::State)?;
                self.sink
                    .stage(entry.planned_identity(), entry.observed_identity(), member)
                    .map_err(SerialProductPublicationExecutionError::Sink)?;
                artifact_measurements.push(
                    ArtifactMeasurement::new(
                        entry.planned_identity(),
                        Some(entry.observed_identity()),
                        ArtifactDisposition::PublicationPrepared,
                        entry.payload_bytes(),
                        None,
                    )
                    .expect("staged is adapter-owned"),
                );
            }
            *self
                .staged_measurements
                .lock()
                .map_err(|_| SerialProductPublicationExecutionError::State)? =
                Some(std::mem::take(&mut artifact_measurements));
        } else if context.node().id.as_str() == COMMIT {
            artifact_measurements = self
                .staged_measurements
                .lock()
                .map_err(|_| SerialProductPublicationExecutionError::State)?
                .take()
                .ok_or(SerialProductPublicationExecutionError::State)?;
        }
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
        Ok(WorkMeasurements::new(resources, io, artifact_measurements))
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
        completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        let owner = self
            .selected_completion
            .lock()
            .map_err(|_| SerialProductPublicationExecutionError::State)?
            .take()
            .ok_or(SerialProductPublicationExecutionError::State)?;
        completion
            .bind(owner)
            .map_err(|error| SerialProductPublicationExecutionError::Observation(error.to_string()))
    }
    fn complete_product_generation(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<Option<PublicationProjection>, Self::Error> {
        if context.node().id.as_str() != COMMIT {
            return Err(SerialProductPublicationExecutionError::State);
        }
        Ok(Some(self.projection.clone()))
    }
    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        let _ = context;
        Err(SerialProductPublicationExecutionError::MissingAuthorization)
    }

    fn publish_product_member(
        &self,
        context: WorkExecutionContext<'_>,
        entry: AuthorizedProductPublicationEntry,
    ) -> Option<Result<ArtifactMeasurement, ProductMemberPublicationFailure<Self::Error>>> {
        let authorized = context.product_publication().is_some_and(|authorization| {
            authorization.problem_id() == self.publication.problem_id()
                && authorization.graph_id() == self.publication.graph_id()
                && authorization.generation_id() == self.publication.generation_id()
                && authorization.entries().contains(&entry)
        });
        if !authorized {
            return Some(Err(ProductMemberPublicationFailure::new(
                SerialProductPublicationExecutionError::MissingAuthorization,
                publication_measurement(entry, ArtifactDisposition::PublicationFailed),
            )));
        }
        Some(match self.sink.promote(entry) {
            Ok(()) => Ok(publication_measurement(
                entry,
                ArtifactDisposition::Published,
            )),
            Err(error) => {
                let disposition = match error.kind() {
                    MemberPromotionFailureKind::Failed => ArtifactDisposition::PublicationFailed,
                    MemberPromotionFailureKind::Uncertain => {
                        ArtifactDisposition::PublicationUncertain
                    }
                };
                Err(ProductMemberPublicationFailure::new(
                    SerialProductPublicationExecutionError::Promotion(error),
                    publication_measurement(entry, disposition),
                ))
            }
        })
    }
}

fn publication_measurement(
    entry: AuthorizedProductPublicationEntry,
    disposition: ArtifactDisposition,
) -> ArtifactMeasurement {
    ArtifactMeasurement::new(
        entry.planned_identity(),
        Some(entry.observed_identity()),
        disposition,
        entry.payload_bytes(),
        None,
    )
    .expect("publication outcome is adapter-owned")
}

/// Execution failure from the serial publication owner.
#[derive(Debug)]
pub enum SerialProductPublicationExecutionError<E> {
    /// Affine execution state was missing or had already been consumed.
    State,
    /// The terminal publication context omitted the runtime-minted authorization.
    MissingAuthorization,
    /// Selected-observation traversal or completion binding failed.
    Observation(String),
    /// The planned generation and completed projection did not match.
    Publication(ProductPublicationError),
    /// Product projection failed after scientific production.
    Products(casa_imaging_products::ProductsError),
    /// The storage sink rejected private staging.
    Sink(E),
    /// One independently atomic member replacement failed or became uncertain.
    Promotion(MemberPromotionFailure<E>),
}
impl<E: Error> fmt::Display for SerialProductPublicationExecutionError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "serial product publication failed: {self:?}")
    }
}
impl<E: Error + 'static> Error for SerialProductPublicationExecutionError<E> {}

/// Immutable registry for one serial publication implementation.
pub struct SerialProductPublicationRegistry<I> {
    id: ImplementationRegistryId,
    implementation_id: WorkImplementationId,
    metadata: ImplementationContractMetadata,
    implementation: I,
}
impl<I> SerialProductPublicationRegistry<I> {
    /// Bind one implementation to its exact compiled contract.
    #[must_use]
    pub fn new(
        id: ImplementationRegistryId,
        implementation_id: WorkImplementationId,
        problem: &CompiledProblem,
        implementation: I,
    ) -> Self {
        Self {
            id,
            implementation_id,
            metadata: ImplementationContractMetadata::new(
                problem.problem_id(),
                problem.numerics_id(),
                problem.required_capabilities().clone(),
            ),
            implementation,
        }
    }
    /// Borrow the stateful implementation.
    #[must_use]
    pub const fn implementation(&self) -> &I {
        &self.implementation
    }
}
impl<I: WorkImplementation> ImplementationRegistry for SerialProductPublicationRegistry<I> {
    type Implementation = I;
    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }
    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        (id == &self.implementation_id).then_some(&self.implementation)
    }
    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        (id == &self.implementation_id).then(|| self.metadata.clone())
    }
}
