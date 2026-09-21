// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded product generation directly into private CASA outputs.

use crate::*;
use casa_imaging_model::CompiledProblem;
use casa_imaging_products::{
    ContinuumGenerationDemand, ContinuumProductInputs, PlannedContinuumGeneration, ProductOutput,
    ProductStoragePlan, PublishedContinuumGeneration, produce_continuum_members,
};
use casa_imaging_reconstruction::{MajorCycleCompletion, ReconstructionMaskSet};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Mutex,
};

const GENERATE: &str = "product-generation-write";
const COMMIT: &str = "product-publication-commit";

impl casa_imaging_products::ProductWindowExecutor for crate::bounded_stream::FixedWorkerTeam {
    fn prepare(
        &self,
        slots: &mut [Option<casa_imaging_products::ProductWindow>],
        operation: &(
             dyn Fn(
            usize,
        ) -> Result<
            casa_imaging_products::ProductWindow,
            casa_imaging_products::ProductsError,
        > + Sync
         ),
    ) -> Result<(), casa_imaging_products::ProductsError> {
        self.for_each_mut(slots, |index, slot| {
            *slot = Some(operation(index)?);
            Ok(())
        })
    }
}

/// Write-only generation destination with individual-image atomic replacement.
///
/// A failed publication fails the run. The output set is incomplete and must be
/// regenerated; no per-member resume or whole-set rollback is supported.
pub trait SerialProductPublicationSink: ProductOutput {
    /// Sink-specific I/O failure.
    type Error: Error + 'static;
    /// Peak writer-owned residency, including the bounded cache and registry.
    fn residency(
        &self,
        planned: &PlannedContinuumGeneration,
        demand: &ContinuumGenerationDemand,
    ) -> Result<ProductSinkResidency, Self::Error>;
    /// Publish the completed private image collection in generation order.
    fn publish(&self) -> Result<(), Self::Error>;
}

/// Output-owner memory separated by its actual lifetime.
#[derive(Debug, Clone, Copy)]
pub struct ProductSinkResidency {
    /// Active image writer, cache and serialization workspace, released after generation.
    pub writer_bytes: u64,
    /// Staged-member registry retained until publication settles.
    pub retained_bytes: u64,
}

/// Deployment inputs for ordinary product I/O planning.
#[derive(Clone)]
pub struct SerialProductPublicationPolicy {
    implementation: WorkImplementationId,
    storage_io: StorageIoResourceBinding,
    stage_nanos: u64,
    confidence_parts_per_million: u32,
}
impl SerialProductPublicationPolicy {
    /// Bind the implementation, output storage, and prediction inputs.
    pub fn new(
        implementation: WorkImplementationId,
        storage_io: StorageIoResourceBinding,
        stage_nanos: u64,
        confidence_parts_per_million: u32,
    ) -> Self {
        Self {
            implementation,
            storage_io,
            stage_nanos,
            confidence_parts_per_million,
        }
    }
}

/// Admitted bounded generation and output publication.
pub struct SerialProductPublicationPlan {
    physical: PhysicalWorkBinding,
    publication: ProductPublicationPlan,
    window: ProductStoragePlan,
}
impl SerialProductPublicationPlan {
    /// Plan before allocating generation windows or opening output images.
    pub fn new<R: ImplementationRegistry>(
        problem: &CompiledProblem,
        planned: &PlannedContinuumGeneration,
        demand: &ContinuumGenerationDemand,
        sink_residency: ProductSinkResidency,
        registry: &R,
        policy: SerialProductPublicationPolicy,
    ) -> Result<Self, SerialProductPublicationPlanError> {
        let publication = ProductPublicationPlan::bind(problem, planned)?;
        let physical = build_physical(registry, &policy, &publication, demand, sink_residency)?;
        Ok(Self {
            physical,
            publication,
            window: demand.storage_plan(),
        })
    }
    /// Ordinary physical candidate.
    pub const fn physical_work(&self) -> &PhysicalWorkBinding {
        &self.physical
    }
    /// Planned output inventory.
    pub const fn publication(&self) -> &ProductPublicationPlan {
        &self.publication
    }
    /// Consume the plan into execution construction parts.
    pub fn into_parts(
        self,
    ) -> (
        PhysicalWorkBinding,
        ProductPublicationPlan,
        ProductStoragePlan,
    ) {
        (self.physical, self.publication, self.window)
    }
}

fn build_physical<R: ImplementationRegistry>(
    registry: &R,
    policy: &SerialProductPublicationPolicy,
    publication: &ProductPublicationPlan,
    demand: &ContinuumGenerationDemand,
    sink_residency: ProductSinkResidency,
) -> Result<PhysicalWorkBinding, SerialProductPublicationPlanError> {
    let generate = WorkNodeId::new(GENERATE);
    let commit = WorkNodeId::new(COMMIT);
    let workers = demand.storage_plan().maximum_workers() as u64;
    let stack_bytes = if workers == 1 {
        0
    } else {
        workers
            .checked_mul(
                (crate::bounded_stream::BOUNDED_WORKER_STACK_BYTES
                    + std::mem::size_of::<std::thread::JoinHandle<()>>()) as u64,
            )
            .ok_or(SerialProductPublicationPlanError::Overflow)?
    };
    let payload_bytes = publication
        .entries()
        .iter()
        .try_fold(0_u64, |total, member| {
            total
                .checked_add(member.payload_bytes())
                .ok_or(SerialProductPublicationPlanError::Overflow)
        })?;
    let writer_bytes = sink_residency.writer_bytes.max(1);
    let metadata_bytes = demand
        .retained_metadata_bytes()
        .checked_add(sink_residency.retained_bytes)
        .ok_or(SerialProductPublicationPlanError::Overflow)?
        .max(1);
    let scratch_bytes = demand
        .peak_residency_bytes()
        .checked_sub(demand.retained_metadata_bytes())
        .and_then(|bytes| bytes.checked_add(stack_bytes))
        .ok_or(SerialProductPublicationPlanError::Overflow)?
        .max(1);
    let allocation_id = AllocationId::new("product-generation-window");
    let metadata_id = AllocationId::new("product-generation-metadata");
    let writer_id = AllocationId::new("product-output-writer");
    let commit_id = AllocationId::new("product-publication-commit-buffer");
    let scratch_slot = PhysicalSlotId::new("product-generation-window-slot");
    let metadata_slot = PhysicalSlotId::new("product-generation-metadata-slot");
    let writer_slot = PhysicalSlotId::new("product-output-writer-slot");
    let commit_slot = PhysicalSlotId::new("product-publication-commit-slot");
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let storage_demand = "product-publication-output".to_string();
    let rate_demand = "product-publication-output-write-rate".to_string();
    let queue_demand = "product-publication-output-queue".to_string();
    let staged = LeaseResource::Storage {
        demand_id: storage_demand.clone(),
        use_kind: StorageUseKind::StagedOutput,
    };
    let nodes = vec![
        WorkNode {
            id: generate.clone(),
            kind: WorkKind::Serialization,
            domain: WorkDomain::Cpu,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::new(),
            claims: vec![
                claim(LeaseResource::Workers, workers, ClaimLifetime::Work),
                claim(LeaseResource::FileDescriptors, 1, ClaimLifetime::Work),
                claim(staged.clone(), payload_bytes, ClaimLifetime::Work),
                claim(
                    LeaseResource::IoBuffer(IoBufferKind::Serialization),
                    writer_bytes,
                    ClaimLifetime::Work,
                ),
            ],
            allocations: vec![
                AllocationUse {
                    allocation: metadata_id.clone(),
                    lifetime: ClaimLifetime::Work,
                },
                AllocationUse {
                    allocation: allocation_id.clone(),
                    lifetime: ClaimLifetime::Work,
                },
                AllocationUse {
                    allocation: writer_id.clone(),
                    lifetime: ClaimLifetime::Work,
                },
            ],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: commit.clone(),
            kind: WorkKind::Publication,
            domain: WorkDomain::Io,
            implementation: policy.implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(generate.clone())]),
            claims: vec![
                claim(
                    LeaseResource::FileDescriptors,
                    1,
                    publication_lifetime.clone(),
                ),
                claim(
                    LeaseResource::Rate {
                        demand_id: rate_demand.clone(),
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
                claim(staged, payload_bytes, publication_lifetime.clone()),
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
            ],
            allocations: vec![
                AllocationUse {
                    allocation: metadata_id.clone(),
                    lifetime: publication_lifetime.clone(),
                },
                AllocationUse {
                    allocation: commit_id.clone(),
                    lifetime: publication_lifetime.clone(),
                },
            ],
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
    let scratch_compat = compatibility("product-generation-window");
    let metadata_compat = compatibility("product-generation-metadata");
    let writer_compat = compatibility("product-output-writer");
    let commit_compat = compatibility("product-publication-commit");
    let allocations = vec![
        LogicalAllocation {
            id: metadata_id,
            bytes: metadata_bytes,
            purpose: AllocationPurpose::Data,
            compatibility: metadata_compat.clone(),
            physical_slot: metadata_slot.clone(),
            lifetime: AllocationLifetime {
                disposition: AllocationDisposition::Release,
                acquire_at: generate.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
                ]),
            },
        },
        allocation(
            allocation_id,
            scratch_bytes,
            AllocationPurpose::Data,
            scratch_compat.clone(),
            scratch_slot.clone(),
            generate.clone(),
            WorkDependency::Work(generate.clone()),
        ),
        allocation(
            writer_id.clone(),
            writer_bytes,
            AllocationPurpose::IoBuffer(IoBufferKind::Serialization),
            writer_compat.clone(),
            writer_slot.clone(),
            generate.clone(),
            WorkDependency::Work(generate.clone()),
        ),
        LogicalAllocation {
            id: commit_id,
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
            compatibility: commit_compat.clone(),
            physical_slot: commit_slot.clone(),
            lifetime: AllocationLifetime {
                disposition: AllocationDisposition::Release,
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
            metadata_slot,
            "product-generation-metadata",
            metadata_bytes,
            metadata_compat,
        ),
        slot(
            scratch_slot,
            "product-generation-window",
            scratch_bytes,
            scratch_compat,
        ),
        slot(
            writer_slot,
            "product-output-writer",
            writer_bytes,
            writer_compat,
        ),
        slot(commit_slot, "product-publication-commit", 1, commit_compat),
    ];
    let alternative = DemandAlternative {
        id: AlternativeId::new("serial-product-publication"),
        capabilities: CapabilityPredicate::default(),
        demand: DemandEnvelope {
            host_memory_view: CapacityViewId::new("host-memory"),
            memory: vec![
                memory("product-generation-metadata", metadata_bytes),
                memory("product-generation-window", scratch_bytes),
                memory("product-output-writer", writer_bytes),
                memory("product-publication-commit", 1),
            ],
            workers: CountDemand::new(workers, workers),
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
                queue_slots: CountDemand::new(1, 1),
            }],
            rates: vec![RateDemand {
                demand_id: rate_demand,
                resource: policy.storage_io.write_rate().clone(),
                amount: CountDemand::new(1, 1),
            }],
            caches: CacheDemand::zero(),
            locks: CountDemand::zero(),
            file_descriptors: CountDemand::new(1, 1),
            queues: vec![QueueDemand {
                demand_id: queue_demand,
                resource: policy.storage_io.queue().clone(),
                slots: CountDemand::new(1, 1),
            }],
            transfers: vec![],
            accelerators: vec![],
            io_buffers: IoBufferDemand {
                serialization_bytes: writer_bytes,
                publication_bytes: 1,
                ..IoBufferDemand::zero()
            },
        },
        headroom: ResourceHeadroom::default(),
        scaling: ScalingMetadata {
            minimum_workers: workers,
            maximum_workers: workers,
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
        initial_knobs: ExecutionKnobs {
            workers,
            ..ExecutionKnobs::serial()
        },
        adaptations: vec![],
    })?;
    let predictions = dag
        .nodes()
        .keys()
        .map(|node| {
            let prediction = StagePrediction::new(node.clone(), policy.stage_nanos);
            if node == &generate {
                prediction.with_io(vec![IoPrediction::new(
                    IoBufferKind::Serialization,
                    payload_bytes.max(writer_bytes),
                    publication.entries().len() as u64,
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
                        generate.clone(),
                        WorkDependency::Work(generate.clone()),
                        IoBufferKind::Serialization,
                        writer_id.clone(),
                    )
                    .expect("bounded writer"),
                    PublicationResourceBounds::new(
                        entry.payload_bytes(),
                        entry.payload_bytes(),
                        writer_bytes,
                        0,
                    )
                    .expect("nonzero product"),
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
        ObservationTransactionWork::new_generated_product_publication(commit),
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
        allocation_id: id.into(),
        hard_bytes: bytes,
        preferred_bytes: bytes,
        views: vec![CapacityViewId::new("host-memory")],
    }
}
fn slot(
    id: PhysicalSlotId,
    name: &str,
    capacity_bytes: u64,
    compatibility: SlotCompatibility,
) -> PhysicalSlot {
    PhysicalSlot {
        id,
        lease_resource: LeaseResource::Memory {
            allocation_id: name.into(),
        },
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
            disposition: AllocationDisposition::Release,
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

/// Planning failure for direct product publication.
#[derive(Debug)]
pub enum SerialProductPublicationPlanError {
    /// Resource arithmetic overflowed.
    Overflow,
    /// Invalid product inventory.
    Publication(ProductPublicationError),
    /// Invalid execution DAG.
    Execution(ExecutionError),
    /// Invalid physical prediction or binding.
    Physical(PhysicalWorkBindingError),
    /// Invalid output layout.
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

/// Payload-free scientific output summary after successful publication.
pub struct SerialProductPublicationCompletion {
    planned: PlannedContinuumGeneration,
    scientific: MajorCycleCompletion,
    published: PublishedContinuumGeneration,
}
impl SerialProductPublicationCompletion {
    /// Return the input lineage and scientific output metadata.
    pub fn into_parts(
        self,
    ) -> (
        PlannedContinuumGeneration,
        MajorCycleCompletion,
        PublishedContinuumGeneration,
    ) {
        (self.planned, self.scientific, self.published)
    }
}
enum SerialProductPublicationState {
    Pending {
        problem: CompiledProblem,
        planned: PlannedContinuumGeneration,
        scientific: MajorCycleCompletion,
        reconstruction_masks: Option<ReconstructionMaskSet>,
    },
    Generated(SerialProductPublicationCompletion),
    Published(SerialProductPublicationCompletion),
    Consumed,
}
/// Stateful owner of bounded generation and final output publication.
pub struct SerialProductPublicationExecutor<S> {
    id: WorkImplementationId,
    publication: ProductPublicationPlan,
    state: Mutex<SerialProductPublicationState>,
    sink: S,
    window: ProductStoragePlan,
}
impl<S: SerialProductPublicationSink> SerialProductPublicationExecutor<S> {
    /// Transfer scientific inputs into their bounded output executor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: WorkImplementationId,
        problem: CompiledProblem,
        publication: ProductPublicationPlan,
        planned: PlannedContinuumGeneration,
        scientific: MajorCycleCompletion,
        reconstruction_masks: Option<ReconstructionMaskSet>,
        sink: S,
        window: ProductStoragePlan,
    ) -> Result<Self, SerialProductPublicationExecutionError<S::Error>> {
        publication
            .validate_generation(&planned)
            .map_err(|_| SerialProductPublicationExecutionError::State)?;
        if publication.problem_id() != problem.problem_id()
            || scientific.normal_state().problem_id() != problem.problem_id()
            || scientific.model_completion().problem() != problem.problem_id()
        {
            return Err(SerialProductPublicationExecutionError::State);
        }
        Ok(Self {
            id,
            publication,
            state: Mutex::new(SerialProductPublicationState::Pending {
                problem,
                planned,
                scientific,
                reconstruction_masks,
            }),
            sink,
            window,
        })
    }
    /// Borrow the output writer.
    pub const fn sink(&self) -> &S {
        &self.sink
    }
    /// Take the summary only after successful publication, once.
    pub fn take_completion(&self) -> Option<SerialProductPublicationCompletion> {
        let mut state = self.state.lock().ok()?;
        match std::mem::replace(&mut *state, SerialProductPublicationState::Consumed) {
            SerialProductPublicationState::Published(completion) => Some(completion),
            other => {
                *state = other;
                None
            }
        }
    }
}
impl<S: SerialProductPublicationSink> WorkImplementation for SerialProductPublicationExecutor<S> {
    type Error = SerialProductPublicationExecutionError<S::Error>;
    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }
    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let mut artifacts = vec![];
        if context.node().id.as_str() == GENERATE {
            let mut state = self
                .state
                .lock()
                .map_err(|_| SerialProductPublicationExecutionError::State)?;
            let (problem, planned, scientific, reconstruction_masks) =
                match std::mem::replace(&mut *state, SerialProductPublicationState::Consumed) {
                    SerialProductPublicationState::Pending {
                        problem,
                        planned,
                        scientific,
                        reconstruction_masks,
                    } => (problem, planned, scientific, reconstruction_masks),
                    other => {
                        *state = other;
                        return Err(SerialProductPublicationExecutionError::State);
                    }
                };
            let mut inputs = ContinuumProductInputs::from_major_cycle(&problem, &scientific)
                .map_err(SerialProductPublicationExecutionError::Products)?;
            if let Some(masks) = reconstruction_masks.as_ref() {
                inputs = match masks {
                    ReconstructionMaskSet::Shared(mask) => inputs.with_reconstruction_mask(mask),
                    ReconstructionMaskSet::Domains(masks) => {
                        inputs.with_domain_reconstruction_masks(masks)
                    }
                    ReconstructionMaskSet::Coupled(masks) => {
                        inputs.with_coupled_reconstruction_masks(masks)
                    }
                }
                .map_err(SerialProductPublicationExecutionError::Products)?;
            }
            if context.knobs().workers != self.window.maximum_workers() as u64 {
                return Err(SerialProductPublicationExecutionError::State);
            }
            let team = crate::bounded_stream::FixedWorkerTeam::new(self.window.maximum_workers())
                .map_err(|error| {
                SerialProductPublicationExecutionError::Workers(format!("{error:?}"))
            })?;
            let generated =
                produce_continuum_members(&planned, &inputs, self.window, &team, &self.sink)
                    .map_err(SerialProductPublicationExecutionError::Products)?;
            *state = SerialProductPublicationState::Generated(SerialProductPublicationCompletion {
                planned,
                scientific,
                published: generated,
            });
        } else if context.node().id.as_str() == COMMIT {
            let state = self
                .state
                .lock()
                .map_err(|_| SerialProductPublicationExecutionError::State)?;
            if !matches!(*state, SerialProductPublicationState::Generated(_)) {
                return Err(SerialProductPublicationExecutionError::State);
            }
            artifacts = self
                .publication
                .entries()
                .iter()
                .map(|entry| {
                    ArtifactMeasurement::new(
                        entry.artifact(),
                        None,
                        ArtifactDisposition::Staged,
                        entry.payload_bytes(),
                        None,
                    )
                    .expect("ordinary staged output")
                })
                .collect();
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
                LeaseResource::IoBuffer(kind) => Some(IoMeasurement::unobserved(kind)),
                _ => None,
            })
            .collect();
        Ok(WorkMeasurements::new(resources, io, artifacts))
    }
    fn failure_measurements<'a>(&'a self, _: &'a Self::Error) -> Option<&'a WorkMeasurements> {
        None
    }
    fn wait_for_fence(
        &self,
        _: WorkExecutionContext<'_>,
        _: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        Ok(WorkMeasurements::default())
    }
    fn complete_observation_read(
        &self,
        _: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        Err(SerialProductPublicationExecutionError::State)
    }
    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SerialProductPublicationExecutionError::State)?;
        if context.node().id.as_str() != COMMIT {
            return Err(SerialProductPublicationExecutionError::State);
        }
        // Consume the generated set before I/O: failure requires a fresh run,
        // not another publish call against an already partially moved set.
        let completion =
            match std::mem::replace(&mut *state, SerialProductPublicationState::Consumed) {
                SerialProductPublicationState::Generated(completion) => completion,
                other => {
                    *state = other;
                    return Err(SerialProductPublicationExecutionError::State);
                }
            };
        self.sink
            .publish()
            .map_err(SerialProductPublicationExecutionError::Sink)?;
        *state = SerialProductPublicationState::Published(completion);
        Ok(())
    }
}
/// Direct generation or output I/O failure.
#[derive(Debug)]
pub enum SerialProductPublicationExecutionError<E> {
    /// The admitted worker team could not be started.
    Workers(String),
    /// Missing, foreign, or already-consumed execution state.
    State,
    /// Scientific generation or bounded writer error.
    Products(casa_imaging_products::ProductsError),
    /// Output publication failed; the output set requires a rerun.
    Sink(E),
}
impl<E: Error> fmt::Display for SerialProductPublicationExecutionError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "serial product publication failed: {self:?}")
    }
}
impl<E: Error + 'static> Error for SerialProductPublicationExecutionError<E> {}

/// One implementation and its compiled compatibility contract.
pub struct SerialProductPublicationRegistry<I> {
    id: ImplementationRegistryId,
    implementation_id: WorkImplementationId,
    metadata: ImplementationContractMetadata,
    implementation: I,
}
impl<I> SerialProductPublicationRegistry<I> {
    /// Bind the implementation to its compiled problem.
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
    /// Borrow the executor.
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
