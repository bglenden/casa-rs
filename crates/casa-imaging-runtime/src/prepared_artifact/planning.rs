// SPDX-License-Identifier: LGPL-3.0-or-later

//! Canonical composition of one prepared-artifact operation into physical work.

use super::*;
use crate::{
    AllocationAccess, AllocationId, AllocationLayout, AllocationLifetime, AllocationPurpose,
    AllocationUse, AlternativeId, CacheDemand, CapabilityPredicate, CapacityDomainId,
    CapacityViewId, ClaimLifetime, CountDemand, DemandAlternative, DemandEnvelope, ExecutionDag,
    ExecutionDagSpecification, ExecutionError, ExecutionKnobs, ImplementationContractCatalog,
    InitializationPolicy, IoBufferDemand, IoBufferKind, IoPrediction, LeaseResource,
    LogicalAllocation, MemoryDemand, ObservationTransactionWork, PhysicalSlot, PhysicalSlotId,
    PhysicalWorkBinding, PhysicalWorkBindingError, PlanPrediction, PredictionConfidence,
    PublicationLayoutLedger, ResourceClaim, ResourceHeadroom, RuntimeOverheadDemand,
    ScalingMetadata, SlotCompatibility, StagePrediction, StorageDemand, StorageMode,
    WorkDependency, WorkDomain, WorkImplementationId, WorkKind, WorkNode, WorkNodeId,
};

/// Canonical plan fragment for one cold-generate, cold-load, or warm-reuse operation.
pub struct PreparedArtifactPlanFragment<'a> {
    descriptor: &'a PreparedArtifactDescriptor,
    store: &'a PreparedArtifactStore,
    operation: PreparedArtifactOperation,
    producer: WorkNodeId,
    publication_commit: WorkNodeId,
    release_implementation: WorkImplementationId,
    source: Option<PreparedArtifactSourceBinding<'a>>,
}

impl<'a> PreparedArtifactPlanFragment<'a> {
    /// Construct the source-free base used by an application-owned prepared
    /// pre-phase. It performs no observation traversal and publishes no product;
    /// the ordinary commit gate exists solely to close receipt evidence.
    pub fn standalone_base<R: ImplementationRegistry>(
        _problem: &CompiledProblem,
        registry: &R,
        implementation: WorkImplementationId,
        descriptor: &PreparedArtifactDescriptor,
        store: &PreparedArtifactStore,
        stage_nanos: u64,
        confidence_parts_per_million: u32,
    ) -> Result<PhysicalWorkBinding, PreparedArtifactPlanError> {
        let check = WorkNodeId::new("prepared-phase-check");
        let producer = WorkNodeId::new("prepared-phase-producer");
        let reconcile = WorkNodeId::new("prepared-phase-reconcile");
        let commit = WorkNodeId::new("prepared-phase-commit");
        let output_demand = store.storage_demand_id(descriptor);
        let allocation_id = AllocationId::new("prepared-phase-commit-buffer");
        let slot_id = PhysicalSlotId::new("prepared-phase-commit-slot");
        let lifetime =
            ClaimLifetime::through_fences([crate::FenceKind::Io, crate::FenceKind::Publication]);
        let worker = || ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        };
        let nodes = vec![
            WorkNode {
                id: check.clone(),
                kind: WorkKind::DataCensus,
                domain: WorkDomain::Cpu,
                implementation: implementation.clone(),
                dependencies: BTreeSet::new(),
                claims: vec![worker()],
                allocations: vec![],
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: producer.clone(),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: implementation.clone(),
                dependencies: BTreeSet::from([WorkDependency::Work(check.clone())]),
                claims: vec![worker()],
                allocations: vec![],
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: reconcile.clone(),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: implementation.clone(),
                dependencies: BTreeSet::from([WorkDependency::Work(producer)]),
                claims: vec![worker()],
                allocations: vec![],
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: commit.clone(),
                kind: WorkKind::Publication,
                domain: WorkDomain::Io,
                implementation: implementation.clone(),
                dependencies: BTreeSet::from([WorkDependency::Work(reconcile.clone())]),
                claims: vec![
                    ResourceClaim {
                        resource: LeaseResource::Workers,
                        amount: 1,
                        lifetime: lifetime.clone(),
                    },
                    ResourceClaim {
                        resource: LeaseResource::Storage {
                            demand_id: output_demand.clone(),
                            use_kind: StorageUseKind::StagedOutput,
                        },
                        amount: 1,
                        lifetime: lifetime.clone(),
                    },
                    ResourceClaim {
                        resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
                        amount: 1,
                        lifetime: lifetime.clone(),
                    },
                    ResourceClaim {
                        resource: LeaseResource::StorageWriteRate {
                            demand_id: output_demand.clone(),
                        },
                        amount: 1,
                        lifetime: lifetime.clone(),
                    },
                    ResourceClaim {
                        resource: LeaseResource::StorageQueue {
                            demand_id: output_demand.clone(),
                        },
                        amount: 1,
                        lifetime: lifetime.clone(),
                    },
                ],
                allocations: vec![AllocationUse {
                    allocation: allocation_id.clone(),
                    lifetime,
                }],
                fences: BTreeSet::from([crate::FenceKind::Io, crate::FenceKind::Publication]),
                quiescence_after: BTreeSet::new(),
            },
        ];
        let compatibility = SlotCompatibility {
            memory_domain: CapacityDomainId::new("host-memory"),
            views: BTreeSet::from([CapacityViewId::new("host-memory")]),
            alignment_bytes: 1,
            storage_mode: StorageMode::Host,
            layout: AllocationLayout::new("prepared-phase-commit-buffer"),
            initialization: InitializationPolicy::OverwriteBeforeRead,
            access: AllocationAccess::ReadWrite,
        };
        let alternative = DemandAlternative {
            id: AlternativeId::new("prepared-phase-serial"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: vec![MemoryDemand {
                    allocation_id: allocation_id.as_str().to_string(),
                    hard_bytes: 1,
                    preferred_bytes: 1,
                    views: vec![CapacityViewId::new("host-memory")],
                }],
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: vec![StorageDemand {
                    demand_id: output_demand.clone(),
                    domain: store.storage_domain().clone(),
                    temporary_bytes: 0,
                    staged_output_bytes: 1,
                    final_output_bytes: 0,
                    persistent_cache_bytes: 0,
                    read_rate: CountDemand::zero(),
                    write_rate: CountDemand::new(1, 1),
                    operations_rate: CountDemand::zero(),
                    queue_slots: CountDemand::new(1, 1),
                }],
                rates: vec![],
                caches: CacheDemand::zero(),
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: vec![],
                transfers: vec![],
                accelerators: vec![],
                io_buffers: IoBufferDemand {
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
            quiescence_points: BTreeSet::from([crate::QuiescencePoint::RunBoundary]),
        };
        let dag = ExecutionDag::new(ExecutionDagSpecification {
            required_resource_capabilities: BTreeSet::new(),
            resource_alternative: alternative,
            nodes,
            logical_allocations: vec![LogicalAllocation {
                id: allocation_id.clone(),
                bytes: 1,
                purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
                compatibility: compatibility.clone(),
                physical_slot: slot_id.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: commit.clone(),
                    release_after: BTreeSet::from([
                        WorkDependency::Fence(crate::FenceId::new(
                            commit.clone(),
                            crate::FenceKind::Io,
                        )),
                        WorkDependency::Fence(crate::FenceId::new(
                            commit.clone(),
                            crate::FenceKind::Publication,
                        )),
                    ]),
                },
            }],
            physical_slots: vec![PhysicalSlot {
                id: slot_id,
                lease_resource: LeaseResource::Memory {
                    allocation_id: allocation_id.as_str().to_string(),
                },
                capacity_bytes: 1,
                compatibility,
            }],
            initial_knobs: ExecutionKnobs::serial(),
            adaptations: vec![],
        })?;
        let stages = dag
            .nodes()
            .keys()
            .map(|node| {
                let stage = StagePrediction::new(node.clone(), stage_nanos);
                if node == &commit {
                    stage.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 1, 1)])
                } else {
                    stage
                }
            })
            .collect::<Vec<_>>();
        let prediction = PlanPrediction::new(
            stage_nanos.saturating_mul(stages.len() as u64),
            PredictionConfidence::new(confidence_parts_per_million)?,
            vec![],
            stages,
        )?;
        let catalog = ImplementationContractCatalog::from_registry(registry, [implementation])?;
        Ok(PhysicalWorkBinding::new_reconstruction(
            catalog,
            dag,
            prediction,
            vec![],
            ObservationTransactionWork::new_source_free_reconstruction(check, reconcile, commit),
            PublicationLayoutLedger::empty(),
        )?)
    }
    /// Bind a prepared operation to its producer, terminal publication gate, and release owner.
    #[must_use]
    pub fn new(
        descriptor: &'a PreparedArtifactDescriptor,
        store: &'a PreparedArtifactStore,
        operation: PreparedArtifactOperation,
        producer: WorkNodeId,
        publication_commit: WorkNodeId,
        release_implementation: WorkImplementationId,
    ) -> Self {
        Self {
            descriptor,
            store,
            operation,
            producer,
            publication_commit,
            release_implementation,
            source: None,
        }
    }

    /// Bind the exact predecessor-owned source for a cold load.
    #[must_use]
    pub const fn with_load_source(mut self, source: &'a PreparedArtifactLoadSource) -> Self {
        self.source = Some(PreparedArtifactSourceBinding::Files(source));
        self
    }

    /// Bind an exact predecessor-owned structured source for adapter import.
    #[must_use]
    pub const fn with_import_source(mut self, source: &'a PreparedArtifactImportSource) -> Self {
        self.source = Some(PreparedArtifactSourceBinding::Import(source));
        self
    }

    /// Compose this fragment with already validated T17/T28 physical work.
    pub fn compose(
        self,
        base: &PhysicalWorkBinding,
    ) -> Result<PhysicalWorkBinding, PreparedArtifactPlanError> {
        if matches!(self.operation, PreparedArtifactOperation::Load) != self.source.is_some() {
            return Err(PreparedArtifactPlanError::InvalidSourceBinding);
        }
        let reservation = self.store.reservation(self.descriptor, self.operation)?;
        let suffix = match self.operation {
            PreparedArtifactOperation::Generate => "generate",
            PreparedArtifactOperation::Load => "load",
            PreparedArtifactOperation::Reuse => "reuse",
            PreparedArtifactOperation::Consume => "consume",
        };
        let cell_identity = self.descriptor.identity();
        let allocation_id =
            AllocationId::new(format!("prepared-resident-buffer-{suffix}-{cell_identity}"));
        let slot_id =
            PhysicalSlotId::new(format!("prepared-resident-slot-{suffix}-{cell_identity}"));
        let compatibility = SlotCompatibility {
            memory_domain: CapacityDomainId::new("host-memory"),
            views: BTreeSet::from([CapacityViewId::new("host-memory")]),
            alignment_bytes: 8,
            storage_mode: StorageMode::Host,
            layout: AllocationLayout::new("prepared-artifact-streaming-buffer"),
            initialization: InitializationPolicy::OverwriteBeforeRead,
            access: AllocationAccess::ReadWrite,
        };
        let demand_id = self.store.storage_demand_id(self.descriptor);
        let mut alternative: DemandAlternative =
            base.execution_dag().resource_alternative().clone();
        alternative.id = AlternativeId::new(format!(
            "{}-prepared-{suffix}-{cell_identity}",
            alternative.id.as_str()
        ));
        alternative.demand.memory.push(MemoryDemand {
            allocation_id: allocation_id.as_str().to_string(),
            hard_bytes: reservation.resident_buffer_bytes(),
            preferred_bytes: reservation.resident_buffer_bytes(),
            views: vec![CapacityViewId::new("host-memory")],
        });
        alternative.demand.locks = combine_count(alternative.demand.locks, 2);
        alternative.demand.file_descriptors = combine_count(
            alternative.demand.file_descriptors,
            reservation.file_descriptors(),
        );
        alternative.demand.io_buffers.storage_manager_bytes = alternative
            .demand
            .io_buffers
            .storage_manager_bytes
            .max(reservation.resident_buffer_bytes());
        if let Some(cache_demand) = alternative
            .demand
            .storage
            .iter_mut()
            .find(|demand| demand.demand_id == demand_id)
        {
            if cache_demand.domain != *self.store.storage_domain() {
                return Err(PreparedArtifactError::CachePolicyMismatch.into());
            }
            cache_demand.temporary_bytes = cache_demand
                .temporary_bytes
                .max(reservation.temporary_staging_bytes());
            cache_demand.persistent_cache_bytes = cache_demand
                .persistent_cache_bytes
                .max(reservation.persistent_cache_bytes());
            cache_demand.read_rate = combine_count(cache_demand.read_rate, 1);
            cache_demand.write_rate = combine_count(cache_demand.write_rate, 1);
            cache_demand.operations_rate = combine_count(cache_demand.operations_rate, 1);
            cache_demand.queue_slots = combine_count(cache_demand.queue_slots, 1);
        } else {
            alternative.demand.storage.push(StorageDemand {
                demand_id: demand_id.clone(),
                domain: self.store.storage_domain().clone(),
                temporary_bytes: reservation.temporary_staging_bytes(),
                staged_output_bytes: 0,
                final_output_bytes: 0,
                persistent_cache_bytes: reservation.persistent_cache_bytes(),
                read_rate: CountDemand::new(1, 1),
                write_rate: CountDemand::new(1, 1),
                operations_rate: CountDemand::new(1, 1),
                queue_slots: CountDemand::new(1, 1),
            });
        }

        let source_demands = self
            .source
            .map(PreparedArtifactSourceBinding::storage_demands)
            .unwrap_or_default();
        alternative.demand.storage.extend(source_demands.iter().map(
            |(source_demand_id, source_domain)| StorageDemand {
                demand_id: source_demand_id.clone(),
                domain: source_domain.clone(),
                temporary_bytes: 0,
                staged_output_bytes: 0,
                final_output_bytes: 0,
                persistent_cache_bytes: 0,
                read_rate: CountDemand::new(1, 1),
                write_rate: CountDemand::zero(),
                operations_rate: CountDemand::new(1, 1),
                queue_slots: CountDemand::new(1, 1),
            },
        ));

        let mut claims = vec![
            claim(LeaseResource::Workers, 1),
            claim(LeaseResource::Locks, 1),
            claim(
                LeaseResource::FileDescriptors,
                reservation.file_descriptors(),
            ),
            claim(
                LeaseResource::Storage {
                    demand_id: demand_id.clone(),
                    use_kind: StorageUseKind::PersistentCache,
                },
                reservation.persistent_cache_bytes(),
            ),
            claim(
                LeaseResource::IoBuffer(IoBufferKind::StorageManager),
                reservation.resident_buffer_bytes(),
            ),
            claim(
                LeaseResource::StorageReadRate {
                    demand_id: demand_id.clone(),
                },
                1,
            ),
            claim(
                LeaseResource::StorageWriteRate {
                    demand_id: demand_id.clone(),
                },
                1,
            ),
            claim(
                LeaseResource::StorageOperationsRate {
                    demand_id: demand_id.clone(),
                },
                1,
            ),
            claim(
                LeaseResource::StorageQueue {
                    demand_id: demand_id.clone(),
                },
                1,
            ),
        ];
        if reservation.temporary_staging_bytes() > 0 {
            claims.push(claim(
                LeaseResource::Storage {
                    demand_id,
                    use_kind: StorageUseKind::Temporary,
                },
                reservation.temporary_staging_bytes(),
            ));
        }
        for source_demand_id in source_demands.keys() {
            claims.extend([
                claim(
                    LeaseResource::StorageReadRate {
                        demand_id: source_demand_id.clone(),
                    },
                    1,
                ),
                claim(
                    LeaseResource::StorageOperationsRate {
                        demand_id: source_demand_id.clone(),
                    },
                    1,
                ),
                claim(
                    LeaseResource::StorageQueue {
                        demand_id: source_demand_id.clone(),
                    },
                    1,
                ),
            ]);
        }
        let prepared_node = WorkNode {
            id: self.descriptor.work_node_id(self.operation),
            kind: WorkKind::Cache,
            domain: WorkDomain::Cpu,
            implementation: self.descriptor.work_implementation_id(self.operation),
            dependencies: BTreeSet::from([WorkDependency::Work(self.producer)]),
            claims,
            allocations: vec![AllocationUse {
                allocation: allocation_id.clone(),
                lifetime: ClaimLifetime::Work,
            }],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        };
        let release_id = WorkNodeId::new(format!(
            "prepared-release-{suffix}-{}",
            self.descriptor.identity()
        ));
        let release_node = WorkNode {
            id: release_id.clone(),
            kind: WorkKind::Release,
            domain: WorkDomain::Cpu,
            implementation: self.release_implementation,
            dependencies: BTreeSet::from([WorkDependency::Work(prepared_node.id.clone())]),
            claims: vec![
                claim(LeaseResource::Workers, 1),
                claim(
                    LeaseResource::IoBuffer(IoBufferKind::StorageManager),
                    reservation.resident_buffer_bytes(),
                ),
            ],
            allocations: vec![AllocationUse {
                allocation: allocation_id.clone(),
                lifetime: ClaimLifetime::Work,
            }],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        };
        let mut nodes = base
            .execution_dag()
            .nodes()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        nodes
            .iter_mut()
            .find(|node| node.id == self.publication_commit)
            .ok_or(PreparedArtifactPlanError::MissingPublicationCommit)?
            .dependencies
            .insert(WorkDependency::Work(release_id.clone()));
        nodes.extend([prepared_node.clone(), release_node]);

        let allocation = LogicalAllocation {
            id: allocation_id.clone(),
            bytes: reservation.resident_buffer_bytes(),
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::StorageManager),
            compatibility: compatibility.clone(),
            physical_slot: slot_id.clone(),
            lifetime: AllocationLifetime {
                acquire_at: prepared_node.id.clone(),
                release_after: BTreeSet::from([WorkDependency::Work(release_id.clone())]),
            },
        };
        let slot = PhysicalSlot {
            id: slot_id,
            lease_resource: LeaseResource::Memory {
                allocation_id: allocation_id.as_str().to_string(),
            },
            capacity_bytes: reservation.resident_buffer_bytes(),
            compatibility,
        };
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
                .chain([allocation])
                .collect(),
            physical_slots: base
                .execution_dag()
                .physical_slots()
                .values()
                .cloned()
                .chain([slot])
                .collect(),
            initial_knobs: base.execution_dag().initial_knobs().clone(),
            adaptations: base
                .execution_dag()
                .adaptations()
                .values()
                .cloned()
                .collect(),
        })?;
        let source_operations = self
            .source
            .map(PreparedArtifactSourceBinding::import_operations)
            .transpose()?
            .unwrap_or(0);
        let prepared_stage =
            StagePrediction::new(prepared_node.id, 1_000).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                reservation.persistent_cache_bytes().max(
                    reservation
                        .entry_bytes()
                        .checked_add(reservation.source_read_bytes())
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
                ),
                10_000_u64
                    .checked_add(source_operations)
                    .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
            )]);
        let release_stage = StagePrediction::new(release_id, 100).with_io(vec![IoPrediction::new(
            IoBufferKind::StorageManager,
            reservation.resident_buffer_bytes(),
            1,
        )]);
        let prediction = PlanPrediction::new(
            base.prediction().elapsed_nanos()
                + prepared_stage.elapsed_nanos()
                + release_stage.elapsed_nanos(),
            base.prediction().confidence(),
            base.prediction().uncertainty().to_vec(),
            base.prediction()
                .stages()
                .values()
                .cloned()
                .chain([prepared_stage, release_stage])
                .collect(),
        )?;
        let mut artifacts = base.artifacts().to_vec();
        if let Some(source) = self.source {
            artifacts.push(source.planned_artifact());
        }
        artifacts.extend([
            self.descriptor.planned_artifact(self.operation),
            self.descriptor.eviction_artifact(self.operation),
        ]);
        Ok(PhysicalWorkBinding::with_implementation_contract(
            base.implementation_contract().for_execution_dag(&dag)?,
            dag,
            prediction,
            artifacts,
            base.observation_transaction().clone(),
            base.publication_layouts().clone(),
            base.product_publication_authority(),
        )?)
    }
}

fn claim(resource: LeaseResource, amount: u64) -> ResourceClaim {
    ResourceClaim {
        resource,
        amount,
        lifetime: ClaimLifetime::Work,
    }
}

fn combine_count(base: CountDemand, fragment: u64) -> CountDemand {
    CountDemand::new(base.hard().max(fragment), base.preferred().max(fragment))
}

/// Failure to compose a prepared operation with an existing physical plan.
#[derive(Debug)]
pub enum PreparedArtifactPlanError {
    /// The store could not derive a bounded reservation.
    Prepared(PreparedArtifactError),
    /// The composed execution DAG is invalid.
    Execution(ExecutionError),
    /// The complete physical binding is inconsistent.
    Binding(PhysicalWorkBindingError),
    /// A cold load omitted its source or another operation supplied one.
    InvalidSourceBinding,
    /// The named publication commit was absent from the base plan.
    MissingPublicationCommit,
}

impl fmt::Display for PreparedArtifactPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prepared(error) => error.fmt(formatter),
            Self::Execution(error) => error.fmt(formatter),
            Self::Binding(error) => error.fmt(formatter),
            Self::InvalidSourceBinding => {
                formatter.write_str("invalid prepared load-source binding")
            }
            Self::MissingPublicationCommit => {
                formatter.write_str("prepared fragment publication commit is absent")
            }
        }
    }
}

impl Error for PreparedArtifactPlanError {}

impl From<PreparedArtifactError> for PreparedArtifactPlanError {
    fn from(error: PreparedArtifactError) -> Self {
        Self::Prepared(error)
    }
}

impl From<ExecutionError> for PreparedArtifactPlanError {
    fn from(error: ExecutionError) -> Self {
        Self::Execution(error)
    }
}

impl From<PhysicalWorkBindingError> for PreparedArtifactPlanError {
    fn from(error: PhysicalWorkBindingError) -> Self {
        Self::Binding(error)
    }
}
