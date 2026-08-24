// SPDX-License-Identifier: LGPL-3.0-or-later

//! Canonical composition of one prepared-artifact operation into physical work.

use super::*;
use crate::{
    AllocationAccess, AllocationId, AllocationLayout, AllocationLifetime, AllocationPurpose,
    AllocationUse, AlternativeId, CapacityDomainId, CapacityViewId, ClaimLifetime, CountDemand,
    DemandAlternative, ExecutionDag, ExecutionDagSpecification, ExecutionError,
    InitializationPolicy, IoBufferKind, IoPrediction, LeaseResource, LogicalAllocation,
    MemoryDemand, PhysicalSlot, PhysicalSlotId, PhysicalWorkBinding, PhysicalWorkBindingError,
    PlanPrediction, ResourceClaim, SlotCompatibility, StagePrediction, StorageDemand, StorageMode,
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
    load_source: Option<&'a PreparedArtifactLoadSource>,
}

impl<'a> PreparedArtifactPlanFragment<'a> {
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
            load_source: None,
        }
    }

    /// Bind the exact predecessor-owned source for a cold load.
    #[must_use]
    pub const fn with_load_source(mut self, source: &'a PreparedArtifactLoadSource) -> Self {
        self.load_source = Some(source);
        self
    }

    /// Compose this fragment with already validated T17/T28 physical work.
    pub fn compose(
        self,
        base: &PhysicalWorkBinding,
    ) -> Result<PhysicalWorkBinding, PreparedArtifactPlanError> {
        if matches!(self.operation, PreparedArtifactOperation::Load) != self.load_source.is_some() {
            return Err(PreparedArtifactPlanError::InvalidSourceBinding);
        }
        let reservation = self.store.reservation(self.descriptor, self.operation)?;
        let suffix = match self.operation {
            PreparedArtifactOperation::Generate => "generate",
            PreparedArtifactOperation::Load => "load",
            PreparedArtifactOperation::Reuse => "reuse",
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
        let demand_id = self.descriptor.storage_demand_id();
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

        let source_demands = self
            .load_source
            .map(PreparedArtifactLoadSource::storage_demands)
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
        let prepared_stage =
            StagePrediction::new(prepared_node.id, 1_000).with_io(vec![IoPrediction::new(
                IoBufferKind::StorageManager,
                reservation.persistent_cache_bytes().max(
                    reservation
                        .entry_bytes()
                        .checked_add(reservation.source_read_bytes())
                        .ok_or(PreparedArtifactError::ArtifactTooLarge)?,
                ),
                10_000,
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
        if let Some(source) = self.load_source {
            artifacts.push(source.planned_artifact());
        }
        artifacts.extend([
            self.descriptor.planned_artifact(self.operation),
            self.descriptor.eviction_artifact(self.operation),
        ]);
        Ok(PhysicalWorkBinding::new(
            dag,
            prediction,
            artifacts,
            base.observation_transaction().clone(),
            base.publication_layouts().clone(),
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
