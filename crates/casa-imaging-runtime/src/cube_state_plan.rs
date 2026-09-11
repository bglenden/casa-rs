// SPDX-License-Identifier: LGPL-3.0-or-later

//! Physical ownership of channel-local model and normal generations.

use std::{
    collections::BTreeSet,
    io,
    sync::{Arc, OnceLock},
};

use casa_imaging_model::{CompiledProblem, ModelSample};
use casa_imaging_reconstruction::{
    ModelStoragePlan, SpectralOperatorSpecification,
    runtime_adapter::{ChannelNormalStorageRequirement, NormalStoragePlan},
};

use crate::{
    paged_cube_state::{
        CubeArrayLayout, CubeBackingMetrics, PagedModelStorageFactory, PagedNormalStorageFactory,
    },
    *,
};

/// Reservations are attached at the scheduler's existing export boundaries.
/// Every physical backing retains this owner after its arrays and directory.
#[derive(Debug, Default)]
struct CubeStateRetention {
    capacity: OnceLock<RetainedArtifactPermit>,
    heap: OnceLock<RetainedArtifactPermit>,
}

/// A candidate owns one new model backing and one normal backing per domain.
/// Prior generations keep their own reservations while this generation is built.
#[derive(Debug)]
pub(crate) struct CubeStatePlan {
    model: Arc<PagedModelStorageFactory>,
    normal: Arc<PagedNormalStorageFactory>,
    retention: Arc<CubeStateRetention>,
    metrics: Arc<CubeBackingMetrics>,
    model_window_samples: usize,
    normal_window_channels: usize,
    acquire: WorkNodeId,
    terminal: WorkNodeId,
    heap: LogicalAllocation,
    scratch: LogicalAllocation,
    storage_id: String,
    storage_bytes: u64,
    file_handles: u64,
}

impl CubeStatePlan {
    pub(crate) fn new(
        problem: &CompiledProblem,
        storage: &ManagedSpillStorage,
        window_channels: usize,
        acquire: WorkNodeId,
        terminal: WorkNodeId,
    ) -> io::Result<Self> {
        let specification =
            SpectralOperatorSpecification::new(problem).map_err(io::Error::other)?;
        let requirements =
            ChannelNormalStorageRequirement::for_specification(&specification, window_channels)
                .map_err(io::Error::other)?;
        let shape = problem.model_lifecycle().target();
        let model_window_samples = shape.domains().iter().try_fold(0usize, |largest, domain| {
            let [width, height] = domain.pixels();
            width
                .checked_mul(height)
                .and_then(|n| n.checked_mul(shape.polarizations()))
                .map(|n| largest.max(n))
                .ok_or_else(overflow)
        })?;
        let retention = Arc::new(CubeStateRetention::default());
        let metrics = Arc::new(CubeBackingMetrics::default());
        let model_layout = CubeArrayLayout::new(
            shape.sample_count(),
            model_window_samples,
            model_window_samples,
            1,
        )
        .map_err(io::Error::other)?;
        let model = Arc::new(
            PagedModelStorageFactory::new(
                storage.directory(),
                model_layout,
                retention.clone(),
                metrics.clone(),
            )
            .map_err(io::Error::other)?,
        );
        let model_ledger = model.ledger().map_err(io::Error::other)?;
        let layouts = requirements
            .iter()
            .map(|requirement| {
                CubeArrayLayout::new(
                    requirement.scalar_capacity(),
                    requirement.complex_plane_scalars(),
                    requirement.maximum_window_scalars(),
                    1,
                )
                .map_err(io::Error::other)
            })
            .collect::<io::Result<Box<[_]>>>()?;
        let mut retained_bytes = model_ledger.retained_bytes;
        let mut storage_bytes = model_ledger.storage_bytes;
        let mut file_handles = model_ledger.file_handles;
        let mut scratch_bytes = model_ledger
            .read_scratch_bytes
            .max(model_ledger.write_scratch_bytes)
            .max(model_ledger.flush_scratch_bytes)
            .checked_add(
                model_window_samples
                    .checked_mul(size_of::<ModelSample>())
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
        for (layout, requirement) in layouts.iter().zip(&requirements) {
            let ledger = layout
                .normal_ledger(storage.directory())
                .map_err(io::Error::other)?;
            retained_bytes = add(
                retained_bytes,
                add(ledger.retained_bytes, requirement.retained_metadata_bytes())?,
            )?;
            storage_bytes = add(storage_bytes, ledger.storage_bytes)?;
            file_handles = add(file_handles, ledger.file_handles)?;
            // The normal owner converts a complex source to f64 before the
            // backing's ndarray copy. Both coexist with the source primitive.
            let conversion = requirement
                .maximum_window_scalars()
                .checked_mul(size_of::<f64>())
                .ok_or_else(overflow)?;
            scratch_bytes = scratch_bytes.max(
                add(conversion, ledger.write_scratch_bytes)?
                    .max(ledger.read_scratch_bytes)
                    .max(ledger.flush_scratch_bytes),
            );
        }
        let normal = Arc::new(PagedNormalStorageFactory::new(
            storage.directory(),
            layouts,
            retention.clone(),
            metrics.clone(),
        ));
        retained_bytes = add(
            retained_bytes,
            model.owned_metadata_bytes().map_err(io::Error::other)?,
        )?;
        retained_bytes = add(
            retained_bytes,
            normal.owned_metadata_bytes().map_err(io::Error::other)?,
        )?;
        retained_bytes = add(
            retained_bytes,
            size_of::<Self>()
                + size_of::<CubeStateRetention>()
                + size_of::<CubeBackingMetrics>()
                + 10 * size_of::<usize>(),
        )?;
        let storage_id = format!("cube-state-storage-{}", acquire.as_str());
        let heap_id = format!("cube-state-retained-{}", acquire.as_str());
        let capacity_resources = [
            LeaseResource::Storage {
                demand_id: storage_id.clone(),
                use_kind: StorageUseKind::Temporary,
            },
            LeaseResource::FileDescriptors,
        ];
        let heap_resources = [LeaseResource::Memory {
            allocation_id: heap_id.clone(),
        }];
        let permit_bytes = RetainedArtifactPermit::heap_bytes_for_resources(
            &capacity_resources,
            0,
            "host-memory",
            storage.resources().domain().as_str(),
        )
        .and_then(|bytes| {
            RetainedArtifactPermit::heap_bytes_for_resources(
                &heap_resources,
                1,
                "host-memory",
                storage.resources().domain().as_str(),
            )
            .and_then(|other| bytes.checked_add(other))
        })
        .ok_or_else(overflow)?;
        let retained_bytes = as_u64(retained_bytes)?
            .checked_add(permit_bytes)
            .ok_or_else(overflow)?;
        let heap = allocation(heap_id, retained_bytes, &acquire, &terminal, true);
        let scratch = allocation(
            format!("cube-state-access-{}", acquire.as_str()),
            as_u64(scratch_bytes)?,
            &acquire,
            &terminal,
            false,
        );
        Ok(Self {
            model,
            normal,
            retention,
            metrics,
            model_window_samples,
            normal_window_channels: window_channels,
            acquire,
            terminal,
            heap,
            scratch,
            storage_id,
            storage_bytes: as_u64(storage_bytes)?,
            file_handles: as_u64(file_handles)?,
        })
    }

    /// Allocation capabilities must be live before any mutable backing is made.
    pub(crate) fn model_storage(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> io::Result<ModelStoragePlan> {
        if context.node().id != self.acquire
            || !context
                .node()
                .allocations
                .iter()
                .any(|use_| use_.allocation == self.heap.id)
            || !context.node().claims.iter().any(|claim| {
                claim.resource == self.storage_resource()
                    && claim.amount == self.storage_bytes
                    && claim.lifetime == ClaimLifetime::Artifact
            })
            || !context.node().claims.iter().any(|claim| {
                claim.resource == LeaseResource::FileDescriptors
                    && claim.amount == self.file_handles
                    && claim.lifetime == ClaimLifetime::Artifact
            })
        {
            return Err(io::Error::other(
                "cube storage lacks its plan-issued allocation and capacity claims",
            ));
        }
        ModelStoragePlan::new(self.model.clone(), self.model_window_samples)
            .map_err(io::Error::other)
    }

    pub(crate) fn normal_storage(&self) -> io::Result<NormalStoragePlan> {
        if self.retention.capacity.get().is_none() {
            return Err(io::Error::other(
                "cube normal creation precedes backing-capacity retention",
            ));
        }
        NormalStoragePlan::new(self.normal.clone(), self.normal_window_channels)
            .map_err(io::Error::other)
    }

    pub(crate) fn log_measurements(&self, node: &WorkNodeId) {
        if std::env::var_os("CASA_RS_TRACE_IMAGING_STAGE_TIMING").is_some() {
            eprintln!(
                "imaging_cube_backing_measurements node={} planned_retained_bytes={} planned_access_scratch_bytes={} planned_storage_bytes={} planned_file_handles={} observed={:?}",
                node.as_str(),
                self.heap.bytes,
                self.scratch.bytes,
                self.storage_bytes,
                self.file_handles,
                self.metrics.snapshot()
            );
        }
    }

    pub(crate) fn retains_at(&self, node: &WorkNodeId) -> bool {
        node == &self.acquire || node == &self.terminal
    }

    pub(crate) fn retain(
        &self,
        node: &WorkNodeId,
        permit: RetainedArtifactPermit,
    ) -> io::Result<()> {
        if node == &self.acquire {
            if !permit.covers_exact_resources(&[
                (self.storage_resource(), self.storage_bytes),
                (LeaseResource::FileDescriptors, self.file_handles),
            ]) {
                return Err(io::Error::other(
                    "cube backing reservation differs from its admitted capacity",
                ));
            }
            self.retention
                .capacity
                .set(permit)
                .map_err(|_| io::Error::other("cube backing capacity was retained twice"))
        } else if node == &self.terminal {
            if !permit.covers_exact_immutable_allocation(node, &self.heap) {
                return Err(io::Error::other(
                    "cube backing cache export differs from its admitted allocation",
                ));
            }
            self.retention
                .heap
                .set(permit)
                .map_err(|_| io::Error::other("cube backing cache was retained twice"))
        } else {
            Err(io::Error::other(
                "cube backing reservation was exported by an unrelated node",
            ))
        }
    }

    fn storage_resource(&self) -> LeaseResource {
        LeaseResource::Storage {
            demand_id: self.storage_id.clone(),
            use_kind: StorageUseKind::Temporary,
        }
    }

    pub(crate) fn compose<R: ImplementationRegistry>(
        &self,
        registry: &R,
        implementation: WorkImplementationId,
        storage: &ManagedSpillStorage,
        base: PhysicalWorkBinding,
        replay: &WorkNodeId,
        reconcile: &WorkNodeId,
    ) -> Result<PhysicalWorkBinding, SpectralCyclePlanError> {
        // Cube I/O runs synchronously on the coordinator, using the existing
        // transaction queue. It does not add a concurrent reader or writer.
        let existing_queue = base
            .execution_dag()
            .resource_alternative()
            .demand
            .queues
            .iter()
            .find(|queue| &queue.resource == storage.resources().queue())
            .map(|queue| queue.demand_id.clone());
        let new_queue = existing_queue.is_none().then(|| QueueDemand {
            demand_id: format!("{}-queue", self.storage_id),
            resource: storage.resources().queue().clone(),
            slots: CountDemand::new(1, 1),
        });
        let queue = LeaseResource::Queue {
            demand_id: existing_queue.unwrap_or_else(|| {
                new_queue
                    .as_ref()
                    .expect("new queue needed")
                    .demand_id
                    .clone()
            }),
        };
        let mut nodes = base
            .execution_dag()
            .nodes()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let users = BTreeSet::from([
            self.acquire.clone(),
            replay.clone(),
            reconcile.clone(),
            self.terminal.clone(),
        ]);
        for node in &mut nodes {
            if !users.contains(&node.id) {
                continue;
            }
            let lifetime = if node.fences.contains(&FenceKind::Io) {
                ClaimLifetime::through_fence(FenceKind::Io)
            } else {
                ClaimLifetime::Work
            };
            node.allocations.extend([
                AllocationUse {
                    allocation: self.heap.id.clone(),
                    lifetime: lifetime.clone(),
                },
                AllocationUse {
                    allocation: self.scratch.id.clone(),
                    lifetime: lifetime.clone(),
                },
            ]);
            node.claims.extend([
                ResourceClaim {
                    resource: LeaseResource::StorageReadRate {
                        demand_id: self.storage_id.clone(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::StorageWriteRate {
                        demand_id: self.storage_id.clone(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
            ]);
            if !node.claims.iter().any(|claim| claim.resource == queue) {
                node.claims.push(ResourceClaim {
                    resource: queue.clone(),
                    amount: 1,
                    lifetime,
                });
            }
            if node.id == self.acquire {
                node.claims.extend([
                    ResourceClaim {
                        resource: self.storage_resource(),
                        amount: self.storage_bytes,
                        lifetime: ClaimLifetime::Artifact,
                    },
                    ResourceClaim {
                        resource: LeaseResource::FileDescriptors,
                        amount: self.file_handles,
                        lifetime: ClaimLifetime::Artifact,
                    },
                ]);
            }
        }
        let mut alternative = base.execution_dag().resource_alternative().clone();
        alternative.demand.queues.extend(new_queue);
        for allocation in [&self.heap, &self.scratch] {
            alternative.demand.memory.push(MemoryDemand {
                allocation_id: allocation.id.as_str().to_owned(),
                hard_bytes: allocation.bytes,
                preferred_bytes: allocation.bytes,
                views: vec![CapacityViewId::new("host-memory")],
            });
        }
        alternative.demand.file_descriptors = CountDemand::new(
            alternative
                .demand
                .file_descriptors
                .hard()
                .checked_add(self.file_handles)
                .ok_or(SpectralCyclePlanError::Overflow)?,
            alternative
                .demand
                .file_descriptors
                .preferred()
                .checked_add(self.file_handles)
                .ok_or(SpectralCyclePlanError::Overflow)?,
        );
        alternative.demand.storage.push(StorageDemand {
            demand_id: self.storage_id.clone(),
            domain: storage.resources().domain().clone(),
            temporary_bytes: self.storage_bytes,
            staged_output_bytes: 0,
            final_output_bytes: 0,
            persistent_cache_bytes: 0,
            read_rate: CountDemand::new(1, 1),
            write_rate: CountDemand::new(1, 1),
            operations_rate: CountDemand::zero(),
            queue_slots: CountDemand::zero(),
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
                .chain([self.heap.clone(), self.scratch.clone()])
                .collect(),
            physical_slots: base
                .execution_dag()
                .physical_slots()
                .values()
                .cloned()
                .chain([&self.heap, &self.scratch].map(|allocation| PhysicalSlot {
                    id: allocation.physical_slot.clone(),
                    lease_resource: LeaseResource::Memory {
                        allocation_id: allocation.id.as_str().to_owned(),
                    },
                    capacity_bytes: allocation.bytes,
                    compatibility: allocation.compatibility.clone(),
                }))
                .collect(),
            initial_knobs: base.execution_dag().initial_knobs().clone(),
            adaptations: base
                .execution_dag()
                .adaptations()
                .values()
                .cloned()
                .collect(),
        })?;
        let catalog = ImplementationContractCatalog::from_registry(registry, [implementation])?;
        Ok(PhysicalWorkBinding::new_reconstruction(
            catalog,
            dag,
            base.prediction().clone(),
            base.artifacts().to_vec(),
            base.observation_transaction().clone(),
            base.publication_layouts().clone(),
        )?)
    }
}

fn allocation(
    id: String,
    bytes: u64,
    acquire: &WorkNodeId,
    terminal: &WorkNodeId,
    retained: bool,
) -> LogicalAllocation {
    LogicalAllocation {
        physical_slot: PhysicalSlotId::new(format!("{id}-slot")),
        id: AllocationId::new(id),
        bytes,
        purpose: AllocationPurpose::Data,
        compatibility: SlotCompatibility {
            memory_domain: CapacityDomainId::new("host-memory"),
            views: BTreeSet::from([CapacityViewId::new("host-memory")]),
            alignment_bytes: 64,
            storage_mode: StorageMode::Host,
            layout: AllocationLayout::new(if retained {
                "cube-state-cache-and-metadata"
            } else {
                "cube-state-access"
            }),
            initialization: InitializationPolicy::OverwriteBeforeRead,
            access: if retained {
                AllocationAccess::ReadOnly
            } else {
                AllocationAccess::ReadWrite
            },
        },
        lifetime: AllocationLifetime {
            acquire_at: acquire.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(terminal.clone())]),
            disposition: if retained {
                AllocationDisposition::ExportImmutableArtifact {
                    owner_node: terminal.clone(),
                }
            } else {
                AllocationDisposition::Release
            },
        },
    }
}

fn overflow() -> io::Error {
    io::Error::other("cube state storage layout overflow")
}
fn add(left: usize, right: usize) -> io::Result<usize> {
    left.checked_add(right).ok_or_else(overflow)
}
fn as_u64(value: usize) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| overflow())
}
