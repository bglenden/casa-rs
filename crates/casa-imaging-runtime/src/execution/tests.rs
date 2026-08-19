// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};
use super::*;
use crate::{
    Accelerator, AcceleratorDemand, AcceleratorId, AcceleratorKind, AlternativeId, CacheDemand,
    CapabilityPredicate, CapacityDomainId, CapacityViewId, CountDemand, CpuClassCapacity,
    DemandAlternative, DemandEnvelope, ExternalPressure, HostInventory, IoBufferDemand,
    MemoryCapacityDomain, MemoryCapacityKind, MemoryDemand, MemoryView, MemoryViewKind,
    QueueDemand, QueueResource, QueueResourceId, QuiescencePoint, RateDemand, RateResource,
    RateResourceId, RateUnit, ResourceAuthority, ResourceHeadroom, ResourcePolicy,
    ResourceTopology, RuntimeOverheadDemand, ScalingMetadata,
};

fn cpu_node(id: &str, dependencies: BTreeSet<WorkDependency>) -> WorkNode {
    WorkNode {
        id: WorkNodeId::new(id),
        kind: WorkKind::Compute,
        domain: WorkDomain::Cpu,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies,
        claims: vec![ResourceClaim {
            resource: crate::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }],
        allocations: Vec::new(),
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    }
}

fn synchronization_node(id: &str, dependencies: BTreeSet<WorkDependency>) -> WorkNode {
    WorkNode {
        id: WorkNodeId::new(id),
        kind: WorkKind::Synchronization,
        domain: WorkDomain::Control,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies,
        claims: Vec::new(),
        allocations: Vec::new(),
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    }
}

fn plan_spec(nodes: Vec<WorkNode>) -> ExecutionDagSpecification {
    let host = CapacityViewId::new("host-memory");
    ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("cpu-reference"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: host,
                memory: Vec::new(),
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: Vec::new(),
                rates: Vec::new(),
                caches: CacheDemand::zero(),
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: Vec::new(),
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand::zero(),
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: 1,
                maximum_batch_size: 8,
                maximum_tile_width: 8,
                maximum_tile_height: 8,
                maximum_slab_depth: 8,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
        },
        nodes,
        logical_allocations: Vec::new(),
        physical_slots: Vec::new(),
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: Vec::new(),
    }
}

fn cpu_authority() -> ResourceAuthority {
    let domain = CapacityDomainId::new("host-memory");
    let view = CapacityViewId::new("host-memory");
    ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1_024,
            }],
            memory_views: vec![MemoryView {
                id: view,
                domain: domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: Vec::new(),
            queue_resources: Vec::new(),
            logical_cpu_threads: 2,
            performance_cpu_cores: CpuClassCapacity::Known(2),
            cache_capacity_bytes: 1_024,
            lock_capacity: 8,
            file_descriptor_capacity: 8,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_024)]),
            available_cpu_threads: 2,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::new(),
            queue_available_slots: BTreeMap::new(),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_024,
            available_locks: 8,
            available_file_descriptors: 8,
        },
    })
    .expect("valid scheduler test inventory")
}

fn unified_authority() -> ResourceAuthority {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let metal = CapacityViewId::new("metal-memory");
    let accelerator = AcceleratorId::new("metal-0");
    let command_queue = QueueResourceId::new("metal-command-queue");
    let io_rate = RateResourceId::new("io-rate");
    let io_queue = QueueResourceId::new("io-queue");
    ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Unified,
                capacity_bytes: 1_024,
            }],
            memory_views: vec![
                MemoryView {
                    id: host,
                    domain: domain.clone(),
                    kind: MemoryViewKind::Host,
                },
                MemoryView {
                    id: metal.clone(),
                    domain: domain.clone(),
                    kind: MemoryViewKind::Metal,
                },
            ],
            accelerators: vec![Accelerator {
                id: accelerator.clone(),
                kind: AcceleratorKind::Metal,
                memory_view: metal,
                command_queue: command_queue.clone(),
                occupancy_slots: 1,
            }],
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: vec![RateResource::new(
                io_rate.clone(),
                RateUnit::BytesPerSecond,
                100,
            )],
            queue_resources: vec![
                QueueResource::new(command_queue.clone(), 1),
                QueueResource::new(io_queue.clone(), 1),
            ],
            logical_cpu_threads: 2,
            performance_cpu_cores: CpuClassCapacity::Known(2),
            cache_capacity_bytes: 1_024,
            lock_capacity: 8,
            file_descriptor_capacity: 8,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_024)]),
            available_cpu_threads: 2,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::from([(io_rate, 100)]),
            queue_available_slots: BTreeMap::from([(command_queue, 1), (io_queue, 1)]),
            accelerator_available_slots: BTreeMap::from([(accelerator, 1)]),
            cache_available_bytes: 1_024,
            available_locks: 8,
            available_file_descriptors: 8,
        },
    })
    .expect("valid Apple-style unified scheduler inventory")
}

fn io_authority() -> ResourceAuthority {
    let domain = CapacityDomainId::new("host-memory");
    let view = CapacityViewId::new("host-memory");
    let rate = RateResourceId::new("io-rate");
    let queue = QueueResourceId::new("io-queue");
    ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1_024,
            }],
            memory_views: vec![MemoryView {
                id: view,
                domain: domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: vec![RateResource::new(
                rate.clone(),
                RateUnit::BytesPerSecond,
                100,
            )],
            queue_resources: vec![QueueResource::new(queue.clone(), 1)],
            logical_cpu_threads: 2,
            performance_cpu_cores: CpuClassCapacity::Known(2),
            cache_capacity_bytes: 1_024,
            lock_capacity: 8,
            file_descriptor_capacity: 8,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_024)]),
            available_cpu_threads: 2,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::from([(rate, 100)]),
            queue_available_slots: BTreeMap::from([(queue, 1)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_024,
            available_locks: 8,
            available_file_descriptors: 8,
        },
    })
    .expect("valid scheduler I/O inventory")
}

fn inactive_release_predecessor_plan(fenced_predecessor: bool) -> (ExecutionDag, WorkNodeId) {
    let active_prepare_id = WorkNodeId::new("0-prepare-active");
    let inactive_prepare_id = WorkNodeId::new("z-prepare-inactive");
    let inactive_release_id = WorkNodeId::new("m-release-inactive");
    let active_release_id = WorkNodeId::new("a-release-active");
    let active_allocation = AllocationId::new("active-pages");
    let inactive_allocation = AllocationId::new("inactive-pages");
    let work_claim = |kind| ResourceClaim {
        resource: crate::LeaseResource::IoBuffer(kind),
        amount: 100,
        lifetime: ClaimLifetime::Work,
    };
    let work_use = |allocation| AllocationUse {
        allocation,
        lifetime: ClaimLifetime::Work,
    };
    let mut active_prepare = cpu_node(active_prepare_id.as_str(), BTreeSet::new());
    active_prepare.kind = WorkKind::Cache;
    active_prepare.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    active_prepare
        .claims
        .push(work_claim(crate::IoBufferKind::MappedPageCache));
    active_prepare
        .allocations
        .push(work_use(active_allocation.clone()));
    let mut inactive_prepare = cpu_node(inactive_prepare_id.as_str(), BTreeSet::new());
    inactive_prepare.kind = WorkKind::Cache;
    inactive_prepare.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    inactive_prepare
        .claims
        .push(work_claim(crate::IoBufferKind::MappedPageCache));
    inactive_prepare
        .allocations
        .push(work_use(inactive_allocation.clone()));
    let mut inactive_release = cpu_node(
        inactive_release_id.as_str(),
        BTreeSet::from([WorkDependency::Work(inactive_prepare_id.clone())]),
    );
    inactive_release.kind = WorkKind::Release;
    if fenced_predecessor {
        inactive_release.domain = WorkDomain::Io;
        inactive_release.claims = vec![
            ResourceClaim {
                resource: crate::LeaseResource::Rate {
                    demand_id: "io-rate".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            },
            ResourceClaim {
                resource: crate::LeaseResource::Queue {
                    demand_id: "io-queue".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            },
            ResourceClaim {
                resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::MappedPageCache),
                amount: 100,
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            },
        ];
        inactive_release.allocations = vec![AllocationUse {
            allocation: inactive_allocation.clone(),
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        }];
        inactive_release.fences = BTreeSet::from([FenceKind::Io]);
    } else {
        inactive_release
            .claims
            .push(work_claim(crate::IoBufferKind::MappedPageCache));
        inactive_release
            .allocations
            .push(work_use(inactive_allocation.clone()));
    }
    let inactive_dependency = if fenced_predecessor {
        WorkDependency::Fence(FenceId::new(inactive_release_id.clone(), FenceKind::Io))
    } else {
        WorkDependency::Work(inactive_release_id.clone())
    };
    let mut active_release = cpu_node(
        active_release_id.as_str(),
        BTreeSet::from([
            WorkDependency::Work(active_prepare_id.clone()),
            inactive_dependency,
        ]),
    );
    active_release.kind = WorkKind::Release;
    active_release
        .claims
        .push(work_claim(crate::IoBufferKind::MappedPageCache));
    active_release
        .allocations
        .push(work_use(active_allocation.clone()));
    let mut specification = plan_spec(vec![
        active_prepare,
        inactive_prepare,
        inactive_release,
        active_release,
    ]);
    specification.resource_alternative.demand.memory = ["active-slot", "inactive-slot"]
        .map(|allocation_id| MemoryDemand {
            allocation_id: allocation_id.to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![CapacityViewId::new("host-memory")],
        })
        .to_vec();
    specification
        .resource_alternative
        .demand
        .io_buffers
        .mapped_page_cache_bytes = 100;
    specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 100,
        preferred_resident_bytes: 100,
    };
    specification.initial_knobs.cache_retention_bytes = 100;
    if fenced_predecessor {
        specification.resource_alternative.demand.rates = vec![RateDemand {
            demand_id: "io-rate".to_string(),
            resource: RateResourceId::new("io-rate"),
            amount: CountDemand::new(1, 1),
        }];
        specification.resource_alternative.demand.queues = vec![QueueDemand {
            demand_id: "io-queue".to_string(),
            resource: QueueResourceId::new("io-queue"),
            slots: CountDemand::new(1, 1),
        }];
    }
    let compatibility = |layout| SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(layout),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let active_compatibility = compatibility("active-pages");
    let inactive_compatibility = compatibility("inactive-pages");
    let inactive_release_after = if fenced_predecessor {
        WorkDependency::Fence(FenceId::new(inactive_release_id, FenceKind::Io))
    } else {
        WorkDependency::Work(inactive_release_id)
    };
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: active_allocation,
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::MappedPageCache),
            compatibility: active_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("active-slot"),
            lifetime: AllocationLifetime {
                acquire_at: active_prepare_id,
                release_after: BTreeSet::from([WorkDependency::Work(active_release_id.clone())]),
            },
        },
        LogicalAllocation {
            id: inactive_allocation,
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::MappedPageCache),
            compatibility: inactive_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("inactive-slot"),
            lifetime: AllocationLifetime {
                acquire_at: inactive_prepare_id,
                release_after: BTreeSet::from([inactive_release_after]),
            },
        },
    ];
    specification.physical_slots = vec![
        PhysicalSlot {
            id: PhysicalSlotId::new("active-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "active-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: active_compatibility,
        },
        PhysicalSlot {
            id: PhysicalSlotId::new("inactive-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "inactive-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: inactive_compatibility,
        },
    ];
    (
        ExecutionDag::new(specification).expect("valid cleanup projection plan"),
        active_release_id,
    )
}

#[test]
fn execution_plan_rejects_a_dependency_cycle() {
    let first = cpu_node(
        "first",
        BTreeSet::from([WorkDependency::Work(WorkNodeId::new("second"))]),
    );
    let second = cpu_node(
        "second",
        BTreeSet::from([WorkDependency::Work(WorkNodeId::new("first"))]),
    );

    let error = ExecutionDag::new(plan_spec(vec![first, second]))
        .expect_err("cyclic work is not an executable plan");

    assert!(matches!(error, ExecutionError::InvalidPlan(message) if message.contains("cycle")));
}

#[test]
fn physical_work_identity_is_canonical_and_changes_with_work() {
    let first = cpu_node("first", BTreeSet::new());
    let second = cpu_node("second", BTreeSet::new());
    let rate = |id: &str| RateDemand {
        demand_id: id.to_string(),
        resource: RateResourceId::new(format!("{id}-resource")),
        amount: CountDemand::new(1, 1),
    };
    let mut canonical_specification = plan_spec(vec![first.clone(), second.clone()]);
    canonical_specification.resource_alternative.demand.rates =
        vec![rate("a-rate"), rate("z-rate")];
    let canonical = ExecutionDag::new(canonical_specification).expect("canonical physical work");
    let mut reordered_specification = plan_spec(vec![second, first]);
    reordered_specification.resource_alternative.demand.rates =
        vec![rate("z-rate"), rate("a-rate")];
    let reordered = ExecutionDag::new(reordered_specification).expect("reordered physical work");
    assert_eq!(canonical.physical_work_id(), reordered.physical_work_id());
    assert_eq!(canonical, reordered);

    let mut changed = cpu_node("first", BTreeSet::new());
    changed.kind = WorkKind::DataCensus;
    let changed = ExecutionDag::new(plan_spec(vec![changed])).expect("changed physical work");
    assert_ne!(canonical.physical_work_id(), changed.physical_work_id());
}

#[test]
fn explicit_work_kinds_cannot_hide_their_resource_contracts() {
    for kind in [
        WorkKind::Cache,
        WorkKind::FftPlanning,
        WorkKind::Jit,
        WorkKind::Transfer,
        WorkKind::Spill,
        WorkKind::Prefetch,
        WorkKind::Io,
        WorkKind::ObservationRead,
        WorkKind::Writeback,
        WorkKind::Publication,
        WorkKind::Release,
        WorkKind::Synchronization,
    ] {
        let mut node = cpu_node("work", BTreeSet::new());
        node.kind = kind;
        assert!(
            ExecutionDag::new(plan_spec(vec![node])).is_err(),
            "{kind:?} must declare its typed domain and resources"
        );
    }
}

#[test]
fn scheduler_rejects_discrete_metal_memory_instead_of_inventing_a_mac_model() {
    let host_domain = CapacityDomainId::new("host-memory");
    let device_domain = CapacityDomainId::new("device-memory");
    let host_view = CapacityViewId::new("host-memory");
    let metal_view = CapacityViewId::new("metal-memory");
    let accelerator = AcceleratorId::new("metal-0");
    let command_queue = QueueResourceId::new("metal-command-queue");
    let authority = ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![
                MemoryCapacityDomain {
                    id: host_domain.clone(),
                    kind: MemoryCapacityKind::Host,
                    capacity_bytes: 1_024,
                },
                MemoryCapacityDomain {
                    id: device_domain.clone(),
                    kind: MemoryCapacityKind::DevicePrivate,
                    capacity_bytes: 1_024,
                },
            ],
            memory_views: vec![
                MemoryView {
                    id: host_view,
                    domain: host_domain.clone(),
                    kind: MemoryViewKind::Host,
                },
                MemoryView {
                    id: metal_view.clone(),
                    domain: device_domain.clone(),
                    kind: MemoryViewKind::Metal,
                },
            ],
            accelerators: vec![Accelerator {
                id: accelerator.clone(),
                kind: AcceleratorKind::Metal,
                memory_view: metal_view,
                command_queue: command_queue.clone(),
                occupancy_slots: 1,
            }],
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: Vec::new(),
            queue_resources: vec![QueueResource::new(command_queue.clone(), 1)],
            logical_cpu_threads: 2,
            performance_cpu_cores: CpuClassCapacity::Known(2),
            cache_capacity_bytes: 1_024,
            lock_capacity: 8,
            file_descriptor_capacity: 8,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(host_domain, 1_024), (device_domain, 1_024)]),
            available_cpu_threads: 2,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::new(),
            queue_available_slots: BTreeMap::from([(command_queue, 1)]),
            accelerator_available_slots: BTreeMap::from([(accelerator.clone(), 1)]),
            cache_available_bytes: 1_024,
            available_locks: 8,
            available_file_descriptors: 8,
        },
    })
    .expect("resource layer can describe a topology the Apple scheduler rejects");
    let lifetime = ClaimLifetime::through_fence(FenceKind::Device);
    let node = WorkNode {
        id: WorkNodeId::new("metal-work"),
        kind: WorkKind::Compute,
        domain: WorkDomain::Metal {
            demand_id: "metal".to_string(),
        },
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::new(),
        claims: vec![
            ResourceClaim {
                resource: crate::LeaseResource::Accelerator {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: lifetime.clone(),
            },
            ResourceClaim {
                resource: crate::LeaseResource::AcceleratorCommandQueue {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime,
            },
        ],
        allocations: Vec::new(),
        fences: BTreeSet::from([FenceKind::Device]),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![node]);
    specification.resource_alternative.demand.accelerators = vec![AcceleratorDemand {
        demand_id: "metal".to_string(),
        accelerator,
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    }];
    let plan = ExecutionDag::new(specification).expect("valid declared Metal work");

    assert!(matches!(
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &authority, None),
        Err(ExecutionError::InvalidPlan(message)) if message.contains("unified")
    ));
}

#[test]
fn scheduler_dispatches_ready_work_deterministically_under_lease_limits() {
    let mut specification = plan_spec(vec![
        cpu_node("z-last", BTreeSet::new()),
        cpu_node("a-first", BTreeSet::new()),
    ]);
    specification.resource_alternative.demand.workers = CountDemand::new(2, 2);
    specification.resource_alternative.scaling.maximum_workers = 2;
    let dag = ExecutionDag::new(specification).expect("valid concurrent work");
    let plan = dag;
    let authority = cpu_authority();
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &authority, None)
            .expect("admitted scheduler");
    assert!(scheduler.lease_epoch().is_some());
    assert_eq!(scheduler.knobs(), &ExecutionKnobs::serial());

    let SchedulerAction::Work(first) = scheduler.next_action().expect("first dispatch") else {
        panic!("first scheduler action must dispatch work");
    };
    assert_eq!(first.node().id, WorkNodeId::new("a-first"));
    assert_eq!(
        first.node().implementation,
        WorkImplementationId::new("cpu-reference")
    );

    assert_eq!(
        scheduler.next_action().expect("capacity wait"),
        SchedulerAction::Waiting {
            running_work: 1,
            pending_fences: 0,
        }
    );
    scheduler
        .finish_work(first.node().id.clone(), WorkResult::Succeeded)
        .expect("first work succeeds");

    let SchedulerAction::Work(second) = scheduler.next_action().expect("second dispatch") else {
        panic!("second scheduler action must dispatch work");
    };
    assert_eq!(second.node().id, WorkNodeId::new("z-last"));
    scheduler
        .finish_work(second.node().id.clone(), WorkResult::Succeeded)
        .expect("second work succeeds");
    assert_eq!(
        scheduler.next_action().expect("terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Succeeded)
    );
}

#[test]
fn unified_physical_slot_reuse_waits_for_every_declared_fence() {
    let domain = CapacityDomainId::new("unified-memory");
    let views = BTreeSet::from([
        CapacityViewId::new("host-memory"),
        CapacityViewId::new("metal-memory"),
    ]);
    let compatibility = SlotCompatibility {
        memory_domain: domain,
        views: views.clone(),
        alignment_bytes: 64,
        storage_mode: StorageMode::MetalShared,
        layout: AllocationLayout::new("f32-grid-row-major"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let lease_resource = crate::LeaseResource::Memory {
        allocation_id: "shared-slot".to_string(),
    };
    let compute_id = WorkNodeId::new("a-compute");
    let io_id = WorkNodeId::new("b-io");
    let writeback_id = WorkNodeId::new("c-writeback");
    let publication_id = WorkNodeId::new("d-publication");
    let reuse_id = WorkNodeId::new("e-reuse");
    let device_fence = FenceId::new(compute_id.clone(), FenceKind::Device);
    let io_fence = FenceId::new(io_id.clone(), FenceKind::Io);
    let writeback_io_fence = FenceId::new(writeback_id.clone(), FenceKind::Io);
    let writeback_fence = FenceId::new(writeback_id.clone(), FenceKind::Writeback);
    let publication_io_fence = FenceId::new(publication_id.clone(), FenceKind::Io);
    let publication_fence = FenceId::new(publication_id.clone(), FenceKind::Publication);
    let compute = WorkNode {
        id: compute_id.clone(),
        kind: WorkKind::Compute,
        domain: WorkDomain::Metal {
            demand_id: "metal".to_string(),
        },
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::new(),
        claims: vec![
            ResourceClaim {
                resource: crate::LeaseResource::Accelerator {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Device),
            },
            ResourceClaim {
                resource: crate::LeaseResource::AcceleratorCommandQueue {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Device),
            },
        ],
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("first-grid"),
            lifetime: ClaimLifetime::through_fence(FenceKind::Device),
        }],
        fences: BTreeSet::from([FenceKind::Device]),
        quiescence_after: BTreeSet::new(),
    };
    let io_claims = |lifetime: ClaimLifetime| {
        vec![
            ResourceClaim {
                resource: crate::LeaseResource::Rate {
                    demand_id: "output-rate".to_string(),
                },
                amount: 1,
                lifetime: lifetime.clone(),
            },
            ResourceClaim {
                resource: crate::LeaseResource::Queue {
                    demand_id: "output-queue".to_string(),
                },
                amount: 1,
                lifetime,
            },
        ]
    };
    let io = WorkNode {
        id: io_id.clone(),
        kind: WorkKind::Io,
        domain: WorkDomain::Io,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([WorkDependency::Fence(device_fence.clone())]),
        claims: io_claims(ClaimLifetime::through_fence(FenceKind::Io)),
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("first-grid"),
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        }],
        fences: BTreeSet::from([FenceKind::Io]),
        quiescence_after: BTreeSet::new(),
    };
    let writeback_lifetime = ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Writeback]);
    let writeback = WorkNode {
        id: writeback_id.clone(),
        kind: WorkKind::Writeback,
        domain: WorkDomain::Io,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([WorkDependency::Fence(io_fence.clone())]),
        claims: io_claims(writeback_lifetime.clone()),
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("first-grid"),
            lifetime: writeback_lifetime,
        }],
        fences: BTreeSet::from([FenceKind::Io, FenceKind::Writeback]),
        quiescence_after: BTreeSet::new(),
    };
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let publication = WorkNode {
        id: publication_id.clone(),
        kind: WorkKind::Publication,
        domain: WorkDomain::Io,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([
            WorkDependency::Fence(writeback_io_fence.clone()),
            WorkDependency::Fence(writeback_fence.clone()),
        ]),
        claims: io_claims(publication_lifetime.clone()),
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("first-grid"),
            lifetime: publication_lifetime,
        }],
        fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
        quiescence_after: BTreeSet::new(),
    };
    let reuse = WorkNode {
        id: reuse_id.clone(),
        kind: WorkKind::Compute,
        domain: WorkDomain::Cpu,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([
            WorkDependency::Fence(publication_io_fence.clone()),
            WorkDependency::Fence(publication_fence.clone()),
        ]),
        claims: vec![ResourceClaim {
            resource: crate::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }],
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("second-grid"),
            lifetime: ClaimLifetime::Work,
        }],
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![compute, io, writeback, publication, reuse]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "shared-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: views.into_iter().collect(),
    }];
    specification.resource_alternative.demand.accelerators = vec![AcceleratorDemand {
        demand_id: "metal".to_string(),
        accelerator: AcceleratorId::new("metal-0"),
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "output-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "output-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    let first_release = BTreeSet::from([
        WorkDependency::Fence(device_fence.clone()),
        WorkDependency::Fence(io_fence.clone()),
        WorkDependency::Fence(writeback_io_fence.clone()),
        WorkDependency::Fence(writeback_fence.clone()),
        WorkDependency::Fence(publication_io_fence.clone()),
        WorkDependency::Fence(publication_fence.clone()),
    ]);
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: AllocationId::new("first-grid"),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("reused-slot"),
            lifetime: AllocationLifetime {
                acquire_at: compute_id.clone(),
                release_after: first_release,
            },
        },
        LogicalAllocation {
            id: AllocationId::new("second-grid"),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("reused-slot"),
            lifetime: AllocationLifetime {
                acquire_at: reuse_id.clone(),
                release_after: BTreeSet::from([WorkDependency::Work(reuse_id)]),
            },
        },
    ];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("reused-slot"),
        lease_resource,
        capacity_bytes: 100,
        compatibility,
    }];
    let plan = ExecutionDag::new(specification).expect("valid reuse plan");
    let mut scheduler = ExecutionScheduler::start(
        &plan,
        &ResourcePolicy::Exclusive,
        &unified_authority(),
        None,
    )
    .expect("admitted reuse plan");

    for (node_id, fences) in [
        (compute_id, vec![device_fence]),
        (io_id, vec![io_fence]),
        (writeback_id, vec![writeback_io_fence, writeback_fence]),
        (
            publication_id,
            vec![publication_io_fence, publication_fence],
        ),
    ] {
        let SchedulerAction::Work(work) = scheduler.next_action().expect("pipeline dispatch")
        else {
            panic!("pipeline node must dispatch");
        };
        assert_eq!(work.node().id, node_id);
        let declared = scheduler
            .finish_work(node_id.clone(), WorkResult::Succeeded)
            .expect("pipeline work completes");
        assert_eq!(declared, fences.iter().cloned().collect());
        for (index, fence) in fences.into_iter().enumerate() {
            scheduler
                .complete_fence(fence)
                .expect("pipeline fence completes");
            if node_id == WorkNodeId::new("d-publication") && index == 0 {
                assert!(matches!(
                    scheduler.next_action().expect("slot remains fenced"),
                    SchedulerAction::Waiting { .. }
                ));
            }
        }
    }
    let SchedulerAction::Work(second) = scheduler.next_action().expect("slot reuse dispatch")
    else {
        panic!("slot may be reused only after every fence");
    };
    assert_eq!(second.node().id, WorkNodeId::new("e-reuse"));
    scheduler
        .finish_work(second.node().id.clone(), WorkResult::Succeeded)
        .expect("consumer completes");
    assert_eq!(
        scheduler.next_action().expect("terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Succeeded)
    );
}

#[test]
fn disjoint_io_buffer_purposes_share_one_physical_memory_charge() {
    let first_id = WorkNodeId::new("read-ahead");
    let second_id = WorkNodeId::new("writeback");
    let third_id = WorkNodeId::new("publication");
    let first_fences = BTreeSet::from([FenceKind::Io]);
    let second_fences = BTreeSet::from([FenceKind::Io, FenceKind::Writeback]);
    let third_fences = BTreeSet::from([FenceKind::Io, FenceKind::Publication]);
    let make_node = |id: WorkNodeId,
                     kind: WorkKind,
                     buffer: crate::IoBufferKind,
                     dependencies: BTreeSet<WorkDependency>,
                     fences: BTreeSet<FenceKind>| {
        let lifetime = ClaimLifetime::through_fences(fences.iter().copied());
        WorkNode {
            id: id.clone(),
            kind,
            domain: WorkDomain::Io,
            implementation: WorkImplementationId::new("cpu-reference"),
            dependencies,
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::IoBuffer(buffer),
                    amount: 600,
                    lifetime: lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Rate {
                        demand_id: "output-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Queue {
                        demand_id: "output-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: AllocationId::new(format!("{}-buffer", id.as_str())),
                lifetime,
            }],
            fences,
            quiescence_after: BTreeSet::new(),
        }
    };
    let first = make_node(
        first_id.clone(),
        WorkKind::Prefetch,
        crate::IoBufferKind::SourceReadAhead,
        BTreeSet::new(),
        first_fences.clone(),
    );
    let second = make_node(
        second_id.clone(),
        WorkKind::Writeback,
        crate::IoBufferKind::Writeback,
        BTreeSet::from([WorkDependency::Fence(FenceId::new(
            first_id.clone(),
            FenceKind::Io,
        ))]),
        second_fences.clone(),
    );
    let third = make_node(
        third_id.clone(),
        WorkKind::Publication,
        crate::IoBufferKind::Publication,
        second_fences
            .iter()
            .map(|kind| WorkDependency::Fence(FenceId::new(second_id.clone(), *kind)))
            .collect(),
        third_fences.clone(),
    );
    let mut specification = plan_spec(vec![first, second, third]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "io-slot".to_string(),
        hard_bytes: 600,
        preferred_bytes: 600,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "output-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "output-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.io_buffers = IoBufferDemand {
        source_read_ahead_bytes: 600,
        writeback_bytes: 600,
        publication_bytes: 600,
        ..IoBufferDemand::zero()
    };
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("unified-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("byte-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let allocation = |id: &WorkNodeId,
                      purpose: crate::IoBufferKind,
                      fences: &BTreeSet<FenceKind>| LogicalAllocation {
        id: AllocationId::new(format!("{}-buffer", id.as_str())),
        bytes: 600,
        purpose: AllocationPurpose::IoBuffer(purpose),
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("io-slot"),
        lifetime: AllocationLifetime {
            acquire_at: id.clone(),
            release_after: fences
                .iter()
                .map(|kind| WorkDependency::Fence(FenceId::new(id.clone(), *kind)))
                .collect(),
        },
    };
    specification.logical_allocations = vec![
        allocation(
            &first_id,
            crate::IoBufferKind::SourceReadAhead,
            &first_fences,
        ),
        allocation(&second_id, crate::IoBufferKind::Writeback, &second_fences),
        allocation(&third_id, crate::IoBufferKind::Publication, &third_fences),
    ];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("io-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "io-slot".to_string(),
        },
        capacity_bytes: 600,
        compatibility,
    }];
    let plan = ExecutionDag::new(specification).expect("valid I/O-buffer reuse plan");
    let mut scheduler = ExecutionScheduler::start(
        &plan,
        &ResourcePolicy::Exclusive,
        &unified_authority(),
        None,
    )
    .expect("three logical 600-byte buffers admit as one 600-byte physical slot");

    for (expected, fences) in [
        (first_id, first_fences),
        (second_id, second_fences),
        (third_id, third_fences),
    ] {
        let SchedulerAction::Work(work) = scheduler.next_action().expect("I/O dispatch") else {
            panic!("the next buffer stage must dispatch");
        };
        assert_eq!(work.node().id, expected);
        scheduler
            .finish_work(expected.clone(), WorkResult::Succeeded)
            .expect("I/O work returns");
        for kind in fences {
            scheduler
                .complete_fence(FenceId::new(expected.clone(), kind))
                .expect("I/O fence completes before slot reuse");
        }
    }
    assert_eq!(
        scheduler.next_action().expect("terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Succeeded)
    );
}

#[test]
fn io_buffer_claims_and_logical_allocations_match_exactly() {
    let mut node = cpu_node("prepare", BTreeSet::new());
    node.kind = WorkKind::Preparation;
    node.claims.push(ResourceClaim {
        resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::Preparation),
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    node.allocations = vec![AllocationUse {
        allocation: AllocationId::new("prepare-buffer"),
        lifetime: ClaimLifetime::Work,
    }];
    let mut specification = plan_spec(vec![node]);
    specification
        .resource_alternative
        .demand
        .io_buffers
        .preparation_bytes = 100;
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "prepare-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("byte-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("prepare-buffer"),
        bytes: 100,
        purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::Preparation),
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("prepare-slot"),
        lifetime: AllocationLifetime {
            acquire_at: WorkNodeId::new("prepare"),
            release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new("prepare"))]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("prepare-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "prepare-slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];
    let canonical =
        ExecutionDag::new(specification.clone()).expect("exact buffer accounting is valid");
    let mut changed_purpose = specification.clone();
    changed_purpose.nodes[0].claims[1].resource =
        crate::LeaseResource::IoBuffer(crate::IoBufferKind::Decode);
    changed_purpose.logical_allocations[0].purpose =
        AllocationPurpose::IoBuffer(crate::IoBufferKind::Decode);
    changed_purpose
        .resource_alternative
        .demand
        .io_buffers
        .preparation_bytes = 0;
    changed_purpose
        .resource_alternative
        .demand
        .io_buffers
        .decode_bytes = 100;
    let changed =
        ExecutionDag::new(changed_purpose).expect("changed typed buffer purpose is valid");
    assert_ne!(canonical.physical_work_id(), changed.physical_work_id());
    let mut orphan_claim = specification.clone();
    orphan_claim.nodes[0].allocations.clear();
    let mut amount_mismatch = specification.clone();
    amount_mismatch.nodes[0].claims[1].amount = 99;
    let mut kind_mismatch = specification.clone();
    kind_mismatch.nodes[0].claims[1].resource =
        crate::LeaseResource::IoBuffer(crate::IoBufferKind::Decode);
    kind_mismatch
        .resource_alternative
        .demand
        .io_buffers
        .preparation_bytes = 0;
    kind_mismatch
        .resource_alternative
        .demand
        .io_buffers
        .decode_bytes = 100;
    let mut unused_ceiling = specification;
    unused_ceiling
        .resource_alternative
        .demand
        .io_buffers
        .decode_bytes = 1;

    for invalid in [orphan_claim, amount_mismatch, kind_mismatch] {
        let error = ExecutionDag::new(invalid)
            .expect_err("buffer claim kind, amount, lifetime, and use must match");
        assert!(
            matches!(error, ExecutionError::InvalidPlan(message) if message.contains("exactly match"))
        );
    }
    let error = ExecutionDag::new(unused_ceiling)
        .expect_err("nonzero buffer demand must be used by the work graph");
    assert!(matches!(error, ExecutionError::InvalidPlan(message) if message.contains("unused")));
}

#[test]
fn every_io_buffer_kind_has_exact_supported_and_unsupported_work_semantics() {
    let all_work_kinds = [
        WorkKind::DataCensus,
        WorkKind::Preparation,
        WorkKind::Cache,
        WorkKind::ConvolutionFunction,
        WorkKind::FftPlanning,
        WorkKind::Jit,
        WorkKind::Compute,
        WorkKind::Transfer,
        WorkKind::Spill,
        WorkKind::Prefetch,
        WorkKind::Io,
        WorkKind::Serialization,
        WorkKind::Writeback,
        WorkKind::Publication,
        WorkKind::Release,
        WorkKind::Synchronization,
    ];
    let mappings = [
        (
            crate::IoBufferKind::SourceReadAhead,
            &[WorkKind::Prefetch][..],
        ),
        (crate::IoBufferKind::Decode, &[WorkKind::Preparation][..]),
        (
            crate::IoBufferKind::Preparation,
            &[WorkKind::Preparation][..],
        ),
        (
            crate::IoBufferKind::HostToDeviceTransfer,
            &[WorkKind::Transfer][..],
        ),
        (
            crate::IoBufferKind::DeviceToHostTransfer,
            &[WorkKind::Transfer][..],
        ),
        (
            crate::IoBufferKind::SpillRead,
            &[WorkKind::Spill, WorkKind::Prefetch][..],
        ),
        (crate::IoBufferKind::SpillWrite, &[WorkKind::Spill][..]),
        (
            crate::IoBufferKind::Serialization,
            &[WorkKind::Serialization][..],
        ),
        (
            crate::IoBufferKind::StorageManager,
            &[WorkKind::Io, WorkKind::Release][..],
        ),
        (crate::IoBufferKind::TiledColumnWriter, &[WorkKind::Io][..]),
        (crate::IoBufferKind::ScalarColumnWriter, &[WorkKind::Io][..]),
        (crate::IoBufferKind::Writeback, &[WorkKind::Writeback][..]),
        (
            crate::IoBufferKind::Publication,
            &[WorkKind::Publication][..],
        ),
        (
            crate::IoBufferKind::MappedPageCache,
            &[WorkKind::Cache, WorkKind::Release][..],
        ),
    ];
    assert_eq!(
        mappings.map(|(kind, _)| kind),
        crate::IoBufferKind::ALL,
        "the semantic mapping must enumerate every typed I/O buffer exactly once"
    );

    for (io_kind, supported) in mappings {
        for work_kind in all_work_kinds {
            assert_eq!(
                io_buffer_kind_supports_work_kind(io_kind, work_kind),
                supported.contains(&work_kind),
                "{io_kind:?} support for {work_kind:?}"
            );
        }
    }
}

#[test]
fn allocation_lifetime_rejects_release_before_an_async_use_fence() {
    let domain = CapacityDomainId::new("unified-memory");
    let views = BTreeSet::from([
        CapacityViewId::new("host-memory"),
        CapacityViewId::new("metal-memory"),
    ]);
    let compatibility = SlotCompatibility {
        memory_domain: domain,
        views: views.clone(),
        alignment_bytes: 64,
        storage_mode: StorageMode::MetalShared,
        layout: AllocationLayout::new("f32-grid-row-major"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let node_id = WorkNodeId::new("metal-use");
    let node = WorkNode {
        id: node_id.clone(),
        kind: WorkKind::Compute,
        domain: WorkDomain::Metal {
            demand_id: "metal".to_string(),
        },
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::new(),
        claims: vec![
            ResourceClaim {
                resource: crate::LeaseResource::Accelerator {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Device),
            },
            ResourceClaim {
                resource: crate::LeaseResource::AcceleratorCommandQueue {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Device),
            },
        ],
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("grid"),
            lifetime: ClaimLifetime::through_fence(FenceKind::Device),
        }],
        fences: BTreeSet::from([FenceKind::Device]),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![node]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: views.into_iter().collect(),
    }];
    specification.resource_alternative.demand.accelerators = vec![AcceleratorDemand {
        demand_id: "metal".to_string(),
        accelerator: AcceleratorId::new("metal-0"),
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    }];
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("grid"),
        bytes: 100,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("slot"),
        lifetime: AllocationLifetime {
            acquire_at: node_id.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(node_id.clone())]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];

    let mut synchronous_use = specification.clone();
    synchronous_use.nodes[0].allocations[0].lifetime = ClaimLifetime::Work;
    synchronous_use.logical_allocations[0]
        .lifetime
        .release_after = BTreeSet::from([WorkDependency::Fence(FenceId::new(
        node_id.clone(),
        FenceKind::Device,
    ))]);
    let error = ExecutionDag::new(synchronous_use)
        .expect_err("Metal allocation uses must remain live through device completion");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("exact asynchronous lifetime"))
    );

    let error = ExecutionDag::new(specification)
        .expect_err("slot release cannot precede the declared device use fence");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("released before"))
    );
}

#[test]
fn asynchronous_payload_claims_cannot_end_with_synchronous_work() {
    let node = WorkNode {
        id: WorkNodeId::new("metal-work"),
        kind: WorkKind::Compute,
        domain: WorkDomain::Metal {
            demand_id: "metal".to_string(),
        },
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::new(),
        claims: vec![
            ResourceClaim {
                resource: crate::LeaseResource::Accelerator {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::Work,
            },
            ResourceClaim {
                resource: crate::LeaseResource::AcceleratorCommandQueue {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::Work,
            },
        ],
        allocations: Vec::new(),
        fences: BTreeSet::from([FenceKind::Device]),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![node]);
    specification.resource_alternative.demand.accelerators = vec![AcceleratorDemand {
        demand_id: "metal".to_string(),
        accelerator: AcceleratorId::new("metal-0"),
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("Metal payload permits must remain live through device completion");

    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("asynchronous lifetime"))
    );

    for (kind, fences) in [
        (WorkKind::Io, BTreeSet::from([FenceKind::Io])),
        (WorkKind::ObservationRead, BTreeSet::from([FenceKind::Io])),
        (
            WorkKind::Writeback,
            BTreeSet::from([FenceKind::Io, FenceKind::Writeback]),
        ),
        (
            WorkKind::Publication,
            BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
        ),
    ] {
        let node = WorkNode {
            id: WorkNodeId::new("io-work"),
            kind,
            domain: WorkDomain::Io,
            implementation: WorkImplementationId::new("cpu-reference"),
            dependencies: BTreeSet::new(),
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::Rate {
                        demand_id: "io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Queue {
                        demand_id: "io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: Vec::new(),
            fences,
            quiescence_after: BTreeSet::new(),
        };
        let mut specification = plan_spec(vec![node]);
        specification.resource_alternative.demand.rates = vec![RateDemand {
            demand_id: "io-rate".to_string(),
            resource: RateResourceId::new("io-rate"),
            amount: CountDemand::new(1, 1),
        }];
        specification.resource_alternative.demand.queues = vec![QueueDemand {
            demand_id: "io-queue".to_string(),
            resource: QueueResourceId::new("io-queue"),
            slots: CountDemand::new(1, 1),
        }];

        let error = ExecutionDag::new(specification)
            .expect_err("I/O payload permits must remain live through exact completion fences");
        assert!(
            matches!(error, ExecutionError::InvalidPlan(message) if message.contains("asynchronous lifetime"))
        );
    }
}

#[test]
fn mutable_allocation_use_waits_for_every_prior_async_use_fence() {
    let first_id = WorkNodeId::new("first-use");
    let second_id = WorkNodeId::new("second-use");
    let mut first = cpu_node(first_id.as_str(), BTreeSet::new());
    first.kind = WorkKind::Io;
    first.domain = WorkDomain::Io;
    first.claims = vec![
        ResourceClaim {
            resource: crate::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::Queue {
                demand_id: "io-queue".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
    ];
    first.allocations = vec![AllocationUse {
        allocation: AllocationId::new("grid"),
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    }];
    first.fences = BTreeSet::from([FenceKind::Io]);
    let mut second = cpu_node(
        second_id.as_str(),
        BTreeSet::from([WorkDependency::Work(first_id.clone())]),
    );
    second.allocations = vec![AllocationUse {
        allocation: AllocationId::new("grid"),
        lifetime: ClaimLifetime::Work,
    }];

    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("mutable-grid"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut specification = plan_spec(vec![first, second]);
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "io-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "io-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("grid"),
        bytes: 100,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("slot"),
        lifetime: AllocationLifetime {
            acquire_at: first_id,
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(WorkNodeId::new("first-use"), FenceKind::Io)),
                WorkDependency::Work(second_id),
            ]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];

    let mut read_only_initialization = specification.clone();
    read_only_initialization.logical_allocations[0]
        .compatibility
        .access = AllocationAccess::ReadOnly;
    read_only_initialization.physical_slots[0]
        .compatibility
        .access = AllocationAccess::ReadOnly;
    let mut concurrent_preserved_reads = read_only_initialization.clone();
    concurrent_preserved_reads.logical_allocations[0]
        .compatibility
        .initialization = InitializationPolicy::Preserve;
    concurrent_preserved_reads.physical_slots[0]
        .compatibility
        .initialization = InitializationPolicy::Preserve;
    let error = ExecutionDag::new(specification)
        .expect_err("mutable reuse cannot race a predecessor's async access");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("unordered mutable uses"))
    );
    let error = ExecutionDag::new(read_only_initialization)
        .expect_err("read-only use must still wait for async initialization");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("asynchronous initialization"))
    );
    ExecutionDag::new(concurrent_preserved_reads)
        .expect("preserved read-only contents may be consumed concurrently through exact fences");
}

#[test]
fn cancellation_prevents_pending_publication_from_starting() {
    let compute_id = WorkNodeId::new("compute");
    let publication_id = WorkNodeId::new("publish");
    let mut first = cpu_node(compute_id.as_str(), BTreeSet::new());
    first.allocations = vec![AllocationUse {
        allocation: AllocationId::new("cancel-grid"),
        lifetime: ClaimLifetime::Work,
    }];
    let publication_fences = BTreeSet::from([FenceKind::Io, FenceKind::Publication]);
    let publication_lifetime = ClaimLifetime::through_fences(publication_fences.iter().copied());
    let publication = WorkNode {
        id: publication_id.clone(),
        kind: WorkKind::Publication,
        domain: WorkDomain::Io,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([WorkDependency::Work(first.id.clone())]),
        claims: vec![
            ResourceClaim {
                resource: crate::LeaseResource::Rate {
                    demand_id: "output-rate".to_string(),
                },
                amount: 1,
                lifetime: publication_lifetime.clone(),
            },
            ResourceClaim {
                resource: crate::LeaseResource::Queue {
                    demand_id: "output-queue".to_string(),
                },
                amount: 1,
                lifetime: publication_lifetime.clone(),
            },
        ],
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("cancel-grid"),
            lifetime: publication_lifetime,
        }],
        fences: publication_fences,
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![first, publication]);
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "output-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "output-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("cancel-grid"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "cancel-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("cancel-grid"),
        bytes: 100,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("cancel-slot"),
        lifetime: AllocationLifetime {
            acquire_at: compute_id,
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(publication_id.clone(), FenceKind::Io)),
                WorkDependency::Fence(FenceId::new(publication_id, FenceKind::Publication)),
            ]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("cancel-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "cancel-slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];
    let plan = ExecutionDag::new(specification).expect("valid publication plan");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &io_authority(), None)
            .expect("admitted publication plan");

    let SchedulerAction::Work(compute) = scheduler.next_action().expect("compute dispatch") else {
        panic!("compute must dispatch first");
    };
    scheduler.cancel().expect("cancellation starts draining");
    scheduler
        .finish_work(compute.node().id.clone(), WorkResult::Succeeded)
        .expect("launched compute settles");
    assert_eq!(
        scheduler.next_action().expect("cancelled terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Cancelled)
    );
    assert_eq!(scheduler.lease_epoch(), None);
}

#[test]
fn failed_work_cancels_pending_nodes_and_releases_the_lease() {
    let failed = cpu_node("a-failed", BTreeSet::new());
    let pending = cpu_node(
        "b-pending",
        BTreeSet::from([WorkDependency::Work(failed.id.clone())]),
    );
    let plan = ExecutionDag::new(plan_spec(vec![failed, pending])).expect("valid failure plan");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &cpu_authority(), None)
            .expect("admitted failure plan");
    let SchedulerAction::Work(work) = scheduler.next_action().expect("failed work dispatch") else {
        panic!("first work must dispatch");
    };

    scheduler
        .finish_work(
            work.node().id.clone(),
            WorkResult::Failed {
                message: "kernel failure".to_string(),
            },
        )
        .expect("failure enters draining state");
    assert_eq!(
        scheduler.next_action().expect("failed terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Failed {
            node: WorkNodeId::new("a-failed"),
            message: "kernel failure".to_string(),
        })
    );
    assert_eq!(scheduler.lease_epoch(), None);
}

#[test]
fn adaptation_requires_the_listed_transition_at_its_quiescence_point() {
    let first = cpu_node("major-cycle-work", BTreeSet::new());
    let mut boundary = synchronization_node(
        "major-cycle-boundary",
        BTreeSet::from([WorkDependency::Work(first.id.clone())]),
    );
    boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let second = cpu_node(
        "minor-cycle",
        BTreeSet::from([WorkDependency::Work(boundary.id.clone())]),
    );
    let mut specification = plan_spec(vec![first, boundary, second]);
    specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 2;
    specification.adaptations = vec![AdaptationTransition {
        id: AdaptationId::new("larger-batch"),
        from: ExecutionKnobs::serial(),
        to: adapted.clone(),
        at: QuiescencePoint::MajorCycle,
    }];
    let plan = ExecutionDag::new(specification).expect("valid adaptive plan");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &cpu_authority(), None)
            .expect("admitted adaptive plan");

    assert!(scheduler.adapt(&AdaptationId::new("larger-batch")).is_err());
    let SchedulerAction::Work(first) = scheduler.next_action().expect("major-cycle dispatch")
    else {
        panic!("major-cycle node must dispatch first");
    };
    scheduler
        .finish_work(first.node().id.clone(), WorkResult::Succeeded)
        .expect("major-cycle work settles");
    let SchedulerAction::Work(boundary) = scheduler.next_action().expect("boundary dispatch")
    else {
        panic!("major-cycle synchronization must dispatch second");
    };
    scheduler
        .finish_work(boundary.node().id.clone(), WorkResult::Succeeded)
        .expect("major-cycle boundary settles");
    assert!(scheduler.adapt(&AdaptationId::new("unlisted")).is_err());
    scheduler
        .adapt(&AdaptationId::new("larger-batch"))
        .expect("listed transition at exact boundary");
    let SchedulerAction::Work(second) = scheduler.next_action().expect("minor-cycle dispatch")
    else {
        panic!("minor-cycle node must dispatch");
    };
    assert_eq!(second.knobs(), &adapted);
    assert_eq!(
        scheduler.applied_adaptations(),
        &[AdaptationId::new("larger-batch")]
    );
}

#[test]
fn quiescence_marker_must_form_a_global_synchronization_cut() {
    let prior = cpu_node("prior", BTreeSet::new());
    let unrelated = cpu_node("unrelated", BTreeSet::new());
    let mut boundary = synchronization_node(
        "boundary",
        BTreeSet::from([WorkDependency::Work(prior.id.clone())]),
    );
    boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let mut specification = plan_spec(vec![prior, unrelated, boundary]);
    specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);

    let error = ExecutionDag::new(specification)
        .expect_err("a quiescence marker cannot leave unrelated work in flight");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("global execution cut"))
    );
}

#[test]
fn adaptation_transition_must_be_reachable_in_boundary_order() {
    let prior = cpu_node("prior", BTreeSet::new());
    let mut boundary = synchronization_node(
        "major-boundary",
        BTreeSet::from([WorkDependency::Work(prior.id.clone())]),
    );
    boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let mut specification = plan_spec(vec![prior, boundary]);
    specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);
    let mut after_major = ExecutionKnobs::serial();
    after_major.batch_size = 2;
    let mut impossible = after_major.clone();
    impossible.batch_size = 3;
    specification.adaptations = vec![
        AdaptationTransition {
            id: AdaptationId::new("at-major"),
            from: ExecutionKnobs::serial(),
            to: after_major.clone(),
            at: QuiescencePoint::MajorCycle,
        },
        AdaptationTransition {
            id: AdaptationId::new("back-at-run-start"),
            from: after_major,
            to: impossible,
            at: QuiescencePoint::RunBoundary,
        },
    ];

    let error = ExecutionDag::new(specification)
        .expect_err("a later configuration cannot return to an earlier boundary");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("unreachable at every declared boundary"))
    );
}

#[test]
fn adaptation_cannot_enable_undeclared_spill_work() {
    let mut adapted = ExecutionKnobs::serial();
    adapted.spill = true;
    let mut specification = plan_spec(vec![cpu_node("compute", BTreeSet::new())]);
    specification.adaptations = vec![AdaptationTransition {
        id: AdaptationId::new("invent-spill"),
        from: ExecutionKnobs::serial(),
        to: adapted,
        at: QuiescencePoint::RunBoundary,
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("adaptation cannot create spill work absent from the immutable DAG");

    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("spill work node"))
    );
}

#[test]
fn adaptation_shape_must_fit_the_selected_scaling_envelope() {
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 9;
    let mut specification = plan_spec(vec![cpu_node("compute", BTreeSet::new())]);
    specification.adaptations = vec![AdaptationTransition {
        id: AdaptationId::new("oversized-batch"),
        from: ExecutionKnobs::serial(),
        to: adapted,
        at: QuiescencePoint::RunBoundary,
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("adaptation cannot exceed the plan-sealed batch envelope");

    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("hard bounds"))
    );
}

#[test]
fn execution_knobs_must_admit_every_reachable_mandatory_claim() {
    let mut two_worker_node = cpu_node("two-worker-kernel", BTreeSet::new());
    two_worker_node.claims[0].amount = 2;
    let mut worker_specification = plan_spec(vec![two_worker_node]);
    worker_specification.resource_alternative.demand.workers = CountDemand::new(2, 2);
    worker_specification
        .resource_alternative
        .scaling
        .maximum_workers = 2;

    let error = ExecutionDag::new(worker_specification)
        .expect_err("initial knobs cannot leave a mandatory worker claim undispatchable");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("mandatory claims"))
    );

    let mut cache_node = cpu_node("cache-kernel", BTreeSet::new());
    cache_node.kind = WorkKind::Cache;
    cache_node.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 8,
        lifetime: ClaimLifetime::Work,
    });
    let mut cache_specification = plan_spec(vec![cache_node]);
    cache_specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 8,
        preferred_resident_bytes: 8,
    };

    let error = ExecutionDag::new(cache_specification)
        .expect_err("initial knobs cannot leave a mandatory cache claim undispatchable");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("mandatory claims"))
    );

    let first = cpu_node("first", BTreeSet::new());
    let mut boundary = synchronization_node(
        "major-boundary",
        BTreeSet::from([WorkDependency::Work(first.id.clone())]),
    );
    boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let mut after = cpu_node(
        "after-boundary",
        BTreeSet::from([WorkDependency::Work(boundary.id.clone())]),
    );
    after.claims[0].amount = 2;
    let mut transition_specification = plan_spec(vec![first, boundary, after]);
    transition_specification.resource_alternative.demand.workers = CountDemand::new(2, 2);
    transition_specification
        .resource_alternative
        .scaling
        .maximum_workers = 2;
    transition_specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);
    let mut initial = ExecutionKnobs::serial();
    initial.workers = 2;
    transition_specification.initial_knobs = initial.clone();
    transition_specification.adaptations = vec![AdaptationTransition {
        id: AdaptationId::new("starve-later-work"),
        from: initial,
        to: ExecutionKnobs::serial(),
        at: QuiescencePoint::MajorCycle,
    }];

    let error = ExecutionDag::new(transition_specification)
        .expect_err("an adaptation cannot starve work reachable after its boundary");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("mandatory claims"))
    );
}

#[test]
fn adaptation_feasibility_uses_the_exact_repeated_boundary_occurrence() {
    let first = cpu_node("before-first-major", BTreeSet::new());
    let mut first_boundary = synchronization_node(
        "first-major-boundary",
        BTreeSet::from([WorkDependency::Work(first.id.clone())]),
    );
    first_boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let mut expensive = cpu_node(
        "expensive-between-majors",
        BTreeSet::from([WorkDependency::Work(first_boundary.id.clone())]),
    );
    expensive.claims[0].amount = 2;
    let mut second_boundary = synchronization_node(
        "second-major-boundary",
        BTreeSet::from([WorkDependency::Work(expensive.id.clone())]),
    );
    second_boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let cheap = cpu_node(
        "cheap-after-second-major",
        BTreeSet::from([WorkDependency::Work(second_boundary.id.clone())]),
    );
    let mut specification = plan_spec(vec![
        first,
        first_boundary,
        expensive,
        second_boundary,
        cheap,
    ]);
    specification.resource_alternative.demand.workers = CountDemand::new(2, 2);
    specification.resource_alternative.scaling.maximum_workers = 2;
    specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);
    let mut initial = ExecutionKnobs::serial();
    initial.workers = 2;
    specification.initial_knobs = initial.clone();
    let mut after_first = initial.clone();
    after_first.batch_size = 2;
    let mut after_second = after_first.clone();
    after_second.workers = 1;
    specification.adaptations = vec![
        AdaptationTransition {
            id: AdaptationId::new("select-late-path"),
            from: initial,
            to: after_first.clone(),
            at: QuiescencePoint::MajorCycle,
        },
        AdaptationTransition {
            id: AdaptationId::new("shrink-after-second-major"),
            from: after_first,
            to: after_second,
            at: QuiescencePoint::MajorCycle,
        },
    ];

    ExecutionDag::new(specification).expect(
        "the one-worker target is reachable only at the second major-cycle occurrence, after the two-worker node",
    );
}

#[test]
fn release_node_must_own_exactly_one_logical_allocation() {
    let first_prepare_id = WorkNodeId::new("prepare-first");
    let second_prepare_id = WorkNodeId::new("prepare-second");
    let release_id = WorkNodeId::new("release-both");
    let first_allocation = AllocationId::new("first-data");
    let second_allocation = AllocationId::new("second-data");
    let allocation_use = |allocation| AllocationUse {
        allocation,
        lifetime: ClaimLifetime::Work,
    };
    let mut first_prepare = cpu_node(first_prepare_id.as_str(), BTreeSet::new());
    first_prepare
        .allocations
        .push(allocation_use(first_allocation.clone()));
    let mut second_prepare = cpu_node(second_prepare_id.as_str(), BTreeSet::new());
    second_prepare
        .allocations
        .push(allocation_use(second_allocation.clone()));
    let mut release = cpu_node(
        release_id.as_str(),
        BTreeSet::from([
            WorkDependency::Work(first_prepare_id.clone()),
            WorkDependency::Work(second_prepare_id.clone()),
        ]),
    );
    release.kind = WorkKind::Release;
    release
        .allocations
        .extend([first_allocation.clone(), second_allocation.clone()].map(allocation_use));
    let mut specification = plan_spec(vec![first_prepare, second_prepare, release]);
    specification.resource_alternative.demand.memory = ["first-slot", "second-slot"]
        .map(|allocation_id| MemoryDemand {
            allocation_id: allocation_id.to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![CapacityViewId::new("host-memory")],
        })
        .to_vec();
    let compatibility = |layout| SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(layout),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let first_compatibility = compatibility("first-data");
    let second_compatibility = compatibility("second-data");
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: first_allocation,
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: first_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("first-slot"),
            lifetime: AllocationLifetime {
                acquire_at: first_prepare_id,
                release_after: BTreeSet::from([WorkDependency::Work(release_id.clone())]),
            },
        },
        LogicalAllocation {
            id: second_allocation,
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: second_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("second-slot"),
            lifetime: AllocationLifetime {
                acquire_at: second_prepare_id,
                release_after: BTreeSet::from([WorkDependency::Work(release_id)]),
            },
        },
    ];
    specification.physical_slots = vec![
        PhysicalSlot {
            id: PhysicalSlotId::new("first-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "first-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: first_compatibility,
        },
        PhysicalSlot {
            id: PhysicalSlotId::new("second-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "second-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: second_compatibility,
        },
    ];

    let error = ExecutionDag::new(specification)
        .expect_err("a Release node cannot make partial failure ownership ambiguous");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("exactly one logical allocation"))
    );
}

#[test]
fn externally_retained_io_buffer_release_is_terminal_after_every_use() {
    let prepare_id = WorkNodeId::new("prepare-mapping");
    let release_id = WorkNodeId::new("release-mapping");
    let later_id = WorkNodeId::new("later-use");
    let buffer_claim = || ResourceClaim {
        resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::MappedPageCache),
        amount: 100,
        lifetime: ClaimLifetime::Work,
    };
    let buffer_use = || AllocationUse {
        allocation: AllocationId::new("mapped-pages"),
        lifetime: ClaimLifetime::Work,
    };
    let mut prepare = cpu_node(prepare_id.as_str(), BTreeSet::new());
    prepare.kind = WorkKind::Cache;
    prepare.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    prepare.claims.push(buffer_claim());
    prepare.allocations.push(buffer_use());
    let mut release = cpu_node(
        release_id.as_str(),
        BTreeSet::from([WorkDependency::Work(prepare_id.clone())]),
    );
    release.kind = WorkKind::Release;
    release.claims.push(buffer_claim());
    release.allocations.push(buffer_use());
    let mut later = cpu_node(
        later_id.as_str(),
        BTreeSet::from([WorkDependency::Work(release_id.clone())]),
    );
    later.kind = WorkKind::Cache;
    later.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    later.claims.push(buffer_claim());
    later.allocations.push(buffer_use());
    let mut specification = plan_spec(vec![prepare, release, later]);
    specification
        .resource_alternative
        .demand
        .io_buffers
        .mapped_page_cache_bytes = 100;
    specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 100,
        preferred_resident_bytes: 100,
    };
    specification.initial_knobs.cache_retention_bytes = 100;
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "mapped-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("mapped-pages"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("mapped-pages"),
        bytes: 100,
        purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::MappedPageCache),
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("mapped-slot"),
        lifetime: AllocationLifetime {
            acquire_at: prepare_id.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(later_id.clone())]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("mapped-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "mapped-slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];

    let mut valid = specification.clone();
    valid
        .nodes
        .iter_mut()
        .find(|node| node.id == later_id)
        .expect("later use")
        .dependencies = BTreeSet::from([WorkDependency::Work(prepare_id.clone())]);
    valid
        .nodes
        .iter_mut()
        .find(|node| node.id == release_id)
        .expect("release work")
        .dependencies = BTreeSet::from([WorkDependency::Work(later_id)]);
    valid.logical_allocations[0].lifetime.release_after =
        BTreeSet::from([WorkDependency::Work(release_id.clone())]);
    let plan = ExecutionDag::new(valid).expect("valid terminal mapping release");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &cpu_authority(), None)
            .expect("admitted mapping plan");
    let SchedulerAction::Work(prepare) = scheduler.next_action().expect("mapping preparation")
    else {
        panic!("mapping preparation must dispatch first");
    };
    scheduler
        .finish_work(prepare.node().id.clone(), WorkResult::Succeeded)
        .expect("mapping preparation settles");
    scheduler.cancel().expect("cancellation enters cleanup");
    let SchedulerAction::Work(release) = scheduler.next_action().expect("release cleanup") else {
        panic!("active mapped storage must dispatch its release during cancellation");
    };
    assert_eq!(release.node().id, release_id);
    scheduler
        .finish_work(release.node().id.clone(), WorkResult::Succeeded)
        .expect("release cleanup settles");
    assert_eq!(
        scheduler.next_action().expect("cancelled terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Cancelled)
    );

    let error = ExecutionDag::new(specification)
        .expect_err("unmap work cannot precede a later use of externally retained storage");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("terminal release"))
    );
}

#[test]
fn cancellation_cleanup_respects_release_to_release_dependencies() {
    let mapped_prepare_id = WorkNodeId::new("prepare-mapped");
    let storage_prepare_id = WorkNodeId::new("prepare-storage");
    let first_release_id = WorkNodeId::new("z-release-first");
    let second_release_id = WorkNodeId::new("a-release-second");
    let mapped_id = AllocationId::new("mapped-pages");
    let storage_id = AllocationId::new("storage-manager");
    let buffer_claim = |kind| ResourceClaim {
        resource: crate::LeaseResource::IoBuffer(kind),
        amount: 100,
        lifetime: ClaimLifetime::Work,
    };
    let buffer_use = |allocation| AllocationUse {
        allocation,
        lifetime: ClaimLifetime::Work,
    };
    let mut mapped_prepare = cpu_node(mapped_prepare_id.as_str(), BTreeSet::new());
    mapped_prepare.kind = WorkKind::Cache;
    mapped_prepare.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    mapped_prepare
        .claims
        .push(buffer_claim(crate::IoBufferKind::MappedPageCache));
    mapped_prepare
        .allocations
        .push(buffer_use(mapped_id.clone()));
    let mut storage_prepare = cpu_node(storage_prepare_id.as_str(), BTreeSet::new());
    storage_prepare.kind = WorkKind::Io;
    storage_prepare.domain = WorkDomain::Io;
    storage_prepare.claims = vec![
        ResourceClaim {
            resource: crate::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::Queue {
                demand_id: "io-queue".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::StorageManager),
            amount: 100,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
    ];
    storage_prepare.allocations = vec![AllocationUse {
        allocation: storage_id.clone(),
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    }];
    storage_prepare.fences = BTreeSet::from([FenceKind::Io]);
    let mut first_release = cpu_node(
        first_release_id.as_str(),
        BTreeSet::from([WorkDependency::Work(mapped_prepare_id.clone())]),
    );
    first_release.kind = WorkKind::Release;
    first_release.domain = WorkDomain::Io;
    first_release.claims = vec![
        ResourceClaim {
            resource: crate::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::Queue {
                demand_id: "io-queue".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::MappedPageCache),
            amount: 100,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
    ];
    first_release.allocations.push(AllocationUse {
        allocation: mapped_id.clone(),
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    });
    first_release.fences = BTreeSet::from([FenceKind::Io]);
    let mut second_release = cpu_node(
        second_release_id.as_str(),
        BTreeSet::from([
            WorkDependency::Fence(FenceId::new(storage_prepare_id.clone(), FenceKind::Io)),
            WorkDependency::Fence(FenceId::new(first_release_id.clone(), FenceKind::Io)),
        ]),
    );
    second_release.kind = WorkKind::Release;
    second_release
        .claims
        .push(buffer_claim(crate::IoBufferKind::StorageManager));
    second_release
        .allocations
        .push(buffer_use(storage_id.clone()));
    let mut specification = plan_spec(vec![
        mapped_prepare,
        storage_prepare,
        first_release,
        second_release,
    ]);
    specification.resource_alternative.demand.memory = vec![
        MemoryDemand {
            allocation_id: "mapped-slot".to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![CapacityViewId::new("host-memory")],
        },
        MemoryDemand {
            allocation_id: "storage-slot".to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![CapacityViewId::new("host-memory")],
        },
    ];
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "io-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "io-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    specification
        .resource_alternative
        .demand
        .io_buffers
        .mapped_page_cache_bytes = 100;
    specification
        .resource_alternative
        .demand
        .io_buffers
        .storage_manager_bytes = 100;
    specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 100,
        preferred_resident_bytes: 100,
    };
    specification.initial_knobs.cache_retention_bytes = 100;
    let compatibility = |layout| SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(layout),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let mapped_compatibility = compatibility("mapped-pages");
    let storage_compatibility = compatibility("storage-manager");
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: mapped_id.clone(),
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::MappedPageCache),
            compatibility: mapped_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("mapped-slot"),
            lifetime: AllocationLifetime {
                acquire_at: mapped_prepare_id,
                release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    first_release_id.clone(),
                    FenceKind::Io,
                ))]),
            },
        },
        LogicalAllocation {
            id: storage_id.clone(),
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::StorageManager),
            compatibility: storage_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("storage-slot"),
            lifetime: AllocationLifetime {
                acquire_at: storage_prepare_id.clone(),
                release_after: BTreeSet::from([WorkDependency::Work(second_release_id.clone())]),
            },
        },
    ];
    specification.physical_slots = vec![
        PhysicalSlot {
            id: PhysicalSlotId::new("mapped-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "mapped-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: mapped_compatibility,
        },
        PhysicalSlot {
            id: PhysicalSlotId::new("storage-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "storage-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: storage_compatibility,
        },
    ];
    let plan = ExecutionDag::new(specification).expect("valid ordered cleanup plan");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &io_authority(), None)
            .expect("admitted cleanup plan");

    for expected in [&mapped_id, &storage_id] {
        let SchedulerAction::Work(work) = scheduler.next_action().expect("preparation dispatch")
        else {
            panic!("both external allocations must be prepared before cancellation");
        };
        assert_eq!(&work.node().allocations[0].allocation, expected);
        scheduler
            .finish_work(work.node().id.clone(), WorkResult::Succeeded)
            .expect("preparation settles");
        if work.node().id == storage_prepare_id {
            scheduler
                .complete_fence(FenceId::new(storage_prepare_id.clone(), FenceKind::Io))
                .expect("storage-manager preparation fence settles");
        }
    }
    scheduler.cancel().expect("cancellation enters cleanup");

    let SchedulerAction::Work(first_release) =
        scheduler.next_action().expect("first cleanup dispatch")
    else {
        panic!("the predecessor release must dispatch first");
    };
    assert_eq!(first_release.node().id, first_release_id);
    scheduler
        .finish_work(first_release.node().id.clone(), WorkResult::Succeeded)
        .expect("first release launches its cleanup fence");
    assert!(matches!(
        scheduler
            .next_action()
            .expect("dependent cleanup waits for predecessor fence"),
        SchedulerAction::Waiting {
            pending_fences: 1,
            ..
        }
    ));
    scheduler
        .complete_fence(FenceId::new(first_release_id, FenceKind::Io))
        .expect("first release fence settles");
    let SchedulerAction::Work(second_release) =
        scheduler.next_action().expect("dependent cleanup dispatch")
    else {
        panic!("the dependent release must dispatch second");
    };
    assert_eq!(second_release.node().id, second_release_id);
}

#[test]
fn cancellation_cleanup_projects_out_inactive_release_work_and_fences() {
    for fenced_predecessor in [false, true] {
        let (plan, active_release_id) = inactive_release_predecessor_plan(fenced_predecessor);
        let mut scheduler =
            ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &io_authority(), None)
                .expect("admitted cleanup projection plan");
        let SchedulerAction::Work(prepare) = scheduler.next_action().expect("active preparation")
        else {
            panic!("the active external allocation must be acquired first");
        };
        assert_eq!(prepare.node().id, WorkNodeId::new("0-prepare-active"));
        scheduler
            .finish_work(prepare.node().id.clone(), WorkResult::Succeeded)
            .expect("active preparation settles");
        scheduler.cancel().expect("cancellation enters cleanup");

        let SchedulerAction::Work(release) = scheduler
            .next_action()
            .expect("inactive predecessor is projected out")
        else {
            panic!("the still-possible active release must dispatch");
        };
        assert_eq!(release.node().id, active_release_id);
        scheduler
            .finish_work(release.node().id.clone(), WorkResult::Succeeded)
            .expect("active release settles");
        assert_eq!(
            scheduler.next_action().expect("cancelled terminal action"),
            SchedulerAction::Complete(SchedulerTerminal::Cancelled)
        );
    }
}

#[test]
fn temporal_reuse_rejects_preserved_contents_from_another_allocation() {
    let domain = CapacityDomainId::new("unified-memory");
    let views = BTreeSet::from([CapacityViewId::new("host-memory")]);
    let compatibility = SlotCompatibility {
        memory_domain: domain,
        views: views.clone(),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("preserved-grid"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
    };
    let lease_resource = crate::LeaseResource::Memory {
        allocation_id: "shared-slot".to_string(),
    };
    let make_node = |id: &str, allocation: &str| WorkNode {
        id: WorkNodeId::new(id),
        kind: WorkKind::Compute,
        domain: WorkDomain::Cpu,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::new(),
        claims: vec![ResourceClaim {
            resource: crate::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }],
        allocations: vec![AllocationUse {
            allocation: AllocationId::new(allocation),
            lifetime: ClaimLifetime::Work,
        }],
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![
        make_node("first", "first-grid"),
        make_node("second", "second-grid"),
    ]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "shared-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: views.into_iter().collect(),
    }];
    specification.logical_allocations = [("first-grid", "first"), ("second-grid", "second")]
        .into_iter()
        .map(|(allocation, node)| LogicalAllocation {
            id: AllocationId::new(allocation),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("reused-slot"),
            lifetime: AllocationLifetime {
                acquire_at: WorkNodeId::new(node),
                release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new(node))]),
            },
        })
        .collect();
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("reused-slot"),
        lease_resource,
        capacity_bytes: 100,
        compatibility,
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("preserved bytes cannot be rebound to another logical allocation");
    assert!(matches!(error, ExecutionError::InvalidPlan(message) if message.contains("Preserve")));
}

#[test]
fn temporal_reuse_requires_release_strictly_before_the_next_acquisition() {
    let views = BTreeSet::from([CapacityViewId::new("host-memory")]);
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: views.clone(),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("temporary-grid"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let first = cpu_node("first", BTreeSet::new());
    let second = cpu_node(
        "second",
        BTreeSet::from([WorkDependency::Work(first.id.clone())]),
    );
    let mut first = first;
    first.allocations = vec![AllocationUse {
        allocation: AllocationId::new("first-grid"),
        lifetime: ClaimLifetime::Work,
    }];
    let mut second = second;
    second.allocations = vec![AllocationUse {
        allocation: AllocationId::new("second-grid"),
        lifetime: ClaimLifetime::Work,
    }];
    let mut specification = plan_spec(vec![first, second]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "shared-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: views.into_iter().collect(),
    }];
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: AllocationId::new("first-grid"),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("shared-slot"),
            lifetime: AllocationLifetime {
                acquire_at: WorkNodeId::new("first"),
                release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new("second"))]),
            },
        },
        LogicalAllocation {
            id: AllocationId::new("second-grid"),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("shared-slot"),
            lifetime: AllocationLifetime {
                acquire_at: WorkNodeId::new("second"),
                release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new("second"))]),
            },
        },
    ];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("shared-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "shared-slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("a slot cannot be released by the work that is waiting to acquire it");

    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("strictly ordered"))
    );
}

#[test]
fn receipt_store_checkpoints_atomically_rejects_corruption_and_enforces_retention() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let provenance = |attempt, build| {
        crate::ExecutionProvenance::new(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([build; 32]),
        )
    };

    let directory = tempfile::tempdir().expect("receipt directory");
    let store = crate::ExecutionReceiptStore::new(
        directory.path(),
        crate::ReceiptRetention::new(2, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    let first = provenance(61, 62);
    let mut recorder = store.begin(first, &problem, &plan).expect("begin receipt");
    assert_eq!(
        store
            .open(first.attempt_id())
            .expect("initial receipt")
            .status(),
        crate::ReceiptStatus::Running
    );
    assert!(matches!(
        store.begin(first, &problem, &plan),
        Err(crate::ReceiptError::AttemptAlreadyExists)
    ));

    let work = WorkNodeId::new("work");
    recorder
        .work_started(&work)
        .expect("atomically checkpoint started work");
    let checkpoint = store
        .open(first.attempt_id())
        .expect("reopen intermediate checkpoint");
    assert_eq!(
        checkpoint.node_status(&work),
        Some(crate::ReceiptStatus::Running)
    );
    drop(recorder);
    assert_eq!(
        store
            .open(first.attempt_id())
            .expect("aborted receipt")
            .status(),
        crate::ReceiptStatus::Aborted
    );
    assert!(
        fs::read_dir(directory.path())
            .expect("receipt entries")
            .all(|entry| entry
                .expect("receipt entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".receipt.json"))
    );

    let path = directory
        .path()
        .join(format!("{}.receipt.json", first.attempt_id()));
    let original = fs::read(&path).expect("serialized receipt");
    let mut corrupted: serde_json::Value = serde_json::from_slice(&original).expect("receipt JSON");
    corrupted["receipt"]["revision"] = serde_json::Value::from(999_u64);
    fs::write(
        &path,
        serde_json::to_vec_pretty(&corrupted).expect("corrupt JSON"),
    )
    .expect("write corrupt receipt");
    assert!(matches!(
        store.open(first.attempt_id()),
        Err(crate::ReceiptError::IntegrityMismatch)
    ));

    let mut unsupported: serde_json::Value =
        serde_json::from_slice(&original).expect("receipt JSON");
    unsupported["schema"]["version"] = serde_json::Value::from(999_u64);
    fs::write(
        &path,
        serde_json::to_vec_pretty(&unsupported).expect("unsupported JSON"),
    )
    .expect("write unsupported receipt");
    assert!(matches!(
        store.open(first.attempt_id()),
        Err(crate::ReceiptError::UnsupportedSchema { version: 999, .. })
    ));
    fs::write(&path, original).expect("restore receipt");

    let pruning_directory = tempfile::tempdir().expect("pruning directory");
    let pruning_store = crate::ExecutionReceiptStore::new(
        pruning_directory.path(),
        crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("pruning store");
    let pruned = provenance(63, 64);
    drop(
        pruning_store
            .begin(pruned, &problem, &plan)
            .expect("first retained receipt"),
    );
    let retained = provenance(65, 66);
    let retained_recorder = pruning_store
        .begin(retained, &problem, &plan)
        .expect("terminal evidence can be pruned within the count ceiling");
    assert!(matches!(
        pruning_store.open(pruned.attempt_id()),
        Err(crate::ReceiptError::Io { .. })
    ));
    drop(retained_recorder);

    let active_directory = tempfile::tempdir().expect("active directory");
    let active_store = crate::ExecutionReceiptStore::new(
        active_directory.path(),
        crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("active store");
    let active = provenance(67, 68);
    let active_recorder = active_store
        .begin(active, &problem, &plan)
        .expect("active receipt");
    assert!(matches!(
        active_store.begin(provenance(69, 70), &problem, &plan),
        Err(crate::ReceiptError::RetentionExceeded)
    ));
    assert_eq!(
        active_store
            .open(active.attempt_id())
            .expect("active evidence preserved")
            .status(),
        crate::ReceiptStatus::Running
    );
    drop(active_recorder);

    let byte_directory = tempfile::tempdir().expect("byte-bound directory");
    let byte_store = crate::ExecutionReceiptStore::new(
        byte_directory.path(),
        crate::ReceiptRetention::new(1, 1).expect("retention"),
    )
    .expect("byte-bound store");
    assert!(matches!(
        byte_store.begin(provenance(71, 72), &problem, &plan),
        Err(crate::ReceiptError::RetentionExceeded)
    ));
}

#[test]
fn receipt_store_rejects_an_initial_checkpoint_that_cannot_hold_terminal_evidence() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let provenance = |attempt, build| {
        crate::ExecutionProvenance::new(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([build; 32]),
        )
    };

    let sizing_directory = tempfile::tempdir().expect("sizing directory");
    let sizing_store = crate::ExecutionReceiptStore::new(
        sizing_directory.path(),
        crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("sizing store");
    let sizing_provenance = provenance(73, 74);
    let recorder = sizing_store
        .begin(sizing_provenance, &problem, &plan)
        .expect("initial checkpoint");
    let receipt_path = sizing_directory
        .path()
        .join(format!("{}.receipt.json", sizing_provenance.attempt_id()));
    let running_bytes = fs::metadata(&receipt_path).expect("running receipt").len();
    drop(recorder);
    let terminal_bytes = fs::metadata(&receipt_path).expect("terminal receipt").len();
    assert!(
        terminal_bytes > running_bytes,
        "the fixture must expose the Running-to-terminal growth hazard"
    );

    let constrained_directory = tempfile::tempdir().expect("constrained directory");
    let constrained_store = crate::ExecutionReceiptStore::new(
        constrained_directory.path(),
        crate::ReceiptRetention::new(1, terminal_bytes - 1).expect("retention"),
    )
    .expect("constrained store");

    assert!(matches!(
        constrained_store.begin(provenance(75, 76), &problem, &plan),
        Err(crate::ReceiptError::RetentionExceeded)
    ));
    assert!(
        fs::read_dir(constrained_directory.path())
            .expect("constrained receipt directory")
            .next()
            .is_none(),
        "failed preflight must not leave durable Running evidence"
    );
}

#[test]
fn receipt_store_reserves_json_escaped_terminal_evidence_before_begin() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let provenance = |attempt| {
        crate::ExecutionProvenance::new(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([83; 32]),
        )
    };
    let terminal_size = |attempt, resource: String| {
        let directory = tempfile::tempdir().expect("terminal sizing directory");
        let store = crate::ExecutionReceiptStore::new(
            directory.path(),
            crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
        )
        .expect("terminal sizing store");
        let identity = provenance(attempt);
        let mut recorder = store
            .begin(identity, &problem, &plan)
            .expect("initial checkpoint");
        recorder
            .finish(
                crate::ReceiptStatus::Infeasible,
                Some(crate::receipt::ReceiptFailure::infeasible(
                    &crate::ResourceError::Infeasible {
                        resource,
                        required: u64::MAX,
                        available: 0,
                    },
                )),
            )
            .expect("terminal checkpoint");
        fs::metadata(
            directory
                .path()
                .join(format!("{}.receipt.json", identity.attempt_id())),
        )
        .expect("terminal receipt")
        .len()
    };
    let plain_bytes = terminal_size(84, "x".repeat(128));
    let escaped_bytes = terminal_size(85, "\0".repeat(128));
    assert!(
        escaped_bytes > plain_bytes,
        "control characters must expose JSON escaping growth"
    );
    let between = plain_bytes + (escaped_bytes - plain_bytes) / 2;
    let constrained_directory = tempfile::tempdir().expect("constrained directory");
    let constrained_store = crate::ExecutionReceiptStore::new(
        constrained_directory.path(),
        crate::ReceiptRetention::new(1, between).expect("retention"),
    )
    .expect("constrained store");

    assert!(matches!(
        constrained_store.begin(provenance(86), &problem, &plan),
        Err(crate::ReceiptError::RetentionExceeded)
    ));
    assert!(
        fs::read_dir(constrained_directory.path())
            .expect("constrained receipt directory")
            .next()
            .is_none(),
        "escaped terminal evidence must be reserved before Running is persisted"
    );
}

#[test]
fn receipts_reopen_machine_readable_infeasibility_certificates() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let directory = tempfile::tempdir().expect("receipt directory");
    let store = crate::ExecutionReceiptStore::new(
        directory.path(),
        crate::ReceiptRetention::new(2, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    let provenance = |attempt| {
        crate::ExecutionProvenance::new(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([77; 32]),
        )
    };

    let no_capable = provenance(78);
    let mut recorder = store
        .begin(no_capable, &problem, &plan)
        .expect("begin no-capable receipt");
    recorder
        .finish(
            crate::ReceiptStatus::Infeasible,
            Some(crate::receipt::ReceiptFailure::infeasible(
                &crate::ResourceError::NoCapableAlternative,
            )),
        )
        .expect("finish no-capable receipt");
    assert_eq!(
        store
            .open(no_capable.attempt_id())
            .expect("no-capable receipt")
            .infeasibility_certificate(),
        Some(crate::ReceiptInfeasibilityCertificate::NoCapableAlternative)
    );

    let insufficient = provenance(79);
    let mut recorder = store
        .begin(insufficient, &problem, &plan)
        .expect("begin quantitative receipt");
    recorder
        .finish(
            crate::ReceiptStatus::Infeasible,
            Some(crate::receipt::ReceiptFailure::infeasible(
                &crate::ResourceError::Infeasible {
                    resource: "host-memory".to_string(),
                    required: 4_096,
                    available: 1_024,
                },
            )),
        )
        .expect("finish quantitative receipt");
    assert_eq!(
        store
            .open(insufficient.attempt_id())
            .expect("quantitative receipt")
            .infeasibility_certificate(),
        Some(crate::ReceiptInfeasibilityCertificate::Infeasible {
            resource: "host-memory".to_string(),
            required: 4_096,
            available: 1_024,
        })
    );
}
