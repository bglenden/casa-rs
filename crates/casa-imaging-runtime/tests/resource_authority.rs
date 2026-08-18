// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;

use casa_imaging_runtime::{
    CacheDemand, CapacityDomainId, CapacityViewId, CountDemand, DemandEnvelope, ExternalPressure,
    HostInventory, IoBufferDemand, MemoryCapacityDomain, MemoryCapacityKind, MemoryDemand,
    MemoryView, MemoryViewKind, QueueDemand, ResourceAuthority, ResourceOverride, ResourcePolicy,
    ResourceTopology, RuntimeOverheadDemand, StorageDemand,
};

const GIB: u64 = 1024 * 1024 * 1024;

fn complete_demand(
    allocation_id: &str,
    hard_bytes: u64,
    preferred_bytes: u64,
    views: Vec<CapacityViewId>,
) -> DemandEnvelope {
    DemandEnvelope {
        host_memory_view: CapacityViewId::new("host-memory"),
        memory: vec![MemoryDemand {
            allocation_id: allocation_id.to_string(),
            hard_bytes,
            preferred_bytes,
            views,
        }],
        workers: CountDemand::new(1, 1),
        overhead: RuntimeOverheadDemand::zero(),
        storage: StorageDemand::zero(),
        caches: CacheDemand::zero(),
        locks: CountDemand::zero(),
        file_descriptors: CountDemand::zero(),
        queues: QueueDemand::zero(),
        io_buffers: IoBufferDemand::zero(),
    }
}

#[test]
fn public_authority_arbitrates_unified_memory_and_external_pressure_across_concurrent_runs() {
    let unified = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let metal = CapacityViewId::new("metal-memory");
    let topology = ResourceTopology {
        memory_domains: vec![MemoryCapacityDomain {
            id: unified.clone(),
            kind: MemoryCapacityKind::Unified,
            capacity_bytes: 16 * GIB,
        }],
        memory_views: vec![
            MemoryView {
                id: host.clone(),
                domain: unified.clone(),
                kind: MemoryViewKind::Host,
            },
            MemoryView {
                id: metal.clone(),
                domain: unified.clone(),
                kind: MemoryViewKind::Metal,
            },
        ],
        logical_cpu_threads: 8,
        performance_cpu_cores: 4,
        storage_capacity_bytes: 64 * GIB,
        storage_read_bytes_per_second: 2 * GIB,
        storage_write_bytes_per_second: GIB,
        cache_capacity_bytes: 8 * GIB,
        lock_capacity: 64,
        file_descriptor_capacity: 256,
        queue_capacity: 128,
    };
    let authority = ResourceAuthority::with_inventory(HostInventory {
        topology,
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(unified.clone(), 12 * GIB)]),
            available_cpu_threads: 8,
            storage_available_bytes: 64 * GIB,
            cache_available_bytes: 8 * GIB,
            available_locks: 64,
            available_file_descriptors: 256,
            available_queue_slots: 128,
        },
    })
    .expect("valid injected inventory");

    let run_a = complete_demand("shared-grid", 8 * GIB, 8 * GIB, vec![host, metal]);
    let lease_a = authority
        .acquire(ResourcePolicy::Balanced, run_a)
        .expect("the first run fits");
    assert_eq!(lease_a.hard_ceilings().memory_bytes(&unified), 8 * GIB);

    let run_b = complete_demand(
        "host-products",
        5 * GIB,
        5 * GIB,
        vec![CapacityViewId::new("host-memory")],
    );
    let error = authority
        .acquire(ResourcePolicy::Balanced, run_b.clone())
        .expect_err("only four GiB remain while the first lease is active");
    assert_eq!(error.available(), Some(4 * GIB));
    assert_eq!(error.required(), Some(5 * GIB));

    authority
        .update_external_pressure(ExternalPressure {
            memory_available_bytes: BTreeMap::from([(unified.clone(), 6 * GIB)]),
            available_cpu_threads: 8,
            storage_available_bytes: 64 * GIB,
            cache_available_bytes: 8 * GIB,
            available_locks: 64,
            available_file_descriptors: 256,
            available_queue_slots: 128,
        })
        .expect("active leases survive a valid pressure update");
    let pressured = complete_demand(
        "pressure-probe",
        1,
        1,
        vec![CapacityViewId::new("host-memory")],
    );
    let pressure_error = authority
        .acquire(ResourcePolicy::Balanced, pressured)
        .expect_err("new work cannot consume capacity displaced by external pressure");
    assert_eq!(pressure_error.available(), Some(0));
    assert_eq!(lease_a.hard_ceilings().memory_bytes(&unified), 8 * GIB);

    let first_epoch = lease_a.epoch();
    assert!(
        lease_a
            .release()
            .expect("unfenced release succeeds")
            .is_released()
    );
    authority
        .update_external_pressure(ExternalPressure {
            memory_available_bytes: BTreeMap::from([(unified.clone(), 6 * GIB)]),
            available_cpu_threads: 8,
            storage_available_bytes: 64 * GIB,
            cache_available_bytes: 8 * GIB,
            available_locks: 64,
            available_file_descriptors: 256,
            available_queue_slots: 128,
        })
        .expect("pressure update matches the inventory");

    let lease_b = authority
        .acquire(ResourcePolicy::Balanced, run_b)
        .expect("five GiB fits under the refreshed six GiB ceiling");
    assert!(lease_b.epoch() > first_epoch);
    let too_large = complete_demand(
        "too-large",
        7 * GIB,
        7 * GIB,
        vec![CapacityViewId::new("host-memory")],
    );
    assert!(
        authority
            .acquire(ResourcePolicy::Balanced, too_large)
            .is_err()
    );
}

#[test]
fn resource_policies_change_preferred_targets_without_weakening_hard_ceilings() {
    fn authority() -> ResourceAuthority {
        let memory = CapacityDomainId::new("host-domain");
        ResourceAuthority::with_inventory(HostInventory {
            topology: ResourceTopology {
                memory_domains: vec![MemoryCapacityDomain {
                    id: memory.clone(),
                    kind: MemoryCapacityKind::Host,
                    capacity_bytes: 16 * GIB,
                }],
                memory_views: vec![MemoryView {
                    id: CapacityViewId::new("host"),
                    domain: memory.clone(),
                    kind: MemoryViewKind::Host,
                }],
                logical_cpu_threads: 8,
                performance_cpu_cores: 4,
                storage_capacity_bytes: 64 * GIB,
                storage_read_bytes_per_second: 2 * GIB,
                storage_write_bytes_per_second: GIB,
                cache_capacity_bytes: 8 * GIB,
                lock_capacity: 64,
                file_descriptor_capacity: 256,
                queue_capacity: 128,
            },
            pressure: ExternalPressure {
                memory_available_bytes: BTreeMap::from([(memory, 12 * GIB)]),
                available_cpu_threads: 8,
                storage_available_bytes: 64 * GIB,
                cache_available_bytes: 8 * GIB,
                available_locks: 64,
                available_file_descriptors: 256,
                available_queue_slots: 128,
            },
        })
        .expect("valid policy inventory")
    }

    fn policy_demand() -> DemandEnvelope {
        let mut demand = complete_demand(
            "policy-memory",
            12 * GIB,
            12 * GIB,
            vec![CapacityViewId::new("host")],
        );
        demand.host_memory_view = CapacityViewId::new("host");
        demand.workers = CountDemand::new(8, 8);
        demand
    }

    let interactive = authority()
        .acquire(ResourcePolicy::Interactive, policy_demand())
        .expect("interactive hard envelope fits");
    assert_eq!(
        interactive
            .hard_ceilings()
            .memory_bytes(&CapacityDomainId::new("host-domain")),
        12 * GIB
    );
    assert_eq!(interactive.hard_ceilings().workers(), 8);
    assert_eq!(
        interactive
            .preferred_targets()
            .memory_bytes(&CapacityDomainId::new("host-domain")),
        6 * GIB
    );
    assert_eq!(interactive.preferred_targets().workers(), 2);

    let balanced = authority()
        .acquire(ResourcePolicy::Balanced, policy_demand())
        .expect("balanced hard envelope fits");
    assert_eq!(
        balanced
            .preferred_targets()
            .memory_bytes(&CapacityDomainId::new("host-domain")),
        9 * GIB
    );
    assert_eq!(balanced.preferred_targets().workers(), 4);

    let exclusive = authority()
        .acquire(ResourcePolicy::Exclusive, policy_demand())
        .expect("exclusive hard envelope fits");
    assert_eq!(
        exclusive
            .preferred_targets()
            .memory_bytes(&CapacityDomainId::new("host-domain")),
        12 * GIB
    );
    assert_eq!(exclusive.preferred_targets().workers(), 8);
}

#[test]
fn release_waits_for_explicit_fence_completion_before_reusing_capacity() {
    let memory = CapacityDomainId::new("fenced-host-domain");
    let authority = ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: memory.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 12 * GIB,
            }],
            memory_views: vec![MemoryView {
                id: CapacityViewId::new("fenced-host"),
                domain: memory.clone(),
                kind: MemoryViewKind::Host,
            }],
            logical_cpu_threads: 4,
            performance_cpu_cores: 4,
            storage_capacity_bytes: 64 * GIB,
            storage_read_bytes_per_second: 2 * GIB,
            storage_write_bytes_per_second: GIB,
            cache_capacity_bytes: 8 * GIB,
            lock_capacity: 64,
            file_descriptor_capacity: 256,
            queue_capacity: 128,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(memory, 12 * GIB)]),
            available_cpu_threads: 4,
            storage_available_bytes: 64 * GIB,
            cache_available_bytes: 8 * GIB,
            available_locks: 64,
            available_file_descriptors: 256,
            available_queue_slots: 128,
        },
    })
    .expect("valid fence inventory");
    let mut first = complete_demand(
        "fenced-grid",
        8 * GIB,
        8 * GIB,
        vec![CapacityViewId::new("fenced-host")],
    );
    first.host_memory_view = CapacityViewId::new("fenced-host");
    let lease = authority
        .acquire(ResourcePolicy::Exclusive, first)
        .expect("first lease fits");
    let fence = lease
        .register_fence()
        .expect("active lease accepts a fence");
    assert!(
        !lease
            .release()
            .expect("fenced release request succeeds")
            .is_released()
    );

    let mut second = complete_demand(
        "waiting-products",
        5 * GIB,
        5 * GIB,
        vec![CapacityViewId::new("fenced-host")],
    );
    second.host_memory_view = CapacityViewId::new("fenced-host");
    assert!(
        authority
            .acquire(ResourcePolicy::Exclusive, second.clone())
            .is_err()
    );

    assert!(
        fence
            .complete()
            .expect("fence completion succeeds")
            .is_released()
    );
    assert!(authority.acquire(ResourcePolicy::Exclusive, second).is_ok());
}

#[test]
fn complete_demand_charges_every_runtime_storage_queue_and_io_buffer_category() {
    let memory = CapacityDomainId::new("accounting-host-domain");
    let authority = ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: memory.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 10_000,
            }],
            memory_views: vec![MemoryView {
                id: CapacityViewId::new("accounting-host"),
                domain: memory.clone(),
                kind: MemoryViewKind::Host,
            }],
            logical_cpu_threads: 8,
            performance_cpu_cores: 4,
            storage_capacity_bytes: 100,
            storage_read_bytes_per_second: 50,
            storage_write_bytes_per_second: 50,
            cache_capacity_bytes: 100,
            lock_capacity: 10,
            file_descriptor_capacity: 10,
            queue_capacity: 10,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(memory.clone(), 10_000)]),
            available_cpu_threads: 8,
            storage_available_bytes: 100,
            cache_available_bytes: 100,
            available_locks: 10,
            available_file_descriptors: 10,
            available_queue_slots: 10,
        },
    })
    .expect("valid accounting inventory");
    let demand = DemandEnvelope {
        host_memory_view: CapacityViewId::new("accounting-host"),
        memory: vec![MemoryDemand {
            allocation_id: "image-grid".to_string(),
            hard_bytes: 100,
            preferred_bytes: 80,
            views: vec![CapacityViewId::new("accounting-host")],
        }],
        workers: CountDemand::new(4, 2),
        overhead: RuntimeOverheadDemand {
            thread_stack_bytes: 10,
            allocator_fragmentation_bytes: 10,
            external_library_bytes: 10,
            fft_workspace_bytes: 10,
            driver_bytes: 10,
            jit_bytes: 10,
            command_buffer_bytes: 10,
        },
        storage: StorageDemand {
            temporary_bytes: 1,
            staged_output_bytes: 2,
            final_output_bytes: 3,
            persistent_cache_bytes: 4,
            read_bytes_per_second: 5,
            write_bytes_per_second: 6,
        },
        caches: CacheDemand {
            hard_resident_bytes: 30,
            preferred_resident_bytes: 20,
        },
        locks: CountDemand::new(3, 2),
        file_descriptors: CountDemand::new(4, 3),
        queues: QueueDemand {
            source_read_ahead_slots: 1,
            preparation_slots: 1,
            device_command_slots: 1,
            transfer_slots: 1,
            spill_slots: 1,
            writeback_slots: 1,
        },
        io_buffers: IoBufferDemand {
            source_read_ahead_bytes: 10,
            decode_bytes: 10,
            preparation_bytes: 10,
            host_to_device_transfer_bytes: 10,
            device_to_host_transfer_bytes: 10,
            spill_read_bytes: 10,
            spill_write_bytes: 10,
            serialization_bytes: 10,
            storage_manager_bytes: 10,
            tiled_column_writer_bytes: 10,
            scalar_column_writer_bytes: 10,
            writeback_bytes: 10,
            publication_bytes: 10,
            mapped_page_cache_bytes: 10,
        },
    };

    let lease = authority
        .acquire(ResourcePolicy::Exclusive, demand)
        .expect("the complete envelope fits");
    let hard = lease.hard_ceilings();
    assert_eq!(hard.memory_bytes(&memory), 340);
    assert_eq!(hard.workers(), 4);
    assert_eq!(hard.storage_bytes(), 10);
    assert_eq!(hard.storage_read_bytes_per_second(), 5);
    assert_eq!(hard.storage_write_bytes_per_second(), 6);
    assert_eq!(hard.cache_bytes(), 30);
    assert_eq!(hard.locks(), 3);
    assert_eq!(hard.file_descriptors(), 4);
    assert_eq!(hard.queue_slots(), 6);
    assert_eq!(lease.preferred_targets().memory_bytes(&memory), 310);
    assert_eq!(lease.preferred_targets().workers(), 2);
}

#[test]
fn production_inventory_is_positive_and_process_authority_is_singleton() {
    let inventory = HostInventory::detect().expect("production host inventory is available");
    assert!(inventory.topology.logical_cpu_threads > 0);
    assert!(inventory.topology.performance_cpu_cores > 0);
    assert!(!inventory.topology.memory_domains.is_empty());
    assert!(!inventory.topology.memory_views.is_empty());
    for domain in &inventory.topology.memory_domains {
        assert!(domain.capacity_bytes > 0);
        assert!(
            inventory
                .pressure
                .memory_available_bytes
                .get(&domain.id)
                .is_some_and(|available| *available <= domain.capacity_bytes)
        );
    }

    let first = ResourceAuthority::production().expect("production authority");
    let second = ResourceAuthority::production().expect("same production authority");
    assert!(std::ptr::eq(first, second));
}

#[test]
fn explicit_policy_rejects_unknown_domains_and_caps_known_capacity() {
    let memory = CapacityDomainId::new("override-host-domain");
    let make_authority = || {
        ResourceAuthority::with_inventory(HostInventory {
            topology: ResourceTopology {
                memory_domains: vec![MemoryCapacityDomain {
                    id: memory.clone(),
                    kind: MemoryCapacityKind::Host,
                    capacity_bytes: 10 * GIB,
                }],
                memory_views: vec![MemoryView {
                    id: CapacityViewId::new("override-host"),
                    domain: memory.clone(),
                    kind: MemoryViewKind::Host,
                }],
                logical_cpu_threads: 8,
                performance_cpu_cores: 4,
                storage_capacity_bytes: 64 * GIB,
                storage_read_bytes_per_second: 2 * GIB,
                storage_write_bytes_per_second: GIB,
                cache_capacity_bytes: 8 * GIB,
                lock_capacity: 64,
                file_descriptor_capacity: 256,
                queue_capacity: 128,
            },
            pressure: ExternalPressure {
                memory_available_bytes: BTreeMap::from([(memory.clone(), 10 * GIB)]),
                available_cpu_threads: 8,
                storage_available_bytes: 64 * GIB,
                cache_available_bytes: 8 * GIB,
                available_locks: 64,
                available_file_descriptors: 256,
                available_queue_slots: 128,
            },
        })
        .expect("valid override inventory")
    };
    let mut demand = complete_demand(
        "override-grid",
        7 * GIB,
        7 * GIB,
        vec![CapacityViewId::new("override-host")],
    );
    demand.host_memory_view = CapacityViewId::new("override-host");

    let unknown = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: BTreeMap::from([(CapacityDomainId::new("missing"), 6 * GIB)]),
        ..ResourceOverride::default()
    });
    assert!(make_authority().acquire(unknown, demand.clone()).is_err());

    let capped = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: BTreeMap::from([(memory.clone(), 6 * GIB)]),
        workers: Some(2),
        ..ResourceOverride::default()
    });
    let error = make_authority()
        .acquire(capped, demand)
        .expect_err("the explicit six GiB ceiling is binding");
    assert_eq!(error.required(), Some(7 * GIB));
    assert_eq!(error.available(), Some(6 * GIB));
}
