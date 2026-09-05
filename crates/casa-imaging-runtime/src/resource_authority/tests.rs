// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

fn single_alternative(demand: DemandEnvelope) -> DemandAlternatives {
    let workers = demand.workers.hard().max(1);
    DemandAlternatives {
        required_capabilities: BTreeSet::new(),
        alternatives: vec![DemandAlternative {
            id: AlternativeId::new("only"),
            capabilities: CapabilityPredicate::default(),
            demand,
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: workers,
                maximum_batch_size: 1,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
        }],
    }
}

#[test]
fn profiled_authority_refreshes_mutable_storage_pressure_only_for_same_calibration() {
    let root = tempfile::tempdir().expect("storage root");
    let initial = ProductionStorageProfile::new(root.path(), 10_000, 8_000, 400, 300, 4, 3)
        .expect("initial profile");
    let authority =
        ResourceAuthority::detected_with_storage_profile(&initial).expect("profiled authority");
    let refreshed = ProductionStorageProfile::new(root.path(), 10_000, 6_000, 400, 300, 4, 3)
        .expect("refreshed pressure");

    let update = authority
        .refresh_storage_profile_pressure(&refreshed)
        .expect("same calibration accepts new pressure");
    assert_eq!(update.previous_epoch(), 0);
    assert_eq!(update.current_epoch(), 1);
    let pressure = authority.inner.state.lock().expect("authority state");
    assert_eq!(
        pressure
            .pressure
            .storage_available_bytes
            .get(refreshed.domain_id()),
        Some(&6_000)
    );
    assert_eq!(
        pressure
            .pressure
            .rate_available_per_second
            .get(refreshed.read_rate_id()),
        Some(&400)
    );
    assert_eq!(
        pressure
            .pressure
            .queue_available_slots
            .get(refreshed.queue_id()),
        Some(&4)
    );
    drop(pressure);

    let recalibrated = ProductionStorageProfile::new(root.path(), 10_000, 6_000, 401, 300, 4, 3)
        .expect("different calibration");
    assert!(matches!(
        authority.refresh_storage_profile_pressure(&recalibrated),
        Err(ResourceError::IncompatibleProductionStorageProfile)
    ));
}

fn inventory_with_views(memory_views: Vec<MemoryView>) -> HostInventory {
    let domain = CapacityDomainId::new("unified-memory");
    HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Unified,
                capacity_bytes: 1_000,
            }],
            memory_views,
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: Vec::new(),
            queue_resources: Vec::new(),
            logical_cpu_threads: 4,
            performance_cpu_cores: CpuClassCapacity::Known(2),
            cache_capacity_bytes: 1_000,
            lock_capacity: 10,
            file_descriptor_capacity: 10,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_000)]),
            available_cpu_threads: 4,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::new(),
            queue_available_slots: BTreeMap::new(),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_000,
            available_locks: 10,
            available_file_descriptors: 10,
        },
    }
}

#[test]
fn pure_memory_lease_remains_accounted_until_its_last_owner_drops() {
    let host_view = CapacityViewId::new("host-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: host_view.clone(),
        domain: CapacityDomainId::new("unified-memory"),
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid host inventory");
    let alternatives = DemandAlternatives {
        required_capabilities: BTreeSet::new(),
        alternatives: vec![DemandAlternative {
            id: AlternativeId::new("cross-plan-artifact"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: host_view.clone(),
                memory: vec![MemoryDemand {
                    allocation_id: "cross-plan-artifact".to_string(),
                    hard_bytes: 600,
                    preferred_bytes: 600,
                    views: vec![host_view],
                }],
                workers: CountDemand::zero(),
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
                minimum_workers: 0,
                maximum_workers: 0,
                maximum_batch_size: 1,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::MajorCycle]),
        }],
    };

    let lease = std::sync::Arc::new(
        authority
            .acquire(ResourcePolicy::Exclusive, alternatives.clone())
            .expect("first cross-plan artifact fits"),
    );
    let cloned_owner = std::sync::Arc::clone(&lease);
    assert_eq!(
        authority
            .acquire(ResourcePolicy::Exclusive, alternatives.clone())
            .expect_err("live artifact lease still owns its memory")
            .available(),
        Some(400)
    );

    drop(lease);
    assert_eq!(
        authority
            .acquire(ResourcePolicy::Exclusive, alternatives.clone())
            .expect_err("a cloned artifact still owns the shared lease")
            .available(),
        Some(400)
    );

    drop(cloned_owner);
    authority
        .acquire(ResourcePolicy::Exclusive, alternatives)
        .expect("dropping the final artifact owner releases its memory");
}

fn lock_only_demand() -> DemandEnvelope {
    DemandEnvelope {
        host_memory_view: CapacityViewId::new("host-memory"),
        memory: Vec::new(),
        workers: CountDemand::new(1, 1),
        overhead: RuntimeOverheadDemand::zero(),
        storage: Vec::new(),
        rates: Vec::new(),
        caches: CacheDemand::zero(),
        locks: CountDemand::new(1, 1),
        file_descriptors: CountDemand::zero(),
        queues: Vec::new(),
        transfers: Vec::new(),
        accelerators: Vec::new(),
        io_buffers: IoBufferDemand::zero(),
    }
}

#[test]
fn exact_measurement_set_lock_conflicts_are_source_scoped() {
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: CapacityViewId::new("host-memory"),
        domain: CapacityDomainId::new("unified-memory"),
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid lock inventory");
    let first = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(lock_only_demand()),
        )
        .expect("first source writer is admitted");
    let second = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(lock_only_demand()),
        )
        .expect("second source writer is admitted");
    let source_a =
        MeasurementSetIdentity::new(casa_imaging_model::LogicalIdentity::from_sha256([31; 32]));
    let source_b =
        MeasurementSetIdentity::new(casa_imaging_model::LogicalIdentity::from_sha256([32; 32]));

    let source_a_permit = first
        .permit(
            LeaseResource::MeasurementSetLock {
                measurement_set: source_a,
            },
            1,
        )
        .expect("first writer owns source A");
    assert_eq!(
        first
            .permit(
                LeaseResource::MeasurementSetLock {
                    measurement_set: source_a,
                },
                1,
            )
            .expect_err("source identity remains typed before aggregate lock capacity"),
        ResourceError::MeasurementSetLockUnavailable {
            measurement_set: source_a
        }
    );
    assert_eq!(
        second
            .permit(
                LeaseResource::MeasurementSetLock {
                    measurement_set: source_a,
                },
                1,
            )
            .expect_err("source A conflicts while its permit is live"),
        ResourceError::MeasurementSetLockUnavailable {
            measurement_set: source_a
        }
    );
    let source_b_permit = second
        .permit(
            LeaseResource::MeasurementSetLock {
                measurement_set: source_b,
            },
            1,
        )
        .expect("unrelated source B writes concurrently");

    source_a_permit.release().expect("release source A");
    let replacement = first
        .permit(
            LeaseResource::MeasurementSetLock {
                measurement_set: source_a,
            },
            1,
        )
        .expect("source A can be reacquired after release");
    drop(replacement);
    drop(source_b_permit);
}

#[test]
fn unknown_cpu_class_is_preserved_without_logical_thread_fallback() {
    let host = CapacityViewId::new("host-memory");
    let domain = CapacityDomainId::new("unified-memory");
    let mut inventory = inventory_with_views(vec![MemoryView {
        id: host,
        domain,
        kind: MemoryViewKind::Host,
    }]);
    inventory.topology.performance_cpu_cores = CpuClassCapacity::Unknown;

    let authority = ResourceAuthority::with_inventory(inventory)
        .expect("unknown CPU class is valid inventory knowledge");
    assert_eq!(
        authority.topology().performance_cpu_cores,
        CpuClassCapacity::Unknown
    );
}

#[test]
fn inventory_retains_typed_storage_transfer_queue_and_metal_topology() {
    let host = CapacityViewId::new("host-memory");
    let metal = CapacityViewId::new("metal-memory");
    let domain = CapacityDomainId::new("unified-memory");
    let read_rate = RateResourceId::new("scratch-read");
    let write_rate = RateResourceId::new("scratch-write");
    let transfer_rate = RateResourceId::new("unified-transfer");
    let storage_queue = QueueResourceId::new("scratch-queue");
    let command_queue = QueueResourceId::new("metal-command-queue");
    let transfer_queue = QueueResourceId::new("metal-transfer-queue");
    let mut inventory = inventory_with_views(vec![
        MemoryView {
            id: host.clone(),
            domain: domain.clone(),
            kind: MemoryViewKind::Host,
        },
        MemoryView {
            id: metal.clone(),
            domain,
            kind: MemoryViewKind::Metal,
        },
    ]);
    inventory.topology.rate_resources = vec![
        RateResource::new(read_rate.clone(), RateUnit::BytesPerSecond, 800),
        RateResource::new(write_rate.clone(), RateUnit::BytesPerSecond, 400),
        RateResource::new(transfer_rate.clone(), RateUnit::BytesPerSecond, 2_000),
    ];
    inventory.topology.queue_resources = vec![
        QueueResource::new(storage_queue.clone(), 8),
        QueueResource::new(command_queue.clone(), 4),
        QueueResource::new(transfer_queue.clone(), 2),
    ];
    inventory.topology.storage_domains = vec![StorageDomain {
        id: StorageDomainId::new("scratch-volume"),
        root: "/Volumes/scratch".into(),
        capacity_bytes: 10_000,
        read_rate,
        write_rate,
        operations_rate: None,
        queue: storage_queue,
    }];
    inventory.topology.accelerators = vec![Accelerator {
        id: AcceleratorId::new("metal-0"),
        kind: AcceleratorKind::Metal,
        memory_view: metal.clone(),
        command_queue,
        occupancy_slots: 1,
    }];
    inventory.topology.transfer_links = vec![TransferLink {
        id: TransferLinkId::new("host-to-metal"),
        source_view: host,
        destination_view: metal,
        rate: transfer_rate,
        queue: transfer_queue,
    }];
    inventory.pressure.storage_available_bytes =
        BTreeMap::from([(StorageDomainId::new("scratch-volume"), 9_000)]);
    inventory.pressure.rate_available_per_second = BTreeMap::from([
        (RateResourceId::new("scratch-read"), 700),
        (RateResourceId::new("scratch-write"), 350),
        (RateResourceId::new("unified-transfer"), 1_500),
    ]);
    inventory.pressure.queue_available_slots = BTreeMap::from([
        (QueueResourceId::new("scratch-queue"), 7),
        (QueueResourceId::new("metal-command-queue"), 4),
        (QueueResourceId::new("metal-transfer-queue"), 2),
    ]);
    inventory.pressure.accelerator_available_slots =
        BTreeMap::from([(AcceleratorId::new("metal-0"), 1)]);

    let authority = ResourceAuthority::with_inventory(inventory)
        .expect("all typed topology references are valid");
    assert_eq!(
        authority.topology().storage_domains[0].root,
        std::path::Path::new("/Volumes/scratch")
    );
    assert_eq!(
        authority.topology().accelerators[0].id,
        AcceleratorId::new("metal-0")
    );
    assert_eq!(
        authority.topology().transfer_links[0].id,
        TransferLinkId::new("host-to-metal")
    );
}

#[test]
fn metal_inventory_requires_process_device_and_queue_access() {
    assert!(!metal_inventory_available(false, true));
    assert!(!metal_inventory_available(true, false));
    assert!(metal_inventory_available(true, true));
}

#[test]
fn demand_is_admitted_against_named_storage_rate_and_queue_domains() {
    let host_domain = CapacityDomainId::new("host-domain");
    let host_view = CapacityViewId::new("host-view");
    let storage = StorageDomainId::new("products-volume");
    let read_rate = RateResourceId::new("products-read");
    let write_rate = RateResourceId::new("products-write");
    let queue = QueueResourceId::new("products-queue");
    let inventory = HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: host_domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 10_000,
            }],
            memory_views: vec![MemoryView {
                id: host_view.clone(),
                domain: host_domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: vec![StorageDomain {
                id: storage.clone(),
                root: "/data/products".into(),
                capacity_bytes: 10_000,
                read_rate: read_rate.clone(),
                write_rate: write_rate.clone(),
                operations_rate: None,
                queue: queue.clone(),
            }],
            rate_resources: vec![
                RateResource::new(read_rate.clone(), RateUnit::BytesPerSecond, 500),
                RateResource::new(write_rate.clone(), RateUnit::BytesPerSecond, 250),
            ],
            queue_resources: vec![QueueResource::new(queue.clone(), 8)],
            logical_cpu_threads: 8,
            performance_cpu_cores: CpuClassCapacity::Unknown,
            cache_capacity_bytes: 1_000,
            lock_capacity: 10,
            file_descriptor_capacity: 20,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(host_domain.clone(), 9_000)]),
            available_cpu_threads: 8,
            storage_available_bytes: BTreeMap::from([(storage.clone(), 8_000)]),
            rate_available_per_second: BTreeMap::from([
                (read_rate.clone(), 400),
                (write_rate.clone(), 200),
            ]),
            queue_available_slots: BTreeMap::from([(queue.clone(), 6)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_000,
            available_locks: 10,
            available_file_descriptors: 20,
        },
    };
    let authority = ResourceAuthority::with_inventory(inventory).expect("valid typed inventory");
    let demand = DemandEnvelope {
        host_memory_view: host_view.clone(),
        memory: vec![MemoryDemand {
            allocation_id: "image-grid".to_string(),
            hard_bytes: 100,
            preferred_bytes: 80,
            views: vec![host_view],
        }],
        workers: CountDemand::new(2, 1),
        overhead: RuntimeOverheadDemand::zero(),
        storage: vec![StorageDemand {
            demand_id: "published-products".to_string(),
            domain: storage.clone(),
            temporary_bytes: 100,
            staged_output_bytes: 200,
            final_output_bytes: 300,
            persistent_cache_bytes: 400,
            read_rate: CountDemand::zero(),
            write_rate: CountDemand::zero(),
            operations_rate: CountDemand::zero(),
            queue_slots: CountDemand::zero(),
        }],
        rates: vec![
            RateDemand {
                demand_id: "product-read".to_string(),
                resource: read_rate.clone(),
                amount: CountDemand::new(100, 80),
            },
            RateDemand {
                demand_id: "product-write".to_string(),
                resource: write_rate.clone(),
                amount: CountDemand::new(50, 40),
            },
        ],
        queues: vec![QueueDemand {
            demand_id: "product-io".to_string(),
            resource: queue.clone(),
            slots: CountDemand::new(3, 2),
        }],
        transfers: Vec::new(),
        accelerators: Vec::new(),
        caches: CacheDemand::zero(),
        locks: CountDemand::zero(),
        file_descriptors: CountDemand::zero(),
        io_buffers: IoBufferDemand::zero(),
    };

    let lease = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(demand.clone()),
        )
        .expect("named storage and I/O resources fit their domains");
    assert_eq!(lease.hard_ceilings().storage_bytes(&storage), 1_000);
    assert_eq!(lease.hard_ceilings().rate_per_second(&read_rate), 100);
    assert_eq!(lease.hard_ceilings().rate_per_second(&write_rate), 50);
    assert_eq!(lease.hard_ceilings().queue_slots(&queue), 3);
    assert_eq!(
        lease.declared_limit(&LeaseResource::Storage {
            demand_id: "published-products".to_string(),
            use_kind: StorageUseKind::PersistentCache,
        }),
        Some(400)
    );
    assert_eq!(
        lease.declared_limit(&LeaseResource::Rate {
            demand_id: "product-read".to_string(),
        }),
        Some(100)
    );
    assert_eq!(
        lease.declared_limit(&LeaseResource::Queue {
            demand_id: "product-io".to_string(),
        }),
        Some(3)
    );
}

#[test]
fn authority_selects_a_capable_feasible_alternative_and_reserves_its_headroom() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain: domain.clone(),
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid alternative inventory");
    let demand = |allocation_id: &str, bytes: u64| DemandEnvelope {
        host_memory_view: host.clone(),
        memory: vec![MemoryDemand {
            allocation_id: allocation_id.to_string(),
            hard_bytes: bytes,
            preferred_bytes: bytes,
            views: vec![host.clone()],
        }],
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
    };
    let alternative = |id: &str,
                       supported: &[&str],
                       demand: DemandEnvelope,
                       headroom_bytes: u64| DemandAlternative {
        id: AlternativeId::new(id),
        capabilities: CapabilityPredicate {
            supported: supported
                .iter()
                .map(|capability| CapabilityId::new(*capability))
                .collect(),
        },
        demand,
        headroom: ResourceHeadroom {
            memory_bytes: BTreeMap::from([(domain.clone(), headroom_bytes)]),
            ..ResourceHeadroom::default()
        },
        scaling: ScalingMetadata {
            minimum_workers: 1,
            maximum_workers: 4,
            maximum_batch_size: 1,
            maximum_tile_width: 1,
            maximum_tile_height: 1,
            maximum_slab_depth: 1,
            memory_bytes_per_worker: BTreeMap::from([(domain.clone(), 100)]),
        },
        quiescence_points: BTreeSet::from([QuiescencePoint::MajorCycle]),
    };
    let alternatives = DemandAlternatives {
        required_capabilities: BTreeSet::from([CapabilityId::new("mfs")]),
        alternatives: vec![
            alternative("wrong-capability", &["cube"], demand("wrong", 1), 0),
            alternative("too-large", &["mfs"], demand("large", 950), 100),
            alternative("selected", &["mfs", "cube"], demand("grid", 600), 100),
        ],
    };

    let lease = authority
        .acquire(ResourcePolicy::Exclusive, alternatives)
        .expect("the later capable and feasible alternative is selected");
    assert_eq!(
        lease.selected_alternative(),
        &AlternativeId::new("selected")
    );
    assert_eq!(lease.scaling().maximum_workers, 4);
    assert!(
        lease
            .capabilities()
            .supported
            .contains(&CapabilityId::new("mfs"))
    );
    assert_eq!(lease.demand().memory[0].allocation_id, "grid");
    assert_eq!(lease.headroom().memory_bytes.get(&domain), Some(&100));
    assert!(
        lease
            .quiescence_points()
            .contains(&QuiescencePoint::MajorCycle)
    );

    let probe = DemandAlternatives {
        required_capabilities: BTreeSet::new(),
        alternatives: vec![alternative("headroom-probe", &[], demand("probe", 301), 0)],
    };
    let error = authority
        .acquire(ResourcePolicy::Exclusive, probe)
        .expect_err("the selected alternative's 100-byte headroom remains reserved");
    assert_eq!(error.required(), Some(301));
    assert_eq!(error.available(), Some(300));
}

#[test]
fn cache_headroom_is_charged_to_its_physical_host_memory_domain() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain,
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid cache-headroom inventory");
    let demand = DemandEnvelope {
        host_memory_view: host.clone(),
        memory: vec![MemoryDemand {
            allocation_id: "grid".to_string(),
            hard_bytes: 900,
            preferred_bytes: 900,
            views: vec![host],
        }],
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
    };
    let alternatives = DemandAlternatives {
        required_capabilities: BTreeSet::new(),
        alternatives: vec![DemandAlternative {
            id: AlternativeId::new("cache-headroom"),
            capabilities: CapabilityPredicate::default(),
            demand,
            headroom: ResourceHeadroom {
                cache_bytes: 101,
                ..ResourceHeadroom::default()
            },
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
        }],
    };

    let error = authority
        .acquire(ResourcePolicy::Exclusive, alternatives)
        .expect_err("cache headroom cannot overcommit physical host memory");
    assert_eq!(error.required(), Some(1_001));
    assert_eq!(error.available(), Some(1_000));
}

#[test]
fn host_use_policies_apply_distinct_aggregate_hard_admission_caps() {
    let make_authority = || {
        let domain = CapacityDomainId::new("unified-memory");
        ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
            id: CapacityViewId::new("host-memory"),
            domain,
            kind: MemoryViewKind::Host,
        }]))
        .expect("valid policy inventory")
    };
    let demand = |id: &str, bytes: u64| {
        single_alternative(DemandEnvelope {
            host_memory_view: CapacityViewId::new("host-memory"),
            memory: vec![MemoryDemand {
                allocation_id: id.to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views: vec![CapacityViewId::new("host-memory")],
            }],
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
        })
    };

    let interactive = make_authority();
    let mut planning_base = demand("planning-base", 100);
    planning_base.alternatives[0].headroom.memory_bytes =
        BTreeMap::from([(CapacityDomainId::new("unified-memory"), 100)]);
    assert_eq!(
        interactive
            .remaining_planning_memory_bytes(
                &ResourcePolicy::Interactive,
                &planning_base.alternatives[0],
            )
            .expect("interactive planning capacity"),
        300
    );
    let error = interactive
        .acquire(
            ResourcePolicy::Interactive,
            demand("interactive-too-large", 501),
        )
        .expect_err("interactive admission preserves half of host memory");
    assert_eq!(error.available(), Some(500));
    let _interactive_a = interactive
        .acquire(ResourcePolicy::Interactive, demand("interactive-a", 250))
        .expect("first interactive lease fits");
    assert_eq!(
        interactive
            .remaining_planning_memory_bytes(
                &ResourcePolicy::Interactive,
                &planning_base.alternatives[0],
            )
            .expect("active leases reduce planning capacity"),
        50
    );
    let _interactive_b = interactive
        .acquire(ResourcePolicy::Interactive, demand("interactive-b", 250))
        .expect("second interactive lease reaches the aggregate cap");
    assert_eq!(
        interactive
            .acquire(ResourcePolicy::Interactive, demand("interactive-c", 1))
            .expect_err("aggregate interactive leases cannot exceed fifty percent")
            .available(),
        Some(0)
    );

    let balanced = make_authority();
    assert_eq!(
        balanced
            .remaining_planning_memory_bytes(
                &ResourcePolicy::Balanced,
                &planning_base.alternatives[0],
            )
            .expect("balanced planning capacity"),
        550
    );
    assert_eq!(
        balanced
            .acquire(ResourcePolicy::Balanced, demand("balanced-too-large", 751))
            .expect_err("balanced admission caps memory at seventy-five percent")
            .available(),
        Some(750)
    );
    balanced
        .acquire(ResourcePolicy::Balanced, demand("balanced", 750))
        .expect("balanced hard cap is usable");

    let exclusive = make_authority();
    assert_eq!(
        exclusive
            .remaining_planning_memory_bytes(
                &ResourcePolicy::Exclusive,
                &planning_base.alternatives[0],
            )
            .expect("exclusive planning capacity"),
        800
    );
    exclusive
        .acquire(ResourcePolicy::Exclusive, demand("exclusive", 1_000))
        .expect("exclusive admission may consume all pressured capacity");
}

#[test]
fn t51_explicit_serial_queue_capacity_does_not_remove_balanced_headroom() {
    let topology = inventory_with_views(Vec::new()).topology;
    let queue = QueueResourceId::new("source-spill-aw-reader");
    let capacity = ResourceGrant {
        workers: 4,
        queue_slots: BTreeMap::from([(queue.clone(), 4)]),
        ..ResourceGrant::default()
    };
    let balanced = apply_policy(&topology, &ResourcePolicy::Balanced, capacity.clone());
    assert_eq!(balanced.hard.queue_slots(&queue), 3);
    assert!(matches!(
        require_fit(
            "source-spill-aw-reader".to_string(),
            4,
            balanced.hard.queue_slots(&queue)
        ),
        Err(ResourceError::Infeasible {
            required: 4,
            available: 3,
            ..
        })
    ));
    let serial = apply_policy(
        &topology,
        &ResourcePolicy::Explicit(ResourceOverride {
            workers: Some(1),
            ..ResourceOverride::default()
        }),
        capacity,
    );
    assert_eq!(serial.hard.workers, 1);
    assert_eq!(serial.hard.queue_slots(&queue), 4);
}

#[test]
fn concurrent_admission_is_atomic_at_the_process_policy_ceiling() {
    use std::sync::{Arc, Barrier};

    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let mut inventory = inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain,
        kind: MemoryViewKind::Host,
    }]);
    inventory.topology.logical_cpu_threads = 10;
    inventory.topology.performance_cpu_cores = CpuClassCapacity::Known(10);
    inventory.pressure.available_cpu_threads = 10;
    let authority =
        ResourceAuthority::with_inventory(inventory).expect("valid concurrent inventory");
    let start = Arc::new(Barrier::new(11));
    let finish = Arc::new(Barrier::new(11));
    let workers = (0..10)
        .map(|worker| {
            let authority = authority.clone();
            let host = host.clone();
            let start = Arc::clone(&start);
            let finish = Arc::clone(&finish);
            std::thread::spawn(move || {
                let demand = DemandEnvelope {
                    host_memory_view: host.clone(),
                    memory: vec![MemoryDemand {
                        allocation_id: format!("grid-{worker}"),
                        hard_bytes: 100,
                        preferred_bytes: 100,
                        views: vec![host],
                    }],
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
                };
                start.wait();
                let lease =
                    authority.acquire(ResourcePolicy::Interactive, single_alternative(demand));
                finish.wait();
                lease.is_ok()
            })
        })
        .collect::<Vec<_>>();
    start.wait();
    finish.wait();

    let admitted = workers
        .into_iter()
        .map(|worker| worker.join().expect("admission worker did not panic"))
        .filter(|admitted| *admitted)
        .count();
    assert_eq!(admitted, 5);
}

#[test]
fn lease_permits_enforce_named_hard_limits_and_own_capacity_until_drop() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain,
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid permit inventory");
    let demand = |id: &str, bytes: u64| {
        single_alternative(DemandEnvelope {
            host_memory_view: host.clone(),
            memory: vec![MemoryDemand {
                allocation_id: id.to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views: vec![host.clone()],
            }],
            workers: CountDemand::new(1, 1),
            overhead: RuntimeOverheadDemand {
                thread_stack_bytes: 10,
                ..RuntimeOverheadDemand::zero()
            },
            storage: Vec::new(),
            rates: Vec::new(),
            caches: CacheDemand::zero(),
            locks: CountDemand::zero(),
            file_descriptors: CountDemand::zero(),
            queues: Vec::new(),
            transfers: Vec::new(),
            accelerators: Vec::new(),
            io_buffers: IoBufferDemand {
                source_read_ahead_bytes: 20,
                ..IoBufferDemand::zero()
            },
        })
    };
    let lease = authority
        .acquire(ResourcePolicy::Exclusive, demand("grid", 100))
        .expect("permit demand fits");
    let grid = LeaseResource::Memory {
        allocation_id: "grid".to_string(),
    };
    assert_eq!(lease.declared_limit(&grid), Some(100));
    assert_eq!(
        lease.declared_limit(&LeaseResource::RuntimeOverhead(
            RuntimeOverheadKind::ThreadStack
        )),
        Some(10)
    );
    assert_eq!(
        lease.declared_limit(&LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)),
        Some(20)
    );

    let permit = lease
        .permit(grid.clone(), 60)
        .expect("first consumption fits the named ceiling");
    let error = lease
        .permit(grid.clone(), 41)
        .expect_err("concurrent consumption cannot exceed the remaining forty bytes");
    assert_eq!(error.available(), Some(40));
    assert!(matches!(
        lease.permit(
            LeaseResource::Memory {
                allocation_id: "undeclared".to_string(),
            },
            1,
        ),
        Err(ResourceError::UndeclaredLeaseResource(_))
    ));
    assert!(
        !lease
            .release()
            .expect("release waits for owned permits")
            .is_released()
    );

    assert_eq!(
        authority
            .acquire(ResourcePolicy::Exclusive, demand("blocked", 881))
            .expect_err("the released lease still owns 110 physical bytes")
            .available(),
        Some(890)
    );
    drop(permit);
    authority
        .acquire(ResourcePolicy::Exclusive, demand("after-drop", 881))
        .expect("dropping the last permit returns the pending lease reservation");
}

#[test]
fn io_buffer_activity_ceilings_do_not_duplicate_physical_slot_capacity() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain: domain.clone(),
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid I/O-buffer accounting inventory");
    let demand = DemandEnvelope {
        host_memory_view: host.clone(),
        memory: vec![MemoryDemand {
            allocation_id: "shared-io-slot".to_string(),
            hard_bytes: 600,
            preferred_bytes: 600,
            views: vec![host],
        }],
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
        io_buffers: IoBufferDemand {
            source_read_ahead_bytes: 600,
            writeback_bytes: 600,
            publication_bytes: 600,
            ..IoBufferDemand::zero()
        },
    };

    let lease = authority
        .acquire(ResourcePolicy::Exclusive, single_alternative(demand))
        .expect("three disjoint logical ceilings share one physical 600-byte slot");

    assert_eq!(lease.hard_ceilings().memory_bytes(&domain), 600);
    for kind in [
        IoBufferKind::SourceReadAhead,
        IoBufferKind::Writeback,
        IoBufferKind::Publication,
    ] {
        assert_eq!(
            lease.declared_limit(&LeaseResource::IoBuffer(kind)),
            Some(600)
        );
    }
}

#[test]
fn narrowed_quarantine_reserves_only_external_resources_without_retaining_policy_caps() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain: domain.clone(),
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid quarantine-policy inventory");
    let demand = |allocation: Option<(&str, u64)>, workers| DemandEnvelope {
        host_memory_view: host.clone(),
        memory: allocation
            .map(|(allocation_id, bytes)| MemoryDemand {
                allocation_id: allocation_id.to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views: vec![host.clone()],
            })
            .into_iter()
            .collect(),
        workers: CountDemand::new(workers, workers),
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
    };
    let lease = authority
        .acquire(
            ResourcePolicy::Interactive,
            single_alternative(demand(Some(("failed-slot", 100)), 1)),
        )
        .expect("interactive failed run is admitted");
    let failed_slot = lease
        .permit(
            LeaseResource::Memory {
                allocation_id: "failed-slot".to_string(),
            },
            100,
        )
        .expect("failed physical slot is live");

    lease
        .quarantine_external_permits(vec![failed_slot])
        .expect("quarantine narrows to the failed memory permit");

    let exclusive = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(demand(None, 4)),
        )
        .expect("quarantined Interactive policy does not cap unrelated Exclusive workers");
    assert!(
        exclusive
            .release()
            .expect("exclusive worker lease releases")
            .is_released()
    );
    let error = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(demand(Some(("too-large", 901)), 1)),
        )
        .expect_err("the quarantined one hundred memory bytes remain unavailable");
    assert_eq!(error.available(), Some(900));
}

#[test]
fn source_handle_quarantine_retains_exact_lock_and_file_descriptor_reservations() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain,
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid source-handle quarantine inventory");
    let demand = |locks, file_descriptors, workers| DemandEnvelope {
        host_memory_view: host.clone(),
        memory: Vec::new(),
        workers: CountDemand::new(workers, workers),
        overhead: RuntimeOverheadDemand::zero(),
        storage: Vec::new(),
        rates: Vec::new(),
        caches: CacheDemand::zero(),
        locks: CountDemand::new(locks, locks),
        file_descriptors: CountDemand::new(file_descriptors, file_descriptors),
        queues: Vec::new(),
        transfers: Vec::new(),
        accelerators: Vec::new(),
        io_buffers: IoBufferDemand::zero(),
    };
    let lease = authority
        .acquire(
            ResourcePolicy::Interactive,
            single_alternative(demand(1, 2, 1)),
        )
        .expect("source-handle run is admitted");
    let measurement_set = casa_imaging_model::MeasurementSetIdentity::new(
        casa_imaging_model::LogicalIdentity::from_sha256([17; 32]),
    );
    let lock = lease
        .permit(LeaseResource::MeasurementSetLock { measurement_set }, 1)
        .expect("MeasurementSet lock is live");
    let file_descriptors = lease
        .permit(LeaseResource::FileDescriptors, 2)
        .expect("source file descriptors are live");

    lease
        .quarantine_external_permits(vec![lock, file_descriptors])
        .expect("failed source release retains only its source handles");

    let lock_error = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(demand(10, 0, 1)),
        )
        .expect_err("one quarantined lock remains unavailable");
    assert_eq!(lock_error.available(), Some(9));
    let descriptor_error = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(demand(0, 9, 1)),
        )
        .expect_err("two quarantined file descriptors remain unavailable");
    assert_eq!(descriptor_error.available(), Some(8));
    let unrelated = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(demand(0, 0, 4)),
        )
        .expect("quarantined Interactive policy does not cap unrelated workers");
    assert!(
        unrelated
            .release()
            .expect("unrelated worker lease releases")
            .is_released()
    );
}

#[test]
fn pressure_updates_reject_active_overcommit_and_advance_an_observable_epoch() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let inventory = inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain: domain.clone(),
        kind: MemoryViewKind::Host,
    }]);
    let mut pressure = inventory.pressure.clone();
    let authority = ResourceAuthority::with_inventory(inventory).expect("valid pressure inventory");
    let demand = |id: &str, bytes: u64| {
        single_alternative(DemandEnvelope {
            host_memory_view: host.clone(),
            memory: vec![MemoryDemand {
                allocation_id: id.to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views: vec![host.clone()],
            }],
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
        })
    };
    let lease = authority
        .acquire(ResourcePolicy::Exclusive, demand("active", 600))
        .expect("active lease fits initial pressure");
    let acquired_pressure_epoch = lease.pressure_epoch();

    pressure.memory_available_bytes.insert(domain.clone(), 500);
    let error = authority
        .update_external_pressure(pressure.clone())
        .expect_err("pressure cannot invalidate an active hard reservation");
    assert!(matches!(
        error,
        ResourceError::PressureWouldInvalidateLeases {
            reserved: 600,
            available: 500,
            ..
        }
    ));
    assert_eq!(
        authority
            .pressure_epoch()
            .expect("authority pressure epoch"),
        acquired_pressure_epoch + 1
    );
    assert!(lease.pressure_changed().expect("lease epoch is observable"));
    assert_eq!(
        authority
            .acquire(ResourcePolicy::Exclusive, demand("stale-pressure-probe", 1))
            .expect_err("observed pressure prevents new admission while overcommitted")
            .available(),
        Some(0)
    );

    pressure.memory_available_bytes.insert(domain, 700);
    let update = authority
        .update_external_pressure(pressure)
        .expect("pressure still covers all active hard reservations");
    assert_eq!(update.previous_epoch(), acquired_pressure_epoch + 1);
    assert_eq!(update.current_epoch(), acquired_pressure_epoch + 2);
    assert!(
        lease
            .pressure_changed()
            .expect("lease observes the new epoch")
    );
    assert_eq!(
        authority
            .acquire(ResourcePolicy::Exclusive, demand("probe", 101))
            .expect_err("only one hundred bytes remain under new pressure")
            .available(),
        Some(100)
    );
}

#[test]
fn active_policy_headroom_remains_binding_under_concurrency_and_pressure() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let inventory = inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain: domain.clone(),
        kind: MemoryViewKind::Host,
    }]);
    let mut pressure = inventory.pressure.clone();
    let authority = ResourceAuthority::with_inventory(inventory).expect("valid policy inventory");
    let demand = |id: &str, bytes: u64| {
        single_alternative(DemandEnvelope {
            host_memory_view: host.clone(),
            memory: vec![MemoryDemand {
                allocation_id: id.to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views: vec![host.clone()],
            }],
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
        })
    };
    let lease = authority
        .acquire(ResourcePolicy::Interactive, demand("interactive", 400))
        .expect("interactive reservation fits its initial fifty-percent ceiling");

    assert_eq!(
        authority
            .acquire(
                ResourcePolicy::Exclusive,
                demand("conflicting-exclusive", 101)
            )
            .expect_err("a looser concurrent policy cannot consume active interactive headroom")
            .available(),
        Some(100)
    );

    pressure.memory_available_bytes.insert(domain.clone(), 700);
    assert!(matches!(
        authority.update_external_pressure(pressure.clone()),
        Err(ResourceError::PressureWouldInvalidateLeases {
            reserved: 400,
            available: 350,
            ..
        })
    ));
    assert!(
        lease
            .pressure_changed()
            .expect("overcommitting pressure remains observable")
    );

    pressure.memory_available_bytes.insert(domain, 800);
    authority
        .update_external_pressure(pressure)
        .expect("the reduced pressure preserves the interactive hard headroom");
    assert!(
        lease
            .pressure_changed()
            .expect("accepted pressure is observable")
    );
}

#[test]
fn explicit_policy_rejects_unknown_typed_resources_and_caps_known_memory() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let make_authority = || {
        ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
            id: host.clone(),
            domain: domain.clone(),
            kind: MemoryViewKind::Host,
        }]))
        .expect("valid override inventory")
    };
    let demand = |bytes: u64| {
        single_alternative(DemandEnvelope {
            host_memory_view: host.clone(),
            memory: vec![MemoryDemand {
                allocation_id: "grid".to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views: vec![host.clone()],
            }],
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
        })
    };
    let unknown_queue = ResourcePolicy::Explicit(ResourceOverride {
        queue_slots: BTreeMap::from([(QueueResourceId::new("missing"), 1)]),
        ..ResourceOverride::default()
    });
    assert!(matches!(
        make_authority().acquire(unknown_queue, demand(1)),
        Err(ResourceError::Invalid(message)) if message.contains("queue")
    ));

    let capped = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: BTreeMap::from([(domain.clone(), 500)]),
        ..ResourceOverride::default()
    });
    let error = make_authority()
        .acquire(capped, demand(501))
        .expect_err("known explicit memory ceiling is binding");
    assert_eq!(error.available(), Some(500));
}

#[test]
fn unified_memory_is_single_charged_and_fences_gate_reuse_fail_closed() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let metal = CapacityViewId::new("metal-memory");
    let make_authority = || {
        ResourceAuthority::with_inventory(inventory_with_views(vec![
            MemoryView {
                id: host.clone(),
                domain: domain.clone(),
                kind: MemoryViewKind::Host,
            },
            MemoryView {
                id: metal.clone(),
                domain: domain.clone(),
                kind: MemoryViewKind::Metal,
            },
        ]))
        .expect("valid unified inventory")
    };
    let demand = |id: &str, bytes: u64, views: Vec<CapacityViewId>| {
        single_alternative(DemandEnvelope {
            host_memory_view: host.clone(),
            memory: vec![MemoryDemand {
                allocation_id: id.to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views,
            }],
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
        })
    };

    let authority = make_authority();
    let lease = authority
        .acquire(
            ResourcePolicy::Exclusive,
            demand("shared-grid", 600, vec![host.clone(), metal.clone()]),
        )
        .expect("one logical unified allocation fits");
    assert_eq!(lease.hard_ceilings().memory_bytes(&domain), 600);
    let fence = lease
        .register_fence()
        .expect("active lease accepts a fence");
    assert!(!lease.release().expect("release is pending").is_released());
    assert_eq!(
        authority
            .acquire(
                ResourcePolicy::Exclusive,
                demand("blocked", 401, vec![host.clone()]),
            )
            .expect_err("fenced capacity cannot be reused")
            .available(),
        Some(400)
    );
    assert!(
        fence
            .complete()
            .expect("fence completion releases")
            .is_released()
    );
    authority
        .acquire(
            ResourcePolicy::Exclusive,
            demand("reused", 401, vec![host.clone()]),
        )
        .expect("completed fence permits reuse");

    let fail_closed = make_authority();
    let lease = fail_closed
        .acquire(
            ResourcePolicy::Exclusive,
            demand("dropped-fence", 600, vec![host.clone(), metal]),
        )
        .expect("fenced lease fits");
    let fence = lease.register_fence().expect("fence registration");
    assert!(!lease.release().expect("release is pending").is_released());
    drop(fence);
    assert!(
        fail_closed
            .acquire(
                ResourcePolicy::Exclusive,
                demand("still-blocked", 401, vec![host.clone()]),
            )
            .is_err()
    );
}

#[test]
fn every_runtime_and_io_buffer_category_retains_a_named_lease_limit() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: host.clone(),
        domain,
        kind: MemoryViewKind::Host,
    }]))
    .expect("valid accounting inventory");
    let demand = DemandEnvelope {
        host_memory_view: host,
        memory: Vec::new(),
        workers: CountDemand::new(1, 1),
        overhead: RuntimeOverheadDemand {
            thread_stack_bytes: 1,
            allocator_fragmentation_bytes: 2,
            external_library_bytes: 3,
            fft_workspace_bytes: 4,
            driver_bytes: 5,
            jit_bytes: 6,
            command_buffer_bytes: 7,
        },
        storage: Vec::new(),
        rates: Vec::new(),
        caches: CacheDemand::zero(),
        locks: CountDemand::zero(),
        file_descriptors: CountDemand::zero(),
        queues: Vec::new(),
        transfers: Vec::new(),
        accelerators: Vec::new(),
        io_buffers: IoBufferDemand {
            source_read_ahead_bytes: 11,
            decode_bytes: 12,
            preparation_bytes: 13,
            host_to_device_transfer_bytes: 14,
            device_to_host_transfer_bytes: 15,
            spill_read_bytes: 16,
            spill_write_bytes: 17,
            serialization_bytes: 18,
            storage_manager_bytes: 19,
            tiled_column_writer_bytes: 20,
            scalar_column_writer_bytes: 21,
            writeback_bytes: 22,
            publication_bytes: 23,
            mapped_page_cache_bytes: 24,
        },
    };
    let lease = authority
        .acquire(ResourcePolicy::Exclusive, single_alternative(demand))
        .expect("complete named accounting fits");
    for (kind, expected) in [
        (RuntimeOverheadKind::ThreadStack, 1),
        (RuntimeOverheadKind::AllocatorFragmentation, 2),
        (RuntimeOverheadKind::ExternalLibrary, 3),
        (RuntimeOverheadKind::FftWorkspace, 4),
        (RuntimeOverheadKind::Driver, 5),
        (RuntimeOverheadKind::Jit, 6),
        (RuntimeOverheadKind::CommandBuffer, 7),
    ] {
        assert_eq!(
            lease.declared_limit(&LeaseResource::RuntimeOverhead(kind)),
            Some(expected)
        );
    }
    for (kind, expected) in [
        (IoBufferKind::SourceReadAhead, 11),
        (IoBufferKind::Decode, 12),
        (IoBufferKind::Preparation, 13),
        (IoBufferKind::HostToDeviceTransfer, 14),
        (IoBufferKind::DeviceToHostTransfer, 15),
        (IoBufferKind::SpillRead, 16),
        (IoBufferKind::SpillWrite, 17),
        (IoBufferKind::Serialization, 18),
        (IoBufferKind::StorageManager, 19),
        (IoBufferKind::TiledColumnWriter, 20),
        (IoBufferKind::ScalarColumnWriter, 21),
        (IoBufferKind::Writeback, 22),
        (IoBufferKind::Publication, 23),
        (IoBufferKind::MappedPageCache, 24),
    ] {
        assert_eq!(
            lease.declared_limit(&LeaseResource::IoBuffer(kind)),
            Some(expected)
        );
    }
}

#[test]
fn internal_detection_invents_no_storage_domain_and_identifies_at_most_one_metal_device() {
    let inventory = HostInventory::detect().expect("host inventory detection succeeds");
    assert!(inventory.topology.storage_domains.is_empty());
    let metal = inventory
        .topology
        .accelerators
        .iter()
        .filter(|accelerator| accelerator.kind == AcceleratorKind::Metal)
        .collect::<Vec<_>>();
    assert!(metal.len() <= 1);
    let has_metal_view = inventory
        .topology
        .memory_views
        .iter()
        .any(|view| view.kind == MemoryViewKind::Metal);
    assert_eq!(metal.len(), usize::from(has_metal_view));
    if let Some(accelerator) = metal.first() {
        assert_eq!(accelerator.id, AcceleratorId::new("production-metal-0"));
    }
}

#[test]
fn storage_transfer_and_accelerator_demands_bind_their_topology_resources() {
    let memory = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let metal = CapacityViewId::new("metal-memory");
    let storage = StorageDomainId::new("scratch");
    let read = RateResourceId::new("scratch-read");
    let write = RateResourceId::new("scratch-write");
    let iops = RateResourceId::new("scratch-iops");
    let transfer_rate = RateResourceId::new("host-metal-bandwidth");
    let storage_queue = QueueResourceId::new("scratch-queue");
    let transfer_queue = QueueResourceId::new("transfer-queue");
    let command_queue = QueueResourceId::new("metal-command-queue");
    let accelerator = AcceleratorId::new("metal-0");
    let transfer = TransferLinkId::new("host-to-metal");
    let inventory = HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: memory.clone(),
                kind: MemoryCapacityKind::Unified,
                capacity_bytes: 10_000,
            }],
            memory_views: vec![
                MemoryView {
                    id: host.clone(),
                    domain: memory.clone(),
                    kind: MemoryViewKind::Host,
                },
                MemoryView {
                    id: metal.clone(),
                    domain: memory.clone(),
                    kind: MemoryViewKind::Metal,
                },
            ],
            accelerators: vec![Accelerator {
                id: accelerator.clone(),
                kind: AcceleratorKind::Metal,
                memory_view: metal.clone(),
                command_queue: command_queue.clone(),
                occupancy_slots: 1,
            }],
            transfer_links: vec![TransferLink {
                id: transfer.clone(),
                source_view: host.clone(),
                destination_view: metal.clone(),
                rate: transfer_rate.clone(),
                queue: transfer_queue.clone(),
            }],
            storage_domains: vec![StorageDomain {
                id: storage.clone(),
                root: "/data/scratch".into(),
                capacity_bytes: 10_000,
                read_rate: read.clone(),
                write_rate: write.clone(),
                operations_rate: Some(iops.clone()),
                queue: storage_queue.clone(),
            }],
            rate_resources: vec![
                RateResource::new(read.clone(), RateUnit::BytesPerSecond, 1_000),
                RateResource::new(write.clone(), RateUnit::BytesPerSecond, 500),
                RateResource::new(iops.clone(), RateUnit::OperationsPerSecond, 100),
                RateResource::new(transfer_rate.clone(), RateUnit::BytesPerSecond, 2_000),
            ],
            queue_resources: vec![
                QueueResource::new(storage_queue.clone(), 8),
                QueueResource::new(transfer_queue.clone(), 2),
                QueueResource::new(command_queue.clone(), 4),
            ],
            logical_cpu_threads: 4,
            performance_cpu_cores: CpuClassCapacity::Unknown,
            cache_capacity_bytes: 1_000,
            lock_capacity: 10,
            file_descriptor_capacity: 20,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(memory.clone(), 10_000)]),
            available_cpu_threads: 4,
            storage_available_bytes: BTreeMap::from([(storage.clone(), 10_000)]),
            rate_available_per_second: BTreeMap::from([
                (read.clone(), 1_000),
                (write.clone(), 500),
                (iops.clone(), 100),
                (transfer_rate.clone(), 2_000),
            ]),
            queue_available_slots: BTreeMap::from([
                (storage_queue.clone(), 8),
                (transfer_queue.clone(), 2),
                (command_queue.clone(), 4),
            ]),
            accelerator_available_slots: BTreeMap::from([(accelerator.clone(), 1)]),
            cache_available_bytes: 1_000,
            available_locks: 10,
            available_file_descriptors: 20,
        },
    };
    let authority = ResourceAuthority::with_inventory(inventory).expect("valid linked topology");
    let demand = DemandEnvelope {
        host_memory_view: host.clone(),
        memory: vec![MemoryDemand {
            allocation_id: "shared-grid".to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![host, metal],
        }],
        workers: CountDemand::new(1, 1),
        overhead: RuntimeOverheadDemand::zero(),
        storage: vec![StorageDemand {
            demand_id: "scratch-io".to_string(),
            domain: storage.clone(),
            temporary_bytes: 1_000,
            staged_output_bytes: 0,
            final_output_bytes: 0,
            persistent_cache_bytes: 0,
            read_rate: CountDemand::new(100, 80),
            write_rate: CountDemand::new(50, 40),
            operations_rate: CountDemand::new(10, 8),
            queue_slots: CountDemand::new(2, 1),
        }],
        rates: Vec::new(),
        caches: CacheDemand::zero(),
        locks: CountDemand::zero(),
        file_descriptors: CountDemand::zero(),
        queues: Vec::new(),
        transfers: vec![TransferDemand {
            demand_id: "grid-upload".to_string(),
            link: transfer,
            rate: CountDemand::new(200, 150),
            queue_slots: CountDemand::new(1, 1),
        }],
        accelerators: vec![AcceleratorDemand {
            demand_id: "metal-gridder".to_string(),
            accelerator: accelerator.clone(),
            slots: CountDemand::new(1, 1),
            command_queue_slots: CountDemand::new(2, 1),
        }],
        io_buffers: IoBufferDemand::zero(),
    };

    let lease = authority
        .acquire(
            ResourcePolicy::Exclusive,
            single_alternative(demand.clone()),
        )
        .expect("linked storage, transfer, and accelerator demands fit");
    assert_eq!(lease.hard_ceilings().memory_bytes(&memory), 100);
    assert_eq!(lease.hard_ceilings().rate_per_second(&read), 100);
    assert_eq!(lease.hard_ceilings().rate_per_second(&write), 50);
    assert_eq!(lease.hard_ceilings().rate_per_second(&iops), 10);
    assert_eq!(lease.hard_ceilings().rate_per_second(&transfer_rate), 200);
    assert_eq!(lease.hard_ceilings().queue_slots(&storage_queue), 2);
    assert_eq!(lease.hard_ceilings().queue_slots(&transfer_queue), 1);
    assert_eq!(lease.hard_ceilings().queue_slots(&command_queue), 2);
    assert_eq!(lease.hard_ceilings().accelerator_slots(&accelerator), 1);
    assert_eq!(
        lease.declared_limit(&LeaseResource::StorageOperationsRate {
            demand_id: "scratch-io".to_string(),
        }),
        Some(10)
    );
    assert_eq!(
        lease.declared_limit(&LeaseResource::TransferRate {
            demand_id: "grid-upload".to_string(),
        }),
        Some(200)
    );
    assert_eq!(
        lease.declared_limit(&LeaseResource::AcceleratorCommandQueue {
            demand_id: "metal-gridder".to_string(),
        }),
        Some(2)
    );

    let mut retained = lease
        .permit(
            LeaseResource::Storage {
                demand_id: "scratch-io".to_string(),
                use_kind: StorageUseKind::Temporary,
            },
            1_000,
        )
        .expect("artifact storage fits the admitted plan");
    retained
        .narrow_temporary_storage_to(100)
        .expect("sealed artifact returns unused planned storage");
    assert!(
        !lease
            .release_retaining_artifact_storage()
            .expect("plan narrows to its artifact storage permit")
            .is_released()
    );
    let mut competing = demand;
    competing.memory.clear();
    competing.storage[0].temporary_bytes = 9_900;
    competing.storage[0].queue_slots = CountDemand::new(8, 8);
    competing.transfers.clear();
    competing.accelerators.clear();
    authority
        .acquire(ResourcePolicy::Exclusive, single_alternative(competing))
        .expect("artifact retention releases the plan's unrelated queues, rates, and workers");
    drop(retained);
}

#[test]
fn demand_host_memory_view_must_be_host_visible() {
    let domain = CapacityDomainId::new("unified-memory");
    let metal_view = CapacityViewId::new("metal-memory");
    let authority = ResourceAuthority::with_inventory(inventory_with_views(vec![MemoryView {
        id: metal_view.clone(),
        domain,
        kind: MemoryViewKind::Metal,
    }]))
    .expect("the inventory is structurally valid");
    let demand = DemandEnvelope {
        host_memory_view: metal_view.clone(),
        memory: vec![MemoryDemand {
            allocation_id: "grid".to_string(),
            hard_bytes: 1,
            preferred_bytes: 1,
            views: vec![metal_view],
        }],
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
    };

    let error = authority
        .acquire(ResourcePolicy::Exclusive, single_alternative(demand))
        .expect_err("Metal-only memory is not a host-visible accounting domain");
    assert!(matches!(error, ResourceError::Invalid(message) if message.contains("host view")));
}

#[test]
fn fractional_policy_scaling_preserves_large_capacities() {
    assert_eq!(
        scale_count_floor(u64::MAX, 3, 4),
        13_835_058_055_282_163_711
    );
    assert_eq!(scale_count_ceil(u64::MAX, 3, 4), 13_835_058_055_282_163_712);
}

#[test]
fn os_swap_or_compression_never_becomes_planned_capacity() {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let make_inventory = |available_bytes: u64| {
        let mut inventory = inventory_with_views(vec![MemoryView {
            id: host.clone(),
            domain: domain.clone(),
            kind: MemoryViewKind::Host,
        }]);
        inventory
            .pressure
            .memory_available_bytes
            .insert(domain.clone(), available_bytes);
        inventory
    };

    // An OS-level observation claiming more available memory than the declared
    // physical domain (for example from compressed or swapped pages) is not a
    // new physical capacity and is rejected outright.
    let authority = ResourceAuthority::with_inventory(make_inventory(1_000))
        .expect("valid pressure-bound inventory");

    let mut swap_backed = make_inventory(0).pressure;
    swap_backed.memory_available_bytes = BTreeMap::from([(domain.clone(), 5_000)]);
    assert!(matches!(
        authority.update_external_pressure(swap_backed),
        Err(ResourceError::Invalid(message)) if message.contains("exceeds physical capacity")
    ));

    // Admission remains bounded by declared physical capacity, never by the
    // rejected observation.
    let demand = |bytes: u64| {
        single_alternative(DemandEnvelope {
            host_memory_view: host.clone(),
            memory: vec![MemoryDemand {
                allocation_id: "grid".to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views: vec![host.clone()],
            }],
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
        })
    };
    let error = authority
        .acquire(ResourcePolicy::Exclusive, demand(1_001))
        .expect_err("planned capacity stays bounded by physical domains");
    assert_eq!(error.available(), Some(1_000));
}

#[test]
fn path_shaped_resource_identity_is_redacted_and_still_matches_capacity() {
    let domain = StorageDomainId::new("/private/data");
    let available = BTreeMap::from([(domain.clone(), 42)]);
    let identity = ResourceIdentity::new(format!("storage-domain:{domain:?}"));

    assert!(identity.as_str().starts_with("redacted:"));
    assert!(!identity.as_str().contains('/'));
    assert_eq!(
        resource_map_available("storage-domain", &available, identity.as_str()),
        Some(42)
    );
}
