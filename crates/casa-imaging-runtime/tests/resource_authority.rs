// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;

use casa_imaging_runtime::{
    CapacityDomainId, CapacityViewId, CpuClassCapacity, ExternalPressure, HostInventory,
    MemoryCapacityDomain, MemoryCapacityKind, MemoryView, MemoryViewKind, ResourceAuthority,
    ResourceError, ResourceTopology,
};

#[test]
fn production_inventory_override_is_one_time_and_process_authority_is_singleton() {
    let domain = CapacityDomainId::new("configured-host-memory");
    let inventory = HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1_000,
            }],
            memory_views: vec![MemoryView {
                id: CapacityViewId::new("configured-host-view"),
                domain: domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: Vec::new(),
            queue_resources: Vec::new(),
            logical_cpu_threads: 2,
            performance_cpu_cores: CpuClassCapacity::Unknown,
            cache_capacity_bytes: 500,
            lock_capacity: 4,
            file_descriptor_capacity: 32,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 900)]),
            available_cpu_threads: 2,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::new(),
            queue_available_slots: BTreeMap::new(),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 400,
            available_locks: 4,
            available_file_descriptors: 32,
        },
    };

    let first = ResourceAuthority::install_production_inventory(inventory.clone())
        .expect("runtime bootstrap installs one authoritative profile");
    let second = ResourceAuthority::production().expect("same production authority");
    assert!(std::ptr::eq(first, second));
    assert!(matches!(
        ResourceAuthority::install_production_inventory(inventory),
        Err(ResourceError::ProductionAlreadyInitialized)
    ));
}
