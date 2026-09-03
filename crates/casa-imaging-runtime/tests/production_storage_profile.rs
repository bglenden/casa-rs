// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_runtime::{
    HostInventory, ProductionStorageProfile, ResourceAuthority, ResourceError,
};

#[test]
fn production_storage_profile_rejects_missing_and_zero_facts() {
    assert!(ProductionStorageProfile::new("relative", 1, 1, 1, 1, 1, 1).is_err());
    let root = tempfile::tempdir().expect("storage root");
    for values in [
        [0, 1, 1, 1, 1, 1],
        [1, 0, 1, 1, 1, 1],
        [1, 2, 1, 1, 1, 1],
        [1, 1, 0, 1, 1, 1],
        [1, 1, 1, 0, 1, 1],
        [1, 1, 1, 1, 0, 1],
        [1, 1, 1, 1, 1, 0],
    ] {
        assert!(
            ProductionStorageProfile::new(
                root.path(),
                values[0],
                values[1],
                values[2],
                values[3],
                values[4],
                values[5],
            )
            .is_err()
        );
    }
}

#[test]
fn detected_inventory_contains_one_path_redacted_coherent_storage_domain() {
    let root = tempfile::tempdir().expect("storage root");
    let profile = ProductionStorageProfile::new(root.path(), 10_000, 8_000, 400, 300, 4, 3)
        .expect("valid profile");
    let inventory = HostInventory::detect_with_storage_profile(&profile).expect("host inventory");
    assert_eq!(inventory.topology.storage_domains.len(), 1);
    let storage = &inventory.topology.storage_domains[0];
    assert_eq!(&storage.id, profile.domain_id());
    assert_eq!(storage.root, root.path());
    assert_eq!(storage.capacity_bytes, 10_000);
    assert_eq!(&storage.read_rate, profile.read_rate_id());
    assert_eq!(&storage.write_rate, profile.write_rate_id());
    assert_eq!(&storage.queue, profile.queue_id());
    assert!(!profile.domain_id().as_str().contains('/'));
    assert_eq!(
        inventory
            .topology
            .rate_resources
            .iter()
            .find(|rate| &rate.id == profile.read_rate_id())
            .map(|rate| rate.units_per_second),
        Some(400)
    );
    assert_eq!(
        inventory
            .topology
            .rate_resources
            .iter()
            .find(|rate| &rate.id == profile.write_rate_id())
            .map(|rate| rate.units_per_second),
        Some(300)
    );
    assert_eq!(
        inventory
            .topology
            .queue_resources
            .iter()
            .find(|queue| &queue.id == profile.queue_id())
            .map(|queue| queue.slots),
        Some(4)
    );
    assert_eq!(inventory.topology.lock_capacity, 3);
    assert_eq!(inventory.pressure.available_locks, 3);
    assert_eq!(
        inventory
            .pressure
            .storage_available_bytes
            .get(profile.domain_id()),
        Some(&8_000)
    );
}

#[test]
fn production_profile_reinstallation_is_idempotent_only_when_compatible() {
    let root = tempfile::tempdir().expect("storage root");
    let profile = ProductionStorageProfile::new(root.path(), 10_000, 8_000, 400, 300, 4, 3)
        .expect("valid profile");
    let first = ResourceAuthority::production_with_storage_profile(&profile)
        .expect("install production profile");
    let repeated = ResourceAuthority::production_with_storage_profile(&profile)
        .expect("same profile is idempotent");
    assert!(std::ptr::eq(first, repeated));
    let refreshed = ProductionStorageProfile::new(root.path(), 10_000, 7_000, 400, 300, 4, 3)
        .expect("same calibration with refreshed availability");
    let refreshed_authority = ResourceAuthority::production_with_storage_profile(&refreshed)
        .expect("availability refresh is compatible");
    assert!(std::ptr::eq(first, refreshed_authority));

    let incompatible = ProductionStorageProfile::new(root.path(), 10_001, 8_000, 400, 300, 4, 3)
        .expect("individually valid profile");
    assert!(matches!(
        ResourceAuthority::production_with_storage_profile(&incompatible),
        Err(ResourceError::IncompatibleProductionStorageProfile)
    ));
}
