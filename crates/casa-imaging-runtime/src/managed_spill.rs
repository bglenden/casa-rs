// SPDX-License-Identifier: LGPL-3.0-or-later
//! The authority-validated directory a run pages its cube state into.

use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::resource_authority::{ResourceAuthority, StorageIoResourceBinding};

/// Authority-validated writable location for one run's private paged state.
///
/// The directory lies inside the root of the storage domain its resources
/// name, on the same device, so the domain's capacity and rates govern it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedSpillStorage {
    resources: StorageIoResourceBinding,
    directory: PathBuf,
}

impl ManagedSpillStorage {
    /// Bind one writable directory to the calibrated storage domain that owns it.
    pub fn bind(
        authority: &ResourceAuthority,
        resources: StorageIoResourceBinding,
        directory: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let directory = validate_storage_directory(authority, &resources, directory.as_ref())
            .map_err(io::Error::other)?;
        Ok(Self {
            resources,
            directory,
        })
    }

    /// Return the path-free storage resources used by physical planning.
    #[must_use]
    pub const fn resources(&self) -> &StorageIoResourceBinding {
        &self.resources
    }

    /// Directory already certified against the Resource Authority's storage domain.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }
}

#[derive(Debug, Error)]
enum StorageBindingError {
    #[error("the storage binding does not match its authority domain")]
    Mismatch,
    #[error("the storage directory is not an absolute directory on its domain's device")]
    InvalidRoot,
    #[error("{operation} failed: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
}

fn validate_storage_directory(
    authority: &ResourceAuthority,
    storage: &StorageIoResourceBinding,
    directory: &Path,
) -> Result<PathBuf, StorageBindingError> {
    let domain = authority
        .topology()
        .storage_domains
        .iter()
        .find(|domain| &domain.id == storage.domain())
        .ok_or(StorageBindingError::Mismatch)?;
    if &domain.read_rate != storage.read_rate()
        || &domain.write_rate != storage.write_rate()
        || &domain.queue != storage.queue()
    {
        return Err(StorageBindingError::Mismatch);
    }
    let canonical = |path: &Path, operation| {
        path.canonicalize()
            .map_err(|source| StorageBindingError::Io { operation, source })
    };
    let root = canonical(&domain.root, "resolve the storage-domain root")?;
    let directory = canonical(directory, "resolve the storage directory")?;
    let metadata = |path: &Path, operation| {
        path.metadata()
            .map_err(|source| StorageBindingError::Io { operation, source })
    };
    let root_metadata = metadata(&root, "inspect the storage-domain root")?;
    let directory_metadata = metadata(&directory, "inspect the storage directory")?;
    if !root.is_absolute()
        || !root_metadata.is_dir()
        || !directory.is_absolute()
        || !directory_metadata.is_dir()
        || !directory.starts_with(&root)
        || directory_metadata.dev() != root_metadata.dev()
    {
        return Err(StorageBindingError::InvalidRoot);
    }
    Ok(directory)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::resource_authority::{
        CapacityDomainId, CpuClassCapacity, ExternalPressure, HostInventory, MemoryCapacityDomain,
        MemoryCapacityKind, MemoryView, MemoryViewKind, QueueResource, QueueResourceId,
        RateResource, RateResourceId, RateUnit, ResourceTopology, StorageDomain, StorageDomainId,
    };

    fn authority(root: &Path) -> (ResourceAuthority, StorageIoResourceBinding) {
        let memory_domain = CapacityDomainId::new("test-memory");
        let storage_domain = StorageDomainId::new("test-storage");
        let read_rate = RateResourceId::new("test-storage-read");
        let write_rate = RateResourceId::new("test-storage-write");
        let queue = QueueResourceId::new("test-storage-queue");
        let topology = ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: memory_domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1 << 20,
            }],
            memory_views: vec![MemoryView {
                id: crate::CapacityViewId::new("host-memory"),
                domain: memory_domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: vec![StorageDomain {
                id: storage_domain.clone(),
                root: root.to_path_buf(),
                capacity_bytes: 4_096,
                read_rate: read_rate.clone(),
                write_rate: write_rate.clone(),
                operations_rate: None,
                queue: queue.clone(),
            }],
            rate_resources: vec![
                RateResource::new(read_rate.clone(), RateUnit::BytesPerSecond, 1 << 20),
                RateResource::new(write_rate.clone(), RateUnit::BytesPerSecond, 1 << 20),
            ],
            queue_resources: vec![QueueResource::new(queue.clone(), 1)],
            logical_cpu_threads: 1,
            native_thread_stack_bytes: 512 << 10,
            page_bytes: 16 << 10,
            performance_cpu_cores: CpuClassCapacity::Known(1),
            cache_capacity_bytes: 1 << 20,
            lock_capacity: 0,
            file_descriptor_capacity: 4,
        };
        let pressure = ExternalPressure {
            memory_available_bytes: BTreeMap::from([(memory_domain, 1 << 20)]),
            available_cpu_threads: 1,
            storage_available_bytes: BTreeMap::from([(storage_domain.clone(), 4_096)]),
            rate_available_per_second: BTreeMap::from([
                (read_rate.clone(), 1 << 20),
                (write_rate.clone(), 1 << 20),
            ]),
            queue_available_slots: BTreeMap::from([(queue.clone(), 1)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1 << 20,
            available_locks: 0,
            available_file_descriptors: 4,
        };
        let authority = ResourceAuthority::with_inventory(HostInventory { topology, pressure })
            .expect("test resource authority");
        (
            authority,
            StorageIoResourceBinding::new(storage_domain, read_rate, write_rate, queue),
        )
    }

    #[test]
    fn storage_binds_only_directories_inside_its_domain_root_with_its_resources() {
        let root = tempfile::tempdir().expect("storage root");
        let nested = root.path().join("run");
        std::fs::create_dir(&nested).expect("run directory");
        let (authority, resources) = authority(root.path());
        let storage =
            ManagedSpillStorage::bind(&authority, resources.clone(), &nested).expect("binding");
        assert_eq!(storage.directory(), nested.canonicalize().unwrap());

        let outside = tempfile::tempdir().expect("outside root");
        assert!(ManagedSpillStorage::bind(&authority, resources.clone(), outside.path()).is_err());
        let foreign_queue = StorageIoResourceBinding::new(
            resources.domain().clone(),
            resources.read_rate().clone(),
            resources.write_rate().clone(),
            QueueResourceId::new("foreign-queue"),
        );
        assert!(ManagedSpillStorage::bind(&authority, foreign_queue, root.path()).is_err());
    }
}
