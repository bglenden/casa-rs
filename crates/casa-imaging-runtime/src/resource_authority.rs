// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};

/// Stable identity of one physical memory-capacity domain.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CapacityDomainId(String);

impl CapacityDomainId {
    /// Creates a capacity-domain identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the identity text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable identity of one host or accelerator view of a capacity domain.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CapacityViewId(String);

impl CapacityViewId {
    /// Creates a capacity-view identity.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the identity text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Physical location represented by a memory-capacity domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryCapacityKind {
    /// Ordinary host memory.
    Host,
    /// Physical memory shared by host and accelerator views.
    Unified,
    /// Accelerator-private memory.
    DevicePrivate,
}

/// One physical memory-capacity domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryCapacityDomain {
    /// Stable domain identity.
    pub id: CapacityDomainId,
    /// Physical kind of the domain.
    pub kind: MemoryCapacityKind,
    /// Installed or assigned physical capacity.
    pub capacity_bytes: u64,
}

/// Consumer-facing interpretation of a physical memory domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryViewKind {
    /// CPU-visible host-memory view.
    Host,
    /// Metal-visible memory view.
    Metal,
}

/// A host or accelerator view of a physical memory-capacity domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryView {
    /// Stable view identity.
    pub id: CapacityViewId,
    /// Physical domain viewed through this entry.
    pub domain: CapacityDomainId,
    /// Host or accelerator interpretation.
    pub kind: MemoryViewKind,
}

/// Physical, rate, and count topology inventoried for one process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceTopology {
    /// Physical memory-capacity domains.
    pub memory_domains: Vec<MemoryCapacityDomain>,
    /// Host and accelerator views of those domains.
    pub memory_views: Vec<MemoryView>,
    /// Logical CPU threads available to the process.
    pub logical_cpu_threads: u64,
    /// Performance-oriented CPU cores available to the process.
    pub performance_cpu_cores: u64,
    /// Capacity available for temporary and output storage reservations.
    pub storage_capacity_bytes: u64,
    /// Sequential storage read-rate capacity.
    pub storage_read_bytes_per_second: u64,
    /// Sequential storage write-rate capacity.
    pub storage_write_bytes_per_second: u64,
    /// Process-wide resident-cache capacity.
    pub cache_capacity_bytes: u64,
    /// Simultaneously held table or synchronization locks.
    pub lock_capacity: u64,
    /// File descriptors available to imaging work.
    pub file_descriptor_capacity: u64,
    /// Total bounded queue slots available to imaging work.
    pub queue_capacity: u64,
}

/// Mutable external pressure observed around the process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalPressure {
    /// Currently usable bytes in each physical memory domain.
    pub memory_available_bytes: BTreeMap<CapacityDomainId, u64>,
    /// CPU threads currently assignable to imaging work.
    pub available_cpu_threads: u64,
    /// Storage bytes currently assignable to imaging work.
    pub storage_available_bytes: u64,
    /// Resident-cache bytes currently assignable to imaging work.
    pub cache_available_bytes: u64,
    /// Lock slots currently assignable to imaging work.
    pub available_locks: u64,
    /// File descriptors currently assignable to imaging work.
    pub available_file_descriptors: u64,
    /// Queue slots currently assignable to imaging work.
    pub available_queue_slots: u64,
}

/// Immutable topology plus the initial pressure snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostInventory {
    /// Physical and rate topology.
    pub topology: ResourceTopology,
    /// Initial external-pressure snapshot.
    pub pressure: ExternalPressure,
}

impl HostInventory {
    /// Detects the production process's local CPU, memory, storage, and Metal
    /// topology without consulting a frontend.
    pub fn detect() -> Result<Self, ResourceError> {
        let (physical_memory_bytes, available_memory_bytes) = detect_host_memory()?;
        let logical_cpu_threads = std::thread::available_parallelism()
            .map_err(|error| ResourceError::Detection(error.to_string()))?
            .get() as u64;
        let performance_cpu_cores = detect_performance_cpu_cores()
            .unwrap_or(logical_cpu_threads)
            .clamp(1, logical_cpu_threads);
        let unified_metal_available = detect_unified_metal_device();
        let host_domain = CapacityDomainId::new("production-host-memory");
        let mut memory_views = vec![MemoryView {
            id: CapacityViewId::new("production-host-memory"),
            domain: host_domain.clone(),
            kind: MemoryViewKind::Host,
        }];
        if unified_metal_available {
            memory_views.push(MemoryView {
                id: CapacityViewId::new("production-metal-memory"),
                domain: host_domain.clone(),
                kind: MemoryViewKind::Metal,
            });
        }
        let (storage_capacity_bytes, storage_available_bytes) = detect_storage_capacity()?;
        let file_descriptor_capacity = detect_open_file_limit()?;
        let topology = ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: host_domain.clone(),
                kind: if unified_metal_available {
                    MemoryCapacityKind::Unified
                } else {
                    MemoryCapacityKind::Host
                },
                capacity_bytes: physical_memory_bytes,
            }],
            memory_views,
            logical_cpu_threads,
            performance_cpu_cores,
            storage_capacity_bytes,
            // Rates require a measured profile. Zero fails closed for a run
            // that declares a nonzero rate until such a profile is supplied.
            storage_read_bytes_per_second: 0,
            storage_write_bytes_per_second: 0,
            cache_capacity_bytes: physical_memory_bytes,
            // Table and synchronization capacity has no portable detector.
            // Zero fails closed until the embedding runtime supplies a profile.
            lock_capacity: 0,
            file_descriptor_capacity,
            // Queue capacity is a runtime-owned topology fact, not an FD proxy.
            queue_capacity: 0,
        };
        let pressure = ExternalPressure {
            memory_available_bytes: BTreeMap::from([(
                host_domain,
                available_memory_bytes.min(physical_memory_bytes),
            )]),
            available_cpu_threads: logical_cpu_threads,
            storage_available_bytes: storage_available_bytes.min(storage_capacity_bytes),
            cache_available_bytes: available_memory_bytes.min(physical_memory_bytes),
            available_locks: 0,
            available_file_descriptors: file_descriptor_capacity,
            available_queue_slots: 0,
        };
        Ok(Self { topology, pressure })
    }
}

/// Required and preferred count for one resource category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CountDemand {
    hard: u64,
    preferred: u64,
}

impl CountDemand {
    /// Creates a hard ceiling and a preferred target at or below it.
    pub const fn new(hard: u64, preferred: u64) -> Self {
        Self { hard, preferred }
    }

    /// Creates an empty demand.
    pub const fn zero() -> Self {
        Self::new(0, 0)
    }

    /// Returns the hard count ceiling that admission must guarantee.
    pub const fn hard(self) -> u64 {
        self.hard
    }

    /// Returns the preferred operating count.
    pub const fn preferred(self) -> u64 {
        self.preferred
    }
}

/// One logical resident allocation and all physical views that use it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryDemand {
    /// Stable logical allocation identity.
    pub allocation_id: String,
    /// Hard resident-byte ceiling that admission must guarantee.
    pub hard_bytes: u64,
    /// Preferred resident bytes at or below the hard ceiling.
    pub preferred_bytes: u64,
    /// Host or accelerator views through which the allocation is accessed.
    pub views: Vec<CapacityViewId>,
}

/// Explicit runtime and external-library resident-memory envelopes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeOverheadDemand {
    /// Worker thread-stack bytes.
    pub thread_stack_bytes: u64,
    /// Allocator fragmentation and bookkeeping bytes.
    pub allocator_fragmentation_bytes: u64,
    /// External-library workspace bytes.
    pub external_library_bytes: u64,
    /// FFT plan and workspace bytes.
    pub fft_workspace_bytes: u64,
    /// Accelerator driver-owned bytes attributable to the run.
    pub driver_bytes: u64,
    /// JIT compiler and generated-code bytes.
    pub jit_bytes: u64,
    /// Accelerator command-buffer bytes.
    pub command_buffer_bytes: u64,
}

impl RuntimeOverheadDemand {
    /// Creates an envelope with no runtime overhead.
    pub const fn zero() -> Self {
        Self {
            thread_stack_bytes: 0,
            allocator_fragmentation_bytes: 0,
            external_library_bytes: 0,
            fft_workspace_bytes: 0,
            driver_bytes: 0,
            jit_bytes: 0,
            command_buffer_bytes: 0,
        }
    }

    fn checked_total(self) -> Result<u64, ResourceError> {
        checked_sum(
            [
                self.thread_stack_bytes,
                self.allocator_fragmentation_bytes,
                self.external_library_bytes,
                self.fft_workspace_bytes,
                self.driver_bytes,
                self.jit_bytes,
                self.command_buffer_bytes,
            ],
            "runtime overhead",
        )
    }
}

/// Every bounded I/O buffer category attributable to one run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoBufferDemand {
    /// Source read-ahead buffers.
    pub source_read_ahead_bytes: u64,
    /// Decode buffers.
    pub decode_bytes: u64,
    /// Visibility or product preparation buffers.
    pub preparation_bytes: u64,
    /// Host-to-device transfer buffers.
    pub host_to_device_transfer_bytes: u64,
    /// Device-to-host transfer buffers.
    pub device_to_host_transfer_bytes: u64,
    /// Spill-read buffers.
    pub spill_read_bytes: u64,
    /// Spill-write buffers.
    pub spill_write_bytes: u64,
    /// Protocol or product serialization buffers.
    pub serialization_bytes: u64,
    /// Generic storage-manager buffers not owned by a column writer.
    pub storage_manager_bytes: u64,
    /// Sequential tiled-column writer buffers.
    pub tiled_column_writer_bytes: u64,
    /// Scalar-column writer buffers.
    pub scalar_column_writer_bytes: u64,
    /// Storage-manager and product writeback buffers.
    pub writeback_bytes: u64,
    /// Staged publication buffers.
    pub publication_bytes: u64,
    /// Mapped-file and page-cache resident exposure.
    pub mapped_page_cache_bytes: u64,
}

impl IoBufferDemand {
    /// Creates an envelope with no I/O buffers.
    pub const fn zero() -> Self {
        Self {
            source_read_ahead_bytes: 0,
            decode_bytes: 0,
            preparation_bytes: 0,
            host_to_device_transfer_bytes: 0,
            device_to_host_transfer_bytes: 0,
            spill_read_bytes: 0,
            spill_write_bytes: 0,
            serialization_bytes: 0,
            storage_manager_bytes: 0,
            tiled_column_writer_bytes: 0,
            scalar_column_writer_bytes: 0,
            writeback_bytes: 0,
            publication_bytes: 0,
            mapped_page_cache_bytes: 0,
        }
    }

    fn checked_total(self) -> Result<u64, ResourceError> {
        checked_sum(
            [
                self.source_read_ahead_bytes,
                self.decode_bytes,
                self.preparation_bytes,
                self.host_to_device_transfer_bytes,
                self.device_to_host_transfer_bytes,
                self.spill_read_bytes,
                self.spill_write_bytes,
                self.serialization_bytes,
                self.storage_manager_bytes,
                self.tiled_column_writer_bytes,
                self.scalar_column_writer_bytes,
                self.writeback_bytes,
                self.publication_bytes,
                self.mapped_page_cache_bytes,
            ],
            "I/O buffers",
        )
    }
}

/// Bounded queue depths attributable to one run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueDemand {
    /// Source read-ahead queue slots.
    pub source_read_ahead_slots: u64,
    /// Decode and preparation queue slots.
    pub preparation_slots: u64,
    /// Accelerator command queue slots.
    pub device_command_slots: u64,
    /// Transfer queue slots.
    pub transfer_slots: u64,
    /// Spill queue slots.
    pub spill_slots: u64,
    /// Writeback and publication queue slots.
    pub writeback_slots: u64,
}

impl QueueDemand {
    /// Creates an envelope with no queue slots.
    pub const fn zero() -> Self {
        Self {
            source_read_ahead_slots: 0,
            preparation_slots: 0,
            device_command_slots: 0,
            transfer_slots: 0,
            spill_slots: 0,
            writeback_slots: 0,
        }
    }

    fn checked_total(self) -> Result<u64, ResourceError> {
        checked_sum(
            [
                self.source_read_ahead_slots,
                self.preparation_slots,
                self.device_command_slots,
                self.transfer_slots,
                self.spill_slots,
                self.writeback_slots,
            ],
            "queue slots",
        )
    }
}

/// Resident and persistent cache demand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheDemand {
    /// Hard resident-cache ceiling that admission must guarantee.
    pub hard_resident_bytes: u64,
    /// Preferred resident cache bytes.
    pub preferred_resident_bytes: u64,
}

impl CacheDemand {
    /// Creates an empty cache demand.
    pub const fn zero() -> Self {
        Self {
            hard_resident_bytes: 0,
            preferred_resident_bytes: 0,
        }
    }
}

/// Storage-capacity and shared-rate demand.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageDemand {
    /// Mandatory temporary spill storage.
    pub temporary_bytes: u64,
    /// Mandatory staged-output storage.
    pub staged_output_bytes: u64,
    /// Mandatory final-output storage.
    pub final_output_bytes: u64,
    /// Mandatory persistent-cache storage.
    pub persistent_cache_bytes: u64,
    /// Mandatory sequential read rate.
    pub read_bytes_per_second: u64,
    /// Mandatory sequential write rate.
    pub write_bytes_per_second: u64,
}

impl StorageDemand {
    /// Creates an envelope with no storage demand.
    pub const fn zero() -> Self {
        Self {
            temporary_bytes: 0,
            staged_output_bytes: 0,
            final_output_bytes: 0,
            persistent_cache_bytes: 0,
            read_bytes_per_second: 0,
            write_bytes_per_second: 0,
        }
    }

    fn checked_capacity(self) -> Result<u64, ResourceError> {
        checked_sum(
            [
                self.temporary_bytes,
                self.staged_output_bytes,
                self.final_output_bytes,
                self.persistent_cache_bytes,
            ],
            "storage capacity",
        )
    }
}

/// Complete declarative resource demand for one implementation alternative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DemandEnvelope {
    /// Host-memory view charged for runtime overhead, caches, and I/O buffers.
    pub host_memory_view: CapacityViewId,
    /// Logical resident allocations.
    pub memory: Vec<MemoryDemand>,
    /// Worker demand.
    pub workers: CountDemand,
    /// Runtime, allocator, library, FFT, driver, JIT, and command-buffer demand.
    pub overhead: RuntimeOverheadDemand,
    /// Temporary, output, persistent-cache, and rate demand.
    pub storage: StorageDemand,
    /// Resident-cache demand.
    pub caches: CacheDemand,
    /// Table and synchronization lock demand.
    pub locks: CountDemand,
    /// File-descriptor demand.
    pub file_descriptors: CountDemand,
    /// Bounded queue-depth demand.
    pub queues: QueueDemand,
    /// Every I/O buffer category owned by the run.
    pub io_buffers: IoBufferDemand,
}

/// User-selected host-use policy; detection remains inside the authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourcePolicy {
    /// Preserve responsiveness by preferring a conservative share.
    Interactive,
    /// Prefer performance cores and most currently available memory.
    Balanced,
    /// Prefer all currently available process resources.
    Exclusive,
    /// Apply explicit ceilings without inventing unavailable capacity.
    Explicit(ResourceOverride),
}

/// Explicit resource ceilings applied within detected physical limits.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceOverride {
    /// Optional per-domain memory ceilings.
    pub memory_bytes: BTreeMap<CapacityDomainId, u64>,
    /// Optional worker ceiling.
    pub workers: Option<u64>,
    /// Optional storage-capacity ceiling.
    pub storage_bytes: Option<u64>,
    /// Optional storage read-rate ceiling.
    pub storage_read_bytes_per_second: Option<u64>,
    /// Optional storage write-rate ceiling.
    pub storage_write_bytes_per_second: Option<u64>,
    /// Optional resident-cache ceiling.
    pub cache_bytes: Option<u64>,
    /// Optional lock ceiling.
    pub locks: Option<u64>,
    /// Optional file-descriptor ceiling.
    pub file_descriptors: Option<u64>,
    /// Optional queue-slot ceiling.
    pub queue_slots: Option<u64>,
}

/// Hard ceilings or preferred targets granted to one run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceGrant {
    memory_bytes: BTreeMap<CapacityDomainId, u64>,
    workers: u64,
    storage_bytes: u64,
    storage_read_bytes_per_second: u64,
    storage_write_bytes_per_second: u64,
    cache_bytes: u64,
    locks: u64,
    file_descriptors: u64,
    queue_slots: u64,
}

impl ResourceGrant {
    /// Returns granted bytes in one physical memory domain.
    pub fn memory_bytes(&self, domain: &CapacityDomainId) -> u64 {
        self.memory_bytes.get(domain).copied().unwrap_or(0)
    }

    /// Returns the granted worker count.
    pub const fn workers(&self) -> u64 {
        self.workers
    }

    /// Returns the granted storage capacity.
    pub const fn storage_bytes(&self) -> u64 {
        self.storage_bytes
    }

    /// Returns the granted storage read rate.
    pub const fn storage_read_bytes_per_second(&self) -> u64 {
        self.storage_read_bytes_per_second
    }

    /// Returns the granted storage write rate.
    pub const fn storage_write_bytes_per_second(&self) -> u64 {
        self.storage_write_bytes_per_second
    }

    /// Returns the granted resident-cache bytes.
    pub const fn cache_bytes(&self) -> u64 {
        self.cache_bytes
    }

    /// Returns the granted lock count.
    pub const fn locks(&self) -> u64 {
        self.locks
    }

    /// Returns the granted file-descriptor count.
    pub const fn file_descriptors(&self) -> u64 {
        self.file_descriptors
    }

    /// Returns the granted total queue slots.
    pub const fn queue_slots(&self) -> u64 {
        self.queue_slots
    }
}

/// Resource inventory or admission failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceError {
    /// An inventory or demand violated a structural invariant.
    Invalid(String),
    /// Checked resource arithmetic overflowed.
    Overflow(&'static str),
    /// Production host detection failed.
    Detection(String),
    /// A mandatory demand did not fit the current hard ceiling.
    Infeasible {
        /// Resource category that failed admission.
        resource: String,
        /// Mandatory requested amount.
        required: u64,
        /// Amount available after policy, pressure, and active leases.
        available: u64,
    },
    /// The process-wide authority lock was poisoned.
    AuthorityPoisoned,
}

impl ResourceError {
    /// Returns the mandatory amount for an infeasibility error.
    pub const fn required(&self) -> Option<u64> {
        match self {
            Self::Infeasible { required, .. } => Some(*required),
            _ => None,
        }
    }

    /// Returns the available amount for an infeasibility error.
    pub const fn available(&self) -> Option<u64> {
        match self {
            Self::Infeasible { available, .. } => Some(*available),
            _ => None,
        }
    }
}

impl fmt::Display for ResourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "invalid resource declaration: {message}"),
            Self::Overflow(category) => write!(formatter, "{category} arithmetic overflowed"),
            Self::Detection(message) => write!(formatter, "resource detection failed: {message}"),
            Self::Infeasible {
                resource,
                required,
                available,
            } => write!(
                formatter,
                "resource {resource} requires {required}, but only {available} is available"
            ),
            Self::AuthorityPoisoned => formatter.write_str("resource authority lock is poisoned"),
        }
    }
}

impl Error for ResourceError {}

#[derive(Clone, Debug, Default)]
struct ResourceTotals {
    hard: ResourceGrant,
    preferred: ResourceGrant,
}

#[derive(Clone, Debug)]
struct LeaseRecord {
    hard: ResourceGrant,
    outstanding_fences: u64,
    release_requested: bool,
}

#[derive(Debug)]
struct AuthorityState {
    pressure: ExternalPressure,
    leases: BTreeMap<u64, LeaseRecord>,
    next_lease_id: u64,
    epoch: u64,
}

#[derive(Debug)]
struct AuthorityInner {
    topology: ResourceTopology,
    state: Mutex<AuthorityState>,
}

/// Process-synchronized owner of inventory, admission, and active leases.
#[derive(Clone, Debug)]
pub struct ResourceAuthority {
    inner: Arc<AuthorityInner>,
}

impl ResourceAuthority {
    /// Returns the single lazily detected authority shared by this process.
    pub fn production() -> Result<&'static Self, ResourceError> {
        static PRODUCTION: OnceLock<Result<ResourceAuthority, ResourceError>> = OnceLock::new();
        match PRODUCTION.get_or_init(|| HostInventory::detect().and_then(Self::with_inventory)) {
            Ok(authority) => Ok(authority),
            Err(error) => Err(error.clone()),
        }
    }

    /// Creates an isolated authority from an injected deterministic inventory.
    pub fn with_inventory(inventory: HostInventory) -> Result<Self, ResourceError> {
        validate_inventory(&inventory)?;
        Ok(Self {
            inner: Arc::new(AuthorityInner {
                topology: inventory.topology,
                state: Mutex::new(AuthorityState {
                    pressure: inventory.pressure,
                    leases: BTreeMap::new(),
                    next_lease_id: 1,
                    epoch: 0,
                }),
            }),
        })
    }

    /// Atomically admits and reserves one complete demand envelope.
    pub fn acquire(
        &self,
        policy: ResourcePolicy,
        demand: DemandEnvelope,
    ) -> Result<ResourceLease, ResourceError> {
        validate_policy(&self.inner.topology, &policy)?;
        let totals = demand.resource_totals(&self.inner.topology)?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        let available = available_after_active_leases(&self.inner.topology, &state)?;
        let policy_available = apply_policy(&self.inner.topology, &policy, available);
        let granted = admit_totals(&totals, &policy_available)?;
        let lease_id = state.next_lease_id;
        state.next_lease_id = state
            .next_lease_id
            .checked_add(1)
            .ok_or(ResourceError::Overflow("lease identity"))?;
        state.epoch = state
            .epoch
            .checked_add(1)
            .ok_or(ResourceError::Overflow("resource epoch"))?;
        let epoch = state.epoch;
        state.leases.insert(
            lease_id,
            LeaseRecord {
                hard: granted.hard.clone(),
                outstanding_fences: 0,
                release_requested: false,
            },
        );
        Ok(ResourceLease {
            inner: Arc::clone(&self.inner),
            lease_id,
            epoch,
            hard: granted.hard,
            preferred: granted.preferred,
            release_requested: false,
        })
    }

    /// Replaces the external-pressure snapshot used for subsequent admissions.
    pub fn update_external_pressure(
        &self,
        pressure: ExternalPressure,
    ) -> Result<(), ResourceError> {
        validate_pressure(&self.inner.topology, &pressure)?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        state.pressure = pressure;
        state.epoch = state
            .epoch
            .checked_add(1)
            .ok_or(ResourceError::Overflow("resource epoch"))?;
        Ok(())
    }
}

fn validate_policy(
    topology: &ResourceTopology,
    policy: &ResourcePolicy,
) -> Result<(), ResourceError> {
    let ResourcePolicy::Explicit(overrides) = policy else {
        return Ok(());
    };
    let domains = topology
        .memory_domains
        .iter()
        .map(|domain| &domain.id)
        .collect::<BTreeSet<_>>();
    if let Some(domain) = overrides
        .memory_bytes
        .keys()
        .find(|domain| !domains.contains(domain))
    {
        return Err(ResourceError::Invalid(format!(
            "explicit policy references unknown memory domain {}",
            domain.as_str()
        )));
    }
    Ok(())
}

/// Result of requesting lease release.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseRelease {
    released: bool,
}

impl LeaseRelease {
    /// Returns true when all reservations were released immediately.
    pub const fn is_released(self) -> bool {
        self.released
    }
}

/// Epoch-bearing resource grant held until release and fence completion.
#[derive(Debug)]
pub struct ResourceLease {
    inner: Arc<AuthorityInner>,
    lease_id: u64,
    epoch: u64,
    hard: ResourceGrant,
    preferred: ResourceGrant,
    release_requested: bool,
}

impl ResourceLease {
    /// Returns the monotonically increasing authority epoch of this grant.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the lease's enforceable hard ceilings.
    pub const fn hard_ceilings(&self) -> &ResourceGrant {
        &self.hard
    }

    /// Returns the lease's preferred operating targets.
    pub const fn preferred_targets(&self) -> &ResourceGrant {
        &self.preferred
    }

    /// Registers asynchronous work whose completion gates capacity reuse.
    pub fn register_fence(&self) -> Result<ResourceFence, ResourceError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        let record = state.leases.get_mut(&self.lease_id).ok_or_else(|| {
            ResourceError::Invalid("cannot fence a released resource lease".to_string())
        })?;
        if record.release_requested {
            return Err(ResourceError::Invalid(
                "cannot register a fence after release was requested".to_string(),
            ));
        }
        record.outstanding_fences = record
            .outstanding_fences
            .checked_add(1)
            .ok_or(ResourceError::Overflow("resource fences"))?;
        Ok(ResourceFence {
            inner: Arc::clone(&self.inner),
            lease_id: self.lease_id,
            completed: false,
        })
    }

    /// Requests release; reservations remain until all registered fences complete.
    pub fn release(mut self) -> Result<LeaseRelease, ResourceError> {
        self.release_requested = true;
        let released = request_release(&self.inner, self.lease_id)?;
        Ok(LeaseRelease { released })
    }
}

impl Drop for ResourceLease {
    fn drop(&mut self) {
        if !self.release_requested {
            let _ = request_release(&self.inner, self.lease_id);
        }
    }
}

/// Explicit completion token for device, I/O, writeback, or publication work.
///
/// Dropping an incomplete fence intentionally retains the lease reservation. A
/// scheduler must call [`Self::complete`] only after the corresponding work is
/// known to be quiescent.
#[derive(Debug)]
pub struct ResourceFence {
    inner: Arc<AuthorityInner>,
    lease_id: u64,
    completed: bool,
}

impl ResourceFence {
    /// Marks the fenced work complete and releases a pending lease when this is
    /// its final outstanding fence.
    pub fn complete(mut self) -> Result<LeaseRelease, ResourceError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        let record = state.leases.get_mut(&self.lease_id).ok_or_else(|| {
            ResourceError::Invalid("cannot complete a fence for an absent lease".to_string())
        })?;
        record.outstanding_fences = record.outstanding_fences.checked_sub(1).ok_or_else(|| {
            ResourceError::Invalid("resource fence count underflowed".to_string())
        })?;
        let released = record.release_requested && record.outstanding_fences == 0;
        if released {
            state.leases.remove(&self.lease_id);
        }
        self.completed = true;
        Ok(LeaseRelease { released })
    }
}

fn request_release(inner: &AuthorityInner, lease_id: u64) -> Result<bool, ResourceError> {
    let mut state = inner
        .state
        .lock()
        .map_err(|_| ResourceError::AuthorityPoisoned)?;
    let Some(record) = state.leases.get_mut(&lease_id) else {
        return Ok(true);
    };
    record.release_requested = true;
    if record.outstanding_fences == 0 {
        state.leases.remove(&lease_id);
        Ok(true)
    } else {
        Ok(false)
    }
}

#[derive(Clone, Debug)]
struct GrantedTotals {
    hard: ResourceGrant,
    preferred: ResourceGrant,
}

#[derive(Clone, Debug)]
struct PolicyAvailability {
    hard: ResourceGrant,
    preferred: ResourceGrant,
}

fn admit_totals(
    totals: &ResourceTotals,
    available: &PolicyAvailability,
) -> Result<GrantedTotals, ResourceError> {
    let hard = totals.hard.clone();
    let mut preferred = totals.preferred.clone();
    for (domain, required) in &totals.hard.memory_bytes {
        let domain_available = available.hard.memory_bytes(domain);
        require_fit(
            format!("memory-domain:{}", domain.as_str()),
            *required,
            domain_available,
        )?;
        let desired = totals.preferred.memory_bytes(domain);
        let target = desired.min(available.preferred.memory_bytes(domain));
        preferred.memory_bytes.insert(domain.clone(), target);
    }
    require_scalar("workers", totals.hard.workers, available.hard.workers)?;
    preferred.workers = totals.preferred.workers.min(available.preferred.workers);
    require_scalar(
        "storage-bytes",
        totals.hard.storage_bytes,
        available.hard.storage_bytes,
    )?;
    preferred.storage_bytes = totals
        .preferred
        .storage_bytes
        .min(available.preferred.storage_bytes);
    require_scalar(
        "storage-read-rate",
        totals.hard.storage_read_bytes_per_second,
        available.hard.storage_read_bytes_per_second,
    )?;
    preferred.storage_read_bytes_per_second = totals
        .preferred
        .storage_read_bytes_per_second
        .min(available.preferred.storage_read_bytes_per_second);
    require_scalar(
        "storage-write-rate",
        totals.hard.storage_write_bytes_per_second,
        available.hard.storage_write_bytes_per_second,
    )?;
    preferred.storage_write_bytes_per_second = totals
        .preferred
        .storage_write_bytes_per_second
        .min(available.preferred.storage_write_bytes_per_second);
    require_scalar(
        "cache-bytes",
        totals.hard.cache_bytes,
        available.hard.cache_bytes,
    )?;
    preferred.cache_bytes = totals
        .preferred
        .cache_bytes
        .min(available.preferred.cache_bytes);
    require_scalar("locks", totals.hard.locks, available.hard.locks)?;
    preferred.locks = totals.preferred.locks.min(available.preferred.locks);
    require_scalar(
        "file-descriptors",
        totals.hard.file_descriptors,
        available.hard.file_descriptors,
    )?;
    preferred.file_descriptors = totals
        .preferred
        .file_descriptors
        .min(available.preferred.file_descriptors);
    require_scalar(
        "queue-slots",
        totals.hard.queue_slots,
        available.hard.queue_slots,
    )?;
    preferred.queue_slots = totals
        .preferred
        .queue_slots
        .min(available.preferred.queue_slots);
    Ok(GrantedTotals { hard, preferred })
}

fn require_scalar(name: &str, required: u64, available: u64) -> Result<(), ResourceError> {
    require_fit(name.to_string(), required, available)
}

fn require_fit(resource: String, required: u64, available: u64) -> Result<(), ResourceError> {
    if required > available {
        return Err(ResourceError::Infeasible {
            resource,
            required,
            available,
        });
    }
    Ok(())
}

fn apply_policy(
    topology: &ResourceTopology,
    policy: &ResourcePolicy,
    mut hard: ResourceGrant,
) -> PolicyAvailability {
    let mut preferred = hard.clone();
    match policy {
        ResourcePolicy::Interactive => {
            for bytes in preferred.memory_bytes.values_mut() {
                *bytes /= 2;
            }
            preferred.workers = preferred
                .workers
                .min(topology.performance_cpu_cores.div_ceil(2).max(1));
        }
        ResourcePolicy::Balanced => {
            for bytes in preferred.memory_bytes.values_mut() {
                *bytes = bytes.saturating_sub(*bytes / 4);
            }
            preferred.workers = preferred.workers.min(topology.performance_cpu_cores.max(1));
        }
        ResourcePolicy::Exclusive => {
            preferred.workers = preferred.workers.min(topology.logical_cpu_threads);
        }
        ResourcePolicy::Explicit(overrides) => {
            for (domain, ceiling) in &overrides.memory_bytes {
                if let Some(available_bytes) = hard.memory_bytes.get_mut(domain) {
                    *available_bytes = (*available_bytes).min(*ceiling);
                }
            }
            cap_optional(&mut hard.workers, overrides.workers);
            cap_optional(&mut hard.storage_bytes, overrides.storage_bytes);
            cap_optional(
                &mut hard.storage_read_bytes_per_second,
                overrides.storage_read_bytes_per_second,
            );
            cap_optional(
                &mut hard.storage_write_bytes_per_second,
                overrides.storage_write_bytes_per_second,
            );
            cap_optional(&mut hard.cache_bytes, overrides.cache_bytes);
            cap_optional(&mut hard.locks, overrides.locks);
            cap_optional(&mut hard.file_descriptors, overrides.file_descriptors);
            cap_optional(&mut hard.queue_slots, overrides.queue_slots);
            preferred = hard.clone();
        }
    }
    PolicyAvailability { hard, preferred }
}

fn cap_optional(value: &mut u64, cap: Option<u64>) {
    if let Some(cap) = cap {
        *value = (*value).min(cap);
    }
}

fn available_after_active_leases(
    topology: &ResourceTopology,
    state: &AuthorityState,
) -> Result<ResourceGrant, ResourceError> {
    let mut reserved = ResourceGrant::default();
    for record in state.leases.values() {
        add_grant(&mut reserved, &record.hard)?;
    }
    let memory_bytes = state
        .pressure
        .memory_available_bytes
        .iter()
        .map(|(domain, available)| {
            (
                domain.clone(),
                available.saturating_sub(reserved.memory_bytes(domain)),
            )
        })
        .collect();
    Ok(ResourceGrant {
        memory_bytes,
        workers: state
            .pressure
            .available_cpu_threads
            .min(topology.logical_cpu_threads)
            .saturating_sub(reserved.workers),
        storage_bytes: state
            .pressure
            .storage_available_bytes
            .saturating_sub(reserved.storage_bytes),
        storage_read_bytes_per_second: topology
            .storage_read_bytes_per_second
            .saturating_sub(reserved.storage_read_bytes_per_second),
        storage_write_bytes_per_second: topology
            .storage_write_bytes_per_second
            .saturating_sub(reserved.storage_write_bytes_per_second),
        cache_bytes: state
            .pressure
            .cache_available_bytes
            .saturating_sub(reserved.cache_bytes),
        locks: state
            .pressure
            .available_locks
            .saturating_sub(reserved.locks),
        file_descriptors: state
            .pressure
            .available_file_descriptors
            .saturating_sub(reserved.file_descriptors),
        queue_slots: state
            .pressure
            .available_queue_slots
            .saturating_sub(reserved.queue_slots),
    })
}

fn add_grant(total: &mut ResourceGrant, value: &ResourceGrant) -> Result<(), ResourceError> {
    for (domain, bytes) in &value.memory_bytes {
        let current = total.memory_bytes.entry(domain.clone()).or_default();
        *current = current
            .checked_add(*bytes)
            .ok_or(ResourceError::Overflow("reserved memory"))?;
    }
    total.workers = checked_add(total.workers, value.workers, "reserved workers")?;
    total.storage_bytes =
        checked_add(total.storage_bytes, value.storage_bytes, "reserved storage")?;
    total.storage_read_bytes_per_second = checked_add(
        total.storage_read_bytes_per_second,
        value.storage_read_bytes_per_second,
        "reserved storage read rate",
    )?;
    total.storage_write_bytes_per_second = checked_add(
        total.storage_write_bytes_per_second,
        value.storage_write_bytes_per_second,
        "reserved storage write rate",
    )?;
    total.cache_bytes = checked_add(total.cache_bytes, value.cache_bytes, "reserved cache")?;
    total.locks = checked_add(total.locks, value.locks, "reserved locks")?;
    total.file_descriptors = checked_add(
        total.file_descriptors,
        value.file_descriptors,
        "reserved file descriptors",
    )?;
    total.queue_slots = checked_add(total.queue_slots, value.queue_slots, "reserved queue slots")?;
    Ok(())
}

impl DemandEnvelope {
    fn resource_totals(
        &self,
        topology: &ResourceTopology,
    ) -> Result<ResourceTotals, ResourceError> {
        validate_count("workers", self.workers)?;
        validate_count("locks", self.locks)?;
        validate_count("file descriptors", self.file_descriptors)?;
        if self.caches.preferred_resident_bytes > self.caches.hard_resident_bytes {
            return Err(ResourceError::Invalid(
                "preferred cache bytes cannot exceed the hard ceiling".to_string(),
            ));
        }
        let views = topology
            .memory_views
            .iter()
            .map(|view| (&view.id, &view.domain))
            .collect::<BTreeMap<_, _>>();
        let host_domain = views.get(&self.host_memory_view).ok_or_else(|| {
            ResourceError::Invalid(format!(
                "host-memory view {} is absent from topology",
                self.host_memory_view.as_str()
            ))
        })?;
        let mut hard_memory = BTreeMap::<CapacityDomainId, u64>::new();
        let mut preferred_memory = BTreeMap::<CapacityDomainId, u64>::new();
        let mut allocation_ids = BTreeSet::new();
        for allocation in &self.memory {
            if allocation.allocation_id.is_empty()
                || !allocation_ids.insert(&allocation.allocation_id)
            {
                return Err(ResourceError::Invalid(
                    "memory allocation identities must be non-empty and unique".to_string(),
                ));
            }
            if allocation.preferred_bytes > allocation.hard_bytes {
                return Err(ResourceError::Invalid(format!(
                    "memory allocation {} prefers more than its hard ceiling",
                    allocation.allocation_id
                )));
            }
            if allocation.views.is_empty() {
                return Err(ResourceError::Invalid(format!(
                    "memory allocation {} has no capacity view",
                    allocation.allocation_id
                )));
            }
            let mut physical_domains = BTreeSet::new();
            for view in &allocation.views {
                let domain = views.get(view).ok_or_else(|| {
                    ResourceError::Invalid(format!(
                        "memory view {} is absent from topology",
                        view.as_str()
                    ))
                })?;
                physical_domains.insert((*domain).clone());
            }
            for domain in physical_domains {
                add_map_bytes(
                    &mut hard_memory,
                    domain.clone(),
                    allocation.hard_bytes,
                    "memory demand",
                )?;
                add_map_bytes(
                    &mut preferred_memory,
                    domain,
                    allocation.preferred_bytes,
                    "preferred memory demand",
                )?;
            }
        }
        let fixed_host_bytes = self
            .overhead
            .checked_total()?
            .checked_add(self.io_buffers.checked_total()?)
            .ok_or(ResourceError::Overflow("host-memory envelope"))?;
        add_map_bytes(
            &mut hard_memory,
            (*host_domain).clone(),
            fixed_host_bytes
                .checked_add(self.caches.hard_resident_bytes)
                .ok_or(ResourceError::Overflow("host-memory demand"))?,
            "host-memory demand",
        )?;
        add_map_bytes(
            &mut preferred_memory,
            (*host_domain).clone(),
            fixed_host_bytes
                .checked_add(self.caches.preferred_resident_bytes)
                .ok_or(ResourceError::Overflow("preferred host-memory demand"))?,
            "preferred host-memory demand",
        )?;
        let storage_bytes = self.storage.checked_capacity()?;
        let queue_slots = self.queues.checked_total()?;
        Ok(ResourceTotals {
            hard: ResourceGrant {
                memory_bytes: hard_memory,
                workers: self.workers.hard,
                storage_bytes,
                storage_read_bytes_per_second: self.storage.read_bytes_per_second,
                storage_write_bytes_per_second: self.storage.write_bytes_per_second,
                cache_bytes: self.caches.hard_resident_bytes,
                locks: self.locks.hard,
                file_descriptors: self.file_descriptors.hard,
                queue_slots,
            },
            preferred: ResourceGrant {
                memory_bytes: preferred_memory,
                workers: self.workers.preferred,
                storage_bytes,
                storage_read_bytes_per_second: self.storage.read_bytes_per_second,
                storage_write_bytes_per_second: self.storage.write_bytes_per_second,
                cache_bytes: self.caches.preferred_resident_bytes,
                locks: self.locks.preferred,
                file_descriptors: self.file_descriptors.preferred,
                queue_slots,
            },
        })
    }
}

fn add_map_bytes(
    values: &mut BTreeMap<CapacityDomainId, u64>,
    domain: CapacityDomainId,
    bytes: u64,
    category: &'static str,
) -> Result<(), ResourceError> {
    let current = values.entry(domain).or_default();
    *current = current
        .checked_add(bytes)
        .ok_or(ResourceError::Overflow(category))?;
    Ok(())
}

fn validate_count(name: &str, demand: CountDemand) -> Result<(), ResourceError> {
    if demand.preferred > demand.hard {
        return Err(ResourceError::Invalid(format!(
            "preferred {name} cannot exceed the hard ceiling"
        )));
    }
    Ok(())
}

fn validate_inventory(inventory: &HostInventory) -> Result<(), ResourceError> {
    let topology = &inventory.topology;
    if topology.logical_cpu_threads == 0 {
        return Err(ResourceError::Invalid(
            "logical CPU topology must be nonzero".to_string(),
        ));
    }
    if topology.performance_cpu_cores == 0
        || topology.performance_cpu_cores > topology.logical_cpu_threads
    {
        return Err(ResourceError::Invalid(
            "performance CPU topology must be within logical CPU capacity".to_string(),
        ));
    }
    let mut domains = BTreeMap::new();
    for domain in &topology.memory_domains {
        if domain.id.as_str().is_empty()
            || domain.capacity_bytes == 0
            || domains.insert(&domain.id, domain.capacity_bytes).is_some()
        {
            return Err(ResourceError::Invalid(
                "memory domains must have unique non-empty identities and positive capacity"
                    .to_string(),
            ));
        }
    }
    let mut views = BTreeSet::new();
    for view in &topology.memory_views {
        if view.id.as_str().is_empty()
            || !views.insert(&view.id)
            || !domains.contains_key(&view.domain)
        {
            return Err(ResourceError::Invalid(
                "memory views must be unique and reference physical domains".to_string(),
            ));
        }
    }
    validate_pressure(topology, &inventory.pressure)
}

fn validate_pressure(
    topology: &ResourceTopology,
    pressure: &ExternalPressure,
) -> Result<(), ResourceError> {
    let domains = topology
        .memory_domains
        .iter()
        .map(|domain| (&domain.id, domain.capacity_bytes))
        .collect::<BTreeMap<_, _>>();
    if pressure.memory_available_bytes.len() != domains.len() {
        return Err(ResourceError::Invalid(
            "external pressure must report every physical memory domain".to_string(),
        ));
    }
    for (domain, available) in &pressure.memory_available_bytes {
        let capacity = domains.get(domain).ok_or_else(|| {
            ResourceError::Invalid(format!(
                "external pressure references unknown memory domain {}",
                domain.as_str()
            ))
        })?;
        if available > capacity {
            return Err(ResourceError::Invalid(format!(
                "available memory for {} exceeds physical capacity",
                domain.as_str()
            )));
        }
    }
    let bounded_counts = [
        (
            pressure.available_cpu_threads,
            topology.logical_cpu_threads,
            "CPU pressure",
        ),
        (
            pressure.storage_available_bytes,
            topology.storage_capacity_bytes,
            "storage pressure",
        ),
        (
            pressure.cache_available_bytes,
            topology.cache_capacity_bytes,
            "cache pressure",
        ),
        (
            pressure.available_locks,
            topology.lock_capacity,
            "lock pressure",
        ),
        (
            pressure.available_file_descriptors,
            topology.file_descriptor_capacity,
            "file-descriptor pressure",
        ),
        (
            pressure.available_queue_slots,
            topology.queue_capacity,
            "queue pressure",
        ),
    ];
    for (available, capacity, name) in bounded_counts {
        if available > capacity {
            return Err(ResourceError::Invalid(format!(
                "{name} exceeds inventoried capacity"
            )));
        }
    }
    Ok(())
}

fn checked_sum(
    values: impl IntoIterator<Item = u64>,
    category: &'static str,
) -> Result<u64, ResourceError> {
    values.into_iter().try_fold(0u64, |total, value| {
        total
            .checked_add(value)
            .ok_or(ResourceError::Overflow(category))
    })
}

fn checked_add(left: u64, right: u64, category: &'static str) -> Result<u64, ResourceError> {
    left.checked_add(right)
        .ok_or(ResourceError::Overflow(category))
}

#[cfg(target_os = "linux")]
fn detect_host_memory() -> Result<(u64, u64), ResourceError> {
    let contents = std::fs::read_to_string("/proc/meminfo")
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    let kib = |name: &str| {
        contents.lines().find_map(|line| {
            let value = line.strip_prefix(name)?;
            value.split_ascii_whitespace().next()?.parse::<u64>().ok()
        })
    };
    let total = kib("MemTotal:").ok_or_else(|| {
        ResourceError::Detection("/proc/meminfo has no MemTotal entry".to_string())
    })?;
    let available = kib("MemAvailable:").ok_or_else(|| {
        ResourceError::Detection("/proc/meminfo has no MemAvailable entry".to_string())
    })?;
    Ok((
        total
            .checked_mul(1024)
            .ok_or(ResourceError::Overflow("physical memory detection"))?,
        available
            .checked_mul(1024)
            .ok_or(ResourceError::Overflow("available memory detection"))?,
    ))
}

#[cfg(target_os = "macos")]
fn detect_host_memory() -> Result<(u64, u64), ResourceError> {
    let physical = command_u64("/usr/sbin/sysctl", &["-n", "hw.memsize"])
        .or_else(|_| detect_macos_physical_memory_from_system_profiler())?;
    let output = Command::new("/usr/bin/vm_stat")
        .output()
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    if !output.status.success() {
        return Err(ResourceError::Detection(
            "vm_stat did not complete successfully".to_string(),
        ));
    }
    let text = String::from_utf8(output.stdout)
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    let page_size = text
        .lines()
        .next()
        .and_then(|line| line.split("page size of ").nth(1))
        .and_then(|value| value.split_ascii_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| ResourceError::Detection("vm_stat page size is unavailable".to_string()))?;
    let pages = ["Pages free", "Pages inactive", "Pages speculative"]
        .into_iter()
        .try_fold(0u64, |total, name| {
            let pages = text
                .lines()
                .find_map(|line| {
                    let value = line.strip_prefix(name)?.strip_prefix(':')?;
                    value.trim().trim_end_matches('.').parse::<u64>().ok()
                })
                .unwrap_or(0);
            total
                .checked_add(pages)
                .ok_or(ResourceError::Overflow("available page detection"))
        })?;
    let available = pages
        .checked_mul(page_size)
        .ok_or(ResourceError::Overflow("available memory detection"))?;
    Ok((physical, available))
}

#[cfg(target_os = "macos")]
fn detect_macos_physical_memory_from_system_profiler() -> Result<u64, ResourceError> {
    let output = Command::new("/usr/sbin/system_profiler")
        .args(["SPHardwareDataType", "-json"])
        .output()
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    if !output.status.success() {
        return Err(ResourceError::Detection(
            "system_profiler hardware inventory did not complete successfully".to_string(),
        ));
    }
    let text = String::from_utf8(output.stdout)
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    let value = text
        .lines()
        .find(|line| line.contains("\"physical_memory\""))
        .and_then(|line| line.split_once(':'))
        .map(|(_, value)| {
            value
                .trim()
                .trim_matches(|character| matches!(character, '\"' | ',' | ' '))
        })
        .ok_or_else(|| {
            ResourceError::Detection(
                "system_profiler hardware inventory has no physical memory".to_string(),
            )
        })?;
    let mut parts = value.split_ascii_whitespace();
    let amount = parts
        .next()
        .and_then(|amount| amount.parse::<u64>().ok())
        .ok_or_else(|| ResourceError::Detection("invalid physical memory amount".to_string()))?;
    let multiplier = match parts.next() {
        Some("KB") => 1024,
        Some("MB") => 1024 * 1024,
        Some("GB") => 1024 * 1024 * 1024,
        Some("TB") => 1024u64.pow(4),
        _ => {
            return Err(ResourceError::Detection(
                "invalid physical memory unit".to_string(),
            ));
        }
    };
    amount
        .checked_mul(multiplier)
        .ok_or(ResourceError::Overflow("physical memory detection"))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn detect_host_memory() -> Result<(u64, u64), ResourceError> {
    Err(ResourceError::Detection(
        "physical-memory detection is unsupported on this platform".to_string(),
    ))
}

#[cfg(target_os = "macos")]
fn detect_performance_cpu_cores() -> Option<u64> {
    command_u64("/usr/sbin/sysctl", &["-n", "hw.perflevel0.physicalcpu"]).ok()
}

#[cfg(not(target_os = "macos"))]
fn detect_performance_cpu_cores() -> Option<u64> {
    None
}

#[cfg(target_os = "macos")]
fn detect_unified_metal_device() -> bool {
    let metal = Command::new("/usr/sbin/system_profiler")
        .args(["SPDisplaysDataType", "-json"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| String::from_utf8_lossy(&output.stdout).contains("spdisplays_metal"));
    if !metal {
        return false;
    }
    Command::new("/usr/sbin/system_profiler")
        .args(["SPHardwareDataType", "-json"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| String::from_utf8_lossy(&output.stdout).contains("\"chip_type\""))
}

#[cfg(not(target_os = "macos"))]
fn detect_unified_metal_device() -> bool {
    false
}

fn detect_storage_capacity() -> Result<(u64, u64), ResourceError> {
    let output = Command::new("df")
        .args(["-Pk", "."])
        .output()
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    if !output.status.success() {
        return Err(ResourceError::Detection(
            "df did not complete successfully".to_string(),
        ));
    }
    let text = String::from_utf8(output.stdout)
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    let fields = text
        .lines()
        .last()
        .ok_or_else(|| ResourceError::Detection("df returned no rows".to_string()))?
        .split_ascii_whitespace()
        .collect::<Vec<_>>();
    let parse_field = |index: usize, name: &str| {
        fields
            .get(index)
            .ok_or_else(|| ResourceError::Detection(format!("df has no {name} field")))?
            .parse::<u64>()
            .map_err(|error| ResourceError::Detection(error.to_string()))
    };
    let capacity_kib = parse_field(1, "capacity")?;
    let available_kib = parse_field(3, "available capacity")?;
    Ok((
        capacity_kib
            .checked_mul(1024)
            .ok_or(ResourceError::Overflow("storage capacity detection"))?,
        available_kib
            .checked_mul(1024)
            .ok_or(ResourceError::Overflow("storage availability detection"))?,
    ))
}

fn detect_open_file_limit() -> Result<u64, ResourceError> {
    let output = Command::new("getconf")
        .arg("OPEN_MAX")
        .output()
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    if !output.status.success() {
        return Err(ResourceError::Detection(
            "getconf OPEN_MAX did not complete successfully".to_string(),
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| ResourceError::Detection(error.to_string()))?
        .trim()
        .parse::<u64>()
        .map_err(|error| ResourceError::Detection(error.to_string()))
}

#[cfg(target_os = "macos")]
fn command_u64(program: &str, arguments: &[&str]) -> Result<u64, ResourceError> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .map_err(|error| ResourceError::Detection(error.to_string()))?;
    if !output.status.success() {
        return Err(ResourceError::Detection(format!(
            "{program} did not complete successfully"
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| ResourceError::Detection(error.to_string()))?
        .trim()
        .parse::<u64>()
        .map_err(|error| ResourceError::Detection(error.to_string()))
}
