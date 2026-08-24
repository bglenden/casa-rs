// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};

use crate::{ExecutionAttemptId, ReceiptStatus};

use casa_imaging_model::MeasurementSetIdentity;

static PRODUCTION_AUTHORITY: OnceLock<Result<ResourceAuthority, ResourceError>> = OnceLock::new();

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

macro_rules! resource_identity {
    ($name:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Creates a stable resource identity.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the identity text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

resource_identity!(AcceleratorId, "Stable identity of one accelerator.");
resource_identity!(TransferLinkId, "Stable identity of one transfer link.");
resource_identity!(StorageDomainId, "Stable identity of one storage domain.");
resource_identity!(RateResourceId, "Stable identity of one rate resource.");
resource_identity!(QueueResourceId, "Stable identity of one bounded queue.");
resource_identity!(AlternativeId, "Stable identity of one demand alternative.");
resource_identity!(CapabilityId, "Stable identity of one required capability.");
resource_identity!(
    ResourceIdentity,
    "Stable identity of one Resource Authority resource."
);

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

/// Knowledge available about performance-oriented CPU cores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CpuClassCapacity {
    /// The host reported a distinct performance-core count.
    Known(u64),
    /// The host did not expose a reliable CPU-class classification.
    Unknown,
}

/// Accelerator implementation class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceleratorKind {
    /// Apple Metal accelerator.
    Metal,
}

/// One typed accelerator in the process topology.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Accelerator {
    /// Stable accelerator identity.
    pub id: AcceleratorId,
    /// Accelerator implementation class.
    pub kind: AcceleratorKind,
    /// Memory view used by the accelerator.
    pub memory_view: CapacityViewId,
    /// Runtime-owned command queue.
    pub command_queue: QueueResourceId,
    /// Simultaneous occupancy slots.
    pub occupancy_slots: u64,
}

/// Unit carried by a typed rate resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RateUnit {
    /// Bytes transferred per second.
    BytesPerSecond,
    /// Discrete operations completed per second.
    OperationsPerSecond,
}

/// One explicitly measured or configured rate capacity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RateResource {
    /// Stable rate identity.
    pub id: RateResourceId,
    /// Physical unit measured by this resource.
    pub unit: RateUnit,
    /// Maximum units per second; zero means unprofiled and fails closed.
    pub units_per_second: u64,
}

impl RateResource {
    /// Creates a typed rate resource.
    pub const fn new(id: RateResourceId, unit: RateUnit, units_per_second: u64) -> Self {
        Self {
            id,
            unit,
            units_per_second,
        }
    }
}

/// One bounded runtime queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueResource {
    /// Stable queue identity.
    pub id: QueueResourceId,
    /// Maximum simultaneously occupied slots.
    pub slots: u64,
}

impl QueueResource {
    /// Creates a typed queue resource.
    pub const fn new(id: QueueResourceId, slots: u64) -> Self {
        Self { id, slots }
    }
}

/// One configured storage capacity rooted at a real filesystem domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageDomain {
    /// Stable storage-domain identity.
    pub id: StorageDomainId,
    /// Absolute root whose capacity this domain represents.
    pub root: PathBuf,
    /// Total configured capacity.
    pub capacity_bytes: u64,
    /// Rate resource used for reads from this domain.
    pub read_rate: RateResourceId,
    /// Rate resource used for writes to this domain.
    pub write_rate: RateResourceId,
    /// Optional operations-per-second resource for this domain.
    pub operations_rate: Option<RateResourceId>,
    /// Queue resource used for storage operations.
    pub queue: QueueResourceId,
}

/// One directed transfer path between memory views.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferLink {
    /// Stable transfer-link identity.
    pub id: TransferLinkId,
    /// Source memory view.
    pub source_view: CapacityViewId,
    /// Destination memory view.
    pub destination_view: CapacityViewId,
    /// Shared transfer-rate resource.
    pub rate: RateResourceId,
    /// Shared transfer-queue resource.
    pub queue: QueueResourceId,
}

/// Physical, rate, and count topology inventoried for one process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceTopology {
    /// Physical memory-capacity domains.
    pub memory_domains: Vec<MemoryCapacityDomain>,
    /// Host and accelerator views of those domains.
    pub memory_views: Vec<MemoryView>,
    /// Typed accelerators visible to the runtime.
    pub accelerators: Vec<Accelerator>,
    /// Directed transfer paths between memory views.
    pub transfer_links: Vec<TransferLink>,
    /// Explicit storage domains supplied by runtime bootstrap.
    pub storage_domains: Vec<StorageDomain>,
    /// Typed rate capacities shared by storage and transfer paths.
    pub rate_resources: Vec<RateResource>,
    /// Typed bounded queues shared by runtime paths.
    pub queue_resources: Vec<QueueResource>,
    /// Logical CPU threads available to the process.
    pub logical_cpu_threads: u64,
    /// Performance-oriented CPU cores available to the process.
    pub performance_cpu_cores: CpuClassCapacity,
    /// Process-wide resident-cache capacity.
    pub cache_capacity_bytes: u64,
    /// Simultaneously held table or synchronization locks.
    pub lock_capacity: u64,
    /// File descriptors available to imaging work.
    pub file_descriptor_capacity: u64,
}

/// Mutable external pressure observed around the process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalPressure {
    /// Currently usable bytes in each physical memory domain.
    pub memory_available_bytes: BTreeMap<CapacityDomainId, u64>,
    /// CPU threads currently assignable to imaging work.
    pub available_cpu_threads: u64,
    /// Storage bytes currently assignable to imaging work.
    pub storage_available_bytes: BTreeMap<StorageDomainId, u64>,
    /// Currently usable units per second for every typed rate resource.
    pub rate_available_per_second: BTreeMap<RateResourceId, u64>,
    /// Currently usable slots for every typed queue resource.
    pub queue_available_slots: BTreeMap<QueueResourceId, u64>,
    /// Currently usable occupancy slots for every accelerator.
    pub accelerator_available_slots: BTreeMap<AcceleratorId, u64>,
    /// Resident-cache bytes currently assignable to imaging work.
    pub cache_available_bytes: u64,
    /// Lock slots currently assignable to imaging work.
    pub available_locks: u64,
    /// File descriptors currently assignable to imaging work.
    pub available_file_descriptors: u64,
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
    /// Detects local CPU, memory, and Metal topology inside the runtime.
    /// Storage domains and measured rates require an explicit bootstrap
    /// profile and are never inferred from the current working directory.
    fn detect() -> Result<Self, ResourceError> {
        let (physical_memory_bytes, available_memory_bytes) = detect_host_memory()?;
        let logical_cpu_threads = std::thread::available_parallelism()
            .map_err(|error| ResourceError::Detection(error.to_string()))?
            .get() as u64;
        let performance_cpu_cores = detect_performance_cpu_cores()
            .map(|cores| CpuClassCapacity::Known(cores.clamp(1, logical_cpu_threads)))
            .unwrap_or(CpuClassCapacity::Unknown);
        let unified_metal_available = detect_unified_metal_device();
        let host_domain = CapacityDomainId::new("production-host-memory");
        let host_view = CapacityViewId::new("production-host-memory");
        let mut memory_views = vec![MemoryView {
            id: host_view.clone(),
            domain: host_domain.clone(),
            kind: MemoryViewKind::Host,
        }];
        let mut accelerators = Vec::new();
        let mut transfer_links = Vec::new();
        let mut rate_resources = Vec::new();
        let mut queue_resources = Vec::new();
        if unified_metal_available {
            let metal_view = CapacityViewId::new("production-metal-memory");
            let command_queue = QueueResourceId::new("production-metal-command-queue");
            let transfer_queue = QueueResourceId::new("production-metal-transfer-queue");
            let transfer_rate = RateResourceId::new("production-unified-transfer-rate");
            memory_views.push(MemoryView {
                id: metal_view.clone(),
                domain: host_domain.clone(),
                kind: MemoryViewKind::Metal,
            });
            queue_resources.push(QueueResource::new(command_queue.clone(), 1));
            queue_resources.push(QueueResource::new(transfer_queue.clone(), 1));
            rate_resources.push(RateResource::new(
                transfer_rate.clone(),
                RateUnit::BytesPerSecond,
                0,
            ));
            accelerators.push(Accelerator {
                id: AcceleratorId::new("production-metal-0"),
                kind: AcceleratorKind::Metal,
                memory_view: metal_view.clone(),
                command_queue,
                occupancy_slots: 1,
            });
            transfer_links.push(TransferLink {
                id: TransferLinkId::new("production-host-to-metal"),
                source_view: host_view,
                destination_view: metal_view,
                rate: transfer_rate,
                queue: transfer_queue,
            });
        }
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
            accelerators,
            transfer_links,
            storage_domains: Vec::new(),
            rate_resources,
            queue_resources,
            logical_cpu_threads,
            performance_cpu_cores,
            cache_capacity_bytes: physical_memory_bytes,
            // Table and synchronization capacity has no portable detector.
            // Zero fails closed until the embedding runtime supplies a profile.
            lock_capacity: 0,
            file_descriptor_capacity,
        };
        let pressure = ExternalPressure {
            memory_available_bytes: BTreeMap::from([(
                host_domain,
                available_memory_bytes.min(physical_memory_bytes),
            )]),
            available_cpu_threads: logical_cpu_threads,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: topology
                .rate_resources
                .iter()
                .map(|rate| (rate.id.clone(), rate.units_per_second))
                .collect(),
            queue_available_slots: topology
                .queue_resources
                .iter()
                .map(|queue| (queue.id.clone(), queue.slots))
                .collect(),
            accelerator_available_slots: topology
                .accelerators
                .iter()
                .map(|accelerator| (accelerator.id.clone(), accelerator.occupancy_slots))
                .collect(),
            cache_available_bytes: available_memory_bytes.min(physical_memory_bytes),
            available_locks: 0,
            available_file_descriptors: file_descriptor_capacity,
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

/// Concurrent active logical-byte ceilings for typed I/O-buffer purposes.
///
/// These values bound typed activity permits but do not reserve physical
/// memory. The execution DAG assigns every such logical buffer to a
/// MemoryDemand-backed PhysicalSlot, which is the sole byte-capacity charge
/// and may be reused across disjoint compatible lifetimes.
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

    /// Returns the concurrent logical-byte ceiling for one typed purpose.
    #[must_use]
    pub const fn bytes(self, kind: IoBufferKind) -> u64 {
        match kind {
            IoBufferKind::SourceReadAhead => self.source_read_ahead_bytes,
            IoBufferKind::Decode => self.decode_bytes,
            IoBufferKind::Preparation => self.preparation_bytes,
            IoBufferKind::HostToDeviceTransfer => self.host_to_device_transfer_bytes,
            IoBufferKind::DeviceToHostTransfer => self.device_to_host_transfer_bytes,
            IoBufferKind::SpillRead => self.spill_read_bytes,
            IoBufferKind::SpillWrite => self.spill_write_bytes,
            IoBufferKind::Serialization => self.serialization_bytes,
            IoBufferKind::StorageManager => self.storage_manager_bytes,
            IoBufferKind::TiledColumnWriter => self.tiled_column_writer_bytes,
            IoBufferKind::ScalarColumnWriter => self.scalar_column_writer_bytes,
            IoBufferKind::Writeback => self.writeback_bytes,
            IoBufferKind::Publication => self.publication_bytes,
            IoBufferKind::MappedPageCache => self.mapped_page_cache_bytes,
        }
    }
}

/// One named demand on a typed bounded queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueDemand {
    /// Stable identity retained by the lease.
    pub demand_id: String,
    /// Queue resource charged by this demand.
    pub resource: QueueResourceId,
    /// Hard and preferred occupied slots.
    pub slots: CountDemand,
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

/// Storage-capacity demand bound to one configured filesystem domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageDemand {
    /// Stable identity retained by the lease.
    pub demand_id: String,
    /// Storage domain charged by this demand.
    pub domain: StorageDomainId,
    /// Mandatory temporary spill storage.
    pub temporary_bytes: u64,
    /// Mandatory staged-output storage.
    pub staged_output_bytes: u64,
    /// Mandatory final-output storage.
    pub final_output_bytes: u64,
    /// Mandatory persistent-cache storage.
    pub persistent_cache_bytes: u64,
    /// Required and preferred sequential read bandwidth.
    pub read_rate: CountDemand,
    /// Required and preferred sequential write bandwidth.
    pub write_rate: CountDemand,
    /// Required and preferred storage operations per second.
    pub operations_rate: CountDemand,
    /// Required and preferred storage queue occupancy.
    pub queue_slots: CountDemand,
}

impl StorageDemand {
    fn checked_capacity(&self) -> Result<u64, ResourceError> {
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

/// One named demand on a typed throughput resource.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RateDemand {
    /// Stable identity retained by the lease.
    pub demand_id: String,
    /// Rate resource charged by this demand.
    pub resource: RateResourceId,
    /// Hard and preferred units per second.
    pub amount: CountDemand,
}

/// One named accelerator occupancy demand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceleratorDemand {
    /// Stable identity retained by the lease.
    pub demand_id: String,
    /// Accelerator charged by this demand.
    pub accelerator: AcceleratorId,
    /// Hard and preferred occupancy slots.
    pub slots: CountDemand,
    /// Hard and preferred command-queue occupancy.
    pub command_queue_slots: CountDemand,
}

/// One named demand bound to a typed transfer link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferDemand {
    /// Stable identity retained by the lease.
    pub demand_id: String,
    /// Transfer link charged by this demand.
    pub link: TransferLinkId,
    /// Hard and preferred transfer rate.
    pub rate: CountDemand,
    /// Hard and preferred transfer-queue occupancy.
    pub queue_slots: CountDemand,
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
    pub storage: Vec<StorageDemand>,
    /// Typed throughput demands for storage and transfer paths.
    pub rates: Vec<RateDemand>,
    /// Resident-cache demand.
    pub caches: CacheDemand,
    /// Table and synchronization lock demand.
    pub locks: CountDemand,
    /// File-descriptor demand.
    pub file_descriptors: CountDemand,
    /// Bounded queue-depth demand.
    pub queues: Vec<QueueDemand>,
    /// Typed transfer-link demands.
    pub transfers: Vec<TransferDemand>,
    /// Accelerator occupancy demands.
    pub accelerators: Vec<AcceleratorDemand>,
    /// Every I/O buffer category owned by the run.
    pub io_buffers: IoBufferDemand,
}

/// Capability coverage declared by one implementation alternative.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CapabilityPredicate {
    /// Capabilities this alternative can satisfy.
    pub supported: BTreeSet<CapabilityId>,
}

impl CapabilityPredicate {
    fn accepts(&self, required: &BTreeSet<CapabilityId>) -> bool {
        required.is_subset(&self.supported)
    }
}

/// Scaling facts retained with a selected alternative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScalingMetadata {
    /// Smallest supported worker count.
    pub minimum_workers: u64,
    /// Largest supported worker count.
    pub maximum_workers: u64,
    /// Largest plan-sealed batch size covered by the hard demand envelope.
    pub maximum_batch_size: u64,
    /// Largest plan-sealed tile width covered by the hard demand envelope.
    pub maximum_tile_width: u64,
    /// Largest plan-sealed tile height covered by the hard demand envelope.
    pub maximum_tile_height: u64,
    /// Largest plan-sealed slab depth covered by the hard demand envelope.
    pub maximum_slab_depth: u64,
    /// Additional resident bytes per worker by physical memory domain.
    pub memory_bytes_per_worker: BTreeMap<CapacityDomainId, u64>,
}

/// A point at which a run can safely reconsider resource allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QuiescencePoint {
    /// Entire run boundary.
    RunBoundary,
    /// Boundary between declared processing stages.
    Stage,
    /// Major-cycle boundary.
    MajorCycle,
    /// Tile-batch boundary.
    TileBatch,
    /// Cube-slab boundary.
    Slab,
}

/// Capacity reserved in addition to a run's consumable hard ceilings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceHeadroom {
    /// Reserved memory bytes by physical domain.
    pub memory_bytes: BTreeMap<CapacityDomainId, u64>,
    /// Reserved worker slots.
    pub workers: u64,
    /// Reserved storage bytes by storage domain.
    pub storage_bytes: BTreeMap<StorageDomainId, u64>,
    /// Reserved rate capacity by rate resource.
    pub rates_per_second: BTreeMap<RateResourceId, u64>,
    /// Reserved resident-cache bytes.
    pub cache_bytes: u64,
    /// Reserved lock capacity.
    pub locks: u64,
    /// Reserved file descriptors.
    pub file_descriptors: u64,
    /// Reserved queue slots by queue resource.
    pub queue_slots: BTreeMap<QueueResourceId, u64>,
    /// Reserved occupancy slots by accelerator.
    pub accelerator_slots: BTreeMap<AcceleratorId, u64>,
}

/// One declarative implementation alternative offered for arbitration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DemandAlternative {
    /// Stable alternative identity.
    pub id: AlternativeId,
    /// Capabilities supported by this alternative.
    pub capabilities: CapabilityPredicate,
    /// Consumable resource demand.
    pub demand: DemandEnvelope,
    /// Non-consumable admission margin retained while the lease is active.
    pub headroom: ResourceHeadroom,
    /// Scaling facts for later quiescent decisions.
    pub scaling: ScalingMetadata,
    /// Safe points at which the runtime may reconsider allocation.
    pub quiescence_points: BTreeSet<QuiescencePoint>,
}

/// Ordered implementation alternatives for one scientific problem.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DemandAlternatives {
    /// Capabilities every selectable alternative must satisfy.
    pub required_capabilities: BTreeSet<CapabilityId>,
    /// Alternatives in caller preference order.
    pub alternatives: Vec<DemandAlternative>,
}

/// One integrity-checked quantitative receipt constraint supplied to Resource
/// Authority during planning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordedAdmissionConstraint {
    pub(crate) alternative: AlternativeId,
    pub(crate) resource: ResourceIdentity,
    pub(crate) required: u64,
    pub(crate) available: u64,
    pub(crate) attempt: ExecutionAttemptId,
    pub(crate) status: ReceiptStatus,
}

impl RecordedAdmissionConstraint {
    fn matches(&self, alternative: &AlternativeId) -> bool {
        self.alternative == *alternative
            && !self.resource.as_str().is_empty()
            && self.required > self.available
    }
}

/// Named runtime-overhead category owned by a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeOverheadKind {
    /// Worker thread stacks.
    ThreadStack,
    /// Allocator fragmentation and bookkeeping.
    AllocatorFragmentation,
    /// External-library workspace.
    ExternalLibrary,
    /// FFT plans and workspace.
    FftWorkspace,
    /// Accelerator driver allocations.
    Driver,
    /// JIT compiler allocations.
    Jit,
    /// Accelerator command buffers.
    CommandBuffer,
}

/// Named logical I/O-buffer purpose owned by a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IoBufferKind {
    /// Source read-ahead buffers.
    SourceReadAhead,
    /// Decode buffers.
    Decode,
    /// Visibility or product preparation buffers.
    Preparation,
    /// Host-to-device transfer buffers.
    HostToDeviceTransfer,
    /// Device-to-host transfer buffers.
    DeviceToHostTransfer,
    /// Spill-read buffers.
    SpillRead,
    /// Spill-write buffers.
    SpillWrite,
    /// Serialization buffers.
    Serialization,
    /// Generic storage-manager buffers.
    StorageManager,
    /// Tiled-column writer buffers.
    TiledColumnWriter,
    /// Scalar-column writer buffers.
    ScalarColumnWriter,
    /// Writeback buffers.
    Writeback,
    /// Publication buffers.
    Publication,
    /// Mapped-file and page-cache exposure.
    MappedPageCache,
}

impl IoBufferKind {
    /// Every typed I/O-buffer purpose in canonical identity order.
    pub const ALL: [Self; 14] = [
        Self::SourceReadAhead,
        Self::Decode,
        Self::Preparation,
        Self::HostToDeviceTransfer,
        Self::DeviceToHostTransfer,
        Self::SpillRead,
        Self::SpillWrite,
        Self::Serialization,
        Self::StorageManager,
        Self::TiledColumnWriter,
        Self::ScalarColumnWriter,
        Self::Writeback,
        Self::Publication,
        Self::MappedPageCache,
    ];
}

/// Named storage-capacity category owned by a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageUseKind {
    /// Temporary spill capacity.
    Temporary,
    /// Staged-output capacity.
    StagedOutput,
    /// Final-output capacity.
    FinalOutput,
    /// Persistent-cache capacity.
    PersistentCache,
}

/// One typed consumable resource declared by a selected lease alternative.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LeaseResource {
    /// One logical resident allocation.
    Memory {
        /// Stable allocation identity from [`MemoryDemand`].
        allocation_id: String,
    },
    /// Worker slots.
    Workers,
    /// One runtime-overhead category.
    RuntimeOverhead(RuntimeOverheadKind),
    /// One I/O-buffer category.
    IoBuffer(IoBufferKind),
    /// One storage-capacity category in a named storage demand.
    Storage {
        /// Stable storage-demand identity.
        demand_id: String,
        /// Storage-capacity category.
        use_kind: StorageUseKind,
    },
    /// Read-rate demand derived from a named storage domain.
    StorageReadRate {
        /// Stable storage-demand identity.
        demand_id: String,
    },
    /// Write-rate demand derived from a named storage domain.
    StorageWriteRate {
        /// Stable storage-demand identity.
        demand_id: String,
    },
    /// IOPS demand derived from a named storage domain.
    StorageOperationsRate {
        /// Stable storage-demand identity.
        demand_id: String,
    },
    /// Queue demand derived from a named storage domain.
    StorageQueue {
        /// Stable storage-demand identity.
        demand_id: String,
    },
    /// One named rate demand.
    Rate {
        /// Stable rate-demand identity.
        demand_id: String,
    },
    /// One named queue demand.
    Queue {
        /// Stable queue-demand identity.
        demand_id: String,
    },
    /// Rate demand derived from a named transfer link.
    TransferRate {
        /// Stable transfer-demand identity.
        demand_id: String,
    },
    /// Queue demand derived from a named transfer link.
    TransferQueue {
        /// Stable transfer-demand identity.
        demand_id: String,
    },
    /// One named accelerator demand.
    Accelerator {
        /// Stable accelerator-demand identity.
        demand_id: String,
    },
    /// Command-queue demand derived from a named accelerator.
    AcceleratorCommandQueue {
        /// Stable accelerator-demand identity.
        demand_id: String,
    },
    /// Resident cache bytes.
    ResidentCache,
    /// Table or synchronization locks.
    Locks,
    /// One exact MeasurementSet table lock, charged to the aggregate lock ceiling.
    MeasurementSetLock {
        /// Location-independent MeasurementSet identity protected by this lock.
        measurement_set: MeasurementSetIdentity,
    },
    /// File descriptors.
    FileDescriptors,
}

/// User-selected host-use policy; detection remains inside the authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourcePolicy {
    /// Admit at most half of byte/rate capacity while preserving one slot of
    /// each available indivisible count resource.
    Interactive,
    /// Admit at most three quarters of capacity and, when known, stay within
    /// the performance-core class.
    Balanced,
    /// Admit against all currently available process resources.
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
    /// Optional per-domain storage ceilings.
    pub storage_bytes: BTreeMap<StorageDomainId, u64>,
    /// Optional per-resource rate ceilings.
    pub rates_per_second: BTreeMap<RateResourceId, u64>,
    /// Optional resident-cache ceiling.
    pub cache_bytes: Option<u64>,
    /// Optional lock ceiling.
    pub locks: Option<u64>,
    /// Optional file-descriptor ceiling.
    pub file_descriptors: Option<u64>,
    /// Optional per-resource queue ceilings.
    pub queue_slots: BTreeMap<QueueResourceId, u64>,
    /// Optional per-accelerator occupancy ceilings.
    pub accelerator_slots: BTreeMap<AcceleratorId, u64>,
}

/// Hard ceilings or preferred targets granted to one run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceGrant {
    memory_bytes: BTreeMap<CapacityDomainId, u64>,
    workers: u64,
    storage_bytes: BTreeMap<StorageDomainId, u64>,
    rates_per_second: BTreeMap<RateResourceId, u64>,
    cache_bytes: u64,
    locks: u64,
    file_descriptors: u64,
    queue_slots: BTreeMap<QueueResourceId, u64>,
    accelerator_slots: BTreeMap<AcceleratorId, u64>,
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

    /// Returns granted bytes in one storage domain.
    pub fn storage_bytes(&self, domain: &StorageDomainId) -> u64 {
        self.storage_bytes.get(domain).copied().unwrap_or(0)
    }

    /// Returns granted units per second for one rate resource.
    pub fn rate_per_second(&self, resource: &RateResourceId) -> u64 {
        self.rates_per_second.get(resource).copied().unwrap_or(0)
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

    /// Returns granted slots for one queue resource.
    pub fn queue_slots(&self, resource: &QueueResourceId) -> u64 {
        self.queue_slots.get(resource).copied().unwrap_or(0)
    }

    /// Returns granted occupancy slots for one accelerator.
    pub fn accelerator_slots(&self, accelerator: &AcceleratorId) -> u64 {
        self.accelerator_slots
            .get(accelerator)
            .copied()
            .unwrap_or(0)
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
    /// The process production authority was already initialized.
    ProductionAlreadyInitialized,
    /// No declared alternative satisfied the required capabilities.
    NoCapableAlternative,
    /// No declared alternative fits current policy, pressure, and reservations.
    ///
    /// The retained certificate is machine-readable evidence of exactly why
    /// every offered alternative was refused.
    NoFeasibleAlternative(AdmissionInfeasibilityCertificate),
    /// A mandatory demand did not fit the current hard ceiling.
    Infeasible {
        /// Resource category that failed admission.
        resource: String,
        /// Mandatory requested amount.
        required: u64,
        /// Amount available after policy, pressure, and active leases.
        available: u64,
    },
    /// A permit requested a resource absent from the selected declaration.
    UndeclaredLeaseResource(LeaseResource),
    /// Concurrent permits would exceed one named lease ceiling.
    LeaseLimitExceeded {
        /// Named resource whose lease ceiling would be exceeded.
        resource: LeaseResource,
        /// Additional amount requested by the caller.
        requested: u64,
        /// Amount remaining under the named hard ceiling.
        available: u64,
    },
    /// Observed external pressure strands active hard reservations.
    ///
    /// The authority retains the observation and advances its pressure epoch
    /// before returning this error, preventing admissions against stale capacity.
    PressureWouldInvalidateLeases {
        /// Physical resource that would become overcommitted.
        resource: String,
        /// Capacity retained by active leases and headroom.
        reserved: u64,
        /// Capacity exposed by the observed pressure snapshot.
        available: u64,
    },
    /// The process-wide authority lock was poisoned.
    AuthorityPoisoned,
}

impl ResourceError {
    /// Returns the mandatory amount for an infeasibility error.
    pub fn required(&self) -> Option<u64> {
        match self {
            Self::Infeasible { required, .. } => Some(*required),
            Self::LeaseLimitExceeded { requested, .. } => Some(*requested),
            Self::PressureWouldInvalidateLeases { reserved, .. } => Some(*reserved),
            Self::NoFeasibleAlternative(certificate) => {
                certificate.first_infeasible().map(|(required, _)| required)
            }
            _ => None,
        }
    }

    /// Returns the available amount for an infeasibility error.
    pub fn available(&self) -> Option<u64> {
        match self {
            Self::Infeasible { available, .. }
            | Self::LeaseLimitExceeded { available, .. }
            | Self::PressureWouldInvalidateLeases { available, .. } => Some(*available),
            Self::NoFeasibleAlternative(certificate) => certificate
                .first_infeasible()
                .map(|(_, available)| available),
            _ => None,
        }
    }
}

/// Why one demand alternative was refused during atomic admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlternativeRejectionReason {
    /// The alternative does not support every required capability.
    NoCapableAlternative,
    /// One mandatory demand exceeded its available hard ceiling.
    Infeasible {
        /// Resource category that failed admission.
        resource: String,
        /// Mandatory requested amount.
        required: u64,
        /// Amount available after policy, pressure, and active leases.
        available: u64,
    },
    /// Resource Authority applied an explicit prior terminal receipt constraint
    /// for the same quantitative pressure region. This is an admission input,
    /// not cost-model learning.
    RecordedFailure {
        /// Attempt whose terminal receipt recorded the failure.
        attempt: ExecutionAttemptId,
        /// Terminal status retained by that receipt.
        status: ReceiptStatus,
    },
}

/// Machine-readable refusal evidence for one named demand alternative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlternativeRejection {
    alternative: AlternativeId,
    reason: AlternativeRejectionReason,
}

impl AlternativeRejection {
    pub(crate) fn new(alternative: AlternativeId, reason: AlternativeRejectionReason) -> Self {
        Self {
            alternative,
            reason,
        }
    }

    /// Return the refused alternative identity.
    #[must_use]
    pub const fn alternative(&self) -> &AlternativeId {
        &self.alternative
    }

    /// Return the exact refusal reason.
    #[must_use]
    pub const fn reason(&self) -> &AlternativeRejectionReason {
        &self.reason
    }
}

/// Complete machine-readable proof that no offered demand alternative fits
/// current policy, pressure, and active reservations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionInfeasibilityCertificate {
    rejections: Vec<AlternativeRejection>,
}

impl AdmissionInfeasibilityCertificate {
    pub(crate) fn from_rejections(rejections: Vec<AlternativeRejection>) -> Self {
        Self { rejections }
    }

    /// Return one refusal per offered alternative in caller order.
    #[must_use]
    pub fn rejections(&self) -> &[AlternativeRejection] {
        &self.rejections
    }

    /// Return the first hard-capacity shortfall recorded by this certificate.
    pub(crate) fn first_infeasible(&self) -> Option<(u64, u64)> {
        self.rejections
            .iter()
            .find_map(|rejection| match &rejection.reason {
                AlternativeRejectionReason::Infeasible {
                    required,
                    available,
                    ..
                } => Some((*required, *available)),
                AlternativeRejectionReason::NoCapableAlternative
                | AlternativeRejectionReason::RecordedFailure { .. } => None,
            })
    }
}

impl fmt::Display for AdmissionInfeasibilityCertificate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, rejection) in self.rejections.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            let alternative = rejection.alternative.as_str();
            match &rejection.reason {
                AlternativeRejectionReason::NoCapableAlternative => {
                    write!(formatter, "{alternative} lacks a required capability")?
                }
                AlternativeRejectionReason::Infeasible {
                    resource,
                    required,
                    available,
                } => write!(
                    formatter,
                    "{alternative} requires {required} {resource}, but only {available} is available"
                )?,
                AlternativeRejectionReason::RecordedFailure { attempt, status } => write!(
                    formatter,
                    "{alternative} was recorded terminally {status:?} by attempt {attempt}"
                )?,
            }
        }
        Ok(())
    }
}

impl fmt::Display for ResourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "invalid resource declaration: {message}"),
            Self::Overflow(category) => write!(formatter, "{category} arithmetic overflowed"),
            Self::Detection(message) => write!(formatter, "resource detection failed: {message}"),
            Self::ProductionAlreadyInitialized => {
                formatter.write_str("production resource authority is already initialized")
            }
            Self::NoCapableAlternative => {
                formatter.write_str("no demand alternative satisfies the required capabilities")
            }
            Self::NoFeasibleAlternative(certificate) => {
                write!(formatter, "no demand alternative fits: {certificate}")
            }
            Self::Infeasible {
                resource,
                required,
                available,
            } => write!(
                formatter,
                "resource {resource} requires {required}, but only {available} is available"
            ),
            Self::UndeclaredLeaseResource(resource) => {
                write!(formatter, "lease did not declare resource {resource:?}")
            }
            Self::LeaseLimitExceeded {
                resource,
                requested,
                available,
            } => write!(
                formatter,
                "lease resource {resource:?} requested {requested}, but only {available} remains"
            ),
            Self::PressureWouldInvalidateLeases {
                resource,
                reserved,
                available,
            } => write!(
                formatter,
                "observed pressure strands {reserved} reserved {resource} with only {available} available"
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
    reserved: ResourceGrant,
    policy: ResourcePolicy,
    quarantined: bool,
    limits: BTreeMap<LeaseResource, u64>,
    consumed: BTreeMap<LeaseResource, u64>,
    outstanding_fences: u64,
    release_requested: bool,
}

#[derive(Debug)]
struct AuthorityState {
    pressure: ExternalPressure,
    leases: BTreeMap<u64, LeaseRecord>,
    next_lease_id: u64,
    epoch: u64,
    pressure_epoch: u64,
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

/// Successful external-pressure epoch transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PressureUpdate {
    previous_epoch: u64,
    current_epoch: u64,
}

impl PressureUpdate {
    /// Returns the pressure epoch replaced by this update.
    pub const fn previous_epoch(self) -> u64 {
        self.previous_epoch
    }

    /// Returns the pressure epoch established by this update.
    pub const fn current_epoch(self) -> u64 {
        self.current_epoch
    }
}

impl ResourceAuthority {
    /// Returns the single lazily detected authority shared by this process.
    pub fn production() -> Result<&'static Self, ResourceError> {
        match PRODUCTION_AUTHORITY
            .get_or_init(|| HostInventory::detect().and_then(Self::with_inventory))
        {
            Ok(authority) => Ok(authority),
            Err(error) => Err(error.clone()),
        }
    }

    /// Installs the inventory used by the one process production authority.
    ///
    /// This must be called by runtime bootstrap before [`Self::production`].
    /// Subsequent initialization attempts are rejected.
    pub fn install_production_inventory(
        inventory: HostInventory,
    ) -> Result<&'static Self, ResourceError> {
        let authority = Self::with_inventory(inventory)?;
        PRODUCTION_AUTHORITY
            .set(Ok(authority))
            .map_err(|_| ResourceError::ProductionAlreadyInitialized)?;
        match PRODUCTION_AUTHORITY.get() {
            Some(Ok(authority)) => Ok(authority),
            Some(Err(error)) => Err(error.clone()),
            None => Err(ResourceError::Invalid(
                "production authority initialization did not persist".to_string(),
            )),
        }
    }

    /// Returns the immutable physical topology owned by this authority.
    pub fn topology(&self) -> &ResourceTopology {
        &self.inner.topology
    }

    /// Returns the current external-pressure epoch.
    pub fn pressure_epoch(&self) -> Result<u64, ResourceError> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        Ok(state.pressure_epoch)
    }

    pub(crate) fn with_inventory(inventory: HostInventory) -> Result<Self, ResourceError> {
        validate_inventory(&inventory)?;
        Ok(Self {
            inner: Arc::new(AuthorityInner {
                topology: inventory.topology,
                state: Mutex::new(AuthorityState {
                    pressure: inventory.pressure,
                    leases: BTreeMap::new(),
                    next_lease_id: 1,
                    epoch: 0,
                    pressure_epoch: 0,
                }),
            }),
        })
    }

    /// Atomically selects, admits, and reserves one complete demand alternative.
    pub(crate) fn acquire(
        &self,
        policy: ResourcePolicy,
        alternatives: DemandAlternatives,
    ) -> Result<ResourceLease, ResourceError> {
        self.acquire_with_recorded_constraints(policy, alternatives, &[])
    }

    /// Atomically selects, admits, and reserves one demand alternative while
    /// applying explicit integrity-checked receipt constraints. Receipt
    /// evidence is an admission input owned by this authority; it is never a
    /// planner-side candidate filter or a cost-model update.
    pub(crate) fn acquire_with_recorded_constraints(
        &self,
        policy: ResourcePolicy,
        alternatives: DemandAlternatives,
        recorded_constraints: &[RecordedAdmissionConstraint],
    ) -> Result<ResourceLease, ResourceError> {
        validate_policy(&self.inner.topology, &policy)?;
        if alternatives.alternatives.is_empty() {
            return Err(ResourceError::Invalid(
                "at least one demand alternative is required".to_string(),
            ));
        }
        let mut alternative_ids = BTreeSet::new();
        if alternatives.alternatives.iter().any(|alternative| {
            alternative.id.as_str().is_empty() || !alternative_ids.insert(&alternative.id)
        }) {
            return Err(ResourceError::Invalid(
                "alternative identities must be non-empty and unique".to_string(),
            ));
        }
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        let pressured = capacity_under_pressure(&self.inner.topology, &state);
        let policy_capacity =
            apply_concurrent_policies(&self.inner.topology, &state, &policy, &pressured);
        let policy_available = available_after_active_leases(&state, policy_capacity)?;
        let mut rejections = Vec::new();
        let mut selected = None;
        for alternative in alternatives.alternatives {
            if !alternative
                .capabilities
                .accepts(&alternatives.required_capabilities)
            {
                rejections.push(AlternativeRejection::new(
                    alternative.id.clone(),
                    AlternativeRejectionReason::NoCapableAlternative,
                ));
                continue;
            }
            validate_alternative(&self.inner.topology, &alternative)?;
            if let Some(constraint) = recorded_constraints
                .iter()
                .find(|constraint| constraint.matches(&alternative.id))
            {
                rejections.push(AlternativeRejection::new(
                    alternative.id.clone(),
                    AlternativeRejectionReason::RecordedFailure {
                        attempt: constraint.attempt,
                        status: constraint.status,
                    },
                ));
                continue;
            }
            let totals = alternative.demand.resource_totals(&self.inner.topology)?;
            let limits = alternative.demand.lease_limits();
            let mut reserved = totals.hard.clone();
            let headroom = headroom_grant(
                &self.inner.topology,
                &alternative.headroom,
                &alternative.demand.host_memory_view,
            )?;
            add_grant(&mut reserved, &headroom)?;
            let reservation_totals = ResourceTotals {
                hard: reserved.clone(),
                preferred: reserved.clone(),
            };
            if let Err(ResourceError::Infeasible {
                resource,
                required,
                available,
            }) = admit_totals(&reservation_totals, &policy_available)
            {
                rejections.push(AlternativeRejection::new(
                    alternative.id.clone(),
                    AlternativeRejectionReason::Infeasible {
                        resource,
                        required,
                        available,
                    },
                ));
                continue;
            }
            let granted = admit_totals(&totals, &policy_available)?;
            selected = Some((alternative, granted, reserved, limits));
            break;
        }
        let Some((alternative, granted, reserved, limits)) = selected else {
            return Err(ResourceError::NoFeasibleAlternative(
                AdmissionInfeasibilityCertificate::from_rejections(rejections),
            ));
        };
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
        let pressure_epoch = state.pressure_epoch;
        state.leases.insert(
            lease_id,
            LeaseRecord {
                reserved,
                policy,
                quarantined: false,
                limits: limits.clone(),
                consumed: BTreeMap::new(),
                outstanding_fences: 0,
                release_requested: false,
            },
        );
        Ok(ResourceLease {
            inner: Arc::clone(&self.inner),
            lease_id,
            epoch,
            pressure_epoch,
            hard: granted.hard,
            preferred: granted.preferred,
            alternative,
            limits,
            release_requested: false,
        })
    }

    /// Replaces the external-pressure snapshot used for subsequent admissions.
    ///
    /// A structurally valid observation is always retained and advances the
    /// pressure epoch. If it undercuts active reservations, the method reports
    /// [`ResourceError::PressureWouldInvalidateLeases`] after publishing the
    /// lower capacity so later admissions fail closed.
    pub fn update_external_pressure(
        &self,
        pressure: ExternalPressure,
    ) -> Result<PressureUpdate, ResourceError> {
        validate_pressure(&self.inner.topology, &pressure)?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        let mut reserved = ResourceGrant::default();
        for record in state.leases.values() {
            add_grant(&mut reserved, &record.reserved)?;
        }
        let available = capacity_for_pressure(&self.inner.topology, &pressure);
        let policy_available = active_policy_capacity(&self.inner.topology, &state, &available);
        let shortfall = grant_shortfall(&reserved, &policy_available);
        let previous_epoch = state.pressure_epoch;
        let current_epoch = state
            .pressure_epoch
            .checked_add(1)
            .ok_or(ResourceError::Overflow("pressure epoch"))?;
        let resource_epoch = state
            .epoch
            .checked_add(1)
            .ok_or(ResourceError::Overflow("resource epoch"))?;
        state.pressure = pressure;
        state.pressure_epoch = current_epoch;
        state.epoch = resource_epoch;
        if let Some((resource, reserved, available)) = shortfall {
            return Err(ResourceError::PressureWouldInvalidateLeases {
                resource,
                reserved,
                available,
            });
        }
        Ok(PressureUpdate {
            previous_epoch,
            current_epoch,
        })
    }
}

fn validate_policy(
    topology: &ResourceTopology,
    policy: &ResourcePolicy,
) -> Result<(), ResourceError> {
    let ResourcePolicy::Explicit(overrides) = policy else {
        return Ok(());
    };
    validate_known_resources(
        "explicit memory domain",
        overrides.memory_bytes.keys(),
        topology.memory_domains.iter().map(|domain| &domain.id),
    )?;
    validate_known_resources(
        "explicit storage domain",
        overrides.storage_bytes.keys(),
        topology.storage_domains.iter().map(|domain| &domain.id),
    )?;
    validate_known_resources(
        "explicit rate resource",
        overrides.rates_per_second.keys(),
        topology.rate_resources.iter().map(|resource| &resource.id),
    )?;
    validate_known_resources(
        "explicit queue resource",
        overrides.queue_slots.keys(),
        topology.queue_resources.iter().map(|resource| &resource.id),
    )?;
    validate_known_resources(
        "explicit accelerator",
        overrides.accelerator_slots.keys(),
        topology
            .accelerators
            .iter()
            .map(|accelerator| &accelerator.id),
    )?;
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
    pressure_epoch: u64,
    hard: ResourceGrant,
    preferred: ResourceGrant,
    alternative: DemandAlternative,
    limits: BTreeMap<LeaseResource, u64>,
    release_requested: bool,
}

impl ResourceLease {
    /// Returns the monotonically increasing authority epoch of this grant.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the external-pressure epoch observed at admission.
    pub const fn pressure_epoch(&self) -> u64 {
        self.pressure_epoch
    }

    /// Returns true when external pressure changed after this lease was admitted.
    pub fn pressure_changed(&self) -> Result<bool, ResourceError> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        Ok(state.pressure_epoch != self.pressure_epoch)
    }

    /// Returns the lease's enforceable hard ceilings.
    pub const fn hard_ceilings(&self) -> &ResourceGrant {
        &self.hard
    }

    /// Returns the lease's preferred operating targets.
    pub const fn preferred_targets(&self) -> &ResourceGrant {
        &self.preferred
    }

    /// Returns the selected implementation alternative identity.
    pub const fn selected_alternative(&self) -> &AlternativeId {
        &self.alternative.id
    }

    /// Returns the capability predicate of the selected alternative.
    pub const fn capabilities(&self) -> &CapabilityPredicate {
        &self.alternative.capabilities
    }

    /// Returns the complete named demand retained from the selected alternative.
    pub const fn demand(&self) -> &DemandEnvelope {
        &self.alternative.demand
    }

    /// Returns the admission headroom reserved for the selected alternative.
    pub const fn headroom(&self) -> &ResourceHeadroom {
        &self.alternative.headroom
    }

    /// Returns the selected alternative's declared scaling facts.
    pub const fn scaling(&self) -> &ScalingMetadata {
        &self.alternative.scaling
    }

    /// Returns the selected alternative's safe reallocation points.
    pub const fn quiescence_points(&self) -> &BTreeSet<QuiescencePoint> {
        &self.alternative.quiescence_points
    }

    /// Returns the selected alternative's hard limit for one named resource.
    pub fn declared_limit(&self, resource: &LeaseResource) -> Option<u64> {
        self.limits.get(&resource.accounting_resource()).copied()
    }

    /// Returns every named hard limit retained from the selected demand.
    pub const fn declared_limits(&self) -> &BTreeMap<LeaseResource, u64> {
        &self.limits
    }

    /// Acquires ownership of consumption under one named hard ceiling.
    pub fn permit(
        &self,
        resource: LeaseResource,
        amount: u64,
    ) -> Result<ResourcePermit, ResourceError> {
        if amount == 0 {
            return Err(ResourceError::Invalid(
                "resource permits must own a positive amount".to_string(),
            ));
        }
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| ResourceError::AuthorityPoisoned)?;
        let record = state.leases.get_mut(&self.lease_id).ok_or_else(|| {
            ResourceError::Invalid("cannot permit a released resource lease".to_string())
        })?;
        if record.release_requested {
            return Err(ResourceError::Invalid(
                "cannot acquire a permit after release was requested".to_string(),
            ));
        }
        let accounting_resource = resource.accounting_resource();
        let limit = record
            .limits
            .get(&accounting_resource)
            .copied()
            .ok_or_else(|| ResourceError::UndeclaredLeaseResource(resource.clone()))?;
        let consumed = record
            .consumed
            .get(&accounting_resource)
            .copied()
            .unwrap_or(0);
        let available = limit.saturating_sub(consumed);
        if amount > available {
            return Err(ResourceError::LeaseLimitExceeded {
                resource,
                requested: amount,
                available,
            });
        }
        record.consumed.insert(
            accounting_resource.clone(),
            consumed
                .checked_add(amount)
                .ok_or(ResourceError::Overflow("lease consumption"))?,
        );
        Ok(ResourcePermit {
            inner: Arc::clone(&self.inner),
            lease_id: self.lease_id,
            resource,
            accounting_resource,
            amount,
            released: false,
        })
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
        })
    }

    /// Requests release; reservations remain until all registered fences complete.
    pub fn release(mut self) -> Result<LeaseRelease, ResourceError> {
        self.release_requested = true;
        let released = request_release(&self.inner, self.lease_id)?;
        Ok(LeaseRelease { released })
    }

    /// Retains only physical-memory permits whose external release could not
    /// be proven, while releasing every other reservation owned by this run.
    pub(crate) fn quarantine_memory_permits(
        mut self,
        mut permits: Vec<ResourcePermit>,
    ) -> Result<(), ResourceError> {
        let result = (|| {
            if permits.is_empty() {
                return Err(ResourceError::Invalid(
                    "memory quarantine requires at least one retained permit".to_string(),
                ));
            }
            let mut retained_consumption = BTreeMap::<LeaseResource, u64>::new();
            let mut retained_reservation = ResourceGrant::default();
            for permit in &permits {
                if permit.released
                    || permit.lease_id != self.lease_id
                    || !Arc::ptr_eq(&permit.inner, &self.inner)
                {
                    return Err(ResourceError::Invalid(
                        "memory quarantine received a permit from another or released lease"
                            .to_string(),
                    ));
                }
                let LeaseResource::Memory { allocation_id } = &permit.resource else {
                    return Err(ResourceError::Invalid(
                        "only physical memory permits may be quarantined".to_string(),
                    ));
                };
                let memory = self
                    .alternative
                    .demand
                    .memory
                    .iter()
                    .find(|memory| &memory.allocation_id == allocation_id)
                    .ok_or_else(|| {
                        ResourceError::Invalid(format!(
                            "quarantined allocation {allocation_id} is absent from lease demand"
                        ))
                    })?;
                if permit.amount > memory.hard_bytes {
                    return Err(ResourceError::Invalid(format!(
                        "quarantined allocation {allocation_id} exceeds its hard memory demand"
                    )));
                }
                let domains = memory
                    .views
                    .iter()
                    .map(|view_id| {
                        self.inner
                            .topology
                            .memory_views
                            .iter()
                            .find(|view| &view.id == view_id)
                            .map(|view| view.domain.clone())
                            .ok_or_else(|| {
                                ResourceError::Invalid(format!(
                                    "quarantined allocation {allocation_id} references unknown memory view {}",
                                    view_id.as_str()
                                ))
                            })
                    })
                    .collect::<Result<BTreeSet<_>, _>>()?;
                for domain in domains {
                    add_map_bytes(
                        &mut retained_reservation.memory_bytes,
                        domain,
                        permit.amount,
                        "quarantined memory",
                    )?;
                }
                let amount = retained_consumption
                    .entry(permit.accounting_resource.clone())
                    .or_default();
                *amount = amount
                    .checked_add(permit.amount)
                    .ok_or(ResourceError::Overflow("quarantined memory consumption"))?;
            }
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| ResourceError::AuthorityPoisoned)?;
            let record = state.leases.get_mut(&self.lease_id).ok_or_else(|| {
                ResourceError::Invalid("cannot quarantine an absent resource lease".to_string())
            })?;
            if record.consumed != retained_consumption {
                return Err(ResourceError::Invalid(
                    "memory quarantine requires every other resource permit to be released"
                        .to_string(),
                ));
            }
            record.reserved = retained_reservation;
            record.release_requested = true;
            record.quarantined = true;
            Ok(())
        })();
        // Whether precise narrowing succeeded or not, dropping these handles
        // must never make externally retained bytes available again.
        self.release_requested = true;
        for permit in &mut permits {
            permit.released = true;
        }
        result
    }
}

impl Drop for ResourceLease {
    fn drop(&mut self) {
        if !self.release_requested {
            let _ = request_release(&self.inner, self.lease_id);
        }
    }
}

/// Lease-owned proof that one named consumption fits its hard ceiling.
#[derive(Debug)]
pub struct ResourcePermit {
    inner: Arc<AuthorityInner>,
    lease_id: u64,
    resource: LeaseResource,
    accounting_resource: LeaseResource,
    amount: u64,
    released: bool,
}

impl ResourcePermit {
    /// Returns the named resource owned by this permit.
    pub const fn resource(&self) -> &LeaseResource {
        &self.resource
    }

    /// Returns the amount owned by this permit.
    pub const fn amount(&self) -> u64 {
        self.amount
    }

    /// Releases this consumption and any now-quiescent pending lease.
    pub fn release(mut self) -> Result<LeaseRelease, ResourceError> {
        let released = release_permit(
            &self.inner,
            self.lease_id,
            &self.accounting_resource,
            self.amount,
        )?;
        self.released = true;
        Ok(LeaseRelease { released })
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        if !self.released {
            let _ = release_permit(
                &self.inner,
                self.lease_id,
                &self.accounting_resource,
                self.amount,
            );
        }
    }
}

impl LeaseResource {
    pub(crate) fn accounting_resource(&self) -> Self {
        match self {
            Self::MeasurementSetLock { .. } => Self::Locks,
            resource => resource.clone(),
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
}

impl ResourceFence {
    /// Marks the fenced work complete and releases a pending lease when this is
    /// its final outstanding fence.
    pub fn complete(self) -> Result<LeaseRelease, ResourceError> {
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
        let released = record.release_requested
            && record.outstanding_fences == 0
            && record.consumed.is_empty();
        if released {
            state.leases.remove(&self.lease_id);
        }
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
    if record.outstanding_fences == 0 && record.consumed.is_empty() {
        state.leases.remove(&lease_id);
        Ok(true)
    } else {
        Ok(false)
    }
}

fn release_permit(
    inner: &AuthorityInner,
    lease_id: u64,
    resource: &LeaseResource,
    amount: u64,
) -> Result<bool, ResourceError> {
    let mut state = inner
        .state
        .lock()
        .map_err(|_| ResourceError::AuthorityPoisoned)?;
    let record = state.leases.get_mut(&lease_id).ok_or_else(|| {
        ResourceError::Invalid("cannot release a permit for an absent lease".to_string())
    })?;
    let consumed =
        record.consumed.get(resource).copied().ok_or_else(|| {
            ResourceError::Invalid("lease permit consumption is absent".to_string())
        })?;
    let remaining = consumed.checked_sub(amount).ok_or_else(|| {
        ResourceError::Invalid("lease permit consumption underflowed".to_string())
    })?;
    if remaining == 0 {
        record.consumed.remove(resource);
    } else {
        record.consumed.insert(resource.clone(), remaining);
    }
    let released =
        record.release_requested && record.outstanding_fences == 0 && record.consumed.is_empty();
    if released {
        state.leases.remove(&lease_id);
    }
    Ok(released)
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

fn validate_alternative(
    topology: &ResourceTopology,
    alternative: &DemandAlternative,
) -> Result<(), ResourceError> {
    if alternative.scaling.minimum_workers == 0
        || alternative.scaling.maximum_workers < alternative.scaling.minimum_workers
        || alternative.demand.workers.hard < alternative.scaling.minimum_workers
        || alternative.demand.workers.hard > alternative.scaling.maximum_workers
        || alternative.scaling.maximum_batch_size == 0
        || alternative.scaling.maximum_tile_width == 0
        || alternative.scaling.maximum_tile_height == 0
        || alternative.scaling.maximum_slab_depth == 0
    {
        return Err(ResourceError::Invalid(format!(
            "alternative {} has scaling metadata inconsistent with its worker ceiling",
            alternative.id.as_str()
        )));
    }
    if alternative.quiescence_points.is_empty() {
        return Err(ResourceError::Invalid(format!(
            "alternative {} declares no quiescence point",
            alternative.id.as_str()
        )));
    }
    let domains = topology
        .memory_domains
        .iter()
        .map(|domain| &domain.id)
        .collect::<BTreeSet<_>>();
    if let Some(domain) = alternative
        .scaling
        .memory_bytes_per_worker
        .keys()
        .find(|domain| !domains.contains(domain))
    {
        return Err(ResourceError::Invalid(format!(
            "alternative {} scaling references unknown memory domain {}",
            alternative.id.as_str(),
            domain.as_str()
        )));
    }
    Ok(())
}

fn headroom_grant(
    topology: &ResourceTopology,
    headroom: &ResourceHeadroom,
    host_memory_view: &CapacityViewId,
) -> Result<ResourceGrant, ResourceError> {
    validate_known_resources(
        "headroom memory domain",
        headroom.memory_bytes.keys(),
        topology.memory_domains.iter().map(|domain| &domain.id),
    )?;
    validate_known_resources(
        "headroom storage domain",
        headroom.storage_bytes.keys(),
        topology.storage_domains.iter().map(|domain| &domain.id),
    )?;
    validate_known_resources(
        "headroom rate resource",
        headroom.rates_per_second.keys(),
        topology.rate_resources.iter().map(|resource| &resource.id),
    )?;
    validate_known_resources(
        "headroom queue resource",
        headroom.queue_slots.keys(),
        topology.queue_resources.iter().map(|resource| &resource.id),
    )?;
    validate_known_resources(
        "headroom accelerator",
        headroom.accelerator_slots.keys(),
        topology
            .accelerators
            .iter()
            .map(|accelerator| &accelerator.id),
    )?;
    let host_domain = topology
        .memory_views
        .iter()
        .find(|view| &view.id == host_memory_view && view.kind == MemoryViewKind::Host)
        .map(|view| view.domain.clone())
        .ok_or_else(|| {
            ResourceError::Invalid(format!(
                "headroom host-memory view {} is absent or not host-visible",
                host_memory_view.as_str()
            ))
        })?;
    let mut memory_bytes = headroom.memory_bytes.clone();
    add_map_bytes(
        &mut memory_bytes,
        host_domain,
        headroom.cache_bytes,
        "cache headroom",
    )?;
    Ok(ResourceGrant {
        memory_bytes,
        workers: headroom.workers,
        storage_bytes: headroom.storage_bytes.clone(),
        rates_per_second: headroom.rates_per_second.clone(),
        cache_bytes: headroom.cache_bytes,
        locks: headroom.locks,
        file_descriptors: headroom.file_descriptors,
        queue_slots: headroom.queue_slots.clone(),
        accelerator_slots: headroom.accelerator_slots.clone(),
    })
}

fn validate_known_resources<'a, Id: Ord + fmt::Debug + 'a>(
    kind: &str,
    declared: impl Iterator<Item = &'a Id>,
    known: impl Iterator<Item = &'a Id>,
) -> Result<(), ResourceError> {
    let known = known.collect::<BTreeSet<_>>();
    if let Some(id) = declared.into_iter().find(|id| !known.contains(id)) {
        return Err(ResourceError::Invalid(format!(
            "{kind} references unknown resource {id:?}"
        )));
    }
    Ok(())
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
    preferred.storage_bytes = admit_resource_map(
        "storage-domain",
        &totals.hard.storage_bytes,
        &totals.preferred.storage_bytes,
        &available.hard.storage_bytes,
        &available.preferred.storage_bytes,
    )?;
    preferred.rates_per_second = admit_resource_map(
        "rate-resource",
        &totals.hard.rates_per_second,
        &totals.preferred.rates_per_second,
        &available.hard.rates_per_second,
        &available.preferred.rates_per_second,
    )?;
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
    preferred.queue_slots = admit_resource_map(
        "queue-resource",
        &totals.hard.queue_slots,
        &totals.preferred.queue_slots,
        &available.hard.queue_slots,
        &available.preferred.queue_slots,
    )?;
    preferred.accelerator_slots = admit_resource_map(
        "accelerator",
        &totals.hard.accelerator_slots,
        &totals.preferred.accelerator_slots,
        &available.hard.accelerator_slots,
        &available.preferred.accelerator_slots,
    )?;
    Ok(GrantedTotals { hard, preferred })
}

fn admit_resource_map<Id: Clone + Ord + fmt::Debug>(
    kind: &str,
    hard: &BTreeMap<Id, u64>,
    preferred: &BTreeMap<Id, u64>,
    hard_available: &BTreeMap<Id, u64>,
    preferred_available: &BTreeMap<Id, u64>,
) -> Result<BTreeMap<Id, u64>, ResourceError> {
    let mut granted = BTreeMap::new();
    for (id, required) in hard {
        let available = hard_available.get(id).copied().unwrap_or(0);
        require_fit(format!("{kind}:{id:?}"), *required, available)?;
        granted.insert(
            id.clone(),
            preferred
                .get(id)
                .copied()
                .unwrap_or(*required)
                .min(preferred_available.get(id).copied().unwrap_or(0)),
        );
    }
    Ok(granted)
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
    match policy {
        ResourcePolicy::Interactive => {
            scale_grant(&mut hard, 1, 2);
        }
        ResourcePolicy::Balanced => {
            scale_grant(&mut hard, 3, 4);
            if let CpuClassCapacity::Known(cores) = topology.performance_cpu_cores {
                hard.workers = hard.workers.min(cores);
            }
        }
        ResourcePolicy::Exclusive => {
            hard.workers = hard.workers.min(topology.logical_cpu_threads);
        }
        ResourcePolicy::Explicit(overrides) => {
            for (domain, ceiling) in &overrides.memory_bytes {
                if let Some(available_bytes) = hard.memory_bytes.get_mut(domain) {
                    *available_bytes = (*available_bytes).min(*ceiling);
                }
            }
            cap_optional(&mut hard.workers, overrides.workers);
            cap_map(&mut hard.storage_bytes, &overrides.storage_bytes);
            cap_map(&mut hard.rates_per_second, &overrides.rates_per_second);
            cap_optional(&mut hard.cache_bytes, overrides.cache_bytes);
            cap_optional(&mut hard.locks, overrides.locks);
            cap_optional(&mut hard.file_descriptors, overrides.file_descriptors);
            cap_map(&mut hard.queue_slots, &overrides.queue_slots);
            cap_map(&mut hard.accelerator_slots, &overrides.accelerator_slots);
        }
    }
    let preferred = hard.clone();
    PolicyAvailability { hard, preferred }
}

fn apply_concurrent_policies(
    topology: &ResourceTopology,
    state: &AuthorityState,
    requested: &ResourcePolicy,
    pressured: &ResourceGrant,
) -> PolicyAvailability {
    let mut available = apply_policy(topology, requested, pressured.clone());
    for record in state.leases.values().filter(|record| !record.quarantined) {
        let active = apply_policy(topology, &record.policy, pressured.clone());
        intersect_grant(&mut available.hard, &active.hard);
        intersect_grant(&mut available.preferred, &active.preferred);
    }
    available
}

fn active_policy_capacity(
    topology: &ResourceTopology,
    state: &AuthorityState,
    pressured: &ResourceGrant,
) -> ResourceGrant {
    let mut available = pressured.clone();
    for record in state.leases.values().filter(|record| !record.quarantined) {
        let active = apply_policy(topology, &record.policy, pressured.clone());
        intersect_grant(&mut available, &active.hard);
    }
    available
}

fn intersect_grant(capacity: &mut ResourceGrant, other: &ResourceGrant) {
    intersect_resource_map(&mut capacity.memory_bytes, &other.memory_bytes);
    capacity.workers = capacity.workers.min(other.workers);
    intersect_resource_map(&mut capacity.storage_bytes, &other.storage_bytes);
    intersect_resource_map(&mut capacity.rates_per_second, &other.rates_per_second);
    capacity.cache_bytes = capacity.cache_bytes.min(other.cache_bytes);
    capacity.locks = capacity.locks.min(other.locks);
    capacity.file_descriptors = capacity.file_descriptors.min(other.file_descriptors);
    intersect_resource_map(&mut capacity.queue_slots, &other.queue_slots);
    intersect_resource_map(&mut capacity.accelerator_slots, &other.accelerator_slots);
}

fn intersect_resource_map<Id: Ord>(capacity: &mut BTreeMap<Id, u64>, other: &BTreeMap<Id, u64>) {
    for (id, amount) in capacity {
        *amount = (*amount).min(other.get(id).copied().unwrap_or(0));
    }
}

fn scale_grant(grant: &mut ResourceGrant, numerator: u64, denominator: u64) {
    scale_map_floor(&mut grant.memory_bytes, numerator, denominator);
    scale_map_floor(&mut grant.storage_bytes, numerator, denominator);
    scale_map_floor(&mut grant.rates_per_second, numerator, denominator);
    scale_map_ceil(&mut grant.queue_slots, numerator, denominator);
    scale_map_ceil(&mut grant.accelerator_slots, numerator, denominator);
    grant.workers = scale_count_ceil(grant.workers, numerator, denominator);
    grant.cache_bytes = grant.cache_bytes.saturating_mul(numerator) / denominator;
    grant.locks = scale_count_ceil(grant.locks, numerator, denominator);
    grant.file_descriptors = scale_count_ceil(grant.file_descriptors, numerator, denominator);
}

fn scale_map_floor<Id>(values: &mut BTreeMap<Id, u64>, numerator: u64, denominator: u64) {
    for value in values.values_mut() {
        *value = scale_count_floor(*value, numerator, denominator);
    }
}

fn scale_map_ceil<Id>(values: &mut BTreeMap<Id, u64>, numerator: u64, denominator: u64) {
    for value in values.values_mut() {
        *value = scale_count_ceil(*value, numerator, denominator);
    }
}

fn scale_count_ceil(value: u64, numerator: u64, denominator: u64) -> u64 {
    let scaled = u128::from(value) * u128::from(numerator);
    u64::try_from(scaled.div_ceil(u128::from(denominator))).unwrap_or(u64::MAX)
}

fn scale_count_floor(value: u64, numerator: u64, denominator: u64) -> u64 {
    let scaled = u128::from(value) * u128::from(numerator);
    u64::try_from(scaled / u128::from(denominator)).unwrap_or(u64::MAX)
}

fn cap_map<Id: Ord>(values: &mut BTreeMap<Id, u64>, caps: &BTreeMap<Id, u64>) {
    for (id, cap) in caps {
        if let Some(value) = values.get_mut(id) {
            *value = (*value).min(*cap);
        }
    }
}

fn cap_optional(value: &mut u64, cap: Option<u64>) {
    if let Some(cap) = cap {
        *value = (*value).min(cap);
    }
}

fn capacity_under_pressure(topology: &ResourceTopology, state: &AuthorityState) -> ResourceGrant {
    capacity_for_pressure(topology, &state.pressure)
}

fn capacity_for_pressure(
    topology: &ResourceTopology,
    pressure: &ExternalPressure,
) -> ResourceGrant {
    let memory_bytes = pressure
        .memory_available_bytes
        .iter()
        .map(|(domain, available)| (domain.clone(), *available))
        .collect();
    ResourceGrant {
        memory_bytes,
        workers: pressure
            .available_cpu_threads
            .min(topology.logical_cpu_threads),
        storage_bytes: pressure.storage_available_bytes.clone(),
        rates_per_second: pressure.rate_available_per_second.clone(),
        cache_bytes: pressure.cache_available_bytes,
        locks: pressure.available_locks,
        file_descriptors: pressure.available_file_descriptors,
        queue_slots: pressure.queue_available_slots.clone(),
        accelerator_slots: pressure.accelerator_available_slots.clone(),
    }
}

fn grant_shortfall(
    reserved: &ResourceGrant,
    available: &ResourceGrant,
) -> Option<(String, u64, u64)> {
    map_shortfall(
        "memory-domain",
        &reserved.memory_bytes,
        &available.memory_bytes,
    )
    .or_else(|| scalar_shortfall("workers", reserved.workers, available.workers))
    .or_else(|| {
        map_shortfall(
            "storage-domain",
            &reserved.storage_bytes,
            &available.storage_bytes,
        )
    })
    .or_else(|| {
        map_shortfall(
            "rate-resource",
            &reserved.rates_per_second,
            &available.rates_per_second,
        )
    })
    .or_else(|| {
        scalar_shortfall(
            "resident-cache",
            reserved.cache_bytes,
            available.cache_bytes,
        )
    })
    .or_else(|| scalar_shortfall("locks", reserved.locks, available.locks))
    .or_else(|| {
        scalar_shortfall(
            "file-descriptors",
            reserved.file_descriptors,
            available.file_descriptors,
        )
    })
    .or_else(|| {
        map_shortfall(
            "queue-resource",
            &reserved.queue_slots,
            &available.queue_slots,
        )
    })
    .or_else(|| {
        map_shortfall(
            "accelerator",
            &reserved.accelerator_slots,
            &available.accelerator_slots,
        )
    })
}

fn map_shortfall<Id: Ord + fmt::Debug>(
    kind: &str,
    reserved: &BTreeMap<Id, u64>,
    available: &BTreeMap<Id, u64>,
) -> Option<(String, u64, u64)> {
    reserved.iter().find_map(|(id, reserved)| {
        let available = available.get(id).copied().unwrap_or(0);
        (*reserved > available).then(|| (format!("{kind}:{id:?}"), *reserved, available))
    })
}

fn scalar_shortfall(kind: &str, reserved: u64, available: u64) -> Option<(String, u64, u64)> {
    (reserved > available).then(|| (kind.to_string(), reserved, available))
}

fn available_after_active_leases(
    state: &AuthorityState,
    mut available: PolicyAvailability,
) -> Result<PolicyAvailability, ResourceError> {
    let mut reserved = ResourceGrant::default();
    for record in state.leases.values() {
        add_grant(&mut reserved, &record.reserved)?;
    }
    subtract_grant(&mut available.hard, &reserved);
    subtract_grant(&mut available.preferred, &reserved);
    Ok(available)
}

fn subtract_grant(available: &mut ResourceGrant, reserved: &ResourceGrant) {
    available.memory_bytes = subtract_resource_map(&available.memory_bytes, &reserved.memory_bytes);
    available.workers = available.workers.saturating_sub(reserved.workers);
    available.storage_bytes =
        subtract_resource_map(&available.storage_bytes, &reserved.storage_bytes);
    available.rates_per_second =
        subtract_resource_map(&available.rates_per_second, &reserved.rates_per_second);
    available.cache_bytes = available.cache_bytes.saturating_sub(reserved.cache_bytes);
    available.locks = available.locks.saturating_sub(reserved.locks);
    available.file_descriptors = available
        .file_descriptors
        .saturating_sub(reserved.file_descriptors);
    available.queue_slots = subtract_resource_map(&available.queue_slots, &reserved.queue_slots);
    available.accelerator_slots =
        subtract_resource_map(&available.accelerator_slots, &reserved.accelerator_slots);
}

fn subtract_resource_map<Id: Clone + Ord>(
    available: &BTreeMap<Id, u64>,
    reserved: &BTreeMap<Id, u64>,
) -> BTreeMap<Id, u64> {
    available
        .iter()
        .map(|(id, available)| {
            (
                id.clone(),
                available.saturating_sub(reserved.get(id).copied().unwrap_or(0)),
            )
        })
        .collect()
}

fn add_grant(total: &mut ResourceGrant, value: &ResourceGrant) -> Result<(), ResourceError> {
    for (domain, bytes) in &value.memory_bytes {
        let current = total.memory_bytes.entry(domain.clone()).or_default();
        *current = current
            .checked_add(*bytes)
            .ok_or(ResourceError::Overflow("reserved memory"))?;
    }
    total.workers = checked_add(total.workers, value.workers, "reserved workers")?;
    add_resource_map(
        &mut total.storage_bytes,
        &value.storage_bytes,
        "reserved storage",
    )?;
    add_resource_map(
        &mut total.rates_per_second,
        &value.rates_per_second,
        "reserved rate",
    )?;
    total.cache_bytes = checked_add(total.cache_bytes, value.cache_bytes, "reserved cache")?;
    total.locks = checked_add(total.locks, value.locks, "reserved locks")?;
    total.file_descriptors = checked_add(
        total.file_descriptors,
        value.file_descriptors,
        "reserved file descriptors",
    )?;
    add_resource_map(
        &mut total.queue_slots,
        &value.queue_slots,
        "reserved queue slots",
    )?;
    add_resource_map(
        &mut total.accelerator_slots,
        &value.accelerator_slots,
        "reserved accelerator slots",
    )?;
    Ok(())
}

fn add_resource_map<Id: Clone + Ord>(
    total: &mut BTreeMap<Id, u64>,
    value: &BTreeMap<Id, u64>,
    category: &'static str,
) -> Result<(), ResourceError> {
    for (id, amount) in value {
        let current = total.entry(id.clone()).or_default();
        *current = current
            .checked_add(*amount)
            .ok_or(ResourceError::Overflow(category))?;
    }
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
            .map(|view| (&view.id, view))
            .collect::<BTreeMap<_, _>>();
        let host_view = views.get(&self.host_memory_view).ok_or_else(|| {
            ResourceError::Invalid(format!(
                "host-memory view {} is absent from topology",
                self.host_memory_view.as_str()
            ))
        })?;
        if host_view.kind != MemoryViewKind::Host {
            return Err(ResourceError::Invalid(format!(
                "host view {} is not host-visible",
                self.host_memory_view.as_str()
            )));
        }
        let host_domain = &host_view.domain;
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
                let view = views.get(view).ok_or_else(|| {
                    ResourceError::Invalid(format!(
                        "memory view {} is absent from topology",
                        view.as_str()
                    ))
                })?;
                physical_domains.insert(view.domain.clone());
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
        // I/O-buffer demands are concurrent logical-byte ceilings. Their
        // physical bytes are admitted exactly once through MemoryDemand and
        // may be reused by disjoint logical lifetimes in the execution DAG.
        let fixed_host_bytes = self.overhead.checked_total()?;
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
        let storage_domains = topology
            .storage_domains
            .iter()
            .map(|domain| (&domain.id, domain))
            .collect::<BTreeMap<_, _>>();
        let rate_resources = topology
            .rate_resources
            .iter()
            .map(|resource| &resource.id)
            .collect::<BTreeSet<_>>();
        let queue_resources = topology
            .queue_resources
            .iter()
            .map(|resource| &resource.id)
            .collect::<BTreeSet<_>>();
        let accelerator_resources = topology
            .accelerators
            .iter()
            .map(|accelerator| (&accelerator.id, accelerator))
            .collect::<BTreeMap<_, _>>();
        let transfer_resources = topology
            .transfer_links
            .iter()
            .map(|transfer| (&transfer.id, transfer))
            .collect::<BTreeMap<_, _>>();
        let mut hard_storage = BTreeMap::new();
        let mut hard_rates = BTreeMap::new();
        let mut preferred_rates = BTreeMap::new();
        let mut hard_queues = BTreeMap::new();
        let mut preferred_queues = BTreeMap::new();
        let mut hard_accelerators = BTreeMap::new();
        let mut preferred_accelerators = BTreeMap::new();
        let mut demand_ids = BTreeSet::new();
        for demand in &self.storage {
            validate_demand_id(&mut demand_ids, "storage", &demand.demand_id)?;
            let storage = storage_domains.get(&demand.domain).ok_or_else(|| {
                ResourceError::Invalid(format!(
                    "storage demand {} references unknown domain {}",
                    demand.demand_id,
                    demand.domain.as_str()
                ))
            })?;
            validate_count("storage read rate", demand.read_rate)?;
            validate_count("storage write rate", demand.write_rate)?;
            validate_count("storage operations rate", demand.operations_rate)?;
            validate_count("storage queue", demand.queue_slots)?;
            add_resource_amount(
                &mut hard_storage,
                demand.domain.clone(),
                demand.checked_capacity()?,
                "storage demand",
            )?;
            add_demand_amounts(
                &mut hard_rates,
                &mut preferred_rates,
                storage.read_rate.clone(),
                demand.read_rate,
                "storage read rate",
            )?;
            add_demand_amounts(
                &mut hard_rates,
                &mut preferred_rates,
                storage.write_rate.clone(),
                demand.write_rate,
                "storage write rate",
            )?;
            if let Some(operations_rate) = &storage.operations_rate {
                add_demand_amounts(
                    &mut hard_rates,
                    &mut preferred_rates,
                    operations_rate.clone(),
                    demand.operations_rate,
                    "storage operations rate",
                )?;
            } else if demand.operations_rate.hard > 0 {
                return Err(ResourceError::Invalid(format!(
                    "storage demand {} requests operations rate from a domain without an IOPS resource",
                    demand.demand_id
                )));
            }
            add_demand_amounts(
                &mut hard_queues,
                &mut preferred_queues,
                storage.queue.clone(),
                demand.queue_slots,
                "storage queue demand",
            )?;
        }
        for demand in &self.rates {
            validate_demand_id(&mut demand_ids, "rate", &demand.demand_id)?;
            validate_count("rate", demand.amount)?;
            if !rate_resources.contains(&demand.resource) {
                return Err(ResourceError::Invalid(format!(
                    "rate demand {} references unknown resource {}",
                    demand.demand_id,
                    demand.resource.as_str()
                )));
            }
            add_resource_amount(
                &mut hard_rates,
                demand.resource.clone(),
                demand.amount.hard,
                "rate demand",
            )?;
            add_resource_amount(
                &mut preferred_rates,
                demand.resource.clone(),
                demand.amount.preferred,
                "preferred rate demand",
            )?;
        }
        for demand in &self.queues {
            validate_demand_id(&mut demand_ids, "queue", &demand.demand_id)?;
            validate_count("queue", demand.slots)?;
            if !queue_resources.contains(&demand.resource) {
                return Err(ResourceError::Invalid(format!(
                    "queue demand {} references unknown resource {}",
                    demand.demand_id,
                    demand.resource.as_str()
                )));
            }
            add_resource_amount(
                &mut hard_queues,
                demand.resource.clone(),
                demand.slots.hard,
                "queue demand",
            )?;
            add_resource_amount(
                &mut preferred_queues,
                demand.resource.clone(),
                demand.slots.preferred,
                "preferred queue demand",
            )?;
        }
        for demand in &self.transfers {
            validate_demand_id(&mut demand_ids, "transfer", &demand.demand_id)?;
            validate_count("transfer rate", demand.rate)?;
            validate_count("transfer queue", demand.queue_slots)?;
            let transfer = transfer_resources.get(&demand.link).ok_or_else(|| {
                ResourceError::Invalid(format!(
                    "transfer demand {} references unknown link {}",
                    demand.demand_id,
                    demand.link.as_str()
                ))
            })?;
            add_demand_amounts(
                &mut hard_rates,
                &mut preferred_rates,
                transfer.rate.clone(),
                demand.rate,
                "transfer rate demand",
            )?;
            add_demand_amounts(
                &mut hard_queues,
                &mut preferred_queues,
                transfer.queue.clone(),
                demand.queue_slots,
                "transfer queue demand",
            )?;
        }
        for demand in &self.accelerators {
            validate_demand_id(&mut demand_ids, "accelerator", &demand.demand_id)?;
            validate_count("accelerator", demand.slots)?;
            validate_count("accelerator command queue", demand.command_queue_slots)?;
            let accelerator = accelerator_resources
                .get(&demand.accelerator)
                .ok_or_else(|| {
                    ResourceError::Invalid(format!(
                        "accelerator demand {} references unknown accelerator {}",
                        demand.demand_id,
                        demand.accelerator.as_str()
                    ))
                })?;
            add_resource_amount(
                &mut hard_accelerators,
                demand.accelerator.clone(),
                demand.slots.hard,
                "accelerator demand",
            )?;
            add_resource_amount(
                &mut preferred_accelerators,
                demand.accelerator.clone(),
                demand.slots.preferred,
                "preferred accelerator demand",
            )?;
            add_demand_amounts(
                &mut hard_queues,
                &mut preferred_queues,
                accelerator.command_queue.clone(),
                demand.command_queue_slots,
                "accelerator command queue demand",
            )?;
        }
        Ok(ResourceTotals {
            hard: ResourceGrant {
                memory_bytes: hard_memory,
                workers: self.workers.hard,
                storage_bytes: hard_storage.clone(),
                rates_per_second: hard_rates,
                cache_bytes: self.caches.hard_resident_bytes,
                locks: self.locks.hard,
                file_descriptors: self.file_descriptors.hard,
                queue_slots: hard_queues,
                accelerator_slots: hard_accelerators,
            },
            preferred: ResourceGrant {
                memory_bytes: preferred_memory,
                workers: self.workers.preferred,
                storage_bytes: hard_storage,
                rates_per_second: preferred_rates,
                cache_bytes: self.caches.preferred_resident_bytes,
                locks: self.locks.preferred,
                file_descriptors: self.file_descriptors.preferred,
                queue_slots: preferred_queues,
                accelerator_slots: preferred_accelerators,
            },
        })
    }
}

impl DemandEnvelope {
    pub(crate) fn lease_limits(&self) -> BTreeMap<LeaseResource, u64> {
        let mut limits = BTreeMap::new();
        for demand in &self.memory {
            limits.insert(
                LeaseResource::Memory {
                    allocation_id: demand.allocation_id.clone(),
                },
                demand.hard_bytes,
            );
        }
        limits.insert(LeaseResource::Workers, self.workers.hard);
        for (kind, amount) in [
            (
                RuntimeOverheadKind::ThreadStack,
                self.overhead.thread_stack_bytes,
            ),
            (
                RuntimeOverheadKind::AllocatorFragmentation,
                self.overhead.allocator_fragmentation_bytes,
            ),
            (
                RuntimeOverheadKind::ExternalLibrary,
                self.overhead.external_library_bytes,
            ),
            (
                RuntimeOverheadKind::FftWorkspace,
                self.overhead.fft_workspace_bytes,
            ),
            (RuntimeOverheadKind::Driver, self.overhead.driver_bytes),
            (RuntimeOverheadKind::Jit, self.overhead.jit_bytes),
            (
                RuntimeOverheadKind::CommandBuffer,
                self.overhead.command_buffer_bytes,
            ),
        ] {
            limits.insert(LeaseResource::RuntimeOverhead(kind), amount);
        }
        for kind in IoBufferKind::ALL {
            limits.insert(LeaseResource::IoBuffer(kind), self.io_buffers.bytes(kind));
        }
        for demand in &self.storage {
            for (use_kind, amount) in [
                (StorageUseKind::Temporary, demand.temporary_bytes),
                (StorageUseKind::StagedOutput, demand.staged_output_bytes),
                (StorageUseKind::FinalOutput, demand.final_output_bytes),
                (
                    StorageUseKind::PersistentCache,
                    demand.persistent_cache_bytes,
                ),
            ] {
                limits.insert(
                    LeaseResource::Storage {
                        demand_id: demand.demand_id.clone(),
                        use_kind,
                    },
                    amount,
                );
            }
            limits.insert(
                LeaseResource::StorageReadRate {
                    demand_id: demand.demand_id.clone(),
                },
                demand.read_rate.hard,
            );
            limits.insert(
                LeaseResource::StorageWriteRate {
                    demand_id: demand.demand_id.clone(),
                },
                demand.write_rate.hard,
            );
            limits.insert(
                LeaseResource::StorageOperationsRate {
                    demand_id: demand.demand_id.clone(),
                },
                demand.operations_rate.hard,
            );
            limits.insert(
                LeaseResource::StorageQueue {
                    demand_id: demand.demand_id.clone(),
                },
                demand.queue_slots.hard,
            );
        }
        for demand in &self.rates {
            limits.insert(
                LeaseResource::Rate {
                    demand_id: demand.demand_id.clone(),
                },
                demand.amount.hard,
            );
        }
        for demand in &self.queues {
            limits.insert(
                LeaseResource::Queue {
                    demand_id: demand.demand_id.clone(),
                },
                demand.slots.hard,
            );
        }
        for demand in &self.transfers {
            limits.insert(
                LeaseResource::TransferRate {
                    demand_id: demand.demand_id.clone(),
                },
                demand.rate.hard,
            );
            limits.insert(
                LeaseResource::TransferQueue {
                    demand_id: demand.demand_id.clone(),
                },
                demand.queue_slots.hard,
            );
        }
        for demand in &self.accelerators {
            limits.insert(
                LeaseResource::Accelerator {
                    demand_id: demand.demand_id.clone(),
                },
                demand.slots.hard,
            );
            limits.insert(
                LeaseResource::AcceleratorCommandQueue {
                    demand_id: demand.demand_id.clone(),
                },
                demand.command_queue_slots.hard,
            );
        }
        limits.insert(
            LeaseResource::ResidentCache,
            self.caches.hard_resident_bytes,
        );
        limits.insert(LeaseResource::Locks, self.locks.hard);
        limits.insert(LeaseResource::FileDescriptors, self.file_descriptors.hard);
        limits
    }
}

fn validate_demand_id<'a>(
    identities: &mut BTreeSet<&'a str>,
    kind: &str,
    identity: &'a str,
) -> Result<(), ResourceError> {
    if identity.is_empty() || !identities.insert(identity) {
        return Err(ResourceError::Invalid(format!(
            "{kind} demand identities must be non-empty and unique within the envelope"
        )));
    }
    Ok(())
}

fn add_resource_amount<Id: Ord>(
    values: &mut BTreeMap<Id, u64>,
    id: Id,
    amount: u64,
    category: &'static str,
) -> Result<(), ResourceError> {
    let current = values.entry(id).or_default();
    *current = current
        .checked_add(amount)
        .ok_or(ResourceError::Overflow(category))?;
    Ok(())
}

fn add_demand_amounts<Id: Clone + Ord>(
    hard: &mut BTreeMap<Id, u64>,
    preferred: &mut BTreeMap<Id, u64>,
    id: Id,
    demand: CountDemand,
    category: &'static str,
) -> Result<(), ResourceError> {
    add_resource_amount(hard, id.clone(), demand.hard, category)?;
    add_resource_amount(preferred, id, demand.preferred, category)
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
    if let CpuClassCapacity::Known(performance_cpu_cores) = topology.performance_cpu_cores {
        if performance_cpu_cores == 0 || performance_cpu_cores > topology.logical_cpu_threads {
            return Err(ResourceError::Invalid(
                "known performance CPU topology must be within logical CPU capacity".to_string(),
            ));
        }
    }
    let mut domains = BTreeMap::new();
    for domain in &topology.memory_domains {
        if domain.id.as_str().is_empty()
            || domain.capacity_bytes == 0
            || domains.insert(&domain.id, domain).is_some()
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
        let domain = domains[&view.domain];
        let valid_semantics = matches!(
            (view.kind, domain.kind),
            (
                MemoryViewKind::Host,
                MemoryCapacityKind::Host | MemoryCapacityKind::Unified
            ) | (
                MemoryViewKind::Metal,
                MemoryCapacityKind::Unified | MemoryCapacityKind::DevicePrivate
            )
        );
        if !valid_semantics {
            return Err(ResourceError::Invalid(format!(
                "memory view {} has incompatible view and capacity-domain semantics",
                view.id.as_str()
            )));
        }
    }
    let rates = topology
        .rate_resources
        .iter()
        .map(|resource| (&resource.id, resource))
        .collect::<BTreeMap<_, _>>();
    if rates.len() != topology.rate_resources.len()
        || topology
            .rate_resources
            .iter()
            .any(|resource| resource.id.as_str().is_empty())
    {
        return Err(ResourceError::Invalid(
            "rate resources must have unique non-empty identities".to_string(),
        ));
    }
    let queues = topology
        .queue_resources
        .iter()
        .map(|resource| (&resource.id, resource.slots))
        .collect::<BTreeMap<_, _>>();
    if queues.len() != topology.queue_resources.len()
        || topology
            .queue_resources
            .iter()
            .any(|resource| resource.id.as_str().is_empty())
    {
        return Err(ResourceError::Invalid(
            "queue resources must have unique non-empty identities".to_string(),
        ));
    }
    let view_by_id = topology
        .memory_views
        .iter()
        .map(|view| (&view.id, view))
        .collect::<BTreeMap<_, _>>();
    let mut storage_ids = BTreeSet::new();
    let mut storage_roots = BTreeSet::new();
    for storage in &topology.storage_domains {
        if storage.id.as_str().is_empty()
            || storage.capacity_bytes == 0
            || !storage.root.is_absolute()
            || !storage_ids.insert(&storage.id)
            || !storage_roots.insert(&storage.root)
            || !rates
                .get(&storage.read_rate)
                .is_some_and(|rate| rate.unit == RateUnit::BytesPerSecond)
            || !rates
                .get(&storage.write_rate)
                .is_some_and(|rate| rate.unit == RateUnit::BytesPerSecond)
            || storage.operations_rate.as_ref().is_some_and(|operations| {
                !rates
                    .get(operations)
                    .is_some_and(|rate| rate.unit == RateUnit::OperationsPerSecond)
            })
            || !queues.contains_key(&storage.queue)
        {
            return Err(ResourceError::Invalid(
                "storage domains must be unique absolute roots with positive capacity, byte-rate resources, an optional IOPS resource, and a typed queue"
                    .to_string(),
            ));
        }
    }
    let mut accelerator_ids = BTreeSet::new();
    for accelerator in &topology.accelerators {
        let memory_view = view_by_id.get(&accelerator.memory_view);
        if accelerator.id.as_str().is_empty()
            || accelerator.occupancy_slots == 0
            || !accelerator_ids.insert(&accelerator.id)
            || !queues.contains_key(&accelerator.command_queue)
            || !memory_view.is_some_and(|view| view.kind == MemoryViewKind::Metal)
        {
            return Err(ResourceError::Invalid(
                "accelerators must have unique identities, positive occupancy, a Metal view, and a typed command queue"
                    .to_string(),
            ));
        }
    }
    let mut transfer_ids = BTreeSet::new();
    for transfer in &topology.transfer_links {
        if transfer.id.as_str().is_empty()
            || !transfer_ids.insert(&transfer.id)
            || !view_by_id.contains_key(&transfer.source_view)
            || !view_by_id.contains_key(&transfer.destination_view)
            || !rates
                .get(&transfer.rate)
                .is_some_and(|rate| rate.unit == RateUnit::BytesPerSecond)
            || !queues.contains_key(&transfer.queue)
        {
            return Err(ResourceError::Invalid(
                "transfer links must have unique identities and reference typed views, rates, and queues"
                    .to_string(),
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
    validate_pressure_map(
        "storage",
        &pressure.storage_available_bytes,
        topology
            .storage_domains
            .iter()
            .map(|domain| (domain.id.clone(), domain.capacity_bytes))
            .collect(),
    )?;
    validate_pressure_map(
        "rate",
        &pressure.rate_available_per_second,
        topology
            .rate_resources
            .iter()
            .map(|resource| (resource.id.clone(), resource.units_per_second))
            .collect(),
    )?;
    validate_pressure_map(
        "queue",
        &pressure.queue_available_slots,
        topology
            .queue_resources
            .iter()
            .map(|resource| (resource.id.clone(), resource.slots))
            .collect(),
    )?;
    validate_pressure_map(
        "accelerator",
        &pressure.accelerator_available_slots,
        topology
            .accelerators
            .iter()
            .map(|resource| (resource.id.clone(), resource.occupancy_slots))
            .collect(),
    )?;
    let bounded_counts = [
        (
            pressure.available_cpu_threads,
            topology.logical_cpu_threads,
            "CPU pressure",
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

fn validate_pressure_map<Id: Ord + fmt::Debug>(
    name: &str,
    available: &BTreeMap<Id, u64>,
    capacity: BTreeMap<Id, u64>,
) -> Result<(), ResourceError> {
    if available.len() != capacity.len() {
        return Err(ResourceError::Invalid(format!(
            "external pressure must report every {name} resource"
        )));
    }
    for (id, available) in available {
        let capacity = capacity.get(id).ok_or_else(|| {
            ResourceError::Invalid(format!(
                "external pressure references unknown {name} resource {id:?}"
            ))
        })?;
        if available > capacity {
            return Err(ResourceError::Invalid(format!(
                "available {name} capacity for {id:?} exceeds topology"
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
    let hardware_support = Command::new("/usr/sbin/system_profiler")
        .args(["SPDisplaysDataType", "-json"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| String::from_utf8_lossy(&output.stdout).contains("spdisplays_metal"));
    let (process_access, has_unified_memory) = detect_process_metal_access();
    metal_inventory_available(hardware_support, process_access, has_unified_memory)
}

#[cfg(any(target_os = "macos", test))]
fn metal_inventory_available(
    hardware_support: bool,
    process_access: bool,
    has_unified_memory: bool,
) -> bool {
    hardware_support && process_access && has_unified_memory
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn detect_process_metal_access() -> (bool, bool) {
    use objc2_metal::{MTLCreateSystemDefaultDevice, MTLDevice};

    let Some(device) = MTLCreateSystemDefaultDevice() else {
        return (false, false);
    };
    (
        device.newCommandQueue().is_some(),
        device.hasUnifiedMemory(),
    )
}

#[cfg(all(target_os = "macos", coverage))]
fn detect_process_metal_access() -> (bool, bool) {
    (false, false)
}

#[cfg(not(target_os = "macos"))]
fn detect_unified_metal_device() -> bool {
    false
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

#[cfg(test)]
mod tests;
