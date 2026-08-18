// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Process-level resource inventory, arbitration, and leases for imaging.

mod resource_authority;

pub use resource_authority::{
    CacheDemand, CapacityDomainId, CapacityViewId, CountDemand, DemandEnvelope, ExternalPressure,
    HostInventory, IoBufferDemand, LeaseRelease, MemoryCapacityDomain, MemoryCapacityKind,
    MemoryDemand, MemoryView, MemoryViewKind, QueueDemand, ResourceAuthority, ResourceError,
    ResourceFence, ResourceGrant, ResourceLease, ResourceOverride, ResourcePolicy,
    ResourceTopology, RuntimeOverheadDemand, StorageDemand,
};
