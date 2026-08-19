// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Process-level resource inventory, arbitration, and leases for imaging.

mod execution_bindings;
mod resource_authority;

pub use execution_bindings::{
    BindingKind, ExecutionPlan, ExecutionPlanId, ImplementationRegistryId, PhysicalWorkId,
    PlannerCostModelProfileId, PlanningBindings, ResourcePolicyId, RunBindings, RunError, plan,
    run,
};

pub use resource_authority::{
    Accelerator, AcceleratorDemand, AcceleratorId, AcceleratorKind, AlternativeId, CacheDemand,
    CapabilityId, CapabilityPredicate, CapacityDomainId, CapacityViewId, CountDemand,
    CpuClassCapacity, DemandAlternative, DemandAlternatives, DemandEnvelope, ExternalPressure,
    HostInventory, IoBufferDemand, IoBufferKind, LeaseRelease, LeaseResource, MemoryCapacityDomain,
    MemoryCapacityKind, MemoryDemand, MemoryView, MemoryViewKind, PressureUpdate, QueueDemand,
    QueueResource, QueueResourceId, QuiescencePoint, RateDemand, RateResource, RateResourceId,
    RateUnit, ResourceAuthority, ResourceError, ResourceFence, ResourceGrant, ResourceHeadroom,
    ResourceLease, ResourceOverride, ResourcePermit, ResourcePolicy, ResourceTopology,
    RuntimeOverheadDemand, RuntimeOverheadKind, ScalingMetadata, StorageDemand, StorageDomain,
    StorageDomainId, StorageUseKind, TransferDemand, TransferLink, TransferLinkId,
};
