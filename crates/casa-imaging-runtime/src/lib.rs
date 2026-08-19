// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Plan-bound imaging execution, process resource arbitration, and leases.

mod execution;
mod execution_bindings;
mod resource_authority;

pub use execution_bindings::{
    BindingKind, ExecutionPlan, ExecutionPlanId, ExecutionStatus, ImplementationRegistry,
    ImplementationRegistryId, PhysicalWorkBinding, PhysicalWorkId, PlannerCostModelProfileId,
    PlanningBindings, ResourcePolicyId, RunBindings, RunController, RunDirective, RunError,
    RunToCompletion, WorkImplementation, plan, run,
};

pub use execution::{
    AdaptationId, AdaptationTransition, AllocationAccess, AllocationId, AllocationLayout,
    AllocationLifetime, AllocationPurpose, AllocationUse, ClaimLifetime, ExecutionDag,
    ExecutionDagSpecification, ExecutionError, ExecutionKnobs, ExecutionOutcome, FenceId,
    FenceKind, InitializationPolicy, LogicalAllocation, PhysicalSlot, PhysicalSlotId,
    ResourceClaim, ScheduledWork, SlotCompatibility, StorageMode, WorkDependency, WorkDomain,
    WorkImplementationId, WorkKind, WorkNode, WorkNodeId,
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
