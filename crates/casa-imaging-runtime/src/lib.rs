// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Plan-bound imaging execution, process resource arbitration, and leases.

mod execution;
mod execution_bindings;
mod receipt;
mod resource_authority;

pub use execution_bindings::{
    ArtifactDisposition, ArtifactIdentity, ArtifactMeasurement, ArtifactRole, BindingKind,
    CacheIdentity, ExecutionEvidenceError, ExecutionPlan, ExecutionPlanId, ExecutionStatus,
    ImplementationRegistry, ImplementationRegistryId, IoMeasurement, IoPrediction,
    PhysicalWorkBinding, PhysicalWorkBindingError, PhysicalWorkId, PlanError, PlanPrediction,
    PlannedArtifact, PlannerCostModelProfileId, PlanningBindings, PredictionConfidence,
    PredictionUncertainty, RedactedPath, ResourceMeasurement, ResourcePolicyId, RunBindings,
    RunController, RunDirective, RunError, RunToCompletion, StagePrediction, WorkImplementation,
    WorkMeasurements, plan, run,
};

pub use execution::{
    AdaptationId, AdaptationTransition, AllocationAccess, AllocationId, AllocationLayout,
    AllocationLifetime, AllocationPurpose, AllocationUse, ClaimLifetime, ExecutionDag,
    ExecutionDagSpecification, ExecutionError, ExecutionKnobs, ExecutionOutcome, FenceId,
    FenceKind, InitializationPolicy, LogicalAllocation, PhysicalSlot, PhysicalSlotId,
    ResourceClaim, SlotCompatibility, StorageMode, WorkAllocationCapability, WorkDependency,
    WorkDomain, WorkExecutionContext, WorkImplementationId, WorkKind, WorkNode, WorkNodeId,
    WorkResourceCapability,
};
pub use receipt::{
    BuildIdentity, CompiledProblemEvidence, ExecutionAttemptId, ExecutionProvenance,
    ExecutionReceipt, ExecutionReceiptBinding, ExecutionReceiptStore, ExecutionRouteDisposition,
    ExecutionRouteEvidence, ExecutionRouteRequirement, ExecutionRouteRequirementEvidence,
    ExecutionRouteRequirementKind, ReceiptAdaptation, ReceiptError, ReceiptFailureKind,
    ReceiptInfeasibilityCertificate, ReceiptRetention, ReceiptStatus,
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
