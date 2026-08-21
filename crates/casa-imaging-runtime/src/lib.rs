// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Plan-bound imaging execution, process resource arbitration, and leases.

mod execution;
mod execution_bindings;
mod observation_read;
mod observation_transaction;
mod product_publication;
mod publication_layout;
mod receipt;
mod resource_authority;

pub use execution_bindings::{
    ArtifactDisposition, ArtifactIdentity, ArtifactMeasurement, ArtifactRole, BindingKind,
    CacheIdentity, ExecutionEvidenceError, ExecutionPlan, ExecutionPlanId, ExecutionStatus,
    ImplementationRegistry, ImplementationRegistryId, IoMeasurement, IoPrediction,
    PhysicalWorkBinding, PhysicalWorkBindingError, PhysicalWorkId, PlanError, PlanPrediction,
    PlannedArtifact, PlannerCostModelProfileId, PlanningBindings, PredictionConfidence,
    PredictionUncertainty, PublicationResources, RedactedPath, ResourceMeasurement,
    ResourcePolicyId, RunBindings, RunController, RunDirective, RunError, RunToCompletion,
    StagePrediction, WorkCompletion, WorkExecutionContext, WorkImplementation, WorkMeasurements,
    plan, run,
};
pub use observation_read::{
    ObservationReadCompletion, ObservationReadCompletionError, ObservationReadCompletionOwner,
    ObservationReadSourceReport,
};

pub use execution::{
    AdaptationId, AdaptationTransition, AllocationAccess, AllocationId, AllocationLayout,
    AllocationLifetime, AllocationPurpose, AllocationUse, ClaimLifetime, ExecutionDag,
    ExecutionDagSpecification, ExecutionError, ExecutionKnobs, ExecutionOutcome, FenceId,
    FenceKind, InitializationPolicy, LogicalAllocation, PhysicalSlot, PhysicalSlotId,
    ResourceClaim, ScheduledWork, SlotCompatibility, StorageMode, WorkDependency, WorkDomain,
    WorkImplementationId, WorkKind, WorkNode, WorkNodeId,
};
pub use observation_transaction::{
    BoundObservationTransaction, ObservationTransactionPlanError, ObservationTransactionWork,
};
pub use product_publication::{
    ProductPlannedArtifact, ProductPublicationBinding, ProductPublicationBindingError,
};
pub use publication_layout::{
    PhysicalLayoutId, PublicationBoundKind, PublicationLayoutError, PublicationLayoutLedger,
    PublicationMappedStaging, PublicationParticipant, PublicationPhysicalLayout,
    PublicationResourceBounds, PublicationResourceBoundsError, PublicationStaging,
    PublicationStagingError,
};
pub use receipt::{
    BuildIdentity, CompiledProblemEvidence, ExecutionAttemptId, ExecutionProvenance,
    ExecutionReceipt, ExecutionReceiptBinding, ExecutionReceiptStore, ProductAxesEvidence,
    ProductBeamEvidence, ProductExecutionEvidence, ProductGraphEvidence, ProductGraphNodeEvidence,
    ProductParticipantEvidence, ProductPayloadEvidence, ProductPublicationMemberEvidence,
    ProductSourceBindingEvidence, ProductSourceEvidence, PublicationMappedStagingEvidence,
    PublicationPhysicalLayoutEvidence, PublicationStagingEvidence, ReceiptAdaptation, ReceiptError,
    ReceiptFailureKind, ReceiptInfeasibilityCertificate, ReceiptRetention, ReceiptStatus,
    RestoringBeamEvidence,
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
