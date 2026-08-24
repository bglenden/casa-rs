// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Plan-bound imaging execution, process resource arbitration, and leases.

mod cost_model;
mod execution;
mod execution_bindings;
mod observation_transaction;
mod prepared_artifact;
mod publication_layout;
mod receipt;
mod resource_authority;

pub use execution_bindings::{
    ArtifactDisposition, ArtifactIdentity, ArtifactMeasurement, ArtifactMeasurementError,
    ArtifactRole, AttemptBoundObservationCompletion, BindingKind, CacheIdentity,
    CompiledWorkContext, ExecutionEvidenceError, ExecutionPlan, ExecutionPlanId, ExecutionStatus,
    ImplementationRegistry, ImplementationRegistryId, IoMeasurement, IoPrediction,
    ObservationCompletionBindingError, ObservationReadCompletionContext, PhysicalWorkBinding,
    PhysicalWorkBindingError, PhysicalWorkId, PlanError, PlanPrediction, PlannedArtifact,
    PlannerCostModelProfileId, PlanningBindings, PredictionConfidence, PredictionUncertainty,
    PublicationResources, RecordedInfeasibility, RedactedPath, ResourceMeasurement,
    ResourcePolicyId, RunBindings, RunController, RunDirective, RunError, RunToCompletion,
    StagePrediction, WorkExecutionContext, WorkImplementation, WorkMeasurements, plan, run,
};

pub use cost_model::{
    PlannerCostModelProfileRecord, ProfileEvidenceEntry, ProfilePromotionError, ProfileReview,
    open_cost_model_profile, promote_cost_model_profile,
};
pub use execution::{
    AdaptationId, AdaptationTransition, AllocationAccess, AllocationId, AllocationLayout,
    AllocationLifetime, AllocationPurpose, AllocationUse, ClaimLifetime, ExecutionDag,
    ExecutionDagSpecification, ExecutionError, ExecutionKnobs, ExecutionOutcome, FenceId,
    FenceKind, InitializationPolicy, LogicalAllocation, PhysicalSlot, PhysicalSlotId,
    ResourceClaim, SlotCompatibility, StorageMode, WorkAllocationCapability, WorkDependency,
    WorkDomain, WorkImplementationId, WorkKind, WorkNode, WorkNodeId, WorkResourceCapability,
};
pub use observation_transaction::{
    BoundObservationTransaction, ObservationTransactionPlanError, ObservationTransactionWork,
};
pub use prepared_artifact::{
    PreparedArtifact, PreparedArtifactBudget, PreparedArtifactDescriptor, PreparedArtifactError,
    PreparedArtifactGenerator, PreparedArtifactKind, PreparedArtifactLoadSource,
    PreparedArtifactOperation, PreparedArtifactOrder, PreparedArtifactPlanError,
    PreparedArtifactPlanFragment, PreparedArtifactPlaneDescriptor, PreparedArtifactPrecision,
    PreparedArtifactRegistration, PreparedArtifactRejection, PreparedArtifactReservation,
    PreparedArtifactReuseOutcome, PreparedArtifactSegmentDescriptor, PreparedArtifactSourceSegment,
    PreparedArtifactStore, PreparedArtifactUvAffine,
};
pub use publication_layout::{
    PhysicalLayoutId, PublicationBoundKind, PublicationLayoutError, PublicationLayoutLedger,
    PublicationMappedStaging, PublicationParticipant, PublicationPhysicalLayout,
    PublicationResourceBounds, PublicationResourceBoundsError, PublicationStaging,
    PublicationStagingError,
};
pub use receipt::{
    BuildIdentity, CompiledProblemEvidence, ExecutionAttemptId, ExecutionProvenance,
    ExecutionReceipt, ExecutionReceiptBinding, ExecutionReceiptStore, ExecutionRouteDisposition,
    ExecutionRouteEvidence, ExecutionRouteRequirement, ExecutionRouteRequirementEvidence,
    ExecutionRouteRequirementKind, ReceiptAdaptation, ReceiptError, ReceiptFailureKind,
    ReceiptInfeasibilityCertificate, ReceiptPublicationParticipant, ReceiptRetention,
    ReceiptStatus,
};
pub use resource_authority::{
    Accelerator, AcceleratorDemand, AcceleratorId, AcceleratorKind,
    AdmissionInfeasibilityCertificate, AlternativeId, AlternativeRejection,
    AlternativeRejectionReason, CacheDemand, CapabilityId, CapabilityPredicate, CapacityDomainId,
    CapacityViewId, CountDemand, CpuClassCapacity, DemandAlternative, DemandAlternatives,
    DemandEnvelope, ExternalPressure, HostInventory, IoBufferDemand, IoBufferKind, LeaseRelease,
    LeaseResource, MemoryCapacityDomain, MemoryCapacityKind, MemoryDemand, MemoryView,
    MemoryViewKind, PressureUpdate, QueueDemand, QueueResource, QueueResourceId, QuiescencePoint,
    RateDemand, RateResource, RateResourceId, RateUnit, ResourceAuthority, ResourceError,
    ResourceFence, ResourceGrant, ResourceHeadroom, ResourceLease, ResourceOverride,
    ResourcePermit, ResourcePolicy, ResourceTopology, RuntimeOverheadDemand, RuntimeOverheadKind,
    ScalingMetadata, StorageDemand, StorageDomain, StorageDomainId, StorageUseKind, TransferDemand,
    TransferLink, TransferLinkId,
};
