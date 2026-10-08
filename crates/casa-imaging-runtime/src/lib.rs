// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Plan-bound imaging execution, process resource arbitration, and leases.

mod cube_state;
mod execution;
mod execution_bindings;
#[allow(
    dead_code,
    reason = "review-1 foundation is connected to cube callers in milestone B"
)]
mod managed_cube_blocks;
mod managed_model;
mod managed_normal;
mod managed_spill;
mod observation_transaction;
pub mod pass;
pub use cube_state::CubeState;
mod minor;
pub use minor::{MinorCycleOutcome, MinorCycleRunError, run_minor_cycle};
// IF-2 removed the AW replay that read prepared artifacts; IF-3 (#652)
// replaces the store with `AwCatalog` (plan section 6 row 5).
#[expect(dead_code, reason = "IF-3 (#652) deletes the prepared-artifact store")]
mod prepared_artifact;
pub mod product_publication;
mod publication_layout;
mod receipt;
mod resource_authority;
mod serial_product_publication;
mod source_access;
pub use source_access::{bootstrap_source_budget, finalize_source_access};

pub use execution_bindings::{
    ArtifactDisposition, ArtifactIdentity, ArtifactMeasurement, ArtifactMeasurementError,
    ArtifactRole, AttemptBoundObservationCompletion, BatchMeasurement, BindingKind, CacheIdentity,
    CompiledWorkContext, ExecutionEvidenceError, ExecutionPlan, ExecutionPlanId, ExecutionStatus,
    ImplementationContractCatalog, ImplementationContractMetadata, ImplementationRegistry,
    ImplementationRegistryId, IoMeasurement, IoPrediction, ObservationCompletionBindingError,
    ObservationReadCompletionContext, PhysicalWorkBinding, PhysicalWorkBindingError,
    PhysicalWorkId, PlanError, PlanPrediction, PlannedArtifact, PlannerCostModelProfileId,
    PlanningBindings, PredictionConfidence, PredictionUncertainty, PublicationResources,
    RedactedPath, ResourceMeasurement, ResourcePolicyId, RunBindings, RunController, RunDirective,
    RunError, RunToCompletion, StagePrediction, WorkExecutionContext, WorkImplementation,
    WorkMeasurements, plan, run,
};

pub use casa_imaging_reconstruction::{MajorCyclePreparation, SpectralPrimitiveCatalog};
pub use execution::{
    AdaptationId, AdaptationTransition, AllocationAccess, AllocationDisposition, AllocationId,
    AllocationLayout, AllocationLifetime, AllocationPurpose, AllocationUse, ClaimLifetime,
    ExecutionDag, ExecutionDagSpecification, ExecutionError, ExecutionKnobs, ExecutionOutcome,
    FenceId, FenceKind, InitializationPolicy, LogicalAllocation, PhysicalSlot, PhysicalSlotId,
    ResourceClaim, RetainedArtifactPermit, SlotCompatibility, StorageMode,
    WorkAllocationCapability, WorkDependency, WorkDomain, WorkImplementationId, WorkKind, WorkNode,
    WorkNodeId, WorkResourceCapability,
};
pub use managed_spill::ManagedSpillStorage;
pub use observation_transaction::{
    BoundObservationTransaction, ObservationTransactionPlanError,
    ObservationTransactionPublicationScope, ObservationTransactionWork,
};
pub use prepared_artifact::{
    PreparedArtifact, PreparedArtifactBudget, PreparedArtifactCatalogEntryOutcome,
    PreparedArtifactCatalogPlanFragment, PreparedArtifactCatalogReuseOutcome,
    PreparedArtifactConsumer, PreparedArtifactDescriptor, PreparedArtifactError,
    PreparedArtifactExecutionBinding, PreparedArtifactGenerator, PreparedArtifactImportSegment,
    PreparedArtifactImportSource, PreparedArtifactImporter, PreparedArtifactKind,
    PreparedArtifactLoadSource, PreparedArtifactNativeCatalogOutcome,
    PreparedArtifactNativeEntryOutcome, PreparedArtifactNativeGenerator,
    PreparedArtifactNativeLayout, PreparedArtifactNativeOperation,
    PreparedArtifactNativePlanFragment, PreparedArtifactNativePlaneLayout,
    PreparedArtifactNativeRequest, PreparedArtifactOperation, PreparedArtifactOrder,
    PreparedArtifactPlanError, PreparedArtifactPlanFragment, PreparedArtifactPlaneDescriptor,
    PreparedArtifactPrecision, PreparedArtifactReader, PreparedArtifactReaderFactory,
    PreparedArtifactReaderPlan, PreparedArtifactReaderResidency, PreparedArtifactRegistration,
    PreparedArtifactRejection, PreparedArtifactReservation, PreparedArtifactResidencyMeasurements,
    PreparedArtifactReuseOutcome, PreparedArtifactSegmentDescriptor, PreparedArtifactSourceSegment,
    PreparedArtifactStore, PreparedArtifactUvAffine,
};
pub use product_publication::{
    ProductPublicationEntry, ProductPublicationError, ProductPublicationPlan,
};
pub use publication_layout::{
    PhysicalLayoutId, PublicationBoundKind, PublicationLayoutError, PublicationLayoutLedger,
    PublicationMappedStaging, PublicationParticipant, PublicationPhysicalLayout,
    PublicationResourceBounds, PublicationResourceBoundsError, PublicationStaging,
    PublicationStagingError,
};
pub use receipt::{
    BuildIdentity, CompiledProblemEvidence, ExecutionAttemptId, ExecutionProvenance,
    ExecutionReceipt, ExecutionReceiptBinding, ExecutionReceiptStore, ReceiptAdaptation,
    ReceiptError, ReceiptFailureKind, ReceiptInfeasibilityCertificate,
    ReceiptPublicationParticipant, ReceiptRetention, ReceiptStatus,
};
pub use resource_authority::{
    Accelerator, AcceleratorDemand, AcceleratorId, AcceleratorKind,
    AdmissionInfeasibilityCertificate, AlternativeId, AlternativeRejection,
    AlternativeRejectionReason, CacheDemand, CapabilityId, CapabilityPredicate, CapacityDomainId,
    CapacityViewId, CountDemand, CpuClassCapacity, DemandAlternative, DemandAlternatives,
    DemandEnvelope, ExternalPressure, HostInventory, IoBufferDemand, IoBufferKind, LeaseRelease,
    LeaseResource, MemoryCapacityDomain, MemoryCapacityKind, MemoryDemand, MemoryView,
    MemoryViewKind, PressureUpdate, ProductionStorageProfile, QueueDemand, QueueResource,
    QueueResourceId, QuiescencePoint, RateDemand, RateResource, RateResourceId, RateUnit,
    ResourceAuthority, ResourceError, ResourceFence, ResourceGrant, ResourceHeadroom,
    ResourceIdentity, ResourceLease, ResourceOverride, ResourcePermit, ResourcePolicy,
    ResourceTopology, RuntimeOverheadDemand, RuntimeOverheadKind, ScalingMetadata, StorageDemand,
    StorageDomain, StorageDomainId, StorageIoResourceBinding, StorageUseKind, TransferDemand,
    TransferLink, TransferLinkId,
};
pub use serial_product_publication::{
    ProductSinkResidency, SerialProductPublicationCompletion,
    SerialProductPublicationExecutionError, SerialProductPublicationExecutor,
    SerialProductPublicationPlan, SerialProductPublicationPlanError,
    SerialProductPublicationPolicy, SerialProductPublicationRegistry, SerialProductPublicationSink,
};
