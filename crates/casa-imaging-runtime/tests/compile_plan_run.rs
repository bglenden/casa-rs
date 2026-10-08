// SPDX-License-Identifier: LGPL-3.0-or-later

//! The compile/plan/run suite: physical-work binding, the plan-bound
//! executor, receipts, the observation transaction and sealed product
//! publication.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    ImagingRequestVersion, InstrumentModel, InstrumentResponse, MeasurementEquationContract,
    MetadataTableKind, ModelColumnWrite, ModelExecutionAttemptId, ModelInnerProduct,
    ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage, NumericsContract,
    ObservationSourceState, ObservationTransactionId, ObservationTransactionRequirements,
    PhaseCentreLaw, PointingCentreLaw, PolarizationContract, PolarizationCoordinate,
    PreparedArtifactAwInterpretation, PreparedArtifactCellSemantics,
    PreparedArtifactKernelAlgorithm, PreparedArtifactKernelSemantics,
    PreparedArtifactScientificIdentity, PreparedArtifactSpectralMapSemantics, ProblemSpecification,
    ProductKind, ProductNormalization, ProductRequirements, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct,
    WeightDensityScope, WeightingContract, WeightingScheme, compile,
};
use casa_imaging_products::{
    ContinuumGenerationDemand, ContinuumProductControls, ContinuumProductInputs,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, MajorCycleCompletion, MajorCycleOwner, MajorCyclePreparation,
    ModelLifecycle,
};
use casa_imaging_runtime::{
    AdaptationId, AdaptationTransition, AllocationAccess, AllocationId, AllocationLayout,
    AllocationLifetime, AllocationPurpose, AllocationUse, AlternativeId,
    AlternativeRejectionReason, ArtifactDisposition, ArtifactIdentity, ArtifactMeasurement,
    ArtifactRole, AttemptBoundObservationCompletion, BindingKind, BuildIdentity, CacheDemand,
    CacheIdentity, CapabilityPredicate, CapacityDomainId, CapacityViewId, ClaimLifetime,
    CompiledProblemEvidence, CountDemand, CpuClassCapacity, DemandAlternative, DemandEnvelope,
    ExecutionDag, ExecutionDagSpecification, ExecutionError, ExecutionEvidenceError,
    ExecutionKnobs, ExecutionOutcome, ExecutionPlanId, ExecutionProvenance, ExecutionReceipt,
    ExecutionReceiptBinding, ExecutionReceiptStore, ExecutionStatus, ExternalPressure, FenceId,
    FenceKind, HostInventory, ImplementationContractCatalog, ImplementationContractMetadata,
    ImplementationRegistry, ImplementationRegistryId, InitializationPolicy, IoBufferDemand,
    IoBufferKind, IoMeasurement, IoPrediction, LeaseResource, LogicalAllocation,
    MemoryCapacityDomain, MemoryCapacityKind, MemoryDemand, MemoryView, MemoryViewKind,
    ObservationReadCompletionContext, ObservationTransactionWork, PhysicalLayoutId, PhysicalSlot,
    PhysicalSlotId, PhysicalWorkBinding, PhysicalWorkBindingError, PlanError, PlanPrediction,
    PlannedArtifact, PlannerCostModelProfileId, PlanningBindings, PredictionConfidence,
    PredictionUncertainty, PreparedArtifactBudget, PreparedArtifactCatalogPlanFragment,
    PreparedArtifactDescriptor, PreparedArtifactError, PreparedArtifactLoadSource,
    PreparedArtifactOperation, PreparedArtifactOrder, PreparedArtifactPlanFragment,
    PreparedArtifactPlaneDescriptor, PreparedArtifactPrecision, PreparedArtifactRegistration,
    PreparedArtifactRejection, PreparedArtifactReuseOutcome, PreparedArtifactSegmentDescriptor,
    PreparedArtifactSourceSegment, PreparedArtifactStore, PreparedArtifactUvAffine,
    ProductPublicationPlan, ProductionStorageProfile, PublicationLayoutLedger,
    PublicationMappedStaging, PublicationParticipant, PublicationPhysicalLayout,
    PublicationResourceBounds, PublicationStaging, QueueDemand, QueueResource, QueueResourceId,
    QuiescencePoint, RateDemand, RateResource, RateResourceId, RateUnit, ReceiptFailureKind,
    ReceiptRetention, ReceiptStatus, RedactedPath, ResourceAuthority, ResourceClaim, ResourceError,
    ResourceHeadroom, ResourceMeasurement, ResourceOverride, ResourcePolicy, ResourceTopology,
    RunBindings, RunController, RunDirective, RunError, RunToCompletion, RuntimeOverheadDemand,
    ScalingMetadata, SerialProductPublicationExecutor, SerialProductPublicationPlan,
    SerialProductPublicationPolicy, SerialProductPublicationRegistry, SerialProductPublicationSink,
    SlotCompatibility, StagePrediction, StorageDomain, StorageDomainId, StorageIoResourceBinding,
    StorageMode, StorageUseKind, WorkDependency, WorkDomain, WorkExecutionContext,
    WorkImplementation, WorkImplementationId, WorkKind, WorkMeasurements, WorkNode, WorkNodeId,
    plan as runtime_plan, run as runtime_run,
};
use casa_ms::{
    BoundSelectedObservation, ObservationSourceBinding, SelectedObservationCompletion,
    SelectedObservationContentBudget, SelectedObservationMeasures,
    SelectedObservationResidencyCertificate,
};

mod common;

#[path = "compile_plan_run/harness.rs"]
mod harness;
#[path = "compile_plan_run/physical_work.rs"]
mod physical_work;
#[path = "compile_plan_run/physical_work_variants.rs"]
mod physical_work_variants;
#[path = "compile_plan_run/receipt_documents.rs"]
mod receipt_documents;
#[path = "compile_plan_run/recording_executor.rs"]
mod recording_executor;
#[path = "compile_plan_run/requests.rs"]
mod requests;
#[path = "compile_plan_run/sealed_publication.rs"]
mod sealed_publication;

mod imaging_plan_selection;
#[path = "compile_plan_run/observation_transaction.rs"]
mod observation_transaction;
#[path = "compile_plan_run/plan_binding.rs"]
mod plan_binding;
#[path = "compile_plan_run/prepared_artifact.rs"]
mod prepared_artifact;
#[path = "compile_plan_run/publication_lifecycle.rs"]
mod publication_lifecycle;
#[path = "compile_plan_run/receipt_evidence.rs"]
mod receipt_evidence;
#[path = "compile_plan_run/receipt_progress.rs"]
mod receipt_progress;
#[path = "compile_plan_run/receipt_projection.rs"]
mod receipt_projection;
#[path = "compile_plan_run/run_control.rs"]
mod run_control;
mod walking_skeleton;

use common::{identity, model_lifecycle, problem_inputs, problem_inputs_with_source_count};
use harness::*;
use physical_work::*;
use physical_work_variants::*;
use receipt_documents::*;
use recording_executor::*;
use requests::*;
use sealed_publication::*;

const SELECTED_CONTENT_BYTES: usize = 192 * 1024;

fn implementation_catalog(
    problem: &casa_imaging_model::CompiledProblem,
    dag: &ExecutionDag,
) -> ImplementationContractCatalog {
    let registry = ContractOnlyRegistry::new(
        registry(3),
        implementation_metadata(problem),
        dag.nodes().values().map(|node| node.implementation.clone()),
    );
    ImplementationContractCatalog::from_registry(
        &registry,
        dag.nodes().values().map(|node| node.implementation.clone()),
    )
    .expect("registry publishes every physical implementation contract")
}

fn implementation_metadata(
    problem: &casa_imaging_model::CompiledProblem,
) -> ImplementationContractMetadata {
    ImplementationContractMetadata::new(
        problem.problem_id(),
        problem.numerics_id(),
        problem.required_capabilities().clone(),
    )
}

fn product_validity() -> casa_imaging_model::ProductValidityPolicies {
    casa_imaging_model::ProductValidityPolicies::new(
        casa_imaging_model::PrimaryBeamValidityPolicy::new(
            0.2,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::Zero,
        )
        .expect("valid PB policy"),
        casa_imaging_model::TaylorValidityPolicy::new(
            casa_imaging_model::TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::Zero,
        )
        .expect("valid Taylor policy"),
    )
}

const fn selected_content_budget() -> SelectedObservationContentBudget {
    SelectedObservationContentBudget::new(SELECTED_CONTENT_BYTES, 1, 4)
}

fn serial_storage_io() -> StorageIoResourceBinding {
    StorageIoResourceBinding::new(
        StorageDomainId::new("atomic-output"),
        RateResourceId::new("transaction-io-rate"),
        RateResourceId::new("transaction-io-rate"),
        QueueResourceId::new("transaction-io-queue"),
    )
}

fn selected_observation_bindings(
    problem: &casa_imaging_model::CompiledProblem,
    mut budget: impl FnMut(usize) -> SelectedObservationContentBudget,
) -> Vec<ObservationSourceBinding> {
    problem
        .inputs()
        .observation_snapshot()
        .sources()
        .iter()
        .enumerate()
        .map(|(source_index, source)| {
            ObservationSourceBinding::new(
                ObservationSourceState::new(
                    source.identity(),
                    source.selection().rows().clone(),
                    source.generations().clone(),
                ),
                budget(source_index),
            )
        })
        .collect()
}

fn selected_content_residency(
    problem: &casa_imaging_model::CompiledProblem,
) -> SelectedObservationResidencyCertificate {
    selected_content_residency_with(problem, |_| selected_content_budget())
}

fn selected_content_residency_with(
    problem: &casa_imaging_model::CompiledProblem,
    budget: impl FnMut(usize) -> SelectedObservationContentBudget,
) -> SelectedObservationResidencyCertificate {
    let bindings = selected_observation_bindings(problem, budget);
    BoundSelectedObservation::certify_residency(problem, &bindings)
        .expect("owner-certified selected-content residency")
}
