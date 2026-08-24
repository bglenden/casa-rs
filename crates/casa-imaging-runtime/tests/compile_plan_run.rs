// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use sha2::{Digest, Sha256};

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    ImagingRequestVersion, InstrumentResponse, MeasurementEquationContract, MetadataTableKind,
    MissingPointingPolicy, ModelColumnWrite, ModelInnerProduct, ModelStateIdentity, MsColumnKind,
    NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationSourceState, ObservationTransactionId, ObservationTransactionRequirements,
    PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn, PointingDirectionSemantic,
    PointingExtrapolation, PointingInterpolation, PointingTimeSampling, PolarizationContract,
    PolarizationCoordinate, PreparedArtifactAwInterpretation, PreparedArtifactCellSemantics,
    PreparedArtifactKernelAlgorithm, PreparedArtifactKernelSemantics,
    PreparedArtifactScientificIdentity, PreparedArtifactSpectralMapSemantics, ProblemSpecification,
    ProductKind, ProductNormalization, ProductRequirements, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct,
    WeightDensityScope, WeightingContract, WeightingScheme, compile,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, WeightingExecutionLimits, WeightingPlan, plan_weighting,
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
    ExecutionReceiptBinding, ExecutionReceiptStore, ExecutionRouteDisposition,
    ExecutionRouteEvidence, ExecutionRouteRequirement, ExecutionRouteRequirementKind,
    ExecutionStatus, ExternalPressure, FenceId, FenceKind, HostInventory,
    ImplementationContractCatalog, ImplementationContractMetadata, ImplementationRegistry,
    ImplementationRegistryId, InitializationPolicy, IoBufferDemand, IoBufferKind, IoMeasurement,
    IoPrediction, LeaseResource, LogicalAllocation, MemoryCapacityDomain, MemoryCapacityKind,
    MemoryDemand, MemoryView, MemoryViewKind, ObservationReadCompletionContext,
    ObservationTransactionWork, PhysicalLayoutId, PhysicalSlot, PhysicalSlotId,
    PhysicalWorkBinding, PhysicalWorkBindingError, PlanError, PlanPrediction, PlannedArtifact,
    PlannerCostModelProfileBootstrap, PlannerCostModelProfileId, PlanningBindings,
    PredictionConfidence, PredictionUncertainty, PreparedArtifactBudget,
    PreparedArtifactDescriptor, PreparedArtifactError, PreparedArtifactLoadSource,
    PreparedArtifactOperation, PreparedArtifactOrder, PreparedArtifactPlanFragment,
    PreparedArtifactPlaneDescriptor, PreparedArtifactPrecision, PreparedArtifactRegistration,
    PreparedArtifactRejection, PreparedArtifactReuseOutcome, PreparedArtifactSegmentDescriptor,
    PreparedArtifactSourceSegment, PreparedArtifactStore, PreparedArtifactUvAffine,
    PublicationLayoutLedger, PublicationMappedStaging, PublicationParticipant,
    PublicationPhysicalLayout, PublicationResourceBounds, PublicationStaging, QueueDemand,
    QueueResource, QueueResourceId, QuiescencePoint, RateDemand, RateResource, RateResourceId,
    RateUnit, ReceiptFailureKind, ReceiptRetention, ReceiptStatus, RedactedPath, ResourceAuthority,
    ResourceClaim, ResourceError, ResourceHeadroom, ResourceMeasurement, ResourceOverride,
    ResourcePolicy, ResourceTopology, RunBindings, RunController, RunDirective, RunError,
    RunToCompletion, RuntimeOverheadDemand, ScalingMetadata, SelectedObservationSourceResources,
    SlotCompatibility, StagePrediction, StorageDomain, StorageDomainId, StorageMode,
    StorageUseKind, WeightingExecutionState, WeightingPlanFragment, WorkDependency, WorkDomain,
    WorkExecutionContext, WorkImplementation, WorkImplementationId, WorkKind, WorkMeasurements,
    WorkNode, WorkNodeId, plan as runtime_plan, run as runtime_run,
};
use casa_ms::{
    BoundSelectedObservation, ObservationSourceBinding, SelectedObservationCompletion,
    SelectedObservationContentBudget, SelectedObservationMeasures,
    SelectedObservationResidencyCertificate,
};

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
            casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB policy"),
        casa_imaging_model::TaylorValidityPolicy::new(
            casa_imaging_model::TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy"),
    )
}

fn planning_profile(byte: u8) -> PlannerCostModelProfileBootstrap {
    PlannerCostModelProfileBootstrap::new(cost_model(byte))
}

mod common;

mod cost_model_profile;
mod imaging_plan_selection;
mod walking_skeleton;

use common::{identity, model_lifecycle, problem_inputs, problem_inputs_with_source_count};

const SELECTED_CONTENT_BYTES: usize = 128 * 1024;

const fn selected_content_budget() -> SelectedObservationContentBudget {
    SelectedObservationContentBudget::new(SELECTED_CONTENT_BYTES, 1, 4)
}

fn selected_content_allocations() -> BTreeSet<AllocationId> {
    BTreeSet::from([AllocationId::new("selected-observation-source-buffer")])
}

fn selected_content_queue() -> LeaseResource {
    LeaseResource::Queue {
        demand_id: "transaction-io-queue".to_string(),
    }
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

fn selected_content_resources(
    problem: &casa_imaging_model::CompiledProblem,
) -> SelectedObservationSourceResources {
    let residency = selected_content_residency(problem);
    let allocations = selected_content_allocations();
    let queue = selected_content_queue();
    SelectedObservationSourceResources::new(residency, allocations, queue)
}

fn only_receipt_path(root: &Path) -> PathBuf {
    let mut entries = fs::read_dir(root)
        .expect("receipt directory listing")
        .map(|entry| entry.expect("receipt entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        });
    let path = entries.next().expect("one persisted receipt");
    assert!(
        entries.next().is_none(),
        "fixture persists exactly one receipt"
    );
    path
}

fn compact_json(value: &str) -> String {
    let mut compact = String::with_capacity(value.len());
    let mut in_string = false;
    let mut escaped = false;
    for character in value.chars() {
        if in_string {
            compact.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
        } else if character == '"' {
            in_string = true;
            compact.push(character);
        } else if !character.is_whitespace() {
            compact.push(character);
        }
    }
    assert!(!in_string && !escaped, "complete JSON string");
    compact
}

fn receipt_payload(document: &str) -> &str {
    let marker = "\"receipt\":";
    let marker_start = document.find(marker).expect("receipt payload field");
    let start = document[marker_start + marker.len()..]
        .find('{')
        .map(|offset| marker_start + marker.len() + offset)
        .expect("receipt payload object");
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, character) in document[start..].char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            continue;
        }
        match character {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &document[start..=start + offset];
                }
            }
            _ => {}
        }
    }
    panic!("complete receipt payload object")
}

fn payload_sha256(document: &str) -> String {
    let payload = compact_json(receipt_payload(document));
    Sha256::digest(payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn with_current_payload_checksum(mut document: String) -> String {
    let digest = payload_sha256(&document);
    let marker = "\"payload_sha256\": \"";
    let start = document.find(marker).expect("payload checksum") + marker.len();
    let end = start + 64;
    assert_eq!(&document[end..end + 1], "\"");
    document.replace_range(start..end, &digest);
    document
}

fn with_usize_array(mut document: String, field: &str, values: &[usize]) -> String {
    let marker = format!("\"{field}\": [");
    let start = document.find(&marker).expect("typed projection field") + marker.len();
    let end = document[start..]
        .find(']')
        .map(|offset| start + offset)
        .expect("typed projection array");
    let replacement = values
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    document.replace_range(start..end, &replacement);
    with_current_payload_checksum(document)
}

fn with_forged_product_graph_identity(mut document: String) -> String {
    let graph_marker = "\"product_graph\": {";
    let graph_start = document
        .find(graph_marker)
        .expect("typed Product Graph projection");
    let identity_marker = "\"identity\": \"";
    let start = document[graph_start..]
        .find(identity_marker)
        .map(|offset| graph_start + offset + identity_marker.len())
        .expect("typed Product Graph identity");
    let end = start + 64;
    document.replace_range(start..end, &"f".repeat(64));
    with_current_payload_checksum(document)
}

fn with_forged_reprojection_identity(mut document: String) -> String {
    let reprojection_marker = "\"reprojection\": {";
    let reprojection_start = document
        .find(reprojection_marker)
        .expect("typed model reprojection projection");
    let identity_marker = "\"identity\": \"";
    let start = document[reprojection_start..]
        .find(identity_marker)
        .map(|offset| reprojection_start + offset + identity_marker.len())
        .expect("typed model reprojection identity");
    let original = document[start..start + 64].to_owned();
    assert_ne!(original, "e".repeat(64));
    document = document.replace(&original, &"e".repeat(64));
    with_current_payload_checksum(document)
}

fn with_forged_model_lifecycle_identity(mut document: String, replacement: &str) -> String {
    let lifecycle_marker = "\"model_lifecycle\": {";
    let lifecycle_start = document
        .find(lifecycle_marker)
        .expect("typed model lifecycle projection");
    let identity_marker = "\"identity\": \"";
    let start = document[lifecycle_start..]
        .find(identity_marker)
        .map(|offset| lifecycle_start + offset + identity_marker.len())
        .expect("typed model lifecycle identity");
    let original = document[start..start + 64].to_owned();
    assert_eq!(replacement.len(), 64);
    assert_ne!(original, replacement);
    document = document.replace(&original, replacement);
    with_current_payload_checksum(document)
}

fn with_forged_model_input_source_identity(mut document: String, replacement: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(&document).expect("receipt JSON");
    let original = value["receipt"]["problem"]["model_lifecycle"]["input"]["source_identity"]
        .as_str()
        .expect("typed model input source identity")
        .to_owned();
    assert_eq!(replacement.len(), 64);
    assert_ne!(original, replacement);
    document = document.replace(&original, replacement);
    with_current_payload_checksum(document)
}

fn with_forged_problem_model_and_audit_identity(mut document: String, replacement: &str) -> String {
    assert_eq!(replacement.len(), 64);
    let model_marker = "\"model_identity\": {";
    let model_start = document.find(model_marker).expect("typed model identity");
    let identity_marker = "\"identity\": \"";
    let typed_start = document[model_start..]
        .find(identity_marker)
        .map(|offset| model_start + offset + identity_marker.len())
        .expect("typed model digest");
    assert_ne!(&document[typed_start..typed_start + 64], replacement);
    document.replace_range(typed_start..typed_start + 64, replacement);

    let audit_marker = "\"observation.model.identity\": \"";
    let audit_start =
        document.find(audit_marker).expect("audit model identity") + audit_marker.len();
    assert_ne!(&document[audit_start..audit_start + 64], replacement);
    document.replace_range(audit_start..audit_start + 64, replacement);
    with_current_payload_checksum(document)
}

fn with_forged_parent_problem_identity(mut document: String, replacement: &str) -> String {
    assert_eq!(replacement.len(), 64);
    for marker in ["\"problem_identity\": \"", "\"problem.identity\": \""] {
        let start = document.find(marker).expect("problem identity projection") + marker.len();
        assert_ne!(&document[start..start + 64], replacement);
        document.replace_range(start..start + 64, replacement);
    }
    with_current_payload_checksum(document)
}

fn with_forged_audit_field(mut document: String, field: &str, value: &str) -> String {
    let marker = format!("\"{field}\": \"");
    let start = document.find(&marker).expect("Product Graph audit field") + marker.len();
    let end = document[start..]
        .find('"')
        .map(|offset| start + offset)
        .expect("Product Graph audit value");
    document.replace_range(start..end, value);
    with_current_payload_checksum(document)
}

#[derive(Clone, Copy, Debug)]
enum RetainedClaimTamper {
    RemoveClaim,
    ChangeAmount,
    WrongRelease,
    AddUnorderedUse,
    AddPostReleaseUse,
    ChangeDagIdentity,
}

fn with_retained_claim_tamper(document: &str, tamper: RetainedClaimTamper) -> String {
    let value: serde_json::Value = serde_json::from_str(document).expect("receipt JSON");
    let plan = value["receipt"]["plan"]
        .as_object()
        .expect("typed plan projection");
    let mut document = document.to_owned();
    if matches!(tamper, RetainedClaimTamper::ChangeDagIdentity) {
        let marker = "\"dag_identity\": \"";
        let start = document.find(marker).expect("DAG identity") + marker.len();
        document.replace_range(start..start + 64, &"f".repeat(64));
    } else {
        let nodes = plan["nodes"]
            .as_array()
            .expect("typed work-node projections");
        let retained_nodes = nodes
            .iter()
            .filter(|node| {
                node["claims"]
                    .as_array()
                    .expect("typed resource claims")
                    .iter()
                    .any(|claim| {
                        claim["lifetime"]
                            .as_str()
                            .is_some_and(|value| value.starts_with("retained_until:"))
                    })
            })
            .collect::<Vec<_>>();
        assert!(
            retained_nodes.len() >= 4,
            "weighting retained-claim topology"
        );
        let retained_marker = "\"lifetime\": \"retained_until:";
        let lifetime_start = document
            .find(retained_marker)
            .expect("serialized retained claim");
        let claim_start = document[..lifetime_start]
            .rfind('{')
            .expect("retained claim object");
        let claim_end = document[lifetime_start..]
            .find('}')
            .map(|offset| lifetime_start + offset + 1)
            .expect("retained claim object end");
        match tamper {
            RetainedClaimTamper::RemoveClaim => {
                let mut end = claim_end;
                while document
                    .as_bytes()
                    .get(end)
                    .is_some_and(u8::is_ascii_whitespace)
                {
                    end += 1;
                }
                if document.as_bytes().get(end) == Some(&b',') {
                    end += 1;
                    document.replace_range(claim_start..end, "");
                } else {
                    let mut start = claim_start;
                    while start > 0
                        && document
                            .as_bytes()
                            .get(start - 1)
                            .is_some_and(u8::is_ascii_whitespace)
                    {
                        start -= 1;
                    }
                    assert_eq!(document.as_bytes().get(start - 1), Some(&b','));
                    document.replace_range(start - 1..claim_end, "");
                }
            }
            RetainedClaimTamper::ChangeAmount => {
                let marker = "\"amount\": ";
                let start = document[claim_start..lifetime_start]
                    .find(marker)
                    .map(|offset| claim_start + offset + marker.len())
                    .expect("retained claim amount");
                let end = document[start..]
                    .find(',')
                    .map(|offset| start + offset)
                    .expect("retained claim amount end");
                let amount = document[start..end]
                    .parse::<u64>()
                    .expect("numeric retained claim amount");
                document.replace_range(start..end, &(amount + 1).to_string());
            }
            RetainedClaimTamper::WrongRelease => {
                let wrong_release = nodes
                    .iter()
                    .find(|node| node["kind"] != "release")
                    .and_then(|node| node["node_id"].as_str())
                    .expect("non-release node")
                    .to_owned();
                let start = lifetime_start + retained_marker.len();
                let end = document[start..]
                    .find('"')
                    .map(|offset| start + offset)
                    .expect("retained release end");
                document.replace_range(start..end, &wrong_release);
            }
            RetainedClaimTamper::AddUnorderedUse | RetainedClaimTamper::AddPostReleaseUse => {
                let claim = document[claim_start..claim_end].to_owned();
                let release_start = lifetime_start + retained_marker.len();
                let release = document[release_start..]
                    .split('"')
                    .next()
                    .expect("retained release");
                let release_event = format!("work:{release}");
                let target = if matches!(tamper, RetainedClaimTamper::AddUnorderedUse) {
                    nodes.iter().find(|node| {
                        node["kind"] == "serialization"
                            && node["claims"]
                                .as_array()
                                .is_some_and(|claims| !claims.is_empty())
                    })
                } else {
                    nodes.iter().find(|node| {
                        node["dependencies"].as_array().is_some_and(|dependencies| {
                            dependencies
                                .iter()
                                .any(|dependency| dependency == &release_event)
                        }) && node["claims"]
                            .as_array()
                            .is_some_and(|claims| !claims.is_empty())
                    })
                }
                .and_then(|node| node["node_id"].as_str())
                .expect("unexpected retained-claim target with claims");
                let target_marker = format!("\"node_id\": \"{target}\"");
                let target_start = document
                    .find(&target_marker)
                    .expect("target node projection");
                let claims_marker = "\"claims\": [";
                let insertion = document[target_start..]
                    .find(claims_marker)
                    .map(|offset| target_start + offset + claims_marker.len())
                    .expect("target claims projection");
                document.insert_str(insertion, &format!("\n{claim},"));
            }
            RetainedClaimTamper::ChangeDagIdentity => unreachable!(),
        }
    }
    with_current_payload_checksum(document)
}

fn geometry(reference_pixel: f64) -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [reference_pixel, 255.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(512, 512),
            direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )],
        CentreLaws::new(
            PhaseCentreLaw::Fixed(direction.reference_direction()),
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTimeCentroid,
                PointingInterpolation::GreatCircleShortestArc,
                PointingExtrapolation::Reject,
                MissingPointingPolicy::Reject,
            )),
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    )
}

fn request(observation: u8) -> ImagingRequest {
    request_with_geometry_and_references(observation, geometry(255.0), default_references())
}

fn request_with_source_count(observation: u8, source_count: usize) -> ImagingRequest {
    request_with_geometry_references_weighting_products_model_write_input_and_source_count(
        observation,
        geometry(255.0),
        default_references(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        vec![ProductKind::Psf],
        ModelColumnWrite::Disabled,
        ModelStateIdentity::Empty,
        source_count,
    )
}

fn request_with_geometry(observation: u8, geometry: GeometryInput) -> ImagingRequest {
    request_with_geometry_and_references(observation, geometry, default_references())
}

fn default_references() -> Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)> {
    vec![(ReferenceDataKind::Measures, identity(90))]
}

fn request_with_geometry_and_references(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
) -> ImagingRequest {
    request_with_geometry_references_and_weighting(
        observation,
        geometry,
        references,
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
    )
}

fn request_with_geometry_references_and_weighting(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        references,
        weighting,
        vec![ProductKind::Psf],
        ModelColumnWrite::Disabled,
    )
}

fn request_with_products(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        default_references(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        ModelColumnWrite::Disabled,
    )
}

fn request_with_products_and_initial_model(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
    model: ModelStateIdentity,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_model_write_and_input(
        observation,
        geometry,
        default_references(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        ModelColumnWrite::Disabled,
        model,
    )
}

fn request_with_model_write(observation: u8) -> ImagingRequest {
    request_with_products_and_model(
        observation,
        geometry(255.0),
        vec![ProductKind::Psf],
        ModelColumnWrite::SelectedRows,
    )
}

fn request_with_products_and_model(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        default_references(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        model_column_write,
    )
}

fn request_with_geometry_references_weighting_products_and_model(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_model_write_and_input(
        observation,
        geometry,
        references,
        weighting,
        products,
        model_column_write,
        ModelStateIdentity::Empty,
    )
}

fn request_with_geometry_references_weighting_products_model_write_and_input(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
    model: ModelStateIdentity,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_model_write_input_and_source_count(
        observation,
        geometry,
        references,
        weighting,
        products,
        model_column_write,
        model,
        1,
    )
}

#[allow(clippy::too_many_arguments)]
fn request_with_geometry_references_weighting_products_model_write_input_and_source_count(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
    model: ModelStateIdentity,
    source_count: usize,
) -> ImagingRequest {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    let specification = ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        weighting,
        ProductRequirements::new(
            products,
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        ObservationTransactionRequirements::new(model_column_write),
        numerics,
    );
    ImagingRequest::new(
        specification,
        geometry,
        problem_inputs_with_source_count(observation, references, model, source_count),
        model_lifecycle(model),
    )
}

fn registry(byte: u8) -> ImplementationRegistryId {
    ImplementationRegistryId::from_sha256([byte; 32])
}

fn cost_model(byte: u8) -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256([byte; 32])
}

fn implementation(byte: u8) -> WorkImplementationId {
    WorkImplementationId::new(format!("test-cpu-{byte}"))
}

fn artifact_measurement(
    planned: ArtifactIdentity,
    observed: Option<ArtifactIdentity>,
    disposition: ArtifactDisposition,
    bytes: u64,
    path: Option<RedactedPath>,
) -> ArtifactMeasurement {
    ArtifactMeasurement::new(planned, observed, disposition, bytes, path)
        .expect("test adapters only report externally constructible artifact dispositions")
}

fn recording_executor(
    byte: u8,
    failure: Option<&'static str>,
    fence_failure: Option<&'static str>,
) -> RecordingExecutor {
    RecordingExecutor {
        id: implementation(byte),
        failure,
        fence_failure,
        fail_only_fence: None,
        calls: AtomicUsize::new(0),
        fence_waits: AtomicUsize::new(0),
        observed_knobs: Mutex::new(Vec::new()),
        measurements: BTreeMap::new(),
        resource_peak_overrides: BTreeMap::new(),
        panic_on_execute: false,
        publication_launched: None,
        visible_generation: None,
        failure_node: None,
        fence_failure_event: None,
        weighting_failure_node: None,
        weighting_fence_failure_event: None,
        publication_failure: None,
        generic_source_access: None,
        initial_consistency_expected: None,
        visibility_during_fence_settlement: None,
        publication_buffer_held: None,
        receipt_root_to_disrupt: None,
        publication_pause: None,
        publication_probe: None,
        observation_completions: None,
        delivered_observation_completions: None,
        observation_completion_failure: None,
        bind_foreign_observation_completion: false,
        selected_observation_completion: Mutex::new(None),
        reopen_weighting_before_replay: false,
        weighting_plan: None,
        weighting_source_residency: None,
        weighting_actual_source_residency: None,
        weighting_source_read: WorkNodeId::new("transaction-read"),
        weighting_state: Mutex::new(WeightingExecutionState::new()),
        weighting_source_sample_count: AtomicUsize::new(0),
        weighted_sample_count: AtomicUsize::new(0),
        weighting_reconciled: AtomicBool::new(false),
        weighting_released: AtomicBool::new(false),
        weighting_cleanup_released: AtomicBool::new(false),
        weighting_owner_at_replay_fence: AtomicBool::new(false),
        weighting_owner_at_reconciliation: AtomicBool::new(false),
        weighting_owner_at_release: AtomicBool::new(false),
    }
}

#[derive(Debug, Default)]
struct PublicationPause {
    entered: AtomicBool,
    release: Mutex<bool>,
    released: Condvar,
}

impl PublicationPause {
    fn wait_until_entered(&self) {
        while !self.entered.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    }

    fn release(&self) {
        *self.release.lock().expect("publication pause lock") = true;
        self.released.notify_all();
    }
}

#[derive(Debug)]
struct RecordedObservationCompletion {
    attempt_id: casa_imaging_runtime::ExecutionAttemptId,
    owner_node: WorkNodeId,
    settled_fences: BTreeSet<FenceKind>,
    lease_epoch: u64,
}

type DeliveredObservationCompletions = Arc<Mutex<Vec<(WorkNodeId, WorkNodeId)>>>;

struct RecordingExecutor {
    id: WorkImplementationId,
    failure: Option<&'static str>,
    fence_failure: Option<&'static str>,
    fail_only_fence: Option<FenceKind>,
    calls: AtomicUsize,
    fence_waits: AtomicUsize,
    observed_knobs: Mutex<Vec<ExecutionKnobs>>,
    measurements: BTreeMap<WorkNodeId, (Vec<IoMeasurement>, Vec<ArtifactMeasurement>)>,
    resource_peak_overrides: BTreeMap<WorkNodeId, u64>,
    panic_on_execute: bool,
    publication_launched: Option<Arc<AtomicBool>>,
    visible_generation: Option<Arc<AtomicUsize>>,
    failure_node: Option<&'static str>,
    fence_failure_event: Option<(&'static str, FenceKind)>,
    weighting_failure_node: Option<WorkNodeId>,
    weighting_fence_failure_event: Option<(WorkNodeId, FenceKind)>,
    publication_failure: Option<&'static str>,
    generic_source_access: Option<Arc<AtomicBool>>,
    initial_consistency_expected: Option<(ObservationTransactionId, Arc<AtomicBool>)>,
    visibility_during_fence_settlement: Option<Arc<AtomicBool>>,
    publication_buffer_held: Option<Arc<AtomicBool>>,
    receipt_root_to_disrupt: Option<PathBuf>,
    publication_pause: Option<Arc<PublicationPause>>,
    publication_probe: Option<PublicationProbe>,
    observation_completions: Option<Arc<Mutex<Vec<RecordedObservationCompletion>>>>,
    delivered_observation_completions: Option<DeliveredObservationCompletions>,
    observation_completion_failure: Option<&'static str>,
    bind_foreign_observation_completion: bool,
    selected_observation_completion: Mutex<Option<SelectedObservationCompletion>>,
    reopen_weighting_before_replay: bool,
    weighting_plan: Option<WeightingPlan>,
    weighting_source_residency: Option<SelectedObservationResidencyCertificate>,
    weighting_actual_source_residency: Option<SelectedObservationResidencyCertificate>,
    weighting_source_read: WorkNodeId,
    weighting_state: Mutex<WeightingExecutionState>,
    weighting_source_sample_count: AtomicUsize,
    weighted_sample_count: AtomicUsize,
    weighting_reconciled: AtomicBool,
    weighting_released: AtomicBool,
    weighting_cleanup_released: AtomicBool,
    weighting_owner_at_replay_fence: AtomicBool,
    weighting_owner_at_reconciliation: AtomicBool,
    weighting_owner_at_release: AtomicBool,
}

#[derive(Debug)]
struct PublicationProbe {
    receipts: Arc<ExecutionReceiptStore>,
    attempt: casa_imaging_runtime::ExecutionAttemptId,
    prepared_observed: Arc<AtomicBool>,
    publication_calls: Arc<AtomicUsize>,
}

impl RecordingExecutor {
    fn weighting_fragment(&self) -> Option<WeightingPlanFragment<'_>> {
        self.weighting_plan.as_ref().map(|plan| {
            WeightingPlanFragment::new(
                plan,
                self.weighting_source_read.clone(),
                SelectedObservationSourceResources::new(
                    self.weighting_source_residency
                        .clone()
                        .expect("weighting executor source residency"),
                    selected_content_allocations(),
                    selected_content_queue(),
                ),
                self.id.clone(),
                self.id.clone(),
                self.id.clone(),
            )
        })
    }
}

fn open_selected_observation(
    problem: &casa_imaging_model::CompiledProblem,
    residency: &SelectedObservationResidencyCertificate,
) -> io::Result<BoundSelectedObservation> {
    let sources = problem.inputs().observation_snapshot().sources();
    let mut budgets = Vec::with_capacity(sources.len());
    for source in sources {
        budgets.push(
            residency
                .content_budget(source.identity())
                .ok_or_else(|| io::Error::other("residency certificate omits source"))?,
        );
    }
    let bindings = selected_observation_bindings(problem, |index| budgets[index]);
    let measures_identity = problem
        .inputs()
        .reference_data()
        .iter()
        .find_map(|(kind, identity)| (*kind == ReferenceDataKind::Measures).then_some(*identity))
        .ok_or_else(|| io::Error::other("ObservationRead has no Measures identity"))?;
    let measures = SelectedObservationMeasures::new(
        casa_test_support::deterministic_measures_provider_for_identity(
            measures_identity.as_bytes(),
        ),
    )
    .map_err(io::Error::other)?;
    BoundSelectedObservation::open(problem, measures, bindings).map_err(io::Error::other)
}

impl WorkImplementation for RecordingExecutor {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        assert!(!self.panic_on_execute, "interrupted adapter");
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed_knobs
            .lock()
            .expect("recording executor knobs lock")
            .push(context.knobs().clone());
        if let Some(message) = self.failure {
            return Err(io::Error::other(message));
        }
        if self.failure_node == Some(context.node().id.as_str()) {
            return Err(io::Error::other("stateful transaction execute failure"));
        }
        if self.weighting_failure_node.as_ref() == Some(&context.node().id) {
            return Err(io::Error::other("weighting lifecycle execute failure"));
        }
        if context.node().kind == WorkKind::ObservationRead {
            let bound_problem = context
                .selected_observation()
                .expect("ObservationRead owns exact selected-observation authority");
            let foreign_problem = self
                .bind_foreign_observation_completion
                .then(|| compile(request(2)).map_err(io::Error::other))
                .transpose()?;
            let problem = foreign_problem.as_ref().unwrap_or(bound_problem);
            let fragment = self.weighting_fragment();
            let mut residency = fragment
                .as_ref()
                .filter(|fragment| {
                    context.node().id == *fragment.generation_node()
                        || context.node().id == *fragment.replay_node()
                })
                .map_or_else(
                    || {
                        Ok::<_, io::Error>(
                            self.weighting_source_residency
                                .clone()
                                .unwrap_or_else(|| selected_content_residency(problem)),
                        )
                    },
                    |fragment| {
                        fragment
                            .selected_observation_residency(context, problem)
                            .cloned()
                            .map_err(io::Error::other)
                    },
                )?;
            if fragment
                .as_ref()
                .is_some_and(|fragment| context.node().id == *fragment.source_read_node())
                && let Some(actual) = &self.weighting_actual_source_residency
            {
                residency = actual.clone();
            }
            if let Some(fragment) = fragment.as_ref()
                && context.node().id == *fragment.generation_node()
            {
                self.weighting_state
                    .lock()
                    .expect("weighting execution state lock")
                    .traverse_generation(context, fragment, problem)
                    .map_err(io::Error::other)?;
            } else if let Some(fragment) = fragment.as_ref()
                && context.node().id == *fragment.replay_node()
            {
                if self.reopen_weighting_before_replay {
                    let replacement = open_selected_observation(problem, &residency)?;
                    self.weighting_state
                        .lock()
                        .expect("weighting execution state lock")
                        .traverse_and_retain_source(context, fragment, replacement, problem, |_| {
                            Ok::<_, io::Error>(())
                        })
                        .map_err(io::Error::other)?;
                }
                self.weighting_state
                    .lock()
                    .expect("weighting execution state lock")
                    .traverse_replay(context, fragment, problem, |block| {
                        self.weighted_sample_count
                            .fetch_add(block.samples().count(), Ordering::SeqCst);
                        Ok::<_, io::Error>(())
                    })
                    .map_err(io::Error::other)?;
            } else {
                let observation = open_selected_observation(problem, &residency)?;
                let completion = if let Some(fragment) = fragment.as_ref()
                    && context.node().id == *fragment.source_read_node()
                {
                    self.weighting_state
                        .lock()
                        .expect("weighting execution state lock")
                        .traverse_and_retain_source(context, fragment, observation, problem, |_| {
                            self.weighting_source_sample_count
                                .fetch_add(1, Ordering::SeqCst);
                            Ok::<_, io::Error>(())
                        })
                        .map_err(io::Error::other)?
                } else {
                    let mut observation = observation;
                    observation
                        .traverse(problem, |_| Ok::<_, io::Error>(()))
                        .map_err(io::Error::other)?
                };
                *self
                    .selected_observation_completion
                    .lock()
                    .expect("selected-observation completion lock") = Some(completion);
            }
        } else if let Some(delivered) = &self.delivered_observation_completions {
            let owner = WorkNodeId::new("transaction-read");
            if let Some(completion) = context.predecessor_observation_completion(&owner) {
                assert_eq!(completion.owner_node(), &owner);
                assert!(
                    context
                        .predecessor_observation_completion(&WorkNodeId::new("not-a-predecessor"))
                        .is_none(),
                    "attempt evidence must not escape its explicit dependency edge"
                );
                delivered
                    .lock()
                    .expect("delivered observation completion lock")
                    .push((context.node().id.clone(), owner));
            }
        }
        if let Some(fragment) = self.weighting_fragment() {
            if context.node().id == *fragment.release_node() {
                self.weighting_owner_at_release.store(
                    self.weighting_state
                        .lock()
                        .expect("weighting execution state lock")
                        .has_retained_observation(),
                    Ordering::SeqCst,
                );
                self.weighting_state
                    .lock()
                    .expect("weighting execution state lock")
                    .release(context, &fragment)
                    .map_err(io::Error::other)?;
                self.weighting_released.store(true, Ordering::SeqCst);
                self.weighting_cleanup_released
                    .store(context.is_cleanup(), Ordering::SeqCst);
            } else if let Some(predecessor) =
                context.predecessor_observation_completion(fragment.replay_node())
            {
                let state = self
                    .weighting_state
                    .lock()
                    .expect("weighting execution state lock");
                self.weighting_owner_at_reconciliation
                    .store(state.has_retained_observation(), Ordering::SeqCst);
                let completion = state
                    .replay_completion()
                    .ok_or_else(|| io::Error::other("reconciliation has no weighting replay"))?;
                if predecessor.attempt_id() != completion.attempt_id()
                    || predecessor.owner_node() != completion.owner_node()
                    || predecessor.lease_epoch() != completion.lease_epoch()
                    || predecessor.owner_completion().generation_id()
                        != completion.selected_generation()
                    || predecessor.owner_completion().sample_count() != completion.sample_count()
                {
                    return Err(io::Error::other(
                        "reconciliation received mismatched weighting replay evidence",
                    ));
                }
                self.weighting_reconciled.store(true, Ordering::SeqCst);
            }
        }
        if let Some((expected, accessed)) = &self.initial_consistency_expected {
            if context.node().kind == WorkKind::DataCensus {
                accessed.store(
                    context
                        .observation_consistency()
                        .is_some_and(|transaction| {
                            transaction.transaction_id() == *expected
                                && !transaction.read_set().sources().is_empty()
                        }),
                    Ordering::SeqCst,
                );
            } else if context.observation_consistency().is_some() {
                return Err(io::Error::other(
                    "observation consistency capability escaped its initial check",
                ));
            }
        }
        if context.node().kind == WorkKind::Io
            && context.observation_reads().is_some()
            && let Some(accessed) = &self.generic_source_access
        {
            accessed.store(true, Ordering::SeqCst);
        }
        if context.node().kind == WorkKind::Publication
            && let Some(launched) = &self.publication_launched
        {
            launched.store(true, Ordering::SeqCst);
        }
        let resources = context
            .node()
            .claims
            .iter()
            .map(|claim| {
                ResourceMeasurement::new(
                    claim.resource.clone(),
                    claim.lifetime.clone(),
                    self.resource_peak_overrides
                        .get(&context.node().id)
                        .copied()
                        .unwrap_or(claim.amount),
                )
            })
            .collect();
        let (io, mut artifacts) = self
            .measurements
            .get(&context.node().id)
            .cloned()
            .unwrap_or_else(|| {
                (
                    context
                        .node()
                        .claims
                        .iter()
                        .filter_map(|claim| match claim.resource {
                            LeaseResource::IoBuffer(kind) => {
                                Some(IoMeasurement::new(kind, claim.amount, 1))
                            }
                            _ => None,
                        })
                        .collect(),
                    Vec::new(),
                )
            });
        if context.node().kind == WorkKind::Publication && artifacts.is_empty() {
            let product_count = context.compiled().products().products().len();
            let model_count = context.publication().map_or(0, |transaction| {
                transaction.write_set().model_columns().len()
            });
            artifacts = (0..product_count + model_count)
                .map(|index| {
                    let identity = ArtifactIdentity::from_sha256([34 + index as u8; 32]);
                    ArtifactMeasurement::new(
                        identity,
                        Some(identity),
                        ArtifactDisposition::Staged,
                        1,
                        None,
                    )
                    .expect("publication evidence is externally constructible")
                })
                .collect();
        }
        Ok(WorkMeasurements::new(resources, io, artifacts))
    }

    fn failure_measurements<'error>(
        &'error self,
        _error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        None
    }

    fn wait_for_fence(
        &self,
        context: WorkExecutionContext<'_>,
        fence: FenceKind,
    ) -> Result<(), Self::Error> {
        self.fence_waits.fetch_add(1, Ordering::SeqCst);
        if let Some(message) = self.fence_failure
            && self.fail_only_fence.is_none_or(|kind| kind == fence)
        {
            return Err(io::Error::other(message));
        }
        if self.fence_failure_event == Some((context.node().id.as_str(), fence)) {
            return Err(io::Error::other("stateful transaction fence failure"));
        }
        if self
            .weighting_fence_failure_event
            .as_ref()
            .is_some_and(|(node, kind)| node == &context.node().id && *kind == fence)
        {
            return Err(io::Error::other("weighting lifecycle fence failure"));
        }
        if self
            .visible_generation
            .as_ref()
            .is_some_and(|visible| visible.load(Ordering::SeqCst) == 1)
            && let Some(observed) = &self.visibility_during_fence_settlement
        {
            observed.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn complete_observation_read(
        &self,
        completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        if let Some(completions) = &self.observation_completions {
            completions
                .lock()
                .expect("observation completion lock")
                .push(RecordedObservationCompletion {
                    attempt_id: completion.attempt_id(),
                    owner_node: completion.owner_node().clone(),
                    settled_fences: completion.settled_fences().clone(),
                    lease_epoch: completion.lease_epoch(),
                });
        }
        if let Some(message) = self.observation_completion_failure {
            return Err(io::Error::other(message));
        }
        if let Some(fragment) = self.weighting_fragment() {
            if completion.owner_node() == fragment.generation_node() {
                return self
                    .weighting_state
                    .lock()
                    .expect("weighting execution state lock")
                    .complete_generation(completion)
                    .map_err(io::Error::other);
            }
            if completion.owner_node() == fragment.replay_node() {
                let predecessor = {
                    let mut state = self
                        .weighting_state
                        .lock()
                        .expect("weighting execution state lock");
                    let predecessor = state
                        .complete_replay(completion)
                        .map_err(io::Error::other)?;
                    self.weighting_owner_at_replay_fence
                        .store(state.has_retained_observation(), Ordering::SeqCst);
                    predecessor
                };
                return Ok(predecessor);
            }
        }
        let owner_completion = self
            .selected_observation_completion
            .lock()
            .expect("selected-observation completion lock")
            .take()
            .ok_or_else(|| io::Error::other("ObservationRead produced no scientific completion"))?;
        completion.bind(owner_completion).map_err(io::Error::other)
    }

    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        if context.node().kind != WorkKind::Publication || context.publication().is_none() {
            return Err(io::Error::other(
                "publication requires the transaction-bound Publication node",
            ));
        }
        if let Some(probe) = &self.publication_probe {
            probe.publication_calls.fetch_add(1, Ordering::SeqCst);
            let receipt = probe
                .receipts
                .open(probe.attempt)
                .map_err(io::Error::other)?;
            let prepared = receipt.status() == ReceiptStatus::PublicationPrepared
                && receipt.artifact_identities().into_iter().all(|artifact| {
                    receipt.artifact_role(artifact) != Some(ArtifactRole::Output)
                        || receipt.artifact_disposition(artifact)
                            == Some(ArtifactDisposition::Staged)
                });
            probe.prepared_observed.store(prepared, Ordering::SeqCst);
            if !prepared {
                return Err(io::Error::other(
                    "publication became callable before durable receipt preparation",
                ));
            }
        }
        if let Some(message) = self.publication_failure {
            return Err(io::Error::other(message));
        }
        if let Some(pause) = &self.publication_pause {
            pause.entered.store(true, Ordering::SeqCst);
            let mut release = pause.release.lock().expect("publication pause lock");
            while !*release {
                release = pause
                    .released
                    .wait(release)
                    .expect("publication pause wait");
            }
        }
        if let Some(observed) = &self.publication_buffer_held {
            let allocation = AllocationId::new("transaction-publication-buffer");
            let slot = PhysicalSlotId::new("transaction-publication-slot");
            observed.store(
                context.publication_resources().is_some_and(|resources| {
                    resources.lease_epoch() > 0
                        && resources.allocation_slot(&allocation) == Some(&slot)
                }),
                Ordering::SeqCst,
            );
        }
        if let Some(visible) = &self.visible_generation {
            visible.store(1, Ordering::SeqCst);
        }
        if let Some(root) = &self.receipt_root_to_disrupt {
            for entry in fs::read_dir(root)? {
                let path = entry?.path();
                if path.extension().is_none_or(|extension| extension != "json") {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(())
    }
}

fn publication_recording_executor(
    byte: u8,
    launched: Arc<AtomicBool>,
    visible_generation: Arc<AtomicUsize>,
) -> RecordingExecutor {
    let mut executor = recording_executor(byte, None, None);
    executor.publication_launched = Some(launched);
    executor.visible_generation = Some(visible_generation);
    executor
}

fn failing_transaction_executor(
    byte: u8,
    visible_generation: Arc<AtomicUsize>,
    failure_node: Option<&'static str>,
    fence_failure_event: Option<(&'static str, FenceKind)>,
    publication_failure: Option<&'static str>,
) -> RecordingExecutor {
    let mut executor = recording_executor(byte, None, None);
    executor.visible_generation = Some(visible_generation);
    executor.failure_node = failure_node;
    executor.fence_failure_event = fence_failure_event;
    executor.publication_failure = publication_failure;
    executor
}

fn physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("default physical-work problem");
    physical_work_with_transaction_staging(
        &problem,
        implementation_byte,
        product_participants(&problem),
        false,
        false,
        true,
    )
}

fn physical_work_with_synchronous_observation_read(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("synchronous observation-read problem");
    physical_work_with_transaction_staging(
        &problem,
        implementation_byte,
        product_participants(&problem),
        false,
        false,
        false,
    )
}

#[test]
fn production_weighting_fragment_owns_generation_replay_and_release_lifetimes() {
    let problem = compile(request(1)).expect("logical weighting compilation");
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 3).expect("weighting limits"),
    )
    .expect("weighting plan");
    let base = physical_work_for_weighting_problem(&problem, 6);
    let fragment = WeightingPlanFragment::new(
        &plan,
        WorkNodeId::new("transaction-read"),
        selected_content_resources(&problem),
        implementation(6),
        implementation(6),
        implementation(6),
    );
    let generation = fragment.generation_node().clone();
    let replay = fragment.replay_node().clone();
    let release = fragment.release_node().clone();
    let frozen = fragment.frozen_allocation().clone();
    let composed = fragment
        .compose(&base)
        .expect("production weighting fragment");
    let dag = composed.execution_dag();
    let source = WorkNodeId::new("transaction-read");
    let source_buffer = AllocationId::new("selected-observation-source-buffer");
    let source_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let retained_lifetime = ClaimLifetime::retained_until(release.clone());
    let retained_resources = dag.nodes()[&source]
        .claims
        .iter()
        .filter(|claim| {
            matches!(
                claim.resource,
                LeaseResource::MeasurementSetLock { .. } | LeaseResource::FileDescriptors
            )
        })
        .map(|claim| (claim.resource.clone(), claim.amount))
        .collect::<BTreeSet<_>>();

    assert_eq!(dag.nodes()[&generation].kind, WorkKind::ObservationRead);
    assert_eq!(dag.nodes()[&replay].kind, WorkKind::ObservationRead);
    assert_eq!(dag.nodes()[&release].kind, WorkKind::Release);
    assert_eq!(
        retained_resources.len(),
        2,
        "the test source must exercise both retained handle classes"
    );
    assert_eq!(
        dag.logical_allocations().len() - base.execution_dag().logical_allocations().len(),
        5
    );
    assert_eq!(
        dag.physical_slots().len() - base.execution_dag().physical_slots().len(),
        5
    );
    assert_eq!(
        dag.logical_allocations()[&frozen].lifetime.release_after,
        BTreeSet::from([WorkDependency::Work(release.clone())])
    );
    assert_eq!(
        dag.logical_allocations()[&frozen].lifetime.acquire_at,
        source
    );
    assert!(
        dag.nodes()[&source]
            .allocations
            .iter()
            .any(|usage| usage.allocation == frozen)
    );
    assert_eq!(
        dag.logical_allocations()[&source_buffer]
            .lifetime
            .release_after,
        BTreeSet::from([WorkDependency::Work(release.clone())])
    );
    for node in [&generation, &replay] {
        assert!(dag.nodes()[node].claims.iter().any(|claim| {
            claim.resource == LeaseResource::Workers
                && claim.amount == 1
                && claim.lifetime == ClaimLifetime::Work
        }));
        assert!(dag.nodes()[node].claims.iter().any(|claim| {
            claim.resource == LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
                && claim.amount == SELECTED_CONTENT_BYTES as u64
                && claim.lifetime == source_lifetime
        }));
        assert!(dag.nodes()[node].claims.iter().any(|claim| {
            matches!(claim.resource, LeaseResource::Queue { .. })
                && claim.amount == 1
                && claim.lifetime == source_lifetime
        }));
        assert!(dag.nodes()[node].allocations.iter().any(|usage| {
            usage.allocation == source_buffer && usage.lifetime == source_lifetime
        }));
    }
    for node in [&source, &generation, &replay, &release] {
        for (resource, amount) in &retained_resources {
            assert!(dag.nodes()[node].claims.iter().any(|claim| {
                &claim.resource == resource
                    && claim.amount == *amount
                    && claim.lifetime == retained_lifetime
            }));
        }
    }
    assert!(dag.nodes()[&release].allocations.iter().any(|usage| {
        usage.allocation == source_buffer && usage.lifetime == ClaimLifetime::Work
    }));
    assert!(dag.nodes()[&release].claims.iter().any(|claim| {
        claim.resource == LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
            && claim.amount == SELECTED_CONTENT_BYTES as u64
            && claim.lifetime == ClaimLifetime::Work
    }));
    assert!(dag.nodes()[&release].claims.iter().all(|claim| {
        matches!(claim.resource, LeaseResource::Workers)
            || claim.resource == LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead)
            || retained_resources
                .iter()
                .any(|(resource, _)| resource == &claim.resource)
    }));
    assert!(
        dag.nodes()[&replay]
            .dependencies
            .contains(&WorkDependency::Fence(FenceId::new(
                generation,
                FenceKind::Io,
            )))
    );
    assert!(
        dag.nodes()[base.observation_transaction().final_reconciliation()]
            .dependencies
            .contains(&WorkDependency::Fence(FenceId::new(replay, FenceKind::Io,)))
    );
    assert!(
        dag.nodes()[&release]
            .dependencies
            .contains(&WorkDependency::Work(
                base.observation_transaction()
                    .final_reconciliation()
                    .clone(),
            ))
    );
    assert!(
        dag.nodes()[base.observation_transaction().commit()]
            .dependencies
            .contains(&WorkDependency::Work(release))
    );
}

#[test]
fn production_weighting_fragment_rejects_unplanned_source_traversal_resources() {
    let problem = compile(request(1)).expect("logical weighting compilation");
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 3).expect("weighting limits"),
    )
    .expect("weighting plan");
    let fragment = WeightingPlanFragment::new(
        &plan,
        WorkNodeId::new("transaction-read"),
        selected_content_resources(&problem),
        implementation(6),
        implementation(6),
        implementation(6),
    );

    let error = fragment
        .compose(&physical_work_with_synchronous_observation_read(6))
        .expect_err("an unplanned worker and source buffer must fail closed");

    assert!(matches!(
        error,
        casa_imaging_runtime::WeightingPlanFragmentError::InvalidSourceAuthority { .. }
    ));
}

#[test]
fn production_weighting_fragment_rejects_fungible_unrelated_queues() {
    let problem = compile(request(1)).expect("logical weighting compilation");
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 3).expect("weighting limits"),
    )
    .expect("weighting plan");
    let base = physical_work_for_weighting_problem(&problem, 6);
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("transaction-read"))
        .expect("transaction source read")
        .claims
        .push(ResourceClaim {
            resource: LeaseResource::Queue {
                demand_id: "unrelated-queue".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        });
    let mut alternative = base.execution_dag().resource_alternative().clone();
    alternative.demand.queues.push(QueueDemand {
        demand_id: "unrelated-queue".to_string(),
        resource: QueueResourceId::new("unrelated-queue"),
        slots: CountDemand::new(1, 1),
    });
    let split_queue_dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base
            .execution_dag()
            .required_resource_capabilities()
            .clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations: base
            .execution_dag()
            .logical_allocations()
            .values()
            .cloned()
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .collect(),
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })
    .expect("two distinct queue identities are structurally valid");
    let split_queue_base = PhysicalWorkBinding::new(
        implementation_catalog(&problem, &split_queue_dag),
        split_queue_dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("split-queue physical work");
    let fragment = WeightingPlanFragment::new(
        &plan,
        WorkNodeId::new("transaction-read"),
        SelectedObservationSourceResources::new(
            selected_content_residency_with(&problem, |_| {
                SelectedObservationContentBudget::new(SELECTED_CONTENT_BYTES, 2, 4)
            }),
            selected_content_allocations(),
            selected_content_queue(),
        ),
        implementation(6),
        implementation(6),
        implementation(6),
    );

    let error = fragment
        .compose(&split_queue_base)
        .expect_err("unrelated queue capacity cannot satisfy the selected-content queue");

    assert!(matches!(
        error,
        casa_imaging_runtime::WeightingPlanFragmentError::InvalidSourceAuthority { .. }
    ));
}

fn product_participants(
    problem: &casa_imaging_model::CompiledProblem,
) -> Vec<PublicationParticipant> {
    let graph_id = problem.product_graph().graph_id();
    problem
        .product_graph()
        .publication()
        .members()
        .iter()
        .copied()
        .map(|node_id| PublicationParticipant::Product { graph_id, node_id })
        .collect()
}

fn default_product_participants() -> Vec<PublicationParticipant> {
    product_participants(&compile(request(1)).expect("default physical-work problem"))
}

fn physical_work_for_problem(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let graph_id = problem.product_graph().graph_id();
    let participants = problem
        .product_graph()
        .publication()
        .members()
        .iter()
        .copied()
        .map(|node_id| PublicationParticipant::Product { graph_id, node_id })
        .chain(
            problem
                .observation_transaction()
                .write_set()
                .model_columns()
                .iter()
                .map(|write| PublicationParticipant::ModelData(write.measurement_set())),
        )
        .collect();
    let base = physical_work_with_transaction_staging(
        problem,
        implementation_byte,
        participants,
        !problem
            .observation_transaction()
            .write_set()
            .model_columns()
            .is_empty(),
        false,
        true,
    );
    let measurement_sets = problem
        .observation_transaction()
        .read_set()
        .sources()
        .iter()
        .map(|source| source.measurement_set())
        .collect::<Vec<_>>();
    assert!(
        !measurement_sets.is_empty(),
        "compiled read set is non-empty"
    );
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for node in &mut nodes {
        let mut claims = Vec::with_capacity(
            node.claims
                .len()
                .saturating_add(measurement_sets.len().saturating_sub(1)),
        );
        for claim in std::mem::take(&mut node.claims) {
            if matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }) {
                claims.extend(measurement_sets.iter().copied().map(|measurement_set| {
                    ResourceClaim {
                        resource: LeaseResource::MeasurementSetLock { measurement_set },
                        amount: claim.amount,
                        lifetime: claim.lifetime.clone(),
                    }
                }));
            } else {
                claims.push(claim);
            }
        }
        node.claims = claims;
    }
    let mut alternative = base.execution_dag().resource_alternative().clone();
    let lock_count = u64::try_from(measurement_sets.len()).expect("test lock count fits u64");
    alternative.demand.locks = CountDemand::new(lock_count, lock_count);
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base
            .execution_dag()
            .required_resource_capabilities()
            .clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations: base
            .execution_dag()
            .logical_allocations()
            .values()
            .cloned()
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .collect(),
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })
    .expect("problem-bound transaction DAG");
    PhysicalWorkBinding::new(
        implementation_catalog(problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("problem-bound physical work")
}

fn physical_work_for_weighting_problem(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let residency = selected_content_residency(problem);
    physical_work_for_weighting_problem_with_residency(problem, implementation_byte, &residency)
}

fn physical_work_for_weighting_problem_with_residency(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
    residency: &SelectedObservationResidencyCertificate,
) -> PhysicalWorkBinding {
    let base = physical_work_for_problem(problem, implementation_byte);
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let source_buffer = AllocationId::new("selected-observation-source-buffer");
    let source_slot = PhysicalSlotId::new("selected-observation-source-slot");
    let source_bytes = u64::try_from(residency.aggregate_resident_bytes())
        .expect("test source residency fits u64");
    let source_blocks =
        u64::try_from(residency.peak_live_blocks()).expect("test source queue depth fits u64");
    let source_count = u64::try_from(problem.inputs().observation_snapshot().sources().len())
        .expect("test source count fits u64");
    let source_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let source_compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("selected-observation-source-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let source_read = nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("transaction-read"))
        .expect("transaction source read");
    source_read
        .claims
        .iter_mut()
        .filter(|claim| claim.resource == selected_content_queue())
        .for_each(|claim| claim.amount = source_blocks);
    source_read.claims.extend([
        ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead),
            amount: source_bytes,
            lifetime: source_lifetime.clone(),
        },
        ResourceClaim {
            resource: LeaseResource::FileDescriptors,
            amount: source_count,
            lifetime: source_lifetime.clone(),
        },
    ]);
    source_read.allocations.push(AllocationUse {
        allocation: source_buffer.clone(),
        lifetime: source_lifetime,
    });
    let mut alternative = base.execution_dag().resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "selected-observation-source-memory".to_string(),
        hard_bytes: source_bytes,
        preferred_bytes: source_bytes,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.io_buffers.source_read_ahead_bytes = source_bytes;
    alternative.demand.file_descriptors = CountDemand::new(source_count, source_count);
    for demand in &mut alternative.demand.queues {
        if demand.demand_id == "transaction-io-queue" {
            demand.slots = CountDemand::new(source_blocks, source_blocks);
        }
    }
    let mut logical_allocations = base
        .execution_dag()
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    logical_allocations.push(LogicalAllocation {
        id: source_buffer.clone(),
        bytes: source_bytes,
        purpose: AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead),
        compatibility: source_compatibility.clone(),
        physical_slot: source_slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: WorkNodeId::new("transaction-read"),
            release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                WorkNodeId::new("transaction-read"),
                FenceKind::Io,
            ))]),
        },
    });
    let mut physical_slots = base
        .execution_dag()
        .physical_slots()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    physical_slots.push(PhysicalSlot {
        id: source_slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "selected-observation-source-memory".to_string(),
        },
        capacity_bytes: source_bytes,
        compatibility: source_compatibility,
    });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base
            .execution_dag()
            .required_resource_capabilities()
            .clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations,
        physical_slots,
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })
    .expect("problem-bound transaction DAG");
    let prediction = PlanPrediction::new(
        base.prediction().elapsed_nanos(),
        base.prediction().confidence(),
        base.prediction().uncertainty().to_vec(),
        base.prediction()
            .stages()
            .values()
            .map(|stage| {
                if stage.node() == &WorkNodeId::new("transaction-read") {
                    StagePrediction::new(stage.node().clone(), stage.elapsed_nanos()).with_io(
                        stage
                            .io()
                            .iter()
                            .copied()
                            .chain([IoPrediction::new(
                                IoBufferKind::SourceReadAhead,
                                source_bytes,
                                1,
                            )])
                            .collect(),
                    )
                } else {
                    stage.clone()
                }
            })
            .collect(),
    )
    .expect("problem-bound source prediction");
    PhysicalWorkBinding::new(
        implementation_catalog(problem, &dag),
        dag,
        prediction,
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("problem-bound physical work")
}

fn with_work_implementation(
    problem: &casa_imaging_model::CompiledProblem,
    base: &PhysicalWorkBinding,
    implementation: WorkImplementationId,
) -> PhysicalWorkBinding {
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base
            .execution_dag()
            .required_resource_capabilities()
            .clone(),
        resource_alternative: base.execution_dag().resource_alternative().clone(),
        nodes: base
            .execution_dag()
            .nodes()
            .values()
            .cloned()
            .map(|mut node| {
                node.implementation = implementation.clone();
                node
            })
            .collect(),
        logical_allocations: base
            .execution_dag()
            .logical_allocations()
            .values()
            .cloned()
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .collect(),
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })
    .expect("rebinding a validated DAG preserves its topology");
    PhysicalWorkBinding::new(
        implementation_catalog(problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("rebinding a validated physical plan preserves its contracts")
}

fn physical_work_with_product_staging(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
    participants: Vec<PublicationParticipant>,
) -> PhysicalWorkBinding {
    physical_work_with_transaction_staging(
        problem,
        implementation_byte,
        participants,
        false,
        false,
        true,
    )
}

fn physical_work_with_model_staging(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request_with_model_write(1)).expect("model-write physical-work problem");
    physical_work_for_problem(&problem, implementation_byte)
}

fn physical_work_with_early_publication_buffer(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("early-publication physical-work problem");
    let graph_id = problem.product_graph().graph_id();
    physical_work_with_transaction_staging(
        &problem,
        implementation_byte,
        problem
            .product_graph()
            .publication()
            .members()
            .iter()
            .copied()
            .map(|node_id| PublicationParticipant::Product { graph_id, node_id })
            .collect(),
        false,
        true,
        true,
    )
}

fn physical_work_with_transaction_staging(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
    participants: Vec<PublicationParticipant>,
    include_model_staging: bool,
    acquire_publication_early: bool,
    fenced_observation_read: bool,
) -> PhysicalWorkBinding {
    let work_implementation = implementation(implementation_byte);
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("test-cpu"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: Vec::new(),
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: Vec::new(),
                rates: vec![RateDemand {
                    demand_id: "io-rate".to_string(),
                    resource: RateResourceId::new("io-rate"),
                    amount: CountDemand::new(1, 1),
                }],
                caches: CacheDemand::zero(),
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: vec![QueueDemand {
                    demand_id: "io-queue".to_string(),
                    resource: QueueResourceId::new("io-queue"),
                    slots: CountDemand::new(1, 1),
                }],
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand::zero(),
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: 1,
                maximum_batch_size: 1,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
        },
        nodes: vec![
            WorkNode {
                id: WorkNodeId::new("read"),
                kind: WorkKind::Io,
                domain: WorkDomain::Io,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::new(),
                claims: vec![
                    ResourceClaim {
                        resource: LeaseResource::Rate {
                            demand_id: "io-rate".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: LeaseResource::Queue {
                            demand_id: "io-queue".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                ],
                allocations: Vec::new(),
                fences: BTreeSet::from([FenceKind::Io]),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: WorkNodeId::new("execute"),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: work_implementation,
                dependencies: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("read"),
                    FenceKind::Io,
                ))]),
                claims: vec![ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                }],
                allocations: Vec::new(),
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
        ],
        logical_allocations: Vec::new(),
        physical_slots: Vec::new(),
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: Vec::new(),
    };
    transaction_binding(
        problem,
        specification,
        implementation(implementation_byte),
        participants,
        include_model_staging,
        acquire_publication_early,
        fenced_observation_read,
    )
}

fn transaction_binding(
    problem: &casa_imaging_model::CompiledProblem,
    mut specification: ExecutionDagSpecification,
    work_implementation: WorkImplementationId,
    participants: Vec<PublicationParticipant>,
    include_model_staging: bool,
    acquire_publication_early: bool,
    fenced_observation_read: bool,
) -> PhysicalWorkBinding {
    let product_count = participants
        .iter()
        .filter(|participant| matches!(participant, PublicationParticipant::Product { .. }))
        .count() as u64;
    let member_count = participants.len() as u64;
    let initial = WorkNodeId::new("transaction-check");
    let read = WorkNodeId::new("transaction-read");
    let reconciliation = WorkNodeId::new("transaction-reconciliation");
    let product = WorkNodeId::new("transaction-stage-psf");
    let model = WorkNodeId::new("transaction-stage-model");
    let commit = WorkNodeId::new("transaction-commit");
    let publication_allocation = AllocationId::new("transaction-publication-buffer");
    let publication_slot = PhysicalSlotId::new("transaction-publication-slot");
    let commit_allocation = AllocationId::new("transaction-commit-buffer");
    let commit_slot = PhysicalSlotId::new("transaction-commit-slot");
    let observation_read_lifetime = if fenced_observation_read {
        ClaimLifetime::through_fence(FenceKind::Io)
    } else {
        ClaimLifetime::Work
    };
    let observation_read_fences = if fenced_observation_read {
        BTreeSet::from([FenceKind::Io])
    } else {
        BTreeSet::new()
    };
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let publication_compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 1,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("transaction-publication-buffer"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
    };
    let product_writer_allocation = AllocationId::new("transaction-product-writer-buffer");
    let product_writer_slot = PhysicalSlotId::new("transaction-product-writer-slot");
    let product_writer_compatibility = SlotCompatibility {
        layout: AllocationLayout::new("transaction-product-writer-buffer"),
        ..publication_compatibility.clone()
    };
    let commit_compatibility = SlotCompatibility {
        layout: AllocationLayout::new("transaction-commit-buffer"),
        ..publication_compatibility.clone()
    };
    let writeback_allocation = AllocationId::new("transaction-writeback-buffer");
    let writeback_slot = PhysicalSlotId::new("transaction-writeback-slot");
    let writeback_lifetime = ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Writeback]);
    let writeback_compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 1,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("transaction-writeback-buffer"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
    };

    let predecessors = specification
        .nodes
        .iter()
        .flat_map(|node| {
            node.dependencies.iter().map(|dependency| match dependency {
                WorkDependency::Work(node) => node,
                WorkDependency::Fence(fence) => fence.node(),
            })
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let terminals = specification
        .nodes
        .iter()
        .filter(|node| !predecessors.contains(&node.id))
        .flat_map(|node| {
            if node.fences.is_empty() {
                vec![WorkDependency::Work(node.id.clone())]
            } else {
                node.fences
                    .iter()
                    .map(|fence| WorkDependency::Fence(FenceId::new(node.id.clone(), *fence)))
                    .collect()
            }
        })
        .collect::<BTreeSet<_>>();
    let read_completion = if fenced_observation_read {
        WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io))
    } else {
        WorkDependency::Work(read.clone())
    };
    for node in specification
        .nodes
        .iter_mut()
        .filter(|node| node.dependencies.is_empty())
    {
        node.dependencies.insert(read_completion.clone());
    }

    specification
        .resource_alternative
        .demand
        .memory
        .push(MemoryDemand {
            allocation_id: "transaction-publication-slot".to_string(),
            hard_bytes: 1,
            preferred_bytes: 1,
            views: vec![CapacityViewId::new("host-memory")],
        });
    specification
        .resource_alternative
        .demand
        .memory
        .push(MemoryDemand {
            allocation_id: "transaction-product-writer-slot".to_string(),
            hard_bytes: product_count,
            preferred_bytes: product_count,
            views: vec![CapacityViewId::new("host-memory")],
        });
    if acquire_publication_early {
        specification
            .resource_alternative
            .demand
            .memory
            .push(MemoryDemand {
                allocation_id: "transaction-commit-slot".to_string(),
                hard_bytes: 1,
                preferred_bytes: 1,
                views: vec![CapacityViewId::new("host-memory")],
            });
    }
    if include_model_staging {
        specification
            .resource_alternative
            .demand
            .memory
            .push(MemoryDemand {
                allocation_id: "transaction-writeback-slot".to_string(),
                hard_bytes: 1,
                preferred_bytes: 1,
                views: vec![CapacityViewId::new("host-memory")],
            });
    }
    specification
        .resource_alternative
        .demand
        .storage
        .push(casa_imaging_runtime::StorageDemand {
            demand_id: "transaction-output".to_string(),
            domain: casa_imaging_runtime::StorageDomainId::new("atomic-output"),
            temporary_bytes: 0,
            staged_output_bytes: member_count,
            final_output_bytes: member_count,
            persistent_cache_bytes: 0,
            read_rate: CountDemand::zero(),
            write_rate: CountDemand::zero(),
            operations_rate: CountDemand::zero(),
            queue_slots: CountDemand::zero(),
        });
    specification
        .resource_alternative
        .demand
        .rates
        .push(RateDemand {
            demand_id: "transaction-io-rate".to_string(),
            resource: RateResourceId::new("transaction-io-rate"),
            amount: CountDemand::new(1, 1),
        });
    specification
        .resource_alternative
        .demand
        .queues
        .push(QueueDemand {
            demand_id: "transaction-io-queue".to_string(),
            resource: QueueResourceId::new("transaction-io-queue"),
            slots: CountDemand::new(1, 1),
        });
    specification.resource_alternative.demand.locks = CountDemand::new(1, 1);
    specification
        .resource_alternative
        .demand
        .io_buffers
        .serialization_bytes = product_count;
    specification
        .resource_alternative
        .demand
        .io_buffers
        .publication_bytes = 1;
    if include_model_staging {
        specification
            .resource_alternative
            .demand
            .io_buffers
            .writeback_bytes = 1;
    }

    let mut commit_dependencies = BTreeSet::from([WorkDependency::Work(product.clone())]);
    if include_model_staging {
        commit_dependencies.extend([
            WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Io)),
            WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Writeback)),
        ]);
    }
    let product_claims = vec![
        ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::Storage {
                demand_id: "transaction-output".to_string(),
                use_kind: casa_imaging_runtime::StorageUseKind::StagedOutput,
            },
            amount: product_count,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::IoBuffer(IoBufferKind::Serialization),
            amount: product_count,
            lifetime: ClaimLifetime::Work,
        },
    ];
    let mut product_allocations = vec![AllocationUse {
        allocation: product_writer_allocation.clone(),
        lifetime: ClaimLifetime::Work,
    }];
    if acquire_publication_early {
        product_allocations.push(AllocationUse {
            allocation: publication_allocation.clone(),
            lifetime: ClaimLifetime::Work,
        });
    }

    specification.nodes.extend([
        WorkNode {
            id: initial.clone(),
            kind: WorkKind::DataCensus,
            domain: WorkDomain::Cpu,
            implementation: work_implementation.clone(),
            dependencies: BTreeSet::new(),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: LeaseResource::MeasurementSetLock {
                        measurement_set: casa_imaging_model::MeasurementSetIdentity::new(identity(
                            1,
                        )),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: Vec::new(),
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: read.clone(),
            kind: WorkKind::ObservationRead,
            domain: WorkDomain::Io,
            implementation: work_implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(initial.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: observation_read_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: observation_read_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::MeasurementSetLock {
                        measurement_set: casa_imaging_model::MeasurementSetIdentity::new(identity(
                            1,
                        )),
                    },
                    amount: 1,
                    lifetime: observation_read_lifetime,
                },
            ],
            allocations: Vec::new(),
            fences: observation_read_fences,
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: reconciliation.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: work_implementation.clone(),
            dependencies: terminals,
            claims: vec![ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }],
            allocations: Vec::new(),
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: product.clone(),
            kind: WorkKind::Serialization,
            domain: WorkDomain::Cpu,
            implementation: work_implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
            claims: product_claims,
            allocations: product_allocations,
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: commit.clone(),
            kind: WorkKind::Publication,
            domain: WorkDomain::Io,
            implementation: work_implementation.clone(),
            dependencies: commit_dependencies,
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::MeasurementSetLock {
                        measurement_set: casa_imaging_model::MeasurementSetIdentity::new(identity(
                            1,
                        )),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: casa_imaging_runtime::StorageUseKind::StagedOutput,
                    },
                    amount: member_count,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: casa_imaging_runtime::StorageUseKind::FinalOutput,
                    },
                    amount: member_count,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
            ],
            allocations: if acquire_publication_early {
                vec![
                    AllocationUse {
                        allocation: publication_allocation.clone(),
                        lifetime: publication_lifetime.clone(),
                    },
                    AllocationUse {
                        allocation: commit_allocation.clone(),
                        lifetime: publication_lifetime.clone(),
                    },
                ]
            } else {
                vec![AllocationUse {
                    allocation: publication_allocation.clone(),
                    lifetime: publication_lifetime.clone(),
                }]
            },
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
            quiescence_after: BTreeSet::new(),
        },
    ]);
    if include_model_staging {
        specification.nodes.push(WorkNode {
            id: model.clone(),
            kind: WorkKind::Writeback,
            domain: WorkDomain::Io,
            implementation: work_implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: casa_imaging_runtime::StorageUseKind::StagedOutput,
                    },
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Writeback),
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: writeback_allocation.clone(),
                lifetime: writeback_lifetime,
            }],
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Writeback]),
            quiescence_after: BTreeSet::new(),
        });
    }
    specification.logical_allocations.push(LogicalAllocation {
        id: product_writer_allocation.clone(),
        bytes: product_count,
        purpose: AllocationPurpose::IoBuffer(IoBufferKind::Serialization),
        compatibility: product_writer_compatibility.clone(),
        physical_slot: product_writer_slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: product.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(product.clone())]),
        },
    });
    specification.physical_slots.push(PhysicalSlot {
        id: product_writer_slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "transaction-product-writer-slot".to_string(),
        },
        capacity_bytes: product_count,
        compatibility: product_writer_compatibility,
    });
    specification.logical_allocations.push(LogicalAllocation {
        id: publication_allocation,
        bytes: 1,
        purpose: if acquire_publication_early {
            AllocationPurpose::Data
        } else {
            AllocationPurpose::IoBuffer(IoBufferKind::Publication)
        },
        compatibility: publication_compatibility.clone(),
        physical_slot: publication_slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: if acquire_publication_early {
                product.clone()
            } else {
                commit.clone()
            },
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
            ]),
        },
    });
    specification.physical_slots.push(PhysicalSlot {
        id: publication_slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "transaction-publication-slot".to_string(),
        },
        capacity_bytes: 1,
        compatibility: publication_compatibility,
    });
    if acquire_publication_early {
        specification.logical_allocations.push(LogicalAllocation {
            id: commit_allocation,
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
            compatibility: commit_compatibility.clone(),
            physical_slot: commit_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: commit.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
                ]),
            },
        });
        specification.physical_slots.push(PhysicalSlot {
            id: commit_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "transaction-commit-slot".to_string(),
            },
            capacity_bytes: 1,
            compatibility: commit_compatibility,
        });
    }
    if include_model_staging {
        specification.logical_allocations.push(LogicalAllocation {
            id: writeback_allocation.clone(),
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Writeback),
            compatibility: writeback_compatibility.clone(),
            physical_slot: writeback_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: model.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Writeback)),
                ]),
            },
        });
        specification.physical_slots.push(PhysicalSlot {
            id: writeback_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "transaction-writeback-slot".to_string(),
            },
            capacity_bytes: 1,
            compatibility: writeback_compatibility,
        });
    }

    let dag = ExecutionDag::new(specification).expect("valid transaction-bound physical work");
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let io = node
                .claims
                .iter()
                .filter_map(|claim| match claim.resource {
                    LeaseResource::IoBuffer(kind) => Some(IoPrediction::new(kind, claim.amount, 1)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let stage = StagePrediction::new(node.id.clone(), 100);
            if io.is_empty() {
                stage
            } else {
                stage.with_io(io)
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete transaction prediction");
    PhysicalWorkBinding::new(
        implementation_catalog(problem, &dag),
        dag,
        prediction,
        participants
            .iter()
            .enumerate()
            .map(|(index, _)| {
                PlannedArtifact::new(
                    ArtifactIdentity::from_sha256([34 + index as u8; 32]),
                    commit.clone(),
                    ArtifactRole::Output,
                    None,
                )
            })
            .collect(),
        ObservationTransactionWork::new(
            initial,
            reconciliation,
            include_model_staging.then_some(model.clone()),
            commit.clone(),
        ),
        PublicationLayoutLedger::new(
            participants
                .into_iter()
                .enumerate()
                .map(|(index, participant)| {
                    let (producer, terminal, kind, allocation) = match participant {
                        PublicationParticipant::Product { .. } => (
                            product.clone(),
                            WorkDependency::Work(product.clone()),
                            IoBufferKind::Serialization,
                            product_writer_allocation.clone(),
                        ),
                        PublicationParticipant::ModelData(_) => (
                            model.clone(),
                            WorkDependency::Fence(FenceId::new(
                                model.clone(),
                                FenceKind::Writeback,
                            )),
                            IoBufferKind::Writeback,
                            writeback_allocation.clone(),
                        ),
                    };
                    PublicationPhysicalLayout::new(
                        participant,
                        ArtifactIdentity::from_sha256([34 + index as u8; 32]),
                        PhysicalLayoutId::from_sha256([150 + index as u8; 32]),
                        PublicationStaging::new(producer, terminal, kind, allocation)
                            .expect("valid publication staging"),
                        PublicationResourceBounds::new(1, 1, 1, 0)
                            .expect("valid publication bounds"),
                    )
                })
                .collect(),
        )
        .expect("complete publication layout ledger"),
    )
    .expect("bound transaction physical work")
}

fn evidenced_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("evidenced physical-work problem");
    let base = physical_work(implementation_byte);
    let base_dag = base.execution_dag();
    let read = WorkNodeId::new("read");
    let publish = WorkNodeId::new("transaction-commit");
    let source_buffer = AllocationId::new("source-read-ahead-buffer");
    let source_slot = PhysicalSlotId::new("source-read-ahead-slot");
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("source-read-ahead-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut alternative = base_dag.resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "source-read-ahead-memory".to_string(),
        hard_bytes: 32,
        preferred_bytes: 32,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.io_buffers.source_read_ahead_bytes = 32;
    let mut nodes = base_dag.nodes().values().cloned().collect::<Vec<_>>();
    let read_node = nodes
        .iter_mut()
        .find(|node| node.id == read)
        .expect("source read node");
    read_node.kind = WorkKind::Prefetch;
    read_node.claims.push(ResourceClaim {
        resource: LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead),
        amount: 32,
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    });
    read_node.allocations.push(AllocationUse {
        allocation: source_buffer.clone(),
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    });
    let mut logical_allocations = base_dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    logical_allocations.push(LogicalAllocation {
        id: source_buffer,
        bytes: 32,
        purpose: AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead),
        compatibility: compatibility.clone(),
        physical_slot: source_slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: read.clone(),
            release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                read.clone(),
                FenceKind::Io,
            ))]),
        },
    });
    let mut physical_slots = base_dag
        .physical_slots()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    physical_slots.push(PhysicalSlot {
        id: source_slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "source-read-ahead-memory".to_string(),
        },
        capacity_bytes: 32,
        compatibility,
    });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base_dag.required_resource_capabilities().clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations,
        physical_slots,
        initial_knobs: base_dag.initial_knobs().clone(),
        adaptations: base_dag.adaptations().values().cloned().collect(),
    })
    .expect("valid evidenced transaction DAG");
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let stage = StagePrediction::new(node.id.clone(), 100);
            if node.id == read {
                stage.with_io(vec![IoPrediction::new(
                    IoBufferKind::SourceReadAhead,
                    8_192,
                    4,
                )])
            } else if node.id == publish {
                stage.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 2_048, 1)])
            } else {
                let io = node
                    .claims
                    .iter()
                    .filter_map(|claim| match claim.resource {
                        LeaseResource::IoBuffer(kind) => {
                            Some(IoPrediction::new(kind, claim.amount, 1))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if io.is_empty() {
                    stage
                } else {
                    stage.with_io(io)
                }
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete evidence prediction");
    PhysicalWorkBinding::new(
        implementation_catalog(&problem, &dag),
        dag,
        prediction,
        vec![
            PlannedArtifact::new(
                ArtifactIdentity::from_sha256([31; 32]),
                read.clone(),
                ArtifactRole::Input,
                None,
            ),
            PlannedArtifact::new(
                ArtifactIdentity::from_sha256([32; 32]),
                read,
                ArtifactRole::Cache,
                Some(CacheIdentity::from_sha256([33; 32])),
            ),
        ]
        .into_iter()
        .chain(base.artifacts().iter().cloned())
        .collect(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("bound evidenced transaction work")
}

fn adaptive_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("adaptive physical-work problem");
    let work_implementation = implementation(implementation_byte);
    let first_id = WorkNodeId::new("first-major-work");
    let boundary_id = WorkNodeId::new("major-boundary");
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 2;
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("adaptive-cpu"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: Vec::new(),
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: Vec::new(),
                rates: Vec::new(),
                caches: CacheDemand::zero(),
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: Vec::new(),
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand::zero(),
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: 1,
                maximum_batch_size: 2,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([
                QuiescencePoint::RunBoundary,
                QuiescencePoint::MajorCycle,
            ]),
        },
        nodes: vec![
            WorkNode {
                id: first_id.clone(),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::new(),
                claims: vec![ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                }],
                allocations: Vec::new(),
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: boundary_id.clone(),
                kind: WorkKind::Synchronization,
                domain: WorkDomain::Control,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::from([WorkDependency::Work(first_id)]),
                claims: Vec::new(),
                allocations: Vec::new(),
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::from([QuiescencePoint::MajorCycle]),
            },
            WorkNode {
                id: WorkNodeId::new("minor-work"),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: work_implementation,
                dependencies: BTreeSet::from([WorkDependency::Work(boundary_id)]),
                claims: vec![ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                }],
                allocations: Vec::new(),
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
        ],
        logical_allocations: Vec::new(),
        physical_slots: Vec::new(),
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: vec![AdaptationTransition {
            id: AdaptationId::new("larger-batch"),
            from: ExecutionKnobs::serial(),
            to: adapted,
            at: QuiescencePoint::MajorCycle,
        }],
    };
    transaction_binding(
        &problem,
        specification,
        implementation(implementation_byte),
        default_product_participants(),
        false,
        false,
        true,
    )
}

fn auditable_physical_work(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let base = adaptive_physical_work(implementation_byte);
    let base_dag = base.execution_dag();
    let allocation = AllocationId::new("audit-generation");
    let slot = PhysicalSlotId::new("audit-slot");
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("audit-layout"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let mut alternative = base_dag.resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "audit-memory".to_string(),
        hard_bytes: 64,
        preferred_bytes: 32,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.headroom.memory_bytes =
        BTreeMap::from([(CapacityDomainId::new("host-memory"), 16)]);
    alternative.capabilities.supported =
        BTreeSet::from([casa_imaging_runtime::CapabilityId::new("audit-capability")]);
    alternative.scaling.memory_bytes_per_worker =
        BTreeMap::from([(CapacityDomainId::new("host-memory"), 8)]);
    let mut nodes = base_dag.nodes().values().cloned().collect::<Vec<_>>();
    nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("first-major-work"))
        .expect("audit allocation acquisition node")
        .allocations
        .push(AllocationUse {
            allocation: allocation.clone(),
            lifetime: ClaimLifetime::Work,
        });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::from([casa_imaging_runtime::CapabilityId::new(
            "audit-capability",
        )]),
        resource_alternative: alternative,
        nodes,
        logical_allocations: base_dag
            .logical_allocations()
            .values()
            .cloned()
            .chain([LogicalAllocation {
                id: allocation,
                bytes: 64,
                purpose: AllocationPurpose::Data,
                compatibility: compatibility.clone(),
                physical_slot: slot.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: WorkNodeId::new("first-major-work"),
                    release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new(
                        "minor-work",
                    ))]),
                },
            }])
            .collect(),
        physical_slots: base_dag
            .physical_slots()
            .values()
            .cloned()
            .chain([PhysicalSlot {
                id: slot,
                lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                    allocation_id: "audit-memory".to_string(),
                },
                capacity_bytes: 64,
                compatibility,
            }])
            .collect(),
        initial_knobs: base_dag.initial_knobs().clone(),
        adaptations: base_dag.adaptations().values().cloned().collect(),
    })
    .expect("valid auditable physical work DAG");
    PhysicalWorkBinding::new(
        implementation_catalog(problem, &dag),
        dag,
        base.prediction().clone(),
        [PlannedArtifact::new(
            ArtifactIdentity::from_sha256([51; 32]),
            WorkNodeId::new("first-major-work"),
            ArtifactRole::Cache,
            Some(CacheIdentity::from_sha256([52; 32])),
        )]
        .into_iter()
        .chain(base.artifacts().iter().cloned())
        .collect(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("auditable physical work binding")
}

fn release_failure_physical_work(
    implementation_byte: u8,
    release_implementation_byte: u8,
    fail_at_fence: bool,
) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("release-failure physical-work problem");
    let (independent_id, prepare_id, release_id, allocation_name, slot_name) = if fail_at_fence {
        (
            WorkNodeId::new("z-independent-io"),
            WorkNodeId::new("0-prepare-mapping"),
            WorkNodeId::new("a-release-mapping"),
            "fence-failed-mapping",
            "fence-failed-slot",
        )
    } else {
        (
            WorkNodeId::new("0-independent-io"),
            WorkNodeId::new("1-prepare-mapping"),
            WorkNodeId::new("2-release-mapping"),
            "execute-failed-mapping",
            "execute-failed-slot",
        )
    };
    let allocation_id = AllocationId::new(allocation_name);
    let physical_slot_id = PhysicalSlotId::new(slot_name);
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(allocation_name),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let asynchronous = ClaimLifetime::through_fence(FenceKind::Io);
    let release_lifetime = if fail_at_fence {
        asynchronous.clone()
    } else {
        ClaimLifetime::Work
    };
    let release_domain = if fail_at_fence {
        WorkDomain::Io
    } else {
        WorkDomain::Cpu
    };
    let mut release_claims = if fail_at_fence {
        vec![
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::Rate {
                    demand_id: "io-rate".to_string(),
                },
                amount: 1,
                lifetime: asynchronous.clone(),
            },
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::Queue {
                    demand_id: "io-queue".to_string(),
                },
                amount: 1,
                lifetime: asynchronous.clone(),
            },
        ]
    } else {
        vec![ResourceClaim {
            resource: casa_imaging_runtime::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }]
    };
    release_claims.push(ResourceClaim {
        resource: casa_imaging_runtime::LeaseResource::IoBuffer(IoBufferKind::MappedPageCache),
        amount: 100,
        lifetime: release_lifetime.clone(),
    });
    let release_fences = if fail_at_fence {
        BTreeSet::from([FenceKind::Io])
    } else {
        BTreeSet::new()
    };
    let release_after = if fail_at_fence {
        BTreeSet::from([WorkDependency::Fence(FenceId::new(
            release_id.clone(),
            FenceKind::Io,
        ))])
    } else {
        BTreeSet::from([WorkDependency::Work(release_id.clone())])
    };
    let mut initial_knobs = ExecutionKnobs::serial();
    if fail_at_fence {
        initial_knobs.io_depth = 2;
    }
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new(if fail_at_fence {
                "release-fence-failure"
            } else {
                "release-execute-failure"
            }),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: vec![MemoryDemand {
                    allocation_id: slot_name.to_string(),
                    hard_bytes: 100,
                    preferred_bytes: 100,
                    views: vec![CapacityViewId::new("host-memory")],
                }],
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: Vec::new(),
                rates: vec![RateDemand {
                    demand_id: "io-rate".to_string(),
                    resource: RateResourceId::new("io-rate"),
                    amount: CountDemand::new(2, 1),
                }],
                caches: CacheDemand {
                    hard_resident_bytes: 100,
                    preferred_resident_bytes: 100,
                },
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: vec![QueueDemand {
                    demand_id: "io-queue".to_string(),
                    resource: QueueResourceId::new("io-queue"),
                    slots: CountDemand::new(2, 1),
                }],
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand {
                    mapped_page_cache_bytes: 100,
                    ..IoBufferDemand::zero()
                },
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: 1,
                maximum_batch_size: 1,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
        },
        nodes: vec![
            WorkNode {
                id: independent_id,
                kind: WorkKind::Io,
                domain: WorkDomain::Io,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::new(),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Rate {
                            demand_id: "io-rate".to_string(),
                        },
                        amount: 1,
                        lifetime: asynchronous.clone(),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Queue {
                            demand_id: "io-queue".to_string(),
                        },
                        amount: 1,
                        lifetime: asynchronous,
                    },
                ],
                allocations: Vec::new(),
                fences: BTreeSet::from([FenceKind::Io]),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: prepare_id.clone(),
                kind: WorkKind::Cache,
                domain: WorkDomain::Cpu,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::new(),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Workers,
                        amount: 1,
                        lifetime: ClaimLifetime::Work,
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::ResidentCache,
                        amount: 100,
                        lifetime: ClaimLifetime::Work,
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                            IoBufferKind::MappedPageCache,
                        ),
                        amount: 100,
                        lifetime: ClaimLifetime::Work,
                    },
                ],
                allocations: vec![AllocationUse {
                    allocation: allocation_id.clone(),
                    lifetime: ClaimLifetime::Work,
                }],
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: release_id.clone(),
                kind: WorkKind::Release,
                domain: release_domain,
                implementation: implementation(release_implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Work(prepare_id.clone())]),
                claims: release_claims,
                allocations: vec![AllocationUse {
                    allocation: allocation_id.clone(),
                    lifetime: release_lifetime,
                }],
                fences: release_fences,
                quiescence_after: BTreeSet::new(),
            },
        ],
        logical_allocations: vec![LogicalAllocation {
            id: allocation_id,
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::MappedPageCache),
            compatibility: compatibility.clone(),
            physical_slot: physical_slot_id.clone(),
            lifetime: AllocationLifetime {
                acquire_at: prepare_id.clone(),
                release_after,
            },
        }],
        physical_slots: vec![PhysicalSlot {
            id: physical_slot_id,
            lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                allocation_id: slot_name.to_string(),
            },
            capacity_bytes: 100,
            compatibility,
        }],
        initial_knobs: ExecutionKnobs {
            cache_retention_bytes: 100,
            ..initial_knobs
        },
        adaptations: Vec::new(),
    };
    transaction_binding(
        &problem,
        specification,
        implementation(implementation_byte),
        default_product_participants(),
        false,
        false,
        true,
    )
}

fn mapped_publication_candidate(
    producer: WorkNodeId,
    terminal: WorkDependency,
    allocation: AllocationId,
) -> Result<PhysicalWorkBinding, PhysicalWorkBindingError> {
    let problem = compile(request(1)).expect("mapped-publication physical-work problem");
    let base = release_failure_physical_work(6, 8, false);
    let mapped = PublicationMappedStaging::new(producer, terminal, allocation)
        .expect("mapped producer and release differ");
    let layouts = PublicationLayoutLedger::new(
        base.publication_layouts()
            .entries()
            .iter()
            .map(|layout| {
                let writer = layout.staging();
                PublicationPhysicalLayout::new(
                    layout.participant(),
                    layout.artifact(),
                    layout.layout_id(),
                    PublicationStaging::new(
                        writer.producer().clone(),
                        writer.terminal().clone(),
                        writer.writer_buffer_kind(),
                        writer.writer_allocation().clone(),
                    )
                    .expect("existing writer staging")
                    .with_mapped_page_cache(mapped.clone()),
                    PublicationResourceBounds::new(
                        layout.resource_bounds().staged_storage_bytes(),
                        layout.resource_bounds().final_storage_bytes(),
                        layout.resource_bounds().writer_buffer_bytes(),
                        100,
                    )
                    .expect("mapped publication bounds"),
                )
            })
            .collect(),
    )
    .expect("one mapped layout per participant");
    let dag = base.execution_dag().clone();
    PhysicalWorkBinding::new(
        implementation_catalog(&problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        layouts,
    )
}

fn test_registry(
    problem: &casa_imaging_model::CompiledProblem,
    registry_byte: u8,
    implementation_byte: u8,
    failure: Option<&'static str>,
) -> TestRegistry {
    TestRegistry {
        id: registry(registry_byte),
        metadata: implementation_metadata(problem),
        executors: BTreeMap::from([(
            implementation(implementation_byte),
            recording_executor(implementation_byte, failure, None),
        )]),
    }
}

struct TestRegistry {
    id: ImplementationRegistryId,
    metadata: ImplementationContractMetadata,
    executors: BTreeMap<WorkImplementationId, RecordingExecutor>,
}

impl TestRegistry {
    fn metadata_for(
        problem: &casa_imaging_model::CompiledProblem,
    ) -> ImplementationContractMetadata {
        implementation_metadata(problem)
    }
}

struct ContractOnlyRegistry {
    id: ImplementationRegistryId,
    metadata: ImplementationContractMetadata,
    executors: BTreeMap<WorkImplementationId, RecordingExecutor>,
}

impl ContractOnlyRegistry {
    fn new(
        id: ImplementationRegistryId,
        metadata: ImplementationContractMetadata,
        implementation_ids: impl IntoIterator<Item = WorkImplementationId>,
    ) -> Self {
        let executors = implementation_ids
            .into_iter()
            .map(|implementation_id| {
                let mut executor = recording_executor(0, None, None);
                executor.id = implementation_id.clone();
                (implementation_id, executor)
            })
            .collect();
        Self {
            id,
            metadata,
            executors,
        }
    }
}

#[derive(Default)]
struct RejectAfterLaunch {
    polls: usize,
}

impl RunController for RejectAfterLaunch {
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective {
        self.polls += 1;
        if self.polls <= 2 {
            RunDirective::Continue
        } else {
            assert!(status.eligible_adaptations().is_empty());
            RunDirective::Adapt(casa_imaging_runtime::AdaptationId::new("not-eligible"))
        }
    }
}

#[derive(Default)]
struct AdaptAtMajorBoundary {
    applied: bool,
}

impl RunController for AdaptAtMajorBoundary {
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective {
        let adaptation = AdaptationId::new("larger-batch");
        if !self.applied
            && status
                .eligible_adaptations()
                .iter()
                .any(|transition| transition.id == adaptation)
        {
            self.applied = true;
            RunDirective::Adapt(adaptation)
        } else {
            RunDirective::Continue
        }
    }
}

#[derive(Default)]
struct CancelAfterLaunch {
    polls: usize,
}

impl RunController for CancelAfterLaunch {
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective {
        self.polls += 1;
        if self.polls == 1 {
            RunDirective::Continue
        } else {
            assert!(status.eligible_adaptations().is_empty());
            RunDirective::Cancel
        }
    }
}

struct CancelAtPublicationState {
    publication_launched: Arc<AtomicBool>,
    visible_generation: Arc<AtomicUsize>,
    after_fence: bool,
    requested: bool,
}

struct AdaptAtPublicationLaunch {
    publication_launched: Arc<AtomicBool>,
    requested: bool,
}

impl RunController for AdaptAtPublicationLaunch {
    fn directive(&mut self, _status: &ExecutionStatus) -> RunDirective {
        if self.publication_launched.load(Ordering::SeqCst) && !self.requested {
            self.requested = true;
            RunDirective::Adapt(AdaptationId::new("post-publication-adaptation"))
        } else {
            RunDirective::Continue
        }
    }
}

impl RunController for CancelAtPublicationState {
    fn directive(&mut self, _status: &ExecutionStatus) -> RunDirective {
        let reached = if self.after_fence {
            self.visible_generation.load(Ordering::SeqCst) == 1
        } else {
            self.publication_launched.load(Ordering::SeqCst)
        };
        if reached && !self.requested {
            self.requested = true;
            RunDirective::Cancel
        } else {
            RunDirective::Continue
        }
    }
}

impl ImplementationRegistry for TestRegistry {
    type Implementation = RecordingExecutor;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        self.executors.get(id)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        self.executors
            .contains_key(id)
            .then(|| self.metadata.clone())
    }
}

impl ImplementationRegistry for ContractOnlyRegistry {
    type Implementation = RecordingExecutor;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        self.executors.get(id)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        self.executors
            .contains_key(id)
            .then(|| self.metadata.clone())
    }
}

fn authority() -> &'static ResourceAuthority {
    static AUTHORITY: OnceLock<&'static ResourceAuthority> = OnceLock::new();
    AUTHORITY.get_or_init(|| {
        ResourceAuthority::install_production_inventory(runtime_inventory(4))
            .expect("install deterministic runtime inventory")
    })
}

fn runtime_inventory(available_locks: u64) -> HostInventory {
    let domain = CapacityDomainId::new("host-memory");
    let view = CapacityViewId::new("host-memory");
    let rate = RateResourceId::new("io-rate");
    let operations_rate = RateResourceId::new("io-operations-rate");
    let queue = QueueResourceId::new("io-queue");
    let transaction_rate = RateResourceId::new("transaction-io-rate");
    let transaction_queue = QueueResourceId::new("transaction-io-queue");
    let storage = StorageDomainId::new("atomic-output");
    let source_storage = StorageDomainId::new("prepared-source-secondary");
    HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1_048_576,
            }],
            memory_views: vec![MemoryView {
                id: view,
                domain: domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: vec![
                StorageDomain {
                    id: storage.clone(),
                    root: PathBuf::from("/tmp/casa-rs-imaging-runtime-tests"),
                    capacity_bytes: 1_048_576,
                    read_rate: rate.clone(),
                    write_rate: rate.clone(),
                    operations_rate: Some(operations_rate.clone()),
                    queue: queue.clone(),
                },
                StorageDomain {
                    id: source_storage.clone(),
                    root: PathBuf::from("/tmp/casa-rs-imaging-runtime-source-tests"),
                    capacity_bytes: 1_048_576,
                    read_rate: rate.clone(),
                    write_rate: rate.clone(),
                    operations_rate: Some(operations_rate.clone()),
                    queue: queue.clone(),
                },
            ],
            rate_resources: vec![
                RateResource::new(rate.clone(), RateUnit::BytesPerSecond, 16),
                RateResource::new(operations_rate.clone(), RateUnit::OperationsPerSecond, 16),
                RateResource::new(transaction_rate.clone(), RateUnit::BytesPerSecond, 16),
            ],
            queue_resources: vec![
                QueueResource::new(queue.clone(), 8),
                QueueResource::new(transaction_queue.clone(), 4),
            ],
            logical_cpu_threads: 4,
            performance_cpu_cores: CpuClassCapacity::Known(4),
            cache_capacity_bytes: 1_048_576,
            lock_capacity: 4,
            file_descriptor_capacity: 16,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_048_576)]),
            available_cpu_threads: 4,
            storage_available_bytes: BTreeMap::from([
                (storage, 1_048_576),
                (source_storage, 1_048_576),
            ]),
            rate_available_per_second: BTreeMap::from([
                (rate, 16),
                (operations_rate, 16),
                (transaction_rate, 16),
            ]),
            queue_available_slots: BTreeMap::from([(queue, 8), (transaction_queue, 4)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_048_576,
            available_locks,
            available_file_descriptors: 16,
        },
    }
}

fn run_lock() -> &'static Mutex<()> {
    static RUN_LOCK: Mutex<()> = Mutex::new(());
    &RUN_LOCK
}

fn plan<E>(
    problem: &casa_imaging_model::CompiledProblem,
    bindings: PlanningBindings,
    planner: impl FnOnce(
        &casa_imaging_model::CompiledProblem,
        &PlanningBindings,
    ) -> Result<PhysicalWorkBinding, E>,
) -> Result<casa_imaging_runtime::ExecutionPlan, PlanError<E>> {
    let directory = tempfile::tempdir().expect("empty receipt directory");
    let root = directory.keep();
    let receipts = ExecutionReceiptStore::new(
        root,
        ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("empty receipt store");
    plan_with_receipts(problem, bindings, &receipts, planner)
}

fn plan_with_receipts<E>(
    problem: &casa_imaging_model::CompiledProblem,
    bindings: PlanningBindings,
    receipts: &ExecutionReceiptStore,
    planner: impl FnOnce(
        &casa_imaging_model::CompiledProblem,
        &PlanningBindings,
    ) -> Result<PhysicalWorkBinding, E>,
) -> Result<casa_imaging_runtime::ExecutionPlan, PlanError<E>> {
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let candidate = planner(problem, &bindings).map_err(PlanError::Planner)?;
    let implementation_ids = candidate
        .execution_dag()
        .nodes()
        .values()
        .map(|node| node.implementation.clone());
    let registry = ContractOnlyRegistry::new(
        bindings.implementation_registry_id(),
        implementation_metadata(problem),
        implementation_ids,
    );
    let candidates = vec![candidate];
    runtime_plan(
        problem,
        bindings,
        authority(),
        &registry,
        receipts,
        move |_, _| Ok(candidates),
    )
}

fn execution_provenance(
    attempt: casa_imaging_runtime::ExecutionAttemptId,
    build: BuildIdentity,
) -> ExecutionProvenance {
    ExecutionProvenance::new(
        attempt,
        build,
        ExecutionRouteEvidence::new(
            1,
            1,
            ExecutionRouteDisposition::Native,
            vec![
                ExecutionRouteRequirement::new(
                    "capability.compiled-problem",
                    ExecutionRouteRequirementKind::Capability,
                    ExecutionRouteDisposition::Native,
                    casa_imaging_runtime::ExecutionRouteRequirementEvidence {
                        current_owner: "crates/casa-imaging-model".to_string(),
                        destination_tickets: vec!["T05/#491".to_string()],
                        evidence_issues: vec![486, 491],
                        baseline_manifests: vec![
                            "repo://crates/casa-imaging-model/src/lib.rs".to_string(),
                        ],
                        acceptance_contract: "compiled-problem-foundation-v1".to_string(),
                        transfer_point: "immutable backend-independent logical problem and stable identity landed in Wave 1".to_string(),
                        deletion_condition: "not applicable; canonical logical-problem owner".to_string(),
                        source_evidence: vec![
                            "crates/casa-imaging-model/src/lib.rs::CompiledProblem".to_string(),
                        ],
                        obligation_ticket: None,
                        obligation_reason: None,
                    },
                )
                .expect("canonical route row"),
            ],
        )
        .expect("canonical native route"),
    )
}

fn run<C: RunController>(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
    authority: &ResourceAuthority,
    controller: &mut C,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let receipts = plan.receipt_store();
    static NEXT_RUN_ATTEMPT: AtomicUsize = AtomicUsize::new(241);
    let attempt_seed =
        u8::try_from(NEXT_RUN_ATTEMPT.fetch_add(1, Ordering::SeqCst) % usize::from(u8::MAX))
            .expect("bounded synthetic attempt seed");
    run_receipted(
        problem,
        plan,
        current,
        registry,
        authority,
        controller,
        receipts.bind(execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([attempt_seed; 32]),
            BuildIdentity::from_sha256([242; 32]),
        )),
    )
}

fn run_receipted<C: RunController>(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
    authority: &ResourceAuthority,
    controller: &mut C,
    receipt: ExecutionReceiptBinding<'_>,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem");
    runtime_run(
        &executable,
        plan,
        current,
        registry,
        authority,
        controller,
        receipt,
    )
}

fn execute_plan(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let mut controller = RunToCompletion;
    run(
        problem,
        plan,
        current,
        registry,
        authority(),
        &mut controller,
    )
}

#[test]
fn physical_work_binding_rejects_io_and_publication_evidence_outside_plan_semantics() {
    let problem = compile(request(1)).expect("physical-work contract problem");
    let io_base = physical_work(6);
    let io_dag = io_base.execution_dag().clone();
    let io_prediction = PlanPrediction::new(
        200,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        io_dag
            .nodes()
            .keys()
            .map(|node| {
                let prediction = StagePrediction::new(node.clone(), 100);
                if node.as_str() == "execute" {
                    prediction.with_io(vec![IoPrediction::new(
                        IoBufferKind::SourceReadAhead,
                        8_192,
                        4,
                    )])
                } else {
                    prediction
                }
            })
            .collect(),
    )
    .expect("well-formed prediction ledger");

    assert!(matches!(
        PhysicalWorkBinding::new(
            implementation_catalog(&problem, &io_dag),
            io_dag,
            io_prediction,
            Vec::new(),
            io_base.observation_transaction().clone(),
            PublicationLayoutLedger::empty(),
        ),
        Err(PhysicalWorkBindingError::IoKindMismatch {
            kind: IoBufferKind::SourceReadAhead,
            work_kind: WorkKind::Compute,
            ..
        })
    ));

    let contract_base = physical_work(6);
    let contract_base_dag = contract_base.execution_dag();
    let mut contract_nodes = contract_base_dag
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    contract_nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("read"))
        .expect("read node")
        .kind = WorkKind::Prefetch;
    let contract_dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: contract_base_dag.required_resource_capabilities().clone(),
        resource_alternative: contract_base_dag.resource_alternative().clone(),
        nodes: contract_nodes,
        logical_allocations: contract_base_dag
            .logical_allocations()
            .values()
            .cloned()
            .collect(),
        physical_slots: contract_base_dag
            .physical_slots()
            .values()
            .cloned()
            .collect(),
        initial_knobs: contract_base_dag.initial_knobs().clone(),
        adaptations: contract_base_dag.adaptations().values().cloned().collect(),
    })
    .expect("Prefetch node may be zero-copy without I/O evidence");
    let contract_prediction = PlanPrediction::new(
        200,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        contract_dag
            .nodes()
            .keys()
            .map(|node| {
                let prediction = StagePrediction::new(node.clone(), 100);
                if node.as_str() == "read" {
                    prediction.with_io(vec![IoPrediction::new(
                        IoBufferKind::SourceReadAhead,
                        8_192,
                        4,
                    )])
                } else {
                    prediction
                }
            })
            .collect(),
    )
    .expect("well-formed prediction ledger");
    assert!(matches!(
        PhysicalWorkBinding::new(
            implementation_catalog(&problem, &contract_dag),
            contract_dag,
            contract_prediction,
            Vec::new(),
            contract_base.observation_transaction().clone(),
            PublicationLayoutLedger::empty(),
        ),
        Err(PhysicalWorkBindingError::MissingIoContract {
            kind: IoBufferKind::SourceReadAhead,
            ..
        })
    ));

    let publication_base = physical_work(6);
    let publication_dag = publication_base.execution_dag().clone();
    let publication_prediction = publication_base.prediction().clone();
    let output = PlannedArtifact::new(
        ArtifactIdentity::from_sha256([79; 32]),
        WorkNodeId::new("execute"),
        ArtifactRole::Output,
        None,
    );

    assert!(matches!(
        PhysicalWorkBinding::new(
            implementation_catalog(&problem, &publication_dag),
            publication_dag,
            publication_prediction,
            vec![output],
            publication_base.observation_transaction().clone(),
            PublicationLayoutLedger::empty(),
        ),
        Err(PhysicalWorkBindingError::MissingPublicationContract { .. })
    ));
}

#[test]
fn physical_work_binding_rejects_typed_io_contracts_without_predictions() {
    let problem = compile(request(1)).expect("physical-work contract problem");
    let base = evidenced_physical_work(6);
    let dag = base.execution_dag().clone();
    let prediction = PlanPrediction::new(
        300,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        dag.nodes()
            .keys()
            .cloned()
            .map(|node| StagePrediction::new(node, 100))
            .collect(),
    )
    .expect("complete stage ledger with no typed I/O evidence");

    assert!(matches!(
        PhysicalWorkBinding::new(
            implementation_catalog(&problem, &dag),
            dag,
            prediction,
            Vec::new(),
            base.observation_transaction().clone(),
            PublicationLayoutLedger::empty(),
        ),
        Err(PhysicalWorkBindingError::MissingIoPrediction { .. })
    ));
}

#[test]
fn physical_work_binding_rejects_cpu_io_buffer_contracts_without_predictions() {
    let problem = compile(request(1)).expect("physical-work contract problem");
    let base = evidenced_physical_work(6);
    let base_dag = base.execution_dag();
    let prepare = WorkNodeId::new("read");
    let source_buffer = AllocationId::new("source-read-ahead-buffer");
    let mut nodes = base_dag.nodes().values().cloned().collect::<Vec<_>>();
    let prepare_node = nodes
        .iter_mut()
        .find(|node| node.id == prepare)
        .expect("preparation node");
    prepare_node.kind = WorkKind::Preparation;
    prepare_node.domain = WorkDomain::Cpu;
    prepare_node.claims = vec![
        ResourceClaim {
            resource: casa_imaging_runtime::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: casa_imaging_runtime::LeaseResource::IoBuffer(IoBufferKind::Preparation),
            amount: 32,
            lifetime: ClaimLifetime::Work,
        },
    ];
    prepare_node.allocations[0].lifetime = ClaimLifetime::Work;
    prepare_node.fences.clear();
    nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("execute"))
        .expect("compute node")
        .dependencies = BTreeSet::from([WorkDependency::Work(prepare.clone())]);

    let mut resource_alternative = base_dag.resource_alternative().clone();
    resource_alternative
        .demand
        .io_buffers
        .source_read_ahead_bytes = 0;
    resource_alternative.demand.io_buffers.preparation_bytes = 32;
    let mut logical_allocations = base_dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let allocation = logical_allocations
        .iter_mut()
        .find(|allocation| allocation.id == source_buffer)
        .expect("preparation allocation");
    allocation.purpose = AllocationPurpose::IoBuffer(IoBufferKind::Preparation);
    allocation.lifetime.release_after = BTreeSet::from([WorkDependency::Work(prepare.clone())]);

    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base_dag.required_resource_capabilities().clone(),
        resource_alternative,
        nodes,
        logical_allocations,
        physical_slots: base_dag.physical_slots().values().cloned().collect(),
        initial_knobs: base_dag.initial_knobs().clone(),
        adaptations: base_dag.adaptations().values().cloned().collect(),
    })
    .expect("CPU preparation may own a typed preparation buffer");
    let prediction = PlanPrediction::new(
        300,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        dag.nodes()
            .keys()
            .map(|node| {
                let stage = StagePrediction::new(node.clone(), 100);
                if node.as_str() == "transaction-commit" {
                    stage.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 2_048, 1)])
                } else {
                    stage
                }
            })
            .collect(),
    )
    .expect("complete prediction ledger without preparation I/O evidence");

    assert!(matches!(
        PhysicalWorkBinding::new(
            implementation_catalog(&problem, &dag),
            dag,
            prediction,
            Vec::new(),
            base.observation_transaction().clone(),
            PublicationLayoutLedger::empty(),
        ),
        Err(PhysicalWorkBindingError::MissingIoPrediction {
            kind: IoBufferKind::Preparation,
            ..
        })
    ));
}

#[test]
fn run_rejects_artifact_dispositions_that_contradict_plan_semantics() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(evidenced_physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let input = ArtifactIdentity::from_sha256([31; 32]);
    let cache = ArtifactIdentity::from_sha256([32; 32]);
    let mut executor = recording_executor(6, None, None);
    executor.measurements = BTreeMap::from([(
        WorkNodeId::new("read"),
        (
            vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
            vec![
                artifact_measurement(input, Some(input), ArtifactDisposition::Staged, 4_096, None),
                artifact_measurement(cache, Some(cache), ArtifactDisposition::Reused, 0, None),
            ],
        ),
    )]);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    let error = execute_plan(&problem, &execution_plan, &current, &registry)
        .expect_err("an input artifact cannot claim publication");

    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::ArtifactDispositionMismatch {
            node,
            artifact,
            role: ArtifactRole::Input,
            disposition: ArtifactDisposition::Staged,
        }) if node == WorkNodeId::new("read") && artifact == input
    ));
}

#[test]
fn run_can_invoke_only_the_implementation_identity_sealed_by_plan() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let selected = recording_executor(6, None, None);
    let different = recording_executor(7, None, None);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([
            (implementation(6), selected),
            (implementation(7), different),
        ]),
    };
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let output =
        execute_plan(&problem, &execution_plan, &current, &registry).expect("bound execution");

    assert_eq!(output, ExecutionOutcome::Succeeded);
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        7,
        "two planned nodes and five mandatory transaction nodes must execute"
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        4,
        "planned and transaction reads plus both publication fences must settle"
    );
    assert_eq!(
        registry.executors[&implementation(7)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn initial_consistency_check_receives_the_exact_observation_transaction() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let observed = Arc::new(AtomicBool::new(false));
    let mut executor = recording_executor(6, None, None);
    executor.initial_consistency_expected = Some((
        problem.observation_transaction().transaction_id(),
        Arc::clone(&observed),
    ));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    execute_plan(&problem, &execution_plan, &current, &registry)
        .expect("initial consistency check executes with its capability");

    assert!(
        observed.load(Ordering::SeqCst),
        "the initial consistency node must receive the exact transaction state"
    );
}

#[test]
fn generic_io_cannot_receive_observation_sources() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let generic_source_access = Arc::new(AtomicBool::new(false));
    let mut executor = recording_executor(6, None, None);
    executor.generic_source_access = Some(Arc::clone(&generic_source_access));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    execute_plan(&problem, &execution_plan, &current, &registry).expect("bound execution");

    assert!(
        !generic_source_access.load(Ordering::SeqCst),
        "generic Io must not receive the MeasurementSet observation source set"
    );
}

#[test]
fn run_rejects_a_registry_that_cannot_resolve_the_bound_implementation() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let registry = test_registry(&problem, 3, 7, None);
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let result = execute_plan(&problem, &execution_plan, &current, &registry);

    assert!(matches!(
        result,
        Err(RunError::ImplementationUnavailable { implementation: id })
            if id == implementation(6)
    ));
    assert_eq!(
        registry.executors[&implementation(7)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn run_rejects_a_different_implementation_returned_under_the_bound_key() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let mut registry = test_registry(&problem, 3, 6, None);
    registry
        .executors
        .get_mut(&implementation(6))
        .expect("registered key")
        .id = implementation(7);
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let result = execute_plan(&problem, &execution_plan, &current, &registry);

    assert!(matches!(
        result,
        Err(RunError::ImplementationMismatch { planned, observed })
            if planned == implementation(6) && observed == implementation(7)
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn versioned_request_compiles_before_physical_planning() {
    let request = request(1);
    assert_eq!(request.version(), ImagingRequestVersion::V3);

    let problem = compile(request).expect("logical compilation");
    assert_eq!(problem.numerics_id().as_bytes().len(), 32);
}

#[test]
fn plan_seals_physical_work_and_every_required_binding() {
    assert_eq!(ExecutionPlanId::SCHEMA_VERSION, 10);
    let problem = compile(request(1)).expect("logical compilation");
    let expected_problem_id = problem.problem_id();
    let bindings =
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4));
    let receipts = ExecutionReceiptStore::new(
        "/tmp/casa-rs-imaging-plan-id-regression",
        ReceiptRetention::new(4, 1_048_576).expect("plan-id retention"),
    )
    .expect("plan-id receipt store");
    let execution_plan = plan_with_receipts(
        &problem,
        bindings.clone(),
        &receipts,
        |problem, bindings| {
            assert_eq!(problem.problem_id(), expected_problem_id);
            assert_eq!(bindings.resource_policy(), &ResourcePolicy::Balanced);
            Ok::<_, ()>(physical_work(6))
        },
    )
    .expect("physical planning");

    assert_eq!(execution_plan.problem_id(), problem.problem_id());
    assert_eq!(
        execution_plan.product_graph_id(),
        problem.product_graph().graph_id()
    );
    assert_eq!(
        execution_plan.geometry_id(),
        problem.geometry().geometry_id()
    );
    assert_eq!(
        execution_plan.observation_snapshot_id(),
        problem.inputs().observation()
    );
    assert_eq!(execution_plan.numerics_id(), problem.numerics_id());
    assert_eq!(execution_plan.implementation_registry_id(), registry(3));
    assert_eq!(
        execution_plan.resource_policy_id(),
        bindings.resource_policy_id()
    );
    assert_eq!(
        execution_plan.planner_cost_model_profile_id(),
        cost_model(4)
    );
    assert_eq!(
        execution_plan.physical_work_id(),
        execution_plan.execution_dag().physical_work_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().problem_id(),
        problem.problem_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().product_graph_id(),
        problem.product_graph().graph_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().transaction_id(),
        problem.observation_transaction().transaction_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().physical_work_id(),
        execution_plan.physical_work_id()
    );

    let repeated = plan_with_receipts(&problem, bindings, &receipts, |_, _| {
        Ok::<_, ()>(physical_work(6))
    })
    .expect("repeat physical planning");
    assert_eq!(execution_plan.plan_id(), repeated.plan_id());
    assert_eq!(
        execution_plan.plan_id().as_bytes(),
        [
            90, 161, 155, 186, 10, 78, 134, 46, 62, 1, 115, 175, 118, 198, 110, 233, 245, 140, 85,
            30, 183, 39, 145, 242, 215, 131, 96, 32, 37, 197, 91, 101,
        ]
    );
}

#[test]
fn receipt_store_location_does_not_change_the_logical_plan_identity() {
    let problem = compile(request(1)).expect("logical compilation");
    let first_directory = tempfile::tempdir().expect("first receipt directory");
    let second_directory = tempfile::tempdir().expect("second receipt directory");
    let retention = ReceiptRetention::new(4, 1_048_576).expect("plan-id retention");
    let first_receipts =
        ExecutionReceiptStore::new(first_directory.path(), retention).expect("first receipt store");
    let second_receipts = ExecutionReceiptStore::new(second_directory.path(), retention)
        .expect("second receipt store");
    let first = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &first_receipts,
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("first physical planning");
    let second = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &second_receipts,
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("second physical planning");

    assert_eq!(first.plan_id(), second.plan_id());
    assert_ne!(first.receipt_store(), second.receipt_store());
}

#[test]
fn transaction_seal_rejects_omitted_product_graph_publication_member() {
    let problem = compile(request_with_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual],
    ))
    .expect("two-product logical compilation");
    plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |problem, _| Ok::<_, io::Error>(physical_work_for_problem(problem, 6)),
    )
    .expect("canonical complete two-product transaction seal");

    let omitted = product_participants(&problem).into_iter().take(1).collect();
    let result = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, io::Error>(physical_work_with_product_staging(&problem, 6, omitted)),
    );

    let error = result.expect_err("one omitted product must fail the exact plan seal");
    assert!(
        error.to_string().contains("publication product nodes")
            && error.to_string().contains("do not match graph members")
    );
}

#[test]
fn transaction_seal_rejects_matching_ordinals_from_a_foreign_product_graph() {
    let problem = compile(request_with_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual],
    ))
    .expect("expected product graph");
    let foreign = compile(request_with_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Model],
    ))
    .expect("foreign product graph with matching node ordinals");
    assert_ne!(
        problem.product_graph().graph_id(),
        foreign.product_graph().graph_id()
    );
    assert_eq!(
        problem.product_graph().publication().members(),
        foreign.product_graph().publication().members()
    );

    let result = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| {
            Ok::<_, io::Error>(physical_work_with_product_staging(
                &problem,
                6,
                product_participants(&foreign),
            ))
        },
    );

    let error = result.expect_err("foreign product graph must fail the transaction seal");
    assert!(matches!(error, PlanError::ObservationTransaction(_)));
    assert!(error.to_string().contains("do not match graph members"));
}

#[test]
fn mapped_publication_staging_binds_its_producer_release_allocation_and_plan_identity() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings =
        || PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4));
    let valid = mapped_publication_candidate(
        WorkNodeId::new("1-prepare-mapping"),
        WorkDependency::Work(WorkNodeId::new("2-release-mapping")),
        AllocationId::new("execute-failed-mapping"),
    )
    .expect("producer-owned mapped staging retained through its release");
    let mapped_plan =
        plan(&problem, bindings(), |_, _| Ok::<_, ()>(valid)).expect("mapped publication planning");
    let unmapped_plan = plan(&problem, bindings(), |_, _| {
        Ok::<_, ()>(release_failure_physical_work(6, 8, false))
    })
    .expect("otherwise identical unmapped publication planning");
    assert_ne!(mapped_plan.plan_id(), unmapped_plan.plan_id());

    for (producer, terminal, allocation, expected) in [
        (
            WorkNodeId::new("0-independent-io"),
            WorkDependency::Work(WorkNodeId::new("2-release-mapping")),
            AllocationId::new("execute-failed-mapping"),
            "not acquired by its producer",
        ),
        (
            WorkNodeId::new("1-prepare-mapping"),
            WorkDependency::Fence(FenceId::new(
                WorkNodeId::new("0-independent-io"),
                FenceKind::Io,
            )),
            AllocationId::new("execute-failed-mapping"),
            "not acquired by its producer",
        ),
        (
            WorkNodeId::new("1-prepare-mapping"),
            WorkDependency::Work(WorkNodeId::new("2-release-mapping")),
            AllocationId::new("transaction-product-writer-buffer"),
            "not acquired by its producer",
        ),
    ] {
        let error = mapped_publication_candidate(producer, terminal, allocation)
            .expect_err("mismatched mapped staging must be rejected");
        assert!(
            matches!(
                error,
                PhysicalWorkBindingError::InvalidPublicationLayout { .. }
            ),
            "{error}"
        );
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn transaction_seal_blocks_unbound_transaction_work() {
    let problem = compile(request(1)).expect("logical compilation");
    let base = physical_work_for_problem(&problem, 6);
    let dag = base.execution_dag().clone();
    let unbound = PhysicalWorkBinding::new(
        implementation_catalog(&problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        ObservationTransactionWork::new(
            WorkNodeId::new("execute"),
            base.observation_transaction()
                .final_reconciliation()
                .clone(),
            base.observation_transaction()
                .model_column_staging()
                .cloned(),
            base.observation_transaction().commit().clone(),
        ),
        base.publication_layouts().clone(),
    )
    .expect("physically valid but transaction-unbound candidate");
    let result = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, io::Error>(unbound),
    );

    assert!(
        matches!(result, Err(PlanError::ObservationTransaction(_))),
        "the public plan boundary must reject physical work that bypasses transaction sealing"
    );
}

#[test]
fn run_rejects_changed_registry_policy_and_cost_model_bindings() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current =
        |policy, cost_model_id| RunBindings::new(problem.inputs().clone(), policy, cost_model_id);
    let reject = |bindings, registry| execute_plan(&problem, &execution_plan, &bindings, registry);
    let wrong_registry = test_registry(&problem, 9, 6, None);
    let correct_registry = test_registry(&problem, 3, 6, None);

    assert!(matches!(
        reject(
            current(&ResourcePolicy::Balanced, cost_model(4)),
            &wrong_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::ImplementationRegistry
        })
    ));
    assert!(matches!(
        reject(
            current(&ResourcePolicy::Exclusive, cost_model(4)),
            &correct_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::ResourcePolicy
        })
    ));
    assert!(matches!(
        reject(
            current(&ResourcePolicy::Balanced, cost_model(9)),
            &correct_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::PlannerCostModelProfile
        })
    ));
}

#[test]
fn run_rejects_every_stale_problem_input_before_calling_the_executor() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings =
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4));
    let execution_plan = plan(&problem, bindings.clone(), |_, _| {
        Ok::<_, ()>(physical_work(6))
    })
    .expect("physical planning");
    let registry = test_registry(&problem, 3, 6, None);
    let stale_inputs = [
        (
            problem_inputs(9, default_references(), ModelStateIdentity::Empty),
            BindingKind::ObservationSnapshot,
        ),
        (
            problem_inputs(
                1,
                vec![(ReferenceDataKind::Measures, identity(9))],
                ModelStateIdentity::Empty,
            ),
            BindingKind::ReferenceDataSnapshots,
        ),
        (
            problem_inputs(
                1,
                default_references(),
                ModelStateIdentity::Seed(identity(9)),
            ),
            BindingKind::ModelState,
        ),
    ];

    for (inputs, expected) in stale_inputs {
        let stale = RunBindings::new(inputs, &ResourcePolicy::Balanced, cost_model(4));
        let result = execute_plan(&problem, &execution_plan, &stale, &registry);
        assert!(matches!(
            result,
            Err(RunError::BindingMismatch { binding }) if binding == expected
        ));
    }

    let changed_problem = compile(request(9)).expect("changed logical problem");
    let current = RunBindings::new(
        changed_problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let result = execute_plan(&changed_problem, &execution_plan, &current, &registry);
    assert!(matches!(
        result,
        Err(RunError::BindingMismatch {
            binding: BindingKind::CompiledProblem
        })
    ));
    let changed_geometry_problem =
        compile(request_with_geometry(1, geometry(254.0))).expect("changed compiled geometry");
    assert_ne!(
        execution_plan.geometry_id(),
        changed_geometry_problem.geometry().geometry_id()
    );
    let current = RunBindings::new(
        changed_geometry_problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    assert!(matches!(
        execute_plan(
            &changed_geometry_problem,
            &execution_plan,
            &current,
            &registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::CompiledProblem
        })
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn run_executes_one_exactly_bound_plan_without_routing_or_replanning() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings =
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4));
    let execution_plan = plan(&problem, bindings.clone(), |_, _| {
        Ok::<_, ()>(physical_work(6))
    })
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, None);

    let output =
        execute_plan(&problem, &execution_plan, &current, &registry).expect("exact execution");

    assert_eq!(output, ExecutionOutcome::Succeeded);
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        7,
        "two planned nodes and five mandatory transaction nodes must execute"
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        4,
        "planned and transaction reads plus both publication fences must settle"
    );
}

#[test]
fn run_preserves_the_selected_executors_error_chain() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, Some("selected executor failed"));

    let error =
        execute_plan(&problem, &execution_plan, &current, &registry).expect_err("executor failure");

    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("selected executor failed")
    );
}

#[test]
fn rejected_post_launch_adaptation_drains_fences_before_returning() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let mut controller = RejectAfterLaunch::default();

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("an ineligible adaptation must be rejected after safe drain");

    assert!(matches!(error, RunError::Scheduler(_)));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        2
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        1,
        "the launched I/O fence must settle before run returns"
    );
}

#[test]
fn run_applies_an_eligible_transition_to_later_scheduled_work() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(adaptive_physical_work(6)),
    )
    .expect("adaptive physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let mut controller = AdaptAtMajorBoundary::default();

    let outcome = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("eligible adaptation executes");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert!(controller.applied);
    let observed = registry.executors[&implementation(6)]
        .observed_knobs
        .lock()
        .expect("recorded knobs");
    assert_eq!(observed.len(), 8);
    assert!(
        observed[..4]
            .iter()
            .all(|knobs| knobs == &ExecutionKnobs::serial())
    );
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 2;
    assert!(observed[4..].iter().all(|knobs| knobs == &adapted));
}

#[test]
fn run_cancellation_at_the_pre_read_cut_releases_authority_capacity() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(
            implementation(6),
            publication_recording_executor(
                6,
                publication_launched,
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let mut cancelling = CancelAfterLaunch::default();

    let cancelled = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut cancelling,
    )
    .expect("cancellation drains safely");

    assert_eq!(cancelled, ExecutionOutcome::Cancelled);
    assert_eq!(
        visible_generation.load(Ordering::SeqCst),
        0,
        "pre-read cancellation must retain the old visible generation"
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        0,
        "cancellation after consistency checking must prevent the observation read from launching"
    );
    let mut completion = RunToCompletion;
    assert_eq!(
        run(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut completion,
        )
        .expect("released authority admits the same plan again"),
        ExecutionOutcome::Succeeded
    );
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        8
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        4
    );
}

#[test]
fn cancellation_cannot_report_cancelled_after_atomic_publication_is_irrevocable() {
    for after_fence in [false, true] {
        let problem = compile(request(1)).expect("logical compilation");
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
        .expect("physical planning");
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let publication_launched = Arc::new(AtomicBool::new(false));
        let visible_generation = Arc::new(AtomicUsize::new(0));
        let registry = TestRegistry {
            id: registry(3),
            metadata: implementation_metadata(&problem),
            executors: BTreeMap::from([(
                implementation(6),
                publication_recording_executor(
                    6,
                    Arc::clone(&publication_launched),
                    Arc::clone(&visible_generation),
                ),
            )]),
        };
        let mut controller = CancelAtPublicationState {
            publication_launched,
            visible_generation: Arc::clone(&visible_generation),
            after_fence,
            requested: false,
        };
        let outcome = run(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut controller,
        )
        .expect("late cancellation must settle the committed transaction");

        assert!(
            !controller.requested,
            "controller polling must stop once atomic publication launches"
        );
        assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
        assert_eq!(
            outcome,
            ExecutionOutcome::Succeeded,
            "a cancellation request cannot report Cancelled once publication {}",
            if after_fence {
                "has completed"
            } else {
                "has launched"
            }
        );
    }
}

#[test]
fn controller_cannot_adapt_after_atomic_publication_is_irrevocable() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(
            implementation(6),
            publication_recording_executor(
                6,
                Arc::clone(&publication_launched),
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let mut controller = AdaptAtPublicationLaunch {
        publication_launched,
        requested: false,
    };
    let outcome = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("publication launch must close the controller seam");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
    assert!(
        !controller.requested,
        "the controller cannot adapt after atomic publication launches"
    );
}

#[test]
fn publication_visibility_is_final_after_fence_and_scheduler_settlement() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let visible_during_settlement = Arc::new(AtomicBool::new(false));
    let mut executor =
        publication_recording_executor(6, publication_launched, Arc::clone(&visible_generation));
    executor.visibility_during_fence_settlement = Some(Arc::clone(&visible_during_settlement));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;
    let outcome = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("atomic publication executes after settlement");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
    assert!(
        !visible_during_settlement.load(Ordering::SeqCst),
        "the new generation cannot become visible during fallible fence settlement"
    );
}

#[test]
fn receipt_finalize_failure_after_publish_returns_success_and_reopens_prepared_evidence() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([91; 32]),
        BuildIdentity::from_sha256([92; 32]),
    );
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let mut executor = publication_recording_executor(
        6,
        Arc::clone(&publication_launched),
        Arc::clone(&visible_generation),
    );
    executor.receipt_root_to_disrupt = Some(receipts.root_path().to_owned());
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let outcome = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("publication success remains the terminal runtime result");
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("prepared receipt remains reopenable");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert!(publication_launched.load(Ordering::SeqCst));
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
    assert_eq!(receipt.schema_version(), 14);
    assert_eq!(receipt.status(), ReceiptStatus::PublicationPrepared);
    for layout in execution_plan.publication_layouts().entries() {
        assert_eq!(
            receipt.artifact_disposition(layout.artifact()),
            Some(ArtifactDisposition::Staged)
        );
        assert_eq!(
            receipt.publication_layout_identity(layout.artifact()),
            Some(layout.layout_id())
        );
        assert_eq!(
            receipt.publication_resource_bounds(layout.artifact()),
            Some(layout.resource_bounds())
        );
    }
}

#[test]
fn prepared_publication_holds_the_shared_root_reservation_through_publish() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let max_bytes = 1_048_576;
    let receipts = execution_plan.receipt_store();
    let pause = Arc::new(PublicationPause::default());
    let mut executor = recording_executor(6, None, None);
    executor.publication_pause = Some(Arc::clone(&pause));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let first = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([93; 32]),
        BuildIdentity::from_sha256([94; 32]),
    );
    let second = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([95; 32]),
        BuildIdentity::from_sha256([96; 32]),
    );
    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem");

    std::thread::scope(|scope| {
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let first_provenance = first.clone();
        let problem = &executable;
        let execution_plan = &execution_plan;
        let current = &current;
        let registry = &registry;
        let receipts = &receipts;
        scope.spawn(move || {
            let mut controller = RunToCompletion;
            first_tx
                .send(runtime_run(
                    problem,
                    execution_plan,
                    current,
                    registry,
                    authority(),
                    &mut controller,
                    receipts.bind(first_provenance),
                ))
                .expect("first run result receiver");
        });

        pause.wait_until_entered();
        let retained_bytes = fs::read_dir(receipts.root_path())
            .expect("receipt root")
            .map(|entry| {
                entry
                    .expect("receipt entry")
                    .metadata()
                    .expect("receipt metadata")
                    .len()
            })
            .sum::<u64>();
        assert!(
            retained_bytes <= max_bytes,
            "prepared marker plus terminal candidate must remain within retention"
        );

        let (second_tx, second_rx) = std::sync::mpsc::channel();
        let second_provenance = second.clone();
        scope.spawn(move || {
            let mut controller = RunToCompletion;
            second_tx
                .send(runtime_run(
                    problem,
                    execution_plan,
                    current,
                    registry,
                    authority(),
                    &mut controller,
                    receipts.bind(second_provenance),
                ))
                .expect("second run result receiver");
        });
        assert!(matches!(
            second_rx.recv_timeout(Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));

        pause.release();
        assert_eq!(
            first_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("first run result")
                .expect("first run"),
            ExecutionOutcome::Succeeded
        );
        assert_eq!(
            second_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("second run result")
                .expect("second run"),
            ExecutionOutcome::Succeeded
        );
    });

    assert_eq!(
        receipts
            .open(first.attempt_id())
            .expect("first receipt")
            .status(),
        ReceiptStatus::Completed
    );
    assert_eq!(
        receipts
            .open(second.attempt_id())
            .expect("second receipt")
            .status(),
        ReceiptStatus::Completed
    );
}

#[test]
fn earlier_acquired_publication_buffer_is_held_through_publish_and_then_released() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work_with_early_publication_buffer(6)),
    )
    .expect("earlier-acquired publication buffer is valid physical work");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let held_during_publish = Arc::new(AtomicBool::new(false));
    let mut executor = recording_executor(6, None, None);
    executor.publication_buffer_held = Some(Arc::clone(&held_during_publish));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    for attempt in 1..=2 {
        held_during_publish.store(false, Ordering::SeqCst);
        let mut controller = RunToCompletion;
        let outcome = run(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut controller,
        )
        .unwrap_or_else(|error| panic!("publication attempt {attempt} failed: {error}"));

        assert_eq!(outcome, ExecutionOutcome::Succeeded);
        assert!(
            held_during_publish.load(Ordering::SeqCst),
            "the earlier-acquired Publication buffer, slot, permit, and lease must remain held through publish"
        );
    }
}

#[test]
fn observation_completion_is_attempt_node_and_fence_bound_after_successful_settlement() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let receipts = execution_plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([157; 32]);
    let completions = Arc::new(Mutex::new(Vec::new()));
    let mut executor = recording_executor(6, None, None);
    executor.observation_completions = Some(Arc::clone(&completions));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([158; 32]),
        )),
    )
    .expect("observation completion must bind after its fence settles");

    let completions = completions.lock().expect("observation completion lock");
    assert_eq!(completions.len(), 1);
    let completion = &completions[0];
    assert_eq!(completion.attempt_id, attempt);
    assert_eq!(completion.owner_node, WorkNodeId::new("transaction-read"));
    assert_eq!(completion.settled_fences, BTreeSet::from([FenceKind::Io]));
    assert!(completion.lease_epoch > 0);
}

#[test]
fn settled_observation_completion_is_delivered_only_to_explicit_predecessor_consumers() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let mut executor = recording_executor(6, None, None);
    executor.delivered_observation_completions = Some(Arc::clone(&delivered));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("settled selected-observation completion unlocks dependent work");

    let delivered = delivered
        .lock()
        .expect("delivered observation completion lock");
    assert!(
        delivered.iter().any(|(consumer, owner)| {
            consumer == &WorkNodeId::new("read") && owner == &WorkNodeId::new("transaction-read")
        }),
        "the first physical consumer must receive its scheduler-retained T17 predecessor evidence"
    );
    assert!(
        delivered
            .iter()
            .all(|(_, owner)| owner == &WorkNodeId::new("transaction-read"))
    );
}

#[test]
fn synchronous_observation_completion_is_exactly_once_attempt_node_and_lease_bound() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work_with_synchronous_observation_read(6)),
    )
    .expect("synchronous ObservationRead is valid physical work");
    let read = &execution_plan.execution_dag().nodes()[&WorkNodeId::new("transaction-read")];
    assert!(read.fences.is_empty());
    assert!(
        read.claims
            .iter()
            .all(|claim| claim.lifetime == ClaimLifetime::Work),
        "a fence-free read must retain each source claim through synchronous work completion"
    );
    assert!(
        execution_plan
            .observation_transaction()
            .work()
            .observation_reads()
            .contains(&WorkDependency::Work(read.id.clone()))
    );
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let receipts = execution_plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([159; 32]);
    let completions = Arc::new(Mutex::new(Vec::new()));
    let mut executor = recording_executor(6, None, None);
    executor.observation_completions = Some(Arc::clone(&completions));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([160; 32]),
        )),
    )
    .expect("synchronous ObservationRead completion must precede dependent work");

    let completions = completions.lock().expect("observation completion lock");
    assert_eq!(completions.len(), 1);
    let completion = &completions[0];
    assert_eq!(completion.attempt_id, attempt);
    assert_eq!(completion.owner_node, WorkNodeId::new("transaction-read"));
    assert!(completion.settled_fences.is_empty());
    assert!(completion.lease_epoch > 0);
}

#[test]
fn actual_bound_observation_traversals_drive_both_weighting_generation_passes() {
    let problem = compile(request(198)).expect("logical weighting compilation");
    let weighting_plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let fragment = WeightingPlanFragment::new(
        &weighting_plan,
        WorkNodeId::new("transaction-read"),
        selected_content_resources(&problem),
        implementation(6),
        implementation(6),
        implementation(6),
    );
    let physical = fragment
        .compose(&physical_work_for_weighting_problem(&problem, 6))
        .expect("production weighting physical work");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .expect("production weighting execution plan");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.weighting_source_residency = Some(selected_content_residency(&problem));
    executor.weighting_plan = Some(weighting_plan);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("all three owner traversals and explicit release complete");

    let executor = &registry.executors[&implementation(6)];
    assert_eq!(executor.weighted_sample_count.load(Ordering::SeqCst), 1);
    assert!(
        executor
            .weighting_owner_at_replay_fence
            .load(Ordering::SeqCst)
    );
    assert!(
        executor
            .weighting_owner_at_reconciliation
            .load(Ordering::SeqCst)
    );
    assert!(executor.weighting_owner_at_release.load(Ordering::SeqCst));
    assert!(executor.weighting_reconciled.load(Ordering::SeqCst));
    assert!(executor.weighting_released.load(Ordering::SeqCst));
    assert!(
        executor
            .weighting_state
            .lock()
            .expect("weighting execution state lock")
            .is_empty()
    );
}

#[test]
fn failed_weighting_release_quarantines_the_actual_selected_observation_owner() {
    const CHILD_PROCESS: &str = "CASA_RS_T18_FAILED_RELEASE_CHILD";
    if std::env::var_os(CHILD_PROCESS).is_none() {
        let status =
            Command::new(std::env::current_exe().expect("current integration-test binary"))
                .args([
                    "--exact",
                    "failed_weighting_release_quarantines_the_actual_selected_observation_owner",
                    "--nocapture",
                ])
                .env(CHILD_PROCESS, "1")
                .status()
                .expect("isolated failed-release integration test");
        assert!(
            status.success(),
            "isolated failed-release integration test must pass"
        );
        return;
    }

    let problem = compile(request(198)).expect("logical weighting compilation");
    let weighting_plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let fragment = WeightingPlanFragment::new(
        &weighting_plan,
        WorkNodeId::new("transaction-read"),
        selected_content_resources(&problem),
        implementation(6),
        implementation(6),
        implementation(6),
    );
    let release = fragment.release_node().clone();
    let physical = fragment
        .compose(&physical_work_for_weighting_problem(&problem, 6))
        .expect("production weighting physical work");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .expect("production weighting execution plan");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.weighting_source_residency = Some(selected_content_residency(&problem));
    executor.weighting_plan = Some(weighting_plan);
    executor.weighting_failure_node = Some(release.clone());
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([201; 32]),
        BuildIdentity::from_sha256([202; 32]),
    );

    let error = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect_err("failed success and cleanup release must quarantine ownership");

    assert!(matches!(error, RunError::Execution { node, .. } if node == release));
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("failed release remains durably receipted");
    let selected_content_allocation = selected_content_allocations()
        .into_iter()
        .next()
        .expect("one selected-content allocation");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(receipt.node_status(&release), Some(ReceiptStatus::Failed));
    assert_eq!(
        receipt
            .allocation_uses(&release)
            .and_then(|uses| uses.get(&selected_content_allocation).cloned()),
        Some(ClaimLifetime::Work)
    );
    let executor = &registry.executors[&implementation(6)];
    let state = executor
        .weighting_state
        .lock()
        .expect("weighting execution state lock");
    assert!(state.has_retained_observation());
    assert!(!state.is_empty());
    assert!(!executor.weighting_released.load(Ordering::SeqCst));
    assert!(!executor.weighting_cleanup_released.load(Ordering::SeqCst));
}

#[test]
fn failed_weighting_lifecycle_cuts_run_the_scheduler_owned_release() {
    #[derive(Clone, Copy, Debug)]
    enum FailureCut {
        SourceFence,
        GenerationFence,
        ReplayWork,
        ReplayFence,
        Reconciliation,
    }

    for (cut_index, cut) in [
        FailureCut::SourceFence,
        FailureCut::GenerationFence,
        FailureCut::ReplayWork,
        FailureCut::ReplayFence,
        FailureCut::Reconciliation,
    ]
    .into_iter()
    .enumerate()
    {
        let problem = compile(request(198)).expect("logical weighting compilation");
        let weighting_plan = plan_weighting(
            &problem,
            WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
        )
        .expect("weighting plan");
        let fragment = WeightingPlanFragment::new(
            &weighting_plan,
            WorkNodeId::new("transaction-read"),
            selected_content_resources(&problem),
            implementation(6),
            implementation(6),
            implementation(6),
        );
        let generation = fragment.generation_node().clone();
        let replay = fragment.replay_node().clone();
        let release = fragment.release_node().clone();
        let source = WorkNodeId::new("transaction-read");
        let expected_failure = match cut {
            FailureCut::SourceFence => source.clone(),
            FailureCut::GenerationFence => generation.clone(),
            FailureCut::ReplayWork | FailureCut::ReplayFence => replay.clone(),
            FailureCut::Reconciliation => WorkNodeId::new("transaction-reconciliation"),
        };
        let physical = fragment
            .compose(&physical_work_for_weighting_problem(&problem, 6))
            .expect("production weighting physical work");
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(physical),
        )
        .expect("production weighting execution plan");
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let mut executor = recording_executor(6, None, None);
        executor.weighting_source_residency = Some(selected_content_residency(&problem));
        executor.weighting_plan = Some(weighting_plan);
        match cut {
            FailureCut::SourceFence => {
                executor.weighting_fence_failure_event = Some((source, FenceKind::Io));
            }
            FailureCut::GenerationFence => {
                executor.weighting_fence_failure_event = Some((generation, FenceKind::Io));
            }
            FailureCut::ReplayWork => executor.weighting_failure_node = Some(replay),
            FailureCut::ReplayFence => {
                executor.weighting_fence_failure_event = Some((replay, FenceKind::Io));
            }
            FailureCut::Reconciliation => {
                executor.weighting_failure_node = Some(expected_failure.clone());
            }
        }
        let registry = TestRegistry {
            id: registry(3),
            metadata: implementation_metadata(&problem),
            executors: BTreeMap::from([(implementation(6), executor)]),
        };
        let mut controller = RunToCompletion;
        let receipts = execution_plan.receipt_store();
        let attempt_byte = u8::try_from(210 + cut_index).expect("bounded failure-cut index");
        let provenance = execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([attempt_byte; 32]),
            BuildIdentity::from_sha256([220; 32]),
        );

        let error = run_receipted(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut controller,
            receipts.bind(provenance.clone()),
        )
        .expect_err("the injected lifecycle cut must fail the attempt");

        assert!(
            matches!(&error, RunError::Execution { node, .. } if node == &expected_failure),
            "unexpected {cut:?} failure: {error:?}"
        );
        let receipt = receipts
            .open(provenance.attempt_id())
            .expect("failed lifecycle cut remains durably receipted");
        let selected_content_allocation = selected_content_allocations()
            .into_iter()
            .next()
            .expect("one selected-content allocation");
        assert_eq!(receipt.status(), ReceiptStatus::Failed);
        assert_eq!(
            receipt.node_status(&release),
            Some(ReceiptStatus::Completed)
        );
        assert_eq!(
            receipt
                .allocation_uses(&release)
                .and_then(|uses| uses.get(&selected_content_allocation).cloned()),
            Some(ClaimLifetime::Work)
        );
        let release_buffer = LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead);
        assert_eq!(
            receipt.actual_resource_peak(&release, &release_buffer, &ClaimLifetime::Work),
            Some(SELECTED_CONTENT_BYTES as u64)
        );
        let executor = &registry.executors[&implementation(6)];
        assert!(
            executor
                .weighting_state
                .lock()
                .expect("weighting execution state lock")
                .is_empty(),
            "{cut:?} must not retain weighting state after draining"
        );
        assert!(executor.weighting_released.load(Ordering::SeqCst));
        assert!(executor.weighting_cleanup_released.load(Ordering::SeqCst));
    }
}

#[test]
fn weighting_replay_rejects_a_fresh_binding_with_identical_selected_content() {
    let problem = compile(request(198)).expect("logical weighting compilation");
    let weighting_plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let fragment = WeightingPlanFragment::new(
        &weighting_plan,
        WorkNodeId::new("transaction-read"),
        selected_content_resources(&problem),
        implementation(6),
        implementation(6),
        implementation(6),
    );
    let replay = fragment.replay_node().clone();
    let physical = fragment
        .compose(&physical_work_for_weighting_problem(&problem, 6))
        .expect("production weighting physical work");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .expect("production weighting execution plan");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.weighting_source_residency = Some(selected_content_residency(&problem));
    executor.weighting_plan = Some(weighting_plan);
    executor.reopen_weighting_before_replay = true;
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("content identity cannot substitute for retained owner identity");

    assert!(matches!(error, RunError::Execution { node, .. } if node == replay));
    let executor = &registry.executors[&implementation(6)];
    assert_eq!(executor.weighted_sample_count.load(Ordering::SeqCst), 0);
    assert!(
        executor
            .weighting_state
            .lock()
            .expect("weighting execution state lock")
            .is_empty()
    );
    assert!(executor.weighting_cleanup_released.load(Ordering::SeqCst));
    assert!(!executor.weighting_reconciled.load(Ordering::SeqCst));
}

#[test]
fn weighting_generation_rejects_missing_direct_predecessor_before_state_exists() {
    let problem = compile(request(198)).expect("logical weighting compilation");
    let weighting_plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let fragment = WeightingPlanFragment::new(
        &weighting_plan,
        WorkNodeId::new("transaction-read"),
        selected_content_resources(&problem),
        implementation(6),
        implementation(6),
        implementation(6),
    );
    let generation = fragment.generation_node().clone();
    let physical = fragment
        .compose(&physical_work_for_weighting_problem(&problem, 6))
        .expect("production weighting physical work");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .expect("production weighting execution plan");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.weighting_source_residency = Some(selected_content_residency(&problem));
    executor.weighting_plan = Some(weighting_plan);
    executor.weighting_source_read = WorkNodeId::new("not-a-direct-predecessor");
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("generation must not begin without its direct T17 predecessor");

    assert!(matches!(error, RunError::Execution { node, .. } if node == generation));
    let executor = &registry.executors[&implementation(6)];
    assert!(
        executor
            .weighting_state
            .lock()
            .expect("weighting execution state lock")
            .is_empty()
    );
    assert!(executor.weighting_cleanup_released.load(Ordering::SeqCst));
    assert_eq!(executor.weighted_sample_count.load(Ordering::SeqCst), 0);
}

#[test]
fn weighting_generation_rejects_mismatched_allocation_capabilities_before_state_exists() {
    let problem = compile(request(198)).expect("logical weighting compilation");
    let planned_weighting = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("planned weighting limits"),
    )
    .expect("planned weighting");
    let fragment = WeightingPlanFragment::new(
        &planned_weighting,
        WorkNodeId::new("transaction-read"),
        selected_content_resources(&problem),
        implementation(6),
        implementation(6),
        implementation(6),
    );
    let generation = fragment.generation_node().clone();
    let physical = fragment
        .compose(&physical_work_for_weighting_problem(&problem, 6))
        .expect("production weighting physical work");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .expect("production weighting execution plan");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let differently_sized_weighting = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 3).expect("different weighting limits"),
    )
    .expect("differently sized weighting");
    let mut executor = recording_executor(6, None, None);
    executor.weighting_source_residency = Some(selected_content_residency(&problem));
    executor.weighting_plan = Some(differently_sized_weighting);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("generation must not begin with different allocation capacities");

    assert!(matches!(error, RunError::Execution { node, .. } if node == generation));
    let executor = &registry.executors[&implementation(6)];
    assert!(
        executor
            .weighting_state
            .lock()
            .expect("weighting execution state lock")
            .is_empty()
    );
    assert_eq!(executor.weighted_sample_count.load(Ordering::SeqCst), 0);
}

fn assert_source_residency_mismatch_fails_before_traversal(
    problem: &casa_imaging_model::CompiledProblem,
    planned: SelectedObservationResidencyCertificate,
    actual: SelectedObservationResidencyCertificate,
) {
    let weighting_plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let fragment = WeightingPlanFragment::new(
        &weighting_plan,
        WorkNodeId::new("transaction-read"),
        SelectedObservationSourceResources::new(
            planned.clone(),
            selected_content_allocations(),
            selected_content_queue(),
        ),
        implementation(6),
        implementation(6),
        implementation(6),
    );
    let physical = fragment
        .compose(&physical_work_for_weighting_problem_with_residency(
            problem, 6, &planned,
        ))
        .expect("planned source residency composes");
    let execution_plan = plan(
        problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .expect("plan with mismatched runtime owner");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.weighting_source_residency = Some(planned);
    executor.weighting_actual_source_residency = Some(actual);
    executor.weighting_plan = Some(weighting_plan);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let error = run(
        problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("a different owner residency certificate must fail closed");

    assert!(
        matches!(error, RunError::Execution { node, .. } if node == WorkNodeId::new("transaction-read"))
    );
    let executor = &registry.executors[&implementation(6)];
    assert_eq!(
        executor
            .weighting_source_sample_count
            .load(Ordering::SeqCst),
        0,
        "owner mismatch must fail before the first selected sample"
    );
    assert!(
        executor
            .weighting_state
            .lock()
            .expect("weighting execution state lock")
            .is_empty()
    );
}

#[test]
fn weighting_source_rejects_incomplete_and_differently_budgeted_owner_certificates() {
    let problem = compile(request_with_source_count(230, 2)).expect("multi-source compilation");
    let planned = selected_content_residency_with(&problem, |source_index| {
        SelectedObservationContentBudget::new(
            (source_index + 1) * SELECTED_CONTENT_BYTES,
            source_index + 1,
            4,
        )
    });
    let differently_budgeted = selected_content_residency_with(&problem, |source_index| {
        SelectedObservationContentBudget::new(
            if source_index == 0 {
                SELECTED_CONTENT_BYTES
            } else {
                3 * SELECTED_CONTENT_BYTES
            },
            source_index + 1,
            4,
        )
    });
    assert_source_residency_mismatch_fails_before_traversal(
        &problem,
        planned.clone(),
        differently_budgeted,
    );

    let single_source_problem =
        compile(request(231)).expect("single-source certificate compilation");
    assert_source_residency_mismatch_fails_before_traversal(
        &problem,
        selected_content_residency(&single_source_problem),
        planned,
    );
}

#[test]
fn multi_source_weighting_receipts_certified_aggregate_residency_through_release() {
    let problem = compile(request_with_source_count(232, 2)).expect("multi-source compilation");
    let residency = selected_content_residency_with(&problem, |source_index| {
        SelectedObservationContentBudget::new(
            (source_index + 1) * SELECTED_CONTENT_BYTES,
            source_index + 1,
            4,
        )
    });
    let aggregate_bytes = u64::try_from(residency.aggregate_resident_bytes())
        .expect("aggregate source residency fits u64");
    let peak_blocks =
        u64::try_from(residency.peak_live_blocks()).expect("source queue depth fits u64");
    assert_eq!(aggregate_bytes, 3 * SELECTED_CONTENT_BYTES as u64);
    assert_eq!(peak_blocks, 2);
    let weighting_plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let fragment = WeightingPlanFragment::new(
        &weighting_plan,
        WorkNodeId::new("transaction-read"),
        SelectedObservationSourceResources::new(
            residency.clone(),
            selected_content_allocations(),
            selected_content_queue(),
        ),
        implementation(6),
        implementation(6),
        implementation(6),
    );
    let source = WorkNodeId::new("transaction-read");
    let generation = fragment.generation_node().clone();
    let replay = fragment.replay_node().clone();
    let release = fragment.release_node().clone();
    let base = physical_work_for_weighting_problem_with_residency(&problem, 6, &residency);
    let physical = fragment
        .compose(&base)
        .expect("owner-certified multi-source weighting work");
    assert_eq!(
        physical.execution_dag().logical_allocations().len()
            - base.execution_dag().logical_allocations().len(),
        5,
        "the existing selected-source allocation is reused"
    );
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .expect("plan owner-certified multi-source weighting");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.weighting_source_residency = Some(residency);
    executor.weighting_plan = Some(weighting_plan);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([233; 32]),
        BuildIdentity::from_sha256([234; 32]),
    );

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("owner-certified multi-source weighting completes");
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("multi-source weighting receipt");
    let io_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let buffer = LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead);
    let queue = selected_content_queue();
    for node in [&source, &generation, &replay] {
        assert_eq!(
            receipt.planned_resource_amount(node, &buffer, &io_lifetime),
            Some(aggregate_bytes)
        );
        assert_eq!(
            receipt.actual_resource_peak(node, &buffer, &io_lifetime),
            Some(aggregate_bytes)
        );
        assert_eq!(
            receipt.planned_resource_amount(node, &queue, &io_lifetime),
            Some(peak_blocks)
        );
        assert_eq!(
            receipt.actual_resource_peak(node, &queue, &io_lifetime),
            Some(peak_blocks)
        );
    }
    assert_eq!(
        receipt.planned_resource_amount(&release, &buffer, &ClaimLifetime::Work),
        Some(aggregate_bytes)
    );
    assert_eq!(
        receipt.actual_resource_peak(&release, &buffer, &ClaimLifetime::Work),
        Some(aggregate_bytes)
    );
    let selected_allocation = selected_content_allocations()
        .pop_first()
        .expect("one selected-source allocation");
    assert_eq!(
        receipt
            .allocation_uses(&release)
            .and_then(|uses| uses.get(&selected_allocation).cloned()),
        Some(ClaimLifetime::Work)
    );
    let executor = &registry.executors[&implementation(6)];
    assert_eq!(
        executor
            .weighting_source_sample_count
            .load(Ordering::SeqCst),
        2,
        "both certified sources traverse before retention"
    );
    assert!(executor.weighting_released.load(Ordering::SeqCst));
    assert!(
        executor
            .weighting_state
            .lock()
            .expect("weighting execution state lock")
            .is_empty()
    );
}

#[test]
fn owner_traversed_weighting_freezes_only_at_settled_plan_node_and_lease() {
    let problem = compile(request(1)).expect("logical weighting compilation");
    let weighting_plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting residency plan");
    let pathlike_implementation =
        WorkImplementationId::new("/private/t18/selected-observation-owner");
    let base = with_work_implementation(
        &problem,
        &physical_work_for_weighting_problem(&problem, 6),
        pathlike_implementation.clone(),
    );
    let fragment = WeightingPlanFragment::new(
        &weighting_plan,
        WorkNodeId::new("transaction-read"),
        selected_content_resources(&problem),
        pathlike_implementation.clone(),
        pathlike_implementation.clone(),
        pathlike_implementation.clone(),
    );
    let generation = fragment.generation_node().clone();
    let replay = fragment.replay_node().clone();
    let release = fragment.release_node().clone();
    let physical = fragment
        .compose(&base)
        .expect("production weighting physical work");
    let source = WorkNodeId::new("transaction-read");
    let retained_lifetime = ClaimLifetime::retained_until(release.clone());
    let retained_resources = physical.execution_dag().nodes()[&source]
        .claims
        .iter()
        .filter(|claim| {
            matches!(
                claim.resource,
                LeaseResource::MeasurementSetLock { .. } | LeaseResource::FileDescriptors
            )
        })
        .map(|claim| (claim.resource.clone(), claim.amount))
        .collect::<BTreeSet<_>>();
    let weighting_allocations = physical
        .execution_dag()
        .logical_allocations()
        .keys()
        .filter(|allocation| {
            !base
                .execution_dag()
                .logical_allocations()
                .contains_key(*allocation)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let weighting_slots = physical
        .execution_dag()
        .physical_slots()
        .keys()
        .filter(|slot| !base.execution_dag().physical_slots().contains_key(*slot))
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected_uses = [&source, &generation, &replay, &release]
        .into_iter()
        .map(|node| {
            (
                node.clone(),
                physical.execution_dag().nodes()[node]
                    .allocations
                    .iter()
                    .map(|usage| (usage.allocation.clone(), usage.lifetime.clone()))
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical),
    )
    .expect("plan with production weighting lifecycle");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.id = pathlike_implementation.clone();
    executor.weighting_source_residency = Some(selected_content_residency(&problem));
    executor.weighting_plan = Some(weighting_plan);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(pathlike_implementation.clone(), executor)]),
    };
    let mut controller = RunToCompletion;
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([199; 32]),
        BuildIdentity::from_sha256([200; 32]),
    );

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("scheduler-authorized weighting lifecycle completes");
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("weighting execution receipt");
    assert_eq!(weighting_allocations.len(), 5);
    assert!(weighting_allocations.is_subset(&receipt.allocation_generation_identities()));
    assert_eq!(weighting_slots.len(), 5);
    assert!(weighting_slots.is_subset(&receipt.physical_slot_identities()));
    for (node, uses) in expected_uses {
        assert_eq!(receipt.allocation_uses(&node), Some(uses));
    }
    let selected_content_allocation = selected_content_allocations()
        .pop_first()
        .expect("one selected-content allocation");
    assert!(
        receipt
            .allocation_generation_identities()
            .contains(&selected_content_allocation)
    );
    assert_eq!(
        receipt
            .allocation_uses(&release)
            .and_then(|uses| uses.get(&selected_content_allocation).cloned()),
        Some(ClaimLifetime::Work)
    );
    let io_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let source_resources = [
        (LeaseResource::Workers, ClaimLifetime::Work, 1),
        (
            LeaseResource::Queue {
                demand_id: "transaction-io-queue".to_string(),
            },
            io_lifetime.clone(),
            1,
        ),
        (
            LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead),
            io_lifetime,
            SELECTED_CONTENT_BYTES as u64,
        ),
    ];
    for node in [&generation, &replay] {
        for (resource, lifetime, amount) in &source_resources {
            assert_eq!(
                receipt.planned_resource_amount(node, resource, lifetime),
                Some(*amount)
            );
            assert_eq!(
                receipt.actual_resource_peak(node, resource, lifetime),
                Some(*amount)
            );
        }
    }
    for node in [&source, &generation, &replay, &release] {
        for (resource, amount) in &retained_resources {
            assert_eq!(
                receipt.planned_resource_amount(node, resource, &retained_lifetime),
                Some(*amount)
            );
            assert_eq!(
                receipt.actual_resource_peak(node, resource, &retained_lifetime),
                Some(*amount)
            );
        }
    }
    let release_buffer = LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead);
    assert_eq!(
        receipt.planned_resource_amount(&release, &release_buffer, &ClaimLifetime::Work),
        Some(SELECTED_CONTENT_BYTES as u64)
    );
    assert_eq!(
        receipt.actual_resource_peak(&release, &release_buffer, &ClaimLifetime::Work),
        Some(SELECTED_CONTENT_BYTES as u64)
    );

    let executor = registry
        .executors
        .get(&pathlike_implementation)
        .expect("weighting executor");
    assert!(executor.weighting_reconciled.load(Ordering::SeqCst));
    assert!(executor.weighting_released.load(Ordering::SeqCst));
    assert!(!executor.weighting_cleanup_released.load(Ordering::SeqCst));
    assert!(
        executor
            .weighting_state
            .lock()
            .expect("weighting execution state lock")
            .is_empty(),
        "the explicit release node must consume the complete weighting lifecycle"
    );

    let path = only_receipt_path(receipts.root_path());
    let original = fs::read_to_string(&path).expect("serialized weighting receipt");
    assert!(original.contains("redacted:"));
    assert!(!original.contains(pathlike_implementation.as_str()));
    for tamper in [
        RetainedClaimTamper::RemoveClaim,
        RetainedClaimTamper::ChangeAmount,
        RetainedClaimTamper::WrongRelease,
        RetainedClaimTamper::AddUnorderedUse,
        RetainedClaimTamper::AddPostReleaseUse,
        RetainedClaimTamper::ChangeDagIdentity,
    ] {
        fs::write(&path, with_retained_claim_tamper(&original, tamper))
            .expect("rewrite checksum-valid weighting receipt");
        assert!(
            matches!(
                receipts.open(provenance.attempt_id()),
                Err(casa_imaging_runtime::ReceiptError::IntegrityMismatch)
            ),
            "{tamper:?} must fail canonical retained-topology validation"
        );
    }
}

#[test]
fn completion_from_a_different_compiled_observation_cannot_unlock_dependents() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work_with_synchronous_observation_read(6)),
    )
    .expect("synchronous ObservationRead is valid physical work");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.bind_foreign_observation_completion = true;
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("a foreign owner completion must not satisfy this ObservationRead node");

    assert!(matches!(
        error,
        RunError::Execution { node, .. } if node == WorkNodeId::new("transaction-read")
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        2,
        "no dependent numerical or publication work may launch"
    );
}

#[test]
fn failed_synchronous_observation_completion_prevents_dependent_work() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work_with_synchronous_observation_read(6)),
    )
    .expect("synchronous ObservationRead is valid physical work");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let completions = Arc::new(Mutex::new(Vec::new()));
    let mut executor = recording_executor(6, None, None);
    executor.observation_completions = Some(Arc::clone(&completions));
    executor.observation_completion_failure = Some("selected-observation completion failed");
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("failed synchronous selected-observation completion must fail the attempt");

    assert!(matches!(
        error,
        RunError::Execution { node, .. } if node == WorkNodeId::new("transaction-read")
    ));
    assert_eq!(
        completions
            .lock()
            .expect("observation completion lock")
            .len(),
        1,
        "the failing affine completion hook is still invoked exactly once"
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        2,
        "only the initial check and synchronous ObservationRead may execute"
    );
}

#[test]
fn failed_observation_fence_cannot_mint_attempt_bound_completion() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let completions = Arc::new(Mutex::new(Vec::new()));
    let mut executor = failing_transaction_executor(
        6,
        Arc::new(AtomicUsize::new(0)),
        None,
        Some(("transaction-read", FenceKind::Io)),
        None,
    );
    executor.observation_completions = Some(Arc::clone(&completions));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("failed observation fence must fail the attempt");

    assert!(
        completions
            .lock()
            .expect("observation completion lock")
            .is_empty(),
        "physical fence failure cannot mint selected-observation completion"
    );
}

#[test]
fn transaction_failures_leave_the_old_generation_visible() {
    for (label, failure_node, fence_failure_event, publication_failure) in [
        ("input mutation", Some("transaction-check"), None, None),
        (
            "numerical reconciliation",
            Some("transaction-reconciliation"),
            None,
            None,
        ),
        ("product output", Some("transaction-stage-psf"), None, None),
        (
            "atomic commit publication fence",
            None,
            Some(("transaction-commit", FenceKind::Publication)),
            None,
        ),
        (
            "atomic commit I/O fence",
            None,
            Some(("transaction-commit", FenceKind::Io)),
            None,
        ),
        (
            "atomic publication",
            None,
            None,
            Some("publication failure"),
        ),
    ] {
        let problem = compile(request(1)).expect("logical compilation");
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
        .expect("physical planning");
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let visible_generation = Arc::new(AtomicUsize::new(0));
        let registry = TestRegistry {
            id: registry(3),
            metadata: implementation_metadata(&problem),
            executors: BTreeMap::from([(
                implementation(6),
                failing_transaction_executor(
                    6,
                    Arc::clone(&visible_generation),
                    failure_node,
                    fence_failure_event,
                    publication_failure,
                ),
            )]),
        };
        let mut completion = RunToCompletion;
        run(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut completion,
        )
        .expect_err(label);

        assert_eq!(
            visible_generation.load(Ordering::SeqCst),
            0,
            "{label} cannot expose staged output"
        );
    }

    let problem = compile(request_with_model_write(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work_with_model_staging(6)),
    )
    .expect("model-write physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let model_registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(
            implementation(6),
            failing_transaction_executor(
                6,
                Arc::clone(&visible_generation),
                None,
                Some(("transaction-stage-model", FenceKind::Writeback)),
                None,
            ),
        )]),
    };
    let mut completion = RunToCompletion;
    run(
        &problem,
        &execution_plan,
        &current,
        &model_registry,
        authority(),
        &mut completion,
    )
    .expect_err("model-column writeback failure");

    assert_eq!(
        visible_generation.load(Ordering::SeqCst),
        0,
        "model-column writeback failure cannot expose staged output"
    );

    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let admission_registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(
            implementation(6),
            failing_transaction_executor(6, Arc::clone(&visible_generation), None, None, None),
        )]),
    };
    let mut completion = RunToCompletion;
    let admission_receipts = execution_plan.receipt_store();
    let pressure_guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    authority()
        .update_external_pressure(runtime_inventory(0).pressure)
        .expect("install zero-lock external pressure");

    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem");
    let result = runtime_run(
        &executable,
        &execution_plan,
        &current,
        &admission_registry,
        authority(),
        &mut completion,
        admission_receipts.bind(execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([243; 32]),
            BuildIdentity::from_sha256([244; 32]),
        )),
    );
    authority()
        .update_external_pressure(runtime_inventory(4).pressure)
        .expect("restore external pressure");
    drop(pressure_guard);
    let error = result.expect_err("resource admission must fail before transaction work");

    assert!(matches!(
        error,
        RunError::Scheduler(ExecutionError::Resource(
            ResourceError::NoFeasibleAlternative(certificate),
        )) if matches!(certificate.rejections(), [rejection]
            if rejection.alternative() == &AlternativeId::new("test-cpu")
                && matches!(rejection.reason(),
                    AlternativeRejectionReason::Infeasible { resource, required: 1, available: 0 }
                    if resource == "locks"))
    ));
    assert_eq!(visible_generation.load(Ordering::SeqCst), 0);
    assert_eq!(
        admission_registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0,
        "failed admission cannot launch mutation or publication work"
    );

    // The receipt produced by the real run seam is the only source accepted
    // for historical quantitative constraints. Replaying it at the same
    // pressure is reported as a recorded refusal rather than a fresh
    // synthetic admission failure.
    let pressure_guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    authority()
        .update_external_pressure(runtime_inventory(0).pressure)
        .expect("reinstall zero-lock pressure for receipt replay");
    let replay_registry = ContractOnlyRegistry::new(
        registry(3),
        implementation_metadata(&problem),
        [implementation(6)],
    );
    let replay = runtime_plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        authority(),
        &replay_registry,
        &admission_receipts,
        |_, _| Ok::<_, io::Error>(vec![physical_work(6)]),
    );
    authority()
        .update_external_pressure(runtime_inventory(4).pressure)
        .expect("restore pressure after receipt replay");
    drop(pressure_guard);
    assert!(matches!(
        replay,
        Err(PlanError::Resource(ResourceError::NoFeasibleAlternative(certificate)))
            if matches!(certificate.rejections(), [rejection]
                if matches!(rejection.reason(), AlternativeRejectionReason::RecordedFailure {
                    attempt,
                    status: ReceiptStatus::Infeasible,
                } if *attempt == casa_imaging_runtime::ExecutionAttemptId::from_sha256([243; 32])))
    ));
}

#[test]
fn release_failures_drain_independent_fences_and_quarantine_only_failed_slots() {
    let problem = compile(request(1)).expect("logical compilation");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    for fail_at_fence in [false, true] {
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(release_failure_physical_work(6, 8, fail_at_fence)),
        )
        .expect("external-release failure planning");
        let mut release_executor = recording_executor(
            8,
            (!fail_at_fence).then_some("release execute failed"),
            fail_at_fence.then_some("release fence failed"),
        );
        if fail_at_fence {
            release_executor.measurements.insert(
                WorkNodeId::new("a-release-mapping"),
                (
                    vec![IoMeasurement::new(IoBufferKind::MappedPageCache, 100, 1)],
                    Vec::new(),
                ),
            );
        }
        let mut prepare_executor = recording_executor(6, None, None);
        prepare_executor.measurements.insert(
            WorkNodeId::new(if fail_at_fence {
                "0-prepare-mapping"
            } else {
                "1-prepare-mapping"
            }),
            (
                vec![IoMeasurement::new(IoBufferKind::MappedPageCache, 100, 1)],
                Vec::new(),
            ),
        );
        let executor_registry = TestRegistry {
            id: registry(3),
            metadata: implementation_metadata(&problem),
            executors: BTreeMap::from([
                (implementation(6), prepare_executor),
                (implementation(8), release_executor),
            ]),
        };
        let mut controller = RunToCompletion;

        let error = run(
            &problem,
            &execution_plan,
            &current,
            &executor_registry,
            authority(),
            &mut controller,
        )
        .expect_err("failed external release must remain the primary error after drain");

        assert!(
            matches!(
                &error,
                RunError::Execution { node, .. }
                    if node.as_str().contains("release-mapping")
            ),
            "unexpected release failure: {error:?}"
        );
        assert_eq!(
            executor_registry.executors[&implementation(6)]
                .fence_waits
                .load(Ordering::SeqCst),
            2,
            "the transaction-read and independent I/O fences must drain after a Release failure"
        );

        let readmission_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
        .expect("post-quarantine planning");
        let mut completion = RunToCompletion;
        assert_eq!(
            run(
                &problem,
                &readmission_plan,
                &current,
                &executor_registry,
                authority(),
                &mut completion,
            )
            .expect("only the failed physical slot remains quarantined"),
            ExecutionOutcome::Succeeded
        );
    }
}

#[test]
fn run_persists_a_reopenable_receipt_with_exact_identities_and_every_plan_node() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([9; 32]),
        BuildIdentity::from_sha256([10; 32]),
    );
    let mut controller = RunToCompletion;

    let outcome = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen durable receipt");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert_eq!(receipt.schema_version(), 14);
    assert_eq!(receipt.route_matrix_schema_version(), 1);
    assert_eq!(receipt.route_matrix_contract_revision(), 1);
    assert_eq!(receipt.route_disposition(), "native");
    assert_eq!(
        receipt.route_requirement_identities(),
        vec!["capability.compiled-problem"]
    );
    assert_eq!(
        receipt
            .route_requirement("capability.compiled-problem")
            .expect("lossless routed row"),
        ExecutionRouteRequirement::new(
            "capability.compiled-problem",
            ExecutionRouteRequirementKind::Capability,
            ExecutionRouteDisposition::Native,
            casa_imaging_runtime::ExecutionRouteRequirementEvidence {
                current_owner: "crates/casa-imaging-model".to_string(),
                destination_tickets: vec!["T05/#491".to_string()],
                evidence_issues: vec![486, 491],
                baseline_manifests: vec![
                    "repo://crates/casa-imaging-model/src/lib.rs".to_string(),
                ],
                acceptance_contract: "compiled-problem-foundation-v1".to_string(),
                transfer_point: "immutable backend-independent logical problem and stable identity landed in Wave 1".to_string(),
                deletion_condition: "not applicable; canonical logical-problem owner".to_string(),
                source_evidence: vec![
                    "crates/casa-imaging-model/src/lib.rs::CompiledProblem".to_string(),
                ],
                obligation_ticket: None,
                obligation_reason: None,
            },
        )
        .expect("authoritative routed row")
    );
    assert_eq!(receipt.status(), ReceiptStatus::Completed);
    assert_eq!(receipt.plan_identity(), execution_plan.plan_id().as_bytes());
    assert_eq!(receipt.problem_identity(), problem.problem_id().as_bytes());
    assert_eq!(
        receipt.product_graph_identity(),
        problem.product_graph().graph_id().as_bytes()
    );
    assert_eq!(
        receipt.geometry_identity(),
        problem.geometry().geometry_id().as_bytes()
    );
    assert_eq!(
        receipt.observation_identity(),
        problem.inputs().observation().identity().as_bytes()
    );
    assert_eq!(
        receipt.implementation_registry_identity(),
        registry.registry_id().as_bytes()
    );
    assert_eq!(
        receipt.resource_policy_identity(),
        execution_plan.resource_policy_id().as_bytes()
    );
    assert_eq!(receipt.cost_model_identity(), cost_model(4).as_bytes());
    assert_eq!(
        receipt.dag_identity(),
        execution_plan.physical_work_id().as_bytes()
    );
    assert_eq!(
        receipt.plan_node_count(),
        execution_plan.execution_dag().nodes().len()
    );
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Completed)
    );
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Completed)
    );
}

#[test]
fn receipt_rejects_checksum_valid_typed_projection_and_audit_forgery() {
    let problem = compile(request_with_products_and_initial_model(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual],
        ModelStateIdentity::Seed(identity(89)),
    ))
    .expect("two-product logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |problem, _| Ok::<_, ()>(physical_work_for_problem(problem, 6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([85; 32]),
        BuildIdentity::from_sha256([86; 32]),
    );
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let path = only_receipt_path(receipts.root_path());
    let original = fs::read_to_string(&path).expect("serialized receipt");
    let checksum_marker = "\"payload_sha256\": \"";
    let checksum_start =
        original.find(checksum_marker).expect("payload checksum") + checksum_marker.len();
    assert_eq!(
        &original[checksum_start..checksum_start + 64],
        payload_sha256(&original),
        "the raw-test checksum reproduces receipt canonicalization"
    );
    let reopened = receipts
        .open(provenance.attempt_id())
        .expect("valid receipt");
    assert_eq!(
        reopened.product_graph_schema_version(),
        problem.product_graph().schema_version()
    );
    assert_eq!(reopened.product_graph_node_ordinals(), &[0, 1]);
    assert_eq!(
        reopened.product_graph_publication_member_ordinals(),
        &[0, 1]
    );

    let cases = [
        (
            "forged graph identity",
            with_forged_product_graph_identity(original.clone()),
        ),
        (
            "coordinated reprojection identity and audit forgery",
            with_forged_reprojection_identity(original.clone()),
        ),
        (
            "coordinated model lifecycle identity and audit forgery",
            with_forged_model_lifecycle_identity(original.clone(), &"d".repeat(64)),
        ),
        (
            "coordinated model input and audit forgery",
            with_forged_model_input_source_identity(original.clone(), &"c".repeat(64)),
        ),
        (
            "zero model lifecycle identity sentinel",
            with_forged_model_lifecycle_identity(original.clone(), &"0".repeat(64)),
        ),
        (
            "zero model input identity sentinel",
            with_forged_model_input_source_identity(original.clone(), &"0".repeat(64)),
        ),
        (
            "coordinated zero problem-model and audit identity",
            with_forged_problem_model_and_audit_identity(original.clone(), &"0".repeat(64)),
        ),
        (
            "coordinated parent problem and audit identity forgery",
            with_forged_parent_problem_identity(original.clone(), &"b".repeat(64)),
        ),
        (
            "coordinated zero parent problem and audit identity",
            with_forged_parent_problem_identity(original.clone(), &"0".repeat(64)),
        ),
        (
            "missing publication member",
            with_usize_array(original.clone(), "publication_member_ordinals", &[0]),
        ),
        (
            "extra unknown publication member",
            with_usize_array(original.clone(), "publication_member_ordinals", &[0, 1, 2]),
        ),
        (
            "duplicate publication member",
            with_usize_array(original.clone(), "publication_member_ordinals", &[0, 0, 1]),
        ),
        (
            "reordered publication members",
            with_usize_array(original.clone(), "publication_member_ordinals", &[1, 0]),
        ),
        (
            "audit node ordinal contradicts the typed projection",
            with_forged_audit_field(original.clone(), "products.graph.nodes.0.ordinal", "1"),
        ),
        (
            "audit publication member contradicts the typed projection",
            with_forged_audit_field(
                original.clone(),
                "products.graph.publication.members.0",
                "1",
            ),
        ),
        (
            "audit reprojection contract contradicts the typed projection",
            with_forged_audit_field(
                original.clone(),
                "model_lifecycle.reprojection.direction_registry",
                "forged",
            ),
        ),
    ];
    for (case, document) in cases {
        fs::write(&path, document).expect("rewrite checksum-valid receipt");
        assert!(
            matches!(
                receipts.open(provenance.attempt_id()),
                Err(casa_imaging_runtime::ReceiptError::IntegrityMismatch)
            ),
            "{case} must fail canonical typed projection validation"
        );
    }
}

#[test]
fn effective_problem_projection_normalizes_signed_zero_like_canonical_identities() {
    let compile_with_robust = |robust| {
        compile(request_with_geometry_references_and_weighting(
            81,
            geometry(255.0),
            Vec::new(),
            WeightingContract::new(
                WeightingScheme::Briggs { robust },
                WeightDensityScope::GlobalSelection,
            ),
        ))
        .expect("logical compilation")
    };
    let positive_zero = compile_with_robust(0.0);
    let negative_zero = compile_with_robust(-0.0);

    assert_eq!(positive_zero.problem_id(), negative_zero.problem_id());
    assert_eq!(
        positive_zero.weighting().commitment_id(),
        negative_zero.weighting().commitment_id()
    );

    let positive_projection = CompiledProblemEvidence::project(&positive_zero);
    let negative_projection = CompiledProblemEvidence::project(&negative_zero);
    assert_eq!(
        positive_projection.field("weighting.scheme.robust"),
        Some("f64:0000000000000000")
    );
    assert_eq!(positive_projection, negative_projection);
}

#[test]
fn receipt_reopens_the_complete_versioned_effective_problem_projection() {
    let problem = compile(request_with_geometry_and_references(
        81,
        geometry(255.0),
        vec![(ReferenceDataKind::Measures, identity(82))],
    ))
    .expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |problem, _| Ok::<_, ()>(physical_work_for_problem(problem, 6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([83; 32]),
        BuildIdentity::from_sha256([84; 32]),
    );
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let reopened = receipts.open(provenance.attempt_id()).expect("receipt");
    let projected = reopened.compiled_problem_evidence();
    assert_eq!(
        reopened.product_graph_node_ordinals(),
        problem
            .product_graph()
            .nodes()
            .iter()
            .map(|node| node.node_id().ordinal())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        reopened.product_graph_publication_member_ordinals(),
        problem
            .product_graph()
            .publication()
            .members()
            .iter()
            .map(|member| member.ordinal())
            .collect::<Vec<_>>()
    );
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let data_generation = source
        .generations()
        .columns()
        .generation(MsColumnKind::Data)
        .expect("data generation")
        .to_string();
    let antenna_generation = source
        .generations()
        .metadata(MetadataTableKind::Antenna)
        .expect("antenna generation")
        .to_string();

    assert_eq!(projected.schema_version(), 9);
    assert_eq!(projected, &CompiledProblemEvidence::project(&problem));
    assert_eq!(
        reopened.model_lifecycle_identity(),
        problem.model_lifecycle().contract_id().as_bytes()
    );
    let lifecycle_identity = problem.model_lifecycle().contract_id().to_string();
    let target_shape_identity = problem.model_lifecycle().target().identity().to_string();
    assert_eq!(
        projected.field("model_lifecycle.identity"),
        Some(lifecycle_identity.as_str())
    );
    assert_eq!(
        projected.field("model_lifecycle.target_shape_identity"),
        Some(target_shape_identity.as_str())
    );
    assert_eq!(projected.field("model_lifecycle.input.kind"), Some("empty"));
    assert_eq!(
        projected.field("science.spectral.sampling"),
        Some("identity")
    );
    assert_eq!(
        projected.field("science.measurement_equation.instrument_response"),
        Some("scalar")
    );
    assert_eq!(
        projected.field("science.measurement_equation.inner_products.model"),
        Some("hermitian_euclidean")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.0.kind"),
        Some("spectral_basis")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.1.kind"),
        Some("polarization")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.2.kind"),
        Some("direction_dependent_response")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.3.kind"),
        Some("phase")
    );
    assert_eq!(
        projected.field("science.normal_equation.output.normalization"),
        Some("unnormalized")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.0"),
        Some("right_hand_side_a_star_w_d")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.1"),
        Some("residual_a_star_w_d_minus_a_x")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.2"),
        Some("normal_operator_a_star_w_a")
    );
    assert_eq!(
        projected.field("reconstruction.algorithm.kind"),
        Some("dirty")
    );
    assert_eq!(projected.field("weighting.scheme.kind"), Some("natural"));
    assert_eq!(
        projected.field("weighting.commitment.identity"),
        Some(
            problem
                .normal_equation()
                .weighting()
                .commitment_id()
                .to_string()
                .as_str()
        )
    );
    assert_eq!(
        projected.field("weighting.commitment.selected_observation"),
        Some(
            problem
                .selected_observation()
                .commitment_id()
                .to_string()
                .as_str()
        )
    );
    assert_eq!(
        projected.field("weighting.commitment.visibility_inner_product"),
        Some("hermitian_euclidean")
    );
    assert_eq!(
        projected.field("weighting.sources.0.flag_policy"),
        Some("flag_or_flag_row")
    );
    assert_eq!(
        projected.field("weighting.commitment.snapshot_identity"),
        Some(problem.inputs().observation().to_string().as_str())
    );
    assert_eq!(
        projected.field("weighting.sources.0.input_weight_column"),
        Some("weight")
    );
    assert_eq!(projected.field("products.requested.0"), Some("psf"));
    assert_eq!(
        projected.field("products.graph.identity"),
        Some(problem.product_graph().graph_id().to_string().as_str())
    );
    assert_eq!(projected.field("products.graph.schema_version"), Some("2"));
    assert_eq!(projected.field("products.graph.nodes.0.ordinal"), Some("0"));
    assert_eq!(
        projected.field("products.graph.publication.members.0"),
        Some("0")
    );
    assert_eq!(
        projected.field("products.normalization_boundary.input"),
        Some("unnormalized")
    );
    assert_eq!(
        projected.field("products.normalization_boundary.operations.0.kind"),
        Some("normalize")
    );
    assert_eq!(
        projected.field("products.normalization_boundary.operations.1.kind"),
        Some("convert_units")
    );
    assert_eq!(projected.field("numerics.reduction"), Some("compensated"));
    assert_eq!(
        projected.field("geometry.domains.0.direction.projection"),
        Some("sin")
    );
    assert_eq!(
        projected.field("observation.sources.0.selection.rows.selected_count"),
        Some("1")
    );
    assert_eq!(
        projected.field("observation.sources.0.generations.columns.data"),
        Some(data_generation.as_str())
    );
    assert_eq!(
        projected.field("observation.sources.0.generations.metadata.antenna"),
        Some(antenna_generation.as_str())
    );
    assert_eq!(
        projected.field("observation.reference_data.measures"),
        Some(identity(82).to_string().as_str())
    );
    assert_eq!(projected.field("observation.model.kind"), Some("empty"));
    assert!(projected.fields().len() > 80);
}

#[test]
fn receipt_reopens_the_complete_selected_plan_projection() {
    let problem = compile(request_with_geometry_and_references(
        1,
        geometry(255.0),
        vec![(ReferenceDataKind::Measures, identity(71))],
    ))
    .expect("logical compilation");
    let policy = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), 512)]),
        workers: Some(2),
        storage_bytes: BTreeMap::new(),
        rates_per_second: BTreeMap::from([(RateResourceId::new("io-rate"), 8)]),
        cache_bytes: Some(256),
        locks: Some(2),
        file_descriptors: Some(8),
        queue_slots: BTreeMap::from([(QueueResourceId::new("io-queue"), 2)]),
        accelerator_slots: BTreeMap::new(),
    });
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), policy.clone(), planning_profile(4)),
        |_, _| Ok::<_, ()>(auditable_physical_work(&problem, 6)),
    )
    .expect("auditable physical planning");
    let current = RunBindings::new(problem.inputs().clone(), &policy, cost_model(4));
    let cache_artifact = ArtifactIdentity::from_sha256([51; 32]);
    let mut executor = recording_executor(6, None, None);
    executor.measurements = BTreeMap::from([(
        WorkNodeId::new("first-major-work"),
        (
            Vec::new(),
            vec![artifact_measurement(
                cache_artifact,
                Some(cache_artifact),
                ArtifactDisposition::Built,
                64,
                None,
            )],
        ),
    )]);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([61; 32]),
        BuildIdentity::from_sha256([62; 32]),
    );
    let mut controller = AdaptAtMajorBoundary::default();

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let dag = execution_plan.execution_dag();
    let adaptation_id = AdaptationId::new("larger-batch");
    let adaptation = receipt
        .adaptation_projection(&adaptation_id)
        .expect("adaptation projection");

    assert_eq!(receipt.attempt_id(), provenance.attempt_id());
    assert_eq!(receipt.build_identity(), provenance.build_identity());
    assert_eq!(
        receipt.reference_identity(ReferenceDataKind::Measures),
        Some(identity(71).as_bytes())
    );
    assert_eq!(receipt.model_identity(), ModelStateIdentity::Empty);
    assert_eq!(
        receipt.numerics_identity(),
        problem.numerics_id().as_bytes()
    );
    assert_eq!(receipt.projected_resource_policy(), policy);
    assert_eq!(
        receipt.selected_alternative_projection(),
        dag.resource_alternative().clone()
    );
    assert_eq!(
        receipt.required_resource_capability_identities(),
        dag.required_resource_capabilities().clone()
    );
    assert_eq!(
        receipt.selected_implementation_identities(),
        dag.selected_implementations().clone()
    );
    assert_eq!(
        receipt.plan_node_identities(),
        dag.nodes().keys().cloned().collect()
    );
    assert_eq!(
        receipt.allocation_generation_identities(),
        dag.logical_allocations().keys().cloned().collect()
    );
    for node in dag.nodes().values() {
        let expected = node
            .allocations
            .iter()
            .map(|usage| (usage.allocation.clone(), usage.lifetime.clone()))
            .collect();
        assert_eq!(receipt.allocation_uses(&node.id), Some(expected));
    }
    assert_eq!(
        receipt.physical_slot_identities(),
        dag.physical_slots().keys().cloned().collect()
    );
    assert_eq!(
        receipt.artifact_identities(),
        execution_plan
            .artifacts()
            .iter()
            .map(PlannedArtifact::identity)
            .collect()
    );
    assert_eq!(
        receipt.cache_identities(),
        BTreeSet::from([CacheIdentity::from_sha256([52; 32])])
    );
    assert_eq!(
        receipt.initial_execution_knobs(),
        dag.initial_knobs().clone()
    );
    assert_eq!(
        receipt.adaptation_identities(),
        dag.adaptations().keys().cloned().collect()
    );
    assert_eq!(adaptation.transition(), &dag.adaptations()[&adaptation_id]);
    assert!(adaptation.was_applied());
    assert!(adaptation.applied_revision().is_some());
}

#[test]
fn receipt_compares_plan_predictions_with_actual_stage_resource_and_fence_use() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([11; 32]),
        BuildIdentity::from_sha256([12; 32]),
    );
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let read = WorkNodeId::new("read");
    let io_fence = FenceId::new(read.clone(), FenceKind::Io);

    assert_eq!(receipt.predicted_elapsed_nanos(), 700);
    assert_eq!(receipt.prediction_confidence_ppm(), 900_000);
    assert_eq!(receipt.prediction_uncertainty_count(), 1);
    assert_eq!(receipt.stage_predicted_elapsed_nanos(&read), Some(100));
    assert!(receipt.stage_actual_elapsed_nanos(&read).is_some());
    assert_eq!(
        receipt.planned_resource_amount(
            &read,
            &casa_imaging_runtime::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            &ClaimLifetime::through_fence(FenceKind::Io),
        ),
        Some(1)
    );
    assert_eq!(
        receipt.actual_resource_peak(
            &read,
            &casa_imaging_runtime::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            &ClaimLifetime::through_fence(FenceKind::Io),
        ),
        Some(1)
    );
    assert_eq!(
        receipt.fence_status(&io_fence),
        Some(ReceiptStatus::Completed)
    );
    assert!(receipt.fence_actual_elapsed_nanos(&io_fence).is_some());
}

#[test]
fn receipt_compares_planned_and_actual_io_artifacts_and_never_persists_paths() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(evidenced_physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let input = ArtifactIdentity::from_sha256([31; 32]);
    let cache = ArtifactIdentity::from_sha256([32; 32]);
    let output = ArtifactIdentity::from_sha256([34; 32]);
    let input_path = RedactedPath::from_path("/Users/private/secret-source.ms");
    let output_path = RedactedPath::from_path("/Volumes/private/secret-image.table");
    let mut executor = recording_executor(6, None, None);
    executor.measurements = BTreeMap::from([
        (
            WorkNodeId::new("read"),
            (
                vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
                vec![
                    artifact_measurement(
                        input,
                        Some(input),
                        ArtifactDisposition::Loaded,
                        4_096,
                        Some(input_path),
                    ),
                    artifact_measurement(
                        cache,
                        Some(cache),
                        ArtifactDisposition::Reused,
                        1_024,
                        None,
                    ),
                ],
            ),
        ),
        (
            WorkNodeId::new("transaction-commit"),
            (
                vec![IoMeasurement::new(IoBufferKind::Publication, 2_048, 1)],
                vec![artifact_measurement(
                    output,
                    Some(ArtifactIdentity::from_sha256([36; 32])),
                    ArtifactDisposition::Staged,
                    2_048,
                    Some(output_path),
                )],
            ),
        ),
    ]);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([37; 32]),
        BuildIdentity::from_sha256([38; 32]),
    );
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let read = WorkNodeId::new("read");
    let publish = WorkNodeId::new("transaction-commit");

    assert_eq!(
        receipt.stage_predicted_io(&read, IoBufferKind::SourceReadAhead),
        Some((8_192, 4))
    );
    assert_eq!(
        receipt.stage_actual_io(&read, IoBufferKind::SourceReadAhead),
        Some((4_096, 2))
    );
    assert_eq!(
        receipt.stage_predicted_io(&publish, IoBufferKind::Publication),
        Some((2_048, 1))
    );
    assert_eq!(
        receipt.stage_actual_io(&publish, IoBufferKind::Publication),
        Some((2_048, 1))
    );
    assert_eq!(receipt.artifact_count(), 3);
    assert_eq!(
        receipt.artifact_disposition(input),
        Some(ArtifactDisposition::Loaded)
    );
    assert_eq!(receipt.artifact_role(output), Some(ArtifactRole::Output));
    assert_eq!(
        receipt.artifact_node(output),
        Some(WorkNodeId::new("transaction-commit"))
    );
    assert_eq!(receipt.artifact_actual_bytes(output), Some(2_048));
    assert_eq!(
        receipt.artifact_disposition(output),
        Some(ArtifactDisposition::Published)
    );
    assert_eq!(
        receipt.artifact_disposition(cache),
        Some(ArtifactDisposition::Reused)
    );
    assert_eq!(
        receipt.artifact_cache_identity(cache),
        Some(CacheIdentity::from_sha256([33; 32]).as_bytes())
    );
    assert_eq!(
        receipt.artifact_observed_identity(output),
        Some(ArtifactIdentity::from_sha256([36; 32]).as_bytes())
    );
    assert_eq!(
        receipt.artifact_path_identity(input),
        Some(input_path.as_bytes())
    );
    assert_eq!(
        receipt.artifact_path_identity(output),
        Some(output_path.as_bytes())
    );
    let persisted = std::fs::read_to_string(
        std::fs::read_dir(receipts.root_path())
            .expect("receipt directory listing")
            .next()
            .expect("receipt file")
            .expect("receipt entry")
            .path(),
    )
    .expect("serialized receipt");
    assert!(!persisted.contains("secret-source.ms"));
    assert!(!persisted.contains("secret-image.table"));
}

#[test]
fn failed_publication_fence_never_records_a_published_output() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(evidenced_physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let input = ArtifactIdentity::from_sha256([31; 32]);
    let cache = ArtifactIdentity::from_sha256([32; 32]);
    let output = ArtifactIdentity::from_sha256([34; 32]);
    let staged_output = ArtifactIdentity::from_sha256([36; 32]);
    let mut executor = recording_executor(6, None, Some("publication fence failed"));
    executor.fail_only_fence = Some(FenceKind::Publication);
    executor.measurements = BTreeMap::from([
        (
            WorkNodeId::new("read"),
            (
                vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
                vec![
                    artifact_measurement(
                        input,
                        Some(input),
                        ArtifactDisposition::Loaded,
                        4_096,
                        None,
                    ),
                    artifact_measurement(cache, Some(cache), ArtifactDisposition::Reused, 0, None),
                ],
            ),
        ),
        (
            WorkNodeId::new("transaction-commit"),
            (
                vec![IoMeasurement::new(IoBufferKind::Publication, 2_048, 1)],
                vec![artifact_measurement(
                    output,
                    Some(staged_output),
                    ArtifactDisposition::Staged,
                    2_048,
                    None,
                )],
            ),
        ),
    ]);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([87; 32]),
        BuildIdentity::from_sha256([88; 32]),
    );
    let mut controller = RunToCompletion;

    let error = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect_err("publication fence failure must fail the run");
    assert!(matches!(
        error,
        RunError::Execution { ref node, .. } if node == &WorkNodeId::new("transaction-commit")
    ));

    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("failed receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        receipt.artifact_disposition(output),
        Some(ArtifactDisposition::Staged)
    );
    assert_eq!(
        receipt.artifact_observed_identity(output),
        Some(staged_output.as_bytes())
    );
    assert_eq!(receipt.artifact_actual_bytes(output), Some(2_048));
    assert_eq!(
        receipt.fence_status(&FenceId::new(
            WorkNodeId::new("transaction-commit"),
            FenceKind::Publication,
        )),
        Some(ReceiptStatus::Failed)
    );
}

#[test]
fn receipts_preserve_typed_terminal_outcomes_and_every_node_state() {
    let problem = compile(request(1)).expect("logical compilation");
    let balanced_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let failed_receipts = balanced_plan.receipt_store();
    let failed_provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([41; 32]),
        BuildIdentity::from_sha256([42; 32]),
    );
    let failed_registry = test_registry(&problem, 3, 6, Some("adapter failed"));
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &failed_registry,
            authority(),
            &mut completion,
            failed_receipts.bind(failed_provenance.clone()),
        ),
        Err(RunError::Execution { .. })
    ));
    let failed = failed_receipts
        .open(failed_provenance.attempt_id())
        .expect("failed receipt");
    assert_eq!(failed.status(), ReceiptStatus::Failed);
    assert_eq!(failed.failure_kind(), Some(ReceiptFailureKind::Adapter));
    assert_eq!(
        failed.failure_node(),
        Some(WorkNodeId::new("transaction-check"))
    );
    assert_eq!(
        failed.node_status(&WorkNodeId::new("transaction-check")),
        Some(ReceiptStatus::Failed)
    );
    assert_eq!(
        failed.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Cancelled)
    );

    let cancelled_receipts = balanced_plan.receipt_store();
    let cancelled_provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([43; 32]),
        BuildIdentity::from_sha256([44; 32]),
    );
    let successful_registry = test_registry(&problem, 3, 6, None);
    let mut cancellation = CancelAfterLaunch::default();
    assert_eq!(
        run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &successful_registry,
            authority(),
            &mut cancellation,
            cancelled_receipts.bind(cancelled_provenance.clone()),
        )
        .expect("cancelled execution"),
        ExecutionOutcome::Cancelled
    );
    let cancelled = cancelled_receipts
        .open(cancelled_provenance.attempt_id())
        .expect("cancelled receipt");
    assert_eq!(cancelled.status(), ReceiptStatus::Cancelled);
    assert_eq!(cancelled.failure_kind(), None);
    assert_eq!(
        cancelled.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Cancelled)
    );
    assert_eq!(
        cancelled.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Cancelled)
    );

    let mutation_receipts = balanced_plan.receipt_store();
    let mutation_provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([45; 32]),
        BuildIdentity::from_sha256([46; 32]),
    );
    let changed = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Interactive,
        cost_model(4),
    );
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &balanced_plan,
            &changed,
            &successful_registry,
            authority(),
            &mut completion,
            mutation_receipts.bind(mutation_provenance.clone()),
        ),
        Err(RunError::BindingMismatch { .. })
    ));
    let mutation = mutation_receipts
        .open(mutation_provenance.attempt_id())
        .expect("mutation receipt");
    assert_eq!(mutation.status(), ReceiptStatus::Mutation);
    assert_eq!(
        mutation.failure_kind(),
        Some(ReceiptFailureKind::BindingMutation)
    );
    assert_eq!(
        mutation.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::NotStarted)
    );

    let aborted_receipts = balanced_plan.receipt_store();
    let aborted_provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([49; 32]),
        BuildIdentity::from_sha256([50; 32]),
    );
    let mut interrupted_executor = recording_executor(6, None, None);
    interrupted_executor.panic_on_execute = true;
    let interrupted_registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), interrupted_executor)]),
    };
    let mut completion = RunToCompletion;
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &interrupted_registry,
            authority(),
            &mut completion,
            aborted_receipts.bind(aborted_provenance.clone()),
        );
    }));
    assert!(interrupted.is_err());
    let aborted = aborted_receipts
        .open(aborted_provenance.attempt_id())
        .expect("aborted receipt");
    assert_eq!(aborted.status(), ReceiptStatus::Aborted);
    assert_eq!(
        aborted.failure_kind(),
        Some(ReceiptFailureKind::Interrupted)
    );
    assert_eq!(
        aborted.failure_node(),
        Some(WorkNodeId::new("transaction-check"))
    );
    assert_eq!(
        aborted.node_status(&WorkNodeId::new("transaction-check")),
        Some(ReceiptStatus::Aborted)
    );
    assert_eq!(
        aborted.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::NotStarted)
    );
}

#[test]
fn stale_binding_uses_the_plan_receipt_store_for_mutation_evidence() {
    let problem = compile(request(1)).expect("logical compilation");
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Interactive,
        cost_model(4),
    );
    let canonical = plan.receipt_store();
    let alternate_directory = tempfile::tempdir().expect("alternate receipt directory");
    let alternate = ExecutionReceiptStore::new(
        alternate_directory.path(),
        ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("alternate receipt store");
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([47; 32]),
        BuildIdentity::from_sha256([48; 32]),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &plan,
            &current,
            &registry,
            authority(),
            &mut completion,
            alternate.bind(provenance.clone()),
        ),
        Err(RunError::BindingMismatch { .. })
    ));
    let mutation = canonical
        .open(provenance.attempt_id())
        .expect("mutation receipt is canonicalized to the plan store");
    assert_eq!(mutation.status(), ReceiptStatus::Mutation);
    assert!(matches!(
        alternate.open(provenance.attempt_id()),
        Err(casa_imaging_runtime::ReceiptError::Io { .. })
    ));
}

#[path = "compile_plan_run/prepared_artifact.rs"]
mod prepared_artifact;
