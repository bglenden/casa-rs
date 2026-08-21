// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    io,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, ImagingRequestVersion, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    MissingPointingPolicy, ModelColumnWrite, ModelInnerProduct, ModelStateIdentity, MsColumnKind,
    NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationSelection, ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemSpecification, ProductAxisKind, ProductBlankingPolicy,
    ProductElementRepresentation, ProductGeneration, ProductKind, ProductNormalization,
    ProductRequirements, ProductRole, ProductSchema, ProductSourceGenerationId,
    ProductSupportComparison, ProductTerm, ProductValidityPolicies, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, ReferenceDataKind, RestFrequency, RestoringBeamPolicy, RowSelection,
    ScientificContract, SelectedColumns, SelectedMainRow, SelectedObservationSample,
    SelectedPredictionTarget, SelectedRows, SelectedSampleAddress, SelectedSampleCoordinates,
    SelectedSampleMetadata, SelectedVisibilitySample, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSampling, SpectralWcs,
    SpectralWindowSelection, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    TimeScale, TimeSelection, UvSelection, UvwCoordinateLaw, VisibilityColumn,
    VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract, WeightingScheme,
    compile,
};
use casa_imaging_runtime::{
    AdaptationId, AdaptationTransition, AllocationAccess, AllocationId, AllocationLayout,
    AllocationLifetime, AllocationPurpose, AllocationUse, AlternativeId, ArtifactDisposition,
    ArtifactIdentity, ArtifactMeasurement, ArtifactRole, BindingKind, BuildIdentity, CacheDemand,
    CacheIdentity, CapabilityPredicate, CapacityDomainId, CapacityViewId, ClaimLifetime,
    CompiledProblemEvidence, CountDemand, CpuClassCapacity, DemandAlternative, DemandEnvelope,
    ExecutionDag, ExecutionDagSpecification, ExecutionError, ExecutionEvidenceError,
    ExecutionKnobs, ExecutionOutcome, ExecutionPlan, ExecutionPlanId, ExecutionProvenance,
    ExecutionReceiptStore, ExecutionStatus, ExternalPressure, FenceId, FenceKind, HostInventory,
    ImplementationRegistry, ImplementationRegistryId, InitializationPolicy, IoBufferDemand,
    IoBufferKind, IoMeasurement, IoPrediction, LeaseResource, LogicalAllocation,
    MemoryCapacityDomain, MemoryCapacityKind, MemoryDemand, MemoryView, MemoryViewKind,
    ObservationReadSourceReport, ObservationTransactionPlanError, ObservationTransactionWork,
    PhysicalLayoutId, PhysicalSlot, PhysicalSlotId, PhysicalWorkBinding, PhysicalWorkBindingError,
    PlanError, PlanPrediction, PlannedArtifact, PlannerCostModelProfileId, PlanningBindings,
    PredictionConfidence, PredictionUncertainty, ProductParticipantEvidence,
    ProductPublicationBindingError, PublicationBoundKind, PublicationLayoutError,
    PublicationLayoutLedger, PublicationMappedStaging, PublicationParticipant,
    PublicationPhysicalLayout, PublicationResourceBounds, PublicationStaging, QueueDemand,
    QueueResource, QueueResourceId, QuiescencePoint, RateDemand, RateResource, RateResourceId,
    RateUnit, ReceiptFailureKind, ReceiptRetention, ReceiptStatus, RedactedPath, ResourceAuthority,
    ResourceClaim, ResourceError, ResourceHeadroom, ResourceMeasurement, ResourceOverride,
    ResourcePolicy, ResourceTopology, RunBindings, RunController, RunDirective, RunError,
    RunToCompletion, RuntimeOverheadDemand, ScalingMetadata, ScheduledWork, SlotCompatibility,
    StagePrediction, StorageDemand, StorageDomain, StorageDomainId, StorageMode, StorageUseKind,
    WorkCompletion, WorkDependency, WorkDomain, WorkExecutionContext, WorkImplementation,
    WorkImplementationId, WorkKind, WorkMeasurements, WorkNode, WorkNodeId, plan,
    run as run_receipted,
};
use sha2::{Digest, Sha256};

fn validity_policies() -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB policy fixture"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy fixture"),
    )
}

mod common;

use common::{identity, problem_inputs};

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
    request_with_geometry_and_references(observation, geometry(255.0), Vec::new())
}

fn mtmfs_request(observation: u8) -> ImagingRequest {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F32],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    ImagingRequest::new(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
                MeasurementEquationContract::new(
                    InstrumentResponse::PrimaryBeam,
                    DeclaredInnerProducts::new(
                        ModelInnerProduct::HermitianEuclidean,
                        VisibilityInnerProduct::HermitianEuclidean,
                    ),
                ),
            ),
            ReconstructionContract::new(
                ReconstructionBasis::Taylor { terms: 2 },
                ReconstructionAlgorithm::Mtmfs,
                ReconstructionControls::new(100, 0.1, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(
                WeightingScheme::Briggs { robust: 0.5 },
                WeightDensityScope::GlobalSelection,
            ),
            ProductRequirements::new(
                vec![
                    ProductKind::Psf,
                    ProductKind::Residual,
                    ProductKind::Model,
                    ProductKind::RestoredImage,
                    ProductKind::SumWeights,
                    ProductKind::Mask,
                    ProductKind::Weight,
                    ProductKind::PrimaryBeam,
                    ProductKind::Sensitivity,
                    ProductKind::PbCorrectedImage,
                    ProductKind::TaylorTerms,
                    ProductKind::SpectralIndex,
                    ProductKind::SpectralIndexError,
                    ProductKind::PbCorrectedSpectralIndex,
                    ProductKind::Beam,
                ],
                ProductNormalization::FlatNoise,
                RestoringBeamPolicy::PerPlane,
                validity_policies(),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            numerics,
        ),
        geometry(255.0),
        problem_inputs(
            observation,
            vec![
                (ReferenceDataKind::Measures, identity(2)),
                (ReferenceDataKind::Instrument, identity(3)),
            ],
            ModelStateIdentity::Seed(identity(4)),
        ),
    )
}

fn request_with_products(observation: u8, products: Vec<ProductKind>) -> ImagingRequest {
    request_with_geometry_references_weighting_and_products(
        observation,
        geometry(255.0),
        Vec::new(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
    )
}

fn request_with_geometry(observation: u8, geometry: GeometryInput) -> ImagingRequest {
    request_with_geometry_and_references(observation, geometry, Vec::new())
}

fn request_with_geometry_and_references(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
) -> ImagingRequest {
    request_with_geometry_references_weighting_and_products(
        observation,
        geometry,
        references,
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        vec![ProductKind::Psf],
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

fn request_with_geometry_references_weighting_and_products(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
    products: Vec<ProductKind>,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        references,
        weighting,
        products,
        ModelColumnWrite::Disabled,
    )
}

fn request_with_geometry_and_products(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        Vec::new(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        ModelColumnWrite::Disabled,
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
        Vec::new(),
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
            validity_policies(),
        ),
        ObservationTransactionRequirements::new(model_column_write),
        numerics,
    );
    ImagingRequest::new(
        specification,
        geometry,
        problem_inputs(observation, references, ModelStateIdentity::Empty),
    )
}

fn registry(byte: u8) -> ImplementationRegistryId {
    ImplementationRegistryId::from_sha256([byte; 32])
}

fn cost_model(byte: u8) -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256([byte; 32])
}

fn product_generation(problem: &casa_imaging_model::CompiledProblem) -> ProductGeneration {
    product_generation_with_source(problem, 41)
}

fn product_generation_with_source(
    problem: &casa_imaging_model::CompiledProblem,
    source_byte: u8,
) -> ProductGeneration {
    let graph = problem.product_graph();
    let sources = graph
        .sources()
        .iter()
        .map(|source| {
            graph
                .bind_source_generation(
                    source.source_id(),
                    ProductSourceGenerationId::from_sha256([source_byte; 32]),
                )
                .expect("bind product source")
        })
        .collect();
    graph
        .bind_generation(sources)
        .expect("bind product generation")
}

fn planning_bindings(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_registry: ImplementationRegistryId,
    resource_policy: ResourcePolicy,
    planner_cost_model_profile: PlannerCostModelProfileId,
) -> PlanningBindings {
    PlanningBindings::new(
        product_generation(problem),
        implementation_registry,
        resource_policy,
        planner_cost_model_profile,
    )
}

fn publication_artifact(
    problem: &casa_imaging_model::CompiledProblem,
    generation: &ProductGeneration,
) -> ArtifactIdentity {
    let member = problem.product_graph().publication().members()[0];
    ArtifactIdentity::from_sha256(
        generation
            .artifact_id(member)
            .expect("publication artifact")
            .as_bytes(),
    )
}

fn publication_artifacts(
    problem: &casa_imaging_model::CompiledProblem,
    generation: &ProductGeneration,
) -> Vec<ArtifactIdentity> {
    problem
        .product_graph()
        .publication()
        .members()
        .iter()
        .map(|member| {
            ArtifactIdentity::from_sha256(
                generation
                    .artifact_id(*member)
                    .expect("publication artifact")
                    .as_bytes(),
            )
        })
        .collect()
}

fn product_publication_layouts(
    problem: &casa_imaging_model::CompiledProblem,
    generation: &ProductGeneration,
    staging: PublicationStaging,
    staged_bytes: u64,
    final_bytes: u64,
    writer_bytes: u64,
    mapped_page_cache_bytes: u64,
) -> PublicationLayoutLedger {
    PublicationLayoutLedger::new(
        problem
            .product_graph()
            .publication()
            .members()
            .iter()
            .enumerate()
            .map(|(ordinal, product)| {
                PublicationPhysicalLayout::new(
                    PublicationParticipant::Product(*product),
                    ArtifactIdentity::from_sha256(
                        generation
                            .artifact_id(*product)
                            .expect("generation artifact")
                            .as_bytes(),
                    ),
                    PhysicalLayoutId::from_sha256(
                        [u8::try_from(ordinal + 1).expect("small product fixture"); 32],
                    ),
                    staging.clone(),
                    PublicationResourceBounds::new(
                        staged_bytes,
                        final_bytes,
                        writer_bytes,
                        mapped_page_cache_bytes,
                    )
                    .expect("product physical bounds"),
                )
            })
            .collect(),
    )
    .expect("canonical product physical layouts")
}

fn tiled_publication_staging(
    writer: &str,
    writer_allocation: &str,
    mapped: &str,
    mapped_allocation: &str,
    mapped_release: &str,
) -> PublicationStaging {
    let writer = WorkNodeId::new(writer);
    let mapped = WorkNodeId::new(mapped);
    let mapped_release = WorkNodeId::new(mapped_release);
    PublicationStaging::new(
        writer.clone(),
        WorkDependency::Fence(FenceId::new(writer, FenceKind::Io)),
        IoBufferKind::TiledColumnWriter,
        AllocationId::new(writer_allocation),
        Some(
            PublicationMappedStaging::new(
                mapped,
                WorkDependency::Work(mapped_release),
                AllocationId::new(mapped_allocation),
            )
            .expect("valid mapped publication staging"),
        ),
    )
    .expect("valid tiled publication staging")
}

fn model_data_layout(
    artifact: ArtifactIdentity,
    measurement_set: MeasurementSetIdentity,
) -> PublicationLayoutLedger {
    PublicationLayoutLedger::new(vec![PublicationPhysicalLayout::new(
        PublicationParticipant::ModelData(measurement_set),
        artifact,
        PhysicalLayoutId::from_sha256([34; 32]),
        tiled_publication_staging(
            "model-data-writer",
            "model-data-writer-buffer",
            "model-data-mapped-pages",
            "model-data-mapped-pages",
            "model-data-mapped-release",
        ),
        PublicationResourceBounds::new(2_048, 2_048, 16, 32).expect("MODEL_DATA physical bounds"),
    )])
    .expect("MODEL_DATA physical layout")
}

fn staged_model_data_physical_work(
    artifact: ArtifactIdentity,
    measurement_set: MeasurementSetIdentity,
) -> PhysicalWorkBinding {
    let base = evidenced_physical_work_with_output(6, artifact);
    PhysicalWorkBinding::new(
        base.execution_dag().clone(),
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        model_data_layout(artifact, measurement_set),
    )
    .expect("bound staged MODEL_DATA work")
}

fn implementation(byte: u8) -> WorkImplementationId {
    WorkImplementationId::new(format!("test-cpu-{byte}"))
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
        events: Mutex::new(Vec::new()),
        measurements: BTreeMap::new(),
        panic_on_execute: false,
        publication_launched: None,
        visible_generation: None,
        failure_node: None,
        fence_failure_event: None,
        publication_failure: None,
        generic_source_access: None,
        visibility_during_fence_settlement: None,
        publication_buffer_held: None,
        omit_observation_read_completion: false,
        mismatch_observation_read_coverage: false,
        mismatch_observation_read_metadata: false,
        observation_read_report_mutation: None,
        observation_read_reports: actual_observation_read_reports(1),
        observation_read_samples: actual_observation_read_samples(1),
        observation_read_stream_failure: None,
        mismatch_observation_read_samples: false,
        observation_read_completion_cache: None,
    }
}

fn evidenced_measurements(
    execution_plan: &ExecutionPlan,
) -> BTreeMap<WorkNodeId, (Vec<IoMeasurement>, Vec<ArtifactMeasurement>)> {
    let input = ArtifactIdentity::from_sha256([31; 32]);
    let cache = ArtifactIdentity::from_sha256([32; 32]);
    let publication = execution_plan
        .product_publication()
        .publication_node()
        .clone();
    let outputs = execution_plan
        .product_publication()
        .artifacts()
        .iter()
        .map(|planned| {
            let artifact = planned.artifact();
            ArtifactMeasurement::new(
                artifact,
                Some(artifact),
                ArtifactDisposition::Published,
                2_048,
                None,
            )
        })
        .collect();

    BTreeMap::from([
        (
            WorkNodeId::new("read"),
            (
                vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
                vec![
                    ArtifactMeasurement::new(
                        input,
                        Some(input),
                        ArtifactDisposition::Loaded,
                        4_096,
                        None,
                    ),
                    ArtifactMeasurement::new(
                        cache,
                        None,
                        ArtifactDisposition::RejectedStale,
                        0,
                        None,
                    ),
                ],
            ),
        ),
        (
            WorkNodeId::new("model-data-writer"),
            (
                vec![IoMeasurement::new(IoBufferKind::TiledColumnWriter, 16, 1)],
                Vec::new(),
            ),
        ),
        (
            WorkNodeId::new("model-data-mapped-pages"),
            (
                vec![IoMeasurement::new(IoBufferKind::MappedPageCache, 32, 1)],
                Vec::new(),
            ),
        ),
        (
            WorkNodeId::new("model-data-mapped-release"),
            (
                vec![IoMeasurement::new(IoBufferKind::MappedPageCache, 32, 1)],
                Vec::new(),
            ),
        ),
        (
            publication,
            (
                vec![IoMeasurement::new(IoBufferKind::Publication, 2_048, 1)],
                outputs,
            ),
        ),
    ])
}

fn observed_identity(observation: u8, scope: u8) -> LogicalIdentity {
    let mut digest = [observation; 32];
    digest[0] = scope;
    LogicalIdentity::from_sha256(digest)
}

fn actual_observation_read_reports(observation: u8) -> Vec<ObservationReadSourceReport> {
    let columns = [
        MsColumnKind::Data,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::Weight,
        MsColumnKind::Uvw,
        MsColumnKind::Time,
        MsColumnKind::TimeCentroid,
        MsColumnKind::Interval,
        MsColumnKind::Exposure,
        MsColumnKind::FieldId,
        MsColumnKind::DataDescriptionId,
        MsColumnKind::Antenna1,
        MsColumnKind::Antenna2,
        MsColumnKind::Feed1,
        MsColumnKind::Feed2,
        MsColumnKind::ScanNumber,
        MsColumnKind::StateId,
        MsColumnKind::ObservationId,
        MsColumnKind::ArrayId,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| {
        ColumnGeneration::new(kind, observed_identity(observation, 20 + index as u8))
    })
    .collect();
    let metadata = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| {
        MetadataGeneration::new(kind, observed_identity(observation, 60 + index as u8))
    })
    .collect();
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(1, [SelectedMainRow::new(0, 0)])
            .expect("single selected MAIN row fixture"),
        RowSelection::new(
            IdSelection::All,
            TimeSelection::All,
            UvSelection::All,
            AntennaSelection::All,
            IdSelection::All,
            IdSelection::All,
            IntentSelection::All,
            IdSelection::All,
        ),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, vec![0])],
        vec![CorrelationSelection::new(
            0,
            vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
        )],
    );
    vec![ObservationReadSourceReport::new(
        MeasurementSetIdentity::new(observed_identity(observation, 1)),
        selection,
        SelectedColumns::new(
            VisibilityColumn::Data,
            FlagPolicy::FlagOrFlagRow,
            WeightColumn::Weight,
            columns,
        ),
        metadata,
        ConsistencyToken::new(observed_identity(observation, 4)),
    )]
}

fn actual_observation_read_samples(observation: u8) -> Vec<SelectedObservationSample> {
    vec![SelectedObservationSample {
        address: SelectedSampleAddress {
            measurement_set: MeasurementSetIdentity::new(observed_identity(observation, 1)),
            physical_row: 0,
            data_description_id: 0,
            spectral_window_id: 0,
            channel_index: 0,
            frequency_centre_hz: 1.4e9,
            frequency_lower_hz: 1.399_5e9,
            frequency_upper_hz: 1.400_5e9,
            channel_width_hz: 1.0e6,
            frequency_frame: FrequencyFrame::Topocentric,
            polarization_id: 0,
            correlation_index: 0,
            correlation_type: CorrelationType::StokesI,
        },
        visibility: SelectedVisibilitySample::Complex32([1.0, -0.5]),
        prediction_target: SelectedPredictionTarget::NotRequested,
        channel_flag: false,
        row_flag: false,
        input_weight: 2.0,
        coordinates: SelectedSampleCoordinates {
            raw_uvw_m: [12.0, -4.0, 2.0],
            density_uvw_m: [12.5, -4.25, 2.25],
            transformed_uvw_m: [11.75, -3.75, 1.5],
            phase_shift_m: 0.125,
            uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
            time: Epoch::new(59_000.0, TimeScale::Utc),
            time_centroid: Epoch::new(59_000.000_001, TimeScale::Utc),
            interval_seconds: 1.0,
            exposure_seconds: 0.8,
            phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.000_5, -0.500_5),
            pointing_directions: casa_imaging_model::SelectedPointingDirections {
                antenna1: SkyDirection::new(DirectionFrame::J2000, 1.001, -0.499),
                antenna2: SkyDirection::new(DirectionFrame::J2000, 1.002, -0.498),
            },
        },
        metadata: SelectedSampleMetadata {
            field_id: 0,
            antenna1: 0,
            antenna2: 1,
            feed1: 0,
            feed2: 0,
            scan_number: 7,
            state_id: 0,
            observation_id: 0,
            array_id: 0,
        },
    }]
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AdapterEvent {
    Execute(WorkNodeId),
    Fence(FenceId),
    Complete(WorkNodeId),
}

#[derive(Clone, Copy, Debug)]
enum ObservationReadReportMutation {
    Selection,
    SelectedColumns,
    ConsistencyToken,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObservationReadStreamFailure {
    Midstream,
    Terminal,
}

#[derive(Debug)]
struct RecordingExecutor {
    id: WorkImplementationId,
    failure: Option<&'static str>,
    fence_failure: Option<&'static str>,
    fail_only_fence: Option<FenceKind>,
    calls: AtomicUsize,
    fence_waits: AtomicUsize,
    observed_knobs: Mutex<Vec<ExecutionKnobs>>,
    events: Mutex<Vec<AdapterEvent>>,
    measurements: BTreeMap<WorkNodeId, (Vec<IoMeasurement>, Vec<ArtifactMeasurement>)>,
    panic_on_execute: bool,
    publication_launched: Option<Arc<AtomicBool>>,
    visible_generation: Option<Arc<AtomicUsize>>,
    failure_node: Option<&'static str>,
    fence_failure_event: Option<(&'static str, FenceKind)>,
    publication_failure: Option<&'static str>,
    generic_source_access: Option<Arc<AtomicBool>>,
    visibility_during_fence_settlement: Option<Arc<AtomicBool>>,
    publication_buffer_held: Option<Arc<AtomicBool>>,
    omit_observation_read_completion: bool,
    mismatch_observation_read_coverage: bool,
    mismatch_observation_read_metadata: bool,
    observation_read_report_mutation: Option<ObservationReadReportMutation>,
    observation_read_reports: Vec<ObservationReadSourceReport>,
    observation_read_samples: Vec<SelectedObservationSample>,
    observation_read_stream_failure: Option<ObservationReadStreamFailure>,
    mismatch_observation_read_samples: bool,
    observation_read_completion_cache: Option<Arc<Mutex<Option<WorkCompletion>>>>,
}

impl WorkImplementation for RecordingExecutor {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(
        &self,
        context: WorkExecutionContext<'_>,
        work: &ScheduledWork,
    ) -> Result<WorkMeasurements, Self::Error> {
        assert!(!self.panic_on_execute, "interrupted adapter");
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .expect("recording executor event lock")
            .push(AdapterEvent::Execute(work.node().id.clone()));
        self.observed_knobs
            .lock()
            .expect("recording executor knobs lock")
            .push(work.knobs().clone());
        if let Some(message) = self.failure {
            return Err(io::Error::other(message));
        }
        if self.failure_node == Some(work.node().id.as_str()) {
            return Err(io::Error::other("stateful transaction execute failure"));
        }
        if work.node().kind == WorkKind::Io
            && context.observation_reads().is_some()
            && let Some(accessed) = &self.generic_source_access
        {
            accessed.store(true, Ordering::SeqCst);
        }
        if work.node().kind == WorkKind::Publication
            && let Some(launched) = &self.publication_launched
        {
            launched.store(true, Ordering::SeqCst);
        }
        let resources = work
            .node()
            .claims
            .iter()
            .map(|claim| {
                ResourceMeasurement::new(
                    claim.resource.clone(),
                    claim.lifetime.clone(),
                    claim.amount,
                )
            })
            .collect();
        let (io, artifacts) = self
            .measurements
            .get(&work.node().id)
            .cloned()
            .unwrap_or_default();
        Ok(WorkMeasurements::new(resources, io, artifacts))
    }

    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        work: &ScheduledWork,
        fence: FenceKind,
    ) -> Result<(), Self::Error> {
        self.fence_waits.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .expect("recording executor event lock")
            .push(AdapterEvent::Fence(FenceId::new(
                work.node().id.clone(),
                fence,
            )));
        if let Some(message) = self.fence_failure
            && self.fail_only_fence.is_none_or(|kind| kind == fence)
        {
            return Err(io::Error::other(message));
        }
        if self.fence_failure_event == Some((work.node().id.as_str(), fence)) {
            return Err(io::Error::other("stateful transaction fence failure"));
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

    fn complete(
        &self,
        mut context: WorkExecutionContext<'_>,
        work: &ScheduledWork,
    ) -> Result<WorkCompletion, Self::Error> {
        self.events
            .lock()
            .expect("recording executor event lock")
            .push(AdapterEvent::Complete(work.node().id.clone()));
        if work.node().kind != WorkKind::ObservationRead || self.omit_observation_read_completion {
            return Ok(WorkCompletion::new());
        }
        if let Some(cache) = &self.observation_read_completion_cache
            && let Some(completion) = cache
                .lock()
                .expect("observation-read completion cache lock")
                .clone()
        {
            return Ok(completion);
        }
        assert!(
            context.observation_reads().is_some(),
            "ObservationRead receives its exact canonical read set"
        );
        let mut observed = self.observation_read_reports.clone();
        if self.mismatch_observation_read_coverage {
            let first = observed
                .first_mut()
                .expect("compiled ObservationRead has at least one canonical source");
            *first = ObservationReadSourceReport::new(
                MeasurementSetIdentity::new(identity(250)),
                first.selection().clone(),
                first.selected_columns().clone(),
                first.metadata().to_vec(),
                first.consistency_token(),
            );
        }
        if self.mismatch_observation_read_metadata {
            let first = observed
                .first_mut()
                .expect("compiled ObservationRead has at least one canonical source");
            let mut metadata = first.metadata().to_vec();
            metadata[0] = MetadataGeneration::new(
                metadata[0].kind(),
                LogicalIdentity::from_sha256([251; 32]),
            );
            *first = ObservationReadSourceReport::new(
                first.measurement_set(),
                first.selection().clone(),
                first.selected_columns().clone(),
                metadata,
                first.consistency_token(),
            );
        }
        if let Some(mutation) = self.observation_read_report_mutation {
            let first = observed
                .first_mut()
                .expect("compiled ObservationRead has at least one canonical source");
            let measurement_set = first.measurement_set();
            let mut selection = first.selection().clone();
            let mut selected_columns = first.selected_columns().clone();
            let metadata = first.metadata().to_vec();
            let mut consistency_token = first.consistency_token();
            match mutation {
                ObservationReadReportMutation::Selection => {
                    selection = ObservationSelection::new(
                        SelectedRows::from_ordered_main_rows(2, [SelectedMainRow::new(1, 0)])
                            .expect("changed selected MAIN row report"),
                        selection.rows_filter().clone(),
                        selection.data_descriptions().to_vec(),
                        selection.spectral_windows().to_vec(),
                        selection.correlations().to_vec(),
                    );
                }
                ObservationReadReportMutation::SelectedColumns => {
                    let mut generations = selected_columns.generations().to_vec();
                    generations.push(ColumnGeneration::new(
                        MsColumnKind::WeightSpectrum,
                        identity(253),
                    ));
                    selected_columns = SelectedColumns::new(
                        selected_columns.visibility(),
                        selected_columns.flags(),
                        WeightColumn::WeightSpectrum,
                        generations,
                    );
                }
                ObservationReadReportMutation::ConsistencyToken => {
                    consistency_token = ConsistencyToken::new(identity(254));
                }
            }
            *first = ObservationReadSourceReport::new(
                measurement_set,
                selection,
                selected_columns,
                metadata,
                consistency_token,
            );
        }
        let mut samples = self.observation_read_samples.clone();
        if self.mismatch_observation_read_samples {
            samples[0].address.physical_row = 1;
        }
        let mut sample_index = 0_usize;
        let stream_failure = self.observation_read_stream_failure;
        let sample_stream = std::iter::from_fn(move || {
            if stream_failure == Some(ObservationReadStreamFailure::Midstream) && sample_index == 1
            {
                sample_index += 1;
                return Some(Err(io::Error::other(
                    "synthetic midstream selected-observation read failure",
                )));
            }
            if let Some(sample) = samples.get(sample_index).copied() {
                sample_index += 1;
                return Some(Ok(sample));
            }
            if stream_failure == Some(ObservationReadStreamFailure::Terminal)
                && sample_index == samples.len()
            {
                sample_index += 1;
                return Some(Err(io::Error::other(
                    "synthetic terminal selected-observation read failure",
                )));
            }
            None
        });
        let completion = context
            .take_observation_read_completion()
            .expect("ObservationRead receives terminal evidence authority")
            .complete(observed, sample_stream)
            .map_err(io::Error::other)?;
        if let Some(cache) = &self.observation_read_completion_cache {
            *cache
                .lock()
                .expect("observation-read completion cache lock") = Some(completion.clone());
        }
        Ok(completion)
    }

    fn publish(
        &self,
        context: WorkExecutionContext<'_>,
        work: &ScheduledWork,
    ) -> Result<(), Self::Error> {
        if work.node().kind != WorkKind::Publication || context.publication().is_none() {
            return Err(io::Error::other(
                "publication requires the transaction-bound Publication node",
            ));
        }
        if let Some(message) = self.publication_failure {
            return Err(io::Error::other(message));
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
        Ok(())
    }
}

fn publication_recording_executor(
    byte: u8,
    execution_plan: &ExecutionPlan,
    launched: Arc<AtomicBool>,
    visible_generation: Arc<AtomicUsize>,
) -> RecordingExecutor {
    RecordingExecutor {
        id: implementation(byte),
        failure: None,
        fence_failure: None,
        fail_only_fence: None,
        calls: AtomicUsize::new(0),
        fence_waits: AtomicUsize::new(0),
        observed_knobs: Mutex::new(Vec::new()),
        events: Mutex::new(Vec::new()),
        measurements: evidenced_measurements(execution_plan),
        panic_on_execute: false,
        publication_launched: Some(launched),
        visible_generation: Some(visible_generation),
        failure_node: None,
        fence_failure_event: None,
        publication_failure: None,
        generic_source_access: None,
        visibility_during_fence_settlement: None,
        publication_buffer_held: None,
        omit_observation_read_completion: false,
        mismatch_observation_read_coverage: false,
        mismatch_observation_read_metadata: false,
        observation_read_report_mutation: None,
        observation_read_reports: actual_observation_read_reports(1),
        observation_read_samples: actual_observation_read_samples(1),
        observation_read_stream_failure: None,
        mismatch_observation_read_samples: false,
        observation_read_completion_cache: None,
    }
}

fn failing_transaction_executor(
    byte: u8,
    visible_generation: Arc<AtomicUsize>,
    failure_node: Option<&'static str>,
    fence_failure_event: Option<(&'static str, FenceKind)>,
    publication_failure: Option<&'static str>,
) -> RecordingExecutor {
    RecordingExecutor {
        id: implementation(byte),
        failure: None,
        fence_failure: None,
        fail_only_fence: None,
        calls: AtomicUsize::new(0),
        fence_waits: AtomicUsize::new(0),
        observed_knobs: Mutex::new(Vec::new()),
        events: Mutex::new(Vec::new()),
        measurements: BTreeMap::new(),
        panic_on_execute: false,
        publication_launched: None,
        visible_generation: Some(visible_generation),
        failure_node,
        fence_failure_event,
        publication_failure,
        generic_source_access: None,
        visibility_during_fence_settlement: None,
        publication_buffer_held: None,
        omit_observation_read_completion: false,
        mismatch_observation_read_coverage: false,
        mismatch_observation_read_metadata: false,
        observation_read_report_mutation: None,
        observation_read_reports: actual_observation_read_reports(1),
        observation_read_samples: actual_observation_read_samples(1),
        observation_read_stream_failure: None,
        mismatch_observation_read_samples: false,
        observation_read_completion_cache: None,
    }
}

fn physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    physical_work_with_transaction_staging(implementation_byte, None, false, false)
}

fn physical_work_with_product_staging(
    implementation_byte: u8,
    required_product_staging: Option<BTreeMap<ProductKind, BTreeSet<WorkDependency>>>,
) -> PhysicalWorkBinding {
    physical_work_with_transaction_staging(
        implementation_byte,
        required_product_staging,
        false,
        false,
    )
}

fn physical_work_with_model_staging(implementation_byte: u8) -> PhysicalWorkBinding {
    physical_work_with_transaction_staging(implementation_byte, None, true, false)
}

fn physical_work_with_transaction_staging(
    implementation_byte: u8,
    required_product_staging: Option<BTreeMap<ProductKind, BTreeSet<WorkDependency>>>,
    include_model_staging: bool,
    acquire_publication_early: bool,
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
        specification,
        implementation(implementation_byte),
        required_product_staging,
        include_model_staging,
        acquire_publication_early,
    )
}

fn transaction_binding(
    mut specification: ExecutionDagSpecification,
    work_implementation: WorkImplementationId,
    required_product_staging: Option<BTreeMap<ProductKind, BTreeSet<WorkDependency>>>,
    include_model_staging: bool,
    acquire_publication_early: bool,
) -> PhysicalWorkBinding {
    let initial = WorkNodeId::new("transaction-check");
    let read = WorkNodeId::new("transaction-read");
    let reconciliation = WorkNodeId::new("transaction-reconciliation");
    let product = WorkNodeId::new("transaction-stage-psf");
    let model = WorkNodeId::new("transaction-stage-model");
    let commit = WorkNodeId::new("transaction-commit");
    let publication_allocation = AllocationId::new("transaction-publication-buffer");
    let publication_slot = PhysicalSlotId::new("transaction-publication-slot");
    let commit_publication_allocation = acquire_publication_early
        .then(|| AllocationId::new("transaction-commit-publication-buffer"));
    let commit_publication_slot = acquire_publication_early
        .then(|| PhysicalSlotId::new("transaction-commit-publication-slot"));
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
    let read_completion = WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io));
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
    if acquire_publication_early {
        specification
            .resource_alternative
            .demand
            .memory
            .push(MemoryDemand {
                allocation_id: "transaction-commit-publication-slot".to_string(),
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
            staged_output_bytes: if include_model_staging { 2 } else { 1 },
            final_output_bytes: 0,
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
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
    ];
    let product_allocations = if acquire_publication_early {
        vec![AllocationUse {
            allocation: publication_allocation.clone(),
            lifetime: ClaimLifetime::Work,
        }]
    } else {
        Vec::new()
    };

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
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: LeaseResource::MeasurementSetLock {
                        measurement_set: casa_imaging_model::MeasurementSetIdentity::new(identity(
                            1,
                        )),
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
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: commit_publication_allocation
                    .as_ref()
                    .unwrap_or(&publication_allocation)
                    .clone(),
                lifetime: publication_lifetime.clone(),
            }],
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
        id: publication_allocation.clone(),
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
        compatibility: publication_compatibility.clone(),
    });
    if let (Some(allocation), Some(slot)) = (commit_publication_allocation, commit_publication_slot)
    {
        specification.logical_allocations.push(LogicalAllocation {
            id: allocation,
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
            compatibility: publication_compatibility.clone(),
            physical_slot: slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: commit.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
                ]),
            },
        });
        specification.physical_slots.push(PhysicalSlot {
            id: slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "transaction-commit-publication-slot".to_string(),
            },
            capacity_bytes: 1,
            compatibility: publication_compatibility.clone(),
        });
    }
    if include_model_staging {
        specification.logical_allocations.push(LogicalAllocation {
            id: writeback_allocation,
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
    let observation_transaction = ObservationTransactionWork::new(
        initial,
        reconciliation,
        required_product_staging.unwrap_or_else(|| {
            BTreeMap::from([(
                ProductKind::Psf,
                BTreeSet::from([WorkDependency::Work(product)]),
            )])
        }),
        include_model_staging.then_some(model),
        commit,
    );
    bind_physical_work(
        dag,
        observation_transaction,
        Vec::new(),
        PublicationLayoutLedger::empty(),
    )
}

fn bind_physical_work(
    dag: ExecutionDag,
    observation_transaction: ObservationTransactionWork,
    artifacts: Vec<PlannedArtifact>,
    publication_layouts: PublicationLayoutLedger,
) -> PhysicalWorkBinding {
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let mut io = BTreeMap::<IoBufferKind, u64>::new();
            for claim in &node.claims {
                if let LeaseResource::IoBuffer(kind) = claim.resource {
                    *io.entry(kind).or_default() += claim.amount;
                }
            }
            let stage = StagePrediction::new(node.id.clone(), 100);
            if io.is_empty() {
                stage
            } else {
                stage.with_io(
                    io.into_iter()
                        .map(|(kind, bytes)| IoPrediction::new(kind, bytes, 1))
                        .collect(),
                )
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete prediction");
    PhysicalWorkBinding::new(
        dag,
        prediction,
        artifacts,
        observation_transaction,
        publication_layouts,
    )
    .expect("bound physical work")
}

fn exact_product_physical_work(
    problem: &casa_imaging_model::CompiledProblem,
    generation: &ProductGeneration,
    physical_work: PhysicalWorkBinding,
) -> PhysicalWorkBinding {
    let product_artifacts = publication_artifacts(problem, generation);
    let product_count = u64::try_from(product_artifacts.len()).expect("product count fits u64");
    let staged_bytes = product_count
        .checked_mul(128)
        .expect("test staged bound fits u64");
    let final_bytes = product_count
        .checked_mul(128)
        .expect("test final bound fits u64");
    let writer_bytes = product_count
        .checked_mul(2)
        .expect("test writer bound fits u64");
    let mapped_bytes = product_count
        .checked_mul(2)
        .expect("test mapped bound fits u64");
    let publication_nodes = physical_work
        .execution_dag()
        .nodes()
        .values()
        .filter(|node| node.kind == WorkKind::Publication)
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    if let [publication] = publication_nodes.as_slice() {
        let publication_layouts = product_publication_layouts(
            problem,
            generation,
            tiled_publication_staging(
                "model-data-writer",
                "model-data-writer-buffer",
                "model-data-mapped-pages",
                "model-data-mapped-pages",
                "model-data-mapped-release",
            ),
            128,
            128,
            2,
            2,
        );
        let mut artifacts = physical_work
            .artifacts()
            .iter()
            .filter(|planned| {
                planned.node() != publication || planned.role() != ArtifactRole::Output
            })
            .cloned()
            .collect::<Vec<_>>();
        artifacts.extend(product_artifacts.iter().map(|artifact| {
            PlannedArtifact::new(*artifact, publication.clone(), ArtifactRole::Output, None)
        }));
        return PhysicalWorkBinding::new(
            physical_work.execution_dag().clone(),
            physical_work.prediction().clone(),
            artifacts,
            physical_work.observation_transaction().clone(),
            publication_layouts,
        )
        .expect("replace legacy publication output with exact product generation");
    }
    assert!(
        publication_nodes.is_empty(),
        "test fixtures must have at most one Publication node"
    );
    let publication_layouts = product_publication_layouts(
        problem,
        generation,
        tiled_publication_staging(
            "model-data-writer",
            "product-writer-buffer",
            "model-data-mapped-pages",
            "product-mapped-pages",
            "model-data-mapped-release",
        ),
        128,
        128,
        2,
        2,
    );

    let dag = physical_work.execution_dag();
    let publication = WorkNodeId::new("product-publication");
    let writer = WorkNodeId::new("model-data-writer");
    let mapped = WorkNodeId::new("model-data-mapped-pages");
    let mapped_release = WorkNodeId::new("model-data-mapped-release");
    let allocation = AllocationId::new("product-publication-buffer");
    let writer_allocation = AllocationId::new("product-writer-buffer");
    let mapped_allocation = AllocationId::new("product-mapped-pages");
    let slot = PhysicalSlotId::new("product-publication-slot");
    let writer_slot = PhysicalSlotId::new("product-writer-slot");
    let mapped_slot = PhysicalSlotId::new("product-mapped-slot");
    let implementation = dag
        .nodes()
        .values()
        .next()
        .expect("test plan has work")
        .implementation
        .clone();
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([dag.resource_alternative().demand.host_memory_view.clone()]),
        alignment_bytes: 1,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("product-publication-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut alternative = dag.resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "product-publication-buffer".to_string(),
        hard_bytes: writer_bytes,
        preferred_bytes: writer_bytes,
        views: vec![alternative.demand.host_memory_view.clone()],
    });
    alternative.demand.memory.extend([
        MemoryDemand {
            allocation_id: "product-writer-buffer".to_string(),
            hard_bytes: writer_bytes,
            preferred_bytes: writer_bytes,
            views: vec![alternative.demand.host_memory_view.clone()],
        },
        MemoryDemand {
            allocation_id: "product-mapped-pages".to_string(),
            hard_bytes: mapped_bytes,
            preferred_bytes: mapped_bytes,
            views: vec![alternative.demand.host_memory_view.clone()],
        },
    ]);
    alternative.demand.storage.push(StorageDemand {
        demand_id: "output-storage".to_string(),
        domain: StorageDomainId::new("test-output"),
        temporary_bytes: 0,
        staged_output_bytes: staged_bytes,
        final_output_bytes: final_bytes,
        persistent_cache_bytes: 0,
        read_rate: CountDemand::zero(),
        write_rate: CountDemand::new(1, 1),
        operations_rate: CountDemand::zero(),
        queue_slots: CountDemand::new(1, 1),
    });
    alternative.demand.io_buffers.publication_bytes = alternative
        .demand
        .io_buffers
        .publication_bytes
        .checked_add(writer_bytes)
        .expect("test publication buffer fits u64");
    alternative.demand.io_buffers.tiled_column_writer_bytes = alternative
        .demand
        .io_buffers
        .tiled_column_writer_bytes
        .max(writer_bytes);
    alternative.demand.io_buffers.mapped_page_cache_bytes = alternative
        .demand
        .io_buffers
        .mapped_page_cache_bytes
        .max(mapped_bytes);
    alternative.demand.caches.hard_resident_bytes = alternative
        .demand
        .caches
        .hard_resident_bytes
        .max(mapped_bytes);
    alternative.demand.caches.preferred_resident_bytes = alternative
        .demand
        .caches
        .preferred_resident_bytes
        .max(mapped_bytes);
    let dependencies = dag
        .nodes()
        .values()
        .flat_map(|node| {
            if node.fences.is_empty() {
                vec![WorkDependency::Work(node.id.clone())]
            } else {
                node.fences
                    .iter()
                    .map(|kind| WorkDependency::Fence(FenceId::new(node.id.clone(), *kind)))
                    .collect()
            }
        })
        .collect();
    let publication_node = WorkNode {
        id: publication.clone(),
        kind: WorkKind::Publication,
        domain: WorkDomain::Io,
        implementation: implementation.clone(),
        dependencies: BTreeSet::from([WorkDependency::Work(mapped_release.clone())]),
        claims: vec![
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::IoBuffer(IoBufferKind::Publication),
                amount: writer_bytes,
                lifetime: ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]),
            },
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::Storage {
                    demand_id: "output-storage".to_string(),
                    use_kind: StorageUseKind::StagedOutput,
                },
                amount: staged_bytes,
                lifetime: ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]),
            },
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::Storage {
                    demand_id: "output-storage".to_string(),
                    use_kind: StorageUseKind::FinalOutput,
                },
                amount: final_bytes,
                lifetime: ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]),
            },
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::StorageWriteRate {
                    demand_id: "output-storage".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]),
            },
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::StorageQueue {
                    demand_id: "output-storage".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]),
            },
        ],
        allocations: vec![AllocationUse {
            allocation: allocation.clone(),
            lifetime: ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]),
        }],
        fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
        quiescence_after: BTreeSet::new(),
    };
    let mut nodes = dag.nodes().values().cloned().collect::<Vec<_>>();
    nodes.extend([
        WorkNode {
            id: writer.clone(),
            kind: WorkKind::Io,
            domain: WorkDomain::Io,
            implementation: implementation.clone(),
            dependencies,
            claims: vec![
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::StorageWriteRate {
                        demand_id: "output-storage".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::StorageQueue {
                        demand_id: "output-storage".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                        IoBufferKind::TiledColumnWriter,
                    ),
                    amount: writer_bytes,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::Storage {
                        demand_id: "output-storage".to_string(),
                        use_kind: StorageUseKind::StagedOutput,
                    },
                    amount: staged_bytes,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: writer_allocation.clone(),
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            }],
            fences: BTreeSet::from([FenceKind::Io]),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: mapped.clone(),
            kind: WorkKind::Cache,
            domain: WorkDomain::Cpu,
            implementation: implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                writer.clone(),
                FenceKind::Io,
            ))]),
            claims: vec![
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                        IoBufferKind::MappedPageCache,
                    ),
                    amount: mapped_bytes,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::ResidentCache,
                    amount: mapped_bytes,
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: vec![AllocationUse {
                allocation: mapped_allocation.clone(),
                lifetime: ClaimLifetime::Work,
            }],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: mapped_release.clone(),
            kind: WorkKind::Release,
            domain: WorkDomain::Cpu,
            implementation,
            dependencies: BTreeSet::from([WorkDependency::Work(mapped.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                        IoBufferKind::MappedPageCache,
                    ),
                    amount: mapped_bytes,
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: vec![AllocationUse {
                allocation: mapped_allocation.clone(),
                lifetime: ClaimLifetime::Work,
            }],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        publication_node,
    ]);
    let mut logical_allocations = dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    logical_allocations.push(LogicalAllocation {
        id: allocation,
        bytes: writer_bytes,
        purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
        compatibility: compatibility.clone(),
        physical_slot: slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: publication.clone(),
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(publication.clone(), FenceKind::Io)),
                WorkDependency::Fence(FenceId::new(publication.clone(), FenceKind::Publication)),
            ]),
        },
    });
    logical_allocations.extend([
        LogicalAllocation {
            id: writer_allocation,
            bytes: writer_bytes,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::TiledColumnWriter),
            compatibility: compatibility.clone(),
            physical_slot: writer_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: writer.clone(),
                release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    writer.clone(),
                    FenceKind::Io,
                ))]),
            },
        },
        LogicalAllocation {
            id: mapped_allocation,
            bytes: mapped_bytes,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::MappedPageCache),
            compatibility: compatibility.clone(),
            physical_slot: mapped_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: mapped.clone(),
                release_after: BTreeSet::from([WorkDependency::Work(mapped_release.clone())]),
            },
        },
    ]);
    let mut physical_slots = dag.physical_slots().values().cloned().collect::<Vec<_>>();
    physical_slots.push(PhysicalSlot {
        id: slot,
        lease_resource: casa_imaging_runtime::LeaseResource::Memory {
            allocation_id: "product-publication-buffer".to_string(),
        },
        capacity_bytes: writer_bytes,
        compatibility,
    });
    let stage_compatibility = physical_slots
        .last()
        .expect("publication physical slot")
        .compatibility
        .clone();
    physical_slots.extend([
        PhysicalSlot {
            id: writer_slot,
            lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                allocation_id: "product-writer-buffer".to_string(),
            },
            capacity_bytes: writer_bytes,
            compatibility: stage_compatibility.clone(),
        },
        PhysicalSlot {
            id: mapped_slot,
            lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                allocation_id: "product-mapped-pages".to_string(),
            },
            capacity_bytes: mapped_bytes,
            compatibility: stage_compatibility,
        },
    ]);
    let mut initial_knobs = dag.initial_knobs().clone();
    initial_knobs.cache_retention_bytes = initial_knobs.cache_retention_bytes.max(mapped_bytes);
    let adaptations = dag
        .adaptations()
        .values()
        .cloned()
        .map(|mut transition| {
            transition.from.cache_retention_bytes =
                transition.from.cache_retention_bytes.max(mapped_bytes);
            transition.to.cache_retention_bytes =
                transition.to.cache_retention_bytes.max(mapped_bytes);
            transition
        })
        .collect();
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: dag.required_resource_capabilities().clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations,
        physical_slots,
        initial_knobs,
        adaptations,
    })
    .expect("valid exact-product test DAG");
    let mut stages = physical_work.prediction().stages().clone();
    stages.insert(
        writer.clone(),
        StagePrediction::new(writer, 1).with_io(vec![IoPrediction::new(
            IoBufferKind::TiledColumnWriter,
            writer_bytes,
            1,
        )]),
    );
    stages.insert(
        mapped.clone(),
        StagePrediction::new(mapped, 1).with_io(vec![IoPrediction::new(
            IoBufferKind::MappedPageCache,
            mapped_bytes,
            1,
        )]),
    );
    stages.insert(
        mapped_release.clone(),
        StagePrediction::new(mapped_release, 1).with_io(vec![IoPrediction::new(
            IoBufferKind::MappedPageCache,
            mapped_bytes,
            1,
        )]),
    );
    stages.insert(
        publication.clone(),
        StagePrediction::new(publication.clone(), 1).with_io(vec![IoPrediction::new(
            IoBufferKind::Publication,
            writer_bytes,
            1,
        )]),
    );
    let stages = stages.into_values().collect();
    let prediction = PlanPrediction::new(
        physical_work
            .prediction()
            .elapsed_nanos()
            .checked_add(4)
            .expect("test prediction fits u64"),
        physical_work.prediction().confidence(),
        physical_work.prediction().uncertainty().to_vec(),
        stages,
    )
    .expect("complete exact-product prediction");
    let mut artifacts = physical_work.artifacts().to_vec();
    artifacts.extend(product_artifacts.into_iter().map(|artifact| {
        PlannedArtifact::new(artifact, publication.clone(), ArtifactRole::Output, None)
    }));
    PhysicalWorkBinding::new(
        dag,
        prediction,
        artifacts,
        physical_work.observation_transaction().clone(),
        publication_layouts,
    )
    .expect("bound exact-product physical work")
}

fn evidenced_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    evidenced_physical_work_with_output(
        implementation_byte,
        ArtifactIdentity::from_sha256([34; 32]),
    )
}

fn evidenced_product_physical_work(
    problem: &casa_imaging_model::CompiledProblem,
    generation: &ProductGeneration,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    exact_product_physical_work(
        problem,
        generation,
        evidenced_physical_work_with_output(
            implementation_byte,
            publication_artifact(problem, generation),
        ),
    )
}

fn physical_work_with_required_product_staging(
    physical_work: &PhysicalWorkBinding,
    required_product_staging: BTreeMap<ProductKind, BTreeSet<WorkDependency>>,
) -> PhysicalWorkBinding {
    let transaction = physical_work.observation_transaction();
    let transaction = ObservationTransactionWork::new(
        transaction.initial_consistency_check().clone(),
        transaction.final_reconciliation().clone(),
        required_product_staging,
        transaction.model_column_staging().cloned(),
        transaction.commit().clone(),
    );
    PhysicalWorkBinding::new(
        physical_work.execution_dag().clone(),
        physical_work.prediction().clone(),
        physical_work.artifacts().to_vec(),
        transaction,
        physical_work.publication_layouts().clone(),
    )
    .expect("replace exact required-product staging fixture")
}

fn with_early_retained_publication_allocation(
    physical_work: PhysicalWorkBinding,
) -> PhysicalWorkBinding {
    let dag = physical_work.execution_dag();
    let acquire = physical_work
        .observation_transaction()
        .final_reconciliation()
        .clone();
    let publication = physical_work.observation_transaction().commit().clone();
    let allocation = AllocationId::new("transaction-publication-buffer");
    let slot = PhysicalSlotId::new("transaction-publication-slot");
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([dag.resource_alternative().demand.host_memory_view.clone()]),
        alignment_bytes: 1,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("transaction-publication-retention"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
    };

    let mut resource_alternative = dag.resource_alternative().clone();
    resource_alternative.demand.memory.push(MemoryDemand {
        allocation_id: "transaction-publication-slot".to_string(),
        hard_bytes: 1,
        preferred_bytes: 1,
        views: vec![resource_alternative.demand.host_memory_view.clone()],
    });
    let mut nodes = dag.nodes().values().cloned().collect::<Vec<_>>();
    nodes
        .iter_mut()
        .find(|node| node.id == acquire)
        .expect("transaction reconciliation node")
        .allocations
        .push(AllocationUse {
            allocation: allocation.clone(),
            lifetime: ClaimLifetime::Work,
        });
    nodes
        .iter_mut()
        .find(|node| node.id == publication)
        .expect("transaction publication node")
        .allocations
        .push(AllocationUse {
            allocation: allocation.clone(),
            lifetime: publication_lifetime,
        });

    let mut logical_allocations = dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    logical_allocations.push(LogicalAllocation {
        id: allocation,
        bytes: 1,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: acquire,
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(publication.clone(), FenceKind::Io)),
                WorkDependency::Fence(FenceId::new(publication, FenceKind::Publication)),
            ]),
        },
    });
    let mut physical_slots = dag.physical_slots().values().cloned().collect::<Vec<_>>();
    physical_slots.push(PhysicalSlot {
        id: slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "transaction-publication-slot".to_string(),
        },
        capacity_bytes: 1,
        compatibility,
    });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: dag.required_resource_capabilities().clone(),
        resource_alternative,
        nodes,
        logical_allocations,
        physical_slots,
        initial_knobs: dag.initial_knobs().clone(),
        adaptations: dag.adaptations().values().cloned().collect(),
    })
    .expect("valid early-retained publication allocation fixture");
    PhysicalWorkBinding::new(
        dag,
        physical_work.prediction().clone(),
        physical_work.artifacts().to_vec(),
        physical_work.observation_transaction().clone(),
        physical_work.publication_layouts().clone(),
    )
    .expect("bound early-retained publication allocation fixture")
}

fn evidenced_product_physical_work_without_mapped_layout(
    problem: &casa_imaging_model::CompiledProblem,
    generation: &ProductGeneration,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let product_node = problem.product_graph().publication().members()[0];
    let artifact = publication_artifact(problem, generation);
    let physical_work = evidenced_physical_work_with_output(implementation_byte, artifact);
    let writer = WorkNodeId::new("model-data-writer");
    let staging = PublicationStaging::new(
        writer.clone(),
        WorkDependency::Fence(FenceId::new(writer, FenceKind::Io)),
        IoBufferKind::TiledColumnWriter,
        AllocationId::new("model-data-writer-buffer"),
        None,
    )
    .expect("valid zero-mapped publication staging");
    let layout = PublicationPhysicalLayout::new(
        PublicationParticipant::Product(product_node),
        artifact,
        PhysicalLayoutId::from_sha256([92; 32]),
        staging,
        PublicationResourceBounds::new(2_048, 2_048, 16, 0)
            .expect("valid zero-mapped physical bounds"),
    );
    PhysicalWorkBinding::new(
        physical_work.execution_dag().clone(),
        physical_work.prediction().clone(),
        physical_work.artifacts().to_vec(),
        physical_work.observation_transaction().clone(),
        PublicationLayoutLedger::new(vec![layout]).expect("one physical product layout"),
    )
    .expect("bound zero-mapped product publication")
}

fn evidenced_physical_work_with_output(
    implementation_byte: u8,
    output: ArtifactIdentity,
) -> PhysicalWorkBinding {
    let initial = WorkNodeId::new("transaction-check");
    let observation_read = WorkNodeId::new("transaction-read");
    let read = WorkNodeId::new("read");
    let execute = WorkNodeId::new("execute");
    let writer = WorkNodeId::new("model-data-writer");
    let mapped = WorkNodeId::new("model-data-mapped-pages");
    let mapped_release = WorkNodeId::new("model-data-mapped-release");
    let publish = WorkNodeId::new("publish");
    let slot = PhysicalSlotId::new("evidence-io-slot");
    let writer_slot = PhysicalSlotId::new("model-data-writer-slot");
    let mapped_slot = PhysicalSlotId::new("model-data-mapped-slot");
    let source_buffer = AllocationId::new("source-read-ahead-buffer");
    let writer_buffer = AllocationId::new("model-data-writer-buffer");
    let mapped_buffer = AllocationId::new("model-data-mapped-pages");
    let publication_buffer = AllocationId::new("publication-buffer");
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("source-read-ahead"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("evidenced-cpu"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: vec![
                    MemoryDemand {
                        allocation_id: "evidence-io-memory".to_string(),
                        hard_bytes: 32,
                        preferred_bytes: 32,
                        views: vec![CapacityViewId::new("host-memory")],
                    },
                    MemoryDemand {
                        allocation_id: "model-data-writer-memory".to_string(),
                        hard_bytes: 16,
                        preferred_bytes: 16,
                        views: vec![CapacityViewId::new("host-memory")],
                    },
                    MemoryDemand {
                        allocation_id: "model-data-mapped-memory".to_string(),
                        hard_bytes: 32,
                        preferred_bytes: 32,
                        views: vec![CapacityViewId::new("host-memory")],
                    },
                ],
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: vec![StorageDemand {
                    demand_id: "output-storage".to_string(),
                    domain: StorageDomainId::new("test-output"),
                    temporary_bytes: 0,
                    staged_output_bytes: 2_048,
                    final_output_bytes: 2_048,
                    persistent_cache_bytes: 0,
                    read_rate: CountDemand::zero(),
                    write_rate: CountDemand::new(1, 1),
                    operations_rate: CountDemand::zero(),
                    queue_slots: CountDemand::new(1, 1),
                }],
                rates: vec![RateDemand {
                    demand_id: "io-rate".to_string(),
                    resource: RateResourceId::new("io-rate"),
                    amount: CountDemand::new(1, 1),
                }],
                caches: CacheDemand {
                    hard_resident_bytes: 32,
                    preferred_resident_bytes: 32,
                },
                locks: CountDemand::new(1, 1),
                file_descriptors: CountDemand::zero(),
                queues: vec![QueueDemand {
                    demand_id: "io-queue".to_string(),
                    resource: QueueResourceId::new("io-queue"),
                    slots: CountDemand::new(1, 1),
                }],
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand {
                    source_read_ahead_bytes: 32,
                    tiled_column_writer_bytes: 16,
                    publication_bytes: 32,
                    mapped_page_cache_bytes: 32,
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
                id: initial.clone(),
                kind: WorkKind::DataCensus,
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
                        resource: casa_imaging_runtime::LeaseResource::MeasurementSetLock {
                            measurement_set: MeasurementSetIdentity::new(identity(1)),
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
                id: observation_read.clone(),
                kind: WorkKind::ObservationRead,
                domain: WorkDomain::Io,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Work(initial.clone())]),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Rate {
                            demand_id: "io-rate".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Queue {
                            demand_id: "io-queue".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::MeasurementSetLock {
                            measurement_set: MeasurementSetIdentity::new(identity(1)),
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
                id: read.clone(),
                kind: WorkKind::Prefetch,
                domain: WorkDomain::Io,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    observation_read.clone(),
                    FenceKind::Io,
                ))]),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Rate {
                            demand_id: "io-rate".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Queue {
                            demand_id: "io-queue".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                            IoBufferKind::SourceReadAhead,
                        ),
                        amount: 32,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                ],
                allocations: vec![AllocationUse {
                    allocation: source_buffer.clone(),
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                }],
                fences: BTreeSet::from([FenceKind::Io]),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: execute.clone(),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    read.clone(),
                    FenceKind::Io,
                ))]),
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
                id: writer.clone(),
                kind: WorkKind::Io,
                domain: WorkDomain::Io,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Work(execute.clone())]),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Rate {
                            demand_id: "io-rate".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Queue {
                            demand_id: "io-queue".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                            IoBufferKind::TiledColumnWriter,
                        ),
                        amount: 16,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Storage {
                            demand_id: "output-storage".to_string(),
                            use_kind: StorageUseKind::StagedOutput,
                        },
                        amount: 2_048,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                ],
                allocations: vec![AllocationUse {
                    allocation: writer_buffer.clone(),
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                }],
                fences: BTreeSet::from([FenceKind::Io]),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: mapped.clone(),
                kind: WorkKind::Cache,
                domain: WorkDomain::Cpu,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    writer.clone(),
                    FenceKind::Io,
                ))]),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Workers,
                        amount: 1,
                        lifetime: ClaimLifetime::Work,
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                            IoBufferKind::MappedPageCache,
                        ),
                        amount: 32,
                        lifetime: ClaimLifetime::Work,
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::ResidentCache,
                        amount: 32,
                        lifetime: ClaimLifetime::Work,
                    },
                ],
                allocations: vec![AllocationUse {
                    allocation: mapped_buffer.clone(),
                    lifetime: ClaimLifetime::Work,
                }],
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: mapped_release.clone(),
                kind: WorkKind::Release,
                domain: WorkDomain::Cpu,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Work(mapped.clone())]),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Workers,
                        amount: 1,
                        lifetime: ClaimLifetime::Work,
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                            IoBufferKind::MappedPageCache,
                        ),
                        amount: 32,
                        lifetime: ClaimLifetime::Work,
                    },
                ],
                allocations: vec![AllocationUse {
                    allocation: mapped_buffer.clone(),
                    lifetime: ClaimLifetime::Work,
                }],
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: publish.clone(),
                kind: WorkKind::Publication,
                domain: WorkDomain::Io,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Work(mapped_release.clone())]),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                            IoBufferKind::Publication,
                        ),
                        amount: 32,
                        lifetime: ClaimLifetime::through_fences([
                            FenceKind::Io,
                            FenceKind::Publication,
                        ]),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Storage {
                            demand_id: "output-storage".to_string(),
                            use_kind: StorageUseKind::StagedOutput,
                        },
                        amount: 2_048,
                        lifetime: ClaimLifetime::through_fences([
                            FenceKind::Io,
                            FenceKind::Publication,
                        ]),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Storage {
                            demand_id: "output-storage".to_string(),
                            use_kind: StorageUseKind::FinalOutput,
                        },
                        amount: 2_048,
                        lifetime: ClaimLifetime::through_fences([
                            FenceKind::Io,
                            FenceKind::Publication,
                        ]),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::StorageWriteRate {
                            demand_id: "output-storage".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fences([
                            FenceKind::Io,
                            FenceKind::Publication,
                        ]),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::StorageQueue {
                            demand_id: "output-storage".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fences([
                            FenceKind::Io,
                            FenceKind::Publication,
                        ]),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::MeasurementSetLock {
                            measurement_set: MeasurementSetIdentity::new(identity(1)),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fences([
                            FenceKind::Io,
                            FenceKind::Publication,
                        ]),
                    },
                ],
                allocations: vec![AllocationUse {
                    allocation: publication_buffer.clone(),
                    lifetime: ClaimLifetime::through_fences([
                        FenceKind::Io,
                        FenceKind::Publication,
                    ]),
                }],
                fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
                quiescence_after: BTreeSet::new(),
            },
        ],
        logical_allocations: vec![
            LogicalAllocation {
                id: source_buffer,
                bytes: 32,
                purpose: AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead),
                compatibility: compatibility.clone(),
                physical_slot: slot.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: read.clone(),
                    release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                        read.clone(),
                        FenceKind::Io,
                    ))]),
                },
            },
            LogicalAllocation {
                id: writer_buffer,
                bytes: 16,
                purpose: AllocationPurpose::IoBuffer(IoBufferKind::TiledColumnWriter),
                compatibility: compatibility.clone(),
                physical_slot: writer_slot.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: writer.clone(),
                    release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                        writer.clone(),
                        FenceKind::Io,
                    ))]),
                },
            },
            LogicalAllocation {
                id: mapped_buffer,
                bytes: 32,
                purpose: AllocationPurpose::IoBuffer(IoBufferKind::MappedPageCache),
                compatibility: compatibility.clone(),
                physical_slot: mapped_slot.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: mapped.clone(),
                    release_after: BTreeSet::from([WorkDependency::Work(mapped_release.clone())]),
                },
            },
            LogicalAllocation {
                id: publication_buffer,
                bytes: 32,
                purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
                compatibility: compatibility.clone(),
                physical_slot: slot.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: publish.clone(),
                    release_after: BTreeSet::from([
                        WorkDependency::Fence(FenceId::new(publish.clone(), FenceKind::Io)),
                        WorkDependency::Fence(FenceId::new(
                            publish.clone(),
                            FenceKind::Publication,
                        )),
                    ]),
                },
            },
        ],
        physical_slots: vec![
            PhysicalSlot {
                id: slot,
                lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                    allocation_id: "evidence-io-memory".to_string(),
                },
                capacity_bytes: 32,
                compatibility: compatibility.clone(),
            },
            PhysicalSlot {
                id: writer_slot,
                lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                    allocation_id: "model-data-writer-memory".to_string(),
                },
                capacity_bytes: 16,
                compatibility: compatibility.clone(),
            },
            PhysicalSlot {
                id: mapped_slot,
                lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                    allocation_id: "model-data-mapped-memory".to_string(),
                },
                capacity_bytes: 32,
                compatibility,
            },
        ],
        initial_knobs: ExecutionKnobs {
            cache_retention_bytes: 32,
            ..ExecutionKnobs::serial()
        },
        adaptations: Vec::new(),
    })
    .expect("valid evidenced physical work DAG");
    let stages = dag
        .nodes()
        .keys()
        .map(|node| {
            let stage = StagePrediction::new(node.clone(), 100);
            if node == &read {
                stage.with_io(vec![IoPrediction::new(
                    IoBufferKind::SourceReadAhead,
                    8_192,
                    4,
                )])
            } else if node == &writer {
                stage.with_io(vec![IoPrediction::new(
                    IoBufferKind::TiledColumnWriter,
                    16,
                    1,
                )])
            } else if node == &mapped || node == &mapped_release {
                stage.with_io(vec![IoPrediction::new(
                    IoBufferKind::MappedPageCache,
                    32,
                    1,
                )])
            } else if node == &publish {
                stage.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 2_048, 1)])
            } else {
                stage
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
    let observation_transaction = ObservationTransactionWork::new(
        initial,
        execute,
        BTreeMap::from([(
            ProductKind::Psf,
            BTreeSet::from([WorkDependency::Fence(FenceId::new(writer, FenceKind::Io))]),
        )]),
        None,
        publish.clone(),
    );
    PhysicalWorkBinding::new(
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
            PlannedArtifact::new(output, publish, ArtifactRole::Output, None),
        ],
        observation_transaction,
        model_data_layout(output, MeasurementSetIdentity::new(identity(80))),
    )
    .expect("bound evidenced physical work")
}

fn physical_work_with_artifacts(
    physical_work: &PhysicalWorkBinding,
    artifacts: Vec<PlannedArtifact>,
) -> PhysicalWorkBinding {
    let outputs = artifacts
        .iter()
        .filter(|artifact| artifact.role() == ArtifactRole::Output)
        .map(PlannedArtifact::identity)
        .collect::<BTreeSet<_>>();
    let layouts = physical_work
        .publication_layouts()
        .entries()
        .iter()
        .filter(|layout| outputs.contains(&layout.artifact()))
        .cloned()
        .collect::<Vec<_>>();
    let layouts = if layouts.is_empty() {
        PublicationLayoutLedger::empty()
    } else {
        PublicationLayoutLedger::new(layouts).expect("mutated publication layouts")
    };
    PhysicalWorkBinding::new(
        physical_work.execution_dag().clone(),
        physical_work.prediction().clone(),
        artifacts,
        physical_work.observation_transaction().clone(),
        layouts,
    )
    .expect("valid artifact mutation fixture")
}

fn physical_work_with_second_publication(
    physical_work: &PhysicalWorkBinding,
) -> PhysicalWorkBinding {
    let dag = physical_work.execution_dag();
    let mut second = dag
        .nodes()
        .values()
        .find(|node| node.kind == WorkKind::Publication)
        .expect("first Publication node")
        .clone();
    let first_publication = second.id.clone();
    second.id = WorkNodeId::new("second-publication");
    second.dependencies = BTreeSet::from([
        WorkDependency::Fence(FenceId::new(first_publication.clone(), FenceKind::Io)),
        WorkDependency::Fence(FenceId::new(
            first_publication.clone(),
            FenceKind::Publication,
        )),
    ]);
    let mut nodes = dag.nodes().values().cloned().collect::<Vec<_>>();
    nodes.push(second.clone());
    let second_allocations = second
        .allocations
        .iter()
        .map(|allocation| allocation.allocation.clone())
        .collect::<BTreeSet<_>>();
    let logical_allocations = dag
        .logical_allocations()
        .values()
        .cloned()
        .map(|mut allocation| {
            if second_allocations.contains(&allocation.id) {
                allocation.lifetime.release_after.extend([
                    WorkDependency::Fence(FenceId::new(second.id.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(second.id.clone(), FenceKind::Publication)),
                ]);
            }
            allocation
        })
        .collect();
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: dag.required_resource_capabilities().clone(),
        resource_alternative: dag.resource_alternative().clone(),
        nodes,
        logical_allocations,
        physical_slots: dag.physical_slots().values().cloned().collect(),
        initial_knobs: dag.initial_knobs().clone(),
        adaptations: dag.adaptations().values().cloned().collect(),
    })
    .expect("valid two-publication DAG fixture");
    let mut stages = physical_work.prediction().stages().clone();
    let first_stage = stages
        .get(&first_publication)
        .expect("first Publication prediction");
    let second_stage = StagePrediction::new(second.id.clone(), first_stage.elapsed_nanos())
        .with_io(first_stage.io().to_vec());
    let second_elapsed = second_stage.elapsed_nanos();
    stages.insert(second.id, second_stage);
    let prediction = PlanPrediction::new(
        physical_work.prediction().elapsed_nanos() + second_elapsed,
        physical_work.prediction().confidence(),
        physical_work.prediction().uncertainty().to_vec(),
        stages.into_values().collect(),
    )
    .expect("complete two-publication prediction");
    PhysicalWorkBinding::new(
        dag,
        prediction,
        physical_work.artifacts().to_vec(),
        physical_work.observation_transaction().clone(),
        physical_work.publication_layouts().clone(),
    )
    .expect("bound two-publication fixture")
}

#[test]
fn plan_requires_the_exact_product_generation_publication() {
    let problem = compile(request(1)).expect("compile product problem");
    let graph = problem.product_graph();
    let generation = product_generation(&problem);
    let member = graph.publication().members()[0];
    let expected_artifact = publication_artifact(&problem, &generation);
    let bindings = PlanningBindings::new(
        generation.clone(),
        registry(3),
        ResourcePolicy::Balanced,
        cost_model(4),
    );
    let execution_plan = plan(&problem, bindings.clone(), |_, _| {
        Ok::<_, ()>(evidenced_product_physical_work(&problem, &generation, 6))
    })
    .expect("seal exact product publication");
    let binding = execution_plan.product_publication();
    assert_eq!(ExecutionPlanId::SCHEMA_VERSION, 7);
    assert_eq!(
        execution_plan.product_generation_id(),
        generation.generation_id()
    );
    assert_eq!(binding.generation_id(), generation.generation_id());
    assert_eq!(binding.publication_node(), &WorkNodeId::new("publish"));
    assert_eq!(binding.artifacts()[0].product_node(), member);
    assert_eq!(binding.artifacts()[0].artifact(), expected_artifact);

    assert!(matches!(
        plan(&problem, bindings, |_, _| Ok::<_, ()>(
            evidenced_physical_work(6)
        )),
        Err(PlanError::ProductPublication(
            ProductPublicationBindingError::MissingArtifact { .. }
        ))
    ));
}

#[test]
fn plan_rejects_model_data_without_bound_observation_write_set() {
    let problem = compile(request(1)).expect("compile product problem");
    let generation = product_generation(&problem);
    let product_node = problem.product_graph().publication().members()[0];
    let product_artifact = publication_artifact(&problem, &generation);
    let model_artifact = ArtifactIdentity::from_sha256([90; 32]);
    let measurement_set = MeasurementSetIdentity::new(identity(91));
    let base = evidenced_physical_work_with_output(6, model_artifact);
    let publication = WorkNodeId::new("publish");
    let staging = tiled_publication_staging(
        "model-data-writer",
        "model-data-writer-buffer",
        "model-data-mapped-pages",
        "model-data-mapped-pages",
        "model-data-mapped-release",
    );
    let layouts = PublicationLayoutLedger::new(vec![
        PublicationPhysicalLayout::new(
            PublicationParticipant::Product(product_node),
            product_artifact,
            PhysicalLayoutId::from_sha256([34; 32]),
            staging.clone(),
            PublicationResourceBounds::new(1_024, 1_024, 8, 16).expect("product physical bounds"),
        ),
        PublicationPhysicalLayout::new(
            PublicationParticipant::ModelData(measurement_set),
            model_artifact,
            PhysicalLayoutId::from_sha256([35; 32]),
            staging,
            PublicationResourceBounds::new(1_024, 1_024, 8, 16)
                .expect("MODEL_DATA physical bounds"),
        ),
    ])
    .expect("canonical product and MODEL_DATA layouts");
    let mut artifacts = base.artifacts().to_vec();
    artifacts.push(PlannedArtifact::new(
        product_artifact,
        publication,
        ArtifactRole::Output,
        None,
    ));
    let physical_work = PhysicalWorkBinding::new(
        base.execution_dag().clone(),
        base.prediction().clone(),
        artifacts,
        base.observation_transaction().clone(),
        layouts,
    )
    .expect("otherwise-valid product and MODEL_DATA physical work");
    let bindings = PlanningBindings::new(
        generation,
        registry(3),
        ResourcePolicy::Balanced,
        cost_model(4),
    );

    let Err(error) = plan(&problem, bindings, |_, _| Ok::<_, ()>(physical_work)) else {
        panic!("MODEL_DATA requires observation write-set authority");
    };
    let PlanError::ProductPublication(publication_error) = error else {
        panic!("MODEL_DATA must fail at the product-publication seal");
    };
    assert!(matches!(
        &publication_error,
        ProductPublicationBindingError::MissingObservationWriteSetAuthority {
            participant: PublicationParticipant::ModelData(actual),
        } if *actual == measurement_set
    ));
    assert!(
        publication_error
            .to_string()
            .contains("exact observation write-set authority")
    );
}

#[test]
fn physical_work_requires_adapter_derived_publication_layouts() {
    let problem = compile(request_with_products(
        1,
        vec![ProductKind::Psf, ProductKind::Residual],
    ))
    .expect("compile multi-product problem");
    let generation = product_generation(&problem);
    let physical_work =
        exact_product_physical_work(&problem, &generation, evidenced_physical_work(6));
    let publication = physical_work
        .execution_dag()
        .nodes()
        .values()
        .find(|node| node.kind == WorkKind::Publication)
        .expect("sole publication node")
        .id
        .clone();
    let model_artifact = ArtifactIdentity::from_sha256([90; 32]);
    let mut artifacts = physical_work.artifacts().to_vec();
    artifacts.push(PlannedArtifact::new(
        model_artifact,
        publication,
        ArtifactRole::Output,
        None,
    ));

    let mut layouts = problem
        .product_graph()
        .publication()
        .members()
        .iter()
        .enumerate()
        .map(|(index, product_node)| {
            PublicationPhysicalLayout::new(
                PublicationParticipant::Product(*product_node),
                ArtifactIdentity::from_sha256(
                    generation
                        .artifact_id(*product_node)
                        .expect("generation artifact")
                        .as_bytes(),
                ),
                PhysicalLayoutId::from_sha256(
                    [u8::try_from(index + 1).expect("small layout fixture"); 32],
                ),
                tiled_publication_staging(
                    "model-data-writer",
                    "model-data-writer-buffer",
                    "model-data-mapped-pages",
                    "model-data-mapped-pages",
                    "model-data-mapped-release",
                ),
                PublicationResourceBounds::new(
                    4_096 + u64::try_from(index).expect("small layout fixture"),
                    8_192 + u64::try_from(index).expect("small layout fixture"),
                    512 + u64::try_from(index).expect("small layout fixture"),
                    64 + u64::try_from(index).expect("small layout fixture"),
                )
                .expect("large physical bounds"),
            )
        })
        .collect::<Vec<_>>();
    layouts.push(PublicationPhysicalLayout::new(
        PublicationParticipant::ModelData(MeasurementSetIdentity::new(identity(90))),
        model_artifact,
        PhysicalLayoutId::from_sha256([91; 32]),
        tiled_publication_staging(
            "model-data-writer",
            "model-data-writer-buffer",
            "model-data-mapped-pages",
            "model-data-mapped-pages",
            "model-data-mapped-release",
        ),
        PublicationResourceBounds::new(16_384, 32_768, 2_048, 4_096)
            .expect("MODEL_DATA physical bounds"),
    ));
    let ledger = PublicationLayoutLedger::new(layouts).expect("canonical physical layouts");
    assert_eq!(ledger.entries().len(), 3);
    assert_eq!(ledger.staged_storage_bytes(), 24_577);
    assert_eq!(ledger.final_storage_bytes(), 49_153);
    assert_eq!(ledger.writer_buffer_bytes(), 3_073);
    assert_eq!(ledger.mapped_page_cache_bytes(), 4_225);

    assert!(matches!(
        PhysicalWorkBinding::new(
            physical_work.execution_dag().clone(),
            physical_work.prediction().clone(),
            artifacts,
            physical_work.observation_transaction().clone(),
            ledger,
        ),
        Err(PhysicalWorkBindingError::InsufficientPublicationResources { .. })
    ));
}

#[test]
fn publication_layouts_bind_staging_topology_and_resources() {
    let first_ms = MeasurementSetIdentity::new(identity(81));
    let second_ms = MeasurementSetIdentity::new(identity(82));
    let first_artifact = ArtifactIdentity::from_sha256([83; 32]);
    let second_artifact = ArtifactIdentity::from_sha256([84; 32]);
    let staging = |producer: &str, allocation: &str| {
        let producer = WorkNodeId::new(producer);
        PublicationStaging::new(
            producer.clone(),
            WorkDependency::Fence(FenceId::new(producer, FenceKind::Io)),
            IoBufferKind::TiledColumnWriter,
            AllocationId::new(allocation),
            None,
        )
        .expect("valid exact publication staging binding")
    };
    let zero_mapped_bounds = PublicationResourceBounds::new(2_048, 2_048, 16, 0)
        .expect("zero mapped exposure is a valid physical bound");

    let serial_reuse = PublicationLayoutLedger::new(vec![
        PublicationPhysicalLayout::new(
            PublicationParticipant::ModelData(first_ms),
            first_artifact,
            PhysicalLayoutId::from_sha256([85; 32]),
            staging("first-ms-writer", "shared-writer-buffer"),
            zero_mapped_bounds,
        ),
        PublicationPhysicalLayout::new(
            PublicationParticipant::ModelData(second_ms),
            second_artifact,
            PhysicalLayoutId::from_sha256([86; 32]),
            staging("second-ms-writer", "shared-writer-buffer"),
            zero_mapped_bounds,
        ),
    ])
    .expect("distinct MeasurementSets are distinct publication participants");
    assert_eq!(serial_reuse.entries().len(), 2);
    assert_eq!(serial_reuse.staged_storage_bytes(), 4_096);
    assert_eq!(serial_reuse.final_storage_bytes(), 4_096);
    assert_eq!(serial_reuse.writer_buffer_bytes(), 16);
    assert_eq!(serial_reuse.mapped_page_cache_bytes(), 0);

    let shared_producer = staging("shared-ms-writer", "shared-writer-buffer");
    let concurrent_members = PublicationLayoutLedger::new(vec![
        PublicationPhysicalLayout::new(
            PublicationParticipant::ModelData(first_ms),
            first_artifact,
            PhysicalLayoutId::from_sha256([85; 32]),
            shared_producer.clone(),
            zero_mapped_bounds,
        ),
        PublicationPhysicalLayout::new(
            PublicationParticipant::ModelData(second_ms),
            second_artifact,
            PhysicalLayoutId::from_sha256([86; 32]),
            shared_producer,
            zero_mapped_bounds,
        ),
    ])
    .expect("one producer may stage multiple concurrent publication members");
    assert_eq!(concurrent_members.writer_buffer_bytes(), 32);

    assert!(matches!(
        PublicationLayoutLedger::new(vec![
            PublicationPhysicalLayout::new(
                PublicationParticipant::ModelData(first_ms),
                first_artifact,
                PhysicalLayoutId::from_sha256([85; 32]),
                staging("first-stage", "first-writer-buffer"),
                zero_mapped_bounds,
            ),
            PublicationPhysicalLayout::new(
                PublicationParticipant::ModelData(first_ms),
                second_artifact,
                PhysicalLayoutId::from_sha256([86; 32]),
                staging("second-stage", "second-writer-buffer"),
                zero_mapped_bounds,
            ),
        ]),
        Err(PublicationLayoutError::DuplicateParticipant {
            participant: PublicationParticipant::ModelData(actual),
        }) if actual == first_ms
    ));

    let physical_work = staged_model_data_physical_work(first_artifact, first_ms);
    let writer = WorkNodeId::new("model-data-writer");
    let completion = WorkDependency::Fence(FenceId::new(writer.clone(), FenceKind::Io));
    let exact_staging = PublicationStaging::new(
        writer.clone(),
        completion.clone(),
        IoBufferKind::TiledColumnWriter,
        AllocationId::new("model-data-writer-buffer"),
        None,
    )
    .expect("exact writer-owned staging binding");
    let layout = PublicationPhysicalLayout::new(
        PublicationParticipant::ModelData(first_ms),
        first_artifact,
        PhysicalLayoutId::from_sha256([85; 32]),
        exact_staging.clone(),
        zero_mapped_bounds,
    );
    assert_eq!(layout.staging(), &exact_staging);
    assert_eq!(layout.resource_bounds(), zero_mapped_bounds);
    let publication = physical_work
        .execution_dag()
        .nodes()
        .values()
        .find(|node| node.kind == WorkKind::Publication)
        .expect("final Publication node");
    assert!(publication.dependencies.iter().any(|dependency| {
        dependency == &completion
            || dependency == &WorkDependency::Work(WorkNodeId::new("model-data-mapped-release"))
    }));

    let bind_with = |layout: PublicationPhysicalLayout| {
        PhysicalWorkBinding::new(
            physical_work.execution_dag().clone(),
            physical_work.prediction().clone(),
            physical_work.artifacts().to_vec(),
            physical_work.observation_transaction().clone(),
            PublicationLayoutLedger::new(vec![layout]).expect("one physical layout"),
        )
    };
    let layout_with = |staging, bounds| {
        PublicationPhysicalLayout::new(
            PublicationParticipant::ModelData(first_ms),
            first_artifact,
            PhysicalLayoutId::from_sha256([85; 32]),
            staging,
            bounds,
        )
    };
    bind_with(layout.clone()).expect("writer resources may be released after staging completes");

    assert!(matches!(
        bind_with(layout_with(
            staging("missing-stage", "model-data-writer-buffer"),
            zero_mapped_bounds,
        )),
        Err(PhysicalWorkBindingError::UnknownPublicationStage { .. })
    ));
    assert!(matches!(
        bind_with(layout_with(
            PublicationStaging::new(
                writer.clone(),
                WorkDependency::Work(writer.clone()),
                IoBufferKind::TiledColumnWriter,
                AllocationId::new("model-data-writer-buffer"),
                None,
            )
            .expect("same-node nonterminal staging fixture"),
            zero_mapped_bounds,
        )),
        Err(PhysicalWorkBindingError::NonterminalPublicationStage { .. })
    ));
    assert!(matches!(
        bind_with(layout_with(
            PublicationStaging::new(
                WorkNodeId::new("publish"),
                WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("publish"),
                    FenceKind::Publication,
                )),
                IoBufferKind::TiledColumnWriter,
                AllocationId::new("model-data-writer-buffer"),
                None,
            )
            .expect("same-node publication-order fixture"),
            zero_mapped_bounds,
        )),
        Err(PhysicalWorkBindingError::PublicationStageOrder { .. })
    ));
    assert!(matches!(
        bind_with(layout_with(
            exact_staging.clone(),
            PublicationResourceBounds::new(2_048, 2_048, 17, 0).expect("writer shortage fixture"),
        )),
        Err(PhysicalWorkBindingError::InsufficientPublicationResources {
            kind: PublicationBoundKind::WriterBuffer,
            required: 17,
            declared: 16,
        })
    ));
    assert!(matches!(
        bind_with(layout_with(
            PublicationStaging::new(
                WorkNodeId::new("model-data-mapped-pages"),
                WorkDependency::Work(WorkNodeId::new("model-data-mapped-pages")),
                IoBufferKind::TiledColumnWriter,
                AllocationId::new("model-data-writer-buffer"),
                None,
            )
            .expect("shared-ancestry fixture"),
            zero_mapped_bounds,
        )),
        Err(PhysicalWorkBindingError::PublicationStageAllocation { .. })
    ));
}

#[test]
fn plan_rejects_inexact_product_publications_and_hashes_generation() {
    let problem = compile(request(1)).expect("compile product problem");
    let generation = product_generation(&problem);
    let expected_artifact = publication_artifact(&problem, &generation);
    let bindings = || {
        PlanningBindings::new(
            generation.clone(),
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        )
    };

    let stale_problem = compile(request(2)).expect("compile stale product problem");
    let stale_generation = product_generation(&stale_problem);
    assert!(matches!(
        plan(
            &problem,
            PlanningBindings::new(
                stale_generation,
                registry(3),
                ResourcePolicy::Balanced,
                cost_model(4),
            ),
            |_, _| Ok::<_, ()>(evidenced_product_physical_work(&problem, &generation, 6)),
        ),
        Err(PlanError::ProductPublication(
            ProductPublicationBindingError::StaleGeneration { .. }
        ))
    ));

    let exact = evidenced_product_physical_work(&problem, &generation, 6);
    let wrong_role = physical_work_with_artifacts(
        &exact,
        exact
            .artifacts()
            .iter()
            .map(|artifact| {
                if artifact.identity() == expected_artifact {
                    PlannedArtifact::new(
                        artifact.identity(),
                        artifact.node().clone(),
                        ArtifactRole::Prepared,
                        None,
                    )
                } else {
                    artifact.clone()
                }
            })
            .collect(),
    );
    assert!(matches!(
        plan(&problem, bindings(), |_, _| Ok::<_, ()>(wrong_role)),
        Err(PlanError::ProductPublication(
            ProductPublicationBindingError::ArtifactRole { .. }
        ))
    ));

    let wrong_node_artifacts = exact
        .artifacts()
        .iter()
        .map(|artifact| {
            if artifact.identity() == expected_artifact {
                PlannedArtifact::new(
                    artifact.identity(),
                    WorkNodeId::new("read"),
                    ArtifactRole::Output,
                    None,
                )
            } else {
                artifact.clone()
            }
        })
        .collect();
    assert!(matches!(
        PhysicalWorkBinding::new(
            exact.execution_dag().clone(),
            exact.prediction().clone(),
            wrong_node_artifacts,
            exact.observation_transaction().clone(),
            exact.publication_layouts().clone(),
        ),
        Err(PhysicalWorkBindingError::MissingPublicationContract { .. })
    ));

    assert!(matches!(
        plan(&problem, bindings(), |_, _| Ok::<_, ()>(physical_work(6))),
        Err(PlanError::ProductPublication(
            ProductPublicationBindingError::MissingArtifact { .. }
        ))
    ));
    let multiple = physical_work_with_second_publication(&exact);
    assert!(matches!(
        plan(&problem, bindings(), |_, _| Ok::<_, ()>(multiple)),
        Err(PlanError::ObservationTransaction(
            ObservationTransactionPlanError::InvalidPlan { ref reason }
        )) if reason == "publication node second-publication bypasses the atomic commit gate"
    ));

    let multi_problem = compile(request_with_products(
        1,
        vec![ProductKind::Psf, ProductKind::Residual],
    ))
    .expect("compile multi-member products");
    let multi_generation = product_generation(&multi_problem);
    let multi_base = evidenced_physical_work(6);
    let product_completion = multi_base
        .observation_transaction()
        .required_product_staging()[&ProductKind::Psf]
        .clone();
    let multi_base = physical_work_with_required_product_staging(
        &multi_base,
        BTreeMap::from([
            (ProductKind::Psf, product_completion.clone()),
            (ProductKind::Residual, product_completion),
        ]),
    );
    let complete = exact_product_physical_work(&multi_problem, &multi_generation, multi_base);
    let omitted = publication_artifacts(&multi_problem, &multi_generation)
        .pop()
        .expect("second product artifact");
    let partial = physical_work_with_artifacts(
        &complete,
        complete
            .artifacts()
            .iter()
            .filter(|artifact| artifact.identity() != omitted)
            .cloned()
            .collect(),
    );
    assert!(matches!(
        plan(
            &multi_problem,
            PlanningBindings::new(
                multi_generation,
                registry(3),
                ResourcePolicy::Balanced,
                cost_model(4),
            ),
            |_, _| Ok::<_, ()>(partial),
        ),
        Err(PlanError::ProductPublication(
            ProductPublicationBindingError::MissingArtifact { .. }
        ))
    ));

    let first_generation = product_generation_with_source(&problem, 41);
    let second_generation = product_generation_with_source(&problem, 42);
    let first_plan = plan(
        &problem,
        PlanningBindings::new(
            first_generation.clone(),
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| {
            Ok::<_, ()>(evidenced_product_physical_work(
                &problem,
                &first_generation,
                6,
            ))
        },
    )
    .expect("first exact generation plan");
    let second_plan = plan(
        &problem,
        PlanningBindings::new(
            second_generation.clone(),
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| {
            Ok::<_, ()>(evidenced_product_physical_work(
                &problem,
                &second_generation,
                6,
            ))
        },
    )
    .expect("second exact generation plan");
    assert_ne!(first_plan.plan_id(), second_plan.plan_id());
}

fn adaptive_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
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
        specification,
        implementation(implementation_byte),
        None,
        false,
        false,
    )
}

fn auditable_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
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
    let mut logical_allocations = base_dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    logical_allocations.push(LogicalAllocation {
        id: allocation,
        bytes: 64,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: WorkNodeId::new("first-major-work"),
            release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new("minor-work"))]),
        },
    });
    let mut physical_slots = base_dag
        .physical_slots()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    physical_slots.push(PhysicalSlot {
        id: slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "audit-memory".to_string(),
        },
        capacity_bytes: 64,
        compatibility,
    });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::from([casa_imaging_runtime::CapabilityId::new(
            "audit-capability",
        )]),
        resource_alternative: alternative,
        nodes,
        logical_allocations,
        physical_slots,
        initial_knobs: base_dag.initial_knobs().clone(),
        adaptations: base_dag.adaptations().values().cloned().collect(),
    })
    .expect("valid auditable transaction work");
    bind_physical_work(
        dag,
        base.observation_transaction().clone(),
        vec![PlannedArtifact::new(
            ArtifactIdentity::from_sha256([51; 32]),
            WorkNodeId::new("first-major-work"),
            ArtifactRole::Cache,
            Some(CacheIdentity::from_sha256([52; 32])),
        )],
        PublicationLayoutLedger::empty(),
    )
}

fn release_failure_physical_work(
    implementation_byte: u8,
    release_implementation_byte: u8,
    fail_at_fence: bool,
) -> PhysicalWorkBinding {
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
    let binding = transaction_binding(
        specification,
        implementation(implementation_byte),
        None,
        false,
        false,
    );
    let dag = binding.execution_dag().clone();
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let mut io = BTreeMap::<IoBufferKind, u64>::new();
            for claim in &node.claims {
                if let LeaseResource::IoBuffer(kind) = claim.resource {
                    *io.entry(kind).or_default() += claim.amount;
                }
            }
            let stage = StagePrediction::new(node.id.clone(), 100);
            if io.is_empty() {
                stage
            } else {
                stage.with_io(
                    io.into_iter()
                        .map(|(kind, bytes)| {
                            let predicted = if node.id == prepare_id || node.id == release_id {
                                100
                            } else {
                                bytes
                            };
                            IoPrediction::new(kind, predicted, 1)
                        })
                        .collect(),
                )
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete release-failure prediction");
    PhysicalWorkBinding::new(
        dag,
        prediction,
        binding.artifacts().to_vec(),
        binding.observation_transaction().clone(),
        binding.publication_layouts().clone(),
    )
    .expect("bound external-release failure work")
}

fn test_registry(
    registry_byte: u8,
    implementation_byte: u8,
    failure: Option<&'static str>,
) -> TestRegistry {
    TestRegistry {
        id: registry(registry_byte),
        executors: BTreeMap::from([(
            implementation(implementation_byte),
            recording_executor(implementation_byte, failure, None),
        )]),
    }
}

struct TestRegistry {
    id: ImplementationRegistryId,
    executors: BTreeMap<WorkImplementationId, RecordingExecutor>,
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
}

fn authority() -> &'static ResourceAuthority {
    static AUTHORITY: OnceLock<&'static ResourceAuthority> = OnceLock::new();
    AUTHORITY.get_or_init(|| {
        ResourceAuthority::install_production_inventory(runtime_inventory(4))
            .expect("install deterministic combined receipt and transaction inventory")
    })
}

fn runtime_inventory(available_locks: u64) -> HostInventory {
    let domain = CapacityDomainId::new("host-memory");
    let view = CapacityViewId::new("host-memory");
    let rate = RateResourceId::new("io-rate");
    let queue = QueueResourceId::new("io-queue");
    let transaction_rate = RateResourceId::new("transaction-io-rate");
    let transaction_queue = QueueResourceId::new("transaction-io-queue");
    let storage = StorageDomainId::new("atomic-output");
    let product_storage = StorageDomainId::new("test-output");
    HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1_024,
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
                    capacity_bytes: 1_024,
                    read_rate: rate.clone(),
                    write_rate: rate.clone(),
                    operations_rate: None,
                    queue: queue.clone(),
                },
                StorageDomain {
                    id: product_storage.clone(),
                    root: PathBuf::from("/tmp/casa-rs-imaging-product-tests"),
                    capacity_bytes: 5_462,
                    read_rate: rate.clone(),
                    write_rate: rate.clone(),
                    operations_rate: None,
                    queue: queue.clone(),
                },
            ],
            rate_resources: vec![
                RateResource::new(rate.clone(), RateUnit::BytesPerSecond, 16),
                RateResource::new(transaction_rate.clone(), RateUnit::BytesPerSecond, 16),
            ],
            queue_resources: vec![
                QueueResource::new(queue.clone(), 4),
                QueueResource::new(transaction_queue.clone(), 4),
            ],
            logical_cpu_threads: 4,
            performance_cpu_cores: CpuClassCapacity::Known(4),
            cache_capacity_bytes: 1_024,
            lock_capacity: 4,
            file_descriptor_capacity: 16,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_024)]),
            available_cpu_threads: 4,
            storage_available_bytes: BTreeMap::from([(storage, 1_024), (product_storage, 5_462)]),
            rate_available_per_second: BTreeMap::from([(rate, 16), (transaction_rate, 16)]),
            queue_available_slots: BTreeMap::from([(queue, 4), (transaction_queue, 4)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_024,
            available_locks,
            available_file_descriptors: 16,
        },
    }
}

fn run_lock() -> &'static Mutex<()> {
    static RUN_LOCK: Mutex<()> = Mutex::new(());
    &RUN_LOCK
}

fn run<C: RunController>(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
    authority: &ResourceAuthority,
    controller: &mut C,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    run_receipted(
        problem,
        plan,
        current,
        registry,
        authority,
        controller,
        receipts.bind(ExecutionProvenance::new(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([241; 32]),
            BuildIdentity::from_sha256([242; 32]),
        )),
    )
}

fn execute_plan(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let _guard = run_lock().lock().expect("runtime test lock");
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
    let output_layouts =
        model_data_layout(output.identity(), MeasurementSetIdentity::new(identity(79)));

    assert!(matches!(
        PhysicalWorkBinding::new(
            publication_dag,
            publication_prediction,
            vec![output],
            publication_base.observation_transaction().clone(),
            output_layouts,
        ),
        Err(PhysicalWorkBindingError::MissingPublicationContract { .. })
    ));
}

#[test]
fn physical_work_binding_rejects_typed_io_contracts_without_predictions() {
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
    let mut stages = base.prediction().stages().clone();
    stages.insert(prepare.clone(), StagePrediction::new(prepare, 100));
    let prediction = PlanPrediction::new(
        300,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        stages.into_values().collect(),
    )
    .expect("complete prediction ledger without preparation I/O evidence");

    assert!(matches!(
        PhysicalWorkBinding::new(
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
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                evidenced_physical_work(6),
            ))
        },
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
                ArtifactMeasurement::new(
                    input,
                    Some(input),
                    ArtifactDisposition::Published,
                    4_096,
                    None,
                ),
                ArtifactMeasurement::new(cache, None, ArtifactDisposition::RejectedStale, 0, None),
            ],
        ),
    )]);
    let registry = TestRegistry {
        id: registry(3),
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
            disposition: ArtifactDisposition::Published,
        }) if node == WorkNodeId::new("read") && artifact == input
    ));
}

#[test]
fn run_can_invoke_only_the_implementation_identity_sealed_by_plan() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                evidenced_physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let selected = recording_executor(6, None, None);
    let different = recording_executor(7, None, None);
    let registry = TestRegistry {
        id: registry(3),
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
fn generic_io_cannot_receive_observation_sources() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
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
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    execute_plan(&problem, &execution_plan, &current, &registry).expect("bound execution");

    assert!(
        !generic_source_access.load(Ordering::SeqCst),
        "generic Io must not receive the MeasurementSet observation source set"
    );
}

#[test]
fn observation_read_completion_requires_terminal_owner_evidence() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(evidenced_product_physical_work(
                problem,
                bindings.product_generation(),
                6,
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    executor.omit_observation_read_completion = true;
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    let error = execute_plan(&problem, &execution_plan, &current, &registry)
        .expect_err("an ObservationRead cannot complete without owner-minted coverage evidence");

    assert_eq!(
        error.to_string(),
        "execution evidence failed: node transaction-read omitted observation-read completion evidence"
    );
}

#[test]
fn observation_read_completion_binds_compiled_inspection_and_rejects_content_mismatch() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(evidenced_product_physical_work(
                problem,
                bindings.product_generation(),
                6,
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let cache = Arc::new(Mutex::new(None));
    let samples = actual_observation_read_samples(1);
    let expected = problem
        .inspect_selected_observation(samples.iter().copied())
        .expect("inspect exact adapter samples");
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    executor.observation_read_completion_cache = Some(Arc::clone(&cache));
    let exact_registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    execute_plan(&problem, &execution_plan, &current, &exact_registry)
        .expect("exact inspected samples mint terminal read evidence");
    let completion = cache
        .lock()
        .expect("observation-read completion cache lock")
        .clone()
        .expect("cached observation-read completion");
    let evidence = completion
        .observation_read_completion()
        .expect("typed observation-read completion");
    assert_eq!(
        evidence.commitment_id(),
        problem.selected_observation().commitment_id()
    );
    assert_eq!(evidence.generation_id(), expected.0);
    assert_eq!(evidence.sample_count(), expected.1);

    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    executor.mismatch_observation_read_samples = true;
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let error = execute_plan(&problem, &execution_plan, &current, &registry)
        .expect_err("same-count wrong physical rows cannot mint terminal evidence");
    assert!(matches!(
        error,
        RunError::Execution { node, source }
            if node == WorkNodeId::new("transaction-read")
                && source.to_string().contains("selected physical MAIN rows do not match")
    ));
}

#[test]
fn observation_read_completion_preserves_midstream_and_terminal_stream_failures() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(evidenced_product_physical_work(
                problem,
                bindings.product_generation(),
                6,
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    for failure in [
        ObservationReadStreamFailure::Midstream,
        ObservationReadStreamFailure::Terminal,
    ] {
        let publication_launched = Arc::new(AtomicBool::new(false));
        let mut executor = recording_executor(6, None, None);
        executor.measurements = evidenced_measurements(&execution_plan);
        executor.observation_read_stream_failure = Some(failure);
        executor.publication_launched = Some(Arc::clone(&publication_launched));
        let registry = TestRegistry {
            id: registry(3),
            executors: BTreeMap::from([(implementation(6), executor)]),
        };

        let error = execute_plan(&problem, &execution_plan, &current, &registry)
            .expect_err("a failed selected-observation stream cannot mint terminal evidence");
        let RunError::Execution { node, source } = error else {
            panic!("unexpected stream-failure result: {error}");
        };
        assert_eq!(node, WorkNodeId::new("transaction-read"));
        assert!(
            source
                .to_string()
                .contains("selected-observation sample stream failed")
        );
        let mut causes = Vec::new();
        let mut cause = source.source();
        while let Some(error) = cause {
            causes.push(error.to_string());
            cause = error.source();
        }
        assert!(
            causes.iter().any(|cause| cause.contains("synthetic")),
            "original stream failure remains in the source chain: {causes:?}"
        );
        assert!(!publication_launched.load(Ordering::SeqCst));
    }
}

#[test]
fn observation_read_owner_rejects_mismatched_source_coverage() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(evidenced_product_physical_work(
                problem,
                bindings.product_generation(),
                6,
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    executor.mismatch_observation_read_coverage = true;
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    let error = execute_plan(&problem, &execution_plan, &current, &registry)
        .expect_err("owner authority must reject a different runtime source identity");

    assert!(matches!(
        error,
        RunError::Execution { node, source }
            if node == WorkNodeId::new("transaction-read")
                && source.to_string()
                    == "runtime ObservationRead report does not match its bound read set"
    ));
}

#[test]
fn observation_read_owner_rejects_mismatched_metadata_generation() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(evidenced_product_physical_work(
                problem,
                bindings.product_generation(),
                6,
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    executor.mismatch_observation_read_metadata = true;
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    let error = execute_plan(&problem, &execution_plan, &current, &registry)
        .expect_err("owner authority must reject a different metadata generation");

    assert!(matches!(
        error,
        RunError::Execution { node, source }
            if node == WorkNodeId::new("transaction-read")
                && source.to_string()
                    == "runtime ObservationRead report does not match its bound read set"
    ));
}

#[test]
fn observation_read_owner_rejects_inexact_source_reports() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(evidenced_product_physical_work(
                problem,
                bindings.product_generation(),
                6,
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    for mutation in [
        ObservationReadReportMutation::Selection,
        ObservationReadReportMutation::SelectedColumns,
        ObservationReadReportMutation::ConsistencyToken,
    ] {
        let mut executor = recording_executor(6, None, None);
        executor.measurements = evidenced_measurements(&execution_plan);
        executor.observation_read_report_mutation = Some(mutation);
        let registry = TestRegistry {
            id: registry(3),
            executors: BTreeMap::from([(implementation(6), executor)]),
        };

        let error = execute_plan(&problem, &execution_plan, &current, &registry)
            .expect_err("owner authority must reject every inexact source report");

        assert!(matches!(
            error,
            RunError::Execution { node, source }
                if node == WorkNodeId::new("transaction-read")
                    && source.to_string()
                        == "runtime ObservationRead report does not match its bound read set"
        ));
    }
}

#[test]
fn observation_read_completion_rejects_cached_replay_across_runs() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(evidenced_product_physical_work(
                problem,
                bindings.product_generation(),
                6,
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let cache = Arc::new(Mutex::new(None));
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    executor.observation_read_completion_cache = Some(Arc::clone(&cache));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    execute_plan(&problem, &execution_plan, &current, &registry)
        .expect("first run mints current observation-read completion");
    let error = execute_plan(&problem, &execution_plan, &current, &registry)
        .expect_err("a later run must reject cached observation-read completion");

    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::MismatchedObservationReadCompletion { node })
            if node == WorkNodeId::new("transaction-read")
    ));
}

#[test]
fn observation_read_completion_is_durable_exact_and_tamper_evident() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(evidenced_product_physical_work(
                problem,
                bindings.product_generation(),
                6,
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([141; 32]),
        BuildIdentity::from_sha256([142; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let expected_samples = actual_observation_read_samples(1);
    let (expected_generation, expected_count) = problem
        .inspect_selected_observation(expected_samples)
        .expect("expected selected-observation generation");
    let node = WorkNodeId::new("transaction-read");
    let reopened = receipts.open(provenance.attempt_id()).expect("receipt");
    assert_eq!(reopened.schema_version(), 4);
    assert_eq!(reopened.compiled_problem_evidence().schema_version(), 6);
    let evidence = reopened
        .observation_read_completion(&node)
        .expect("durable ObservationRead completion");
    assert_eq!(
        evidence.observation_snapshot_id(),
        problem
            .observation_transaction()
            .observation_snapshot_id()
            .as_bytes()
    );
    assert_eq!(
        evidence.commitment_id(),
        problem.selected_observation().commitment_id().as_bytes()
    );
    assert_eq!(evidence.generation_id(), expected_generation.as_bytes());
    assert_eq!(evidence.sample_count(), expected_count);
    assert_eq!(evidence.terminal_owner(), &node);
    assert_eq!(evidence.source_count(), 1);

    let receipt_path = std::fs::read_dir(directory.path())
        .expect("receipt directory listing")
        .next()
        .expect("receipt file")
        .expect("receipt entry")
        .path();
    let original = std::fs::read(&receipt_path).expect("serialized receipt");
    let mut document: serde_json::Value = serde_json::from_slice(&original).expect("receipt JSON");
    let read_node = document["receipt"]["plan"]["nodes"]
        .as_array_mut()
        .expect("receipt nodes")
        .iter_mut()
        .find(|node| node["node_id"] == "transaction-read")
        .expect("ObservationRead node");
    read_node["observation_read_completion"]["generation_identity"] =
        serde_json::Value::String("00".repeat(32));
    let payload = serde_json::to_vec(&document["receipt"]).expect("tampered receipt body");
    document["payload_sha256"] = serde_json::Value::String(
        Sha256::digest(payload)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    );
    std::fs::write(
        &receipt_path,
        serde_json::to_vec_pretty(&document).expect("tampered receipt JSON"),
    )
    .expect("tampered receipt");
    assert!(matches!(
        receipts.open(provenance.attempt_id()),
        Err(casa_imaging_runtime::ReceiptError::IntegrityMismatch)
    ));
    std::fs::write(receipt_path, original).expect("restore receipt fixture");
}

#[test]
fn run_rejects_a_registry_that_cannot_resolve_the_bound_implementation() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let registry = test_registry(3, 7, None);
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
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let mut registry = test_registry(3, 6, None);
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
    assert_eq!(ExecutionPlanId::SCHEMA_VERSION, 7);
    let problem = compile(request(1)).expect("logical compilation");
    let expected_problem_id = problem.problem_id();
    let bindings = planning_bindings(
        &problem,
        registry(3),
        ResourcePolicy::Balanced,
        cost_model(4),
    );
    let execution_plan = plan(&problem, bindings.clone(), |problem, bindings| {
        assert_eq!(problem.problem_id(), expected_problem_id);
        assert_eq!(bindings.resource_policy(), &ResourcePolicy::Balanced);
        Ok::<_, ()>(exact_product_physical_work(
            problem,
            bindings.product_generation(),
            physical_work(6),
        ))
    })
    .expect("physical planning");

    assert_eq!(execution_plan.problem_id(), problem.problem_id());
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
        execution_plan.observation_transaction().transaction_id(),
        problem.observation_transaction().transaction_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().physical_work_id(),
        execution_plan.physical_work_id()
    );

    let repeated = plan(&problem, bindings, |problem, bindings| {
        Ok::<_, ()>(exact_product_physical_work(
            problem,
            bindings.product_generation(),
            physical_work(6),
        ))
    })
    .expect("repeat physical planning");
    assert_eq!(execution_plan.plan_id(), repeated.plan_id());
    assert_eq!(
        execution_plan.plan_id().as_bytes(),
        [
            50, 82, 44, 253, 147, 108, 140, 234, 107, 182, 127, 134, 177, 23, 66, 27, 6, 38, 149,
            24, 253, 180, 183, 8, 196, 76, 220, 189, 186, 72, 78, 163,
        ]
    );
}

#[test]
fn transaction_seal_rejects_omitted_required_product_staging() {
    let problem = compile(request_with_geometry_and_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual],
    ))
    .expect("two-product logical compilation");
    let generation = product_generation(&problem);
    let product_completion = BTreeSet::from([WorkDependency::Fence(FenceId::new(
        WorkNodeId::new("model-data-writer"),
        FenceKind::Io,
    ))]);
    plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            let exact = evidenced_product_physical_work(problem, bindings.product_generation(), 6);
            Ok::<_, io::Error>(physical_work_with_required_product_staging(
                &exact,
                BTreeMap::from([
                    (ProductKind::Psf, product_completion.clone()),
                    (ProductKind::Residual, product_completion.clone()),
                ]),
            ))
        },
    )
    .expect("canonical complete two-product transaction seal");

    let result = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| {
            let exact = evidenced_product_physical_work(&problem, &generation, 6);
            Ok::<_, io::Error>(physical_work_with_required_product_staging(
                &exact,
                BTreeMap::from([(ProductKind::Psf, product_completion.clone())]),
            ))
        },
    );

    let error = result.expect_err("one omitted product must fail the exact plan seal");
    assert_eq!(
        error.to_string(),
        "invalid observation transaction plan: required product staging keys {Psf} do not match compiled product requirements {Psf, Residual}"
    );
}

#[test]
fn transaction_seal_blocks_unbound_transaction_work() {
    let problem = compile(request(1)).expect("logical compilation");
    let result = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| {
            Ok::<_, io::Error>(physical_work_with_product_staging(
                6,
                Some(BTreeMap::from([(
                    ProductKind::Psf,
                    BTreeSet::from([WorkDependency::Work(WorkNodeId::new("execute"))]),
                )])),
            ))
        },
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
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current =
        |policy, cost_model_id| RunBindings::new(problem.inputs().clone(), policy, cost_model_id);
    let reject = |bindings, registry| execute_plan(&problem, &execution_plan, &bindings, registry);
    let wrong_registry = test_registry(9, 6, None);
    let correct_registry = test_registry(3, 6, None);

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
    let bindings = planning_bindings(
        &problem,
        registry(3),
        ResourcePolicy::Balanced,
        cost_model(4),
    );
    let execution_plan = plan(&problem, bindings.clone(), |problem, bindings| {
        Ok::<_, ()>(exact_product_physical_work(
            problem,
            bindings.product_generation(),
            physical_work(6),
        ))
    })
    .expect("physical planning");
    let registry = test_registry(3, 6, None);
    let stale_inputs = [
        (
            problem_inputs(9, Vec::new(), ModelStateIdentity::Empty),
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
            problem_inputs(1, Vec::new(), ModelStateIdentity::Seed(identity(9))),
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
    let bindings = planning_bindings(
        &problem,
        registry(3),
        ResourcePolicy::Balanced,
        cost_model(4),
    );
    let execution_plan = plan(&problem, bindings.clone(), |problem, bindings| {
        Ok::<_, ()>(exact_product_physical_work(
            problem,
            bindings.product_generation(),
            physical_work(6),
        ))
    })
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);

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
fn run_collects_work_completion_only_after_every_declared_fence_settles() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);

    assert_eq!(
        execute_plan(&problem, &execution_plan, &current, &registry)
            .expect("terminal completion run"),
        ExecutionOutcome::Succeeded
    );

    let events = registry.executors[&implementation(6)]
        .events
        .lock()
        .expect("recorded adapter events");
    let read = WorkNodeId::new("read");
    let execute = WorkNodeId::new("execute");
    let position = |expected: &AdapterEvent| {
        events
            .iter()
            .position(|event| event == expected)
            .expect("expected adapter event")
    };
    let read_started = position(&AdapterEvent::Execute(read.clone()));
    let read_fence = position(&AdapterEvent::Fence(FenceId::new(
        read.clone(),
        FenceKind::Io,
    )));
    let read_completed = position(&AdapterEvent::Complete(read));
    let dependent_started = position(&AdapterEvent::Execute(execute));
    let publish = execution_plan
        .product_publication()
        .publication_node()
        .clone();
    let publication_io_fence = position(&AdapterEvent::Fence(FenceId::new(
        publish.clone(),
        FenceKind::Io,
    )));
    let publication_fence = position(&AdapterEvent::Fence(FenceId::new(
        publish.clone(),
        FenceKind::Publication,
    )));
    let publication_completed = position(&AdapterEvent::Complete(publish));

    assert!(read_started < read_fence);
    assert!(read_fence < read_completed);
    assert!(read_completed < dependent_started);
    assert!(publication_io_fence < publication_completed);
    assert!(publication_fence < publication_completed);

    let executed = events
        .iter()
        .filter_map(|event| match event {
            AdapterEvent::Execute(node) => Some(node),
            AdapterEvent::Fence(_) | AdapterEvent::Complete(_) => None,
        })
        .collect::<Vec<_>>();
    let completed = events
        .iter()
        .filter_map(|event| match event {
            AdapterEvent::Complete(node) => Some(node),
            AdapterEvent::Execute(_) | AdapterEvent::Fence(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), executed.len());
    for node in executed {
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    matches!(event, AdapterEvent::Complete(completed_node) if completed_node == node)
                })
                .count(),
            1,
            "successful node {node:?} must complete exactly once"
        );
    }
}

#[test]
fn run_preserves_the_selected_executors_error_chain() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, Some("selected executor failed"));

    let error =
        execute_plan(&problem, &execution_plan, &current, &registry).expect_err("executor failure");

    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("selected executor failed")
    );
    let events = registry.executors[&implementation(6)]
        .events
        .lock()
        .expect("recorded adapter events");
    assert!(events.contains(&AdapterEvent::Execute(WorkNodeId::new("read"))));
    assert!(!events.contains(&AdapterEvent::Complete(WorkNodeId::new("read"))));
}

#[test]
fn rejected_post_launch_adaptation_drains_fences_before_returning() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let mut controller = RejectAfterLaunch::default();
    let _guard = run_lock().lock().expect("runtime test lock");

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
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                adaptive_physical_work(6),
            ))
        },
    )
    .expect("adaptive physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let mut controller = AdaptAtMajorBoundary::default();
    let _guard = run_lock().lock().expect("runtime test lock");

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
    assert!(
        observed
            .iter()
            .all(|knobs| knobs.cache_retention_bytes == 2)
    );
}

#[test]
fn run_cancellation_at_the_pre_read_cut_releases_authority_capacity() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
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
        executors: BTreeMap::from([(
            implementation(6),
            publication_recording_executor(
                6,
                &execution_plan,
                publication_launched,
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let mut cancelling = CancelAfterLaunch::default();
    let _guard = run_lock().lock().expect("runtime test lock");

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
        let generation = product_generation(&problem);
        let execution_plan = plan(
            &problem,
            planning_bindings(
                &problem,
                registry(3),
                ResourcePolicy::Balanced,
                cost_model(4),
            ),
            |_, _| Ok::<_, ()>(evidenced_product_physical_work(&problem, &generation, 6)),
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
            executors: BTreeMap::from([(
                implementation(6),
                publication_recording_executor(
                    6,
                    &execution_plan,
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
        let _guard = run_lock().lock().expect("runtime test lock");

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
    let generation = product_generation(&problem);
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| Ok::<_, ()>(evidenced_product_physical_work(&problem, &generation, 6)),
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
        executors: BTreeMap::from([(
            implementation(6),
            publication_recording_executor(
                6,
                &execution_plan,
                Arc::clone(&publication_launched),
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let mut controller = AdaptAtPublicationLaunch {
        publication_launched,
        requested: false,
    };
    let _guard = run_lock().lock().expect("runtime test lock");

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
    let generation = product_generation(&problem);
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| Ok::<_, ()>(evidenced_product_physical_work(&problem, &generation, 6)),
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
    let mut executor = publication_recording_executor(
        6,
        &execution_plan,
        publication_launched,
        Arc::clone(&visible_generation),
    );
    executor.visibility_during_fence_settlement = Some(Arc::clone(&visible_during_settlement));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

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
fn earlier_acquired_publication_buffer_is_held_through_publish_and_then_released() {
    let problem = compile(request(1)).expect("logical compilation");
    let generation = product_generation(&problem);
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| {
            Ok::<_, ()>(with_early_retained_publication_allocation(
                evidenced_product_physical_work(&problem, &generation, 6),
            ))
        },
    )
    .expect("earlier-acquired publication buffer is valid physical work");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let held_during_publish = Arc::new(AtomicBool::new(false));
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    executor.publication_buffer_held = Some(Arc::clone(&held_during_publish));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let _guard = run_lock().lock().expect("runtime test lock");

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
fn transaction_failures_leave_the_old_generation_visible() {
    for (label, failure_node, fence_failure_event, publication_failure) in [
        ("input mutation", Some("transaction-check"), None, None),
        ("numerical reconciliation", Some("execute"), None, None),
        ("product output", Some("model-data-writer"), None, None),
        (
            "atomic commit publication fence",
            None,
            Some(("publish", FenceKind::Publication)),
            None,
        ),
        (
            "atomic commit I/O fence",
            None,
            Some(("publish", FenceKind::Io)),
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
        let generation = product_generation(&problem);
        let execution_plan = plan(
            &problem,
            planning_bindings(
                &problem,
                registry(3),
                ResourcePolicy::Balanced,
                cost_model(4),
            ),
            |_, _| Ok::<_, ()>(evidenced_product_physical_work(&problem, &generation, 6)),
        )
        .expect("physical planning");
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let visible_generation = Arc::new(AtomicUsize::new(0));
        let mut executor = failing_transaction_executor(
            6,
            Arc::clone(&visible_generation),
            failure_node,
            fence_failure_event,
            publication_failure,
        );
        executor.measurements = evidenced_measurements(&execution_plan);
        let registry = TestRegistry {
            id: registry(3),
            executors: BTreeMap::from([(implementation(6), executor)]),
        };
        let mut completion = RunToCompletion;
        let _guard = run_lock().lock().expect("runtime test lock");

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
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
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
    let _guard = run_lock().lock().expect("runtime test lock");

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
    let generation = product_generation(&problem);
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| Ok::<_, ()>(evidenced_product_physical_work(&problem, &generation, 6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let mut executor =
        failing_transaction_executor(6, Arc::clone(&visible_generation), None, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    let admission_registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut completion = RunToCompletion;
    authority()
        .update_external_pressure(runtime_inventory(0).pressure)
        .expect("install zero-lock external pressure");

    let result = run(
        &problem,
        &execution_plan,
        &current,
        &admission_registry,
        authority(),
        &mut completion,
    );
    authority()
        .update_external_pressure(runtime_inventory(4).pressure)
        .expect("restore external pressure");
    let error = result.expect_err("resource admission must fail before transaction work");

    assert!(matches!(
        error,
        RunError::Scheduler(ExecutionError::Resource(ResourceError::Infeasible {
            ref resource,
            required: 1,
            available: 0,
        })) if resource == "locks"
    ));
    assert_eq!(visible_generation.load(Ordering::SeqCst), 0);
    assert_eq!(
        admission_registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0,
        "failed admission cannot launch mutation or publication work"
    );
}

#[test]
fn release_failures_drain_independent_fences_and_quarantine_only_failed_slots() {
    let problem = compile(request(1)).expect("logical compilation");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let _guard = run_lock().lock().expect("runtime test lock");

    for fail_at_fence in [false, true] {
        let execution_plan = plan(
            &problem,
            planning_bindings(
                &problem,
                registry(3),
                ResourcePolicy::Balanced,
                cost_model(4),
            ),
            |problem, bindings| {
                Ok::<_, ()>(exact_product_physical_work(
                    problem,
                    bindings.product_generation(),
                    release_failure_physical_work(6, 8, fail_at_fence),
                ))
            },
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
            planning_bindings(
                &problem,
                registry(3),
                ResourcePolicy::Balanced,
                cost_model(4),
            ),
            |problem, bindings| {
                Ok::<_, ()>(exact_product_physical_work(
                    problem,
                    bindings.product_generation(),
                    physical_work(6),
                ))
            },
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
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([9; 32]),
        BuildIdentity::from_sha256([10; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    let outcome = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen durable receipt");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert_eq!(receipt.schema_version(), 3);
    assert_eq!(receipt.status(), ReceiptStatus::Completed);
    assert_eq!(receipt.plan_identity(), execution_plan.plan_id().as_bytes());
    assert_eq!(receipt.problem_identity(), problem.problem_id().as_bytes());
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
        positive_zero.weighting().generation_id(),
        negative_zero.weighting().generation_id()
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
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([83; 32]),
        BuildIdentity::from_sha256([84; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let reopened = receipts.open(provenance.attempt_id()).expect("receipt");
    let projected = reopened.compiled_problem_evidence();
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

    assert_eq!(projected.schema_version(), 5);
    assert_eq!(projected, &CompiledProblemEvidence::project(&problem));
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
        projected.field("weighting.generation.identity"),
        Some(
            problem
                .normal_equation()
                .weighting()
                .generation_id()
                .to_string()
                .as_str()
        )
    );
    assert_eq!(
        projected.field("weighting.sources.0.flag_policy"),
        Some("flag_or_flag_row")
    );
    assert_eq!(
        projected.field("weighting.generation.snapshot_identity"),
        Some(problem.inputs().observation().to_string().as_str())
    );
    assert_eq!(
        projected.field("weighting.sources.0.input_weight_column"),
        Some("weight")
    );
    assert_eq!(projected.field("products.requested.0"), Some("psf"));
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
        projected.field("observation.sources.0.selection.data_descriptions.count"),
        Some("1")
    );
    assert_eq!(
        projected.field("observation.sources.0.selection.data_descriptions.0.data_description_id"),
        Some("0")
    );
    assert_eq!(
        projected.field("observation.sources.0.selection.data_descriptions.0.spectral_window_id"),
        Some("0")
    );
    assert_eq!(
        projected.field("observation.sources.0.selection.data_descriptions.0.polarization_id"),
        Some("0")
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

    let receipt_path = std::fs::read_dir(directory.path())
        .expect("receipt directory listing")
        .next()
        .expect("receipt file")
        .expect("receipt entry")
        .path();
    let original = std::fs::read(&receipt_path).expect("serialized receipt");
    let mut document: serde_json::Value = serde_json::from_slice(&original).expect("receipt JSON");
    document["receipt"]["problem"]["effective"]["fields"]
        .as_object_mut()
        .expect("compiled-problem field map")
        .remove("observation.sources.0.selection.data_descriptions.0.polarization_id");
    let payload = serde_json::to_vec(&document["receipt"]).expect("tampered receipt body");
    let digest = Sha256::digest(payload);
    document["payload_sha256"] =
        serde_json::Value::String(digest.iter().map(|byte| format!("{byte:02x}")).collect());
    std::fs::write(
        &receipt_path,
        serde_json::to_vec_pretty(&document).expect("tampered receipt JSON"),
    )
    .expect("tampered receipt");
    assert!(matches!(
        receipts.open(provenance.attempt_id()),
        Err(casa_imaging_runtime::ReceiptError::IntegrityMismatch)
    ));
    std::fs::write(receipt_path, original).expect("restore receipt fixture");
}

#[test]
fn receipt_reopens_one_product_publication_layout() {
    let problem = compile(request(1)).expect("logical compilation");
    let generation = product_generation(&problem);
    let graph = problem.product_graph();
    let member = graph.publication().members()[0];
    let artifact = ArtifactIdentity::from_sha256(
        generation
            .artifact_id(member)
            .expect("physical product artifact")
            .as_bytes(),
    );
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(
            generation.clone(),
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| {
            Ok::<_, ()>(evidenced_product_physical_work_without_mapped_layout(
                &problem,
                &generation,
                6,
            ))
        },
    )
    .expect("physical planning");
    let expected_layout = execution_plan.publication_layouts().entries()[0].clone();
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([93; 32]),
        BuildIdentity::from_sha256([94; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let reopened = receipts.open(provenance.attempt_id()).expect("receipt");

    assert_eq!(reopened.schema_version(), 3);
    assert_eq!(reopened.compiled_problem_evidence().schema_version(), 5);
    assert_eq!(
        reopened
            .compiled_problem_evidence()
            .field("products.validity.primary_beam.cutoff"),
        Some("f32:3e4ccccd")
    );

    let graph_evidence = reopened.compiled_problem_evidence().product_graph();
    assert_eq!(graph_evidence.schema_version(), 3);
    assert_eq!(graph_evidence.graph_id(), graph.graph_id().as_bytes());
    assert_eq!(graph_evidence.nodes().len(), 1);
    assert_eq!(graph_evidence.nodes()[0].node_ordinal(), member.ordinal());
    assert_eq!(
        graph_evidence.nodes()[0].role(),
        ProductRole::Psf(ProductTerm::Single)
    );
    assert_eq!(
        graph_evidence.nodes()[0].schema(),
        ProductSchema::CasaPagedImageF32
    );
    assert_eq!(graph_evidence.publication_members(), &[member.ordinal()]);

    let product_evidence = reopened.product_execution_evidence();
    assert_eq!(product_evidence.generation_id(), generation.generation_id());
    assert_eq!(product_evidence.graph_id(), graph.graph_id().as_bytes());
    assert_eq!(
        product_evidence.source_bindings().len(),
        generation.source_bindings().len()
    );
    for (actual, expected) in product_evidence
        .source_bindings()
        .iter()
        .zip(generation.source_bindings())
    {
        assert_eq!(actual.source_ordinal(), expected.source_id().ordinal());
        assert_eq!(actual.generation_id(), expected.generation_id());
    }
    assert_eq!(
        product_evidence.publication_node(),
        execution_plan
            .product_publication()
            .publication_node()
            .clone()
    );
    let [member_evidence] = product_evidence.members() else {
        panic!("receipt must reopen one physical publication member");
    };
    assert_eq!(
        member_evidence.participant(),
        &ProductParticipantEvidence::Product(member.ordinal())
    );
    assert_eq!(member_evidence.planned_artifact(), artifact);
    let layout_evidence = member_evidence.layout();
    assert_eq!(layout_evidence.layout_id(), expected_layout.layout_id());
    assert_eq!(
        layout_evidence.staging().producer(),
        expected_layout.staging().producer().clone()
    );
    assert_eq!(
        layout_evidence.staging().terminal(),
        expected_layout.staging().terminal().clone()
    );
    assert_eq!(
        layout_evidence.staging().writer_buffer_kind(),
        IoBufferKind::TiledColumnWriter
    );
    assert_eq!(
        layout_evidence.staging().writer_allocation(),
        AllocationId::new("model-data-writer-buffer")
    );
    assert_eq!(layout_evidence.staging().mapped_page_cache(), None);
    assert_eq!(layout_evidence.staged_storage_bytes(), 2_048);
    assert_eq!(layout_evidence.final_storage_bytes(), 2_048);
    assert_eq!(layout_evidence.writer_buffer_bytes(), 16);
    assert_eq!(layout_evidence.mapped_page_cache_bytes(), 0);
}

#[test]
fn receipt_reopens_complete_product_graph_projection() {
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([95; 32]),
        BuildIdentity::from_sha256([96; 32]),
    );
    let (expected_graph_id, expected_sources, expected_nodes, expected_publication) = {
        let problem = compile(mtmfs_request(1)).expect("compile full MT-MFS product problem");
        let generation = product_generation(&problem);
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(
                generation.clone(),
                registry(3),
                ResourcePolicy::Balanced,
                cost_model(4),
            ),
            |_, _| {
                Ok::<_, ()>(exact_product_physical_work(
                    &problem,
                    &generation,
                    physical_work(6),
                ))
            },
        )
        .expect("physical planning");
        let publication = execution_plan
            .product_publication()
            .publication_node()
            .clone();
        let publication_bytes =
            u64::try_from(problem.product_graph().publication().members().len() * 2)
                .expect("publication buffer bytes");
        let mut executor = recording_executor(6, None, None);
        executor.measurements = BTreeMap::from([(
            publication,
            (
                vec![IoMeasurement::new(
                    IoBufferKind::Publication,
                    publication_bytes,
                    1,
                )],
                execution_plan
                    .product_publication()
                    .artifacts()
                    .iter()
                    .map(|artifact| {
                        ArtifactMeasurement::new(
                            artifact.artifact(),
                            Some(artifact.artifact()),
                            ArtifactDisposition::Published,
                            128,
                            None,
                        )
                    })
                    .collect(),
            ),
        )]);
        let registry = TestRegistry {
            id: registry(3),
            executors: BTreeMap::from([(implementation(6), executor)]),
        };
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let mut controller = RunToCompletion;
        let _guard = run_lock().lock().expect("runtime test lock");
        run_receipted(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut controller,
            receipts.bind(provenance),
        )
        .expect("receipted execution");
        let graph = problem.product_graph();
        (
            graph.graph_id().as_bytes(),
            graph.sources().to_vec(),
            graph.nodes().to_vec(),
            graph
                .publication()
                .members()
                .iter()
                .map(|member| member.ordinal())
                .collect::<Vec<_>>(),
        )
    };

    let reopened = receipts.open(provenance.attempt_id()).expect("receipt");
    let graph = reopened.compiled_problem_evidence().product_graph();
    assert_eq!(reopened.compiled_problem_evidence().schema_version(), 5);
    assert_eq!(graph.graph_id(), expected_graph_id);
    assert_eq!(graph.sources().len(), expected_sources.len());
    assert_eq!(graph.nodes().len(), expected_nodes.len());
    assert_eq!(graph.publication_members(), expected_publication);

    for (actual, expected) in graph.sources().iter().zip(&expected_sources) {
        assert_eq!(actual.source_ordinal(), expected.source_id().ordinal());
        assert_eq!(actual.role(), expected.role());
        assert_eq!(actual.domain(), expected.domain().clone());
        assert_eq!(actual.term(), expected.term());
    }
    for (actual, expected) in graph.nodes().iter().zip(&expected_nodes) {
        assert_eq!(actual.node_ordinal(), expected.node_id().ordinal());
        assert_eq!(actual.role(), expected.role());
        assert_eq!(actual.name(), expected.name());
        assert_eq!(actual.axes().kind(), expected.axes().kind());
        assert_eq!(
            actual.axes().geometry_identity(),
            expected.axes().geometry_id().as_bytes()
        );
        assert_eq!(actual.axes().domain(), expected.axes().domain().clone());
        assert_eq!(actual.axes().order(), *expected.axes().order().positions());
        assert_eq!(actual.axes().shape(), expected.axes().shape());
        assert_eq!(actual.axes().direction(), expected.axes().direction());
        assert_eq!(actual.axes().spectral(), expected.axes().spectral().clone());
        assert_eq!(actual.axes().polarization(), expected.axes().polarization());
        assert_eq!(actual.unit(), expected.unit());
        assert_eq!(actual.normalization(), expected.normalization());
        assert_eq!(actual.beam(), expected.beam());
        assert_eq!(actual.validity(), expected.validity());
        assert_eq!(actual.schema(), expected.schema());
        assert_eq!(
            actual.payload().element_representation(),
            expected.payload().element_representation()
        );
        assert_eq!(
            actual.payload().logical_elements(),
            expected.payload().logical_elements()
        );
        assert_eq!(
            actual.payload().logical_pixel_bytes(),
            expected.payload().logical_pixel_bytes()
        );
        assert_eq!(
            actual.payload().identity_metadata_bytes(),
            expected.payload().identity_metadata_bytes()
        );
        assert_eq!(
            actual.payload().identity_envelope_bytes(),
            expected.payload().identity_envelope_bytes()
        );
        assert_eq!(
            actual.source_dependencies(),
            expected
                .source_dependencies()
                .iter()
                .map(|source| source.ordinal())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            actual.dependencies(),
            expected
                .dependencies()
                .iter()
                .map(|node| node.ordinal())
                .collect::<Vec<_>>()
        );
    }

    let taylor_set = graph
        .nodes()
        .iter()
        .find(|node| node.role() == ProductRole::TaylorCoefficientSet)
        .expect("logical Taylor coefficient collection");
    let beam_metadata = graph
        .nodes()
        .iter()
        .find(|node| node.role() == ProductRole::BeamMetadata)
        .expect("embedded beam metadata");
    for logical in [taylor_set, beam_metadata] {
        assert_eq!(logical.axes().kind(), ProductAxisKind::Metadata);
        assert_eq!(
            logical.payload().element_representation(),
            ProductElementRepresentation::NotApplicable
        );
        assert_eq!(logical.payload().logical_elements(), 0);
        assert_eq!(logical.payload().logical_pixel_bytes(), 0);
        assert!(
            !graph
                .publication_members()
                .contains(&logical.node_ordinal())
        );
    }
    let restored = graph
        .nodes()
        .iter()
        .find(|node| node.role() == ProductRole::RestoredImage(ProductTerm::Taylor(0)))
        .expect("Taylor-zero restored image");
    assert_eq!(restored.axes().kind(), ProductAxisKind::SkyImage);
    assert_eq!(restored.axes().shape(), [512, 512, 1, 1]);
    assert_eq!(
        restored.payload().element_representation(),
        ProductElementRepresentation::Float32
    );
    assert_eq!(restored.payload().logical_elements(), 262_144);
    assert_eq!(restored.payload().logical_pixel_bytes(), 1_048_576);
    assert!(restored.payload().identity_metadata_bytes() > 0);
    assert!(
        graph
            .publication_members()
            .iter()
            .all(|member| { graph.nodes()[*member].schema() == ProductSchema::CasaPagedImageF32 })
    );
}

#[test]
fn receipt_reopens_mapped_publication_layout_projection() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                evidenced_physical_work(6),
            ))
        },
    )
    .expect("mapped product publication plan");
    let mut executor = recording_executor(6, None, None);
    executor.measurements = evidenced_measurements(&execution_plan);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([97; 32]),
        BuildIdentity::from_sha256([98; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");
    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted mapped publication");

    let reopened = receipts.open(provenance.attempt_id()).expect("receipt");
    let [member] = reopened.product_execution_evidence().members() else {
        panic!("receipt must reopen one product member");
    };
    let layout = member.layout();
    let staging = layout.staging();
    assert_eq!(staging.producer(), WorkNodeId::new("model-data-writer"));
    assert_eq!(
        staging.terminal(),
        WorkDependency::Fence(FenceId::new(
            WorkNodeId::new("model-data-writer"),
            FenceKind::Io,
        ))
    );
    assert_eq!(
        staging.writer_allocation(),
        AllocationId::new("model-data-writer-buffer")
    );
    assert_eq!(
        staging.writer_physical_slot(),
        PhysicalSlotId::new("model-data-writer-slot")
    );
    assert_eq!(
        staging.writer_lease_resource(),
        casa_imaging_runtime::LeaseResource::Memory {
            allocation_id: "model-data-writer-memory".to_string(),
        }
    );
    let mapped = staging
        .mapped_page_cache()
        .expect("mapped/page-cache staging evidence");
    assert_eq!(
        mapped.producer(),
        WorkNodeId::new("model-data-mapped-pages")
    );
    assert_eq!(
        mapped.terminal(),
        WorkDependency::Work(WorkNodeId::new("model-data-mapped-release"))
    );
    assert_eq!(
        mapped.allocation(),
        AllocationId::new("model-data-mapped-pages")
    );
    assert_eq!(
        mapped.physical_slot(),
        PhysicalSlotId::new("model-data-mapped-slot")
    );
    assert_eq!(
        mapped.lease_resource(),
        casa_imaging_runtime::LeaseResource::Memory {
            allocation_id: "model-data-mapped-memory".to_string(),
        }
    );
    assert_eq!(layout.staged_storage_bytes(), 128);
    assert_eq!(layout.final_storage_bytes(), 128);
    assert_eq!(layout.writer_buffer_bytes(), 2);
    assert_eq!(layout.mapped_page_cache_bytes(), 2);

    let receipt_path = std::fs::read_dir(directory.path())
        .expect("receipt directory listing")
        .next()
        .expect("receipt file")
        .expect("receipt entry")
        .path();
    let original = std::fs::read(&receipt_path).expect("serialized receipt");
    for (field, replacement) in [
        (
            "allocation",
            serde_json::json!("tampered-mapped-allocation"),
        ),
        ("terminal", serde_json::json!("work:missing-mapped-release")),
        (
            "lease_resource",
            serde_json::json!("memory:tampered-mapped-pages"),
        ),
    ] {
        let mut document: serde_json::Value =
            serde_json::from_slice(&original).expect("receipt JSON");
        document["receipt"]["plan"]["product_execution"]["members"][0]["layout"]["staging"]["mapped_page_cache"]
            [field] = replacement;
        std::fs::write(
            &receipt_path,
            serde_json::to_vec_pretty(&document).expect("tampered receipt JSON"),
        )
        .expect("tampered receipt");
        assert!(matches!(
            receipts.open(provenance.attempt_id()),
            Err(casa_imaging_runtime::ReceiptError::IntegrityMismatch)
        ));
    }
    std::fs::write(receipt_path, original).expect("restore receipt fixture");
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
        planning_bindings(&problem, registry(3), policy.clone(), cost_model(4)),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                auditable_physical_work(6),
            ))
        },
    )
    .expect("auditable physical planning");
    let current = RunBindings::new(problem.inputs().clone(), &policy, cost_model(4));
    let cache_artifact = ArtifactIdentity::from_sha256([51; 32]);
    let mut executor = recording_executor(6, None, None);
    executor.measurements = BTreeMap::from([(
        WorkNodeId::new("first-major-work"),
        (
            Vec::new(),
            vec![ArtifactMeasurement::new(
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
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([61; 32]),
        BuildIdentity::from_sha256([62; 32]),
    );
    let mut controller = AdaptAtMajorBoundary::default();
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let dag = execution_plan.execution_dag();
    let product_artifact = execution_plan.product_publication().artifacts()[0].artifact();
    let adaptation_id = AdaptationId::new("larger-batch");
    let adaptation = receipt
        .adaptation_projection(&adaptation_id)
        .expect("adaptation projection");

    assert_eq!(receipt.schema_version(), 3);
    assert_eq!(receipt.compiled_problem_evidence().schema_version(), 5);
    assert_eq!(
        receipt.product_execution_evidence().generation_id(),
        execution_plan.product_generation_id()
    );
    assert_eq!(
        receipt.product_execution_evidence().members().len(),
        execution_plan.product_publication().artifacts().len()
    );
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
        BTreeSet::from([cache_artifact, product_artifact])
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
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([11; 32]),
        BuildIdentity::from_sha256([12; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let read = WorkNodeId::new("read");
    let io_fence = FenceId::new(read.clone(), FenceKind::Io);

    assert_eq!(receipt.predicted_elapsed_nanos(), 204);
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
    let generation = product_generation(&problem);
    let output = publication_artifact(&problem, &generation);
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(
            generation.clone(),
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |_, _| Ok::<_, ()>(evidenced_product_physical_work(&problem, &generation, 6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let input = ArtifactIdentity::from_sha256([31; 32]);
    let cache = ArtifactIdentity::from_sha256([32; 32]);
    let input_path = RedactedPath::from_path("/Users/private/secret-source.ms");
    let output_path = RedactedPath::from_path("/Volumes/private/secret-image.table");
    let mut executor = recording_executor(6, None, None);
    executor.measurements = BTreeMap::from([
        (
            WorkNodeId::new("read"),
            (
                vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
                vec![
                    ArtifactMeasurement::new(
                        input,
                        Some(input),
                        ArtifactDisposition::Loaded,
                        4_096,
                        Some(input_path),
                    ),
                    ArtifactMeasurement::new(
                        cache,
                        Some(ArtifactIdentity::from_sha256([35; 32])),
                        ArtifactDisposition::RejectedStale,
                        1_024,
                        None,
                    ),
                ],
            ),
        ),
        (
            WorkNodeId::new("publish"),
            (
                vec![IoMeasurement::new(IoBufferKind::Publication, 2_048, 1)],
                vec![ArtifactMeasurement::new(
                    output,
                    Some(ArtifactIdentity::from_sha256([36; 32])),
                    ArtifactDisposition::Published,
                    2_048,
                    Some(output_path),
                )],
            ),
        ),
    ]);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([37; 32]),
        BuildIdentity::from_sha256([38; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    assert_eq!(receipt.schema_version(), 3);
    assert_eq!(
        receipt.product_execution_evidence().generation_id(),
        generation.generation_id()
    );
    let read = WorkNodeId::new("read");
    let publish = WorkNodeId::new("publish");

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
        Some(WorkNodeId::new("publish"))
    );
    assert_eq!(receipt.artifact_actual_bytes(output), Some(2_048));
    assert_eq!(
        receipt.artifact_disposition(cache),
        Some(ArtifactDisposition::RejectedStale)
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
        std::fs::read_dir(directory.path())
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
    let generation = product_generation(&problem);
    let output = publication_artifact(&problem, &generation);
    let execution_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                evidenced_physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, Some("publication fence failed"));
    executor.fail_only_fence = Some(FenceKind::Publication);
    executor.measurements = evidenced_measurements(&execution_plan);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([87; 32]),
        BuildIdentity::from_sha256([88; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    let error = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect_err("publication fence failure must fail the run");
    assert!(matches!(
        error,
        RunError::Execution { ref node, .. } if node == &WorkNodeId::new("publish")
    ));

    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("failed receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(receipt.artifact_disposition(output), None);
    assert_eq!(
        receipt.artifact_observed_identity(output),
        Some(output.as_bytes())
    );
    assert_eq!(receipt.artifact_actual_bytes(output), Some(2_048));
    assert_eq!(
        receipt.fence_status(&FenceId::new(
            WorkNodeId::new("publish"),
            FenceKind::Publication,
        )),
        Some(ReceiptStatus::Failed)
    );
    let events = registry.executors[&implementation(6)]
        .events
        .lock()
        .expect("recorded adapter events");
    assert!(events.contains(&AdapterEvent::Fence(FenceId::new(
        WorkNodeId::new("publish"),
        FenceKind::Publication,
    ))));
    assert!(!events.contains(&AdapterEvent::Complete(WorkNodeId::new("publish",))));
}

#[test]
fn receipts_preserve_typed_terminal_outcomes_and_every_node_state() {
    let problem = compile(request(1)).expect("logical compilation");
    let balanced_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let store = |root: &std::path::Path| {
        ExecutionReceiptStore::new(
            root,
            ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
        )
        .expect("receipt store")
    };
    let _guard = run_lock().lock().expect("runtime test lock");

    let failed_directory = tempfile::tempdir().expect("failed receipt directory");
    let failed_receipts = store(failed_directory.path());
    let failed_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([41; 32]),
        BuildIdentity::from_sha256([42; 32]),
    );
    let failed_registry = test_registry(3, 6, Some("adapter failed"));
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &failed_registry,
            authority(),
            &mut completion,
            failed_receipts.bind(failed_provenance),
        ),
        Err(RunError::Execution { .. })
    ));
    let failed = failed_receipts
        .open(failed_provenance.attempt_id())
        .expect("failed receipt");
    assert_eq!(failed.status(), ReceiptStatus::Failed);
    assert_eq!(failed.failure_kind(), Some(ReceiptFailureKind::Adapter));
    assert_eq!(failed.failure_node(), Some(WorkNodeId::new("read")));
    assert_eq!(
        failed.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Failed)
    );
    assert_eq!(
        failed.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Cancelled)
    );

    let cancelled_directory = tempfile::tempdir().expect("cancelled receipt directory");
    let cancelled_receipts = store(cancelled_directory.path());
    let cancelled_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([43; 32]),
        BuildIdentity::from_sha256([44; 32]),
    );
    let successful_registry = test_registry(3, 6, None);
    let mut cancellation = CancelAfterLaunch::default();
    assert_eq!(
        run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &successful_registry,
            authority(),
            &mut cancellation,
            cancelled_receipts.bind(cancelled_provenance),
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
        Some(ReceiptStatus::Completed)
    );
    assert_eq!(
        cancelled.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Cancelled)
    );

    let mutation_directory = tempfile::tempdir().expect("mutation receipt directory");
    let mutation_receipts = store(mutation_directory.path());
    let mutation_provenance = ExecutionProvenance::new(
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
            mutation_receipts.bind(mutation_provenance),
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

    let constrained_policy = ResourcePolicy::Explicit(ResourceOverride {
        workers: Some(0),
        ..ResourceOverride::default()
    });
    let infeasible_plan = plan(
        &problem,
        planning_bindings(
            &problem,
            registry(3),
            constrained_policy.clone(),
            cost_model(4),
        ),
        |problem, bindings| {
            Ok::<_, ()>(exact_product_physical_work(
                problem,
                bindings.product_generation(),
                physical_work(6),
            ))
        },
    )
    .expect("infeasible physical plan remains recordable");
    let infeasible_current =
        RunBindings::new(problem.inputs().clone(), &constrained_policy, cost_model(4));
    let infeasible_directory = tempfile::tempdir().expect("infeasible receipt directory");
    let infeasible_receipts = store(infeasible_directory.path());
    let infeasible_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([47; 32]),
        BuildIdentity::from_sha256([48; 32]),
    );
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &infeasible_plan,
            &infeasible_current,
            &successful_registry,
            authority(),
            &mut completion,
            infeasible_receipts.bind(infeasible_provenance),
        ),
        Err(RunError::Scheduler(
            casa_imaging_runtime::ExecutionError::Resource(_)
        ))
    ));
    let infeasible = infeasible_receipts
        .open(infeasible_provenance.attempt_id())
        .expect("infeasible receipt");
    assert_eq!(infeasible.status(), ReceiptStatus::Infeasible);
    assert_eq!(
        infeasible.failure_kind(),
        Some(ReceiptFailureKind::ResourceInfeasible)
    );
    assert_eq!(
        infeasible.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::NotStarted)
    );

    let aborted_directory = tempfile::tempdir().expect("aborted receipt directory");
    let aborted_receipts = store(aborted_directory.path());
    let aborted_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([49; 32]),
        BuildIdentity::from_sha256([50; 32]),
    );
    let mut interrupted_executor = recording_executor(6, None, None);
    interrupted_executor.panic_on_execute = true;
    let interrupted_registry = TestRegistry {
        id: registry(3),
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
            aborted_receipts.bind(aborted_provenance),
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
    assert_eq!(aborted.failure_node(), Some(WorkNodeId::new("read")));
    assert_eq!(
        aborted.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Aborted)
    );
    assert_eq!(
        aborted.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::NotStarted)
    );
}
