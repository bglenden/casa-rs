// SPDX-License-Identifier: LGPL-3.0-or-later

//! End-to-end serial/multi-worker evidence for the bounded complete-data MFS path.

use std::{collections::BTreeMap, convert::Infallible, fs, io, path::PathBuf, sync::OnceLock};

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, CorrelationProduct, CorrelationSelection,
    CorrelationType, DataDescriptionSelection, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy,
    FrequencyFrame, GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, ImagingRequest, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, ModelBounds, ModelColumnWrite, ModelInnerProduct,
    ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity, NumericPrecision,
    NumericalStage, NumericsContract, ObservationPointingLaw, ObservationSelection,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection, ScientificContract,
    SelectedMainRow, SelectedRows, SkyDirection, SpectralContract, SpectralCoordinateSpec,
    SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs,
    SpectralWindowSelection, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    TimeSelection, UvSelection, UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct,
    WeightColumn, WeightDensityScope, WeightingContract, WeightingScheme, compile,
    compile_observation,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, WeightingExecutionLimits,
    runtime_adapter::gridded_normal_route_window_capacity_bytes,
};
use casa_imaging_runtime::{
    AttemptBoundObservationCompletion, BuildIdentity, CapacityDomainId, CapacityViewId,
    CompleteDataStreamEvidence, CpuClassCapacity, ExecutionAttemptId, ExecutionProvenance,
    ExecutionReceiptStore, ExternalPressure, FenceKind, FrozenWeightingReservation,
    GriddedNormalReplayStorage, HostInventory, ImplementationContractMetadata,
    ImplementationRegistry, ImplementationRegistryId, MemoryCapacityDomain, MemoryCapacityKind,
    MemoryView, MemoryViewKind, ObservationReadCompletionContext, PlannerCostModelProfileBootstrap,
    PlannerCostModelProfileId, PlanningBindings, QueueResource, QueueResourceId, RateResource,
    RateResourceId, RateUnit, ReceiptRetention, ResourceAuthority, ResourceOverride,
    ResourcePolicy, ResourceTopology, RunBindings, RunToCompletion, SpectralCycleExecutionPolicy,
    SpectralCycleExecutor, SpectralCyclePassInput, SpectralCyclePlan, SpectralCyclePlanParts,
    SpectralCycleRegistry, StorageDomain, StorageDomainId, StorageIoResourceBinding,
    WorkExecutionContext, WorkImplementation, WorkImplementationId, WorkMeasurements,
    plan as runtime_plan, run as runtime_run,
};
use casa_ms::{
    SelectedObservationContentBudget, SelectedObservationResolutionRequest,
    SyntheticAnalyticComponent, SyntheticAnalyticSpectrum, SyntheticObservationRequest,
    SyntheticPolarizationBasis, SyntheticPolarizationSetup, SyntheticSkyModel,
    SyntheticWorkerPolicy, generate_synthetic_observation_ms,
    initialize_measurement_set_owner_manifest, resolve_selected_observation,
    tutorial_vla_a_antennas,
};
use num_complex::Complex64;

const TEST_IMPLEMENTATION_BYTE: u8 = 81;
const HOST_MEMORY_BYTES: u64 = 16 << 20;
const STORAGE_BYTES: u64 = 16 << 20;
const IMAGE_PIXELS: usize = 16 * 16;

#[derive(Debug, PartialEq, Eq)]
struct StreamSummary {
    planned_workers: u64,
    actual_workers: u64,
    active_worker_slots: u64,
    partitions_executed: u64,
    commits_completed: u64,
    peak_partial_dynamic_capacity_bytes: u64,
    peak_worker_stack_capacity_bytes: u64,
    peak_kernel_window_capacity_bytes: u64,
    planned_gridded_route_capacity_bytes: u64,
    executed_work_identity_digest: [u8; 32],
    committed_work_identity_digest: [u8; 32],
    source_pass_count: u64,
    artifact_pass_count: u64,
    grid_resident_bytes: u64,
}

impl From<CompleteDataStreamEvidence> for StreamSummary {
    fn from(evidence: CompleteDataStreamEvidence) -> Self {
        Self {
            planned_workers: evidence.planned_workers(),
            actual_workers: evidence.actual_workers(),
            active_worker_slots: evidence.active_worker_slots(),
            partitions_executed: evidence.partitions_executed(),
            commits_completed: evidence.commits_completed(),
            peak_partial_dynamic_capacity_bytes: evidence.peak_partial_dynamic_capacity_bytes(),
            peak_worker_stack_capacity_bytes: evidence.peak_worker_stack_capacity_bytes(),
            peak_kernel_window_capacity_bytes: evidence.peak_kernel_window_capacity_bytes(),
            planned_gridded_route_capacity_bytes: evidence.planned_gridded_route_capacity_bytes(),
            executed_work_identity_digest: evidence.executed_work_identity_digest(),
            committed_work_identity_digest: evidence.committed_work_identity_digest(),
            source_pass_count: evidence.source_pass_count(),
            artifact_pass_count: evidence.artifact_pass_count(),
            grid_resident_bytes: evidence.grid_resident_bytes(),
        }
    }
}

#[derive(Debug)]
struct RunEvidence {
    dirty: Vec<Complex64>,
    psf: Vec<Complex64>,
    model: Vec<casa_imaging_model::ModelSample>,
    residual: Vec<Complex64>,
    sum_weights: Vec<f64>,
    initial_stream: StreamSummary,
    final_stream: StreamSummary,
}

#[test]
fn complete_data_mfs_products_and_identities_are_exact_for_one_two_and_four_workers() {
    let runs = [1, 2, 4].map(execute_complete_data_mfs);
    let serial = &runs[0];

    for (workers, run) in [(2, &runs[1]), (4, &runs[2])] {
        assert_eq!(serial.dirty, run.dirty, "{workers}-worker dirty changed");
        assert_eq!(serial.psf, run.psf, "{workers}-worker PSF changed");
        assert_eq!(
            serial.residual, run.residual,
            "{workers}-worker residual changed"
        );
        assert_eq!(serial.model, run.model, "{workers}-worker model changed");
        assert_eq!(
            serial.sum_weights, run.sum_weights,
            "{workers}-worker sum weight changed"
        );

        assert_worker_independent_stream(&serial.initial_stream, &run.initial_stream, "initial");
        assert_worker_independent_stream(&serial.final_stream, &run.final_stream, "replay");
    }

    for (workers, run) in [(1, &runs[0]), (2, &runs[1]), (4, &runs[2])] {
        assert_stream_contract(&run.initial_stream, 1, 1, 2, 0);
        assert_stream_contract(
            &run.final_stream,
            workers,
            workers.min(run.final_stream.partitions_executed),
            0,
            1,
        );
        assert_eq!(run.initial_stream.planned_gridded_route_capacity_bytes, 0);
        assert_eq!(
            run.final_stream.planned_gridded_route_capacity_bytes,
            gridded_normal_route_window_capacity_bytes(1, 64).unwrap(),
            "the shared route window must cover 64 one-record frames",
        );
        assert_eq!(
            run.final_stream.peak_partial_dynamic_capacity_bytes, 0,
            "route-once replay must not retain dynamic scientific partials",
        );
        assert!(
            run.final_stream.peak_kernel_window_capacity_bytes
                >= gridded_normal_route_window_capacity_bytes(1, 3).unwrap(),
            "the measured kernel window must include all three live frame routes",
        );
        assert!(
            run.final_stream.planned_gridded_route_capacity_bytes
                >= gridded_normal_route_window_capacity_bytes(1, 3).unwrap(),
            "the 64-frame route plan must bound the live three-frame window",
        );
    }
}

fn assert_stream_contract(
    stream: &StreamSummary,
    workers: u64,
    minimum_active_workers: u64,
    source_passes: u64,
    artifact_passes: u64,
) {
    assert_eq!(stream.planned_workers, workers);
    assert_eq!(stream.actual_workers, workers);
    assert!(
        stream.active_worker_slots >= minimum_active_workers,
        "{workers}-worker stream used only {} worker slots",
        stream.active_worker_slots,
    );
    assert_eq!(stream.source_pass_count, source_passes);
    assert_eq!(stream.artifact_pass_count, artifact_passes);
    assert_eq!(stream.partitions_executed, stream.commits_completed);
    assert_eq!(
        stream.executed_work_identity_digest, stream.committed_work_identity_digest,
        "every executed partition must commit in the same stable identity order",
    );
    assert!(stream.peak_partial_dynamic_capacity_bytes <= stream.peak_kernel_window_capacity_bytes,);
}

fn assert_worker_independent_stream(serial: &StreamSummary, parallel: &StreamSummary, stage: &str) {
    assert_eq!(
        serial.partitions_executed, parallel.partitions_executed,
        "{stage} partition identities must not depend on worker count",
    );
    assert_eq!(
        serial.commits_completed, parallel.commits_completed,
        "{stage} deterministic reductions must not depend on worker count",
    );
    assert_eq!(
        serial.executed_work_identity_digest, parallel.executed_work_identity_digest,
        "{stage} work identities must not depend on worker count",
    );
    assert_eq!(
        serial.committed_work_identity_digest, parallel.committed_work_identity_digest,
        "{stage} commit identities must not depend on worker count",
    );
    assert_eq!(
        serial.grid_resident_bytes, parallel.grid_resident_bytes,
        "{stage} must not allocate a full grid per worker",
    );
}

fn execute_complete_data_mfs(worker_count: u64) -> RunEvidence {
    let weighting = WeightingContract::new(
        WeightingScheme::Uniform,
        WeightDensityScope::GlobalSelection,
    );
    let resolution = observation_resolution();
    let (snapshot_input, initial_access) = resolve_selected_observation(resolution.clone())
        .expect("resolve three-baseline fixture")
        .into_parts();
    let snapshot = compile_observation(snapshot_input).expect("compile owner snapshot");
    let problem = compile(ImagingRequest::new(
        problem_specification(weighting),
        geometry(),
        ProblemInputIdentities::new(snapshot),
        model_lifecycle(ModelStateIdentity::Empty),
    ))
    .expect("compile constant-basis MFS problem");
    let residency = initial_access
        .certify_residency(&problem)
        .expect("certify selected-content residency");
    let replay_proof_bytes = initial_access
        .replay_proof_retained_heap_bytes(&problem)
        .expect("bounded replay-proof residency");
    let planning_registry = PlanningRegistry::new(&problem);
    let resource_policy = ResourcePolicy::Explicit(ResourceOverride {
        workers: Some(worker_count),
        ..ResourceOverride::default()
    });
    let gridded_storage = artifact_storage(worker_count);
    let execution_policy = || {
        SpectralCycleExecutionPolicy::new(
            implementation_id(),
            WeightingExecutionLimits::new(1, 1).expect("one-sample artifact frames"),
            residency.clone(),
            storage_io(),
            1_000,
            (IMAGE_PIXELS * std::mem::size_of::<Complex64>() * 3) as u64,
            900_000,
        )
        .with_planned_workers(authority(), &resource_policy)
        .expect("authorize requested worker count")
        .with_gridded_normal_storage(gridded_storage.clone())
    };
    let planned = SpectralCyclePlan::initial(&problem, &planning_registry, execution_policy())
        .expect("plan initial complete-data MFS pass");
    let minor_node = planned
        .minor_cycle_node()
        .expect("initial plan includes reconstruction cycle")
        .clone();
    let SpectralCyclePlanParts {
        physical,
        weighting: weighting_plan,
        complete_data,
        source_resources,
        pass,
        gridded_normal,
        ..
    } = planned.into_parts();
    let frozen_reservation = FrozenWeightingReservation::acquire(
        authority(),
        resource_policy.clone(),
        weighting_plan.planned_residency(),
        replay_proof_bytes,
    )
    .expect("reserve frozen weighting state");
    let selected = initial_access
        .open(&problem)
        .expect("open owner-validated selected observation");
    let executor = SpectralCycleExecutor::new(
        implementation_id(),
        problem.clone(),
        weighting_plan,
        source_resources,
        pass,
        complete_data,
        selected,
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable initial model"),
        SpectralCyclePassInput::Initial,
    )
    .with_frozen_weighting_reservation(frozen_reservation)
    .with_planned_gridded_normal_binding(
        gridded_normal.expect("initial plan binds gridded-normal compilation"),
    )
    .expect("bind gridded-normal compiler")
    .with_reconstruction_cycle(
        minor_node,
        casa_imaging_reconstruction::ReconstructionMaskPlan::FullPlane {
            coordinate: problem.geometry().domains()[0].direction(),
        },
        casa_imaging_reconstruction::MinorCycleProgram::new(0.1, 0.0, 2)
            .expect("bounded minor-cycle controls"),
    );
    let initial_registry =
        SpectralCycleRegistry::new(registry_id(), implementation_id(), &problem, executor);
    let receipt_directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        receipt_directory.path(),
        ReceiptRetention::new(4, 1 << 20).expect("receipt retention"),
    )
    .expect("receipt store");
    let initial_plan = runtime_plan(
        &problem,
        PlanningBindings::new(
            registry_id(),
            resource_policy.clone(),
            PlannerCostModelProfileBootstrap::new(cost_model_id()),
        ),
        authority(),
        &initial_registry,
        &receipts,
        move |_, _| Ok::<_, Infallible>(vec![physical]),
    )
    .expect("bind initial execution plan");
    let executable = ExecutableModelProblem::from_compiled(problem.clone()).expect("executable");
    let current = RunBindings::new(problem.inputs().clone(), &resource_policy, cost_model_id());
    let initial_attempt = attempt_id(worker_count, 0);
    runtime_run(
        &executable,
        &initial_plan,
        &current,
        &initial_registry,
        authority(),
        &mut RunToCompletion,
        receipts.bind(ExecutionProvenance::new(
            initial_attempt,
            BuildIdentity::from_sha256([82; 32]),
        )),
    )
    .expect("run initial complete-data MFS pass");
    let initial_stream = initial_registry
        .implementation()
        .latest_complete_data_stream_evidence()
        .expect("initial stream evidence")
        .into();
    let minor = initial_registry
        .implementation()
        .take_reconstruction_cycle_completion()
        .expect("initial reconstruction completion");
    let frozen_weighting = initial_registry
        .implementation()
        .take_frozen_weighting()
        .expect("frozen weighting generation");
    let gridded_replay = initial_registry
        .implementation()
        .take_gridded_normal_replay()
        .expect("sealed gridded-normal replay");
    let final_input = minor.into_final_major_input();
    let dirty = final_input.evidence().normal_state().residual().to_vec();
    let psf = final_input
        .evidence()
        .normal_state()
        .normal_approximation()
        .to_vec();

    drop(resolution);
    let final_planned = SpectralCyclePlan::final_major(
        &problem,
        &planning_registry,
        execution_policy(),
        &final_input,
        gridded_replay,
    )
    .expect("plan final gridded replay");
    let SpectralCyclePlanParts {
        physical: final_physical,
        weighting: final_weighting,
        complete_data: final_complete_data,
        pass: final_pass,
        gridded_normal: final_gridded_normal,
        ..
    } = final_planned.into_parts();
    let final_executor = SpectralCycleExecutor::new_gridded(
        implementation_id(),
        problem.clone(),
        final_weighting,
        final_pass,
        final_complete_data,
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable final model"),
        SpectralCyclePassInput::FinalMajor(final_input),
        final_gridded_normal.expect("final plan binds retained replay"),
    )
    .expect("bind final gridded replay")
    .with_frozen_weighting(frozen_weighting);
    let final_registry =
        SpectralCycleRegistry::new(registry_id(), implementation_id(), &problem, final_executor);
    let final_plan = runtime_plan(
        &problem,
        PlanningBindings::new(
            registry_id(),
            resource_policy.clone(),
            PlannerCostModelProfileBootstrap::new(cost_model_id()),
        ),
        authority(),
        &final_registry,
        &receipts,
        move |_, _| Ok::<_, Infallible>(vec![final_physical]),
    )
    .expect("bind final execution plan");
    runtime_run(
        &executable,
        &final_plan,
        &current,
        &final_registry,
        authority(),
        &mut RunToCompletion,
        receipts.bind(ExecutionProvenance::new(
            attempt_id(worker_count, 1),
            BuildIdentity::from_sha256([83; 32]),
        )),
    )
    .expect("run final complete-data MFS pass");
    let final_stream = final_registry
        .implementation()
        .latest_complete_data_stream_evidence()
        .expect("final stream evidence")
        .into();
    let completion = final_registry
        .implementation()
        .take_completion()
        .expect("final-major completion")
        .into_completion();

    RunEvidence {
        dirty,
        psf,
        model: completion.final_model().samples().to_vec(),
        residual: completion.normal_state().residual().to_vec(),
        sum_weights: completion.normal_state().sum_weights().to_vec(),
        initial_stream,
        final_stream,
    }
}

fn observation_resolution() -> SelectedObservationResolutionRequest {
    let rows = SelectedRows::from_ordered_main_rows(
        3,
        (0_usize..3).map(|row| SelectedMainRow::new(row as u64, 0)),
    )
    .expect("three canonical MAIN rows");
    let selection = ObservationSelection::new(
        rows,
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
            vec![CorrelationProduct::new(0, CorrelationType::CircularRr)],
        )],
    );
    SelectedObservationResolutionRequest::new(
        fixture().measurement_set.display().to_string(),
        identity(84),
        selection,
        VisibilityColumn::Data,
        WeightColumn::Weight,
        Vec::new(),
        ModelStateIdentity::Empty,
        SelectedObservationContentBudget::new(160 * 1024, 1, 4),
        casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
    )
}

fn geometry() -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [8.0, 8.0],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(16, 16),
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
                casa_imaging_model::MissingPointingPolicy::Reject,
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

fn problem_specification(weighting: WeightingContract) -> ProblemSpecification {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    let validity = casa_imaging_model::ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid primary-beam policy"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy"),
    );
    ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
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
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            validity,
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        numerics,
    )
}

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

fn model_lifecycle(model: ModelStateIdentity) -> ModelLifecycleRequirements {
    let input = match model {
        ModelStateIdentity::Empty => ModelInputCommitment::Empty,
        ModelStateIdentity::Seed(source) => ModelInputCommitment::AlignedSeed {
            source,
            support: identity(0xa5),
        },
        ModelStateIdentity::Generation(generation) => ModelInputCommitment::Generation(generation),
    };
    ModelLifecycleRequirements::new(
        ModelBounds::new(
            10_000_000, 10_000_000, 10_000_000, 10_000_000, 1.0e30, 1.0e30,
        )
        .expect("valid model lifecycle bounds"),
        NumericPrecision::F64,
        input,
    )
}

struct Fixture {
    _directory: tempfile::TempDir,
    measurement_set: PathBuf,
    storage_root: PathBuf,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let directory = tempfile::tempdir().expect("dedicated MFS fixture directory");
        let measurement_set = directory.path().join("three-baseline.ms");
        let storage_root = directory.path().join("artifacts");
        fs::create_dir_all(&storage_root).expect("artifact root");
        let mut antennas = tutorial_vla_a_antennas();
        antennas.truncate(3);
        let mut request =
            SyntheticObservationRequest::vla_ppdisk("unused.fits", &measurement_set, antennas);
        request.allow_below_elevation_limit = true;
        request.duration_seconds = 1.0;
        request.integration_seconds = 1.0;
        request.polarization_setup =
            SyntheticPolarizationSetup::new(SyntheticPolarizationBasis::Circular, 1)
                .expect("one circular correlation");
        request.spectral_setup.channel_count = 1;
        request.worker_policy = SyntheticWorkerPolicy::Fixed;
        request.row_workers = Some(1);
        request.channel_workers = Some(1);
        request.model = Some(SyntheticSkyModel::AnalyticComponents {
            path: None,
            schema_version: Some(1),
            name: Some("phase-centre-point".to_string()),
            components: vec![SyntheticAnalyticComponent::Point {
                name: Some("phase-centre".to_string()),
                l_rad: 0.0,
                m_rad: 0.0,
                spectrum: SyntheticAnalyticSpectrum {
                    flux_jy: 1.0,
                    spectral_index: 0.0,
                    reference_frequency_hz: None,
                    line_peak_jy: 1.0,
                    line_center_fraction: 1.0,
                    line_sigma_fraction: 0.1,
                    absorption_peak_jy: 0.0,
                    absorption_center_fraction: 0.5,
                    absorption_sigma_fraction: 0.1,
                },
            }],
        });
        request.predict_model = true;
        generate_synthetic_observation_ms(&request).expect("generate three-baseline fixture");
        initialize_measurement_set_owner_manifest(&measurement_set)
            .expect("initialize fixture owner manifest");
        Fixture {
            _directory: directory,
            measurement_set,
            storage_root,
        }
    })
}

fn artifact_storage(worker_count: u64) -> GriddedNormalReplayStorage {
    let directory = fixture()
        .storage_root
        .join(format!("workers-{worker_count}"));
    fs::create_dir_all(&directory).expect("worker artifact directory");
    GriddedNormalReplayStorage::bind(authority(), artifact_storage_io(), directory)
        .expect("bind gridded-normal storage")
}

fn authority() -> &'static ResourceAuthority {
    static AUTHORITY: OnceLock<&'static ResourceAuthority> = OnceLock::new();
    AUTHORITY.get_or_init(|| {
        ResourceAuthority::install_production_inventory(runtime_inventory())
            .expect("install dedicated deterministic authority")
    })
}

fn runtime_inventory() -> HostInventory {
    let memory_domain = CapacityDomainId::new("host-memory");
    let memory_view = CapacityViewId::new("host-memory");
    let io_rate = RateResourceId::new("io-rate");
    let io_operations = RateResourceId::new("io-operations-rate");
    let io_queue = QueueResourceId::new("io-queue");
    let transaction_rate = RateResourceId::new("transaction-io-rate");
    let transaction_queue = QueueResourceId::new("transaction-io-queue");
    let storage = StorageDomainId::new("atomic-output");
    let source_storage = StorageDomainId::new("prepared-source-secondary");
    HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: memory_domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: HOST_MEMORY_BYTES,
            }],
            memory_views: vec![MemoryView {
                id: memory_view,
                domain: memory_domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: vec![
                StorageDomain {
                    id: storage.clone(),
                    root: fixture().storage_root.clone(),
                    capacity_bytes: STORAGE_BYTES,
                    read_rate: io_rate.clone(),
                    write_rate: io_rate.clone(),
                    operations_rate: Some(io_operations.clone()),
                    queue: io_queue.clone(),
                },
                StorageDomain {
                    id: source_storage.clone(),
                    root: fixture().measurement_set.clone(),
                    capacity_bytes: STORAGE_BYTES,
                    read_rate: io_rate.clone(),
                    write_rate: io_rate.clone(),
                    operations_rate: Some(io_operations.clone()),
                    queue: io_queue.clone(),
                },
            ],
            rate_resources: vec![
                RateResource::new(io_rate.clone(), RateUnit::BytesPerSecond, STORAGE_BYTES),
                RateResource::new(io_operations.clone(), RateUnit::OperationsPerSecond, 1_024),
                RateResource::new(
                    transaction_rate.clone(),
                    RateUnit::BytesPerSecond,
                    STORAGE_BYTES,
                ),
            ],
            queue_resources: vec![
                QueueResource::new(io_queue.clone(), 8),
                QueueResource::new(transaction_queue.clone(), 4),
            ],
            logical_cpu_threads: 4,
            performance_cpu_cores: CpuClassCapacity::Known(4),
            cache_capacity_bytes: 1 << 20,
            lock_capacity: 4,
            file_descriptor_capacity: 16,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(memory_domain, HOST_MEMORY_BYTES)]),
            available_cpu_threads: 4,
            storage_available_bytes: BTreeMap::from([
                (storage, STORAGE_BYTES),
                (source_storage, STORAGE_BYTES),
            ]),
            rate_available_per_second: BTreeMap::from([
                (io_rate, STORAGE_BYTES),
                (io_operations, 1_024),
                (transaction_rate, STORAGE_BYTES),
            ]),
            queue_available_slots: BTreeMap::from([(io_queue, 8), (transaction_queue, 4)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1 << 20,
            available_locks: 4,
            available_file_descriptors: 16,
        },
    }
}

fn storage_io() -> StorageIoResourceBinding {
    StorageIoResourceBinding::new(
        StorageDomainId::new("atomic-output"),
        RateResourceId::new("transaction-io-rate"),
        RateResourceId::new("transaction-io-rate"),
        QueueResourceId::new("transaction-io-queue"),
    )
}

fn artifact_storage_io() -> StorageIoResourceBinding {
    StorageIoResourceBinding::new(
        StorageDomainId::new("atomic-output"),
        RateResourceId::new("io-rate"),
        RateResourceId::new("io-rate"),
        QueueResourceId::new("io-queue"),
    )
}

fn registry_id() -> ImplementationRegistryId {
    ImplementationRegistryId::from_sha256([TEST_IMPLEMENTATION_BYTE; 32])
}

fn implementation_id() -> WorkImplementationId {
    WorkImplementationId::new("issue-581-complete-data-mfs")
}

fn cost_model_id() -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256([85; 32])
}

fn attempt_id(worker_count: u64, phase: u8) -> ExecutionAttemptId {
    let worker = u8::try_from(worker_count).expect("bounded test worker count");
    ExecutionAttemptId::from_sha256([86_u8.wrapping_add(worker).wrapping_add(phase); 32])
}

struct PlanningImplementation {
    id: WorkImplementationId,
}

impl WorkImplementation for PlanningImplementation {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(&self, _context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        unreachable!("contract-only implementation is never executed")
    }

    fn failure_measurements<'error>(
        &'error self,
        _error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        None
    }

    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        _fence: FenceKind,
    ) -> Result<(), Self::Error> {
        unreachable!("contract-only implementation owns no fence")
    }

    fn complete_observation_read(
        &self,
        _completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        unreachable!("contract-only implementation owns no observation read")
    }

    fn publish(&self, _context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        unreachable!("contract-only implementation owns no publication")
    }
}

struct PlanningRegistry {
    id: ImplementationRegistryId,
    metadata: ImplementationContractMetadata,
    implementation: PlanningImplementation,
}

impl PlanningRegistry {
    fn new(problem: &casa_imaging_model::CompiledProblem) -> Self {
        Self {
            id: registry_id(),
            metadata: ImplementationContractMetadata::new(
                problem.problem_id(),
                problem.numerics_id(),
                problem.required_capabilities().clone(),
            ),
            implementation: PlanningImplementation {
                id: implementation_id(),
            },
        }
    }
}

impl ImplementationRegistry for PlanningRegistry {
    type Implementation = PlanningImplementation;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        (id == &self.implementation.id).then_some(&self.implementation)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        (id == &self.implementation.id).then(|| self.metadata.clone())
    }
}
