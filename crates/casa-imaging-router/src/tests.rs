// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, MeasurementEquationContract, MissingPointingPolicy, ModelColumnWrite,
    ModelInnerProduct, NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, ProblemSpecification, ProductKind,
    ProductNormalization, ProductRequirements, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSampling, SpectralWcs,
    StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct, WeightDensityScope,
    WeightingContract, WeightingScheme,
};
use casa_imaging_runtime::{
    ExecutionRouteDisposition, ExecutionRouteEvidence, ExecutionRouteRequirement,
    ExecutionRouteRequirementEvidence, ExecutionRouteRequirementKind,
};

#[path = "../tests/common/mod.rs"]
mod common;

use super::{
    DispatchError, ImagingRouter, LegacyWholeRunEnginePort, MIGRATION_MATRIX_JSON,
    MigrationRowKind, NativeEnginePort, RequestDisposition, RouteRecord, parse_matrix,
};

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

const STANDARD_DIRTY_ROWS: [&str; 7] = [
    "capability.compiled-problem",
    "capability.continuum-mfs",
    "capability.ms-selection",
    "capability.observation-transaction",
    "capability.standard-gridder",
    "capability.stokes-i",
    "product.psf",
];

const MTMFS_ROWS: [&str; 9] = [
    "capability.compiled-problem",
    "capability.major-minor-cycles",
    "capability.ms-selection",
    "capability.mtmfs",
    "capability.observation-transaction",
    "capability.standard-gridder",
    "capability.stokes-i",
    "product.taylor-terms",
    "solver.mtmfs",
];

#[test]
fn mixed_request_stays_wholly_legacy_until_its_last_required_row_transfers() {
    let native_calls = Arc::new(AtomicUsize::new(0));
    let legacy_calls = Arc::new(AtomicUsize::new(0));
    let mixed_router = router(
        matrix_with_transfers(&STANDARD_DIRTY_ROWS[..6]),
        Arc::clone(&native_calls),
        Arc::clone(&legacy_calls),
    );

    let mixed = mixed_router.dispatch(standard_dirty_request()).unwrap();

    assert_eq!(
        mixed.route().disposition(),
        RequestDisposition::LegacyWholeRun
    );
    assert_eq!(mixed.output(), &"legacy");
    assert_eq!(
        mixed
            .route()
            .requirements()
            .iter()
            .map(|requirement| requirement.id())
            .collect::<Vec<_>>(),
        STANDARD_DIRTY_ROWS
    );
    assert_eq!(native_calls.load(Ordering::SeqCst), 0);
    assert_eq!(legacy_calls.load(Ordering::SeqCst), 1);

    let transferred_router = router(
        matrix_with_transfers(&STANDARD_DIRTY_ROWS),
        Arc::clone(&native_calls),
        Arc::clone(&legacy_calls),
    );
    let transferred = transferred_router
        .dispatch(standard_dirty_request())
        .unwrap();
    assert_eq!(
        transferred.route().disposition(),
        RequestDisposition::Native
    );
    assert_eq!(transferred.output(), &"native");
    assert_eq!(native_calls.load(Ordering::SeqCst), 1);
    assert_eq!(legacy_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn selected_engine_receives_the_exact_route_record() {
    let router = ImagingRouter::with_matrix_json(
        NativeEnginePort::new(|_, route| Ok::<_, &'static str>(route.clone())),
        LegacyWholeRunEnginePort::new(|_, route| Ok::<_, &'static str>(route.clone())),
        matrix_with_transfers(&STANDARD_DIRTY_ROWS),
    );

    let outcome = router.dispatch(standard_dirty_request()).unwrap();

    assert_eq!(outcome.output(), outcome.route());
    assert_eq!(outcome.output().matrix_schema_version(), 1);
    assert_eq!(outcome.output().matrix_contract_revision(), 9);
    assert_eq!(outcome.output().disposition(), RequestDisposition::Native);
    assert_eq!(
        outcome
            .output()
            .requirements()
            .iter()
            .map(|requirement| requirement.id())
            .collect::<Vec<_>>(),
        STANDARD_DIRTY_ROWS
    );
}

#[test]
fn authoritative_route_record_projects_losslessly_to_receipt_evidence() {
    let routed = router(
        MIGRATION_MATRIX_JSON.to_string(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )
    .dispatch(standard_dirty_request())
    .expect("authoritative route");

    let receipt_route = execution_route_evidence(routed.route());

    assert_eq!(
        receipt_route.matrix_schema_version(),
        routed.route().matrix_schema_version()
    );
    assert_eq!(
        receipt_route.matrix_contract_revision(),
        routed.route().matrix_contract_revision()
    );
    assert_eq!(
        receipt_route.disposition(),
        execution_disposition(routed.route().disposition())
    );
    assert_eq!(
        receipt_route.requirements().len(),
        routed.route().requirements().len()
    );
    for (receipt, routed) in receipt_route
        .requirements()
        .iter()
        .zip(routed.route().requirements())
    {
        let obligation = routed.obligation();
        assert_eq!(receipt.id(), routed.id());
        assert_eq!(receipt.kind(), execution_kind(routed.kind()));
        assert_eq!(
            receipt.disposition(),
            execution_disposition(routed.status())
        );
        assert_eq!(receipt.evidence().current_owner, routed.current_owner());
        assert_eq!(
            receipt.evidence().destination_tickets,
            routed.destination_tickets()
        );
        assert_eq!(receipt.evidence().evidence_issues, routed.evidence_issues());
        assert_eq!(
            receipt.evidence().baseline_manifests,
            routed.baseline_manifests()
        );
        assert_eq!(
            receipt.evidence().acceptance_contract,
            routed.acceptance_contract()
        );
        assert_eq!(receipt.evidence().transfer_point, routed.transfer_point());
        assert_eq!(
            receipt.evidence().deletion_condition,
            routed.deletion_condition()
        );
        assert_eq!(receipt.evidence().source_evidence, routed.source_evidence());
        assert_eq!(
            receipt.evidence().obligation_ticket.as_deref(),
            obligation.as_ref().map(|obligation| obligation.ticket())
        );
        assert_eq!(
            receipt.evidence().obligation_reason.as_deref(),
            obligation.as_ref().map(|obligation| obligation.reason())
        );
    }
}

fn execution_route_evidence(route: &RouteRecord) -> ExecutionRouteEvidence {
    ExecutionRouteEvidence::new(
        route.matrix_schema_version(),
        route.matrix_contract_revision(),
        execution_disposition(route.disposition()),
        route
            .requirements()
            .iter()
            .map(|requirement| {
                let obligation = requirement.obligation();
                ExecutionRouteRequirement::new(
                    requirement.id(),
                    execution_kind(requirement.kind()),
                    execution_disposition(requirement.status()),
                    ExecutionRouteRequirementEvidence {
                        current_owner: requirement.current_owner().to_string(),
                        destination_tickets: requirement.destination_tickets().to_vec(),
                        evidence_issues: requirement.evidence_issues().to_vec(),
                        baseline_manifests: requirement.baseline_manifests().to_vec(),
                        acceptance_contract: requirement.acceptance_contract().to_string(),
                        transfer_point: requirement.transfer_point().to_string(),
                        deletion_condition: requirement.deletion_condition().to_string(),
                        source_evidence: requirement.source_evidence().to_vec(),
                        obligation_ticket: obligation
                            .map(|obligation| obligation.ticket().to_string()),
                        obligation_reason: obligation
                            .map(|obligation| obligation.reason().to_string()),
                    },
                )
                .expect("authoritative route requirement")
            })
            .collect(),
    )
    .expect("authoritative route evidence")
}

const fn execution_disposition(disposition: RequestDisposition) -> ExecutionRouteDisposition {
    match disposition {
        RequestDisposition::Native => ExecutionRouteDisposition::Native,
        RequestDisposition::LegacyWholeRun => ExecutionRouteDisposition::LegacyWholeRun,
        RequestDisposition::TemporarilyUnavailable => {
            ExecutionRouteDisposition::TemporarilyUnavailable
        }
    }
}

const fn execution_kind(kind: MigrationRowKind) -> ExecutionRouteRequirementKind {
    match kind {
        MigrationRowKind::Capability => ExecutionRouteRequirementKind::Capability,
        MigrationRowKind::Product => ExecutionRouteRequirementKind::Product,
        MigrationRowKind::Solver => ExecutionRouteRequirementKind::Solver,
        MigrationRowKind::Frontend => ExecutionRouteRequirementKind::Frontend,
        MigrationRowKind::Backend => ExecutionRouteRequirementKind::Backend,
    }
}

#[test]
fn mtmfs_request_derives_its_solver_and_major_minor_cycle_rows() {
    let routed = router(
        MIGRATION_MATRIX_JSON.to_string(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )
    .dispatch(mtmfs_request())
    .unwrap();

    assert_eq!(
        routed
            .route()
            .requirements()
            .iter()
            .map(|requirement| requirement.id())
            .collect::<Vec<_>>(),
        MTMFS_ROWS
    );
}

#[test]
fn authoritative_matrix_binding_drives_product_requirement() {
    let routed = router(
        matrix_with_product_binding("Psf", "product.residual"),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )
    .dispatch(standard_dirty_request())
    .unwrap();

    assert_eq!(
        routed
            .route()
            .requirements()
            .iter()
            .map(|requirement| requirement.id())
            .collect::<Vec<_>>(),
        [
            "capability.compiled-problem",
            "capability.continuum-mfs",
            "capability.ms-selection",
            "capability.observation-transaction",
            "capability.standard-gridder",
            "capability.stokes-i",
            "product.residual",
        ]
    );
}

#[test]
fn matrix_contract_revision_is_a_positive_u32() {
    let routed = router(
        MIGRATION_MATRIX_JSON.to_string(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )
    .dispatch(standard_dirty_request())
    .unwrap();

    let revision: u32 = routed.route().matrix_contract_revision();
    assert_eq!(revision, 9);

    for invalid in [serde_json::json!(0), serde_json::json!("5")] {
        let mut matrix = serde_json::from_str::<serde_json::Value>(MIGRATION_MATRIX_JSON).unwrap();
        matrix["contract_revision"] = invalid;
        assert!(parse_matrix(&serde_json::to_string(&matrix).unwrap()).is_err());
    }
}

#[test]
fn observation_transaction_contract_is_required_by_every_native_plan() {
    let routed = router(
        MIGRATION_MATRIX_JSON.to_string(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )
    .dispatch(standard_dirty_request())
    .unwrap();

    let transaction = routed
        .route()
        .requirements()
        .iter()
        .find(|requirement| requirement.id() == "capability.observation-transaction")
        .expect("native plan requires its observation transaction");
    assert_eq!(transaction.status(), RequestDisposition::Native);
    assert_eq!(
        transaction.acceptance_contract(),
        "observation-transaction-v1"
    );
    assert_eq!(
        transaction.source_evidence(),
        [
            "crates/casa-imaging-model/src/transaction.rs::pub struct ObservationTransactionContract",
            "crates/casa-imaging-runtime/src/execution_bindings.rs::pub fn plan",
            "crates/casa-imaging-runtime/src/observation_transaction.rs::pub(crate) fn bind_observation_transaction",
        ]
    );
}

#[test]
fn selected_model_column_write_keeps_the_whole_request_legacy() {
    let routed = router(
        matrix_with_transfers(&STANDARD_DIRTY_ROWS),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )
    .dispatch(standard_dirty_model_write_request())
    .unwrap();

    assert_eq!(
        routed.route().disposition(),
        RequestDisposition::LegacyWholeRun
    );
    let model_write = routed
        .route()
        .requirements()
        .iter()
        .find(|requirement| requirement.id() == "capability.model-column-write")
        .expect("selected MODEL_DATA writes require their migration row");
    assert_eq!(model_write.status(), RequestDisposition::LegacyWholeRun);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeStageFailure {
    Compile,
    Plan,
    Run,
}

#[test]
fn native_compile_plan_or_run_failure_never_invokes_legacy() {
    for failure in [
        NativeStageFailure::Compile,
        NativeStageFailure::Plan,
        NativeStageFailure::Run,
    ] {
        let native_calls = Arc::new(AtomicUsize::new(0));
        let legacy_calls = Arc::new(AtomicUsize::new(0));
        let native_counter = Arc::clone(&native_calls);
        let legacy_counter = Arc::clone(&legacy_calls);
        let router = ImagingRouter::with_matrix_json(
            NativeEnginePort::new(move |_, _| {
                native_counter.fetch_add(1, Ordering::SeqCst);
                Err(failure)
            }),
            LegacyWholeRunEnginePort::new(move |_, _| {
                legacy_counter.fetch_add(1, Ordering::SeqCst);
                Ok("legacy")
            }),
            matrix_with_transfers(&STANDARD_DIRTY_ROWS),
        );

        let error = router.dispatch(standard_dirty_request()).unwrap_err();

        match error {
            DispatchError::Native { route, source } => {
                assert_eq!(source, failure);
                assert_eq!(route.disposition(), RequestDisposition::Native);
            }
            other => panic!("expected terminal native failure, got {other:?}"),
        }
        assert_eq!(native_calls.load(Ordering::SeqCst), 1);
        assert_eq!(legacy_calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn absent_required_matrix_row_fails_closed_before_engine_invocation() {
    let native_calls = Arc::new(AtomicUsize::new(0));
    let legacy_calls = Arc::new(AtomicUsize::new(0));
    let router = router(
        matrix_without("capability.compiled-problem"),
        Arc::clone(&native_calls),
        Arc::clone(&legacy_calls),
    );

    let error = router.dispatch(standard_dirty_request()).unwrap_err();

    assert!(matches!(
        error,
        DispatchError::InvalidMatrix(message) if message.contains("capability.compiled-problem")
    ));
    assert_eq!(native_calls.load(Ordering::SeqCst), 0);
    assert_eq!(legacy_calls.load(Ordering::SeqCst), 0);
}

fn router(
    matrix_json: String,
    native_calls: Arc<AtomicUsize>,
    legacy_calls: Arc<AtomicUsize>,
) -> ImagingRouter<&'static str, &'static str> {
    ImagingRouter::with_matrix_json(
        NativeEnginePort::new(move |_, _| {
            native_calls.fetch_add(1, Ordering::SeqCst);
            Ok("native")
        }),
        LegacyWholeRunEnginePort::new(move |_, _| {
            legacy_calls.fetch_add(1, Ordering::SeqCst);
            Ok("legacy")
        }),
        matrix_json,
    )
}

fn matrix_with_transfers(native_rows: &[&str]) -> String {
    let mut matrix = serde_json::from_str::<serde_json::Value>(MIGRATION_MATRIX_JSON).unwrap();
    for row in matrix["rows"].as_array_mut().unwrap() {
        let identifier = row["id"].as_str().unwrap();
        if native_rows.contains(&identifier) {
            row["status"] = serde_json::Value::String("Native".to_string());
            row["migration_obligation"] = serde_json::Value::Null;
        }
    }
    serde_json::to_string(&matrix).unwrap()
}

fn matrix_without(identifier: &str) -> String {
    let mut matrix = serde_json::from_str::<serde_json::Value>(MIGRATION_MATRIX_JSON).unwrap();
    matrix["rows"]
        .as_array_mut()
        .unwrap()
        .retain(|row| row["id"].as_str() != Some(identifier));
    serde_json::to_string(&matrix).unwrap()
}

fn matrix_with_product_binding(product: &str, row: &str) -> String {
    let mut matrix = serde_json::from_str::<serde_json::Value>(MIGRATION_MATRIX_JSON).unwrap();
    matrix["product_kind_inventory"][product] = serde_json::Value::String(row.to_string());
    serde_json::to_string(&matrix).unwrap()
}

fn standard_dirty_request() -> ImagingRequest {
    standard_dirty_request_with(ModelColumnWrite::Disabled)
}

fn standard_dirty_model_write_request() -> ImagingRequest {
    standard_dirty_request_with(ModelColumnWrite::SelectedRows)
}

fn standard_dirty_request_with(model_column_write: ModelColumnWrite) -> ImagingRequest {
    request(
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        model_column_write,
    )
}

fn mtmfs_request() -> ImagingRequest {
    request(
        ReconstructionContract::new(
            ReconstructionBasis::Taylor { terms: 2 },
            ReconstructionAlgorithm::Mtmfs,
            ReconstructionControls::new(100, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        ProductRequirements::new(
            vec![ProductKind::TaylorTerms],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        ModelColumnWrite::Disabled,
    )
}

fn request(
    reconstruction: ReconstructionContract,
    products: ProductRequirements,
    model_column_write: ModelColumnWrite,
) -> ImagingRequest {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [255.0, 255.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
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
    );
    ImagingRequest::new(
        ProblemSpecification::new(
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
            reconstruction,
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            products,
            ObservationTransactionRequirements::new(model_column_write),
            NumericsContract::new(
                vec![NumericPrecision::F64],
                ReductionPolicy::Compensated,
                FiniteValuePolicy::FlagInputRejectGenerated,
                NumericalStage::ALL
                    .into_iter()
                    .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                    .collect(),
            ),
        ),
        geometry,
        common::problem_inputs(Vec::new()),
    )
}
