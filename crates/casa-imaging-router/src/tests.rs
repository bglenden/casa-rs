// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use casa_imaging_model::{
    AxisOrder, CentreLaws, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame, GeometryInput, ImageAxis,
    ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest, InstrumentResponse,
    MeasurementEquationContract, MissingPointingPolicy, ModelStateIdentity, NumericPrecision,
    NumericalStage, NumericsContract, ObservationPointingLaw, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    ProblemSpecification, ProductKind, ProductNormalization, ProductRequirements, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, StageErrorBudget, UvwCoordinateLaw, WeightDensityScope,
    WeightingContract, WeightingScheme,
};

#[path = "../tests/common/mod.rs"]
mod common;

use common::problem_inputs;

use super::{
    DispatchError, ImagingRouter, LegacyWholeRunEnginePort, MIGRATION_MATRIX_JSON,
    NativeEnginePort, RequestDisposition, parse_matrix,
};

const STANDARD_DIRTY_ROWS: [&str; 6] = [
    "capability.compiled-problem",
    "capability.continuum-mfs",
    "capability.ms-selection",
    "capability.standard-gridder",
    "capability.stokes-i",
    "product.psf",
];

const MTMFS_ROWS: [&str; 8] = [
    "capability.compiled-problem",
    "capability.major-minor-cycles",
    "capability.ms-selection",
    "capability.mtmfs",
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
        matrix_with_transfers(&STANDARD_DIRTY_ROWS[..5]),
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
    assert_eq!(revision, 5);

    for invalid in [serde_json::json!(0), serde_json::json!("5")] {
        let mut matrix = serde_json::from_str::<serde_json::Value>(MIGRATION_MATRIX_JSON).unwrap();
        matrix["contract_revision"] = invalid;
        assert!(parse_matrix(&serde_json::to_string(&matrix).unwrap()).is_err());
    }
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
            NativeEnginePort::new(move |_| {
                native_counter.fetch_add(1, Ordering::SeqCst);
                Err(failure)
            }),
            LegacyWholeRunEnginePort::new(move |_| {
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
        NativeEnginePort::new(move |_| {
            native_calls.fetch_add(1, Ordering::SeqCst);
            Ok("native")
        }),
        LegacyWholeRunEnginePort::new(move |_| {
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
        ),
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
        ),
    )
}

fn request(
    reconstruction: ReconstructionContract,
    products: ProductRequirements,
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
                MeasurementEquationContract::new(InstrumentResponse::Scalar),
            ),
            reconstruction,
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            products,
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
        problem_inputs(1, Vec::new(), ModelStateIdentity::Empty),
    )
}
