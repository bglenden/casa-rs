// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, LogicalIdentity, MeasurementEquationContract, MissingPointingPolicy,
    ModelInnerProduct, ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract,
    ObservationPointingLaw, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, ProblemSpecification, ProductKind,
    ProductNormalization, ProductRequirements, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct,
    WeightDensityScope, WeightingContract, WeightingScheme,
};
use casa_imaging_router::{
    DispatchError, ImagingRouter, LegacyWholeRunEnginePort, NativeEnginePort, RequestDisposition,
};

mod common;

use common::problem_inputs;

#[test]
fn legacy_request_invokes_only_whole_run_legacy_engine() {
    let native_calls = Arc::new(AtomicUsize::new(0));
    let legacy_calls = Arc::new(AtomicUsize::new(0));
    let router = ImagingRouter::new(
        NativeEnginePort::new({
            let native_calls = Arc::clone(&native_calls);
            move |_, _| {
                native_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &'static str>("native")
            }
        }),
        LegacyWholeRunEnginePort::new({
            let legacy_calls = Arc::clone(&legacy_calls);
            move |_, _| {
                legacy_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &'static str>("legacy")
            }
        }),
    );

    let outcome = router.dispatch(standard_dirty_request()).unwrap();

    assert_eq!(
        outcome.route().disposition(),
        RequestDisposition::LegacyWholeRun
    );
    assert_eq!(outcome.output(), &"legacy");
    assert_eq!(native_calls.load(Ordering::SeqCst), 0);
    assert_eq!(legacy_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn unavailable_request_invokes_neither_engine_and_reports_its_migration_obligation() {
    let native_calls = Arc::new(AtomicUsize::new(0));
    let legacy_calls = Arc::new(AtomicUsize::new(0));
    let router = ImagingRouter::new(
        NativeEnginePort::new({
            let native_calls = Arc::clone(&native_calls);
            move |_, _| {
                native_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &'static str>("native")
            }
        }),
        LegacyWholeRunEnginePort::new({
            let legacy_calls = Arc::clone(&legacy_calls);
            move |_, _| {
                legacy_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &'static str>("legacy")
            }
        }),
    );

    let error = router.dispatch(moving_source_request()).unwrap_err();

    let DispatchError::TemporarilyUnavailable(route) = error else {
        panic!("moving-source request did not fail at the routing seam");
    };
    assert_eq!(
        route.disposition(),
        RequestDisposition::TemporarilyUnavailable
    );
    let requirement = route
        .requirements()
        .iter()
        .find(|requirement| requirement.id() == "capability.moving-source")
        .expect("moving-source routing evidence");
    assert_eq!(
        requirement.status(),
        RequestDisposition::TemporarilyUnavailable
    );
    assert_eq!(requirement.acceptance_contract(), "exact-routing-v1");
    assert!(!requirement.evidence_issues().is_empty());
    assert!(!requirement.baseline_manifests().is_empty());
    let obligation = requirement.obligation().expect("migration obligation");
    assert_eq!(obligation.capability(), "capability.moving-source");
    assert_eq!(obligation.ticket(), "T41/#527");
    assert!(!obligation.current_owner().is_empty());
    assert!(!obligation.reason().is_empty());
    assert!(!obligation.destination_tickets().is_empty());
    assert!(!obligation.transfer_point().is_empty());
    assert!(!obligation.deletion_condition().is_empty());
    assert_eq!(native_calls.load(Ordering::SeqCst), 0);
    assert_eq!(legacy_calls.load(Ordering::SeqCst), 0);
}

fn standard_dirty_request() -> ImagingRequest {
    request_with_phase_centre(
        PhaseCentreLaw::Fixed(SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5)),
        Vec::new(),
    )
}

fn moving_source_request() -> ImagingRequest {
    request_with_phase_centre(
        PhaseCentreLaw::Ephemeris("Mars".to_string()),
        vec![(
            ReferenceDataKind::Ephemeris,
            LogicalIdentity::from_sha256([2; 32]),
        )],
    )
}

fn request_with_phase_centre(
    phase_centre: PhaseCentreLaw,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
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
            phase_centre,
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
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
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
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                vec![ProductKind::Psf],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
            ),
            numerics,
        ),
        geometry,
        problem_inputs(1, reference_data, ModelStateIdentity::Empty),
    )
}
