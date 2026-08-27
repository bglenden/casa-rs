// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_application::{
    ImplementationUnavailable, TaskRequirement, UnsupportedRequirement,
};
use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, LogicalIdentity, MeasurementEquationContract, MissingPointingPolicy,
    ModelColumnWrite, ModelInnerProduct, NumericPrecision, NumericalStage, NumericsContract,
    ObservationPointingLaw, ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    ProblemSpecification, ProductKind, ProductNormalization, ProductRequirements, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, ReferenceDataKind, RestFrequency, RestoringBeamPolicy, ScientificContract,
    SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct,
    WeightDensityScope, WeightingContract, WeightingScheme, compile,
};

mod common;

fn require_installed_implementation(
    problem: &casa_imaging_model::CompiledProblem,
    requirements: impl IntoIterator<Item = TaskRequirement>,
) -> Result<(), ImplementationUnavailable> {
    casa_imaging_application::validate_installed_implementation(problem, requirements)
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
#[test]
fn installed_spectral_cycle_accepts_its_compiled_contract() {
    let problem = compile(standard_dirty_request()).expect("compile spectral cycle request");
    require_installed_implementation(
        &problem,
        [TaskRequirement::SerialCpu, TaskRequirement::RustFft],
    )
    .expect("installed spectral cycle contract");
}

#[test]
fn moving_source_fails_typed_before_execution() {
    let problem = compile(moving_source_request()).expect("compile moving-source request");
    let error = require_installed_implementation(&problem, [])
        .expect_err("moving-source request must fail closed");
    assert!(
        error
            .unsupported()
            .contains(&UnsupportedRequirement::FixedPhaseCentre)
    );
}

#[test]
fn unavailable_task_requirements_are_exact_and_typed() {
    let problem = compile(standard_dirty_request()).expect("compile spectral cycle request");
    let error = require_installed_implementation(
        &problem,
        [TaskRequirement::ExecutionAuto, TaskRequirement::FftAuto],
    )
    .expect_err("automatic backends have no installed implementation");
    assert_eq!(
        error.unsupported(),
        [
            UnsupportedRequirement::Task(TaskRequirement::ExecutionAuto),
            UnsupportedRequirement::Task(TaskRequirement::FftAuto),
        ]
    );
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
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                vec![ProductKind::Psf],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                product_validity(),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            numerics,
        ),
        geometry,
        common::problem_inputs(reference_data),
        common::model_lifecycle(),
    )
}
