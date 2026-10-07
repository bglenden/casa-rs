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
    ProblemInputIdentities, ProblemSpecification, ProductKind, ProductNormalization,
    ProductRequirements, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, ReferenceDataKind,
    RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, SpectralWindowCoordinateCatalog, SpectralWindowSelection, StageErrorBudget,
    UvwCoordinateLaw, VisibilityInnerProduct, WProjectionContract, WeightDensityScope,
    WeightingContract, WeightingScheme, compile,
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
#[test]
fn installed_spectral_cycle_accepts_its_compiled_contract() {
    let problem = compile(standard_dirty_request()).expect("compile spectral cycle request");
    require_installed_implementation(&problem, [TaskRequirement::SerialCpu])
        .expect("installed spectral cycle contract");
}

#[test]
fn installed_spectral_cycle_accepts_planned_multi_cpu_execution() {
    let problem = compile(standard_dirty_request()).expect("compile spectral cycle request");
    require_installed_implementation(
        &problem,
        [TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu],
    )
    .expect("installed spectral cycle supports planned multi-CPU execution");
}

#[test]
fn moving_source_is_available_through_selected_observation_geometry() {
    let problem = compile(moving_source_request()).expect("compile moving-source request");
    require_installed_implementation(&problem, [])
        .expect("moving-source geometry is evaluated by selected observation traversal");
}

#[test]
fn coupled_taylor_basis_rejects_non_stokes_i_polarization() {
    for coordinate in [
        PolarizationCoordinate::StokesQ,
        PolarizationCoordinate::CircularRl,
    ] {
        let problem = compile(request_with_reconstruction(
            PhaseCentreLaw::Fixed(SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5)),
            Vec::new(),
            ReconstructionBasis::Taylor { terms: 2 },
            ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            vec![coordinate],
        ))
        .expect("compile non-Stokes-I Taylor request");
        let error = require_installed_implementation(&problem, [])
            .expect_err("coupled Taylor polarization must fail closed");
        assert!(
            error
                .unsupported()
                .contains(&UnsupportedRequirement::IndependentBasisForPolarizationSelection)
        );
    }
}

#[test]
fn unavailable_task_requirements_are_exact_and_typed() {
    let problem = compile(standard_dirty_request()).expect("compile spectral cycle request");
    let error = require_installed_implementation(&problem, [TaskRequirement::ExecutionAuto])
        .expect_err("automatic backends have no installed implementation");
    assert_eq!(
        error.unsupported(),
        [UnsupportedRequirement::Task(TaskRequirement::ExecutionAuto),]
    );
}

#[test]
fn w_projection_with_mosaic_is_rejected_at_the_typed_availability_boundary() {
    let request = request_with_reconstruction_geometry(
        PhaseCentreLaw::Fixed(SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5)),
        common::problem_inputs(Vec::new()),
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        vec![PolarizationCoordinate::StokesI],
        UvwCoordinateLaw::MosaicPhaseTrackingCentre,
        Some(WProjectionContract::new(100.0, None).expect("W contract")),
        SpectralSamplingLaw::IDENTITY,
        spectral_axis(1, 1.4e9, 1.0e6),
    );
    let problem = compile(request).expect("compile mosaic W request");
    let error = require_installed_implementation(
        &problem,
        [TaskRequirement::MosaicGridder, TaskRequirement::WProjection],
    )
    .expect_err("mosaic W must reject before physical planning");
    assert!(
        error
            .unsupported()
            .contains(&UnsupportedRequirement::WProjectionWithMosaic)
    );
}

#[test]
fn metal_cube_coarse_output_channels_are_rejected_at_the_typed_availability_boundary() {
    // Twelve 1 MHz native channels at 0.996-1.007 GHz; four output channels.
    let cube = |increment_hz: f64| {
        let native_hz: Vec<f64> = (0..12).map(|ch| 0.996e9 + f64::from(ch) * 1.0e6).collect();
        let spectral_window = SpectralWindowSelection::new(0, (0..12).collect())
            .with_coordinate_catalog(
                SpectralWindowCoordinateCatalog::new(native_hz, 1.0e6).expect("native catalog"),
            );
        compile(request_with_reconstruction_geometry(
            PhaseCentreLaw::Fixed(SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5)),
            common::problem_inputs_with_spectral_window(Vec::new(), spectral_window),
            ReconstructionBasis::ChannelLocal { channels: 4 },
            ReconstructionAlgorithm::Dirty,
            vec![PolarizationCoordinate::StokesI],
            UvwCoordinateLaw::PhaseTrackingCentre,
            None,
            SpectralSamplingLaw::LINEAR,
            spectral_axis(4, 1.0e9, increment_hz),
        ))
        .expect("compile channel-local cube request")
    };
    let coarse = cube(2.0e6);
    let error = require_installed_implementation(&coarse, [TaskRequirement::MetalGridder])
        .expect_err("two native channels per output must reject Metal before planning");
    if cfg!(all(target_os = "macos", not(coverage))) {
        assert_eq!(
            error.unsupported(),
            [UnsupportedRequirement::MetalCubeCoarseOutputChannels]
        );
        // The native-width cube reaches the same Metal cube route.
        require_installed_implementation(&cube(1.0e6), [TaskRequirement::MetalGridder])
            .expect("native-width Metal cube is admitted");
    } else {
        assert!(
            error
                .unsupported()
                .contains(&UnsupportedRequirement::MetalCubeCoarseOutputChannels)
        );
    }
    assert_eq!(
        UnsupportedRequirement::MetalCubeCoarseOutputChannels.catalog_id(),
        "constraint.metal_cube_coarse_output_channels"
    );
    require_installed_implementation(&coarse, [TaskRequirement::SerialCpu])
        .expect("the CPU cube admits coarse output channels");
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
    request_with_reconstruction(
        phase_centre,
        reference_data,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        vec![PolarizationCoordinate::StokesI],
    )
}

fn request_with_reconstruction(
    phase_centre: PhaseCentreLaw,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    polarizations: Vec<PolarizationCoordinate>,
) -> ImagingRequest {
    request_with_reconstruction_geometry(
        phase_centre,
        common::problem_inputs(reference_data),
        basis,
        algorithm,
        polarizations,
        UvwCoordinateLaw::PhaseTrackingCentre,
        None,
        SpectralSamplingLaw::IDENTITY,
        spectral_axis(1, 1.4e9, 1.0e6),
    )
}

fn spectral_axis(
    channels: usize,
    reference_frequency_hz: f64,
    increment_hz: f64,
) -> SpectralCoordinateSpec {
    SpectralCoordinateSpec::new(
        FrequencyFrame::Topocentric,
        FrequencyFrame::Topocentric,
        SpectralFrameAnchor::NotApplicable,
        SpectralWcs::Linear {
            channels,
            reference_pixel: 0.0,
            reference_frequency_hz,
            increment_hz,
        },
        RestFrequency::NotApplicable,
        DopplerConvention::NotApplicable,
    )
}

#[allow(clippy::too_many_arguments)]
fn request_with_reconstruction_geometry(
    phase_centre: PhaseCentreLaw,
    inputs: ProblemInputIdentities,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    polarizations: Vec<PolarizationCoordinate>,
    uvw: UvwCoordinateLaw,
    w_projection: Option<WProjectionContract>,
    sampling: SpectralSamplingLaw,
    spectral: SpectralCoordinateSpec,
) -> ImagingRequest {
    let iteration_budget = usize::from(!matches!(algorithm, ReconstructionAlgorithm::Dirty));
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
        uvw,
        spectral,
    );
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::UnorderedWithinBudget,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    ImagingRequest::new(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(sampling, SpectralCoupling::Independent),
                w_projection.map_or_else(
                    || {
                        MeasurementEquationContract::new(
                            InstrumentResponse::Scalar,
                            DeclaredInnerProducts::new(
                                ModelInnerProduct::HermitianEuclidean,
                                VisibilityInnerProduct::HermitianEuclidean,
                            ),
                        )
                    },
                    |contract| {
                        MeasurementEquationContract::new(
                            InstrumentResponse::Scalar,
                            DeclaredInnerProducts::new(
                                ModelInnerProduct::HermitianEuclidean,
                                VisibilityInnerProduct::HermitianEuclidean,
                            ),
                        )
                        .with_w_projection(contract)
                    },
                ),
            ),
            ReconstructionContract::new(
                basis,
                algorithm,
                ReconstructionControls::new(iteration_budget, 1.0, 0.0),
                PolarizationContract::new(polarizations),
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
        inputs,
        common::model_lifecycle(),
    )
}
