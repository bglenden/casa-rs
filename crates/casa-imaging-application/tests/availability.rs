// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_application::{
    ImplementationUnavailable, TaskRequirement, UnsupportedRequirement,
};
use casa_imaging_model::{
    AwProjectionContract, AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy,
    FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentModel, InstrumentResponse, LogicalIdentity,
    MeasurementEquationContract, MissingPointingPolicy, ModelColumnWrite, ModelInnerProduct,
    NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, ProblemInputIdentities, ProblemSpecification,
    ProductKind, ProductNormalization, ProductRequirements, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RequiredCapability, RestFrequency, RestoringBeamPolicy, ScientificContract,
    SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, SpectralWindowCoordinateCatalog, SpectralWindowSelection,
    StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct, WProjectionContract,
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
fn installed_major_cycle_pass_accepts_its_compiled_contract() {
    let problem = ProblemFixture::standard().compile();
    require_installed_implementation(&problem, [TaskRequirement::SerialCpu])
        .expect("installed major-cycle pass contract");
}

#[test]
fn installed_major_cycle_pass_accepts_planned_multi_cpu_execution() {
    let problem = ProblemFixture::standard().compile();
    require_installed_implementation(
        &problem,
        [TaskRequirement::SerialCpu, TaskRequirement::FixedTileCpu],
    )
    .expect("installed major-cycle pass supports planned multi-CPU execution");
}

#[test]
fn moving_source_is_available_through_selected_observation_geometry() {
    let problem = ProblemFixture {
        phase_centre: PhaseCentreLaw::Ephemeris("Mars".to_string()),
        inputs: common::problem_inputs(vec![(
            ReferenceDataKind::Ephemeris,
            LogicalIdentity::from_sha256([2; 32]),
        )]),
        ..ProblemFixture::standard()
    }
    .compile();
    require_installed_implementation(&problem, [])
        .expect("moving-source geometry is evaluated by selected observation traversal");
}

#[test]
fn coupled_taylor_basis_rejects_non_stokes_i_polarization() {
    for coordinate in [
        PolarizationCoordinate::StokesQ,
        PolarizationCoordinate::CircularRl,
    ] {
        let problem = ProblemFixture {
            basis: ReconstructionBasis::Taylor { terms: 2 },
            algorithm: ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            polarizations: vec![coordinate],
            ..ProblemFixture::standard()
        }
        .compile();
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
    let problem = ProblemFixture::standard().compile();
    let error = require_installed_implementation(&problem, [TaskRequirement::ExecutionAuto])
        .expect_err("automatic backends have no installed implementation");
    assert_eq!(
        error.unsupported(),
        [UnsupportedRequirement::Task(TaskRequirement::ExecutionAuto),]
    );
}

#[test]
fn w_projection_rejects_until_its_convolution_function_set_is_installed() {
    let mut fixture = ProblemFixture::standard();
    fixture.measurement_equation = fixture
        .measurement_equation
        .with_w_projection(WProjectionContract::new(100.0, None).expect("W contract"));
    let error = require_installed_implementation(
        &fixture.compile(),
        [
            TaskRequirement::WProjection,
            TaskRequirement::WProjectionPlanes,
        ],
    )
    .expect_err("W projection must reject before physical planning");
    assert_exactly_unsupported(
        &error,
        [
            UnsupportedRequirement::Capability(RequiredCapability::WProjection),
            UnsupportedRequirement::Task(TaskRequirement::WProjection),
            UnsupportedRequirement::Task(TaskRequirement::WProjectionPlanes),
        ],
    );
}

#[test]
fn aw_projection_rejects_until_its_convolution_function_set_is_installed() {
    let mut fixture =
        ProblemFixture::standard().with_primary_beam(InstrumentModel::CasaEvlaWidebandAwV1);
    fixture.measurement_equation = fixture.measurement_equation.with_aw_projection(
        AwProjectionContract::new(
            12_500.0,
            std::num::NonZeroUsize::new(32).expect("W planes"),
            true,
            false,
            true,
            true,
            false,
            [300.0, 30.0],
            5.0,
            5.0,
        )
        .expect("AW contract"),
    );
    fixture.products.push(ProductKind::Weight);
    let error =
        require_installed_implementation(&fixture.compile(), [TaskRequirement::AwProjection])
            .expect_err("AW projection must reject before physical planning");
    assert_exactly_unsupported(
        &error,
        [
            UnsupportedRequirement::Capability(RequiredCapability::AwProjection),
            UnsupportedRequirement::Capability(RequiredCapability::PrimaryBeamResponse),
            UnsupportedRequirement::Capability(RequiredCapability::Product(ProductKind::Weight)),
            UnsupportedRequirement::Task(TaskRequirement::AwProjection),
            UnsupportedRequirement::ScalarInstrumentResponse,
        ],
    );
}

#[test]
fn mosaic_primary_beam_response_rejects_until_its_convolution_function_set_is_installed() {
    let mut fixture = ProblemFixture::standard()
        .with_primary_beam(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1);
    fixture.uvw = UvwCoordinateLaw::MosaicPhaseTrackingCentre;
    fixture.products.push(ProductKind::Sensitivity);
    let error =
        require_installed_implementation(&fixture.compile(), [TaskRequirement::MosaicGridder])
            .expect_err("mosaic must reject before physical planning");
    assert_exactly_unsupported(
        &error,
        [
            UnsupportedRequirement::Capability(RequiredCapability::PrimaryBeamResponse),
            UnsupportedRequirement::Capability(RequiredCapability::Product(
                ProductKind::Sensitivity,
            )),
            UnsupportedRequirement::Task(TaskRequirement::MosaicGridder),
            UnsupportedRequirement::ScalarInstrumentResponse,
        ],
    );
}

#[test]
fn mtmfs_via_cube_rejects_until_its_primary_beam_is_installed() {
    let problem = ProblemFixture {
        basis: ReconstructionBasis::TaylorViaChannelMajor {
            terms: 2,
            channels: 4,
        },
        algorithm: ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
        ..ProblemFixture::channel_local_cube(SpectralSamplingLaw::LINEAR)
    }
    .compile();
    let error = require_installed_implementation(&problem, [TaskRequirement::SpectralMtmfsViaCube])
        .expect_err("mvc must reject before physical planning");
    assert_exactly_unsupported(
        &error,
        [UnsupportedRequirement::Task(
            TaskRequirement::SpectralMtmfsViaCube,
        )],
    );
}

#[test]
fn metal_gridding_is_installed_on_macos_only() {
    let outcome = require_installed_implementation(
        &ProblemFixture::standard().compile(),
        [TaskRequirement::MetalGridder],
    );
    if cfg!(target_os = "macos") {
        outcome.expect("the Metal backend is installed on macOS");
    } else {
        let error = outcome.expect_err("Metal gridding must reject before physical planning");
        assert_exactly_unsupported(
            &error,
            [UnsupportedRequirement::Task(TaskRequirement::MetalGridder)],
        );
    }
}

#[test]
fn faceted_geometry_rejects_without_a_pass_implementation() {
    let problem = ProblemFixture {
        facets: FacetLayout::Regular {
            columns: 2,
            rows: 2,
        },
        ..ProblemFixture::standard()
    }
    .compile();
    let error = require_installed_implementation(&problem, [])
        .expect_err("faceted geometry must reject before physical planning");
    assert_exactly_unsupported(
        &error,
        [UnsupportedRequirement::Capability(
            RequiredCapability::FacetedGeometry,
        )],
    );
}

#[test]
fn cube_interpolation_other_than_nearest_or_linear_rejects() {
    let error = require_installed_implementation(
        &ProblemFixture::channel_local_cube(SpectralSamplingLaw::CUBIC).compile(),
        [TaskRequirement::SpectralCube],
    )
    .expect_err("cubic cube interpolation must reject before physical planning");
    assert_exactly_unsupported(
        &error,
        [UnsupportedRequirement::NearestOrLinearCubeInterpolation],
    );
    assert_eq!(
        UnsupportedRequirement::NearestOrLinearCubeInterpolation.catalog_id(),
        "constraint.nearest_or_linear_cube_interpolation"
    );
    for sampling in [SpectralSamplingLaw::NEAREST, SpectralSamplingLaw::LINEAR] {
        require_installed_implementation(
            &ProblemFixture::channel_local_cube(sampling).compile(),
            [TaskRequirement::SpectralCube, TaskRequirement::SerialCpu],
        )
        .expect("the pass runs nearest and linear cube interpolation");
    }
}

fn assert_exactly_unsupported<const N: usize>(
    error: &ImplementationUnavailable,
    expected: [UnsupportedRequirement; N],
) {
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    assert_eq!(error.unsupported(), expected);
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

/// One compiled-problem fixture; [`ProblemFixture::standard`] is a dirty
/// Stokes-I MFS request with a scalar measurement equation.
struct ProblemFixture {
    phase_centre: PhaseCentreLaw,
    inputs: ProblemInputIdentities,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    polarizations: Vec<PolarizationCoordinate>,
    uvw: UvwCoordinateLaw,
    facets: FacetLayout,
    measurement_equation: MeasurementEquationContract,
    instrument_model: Option<InstrumentModel>,
    sampling: SpectralSamplingLaw,
    spectral: SpectralCoordinateSpec,
    products: Vec<ProductKind>,
}

impl ProblemFixture {
    fn standard() -> Self {
        Self {
            phase_centre: PhaseCentreLaw::Fixed(SkyDirection::new(
                DirectionFrame::J2000,
                1.0,
                -0.5,
            )),
            inputs: common::problem_inputs(Vec::new()),
            basis: ReconstructionBasis::Constant,
            algorithm: ReconstructionAlgorithm::Dirty,
            polarizations: vec![PolarizationCoordinate::StokesI],
            uvw: UvwCoordinateLaw::PhaseTrackingCentre,
            facets: FacetLayout::Single,
            measurement_equation: MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                inner_products(),
            ),
            instrument_model: None,
            sampling: SpectralSamplingLaw::IDENTITY,
            spectral: spectral_axis(1, 1.4e9, 1.0e6),
            products: vec![ProductKind::Psf],
        }
    }

    /// Four channel-local outputs at 2 MHz over twelve 1 MHz native channels
    /// at 0.996-1.007 GHz.
    fn channel_local_cube(sampling: SpectralSamplingLaw) -> Self {
        let native_hz: Vec<f64> = (0..12).map(|ch| 0.996e9 + f64::from(ch) * 1.0e6).collect();
        let spectral_window = SpectralWindowSelection::new(0, (0..12).collect())
            .with_coordinate_catalog(
                SpectralWindowCoordinateCatalog::new(native_hz, 1.0e6).expect("native catalog"),
            );
        Self {
            inputs: common::problem_inputs_with_spectral_window(Vec::new(), spectral_window),
            basis: ReconstructionBasis::ChannelLocal { channels: 4 },
            sampling,
            spectral: spectral_axis(4, 1.0e9, 2.0e6),
            ..Self::standard()
        }
    }

    /// Bind a direction-dependent primary-beam response to its exact
    /// instrument model and instrument reference data.
    fn with_primary_beam(mut self, instrument_model: InstrumentModel) -> Self {
        self.measurement_equation =
            MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam, inner_products());
        self.instrument_model = Some(instrument_model);
        self.inputs = common::problem_inputs(vec![(
            ReferenceDataKind::Instrument,
            LogicalIdentity::from_sha256([6; 32]),
        )]);
        self
    }

    fn compile(self) -> casa_imaging_model::CompiledProblem {
        compile(self.request()).expect("compile availability fixture")
    }

    fn request(self) -> ImagingRequest {
        let iteration_budget =
            usize::from(!matches!(self.algorithm, ReconstructionAlgorithm::Dirty));
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
                self.facets,
                AxisOrder::new([
                    ImageAxis::DirectionLongitude,
                    ImageAxis::DirectionLatitude,
                    ImageAxis::Polarization,
                    ImageAxis::Spectral,
                ]),
            )],
            CentreLaws::new(
                self.phase_centre,
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
            self.uvw,
            self.spectral,
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
        let science = ScientificContract::new(
            SpectralContract::new(self.sampling, SpectralCoupling::Independent),
            self.measurement_equation,
        );
        let science = match self.instrument_model {
            Some(model) => science.with_instrument_model(model),
            None => science,
        };
        ImagingRequest::new(
            ProblemSpecification::new(
                science,
                ReconstructionContract::new(
                    self.basis,
                    self.algorithm,
                    ReconstructionControls::new(iteration_budget, 1.0, 0.0),
                    PolarizationContract::new(self.polarizations),
                ),
                WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
                ProductRequirements::new(
                    self.products,
                    ProductNormalization::UnitResponse,
                    RestoringBeamPolicy::None,
                    product_validity(),
                ),
                ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
                numerics,
            ),
            geometry,
            self.inputs,
            common::model_lifecycle(),
        )
    }
}

fn inner_products() -> DeclaredInnerProducts {
    DeclaredInnerProducts::new(
        ModelInnerProduct::HermitianEuclidean,
        VisibilityInnerProduct::HermitianEuclidean,
    )
}
