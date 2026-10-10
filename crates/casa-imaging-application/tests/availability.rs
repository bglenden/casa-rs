// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_application::{
    BackendChoice, GridPrecision, HostResources,
    availability::{ImplementationUnavailable, Unsupported, check},
};
use casa_imaging_model::{
    AwProjectionContract, AxisOrder, CentreLaws, CompiledProblem, DeclaredInnerProducts,
    DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout,
    FiniteValuePolicy, FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, InstrumentModel, InstrumentResponse, LogicalIdentity, MeasurementEquationContract,
    MissingPointingPolicy, ModelColumnWrite, ModelInnerProduct, NumericPrecision, NumericalStage,
    NumericsContract, ObservationPointingLaw, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    ProblemInput, ProblemInputIdentities, ProblemSpecification, ProductKind, ProductNormalization,
    ProductRequirements, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, ReferenceDataKind,
    RequiredCapability, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct,
    WProjectionContract, WeightDensityScope, WeightingContract, WeightingScheme, compile,
};

mod common;

/// A host with one core, a gibibyte free and a Metal device or not.
const fn host(metal: bool) -> HostResources {
    HostResources {
        threads: 1,
        performance_cores: 1,
        available_memory: 1 << 30,
        metal,
    }
}

/// The CPU backend at plan decision D2's precision.
fn on_cpu(problem: &CompiledProblem) -> Result<(), ImplementationUnavailable> {
    check(problem, BackendChoice::Cpu, None, &host(false))
}

fn assert_exactly_unsupported<const N: usize>(
    outcome: Result<(), ImplementationUnavailable>,
    expected: [Unsupported; N],
) {
    let error = outcome.expect_err("the installed implementation must refuse");
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    assert_eq!(error.unsupported(), expected);
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
fn the_major_cycle_pass_accepts_its_compiled_contract() {
    on_cpu(&ProblemFixture::standard().compile()).expect("installed major-cycle pass contract");
}

#[test]
fn moving_source_is_available_through_selected_observation_geometry() {
    let problem = ProblemFixture {
        phase_centre: PhaseCentreLaw::Ephemeris("Mars".to_string()),
        inputs: common::problem_inputs(vec![(
            ReferenceDataKind::Ephemeris,
            LogicalIdentity::from_bytes([2; 32]),
        )]),
        ..ProblemFixture::standard()
    }
    .compile();
    on_cpu(&problem)
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
        assert_exactly_unsupported(on_cpu(&problem), [Unsupported::PolarizedTaylorBasis]);
    }
}

#[test]
fn w_projection_runs_on_the_cpu_pass_only() {
    let mut fixture = ProblemFixture::standard();
    fixture.measurement_equation = fixture
        .measurement_equation
        .with_w_projection(WProjectionContract::new(100.0, None).expect("W contract"));
    let problem = fixture.compile();
    on_cpu(&problem).expect("W projection runs with its W-planes set");
    assert_exactly_unsupported(
        check(&problem, BackendChoice::Metal, None, &host(true)),
        [Unsupported::KernelSetOnMetal],
    );
}

#[test]
fn aw_projection_runs_on_the_cpu_pass() {
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
    on_cpu(&fixture.compile()).expect("AW projection runs with its catalog and weight image");
}

#[test]
fn mosaic_runs_on_the_cpu_pass() {
    let mut fixture = ProblemFixture::standard()
        .with_primary_beam(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1);
    fixture.uvw = UvwCoordinateLaw::MosaicPhaseTrackingCentre;
    fixture.products.push(ProductKind::Sensitivity);
    on_cpu(&fixture.compile())
        .expect("mosaic runs with its primary-beam set and sensitivity image");
}

/// Metal runs the standard kernel set in `f32` on a host with a device;
/// every other combination is refused, each reason named.
#[test]
fn metal_needs_a_device_single_precision_and_the_standard_kernel_set() {
    let problem = ProblemFixture::standard().compile();
    check(&problem, BackendChoice::Metal, None, &host(true))
        .expect("standard gridding on a Metal device");
    check(
        &problem,
        BackendChoice::Metal,
        Some(GridPrecision::F32),
        &host(true),
    )
    .expect("explicit f32 grids on a Metal device");
    assert_exactly_unsupported(
        check(&problem, BackendChoice::Metal, None, &host(false)),
        [Unsupported::NoMetalDevice],
    );
    assert_exactly_unsupported(
        check(
            &problem,
            BackendChoice::Metal,
            Some(GridPrecision::F64),
            &host(false),
        ),
        [Unsupported::NoMetalDevice, Unsupported::F64GridsOnMetal],
    );
    check(
        &problem,
        BackendChoice::Cpu,
        Some(GridPrecision::F64),
        &host(true),
    )
    .expect("the CPU grids in either precision");
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
    assert_exactly_unsupported(
        on_cpu(&problem),
        [Unsupported::Capability(RequiredCapability::FacetedGeometry)],
    );
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
            products: vec![ProductKind::Psf],
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
            LogicalIdentity::from_bytes([6; 32]),
        )]);
        self
    }

    fn compile(self) -> CompiledProblem {
        compile(self.request()).expect("compile availability fixture")
    }

    fn request(self) -> ProblemInput {
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
        let spectral = SpectralCoordinateSpec::new(
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
        let science = ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
            self.measurement_equation,
        );
        let science = match self.instrument_model {
            Some(model) => science.with_instrument_model(model),
            None => science,
        };
        ProblemInput::new(
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
