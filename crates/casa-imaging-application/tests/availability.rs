// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_application::{
    BackendChoice, BackendRequest, GridPrecision, HostResources,
    availability::{ImplementationUnavailable, Unsupported, check, resolve_backend},
};
use casa_imaging_model::{
    AwProjectionContract, AxisOrder, CentreLaws, CompiledProblem, DeclaredInnerProducts,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy,
    FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    InstrumentModel, InstrumentResponse, MeasurementEquationContract, MissingPointingPolicy,
    ModelColumnWrite, ModelInnerProduct, NumericPrecision, NumericalStage, NumericsContract,
    ObservationPointingLaw, ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    ProblemInput, ProblemSpecification, ProductKind, ProductNormalization, ProductRequirements,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RequiredCapability, RestFrequency,
    RestoringBeamPolicy, ScientificContract, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct, WProjectionContract,
    WeightDensityScope, WeightingContract, WeightingScheme, compile,
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
fn a_taylor_basis_images_one_domain() {
    let taylor = || ProblemFixture {
        basis: ReconstructionBasis::Taylor { terms: 2 },
        algorithm: ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
        ..ProblemFixture::standard()
    };
    assert!(on_cpu(&taylor().compile()).is_ok());
    let problem = ProblemFixture {
        outlier: true,
        ..taylor()
    }
    .compile();
    assert_exactly_unsupported(on_cpu(&problem), [Unsupported::MultiDomainTaylorBasis]);
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
    let cube = ProblemFixture {
        basis: ReconstructionBasis::ChannelLocal { channels: 1 },
        measurement_equation: fixture.measurement_equation,
        products: fixture.products.clone(),
        ..ProblemFixture::standard().with_primary_beam(InstrumentModel::CasaEvlaWidebandAwV1)
    };
    let square_cells = fixture.image.1;
    let non_square = ProblemFixture {
        image: ([512, 256], square_cells),
        measurement_equation: fixture.measurement_equation,
        products: fixture.products.clone(),
        ..ProblemFixture::standard().with_primary_beam(InstrumentModel::CasaEvlaWidebandAwV1)
    };
    let unequal_cells = ProblemFixture {
        image: ([512, 512], [square_cells[0], 0.75 * square_cells[1]]),
        measurement_equation: fixture.measurement_equation,
        products: fixture.products.clone(),
        ..ProblemFixture::standard().with_primary_beam(InstrumentModel::CasaEvlaWidebandAwV1)
    };
    on_cpu(&fixture.compile()).expect("AW projection runs with its catalog and weight image");
    // An AW cube's per-plane weights wait for a CASA comparison (#719).
    assert_exactly_unsupported(on_cpu(&cube.compile()), [Unsupported::CubeAwProjection]);
    // Its convolution functions are laid out on one axis (#841 Phase 6b).
    for problem in [non_square, unequal_cells] {
        assert_exactly_unsupported(
            on_cpu(&problem.compile()),
            [Unsupported::NonSquareAwProjection],
        );
    }
}

/// #841 Phase 6b: the standard gridder images a non-square image of
/// non-square cells.
#[test]
fn the_standard_gridder_images_any_shape_and_cell() {
    let fixture = ProblemFixture {
        image: ([512, 256], [4.848_136_811_095_36e-6, 3.6e-6]),
        ..ProblemFixture::standard()
    };
    on_cpu(&fixture.compile()).expect("a non-square standard image");
}

#[test]
fn mosaic_runs_on_the_cpu_pass_and_on_metal() {
    let mut fixture = ProblemFixture::standard()
        .with_primary_beam(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1);
    fixture.uvw = UvwCoordinateLaw::MosaicPhaseTrackingCentre;
    fixture.products.push(ProductKind::Sensitivity);
    let problem = fixture.compile();
    on_cpu(&problem).expect("mosaic runs with its primary-beam set and sensitivity image");
    // #787: Metal grids the mosaic set, in f32, its weight image summed
    // by contribution first (#792).
    check(&problem, BackendChoice::Metal, None, &host(true)).expect("mosaic runs on Metal");
    assert_exactly_unsupported(
        check(
            &problem,
            BackendChoice::Metal,
            Some(GridPrecision::F64),
            &host(true),
        ),
        [Unsupported::F64GridsOnMetal],
    );
}

/// Metal runs the standard set in `f32` on a host with a device, and is
/// refused without one or for `f64` grids, each reason named. The mosaic
/// set runs there too (`mosaic_runs_on_the_cpu_pass_and_on_metal`); W and
/// AW are refused in their own laws.
#[test]
fn metal_needs_a_device_and_single_precision() {
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

/// `backend = auto` takes the GPU for what it runs and the T1 Metal laws
/// compare with the CPU (one image domain, any basis, facets or not), and
/// the CPU otherwise; `cpu` and `gpu` are taken as asked, and a `gpu`
/// request the GPU cannot run is refused by the gate.
#[test]
fn auto_takes_the_gpu_where_it_runs_the_problem_and_the_cpu_otherwise() {
    use BackendChoice::{Cpu, Metal};
    let resolve = |problem: &CompiledProblem, requested, precision, metal| {
        resolve_backend(problem, requested, precision, &host(metal))
    };
    let standard = ProblemFixture::standard().compile();
    assert_eq!(resolve(&standard, BackendRequest::Auto, None, true), Metal);
    let f32 = Some(GridPrecision::F32);
    assert_eq!(resolve(&standard, BackendRequest::Auto, f32, true), Metal);
    assert_eq!(resolve(&standard, BackendRequest::Auto, None, false), Cpu);
    let f64 = Some(GridPrecision::F64);
    assert_eq!(resolve(&standard, BackendRequest::Auto, f64, true), Cpu);
    assert_eq!(resolve(&standard, BackendRequest::Cpu, None, true), Cpu);
    assert_eq!(resolve(&standard, BackendRequest::Gpu, None, false), Metal);

    let taylor = ProblemFixture {
        basis: ReconstructionBasis::Taylor { terms: 2 },
        algorithm: ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
        ..ProblemFixture::standard()
    }
    .compile();
    assert_eq!(resolve(&taylor, BackendRequest::Auto, None, true), Metal);
    let faceted = ProblemFixture {
        facets: FacetLayout::Regular {
            columns: 2,
            rows: 2,
        },
        ..ProblemFixture::standard()
    }
    .compile();
    assert_eq!(resolve(&faceted, BackendRequest::Auto, None, true), Metal);

    let outliers = ProblemFixture {
        outlier: true,
        ..ProblemFixture::standard()
    }
    .compile();
    assert_eq!(resolve(&outliers, BackendRequest::Auto, None, true), Cpu);
    // No Metal law covers another polarization or a visibility write yet:
    // `auto` keeps them on the CPU, `gpu` still takes the GPU.
    for coordinate in [
        PolarizationCoordinate::StokesQ,
        PolarizationCoordinate::CircularRl,
    ] {
        let polarized = ProblemFixture {
            polarizations: vec![coordinate],
            ..ProblemFixture::standard()
        }
        .compile();
        assert_eq!(resolve(&polarized, BackendRequest::Auto, None, true), Cpu);
        assert_eq!(resolve(&polarized, BackendRequest::Gpu, None, true), Metal);
    }
    let model_write = ProblemFixture {
        model_column_write: ModelColumnWrite::SelectedRows,
        ..ProblemFixture::standard()
    }
    .compile();
    assert!(
        !model_write
            .observation_transaction()
            .write_set()
            .visibility_columns()
            .is_empty()
    );
    assert_eq!(resolve(&model_write, BackendRequest::Auto, None, true), Cpu);
    let mut w = ProblemFixture::standard();
    w.measurement_equation = w
        .measurement_equation
        .with_w_projection(WProjectionContract::new(100.0, None).expect("W contract"));
    assert_eq!(resolve(&w.compile(), BackendRequest::Auto, None, true), Cpu);
    let mut mosaic = ProblemFixture::standard()
        .with_primary_beam(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1);
    mosaic.uvw = UvwCoordinateLaw::MosaicPhaseTrackingCentre;
    mosaic.products.push(ProductKind::Sensitivity);
    let mosaic = mosaic.compile();
    // #787: a mosaic is eligible on Metal and covered by its Metal laws, so
    // `auto` takes the GPU where there is one (the owner kept the rule,
    // 2026-10-19), and the CPU otherwise.
    assert_eq!(resolve(&mosaic, BackendRequest::Auto, None, true), Metal);
    assert_eq!(resolve(&mosaic, BackendRequest::Auto, None, false), Cpu);
    let asked = resolve(&mosaic, BackendRequest::Gpu, None, true);
    assert_eq!(asked, Metal);
    check(&mosaic, asked, None, &host(true)).expect("mosaic on Metal");
}

/// Facets (#664) run with the standard kernel set and a constant basis, on
/// the CPU and on Metal; a Taylor basis or a W, mosaic or AW set is refused,
/// each with its own reason.
#[test]
fn facets_run_with_the_standard_set_and_a_constant_basis() {
    let faceted = || ProblemFixture {
        facets: FacetLayout::Regular {
            columns: 2,
            rows: 2,
        },
        ..ProblemFixture::standard()
    };
    let problem = faceted().compile();
    assert!(
        problem
            .required_capabilities()
            .contains(&RequiredCapability::FacetedGeometry)
    );
    on_cpu(&problem).expect("standard facets on the CPU");
    check(&problem, BackendChoice::Metal, None, &host(true)).expect("standard facets on Metal");
    let taylor = ProblemFixture {
        basis: ReconstructionBasis::Taylor { terms: 2 },
        algorithm: ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
        ..faceted()
    }
    .compile();
    assert_exactly_unsupported(on_cpu(&taylor), [Unsupported::FacetedBasis]);
    let mut w = faceted();
    w.measurement_equation = w
        .measurement_equation
        .with_w_projection(WProjectionContract::new(100.0, None).expect("W contract"));
    assert_exactly_unsupported(on_cpu(&w.compile()), [Unsupported::FacetedKernelSet]);
}

/// One compiled-problem fixture; [`ProblemFixture::standard`] is a dirty
/// Stokes-I MFS request with a scalar measurement equation.
struct ProblemFixture {
    phase_centre: PhaseCentreLaw,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    polarizations: Vec<PolarizationCoordinate>,
    uvw: UvwCoordinateLaw,
    facets: FacetLayout,
    measurement_equation: MeasurementEquationContract,
    instrument_model: Option<InstrumentModel>,
    products: Vec<ProductKind>,
    outlier: bool,
    model_column_write: ModelColumnWrite,
    /// The main domain's pixels and cell (radians), x then y.
    image: ([usize; 2], [f64; 2]),
}

impl ProblemFixture {
    fn standard() -> Self {
        Self {
            phase_centre: PhaseCentreLaw::Fixed(SkyDirection::new(
                DirectionFrame::J2000,
                1.0,
                -0.5,
            )),
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
            outlier: false,
            model_column_write: ModelColumnWrite::Disabled,
            image: ([512, 512], [4.848_136_811_095_36e-6; 2]),
        }
    }

    /// Bind a direction-dependent primary-beam response to its exact
    /// instrument model.
    fn with_primary_beam(mut self, instrument_model: InstrumentModel) -> Self {
        self.measurement_equation =
            MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam, inner_products());
        self.instrument_model = Some(instrument_model);
        self
    }

    fn compile(self) -> CompiledProblem {
        compile(self.request()).expect("compile availability fixture")
    }

    fn request(self) -> ProblemInput {
        let iteration_budget =
            usize::from(!matches!(self.algorithm, ReconstructionAlgorithm::Dirty));
        let ([nx, ny], [cell_x, cell_y]) = self.image;
        let direction = DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [(nx / 2) as f64 - 1.0, (ny / 2) as f64 - 1.0],
            [-cell_x, cell_y],
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
        let axes = AxisOrder::new([
            ImageAxis::DirectionLongitude,
            ImageAxis::DirectionLatitude,
            ImageAxis::Polarization,
            ImageAxis::Spectral,
        ]);
        let mut domains = vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(nx, ny),
            direction,
            self.facets,
            axes.clone(),
        )];
        if self.outlier {
            domains.push(ImageDomainSpec::new(
                ImageDomainRole::Outlier("east".into()),
                ImageShape::new(64, 64),
                DirectionCoordinateSpec::new(
                    Projection::Sin,
                    SkyDirection::new(DirectionFrame::J2000, 1.01, -0.5),
                    [31.0, 31.0],
                    [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
                    [[1.0, 0.0], [0.0, 1.0]],
                    [180.0, 0.0],
                ),
                FacetLayout::Single,
                axes,
            ));
        }
        let geometry = GeometryInput::new(
            domains,
            CentreLaws::new(
                self.phase_centre,
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
            FiniteValuePolicy::RejectUnflaggedInput,
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
                ObservationTransactionRequirements::new(self.model_column_write),
                numerics,
            ),
            geometry,
            common::observation(),
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
