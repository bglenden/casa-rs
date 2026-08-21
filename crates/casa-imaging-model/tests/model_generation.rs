// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, MeasurementEquationContract, MissingPointingPolicy, ModelColumnWrite,
    ModelGeneration, ModelGenerationCommitment, ModelGenerationCommitmentError,
    ModelGenerationCommitmentId, ModelGenerationCompletionEvidence, ModelGenerationError,
    ModelGenerationId, ModelInnerProduct, ModelStateIdentity, NormalEquationContractId,
    NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemSpecification,
    ProductBlankingPolicy, ProductKind, ProductNormalization, ProductRequirements,
    ProductSupportComparison, ProductValidityPolicies, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    VisibilityInnerProduct, WeightDensityScope, WeightingContract, WeightingScheme, compile,
};

mod common;

use common::{identity, problem_inputs};

#[test]
fn model_generation_commitment_is_compiler_bound_and_schema_pinned() {
    assert_eq!(ModelGenerationCommitment::SCHEMA_VERSION, 1);
    assert_eq!(NormalEquationContractId::SCHEMA_VERSION, 1);

    let baseline = compile(request(RequestFixture::baseline()))
        .expect("compile baseline empty-lineage model problem");
    let changed = compile(request(RequestFixture::changed_model_contract()))
        .expect("compile changed empty-lineage model problem");
    let product_only = compile(request(RequestFixture::product_only()))
        .expect("compile product-only changed model problem");
    let raw_seed = compile(request(RequestFixture::raw_seed()))
        .expect("compile legacy raw-seed input problem");
    let raw_generation = compile(request(RequestFixture::raw_generation()))
        .expect("compile legacy raw-generation input problem");
    let hogbom = compile(request(RequestFixture::constant_algorithm(
        ReconstructionAlgorithm::Hogbom,
    )))
    .expect("compile Hogbom model contract");
    let clark = compile(request(RequestFixture::constant_algorithm(
        ReconstructionAlgorithm::Clark,
    )))
    .expect("compile Clark model contract");
    let channel_local = compile(request(RequestFixture::channel_local()))
        .expect("compile channel-local model contract");

    assert!(matches!(
        ModelGenerationCommitment::from_problem(&raw_seed),
        Err(ModelGenerationCommitmentError::UnownedLineageRoot {
            root: ModelStateIdentity::Seed(root),
        }) if root == identity(4)
    ));
    assert!(matches!(
        ModelGenerationCommitment::from_problem(&raw_generation),
        Err(ModelGenerationCommitmentError::UnownedLineageRoot {
            root: ModelStateIdentity::Generation(root),
        }) if root == identity(5)
    ));

    let baseline_commitment = ModelGenerationCommitment::from_problem(&baseline)
        .expect("commit empty-lineage baseline model contract");
    let changed_commitment = ModelGenerationCommitment::from_problem(&changed)
        .expect("commit changed empty-lineage model contract");
    let product_only_commitment = ModelGenerationCommitment::from_problem(&product_only)
        .expect("commit product-independent model contract");
    let hogbom_commitment =
        ModelGenerationCommitment::from_problem(&hogbom).expect("commit Hogbom model contract");
    let clark_commitment =
        ModelGenerationCommitment::from_problem(&clark).expect("commit Clark model contract");
    let channel_local_commitment = ModelGenerationCommitment::from_problem(&channel_local)
        .expect("commit channel-local model contract");
    let baseline_id: ModelGenerationCommitmentId = baseline_commitment.commitment_id();
    let changed_id: ModelGenerationCommitmentId = changed_commitment.commitment_id();

    assert_eq!(
        baseline_id.as_bytes(),
        [
            170, 9, 125, 196, 153, 168, 113, 168, 198, 42, 128, 79, 236, 250, 226, 165, 234, 221,
            32, 182, 209, 90, 33, 0, 216, 53, 233, 12, 102, 58, 123, 178,
        ]
    );
    assert_ne!(baseline_id, changed_id);
    assert_ne!(baseline_commitment, changed_commitment);
    assert_ne!(baseline.problem_id(), changed.problem_id());
    assert_ne!(
        baseline.geometry().geometry_id(),
        changed.geometry().geometry_id()
    );
    assert_ne!(
        baseline.reconstruction().basis(),
        changed.reconstruction().basis()
    );
    assert_ne!(baseline.numerics_id(), changed.numerics_id());
    assert_ne!(
        baseline.reconstruction().algorithm(),
        changed.reconstruction().algorithm()
    );
    assert_ne!(
        hogbom_commitment.commitment_id(),
        clark_commitment.commitment_id(),
        "reconstruction algorithm is model-generation identity"
    );

    assert_ne!(baseline.problem_id(), product_only.problem_id());
    assert_ne!(baseline_commitment, product_only_commitment);
    assert_eq!(
        baseline_commitment.commitment_id(),
        product_only_commitment.commitment_id(),
        "product-only request changes must not create a different model-generation contract"
    );

    assert_ne!(baseline_id, channel_local_commitment.commitment_id());
}

#[test]
fn empty_model_generation_requires_exact_completion_coverage() {
    assert_eq!(ModelGeneration::SCHEMA_VERSION, 1);
    assert_eq!(ModelGenerationCompletionEvidence::SCHEMA_VERSION, 1);

    let problem =
        compile(request(RequestFixture::baseline())).expect("compile empty-lineage model problem");
    let commitment = ModelGenerationCommitment::from_problem(&problem)
        .expect("commit empty-lineage model contract");
    let product_only_problem =
        compile(request(RequestFixture::product_only())).expect("compile product-only variant");
    let product_only_commitment = ModelGenerationCommitment::from_problem(&product_only_problem)
        .expect("commit product-only variant");
    assert_eq!(
        commitment.commitment_id(),
        product_only_commitment.commitment_id()
    );
    let stale_completion = ModelGenerationCompletionEvidence::empty(&product_only_commitment);
    assert!(matches!(
        ModelGeneration::complete(&commitment, stale_completion),
        Err(ModelGenerationError::StaleCompletion {
            expected_problem,
            actual_problem,
        }) if expected_problem == problem.problem_id()
            && actual_problem == product_only_problem.problem_id()
    ));

    let completion = ModelGenerationCompletionEvidence::empty(&commitment);
    let generation =
        ModelGeneration::complete(&commitment, completion).expect("own exact model generation");
    let generation_id: ModelGenerationId = generation.generation_id();
    assert_eq!(
        generation_id.as_bytes(),
        [
            203, 55, 34, 217, 110, 35, 247, 150, 30, 225, 11, 201, 176, 128, 10, 93, 106, 106, 110,
            27, 252, 156, 175, 192, 174, 228, 159, 104, 30, 103, 112, 192,
        ]
    );
    assert_eq!(generation.commitment_id(), commitment.commitment_id());
    assert_eq!(generation.problem_id(), problem.problem_id());
    assert_eq!(generation.coefficient_count(), 4);
}

#[test]
fn model_generation_commitment_identity_encodes_every_model_semantic() {
    let baseline = model_commitment_id(RequestFixture::baseline());
    let mutations = [
        (
            "compiled geometry",
            RequestFixture {
                width: 96,
                ..RequestFixture::baseline()
            },
        ),
        (
            "observation snapshot",
            RequestFixture {
                observation: 9,
                ..RequestFixture::baseline()
            },
        ),
        (
            "weighting contract",
            RequestFixture {
                weighting: WeightingContract::new(
                    WeightingScheme::Natural,
                    WeightDensityScope::NotApplicable,
                ),
                ..RequestFixture::baseline()
            },
        ),
        (
            "normal-equation input",
            RequestFixture {
                instrument_response: InstrumentResponse::PrimaryBeam,
                ..RequestFixture::baseline()
            },
        ),
        (
            "maximum minor iterations",
            RequestFixture {
                controls: ReconstructionControls::new(101, 0.1, 0.0),
                ..RequestFixture::baseline()
            },
        ),
        (
            "loop gain",
            RequestFixture {
                controls: ReconstructionControls::new(100, 0.2, 0.0),
                ..RequestFixture::baseline()
            },
        ),
        (
            "stopping threshold",
            RequestFixture {
                controls: ReconstructionControls::new(100, 0.1, 1.0e-5),
                ..RequestFixture::baseline()
            },
        ),
        (
            "polarization coordinates",
            RequestFixture {
                polarization: vec![
                    PolarizationCoordinate::StokesI,
                    PolarizationCoordinate::StokesQ,
                ],
                ..RequestFixture::baseline()
            },
        ),
        (
            "numerics",
            RequestFixture {
                precision: NumericPrecision::F64,
                ..RequestFixture::baseline()
            },
        ),
    ];
    for (semantic, fixture) in mutations {
        assert_ne!(
            baseline,
            model_commitment_id(fixture),
            "{semantic} must alter model-generation commitment identity"
        );
    }

    let multiscale = model_commitment_id(RequestFixture::multiscale(vec![0.0, 3.0]));
    assert_ne!(
        multiscale,
        model_commitment_id(RequestFixture::multiscale(vec![0.0, 5.0])),
        "each multiscale scale list is model-generation identity"
    );
    let independent_coupling = model_commitment_id(RequestFixture::spectral_coupling(
        SpectralCoupling::Independent,
    ));
    assert_eq!(
        independent_coupling,
        model_commitment_id(RequestFixture::spectral_coupling(
            SpectralCoupling::CommonRestoringBeam,
        )),
        "spectral product coupling is intentionally outside model-generation identity"
    );
}

fn model_commitment_id(fixture: RequestFixture) -> ModelGenerationCommitmentId {
    let problem = compile(request(fixture)).expect("compile model-generation identity fixture");
    ModelGenerationCommitment::from_problem(&problem)
        .expect("commit model-generation identity fixture")
        .commitment_id()
}

#[derive(Debug, Clone)]
struct RequestFixture {
    observation: u8,
    width: usize,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
    spectral_coupling: SpectralCoupling,
    instrument_response: InstrumentResponse,
    polarization: Vec<PolarizationCoordinate>,
    weighting: WeightingContract,
    model: ModelStateIdentity,
    precision: NumericPrecision,
    products: Vec<ProductKind>,
    restoring_beam: RestoringBeamPolicy,
}

impl RequestFixture {
    fn baseline() -> Self {
        Self {
            observation: 1,
            width: 64,
            basis: ReconstructionBasis::Taylor { terms: 2 },
            algorithm: ReconstructionAlgorithm::Mtmfs,
            controls: ReconstructionControls::new(100, 0.1, 0.0),
            spectral_coupling: SpectralCoupling::Independent,
            instrument_response: InstrumentResponse::Scalar,
            polarization: vec![PolarizationCoordinate::StokesI],
            weighting: WeightingContract::new(
                WeightingScheme::Briggs { robust: 0.5 },
                WeightDensityScope::GlobalSelection,
            ),
            model: ModelStateIdentity::Empty,
            precision: NumericPrecision::F32,
            products: vec![ProductKind::Model],
            restoring_beam: RestoringBeamPolicy::None,
        }
    }

    fn changed_model_contract() -> Self {
        Self {
            width: 96,
            basis: ReconstructionBasis::Constant,
            algorithm: ReconstructionAlgorithm::Hogbom,
            controls: ReconstructionControls::new(200, 0.2, 1.0e-5),
            precision: NumericPrecision::F64,
            ..Self::baseline()
        }
    }

    fn product_only() -> Self {
        Self {
            products: vec![ProductKind::SumWeights],
            ..Self::baseline()
        }
    }

    fn raw_seed() -> Self {
        Self {
            model: ModelStateIdentity::Seed(identity(4)),
            ..Self::baseline()
        }
    }

    fn raw_generation() -> Self {
        Self {
            model: ModelStateIdentity::Generation(identity(5)),
            ..Self::baseline()
        }
    }

    fn constant_algorithm(algorithm: ReconstructionAlgorithm) -> Self {
        Self {
            basis: ReconstructionBasis::Constant,
            algorithm,
            ..Self::baseline()
        }
    }

    fn channel_local() -> Self {
        Self {
            basis: ReconstructionBasis::ChannelLocal { channels: 1 },
            algorithm: ReconstructionAlgorithm::Hogbom,
            ..Self::baseline()
        }
    }

    fn multiscale(scales_px: Vec<f64>) -> Self {
        Self {
            basis: ReconstructionBasis::Constant,
            algorithm: ReconstructionAlgorithm::Multiscale { scales_px },
            ..Self::baseline()
        }
    }

    fn spectral_coupling(spectral_coupling: SpectralCoupling) -> Self {
        Self {
            spectral_coupling,
            products: vec![ProductKind::Model, ProductKind::RestoredImage],
            restoring_beam: match spectral_coupling {
                SpectralCoupling::Independent => RestoringBeamPolicy::PerPlane,
                SpectralCoupling::CommonRestoringBeam => RestoringBeamPolicy::Common,
            },
            ..Self::baseline()
        }
    }
}

fn request(fixture: RequestFixture) -> ImagingRequest {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [31.0, 23.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![
            ImageDomainSpec::new(
                ImageDomainRole::Outlier("field-b".to_owned()),
                ImageShape::new(fixture.width / 2, 24),
                direction,
                FacetLayout::Single,
                image_axes(),
            ),
            ImageDomainSpec::new(
                ImageDomainRole::Main,
                ImageShape::new(fixture.width, 48),
                direction,
                FacetLayout::Single,
                image_axes(),
            ),
        ],
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
        casa_imaging_model::UvwCoordinateLaw::PhaseTrackingCentre,
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
    let science = ScientificContract::new(
        SpectralContract::new(SpectralSampling::Identity, fixture.spectral_coupling),
        MeasurementEquationContract::new(
            fixture.instrument_response,
            DeclaredInnerProducts::new(
                ModelInnerProduct::HermitianEuclidean,
                VisibilityInnerProduct::HermitianEuclidean,
            ),
        ),
    );
    let reconstruction = ReconstructionContract::new(
        fixture.basis,
        fixture.algorithm,
        fixture.controls,
        PolarizationContract::new(fixture.polarization),
    );
    let validity = ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB cutoff fixture"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor support fixture"),
    );
    let numerics = NumericsContract::new(
        vec![fixture.precision],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    let mut references = vec![(ReferenceDataKind::Measures, identity(2))];
    if fixture.instrument_response != InstrumentResponse::Scalar {
        references.push((ReferenceDataKind::Instrument, identity(3)));
    }
    ImagingRequest::new(
        ProblemSpecification::new(
            science,
            reconstruction,
            fixture.weighting,
            ProductRequirements::new(
                fixture.products,
                ProductNormalization::UnitResponse,
                fixture.restoring_beam,
                validity,
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            numerics,
        ),
        geometry,
        problem_inputs(fixture.observation, references, fixture.model),
    )
}

fn image_axes() -> AxisOrder {
    AxisOrder::new([
        ImageAxis::DirectionLongitude,
        ImageAxis::DirectionLatitude,
        ImageAxis::Polarization,
        ImageAxis::Spectral,
    ])
}
