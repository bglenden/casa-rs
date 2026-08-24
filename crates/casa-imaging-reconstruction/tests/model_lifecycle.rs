// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompileProblemError, CompiledProblem, DeclaredInnerProducts,
    DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout,
    FiniteValuePolicy, FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, ImagingRequest, InstrumentResponse, LogicalIdentity, MeasurementEquationContract,
    ModelBounds, ModelCell, ModelColumnWrite, ModelContractError, ModelDeltaTerm,
    ModelExecutionAttemptId, ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements,
    ModelSample, ModelSourceShape, ModelStateIdentity, ModelSupport, ModelValue, NumericPrecision,
    NumericalStage, NumericsContract, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    UvwCoordinateLaw, VisibilityInnerProduct, WeightDensityScope, WeightingContract,
    WeightingScheme, compile,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FinalModelCompletionId, ModelDeltaId, ModelGenerationId,
    ModelGenerationOrigin, ModelLifecycle, ModelLifecycleError, ModelReprojectionError,
    ModelReprojectionId, ModelSourceReader, PreparedReprojectedSeed, model_support_identity,
    prepare_reprojected_seed,
};

#[path = "../../casa-imaging-model/tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
}

fn bounds() -> ModelBounds {
    ModelBounds::new(16, 16, 32, 8, 1.0e30, 1.0e30).expect("valid bounds")
}

fn empty_requirements(precision: NumericPrecision) -> ModelLifecycleRequirements {
    ModelLifecycleRequirements::new(bounds(), precision, ModelInputCommitment::Empty)
}

fn product_validity() -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB policy"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy"),
    )
}

fn geometry(width: usize) -> GeometryInput {
    geometry_with_direction(width, 1.0, [0.0, 0.0])
}

fn geometry_with_longitude(width: usize, longitude: f64) -> GeometryInput {
    geometry_with_direction(width, longitude, [0.0, 0.0])
}

fn geometry_with_reference_pixel(width: usize, reference_pixel: [f64; 2]) -> GeometryInput {
    geometry_with_direction(width, 1.0, reference_pixel)
}

fn geometry_with_direction(
    width: usize,
    longitude: f64,
    reference_pixel: [f64; 2],
) -> GeometryInput {
    geometry_with_direction_and_spectral(
        width,
        longitude,
        reference_pixel,
        SpectralWcs::Linear {
            channels: 1,
            reference_pixel: 0.0,
            reference_frequency_hz: 1.4e9,
            increment_hz: 1.0e6,
        },
    )
}

fn geometry_with_spectral(width: usize, wcs: SpectralWcs) -> GeometryInput {
    geometry_with_direction_and_spectral(width, 1.0, [0.0, 0.0], wcs)
}

fn geometry_with_direction_and_spectral(
    width: usize,
    longitude: f64,
    reference_pixel: [f64; 2],
    spectral_wcs: SpectralWcs,
) -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, longitude, -0.5),
        reference_pixel,
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(width, 1),
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
            PointingCentreLaw::PhaseTrackingCentre,
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            spectral_wcs,
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    )
}

fn problem(
    observation: u8,
    width: usize,
    model: ModelStateIdentity,
    lifecycle: ModelLifecycleRequirements,
    precision: NumericPrecision,
) -> casa_imaging_model::CompiledProblem {
    problem_with_geometry(observation, geometry(width), model, lifecycle, precision)
}

fn problem_with_geometry(
    observation: u8,
    geometry: GeometryInput,
    model: ModelStateIdentity,
    lifecycle: ModelLifecycleRequirements,
    precision: NumericPrecision,
) -> casa_imaging_model::CompiledProblem {
    problem_with_polarizations(
        observation,
        geometry,
        model,
        lifecycle,
        precision,
        vec![PolarizationCoordinate::StokesI],
    )
}

fn problem_with_polarizations(
    observation: u8,
    geometry: GeometryInput,
    model: ModelStateIdentity,
    lifecycle: ModelLifecycleRequirements,
    precision: NumericPrecision,
    polarizations: Vec<PolarizationCoordinate>,
) -> casa_imaging_model::CompiledProblem {
    problem_with_contract(
        observation,
        geometry,
        model,
        lifecycle,
        precision,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        polarizations,
        vec![ProductKind::Psf],
    )
    .expect("compile model lifecycle problem")
}

#[allow(clippy::too_many_arguments)]
fn problem_with_contract(
    observation: u8,
    geometry: GeometryInput,
    model: ModelStateIdentity,
    lifecycle: ModelLifecycleRequirements,
    precision: NumericPrecision,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    polarizations: Vec<PolarizationCoordinate>,
    products: Vec<ProductKind>,
) -> Result<CompiledProblem, CompileProblemError> {
    let controls = if matches!(algorithm, ReconstructionAlgorithm::Dirty) {
        ReconstructionControls::new(0, 1.0, 0.0)
    } else {
        ReconstructionControls::new(100, 0.1, 0.0)
    };
    let specification = ProblemSpecification::new(
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
            basis,
            algorithm,
            controls,
            PolarizationContract::new(polarizations),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            products,
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![precision],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    );
    compile(ImagingRequest::new(
        specification,
        geometry,
        common::problem_inputs(observation, Vec::new(), model),
        lifecycle,
    ))
}

fn cell(x: usize) -> ModelCell {
    ModelCell::new(0, 0, 0, [x, 0])
}

fn value(value: f64) -> ModelValue {
    ModelValue::new(value).expect("finite model value")
}

fn attempt(byte: u8) -> ModelExecutionAttemptId {
    ModelExecutionAttemptId::new(identity(byte))
}

fn bind_direct(
    problem: &CompiledProblem,
    attempt: ModelExecutionAttemptId,
    epoch: u64,
) -> ModelLifecycle {
    ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem"),
        attempt,
        epoch,
    )
    .expect("bind model lifecycle")
}

struct SliceModelSource<'a> {
    source: LogicalIdentity,
    shape: &'a ModelSourceShape,
    samples: &'a [ModelSample],
    reads: usize,
    fail_at: Option<ModelCell>,
}

impl<'a> SliceModelSource<'a> {
    fn new(
        source: LogicalIdentity,
        shape: &'a ModelSourceShape,
        samples: &'a [ModelSample],
    ) -> Self {
        Self {
            source,
            shape,
            samples,
            reads: 0,
            fail_at: None,
        }
    }
}

impl ModelSourceReader for SliceModelSource<'_> {
    type Error = &'static str;

    fn source_identity(&self) -> LogicalIdentity {
        self.source
    }

    fn source_shape(&self) -> &ModelSourceShape {
        self.shape
    }

    fn read_sample(&mut self, cell: ModelCell) -> Result<ModelSample, Self::Error> {
        self.reads += 1;
        if self.fail_at == Some(cell) {
            return Err("injected source failure");
        }
        self.samples
            .get(
                self.shape
                    .flat_index(cell)
                    .ok_or("source cell outside shape")?,
            )
            .copied()
            .ok_or("source sample missing")
    }
}

fn prepare_seed(
    source: LogicalIdentity,
    source_shape: &ModelSourceShape,
    target_problem: &CompiledProblem,
    samples: &[ModelSample],
) -> PreparedReprojectedSeed {
    let mut reader = SliceModelSource::new(source, source_shape, samples);
    prepare_reprojected_seed(&mut reader, target_problem)
        .expect("prepare owner-derived reprojection")
}

#[test]
fn compiled_commitment_binds_problem_input_numerics_and_bounds() {
    let first = problem(
        1,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let other_observation = problem(
        2,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let f32_problem = problem(
        1,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F32),
        NumericPrecision::F32,
    );

    assert_ne!(first.problem_id(), other_observation.problem_id());
    assert_ne!(
        first.model_lifecycle().contract_id(),
        f32_problem.model_lifecycle().contract_id()
    );
    assert_eq!(
        first.model_lifecycle().bounds().max_delta_terms(),
        bounds().max_delta_terms()
    );
    assert_eq!(
        first.model_lifecycle().arithmetic_precision(),
        NumericPrecision::F64
    );
}

#[test]
fn empty_generation_and_delta_have_exact_golden_identities_and_finalization_is_affine() {
    let compiled = problem(
        1,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let mut owner = bind_direct(&compiled, attempt(90), 1);
    let base = owner.initial_empty().expect("empty generation");
    let replay_base = owner.initial_empty().expect("second pre-final base");
    assert_eq!(ModelGenerationId::SCHEMA_VERSION, 2);
    assert_eq!(ModelDeltaId::SCHEMA_VERSION, 2);
    assert_eq!(ModelReprojectionId::SCHEMA_VERSION, 3);
    assert_eq!(FinalModelCompletionId::SCHEMA_VERSION, 1);
    assert_eq!(
        base.generation_id().to_string(),
        "c8aa1f9f2d0df2cd47f25696232e8f0f12d0058ba35783dd3ea1dc87b394ad37"
    );

    let delta = owner
        .compile_delta(&base, [ModelDeltaTerm::new(cell(0), value(1.5))])
        .expect("compile delta");
    let replay_delta = owner
        .compile_delta(&replay_base, [ModelDeltaTerm::new(cell(0), value(1.5))])
        .expect("compile replay delta");
    assert_eq!(
        delta.delta_id().to_string(),
        "c51140ee15cc8f4e318f71c534e3791106433916ffaf108712fc7ec2c3be4e87"
    );
    let update = owner
        .apply_final_delta(base, delta)
        .expect("apply final affine update");
    assert_eq!(update.generation().samples()[0].value().value(), 1.5);
    assert_eq!(
        update.completion().generation(),
        update.generation().generation_id()
    );
    assert_ne!(
        update.completion().completion_id().as_bytes(),
        update.generation().generation_id().as_bytes()
    );
    assert!(matches!(
        owner.apply_final_delta(replay_base, replay_delta),
        Err(ModelLifecycleError::FinalModelAlreadyCompleted)
    ));
    assert!(matches!(
        owner.initial_empty(),
        Err(ModelLifecycleError::FinalModelAlreadyCompleted)
    ));
}

#[test]
fn aligned_ingest_preserves_support_and_rejects_wrong_evidence() {
    let seed = identity(7);
    let samples = [ModelSample::valid(value(2.0)), ModelSample::invalid()];
    let support = model_support_identity(samples.iter().map(|sample| sample.support()));
    let compiled = problem(
        1,
        2,
        ModelStateIdentity::Seed(seed),
        ModelLifecycleRequirements::new(
            bounds(),
            NumericPrecision::F64,
            ModelInputCommitment::AlignedSeed {
                source: seed,
                support,
            },
        ),
        NumericPrecision::F64,
    );
    let owner = bind_direct(&compiled, attempt(91), 1);
    let target_shape = owner.contract().target().clone();
    let generation = owner
        .ingest_aligned(seed, &target_shape, samples.into_iter().map(Ok::<_, ()>))
        .expect("aligned source stream")
        .expect("ingest aligned seed");
    assert_eq!(generation.samples()[1].support(), ModelSupport::Invalid);
    assert_eq!(generation.samples()[1].value().value(), 0.0);

    let other_space = problem_with_geometry(
        1,
        geometry_with_longitude(2, 1.25),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    assert_eq!(
        target_shape.sample_count(),
        other_space.model_lifecycle().target().sample_count()
    );
    assert_ne!(target_shape, *other_space.model_lifecycle().target());
    assert!(matches!(
        owner
            .ingest_aligned(
                seed,
                other_space.model_lifecycle().target(),
                samples.into_iter().map(Ok::<_, ()>),
            )
            .expect("aligned source stream"),
        Err(ModelLifecycleError::SourceProvenanceMismatch)
    ));

    let wrong = [
        ModelSample::valid(value(2.0)),
        ModelSample::valid(value(0.0)),
    ];
    assert!(matches!(
        owner
            .ingest_aligned(seed, &target_shape, wrong.into_iter().map(Ok::<_, ()>))
            .expect("aligned source stream"),
        Err(ModelLifecycleError::SupportIdentityMismatch)
    ));
}

#[test]
fn aligned_ingest_preserves_a_terminal_source_error() {
    let seed = identity(72);
    let samples = [ModelSample::valid(value(2.0)), ModelSample::invalid()];
    let support = model_support_identity(samples.iter().map(|sample| sample.support()));
    let compiled = problem(
        1,
        2,
        ModelStateIdentity::Seed(seed),
        ModelLifecycleRequirements::new(
            bounds(),
            NumericPrecision::F64,
            ModelInputCommitment::AlignedSeed {
                source: seed,
                support,
            },
        ),
        NumericPrecision::F64,
    );
    let owner = bind_direct(&compiled, attempt(125), 1);
    let stream = samples
        .into_iter()
        .map(Ok)
        .chain([Err("terminal source failure")]);

    assert!(matches!(
        owner.ingest_aligned(seed, owner.contract().target(), stream),
        Err("terminal source failure")
    ));
}

#[test]
fn reprojection_is_owner_derived_streamed_support_aware_and_golden_pinned() {
    let source_problem = problem(
        3,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let target_shell = problem_with_geometry(
        4,
        geometry_with_reference_pixel(2, [-0.75, 0.0]),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let source_shape = source_problem.model_lifecycle().target().clone();
    let seed = identity(8);
    let source = [
        ModelSample::valid(value(2.0)),
        ModelSample::valid(value(6.0)),
    ];
    let mut reader = SliceModelSource::new(seed, &source_shape, &source);
    let prepared = prepare_reprojected_seed(&mut reader, &target_shell)
        .expect("derive reprojection from source geometry and samples");
    assert_eq!(reader.reads, 2, "only the current derived stencil is read");
    let mapping_id = prepared.reprojection_id();
    assert_eq!(
        mapping_id.to_string(),
        "f7bc726307177a322c156232605b670cfa5fb3a43ead14b8fb917f40adba6fde"
    );
    assert_eq!(
        prepared.support_identity(),
        model_support_identity([ModelSupport::Valid, ModelSupport::Invalid])
    );
    let compiled = problem_with_geometry(
        4,
        geometry_with_reference_pixel(2, [-0.75, 0.0]),
        ModelStateIdentity::Seed(seed),
        prepared.lifecycle_requirements(),
        NumericPrecision::F64,
    );
    assert!(matches!(
        ExecutableModelProblem::from_compiled(compiled.clone()),
        Err(ModelLifecycleError::OwnerPreparationRequired)
    ));
    let executable = prepared
        .bind_compiled_problem(compiled)
        .expect("bind owner-derived preparation");
    let mut owner = ModelLifecycle::bind(executable, attempt(92), 1).expect("bind owner");
    let generation = owner
        .initial_reprojected()
        .expect("consume derived reprojection");
    assert_eq!(generation.samples()[0].value().value(), 5.0);
    assert_eq!(generation.samples()[1].support(), ModelSupport::Invalid);

    let wrong_source_space = problem_with_geometry(
        3,
        geometry_with_longitude(2, 1.25),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let wrong_source_shape = wrong_source_space.model_lifecycle().target().clone();
    let mut wrong_reader = SliceModelSource::new(seed, &wrong_source_shape, &source);
    assert!(matches!(
        prepare_reprojected_seed(&mut wrong_reader, &target_shell,),
        Err(ModelReprojectionError::Lifecycle(
            ModelLifecycleError::UnsupportedDirectionConversion
        ))
    ));
    assert_eq!(wrong_reader.reads, 0);

    let other_target = problem_with_geometry(
        4,
        geometry_with_reference_pixel(2, [-0.25, 0.0]),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let mut other_reader = SliceModelSource::new(seed, &source_shape, &source);
    let other_prepared = prepare_reprojected_seed(&mut other_reader, &other_target)
        .expect("derive other target mapping");
    assert_ne!(
        mapping_id,
        other_prepared.reprojection_id(),
        "target coordinate law must change owner-derived evidence"
    );

    let mut failing_reader = SliceModelSource::new(seed, &source_shape, &source);
    failing_reader.fail_at = Some(cell(1));
    assert!(matches!(
        prepare_reprojected_seed(&mut failing_reader, &target_shell,),
        Err(ModelReprojectionError::Source("injected source failure"))
    ));
}

#[test]
fn reprojected_seed_proof_binds_projected_values_and_stencils() {
    let source_problem = problem(
        3,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let target = problem_with_geometry(
        4,
        geometry_with_reference_pixel(2, [-0.75, 0.0]),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let other_stencil = problem_with_geometry(
        4,
        geometry_with_reference_pixel(2, [-0.25, 0.0]),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let source_shape = source_problem.model_lifecycle().target().clone();
    let seed = identity(201);
    let values = [
        ModelSample::valid(value(2.0)),
        ModelSample::valid(value(6.0)),
    ];
    let changed_values = [
        ModelSample::valid(value(3.0)),
        ModelSample::valid(value(6.0)),
    ];

    let original = prepare_seed(seed, &source_shape, &target, &values);
    let value_changed = prepare_seed(seed, &source_shape, &target, &changed_values);
    let stencil_changed = prepare_seed(seed, &source_shape, &other_stencil, &values);

    assert_ne!(
        original.proof_identity(),
        value_changed.proof_identity(),
        "equal support with different projected values must not share a proof",
    );
    assert_ne!(
        original.proof_identity(),
        stencil_changed.proof_identity(),
        "different ordered interpolation stencils must not share a proof",
    );
}

#[test]
fn reprojection_converts_taylor_coefficients_to_channel_coordinates() {
    let spectral = SpectralWcs::Linear {
        channels: 2,
        reference_pixel: 0.0,
        reference_frequency_hz: 1.5e9,
        increment_hz: 1.0e9,
    };
    let source_problem = problem_with_contract(
        9,
        geometry_with_spectral(1, spectral.clone()),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
        ReconstructionBasis::Taylor { terms: 2 },
        ReconstructionAlgorithm::Mtmfs,
        vec![PolarizationCoordinate::StokesI],
        vec![ProductKind::Psf],
    )
    .expect("compile Taylor source space");
    let target_shell = problem_with_contract(
        10,
        geometry_with_spectral(1, spectral.clone()),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        vec![PolarizationCoordinate::StokesI],
        vec![ProductKind::Psf],
    )
    .expect("compile channel-local target space");
    let source_shape = source_problem.model_lifecycle().target().clone();
    let seed = identity(81);
    let samples = [
        ModelSample::valid(value(2.0)),
        ModelSample::valid(value(4.0)),
    ];
    let mut reader = SliceModelSource::new(seed, &source_shape, &samples);
    let prepared = prepare_reprojected_seed(&mut reader, &target_shell)
        .expect("derive channel values from the Taylor polynomial");
    assert_eq!(reader.reads, 4, "each channel evaluates both Taylor terms");
    let compiled = problem_with_contract(
        10,
        geometry_with_spectral(1, spectral),
        ModelStateIdentity::Seed(seed),
        prepared.lifecycle_requirements(),
        NumericPrecision::F64,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        vec![PolarizationCoordinate::StokesI],
        vec![ProductKind::Psf],
    )
    .expect("bind channel-local reprojection evidence");
    let executable = prepared
        .bind_compiled_problem(compiled)
        .expect("bind owner-derived preparation");
    let generation = ModelLifecycle::bind(executable, attempt(124), 1)
        .expect("bind basis-conversion owner")
        .initial_reprojected()
        .expect("consume owner-derived basis conversion");
    assert_eq!(
        generation
            .samples()
            .iter()
            .map(|sample| sample.value().value())
            .collect::<Vec<_>>(),
        vec![1.0, 3.0]
    );
}

#[test]
fn reprojection_converts_stokes_to_linear_parallel_hands() {
    let source_problem = problem_with_polarizations(
        9,
        geometry(1),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
        vec![
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
        ],
    );
    let target_shell = problem_with_polarizations(
        10,
        geometry(1),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
        vec![
            PolarizationCoordinate::LinearXx,
            PolarizationCoordinate::LinearYy,
        ],
    );
    let source_shape = source_problem.model_lifecycle().target().clone();
    let seed = identity(82);
    let samples = [
        ModelSample::valid(value(10.0)),
        ModelSample::valid(value(2.0)),
    ];
    let mut reader = SliceModelSource::new(seed, &source_shape, &samples);
    let prepared = prepare_reprojected_seed(&mut reader, &target_shell)
        .expect("derive linear parallel hands from Stokes I and Q");
    assert_eq!(reader.reads, 4, "each target hand consumes Stokes I and Q");
    let compiled = problem_with_polarizations(
        10,
        geometry(1),
        ModelStateIdentity::Seed(seed),
        prepared.lifecycle_requirements(),
        NumericPrecision::F64,
        vec![
            PolarizationCoordinate::LinearXx,
            PolarizationCoordinate::LinearYy,
        ],
    );
    let executable = prepared
        .bind_compiled_problem(compiled)
        .expect("bind owner-derived preparation");
    let generation = ModelLifecycle::bind(executable, attempt(126), 1)
        .expect("bind polarization owner")
        .initial_reprojected()
        .expect("consume owner-derived polarization conversion");
    assert_eq!(
        generation
            .samples()
            .iter()
            .map(|sample| sample.value().value())
            .collect::<Vec<_>>(),
        vec![12.0, 8.0]
    );
}

#[test]
fn reprojected_seed_rejects_a_different_product_contract() {
    let source_problem = problem(
        11,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let target_geometry = geometry_with_reference_pixel(2, [-0.75, 0.0]);
    let target_shell = problem_with_geometry(
        12,
        target_geometry.clone(),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let source_shape = source_problem.model_lifecycle().target().clone();
    let seed = identity(83);
    let samples = [
        ModelSample::valid(value(2.0)),
        ModelSample::valid(value(6.0)),
    ];
    let prepared = prepare_seed(seed, &source_shape, &target_shell, &samples);

    assert!(matches!(
        problem_with_contract(
            12,
            target_geometry,
            ModelStateIdentity::Seed(seed),
            prepared.lifecycle_requirements(),
            NumericPrecision::F64,
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
            vec![PolarizationCoordinate::StokesI],
            vec![ProductKind::Weight],
        ),
        Err(CompileProblemError::ModelLifecycle(
            ModelContractError::ReprojectionContractMismatch
        ))
    ));
}

#[test]
fn reprojected_compile_and_ingest_reject_foreign_owner_evidence() {
    let source_problem = problem(
        3,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let target_shell = problem_with_geometry(
        4,
        geometry_with_reference_pixel(2, [-0.75, 0.0]),
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let source_shape = source_problem.model_lifecycle().target().clone();
    let seed = identity(8);
    let source = [
        ModelSample::valid(value(2.0)),
        ModelSample::valid(value(6.0)),
    ];
    let invalid_source = [ModelSample::valid(value(2.0)), ModelSample::invalid()];
    let expected = prepare_seed(seed, &source_shape, &target_shell, &source);

    let support_claim = problem_with_geometry(
        4,
        geometry_with_reference_pixel(2, [-0.75, 0.0]),
        ModelStateIdentity::Seed(seed),
        prepare_seed(seed, &source_shape, &target_shell, &invalid_source).lifecycle_requirements(),
        NumericPrecision::F64,
    );
    assert!(matches!(
        expected.bind_compiled_problem(support_claim),
        Err(ModelLifecycleError::SupportIdentityMismatch)
    ));

    assert!(matches!(
        problem_with_contract(
            4,
            geometry_with_reference_pixel(2, [-0.75, 0.0]),
            ModelStateIdentity::Seed(seed),
            prepare_seed(seed, &source_shape, &target_shell, &source).lifecycle_requirements(),
            NumericPrecision::F64,
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
            vec![PolarizationCoordinate::StokesI],
            vec![ProductKind::Weight],
        ),
        Err(CompileProblemError::ModelLifecycle(
            ModelContractError::ReprojectionContractMismatch
        ))
    ));

    let correct = problem_with_geometry(
        4,
        geometry_with_reference_pixel(2, [-0.75, 0.0]),
        ModelStateIdentity::Seed(seed),
        prepare_seed(seed, &source_shape, &target_shell, &source).lifecycle_requirements(),
        NumericPrecision::F64,
    );
    assert!(matches!(
        prepare_seed(identity(88), &source_shape, &target_shell, &source)
            .bind_compiled_problem(correct.clone()),
        Err(ModelLifecycleError::SourceProvenanceMismatch)
    ));

    let invalid_prepared = prepare_seed(seed, &source_shape, &target_shell, &invalid_source);
    assert_eq!(
        invalid_prepared.reprojection_id(),
        prepare_seed(seed, &source_shape, &target_shell, &source).reprojection_id()
    );
    assert_ne!(
        invalid_prepared.support_identity(),
        prepare_seed(seed, &source_shape, &target_shell, &source).support_identity()
    );
    assert!(matches!(
        invalid_prepared.bind_compiled_problem(correct),
        Err(ModelLifecycleError::SupportIdentityMismatch)
    ));
}

#[test]
fn owner_rejects_zero_noncanonical_unsupported_and_foreign_deltas() {
    assert!(ModelBounds::new(1, 1, 1, 0, 1.0, 1.0).is_err());
    let compiled = problem(
        5,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let first = bind_direct(&compiled, attempt(93), 1);
    let duplicate = bind_direct(&compiled, attempt(93), 1);
    let base = first.initial_empty().expect("base");
    assert!(matches!(
        first.compile_delta(&base, [ModelDeltaTerm::new(cell(0), value(0.0))]),
        Err(ModelLifecycleError::ZeroDeltaTerm)
    ));
    assert!(matches!(
        first.compile_delta(
            &base,
            [
                ModelDeltaTerm::new(cell(1), value(1.0)),
                ModelDeltaTerm::new(cell(0), value(1.0)),
            ],
        ),
        Err(ModelLifecycleError::NonCanonicalDelta)
    ));
    assert!(matches!(
        duplicate.compile_delta(&base, [ModelDeltaTerm::new(cell(0), value(1.0))]),
        Err(ModelLifecycleError::ForeignModelLifecycle)
    ));

    let seed = identity(9);
    let aligned = [ModelSample::valid(value(1.0)), ModelSample::invalid()];
    let support = model_support_identity(aligned.iter().map(|sample| sample.support()));
    let seeded = problem(
        5,
        2,
        ModelStateIdentity::Seed(seed),
        ModelLifecycleRequirements::new(
            bounds(),
            NumericPrecision::F64,
            ModelInputCommitment::AlignedSeed {
                source: seed,
                support,
            },
        ),
        NumericPrecision::F64,
    );
    let seeded_owner = bind_direct(&seeded, attempt(94), 1);
    let seeded_base = seeded_owner
        .ingest_aligned(
            seed,
            seeded_owner.contract().target(),
            aligned.into_iter().map(Ok::<_, ()>),
        )
        .expect("aligned source stream")
        .expect("seed generation");
    assert!(matches!(
        seeded_owner.compile_delta(&seeded_base, [ModelDeltaTerm::new(cell(1), value(1.0))],),
        Err(ModelLifecycleError::DeltaOutsideValidSupport)
    ));
}

#[test]
fn compiled_precision_governs_delta_arithmetic() {
    let f32_problem = problem(
        6,
        1,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F32),
        NumericPrecision::F32,
    );
    let f64_problem = problem(
        6,
        1,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let f32_owner = bind_direct(&f32_problem, attempt(95), 1);
    let f64_owner = bind_direct(&f64_problem, attempt(95), 1);
    let f32_base = f32_owner.initial_empty().expect("f32 base");
    let f64_base = f64_owner.initial_empty().expect("f64 base");
    let f32_seed_delta = f32_owner
        .compile_delta(
            &f32_base,
            [ModelDeltaTerm::new(cell(0), value(16_777_216.0))],
        )
        .expect("f32 seed delta");
    let f64_seed_delta = f64_owner
        .compile_delta(
            &f64_base,
            [ModelDeltaTerm::new(cell(0), value(16_777_216.0))],
        )
        .expect("f64 seed delta");
    let f32_base = f32_owner
        .apply_delta(f32_base, f32_seed_delta)
        .expect("f32 seed apply");
    let f64_base = f64_owner
        .apply_delta(f64_base, f64_seed_delta)
        .expect("f64 seed apply");
    let f32_delta = f32_owner
        .compile_delta(&f32_base, [ModelDeltaTerm::new(cell(0), value(1.0))])
        .expect("f32 delta");
    let f64_delta = f64_owner
        .compile_delta(&f64_base, [ModelDeltaTerm::new(cell(0), value(1.0))])
        .expect("f64 delta");
    let f32_next = f32_owner
        .apply_delta(f32_base, f32_delta)
        .expect("f32 apply");
    let f64_next = f64_owner
        .apply_delta(f64_base, f64_delta)
        .expect("f64 apply");
    assert_eq!(f32_next.samples()[0].value().value(), 16_777_216.0);
    assert_eq!(f64_next.samples()[0].value().value(), 16_777_217.0);
}

#[test]
fn resume_preserves_named_generation_then_enters_new_owner() {
    let initial_problem = problem(
        7,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let initial = bind_direct(&initial_problem, attempt(96), 1);
    let generation = initial.initial_empty().expect("initial generation");
    let generation_id = generation.generation_id();
    let resume_problem = problem(
        7,
        2,
        ModelStateIdentity::Generation(generation_id.identity()),
        ModelLifecycleRequirements::new(
            bounds(),
            NumericPrecision::F64,
            ModelInputCommitment::Generation(generation_id.identity()),
        ),
        NumericPrecision::F64,
    );
    let resumed_owner = bind_direct(&resume_problem, attempt(97), 2);
    let resumed = resumed_owner.resume(generation).expect("resume generation");
    assert_eq!(resumed.generation_id(), generation_id);
    let delta = resumed_owner
        .compile_delta(&resumed, [ModelDeltaTerm::new(cell(0), value(1.0))])
        .expect("compile resumed delta");
    let next = resumed_owner
        .apply_delta(resumed, delta)
        .expect("apply resumed delta");
    assert_eq!(next.samples()[0].value().value(), 1.0);
    assert!(matches!(
        next.origin(),
        ModelGenerationOrigin::Delta { base, .. } if base == generation_id
    ));

    let foreign_problem = problem(
        8,
        2,
        ModelStateIdentity::Empty,
        empty_requirements(NumericPrecision::F64),
        NumericPrecision::F64,
    );
    let foreign = bind_direct(&foreign_problem, attempt(98), 1)
        .initial_empty()
        .expect("foreign generation");
    assert!(matches!(
        resumed_owner.resume(foreign),
        Err(ModelLifecycleError::GenerationIdentityMismatch)
    ));
}
