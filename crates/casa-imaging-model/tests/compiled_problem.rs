// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompileObservationError, CompileProblemError, ContinuumChannelRole,
    ContinuumChannelUse, ContinuumFitRule, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, Epoch, FacetLayout,
    FiniteValuePolicy, FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, ImagingRequest, InstrumentModel, InstrumentResponse, ItrfPosition,
    JointContinuumLineContract, MeasurementEquationContract, MissingPointingPolicy,
    ModelColumnWrite, ModelInnerProduct, ModelStateIdentity, NumericPrecision, NumericalStage,
    NumericsContract, ObservationPointingLaw, ObservationSnapshotInput,
    ObservationTransactionRequirements, PairedMeasurementTransform, PhaseCentreLaw,
    PointingCentreLaw, PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemInputIdentities, ProblemSpecification, ProductAxisKind,
    ProductBeamRule, ProductBlankingPolicy, ProductKind, ProductNormalization, ProductRequirements,
    ProductRole, ProductSchema, ProductSupportComparison, ProductTerm, ProductUnit,
    ProductValidityPolicies, ProductValidityRule, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RequiredCapability, RestFrequency, RestoringBeamPolicy, ScientificContract,
    SequentialContinuumTransform, SkyDirection, SpectralContract, SpectralCoordinateSpec,
    SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeScale, UvTaper, UvwCoordinateLaw,
    VisibilityInnerProduct, WeightDensityScope, WeightingContract, WeightingScheme, compile,
    compile_observation,
};

mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use common::{identity, observation_source, problem_inputs};
use model_lifecycle_fixture::model_lifecycle;

fn compile_request(
    specification: ProblemSpecification,
    inputs: ProblemInputIdentities,
) -> Result<casa_imaging_model::CompiledProblem, CompileProblemError> {
    compile_with_geometry(specification, geometry(), inputs)
}

fn compile_with_geometry(
    specification: ProblemSpecification,
    geometry: GeometryInput,
    inputs: ProblemInputIdentities,
) -> Result<casa_imaging_model::CompiledProblem, CompileProblemError> {
    let lifecycle = model_lifecycle(inputs.model());
    compile(ImagingRequest::new(
        specification,
        geometry,
        inputs,
        lifecycle,
    ))
}

fn numerics(reverse: bool) -> NumericsContract {
    let mut precisions = vec![NumericPrecision::F32, NumericPrecision::F64];
    let mut budgets = NumericalStage::ALL
        .into_iter()
        .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
        .collect::<Vec<_>>();
    if reverse {
        precisions.reverse();
        budgets.reverse();
    }
    NumericsContract::new(
        precisions,
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        budgets,
    )
}

fn reconstruction() -> ReconstructionContract {
    ReconstructionContract::new(
        ReconstructionBasis::Taylor { terms: 2 },
        ReconstructionAlgorithm::Mtmfs {
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        },
        ReconstructionControls::new(100, 0.1, 0.0),
        PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
    )
}

fn inner_products() -> DeclaredInnerProducts {
    DeclaredInnerProducts::new(
        ModelInnerProduct::HermitianEuclidean,
        VisibilityInnerProduct::HermitianEuclidean,
    )
}

fn products(reverse: bool) -> ProductRequirements {
    products_with_beam(reverse, RestoringBeamPolicy::PerPlane)
}

fn products_with_beam(reverse: bool, restoring_beam: RestoringBeamPolicy) -> ProductRequirements {
    let mut products = vec![
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
        ProductKind::Sensitivity,
        ProductKind::TaylorTerms,
        ProductKind::SpectralIndex,
    ];
    if reverse {
        products.reverse();
    }
    ProductRequirements::new(
        products,
        ProductNormalization::FlatNoise,
        restoring_beam,
        product_validity(),
    )
}

fn product_validity() -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid primary-beam support"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor support"),
    )
}

fn science() -> ScientificContract {
    ScientificContract::new(
        SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
        MeasurementEquationContract::new(InstrumentResponse::Scalar, inner_products()),
    )
}

fn read_only_transaction() -> ObservationTransactionRequirements {
    ObservationTransactionRequirements::new(ModelColumnWrite::Disabled)
}

fn geometry() -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [255.0, 255.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
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
    )
}

fn joint_geometry() -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [31.0, 31.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(64, 64),
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
            SpectralWcs::Linear {
                channels: 8,
                reference_pixel: 3.5,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    )
}

fn joint_specification(contract: JointContinuumLineContract) -> ProblemSpecification {
    ProblemSpecification::new(
        science(),
        ReconstructionContract::new(
            ReconstructionBasis::JointContinuumLine {
                continuum_terms: 2,
                line_terms: 2,
            },
            ReconstructionAlgorithm::JointContinuumLine {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            ReconstructionControls::new(100, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        )
        .with_joint_continuum_line(contract),
        weighting(),
        ProductRequirements::new(
            vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::SumWeights,
            ],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        read_only_transaction(),
        numerics(false),
    )
}

#[test]
fn t46_joint_contract_is_canonical_identifiable_and_distinct() {
    let first = compile_with_geometry(
        joint_specification(JointContinuumLineContract::new(
            [0, 1, 2, 5, 6, 7],
            [3, 4],
            1.0e8,
        )),
        joint_geometry(),
        inputs(false),
    )
    .expect("compile identifiable joint contract");
    let reordered = compile_with_geometry(
        joint_specification(JointContinuumLineContract::new(
            [7, 2, 6, 0, 5, 1],
            [4, 3],
            1.0e8,
        )),
        joint_geometry(),
        inputs(false),
    )
    .expect("canonicalize support ordering");

    assert_eq!(first.problem_id(), reordered.problem_id());
    assert_eq!(
        first.reconstruction().joint_continuum_line(),
        reordered.reconstruction().joint_continuum_line()
    );
    assert!(
        first
            .required_capabilities()
            .contains(&RequiredCapability::JointContinuumLineReconstruction)
    );
    let graph = first.product_graph();
    assert!(
        graph
            .node(ProductRole::Residual(ProductTerm::Total))
            .is_some()
    );
    assert!(
        graph
            .node(ProductRole::Residual(ProductTerm::Line))
            .is_none()
    );
    for term in [
        ProductTerm::Continuum(0),
        ProductTerm::Continuum(1),
        ProductTerm::Line,
        ProductTerm::Total,
    ] {
        assert!(graph.node(ProductRole::Model(term)).is_some());
    }
    assert_eq!(
        graph
            .node(ProductRole::Model(ProductTerm::Continuum(0)))
            .expect("continuum coefficient product")
            .axes()
            .shape()[3],
        1
    );
    assert_eq!(
        graph
            .node(ProductRole::Model(ProductTerm::Line))
            .expect("line cube product")
            .axes()
            .shape()[3],
        8
    );
    assert_eq!(
        graph
            .nodes()
            .iter()
            .filter(|node| {
                matches!(
                    node.role(),
                    ProductRole::Psf(ProductTerm::JointNormal { .. })
                )
            })
            .count(),
        16
    );

    for invalid in [
        JointContinuumLineContract::new([], [0, 1, 2, 3, 4, 5, 6, 7], 1.0e8),
        JointContinuumLineContract::new([0, 1, 2, 3, 4, 5, 6, 7], [], 1.0e8),
        JointContinuumLineContract::new([0, 1, 2, 5, 6, 7], [2, 3], 1.0e8),
        JointContinuumLineContract::new([0], [1, 2, 3, 4, 5, 6, 7], 1.0e8),
        JointContinuumLineContract::new([0, 1, 2, 5, 6, 7], [3, 3], 1.0e8),
    ] {
        assert!(matches!(
            compile_with_geometry(
                joint_specification(invalid),
                joint_geometry(),
                inputs(false),
            ),
            Err(CompileProblemError::InvalidCapabilityCombination { .. })
        ));
    }
}

fn specification(reverse: bool) -> ProblemSpecification {
    ProblemSpecification::new(
        science(),
        reconstruction(),
        WeightingContract::new(
            WeightingScheme::Briggs { robust: 0.5 },
            WeightDensityScope::GlobalSelection,
        ),
        products(reverse),
        read_only_transaction(),
        numerics(reverse),
    )
}

fn weighting() -> WeightingContract {
    WeightingContract::new(
        WeightingScheme::Briggs { robust: 0.5 },
        WeightDensityScope::GlobalSelection,
    )
}

#[test]
fn prepared_cf_dependencies_exclude_solve_controls_but_retain_operator_science() {
    use casa_imaging_model::PreparedArtifactScientificKind::{
        ConvolutionFunction, Kernel, SpectralMap,
    };

    let make = |reconstruction, science, weighting, products, numerics, inputs| {
        compile_request(
            ProblemSpecification::new(
                science,
                reconstruction,
                weighting,
                products,
                read_only_transaction(),
                numerics,
            ),
            inputs,
        )
        .expect("prepared dependency fixture")
    };
    let baseline = make(
        reconstruction(),
        science(),
        weighting(),
        products(false),
        numerics(false),
        inputs(false),
    );
    let dependency = baseline.prepared_artifact_dependency_id(ConvolutionFunction);
    for (algorithm, controls) in [
        (
            ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            ReconstructionControls::new(0, 0.1, 0.0),
        ),
        (
            ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            ReconstructionControls::new(30, 0.2, 0.001),
        ),
    ] {
        let other = make(
            ReconstructionContract::new(
                ReconstructionBasis::Taylor { terms: 2 },
                algorithm,
                controls,
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            science(),
            weighting(),
            products(false),
            numerics(false),
            inputs(false),
        );
        assert_ne!(baseline.problem_id(), other.problem_id());
        assert_eq!(
            dependency,
            other.prepared_artifact_dependency_id(ConvolutionFunction)
        );
        for kind in [SpectralMap, Kernel] {
            assert_ne!(
                baseline.prepared_artifact_dependency_id(kind),
                other.prepared_artifact_dependency_id(kind)
            );
        }
    }
    let selected_products = ProductRequirements::new(
        vec![ProductKind::Psf],
        ProductNormalization::UnitResponse,
        RestoringBeamPolicy::None,
        product_validity(),
    );
    let publication = make(
        reconstruction(),
        science(),
        weighting(),
        selected_products,
        numerics(false),
        inputs(false),
    );
    assert_ne!(baseline.problem_id(), publication.problem_id());
    assert_eq!(
        dependency,
        publication.prepared_artifact_dependency_id(ConvolutionFunction)
    );

    let variants = [
        make(
            reconstruction(),
            ScientificContract::new(
                SpectralContract::new(SpectralSamplingLaw::LINEAR, SpectralCoupling::Independent),
                MeasurementEquationContract::new(InstrumentResponse::Scalar, inner_products()),
            ),
            weighting(),
            products(false),
            numerics(false),
            inputs(false),
        ),
        make(
            reconstruction(),
            science(),
            WeightingContract::new(
                WeightingScheme::Briggs { robust: -0.5 },
                WeightDensityScope::GlobalSelection,
            ),
            products(false),
            numerics(false),
            inputs(false),
        ),
        make(
            reconstruction(),
            science(),
            weighting(),
            products(false),
            NumericsContract::new(
                vec![NumericPrecision::F32, NumericPrecision::F64],
                ReductionPolicy::Compensated,
                FiniteValuePolicy::FlagInputRejectGenerated,
                NumericalStage::ALL
                    .into_iter()
                    .map(|stage| (stage, StageErrorBudget::new(2.0e-7, 1.0e-3)))
                    .collect(),
            ),
            inputs(false),
        ),
        make(
            reconstruction(),
            science(),
            weighting(),
            products(false),
            numerics(false),
            problem_inputs(
                2,
                vec![
                    (ReferenceDataKind::Measures, identity(3)),
                    (ReferenceDataKind::Ephemeris, identity(4)),
                ],
                ModelStateIdentity::Seed(identity(5)),
            ),
        ),
        make(
            reconstruction(),
            science(),
            weighting(),
            products(false),
            numerics(false),
            problem_inputs(
                1,
                vec![
                    (ReferenceDataKind::Measures, identity(8)),
                    (ReferenceDataKind::Ephemeris, identity(4)),
                ],
                ModelStateIdentity::Seed(identity(5)),
            ),
        ),
        compile_with_geometry(
            specification(false),
            geometry().with_domains(vec![geometry().domains()[0].clone().with_facets(
                FacetLayout::Regular {
                    columns: 2,
                    rows: 2,
                },
            )]),
            inputs(false),
        )
        .expect("changed geometry"),
    ];
    for other in variants {
        assert_ne!(
            dependency,
            other.prepared_artifact_dependency_id(ConvolutionFunction)
        );
    }
    let model = make(
        reconstruction(),
        science(),
        weighting(),
        products(false),
        numerics(false),
        problem_inputs(
            1,
            vec![
                (ReferenceDataKind::Measures, identity(3)),
                (ReferenceDataKind::Ephemeris, identity(4)),
            ],
            ModelStateIdentity::Seed(identity(9)),
        ),
    );
    assert_ne!(baseline.problem_id(), model.problem_id());
    assert_ne!(
        baseline.inputs().observation(),
        model.inputs().observation()
    );
    assert_ne!(
        dependency,
        model.prepared_artifact_dependency_id(ConvolutionFunction),
        "the retained observation snapshot includes its initial model generation"
    );
}

fn inputs(reverse: bool) -> ProblemInputIdentities {
    let mut references = vec![
        (ReferenceDataKind::Measures, identity(3)),
        (ReferenceDataKind::Ephemeris, identity(4)),
    ];
    if reverse {
        references.reverse();
    }
    problem_inputs(1, references, ModelStateIdentity::Seed(identity(5)))
}

fn inputs_with_instrument() -> ProblemInputIdentities {
    problem_inputs(
        1,
        vec![
            (ReferenceDataKind::Measures, identity(3)),
            (ReferenceDataKind::Ephemeris, identity(4)),
            (ReferenceDataKind::Instrument, identity(6)),
        ],
        ModelStateIdentity::Seed(identity(5)),
    )
}

fn compile_product_set(
    requested: Vec<ProductKind>,
    instrument_response: InstrumentResponse,
) -> Result<casa_imaging_model::CompiledProblem, CompileProblemError> {
    let restoring_beam = if requested.contains(&ProductKind::RestoredImage) {
        RestoringBeamPolicy::PerPlane
    } else {
        RestoringBeamPolicy::None
    };
    let inputs = if instrument_response == InstrumentResponse::Scalar {
        inputs(false)
    } else {
        inputs_with_instrument()
    };
    let science = ScientificContract::new(
        SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
        MeasurementEquationContract::new(instrument_response, inner_products()),
    );
    let science = if instrument_response == InstrumentResponse::PrimaryBeam {
        science.with_instrument_model(
            InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1,
        )
    } else {
        science
    };
    compile_request(
        ProblemSpecification::new(
            science,
            reconstruction(),
            weighting(),
            ProductRequirements::new(
                requested,
                ProductNormalization::UnitResponse,
                restoring_beam,
                product_validity(),
            ),
            read_only_transaction(),
            numerics(false),
        ),
        inputs,
    )
}

#[test]
fn equivalent_science_has_one_canonical_compiled_identity() {
    let first = compile_request(specification(false), inputs(false)).expect("compile first");
    let reordered = compile_request(specification(true), inputs(true)).expect("compile reordered");

    assert_eq!(first.problem_id(), reordered.problem_id());
    assert_eq!(
        first.required_capabilities(),
        reordered.required_capabilities()
    );
    assert_eq!(first.inputs(), reordered.inputs());
    assert_eq!(first.reconstruction(), reordered.reconstruction());
    assert_eq!(first.weighting(), reordered.weighting());
    assert_eq!(first.products(), reordered.products());
    assert_eq!(first.numerics(), reordered.numerics());
}

#[test]
fn compiler_owns_the_exact_product_graph_and_atomic_publication_contract() {
    let compiled = compile_request(specification(false), inputs(false)).expect("compile problem");
    let graph = compiled.product_graph();
    let reordered = compile_request(specification(true), inputs(true)).expect("compile reordered");

    assert_eq!(graph.graph_id(), reordered.product_graph().graph_id());
    assert_eq!(graph.schema_version(), 3);
    assert_eq!(
        graph
            .nodes()
            .iter()
            .filter_map(|node| node.name())
            .collect::<Vec<_>>(),
        vec![
            ".psf.tt0",
            ".psf.tt1",
            ".psf.tt2",
            ".residual.tt0",
            ".residual.tt1",
            ".model.tt0",
            ".model.tt1",
            ".image.tt0",
            ".image.tt1",
            ".sumwt.tt0",
            ".sumwt.tt1",
            ".sumwt.tt2",
            ".sensitivity",
            ".alpha",
        ]
    );

    let restored = graph
        .nodes()
        .iter()
        .find(|node| node.role() == ProductRole::RestoredImage(ProductTerm::Taylor(0)))
        .expect("restored Taylor-zero node");
    assert_eq!(restored.axes().kind(), ProductAxisKind::SkyImage);
    assert_eq!(restored.axes().shape(), [512, 512, 1, 1]);
    assert_eq!(restored.unit(), ProductUnit::JyPerBeam);
    assert_eq!(
        restored.beam(),
        ProductBeamRule::Restoring(RestoringBeamPolicy::PerPlane)
    );
    assert_eq!(
        restored.validity(),
        ProductValidityRule::PrimaryBeam(product_validity().primary_beam())
    );
    assert_eq!(restored.schema(), ProductSchema::ImageF32V1);

    let spectral_index = graph
        .nodes()
        .iter()
        .find(|node| node.role() == ProductRole::SpectralIndex)
        .expect("spectral-index node");
    assert_eq!(spectral_index.unit(), ProductUnit::Dimensionless);
    assert_eq!(
        spectral_index.validity(),
        ProductValidityRule::Taylor(product_validity().taylor())
    );
    let mut alpha_sources = [0, 1]
        .into_iter()
        .flat_map(|term| {
            [
                graph
                    .node(ProductRole::Residual(ProductTerm::Taylor(term)))
                    .expect("principal-residual Taylor node")
                    .node_id(),
                graph
                    .node(ProductRole::RestoredImage(ProductTerm::Taylor(term)))
                    .expect("restored-image Taylor node")
                    .node_id(),
            ]
        })
        .collect::<Vec<_>>();
    alpha_sources.sort_unstable();
    assert_eq!(spectral_index.dependencies(), alpha_sources);
    assert!(spectral_index.dependencies().iter().all(|dependency| {
        !matches!(
            graph.nodes()[dependency.ordinal()].role(),
            ProductRole::Model(_)
        )
    }));

    assert_eq!(
        graph.publication().members(),
        graph
            .nodes()
            .iter()
            .filter(|node| node.schema() == ProductSchema::ImageF32V1)
            .map(|node| node.node_id())
            .collect::<Vec<_>>()
    );
    assert!(graph.publication().protocol().requires_durable_prepare());
    assert!(
        graph
            .publication()
            .protocol()
            .has_one_visibility_operation_per_member()
    );
    assert!(
        graph
            .publication()
            .protocol()
            .preserves_promoted_members_on_later_failure()
    );
}

#[test]
fn unit_response_primary_beam_validity_is_explicit_request_semantics() {
    let validity = product_validity()
        .with_unit_response(casa_imaging_model::UnitResponseValidityPolicy::PrimaryBeam);
    let compiled = compile_request(
        ProblemSpecification::new(
            science(),
            reconstruction(),
            weighting(),
            ProductRequirements::new(
                vec![
                    ProductKind::Residual,
                    ProductKind::Model,
                    ProductKind::RestoredImage,
                    ProductKind::PrimaryBeam,
                ],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::PerPlane,
                validity,
            ),
            read_only_transaction(),
            numerics(false),
        ),
        inputs(false),
    )
    .expect("compile explicit primary-beam validity");

    for role in [
        ProductRole::Residual(ProductTerm::Taylor(0)),
        ProductRole::RestoredImage(ProductTerm::Taylor(0)),
    ] {
        assert_eq!(
            compiled
                .product_graph()
                .node(role)
                .expect("uncorrected product")
                .validity(),
            ProductValidityRule::PrimaryBeam(validity.primary_beam()),
        );
    }
}

#[test]
fn publishing_a_primary_beam_does_not_change_uncorrected_product_validity() {
    let compiled = compile_product_set(
        vec![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::PrimaryBeam,
        ],
        InstrumentResponse::PrimaryBeam,
    )
    .expect("compile unit-response products with a published primary beam");
    let graph = compiled.product_graph();

    for role in [
        ProductRole::Residual(ProductTerm::Taylor(0)),
        ProductRole::RestoredImage(ProductTerm::Taylor(0)),
    ] {
        assert_eq!(
            graph.node(role).expect("uncorrected product").validity(),
            ProductValidityRule::FinalNormalState,
        );
    }
    assert_eq!(
        graph
            .node(ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
            .expect("published primary beam")
            .validity(),
        ProductValidityRule::PrimaryBeam(product_validity().primary_beam()),
    );
}

#[test]
fn product_graph_identity_is_content_derived_and_stable_across_unrelated_problem_inputs() {
    let first = compile_request(specification(false), inputs(false)).expect("compile first");
    let different_numerics = compile_request(
        ProblemSpecification::new(
            science(),
            reconstruction(),
            weighting(),
            products(false),
            read_only_transaction(),
            NumericsContract::new(
                vec![NumericPrecision::F32, NumericPrecision::F64],
                ReductionPolicy::DeterministicPairwise,
                FiniteValuePolicy::FlagInputRejectGenerated,
                NumericalStage::ALL
                    .into_iter()
                    .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                    .collect(),
            ),
        ),
        inputs(false),
    )
    .expect("compile with different numerics");
    let different_products = compile_request(
        ProblemSpecification::new(
            science(),
            reconstruction(),
            weighting(),
            ProductRequirements::new(
                products(false)
                    .products()
                    .iter()
                    .copied()
                    .chain([ProductKind::Mask])
                    .collect(),
                ProductNormalization::FlatNoise,
                RestoringBeamPolicy::PerPlane,
                product_validity(),
            ),
            read_only_transaction(),
            numerics(false),
        ),
        inputs(false),
    )
    .expect("compile with different product topology");

    assert_ne!(first.problem_id(), different_numerics.problem_id());
    assert_eq!(
        first.product_graph().graph_id(),
        different_numerics.product_graph().graph_id()
    );
    assert_ne!(
        first.product_graph().graph_id(),
        different_products.product_graph().graph_id()
    );
    assert_eq!(
        first.product_graph().graph_id().as_bytes(),
        [
            139, 130, 57, 178, 38, 63, 150, 135, 182, 215, 213, 237, 156, 152, 40, 187, 235, 26,
            221, 172, 147, 63, 210, 224, 10, 236, 117, 22, 5, 151, 235, 95,
        ]
    );
}

#[test]
fn spectral_index_error_and_pb_correction_name_every_scientific_input() {
    let products = ProductRequirements::new(
        products(false)
            .products()
            .iter()
            .copied()
            .chain([
                ProductKind::PrimaryBeam,
                ProductKind::SpectralIndexError,
                ProductKind::PbCorrectedSpectralIndex,
            ])
            .collect(),
        ProductNormalization::FlatNoise,
        RestoringBeamPolicy::PerPlane,
        product_validity(),
    );
    let compiled = compile_request(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
                MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam, inner_products()),
            )
            .with_instrument_model(
                InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1,
            ),
            reconstruction(),
            weighting(),
            products,
            read_only_transaction(),
            numerics(false),
        ),
        inputs_with_instrument(),
    )
    .expect("compile PB-corrected Taylor products");
    let graph = compiled.product_graph();
    let alpha = graph
        .node(ProductRole::SpectralIndex)
        .expect("spectral-index product");
    let mut alpha_sources = alpha.dependencies().to_vec();
    let alpha_error = graph
        .node(ProductRole::SpectralIndexError)
        .expect("spectral-index-error product");
    alpha_sources.push(alpha.node_id());
    alpha_sources.sort_unstable();
    assert_eq!(alpha_error.dependencies(), alpha_sources);

    let pb_alpha = graph
        .node(ProductRole::PrimaryBeamSpectralIndex)
        .expect("internal primary-beam spectral index");
    assert_eq!(pb_alpha.name(), None);
    assert_eq!(pb_alpha.schema(), ProductSchema::InternalImageF32V1);
    assert_eq!(
        pb_alpha.dependencies(),
        [graph
            .node(ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
            .expect("primary-beam Taylor-zero product")
            .node_id()]
    );
    assert!(!graph.publication().members().contains(&pb_alpha.node_id()));
    assert_eq!(
        graph
            .node(ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
            .expect("primary-beam Taylor-zero product")
            .validity(),
        ProductValidityRule::PrimaryBeam(product_validity().primary_beam())
    );
    assert_eq!(
        graph
            .node(ProductRole::PrimaryBeam(ProductTerm::Taylor(1)))
            .expect("primary-beam Taylor-one product")
            .validity(),
        ProductValidityRule::All
    );
    assert!(
        graph
            .nodes()
            .iter()
            .all(|node| node.name() != Some(".pb.alpha"))
    );

    let corrected_alpha = graph
        .node(ProductRole::PbCorrectedSpectralIndex)
        .expect("PB-corrected spectral index");
    assert!(corrected_alpha.dependencies().contains(&pb_alpha.node_id()));
    assert!(
        graph
            .publication()
            .members()
            .contains(&corrected_alpha.node_id())
    );
}

#[test]
fn model_column_side_effects_are_compiled_into_problem_identity() {
    let read_only =
        compile_request(specification(false), inputs(false)).expect("compile read-only");
    let writable = compile_request(
        ProblemSpecification::new(
            science(),
            reconstruction(),
            weighting(),
            products(false),
            ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
            numerics(false),
        ),
        inputs(false),
    )
    .expect("compile model-column write");

    assert_ne!(read_only.problem_id(), writable.problem_id());
    assert_eq!(
        writable.observation_transaction().observation_snapshot_id(),
        writable.inputs().observation()
    );
    assert_eq!(
        writable
            .observation_transaction()
            .write_set()
            .visibility_columns()
            .len(),
        1
    );
}

#[test]
fn derived_capabilities_cover_normalization_without_naming_a_backend() {
    let compiled = compile_request(specification(false), inputs(false)).expect("compile problem");

    assert!(
        compiled
            .required_capabilities()
            .contains(&RequiredCapability::FlatNoiseNormalization)
    );
    assert!(
        compiled
            .required_capabilities()
            .contains(&RequiredCapability::MtmfsReconstruction)
    );
    assert!(
        compiled
            .required_capabilities()
            .contains(&RequiredCapability::BriggsWeighting)
    );
    assert!(
        compiled
            .required_capabilities()
            .contains(&RequiredCapability::Product(ProductKind::SpectralIndex))
    );
}

#[test]
fn natural_weighting_rejects_a_meaningless_per_channel_density_scope() {
    let specification = ProblemSpecification::new(
        science(),
        reconstruction(),
        WeightingContract::new(
            WeightingScheme::Natural,
            WeightDensityScope::PerOutputChannel,
        ),
        products(false),
        read_only_transaction(),
        numerics(false),
    );

    assert!(matches!(
        compile_request(specification, inputs(false)),
        Err(CompileProblemError::InvalidWeighting { .. })
    ));
}

#[test]
fn natural_weighting_declares_that_density_generation_is_not_applicable() {
    let specification = ProblemSpecification::new(
        science(),
        reconstruction(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products(false),
        read_only_transaction(),
        numerics(false),
    );

    let compiled = compile_request(specification, inputs(false)).expect("natural weighting");
    assert!(
        compiled
            .required_capabilities()
            .contains(&RequiredCapability::NaturalWeighting)
    );
}

#[test]
fn incompatible_reconstruction_capabilities_fail_before_execution_inputs_exist() {
    let specification = ProblemSpecification::new(
        science(),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            ReconstructionControls::new(100, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        weighting(),
        products(false),
        read_only_transaction(),
        numerics(false),
    );

    assert!(matches!(
        compile_request(specification, inputs(false)),
        Err(CompileProblemError::InvalidCapabilityCombination { .. })
    ));
}

#[test]
fn channel_local_basis_must_match_compiled_geometry_channels() {
    let channel_local = |channels| {
        ProblemSpecification::new(
            science(),
            ReconstructionContract::new(
                ReconstructionBasis::ChannelLocal { channels },
                ReconstructionAlgorithm::Dirty,
                ReconstructionControls::new(0, 1.0, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            weighting(),
            ProductRequirements::new(
                vec![
                    ProductKind::Psf,
                    ProductKind::Residual,
                    ProductKind::SumWeights,
                ],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                product_validity(),
            ),
            read_only_transaction(),
            numerics(false),
        )
    };
    let two_channel_geometry =
        geometry().with_spectral(geometry().spectral().clone().with_wcs(SpectralWcs::Linear {
            channels: 2,
            reference_pixel: 0.0,
            reference_frequency_hz: 1.4e9,
            increment_hz: 1.0e6,
        }));

    compile_with_geometry(
        channel_local(2),
        two_channel_geometry.clone(),
        inputs(false),
    )
    .expect("matching channel-local geometry");
    assert!(matches!(
        compile_with_geometry(channel_local(1), two_channel_geometry, inputs(false)),
        Err(CompileProblemError::SpectralChannelCountMismatch {
            geometry_channels: 2,
            reconstruction_channels: 1,
        })
    ));
}

#[test]
fn t41_taylor_via_channel_major_is_distinct_and_channel_bounded() {
    let contract = |basis| {
        ProblemSpecification::new(
            science(),
            ReconstructionContract::new(
                basis,
                ReconstructionAlgorithm::Mtmfs {
                    scales_px: vec![0.0],
                    small_scale_bias: 0.0,
                },
                ReconstructionControls::new(100, 0.1, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            weighting(),
            products(false),
            read_only_transaction(),
            numerics(false),
        )
    };
    let four_channel_geometry =
        geometry().with_spectral(geometry().spectral().clone().with_wcs(SpectralWcs::Linear {
            channels: 4,
            reference_pixel: 1.5,
            reference_frequency_hz: 1.4e9,
            increment_hz: 1.0e6,
        }));
    let dual = compile_with_geometry(
        contract(ReconstructionBasis::TaylorViaChannelMajor {
            terms: 2,
            channels: 4,
        }),
        four_channel_geometry.clone(),
        inputs(false),
    )
    .expect("four channels support two Taylor terms");
    let direct = compile_with_geometry(
        contract(ReconstructionBasis::Taylor { terms: 2 }),
        four_channel_geometry.clone(),
        inputs(false),
    )
    .expect("direct Taylor contract compiles independently of its implementation route");
    assert_eq!(
        dual.reconstruction().basis(),
        ReconstructionBasis::TaylorViaChannelMajor {
            terms: 2,
            channels: 4,
        }
    );
    assert!(
        dual.product_graph()
            .nodes()
            .iter()
            .filter(|node| node.schema() == ProductSchema::ImageF32V1)
            .all(|node| node.axes().shape()[3] == 1),
        "public MT-MFS products retain singleton Taylor axes while the major cycle uses channels"
    );
    assert_ne!(dual.problem_id(), direct.problem_id());
    assert!(matches!(
        compile_with_geometry(
            contract(ReconstructionBasis::TaylorViaChannelMajor {
                terms: 2,
                channels: 3,
            }),
            four_channel_geometry.clone(),
            inputs(false),
        ),
        Err(CompileProblemError::SpectralChannelCountMismatch {
            geometry_channels: 4,
            reconstruction_channels: 3,
        })
    ));
    assert!(matches!(
        compile_with_geometry(
            contract(ReconstructionBasis::TaylorViaChannelMajor {
                terms: 5,
                channels: 4,
            }),
            four_channel_geometry,
            inputs(false),
        ),
        Err(CompileProblemError::InvalidCapabilityCombination { .. })
    ));
}

#[test]
fn one_term_mfs_uses_the_constant_basis_instead_of_taylor() {
    let specification = ProblemSpecification::new(
        science(),
        ReconstructionContract::new(
            ReconstructionBasis::Taylor { terms: 1 },
            ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            ReconstructionControls::new(100, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        weighting(),
        products(false),
        read_only_transaction(),
        numerics(false),
    );

    assert!(matches!(
        compile_request(specification, inputs(false)),
        Err(CompileProblemError::InvalidCapabilityCombination { .. })
    ));
}

#[test]
fn flat_normalization_without_sensitivity_fails_at_compile_time() {
    let specification = ProblemSpecification::new(
        science(),
        reconstruction(),
        weighting(),
        ProductRequirements::new(
            vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::SumWeights,
                ProductKind::TaylorTerms,
                ProductKind::SpectralIndex,
            ],
            ProductNormalization::FlatNoise,
            RestoringBeamPolicy::PerPlane,
            product_validity(),
        ),
        read_only_transaction(),
        numerics(false),
    );

    assert!(matches!(
        compile_request(specification, inputs(false)),
        Err(CompileProblemError::InvalidNormalizationCombination { .. })
    ));
}

#[test]
fn incomplete_or_non_finite_numerics_fail_at_compile_time() {
    let mut incomplete = NumericalStage::ALL
        .into_iter()
        .map(|stage| (stage, StageErrorBudget::new(0.0, 1.0e-3)))
        .collect::<Vec<_>>();
    incomplete.pop();
    let incomplete_specification = ProblemSpecification::new(
        science(),
        reconstruction(),
        weighting(),
        products(false),
        read_only_transaction(),
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::DeterministicPairwise,
            FiniteValuePolicy::RejectAll,
            incomplete,
        ),
    );
    assert!(matches!(
        compile_request(incomplete_specification, inputs(false)),
        Err(CompileProblemError::InvalidNumerics { .. })
    ));

    let non_finite = NumericalStage::ALL
        .into_iter()
        .map(|stage| {
            let relative = if stage == NumericalStage::Reductions {
                f64::NAN
            } else {
                1.0e-3
            };
            (stage, StageErrorBudget::new(0.0, relative))
        })
        .collect();
    let non_finite_specification = ProblemSpecification::new(
        science(),
        reconstruction(),
        weighting(),
        products(false),
        read_only_transaction(),
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            non_finite,
        ),
    );
    assert!(matches!(
        compile_request(non_finite_specification, inputs(false)),
        Err(CompileProblemError::InvalidNumerics { .. })
    ));
}

#[test]
fn every_derived_product_requires_a_closed_scientific_source_set() {
    let cases = [
        (
            "restored image without residual",
            vec![ProductKind::Model, ProductKind::RestoredImage],
            InstrumentResponse::Scalar,
        ),
        (
            "restored image without model",
            vec![ProductKind::Residual, ProductKind::RestoredImage],
            InstrumentResponse::Scalar,
        ),
        (
            "PB-corrected image without restored image",
            vec![ProductKind::PrimaryBeam, ProductKind::PbCorrectedImage],
            InstrumentResponse::PrimaryBeam,
        ),
        (
            "Taylor collection without a Taylor image",
            vec![ProductKind::TaylorTerms],
            InstrumentResponse::Scalar,
        ),
        (
            "spectral index without Taylor collection",
            vec![
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::SpectralIndex,
            ],
            InstrumentResponse::Scalar,
        ),
        (
            "spectral index without residual",
            vec![
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::TaylorTerms,
                ProductKind::SpectralIndex,
            ],
            InstrumentResponse::Scalar,
        ),
        (
            "spectral index without restored image",
            vec![
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::TaylorTerms,
                ProductKind::SpectralIndex,
            ],
            InstrumentResponse::Scalar,
        ),
        (
            "spectral-index error without spectral index",
            vec![
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::TaylorTerms,
                ProductKind::SpectralIndexError,
            ],
            InstrumentResponse::Scalar,
        ),
        (
            "PB-corrected spectral index without spectral index",
            vec![
                ProductKind::PrimaryBeam,
                ProductKind::PbCorrectedSpectralIndex,
            ],
            InstrumentResponse::PrimaryBeam,
        ),
        (
            "PB-corrected spectral index without primary beam",
            vec![
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::TaylorTerms,
                ProductKind::SpectralIndex,
                ProductKind::PbCorrectedSpectralIndex,
            ],
            InstrumentResponse::PrimaryBeam,
        ),
        (
            "beam metadata without a beam-bearing image",
            vec![ProductKind::Model, ProductKind::Beam],
            InstrumentResponse::Scalar,
        ),
    ];

    for (case, requested, response) in cases {
        assert!(
            matches!(
                compile_product_set(requested, response),
                Err(CompileProblemError::InvalidProductCombination { .. })
            ),
            "{case} must fail before Product Graph construction"
        );
    }
}

#[test]
fn taylor_collection_accepts_an_explicit_taylor_image_source() {
    let compiled = compile_product_set(
        vec![ProductKind::Psf, ProductKind::TaylorTerms],
        InstrumentResponse::Scalar,
    )
    .expect("Taylor PSF terms form a nonempty coefficient collection");
    let graph = compiled.product_graph();
    let collection = graph
        .node(ProductRole::TaylorCoefficientSet)
        .expect("Taylor collection");

    assert!(!collection.dependencies().is_empty());
    assert!(collection.dependencies().iter().all(|dependency| {
        matches!(
            graph.nodes()[dependency.ordinal()].role(),
            ProductRole::Psf(ProductTerm::Taylor(_))
        )
    }));
}

#[test]
fn duplicate_reference_families_are_rejected_instead_of_ordered_accidentally() {
    assert_eq!(
        compile_observation(ObservationSnapshotInput::new(
            vec![observation_source(1)],
            vec![
                (ReferenceDataKind::Measures, identity(3)),
                (ReferenceDataKind::Measures, identity(4)),
            ],
            ModelStateIdentity::Empty,
        )),
        Err(CompileObservationError::DuplicateReferenceData {
            kind: ReferenceDataKind::Measures,
        })
    );
}

#[test]
fn canonical_identity_normalizes_signed_zero_but_changes_with_science() {
    let with_robust = |robust| {
        ProblemSpecification::new(
            science(),
            reconstruction(),
            WeightingContract::new(
                WeightingScheme::Briggs { robust },
                WeightDensityScope::GlobalSelection,
            ),
            products(false),
            read_only_transaction(),
            numerics(false),
        )
    };
    let negative_zero = compile_request(with_robust(-0.0), inputs(false)).expect("negative zero");
    let positive_zero = compile_request(with_robust(0.0), inputs(false)).expect("positive zero");
    let changed = compile_request(with_robust(0.5), inputs(false)).expect("changed science");

    assert_eq!(negative_zero.problem_id(), positive_zero.problem_id());
    assert_eq!(
        negative_zero.weighting().commitment_id(),
        positive_zero.weighting().commitment_id()
    );
    assert_ne!(positive_zero.problem_id(), changed.problem_id());
    assert_eq!(casa_imaging_model::CompiledProblemId::SCHEMA_VERSION, 23);
}

#[test]
fn numerics_contract_has_an_independent_stable_identity() {
    let first = compile_request(specification(false), inputs(false)).expect("compile first");
    let reordered = compile_request(specification(true), inputs(true)).expect("compile reordered");
    assert_eq!(first.numerics_id(), reordered.numerics_id());
    assert_eq!(
        first.numerics_id().as_bytes(),
        [
            248, 232, 21, 246, 91, 89, 229, 141, 87, 199, 232, 27, 197, 224, 106, 80, 183, 210,
            185, 125, 118, 131, 243, 133, 31, 193, 111, 19, 68, 150, 117, 86,
        ]
    );

    let mut changed = numerics(false);
    changed = NumericsContract::new(
        changed.permitted_precisions().to_vec(),
        ReductionPolicy::DeterministicPairwise,
        changed.finite_values(),
        changed.stage_error_budgets().to_vec(),
    );
    let changed = compile_request(
        ProblemSpecification::new(
            science(),
            reconstruction(),
            weighting(),
            products(false),
            read_only_transaction(),
            changed,
        ),
        inputs(false),
    )
    .expect("compile changed numerics");

    assert_ne!(first.numerics_id(), changed.numerics_id());
    assert_eq!(casa_imaging_model::NumericsContractId::SCHEMA_VERSION, 1);
}

#[test]
fn multiscale_order_and_duplicate_scales_do_not_change_scientific_identity() {
    let specification = |scales_px| {
        ProblemSpecification::new(
            science(),
            ReconstructionContract::new(
                ReconstructionBasis::Constant,
                ReconstructionAlgorithm::Multiscale {
                    scales_px,
                    small_scale_bias: 0.6,
                },
                ReconstructionControls::new(100, 0.1, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            weighting(),
            ProductRequirements::new(
                vec![
                    ProductKind::Psf,
                    ProductKind::Residual,
                    ProductKind::Model,
                    ProductKind::RestoredImage,
                    ProductKind::SumWeights,
                ],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::PerPlane,
                product_validity(),
            ),
            read_only_transaction(),
            numerics(false),
        )
    };
    let canonical = compile_request(specification(vec![0.0, 3.0, 10.0]), inputs(false))
        .expect("canonical scales");
    let reordered = compile_request(specification(vec![10.0, 3.0, -0.0, 3.0]), inputs(false))
        .expect("reordered scales");

    assert_eq!(canonical.problem_id(), reordered.problem_id());
    assert_eq!(canonical.reconstruction(), reordered.reconstruction());
}

#[test]
fn mtmfs_scales_are_canonical_and_part_of_scientific_identity() {
    let make = |scales_px| {
        ProblemSpecification::new(
            science(),
            ReconstructionContract::new(
                ReconstructionBasis::Taylor { terms: 2 },
                ReconstructionAlgorithm::Mtmfs {
                    scales_px,
                    small_scale_bias: 0.3,
                },
                ReconstructionControls::new(100, 0.1, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            weighting(),
            products(false),
            read_only_transaction(),
            numerics(false),
        )
    };
    let canonical = compile_request(make(vec![0.0, 3.0]), inputs(false)).expect("MT-MFS scales");
    let reordered = compile_request(make(vec![3.0, -0.0, 3.0]), inputs(false))
        .expect("canonical MT-MFS scales");
    let changed = compile_request(make(vec![0.0, 5.0]), inputs(false)).expect("changed scales");

    assert_eq!(canonical.problem_id(), reordered.problem_id());
    assert_ne!(canonical.problem_id(), changed.problem_id());
}

#[test]
fn complete_science_contract_changes_identity_and_capabilities() {
    let make = |science, weighting, products| {
        ProblemSpecification::new(
            science,
            reconstruction(),
            weighting,
            products,
            read_only_transaction(),
            numerics(false),
        )
    };
    let baseline = compile_request(make(science(), weighting(), products(false)), inputs(false))
        .expect("baseline");
    let tagged_scalar = compile_request(
        make(
            science().with_instrument_model(
                InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1,
            ),
            weighting(),
            products(false),
        ),
        inputs(false),
    );
    assert!(matches!(
        tagged_scalar,
        Err(CompileProblemError::InvalidScientificContract {
            reason: "instrument response and instrument model must form one supported exact pair"
        })
    ));
    let widefield_science = ScientificContract::new(
        SpectralContract::new(
            SpectralSamplingLaw::LINEAR,
            SpectralCoupling::CommonRestoringBeam,
        ),
        MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam, inner_products()),
    )
    .with_instrument_model(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1);
    let widefield_geometry = geometry()
        .with_domains(vec![geometry().domains()[0].clone().with_facets(
            FacetLayout::Regular {
                columns: 2,
                rows: 2,
            },
        )])
        .with_spectral(
            geometry()
                .spectral()
                .clone()
                .with_output_frame(FrequencyFrame::Lsrk)
                .with_anchor(SpectralFrameAnchor::Conversion {
                    epoch: Epoch::new(59_000.0, TimeScale::Utc),
                    direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    observatory_position: ItrfPosition::new(
                        -1_601_188.0,
                        -5_041_977.0,
                        3_554_875.0,
                    ),
                })
                .with_rest_frequency(RestFrequency::Line {
                    hertz: 1.420_405_751_77e9,
                })
                .with_doppler_convention(DopplerConvention::Radio),
        );
    let tapered = WeightingContract::new(
        WeightingScheme::Briggs { robust: 0.5 },
        WeightDensityScope::GlobalSelection,
    )
    .with_uv_taper(UvTaper::new(12_000.0, 8_000.0, 0.25));
    let widefield = compile_with_geometry(
        make(
            widefield_science,
            tapered,
            products_with_beam(false, RestoringBeamPolicy::Common),
        ),
        widefield_geometry,
        inputs_with_instrument(),
    )
    .expect("widefield science");

    assert_ne!(baseline.problem_id(), widefield.problem_id());
    for capability in [
        RequiredCapability::FacetedGeometry,
        RequiredCapability::SpectralFrameTransform,
        RequiredCapability::SpectralResampling,
        RequiredCapability::CommonBeamSpectralCoupling,
        RequiredCapability::PrimaryBeamResponse,
        RequiredCapability::UvTaper,
        RequiredCapability::Polarization(PolarizationCoordinate::StokesI),
    ] {
        assert!(widefield.required_capabilities().contains(&capability));
    }
}

#[test]
fn primary_beam_response_requires_exact_model_and_instrument_identity() {
    let direction_dependent = ScientificContract::new(
        SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
        MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam, inner_products()),
    );
    let specification = |science| {
        ProblemSpecification::new(
            science,
            reconstruction(),
            weighting(),
            products(false),
            read_only_transaction(),
            numerics(false),
        )
    };

    assert!(matches!(
        compile_request(
            specification(direction_dependent.clone()),
            inputs_with_instrument()
        ),
        Err(CompileProblemError::InvalidScientificContract {
            reason: "instrument response and instrument model must form one supported exact pair"
        })
    ));

    let direction_dependent = direction_dependent
        .with_instrument_model(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1);
    assert_eq!(
        direction_dependent.instrument_model(),
        Some(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1)
    );
    assert!(matches!(
        compile_request(specification(direction_dependent.clone()), inputs(false)),
        Err(CompileProblemError::InvalidScientificContract {
            reason: "direction-dependent response requires bound instrument reference data"
        })
    ));

    let compiled = compile_request(specification(direction_dependent), inputs_with_instrument())
        .expect("compile exact primary-beam instrument model");
    assert!(
        compiled
            .normal_equation()
            .measurement_operator()
            .transforms()
            .contains(&PairedMeasurementTransform::DirectionDependentResponse {
                response: InstrumentResponse::PrimaryBeam,
                instrument_model: Some(
                    InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1
                ),
            })
    );
}

#[test]
fn sequential_continuum_transform_is_a_compiled_capability_and_identity_input() {
    let base = || {
        ProblemSpecification::new(
            science(),
            ReconstructionContract::new(
                ReconstructionBasis::ChannelLocal { channels: 1 },
                ReconstructionAlgorithm::Dirty,
                ReconstructionControls::new(0, 1.0, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                vec![
                    ProductKind::Psf,
                    ProductKind::Residual,
                    ProductKind::SumWeights,
                ],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                product_validity(),
            ),
            read_only_transaction(),
            numerics(false),
        )
    };
    let transform = SequentialContinuumTransform::new(vec![
        ContinuumFitRule::new(
            0,
            0,
            0,
            vec![ContinuumChannelRole::new(
                0,
                ContinuumChannelUse::FitAndApply,
            )],
        )
        .expect("fit/apply rule"),
    ])
    .expect("transform");
    let plain = compile_request(base(), inputs(false)).expect("plain problem");
    let transformed = compile_request(
        base().with_visibility_transform(transform.clone()),
        inputs(false),
    )
    .expect("transformed problem");

    assert_eq!(transformed.visibility_transform(), Some(&transform));
    assert!(
        transformed
            .required_capabilities()
            .contains(&RequiredCapability::SequentialContinuumTransform)
    );
    assert_ne!(plain.problem_id(), transformed.problem_id());
    assert_ne!(
        plain.prepared_artifact_dependency_id(
            casa_imaging_model::PreparedArtifactScientificKind::ConvolutionFunction
        ),
        transformed.prepared_artifact_dependency_id(
            casa_imaging_model::PreparedArtifactScientificKind::ConvolutionFunction
        ),
    );
}

#[test]
fn dirty_reconstruction_rejects_scientifically_unused_controls() {
    let dirty = |gain, threshold| {
        ProblemSpecification::new(
            science(),
            ReconstructionContract::new(
                ReconstructionBasis::Constant,
                ReconstructionAlgorithm::Dirty,
                ReconstructionControls::new(0, gain, threshold),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            weighting(),
            ProductRequirements::new(
                vec![
                    ProductKind::Psf,
                    ProductKind::Residual,
                    ProductKind::SumWeights,
                ],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                product_validity(),
            ),
            read_only_transaction(),
            numerics(false),
        )
    };

    compile_request(dirty(1.0, 0.0), inputs(false)).expect("canonical dirty problem");
    for specification in [dirty(0.1, 0.0), dirty(1.0, 0.5)] {
        assert!(matches!(
            compile_request(specification, inputs(false)),
            Err(CompileProblemError::InvalidCapabilityCombination {
                reason: "dirty reconstruction requires canonical inactive controls: gain 1 and threshold 0"
            })
        ));
    }
}

#[test]
fn spectral_coupling_and_restoring_beam_policy_must_agree() {
    let science_with_coupling = |coupling| {
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, coupling),
            MeasurementEquationContract::new(InstrumentResponse::Scalar, inner_products()),
        )
    };
    let compile = |coupling, beam| {
        compile_request(
            ProblemSpecification::new(
                science_with_coupling(coupling),
                reconstruction(),
                weighting(),
                products_with_beam(false, beam),
                read_only_transaction(),
                numerics(false),
            ),
            inputs(false),
        )
    };

    assert!(matches!(
        compile(
            SpectralCoupling::CommonRestoringBeam,
            RestoringBeamPolicy::PerPlane
        ),
        Err(CompileProblemError::InvalidProductCombination { .. })
    ));
    assert!(matches!(
        compile(SpectralCoupling::Independent, RestoringBeamPolicy::Common),
        Err(CompileProblemError::InvalidProductCombination { .. })
    ));
    compile(
        SpectralCoupling::CommonRestoringBeam,
        RestoringBeamPolicy::Common,
    )
    .expect("matching common-beam contracts compile");
}

#[test]
fn invalid_science_contracts_fail_before_bulk_io() {
    let invalid_sampling = ScientificContract::new(
        SpectralContract::new(
            SpectralSamplingLaw::channel_integration(0),
            SpectralCoupling::Independent,
        ),
        MeasurementEquationContract::new(InstrumentResponse::Scalar, inner_products()),
    );
    let specification = ProblemSpecification::new(
        invalid_sampling,
        reconstruction(),
        weighting(),
        products(false),
        read_only_transaction(),
        numerics(false),
    );
    assert!(matches!(
        compile_request(specification, inputs(false)),
        Err(CompileProblemError::InvalidScientificContract {
            reason: "spectral channel averaging requires a positive bin width"
        })
    ));
}

#[test]
fn invalid_polarization_is_a_reconstruction_contract_error() {
    let specification = |coordinates| {
        ProblemSpecification::new(
            science(),
            ReconstructionContract::new(
                ReconstructionBasis::Constant,
                ReconstructionAlgorithm::Dirty,
                ReconstructionControls::new(0, 1.0, 0.0),
                PolarizationContract::new(coordinates),
            ),
            weighting(),
            products(false),
            read_only_transaction(),
            numerics(false),
        )
    };

    assert!(matches!(
        compile_request(specification(Vec::new()), inputs(false)),
        Err(CompileProblemError::InvalidReconstructionContract {
            reason: "at least one polarization coordinate must be requested"
        })
    ));
    assert!(matches!(
        compile_request(
            specification(vec![
                PolarizationCoordinate::StokesI,
                PolarizationCoordinate::LinearXx,
            ]),
            inputs(false),
        ),
        Err(CompileProblemError::InvalidReconstructionContract {
            reason: "one reconstruction cannot mix Stokes, linear, and circular coordinates"
        })
    ));
}

#[test]
fn compiled_problem_identity_has_a_pinned_schema_twenty_digest() {
    let compiled = compile_request(specification(false), inputs(false)).expect("compile problem");

    assert_eq!(casa_imaging_model::CompiledProblemId::SCHEMA_VERSION, 23);
    assert_eq!(
        compiled.problem_id().to_string(),
        "ead5796690bd3ce87c2f86634c5fc896f4dcf8abbc7cb49387985c1162ed525a"
    );
    let lifecycle = casa_imaging_model::LogicalIdentity::from_sha256(
        compiled.model_lifecycle().contract_id().as_bytes(),
    );
    assert!(casa_imaging_model::validate_compiled_problem_identity(
        compiled.problem_id().as_bytes(),
        compiled.problem_identity_basis(),
        compiled.inputs().model(),
        lifecycle,
    ));
    assert!(!casa_imaging_model::validate_compiled_problem_identity(
        compiled.problem_id().as_bytes(),
        compiled.problem_identity_basis(),
        ModelStateIdentity::Seed(identity(200)),
        lifecycle,
    ));
    assert!(!casa_imaging_model::validate_compiled_problem_identity(
        compiled.problem_id().as_bytes(),
        casa_imaging_model::LogicalIdentity::from_sha256([0; 32]),
        compiled.inputs().model(),
        lifecycle,
    ));
    assert!(!casa_imaging_model::validate_compiled_problem_identity(
        [0; 32],
        compiled.problem_identity_basis(),
        compiled.inputs().model(),
        lifecycle,
    ));
}
