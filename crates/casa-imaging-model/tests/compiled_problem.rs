// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompileProblemError, ContinuumChannelRole, ContinuumChannelUse,
    ContinuumFitRule, DeclaredInnerProducts, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FrequencyFrame, GeometryInput,
    ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, InstrumentModel, InstrumentResponse,
    ItrfPosition, MeasurementEquationContract, MissingPointingPolicy, ModelColumnWrite,
    ModelInnerProduct, NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationSnapshot, ObservationTransactionRequirements, PairedMeasurementTransform,
    PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn, PointingDirectionSemantic,
    PointingExtrapolation, PointingInterpolation, PointingTimeSampling, PolarizationContract,
    PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInput, ProblemSpecification,
    ProductAxisKind, ProductBeamRule, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductRole, ProductSupportComparison, ProductTerm, ProductUnit,
    ProductValidityPolicies, ProductValidityRule, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    RequiredCapability, RestFrequency, RestoringBeamPolicy, ScientificContract,
    SequentialContinuumTransform, SkyDirection, SpectralContract, SpectralCoordinateSpec,
    SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeScale, UvTaper, UvwCoordinateLaw,
    VisibilityInnerProduct, WeightDensityScope, WeightingContract, WeightingScheme, compile,
};

mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use common::observation_snapshot;
use model_lifecycle_fixture::model_lifecycle;

fn compile_request(
    specification: ProblemSpecification,
    observation: ObservationSnapshot,
) -> Result<casa_imaging_model::CompiledProblem, CompileProblemError> {
    compile_with_geometry(specification, geometry(), observation)
}

fn compile_with_geometry(
    specification: ProblemSpecification,
    geometry: GeometryInput,
    observation: ObservationSnapshot,
) -> Result<casa_imaging_model::CompiledProblem, CompileProblemError> {
    compile(ProblemInput::new(
        specification,
        geometry,
        observation,
        model_lifecycle(),
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
            ProductBlankingPolicy::Zero,
        )
        .expect("valid primary-beam support"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
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
fn native_aw_request_input_validates_its_axes_and_terms() {
    use casa_imaging_model::{
        EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
    };
    let surface = EvlaDishSurface::new(
        (0..=1250)
            .map(|n| {
                let r = n as f64 / 100.0;
                [r, 0.028 * r * r, 0.056 * r]
            })
            .collect(),
    )
    .unwrap();
    let input = NativeAwRequestInput {
        surface,
        antenna_diameter_m: 25.0,
        frequencies: vec![
            NativeAwFrequencyGroup {
                spectral_window: 2,
                channel_frequencies_hz: vec![2.0e9, 2.1e9],
                cf_frequency_hz: 2.05e9,
            },
            NativeAwFrequencyGroup {
                spectral_window: 7,
                channel_frequencies_hz: vec![3.2e9, 3.3e9],
                cf_frequency_hz: 3.25e9,
            },
        ],
        w_values: vec![0.0, 100.0],
        w_increment: 0.01,
        pa_values: vec![0.3, 0.9],
        mueller_elements: vec![0, 15],
        reference_frequency_hz: 2.9e9,
        grid: NativeAwGrid {
            size: 128,
            sky_increment_rad: [-0.001, 0.001],
            oversampling: 4,
        },
        terms: NativeAwTerms {
            aperture: true,
            w_term: true,
            prolate_spheroidal: true,
            wideband: true,
            conjugate_beams: true,
        },
        maximum_cells: 32,
    };
    assert!(input.validate().is_ok());
    let mut bounded = input.clone();
    bounded.maximum_cells = 15;
    assert!(bounded.validate().is_err());
    let mut diameter = input.clone();
    diameter.antenna_diameter_m = 24.0;
    assert!(diameter.validate().is_err());
    let mut duplicate = input.clone();
    duplicate.frequencies[1].spectral_window = 2;
    assert!(duplicate.validate().is_err());
    let mut narrow = input;
    narrow.terms.wideband = false;
    assert!(narrow.validate().is_err());
}

fn observation() -> ObservationSnapshot {
    observation_snapshot(1)
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
        observation(),
    )
}

#[test]
fn equivalent_science_has_one_canonical_compiled_problem() {
    let first = compile_request(specification(false), observation()).expect("compile first");
    let reordered = compile_request(specification(true), observation()).expect("compile reordered");

    assert_eq!(first, reordered);
    assert_eq!(
        first.required_capabilities(),
        reordered.required_capabilities()
    );
    assert_eq!(first.observation(), reordered.observation());
    assert_eq!(first.reconstruction(), reordered.reconstruction());
    assert_eq!(first.weighting(), reordered.weighting());
    assert_eq!(first.products(), reordered.products());
    assert_eq!(first.numerics(), reordered.numerics());
}

#[test]
fn compiler_owns_the_exact_product_graph_and_atomic_publication_contract() {
    let compiled = compile_request(specification(false), observation()).expect("compile problem");
    let graph = compiled.product_graph();
    let reordered = compile_request(specification(true), observation()).expect("compile reordered");

    assert_eq!(graph, reordered.product_graph());
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
    assert_eq!(restored.validity(), ProductValidityRule::FinalNormalState);
    assert!(graph.publication().members().contains(&restored.node_id()));

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

    assert_eq!(
        graph.publication().members(),
        graph
            .nodes()
            .iter()
            .filter(|node| node.name().is_some())
            .map(|node| node.node_id())
            .collect::<Vec<_>>()
    );
}

#[test]
fn uncorrected_mask_is_separate_from_numeric_support_and_binds_publication() {
    let validity = product_validity()
        .with_uncorrected_mask(casa_imaging_model::UncorrectedImageMaskPolicy::PrimaryBeam);
    let compile_with_policy = |validity| {
        compile_request(
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
            observation(),
        )
        .expect("compile explicit primary-beam mask policy")
    };
    let compiled = compile_with_policy(validity);
    let absent = compile_with_policy(product_validity());

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
            ProductValidityRule::FinalNormalState,
        );
        assert_eq!(
            compiled
                .product_graph()
                .node(role)
                .unwrap()
                .storage()
                .pixel_mask(),
            casa_imaging_model::ProductPixelMask::Explicit(ProductValidityRule::PrimaryBeam(
                validity.primary_beam()
            )),
        );
        assert_eq!(
            absent
                .product_graph()
                .node(role)
                .unwrap()
                .storage()
                .pixel_mask(),
            casa_imaging_model::ProductPixelMask::Absent
        );
    }
}

#[test]
fn single_and_taylor_storage_contracts_preserve_science_and_exact_casa_metadata() {
    use casa_imaging_model::{ProductPixelMask, UncorrectedImageMaskPolicy};
    for taylor in [false, true] {
        for mask in [
            UncorrectedImageMaskPolicy::None,
            UncorrectedImageMaskPolicy::PrimaryBeam,
        ] {
            let validity = product_validity().with_uncorrected_mask(mask);
            let mut requested = vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::PrimaryBeam,
                ProductKind::PbCorrectedImage,
            ];
            if taylor {
                requested.extend([
                    ProductKind::TaylorTerms,
                    ProductKind::SpectralIndex,
                    ProductKind::SpectralIndexError,
                    ProductKind::PbCorrectedSpectralIndex,
                ]);
            }
            let reconstruction = if taylor {
                reconstruction()
            } else {
                ReconstructionContract::new(
                    ReconstructionBasis::Constant,
                    ReconstructionAlgorithm::Hogbom,
                    ReconstructionControls::new(100, 0.1, 0.0),
                    PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
                )
            };
            let compiled = compile_request(
                ProblemSpecification::new(
                    science(),
                    reconstruction,
                    weighting(),
                    ProductRequirements::new(
                        requested,
                        ProductNormalization::UnitResponse,
                        RestoringBeamPolicy::PerPlane,
                        validity,
                    ),
                    read_only_transaction(),
                    numerics(false),
                ),
                observation(),
            )
            .expect("storage contract matrix");
            let graph = compiled.product_graph();
            let terms = if taylor {
                vec![ProductTerm::Taylor(0), ProductTerm::Taylor(1)]
            } else {
                vec![ProductTerm::Single]
            };
            let pb_mask = ProductPixelMask::Explicit(ProductValidityRule::PrimaryBeam(
                validity.primary_beam(),
            ));
            for term in terms {
                let psf = graph.node(ProductRole::Psf(term)).unwrap();
                assert_eq!(psf.unit(), ProductUnit::JyPerBeam);
                assert_eq!(psf.storage().unit(), None);
                assert_eq!(psf.storage().pixel_mask(), ProductPixelMask::Absent);
                let residual = graph.node(ProductRole::Residual(term)).unwrap();
                assert_eq!(residual.unit(), ProductUnit::JyPerBeam);
                assert_eq!(residual.storage().unit(), None);
                assert!(!residual.storage().attach_beam());
                assert_eq!(residual.validity(), ProductValidityRule::FinalNormalState);
                let image = graph.node(ProductRole::RestoredImage(term)).unwrap();
                assert_eq!(image.storage().unit(), Some(ProductUnit::JyPerBeam));
                assert!(image.storage().attach_beam());
                for member in [residual, image] {
                    assert_eq!(
                        member.storage().pixel_mask(),
                        if mask == UncorrectedImageMaskPolicy::PrimaryBeam {
                            pb_mask
                        } else {
                            ProductPixelMask::Absent
                        }
                    );
                }
                assert_eq!(
                    graph
                        .node(ProductRole::PbCorrectedImage(term))
                        .unwrap()
                        .storage()
                        .pixel_mask(),
                    pb_mask
                );
            }
            let response_term = if taylor {
                ProductTerm::Taylor(0)
            } else {
                ProductTerm::Single
            };
            assert_eq!(
                graph
                    .node(ProductRole::PrimaryBeam(response_term))
                    .unwrap()
                    .storage()
                    .pixel_mask(),
                pb_mask
            );
            if taylor {
                assert_eq!(
                    graph
                        .node(ProductRole::PrimaryBeam(ProductTerm::Taylor(1)))
                        .unwrap()
                        .storage()
                        .pixel_mask(),
                    ProductPixelMask::Absent
                );
                for role in [ProductRole::SpectralIndex, ProductRole::SpectralIndexError] {
                    assert_eq!(
                        graph.node(role).unwrap().storage().pixel_mask(),
                        ProductPixelMask::Explicit(ProductValidityRule::Taylor(validity.taylor()))
                    );
                }
                assert_eq!(
                    graph
                        .node(ProductRole::PbCorrectedSpectralIndex)
                        .unwrap()
                        .storage()
                        .pixel_mask(),
                    ProductPixelMask::Explicit(ProductValidityRule::TaylorAndPrimaryBeam {
                        taylor: validity.taylor(),
                        primary_beam: validity.primary_beam(),
                    })
                );
            }
        }
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
        observation(),
    )
    .expect("compile PB-corrected Taylor products");
    let graph = compiled.product_graph();
    let alpha = graph
        .node(ProductRole::SpectralIndex)
        .expect("spectral-index product");
    let alpha_error = graph
        .node(ProductRole::SpectralIndexError)
        .expect("spectral-index-error product");
    assert!(graph.publication().members().contains(&alpha.node_id()));
    assert!(
        graph
            .publication()
            .members()
            .contains(&alpha_error.node_id())
    );

    let pb_alpha = graph
        .node(ProductRole::PrimaryBeamSpectralIndex)
        .expect("internal primary-beam spectral index");
    assert_eq!(pb_alpha.name(), None);
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
    assert_eq!(
        corrected_alpha.beam(),
        ProductBeamRule::Inherit(alpha.node_id())
    );
    assert!(
        graph
            .publication()
            .members()
            .contains(&corrected_alpha.node_id())
    );
}

#[test]
fn model_column_side_effects_are_compiled_into_the_problem() {
    let writable = compile_request(
        ProblemSpecification::new(
            science(),
            reconstruction(),
            weighting(),
            products(false),
            ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
            numerics(false),
        ),
        observation(),
    )
    .expect("compile model-column write");

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
    let compiled = compile_request(specification(false), observation()).expect("compile problem");

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
        compile_request(specification, observation()),
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

    let compiled = compile_request(specification, observation()).expect("natural weighting");
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
        compile_request(specification, observation()),
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
        observation(),
    )
    .expect("matching channel-local geometry");
    assert!(matches!(
        compile_with_geometry(channel_local(1), two_channel_geometry, observation()),
        Err(CompileProblemError::SpectralChannelCountMismatch {
            geometry_channels: 2,
            reconstruction_channels: 1,
        })
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
        compile_request(specification, observation()),
        Err(CompileProblemError::InvalidCapabilityCombination { .. })
    ));
}

#[test]
fn flat_normalization_uses_internal_sensitivity_without_publishing_it() {
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

    let compiled = compile_request(specification, observation())
        .expect("flat normalization reads the reconstruction normal state");
    assert!(
        compiled
            .product_graph()
            .node(ProductRole::Sensitivity)
            .is_none()
    );
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
        compile_request(incomplete_specification, observation()),
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
        compile_request(non_finite_specification, observation()),
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

    assert_eq!(collection.name(), None);
    assert!(
        !graph
            .publication()
            .members()
            .contains(&collection.node_id())
    );
}

#[test]
fn multiscale_order_and_duplicate_scales_do_not_change_the_reconstruction() {
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
    let canonical = compile_request(specification(vec![0.0, 3.0, 10.0]), observation())
        .expect("canonical scales");
    let reordered = compile_request(specification(vec![10.0, 3.0, -0.0, 3.0]), observation())
        .expect("reordered scales");

    assert_eq!(canonical.reconstruction(), reordered.reconstruction());
}

#[test]
fn mtmfs_scales_are_canonical_and_part_of_the_reconstruction() {
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
    let canonical = compile_request(make(vec![0.0, 3.0]), observation()).expect("MT-MFS scales");
    let reordered = compile_request(make(vec![3.0, -0.0, 3.0]), observation())
        .expect("canonical MT-MFS scales");
    let changed = compile_request(make(vec![0.0, 5.0]), observation()).expect("changed scales");

    assert_eq!(canonical.reconstruction(), reordered.reconstruction());
    assert_ne!(canonical.reconstruction(), changed.reconstruction());
}

#[test]
fn complete_science_contract_changes_capabilities() {
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
    compile_request(make(science(), weighting(), products(false)), observation())
        .expect("baseline");
    let tagged_scalar = compile_request(
        make(
            science().with_instrument_model(
                InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1,
            ),
            weighting(),
            products(false),
        ),
        observation(),
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
        observation(),
    )
    .expect("widefield science");

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
fn primary_beam_response_requires_an_exact_instrument_model() {
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
        compile_request(specification(direction_dependent.clone()), observation()),
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

    let compiled = compile_request(specification(direction_dependent), observation())
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
fn sequential_continuum_transform_is_a_compiled_capability() {
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
    compile_request(base(), observation()).expect("plain problem");
    let transformed = compile_request(
        base().with_visibility_transform(transform.clone()),
        observation(),
    )
    .expect("transformed problem");

    assert_eq!(transformed.visibility_transform(), Some(&transform));
    assert!(
        transformed
            .required_capabilities()
            .contains(&RequiredCapability::SequentialContinuumTransform)
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

    compile_request(dirty(1.0, 0.0), observation()).expect("canonical dirty problem");
    for specification in [dirty(0.1, 0.0), dirty(1.0, 0.5)] {
        assert!(matches!(
            compile_request(specification, observation()),
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
            observation(),
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
        compile_request(specification, observation()),
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
        compile_request(specification(Vec::new()), observation()),
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
            observation(),
        ),
        Err(CompileProblemError::InvalidReconstructionContract {
            reason: "one reconstruction cannot mix Stokes, linear, and circular coordinates"
        })
    ));
}
