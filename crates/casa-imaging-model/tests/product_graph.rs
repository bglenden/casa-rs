// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, MeasurementEquationContract, ModelColumnWrite, ModelInnerProduct,
    ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemSpecification,
    ProductAxisKind, ProductBeamRule, ProductBlankingPolicy, ProductElementRepresentation,
    ProductGenerationAuthority, ProductGenerationAuthorityError, ProductGenerationError,
    ProductKind, ProductNormalization, ProductPublicationJoin, ProductRequirements, ProductRole,
    ProductSchema, ProductSourceCommitment, ProductSourceCompletionEvidence,
    ProductSourceGenerationId, ProductSourceRole, ProductSupportComparison, ProductTerm,
    ProductUnit, ProductValidityPolicies, ProductValidityPolicyError, ProductValidityRule,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, ReferenceDataKind, RestFrequency, RestoringBeamPolicy,
    ScientificContract, SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSampling, SpectralWcs, StageErrorBudget, TaylorSupportReference,
    TaylorValidityPolicy, VisibilityInnerProduct, WeightDensityScope, WeightingContract,
    WeightingGenerationCompletionError, WeightingGenerationCompletionEvidence, WeightingScheme,
    WeightingSourceCompletion, compile,
};

mod common;

use common::{identity, problem_inputs};

#[test]
fn product_graph_compiles_complete_mtmfs_contract() {
    assert_eq!(
        PrimaryBeamValidityPolicy::new(
            0.0,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        ),
        Err(ProductValidityPolicyError::InvalidPrimaryBeamCutoff)
    );
    assert_eq!(
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            f32::NAN,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        ),
        Err(ProductValidityPolicyError::InvalidTaylorPeakFraction)
    );
    let problem = compile(request(false)).expect("compile product problem");
    let reordered = compile(request(true)).expect("compile reordered product problem");
    let graph = problem.product_graph();

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
            ".mask",
            ".weight.tt0",
            ".weight.tt1",
            ".weight.tt2",
            ".pb.tt0",
            ".pb.alpha",
            ".sensitivity",
            ".image.tt0.pbcor",
            ".image.tt1.pbcor",
            ".alpha",
            ".alpha.error",
            ".alpha.pbcor",
        ]
    );

    let restored = node(graph, ProductRole::RestoredImage(ProductTerm::Taylor(0)));
    assert_eq!(restored.axes().kind(), ProductAxisKind::SkyImage);
    assert_eq!(restored.axes().shape(), [64, 48, 1, 1]);
    assert_eq!(restored.unit(), ProductUnit::JyPerBeam);
    assert_eq!(
        restored.normalization(),
        Some(ProductNormalization::FlatNoise)
    );
    assert_eq!(
        restored.beam(),
        ProductBeamRule::Restoring(RestoringBeamPolicy::PerPlane)
    );
    assert_eq!(restored.validity(), ProductValidityRule::FinalNormalState);
    assert_eq!(restored.schema(), ProductSchema::CasaPagedImageF32);

    let primary_beam = node(graph, ProductRole::PrimaryBeam(ProductTerm::Taylor(0)));
    let primary_beam_alpha = node(graph, ProductRole::PrimaryBeamSpectralIndex);
    for term in [ProductTerm::Taylor(0), ProductTerm::Taylor(1)] {
        let restored = node(graph, ProductRole::RestoredImage(term));
        let corrected = node(graph, ProductRole::PbCorrectedImage(term));
        assert!(corrected.dependencies().contains(&restored.node_id()));
        assert!(corrected.dependencies().contains(&primary_beam.node_id()));
        assert!(
            !corrected
                .dependencies()
                .contains(&primary_beam_alpha.node_id())
        );
    }
    let corrected_alpha = node(graph, ProductRole::PbCorrectedSpectralIndex);
    let alpha = node(graph, ProductRole::SpectralIndex);
    assert_eq!(
        corrected_alpha.dependencies(),
        &[
            primary_beam.node_id(),
            primary_beam_alpha.node_id(),
            alpha.node_id(),
        ]
    );

    let ProductValidityRule::PrimaryBeam(primary_beam_policy) =
        node(graph, ProductRole::PbCorrectedImage(ProductTerm::Taylor(0))).validity()
    else {
        panic!("PB-corrected image requires its exact primary-beam policy");
    };
    assert_eq!(primary_beam_policy.cutoff(), 0.2);
    assert_eq!(
        primary_beam_policy.comparison(),
        ProductSupportComparison::StrictlyGreater
    );
    assert_eq!(
        primary_beam_policy.blanking(),
        ProductBlankingPolicy::ZeroAndFalseMask
    );
    let ProductValidityRule::Taylor(taylor_policy) = alpha.validity() else {
        panic!("spectral index requires its exact Taylor support policy");
    };
    assert_eq!(
        taylor_policy.reference(),
        TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum
    );
    assert_eq!(taylor_policy.peak_fraction(), 0.1);
    assert_eq!(
        taylor_policy.comparison(),
        ProductSupportComparison::StrictlyGreater
    );
    assert_eq!(
        taylor_policy.blanking(),
        ProductBlankingPolicy::ZeroAndFalseMask
    );

    let source_roles = graph
        .sources()
        .iter()
        .map(|source| source.role())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(source_roles.contains(&ProductSourceRole::FinalNormalState));
    assert!(source_roles.contains(&ProductSourceRole::FinalModel));
    assert!(source_roles.contains(&ProductSourceRole::WeightingGeneration));
    assert!(source_roles.contains(&ProductSourceRole::CleanMaskGeneration));
    assert!(source_roles.contains(&ProductSourceRole::PrimaryBeamGeneration));
    assert!(source_roles.contains(&ProductSourceRole::PrimaryBeamSpectralIndexGeneration));
    assert!(source_roles.contains(&ProductSourceRole::SensitivityGeneration));
    assert!(source_roles.contains(&ProductSourceRole::RestoringBeamGeneration));

    assert_eq!(
        graph.publication().join(),
        ProductPublicationJoin::ObservationTransaction
    );
    assert_eq!(
        graph.publication().members(),
        graph
            .nodes()
            .iter()
            .filter(|node| node.schema() == ProductSchema::CasaPagedImageF32)
            .map(|node| node.node_id())
            .collect::<Vec<_>>()
    );

    let error = compile(request_with_products(vec![ProductKind::TaylorTerms]))
        .expect_err("TaylorTerms without concrete coefficient products must be rejected");
    assert_eq!(
        error.to_string(),
        "invalid product combination: Taylor products require at least one concrete Taylor coefficient product"
    );
}

#[test]
fn product_graph_publishes_only_physical_casa_products() {
    let problem = compile(request(false)).expect("compile full MT-MFS/beam product problem");
    let graph = problem.product_graph();
    let coefficient_set = node(graph, ProductRole::TaylorCoefficientSet);
    let beam_metadata = node(graph, ProductRole::BeamMetadata);

    assert_eq!(coefficient_set.schema(), ProductSchema::LogicalCollection);
    assert!(!coefficient_set.dependencies().is_empty());
    assert_eq!(beam_metadata.schema(), ProductSchema::CasaImageMetadata);
    assert!(!beam_metadata.dependencies().is_empty());

    let physical_products = graph
        .nodes()
        .iter()
        .filter(|node| node.schema() == ProductSchema::CasaPagedImageF32)
        .map(|node| node.node_id())
        .collect::<Vec<_>>();
    assert_eq!(graph.publication().members(), physical_products);
    assert!(
        !graph
            .publication()
            .members()
            .contains(&coefficient_set.node_id())
    );
    assert!(
        !graph
            .publication()
            .members()
            .contains(&beam_metadata.node_id())
    );

    let bindings = graph
        .sources()
        .iter()
        .map(|source| {
            graph
                .bind_source_generation(
                    source.source_id(),
                    ProductSourceGenerationId::from_sha256(
                        [u8::try_from(source.source_id().ordinal() + 1)
                            .expect("small source fixture"); 32],
                    ),
                )
                .expect("bind graph source")
        })
        .collect();
    let generation = graph
        .bind_generation(bindings)
        .expect("bind complete generation");
    assert_eq!(generation.artifact_id(coefficient_set.node_id()), None);
    assert_eq!(generation.artifact_id(beam_metadata.node_id()), None);
    assert!(
        graph
            .publication()
            .members()
            .iter()
            .all(|member| generation.artifact_id(*member).is_some())
    );
}

#[test]
fn product_graph_separates_logical_payload_from_physical_writer_layout() {
    let problem = compile(request(false)).expect("compile product problem");
    let graph = problem.product_graph();
    let restored = node(graph, ProductRole::RestoredImage(ProductTerm::Taylor(0)));
    let payload = restored.payload();

    assert_eq!(
        payload.element_representation(),
        ProductElementRepresentation::Float32
    );
    assert_eq!(payload.logical_elements(), 64 * 48);
    assert_eq!(payload.logical_pixel_bytes(), 64 * 48 * 4);
    assert!(payload.identity_metadata_bytes() > 0);
    assert_eq!(
        payload.identity_envelope_bytes(),
        payload.logical_pixel_bytes() + payload.identity_metadata_bytes()
    );

    let coefficient_set = node(graph, ProductRole::TaylorCoefficientSet);
    assert_eq!(
        coefficient_set.payload().element_representation(),
        ProductElementRepresentation::NotApplicable
    );
    assert_eq!(coefficient_set.payload().logical_elements(), 0);
    assert_eq!(coefficient_set.payload().logical_pixel_bytes(), 0);
    assert!(coefficient_set.payload().identity_metadata_bytes() > 0);

    assert_eq!(
        graph.publication().members(),
        graph
            .nodes()
            .iter()
            .filter(|node| node.schema() == ProductSchema::CasaPagedImageF32)
            .map(|node| node.node_id())
            .collect::<Vec<_>>()
    );
}

#[test]
fn product_generation_authority_plans_and_seals_weighting_generation() {
    assert_eq!(ProductGenerationAuthority::SCHEMA_VERSION, 1);
    assert_eq!(WeightingGenerationCompletionEvidence::SCHEMA_VERSION, 1);
    let problem = compile(request_with_products(vec![ProductKind::SumWeights]))
        .expect("compile weighting-owned product problem");
    let graph = problem.product_graph();
    assert!(
        graph
            .sources()
            .iter()
            .filter(|source| source.role() == ProductSourceRole::WeightingGeneration)
            .count()
            > 1,
        "one global weighting generation must cover multiple compatible product source slots"
    );
    let weighting = problem.normal_equation().weighting().clone();
    assert!(matches!(
        ProductGenerationAuthority::plan(graph, vec![]),
        Err(ProductGenerationAuthorityError::MissingCommitment {
            role: ProductSourceRole::WeightingGeneration,
        })
    ));
    assert!(matches!(
        ProductGenerationAuthority::plan(
            graph,
            vec![
                ProductSourceCommitment::Weighting(weighting.clone()),
                ProductSourceCommitment::Weighting(weighting.clone()),
            ],
        ),
        Err(ProductGenerationAuthorityError::DuplicateCommitment {
            role: ProductSourceRole::WeightingGeneration,
        })
    ));
    let planned = ProductGenerationAuthority::plan(
        graph,
        vec![ProductSourceCommitment::Weighting(weighting.clone())],
    )
    .expect("plan exact weighting-owned product generation");
    assert_eq!(
        planned.commitments(),
        &[ProductSourceCommitment::Weighting(weighting.clone())]
    );
    assert!(
        graph
            .publication()
            .members()
            .iter()
            .all(|member| planned.artifact_id(*member).is_some())
    );

    assert!(matches!(
        ProductGenerationAuthority::authorize(&planned, vec![]),
        Err(ProductGenerationAuthorityError::MissingCompletion {
            role: ProductSourceRole::WeightingGeneration,
        })
    ));
    assert_eq!(weighting.sources().len(), 1);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    assert_eq!(source.selection().rows().selected_row_count(), 1);
    assert!(matches!(
        WeightingSourceCompletion::new(source.identity(), 1, 0, 1),
        Err(
            WeightingGenerationCompletionError::AcceptedVisibilitySamplesExceedProcessed {
                measurement_set: invalid_source,
                processed: 0,
                accepted: 1,
            }
        ) if invalid_source == source.identity()
    ));
    assert!(matches!(
        WeightingGenerationCompletionEvidence::new(
            &weighting,
            problem.inputs().observation_snapshot(),
            vec![],
        ),
        Err(
            WeightingGenerationCompletionError::SourceCoverageCountMismatch {
                expected: 1,
                actual: 0,
            }
        )
    ));

    let stale_problem = compile(request_with_width_products_and_weighting(
        false,
        64,
        vec![ProductKind::SumWeights],
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
    ))
    .expect("compile stale weighting generation");
    let stale_weighting = stale_problem.normal_equation().weighting();
    assert!(matches!(
        ProductGenerationAuthority::plan(
            graph,
            vec![ProductSourceCommitment::Weighting(stale_weighting.clone(),)],
        ),
        Err(ProductGenerationAuthorityError::StaleCommitment {
            role: ProductSourceRole::WeightingGeneration,
            ..
        })
    ));
    let stale_completion = WeightingGenerationCompletionEvidence::new(
        stale_weighting,
        stale_problem.inputs().observation_snapshot(),
        completed_weighting_sources(stale_problem.inputs().observation_snapshot()),
    )
    .expect("complete stale weighting generation");
    assert!(matches!(
        ProductGenerationAuthority::authorize(
            &planned,
            vec![ProductSourceCompletionEvidence::Weighting(stale_completion)],
        ),
        Err(ProductGenerationAuthorityError::StaleCompletion {
            role: ProductSourceRole::WeightingGeneration,
            ..
        })
    ));

    let completion = WeightingGenerationCompletionEvidence::new(
        &weighting,
        problem.inputs().observation_snapshot(),
        completed_weighting_sources(problem.inputs().observation_snapshot()),
    )
    .expect("complete exact weighting generation");
    assert!(matches!(
        ProductGenerationAuthority::authorize(
            &planned,
            vec![
                ProductSourceCompletionEvidence::Weighting(completion.clone()),
                ProductSourceCompletionEvidence::Weighting(completion.clone()),
            ],
        ),
        Err(ProductGenerationAuthorityError::DuplicateCompletion {
            role: ProductSourceRole::WeightingGeneration,
        })
    ));
    let seal = ProductGenerationAuthority::authorize(
        &planned,
        vec![ProductSourceCompletionEvidence::Weighting(
            completion.clone(),
        )],
    )
    .expect("authorize exact weighting generation completion");
    assert_eq!(seal.generation_id(), planned.generation_id());
    assert_eq!(seal.graph_id(), planned.graph_id());
    assert_eq!(seal.commitments(), planned.commitments());
    assert_eq!(
        seal.completions(),
        &[ProductSourceCompletionEvidence::Weighting(completion)]
    );
    assert!(
        graph
            .publication()
            .members()
            .iter()
            .all(|member| seal.artifact_id(*member).is_some())
    );
}

fn completed_weighting_sources(
    snapshot: &casa_imaging_model::ObservationSnapshot,
) -> Vec<WeightingSourceCompletion> {
    snapshot
        .sources()
        .iter()
        .map(|source| {
            WeightingSourceCompletion::new(
                source.identity(),
                source.selection().rows().selected_row_count(),
                1,
                0,
            )
            .expect("valid weighting source completion")
        })
        .collect()
}

#[test]
fn product_generation_rejects_missing_duplicate_and_stale_sources() {
    let problem = compile(request(false)).expect("compile product problem");
    let graph = problem.product_graph();
    let bindings = graph
        .sources()
        .iter()
        .map(|source| {
            graph
                .bind_source_generation(
                    source.source_id(),
                    ProductSourceGenerationId::from_sha256(
                        [u8::try_from(source.source_id().ordinal() + 1)
                            .expect("small source fixture"); 32],
                    ),
                )
                .expect("bind graph source")
        })
        .collect::<Vec<_>>();

    let generation = graph
        .bind_generation(bindings.clone())
        .expect("bind complete generation");
    let mut reordered = bindings.clone();
    reordered.reverse();
    assert_eq!(
        generation.generation_id(),
        graph
            .bind_generation(reordered)
            .expect("canonical reordered generation")
            .generation_id()
    );

    let missing = bindings[..bindings.len() - 1].to_vec();
    assert!(matches!(
        graph.bind_generation(missing),
        Err(ProductGenerationError::MissingSource { .. })
    ));

    assert!(matches!(
        graph.bind_source_generation(
            casa_imaging_model::ProductSourceId::from_ordinal(graph.sources().len()),
            ProductSourceGenerationId::from_sha256([98; 32]),
        ),
        Err(ProductGenerationError::UnexpectedSource { .. })
    ));

    let mut duplicate = bindings.clone();
    duplicate.push(bindings[0].clone());
    assert!(matches!(
        graph.bind_generation(duplicate),
        Err(ProductGenerationError::DuplicateSource { .. })
    ));

    let other_problem = compile(request_with_width(false, 65)).expect("compile other graph");
    let stale = other_problem
        .product_graph()
        .bind_source_generation(
            other_problem.product_graph().sources()[0].source_id(),
            ProductSourceGenerationId::from_sha256([99; 32]),
        )
        .expect("bind other graph source");
    let mut stale_bindings = bindings;
    stale_bindings[0] = stale;
    assert!(matches!(
        graph.bind_generation(stale_bindings),
        Err(ProductGenerationError::StaleSourceBinding { .. })
    ));

    let artifact_ids = graph
        .publication()
        .members()
        .iter()
        .map(|member| {
            generation
                .artifact_id(*member)
                .expect("publication artifact")
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(artifact_ids.len(), graph.publication().members().len());
}

#[test]
fn product_graph_identity_is_canonical_and_semantically_sensitive() {
    let problem = compile(request(false)).expect("compile product problem");
    let reordered = compile(request(true)).expect("compile reordered product problem");
    let changed_axes = compile(request_with_width(false, 65)).expect("compile changed axes");

    assert_eq!(casa_imaging_model::ProductGraphId::SCHEMA_VERSION, 3);
    assert_eq!(
        problem.product_graph().graph_id(),
        reordered.product_graph().graph_id()
    );
    assert_ne!(
        problem.product_graph().graph_id(),
        changed_axes.product_graph().graph_id()
    );
}

fn node(
    graph: &casa_imaging_model::ProductGraph,
    role: ProductRole,
) -> &casa_imaging_model::ProductNode {
    graph
        .nodes()
        .iter()
        .find(|node| node.role() == role)
        .expect("compiled product role")
}

fn request(reverse_products: bool) -> ImagingRequest {
    request_with_width(reverse_products, 64)
}

fn request_with_width(reverse_products: bool, width: usize) -> ImagingRequest {
    request_with_width_and_products(reverse_products, width, all_products())
}

fn request_with_products(products: Vec<ProductKind>) -> ImagingRequest {
    request_with_width_and_products(false, 64, products)
}

fn request_with_width_and_products(
    reverse_products: bool,
    width: usize,
    products: Vec<ProductKind>,
) -> ImagingRequest {
    request_with_width_products_and_weighting(
        reverse_products,
        width,
        products,
        WeightingContract::new(
            WeightingScheme::Briggs { robust: 0.5 },
            WeightDensityScope::GlobalSelection,
        ),
    )
}

fn request_with_width_products_and_weighting(
    reverse_products: bool,
    width: usize,
    mut products: Vec<ProductKind>,
    weighting: WeightingContract,
) -> ImagingRequest {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [31.0, 23.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(width, 48),
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
                casa_imaging_model::MissingPointingPolicy::Reject,
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
        SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
        MeasurementEquationContract::new(
            InstrumentResponse::PrimaryBeam,
            DeclaredInnerProducts::new(
                ModelInnerProduct::HermitianEuclidean,
                VisibilityInnerProduct::HermitianEuclidean,
            ),
        ),
    );
    let reconstruction = ReconstructionContract::new(
        ReconstructionBasis::Taylor { terms: 2 },
        ReconstructionAlgorithm::Mtmfs,
        ReconstructionControls::new(100, 0.1, 0.0),
        PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
    );
    if reverse_products {
        products.reverse();
    }
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
    let normalization = if products.contains(&ProductKind::Sensitivity) {
        ProductNormalization::FlatNoise
    } else {
        ProductNormalization::UnitResponse
    };
    let restoring_beam = if products.contains(&ProductKind::RestoredImage) {
        RestoringBeamPolicy::PerPlane
    } else {
        RestoringBeamPolicy::None
    };
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F32],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    ImagingRequest::new(
        ProblemSpecification::new(
            science,
            reconstruction,
            weighting,
            ProductRequirements::new(products, normalization, restoring_beam, validity),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            numerics,
        ),
        geometry,
        problem_inputs(
            1,
            vec![
                (ReferenceDataKind::Measures, identity(2)),
                (ReferenceDataKind::Instrument, identity(3)),
            ],
            ModelStateIdentity::Seed(identity(4)),
        ),
    )
}

fn all_products() -> Vec<ProductKind> {
    vec![
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
        ProductKind::Mask,
        ProductKind::Weight,
        ProductKind::PrimaryBeam,
        ProductKind::Sensitivity,
        ProductKind::PbCorrectedImage,
        ProductKind::TaylorTerms,
        ProductKind::SpectralIndex,
        ProductKind::SpectralIndexError,
        ProductKind::PbCorrectedSpectralIndex,
        ProductKind::Beam,
    ]
}
