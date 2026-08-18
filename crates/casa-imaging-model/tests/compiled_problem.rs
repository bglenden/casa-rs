// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompileProblemError, CompiledGeometryId, FieldGeometry, FiniteValuePolicy, GeometryContract,
    InstrumentResponse, LogicalIdentity, MeasurementEquationContract, ModelStateIdentity,
    NumericPrecision, NumericalStage, NumericsContract, ObservationSnapshotId,
    PolarizationContract, PolarizationCoordinate, ProblemInputIdentities, ProblemSpecification,
    ProductKind, ProductNormalization, ProductRequirements, ProjectionGeometry,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, ReferenceDataKind, RequiredCapability, RestoringBeamPolicy,
    ScientificContract, SpectralContract, SpectralCoupling, SpectralFrame, SpectralSampling,
    StageErrorBudget, UvTaper, WeightDensityScope, WeightingContract, WeightingScheme,
    compile_problem,
};

fn identity(byte: u8) -> LogicalIdentity {
    LogicalIdentity::from_sha256([byte; 32])
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
        ReconstructionAlgorithm::Mtmfs,
        ReconstructionControls::new(100, 0.1, 0.0),
    )
}

fn products(reverse: bool) -> ProductRequirements {
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
        RestoringBeamPolicy::PerPlane,
    )
}

fn science() -> ScientificContract {
    ScientificContract::new(
        GeometryContract::new(ProjectionGeometry::Coplanar, FieldGeometry::Single),
        SpectralContract::new(
            SpectralFrame::Native,
            SpectralSampling::Identity,
            SpectralCoupling::Independent,
            None,
        ),
        PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        MeasurementEquationContract::new(InstrumentResponse::Scalar),
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
        numerics(reverse),
    )
}

fn weighting() -> WeightingContract {
    WeightingContract::new(
        WeightingScheme::Briggs { robust: 0.5 },
        WeightDensityScope::GlobalSelection,
    )
}

fn inputs(reverse: bool) -> ProblemInputIdentities {
    let mut references = vec![
        (ReferenceDataKind::Measures, identity(3)),
        (ReferenceDataKind::Ephemeris, identity(4)),
    ];
    if reverse {
        references.reverse();
    }
    ProblemInputIdentities::new(
        ObservationSnapshotId::new(identity(1)),
        CompiledGeometryId::new(identity(2)),
        references,
        ModelStateIdentity::Seed(identity(5)),
    )
}

#[test]
fn equivalent_science_has_one_canonical_compiled_identity() {
    let first = compile_problem(specification(false), inputs(false)).expect("compile first");
    let reordered = compile_problem(specification(true), inputs(true)).expect("compile reordered");

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
fn derived_capabilities_cover_normalization_without_naming_a_backend() {
    let compiled = compile_problem(specification(false), inputs(false)).expect("compile problem");

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
        numerics(false),
    );

    assert!(matches!(
        compile_problem(specification, inputs(false)),
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
        numerics(false),
    );

    let compiled = compile_problem(specification, inputs(false)).expect("natural weighting");
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
            ReconstructionAlgorithm::Mtmfs,
            ReconstructionControls::new(100, 0.1, 0.0),
        ),
        weighting(),
        products(false),
        numerics(false),
    );

    assert!(matches!(
        compile_problem(specification, inputs(false)),
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
        ),
        numerics(false),
    );

    assert!(matches!(
        compile_problem(specification, inputs(false)),
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
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::DeterministicPairwise,
            FiniteValuePolicy::RejectAll,
            incomplete,
        ),
    );
    assert!(matches!(
        compile_problem(incomplete_specification, inputs(false)),
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
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            non_finite,
        ),
    );
    assert!(matches!(
        compile_problem(non_finite_specification, inputs(false)),
        Err(CompileProblemError::InvalidNumerics { .. })
    ));
}

#[test]
fn derived_products_require_their_scientific_sources() {
    let specification = ProblemSpecification::new(
        science(),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(100, 0.1, 0.0),
        ),
        weighting(),
        ProductRequirements::new(
            vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::SumWeights,
                ProductKind::TaylorTerms,
            ],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::PerPlane,
        ),
        numerics(false),
    );

    assert!(matches!(
        compile_problem(specification, inputs(false)),
        Err(CompileProblemError::InvalidProductCombination { .. })
    ));
}

#[test]
fn duplicate_reference_families_are_rejected_instead_of_ordered_accidentally() {
    let inputs = ProblemInputIdentities::new(
        ObservationSnapshotId::new(identity(1)),
        CompiledGeometryId::new(identity(2)),
        vec![
            (ReferenceDataKind::Measures, identity(3)),
            (ReferenceDataKind::Measures, identity(4)),
        ],
        ModelStateIdentity::Empty,
    );

    assert_eq!(
        compile_problem(specification(false), inputs),
        Err(CompileProblemError::DuplicateReferenceData(
            ReferenceDataKind::Measures
        ))
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
            numerics(false),
        )
    };
    let negative_zero = compile_problem(with_robust(-0.0), inputs(false)).expect("negative zero");
    let positive_zero = compile_problem(with_robust(0.0), inputs(false)).expect("positive zero");
    let changed = compile_problem(with_robust(0.5), inputs(false)).expect("changed science");

    assert_eq!(negative_zero.problem_id(), positive_zero.problem_id());
    assert_ne!(positive_zero.problem_id(), changed.problem_id());
    assert_eq!(casa_imaging_model::CompiledProblemId::SCHEMA_VERSION, 2);
}

#[test]
fn multiscale_order_and_duplicate_scales_do_not_change_scientific_identity() {
    let specification = |scales_px| {
        ProblemSpecification::new(
            science(),
            ReconstructionContract::new(
                ReconstructionBasis::Constant,
                ReconstructionAlgorithm::Multiscale { scales_px },
                ReconstructionControls::new(100, 0.1, 0.0),
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
            ),
            numerics(false),
        )
    };
    let canonical = compile_problem(specification(vec![0.0, 3.0, 10.0]), inputs(false))
        .expect("canonical scales");
    let reordered = compile_problem(specification(vec![10.0, 3.0, -0.0, 3.0]), inputs(false))
        .expect("reordered scales");

    assert_eq!(canonical.problem_id(), reordered.problem_id());
    assert_eq!(canonical.reconstruction(), reordered.reconstruction());
}

#[test]
fn complete_science_contract_changes_identity_and_capabilities() {
    let make = |science, weighting| {
        ProblemSpecification::new(
            science,
            reconstruction(),
            weighting,
            products(false),
            numerics(false),
        )
    };
    let baseline = compile_problem(make(science(), weighting()), inputs(false)).expect("baseline");
    let widefield_science = ScientificContract::new(
        GeometryContract::new(ProjectionGeometry::NonCoplanarW, FieldGeometry::Mosaic),
        SpectralContract::new(
            SpectralFrame::Lsrk,
            SpectralSampling::Linear,
            SpectralCoupling::CommonRestoringBeam,
            Some(1.420_405_751_77e9),
        ),
        PolarizationContract::new(vec![
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            PolarizationCoordinate::StokesU,
            PolarizationCoordinate::StokesV,
        ]),
        MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam),
    );
    let tapered = WeightingContract::new(
        WeightingScheme::Briggs { robust: 0.5 },
        WeightDensityScope::GlobalSelection,
    )
    .with_uv_taper(UvTaper::new(12_000.0, 8_000.0, 0.25));
    let widefield = compile_problem(make(widefield_science, tapered), inputs(false))
        .expect("widefield science");

    assert_ne!(baseline.problem_id(), widefield.problem_id());
    for capability in [
        RequiredCapability::NonCoplanarWGeometry,
        RequiredCapability::MosaicGeometry,
        RequiredCapability::SpectralFrameTransform,
        RequiredCapability::SpectralResampling,
        RequiredCapability::CommonBeamSpectralCoupling,
        RequiredCapability::PrimaryBeamResponse,
        RequiredCapability::UvTaper,
        RequiredCapability::Polarization(PolarizationCoordinate::StokesQ),
    ] {
        assert!(widefield.required_capabilities().contains(&capability));
    }
}

#[test]
fn invalid_science_contracts_fail_before_bulk_io() {
    let invalid_mosaic = ScientificContract::new(
        GeometryContract::new(ProjectionGeometry::Coplanar, FieldGeometry::Mosaic),
        SpectralContract::new(
            SpectralFrame::Native,
            SpectralSampling::Identity,
            SpectralCoupling::Independent,
            None,
        ),
        PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        MeasurementEquationContract::new(InstrumentResponse::Scalar),
    );
    let specification = ProblemSpecification::new(
        invalid_mosaic,
        reconstruction(),
        weighting(),
        products(false),
        numerics(false),
    );
    assert!(matches!(
        compile_problem(specification, inputs(false)),
        Err(CompileProblemError::InvalidScientificContract { .. })
    ));

    let invalid_sampling = ScientificContract::new(
        GeometryContract::new(ProjectionGeometry::Coplanar, FieldGeometry::Single),
        SpectralContract::new(
            SpectralFrame::Native,
            SpectralSampling::ChannelAverage {
                channels_per_bin: 0,
            },
            SpectralCoupling::Independent,
            None,
        ),
        PolarizationContract::new(Vec::new()),
        MeasurementEquationContract::new(InstrumentResponse::Scalar),
    );
    let specification = ProblemSpecification::new(
        invalid_sampling,
        reconstruction(),
        weighting(),
        products(false),
        numerics(false),
    );
    assert!(matches!(
        compile_problem(specification, inputs(false)),
        Err(CompileProblemError::InvalidScientificContract { .. })
    ));
}

#[test]
fn compiled_problem_identity_has_a_pinned_schema_two_digest() {
    let compiled = compile_problem(specification(false), inputs(false)).expect("compile problem");

    assert_eq!(casa_imaging_model::CompiledProblemId::SCHEMA_VERSION, 2);
    assert_eq!(
        compiled.problem_id().to_string(),
        "c486305a146bda4ba64fd82781a21bfe4013abf9cbfdfe19a7be995ddd707801"
    );
}
