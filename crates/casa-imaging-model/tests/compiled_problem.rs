// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompileProblemError, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, ItrfPosition, LogicalIdentity, MeasurementEquationContract,
    MissingPointingPolicy, ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract,
    ObservationPointingLaw, ObservationSnapshotId, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    ProblemInputIdentities, ProblemSpecification, ProductKind, ProductNormalization,
    ProductRequirements, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, ReferenceDataKind,
    RequiredCapability, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, StageErrorBudget, TimeScale, UvTaper, UvwCoordinateLaw,
    WeightDensityScope, WeightingContract, WeightingScheme, compile,
};

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
    compile(ImagingRequest::new(specification, geometry, inputs))
}

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
        PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
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
    ProductRequirements::new(products, ProductNormalization::FlatNoise, restoring_beam)
}

fn science() -> ScientificContract {
    ScientificContract::new(
        SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
        MeasurementEquationContract::new(InstrumentResponse::Scalar),
    )
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
        references,
        ModelStateIdentity::Seed(identity(5)),
    )
}

fn inputs_with_instrument() -> ProblemInputIdentities {
    ProblemInputIdentities::new(
        ObservationSnapshotId::new(identity(1)),
        vec![
            (ReferenceDataKind::Measures, identity(3)),
            (ReferenceDataKind::Ephemeris, identity(4)),
            (ReferenceDataKind::Instrument, identity(6)),
        ],
        ModelStateIdentity::Seed(identity(5)),
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
            ReconstructionAlgorithm::Mtmfs,
            ReconstructionControls::new(100, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        weighting(),
        products(false),
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
            ),
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
fn one_term_mfs_uses_the_constant_basis_instead_of_taylor() {
    let specification = ProblemSpecification::new(
        science(),
        ReconstructionContract::new(
            ReconstructionBasis::Taylor { terms: 1 },
            ReconstructionAlgorithm::Mtmfs,
            ReconstructionControls::new(100, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        weighting(),
        products(false),
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
        ),
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
fn derived_products_require_their_scientific_sources() {
    let specification = ProblemSpecification::new(
        science(),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Hogbom,
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
                ProductKind::TaylorTerms,
            ],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::PerPlane,
        ),
        numerics(false),
    );

    assert!(matches!(
        compile_request(specification, inputs(false)),
        Err(CompileProblemError::InvalidProductCombination { .. })
    ));
}

#[test]
fn duplicate_reference_families_are_rejected_instead_of_ordered_accidentally() {
    let inputs = ProblemInputIdentities::new(
        ObservationSnapshotId::new(identity(1)),
        vec![
            (ReferenceDataKind::Measures, identity(3)),
            (ReferenceDataKind::Measures, identity(4)),
        ],
        ModelStateIdentity::Empty,
    );

    assert_eq!(
        compile_request(specification(false), inputs),
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
    let negative_zero = compile_request(with_robust(-0.0), inputs(false)).expect("negative zero");
    let positive_zero = compile_request(with_robust(0.0), inputs(false)).expect("positive zero");
    let changed = compile_request(with_robust(0.5), inputs(false)).expect("changed science");

    assert_eq!(negative_zero.problem_id(), positive_zero.problem_id());
    assert_ne!(positive_zero.problem_id(), changed.problem_id());
    assert_eq!(casa_imaging_model::CompiledProblemId::SCHEMA_VERSION, 3);
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
                ReconstructionAlgorithm::Multiscale { scales_px },
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
            ),
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
fn complete_science_contract_changes_identity_and_capabilities() {
    let make = |science, weighting, products| {
        ProblemSpecification::new(
            science,
            reconstruction(),
            weighting,
            products,
            numerics(false),
        )
    };
    let baseline = compile_request(make(science(), weighting(), products(false)), inputs(false))
        .expect("baseline");
    let widefield_science = ScientificContract::new(
        SpectralContract::new(
            SpectralSampling::Linear,
            SpectralCoupling::CommonRestoringBeam,
        ),
        MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam),
    );
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
fn direction_dependent_response_requires_instrument_identity() {
    let direction_dependent = ScientificContract::new(
        SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
        MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam),
    );
    let specification = ProblemSpecification::new(
        direction_dependent,
        reconstruction(),
        weighting(),
        products(false),
        numerics(false),
    );

    assert!(matches!(
        compile_request(specification, inputs(false)),
        Err(CompileProblemError::InvalidScientificContract {
            reason: "direction-dependent response requires bound instrument reference data"
        })
    ));
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
            ),
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
            SpectralContract::new(SpectralSampling::Identity, coupling),
            MeasurementEquationContract::new(InstrumentResponse::Scalar),
        )
    };
    let compile = |coupling, beam| {
        compile_request(
            ProblemSpecification::new(
                science_with_coupling(coupling),
                reconstruction(),
                weighting(),
                products_with_beam(false, beam),
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
            SpectralSampling::ChannelAverage {
                channels_per_bin: 0,
            },
            SpectralCoupling::Independent,
        ),
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
fn compiled_problem_identity_has_a_pinned_schema_three_digest() {
    let compiled = compile_request(specification(false), inputs(false)).expect("compile problem");

    assert_eq!(casa_imaging_model::CompiledProblemId::SCHEMA_VERSION, 3);
    assert_eq!(
        compiled.problem_id().to_string(),
        "59bce25914ae4167dc46849604e3872d68a3276a680c15fbca61cc6f0c0557b2"
    );
}
