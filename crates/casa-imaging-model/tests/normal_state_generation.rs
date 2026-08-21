// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompleteDataOperatorOutputId, CompleteDataPrimitiveCatalog,
    CompleteDataPrimitiveId, CompleteDataPrimitiveKind, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout,
    FinalReconciliationCommitment, FinalReconciliationCommitmentError,
    FinalReconciliationCommitmentId, FiniteValuePolicy, FrequencyFrame, GeometryInput, ImageAxis,
    ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest, InstrumentResponse,
    MeasurementEquationContract, MissingPointingPolicy, ModelColumnWrite, ModelGeneration,
    ModelGenerationCommitment, ModelGenerationCompletionEvidence, ModelInnerProduct,
    ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
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
fn final_reconciliation_commitment_is_compiler_and_model_bound() {
    assert_eq!(FinalReconciliationCommitment::SCHEMA_VERSION, 1);
    assert_eq!(FinalReconciliationCommitmentId::SCHEMA_VERSION, 1);

    let baseline = owned_model(RequestFixture::baseline());
    let product_only = owned_model(RequestFixture::product_only());
    let baseline_commitment =
        FinalReconciliationCommitment::from_problem_and_model(&baseline.problem, &baseline.model)
            .expect("commit final reconciliation for exact model context");
    let product_only_commitment = FinalReconciliationCommitment::from_problem_and_model(
        &product_only.problem,
        &product_only.model,
    )
    .expect("commit product-independent final reconciliation");

    assert_eq!(
        baseline_commitment.commitment_id().as_bytes(),
        [
            170, 50, 234, 49, 29, 63, 120, 239, 127, 238, 213, 249, 79, 81, 168, 211, 190, 251,
            111, 235, 242, 37, 25, 67, 155, 187, 247, 124, 144, 73, 59, 200,
        ]
    );
    assert_ne!(baseline_commitment, product_only_commitment);
    assert_eq!(
        baseline_commitment.commitment_id(),
        product_only_commitment.commitment_id(),
        "product-only changes must not alter final-reconciliation semantics"
    );
    assert!(matches!(
        FinalReconciliationCommitment::from_problem_and_model(
            &baseline.problem,
            &product_only.model,
        ),
        Err(FinalReconciliationCommitmentError::StaleModelContext {
            expected_problem,
            actual_problem,
        }) if expected_problem == baseline.problem.problem_id()
            && actual_problem == product_only.problem.problem_id()
    ));

    let changed = [
        (
            "observation snapshot",
            owned_model(RequestFixture {
                observation: 9,
                ..RequestFixture::baseline()
            }),
        ),
        (
            "model generation",
            owned_model(RequestFixture {
                width: 96,
                ..RequestFixture::baseline()
            }),
        ),
        (
            "weighting generation",
            owned_model(RequestFixture {
                weighting: WeightingContract::new(
                    WeightingScheme::Natural,
                    WeightDensityScope::NotApplicable,
                ),
                ..RequestFixture::baseline()
            }),
        ),
        (
            "normal-equation contract",
            owned_model(RequestFixture {
                instrument_response: InstrumentResponse::PrimaryBeam,
                ..RequestFixture::baseline()
            }),
        ),
        (
            "numerics contract",
            owned_model(RequestFixture {
                precision: NumericPrecision::F64,
                ..RequestFixture::baseline()
            }),
        ),
    ];
    for (semantic, changed) in changed {
        let commitment =
            FinalReconciliationCommitment::from_problem_and_model(&changed.problem, &changed.model)
                .expect("commit changed final-reconciliation semantics");
        assert_ne!(
            baseline_commitment.commitment_id(),
            commitment.commitment_id(),
            "{semantic} must alter final-reconciliation commitment identity"
        );
    }
}

#[test]
fn complete_data_output_identity_is_domain_separated_and_final_commitment_bound() {
    assert_eq!(CompleteDataOperatorOutputId::SCHEMA_VERSION, 1);

    let baseline = owned_model(RequestFixture::baseline());
    let product_only = owned_model(RequestFixture::product_only());
    let changed_weighting = owned_model(RequestFixture {
        weighting: WeightingContract::new(
            WeightingScheme::Natural,
            WeightDensityScope::NotApplicable,
        ),
        ..RequestFixture::baseline()
    });
    let commitment =
        FinalReconciliationCommitment::from_problem_and_model(&baseline.problem, &baseline.model)
            .expect("commit baseline complete-data output");
    let product_only_commitment = FinalReconciliationCommitment::from_problem_and_model(
        &product_only.problem,
        &product_only.model,
    )
    .expect("commit product-independent complete-data output");
    let changed_weighting_commitment = FinalReconciliationCommitment::from_problem_and_model(
        &changed_weighting.problem,
        &changed_weighting.model,
    )
    .expect("commit changed weighting complete-data output");

    let output = CompleteDataOperatorOutputId::from_reconciliation(&commitment);
    let product_only_output =
        CompleteDataOperatorOutputId::from_reconciliation(&product_only_commitment);
    let changed_weighting_output =
        CompleteDataOperatorOutputId::from_reconciliation(&changed_weighting_commitment);

    assert_eq!(
        output.as_bytes(),
        [
            72, 199, 2, 229, 115, 59, 45, 107, 109, 217, 40, 154, 239, 187, 166, 194, 7, 139, 21,
            230, 53, 58, 118, 221, 203, 133, 242, 31, 97, 4, 238, 113,
        ]
    );
    assert_ne!(output.as_bytes(), commitment.commitment_id().as_bytes());
    assert_eq!(output, product_only_output);
    assert_ne!(
        output, changed_weighting_output,
        "weighting generation must alter complete-data output identity"
    );
}

#[test]
fn complete_data_primitive_catalog_is_closed_exact_and_output_bound() {
    assert_eq!(CompleteDataPrimitiveId::SCHEMA_VERSION, 1);
    assert_eq!(
        CompleteDataPrimitiveKind::ALL,
        [
            CompleteDataPrimitiveKind::RightHandSide,
            CompleteDataPrimitiveKind::NormalResidual,
            CompleteDataPrimitiveKind::NormalApproximation,
            CompleteDataPrimitiveKind::Sensitivity,
            CompleteDataPrimitiveKind::SumWeights,
            CompleteDataPrimitiveKind::ValidSupport,
        ]
    );

    let baseline = owned_model(RequestFixture::baseline());
    let changed_weighting = owned_model(RequestFixture {
        weighting: WeightingContract::new(
            WeightingScheme::Natural,
            WeightDensityScope::NotApplicable,
        ),
        ..RequestFixture::baseline()
    });
    let commitment =
        FinalReconciliationCommitment::from_problem_and_model(&baseline.problem, &baseline.model)
            .expect("commit baseline primitive catalog");
    let changed_weighting_commitment = FinalReconciliationCommitment::from_problem_and_model(
        &changed_weighting.problem,
        &changed_weighting.model,
    )
    .expect("commit changed weighting primitive catalog");
    let catalog = CompleteDataPrimitiveCatalog::from_reconciliation(&commitment);
    let changed_catalog =
        CompleteDataPrimitiveCatalog::from_reconciliation(&changed_weighting_commitment);

    assert_eq!(
        catalog.output_id(),
        CompleteDataOperatorOutputId::from_reconciliation(&commitment)
    );
    let primitive_ids = CompleteDataPrimitiveKind::ALL.map(|kind| catalog.primitive(kind));
    assert_eq!(
        primitive_ids.into_iter().collect::<BTreeSet<_>>().len(),
        CompleteDataPrimitiveKind::ALL.len(),
        "the closed primitive catalog must contain six distinct identities"
    );
    assert_eq!(
        primitive_ids[0].as_bytes(),
        [
            222, 191, 175, 215, 72, 109, 178, 29, 196, 40, 191, 203, 89, 244, 140, 19, 1, 77, 254,
            242, 235, 62, 107, 102, 71, 237, 150, 9, 236, 94, 39, 92,
        ]
    );
    for kind in CompleteDataPrimitiveKind::ALL {
        assert_ne!(catalog.primitive(kind), changed_catalog.primitive(kind));
    }
}

struct OwnedModel {
    problem: casa_imaging_model::CompiledProblem,
    model: ModelGeneration,
}

fn owned_model(fixture: RequestFixture) -> OwnedModel {
    let problem = compile(request(fixture)).expect("compile final-reconciliation fixture");
    let commitment = ModelGenerationCommitment::from_problem(&problem)
        .expect("commit empty model for final-reconciliation fixture");
    let completion = ModelGenerationCompletionEvidence::empty(&commitment);
    let model = ModelGeneration::complete(&commitment, completion)
        .expect("own empty model for final-reconciliation fixture");
    OwnedModel { problem, model }
}

#[derive(Clone)]
struct RequestFixture {
    observation: u8,
    width: usize,
    instrument_response: InstrumentResponse,
    weighting: WeightingContract,
    precision: NumericPrecision,
    products: Vec<ProductKind>,
}

impl RequestFixture {
    fn baseline() -> Self {
        Self {
            observation: 1,
            width: 64,
            instrument_response: InstrumentResponse::Scalar,
            weighting: WeightingContract::new(
                WeightingScheme::Briggs { robust: 0.5 },
                WeightDensityScope::GlobalSelection,
            ),
            precision: NumericPrecision::F32,
            products: vec![ProductKind::Model],
        }
    }

    fn product_only() -> Self {
        Self {
            products: vec![ProductKind::SumWeights],
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
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(fixture.width, 48),
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
            fixture.instrument_response,
            DeclaredInnerProducts::new(
                ModelInnerProduct::HermitianEuclidean,
                VisibilityInnerProduct::HermitianEuclidean,
            ),
        ),
    );
    let reconstruction = ReconstructionContract::new(
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        ReconstructionControls::new(0, 1.0, 0.0),
        PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
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
                RestoringBeamPolicy::None,
                validity,
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            numerics,
        ),
        geometry,
        problem_inputs(fixture.observation, references, ModelStateIdentity::Empty),
    )
}
