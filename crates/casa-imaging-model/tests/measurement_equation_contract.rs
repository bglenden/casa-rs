// SPDX-License-Identifier: LGPL-3.0-or-later

//! Contract laws for the typed measurement equation and product-normalization seam.
//!
//! Source evidence: the weighting equation and its global normalization are in
//! *Synthesis Imaging II* (1998), p. 203; dirty/restoring-beam unit conversion
//! and residual scaling are on p. 184; final-image PB correction is on p. 328.
//! Marvil, *Imaging* (2026), slide 22, independently ties the PSF to the complete
//! gridded weighting generation. These tests exercise the logical contract, not
//! a numerical backend implementation.

use std::collections::BTreeSet;

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompiledProblemId, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FiniteValuePolicy, FlagPolicy,
    FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, MeasurementEquationContract, ModelColumnWrite,
    ModelInnerProduct, ModelStateIdentity, NormalEquationForm, NormalStateNormalization,
    NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationTransactionRequirements, PairedMeasurementTransform, PhaseCentreLaw,
    PointingCentreLaw, PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemSpecification, ProductBlankingPolicy,
    ProductBoundaryOperation, ProductKind, ProductNormalization, ProductRequirements,
    ProductSupportComparison, ProductValidityPolicies, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    UvwCoordinateLaw, VisibilityInnerProduct, VisibilityPhaseConvention, WeightColumn,
    WeightDensityScope, WeightingContract, WeightingGenerationId, WeightingScheme, compile,
};

mod common;

use common::{identity, problem_inputs};

fn validity_policies() -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB policy fixture"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy fixture"),
    )
}

#[derive(Clone, Copy, Debug, Default)]
struct Complex {
    re: f64,
    im: f64,
}

impl Complex {
    const fn new(re: f64, im: f64) -> Self {
        Self { re, im }
    }

    const fn conj(self) -> Self {
        Self::new(self.re, -self.im)
    }

    fn abs(self) -> f64 {
        self.re.hypot(self.im)
    }
}

impl std::ops::Add for Complex {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.re + rhs.re, self.im + rhs.im)
    }
}

impl std::ops::Mul for Complex {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::new(
            self.re * rhs.re - self.im * rhs.im,
            self.re * rhs.im + self.im * rhs.re,
        )
    }
}

type Vector = [Complex; 2];
type Matrix = [[Complex; 2]; 2];

fn matrix(transform: PairedMeasurementTransform) -> Matrix {
    match transform {
        PairedMeasurementTransform::SpectralBasis { .. } => [
            [Complex::new(1.0, 0.0), Complex::new(0.25, -0.1)],
            [Complex::new(0.0, 0.0), Complex::new(0.8, 0.0)],
        ],
        PairedMeasurementTransform::PolarizationMapping => [
            [Complex::new(0.5, 0.0), Complex::new(0.0, 0.5)],
            [Complex::new(0.5, 0.0), Complex::new(0.0, -0.5)],
        ],
        PairedMeasurementTransform::DirectionDependentResponse { .. } => [
            [Complex::new(0.9, 0.1), Complex::new(0.0, 0.0)],
            [Complex::new(0.0, 0.0), Complex::new(0.7, -0.2)],
        ],
        PairedMeasurementTransform::PhaseRotation { .. } => [
            [Complex::new(0.8, -0.6), Complex::new(0.0, 0.0)],
            [Complex::new(0.0, 0.0), Complex::new(0.6, 0.8)],
        ],
        PairedMeasurementTransform::SpectralResampling { .. } => [
            [Complex::new(0.75, 0.0), Complex::new(0.25, 0.0)],
            [Complex::new(0.2, 0.0), Complex::new(0.8, 0.0)],
        ],
        PairedMeasurementTransform::ChannelIntegration { .. } => [
            [Complex::new(0.5, 0.0), Complex::new(0.5, 0.0)],
            [Complex::new(0.25, 0.0), Complex::new(0.75, 0.0)],
        ],
    }
}

fn apply(matrix: Matrix, vector: Vector) -> Vector {
    [
        matrix[0][0] * vector[0] + matrix[0][1] * vector[1],
        matrix[1][0] * vector[0] + matrix[1][1] * vector[1],
    ]
}

fn apply_adjoint(matrix: Matrix, vector: Vector) -> Vector {
    [
        matrix[0][0].conj() * vector[0] + matrix[1][0].conj() * vector[1],
        matrix[0][1].conj() * vector[0] + matrix[1][1].conj() * vector[1],
    ]
}

fn apply_composition(transforms: &[PairedMeasurementTransform], mut vector: Vector) -> Vector {
    for transform in transforms {
        vector = apply(matrix(*transform), vector);
    }
    vector
}

fn apply_composition_adjoint(
    transforms: &[PairedMeasurementTransform],
    mut vector: Vector,
) -> Vector {
    for transform in transforms.iter().rev() {
        vector = apply_adjoint(matrix(*transform), vector);
    }
    vector
}

fn inner(left: Vector, right: Vector) -> Complex {
    left[0].conj() * right[0] + left[1].conj() * right[1]
}

fn assert_close(left: Complex, right: Complex) {
    assert!((left + Complex::new(-right.re, -right.im)).abs() < 1.0e-12);
}

fn geometry() -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [15.0, 15.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(32, 32),
            direction,
            casa_imaging_model::FacetLayout::Single,
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

fn compile_contract(sampling: SpectralSampling) -> casa_imaging_model::CompiledProblem {
    compile_contract_with_precision(sampling, NumericPrecision::F64)
}

fn compile_contract_with_precision(
    sampling: SpectralSampling,
    precision: NumericPrecision,
) -> casa_imaging_model::CompiledProblem {
    let inner_products = DeclaredInnerProducts::new(
        ModelInnerProduct::HermitianEuclidean,
        VisibilityInnerProduct::HermitianEuclidean,
    );
    let science = ScientificContract::new(
        SpectralContract::new(sampling, SpectralCoupling::Independent),
        MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam, inner_products),
    );
    let reconstruction = ReconstructionContract::new(
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Hogbom,
        ReconstructionControls::new(10, 0.1, 0.0),
        PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
    );
    let weighting = WeightingContract::new(
        WeightingScheme::Briggs { robust: 0.5 },
        WeightDensityScope::GlobalSelection,
    );
    let products = ProductRequirements::new(
        vec![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::PrimaryBeam,
            ProductKind::Sensitivity,
            ProductKind::PbCorrectedImage,
            ProductKind::Beam,
        ],
        ProductNormalization::FlatNoise,
        RestoringBeamPolicy::PerPlane,
        validity_policies(),
    );
    let numerics = NumericsContract::new(
        vec![precision],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-9, 1.0e-6)))
            .collect(),
    );
    let inputs = problem_inputs(
        41,
        vec![(ReferenceDataKind::Instrument, identity(42))],
        ModelStateIdentity::Empty,
    );

    compile(ImagingRequest::new(
        ProblemSpecification::new(
            science,
            reconstruction,
            weighting,
            products,
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            numerics,
        ),
        geometry(),
        inputs,
    ))
    .expect("compile typed measurement equation")
}

#[test]
fn weighting_generation_identity_binds_sampling_and_numerics_not_execution_choices() {
    let baseline = compile_contract(SpectralSampling::Linear);
    let changed_sampling = compile_contract(SpectralSampling::Identity);
    let changed_numerics =
        compile_contract_with_precision(SpectralSampling::Linear, NumericPrecision::F32);
    let weighting = baseline.normal_equation().weighting();

    assert_eq!(WeightingGenerationId::SCHEMA_VERSION, 2);
    assert_eq!(weighting.geometry_id(), baseline.geometry().geometry_id());
    assert_eq!(weighting.spectral_sampling(), SpectralSampling::Linear);
    assert_eq!(weighting.numerics_id(), baseline.numerics_id());
    assert_ne!(
        weighting.generation_id(),
        changed_sampling
            .normal_equation()
            .weighting()
            .generation_id(),
        "spectral sampling changes complete-selection density semantics"
    );
    assert_ne!(
        weighting.generation_id(),
        changed_numerics
            .normal_equation()
            .weighting()
            .generation_id(),
        "numerics changes the frozen weighting reduction"
    );
}

#[test]
fn compiled_contract_owns_paired_operator_weighting_and_product_boundary() {
    let problem = compile_contract(SpectralSampling::ChannelAverage {
        channels_per_bin: 2,
    });
    let normal = problem.normal_equation();
    let operator = normal.measurement_operator();

    assert_eq!(
        operator.domain().inner_product(),
        ModelInnerProduct::HermitianEuclidean
    );
    assert_eq!(
        operator.codomain().inner_product(),
        VisibilityInnerProduct::HermitianEuclidean
    );
    assert_eq!(
        operator.domain().geometry(),
        problem.geometry().geometry_id()
    );
    assert_eq!(
        operator.codomain().observation(),
        problem.inputs().observation()
    );
    assert_eq!(
        operator.transforms(),
        [
            PairedMeasurementTransform::SpectralBasis {
                basis: ReconstructionBasis::Constant,
            },
            PairedMeasurementTransform::PolarizationMapping,
            PairedMeasurementTransform::DirectionDependentResponse {
                response: InstrumentResponse::PrimaryBeam,
            },
            PairedMeasurementTransform::PhaseRotation {
                convention: VisibilityPhaseConvention::NegativeTwoPiFrequencyDelay,
            },
            PairedMeasurementTransform::ChannelIntegration {
                channels_per_bin: 2,
            },
        ]
    );

    assert_eq!(normal.forms(), NormalEquationForm::ALL);
    assert_eq!(
        normal.output().normalization(),
        NormalStateNormalization::Unnormalized
    );
    assert_eq!(
        normal.weighting().snapshot(),
        problem.inputs().observation()
    );
    assert_ne!(normal.weighting().generation_id().as_bytes(), [0; 32]);
    assert_eq!(normal.weighting().sources().len(), 1);
    assert_eq!(
        normal.weighting().sources()[0].flags(),
        FlagPolicy::FlagOrFlagRow
    );
    assert_eq!(
        normal.weighting().sources()[0].input_weights(),
        WeightColumn::Weight
    );
    match normal.weighting().density_scope() {
        WeightDensityScope::NotApplicable
        | WeightDensityScope::GlobalSelection
        | WeightDensityScope::PerOutputChannel => {}
    }

    let boundary = problem.products().normalization_boundary();
    assert_eq!(boundary.input(), NormalStateNormalization::Unnormalized);
    assert_eq!(
        boundary.operations(),
        [
            ProductBoundaryOperation::Normalize(ProductNormalization::FlatNoise),
            ProductBoundaryOperation::ScaleResidual,
            ProductBoundaryOperation::Restore(RestoringBeamPolicy::PerPlane),
            ProductBoundaryOperation::CorrectPrimaryBeam,
            ProductBoundaryOperation::BlankInvalid,
            ProductBoundaryOperation::ConvertUnits,
        ]
    );
}

#[test]
fn paired_compositions_obey_linearity_and_weighted_adjointness() {
    let channel_integration = compile_contract(SpectralSampling::ChannelAverage {
        channels_per_bin: 2,
    });
    let spectral_resampling = compile_contract(SpectralSampling::Linear);
    let compositions = [
        channel_integration
            .normal_equation()
            .measurement_operator()
            .transforms(),
        spectral_resampling
            .normal_equation()
            .measurement_operator()
            .transforms(),
    ];
    let mut covered = BTreeSet::new();
    let x = [Complex::new(0.4, -0.3), Complex::new(1.2, 0.5)];
    let y = [Complex::new(-0.7, 0.1), Complex::new(0.2, 0.9)];
    let alpha = Complex::new(0.3, -0.4);
    let data = [Complex::new(0.6, 0.2), Complex::new(-0.1, 0.8)];
    let weighted_data = [Complex::new(1.2, 0.4), Complex::new(-0.025, 0.2)];

    for transforms in compositions {
        for transform in transforms {
            covered.insert(transform.kind());
        }
        let alpha_x_plus_y = [alpha * x[0] + y[0], alpha * x[1] + y[1]];
        let composed = apply_composition(transforms, alpha_x_plus_y);
        let ax = apply_composition(transforms, x);
        let ay = apply_composition(transforms, y);
        assert_close(composed[0], alpha * ax[0] + ay[0]);
        assert_close(composed[1], alpha * ax[1] + ay[1]);

        let left = inner(apply_composition(transforms, x), weighted_data);
        let right = inner(x, apply_composition_adjoint(transforms, weighted_data));
        assert_close(left, right);

        // W is a positive-semidefinite metric: one flagged sample contributes zero.
        let flagged_metric = [data[0], Complex::default()];
        assert!(inner(data, flagged_metric).re >= 0.0);
    }

    assert_eq!(
        covered,
        BTreeSet::from([
            casa_imaging_model::PairedTransformKind::SpectralBasis,
            casa_imaging_model::PairedTransformKind::Polarization,
            casa_imaging_model::PairedTransformKind::DirectionDependentResponse,
            casa_imaging_model::PairedTransformKind::Phase,
            casa_imaging_model::PairedTransformKind::SpectralResampling,
            casa_imaging_model::PairedTransformKind::ChannelIntegration,
        ])
    );
}

#[test]
fn schema_six_problem_and_weighting_generation_identities_are_pinned() {
    let problem = compile_contract(SpectralSampling::Linear);

    assert_eq!(CompiledProblemId::SCHEMA_VERSION, 6);
    assert_eq!(WeightingGenerationId::SCHEMA_VERSION, 1);
    assert_eq!(
        (
            problem.problem_id().to_string(),
            problem
                .normal_equation()
                .weighting()
                .generation_id()
                .to_string(),
        ),
        (
            "a3406af7e9626a446f6096793072d6c93370d2c987ea967ef167bf5d1a2ff189".to_string(),
            "60dec5d9b99b65683119f97b83d56e77202f91acdd58731b0fff7d1bfde56d8d".to_string(),
        )
    );
}
