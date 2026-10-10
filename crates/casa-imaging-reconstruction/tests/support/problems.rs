// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compiled reconstruction problems on a small synthetic observation, and
//! their model lifecycles, for tests that drive major and minor cycles over
//! `synthetic_pass` scenes. Shared with the runtime's minor-cycle
//! tests by `#[path]`.

#![allow(
    dead_code,
    reason = "each integration-test crate uses a subset of the shared fixture"
)]

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompiledProblem, CorrelationProduct, CorrelationSelection,
    CorrelationType, DataDescriptionSelection, DeclaredInnerProducts, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    InstrumentResponse, IntentSelection, MeasurementEquationContract, ModelBounds,
    ModelColumnWrite, ModelInnerProduct, ModelLifecycleRequirements, NumericPrecision,
    NumericalStage, NumericsContract, ObservationSelection, ObservationSnapshotInput,
    ObservationSourceInput, ObservationSourceProvenance, ObservationTransactionRequirements,
    PhaseCentreLaw, PointingCentreLaw, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemInput, ProblemSpecification, ProductBlankingPolicy,
    ProductKind, ProductNormalization, ProductRequirements, ProductSupportComparison,
    ProductValidityPolicies, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, RestFrequency,
    RestoringBeamPolicy, RowSelection, ScientificContract, SelectedColumns, SelectedMainRow,
    SelectedRows, SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection,
    StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy, UvSelection, UvwCoordinateLaw,
    VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract,
    WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{ModelLifecycle, ModelStoragePlan, PreparedFinalModel};

pub fn source(seed: u8) -> ObservationSourceInput {
    ObservationSourceInput::new(
        ObservationSourceProvenance::new(format!("fixture://major-cycle/{seed}")),
        ObservationSelection::new(
            // The seed sets the MeasurementSet's row count, so sources from
            // different seeds are different observations.
            SelectedRows::from_ordered_main_rows(
                3 + u64::from(seed),
                [SelectedMainRow::new(0, 0), SelectedMainRow::new(2, 1)],
            )
            .expect("two selected rows"),
            RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
            vec![
                DataDescriptionSelection::new(0, 0, 0),
                DataDescriptionSelection::new(1, 1, 0),
            ],
            vec![
                SpectralWindowSelection::new(0, vec![0]),
                SpectralWindowSelection::new(1, vec![1]),
            ],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
            )],
        ),
        SelectedColumns::new(
            VisibilityColumn::Data,
            FlagPolicy::FlagOrFlagRow,
            WeightColumn::Weight,
        ),
        false,
    )
}

pub fn validity() -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .expect("valid primary-beam policy"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .expect("valid Taylor policy"),
    )
}

pub fn reconstruction_problem(
    observation: u8,
    width: usize,
    channels: usize,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
) -> CompiledProblem {
    reconstruction_problem_with_domains(observation, width, channels, 1, basis, algorithm, controls)
}

/// A problem of `domains` equal-shape image domains: the main field and
/// outliers `outlier-1`, `outlier-2`, …, each on the main field's tangent
/// plane and displaced one image width further along the first axis, so no
/// two domains overlap.
pub fn reconstruction_problem_with_domains(
    observation: u8,
    width: usize,
    channels: usize,
    domains: usize,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
) -> CompiledProblem {
    let centre = width as f64 / 2.0;
    let direction = |ordinal: usize| {
        DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [centre + (ordinal * width) as f64, centre],
            [-1.0e-6, 1.0e-6],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 0.0],
        )
    };
    let geometry = GeometryInput::new(
        (0..domains)
            .map(|ordinal| {
                ImageDomainSpec::new(
                    if ordinal == 0 {
                        ImageDomainRole::Main
                    } else {
                        ImageDomainRole::Outlier(format!("outlier-{ordinal}"))
                    },
                    ImageShape::new(width, width),
                    direction(ordinal),
                    FacetLayout::Single,
                    AxisOrder::new([
                        ImageAxis::DirectionLongitude,
                        ImageAxis::DirectionLatitude,
                        ImageAxis::Polarization,
                        ImageAxis::Spectral,
                    ]),
                )
            })
            .collect(),
        CentreLaws::new(
            PhaseCentreLaw::Fixed(direction(0).reference_direction()),
            PointingCentreLaw::PhaseTrackingCentre,
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.05e9,
                increment_hz: 1.0e8,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(vec![source(observation)]))
        .expect("compile observation snapshot");
    compile(ProblemInput::new(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
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
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                vec![ProductKind::Psf],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                validity(),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            NumericsContract::new(
                vec![NumericPrecision::F64],
                ReductionPolicy::UnorderedWithinBudget,
                FiniteValuePolicy::FlagInputRejectGenerated,
                NumericalStage::ALL
                    .into_iter()
                    .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                    .collect(),
            ),
        ),
        geometry,
        snapshot,
        ModelLifecycleRequirements::new(
            ModelBounds::new(4_096, 4_096, 1.0e30, 1.0e30).expect("valid bounds"),
            NumericPrecision::F64,
        ),
    ))
    .expect("compile T20 reconciliation problem")
}

/// The model lifecycle of `problem`, its generations resident in one window.
pub fn model_lifecycle(problem: &CompiledProblem) -> ModelLifecycle {
    ModelLifecycle::new(
        problem,
        ModelStoragePlan::resident(usize::MAX).expect("positive model window"),
    )
}

/// The empty model as the final model of an initial major cycle.
pub fn empty_final_model(lifecycle: &ModelLifecycle) -> PreparedFinalModel {
    lifecycle
        .prepare_final_model(
            lifecycle.initial_empty().expect("empty model generation"),
            [],
        )
        .expect("prepare the empty final model")
}
