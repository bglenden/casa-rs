// SPDX-License-Identifier: LGPL-3.0-or-later

//! Small cube contract for numerical band tests.

use casa_imaging_model::*;

#[path = "../../../casa-imaging-model/tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

pub fn problem(sampling: SpectralSamplingLaw) -> CompiledProblem {
    problem_with_inputs(sampling, inputs())
}

pub fn problem_with_inputs(
    sampling: SpectralSamplingLaw,
    inputs: ProblemInputIdentities,
) -> CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [4.0; 2],
        [-0.002, 0.002],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(8, 8),
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
                channels: 4,
                reference_pixel: 0.0,
                reference_frequency_hz: 1e9,
                increment_hz: 1e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let validity = ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .unwrap(),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .unwrap(),
    );
    let specification = ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(sampling, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::ChannelLocal { channels: 4 },
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            validity,
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::UnorderedWithinBudget,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1e-7, 1e-3)))
                .collect(),
        ),
    );
    compile(ImagingRequest::new(
        specification,
        geometry,
        inputs,
        ModelLifecycleRequirements::new(
            ModelBounds::new(256, 256, 256, 256, 1e30, 1e30).unwrap(),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .unwrap()
}

fn inputs() -> ProblemInputIdentities {
    let template = common::observation_snapshot(1, Vec::new(), ModelStateIdentity::Empty);
    let source = &template.sources()[0];
    let selection = ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(
            5,
            (0..5_u32).map(|row| SelectedMainRow::new(u64::from(row), 0)),
        )
        .unwrap(),
        RowSelection::new(
            IdSelection::All,
            TimeSelection::All,
            UvSelection::All,
            AntennaSelection::All,
            IdSelection::All,
            IdSelection::All,
            IntentSelection::All,
            IdSelection::All,
        ),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, (0..6).collect())],
        vec![CorrelationSelection::new(
            0,
            vec![
                CorrelationProduct::new(0, CorrelationType::CircularRr),
                CorrelationProduct::new(1, CorrelationType::CircularLl),
            ],
        )],
    );
    ProblemInputIdentities::new(
        compile_observation(ObservationSnapshotInput::new(
            vec![ObservationSourceInput::new(
                source.identity(),
                source.provenance().clone(),
                selection,
                source.generations().clone(),
            )],
            Vec::new(),
            ModelStateIdentity::Empty,
        ))
        .unwrap(),
    )
}
