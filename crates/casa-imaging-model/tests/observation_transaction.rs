// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use casa_imaging_model::{
    AxisOrder, CentreLaws, CompileProblemError, ContinuumChannelRole, ContinuumChannelUse,
    ContinuumFitRule, CorrectedDataWrite, DeclaredInnerProducts, DelayCentreLaw,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy,
    FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    InstrumentResponse, MeasurementEquationContract, MissingPointingPolicy, ModelColumnWrite,
    ModelInnerProduct, ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage,
    NumericsContract, ObservationPointingLaw, ObservationSnapshot, ObservationSnapshotInput,
    ObservationTransactionCompileError, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    ProblemInput, ProblemInputIdentities, ProblemSpecification, ProductKind, ProductNormalization,
    ProductRequirements, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, RestFrequency,
    RestoringBeamPolicy, ScientificContract, SequentialContinuumTransform, SkyDirection,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct,
    WeightDensityScope, WeightingContract, WeightingScheme, compile, compile_observation,
};

fn product_validity() -> casa_imaging_model::ProductValidityPolicies {
    casa_imaging_model::ProductValidityPolicies::new(
        casa_imaging_model::PrimaryBeamValidityPolicy::new(
            0.2,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::Zero,
        )
        .expect("valid PB policy"),
        casa_imaging_model::TaylorValidityPolicy::new(
            casa_imaging_model::TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::Zero,
        )
        .expect("valid Taylor policy"),
    )
}

fn compile_transaction(
    snapshot: ObservationSnapshot,
    transaction: ObservationTransactionRequirements,
) -> casa_imaging_model::CompiledProblem {
    compile_transaction_with_transform(snapshot, transaction, None)
}

fn compile_transaction_with_transform(
    snapshot: ObservationSnapshot,
    transaction: ObservationTransactionRequirements,
    transform: Option<SequentialContinuumTransform>,
) -> casa_imaging_model::CompiledProblem {
    try_compile_transaction(snapshot, transaction, transform)
        .expect("compile problem with observation transaction")
}

fn try_compile_transaction(
    snapshot: ObservationSnapshot,
    transaction: ObservationTransactionRequirements,
    transform: Option<SequentialContinuumTransform>,
) -> Result<casa_imaging_model::CompiledProblem, CompileProblemError> {
    let lifecycle = model_lifecycle_fixture::model_lifecycle(snapshot.model());
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [255.0, 255.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
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
    );
    let specification = ProblemSpecification::new(
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
            if transform.is_some() {
                ReconstructionBasis::ChannelLocal { channels: 1 }
            } else {
                ReconstructionBasis::Constant
            },
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        transaction,
        NumericsContract::new(
            vec![NumericPrecision::F32, NumericPrecision::F64],
            ReductionPolicy::Compensated,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
                .collect(),
        ),
    );
    let specification = transform.map_or(specification.clone(), |transform| {
        specification.with_visibility_transform(transform)
    });
    compile(ProblemInput::new(
        specification,
        geometry,
        ProblemInputIdentities::new(snapshot),
        lifecycle,
    ))
}

#[test]
fn transaction_contract_derives_the_exact_snapshot_read_set() {
    let snapshot =
        common::observation_snapshot(7, Vec::new(), casa_imaging_model::ModelStateIdentity::Empty);

    let problem = compile_transaction(
        snapshot.clone(),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
    );
    let contract = problem.observation_transaction();

    assert_eq!(contract.observation_snapshot_id(), snapshot.snapshot_id());
    assert_eq!(contract.read_set().sources().len(), 1);
    let source = &contract.read_set().sources()[0];
    assert_eq!(
        source.measurement_set(),
        snapshot.sources()[0].input_ordinal()
    );
    assert_eq!(source.selection(), snapshot.sources()[0].selection());
    assert_eq!(source.selected_columns(), snapshot.sources()[0].columns());
    assert!(contract.write_set().visibility_columns().is_empty());
}

#[test]
fn selected_model_column_writes_have_a_pinned_schema_three_identity() {
    let snapshot =
        common::observation_snapshot(8, Vec::new(), casa_imaging_model::ModelStateIdentity::Empty);
    let read_only_problem = compile_transaction(
        snapshot.clone(),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
    );
    let read_only = read_only_problem.observation_transaction();

    let writable_problem = compile_transaction(
        snapshot.clone(),
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );
    let writable = writable_problem.observation_transaction();

    assert_ne!(read_only.transaction_id(), writable.transaction_id());
    assert_eq!(
        casa_imaging_model::ObservationTransactionId::SCHEMA_VERSION,
        3
    );
    assert_eq!(
        writable.transaction_id().to_string(),
        "8700e2ccd686d96a3aec77841f1bd0b904e037298b209f13ff5369e539737eab"
    );
    assert_eq!(writable.write_set().visibility_columns().len(), 1);
    let write = &writable.write_set().visibility_columns()[0];
    assert_eq!(
        write.measurement_set(),
        snapshot.sources()[0].input_ordinal()
    );
    assert_eq!(write.selection(), snapshot.sources()[0].selection());
    assert_eq!(
        writable.read_set().sources()[0].selection().rows(),
        snapshot.sources()[0].selection().rows(),
        "the transaction read contract must retain the compact row identity"
    );
    assert_eq!(
        write.selection().rows(),
        snapshot.sources()[0].selection().rows(),
        "MODEL_DATA write access must retain the compact row identity"
    );
    assert_eq!(write.column(), MsColumnKind::ModelData);
}

fn one_channel_transform() -> SequentialContinuumTransform {
    SequentialContinuumTransform::new(vec![
        ContinuumFitRule::new(
            0,
            0,
            0,
            vec![ContinuumChannelRole::new(
                0,
                ContinuumChannelUse::FitAndApply,
            )],
        )
        .expect("one output-role fit rule"),
    ])
    .expect("continuum transform")
}

#[test]
fn a_corrected_data_write_without_the_column_is_refused_at_compile() {
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![common::observation_source_with_corrected_data(10, false)],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile observation without CORRECTED_DATA");

    assert!(matches!(
        try_compile_transaction(
            snapshot,
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled)
                .with_corrected_data_write(CorrectedDataWrite::SelectedOutputRows),
            Some(one_channel_transform()),
        ),
        Err(CompileProblemError::ObservationTransaction(
            ObservationTransactionCompileError::MissingCorrectedDataDestination
        ))
    ));
}

#[test]
fn corrected_data_write_needs_the_column_and_writes_output_role_channels_only() {
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![common::observation_source_with_corrected_data(10, true)],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile observation with CORRECTED_DATA destination");
    let transform = one_channel_transform();
    let problem = compile_transaction_with_transform(
        snapshot,
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled)
            .with_corrected_data_write(CorrectedDataWrite::SelectedOutputRows),
        Some(transform),
    );

    let writes = problem
        .observation_transaction()
        .write_set()
        .visibility_columns();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].column(), MsColumnKind::CorrectedData);
    assert_eq!(
        writes[0].selection().spectral_windows()[0].channel_indices(),
        &[0]
    );
}

#[test]
fn multi_ms_read_and_write_sets_keep_request_order() {
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![
            common::observation_source(12),
            common::observation_source(11),
        ],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile multi-MS observation");
    assert_eq!(
        snapshot.sources()[0].provenance().locator(),
        "fixture://observation/12"
    );

    let problem = compile_transaction(
        snapshot.clone(),
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );
    let contract = problem.observation_transaction();
    let canonical_sources = vec![0, 1];

    assert_eq!(
        contract
            .read_set()
            .sources()
            .iter()
            .map(|source| source.measurement_set())
            .collect::<Vec<_>>(),
        canonical_sources
    );
    assert_eq!(
        contract
            .write_set()
            .visibility_columns()
            .iter()
            .map(|write| write.measurement_set())
            .collect::<Vec<_>>(),
        canonical_sources
    );
}
