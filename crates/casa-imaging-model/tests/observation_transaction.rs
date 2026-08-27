// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, MeasurementEquationContract, MissingPointingPolicy,
    ModelColumnInitialization, ModelColumnPrecondition, ModelColumnState, ModelColumnWrite,
    ModelColumnWriteDisposition, ModelInnerProduct, ModelStateIdentity, MsColumnKind,
    NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationSnapshot, ObservationSnapshotInput, ObservationTransactionRequirements,
    PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn, PointingDirectionSemantic,
    PointingExtrapolation, PointingInterpolation, PointingTimeSampling, PolarizationContract,
    PolarizationCoordinate, ProblemInputIdentities, ProblemSpecification, ProductKind,
    ProductNormalization, ProductRequirements, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    RestFrequency, RestoringBeamPolicy, ScientificContract, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct, WeightDensityScope,
    WeightingContract, WeightingScheme, compile, compile_observation,
};

fn product_validity() -> casa_imaging_model::ProductValidityPolicies {
    casa_imaging_model::ProductValidityPolicies::new(
        casa_imaging_model::PrimaryBeamValidityPolicy::new(
            0.2,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB policy"),
        casa_imaging_model::TaylorValidityPolicy::new(
            casa_imaging_model::TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy"),
    )
}

fn compile_transaction(
    snapshot: ObservationSnapshot,
    transaction: ObservationTransactionRequirements,
) -> casa_imaging_model::CompiledProblem {
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
            ReconstructionBasis::Constant,
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
    compile(ImagingRequest::new(
        specification,
        geometry,
        ProblemInputIdentities::new(snapshot),
        lifecycle,
    ))
    .expect("compile problem with observation transaction")
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
    assert_eq!(source.measurement_set(), snapshot.sources()[0].identity());
    assert_eq!(source.selection(), snapshot.sources()[0].selection());
    assert_eq!(
        source.column_generations(),
        snapshot.sources()[0].generations().columns().generations()
    );
    assert_eq!(
        source.consistency_token(),
        snapshot.sources()[0].generations().consistency_token()
    );
    assert_eq!(
        source.metadata(),
        snapshot.sources()[0].generations().metadata_generations()
    );
    assert!(contract.write_set().model_columns().is_empty());
}

#[test]
fn selected_model_column_writes_have_a_pinned_schema_two_identity() {
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
        2
    );
    assert_eq!(
        writable.transaction_id().to_string(),
        "85223b7bb80d4613734481e74bebecdf77cb2dce864eaa18e9884043849bdec4"
    );
    assert_eq!(writable.write_set().model_columns().len(), 1);
    let write = &writable.write_set().model_columns()[0];
    assert_eq!(write.measurement_set(), snapshot.sources()[0].identity());
    assert_eq!(write.selection(), snapshot.sources()[0].selection());
    let snapshot_rows = snapshot.sources()[0]
        .selection()
        .rows()
        .ordered_main_rows()
        .as_ptr();
    assert_eq!(
        writable.read_set().sources()[0]
            .selection()
            .rows()
            .ordered_main_rows()
            .as_ptr(),
        snapshot_rows,
        "the transaction read contract must share the compiler-owned row manifest"
    );
    assert_eq!(
        write.selection().rows().ordered_main_rows().as_ptr(),
        snapshot_rows,
        "MODEL_DATA write access must not deep-clone the selected row vector"
    );
    assert_eq!(write.column(), MsColumnKind::ModelData);
    assert_eq!(write.precondition(), ModelColumnPrecondition::Absent);
    assert_eq!(
        write.disposition(),
        ModelColumnWriteDisposition::CreateAndInitializeAllRows {
            row_count: snapshot.sources()[0].selection().rows().source_row_count(),
            initialization: ModelColumnInitialization::Zero,
        }
    );
    assert_eq!(
        write.expected_consistency_token(),
        snapshot.sources()[0].generations().consistency_token()
    );
}

#[test]
fn model_write_preconditions_preserve_the_previous_generation() {
    let previous_generation = common::identity(99);
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![common::observation_source_with_model_generation(
            9,
            Some(previous_generation),
        )],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile observation with MODEL_DATA");

    let problem = compile_transaction(
        snapshot.clone(),
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );
    let contract = problem.observation_transaction();

    assert_eq!(
        contract.write_set().model_columns()[0].precondition(),
        ModelColumnPrecondition::Generation(previous_generation)
    );
    assert_eq!(
        contract.write_set().model_columns()[0].disposition(),
        ModelColumnWriteDisposition::ReplaceSelectedCells
    );
}

#[test]
fn output_only_model_columns_are_preconditioned_without_entering_the_read_set() {
    let previous_generation = common::identity(98);
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![common::observation_source_with_model_state(
            10,
            ModelColumnState::Present(previous_generation),
            None,
        )],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile observation with output-only MODEL_DATA");

    let problem = compile_transaction(
        snapshot.clone(),
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );
    let contract = problem.observation_transaction();

    assert!(
        contract.read_set().sources()[0]
            .column_generations()
            .iter()
            .all(|generation| generation.kind() != MsColumnKind::ModelData)
    );
    assert_eq!(
        contract.write_set().model_columns()[0].precondition(),
        ModelColumnPrecondition::Generation(previous_generation)
    );
}

#[test]
fn multi_ms_read_and_write_sets_are_canonical() {
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![
            common::observation_source(12),
            common::observation_source(11),
        ],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile reversed multi-MS observation");

    let problem = compile_transaction(
        snapshot.clone(),
        ObservationTransactionRequirements::new(ModelColumnWrite::SelectedRows),
    );
    let contract = problem.observation_transaction();
    let canonical_sources = snapshot
        .sources()
        .iter()
        .map(|source| source.identity())
        .collect::<Vec<_>>();

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
            .model_columns()
            .iter()
            .map(|write| write.measurement_set())
            .collect::<Vec<_>>(),
        canonical_sources
    );
}
