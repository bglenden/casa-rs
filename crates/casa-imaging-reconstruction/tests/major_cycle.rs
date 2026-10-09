// SPDX-License-Identifier: LGPL-3.0-or-later

//! Major-Cycle reconciliation of complete-data normal states with the model
//! lifecycle, driven entirely through owner seams. Normal states come from
//! synthetic passes over a known sky (`support/synthetic_pass.rs`), assembled
//! through `PassNormalState` exactly as the runtime's major-cycle pass
//! assembles them.
//!
//! This root holds the shared problems and sky; the tests live in
//! `major_cycle/reconciliation.rs` (reconciliation, lineage and refreshes).

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame, GeometryInput,
    IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, IntentSelection, LogicalIdentity, MeasurementEquationContract,
    MeasurementSetIdentity, MetadataGeneration, MetadataTableKind, ModelBounds, ModelCell,
    ModelColumnState, ModelColumnWrite, ModelDeltaTerm, ModelExecutionAttemptId, ModelInnerProduct,
    ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity, MsColumnKind,
    NumericPrecision, NumericalStage, NumericsContract, ObservationSelection,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PolarizationContract,
    PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInputIdentities,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection, ScientificContract,
    SelectedColumns, SelectedMainRow, SelectedRows, SkyDirection, SourceGenerations,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeSelection, UvSelection, UvwCoordinateLaw,
    VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract,
    WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, MajorCycleError, MajorCycleOwner, MajorCyclePreparation,
    ModelLifecycle, ModelLifecycleError, PassNormalState, SpectralOperatorError,
    WeightingGenerationId, runtime_adapter::NormalStoragePlan,
};

#[path = "support/synthetic_pass.rs"]
mod synthetic_pass;
use synthetic_pass::Scene;

#[path = "major_cycle/reconciliation.rs"]
mod reconciliation;

fn identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut bytes = [seed; 32];
    bytes[0] = scope;
    LogicalIdentity::from_sha256(bytes)
}

fn attempt(byte: u8) -> ModelExecutionAttemptId {
    ModelExecutionAttemptId::new(identity(byte, 0))
}

fn source(seed: u8) -> ObservationSourceInput {
    let columns = [
        MsColumnKind::Data,
        MsColumnKind::Flag,
        MsColumnKind::FlagRow,
        MsColumnKind::Weight,
        MsColumnKind::Uvw,
        MsColumnKind::Time,
        MsColumnKind::TimeCentroid,
        MsColumnKind::Interval,
        MsColumnKind::Exposure,
        MsColumnKind::FieldId,
        MsColumnKind::DataDescriptionId,
        MsColumnKind::Antenna1,
        MsColumnKind::Antenna2,
        MsColumnKind::Feed1,
        MsColumnKind::Feed2,
        MsColumnKind::ScanNumber,
        MsColumnKind::StateId,
        MsColumnKind::ObservationId,
        MsColumnKind::ArrayId,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| ColumnGeneration::new(kind, identity(seed, 20 + index as u8)))
    .collect();
    let metadata = [
        MetadataTableKind::Antenna,
        MetadataTableKind::DataDescription,
        MetadataTableKind::Feed,
        MetadataTableKind::Field,
        MetadataTableKind::Observation,
        MetadataTableKind::Pointing,
        MetadataTableKind::Polarization,
        MetadataTableKind::SpectralWindow,
        MetadataTableKind::State,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, kind)| MetadataGeneration::new(kind, identity(seed, 60 + index as u8)))
    .collect();
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(identity(seed, 1)),
        ObservationSourceProvenance::new(
            format!("fixture://major-cycle/{seed}"),
            identity(seed, 2),
        ),
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(
                3,
                [SelectedMainRow::new(0, 0), SelectedMainRow::new(2, 1)],
            )
            .expect("two selected rows"),
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
        SourceGenerations::new(
            ConsistencyToken::new(identity(seed, 3)),
            SelectedColumns::new(
                VisibilityColumn::Data,
                FlagPolicy::FlagOrFlagRow,
                WeightColumn::Weight,
                columns,
            ),
            metadata,
            ModelColumnState::Absent,
        ),
    )
}

fn validity() -> ProductValidityPolicies {
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

fn t19_compatible_problem(observation: u8) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem(
        observation,
        8,
        1,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        ReconstructionControls::new(0, 1.0, 0.0),
    )
}

fn t38_cube_problem(observation: u8) -> casa_imaging_model::CompiledProblem {
    t38_cube_problem_with_channels(observation, 2)
}

fn t38_cube_problem_with_channels(
    observation: u8,
    channels: usize,
) -> casa_imaging_model::CompiledProblem {
    t38_cube_problem_with_controls(
        observation,
        channels,
        ReconstructionControls::new(8, 0.5, 0.0).with_noise_sigma(0.0),
    )
}

fn t38_cube_problem_with_controls(
    observation: u8,
    channels: usize,
    controls: ReconstructionControls,
) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem(
        observation,
        8,
        channels,
        ReconstructionBasis::ChannelLocal { channels },
        ReconstructionAlgorithm::Hogbom,
        controls,
    )
}

fn reconstruction_problem(
    observation: u8,
    width: usize,
    channels: usize,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    controls: ReconstructionControls,
) -> casa_imaging_model::CompiledProblem {
    let centre = width as f64 / 2.0;
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [centre, centre],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(width, width),
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
                channels,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.05e9,
                increment_hz: 1.0e8,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source(observation)],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile observation snapshot");
    compile(ImagingRequest::new(
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
        ProblemInputIdentities::new(snapshot),
        ModelLifecycleRequirements::new(
            ModelBounds::new(4_096, 4_096, 4_096, 4_096, 1.0e30, 1.0e30).expect("valid bounds"),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .expect("compile T20 reconciliation problem")
}

/// The fixed sky every pass images: an off-diagonal point source, brighter in
/// each later channel, over a low residual field. The two-row observation
/// reaches only output channels 0 and 1, so later channels carry no weight.
fn scene(problem: &casa_imaging_model::CompiledProblem) -> Scene {
    let ReconstructionBasis::ChannelLocal { channels } = problem.reconstruction().basis() else {
        return Scene::new(problem)
            .with_point([5, 3], &[1.3])
            .with_noise(0.01);
    };
    let amplitudes = (0..channels)
        .map(|channel| 1.3 + 0.8 * channel as f64)
        .collect::<Vec<_>>();
    Scene::new(problem)
        .with_point([5, 3], &amplitudes)
        .with_weights(
            (0..channels)
                .map(|channel| if channel < 2 { 1.0 } else { 0.0 })
                .collect(),
        )
        .with_noise(0.01)
}

fn bind_lifecycle(
    problem: &casa_imaging_model::CompiledProblem,
    attempt: ModelExecutionAttemptId,
) -> ModelLifecycle {
    ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem"),
        attempt,
        7,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("bind model lifecycle")
}

/// Content of an independent initial pass of `scene` over the empty model:
/// the data side of every reconciliation of the same sky.
fn confirm_content(
    problem: &casa_imaging_model::CompiledProblem,
    scene: &Scene,
) -> LogicalIdentity {
    let lifecycle = bind_lifecycle(problem, attempt(0xf0));
    let named = lifecycle.initial_empty().expect("confirm empty generation");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("confirm preparation");
    scene
        .initial(problem, &preparation)
        .diagnostic_content_identity()
        .expect("confirm content")
}

fn cell(x: usize) -> ModelCell {
    ModelCell::new(0, 0, 0, [x, 0])
}

fn delta_value(value: f64) -> casa_imaging_model::ModelValue {
    casa_imaging_model::ModelValue::new(value).expect("finite model value")
}
