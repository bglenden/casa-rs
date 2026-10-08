// SPDX-License-Identifier: LGPL-3.0-or-later

//! T21 bounded Högbom Minor Cycles over authoritative Normal State views,
//! driven entirely through the reconstruction owner seams. Normal states come
//! from synthetic passes over a known sky (`support/synthetic_pass.rs`).
//!
//! This root holds the shared problems, sky and first round; the tests live
//! in `minor_cycle/stopping.rs` (controls and stop rules) and
//! `minor_cycle/lineage.rs` (lineage and component placement).

use std::collections::BTreeMap;

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame, GeometryInput,
    HogbomIterationAccounting, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec,
    ImageShape, ImagingRequest, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    ModelBounds, ModelColumnState, ModelColumnWrite, ModelExecutionAttemptId, ModelInnerProduct,
    ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity, ModelSupport,
    MsColumnKind, NumericPrecision, NumericalStage, NumericsContract, ObservationSelection,
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
    AutoMultithreshControls, ExecutableModelProblem, FinalModelCompletion, FinalNormalState,
    MajorCyclePreparation, MaskBox, MinorCycleError, MinorCycleModelPlane,
    MinorCycleProgram as HogbomControls, MinorCycleStopReason, MinorCycleValidity, ModelGeneration,
    ModelLifecycle, ModelLifecycleError, ReconstructionMask, auto_multithresh,
    model_support_identity, run_minor_cycle as hogbom_minor_cycle,
};

#[path = "support/synthetic_pass.rs"]
mod synthetic_pass;
use synthetic_pass::Scene;

#[path = "minor_cycle/lineage.rs"]
mod lineage;
#[path = "minor_cycle/stopping.rs"]
mod stopping;

const SHAPE: [usize; 2] = [8, 8];

fn mask_coordinate(shape: [usize; 2]) -> DirectionCoordinateSpec {
    DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [shape[0] as f64 / 2.0, shape[1] as f64 / 2.0],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    )
}

fn full_mask(normal: &FinalNormalState, model: &ModelGeneration) -> ReconstructionMask {
    ReconstructionMask::full_plane(
        normal.problem_id(),
        model.generation_id(),
        mask_coordinate(normal.shape()),
        normal.shape(),
    )
    .expect("full reconstruction mask")
}

fn box_mask(
    normal: &FinalNormalState,
    model: &ModelGeneration,
    blc: [usize; 2],
    trc: [usize; 2],
) -> ReconstructionMask {
    ReconstructionMask::from_boxes(
        normal.problem_id(),
        model.generation_id(),
        mask_coordinate(normal.shape()),
        normal.shape(),
        [MaskBox::new(blc, trc).expect("ordered mask box")],
    )
    .expect("nonempty reconstruction mask")
}

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
            format!("fixture://minor-cycle/{seed}"),
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

/// Compile one T19/T21-compatible single-field Stokes-I constant-basis problem.
fn problem_with_model(
    observation: u8,
    model: ModelStateIdentity,
) -> casa_imaging_model::CompiledProblem {
    problem_with_model_and_width(observation, model, SHAPE[0])
}

fn problem_with_model_requirements(
    observation: u8,
    model: ModelStateIdentity,
    input: ModelInputCommitment,
) -> casa_imaging_model::CompiledProblem {
    compile_problem(observation, model, SHAPE[0], input)
}

fn problem_with_model_and_width(
    observation: u8,
    model: ModelStateIdentity,
    width: usize,
) -> casa_imaging_model::CompiledProblem {
    let input = match model {
        ModelStateIdentity::Empty => ModelInputCommitment::Empty,
        ModelStateIdentity::Generation(generation) => ModelInputCommitment::Generation(generation),
        // Seeded fixtures declare their exact aligned-support commitment.
        ModelStateIdentity::Seed(_) => unreachable!("seeded fixtures pass their own commitment"),
    };
    compile_problem(observation, model, width, input)
}

fn compile_problem(
    observation: u8,
    model: ModelStateIdentity,
    width: usize,
    input: ModelInputCommitment,
) -> casa_imaging_model::CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [width as f64 / 2.0; 2],
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
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source(observation)],
        Vec::new(),
        model,
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
            input,
        ),
    ))
    .expect("compile T21 problem")
}

/// The fixed sky every round images, identical for every compiled problem so
/// consecutive Major-Cycle rounds observe identical data: a bright and a
/// faint point source off the diagonal plus a low residual field.
fn scene(problem: &casa_imaging_model::CompiledProblem) -> Scene {
    Scene::new(problem)
        .with_point([5, 3], &[1.3])
        .with_point([2, 6], &[0.45])
        .with_noise(0.01)
}

fn bind_lifecycle(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
    epoch: u64,
) -> ModelLifecycle {
    ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable problem"),
        attempt(attempt_byte),
        epoch,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("bind model lifecycle")
}

fn controls() -> HogbomControls {
    HogbomControls::new(0.5, 1.0e-30, 64)
        .expect("valid controls")
        .record_component_sequence(64)
        .expect("recording limit")
}

#[track_caller]
fn residual_peak(normal_state_residual: &[num_complex::Complex64]) -> f64 {
    normal_state_residual
        .iter()
        .map(|value| value.re.abs())
        .fold(0.0_f64, f64::max)
}

/// Round-trip fixture: one confirm-only Major-Cycle round over the shared
/// stream, releasing the normal state, model completion, and final model.
struct FirstRound {
    normal_state: FinalNormalState,
    model_completion: FinalModelCompletion,
    final_model: ModelGeneration,
    residual_peak: f64,
}

fn first_confirm_round(observation: u8, attempt_byte: u8) -> FirstRound {
    first_confirm_round_scaled(observation, attempt_byte, 1.0)
}

fn first_confirm_round_scaled(observation: u8, attempt_byte: u8, sky_scale: f64) -> FirstRound {
    let problem = problem_with_model(observation, ModelStateIdentity::Empty);
    let mut lifecycle = bind_lifecycle(&problem, attempt_byte, 7);
    let named = lifecycle.initial_empty().expect("empty named generation");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("prepare final model");
    let joined = scene(&problem)
        .scaled(sky_scale)
        .reconcile(&problem, &mut lifecycle, preparation);
    let residual_peak = residual_peak(
        joined
            .normal_state()
            .read_window(0..1)
            .expect("single-plane fixture window")
            .residual()
            .complex()
            .unwrap(),
    );
    let (normal_state, model_completion, final_model) = joined.into_parts();
    // The completed lifecycle cannot reopen or finalize its model again.
    assert!(matches!(
        lifecycle.initial_empty(),
        Err(ModelLifecycleError::FinalModelAlreadyCompleted)
    ));
    FirstRound {
        normal_state,
        model_completion,
        final_model,
        residual_peak,
    }
}

fn maximal_pixel(plane: &[num_complex::Complex64]) -> [usize; 2] {
    let mut best = (0.0_f64, 0_usize);
    for (index, value) in plane.iter().enumerate() {
        if value.re.abs() > best.0 {
            best = (value.re.abs(), index);
        }
    }
    [best.1 / SHAPE[1], best.1 % SHAPE[1]]
}

fn plane_index(pixel: [usize; 2]) -> usize {
    pixel[0] * SHAPE[1] + pixel[1]
}
