// SPDX-License-Identifier: LGPL-3.0-or-later

//! T21 bounded Högbom Minor Cycles over authoritative Normal State views,
//! driven entirely through the reconstruction owner seams.

use std::convert::Infallible;

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    ModelBounds, ModelColumnState, ModelColumnWrite,
    ModelExecutionAttemptId, ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements,
    ModelStateIdentity, ModelSupport, MsColumnKind, NumericPrecision, NumericalStage,
    NumericsContract, ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection,
    ScientificContract, SelectedColumns, SelectedMainRow, SelectedObservationGenerationId,
    SelectedObservationSample, SelectedPredictionTarget, SelectedRows, SelectedSampleAddress,
    SelectedSampleCoordinates, SelectedSampleMetadata, SelectedSpectralContribution,
    SelectedSpectralContributions, SelectedVisibilitySample, SkyDirection, SourceGenerations,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, SpectralWindowSelection, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeScale, TimeSelection, UvSelection,
    UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope,
    WeightingContract, WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    CleanWindow, ExecutableModelProblem, FinalModelCompletion,
    FinalNormalState, HogbomControls, MajorCycleOwner, MajorCyclePreparation,
    MinorCycleError, MinorCycleStopReason, ModelGeneration, ModelLifecycle,
    ModelLifecycleError, SerialMfsSpecification, WeightingAlgorithmState, WeightingError,
    WeightingExecutionLimits, WeightingPlan, WeightingReplayChunk, WeightingReplaySummary,
    begin_weighting_generation, hogbom_minor_cycle, model_support_identity, plan_weighting,
    runtime_adapter::{CompleteDataOwnerResult, prepare_serial_mfs_operator, serial_mfs_workload},
};

const SHAPE: [usize; 2] = [8, 8];

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
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid primary-beam policy"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
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
        ModelStateIdentity::Generation(generation) => {
            ModelInputCommitment::Generation(generation)
        }
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
        [((width - 1) / 2) as f64; 2],
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
                SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
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
                ReductionPolicy::Compensated,
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

/// The fixed fixture sample stream; identical for every compiled problem so
/// consecutive Major-Cycle rounds observe identical data.
fn fixture_samples(
    problem: &casa_imaging_model::CompiledProblem,
) -> Vec<SelectedObservationSample> {
    let mut samples = Vec::new();
    for (source_index, source) in problem
        .selected_observation()
        .read_set()
        .sources()
        .iter()
        .enumerate()
    {
        for (row_index, physical_row) in [0_u64, 2].into_iter().enumerate() {
            let frequency = if row_index == 0 { 1.05e9 } else { 1.15e9 };
            samples.push(SelectedObservationSample {
                address: SelectedSampleAddress {
                    measurement_set: source.measurement_set(),
                    physical_row,
                    data_description_id: row_index as i32,
                    spectral_window_id: row_index as u32,
                    channel_index: row_index as u32,
                    frequency_centre_hz: frequency,
                    frequency_lower_hz: frequency - 5.0e6,
                    frequency_upper_hz: frequency + 5.0e6,
                    channel_width_hz: 1.0e7,
                    frequency_frame: FrequencyFrame::Topocentric,
                    polarization_id: 0,
                    correlation_index: 0,
                    correlation_type: CorrelationType::StokesI,
                },
                visibility: SelectedVisibilitySample::Complex32([
                    1.0 + source_index as f32,
                    row_index as f32,
                ]),
                prediction_target: SelectedPredictionTarget::NotRequested,
                channel_flag: false,
                row_flag: false,
                input_weight: 1.0 + (source_index * 2 + row_index) as f32,
                coordinates: SelectedSampleCoordinates {
                    raw_uvw_m: [1.0 + row_index as f64, source_index as f64, 0.0],
                    density_uvw_m: [1.0 + row_index as f64, source_index as f64, 0.0],
                    transformed_uvw_m: [1.0 + row_index as f64, source_index as f64, 0.0],
                    phase_shift_m: 0.0,
                    uvw_law: UvwCoordinateLaw::PhaseTrackingCentre,
                    time: Epoch::new(59_000.0 + physical_row as f64, TimeScale::Utc),
                    time_centroid: Epoch::new(59_000.0 + physical_row as f64, TimeScale::Utc),
                    interval_seconds: 1.0,
                    exposure_seconds: 1.0,
                    phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    pointing_directions: casa_imaging_model::SelectedPointingDirections {
                        antenna1: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                        antenna2: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    },
                },
                metadata: SelectedSampleMetadata {
                    field_id: 0,
                    antenna1: 0,
                    antenna2: 1,
                    feed1: 0,
                    feed2: 0,
                    scan_number: 1,
                    state_id: 0,
                    observation_id: 0,
                    array_id: 0,
                },
            });
        }
    }
    samples
}

fn exact_contributions(sample: &SelectedObservationSample) -> SelectedSpectralContributions {
    SelectedSpectralContributions::new([
        SelectedSpectralContribution::new(sample.address.channel_index, 1.0),
        None,
    ])
    .expect("one exact output contribution")
}

fn freeze_weighting_generation(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
) -> Result<WeightingAlgorithmState, WeightingError> {
    let mut density = begin_weighting_generation(problem, plan)?;
    for sample in samples {
        density.consume(problem, *sample, exact_contributions(sample))?;
    }
    let mut sum_weight = density.finish(problem)?;
    for sample in samples {
        sum_weight.consume(problem, *sample, exact_contributions(sample))?;
    }
    sum_weight.finish()
}

fn replay(
    generation: &WeightingAlgorithmState,
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
) -> (Vec<WeightingReplayChunk>, WeightingReplaySummary) {
    let mut blocks = Vec::new();
    let mut phase = generation
        .begin_replay(problem, plan)
        .expect("begin replay");
    for sample in samples {
        if let Some(block) = phase
            .consume(problem, *sample, exact_contributions(sample))
            .expect("weight sample")
        {
            blocks.push(block);
        }
    }
    let (final_block, completion) = phase.finish().expect("finish replay state");
    if let Some(block) = final_block {
        blocks.push(block);
    }
    (blocks, completion)
}

/// Drive one exhaustive T19 complete-data replay bound to the prepared model.
fn run_t19_complete_data(
    problem: &casa_imaging_model::CompiledProblem,
    preparation: Option<&MajorCyclePreparation>,
    samples: &[SelectedObservationSample],
) -> CompleteDataOwnerResult {
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting residency plan");
    let selected_generation = replay_selected_generation(problem, samples);
    let generation = freeze_weighting_generation(problem, &plan, samples)
        .expect("freeze global weighting generation");
    let (blocks, summary) = replay(&generation, problem, &plan, samples);
    assert!(!blocks.is_empty(), "replay must emit bounded blocks");

    let specification = SerialMfsSpecification::new(problem).expect("serial MFS specification");
    let workload =
        serial_mfs_workload(&specification, plan.limits().max_block_samples()).expect("workload");
    let prepared = prepare_serial_mfs_operator(specification, workload).expect("prepare operator");
    let mut state = prepared
        .begin(problem, &generation)
        .expect("begin complete-data owner");
    if let Some(preparation) = preparation {
        state
            .bind_major_cycle_model(preparation.final_model())
            .expect("bind exact final model before replay");
    }
    for block in &blocks {
        state.consume_block(block).expect("consume weighted block");
    }
    state
        .complete(&summary, selected_generation)
        .expect("complete T19 evidence")
}

/// Mint the authoritative T17 observation generation of the fixture stream.
fn replay_selected_generation(
    problem: &casa_imaging_model::CompiledProblem,
    samples: &[SelectedObservationSample],
) -> SelectedObservationGenerationId {
    let (generation, count) = problem
        .inspect_selected_observation(samples.iter().copied().map(Ok::<_, Infallible>), |_| {
            Ok::<_, Infallible>(())
        })
        .expect("inspect fixture sample stream");
    assert_eq!(
        usize::try_from(count).expect("fixture sample count"),
        samples.len()
    );
    generation
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
    )
    .expect("bind model lifecycle")
}

fn controls() -> HogbomControls {
    HogbomControls::new(0.5, 1.0e-30, 64, 1.0e30)
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

mod window_geometry {
    use super::*;

    #[test]
    fn inner_quarter_matches_the_reference_default_for_one_shape() {
        let window = CleanWindow::inner_quarter([8, 8]);
        assert_eq!(window.blc(), [2, 2]);
        assert_eq!(window.trc(), [5, 5]);

        // Tiny shapes clamp into the plane instead of underflowing.
        let tiny = CleanWindow::inner_quarter([1, 1]);
        assert_eq!((tiny.blc(), tiny.trc()), ([0, 0], [0, 0]));
        let two = CleanWindow::inner_quarter([2, 2]);
        assert_eq!(two.blc(), [0, 0]);
        assert_eq!(two.trc(), [0, 0]);
    }

    #[test]
    fn inverted_windows_fail_the_constructor() {
        assert!(matches!(
            CleanWindow::new([3, 0], [2, 7]),
            Err(MinorCycleError::InvertedWindow)
        ));
        assert!(matches!(
            CleanWindow::new([0, 4], [7, 3]),
            Err(MinorCycleError::InvertedWindow)
        ));
    }
}

#[test]
fn controls_are_validated_explicitly() {
    assert!(matches!(
        HogbomControls::new(0.0, 1.0, 8, 1.0),
        Err(MinorCycleError::InvalidGain)
    ));
    assert!(matches!(
        HogbomControls::new(1.25, 1.0, 8, 1.0),
        Err(MinorCycleError::InvalidGain)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, -1.0, 8, 1.0),
        Err(MinorCycleError::InvalidThreshold)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, f64::NAN, 8, 1.0),
        Err(MinorCycleError::InvalidThreshold)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, 0.0, 0, 1.0),
        Err(MinorCycleError::InvalidIterationBound)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, 0.0, 8, 0.0),
        Err(MinorCycleError::InvalidMaximumModelUpdate)
    ));
    assert!(matches!(
        HogbomControls::new(0.5, 0.0, 8, 1.0)
            .expect("controls")
            .record_component_sequence(0),
        Err(MinorCycleError::InvalidRecordingLimit)
    ));
    let valid = HogbomControls::new(0.1, 0.5, 7, 3.25).expect("valid controls");
    assert_eq!(
        (
            valid.gain(),
            valid.threshold(),
            valid.max_iterations(),
            valid.maximum_model_update(),
            valid.component_sequence_limit()
        ),
        (0.1, 0.5, 7, 3.25, None)
    );
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
    let problem = problem_with_model(observation, ModelStateIdentity::Empty);
    let samples = fixture_samples(&problem);
    let mut lifecycle = bind_lifecycle(&problem, attempt_byte, 7);
    let named = lifecycle.initial_empty().expect("empty named generation");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("prepare final model");
    let evidence = run_t19_complete_data(&problem, Some(&preparation), &samples);
    let joined = MajorCycleOwner::from_complete_data(evidence, preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut lifecycle)
        .expect("atomic Major-Cycle reconciliation");
    let residual_peak = residual_peak(joined.normal_state().residual());
    let (normal_state, model_completion, final_model) = joined.into_parts();
    // The closed lifecycle cannot mint anything further.
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

#[test]
fn minor_cycle_delta_composes_with_the_next_major_cycle_reconciliation() {
    let round = first_confirm_round(41, 42);
    let final_generation = round.final_model.generation_id();
    assert_eq!(
        round.model_completion.generation(),
        final_generation,
        "the confirm round commits the named generation"
    );

    // The next round continues from the released generation through a
    // continuation commitment naming it; only the model owner can accept the
    // resulting delta against that base.
    let continuation = problem_with_model(
        43,
        ModelStateIdentity::Generation(final_generation.identity()),
    );
    let mut lifecycle = bind_lifecycle(&continuation, 44, 8);

    let residual_before = round.normal_state.residual().to_vec();
    let model_before = round
        .final_model
        .samples()
        .iter()
        .map(|sample| (sample.value().value(), sample.support()))
        .collect::<Vec<_>>();

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        CleanWindow::full_plane(SHAPE),
        controls(),
    )
    .expect("bounded Högbom solve");

    // Lineage binds the exact approximation and the exact named generation.
    let evidence = outcome.evidence();
    assert_eq!(evidence.problem_id(), continuation.problem_id());
    assert_eq!(evidence.attempt(), attempt(44));
    assert_eq!(evidence.epoch(), 8);
    assert_eq!(evidence.input_generation(), final_generation);
    assert_eq!(
        evidence.normal_state_completion(),
        round.normal_state.completion_id()
    );
    assert_eq!(
        evidence.normal_state_content(),
        round.normal_state.content_identity()
    );
    assert!(evidence.iterations() > 1, "the fixture must clean repeatedly");
    assert!(
        matches!(
            evidence.stop_reason(),
            MinorCycleStopReason::IterationBound | MinorCycleStopReason::StalenessBound | MinorCycleStopReason::ThresholdReached
        ),
        "the solve stops explicitly"
    );

    // Authoritative state is untouched by the solve.
    assert_eq!(round.normal_state.residual(), residual_before);
    let model_after = round
        .final_model
        .samples()
        .iter()
        .map(|sample| (sample.value().value(), sample.support()))
        .collect::<Vec<_>>();
    assert_eq!(model_before, model_after);

    // The delta is minted by the lifecycle owner against the exact base, in
    // canonical order, and accumulates the recorded components per cell.
    let (delta, evidence) = outcome.into_parts();
    let delta = delta.expect("an active solve mints a delta");
    let delta_id = delta.delta_id();
    assert_eq!(delta.base(), final_generation);
    let recorded = evidence
        .recorded_component_sequence()
        .expect("recording was requested");
    let mut expected: Vec<(usize, f64)> = Vec::new();
    for component in recorded {
        let flat = round
            .final_model
            .shape()
            .flat_index(component.cell())
            .expect("component cell inside shape");
        match expected.last_mut() {
            Some((last_flat, sum)) if *last_flat == flat => *sum += component.flux(),
            _ => expected.push((flat, component.flux())),
        }
    }
    expected.sort_by_key(|(flat, _)| *flat);
    let terms = delta
        .terms()
        .iter()
        .map(|term| {
            (
                round
                    .final_model
                    .shape()
                    .flat_index(term.cell())
                    .expect("inside"),
                term.increment().value(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(terms.len(), expected.len());
    for ((flat, sum), (expected_flat, expected_sum)) in terms.iter().zip(&expected) {
        assert_eq!(flat, expected_flat, "canonical ascending cell order");
        assert!((sum - expected_sum).abs() <= 1.0e-12 * sum.abs().max(1.0));
    }

    // Recorded components carry the CASA normalization: gain times the
    // PSF-normalized residual peak, and the first lands on the global peak.
    let psf_peak = round
        .normal_state
        .normal_approximation()
        .iter()
        .map(|value| value.re.abs())
        .fold(0.0_f64, f64::max);
    let residual_peak_pixel = maximal_pixel(round.normal_state.residual());
    assert_eq!(
        recorded[0].cell().pixel(),
        residual_peak_pixel,
        "the first component sits on the residual peak inside the window"
    );
    let expected_first_flux =
        controls().gain() * round.normal_state.residual()[plane_index(residual_peak_pixel)].re
            / psf_peak;
    assert!((recorded[0].flux() - expected_first_flux).abs() <= 1.0e-12);
    assert!((evidence.total_flux() - recorded.iter().map(|c| c.flux().abs()).sum::<f64>()).abs() <= 1.0e-12);

    // Applying the delta happens only through the owner, which mints the next
    // generation and reconciles a strictly reduced residual.
    let preparation = MajorCyclePreparation::prepare(&lifecycle, round.final_model, Some(delta))
        .expect("owner accepts its own delta");
    // The continuation problem carries its own MS identity, so round two
    // replays the identically valued stream bound to that identity.
    let continuation_samples = fixture_samples(&continuation);
    let evidence2 = run_t19_complete_data(&continuation, Some(&preparation), &continuation_samples);
    let joined2 = MajorCycleOwner::from_complete_data(evidence2, preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut lifecycle)
        .expect("second-round reconciliation");
    let model_completion2 = joined2.model_completion();
    assert_eq!(model_completion2.delta(), Some(delta_id));
    assert_eq!(model_completion2.base(), final_generation);
    let final2 = joined2.final_model();
    assert_ne!(final2.generation_id(), final_generation);
    assert_eq!(
        final2.origin(),
        casa_imaging_reconstruction::ModelGenerationOrigin::Delta {
            base: final_generation,
            delta: delta_id,
        }
    );
    for (flat, increment) in &terms {
        let updated = final2.samples()[*flat].value().value();
        let base_value = model_before[*flat].0;
        assert!((updated - (base_value + increment)).abs() <= 1.0e-12);
    }
    let peak2 = residual_peak(joined2.normal_state().residual());
    assert!(
        peak2 < round.residual_peak,
        "one clean-and-reconcile round must reduce the residual peak: {peak2} !< {}",
        round.residual_peak
    );
}

#[test]
fn threshold_stop_converges_without_a_delta_or_a_reconciliation_request() {
    let round = first_confirm_round(45, 46);
    let continuation = problem_with_model(
        47,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 48, 9);
    let converging_controls = HogbomControls::new(0.5, 1.0e12, 32, 1.0e30)
        .expect("valid controls")
        .record_component_sequence(8)
        .expect("recording limit");

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        CleanWindow::inner_quarter(SHAPE),
        converging_controls,
    )
    .expect("bounded Högbom solve");
    let evidence = outcome.evidence();
    assert_eq!(evidence.stop_reason(), MinorCycleStopReason::ThresholdReached);
    assert_eq!(evidence.iterations(), 0);
    assert!(!evidence.requests_reconciliation());
    assert!(outcome.delta().is_none());
    assert!(evidence.recorded_component_sequence().is_none());

    // Identical solves mint identical stable evidence identities.
    let other_lifecycle = bind_lifecycle(&continuation, 48, 9);
    let repeat = hogbom_minor_cycle(
        &other_lifecycle,
        &round.final_model,
        &round.normal_state,
        CleanWindow::inner_quarter(SHAPE),
        converging_controls,
    )
    .expect("repeat solve");
    assert_eq!(
        repeat.evidence().evidence_id(),
        evidence.evidence_id(),
        "evidence identities hash stable authorities, not process-local seals"
    );
    assert!(repeat
        .evidence()
        .first_divergence(evidence)
        .is_none());
}

#[test]
fn iteration_bound_stops_with_an_explicit_reconciliation_request() {
    let round = first_confirm_round(49, 50);
    let continuation = problem_with_model(
        51,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 52, 10);
    let bounded = HogbomControls::new(0.5, 0.0, 1, 1.0e30)
        .expect("valid controls")
        .record_component_sequence(8)
        .expect("recording limit");

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        CleanWindow::full_plane(SHAPE),
        bounded,
    )
    .expect("bounded Högbom solve");
    let evidence = outcome.evidence();
    assert_eq!(evidence.stop_reason(), MinorCycleStopReason::IterationBound);
    assert_eq!(evidence.iterations(), 1);
    assert!(evidence.requests_reconciliation());
    assert!(outcome.delta().is_some());
}

#[test]
fn staleness_bound_stops_before_extrapolating_past_the_view_envelope() {
    let round = first_confirm_round(53, 54);
    let continuation = problem_with_model(
        55,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 56, 11);
    // One component already reaches the cumulative update ceiling.
    let tight = HogbomControls::new(0.5, 0.0, 64, 1.0e-300).expect("valid controls");

    let outcome = hogbom_minor_cycle(
        &lifecycle,
        &round.final_model,
        &round.normal_state,
        CleanWindow::full_plane(SHAPE),
        tight,
    )
    .expect("bounded Högbom solve");
    let evidence = outcome.evidence();
    assert_eq!(evidence.stop_reason(), MinorCycleStopReason::StalenessBound);
    assert_eq!(evidence.iterations(), 1);
    assert!(evidence.total_flux() >= tight.maximum_model_update());
    assert!(evidence.requests_reconciliation());
}

#[test]
fn window_and_valid_support_constrain_component_placement() {
    // An aligned seed marks one pixel invalid; the solver must skip it.
    let seed = identity(57, 90);
    let invalid_flat = 3 * SHAPE[0] + 3;
    let mut supports = Vec::new();
    let mut seed_values: Vec<Result<casa_imaging_model::ModelSample, ()>> = Vec::new();
    for index in 0..SHAPE[0] * SHAPE[1] {
        let support = if index == invalid_flat {
            ModelSupport::Invalid
        } else {
            ModelSupport::Valid
        };
        supports.push(support);
        let value = casa_imaging_model::ModelValue::new(f64::from(index as i16) * 0.125 - 4.0)
            .expect("finite seed value");
        seed_values.push(if support == ModelSupport::Valid {
            Ok(casa_imaging_model::ModelSample::valid(value))
        } else {
            Ok(casa_imaging_model::ModelSample::invalid())
        });
    }
    let input = ModelInputCommitment::AlignedSeed {
        source: seed,
        support: model_support_identity(supports.iter().copied()),
    };
    let problem = problem_with_model_requirements(58, ModelStateIdentity::Seed(seed), input);
    let samples = fixture_samples(&problem);
    let mut lifecycle = bind_lifecycle(&problem, 59, 12);
    let seeded = lifecycle
        .ingest_aligned(seed, lifecycle.contract().target(), seed_values)
        .expect("aligned stream")
        .expect("aligned seed ingest");
    assert_eq!(seeded.samples()[invalid_flat].support(), ModelSupport::Invalid);

    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, seeded, None).expect("prepare seeded model");
    let evidence = run_t19_complete_data(&problem, Some(&preparation), &samples);
    let joined = MajorCycleOwner::from_complete_data(evidence, preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut lifecycle)
        .expect("seeded reconciliation");
    let (normal_state, _, final_model) = joined.into_parts();

    // The solve needs an open owner: continue through a fresh lifecycle whose
    // commitment names the seeded final generation.
    let continuation = problem_with_model(
        60,
        ModelStateIdentity::Generation(final_model.generation_id().identity()),
    );
    let open = bind_lifecycle(&continuation, 61, 13);

    let solving_controls = HogbomControls::new(0.5, 1.0e-30, 64, 1.0e30)
        .expect("valid controls")
        .record_component_sequence(64)
        .expect("recording limit");
    let outcome = hogbom_minor_cycle(
        &open,
        &final_model,
        &normal_state,
        CleanWindow::full_plane(SHAPE),
        solving_controls,
    )
    .expect("bounded solve over seeded support");
    for component in outcome
        .evidence()
        .recorded_component_sequence()
        .expect("recording requested")
    {
        assert_ne!(
            final_model.shape().flat_index(component.cell()),
            Some(invalid_flat),
            "no component may land outside valid support"
        );
    }

    // A window restricted to the invalid pixel has no valid support at all.
    let invalid_only = CleanWindow::new([3, 3], [3, 3]).expect("single-cell window");
    assert!(matches!(
        hogbom_minor_cycle(
            &open,
            &final_model,
            &normal_state,
            invalid_only,
            HogbomControls::new(0.5, 0.0, 8, 1.0e30).expect("valid controls"),
        ),
        Err(MinorCycleError::EmptyValidSupport)
    ));

    // Components respect the declared window bounds.
    let quarter_window = CleanWindow::inner_quarter(SHAPE);
    let quarter_outcome = hogbom_minor_cycle(
        &open,
        &final_model,
        &normal_state,
        quarter_window,
        solving_controls,
    )
    .expect("quarter-window solve");
    for component in quarter_outcome
        .evidence()
        .recorded_component_sequence()
        .expect("recording requested")
    {
        let pixel = component.cell().pixel();
        assert!(pixel[0] >= quarter_window.blc()[0] && pixel[0] <= quarter_window.trc()[0]);
        assert!(pixel[1] >= quarter_window.blc()[1] && pixel[1] <= quarter_window.trc()[1]);
    }
}

#[test]
fn mismatched_lineage_geometry_and_windows_fail_closed() {
    let round = first_confirm_round(62, 63);

    // A generation minted by another lifecycle authority is foreign even with
    // identical content shape.
    let other = problem_with_model(64, ModelStateIdentity::Empty);
    let other_lifecycle = bind_lifecycle(&other, 65, 14);
    let foreign_base = other_lifecycle.initial_empty().expect("foreign generation");
    assert!(matches!(
        hogbom_minor_cycle(
            &other_lifecycle,
            &foreign_base,
            &round.normal_state,
            CleanWindow::full_plane(SHAPE),
            controls(),
        ),
        Err(MinorCycleError::ForeignNormalState)
    ));

    // A base from another model space cannot address this plane. The shape
    // guard fires before the lineage check, so a 12x12 base fails on shape
    // even though the window would still fit.
    let wide_problem = problem_with_model_and_width(66, ModelStateIdentity::Empty, 12);
    let wide_lifecycle = bind_lifecycle(&wide_problem, 67, 15);
    let wide_base = wide_lifecycle.initial_empty().expect("wide generation");
    assert!(matches!(
        hogbom_minor_cycle(
            &wide_lifecycle,
            &wide_base,
            &round.normal_state,
            CleanWindow::full_plane(SHAPE),
            controls(),
        ),
        Err(MinorCycleError::ModelShapeMismatch)
    ));

    // The remaining guards use an open lifecycle bound to this chain.
    let continuation = problem_with_model(
        69,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let lifecycle = bind_lifecycle(&continuation, 67, 15);

    // Windows must lie inside the plane.
    assert!(matches!(
        hogbom_minor_cycle(
            &lifecycle,
            &round.final_model,
            &round.normal_state,
            CleanWindow::new([6, 6], [10, 10]).expect("ordered window"),
            controls(),
        ),
        Err(MinorCycleError::WindowOutsidePlane)
    ));
}

#[test]
fn component_sequence_divergence_is_informational_only() {
    let round = first_confirm_round(70, 71);
    let continuation = problem_with_model(
        72,
        ModelStateIdentity::Generation(round.final_model.generation_id().identity()),
    );
    let first = bind_lifecycle(&continuation, 73, 16);
    let second = bind_lifecycle(&continuation, 73, 16);

    let baseline = hogbom_minor_cycle(
        &first,
        &round.final_model,
        &round.normal_state,
        CleanWindow::full_plane(SHAPE),
        controls(),
    )
    .expect("baseline solve");
    let halved_gain = HogbomControls::new(0.25, 1.0e-30, 64, 1.0e30)
        .expect("valid controls")
        .record_component_sequence(64)
        .expect("recording limit");
    let candidate = hogbom_minor_cycle(
        &second,
        &round.final_model,
        &round.normal_state,
        CleanWindow::full_plane(SHAPE),
        halved_gain,
    )
    .expect("candidate solve");

    // Different gains diverge immediately, but the comparison is purely
    // informational: it reports facts and never fails either evidence.
    let divergence = candidate
        .evidence()
        .first_divergence(baseline.evidence())
        .expect("different gains diverge");
    let index = divergence.index();
    let b = divergence.baseline().expect("baseline entry present");
    let c = divergence.candidate().expect("candidate entry present");
    assert_eq!(index, 0);
    assert_eq!(b.cell(), c.cell(), "the peak pixel is gain-independent");
    assert_ne!(b.flux().to_bits(), c.flux().to_bits());
    assert!((c.flux() - b.flux() / 2.0).abs() <= 1.0e-12 * b.flux().abs());

    // Sequence-length differences report the terminal divergence.
    let shorter_controls = HogbomControls::new(0.5, 1.0e-30, 64, 1.0e30)
        .expect("valid controls")
        .record_component_sequence(1)
        .expect("recording limit");
    let third = bind_lifecycle(&continuation, 73, 16);
    let shorter = hogbom_minor_cycle(
        &third,
        &round.final_model,
        &round.normal_state,
        CleanWindow::full_plane(SHAPE),
        shorter_controls,
    )
    .expect("short-recording solve");
    let terminal = shorter
        .evidence()
        .first_divergence(baseline.evidence())
        .expect("lengths differ");
    assert_eq!(terminal.index(), 1);
    assert!(terminal.baseline().is_some());
    assert!(terminal.candidate().is_none());

    // Unrecorded sequences simply cannot be compared.
    let unrecorded_controls = HogbomControls::new(0.5, 1.0e-30, 64, 1.0e30).expect("no recording");
    let fourth = bind_lifecycle(&continuation, 73, 16);
    let unrecorded = hogbom_minor_cycle(
        &fourth,
        &round.final_model,
        &round.normal_state,
        CleanWindow::full_plane(SHAPE),
        unrecorded_controls,
    )
    .expect("unrecorded solve");
    assert!(unrecorded
        .evidence()
        .first_divergence(baseline.evidence())
        .is_none());
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
