// SPDX-License-Identifier: LGPL-3.0-or-later

//! T20 Major-Cycle reconciliation join over T19 complete-data evidence and
//! the T28 model lifecycle, driven entirely through owner seams.

use std::convert::Infallible;

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    ModelBounds, ModelCell, ModelColumnState, ModelColumnWrite, ModelDeltaTerm,
    ModelExecutionAttemptId, ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements,
    ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage, NumericsContract,
    ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
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
    ExecutableModelProblem, FinalModelCompletionId, MajorCycleError, MajorCycleOwner,
    MajorCyclePreparation, ModelDelta, ModelGeneration, ModelLifecycle, ModelLifecycleError,
    SerialMfsError, SerialMfsSpecification, WeightingAlgorithmState, WeightingError,
    WeightingExecutionLimits, WeightingPlan, WeightingReplayChunk, WeightingReplaySummary,
    begin_weighting_generation, plan_weighting,
    runtime_adapter::{CompleteDataOwnerResult, prepare_serial_mfs_operator, serial_mfs_workload},
};

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

fn t19_compatible_problem(observation: u8) -> casa_imaging_model::CompiledProblem {
    t19_compatible_problem_with_width(observation, 8)
}

fn t19_compatible_problem_with_width(
    observation: u8,
    width: usize,
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
        ModelStateIdentity::Empty,
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
            ModelInputCommitment::Empty,
        ),
    ))
    .expect("compile T20 reconciliation problem")
}

/// The fixture sample stream for one compiled problem.
///
/// Distinct fixture problems describe distinct observation streams: the
/// sample content carries one byte of the compiled problem identity so the
/// minted T17 generation differs whenever the source content differs.
fn fixture_samples(
    problem: &casa_imaging_model::CompiledProblem,
) -> Vec<SelectedObservationSample> {
    let mut samples = exact_samples(problem);
    let flavour = f64::from(u32::from(problem.problem_id().as_bytes()[0] % 8));
    for sample in &mut samples {
        sample.input_weight = (sample.input_weight as f64 * (1.0 + flavour * 0.125)) as f32;
    }
    samples
}

fn exact_samples(problem: &casa_imaging_model::CompiledProblem) -> Vec<SelectedObservationSample> {
    let mut samples = Vec::new();
    for (source_index, source) in problem
        .selected_observation()
        .read_set()
        .sources()
        .iter()
        .enumerate()
    {
        for (row_index, physical_row) in [0_u64, 2].into_iter().enumerate() {
            let data_description_id = row_index as i32;
            let spectral_window_id = row_index as u32;
            let channel_index = row_index as u32;
            let frequency = if row_index == 0 { 1.05e9 } else { 1.15e9 };
            samples.push(SelectedObservationSample {
                address: SelectedSampleAddress {
                    measurement_set: source.measurement_set(),
                    physical_row,
                    data_description_id,
                    spectral_window_id,
                    channel_index,
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

/// Mint the authoritative T17 observation generation of the fixture stream.
///
/// Production binds this identity through the casa-ms traversal seam; the
/// reconstruction fixtures use the same compiler-owned inspection pass, so no
/// second construction path exists.
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

/// Drive one exhaustive T19 complete-data replay and mint its paired evidence.
///
/// The returned value keeps primitives and completion inseparably paired; the
/// only way to split them is to hand the whole result to the Major-Cycle owner.
fn run_t19_complete_data(
    problem: &casa_imaging_model::CompiledProblem,
    preparation: Option<&MajorCyclePreparation>,
) -> CompleteDataOwnerResult {
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting residency plan");
    let samples = fixture_samples(problem);
    let selected_generation = replay_selected_generation(problem, &samples);
    let generation = freeze_weighting_generation(problem, &plan, &samples)
        .expect("freeze global weighting generation");
    let (blocks, summary) = replay(&generation, problem, &plan, &samples);
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

fn prepare_reconciliation(
    problem: &casa_imaging_model::CompiledProblem,
    lifecycle: &ModelLifecycle,
    named: ModelGeneration,
    delta: Option<ModelDelta>,
) -> (CompleteDataOwnerResult, MajorCyclePreparation) {
    let preparation =
        MajorCyclePreparation::prepare(lifecycle, named, delta).expect("prepare final model");
    let evidence = run_t19_complete_data(problem, Some(&preparation));
    (evidence, preparation)
}

#[test]
fn bound_major_cycle_model_cannot_be_replaced_by_diagnostic_prediction() {
    let problem = t19_compatible_problem(37);
    let lifecycle = bind_lifecycle(&problem, attempt(38));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("prepare final model");
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let samples = fixture_samples(&problem);
    let generation = freeze_weighting_generation(&problem, &plan, &samples)
        .expect("freeze weighting generation");
    let (blocks, _) = replay(&generation, &problem, &plan, &samples);
    let specification = SerialMfsSpecification::new(&problem).expect("serial MFS specification");
    let workload = serial_mfs_workload(&specification, plan.limits().max_block_samples())
        .expect("serial MFS workload");
    let prepared = prepare_serial_mfs_operator(specification, workload).expect("prepare operator");
    let mut state = prepared
        .begin(&problem, &generation)
        .expect("begin complete-data owner");
    state
        .bind_major_cycle_model(preparation.final_model())
        .expect("bind final model");
    let arbitrary_model = vec![num_complex::Complex64::new(1.0, 0.0); 8 * 8];

    assert_eq!(
        state
            .predict_block(&arbitrary_model, &blocks[0])
            .expect_err("prediction must not overwrite the final-model grid"),
        SerialMfsError::PredictionAfterMajorCycleBinding
    );
}

fn bind_lifecycle(
    problem: &casa_imaging_model::CompiledProblem,
    attempt: ModelExecutionAttemptId,
) -> ModelLifecycle {
    ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem"),
        attempt,
        7,
    )
    .expect("bind model lifecycle")
}

fn cell(x: usize) -> ModelCell {
    ModelCell::new(0, 0, 0, [x, 0])
}

fn delta_value(value: f64) -> casa_imaging_model::ModelValue {
    casa_imaging_model::ModelValue::new(value).expect("finite model value")
}

#[test]
fn schema_versions_record_the_t20_completion_records() {
    assert_eq!(FinalModelCompletionId::SCHEMA_VERSION, 2);
    assert_eq!(
        casa_imaging_reconstruction::FinalNormalStateCompletionId::SCHEMA_VERSION,
        2
    );
    assert_eq!(
        casa_imaging_reconstruction::MajorCycleCompletionId::SCHEMA_VERSION,
        2
    );
}

#[test]
fn reconciliation_applies_one_pending_delta_through_the_model_owner() {
    let problem = t19_compatible_problem(11);
    let mut lifecycle = bind_lifecycle(&problem, attempt(21));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let delta = lifecycle
        .compile_delta(&named, [ModelDeltaTerm::new(cell(1), delta_value(-2.5))])
        .expect("pending Högbom-style delta");
    let delta_id = delta.delta_id();
    let (evidence, preparation) = prepare_reconciliation(&problem, &lifecycle, named, Some(delta));
    // The data-side dirty plane remains T19 evidence beside the exact residual.
    let data_side_content = evidence.primitives().normal_state_content_identity();
    let sample_count = evidence.completion().sample_count();
    let block_count = evidence.completion().block_count();
    let weighting_generation = evidence.completion().weighting_generation();

    let owner =
        MajorCycleOwner::from_complete_data(evidence, preparation).expect("T20 owner from T19");
    assert_eq!(owner.weighting_generation(), weighting_generation);
    let joined = owner
        .reconcile(&mut lifecycle)
        .expect("atomic Major-Cycle reconciliation");

    // One inseparable result carrying two distinct opaque typed records plus
    // the authoritative final model generation.
    let normal_state = joined.normal_state();
    let model_completion = joined.model_completion();
    assert_ne!(
        normal_state.completion_id().as_bytes(),
        model_completion.completion_id().as_bytes()
    );
    assert_ne!(
        joined.completion_id().as_bytes(),
        normal_state.completion_id().as_bytes()
    );

    // The Normal State record names the full T17/T18/T19 lineage and both models.
    assert_eq!(normal_state.problem_id(), problem.problem_id());
    assert_eq!(normal_state.geometry_id(), problem.geometry().geometry_id());
    assert_eq!(normal_state.numerics_id(), problem.numerics_id());
    assert_eq!(normal_state.sample_count(), sample_count);
    assert_eq!(normal_state.block_count(), block_count);
    assert_eq!(
        normal_state.catalog(),
        casa_imaging_reconstruction::NormalStateCatalog::UnnormalizedNterms1V1
    );
    // The residual content is model-dependent: a nonzero final model never
    // relabels the data-side dirty plane.
    assert_ne!(normal_state.content_identity(), data_side_content);
    assert_eq!(
        normal_state.input_model_generation(),
        model_completion.base()
    );
    assert_eq!(
        normal_state.final_model_generation(),
        model_completion.generation()
    );
    assert_eq!(
        joined.final_model().generation_id(),
        model_completion.generation()
    );
    assert!(joined.final_model().samples()[1].value().value() == -2.5);

    // The pending delta was applied only through the model owner.
    assert_eq!(model_completion.delta(), Some(delta_id));
    assert_eq!(model_completion.attempt(), attempt(21));
    assert_eq!(model_completion.epoch(), 7);
}

#[test]
fn reconciliation_without_a_pending_delta_confirms_the_named_generation_final() {
    let problem = t19_compatible_problem(12);
    let mut lifecycle = bind_lifecycle(&problem, attempt(22));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let input_id = named.generation_id();
    let (evidence, preparation) = prepare_reconciliation(&problem, &lifecycle, named, None);
    let data_side_content = evidence.primitives().normal_state_content_identity();
    let data_side_dirty = evidence.primitives().dirty().to_vec();

    let owner =
        MajorCycleOwner::from_complete_data(evidence, preparation).expect("T20 owner from T19");
    let joined = owner
        .reconcile(&mut lifecycle)
        .expect("confirm-only reconciliation");

    assert_eq!(joined.model_completion().delta(), None);
    assert_eq!(joined.model_completion().base(), input_id);
    assert_eq!(joined.model_completion().generation(), input_id);
    assert_eq!(joined.normal_state().input_model_generation(), input_id);
    assert_eq!(joined.normal_state().final_model_generation(), input_id);
    // An empty final model reconciles to the exact T19 dirty plane bit-for-bit.
    assert_eq!(joined.normal_state().content_identity(), data_side_content);
    assert_eq!(joined.normal_state().residual(), data_side_dirty);
    assert_eq!(joined.normal_state().normal_approximation().len(), 8 * 8);
    assert_eq!(joined.normal_state().sensitivity().len(), 8 * 8);
    assert!(joined.normal_state().sum_weight() > 0.0);
}

#[test]
fn residual_content_depends_on_the_exact_final_model() {
    let problem = t19_compatible_problem(27);
    // The same T19 evidence reconciled against two different final models.
    let mut empty_lifecycle = bind_lifecycle(&problem, attempt(28));
    let empty_named = empty_lifecycle
        .initial_empty()
        .expect("empty named generation");
    let (empty_evidence, empty_preparation) =
        prepare_reconciliation(&problem, &empty_lifecycle, empty_named, None);
    let empty_join = MajorCycleOwner::from_complete_data(empty_evidence, empty_preparation)
        .expect("owner from intact T19 pairing")
        .reconcile(&mut empty_lifecycle)
        .expect("empty-model reconciliation");

    let mut delta_lifecycle = bind_lifecycle(&problem, attempt(29));
    let delta_named = delta_lifecycle
        .initial_empty()
        .expect("empty named generation");
    let delta = delta_lifecycle
        .compile_delta(
            &delta_named,
            [ModelDeltaTerm::new(cell(3), delta_value(1.75))],
        )
        .expect("pending delta");
    let (delta_evidence, delta_preparation) =
        prepare_reconciliation(&problem, &delta_lifecycle, delta_named, Some(delta));
    let delta_join = MajorCycleOwner::from_complete_data(delta_evidence, delta_preparation)
        .expect("owner from intact T19 pairing")
        .reconcile(&mut delta_lifecycle)
        .expect("delta reconciliation");

    assert_ne!(
        empty_join.normal_state().content_identity(),
        delta_join.normal_state().content_identity(),
        "a nonzero final model must change the authoritative residual content"
    );
    assert_ne!(
        empty_join.normal_state().residual(),
        delta_join.normal_state().residual(),
        "the retained authoritative residual must depend on the final model"
    );
    assert_ne!(empty_join.completion_id(), delta_join.completion_id());
    assert_ne!(
        empty_join.normal_state().final_model_generation(),
        delta_join.normal_state().final_model_generation()
    );
}

#[test]
fn completion_ids_stay_stable_across_owner_allocations() {
    let problem = t19_compatible_problem(30);
    // Two independent reconciliation passes over identical evidence, each with
    // its own process-local lifecycle allocation but the same stable
    // problem/attempt/epoch binding.
    let mut first = bind_lifecycle(&problem, attempt(31));
    let mut second = bind_lifecycle(&problem, attempt(31));
    let first_named = first.initial_empty().expect("first named generation");
    let second_named = second.initial_empty().expect("second named generation");
    let (first_evidence, first_preparation) =
        prepare_reconciliation(&problem, &first, first_named, None);
    let (second_evidence, second_preparation) =
        prepare_reconciliation(&problem, &second, second_named, None);

    let first_join = MajorCycleOwner::from_complete_data(first_evidence, first_preparation)
        .expect("first owner")
        .reconcile(&mut first)
        .expect("first reconciliation");
    let second_join = MajorCycleOwner::from_complete_data(second_evidence, second_preparation)
        .expect("second owner")
        .reconcile(&mut second)
        .expect("second reconciliation");

    assert_eq!(
        first_join.completion_id(),
        second_join.completion_id(),
        "completion IDs hash the stable lifecycle authority, not a process-local seal"
    );
    assert_eq!(
        first_join.normal_state().completion_id(),
        second_join.normal_state().completion_id()
    );
}

#[test]
fn observation_generation_lineage_is_bound_into_the_normal_state() {
    // Two problems whose fixture streams carry different observation content.
    let first_problem = t19_compatible_problem(33);
    let second_problem = t19_compatible_problem_with_width(34, 12);

    for (problem, attempt_byte) in [(&first_problem, 35_u8), (&second_problem, 36)] {
        let expected = replay_selected_generation(problem, &fixture_samples(problem));
        let mut lifecycle = bind_lifecycle(problem, attempt(attempt_byte));
        let named = lifecycle.initial_empty().expect("named generation");
        let (evidence, preparation) = prepare_reconciliation(problem, &lifecycle, named, None);
        let join = MajorCycleOwner::from_complete_data(evidence, preparation)
            .expect("owner from intact T19 pairing")
            .reconcile(&mut lifecycle)
            .expect("reconciliation");
        assert_eq!(
            join.normal_state().selected_generation(),
            expected,
            "Final Normal State carries the exact authoritative observation generation"
        );
    }

    // Distinct observation streams never share one lineage identity.
    let first_generation =
        replay_selected_generation(&first_problem, &fixture_samples(&first_problem));
    let second_generation =
        replay_selected_generation(&second_problem, &fixture_samples(&second_problem));
    assert_ne!(first_generation, second_generation);
}

#[test]
fn reconciliation_fails_atomically_and_leaves_both_authorities_intact() {
    let problem = t19_compatible_problem(13);
    let other_problem = t19_compatible_problem(14);

    // A lifecycle bound to another compiled problem is stale model evidence.
    let mut foreign_problem_lifecycle = bind_lifecycle(&other_problem, attempt(23));
    let foreign_named = foreign_problem_lifecycle
        .initial_empty()
        .expect("foreign empty generation");
    let foreign_preparation =
        MajorCyclePreparation::prepare(&foreign_problem_lifecycle, foreign_named, None)
            .expect("prepare foreign problem model");
    let foreign_evidence = run_t19_complete_data(&problem, Some(&foreign_preparation));
    let stale_problem = MajorCycleOwner::from_complete_data(foreign_evidence, foreign_preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut foreign_problem_lifecycle)
        .expect_err("stale model evidence must fail closed");
    assert!(matches!(stale_problem, MajorCycleError::StaleModelEvidence));

    let mut lifecycle = bind_lifecycle(&problem, attempt(23));

    // A foreign generation cannot be named through this lifecycle owner.
    let other_owner_same_problem = bind_lifecycle(&problem, attempt(24));
    let foreign_generation = other_owner_same_problem
        .initial_empty()
        .expect("same-problem foreign generation");
    let foreign = MajorCyclePreparation::prepare(&lifecycle, foreign_generation, None)
        .expect_err("foreign generation must fail before replay");
    assert!(matches!(
        foreign,
        MajorCycleError::Model(ModelLifecycleError::ForeignModelLifecycle)
    ));

    // A delta bound to another base fails before anything is minted, and the
    // lifecycle remains open for a correct reconciliation afterwards.
    let scratch_base = lifecycle.initial_empty().expect("scratch base generation");
    let bump = lifecycle
        .compile_delta(
            &scratch_base,
            [ModelDeltaTerm::new(cell(3), delta_value(0.5))],
        )
        .expect("non-final scratch delta");
    let alternative_base = lifecycle
        .apply_delta(scratch_base, bump)
        .expect("advanced scratch base");
    let misbound_delta = lifecycle
        .compile_delta(
            &alternative_base,
            [ModelDeltaTerm::new(cell(2), delta_value(1.0))],
        )
        .expect("delta against the alternative base");
    let named = lifecycle.initial_empty().expect("fresh named generation");
    assert_ne!(alternative_base.generation_id(), named.generation_id());
    let misbound = MajorCyclePreparation::prepare(&lifecycle, named, Some(misbound_delta))
        .expect_err("misbound delta must fail before replay");
    assert!(matches!(
        misbound,
        MajorCycleError::Model(ModelLifecycleError::DeltaBaseMismatch)
    ));

    // The same authorities then complete exactly once.
    let named = lifecycle.initial_empty().expect("named after repairs");
    let delta = lifecycle
        .compile_delta(&named, [ModelDeltaTerm::new(cell(2), delta_value(1.0))])
        .expect("correctly bound delta");
    let (evidence, preparation) = prepare_reconciliation(&problem, &lifecycle, named, Some(delta));
    let joined = MajorCycleOwner::from_complete_data(evidence, preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut lifecycle)
        .expect("reconciliation succeeds after atomic failures");
    assert_eq!(
        joined.model_completion().attempt(),
        casa_imaging_model::ModelExecutionAttemptId::new(identity(23, 0))
    );

    // Mutation and replay are impossible: the final authority is consumed.
    let late_base = lifecycle.initial_empty();
    assert!(matches!(
        late_base,
        Err(ModelLifecycleError::FinalModelAlreadyCompleted)
    ));
}

#[test]
fn incomplete_or_foreign_operator_evidence_cannot_become_a_major_cycle_owner() {
    // A specification mismatch keeps raw problems from forging operator plans.
    let problem = t19_compatible_problem(15);
    let other = t19_compatible_problem(16);
    let specification = SerialMfsSpecification::new(&other).expect("other specification");
    let workload = serial_mfs_workload(&specification, 1).expect("other workload");
    let prepared = prepare_serial_mfs_operator(specification, workload).expect("prepared");
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("limits"),
    )
    .expect("plan");
    let samples = exact_samples(&problem);
    let generation = freeze_weighting_generation(&problem, &plan, &samples).expect("frozen W");
    let foreign = prepared
        .begin(&problem, &generation)
        .expect_err("foreign prepared operator cannot adopt this problem");
    assert!(matches!(foreign, SerialMfsError::ProblemMismatch));

    // Exhaustive coverage is required before any owner can exist.
    let evidence = run_t19_complete_data(&problem, None);
    let completion = evidence.completion();
    assert!(completion.sample_count() > 0 && completion.block_count() > 0);
    assert_eq!(completion.problem_id(), problem.problem_id());
}
