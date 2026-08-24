// SPDX-License-Identifier: LGPL-3.0-or-later

//! T20 Major-Cycle reconciliation join over T19 complete-data evidence and
//! the T28 model lifecycle, driven entirely through owner seams.

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    ModelBounds, ModelCell, ModelColumnState, ModelColumnWrite, ModelDeltaTerm,
    ModelExecutionAttemptId, ModelInputCommitment, ModelInnerProduct, ModelLifecycleRequirements,
    ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage, NumericsContract,
    ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection,
    ScientificContract, SelectedColumns, SelectedMainRow, SelectedObservationSample,
    SelectedPredictionTarget, SelectedRows, SelectedSampleAddress, SelectedSampleCoordinates,
    SelectedSampleMetadata, SelectedSpectralContribution, SelectedSpectralContributions,
    SelectedVisibilitySample, SkyDirection, SourceGenerations, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSampling, SpectralWcs,
    SpectralWindowSelection, StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy,
    TimeScale, TimeSelection, UvSelection, UvwCoordinateLaw, VisibilityColumn,
    VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract, WeightingScheme,
    compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FinalModelCompletionId, MajorCycleError, MajorCycleOwner,
    ModelLifecycle, ModelLifecycleError, SerialMfsError, SerialMfsSpecification,
    WeightingAlgorithmState, WeightingError, WeightingExecutionLimits, WeightingPlan,
    WeightingReplayChunk, WeightingReplaySummary, begin_weighting_generation, plan_weighting,
    runtime_adapter::{CompleteDataOwnerCompletion, prepare_serial_mfs_operator, serial_mfs_workload},
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
        ObservationSourceProvenance::new(format!("fixture://major-cycle/{seed}"), identity(seed, 2)),
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
    let centre = ((width - 1) / 2) as f64;
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
    let mut phase = generation.begin_replay(problem, plan).expect("begin replay");
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

/// Drive one exhaustive T19 complete-data replay and mint its owner evidence.
fn run_t19_complete_data(
    problem: &casa_imaging_model::CompiledProblem,
) -> (
    casa_imaging_reconstruction::SerialMfsPrimitives,
    CompleteDataOwnerCompletion,
) {
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting residency plan");
    let samples = exact_samples(problem);
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
    for block in &blocks {
        state.consume_block(block).expect("consume weighted block");
    }
    let result = state.complete(&summary).expect("complete T19 evidence");
    result.into_parts()
}

fn bind_lifecycle(
    problem: &casa_imaging_model::CompiledProblem,
    attempt: ModelExecutionAttemptId,
) -> ModelLifecycle {
    ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone())
            .expect("direct executable problem"),
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
        1
    );
    assert_eq!(
        casa_imaging_reconstruction::MajorCycleCompletionId::SCHEMA_VERSION,
        1
    );
}

#[test]
fn reconciliation_applies_one_pending_delta_through_the_model_owner() {
    let problem = t19_compatible_problem(11);
    let (primitives, completion) = run_t19_complete_data(&problem);
    let mut lifecycle = bind_lifecycle(&problem, attempt(21));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let delta = lifecycle
        .compile_delta(&named, [ModelDeltaTerm::new(cell(1), delta_value(-2.5))])
        .expect("pending Högbom-style delta");
    let delta_id = delta.delta_id();

    let owner = MajorCycleOwner::from_owner_evidence(&completion, &primitives).expect("T20 owner from T19");
    assert_eq!(owner.weighting_generation(), completion.weighting_generation());
    let joined = owner
        .reconcile(&mut lifecycle, named, Some(delta), &primitives)
        .expect("atomic Major-Cycle reconciliation");

    // One inseparable result carrying two distinct opaque typed records.
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

    // The Normal State record names the full T18/T19 lineage and both models.
    assert_eq!(normal_state.problem_id(), problem.problem_id());
    assert_eq!(normal_state.geometry_id(), problem.geometry().geometry_id());
    assert_eq!(normal_state.numerics_id(), problem.numerics_id());
    assert_eq!(
        normal_state.weighting_commitment_id(),
        completion.weighting_commitment_id()
    );
    assert_eq!(
        normal_state.weighting_generation(),
        completion.weighting_generation()
    );
    assert_eq!(normal_state.replay_id(), completion.replay_id());
    assert_eq!(normal_state.coverage(), completion.coverage());
    assert_eq!(normal_state.sample_count(), completion.sample_count());
    assert_eq!(normal_state.block_count(), completion.block_count());
    assert_ne!(normal_state.content_identity().as_bytes(), [0u8; 32]);
    assert_eq!(
        normal_state.content_identity(),
        primitives.normal_state_content_identity()
    );
    assert_eq!(
        normal_state.catalog(),
        casa_imaging_reconstruction::NormalStateCatalog::UnnormalizedNterms1V1
    );
    assert_eq!(
        normal_state.final_model_generation(),
        model_completion.generation()
    );

    // The pending delta was applied only through the model owner.
    assert_eq!(model_completion.delta(), Some(delta_id));
    assert_eq!(model_completion.attempt(), attempt(21));
    assert_eq!(model_completion.epoch(), 7);
    assert_eq!(model_completion.generation(), {
        // The authoritative final generation differs from its named input.
        assert_ne!(
            normal_state.final_model_generation(),
            normal_state.input_model_generation()
        );
        normal_state.final_model_generation()
    });
}

#[test]
fn reconciliation_without_a_pending_delta_confirms_the_named_generation_final() {
    let problem = t19_compatible_problem(12);
    let (primitives, completion) = run_t19_complete_data(&problem);
    let mut lifecycle = bind_lifecycle(&problem, attempt(22));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let input_id = named.generation_id();

    let owner = MajorCycleOwner::from_owner_evidence(&completion, &primitives).expect("T20 owner from T19");
    let joined = owner
        .reconcile(&mut lifecycle, named, None, &primitives)
        .expect("confirm-only reconciliation");

    assert_eq!(joined.model_completion().delta(), None);
    assert_eq!(joined.model_completion().base(), input_id);
    assert_eq!(joined.model_completion().generation(), input_id);
    assert_eq!(joined.normal_state().input_model_generation(), input_id);
    assert_eq!(joined.normal_state().final_model_generation(), input_id);
}

#[test]
fn reconciliation_fails_atomically_and_leaves_both_authorities_intact() {
    let problem = t19_compatible_problem(13);
    let (primitives, completion) = run_t19_complete_data(&problem);

    // A lifecycle bound to another compiled problem is stale model evidence.
    let other_problem = t19_compatible_problem(14);
    let mut foreign_problem_lifecycle = bind_lifecycle(&other_problem, attempt(23));
    let foreign_named = foreign_problem_lifecycle
        .initial_empty()
        .expect("foreign empty generation");
    let mut lifecycle = bind_lifecycle(&problem, attempt(23));

    let owner = MajorCycleOwner::from_owner_evidence(&completion, &primitives).expect("T20 owner from T19");
    let stale_problem = owner
        .reconcile(
            &mut foreign_problem_lifecycle,
            foreign_named,
            None,
            &primitives,
        )
        .expect_err("stale model evidence must fail closed");
    assert!(matches!(stale_problem, MajorCycleError::StaleModelEvidence));

    // A foreign generation cannot be named through this lifecycle owner.
    let other_owner_same_problem = bind_lifecycle(&problem, attempt(24));
    let foreign_generation = other_owner_same_problem
        .initial_empty()
        .expect("same-problem foreign generation");
    let owner = MajorCycleOwner::from_owner_evidence(&completion, &primitives).expect("T20 owner from T19");
    let foreign = owner
        .reconcile(&mut lifecycle, foreign_generation, None, &primitives)
        .expect_err("foreign generation must fail closed");
    assert!(matches!(
        foreign,
        MajorCycleError::Model(ModelLifecycleError::ForeignModelLifecycle)
    ));

    // A delta bound to another base fails before anything is minted, and the
    // lifecycle remains open for a correct reconciliation afterwards.
    let scratch_base = lifecycle.initial_empty().expect("scratch base generation");
    let bump = lifecycle
        .compile_delta(&scratch_base, [ModelDeltaTerm::new(cell(3), delta_value(0.5))])
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
    let owner = MajorCycleOwner::from_owner_evidence(&completion, &primitives).expect("T20 owner from T19");
    let misbound = owner
        .reconcile(&mut lifecycle, named, Some(misbound_delta), &primitives)
        .expect_err("misbound delta must fail atomically");
    assert!(matches!(
        misbound,
        MajorCycleError::Model(ModelLifecycleError::DeltaBaseMismatch)
    ));

    // The same authorities then complete exactly once.
    let named = lifecycle.initial_empty().expect("named after repairs");
    let delta = lifecycle
        .compile_delta(&named, [ModelDeltaTerm::new(cell(2), delta_value(1.0))])
        .expect("correctly bound delta");
    let owner = MajorCycleOwner::from_owner_evidence(&completion, &primitives).expect("T20 owner from T19");
    let joined = owner
        .reconcile(&mut lifecycle, named, Some(delta), &primitives)
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
fn substituted_normal_state_evidence_fails_atomically() {
    let problem = t19_compatible_problem(17);
    let (true_primitives, true_completion) = run_t19_complete_data(&problem);

    // Deliberately pair another execution's primitives with this completion;
    // reconciliation accepts only the exact bound pairing.
    let other_problem = t19_compatible_problem_with_width(18, 12);
    let (other_primitives, _) = run_t19_complete_data(&other_problem);
    let mispaired = MajorCycleOwner::from_owner_evidence(
        &true_completion,
        &other_primitives,
    )
    .expect("the constructor binds exactly the pairing it is given");

    let mut lifecycle = bind_lifecycle(&problem, attempt(25));
    let named = lifecycle.initial_empty().expect("empty named generation");
    let rejected = mispaired
        .reconcile(&mut lifecycle, named, None, &true_primitives)
        .expect_err("substituted primitives must fail atomically");
    assert!(matches!(rejected, MajorCycleError::MutatedNormalStateEvidence));

    // The correctly paired owner still reconciles afterwards.
    let mut lifecycle = bind_lifecycle(&problem, attempt(26));
    let named = lifecycle.initial_empty().expect("fresh empty generation");
    let joined = MajorCycleOwner::from_owner_evidence(
        &true_completion,
        &true_primitives,
    )
    .expect("owner from the exact T19 pairing")
    .reconcile(&mut lifecycle, named, None, &true_primitives)
    .expect("correct pairing reconciles");
    assert_eq!(joined.model_completion().delta(), None);
}

#[test]
fn incomplete_or_foreign_operator_evidence_cannot_become_a_major_cycle_owner() {
    let problem = t19_compatible_problem(15);
    let (primitives, completion) = run_t19_complete_data(&problem);
    let owner =
        MajorCycleOwner::from_owner_evidence(&completion, &primitives).expect("owner from T19 evidence");
    assert_eq!(owner.weighting_generation(), completion.weighting_generation());

    // A specification mismatch keeps raw problems from forging operator plans.
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

}
