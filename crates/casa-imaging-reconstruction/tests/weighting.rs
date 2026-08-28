// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    ModelBounds, ModelColumnState, ModelColumnWrite, ModelInputCommitment,
    ModelLifecycleRequirements, ModelStateIdentity, MsColumnKind, NumericPrecision, NumericalStage,
    NumericsContract, ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
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
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, SpectralWindowSelection, StageErrorBudget, TaylorSupportReference,
    TaylorValidityPolicy, TimeScale, TimeSelection, UvSelection, UvTaper, UvwCoordinateLaw,
    VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract,
    WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    WeightingAlgorithmState, WeightingExecutionLimits, WeightingReplayChunk,
    WeightingReplaySummary, begin_natural_weighting_stream, begin_weighting_generation,
    plan_weighting,
};

fn identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut bytes = [seed; 32];
    bytes[0] = scope;
    LogicalIdentity::from_sha256(bytes)
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
        ObservationSourceProvenance::new(format!("fixture://weighting/{seed}"), identity(seed, 2)),
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

fn problem(
    scheme: WeightingScheme,
    scope: WeightDensityScope,
    taper: Option<UvTaper>,
) -> casa_imaging_model::CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [16.0, 16.0],
        [-0.01, 0.01],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(32, 32),
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
            SpectralWcs::Tabular {
                channel_centres_hz: vec![1.05e9, 1.15e9],
                channel_boundaries_hz: vec![1.0e9, 1.1e9, 1.2e9],
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source(1), source(2)],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile multi-source observation");
    let mut weighting = WeightingContract::new(scheme, scope);
    if let Some(taper) = taper {
        weighting = weighting.with_uv_taper(taper);
    }
    compile(ImagingRequest::new(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(SpectralSamplingLaw::LINEAR, SpectralCoupling::Independent),
                MeasurementEquationContract::new(
                    InstrumentResponse::Scalar,
                    DeclaredInnerProducts::new(
                        casa_imaging_model::ModelInnerProduct::HermitianEuclidean,
                        VisibilityInnerProduct::HermitianEuclidean,
                    ),
                ),
            ),
            ReconstructionContract::new(
                ReconstructionBasis::ChannelLocal { channels: 2 },
                ReconstructionAlgorithm::Dirty,
                ReconstructionControls::new(0, 1.0, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            weighting,
            ProductRequirements::new(
                vec![ProductKind::Psf, ProductKind::SumWeights],
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::None,
                validity(),
            ),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            NumericsContract::new(
                vec![NumericPrecision::F64],
                ReductionPolicy::DeterministicPairwise,
                FiniteValuePolicy::FlagInputRejectGenerated,
                NumericalStage::ALL
                    .into_iter()
                    .map(|stage| (stage, StageErrorBudget::new(1.0e-12, 1.0e-9)))
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
    .expect("compile weighting problem")
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
                parallel_hand_group_flag: false,
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
        SelectedSpectralContribution::new(
            sample.address.channel_index,
            1.0,
            sample.address.frequency_centre_hz,
        ),
        None,
    ])
    .expect("one exact output contribution")
}

fn freeze_weighting_generation(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_reconstruction::WeightingPlan,
    samples: &[SelectedObservationSample],
) -> Result<WeightingAlgorithmState, casa_imaging_reconstruction::WeightingError> {
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
    plan: &casa_imaging_reconstruction::WeightingPlan,
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

fn replay_with_evaluation_frequency(
    generation: &WeightingAlgorithmState,
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_reconstruction::WeightingPlan,
    samples: &[SelectedObservationSample],
    evaluation_frequency_hz: f64,
) -> WeightingReplaySummary {
    let mut phase = generation
        .begin_replay(problem, plan)
        .expect("begin replay");
    for sample in samples {
        let contributions = SelectedSpectralContributions::new([
            SelectedSpectralContribution::new(
                sample.address.channel_index,
                1.0,
                evaluation_frequency_hz,
            ),
            None,
        ])
        .expect("one shifted-frame contribution");
        phase
            .consume(problem, *sample, contributions)
            .expect("weight shifted-frame sample");
    }
    phase.finish().expect("finish shifted-frame replay").1
}

fn replay_weights(
    generation: &WeightingAlgorithmState,
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_reconstruction::WeightingPlan,
    samples: &[SelectedObservationSample],
) -> Vec<f64> {
    let (blocks, _) = replay(generation, problem, plan, samples);
    blocks
        .iter()
        .flat_map(|block| block.samples())
        .flat_map(|sample| sample.spectral_values())
        .map(|value| value.imaging_weight())
        .collect()
}

fn fused_stream(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_reconstruction::WeightingPlan,
    samples: &[SelectedObservationSample],
) -> (
    WeightingAlgorithmState,
    Vec<WeightingReplayChunk>,
    WeightingReplaySummary,
) {
    let mut stream = if matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
        begin_natural_weighting_stream(problem, plan).expect("begin natural fused stream")
    } else {
        let mut density = begin_weighting_generation(problem, plan).expect("begin density pass");
        for sample in samples {
            density
                .consume(problem, *sample, exact_contributions(sample))
                .expect("density sample");
        }
        density
            .finish_into_stream(problem, plan)
            .expect("begin density-weighted fused stream")
    };
    let mut blocks = Vec::new();
    for sample in samples {
        if let Some(block) = stream
            .consume(problem, *sample, exact_contributions(sample))
            .expect("fused weighted sample")
        {
            blocks.push(block);
        }
    }
    let (final_block, state, summary) = stream.finish().expect("finish fused stream");
    if let Some(block) = final_block {
        blocks.push(block);
    }
    (state, blocks, summary)
}

#[test]
fn compiler_commitment_freezes_only_after_two_exhaustive_owner_passes() {
    let problem = problem(
        WeightingScheme::Briggs { robust: 0.5 },
        WeightDensityScope::GlobalSelection,
        None,
    );
    let samples = exact_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 3).expect("limits"),
    )
    .expect("plan");
    let generation =
        freeze_weighting_generation(&problem, &plan, &samples).expect("freeze global W");

    assert_eq!(
        generation.commitment_id(),
        problem.weighting().commitment_id()
    );
    assert_ne!(
        generation.generation_id().as_bytes(),
        generation.commitment_id().as_bytes()
    );
    assert_eq!(generation.sample_count(), samples.len() as u64);
    assert_eq!(generation.sum_weights().len(), 1);
    assert!(generation.sum_weights()[0] > 0.0);
}

#[test]
fn casa_global_natural_uniform_and_briggs_formulas_are_preserved() {
    let limits = WeightingExecutionLimits::new(4, 3).expect("limits");
    let natural = problem(
        WeightingScheme::Natural,
        WeightDensityScope::NotApplicable,
        None,
    );
    let natural_samples = exact_samples(&natural);
    let natural_plan = plan_weighting(&natural, limits).expect("natural plan");
    let natural_generation = freeze_weighting_generation(&natural, &natural_plan, &natural_samples)
        .expect("natural generation");
    assert_eq!(
        replay_weights(
            &natural_generation,
            &natural,
            &natural_plan,
            &natural_samples,
        ),
        [1.0, 2.0, 3.0, 4.0]
    );
    assert_eq!(natural_generation.sum_weights(), [10.0]);

    let uniform = problem(
        WeightingScheme::Uniform,
        WeightDensityScope::GlobalSelection,
        None,
    );
    let uniform_samples = exact_samples(&uniform);
    let uniform_plan = plan_weighting(&uniform, limits).expect("uniform plan");
    let uniform_generation = freeze_weighting_generation(&uniform, &uniform_plan, &uniform_samples)
        .expect("uniform generation");
    assert_eq!(
        replay_weights(
            &uniform_generation,
            &uniform,
            &uniform_plan,
            &uniform_samples,
        ),
        [1.0; 4]
    );
    assert_eq!(uniform_generation.sum_weights(), [4.0]);

    let briggs = problem(
        WeightingScheme::Briggs { robust: 0.5 },
        WeightDensityScope::GlobalSelection,
        None,
    );
    let briggs_samples = exact_samples(&briggs);
    let briggs_plan = plan_weighting(&briggs, limits).expect("Briggs plan");
    let briggs_generation = freeze_weighting_generation(&briggs, &briggs_plan, &briggs_samples)
        .expect("Briggs generation");
    let expected_f2 = (5.0_f64 * 10_f64.powf(-0.5)).powi(2) / 3.0;
    for (actual, input) in
        replay_weights(&briggs_generation, &briggs, &briggs_plan, &briggs_samples)
            .into_iter()
            .zip([1.0_f64, 2.0, 3.0, 4.0])
    {
        let expected = input / (input * expected_f2 + 1.0);
        assert!((actual - expected).abs() <= 1.0e-15);
    }
}

#[test]
fn fit_only_samples_cannot_change_density_dependent_output_weights() {
    let problem = problem(
        WeightingScheme::Uniform,
        WeightDensityScope::GlobalSelection,
        None,
    );
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 2).expect("limits"),
    )
    .expect("plan");
    let target = exact_samples(&problem)[0];
    let mut fit_only = target;
    fit_only.input_weight = 1.0e30;

    let output_weight = |include_fit_only: bool| {
        let mut density = begin_weighting_generation(&problem, &plan).expect("density");
        density
            .consume(&problem, target, exact_contributions(&target))
            .expect("target density");
        if include_fit_only {
            density
                .consume(&problem, fit_only, SelectedSpectralContributions::empty())
                .expect("fit-only density exclusion");
        }
        let mut sum_weight = density.finish(&problem).expect("sum-weight phase");
        sum_weight
            .consume(&problem, target, exact_contributions(&target))
            .expect("target sum weight");
        if include_fit_only {
            sum_weight
                .consume(&problem, fit_only, SelectedSpectralContributions::empty())
                .expect("fit-only sum-weight exclusion");
        }
        let generation = sum_weight.finish().expect("freeze weighting");
        let mut replay = generation.begin_replay(&problem, &plan).expect("replay");
        replay
            .consume(&problem, target, exact_contributions(&target))
            .expect("target replay")
            .expect("single-sample block")
            .samples()[0]
            .spectral_values()
            .next()
            .expect("target spectral weight")
            .imaging_weight()
    };

    assert_eq!(output_weight(false), output_weight(true));
}

#[test]
fn fused_terminal_stream_is_identical_to_separate_sum_weight_and_replay_passes() {
    for (scheme, scope) in [
        (WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        (
            WeightingScheme::Uniform,
            WeightDensityScope::GlobalSelection,
        ),
        (
            WeightingScheme::Briggs { robust: 0.5 },
            WeightDensityScope::GlobalSelection,
        ),
    ] {
        let problem = problem(scheme, scope, None);
        let samples = exact_samples(&problem);
        let plan = plan_weighting(
            &problem,
            WeightingExecutionLimits::new(3, 2).expect("limits"),
        )
        .expect("plan");
        let separate =
            freeze_weighting_generation(&problem, &plan, &samples).expect("separate generation");
        let (separate_blocks, separate_summary) = replay(&separate, &problem, &plan, &samples);
        let (fused, fused_blocks, fused_summary) = fused_stream(&problem, &plan, &samples);

        assert_eq!(fused.generation_id(), separate.generation_id());
        assert_eq!(fused.sum_weights(), separate.sum_weights());
        assert_eq!(fused_summary.coverage(), separate_summary.coverage());
        assert_eq!(
            fused_summary.sample_count(),
            separate_summary.sample_count()
        );
        assert_eq!(fused_summary.block_count(), separate_summary.block_count());
        let weights = |blocks: &[WeightingReplayChunk]| {
            blocks
                .iter()
                .flat_map(WeightingReplayChunk::samples)
                .flat_map(|sample| sample.spectral_values())
                .map(|value| value.imaging_weight())
                .collect::<Vec<_>>()
        };
        assert_eq!(weights(&fused_blocks), weights(&separate_blocks));
    }
}

#[test]
fn partition_block_worker_and_repeated_replay_choices_are_invariant() {
    let problem = problem(
        WeightingScheme::Uniform,
        WeightDensityScope::GlobalSelection,
        None,
    );
    let samples = exact_samples(&problem);
    let serial_plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("serial limits"),
    )
    .expect("serial plan");
    let partitioned_plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(3, 4).expect("partitioned limits"),
    )
    .expect("partitioned plan");
    let serial =
        freeze_weighting_generation(&problem, &serial_plan, &samples).expect("serial generation");
    let partitioned = freeze_weighting_generation(&problem, &partitioned_plan, &samples)
        .expect("partitioned generation");

    assert_eq!(serial.generation_id(), partitioned.generation_id());
    assert_eq!(serial.sum_weights(), partitioned.sum_weights());
    let (serial_blocks, serial_completion) = replay(&serial, &problem, &serial_plan, &samples);
    let (_, repeated_serial_completion) = replay(&serial, &problem, &serial_plan, &samples);
    let (partitioned_blocks, partitioned_completion) =
        replay(&partitioned, &problem, &partitioned_plan, &samples);
    assert_eq!(serial_blocks.len(), samples.len());
    assert_eq!(partitioned_blocks.len(), 2);
    assert_eq!(
        serial_completion.coverage(),
        partitioned_completion.coverage()
    );
    assert_eq!(
        serial_completion.weighting_generation(),
        partitioned_completion.weighting_generation()
    );
    assert_ne!(
        serial_completion.replay_id(),
        partitioned_completion.replay_id()
    );
    assert_eq!(serial_completion.replay_sequence(), 0);
    assert_eq!(repeated_serial_completion.replay_sequence(), 1);
    assert_ne!(
        serial_completion.replay_id(),
        repeated_serial_completion.replay_id()
    );
    let terminal = partitioned_blocks
        .into_iter()
        .last()
        .expect("partial terminal block")
        .into_samples();
    assert_eq!(terminal.len(), 1);
    assert_eq!(terminal.capacity(), 3);
}

#[test]
fn fused_and_replay_streams_reuse_returned_weighted_block_storage() {
    let problem = problem(
        WeightingScheme::Natural,
        WeightDensityScope::NotApplicable,
        None,
    );
    let samples = exact_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 1).expect("limits"),
    )
    .expect("plan");

    let mut fused = begin_natural_weighting_stream(&problem, &plan).expect("fused stream");
    let first = samples[..2]
        .iter()
        .find_map(|sample| {
            fused
                .consume(&problem, *sample, exact_contributions(sample))
                .expect("first fused block")
        })
        .expect("full first fused block");
    let fused_pointer = first.samples().as_ptr();
    fused
        .reuse_emitted_block(first)
        .expect("reuse first fused block");
    let second = samples[2..]
        .iter()
        .find_map(|sample| {
            fused
                .consume(&problem, *sample, exact_contributions(sample))
                .expect("second fused block")
        })
        .expect("full second fused block");
    assert_eq!(second.samples().as_ptr(), fused_pointer);
    fused
        .reuse_emitted_block(second)
        .expect("reuse second fused block");
    let (_, generation, _) = fused.finish().expect("finish fused stream");

    let mut replay = generation.begin_replay(&problem, &plan).expect("replay");
    let first = samples[..2]
        .iter()
        .find_map(|sample| {
            replay
                .consume(&problem, *sample, exact_contributions(sample))
                .expect("first replay block")
        })
        .expect("full first replay block");
    let replay_pointer = first.samples().as_ptr();
    replay
        .reuse_emitted_block(first)
        .expect("reuse first replay block");
    let second = samples[2..]
        .iter()
        .find_map(|sample| {
            replay
                .consume(&problem, *sample, exact_contributions(sample))
                .expect("second replay block")
        })
        .expect("full second replay block");
    assert_eq!(second.samples().as_ptr(), replay_pointer);
    replay
        .reuse_emitted_block(second)
        .expect("reuse second replay block");
    replay.finish().expect("finish replay");
}

#[test]
fn replay_coverage_binds_the_owner_evaluated_operator_frequency() {
    let problem = problem(
        WeightingScheme::Natural,
        WeightDensityScope::NotApplicable,
        None,
    );
    let samples = exact_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 2).expect("limits"),
    )
    .expect("plan");
    let generation =
        freeze_weighting_generation(&problem, &plan, &samples).expect("weighting generation");
    let native = replay_with_evaluation_frequency(
        &generation,
        &problem,
        &plan,
        &samples,
        samples[0].address.frequency_centre_hz,
    );
    let shifted = replay_with_evaluation_frequency(
        &generation,
        &problem,
        &plan,
        &samples,
        samples[0].address.frequency_centre_hz + 6.0e6,
    );

    assert_ne!(
        native.coverage(),
        shifted.coverage(),
        "T18 coverage must make a spectral-frame substitution observable"
    );
}

#[test]
fn multi_source_multi_spw_per_channel_generation_never_uses_chunk_local_density() {
    let problem = problem(
        WeightingScheme::Briggs { robust: 0.0 },
        WeightDensityScope::PerOutputChannel,
        None,
    );
    let samples = exact_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 2).expect("limits"),
    )
    .expect("plan");
    let generation =
        freeze_weighting_generation(&problem, &plan, &samples).expect("freeze per-channel W");

    assert_eq!(generation.sum_weights().len(), 2);
    assert!(generation.sum_weights().iter().all(|weight| *weight > 0.0));
    let (blocks, completion) = replay(&generation, &problem, &plan, &samples);
    assert_eq!(completion.sample_count(), 4);
    assert!(
        blocks
            .iter()
            .flat_map(|block| block.samples())
            .flat_map(|sample| sample.spectral_values())
            .all(|value| value.imaging_weight() >= 0.0)
    );
}

#[test]
fn linear_contribution_coefficients_drive_per_output_density_and_replay() {
    let problem = problem(
        WeightingScheme::Uniform,
        WeightDensityScope::PerOutputChannel,
        None,
    );
    let sample = exact_samples(&problem)[0];
    let contributions = SelectedSpectralContributions::new([
        SelectedSpectralContribution::new(0, 0.25, sample.address.frequency_centre_hz),
        SelectedSpectralContribution::new(1, 0.75, sample.address.frequency_centre_hz),
    ])
    .expect("two output contributions");
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(1, 1).expect("limits"),
    )
    .expect("plan");
    let mut density = begin_weighting_generation(&problem, &plan).expect("density");
    density
        .consume(&problem, sample, contributions.clone())
        .expect("accumulate split density");
    let mut sum_weight = density.finish(&problem).expect("sum-weight phase");
    sum_weight
        .consume(&problem, sample, contributions.clone())
        .expect("accumulate split sum weights");
    let generation = sum_weight.finish().expect("freeze split generation");
    let mut replay = generation.begin_replay(&problem, &plan).expect("replay");
    let block = replay
        .consume(&problem, sample, contributions)
        .expect("weight split sample")
        .expect("one-sample block");
    let weighted = block.samples()[0].spectral_values().collect::<Vec<_>>();

    assert_eq!(weighted.len(), 2);
    assert_eq!(weighted[0].contribution().factor(), 0.25);
    assert_eq!(weighted[1].contribution().factor(), 0.75);
    assert!(
        (weighted[0].imaging_weight() * 0.25 - weighted[1].imaging_weight() * 0.75).abs() < 1.0e-12,
        "uniform W must normalize coefficient-scaled density independently per output plane"
    );
    assert_eq!(generation.sum_weights(), &[1.0, 1.0]);
}

#[test]
fn per_channel_density_preserves_cube_rounding_instead_of_mfs_truncation() {
    let problem = problem(
        WeightingScheme::Uniform,
        WeightDensityScope::PerOutputChannel,
        None,
    );
    let mut samples = exact_samples(&problem);
    for (sample_index, u_lambda) in [(0, 4.68_f64), (2, 4.70_f64)] {
        let scale =
            f64::from((samples[sample_index].address.frequency_centre_hz / 299_792_458.0) as f32);
        samples[sample_index].coordinates.density_uvw_m = [u_lambda / scale, 0.0, 0.0];
    }
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(4, 2).expect("limits"),
    )
    .expect("plan");
    let generation = freeze_weighting_generation(&problem, &plan, &samples).expect("generation");
    let weights = replay_weights(&generation, &problem, &plan, &samples);

    assert_eq!(weights[0], 1.0);
    assert_eq!(weights[2], 1.0);
}

#[test]
fn briggs_bandwidth_taper_preserves_the_casa_uv_distance_factor() {
    let limits = WeightingExecutionLimits::new(4, 2).expect("limits");
    let standard = problem(
        WeightingScheme::Briggs { robust: 0.0 },
        WeightDensityScope::PerOutputChannel,
        None,
    );
    let bandwidth = problem(
        WeightingScheme::BriggsBandwidthTaper { robust: 0.0 },
        WeightDensityScope::PerOutputChannel,
        None,
    );
    let mut standard_samples = exact_samples(&standard);
    let mut bandwidth_samples = exact_samples(&bandwidth);
    for samples in [&mut standard_samples, &mut bandwidth_samples] {
        let scale = f64::from((samples[0].address.frequency_centre_hz / 299_792_458.0) as f32);
        samples[0].coordinates.density_uvw_m = [35.0 / scale, 0.0, 0.0];
    }
    let standard_plan = plan_weighting(&standard, limits).expect("standard plan");
    let bandwidth_plan = plan_weighting(&bandwidth, limits).expect("bandwidth plan");
    let standard_generation =
        freeze_weighting_generation(&standard, &standard_plan, &standard_samples)
            .expect("standard generation");
    let bandwidth_generation =
        freeze_weighting_generation(&bandwidth, &bandwidth_plan, &bandwidth_samples)
            .expect("bandwidth generation");
    let standard_weight = replay_weights(
        &standard_generation,
        &standard,
        &standard_plan,
        &standard_samples,
    )[0];
    let bandwidth_weight = replay_weights(
        &bandwidth_generation,
        &bandwidth,
        &bandwidth_plan,
        &bandwidth_samples,
    )[0];
    let fractional_bandwidth = 0.1e9_f64 / 1.1e9_f64;
    let n_cells = fractional_bandwidth * (35.0_f64 * 32.0 * -0.01).abs();
    let factor = n_cells + 0.5;
    let expected = 1.0 / (10.0 / factor + 1.0);

    assert!((standard_weight - 1.0 / 11.0).abs() <= 1.0e-15);
    assert!((bandwidth_weight - expected).abs() <= 1.0e-15);
    assert!(bandwidth_weight > standard_weight);
}

#[test]
fn taper_is_applied_after_natural_weighting() {
    let untapered = problem(
        WeightingScheme::Natural,
        WeightDensityScope::NotApplicable,
        None,
    );
    let tapered = problem(
        WeightingScheme::Natural,
        WeightDensityScope::NotApplicable,
        Some(UvTaper::new(10.0, 5.0, 0.2)),
    );
    let untapered_samples = exact_samples(&untapered);
    let tapered_samples = exact_samples(&tapered);
    let limits = WeightingExecutionLimits::new(4, 1).expect("limits");
    let untapered_plan = plan_weighting(&untapered, limits).expect("untapered plan");
    let tapered_plan = plan_weighting(&tapered, limits).expect("tapered plan");
    let untapered_generation =
        freeze_weighting_generation(&untapered, &untapered_plan, &untapered_samples)
            .expect("untapered generation");
    let tapered_generation = freeze_weighting_generation(&tapered, &tapered_plan, &tapered_samples)
        .expect("tapered generation");
    let (untapered_blocks, _) = replay(
        &untapered_generation,
        &untapered,
        &untapered_plan,
        &untapered_samples,
    );
    let (tapered_blocks, _) = replay(
        &tapered_generation,
        &tapered,
        &tapered_plan,
        &tapered_samples,
    );
    let untapered_weight = untapered_blocks[0].samples()[0]
        .spectral_values()
        .next()
        .expect("untapered output contribution")
        .imaging_weight();
    let tapered_weight = tapered_blocks[0].samples()[0]
        .spectral_values()
        .next()
        .expect("tapered output contribution")
        .imaging_weight();
    assert!(tapered_weight > 0.0 && tapered_weight < untapered_weight);
}

#[test]
fn weighted_metric_satisfies_psd_and_weighted_adjoint_laws() {
    let problem = problem(
        WeightingScheme::Briggs { robust: -0.5 },
        WeightDensityScope::GlobalSelection,
        None,
    );
    let samples = exact_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(4, 2).expect("limits"),
    )
    .expect("plan");
    let generation = freeze_weighting_generation(&problem, &plan, &samples).expect("generation");
    let (blocks, _completion) = replay(&generation, &problem, &plan, &samples);

    let x = [0.4, -0.7];
    let ax = [0.4 - 2.0 * -0.7, 3.0 * 0.4 + 0.5 * -0.7];
    for weighted in blocks
        .iter()
        .flat_map(|block| block.samples())
        .flat_map(|sample| sample.spectral_values())
    {
        let wax = weighted.apply_metric(ax);
        let lhs = ax[0] * wax[0] + ax[1] * wax[1];
        let adjoint_wax = [wax[0] + 3.0 * wax[1], -2.0 * wax[0] + 0.5 * wax[1]];
        let rhs = x[0] * adjoint_wax[0] + x[1] * adjoint_wax[1];
        assert!(lhs >= 0.0, "W must be positive semidefinite");
        assert!((lhs - rhs).abs() < 1.0e-12, "same W must enter A and A*");
    }
}

#[test]
fn planned_and_receipted_residency_cover_every_weighting_buffer_class() {
    let problem = problem(
        WeightingScheme::Uniform,
        WeightDensityScope::GlobalSelection,
        None,
    );
    let samples = exact_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 3).expect("limits"),
    )
    .expect("plan");
    let generation = freeze_weighting_generation(&problem, &plan, &samples).expect("generation");
    let (_, replay) = replay(&generation, &problem, &plan, &samples);
    let planned = plan.planned_residency();
    let generated = generation.generation_residency();
    let replayed = replay.residency();

    assert!(generated.density_grid_bytes() <= planned.density_grid_bytes());
    assert!(generated.deterministic_partial_bytes() <= planned.deterministic_partial_bytes());
    assert!(generated.reduction_scratch_bytes() <= planned.reduction_scratch_bytes());
    assert!(generated.robust_factor_bytes() <= planned.robust_factor_bytes());
    assert!(generated.sum_weight_bytes() <= planned.sum_weight_bytes());
    assert_eq!(replayed.replay_read_bytes(), planned.replay_read_bytes());
    assert_eq!(
        replayed.weighted_block_bytes(),
        planned.weighted_block_bytes()
    );
    assert_eq!(
        replayed.simultaneous_selected_weighted_bytes(),
        planned.simultaneous_selected_weighted_bytes(),
        "the real replay-input block and weighted-output block coexist at the declared peak"
    );
    assert!(generated.peak_bytes() <= planned.peak_bytes());
    assert!(replayed.peak_bytes() <= planned.peak_bytes());
}

#[test]
fn incomplete_callback_phase_cannot_finalize_weighting_state() {
    let problem = problem(
        WeightingScheme::Uniform,
        WeightDensityScope::GlobalSelection,
        None,
    );
    let samples = exact_samples(&problem);
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(2, 2).expect("limits"),
    )
    .expect("plan");
    let mut density = begin_weighting_generation(&problem, &plan).expect("density phase");
    for sample in &samples {
        density
            .consume(&problem, *sample, exact_contributions(sample))
            .expect("density sample");
    }
    let mut sum_weight = density.finish(&problem).expect("sum-weight phase");
    for sample in &samples[..samples.len() - 1] {
        sum_weight
            .consume(&problem, *sample, exact_contributions(sample))
            .expect("sum-weight sample");
    }
    assert!(matches!(
        sum_weight.finish(),
        Err(casa_imaging_reconstruction::WeightingError::SelectedGenerationMismatch)
    ));
}
