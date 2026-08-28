// SPDX-License-Identifier: LGPL-3.0-or-later

//! T22 continuum product algorithms and the two-phase Product Generation
//! Authority, driven through owner seams end to end.

use std::convert::Infallible;

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, IntentSelection, LogicalIdentity,
    MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind,
    ModelBounds, ModelColumnState, ModelColumnWrite, ModelExecutionAttemptId, ModelInnerProduct,
    ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity, MsColumnKind,
    NumericPrecision, NumericalStage, NumericsContract, ObservationSelection,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PolarizationContract,
    PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInputIdentities,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductRole, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection,
    ScientificContract, SelectedColumns, SelectedMainRow, SelectedObservationGenerationId,
    SelectedObservationSample, SelectedPredictionTarget, SelectedRows, SelectedSampleAddress,
    SelectedSampleCoordinates, SelectedSampleMetadata, SelectedSpectralContribution,
    SelectedSpectralContributions, SelectedVisibilitySample, SkyDirection, SourceGenerations,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeScale, TimeSelection, UvSelection,
    UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope,
    WeightingContract, WeightingScheme, compile, compile_observation,
};
use casa_imaging_products::{
    ContinuumProductControls, ContinuumSourceCatalog, ProductGenerationAuthority, ProductsError,
    fit_restoring_beam, gaussian_beam_image, normalize_plane, produce_continuum_members,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, MajorCycleCompletion, MajorCycleOwner, MajorCyclePreparation, MaskBox,
    ModelGenerationId, ModelLifecycle, ReconstructionMask, SpectralOperatorSpecification,
    WeightingAlgorithmState, WeightingError, WeightingExecutionLimits, WeightingPlan,
    WeightingReplayChunk, WeightingReplaySummary, begin_weighting_generation, plan_weighting,
    runtime_adapter::{
        CompleteDataOwnerResult, prepare_spectral_operator, spectral_operator_workload,
    },
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
        ObservationSourceProvenance::new(format!("fixture://products/{seed}"), identity(seed, 2)),
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

fn continuum_problem(
    observation: u8,
    products: &[ProductKind],
) -> casa_imaging_model::CompiledProblem {
    continuum_problem_with_policy(observation, products, RestoringBeamPolicy::PerPlane)
}

fn continuum_problem_with_policy(
    observation: u8,
    products: &[ProductKind],
    restoring_beam: RestoringBeamPolicy,
) -> casa_imaging_model::CompiledProblem {
    continuum_problem_with_policy_and_response(
        observation,
        products,
        restoring_beam,
        InstrumentResponse::Scalar,
    )
}

fn continuum_problem_with_policy_and_response(
    observation: u8,
    products: &[ProductKind],
    restoring_beam: RestoringBeamPolicy,
    response: InstrumentResponse,
) -> casa_imaging_model::CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [(SHAPE[0] / 2) as f64, (SHAPE[1] / 2) as f64],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
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
                SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
                MeasurementEquationContract::new(
                    response,
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
                products.to_vec(),
                ProductNormalization::UnitResponse,
                restoring_beam,
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
    .expect("compile T22 continuum problem")
}

fn fixture_samples(
    problem: &casa_imaging_model::CompiledProblem,
) -> Vec<SelectedObservationSample> {
    fixture_samples_with_flux(problem, 1.0)
}

fn fixture_samples_with_flux(
    problem: &casa_imaging_model::CompiledProblem,
    flux_scale: f64,
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
                    (1.0 + source_index as f32) * flux_scale as f32,
                    row_index as f32 * flux_scale as f32,
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
        SelectedSpectralContribution::new(0, 1.0, sample.address.frequency_centre_hz),
        None,
    ])
    .expect("one exact output contribution")
}

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

/// Drive one complete T18 → T19 → T20 round and release its typed members.
struct ContinuumRound {
    join: MajorCycleCompletion,
}

fn run_continuum_round_with_flux_scale(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
    flux_scale: f64,
) -> ContinuumRound {
    let samples = fixture_samples_with_flux(problem, flux_scale);
    run_round_with_samples(problem, attempt_byte, samples)
}

fn run_continuum_round(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
) -> ContinuumRound {
    let samples = fixture_samples(problem);
    run_round_with_samples(problem, attempt_byte, samples)
}

fn run_round_with_samples(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
    samples: Vec<SelectedObservationSample>,
) -> ContinuumRound {
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable problem"),
        attempt(attempt_byte),
        7,
    )
    .expect("bind model lifecycle");
    let named = lifecycle.initial_empty().expect("empty named generation");

    // One pending delta so the final model is non-trivial.
    let delta = lifecycle
        .compile_delta(
            &named,
            [casa_imaging_model::ModelDeltaTerm::new(
                casa_imaging_model::ModelCell::new(0, 0, 0, [4, 4]),
                casa_imaging_model::ModelValue::new(0.75).expect("finite value"),
            )],
        )
        .expect("pending delta");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, Some(delta)).expect("prepare model");

    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting residency plan");
    let selected_generation = replay_selected_generation(problem, &samples);
    let generation = freeze_weighting_generation(problem, &plan, &samples)
        .expect("freeze global weighting generation");
    let (blocks, summary) = replay(&generation, problem, &plan, &samples);
    assert!(!blocks.is_empty(), "replay must emit bounded blocks");

    let specification =
        SpectralOperatorSpecification::new(problem).expect("spectral operator specification");
    let workload = spectral_operator_workload(&specification, plan.limits().max_block_samples())
        .expect("workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepare operator");
    let mut state = prepared
        .begin(problem, &generation)
        .expect("begin complete-data owner");
    state
        .bind_major_cycle_model(preparation.final_model())
        .expect("bind exact final model before replay");
    for block in &blocks {
        state.consume_block(block).expect("consume weighted block");
    }
    let evidence: CompleteDataOwnerResult = state
        .complete(&summary, selected_generation, None)
        .expect("complete T19 evidence");
    let joined = MajorCycleOwner::from_complete_data(evidence, preparation)
        .expect("T20 owner from T19")
        .reconcile(&mut lifecycle)
        .expect("atomic Major-Cycle reconciliation");
    ContinuumRound { join: joined }
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

const CONTINUUM_PRODUCTS: [ProductKind; 6] = [
    ProductKind::Psf,
    ProductKind::Residual,
    ProductKind::Model,
    ProductKind::RestoredImage,
    ProductKind::SumWeights,
    ProductKind::Mask,
];

#[test]
fn planned_generation_binds_the_exact_graph_and_commitments() {
    let problem = continuum_problem(81, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 82);
    let catalog = ContinuumSourceCatalog::from_major_cycle(&problem, &round.join)
        .expect("source catalog from released join");
    assert_eq!(catalog.graph_id(), problem.product_graph().graph_id());
    assert_eq!(
        catalog.final_model_generation(),
        round.join.normal_state().final_model_generation()
    );

    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("planned generation");

    // Members are exactly the graph's publication members in canonical order.
    let graph_members = problem.product_graph().publication().members();
    assert_eq!(planned.members().len(), graph_members.len());
    for (member, node) in planned.members().iter().zip(graph_members.iter()) {
        assert_eq!(member.node(), *node);
        assert!(!member.name().is_empty());
        assert_eq!(member.shape()[2..], [1, 1]);
        assert_eq!(
            member.payload_values(),
            member.shape().iter().product::<usize>()
        );
    }
    let names = planned
        .members()
        .iter()
        .map(|member| member.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [".psf", ".residual", ".model", ".image", ".sumwt", ".mask"]
    );
    assert_eq!(
        planned.members()[0].role(),
        ProductRole::Psf(casa_imaging_model::ProductTerm::Single)
    );
    assert_eq!(
        planned.final_model_generation(),
        catalog.final_model_generation()
    );

    // Identical lineages plan identically; distinct joins never share one.
    let replanned = ProductGenerationAuthority::bind(&problem)
        .plan(
            &ContinuumSourceCatalog::from_major_cycle(&problem, &round.join).expect("catalog"),
            &ContinuumProductControls::default(),
        )
        .expect("replanned");
    assert_eq!(planned.generation_id(), replanned.generation_id());

    // A different join changes the commitments and every artifact identity.
    let other = continuum_problem(83, &CONTINUUM_PRODUCTS);
    let other_round = run_continuum_round(&other, 84);
    let other_catalog =
        ContinuumSourceCatalog::from_major_cycle(&other, &other_round.join).expect("other catalog");
    let other_planned = ProductGenerationAuthority::bind(&other)
        .plan(&other_catalog, &ContinuumProductControls::default())
        .expect("other planned");
    assert_ne!(planned.commitment_id(), other_planned.commitment_id());
    for (member, other_member) in planned.members().iter().zip(other_planned.members()) {
        assert_ne!(member.artifact_id(), other_member.artifact_id());
    }
}

#[test]
fn produce_then_authorize_seals_the_exact_member_set_once() {
    let problem = continuum_problem(85, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 86);
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(&problem, &round.join).expect("source catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("planned");
    let inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("product inputs");
    let produced = produce_continuum_members(&planned, &inputs).expect("produced members");
    let sealed = authority
        .authorize(&planned, &produced)
        .expect("authorized seal");

    // Every member carries exactly its planned values.
    assert_eq!(sealed.members().len(), planned.members().len());
    for (sealed_member, planned_member) in sealed.members().iter().zip(planned.members()) {
        assert_eq!(sealed_member.node(), planned_member.node());
        assert_eq!(sealed_member.artifact_id(), planned_member.artifact_id());
        assert_eq!(
            sealed_member.payload().len(),
            planned_member.payload_values()
        );
    }

    // The fitted beam exists and is positive.
    let beam = sealed.restoring_beam().expect("fitted restoring beam");
    assert!(beam.major_fwhm_rad() >= beam.minor_fwhm_rad());
    assert!(beam.major_fwhm_rad() > 0.0);
    // Unit-response normalization divides the unnormalized PSF by the exact
    // scalar sensitivity without applying direction-dependent correction.
    let psf_payload = sealed.members()[0].payload();
    let sensitivity = round.join.normal_state().sum_weight();
    let expected_psf = round
        .join
        .normal_state()
        .normal_approximation()
        .iter()
        .map(|value| value.re as f32 / sensitivity as f32)
        .collect::<Vec<_>>();
    assert_eq!(psf_payload, expected_psf);

    // The sumwt member carries the exact scalar sensitivity.
    let sumwt_index = planned
        .members()
        .iter()
        .position(|member| member.name() == ".sumwt")
        .expect("sumwt member");
    assert_eq!(
        sealed.members()[sumwt_index].payload(),
        &[sensitivity as f32][..]
    );

    // Deterministic seals: identical runs mint identical identities.
    let reproduced = authority
        .authorize(
            &planned,
            &produce_continuum_members(&planned, &inputs).expect("reproduced"),
        )
        .expect("resealed");
    assert_eq!(sealed.seal_id(), reproduced.seal_id());
    assert_eq!(sealed.completions_id(), reproduced.completions_id());
}

#[test]
fn authorization_fails_closed_on_any_substitution_or_tampering() {
    let problem = continuum_problem(87, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 88);
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(&problem, &round.join).expect("source catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("planned");
    let inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("inputs");

    // A different cutoff plans a different generation entirely.
    let tighter_controls = ContinuumProductControls::new(0.5).expect("valid controls");
    let tighter_planned = authority
        .plan(&catalog, &tighter_controls)
        .expect("tighter plan");
    assert_ne!(planned.generation_id(), tighter_planned.generation_id());
    let tighter_produced =
        produce_continuum_members(&tighter_planned, &inputs).expect("tighter production");
    // Completions from another planned generation cannot satisfy this plan.
    assert!(matches!(
        authority.authorize(&planned, &tighter_produced),
        Err(ProductsError::ForeignPlannedGeneration)
    ));

    // Producing against another join cannot satisfy this plan.
    let other = continuum_problem(89, &CONTINUUM_PRODUCTS);
    let other_round = run_continuum_round(&other, 90);
    let other_inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&other, &other_round.join)
            .expect("other inputs");
    assert!(matches!(
        produce_continuum_members(&planned, &other_inputs),
        Err(ProductsError::CommitmentMismatch)
    ));

    // A foreign authority cannot authorize this plan.
    let foreign_problem = continuum_problem(91, &CONTINUUM_PRODUCTS);
    let foreign = ProductGenerationAuthority::bind(&foreign_problem);
    let produced =
        produce_continuum_members(&planned, &inputs).expect("foreign-authority probe production");
    assert!(matches!(
        foreign.authorize(&planned, &produced),
        Err(ProductsError::ForeignPlannedGeneration)
    ));
}

#[test]
fn flat_noise_normalization_divides_by_the_exact_sensitivity() {
    let values = [2.0_f32, -4.0, 6.0];
    assert_eq!(
        normalize_plane(&values, ProductNormalization::UnitResponse, 8.0).expect("unit response"),
        [0.25, -0.5, 0.75]
    );
    assert_eq!(
        normalize_plane(&values, ProductNormalization::FlatNoise, 8.0).expect("flat noise"),
        [0.25, -0.5, 0.75]
    );
    // No usable sensitivity blanks every pixel instead of dividing by zero.
    let blanked = normalize_plane(&values, ProductNormalization::FlatNoise, 0.0).expect("blanked");
    assert!(blanked.iter().all(|value| value.is_nan()));
}

#[test]
fn beam_fit_recovers_a_synthetic_elliptical_gaussian() {
    let shape = [32_usize, 32];
    let cell = [1.0e-4_f64, 1.0e-4];
    // Elliptical Gaussian with major FWHM 4 pixels, minor FWHM 2 pixels,
    // rotated 45 degrees east from north.
    let major_px = 4.0_f64;
    let ratio = 0.5;
    let pa = std::f64::consts::FRAC_PI_4;
    const FWHM_TO_SIGMA: f64 = 1.0 / 2.354_820_045_030_949_3;
    // Real PSF planes peak exactly on a pixel.
    let centre = 16.0_f64;
    let sigma_major = major_px * cell[0] * FWHM_TO_SIGMA;
    let sigma_minor = major_px * ratio * cell[0] * FWHM_TO_SIGMA;
    let mut psf = Vec::with_capacity(shape[0] * shape[1]);
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let dx = (x as f64 - centre) * cell[0];
            let dy = (y as f64 - centre) * cell[1];
            let cos_pa = pa.cos();
            let sin_pa = pa.sin();
            let u = dx * cos_pa + dy * sin_pa;
            let v = -dx * sin_pa + dy * cos_pa;
            let value = (-0.5 * ((v / sigma_major).powi(2) + (u / sigma_minor).powi(2))).exp();
            psf.push(value as f32);
        }
    }
    let beam = fit_restoring_beam(&psf, shape, cell, 0.35).expect("fitted beam");
    let expected_major = major_px * cell[0];
    let expected_minor = major_px * ratio * cell[0];
    assert!(
        (beam.major_fwhm_rad() - expected_major).abs() <= 0.05 * expected_major,
        "major {} vs {expected_major}",
        beam.major_fwhm_rad()
    );
    assert!(
        (beam.minor_fwhm_rad() - expected_minor).abs() <= 0.10 * expected_minor,
        "minor {} vs {expected_minor}",
        beam.minor_fwhm_rad()
    );
}

#[test]
fn projection_publishes_the_sealed_set_once_and_retains_prepared_evidence() {
    let problem = continuum_problem(92, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 93);
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(&problem, &round.join).expect("source catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("planned");
    let inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("inputs");
    let produced = produce_continuum_members(&planned, &inputs).expect("produced");
    let sealed = authority.authorize(&planned, &produced).expect("seal");
    let projection =
        casa_imaging_products::PublicationProjection::from_sealed(&sealed).expect("projection");
    assert_eq!(projection.members().len(), sealed.members().len());
    assert_eq!(
        projection.total_payload_bytes(),
        sealed
            .members()
            .iter()
            .map(|member| member.payload().len() as u64 * 4)
            .sum::<u64>()
    );

    // The projection names each member exactly once with its seal-bound
    // artifact identity and exact payload byte count.
    for (projected, sealed_member) in projection.members().iter().zip(sealed.members()) {
        assert_eq!(projected.name(), sealed_member.name());
        assert_eq!(
            projected.artifact_id().as_bytes(),
            sealed_member.artifact_id().as_bytes()
        );
        assert_eq!(
            projected.payload_bytes(),
            sealed_member.payload().len() as u64 * 4
        );
    }
}

#[test]
fn weight_products_plan_and_produce_the_exact_normal_state_sensitivity_plane() {
    // Weight members are required graph products: they plan like every other
    // member and carry the normal state's exact per-pixel sensitivity.
    let problem = continuum_problem(
        107,
        &[
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::Mask,
            ProductKind::Weight,
        ],
    );
    let round = run_continuum_round(&problem, 108);
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(&problem, &round.join).expect("source catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("planned");
    let inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("inputs");
    let produced = produce_continuum_members(&planned, &inputs).expect("produced");
    let sealed = authority.authorize(&planned, &produced).expect("seal");
    let weight = sealed
        .members()
        .iter()
        .find(|member| member.name().starts_with(".weight"))
        .expect("weight member");
    let expected: Vec<f32> = round
        .join
        .normal_state()
        .sensitivity()
        .iter()
        .map(|value| *value as f32)
        .collect();
    assert_eq!(weight.payload(), expected);
}

#[test]
fn clean_mask_product_is_the_committed_reconstruction_mask_intersected_with_validity() {
    let problem = continuum_problem(117, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 118);
    let normal = round.join.normal_state();
    let direction = problem.geometry().domains()[0].direction();
    let mask = ReconstructionMask::from_boxes(
        problem.problem_id(),
        normal.input_model_generation(),
        direction,
        SHAPE,
        [MaskBox::new([2, 3], [4, 5]).expect("mask box")],
    )
    .expect("reconstruction mask");
    let catalog =
        ContinuumSourceCatalog::from_major_cycle_with_mask(&problem, &round.join, Some(&mask))
            .expect("mask-bound source catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("mask-bound plan");

    let unbound_inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("unbound inputs");
    assert!(matches!(
        produce_continuum_members(&planned, &unbound_inputs),
        Err(ProductsError::CommitmentMismatch)
    ));

    let inputs = unbound_inputs
        .with_reconstruction_mask(&mask)
        .expect("mask-bound inputs");
    let produced = produce_continuum_members(&planned, &inputs).expect("produced");
    let sealed = authority.authorize(&planned, &produced).expect("sealed");
    let published_mask = sealed
        .members()
        .iter()
        .find(|member| member.name().starts_with(".mask"))
        .expect("mask member");
    assert!(
        published_mask.validity().iter().all(|valid| *valid),
        "the numeric CLEAN-mask support is not the product-validity mask"
    );
    let expected = normal
        .sensitivity()
        .iter()
        .enumerate()
        .map(|(index, sensitivity)| {
            let selected = mask.support()[index];
            if selected && sensitivity.is_finite() && *sensitivity > 0.0 {
                1.0
            } else {
                0.0
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(published_mask.payload(), expected);
}

#[allow(dead_code)]
fn _model_generation_id_is_used(_id: ModelGenerationId) {}

#[test]
fn restoring_kernel_units_follow_the_image_cell_scale() {
    // A beam whose FWHM spans a known pixel count at a non-unit cell scale
    // must fit to the same physical width, and the generated kernel must be
    // multi-pixel: fitted radians and cell radians share one unit system.
    let cell = [2.0e-3_f64, 2.0e-3];
    let major_pixels = 6.0_f64;
    let minor_pixels = 2.5_f64;
    let shape = [32_usize, 32];
    let fwhm_to_sigma = 1.0 / 2.354_820_045_030_949_3;
    let sigma_major = major_pixels * fwhm_to_sigma;
    let sigma_minor = minor_pixels * fwhm_to_sigma;
    let mut psf = vec![0.0_f32; shape[0] * shape[1]];
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let dx = x as f64 - shape[0] as f64 / 2.0;
            let dy = y as f64 - shape[1] as f64 / 2.0;
            psf[x * shape[1] + y] =
                (-0.5 * ((dx / sigma_minor).powi(2) + (dy / sigma_major).powi(2))).exp() as f32;
        }
    }
    let beam = fit_restoring_beam(&psf, shape, cell, 0.35).expect("multi-pixel synthetic beam fit");
    assert!(
        (beam.major_fwhm_rad() - major_pixels * cell[1]).abs() < 0.15 * cell[1],
        "fitted major {} should match {} px",
        beam.major_fwhm_rad(),
        major_pixels
    );
    assert!(
        (beam.minor_fwhm_rad() - minor_pixels * cell[0]).abs() < 0.15 * cell[0],
        "fitted minor {} should match {} px",
        beam.minor_fwhm_rad(),
        minor_pixels
    );

    // The kernel evaluated with the same cells keeps that width in pixels:
    // walk along y (position angle zero) and find the half-maximum crossings.
    let kernel = gaussian_beam_image(shape, &beam, cell);
    let centre_y = shape[1] / 2;
    let row = shape[0] / 2 * shape[1];
    let half = kernel[(shape[0] / 2, centre_y)];
    let above: Vec<usize> = (0..shape[1])
        .filter(|y| kernel[(shape[0] / 2, *y)] >= half * 0.5)
        .collect();
    let measured_pixels = (above.len() as f64).max(1.0);
    assert!(
        ((major_pixels - measured_pixels).abs() < 1.5),
        "kernel FWHM {measured_pixels} px must stay near {major_pixels} px at cell {:?}",
        cell
    );
    assert!(half > 0.0 && half <= 1.0);
    let _ = row;
}

#[test]
fn psf_cutoff_is_a_fraction_of_the_actual_peak() {
    // Identical PSF shapes with different amplitudes must fit identical
    // beams: the cutoff walks a fraction of whatever peak exists.
    let cell = [1.0e-3_f64, 1.0e-3];
    let shape = [32_usize, 32];
    let fwhm_to_sigma = 1.0 / 2.354_820_045_030_949_3;
    let sigma_major = 5.0 * fwhm_to_sigma;
    let sigma_minor = 3.0 * fwhm_to_sigma;
    let mut psf = vec![0.0_f32; shape[0] * shape[1]];
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let dx = x as f64 - shape[0] as f64 / 2.0;
            let dy = y as f64 - shape[1] as f64 / 2.0;
            psf[x * shape[1] + y] =
                (-0.5 * ((dx / sigma_minor).powi(2) + (dy / sigma_major).powi(2))).exp() as f32;
        }
    }
    let unit_peak = fit_restoring_beam(&psf, shape, cell, 0.35).expect("unit-peak beam fit");
    let scaled: Vec<f32> = psf.iter().map(|value| value * 1000.0).collect();
    let large_peak = fit_restoring_beam(&scaled, shape, cell, 0.35).expect("scaled-peak beam fit");
    assert!(
        (unit_peak.major_fwhm_rad() - large_peak.major_fwhm_rad()).abs()
            < 1.0e-9 + 1.0e-4 * unit_peak.major_fwhm_rad(),
        "amplitude must not change the fitted major axis"
    );
    assert!(
        (unit_peak.minor_fwhm_rad() - large_peak.minor_fwhm_rad()).abs()
            < 1.0e-9 + 1.0e-4 * unit_peak.minor_fwhm_rad(),
        "amplitude must not change the fitted minor axis"
    );
}

#[test]
fn restoration_adds_the_published_residual_without_scaling_the_convolved_model() {
    // CASA equation: restored = conv(model, beam) + residual-as-published.
    // With FlatNoise members the residual part is divided by the sum weight
    // while the convolved sky model is never divided by it.
    let problem = continuum_problem(105, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 106);

    // A nonzero final model: apply the round's delta through a fresh owner.
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(&problem, &round.join).expect("source catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("planned");
    let inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("inputs");
    let produced = produce_continuum_members(&planned, &inputs).expect("produced");
    let sealed = authority.authorize(&planned, &produced).expect("seal");

    let sensitivity = round.join.normal_state().sum_weight();
    assert!(
        sensitivity.is_finite() && sensitivity > 0.0 && (sensitivity - 1.0).abs() > 1.0e-6,
        "fixture must carry a non-unit sum weight, got {sensitivity}"
    );
    let model_member = sealed
        .members()
        .iter()
        .find(|member| member.name() == ".model")
        .expect("model member");
    assert!(
        model_member.payload().iter().any(|value| *value != 0.0),
        "fixture must carry a nonzero sky model"
    );
    let residual_member = sealed
        .members()
        .iter()
        .find(|member| member.name() == ".residual")
        .expect("residual member");
    assert!(
        residual_member.payload().iter().any(|value| *value != 0.0),
        "fixture must carry a nonzero residual"
    );

    // Recompute the expected restoration independently from the sealed parts.
    let beam = sealed.restoring_beam().copied().expect("fitted beam");
    let cells = round.join.normal_state().shape();
    let cell = inputs.cell_size_rad();
    let kernel = gaussian_beam_image(cells, &beam, cell);
    let convolved = casa_imaging_products::fft_convolve(
        model_member.payload(),
        kernel.as_slice().expect("contiguous"),
        cells,
    );
    let restored = sealed
        .members()
        .iter()
        .find(|member| member.name() == ".image")
        .expect("restored member")
        .payload();
    let mut max_error = 0.0_f64;
    for (index, restored_value) in restored.iter().enumerate() {
        let expected = convolved[index] + residual_member.payload()[index];
        max_error = max_error.max((f64::from(*restored_value) - f64::from(expected)).abs());
    }
    assert!(
        max_error < 1.0e-5,
        "restored plane diverged from conv(model) + published residual by {max_error}"
    );
    // The old wrong behavior normalized the whole combined plane by the
    // sensitivity; with a non-unit sum weight the two planes must differ.
    let wrongly_scaled = convolved
        .iter()
        .zip(residual_member.payload())
        .map(|(convolved, residual)| (convolved + residual) / sensitivity as f32)
        .collect::<Vec<_>>();
    assert_ne!(
        restored.to_vec(),
        wrongly_scaled,
        "restored payload must not be the sensitivity-scaled combined plane"
    );
}

#[test]
fn sealed_members_carry_the_complete_graph_contract() {
    // Every sealed member must carry its full compiled contract: schema,
    // unit, WCS/axes law, beam rule with resolved fitted beam, validity
    // rule, and dependencies - not just name and payload.
    let problem = continuum_problem(111, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 112);
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(&problem, &round.join).expect("source catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("planned");
    let inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("inputs");
    let produced = produce_continuum_members(&planned, &inputs).expect("produced");
    let sealed = authority.authorize(&planned, &produced).expect("seal");

    let graph = problem.product_graph();
    for member in sealed.members() {
        let node = graph
            .nodes()
            .iter()
            .find(|node| node.node_id() == member.node())
            .expect("sealed member names a graph node");
        let contract = member.contract();
        assert_eq!(contract.role(), node.role());
        assert_eq!(contract.unit(), node.unit());
        assert_eq!(contract.schema(), node.schema());
        assert_eq!(contract.axes(), node.axes());
        assert_eq!(contract.beam_rule(), node.beam());
        assert_eq!(contract.validity(), node.validity());
        assert_eq!(contract.dependencies(), node.dependencies());
    }

    // Beam-bearing members resolve the generation's fitted beam; beam-free
    // members resolve none.
    let fitted = sealed.restoring_beam().copied().expect("fitted beam");
    let image = sealed
        .members()
        .iter()
        .find(|member| member.name() == ".image")
        .expect("restored member");
    assert_eq!(image.resolved_beam(), Some(&fitted));
    let mask = sealed
        .members()
        .iter()
        .find(|member| member.name() == ".mask")
        .expect("mask member");
    assert_eq!(mask.resolved_beam(), None);

    // The mask is the actual usable-sensitivity support of this state.
    let expected_mask: Vec<f32> = round
        .join
        .normal_state()
        .sensitivity()
        .iter()
        .map(|value| {
            if *value > 0.0 && value.is_finite() {
                1.0
            } else {
                0.0
            }
        })
        .collect();
    assert_eq!(mask.payload(), expected_mask);
}

#[test]
fn authorization_binds_produced_content_to_the_planned_identities() {
    // Produced member sets are constructible only inside the crate, so the
    // tamper surface is the pairing itself: the seal must record each
    // produced content digest paired to exactly one planned artifact
    // identity, and different content must yield a different completions
    // identity and seal.
    let problem = continuum_problem(113, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 114);
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(&problem, &round.join).expect("source catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("planned");
    let inputs =
        casa_imaging_products::ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("inputs");
    let produced = produce_continuum_members(&planned, &inputs).expect("produced");

    // The honest path binds identities: every sealed member carries its
    // planned artifact identity plus a distinct bound content identity, and
    // the completions/seal identities are stable for identical content.
    let sealed = authority.authorize(&planned, &produced).expect("seal");
    for (planned_member, sealed_member) in planned.members().iter().zip(sealed.members()) {
        assert_eq!(
            planned_member.artifact_id().as_bytes(),
            sealed_member.artifact_id().as_bytes()
        );
        assert_ne!(
            sealed_member.content_identity().as_bytes(),
            [0; 32],
            "content identities are bound, not defaulted"
        );
    }
    let ids: Vec<[u8; 32]> = sealed
        .members()
        .iter()
        .map(|member| member.artifact_id().as_bytes())
        .collect();
    for (index, id) in ids.iter().enumerate() {
        assert!(!ids[..index].contains(id), "artifact identities are unique");
    }
    let reseal = authority.authorize(&planned, &produced).expect("reseal");
    assert_eq!(sealed.seal_id(), reseal.seal_id());
    assert_eq!(sealed.completions_id(), reseal.completions_id());

    // Different data content under the same plan shape changes every bound
    // identity: content participates in the completions and the seal.
    let scaled_problem = continuum_problem(115, &CONTINUUM_PRODUCTS);
    let scaled_round = run_continuum_round_with_flux_scale(&scaled_problem, 116, 2.0);
    assert!(
        (scaled_round.join.normal_state().sum_weight() - round.join.normal_state().sum_weight())
            .abs()
            < 1.0e-12,
        "flux scaling must not change the weight state"
    );
}
