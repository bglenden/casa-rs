// SPDX-License-Identifier: LGPL-3.0-or-later

//! T22 continuum product algorithms driven through the direct generation and
//! write-only output seams.

use std::{convert::Infallible, mem::size_of};

mod common;
use common::{GeneratedProducts, MemoryProductOutput, full_window};

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
    ScientificContract, SelectedColumns, SelectedImageDomainProjection,
    SelectedImageDomainProjections, SelectedMainRow, SelectedObservationGenerationId,
    SelectedObservationRunChannel, SelectedObservationRunCorrelation, SelectedObservationRunRow,
    SelectedObservationSample, SelectedPhaseCentreProjection, SelectedPredictionTarget,
    SelectedRows, SelectedSampleAddress, SelectedSampleCoordinates, SelectedSampleMetadata,
    SelectedSpectralContribution, SelectedSpectralContributions, SelectedVisibilitySample,
    SkyDirection, SourceGenerations, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection,
    StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy, TimeScale, TimeSelection,
    UvSelection, UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct, WeightColumn,
    WeightDensityScope, WeightingContract, WeightingScheme, compile, compile_observation,
};
use casa_imaging_products::{
    AnalyticPrimaryBeamModel, ContinuumProductControls, ContinuumProductInputs, MosaicSensitivity,
    PlannedContinuumGeneration, ProductStoragePlan, ProductsError, fit_restoring_beam,
    gaussian_beam_image, normalize_plane, produce_continuum_members,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, ImageDomainReconstructionMaskPlans, ImageDomainReconstructionMasks,
    MajorCycleCompletion, MajorCycleOwner, MajorCyclePreparation, MaskBox, ModelGenerationId,
    ModelLifecycle, ReconstructionMask, ReconstructionMaskPlan, ReconstructionMaskSet,
    SpectralOperatorSpecification, WeightingAlgorithmState, WeightingError,
    WeightingExecutionLimits, WeightingPlan, WeightingReplayChunk, WeightingReplaySummary,
    begin_weighting_generation, plan_weighting,
    runtime_adapter::{
        CompleteDataOwnerResult, SpectralOperatorPass, prepare_spectral_operator,
        spectral_operator_workload,
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
    continuum_problem_with_reconstruction(
        observation,
        products,
        restoring_beam,
        response,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
    )
}

fn continuum_problem_with_reconstruction(
    observation: u8,
    products: &[ProductKind],
    restoring_beam: RestoringBeamPolicy,
    response: InstrumentResponse,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    channels: usize,
) -> casa_imaging_model::CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [(SHAPE[0] / 2) as f64, (SHAPE[1] / 2) as f64],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    continuum_problem_with_domains_and_reconstruction(
        observation,
        products,
        restoring_beam,
        response,
        basis,
        algorithm,
        channels,
        [1.4e9, 1.0e6],
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
    )
}

#[allow(clippy::too_many_arguments)]
fn continuum_problem_with_domains_and_reconstruction(
    observation: u8,
    products: &[ProductKind],
    restoring_beam: RestoringBeamPolicy,
    response: InstrumentResponse,
    basis: ReconstructionBasis,
    algorithm: ReconstructionAlgorithm,
    channels: usize,
    frequency: [f64; 2],
    domains: Vec<ImageDomainSpec>,
) -> casa_imaging_model::CompiledProblem {
    let controls = if matches!(algorithm, ReconstructionAlgorithm::Mtmfs { .. }) {
        ReconstructionControls::new(1, 1.0, 0.0)
    } else {
        ReconstructionControls::new(0, 1.0, 0.0)
    };
    let phase_centre = domains
        .iter()
        .find(|domain| domain.role() == &ImageDomainRole::Main)
        .expect("one main domain")
        .direction()
        .reference_direction();
    let geometry = GeometryInput::new(
        domains,
        CentreLaws::new(
            PhaseCentreLaw::Fixed(phase_centre),
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
                reference_frequency_hz: frequency[0],
                increment_hz: frequency[1],
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
                SpectralContract::new(
                    SpectralSamplingLaw::IDENTITY,
                    if restoring_beam == RestoringBeamPolicy::Common {
                        SpectralCoupling::CommonRestoringBeam
                    } else {
                        SpectralCoupling::Independent
                    },
                ),
                MeasurementEquationContract::new(
                    response,
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
                    parallactic_angles_rad: [0.0, 0.0],
                    phase_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    delay_direction: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    pointing_directions: casa_imaging_model::SelectedPointingDirections {
                        antenna1: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                        antenna2: SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
                    },
                },
                domain_projections: SelectedImageDomainProjections::one_domain_with_shared_psf(
                    SelectedPhaseCentreProjection::new(
                        [1.0 + row_index as f64, source_index as f64, 0.0],
                        0.0,
                    )
                    .expect("finite one-domain projection"),
                ),
                metadata: SelectedSampleMetadata {
                    field_id: 0,
                    antenna1: 0,
                    antenna2: 1,
                    antenna_responses: None,
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

struct TwoDomainFixtureRun {
    row: SelectedObservationRunRow,
    channel: SelectedObservationRunChannel,
    correlation: SelectedObservationRunCorrelation,
    contributions: SelectedSpectralContributions,
}

impl TwoDomainFixtureRun {
    fn view(&self) -> casa_imaging_model::SelectedObservationSampleView<'_> {
        casa_imaging_model::SelectedObservationSampleView::from_run(
            &self.row,
            &self.channel,
            &self.correlation,
        )
    }
}

fn two_domain_fixture_runs(
    problem: &casa_imaging_model::CompiledProblem,
) -> Vec<TwoDomainFixtureRun> {
    fixture_samples(problem)
        .into_iter()
        .map(|sample| {
            let main = SelectedPhaseCentreProjection::new(
                sample.coordinates.transformed_uvw_m,
                sample.coordinates.phase_shift_m,
            )
            .expect("main projection");
            let outlier = SelectedPhaseCentreProjection::new(
                [
                    sample.coordinates.transformed_uvw_m[0] * 1.7,
                    sample.coordinates.transformed_uvw_m[1] + 0.4,
                    sample.coordinates.transformed_uvw_m[2],
                ],
                sample.coordinates.phase_shift_m + 0.2,
            )
            .expect("outlier projection");
            let address = sample.address;
            TwoDomainFixtureRun {
                row: SelectedObservationRunRow {
                    measurement_set: address.measurement_set,
                    physical_row: address.physical_row,
                    data_description_id: address.data_description_id,
                    spectral_window_id: address.spectral_window_id,
                    polarization_id: address.polarization_id,
                    prediction_target: sample.prediction_target,
                    row_flag: sample.row_flag,
                    coordinates: sample.coordinates,
                    domain_projections: SelectedImageDomainProjections::new([
                        SelectedImageDomainProjection::with_shared_psf(0, main),
                        SelectedImageDomainProjection::with_shared_psf(1, outlier),
                    ])
                    .expect("canonical projections"),
                    metadata: sample.metadata,
                },
                channel: SelectedObservationRunChannel {
                    channel_index: address.channel_index,
                    frequency_centre_hz: address.frequency_centre_hz,
                    frequency_lower_hz: address.frequency_lower_hz,
                    frequency_upper_hz: address.frequency_upper_hz,
                    channel_width_hz: address.channel_width_hz,
                    frequency_frame: address.frequency_frame,
                },
                correlation: SelectedObservationRunCorrelation {
                    correlation_index: address.correlation_index,
                    correlation_type: address.correlation_type,
                    visibility: sample.visibility,
                    channel_flag: sample.channel_flag,
                    parallel_hand_group_flag: sample.parallel_hand_group_flag,
                    input_weight: sample.input_weight,
                },
                contributions: exact_contributions(&sample),
            }
        })
        .collect()
}

fn exact_contributions(sample: &SelectedObservationSample) -> SelectedSpectralContributions {
    SelectedSpectralContributions::new([
        SelectedSpectralContribution::new(0, 1.0, sample.address.frequency_centre_hz),
        None,
    ])
    .expect("one exact output contribution")
}

fn channel_contributions(sample: &SelectedObservationSample) -> SelectedSpectralContributions {
    SelectedSpectralContributions::new([
        SelectedSpectralContribution::new(
            sample.address.channel_index,
            1.0,
            sample.address.frequency_centre_hz,
        ),
        None,
    ])
    .expect("one exact channel-local contribution")
}

fn replay_selected_generation(
    problem: &casa_imaging_model::CompiledProblem,
    samples: &[SelectedObservationSample],
) -> SelectedObservationGenerationId {
    let (generation, count) = problem
        .inspect_selected_observation(samples.iter().cloned().map(Ok::<_, Infallible>), |_| {
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
    run_round_with_contributions(problem, attempt_byte, samples, exact_contributions)
}

fn run_round_with_contributions(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
    samples: Vec<SelectedObservationSample>,
    contributions: fn(&SelectedObservationSample) -> SelectedSpectralContributions,
) -> ContinuumRound {
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting residency plan");
    let selected_generation = replay_selected_generation(problem, &samples);
    let generation = freeze_weighting_generation(problem, &plan, &samples, contributions)
        .expect("freeze global weighting generation");
    let (blocks, summary) = replay(&generation, problem, &plan, &samples, contributions);
    assert!(!blocks.is_empty(), "replay must emit bounded blocks");

    run_product_fixture_cycles(
        problem,
        attempt_byte,
        &plan,
        &generation,
        (blocks, summary),
        selected_generation,
        [casa_imaging_model::ModelDeltaTerm::new(
            casa_imaging_model::ModelCell::new(
                0,
                0,
                0,
                problem.geometry().domains()[0]
                    .shape()
                    .pixels()
                    .map(|extent| extent / 2),
            ),
            casa_imaging_model::ModelValue::new(0.75).expect("finite value"),
        )],
    )
}

fn run_two_domain_round(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
) -> ContinuumRound {
    let runs = two_domain_fixture_runs(problem);
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");

    let mut inspection = problem.begin_selected_observation_inspection();
    for run in &runs {
        inspection
            .push_view(run.view())
            .expect("inspect run member");
    }
    let (selected_generation, selected_count) = inspection.finish().expect("selected generation");
    assert_eq!(selected_count as usize, runs.len());

    let mut density = begin_weighting_generation(problem, &plan).expect("density phase");
    for run in &runs {
        density
            .consume(
                problem,
                run.view(),
                run.channel.frequency_centre_hz,
                run.contributions.clone(),
            )
            .expect("density sample");
    }
    let mut sum_weight = density.finish(problem).expect("sum-weight phase");
    for run in &runs {
        sum_weight
            .consume(
                problem,
                run.view(),
                run.channel.frequency_centre_hz,
                run.contributions.clone(),
            )
            .expect("sum-weight sample");
    }
    let generation = sum_weight.finish().expect("weighting generation");
    let mut replay = generation
        .begin_replay(problem, &plan)
        .expect("begin replay");
    let mut blocks = Vec::new();
    for run in &runs {
        if let Some(block) = replay
            .consume(
                problem,
                run.view(),
                run.channel.frequency_centre_hz,
                run.contributions.clone(),
            )
            .expect("replay sample")
        {
            blocks.push(block);
        }
    }
    let (tail, summary) = replay.finish().expect("finish replay");
    if let Some(block) = tail {
        blocks.push(block);
    }

    run_product_fixture_cycles(
        problem,
        attempt_byte,
        &plan,
        &generation,
        (blocks, summary),
        selected_generation,
        [
            casa_imaging_model::ModelDeltaTerm::new(
                casa_imaging_model::ModelCell::new(0, 0, 0, [4, 4]),
                casa_imaging_model::ModelValue::new(0.75).expect("main model value"),
            ),
            casa_imaging_model::ModelDeltaTerm::new(
                casa_imaging_model::ModelCell::new(1, 0, 0, [2, 1]),
                casa_imaging_model::ModelValue::new(0.25).expect("outlier model value"),
            ),
        ],
    )
}

fn run_product_fixture_cycles(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
    plan: &WeightingPlan,
    generation: &WeightingAlgorithmState,
    replay: (Vec<WeightingReplayChunk>, WeightingReplaySummary),
    selected_generation: SelectedObservationGenerationId,
    delta_terms: impl IntoIterator<Item = casa_imaging_model::ModelDeltaTerm>,
) -> ContinuumRound {
    let (blocks, summary) = replay;
    let run = |lifecycle: &mut ModelLifecycle,
               preparation: MajorCyclePreparation,
               prior: Option<casa_imaging_reconstruction::FinalNormalState>| {
        let specification = SpectralOperatorSpecification::new(problem).expect("specification");
        let pass = if prior.is_some() {
            SpectralOperatorPass::ResidualRefresh
        } else {
            SpectralOperatorPass::InitialMajor
        };
        let workload =
            spectral_operator_workload(&specification, plan.limits().max_block_samples(), pass)
                .expect("workload");
        let prepared =
            prepare_spectral_operator(specification, workload).expect("prepare operator");
        let mut state = prepared
            .begin(problem, generation)
            .expect("begin complete-data owner");
        state
            .bind_major_cycle_model(preparation.final_model(), prior)
            .expect("bind exact model");
        for block in &blocks {
            state.consume_block(block).expect("consume weighted block");
        }
        let evidence: CompleteDataOwnerResult = state
            .complete(&summary, selected_generation, None)
            .expect("complete evidence");
        MajorCycleOwner::from_complete_data(
            {
                let storage =
                    casa_imaging_reconstruction::runtime_adapter::NormalStoragePlan::resident(
                        evidence.primitives().slab().total_channels(),
                    )
                    .expect("fixture normal window");
                evidence.seal(&storage).expect("seal fixture normal state")
            },
            preparation,
        )
        .expect("major-cycle owner")
        .reconcile(lifecycle)
        .expect("reconcile")
    };
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable problem"),
        attempt(attempt_byte),
        7,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("initial lifecycle");
    let named = lifecycle.initial_empty().expect("empty model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("initial model");
    let (normal, continuation) = run(&mut lifecycle, preparation, None).into_continuation();
    let (mut lifecycle, named) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable problem"),
        attempt(attempt_byte),
        8,
        continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("continued lifecycle");
    let delta = lifecycle
        .compile_delta(&named, delta_terms)
        .expect("nonzero model delta");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, Some(delta)).expect("final model");
    ContinuumRound {
        join: run(&mut lifecycle, preparation, Some(normal)),
    }
}

fn rerun_two_domain_with_masks(
    problem: &casa_imaging_model::CompiledProblem,
    attempt_byte: u8,
    epoch: u64,
    prior: MajorCycleCompletion,
    masks: &ImageDomainReconstructionMasks,
) -> ContinuumRound {
    let runs = two_domain_fixture_runs(problem);
    let (prior_normal, continuation) = prior.into_continuation();
    let (mut lifecycle, named) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable problem"),
        attempt(attempt_byte),
        epoch,
        continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("continue model lifecycle");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("prepare continuation");
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let mut inspection = problem.begin_selected_observation_inspection();
    for run in &runs {
        inspection
            .push_view(run.view())
            .expect("inspect run member");
    }
    let (selected_generation, _) = inspection.finish().expect("selected generation");
    let mut density = begin_weighting_generation(problem, &plan).expect("density phase");
    for run in &runs {
        density
            .consume(
                problem,
                run.view(),
                run.channel.frequency_centre_hz,
                run.contributions.clone(),
            )
            .expect("density sample");
    }
    let mut sum_weight = density.finish(problem).expect("sum-weight phase");
    for run in &runs {
        sum_weight
            .consume(
                problem,
                run.view(),
                run.channel.frequency_centre_hz,
                run.contributions.clone(),
            )
            .expect("sum-weight sample");
    }
    let generation = sum_weight.finish().expect("weighting generation");
    let mut replay = generation
        .begin_replay(problem, &plan)
        .expect("begin replay");
    let mut blocks = Vec::new();
    for run in &runs {
        if let Some(block) = replay
            .consume(
                problem,
                run.view(),
                run.channel.frequency_centre_hz,
                run.contributions.clone(),
            )
            .expect("replay sample")
        {
            blocks.push(block);
        }
    }
    let (tail, summary) = replay.finish().expect("finish replay");
    if let Some(block) = tail {
        blocks.push(block);
    }
    let specification =
        SpectralOperatorSpecification::new(problem).expect("two-domain specification");
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::ResidualRefresh,
    )
    .expect("two-domain refresh workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepare operator");
    let mut state = prepared
        .begin(problem, &generation)
        .expect("begin complete-data owner");
    state
        .bind_major_cycle_model(preparation.final_model(), Some(prior_normal))
        .expect("bind prior normal state");
    for block in &blocks {
        state.consume_block(block).expect("consume weighted block");
    }
    let evidence = state
        .complete(&summary, selected_generation, None)
        .expect("complete two-domain evidence");
    let joined = MajorCycleOwner::from_complete_data(
        {
            let storage =
                casa_imaging_reconstruction::runtime_adapter::NormalStoragePlan::resident(
                    evidence.primitives().slab().total_channels(),
                )
                .expect("fixture normal window");
            evidence.seal(&storage).expect("seal fixture normal state")
        },
        preparation,
    )
    .expect("major-cycle owner")
    .bind_reconstruction_masks(&ReconstructionMaskSet::Domains(masks.clone()))
    .expect("bind exact domain masks")
    .reconcile(&mut lifecycle)
    .expect("masked two-domain reconciliation");
    ContinuumRound { join: joined }
}

fn freeze_weighting_generation(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
    contributions: fn(&SelectedObservationSample) -> SelectedSpectralContributions,
) -> Result<WeightingAlgorithmState, WeightingError> {
    let mut density = begin_weighting_generation(problem, plan)?;
    for sample in samples {
        density.consume(
            problem,
            sample,
            sample.address.frequency_centre_hz,
            contributions(sample),
        )?;
    }
    let mut sum_weight = density.finish(problem)?;
    for sample in samples {
        sum_weight.consume(
            problem,
            sample,
            sample.address.frequency_centre_hz,
            contributions(sample),
        )?;
    }
    sum_weight.finish()
}

fn replay(
    generation: &WeightingAlgorithmState,
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
    contributions: fn(&SelectedObservationSample) -> SelectedSpectralContributions,
) -> (Vec<WeightingReplayChunk>, WeightingReplaySummary) {
    let mut blocks = Vec::new();
    let mut phase = generation
        .begin_replay(problem, plan)
        .expect("begin replay");
    for sample in samples {
        if let Some(block) = phase
            .consume(
                problem,
                sample,
                sample.address.frequency_centre_hz,
                contributions(sample),
            )
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

fn planned_for<'a>(
    inputs: &ContinuumProductInputs<'a>,
    controls: &ContinuumProductControls,
) -> PlannedContinuumGeneration {
    PlannedContinuumGeneration::new(inputs, controls).expect("planned generation")
}

fn generate_for(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> GeneratedProducts {
    let output = MemoryProductOutput::default();
    let generated = produce_continuum_members(planned, inputs, full_window(planned), &output)
        .expect("direct product generation");
    GeneratedProducts::from_output(&generated, &output)
}

#[test]
fn planned_generation_binds_the_exact_graph_and_run_associations() {
    let problem = continuum_problem(81, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 82);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .expect("direct product inputs");
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned generation");
    assert_eq!(planned.problem_id(), problem.problem_id());
    assert_eq!(planned.graph_id(), problem.product_graph().graph_id());
    assert_eq!(
        planned.major_cycle_completion(),
        inputs.major_cycle_completion()
    );
    assert_eq!(
        planned.normal_state_completion(),
        inputs.normal_state_completion()
    );
    assert_eq!(
        planned.final_model_generation(),
        round.join.normal_state().final_model_generation()
    );

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

    let replanned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("replanned");
    assert_eq!(planned.problem_id(), replanned.problem_id());
    assert_eq!(planned.graph_id(), replanned.graph_id());
    assert_eq!(planned.members().len(), replanned.members().len());
    assert_eq!(
        planned
            .members()
            .iter()
            .map(|member| member.node())
            .collect::<Vec<_>>(),
        replanned
            .members()
            .iter()
            .map(|member| member.node())
            .collect::<Vec<_>>()
    );

    let other = continuum_problem(83, &CONTINUUM_PRODUCTS);
    let other_round = run_continuum_round(&other, 84);
    let other_inputs =
        ContinuumProductInputs::from_major_cycle(&other, &other_round.join).expect("other inputs");
    let other_planned =
        PlannedContinuumGeneration::new(&other_inputs, &ContinuumProductControls::default())
            .expect("other planned");
    assert_ne!(planned.problem_id(), other_planned.problem_id());
    assert_ne!(
        planned.major_cycle_completion(),
        other_planned.major_cycle_completion()
    );
}

#[test]
fn direct_generation_writes_the_exact_member_set_once() {
    let problem = continuum_problem(85, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 86);
    let inputs =
        ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("product inputs");
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned");
    let output = MemoryProductOutput::default();
    let generated = produce_continuum_members(&planned, &inputs, full_window(&planned), &output)
        .expect("generated members");

    assert_eq!(generated.members().len(), planned.members().len());
    for (generated_member, planned_member) in generated.members().iter().zip(planned.members()) {
        assert_eq!(generated_member.node(), planned_member.node());
        assert_eq!(generated_member.contract().role(), planned_member.role());
        assert_eq!(
            output.write_count(planned_member.node()),
            1,
            "full-window generation writes each member once"
        );
        assert!(output.finished(planned_member.node()));
    }
    let beam = generated
        .restoring_beams()
        .iter()
        .copied()
        .flatten()
        .next()
        .expect("fitted restoring beam");
    assert!(beam.major_fwhm_rad() >= beam.minor_fwhm_rad());
    assert!(beam.major_fwhm_rad() > 0.0);

    let collected = GeneratedProducts::from_output(&generated, &output);
    let psf_payload = collected
        .members()
        .iter()
        .find(|member| member.name() == ".psf")
        .expect("psf member")
        .payload();
    let sensitivity = round.join.normal_state().sum_weight();
    let expected_psf = round
        .join
        .normal_state()
        .read_window(0..1)
        .expect("single-plane continuum fixture window")
        .normal_approximation()
        .iter()
        .map(|value| value.re as f32 / sensitivity as f32)
        .collect::<Vec<_>>();
    assert_eq!(psf_payload, expected_psf);

    let sumwt_index = collected
        .members()
        .iter()
        .position(|member| member.name() == ".sumwt")
        .expect("sumwt member");
    assert_eq!(
        collected.members()[sumwt_index].payload(),
        &[sensitivity as f32][..]
    );
}

#[test]
fn two_domain_members_consume_their_matching_normal_and_model_chart() {
    let main_direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [4.0, 4.0],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let outlier_shape = [6, 4];
    let outlier_direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.02, -0.48),
        [3.0, 2.0],
        [-1.5e-6, 1.5e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let domains = vec![
        ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            main_direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        ),
        ImageDomainSpec::new(
            ImageDomainRole::Outlier("east".into()),
            ImageShape::new(outlier_shape[0], outlier_shape[1]),
            outlier_direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        ),
    ];
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::Weight,
        ProductKind::Mask,
    ];
    let problem = continuum_problem_with_domains_and_reconstruction(
        141,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
        [1.4e9, 1.0e6],
        domains,
    );
    let first_round = run_two_domain_round(&problem, 142);
    assert_eq!(first_round.join.normal_state().domain_count(), 2);
    let mask_plans =
        ImageDomainReconstructionMaskPlans::new(problem.geometry().domains().iter().map(
            |domain| ReconstructionMaskPlan::FullPlane {
                coordinate: domain.direction(),
            },
        ))
        .expect("domain mask plans");
    let (masks, _) = mask_plans
        .materialize(
            first_round.join.final_model(),
            first_round.join.normal_state(),
        )
        .expect("domain masks")
        .into_parts();
    let alternate_plans = ImageDomainReconstructionMaskPlans::new([
        ReconstructionMaskPlan::FullPlane {
            coordinate: main_direction,
        },
        ReconstructionMaskPlan::Boxes {
            coordinate: outlier_direction,
            boxes: vec![MaskBox::new([0, 0], [1, 1]).expect("alternate outlier box")],
        },
    ])
    .expect("alternate mask plans");
    let (alternate_masks, _) = alternate_plans
        .materialize(
            first_round.join.final_model(),
            first_round.join.normal_state(),
        )
        .expect("alternate domain masks")
        .into_parts();
    assert_ne!(masks.generation_id(), alternate_masks.generation_id());
    let round = rerun_two_domain_with_masks(&problem, 143, 8, first_round.join, &masks);
    let normal = round.join.normal_state();
    assert_eq!(
        normal.image_domain_mask_generation(),
        Some(masks.generation_id())
    );
    assert!(matches!(
        ContinuumProductInputs::from_major_cycle(&problem, &round.join)
            .expect("alternate inputs")
            .with_domain_reconstruction_masks(&alternate_masks),
        Err(ProductsError::SourceLineageMismatch)
    ));
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .expect("two-domain inputs")
        .with_domain_reconstruction_masks(&masks)
        .expect("domain-mask inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    planned
        .demand(&inputs, full_window(&planned))
        .expect("two-domain product residency demand");
    let generated = generate_for(&planned, &inputs);
    assert_eq!(generated.members().len(), products.len() * 2);

    let window = normal
        .read_window(normal.slab().core_range())
        .expect("continuum fixture window");

    for (ordinal, role) in [
        ImageDomainRole::Main,
        ImageDomainRole::Outlier("east".into()),
    ]
    .iter()
    .enumerate()
    {
        let domain = window.domain_by_role(role).expect("domain normal state");
        let expected_shape = problem.geometry().domains()[ordinal].shape().pixels();
        let model_member = generated
            .members()
            .iter()
            .find(|member| {
                member.contract().axes().domain() == role
                    && member.contract().role()
                        == ProductRole::Model(casa_imaging_model::ProductTerm::Single)
            })
            .expect("domain model member");
        assert_eq!(
            model_member.payload().len(),
            expected_shape[0] * expected_shape[1]
        );
        let expected_model_peak = if ordinal == 0 { 0.75 } else { 0.25 };
        assert_eq!(
            model_member
                .payload()
                .iter()
                .copied()
                .fold(f32::NEG_INFINITY, f32::max),
            expected_model_peak
        );

        let psf_member = generated
            .members()
            .iter()
            .find(|member| {
                member.contract().axes().domain() == role
                    && member.contract().role()
                        == ProductRole::Psf(casa_imaging_model::ProductTerm::Single)
            })
            .expect("domain PSF member");
        let sum_weight = domain.sum_weights()[0] as f32;
        let expected_psf = if sum_weight.is_finite() && sum_weight > 0.0 {
            domain
                .normal_approximation()
                .iter()
                .map(|value| value.re as f32 / sum_weight)
                .collect::<Vec<_>>()
        } else {
            vec![0.0; expected_shape[0] * expected_shape[1]]
        };
        assert_eq!(psf_member.payload(), expected_psf);

        let mask_member = generated
            .members()
            .iter()
            .find(|member| {
                member.contract().axes().domain() == role
                    && member.contract().role() == ProductRole::CleanMask
            })
            .expect("domain mask member");
        let expected_mask = vec![1.0; expected_shape[0] * expected_shape[1]];
        assert_eq!(mask_member.payload(), expected_mask);
    }
}

#[test]
fn direct_generation_rejects_same_problem_with_foreign_completions() {
    let main_direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [4.0, 4.0],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let outlier_direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.02, -0.48),
        [3.0, 2.0],
        [-1.5e-6, 1.5e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let domains = vec![
        ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            main_direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        ),
        ImageDomainSpec::new(
            ImageDomainRole::Outlier("east".into()),
            ImageShape::new(6, 4),
            outlier_direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        ),
    ];
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::Weight,
        ProductKind::Mask,
    ];
    let problem = continuum_problem_with_domains_and_reconstruction(
        145,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
        [1.4e9, 1.0e6],
        domains,
    );
    let first_round = run_two_domain_round(&problem, 146);
    let mask_plans =
        ImageDomainReconstructionMaskPlans::new(problem.geometry().domains().iter().map(
            |domain| ReconstructionMaskPlan::FullPlane {
                coordinate: domain.direction(),
            },
        ))
        .expect("domain mask plans");
    let (masks, _) = mask_plans
        .materialize(
            first_round.join.final_model(),
            first_round.join.normal_state(),
        )
        .expect("domain masks")
        .into_parts();

    let second_round = rerun_two_domain_with_masks(&problem, 147, 8, first_round.join, &masks);
    let second_inputs = ContinuumProductInputs::from_major_cycle(&problem, &second_round.join)
        .expect("second inputs")
        .with_domain_reconstruction_masks(&masks)
        .expect("second mask-bound inputs");
    let planned = planned_for(&second_inputs, &ContinuumProductControls::default());
    drop(second_inputs);

    let third_round = rerun_two_domain_with_masks(&problem, 148, 9, second_round.join, &masks);
    let third_inputs = ContinuumProductInputs::from_major_cycle(&problem, &third_round.join)
        .expect("third inputs")
        .with_domain_reconstruction_masks(&masks)
        .expect("third mask-bound inputs");
    assert_eq!(planned.problem_id(), third_inputs.problem().problem_id());
    assert_ne!(
        planned.final_model_generation(),
        third_inputs.final_model().generation_id(),
        "a distinct execution attempt owns a distinct adopted model generation"
    );
    assert_ne!(
        planned.major_cycle_completion(),
        third_inputs.major_cycle_completion(),
        "each reconciliation has its own run association"
    );
    assert_ne!(
        planned.normal_state_completion(),
        third_inputs.normal_state_completion(),
        "each reconciliation has its own normal-state completion"
    );

    assert!(matches!(
        planned.demand(&third_inputs, full_window(&planned)),
        Err(ProductsError::SourceLineageMismatch)
    ));
    let output = MemoryProductOutput::default();
    assert!(matches!(
        produce_continuum_members(&planned, &third_inputs, full_window(&planned), &output),
        Err(ProductsError::SourceLineageMismatch)
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
fn mosaic_sensitivity_owns_normalization_primary_beam_and_valid_support() {
    let sensitivity =
        MosaicSensitivity::new(&[16.0, 4.0, 1.0, 0.0]).expect("finite positive mosaic sensitivity");
    assert_eq!(sensitivity.primary_beam(), [1.0, 0.5, 0.25, 0.0]);
    assert_eq!(
        sensitivity
            .normalize(&[32.0, 16.0, 8.0, 4.0], ProductNormalization::FlatNoise)
            .expect("flat-noise normalization"),
        [2.0, 2.0, 2.0, 0.0]
    );
    assert_eq!(
        sensitivity
            .normalize(&[32.0, 16.0, 8.0, 4.0], ProductNormalization::FlatSky)
            .expect("flat-sky normalization"),
        [2.0, 4.0, 8.0, 0.0]
    );

    let policy = PrimaryBeamValidityPolicy::new(
        0.25,
        ProductSupportComparison::StrictlyGreater,
        ProductBlankingPolicy::Zero,
    )
    .expect("valid PB policy");
    assert_eq!(sensitivity.validity(policy), [true, true, false, false]);
    assert_eq!(
        sensitivity
            .correct_primary_beam(&[2.0, 2.0, 2.0, 2.0], policy)
            .expect("PB correction"),
        [2.0, 4.0, 0.0, 0.0]
    );
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
fn direct_generation_publishes_metadata_without_payload_residency() {
    let problem = continuum_problem(92, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 93);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("planned");
    let output = MemoryProductOutput::default();
    let generated = produce_continuum_members(&planned, &inputs, full_window(&planned), &output)
        .expect("generated");
    assert_eq!(generated.problem_id(), problem.problem_id());
    assert_eq!(generated.graph_id(), problem.product_graph().graph_id());
    assert_eq!(generated.members().len(), planned.members().len());
    let collected = GeneratedProducts::from_output(&generated, &output);
    for (member, planned_member) in collected.members().iter().zip(planned.members()) {
        assert_eq!(member.node(), planned_member.node());
        assert_eq!(member.name(), planned_member.name());
        assert_eq!(member.contract().role(), planned_member.role());
        assert_eq!(member.contract().unit(), planned_member.unit());
        assert_eq!(member.contract().schema(), planned_member.schema());
        assert_eq!(member.contract().validity(), planned_member.validity());
        assert_eq!(member.payload().len(), planned_member.payload_values());
        assert!(output.finished(planned_member.node()));
    }
}

#[test]
fn direct_generation_counts_bounded_windows_and_finishes_each_member() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::Mask,
    ];
    let problem = continuum_problem_with_reconstruction(
        96,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let round = run_round_with_contributions(
        &problem,
        97,
        fixture_samples(&problem),
        channel_contributions,
    );
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let output = MemoryProductOutput::default();
    let storage_plan = ProductStoragePlan::new(1).expect("one-channel output bound");
    let generated = produce_continuum_members(&planned, &inputs, storage_plan, &output)
        .expect("bounded generation");
    assert_eq!(generated.members().len(), planned.members().len());
    for member in planned.members() {
        let channels = member.axes().spectral().output_channels();
        assert_eq!(output.write_count(member.node()), channels);
        assert!(output.finished(member.node()));
    }
    let bounded = GeneratedProducts::from_output(&generated, &output);
    let full = generate_for(&planned, &inputs);
    for (bounded, full) in bounded.members().iter().zip(full.members()) {
        assert_eq!(bounded.name(), full.name());
        assert_eq!(bounded.payload(), full.payload());
        assert_eq!(bounded.validity(), full.validity());
    }
}

#[test]
fn output_errors_fail_generation_without_a_completion_receipt() {
    let problem = continuum_problem(98, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 99);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());

    let write_failure = MemoryProductOutput::failing_write();
    assert!(matches!(
        produce_continuum_members(&planned, &inputs, full_window(&planned), &write_failure),
        Err(ProductsError::Storage(_))
    ));
    assert_eq!(write_failure.begun_members(), 1);
    assert!(!write_failure.finished(planned.members()[0].node()));

    let finish_failure = MemoryProductOutput::failing_finish();
    assert!(matches!(
        produce_continuum_members(&planned, &inputs, full_window(&planned), &finish_failure),
        Err(ProductsError::Storage(_))
    ));
    assert_eq!(finish_failure.begun_members(), 1);
    assert!(!finish_failure.finished(planned.members()[0].node()));
}

#[test]
fn generic_generation_demand_charges_exact_owned_arrays() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::Mask,
    ];
    let problem = continuum_problem_with_policy(94, &products, RestoringBeamPolicy::None);
    let round = run_continuum_round(&problem, 95);
    let inputs =
        ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("product inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let demand = planned
        .demand(&inputs, full_window(&planned))
        .expect("generic demand");
    let values = planned
        .members()
        .iter()
        .map(|member| member.payload_values() as u64)
        .sum::<u64>();
    let maximum = planned
        .members()
        .iter()
        .map(|member| member.payload_values() as u64)
        .max()
        .expect("members");
    assert_eq!(
        values,
        planned
            .members()
            .iter()
            .map(|member| member.payload_values() as u64)
            .sum::<u64>()
    );
    assert_eq!(demand.maximum_member_payload_bytes(), maximum * 4);
    assert_eq!(demand.maximum_member_validity_bytes(), maximum);
    assert_eq!(demand.maximum_window_payload_bytes(), maximum * 4);
    assert_eq!(demand.maximum_window_validity_bytes(), maximum);
    assert_eq!(
        demand.algorithm_scratch_bytes(),
        (SHAPE[0]
            * SHAPE[1]
            * (2 * size_of::<f32>() + size_of::<casa_imaging_model::ModelSample>())) as u64
            + casa_imaging_reconstruction::normal_state_window_residency_bytes(SHAPE, 1, 1, 1)
                .unwrap()
            + maximum * 5,
        "generic normalization overlaps input windows, its converted plane/result, and one output window"
    );
    assert_eq!(
        demand.peak_residency_bytes(),
        demand.algorithm_scratch_bytes()
            + demand.retained_metadata_bytes()
            + demand.beam_scratch_bytes()
    );
    let generated = produce_continuum_members(
        &planned,
        &inputs,
        full_window(&planned),
        &MemoryProductOutput::default(),
    )
    .unwrap();
    assert_eq!(
        demand.retained_metadata_bytes(),
        common::retained_metadata_bytes(&generated)
    );
    assert!(demand.retained_metadata_bytes() > 0);
}

#[test]
fn cube_generation_demand_retains_channel_beams_and_charges_common_fit_scratch() {
    use casa_imaging_products::RestoringBeam;
    for (offset, policy) in [
        RestoringBeamPolicy::None,
        RestoringBeamPolicy::PerPlane,
        RestoringBeamPolicy::Common,
    ]
    .into_iter()
    .enumerate()
    {
        let mut products = vec![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::SumWeights,
        ];
        if policy != RestoringBeamPolicy::None {
            products.push(ProductKind::RestoredImage);
        }
        let problem = continuum_problem_with_reconstruction(
            180 + offset as u8,
            &products,
            policy,
            InstrumentResponse::Scalar,
            ReconstructionBasis::ChannelLocal { channels: 2 },
            ReconstructionAlgorithm::Dirty,
            2,
        );
        let round = run_round_with_contributions(
            &problem,
            190 + offset as u8,
            fixture_samples(&problem),
            channel_contributions,
        );
        let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).unwrap();
        let planned = planned_for(&inputs, &ContinuumProductControls::default());
        let demand = planned
            .demand(&inputs, ProductStoragePlan::new(1).unwrap())
            .unwrap();
        let generated = produce_continuum_members(
            &planned,
            &inputs,
            ProductStoragePlan::new(1).unwrap(),
            &MemoryProductOutput::default(),
        )
        .unwrap();
        assert_eq!(generated.fitted_beams().len(), 2);
        assert_eq!(
            demand.retained_metadata_bytes(),
            common::retained_metadata_bytes(&generated)
        );
        assert!(
            demand.beam_scratch_bytes()
                >= casa_imaging_reconstruction::psf_fit_workspace_bytes(SHAPE)
        );
        if policy == RestoringBeamPolicy::Common {
            assert!(
                demand.beam_scratch_bytes()
                    >= (2
                        * (size_of::<RestoringBeam>()
                            + 2 * size_of::<casa_numerics::EllipticalGaussian>()))
                        as u64
            );
        }
        assert_eq!(
            demand.peak_residency_bytes(),
            demand.algorithm_scratch_bytes()
                + demand.retained_metadata_bytes()
                + demand.beam_scratch_bytes()
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
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let generated = generate_for(&planned, &inputs);
    let weight = generated
        .members()
        .iter()
        .find(|member| member.name().starts_with(".weight"))
        .expect("weight member");
    let expected: Vec<f32> = round
        .join
        .normal_state()
        .read_window(0..1)
        .expect("single-plane continuum fixture window")
        .sensitivity()
        .iter()
        .map(|value| *value as f32)
        .collect();
    assert_eq!(weight.payload(), expected);
}

#[test]
fn standard_products_publish_the_selected_analytic_primary_beam() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
        ProductKind::PbCorrectedImage,
        ProductKind::Beam,
    ];
    let problem = continuum_problem_with_reconstruction(
        109,
        &products,
        RestoringBeamPolicy::PerPlane,
        InstrumentResponse::Scalar,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        1,
    );
    let round = run_continuum_round(&problem, 110);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let planned = planned_for(&inputs, &controls);
    let generated = generate_for(&planned, &inputs);
    let pb = generated
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("primary beam");
    let centre = pb.payload()[4 * SHAPE[1] + 4];
    let corner = pb.payload()[0];
    assert_eq!(centre, 1.0);
    assert!(
        corner < centre,
        "analytic PB must fall away from phase centre"
    );
}

#[test]
fn primary_beam_plan_rejects_a_cube_crossing_the_vla_band_boundary() {
    let products = [ProductKind::Psf, ProductKind::PrimaryBeam];
    let template = continuum_problem_with_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let problem = continuum_problem_with_domains_and_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
        [54.880e9, 128.0e6],
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(SHAPE[0], SHAPE[1]),
            template.geometry().domains()[0].direction(),
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )],
    );
    let round = run_round_with_contributions(
        &problem,
        112,
        fixture_samples(&problem),
        channel_contributions,
    );
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaVlaBand);
    let error = PlannedContinuumGeneration::new(&inputs, &controls)
        .expect_err("unsupported frequency must fail during planning, not production");
    assert_eq!(
        error,
        ProductsError::UnsupportedPrimaryBeamFrequency {
            model: AnalyticPrimaryBeamModel::CasaVlaBand,
            output_channel: 1,
            frequency_hz: 55.008e9,
        }
    );
    assert_eq!(controls.validate_for_problem(&problem), Err(error));
}

#[test]
fn standard_cube_products_publish_analytic_primary_beams_per_output_channel() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::PrimaryBeam,
    ];
    let problem = continuum_problem_with_reconstruction(
        111,
        &products,
        RestoringBeamPolicy::None,
        InstrumentResponse::Scalar,
        ReconstructionBasis::ChannelLocal { channels: 2 },
        ReconstructionAlgorithm::Dirty,
        2,
    );
    let round = run_round_with_contributions(
        &problem,
        112,
        fixture_samples(&problem),
        channel_contributions,
    );
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let planned = planned_for(&inputs, &controls);
    let generated = generate_for(&planned, &inputs);
    let pb = generated
        .members()
        .iter()
        .find(|member| member.name() == ".pb")
        .expect("primary beam");
    assert_eq!(pb.payload().len(), SHAPE[0] * SHAPE[1] * 2);
    assert_eq!(pb.payload().iter().copied().reduce(f32::max), Some(1.0),);
    assert!(pb.payload().iter().any(|value| *value < 1.0));
    let payload = pb.payload();
    for channel in 0..2 {
        let plane = payload.iter().skip(channel).step_by(2);
        assert_eq!(plane.clone().copied().reduce(f32::max), Some(1.0));
        assert!(plane.copied().any(|value| value < 1.0));
    }
}

#[test]
fn clean_mask_product_is_the_committed_reconstruction_support() {
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
    let unbound_inputs =
        ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("unbound inputs");
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join)
        .expect("mask-bound base inputs")
        .with_reconstruction_mask(&mask)
        .expect("mask-bound inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let unbound_output = MemoryProductOutput::default();
    assert!(matches!(
        produce_continuum_members(
            &planned,
            &unbound_inputs,
            full_window(&planned),
            &unbound_output
        ),
        Err(ProductsError::SourceLineageMismatch)
    ));
    let generated = generate_for(&planned, &inputs);
    let published_mask = generated
        .members()
        .iter()
        .find(|member| member.name().starts_with(".mask"))
        .expect("mask member");
    assert!(
        published_mask.validity().iter().all(|valid| *valid),
        "the numeric CLEAN-mask support is not the product-validity mask"
    );
    let expected = mask
        .support()
        .iter()
        .map(|selected| if *selected { 1.0 } else { 0.0 })
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
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let generated = generate_for(&planned, &inputs);

    let sensitivity = round.join.normal_state().sum_weight();
    assert!(
        sensitivity.is_finite() && sensitivity > 0.0 && (sensitivity - 1.0).abs() > 1.0e-6,
        "fixture must carry a non-unit sum weight, got {sensitivity}"
    );
    let model_member = generated
        .members()
        .iter()
        .find(|member| member.name() == ".model")
        .expect("model member");
    assert!(
        model_member.payload().iter().any(|value| *value != 0.0),
        "fixture must carry a nonzero sky model"
    );
    let residual_member = generated
        .members()
        .iter()
        .find(|member| member.name() == ".residual")
        .expect("residual member");
    assert!(
        residual_member.payload().iter().any(|value| *value != 0.0),
        "fixture must carry a nonzero residual"
    );

    // Recompute the expected restoration independently from the generated parts.
    let beam = generated.restoring_beam().copied().expect("fitted beam");
    let cells = round.join.normal_state().shape();
    let cell = inputs.cell_size_rad();
    let kernel = gaussian_beam_image(cells, &beam, cell);
    let convolved = casa_imaging_products::fft_convolve(
        model_member.payload(),
        kernel.as_slice().expect("contiguous"),
        cells,
    );
    let restored = generated
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
fn generated_members_carry_the_complete_graph_contract() {
    // Every generated member must carry its full compiled contract: schema,
    // unit, WCS/axes law, beam rule with resolved fitted beam, validity
    // rule, and dependencies - not just name and payload.
    let problem = continuum_problem(111, &CONTINUUM_PRODUCTS);
    let round = run_continuum_round(&problem, 112);
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &round.join).expect("inputs");
    let planned = planned_for(&inputs, &ContinuumProductControls::default());
    let generated = generate_for(&planned, &inputs);

    let graph = problem.product_graph();
    for member in generated.members() {
        let node = graph
            .nodes()
            .iter()
            .find(|node| node.node_id() == member.node())
            .expect("generated member names a graph node");
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
    let fitted = generated.restoring_beam().copied().expect("fitted beam");
    let image = generated
        .members()
        .iter()
        .find(|member| member.name() == ".image")
        .expect("restored member");
    assert_eq!(image.resolved_beam(), Some(&fitted));
    let mask = generated
        .members()
        .iter()
        .find(|member| member.name() == ".mask")
        .expect("mask member");
    assert_eq!(mask.resolved_beam(), None);

    let expected_mask = vec![1.0; SHAPE[0] * SHAPE[1]];
    assert_eq!(mask.payload(), expected_mask);
}
