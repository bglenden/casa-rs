// SPDX-License-Identifier: LGPL-3.0-or-later

//! T44 acceptance contract for Taylor-family product construction.

use std::convert::Infallible;

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, IntentSelection, JointContinuumLineContract,
    LogicalIdentity, MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration,
    MetadataTableKind, ModelBounds, ModelColumnState, ModelColumnWrite, ModelExecutionAttemptId,
    ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity,
    MsColumnKind, NumericPrecision, NumericalStage, NumericsContract, ObservationSelection,
    ObservationSnapshotInput, ObservationSourceInput, ObservationSourceProvenance,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PolarizationContract,
    PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInputIdentities,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductRole, ProductSchema, ProductSupportComparison, ProductTerm,
    ProductUnit, ProductValidityPolicies, ProductValidityRule, Projection, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RestFrequency, RestoringBeamPolicy, RowSelection, ScientificContract,
    SelectedColumns, SelectedImageDomainProjections, SelectedMainRow,
    SelectedObservationGenerationId, SelectedObservationSample, SelectedPhaseCentreProjection,
    SelectedPredictionTarget, SelectedRows, SelectedSampleAddress, SelectedSampleCoordinates,
    SelectedSampleMetadata, SelectedSpectralContribution, SelectedSpectralContributions,
    SelectedVisibilitySample, SkyDirection, SourceGenerations, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, SpectralWindowSelection, StageErrorBudget, TaylorSupportReference,
    TaylorValidityPolicy, TimeScale, TimeSelection, UvSelection, UvwCoordinateLaw,
    VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract,
    WeightingScheme, compile, compile_observation,
};
use casa_imaging_products::{
    AnalyticPrimaryBeamModel, ContinuumProductControls, ContinuumProductInputs,
    ContinuumSourceCatalog, ProductGenerationAuthority, ProductsError, SealedContinuumGeneration,
    SealedMember, fft_convolve, gaussian_beam_image, produce_continuum_members,
};
use casa_imaging_reconstruction::{
    CoupledReconstructionMask, ExecutableModelProblem, MajorCycleCompletion, MajorCycleOwner,
    MajorCyclePreparation, MaskBox, ModelLifecycle, ReconstructionMask, ReconstructionMaskSet,
    SpectralOperatorSpecification, WeightingAlgorithmState, WeightingError,
    WeightingExecutionLimits, WeightingPlan, WeightingReplayChunk, WeightingReplaySummary,
    begin_weighting_generation, plan_weighting,
    runtime_adapter::{
        CompleteDataOwnerResult, SpectralOperatorPass, prepare_spectral_operator,
        spectral_operator_workload,
    },
};

const SHAPE: [usize; 2] = [8, 8];
const TERMS: usize = 2;
const TAYLOR_PRODUCTS: [ProductKind; 9] = [
    ProductKind::Psf,
    ProductKind::Residual,
    ProductKind::Model,
    ProductKind::RestoredImage,
    ProductKind::SumWeights,
    ProductKind::TaylorTerms,
    ProductKind::SpectralIndex,
    ProductKind::SpectralIndexError,
    ProductKind::Beam,
];

fn joint_product_masks(
    problem: &casa_imaging_model::CompiledProblem,
    model: casa_imaging_reconstruction::ModelGenerationId,
) -> CoupledReconstructionMask {
    let direction = problem.geometry().domains()[0].direction();
    let continuum = ReconstructionMask::full_plane(problem.problem_id(), model, direction, SHAPE)
        .expect("joint continuum mask");
    let line = ReconstructionMask::from_boxes(
        problem.problem_id(),
        model,
        direction,
        SHAPE,
        [MaskBox::new([3, 3], [4, 4]).expect("line box")],
    )
    .expect("joint line mask");
    CoupledReconstructionMask::new(continuum, line).expect("joint product masks")
}
const PB_PRODUCTS: [ProductKind; 9] = [
    ProductKind::Psf,
    ProductKind::Residual,
    ProductKind::Model,
    ProductKind::RestoredImage,
    ProductKind::SumWeights,
    ProductKind::PrimaryBeam,
    ProductKind::PbCorrectedImage,
    ProductKind::TaylorTerms,
    ProductKind::Beam,
];

fn identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut bytes = [seed; 32];
    bytes[0] = scope;
    LogicalIdentity::from_sha256(bytes)
}

fn attempt(seed: u8) -> ModelExecutionAttemptId {
    ModelExecutionAttemptId::new(identity(seed, 0))
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
        ObservationSourceProvenance::new(format!("fixture://t44/{seed}"), identity(seed, 2)),
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(
                3,
                [SelectedMainRow::new(0, 0), SelectedMainRow::new(2, 1)],
            )
            .expect("selected rows"),
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
    validity_with_taylor_fraction(0.1)
}

fn validity_with_taylor_fraction(taylor_fraction: f32) -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("PB validity"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            taylor_fraction,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("Taylor validity"),
    )
}

fn taylor_problem(
    seed: u8,
    products: &[ProductKind],
    response: InstrumentResponse,
) -> casa_imaging_model::CompiledProblem {
    taylor_problem_with_fraction(seed, products, response, 0.1)
}

fn taylor_problem_with_fraction(
    seed: u8,
    products: &[ProductKind],
    response: InstrumentResponse,
    taylor_fraction: f32,
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
                reference_frequency_hz: 1.1e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let references = if response == InstrumentResponse::Scalar {
        Vec::new()
    } else {
        vec![(ReferenceDataKind::Instrument, identity(seed, 90))]
    };
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source(seed)],
        references,
        ModelStateIdentity::Empty,
    ))
    .expect("observation snapshot");
    compile(ImagingRequest::new(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(
                    SpectralSamplingLaw::IDENTITY,
                    SpectralCoupling::CommonRestoringBeam,
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
                ReconstructionBasis::Taylor { terms: TERMS },
                ReconstructionAlgorithm::Mtmfs {
                    scales_px: vec![0.0],
                    small_scale_bias: 0.0,
                },
                ReconstructionControls::new(1, 1.0, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                products.to_vec(),
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::Common,
                validity_with_taylor_fraction(taylor_fraction),
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
            ModelBounds::new(4_096, 4_096, 4_096, 4_096, 1.0e30, 1.0e30).expect("model bounds"),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .expect("compile Taylor problem")
}

fn joint_problem(seed: u8, products: &[ProductKind]) -> casa_imaging_model::CompiledProblem {
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
                channels: 2,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.05e9,
                increment_hz: 1.0e8,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source(seed)],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("joint observation snapshot");
    compile(ImagingRequest::new(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(
                    SpectralSamplingLaw::IDENTITY,
                    SpectralCoupling::CommonRestoringBeam,
                ),
                MeasurementEquationContract::new(
                    InstrumentResponse::Scalar,
                    DeclaredInnerProducts::new(
                        ModelInnerProduct::HermitianEuclidean,
                        VisibilityInnerProduct::HermitianEuclidean,
                    ),
                ),
            ),
            ReconstructionContract::new(
                ReconstructionBasis::JointContinuumLine {
                    continuum_terms: 1,
                    line_terms: 1,
                },
                ReconstructionAlgorithm::JointContinuumLine {
                    scales_px: vec![0.0],
                    small_scale_bias: 0.0,
                },
                ReconstructionControls::new(2, 1.0, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            )
            .with_joint_continuum_line(JointContinuumLineContract::new(
                [0],
                [1],
                1.0e6,
            )),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            ProductRequirements::new(
                products.to_vec(),
                ProductNormalization::UnitResponse,
                RestoringBeamPolicy::Common,
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
            ModelBounds::new(4_096, 4_096, 4_096, 4_096, 1.0e30, 1.0e30)
                .expect("joint model bounds"),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .expect("compile joint problem")
}

fn samples(problem: &casa_imaging_model::CompiledProblem) -> Vec<SelectedObservationSample> {
    let source = &problem.selected_observation().read_set().sources()[0];
    [0_u64, 2]
        .into_iter()
        .enumerate()
        .map(|(row_index, physical_row)| {
            let frequency = if row_index == 0 { 1.05e9 } else { 1.15e9 };
            SelectedObservationSample {
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
                visibility: SelectedVisibilitySample::Complex32([1.0, row_index as f32]),
                prediction_target: SelectedPredictionTarget::NotRequested,
                channel_flag: false,
                parallel_hand_group_flag: false,
                row_flag: false,
                input_weight: 1.0 + row_index as f32,
                coordinates: SelectedSampleCoordinates {
                    raw_uvw_m: [1.0 + row_index as f64, 0.0, 0.0],
                    density_uvw_m: [1.0 + row_index as f64, 0.0, 0.0],
                    transformed_uvw_m: [1.0 + row_index as f64, 0.0, 0.0],
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
                    SelectedPhaseCentreProjection::new([1.0 + row_index as f64, 0.0, 0.0], 0.0)
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
            }
        })
        .collect()
}

fn contributions_for(
    problem: &casa_imaging_model::CompiledProblem,
    sample: &SelectedObservationSample,
) -> SelectedSpectralContributions {
    let output_channel = if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::JointContinuumLine { .. }
    ) {
        sample.address.channel_index
    } else {
        0
    };
    SelectedSpectralContributions::new([
        SelectedSpectralContribution::new(output_channel, 1.0, sample.address.frequency_centre_hz),
        None,
    ])
    .expect("continuum contribution")
}

fn selected_generation(
    problem: &casa_imaging_model::CompiledProblem,
    samples: &[SelectedObservationSample],
) -> SelectedObservationGenerationId {
    problem
        .inspect_selected_observation(samples.iter().cloned().map(Ok::<_, Infallible>), |_| {
            Ok::<_, Infallible>(())
        })
        .expect("inspect selected stream")
        .0
}

fn weighting_generation(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &WeightingPlan,
    samples: &[SelectedObservationSample],
) -> Result<WeightingAlgorithmState, WeightingError> {
    let mut density = begin_weighting_generation(problem, plan)?;
    for sample in samples {
        density.consume(
            problem,
            sample,
            sample.address.frequency_centre_hz,
            contributions_for(problem, sample),
        )?;
    }
    let mut sum_weight = density.finish(problem)?;
    for sample in samples {
        sum_weight.consume(
            problem,
            sample,
            sample.address.frequency_centre_hz,
            contributions_for(problem, sample),
        )?;
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
    let mut replay = generation
        .begin_replay(problem, plan)
        .expect("begin replay");
    for sample in samples {
        if let Some(block) = replay
            .consume(
                problem,
                sample,
                sample.address.frequency_centre_hz,
                contributions_for(problem, sample),
            )
            .expect("weight sample")
        {
            blocks.push(block);
        }
    }
    let (last, summary) = replay.finish().expect("finish replay");
    if let Some(block) = last {
        blocks.push(block);
    }
    (blocks, summary)
}

fn run_round(problem: &casa_imaging_model::CompiledProblem, seed: u8) -> MajorCycleCompletion {
    run_round_with_model(problem, seed, Some(0.75))
}

fn run_round_with_model(
    problem: &casa_imaging_model::CompiledProblem,
    seed: u8,
    model_value: Option<f64>,
) -> MajorCycleCompletion {
    let terms = model_value
        .into_iter()
        .map(|value| (0, value))
        .collect::<Vec<_>>();
    run_round_with_terms(problem, seed, &terms)
}

fn run_round_with_terms(
    problem: &casa_imaging_model::CompiledProblem,
    seed: u8,
    model_terms: &[(usize, f64)],
) -> MajorCycleCompletion {
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable problem"),
        attempt(seed),
        7,
    )
    .expect("model lifecycle");
    let empty = lifecycle.initial_empty().expect("empty model");
    let delta = (!model_terms.is_empty()).then(|| {
        lifecycle
            .compile_delta(
                &empty,
                model_terms.iter().map(|(coefficient, value)| {
                    casa_imaging_model::ModelDeltaTerm::new(
                        casa_imaging_model::ModelCell::new(0, *coefficient, 0, [4, 4]),
                        casa_imaging_model::ModelValue::new(*value).expect("model value"),
                    )
                }),
            )
            .expect("model delta")
    });
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, empty, delta).expect("preparation");
    let final_model_generation = preparation.final_model_generation();
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(1, 1).expect("weighting limits"),
    )
    .expect("weighting plan");
    let samples = samples(problem);
    let selected = selected_generation(problem, &samples);
    let generation = weighting_generation(problem, &plan, &samples).expect("weighting generation");
    let (blocks, summary) = replay(&generation, problem, &plan, &samples);
    let specification =
        SpectralOperatorSpecification::new(problem).expect("spectral specification");
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::InitialMajor,
    )
    .expect("operator workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepare operator");
    let mut state = prepared
        .begin(problem, &generation)
        .expect("begin complete-data owner");
    state
        .bind_major_cycle_model(preparation.final_model(), None)
        .expect("bind final model");
    for block in &blocks {
        state.consume_block(block).expect("consume block");
    }
    let evidence: CompleteDataOwnerResult = state
        .complete(&summary, selected, None)
        .expect("complete normal state");
    let mut owner =
        MajorCycleOwner::from_complete_data(evidence, preparation).expect("major-cycle owner");
    if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::JointContinuumLine { .. }
    ) {
        owner = owner
            .bind_reconstruction_masks(&ReconstructionMaskSet::Coupled(Box::new(
                joint_product_masks(problem, final_model_generation),
            )))
            .expect("bind joint final masks");
    }
    owner.reconcile(&mut lifecycle).expect("major-cycle join")
}

fn seal(
    problem: &casa_imaging_model::CompiledProblem,
    join: &MajorCycleCompletion,
) -> SealedContinuumGeneration {
    seal_with_controls(problem, join, ContinuumProductControls::default())
}

fn seal_with_controls(
    problem: &casa_imaging_model::CompiledProblem,
    join: &MajorCycleCompletion,
    controls: ContinuumProductControls,
) -> SealedContinuumGeneration {
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(problem, join).expect("T44 Taylor source catalog");
    let authority = ProductGenerationAuthority::bind(problem);
    let planned = authority
        .plan(&catalog, &controls)
        .expect("T44 Taylor plan");
    let inputs = ContinuumProductInputs::from_major_cycle(problem, join).expect("Taylor inputs");
    let produced = produce_continuum_members(&planned, &inputs).expect("T44 Taylor product family");
    authority
        .authorize(&planned, &produced)
        .expect("Taylor seal")
}

fn member<'a>(sealed: &'a SealedContinuumGeneration, name: &str) -> &'a SealedMember {
    sealed
        .members()
        .iter()
        .find(|member| member.name() == name)
        .unwrap_or_else(|| panic!("missing {name}"))
}

fn assert_close(actual: f32, expected: f32, context: &str) {
    let tolerance = 2.0e-5_f32.max(expected.abs() * 2.0e-5);
    assert!(
        (actual - expected).abs() <= tolerance,
        "{context}: {actual} != {expected}"
    );
}

#[test]
fn t46_joint_products_publish_one_lineage_without_component_residuals() {
    let problem = joint_problem(
        146,
        &[
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::Mask,
        ],
    );
    let join = run_round_with_terms(&problem, 146, &[(0, 1.0), (1, 2.0)]);
    let masks = joint_product_masks(&problem, join.final_model().generation_id());
    let catalog =
        ContinuumSourceCatalog::from_major_cycle_with_coupled_masks(&problem, &join, &masks)
            .expect("joint source catalog");
    assert!(
        join.normal_state()
            .channel_sum_weights()
            .iter()
            .all(|weight| weight.is_finite() && *weight > 0.0),
        "joint channel weights: {:?}; normal weights: {:?}",
        join.normal_state().channel_sum_weights(),
        join.normal_state().sum_weights()
    );
    let authority = ProductGenerationAuthority::bind(&problem);
    let planned = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("joint product plan");
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join)
        .expect("joint inputs")
        .with_coupled_reconstruction_masks(&masks)
        .expect("bind joint masks");
    let produced = produce_continuum_members(&planned, &inputs).expect("joint product family");
    let sealed = authority
        .authorize(&planned, &produced)
        .expect("joint product seal");

    assert!(
        member(&sealed, ".continuum.model.ct0")
            .payload()
            .contains(&1.0)
    );
    assert!(member(&sealed, ".line.model").payload().contains(&2.0));
    assert!(member(&sealed, ".total.model").payload().contains(&3.0));
    assert_eq!(
        member(&sealed, ".line.image").payload().len(),
        2 * SHAPE[0] * SHAPE[1]
    );
    assert_eq!(
        member(&sealed, ".total.image").payload().len(),
        2 * SHAPE[0] * SHAPE[1]
    );
    assert_eq!(sealed.restoring_beams().len(), 2);
    assert!(sealed.restoring_beams().iter().all(Option::is_some));
    assert!(
        member(&sealed, ".psf.joint0_1")
            .payload()
            .iter()
            .any(|value| *value != 0.0)
    );
    assert_ne!(
        member(&sealed, ".continuum.mask").payload(),
        member(&sealed, ".line.mask").payload(),
        "distinct coupled supports must remain distinct published members"
    );
    let mut expected_residual = (0..2)
        .flat_map(|channel| {
            let weight = join.normal_state().channel_sum_weights()[channel];
            join.normal_state()
                .joint_common_residual(channel)
                .expect("common residual")
                .iter()
                .map(move |value| (value.re / weight) as f32)
        })
        .collect::<Vec<_>>();
    let mut published_residual = member(&sealed, ".total.residual").payload().to_vec();
    expected_residual.sort_by(f32::total_cmp);
    published_residual.sort_by(f32::total_cmp);
    assert_eq!(published_residual.len(), expected_residual.len());
    for (actual, expected) in published_residual.into_iter().zip(expected_residual) {
        assert_close(actual, expected, "channel-normalized common residual");
    }
    assert_eq!(
        sealed
            .members()
            .iter()
            .filter(|member| member.name().contains("residual"))
            .map(SealedMember::name)
            .collect::<Vec<_>>(),
        [".total.residual"]
    );
    assert!(
        member(&sealed, ".continuum.mask")
            .payload()
            .iter()
            .all(|value| *value == 1.0)
    );
    assert_eq!(
        member(&sealed, ".line.mask")
            .payload()
            .iter()
            .filter(|value| **value == 1.0)
            .count(),
        8
    );
}

fn principal_residuals(join: &MajorCycleCompletion) -> [Vec<f32>; TERMS] {
    let normal = join.normal_state();
    let cells = SHAPE[0] * SHAPE[1];
    let moments = (0..3)
        .map(|term| normal.normal_moment(term).expect("Taylor normal moment"))
        .collect::<Vec<_>>();
    let peak = moments[0]
        .normal_approximation()
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.re.abs().total_cmp(&right.re.abs()))
        .expect("PSF peak")
        .0;
    let h00 = moments[0].normal_approximation()[peak].re;
    let h01 = moments[1].normal_approximation()[peak].re;
    let h11 = moments[2].normal_approximation()[peak].re;
    let determinant = h00 * h11 - h01 * h01;
    assert!(determinant.is_finite() && determinant.abs() > f64::EPSILON);
    let residual0 = normal.coefficient_term(0).expect("residual tt0").residual();
    let residual1 = normal.coefficient_term(1).expect("residual tt1").residual();
    let mut principal = [vec![0.0; cells], vec![0.0; cells]];
    for index in 0..cells {
        principal[0][index] =
            ((h11 * residual0[index].re - h01 * residual1[index].re) / determinant) as f32;
        principal[1][index] =
            ((h00 * residual1[index].re - h01 * residual0[index].re) / determinant) as f32;
    }
    principal
}

#[test]
fn t44_taylor_families_preserve_raw_state_and_share_one_restoring_beam() {
    let problem = taylor_problem(201, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let join = run_round(&problem, 202);
    let sealed = seal(&problem, &join);
    let names = sealed
        .members()
        .iter()
        .map(SealedMember::name)
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            ".psf.tt0",
            ".psf.tt1",
            ".psf.tt2",
            ".residual.tt0",
            ".residual.tt1",
            ".model.tt0",
            ".model.tt1",
            ".image.tt0",
            ".image.tt1",
            ".sumwt.tt0",
            ".sumwt.tt1",
            ".sumwt.tt2",
            ".alpha",
            ".alpha.error",
        ]
    );

    let normal = join.normal_state();
    let principal_weight = normal.normal_moment(0).expect("moment zero").sum_weight();
    for term in 0..3 {
        let psf = member(&sealed, &format!(".psf.tt{term}"));
        let sumwt = member(&sealed, &format!(".sumwt.tt{term}"));
        assert_eq!(psf.contract().unit(), ProductUnit::JyPerBeam);
        assert_eq!(sumwt.contract().unit(), ProductUnit::VisibilityWeight);
        let moment = normal.normal_moment(term).expect("normal moment");
        assert_eq!(sumwt.payload(), &[moment.sum_weight() as f32]);
        for (actual, raw) in psf.payload().iter().zip(moment.normal_approximation()) {
            assert_close(*actual, (raw.re / principal_weight) as f32, "Taylor PSF");
        }
    }
    for term in 0..TERMS {
        let raw = normal
            .coefficient_term(term)
            .expect("raw Taylor residual")
            .residual();
        let residual = member(&sealed, &format!(".residual.tt{term}"));
        let model = member(&sealed, &format!(".model.tt{term}"));
        let restored = member(&sealed, &format!(".image.tt{term}"));
        assert_eq!(residual.contract().unit(), ProductUnit::JyPerBeam);
        assert_eq!(model.contract().unit(), ProductUnit::JyPerPixel);
        assert_eq!(restored.contract().unit(), ProductUnit::JyPerBeam);
        for (actual, raw) in residual.payload().iter().zip(raw) {
            assert_close(
                *actual,
                (raw.re / principal_weight) as f32,
                "published raw Taylor residual",
            );
        }
        assert_eq!(
            restored.resolved_beam(),
            member(&sealed, ".image.tt0").resolved_beam(),
            "every Taylor image must use the same common beam"
        );
    }

    let principal = principal_residuals(&join);
    let beam = member(&sealed, ".image.tt0")
        .resolved_beam()
        .expect("common restoring beam");
    let kernel = gaussian_beam_image(SHAPE, beam, [1.0e-6, 1.0e-6]);
    for (term, principal_term) in principal.iter().enumerate().take(TERMS) {
        let model = member(&sealed, &format!(".model.tt{term}"));
        let restored = member(&sealed, &format!(".image.tt{term}"));
        let convolved = fft_convolve(
            model.payload(),
            kernel.as_slice().expect("contiguous kernel"),
            SHAPE,
        );
        for index in 0..convolved.len() {
            assert_close(
                restored.payload()[index],
                convolved[index] + principal_term[index],
                "principal-solution restoration",
            );
        }
    }
}

#[test]
fn t44_alpha_and_error_use_strict_principal_support_and_zero_false_blanking() {
    let problem = taylor_problem(203, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let join = run_round_with_model(&problem, 204, None);
    let principal = principal_residuals(&join);
    let sealed = seal(&problem, &join);
    let image0 = member(&sealed, ".image.tt0");
    let image1 = member(&sealed, ".image.tt1");
    let alpha = member(&sealed, ".alpha");
    let error = member(&sealed, ".alpha.error");
    assert_eq!(alpha.contract().unit(), ProductUnit::Dimensionless);
    assert_eq!(error.contract().unit(), ProductUnit::Dimensionless);
    let positive_max = principal[0]
        .iter()
        .copied()
        .filter(|value| *value > 0.0 && value.is_finite())
        .fold(f32::NEG_INFINITY, f32::max);
    assert!(positive_max.is_finite());
    let floor = 0.1 * positive_max;
    let mut supported = 0;
    for (index, image0_value) in image0.payload().iter().copied().enumerate() {
        let valid = image0_value > floor;
        assert_eq!(alpha.validity()[index], valid);
        assert_eq!(error.validity()[index], valid);
        if valid {
            supported += 1;
            let i0 = image0.payload()[index];
            let i1 = image1.payload()[index];
            let r0 = principal[0][index];
            let r1 = principal[1][index];
            assert_close(alpha.payload()[index], i1 / i0, "spectral index");
            let expected = ((i1 * r0 / i0.powi(2)).powi(2) + (r1 / i0).powi(2)).sqrt();
            assert_close(error.payload()[index], expected, "spectral-index error");
            assert!(error.payload()[index].is_finite());
        } else {
            assert_eq!(alpha.payload()[index], 0.0);
            assert_eq!(error.payload()[index], 0.0);
        }
    }
    assert!(supported > 0, "fixture must exercise Taylor support");

    let strict_problem =
        taylor_problem_with_fraction(207, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar, 1.0);
    let strict_join = run_round_with_model(&strict_problem, 208, None);
    let strict = seal(&strict_problem, &strict_join);
    for name in [".alpha", ".alpha.error"] {
        let product = member(&strict, name);
        assert!(product.validity().iter().all(|valid| !valid));
        assert!(product.payload().iter().all(|value| *value == 0.0));
    }
}

#[test]
fn t44_standard_pb_family_uses_pb_tt0_and_does_not_invent_weight_or_alpha_pbcor() {
    let problem = taylor_problem(205, &PB_PRODUCTS, InstrumentResponse::Scalar);
    let graph = problem.product_graph();
    let names = graph
        .nodes()
        .iter()
        .filter(|node| node.schema() == ProductSchema::ImageF32V1)
        .filter_map(|node| node.name())
        .collect::<Vec<_>>();
    assert!(names.contains(&".pb.tt0"));
    assert!(names.contains(&".pb.tt1"));
    assert!(names.contains(&".image.tt0.pbcor"));
    assert!(names.contains(&".image.tt1.pbcor"));
    assert!(!names.iter().any(|name| name.starts_with(".weight")));
    assert!(!names.contains(&".alpha.pbcor"));

    let pb0 = graph
        .node(ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
        .expect("Taylor-zero PB");
    assert_eq!(pb0.unit(), ProductUnit::Dimensionless);
    for term in 0..TERMS {
        let restored = graph
            .node(ProductRole::RestoredImage(ProductTerm::Taylor(term)))
            .expect("restored Taylor term");
        let corrected = graph
            .node(ProductRole::PbCorrectedImage(ProductTerm::Taylor(term)))
            .expect("PB-corrected Taylor term");
        assert_eq!(corrected.unit(), ProductUnit::JyPerBeam);
        assert_eq!(
            corrected.validity(),
            ProductValidityRule::PrimaryBeam(validity().primary_beam())
        );
        assert_eq!(
            corrected.dependencies(),
            [restored.node_id(), pb0.node_id()],
            "every Taylor image correction uses PB tt0"
        );
    }

    assert!(
        graph
            .nodes()
            .iter()
            .all(|node| !matches!(node.role(), ProductRole::Weight(ProductTerm::Taylor(_)))),
        "the frozen standard CASA row emits no standalone weight family"
    );

    let join = run_round(&problem, 206);
    let catalog = ContinuumSourceCatalog::from_major_cycle(&problem, &join).expect("PB catalog");
    let authority = ProductGenerationAuthority::bind(&problem);
    let unmodelled = authority
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("unmodelled PB plan");
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaEvlaCommon);
    let planned = authority
        .plan(&catalog, &controls)
        .expect("analytic PB plan");
    assert_ne!(planned.generation_id(), unmodelled.generation_id());
    assert_eq!(
        planned.primary_beam_model(),
        Some(AnalyticPrimaryBeamModel::CasaEvlaCommon)
    );
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join).expect("PB inputs");
    assert_eq!(
        produce_continuum_members(&unmodelled, &inputs).expect_err("requested PB needs a model"),
        ProductsError::UnsupportedProblem
    );
    let sealed = seal_with_controls(&problem, &join, controls);
    let pb0 = member(&sealed, ".pb.tt0");
    let pb1 = member(&sealed, ".pb.tt1");
    assert_eq!(pb0.payload()[4 * SHAPE[1] + 4], 1.0);
    assert!(pb1.payload().iter().all(|value| *value == 0.0));
    for term in 0..TERMS {
        let restored = member(&sealed, &format!(".image.tt{term}"));
        let corrected = member(&sealed, &format!(".image.tt{term}.pbcor"));
        for index in 0..pb0.payload().len() {
            let valid = pb0.payload()[index] > 0.2;
            assert_eq!(corrected.validity()[index], valid);
            let expected = if valid {
                restored.payload()[index] / pb0.payload()[index]
            } else {
                0.0
            };
            assert_close(corrected.payload()[index], expected, "PB correction");
        }
    }
}

#[test]
fn t47_mosaic_taylor_products_publish_weight_and_pb_corrected_alpha() {
    let products = [
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
        ProductKind::Weight,
        ProductKind::Sensitivity,
        ProductKind::PrimaryBeam,
        ProductKind::PbCorrectedImage,
        ProductKind::TaylorTerms,
        ProductKind::SpectralIndex,
        ProductKind::SpectralIndexError,
        ProductKind::PbCorrectedSpectralIndex,
        ProductKind::Beam,
    ];
    let problem = taylor_problem(211, &products, InstrumentResponse::Scalar);
    let join = run_round(&problem, 212);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::MosaicSensitivity);
    let sealed = seal_with_controls(&problem, &join, controls);
    let weight0 = member(&sealed, ".weight.tt0");
    let weight1 = member(&sealed, ".weight.tt1");
    let sensitivity = member(&sealed, ".sensitivity");
    let alpha = member(&sealed, ".alpha");
    let alpha_pbcor = member(&sealed, ".alpha.pbcor");

    assert!(weight0.payload().iter().any(|value| *value > 0.0));
    let principal_sum_weight = join
        .normal_state()
        .normal_moment(0)
        .expect("principal normal moment")
        .sum_weight() as f32;
    let raw_sensitivity = join
        .normal_state()
        .normal_moment(0)
        .expect("principal normal moment")
        .sensitivity();
    for (index, raw) in raw_sensitivity.iter().copied().enumerate() {
        assert_eq!(sensitivity.payload()[index], raw as f32);
        assert_close(
            weight0.payload()[index],
            sensitivity.payload()[index] / principal_sum_weight,
            "normalized principal mosaic weight",
        );
    }
    assert_ne!(
        weight0.payload(),
        sensitivity.payload(),
        "normalized Weight must not alias raw Sensitivity"
    );
    let raw_weight1 = join
        .normal_state()
        .normal_moment(1)
        .expect("first signed normal moment")
        .sensitivity();
    assert_eq!(
        weight1.payload(),
        raw_weight1
            .iter()
            .map(|value| *value as f32)
            .collect::<Vec<_>>(),
        "higher Taylor weights retain CASA's raw signed sensitivity moments"
    );
    for index in 0..alpha_pbcor.payload().len() {
        let expected = if alpha_pbcor.validity()[index] {
            // This scalar-response fixture has no PB spectral slope. The
            // non-zero spectral-slope law is covered by the product-owner unit
            // test rather than by cancelling two tt0-PB-corrected images.
            alpha.payload()[index]
        } else {
            0.0
        };
        assert_close(
            alpha_pbcor.payload()[index],
            expected,
            "PB-corrected spectral index",
        );
    }
}

#[test]
fn taylor_generation_demand_charges_retained_families_and_algorithm_scratch() {
    let problem = taylor_problem(209, &TAYLOR_PRODUCTS, InstrumentResponse::Scalar);
    let join = run_round(&problem, 210);
    let catalog =
        ContinuumSourceCatalog::from_major_cycle(&problem, &join).expect("Taylor catalog");
    let planned = ProductGenerationAuthority::bind(&problem)
        .plan(&catalog, &ContinuumProductControls::default())
        .expect("Taylor plan");
    let inputs = ContinuumProductInputs::from_major_cycle(&problem, &join).expect("Taylor inputs");
    let demand = planned.demand(&inputs).expect("Taylor demand");
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
        .expect("Taylor members");
    assert_eq!(demand.produced_residency_bytes(), values * 5);
    assert_eq!(demand.sealed_residency_bytes(), values * 5);
    assert_eq!(demand.maximum_member_payload_bytes(), maximum * 4);
    assert_eq!(demand.maximum_member_validity_bytes(), maximum);
    assert_eq!(
        demand.algorithm_scratch_bytes(),
        9_100,
        "8x8, two-term Taylor owner retains exact families, solve and rustfft buffers"
    );
    assert_eq!(
        demand.peak_residency_bytes(),
        (values * 10).max(values * 5 + 9_100)
    );
}
