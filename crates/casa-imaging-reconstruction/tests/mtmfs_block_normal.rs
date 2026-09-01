// SPDX-License-Identifier: LGPL-3.0-or-later

//! T42 public-owner acceptance for MT-MFS block-normal construction.

use std::{convert::Infallible, thread};

use casa_imaging_model::{
    AntennaSelection, AxisOrder, CentreLaws, ColumnGeneration, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FlagPolicy, FrequencyFrame,
    GeometryInput, IdSelection, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ImagingRequest, InstrumentResponse, IntentSelection, JointContinuumLineContract,
    LogicalIdentity, MeasurementEquationContract, MeasurementSetIdentity, MetadataGeneration,
    MetadataTableKind, ModelBounds, ModelCell, ModelColumnState, ModelColumnWrite, ModelDeltaTerm,
    ModelExecutionAttemptId, ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements,
    ModelStateIdentity, ModelValue, MsColumnKind, NumericPrecision, NumericalStage,
    NumericsContract, ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection,
    ScientificContract, SelectedColumns, SelectedImageDomainProjections, SelectedMainRow,
    SelectedObservationGenerationId, SelectedObservationSample, SelectedPhaseCentreProjection,
    SelectedPredictionTarget, SelectedRows, SelectedSampleAddress, SelectedSampleCoordinates,
    SelectedSampleMetadata, SelectedSpectralContributions, SelectedSpectralEvaluation,
    SelectedSpectralInterval, SelectedVisibilitySample, SkyDirection, SourceGenerations,
    SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeScale, TimeSelection, UvSelection,
    UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope,
    WeightingContract, WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ChannelCyclePolicy, CoupledReconstructionMask, ExecutableModelProblem, FinalNormalState,
    MajorCycleOwner, MajorCyclePreparation, MinorCycleError, MinorCycleProgram, ModelGeneration,
    ModelLifecycle, NormalStateCatalog, ReconstructionCycle, ReconstructionCycleError,
    ReconstructionMask, ReconstructionMaskPlan, SpectralChannelValidity,
    SpectralOperatorSpecification, SpectralPrimitiveCatalog, SpectralStencilValidity,
    WeightingAlgorithmState, WeightingExecutionLimits, WeightingPlan, WeightingReplayChunk,
    WeightingReplaySummary, begin_weighting_generation, compile_spectral_stencil, plan_weighting,
    runtime_adapter::{
        CompleteDataOwnerResult, GRIDDED_NORMAL_PARTITION_COUNT, GriddedNormalExecutionResidency,
        GriddedNormalOperatorBlock, GriddedNormalOperatorCompiler, GriddedNormalOperatorProgram,
        GriddedNormalRoutingMeasurements, SourceCardinalityObservation, SpectralOperatorPass,
        gridded_normal_execution_residency, gridded_normal_operator_record_bytes,
        gridded_normal_route_capacity_bytes, prepare_spectral_operator, spectral_operator_workload,
    },
};

const REFERENCE_FREQUENCY_HZ: f64 = 1.0e9;
const IMAGE_WIDTH: usize = 8;

fn identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut bytes = [seed; 32];
    bytes[0] = scope;
    LogicalIdentity::from_sha256(bytes)
}

fn source_with_selection(selection: ObservationSelection) -> ObservationSourceInput {
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
    .map(|(index, kind)| ColumnGeneration::new(kind, identity(42, 20 + index as u8)))
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
    .map(|(index, kind)| MetadataGeneration::new(kind, identity(42, 60 + index as u8)))
    .collect();
    ObservationSourceInput::new(
        MeasurementSetIdentity::new(identity(42, 1)),
        ObservationSourceProvenance::new("fixture://t42/multi-spw".to_owned(), identity(42, 2)),
        selection,
        SourceGenerations::new(
            ConsistencyToken::new(identity(42, 3)),
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

fn row_selection() -> RowSelection {
    RowSelection::new(
        IdSelection::All,
        TimeSelection::All,
        UvSelection::All,
        AntennaSelection::All,
        IdSelection::All,
        IdSelection::All,
        IntentSelection::All,
        IdSelection::All,
    )
}

fn correlations() -> Vec<CorrelationSelection> {
    vec![CorrelationSelection::new(
        0,
        vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
    )]
}

fn source() -> ObservationSourceInput {
    source_with_selection(ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(
            2,
            [SelectedMainRow::new(0, 0), SelectedMainRow::new(1, 1)],
        )
        .expect("two selected rows"),
        row_selection(),
        vec![
            DataDescriptionSelection::new(0, 0, 0),
            DataDescriptionSelection::new(1, 1, 0),
        ],
        vec![
            SpectralWindowSelection::new(0, vec![0]),
            SpectralWindowSelection::new(1, vec![0]),
        ],
        correlations(),
    ))
}

fn joint_source() -> ObservationSourceInput {
    source_with_selection(ObservationSelection::new(
        SelectedRows::from_ordered_main_rows(1, [SelectedMainRow::new(0, 0)])
            .expect("one selected row"),
        row_selection(),
        vec![DataDescriptionSelection::new(0, 0, 0)],
        vec![SpectralWindowSelection::new(0, vec![0, 1])],
        correlations(),
    ))
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

fn problem_with(
    reconstruction: ReconstructionContract,
    spectral_channels: usize,
) -> casa_imaging_model::CompiledProblem {
    let is_joint = matches!(
        reconstruction.basis(),
        ReconstructionBasis::JointContinuumLine { .. }
    );
    let mut product_kinds = vec![
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::SumWeights,
        ProductKind::Sensitivity,
    ];
    if is_joint {
        product_kinds.retain(|product| *product != ProductKind::Sensitivity);
    }
    if matches!(reconstruction.basis(), ReconstructionBasis::Taylor { .. }) {
        product_kinds.push(ProductKind::TaylorTerms);
    }
    let centre = IMAGE_WIDTH as f64 / 2.0;
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
            ImageShape::new(IMAGE_WIDTH, IMAGE_WIDTH),
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
                channels: spectral_channels,
                reference_pixel: 0.0,
                reference_frequency_hz: REFERENCE_FREQUENCY_HZ,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![if is_joint { joint_source() } else { source() }],
        Vec::new(),
        ModelStateIdentity::Empty,
    ))
    .expect("compile T42 observation");
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
            reconstruction,
            WeightingContract::new(
                WeightingScheme::Briggs { robust: 0.5 },
                WeightDensityScope::GlobalSelection,
            ),
            ProductRequirements::new(
                product_kinds,
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
            ModelBounds::new(1_024, 1_024, 1_024, 1_024, 1.0e30, 1.0e30).expect("T42 model bounds"),
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))
    .expect("compile T42 MT-MFS problem")
}

fn problem() -> casa_imaging_model::CompiledProblem {
    problem_with(
        ReconstructionContract::new(
            ReconstructionBasis::Taylor { terms: 2 },
            ReconstructionAlgorithm::Mtmfs {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            ReconstructionControls::new(1, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        1,
    )
}

fn joint_problem() -> casa_imaging_model::CompiledProblem {
    joint_problem_with_condition(1.0e6)
}

fn joint_problem_with_condition(
    maximum_condition_number: f64,
) -> casa_imaging_model::CompiledProblem {
    problem_with(
        ReconstructionContract::new(
            ReconstructionBasis::JointContinuumLine {
                continuum_terms: 1,
                line_terms: 1,
            },
            ReconstructionAlgorithm::JointContinuumLine {
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            ReconstructionControls::new(16, 1.0, 1.0e-9),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        )
        .with_joint_continuum_line(JointContinuumLineContract::new(
            [0],
            [1],
            maximum_condition_number,
        )),
        2,
    )
}

fn samples(problem: &casa_imaging_model::CompiledProblem) -> [SelectedObservationSample; 2] {
    let measurement_set = problem.selected_observation().read_set().sources()[0].measurement_set();
    [
        sample(measurement_set, 0, 0, 0, 0.8e9, 2.0, [1.0, 0.25]),
        sample(measurement_set, 1, 1, 0, 1.2e9, 1.0, [0.5, -0.75]),
    ]
}

fn joint_samples(problem: &casa_imaging_model::CompiledProblem) -> [SelectedObservationSample; 2] {
    let measurement_set = problem.selected_observation().read_set().sources()[0].measurement_set();
    [
        sample(measurement_set, 0, 0, 0, 1.0e9, 2.0, [1.0, 0.0]),
        sample(measurement_set, 0, 0, 1, 1.001e9, 2.0, [3.0, 0.0]),
    ]
}

fn sample(
    measurement_set: MeasurementSetIdentity,
    physical_row: u64,
    data_description_id: i32,
    channel_index: u32,
    frequency_hz: f64,
    input_weight: f32,
    visibility: [f32; 2],
) -> SelectedObservationSample {
    SelectedObservationSample {
        address: SelectedSampleAddress {
            measurement_set,
            physical_row,
            data_description_id,
            spectral_window_id: u32::try_from(data_description_id)
                .expect("non-negative fixture DDID"),
            channel_index,
            frequency_centre_hz: frequency_hz,
            frequency_lower_hz: frequency_hz - 1.0e6,
            frequency_upper_hz: frequency_hz + 1.0e6,
            channel_width_hz: 2.0e6,
            frequency_frame: FrequencyFrame::Topocentric,
            polarization_id: 0,
            correlation_index: 0,
            correlation_type: CorrelationType::StokesI,
        },
        visibility: SelectedVisibilitySample::Complex32(visibility),
        prediction_target: SelectedPredictionTarget::NotRequested,
        channel_flag: false,
        parallel_hand_group_flag: false,
        row_flag: false,
        input_weight,
        coordinates: SelectedSampleCoordinates {
            raw_uvw_m: [1.0, 1.0, 0.0],
            density_uvw_m: [1.0, 1.0, 0.0],
            transformed_uvw_m: [1.0, 1.0, 0.0],
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
            SelectedPhaseCentreProjection::new([1.0, 1.0, 0.0], 0.0)
                .expect("finite one-domain projection"),
        ),
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
    }
}

fn stencil(
    problem: &casa_imaging_model::CompiledProblem,
    sample: &SelectedObservationSample,
) -> SelectedSpectralContributions {
    let interval = SelectedSpectralInterval::new(
        sample.address.frequency_centre_hz,
        sample.address.frequency_lower_hz,
        sample.address.frequency_upper_hz,
    )
    .expect("finite fixture interval");
    let evaluation =
        SelectedSpectralEvaluation::new(interval, interval, f64::from(sample.input_weight), true)
            .expect("valid fixture evaluation");
    let receipt = compile_spectral_stencil(problem, sample, evaluation)
        .expect("compile reconstruction-owned Taylor source stencil");
    assert_eq!(receipt.validity(), SpectralStencilValidity::Mapped);
    let terms = receipt.contributions().iter().collect::<Vec<_>>();
    assert_eq!(terms.len(), 1, "basis algebra remains inside the operator");
    if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::Taylor { .. }
    ) {
        assert_eq!(terms[0].output_channel(), 0);
    }
    assert_eq!(terms[0].factor(), 1.0);
    assert_eq!(
        terms[0].evaluation_frequency_hz().to_bits(),
        sample.address.frequency_centre_hz.to_bits()
    );
    receipt.contributions().clone()
}

fn run_operator(
    problem: &casa_imaging_model::CompiledProblem,
    samples: &[SelectedObservationSample],
    block_samples: usize,
    density_partitions: usize,
    preparation: Option<&MajorCyclePreparation>,
) -> (CompleteDataOwnerResult, [f64; 3]) {
    let limits = WeightingExecutionLimits::new(block_samples, density_partitions)
        .expect("T42 execution limits");
    let plan = plan_weighting(problem, limits).expect("T42 weighting plan");
    let stencils = samples
        .iter()
        .map(|sample| stencil(problem, sample))
        .collect::<Vec<_>>();

    let mut density = begin_weighting_generation(problem, &plan).expect("begin density pass");
    for (sample, stencil) in samples.iter().zip(&stencils) {
        density
            .consume(problem, sample, stencil.clone())
            .expect("consume density sample");
    }
    let mut sum_weight = density.finish(problem).expect("finish density pass");
    for (sample, stencil) in samples.iter().zip(&stencils) {
        sum_weight
            .consume(problem, sample, stencil.clone())
            .expect("consume sum-weight sample");
    }
    let generation = sum_weight.finish().expect("freeze global weighting");

    let mut blocks = Vec::new();
    let mut replay = generation
        .begin_replay(problem, &plan)
        .expect("begin weighted replay");
    for (sample, stencil) in samples.iter().zip(&stencils) {
        if let Some(block) = replay
            .consume(problem, sample, stencil.clone())
            .expect("weight replay sample")
        {
            blocks.push(block);
        }
    }
    let (tail, summary) = replay.finish().expect("finish weighted replay");
    if let Some(block) = tail {
        blocks.push(block);
    }

    let mut expected = [0.0; 3];
    let mut compensation = [0.0; 3];
    for weighted in blocks.iter().flat_map(|block| block.samples()) {
        for spectral in weighted.spectral_values() {
            for moment in 0..3 {
                compensated_add(
                    &mut expected[moment],
                    &mut compensation[moment],
                    casa_moment_weight(
                        spectral.contribution().evaluation_frequency_hz(),
                        spectral.imaging_weight(),
                        moment,
                    ),
                );
            }
        }
    }

    let (selected_generation, selected_count) = problem
        .inspect_selected_observation(samples.iter().cloned().map(Ok::<_, Infallible>), |_| {
            Ok::<_, Infallible>(())
        })
        .expect("inspect exact T42 selected stream");
    assert_eq!(selected_count, samples.len() as u64);
    let specification = SpectralOperatorSpecification::new(problem).expect("Taylor operator");
    let workload = spectral_operator_workload(
        &specification,
        block_samples,
        SpectralOperatorPass::InitialMajor,
    )
    .expect("Taylor workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepare Taylor FFT");
    let mut owner = prepared
        .begin(problem, &generation)
        .expect("begin Taylor complete-data owner");
    if let Some(preparation) = preparation {
        owner
            .bind_major_cycle_model(preparation.final_model(), None)
            .expect("bind exact empty Taylor model");
    }
    for block in &blocks {
        assert!(
            owner
                .consume_block(block)
                .expect("consume Taylor block")
                .is_empty()
        );
    }
    let result = owner
        .complete(&summary, selected_generation, None)
        .expect("complete Taylor normal state");
    (result, expected)
}

fn compensated_add(sum: &mut f64, compensation: &mut f64, value: f64) {
    let corrected = value - *compensation;
    let updated = *sum + corrected;
    *compensation = (updated - *sum) - corrected;
    *sum = updated;
}

fn casa_moment_weight(frequency_hz: f64, imaging_weight: f64, order: usize) -> f64 {
    let frequency_hz = f64::from(frequency_hz as f32);
    let normalized = ((frequency_hz - REFERENCE_FREQUENCY_HZ) / REFERENCE_FREQUENCY_HZ) as f32;
    let imaging_weight = imaging_weight as f32;
    f64::from(
        (f64::from(imaging_weight)
            * f64::from(normalized).powi(i32::try_from(order).expect("bounded T42 order")))
            as f32,
    )
}

fn run_final_normal_state(
    problem: &casa_imaging_model::CompiledProblem,
    samples: &[SelectedObservationSample],
) -> (casa_imaging_reconstruction::FinalNormalState, [f64; 3]) {
    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable Taylor problem");
    let mut lifecycle = ModelLifecycle::bind(
        executable,
        ModelExecutionAttemptId::new(identity(42, 120)),
        1,
    )
    .expect("bind Taylor model lifecycle");
    let initial = lifecycle.initial_empty().expect("empty Taylor model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial, None).expect("prepare Taylor model");
    let (complete_data, expected) = run_operator(problem, samples, 1, 1, Some(&preparation));
    let completion = MajorCycleOwner::from_complete_data(complete_data, preparation)
        .expect("join Taylor complete-data evidence")
        .reconcile(&mut lifecycle)
        .expect("reconcile Taylor normal state");
    let (normal, model_completion, final_model) = completion.into_parts();
    assert_eq!(
        normal.input_model_generation(),
        model_completion.base(),
        "normal and model completions bind the same input generation"
    );
    assert_eq!(
        normal.final_model_generation(),
        final_model.generation_id(),
        "normal state binds the exact authoritative final model"
    );
    (normal, expected)
}

fn run_joint_final_normal_state(
    problem: &casa_imaging_model::CompiledProblem,
    selected: &[SelectedObservationSample],
) -> (ModelLifecycle, FinalNormalState, ModelGeneration) {
    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable joint problem");
    let mut lifecycle = ModelLifecycle::bind(
        executable,
        ModelExecutionAttemptId::new(identity(46, 120)),
        1,
    )
    .expect("bind joint model lifecycle");
    let initial = lifecycle.initial_empty().expect("empty joint model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial, None).expect("prepare joint model");
    let (complete_data, _) = run_operator(problem, selected, 1, 1, Some(&preparation));
    let completion = MajorCycleOwner::from_complete_data(complete_data, preparation)
        .expect("join joint complete-data evidence")
        .reconcile(&mut lifecycle)
        .expect("reconcile joint normal state");
    let (normal, _, model) = completion.into_parts();
    (lifecycle, normal, model)
}

fn full_joint_masks(
    normal: &FinalNormalState,
    model: &ModelGeneration,
) -> CoupledReconstructionMask {
    let centre = normal.shape()[0] as f64 / 2.0;
    let coordinate = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [centre, centre],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let mask = || {
        ReconstructionMask::full_plane(
            normal.problem_id(),
            model.generation_id(),
            coordinate,
            normal.shape(),
        )
        .expect("full joint mask")
    };
    CoupledReconstructionMask::new(mask(), mask()).expect("same-lineage joint masks")
}

struct FrozenTaylorReplay {
    weighting: WeightingAlgorithmState,
    plan: WeightingPlan,
    blocks: Vec<WeightingReplayChunk>,
    summary: WeightingReplaySummary,
    selected_generation: SelectedObservationGenerationId,
}

fn freeze_taylor_replay(
    problem: &casa_imaging_model::CompiledProblem,
    samples: &[SelectedObservationSample],
) -> FrozenTaylorReplay {
    let plan = plan_weighting(
        problem,
        WeightingExecutionLimits::new(samples.len(), 2).expect("T42 compact execution limits"),
    )
    .expect("T42 compact weighting plan");
    let stencils = samples
        .iter()
        .map(|sample| stencil(problem, sample))
        .collect::<Vec<_>>();
    let mut density = begin_weighting_generation(problem, &plan).expect("begin density pass");
    for (sample, stencil) in samples.iter().zip(&stencils) {
        density
            .consume(problem, sample, stencil.clone())
            .expect("consume density sample");
    }
    let mut sum_weight = density.finish(problem).expect("finish density pass");
    for (sample, stencil) in samples.iter().zip(&stencils) {
        sum_weight
            .consume(problem, sample, stencil.clone())
            .expect("consume sum-weight sample");
    }
    let weighting = sum_weight.finish().expect("freeze global weighting");
    let mut replay = weighting
        .begin_replay(problem, &plan)
        .expect("begin frozen compact replay");
    let mut blocks = Vec::new();
    for (sample, stencil) in samples.iter().zip(&stencils) {
        if let Some(block) = replay
            .consume(problem, sample, stencil.clone())
            .expect("consume frozen compact replay")
        {
            blocks.push(block);
        }
    }
    let (tail, summary) = replay.finish().expect("finish frozen compact replay");
    if let Some(block) = tail {
        blocks.push(block);
    }
    let (selected_generation, selected_count) = problem
        .inspect_selected_observation(samples.iter().cloned().map(Ok::<_, Infallible>), |_| {
            Ok::<_, Infallible>(())
        })
        .expect("inspect exact compact selected stream");
    assert_eq!(selected_count, samples.len() as u64);
    FrozenTaylorReplay {
        weighting,
        plan,
        blocks,
        summary,
        selected_generation,
    }
}

fn complete_frozen_taylor_operator(
    problem: &casa_imaging_model::CompiledProblem,
    frozen: &FrozenTaylorReplay,
    preparation: &MajorCyclePreparation,
    pass: SpectralOperatorPass,
    prior: Option<FinalNormalState>,
) -> CompleteDataOwnerResult {
    let specification = SpectralOperatorSpecification::new(problem).expect("Taylor operator");
    let workload = spectral_operator_workload(
        &specification,
        frozen.plan.limits().max_block_samples(),
        pass,
    )
    .expect("Taylor workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepare Taylor FFT");
    let mut owner = prepared
        .begin(problem, &frozen.weighting)
        .expect("begin frozen Taylor owner");
    owner
        .bind_major_cycle_model(preparation.final_model(), prior)
        .expect("bind frozen Taylor model");
    for block in &frozen.blocks {
        assert!(
            owner
                .consume_block(block)
                .expect("consume frozen Taylor block")
                .is_empty()
        );
    }
    owner
        .complete(&frozen.summary, frozen.selected_generation, None)
        .expect("complete frozen Taylor normal state")
}

fn initial_normal_from_frozen(
    problem: &casa_imaging_model::CompiledProblem,
    frozen: &FrozenTaylorReplay,
) -> FinalNormalState {
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable Taylor problem"),
        ModelExecutionAttemptId::new(identity(42, 121)),
        1,
    )
    .expect("bind compact initial lifecycle");
    let initial = lifecycle.initial_empty().expect("empty compact model");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial, None).expect("prepare compact model");
    let complete = complete_frozen_taylor_operator(
        problem,
        frozen,
        &preparation,
        SpectralOperatorPass::InitialMajor,
        None,
    );
    MajorCycleOwner::from_complete_data(complete, preparation)
        .expect("join compact initial evidence")
        .reconcile(&mut lifecycle)
        .expect("reconcile compact initial state")
        .into_parts()
        .0
}

fn nonzero_taylor_model(problem: &casa_imaging_model::CompiledProblem) -> MajorCyclePreparation {
    let lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("executable Taylor problem"),
        ModelExecutionAttemptId::new(identity(42, 122)),
        2,
    )
    .expect("bind compact residual lifecycle");
    let initial = lifecycle
        .initial_empty()
        .expect("empty compact source model");
    let delta = lifecycle
        .compile_delta(
            &initial,
            [
                ModelDeltaTerm::new(
                    ModelCell::new(0, 0, 0, [3, 3]),
                    ModelValue::new(0.5).expect("finite Taylor-zero model"),
                ),
                ModelDeltaTerm::new(
                    ModelCell::new(0, 1, 0, [4, 3]),
                    ModelValue::new(-0.25).expect("finite Taylor-one model"),
                ),
            ],
        )
        .expect("compile genuinely two-term model");
    MajorCyclePreparation::prepare(&lifecycle, initial, Some(delta))
        .expect("prepare genuinely two-term model")
}

fn compile_compact_program(
    problem: &casa_imaging_model::CompiledProblem,
    frozen: &FrozenTaylorReplay,
) -> (
    GriddedNormalOperatorProgram,
    Vec<GriddedNormalOperatorBlock>,
) {
    let mut compiler =
        GriddedNormalOperatorCompiler::new(problem, SourceCardinalityObservation::Enabled)
            .expect("begin Taylor compact compiler");
    let blocks =
        frozen
            .blocks
            .iter()
            .map(|block| {
                let (compiled, timings) = compiler
                    .compile_block_observed(block)
                    .expect("compile observed Taylor compact block");
                let measurements = compiled.measurements();
                assert_eq!(
                measurements.source_cardinality,
                Some(casa_imaging_reconstruction::runtime_adapter::GriddedNormalSourceCardinality {
                    groups: block.samples().len() as u64,
                    records: block.samples().len() as u64,
                })
            );
                assert_eq!(
                    measurements.encoded_buffer_bytes,
                    compiled.record_count() * 32
                );
                assert_eq!(measurements.encoded_buffer_allocations, 1);
                let _exclusive_stage_total = timings.record_key_construction
                    + timings.grouping_reduction
                    + timings.encoding_checksum
                    + timings.completion;
                compiled
            })
            .collect::<Vec<_>>();
    let program = compiler
        .complete(&frozen.summary, frozen.selected_generation, None)
        .expect("seal v3 Taylor compact program");
    (program, blocks)
}

struct CompactTaylorResult {
    residual: Vec<num_complex::Complex64>,
    common_residual: Option<Vec<num_complex::Complex64>>,
    routing: GriddedNormalRoutingMeasurements,
    grid_residency: GriddedNormalExecutionResidency,
}

fn execute_compact_taylor(
    problem: &casa_imaging_model::CompiledProblem,
    frozen: &FrozenTaylorReplay,
    program: &GriddedNormalOperatorProgram,
    blocks: &[GriddedNormalOperatorBlock],
    preparation: &MajorCyclePreparation,
    prior: FinalNormalState,
    workers: usize,
) -> CompactTaylorResult {
    let specification = SpectralOperatorSpecification::new(problem).expect("Taylor operator");
    let grid_residency = gridded_normal_execution_residency(
        specification.grid_shape(),
        program.accumulation_width(),
    )
    .expect("exact Taylor compact grid residency");
    let workload = spectral_operator_workload(
        &specification,
        frozen.plan.limits().max_block_samples(),
        SpectralOperatorPass::ResidualRefresh,
    )
    .expect("Taylor residual workload");
    let prepared = prepare_spectral_operator(specification, workload).expect("prepare Taylor FFT");
    let capacities = blocks
        .iter()
        .map(|block| usize::try_from(block.record_count()).expect("record capacity fits usize"))
        .collect::<Vec<_>>();
    let mut apply = program
        .begin_apply_with_route_capacities(
            problem,
            preparation.final_model(),
            prior,
            prepared,
            &capacities,
        )
        .expect("begin compact Taylor apply");
    assert_eq!(
        apply
            .two_domain_window_partition_count(
                blocks
                    .iter()
                    .map(|block| (block.sequence(), block.encoded_bytes())),
            )
            .expect("prepare the complete compact window"),
        GRIDDED_NORMAL_PARTITION_COUNT
    );

    for phase in 0..2 {
        let first = phase * (GRIDDED_NORMAL_PARTITION_COUNT / 2);
        let works = (first..first + GRIDDED_NORMAL_PARTITION_COUNT / 2)
            .map(|ordinal| {
                (
                    ordinal,
                    apply
                        .two_domain_window_partition(0, blocks.len(), ordinal)
                        .expect("derive stable compact logical work"),
                )
            })
            .collect::<Vec<_>>();
        let mut partials = thread::scope(|scope| {
            let handles = (0..workers)
                .map(|worker| {
                    let assigned = works
                        .iter()
                        .copied()
                        .filter(|(ordinal, _)| ordinal % workers == worker)
                        .collect::<Vec<_>>();
                    let apply = &apply;
                    scope.spawn(move || {
                        assigned
                            .into_iter()
                            .map(|(ordinal, work)| {
                                (
                                    ordinal,
                                    apply
                                        .execute_two_domain_window(
                                            |frame| {
                                                blocks.get(frame).map(|block| {
                                                    (block.sequence(), block.encoded_bytes())
                                                })
                                            },
                                            work,
                                        )
                                        .expect("execute compact Taylor logical work"),
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().expect("compact Taylor worker joined"))
                .collect::<Vec<_>>()
        });
        partials.sort_by_key(|(ordinal, _)| *ordinal);
        for (_, partial) in partials {
            apply
                .commit_two_domain(partial)
                .expect("deterministic compact Taylor commit");
        }
    }

    let (complete, routing) = apply
        .finish_with_routing_measurements()
        .expect("finish compact Taylor apply");
    CompactTaylorResult {
        residual: complete.primitives().dirty().to_vec(),
        common_residual: complete.primitives().common_residual().map(<[_]>::to_vec),
        routing,
        grid_residency,
    }
}

fn complex_nrms(actual: &[num_complex::Complex64], expected: &[num_complex::Complex64]) -> f64 {
    assert_eq!(actual.len(), expected.len());
    let error = actual
        .iter()
        .zip(expected)
        .map(|(actual, expected)| (*actual - *expected).norm_sqr())
        .sum::<f64>();
    let reference = expected.iter().map(|value| value.norm_sqr()).sum::<f64>();
    (error / reference).sqrt()
}

#[test]
fn t42_multi_spw_block_normal_is_global_signed_and_partition_deterministic() {
    let problem = problem();
    let samples = samples(&problem);
    let (single_sample_blocks, expected) = run_operator(&problem, &samples, 1, 1, None);
    let (single_full_block, full_expected) =
        run_operator(&problem, &samples, samples.len(), 2, None);
    assert_eq!(expected, full_expected);

    for result in [&single_sample_blocks, &single_full_block] {
        assert_eq!(
            result.completion().primitive_catalog(),
            SpectralPrimitiveCatalog::UnnormalizedTaylorBlockV1
        );
        let primitives = result.primitives();
        assert_eq!(primitives.coefficient_term_count(), 2);
        assert_eq!(primitives.normal_moment_count(), 3);
        assert_eq!(
            primitives.reference_frequency_hz().map(f64::to_bits),
            Some(REFERENCE_FREQUENCY_HZ.to_bits())
        );
        assert_eq!(primitives.normal_moment_index(0, 0), Some(0));
        assert_eq!(primitives.normal_moment_index(0, 1), Some(1));
        assert_eq!(primitives.normal_moment_index(1, 0), Some(1));
        assert_eq!(primitives.normal_moment_index(1, 1), Some(2));
        assert_eq!(primitives.sum_weights(), expected);
        assert!(
            primitives.sum_weights()[1] < 0.0,
            "the asymmetric low-frequency weight must retain a signed odd moment"
        );
        assert_eq!(
            primitives.channel_validity(),
            &[SpectralChannelValidity::Valid],
            "Taylor support follows mapped positive moment zero, not the signed odd moment"
        );

        let cells = IMAGE_WIDTH * IMAGE_WIDTH;
        assert_eq!(primitives.dirty().len(), 2 * cells);
        assert_eq!(primitives.psf().len(), 3 * cells);
        assert_eq!(primitives.sensitivity().len(), 3 * cells);
        for (moment, sum_weight) in expected.into_iter().enumerate() {
            assert!(
                primitives.sensitivity()[moment * cells..(moment + 1) * cells]
                    .iter()
                    .all(|value| value.to_bits() == sum_weight.to_bits()),
                "sensitivity moment {moment} must retain its exact signed sum weight"
            );
        }
    }

    let left = single_sample_blocks.primitives();
    let right = single_full_block.primitives();
    assert_eq!(left.dirty(), right.dirty());
    assert_eq!(left.psf(), right.psf());
    assert_eq!(left.sensitivity(), right.sensitivity());
    assert_eq!(left.sum_weights(), right.sum_weights());
    assert_eq!(left.channel_validity(), right.channel_validity());

    let mut symmetric_samples = samples;
    symmetric_samples[0].input_weight = 1.0;
    let (symmetric, symmetric_expected) = run_operator(
        &problem,
        &symmetric_samples,
        symmetric_samples.len(),
        2,
        None,
    );
    assert_eq!(symmetric_expected[1].to_bits(), 0.0_f64.to_bits());
    assert_eq!(symmetric.primitives().sum_weights(), symmetric_expected);
    assert_eq!(
        symmetric.primitives().channel_validity(),
        &[SpectralChannelValidity::Valid],
        "an exactly cancelled odd moment does not erase principal Taylor support"
    );
}

#[test]
fn t46_joint_block_accumulates_cross_terms_once_and_is_partition_deterministic() {
    let problem = joint_problem();
    let samples = joint_samples(&problem);
    let (one_sample_blocks, _) = run_operator(&problem, &samples, 1, 1, None);
    let (one_full_block, _) = run_operator(&problem, &samples, samples.len(), 2, None);

    for result in [&one_sample_blocks, &one_full_block] {
        assert_eq!(
            result.completion().primitive_catalog(),
            SpectralPrimitiveCatalog::UnnormalizedJointBlockV1
        );
        let primitives = result.primitives();
        assert_eq!(primitives.coefficient_term_count(), 2);
        assert_eq!(primitives.joint_continuum_term_count(), Some(1));
        assert_eq!(primitives.normal_moment_count(), 4);
        assert_eq!(primitives.normal_moment_index(0, 0), Some(0));
        assert_eq!(primitives.normal_moment_index(0, 1), Some(1));
        assert_eq!(primitives.normal_moment_index(1, 0), Some(2));
        assert_eq!(primitives.normal_moment_index(1, 1), Some(3));

        let cells = IMAGE_WIDTH * IMAGE_WIDTH;
        let cross = &primitives.psf()[cells..2 * cells];
        assert!(
            cross.iter().any(|value| value.norm() > 0.0),
            "the line-bearing visibility must contribute the continuum-line cross block"
        );
        assert_eq!(
            &primitives.psf()[cells..2 * cells],
            &primitives.psf()[2 * cells..3 * cells],
            "the two explicitly retained cross blocks must be Hermitian-equal for real weights"
        );
    }

    assert_eq!(
        one_sample_blocks
            .primitives()
            .normal_state_content_identity(),
        one_full_block.primitives().normal_state_content_identity(),
        "block size and density partitioning cannot alter the joint reduction"
    );
}

#[test]
fn t46_joint_minor_cycle_recovers_mixed_components_in_one_atomic_delta() {
    let problem = joint_problem();
    let selected = joint_samples(&problem);
    let (lifecycle, normal, model) = run_joint_final_normal_state(&problem, &selected);
    let coordinate = full_joint_masks(&normal, &model).continuum().coordinate();
    let mask_plan = ReconstructionMaskPlan::Coupled {
        continuum: Box::new(ReconstructionMaskPlan::FullPlane { coordinate }),
        line: Box::new(ReconstructionMaskPlan::FullPlane { coordinate }),
    };
    let (masks, auto_masks) = mask_plan
        .materialize_coupled(&model, &normal)
        .expect("materialize one coupled mask generation");
    assert_eq!(auto_masks, [None, None]);
    let result = ReconstructionCycle::new(
        ChannelCyclePolicy::Coupled,
        MinorCycleProgram::for_problem(&problem)
            .expect("joint controls")
            .limit_iterations(1)
            .expect("one joint iteration"),
    )
    .run_coupled(&lifecycle, &model, &normal, &masks)
    .expect("joint block solve through the shared reconstruction cycle");

    assert_eq!(result.evidence().iterations(), 1);
    let terms = result.delta().expect("one atomic joint delta").terms();
    assert_eq!(terms.len(), 2);
    assert_eq!(terms[0].cell().coefficient(), 0);
    assert_eq!(terms[1].cell().coefficient(), 1);
    assert_eq!(terms[0].cell().pixel(), terms[1].cell().pixel());
    assert!((terms[0].increment().value() - 1.0).abs() < 1.0e-6);
    assert!((terms[1].increment().value() - 2.0).abs() < 1.0e-6);
    assert!(result.evidence().final_peak_flux() < 1.0e-6);
}

#[test]
fn t46_joint_minor_cycle_rejects_an_active_block_above_the_condition_ceiling() {
    let problem = joint_problem_with_condition(1.0);
    let selected = joint_samples(&problem);
    let (lifecycle, normal, model) = run_joint_final_normal_state(&problem, &selected);
    let masks = full_joint_masks(&normal, &model);
    let error = ReconstructionCycle::new(
        ChannelCyclePolicy::Coupled,
        MinorCycleProgram::for_problem(&problem).expect("joint controls"),
    )
    .run_coupled(&lifecycle, &model, &normal, &masks)
    .expect_err("ill-conditioned active block must fail closed");
    assert!(matches!(
        error,
        ReconstructionCycleError::Minor(MinorCycleError::SingularJointNormalBlock)
    ));
}

#[test]
fn t46_joint_cycle_rejects_missing_positive_weight_support() {
    let problem = joint_problem();
    let mut selected = joint_samples(&problem);
    selected[0].input_weight = 0.0;
    selected[1].input_weight = 0.0;
    let (lifecycle, normal, model) = run_joint_final_normal_state(&problem, &selected);
    let masks = full_joint_masks(&normal, &model);
    let error = ReconstructionCycle::new(
        ChannelCyclePolicy::Coupled,
        MinorCycleProgram::for_problem(&problem).expect("joint controls"),
    )
    .run_coupled(&lifecycle, &model, &normal, &masks)
    .expect_err("zero-weight joint support must fail closed");
    assert!(matches!(
        error,
        ReconstructionCycleError::InvalidJointSupport
    ));
}

#[test]
fn t42_final_normal_state_exposes_taylor_terms_and_hankel_blocks_without_channel_aliases() {
    let problem = problem();
    let samples = samples(&problem);
    let (normal, expected) = run_final_normal_state(&problem, &samples);

    assert_eq!(
        normal.catalog(),
        NormalStateCatalog::UnnormalizedTaylorBlockV1
    );
    assert_eq!(normal.coefficient_term_count(), 2);
    assert_eq!(normal.normal_moment_count(), 3);
    assert_eq!(
        normal.support_validity(),
        Some(SpectralChannelValidity::Valid)
    );
    assert!(normal.plane(0).is_none(), "Taylor terms are not channels");

    for coefficient in 0..2 {
        let term = normal
            .coefficient_term(coefficient)
            .expect("Taylor coefficient view");
        assert_eq!(term.coefficient(), coefficient);
        assert_eq!(term.residual().len(), IMAGE_WIDTH * IMAGE_WIDTH);
    }
    assert!(normal.coefficient_term(2).is_none());

    for (moment, expected_sum_weight) in expected.into_iter().enumerate() {
        let view = normal.normal_moment(moment).expect("Taylor moment view");
        assert_eq!(view.moment(), moment);
        assert_eq!(view.sum_weight().to_bits(), expected_sum_weight.to_bits());
        assert_eq!(view.normal_approximation().len(), IMAGE_WIDTH * IMAGE_WIDTH);
        assert_eq!(view.sensitivity().len(), IMAGE_WIDTH * IMAGE_WIDTH);
    }
    assert!(normal.normal_moment(3).is_none());

    let upper = normal.normal_block(0, 1).expect("upper cross block");
    let lower = normal.normal_block(1, 0).expect("lower cross block");
    assert_eq!(upper.moment(), 1);
    assert_eq!(lower.moment(), 1);
    assert_eq!(upper.normal_approximation(), lower.normal_approximation());
    assert_eq!(upper.sensitivity(), lower.sensitivity());
    assert_eq!(upper.sum_weight().to_bits(), lower.sum_weight().to_bits());
    assert!(normal.normal_block(2, 0).is_none());
}

#[test]
fn t42_compact_v3_replay_matches_direct_residual_and_is_worker_bitwise_stable() {
    let problem = problem();
    let samples = samples(&problem);
    let frozen = freeze_taylor_replay(&problem, &samples);
    let preparation = nonzero_taylor_model(&problem);
    let (program, blocks) = compile_compact_program(&problem, &frozen);

    assert_eq!(program.schema_version(), 3);
    assert_eq!(
        gridded_normal_operator_record_bytes(&problem).expect("Taylor record width"),
        32,
        "T=2 retains one tap key plus K=3 signed f64 normal moments"
    );
    assert_eq!(program.record_bytes(), 32);
    assert_eq!(program.prediction_width(), 2);
    assert_eq!(program.block_count(), blocks.len() as u64);
    assert_eq!(
        program.record_count(),
        blocks
            .iter()
            .map(GriddedNormalOperatorBlock::record_count)
            .sum()
    );
    assert!(program.record_count() > 0);
    for block in &blocks {
        assert_eq!(
            program.block_encoded_bytes(block.sequence()),
            Some(block.encoded_bytes().len())
        );
        assert_eq!(
            block.encoded_bytes().len(),
            block.record_count() as usize * 32
        );
    }

    let direct_prior = initial_normal_from_frozen(&problem, &frozen);
    let initial_content = direct_prior.content_identity();
    assert_eq!(direct_prior.coefficient_term_count(), 2);
    assert_eq!(direct_prior.normal_moment_count(), 3);
    let direct = complete_frozen_taylor_operator(
        &problem,
        &frozen,
        &preparation,
        SpectralOperatorPass::ResidualRefresh,
        Some(direct_prior),
    );
    assert_eq!(
        direct.completion().primitive_catalog(),
        SpectralPrimitiveCatalog::UnnormalizedTaylorBlockV1
    );
    assert_eq!(direct.primitives().coefficient_term_count(), 2);
    assert_eq!(direct.primitives().normal_moment_count(), 3);
    let direct_residual = direct.primitives().dirty().to_vec();

    let serial_prior = initial_normal_from_frozen(&problem, &frozen);
    assert_eq!(serial_prior.content_identity(), initial_content);
    let serial = execute_compact_taylor(
        &problem,
        &frozen,
        &program,
        &blocks,
        &preparation,
        serial_prior,
        1,
    );
    let two_worker_prior = initial_normal_from_frozen(&problem, &frozen);
    assert_eq!(two_worker_prior.content_identity(), initial_content);
    let two_workers = execute_compact_taylor(
        &problem,
        &frozen,
        &program,
        &blocks,
        &preparation,
        two_worker_prior,
        2,
    );
    let four_worker_prior = initial_normal_from_frozen(&problem, &frozen);
    assert_eq!(four_worker_prior.content_identity(), initial_content);
    let four_workers = execute_compact_taylor(
        &problem,
        &frozen,
        &program,
        &blocks,
        &preparation,
        four_worker_prior,
        4,
    );

    let nrms = complex_nrms(&serial.residual, &direct_residual);
    assert!(
        nrms <= 0.001,
        "compact Taylor dirty-Hx must match selected-sample residual refresh: NRMS={nrms:e}"
    );
    assert_eq!(
        serial.residual, two_workers.residual,
        "workers=1 and workers=2 must commit bitwise-identical Taylor residuals"
    );
    assert_eq!(
        serial.residual, four_workers.residual,
        "workers=1 and workers=4 must commit bitwise-identical Taylor residuals"
    );

    let expected_route_capacity = gridded_normal_route_capacity_bytes(
        usize::try_from(program.record_count()).expect("program records fit usize"),
        blocks.len(),
        program.prediction_width(),
    )
    .expect("exact Taylor route residency");
    let expected_routing = GriddedNormalRoutingMeasurements {
        frames_routed: program.block_count(),
        encoded_records: program.record_count(),
        routed_record_memberships: program.record_count(),
        prediction_groups: program.record_count(),
        degrid_records: program.record_count(),
        grid_records: program.record_count(),
        sector_rescans: 0,
        peak_physical_route_capacity_bytes: expected_route_capacity,
    };
    assert_eq!(serial.routing, expected_routing);
    assert_eq!(two_workers.routing, expected_routing);
    assert_eq!(four_workers.routing, expected_routing);

    let grid_shape = SpectralOperatorSpecification::new(&problem)
        .expect("Taylor residency specification")
        .grid_shape();
    let grid_cells = grid_shape[0] * grid_shape[1];
    assert_eq!(
        serial.grid_residency.merge_complex_values(),
        grid_cells * 2 * 2,
        "only T model-dependent grids plus compensation are retained for final merge"
    );
    assert_eq!(serial.grid_residency, two_workers.grid_residency);
    assert_eq!(serial.grid_residency, four_workers.grid_residency);
    assert!(serial.grid_residency.tile_accumulator_complex_values() > 0);
    assert_eq!(
        serial.grid_residency.peak_complex_values(),
        serial.grid_residency.tile_accumulator_complex_values()
            + serial.grid_residency.merge_complex_values()
    );
    assert!(serial.grid_residency.metadata_bytes() > 0);
}

#[test]
fn t46_joint_compact_replay_matches_direct_residual_and_is_worker_bitwise_stable() {
    let problem = joint_problem();
    let selected = joint_samples(&problem);
    let frozen = freeze_taylor_replay(&problem, &selected);
    let preparation = nonzero_taylor_model(&problem);
    let (program, blocks) = compile_compact_program(&problem, &frozen);

    assert_eq!(program.record_bytes(), 32);
    assert_eq!(program.prediction_width(), 1);
    let direct_prior = initial_normal_from_frozen(&problem, &frozen);
    let initial_content = direct_prior.content_identity();
    assert_eq!(direct_prior.coefficient_term_count(), 2);
    assert_eq!(direct_prior.normal_moment_count(), 4);
    let direct = complete_frozen_taylor_operator(
        &problem,
        &frozen,
        &preparation,
        SpectralOperatorPass::ResidualRefresh,
        Some(direct_prior),
    );
    assert_eq!(
        direct.completion().primitive_catalog(),
        SpectralPrimitiveCatalog::UnnormalizedJointBlockV1
    );
    let expected = direct.primitives().dirty();
    let expected_common = direct
        .primitives()
        .common_residual()
        .expect("joint operator retains the channel-local common residual");
    let mut serial_residual = None;
    let mut serial_common_residual = None;

    for workers in [1, 2] {
        let prior = initial_normal_from_frozen(&problem, &frozen);
        assert_eq!(prior.content_identity(), initial_content);
        let compact = execute_compact_taylor(
            &problem,
            &frozen,
            &program,
            &blocks,
            &preparation,
            prior,
            workers,
        );
        let nrms = complex_nrms(&compact.residual, expected);
        assert!(
            nrms <= 0.001,
            "compact joint dirty-Hx must match selected-sample residual refresh: NRMS={nrms:e}"
        );
        let common = compact
            .common_residual
            .as_deref()
            .expect("compact joint replay retains the common residual");
        let common_nrms = complex_nrms(common, expected_common);
        assert!(
            common_nrms <= 0.001,
            "compact joint common residual must match selected-sample refresh: NRMS={common_nrms:e}"
        );
        if let Some(serial) = &serial_residual {
            assert_eq!(
                &compact.residual, serial,
                "workers=1 and workers=2 must commit bitwise-identical joint residuals"
            );
        } else {
            serial_residual = Some(compact.residual.clone());
        }
        if let Some(serial) = &serial_common_residual {
            assert_eq!(
                common, serial,
                "workers=1 and workers=2 must commit bitwise-identical common residuals"
            );
        } else {
            serial_common_residual = Some(common.to_vec());
        }
    }
}
