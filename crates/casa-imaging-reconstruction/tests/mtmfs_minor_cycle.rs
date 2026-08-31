// SPDX-License-Identifier: LGPL-3.0-or-later

//! T43 focused acceptance for coupled MT-MFS point and multiscale Minor Cycles.

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
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestFrequency, RestoringBeamPolicy, RowSelection, ScientificContract,
    SelectedColumns, SelectedMainRow, SelectedObservationSample, SelectedPredictionTarget,
    SelectedRows, SelectedSampleAddress, SelectedSampleCoordinates, SelectedSampleMetadata,
    SelectedSpectralContributions, SelectedSpectralEvaluation, SelectedSpectralInterval,
    SelectedVisibilitySample, SkyDirection, SourceGenerations, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, SpectralWindowSelection, StageErrorBudget, TaylorSupportReference,
    TaylorValidityPolicy, TimeScale, TimeSelection, UvSelection, UvwCoordinateLaw,
    VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract,
    WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FinalNormalState, MajorCycleOwner, MajorCyclePreparation,
    MinorCycleProgram, MinorCycleStopReason, MinorCycleValidity, ModelGeneration, ModelLifecycle,
    ReconstructionMask, SpectralOperatorSpecification, SpectralStencilValidity,
    WeightingExecutionLimits, begin_weighting_generation, compile_spectral_stencil, plan_weighting,
    run_minor_cycle,
    runtime_adapter::{
        CompleteDataOwnerResult, SpectralOperatorPass, prepare_spectral_operator,
        spectral_operator_workload,
    },
};

const REFERENCE_FREQUENCY_HZ: f64 = 1.0e9;
const IMAGE_WIDTH: usize = 16;

fn identity(seed: u8, scope: u8) -> LogicalIdentity {
    let mut bytes = [seed; 32];
    bytes[0] = scope;
    LogicalIdentity::from_sha256(bytes)
}

fn source() -> ObservationSourceInput {
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
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(
                2,
                [SelectedMainRow::new(0, 0), SelectedMainRow::new(1, 1)],
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
                SpectralWindowSelection::new(1, vec![0]),
            ],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
            )],
        ),
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

fn problem() -> casa_imaging_model::CompiledProblem {
    problem_with_scales(vec![0.0])
}

fn problem_with_scales(scales_px: Vec<f64>) -> casa_imaging_model::CompiledProblem {
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
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: REFERENCE_FREQUENCY_HZ,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let snapshot = compile_observation(ObservationSnapshotInput::new(
        vec![source()],
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
            ReconstructionContract::new(
                ReconstructionBasis::Taylor { terms: 2 },
                ReconstructionAlgorithm::Mtmfs {
                    scales_px,
                    small_scale_bias: 0.0,
                },
                ReconstructionControls::new(8, 1.0, 0.0),
                PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
            ),
            WeightingContract::new(
                WeightingScheme::Briggs { robust: 0.5 },
                WeightDensityScope::GlobalSelection,
            ),
            ProductRequirements::new(
                vec![
                    ProductKind::Psf,
                    ProductKind::Residual,
                    ProductKind::Model,
                    ProductKind::SumWeights,
                    ProductKind::Sensitivity,
                    ProductKind::TaylorTerms,
                ],
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

fn samples(problem: &casa_imaging_model::CompiledProblem) -> [SelectedObservationSample; 2] {
    let measurement_set = problem.selected_observation().read_set().sources()[0].measurement_set();
    [
        sample(measurement_set, 0, 0, 0.8e9, 2.0, [1.0, 0.25]),
        sample(measurement_set, 1, 1, 1.2e9, 1.0, [0.5, -0.75]),
    ]
}

fn sample(
    measurement_set: MeasurementSetIdentity,
    physical_row: u64,
    data_description_id: i32,
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
            channel_index: 0,
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
    assert_eq!(terms.len(), 1, "Taylor algebra remains inside the operator");
    assert_eq!(terms[0].output_channel(), 0);
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
        .inspect_selected_observation(samples.iter().copied().map(Ok::<_, Infallible>), |_| {
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
    block_samples: usize,
    density_partitions: usize,
) -> (ModelLifecycle, FinalNormalState, ModelGeneration) {
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
    let (complete_data, _) = run_operator(
        problem,
        samples,
        block_samples,
        density_partitions,
        Some(&preparation),
    );
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
    (lifecycle, normal, final_model)
}

fn full_mask(normal: &FinalNormalState, model: &ModelGeneration) -> ReconstructionMask {
    let centre = normal.shape()[0] as f64 / 2.0;
    ReconstructionMask::full_plane(
        normal.problem_id(),
        model.generation_id(),
        DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [centre, centre],
            [-1.0e-6, 1.0e-6],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 0.0],
        ),
        normal.shape(),
    )
    .expect("full Taylor reconstruction mask")
}

fn one_pixel_mask(
    normal: &FinalNormalState,
    model: &ModelGeneration,
    pixel: [usize; 2],
) -> ReconstructionMask {
    let centre = normal.shape()[0] as f64 / 2.0;
    let coordinate = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [centre, centre],
        [-1.0e-6, 1.0e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let mut support = vec![false; normal.shape()[0] * normal.shape()[1]];
    support[pixel[0] * normal.shape()[1] + pixel[1]] = true;
    ReconstructionMask::from_reprojected_support(
        normal.problem_id(),
        model.generation_id(),
        coordinate,
        normal.shape(),
        &support,
        coordinate,
        normal.shape(),
    )
    .expect("one-pixel Taylor mask")
}

fn point_program(
    problem: &casa_imaging_model::CompiledProblem,
    iterations: usize,
) -> MinorCycleProgram {
    MinorCycleProgram::for_problem(problem)
        .expect("coupled point MT-MFS controls")
        .limit_iterations(iterations)
        .expect("positive point iteration limit")
}

fn run_point(
    problem: casa_imaging_model::CompiledProblem,
    block_samples: usize,
    density_partitions: usize,
    program: MinorCycleProgram,
    mask_pixel: Option<[usize; 2]>,
) -> casa_imaging_reconstruction::MinorCycleResult {
    let selected = samples(&problem);
    let (lifecycle, normal, model) =
        run_final_normal_state(&problem, &selected, block_samples, density_partitions);
    assert_eq!(normal.coefficient_term_count(), 2);
    assert_eq!(normal.normal_moment_count(), 3);
    assert_eq!(model.shape().coefficients(), 2);
    assert_eq!(model.shape().domains()[0].pixels(), normal.shape());
    assert_eq!(model.samples().len(), model.shape().sample_count());
    let mask = mask_pixel.map_or_else(
        || full_mask(&normal, &model),
        |pixel| one_pixel_mask(&normal, &model, pixel),
    );
    run_minor_cycle(&lifecycle, &model, &normal, &mask, program)
        .expect("coupled point MT-MFS solve")
}

fn assert_close(actual: f64, expected: f64, context: &str) {
    let scale = actual.abs().max(expected.abs()).max(1.0);
    assert!(
        (actual - expected).abs() <= 1.0e-10 * scale,
        "{context}: actual={actual:e}, expected={expected:e}"
    );
}

#[test]
fn t43_point_selection_solves_the_declared_cross_term_block_in_coefficient_order() {
    let problem = problem();
    let selected = samples(&problem);
    let (lifecycle, normal, model) = run_final_normal_state(&problem, &selected, 1, 1);
    assert_eq!(normal.coefficient_term_count(), 2);
    assert_eq!(normal.normal_moment_count(), 3);
    assert_eq!(model.shape().coefficients(), 2);
    let result = run_minor_cycle(
        &lifecycle,
        &model,
        &normal,
        &full_mask(&normal, &model),
        point_program(&problem, 1),
    )
    .expect("one coupled point selection");

    assert_eq!(result.evidence().iterations(), 1);
    let terms = result.delta().expect("one coupled delta").terms();
    assert_eq!(
        terms.len(),
        2,
        "one spatial selection updates both Taylor coefficients"
    );
    assert_eq!(terms[0].cell().coefficient(), 0);
    assert_eq!(terms[1].cell().coefficient(), 1);
    assert_eq!(terms[0].cell().pixel(), terms[1].cell().pixel());

    let pixel = terms[0].cell().pixel();
    let residual_index = pixel[0] * normal.shape()[1] + pixel[1];
    let psf_peak_index = normal
        .normal_block(0, 0)
        .expect("principal normal block")
        .normal_approximation()
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.re.total_cmp(&right.re))
        .expect("nonempty normal block")
        .0;
    let h00 = normal
        .normal_block(0, 0)
        .expect("H00")
        .normal_approximation()[psf_peak_index]
        .re;
    let h01 = normal
        .normal_block(0, 1)
        .expect("H01 cross term")
        .normal_approximation()[psf_peak_index]
        .re;
    let h11 = normal
        .normal_block(1, 1)
        .expect("H11")
        .normal_approximation()[psf_peak_index]
        .re;
    assert_ne!(
        h01.to_bits(),
        0.0_f64.to_bits(),
        "fixture must exercise coupling"
    );
    let r0 = normal
        .coefficient_term(0)
        .expect("Taylor zero residual")
        .residual()[residual_index]
        .re;
    let r1 = normal
        .coefficient_term(1)
        .expect("Taylor one residual")
        .residual()[residual_index]
        .re;
    let determinant = h00 * h11 - h01 * h01;
    assert!(
        determinant.abs() > f64::EPSILON,
        "fixture Hessian must be invertible"
    );
    let expected0 = (h11 * r0 - h01 * r1) / determinant;
    let expected1 = (h00 * r1 - h01 * r0) / determinant;
    assert_close(
        terms[0].increment().value(),
        expected0,
        "Taylor coefficient zero",
    );
    assert_close(
        terms[1].increment().value(),
        expected1,
        "Taylor coefficient one",
    );
}

#[test]
fn t43_validity_charges_all_coefficients_and_rejects_the_expiring_selection_atomically() {
    let exact_problem = problem();
    let exact_program = point_program(&exact_problem, 1);
    let exact = run_point(exact_problem, 1, 1, exact_program, None);
    let charge = exact
        .delta()
        .expect("exact coupled delta")
        .terms()
        .iter()
        .map(|term| term.increment().value().abs())
        .sum::<f64>();
    assert_close(
        exact.evidence().total_flux(),
        charge,
        "coupled validity charge",
    );

    let bounded_problem = problem();
    let bounded_program = point_program(&bounded_problem, 8)
        .with_validity(MinorCycleValidity::Bounded {
            maximum_absolute_update: charge * 0.99,
        })
        .expect("positive Taylor validity envelope");
    let bounded = run_point(bounded_problem, 1, 1, bounded_program, None);
    assert_eq!(bounded.evidence().iterations(), 0);
    assert_eq!(bounded.evidence().total_flux().to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        bounded.evidence().stop_reason(),
        MinorCycleStopReason::StalenessBound
    );
    assert!(bounded.evidence().requests_reconciliation());
    assert!(
        bounded.delta().is_none(),
        "an expiring coupled selection is all-or-nothing"
    );
}

#[test]
fn t43_multiscale_reuses_canonical_scales_and_counts_one_coupled_selection() {
    let problem = problem_with_scales(vec![2.0, 0.0, 2.0]);
    let program = MinorCycleProgram::for_problem(&problem)
        .expect("coupled multiscale MT-MFS controls")
        .limit_iterations(1)
        .expect("one coupled iteration")
        .record_component_sequence(1)
        .expect("bounded component diagnostics");
    match program.algorithm() {
        ReconstructionAlgorithm::Mtmfs { scales_px, .. } => {
            assert_eq!(
                scales_px,
                &[0.0, 2.0],
                "MT-MFS uses the canonical scale set"
            )
        }
        algorithm => panic!("unexpected algorithm {algorithm:?}"),
    }

    let result = run_point(problem, 1, 1, program, Some([10, 8]));
    assert_eq!(result.evidence().iterations(), 1);
    let terms = result
        .delta()
        .expect("one coupled multiscale delta")
        .terms();
    assert!(terms.iter().any(|term| term.cell().coefficient() == 0));
    assert!(terms.iter().any(|term| term.cell().coefficient() == 1));
    let component = result
        .evidence()
        .recorded_component_sequence()
        .expect("recorded coupled selection")
        .first()
        .expect("one coupled selection");
    assert_eq!(
        component.scale_px(),
        2.0,
        "the fixture must exercise a nonzero MT-MFS scale"
    );
    let coefficient_zero_cells = terms
        .iter()
        .filter(|term| term.cell().coefficient() == 0)
        .count();
    assert!(
        coefficient_zero_cells > 1,
        "a nonzero scale spreads each Taylor coefficient spatially"
    );
}

#[test]
fn t43_source_partitioning_is_backend_neutral_and_bitwise_deterministic() {
    let serial_problem = problem();
    let serial_program = point_program(&serial_problem, 2);
    let serial = run_point(serial_problem, 1, 1, serial_program, None);
    let partitioned_problem = problem();
    let partitioned_program = point_program(&partitioned_problem, 2);
    let partitioned = run_point(partitioned_problem, 2, 2, partitioned_program, None);
    assert_eq!(
        serial.delta().expect("serial delta").terms(),
        partitioned.delta().expect("partitioned delta").terms()
    );
    assert_eq!(
        serial.evidence().iterations(),
        partitioned.evidence().iterations()
    );
    assert_eq!(
        serial.evidence().stop_reason(),
        partitioned.evidence().stop_reason()
    );
    assert_eq!(
        serial.evidence().total_flux().to_bits(),
        partitioned.evidence().total_flux().to_bits()
    );
    assert_eq!(
        serial.evidence().final_peak_flux().to_bits(),
        partitioned.evidence().final_peak_flux().to_bits()
    );
}

#[test]
fn t43_point_scale_can_select_an_image_edge() {
    let problem = problem();
    let program = point_program(&problem, 1);
    let result = run_point(problem, 1, 1, program, Some([0, 0]));
    let delta = result.delta().expect("edge point component");
    assert!(
        delta
            .terms()
            .iter()
            .all(|term| term.cell().pixel() == [0, 0]),
        "the zero scale must not inherit the nonzero-scale border"
    );
}

#[test]
fn t43_oversized_scales_are_pruned_before_bias_and_hessian_setup() {
    let problem = problem_with_scales(vec![20.0, 0.0]);
    let program = MinorCycleProgram::for_problem(&problem)
        .expect("identity-bound MT-MFS program")
        .limit_iterations(1)
        .expect("one iteration")
        .record_component_sequence(2)
        .expect("bounded diagnostics");
    let result = run_point(problem, 1, 1, program, None);
    assert_eq!(
        result
            .evidence()
            .recorded_component_sequence()
            .expect("recorded point coefficient")[0]
            .scale_px(),
        0.0
    );
}
