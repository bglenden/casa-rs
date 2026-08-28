// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    error::Error,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use casa_imaging_model::{
    AxisOrder, CentreLaws, CorrelationProduct, CorrelationSelection, CorrelationType,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FrequencyFrame, GeometryInput,
    ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest, InstrumentResponse,
    ItrfPosition, LogicalIdentity, MeasurementEquationContract, ModelBounds, ModelCell,
    ModelColumnWrite, ModelDeltaTerm, ModelExecutionAttemptId, ModelInnerProduct,
    ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity, NumericPrecision,
    NumericalStage, NumericsContract, ObservationSelection, ObservationTransactionRequirements,
    PhaseCentreLaw, PointingCentreLaw, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy,
    ProductKind, ProductNormalization, ProductRequirements, ProductSupportComparison,
    ProductValidityPolicies, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, RestFrequency,
    RestoringBeamPolicy, RowSelection, ScientificContract, SelectedMainRow, SelectedRowsBuilder,
    SelectionBound, SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection,
    StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy, TimeRange, TimeScale,
    TimeSelection, UvwCoordinateLaw, VisibilityColumn, VisibilityInnerProduct, WeightColumn,
    WeightDensityScope, WeightingContract, WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FrozenWeightingCoverageProof, MajorCycleOwner, MajorCyclePreparation,
    ModelLifecycle, SpectralOperatorSpecification, WeightingExecutionLimits,
    begin_weighting_generation, plan_weighting,
    runtime_adapter::{
        SpectralOperatorPass, prepare_spectral_operator, spectral_operator_workload,
    },
};
use casa_ms::{
    BoundSelectedObservation, MeasurementSet, MsSelectionIoBudget, SelectedObservationBlock,
    SelectedObservationContentBudget, SelectedObservationReplayProof,
    SelectedObservationResolutionRequest, SelectedObservationRow, VisibilityDataColumn,
    resolve_selected_observation,
};
use casa_types::measures::{epoch::EpochRef, frequency::FrequencyRef};
use serde_json::json;

use super::*;

const DATA_ROOT_ENV: &str = "CASA_RS_IMPERF_DATA_ROOT";
const DATASET_RELATIVE_PATH: &str = "wave1/vla/single/medium/ms/wave1-vla-single-medium.ms";
const EXPECTED_FULL_SELECTED_ROWS: u64 = 4_094_064;
const WINDOW_ROWS: u64 = 65_536;
const WINDOW_STARTS: [u64; 4] = [0, 1_342_843, 2_685_685, 4_028_528];
const EXPECTED_SELECTED_ROWS: u64 = 263_250;
const EXPECTED_SELECTED_SAMPLES: u64 = 33_696_000;
const CAPTURED_RESIDENCY_BYTES: usize = 1 << 30;
const CAPTURED_BLOCK_LIMIT: usize = 16;
const EXPECTED_CAPTURED_BLOCKS: usize = 6;
const EXPECTED_CAPTURED_LOGICAL_BYTES: u64 = 330_905_250;
const EXPECTED_CAPTURED_READ_OPERATIONS: u64 = 114;
const EXPECTED_CAPTURED_CURRENT_BYTES: u64 = 375_131_250;
const EXPECTED_CAPTURED_CAPACITY_BYTES: u64 = 378_169_650;
const EXPECTED_WEIGHTED_BLOCKS: u64 = 8_227;
const WEIGHTED_BLOCK_SAMPLES: usize = 4_096;
const EXPECTED_NORMAL_STATE_IDENTITY: &str =
    "e6368112404a3ce2b3b3b9e988bde85dadd5726e09de8d87ca4499dc27a71b91";

struct ProbeProblem {
    problem: CompiledProblem,
    request: SelectedObservationResolutionRequest,
    selected: BoundSelectedObservation,
    selected_rows: u64,
}

struct CapturedBlocks<'a> {
    blocks: Vec<SelectedObservationBlock>,
    consumer: SelectedObservationBlockConsumer<'a>,
    terminal: SelectedObservationTerminal,
    logical_bytes: u64,
    read_operations: u64,
    current_bytes: u64,
    capacity_bytes: u64,
    elapsed: Duration,
}

#[test]
#[ignore = "requires the mounted VLA medium performance dataset"]
fn medium_vla_64ch_owner_validated_open() -> Result<(), Box<dyn Error>> {
    let probe = build_problem(&dataset_path()?)?;
    assert_eq!(probe.selected_rows, EXPECTED_SELECTED_ROWS);
    Ok(())
}

#[test]
#[ignore = "requires the mounted VLA medium performance dataset"]
fn medium_vla_64ch_residual_refresh() -> Result<(), Box<dyn Error>> {
    let total_start = Instant::now();
    let ProbeProblem {
        problem,
        request,
        selected,
        selected_rows,
    } = build_problem(&dataset_path()?)?;
    let selected_samples = selected_rows
        .checked_mul(64 * 2)
        .ok_or("selected sample count overflowed")?;
    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(WEIGHTED_BLOCK_SAMPLES, 1)?,
    )?;
    let captured = capture_blocks(selected, &problem)?;
    assert!(
        captured.blocks.len() <= CAPTURED_BLOCK_LIMIT,
        "captured block count exceeded the fixed residency bound"
    );
    assert_eq!(
        captured.blocks.len(),
        EXPECTED_CAPTURED_BLOCKS,
        "bounded source block shape changed"
    );
    assert!(
        usize::try_from(captured.capacity_bytes)? <= CAPTURED_RESIDENCY_BYTES,
        "captured block capacity exceeded the fixed residency bound"
    );

    let CapturedBlocks {
        blocks,
        consumer: density_consumer,
        terminal,
        logical_bytes,
        read_operations,
        current_bytes,
        capacity_bytes,
        elapsed: capture_elapsed,
    } = captured;
    let setup_start = Instant::now();
    let (selected_generation, selected_replay_proof, density) = freeze_density(
        &problem,
        &plan,
        &blocks,
        density_consumer,
        terminal,
        [current_bytes, capacity_bytes],
        selected_samples,
    )?;
    let mut lifecycle = ModelLifecycle::bind(
        ExecutableModelProblem::from_compiled(problem.clone())?,
        attempt(1),
        1,
    )?;
    let initial_model = lifecycle.initial_empty()?;
    let delta = lifecycle.compile_delta(
        &initial_model,
        [ModelDeltaTerm::new(
            ModelCell::new(0, 0, 0, [512, 512]),
            casa_imaging_model::ModelValue::new(1.0)?,
        )],
    )?;
    let initial_preparation =
        MajorCyclePreparation::prepare(&lifecycle, initial_model, Some(delta))?;

    let initial_weights = density.finish_into_stream(&problem, &plan)?;
    let specification = SpectralOperatorSpecification::new(&problem)?;
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::InitialMajor,
    )?;
    let mut initial_operator =
        prepare_spectral_operator(specification, workload)?.begin_streaming(&problem)?;
    initial_operator.bind_major_cycle_model(initial_preparation.final_model(), None)?;
    let initial_consumer = fresh_consumer(&request, &problem)?;
    let (weighting, initial_summary) = {
        let mut initial_emit = |block: &ReconstructionWeightedBlock| {
            initial_operator.consume_block(block)?;
            Ok::<(), casa_imaging_reconstruction::SpectralOperatorError>(())
        };
        let initial_kernel = WeightingBlockKernel {
            problem: &problem,
            consumer: initial_consumer,
            weights: initial_weights,
            continuum: None,
            spectral_support_sample_count: 0,
            spectral_contributions: SpectralContributionCache::new(),
            emit: &mut initial_emit,
        };
        let WeightingBlockKernelCompletion {
            weights: (weighting, summary),
            ..
        } = replay_weighting_kernel(initial_kernel, &blocks)?;
        (weighting, summary)
    };
    let initial_complete =
        initial_operator.complete(&initial_summary, selected_generation, None)?;
    let coverage_proof = FrozenWeightingCoverageProof::seal(
        &problem,
        &weighting,
        &initial_summary,
        selected_generation,
        selected_samples,
        None,
    )?;
    let initial_join = MajorCycleOwner::from_complete_data(initial_complete, initial_preparation)?
        .reconcile(&mut lifecycle)?;
    let (prior_normal_state, continuation) = initial_join.into_continuation();
    let (continued_lifecycle, carried_model) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone())?,
        attempt(2),
        2,
        continuation,
    )?;
    let preparation = MajorCyclePreparation::prepare(&continued_lifecycle, carried_model, None)?;
    let setup_elapsed = setup_start.elapsed();

    let specification = SpectralOperatorSpecification::new(&problem)?;
    let workload = spectral_operator_workload(
        &specification,
        plan.limits().max_block_samples(),
        SpectralOperatorPass::ResidualRefresh,
    )?;
    let mut operator =
        prepare_spectral_operator(specification, workload)?.begin(&problem, &weighting)?;
    operator.bind_major_cycle_model(preparation.final_model(), Some(prior_normal_state))?;
    operator.authorize_derived_coverage(coverage_proof)?;
    let replay = weighting.begin_derived_replay(&problem, &plan, coverage_proof, None)?;
    let consumer = fresh_rebound_consumer(&request, &problem, &selected_replay_proof)?;
    let (
        replay_summary,
        replay_consumer,
        replay_elapsed,
        operator_elapsed,
        predicted_samples,
        emitted_blocks,
    ) = {
        let mut operator_elapsed = Duration::ZERO;
        let mut predicted_samples = 0_u64;
        let mut emitted_blocks = 0_u64;
        let mut emit = |block: &ReconstructionWeightedBlock| {
            let started = Instant::now();
            let predicted = operator.consume_block(block)?;
            operator_elapsed += started.elapsed();
            predicted_samples = predicted_samples
                .checked_add(u64::try_from(predicted.len()).expect("prediction count fits u64"))
                .expect("prediction count does not overflow");
            emitted_blocks = emitted_blocks
                .checked_add(1)
                .expect("block count does not overflow");
            Ok::<(), casa_imaging_reconstruction::SpectralOperatorError>(())
        };
        let kernel = WeightingBlockKernel {
            problem: &problem,
            consumer,
            weights: replay,
            continuum: None,
            spectral_support_sample_count: 0,
            spectral_contributions: SpectralContributionCache::new(),
            emit: &mut emit,
        };
        let replay_started = Instant::now();
        let WeightingBlockKernelCompletion {
            consumer,
            weights: replay_summary,
            ..
        } = replay_weighting_kernel(kernel, &blocks)?;
        (
            replay_summary,
            consumer,
            replay_started.elapsed(),
            operator_elapsed,
            predicted_samples,
            emitted_blocks,
        )
    };
    assert_eq!(replay_summary.sample_count(), selected_samples);
    coverage_proof.validate_derived_replay(
        selected_generation,
        selected_samples,
        None,
        &replay_summary,
    )?;
    let finish_started = Instant::now();
    let result = operator.complete(&replay_summary, selected_generation, None)?;
    let finish_elapsed = finish_started.elapsed();
    let selected_generation_proof_bytes = replay_consumer.generation_proof_bytes();
    let selected_generation_proof_hash_calls = replay_consumer.generation_proof_hash_calls();
    let weighting_coverage_proof_bytes = replay_summary.coverage_proof_bytes();
    let weighting_coverage_proof_hash_calls = replay_summary.coverage_proof_hash_calls();
    let operator_coverage_proof_bytes = result.completion().coverage_proof_bytes();
    let operator_coverage_proof_hash_calls = result.completion().coverage_proof_hash_calls();
    let total_coverage_proof_bytes = weighting_coverage_proof_bytes
        .checked_add(operator_coverage_proof_bytes)
        .ok_or("coverage proof byte count overflowed")?;
    let total_coverage_proof_hash_calls = weighting_coverage_proof_hash_calls
        .checked_add(operator_coverage_proof_hash_calls)
        .ok_or("coverage proof hash-call count overflowed")?;
    let checksum = result.primitives().normal_state_content_identity();
    let checksum_text = checksum.to_string();
    let source_revision = source_revision()?;
    let weighting_exclusive = replay_elapsed.saturating_sub(operator_elapsed);
    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "casa-rs-serial-compute-discriminator-v1",
            "source_revision": source_revision,
            "dataset": DATASET_RELATIVE_PATH,
            "problem_id": problem.problem_id().to_string(),
            "workers": 1,
            "partitions_per_block": 1,
            "selected_rows": selected_rows,
            "selected_samples": replay_summary.sample_count(),
            "captured_blocks": blocks.len(),
            "captured_logical_bytes": logical_bytes,
            "captured_read_operations": read_operations,
            "captured_current_bytes": current_bytes,
            "captured_capacity_bytes": capacity_bytes,
            "weighted_blocks": emitted_blocks,
            "predicted_samples": predicted_samples,
            "capture_ms": milliseconds(capture_elapsed),
            "setup_ms": milliseconds(setup_elapsed),
            "replay_ms": milliseconds(replay_elapsed),
            "operator_consume_ms": milliseconds(operator_elapsed),
            "projection_spectral_weighting_ms": milliseconds(weighting_exclusive),
            "operator_finish_ms": milliseconds(finish_elapsed),
            "selected_generation_proof_bytes": selected_generation_proof_bytes,
            "selected_generation_proof_hash_calls": selected_generation_proof_hash_calls,
            "selected_generation_proof_terminalized": false,
            "weighting_coverage_proof_bytes": weighting_coverage_proof_bytes,
            "weighting_coverage_proof_hash_calls": weighting_coverage_proof_hash_calls,
            "operator_coverage_proof_bytes": operator_coverage_proof_bytes,
            "operator_coverage_proof_hash_calls": operator_coverage_proof_hash_calls,
            "total_coverage_proof_bytes": total_coverage_proof_bytes,
            "total_coverage_proof_hash_calls": total_coverage_proof_hash_calls,
            "total_ms": milliseconds(total_start.elapsed()),
            "normal_state_identity": checksum_text,
        }))?
    );
    assert_eq!(
        selected_rows, EXPECTED_SELECTED_ROWS,
        "fixture row count changed"
    );
    assert_eq!(
        replay_summary.sample_count(),
        EXPECTED_SELECTED_SAMPLES,
        "fixture sample count changed"
    );
    assert_eq!(
        [
            logical_bytes,
            read_operations,
            current_bytes,
            capacity_bytes
        ],
        [
            EXPECTED_CAPTURED_LOGICAL_BYTES,
            EXPECTED_CAPTURED_READ_OPERATIONS,
            EXPECTED_CAPTURED_CURRENT_BYTES,
            EXPECTED_CAPTURED_CAPACITY_BYTES,
        ],
        "captured source I/O or residency invariants changed"
    );
    assert_eq!(
        [emitted_blocks, predicted_samples],
        [EXPECTED_WEIGHTED_BLOCKS, EXPECTED_SELECTED_SAMPLES],
        "weighted block shape or prediction count changed"
    );
    assert_eq!(
        checksum_text, EXPECTED_NORMAL_STATE_IDENTITY,
        "scientific checksum changed"
    );
    assert!(
        selected_generation_proof_bytes == 0 && selected_generation_proof_hash_calls == 0,
        "rebound selected-generation proof must perform zero timed hashing"
    );
    assert!(
        weighting_coverage_proof_bytes == 0 && weighting_coverage_proof_hash_calls == 0,
        "derived weighting coverage must perform zero timed hashing"
    );
    assert_eq!(
        [
            weighting_coverage_proof_bytes,
            weighting_coverage_proof_hash_calls,
        ],
        [
            operator_coverage_proof_bytes,
            operator_coverage_proof_hash_calls,
        ],
        "weighting and operator coverage derivation must perform the same zero work"
    );
    assert!(
        replay_elapsed.as_secs_f64() <= 8.919_854_174_7,
        "timed candidate replay exceeded the approved discriminator ceiling"
    );
    Ok(())
}

fn build_problem(path: &Path) -> Result<ProbeProblem, Box<dyn Error>> {
    let ms = MeasurementSet::open(path)?;
    let data_description = ms.data_description()?;
    let spectral_window = ms.spectral_window()?;
    let polarization = ms.polarization()?;
    let ddid = 0_usize;
    let spw_id = usize::try_from(data_description.spectral_window_id(ddid)?)?;
    let polarization_id = usize::try_from(data_description.polarization_id(ddid)?)?;
    if spw_id != 0 {
        return Err("probe fixture DDID 0 no longer selects SPW 0".into());
    }
    let row_selection = ms.selected_observation_row_selection(&[0], Some(&[0]), None, None)?;
    let content_budget =
        SelectedObservationContentBudget::new(CAPTURED_RESIDENCY_BYTES, CAPTURED_BLOCK_LIMIT, 4);
    let selection_io = MsSelectionIoBudget {
        available_bytes: content_budget.available_bytes(),
        maximum_live_blocks: content_budget.maximum_live_blocks(),
        requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
        storage_alignment_rows: None,
    };
    let mut full_selected_rows = 0_u64;
    let mut time_bounds = [(None, None); WINDOW_STARTS.len()];
    ms.visit_selected_observation_rows(&row_selection, selection_io, |row| {
        let ordinal = full_selected_rows;
        full_selected_rows += 1;
        for (window, start) in WINDOW_STARTS.into_iter().enumerate() {
            if ordinal == start {
                time_bounds[window].0 = Some(row.time_mjd_seconds());
            }
            if ordinal == start + WINDOW_ROWS - 1 {
                time_bounds[window].1 = Some(row.time_mjd_seconds());
            }
        }
    })?;
    if full_selected_rows != EXPECTED_FULL_SELECTED_ROWS {
        return Err(format!(
            "fixture selected-row count changed: expected {EXPECTED_FULL_SELECTED_ROWS}, got {full_selected_rows}"
        )
        .into());
    }
    let time_ranges = time_bounds
        .into_iter()
        .map(|(lower, upper)| {
            Ok(TimeRange::new(
                Some(SelectionBound::inclusive(
                    lower.ok_or("probe window has no lower timestamp")?,
                )),
                Some(SelectionBound::inclusive(
                    upper.ok_or("probe window has no upper timestamp")?,
                )),
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let base_rows = row_selection.rows();
    let row_filter = RowSelection::new(
        base_rows.fields().clone(),
        TimeSelection::Ranges(time_ranges),
        base_rows.uv_distances().clone(),
        base_rows.antennas().clone(),
        base_rows.scans().clone(),
        base_rows.observations().clone(),
        base_rows.intents().clone(),
        base_rows.arrays().clone(),
    );
    let mut selected_rows =
        SelectedRowsBuilder::with_data_description_capacity(u64::try_from(ms.row_count())?, 1);
    let mut selected_rows_error = None;
    let mut first_selected_time = None;
    ms.visit_selected_observation_rows(&row_selection, selection_io, |row| {
        let selected = time_bounds.into_iter().any(|(lower, upper)| {
            lower.is_some_and(|lower| row.time_mjd_seconds() >= lower)
                && upper.is_some_and(|upper| row.time_mjd_seconds() <= upper)
        });
        if selected {
            first_selected_time.get_or_insert(row.time_mjd_seconds());
            if row.field_id() != 0 || row.data_description_id() != 0 {
                selected_rows_error = Some("probe window crossed field or DDID".to_string());
            } else if selected_rows_error.is_none() {
                selected_rows_error = selected_rows
                    .push(SelectedMainRow::new(
                        u64::try_from(row.physical_row()).expect("MS row fits u64"),
                        0,
                    ))
                    .err()
                    .map(|error| error.to_string());
            }
        }
    })?;
    if let Some(error) = selected_rows_error {
        return Err(error.into());
    }
    let rows = selected_rows.finish();
    let selected_row_count = rows.selected_row_count();
    if selected_row_count != EXPECTED_SELECTED_ROWS {
        return Err(format!(
            "probe row windows changed: expected {EXPECTED_SELECTED_ROWS}, got {selected_row_count}"
        )
        .into());
    }

    let frequencies = spectral_window.chan_freq(spw_id)?;
    if frequencies.len() < 64 {
        return Err("probe SPW no longer contains 64 channels".into());
    }
    let channels = (0_u32..64).collect::<Vec<_>>();
    let correlation_codes = polarization.corr_type(polarization_id)?;
    let correlations = correlation_codes
        .iter()
        .enumerate()
        .map(|(index, code)| {
            Ok(CorrelationProduct::new(
                u32::try_from(index)?,
                correlation_type(*code)?,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if correlations.len() != 2 {
        return Err("probe fixture must retain exactly two correlations".into());
    }
    let observation_selection = ObservationSelection::new(
        rows,
        row_filter,
        row_selection.data_descriptions().to_vec(),
        vec![SpectralWindowSelection::new(spw_id as u32, channels)],
        vec![CorrelationSelection::new(
            u32::try_from(polarization_id)?,
            correlations,
        )],
    );

    let phase = casa_ms::derived::engine::resolve_field_phase_direction_j2000(&ms, 0)?;
    let (right_ascension, declination) = phase.as_angles();
    let cell = 0.25 * std::f64::consts::PI / (180.0 * 3600.0);
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, right_ascension, declination),
        [512.0, 512.0],
        [-cell, cell],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let frame_engine = casa_ms::derived::engine::MsCalEngine::new(&ms)?;
    let source_frequency_reference =
        FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(spw_id)?)
            .ok_or("unsupported source frequency frame")?;
    let source_frame = imaging_frequency_frame(source_frequency_reference)?;
    let output_reference = FrequencyRef::LSRK;
    let output_frame = FrequencyFrame::Lsrk;
    let anchor_time = first_selected_time.ok_or("probe selected no rows")?;
    let source_reference_frequency = frequencies[..64].iter().sum::<f64>() / 64.0;
    let output_reference_frequency = casa_ms::convert_frequency_to_frame(
        source_frequency_reference,
        output_reference,
        source_reference_frequency,
        anchor_time,
        0,
        &frame_engine,
    )?;
    let [x_metres, y_metres, z_metres] = frame_engine.observatory_position().as_itrf();
    let spectral_anchor = if source_frame == output_frame {
        SpectralFrameAnchor::NotApplicable
    } else {
        SpectralFrameAnchor::Conversion {
            epoch: Epoch::new(
                anchor_time / 86_400.0,
                imaging_time_scale(frame_engine.time_reference())?,
            ),
            direction: direction.reference_direction(),
            observatory_position: ItrfPosition::new(x_metres, y_metres, z_metres),
        }
    };
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(1024, 1024),
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
            source_frame,
            output_frame,
            spectral_anchor,
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: output_reference_frequency,
                increment_hz: 1.0,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let specification = specification()?;
    let visibility = if ms.data_column(VisibilityDataColumn::CorrectedData).is_ok() {
        VisibilityColumn::CorrectedData
    } else {
        VisibilityColumn::Data
    };
    let weights = if ms.main_table().column_accessor("WEIGHT_SPECTRUM").is_ok() {
        WeightColumn::WeightSpectrum
    } else {
        WeightColumn::Weight
    };
    let request = SelectedObservationResolutionRequest::new(
        path.display().to_string(),
        LogicalIdentity::from_sha256([0x64; 32]),
        observation_selection,
        visibility,
        weights,
        Vec::new(),
        ModelStateIdentity::Empty,
        content_budget,
        casa_ms::open_measures_runtime()?,
    );
    let resolved = resolve_selected_observation(request.clone())?;
    let (snapshot, access) = resolved.into_parts();
    let observation = compile_observation(snapshot)?;
    let problem = compile(ImagingRequest::new(
        specification,
        geometry,
        ProblemInputIdentities::new(observation),
        ModelLifecycleRequirements::new(
            ModelBounds::new(1024 * 1024, 1, 1, 1024 * 1024, 1.0e30, 1.0e30)?,
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))?;
    access.certify_residency(&problem)?;
    let selected = access.open(&problem)?;
    Ok(ProbeProblem {
        problem,
        request,
        selected,
        selected_rows: selected_row_count,
    })
}

fn specification() -> Result<ProblemSpecification, Box<dyn Error>> {
    Ok(ProblemSpecification::new(
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
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(500, 0.1, 0.0)
                .with_maximum_model_update(1.0e30)
                .with_cycle_limits(50, None),
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
                ProductKind::RestoredImage,
                ProductKind::SumWeights,
                ProductKind::Mask,
                ProductKind::Beam,
            ],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::PerPlane,
            ProductValidityPolicies::new(
                PrimaryBeamValidityPolicy::new(
                    0.2,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )?,
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )?,
            ),
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
    ))
}

fn capture_blocks<'a>(
    selected: BoundSelectedObservation,
    problem: &'a CompiledProblem,
) -> Result<CapturedBlocks<'a>, Box<dyn Error>> {
    let started = Instant::now();
    let (mut source, consumer) = selected.into_block_stream(problem)?;
    let mut blocks = Vec::new();
    let mut logical_bytes = 0_u64;
    let mut read_operations = 0_u64;
    let mut current_bytes = 0_u64;
    let mut capacity_bytes = 0_u64;
    loop {
        let mut block = source.create_storage(blocks.len());
        if source.fill_next(&mut block)?.is_none() {
            break;
        }
        logical_bytes = logical_bytes
            .checked_add(block.logical_bytes())
            .ok_or("byte overflow")?;
        read_operations = read_operations
            .checked_add(block.source_read_operations())
            .ok_or("read-operation overflow")?;
        current_bytes = current_bytes
            .checked_add(block.resident_current_bytes()?)
            .ok_or("current-byte overflow")?;
        capacity_bytes = capacity_bytes
            .checked_add(block.resident_capacity_bytes()?)
            .ok_or("capacity-byte overflow")?;
        blocks.push(block);
        if blocks.len() > CAPTURED_BLOCK_LIMIT
            || usize::try_from(capacity_bytes)? > CAPTURED_RESIDENCY_BYTES
        {
            return Err("captured source residency exceeded its fixed bound".into());
        }
    }
    let terminal = source.complete()?;
    Ok(CapturedBlocks {
        blocks,
        consumer,
        terminal,
        logical_bytes,
        read_operations,
        current_bytes,
        capacity_bytes,
        elapsed: started.elapsed(),
    })
}

fn freeze_density<'a>(
    problem: &'a CompiledProblem,
    plan: &casa_imaging_reconstruction::WeightingPlan,
    blocks: &[SelectedObservationBlock],
    consumer: SelectedObservationBlockConsumer<'a>,
    mut terminal: SelectedObservationTerminal,
    residency_bytes: [u64; 2],
    expected_samples: u64,
) -> Result<
    (
        SelectedObservationGenerationId,
        SelectedObservationReplayProof,
        casa_imaging_reconstruction::WeightingDensityPhase,
    ),
    Box<dyn Error>,
> {
    let density = begin_weighting_generation(problem, plan)?;
    let mut kernel = DensityBlockKernel {
        problem,
        consumer,
        density,
        spectral_contributions: SpectralContributionCache::new(),
    };
    for block in blocks {
        kernel.consume_selected_block(block)?;
    }
    let resolved = kernel.complete()?;
    terminal.record_runtime_residency(blocks.len(), residency_bytes[0], residency_bytes[1])?;
    let (_, completion) = resolved.consumer.complete(terminal)?;
    if completion.sample_count() != expected_samples {
        return Err(format!(
            "selected sample count changed: expected {expected_samples}, got {}",
            completion.sample_count()
        )
        .into());
    }
    let replay_proof = completion
        .replay_proof()
        .ok_or("owner-validated density traversal omitted replay proof")?;
    Ok((completion.generation_id(), replay_proof, resolved.density))
}

fn fresh_consumer<'a>(
    request: &SelectedObservationResolutionRequest,
    problem: &'a CompiledProblem,
) -> Result<SelectedObservationBlockConsumer<'a>, Box<dyn Error>> {
    let resolved = resolve_selected_observation(request.clone())?;
    let (_, access) = resolved.into_parts();
    access.certify_residency(problem)?;
    let selected = access.open(problem)?;
    let (source, consumer) = selected.into_block_stream(problem)?;
    drop(source);
    Ok(consumer)
}

fn fresh_rebound_consumer<'a>(
    request: &SelectedObservationResolutionRequest,
    problem: &'a CompiledProblem,
    proof: &SelectedObservationReplayProof,
) -> Result<SelectedObservationBlockConsumer<'a>, Box<dyn Error>> {
    let resolved = resolve_selected_observation(request.clone())?;
    let (_, access) = resolved.into_parts();
    access.certify_residency(problem)?;
    let selected = access.rebind(problem, proof)?;
    let (source, consumer) = selected.into_block_stream(problem)?;
    drop(source);
    Ok(consumer)
}

fn replay_weighting_kernel<'a, W, F, E>(
    mut kernel: WeightingBlockKernel<'a, W, F>,
    blocks: &[SelectedObservationBlock],
) -> Result<WeightingBlockKernelCompletion<'a, W::Finish>, WeightingBlockKernelError<E>>
where
    W: StreamingWeightPhase + Sync,
    F: FnMut(&ReconstructionWeightedBlock) -> Result<(), E> + Sync,
    E: Error + Send + 'static,
{
    for block in blocks {
        kernel.consume_selected_block(block)?;
    }
    kernel.complete()
}

fn dataset_path() -> Result<PathBuf, Box<dyn Error>> {
    let root = std::env::var_os(DATA_ROOT_ENV).ok_or("CASA_RS_IMPERF_DATA_ROOT is not set")?;
    let path = PathBuf::from(root).join(DATASET_RELATIVE_PATH);
    if !path.is_dir() {
        return Err(format!("medium dataset is missing at {}", path.display()).into());
    }
    Ok(path)
}

fn attempt(byte: u8) -> ModelExecutionAttemptId {
    ModelExecutionAttemptId::new(LogicalIdentity::from_sha256([byte; 32]))
}

fn imaging_frequency_frame(reference: FrequencyRef) -> Result<FrequencyFrame, Box<dyn Error>> {
    match reference {
        FrequencyRef::TOPO => Ok(FrequencyFrame::Topocentric),
        FrequencyRef::BARY => Ok(FrequencyFrame::Barycentric),
        FrequencyRef::LSRK => Ok(FrequencyFrame::Lsrk),
        _ => Err(format!("unsupported source frequency frame {reference}").into()),
    }
}

fn imaging_time_scale(reference: EpochRef) -> Result<TimeScale, Box<dyn Error>> {
    match reference {
        EpochRef::UTC => Ok(TimeScale::Utc),
        EpochRef::TAI => Ok(TimeScale::Tai),
        EpochRef::TT => Ok(TimeScale::Tt),
        EpochRef::TDB => Ok(TimeScale::Tdb),
        _ => Err(format!("unsupported epoch reference {reference}").into()),
    }
}

fn correlation_type(code: i32) -> Result<CorrelationType, Box<dyn Error>> {
    use CorrelationType::*;
    Ok(match code {
        1 => StokesI,
        2 => StokesQ,
        3 => StokesU,
        4 => StokesV,
        5 => CircularRr,
        6 => CircularRl,
        7 => CircularLr,
        8 => CircularLl,
        9 => LinearXx,
        10 => LinearXy,
        11 => LinearYx,
        12 => LinearYy,
        _ => return Err(format!("unsupported fixture correlation code {code}").into()),
    })
}

fn source_revision() -> Result<String, Box<dyn Error>> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()?;
    if !output.status.success() {
        return Err("git rev-parse HEAD failed".into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
