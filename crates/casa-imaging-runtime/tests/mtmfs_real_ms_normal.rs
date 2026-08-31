// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused CASA-oracle gate for T42 MT-MFS block-normal construction.

use std::{
    collections::BTreeSet,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use casa_imaging_model::{
    AxisOrder, CentreLaws, CorrelationProduct, CorrelationSelection, CorrelationType,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FrequencyFrame, GeometryInput,
    ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest, InstrumentResponse,
    ItrfPosition, LogicalIdentity, MeasurementEquationContract, ModelBounds,
    ModelExecutionAttemptId, ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity,
    NumericPrecision, NumericalStage, NumericsContract, ObservationSelection,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PolarizationContract,
    PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInputIdentities,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestFrequency, RestoringBeamPolicy, ScientificContract, SelectedMainRow,
    SelectedRowsBuilder, SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection,
    StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy, TimeScale, UvwCoordinateLaw,
    VisibilityColumn, VisibilityInnerProduct, WeightColumn, WeightDensityScope, WeightingContract,
    WeightingScheme, compile, compile_observation,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, MajorCycleOwner, MajorCyclePreparation, ModelLifecycle,
    NormalStateCatalog, SpectralChannelValidity, SpectralOperatorError,
    SpectralOperatorSpecification, SpectralStencilError, SpectralStencilValidity, WeightingError,
    WeightingExecutionLimits, begin_natural_weighting_stream, compile_spectral_stencil,
    plan_weighting,
    runtime_adapter::{
        SpectralOperatorPass, prepare_spectral_operator, spectral_operator_workload,
    },
};
use casa_ms::{
    MeasurementSet, MsSelectionIoBudget, SelectedObservationContentBudget,
    SelectedObservationResolutionRequest, SelectedObservationRow,
    initialize_measurement_set_owner_manifest, resolve_selected_observation,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casa_types::measures::{epoch::EpochRef, frequency::FrequencyRef};
use serde_json::json;

const DATASET: &str = "measurementset/vla/ref_vlass_wtsp_creation.ms";
const OUTPUT_ENV: &str = "CASA_RS_T42_RUST_OUTPUT";
const IMAGE_SIZE: usize = 128;
const CHANNELS: usize = 16;
const SELECTED_ROWS: usize = 24;
const CORRELATIONS: usize = 4;
const REFERENCE_FREQUENCY_HZ: f64 = 2_091_980_123.117_861_7;
const WEIGHTED_BLOCK_SAMPLES: usize = 512;

#[derive(Debug, thiserror::Error)]
enum StreamError {
    #[error(transparent)]
    Stencil(#[from] SpectralStencilError),
    #[error(transparent)]
    Weighting(#[from] WeightingError),
    #[error(transparent)]
    Operator(#[from] SpectralOperatorError),
    #[error("focused MT-MFS stream violated its fixed contract: {0}")]
    Contract(&'static str),
}

#[test]
#[ignore = "requires slow-parity casatestdata and writes a focused CASA comparison artifact"]
fn t42_real_ms_mtmfs_normal_matches_casa_oracle_inputs() -> Result<(), Box<dyn Error>> {
    let output =
        PathBuf::from(std::env::var_os(OUTPUT_ENV).ok_or("CASA_RS_T42_RUST_OUTPUT is not set")?);
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    if !source.is_dir() {
        return Err(format!("T42 MeasurementSet is missing at {}", source.display()).into());
    }
    let staging = tempfile::tempdir()?;
    let staged = staging.path().join("ref_vlass_wtsp_creation.ms");
    copy_measurement_set(&source, &staged)?;
    initialize_measurement_set_owner_manifest(&staged)?;

    let (problem, mut selected) = build_problem(&staged)?;
    let executable = ExecutableModelProblem::from_compiled(problem.clone())?;
    let mut lifecycle = ModelLifecycle::bind(
        executable,
        ModelExecutionAttemptId::new(LogicalIdentity::from_sha256([0x52; 32])),
        1,
    )?;
    let initial = lifecycle.initial_empty()?;
    let preparation = MajorCyclePreparation::prepare(&lifecycle, initial, None)?;

    let plan = plan_weighting(
        &problem,
        WeightingExecutionLimits::new(WEIGHTED_BLOCK_SAMPLES, 1)?,
    )?;
    let mut weighting = begin_natural_weighting_stream(&problem, &plan)?;
    let specification = SpectralOperatorSpecification::new(&problem)?;
    let workload = spectral_operator_workload(
        &specification,
        WEIGHTED_BLOCK_SAMPLES,
        SpectralOperatorPass::InitialMajor,
    )?;
    let mut owner =
        prepare_spectral_operator(specification, workload)?.begin_streaming(&problem)?;
    owner.bind_major_cycle_model(preparation.final_model(), None)?;

    let mut basis_bits = vec![u32::MAX; 2 * SELECTED_ROWS * CHANNELS];
    let mut evaluated_frequency_hz = vec![f64::NAN; SELECTED_ROWS * CHANNELS];
    let mut flagged_samples = 0_u64;
    let completion = selected.traverse(&problem, |reported| {
        let sample = reported.selected();
        let address = sample.address();
        let row = usize::try_from(address.physical_row)
            .map_err(|_| StreamError::Contract("physical row exceeds usize"))?;
        let channel = usize::try_from(address.channel_index)
            .map_err(|_| StreamError::Contract("channel exceeds usize"))?;
        if row >= SELECTED_ROWS || channel >= CHANNELS {
            return Err(StreamError::Contract("unexpected selected row or channel"));
        }
        let receipt = compile_spectral_stencil(&problem, sample, reported.spectral_evaluation())?;
        match receipt.validity() {
            SpectralStencilValidity::Mapped => {}
            SpectralStencilValidity::Flagged => flagged_samples += 1,
            SpectralStencilValidity::Unmapped => {
                return Err(StreamError::Contract(
                    "valid selected sample did not map to MFS output",
                ));
            }
        }
        let mut contributions = receipt.contributions().iter();
        let contribution = contributions.next().ok_or(StreamError::Contract(
            "selected sample has no MFS contribution",
        ))?;
        if contributions.next().is_some() || contribution.output_channel() != 0 {
            return Err(StreamError::Contract("MFS stencil is not one contribution"));
        }
        let frequency_hz = contribution.evaluation_frequency_hz();
        record_basis(
            &mut basis_bits,
            &mut evaluated_frequency_hz,
            row,
            channel,
            frequency_hz,
        )?;
        if let Some(block) = weighting.consume(&problem, sample, receipt.contributions().clone())? {
            if !owner.consume_block(&block)?.is_empty() {
                return Err(StreamError::Contract(
                    "empty model emitted final visibilities",
                ));
            }
            weighting.reuse_emitted_block(block)?;
        }
        Ok(())
    })?;
    if completion.sample_count() != (SELECTED_ROWS * CHANNELS * CORRELATIONS) as u64 {
        return Err(format!(
            "unexpected selected sample count {}",
            completion.sample_count()
        )
        .into());
    }
    let traversal = *completion.measurements();
    if traversal.source_pass_count() != 1 {
        return Err(format!(
            "focused MT-MFS gate made {} source passes instead of one",
            traversal.source_pass_count()
        )
        .into());
    }
    if traversal.peak_live_blocks() == 0 || traversal.peak_live_blocks() > 2 {
        return Err(format!(
            "focused MT-MFS gate observed {} live source blocks outside the planned 1..=2 bound",
            traversal.peak_live_blocks()
        )
        .into());
    }

    let (tail, _weighting_state, summary) = weighting.finish()?;
    if let Some(block) = tail {
        if !owner.consume_block(&block)?.is_empty() {
            return Err("empty model emitted final visibilities".into());
        }
    }
    let complete_data = owner.complete(&summary, completion.generation_id(), None)?;
    let joined = MajorCycleOwner::from_complete_data(complete_data, preparation)?
        .reconcile(&mut lifecycle)?;
    let (normal, _, _) = joined.into_parts();
    if normal.catalog() != NormalStateCatalog::UnnormalizedTaylorBlockV1
        || normal.coefficient_term_count() != 2
        || normal.normal_moment_count() != 3
        || normal.shape() != [IMAGE_SIZE, IMAGE_SIZE]
        || normal.reference_frequency_hz().map(f64::to_bits)
            != Some(REFERENCE_FREQUENCY_HZ.to_bits())
    {
        return Err("new owner did not produce the expected two-term Taylor normal family".into());
    }

    let dirty = (0..2)
        .map(|term| {
            normal
                .coefficient_term(term)
                .map(|view| view.residual().to_vec())
                .ok_or("missing Taylor dirty term")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let psf = (0..3)
        .map(|moment| {
            normal
                .normal_moment(moment)
                .map(|view| view.normal_approximation().to_vec())
                .ok_or("missing Taylor normal moment")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let sum_weights = normal.sum_weights().to_vec();
    let dirty_divisor = sum_weights[0];
    let psf_divisor = psf[0]
        .iter()
        .map(|value| value.re)
        .fold(f64::NEG_INFINITY, f64::max);
    if !(dirty_divisor > 0.0 && psf_divisor > 0.0) {
        return Err("principal Taylor normalization is not positive".into());
    }
    if basis_bits.iter().any(|bits| *bits == u32::MAX)
        || evaluated_frequency_hz
            .iter()
            .any(|value| !value.is_finite())
    {
        return Err("Taylor basis capture is incomplete".into());
    }
    let support_valid = normal.support_validity() == Some(SpectralChannelValidity::Valid);
    let cells = IMAGE_SIZE * IMAGE_SIZE;
    let artifact = json!({
        "schema": "casa-rs-t42-mtmfs-normal-v1",
        "source": {
            "relative_path": DATASET,
            "physical_rows": (0..SELECTED_ROWS).collect::<Vec<_>>(),
            "selected_rows": SELECTED_ROWS,
            "selected_samples": completion.sample_count(),
            "flagged_samples": flagged_samples,
            "payload_passes": traversal.source_pass_count(),
            "maximum_live_source_blocks": traversal.peak_live_blocks(),
            "planned_maximum_live_source_blocks": 2,
            "weighted_block_samples": WEIGHTED_BLOCK_SAMPLES,
            "weighted_blocks": summary.block_count(),
            "traversal_measurements": {
                "block_count": traversal.block_count(),
                "stored_row_count": traversal.stored_row_count(),
                "stored_sample_count": traversal.stored_sample_count(),
                "logical_output_bytes": traversal.logical_output_bytes(),
                "modeled_physical_read_bytes": traversal.modeled_physical_read_bytes(),
                "source_read_operations": traversal.source_read_operations(),
                "request_handoff_bytes": traversal.request_handoff_bytes(),
                "selected_channel_run_count": traversal.selected_channel_run_count(),
                "selected_sample_handoff_bytes": traversal.selected_sample_handoff_bytes(),
                "peak_consumer_scratch_current_bytes": traversal.peak_consumer_scratch_current_bytes(),
                "consumer_scratch_capacity_bytes": traversal.consumer_scratch_capacity_bytes(),
                "allocated_storage_buffers": traversal.allocated_storage_buffers(),
                "reused_storage_buffers": traversal.reused_storage_buffers(),
                "peak_live_current_bytes": traversal.peak_live_current_bytes(),
                "peak_live_capacity_bytes": traversal.peak_live_capacity_bytes(),
                "source_read_nanos": traversal.source_read_nanos(),
                "source_fill_nanos": traversal.source_fill_nanos(),
                "source_arrangement_nanos": traversal.source_arrangement_nanos(),
            },
        },
        "geometry": {
            "shape": [IMAGE_SIZE, IMAGE_SIZE],
            "layout": "term,x,y",
            "cell_arcsec": 2.5,
            "reference_pixel": [64.0, 64.0],
            "reference_frequency_hz_f64": REFERENCE_FREQUENCY_HZ,
            "reference_frequency_f64_bits": REFERENCE_FREQUENCY_HZ.to_bits(),
        },
        "spectral": {
            "evaluated_frequency_lsrk_hz_f64": evaluated_frequency_hz,
            "taylor_basis_f32_bits": basis_bits,
        },
        "normal": {
            "catalog": "unnormalized_taylor_block_v1",
            "hankel_moment_indices": [[0, 1], [1, 2]],
            "dirty_real_f64": dirty.iter().flat_map(|plane| plane.iter().map(|value| value.re)).collect::<Vec<_>>(),
            "dirty_imag_f64": dirty.iter().flat_map(|plane| plane.iter().map(|value| value.im)).collect::<Vec<_>>(),
            "psf_real_f64": psf.iter().flat_map(|plane| plane.iter().map(|value| value.re)).collect::<Vec<_>>(),
            "psf_imag_f64": psf.iter().flat_map(|plane| plane.iter().map(|value| value.im)).collect::<Vec<_>>(),
            "sum_weights_f64": sum_weights,
            "sum_weights_f32": normal.sum_weights().iter().map(|value| *value as f32).collect::<Vec<_>>(),
            "dirty_divisor_f64": dirty_divisor,
            "psf_divisor_f64": psf_divisor,
            "dirty_normalized_f32": dirty.iter().flat_map(|plane| plane.iter().map(|value| (value.re / dirty_divisor) as f32)).collect::<Vec<_>>(),
            "psf_normalized_f32": psf.iter().flat_map(|plane| plane.iter().map(|value| (value.re / psf_divisor) as f32)).collect::<Vec<_>>(),
            "support_validity": if support_valid { "valid" } else { "invalid" },
            "valid_support_bool": vec![support_valid; cells],
        },
    });
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, serde_json::to_vec(&artifact)?)?;
    eprintln!("t42_mtmfs_rust_normal {}", output.display());
    Ok(())
}

fn build_problem(
    path: &Path,
) -> Result<
    (
        casa_imaging_model::CompiledProblem,
        casa_ms::BoundSelectedObservation,
    ),
    Box<dyn Error>,
> {
    let ms = MeasurementSet::open(path)?;
    let data_description = ms.data_description()?;
    let spectral_window = ms.spectral_window()?;
    let polarization = ms.polarization()?;
    eprintln!(
        "t42_fixture_shape main_rows={} data_descriptions={} spectral_windows={} polarizations={}",
        ms.row_count(),
        data_description.row_count(),
        spectral_window.row_count(),
        polarization.row_count()
    );
    let ddids = [0_i32, 1];
    for (ddid, expected_spw) in ddids.into_iter().zip([0_i32, 1]) {
        if data_description.spectral_window_id(ddid as usize)? != expected_spw
            || data_description.polarization_id(ddid as usize)? != 0
        {
            return Err(format!("fixture DDID {ddid} binding changed").into());
        }
    }
    let row_selection = ms.selected_observation_row_selection(&ddids, Some(&[0]), None, None)?;
    let content_budget = SelectedObservationContentBudget::new(64 << 20, 2, 4);
    let selection_io = MsSelectionIoBudget {
        available_bytes: content_budget.available_bytes(),
        maximum_live_blocks: content_budget.maximum_live_blocks(),
        requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
        storage_alignment_rows: None,
    };
    let mut rows = SelectedRowsBuilder::with_data_description_capacity(
        u64::try_from(ms.row_count())?,
        ddids.len(),
    );
    let mut row_error = None;
    let mut first_time = None;
    let mut physical_rows = Vec::new();
    ms.visit_selected_observation_rows(&row_selection, selection_io, |row| {
        first_time.get_or_insert(row.time_mjd_seconds());
        physical_rows.push(row.physical_row());
        if row_error.is_none() {
            row_error = rows
                .push(SelectedMainRow::new(
                    u64::try_from(row.physical_row()).expect("MS row fits u64"),
                    u32::try_from(row.data_description_id()).expect("validated nonnegative DDID"),
                ))
                .err();
        }
    })?;
    if let Some(error) = row_error {
        return Err(error.into());
    }
    let rows = rows.finish();
    if rows.selected_row_count() != SELECTED_ROWS as u64
        || physical_rows.iter().copied().ne(0..SELECTED_ROWS)
    {
        return Err("fixture selection is not exactly physical rows 0 through 23".into());
    }

    let mut spw_selections = Vec::new();
    let channels = (0..CHANNELS)
        .map(u32::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    for spw in [0_usize, 1] {
        if spectral_window.chan_freq(spw)?.len() != CHANNELS {
            return Err(format!("SPW {spw} no longer has 16 channels").into());
        }
        spw_selections.push(SpectralWindowSelection::new(spw as u32, channels.clone()));
    }
    let polarization_ids = row_selection
        .data_descriptions()
        .iter()
        .map(|description| description.polarization_id())
        .collect::<BTreeSet<_>>();
    let mut correlation_selections = Vec::new();
    for polarization_id in polarization_ids {
        let codes = polarization.corr_type(usize::try_from(polarization_id)?)?;
        let products = codes
            .iter()
            .enumerate()
            .map(|(index, code)| {
                Ok(CorrelationProduct::new(
                    u32::try_from(index)?,
                    correlation_type(*code)?,
                ))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        if products.len() != CORRELATIONS {
            return Err("fixture no longer has four selected correlations".into());
        }
        correlation_selections.push(CorrelationSelection::new(polarization_id, products));
    }
    let observation_selection = ObservationSelection::new(
        rows,
        row_selection.rows().clone(),
        row_selection.data_descriptions().to_vec(),
        spw_selections,
        correlation_selections,
    );

    let phase = casa_ms::derived::engine::resolve_field_phase_direction_j2000(&ms, 0)?;
    let (right_ascension, declination) = phase.as_angles();
    let cell = 2.5 * std::f64::consts::PI / (180.0 * 3600.0);
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, right_ascension, declination),
        [64.0, 64.0],
        [-cell, cell],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let source_reference = FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(0)?)
        .ok_or("unsupported source frequency frame")?;
    if spectral_window.meas_freq_ref(1)? != spectral_window.meas_freq_ref(0)? {
        return Err("selected SPWs use different source frequency frames".into());
    }
    let source_frame = imaging_frequency_frame(source_reference)?;
    let frame_engine = casa_ms::derived::engine::MsCalEngine::new(&ms)?;
    let [x_metres, y_metres, z_metres] = frame_engine.observatory_position().as_itrf();
    let anchor = SpectralFrameAnchor::Conversion {
        epoch: Epoch::new(
            first_time.ok_or("selection has no rows")? / 86_400.0,
            imaging_time_scale(frame_engine.time_reference())?,
        ),
        direction: direction.reference_direction(),
        observatory_position: ItrfPosition::new(x_metres, y_metres, z_metres),
    };
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(IMAGE_SIZE, IMAGE_SIZE),
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
            FrequencyFrame::Lsrk,
            anchor,
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: REFERENCE_FREQUENCY_HZ,
                increment_hz: 1.0,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let request = SelectedObservationResolutionRequest::new(
        path.display().to_string(),
        LogicalIdentity::from_sha256([0x28; 32]),
        observation_selection,
        VisibilityColumn::Data,
        if ms.main_table().column_accessor("WEIGHT_SPECTRUM").is_ok() {
            WeightColumn::WeightSpectrum
        } else {
            WeightColumn::Weight
        },
        Vec::new(),
        ModelStateIdentity::Empty,
        content_budget,
        casa_ms::open_measures_runtime()?,
    );
    let resolved = resolve_selected_observation(request)?;
    let (snapshot, access) = resolved.into_parts();
    let observation = compile_observation(snapshot)?;
    let model_samples = IMAGE_SIZE * IMAGE_SIZE * 2;
    let problem = compile(ImagingRequest::new(
        specification()?,
        geometry,
        ProblemInputIdentities::new(observation),
        ModelLifecycleRequirements::new(
            ModelBounds::new(
                model_samples,
                model_samples,
                model_samples,
                model_samples,
                f64::MAX,
                f64::MAX,
            )?,
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
    ))?;
    access.certify_residency(&problem)?;
    let selected = access.open(&problem)?;
    Ok((problem, selected))
}

fn specification() -> Result<ProblemSpecification, Box<dyn Error>> {
    let validity = ProductValidityPolicies::new(
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
    );
    Ok(ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    casa_imaging_model::ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Taylor { terms: 2 },
            ReconstructionAlgorithm::Mtmfs,
            ReconstructionControls::new(1, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
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
            validity,
        ),
        ObservationTransactionRequirements::new(casa_imaging_model::ModelColumnWrite::Disabled),
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

fn record_basis(
    bits: &mut [u32],
    frequencies: &mut [f64],
    row: usize,
    channel: usize,
    frequency_hz: f64,
) -> Result<(), StreamError> {
    let cell = row * CHANNELS + channel;
    let frequency = &mut frequencies[cell];
    if frequency.is_nan() {
        *frequency = frequency_hz;
    } else if frequency.to_bits() != frequency_hz.to_bits() {
        return Err(StreamError::Contract(
            "correlations disagree on evaluated frequency",
        ));
    }
    let casa_frequency_hz = f64::from(frequency_hz as f32);
    let x = ((casa_frequency_hz - REFERENCE_FREQUENCY_HZ) / REFERENCE_FREQUENCY_HZ) as f32;
    for (term, value) in [1.0_f32, x].into_iter().enumerate() {
        let index = term * SELECTED_ROWS * CHANNELS + cell;
        if bits[index] == u32::MAX {
            bits[index] = value.to_bits();
        } else if bits[index] != value.to_bits() {
            return Err(StreamError::Contract(
                "correlations disagree on Taylor basis",
            ));
        }
    }
    Ok(())
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

fn copy_measurement_set(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source = entry.path();
        let destination = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_measurement_set(&source, &destination)?;
        } else if file_type.is_file() {
            fs::copy(source, destination)?;
        } else if file_type.is_symlink() {
            let target = fs::canonicalize(source)?;
            if target.is_dir() {
                copy_measurement_set(&target, &destination)?;
            } else {
                fs::copy(target, destination)?;
            }
        }
    }
    Ok(())
}
