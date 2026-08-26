// SPDX-License-Identifier: LGPL-3.0-or-later

//! MeasurementSet-facing application request for the native continuum surface.

use std::{
    collections::BTreeSet,
    ffi::CString,
    path::{Path, PathBuf},
};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection as CoordinateProjection, ProjectionType,
    SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_imaging_model::{
    AxisOrder, CentreLaws, CorrelationProduct, CorrelationSelection, CorrelationType,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame, ImageAxis, ImageDomainRole,
    ImageDomainSpec, ImageShape, InstrumentResponse, LogicalIdentity, MeasurementEquationContract,
    ModelBounds, ModelColumnWrite, ModelInnerProduct, ModelInputCommitment,
    ModelLifecycleRequirements, ModelStateIdentity, NumericPrecision, NumericalStage,
    NumericsContract, ObservationSelection, ObservationTransactionRequirements, PhaseCentreLaw,
    PointingCentreLaw, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, RestFrequency, RestoringBeamPolicy, ScientificContract, SelectedMainRow,
    SelectedRows, SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling,
    SpectralFrameAnchor, SpectralSampling, SpectralWcs, SpectralWindowSelection, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, UvwCoordinateLaw,
    VisibilityColumn as OwnerVisibilityColumn, VisibilityInnerProduct,
    WeightColumn as OwnerWeightColumn, WeightDensityScope, WeightingContract, WeightingScheme,
};
use casa_imaging_reconstruction::{
    MinorCycleStopReason, ReconstructionMaskPlan, WeightingExecutionLimits,
};
use casa_imaging_runtime::{
    BuildIdentity, ExecutionAttemptId, ExecutionReceiptStore, ImplementationRegistryId,
    PlannerCostModelProfileId, ProductionStorageProfile, ReceiptRetention, ResourceAuthority,
    ResourcePolicy, WorkImplementationId,
};
use casa_ms::{
    MeasurementSet, MsSelectionIoBudget, SelectedObservationContentBudget,
    SelectedObservationResolutionRequest, SelectedObservationRow, VisibilityDataColumn,
    parse_spw_selector, resolve_channel_selector_selection,
};
use casa_types::measures::{direction::DirectionRef, frequency::FrequencyRef};
use sha2::{Digest, Sha256};

use crate::{
    ApplicationDispatchError, ApplicationNative, ApplicationOutcome, ApplicationPublication,
    ApplicationRequest, ApplicationRuntime, CasaImageProductSink, TaskRequirement,
};

/// Native continuum reconstruction accepted by the application boundary.
#[derive(Clone, Debug, PartialEq)]
pub enum ContinuumAlgorithm {
    /// Produce dirty and derived products without a minor cycle.
    Dirty,
    /// Run the serial Högbom minor cycle and mandatory final major cycle.
    Hogbom,
    /// Request the Clark point-component minor cycle.
    Clark,
    /// Request a multiscale minor cycle with explicit pixel scales.
    Multiscale {
        /// Canonical scale sizes in image pixels.
        scales_px: Vec<f64>,
        /// CASA small-scale preference in `[0, 1]`.
        small_scale_bias: f64,
    },
    /// Request multi-term multi-frequency synthesis.
    Mtmfs {
        /// Number of Taylor coefficients.
        terms: usize,
    },
}

/// Native continuum visibility-weighting law.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ContinuumWeighting {
    /// Use natural visibility weights.
    Natural,
    /// Use uniform density weighting.
    Uniform,
    /// Use Briggs robust weighting.
    Briggs(f64),
    /// Use Briggs bandwidth-taper weighting.
    BriggsBandwidthTaper(f64),
}

/// Restoring-beam policy requested by the task surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuumBeamPolicy {
    /// Fit each plane independently.
    PerPlane,
    /// Use one common beam.
    Common,
}

/// Reconstruction-mask policy evaluated at the initial major-cycle boundary.
#[derive(Clone, Debug, PartialEq)]
pub enum ContinuumMask {
    /// Admit every valid model pixel.
    FullPlane,
    /// Admit the union of inclusive target-grid pixel boxes.
    Boxes(Vec<ContinuumMaskBox>),
    /// Generate CASA auto-multithreshold support from the current Normal State.
    AutoMultithresh(ContinuumAutoMaskControls),
}

/// Inclusive target-grid mask box accepted at the application boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContinuumMaskBox {
    /// Lower-left `[x, y]` pixel.
    pub blc: [usize; 2],
    /// Upper-right `[x, y]` pixel.
    pub trc: [usize; 2],
}

/// Auto-multithreshold values transported by thin frontends.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ContinuumAutoMaskControls {
    /// Sidelobe threshold multiplier.
    pub sidelobe_factor: f64,
    /// Robust-noise threshold multiplier.
    pub noise_factor: f64,
    /// Low-noise growth multiplier.
    pub low_noise_factor: f64,
    /// Negative-feature threshold multiplier.
    pub negative_factor: f64,
    /// Minimum component area as a beam fraction.
    pub minimum_beam_fraction: f64,
    /// Gaussian smoothing FWHM as a beam fraction.
    pub smooth_factor: f64,
    /// Smoothed-mask cutoff fraction.
    pub cut_threshold: f64,
    /// Four-connected growth bound.
    pub grow_iterations: usize,
    /// Percent-change channel-stop threshold; negative disables this stop.
    pub minimum_percent_change: f64,
}

/// Application projection of the native minor-cycle terminal reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuumStopReason {
    /// The absolute stopping threshold was reached.
    ThresholdReached,
    /// The requested component-update bound was reached.
    IterationBound,
    /// The accepted model-update envelope required a fresh major cycle.
    StalenessBound,
}

/// Canonical application input for one MeasurementSet-backed continuum run.
///
/// The frontend parses strings and maps task enums into this record. All
/// MeasurementSet inspection, scientific contract construction, resource
/// binding, and product publication remain owned by this crate.
#[derive(Clone, Debug)]
pub struct ContinuumImagingRequest {
    /// Input MeasurementSet.
    pub measurement_set: PathBuf,
    /// Output product prefix.
    pub image_name: PathBuf,
    /// Square image dimension.
    pub image_size: usize,
    /// Direction pixel size in arcseconds.
    pub cell_arcsec: f64,
    /// Optional selected field identifiers.
    pub field_ids: Option<Vec<i32>>,
    /// Optional UV-distance predicate.
    pub uv_range: Option<String>,
    /// Optional observing-intent predicate.
    pub intent: Option<String>,
    /// Optional selected data-description identifier.
    pub data_description: Option<i32>,
    /// Optional CASA spectral-window selector.
    pub spectral_window: Option<String>,
    /// Optional first selected source channel.
    pub channel_start: Option<usize>,
    /// Optional selected source-channel count.
    pub channel_count: Option<usize>,
    /// Optional explicit visibility column.
    pub data_column: Option<String>,
    /// Reconstruction algorithm.
    pub algorithm: ContinuumAlgorithm,
    /// Visibility-weighting law.
    pub weighting: ContinuumWeighting,
    /// Minor-cycle iteration limit.
    pub iterations: usize,
    /// Maximum component updates accepted in one minor cycle.
    pub cycle_iterations: usize,
    /// Maximum number of major cycles admitted by the controller contract.
    pub maximum_major_cycles: usize,
    /// Optional robust-RMS stopping multiplier.
    pub noise_sigma: Option<f64>,
    /// Minor-cycle gain.
    pub gain: f64,
    /// Absolute stopping threshold in Jy/beam.
    pub threshold_jy: f64,
    /// Restoring-beam fit cutoff.
    pub psf_cutoff: f32,
    /// Restoring-beam policy.
    pub beam_policy: ContinuumBeamPolicy,
    /// Model-update support policy.
    pub mask: ContinuumMask,
    /// Persist the exact final prediction into the MeasurementSet `MODEL_DATA` column.
    pub save_model_column: bool,
    /// Capability constraints derived by the task surface. Unsupported
    /// capabilities are rejected by the installed implementation registry
    /// before physical execution.
    pub task_requirements: Vec<TaskRequirement>,
}

/// Small presentation projection of one completed native continuum run.
pub struct ContinuumImagingResult {
    /// Authoritative application result.
    pub outcome: ApplicationOutcome,
    /// Number of accepted minor-cycle component updates.
    pub minor_iterations: usize,
    /// Scientific minor-cycle terminal reason, when a solve ran.
    pub minor_stop_reason: Option<ContinuumStopReason>,
    /// Exact compiler-planned conventional CASA member names.
    pub product_names: Vec<String>,
}

/// Prepare, execute, and publish one native continuum request.
pub fn execute_continuum(
    request: ContinuumImagingRequest,
) -> Result<ContinuumImagingResult, ApplicationDispatchError> {
    let prepared = prepare(request).map_err(ApplicationDispatchError::Preparation)?;
    let outcome = crate::execute(prepared)?;
    let minor = outcome.output.minor_cycle;
    let product_names = outcome
        .output
        .planned_products
        .members()
        .iter()
        .map(|member| member.name().to_string())
        .collect();
    Ok(ContinuumImagingResult {
        minor_iterations: minor.map_or(0, |value| value.iterations),
        minor_stop_reason: minor.map(|value| match value.stop_reason {
            MinorCycleStopReason::ThresholdReached => ContinuumStopReason::ThresholdReached,
            MinorCycleStopReason::IterationBound => ContinuumStopReason::IterationBound,
            MinorCycleStopReason::StalenessBound => ContinuumStopReason::StalenessBound,
        }),
        product_names,
        outcome,
    })
}

fn prepare(
    mut request: ContinuumImagingRequest,
) -> Result<ApplicationRequest<CasaImageProductSink>, crate::ApplicationError> {
    validate_request(&request)?;
    let ms = MeasurementSet::open(&request.measurement_set)?;
    let data_description = ms.data_description()?;
    let spectral_window = ms.spectral_window()?;
    let polarization = ms.polarization()?;
    let ddids = selected_data_descriptions(&request, &data_description)?;
    let row_selection = ms.selected_observation_row_selection(
        &ddids,
        request.field_ids.as_deref(),
        request.uv_range.as_deref(),
        request.intent.as_deref(),
    )?;
    let content_budget = SelectedObservationContentBudget::new(64 << 20, 2, 4);
    let mut compact_rows = Vec::new();
    let mut selected_ddid = None;
    let mut multiple_ddids = false;
    let mut selected_field = None;
    let mut multiple_fields = false;
    ms.visit_selected_observation_rows(
        &row_selection,
        MsSelectionIoBudget {
            available_bytes: content_budget.available_bytes(),
            maximum_live_blocks: content_budget.maximum_live_blocks(),
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| {
            multiple_ddids |= selected_ddid.is_some_and(|value| value != row.data_description_id());
            selected_ddid.get_or_insert(row.data_description_id());
            multiple_fields |= selected_field.is_some_and(|value| value != row.field_id());
            selected_field.get_or_insert(row.field_id());
            compact_rows.push(SelectedMainRow::new(
                u64::try_from(row.physical_row()).expect("row bounded by MS row count"),
                u32::try_from(row.data_description_id()).expect("validated nonnegative DDID"),
            ));
        },
    )?;
    if compact_rows.is_empty() {
        return Err(boxed("selection resolved to no rows"));
    }
    if multiple_ddids || multiple_fields {
        request
            .task_requirements
            .push(TaskRequirement::UnsupportedControls);
    }
    let ddid = usize::try_from(selected_ddid.expect("nonempty selection"))
        .map_err(|_| boxed("selected DATA_DESC_ID is negative"))?;
    let field_id = usize::try_from(selected_field.expect("nonempty selection"))
        .map_err(|_| boxed("selected FIELD_ID is negative"))?;
    let (spw_id, polarization_id) = data_description_binding(&data_description, ddid)?;
    let frequencies = spectral_window.chan_freq(spw_id)?;
    let channels = selected_channels(&request, spw_id, &frequencies)?;
    let selected_frequencies = channels
        .iter()
        .map(|channel| frequencies[*channel])
        .collect::<Vec<_>>();
    let reference_frequency =
        selected_frequencies.iter().sum::<f64>() / selected_frequencies.len() as f64;
    let correlation_codes = polarization.corr_type(polarization_id)?;
    let correlations = correlation_codes
        .iter()
        .enumerate()
        .map(|(index, code)| {
            Ok(CorrelationProduct::new(
                u32::try_from(index).map_err(|_| boxed("correlation index exceeds u32"))?,
                correlation_type(*code)?,
            ))
        })
        .collect::<Result<Vec<_>, crate::ApplicationError>>()?;
    let rows = SelectedRows::from_ordered_main_row_vec(
        u64::try_from(ms.row_count()).map_err(|_| boxed("MS row count exceeds u64"))?,
        compact_rows,
    )?;
    let observation_selection = ObservationSelection::new(
        rows,
        row_selection.rows().clone(),
        row_selection.data_descriptions().to_vec(),
        vec![SpectralWindowSelection::new(
            u32::try_from(spw_id).map_err(|_| boxed("SPW id exceeds u32"))?,
            channels
                .iter()
                .map(|channel| u32::try_from(*channel).map_err(|_| boxed("channel exceeds u32")))
                .collect::<Result<Vec<_>, _>>()?,
        )],
        vec![CorrelationSelection::new(
            u32::try_from(polarization_id).map_err(|_| boxed("polarization id exceeds u32"))?,
            correlations,
        )],
    );
    let phase = casa_ms::derived::engine::resolve_field_phase_direction_j2000(&ms, field_id)?;
    let (right_ascension, declination) = phase.as_angles();
    let direction = direction_spec(&request, right_ascension, declination);
    let frequency_reference =
        FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(spw_id)?)
            .unwrap_or(FrequencyRef::TOPO);
    let geometry = casa_imaging_model::GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(request.image_size, request.image_size),
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
                reference_frequency_hz: reference_frequency,
                increment_hz: 1.0,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let coordinates = image_coordinates(
        &request,
        [right_ascension, declination],
        frequency_reference,
        reference_frequency,
    );
    let native = production_storage_profile(&request, content_budget)
        .and_then(|profile| {
            profile.ok_or_else(|| {
                boxed("native continuum requires input and output on one filesystem")
            })
        })
        .and_then(|profile| runtime(&request, &profile))
        .map(|runtime| ApplicationNative {
            runtime,
            publication: ApplicationPublication {
                controls: casa_imaging_products::ContinuumProductControls::new(request.psf_cutoff)
                    .expect("validated PSF cutoff"),
                sink: CasaImageProductSink::new(request.image_name.clone(), coordinates),
            },
        });
    let digest = request_digest(&request, b"selection");
    Ok(ApplicationRequest {
        specification: specification(&request)?,
        geometry,
        model_lifecycle: ModelLifecycleRequirements::new(
            ModelBounds::new(
                request.image_size.saturating_mul(request.image_size),
                1,
                1,
                request.iterations.max(1),
                1.0e30,
                1.0e30,
            )?,
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
        mask: match request.mask {
            ContinuumMask::FullPlane => ReconstructionMaskPlan::FullPlane {
                coordinate: direction,
            },
            ContinuumMask::Boxes(boxes) => ReconstructionMaskPlan::Boxes {
                coordinate: direction,
                boxes: boxes
                    .into_iter()
                    .map(|region| casa_imaging_reconstruction::MaskBox::new(region.blc, region.trc))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            ContinuumMask::AutoMultithresh(controls) => ReconstructionMaskPlan::AutoMultithresh {
                coordinate: direction,
                controls: casa_imaging_reconstruction::AutoMultithreshControls {
                    sidelobe_factor: controls.sidelobe_factor,
                    noise_factor: controls.noise_factor,
                    low_noise_factor: controls.low_noise_factor,
                    negative_factor: controls.negative_factor,
                    minimum_beam_fraction: controls.minimum_beam_fraction,
                    smooth_factor: controls.smooth_factor,
                    cut_threshold: controls.cut_threshold,
                    grow_iterations: controls.grow_iterations,
                    minimum_percent_change: controls.minimum_percent_change,
                },
                completed_major_cycles: 0,
                cycle_threshold_reached: false,
                previous: None,
            },
        },
        observation: SelectedObservationResolutionRequest::new(
            request.measurement_set.display().to_string(),
            LogicalIdentity::from_sha256(digest),
            observation_selection,
            visibility_column(&ms, request.data_column.as_deref())?,
            if ms.main_table().column_accessor("WEIGHT_SPECTRUM").is_ok() {
                OwnerWeightColumn::WeightSpectrum
            } else {
                OwnerWeightColumn::Weight
            },
            Vec::new(),
            ModelStateIdentity::Empty,
            content_budget,
            casa_ms::open_measures_runtime()?,
        ),
        write_model_column: request.save_model_column,
        task_requirements: request.task_requirements,
        native,
    })
}

fn validate_request(request: &ContinuumImagingRequest) -> Result<(), crate::ApplicationError> {
    if request.image_size == 0
        || !request.cell_arcsec.is_finite()
        || request.cell_arcsec <= 0.0
        || !request.gain.is_finite()
        || !request.threshold_jy.is_finite()
        || !request.psf_cutoff.is_finite()
        || request.psf_cutoff <= 0.0
        || (request.algorithm != ContinuumAlgorithm::Dirty
            && (request.cycle_iterations == 0 || request.maximum_major_cycles == 0))
        || request
            .noise_sigma
            .is_some_and(|sigma| !sigma.is_finite() || sigma < 0.0)
    {
        return Err(boxed(
            "native continuum geometry and controls must be finite and positive",
        ));
    }
    if request.save_model_column && request.algorithm == ContinuumAlgorithm::Dirty {
        return Err(boxed(
            "MODEL_DATA persistence requires a solved final model, not a dirty-only request",
        ));
    }
    Ok(())
}

fn selected_data_descriptions(
    request: &ContinuumImagingRequest,
    table: &casa_ms::MsDataDescription<'_>,
) -> Result<Vec<i32>, crate::ApplicationError> {
    if let Some(ddid) = request.data_description {
        return Ok(vec![ddid]);
    }
    let selected_spws = request
        .spectral_window
        .as_deref()
        .map(parse_spw_selector)
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .map(|selector| selector.spw_id)
        .collect::<BTreeSet<_>>();
    let mut ddids = Vec::new();
    for row in 0..table.row_count() {
        let spw = table.spectral_window_id(row)?;
        let polarization = table.polarization_id(row)?;
        if spw >= 0
            && polarization >= 0
            && (selected_spws.is_empty() || selected_spws.contains(&spw))
        {
            ddids.push(i32::try_from(row).map_err(|_| boxed("DDID exceeds i32"))?);
        }
    }
    if ddids.is_empty() {
        return Err(boxed("selection resolved to no data descriptions"));
    }
    Ok(ddids)
}

fn data_description_binding(
    table: &casa_ms::MsDataDescription<'_>,
    ddid: usize,
) -> Result<(usize, usize), crate::ApplicationError> {
    let spw = table.spectral_window_id(ddid)?;
    let polarization = table.polarization_id(ddid)?;
    Ok((
        usize::try_from(spw).map_err(|_| boxed("selected DDID has a negative SPW id"))?,
        usize::try_from(polarization)
            .map_err(|_| boxed("selected DDID has a negative polarization id"))?,
    ))
}

fn selected_channels(
    request: &ContinuumImagingRequest,
    spw_id: usize,
    frequencies: &[f64],
) -> Result<Vec<usize>, crate::ApplicationError> {
    if let Some(text) = request.spectral_window.as_deref() {
        if let Some(selector) = parse_spw_selector(text)?
            .into_iter()
            .find(|selector| usize::try_from(selector.spw_id).ok() == Some(spw_id))
            .and_then(|selector| selector.channels)
        {
            return Ok(resolve_channel_selector_selection(frequencies, &selector)?.indices);
        }
    }
    let start = request.channel_start.unwrap_or(0);
    let count = request
        .channel_count
        .unwrap_or_else(|| frequencies.len().saturating_sub(start));
    let end = start
        .checked_add(count)
        .ok_or_else(|| boxed("channel range overflows usize"))?;
    if count == 0 || end > frequencies.len() {
        return Err(boxed("selected channel range is empty or out of bounds"));
    }
    Ok((start..end).collect())
}

fn direction_spec(
    request: &ContinuumImagingRequest,
    right_ascension: f64,
    declination: f64,
) -> DirectionCoordinateSpec {
    let cell = request.cell_arcsec * std::f64::consts::PI / (180.0 * 3600.0);
    let reference_pixel = image_reference_pixel(request.image_size);
    DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, right_ascension, declination),
        [reference_pixel, reference_pixel],
        [-cell, cell],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    )
}

fn image_coordinates(
    request: &ContinuumImagingRequest,
    phase: [f64; 2],
    frequency_reference: FrequencyRef,
    reference_frequency: f64,
) -> CoordinateSystem {
    let cell = request.cell_arcsec * std::f64::consts::PI / (180.0 * 3600.0);
    let reference_pixel = image_reference_pixel(request.image_size);
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        CoordinateProjection::new(ProjectionType::SIN),
        phase,
        [-cell, cell],
        [reference_pixel, reference_pixel],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        frequency_reference,
        reference_frequency,
        1.0,
        0.0,
        reference_frequency,
    ));
    coordinates
}

fn image_reference_pixel(image_size: usize) -> f64 {
    image_size as f64 / 2.0
}

fn specification(
    request: &ContinuumImagingRequest,
) -> Result<ProblemSpecification, crate::ApplicationError> {
    let (basis, algorithm) = match &request.algorithm {
        ContinuumAlgorithm::Dirty => (
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Dirty,
        ),
        ContinuumAlgorithm::Hogbom => (
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Hogbom,
        ),
        ContinuumAlgorithm::Clark => (
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Clark,
        ),
        ContinuumAlgorithm::Multiscale {
            scales_px,
            small_scale_bias,
        } => (
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Multiscale {
                scales_px: scales_px.clone(),
                small_scale_bias: *small_scale_bias,
            },
        ),
        ContinuumAlgorithm::Mtmfs { terms } => (
            ReconstructionBasis::Taylor { terms: *terms },
            ReconstructionAlgorithm::Mtmfs,
        ),
    };
    let (weighting, density) = match request.weighting {
        ContinuumWeighting::Natural => {
            (WeightingScheme::Natural, WeightDensityScope::NotApplicable)
        }
        ContinuumWeighting::Uniform => (
            WeightingScheme::Uniform,
            WeightDensityScope::GlobalSelection,
        ),
        ContinuumWeighting::Briggs(robust) => (
            WeightingScheme::Briggs { robust },
            WeightDensityScope::GlobalSelection,
        ),
        ContinuumWeighting::BriggsBandwidthTaper(robust) => (
            WeightingScheme::BriggsBandwidthTaper { robust },
            WeightDensityScope::GlobalSelection,
        ),
    };
    Ok(ProblemSpecification::new(
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
            basis,
            algorithm.clone(),
            if algorithm == ReconstructionAlgorithm::Dirty {
                ReconstructionControls::new(0, 1.0, 0.0)
            } else {
                let controls = ReconstructionControls::new(
                    request.iterations,
                    request.gain,
                    request.threshold_jy,
                )
                .with_maximum_model_update(1.0e30)
                .with_cycle_limits(request.cycle_iterations, request.maximum_major_cycles);
                request
                    .noise_sigma
                    .map_or(controls, |sigma| controls.with_noise_sigma(sigma))
            },
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(weighting, density),
        ProductRequirements::new(
            vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::SumWeights,
            ],
            ProductNormalization::UnitResponse,
            match request.beam_policy {
                ContinuumBeamPolicy::PerPlane => RestoringBeamPolicy::PerPlane,
                ContinuumBeamPolicy::Common => RestoringBeamPolicy::Common,
            },
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
        ObservationTransactionRequirements::new(if request.save_model_column {
            ModelColumnWrite::SelectedRows
        } else {
            ModelColumnWrite::Disabled
        }),
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

fn visibility_column(
    ms: &MeasurementSet,
    explicit: Option<&str>,
) -> Result<OwnerVisibilityColumn, crate::ApplicationError> {
    let column = match explicit.map(str::to_ascii_uppercase).as_deref() {
        Some("DATA") => VisibilityDataColumn::Data,
        Some("CORRECTED_DATA" | "CORRECTED") => VisibilityDataColumn::CorrectedData,
        Some("MODEL_DATA" | "MODEL") => {
            return Err(boxed("MODEL_DATA is not an observation input"));
        }
        Some(other) => return Err(boxed(format!("unsupported visibility column {other:?}"))),
        None if ms.data_column(VisibilityDataColumn::CorrectedData).is_ok() => {
            VisibilityDataColumn::CorrectedData
        }
        None if ms.data_column(VisibilityDataColumn::Data).is_ok() => VisibilityDataColumn::Data,
        None => return Err(boxed("MS has neither CORRECTED_DATA nor DATA")),
    };
    Ok(match column {
        VisibilityDataColumn::Data => OwnerVisibilityColumn::Data,
        VisibilityDataColumn::CorrectedData => OwnerVisibilityColumn::CorrectedData,
        VisibilityDataColumn::ModelData => unreachable!("rejected above"),
    })
}

fn correlation_type(code: i32) -> Result<CorrelationType, crate::ApplicationError> {
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
        13 => MixedRx,
        14 => MixedRy,
        15 => MixedLx,
        16 => MixedLy,
        17 => MixedXr,
        18 => MixedXl,
        19 => MixedYr,
        20 => MixedYl,
        21 => QuasiOrthogonalPp,
        22 => QuasiOrthogonalPq,
        23 => QuasiOrthogonalQp,
        24 => QuasiOrthogonalQq,
        25 => RightCircular,
        26 => LeftCircular,
        27 => Linear,
        28 => PolarizedIntensity,
        29 => LinearPolarizedIntensity,
        30 => FractionalPolarizedIntensity,
        31 => FractionalLinearPolarizedIntensity,
        32 => PolarizationAngle,
        _ => return Err(boxed(format!("unsupported correlation code {code}"))),
    })
}

fn production_storage_profile(
    request: &ContinuumImagingRequest,
    content_budget: SelectedObservationContentBudget,
) -> Result<Option<ProductionStorageProfile>, crate::ApplicationError> {
    let output_parent = request
        .image_name
        .parent()
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(output_parent)?;
    let output_root = filesystem_root(&output_parent.canonicalize()?)?;
    let input_root = filesystem_root(&request.measurement_set.canonicalize()?)?;
    if output_root != input_root {
        return Ok(None);
    }
    let (capacity, available) = filesystem_capacity(&output_root)?;
    let read_rate = positive_environment("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND")?;
    let write_rate = positive_environment("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND")?;
    Ok(Some(ProductionStorageProfile::new(
        output_root,
        capacity,
        available,
        read_rate,
        write_rate,
        u64::try_from(content_budget.maximum_live_blocks()).unwrap_or(u64::MAX),
        2,
    )?))
}

fn runtime(
    request: &ContinuumImagingRequest,
    profile: &ProductionStorageProfile,
) -> Result<ApplicationRuntime, crate::ApplicationError> {
    let digest = request_digest(request, b"attempt");
    let receipts = request
        .image_name
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".casa-rs-imaging-receipts");
    Ok(ApplicationRuntime {
        registry: ImplementationRegistryId::from_sha256(hash(b"serial-continuum-registry")),
        implementation: WorkImplementationId::new("serial-continuum-cpu-v1"),
        weighting_limits: WeightingExecutionLimits::new(4096, 1)?,
        stage_nanos: 1_000_000,
        minor_cycle_bytes: u64::try_from(
            request
                .image_size
                .saturating_mul(request.image_size)
                .saturating_mul(16),
        )
        .unwrap_or(u64::MAX),
        storage_io: profile.io_resources(),
        confidence_parts_per_million: 900_000,
        resource_policy: ResourcePolicy::Balanced,
        cost_model: PlannerCostModelProfileId::from_sha256(hash(b"serial-continuum-cost-v1"))
            .bootstrap(),
        authority: ResourceAuthority::production_with_storage_profile(profile)?.clone(),
        receipts: ExecutionReceiptStore::new(receipts, ReceiptRetention::new(128, 64 << 20)?)?,
        build: BuildIdentity::from_sha256(hash(env!("CARGO_PKG_VERSION").as_bytes())),
        attempts: [
            ExecutionAttemptId::from_sha256(scoped(digest, 0)),
            ExecutionAttemptId::from_sha256(scoped(digest, 1)),
            ExecutionAttemptId::from_sha256(scoped(digest, 2)),
        ],
    })
}

#[cfg(unix)]
fn filesystem_root(path: &Path) -> Result<PathBuf, crate::ApplicationError> {
    use std::os::unix::fs::MetadataExt;
    let device = std::fs::metadata(path)?.dev();
    let mut root = path.to_path_buf();
    while let Some(parent) = root.parent() {
        if std::fs::metadata(parent)?.dev() != device {
            break;
        }
        root = parent.to_path_buf();
    }
    Ok(root)
}

#[cfg(not(unix))]
fn filesystem_root(_path: &Path) -> Result<PathBuf, crate::ApplicationError> {
    Err(boxed(
        "filesystem-root detection is unavailable on this platform",
    ))
}

#[cfg(unix)]
fn block_count_to_u64<T: Into<u64>>(blocks: T) -> u64 {
    blocks.into()
}

#[cfg(unix)]
fn filesystem_capacity(root: &Path) -> Result<(u64, u64), crate::ApplicationError> {
    use std::os::unix::ffi::OsStrExt;
    let root = CString::new(root.as_os_str().as_bytes())?;
    let mut statistics = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: the path is live and NUL-terminated, and the output allocation is valid.
    if unsafe { libc::statvfs(root.as_ptr(), statistics.as_mut_ptr()) } != 0 {
        return Err(Box::new(std::io::Error::last_os_error()));
    }
    // SAFETY: successful statvfs initialized the complete value.
    let statistics = unsafe { statistics.assume_init() };
    let block_size = statistics.f_frsize;
    Ok((
        block_count_to_u64(statistics.f_blocks).saturating_mul(block_size),
        block_count_to_u64(statistics.f_bavail).saturating_mul(block_size),
    ))
}

#[cfg(not(unix))]
fn filesystem_capacity(_root: &Path) -> Result<(u64, u64), crate::ApplicationError> {
    Err(boxed(
        "filesystem capacity detection is unavailable on this platform",
    ))
}

fn positive_environment(name: &str) -> Result<u64, crate::ApplicationError> {
    std::env::var(name)
        .map_err(|_| boxed(format!("native continuum requires {name}")))?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| boxed(format!("{name} must be a positive integer")))
}

fn request_digest(request: &ContinuumImagingRequest, domain: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(request.measurement_set.as_os_str().as_encoded_bytes());
    hasher.update(request.image_name.as_os_str().as_encoded_bytes());
    hasher.update(request.image_size.to_le_bytes());
    hasher.update(request.cell_arcsec.to_bits().to_le_bytes());
    hasher.finalize().into()
}

fn hash(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

fn scoped(mut digest: [u8; 32], scope: u8) -> [u8; 32] {
    digest[0] ^= scope;
    digest
}

fn boxed(message: impl Into<String>) -> crate::ApplicationError {
    Box::new(std::io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use super::image_reference_pixel;

    #[test]
    fn casa_direction_reference_pixel_uses_half_the_image_extent() {
        assert_eq!(image_reference_pixel(16), 8.0);
        assert_eq!(image_reference_pixel(15), 7.5);
    }
}
