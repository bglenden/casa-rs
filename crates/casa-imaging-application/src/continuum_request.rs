// SPDX-License-Identifier: LGPL-3.0-or-later

//! MeasurementSet-facing application request for the native continuum surface.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    path::{Path, PathBuf},
};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
    Projection as CoordinateProjection, ProjectionType, SpectralCoordinate, StokesCoordinate,
    StokesType,
};
use casa_images::AnyPagedImage;
use casa_imaging_model::{
    AxisOrder, CentreLaws, ContinuumChannelRole, ContinuumChannelUse, ContinuumFitRule,
    CorrelationProduct, CorrelationSelection, CorrelationType, DeclaredInnerProducts,
    DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame, DopplerConvention, Epoch, FacetLayout,
    FiniteValuePolicy, FrequencyFrame, HogbomIterationAccounting, ImageAxis, ImageDomainRole,
    ImageDomainSpec, ImageShape, InstrumentResponse, ItrfPosition, LogicalIdentity,
    MeasurementEquationContract, ModelBounds, ModelColumnWrite, ModelInnerProduct,
    ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity, NumericPrecision,
    NumericalStage, NumericsContract, ObservationSelection, ObservationTransactionRequirements,
    PhaseCentreLaw, PointingCentreLaw, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, RestFrequency, RestoringBeamPolicy,
    ScientificContract, SelectedMainRow, SelectedRowsBuilder, SequentialContinuumTransform,
    SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeScale, UvwCoordinateLaw,
    VisibilityColumn as OwnerVisibilityColumn, VisibilityInnerProduct,
    WeightColumn as OwnerWeightColumn, WeightDensityScope, WeightingContract, WeightingScheme,
};
use casa_imaging_reconstruction::{
    ReconstructionMaskPlan, WeightingExecutionLimits, minor_cycle_workspace_bytes,
};
use casa_imaging_runtime::{
    BuildIdentity, ExecutionAttemptId, ExecutionReceiptStore, GriddedNormalReplayStorage,
    ImplementationRegistryId, PlannerCostModelProfileId, ProductionStorageProfile,
    ReceiptRetention, ResourceAuthority, ResourceOverride, ResourcePolicy, WorkImplementationId,
};
use casa_ms::{
    CubeAxisConfig, CubeInterpolation, CubeSpectralSetup, MeasurementSet, MsSelectionIoBudget,
    SelectedObservationContentBudget, SelectedObservationResolutionRequest, SelectedObservationRow,
    VisibilityDataColumn, parse_spw_selector, resolve_channel_selector_selection,
};
use casa_types::measures::{
    direction::{DirectionRef, MDirection},
    doppler::DopplerRef,
    epoch::EpochRef,
    frequency::FrequencyRef,
};
use sha2::{Digest, Sha256};

use crate::continuum_domains::read_outlier_domains;
use crate::{
    ApplicationDispatchError, ApplicationNative, ApplicationOutcome, ApplicationPublication,
    ApplicationRequest, ApplicationRuntime, CasaImageDomainOutput, CasaImageProductSink,
    TaskRequirement,
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
        /// Canonical scale sizes in image pixels.
        scales_px: Vec<f64>,
        /// CASA small-scale preference in `[0, 1]`.
        small_scale_bias: f64,
    },
    /// Reconstruct one smooth continuum basis and channel-local line basis jointly.
    JointContinuumLine {
        /// Number of smooth continuum coefficients.
        continuum_terms: usize,
        /// Channels where line coefficients are structurally absent.
        continuum_anchor_channels: Vec<usize>,
        /// Channels carrying one channel-local line coefficient each.
        line_channels: Vec<usize>,
        /// Maximum admitted active-block condition estimate.
        maximum_condition_number: f64,
        /// Canonical scale sizes in image pixels.
        scales_px: Vec<f64>,
        /// CASA small-scale preference in `[0, 1]`.
        small_scale_bias: f64,
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
    /// Independently commit continuum and line spatial support for a joint solve.
    Coupled {
        /// Continuum-component support.
        continuum: Box<ContinuumMask>,
        /// Line-component support.
        line: Box<ContinuumMask>,
    },
    /// Admit every valid model pixel.
    FullPlane,
    /// Admit the union of inclusive target-grid pixel boxes.
    Boxes(Vec<ContinuumMaskBox>),
    /// Reproject non-zero pixels from a CASA image mask onto the model grid.
    Image(PathBuf),
    /// Exact row-major target-grid support compiled from a task-domain region.
    PixelSupport(Box<[bool]>),
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
    /// The multiscale component sequence diverged after prior progress.
    MultiscaleDivergence,
}

/// Spectral reconstruction shape selected by the thin task surface.
#[derive(Clone, Debug, PartialEq)]
pub enum SpectralImagingMode {
    /// One constant-basis continuum plane over every selected source channel.
    Continuum,
    /// Preserve one exact output channel for every selected source channel.
    JointContinuumLine,
    /// One independently reconstructed model plane per output cube channel.
    Cube {
        /// CASA-compatible output-axis and interpolation controls.
        axis: CubeAxisConfig,
        /// Requested output channel count; `None` uses every source channel.
        output_channels: Option<usize>,
    },
}

/// Visibility-domain continuum subtraction composed before cube sampling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisibilityContinuumSubtraction {
    /// CASA-style line-free channel selector, for example `0:0~239;281~383`.
    pub fit_spw: String,
    /// Polynomial order fitted independently to real and imaginary visibilities.
    pub fit_order: usize,
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
    /// Number of regular image facets along each direction axis.
    pub facets: usize,
    /// Direction pixel size in arcseconds.
    pub cell_arcsec: f64,
    /// Optional main-chart phase-centre field selected from the MeasurementSet.
    pub phase_center_field: Option<i32>,
    /// Optional CASA-style main-chart phase-centre literal.
    pub phase_center: Option<String>,
    /// Optional CASA outlier definition file compiled into additional image domains.
    pub outlier_file: Option<PathBuf>,
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
    /// Continuum or channel-local cube reconstruction.
    pub spectral_mode: SpectralImagingMode,
    /// Optional sequential visibility-domain continuum subtraction for line cubes.
    pub continuum_subtraction: Option<VisibilityContinuumSubtraction>,
    /// Optional explicit visibility column.
    pub data_column: Option<String>,
    /// Ordered Stokes or raw-correlation reconstruction coordinates.
    pub polarizations: Vec<PolarizationCoordinate>,
    /// Reconstruction algorithm.
    pub algorithm: ContinuumAlgorithm,
    /// Visibility-weighting law.
    pub weighting: ContinuumWeighting,
    /// Reported total minor-iteration budget.
    pub iterations: usize,
    /// Reported component budget for one minor cycle.
    pub cycle_iterations: usize,
    /// Högbom's strict or CASA-inclusive actual-vs-reported accounting policy.
    pub hogbom_iteration_accounting: HogbomIterationAccounting,
    /// Optional maximum number of major cycles admitted by the controller contract.
    pub maximum_major_cycles: Option<usize>,
    /// Optional robust-RMS stopping multiplier.
    pub noise_sigma: Option<f64>,
    /// PSF-sidelobe multiplier used to derive each cycle threshold.
    pub cycle_factor: f64,
    /// Lower clamp for the PSF fraction in the cycle threshold.
    pub minimum_psf_fraction: f64,
    /// Upper clamp for the PSF fraction in the cycle threshold.
    pub maximum_psf_fraction: f64,
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
    /// Persist continuum-subtracted output-role observations into existing `CORRECTED_DATA`.
    pub save_continuum_residual: bool,
    /// Publish the scalar-response primary-beam Taylor family.
    pub write_primary_beam: bool,
    /// Publish primary-beam-corrected restored Taylor images.
    pub pbcor: bool,
    /// Capability constraints derived by the task surface. Unsupported
    /// capabilities are rejected by the installed implementation registry
    /// before physical execution.
    pub task_requirements: Vec<TaskRequirement>,
    /// User-selected host-use policy carried unchanged into physical planning.
    pub resource_policy: ResourcePolicy,
}

/// Small presentation projection of one completed native continuum run.
pub struct ContinuumImagingResult {
    /// Authoritative application result.
    pub outcome: ApplicationOutcome,
    /// Component count charged to the reported task/controller budget.
    pub minor_iterations: usize,
    /// Number of minor-cycle components actually applied.
    pub actual_minor_iterations: usize,
    /// Scientific minor-cycle terminal reason, when a solve ran.
    pub minor_stop_reason: Option<ContinuumStopReason>,
    /// Ordered owner diagnostics for every executed minor cycle.
    pub minor_cycles: Vec<crate::NativeMinorCycleOutcome>,
    /// Exact compiler-planned conventional CASA member names.
    pub product_names: Vec<String>,
}

/// Prepare, execute, and publish one native continuum request.
pub fn execute_continuum(
    request: ContinuumImagingRequest,
) -> Result<ContinuumImagingResult, ApplicationDispatchError> {
    let prepared = prepare(request).map_err(ApplicationDispatchError::Preparation)?;
    let outcome = crate::execute(prepared)?;
    let minor_cycles = outcome.output.minor_cycles.clone();
    let minor = minor_cycles.last();
    let minor_iterations = outcome.output.total_minor_iterations;
    let actual_minor_iterations = outcome.output.total_actual_minor_iterations;
    let product_names = outcome
        .output
        .planned_products
        .members()
        .iter()
        .map(|member| member.name().to_string())
        .collect();
    Ok(ContinuumImagingResult {
        minor_iterations,
        actual_minor_iterations,
        minor_stop_reason: minor.map(|value| match value.stop_reason {
            crate::NativeMinorCycleStopReason::ThresholdReached => {
                ContinuumStopReason::ThresholdReached
            }
            crate::NativeMinorCycleStopReason::IterationBound => {
                ContinuumStopReason::IterationBound
            }
            crate::NativeMinorCycleStopReason::StalenessBound => {
                ContinuumStopReason::StalenessBound
            }
            crate::NativeMinorCycleStopReason::MultiscaleDivergence => {
                ContinuumStopReason::MultiscaleDivergence
            }
        }),
        minor_cycles,
        product_names,
        outcome,
    })
}

struct PreparedSpectralAxis {
    selected_source_channels: BTreeMap<usize, Vec<usize>>,
    source_frame: FrequencyFrame,
    output_frequency_reference: FrequencyRef,
    output_frame: FrequencyFrame,
    anchor: SpectralFrameAnchor,
    wcs: SpectralWcs,
    rest_frequency: RestFrequency,
    doppler: DopplerConvention,
    sampling: SpectralSamplingLaw,
    basis: ReconstructionBasis,
    output_channels: usize,
    reference_frequency_hz: f64,
    increment_hz: f64,
}

struct PreparedImageDomain {
    role: ImageDomainRole,
    output: PathBuf,
    image_size: usize,
    direction: DirectionCoordinateSpec,
    coordinates: CoordinateSystem,
    mask: ContinuumMask,
}

struct SourceSpectralWindow {
    spw_id: usize,
    frequencies_hz: Vec<f64>,
    channel_widths_hz: Vec<f64>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_spectral_axis(
    request: &ContinuumImagingRequest,
    spectral_windows: &[SourceSpectralWindow],
    source_frequency_reference: FrequencyRef,
    anchor_time_mjd_seconds: f64,
    time_bounds_mjd_seconds: [f64; 2],
    field_id: usize,
    phase: MDirection,
    direction: DirectionCoordinateSpec,
    frame_engine: &casa_ms::derived::engine::MsCalEngine,
) -> Result<PreparedSpectralAxis, crate::ApplicationError> {
    let source_frame = imaging_frequency_frame(source_frequency_reference)?;
    match &request.spectral_mode {
        SpectralImagingMode::Continuum => {
            let mut selected_source_channels = BTreeMap::new();
            let mut source_frequency_sum = 0.0;
            let mut source_frequency_count = 0_usize;
            for window in spectral_windows {
                let selected = selected_channels(request, window.spw_id, &window.frequencies_hz)?;
                source_frequency_sum += selected
                    .iter()
                    .map(|channel| window.frequencies_hz[*channel])
                    .sum::<f64>();
                source_frequency_count += selected.len();
                selected_source_channels.insert(window.spw_id, selected);
            }
            let source_reference_frequency = source_frequency_sum / source_frequency_count as f64;
            let output_frequency_reference = FrequencyRef::LSRK;
            let output_frame = imaging_frequency_frame(output_frequency_reference)?;
            let reference_frequency_hz = casa_ms::convert_frequency_to_frame(
                source_frequency_reference,
                output_frequency_reference,
                source_reference_frequency,
                anchor_time_mjd_seconds,
                field_id,
                frame_engine,
            )?;
            Ok(PreparedSpectralAxis {
                selected_source_channels,
                source_frame,
                output_frequency_reference,
                output_frame,
                anchor: spectral_frame_anchor(
                    source_frame,
                    output_frame,
                    anchor_time_mjd_seconds,
                    direction,
                    frame_engine,
                )?,
                wcs: SpectralWcs::Linear {
                    channels: 1,
                    reference_pixel: 0.0,
                    reference_frequency_hz,
                    increment_hz: 1.0,
                },
                rest_frequency: RestFrequency::NotApplicable,
                doppler: DopplerConvention::NotApplicable,
                sampling: SpectralSamplingLaw::IDENTITY,
                basis: ReconstructionBasis::Constant,
                output_channels: 1,
                reference_frequency_hz,
                increment_hz: 1.0,
            })
        }
        SpectralImagingMode::JointContinuumLine => {
            let [window] = spectral_windows else {
                return Err(boxed(
                    "joint continuum-line imaging requires exactly one selected spectral window",
                ));
            };
            let selected = selected_channels(request, window.spw_id, &window.frequencies_hz)?;
            let frequencies = selected
                .iter()
                .map(|channel| window.frequencies_hz[*channel])
                .collect::<Vec<_>>();
            let reference_frequency_hz = frequencies[0];
            let increment_hz = if frequencies.len() > 1 {
                frequencies[1] - frequencies[0]
            } else {
                window.channel_widths_hz[selected[0]]
            };
            if !increment_hz.is_finite()
                || increment_hz == 0.0
                || frequencies.iter().enumerate().any(|(index, frequency)| {
                    let expected = reference_frequency_hz + index as f64 * increment_hz;
                    (*frequency - expected).abs() > expected.abs().max(1.0) * 1.0e-12
                })
            {
                return Err(boxed(
                    "joint continuum-line output requires a finite linear selected source axis",
                ));
            }
            Ok(PreparedSpectralAxis {
                selected_source_channels: BTreeMap::from([(window.spw_id, selected)]),
                source_frame,
                output_frequency_reference: source_frequency_reference,
                output_frame: source_frame,
                anchor: SpectralFrameAnchor::NotApplicable,
                wcs: SpectralWcs::Linear {
                    channels: frequencies.len(),
                    reference_pixel: 0.0,
                    reference_frequency_hz,
                    increment_hz,
                },
                rest_frequency: RestFrequency::NotApplicable,
                doppler: DopplerConvention::NotApplicable,
                sampling: SpectralSamplingLaw::IDENTITY,
                basis: ReconstructionBasis::ChannelLocal {
                    channels: frequencies.len(),
                },
                output_channels: frequencies.len(),
                reference_frequency_hz,
                increment_hz,
            })
        }
        SpectralImagingMode::Cube {
            axis,
            output_channels,
        } => {
            let [window] = spectral_windows else {
                return Err(boxed(
                    "native cube imaging currently requires exactly one selected spectral window",
                ));
            };
            let frequencies_hz = &window.frequencies_hz;
            let channel_widths_hz = &window.channel_widths_hz;
            let output_channels = output_channels.unwrap_or(frequencies_hz.len());
            let (setup, support) = CubeSpectralSetup::for_casa_cube_axis(
                source_frequency_reference,
                frequencies_hz,
                channel_widths_hz,
                output_channels,
                axis,
                anchor_time_mjd_seconds,
                field_id,
                Some(phase),
                time_bounds_mjd_seconds,
                frame_engine,
            )?;
            let mut selected_source_channels = support.indices;
            if let Some(explicit) = explicit_spw_channels(request, window.spw_id, frequencies_hz)? {
                let explicit = explicit.into_iter().collect::<BTreeSet<_>>();
                selected_source_channels.retain(|channel| explicit.contains(channel));
            }
            if selected_source_channels.is_empty() {
                return Err(boxed(
                    "cube axis and SPW selector have no common source channels",
                ));
            }
            let reference_frequency_hz = setup.output_channel_frequencies_hz[0];
            let increment_hz = if output_channels > 1 {
                setup.output_channel_frequencies_hz[1] - reference_frequency_hz
            } else {
                setup.output_channel_widths_hz[0]
            };
            if !increment_hz.is_finite() || increment_hz == 0.0 {
                return Err(boxed(
                    "cube output frequency increment must be finite and non-zero",
                ));
            }
            let output_frequency_reference = setup.output_freq_ref;
            let output_frame = imaging_frequency_frame(output_frequency_reference)?;
            let (rest_frequency, doppler) = match axis.rest_frequency_hz {
                None => (
                    RestFrequency::NotApplicable,
                    DopplerConvention::NotApplicable,
                ),
                Some(hertz) => (
                    RestFrequency::Line { hertz },
                    match axis.veltype {
                        DopplerRef::RADIO => DopplerConvention::Radio,
                        DopplerRef::Z => DopplerConvention::Optical,
                        DopplerRef::BETA => DopplerConvention::Relativistic,
                        DopplerRef::RATIO | DopplerRef::GAMMA => {
                            return Err(boxed("cube Doppler convention is not supported"));
                        }
                    },
                ),
            };
            let sampling = match setup.interpolation {
                CubeInterpolation::Nearest => SpectralSamplingLaw::NEAREST,
                CubeInterpolation::Linear => SpectralSamplingLaw::LINEAR,
                CubeInterpolation::Cubic => SpectralSamplingLaw::CUBIC,
            };
            Ok(PreparedSpectralAxis {
                selected_source_channels: BTreeMap::from([(
                    window.spw_id,
                    selected_source_channels,
                )]),
                source_frame,
                output_frequency_reference,
                output_frame,
                anchor: spectral_frame_anchor(
                    source_frame,
                    output_frame,
                    anchor_time_mjd_seconds,
                    direction,
                    frame_engine,
                )?,
                wcs: SpectralWcs::Linear {
                    channels: output_channels,
                    reference_pixel: 0.0,
                    reference_frequency_hz,
                    increment_hz,
                },
                rest_frequency,
                doppler,
                sampling,
                basis: ReconstructionBasis::ChannelLocal {
                    channels: output_channels,
                },
                output_channels,
                reference_frequency_hz,
                increment_hz,
            })
        }
    }
}

fn spectral_frame_anchor(
    source_frame: FrequencyFrame,
    output_frame: FrequencyFrame,
    anchor_time_mjd_seconds: f64,
    direction: DirectionCoordinateSpec,
    frame_engine: &casa_ms::derived::engine::MsCalEngine,
) -> Result<SpectralFrameAnchor, crate::ApplicationError> {
    if source_frame == output_frame {
        return Ok(SpectralFrameAnchor::NotApplicable);
    }
    let [x_metres, y_metres, z_metres] = frame_engine.observatory_position().as_itrf();
    Ok(SpectralFrameAnchor::Conversion {
        epoch: Epoch::new(
            anchor_time_mjd_seconds / 86_400.0,
            imaging_time_scale(frame_engine.time_reference())?,
        ),
        direction: direction.reference_direction(),
        observatory_position: ItrfPosition::new(x_metres, y_metres, z_metres),
    })
}

fn prepare(
    mut request: ContinuumImagingRequest,
) -> Result<ApplicationRequest<CasaImageProductSink>, crate::ApplicationError> {
    canonicalize_polarizations(&mut request.polarizations);
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
    let mut selected_rows = SelectedRowsBuilder::with_data_description_capacity(
        u64::try_from(ms.row_count()).map_err(|_| boxed("MS row count exceeds u64"))?,
        ddids.len(),
    );
    let mut selected_rows_error = None;
    let mut selected_ddids = BTreeSet::new();
    let mut selected_fields = BTreeSet::new();
    let mut first_selected_time_mjd_seconds = None;
    let mut selected_time_bounds_mjd_seconds = [f64::INFINITY, f64::NEG_INFINITY];
    let main_table = ms.main_table();
    let mut weight_spectrum_complete = main_table.column_accessor("WEIGHT_SPECTRUM").is_ok();
    let mut weight_spectrum_error = None;
    ms.visit_selected_observation_rows(
        &row_selection,
        MsSelectionIoBudget {
            available_bytes: content_budget.available_bytes(),
            maximum_live_blocks: content_budget.maximum_live_blocks(),
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| {
            if weight_spectrum_complete {
                match main_table.is_cell_defined(row.physical_row(), "WEIGHT_SPECTRUM") {
                    Ok(defined) => weight_spectrum_complete = defined,
                    Err(error) => weight_spectrum_error = Some(error),
                }
            }
            selected_ddids.insert(row.data_description_id());
            selected_fields.insert(row.field_id());
            first_selected_time_mjd_seconds.get_or_insert(row.time_mjd_seconds());
            selected_time_bounds_mjd_seconds[0] =
                selected_time_bounds_mjd_seconds[0].min(row.time_mjd_seconds());
            selected_time_bounds_mjd_seconds[1] =
                selected_time_bounds_mjd_seconds[1].max(row.time_mjd_seconds());
            if selected_rows_error.is_none() {
                selected_rows_error = selected_rows
                    .push(SelectedMainRow::new(
                        u64::try_from(row.physical_row()).expect("row bounded by MS row count"),
                        u32::try_from(row.data_description_id())
                            .expect("validated nonnegative DDID"),
                    ))
                    .err();
            }
        },
    )?;
    if let Some(error) = selected_rows_error {
        return Err(Box::new(error));
    }
    if let Some(error) = weight_spectrum_error {
        return Err(Box::new(error));
    }
    let rows = selected_rows.finish();
    if rows.selected_row_count() == 0 {
        return Err(boxed("selection resolved to no rows"));
    }
    let selected_field = request
        .phase_center_field
        .unwrap_or_else(|| *selected_fields.first().expect("nonempty selection"));
    if !selected_fields.contains(&selected_field) {
        return Err(boxed(format!(
            "phase-center FIELD_ID {selected_field} is not part of selected fields {selected_fields:?}"
        )));
    }
    let field_id =
        usize::try_from(selected_field).map_err(|_| boxed("selected FIELD_ID is negative"))?;
    let bindings = selected_ddids
        .into_iter()
        .map(|ddid| {
            usize::try_from(ddid)
                .map_err(|_| boxed("selected DATA_DESC_ID is negative"))
                .and_then(|ddid| data_description_binding(&data_description, ddid))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let spw_ids = bindings
        .iter()
        .map(|(spw_id, _)| *spw_id)
        .collect::<BTreeSet<_>>();
    let mut source_frequency_reference = None;
    let mut spectral_windows = Vec::with_capacity(spw_ids.len());
    for spw_id in spw_ids {
        let reference = FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(spw_id)?)
            .ok_or_else(|| boxed("selected SPW has an unsupported frequency frame"))?;
        if source_frequency_reference.is_some_and(|selected| selected != reference) {
            return Err(boxed(
                "selected spectral windows use different source frequency frames",
            ));
        }
        source_frequency_reference.get_or_insert(reference);
        spectral_windows.push(SourceSpectralWindow {
            spw_id,
            frequencies_hz: spectral_window.chan_freq(spw_id)?,
            channel_widths_hz: spectral_window.chan_width(spw_id)?,
        });
    }
    let phase = casa_ms::derived::engine::resolve_field_phase_direction_j2000(&ms, field_id)?;
    let default_main_direction = SkyDirection::new(
        DirectionFrame::J2000,
        phase.as_angles().0,
        phase.as_angles().1,
    );
    let main_direction = request
        .phase_center
        .as_deref()
        .map(parse_phase_center_direction)
        .transpose()?
        .unwrap_or(default_main_direction);
    let right_ascension = main_direction.longitude_rad();
    let declination = main_direction.latitude_rad();
    let direction = direction_spec(
        request.image_size,
        request.cell_arcsec,
        right_ascension,
        declination,
    );
    let frequency_reference = source_frequency_reference.expect("nonempty selected SPWs");
    let frame_engine = casa_ms::derived::engine::MsCalEngine::new(&ms)?;
    let anchor_time_mjd_seconds =
        first_selected_time_mjd_seconds.expect("nonempty selected row traversal");
    let prepared_spectral = prepare_spectral_axis(
        &request,
        &spectral_windows,
        frequency_reference,
        anchor_time_mjd_seconds,
        selected_time_bounds_mjd_seconds,
        field_id,
        phase,
        direction,
        &frame_engine,
    )?;
    let (continuum_transform, selected_source_channels) = if request.continuum_subtraction.is_some()
    {
        let [window] = spectral_windows.as_slice() else {
            return Err(boxed(
                "native continuum subtraction currently requires exactly one selected spectral window",
            ));
        };
        let selected = prepared_spectral
            .selected_source_channels
            .get(&window.spw_id)
            .expect("prepared selected SPW");
        let (transform, selected) = prepare_continuum_transform(
            request.continuum_subtraction.as_ref(),
            i32::try_from(field_id).map_err(|_| boxed("FIELD_ID exceeds i32"))?,
            window.spw_id,
            &window.frequencies_hz,
            selected,
        )?;
        (transform, BTreeMap::from([(window.spw_id, selected)]))
    } else {
        (None, prepared_spectral.selected_source_channels.clone())
    };
    let spectral_window_selections = selected_source_channels
        .iter()
        .map(|(spw_id, channels)| {
            Ok(SpectralWindowSelection::new(
                u32::try_from(*spw_id).map_err(|_| boxed("SPW id exceeds u32"))?,
                channels
                    .iter()
                    .map(|channel| {
                        u32::try_from(*channel).map_err(|_| boxed("channel exceeds u32"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        })
        .collect::<Result<Vec<_>, crate::ApplicationError>>()?;
    let correlation_selections = bindings
        .iter()
        .map(|(_, polarization_id)| *polarization_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|polarization_id| {
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
            Ok(CorrelationSelection::new(
                u32::try_from(polarization_id).map_err(|_| boxed("polarization id exceeds u32"))?,
                correlations,
            ))
        })
        .collect::<Result<Vec<_>, crate::ApplicationError>>()?;
    let observation_selection = ObservationSelection::new(
        rows,
        row_selection.rows().clone(),
        row_selection.data_descriptions().to_vec(),
        spectral_window_selections,
        correlation_selections,
    );
    let mut prepared_domains = vec![PreparedImageDomain {
        role: ImageDomainRole::Main,
        output: request.image_name.clone(),
        image_size: request.image_size,
        direction,
        coordinates: image_coordinates(
            request.image_size,
            request.cell_arcsec,
            [right_ascension, declination],
            &request.polarizations,
            prepared_spectral.output_frequency_reference,
            prepared_spectral.reference_frequency_hz,
            prepared_spectral.increment_hz,
        ),
        mask: request.mask.clone(),
    }];
    if let Some(path) = request.outlier_file.as_deref() {
        for input in read_outlier_domains(path, request.image_size, request.cell_arcsec)? {
            let centre = parse_phase_center_direction(&input.phase_center)?;
            let direction = direction_spec(
                input.image_size,
                input.cell_arcsec,
                centre.longitude_rad(),
                centre.latitude_rad(),
            );
            prepared_domains.push(PreparedImageDomain {
                role: ImageDomainRole::Outlier(input.name),
                output: input.output,
                image_size: input.image_size,
                direction,
                coordinates: image_coordinates(
                    input.image_size,
                    input.cell_arcsec,
                    [centre.longitude_rad(), centre.latitude_rad()],
                    &request.polarizations,
                    prepared_spectral.output_frequency_reference,
                    prepared_spectral.reference_frequency_hz,
                    prepared_spectral.increment_hz,
                ),
                mask: input.mask,
            });
        }
    }
    prepared_domains.sort_by(|left, right| left.role.cmp(&right.role));
    let geometry = casa_imaging_model::GeometryInput::new(
        prepared_domains
            .iter()
            .map(|domain| {
                ImageDomainSpec::new(
                    domain.role.clone(),
                    ImageShape::new(domain.image_size, domain.image_size),
                    domain.direction,
                    if request.facets == 1 || !matches!(domain.role, ImageDomainRole::Main) {
                        FacetLayout::Single
                    } else {
                        FacetLayout::Regular {
                            columns: request.facets,
                            rows: request.facets,
                        }
                    },
                    AxisOrder::new([
                        ImageAxis::DirectionLongitude,
                        ImageAxis::DirectionLatitude,
                        ImageAxis::Polarization,
                        ImageAxis::Spectral,
                    ]),
                )
            })
            .collect(),
        CentreLaws::new(
            PhaseCentreLaw::Fixed(direction.reference_direction()),
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::PhaseTrackingCentre,
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            prepared_spectral.source_frame,
            prepared_spectral.output_frame,
            prepared_spectral.anchor,
            prepared_spectral.wcs.clone(),
            prepared_spectral.rest_frequency,
            prepared_spectral.doppler,
        ),
    );
    let primary_beam_model = (request.write_primary_beam || request.pbcor)
        .then(|| standard_primary_beam_model(&ms))
        .transpose()?;
    let mut product_controls =
        casa_imaging_products::ContinuumProductControls::new(request.psf_cutoff)
            .expect("validated PSF cutoff");
    if let Some(model) = primary_beam_model {
        product_controls = product_controls.with_primary_beam_model(model);
    }
    let product_sink = CasaImageProductSink::for_domains(prepared_domains.iter().map(|domain| {
        CasaImageDomainOutput::new(
            domain.role.clone(),
            domain.output.clone(),
            domain.coordinates.clone(),
        )
    }))?;
    let native = production_storage_profile(&request, &prepared_domains, content_budget)
        .and_then(|profile| {
            profile.ok_or_else(|| {
                boxed("native continuum requires input and output on one filesystem")
            })
        })
        .and_then(|profile| runtime(&request, &prepared_domains, &profile))
        .map(|runtime| ApplicationNative {
            runtime,
            publication: ApplicationPublication {
                controls: product_controls,
                sink: product_sink,
            },
        });
    let digest = request_digest(&request, b"selection");
    let reconstruction_planes = match &request.algorithm {
        ContinuumAlgorithm::Mtmfs { terms, .. } => *terms,
        ContinuumAlgorithm::JointContinuumLine {
            continuum_terms,
            line_channels,
            ..
        } => continuum_terms.saturating_add(line_channels.len()),
        _ => prepared_spectral.output_channels,
    };
    let model_samples = prepared_domains
        .iter()
        .try_fold(0_usize, |total, domain| {
            model_plane_samples(domain.image_size)
                .checked_add(total)
                .ok_or_else(|| boxed("reconstruction model sample count overflowed"))
        })?
        .checked_mul(reconstruction_planes)
        .and_then(|samples| samples.checked_mul(request.polarizations.len()))
        .ok_or_else(|| boxed("reconstruction model sample count overflowed"))?;
    let specification = match continuum_transform {
        Some(transform) => {
            specification(&request, &prepared_spectral)?.with_visibility_transform(transform)
        }
        None => specification(&request, &prepared_spectral)?,
    };
    let masks = casa_imaging_reconstruction::ImageDomainReconstructionMaskPlans::new(
        prepared_domains
            .iter()
            .map(|domain| {
                reconstruction_mask_plan(domain.mask.clone(), domain.direction, domain.image_size)
            })
            .collect::<Result<Vec<_>, _>>()?,
    )?;
    Ok(ApplicationRequest {
        specification,
        geometry,
        model_lifecycle: ModelLifecycleRequirements::new(
            ModelBounds::new(
                model_samples,
                prepared_spectral.output_channels,
                prepared_spectral.output_channels,
                model_samples,
                f64::MAX,
                f64::MAX,
            )?,
            NumericPrecision::F64,
            ModelInputCommitment::Empty,
        ),
        masks,
        observation: SelectedObservationResolutionRequest::new(
            request.measurement_set.display().to_string(),
            LogicalIdentity::from_sha256(digest),
            observation_selection,
            visibility_column(&ms, request.data_column.as_deref())?,
            if weight_spectrum_complete {
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
        write_corrected_data: request.save_continuum_residual,
        task_requirements: request.task_requirements,
        native,
    })
}

fn canonicalize_polarizations(polarizations: &mut Vec<PolarizationCoordinate>) {
    polarizations.sort_unstable();
    polarizations.dedup();
}

fn standard_primary_beam_model(
    ms: &MeasurementSet,
) -> Result<casa_imaging_products::AnalyticPrimaryBeamModel, crate::ApplicationError> {
    let observation = ms.observation()?;
    let telescopes = (0..observation.row_count())
        .map(|row| {
            observation
                .string(row, "TELESCOPE_NAME")
                .map(|name| name.trim().to_string())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    analytic_primary_beam_model_for_telescopes(&telescopes)
}

fn analytic_primary_beam_model_for_telescopes(
    telescopes: &BTreeSet<String>,
) -> Result<casa_imaging_products::AnalyticPrimaryBeamModel, crate::ApplicationError> {
    match telescopes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["EVLA"] => Ok(casa_imaging_products::AnalyticPrimaryBeamModel::CasaEvlaCommon),
        [] => Err(boxed(
            "standard primary-beam publication requires OBSERVATION telescope metadata",
        )),
        names => Err(boxed(format!(
            "standard primary-beam publication has no installed analytic model for telescope set {names:?}"
        ))),
    }
}

fn validate_request(request: &ContinuumImagingRequest) -> Result<(), crate::ApplicationError> {
    if request.phase_center_field.is_some() && request.phase_center.is_some() {
        return Err(boxed(
            "phase_center and phase_center_field are mutually exclusive",
        ));
    }
    if request.image_size == 0
        || !request.cell_arcsec.is_finite()
        || request.cell_arcsec <= 0.0
        || !request.gain.is_finite()
        || !request.threshold_jy.is_finite()
        || !request.psf_cutoff.is_finite()
        || request.psf_cutoff <= 0.0
        || (request.algorithm != ContinuumAlgorithm::Dirty
            && (request.cycle_iterations == 0 || request.maximum_major_cycles == Some(0)))
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
    if request.save_continuum_residual && request.continuum_subtraction.is_none() {
        return Err(boxed(
            "CORRECTED_DATA residual persistence requires continuum subtraction",
        ));
    }
    if request.continuum_subtraction.is_some()
        && !matches!(request.spectral_mode, SpectralImagingMode::Cube { .. })
    {
        return Err(boxed(
            "visibility-domain continuum subtraction requires channel-local cube imaging",
        ));
    }
    if request.outlier_file.is_some()
        && (!matches!(request.spectral_mode, SpectralImagingMode::Continuum)
            || !matches!(
                request.algorithm,
                ContinuumAlgorithm::Dirty | ContinuumAlgorithm::Hogbom
            )
            || request.weighting != ContinuumWeighting::Natural)
    {
        return Err(boxed(
            "the installed multi-domain slice requires MFS, natural weighting, and dirty or Hogbom reconstruction",
        ));
    }
    Ok(())
}

fn prepare_continuum_transform(
    controls: Option<&VisibilityContinuumSubtraction>,
    field_id: i32,
    spw_id: usize,
    frequencies_hz: &[f64],
    output_channels: &[usize],
) -> Result<(Option<SequentialContinuumTransform>, Vec<usize>), crate::ApplicationError> {
    let Some(controls) = controls else {
        return Ok((None, output_channels.to_vec()));
    };
    let order = u8::try_from(controls.fit_order)
        .map_err(|_| boxed("continuum fit order exceeds the supported contract range"))?;
    let selectors = parse_spw_selector(&controls.fit_spw)?;
    if selectors.len() != 1 || usize::try_from(selectors[0].spw_id).ok() != Some(spw_id) {
        return Err(boxed(
            "continuum fit selector must name exactly the selected spectral window",
        ));
    }
    let fit_channels = match selectors.into_iter().next().expect("one selector").channels {
        Some(selector) => resolve_channel_selector_selection(frequencies_hz, &selector)?.indices,
        None => (0..frequencies_hz.len()).collect(),
    };
    let fit = fit_channels.into_iter().collect::<BTreeSet<_>>();
    let output = output_channels.iter().copied().collect::<BTreeSet<_>>();
    let selected = fit.union(&output).copied().collect::<Vec<_>>();
    let roles = selected
        .iter()
        .copied()
        .map(|channel| {
            let use_role = match (fit.contains(&channel), output.contains(&channel)) {
                (true, false) => ContinuumChannelUse::FitOnly,
                (false, true) => ContinuumChannelUse::ApplyOnly,
                (true, true) => ContinuumChannelUse::FitAndApply,
                (false, false) => unreachable!("set union member"),
            };
            Ok(ContinuumChannelRole::new(
                u32::try_from(channel).map_err(|_| boxed("channel index exceeds u32"))?,
                use_role,
            ))
        })
        .collect::<Result<Vec<_>, crate::ApplicationError>>()?;
    let rule = ContinuumFitRule::new(
        field_id,
        u32::try_from(spw_id).map_err(|_| boxed("SPW id exceeds u32"))?,
        order,
        roles,
    )?;
    Ok((
        Some(SequentialContinuumTransform::new(vec![rule])?),
        selected,
    ))
}

#[cfg(test)]
mod continuum_transform_tests {
    use super::*;

    #[test]
    fn fit_and_output_channels_compile_to_one_union_with_exact_roles() {
        let controls = VisibilityContinuumSubtraction {
            fit_spw: "0:0~1;6~7".to_string(),
            fit_order: 1,
        };
        let (transform, selected) = prepare_continuum_transform(
            Some(&controls),
            5,
            0,
            &[100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
            &[1, 3, 4],
        )
        .expect("compile transform roles");

        assert_eq!(selected, [0, 1, 3, 4, 6, 7]);
        let transform = transform.expect("compiled transform");
        let rule = transform.rule(5, 0).expect("field/SPW rule");
        assert_eq!(rule.channel_use(0), Some(ContinuumChannelUse::FitOnly));
        assert_eq!(rule.channel_use(1), Some(ContinuumChannelUse::FitAndApply));
        assert_eq!(rule.channel_use(3), Some(ContinuumChannelUse::ApplyOnly));
        assert_eq!(rule.channel_use(4), Some(ContinuumChannelUse::ApplyOnly));
        assert_eq!(rule.channel_use(6), Some(ContinuumChannelUse::FitOnly));
        assert_eq!(rule.channel_use(7), Some(ContinuumChannelUse::FitOnly));
    }
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

fn imaging_frequency_frame(
    reference: FrequencyRef,
) -> Result<FrequencyFrame, crate::ApplicationError> {
    match reference {
        FrequencyRef::TOPO => Ok(FrequencyFrame::Topocentric),
        FrequencyRef::BARY => Ok(FrequencyFrame::Barycentric),
        FrequencyRef::LSRK => Ok(FrequencyFrame::Lsrk),
        _ => Err(boxed(format!(
            "native continuum does not support MeasurementSet frequency frame {reference}"
        ))),
    }
}

fn imaging_time_scale(reference: EpochRef) -> Result<TimeScale, crate::ApplicationError> {
    match reference {
        EpochRef::UTC => Ok(TimeScale::Utc),
        EpochRef::TAI => Ok(TimeScale::Tai),
        EpochRef::TT => Ok(TimeScale::Tt),
        EpochRef::TDB => Ok(TimeScale::Tdb),
        _ => Err(boxed(format!(
            "native continuum does not support MeasurementSet epoch reference {reference}"
        ))),
    }
}

fn selected_channels(
    request: &ContinuumImagingRequest,
    spw_id: usize,
    frequencies: &[f64],
) -> Result<Vec<usize>, crate::ApplicationError> {
    if let Some(channels) = explicit_spw_channels(request, spw_id, frequencies)? {
        return Ok(channels);
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

fn explicit_spw_channels(
    request: &ContinuumImagingRequest,
    spw_id: usize,
    frequencies: &[f64],
) -> Result<Option<Vec<usize>>, crate::ApplicationError> {
    let Some(text) = request.spectral_window.as_deref() else {
        return Ok(None);
    };
    let Some(selector) = parse_spw_selector(text)?
        .into_iter()
        .find(|selector| usize::try_from(selector.spw_id).ok() == Some(spw_id))
        .and_then(|selector| selector.channels)
    else {
        return Ok(None);
    };
    Ok(Some(
        resolve_channel_selector_selection(frequencies, &selector)?.indices,
    ))
}

fn direction_spec(
    image_size: usize,
    cell_arcsec: f64,
    right_ascension: f64,
    declination: f64,
) -> DirectionCoordinateSpec {
    let cell = cell_arcsec * std::f64::consts::PI / (180.0 * 3600.0);
    let reference_pixel = image_reference_pixel(image_size);
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
    image_size: usize,
    cell_arcsec: f64,
    phase: [f64; 2],
    polarizations: &[PolarizationCoordinate],
    frequency_reference: FrequencyRef,
    reference_frequency: f64,
    increment_hz: f64,
) -> CoordinateSystem {
    let cell = cell_arcsec * std::f64::consts::PI / (180.0 * 3600.0);
    let reference_pixel = image_reference_pixel(image_size);
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        CoordinateProjection::new(ProjectionType::SIN),
        phase,
        [-cell, cell],
        [reference_pixel, reference_pixel],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(
        polarizations.iter().copied().map(stokes_type).collect(),
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        frequency_reference,
        reference_frequency,
        increment_hz,
        0.0,
        reference_frequency,
    ));
    coordinates
}

const fn stokes_type(coordinate: PolarizationCoordinate) -> StokesType {
    match coordinate {
        PolarizationCoordinate::StokesI => StokesType::I,
        PolarizationCoordinate::StokesQ => StokesType::Q,
        PolarizationCoordinate::StokesU => StokesType::U,
        PolarizationCoordinate::StokesV => StokesType::V,
        PolarizationCoordinate::CircularRr => StokesType::RR,
        PolarizationCoordinate::CircularRl => StokesType::RL,
        PolarizationCoordinate::CircularLr => StokesType::LR,
        PolarizationCoordinate::CircularLl => StokesType::LL,
        PolarizationCoordinate::LinearXx => StokesType::XX,
        PolarizationCoordinate::LinearXy => StokesType::XY,
        PolarizationCoordinate::LinearYx => StokesType::YX,
        PolarizationCoordinate::LinearYy => StokesType::YY,
    }
}

fn parse_phase_center_direction(text: &str) -> Result<SkyDirection, crate::ApplicationError> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 || !parts[0].eq_ignore_ascii_case("J2000") {
        return Err(boxed(
            "phasecenter must be 'J2000 lon lat', for example 'J2000 19:59:28.500 +40.44.01.50'",
        ));
    }
    Ok(SkyDirection::new(
        DirectionFrame::J2000,
        parse_phase_center_angle(parts[1], true)?,
        parse_phase_center_angle(parts[2], false)?,
    ))
}

fn parse_phase_center_angle(text: &str, longitude: bool) -> Result<f64, crate::ApplicationError> {
    let lower = text.to_ascii_lowercase();
    if let Some(radians) = lower.strip_suffix("rad") {
        return radians
            .trim()
            .parse::<f64>()
            .map_err(|error| Box::new(error) as crate::ApplicationError);
    }
    if let Some(degrees) = lower.strip_suffix("deg") {
        return Ok(degrees.trim().parse::<f64>()? * std::f64::consts::PI / 180.0);
    }
    if longitude {
        if let Some(hours) = parse_sexagesimal(text, true) {
            return Ok(hours * std::f64::consts::PI / 12.0);
        }
    } else if let Some(degrees) = parse_sexagesimal(text, false) {
        return Ok(degrees * std::f64::consts::PI / 180.0);
    }
    Err(boxed(format!("unsupported phasecenter angle {text:?}")))
}

fn parse_sexagesimal(text: &str, hours: bool) -> Option<f64> {
    let trimmed = text.trim();
    let sign = if trimmed.starts_with('-') { -1.0 } else { 1.0 };
    let body = trimmed.trim_start_matches(['+', '-']);
    let fields = if body.contains(':') {
        body.split(':').map(str::to_owned).collect::<Vec<_>>()
    } else if body.contains('h') || body.contains('d') || body.contains('m') || body.contains('s') {
        body.replace(['h', 'd', 'm', 's'], " ")
            .split_whitespace()
            .map(str::to_owned)
            .collect()
    } else if !hours && body.matches('.').count() >= 2 {
        let mut split = body.split('.');
        let major = split.next()?.to_owned();
        let minutes = split.next()?.to_owned();
        let seconds = split.collect::<Vec<_>>().join(".");
        vec![major, minutes, seconds]
    } else {
        return None;
    };
    let [major, minutes, seconds] = fields.as_slice() else {
        return None;
    };
    let major = major.parse::<f64>().ok()?;
    let minutes = minutes.parse::<f64>().ok()?;
    let seconds = seconds.parse::<f64>().ok()?;
    if !(major.is_finite()
        && minutes.is_finite()
        && seconds.is_finite()
        && (0.0..60.0).contains(&minutes)
        && (0.0..60.0).contains(&seconds))
    {
        return None;
    }
    Some(sign * (major.abs() + minutes / 60.0 + seconds / 3600.0))
}

fn image_reference_pixel(image_size: usize) -> f64 {
    image_size as f64 / 2.0
}

fn reproject_image_mask(
    path: &Path,
    target_spec: DirectionCoordinateSpec,
    target_size: usize,
) -> Result<ReconstructionMaskPlan, crate::ApplicationError> {
    let image = AnyPagedImage::open(path)?;
    let (source_shape, source_coordinates, source_support) = match image {
        AnyPagedImage::Float32(image) => {
            let shape = image.shape().to_vec();
            let coordinates = image.coordinates().clone();
            let mask = image.get_mask()?;
            let values = image.get()?;
            let support: Vec<bool> = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    value.is_finite()
                        && *value != 0.0
                        && mask.as_ref().is_none_or(|mask| mask[index])
                })
                .collect();
            (shape, coordinates, support)
        }
        AnyPagedImage::Float64(image) => {
            let shape = image.shape().to_vec();
            let coordinates = image.coordinates().clone();
            let mask = image.get_mask()?;
            let values = image.get()?;
            let support: Vec<bool> = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    value.is_finite()
                        && *value != 0.0
                        && mask.as_ref().is_none_or(|mask| mask[index])
                })
                .collect();
            (shape, coordinates, support)
        }
        AnyPagedImage::Complex32(_) | AnyPagedImage::Complex64(_) => {
            return Err(boxed("reconstruction masks require a real CASA image"));
        }
    };
    if source_shape.len() < 2
        || source_shape[2..].iter().any(|extent| *extent != 1)
        || source_support.len() != source_shape[0] * source_shape[1]
    {
        return Err(boxed(
            "reconstruction mask must contain one two-dimensional direction plane",
        ));
    }
    let source_direction = direction_coordinate(&source_coordinates)?;
    let source_spec = direction_model_spec(source_direction)?;
    let support = casa_imaging_reconstruction::reproject_mask_support(
        source_spec,
        [source_shape[0], source_shape[1]],
        &source_support,
        target_spec,
        [target_size, target_size],
    )?;
    Ok(ReconstructionMaskPlan::Reprojected {
        coordinate: target_spec,
        source_coordinate: source_spec,
        source_shape: [source_shape[0], source_shape[1]],
        support,
    })
}

fn reconstruction_mask_plan(
    mask: ContinuumMask,
    coordinate: DirectionCoordinateSpec,
    image_size: usize,
) -> Result<ReconstructionMaskPlan, crate::ApplicationError> {
    Ok(match mask {
        ContinuumMask::Coupled { continuum, line } => ReconstructionMaskPlan::Coupled {
            continuum: Box::new(reconstruction_mask_plan(
                *continuum, coordinate, image_size,
            )?),
            line: Box::new(reconstruction_mask_plan(*line, coordinate, image_size)?),
        },
        ContinuumMask::FullPlane => ReconstructionMaskPlan::FullPlane { coordinate },
        ContinuumMask::Boxes(boxes) => ReconstructionMaskPlan::Boxes {
            coordinate,
            boxes: boxes
                .into_iter()
                .map(|region| casa_imaging_reconstruction::MaskBox::new(region.blc, region.trc))
                .collect::<Result<Vec<_>, _>>()?,
        },
        ContinuumMask::Image(path) => reproject_image_mask(&path, coordinate, image_size)?,
        ContinuumMask::PixelSupport(support) => {
            if image_size
                .checked_mul(image_size)
                .is_none_or(|expected| support.len() != expected)
            {
                return Err(boxed("pixel mask support does not match its image domain"));
            }
            ReconstructionMaskPlan::Reprojected {
                coordinate,
                source_coordinate: coordinate,
                source_shape: [image_size, image_size],
                support,
            }
        }
        ContinuumMask::AutoMultithresh(controls) => ReconstructionMaskPlan::AutoMultithresh {
            coordinate,
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
            evolution_stopped: false,
        },
    })
}

fn direction_coordinate(
    coordinates: &CoordinateSystem,
) -> Result<&CoordinateModel, crate::ApplicationError> {
    let index = coordinates
        .find_coordinate(CoordinateType::Direction)
        .ok_or_else(|| boxed("mask image has no direction coordinate"))?;
    Ok(coordinates.coordinate(index))
}

fn direction_model_spec(
    coordinate: &CoordinateModel,
) -> Result<DirectionCoordinateSpec, crate::ApplicationError> {
    let CoordinateModel::Direction(direction) = coordinate else {
        return Err(boxed("mask direction-coordinate lookup was inconsistent"));
    };
    if direction.projection().projection_type() != ProjectionType::SIN {
        return Err(boxed(
            "native mask reprojection currently requires SIN coordinates",
        ));
    }
    let frame = match direction.direction_ref() {
        DirectionRef::J2000 => DirectionFrame::J2000,
        DirectionRef::B1950 => DirectionFrame::B1950,
        DirectionRef::GALACTIC => DirectionFrame::Galactic,
        DirectionRef::ICRS => DirectionFrame::Icrs,
        _ => {
            return Err(boxed(
                "mask direction frame is not supported by native imaging",
            ));
        }
    };
    let reference = coordinate.reference_value();
    let pixel = coordinate.reference_pixel();
    let increment = coordinate.increment();
    let pc = direction.pc_matrix();
    Ok(DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(frame, reference[0], reference[1]),
        [pixel[0], pixel[1]],
        [increment[0], increment[1]],
        [[pc[[0, 0]], pc[[0, 1]]], [pc[[1, 0]], pc[[1, 1]]]],
        [
            direction.longpole().to_degrees(),
            direction.latpole().to_degrees(),
        ],
    ))
}

fn specification(
    request: &ContinuumImagingRequest,
    spectral: &PreparedSpectralAxis,
) -> Result<ProblemSpecification, crate::ApplicationError> {
    let algorithm = reconstruction_algorithm(&request.algorithm);
    let basis = match &request.algorithm {
        ContinuumAlgorithm::Mtmfs { terms, .. } => ReconstructionBasis::Taylor { terms: *terms },
        ContinuumAlgorithm::JointContinuumLine {
            continuum_terms,
            line_channels,
            ..
        } => ReconstructionBasis::JointContinuumLine {
            continuum_terms: *continuum_terms,
            line_terms: line_channels.len(),
        },
        _ => spectral.basis,
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
    let mut reconstruction = ReconstructionContract::new(
        basis,
        algorithm.clone(),
        if algorithm == ReconstructionAlgorithm::Dirty {
            ReconstructionControls::new(0, 1.0, 0.0)
        } else {
            let controls =
                ReconstructionControls::new(request.iterations, request.gain, request.threshold_jy)
                    .with_cycle_limits(request.cycle_iterations, request.maximum_major_cycles)
                    .with_hogbom_iteration_accounting(request.hogbom_iteration_accounting)
                    .with_cycle_threshold(
                        request.cycle_factor,
                        request.minimum_psf_fraction,
                        request.maximum_psf_fraction,
                    );
            request
                .noise_sigma
                .map_or(controls, |sigma| controls.with_noise_sigma(sigma))
        },
        PolarizationContract::new(request.polarizations.clone()),
    );
    if let ContinuumAlgorithm::JointContinuumLine {
        continuum_anchor_channels,
        line_channels,
        maximum_condition_number,
        ..
    } = &request.algorithm
    {
        reconstruction = reconstruction.with_joint_continuum_line(
            casa_imaging_model::JointContinuumLineContract::new(
                continuum_anchor_channels.clone(),
                line_channels.clone(),
                *maximum_condition_number,
            ),
        );
    }
    Ok(ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(
                spectral.sampling,
                match (&request.algorithm, request.beam_policy) {
                    (ContinuumAlgorithm::Mtmfs { .. }, _)
                    | (ContinuumAlgorithm::JointContinuumLine { .. }, _) => {
                        SpectralCoupling::CommonRestoringBeam
                    }
                    (_, ContinuumBeamPolicy::PerPlane) => SpectralCoupling::Independent,
                    (_, ContinuumBeamPolicy::Common) => SpectralCoupling::CommonRestoringBeam,
                },
            ),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        reconstruction,
        WeightingContract::new(weighting, density),
        ProductRequirements::new(
            {
                let mut products = vec![
                    ProductKind::Psf,
                    ProductKind::Residual,
                    ProductKind::Model,
                    ProductKind::RestoredImage,
                    ProductKind::SumWeights,
                    ProductKind::Mask,
                    ProductKind::Beam,
                ];
                if matches!(request.algorithm, ContinuumAlgorithm::Mtmfs { .. }) {
                    products.extend([
                        ProductKind::TaylorTerms,
                        ProductKind::SpectralIndex,
                        ProductKind::SpectralIndexError,
                    ]);
                }
                if request.write_primary_beam || request.pbcor {
                    products.push(ProductKind::PrimaryBeam);
                }
                if request.pbcor {
                    products.push(ProductKind::PbCorrectedImage);
                }
                products
            },
            ProductNormalization::UnitResponse,
            match (&request.algorithm, request.beam_policy) {
                (ContinuumAlgorithm::Mtmfs { .. }, _)
                | (ContinuumAlgorithm::JointContinuumLine { .. }, _) => RestoringBeamPolicy::Common,
                (_, ContinuumBeamPolicy::PerPlane) => RestoringBeamPolicy::PerPlane,
                (_, ContinuumBeamPolicy::Common) => RestoringBeamPolicy::Common,
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
        })
        .with_corrected_data_write(if request.save_continuum_residual {
            casa_imaging_model::CorrectedDataWrite::SelectedOutputRows
        } else {
            casa_imaging_model::CorrectedDataWrite::Disabled
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

fn reconstruction_algorithm(algorithm: &ContinuumAlgorithm) -> ReconstructionAlgorithm {
    match algorithm {
        ContinuumAlgorithm::Dirty => ReconstructionAlgorithm::Dirty,
        ContinuumAlgorithm::Hogbom => ReconstructionAlgorithm::Hogbom,
        ContinuumAlgorithm::Clark => ReconstructionAlgorithm::Clark,
        ContinuumAlgorithm::Multiscale {
            scales_px,
            small_scale_bias,
        } => ReconstructionAlgorithm::Multiscale {
            scales_px: scales_px.clone(),
            small_scale_bias: *small_scale_bias,
        },
        ContinuumAlgorithm::Mtmfs {
            scales_px,
            small_scale_bias,
            ..
        } => ReconstructionAlgorithm::Mtmfs {
            scales_px: scales_px.clone(),
            small_scale_bias: *small_scale_bias,
        },
        ContinuumAlgorithm::JointContinuumLine {
            scales_px,
            small_scale_bias,
            ..
        } => ReconstructionAlgorithm::JointContinuumLine {
            scales_px: scales_px.clone(),
            small_scale_bias: *small_scale_bias,
        },
    }
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
    domains: &[PreparedImageDomain],
    content_budget: SelectedObservationContentBudget,
) -> Result<Option<ProductionStorageProfile>, crate::ApplicationError> {
    let input_root = filesystem_root(&request.measurement_set.canonicalize()?)?;
    for domain in domains {
        let output_parent = domain.output.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(output_parent)?;
        if filesystem_root(&output_parent.canonicalize()?)? != input_root {
            return Ok(None);
        }
    }
    let (capacity, available) = filesystem_capacity(&input_root)?;
    let read_rate = positive_environment("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND")?;
    let write_rate = positive_environment("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND")?;
    Ok(Some(ProductionStorageProfile::new(
        input_root,
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
    domains: &[PreparedImageDomain],
    profile: &ProductionStorageProfile,
) -> Result<ApplicationRuntime, crate::ApplicationError> {
    let digest = request_digest(request, b"attempt");
    let output_directory = request
        .image_name
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()?;
    let receipts = output_directory.join(".casa-rs-imaging-receipts");
    let authority = ResourceAuthority::production_with_storage_profile(profile)?.clone();
    let storage_io = profile.io_resources();
    let gridded_normal_storage =
        GriddedNormalReplayStorage::bind(&authority, storage_io.clone(), &output_directory)?;
    Ok(ApplicationRuntime {
        registry: ImplementationRegistryId::from_sha256(hash(b"spectral-cycle-registry")),
        implementation: WorkImplementationId::new("spectral-cycle-cpu-v1"),
        weighting_limits: WeightingExecutionLimits::new(4096, 1)?,
        stage_nanos: 1_000_000,
        minor_cycle_bytes: domains.iter().try_fold(0_u64, |total, domain| {
            total
                .checked_add(planned_minor_cycle_bytes(
                    domain.image_size,
                    &request.algorithm,
                    request.iterations,
                ))
                .ok_or_else(|| boxed("multi-domain minor-cycle residency overflowed"))
        })?,
        storage_io,
        gridded_normal_storage,
        confidence_parts_per_million: 900_000,
        resource_policy: match resource_policy_for_task_requirements(&request.task_requirements) {
            ResourcePolicy::Explicit(serial) => ResourcePolicy::Explicit(serial),
            _ => request.resource_policy.clone(),
        },
        cost_model: PlannerCostModelProfileId::from_sha256(hash(b"spectral-cycle-cost-v1"))
            .bootstrap(),
        authority,
        receipts: ExecutionReceiptStore::new(receipts, ReceiptRetention::new(128, 64 << 20)?)?,
        build: BuildIdentity::from_sha256(hash(env!("CARGO_PKG_VERSION").as_bytes())),
        attempts: [
            ExecutionAttemptId::from_sha256(scoped(digest, 0)),
            ExecutionAttemptId::from_sha256(scoped(digest, 1)),
            ExecutionAttemptId::from_sha256(scoped(digest, 2)),
        ],
    })
}

fn planned_minor_cycle_bytes(
    image_size: usize,
    algorithm: &ContinuumAlgorithm,
    maximum_iterations: usize,
) -> u64 {
    let basis = match algorithm {
        ContinuumAlgorithm::Mtmfs { terms, .. } => ReconstructionBasis::Taylor { terms: *terms },
        ContinuumAlgorithm::JointContinuumLine {
            continuum_terms,
            line_channels,
            ..
        } => ReconstructionBasis::JointContinuumLine {
            continuum_terms: *continuum_terms,
            line_terms: line_channels.len(),
        },
        _ => ReconstructionBasis::Constant,
    };
    minor_cycle_workspace_bytes(
        [image_size, image_size],
        basis,
        &reconstruction_algorithm(algorithm),
        maximum_iterations,
        64,
    )
}

/// Project stable task execution intent into the application-owned Resource
/// Policy without inspecting the host or choosing a backend.
#[must_use]
pub fn resource_policy_for_task_requirements(
    task_requirements: &[TaskRequirement],
) -> ResourcePolicy {
    let serial_cpu = task_requirements.contains(&TaskRequirement::SerialCpu);
    let planner_selected_parallelism = task_requirements.iter().any(|requirement| {
        matches!(
            requirement,
            TaskRequirement::ExecutionAuto | TaskRequirement::FixedTileCpu
        )
    });
    if serial_cpu && !planner_selected_parallelism {
        ResourcePolicy::Explicit(ResourceOverride {
            workers: Some(1),
            ..ResourceOverride::default()
        })
    } else {
        ResourcePolicy::Balanced
    }
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
    if let Some(field) = request.phase_center_field {
        hasher.update([1]);
        hasher.update(field.to_le_bytes());
    } else {
        hasher.update([0]);
    }
    if let Some(phase_center) = request.phase_center.as_deref() {
        hasher.update([1]);
        hasher.update(phase_center.as_bytes());
    } else {
        hasher.update([0]);
    }
    if let Some(outlier_file) = request.outlier_file.as_deref() {
        hasher.update([1]);
        hasher.update(outlier_file.as_os_str().as_encoded_bytes());
        if let Ok(contents) = std::fs::read(outlier_file) {
            hasher.update(Sha256::digest(contents));
        }
    } else {
        hasher.update([0]);
    }
    hasher.finalize().into()
}

const fn model_plane_samples(image_size: usize) -> usize {
    image_size.saturating_mul(image_size)
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
    use casa_coordinates::{CoordinateModel, CoordinateType, StokesType};
    use casa_imaging_model::PolarizationCoordinate;
    use casa_imaging_runtime::{ResourceOverride, ResourcePolicy};
    use casa_types::measures::frequency::FrequencyRef;

    use super::{
        ContinuumAlgorithm, TaskRequirement, analytic_primary_beam_model_for_telescopes,
        canonicalize_polarizations, image_coordinates, image_reference_pixel, model_plane_samples,
        parse_phase_center_direction, planned_minor_cycle_bytes,
        resource_policy_for_task_requirements,
    };

    #[test]
    fn casa_direction_reference_pixel_uses_half_the_image_extent() {
        assert_eq!(image_reference_pixel(16), 8.0);
        assert_eq!(image_reference_pixel(15), 7.5);
    }

    #[test]
    fn casa_phase_center_literal_preserves_recentered_chart_coordinates() {
        let direction = parse_phase_center_direction("J2000 19:58:40.895 +40.55.58.543")
            .expect("CASA outlier phase center");
        let expected_ra = (19.0 + 58.0 / 60.0 + 40.895 / 3600.0) * std::f64::consts::PI / 12.0;
        let expected_dec = (40.0 + 55.0 / 60.0 + 58.543 / 3600.0) * std::f64::consts::PI / 180.0;
        assert!((direction.longitude_rad() - expected_ra).abs() < 1.0e-14);
        assert!((direction.latitude_rad() - expected_dec).abs() < 1.0e-14);
    }

    #[test]
    fn request_polarizations_are_canonicalized_before_product_wcs_is_built() {
        let mut polarizations = vec![
            PolarizationCoordinate::StokesQ,
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
        ];
        canonicalize_polarizations(&mut polarizations);

        assert_eq!(
            polarizations,
            [
                PolarizationCoordinate::StokesI,
                PolarizationCoordinate::StokesQ,
            ]
        );
        let coordinates = image_coordinates(
            64,
            8.0,
            [0.0, 0.0],
            &polarizations,
            FrequencyRef::LSRK,
            1.0e9,
            1.0,
        );
        let polarization_coordinate = coordinates
            .find_coordinate(CoordinateType::Stokes)
            .expect("polarization coordinate");
        let CoordinateModel::Stokes(stokes) = coordinates.coordinate(polarization_coordinate)
        else {
            panic!("polarization coordinate has the wrong type");
        };
        assert_eq!(stokes.stokes(), [StokesType::I, StokesType::Q]);
        assert_eq!(stokes.n_stokes(), 2);
    }

    #[test]
    fn model_delta_bound_covers_one_complete_multiscale_plane() {
        assert_eq!(model_plane_samples(64), 4096);
    }

    #[test]
    fn mtmfs_runtime_claim_grows_with_taylor_terms_and_scales() {
        let point = ContinuumAlgorithm::Mtmfs {
            terms: 2,
            scales_px: vec![0.0],
            small_scale_bias: 0.0,
        };
        let higher_order = ContinuumAlgorithm::Mtmfs {
            terms: 3,
            scales_px: vec![0.0, 5.0],
            small_scale_bias: 0.0,
        };

        assert!(
            planned_minor_cycle_bytes(128, &higher_order, 8)
                > planned_minor_cycle_bytes(128, &point, 8)
        );
    }

    #[test]
    fn explicit_serial_cpu_requirement_caps_the_application_to_one_worker() {
        assert_eq!(
            resource_policy_for_task_requirements(&[TaskRequirement::SerialCpu]),
            ResourcePolicy::Explicit(ResourceOverride {
                workers: Some(1),
                ..ResourceOverride::default()
            })
        );
    }

    #[test]
    fn fixed_tile_cpu_requirement_uses_balanced_application_planning() {
        assert_eq!(
            resource_policy_for_task_requirements(&[
                TaskRequirement::SerialCpu,
                TaskRequirement::FixedTileCpu,
            ]),
            ResourcePolicy::Balanced
        );
        assert_eq!(
            resource_policy_for_task_requirements(&[]),
            ResourcePolicy::Balanced
        );
    }

    #[test]
    fn standard_primary_beam_model_is_explicit_and_fails_closed() {
        let evla = std::collections::BTreeSet::from(["EVLA".to_string()]);
        assert_eq!(
            analytic_primary_beam_model_for_telescopes(&evla).expect("EVLA model"),
            casa_imaging_products::AnalyticPrimaryBeamModel::CasaEvlaCommon
        );
        let unsupported = std::collections::BTreeSet::from(["VLA".to_string()]);
        assert!(analytic_primary_beam_model_for_telescopes(&unsupported).is_err());
        assert!(
            analytic_primary_beam_model_for_telescopes(&std::collections::BTreeSet::new()).is_err()
        );
    }
}
