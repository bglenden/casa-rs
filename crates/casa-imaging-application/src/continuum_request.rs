// SPDX-License-Identifier: LGPL-3.0-or-later

//! MeasurementSet-facing application request for the native continuum surface.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    path::{Path, PathBuf},
    sync::Arc,
};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate, ObsInfo,
    Projection as CoordinateProjection, ProjectionType, SpectralCoordinate, StokesCoordinate,
    StokesType,
};
use casa_images::AnyPagedImage;
use casa_imaging_model::{
    AwProjectionContract, AxisOrder, CentreLaws, ContinuumChannelRole, ContinuumChannelUse,
    ContinuumFitRule, CorrelationProduct, CorrelationSelection, CorrelationType,
    DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    HogbomIterationAccounting, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    InstrumentModel, InstrumentResponse, ItrfPosition, LogicalIdentity,
    MeasurementEquationContract, MissingPointingPolicy, ModelBounds, ModelColumnWrite,
    ModelInnerProduct, ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity,
    NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationSelection, ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    PrimaryBeamValidityPolicy, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    Projection, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract,
    ReconstructionControls, ReductionPolicy, ReferenceDataKind, RestFrequency, RestoringBeamPolicy,
    ScientificContract, SelectedMainRow, SelectedRowsBuilder, SequentialContinuumTransform,
    SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSamplingLaw, SpectralWcs, SpectralWindowSelection, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, TimeScale, UnitResponseValidityPolicy,
    UvwCoordinateLaw, VisibilityColumn as OwnerVisibilityColumn, VisibilityInnerProduct,
    WProjectionContract, WeightColumn as OwnerWeightColumn, WeightDensityScope, WeightingContract,
    WeightingScheme,
};
use casa_imaging_reconstruction::{
    ReconstructionMaskPlan, WeightingExecutionLimits, minor_cycle_workspace_bytes,
};
use casa_imaging_runtime::{
    BuildIdentity, ExecutionAttemptId, ExecutionReceiptStore, ImplementationRegistryId,
    ManagedSpillStorage, PlannerCostModelProfileId, ProductionStorageProfile, ReceiptRetention,
    ResourceAuthority, ResourceOverride, ResourcePolicy, WorkImplementationId,
};
use casa_ms::{
    CubeAxisConfig, CubeInterpolation, CubeSpectralSetup, MeasurementSet, MsSelectionIoBudget,
    SelectedObservationContentBudget, SelectedObservationEphemeris, SelectedObservationMeasures,
    SelectedObservationResolutionRequest, SelectedObservationRow, SelectedObservationRowSelection,
    SelectedObservationSpectralEnvelope, SelectedObservationSpectralEnvelopeReducer,
    SelectedObservationSpectralWindow, SubtableId, VisibilityDataColumn, parse_spw_selector,
    resolve_channel_selector_selection,
};
use casa_types::ArrayValue;
use casa_types::measures::{
    direction::{DirectionRef, MDirection},
    doppler::DopplerRef,
    epoch::{EpochRef, MEpoch},
    frame::MeasFrame,
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

/// Complete native AW-projection request retained through application preparation.
#[derive(Clone, Debug, PartialEq)]
pub struct ContinuumAwProjection {
    /// Read-only CASA `CFS_`/`WTCFS_` cache root.
    pub casa_cache: PathBuf,
    /// Hard ceiling for simultaneously resident paired convolution cells.
    pub resident_bytes: usize,
    /// Explicit W-plane count, when supplied by the task surface.
    pub w_plane_count: Option<usize>,
    /// Optional distinct PSF phase centre in radians.
    pub psf_phase_center_direction_rad: Option<[f64; 2]>,
    /// Optional voltage-pattern table.
    pub vp_table: Option<PathBuf>,
    /// Enable the EVLA aperture term.
    pub a_term: bool,
    /// Enable the prolate-spheroidal term.
    pub ps_term: bool,
    /// Enable wideband A-projection frequency selection.
    pub wideband: bool,
    /// Enable conjugate-frequency beam selection.
    pub conjugate_beams: bool,
    /// Use row-local POINTING-table offsets.
    pub use_pointing: bool,
    /// Pointing-offset standard deviations in arcseconds.
    pub pointing_offset_sigdev: Vec<f64>,
    /// Enable mosaic weight-density behavior.
    pub mosaic_weighting: bool,
    /// Parallactic-angle computation step in degrees.
    pub compute_pa_step_deg: f64,
    /// Parallactic-angle rotation step in degrees.
    pub rotate_pa_step_deg: f64,
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
    /// Multi-term continuum reconstruction with cube-shaped major-cycle sampling.
    MtmfsViaCube {
        /// CASA-compatible output-axis and interpolation controls.
        axis: CubeAxisConfig,
        /// Requested major-cycle channel count; `None` uses every source channel.
        output_channels: Option<usize>,
    },
    /// Source-rest-frame cube evaluated against a moving phase centre.
    CubeSource {
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
    /// Positive primary-beam support cutoff corresponding to CASA `abs(pblimit)`.
    pub primary_beam_cutoff: f32,
    /// Direction-dependent image normalization selected by the task surface.
    pub normalization: ProductNormalization,
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
    /// Explicit W-projection plane count; `None` derives it from the selected W envelope.
    pub w_projection_planes: Option<usize>,
    /// Complete AW-projection cache and term contract; mutually exclusive with W-projection.
    pub aw_projection: Option<ContinuumAwProjection>,
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
    image_rest_frequency_hz: f64,
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
    frequency_reference: FrequencyRef,
    frequencies_hz: Vec<f64>,
    channel_widths_hz: Vec<f64>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_spectral_axis(
    request: &ContinuumImagingRequest,
    measurement_set: &MeasurementSet,
    row_selection: &SelectedObservationRowSelection,
    selection_io: MsSelectionIoBudget,
    moving_phase_centre: Option<&SelectedObservationEphemeris>,
    spectral_windows: &[SourceSpectralWindow],
    source_frequency_reference: FrequencyRef,
    anchor_time_mjd_seconds: f64,
    time_bounds_mjd_seconds: [f64; 2],
    field_id: usize,
    phase: MDirection,
    direction: DirectionCoordinateSpec,
    frame_engine: &casa_ms::derived::engine::MsCalEngine,
    moving_rest_frame: Option<&MeasFrame>,
    source_rest_frequency_hz: Option<f64>,
    continuum_selected_source_channels: Option<BTreeMap<usize, Vec<usize>>>,
    continuum_spectral_envelope: Option<SelectedObservationSpectralEnvelope>,
) -> Result<PreparedSpectralAxis, crate::ApplicationError> {
    let source_frame = imaging_frequency_frame(source_frequency_reference)?;
    match &request.spectral_mode {
        SpectralImagingMode::Continuum => {
            let selected_source_channels = continuum_selected_source_channels.ok_or_else(|| {
                boxed("continuum imaging requires a prepared selected-channel map")
            })?;
            let spectral_envelope = continuum_spectral_envelope
                .ok_or_else(|| boxed("continuum imaging requires a selected spectral envelope"))?;
            let [lower_hz, upper_hz] = spectral_envelope.edges_hz();
            let output_frequency_reference = FrequencyRef::LSRK;
            let output_frame = imaging_frequency_frame(output_frequency_reference)?;
            let reference_frequency_hz = spectral_envelope.midpoint_hz();
            let increment_hz = upper_hz - lower_hz;
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
                    increment_hz,
                },
                rest_frequency: RestFrequency::NotApplicable,
                image_rest_frequency_hz: reference_frequency_hz,
                doppler: DopplerConvention::NotApplicable,
                sampling: SpectralSamplingLaw::IDENTITY,
                basis: ReconstructionBasis::Constant,
                output_channels: 1,
                reference_frequency_hz,
                increment_hz,
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
                image_rest_frequency_hz: spectral_window_midpoint_hz(window),
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
            let (resolved_rest_frequency_hz, image_rest_frequency_hz) =
                cube_rest_frequency_hz(axis.rest_frequency_hz, source_rest_frequency_hz, window);
            let (rest_frequency, doppler) = match resolved_rest_frequency_hz {
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
                image_rest_frequency_hz,
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
        SpectralImagingMode::MtmfsViaCube {
            axis,
            output_channels,
        } => prepare_mvc_spectral_axis(
            request,
            measurement_set,
            row_selection,
            selection_io,
            moving_phase_centre,
            spectral_windows,
            source_frequency_reference,
            source_frame,
            anchor_time_mjd_seconds,
            time_bounds_mjd_seconds,
            field_id,
            phase,
            direction,
            frame_engine,
            axis,
            *output_channels,
        ),
        SpectralImagingMode::CubeSource {
            axis,
            output_channels,
        } => {
            let [window] = spectral_windows else {
                return Err(boxed(
                    "source-frame cube imaging requires exactly one selected spectral window",
                ));
            };
            let frame = moving_rest_frame.ok_or_else(|| {
                boxed("source-frame cube imaging requires an ephemeris radial velocity")
            })?;
            let mut native_axis = axis.clone();
            native_axis.specmode = casa_ms::spectral_selection::CubeSpecMode::Cubedata;
            native_axis.outframe = source_frequency_reference;
            let output_channels = output_channels.unwrap_or(window.frequencies_hz.len());
            let (setup, support) = CubeSpectralSetup::for_casa_cube_axis(
                source_frequency_reference,
                &window.frequencies_hz,
                &window.channel_widths_hz,
                output_channels,
                &native_axis,
                anchor_time_mjd_seconds,
                field_id,
                Some(phase),
                time_bounds_mjd_seconds,
                frame_engine,
            )?;
            let factor = casa_ms::convert_frequency_to_frame_with_frame(
                source_frequency_reference,
                FrequencyRef::REST,
                1.0,
                Some(frame),
            )?;
            let reference_frequency_hz = setup.output_channel_frequencies_hz[0] * factor;
            let increment_hz = if output_channels > 1 {
                (setup.output_channel_frequencies_hz[1] - setup.output_channel_frequencies_hz[0])
                    * factor
            } else {
                setup.output_channel_widths_hz[0] * factor
            };
            let rest_frequency_hz = axis
                .rest_frequency_hz
                .or(source_rest_frequency_hz)
                .ok_or_else(|| {
                    boxed("source-frame cube imaging requires REST_FREQUENCY metadata")
                })?;
            Ok(PreparedSpectralAxis {
                selected_source_channels: BTreeMap::from([(window.spw_id, support.indices)]),
                source_frame,
                output_frequency_reference: FrequencyRef::REST,
                output_frame: FrequencyFrame::Rest,
                anchor: spectral_frame_anchor(
                    source_frame,
                    FrequencyFrame::Rest,
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
                rest_frequency: RestFrequency::Line {
                    hertz: rest_frequency_hz,
                },
                image_rest_frequency_hz: rest_frequency_hz,
                doppler: match axis.veltype {
                    DopplerRef::RADIO => DopplerConvention::Radio,
                    DopplerRef::Z => DopplerConvention::Optical,
                    DopplerRef::BETA => DopplerConvention::Relativistic,
                    DopplerRef::RATIO | DopplerRef::GAMMA => {
                        return Err(boxed(
                            "source-frame cube Doppler convention is not supported",
                        ));
                    }
                },
                sampling: match axis.interpolation {
                    CubeInterpolation::Nearest => SpectralSamplingLaw::NEAREST,
                    CubeInterpolation::Linear => SpectralSamplingLaw::LINEAR,
                    CubeInterpolation::Cubic => SpectralSamplingLaw::CUBIC,
                },
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

#[allow(clippy::too_many_arguments)]
fn prepare_mvc_spectral_axis(
    request: &ContinuumImagingRequest,
    measurement_set: &MeasurementSet,
    row_selection: &SelectedObservationRowSelection,
    selection_io: MsSelectionIoBudget,
    moving_phase_centre: Option<&SelectedObservationEphemeris>,
    spectral_windows: &[SourceSpectralWindow],
    source_frequency_reference: FrequencyRef,
    source_frame: FrequencyFrame,
    anchor_time_mjd_seconds: f64,
    time_bounds_mjd_seconds: [f64; 2],
    field_id: usize,
    phase: MDirection,
    direction: DirectionCoordinateSpec,
    frame_engine: &casa_ms::derived::engine::MsCalEngine,
    axis: &CubeAxisConfig,
    requested_output_channels: Option<usize>,
) -> Result<PreparedSpectralAxis, crate::ApplicationError> {
    let mut selected_by_spw = BTreeMap::new();
    let mut spectral_selection = Vec::with_capacity(spectral_windows.len());
    let mut selected_channel_count = 0_usize;
    let mut output_frequency_reference = None;
    // CASA MVC derives the major-cycle cube from the complete selected band,
    // then replaces task start/width with that global linear axis.
    let mut range_axis = axis.clone();
    range_axis.start = None;
    range_axis.width = None;

    for window in spectral_windows {
        let selected = selected_channels(request, window.spw_id, &window.frequencies_hz)?;
        selected_channel_count = selected_channel_count
            .checked_add(selected.len())
            .ok_or_else(|| boxed("selected MVC channel count overflows usize"))?;
        let (probe, _) = CubeSpectralSetup::for_casa_cube_axis(
            source_frequency_reference,
            &window.frequencies_hz,
            &window.channel_widths_hz,
            window.frequencies_hz.len(),
            &range_axis,
            anchor_time_mjd_seconds,
            field_id,
            Some(phase.clone()),
            time_bounds_mjd_seconds,
            frame_engine,
        )?;
        if output_frequency_reference.is_some_and(|reference| reference != probe.output_freq_ref) {
            return Err(boxed(
                "selected MVC spectral windows resolve to different output frequency frames",
            ));
        }
        output_frequency_reference.get_or_insert(probe.output_freq_ref);

        spectral_selection.push(SpectralWindowSelection::new(
            u32::try_from(window.spw_id).map_err(|_| boxed("SPW id exceeds u32"))?,
            selected
                .iter()
                .copied()
                .map(|channel| {
                    u32::try_from(channel).map_err(|_| boxed("channel index exceeds u32"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ));
        selected_by_spw.insert(window.spw_id, selected);
    }

    let output_channels = requested_output_channels.unwrap_or(selected_channel_count);
    if output_channels == 0 {
        return Err(boxed(
            "MVC requires a positive output channel count and finite selected frequency range",
        ));
    }
    let output_frequency_reference =
        output_frequency_reference.expect("nonempty selected spectral windows");
    let range = measurement_set.selected_observation_spectral_range(
        row_selection,
        &spectral_selection,
        source_frequency_reference,
        output_frequency_reference,
        field_id,
        anchor_time_mjd_seconds,
        phase.clone(),
        moving_phase_centre,
        frame_engine,
        selection_io,
    )?;
    let [global_low_hz, global_high_hz] = range.selected_edges_hz();
    let [reference_low_hz, _] = range.reference_edges_hz();
    let expansion_reference_frequency_hz = (global_low_hz + global_high_hz) / 2.0;
    let increment_hz = (global_high_hz - global_low_hz) / output_channels as f64;
    let first_channel_centre_hz = global_low_hz.max(reference_low_hz) + increment_hz / 2.0;
    let public_reference_frequency_hz =
        first_channel_centre_hz + (output_channels as f64 - 1.0) * increment_hz / 2.0;
    let expansion_reference_pixel =
        (expansion_reference_frequency_hz - first_channel_centre_hz) / increment_hz;
    let mut global_axis = axis.clone();
    global_axis.start = Some(casa_ms::CubeAxisValue::FrequencyHz {
        hz: first_channel_centre_hz,
        frame: Some(output_frequency_reference),
    });
    global_axis.width = Some(casa_ms::CubeAxisValue::FrequencyHz {
        hz: increment_hz,
        frame: Some(output_frequency_reference),
    });

    let mut setup = None;
    let mut selected_source_channels = BTreeMap::new();
    for window in spectral_windows {
        let (window_setup, support) = CubeSpectralSetup::for_casa_cube_axis(
            source_frequency_reference,
            &window.frequencies_hz,
            &window.channel_widths_hz,
            output_channels,
            &global_axis,
            anchor_time_mjd_seconds,
            field_id,
            Some(phase.clone()),
            time_bounds_mjd_seconds,
            frame_engine,
        )?;
        if setup.as_ref().is_some_and(|shared: &CubeSpectralSetup| {
            shared.output_channel_frequencies_hz != window_setup.output_channel_frequencies_hz
                || shared.output_channel_widths_hz != window_setup.output_channel_widths_hz
        }) {
            return Err(boxed(
                "selected MVC spectral windows did not resolve to one shared output axis",
            ));
        }
        setup.get_or_insert(window_setup);
        let selected = selected_by_spw
            .get(&window.spw_id)
            .expect("selected MVC SPW");
        let support = support.indices.into_iter().collect::<BTreeSet<_>>();
        let selected = selected
            .iter()
            .copied()
            .filter(|channel| support.contains(channel))
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Err(boxed(format!(
                "MVC output axis has no supporting source channels in SPW {}",
                window.spw_id
            )));
        }
        selected_source_channels.insert(window.spw_id, selected);
    }

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
                    return Err(boxed("MVC Doppler convention is not supported"));
                }
            },
        ),
    };
    // CASA's MVC helper forces the cube-major GridFT interpolation to nearest
    // after deriving the shared frequency range; task interpolation is not
    // carried into this internal cube (`imager_mtmfs_via_cube.py`).
    let sampling = SpectralSamplingLaw::NEAREST;
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
            channels: output_channels,
            reference_pixel: expansion_reference_pixel,
            reference_frequency_hz: expansion_reference_frequency_hz,
            increment_hz,
        },
        rest_frequency,
        image_rest_frequency_hz: axis
            .rest_frequency_hz
            .unwrap_or(expansion_reference_frequency_hz),
        doppler,
        sampling,
        basis: ReconstructionBasis::ChannelLocal {
            channels: output_channels,
        },
        output_channels,
        reference_frequency_hz: public_reference_frequency_hz,
        increment_hz: global_high_hz - global_low_hz,
    })
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
    let candidate_bindings = ddids
        .iter()
        .copied()
        .map(|ddid| {
            usize::try_from(ddid)
                .map_err(|_| boxed("selected DATA_DESC_ID is negative"))
                .and_then(|ddid| data_description_binding(&data_description, ddid))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let candidate_spw_ids = candidate_bindings
        .iter()
        .map(|(spw_id, _)| *spw_id)
        .collect::<BTreeSet<_>>();
    let mut spectral_windows = Vec::with_capacity(candidate_spw_ids.len());
    for spw_id in candidate_spw_ids {
        let frequency_reference =
            FrequencyRef::from_casacore_code(spectral_window.meas_freq_ref(spw_id)?)
                .ok_or_else(|| boxed("selected SPW has an unsupported frequency frame"))?;
        spectral_windows.push(SourceSpectralWindow {
            spw_id,
            frequency_reference,
            frequencies_hz: spectral_window.chan_freq(spw_id)?,
            channel_widths_hz: spectral_window.chan_width(spw_id)?,
        });
    }
    let mut continuum_selected_source_channels =
        if matches!(request.spectral_mode, SpectralImagingMode::Continuum) {
            Some(
                spectral_windows
                    .iter()
                    .map(|window| {
                        Ok((
                            window.spw_id,
                            selected_channels(&request, window.spw_id, &window.frequencies_hz)?,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, crate::ApplicationError>>()?,
            )
        } else {
            None
        };
    let frame_engine = casa_ms::derived::engine::MsCalEngine::new(&ms)?;
    let mut continuum_spectral_reducer = continuum_selected_source_channels
        .as_ref()
        .map(|selected| {
            ms.selected_observation_spectral_envelope_reducer(
                &row_selection,
                spectral_windows.iter().map(|window| {
                    SelectedObservationSpectralWindow::borrow_selected(
                        u32::try_from(window.spw_id).expect("nonnegative i32 SPW fits u32"),
                        window.frequency_reference,
                        &window.frequencies_hz,
                        &window.channel_widths_hz,
                        selected
                            .get(&window.spw_id)
                            .expect("selected every candidate spectral window"),
                    )
                }),
                FrequencyRef::LSRK,
                &frame_engine,
                content_budget.available_bytes(),
            )
        })
        .transpose()?;
    let spectral_reducer_bytes = continuum_spectral_reducer.as_ref().map_or(
        0,
        SelectedObservationSpectralEnvelopeReducer::retained_bytes,
    );
    let row_available_bytes = content_budget
        .available_bytes()
        .checked_sub(spectral_reducer_bytes)
        .ok_or_else(|| boxed("selected spectral envelope exhausts the row traversal budget"))?;
    let mut selected_rows = SelectedRowsBuilder::with_data_description_capacity(
        u64::try_from(ms.row_count()).map_err(|_| boxed("MS row count exceeds u64"))?,
        ddids.len(),
    );
    let mut selected_rows_error = None;
    let mut selected_ddids = BTreeSet::new();
    let mut selected_fields = BTreeSet::new();
    let mut selected_observation_ids = BTreeSet::new();
    let mut first_selected_time_mjd_seconds = None;
    let mut selected_time_bounds_mjd_seconds = [f64::INFINITY, f64::NEG_INFINITY];
    let mut maximum_selected_abs_w_m = 0.0_f64;
    let main_table = ms.main_table();
    let mut weight_spectrum_complete = main_table.column_accessor("WEIGHT_SPECTRUM").is_ok();
    let mut weight_spectrum_error = None;
    let mut spectral_envelope_error = None;
    ms.visit_selected_observation_rows(
        &row_selection,
        MsSelectionIoBudget {
            available_bytes: row_available_bytes,
            maximum_live_blocks: content_budget.maximum_live_blocks(),
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| {
            if spectral_envelope_error.is_none()
                && let Some(reducer) = continuum_spectral_reducer.as_mut()
            {
                spectral_envelope_error = reducer.observe(row).err();
            }
            if weight_spectrum_complete {
                match main_table
                    .column_accessor("WEIGHT_SPECTRUM")
                    .and_then(|column| column.array_cell_is_defined_uncached(row.physical_row()))
                {
                    Ok(defined) => weight_spectrum_complete = defined,
                    Err(error) => weight_spectrum_error = Some(error),
                }
            }
            selected_ddids.insert(row.data_description_id());
            selected_fields.insert(row.field_id());
            selected_observation_ids.insert(row.observation_id());
            first_selected_time_mjd_seconds.get_or_insert(row.time_mjd_seconds());
            selected_time_bounds_mjd_seconds[0] =
                selected_time_bounds_mjd_seconds[0].min(row.time_mjd_seconds());
            selected_time_bounds_mjd_seconds[1] =
                selected_time_bounds_mjd_seconds[1].max(row.time_mjd_seconds());
            let [_, _, w_m] = row.uvw_m();
            maximum_selected_abs_w_m = maximum_selected_abs_w_m.max(w_m.abs());
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
    if let Some(error) = spectral_envelope_error {
        return Err(Box::new(error));
    }
    let continuum_spectral_envelope = continuum_spectral_reducer
        .map(SelectedObservationSpectralEnvelopeReducer::finish)
        .transpose()?;
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
    spectral_windows.retain(|window| spw_ids.contains(&window.spw_id));
    if let Some(selected) = continuum_selected_source_channels.as_mut() {
        selected.retain(|spw_id, _| spw_ids.contains(spw_id));
    }
    let mut source_frequency_reference = None;
    for window in &spectral_windows {
        let reference = window.frequency_reference;
        if source_frequency_reference.is_some_and(|selected| selected != reference) {
            return Err(boxed(
                "selected spectral windows use different source frequency frames",
            ));
        }
        source_frequency_reference.get_or_insert(reference);
    }
    let stored_phase = casa_ms::derived::engine::raw_field_phase_direction(&ms, field_id)?;
    let phase = casa_ms::derived::engine::resolve_field_phase_direction_j2000(&ms, field_id)?;
    let default_main_direction = SkyDirection::new(
        DirectionFrame::J2000,
        phase.as_angles().0,
        phase.as_angles().1,
    );
    let anchor_time_mjd_seconds =
        first_selected_time_mjd_seconds.expect("nonempty selected row traversal");
    let measures_provider = casa_ms::open_measures_runtime()?;
    let measures_identity =
        SelectedObservationMeasures::new(Arc::clone(&measures_provider))?.identity();
    let (phase_centre_law, ephemeris, main_direction) = match request.phase_center.as_deref() {
        None => (
            PhaseCentreLaw::Fixed(default_main_direction),
            None,
            default_main_direction,
        ),
        Some(text) if text.split_ascii_whitespace().next() == Some("J2000") => {
            let direction = parse_phase_center_direction(text)?;
            (PhaseCentreLaw::Fixed(direction), None, direction)
        }
        Some("TRACKFIELD") => {
            let ephemeris = SelectedObservationEphemeris::tracked_fields(
                &ms,
                selected_fields
                    .iter()
                    .map(|field| usize::try_from(*field).expect("validated FIELD_ID")),
                content_budget.reference_data_budget(),
            )?;
            let direction = frame_engine.ephemeris_direction_j2000(
                anchor_time_mjd_seconds,
                field_id,
                "TRACKFIELD",
                &ephemeris,
            )?;
            let (longitude, latitude) = direction.as_angles();
            (
                PhaseCentreLaw::Ephemeris("TRACKFIELD".to_string()),
                Some(ephemeris),
                SkyDirection::new(DirectionFrame::J2000, longitude, latitude),
            )
        }
        Some(text) => {
            let mut ephemeris = if Path::new(text).is_dir() {
                SelectedObservationEphemeris::external(
                    text,
                    content_budget.reference_data_budget(),
                )?
            } else {
                SelectedObservationEphemeris::named(
                    text,
                    measures_identity,
                    content_budget.reference_data_budget(),
                )?
            };
            if let Some(attached) = attached_field_ephemerides(
                &ms,
                &selected_fields,
                content_budget.reference_data_budget(),
            )? {
                ephemeris = ephemeris
                    .with_attached_fields(attached, content_budget.reference_data_budget())?;
            }
            let direction = frame_engine.ephemeris_direction_j2000(
                anchor_time_mjd_seconds,
                field_id,
                text,
                &ephemeris,
            )?;
            let (longitude, latitude) = direction.as_angles();
            (
                PhaseCentreLaw::Ephemeris(text.to_string()),
                Some(ephemeris),
                SkyDirection::new(DirectionFrame::J2000, longitude, latitude),
            )
        }
    };
    let phase = MDirection::from_angles(
        main_direction.longitude_rad(),
        main_direction.latitude_rad(),
        DirectionRef::J2000,
    );
    let image_centre = if ephemeris.is_some() {
        let frame = frame_engine
            .spectral_frame_observatory_direction(anchor_time_mjd_seconds, phase.clone())?;
        phase.convert_to(DirectionRef::ICRS, &frame)?
    } else if request.phase_center.is_none()
        && matches!(
            stored_phase.refer(),
            DirectionRef::J2000 | DirectionRef::ICRS | DirectionRef::B1950 | DirectionRef::GALACTIC
        )
    {
        stored_phase
    } else {
        phase.clone()
    };
    let (right_ascension, declination) = image_centre.as_angles();
    let image_direction_frame = if ephemeris.is_some() {
        DirectionFrame::Icrs
    } else {
        match image_centre.refer() {
            DirectionRef::ICRS => DirectionFrame::Icrs,
            DirectionRef::B1950 => DirectionFrame::B1950,
            DirectionRef::GALACTIC => DirectionFrame::Galactic,
            _ => DirectionFrame::J2000,
        }
    };
    let direction = direction_spec_for_frame(
        request.image_size,
        request.cell_arcsec,
        image_direction_frame,
        right_ascension,
        declination,
    );
    let frequency_reference = source_frequency_reference.expect("nonempty selected SPWs");
    let moving_rest_frame = if matches!(
        request.spectral_mode,
        SpectralImagingMode::CubeSource { .. }
    ) {
        let ephemeris = ephemeris
            .as_ref()
            .ok_or_else(|| boxed("source-frame cube imaging requires a moving phase centre"))?;
        let velocity =
            frame_engine.ephemeris_radial_velocity(anchor_time_mjd_seconds, field_id, ephemeris)?;
        Some(
            frame_engine
                .spectral_frame_observatory_direction(anchor_time_mjd_seconds, phase.clone())?
                .with_radial_velocity(velocity),
        )
    } else {
        None
    };
    let prepared_spectral = prepare_spectral_axis(
        &request,
        &ms,
        &row_selection,
        MsSelectionIoBudget {
            available_bytes: content_budget.available_bytes(),
            maximum_live_blocks: content_budget.maximum_live_blocks(),
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        ephemeris.as_ref(),
        &spectral_windows,
        frequency_reference,
        anchor_time_mjd_seconds,
        selected_time_bounds_mjd_seconds,
        field_id,
        phase,
        direction,
        &frame_engine,
        moving_rest_frame.as_ref(),
        if matches!(
            request.spectral_mode,
            SpectralImagingMode::Cube { .. } | SpectralImagingMode::CubeSource { .. }
        ) {
            source_rest_frequency(&ms, field_id, &spectral_windows)?
        } else {
            None
        },
        continuum_selected_source_channels,
        continuum_spectral_envelope,
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
    let image_spectral_coordinate = ImageSpectralCoordinate {
        frequency_reference: prepared_spectral.output_frequency_reference,
        reference_frequency_hz: prepared_spectral.reference_frequency_hz,
        increment_hz: prepared_spectral.increment_hz,
        rest_frequency_hz: prepared_spectral.image_rest_frequency_hz,
    };
    let observation_id = if selected_observation_ids.len() == 1 {
        usize::try_from(
            *selected_observation_ids
                .first()
                .expect("one selected observation identifier"),
        )
        .map_err(|_| boxed("selected OBSERVATION_ID is negative"))?
    } else {
        return Err(boxed(format!(
            "image observation metadata requires one selected OBSERVATION_ID; found {selected_observation_ids:?}"
        )));
    };
    let observation = ms.observation()?;
    let (telescope_name, observer) = if observation_id < observation.row_count() {
        (
            observation.string(observation_id, "TELESCOPE_NAME")?,
            observation.string(observation_id, "OBSERVER")?,
        )
    } else {
        (String::new(), String::new())
    };
    let pointing_longitude = (right_ascension + std::f64::consts::PI)
        .rem_euclid(std::f64::consts::TAU)
        - std::f64::consts::PI;
    let observation_info = ObsInfo::new(telescope_name)
        .with_observer(observer)
        .with_date(MEpoch::from_mjd(
            anchor_time_mjd_seconds / 86_400.0,
            frame_engine.time_reference(),
        ))
        .with_telescope_position(frame_engine.observatory_position().clone())
        .with_pointing_center(pointing_longitude, declination);
    let mut prepared_domains = vec![PreparedImageDomain {
        role: ImageDomainRole::Main,
        output: request.image_name.clone(),
        image_size: request.image_size,
        direction,
        coordinates: image_coordinates(
            request.image_size,
            request.cell_arcsec,
            direction.reference_direction().frame(),
            [right_ascension, declination],
            &request.polarizations,
            image_spectral_coordinate,
            &observation_info,
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
                    direction.reference_direction().frame(),
                    [centre.longitude_rad(), centre.latitude_rad()],
                    &request.polarizations,
                    image_spectral_coordinate,
                    &observation_info,
                ),
                mask: input.mask,
            });
        }
    }
    prepared_domains.sort_by(|left, right| left.role.cmp(&right.role));
    let mosaic = request
        .task_requirements
        .contains(&TaskRequirement::MosaicGridder);
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
            phase_centre_law,
            DelayCentreLaw::PhaseTrackingCentre,
            if mosaic {
                PointingCentreLaw::Observation(ObservationPointingLaw::new(
                    PointingDirectionColumn::Direction,
                    PointingDirectionSemantic::AntennaBoresight,
                    PointingTimeSampling::VisibilityTimeCentroid,
                    PointingInterpolation::GreatCircleShortestArc,
                    PointingExtrapolation::Reject,
                    MissingPointingPolicy::Reject,
                ))
            } else {
                PointingCentreLaw::PhaseTrackingCentre
            },
        ),
        if mosaic {
            UvwCoordinateLaw::MosaicPhaseTrackingCentre
        } else {
            UvwCoordinateLaw::PhaseTrackingCentre
        },
        SpectralCoordinateSpec::new(
            prepared_spectral.source_frame,
            prepared_spectral.output_frame,
            prepared_spectral.anchor,
            prepared_spectral.wcs.clone(),
            prepared_spectral.rest_frequency,
            prepared_spectral.doppler,
        ),
    );
    let primary_beam_model = if mosaic {
        Some(casa_imaging_products::AnalyticPrimaryBeamModel::MosaicSensitivity)
    } else if request.write_primary_beam || request.pbcor {
        Some(standard_primary_beam_model(&ms)?)
    } else {
        None
    };
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
        .and_then(|profile| {
            let runtime = runtime(&request, &prepared_domains, &profile)?;
            let aw_preparation = request
                .aw_projection
                .as_ref()
                .map(|controls| {
                    let output_directory = request
                        .image_name
                        .parent()
                        .unwrap_or_else(|| Path::new("."))
                        .canonicalize()?;
                    Ok::<_, crate::ApplicationError>(crate::ApplicationAwPreparation {
                        casa_cache: controls.casa_cache.clone(),
                        private_root: output_directory.join(".casa-rs-aw-prepared"),
                        storage_domain: profile.storage_domain(),
                        resident_bytes: controls.resident_bytes,
                        conjugate_beams: controls.conjugate_beams,
                    })
                })
                .transpose()?;
            Ok(ApplicationNative {
                runtime,
                publication: ApplicationPublication {
                    controls: product_controls,
                    sink: product_sink,
                },
                aw_preparation,
            })
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
    let instrument = scientific_instrument_model(&request, &ms)?;
    let unit_response_validity = match primary_beam_model {
        Some(
            casa_imaging_products::AnalyticPrimaryBeamModel::CasaAlma12mAiry
            | casa_imaging_products::AnalyticPrimaryBeamModel::CasaAca7mAiry,
        ) => UnitResponseValidityPolicy::PrimaryBeam,
        _ => UnitResponseValidityPolicy::FinalNormalState,
    };
    let w_projection = request
        .task_requirements
        .contains(&TaskRequirement::WProjection)
        .then(|| {
            let maximum_frequency_hz = spectral_windows
                .iter()
                .flat_map(|window| window.frequencies_hz.iter().copied())
                .fold(0.0_f64, f64::max);
            let maximum_abs_w_lambda =
                maximum_selected_abs_w_m * maximum_frequency_hz / 299_792_458.0;
            let planes = request
                .w_projection_planes
                .map(|planes| {
                    std::num::NonZeroUsize::new(planes)
                        .ok_or_else(|| boxed("W-projection plane count must be positive"))
                })
                .transpose()?;
            WProjectionContract::new(maximum_abs_w_lambda, planes)
                .map_err(|error| Box::new(error) as crate::ApplicationError)
        })
        .transpose()?;
    let aw_projection = request
        .aw_projection
        .as_ref()
        .map(|controls| {
            let maximum_frequency_hz = spectral_windows
                .iter()
                .flat_map(|window| window.frequencies_hz.iter().copied())
                .fold(0.0_f64, f64::max);
            let maximum_abs_w_lambda =
                maximum_selected_abs_w_m * maximum_frequency_hz / 299_792_458.0;
            let planes = controls
                .w_plane_count
                .and_then(std::num::NonZeroUsize::new)
                .ok_or_else(|| {
                    boxed("AW projection requires an explicit positive W-plane count")
                })?;
            if controls.vp_table.is_some() {
                return Err(boxed(
                    "AW projection does not support a separate voltage-pattern table",
                ));
            }
            if controls
                .pointing_offset_sigdev
                .iter()
                .any(|value| !value.is_finite() || *value != 0.0)
            {
                return Err(boxed(
                    "AW projection does not support nonzero synthetic pointing-offset deviations",
                ));
            }
            if controls.mosaic_weighting {
                return Err(boxed(
                    "AW projection does not support mosaic weight-density mode",
                ));
            }
            if !controls.a_term {
                return Err(boxed("AW projection requires the EVLA aperture A term"));
            }
            if controls.ps_term {
                return Err(boxed(
                    "AW projection does not support a separate prolate-spheroidal term",
                ));
            }
            if !controls.wideband || !controls.conjugate_beams {
                return Err(boxed(
                    "AW projection requires wideband and conjugate-beam selection",
                ));
            }
            if planes.get() != 32 {
                return Err(boxed(
                    "AW projection currently requires the frozen 32-plane EVLA cache contract",
                ));
            }
            if controls.compute_pa_step_deg.to_bits() != 360.0_f64.to_bits()
                || controls.rotate_pa_step_deg.to_bits() != 360.0_f64.to_bits()
            {
                return Err(boxed(
                    "AW projection cache currently requires 360-degree parallactic-angle steps",
                ));
            }
            AwProjectionContract::new(
                maximum_abs_w_lambda,
                planes,
                controls.a_term,
                controls.ps_term,
                controls.wideband,
                controls.conjugate_beams,
                controls.use_pointing,
                controls.compute_pa_step_deg,
                controls.rotate_pa_step_deg,
            )
            .map_err(|error| Box::new(error) as crate::ApplicationError)
        })
        .transpose()?;
    let specification = match continuum_transform {
        Some(transform) => specification(
            &request,
            &prepared_spectral,
            instrument.map(|value| value.0),
            unit_response_validity,
            w_projection,
            aw_projection,
        )?
        .with_visibility_transform(transform),
        None => specification(
            &request,
            &prepared_spectral,
            instrument.map(|value| value.0),
            unit_response_validity,
            w_projection,
            aw_projection,
        )?,
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
            instrument
                .map(|value| vec![(ReferenceDataKind::Instrument, value.1)])
                .unwrap_or_default(),
            ModelStateIdentity::Empty,
            content_budget,
            measures_provider,
        )
        .with_ephemeris(ephemeris),
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

fn source_rest_frequency(
    measurement_set: &MeasurementSet,
    field_id: usize,
    spectral_windows: &[SourceSpectralWindow],
) -> Result<Option<f64>, crate::ApplicationError> {
    let source_id = measurement_set.field()?.source_id(field_id)?;
    if source_id < 0 {
        return Ok(None);
    }
    if measurement_set.subtable(SubtableId::Source).is_none() {
        return Ok(None);
    }
    let source = measurement_set.source()?;
    let selected = spectral_windows
        .iter()
        .map(|window| i32::try_from(window.spw_id))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut resolved = None;
    for row in 0..source.row_count() {
        if source.i32(row, "SOURCE_ID")? != source_id {
            continue;
        }
        let spectral_window_id = source.i32(row, "SPECTRAL_WINDOW_ID")?;
        if spectral_window_id >= 0 && !selected.contains(&spectral_window_id) {
            continue;
        }
        let Some(ArrayValue::Float64(values)) = source.optional_array(row, "REST_FREQUENCY")?
        else {
            continue;
        };
        let Some(value) = values.first().copied().filter(|value| *value > 0.0) else {
            continue;
        };
        if resolved.is_some_and(|prior: f64| prior.to_bits() != value.to_bits()) {
            return Err(boxed(
                "selected SOURCE rows disagree on REST_FREQUENCY metadata",
            ));
        }
        resolved = Some(value);
    }
    Ok(resolved)
}

fn cube_rest_frequency_hz(
    explicit_hz: Option<f64>,
    source_hz: Option<f64>,
    spectral_window: &SourceSpectralWindow,
) -> (Option<f64>, f64) {
    let resolved = explicit_hz.or(source_hz);
    (
        resolved,
        resolved.unwrap_or_else(|| spectral_window_midpoint_hz(spectral_window)),
    )
}

fn attached_field_ephemerides(
    measurement_set: &MeasurementSet,
    selected_fields: &BTreeSet<i32>,
    budget: casa_ms::SelectedObservationReferenceDataBudget,
) -> Result<Option<SelectedObservationEphemeris>, crate::ApplicationError> {
    let field = measurement_set.field()?;
    let mut attached = Vec::new();
    for field_id in selected_fields {
        let field_id = usize::try_from(*field_id)?;
        if field
            .ephemeris_id(field_id)?
            .is_some_and(|value| value >= 0)
        {
            attached.push(field_id);
        }
    }
    if attached.is_empty() {
        return Ok(None);
    }
    if attached.len() != selected_fields.len() {
        return Err(boxed(
            "moving-source selection mixes FIELD rows with and without attached ephemerides",
        ));
    }
    SelectedObservationEphemeris::tracked_fields(measurement_set, attached, budget)
        .map(Some)
        .map_err(Into::into)
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
    match telescopes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["ALMA"] => homogeneous_alma_primary_beam_model(
            ms,
            10.0..13.0,
            casa_imaging_products::AnalyticPrimaryBeamModel::CasaAlma12mAiry,
        ),
        ["ACA"] => homogeneous_alma_primary_beam_model(
            ms,
            6.0..8.0,
            casa_imaging_products::AnalyticPrimaryBeamModel::CasaAca7mAiry,
        ),
        _ => analytic_primary_beam_model_for_telescopes(&telescopes),
    }
}

fn homogeneous_alma_primary_beam_model(
    ms: &MeasurementSet,
    diameter_range_m: std::ops::Range<f64>,
    model: casa_imaging_products::AnalyticPrimaryBeamModel,
) -> Result<casa_imaging_products::AnalyticPrimaryBeamModel, crate::ApplicationError> {
    let antenna = ms.antenna()?;
    if antenna.row_count() == 0 {
        return Err(boxed(
            "ALMA primary-beam publication requires ANTENNA dish metadata",
        ));
    }
    for row in 0..antenna.row_count() {
        let diameter = antenna.dish_diameter(row)?;
        if !diameter.is_finite() || !diameter_range_m.contains(&diameter) {
            return Err(boxed(format!(
                "ALMA primary-beam publication requires one homogeneous dish class; row {row} has diameter {diameter} m"
            )));
        }
    }
    Ok(model)
}

fn scientific_instrument_model(
    request: &ContinuumImagingRequest,
    ms: &MeasurementSet,
) -> Result<Option<(InstrumentModel, LogicalIdentity)>, crate::ApplicationError> {
    let aw_projection = request.aw_projection.is_some();
    let mosaic = request
        .task_requirements
        .contains(&TaskRequirement::MosaicGridder);
    if !aw_projection
        && !mosaic
        && !matches!(
            request.spectral_mode,
            SpectralImagingMode::MtmfsViaCube { .. }
        )
    {
        return Ok(None);
    }
    let observation = ms.observation()?;
    let telescopes = (0..observation.row_count())
        .map(|row| {
            observation
                .string(row, "TELESCOPE_NAME")
                .map(|name| name.trim().to_string())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let telescope_names = telescopes.iter().map(String::as_str).collect::<Vec<_>>();
    let supported_telescope = if aw_projection {
        !telescope_names.is_empty()
            && telescope_names
                .iter()
                .all(|name| matches!(*name, "VLA" | "EVLA"))
    } else if mosaic {
        !telescope_names.is_empty()
            && telescope_names
                .iter()
                .all(|name| matches!(*name, "ALMA" | "ACA"))
    } else {
        telescope_names == ["ALMA"]
    };
    if !supported_telescope {
        return Err(boxed(format!(
            "requested instrument response is unsupported for observation metadata {telescopes:?}"
        )));
    }
    let antenna = ms.antenna()?;
    if antenna.row_count() == 0 {
        return Err(boxed(
            "channel-major primary-beam response requires ANTENNA dish metadata",
        ));
    }
    let mut hasher = Sha256::new();
    let instrument_model = if aw_projection {
        hasher.update(b"casa-rs-instrument-reference/casa-evla-wideband-aw-v1");
        InstrumentModel::CasaEvlaWidebandAwV1
    } else if mosaic {
        hasher.update(b"casa-rs-instrument-reference/casa-alma-aca-heterogeneous-response-v1");
        InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1
    } else {
        hasher.update(b"casa-rs-instrument-reference/casa-aca7m-direct-pb-v1");
        InstrumentModel::CasaAca7mInterferometricDirectPbV1
    };
    hasher.update((telescopes.len() as u64).to_le_bytes());
    for telescope in &telescopes {
        hasher.update((telescope.len() as u64).to_le_bytes());
        hasher.update(telescope.as_bytes());
    }
    hasher.update((antenna.row_count() as u64).to_le_bytes());
    for row in 0..antenna.row_count() {
        let diameter = antenna.dish_diameter(row)?;
        let supported_diameter = if aw_projection {
            diameter.is_finite() && (diameter - 25.0).abs() < 1.0
        } else {
            instrument_model_supports_diameter(mosaic, diameter)
        };
        if !supported_diameter {
            let expected = if aw_projection {
                "one EVLA/VLA 25 m antenna class"
            } else if mosaic {
                "CASA 12 m or 7 m antenna classes"
            } else {
                "one homogeneous ACA 7 m antenna class"
            };
            return Err(boxed(format!(
                "ALMA/ACA response requires {expected}; row {row} has diameter {diameter} m"
            )));
        }
        hasher.update(diameter.to_bits().to_le_bytes());
    }
    Ok(Some((
        instrument_model,
        LogicalIdentity::from_sha256(hasher.finalize().into()),
    )))
}

fn instrument_model_supports_diameter(mosaic: bool, diameter_m: f64) -> bool {
    diameter_m.is_finite()
        && if mosaic {
            (diameter_m - 12.0).abs() < 0.5 || (diameter_m - 7.0).abs() < 1.0
        } else {
            (6.0..8.0).contains(&diameter_m)
        }
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
        ["VLA"] => Ok(casa_imaging_products::AnalyticPrimaryBeamModel::CasaVlaBand),
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
        || !request.primary_beam_cutoff.is_finite()
        || !(0.0..1.0).contains(&request.primary_beam_cutoff)
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
    if request.w_projection_planes.is_some()
        && !request
            .task_requirements
            .contains(&TaskRequirement::WProjection)
    {
        return Err(boxed(
            "w_projection_planes requires the explicit W-projection task capability",
        ));
    }
    if request.save_continuum_residual && request.continuum_subtraction.is_none() {
        return Err(boxed(
            "CORRECTED_DATA residual persistence requires continuum subtraction",
        ));
    }
    if request.continuum_subtraction.is_some()
        && !matches!(
            request.spectral_mode,
            SpectralImagingMode::Cube { .. } | SpectralImagingMode::CubeSource { .. }
        )
    {
        return Err(boxed(
            "visibility-domain continuum subtraction requires channel-local cube imaging",
        ));
    }
    if request.outlier_file.is_some()
        && (!matches!(
            request.algorithm,
            ContinuumAlgorithm::Dirty | ContinuumAlgorithm::Hogbom
        ) || request.weighting != ContinuumWeighting::Natural)
    {
        return Err(boxed(
            "the installed multi-domain slice requires natural weighting and dirty or Hogbom reconstruction",
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
        FrequencyRef::REST => Ok(FrequencyFrame::Rest),
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
    direction_spec_for_frame(
        image_size,
        cell_arcsec,
        DirectionFrame::J2000,
        right_ascension,
        declination,
    )
}

fn direction_spec_for_frame(
    image_size: usize,
    cell_arcsec: f64,
    frame: DirectionFrame,
    right_ascension: f64,
    declination: f64,
) -> DirectionCoordinateSpec {
    let cell = cell_arcsec * std::f64::consts::PI / (180.0 * 3600.0);
    let reference_pixel = image_reference_pixel(image_size);
    DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(frame, right_ascension, declination),
        [reference_pixel, reference_pixel],
        [-cell, cell],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    )
}

fn image_coordinates(
    image_size: usize,
    cell_arcsec: f64,
    direction_frame: DirectionFrame,
    phase: [f64; 2],
    polarizations: &[PolarizationCoordinate],
    spectral: ImageSpectralCoordinate,
    observation: &ObsInfo,
) -> CoordinateSystem {
    let cell = cell_arcsec * std::f64::consts::PI / (180.0 * 3600.0);
    let reference_pixel = image_reference_pixel(image_size);
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        direction_ref(direction_frame),
        CoordinateProjection::new(ProjectionType::SIN),
        phase,
        [-cell, cell],
        [reference_pixel, reference_pixel],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(
        polarizations.iter().copied().map(stokes_type).collect(),
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        spectral.frequency_reference,
        spectral.reference_frequency_hz,
        spectral.increment_hz,
        0.0,
        spectral.rest_frequency_hz,
    ));
    *coordinates.obs_info_mut() = observation.clone();
    coordinates
}

#[derive(Clone, Copy)]
struct ImageSpectralCoordinate {
    frequency_reference: FrequencyRef,
    reference_frequency_hz: f64,
    increment_hz: f64,
    rest_frequency_hz: f64,
}

const fn direction_ref(frame: DirectionFrame) -> DirectionRef {
    match frame {
        DirectionFrame::J2000 => DirectionRef::J2000,
        DirectionFrame::Icrs => DirectionRef::ICRS,
        DirectionFrame::B1950 => DirectionRef::B1950,
        DirectionFrame::Galactic => DirectionRef::GALACTIC,
    }
}

fn spectral_window_midpoint_hz(window: &SourceSpectralWindow) -> f64 {
    let first = window.frequencies_hz[0];
    let last = window.frequencies_hz[window.frequencies_hz.len() - 1];
    first + (last - first) / 2.0
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
    instrument_model: Option<InstrumentModel>,
    unit_response_validity: UnitResponseValidityPolicy,
    w_projection: Option<WProjectionContract>,
    aw_projection: Option<AwProjectionContract>,
) -> Result<ProblemSpecification, crate::ApplicationError> {
    let mosaic = request
        .task_requirements
        .contains(&TaskRequirement::MosaicGridder);
    let algorithm = reconstruction_algorithm(&request.algorithm);
    let basis = match (&request.spectral_mode, &request.algorithm) {
        (SpectralImagingMode::MtmfsViaCube { .. }, ContinuumAlgorithm::Mtmfs { terms, .. }) => {
            ReconstructionBasis::TaylorViaChannelMajor {
                terms: *terms,
                channels: spectral.output_channels,
            }
        }
        (_, ContinuumAlgorithm::Mtmfs { terms, .. }) => {
            ReconstructionBasis::Taylor { terms: *terms }
        }
        (
            _,
            ContinuumAlgorithm::JointContinuumLine {
                continuum_terms,
                line_channels,
                ..
            },
        ) => ReconstructionBasis::JointContinuumLine {
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
    let measurement_equation = MeasurementEquationContract::new(
        if instrument_model.is_some() {
            InstrumentResponse::PrimaryBeam
        } else {
            InstrumentResponse::Scalar
        },
        DeclaredInnerProducts::new(
            ModelInnerProduct::HermitianEuclidean,
            VisibilityInnerProduct::HermitianEuclidean,
        ),
    );
    let measurement_equation = w_projection.map_or(measurement_equation, |contract| {
        measurement_equation.with_w_projection(contract)
    });
    let measurement_equation = aw_projection.map_or(measurement_equation, |contract| {
        measurement_equation.with_aw_projection(contract)
    });
    let mut science = ScientificContract::new(
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
        measurement_equation,
    );
    if let Some(model) = instrument_model {
        science = science.with_instrument_model(model);
    }
    Ok(ProblemSpecification::new(
        science,
        reconstruction,
        WeightingContract::new(weighting, density),
        ProductRequirements::new(
            requested_products(
                &request.algorithm,
                request.normalization,
                mosaic,
                request.write_primary_beam,
                request.pbcor,
            ),
            request.normalization,
            match (&request.algorithm, request.beam_policy) {
                (ContinuumAlgorithm::Mtmfs { .. }, _)
                | (ContinuumAlgorithm::JointContinuumLine { .. }, _) => RestoringBeamPolicy::Common,
                (_, ContinuumBeamPolicy::PerPlane) => RestoringBeamPolicy::PerPlane,
                (_, ContinuumBeamPolicy::Common) => RestoringBeamPolicy::Common,
            },
            ProductValidityPolicies::new(
                PrimaryBeamValidityPolicy::new(
                    request.primary_beam_cutoff,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )?,
                TaylorValidityPolicy::new(
                    TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
                    0.1,
                    ProductSupportComparison::StrictlyGreater,
                    ProductBlankingPolicy::ZeroAndFalseMask,
                )?,
            )
            .with_unit_response(unit_response_validity),
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

fn requested_products(
    algorithm: &ContinuumAlgorithm,
    normalization: ProductNormalization,
    mosaic: bool,
    write_primary_beam: bool,
    pbcor: bool,
) -> Vec<ProductKind> {
    let mut products = vec![
        ProductKind::Psf,
        ProductKind::Residual,
        ProductKind::Model,
        ProductKind::RestoredImage,
        ProductKind::SumWeights,
    ];
    if !matches!(algorithm, ContinuumAlgorithm::Dirty) {
        products.push(ProductKind::Mask);
    }
    products.push(ProductKind::Beam);
    if matches!(algorithm, ContinuumAlgorithm::Mtmfs { .. }) {
        products.extend([
            ProductKind::TaylorTerms,
            ProductKind::SpectralIndex,
            ProductKind::SpectralIndexError,
        ]);
    }
    if mosaic {
        products.push(ProductKind::Weight);
    }
    if !matches!(normalization, ProductNormalization::UnitResponse) {
        products.push(ProductKind::Sensitivity);
    }
    if write_primary_beam || pbcor {
        products.push(ProductKind::PrimaryBeam);
    }
    if pbcor {
        products.push(ProductKind::PbCorrectedImage);
        if mosaic && matches!(algorithm, ContinuumAlgorithm::Mtmfs { .. }) {
            products.push(ProductKind::PbCorrectedSpectralIndex);
        }
    }
    products
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
    let mut writable_directory = None;
    for domain in domains {
        let output_parent = domain.output.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(output_parent)?;
        let output_parent = output_parent.canonicalize()?;
        if filesystem_root(&output_parent)? != input_root {
            return Ok(None);
        }
        writable_directory.get_or_insert(output_parent);
    }
    let (capacity, available) = filesystem_capacity(&input_root)?;
    let read_rate = positive_environment("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND")?;
    let write_rate = positive_environment("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND")?;
    let queue_slots = u64::try_from(content_budget.maximum_live_blocks())
        .map_err(|_| boxed("selected source queue depth overflowed"))?
        .checked_add(1)
        .ok_or_else(|| boxed("managed-spill queue depth overflowed"))?;
    let profile = ProductionStorageProfile::new(
        input_root,
        capacity,
        available,
        read_rate,
        write_rate,
        queue_slots,
        2,
    )?;
    let profile = if request.aw_projection.is_some() {
        profile.with_measured_operations_rate(
            writable_directory
                .as_deref()
                .ok_or_else(|| boxed("AW preparation requires a writable output directory"))?,
        )?
    } else {
        profile
    };
    Ok(Some(profile))
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
        ManagedSpillStorage::bind(&authority, storage_io.clone(), &output_directory)?;
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
        resource_policy: request.resource_policy.clone(),
        cost_model: PlannerCostModelProfileId::from_sha256(hash(b"spectral-cycle-cost-v1"))
            .bootstrap(),
        authority,
        receipts: ExecutionReceiptStore::new(receipts, ReceiptRetention::new(512, 256 << 20)?)?,
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
        ContinuumAlgorithm, SourceSpectralWindow, TaskRequirement,
        analytic_primary_beam_model_for_telescopes, canonicalize_polarizations,
        cube_rest_frequency_hz, image_coordinates, image_reference_pixel,
        instrument_model_supports_diameter, model_plane_samples, parse_phase_center_direction,
        planned_minor_cycle_bytes, requested_products, resource_policy_for_task_requirements,
    };

    #[test]
    fn casa_direction_reference_pixel_uses_half_the_image_extent() {
        assert_eq!(image_reference_pixel(16), 8.0);
        assert_eq!(image_reference_pixel(15), 7.5);
    }

    #[test]
    fn cube_rest_frequency_follows_casa_precedence() {
        let window = SourceSpectralWindow {
            spw_id: 0,
            frequency_reference: FrequencyRef::LSRK,
            frequencies_hz: vec![44.0e9, 76.704e9, 109.408e9],
            channel_widths_hz: vec![1.0; 3],
        };

        assert_eq!(
            cube_rest_frequency_hz(Some(115.0e9), Some(110.0e9), &window),
            (Some(115.0e9), 115.0e9)
        );
        assert_eq!(
            cube_rest_frequency_hz(None, Some(110.0e9), &window),
            (Some(110.0e9), 110.0e9)
        );
        assert_eq!(
            cube_rest_frequency_hz(None, None, &window),
            (None, 76.704e9)
        );
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
            casa_imaging_model::DirectionFrame::J2000,
            [0.0, 0.0],
            &polarizations,
            super::ImageSpectralCoordinate {
                frequency_reference: FrequencyRef::LSRK,
                reference_frequency_hz: 1.0e9,
                increment_hz: 1.0,
                rest_frequency_hz: 1.0e9,
            },
            &casa_coordinates::ObsInfo::default(),
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
        let vla = std::collections::BTreeSet::from(["VLA".to_string()]);
        assert_eq!(
            analytic_primary_beam_model_for_telescopes(&vla).expect("VLA model"),
            casa_imaging_products::AnalyticPrimaryBeamModel::CasaVlaBand
        );
        let unsupported = std::collections::BTreeSet::from(["UNKNOWN".to_string()]);
        assert!(analytic_primary_beam_model_for_telescopes(&unsupported).is_err());
        assert!(
            analytic_primary_beam_model_for_telescopes(&std::collections::BTreeSet::new()).is_err()
        );
    }

    #[test]
    fn non_mosaic_direct_pb_accepts_only_the_homogeneous_aca7m_class() {
        assert!(instrument_model_supports_diameter(false, 7.0));
        assert!(!instrument_model_supports_diameter(false, 12.0));
        assert!(instrument_model_supports_diameter(true, 7.0));
        assert!(instrument_model_supports_diameter(true, 12.0));
    }

    #[test]
    fn dirty_execution_does_not_request_a_clean_mask() {
        let dirty = requested_products(
            &ContinuumAlgorithm::Dirty,
            casa_imaging_model::ProductNormalization::UnitResponse,
            false,
            true,
            false,
        );
        assert!(!dirty.contains(&casa_imaging_model::ProductKind::Mask));
        assert!(dirty.contains(&casa_imaging_model::ProductKind::PrimaryBeam));

        let clean = requested_products(
            &ContinuumAlgorithm::Hogbom,
            casa_imaging_model::ProductNormalization::UnitResponse,
            false,
            true,
            false,
        );
        assert!(clean.contains(&casa_imaging_model::ProductKind::Mask));
    }

    #[test]
    fn t47_mosaic_requests_weight_and_wideband_pb_correction() {
        let products = requested_products(
            &ContinuumAlgorithm::Mtmfs {
                terms: 2,
                scales_px: vec![0.0],
                small_scale_bias: 0.0,
            },
            casa_imaging_model::ProductNormalization::FlatNoise,
            true,
            true,
            true,
        );
        for product in [
            casa_imaging_model::ProductKind::Weight,
            casa_imaging_model::ProductKind::Sensitivity,
            casa_imaging_model::ProductKind::PbCorrectedSpectralIndex,
        ] {
            assert!(products.contains(&product));
        }
    }
}
