// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Production composition owner for native imaging.
//!
//! This crate is the single application-layer seam: it compiles an
//! [`ImagingRequest`] against its MeasurementSet, checks the installed
//! implementation can run it, and runs it on the execution runtime.
//! Frontends submit requests here; they do not compose native execution
//! stages directly.

pub mod availability;
mod casa_product_sink;
mod compile;
mod imaging;
mod native;
mod request;

pub use casa_imaging_deconvolution::CleanStop;
pub use casa_imaging_model::{
    HogbomIterationAccounting, PolarizationCoordinate, ProductNormalization, RestoringBeamPolicy,
};
pub use casa_imaging_operator::GridPrecision;
pub use casa_imaging_runtime::pass::{BackendChoice, Cancel};
pub use casa_imaging_runtime::{
    Admission, HostResources, Phase, ResourcePolicy, RunSummary, SummaryTarget, TracedComponent,
};
pub use casa_product_sink::{CasaImageDomainOutput, CasaImageProductSink};
pub use compile::{OutlierProblem, PrepareError};
pub use request::{
    AwCfSource, AwProjection, DataColumn, Deconvolver, Gridder, ImagingRequest, InvalidRequest,
    NativeAwCachePolicy, SpecMode, UseMask, Weighting,
};

use std::{error::Error, fmt, io, path::PathBuf, sync::Arc};

use casa_imaging_model::{
    CompileProblemError, CompiledProblem, ObservationSelection, ProblemInput,
    SpectralWindowSelection, compile, compile_observation,
};
use casa_imaging_products::{
    ContinuumProductControls, PlannedContinuumGeneration, PublishedContinuumGeneration,
    VisibilityProductCompletion,
};
use casa_imaging_reconstruction::MajorCycleCompletion;
use casa_ms::resolve_selected_observation;
use native::{NativeError, NativeInput, run_native};

/// Boxed application failure accepted by the native application composition.
pub type ApplicationError = Box<dyn Error + Send + Sync>;

/// What a run needs besides its request: the host, how much of it the run
/// may use, the cancellation the caller sets, and where the run summary
/// goes.
#[derive(Clone, Debug)]
pub struct RunContext {
    /// The host the run admits its phases against.
    pub host: HostResources,
    /// How much of the host the run may use.
    pub policy: ResourcePolicy,
    /// Set (SIGINT in `casars-imager`) to stop the run at the next block
    /// boundary or phase; nothing is published.
    pub cancel: Cancel,
    /// Where the run writes its summary, whether it completes or fails;
    /// `None` writes no file.
    pub summary: Option<SummaryTarget>,
}

/// The host, policy, backend and cancellation of one native run.
#[derive(Clone, Debug)]
pub(crate) struct ApplicationRuntime {
    pub(crate) host: HostResources,
    pub(crate) resource_policy: ResourcePolicy,
    pub(crate) backend: BackendChoice,
    /// The requested grid precision; `None` is plan decision D2's rule.
    pub(crate) grid_precision: Option<GridPrecision>,
    pub(crate) cancel: Cancel,
    /// Directory the paged cube state of a channel-local run lives in.
    pub(crate) spill_directory: PathBuf,
    pub(crate) summary: Option<SummaryTarget>,
}

/// The runtime, product publication and AW catalog of the native run.
pub(crate) struct ApplicationNative {
    pub(crate) runtime: ApplicationRuntime,
    pub(crate) publication: ApplicationPublication,
    pub(crate) aw_catalog: Option<AwCatalogDeployment>,
}

/// The AW catalog a run opens (`AwCatalog::open_casa`): a directory of CASA
/// `CFS_*`/`WTCFS_*` cells, imported or generated natively at preparation,
/// its index rules and the resident cell bound.
#[derive(Clone, Debug)]
pub(crate) struct AwCatalogDeployment {
    pub(crate) root: PathBuf,
    pub(crate) indexing: casa_imaging_operator::AwIndexing,
    pub(crate) resident_bytes: usize,
}

/// Product-generation controls and the storage sink.
pub(crate) struct ApplicationPublication {
    pub(crate) controls: ContinuumProductControls,
    pub(crate) sink: CasaImageProductSink,
}

/// The result of one whole run.
pub struct ImagingOutcome {
    /// The compiled problem the run executed.
    pub problem: CompiledProblem,
    /// Ordered solve evidence captured before each major-cycle pass.
    pub minor_cycles: Vec<NativeMinorCycleOutcome>,
    /// Why cleaning stopped (CASA's `stopcode`); `None` for a dirty run or
    /// when a minor cycle cleaned nothing.
    pub stop: Option<CleanStop>,
    /// Number of executed major passes, including the initial pass.
    pub major_cycle_count: usize,
    /// Total component count charged to the reported task/controller budget.
    pub total_minor_iterations: usize,
    /// Total number of components actually applied across all minor cycles.
    pub total_actual_minor_iterations: usize,
    /// Size of the worker team the major-cycle passes ran on.
    pub workers: usize,
    /// Planes per wave of the most finely waved major-cycle pass; `None`
    /// when every pass held every plane at once.
    pub planes_per_wave: Option<u32>,
    /// Final per-visibility product identities and provenance, when requested.
    pub visibility_products: Option<VisibilityProductCompletion>,
    /// The run's phases, team and totals; the caller adds the request echo
    /// before writing it beside the products.
    pub summary: RunSummary,
    /// Final authoritative complete-data and model state.
    pub scientific: MajorCycleCompletion,
    /// Planned product generation used before member production.
    pub planned_products: PlannedContinuumGeneration,
    /// Payload-free authorized generation retained after publication.
    pub products: PublishedContinuumGeneration,
    /// The charge of the memory `scientific` holds resident, released when
    /// the outcome drops.
    _retained: casa_imaging_runtime::Reservation,
}

impl ImagingOutcome {
    /// The CASA suffixes of the published products, for example `.image`.
    #[must_use]
    pub fn product_names(&self) -> Vec<String> {
        self.planned_products
            .members()
            .iter()
            .map(|member| member.name().to_string())
            .collect()
    }
}

/// Stable application projection of the T21 owner evidence.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeMinorCycleOutcome {
    /// One-based minor-cycle ordinal within this reconstruction.
    pub cycle: usize,
    /// Cumulative controller iterations before this cycle started.
    pub iterations_entering: usize,
    /// Component count charged to the reported controller budget.
    pub iterations: usize,
    /// Cumulative controller iterations after this cycle completed.
    pub total_iterations: usize,
    /// Cumulative actual component count before this cycle started.
    pub actual_iterations_entering: usize,
    /// Number of components actually applied in this cycle.
    pub actual_iterations: usize,
    /// Cumulative actual component count after this cycle completed.
    pub total_actual_iterations: usize,
    /// Cumulative absolute component flux accepted in this cycle.
    pub total_flux: f64,
    /// Normalized residual peak at entry to this cycle.
    pub initial_peak_flux: f64,
    /// Final normalized residual peak.
    pub final_peak_flux: f64,
    /// Robust RMS used for `nsigma` stopping, when enabled.
    pub noise_rms: Option<f64>,
    /// Effective absolute/noise/cycle threshold used by the owner.
    pub effective_threshold: f64,
    /// Global absolute/noise threshold before applying the cycle threshold.
    pub global_threshold: f64,
    /// PSF-derived cycle threshold, when enabled.
    pub cycle_threshold: Option<f64>,
    /// Scientific terminal reason.
    pub stop_reason: NativeMinorCycleStopReason,
    /// Number of exact whole-plane residual refreshes (Clark's cycles).
    pub clark_refreshes: usize,
    /// One-based major replay ordinal associated with this cycle's accepted update.
    pub associated_replay_ordinal: usize,
    /// The first components of the cycle.
    pub recorded_components: Vec<casa_imaging_runtime::TracedComponent>,
    /// Exact x-major reconstruction support used for component placement.
    pub mask_support: Vec<bool>,
    /// Auto-multithreshold diagnostics, when that mask mode generated support.
    pub auto_mask: Option<casa_imaging_reconstruction::AutoMultithreshEvidence>,
}

/// Stable application spelling of the scientific minor-cycle terminal reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeMinorCycleStopReason {
    /// The normalized residual peak fell below the requested threshold.
    ThresholdReached,
    /// The bounded minor-cycle iteration budget was exhausted.
    IterationBound,
    /// A plane's peak residual rose more than 10% above its minimum in the
    /// cycle (CASA's plane stop code 4, any solver).
    Diverged,
}

impl NativeMinorCycleStopReason {
    /// The cycle's reason from its planes' CASA stop codes: divergence in any
    /// plane, else an iteration limit or early exit in any plane, else a
    /// threshold.
    #[must_use]
    pub fn from_planes(stops: &[casa_imaging_deconvolution::PlaneStop]) -> Self {
        use casa_imaging_deconvolution::PlaneStop;
        if stops.contains(&PlaneStop::Diverged) {
            Self::Diverged
        } else if stops
            .iter()
            .any(|stop| matches!(stop, PlaneStop::Iterations | PlaneStop::Exited))
        {
            Self::IterationBound
        } else {
            Self::ThresholdReached
        }
    }
}

/// Run one imaging request on the installed implementation: validate it,
/// compile it against its MeasurementSet, check once that the
/// implementation can run it ([`Unsupported`]), and run it.
pub fn execute(
    request: &ImagingRequest,
    context: RunContext,
) -> Result<ImagingOutcome, ApplicationDispatchError> {
    request
        .validate()
        .map_err(ApplicationDispatchError::Request)?;
    let prepared =
        compile::prepare(request, &context).map_err(ApplicationDispatchError::Preparation)?;
    let resolved = resolve_selected_observation(prepared.observation.clone())
        .map_err(|error| ApplicationDispatchError::Preparation(error.into()))?;
    let (snapshot, access) = resolved.into_parts();
    let observation = compile_observation(snapshot)
        .map_err(|error| ApplicationDispatchError::Preparation(error.into()))?;
    let problem = compile(ProblemInput::new(
        prepared.specification,
        prepared.geometry,
        observation,
        prepared.model_lifecycle,
    ))
    .map_err(ApplicationDispatchError::Compile)?;
    availability::check(
        &problem,
        request.backend,
        request.gridprecision,
        &context.host,
    )
    .map_err(ApplicationDispatchError::Unavailable)?;
    let native = prepared
        .deployment
        .deploy()
        .map_err(ApplicationDispatchError::Preparation)?;
    let input = NativeInput {
        observation: prepared.observation,
        initial_access: access,
        write_model_column: prepared.write_model_column,
        write_corrected_data: prepared.write_corrected_data,
        masks: prepared.masks,
        minor_cycle_image_response: prepared.minor_cycle_image_response,
    };
    run_native(&problem, input, native).map_err(|error| match error {
        NativeError::Admission(admission) => ApplicationDispatchError::Admission(admission),
        NativeError::Cancelled(_) => ApplicationDispatchError::Cancelled,
        NativeError::Other(error) => ApplicationDispatchError::Native(error),
    })
}

pub(crate) fn visibility_write_selection(
    problem: &CompiledProblem,
    selected: Arc<ObservationSelection>,
) -> Result<Arc<ObservationSelection>, ApplicationError> {
    let Some(transform) = problem.visibility_transform() else {
        return Ok(selected);
    };
    let spectral_windows = selected
        .spectral_windows()
        .iter()
        .map(|selection| {
            let output_channels = selection
                .channel_indices()
                .iter()
                .copied()
                .filter(|channel| {
                    transform.rules().iter().any(|rule| {
                        rule.spectral_window_id() == selection.spectral_window_id()
                            && rule
                                .channel_use(*channel)
                                .is_some_and(|role| role.contributes_to_output())
                    })
                })
                .collect::<Vec<_>>();
            if output_channels.is_empty() {
                return Err(boxed(
                    "continuum transform selected no visibility-write output channels",
                ));
            }
            Ok(SpectralWindowSelection::new(
                selection.spectral_window_id(),
                output_channels,
            ))
        })
        .collect::<Result<Vec<_>, ApplicationError>>()?;
    Ok(Arc::new(ObservationSelection::new(
        selected.rows().clone(),
        selected.rows_filter().clone(),
        selected.data_descriptions().to_vec(),
        spectral_windows,
        selected.correlations().to_vec(),
    )))
}

/// Failure before or within the installed whole-run implementation.
#[derive(Debug)]
pub enum ApplicationDispatchError {
    /// The request's parameters contradict each other.
    Request(InvalidRequest),
    /// The request could not be compiled against its MeasurementSet, or
    /// the compiled run could not be deployed; nothing ran.
    Preparation(PrepareError),
    /// Backend-independent request compilation failed.
    Compile(CompileProblemError),
    /// The installed implementation cannot run the compiled problem.
    Unavailable(availability::ImplementationUnavailable),
    /// A phase's memory did not fit the resource policy; nothing was
    /// published.
    Admission(Admission),
    /// The run was cancelled; nothing was published.
    Cancelled,
    /// The sole installed implementation failed.
    Native(ApplicationError),
}

impl fmt::Display for ApplicationDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(error) => error.fmt(formatter),
            Self::Preparation(error) => {
                write!(formatter, "imaging application preparation failed: {error}")
            }
            Self::Compile(error) => {
                write!(formatter, "imaging request compilation failed: {error}")
            }
            Self::Unavailable(error) => error.fmt(formatter),
            Self::Admission(error) => write!(formatter, "native imaging run refused: {error}"),
            Self::Cancelled => write!(formatter, "native imaging run cancelled"),
            Self::Native(error) => write!(formatter, "native imaging run failed: {error}"),
        }
    }
}

impl Error for ApplicationDispatchError {}

fn boxed(message: &'static str) -> ApplicationError {
    Box::new(io::Error::other(message))
}
