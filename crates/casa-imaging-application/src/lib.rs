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
mod compile;
#[doc(hidden)]
pub mod geometry_law;
mod imaging;
mod native;
mod request;
mod restart;
#[doc(hidden)]
pub mod restart_mask_law;
#[doc(hidden)]
pub mod route_law;
#[doc(hidden)]
pub mod source_law;
#[doc(hidden)]
pub mod start_model_law;
mod tclean;

pub use casa_imaging_deconvolution::CleanStop;
pub use casa_imaging_model::{
    HogbomIterationAccounting, PolarizationCoordinate, ProductNormalization, RestoringBeamPolicy,
};
pub use casa_imaging_operator::GridPrecision;
pub use casa_imaging_runtime::pass::{BackendChoice, Cancel};
pub use casa_imaging_runtime::{
    Admission, Diagnostics, HostResources, Phase, ResourcePolicy, ReturningAllocator, RunStatus,
    RunSummary, SCIENCE_TRACE, STAGE_TIMING, SummaryTarget, TracedComponent,
};
pub use compile::{OutlierProblem, PrepareError};
pub use request::{
    AwCfSource, AwProjection, BackendRequest, DataColumn, Deconvolver, Gridder, ImagingRequest,
    InvalidRequest, NativeAwCachePolicy, SourceRoute, SpecMode, UseMask, Weighting,
};
pub use tclean::TcleanRefusal;

use std::{error::Error, fmt, io, path::PathBuf, sync::Arc};

use casa_imaging_deconvolution::MajorCycleCompletion;
use casa_imaging_model::{
    CompileProblemError, CompiledProblem, ObservationSelection, ProblemInput,
    SpectralWindowSelection, compile, compile_observation,
};
use casa_imaging_products::{
    ContinuumProductControls, PlannedContinuumGeneration, PublishedContinuumGeneration,
    VisibilityProductCompletion,
};
use casa_imaging_runtime::run_phase;
use casa_ms::resolve_selected_observation;
use native::NativeInput;

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
    /// Where the run writes its summary, however it ends ([`RunSummary`]);
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
    pub(crate) diagnostics: Diagnostics,
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
    /// Whether the run raises the bound into the memory its chosen workers
    /// leave (`cf_resident_mb = auto`, #449).
    pub(crate) automatic: bool,
    /// A native cache's shared lock, held (never read) while the run lives
    /// so no other run regenerates the cells it reads lazily (#739 review);
    /// `None` for an imported CASA cache.
    pub(crate) _lock: Option<std::sync::Arc<casa_imaging_operator::NativeCacheLock>>,
    /// The workers that generated the native cache's cells for this run;
    /// `None` when it generated none (#708).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) generation_workers: Option<usize>,
}

/// Product-generation controls, the analytic primary beam's pointings and
/// the CASA image writer.
pub(crate) struct ApplicationPublication {
    pub(crate) controls: ContinuumProductControls,
    pub(crate) primary_beam_centres: Option<casa_imaging_products::PrimaryBeamCentres>,
    pub(crate) writer: casa_imaging_products::CasaImageWriter,
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
    /// The run's summary, as written to the context's summary target.
    pub summary: RunSummary,
    /// Final authoritative complete-data and model state; borrowed through
    /// [`Self::scientific`], so that it cannot outlive its charge.
    scientific: MajorCycleCompletion,
    /// Planned product generation used before member production.
    pub planned_products: PlannedContinuumGeneration,
    /// Payload-free authorized generation retained after publication.
    pub products: PublishedContinuumGeneration,
    /// The charge of the memory `scientific` holds resident, released when
    /// the outcome drops.
    _retained: casa_imaging_runtime::Reservation,
}

impl ImagingOutcome {
    /// The final authoritative complete-data and model state.
    ///
    /// It stays charged against the process's memory until the outcome
    /// drops, so it is only lent: a completion moved out of its outcome
    /// would outlive its charge.
    ///
    /// ```compile_fail
    /// fn detach(outcome: casa_imaging_application::ImagingOutcome) {
    ///     let _ = outcome.scientific;
    /// }
    /// ```
    #[must_use]
    pub const fn scientific(&self) -> &MajorCycleCompletion {
        &self.scientific
    }

    /// The CASA suffixes of the published products, for example `.image`.
    #[must_use]
    pub fn product_names(&self) -> Vec<String> {
        self.planned_products
            .members()
            .iter()
            .map(|member| member.name().to_string())
            .collect()
    }

    /// The deconvolved planes, `[fields, channels, stokes]`: the main image
    /// and each outlier, each with a cube's channels (one otherwise) and
    /// its Stokes planes, as CASA's `summaryminor` keys them.
    #[must_use]
    pub fn plane_layout(&self) -> [usize; 3] {
        let reconstruction = self.problem.reconstruction();
        let channels = match reconstruction.basis() {
            casa_imaging_model::ReconstructionBasis::ChannelLocal { channels } => channels,
            _ => 1,
        };
        [
            self.problem.geometry().domains().len(),
            channels,
            reconstruction.polarization().coordinates().len(),
        ]
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
    /// Signed first-term component flux the cycle added to the model.
    pub model_flux: f64,
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
    pub auto_mask: Option<casa_imaging_deconvolution::AutoMultithreshEvidence>,
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
/// implementation can run it ([`Unsupported`](availability::Unsupported)), and run it.
///
/// Preparation (validation, compile, availability, admitting the source and
/// forming the team) is the run's first phase. The context's summary target
/// receives the [`RunSummary`] however the run ends. A completed run whose
/// summary cannot be written fails, though its products are published: the
/// caller asked for the record and would otherwise read its absence as a
/// run that never ended. A run that fails returns its own error, which
/// matters more, and only logs a summary it cannot write.
pub fn execute(
    request: &ImagingRequest,
    context: RunContext,
) -> Result<ImagingOutcome, ApplicationDispatchError> {
    // The device is resolved once the problem is compiled; until then the
    // summary names the one asked for, and for `auto` the CPU.
    let asked = match request.backend {
        BackendRequest::Gpu => BackendChoice::Metal,
        BackendRequest::Auto | BackendRequest::Cpu => BackendChoice::Cpu,
    };
    let mut summary = RunSummary::begin(
        context
            .summary
            .as_ref()
            .map_or(serde_json::Value::Null, |target| target.request.clone()),
        context.policy.memory(&context.host),
        asked,
    );
    let result = prepare_and_run(request, &context, &mut summary);
    let Some(target) = &context.summary else {
        return result.map(|outcome| outcome(summary));
    };
    match result {
        Ok(outcome) => {
            let outcome = outcome(summary);
            outcome
                .summary
                .write(&RunStatus::Completed, &target.path)
                .map_err(|error| ApplicationDispatchError::Native(Box::new(error)))?;
            Ok(outcome)
        }
        Err(error) => {
            // A rejected continuation never replaces the previous successful record.
            if request.tclean_restart
                && matches!(
                    &error,
                    ApplicationDispatchError::Preparation(PrepareError::Restart { .. })
                )
            {
                return Err(error);
            }
            let status = match &error {
                ApplicationDispatchError::Cancelled => RunStatus::Cancelled,
                error => RunStatus::Failed {
                    error: error.to_string(),
                },
            };
            if let Err(write) = summary.write(&status, &target.path) {
                tracing::warn!("the failed run's summary could not be written: {write}");
            }
            Err(error)
        }
    }
}

/// The preparation phase and the native run, recorded in `summary`; a
/// completed run's outcome takes it once the caller has finished it.
fn prepare_and_run(
    request: &ImagingRequest,
    context: &RunContext,
    summary: &mut RunSummary,
) -> Result<impl FnOnce(RunSummary) -> ImagingOutcome + use<>, ApplicationDispatchError> {
    // The device the request resolves to, recorded in the summary even when
    // the rest of preparation then fails.
    let resolved = std::cell::Cell::new(None);
    // The Measures charge stays with the run.
    let prepared = run_phase(
        "preparation",
        &context.cancel,
        request.diagnostics,
        summary,
        || {
            let (problem, input, deployed, resident) = compile_run(request, context, &resolved)?;
            let run = native::prepare(&problem, input, deployed)?;
            Ok::<_, ApplicationDispatchError>((problem, run, resident))
        },
    );
    if let Some(backend) = resolved.get() {
        summary.backend = backend;
    }
    let (problem, run, _resident) = prepared?;
    Ok(native::run(&problem, run, summary)?)
}

/// Compile `request` for its native run, with the charge of the Measures
/// catalogs compiling loads for the run. They are admitted before compile
/// opens anything, after the process's caches
/// ([`casa_imaging_runtime::charge_process_caches`]);
/// the MeasurementSet walks are admitted once its rows are known
/// ([`compile::prepare`]) and released once the selected observation is
/// resolved. The device the request resolves to is set in
/// `resolved_backend` as soon as it is known.
fn compile_run(
    request: &ImagingRequest,
    context: &RunContext,
    resolved_backend: &std::cell::Cell<Option<BackendChoice>>,
) -> Result<
    (
        CompiledProblem,
        NativeInput,
        ApplicationNative,
        casa_imaging_runtime::Reservation,
    ),
    ApplicationDispatchError,
> {
    request
        .validate()
        .map_err(ApplicationDispatchError::Request)?;
    casa_imaging_runtime::charge_process_caches(&context.host, &context.policy)
        .map_err(ApplicationDispatchError::Admission)?;
    let mut resident = casa_imaging_runtime::admit(
        &context.host,
        &context.policy,
        &casa_imaging_runtime::Demand {
            phase: "measures",
            memory: casa_ms::MEASURES_RUNTIME_BYTES as u64,
        },
    )
    .map_err(ApplicationDispatchError::Admission)?;
    let prepared = compile::prepare(request, context).map_err(|error| match error {
        PrepareError::Admission(admission) => ApplicationDispatchError::Admission(admission),
        error => ApplicationDispatchError::Preparation(error),
    })?;
    // The static restart-mask plan and its clone stay charged through all passes
    // and publication; ordinary minor-cycle mask charges cover their derived masks.
    resident.join(prepared.mask_resident);
    let resolved = resolve_selected_observation(prepared.observation.clone())
        .map_err(|error| ApplicationDispatchError::Preparation(error.into()))?;
    let (snapshot, access) = resolved.into_parts();
    let observation = compile_observation(snapshot)
        .map_err(|error| ApplicationDispatchError::Preparation(error.into()))?;
    drop(prepared.walks);
    let problem = compile(ProblemInput::new(
        prepared.specification,
        prepared.geometry,
        observation,
        prepared.model_lifecycle,
    ))
    .map_err(ApplicationDispatchError::Compile)?;
    let backend = availability::resolve_backend(
        &problem,
        request.backend,
        request.gridprecision,
        &context.host,
    );
    resolved_backend.set(Some(backend));
    availability::check(&problem, backend, request.gridprecision, &context.host)
        .map_err(ApplicationDispatchError::Unavailable)?;
    let native = prepared
        .deployment
        .deploy(backend)
        .map_err(ApplicationDispatchError::Preparation)?;
    let input = NativeInput {
        observation: prepared.observation,
        initial_access: access,
        source_route: request.source_route,
        write_model_column: prepared.write_model_column,
        write_corrected_data: prepared.write_corrected_data,
        masks: prepared.masks,
        start_model: prepared.start_model,
        minor_cycle_image_response: prepared.minor_cycle_image_response,
    };
    Ok((problem, input, native, resident))
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

impl From<casa_imaging_runtime::Cancelled> for ApplicationDispatchError {
    fn from(_: casa_imaging_runtime::Cancelled) -> Self {
        Self::Cancelled
    }
}

fn boxed(message: &'static str) -> ApplicationError {
    Box::new(io::Error::other(message))
}
