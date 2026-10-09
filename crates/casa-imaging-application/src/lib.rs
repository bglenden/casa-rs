// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Production composition owner for native imaging.
//!
//! This crate is the single application-layer seam that binds compiled request
//! availability to MeasurementSet observation authority, scientific
//! reconstruction and products, and the physical execution runtime. Frontends
//! submit requests here; they do not compose native execution stages directly.

mod availability;
mod casa_product_sink;
mod continuum_domains;
mod continuum_request;
mod imaging;
pub use availability::{
    ImagingCapabilityCatalogEntry, ImagingCapabilityRequirement, ImplementationUnavailable,
    TaskRequirement, UnsupportedRequirement, installed_imaging_capability_catalog,
    validate_installed_implementation,
};
pub use casa_imaging_deconvolution::CleanStop;
pub use casa_imaging_model::{
    HogbomIterationAccounting, ImagingRequestVersion, PolarizationCoordinate, ProductNormalization,
};
pub use casa_imaging_runtime::pass::{BackendChoice, Cancel};
pub use casa_imaging_runtime::{
    Admission, HostResources, Phase, ResourcePolicy, RunSummary, TracedComponent,
};
pub use casa_product_sink::{CasaImageDomainOutput, CasaImageProductSink};
pub use continuum_request::{
    ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumAwCfSource, ContinuumAwProjection,
    ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumImagingResult, ContinuumMask,
    ContinuumMaskBox, ContinuumWeighting, NativeAwCachePolicy, NativeEvlaAwCache,
    SpectralImagingMode, VisibilityContinuumSubtraction, execute_continuum,
    resource_policy_for_task_requirements,
};

use std::{error::Error, fmt, io, path::PathBuf, sync::Arc};

use casa_imaging_model::{
    CompileProblemError, CompiledProblem, GeometryInput, ImagingRequest,
    ModelLifecycleRequirements, ObservationSelection, ProblemInputIdentities, ProblemSpecification,
    SpectralWindowSelection, compile, compile_observation,
};
use casa_imaging_products::{
    ContinuumProductControls, ContinuumProductInputs, PlannedContinuumGeneration,
    ProductStoragePlan, PublishedContinuumGeneration, VisibilityProductCompletion,
    produce_continuum_members,
};
use casa_imaging_reconstruction::{
    ImageDomainReconstructionMaskPlans, MajorCycleCompletion, MinorCycleImageResponse,
    ReconstructionMaskSet,
};
use casa_imaging_runtime::pass::WorkerTeam;
use casa_imaging_runtime::{
    Cancelled, Demand, SourceAccessError, admit, finalize_source_access, run_phase,
};
use casa_ms::{
    ResolvedSelectedObservationAccess, SelectedObservationResolutionRequest,
    resolve_selected_observation,
};

/// Boxed application failure accepted by the native application composition.
pub type ApplicationError = Box<dyn Error + Send + Sync>;

/// The host, policy, backend and cancellation of one native run.
#[derive(Clone, Debug)]
pub struct ApplicationRuntime {
    /// The host the run admits its phases against.
    pub host: HostResources,
    /// How much of the host the run may use.
    pub resource_policy: ResourcePolicy,
    /// Where the major-cycle passes grid (`backend`).
    pub backend: BackendChoice,
    /// Set to stop the run at the next block boundary or phase.
    pub cancel: Cancel,
    /// Directory the paged cube state of a channel-local run lives in.
    pub spill_directory: PathBuf,
}

/// Exact native request template resolved at the sole application boundary.
pub struct ApplicationRequest {
    /// Backend-independent scientific and product contract.
    pub specification: ProblemSpecification,
    /// Requested image geometry.
    pub geometry: GeometryInput,
    /// Initial-model lifecycle contract.
    pub model_lifecycle: ModelLifecycleRequirements,
    /// Deferred reconstruction-mask owner input.
    pub masks: ImageDomainReconstructionMaskPlans,
    /// Scientific image-coordinate normalization bound independently of output selection.
    pub minor_cycle_image_response: Option<MinorCycleImageResponse>,
    /// Storage-owner request for the single selected MeasurementSet.
    pub observation: SelectedObservationResolutionRequest,
    /// Whether final paired-operator predictions are committed to `MODEL_DATA`.
    pub write_model_column: bool,
    /// Whether transformed output-role observations overwrite existing `CORRECTED_DATA`.
    pub write_corrected_data: bool,
    /// Task-surface constraints that cannot be inferred from the compiled
    /// backend-independent problem.
    pub task_requirements: Vec<TaskRequirement>,
    /// Native-only deployment inputs evaluated after request compilation.
    /// A preparation error is terminal; there is no alternate execution path.
    pub native: Result<ApplicationNative, ApplicationError>,
}

/// Runtime and publication inputs consumed only by the Native engine port.
pub struct ApplicationNative {
    /// The run's host, policy, backend and cancellation.
    pub runtime: ApplicationRuntime,
    /// Product-generation and independently atomic publication configuration.
    pub publication: ApplicationPublication,
    /// The AW convolution-function catalog of an A-projection run.
    pub aw_catalog: Option<AwCatalogDeployment>,
}

/// The AW catalog a run opens (`AwCatalog::open_casa`): a directory of CASA
/// `CFS_*`/`WTCFS_*` cells, imported or generated natively at preparation,
/// its index rules and the resident cell bound.
#[derive(Clone, Debug)]
pub struct AwCatalogDeployment {
    /// Directory holding the cell images.
    pub root: PathBuf,
    /// Conjugate-beam and parallactic-angle cell rules.
    pub indexing: casa_imaging_operator::AwIndexing,
    /// Largest number of bytes of cells resident at once.
    pub resident_bytes: usize,
}

/// Product-generation controls and the storage sink.
pub struct ApplicationPublication {
    /// Scientific continuum-product controls.
    pub controls: ContinuumProductControls,
    /// Storage adapter that privately stages and atomically publishes members.
    pub sink: CasaImageProductSink,
}

/// Typed native result of one whole run.
pub struct NativeApplicationOutcome {
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
    /// Immutable mask generation used for this cycle.
    pub mask_generation: casa_imaging_reconstruction::ReconstructionMaskGenerationId,
    /// Exact model generation constrained by this mask.
    pub mask_model_generation: casa_imaging_reconstruction::ModelGenerationId,
    /// Current Normal State consumed to generate an automatic mask.
    pub mask_normal_state: Option<casa_imaging_reconstruction::FinalNormalStateCompletionId>,
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

/// Whole-run result from the sole installed implementation.
pub struct ApplicationOutcome {
    /// Output returned by the installed implementation.
    pub output: Box<NativeApplicationOutcome>,
}

/// Execute one imaging request through the sole installed implementation.
/// Unsupported requirements fail typed before physical planning or execution.
pub fn execute(
    request: ApplicationRequest,
) -> Result<ApplicationOutcome, ApplicationDispatchError> {
    let resolved = resolve_selected_observation(request.observation.clone())
        .map_err(|error| ApplicationDispatchError::Preparation(Box::new(error)))?;
    let (snapshot, access) = resolved.into_parts();
    let observation = compile_observation(snapshot)
        .map_err(|error| ApplicationDispatchError::Preparation(Box::new(error)))?;
    let imaging = ImagingRequest::new(
        request.specification,
        request.geometry,
        ProblemInputIdentities::new(observation),
        request.model_lifecycle,
    );
    let problem = compile(imaging).map_err(ApplicationDispatchError::Compile)?;
    validate_installed_implementation(&problem, request.task_requirements)
        .map_err(ApplicationDispatchError::Unavailable)?;
    let input = NativeInput {
        observation: request.observation,
        initial_access: access,
        write_model_column: request.write_model_column,
        write_corrected_data: request.write_corrected_data,
        masks: request.masks,
        minor_cycle_image_response: request.minor_cycle_image_response,
        native: request.native,
    };
    let output =
        run_native(&problem, input).map_err(|error| match error.downcast::<Admission>() {
            Ok(admission) => ApplicationDispatchError::Admission(*admission),
            Err(error) => match error.downcast::<Cancelled>() {
                Ok(_) => ApplicationDispatchError::Cancelled,
                Err(error) => ApplicationDispatchError::Native(error),
            },
        })?;
    Ok(ApplicationOutcome {
        output: Box::new(output),
    })
}

struct NativeInput {
    observation: SelectedObservationResolutionRequest,
    initial_access: ResolvedSelectedObservationAccess,
    write_model_column: bool,
    write_corrected_data: bool,
    masks: ImageDomainReconstructionMaskPlans,
    minor_cycle_image_response: Option<MinorCycleImageResponse>,
    native: Result<ApplicationNative, ApplicationError>,
}

/// Run the major-cycle passes, the minor cycles between them and any
/// visibility write, then publish the products.
fn run_native(
    problem: &CompiledProblem,
    input: NativeInput,
) -> Result<NativeApplicationOutcome, ApplicationError> {
    let ApplicationNative {
        runtime,
        publication,
        aw_catalog,
    } = input.native?;
    publication.controls.validate_for_problem(problem)?;
    let visibility_write = (input.write_model_column || input.write_corrected_data)
        .then(|| {
            Ok::<_, ApplicationError>(imaging::VisibilityWriteTarget {
                path: PathBuf::from(input.observation.locator()),
                expected: input.initial_access.source_state().clone(),
                selection: visibility_write_selection(problem, input.observation.selection())?,
                model_data: input.write_model_column,
                corrected_data: input.write_corrected_data,
            })
        })
        .transpose()?;
    let (access, source) = finalize_source_access(
        problem,
        input.initial_access,
        &runtime.host,
        &runtime.resource_policy,
    )
    .map_err(|error| match error {
        SourceAccessError::Admission(admission) => Box::new(admission) as ApplicationError,
        error => Box::new(error),
    })?;
    let mut summary = RunSummary {
        backend: runtime.backend,
        ..RunSummary::default()
    };
    let outcome = imaging::run(
        imaging::ImagingInputs {
            problem,
            access,
            masks: input.masks,
            image_response: input.minor_cycle_image_response,
            visibility_write,
            host: runtime.host,
            policy: runtime.resource_policy,
            cancel: runtime.cancel.clone(),
            spill_directory: &runtime.spill_directory,
            aw_catalog,
            backend: runtime.backend,
        },
        &mut summary,
    )
    .map_err(imaging::ImagingError::into_application)?;
    drop(source);
    summary.workers = outcome.workers;
    summary.minor_cycles = outcome.minor_cycles.len();
    summary.minor_iterations = outcome.total_minor_iterations;
    let (planned_products, scientific, products) =
        run_phase("products", &runtime.cancel, &mut summary, || {
            publish_products(
                problem,
                outcome.scientific,
                outcome.masks,
                &runtime,
                publication,
                outcome.workers,
            )
        })?;
    summary.products = planned_products
        .members()
        .iter()
        .map(|member| member.name().to_string())
        .collect();
    Ok(NativeApplicationOutcome {
        problem: problem.clone(),
        minor_cycles: outcome.minor_cycles,
        stop: outcome.stop,
        major_cycle_count: outcome.major_cycle_count,
        total_minor_iterations: outcome.total_minor_iterations,
        total_actual_minor_iterations: outcome.total_actual_minor_iterations,
        workers: outcome.workers,
        planes_per_wave: outcome.planes_per_wave,
        visibility_products: outcome.visibility_products,
        summary,
        scientific,
        planned_products,
        products,
    })
}

fn visibility_write_selection(
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

/// Generate every planned product member into the sink's private staging,
/// one output channel per window on `workers` workers, and publish them;
/// a cancelled run publishes nothing.
fn publish_products(
    problem: &CompiledProblem,
    scientific: MajorCycleCompletion,
    reconstruction_masks: Option<ReconstructionMaskSet>,
    runtime: &ApplicationRuntime,
    publication: ApplicationPublication,
    workers: usize,
) -> Result<
    (
        PlannedContinuumGeneration,
        MajorCycleCompletion,
        PublishedContinuumGeneration,
    ),
    ApplicationError,
> {
    let mut inputs = ContinuumProductInputs::from_major_cycle(problem, &scientific)?;
    if let Some(masks) = reconstruction_masks.as_ref() {
        inputs = match masks {
            ReconstructionMaskSet::Shared(mask) => inputs.with_reconstruction_mask(mask)?,
            ReconstructionMaskSet::Domains(masks) => {
                inputs.with_domain_reconstruction_masks(masks)?
            }
        };
    }
    let planned = PlannedContinuumGeneration::new(&inputs, &publication.controls)?;
    let window = ProductStoragePlan::new(1, workers)?;
    let demand = planned.demand(&inputs, window)?;
    let held = admit(
        &runtime.host,
        &runtime.resource_policy,
        &Demand {
            phase: "products",
            memory: demand.peak_residency_bytes()
                + publication.sink.residency(&planned, &demand)?,
        },
    )?;
    let team = WorkerTeam::new(workers)?;
    let products = produce_continuum_members(&planned, &inputs, window, &team, &publication.sink)?;
    drop(inputs);
    drop(held);
    if runtime.cancel.is_cancelled() {
        // The staged members are removed when the sink drops.
        return Err(Cancelled.into());
    }
    publication.sink.publish()?;
    Ok((planned, scientific, products))
}

/// Failure before or within the installed whole-run implementation.
#[derive(Debug)]
pub enum ApplicationDispatchError {
    /// MeasurementSet resolution or request preparation failed before availability checking.
    Preparation(ApplicationError),
    /// Backend-independent request compilation failed.
    Compile(CompileProblemError),
    /// No installed implementation satisfies the compiled and task contract.
    Unavailable(ImplementationUnavailable),
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
