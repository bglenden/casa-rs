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
pub use casa_imaging_model::{
    HogbomIterationAccounting, ImagingRequestVersion, PolarizationCoordinate, ProductNormalization,
};
pub use casa_imaging_runtime::{ResourceOverride, ResourcePolicy};
pub use casa_product_sink::{CasaImageDomainOutput, CasaImageProductSink};
pub use continuum_request::{
    ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumAwCfSource, ContinuumAwProjection,
    ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumImagingResult, ContinuumMask,
    ContinuumMaskBox, ContinuumStopReason, ContinuumWeighting, NativeAwCachePolicy,
    NativeEvlaAwCache, SpectralImagingMode, VisibilityContinuumSubtraction, execute_continuum,
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
    PublishedContinuumGeneration, VisibilityProductCompletion,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, ImageDomainReconstructionMaskPlans, MajorCycleCompletion,
    MinorCycleImageResponse, MinorCycleStopReason, ReconstructionMaskSet,
};
use casa_imaging_runtime::{
    AttemptBoundObservationCompletion, BuildIdentity, ExecutionAttemptId, ExecutionProvenance,
    ExecutionReceipt, ExecutionReceiptStore, ExecutionStatus, FenceKind,
    ImplementationContractMetadata, ImplementationRegistry, ImplementationRegistryId,
    ObservationReadCompletionContext, PagedStateDirectory, PlannerCostModelProfileId,
    PlanningBindings, ResourceAuthority, RunBindings, RunController, RunDirective,
    SerialProductPublicationExecutor, SerialProductPublicationPlan, SerialProductPublicationPolicy,
    SerialProductPublicationRegistry, SerialProductPublicationSink, StorageIoResourceBinding,
    WorkExecutionContext, WorkImplementation, WorkImplementationId, WorkMeasurements,
    finalize_source_access, plan, run,
};
use casa_ms::{
    ResolvedSelectedObservationAccess, SelectedObservationResolutionRequest,
    resolve_selected_observation,
};

/// Boxed application failure accepted by the native application composition.
pub type ApplicationError = Box<dyn Error + Send + Sync>;

/// Exact runtime identities and non-scientific limits for one native whole run.
#[derive(Clone)]
pub struct ApplicationRuntime {
    /// Immutable registry identity used by product publication.
    pub registry: ImplementationRegistryId,
    /// CPU implementation identity.
    pub implementation: WorkImplementationId,
    /// Conservative elapsed estimate for each physical stage.
    pub stage_nanos: u64,
    /// Exact profiled storage resources shared by selected-observation reads,
    /// receipt commits, and product publication.
    pub storage_io: StorageIoResourceBinding,
    /// Writable run-local directory the major-cycle pass pages cube state into
    /// when the state does not fit in memory.
    pub paged_state_storage: PagedStateDirectory,
    /// Fixed-point confidence in parts per million.
    pub confidence_parts_per_million: u32,
    /// Host-use policy bound at planning and execution.
    pub resource_policy: ResourcePolicy,
    /// Deployment-selected cost-model profile.
    pub cost_model: PlannerCostModelProfileId,
    /// Process resource authority used for admission and execution.
    pub authority: ResourceAuthority,
    /// Durable bounded receipt store for product publication.
    pub receipts: ExecutionReceiptStore,
    /// Executable build identity recorded in every receipt.
    pub build: BuildIdentity,
    /// Attempt identity of the product-publication run.
    pub publication_attempt: ExecutionAttemptId,
}

/// Exact native request template resolved at the sole application boundary.
pub struct ApplicationRequest<S> {
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
    pub native: Result<ApplicationNative<S>, ApplicationError>,
}

/// Runtime and publication inputs consumed only by the Native engine port.
pub struct ApplicationNative<S> {
    /// Explicit runtime/resource/receipt inputs.
    pub runtime: ApplicationRuntime,
    /// Product-generation and independently atomic publication configuration.
    pub publication: ApplicationPublication<S>,
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

/// Product-generation controls, deployment resources, and sole storage sink.
pub struct ApplicationPublication<S> {
    /// Scientific continuum-product controls.
    pub controls: ContinuumProductControls,
    /// Storage adapter that privately stages and atomically publishes members.
    pub sink: S,
}

/// Typed native result of one whole run.
pub struct NativeApplicationOutcome {
    /// Ordered solve evidence captured before each major-cycle pass.
    pub minor_cycles: Vec<NativeMinorCycleOutcome>,
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
    /// Atomic product-publication receipt.
    pub publication_receipt: ExecutionReceipt,
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
    /// Number of exact Clark residual refreshes.
    pub clark_refreshes: usize,
    /// One-based major replay ordinal associated with this cycle's accepted update.
    pub associated_replay_ordinal: usize,
    /// Bounded leading component sequence for CASA/Rust first-divergence diagnostics.
    pub recorded_components: Vec<casa_imaging_reconstruction::MinorCycleComponent>,
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
    /// The next update would exceed the frozen-approximation envelope.
    StalenessBound,
    /// The multiscale residual trajectory diverged after accepted progress.
    MultiscaleDivergence,
}

impl From<MinorCycleStopReason> for NativeMinorCycleStopReason {
    fn from(value: MinorCycleStopReason) -> Self {
        match value {
            MinorCycleStopReason::ThresholdReached => Self::ThresholdReached,
            MinorCycleStopReason::IterationBound => Self::IterationBound,
            MinorCycleStopReason::StalenessBound => Self::StalenessBound,
            MinorCycleStopReason::MultiscaleDivergence => Self::MultiscaleDivergence,
        }
    }
}

impl NativeMinorCycleOutcome {
    /// Return the first exact component mismatch against a CASA/parity baseline.
    #[must_use]
    pub fn first_component_divergence(
        &self,
        baseline: &[casa_imaging_reconstruction::MinorCycleComponent],
    ) -> Option<(
        usize,
        Option<casa_imaging_reconstruction::MinorCycleComponent>,
        Option<casa_imaging_reconstruction::MinorCycleComponent>,
    )> {
        let shared = baseline.len().min(self.recorded_components.len());
        for (index, (expected, actual)) in
            baseline.iter().zip(&self.recorded_components).enumerate()
        {
            if expected != actual {
                return Some((index, Some(*expected), Some(*actual)));
            }
        }
        (baseline.len() != self.recorded_components.len()).then(|| {
            (
                shared,
                baseline.get(shared).copied(),
                self.recorded_components.get(shared).copied(),
            )
        })
    }
}

/// Whole-run result from the sole installed implementation.
pub struct ApplicationOutcome {
    /// Output returned by the installed implementation.
    pub output: Box<NativeApplicationOutcome>,
}

/// Execute one imaging request through the sole installed implementation.
/// Unsupported requirements fail typed before physical planning or execution.
pub fn execute<S>(
    request: ApplicationRequest<S>,
) -> Result<ApplicationOutcome, ApplicationDispatchError>
where
    S: SerialProductPublicationSink + Send + 'static,
    S::Error: Send + Sync,
{
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
    let output = run_native(&problem, input).map_err(ApplicationDispatchError::Native)?;
    Ok(ApplicationOutcome {
        output: Box::new(output),
    })
}

struct NativeInput<S> {
    observation: SelectedObservationResolutionRequest,
    initial_access: ResolvedSelectedObservationAccess,
    write_model_column: bool,
    write_corrected_data: bool,
    masks: ImageDomainReconstructionMaskPlans,
    minor_cycle_image_response: Option<MinorCycleImageResponse>,
    native: Result<ApplicationNative<S>, ApplicationError>,
}

/// Run the major-cycle passes, the minor cycles between them and any
/// visibility write, then publish the products.
fn run_native<S>(
    problem: &CompiledProblem,
    input: NativeInput<S>,
) -> Result<NativeApplicationOutcome, ApplicationError>
where
    S: SerialProductPublicationSink + Send + 'static,
    S::Error: Send + Sync,
{
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
    let access = finalize_source_access(
        problem,
        input.initial_access,
        &runtime.authority,
        &runtime.resource_policy,
    )?;
    let paged_state = runtime.paged_state_storage.clone();
    let outcome = imaging::run(imaging::ImagingInputs {
        problem,
        access,
        masks: input.masks,
        image_response: input.minor_cycle_image_response,
        visibility_write,
        authority: &runtime.authority,
        policy: &runtime.resource_policy,
        spill_directory: paged_state.directory(),
        aw_catalog,
    })?;
    publish_products(
        problem,
        outcome.scientific,
        outcome.masks,
        runtime,
        publication,
        PriorPhaseOutcome {
            minor_cycles: outcome.minor_cycles,
            major_cycle_count: outcome.major_cycle_count,
            total_minor_iterations: outcome.total_minor_iterations,
            total_actual_minor_iterations: outcome.total_actual_minor_iterations,
            visibility_products: outcome.visibility_products,
            workers: outcome.workers,
            planes_per_wave: outcome.planes_per_wave,
        },
    )
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

struct PriorPhaseOutcome {
    workers: usize,
    planes_per_wave: Option<u32>,
    minor_cycles: Vec<NativeMinorCycleOutcome>,
    major_cycle_count: usize,
    total_minor_iterations: usize,
    total_actual_minor_iterations: usize,
    visibility_products: Option<VisibilityProductCompletion>,
}

enum ApplicationRunController {
    Continue,
    ApplyEligible,
}

impl RunController for ApplicationRunController {
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective {
        match self {
            Self::ApplyEligible => status
                .eligible_adaptations()
                .first()
                .map_or(RunDirective::Continue, |transition| {
                    RunDirective::Adapt(transition.id.clone())
                }),
            Self::Continue => RunDirective::Continue,
        }
    }
}

fn application_controller(runtime: &ApplicationRuntime) -> ApplicationRunController {
    if runtime.resource_policy.has_explicit_memory_ceiling() {
        ApplicationRunController::ApplyEligible
    } else {
        ApplicationRunController::Continue
    }
}

fn publish_products<S>(
    problem: &CompiledProblem,
    scientific: MajorCycleCompletion,
    reconstruction_masks: Option<ReconstructionMaskSet>,
    runtime: ApplicationRuntime,
    publication_config: ApplicationPublication<S>,
    prior: PriorPhaseOutcome,
) -> Result<NativeApplicationOutcome, ApplicationError>
where
    S: SerialProductPublicationSink + Send + 'static,
    S::Error: Send + Sync,
{
    let (planned_products, generation_demand) = {
        let requested_workers = match &runtime.resource_policy {
            ResourcePolicy::Explicit(policy) => {
                policy.workers.map_or(Ok(prior.workers), usize::try_from)?
            }
            _ => prior.workers,
        };
        let mut inputs = ContinuumProductInputs::from_major_cycle(problem, &scientific)?;
        if let Some(masks) = reconstruction_masks.as_ref() {
            inputs = match masks {
                ReconstructionMaskSet::Shared(mask) => inputs.with_reconstruction_mask(mask)?,
                ReconstructionMaskSet::Domains(masks) => {
                    inputs.with_domain_reconstruction_masks(masks)?
                }
            };
        }
        let planned = PlannedContinuumGeneration::new(&inputs, &publication_config.controls)?;
        let demand = planned.demand(
            &inputs,
            casa_imaging_products::ProductStoragePlan::new(1, requested_workers)?,
        )?;
        (planned, demand)
    };
    let staging_residency_bytes = publication_config
        .sink
        .residency(&planned_products, &generation_demand)?;

    let planning_registry =
        PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), problem);
    let publication_plan = SerialProductPublicationPlan::new(
        problem,
        &planned_products,
        &generation_demand,
        staging_residency_bytes,
        &planning_registry,
        SerialProductPublicationPolicy::new(
            runtime.implementation.clone(),
            runtime.storage_io.clone(),
            runtime.stage_nanos,
            runtime.confidence_parts_per_million,
            runtime.authority.topology().native_thread_stack_bytes,
        ),
    )?;
    let (physical, publication, window) = publication_plan.into_parts();
    // Admit the generation windows and direct writer before opening output images.
    let execution_plan = plan(
        problem,
        PlanningBindings::new(
            runtime.registry,
            runtime.resource_policy.clone(),
            runtime.cost_model,
        ),
        &runtime.authority,
        &planning_registry,
        &runtime.receipts,
        move |_, _| Ok::<_, std::convert::Infallible>(vec![physical]),
    )?;
    let executor = SerialProductPublicationExecutor::new(
        runtime.implementation.clone(),
        problem.clone(),
        publication,
        planned_products,
        scientific,
        reconstruction_masks,
        publication_config.sink,
        window,
    )?;
    let registry = SerialProductPublicationRegistry::new(
        runtime.registry,
        runtime.implementation.clone(),
        problem,
        executor,
    );
    let executable = ExecutableModelProblem::from_compiled(problem.clone())?;
    let current = RunBindings::new(
        problem.inputs().clone(),
        &runtime.resource_policy,
        runtime.cost_model,
    );
    let mut controller = application_controller(&runtime);
    run(
        &executable,
        &execution_plan,
        &current,
        &registry,
        &runtime.authority,
        &mut controller,
        runtime.receipts.bind(ExecutionProvenance::new(
            runtime.publication_attempt,
            runtime.build,
        )),
    )?;
    let publication_receipt = runtime.receipts.open(runtime.publication_attempt)?;
    let completion = registry
        .implementation()
        .take_completion()
        .ok_or_else(|| boxed("publication execution omitted its product completion"))?;
    let (planned_products, scientific, products) = completion.into_parts();
    Ok(NativeApplicationOutcome {
        minor_cycles: prior.minor_cycles,
        major_cycle_count: prior.major_cycle_count,
        total_minor_iterations: prior.total_minor_iterations,
        total_actual_minor_iterations: prior.total_actual_minor_iterations,
        workers: prior.workers,
        planes_per_wave: prior.planes_per_wave,
        visibility_products: prior.visibility_products,
        publication_receipt,
        scientific,
        planned_products,
        products,
    })
}

struct PlanningRegistry {
    id: ImplementationRegistryId,
    implementation_id: WorkImplementationId,
    metadata: ImplementationContractMetadata,
    implementation: PlanningImplementation,
}

impl PlanningRegistry {
    fn new(
        id: ImplementationRegistryId,
        implementation_id: WorkImplementationId,
        problem: &CompiledProblem,
    ) -> Self {
        Self {
            id,
            implementation: PlanningImplementation(implementation_id.clone()),
            implementation_id,
            metadata: ImplementationContractMetadata::new(
                problem.problem_id(),
                problem.numerics_id(),
                problem.required_capabilities().clone(),
            ),
        }
    }
}

impl ImplementationRegistry for PlanningRegistry {
    type Implementation = PlanningImplementation;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        (id == &self.implementation_id).then_some(&self.implementation)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        (id == &self.implementation_id).then(|| self.metadata.clone())
    }
}

struct PlanningImplementation(WorkImplementationId);

impl WorkImplementation for PlanningImplementation {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.0
    }

    fn execute(&self, _: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        Err(io::Error::other("planning-only registry cannot execute"))
    }

    fn failure_measurements<'a>(&'a self, _: &'a Self::Error) -> Option<&'a WorkMeasurements> {
        None
    }

    fn wait_for_fence(
        &self,
        _: WorkExecutionContext<'_>,
        _: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        Err(io::Error::other("planning-only registry cannot execute"))
    }

    fn complete_observation_read(
        &self,
        _: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        Err(io::Error::other("planning-only registry cannot execute"))
    }

    fn publish(&self, _: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        Err(io::Error::other("planning-only registry cannot execute"))
    }
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
            Self::Native(error) => write!(formatter, "native imaging run failed: {error}"),
        }
    }
}

impl Error for ApplicationDispatchError {}

fn boxed(message: &'static str) -> ApplicationError {
    Box::new(io::Error::other(message))
}
