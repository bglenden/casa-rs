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
mod continuum_request;

pub use availability::{
    ImplementationUnavailable, TaskRequirement, UnsupportedRequirement,
    validate_installed_implementation,
};
pub use casa_imaging_model::HogbomIterationAccounting;
pub use casa_product_sink::CasaImageProductSink;
pub use continuum_request::{
    ContinuumAlgorithm, ContinuumAutoMaskControls, ContinuumBeamPolicy, ContinuumImagingRequest,
    ContinuumImagingResult, ContinuumMask, ContinuumMaskBox, ContinuumStopReason,
    ContinuumWeighting, SpectralImagingMode, VisibilityContinuumSubtraction, execute_continuum,
};

use std::{error::Error, fmt, io, sync::Arc};

use casa_imaging_model::{
    CompileProblemError, CompiledProblem, GeometryInput, ImagingRequest,
    ModelLifecycleRequirements, ObservationSelection, ProblemInputIdentities, ProblemSpecification,
    ReconstructionAlgorithm, SpectralWindowSelection, compile, compile_observation,
};
use casa_imaging_products::{
    ContinuumProductControls, ContinuumProductInputs, ContinuumSourceCatalog,
    PlannedContinuumGeneration, ProductGenerationAuthority, SealedContinuumGeneration,
    VisibilityProductCompletion, produce_continuum_members,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, MajorCycleCompletion, MinorCycleProgram, MinorCycleStopReason,
    ReconstructionMaskPlan, WeightingExecutionLimits,
};
use casa_imaging_runtime::{
    AttemptBoundObservationCompletion, BuildIdentity, ExecutionAttemptId, ExecutionProvenance,
    ExecutionReceipt, ExecutionReceiptStore, FenceKind, FinalVisibilityReplay,
    FrozenWeightingReservation, GriddedNormalReplayStorage, ImplementationContractMetadata,
    ImplementationRegistry, ImplementationRegistryId, ObservationReadCompletionContext,
    PlannerCostModelProfileBootstrap, PlanningBindings, ResourceAuthority, ResourcePolicy,
    RunBindings, RunToCompletion, SerialProductPublicationExecutor, SerialProductPublicationPlan,
    SerialProductPublicationPolicy, SerialProductPublicationRegistry, SerialProductPublicationSink,
    SpectralCycleExecutionPolicy, SpectralCycleExecutor, SpectralCyclePassInput, SpectralCyclePlan,
    SpectralCycleRegistry, StorageIoResourceBinding, WorkExecutionContext, WorkImplementation,
    WorkImplementationId, WorkMeasurements, plan, run,
};
use casa_ms::{
    ResolvedSelectedObservationAccess, SelectedObservationResolutionRequest,
    SelectedVisibilityWriteTargets, resolve_selected_observation,
};
use sha2::{Digest, Sha256};

/// Boxed application failure accepted by the native application composition.
pub type ApplicationError = Box<dyn Error + Send + Sync>;

/// Exact runtime identities and non-scientific limits for one native whole run.
#[derive(Clone)]
pub struct ApplicationRuntime {
    /// Immutable registry identity used by all phases.
    pub registry: ImplementationRegistryId,
    /// Sole serial CPU implementation identity.
    pub implementation: WorkImplementationId,
    /// Frozen weighting execution limits.
    pub weighting_limits: WeightingExecutionLimits,
    /// Conservative elapsed estimate for each physical stage.
    pub stage_nanos: u64,
    /// Hard memory bound for the scheduler-owned minor-cycle view.
    pub minor_cycle_bytes: u64,
    /// Exact profiled storage resources shared by selected-observation reads,
    /// receipt commits, and product publication.
    pub storage_io: StorageIoResourceBinding,
    /// Writable run-local directory capability for the private normal-operator spill.
    pub gridded_normal_storage: GriddedNormalReplayStorage,
    /// Fixed-point confidence in parts per million.
    pub confidence_parts_per_million: u32,
    /// Host-use policy bound at planning and execution.
    pub resource_policy: ResourcePolicy,
    /// Deployment-selected cost-model profile.
    pub cost_model: PlannerCostModelProfileBootstrap,
    /// Process resource authority used for admission and execution.
    pub authority: ResourceAuthority,
    /// Durable bounded receipt store shared by all phases.
    pub receipts: ExecutionReceiptStore,
    /// Executable build identity recorded in every receipt.
    pub build: BuildIdentity,
    /// Distinct attempt identities for initial, final-major, and publication phases.
    pub attempts: [ExecutionAttemptId; 3],
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
    pub mask: ReconstructionMaskPlan,
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
}

/// Product-generation controls, deployment resources, and sole storage sink.
pub struct ApplicationPublication<S> {
    /// Scientific continuum-product controls.
    pub controls: ContinuumProductControls,
    /// Storage adapter that privately stages and atomically publishes members.
    pub sink: S,
}

/// Typed native result retained with every ordinary execution receipt.
pub struct NativeApplicationOutcome {
    /// Initial-major receipt (also the final scientific pass for dirty imaging).
    pub initial_receipt: ExecutionReceipt,
    /// Mandatory post-minor final-major receipt for Högbom imaging.
    pub final_major_receipt: Option<ExecutionReceipt>,
    /// Ordered solve evidence captured before each affine major-cycle handoff.
    pub minor_cycles: Vec<NativeMinorCycleOutcome>,
    /// Number of executed major passes, including the initial pass.
    pub major_cycle_count: usize,
    /// Total component count charged to the reported task/controller budget.
    pub total_minor_iterations: usize,
    /// Total number of components actually applied across all minor cycles.
    pub total_actual_minor_iterations: usize,
    /// Final per-visibility product identities and provenance, when requested.
    pub visibility_products: Option<VisibilityProductCompletion>,
    /// Atomic product-publication receipt.
    pub publication_receipt: ExecutionReceipt,
    /// Terminal-pass receipt containing the bounded selected-visibility write, when requested.
    pub visibility_write_receipt: Option<ExecutionReceipt>,
    /// Final authoritative complete-data and model state.
    pub scientific: MajorCycleCompletion,
    /// Planned product generation used before member production.
    pub planned_products: PlannedContinuumGeneration,
    /// Authorized product generation retained after publication.
    pub products: SealedContinuumGeneration,
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
        mask: request.mask,
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
    mask: ReconstructionMaskPlan,
    native: Result<ApplicationNative<S>, ApplicationError>,
}

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
    } = input.native?;
    let algorithm = problem.reconstruction().algorithm().clone();
    let initial_access = input.initial_access;
    let residency = initial_access.certify_residency(problem)?;
    let write_targets =
        SelectedVisibilityWriteTargets::new(input.write_model_column, input.write_corrected_data);
    let visibility_write_requested = write_targets.model_data() || write_targets.corrected_data();
    let initial_write =
        matches!(algorithm, ReconstructionAlgorithm::Dirty) && visibility_write_requested;
    let planning_registry =
        PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), problem);
    let mut policy = execution_policy(&runtime, residency.clone());
    if initial_write {
        policy = policy
            .with_visibility_write(initial_access.selected_visibility_storage_plan(write_targets)?);
    }
    let planned = match algorithm {
        ReconstructionAlgorithm::Dirty => {
            SpectralCyclePlan::dirty(problem, &planning_registry, policy)?
        }
        ReconstructionAlgorithm::Hogbom
        | ReconstructionAlgorithm::Clark
        | ReconstructionAlgorithm::Multiscale { .. } => {
            SpectralCyclePlan::initial(problem, &planning_registry, policy)?
        }
        _ => unreachable!("native validation admits only continuum minor-cycle solvers"),
    };
    let minor_node = planned.minor_cycle_node().cloned();
    let (physical, weighting, complete, resources, pass, _) = planned.into_parts();
    let replay_proof_bytes = initial_access.replay_proof_retained_heap_bytes(problem)?;
    let frozen_reservation = (!matches!(algorithm, ReconstructionAlgorithm::Dirty))
        .then(|| {
            FrozenWeightingReservation::acquire(
                &runtime.authority,
                runtime.resource_policy.clone(),
                weighting.planned_residency(),
                replay_proof_bytes,
            )
        })
        .transpose()?;
    let initial_source_state = initial_access.source_state().clone();
    let selected = initial_access.open(problem)?;
    let mut executor = SpectralCycleExecutor::new(
        runtime.implementation.clone(),
        problem.clone(),
        weighting,
        resources,
        pass,
        complete,
        selected,
        ExecutableModelProblem::from_compiled(problem.clone())?,
        SpectralCyclePassInput::Initial,
    );
    if !matches!(algorithm, ReconstructionAlgorithm::Dirty) {
        executor = executor.with_frozen_weighting_reservation(
            frozen_reservation.expect("non-dirty execution reserves frozen weighting"),
        );
        executor = executor.with_gridded_normal_compilation(
            &runtime.authority,
            runtime.resource_policy.clone(),
            &runtime.gridded_normal_storage,
        )?;
        let program = MinorCycleProgram::for_algorithm(
            algorithm.clone(),
            problem.reconstruction().controls(),
        )?
        .record_component_sequence(64)?;
        executor = executor.with_reconstruction_cycle(
            minor_node.ok_or_else(|| boxed("initial plan omitted its minor-cycle node"))?,
            input.mask.clone(),
            program,
        );
    }
    let mut initial_terminal_replay = None;
    if initial_write {
        let (replay, sink) = FinalVisibilityReplay::with_visibility_write(
            std::path::PathBuf::from(input.observation.locator()),
            initial_source_state,
            visibility_write_selection(problem, input.observation.selection())?,
            write_targets,
        )?;
        executor = executor.with_final_visibility_sink(sink);
        initial_terminal_replay = Some(replay);
    }
    let registry = SpectralCycleRegistry::new(
        runtime.registry,
        runtime.implementation.clone(),
        problem,
        executor,
    );
    let initial_plan = plan(
        problem,
        PlanningBindings::new(
            runtime.registry,
            runtime.resource_policy.clone(),
            runtime.cost_model,
        ),
        &runtime.authority,
        &registry,
        &runtime.receipts,
        move |_, _| Ok::<_, std::convert::Infallible>(vec![physical]),
    )?;
    run_phase(
        problem,
        &initial_plan,
        &registry,
        &runtime,
        runtime.attempts[0],
    )?;
    let initial_receipt = runtime.receipts.open(runtime.attempts[0])?;

    let (
        scientific,
        final_reconstruction_mask,
        final_major_receipt,
        minor_cycles,
        major_cycle_count,
        total_minor_iterations,
        total_actual_minor_iterations,
        visibility_products,
        visibility_replay,
        visibility_output_receipt,
    ) = match algorithm {
        ReconstructionAlgorithm::Dirty => {
            let result = registry
                .implementation()
                .take_completion()
                .ok_or_else(|| boxed("dirty execution omitted final major-cycle evidence"))?;
            let visibility_products = initial_terminal_replay
                .as_ref()
                .map(FinalVisibilityReplay::completion)
                .transpose()?;
            (
                result.into_completion(),
                None,
                None,
                Vec::new(),
                1,
                0,
                0,
                visibility_products,
                initial_terminal_replay,
                None,
            )
        }
        ReconstructionAlgorithm::Hogbom
        | ReconstructionAlgorithm::Clark
        | ReconstructionAlgorithm::Multiscale { .. } => {
            let mut frozen_weighting = registry
                .implementation()
                .take_frozen_weighting()
                .ok_or_else(|| boxed("initial major omitted frozen weighting"))?;
            let mut gridded_replay = registry
                .implementation()
                .take_gridded_normal_replay()
                .ok_or_else(|| boxed("initial major omitted sealed gridded-normal replay"))?;
            let mut minor = registry
                .implementation()
                .take_reconstruction_cycle_completion()
                .ok_or_else(|| boxed("reconstruction cycle omitted scientific evidence"))?;
            let controls = problem.reconstruction().controls();
            // CASA's `nmajor=-1` leaves the major-cycle count unbounded, but
            // every productive cycle consumes at least one of the total
            // minor-iteration budget. That budget is therefore the finite
            // execution ceiling when no explicit major-cycle limit exists.
            let maximum_cycles = controls
                .maximum_major_cycles()
                .unwrap_or(controls.max_minor_iterations());
            let mut cycle = 1_usize;
            let mut total_iterations = 0_usize;
            let mut total_actual_iterations = 0_usize;
            let mut mask_plan = input.mask.clone();
            let mut minor_outcomes = Vec::new();
            loop {
                let iterations_entering = total_iterations;
                let actual_iterations_entering = total_actual_iterations;
                total_iterations = total_iterations
                    .checked_add(minor.evidence().controller_iterations())
                    .ok_or_else(|| boxed("minor-cycle iteration count overflowed"))?;
                total_actual_iterations = total_actual_iterations
                    .checked_add(minor.evidence().iterations())
                    .ok_or_else(|| boxed("actual minor-cycle iteration count overflowed"))?;
                let minor_outcome = NativeMinorCycleOutcome {
                    cycle,
                    iterations_entering,
                    iterations: minor.evidence().controller_iterations(),
                    total_iterations,
                    actual_iterations_entering,
                    actual_iterations: minor.evidence().iterations(),
                    total_actual_iterations,
                    total_flux: minor.evidence().total_flux(),
                    initial_peak_flux: minor.evidence().initial_peak_flux(),
                    final_peak_flux: minor.evidence().final_peak_flux(),
                    noise_rms: minor.evidence().noise_rms(),
                    effective_threshold: minor.evidence().effective_threshold(),
                    global_threshold: minor.evidence().global_threshold(),
                    cycle_threshold: minor.evidence().cycle_threshold(),
                    stop_reason: minor.evidence().stop_reason().into(),
                    clark_refreshes: minor.evidence().clark_refreshes(),
                    associated_replay_ordinal: cycle,
                    recorded_components: minor.evidence().recorded_components().copied().collect(),
                    mask_support: minor.mask().support().to_vec(),
                    mask_generation: minor.mask().generation_id(),
                    mask_model_generation: minor.mask().model_generation(),
                    mask_normal_state: minor.mask().normal_state_completion(),
                    auto_mask: minor.auto_mask_evidence(),
                };
                eprintln!(
                    "imaging_minor_cycle_summary cycle={} associated_replay_ordinal={} controller_iterations_entering={} controller_iterations={} controller_iterations_total={} actual_iterations_entering={} actual_iterations={} actual_iterations_total={} initial_peak_flux={} final_peak_flux={} model_update_abs_flux={} global_threshold={} effective_threshold={} cycle_threshold={} stop_reason={:?} clark_refreshes={}",
                    minor_outcome.cycle,
                    minor_outcome.associated_replay_ordinal,
                    minor_outcome.iterations_entering,
                    minor_outcome.iterations,
                    minor_outcome.total_iterations,
                    minor_outcome.actual_iterations_entering,
                    minor_outcome.actual_iterations,
                    minor_outcome.total_actual_iterations,
                    minor_outcome.initial_peak_flux,
                    minor_outcome.final_peak_flux,
                    minor_outcome.total_flux,
                    minor_outcome.global_threshold,
                    minor_outcome.effective_threshold,
                    minor_outcome
                        .cycle_threshold
                        .map_or_else(|| "none".to_owned(), |value| value.to_string()),
                    minor_outcome.stop_reason,
                    minor_outcome.clark_refreshes,
                );
                let continue_cleaning = cycle < maximum_cycles
                    && total_iterations < controls.max_minor_iterations()
                    && minor.evidence().requests_reconciliation();
                let next_mask = mask_plan.next_cycle(
                    minor.mask(),
                    cycle,
                    minor.evidence().cycle_threshold_is_global(),
                    minor_outcome
                        .auto_mask
                        .is_some_and(|evidence| evidence.channel_stopped),
                );
                let applied_mask = minor.mask().clone();
                minor_outcomes.push(minor_outcome);
                let final_input = minor.into_final_major_input();
                let final_policy = execution_policy(&runtime, residency.clone());
                let ordinal =
                    u32::try_from(cycle).map_err(|_| boxed("major-cycle ordinal exceeds u32"))?;
                let final_planned = if continue_cleaning {
                    SpectralCyclePlan::continuing_major(
                        problem,
                        &planning_registry,
                        final_policy,
                        &final_input,
                        ordinal,
                    )?
                } else {
                    SpectralCyclePlan::final_major_at(
                        problem,
                        &planning_registry,
                        final_policy,
                        &final_input,
                        ordinal,
                    )?
                };
                let (physical, weighting, complete, _resources, pass, minor_node) =
                    final_planned.into_parts();
                let mut executor = SpectralCycleExecutor::new_gridded(
                    runtime.implementation.clone(),
                    problem.clone(),
                    weighting,
                    pass,
                    complete,
                    ExecutableModelProblem::from_compiled(problem.clone())?,
                    SpectralCyclePassInput::FinalMajor(final_input),
                    gridded_replay,
                )
                .with_frozen_weighting(frozen_weighting);
                if continue_cleaning {
                    let remaining = controls
                        .max_minor_iterations()
                        .saturating_sub(total_iterations);
                    let program = MinorCycleProgram::for_algorithm(algorithm.clone(), controls)?
                        .record_component_sequence(64)?
                        .limit_iterations(remaining)?;
                    executor = executor.with_reconstruction_cycle(
                        minor_node.ok_or_else(|| boxed("continuing plan omitted minor node"))?,
                        next_mask.clone(),
                        program,
                    );
                }
                let registry = SpectralCycleRegistry::new(
                    runtime.registry,
                    runtime.implementation.clone(),
                    problem,
                    executor,
                );
                let final_plan = plan(
                    problem,
                    PlanningBindings::new(
                        runtime.registry,
                        runtime.resource_policy.clone(),
                        runtime.cost_model,
                    ),
                    &runtime.authority,
                    &registry,
                    &runtime.receipts,
                    move |_, _| Ok::<_, std::convert::Infallible>(vec![physical]),
                )?;
                let attempt = major_cycle_attempt(runtime.attempts[1], ordinal);
                run_phase(problem, &final_plan, &registry, &runtime, attempt)?;
                let receipt = runtime.receipts.open(attempt)?;
                frozen_weighting = registry
                    .implementation()
                    .take_frozen_weighting()
                    .ok_or_else(|| boxed("later major omitted reusable frozen weighting"))?;
                gridded_replay = registry
                    .implementation()
                    .take_gridded_normal_replay()
                    .ok_or_else(|| boxed("later major omitted reusable gridded-normal replay"))?;
                if continue_cleaning {
                    minor = registry
                        .implementation()
                        .take_reconstruction_cycle_completion()
                        .ok_or_else(|| boxed("continuing major omitted cycle evidence"))?;
                    mask_plan = next_mask;
                    cycle += 1;
                    continue;
                }
                let completion = registry
                    .implementation()
                    .take_completion()
                    .ok_or_else(|| boxed("final-major execution omitted scientific evidence"))?
                    .into_completion();
                if !visibility_write_requested {
                    break (
                        completion,
                        Some(applied_mask),
                        Some(receipt),
                        minor_outcomes,
                        cycle + 1,
                        total_iterations,
                        total_actual_iterations,
                        None,
                        None,
                        None,
                    );
                }
                let resolved = resolve_selected_observation(input.observation.clone())?;
                let (_, access) = resolved.into_parts();
                let output_residency = access.certify_residency(problem)?;
                let source_state = access.source_state().clone();
                let output_policy = execution_policy(&runtime, output_residency)
                    .with_visibility_write(access.selected_visibility_storage_plan(write_targets)?);
                let output_planned = SpectralCyclePlan::selected_output(
                    problem,
                    &planning_registry,
                    output_policy,
                    ordinal,
                )?;
                let (
                    output_physical,
                    output_weighting,
                    output_complete,
                    output_resources,
                    output_pass,
                    _,
                ) = output_planned.into_parts();
                let selected = frozen_weighting.rebind_selected(access, problem)?;
                let (visibility_replay, sink) = FinalVisibilityReplay::with_visibility_write(
                    std::path::PathBuf::from(input.observation.locator()),
                    source_state,
                    visibility_write_selection(problem, input.observation.selection())?,
                    write_targets,
                )?;
                let output_executor = SpectralCycleExecutor::new_selected_output(
                    runtime.implementation.clone(),
                    problem.clone(),
                    output_weighting,
                    output_resources,
                    output_pass,
                    output_complete,
                    selected,
                    completion,
                    frozen_weighting,
                )
                .with_final_visibility_sink(sink);
                let output_registry = SpectralCycleRegistry::new(
                    runtime.registry,
                    runtime.implementation.clone(),
                    problem,
                    output_executor,
                );
                let output_plan = plan(
                    problem,
                    PlanningBindings::new(
                        runtime.registry,
                        runtime.resource_policy.clone(),
                        runtime.cost_model,
                    ),
                    &runtime.authority,
                    &output_registry,
                    &runtime.receipts,
                    move |_, _| Ok::<_, std::convert::Infallible>(vec![output_physical]),
                )?;
                let output_attempt = selected_output_attempt(attempt);
                run_phase(
                    problem,
                    &output_plan,
                    &output_registry,
                    &runtime,
                    output_attempt,
                )?;
                let output_receipt = runtime.receipts.open(output_attempt)?;
                let completion = output_registry
                    .implementation()
                    .take_selected_output_completion()
                    .ok_or_else(|| boxed("selected-output traversal omitted scientific state"))?;
                break (
                    completion,
                    Some(applied_mask),
                    Some(receipt),
                    minor_outcomes,
                    cycle + 1,
                    total_iterations,
                    total_actual_iterations,
                    Some(visibility_replay.completion()?),
                    Some(visibility_replay),
                    Some(output_receipt),
                );
            }
        }
        _ => unreachable!("native validation admits only dirty or Högbom"),
    };

    publish_products(
        problem,
        scientific,
        final_reconstruction_mask,
        runtime,
        publication,
        PriorPhaseOutcome {
            initial_receipt,
            final_major_receipt,
            minor_cycles,
            major_cycle_count,
            total_minor_iterations,
            total_actual_minor_iterations,
            visibility_products,
            visibility_replay,
            visibility_output_receipt,
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
    initial_receipt: ExecutionReceipt,
    final_major_receipt: Option<ExecutionReceipt>,
    minor_cycles: Vec<NativeMinorCycleOutcome>,
    major_cycle_count: usize,
    total_minor_iterations: usize,
    total_actual_minor_iterations: usize,
    visibility_products: Option<VisibilityProductCompletion>,
    visibility_replay: Option<FinalVisibilityReplay>,
    visibility_output_receipt: Option<ExecutionReceipt>,
}

fn execution_policy(
    runtime: &ApplicationRuntime,
    residency: casa_ms::SelectedObservationResidencyCertificate,
) -> SpectralCycleExecutionPolicy {
    SpectralCycleExecutionPolicy::new(
        runtime.implementation.clone(),
        runtime.weighting_limits,
        residency,
        runtime.storage_io.clone(),
        runtime.stage_nanos,
        runtime.minor_cycle_bytes,
        runtime.confidence_parts_per_million,
    )
}

fn run_phase(
    problem: &CompiledProblem,
    execution_plan: &casa_imaging_runtime::ExecutionPlan,
    registry: &SpectralCycleRegistry<SpectralCycleExecutor>,
    runtime: &ApplicationRuntime,
    attempt: ExecutionAttemptId,
) -> Result<(), ApplicationError> {
    let executable = ExecutableModelProblem::from_compiled(problem.clone())?;
    let current = RunBindings::new(
        problem.inputs().clone(),
        &runtime.resource_policy,
        runtime.cost_model.profile_id(),
    );
    let mut controller = RunToCompletion;
    run(
        &executable,
        execution_plan,
        &current,
        registry,
        &runtime.authority,
        &mut controller,
        runtime
            .receipts
            .bind(ExecutionProvenance::new(attempt, runtime.build)),
    )?;
    Ok(())
}

fn major_cycle_attempt(base: ExecutionAttemptId, ordinal: u32) -> ExecutionAttemptId {
    if ordinal == 1 {
        return base;
    }
    let mut hash = Sha256::new();
    hash.update(b"casa-rs:imaging:major-cycle-attempt:v1");
    hash.update(base.as_bytes());
    hash.update(ordinal.to_le_bytes());
    ExecutionAttemptId::from_sha256(hash.finalize().into())
}

fn selected_output_attempt(science: ExecutionAttemptId) -> ExecutionAttemptId {
    let mut hash = Sha256::new();
    hash.update(b"casa-rs:imaging:selected-output-attempt:v1");
    hash.update(science.as_bytes());
    ExecutionAttemptId::from_sha256(hash.finalize().into())
}

fn publish_products<S>(
    problem: &CompiledProblem,
    scientific: MajorCycleCompletion,
    reconstruction_mask: Option<casa_imaging_reconstruction::ReconstructionMask>,
    runtime: ApplicationRuntime,
    publication_config: ApplicationPublication<S>,
    prior: PriorPhaseOutcome,
) -> Result<NativeApplicationOutcome, ApplicationError>
where
    S: SerialProductPublicationSink + Send + 'static,
    S::Error: Send + Sync,
{
    let sources = ContinuumSourceCatalog::from_major_cycle_with_mask(
        problem,
        &scientific,
        reconstruction_mask.as_ref(),
    )?;
    let authority = ProductGenerationAuthority::bind(problem);
    let planned_products = authority.plan(&sources, &publication_config.controls)?;

    let visibility_write_receipt = prior
        .visibility_replay
        .as_ref()
        .is_some_and(FinalVisibilityReplay::has_visibility_write)
        .then(|| {
            prior
                .visibility_output_receipt
                .clone()
                .or_else(|| prior.final_major_receipt.clone())
                .unwrap_or_else(|| prior.initial_receipt.clone())
        });
    let planning_registry =
        PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), problem);
    // The ordinary publication plan is deliberately constructed before member
    // production and sealing; the executor later presents the completed
    // projection to the runtime for authorization at the commit gate.
    let publication_plan = SerialProductPublicationPlan::new(
        problem,
        &planned_products,
        &planning_registry,
        SerialProductPublicationPolicy::new(
            runtime.implementation.clone(),
            runtime.storage_io.clone(),
            runtime.stage_nanos,
            runtime.confidence_parts_per_million,
        ),
    )?;
    let mut inputs = ContinuumProductInputs::from_major_cycle(problem, &scientific)?;
    if let Some(mask) = reconstruction_mask.as_ref() {
        inputs = inputs.with_reconstruction_mask(mask)?;
    }
    let produced = produce_continuum_members(&planned_products, &inputs)?;
    let sealed = authority.authorize(&planned_products, &produced)?;
    let (physical, publication) = publication_plan.into_parts();
    let executor = SerialProductPublicationExecutor::new(
        runtime.implementation.clone(),
        publication,
        sealed,
        publication_config.sink,
    )?;
    let registry = SerialProductPublicationRegistry::new(
        runtime.registry,
        runtime.implementation.clone(),
        problem,
        executor,
    );
    let execution_plan = plan(
        problem,
        PlanningBindings::new(
            runtime.registry,
            runtime.resource_policy.clone(),
            runtime.cost_model,
        ),
        &runtime.authority,
        &registry,
        &runtime.receipts,
        move |_, _| Ok::<_, std::convert::Infallible>(vec![physical]),
    )?;
    let executable = ExecutableModelProblem::from_compiled(problem.clone())?;
    let current = RunBindings::new(
        problem.inputs().clone(),
        &runtime.resource_policy,
        runtime.cost_model.profile_id(),
    );
    let mut controller = RunToCompletion;
    run(
        &executable,
        &execution_plan,
        &current,
        &registry,
        &runtime.authority,
        &mut controller,
        runtime
            .receipts
            .bind(ExecutionProvenance::new(runtime.attempts[2], runtime.build)),
    )?;
    let publication_receipt = runtime.receipts.open(runtime.attempts[2])?;
    let products = registry
        .implementation()
        .take_sealed_generation()
        .ok_or_else(|| boxed("publication execution omitted its sealed product generation"))?;
    Ok(NativeApplicationOutcome {
        initial_receipt: prior.initial_receipt,
        final_major_receipt: prior.final_major_receipt,
        minor_cycles: prior.minor_cycles,
        major_cycle_count: prior.major_cycle_count,
        total_minor_iterations: prior.total_minor_iterations,
        total_actual_minor_iterations: prior.total_actual_minor_iterations,
        visibility_products: prior.visibility_products,
        publication_receipt,
        visibility_write_receipt,
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

    fn wait_for_fence(&self, _: WorkExecutionContext<'_>, _: FenceKind) -> Result<(), Self::Error> {
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
