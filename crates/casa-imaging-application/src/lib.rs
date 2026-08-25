// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Production composition owner for native imaging.
//!
//! This crate is the single application-layer seam that binds the whole-run
//! migration router to MeasurementSet observation authority, scientific
//! reconstruction and products, and the physical execution runtime. Frontends
//! submit requests here; they do not compose native execution stages directly.

mod casa_product_sink;
mod continuum_request;

pub use casa_product_sink::CasaImageProductSink;
pub use continuum_request::{
    ContinuumAlgorithm, ContinuumBeamPolicy, ContinuumImagingRequest, ContinuumImagingResult,
    ContinuumStopReason, ContinuumWeighting, execute_continuum,
};

use std::{error::Error, fmt, io, sync::Mutex};

use casa_imaging_model::{
    CompiledProblem, GeometryInput, ImagingRequest, ModelLifecycleRequirements,
    PolarizationCoordinate, ProblemInputIdentities, ProblemSpecification, ReconstructionAlgorithm,
    ReconstructionBasis, compile_observation,
};
use casa_imaging_products::{
    ContinuumProductControls, ContinuumProductInputs, ContinuumSourceCatalog,
    PlannedContinuumGeneration, ProductGenerationAuthority, SealedContinuumGeneration,
    produce_continuum_members,
};
use casa_imaging_reconstruction::{
    CleanWindow, ExecutableModelProblem, HogbomControls, MajorCycleCompletion,
    MinorCycleStopReason, WeightingExecutionLimits,
};
pub use casa_imaging_router::TaskRouteRequirement;
use casa_imaging_router::{
    DispatchError, ImagingRouter, MigrationRowKind, NativeEnginePort, RequestDisposition,
    RouteRecord,
};
use casa_imaging_runtime::{
    AttemptBoundObservationCompletion, BuildIdentity, ExecutionAttemptId, ExecutionProvenance,
    ExecutionReceipt, ExecutionReceiptStore, ExecutionRouteDisposition, ExecutionRouteEvidence,
    ExecutionRouteRequirement, ExecutionRouteRequirementEvidence, ExecutionRouteRequirementKind,
    FenceKind, ImplementationContractMetadata, ImplementationRegistry, ImplementationRegistryId,
    ObservationReadCompletionContext, PlannerCostModelProfileBootstrap, PlanningBindings,
    ResourceAuthority, ResourcePolicy, RunBindings, RunToCompletion,
    SerialContinuumExecutionPolicy, SerialContinuumExecutor, SerialContinuumPassInput,
    SerialContinuumPlan, SerialContinuumRegistry, SerialProductPublicationExecutor,
    SerialProductPublicationPlan, SerialProductPublicationPolicy, SerialProductPublicationRegistry,
    SerialProductPublicationSink, StorageIoResourceBinding, WorkExecutionContext,
    WorkImplementation, WorkImplementationId, WorkMeasurements, plan, run,
};
use casa_ms::{
    ResolvedSelectedObservationAccess, SelectedObservationResolutionRequest,
    resolve_selected_observation,
};

/// Boxed application failure accepted by both sealed router ports.
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
    /// Storage-owner request for the single selected MeasurementSet.
    pub observation: SelectedObservationResolutionRequest,
    /// Task-surface constraints that cannot be inferred from the compiled
    /// backend-independent problem.
    pub task_route_requirements: Vec<TaskRouteRequirement>,
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
    /// T21 solve evidence captured before its affine final-major handoff.
    pub minor_cycle: Option<NativeMinorCycleOutcome>,
    /// Atomic product-publication receipt.
    pub publication_receipt: ExecutionReceipt,
    /// Final authoritative complete-data and model state.
    pub scientific: MajorCycleCompletion,
    /// Planned product generation used before member production.
    pub planned_products: PlannedContinuumGeneration,
    /// Authorized product generation retained after publication.
    pub products: SealedContinuumGeneration,
}

/// Stable application projection of the T21 owner evidence.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NativeMinorCycleOutcome {
    /// Number of accepted component updates.
    pub iterations: usize,
    /// Final normalized residual peak.
    pub final_peak_flux: f64,
    /// Scientific terminal reason.
    pub stop_reason: MinorCycleStopReason,
}

/// Whole-run result with the authoritative route record.
pub struct ApplicationOutcome {
    /// Pre-plan migration decision.
    pub route: RouteRecord,
    /// Output returned by the one selected engine.
    pub output: Box<NativeApplicationOutcome>,
}

/// Dispatch one imaging request through the sole production native engine.
/// Requests whose required capabilities are not native fail as temporarily
/// unavailable before physical planning or execution begins.
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
    let native_input = Mutex::new(Some(NativeInput {
        observation: request.observation,
        initial_access: access,
        native: request.native,
    }));
    let router = ImagingRouter::new(NativeEnginePort::new(move |problem, route| {
        let input = native_input
            .lock()
            .map_err(|_| boxed("native application input lock poisoned"))?
            .take()
            .ok_or_else(|| boxed("native application request already consumed"))?;
        run_native(problem, route, input).map(Box::new)
    }));
    let dispatched = router
        .dispatch_with_task_requirements(imaging, request.task_route_requirements)
        .map_err(ApplicationDispatchError::Dispatch)?;
    let (route, output) = dispatched.into_parts();
    Ok(ApplicationOutcome { route, output })
}

struct NativeInput<S> {
    observation: SelectedObservationResolutionRequest,
    initial_access: ResolvedSelectedObservationAccess,
    native: Result<ApplicationNative<S>, ApplicationError>,
}

fn run_native<S>(
    problem: &CompiledProblem,
    route: &RouteRecord,
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
    validate_native_problem(problem)?;
    let route_evidence = execution_route(route)?;
    let algorithm = problem.reconstruction().algorithm().clone();
    let initial_access = input.initial_access.with_minimum_content_budget(problem)?;
    let residency = initial_access.certify_residency(problem)?;
    let planning_registry =
        PlanningRegistry::new(runtime.registry, runtime.implementation.clone(), problem);
    let policy = execution_policy(&runtime, residency.clone());
    let planned = match algorithm {
        ReconstructionAlgorithm::Dirty => {
            SerialContinuumPlan::dirty(problem, &planning_registry, policy)?
        }
        ReconstructionAlgorithm::Hogbom => {
            SerialContinuumPlan::initial(problem, &planning_registry, policy)?
        }
        _ => unreachable!("native validation admits only dirty or Högbom"),
    };
    let minor_node = planned.minor_cycle_node().cloned();
    let (physical, weighting, complete, resources, pass, _) = planned.into_parts();
    let selected = initial_access.open(problem)?;
    let mut executor = SerialContinuumExecutor::new(
        runtime.implementation.clone(),
        problem.clone(),
        weighting,
        resources,
        pass,
        complete,
        selected,
        ExecutableModelProblem::from_compiled(problem.clone())?,
        SerialContinuumPassInput::Initial,
    );
    if matches!(algorithm, ReconstructionAlgorithm::Hogbom) {
        let controls = HogbomControls::from_compiled(problem.reconstruction().controls())?;
        let domain = &problem.geometry().domains()[0];
        let [width, height] = domain.shape().pixels();
        executor = executor.with_minor_cycle(
            minor_node.ok_or_else(|| boxed("initial plan omitted its minor-cycle node"))?,
            CleanWindow::new([0, 0], [width - 1, height - 1])?,
            controls,
        );
    }
    let registry = SerialContinuumRegistry::new(
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
        route_evidence.clone(),
        runtime.attempts[0],
    )?;
    let initial_receipt = runtime.receipts.open(runtime.attempts[0])?;

    let (scientific, final_major_receipt, minor_cycle) = match algorithm {
        ReconstructionAlgorithm::Dirty => {
            let result = registry
                .implementation()
                .take_completion()
                .ok_or_else(|| boxed("dirty execution omitted final major-cycle evidence"))?;
            (result.into_completion(), None, None)
        }
        ReconstructionAlgorithm::Hogbom => {
            let minor = registry
                .implementation()
                .take_minor_completion()
                .ok_or_else(|| boxed("Högbom execution omitted minor-cycle evidence"))?;
            let minor_cycle = Some(NativeMinorCycleOutcome {
                iterations: minor.evidence().iterations(),
                final_peak_flux: minor.evidence().final_peak_flux(),
                stop_reason: minor.evidence().stop_reason(),
            });
            let final_input = minor.into_final_major_input();
            let resolved = resolve_selected_observation(input.observation.clone())?;
            let (_, access) = resolved.into_parts();
            let access = access.with_minimum_content_budget(problem)?;
            let final_residency = access.certify_residency(problem)?;
            let final_planned = SerialContinuumPlan::final_major(
                problem,
                &planning_registry,
                execution_policy(&runtime, final_residency),
                &final_input,
            )?;
            let (physical, weighting, complete, resources, pass, _) = final_planned.into_parts();
            let executor = SerialContinuumExecutor::new(
                runtime.implementation.clone(),
                problem.clone(),
                weighting,
                resources,
                pass,
                complete,
                access.open(problem)?,
                ExecutableModelProblem::from_compiled(problem.clone())?,
                SerialContinuumPassInput::FinalMajor(final_input),
            );
            let registry = SerialContinuumRegistry::new(
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
            run_phase(
                problem,
                &final_plan,
                &registry,
                &runtime,
                route_evidence.clone(),
                runtime.attempts[1],
            )?;
            let completion = registry
                .implementation()
                .take_completion()
                .ok_or_else(|| boxed("final-major execution omitted scientific evidence"))?
                .into_completion();
            let receipt = runtime.receipts.open(runtime.attempts[1])?;
            (completion, Some(receipt), minor_cycle)
        }
        _ => unreachable!("native validation admits only dirty or Högbom"),
    };

    publish_products(
        problem,
        scientific,
        input.observation,
        runtime,
        publication,
        PriorPhaseOutcome {
            route: route_evidence,
            initial_receipt,
            final_major_receipt,
            minor_cycle,
        },
    )
}

struct PriorPhaseOutcome {
    route: ExecutionRouteEvidence,
    initial_receipt: ExecutionReceipt,
    final_major_receipt: Option<ExecutionReceipt>,
    minor_cycle: Option<NativeMinorCycleOutcome>,
}

fn validate_native_problem(problem: &CompiledProblem) -> Result<(), ApplicationError> {
    let reconstruction = problem.reconstruction();
    let supported = problem.inputs().observation_snapshot().sources().len() == 1
        && problem.geometry().domains().len() == 1
        && problem.geometry().domains()[0].facets().len() == 1
        && reconstruction.basis() == ReconstructionBasis::Constant
        && reconstruction.polarization().coordinates() == [PolarizationCoordinate::StokesI]
        && matches!(
            reconstruction.algorithm(),
            ReconstructionAlgorithm::Dirty | ReconstructionAlgorithm::Hogbom
        );
    if !supported {
        return Err(boxed(
            "migration matrix routed an unsupported problem to the serial continuum engine",
        ));
    }
    Ok(())
}

fn execution_policy(
    runtime: &ApplicationRuntime,
    residency: casa_ms::SelectedObservationResidencyCertificate,
) -> SerialContinuumExecutionPolicy {
    SerialContinuumExecutionPolicy::new(
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
    registry: &SerialContinuumRegistry<SerialContinuumExecutor>,
    runtime: &ApplicationRuntime,
    route: ExecutionRouteEvidence,
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
            .bind(ExecutionProvenance::new(attempt, runtime.build, route)),
    )?;
    Ok(())
}

fn publish_products<S>(
    problem: &CompiledProblem,
    scientific: MajorCycleCompletion,
    observation: SelectedObservationResolutionRequest,
    runtime: ApplicationRuntime,
    publication_config: ApplicationPublication<S>,
    prior: PriorPhaseOutcome,
) -> Result<NativeApplicationOutcome, ApplicationError>
where
    S: SerialProductPublicationSink + Send + 'static,
    S::Error: Send + Sync,
{
    let sources = ContinuumSourceCatalog::from_major_cycle(problem, &scientific)?;
    let authority = ProductGenerationAuthority::bind(problem);
    let planned_products = authority.plan(&sources, &publication_config.controls)?;

    let resolved = resolve_selected_observation(observation)?;
    let (_, access) = resolved.into_parts();
    let access = access.with_minimum_content_budget(problem)?;
    let residency = access.certify_residency(problem)?;
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
            residency,
            runtime.storage_io.clone(),
            runtime.stage_nanos,
            runtime.confidence_parts_per_million,
        ),
    )?;
    let inputs = ContinuumProductInputs::from_major_cycle(problem, &scientific)?;
    let produced = produce_continuum_members(&planned_products, &inputs)?;
    let sealed = authority.authorize(&planned_products, &produced)?;
    let (physical, publication) = publication_plan.into_parts();
    let executor = SerialProductPublicationExecutor::new(
        runtime.implementation.clone(),
        problem.clone(),
        publication,
        sealed,
        access.open(problem)?,
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
        runtime.receipts.bind(ExecutionProvenance::new(
            runtime.attempts[2],
            runtime.build,
            prior.route,
        )),
    )?;
    let publication_receipt = runtime.receipts.open(runtime.attempts[2])?;
    let products = registry
        .implementation()
        .take_sealed_generation()
        .ok_or_else(|| boxed("publication execution omitted its sealed product generation"))?;
    Ok(NativeApplicationOutcome {
        initial_receipt: prior.initial_receipt,
        final_major_receipt: prior.final_major_receipt,
        minor_cycle: prior.minor_cycle,
        publication_receipt,
        scientific,
        planned_products,
        products,
    })
}

fn execution_route(route: &RouteRecord) -> Result<ExecutionRouteEvidence, ApplicationError> {
    let mut requirements = route
        .requirements()
        .iter()
        .map(|row| {
            let obligation = row.obligation();
            ExecutionRouteRequirement::new(
                row.id(),
                match row.kind() {
                    MigrationRowKind::Capability => ExecutionRouteRequirementKind::Capability,
                    MigrationRowKind::Product => ExecutionRouteRequirementKind::Product,
                    MigrationRowKind::Solver => ExecutionRouteRequirementKind::Solver,
                    MigrationRowKind::Frontend => ExecutionRouteRequirementKind::Frontend,
                    MigrationRowKind::Backend => ExecutionRouteRequirementKind::Backend,
                },
                route_disposition(row.status()),
                ExecutionRouteRequirementEvidence {
                    current_owner: row.current_owner().to_string(),
                    destination_tickets: row.destination_tickets().to_vec(),
                    evidence_issues: row.evidence_issues().to_vec(),
                    baseline_manifests: row.baseline_manifests().to_vec(),
                    acceptance_contract: row.acceptance_contract().to_string(),
                    transfer_point: row.transfer_point().to_string(),
                    deletion_condition: row.deletion_condition().to_string(),
                    source_evidence: row.source_evidence().to_vec(),
                    obligation_ticket: obligation.map(|item| item.ticket().to_string()),
                    obligation_reason: obligation.map(|item| item.reason().to_string()),
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    requirements.sort_unstable_by(|left, right| left.id().cmp(right.id()));
    Ok(ExecutionRouteEvidence::new(
        route.matrix_schema_version(),
        route.matrix_contract_revision(),
        route_disposition(route.disposition()),
        requirements,
    )?)
}

const fn route_disposition(value: RequestDisposition) -> ExecutionRouteDisposition {
    match value {
        RequestDisposition::Native => ExecutionRouteDisposition::Native,
        RequestDisposition::TemporarilyUnavailable => {
            ExecutionRouteDisposition::TemporarilyUnavailable
        }
    }
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

/// Failure before or within the router-selected whole run.
#[derive(Debug)]
pub enum ApplicationDispatchError {
    /// MeasurementSet resolution or request preparation failed before routing.
    Preparation(ApplicationError),
    /// The router or selected whole-run engine failed.
    Dispatch(DispatchError<ApplicationError>),
}

impl fmt::Display for ApplicationDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preparation(error) => {
                write!(formatter, "imaging application preparation failed: {error}")
            }
            Self::Dispatch(error) => error.fmt(formatter),
        }
    }
}

impl Error for ApplicationDispatchError {}

fn boxed(message: &'static str) -> ApplicationError {
    Box::new(io::Error::other(message))
}
