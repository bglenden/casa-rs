// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, error::Error, fmt};

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, NumericsContractId,
    ObservationSnapshotId, ProblemInputIdentities, ReferenceDataKind,
};
use sha2::{Digest, Sha256};

use crate::{
    AdaptationId, AdaptationTransition, ExecutionError, ExecutionKnobs, ExecutionOutcome,
    FenceKind, ResourceAuthority, ResourceOverride, ResourcePolicy, ScheduledWork,
    WorkImplementationId, WorkKind, WorkNodeId,
    execution::{ExecutionDag, ExecutionScheduler, SchedulerAction, SchedulerTerminal, WorkResult},
};

const EXECUTION_PLAN_IDENTITY_DOMAIN: &[u8] = b"casa-rs-execution-plan";
const EXECUTION_PLAN_IDENTITY_VERSION: u32 = 3;
const RESOURCE_POLICY_IDENTITY_DOMAIN: &[u8] = b"casa-rs-resource-policy";
const RESOURCE_POLICY_IDENTITY_VERSION: u32 = 1;

macro_rules! digest_identity {
    ($name:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Construct an identity from an already computed SHA-256 digest.
            #[must_use]
            pub const fn from_sha256(digest: [u8; 32]) -> Self {
                Self(digest)
            }

            /// Return the exact SHA-256 digest.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 32] {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                write_hex(formatter, &self.0)?;
                formatter.write_str(")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(formatter, &self.0)
            }
        }
    };
}

digest_identity!(
    ImplementationRegistryId,
    "Stable content identity of one immutable implementation-registry snapshot."
);
digest_identity!(
    PlannerCostModelProfileId,
    "Stable content identity of one reviewed planner cost-model profile."
);
digest_identity!(
    PhysicalWorkId,
    "Stable content identity of the physical work emitted by planning."
);

/// Complete physical work emitted by the sole planning seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalWorkBinding {
    execution_dag: ExecutionDag,
}

impl PhysicalWorkBinding {
    /// Bind a complete immutable physical work DAG.
    #[must_use]
    pub const fn new(execution_dag: ExecutionDag) -> Self {
        Self { execution_dag }
    }

    /// Return the stable physical-work identity.
    #[must_use]
    pub const fn physical_work_id(&self) -> PhysicalWorkId {
        self.execution_dag.physical_work_id()
    }

    /// Return the complete immutable physical work DAG.
    #[must_use]
    pub const fn execution_dag(&self) -> &ExecutionDag {
        &self.execution_dag
    }
}

/// Stable identity of the exact host-use policy bound into a plan.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourcePolicyId([u8; 32]);

impl ResourcePolicyId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = RESOURCE_POLICY_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for ResourcePolicyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResourcePolicyId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ResourcePolicyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Stable identity of one immutable execution plan and all its bindings.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutionPlanId([u8; 32]);

impl ExecutionPlanId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = EXECUTION_PLAN_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for ExecutionPlanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExecutionPlanId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ExecutionPlanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Immutable inputs available to the sole physical planning entrypoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanningBindings {
    implementation_registry: ImplementationRegistryId,
    resource_policy: ResourcePolicy,
    resource_policy_id: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileId,
}

impl PlanningBindings {
    /// Bind one registry snapshot, host-use policy, and reviewed cost model.
    #[must_use]
    pub fn new(
        implementation_registry: ImplementationRegistryId,
        resource_policy: ResourcePolicy,
        planner_cost_model_profile: PlannerCostModelProfileId,
    ) -> Self {
        let resource_policy_id = resource_policy_id(&resource_policy);
        Self {
            implementation_registry,
            resource_policy,
            resource_policy_id,
            planner_cost_model_profile,
        }
    }

    /// Return the exact implementation-registry snapshot identity.
    #[must_use]
    pub const fn implementation_registry_id(&self) -> ImplementationRegistryId {
        self.implementation_registry
    }

    /// Return the selected host-use policy.
    #[must_use]
    pub const fn resource_policy(&self) -> &ResourcePolicy {
        &self.resource_policy
    }

    /// Return the canonical identity of the selected host-use policy.
    #[must_use]
    pub const fn resource_policy_id(&self) -> ResourcePolicyId {
        self.resource_policy_id
    }

    /// Return the exact reviewed cost-model profile identity.
    #[must_use]
    pub const fn planner_cost_model_profile_id(&self) -> PlannerCostModelProfileId {
        self.planner_cost_model_profile
    }
}

/// Immutable physical execution plan sealed to one complete binding set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPlan {
    plan_id: ExecutionPlanId,
    problem_id: CompiledProblemId,
    problem_inputs: ProblemInputIdentities,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    implementation_registry: ImplementationRegistryId,
    resource_policy: ResourcePolicy,
    resource_policy_id: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileId,
    execution_dag: ExecutionDag,
}

impl ExecutionPlan {
    /// Return the stable identity of this plan and all of its bindings.
    #[must_use]
    pub const fn plan_id(&self) -> ExecutionPlanId {
        self.plan_id
    }

    /// Return the exact compiled problem identity.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the exact observation snapshot identity.
    #[must_use]
    pub const fn observation_snapshot_id(&self) -> ObservationSnapshotId {
        self.problem_inputs.observation()
    }

    /// Return the compiler-derived coordinate and image-domain geometry identity.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry
    }

    /// Return the exact numerical-contract identity.
    #[must_use]
    pub const fn numerics_id(&self) -> NumericsContractId {
        self.numerics
    }

    /// Return the exact implementation-registry snapshot identity.
    #[must_use]
    pub const fn implementation_registry_id(&self) -> ImplementationRegistryId {
        self.implementation_registry
    }

    /// Return the exact resource-policy identity.
    #[must_use]
    pub const fn resource_policy_id(&self) -> ResourcePolicyId {
        self.resource_policy_id
    }

    /// Return the host-use policy selected during planning.
    #[must_use]
    pub const fn resource_policy(&self) -> &ResourcePolicy {
        &self.resource_policy
    }

    /// Return the exact reviewed cost-model profile identity.
    #[must_use]
    pub const fn planner_cost_model_profile_id(&self) -> PlannerCostModelProfileId {
        self.planner_cost_model_profile
    }

    /// Return the stable identity of the emitted physical work.
    #[must_use]
    pub const fn physical_work_id(&self) -> PhysicalWorkId {
        self.execution_dag.physical_work_id()
    }

    /// Return the complete immutable physical work DAG selected by planning.
    #[must_use]
    pub const fn execution_dag(&self) -> &ExecutionDag {
        &self.execution_dag
    }
}

/// Seal planner-emitted physical work to the complete logical and planning context.
pub fn plan<E>(
    problem: &CompiledProblem,
    bindings: PlanningBindings,
    planner: impl FnOnce(&CompiledProblem, &PlanningBindings) -> Result<PhysicalWorkBinding, E>,
) -> Result<ExecutionPlan, E> {
    let physical_work = planner(problem, &bindings)?;
    let mut plan = ExecutionPlan {
        plan_id: ExecutionPlanId([0; 32]),
        problem_id: problem.problem_id(),
        problem_inputs: problem.inputs().clone(),
        geometry: problem.geometry().geometry_id(),
        numerics: problem.numerics_id(),
        implementation_registry: bindings.implementation_registry,
        resource_policy: bindings.resource_policy,
        resource_policy_id: bindings.resource_policy_id,
        planner_cost_model_profile: bindings.planner_cost_model_profile,
        execution_dag: physical_work.execution_dag,
    };
    plan.plan_id = execution_plan_id(&plan);
    Ok(plan)
}

/// Effective identities observed immediately before execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunBindings {
    problem_inputs: ProblemInputIdentities,
    resource_policy: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileId,
}

impl RunBindings {
    /// Capture identities observed immediately before execution.
    #[must_use]
    pub fn new(
        problem_inputs: ProblemInputIdentities,
        resource_policy: &ResourcePolicy,
        planner_cost_model_profile: PlannerCostModelProfileId,
    ) -> Self {
        Self {
            problem_inputs,
            resource_policy: resource_policy_id(resource_policy),
            planner_cost_model_profile,
        }
    }
}

/// Exact binding whose mismatch prevented execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindingKind {
    /// Compiled problem identity.
    CompiledProblem,
    /// Observation snapshot identity.
    ObservationSnapshot,
    /// Compiled coordinate and image-domain geometry identity.
    CompiledGeometry,
    /// Canonical reference-data snapshot identities.
    ReferenceDataSnapshots,
    /// Initial model-state identity.
    ModelState,
    /// Implementation-registry snapshot identity.
    ImplementationRegistry,
    /// Resource-policy identity.
    ResourcePolicy,
    /// Planner cost-model profile identity.
    PlannerCostModelProfile,
}

/// Failure from exact plan binding validation, resource-backed scheduling, or
/// one plan-selected work implementation.
#[derive(Debug, PartialEq, Eq)]
pub enum RunError<E> {
    /// A binding changed after planning; execution was not entered.
    BindingMismatch {
        /// Exact rejected binding.
        binding: BindingKind,
    },
    /// The bound registry snapshot does not contain one selected work implementation.
    ImplementationUnavailable {
        /// Exact selected implementation missing from the registry.
        implementation: WorkImplementationId,
    },
    /// The registry returned an adapter under a different stable identity.
    ImplementationMismatch {
        /// Identity selected by the immutable DAG.
        planned: WorkImplementationId,
        /// Identity reported by the resolved adapter.
        observed: WorkImplementationId,
    },
    /// Resource admission or deterministic scheduling failed.
    Scheduler(ExecutionError),
    /// One exact plan-owned work node or its asynchronous fence failed.
    Execution {
        /// Node whose adapter reported the failure.
        node: WorkNodeId,
        /// Adapter failure retained as the error source.
        source: E,
    },
}

impl<E: fmt::Display> fmt::Display for RunError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BindingMismatch { binding } => {
                write!(formatter, "execution plan binding mismatch: {binding:?}")
            }
            Self::ImplementationUnavailable { implementation } => {
                write!(
                    formatter,
                    "bound implementation is unavailable: {}",
                    implementation.as_str()
                )
            }
            Self::ImplementationMismatch { planned, observed } => write!(
                formatter,
                "implementation registry returned {} for planned work adapter {}",
                observed.as_str(),
                planned.as_str()
            ),
            Self::Scheduler(error) => write!(formatter, "execution scheduling failed: {error}"),
            Self::Execution { node, source } => {
                write!(formatter, "work node {} failed: {source}", node.as_str())
            }
        }
    }
}

impl<E: Error + 'static> Error for RunError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::BindingMismatch { .. }
            | Self::ImplementationUnavailable { .. }
            | Self::ImplementationMismatch { .. } => None,
            Self::Scheduler(error) => Some(error),
            Self::Execution { source, .. } => Some(source),
        }
    }
}

/// One exact plan-selected work-node adapter stored in an immutable registry.
pub trait WorkImplementation {
    /// Execution failure.
    type Error: Error + 'static;

    /// Return this adapter's stable plan identity.
    fn implementation_id(&self) -> &WorkImplementationId;

    /// Launch or synchronously execute exactly one scheduled node.
    ///
    /// Returning `Ok(())` means every fence declared by the node was launched
    /// and can subsequently be joined through [`Self::wait_for_fence`].
    /// Returning `Err` guarantees that no asynchronous work escaped.
    fn execute(&self, problem: &CompiledProblem, work: &ScheduledWork) -> Result<(), Self::Error>;

    /// Block until one exact fence previously launched by [`Self::execute`]
    /// settles. An error means the fence settled unsuccessfully, so the
    /// scheduler may drain and release resources after recording failure.
    fn wait_for_fence(
        &self,
        problem: &CompiledProblem,
        work: &ScheduledWork,
        fence: FenceKind,
    ) -> Result<(), Self::Error>;
}

/// Immutable registry snapshot that resolves selected implementations by identity.
pub trait ImplementationRegistry {
    /// Homogeneous execution interface stored by this registry.
    type Implementation: WorkImplementation;

    /// Return the exact snapshot identity bound during planning.
    fn registry_id(&self) -> ImplementationRegistryId;

    /// Resolve one implementation without substituting another candidate.
    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation>;
}

/// Immutable scheduler state exposed to a run controller without exposing the
/// scheduler or its Resource Lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionStatus {
    lease_epoch: u64,
    pressure_changed: bool,
    knobs: ExecutionKnobs,
    applied_adaptations: Vec<AdaptationId>,
    eligible_adaptations: Vec<AdaptationTransition>,
}

impl ExecutionStatus {
    /// Returns the Resource Authority epoch backing this run.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    /// Returns whether external pressure changed since lease admission.
    #[must_use]
    pub const fn pressure_changed(&self) -> bool {
        self.pressure_changed
    }

    /// Returns the exact current plan-authorized execution configuration.
    #[must_use]
    pub const fn knobs(&self) -> &ExecutionKnobs {
        &self.knobs
    }

    /// Returns the transitions already applied by this run.
    #[must_use]
    pub fn applied_adaptations(&self) -> &[AdaptationId] {
        &self.applied_adaptations
    }

    /// Returns only transitions applicable at the current globally idle cut.
    /// The list is empty while work or fences are active.
    #[must_use]
    pub fn eligible_adaptations(&self) -> &[AdaptationTransition] {
        &self.eligible_adaptations
    }
}

/// One controller request interpreted only through the plan-owned scheduler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunDirective {
    /// Continue with the current plan-authorized configuration.
    Continue,
    /// Cancel pending work and drain all launched work and fences.
    Cancel,
    /// Apply one exact pre-authorized transition at its declared quiescence point.
    Adapt(AdaptationId),
}

/// Scheduling policy consulted by the sole validated [`run`] seam.
pub trait RunController {
    /// Return the next request. The scheduler rejects any unlisted transition
    /// or adaptation outside its exact global quiescence boundary.
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective;
}

/// Controller that executes the sealed initial configuration to completion.
#[derive(Clone, Copy, Debug, Default)]
pub struct RunToCompletion;

impl RunController for RunToCompletion {
    fn directive(&mut self, _status: &ExecutionStatus) -> RunDirective {
        RunDirective::Continue
    }
}

enum PendingRunError<E> {
    Scheduler(ExecutionError),
    Execution { node: WorkNodeId, source: E },
}

impl<E> PendingRunError<E> {
    fn into_run_error(self) -> RunError<E> {
        match self {
            Self::Scheduler(error) => RunError::Scheduler(error),
            Self::Execution { node, source } => RunError::Execution { node, source },
        }
    }
}

fn defer_scheduler_error<E>(
    scheduler: &mut ExecutionScheduler<'_>,
    pending: &mut Option<PendingRunError<E>>,
    error: ExecutionError,
) {
    if pending.is_none() {
        *pending = Some(PendingRunError::Scheduler(error));
    }
    scheduler.cancel_after_error();
}

/// Validate every binding, resolve every selected node adapter, acquire the
/// plan's Resource Authority lease, and drive the complete DAG to settlement.
pub fn run<R, C>(
    problem: &CompiledProblem,
    plan: &ExecutionPlan,
    current: &RunBindings,
    registry: &R,
    authority: &ResourceAuthority,
    controller: &mut C,
) -> Result<ExecutionOutcome, RunError<<R::Implementation as WorkImplementation>::Error>>
where
    R: ImplementationRegistry,
    C: RunController,
{
    validate_bindings(problem, plan, current)?;
    if plan.implementation_registry != registry.registry_id() {
        return Err(RunError::BindingMismatch {
            binding: BindingKind::ImplementationRegistry,
        });
    }
    let mut implementations = BTreeMap::new();
    for identity in plan.execution_dag.selected_implementations() {
        let implementation =
            registry
                .resolve(identity)
                .ok_or_else(|| RunError::ImplementationUnavailable {
                    implementation: identity.clone(),
                })?;
        if implementation.implementation_id() != identity {
            return Err(RunError::ImplementationMismatch {
                planned: identity.clone(),
                observed: implementation.implementation_id().clone(),
            });
        }
        implementations.insert(identity.clone(), implementation);
    }
    let mut scheduler = ExecutionScheduler::start(plan, authority).map_err(RunError::Scheduler)?;
    let mut launched = BTreeMap::<WorkNodeId, ScheduledWork>::new();
    let mut pending = None;
    let mut controller_stopped = false;
    loop {
        if pending.is_none() && !controller_stopped {
            let status = match (scheduler.lease_epoch(), scheduler.pressure_changed()) {
                (Some(lease_epoch), Ok(Some(pressure_changed))) => ExecutionStatus {
                    lease_epoch,
                    pressure_changed,
                    knobs: scheduler.knobs().clone(),
                    applied_adaptations: scheduler.applied_adaptations().to_vec(),
                    eligible_adaptations: scheduler.eligible_adaptations(),
                },
                (None, _) | (_, Ok(None)) => {
                    defer_scheduler_error(
                        &mut scheduler,
                        &mut pending,
                        ExecutionError::InvalidState(
                            "active execution cannot observe its Resource Authority lease"
                                .to_string(),
                        ),
                    );
                    controller_stopped = true;
                    continue;
                }
                (_, Err(error)) => {
                    defer_scheduler_error(&mut scheduler, &mut pending, error);
                    controller_stopped = true;
                    continue;
                }
            };
            match controller.directive(&status) {
                RunDirective::Continue => {}
                RunDirective::Cancel => {
                    if let Err(error) = scheduler.cancel() {
                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                    }
                    controller_stopped = true;
                }
                RunDirective::Adapt(adaptation) => {
                    let eligible = status
                        .eligible_adaptations
                        .iter()
                        .map(|transition| transition.id.clone())
                        .collect::<Vec<_>>();
                    if !eligible.contains(&adaptation) {
                        defer_scheduler_error(
                            &mut scheduler,
                            &mut pending,
                            ExecutionError::IneligibleAdaptation {
                                requested: adaptation,
                                eligible,
                            },
                        );
                        controller_stopped = true;
                    } else if let Err(error) = scheduler.adapt(&adaptation) {
                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
                    }
                }
            }
        }
        let action = match scheduler.next_action() {
            Ok(action) => action,
            Err(error) if pending.is_none() => {
                defer_scheduler_error(&mut scheduler, &mut pending, error);
                controller_stopped = true;
                continue;
            }
            Err(_) => {
                let _ = scheduler.quarantine();
                return Err(pending
                    .take()
                    .expect("draining scheduler error has a primary failure")
                    .into_run_error());
            }
        };
        match action {
            SchedulerAction::Work(work) => {
                let work = *work;
                let node_id = work.node().id.clone();
                let implementation = implementations[&work.node().implementation];
                match implementation.execute(problem, &work) {
                    Ok(()) => {
                        launched.insert(node_id.clone(), work);
                        if let Err(error) = scheduler.finish_work(node_id, WorkResult::Succeeded) {
                            defer_scheduler_error(&mut scheduler, &mut pending, error);
                            controller_stopped = true;
                        }
                    }
                    Err(source) => {
                        if work.node().kind == WorkKind::Release {
                            if pending.is_none() {
                                pending = Some(PendingRunError::Execution {
                                    node: node_id.clone(),
                                    source,
                                });
                            }
                            controller_stopped = true;
                            if scheduler.fail_release_work(&node_id).is_err() {
                                let _ = scheduler.quarantine();
                                return Err(pending
                                    .take()
                                    .expect("release failure is retained")
                                    .into_run_error());
                            }
                            scheduler.cancel_after_error();
                            continue;
                        }
                        let diagnostic = source.to_string();
                        pending = Some(PendingRunError::Execution {
                            node: node_id.clone(),
                            source,
                        });
                        controller_stopped = true;
                        match scheduler.finish_work(
                            node_id,
                            WorkResult::Failed {
                                message: diagnostic,
                            },
                        ) {
                            Ok(fences) => {
                                for fence in fences {
                                    if scheduler.complete_fence(fence).is_err() {
                                        let _ = scheduler.quarantine();
                                        return Err(pending
                                            .take()
                                            .expect("executor failure is retained")
                                            .into_run_error());
                                    }
                                }
                            }
                            Err(_) => {
                                scheduler.cancel_after_error();
                            }
                        }
                    }
                }
            }
            SchedulerAction::Waiting { .. } => {
                let Some(fence) = scheduler.next_pending_fence() else {
                    let error = ExecutionError::InvalidState(
                        "scheduler reported waiting without an outstanding fence".to_string(),
                    );
                    if pending.is_none() {
                        defer_scheduler_error(&mut scheduler, &mut pending, error);
                        controller_stopped = true;
                        continue;
                    }
                    let _ = scheduler.quarantine();
                    return Err(pending
                        .take()
                        .expect("waiting failure has a primary error")
                        .into_run_error());
                };
                let Some(work) = launched.get(fence.node()) else {
                    let _ = scheduler.quarantine();
                    return Err(pending
                        .take()
                        .unwrap_or_else(|| {
                            PendingRunError::Scheduler(ExecutionError::InvalidState(
                                "outstanding fence has no launched work declaration".to_string(),
                            ))
                        })
                        .into_run_error());
                };
                let implementation = implementations[&work.node().implementation];
                if let Err(source) = implementation.wait_for_fence(problem, work, fence.kind()) {
                    if work.node().kind == WorkKind::Release {
                        if pending.is_none() {
                            pending = Some(PendingRunError::Execution {
                                node: fence.node().clone(),
                                source,
                            });
                        }
                        controller_stopped = true;
                        if scheduler.fail_release_fence(fence).is_err() {
                            let _ = scheduler.quarantine();
                            return Err(pending
                                .take()
                                .expect("release fence failure is retained")
                                .into_run_error());
                        }
                        scheduler.cancel_after_error();
                        continue;
                    }
                    if pending.is_none() {
                        pending = Some(PendingRunError::Execution {
                            node: fence.node().clone(),
                            source,
                        });
                    }
                    controller_stopped = true;
                    if scheduler
                        .fail_fence(fence.clone(), "asynchronous work failed".to_string())
                        .is_err()
                    {
                        let _ = scheduler.quarantine();
                        return Err(pending
                            .take()
                            .expect("fence failure is retained")
                            .into_run_error());
                    }
                } else if let Err(error) = scheduler.complete_fence(fence) {
                    defer_scheduler_error(&mut scheduler, &mut pending, error);
                    controller_stopped = true;
                }
            }
            SchedulerAction::Complete(terminal) => {
                return match pending.take() {
                    Some(failure) => Err(failure.into_run_error()),
                    None => match terminal {
                        SchedulerTerminal::Succeeded => Ok(ExecutionOutcome::Succeeded),
                        SchedulerTerminal::Cancelled => Ok(ExecutionOutcome::Cancelled),
                        SchedulerTerminal::Failed { .. } => {
                            Err(RunError::Scheduler(ExecutionError::InvalidState(
                                "scheduler reported failure without its adapter error".to_string(),
                            )))
                        }
                    },
                };
            }
        }
    }
}

fn validate_bindings<E>(
    problem: &CompiledProblem,
    plan: &ExecutionPlan,
    current: &RunBindings,
) -> Result<(), RunError<E>> {
    let mismatch = if plan.problem_id != problem.problem_id() {
        Some(BindingKind::CompiledProblem)
    } else if plan.geometry != problem.geometry().geometry_id() {
        Some(BindingKind::CompiledGeometry)
    } else if plan.problem_inputs.reference_data() != current.problem_inputs.reference_data() {
        Some(BindingKind::ReferenceDataSnapshots)
    } else if plan.problem_inputs.model() != current.problem_inputs.model() {
        Some(BindingKind::ModelState)
    } else if plan.problem_inputs.observation() != current.problem_inputs.observation() {
        Some(BindingKind::ObservationSnapshot)
    } else if plan.resource_policy_id != current.resource_policy {
        Some(BindingKind::ResourcePolicy)
    } else if plan.planner_cost_model_profile != current.planner_cost_model_profile {
        Some(BindingKind::PlannerCostModelProfile)
    } else {
        None
    };
    match mismatch {
        Some(binding) => Err(RunError::BindingMismatch { binding }),
        None => Ok(()),
    }
}

fn resource_policy_id(policy: &ResourcePolicy) -> ResourcePolicyId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(RESOURCE_POLICY_IDENTITY_DOMAIN);
    encoder.u32(RESOURCE_POLICY_IDENTITY_VERSION);
    match policy {
        ResourcePolicy::Interactive => encoder.u8(0),
        ResourcePolicy::Balanced => encoder.u8(1),
        ResourcePolicy::Exclusive => encoder.u8(2),
        ResourcePolicy::Explicit(overrides) => {
            encoder.u8(3);
            encode_resource_overrides(&mut encoder, overrides);
        }
    }
    ResourcePolicyId(encoder.finish())
}

fn encode_resource_overrides(encoder: &mut CanonicalEncoder, overrides: &ResourceOverride) {
    encoder.usize(overrides.memory_bytes.len());
    for (domain, bytes) in &overrides.memory_bytes {
        encoder.string(domain.as_str());
        encoder.u64(*bytes);
    }
    encoder.optional_u64(overrides.workers);
    encoder.usize(overrides.storage_bytes.len());
    for (domain, bytes) in &overrides.storage_bytes {
        encoder.string(domain.as_str());
        encoder.u64(*bytes);
    }
    encoder.usize(overrides.rates_per_second.len());
    for (resource, rate) in &overrides.rates_per_second {
        encoder.string(resource.as_str());
        encoder.u64(*rate);
    }
    encoder.optional_u64(overrides.cache_bytes);
    encoder.optional_u64(overrides.locks);
    encoder.optional_u64(overrides.file_descriptors);
    encoder.usize(overrides.queue_slots.len());
    for (resource, slots) in &overrides.queue_slots {
        encoder.string(resource.as_str());
        encoder.u64(*slots);
    }
    encoder.usize(overrides.accelerator_slots.len());
    for (accelerator, slots) in &overrides.accelerator_slots {
        encoder.string(accelerator.as_str());
        encoder.u64(*slots);
    }
}

fn execution_plan_id(plan: &ExecutionPlan) -> ExecutionPlanId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(EXECUTION_PLAN_IDENTITY_DOMAIN);
    encoder.u32(EXECUTION_PLAN_IDENTITY_VERSION);
    encoder.digest(plan.problem_id.as_bytes());
    encoder.digest(plan.problem_inputs.observation().identity().as_bytes());
    encoder.digest(plan.geometry.as_bytes());
    encoder.usize(plan.problem_inputs.reference_data().len());
    for (kind, identity) in plan.problem_inputs.reference_data() {
        encoder.u8(reference_data_tag(*kind));
        encoder.digest(identity.as_bytes());
    }
    match plan.problem_inputs.model() {
        casa_imaging_model::ModelStateIdentity::Empty => encoder.u8(0),
        casa_imaging_model::ModelStateIdentity::Seed(identity) => {
            encoder.u8(1);
            encoder.digest(identity.as_bytes());
        }
        casa_imaging_model::ModelStateIdentity::Generation(identity) => {
            encoder.u8(2);
            encoder.digest(identity.as_bytes());
        }
    }
    encoder.digest(plan.numerics.as_bytes());
    encoder.digest(plan.implementation_registry.as_bytes());
    encoder.digest(plan.resource_policy_id.as_bytes());
    encoder.digest(plan.planner_cost_model_profile.as_bytes());
    encoder.digest(plan.execution_dag.physical_work_id().as_bytes());
    ExecutionPlanId(encoder.finish())
}

fn reference_data_tag(kind: ReferenceDataKind) -> u8 {
    match kind {
        ReferenceDataKind::Measures => 0,
        ReferenceDataKind::Ephemeris => 1,
        ReferenceDataKind::Observatory => 2,
        ReferenceDataKind::SpectralLines => 3,
        ReferenceDataKind::Instrument => 4,
    }
}

pub(crate) struct CanonicalEncoder(Sha256);

impl CanonicalEncoder {
    pub(crate) fn new() -> Self {
        Self(Sha256::new())
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.0.update(value.to_le_bytes());
    }

    pub(crate) fn usize(&mut self, value: usize) {
        self.0.update((value as u128).to_le_bytes());
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) {
        self.usize(value.len());
        self.0.update(value);
    }

    pub(crate) fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    pub(crate) fn digest(&mut self, value: [u8; 32]) {
        self.0.update(value);
    }

    pub(crate) fn optional_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
            None => self.u8(0),
        }
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
