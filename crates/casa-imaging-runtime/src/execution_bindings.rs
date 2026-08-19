// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, LogicalIdentity, NumericsContractId, ObservationSnapshotId,
    ProblemInputIdentities, ReferenceDataKind,
};
use sha2::{Digest, Sha256};

use crate::{ResourceOverride, ResourcePolicy};

const EXECUTION_PLAN_IDENTITY_DOMAIN: &[u8] = b"casa-rs-execution-plan";
const EXECUTION_PLAN_IDENTITY_VERSION: u32 = 1;
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
    numerics: NumericsContractId,
    implementation_registry: ImplementationRegistryId,
    resource_policy: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileId,
    physical_work: PhysicalWorkId,
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
        self.resource_policy
    }

    /// Return the exact reviewed cost-model profile identity.
    #[must_use]
    pub const fn planner_cost_model_profile_id(&self) -> PlannerCostModelProfileId {
        self.planner_cost_model_profile
    }

    /// Return the stable identity of the emitted physical work.
    #[must_use]
    pub const fn physical_work_id(&self) -> PhysicalWorkId {
        self.physical_work
    }
}

/// Seal planner-emitted physical work to the complete logical and planning context.
pub fn plan<E>(
    problem: &CompiledProblem,
    bindings: PlanningBindings,
    planner: impl FnOnce(&CompiledProblem, &PlanningBindings) -> Result<PhysicalWorkId, E>,
) -> Result<ExecutionPlan, E> {
    let physical_work = planner(problem, &bindings)?;
    let mut plan = ExecutionPlan {
        plan_id: ExecutionPlanId([0; 32]),
        problem_id: problem.problem_id(),
        problem_inputs: problem.inputs().clone(),
        numerics: problem.numerics_id(),
        implementation_registry: bindings.implementation_registry,
        resource_policy: bindings.resource_policy_id,
        planner_cost_model_profile: bindings.planner_cost_model_profile,
        physical_work,
    };
    plan.plan_id = execution_plan_id(&plan);
    Ok(plan)
}

/// Effective identities observed immediately before execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunBindings {
    problem_inputs: ProblemInputIdentities,
    implementation_registry: ImplementationRegistryId,
    resource_policy: ResourcePolicyId,
    planner_cost_model_profile: PlannerCostModelProfileId,
}

impl RunBindings {
    /// Capture identities observed immediately before execution.
    #[must_use]
    pub fn new(
        problem_inputs: ProblemInputIdentities,
        implementation_registry: ImplementationRegistryId,
        resource_policy: &ResourcePolicy,
        planner_cost_model_profile: PlannerCostModelProfileId,
    ) -> Self {
        Self {
            problem_inputs,
            implementation_registry,
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

/// Failure from exact plan binding validation or the selected executor.
#[derive(Debug, PartialEq, Eq)]
pub enum RunError<E> {
    /// A binding changed after planning; execution was not entered.
    BindingMismatch {
        /// Exact rejected binding.
        binding: BindingKind,
    },
    /// The exactly selected executor returned an error.
    Execution(E),
}

impl<E: fmt::Display> fmt::Display for RunError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BindingMismatch { binding } => {
                write!(formatter, "execution plan binding mismatch: {binding:?}")
            }
            Self::Execution(error) => write!(formatter, "execution failed: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for RunError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::BindingMismatch { .. } => None,
            Self::Execution(error) => Some(error),
        }
    }
}

/// Validate every binding and execute exactly the supplied immutable plan.
pub fn run<T, E>(
    problem: &CompiledProblem,
    plan: &ExecutionPlan,
    current: &RunBindings,
    executor: impl FnOnce(&CompiledProblem, &ExecutionPlan) -> Result<T, E>,
) -> Result<T, RunError<E>> {
    validate_bindings(problem, plan, current)?;
    executor(problem, plan).map_err(RunError::Execution)
}

fn validate_bindings<E>(
    problem: &CompiledProblem,
    plan: &ExecutionPlan,
    current: &RunBindings,
) -> Result<(), RunError<E>> {
    let mismatch = if plan.problem_id != problem.problem_id() {
        Some(BindingKind::CompiledProblem)
    } else if plan.problem_inputs.observation() != current.problem_inputs.observation() {
        Some(BindingKind::ObservationSnapshot)
    } else if plan.problem_inputs.geometry() != current.problem_inputs.geometry() {
        Some(BindingKind::CompiledGeometry)
    } else if !same_reference_snapshots(
        plan.problem_inputs.reference_data(),
        current.problem_inputs.reference_data(),
    ) {
        Some(BindingKind::ReferenceDataSnapshots)
    } else if plan.problem_inputs.model() != current.problem_inputs.model() {
        Some(BindingKind::ModelState)
    } else if plan.implementation_registry != current.implementation_registry {
        Some(BindingKind::ImplementationRegistry)
    } else if plan.resource_policy != current.resource_policy {
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

fn same_reference_snapshots(
    planned: &[(ReferenceDataKind, LogicalIdentity)],
    current: &[(ReferenceDataKind, LogicalIdentity)],
) -> bool {
    planned.len() == current.len() && planned.iter().all(|binding| current.contains(binding))
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
    encoder.digest(plan.problem_inputs.geometry().identity().as_bytes());
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
    encoder.digest(plan.resource_policy.as_bytes());
    encoder.digest(plan.planner_cost_model_profile.as_bytes());
    encoder.digest(plan.physical_work.as_bytes());
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

struct CanonicalEncoder(Sha256);

impl CanonicalEncoder {
    fn new() -> Self {
        Self(Sha256::new())
    }

    fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_le_bytes());
    }

    fn usize(&mut self, value: usize) {
        self.0.update((value as u128).to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.usize(value.len());
        self.0.update(value);
    }

    fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn digest(&mut self, value: [u8; 32]) {
        self.0.update(value);
    }

    fn optional_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
            None => self.u8(0),
        }
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}
