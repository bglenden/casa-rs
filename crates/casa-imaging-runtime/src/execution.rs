// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use crate::execution_bindings::CanonicalEncoder;
use crate::{
    AlternativeId, CapabilityId, CapacityDomainId, CapacityViewId, CountDemand, DemandAlternative,
    DemandAlternatives, LeaseResource, MemoryCapacityKind, MemoryViewKind, PhysicalWorkId,
    QuiescencePoint, ResourceAuthority, ResourceError, ResourceFence, ResourceLease,
    ResourcePermit, ResourceTopology, RuntimeOverheadKind, StorageUseKind,
};

const PHYSICAL_WORK_IDENTITY_DOMAIN: &[u8] = b"casa-rs-physical-work-dag";
const PHYSICAL_WORK_IDENTITY_VERSION: u32 = 7;

macro_rules! execution_identity {
    ($name:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Creates an identity from stable plan text.
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the stable identity text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }
    };
}

execution_identity!(
    WorkNodeId,
    "Stable identity of one execution-plan work node."
);
execution_identity!(
    WorkImplementationId,
    "Stable identity of one selected implementation."
);
execution_identity!(
    AllocationId,
    "Stable identity of one logical allocation lifetime."
);
execution_identity!(
    PhysicalSlotId,
    "Stable identity of one reusable physical storage slot."
);
execution_identity!(
    AllocationLayout,
    "Stable identity of one allocation layout contract."
);
execution_identity!(
    AdaptationId,
    "Stable identity of one pre-authorized adaptation."
);

/// An explicit class of work in the complete execution DAG.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WorkKind {
    /// Census selected input metadata or data extents.
    DataCensus,
    /// Decode, transform, or otherwise prepare bounded inputs.
    Preparation,
    /// Look up, validate, load, generate, or retain a prepared cache artifact.
    Cache,
    /// Generate or load a convolution-function artifact.
    ConvolutionFunction,
    /// Create an FFT plan or FFT-owned workspace.
    FftPlanning,
    /// Compile or load generated accelerator code.
    Jit,
    /// Perform numerical CPU or accelerator work.
    Compute,
    /// Move bytes over one explicitly declared transfer link.
    Transfer,
    /// Spill bounded state to declared storage.
    Spill,
    /// Prefetch bounded state from declared storage.
    Prefetch,
    /// Perform a declared source, transfer, or product I/O operation.
    Io,
    /// Read the exact compiled MeasurementSet source set under its named locks.
    ObservationRead,
    /// Read the exact compiled MeasurementSet source set while writing bounded
    /// selected column cells in place under the same transaction.
    ObservationReadWriteback,
    /// Serialize a prepared or scientific artifact.
    Serialization,
    /// Complete a private staged storage writeback without publishing it.
    Writeback,
    /// Revalidate and atomically publish the conventional-product members of
    /// one transaction. `MODEL_DATA` is written in place by the terminal
    /// [`Self::ObservationReadWriteback`] replay and is not a publication member.
    Publication,
    /// Explicitly unmap, evict, destroy, or otherwise release externally
    /// retained storage before its physical slot becomes reusable.
    Release,
    /// Perform resource-free dependency synchronization.
    Synchronization,
}

impl WorkKind {
    /// Return whether this work owns a selected-observation read completion.
    #[must_use]
    pub const fn reads_observation(self) -> bool {
        matches!(self, Self::ObservationRead | Self::ObservationReadWriteback)
    }
}

/// Runtime domain in which one work node executes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkDomain {
    /// CPU work charged to lease worker slots.
    Cpu,
    /// Apple Silicon Metal work charged to one accelerator demand and queue.
    Metal {
        /// Demand identity used by both accelerator and command-queue claims.
        demand_id: String,
    },
    /// Storage or transfer work charged to bounded rate and queue resources.
    /// A selected implementation may additionally declare a typed staging
    /// buffer, but Apple unified-memory paths may operate zero-copy.
    Io,
    /// Resource-free dependency or synchronization bookkeeping.
    Control,
}

/// Asynchronous completion class that extends work liveness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FenceKind {
    /// Accelerator command completion.
    Device,
    /// Source, transfer, spill, prefetch, or serialization I/O completion.
    Io,
    /// Durable staged-output writeback completion.
    Writeback,
    /// Product and MeasurementSet-side-effect publication readiness completion.
    Publication,
}

/// Stable identity of one fence declared by a work node.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FenceId {
    node: WorkNodeId,
    kind: FenceKind,
}

impl FenceId {
    /// Creates a fence identity from its producer and completion class.
    #[must_use]
    pub fn new(node: WorkNodeId, kind: FenceKind) -> Self {
        Self { node, kind }
    }

    /// Returns the producing work node.
    #[must_use]
    pub const fn node(&self) -> &WorkNodeId {
        &self.node
    }

    /// Returns the fence completion class.
    #[must_use]
    pub const fn kind(&self) -> FenceKind {
        self.kind
    }
}

/// One typed predecessor condition for a work node.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WorkDependency {
    /// The predecessor's synchronous work returned.
    Work(WorkNodeId),
    /// One declared asynchronous predecessor fence completed.
    Fence(FenceId),
}

impl WorkDependency {
    fn predecessor(&self) -> &WorkNodeId {
        match self {
            Self::Work(node) => node,
            Self::Fence(fence) => fence.node(),
        }
    }
}

/// Lifetime for one resource permit or physical-slot use.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClaimLifetime {
    /// Release when the synchronous work call returns.
    Work,
    /// Retain until every listed asynchronous fence completes.
    Fences(BTreeSet<FenceKind>),
    /// Retain one permit continuously through an explicit terminal Release node.
    ///
    /// Ordered intermediate nodes borrow the same scheduler-held permit; they
    /// never reacquire or double-charge the underlying resource.
    RetainedUntil(WorkNodeId),
    /// Transfer the permit to the immutable artifact produced by this work.
    ///
    /// The plan's sole admitted lease remains charged until the artifact drops
    /// the opaque permit. This lifetime is valid only for resource claims, not
    /// logical allocations.
    Artifact,
}

impl ClaimLifetime {
    /// Retains a resource or slot through one asynchronous fence.
    #[must_use]
    pub fn through_fence(kind: FenceKind) -> Self {
        Self::Fences(BTreeSet::from([kind]))
    }

    /// Retains a resource or slot until every listed fence completes.
    #[must_use]
    pub fn through_fences(kinds: impl IntoIterator<Item = FenceKind>) -> Self {
        Self::Fences(kinds.into_iter().collect())
    }

    /// Retains one permit continuously until the named Release work returns.
    #[must_use]
    pub fn retained_until(release: WorkNodeId) -> Self {
        Self::RetainedUntil(release)
    }

    fn retains_fence(&self, kind: FenceKind) -> bool {
        matches!(self, Self::Fences(kinds) if kinds.contains(&kind))
            || matches!(self, Self::RetainedUntil(_))
    }
}

/// Opaque plan-issued resources retained by one immutable output artifact.
///
/// This capability can only be minted by the execution scheduler from the
/// plan's admitted lease. Dropping it releases the retained capacity.
#[derive(Debug)]
pub struct RetainedArtifactPermit {
    lease_epoch: u64,
    permits: Vec<ResourcePermit>,
}

impl RetainedArtifactPermit {
    /// Return the lease epoch that admitted the artifact resources.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    /// Narrow this capability to the sealed artifact's exact storage bytes.
    pub(crate) fn narrow_temporary_storage(mut self, amount: u64) -> Result<Self, ResourceError> {
        if self.permits.len() != 1
            || !matches!(
                self.permits[0].resource(),
                LeaseResource::Storage {
                    use_kind: StorageUseKind::Temporary,
                    ..
                }
            )
        {
            return Err(ResourceError::Invalid(
                "artifact retention requires exactly one temporary-storage permit".to_string(),
            ));
        }
        self.permits[0].narrow_temporary_storage_to(amount)?;
        Ok(self)
    }

    /// Return whether this permit contains exactly one matching resource claim.
    pub(crate) fn covers_exact_temporary_storage(&self, amount: u64) -> bool {
        self.permits.len() == 1
            && matches!(
                self.permits[0].resource(),
                LeaseResource::Storage {
                    use_kind: StorageUseKind::Temporary,
                    ..
                }
            )
            && self.permits[0].amount() == amount
    }
}

/// One positive, typed lease claim made by a work node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceClaim {
    /// Exact resource declared by the selected lease alternative.
    pub resource: LeaseResource,
    /// Concurrent amount owned by this work node.
    pub amount: u64,
    /// Point through which the permit remains live.
    pub lifetime: ClaimLifetime,
}

/// Physical storage mode used by one reusable slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageMode {
    /// CPU-visible host storage.
    Host,
    /// Apple Silicon storage visible to both host and Metal views.
    MetalShared,
}

/// Initialization semantics required before allocation reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitializationPolicy {
    /// Existing bytes are authoritative and must be preserved.
    Preserve,
    /// The slot must be zeroed before the allocation is read.
    ZeroBeforeRead,
    /// The allocation overwrites every byte before any read.
    OverwriteBeforeRead,
}

/// Access contract for one allocation lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllocationAccess {
    /// The allocation is immutable after initialization.
    ReadOnly,
    /// Work may read and mutate the allocation.
    ReadWrite,
    /// Work writes the allocation without reading prior contents.
    WriteOnly,
}

/// Semantic purpose of one logical allocation independently of its reusable
/// physical storage compatibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllocationPurpose {
    /// Scientific, cache, workspace, or other non-I/O-buffer state.
    Data,
    /// A typed I/O staging buffer whose concurrent logical bytes are bounded
    /// by the matching [`crate::IoBufferDemand`] category.
    IoBuffer(crate::IoBufferKind),
}

/// Exact compatibility contract shared by a logical allocation and physical slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotCompatibility {
    /// Physical memory-capacity domain charged once for all views.
    pub memory_domain: CapacityDomainId,
    /// Host and Metal views permitted to access the bytes.
    pub views: BTreeSet<CapacityViewId>,
    /// Required byte alignment.
    pub alignment_bytes: u64,
    /// Host or Apple Silicon shared storage mode.
    pub storage_mode: StorageMode,
    /// Stable element, axis, stride, and tiling layout identity.
    pub layout: AllocationLayout,
    /// Initialization requirement before first read.
    pub initialization: InitializationPolicy,
    /// Read/write access requirement.
    pub access: AllocationAccess,
}

/// One logical allocation lifetime in the plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalAllocation {
    /// Stable logical allocation identity.
    pub id: AllocationId,
    /// Live byte count.
    pub bytes: u64,
    /// Typed semantic purpose; physical slots remain purpose-neutral so
    /// compatible disjoint lifetimes may reuse the same bytes.
    pub purpose: AllocationPurpose,
    /// Exact physical compatibility requirement.
    pub compatibility: SlotCompatibility,
    /// Sole physical slot assigned for this logical lifetime.
    pub physical_slot: PhysicalSlotId,
    /// Explicit acquisition and terminal release events.
    pub lifetime: AllocationLifetime,
}

/// Plan-owned liveness interval for one logical allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationLifetime {
    /// Node whose dispatch acquires the physical slot and memory permit.
    pub acquire_at: WorkNodeId,
    /// Work and fence events that must all complete before physical reuse.
    pub release_after: BTreeSet<WorkDependency>,
}

/// One reusable physical storage slot charged to a lease memory resource.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalSlot {
    /// Stable physical slot identity.
    pub id: PhysicalSlotId,
    /// Lease memory allocation that owns these physical bytes.
    pub lease_resource: LeaseResource,
    /// Physical byte capacity charged once even across temporal reuse.
    pub capacity_bytes: u64,
    /// Exact compatibility contract for every logical occupant.
    pub compatibility: SlotCompatibility,
}

/// One work node's use of a plan-owned logical allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationUse {
    /// Logical allocation referenced through the plan-owned slot assignment.
    pub allocation: AllocationId,
    /// Point through which this node may asynchronously access the allocation.
    pub lifetime: ClaimLifetime,
}

/// One complete immutable work declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkNode {
    /// Stable node identity.
    pub id: WorkNodeId,
    /// Explicit work class.
    pub kind: WorkKind,
    /// CPU, Metal, I/O, or resource-free execution domain.
    pub domain: WorkDomain,
    /// Selected implementation used by this node.
    pub implementation: WorkImplementationId,
    /// Typed predecessor conditions.
    pub dependencies: BTreeSet<WorkDependency>,
    /// Every worker, queue, transfer, cache, buffer, and allocation permit.
    pub claims: Vec<ResourceClaim>,
    /// Logical allocations used by this work.
    pub allocations: Vec<AllocationUse>,
    /// Asynchronous completion classes created when work returns.
    pub fences: BTreeSet<FenceKind>,
    /// Safe adaptation boundaries reached after this node and all its fences settle.
    pub quiescence_after: BTreeSet<QuiescencePoint>,
}

/// Execution-only controls that a pre-authorized transition may change.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExecutionKnobs {
    /// CPU worker count.
    pub workers: u64,
    /// Bounded input batch size.
    pub batch_size: u64,
    /// Execution tile width.
    pub tile_width: u64,
    /// Execution tile height.
    pub tile_height: u64,
    /// Cube slab depth.
    pub slab_depth: u64,
    /// Concurrent I/O depth.
    pub io_depth: u64,
    /// Retained resident-cache bytes.
    pub cache_retention_bytes: u64,
    /// Whether listed adjacent work may be fused.
    pub fusion: bool,
    /// Whether listed immutable artifacts may be recomputed.
    pub recomputation: bool,
    /// Whether declared spill nodes are enabled.
    pub spill: bool,
    /// Whether declared prefetch nodes are enabled.
    pub prefetch: bool,
}

impl ExecutionKnobs {
    /// Creates a conservative one-worker configuration for tests and planners.
    #[must_use]
    pub const fn serial() -> Self {
        Self {
            workers: 1,
            batch_size: 1,
            tile_width: 1,
            tile_height: 1,
            slab_depth: 1,
            io_depth: 1,
            cache_retention_bytes: 0,
            fusion: false,
            recomputation: false,
            spill: false,
            prefetch: false,
        }
    }
}

/// One execution-only transition authorized by the immutable plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdaptationTransition {
    /// Stable transition identity.
    pub id: AdaptationId,
    /// Required current execution configuration.
    pub from: ExecutionKnobs,
    /// Authorized next execution configuration.
    pub to: ExecutionKnobs,
    /// Required safe execution boundary.
    pub at: QuiescencePoint,
}

/// Declarative inputs consumed by [`ExecutionDag::new`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionDagSpecification {
    /// Resource capability predicates required by the compiled problem.
    pub required_resource_capabilities: BTreeSet<CapabilityId>,
    /// Sole selected, bounded implementation demand; execution never picks another.
    pub resource_alternative: DemandAlternative,
    /// Complete work DAG.
    pub nodes: Vec<WorkNode>,
    /// Complete logical allocation ledger.
    pub logical_allocations: Vec<LogicalAllocation>,
    /// Complete reusable physical-slot ledger.
    pub physical_slots: Vec<PhysicalSlot>,
    /// Initial execution-only configuration.
    pub initial_knobs: ExecutionKnobs,
    /// Complete pre-authorized adaptation graph.
    pub adaptations: Vec<AdaptationTransition>,
}

/// Complete immutable, problem-bound execution DAG.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionDag {
    physical_work_id: PhysicalWorkId,
    required_resource_capabilities: BTreeSet<CapabilityId>,
    resource_alternative: DemandAlternative,
    selected_implementations: BTreeSet<WorkImplementationId>,
    nodes: BTreeMap<WorkNodeId, WorkNode>,
    logical_allocations: BTreeMap<AllocationId, LogicalAllocation>,
    physical_slots: BTreeMap<PhysicalSlotId, PhysicalSlot>,
    initial_knobs: ExecutionKnobs,
    adaptations: BTreeMap<AdaptationId, AdaptationTransition>,
}

impl ExecutionDag {
    /// Validates and freezes a complete physical work DAG.
    pub fn new(mut specification: ExecutionDagSpecification) -> Result<Self, ExecutionError> {
        canonicalize_specification(&mut specification);
        validate_resource_alternative(&specification)?;
        let nodes = unique_map("work node", specification.nodes, |node| &node.id)?;
        let logical_allocations = unique_map(
            "logical allocation",
            specification.logical_allocations,
            |allocation| &allocation.id,
        )?;
        let physical_slots = unique_map("physical slot", specification.physical_slots, |slot| {
            &slot.id
        })?;
        let adaptations = unique_map("adaptation", specification.adaptations, |item| &item.id)?;
        validate_nodes(&nodes, &logical_allocations)?;
        validate_io_buffer_accounting(
            &nodes,
            &logical_allocations,
            specification.resource_alternative.demand.io_buffers,
        )?;
        let topological_order = validate_acyclic(&nodes)?;
        validate_retained_claims(&nodes)?;
        validate_allocations(
            &nodes,
            &logical_allocations,
            &physical_slots,
            &specification.resource_alternative,
        )?;
        validate_claims_against_demand(&nodes, &specification.resource_alternative.demand)?;
        validate_adaptations(
            &specification.initial_knobs,
            &adaptations,
            &specification.resource_alternative,
            &nodes,
            &topological_order,
        )?;
        let selected_implementations = nodes
            .values()
            .map(|node| node.implementation.clone())
            .collect();
        let mut plan = Self {
            physical_work_id: PhysicalWorkId::from_sha256([0; 32]),
            required_resource_capabilities: specification.required_resource_capabilities,
            resource_alternative: specification.resource_alternative,
            selected_implementations,
            nodes,
            logical_allocations,
            physical_slots,
            initial_knobs: specification.initial_knobs,
            adaptations,
        };
        plan.physical_work_id = canonical_physical_work_id(&plan);
        Ok(plan)
    }

    /// Returns the canonical identity of every physical work declaration.
    #[must_use]
    pub const fn physical_work_id(&self) -> PhysicalWorkId {
        self.physical_work_id
    }

    /// Returns resource capabilities that admission must satisfy.
    #[must_use]
    pub const fn required_resource_capabilities(&self) -> &BTreeSet<CapabilityId> {
        &self.required_resource_capabilities
    }

    /// Returns the sole selected bounded resource alternative.
    #[must_use]
    pub const fn resource_alternative(&self) -> &DemandAlternative {
        &self.resource_alternative
    }

    /// Returns all selected implementation identities.
    #[must_use]
    pub const fn selected_implementations(&self) -> &BTreeSet<WorkImplementationId> {
        &self.selected_implementations
    }

    /// Returns the deterministic node map.
    #[must_use]
    pub const fn nodes(&self) -> &BTreeMap<WorkNodeId, WorkNode> {
        &self.nodes
    }

    /// Returns the logical allocation ledger.
    #[must_use]
    pub const fn logical_allocations(&self) -> &BTreeMap<AllocationId, LogicalAllocation> {
        &self.logical_allocations
    }

    /// Returns the reusable physical-slot ledger.
    #[must_use]
    pub const fn physical_slots(&self) -> &BTreeMap<PhysicalSlotId, PhysicalSlot> {
        &self.physical_slots
    }

    /// Returns the initial execution-only configuration.
    #[must_use]
    pub const fn initial_knobs(&self) -> &ExecutionKnobs {
        &self.initial_knobs
    }

    /// Returns every pre-authorized execution-only transition.
    #[must_use]
    pub const fn adaptations(&self) -> &BTreeMap<AdaptationId, AdaptationTransition> {
        &self.adaptations
    }

    pub(crate) fn fixed_worker_variant(
        &self,
        workers: u64,
        thread_stack_bytes: u64,
    ) -> Result<Self, ExecutionError> {
        let scaling = &self.resource_alternative.scaling;
        if workers < scaling.minimum_workers
            || workers > scaling.maximum_workers
            || scaling.minimum_workers == scaling.maximum_workers
            || !self.adaptations.is_empty()
            || self.initial_knobs.workers != scaling.maximum_workers
            || self.resource_alternative.demand.workers
                != CountDemand::new(scaling.maximum_workers, scaling.maximum_workers)
        {
            return Err(ExecutionError::invalid_plan(
                "worker-scalable template is inconsistent with its sealed scaling range",
            ));
        }
        let template_workers = scaling.maximum_workers;
        let template_stack_bytes = self.resource_alternative.demand.overhead.thread_stack_bytes;
        let mut worker_claim_updated = false;
        let mut stack_claim_updated = template_stack_bytes == 0;
        let mut nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        for node in &mut nodes {
            let mut claims = Vec::with_capacity(node.claims.len());
            for mut claim in std::mem::take(&mut node.claims) {
                match claim.resource {
                    LeaseResource::Workers if claim.amount == template_workers => {
                        claim.amount = workers;
                        worker_claim_updated = true;
                        claims.push(claim);
                    }
                    LeaseResource::RuntimeOverhead(RuntimeOverheadKind::ThreadStack)
                        if claim.amount == template_stack_bytes =>
                    {
                        stack_claim_updated = true;
                        if thread_stack_bytes > 0 {
                            claim.amount = thread_stack_bytes;
                            claims.push(claim);
                        }
                    }
                    _ => claims.push(claim),
                }
            }
            node.claims = claims;
        }
        if !worker_claim_updated || !stack_claim_updated {
            return Err(ExecutionError::invalid_plan(
                "worker-scalable template lacks its exact worker or stack claim",
            ));
        }

        let mut alternative = self.resource_alternative.clone();
        alternative.id =
            AlternativeId::new(format!("{}-workers-{workers}", alternative.id.as_str()));
        alternative.demand.workers = CountDemand::new(workers, workers);
        alternative.demand.overhead.thread_stack_bytes = thread_stack_bytes;
        alternative.scaling.minimum_workers = workers;
        alternative.scaling.maximum_workers = workers;
        let mut initial_knobs = self.initial_knobs.clone();
        initial_knobs.workers = workers;
        Self::new(ExecutionDagSpecification {
            required_resource_capabilities: self.required_resource_capabilities.clone(),
            resource_alternative: alternative,
            nodes,
            logical_allocations: self.logical_allocations.values().cloned().collect(),
            physical_slots: self.physical_slots.values().cloned().collect(),
            initial_knobs,
            adaptations: Vec::new(),
        })
    }
}

fn canonicalize_specification(specification: &mut ExecutionDagSpecification) {
    for node in &mut specification.nodes {
        node.claims.sort_unstable_by(|left, right| {
            (&left.resource, &left.lifetime).cmp(&(&right.resource, &right.lifetime))
        });
        node.allocations.sort_unstable_by(|left, right| {
            (&left.allocation, &left.lifetime).cmp(&(&right.allocation, &right.lifetime))
        });
    }
    let demand = &mut specification.resource_alternative.demand;
    for memory in &mut demand.memory {
        memory.views.sort_unstable();
        memory.views.dedup();
    }
    demand
        .memory
        .sort_unstable_by(|left, right| left.allocation_id.cmp(&right.allocation_id));
    demand
        .storage
        .sort_unstable_by(|left, right| left.demand_id.cmp(&right.demand_id));
    demand
        .rates
        .sort_unstable_by(|left, right| left.demand_id.cmp(&right.demand_id));
    demand
        .queues
        .sort_unstable_by(|left, right| left.demand_id.cmp(&right.demand_id));
    demand
        .transfers
        .sort_unstable_by(|left, right| left.demand_id.cmp(&right.demand_id));
    demand
        .accelerators
        .sort_unstable_by(|left, right| left.demand_id.cmp(&right.demand_id));
}

/// Result reported when one dispatched node's synchronous work returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WorkResult {
    /// The node's synchronous work completed successfully.
    Succeeded,
    /// The node failed; pending work is cancelled while launched fences drain.
    Failed {
        /// Stable diagnostic retained for the terminal outcome.
        message: String,
    },
}

/// Successful or cancelled result returned by the sole public execution seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionOutcome {
    /// Every planned node and asynchronous fence completed.
    Succeeded,
    /// Cancellation prevented all remaining work from starting.
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SchedulerTerminal {
    /// Every planned node completed.
    Succeeded,
    /// Ordinary pending work was cancelled after required cleanup.
    Cancelled,
    /// One dispatched node failed and the scheduler drained launched work.
    Failed {
        /// Work node that failed.
        node: WorkNodeId,
        /// Stable failure diagnostic.
        message: String,
    },
}

/// Scheduler-issued proof of one lease-attributed resource claim.
#[derive(Debug, PartialEq, Eq)]
pub struct WorkResourceCapability {
    resource: LeaseResource,
    amount: u64,
    lifetime: ClaimLifetime,
}

impl WorkResourceCapability {
    /// Return the exact lease resource owned for this adapter call.
    #[must_use]
    pub const fn resource(&self) -> &LeaseResource {
        &self.resource
    }

    /// Return the maximum concurrent amount owned by this capability.
    #[must_use]
    pub const fn amount(&self) -> u64 {
        self.amount
    }

    /// Return the point through which the capability remains live.
    #[must_use]
    pub const fn lifetime(&self) -> &ClaimLifetime {
        &self.lifetime
    }
}

/// Scheduler-issued proof of one plan-owned allocation and physical slot.
#[derive(Debug, PartialEq, Eq)]
pub struct WorkAllocationCapability {
    allocation: AllocationId,
    physical_slot: PhysicalSlotId,
    capacity_bytes: u64,
    lifetime: ClaimLifetime,
}

impl WorkAllocationCapability {
    /// Return the plan-owned logical allocation generation.
    #[must_use]
    pub const fn allocation(&self) -> &AllocationId {
        &self.allocation
    }

    /// Return the exact reusable physical slot currently owned by the allocation.
    #[must_use]
    pub const fn physical_slot(&self) -> &PhysicalSlotId {
        &self.physical_slot
    }

    /// Return the slot's hard byte capacity.
    #[must_use]
    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Return the point through which this adapter call retains the allocation.
    #[must_use]
    pub const fn lifetime(&self) -> &ClaimLifetime {
        &self.lifetime
    }
}

/// Non-constructible, lease-scoped execution context for one exact plan node.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct WorkExecutionContext {
    node: WorkNode,
    knobs: ExecutionKnobs,
    lease_epoch: u64,
    cleanup: bool,
    resources: Vec<WorkResourceCapability>,
    allocations: Vec<WorkAllocationCapability>,
    metal_runtime_claimed: Cell<bool>,
}

impl WorkExecutionContext {
    /// Returns the exact planned work declaration.
    #[must_use]
    pub(crate) const fn node(&self) -> &WorkNode {
        &self.node
    }

    /// Returns the current pre-authorized execution configuration.
    #[must_use]
    pub(crate) const fn knobs(&self) -> &ExecutionKnobs {
        &self.knobs
    }

    /// Return the Resource Authority lease epoch that issued these capabilities.
    #[must_use]
    pub(crate) const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    /// Return whether this Release node was dispatched while draining a failed run.
    #[must_use]
    pub(crate) const fn is_cleanup(&self) -> bool {
        self.cleanup
    }

    /// Return only the scheduler-issued resource capabilities for this call.
    #[must_use]
    pub(crate) fn resources(&self) -> &[WorkResourceCapability] {
        &self.resources
    }

    /// Return only the scheduler-issued allocation capabilities for this call.
    #[must_use]
    pub(crate) fn allocations(&self) -> &[WorkAllocationCapability] {
        &self.allocations
    }

    pub(crate) fn claim_metal_runtime(&self) -> bool {
        !self.metal_runtime_claimed.replace(true)
    }

    pub(crate) fn for_fence(&self, kind: FenceKind) -> Self {
        Self {
            node: self.node.clone(),
            knobs: self.knobs.clone(),
            lease_epoch: self.lease_epoch,
            cleanup: self.cleanup,
            resources: self
                .resources
                .iter()
                .filter(|capability| capability.lifetime.retains_fence(kind))
                .map(|capability| WorkResourceCapability {
                    resource: capability.resource.clone(),
                    amount: capability.amount,
                    lifetime: capability.lifetime.clone(),
                })
                .collect(),
            allocations: self
                .allocations
                .iter()
                .filter(|capability| capability.lifetime.retains_fence(kind))
                .map(|capability| WorkAllocationCapability {
                    allocation: capability.allocation.clone(),
                    physical_slot: capability.physical_slot.clone(),
                    capacity_bytes: capability.capacity_bytes,
                    lifetime: capability.lifetime.clone(),
                })
                .collect(),
            metal_runtime_claimed: Cell::new(false),
        }
    }
}

/// One deterministic scheduler decision.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SchedulerAction {
    /// Execute this exact plan-owned node.
    Work(Box<WorkExecutionContext>),
    /// No additional node currently fits; launched work or fences can unblock it.
    Waiting {
        /// Synchronously executing nodes.
        running_work: usize,
        /// Asynchronous device, I/O, writeback, or publication fences.
        pending_fences: usize,
    },
    /// Every fallible scheduler transition has settled while the terminal
    /// publication node still owns its transaction authority and allocations.
    PublicationReady {
        node: WorkNodeId,
        resources: PublicationReservation,
    },
    /// The lease has been released and execution is terminal.
    Complete(SchedulerTerminal),
}

/// Exact scheduler-owned resources retained for the terminal publish call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PublicationReservation {
    pub(crate) lease_epoch: u64,
    pub(crate) allocations: BTreeMap<AllocationId, PhysicalSlotId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeState {
    Pending,
    CleanupPending,
    CleanupFailed,
    Running,
    WorkComplete,
    Settled,
    Cancelled,
}

#[derive(Debug)]
struct HeldPermit {
    lifetime: ClaimLifetime,
    permit: ResourcePermit,
}

#[derive(Debug)]
struct ActiveWork {
    permits: Vec<HeldPermit>,
    fences: BTreeMap<FenceId, ResourceFence>,
}

#[derive(Debug)]
struct DeferredPermit {
    remaining: BTreeSet<FenceId>,
    permit: ResourcePermit,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RetainedPermitId {
    resource: LeaseResource,
    release: WorkNodeId,
}

#[derive(Debug)]
struct RetainedPermit {
    amount: u64,
    permit: ResourcePermit,
}

pub(crate) struct ObservationCompletionPermitGuard {
    permits: Vec<ResourcePermit>,
}

impl ObservationCompletionPermitGuard {
    pub(crate) fn release(self) -> Result<(), ExecutionError> {
        for permit in self.permits {
            permit.release()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ActiveAllocation {
    slot: PhysicalSlotId,
    remaining: BTreeSet<WorkDependency>,
    permit: ResourcePermit,
}

/// Deterministic, fail-closed executor for one immutable plan-owned work DAG.
#[derive(Debug)]
pub(crate) struct ExecutionScheduler<'plan> {
    dag: &'plan ExecutionDag,
    lease: Option<ResourceLease>,
    states: BTreeMap<WorkNodeId, NodeState>,
    running: BTreeMap<WorkNodeId, ActiveWork>,
    outstanding_fences: BTreeMap<FenceId, ResourceFence>,
    completed_fences: BTreeSet<FenceId>,
    failed_cleanup_fences: BTreeSet<FenceId>,
    deferred_permits: Vec<DeferredPermit>,
    retained_permits: BTreeMap<RetainedPermitId, RetainedPermit>,
    artifact_permits: BTreeMap<WorkNodeId, Vec<ResourcePermit>>,
    transferred_artifact_permits: bool,
    observation_completion_permits: BTreeMap<WorkNodeId, Vec<ResourcePermit>>,
    terminal_publication: Option<WorkNodeId>,
    publication_permits: Vec<ResourcePermit>,
    publication_allocations: BTreeSet<AllocationId>,
    active_allocations: BTreeMap<AllocationId, ActiveAllocation>,
    active_slots: BTreeMap<PhysicalSlotId, AllocationId>,
    quarantined_allocations: BTreeSet<AllocationId>,
    knobs: ExecutionKnobs,
    available_quiescence: BTreeSet<QuiescencePoint>,
    applied_adaptations: Vec<AdaptationId>,
    draining: Option<SchedulerTerminal>,
    terminal: Option<SchedulerTerminal>,
}

impl<'plan> ExecutionScheduler<'plan> {
    /// Validate the bound DAG against topology, admit its sole resource
    /// alternative, and create a scheduler without executing work.
    pub(crate) fn start(
        dag: &'plan ExecutionDag,
        resource_policy: &crate::ResourcePolicy,
        authority: &ResourceAuthority,
        terminal_publication: Option<&WorkNodeId>,
    ) -> Result<Self, ExecutionError> {
        if let Some(publication) = terminal_publication {
            let node = dag.nodes.get(publication).ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "terminal publication node {} is absent from the execution DAG",
                    publication.as_str()
                ))
            })?;
            if node.kind != WorkKind::Publication {
                return Err(ExecutionError::invalid_plan(format!(
                    "terminal publication node {} is not Publication work",
                    publication.as_str()
                )));
            }
        }
        validate_topology(dag, authority.topology())?;
        let lease = authority.acquire(
            resource_policy.clone(),
            DemandAlternatives {
                required_capabilities: dag.required_resource_capabilities.clone(),
                alternatives: vec![dag.resource_alternative.clone()],
            },
        )?;
        if lease.selected_alternative() != &dag.resource_alternative().id {
            return Err(ExecutionError::invalid_state(
                "Resource Authority admitted an unplanned alternative",
            ));
        }
        validate_lease_claims(dag, &lease)?;
        let states = dag
            .nodes
            .keys()
            .cloned()
            .map(|id| (id, NodeState::Pending))
            .collect();
        let available_quiescence = if dag
            .resource_alternative
            .quiescence_points
            .contains(&QuiescencePoint::RunBoundary)
        {
            BTreeSet::from([QuiescencePoint::RunBoundary])
        } else {
            BTreeSet::new()
        };
        Ok(Self {
            dag,
            lease: Some(lease),
            states,
            running: BTreeMap::new(),
            outstanding_fences: BTreeMap::new(),
            completed_fences: BTreeSet::new(),
            failed_cleanup_fences: BTreeSet::new(),
            deferred_permits: Vec::new(),
            retained_permits: BTreeMap::new(),
            artifact_permits: BTreeMap::new(),
            transferred_artifact_permits: false,
            observation_completion_permits: BTreeMap::new(),
            terminal_publication: terminal_publication.cloned(),
            publication_permits: Vec::new(),
            publication_allocations: BTreeSet::new(),
            active_allocations: BTreeMap::new(),
            active_slots: BTreeMap::new(),
            quarantined_allocations: BTreeSet::new(),
            knobs: dag.initial_knobs.clone(),
            available_quiescence,
            applied_adaptations: Vec::new(),
            draining: None,
            terminal: None,
        })
    }

    /// Returns the admitted Resource Authority epoch.
    #[must_use]
    pub(crate) fn lease_epoch(&self) -> Option<u64> {
        self.lease.as_ref().map(ResourceLease::epoch)
    }

    /// Reports whether host pressure changed since admission, or `None` after
    /// the execution lease has been released.
    pub(crate) fn pressure_changed(&self) -> Result<Option<bool>, ExecutionError> {
        match &self.lease {
            Some(lease) => lease.pressure_changed().map(Some).map_err(Into::into),
            None => Ok(None),
        }
    }

    /// Returns the effective execution-only configuration.
    #[must_use]
    pub(crate) const fn knobs(&self) -> &ExecutionKnobs {
        &self.knobs
    }

    /// Returns the exact adaptation transitions applied so far.
    #[must_use]
    pub(crate) fn applied_adaptations(&self) -> &[AdaptationId] {
        &self.applied_adaptations
    }

    pub(crate) fn eligible_adaptations(&self) -> Vec<AdaptationTransition> {
        if self.draining.is_some()
            || self.terminal.is_some()
            || !self.running.is_empty()
            || !self.outstanding_fences.is_empty()
            || !self.deferred_permits.is_empty()
            || !self
                .states
                .values()
                .any(|state| *state == NodeState::Pending)
        {
            return Vec::new();
        }
        self.dag
            .adaptations
            .values()
            .filter(|transition| {
                transition.from == self.knobs && self.available_quiescence.contains(&transition.at)
            })
            .cloned()
            .collect()
    }

    /// Deterministically dispatch one ready node, wait for launched work, or
    /// release the lease and report terminal status.
    pub(crate) fn next_action(&mut self) -> Result<SchedulerAction, ExecutionError> {
        if let Some(outcome) = &self.terminal {
            return Ok(SchedulerAction::Complete(outcome.clone()));
        }
        if self.draining.is_none() {
            let ready = self
                .states
                .iter()
                .filter_map(|(id, state)| {
                    (*state == NodeState::Pending
                        && self.dependencies_satisfied(&self.dag.nodes[id]))
                    .then_some(id.clone())
                })
                .collect::<Vec<_>>();
            for id in ready {
                if self.node_allocations_available(&self.dag.nodes[&id])
                    && let Some(work) = self.try_dispatch(&id)?
                {
                    return Ok(SchedulerAction::Work(Box::new(work)));
                }
            }
        }
        if !self.running.is_empty() || !self.outstanding_fences.is_empty() {
            return Ok(SchedulerAction::Waiting {
                running_work: self.running.len(),
                pending_fences: self.outstanding_fences.len(),
            });
        }
        if !self.deferred_permits.is_empty() {
            return Err(ExecutionError::invalid_state(
                "deferred resources remain without their declared fences",
            ));
        }
        if self.draining.is_some() {
            let cleanup = self
                .states
                .iter()
                .filter_map(|(id, state)| {
                    (*state == NodeState::CleanupPending
                        && self.cleanup_dependencies_satisfied(&self.dag.nodes[id]))
                    .then_some(id.clone())
                })
                .collect::<Vec<_>>();
            for id in cleanup {
                if self.node_allocations_available(&self.dag.nodes[&id])
                    && let Some(work) = self.try_dispatch(&id)?
                {
                    return Ok(SchedulerAction::Work(Box::new(work)));
                }
            }
            if self
                .states
                .values()
                .any(|state| *state == NodeState::CleanupPending)
            {
                return Err(ExecutionError::Deadlock);
            }
        }
        if let Some(outcome) = self.draining.take() {
            return self.finish_draining(outcome);
        }
        if self
            .states
            .values()
            .all(|state| *state == NodeState::Settled)
        {
            if let Some(publication) = &self.terminal_publication {
                let expected_allocations = self.dag.nodes[publication]
                    .allocations
                    .iter()
                    .map(|allocation_use| allocation_use.allocation.clone())
                    .collect::<BTreeSet<_>>();
                if self.publication_allocations != expected_allocations
                    || self
                        .active_allocations
                        .keys()
                        .cloned()
                        .collect::<BTreeSet<_>>()
                        != expected_allocations
                {
                    return Err(ExecutionError::invalid_state(
                        "terminal publication does not retain its exact logical allocations",
                    ));
                }
                if self.publication_permits.is_empty() {
                    return Err(ExecutionError::invalid_state(
                        "terminal publication lost its transaction resource authority",
                    ));
                }
                let lease_epoch = self.lease_epoch().ok_or_else(|| {
                    ExecutionError::invalid_state(
                        "terminal publication lost its Resource Authority lease",
                    )
                })?;
                let allocations = expected_allocations
                    .into_iter()
                    .map(|allocation| {
                        let slot = self.active_allocations[&allocation].slot.clone();
                        (allocation, slot)
                    })
                    .collect();
                return Ok(SchedulerAction::PublicationReady {
                    node: publication.clone(),
                    resources: PublicationReservation {
                        lease_epoch,
                        allocations,
                    },
                });
            }
            if !self.active_allocations.is_empty() {
                return Err(ExecutionError::invalid_state(
                    "completed work retained a logical allocation beyond its terminal events",
                ));
            }
            if !self.retained_permits.is_empty() {
                return Err(ExecutionError::invalid_state(
                    "completed work retained a resource beyond its explicit release node",
                ));
            }
            if !self.artifact_permits.is_empty() {
                return Err(ExecutionError::invalid_state(
                    "completed work retained an artifact permit that was not transferred",
                ));
            }
            if !self.observation_completion_permits.is_empty() {
                return Err(ExecutionError::invalid_state(
                    "completed observation retained its terminal MeasurementSet permit",
                ));
            }
            return self.finish(SchedulerTerminal::Succeeded);
        }
        Err(ExecutionError::Deadlock)
    }

    /// Settle one dispatched synchronous work call and return every fence that
    /// the executor must complete after the corresponding asynchronous work.
    pub(crate) fn finish_work(
        &mut self,
        node_id: WorkNodeId,
        result: WorkResult,
    ) -> Result<BTreeSet<FenceId>, ExecutionError> {
        let active = self.running.remove(&node_id).ok_or_else(|| {
            ExecutionError::invalid_state(format!("work node {} is not running", node_id.as_str()))
        })?;
        self.states.insert(node_id.clone(), NodeState::WorkComplete);
        let terminal_publication = self.terminal_publication.as_ref() == Some(&node_id);
        let observation_read = self.dag.nodes[&node_id].kind.reads_observation();
        let succeeded = matches!(result, WorkResult::Succeeded);
        for held in active.permits {
            if terminal_publication {
                self.publication_permits.push(held.permit);
                continue;
            }
            match held.lifetime {
                ClaimLifetime::Work => {
                    if observation_read
                        && matches!(
                            held.permit.resource(),
                            LeaseResource::MeasurementSetLock { .. }
                        )
                    {
                        self.observation_completion_permits
                            .entry(node_id.clone())
                            .or_default()
                            .push(held.permit);
                    } else {
                        held.permit.release()?;
                    }
                }
                ClaimLifetime::Fences(kinds) => self.deferred_permits.push(DeferredPermit {
                    remaining: fence_ids(&node_id, &kinds),
                    permit: held.permit,
                }),
                ClaimLifetime::RetainedUntil(release) => {
                    if observation_read
                        && release == node_id
                        && matches!(
                            held.permit.resource(),
                            LeaseResource::MeasurementSetLock { .. }
                        )
                    {
                        self.observation_completion_permits
                            .entry(node_id.clone())
                            .or_default()
                            .push(held.permit);
                        continue;
                    }
                    let id = RetainedPermitId {
                        resource: held.permit.resource().clone(),
                        release,
                    };
                    let retained = RetainedPermit {
                        amount: held.permit.amount(),
                        permit: held.permit,
                    };
                    if self.retained_permits.insert(id, retained).is_some() {
                        return Err(ExecutionError::invalid_state(
                            "retained resource permit was acquired more than once",
                        ));
                    }
                }
                ClaimLifetime::Artifact => {
                    if succeeded {
                        self.artifact_permits
                            .entry(node_id.clone())
                            .or_default()
                            .push(held.permit);
                    } else {
                        held.permit.release()?;
                    }
                }
            }
        }
        let fences = active.fences.keys().cloned().collect::<BTreeSet<_>>();
        self.outstanding_fences.extend(active.fences);
        self.complete_allocation_event(&WorkDependency::Work(node_id.clone()))?;
        self.complete_retained_event(&node_id)?;
        if let WorkResult::Failed { message } = result {
            self.begin_draining(SchedulerTerminal::Failed {
                node: node_id.clone(),
                message,
            });
        }
        if !self
            .outstanding_fences
            .keys()
            .any(|fence| fence.node() == &node_id)
        {
            self.settle_node(&node_id)?;
        }
        Ok(fences)
    }

    /// Transfer this settled node's artifact-retained permits to its output.
    pub(crate) fn take_artifact_permit(
        &mut self,
        node_id: &WorkNodeId,
    ) -> Result<Option<RetainedArtifactPermit>, ExecutionError> {
        if self.states.get(node_id) != Some(&NodeState::Settled) {
            return Ok(None);
        }
        let Some(permits) = self.artifact_permits.remove(node_id) else {
            return Ok(None);
        };
        self.transferred_artifact_permits = true;
        let lease_epoch = self.lease_epoch().ok_or_else(|| {
            ExecutionError::invalid_state("artifact permit lost its Resource Authority lease")
        })?;
        Ok(Some(RetainedArtifactPermit {
            lease_epoch,
            permits,
        }))
    }

    /// Complete one exact device, I/O, writeback, or publication fence and
    /// release every permit and physical slot whose full fence set settled.
    pub(crate) fn complete_fence(&mut self, fence_id: FenceId) -> Result<(), ExecutionError> {
        let fence = self.outstanding_fences.remove(&fence_id).ok_or_else(|| {
            ExecutionError::invalid_state(format!(
                "fence {:?} for node {} is not pending",
                fence_id.kind(),
                fence_id.node().as_str()
            ))
        })?;
        fence.complete()?;
        self.completed_fences.insert(fence_id.clone());
        let mut retained_permits = Vec::new();
        let producer = fence_id.node().clone();
        let observation_read = self.dag.nodes[&producer].kind.reads_observation();
        for mut held in std::mem::take(&mut self.deferred_permits) {
            held.remaining.remove(&fence_id);
            if held.remaining.is_empty() {
                if observation_read
                    && matches!(
                        held.permit.resource(),
                        LeaseResource::MeasurementSetLock { .. }
                    )
                {
                    self.observation_completion_permits
                        .entry(producer.clone())
                        .or_default()
                        .push(held.permit);
                } else {
                    held.permit.release()?;
                }
            } else {
                retained_permits.push(held);
            }
        }
        self.deferred_permits = retained_permits;
        self.complete_allocation_event(&WorkDependency::Fence(fence_id.clone()))?;
        if !self
            .outstanding_fences
            .keys()
            .any(|candidate| candidate.node() == &producer)
        {
            match self.states.get(&producer) {
                Some(NodeState::CleanupFailed) => {}
                Some(NodeState::WorkComplete) => self.settle_node(&producer)?,
                state => {
                    return Err(ExecutionError::invalid_state(format!(
                        "fence producer {} cannot settle from state {state:?}",
                        producer.as_str()
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn take_observation_completion_permits(
        &mut self,
        node: &WorkNodeId,
    ) -> ObservationCompletionPermitGuard {
        ObservationCompletionPermitGuard {
            permits: self
                .observation_completion_permits
                .remove(node)
                .unwrap_or_default(),
        }
    }

    pub(crate) fn next_pending_fence(&self) -> Option<FenceId> {
        self.outstanding_fences.keys().next().cloned()
    }

    pub(crate) fn fail_fence(
        &mut self,
        fence_id: FenceId,
        message: String,
    ) -> Result<(), ExecutionError> {
        let node = fence_id.node().clone();
        self.complete_fence(fence_id)?;
        self.begin_draining(SchedulerTerminal::Failed { node, message });
        Ok(())
    }

    pub(crate) fn fail_release_work(&mut self, node_id: &WorkNodeId) -> Result<(), ExecutionError> {
        if self.dag.nodes[node_id].kind != WorkKind::Release {
            return Err(ExecutionError::invalid_state(format!(
                "work node {} is not external-release cleanup",
                node_id.as_str()
            )));
        }
        let active = self.running.remove(node_id).ok_or_else(|| {
            ExecutionError::invalid_state(format!(
                "release work node {} is not running",
                node_id.as_str()
            ))
        })?;
        for held in active.permits {
            held.permit.release()?;
        }
        for (fence_id, fence) in active.fences {
            fence.complete()?;
            self.failed_cleanup_fences.insert(fence_id);
        }
        self.mark_release_allocation_quarantined(node_id)?;
        self.states
            .insert(node_id.clone(), NodeState::CleanupFailed);
        Ok(())
    }

    pub(crate) fn fail_release_fence(&mut self, fence_id: FenceId) -> Result<(), ExecutionError> {
        let node_id = fence_id.node().clone();
        if self.dag.nodes[&node_id].kind != WorkKind::Release {
            return Err(ExecutionError::invalid_state(format!(
                "fence producer {} is not external-release cleanup",
                node_id.as_str()
            )));
        }
        let fence = self.outstanding_fences.remove(&fence_id).ok_or_else(|| {
            ExecutionError::invalid_state(format!(
                "release fence {:?} for node {} is not pending",
                fence_id.kind(),
                node_id.as_str()
            ))
        })?;
        fence.complete()?;
        self.failed_cleanup_fences.insert(fence_id.clone());
        let mut retained_permits = Vec::new();
        for mut held in std::mem::take(&mut self.deferred_permits) {
            held.remaining.remove(&fence_id);
            if held.remaining.is_empty() {
                held.permit.release()?;
            } else {
                retained_permits.push(held);
            }
        }
        self.deferred_permits = retained_permits;
        self.mark_release_allocation_quarantined(&node_id)?;
        self.states.insert(node_id, NodeState::CleanupFailed);
        Ok(())
    }

    /// Prevent pending nodes from starting and drain work already dispatched.
    pub(crate) fn cancel(&mut self) -> Result<(), ExecutionError> {
        if self.terminal.is_some() {
            return Err(ExecutionError::invalid_state(
                "completed execution cannot be cancelled",
            ));
        }
        self.begin_draining(SchedulerTerminal::Cancelled);
        Ok(())
    }

    /// Apply one exact execution-only transition at a globally idle declared
    /// quiescence point. No scientific or numerical field exists in this API.
    pub(crate) fn adapt(&mut self, adaptation: &AdaptationId) -> Result<(), ExecutionError> {
        if self.draining.is_some() || self.terminal.is_some() {
            return Err(ExecutionError::invalid_state(
                "terminal or cancelling execution cannot adapt",
            ));
        }
        if !self.running.is_empty()
            || !self.outstanding_fences.is_empty()
            || !self.deferred_permits.is_empty()
        {
            return Err(ExecutionError::invalid_state(
                "adaptation requires globally quiescent execution",
            ));
        }
        let transition = self.dag.adaptations.get(adaptation).ok_or_else(|| {
            ExecutionError::invalid_state(format!(
                "adaptation {} is absent from the immutable plan",
                adaptation.as_str()
            ))
        })?;
        if transition.from != self.knobs || !self.available_quiescence.contains(&transition.at) {
            return Err(ExecutionError::invalid_state(format!(
                "adaptation {} is not authorized from the current configuration and boundary",
                adaptation.as_str()
            )));
        }
        self.knobs = transition.to.clone();
        self.applied_adaptations.push(adaptation.clone());
        self.available_quiescence.clear();
        Ok(())
    }

    fn dependencies_satisfied(&self, node: &WorkNode) -> bool {
        node.dependencies.iter().all(|dependency| match dependency {
            WorkDependency::Work(predecessor) => matches!(
                self.states.get(predecessor),
                Some(NodeState::WorkComplete | NodeState::Settled)
            ),
            WorkDependency::Fence(fence) => self.completed_fences.contains(fence),
        })
    }

    fn cleanup_dependencies_satisfied(&self, node: &WorkNode) -> bool {
        node.dependencies.iter().all(|dependency| match dependency {
            WorkDependency::Work(predecessor) => {
                let predecessor_node = &self.dag.nodes[predecessor];
                if predecessor_node.kind == WorkKind::Release {
                    matches!(
                        self.states.get(predecessor),
                        Some(NodeState::Settled | NodeState::CleanupFailed | NodeState::Cancelled)
                    )
                } else {
                    matches!(
                        self.states.get(predecessor),
                        Some(NodeState::WorkComplete | NodeState::Settled | NodeState::Cancelled)
                    )
                }
            }
            WorkDependency::Fence(fence) => {
                if self.dag.nodes[fence.node()].kind == WorkKind::Release {
                    self.completed_fences.contains(fence)
                        || self.failed_cleanup_fences.contains(fence)
                        || self.states.get(fence.node()) == Some(&NodeState::Cancelled)
                } else {
                    self.completed_fences.contains(fence)
                        || self.states.get(fence.node()) == Some(&NodeState::Cancelled)
                }
            }
        })
    }

    fn node_allocations_available(&self, node: &WorkNode) -> bool {
        node.allocations.iter().all(|allocation_use| {
            let allocation = &self.dag.logical_allocations[&allocation_use.allocation];
            match self.active_allocations.get(&allocation_use.allocation) {
                Some(active) => active.slot == allocation.physical_slot,
                None => {
                    allocation.lifetime.acquire_at == node.id
                        && !self.active_slots.contains_key(&allocation.physical_slot)
                }
            }
        })
    }

    fn mark_release_allocation_quarantined(
        &mut self,
        node_id: &WorkNodeId,
    ) -> Result<(), ExecutionError> {
        let allocation_uses = &self.dag.nodes[node_id].allocations;
        if allocation_uses.is_empty() {
            return Err(ExecutionError::invalid_state(format!(
                "external-release node {} does not own an allocation",
                node_id.as_str()
            )));
        }
        for allocation_use in allocation_uses {
            if !self
                .active_allocations
                .contains_key(&allocation_use.allocation)
            {
                return Err(ExecutionError::invalid_state(format!(
                    "failed external-release node {} has no active allocation {}",
                    node_id.as_str(),
                    allocation_use.allocation.as_str()
                )));
            }
        }
        self.quarantined_allocations.extend(
            allocation_uses
                .iter()
                .map(|allocation_use| allocation_use.allocation.clone()),
        );
        Ok(())
    }

    fn try_dispatch(
        &mut self,
        node_id: &WorkNodeId,
    ) -> Result<Option<WorkExecutionContext>, ExecutionError> {
        let node = self.dag.nodes[node_id].clone();
        let cleanup = self.states.get(node_id) == Some(&NodeState::CleanupPending);
        if !self.within_execution_limits(&node)? {
            return Ok(None);
        }
        let lease = self
            .lease
            .as_ref()
            .ok_or_else(|| ExecutionError::invalid_state("execution lease was released"))?;
        let lease_epoch = lease.epoch();
        let mut claims = node.claims.iter().collect::<Vec<_>>();
        claims.sort_unstable_by(|left, right| {
            (&left.resource, &left.lifetime).cmp(&(&right.resource, &right.lifetime))
        });
        let mut permits = Vec::with_capacity(claims.len());
        for claim in &claims {
            if let ClaimLifetime::RetainedUntil(release) = &claim.lifetime {
                let id = RetainedPermitId {
                    resource: claim.resource.clone(),
                    release: release.clone(),
                };
                if let Some(retained) = self.retained_permits.get(&id) {
                    if retained.amount != claim.amount {
                        return Err(ExecutionError::invalid_state(format!(
                            "retained resource {:?} changed amount before release {}",
                            claim.resource,
                            release.as_str()
                        )));
                    }
                    continue;
                }
            }
            match lease.permit(claim.resource.clone(), claim.amount) {
                Ok(permit) => permits.push(HeldPermit {
                    lifetime: claim.lifetime.clone(),
                    permit,
                }),
                Err(
                    ResourceError::LeaseLimitExceeded { .. }
                    | ResourceError::MeasurementSetLockUnavailable { .. },
                ) => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
        let mut allocations = Vec::new();
        for allocation_use in &node.allocations {
            if self
                .active_allocations
                .contains_key(&allocation_use.allocation)
            {
                continue;
            }
            let allocation = &self.dag.logical_allocations[&allocation_use.allocation];
            let slot = &self.dag.physical_slots[&allocation.physical_slot];
            match lease.permit(slot.lease_resource.clone(), slot.capacity_bytes) {
                Ok(permit) => allocations.push((allocation.clone(), permit)),
                Err(
                    ResourceError::LeaseLimitExceeded { .. }
                    | ResourceError::MeasurementSetLockUnavailable { .. },
                ) => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
        let mut fences = BTreeMap::new();
        for kind in &node.fences {
            fences.insert(
                FenceId::new(node.id.clone(), *kind),
                lease.register_fence()?,
            );
        }
        for (allocation, permit) in allocations {
            self.active_slots
                .insert(allocation.physical_slot.clone(), allocation.id.clone());
            self.active_allocations.insert(
                allocation.id,
                ActiveAllocation {
                    slot: allocation.physical_slot,
                    remaining: allocation.lifetime.release_after,
                    permit,
                },
            );
        }
        self.states.insert(node.id.clone(), NodeState::Running);
        let resources = claims
            .iter()
            .map(|claim| WorkResourceCapability {
                resource: claim.resource.clone(),
                amount: claim.amount,
                lifetime: claim.lifetime.clone(),
            })
            .collect();
        let allocation_capabilities = node
            .allocations
            .iter()
            .map(|usage| {
                let allocation = &self.dag.logical_allocations[&usage.allocation];
                let slot = &self.dag.physical_slots[&allocation.physical_slot];
                WorkAllocationCapability {
                    allocation: usage.allocation.clone(),
                    physical_slot: allocation.physical_slot.clone(),
                    capacity_bytes: slot.capacity_bytes,
                    lifetime: usage.lifetime.clone(),
                }
            })
            .collect();
        self.running
            .insert(node.id.clone(), ActiveWork { permits, fences });
        self.available_quiescence.clear();
        Ok(Some(WorkExecutionContext {
            node,
            knobs: self.knobs.clone(),
            lease_epoch,
            cleanup,
            resources,
            allocations: allocation_capabilities,
            metal_runtime_claimed: Cell::new(false),
        }))
    }

    fn within_execution_limits(&self, node: &WorkNode) -> Result<bool, ExecutionError> {
        let workers = self.claimed_amount(|resource| matches!(resource, LeaseResource::Workers))?;
        let next_workers = workers
            .checked_add(claimed_by(node, |resource| {
                matches!(resource, LeaseResource::Workers)
            })?)
            .ok_or_else(|| ExecutionError::invalid_plan("worker claims overflow"))?;
        let cache =
            self.claimed_amount(|resource| matches!(resource, LeaseResource::ResidentCache))?;
        let next_cache = cache
            .checked_add(claimed_by(node, |resource| {
                matches!(resource, LeaseResource::ResidentCache)
            })?)
            .ok_or_else(|| ExecutionError::invalid_plan("cache claims overflow"))?;
        let active_io = u64::try_from(
            self.states
                .iter()
                .filter(|(id, state)| {
                    matches!(**state, NodeState::Running | NodeState::WorkComplete)
                        && self.dag.nodes[*id].domain == WorkDomain::Io
                })
                .count(),
        )
        .map_err(|_| ExecutionError::invalid_state("active I/O work count exceeds u64"))?;
        let next_io = active_io
            .checked_add(u64::from(node.domain == WorkDomain::Io))
            .ok_or_else(|| ExecutionError::invalid_state("active I/O work count overflow"))?;
        Ok(next_workers <= self.knobs.workers
            && next_cache <= self.knobs.cache_retention_bytes
            && next_io <= self.knobs.io_depth)
    }

    fn claimed_amount(
        &self,
        predicate: impl Fn(&LeaseResource) -> bool + Copy,
    ) -> Result<u64, ExecutionError> {
        self.running
            .values()
            .flat_map(|work| work.permits.iter().map(|held| &held.permit))
            .chain(self.deferred_permits.iter().map(|held| &held.permit))
            .chain(
                self.retained_permits
                    .values()
                    .map(|retained| &retained.permit),
            )
            .chain(self.observation_completion_permits.values().flatten())
            .chain(self.publication_permits.iter())
            .filter(|permit| predicate(permit.resource()))
            .try_fold(0_u64, |total, permit| {
                total
                    .checked_add(permit.amount())
                    .ok_or_else(|| ExecutionError::invalid_state("active resource claims overflow"))
            })
    }

    fn complete_retained_event(&mut self, node: &WorkNodeId) -> Result<(), ExecutionError> {
        let completed = self
            .retained_permits
            .keys()
            .filter(|id| &id.release == node)
            .cloned()
            .collect::<Vec<_>>();
        for id in completed {
            self.retained_permits
                .remove(&id)
                .expect("retained permit key was collected from this map")
                .permit
                .release()?;
        }
        Ok(())
    }

    fn complete_allocation_event(&mut self, event: &WorkDependency) -> Result<(), ExecutionError> {
        let completed = self
            .active_allocations
            .iter_mut()
            .filter_map(|(id, active)| {
                active.remaining.remove(event);
                active.remaining.is_empty().then_some(id.clone())
            })
            .collect::<Vec<_>>();
        for allocation in completed {
            let used_by_publication =
                self.terminal_publication
                    .as_ref()
                    .is_some_and(|publication| {
                        self.dag.nodes[publication]
                            .allocations
                            .iter()
                            .any(|allocation_use| allocation_use.allocation == allocation)
                    });
            if used_by_publication {
                self.publication_allocations.insert(allocation);
            } else {
                self.release_allocation(&allocation)?;
            }
        }
        Ok(())
    }

    fn release_allocation(&mut self, allocation: &AllocationId) -> Result<(), ExecutionError> {
        let active = self.active_allocations.remove(allocation).ok_or_else(|| {
            ExecutionError::invalid_state(format!(
                "logical allocation {} is not active",
                allocation.as_str()
            ))
        })?;
        match self.active_slots.remove(&active.slot) {
            Some(occupant) if &occupant == allocation => active.permit.release().map(|_| ())?,
            _ => {
                return Err(ExecutionError::invalid_state(format!(
                    "physical slot {} is not occupied by allocation {}",
                    active.slot.as_str(),
                    allocation.as_str()
                )));
            }
        }
        Ok(())
    }

    fn release_all_allocations(&mut self) -> Result<(), ExecutionError> {
        let allocations = self.active_allocations.keys().cloned().collect::<Vec<_>>();
        for allocation in allocations {
            self.release_allocation(&allocation)?;
        }
        self.publication_allocations.clear();
        Ok(())
    }

    fn release_publication_permits(&mut self) -> Result<(), ExecutionError> {
        for permit in std::mem::take(&mut self.publication_permits) {
            permit.release()?;
        }
        Ok(())
    }

    pub(crate) fn complete_publication(&mut self) -> Result<(), ExecutionError> {
        self.release_all_allocations()?;
        self.publication_allocations.clear();
        self.release_publication_permits()?;
        match self.finish(SchedulerTerminal::Succeeded)? {
            SchedulerAction::Complete(SchedulerTerminal::Succeeded) => Ok(()),
            _ => Err(ExecutionError::invalid_state(
                "publication completion did not terminate the scheduler",
            )),
        }
    }

    fn release_all_retained_permits(&mut self) -> Result<(), ExecutionError> {
        for (_, retained) in std::mem::take(&mut self.retained_permits) {
            retained.permit.release()?;
        }
        for permit in std::mem::take(&mut self.observation_completion_permits)
            .into_values()
            .flatten()
        {
            permit.release()?;
        }
        for permit in std::mem::take(&mut self.artifact_permits)
            .into_values()
            .flatten()
        {
            permit.release()?;
        }
        Ok(())
    }

    fn finish_draining(
        &mut self,
        outcome: SchedulerTerminal,
    ) -> Result<SchedulerAction, ExecutionError> {
        if self.quarantined_allocations.is_empty() {
            self.release_all_allocations()?;
            self.release_all_retained_permits()?;
            self.release_publication_permits()?;
            return self.finish(outcome);
        }
        let releasable = self
            .active_allocations
            .keys()
            .filter(|allocation| !self.quarantined_allocations.contains(*allocation))
            .cloned()
            .collect::<Vec<_>>();
        for allocation in releasable {
            self.release_allocation(&allocation)?;
        }
        self.publication_allocations.clear();
        self.release_publication_permits()?;
        for allocation in &self.quarantined_allocations {
            let active = self.active_allocations.get(allocation).ok_or_else(|| {
                ExecutionError::invalid_state(format!(
                    "quarantined logical allocation {} is not active",
                    allocation.as_str()
                ))
            })?;
            if self.active_slots.get(&active.slot) != Some(allocation) {
                return Err(ExecutionError::invalid_state(format!(
                    "quarantined physical slot {} is not occupied by allocation {}",
                    active.slot.as_str(),
                    allocation.as_str()
                )));
            }
        }
        let mut quarantined_permits = Vec::with_capacity(self.quarantined_allocations.len());
        for allocation in std::mem::take(&mut self.quarantined_allocations) {
            let active = self
                .active_allocations
                .remove(&allocation)
                .expect("quarantined allocation was prevalidated");
            self.active_slots.remove(&active.slot);
            quarantined_permits.push(active.permit);
        }
        quarantined_permits.extend(
            std::mem::take(&mut self.retained_permits)
                .into_values()
                .map(|retained| retained.permit),
        );
        if !self.running.is_empty()
            || !self.outstanding_fences.is_empty()
            || !self.deferred_permits.is_empty()
            || !self.retained_permits.is_empty()
            || !self.observation_completion_permits.is_empty()
            || !self.artifact_permits.is_empty()
            || !self.publication_permits.is_empty()
            || !self.active_allocations.is_empty()
            || !self.active_slots.is_empty()
        {
            return Err(ExecutionError::invalid_state(
                "quarantined terminal scheduler still owns unclassified resources",
            ));
        }
        let lease = self
            .lease
            .take()
            .ok_or_else(|| ExecutionError::invalid_state("execution lease was already released"))?;
        lease.quarantine_external_permits(quarantined_permits)?;
        self.terminal = Some(outcome.clone());
        Ok(SchedulerAction::Complete(outcome))
    }

    fn settle_node(&mut self, node_id: &WorkNodeId) -> Result<(), ExecutionError> {
        let state = self.states.get_mut(node_id).ok_or_else(|| {
            ExecutionError::invalid_state(format!("unknown work node {}", node_id.as_str()))
        })?;
        if *state != NodeState::WorkComplete {
            return Err(ExecutionError::invalid_state(format!(
                "work node {} cannot settle from state {state:?}",
                node_id.as_str()
            )));
        }
        *state = NodeState::Settled;
        self.available_quiescence
            .extend(self.dag.nodes[node_id].quiescence_after.iter().copied());
        Ok(())
    }

    fn begin_draining(&mut self, outcome: SchedulerTerminal) {
        if self.draining.is_none() {
            self.draining = Some(outcome);
        }
        let active_external_allocations = self
            .active_allocations
            .keys()
            .filter(|allocation| self.has_external_release(allocation))
            .cloned()
            .collect::<BTreeSet<_>>();
        for (id, state) in &mut self.states {
            if *state != NodeState::Pending {
                continue;
            }
            let node = &self.dag.nodes[id];
            *state = if node.kind == WorkKind::Release
                && node
                    .allocations
                    .iter()
                    .any(|use_| active_external_allocations.contains(&use_.allocation))
            {
                NodeState::CleanupPending
            } else {
                NodeState::Cancelled
            };
        }
    }

    fn has_external_release(&self, allocation: &AllocationId) -> bool {
        self.dag.nodes.values().any(|node| {
            node.kind == WorkKind::Release
                && node
                    .allocations
                    .iter()
                    .any(|use_| &use_.allocation == allocation)
        })
    }

    pub(crate) fn quarantine(&self) -> Result<(), ExecutionError> {
        let lease = self
            .lease
            .as_ref()
            .ok_or_else(|| ExecutionError::invalid_state("execution lease was released"))?;
        let _retained = lease.register_fence()?;
        Ok(())
    }

    pub(crate) fn cancel_after_error(&mut self) {
        if self.terminal.is_none() {
            self.begin_draining(SchedulerTerminal::Cancelled);
        }
    }

    fn finish(&mut self, outcome: SchedulerTerminal) -> Result<SchedulerAction, ExecutionError> {
        if !self.running.is_empty()
            || !self.outstanding_fences.is_empty()
            || !self.deferred_permits.is_empty()
            || !self.retained_permits.is_empty()
            || !self.observation_completion_permits.is_empty()
            || !self.publication_permits.is_empty()
            || !self.active_allocations.is_empty()
            || !self.active_slots.is_empty()
            || !self.quarantined_allocations.is_empty()
        {
            return Err(ExecutionError::invalid_state(
                "terminal scheduler still owns work, fences, permits, or allocations",
            ));
        }
        let lease = self
            .lease
            .take()
            .ok_or_else(|| ExecutionError::invalid_state("execution lease was already released"))?;
        if self.transferred_artifact_permits {
            if lease.release_retaining_artifact_storage()?.is_released() {
                return Err(ExecutionError::invalid_state(
                    "artifact-retained execution lease released prematurely",
                ));
            }
        } else if !lease.release()?.is_released() {
            return Err(ExecutionError::invalid_state(
                "terminal scheduler retained a resource permit or fence",
            ));
        }
        self.terminal = Some(outcome.clone());
        Ok(SchedulerAction::Complete(outcome))
    }
}

fn claimed_by(
    node: &WorkNode,
    predicate: impl Fn(&LeaseResource) -> bool,
) -> Result<u64, ExecutionError> {
    node.claims
        .iter()
        .filter(|claim| predicate(&claim.resource))
        .try_fold(0_u64, |total, claim| {
            total
                .checked_add(claim.amount)
                .ok_or_else(|| ExecutionError::invalid_plan("work-node resource claims overflow"))
        })
}

fn fence_ids(node: &WorkNodeId, kinds: &BTreeSet<FenceKind>) -> BTreeSet<FenceId> {
    kinds
        .iter()
        .map(|kind| FenceId::new(node.clone(), *kind))
        .collect()
}

fn validate_lease_claims(dag: &ExecutionDag, lease: &ResourceLease) -> Result<(), ExecutionError> {
    if lease.declared_limits() != &dag.resource_alternative.demand.lease_limits() {
        return Err(ExecutionError::invalid_state(
            "Resource Authority lease limits differ from the immutable plan demand",
        ));
    }
    Ok(())
}

fn validate_claims_against_demand(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    demand: &crate::DemandEnvelope,
) -> Result<(), ExecutionError> {
    let limits = demand.lease_limits();
    for node in nodes.values() {
        let mut totals = BTreeMap::<LeaseResource, u64>::new();
        for claim in &node.claims {
            let total = totals
                .entry(claim.resource.accounting_resource())
                .or_default();
            *total = total.checked_add(claim.amount).ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "work node {} resource claims overflow",
                    node.id.as_str()
                ))
            })?;
        }
        for (resource, amount) in totals {
            let limit = limits.get(&resource).copied().ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "work node {} claims undeclared lease resource {resource:?}",
                    node.id.as_str()
                ))
            })?;
            if amount > limit {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} claims {amount} of {resource:?}, exceeding plan limit {limit}",
                    node.id.as_str()
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_topology(
    dag: &ExecutionDag,
    topology: &ResourceTopology,
) -> Result<(), ExecutionError> {
    let domains = topology
        .memory_domains
        .iter()
        .map(|domain| (&domain.id, domain))
        .collect::<BTreeMap<_, _>>();
    let views = topology
        .memory_views
        .iter()
        .map(|view| (&view.id, view))
        .collect::<BTreeMap<_, _>>();
    let host_view = views
        .get(&dag.resource_alternative.demand.host_memory_view)
        .ok_or_else(|| ExecutionError::invalid_plan("demand host-memory view is absent"))?;
    if host_view.kind != MemoryViewKind::Host {
        return Err(ExecutionError::invalid_plan(
            "demand host-memory view is not CPU-visible host memory",
        ));
    }
    for slot in dag.physical_slots.values() {
        let domain = domains
            .get(&slot.compatibility.memory_domain)
            .ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "physical slot {} uses an absent memory domain",
                    slot.id.as_str()
                ))
            })?;
        let slot_views = slot
            .compatibility
            .views
            .iter()
            .map(|id| {
                views.get(id).copied().ok_or_else(|| {
                    ExecutionError::invalid_plan(format!(
                        "physical slot {} uses absent memory view {}",
                        slot.id.as_str(),
                        id.as_str()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if slot_views
            .iter()
            .any(|view| view.domain != slot.compatibility.memory_domain)
        {
            return Err(ExecutionError::invalid_plan(format!(
                "physical slot {} views do not share its physical capacity domain",
                slot.id.as_str()
            )));
        }
        match slot.compatibility.storage_mode {
            StorageMode::Host => {
                if slot_views
                    .iter()
                    .any(|view| view.kind != MemoryViewKind::Host)
                {
                    return Err(ExecutionError::invalid_plan(format!(
                        "host slot {} exposes a Metal view",
                        slot.id.as_str()
                    )));
                }
            }
            StorageMode::MetalShared => {
                let has_host = slot_views
                    .iter()
                    .any(|view| view.kind == MemoryViewKind::Host);
                let has_metal = slot_views
                    .iter()
                    .any(|view| view.kind == MemoryViewKind::Metal);
                let has_metal_accelerator = topology
                    .accelerators
                    .iter()
                    .any(|accelerator| slot.compatibility.views.contains(&accelerator.memory_view));
                if domain.kind != MemoryCapacityKind::Unified
                    || !has_host
                    || !has_metal
                    || !has_metal_accelerator
                {
                    return Err(ExecutionError::invalid_plan(format!(
                        "Metal-shared slot {} must use host and accelerator Metal views of one unified domain",
                        slot.id.as_str()
                    )));
                }
            }
        }
    }
    for node in dag.nodes.values() {
        let WorkDomain::Metal { demand_id } = &node.domain else {
            continue;
        };
        let demand = dag
            .resource_alternative
            .demand
            .accelerators
            .iter()
            .find(|demand| &demand.demand_id == demand_id)
            .ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "Metal work node {} references undeclared accelerator demand {demand_id}",
                    node.id.as_str()
                ))
            })?;
        let accelerator = topology
            .accelerators
            .iter()
            .find(|accelerator| accelerator.id == demand.accelerator)
            .ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "Metal work node {} references an absent accelerator",
                    node.id.as_str()
                ))
            })?;
        let metal_view = views
            .get(&accelerator.memory_view)
            .ok_or_else(|| ExecutionError::invalid_plan("Metal accelerator view is absent"))?;
        let domain = domains.get(&metal_view.domain).ok_or_else(|| {
            ExecutionError::invalid_plan("Metal accelerator memory domain is absent")
        })?;
        if accelerator.kind != crate::AcceleratorKind::Metal
            || metal_view.kind != MemoryViewKind::Metal
            || domain.kind != MemoryCapacityKind::Unified
            || host_view.domain != metal_view.domain
        {
            return Err(ExecutionError::invalid_plan(
                "Metal execution requires Apple-style host/device views of one unified domain",
            ));
        }
        for allocation_use in &node.allocations {
            let allocation = &dag.logical_allocations[&allocation_use.allocation];
            let slot = &dag.physical_slots[&allocation.physical_slot];
            if !slot.compatibility.views.contains(&accelerator.memory_view)
                || !slot.compatibility.views.contains(&host_view.id)
                || slot.compatibility.memory_domain != metal_view.domain
            {
                return Err(ExecutionError::invalid_plan(format!(
                    "Metal work node {} uses slot {} outside its unified accelerator memory domain",
                    node.id.as_str(),
                    slot.id.as_str()
                )));
            }
        }
    }
    Ok(())
}

fn canonical_physical_work_id(plan: &ExecutionDag) -> PhysicalWorkId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(PHYSICAL_WORK_IDENTITY_DOMAIN);
    encoder.u32(PHYSICAL_WORK_IDENTITY_VERSION);
    encode_string_set(&mut encoder, &plan.required_resource_capabilities, |id| {
        id.as_str()
    });
    encode_alternative(&mut encoder, &plan.resource_alternative);
    encode_string_set(&mut encoder, &plan.selected_implementations, |id| {
        id.as_str()
    });
    encoder.usize(plan.nodes.len());
    for node in plan.nodes.values() {
        encode_node(&mut encoder, node);
    }
    encoder.usize(plan.logical_allocations.len());
    for allocation in plan.logical_allocations.values() {
        encoder.string(allocation.id.as_str());
        encoder.u64(allocation.bytes);
        match allocation.purpose {
            AllocationPurpose::Data => encoder.u8(0),
            AllocationPurpose::IoBuffer(kind) => {
                encoder.u8(1);
                encode_io_buffer(&mut encoder, kind);
            }
        }
        encode_compatibility(&mut encoder, &allocation.compatibility);
        encoder.string(allocation.physical_slot.as_str());
        encoder.string(allocation.lifetime.acquire_at.as_str());
        encoder.usize(allocation.lifetime.release_after.len());
        for dependency in &allocation.lifetime.release_after {
            encode_dependency(&mut encoder, dependency);
        }
    }
    encoder.usize(plan.physical_slots.len());
    for slot in plan.physical_slots.values() {
        encoder.string(slot.id.as_str());
        encode_lease_resource(&mut encoder, &slot.lease_resource);
        encoder.u64(slot.capacity_bytes);
        encode_compatibility(&mut encoder, &slot.compatibility);
    }
    encode_knobs(&mut encoder, &plan.initial_knobs);
    encoder.usize(plan.adaptations.len());
    for transition in plan.adaptations.values() {
        encoder.string(transition.id.as_str());
        encode_knobs(&mut encoder, &transition.from);
        encode_knobs(&mut encoder, &transition.to);
        encode_quiescence(&mut encoder, transition.at);
    }
    PhysicalWorkId::from_sha256(encoder.finish())
}

fn encode_string_set<T>(
    encoder: &mut CanonicalEncoder,
    values: &BTreeSet<T>,
    string: impl Fn(&T) -> &str,
) {
    encoder.usize(values.len());
    for value in values {
        encoder.string(string(value));
    }
}

fn encode_alternative(encoder: &mut CanonicalEncoder, alternative: &DemandAlternative) {
    encoder.string(alternative.id.as_str());
    encode_string_set(encoder, &alternative.capabilities.supported, |id| {
        id.as_str()
    });
    encode_demand(encoder, &alternative.demand);
    encode_headroom(encoder, &alternative.headroom);
    encoder.u64(alternative.scaling.minimum_workers);
    encoder.u64(alternative.scaling.maximum_workers);
    encoder.u64(alternative.scaling.maximum_batch_size);
    encoder.u64(alternative.scaling.maximum_tile_width);
    encoder.u64(alternative.scaling.maximum_tile_height);
    encoder.u64(alternative.scaling.maximum_slab_depth);
    encode_identity_map(
        encoder,
        &alternative.scaling.memory_bytes_per_worker,
        |id| id.as_str(),
    );
    encoder.usize(alternative.quiescence_points.len());
    for point in &alternative.quiescence_points {
        encode_quiescence(encoder, *point);
    }
}

fn encode_demand(encoder: &mut CanonicalEncoder, demand: &crate::DemandEnvelope) {
    encoder.string(demand.host_memory_view.as_str());
    let mut memory = demand.memory.iter().collect::<Vec<_>>();
    memory.sort_unstable_by_key(|item| item.allocation_id.as_str());
    encoder.usize(memory.len());
    for item in memory {
        encoder.string(&item.allocation_id);
        encoder.u64(item.hard_bytes);
        encoder.u64(item.preferred_bytes);
        let mut views = item.views.iter().collect::<Vec<_>>();
        views.sort_unstable();
        encoder.usize(views.len());
        for view in views {
            encoder.string(view.as_str());
        }
    }
    encode_count(encoder, demand.workers);
    for amount in [
        demand.overhead.thread_stack_bytes,
        demand.overhead.allocator_fragmentation_bytes,
        demand.overhead.external_library_bytes,
        demand.overhead.fft_workspace_bytes,
        demand.overhead.driver_bytes,
        demand.overhead.jit_bytes,
        demand.overhead.command_buffer_bytes,
    ] {
        encoder.u64(amount);
    }
    let mut storage = demand.storage.iter().collect::<Vec<_>>();
    storage.sort_unstable_by_key(|item| item.demand_id.as_str());
    encoder.usize(storage.len());
    for item in storage {
        encoder.string(&item.demand_id);
        encoder.string(item.domain.as_str());
        for amount in [
            item.temporary_bytes,
            item.staged_output_bytes,
            item.final_output_bytes,
            item.persistent_cache_bytes,
        ] {
            encoder.u64(amount);
        }
        encode_count(encoder, item.read_rate);
        encode_count(encoder, item.write_rate);
        encode_count(encoder, item.operations_rate);
        encode_count(encoder, item.queue_slots);
    }
    let mut rates = demand.rates.iter().collect::<Vec<_>>();
    rates.sort_unstable_by_key(|item| item.demand_id.as_str());
    encoder.usize(rates.len());
    for item in rates {
        encoder.string(&item.demand_id);
        encoder.string(item.resource.as_str());
        encode_count(encoder, item.amount);
    }
    encoder.u64(demand.caches.hard_resident_bytes);
    encoder.u64(demand.caches.preferred_resident_bytes);
    encode_count(encoder, demand.locks);
    encode_count(encoder, demand.file_descriptors);
    let mut queues = demand.queues.iter().collect::<Vec<_>>();
    queues.sort_unstable_by_key(|item| item.demand_id.as_str());
    encoder.usize(queues.len());
    for item in queues {
        encoder.string(&item.demand_id);
        encoder.string(item.resource.as_str());
        encode_count(encoder, item.slots);
    }
    let mut transfers = demand.transfers.iter().collect::<Vec<_>>();
    transfers.sort_unstable_by_key(|item| item.demand_id.as_str());
    encoder.usize(transfers.len());
    for item in transfers {
        encoder.string(&item.demand_id);
        encoder.string(item.link.as_str());
        encode_count(encoder, item.rate);
        encode_count(encoder, item.queue_slots);
    }
    let mut accelerators = demand.accelerators.iter().collect::<Vec<_>>();
    accelerators.sort_unstable_by_key(|item| item.demand_id.as_str());
    encoder.usize(accelerators.len());
    for item in accelerators {
        encoder.string(&item.demand_id);
        encoder.string(item.accelerator.as_str());
        encode_count(encoder, item.slots);
        encode_count(encoder, item.command_queue_slots);
    }
    for amount in [
        demand.io_buffers.source_read_ahead_bytes,
        demand.io_buffers.decode_bytes,
        demand.io_buffers.preparation_bytes,
        demand.io_buffers.host_to_device_transfer_bytes,
        demand.io_buffers.device_to_host_transfer_bytes,
        demand.io_buffers.spill_read_bytes,
        demand.io_buffers.spill_write_bytes,
        demand.io_buffers.serialization_bytes,
        demand.io_buffers.storage_manager_bytes,
        demand.io_buffers.tiled_column_writer_bytes,
        demand.io_buffers.scalar_column_writer_bytes,
        demand.io_buffers.writeback_bytes,
        demand.io_buffers.publication_bytes,
        demand.io_buffers.mapped_page_cache_bytes,
    ] {
        encoder.u64(amount);
    }
}

fn encode_count(encoder: &mut CanonicalEncoder, demand: crate::CountDemand) {
    encoder.u64(demand.hard());
    encoder.u64(demand.preferred());
}

fn encode_headroom(encoder: &mut CanonicalEncoder, value: &crate::ResourceHeadroom) {
    encode_identity_map(encoder, &value.memory_bytes, |id| id.as_str());
    encoder.u64(value.workers);
    encode_identity_map(encoder, &value.storage_bytes, |id| id.as_str());
    encode_identity_map(encoder, &value.rates_per_second, |id| id.as_str());
    encoder.u64(value.cache_bytes);
    encoder.u64(value.locks);
    encoder.u64(value.file_descriptors);
    encode_identity_map(encoder, &value.queue_slots, |id| id.as_str());
    encode_identity_map(encoder, &value.accelerator_slots, |id| id.as_str());
}

fn encode_identity_map<Id: Ord>(
    encoder: &mut CanonicalEncoder,
    values: &BTreeMap<Id, u64>,
    string: impl Fn(&Id) -> &str,
) {
    encoder.usize(values.len());
    for (identity, amount) in values {
        encoder.string(string(identity));
        encoder.u64(*amount);
    }
}

fn encode_node(encoder: &mut CanonicalEncoder, node: &WorkNode) {
    encoder.string(node.id.as_str());
    encode_work_kind(encoder, node.kind);
    match &node.domain {
        WorkDomain::Cpu => encoder.u8(0),
        WorkDomain::Metal { demand_id } => {
            encoder.u8(1);
            encoder.string(demand_id);
        }
        WorkDomain::Io => encoder.u8(2),
        WorkDomain::Control => encoder.u8(3),
    }
    encoder.string(node.implementation.as_str());
    encoder.usize(node.dependencies.len());
    for dependency in &node.dependencies {
        encode_dependency(encoder, dependency);
    }
    let mut claims = node.claims.iter().collect::<Vec<_>>();
    claims.sort_unstable_by(|left, right| {
        (&left.resource, &left.lifetime).cmp(&(&right.resource, &right.lifetime))
    });
    encoder.usize(claims.len());
    for claim in claims {
        encode_lease_resource(encoder, &claim.resource);
        encoder.u64(claim.amount);
        encode_lifetime(encoder, &claim.lifetime);
    }
    let mut allocations = node.allocations.iter().collect::<Vec<_>>();
    allocations.sort_unstable_by(|left, right| {
        (&left.allocation, &left.lifetime).cmp(&(&right.allocation, &right.lifetime))
    });
    encoder.usize(allocations.len());
    for allocation in allocations {
        encoder.string(allocation.allocation.as_str());
        encode_lifetime(encoder, &allocation.lifetime);
    }
    encoder.usize(node.fences.len());
    for fence in &node.fences {
        encode_fence(encoder, *fence);
    }
    encoder.usize(node.quiescence_after.len());
    for point in &node.quiescence_after {
        encode_quiescence(encoder, *point);
    }
}

fn encode_lease_resource(encoder: &mut CanonicalEncoder, resource: &LeaseResource) {
    use crate::LeaseResource::*;
    match resource {
        Memory { allocation_id } => encode_named_resource(encoder, 0, allocation_id),
        Workers => encoder.u8(1),
        RuntimeOverhead(kind) => {
            encoder.u8(2);
            encode_runtime_overhead(encoder, *kind);
        }
        IoBuffer(kind) => {
            encoder.u8(3);
            encode_io_buffer(encoder, *kind);
        }
        Storage {
            demand_id,
            use_kind,
        } => {
            encode_named_resource(encoder, 4, demand_id);
            encode_storage_use(encoder, *use_kind);
        }
        StorageReadRate { demand_id } => encode_named_resource(encoder, 5, demand_id),
        StorageWriteRate { demand_id } => encode_named_resource(encoder, 6, demand_id),
        StorageOperationsRate { demand_id } => encode_named_resource(encoder, 7, demand_id),
        StorageQueue { demand_id } => encode_named_resource(encoder, 8, demand_id),
        Rate { demand_id } => encode_named_resource(encoder, 9, demand_id),
        Queue { demand_id } => encode_named_resource(encoder, 10, demand_id),
        TransferRate { demand_id } => encode_named_resource(encoder, 11, demand_id),
        TransferQueue { demand_id } => encode_named_resource(encoder, 12, demand_id),
        Accelerator { demand_id } => encode_named_resource(encoder, 13, demand_id),
        AcceleratorCommandQueue { demand_id } => {
            encode_named_resource(encoder, 14, demand_id);
        }
        ResidentCache => encoder.u8(15),
        Locks => encoder.u8(16),
        FileDescriptors => encoder.u8(17),
        MeasurementSetLock { measurement_set } => {
            encoder.u8(18);
            encoder.digest(measurement_set.identity().as_bytes());
        }
    }
}

fn encode_named_resource(encoder: &mut CanonicalEncoder, tag: u8, identity: &str) {
    encoder.u8(tag);
    encoder.string(identity);
}

fn encode_lifetime(encoder: &mut CanonicalEncoder, lifetime: &ClaimLifetime) {
    match lifetime {
        ClaimLifetime::Work => encoder.u8(0),
        ClaimLifetime::Fences(kinds) => {
            encoder.u8(1);
            encoder.usize(kinds.len());
            for kind in kinds {
                encode_fence(encoder, *kind);
            }
        }
        ClaimLifetime::RetainedUntil(release) => {
            encoder.u8(2);
            encoder.string(release.as_str());
        }
        ClaimLifetime::Artifact => encoder.u8(3),
    }
}

fn encode_fence_id(encoder: &mut CanonicalEncoder, fence: &FenceId) {
    encoder.string(fence.node.as_str());
    encode_fence(encoder, fence.kind);
}

fn encode_dependency(encoder: &mut CanonicalEncoder, dependency: &WorkDependency) {
    match dependency {
        WorkDependency::Work(predecessor) => {
            encoder.u8(0);
            encoder.string(predecessor.as_str());
        }
        WorkDependency::Fence(fence) => {
            encoder.u8(1);
            encode_fence_id(encoder, fence);
        }
    }
}

fn encode_fence(encoder: &mut CanonicalEncoder, kind: FenceKind) {
    encoder.u8(match kind {
        FenceKind::Device => 0,
        FenceKind::Io => 1,
        FenceKind::Writeback => 2,
        FenceKind::Publication => 3,
    });
}

fn encode_quiescence(encoder: &mut CanonicalEncoder, point: QuiescencePoint) {
    encoder.u8(match point {
        QuiescencePoint::RunBoundary => 0,
        QuiescencePoint::MajorCycle => 1,
        QuiescencePoint::TileBatch => 2,
        QuiescencePoint::Slab => 3,
        QuiescencePoint::Stage => 4,
    });
}

fn encode_compatibility(encoder: &mut CanonicalEncoder, value: &SlotCompatibility) {
    encoder.string(value.memory_domain.as_str());
    encode_string_set(encoder, &value.views, |view| view.as_str());
    encoder.u64(value.alignment_bytes);
    encoder.u8(match value.storage_mode {
        StorageMode::Host => 0,
        StorageMode::MetalShared => 1,
    });
    encoder.string(value.layout.as_str());
    encoder.u8(match value.initialization {
        InitializationPolicy::Preserve => 0,
        InitializationPolicy::ZeroBeforeRead => 1,
        InitializationPolicy::OverwriteBeforeRead => 2,
    });
    encoder.u8(match value.access {
        AllocationAccess::ReadOnly => 0,
        AllocationAccess::ReadWrite => 1,
        AllocationAccess::WriteOnly => 2,
    });
}

fn encode_work_kind(encoder: &mut CanonicalEncoder, kind: WorkKind) {
    encoder.u8(match kind {
        WorkKind::DataCensus => 0,
        WorkKind::Preparation => 1,
        WorkKind::Cache => 2,
        WorkKind::ConvolutionFunction => 3,
        WorkKind::FftPlanning => 4,
        WorkKind::Jit => 5,
        WorkKind::Compute => 6,
        WorkKind::Transfer => 7,
        WorkKind::Spill => 8,
        WorkKind::Prefetch => 9,
        WorkKind::Io => 10,
        WorkKind::Serialization => 11,
        WorkKind::Writeback => 12,
        WorkKind::Publication => 13,
        WorkKind::Synchronization => 14,
        WorkKind::Release => 15,
        WorkKind::ObservationRead => 16,
        WorkKind::ObservationReadWriteback => 17,
    });
}

fn encode_runtime_overhead(encoder: &mut CanonicalEncoder, kind: crate::RuntimeOverheadKind) {
    encoder.u8(match kind {
        crate::RuntimeOverheadKind::ThreadStack => 0,
        crate::RuntimeOverheadKind::AllocatorFragmentation => 1,
        crate::RuntimeOverheadKind::ExternalLibrary => 2,
        crate::RuntimeOverheadKind::FftWorkspace => 3,
        crate::RuntimeOverheadKind::Driver => 4,
        crate::RuntimeOverheadKind::Jit => 5,
        crate::RuntimeOverheadKind::CommandBuffer => 6,
    });
}

fn encode_io_buffer(encoder: &mut CanonicalEncoder, kind: crate::IoBufferKind) {
    encoder.u8(match kind {
        crate::IoBufferKind::SourceReadAhead => 0,
        crate::IoBufferKind::Decode => 1,
        crate::IoBufferKind::Preparation => 2,
        crate::IoBufferKind::HostToDeviceTransfer => 3,
        crate::IoBufferKind::DeviceToHostTransfer => 4,
        crate::IoBufferKind::SpillRead => 5,
        crate::IoBufferKind::SpillWrite => 6,
        crate::IoBufferKind::Serialization => 7,
        crate::IoBufferKind::StorageManager => 8,
        crate::IoBufferKind::TiledColumnWriter => 9,
        crate::IoBufferKind::ScalarColumnWriter => 10,
        crate::IoBufferKind::Writeback => 11,
        crate::IoBufferKind::Publication => 12,
        crate::IoBufferKind::MappedPageCache => 13,
    });
}

fn encode_storage_use(encoder: &mut CanonicalEncoder, kind: crate::StorageUseKind) {
    encoder.u8(match kind {
        crate::StorageUseKind::Temporary => 0,
        crate::StorageUseKind::StagedOutput => 1,
        crate::StorageUseKind::FinalOutput => 2,
        crate::StorageUseKind::PersistentCache => 3,
    });
}

fn encode_knobs(encoder: &mut CanonicalEncoder, knobs: &ExecutionKnobs) {
    for value in [
        knobs.workers,
        knobs.batch_size,
        knobs.tile_width,
        knobs.tile_height,
        knobs.slab_depth,
        knobs.io_depth,
        knobs.cache_retention_bytes,
    ] {
        encoder.u64(value);
    }
    for enabled in [
        knobs.fusion,
        knobs.recomputation,
        knobs.spill,
        knobs.prefetch,
    ] {
        encoder.u8(u8::from(enabled));
    }
}

fn unique_map<T, Id, F>(
    label: &str,
    values: Vec<T>,
    identity: F,
) -> Result<BTreeMap<Id, T>, ExecutionError>
where
    Id: Clone + Ord + AsRef<str>,
    F: Fn(&T) -> &Id,
{
    let mut mapped = BTreeMap::new();
    for value in values {
        let id = identity(&value).clone();
        if id.as_ref().is_empty() || mapped.insert(id, value).is_some() {
            return Err(ExecutionError::invalid_plan(format!(
                "{label} identities must be non-empty and unique"
            )));
        }
    }
    Ok(mapped)
}

fn validate_resource_alternative(
    specification: &ExecutionDagSpecification,
) -> Result<(), ExecutionError> {
    let alternative = &specification.resource_alternative;
    if alternative.id.as_str().is_empty() {
        return Err(ExecutionError::invalid_plan(
            "selected resource alternative identity must not be empty",
        ));
    }
    if !specification
        .required_resource_capabilities
        .is_subset(&alternative.capabilities.supported)
    {
        return Err(ExecutionError::invalid_plan(
            "selected resource alternative does not satisfy required capabilities",
        ));
    }
    Ok(())
}

fn validate_nodes(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    allocations: &BTreeMap<AllocationId, LogicalAllocation>,
) -> Result<(), ExecutionError> {
    if nodes.is_empty() {
        return Err(ExecutionError::invalid_plan(
            "execution plan must contain at least one work node",
        ));
    }
    for node in nodes.values() {
        if node.implementation.as_str().is_empty() {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} has an empty implementation identity",
                node.id.as_str()
            )));
        }
        for dependency in &node.dependencies {
            let predecessor = nodes.get(dependency.predecessor()).ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "work node {} references unknown predecessor {}",
                    node.id.as_str(),
                    dependency.predecessor().as_str()
                ))
            })?;
            if dependency.predecessor() == &node.id {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} cannot depend on itself",
                    node.id.as_str()
                )));
            }
            if let WorkDependency::Fence(fence) = dependency
                && !predecessor.fences.contains(&fence.kind())
            {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} depends on undeclared {:?} fence from {}",
                    node.id.as_str(),
                    fence.kind(),
                    predecessor.id.as_str()
                )));
            }
        }
        validate_claims(node)?;
        validate_domain(node)?;
        validate_kind(node)?;
        let mut node_allocations = BTreeSet::new();
        for allocation_use in &node.allocations {
            if !allocations.contains_key(&allocation_use.allocation) {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} references an unknown logical allocation",
                    node.id.as_str()
                )));
            }
            if !node_allocations.insert(allocation_use.allocation.clone()) {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} references logical allocation {} more than once",
                    node.id.as_str(),
                    allocation_use.allocation.as_str()
                )));
            }
            if matches!(allocation_use.lifetime, ClaimLifetime::Artifact) {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} cannot retain a logical allocation in an artifact",
                    node.id.as_str()
                )));
            }
            validate_lifetime(node, &allocation_use.lifetime)?;
            if matches!(allocation_use.lifetime, ClaimLifetime::RetainedUntil(_)) {
                return Err(ExecutionError::invalid_plan(format!(
                    "logical allocation use on node {} must use its allocation ledger for cross-node retention",
                    node.id.as_str()
                )));
            }
            if matches!(&node.domain, WorkDomain::Metal { .. } | WorkDomain::Io)
                && allocation_use.lifetime != required_payload_lifetime(node)
            {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} has an allocation use without its exact asynchronous lifetime",
                    node.id.as_str()
                )));
            }
        }
    }
    Ok(())
}

fn validate_claims(node: &WorkNode) -> Result<(), ExecutionError> {
    let mut claims = BTreeSet::new();
    for claim in &node.claims {
        if claim.amount == 0 {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} has a zero resource claim",
                node.id.as_str()
            )));
        }
        validate_lifetime(node, &claim.lifetime)?;
        if matches!(claim.lifetime, ClaimLifetime::Artifact)
            && !matches!(
                claim.resource,
                LeaseResource::Storage {
                    use_kind: StorageUseKind::Temporary,
                    ..
                }
            )
        {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} retains a non-temporary-storage resource in an artifact",
                node.id.as_str()
            )));
        }
        if claim_requires_domain_lifetime(node, &claim.resource)
            && claim.lifetime != required_payload_lifetime(node)
            && !matches!(claim.lifetime, ClaimLifetime::RetainedUntil(_))
            && !matches!(claim.lifetime, ClaimLifetime::Artifact)
        {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} has a payload claim without its exact asynchronous lifetime",
                node.id.as_str()
            )));
        }
        if !claims.insert((claim.resource.clone(), claim.lifetime.clone())) {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} duplicates one resource claim and lifetime",
                node.id.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_retained_claims(nodes: &BTreeMap<WorkNodeId, WorkNode>) -> Result<(), ExecutionError> {
    let mut groups = BTreeMap::<(LeaseResource, WorkNodeId), Vec<(&WorkNode, u64)>>::new();
    for node in nodes.values() {
        for claim in &node.claims {
            let ClaimLifetime::RetainedUntil(release) = &claim.lifetime else {
                continue;
            };
            if !matches!(
                claim.resource,
                LeaseResource::MeasurementSetLock { .. } | LeaseResource::FileDescriptors
            ) {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} retains a resource that is not an external source handle",
                    node.id.as_str()
                )));
            }
            groups
                .entry((claim.resource.clone(), release.clone()))
                .or_default()
                .push((node, claim.amount));
        }
    }
    for ((resource, release), uses) in groups {
        let release_node = nodes.get(&release).ok_or_else(|| {
            ExecutionError::invalid_plan(format!(
                "retained resource {resource:?} names missing release node {}",
                release.as_str()
            ))
        })?;
        if release_node.kind != WorkKind::Release
            || !uses.iter().any(|(node, _)| node.id == release)
            || !uses.iter().any(|(node, _)| node.id != release)
        {
            return Err(ExecutionError::invalid_plan(format!(
                "retained resource {resource:?} requires pre-release use and ownership by Release node {}",
                release.as_str()
            )));
        }
        let amount = uses[0].1;
        if uses.iter().any(|(_, candidate)| *candidate != amount) {
            return Err(ExecutionError::invalid_plan(format!(
                "retained resource {resource:?} changes amount before release {}",
                release.as_str()
            )));
        }
        let release_event = WorkDependency::Work(release.clone());
        for (node, _) in &uses {
            let event = WorkDependency::Work(node.id.clone());
            if node.id != release && !event_strictly_precedes(nodes, &event, &release_event) {
                return Err(ExecutionError::invalid_plan(format!(
                    "retained resource {resource:?} has use outside its release-ordered lifetime"
                )));
            }
        }
        for (index, (left, _)) in uses.iter().enumerate() {
            for (right, _) in &uses[index + 1..] {
                let left_event = WorkDependency::Work(left.id.clone());
                let right_event = WorkDependency::Work(right.id.clone());
                if !event_precedes(nodes, &left_event, &right_event)
                    && !event_precedes(nodes, &right_event, &left_event)
                {
                    return Err(ExecutionError::invalid_plan(format!(
                        "retained resource {resource:?} has unordered uses at {} and {}",
                        left.id.as_str(),
                        right.id.as_str()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn required_payload_lifetime(node: &WorkNode) -> ClaimLifetime {
    if node.fences.is_empty() {
        ClaimLifetime::Work
    } else {
        ClaimLifetime::Fences(node.fences.clone())
    }
}

fn claim_requires_domain_lifetime(node: &WorkNode, resource: &LeaseResource) -> bool {
    match &node.domain {
        WorkDomain::Metal { .. } | WorkDomain::Io => !matches!(
            resource,
            LeaseResource::Workers
                | LeaseResource::RuntimeOverhead(crate::RuntimeOverheadKind::ThreadStack)
        ),
        WorkDomain::Cpu | WorkDomain::Control => false,
    }
}

fn validate_lifetime(node: &WorkNode, lifetime: &ClaimLifetime) -> Result<(), ExecutionError> {
    if let ClaimLifetime::Fences(kinds) = lifetime {
        if kinds.is_empty() || !kinds.is_subset(&node.fences) {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} retains state through an empty or undeclared fence set",
                node.id.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_domain(node: &WorkNode) -> Result<(), ExecutionError> {
    let expected_fences = match (&node.domain, node.kind) {
        (WorkDomain::Metal { .. }, _) => BTreeSet::from([FenceKind::Device]),
        (WorkDomain::Io, WorkKind::Writeback) => {
            BTreeSet::from([FenceKind::Io, FenceKind::Writeback])
        }
        (WorkDomain::Io, WorkKind::Publication) => {
            BTreeSet::from([FenceKind::Io, FenceKind::Publication])
        }
        (WorkDomain::Io, _) => BTreeSet::from([FenceKind::Io]),
        (WorkDomain::Cpu | WorkDomain::Control, _) => BTreeSet::new(),
    };
    let synchronous_observation_read =
        node.domain == WorkDomain::Io && node.kind.reads_observation() && node.fences.is_empty();
    if node.fences != expected_fences && !synchronous_observation_read {
        return Err(ExecutionError::invalid_plan(format!(
            "work node {} must declare its exact asynchronous fence set {expected_fences:?}",
            node.id.as_str()
        )));
    }
    match &node.domain {
        WorkDomain::Cpu => require_claim(
            node,
            |resource| matches!(resource, LeaseResource::Workers),
            "worker",
        ),
        WorkDomain::Metal { demand_id } => {
            if demand_id.is_empty() {
                return Err(ExecutionError::invalid_plan(format!(
                    "Metal work node {} has an empty demand identity",
                    node.id.as_str()
                )));
            }
            require_claim(
                node,
                |resource| matches!(resource, LeaseResource::Accelerator { demand_id: id } if id == demand_id),
                "accelerator",
            )?;
            require_claim(
                node,
                |resource| matches!(resource, LeaseResource::AcceleratorCommandQueue { demand_id: id } if id == demand_id),
                "Metal command queue",
            )?;
            require_fence(node, FenceKind::Device)
        }
        WorkDomain::Io => {
            require_claim(
                node,
                |resource| {
                    matches!(
                        resource,
                        LeaseResource::Queue { .. }
                            | LeaseResource::StorageQueue { .. }
                            | LeaseResource::TransferQueue { .. }
                    )
                },
                "I/O queue",
            )?;
            require_claim(
                node,
                |resource| {
                    matches!(
                        resource,
                        LeaseResource::StorageReadRate { .. }
                            | LeaseResource::StorageWriteRate { .. }
                            | LeaseResource::StorageOperationsRate { .. }
                            | LeaseResource::Rate { .. }
                            | LeaseResource::TransferRate { .. }
                    )
                },
                "I/O or transfer rate",
            )?;
            if synchronous_observation_read {
                Ok(())
            } else {
                require_fence(node, FenceKind::Io)
            }
        }
        WorkDomain::Control => {
            if node.kind != WorkKind::Synchronization
                || !node.claims.is_empty()
                || !node.allocations.is_empty()
                || !node.fences.is_empty()
            {
                return Err(ExecutionError::invalid_plan(format!(
                    "control node {} must be resource-free synchronization",
                    node.id.as_str()
                )));
            }
            Ok(())
        }
    }
}

fn validate_kind(node: &WorkNode) -> Result<(), ExecutionError> {
    use crate::RuntimeOverheadKind;
    for kind in node.claims.iter().filter_map(|claim| match claim.resource {
        LeaseResource::IoBuffer(kind) => Some(kind),
        _ => None,
    }) {
        if !io_buffer_kind_supports_work_kind(kind, node.kind) {
            return Err(ExecutionError::invalid_plan(format!(
                "{kind:?} I/O buffer is incompatible with {:?} work node {}",
                node.kind,
                node.id.as_str()
            )));
        }
    }
    match node.kind {
        WorkKind::Preparation => Ok(()),
        WorkKind::Cache => require_claim(
            node,
            |resource| {
                matches!(
                    resource,
                    LeaseResource::ResidentCache
                        | LeaseResource::Storage {
                            use_kind: crate::StorageUseKind::PersistentCache,
                            ..
                        }
                )
            },
            "resident or persistent cache reservation",
        ),
        WorkKind::FftPlanning => require_claim(
            node,
            |resource| {
                matches!(
                    resource,
                    LeaseResource::RuntimeOverhead(RuntimeOverheadKind::FftWorkspace)
                )
            },
            "FFT workspace",
        ),
        WorkKind::Jit => require_claim(
            node,
            |resource| {
                matches!(
                    resource,
                    LeaseResource::RuntimeOverhead(RuntimeOverheadKind::Jit)
                )
            },
            "JIT workspace",
        ),
        WorkKind::Transfer => {
            require_io_domain(node)?;
            require_claim(
                node,
                |resource| matches!(resource, LeaseResource::TransferRate { .. }),
                "transfer rate",
            )?;
            require_claim(
                node,
                |resource| matches!(resource, LeaseResource::TransferQueue { .. }),
                "transfer queue",
            )
        }
        WorkKind::Spill | WorkKind::Prefetch => require_io_domain(node),
        WorkKind::Io => require_io_domain(node),
        WorkKind::ObservationRead | WorkKind::ObservationReadWriteback => {
            require_io_domain(node)?;
            require_claim(
                node,
                |resource| matches!(resource, LeaseResource::MeasurementSetLock { .. }),
                "MeasurementSet lock",
            )
        }
        WorkKind::Serialization => Ok(()),
        WorkKind::Writeback => {
            require_io_domain(node)?;
            require_fence(node, FenceKind::Writeback)
        }
        WorkKind::Publication => {
            require_io_domain(node)?;
            require_fence(node, FenceKind::Publication)
        }
        WorkKind::Release => {
            if node.allocations.is_empty() {
                Err(ExecutionError::invalid_plan(format!(
                    "release work node {} must own at least one logical allocation",
                    node.id.as_str()
                )))
            } else {
                Ok(())
            }
        }
        WorkKind::Synchronization => {
            if node.domain == WorkDomain::Control {
                Ok(())
            } else {
                Err(ExecutionError::invalid_plan(format!(
                    "synchronization node {} must execute in the control domain",
                    node.id.as_str()
                )))
            }
        }
        WorkKind::DataCensus | WorkKind::ConvolutionFunction | WorkKind::Compute => Ok(()),
    }
}

pub(crate) fn io_buffer_kind_supports_work_kind(
    io_kind: crate::IoBufferKind,
    work_kind: WorkKind,
) -> bool {
    match io_kind {
        crate::IoBufferKind::SourceReadAhead => {
            matches!(
                work_kind,
                WorkKind::Prefetch
                    | WorkKind::Cache
                    | WorkKind::ObservationRead
                    | WorkKind::ObservationReadWriteback
                    | WorkKind::Release
            )
        }
        crate::IoBufferKind::Decode | crate::IoBufferKind::Preparation => {
            work_kind == WorkKind::Preparation
        }
        crate::IoBufferKind::HostToDeviceTransfer | crate::IoBufferKind::DeviceToHostTransfer => {
            work_kind == WorkKind::Transfer
        }
        crate::IoBufferKind::SpillRead => {
            matches!(work_kind, WorkKind::Spill | WorkKind::Prefetch)
        }
        crate::IoBufferKind::SpillWrite => {
            matches!(work_kind, WorkKind::Spill | WorkKind::ObservationRead)
        }
        crate::IoBufferKind::Serialization => work_kind == WorkKind::Serialization,
        crate::IoBufferKind::StorageManager => {
            matches!(
                work_kind,
                WorkKind::Io | WorkKind::Cache | WorkKind::Release
            )
        }
        crate::IoBufferKind::TiledColumnWriter | crate::IoBufferKind::ScalarColumnWriter => {
            work_kind == WorkKind::Io
        }
        crate::IoBufferKind::Writeback => {
            matches!(
                work_kind,
                WorkKind::Writeback | WorkKind::ObservationReadWriteback | WorkKind::Cache
            )
        }
        crate::IoBufferKind::Publication => work_kind == WorkKind::Publication,
        crate::IoBufferKind::MappedPageCache => {
            matches!(work_kind, WorkKind::Cache | WorkKind::Release)
        }
    }
}

fn validate_io_buffer_accounting(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    allocations: &BTreeMap<AllocationId, LogicalAllocation>,
    demand: crate::IoBufferDemand,
) -> Result<(), ExecutionError> {
    let mut used_kinds = BTreeSet::new();
    for node in nodes.values() {
        let mut claims = BTreeMap::<(crate::IoBufferKind, ClaimLifetime), u64>::new();
        for claim in &node.claims {
            let LeaseResource::IoBuffer(kind) = &claim.resource else {
                continue;
            };
            let amount = claims.entry((*kind, claim.lifetime.clone())).or_default();
            *amount = amount.checked_add(claim.amount).ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "work node {} I/O-buffer claims overflow",
                    node.id.as_str()
                ))
            })?;
            used_kinds.insert(*kind);
        }
        let mut uses = BTreeMap::<(crate::IoBufferKind, ClaimLifetime), u64>::new();
        for allocation_use in &node.allocations {
            let allocation = &allocations[&allocation_use.allocation];
            let AllocationPurpose::IoBuffer(kind) = allocation.purpose else {
                continue;
            };
            let amount = uses
                .entry((kind, allocation_use.lifetime.clone()))
                .or_default();
            *amount = amount.checked_add(allocation.bytes).ok_or_else(|| {
                ExecutionError::invalid_plan(format!(
                    "work node {} I/O-buffer allocation bytes overflow",
                    node.id.as_str()
                ))
            })?;
        }
        if claims != uses {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} must exactly match every typed I/O-buffer claim to logical allocation bytes and lifetime",
                node.id.as_str()
            )));
        }
    }
    if let Some(unused) = crate::IoBufferKind::ALL
        .into_iter()
        .find(|kind| demand.bytes(*kind) != 0 && !used_kinds.contains(kind))
    {
        return Err(ExecutionError::invalid_plan(format!(
            "selected resource demand reserves unused {unused:?} I/O-buffer bytes"
        )));
    }
    Ok(())
}

fn require_io_domain(node: &WorkNode) -> Result<(), ExecutionError> {
    if node.domain == WorkDomain::Io {
        Ok(())
    } else {
        Err(ExecutionError::invalid_plan(format!(
            "{:?} work node {} must execute in the I/O domain",
            node.kind,
            node.id.as_str()
        )))
    }
}

fn require_claim(
    node: &WorkNode,
    predicate: impl Fn(&LeaseResource) -> bool,
    label: &str,
) -> Result<(), ExecutionError> {
    if node.claims.iter().any(|claim| predicate(&claim.resource)) {
        Ok(())
    } else {
        Err(ExecutionError::invalid_plan(format!(
            "work node {} lacks a lease-attributed {label} claim",
            node.id.as_str()
        )))
    }
}

fn require_fence(node: &WorkNode, kind: FenceKind) -> Result<(), ExecutionError> {
    if node.fences.contains(&kind) {
        Ok(())
    } else {
        Err(ExecutionError::invalid_plan(format!(
            "work node {} lacks required {kind:?} fence",
            node.id.as_str()
        )))
    }
}

fn validate_acyclic(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
) -> Result<Vec<WorkNodeId>, ExecutionError> {
    let mut remaining_dependencies = nodes
        .iter()
        .map(|(id, node)| (id.clone(), node.dependencies.len()))
        .collect::<BTreeMap<_, _>>();
    let mut ready = remaining_dependencies
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(id.clone()))
        .collect::<BTreeSet<_>>();
    let mut order = Vec::with_capacity(nodes.len());
    while let Some(id) = ready.pop_first() {
        order.push(id.clone());
        for (candidate_id, candidate) in nodes {
            let edges = candidate
                .dependencies
                .iter()
                .filter(|dependency| dependency.predecessor() == &id)
                .count();
            if edges == 0 {
                continue;
            }
            let count = remaining_dependencies
                .get_mut(candidate_id)
                .expect("node ledger and dependency counts share keys");
            *count -= edges;
            if *count == 0 {
                ready.insert(candidate_id.clone());
            }
        }
    }
    if order.len() == nodes.len() {
        Ok(order)
    } else {
        Err(ExecutionError::invalid_plan(
            "work dependency graph contains a cycle",
        ))
    }
}

fn validate_allocations(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    allocations: &BTreeMap<AllocationId, LogicalAllocation>,
    slots: &BTreeMap<PhysicalSlotId, PhysicalSlot>,
    alternative: &DemandAlternative,
) -> Result<(), ExecutionError> {
    let memory_demands = alternative
        .demand
        .memory
        .iter()
        .map(|demand| (demand.allocation_id.as_str(), demand))
        .collect::<BTreeMap<_, _>>();
    if memory_demands.len() != alternative.demand.memory.len() {
        return Err(ExecutionError::invalid_plan(
            "memory demand identities must be unique",
        ));
    }
    let mut lease_resources = BTreeSet::new();
    let mut used_memory_demands = BTreeSet::new();
    for slot in slots.values() {
        validate_compatibility(&slot.compatibility)?;
        if slot.capacity_bytes == 0 {
            return Err(ExecutionError::invalid_plan(format!(
                "physical slot {} has zero capacity",
                slot.id.as_str()
            )));
        }
        let LeaseResource::Memory { allocation_id } = &slot.lease_resource else {
            return Err(ExecutionError::invalid_plan(format!(
                "physical slot {} is not attributed to lease memory",
                slot.id.as_str()
            )));
        };
        if !lease_resources.insert(&slot.lease_resource) {
            return Err(ExecutionError::invalid_plan(
                "physical slots must have distinct lease memory resources",
            ));
        }
        let demand = memory_demands.get(allocation_id.as_str()).ok_or_else(|| {
            ExecutionError::invalid_plan(format!(
                "physical slot {} references undeclared memory demand {allocation_id}",
                slot.id.as_str()
            ))
        })?;
        used_memory_demands.insert(allocation_id.as_str());
        if slot.capacity_bytes != demand.hard_bytes
            || slot.compatibility.views != demand.views.iter().cloned().collect::<BTreeSet<_>>()
        {
            return Err(ExecutionError::invalid_plan(format!(
                "physical slot {} does not exactly match its memory demand",
                slot.id.as_str()
            )));
        }
    }
    if used_memory_demands.len() != memory_demands.len() {
        return Err(ExecutionError::invalid_plan(
            "every declared memory demand must belong to one physical slot",
        ));
    }
    for node in nodes.values() {
        if node
            .claims
            .iter()
            .any(|claim| matches!(claim.resource, LeaseResource::Memory { .. }))
        {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} duplicates plan-owned allocation accounting with a memory claim",
                node.id.as_str()
            )));
        }
    }

    let mut slot_allocations = BTreeMap::<PhysicalSlotId, BTreeSet<AllocationId>>::new();
    let mut allocation_uses = BTreeMap::<AllocationId, Vec<(&WorkNode, &AllocationUse)>>::new();
    let mut used_allocations = BTreeSet::new();
    let mut used_slots = BTreeSet::new();
    for allocation in allocations.values() {
        validate_compatibility(&allocation.compatibility)?;
        if allocation.bytes == 0 {
            return Err(ExecutionError::invalid_plan(format!(
                "logical allocation {} has zero bytes",
                allocation.id.as_str()
            )));
        }
        let slot = slots.get(&allocation.physical_slot).ok_or_else(|| {
            ExecutionError::invalid_plan(format!(
                "logical allocation {} references unknown physical slot {}",
                allocation.id.as_str(),
                allocation.physical_slot.as_str()
            ))
        })?;
        if allocation.bytes > slot.capacity_bytes || allocation.compatibility != slot.compatibility
        {
            return Err(ExecutionError::invalid_plan(format!(
                "logical allocation {} is incompatible with physical slot {}",
                allocation.id.as_str(),
                slot.id.as_str()
            )));
        }
        let Some(acquire_node) = nodes.get(&allocation.lifetime.acquire_at) else {
            return Err(ExecutionError::invalid_plan(format!(
                "logical allocation {} has an unknown acquisition node",
                allocation.id.as_str()
            )));
        };
        if !acquire_node
            .allocations
            .iter()
            .any(|allocation_use| allocation_use.allocation == allocation.id)
        {
            return Err(ExecutionError::invalid_plan(format!(
                "logical allocation {} is not used by its acquisition node {}",
                allocation.id.as_str(),
                acquire_node.id.as_str()
            )));
        }
        if allocation.lifetime.release_after.is_empty() {
            return Err(ExecutionError::invalid_plan(format!(
                "logical allocation {} has no terminal release event",
                allocation.id.as_str()
            )));
        }
        for release in &allocation.lifetime.release_after {
            validate_declared_event(nodes, release)?;
            if !event_precedes(
                nodes,
                &WorkDependency::Work(acquire_node.id.clone()),
                release,
            ) {
                return Err(ExecutionError::invalid_plan(format!(
                    "logical allocation {} has a release event before its acquisition work",
                    allocation.id.as_str()
                )));
            }
        }
        slot_allocations
            .entry(slot.id.clone())
            .or_default()
            .insert(allocation.id.clone());
        used_slots.insert(slot.id.clone());
    }
    for node in nodes.values() {
        let mut node_slots = BTreeSet::new();
        for allocation_use in &node.allocations {
            let allocation = &allocations[&allocation_use.allocation];
            match &node.domain {
                WorkDomain::Cpu | WorkDomain::Io => {
                    if !allocation
                        .compatibility
                        .views
                        .contains(&alternative.demand.host_memory_view)
                    {
                        return Err(ExecutionError::invalid_plan(format!(
                            "work node {} uses logical allocation {} without the selected host-memory view",
                            node.id.as_str(),
                            allocation.id.as_str()
                        )));
                    }
                }
                WorkDomain::Metal { .. } => {
                    if allocation.compatibility.storage_mode != StorageMode::MetalShared {
                        return Err(ExecutionError::invalid_plan(format!(
                            "Metal work node {} uses non-shared logical allocation {}",
                            node.id.as_str(),
                            allocation.id.as_str()
                        )));
                    }
                }
                WorkDomain::Control => {
                    return Err(ExecutionError::invalid_plan(format!(
                        "control work node {} cannot use a logical allocation",
                        node.id.as_str()
                    )));
                }
            }
            if !node_slots.insert(allocation.physical_slot.clone()) {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} uses multiple logical allocations assigned to physical slot {}",
                    node.id.as_str(),
                    allocation.physical_slot.as_str()
                )));
            }
            if node.id != allocation.lifetime.acquire_at
                && !event_precedes(
                    nodes,
                    &WorkDependency::Work(allocation.lifetime.acquire_at.clone()),
                    &WorkDependency::Work(node.id.clone()),
                )
            {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} can use logical allocation {} before it is acquired",
                    node.id.as_str(),
                    allocation.id.as_str()
                )));
            }
            for use_end in local_use_end_events(node, &allocation_use.lifetime) {
                if !allocation
                    .lifetime
                    .release_after
                    .iter()
                    .any(|release| event_precedes(nodes, &use_end, release))
                {
                    return Err(ExecutionError::invalid_plan(format!(
                        "logical allocation {} can be released before work node {} finishes using it",
                        allocation.id.as_str(),
                        node.id.as_str()
                    )));
                }
            }
            allocation_uses
                .entry(allocation.id.clone())
                .or_default()
                .push((node, allocation_use));
            used_allocations.insert(allocation.id.clone());
        }
    }
    for allocation in allocations.values() {
        let Some(uses) = allocation_uses.get(&allocation.id) else {
            continue;
        };
        let has_release_use = uses.iter().any(|(node, _)| node.kind == WorkKind::Release);
        if has_release_use
            || matches!(
                allocation.purpose,
                AllocationPurpose::IoBuffer(
                    crate::IoBufferKind::StorageManager | crate::IoBufferKind::MappedPageCache
                )
            )
        {
            validate_external_release(nodes, allocation, uses)?;
        }
        if allocation.compatibility.access == AllocationAccess::ReadOnly {
            if allocation.compatibility.initialization != InitializationPolicy::Preserve {
                let Some(acquisition) = uses
                    .iter()
                    .find(|(node, _)| node.id == allocation.lifetime.acquire_at)
                else {
                    return Err(ExecutionError::invalid_plan(format!(
                        "logical allocation {} is absent from its acquisition node",
                        allocation.id.as_str()
                    )));
                };
                for use_after_initialization in uses
                    .iter()
                    .filter(|(node, _)| node.id != allocation.lifetime.acquire_at)
                {
                    if !allocation_use_precedes(nodes, *acquisition, *use_after_initialization) {
                        return Err(ExecutionError::invalid_plan(format!(
                            "logical allocation {} can be read before asynchronous initialization finishes",
                            allocation.id.as_str()
                        )));
                    }
                }
            }
            continue;
        }
        for (index, left) in uses.iter().enumerate() {
            for right in &uses[index + 1..] {
                if !allocation_use_precedes(nodes, *left, *right)
                    && !allocation_use_precedes(nodes, *right, *left)
                {
                    return Err(ExecutionError::invalid_plan(format!(
                        "logical allocation {} has unordered mutable uses at work nodes {} and {}",
                        allocation.id.as_str(),
                        left.0.id.as_str(),
                        right.0.id.as_str()
                    )));
                }
            }
        }
    }
    for (slot_id, allocation_ids) in slot_allocations {
        if allocation_ids.len() > 1
            && slots[&slot_id].compatibility.initialization == InitializationPolicy::Preserve
        {
            return Err(ExecutionError::invalid_plan(format!(
                "physical slot {} cannot Preserve contents while serving multiple logical allocations",
                slot_id.as_str()
            )));
        }
        let allocation_ids = allocation_ids.into_iter().collect::<Vec<_>>();
        for (index, left_id) in allocation_ids.iter().enumerate() {
            for right_id in &allocation_ids[index + 1..] {
                let left = &allocations[left_id];
                let right = &allocations[right_id];
                if !allocation_precedes(nodes, left, right)
                    && !allocation_precedes(nodes, right, left)
                {
                    return Err(ExecutionError::invalid_plan(format!(
                        "physical slot {} does not have strictly ordered logical allocation lifetimes {} and {}",
                        slot_id.as_str(),
                        left.id.as_str(),
                        right.id.as_str()
                    )));
                }
            }
        }
    }
    if used_allocations.len() != allocations.len() || used_slots.len() != slots.len() {
        return Err(ExecutionError::invalid_plan(
            "allocation and physical-slot ledgers must contain only used entries",
        ));
    }
    Ok(())
}

fn validate_external_release(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    allocation: &LogicalAllocation,
    uses: &[(&WorkNode, &AllocationUse)],
) -> Result<(), ExecutionError> {
    let release_uses = uses
        .iter()
        .copied()
        .filter(|(node, _)| node.kind == WorkKind::Release)
        .collect::<Vec<_>>();
    let [release] = release_uses.as_slice() else {
        return Err(ExecutionError::invalid_plan(format!(
            "logical allocation {} requires exactly one terminal release use for externally retained storage",
            allocation.id.as_str()
        )));
    };
    let terminal_events = local_use_end_events(release.0, &release.1.lifetime)
        .into_iter()
        .collect::<BTreeSet<_>>();
    if allocation.lifetime.release_after != terminal_events
        || uses.iter().copied().any(|use_| {
            use_.0.kind != WorkKind::Release && !allocation_use_precedes(nodes, use_, *release)
        })
    {
        return Err(ExecutionError::invalid_plan(format!(
            "logical allocation {} requires terminal release work strictly after every other use",
            allocation.id.as_str()
        )));
    }
    Ok(())
}

fn allocation_precedes(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    left: &LogicalAllocation,
    right: &LogicalAllocation,
) -> bool {
    let right_acquire = WorkDependency::Work(right.lifetime.acquire_at.clone());
    left.lifetime
        .release_after
        .iter()
        .all(|release| event_strictly_precedes(nodes, release, &right_acquire))
}

fn event_strictly_precedes(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    before: &WorkDependency,
    after: &WorkDependency,
) -> bool {
    before != after && event_precedes(nodes, before, after)
}

fn allocation_use_precedes(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    before: (&WorkNode, &AllocationUse),
    after: (&WorkNode, &AllocationUse),
) -> bool {
    let after_start = WorkDependency::Work(after.0.id.clone());
    local_use_end_events(before.0, &before.1.lifetime)
        .iter()
        .all(|end| event_precedes(nodes, end, &after_start))
}

fn local_use_end_events(node: &WorkNode, lifetime: &ClaimLifetime) -> Vec<WorkDependency> {
    match lifetime {
        ClaimLifetime::Work => vec![WorkDependency::Work(node.id.clone())],
        ClaimLifetime::Fences(kinds) => kinds
            .iter()
            .map(|kind| WorkDependency::Fence(FenceId::new(node.id.clone(), *kind)))
            .collect(),
        ClaimLifetime::RetainedUntil(release) => {
            vec![WorkDependency::Work(release.clone())]
        }
        ClaimLifetime::Artifact => {
            unreachable!("artifact lifetime is rejected for logical allocations")
        }
    }
}

fn validate_declared_event(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    event: &WorkDependency,
) -> Result<(), ExecutionError> {
    let node = nodes.get(event.predecessor()).ok_or_else(|| {
        ExecutionError::invalid_plan(format!(
            "allocation lifetime references unknown work node {}",
            event.predecessor().as_str()
        ))
    })?;
    if let WorkDependency::Fence(fence) = event
        && !node.fences.contains(&fence.kind())
    {
        return Err(ExecutionError::invalid_plan(format!(
            "allocation lifetime references undeclared {:?} fence from {}",
            fence.kind(),
            node.id.as_str()
        )));
    }
    Ok(())
}

fn event_precedes(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    before: &WorkDependency,
    after: &WorkDependency,
) -> bool {
    if before == after {
        return true;
    }
    let mut visited = BTreeSet::from([before.clone()]);
    let mut frontier = vec![before.clone()];
    while let Some(event) = frontier.pop() {
        let mut successors = Vec::new();
        match &event {
            WorkDependency::Work(node_id) => {
                successors.extend(
                    nodes[node_id]
                        .fences
                        .iter()
                        .map(|kind| WorkDependency::Fence(FenceId::new(node_id.clone(), *kind))),
                );
            }
            WorkDependency::Fence(_) => {}
        }
        successors.extend(
            nodes
                .values()
                .filter(|node| node.dependencies.contains(&event))
                .map(|node| WorkDependency::Work(node.id.clone())),
        );
        for successor in successors {
            if &successor == after {
                return true;
            }
            if visited.insert(successor.clone()) {
                frontier.push(successor);
            }
        }
    }
    false
}

fn validate_compatibility(compatibility: &SlotCompatibility) -> Result<(), ExecutionError> {
    if compatibility.views.is_empty()
        || compatibility.alignment_bytes == 0
        || !compatibility.alignment_bytes.is_power_of_two()
        || compatibility.layout.as_str().is_empty()
    {
        return Err(ExecutionError::invalid_plan(
            "slot compatibility requires views, power-of-two alignment, and layout identity",
        ));
    }
    Ok(())
}

fn validate_adaptations(
    initial: &ExecutionKnobs,
    adaptations: &BTreeMap<AdaptationId, AdaptationTransition>,
    alternative: &DemandAlternative,
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    topological_order: &[WorkNodeId],
) -> Result<(), ExecutionError> {
    validate_knob_envelope(initial, alternative, nodes)?;
    validate_mandatory_claims(initial, nodes.values())?;
    for node in nodes.values() {
        if !node
            .quiescence_after
            .is_subset(&alternative.quiescence_points)
        {
            return Err(ExecutionError::invalid_plan(format!(
                "work node {} declares a quiescence point absent from its resource alternative",
                node.id.as_str()
            )));
        }
        if !node.quiescence_after.is_empty() {
            if node
                .quiescence_after
                .contains(&QuiescencePoint::RunBoundary)
            {
                return Err(ExecutionError::invalid_plan(format!(
                    "work node {} cannot declare the whole-run boundary",
                    node.id.as_str()
                )));
            }
            validate_quiescence_marker(nodes, node)?;
        }
    }
    for transition in adaptations.values() {
        if transition.from == transition.to {
            return Err(ExecutionError::invalid_plan(format!(
                "adaptation {} does not change execution configuration",
                transition.id.as_str()
            )));
        }
        validate_knob_envelope(&transition.from, alternative, nodes)?;
        validate_knob_envelope(&transition.to, alternative, nodes)?;
    }
    let mut boundary_occurrences = if alternative
        .quiescence_points
        .contains(&QuiescencePoint::RunBoundary)
    {
        vec![(BTreeSet::from([QuiescencePoint::RunBoundary]), None)]
    } else {
        Vec::new()
    };
    boundary_occurrences.extend(topological_order.iter().filter_map(|node_id| {
        let points = &nodes[node_id].quiescence_after;
        (!points.is_empty()).then(|| (points.clone(), Some(node_id.clone())))
    }));
    let mut reachable_configurations = BTreeSet::from([initial.clone()]);
    let mut reachable_transitions = BTreeSet::new();
    for (points, boundary_node) in boundary_occurrences {
        let entering_configurations = reachable_configurations.clone();
        for transition in adaptations.values() {
            if entering_configurations.contains(&transition.from) && points.contains(&transition.at)
            {
                validate_mandatory_claims(
                    &transition.to,
                    nodes_remaining_after(nodes, boundary_node.as_ref()),
                )?;
                reachable_configurations.insert(transition.to.clone());
                reachable_transitions.insert(transition.id.clone());
            }
        }
    }
    if reachable_transitions.len() != adaptations.len() {
        let Some(unreachable) = adaptations
            .keys()
            .find(|id| !reachable_transitions.contains(*id))
        else {
            return Err(ExecutionError::invalid_plan(
                "adaptation reachability ledger is inconsistent",
            ));
        };
        return Err(ExecutionError::invalid_plan(format!(
            "adaptation {} is unreachable at every declared boundary",
            unreachable.as_str()
        )));
    }
    Ok(())
}

fn validate_quiescence_marker(
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
    boundary: &WorkNode,
) -> Result<(), ExecutionError> {
    if boundary.kind != WorkKind::Synchronization || boundary.domain != WorkDomain::Control {
        return Err(ExecutionError::invalid_plan(format!(
            "quiescence marker {} must be a resource-free synchronization node",
            boundary.id.as_str()
        )));
    }
    let boundary_event = WorkDependency::Work(boundary.id.clone());
    for node in nodes.values().filter(|node| node.id != boundary.id) {
        let node_event = WorkDependency::Work(node.id.clone());
        let before_boundary = event_precedes(nodes, &node_event, &boundary_event);
        let after_boundary = event_precedes(nodes, &boundary_event, &node_event);
        if !before_boundary && !after_boundary {
            return Err(ExecutionError::invalid_plan(format!(
                "quiescence marker {} does not form a global execution cut with work node {}",
                boundary.id.as_str(),
                node.id.as_str()
            )));
        }
        if before_boundary
            && node.fences.iter().any(|kind| {
                !event_precedes(
                    nodes,
                    &WorkDependency::Fence(FenceId::new(node.id.clone(), *kind)),
                    &boundary_event,
                )
            })
        {
            return Err(ExecutionError::invalid_plan(format!(
                "quiescence marker {} can be reached before every fence from work node {}",
                boundary.id.as_str(),
                node.id.as_str()
            )));
        }
    }
    Ok(())
}

fn nodes_remaining_after<'node>(
    nodes: &'node BTreeMap<WorkNodeId, WorkNode>,
    boundary: Option<&WorkNodeId>,
) -> Vec<&'node WorkNode> {
    let Some(boundary) = boundary else {
        return nodes.values().collect();
    };
    nodes
        .values()
        .filter(|candidate| {
            boundary != &candidate.id
                && event_precedes(
                    nodes,
                    &WorkDependency::Work(boundary.clone()),
                    &WorkDependency::Work(candidate.id.clone()),
                )
        })
        .collect()
}

fn validate_mandatory_claims<'node>(
    knobs: &ExecutionKnobs,
    nodes: impl IntoIterator<Item = &'node WorkNode>,
) -> Result<(), ExecutionError> {
    for node in nodes {
        let workers = claimed_by(node, |resource| matches!(resource, LeaseResource::Workers))?;
        let cache = claimed_by(node, |resource| {
            matches!(resource, LeaseResource::ResidentCache)
        })?;
        if workers > knobs.workers
            || cache > knobs.cache_retention_bytes
            || (node.domain == WorkDomain::Io && knobs.io_depth == 0)
        {
            return Err(ExecutionError::invalid_plan(format!(
                "execution configuration cannot satisfy mandatory claims for work node {}",
                node.id.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_knob_envelope(
    knobs: &ExecutionKnobs,
    alternative: &DemandAlternative,
    nodes: &BTreeMap<WorkNodeId, WorkNode>,
) -> Result<(), ExecutionError> {
    if knobs.workers < alternative.scaling.minimum_workers
        || knobs.workers > alternative.scaling.maximum_workers
        || knobs.workers > alternative.demand.workers.hard()
        || knobs.batch_size == 0
        || knobs.batch_size > alternative.scaling.maximum_batch_size
        || knobs.tile_width == 0
        || knobs.tile_width > alternative.scaling.maximum_tile_width
        || knobs.tile_height == 0
        || knobs.tile_height > alternative.scaling.maximum_tile_height
        || knobs.slab_depth == 0
        || knobs.slab_depth > alternative.scaling.maximum_slab_depth
        || knobs.io_depth == 0
        || knobs.cache_retention_bytes > alternative.demand.caches.hard_resident_bytes
    {
        return Err(ExecutionError::invalid_plan(
            "execution configuration exceeds selected hard bounds",
        ));
    }
    if knobs.fusion || knobs.recomputation {
        return Err(ExecutionError::invalid_plan(
            "fusion and recomputation require a sealed alternate node configuration",
        ));
    }
    if knobs.spill && !nodes.values().any(|node| node.kind == WorkKind::Spill) {
        return Err(ExecutionError::invalid_plan(
            "spill adaptation lacks an exact declared spill work node and resources",
        ));
    }
    if knobs.prefetch && !nodes.values().any(|node| node.kind == WorkKind::Prefetch) {
        return Err(ExecutionError::invalid_plan(
            "prefetch adaptation lacks an exact declared prefetch work node and resources",
        ));
    }
    Ok(())
}

/// Plan validation, scheduling, or resource-accounting failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionError {
    /// The declarative plan violated a structural or safety invariant.
    InvalidPlan(String),
    /// Resource Authority admission or permit accounting failed.
    Resource(ResourceError),
    /// A scheduler operation addressed work in the wrong state.
    InvalidState(String),
    /// A controller requested a transition outside the current exact cut.
    IneligibleAdaptation {
        /// Requested transition.
        requested: AdaptationId,
        /// Transitions eligible at the current globally idle cut.
        eligible: Vec<AdaptationId>,
    },
    /// No pending work can make progress and no work or fence can unblock it.
    Deadlock,
}

impl ExecutionError {
    fn invalid_plan(message: impl Into<String>) -> Self {
        Self::InvalidPlan(message.into())
    }

    fn invalid_state(message: impl Into<String>) -> Self {
        Self::InvalidState(message.into())
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan(message) => write!(formatter, "invalid execution plan: {message}"),
            Self::Resource(error) => write!(formatter, "execution resource failure: {error}"),
            Self::InvalidState(message) => write!(formatter, "invalid scheduler state: {message}"),
            Self::IneligibleAdaptation {
                requested,
                eligible,
            } => write!(
                formatter,
                "adaptation {} is not eligible at the current execution cut; eligible={eligible:?}",
                requested.as_str()
            ),
            Self::Deadlock => formatter.write_str("execution plan cannot make progress"),
        }
    }
}

impl Error for ExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Resource(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ResourceError> for ExecutionError {
    fn from(error: ResourceError) -> Self {
        Self::Resource(error)
    }
}

#[cfg(test)]
mod tests;
