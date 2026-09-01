// SPDX-License-Identifier: LGPL-3.0-or-later

//! Planner-owned Apple Metal device, residency, and command-fence runtime.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
#[cfg(all(target_os = "macos", not(coverage)))]
use std::sync::{Arc, Mutex};
#[cfg(all(target_os = "macos", not(coverage)))]
use std::time::Instant;

use crate::{
    AcceleratorId, AcceleratorKind, AllocationId, ArtifactMeasurement, CapacityDomainId,
    CapacityViewId, ExecutionAttemptId, ExecutionDag, FenceKind, IoBufferKind, IoMeasurement,
    LeaseResource, MemoryCapacityKind, PhysicalSlotId, QueueResourceId, ResourceMeasurement,
    ResourceTopology, RuntimeOverheadDemand, StorageMode, TransferLinkId, WorkDomain,
    WorkExecutionContext, WorkKind, WorkMeasurements, WorkNodeId,
};

#[cfg(all(target_os = "macos", not(coverage)))]
use objc2::rc::Retained;
#[cfg(all(target_os = "macos", not(coverage)))]
use objc2::runtime::ProtocolObject;
#[cfg(all(target_os = "macos", not(coverage)))]
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandQueue,
    MTLCreateSystemDefaultDevice, MTLDevice, MTLResourceOptions,
};

#[cfg(all(target_os = "macos", not(coverage)))]
#[link(name = "CoreGraphics", kind = "framework")]
unsafe extern "C" {}

/// Runtime-owned physical facts for the one supported Metal device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetalRuntimeInventory {
    accelerator: AcceleratorId,
    memory_domain: CapacityDomainId,
    memory_view: CapacityViewId,
    command_queue: QueueResourceId,
    recommended_working_set_bytes: u64,
    maximum_buffer_bytes: u64,
}

impl MetalRuntimeInventory {
    /// Return the Resource Authority accelerator identity.
    #[must_use]
    pub const fn accelerator(&self) -> &AcceleratorId {
        &self.accelerator
    }

    /// Return the sole physical capacity domain shared with the host.
    #[must_use]
    pub const fn memory_domain(&self) -> &CapacityDomainId {
        &self.memory_domain
    }

    /// Return the Metal view of the unified capacity domain.
    #[must_use]
    pub const fn memory_view(&self) -> &CapacityViewId {
        &self.memory_view
    }

    /// Return the runtime-owned command-queue identity.
    #[must_use]
    pub const fn command_queue(&self) -> &QueueResourceId {
        &self.command_queue
    }

    /// Return Metal's recommended maximum resident working set.
    #[must_use]
    pub const fn recommended_working_set_bytes(&self) -> u64 {
        self.recommended_working_set_bytes
    }

    /// Return the maximum length supported by one Metal buffer.
    #[must_use]
    pub const fn maximum_buffer_bytes(&self) -> u64 {
        self.maximum_buffer_bytes
    }
}

/// One plan-selected Metal work node and its runtime-owned resource identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetalNodeDecision {
    node: WorkNodeId,
    kind: WorkKind,
    demand_id: String,
    allocations: Vec<AllocationId>,
}

impl MetalNodeDecision {
    /// Return the exact work node selected by the immutable execution DAG.
    #[must_use]
    pub const fn node(&self) -> &WorkNodeId {
        &self.node
    }

    /// Return the declared kind without adding mode or science interpretation.
    #[must_use]
    pub const fn kind(&self) -> WorkKind {
        self.kind
    }

    /// Return the accelerator-demand identity charged by this command.
    #[must_use]
    pub fn demand_id(&self) -> &str {
        &self.demand_id
    }

    /// Return logical allocations visible to this command.
    #[must_use]
    pub fn allocations(&self) -> &[AllocationId] {
        &self.allocations
    }
}

/// Closed Metal runtime decision derived only from an already validated execution DAG.
#[derive(Debug, PartialEq, Eq)]
pub struct MetalExecutionDecision {
    inventory: MetalRuntimeInventory,
    nodes: BTreeMap<WorkNodeId, MetalNodeDecision>,
    allocation_slots: BTreeMap<AllocationId, PhysicalSlotId>,
    physical_slots: BTreeMap<PhysicalSlotId, u64>,
    transfers: Vec<TransferLinkId>,
    host_to_device_staging_bytes: u64,
    device_to_host_staging_bytes: u64,
    resident_cache_bytes: u64,
    overhead: RuntimeOverheadDemand,
}

impl MetalExecutionDecision {
    /// Bind a Metal runtime decision to exact plan and Resource Authority topology facts.
    pub fn bind(
        plan: &ExecutionDag,
        topology: &ResourceTopology,
    ) -> Result<Self, MetalRuntimeError> {
        let metal_accelerators = topology
            .accelerators
            .iter()
            .filter(|accelerator| accelerator.kind == AcceleratorKind::Metal)
            .collect::<Vec<_>>();
        let [accelerator] = metal_accelerators.as_slice() else {
            return Err(MetalRuntimeError::Ineligible(
                "the initial Metal runtime requires exactly one inventoried Metal device"
                    .to_string(),
            ));
        };
        let view = topology
            .memory_views
            .iter()
            .find(|view| view.id == accelerator.memory_view)
            .ok_or_else(|| {
                MetalRuntimeError::InvalidPlan(
                    "Metal accelerator names a missing memory view".to_string(),
                )
            })?;
        let domain = topology
            .memory_domains
            .iter()
            .find(|domain| domain.id == view.domain)
            .ok_or_else(|| {
                MetalRuntimeError::InvalidPlan(
                    "Metal memory view names a missing capacity domain".to_string(),
                )
            })?;
        if domain.kind != MemoryCapacityKind::Unified {
            return Err(MetalRuntimeError::Ineligible(
                "Metal execution requires one host/device unified capacity domain".to_string(),
            ));
        }
        let queue = topology
            .queue_resources
            .iter()
            .find(|queue| queue.id == accelerator.command_queue)
            .ok_or_else(|| {
                MetalRuntimeError::InvalidPlan(
                    "Metal accelerator names a missing command queue".to_string(),
                )
            })?;
        if queue.slots == 0 {
            return Err(MetalRuntimeError::Ineligible(
                "Metal command queue has no available occupancy".to_string(),
            ));
        }

        let mut nodes = BTreeMap::new();
        let mut used_allocations = BTreeSet::new();
        for node in plan.nodes().values() {
            let WorkDomain::Metal { demand_id } = &node.domain else {
                continue;
            };
            if !matches!(
                node.kind,
                WorkKind::Preparation | WorkKind::Jit | WorkKind::Compute
            ) {
                return Err(MetalRuntimeError::InvalidPlan(format!(
                    "Metal node {} has runtime-incompatible work kind {:?}",
                    node.id.as_str(),
                    node.kind
                )));
            }
            if node.fences != BTreeSet::from([FenceKind::Device]) {
                return Err(MetalRuntimeError::InvalidPlan(format!(
                    "Metal node {} must declare exactly one device fence",
                    node.id.as_str()
                )));
            }
            let demand = plan
                .resource_alternative()
                .demand
                .accelerators
                .iter()
                .find(|candidate| candidate.demand_id == *demand_id)
                .ok_or_else(|| {
                    MetalRuntimeError::InvalidPlan(format!(
                        "Metal node {} names absent accelerator demand {demand_id}",
                        node.id.as_str()
                    ))
                })?;
            if demand.accelerator != accelerator.id
                || demand.slots.hard() == 0
                || demand.command_queue_slots.hard() == 0
            {
                return Err(MetalRuntimeError::Ineligible(format!(
                    "Metal node {} is not charged to the inventoried device and command queue",
                    node.id.as_str()
                )));
            }
            let allocations = node
                .allocations
                .iter()
                .map(|use_| use_.allocation.clone())
                .collect::<Vec<_>>();
            used_allocations.extend(allocations.iter().cloned());
            nodes.insert(
                node.id.clone(),
                MetalNodeDecision {
                    node: node.id.clone(),
                    kind: node.kind,
                    demand_id: demand_id.clone(),
                    allocations,
                },
            );
        }
        if nodes.is_empty() {
            return Err(MetalRuntimeError::Ineligible(
                "execution DAG contains no plan-selected Metal work".to_string(),
            ));
        }

        let overhead = plan.resource_alternative().demand.overhead;
        if overhead.driver_bytes == 0 || overhead.command_buffer_bytes == 0 {
            return Err(MetalRuntimeError::InvalidPlan(
                "Metal work requires positive driver and command-buffer envelopes".to_string(),
            ));
        }
        if nodes.values().any(|node| node.kind == WorkKind::Jit) && overhead.jit_bytes == 0 {
            return Err(MetalRuntimeError::InvalidPlan(
                "Metal JIT work requires a positive JIT envelope".to_string(),
            ));
        }

        let mut allocation_slots = BTreeMap::new();
        let mut physical_slots = BTreeMap::new();
        for allocation_id in used_allocations {
            let allocation = &plan.logical_allocations()[&allocation_id];
            let slot = &plan.physical_slots()[&allocation.physical_slot];
            if slot.compatibility.storage_mode != StorageMode::MetalShared
                || slot.compatibility.memory_domain != domain.id
                || !slot.compatibility.views.contains(&accelerator.memory_view)
            {
                return Err(MetalRuntimeError::InvalidPlan(format!(
                    "Metal allocation {} is not backed by the inventoried unified memory view",
                    allocation_id.as_str()
                )));
            }
            physical_slots
                .entry(slot.id.clone())
                .and_modify(|bytes: &mut u64| *bytes = (*bytes).max(slot.capacity_bytes))
                .or_insert(slot.capacity_bytes);
            allocation_slots.insert(allocation_id, slot.id.clone());
        }
        let residency_bytes = physical_slots.values().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(*bytes)
                .ok_or(MetalRuntimeError::Overflow("Metal residency"))
        })?;
        if residency_bytes > domain.capacity_bytes {
            return Err(MetalRuntimeError::Ineligible(format!(
                "Metal residency {residency_bytes} exceeds unified capacity {}",
                domain.capacity_bytes
            )));
        }

        let io = plan.resource_alternative().demand.io_buffers;
        let transfers = plan
            .resource_alternative()
            .demand
            .transfers
            .iter()
            .map(|demand| {
                let link = topology
                    .transfer_links
                    .iter()
                    .find(|link| link.id == demand.link)
                    .ok_or_else(|| {
                        MetalRuntimeError::InvalidPlan(format!(
                            "Metal transfer demand {} names an absent link",
                            demand.demand_id
                        ))
                    })?;
                if link.source_view != accelerator.memory_view
                    && link.destination_view != accelerator.memory_view
                {
                    return Err(MetalRuntimeError::InvalidPlan(format!(
                        "Metal transfer demand {} does not reach the selected device view",
                        demand.demand_id
                    )));
                }
                Ok(link.id.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if (io.bytes(IoBufferKind::HostToDeviceTransfer) > 0
            || io.bytes(IoBufferKind::DeviceToHostTransfer) > 0)
            && transfers.is_empty()
        {
            return Err(MetalRuntimeError::InvalidPlan(
                "Metal staging bytes require an explicit transfer-link demand".to_string(),
            ));
        }
        Ok(Self {
            inventory: MetalRuntimeInventory {
                accelerator: accelerator.id.clone(),
                memory_domain: domain.id.clone(),
                memory_view: accelerator.memory_view.clone(),
                command_queue: accelerator.command_queue.clone(),
                recommended_working_set_bytes: domain.capacity_bytes,
                maximum_buffer_bytes: domain.capacity_bytes,
            },
            nodes,
            allocation_slots,
            physical_slots,
            transfers,
            host_to_device_staging_bytes: io.bytes(IoBufferKind::HostToDeviceTransfer),
            device_to_host_staging_bytes: io.bytes(IoBufferKind::DeviceToHostTransfer),
            resident_cache_bytes: plan
                .resource_alternative()
                .demand
                .caches
                .hard_resident_bytes,
            overhead,
        })
    }

    /// Return the exact runtime inventory selected by this plan.
    #[must_use]
    pub const fn inventory(&self) -> &MetalRuntimeInventory {
        &self.inventory
    }

    /// Return the exact plan-selected Metal nodes.
    #[must_use]
    pub const fn nodes(&self) -> &BTreeMap<WorkNodeId, MetalNodeDecision> {
        &self.nodes
    }

    /// Return unique physical slots, charging unified capacity once per slot.
    #[must_use]
    pub const fn physical_slots(&self) -> &BTreeMap<PhysicalSlotId, u64> {
        &self.physical_slots
    }

    /// Return the sum of unique physical-slot capacities.
    #[must_use]
    pub fn residency_bytes(&self) -> u64 {
        self.physical_slots.values().sum()
    }

    /// Return plan-listed transfer-link identities.
    #[must_use]
    pub fn transfers(&self) -> &[TransferLinkId] {
        &self.transfers
    }

    /// Return bounded host-to-device staging bytes.
    #[must_use]
    pub const fn host_to_device_staging_bytes(&self) -> u64 {
        self.host_to_device_staging_bytes
    }

    /// Return bounded device-to-host staging bytes.
    #[must_use]
    pub const fn device_to_host_staging_bytes(&self) -> u64 {
        self.device_to_host_staging_bytes
    }

    /// Return plan-selected resident cache bytes.
    #[must_use]
    pub const fn resident_cache_bytes(&self) -> u64 {
        self.resident_cache_bytes
    }

    /// Return driver, JIT, command-buffer, and other runtime envelopes.
    #[must_use]
    pub const fn overhead(&self) -> RuntimeOverheadDemand {
        self.overhead
    }
}

/// Runtime-owned buffers for one immutable plan decision.
struct MetalResidency {
    #[cfg(all(target_os = "macos", not(coverage)))]
    _buffers: BTreeMap<PhysicalSlotId, Retained<ProtocolObject<dyn MTLBuffer>>>,
}

impl fmt::Debug for MetalResidency {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetalResidency")
            .finish_non_exhaustive()
    }
}

/// Opaque runtime that owns one Metal device and command queue.
pub struct MetalRuntime {
    decision: MetalExecutionDecision,
    attempt_id: ExecutionAttemptId,
    lease_epoch: u64,
    submitted: BTreeSet<WorkNodeId>,
    #[cfg(all(target_os = "macos", not(coverage)))]
    core: Arc<MetalRuntimeCore>,
    #[cfg(not(all(target_os = "macos", not(coverage))))]
    inventory: MetalRuntimeInventory,
}

#[cfg(all(target_os = "macos", not(coverage)))]
struct MetalRuntimeCore {
    inventory: MetalRuntimeInventory,
    active: Mutex<Option<WorkNodeId>>,
    #[cfg(all(target_os = "macos", not(coverage)))]
    device: Retained<ProtocolObject<dyn MTLDevice>>,
    #[cfg(all(target_os = "macos", not(coverage)))]
    queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
}

impl fmt::Debug for MetalRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetalRuntime")
            .field("inventory", self.inventory())
            .finish_non_exhaustive()
    }
}

impl MetalRuntime {
    /// Consume one immutable decision under a live scheduler-issued Metal capability.
    pub fn open(
        decision: MetalExecutionDecision,
        context: WorkExecutionContext<'_>,
    ) -> Result<Self, MetalRuntimeError> {
        validate_execution_context(&decision, context)?;
        if !context.claim_metal_runtime() {
            return Err(MetalRuntimeError::RuntimeAlreadyOpened);
        }
        open_platform_runtime(decision, context.attempt_id(), context.lease_epoch())
    }

    /// Return the detected device facts checked against the immutable plan.
    #[must_use]
    pub fn inventory(&self) -> &MetalRuntimeInventory {
        #[cfg(all(target_os = "macos", not(coverage)))]
        {
            &self.core.inventory
        }
        #[cfg(not(all(target_os = "macos", not(coverage))))]
        {
            &self.inventory
        }
    }

    /// Allocate and submit one scheduler-issued node exactly once.
    #[cfg(all(target_os = "macos", not(coverage)))]
    pub fn submit<F>(
        &mut self,
        context: WorkExecutionContext<'_>,
        encode: F,
    ) -> Result<MetalCommandFence, MetalRuntimeError>
    where
        F: FnOnce(MetalEncodingContext<'_>) -> Result<(), MetalRuntimeError>,
    {
        if context.attempt_id() != self.attempt_id || context.lease_epoch() != self.lease_epoch {
            return Err(MetalRuntimeError::LeaseMismatch);
        }
        validate_execution_context(&self.decision, context)?;
        let node = context.node().id.clone();
        if !self.submitted.insert(node.clone()) {
            return Err(MetalRuntimeError::NodeAlreadySubmitted(node));
        }
        {
            let mut active = self
                .core
                .active
                .lock()
                .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)?;
            if active.is_some() {
                return Err(MetalRuntimeError::CommandAlreadyInFlight);
            }
            *active = Some(node.clone());
        }
        let residency = make_platform_resident(self, &node)?;
        let command = self.core.queue.commandBuffer().ok_or_else(|| {
            clear_active(&self.core);
            MetalRuntimeError::CommandQueueUnavailable
        })?;
        if let Err(error) = encode(MetalEncodingContext {
            node: &self.decision.nodes[&node],
            command: &command,
            residency: &residency,
            allocation_slots: &self.decision.allocation_slots,
        }) {
            clear_active(&self.core);
            return Err(error);
        }
        let started = Instant::now();
        command.commit();
        Ok(MetalCommandFence {
            node,
            command: Some(command),
            _residency: residency,
            measurements: measurements(context),
            started,
            core: Arc::clone(&self.core),
        })
    }
}

/// Borrowed encoding seam with no device, queue, or allocation authority.
#[cfg(all(target_os = "macos", not(coverage)))]
#[derive(Clone, Copy)]
pub struct MetalEncodingContext<'a> {
    node: &'a MetalNodeDecision,
    command: &'a ProtocolObject<dyn MTLCommandBuffer>,
    residency: &'a MetalResidency,
    allocation_slots: &'a BTreeMap<AllocationId, PhysicalSlotId>,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl<'a> MetalEncodingContext<'a> {
    /// Return the exact scheduler-selected node decision.
    #[must_use]
    pub const fn node(self) -> &'a MetalNodeDecision {
        self.node
    }

    /// Return the runtime-owned command buffer for this one node.
    #[must_use]
    pub const fn command_buffer(self) -> &'a ProtocolObject<dyn MTLCommandBuffer> {
        self.command
    }

    /// Resolve one logical allocation to its plan-owned resident buffer.
    #[must_use]
    pub fn buffer(self, allocation: &AllocationId) -> Option<&'a ProtocolObject<dyn MTLBuffer>> {
        if !self.node.allocations.contains(allocation) {
            return None;
        }
        let slot = self.allocation_slots.get(allocation)?;
        self.residency._buffers.get(slot).map(AsRef::as_ref)
    }
}

/// Terminal result of one runtime-owned Metal command fence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetalCommandOutcome {
    node: WorkNodeId,
    elapsed_nanos: u64,
    cancelled: bool,
    measurements: WorkMeasurements,
}

impl MetalCommandOutcome {
    /// Return the exact plan node whose command settled.
    #[must_use]
    pub const fn node(&self) -> &WorkNodeId {
        &self.node
    }

    /// Return command submission-to-completion wall time.
    #[must_use]
    pub const fn elapsed_nanos(&self) -> u64 {
        self.elapsed_nanos
    }

    /// Return whether cancellation requested a drain rather than success.
    #[must_use]
    pub const fn cancelled(&self) -> bool {
        self.cancelled
    }

    /// Complete the canonical execution evidence with owner-observed artifacts.
    #[must_use]
    pub fn into_work_measurements(self, artifacts: Vec<ArtifactMeasurement>) -> WorkMeasurements {
        WorkMeasurements::new(
            self.measurements.resources().to_vec(),
            self.measurements.io().to_vec(),
            artifacts,
        )
    }
}

/// One committed command whose buffers remain live until the device fence settles.
#[cfg(all(target_os = "macos", not(coverage)))]
#[must_use = "Metal commands must be waited or cancelled so device errors are observed"]
pub struct MetalCommandFence {
    node: WorkNodeId,
    command: Option<Retained<ProtocolObject<dyn MTLCommandBuffer>>>,
    _residency: MetalResidency,
    measurements: WorkMeasurements,
    started: Instant,
    core: Arc<MetalRuntimeCore>,
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl MetalCommandFence {
    /// Wait for successful completion and surface native command failures.
    pub fn wait(mut self) -> Result<MetalCommandOutcome, MetalRuntimeError> {
        self.settle(false)
    }

    /// Cancel pending higher-level work while draining this already-committed command.
    pub fn cancel_and_drain(mut self) -> Result<MetalCommandOutcome, MetalRuntimeError> {
        self.settle(true)
    }

    fn settle(&mut self, cancelled: bool) -> Result<MetalCommandOutcome, MetalRuntimeError> {
        let command = self
            .command
            .take()
            .ok_or(MetalRuntimeError::FenceAlreadySettled)?;
        command.waitUntilCompleted();
        let elapsed_nanos = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        if command.status() != MTLCommandBufferStatus::Completed {
            clear_active(&self.core);
            return Err(MetalRuntimeError::CommandFailed {
                node: self.node.clone(),
                status: command.status().0 as u64,
            });
        }
        clear_active(&self.core);
        Ok(MetalCommandOutcome {
            node: self.node.clone(),
            elapsed_nanos,
            cancelled,
            measurements: self.measurements.clone(),
        })
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
impl Drop for MetalCommandFence {
    fn drop(&mut self) {
        if let Some(command) = self.command.take() {
            command.waitUntilCompleted();
        }
        clear_active(&self.core);
    }
}

/// Typed Metal eligibility, plan, allocation, or command failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetalRuntimeError {
    /// This build cannot execute Metal work.
    UnsupportedPlatform,
    /// The selected host or device does not satisfy the Metal contract.
    Ineligible(String),
    /// The immutable execution DAG is inconsistent with the Metal runtime.
    InvalidPlan(String),
    /// Runtime discovery disagreed with the plan-selected inventory.
    PlanRuntimeMismatch,
    /// The work context belongs to another execution attempt or lease.
    LeaseMismatch,
    /// This scheduler-issued node capability already opened a Metal runtime.
    RuntimeAlreadyOpened,
    /// Another command still owns the runtime queue and residency lifetime.
    CommandAlreadyInFlight,
    /// The runtime command state could not be observed safely.
    RuntimeStatePoisoned,
    /// A checked byte calculation overflowed.
    Overflow(&'static str),
    /// A plan-selected node was absent from the Metal decision.
    UnknownNode(WorkNodeId),
    /// The scheduler attempted to submit the same Metal node more than once.
    NodeAlreadySubmitted(WorkNodeId),
    /// The selected device could not create its command queue or buffer.
    CommandQueueUnavailable,
    /// One resident allocation exceeds the device's buffer limit.
    BufferTooLarge {
        /// Exact plan-owned physical slot.
        slot: PhysicalSlotId,
        /// Required resident bytes.
        bytes: u64,
    },
    /// Metal could not allocate a plan-selected physical slot.
    AllocationFailed {
        /// Exact plan-owned physical slot.
        slot: PhysicalSlotId,
        /// Requested resident bytes.
        bytes: u64,
    },
    /// A command fence was consumed more than once.
    FenceAlreadySettled,
    /// The native command completed with an error state.
    CommandFailed {
        /// Exact plan-selected node.
        node: WorkNodeId,
        /// Native `MTLCommandBufferStatus` value.
        status: u64,
    },
    /// A plan-selected implementation rejected command encoding before commit.
    Encoding(String),
}

impl fmt::Display for MetalRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => formatter.write_str("Metal is unavailable on this build"),
            Self::Ineligible(reason) => write!(formatter, "Metal is ineligible: {reason}"),
            Self::InvalidPlan(reason) => write!(formatter, "invalid Metal plan: {reason}"),
            Self::PlanRuntimeMismatch => formatter.write_str(
                "detected Metal runtime does not match the plan-selected device and queue",
            ),
            Self::LeaseMismatch => formatter
                .write_str("Metal work context does not belong to the runtime execution lease"),
            Self::RuntimeAlreadyOpened => {
                formatter.write_str("scheduler-issued Metal runtime authority was already consumed")
            }
            Self::CommandAlreadyInFlight => {
                formatter.write_str("a Metal command is already in flight")
            }
            Self::RuntimeStatePoisoned => {
                formatter.write_str("Metal runtime command state is poisoned")
            }
            Self::Overflow(what) => write!(formatter, "{what} overflowed"),
            Self::UnknownNode(node) => {
                write!(
                    formatter,
                    "node {} is not selected for Metal",
                    node.as_str()
                )
            }
            Self::NodeAlreadySubmitted(node) => write!(
                formatter,
                "Metal node {} was already submitted under this lease",
                node.as_str()
            ),
            Self::CommandQueueUnavailable => {
                formatter.write_str("plan-selected Metal command queue is unavailable")
            }
            Self::BufferTooLarge { slot, bytes } => write!(
                formatter,
                "Metal slot {} requires {bytes} bytes above the device buffer limit",
                slot.as_str()
            ),
            Self::AllocationFailed { slot, bytes } => write!(
                formatter,
                "Metal failed to allocate {bytes} bytes for slot {}",
                slot.as_str()
            ),
            Self::FenceAlreadySettled => formatter.write_str("Metal fence already settled"),
            Self::CommandFailed { node, status } => write!(
                formatter,
                "Metal command for node {} failed with status {status}",
                node.as_str()
            ),
            Self::Encoding(reason) => write!(formatter, "Metal command encoding failed: {reason}"),
        }
    }
}

impl Error for MetalRuntimeError {}

fn validate_execution_context(
    decision: &MetalExecutionDecision,
    context: WorkExecutionContext<'_>,
) -> Result<(), MetalRuntimeError> {
    let node = decision
        .nodes
        .get(&context.node().id)
        .ok_or_else(|| MetalRuntimeError::UnknownNode(context.node().id.clone()))?;
    if context.node().kind != node.kind
        || context.node().domain
            != (WorkDomain::Metal {
                demand_id: node.demand_id.clone(),
            })
    {
        return Err(MetalRuntimeError::LeaseMismatch);
    }
    let owns_accelerator = context.resources().iter().any(|resource| {
        resource.amount() == 1
            && matches!(
                resource.resource(),
                LeaseResource::Accelerator { demand_id } if demand_id == &node.demand_id
            )
    });
    let owns_queue = context.resources().iter().any(|resource| {
        resource.amount() == 1
            && matches!(
                resource.resource(),
                LeaseResource::AcceleratorCommandQueue { demand_id }
                    if demand_id == &node.demand_id
            )
    });
    if !owns_accelerator || !owns_queue {
        return Err(MetalRuntimeError::LeaseMismatch);
    }
    let scheduled_allocations = context
        .allocations()
        .iter()
        .map(|allocation| {
            (
                allocation.allocation().clone(),
                (
                    allocation.physical_slot().clone(),
                    allocation.capacity_bytes(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if scheduled_allocations.len() != node.allocations.len()
        || node.allocations.iter().any(|allocation| {
            let Some((slot, capacity)) = scheduled_allocations.get(allocation) else {
                return true;
            };
            decision.allocation_slots.get(allocation) != Some(slot)
                || decision.physical_slots.get(slot) != Some(capacity)
        })
    {
        return Err(MetalRuntimeError::LeaseMismatch);
    }
    Ok(())
}

fn measurements(context: WorkExecutionContext<'_>) -> WorkMeasurements {
    WorkMeasurements::new(
        context
            .resources()
            .iter()
            .map(|resource| {
                let peak = if matches!(
                    resource.resource(),
                    LeaseResource::Accelerator { .. }
                        | LeaseResource::AcceleratorCommandQueue { .. }
                ) {
                    resource.amount()
                } else {
                    0
                };
                ResourceMeasurement::new(
                    resource.resource().clone(),
                    resource.lifetime().clone(),
                    peak,
                )
            })
            .collect(),
        context
            .stage_prediction()
            .io()
            .iter()
            .map(|prediction| IoMeasurement::unobserved(prediction.kind()))
            .collect(),
        Vec::new(),
    )
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn clear_active(core: &MetalRuntimeCore) {
    if let Ok(mut active) = core.active.lock() {
        *active = None;
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn open_platform_runtime(
    decision: MetalExecutionDecision,
    attempt_id: ExecutionAttemptId,
    lease_epoch: u64,
) -> Result<MetalRuntime, MetalRuntimeError> {
    let device = MTLCreateSystemDefaultDevice().ok_or_else(|| {
        MetalRuntimeError::Ineligible("no process-accessible Metal device".to_string())
    })?;
    if !device.hasUnifiedMemory() {
        return Err(MetalRuntimeError::Ineligible(
            "the selected Metal device does not use unified memory".to_string(),
        ));
    }
    let queue = device
        .newCommandQueue()
        .ok_or(MetalRuntimeError::CommandQueueUnavailable)?;
    let recommended_working_set_bytes = device.recommendedMaxWorkingSetSize();
    let maximum_buffer_bytes = device.maxBufferLength() as u64;
    if decision.residency_bytes() > recommended_working_set_bytes {
        return Err(MetalRuntimeError::Ineligible(format!(
            "planned residency {} exceeds the device working-set recommendation {recommended_working_set_bytes}",
            decision.residency_bytes()
        )));
    }
    let mut inventory = decision.inventory.clone();
    inventory.recommended_working_set_bytes = recommended_working_set_bytes;
    inventory.maximum_buffer_bytes = maximum_buffer_bytes;
    Ok(MetalRuntime {
        decision,
        attempt_id,
        lease_epoch,
        submitted: BTreeSet::new(),
        core: Arc::new(MetalRuntimeCore {
            inventory,
            active: Mutex::new(None),
            device,
            queue,
        }),
    })
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn open_platform_runtime(
    _decision: MetalExecutionDecision,
    _attempt_id: ExecutionAttemptId,
    _lease_epoch: u64,
) -> Result<MetalRuntime, MetalRuntimeError> {
    Err(MetalRuntimeError::UnsupportedPlatform)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn make_platform_resident(
    runtime: &MetalRuntime,
    node: &WorkNodeId,
) -> Result<MetalResidency, MetalRuntimeError> {
    let mut buffers = BTreeMap::new();
    let decision = runtime
        .decision
        .nodes
        .get(node)
        .ok_or_else(|| MetalRuntimeError::UnknownNode(node.clone()))?;
    let slots = decision
        .allocations
        .iter()
        .map(|allocation| &runtime.decision.allocation_slots[allocation])
        .collect::<BTreeSet<_>>();
    for slot in slots {
        let bytes = runtime.decision.physical_slots[slot];
        if bytes > runtime.core.inventory.maximum_buffer_bytes {
            return Err(MetalRuntimeError::BufferTooLarge {
                slot: slot.clone(),
                bytes,
            });
        }
        let buffer = runtime
            .core
            .device
            .newBufferWithLength_options(bytes as usize, MTLResourceOptions::StorageModeShared)
            .ok_or_else(|| MetalRuntimeError::AllocationFailed {
                slot: slot.clone(),
                bytes,
            })?;
        buffers.insert(slot.clone(), buffer);
    }
    Ok(MetalResidency { _buffers: buffers })
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn make_platform_resident(
    _runtime: &MetalRuntime,
    _node: &WorkNodeId,
) -> Result<MetalResidency, MetalRuntimeError> {
    Err(MetalRuntimeError::UnsupportedPlatform)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{ExecutionScheduler, SchedulerAction};
    use crate::{
        Accelerator, AcceleratorDemand, AllocationAccess, AllocationLayout, AllocationLifetime,
        AllocationPurpose, AllocationUse, AlternativeId, CacheDemand, CapabilityPredicate,
        ClaimLifetime, CountDemand, CpuClassCapacity, DemandAlternative, DemandEnvelope,
        ExecutionDagSpecification, ExecutionKnobs, ExternalPressure, HostInventory,
        InitializationPolicy, IoBufferDemand, LogicalAllocation, MemoryCapacityDomain,
        MemoryDemand, MemoryView, MemoryViewKind, PhysicalSlot, QueueDemand, QueueResource,
        ResourceClaim, ResourceHeadroom, ResourcePolicy, ResourceTopology, ScalingMetadata,
        SlotCompatibility, TransferDemand, WorkImplementationId, WorkNode,
    };

    fn metal_plan(shared_slot_count: usize) -> (ExecutionDag, ResourceTopology) {
        let domain = CapacityDomainId::new("unified");
        let host = CapacityViewId::new("host");
        let metal = CapacityViewId::new("metal");
        let accelerator = AcceleratorId::new("metal-0");
        let command_queue = QueueResourceId::new("metal-command");
        let mut allocations = Vec::new();
        let mut logical = Vec::new();
        let mut slots = Vec::new();
        let mut memory = Vec::new();
        for index in 0..shared_slot_count {
            let allocation = AllocationId::new(format!("allocation-{index}"));
            let slot = PhysicalSlotId::new(format!("slot-{index}"));
            allocations.push(AllocationUse {
                allocation: allocation.clone(),
                lifetime: ClaimLifetime::through_fence(FenceKind::Device),
            });
            logical.push(LogicalAllocation {
                id: allocation,
                bytes: 128,
                purpose: AllocationPurpose::Data,
                compatibility: SlotCompatibility {
                    memory_domain: domain.clone(),
                    views: BTreeSet::from([host.clone(), metal.clone()]),
                    alignment_bytes: 64,
                    storage_mode: StorageMode::MetalShared,
                    layout: AllocationLayout::new("f32"),
                    initialization: InitializationPolicy::OverwriteBeforeRead,
                    access: AllocationAccess::ReadWrite,
                },
                physical_slot: slot.clone(),
                lifetime: AllocationLifetime {
                    acquire_at: WorkNodeId::new("metal-work"),
                    release_after: BTreeSet::from([crate::WorkDependency::Fence(
                        crate::FenceId::new(WorkNodeId::new("metal-work"), FenceKind::Device),
                    )]),
                },
            });
            slots.push(PhysicalSlot {
                id: slot,
                lease_resource: crate::LeaseResource::Memory {
                    allocation_id: format!("slot-{index}"),
                },
                capacity_bytes: 128,
                compatibility: logical[index].compatibility.clone(),
            });
            memory.push(MemoryDemand {
                allocation_id: format!("slot-{index}"),
                hard_bytes: 128,
                preferred_bytes: 128,
                views: vec![host.clone(), metal.clone()],
            });
        }
        let node = WorkNode {
            id: WorkNodeId::new("metal-work"),
            kind: WorkKind::Compute,
            domain: WorkDomain::Metal {
                demand_id: "metal".to_string(),
            },
            implementation: WorkImplementationId::new("metal-implementation"),
            dependencies: BTreeSet::new(),
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::Accelerator {
                        demand_id: "metal".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Device),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::AcceleratorCommandQueue {
                        demand_id: "metal".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Device),
                },
            ],
            allocations,
            fences: BTreeSet::from([FenceKind::Device]),
            quiescence_after: BTreeSet::new(),
        };
        let demand = DemandEnvelope {
            host_memory_view: host.clone(),
            memory,
            workers: CountDemand::zero(),
            overhead: RuntimeOverheadDemand {
                driver_bytes: 64,
                jit_bytes: 32,
                command_buffer_bytes: 16,
                ..RuntimeOverheadDemand::zero()
            },
            storage: Vec::new(),
            rates: Vec::new(),
            caches: CacheDemand {
                hard_resident_bytes: 32,
                preferred_resident_bytes: 32,
            },
            locks: CountDemand::zero(),
            file_descriptors: CountDemand::zero(),
            queues: Vec::<QueueDemand>::new(),
            transfers: Vec::<TransferDemand>::new(),
            accelerators: vec![AcceleratorDemand {
                demand_id: "metal".to_string(),
                accelerator: accelerator.clone(),
                slots: CountDemand::new(1, 1),
                command_queue_slots: CountDemand::new(1, 1),
            }],
            io_buffers: IoBufferDemand::zero(),
        };
        let plan = ExecutionDag::new(ExecutionDagSpecification {
            required_resource_capabilities: BTreeSet::new(),
            resource_alternative: DemandAlternative {
                id: AlternativeId::new("metal"),
                capabilities: CapabilityPredicate::default(),
                demand,
                headroom: ResourceHeadroom::default(),
                scaling: ScalingMetadata {
                    minimum_workers: 0,
                    maximum_workers: 0,
                    maximum_batch_size: 1,
                    maximum_tile_width: 1,
                    maximum_tile_height: 1,
                    maximum_slab_depth: 1,
                    memory_bytes_per_worker: BTreeMap::new(),
                },
                quiescence_points: BTreeSet::from([crate::QuiescencePoint::RunBoundary]),
            },
            nodes: vec![node],
            logical_allocations: logical,
            physical_slots: slots,
            initial_knobs: ExecutionKnobs {
                workers: 0,
                ..ExecutionKnobs::serial()
            },
            adaptations: Vec::new(),
        })
        .expect("valid Metal plan");
        let topology = ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Unified,
                capacity_bytes: 1_024,
            }],
            memory_views: vec![
                MemoryView {
                    id: host,
                    domain: domain.clone(),
                    kind: MemoryViewKind::Host,
                },
                MemoryView {
                    id: metal.clone(),
                    domain,
                    kind: MemoryViewKind::Metal,
                },
            ],
            accelerators: vec![Accelerator {
                id: accelerator,
                kind: AcceleratorKind::Metal,
                memory_view: metal,
                command_queue: command_queue.clone(),
                occupancy_slots: 1,
            }],
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: Vec::new(),
            queue_resources: vec![QueueResource::new(command_queue, 1)],
            logical_cpu_threads: 1,
            performance_cpu_cores: CpuClassCapacity::Known(1),
            cache_capacity_bytes: 1_024,
            lock_capacity: 1,
            file_descriptor_capacity: 1,
        };
        (plan, topology)
    }

    #[test]
    fn decision_charges_each_unified_physical_slot_once() {
        let (plan, topology) = metal_plan(2);
        let decision = MetalExecutionDecision::bind(&plan, &topology).expect("Metal decision");
        assert_eq!(decision.residency_bytes(), 256);
        assert_eq!(decision.physical_slots().len(), 2);
        assert_eq!(decision.nodes().len(), 1);
        assert_eq!(decision.overhead().driver_bytes, 64);
        assert_eq!(decision.overhead().jit_bytes, 32);
        assert_eq!(decision.overhead().command_buffer_bytes, 16);
    }

    #[test]
    fn decision_rejects_separate_device_memory() {
        let (plan, mut topology) = metal_plan(1);
        topology.memory_domains[0].kind = MemoryCapacityKind::DevicePrivate;
        let error = MetalExecutionDecision::bind(&plan, &topology)
            .expect_err("separate memory must fail closed");
        assert!(
            matches!(error, MetalRuntimeError::Ineligible(reason) if reason.contains("unified"))
        );
    }

    #[cfg(not(all(target_os = "macos", not(coverage))))]
    #[test]
    fn explicit_runtime_never_substitutes_cpu_when_metal_is_unavailable() {
        let (plan, topology) = metal_plan(1);
        let decision = MetalExecutionDecision::bind(&plan, &topology).expect("Metal decision");
        assert_eq!(
            open_platform_runtime(decision, ExecutionAttemptId::from_sha256([7; 32]), 1)
                .expect_err("non-Metal build must reject"),
            MetalRuntimeError::UnsupportedPlatform
        );
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    fn apple_runtime_allocates_submits_and_drains_one_plan_selected_command() {
        let (plan, topology) = metal_plan(1);
        let decision = MetalExecutionDecision::bind(&plan, &topology).expect("Metal decision");
        let runtime =
            match open_platform_runtime(decision, ExecutionAttemptId::from_sha256([7; 32]), 1) {
                Ok(runtime) => runtime,
                Err(MetalRuntimeError::Ineligible(reason))
                    if reason == "no process-accessible Metal device" =>
                {
                    return;
                }
                Err(error) => panic!("Apple Metal runtime: {error}"),
            };
        let node = WorkNodeId::new("metal-work");
        let residency = make_platform_resident(&runtime, &node).expect("plan-selected residency");
        let command = runtime
            .core
            .queue
            .commandBuffer()
            .expect("plan-selected command queue");
        let started = Instant::now();
        command.commit();
        let fence = MetalCommandFence {
            node: node.clone(),
            command: Some(command),
            _residency: residency,
            measurements: WorkMeasurements::default(),
            started,
            core: Arc::clone(&runtime.core),
        };
        let outcome = fence.wait().expect("device fence");
        assert_eq!(outcome.node(), &node);
        assert!(!outcome.cancelled());
    }

    #[test]
    fn topology_fixture_is_admissible_by_the_resource_authority() {
        let (plan, topology) = metal_plan(1);
        let pressure = ExternalPressure {
            memory_available_bytes: BTreeMap::from([(
                topology.memory_domains[0].id.clone(),
                1_024,
            )]),
            available_cpu_threads: 1,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::new(),
            queue_available_slots: BTreeMap::from([(topology.queue_resources[0].id.clone(), 1)]),
            accelerator_available_slots: BTreeMap::from([(topology.accelerators[0].id.clone(), 1)]),
            cache_available_bytes: 1_024,
            available_locks: 1,
            available_file_descriptors: 1,
        };
        let authority =
            crate::ResourceAuthority::with_inventory(HostInventory { topology, pressure })
                .expect("valid inventory");
        authority
            .acquire(
                ResourcePolicy::Exclusive,
                crate::DemandAlternatives {
                    required_capabilities: BTreeSet::new(),
                    alternatives: vec![plan.resource_alternative().clone()],
                },
            )
            .expect("Metal demand admits");
    }

    #[test]
    fn scheduler_issues_one_affine_runtime_authority_per_metal_node() {
        let (plan, topology) = metal_plan(1);
        let pressure = ExternalPressure {
            memory_available_bytes: BTreeMap::from([(
                topology.memory_domains[0].id.clone(),
                1_024,
            )]),
            available_cpu_threads: 1,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::new(),
            queue_available_slots: BTreeMap::from([(topology.queue_resources[0].id.clone(), 1)]),
            accelerator_available_slots: BTreeMap::from([(topology.accelerators[0].id.clone(), 1)]),
            cache_available_bytes: 1_024,
            available_locks: 1,
            available_file_descriptors: 1,
        };
        let authority =
            crate::ResourceAuthority::with_inventory(HostInventory { topology, pressure })
                .expect("valid inventory");
        let mut scheduler =
            ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &authority, None)
                .expect("lease-backed scheduler");
        let SchedulerAction::Work(work) = scheduler.next_action().expect("scheduled Metal work")
        else {
            panic!("expected scheduler-issued Metal work");
        };
        assert!(work.claim_metal_runtime());
        assert!(!work.claim_metal_runtime());
    }
}
