// SPDX-License-Identifier: LGPL-3.0-or-later

//! Planner-owned Apple Metal device, residency, and command-fence runtime.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::sync::Mutex;
#[cfg(all(target_os = "macos", not(coverage)))]
use std::time::Instant;

use crate::{
    AcceleratorId, AcceleratorKind, AllocationId, CapacityDomainId, CapacityViewId,
    ExecutionAttemptId, ExecutionDag, FenceKind, IoBufferKind, IoMeasurement, LeaseResource,
    MemoryCapacityKind, PhysicalSlotId, QueueResourceId, ResourceMeasurement, ResourceTopology,
    RuntimeOverheadDemand, StorageMode, TransferLinkId, WorkDomain, WorkExecutionContext, WorkKind,
    WorkMeasurements, WorkNodeId,
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

/// One execution-scoped Metal residency domain owned by the scheduler.
///
/// The runtime is deliberately crate-private: work implementations receive
/// constrained operations, never a device, queue, command buffer, or buffer
/// allocation handle from which unplanned authority could be recovered.
pub(crate) struct MetalExecutionState {
    decision: MetalExecutionDecision,
    lease_epoch: u64,
    inner: Mutex<MetalExecutionInner>,
}

#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
struct MetalExecutionInner {
    attempt_id: Option<ExecutionAttemptId>,
    submitted: BTreeSet<WorkNodeId>,
    closed: bool,
    #[cfg(all(target_os = "macos", not(coverage)))]
    platform: Option<MetalPlatformState>,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
struct MetalPlatformState {
    _device: Retained<ProtocolObject<dyn MTLDevice>>,
    queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
    buffers: BTreeMap<PhysicalSlotId, Retained<ProtocolObject<dyn MTLBuffer>>>,
    active: Option<MetalInFlight>,
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
struct MetalInFlight {
    node: WorkNodeId,
    command: Retained<ProtocolObject<dyn MTLCommandBuffer>>,
    measurements: WorkMeasurements,
    _started: Instant,
}

impl fmt::Debug for MetalExecutionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetalExecutionState")
            .field("inventory", self.decision.inventory())
            .field("lease_epoch", &self.lease_epoch)
            .finish_non_exhaustive()
    }
}

impl MetalExecutionState {
    pub(crate) fn bind(
        plan: &ExecutionDag,
        topology: &ResourceTopology,
        lease_epoch: u64,
    ) -> Result<Self, MetalRuntimeError> {
        Ok(Self {
            decision: MetalExecutionDecision::bind(plan, topology)?,
            lease_epoch,
            inner: Mutex::new(MetalExecutionInner {
                attempt_id: None,
                submitted: BTreeSet::new(),
                closed: false,
                #[cfg(all(target_os = "macos", not(coverage)))]
                platform: None,
            }),
        })
    }

    /// Submit a command with no encoders. T57 replaces this host-smoke
    /// operation with typed, crate-private compute encoding operations.
    #[allow(
        dead_code,
        reason = "T57 consumes the constrained Metal execution seam"
    )]
    pub(crate) fn submit_noop(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), MetalRuntimeError> {
        validate_execution_context(&self.decision, context)?;
        if context.lease_epoch() != self.lease_epoch {
            return Err(MetalRuntimeError::LeaseMismatch);
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)?;
        if inner.closed {
            return Err(MetalRuntimeError::RuntimeClosed);
        }
        match inner.attempt_id {
            Some(attempt_id) if attempt_id != context.attempt_id() => {
                return Err(MetalRuntimeError::LeaseMismatch);
            }
            None => inner.attempt_id = Some(context.attempt_id()),
            Some(_) => {}
        }
        let node = context.node().id.clone();
        if inner.submitted.contains(&node) {
            return Err(MetalRuntimeError::NodeAlreadySubmitted(node));
        }
        submit_platform_noop(&self.decision, &mut inner, context)?;
        inner.submitted.insert(node);
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "T57 consumes the constrained Metal execution seam"
    )]
    pub(crate) fn wait(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<WorkMeasurements, MetalRuntimeError> {
        validate_execution_context(&self.decision, context)?;
        if context.lease_epoch() != self.lease_epoch {
            return Err(MetalRuntimeError::LeaseMismatch);
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)?;
        if inner.attempt_id != Some(context.attempt_id()) {
            return Err(MetalRuntimeError::LeaseMismatch);
        }
        wait_platform(&mut inner, &context.node().id)
    }

    pub(crate) fn submitted(&self, node: &WorkNodeId) -> Result<bool, MetalRuntimeError> {
        self.inner
            .lock()
            .map(|inner| inner.submitted.contains(node))
            .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)
    }

    pub(crate) fn close(&self) -> Result<(), MetalRuntimeError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)?;
        close_platform(&mut inner)?;
        inner.closed = true;
        Ok(())
    }

    #[cfg(all(test, target_os = "macos", not(coverage)))]
    fn buffer_identity(&self, slot: &PhysicalSlotId) -> Option<usize> {
        let inner = self.inner.lock().ok()?;
        let platform = inner.platform.as_ref()?;
        platform.buffers.get(slot).map(|buffer| {
            let pointer: *const ProtocolObject<dyn MTLBuffer> = buffer.as_ref();
            pointer.cast::<()>().addr()
        })
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
    /// The work context belongs to another execution attempt or lease.
    LeaseMismatch,
    /// The execution residency was closed before another submission.
    RuntimeClosed,
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
}

impl fmt::Display for MetalRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => formatter.write_str("Metal is unavailable on this build"),
            Self::Ineligible(reason) => write!(formatter, "Metal is ineligible: {reason}"),
            Self::InvalidPlan(reason) => write!(formatter, "invalid Metal plan: {reason}"),
            Self::LeaseMismatch => formatter
                .write_str("Metal work context does not belong to the runtime execution lease"),
            Self::RuntimeClosed => formatter.write_str("Metal execution residency is closed"),
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
        }
    }
}

impl Error for MetalRuntimeError {}

#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
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

#[cfg(all(target_os = "macos", not(coverage)))]
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
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
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
fn open_platform_state(
    decision: &MetalExecutionDecision,
) -> Result<MetalPlatformState, MetalRuntimeError> {
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
    let mut buffers = BTreeMap::new();
    for (slot, bytes) in &decision.physical_slots {
        if *bytes > maximum_buffer_bytes {
            return Err(MetalRuntimeError::BufferTooLarge {
                slot: slot.clone(),
                bytes: *bytes,
            });
        }
        let buffer = device
            .newBufferWithLength_options(*bytes as usize, MTLResourceOptions::StorageModeShared)
            .ok_or_else(|| MetalRuntimeError::AllocationFailed {
                slot: slot.clone(),
                bytes: *bytes,
            })?;
        buffers.insert(slot.clone(), buffer);
    }
    Ok(MetalPlatformState {
        _device: device,
        queue,
        buffers,
        active: None,
    })
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
fn submit_platform_noop(
    decision: &MetalExecutionDecision,
    inner: &mut MetalExecutionInner,
    context: WorkExecutionContext<'_>,
) -> Result<(), MetalRuntimeError> {
    if inner.platform.is_none() {
        inner.platform = Some(open_platform_state(decision)?);
    }
    let platform = inner.platform.as_mut().expect("platform initialized");
    if platform.active.is_some() {
        return Err(MetalRuntimeError::CommandAlreadyInFlight);
    }
    let command = platform
        .queue
        .commandBuffer()
        .ok_or(MetalRuntimeError::CommandQueueUnavailable)?;
    let started = Instant::now();
    command.commit();
    platform.active = Some(MetalInFlight {
        node: context.node().id.clone(),
        command,
        measurements: measurements(context),
        _started: started,
    });
    Ok(())
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
fn submit_platform_noop(
    _decision: &MetalExecutionDecision,
    _inner: &mut MetalExecutionInner,
    _context: WorkExecutionContext<'_>,
) -> Result<(), MetalRuntimeError> {
    Err(MetalRuntimeError::UnsupportedPlatform)
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
fn wait_platform(
    inner: &mut MetalExecutionInner,
    node: &WorkNodeId,
) -> Result<WorkMeasurements, MetalRuntimeError> {
    let platform = inner
        .platform
        .as_mut()
        .ok_or(MetalRuntimeError::FenceAlreadySettled)?;
    let active = platform
        .active
        .take()
        .ok_or(MetalRuntimeError::FenceAlreadySettled)?;
    if &active.node != node {
        platform.active = Some(active);
        return Err(MetalRuntimeError::UnknownNode(node.clone()));
    }
    active.command.waitUntilCompleted();
    if active.command.status() != MTLCommandBufferStatus::Completed {
        return Err(MetalRuntimeError::CommandFailed {
            node: active.node,
            status: active.command.status().0 as u64,
        });
    }
    Ok(active.measurements)
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
fn wait_platform(
    _inner: &mut MetalExecutionInner,
    _node: &WorkNodeId,
) -> Result<WorkMeasurements, MetalRuntimeError> {
    Err(MetalRuntimeError::UnsupportedPlatform)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn close_platform(inner: &mut MetalExecutionInner) -> Result<(), MetalRuntimeError> {
    if let Some(platform) = inner.platform.as_mut()
        && let Some(active) = platform.active.take()
    {
        active.command.waitUntilCompleted();
        if active.command.status() != MTLCommandBufferStatus::Completed {
            return Err(MetalRuntimeError::CommandFailed {
                node: active.node,
                status: active.command.status().0 as u64,
            });
        }
    }
    inner.platform = None;
    Ok(())
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn close_platform(_inner: &mut MetalExecutionInner) -> Result<(), MetalRuntimeError> {
    Ok(())
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn probe_platform(_decision: &MetalExecutionDecision) -> Result<(), MetalRuntimeError> {
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
        metal_plan_with_nodes(shared_slot_count, 1)
    }

    fn metal_plan_with_nodes(
        shared_slot_count: usize,
        node_count: usize,
    ) -> (ExecutionDag, ResourceTopology) {
        assert!(node_count > 0);
        let domain = CapacityDomainId::new("unified");
        let host = CapacityViewId::new("host");
        let metal = CapacityViewId::new("metal");
        let accelerator = AcceleratorId::new("metal-0");
        let command_queue = QueueResourceId::new("metal-command");
        let mut allocations = Vec::new();
        let mut logical = Vec::new();
        let mut slots = Vec::new();
        let mut memory = Vec::new();
        let node_ids = (0..node_count)
            .map(|index| {
                if node_count == 1 {
                    WorkNodeId::new("metal-work")
                } else {
                    WorkNodeId::new(format!("metal-work-{index}"))
                }
            })
            .collect::<Vec<_>>();
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
                    acquire_at: node_ids[0].clone(),
                    release_after: BTreeSet::from([crate::WorkDependency::Fence(
                        crate::FenceId::new(node_ids[node_count - 1].clone(), FenceKind::Device),
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
        let nodes = node_ids
            .iter()
            .enumerate()
            .map(|(index, node_id)| WorkNode {
                id: node_id.clone(),
                kind: WorkKind::Compute,
                domain: WorkDomain::Metal {
                    demand_id: "metal".to_string(),
                },
                implementation: WorkImplementationId::new("metal-implementation"),
                dependencies: (index > 0)
                    .then(|| {
                        crate::WorkDependency::Fence(crate::FenceId::new(
                            node_ids[index - 1].clone(),
                            FenceKind::Device,
                        ))
                    })
                    .into_iter()
                    .collect(),
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
                allocations: allocations.clone(),
                fences: BTreeSet::from([FenceKind::Device]),
                quiescence_after: BTreeSet::new(),
            })
            .collect();
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
            nodes,
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
            probe_platform(&decision).expect_err("non-Metal build must reject"),
            MetalRuntimeError::UnsupportedPlatform
        );
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    fn apple_runtime_retains_one_physical_buffer_across_node_fences() {
        let (plan, topology) = metal_plan_with_nodes(1, 2);
        let state = MetalExecutionState::bind(&plan, &topology, 1).expect("Metal execution");
        let platform = match open_platform_state(&state.decision) {
            Ok(platform) => platform,
            Err(MetalRuntimeError::Ineligible(reason))
                if reason == "no process-accessible Metal device" =>
            {
                return;
            }
            Err(error) => panic!("Apple Metal runtime: {error}"),
        };
        assert_eq!(platform.buffers.len(), 1);
        state.inner.lock().expect("runtime state").platform = Some(platform);
        let slot = PhysicalSlotId::new("slot-0");
        let identity = state.buffer_identity(&slot).expect("resident buffer");
        for index in 0..2 {
            let node = WorkNodeId::new(format!("metal-work-{index}"));
            let mut inner = state.inner.lock().expect("runtime state");
            let platform = inner.platform.as_mut().expect("platform residency");
            let command = platform
                .queue
                .commandBuffer()
                .expect("plan-selected command queue");
            command.commit();
            platform.active = Some(MetalInFlight {
                node: node.clone(),
                command,
                measurements: WorkMeasurements::default(),
                _started: Instant::now(),
            });
            assert_eq!(
                wait_platform(&mut inner, &node),
                Ok(WorkMeasurements::default())
            );
            drop(inner);
            assert_eq!(state.buffer_identity(&slot), Some(identity));
        }
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
    fn scheduler_issues_one_execution_scoped_runtime_authority() {
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
        let execution = work
            .metal_execution()
            .expect("scheduler-owned Metal execution");
        let fence_work = work.for_fence(FenceKind::Device);
        assert!(std::ptr::eq(
            execution,
            fence_work
                .metal_execution()
                .expect("same execution survives through the fence")
        ));
    }
}
