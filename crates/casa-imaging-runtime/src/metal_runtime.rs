// SPDX-License-Identifier: LGPL-3.0-or-later

//! Planner-owned Apple Metal device, residency, and command-fence runtime.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::sync::{Mutex, MutexGuard};
#[cfg(all(target_os = "macos", not(coverage)))]
use std::time::Instant;

use crate::{
    AcceleratorId, AcceleratorKind, AllocationId, CapacityDomainId, CapacityViewId,
    ExecutionAttemptId, ExecutionDag, FenceKind, IoBufferKind, LeaseResource, MemoryCapacityKind,
    PhysicalSlotId, QueueResourceId, ResourceTopology, RuntimeOverheadDemand, StorageMode,
    TransferLinkId, WorkDomain, WorkExecutionContext, WorkKind, WorkMeasurements, WorkNodeId,
};

use crate::{IoMeasurement, ResourceMeasurement};

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
    domain: WorkDomain,
    demand_id: String,
    allocations: Vec<AllocationId>,
    scheduled_allocations: BTreeMap<AllocationId, (PhysicalSlotId, u64)>,
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
    allocation_bytes: BTreeMap<AllocationId, u64>,
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
            let Some(demand_id) = node.metal_demand_id() else {
                continue;
            };
            if !matches!(
                node.kind,
                WorkKind::Preparation
                    | WorkKind::Jit
                    | WorkKind::Compute
                    | WorkKind::ObservationRead
                    | WorkKind::ObservationReadWriteback
            ) {
                return Err(MetalRuntimeError::InvalidPlan(format!(
                    "Metal node {} has runtime-incompatible work kind {:?}",
                    node.id.as_str(),
                    node.kind
                )));
            }
            let fences = if node.domain == WorkDomain::Io {
                BTreeSet::from([FenceKind::Io, FenceKind::Device])
            } else {
                BTreeSet::from([FenceKind::Device])
            };
            if node.fences != fences {
                return Err(MetalRuntimeError::InvalidPlan(format!(
                    "Metal node {} must declare its I/O and device completion fences",
                    node.id.as_str()
                )));
            }
            let demand = plan
                .resource_alternative()
                .demand
                .accelerators
                .iter()
                .find(|candidate| candidate.demand_id == demand_id)
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
            let scheduled_allocations = node
                .allocations
                .iter()
                .map(|use_| {
                    let allocation = &plan.logical_allocations()[&use_.allocation];
                    let slot = &plan.physical_slots()[&allocation.physical_slot];
                    (
                        allocation.id.clone(),
                        (slot.id.clone(), slot.capacity_bytes),
                    )
                })
                .collect();
            let allocations = node
                .allocations
                .iter()
                .filter(|use_| {
                    plan.logical_allocations()[&use_.allocation]
                        .compatibility
                        .storage_mode
                        == StorageMode::MetalShared
                })
                .map(|use_| use_.allocation.clone())
                .collect::<Vec<_>>();
            used_allocations.extend(allocations.iter().cloned());
            nodes.insert(
                node.id.clone(),
                MetalNodeDecision {
                    node: node.id.clone(),
                    kind: node.kind,
                    domain: node.domain.clone(),
                    demand_id: demand_id.to_owned(),
                    allocations,
                    scheduled_allocations,
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
        if overhead.jit_bytes == 0 {
            return Err(MetalRuntimeError::InvalidPlan(
                "Metal pipeline preparation requires a positive JIT envelope".to_string(),
            ));
        }

        let mut allocation_slots = BTreeMap::new();
        let mut allocation_bytes = BTreeMap::new();
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
            allocation_bytes.insert(allocation_id.clone(), allocation.bytes);
            allocation_slots.insert(allocation_id, slot.id.clone());
        }
        physical_slots.values().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(*bytes)
                .ok_or(MetalRuntimeError::Overflow("Metal residency"))
        })?;

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
            allocation_bytes,
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

    /// Return total declared slot capacity, not peak concurrent live residency.
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
    nodes: BTreeMap<WorkNodeId, MetalNodeProgress>,
    closed: bool,
    #[cfg(all(target_os = "macos", not(coverage)))]
    platform: Option<MetalPlatformState>,
}

#[cfg(all(target_os = "macos", not(coverage)))]
struct MetalPlatformState {
    device: Retained<ProtocolObject<dyn MTLDevice>>,
    queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
    buffers: BTreeMap<PhysicalSlotId, Retained<ProtocolObject<dyn MTLBuffer>>>,
    kernels: Option<crate::metal_cube::MetalCubeKernels>,
}

// SAFETY: Metal device/queue/buffer/pipeline handles have no thread affinity.
// MTLBuffer's raw mutable contents prevent objc2 from promising Send/Sync for
// the handle itself. This owner never exposes handles or slices beyond a call:
// its execution mutex covers CPU contents access and GPU submission through
// waitUntilCompleted, and encoders/command buffers remain local to that call.
#[cfg(all(target_os = "macos", not(coverage)))]
unsafe impl Send for MetalPlatformState {}

#[derive(Default)]
struct MetalNodeProgress {
    prepared: bool,
    finished: bool,
    stats: MetalBatchStats,
    empty_source: bool,
}

/// Shareable access derived once from the scheduler's exact work capabilities.
/// It borrows the execution state, so it cannot outlive the admitted stage.
pub(crate) struct MetalBatchAccess<'a> {
    runtime: &'a MetalExecutionState,
    node: WorkNodeId,
    attempt: ExecutionAttemptId,
}

impl MetalBatchAccess<'_> {
    fn lock(&self) -> Result<MutexGuard<'_, MetalExecutionInner>, MetalRuntimeError> {
        let inner = self
            .runtime
            .inner
            .lock()
            .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)?;
        if inner.closed {
            return Err(MetalRuntimeError::RuntimeClosed);
        }
        if inner.attempt_id != Some(self.attempt) {
            return Err(MetalRuntimeError::LeaseMismatch);
        }
        if inner.nodes.get(&self.node).is_some_and(|p| p.finished) {
            return Err(MetalRuntimeError::NodeFinished(self.node.clone()));
        }
        Ok(inner)
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    pub(crate) fn with_bytes<R>(
        &self,
        region: MetalBufferRegion<'_>,
        use_bytes: impl FnOnce(&mut [u8]) -> R,
    ) -> Result<R, MetalRuntimeError> {
        let inner = self.lock()?;
        let platform = inner.platform.as_ref().expect("admitted prepared platform");
        let (buffer, offset) = buffer_region(&self.runtime.decision, platform, &self.node, region)?;
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(
                buffer.contents().as_ptr().cast::<u8>().add(offset),
                region.bytes,
            )
        };
        Ok(use_bytes(bytes))
    }

    #[cfg(not(all(target_os = "macos", not(coverage))))]
    pub(crate) fn with_bytes<R>(
        &self,
        _: MetalBufferRegion<'_>,
        _: impl FnOnce(&mut [u8]) -> R,
    ) -> Result<R, MetalRuntimeError> {
        Err(MetalRuntimeError::UnsupportedPlatform)
    }

    pub(crate) fn execute(
        &self,
        dispatches: &[CubeDispatch<'_>],
    ) -> Result<MetalBatchStats, MetalRuntimeError> {
        if dispatches.is_empty() {
            return Err(MetalRuntimeError::InvalidPlan("empty compute batch".into()));
        }
        let mut inner = self.lock()?;
        let stats =
            execute_platform_batch(&self.runtime.decision, &mut inner, &self.node, dispatches)?;
        let progress = inner.nodes.get_mut(&self.node).expect("prepared node");
        progress.stats.batches += stats.batches;
        progress.stats.grid_samples += stats.grid_samples;
        progress.stats.degrid_samples += stats.degrid_samples;
        progress.stats.gpu_seconds += stats.gpu_seconds;
        progress.stats.submit_wait_seconds += stats.submit_wait_seconds;
        Ok(stats)
    }
}

/// Actual submitted work, not a projection from the resource envelope.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct MetalBatchStats {
    pub batches: u64,
    pub grid_samples: u64,
    pub degrid_samples: u64,
    pub gpu_seconds: f64,
    pub submit_wait_seconds: f64,
}

/// A bounded region of a plan-owned shared allocation. Multiple plane regions
/// can occupy one wave allocation without separate device allocations.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MetalBufferRegion<'a> {
    pub allocation: &'a AllocationId,
    pub offset: usize,
    pub bytes: usize,
}

impl MetalBufferRegion<'_> {
    fn overlaps(self, other: Self) -> bool {
        self.allocation == other.allocation
            && self.offset < other.offset.saturating_add(other.bytes)
            && other.offset < self.offset.saturating_add(self.bytes)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum CubeDispatchKind<'a> {
    Grid,
    Degrid { predicted: MetalBufferRegion<'a> },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CubeDispatch<'a> {
    pub kind: CubeDispatchKind<'a>,
    pub samples: MetalBufferRegion<'a>,
    pub weights: MetalBufferRegion<'a>,
    pub grid: MetalBufferRegion<'a>,
    pub count: u32,
    pub width: u32,
    pub height: u32,
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
    pub(crate) fn batch_access(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<MetalBatchAccess<'_>, MetalRuntimeError> {
        self.prepare(context)?;
        Ok(MetalBatchAccess {
            runtime: self,
            node: context.node().id.clone(),
            attempt: context.attempt_id(),
        })
    }

    /// Called only after the observation owner has consumed its complete source.
    /// All-flagged or empty spatial support needs no fabricated GPU dispatch.
    pub(crate) fn complete_empty_source(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), MetalRuntimeError> {
        let mut inner = self.lock_for_work(context)?;
        let progress = inner
            .nodes
            .get_mut(&context.node().id)
            .ok_or(MetalRuntimeError::FenceAlreadySettled)?;
        if progress.stats.batches == 0 {
            progress.empty_source = true;
        }
        Ok(())
    }

    pub(crate) fn batch_stats(
        &self,
        node: &WorkNodeId,
    ) -> Result<MetalBatchStats, MetalRuntimeError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)?;
        Ok(inner
            .nodes
            .get(node)
            .map_or_else(MetalBatchStats::default, |p| p.stats))
    }
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
                nodes: BTreeMap::new(),
                closed: false,
                #[cfg(all(target_os = "macos", not(coverage)))]
                platform: None,
            }),
        })
    }

    fn lock_for_work(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<MutexGuard<'_, MetalExecutionInner>, MetalRuntimeError> {
        validate_execution_context(&self.decision, context, false)?;
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
        if inner
            .nodes
            .get(&context.node().id)
            .is_some_and(|node| node.finished)
        {
            return Err(MetalRuntimeError::NodeFinished(context.node().id.clone()));
        }
        Ok(inner)
    }

    /// Prepare pipelines and materialize only this admitted node's device slots.
    /// No command is fabricated for preparation-only work.
    pub(crate) fn prepare(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), MetalRuntimeError> {
        let mut inner = self.lock_for_work(context)?;
        prepare_platform(&self.decision, &mut inner, &context.node().id)?;
        inner
            .nodes
            .entry(context.node().id.clone())
            .or_default()
            .prepared = true;
        Ok(())
    }

    /// Consume terminal device evidence once, after any number of drained
    /// batches. This is distinct from batch completion and works in either
    /// terminal I/O/device fence order.
    pub(crate) fn finish(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<WorkMeasurements, MetalRuntimeError> {
        validate_execution_context(&self.decision, context, true)?;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)?;
        if inner.closed {
            return Err(MetalRuntimeError::RuntimeClosed);
        }
        if context.lease_epoch() != self.lease_epoch
            || inner.attempt_id != Some(context.attempt_id())
        {
            return Err(MetalRuntimeError::LeaseMismatch);
        }
        let progress = inner
            .nodes
            .get_mut(&context.node().id)
            .ok_or(MetalRuntimeError::FenceAlreadySettled)?;
        if progress.finished {
            return Err(MetalRuntimeError::FenceAlreadySettled);
        }
        if !progress.prepared
            || (requires_dispatch(context.node().kind)
                && progress.stats.batches == 0
                && !progress.empty_source)
        {
            return Err(MetalRuntimeError::InvalidPlan(
                "Metal compute stage did not dispatch".into(),
            ));
        }
        progress.finished = true;
        Ok(measurements(context))
    }

    pub(crate) fn submitted(&self, node: &WorkNodeId) -> Result<bool, MetalRuntimeError> {
        self.inner
            .lock()
            .map(|inner| {
                inner.nodes.get(node).is_some_and(|progress| {
                    if requires_dispatch(self.decision.nodes[node].kind) {
                        progress.stats.batches > 0 || progress.empty_source
                    } else {
                        progress.prepared
                    }
                })
            })
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

    /// Called by the scheduler at the allocation ledger's actual release event,
    /// before its reservation can be reused by another logical allocation.
    pub(crate) fn release_allocation(
        &self,
        allocation: &AllocationId,
    ) -> Result<(), MetalRuntimeError> {
        let Some(slot) = self.decision.allocation_slots.get(allocation) else {
            return Ok(());
        };
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| MetalRuntimeError::RuntimeStatePoisoned)?;
        #[cfg(all(target_os = "macos", not(coverage)))]
        if let Some(platform) = inner.platform.as_mut() {
            platform.buffers.remove(slot);
        }
        #[cfg(not(all(target_os = "macos", not(coverage))))]
        let _ = (slot, &mut inner);
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
    /// The runtime command state could not be observed safely.
    RuntimeStatePoisoned,
    /// A checked byte calculation overflowed.
    Overflow(&'static str),
    /// A plan-selected node was absent from the Metal decision.
    UnknownNode(WorkNodeId),
    /// Work attempted another batch after terminal device completion.
    NodeFinished(WorkNodeId),
    /// A pipeline or typed dispatch could not be encoded.
    Encoding(String),
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
            Self::NodeFinished(node) => write!(
                formatter,
                "Metal node {} already finished under this lease",
                node.as_str()
            ),
            Self::Encoding(reason) => write!(formatter, "Metal encoding failed: {reason}"),
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
    terminal: bool,
) -> Result<(), MetalRuntimeError> {
    let node = decision
        .nodes
        .get(&context.node().id)
        .ok_or_else(|| MetalRuntimeError::UnknownNode(context.node().id.clone()))?;
    if context.node().kind != node.kind
        || context.node().domain != node.domain
        || context.node().metal_demand_id() != Some(node.demand_id.as_str())
        || context.resources().len()
            != context
                .node()
                .claims
                .iter()
                .filter(|claim| !terminal || claim.lifetime.retains_fence(FenceKind::Device))
                .count()
        || context
            .node()
            .claims
            .iter()
            .filter(|claim| !terminal || claim.lifetime.retains_fence(FenceKind::Device))
            .any(|claim| {
                !context.resources().iter().any(|resource| {
                    resource.resource() == &claim.resource
                        && resource.amount() == claim.amount
                        && resource.lifetime() == &claim.lifetime
                })
            })
        || context.node().allocations.iter().any(|use_| {
            !context.allocations().iter().any(|allocation| {
                allocation.allocation() == &use_.allocation
                    && allocation.lifetime() == &use_.lifetime
            })
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
    if scheduled_allocations != node.scheduled_allocations {
        return Err(MetalRuntimeError::LeaseMismatch);
    }
    Ok(())
}

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

fn requires_dispatch(kind: WorkKind) -> bool {
    !matches!(kind, WorkKind::Preparation | WorkKind::Jit)
}

#[cfg(all(target_os = "macos", not(coverage)))]
#[allow(
    dead_code,
    reason = "T57 consumes the constrained Metal execution seam"
)]
fn open_platform_state(
    _decision: &MetalExecutionDecision,
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
    Ok(MetalPlatformState {
        device,
        queue,
        buffers: BTreeMap::new(),
        kernels: None,
    })
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn prepare_platform(
    decision: &MetalExecutionDecision,
    inner: &mut MetalExecutionInner,
    node: &WorkNodeId,
) -> Result<(), MetalRuntimeError> {
    if inner.platform.is_none() {
        inner.platform = Some(open_platform_state(decision)?);
    }
    let platform = inner.platform.as_mut().expect("platform initialized");
    let slots = decision.nodes[node]
        .allocations
        .iter()
        .map(|id| &decision.allocation_slots[id])
        .collect::<BTreeSet<_>>();
    let added_bytes: u64 = slots
        .iter()
        .filter(|slot| !platform.buffers.contains_key(**slot))
        .map(|slot| decision.physical_slots[*slot])
        .sum();
    let resident_bytes: u64 = platform
        .buffers
        .values()
        .map(|buffer| buffer.length() as u64)
        .sum();
    if resident_bytes
        .checked_add(added_bytes)
        .is_none_or(|bytes| bytes > platform.device.recommendedMaxWorkingSetSize())
    {
        return Err(MetalRuntimeError::Ineligible(
            "admitted live Metal slots exceed device working-set recommendation".into(),
        ));
    }
    for slot in slots {
        if platform.buffers.contains_key(slot) {
            continue;
        }
        let bytes = decision.physical_slots[slot];
        let maximum_buffer_bytes = platform.device.maxBufferLength() as u64;
        if bytes > maximum_buffer_bytes {
            return Err(MetalRuntimeError::BufferTooLarge {
                slot: slot.clone(),
                bytes,
            });
        }
        let buffer = platform
            .device
            .newBufferWithLength_options(bytes as usize, MTLResourceOptions::StorageModeShared)
            .ok_or_else(|| MetalRuntimeError::AllocationFailed {
                slot: slot.clone(),
                bytes,
            })?;
        platform.buffers.insert(slot.clone(), buffer);
    }
    if platform.kernels.is_none() {
        platform.kernels = Some(
            crate::metal_cube::MetalCubeKernels::compile(&platform.device)
                .map_err(MetalRuntimeError::Encoding)?,
        );
    }
    Ok(())
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn prepare_platform(
    _decision: &MetalExecutionDecision,
    _inner: &mut MetalExecutionInner,
    _node: &WorkNodeId,
) -> Result<(), MetalRuntimeError> {
    Err(MetalRuntimeError::UnsupportedPlatform)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn buffer_region<'p>(
    decision: &MetalExecutionDecision,
    platform: &'p MetalPlatformState,
    node: &WorkNodeId,
    region: MetalBufferRegion<'_>,
) -> Result<(&'p ProtocolObject<dyn MTLBuffer>, usize), MetalRuntimeError> {
    if !decision.nodes[node].allocations.contains(region.allocation)
        || region.bytes == 0
        || region
            .offset
            .checked_add(region.bytes)
            .is_none_or(|end| end as u64 > decision.allocation_bytes[region.allocation])
    {
        return Err(MetalRuntimeError::InvalidPlan(
            "Metal region exceeds this node's logical allocation".into(),
        ));
    }
    let buffer = platform
        .buffers
        .get(&decision.allocation_slots[region.allocation])
        .ok_or_else(|| MetalRuntimeError::InvalidPlan("Metal allocation is not resident".into()))?;
    Ok((buffer.as_ref(), region.offset))
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn execute_platform_batch(
    decision: &MetalExecutionDecision,
    inner: &mut MetalExecutionInner,
    node: &WorkNodeId,
    dispatches: &[CubeDispatch<'_>],
) -> Result<MetalBatchStats, MetalRuntimeError> {
    let platform = inner.platform.as_ref().expect("prepared platform");
    let kernels = platform.kernels.as_ref().expect("prepared pipelines");
    let command = platform
        .queue
        .commandBuffer()
        .ok_or(MetalRuntimeError::CommandQueueUnavailable)?;
    let mut stats = MetalBatchStats {
        batches: 1,
        ..MetalBatchStats::default()
    };
    for dispatch in dispatches {
        let region = |r| buffer_region(decision, platform, node, r);
        let samples = region(dispatch.samples)?;
        let weights = region(dispatch.weights)?;
        let grid = region(dispatch.grid)?;
        let count = dispatch.count as usize;
        let output = match dispatch.kind {
            CubeDispatchKind::Grid => dispatch.grid,
            CubeDispatchKind::Degrid { predicted } => predicted,
        };
        if count == 0
            || dispatch.width < 7
            || dispatch.height < 7
            || dispatch.samples.offset % 8 != 0
            || dispatch.grid.offset % 8 != 0
            || dispatch.weights.offset % 4 != 0
            || count
                .checked_mul(size_of::<crate::metal_cube::CubeTap>())
                .is_none_or(|bytes| bytes > dispatch.samples.bytes)
            || (dispatch.width as usize)
                .checked_mul(dispatch.height as usize)
                .and_then(|cells| cells.checked_mul(8))
                .is_none_or(|bytes| bytes > dispatch.grid.bytes)
            || dispatch.weights.bytes < 7 * 4
            || dispatch.width.checked_mul(dispatch.height).is_none()
            || output.overlaps(dispatch.samples)
            || output.overlaps(dispatch.weights)
            || (matches!(dispatch.kind, CubeDispatchKind::Degrid { .. })
                && output.overlaps(dispatch.grid))
        {
            return Err(MetalRuntimeError::InvalidPlan(
                "invalid cube dispatch shape or buffer capacity".into(),
            ));
        }
        // Validate the externally encoded tap layout before the GPU can access
        // it. This is a bounded input check, never a grid-content verification.
        let taps = unsafe {
            std::slice::from_raw_parts(
                samples
                    .0
                    .contents()
                    .as_ptr()
                    .cast::<u8>()
                    .add(samples.1)
                    .cast::<crate::metal_cube::CubeTap>(),
                count,
            )
        };
        let weight_rows = dispatch.weights.bytes / (7 * 4);
        if taps.iter().any(|tap| {
            tap.x > dispatch.width - 7
                || tap.y > dispatch.height - 7
                || tap.x_weights as usize >= weight_rows
                || tap.y_weights as usize >= weight_rows
        }) {
            return Err(MetalRuntimeError::InvalidPlan(
                "cube tap exceeds grid or convolution table".into(),
            ));
        }
        match dispatch.kind {
            CubeDispatchKind::Grid => {
                kernels
                    .encode_grid(
                        &command,
                        samples,
                        weights,
                        grid,
                        dispatch.count,
                        dispatch.width,
                        dispatch.height,
                    )
                    .map_err(MetalRuntimeError::Encoding)?;
                stats.grid_samples += u64::from(dispatch.count);
            }
            CubeDispatchKind::Degrid { predicted } => {
                if predicted.offset % 8 != 0
                    || count
                        .checked_mul(8)
                        .is_none_or(|bytes| bytes > predicted.bytes)
                {
                    return Err(MetalRuntimeError::InvalidPlan(
                        "invalid predicted visibility capacity".into(),
                    ));
                }
                kernels
                    .encode_degrid(
                        &command,
                        samples,
                        weights,
                        grid,
                        region(predicted)?,
                        dispatch.count,
                        dispatch.width,
                        dispatch.height,
                    )
                    .map_err(MetalRuntimeError::Encoding)?;
                stats.degrid_samples += u64::from(dispatch.count);
            }
        }
    }
    let started = Instant::now();
    command.commit();
    command.waitUntilCompleted();
    if command.status() != MTLCommandBufferStatus::Completed {
        return Err(MetalRuntimeError::CommandFailed {
            node: node.clone(),
            status: command.status().0 as u64,
        });
    }
    stats.submit_wait_seconds = started.elapsed().as_secs_f64();
    stats.gpu_seconds = (command.GPUEndTime() - command.GPUStartTime()).max(0.0);
    Ok(stats)
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn execute_platform_batch(
    _decision: &MetalExecutionDecision,
    _inner: &mut MetalExecutionInner,
    _node: &WorkNodeId,
    _dispatches: &[CubeDispatch<'_>],
) -> Result<MetalBatchStats, MetalRuntimeError> {
    Err(MetalRuntimeError::UnsupportedPlatform)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn close_platform(inner: &mut MetalExecutionInner) -> Result<(), MetalRuntimeError> {
    inner.platform = None;
    Ok(())
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn close_platform(_inner: &mut MetalExecutionInner) -> Result<(), MetalRuntimeError> {
    Ok(())
}

#[cfg(all(test, not(all(target_os = "macos", not(coverage)))))]
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

    fn specification(plan: &ExecutionDag) -> ExecutionDagSpecification {
        ExecutionDagSpecification {
            required_resource_capabilities: plan.required_resource_capabilities().clone(),
            resource_alternative: plan.resource_alternative().clone(),
            nodes: plan.nodes().values().cloned().collect(),
            logical_allocations: plan.logical_allocations().values().cloned().collect(),
            physical_slots: plan.physical_slots().values().cloned().collect(),
            initial_knobs: plan.initial_knobs().clone(),
            adaptations: plan.adaptations().values().cloned().collect(),
        }
    }

    fn mixed_specification() -> (ExecutionDagSpecification, ResourceTopology) {
        let (plan, mut topology) = metal_plan(2);
        let mut spec = specification(&plan);
        let node = &mut spec.nodes[0];
        node.domain = WorkDomain::Io;
        node.kind = WorkKind::ObservationRead;
        node.fences.insert(FenceKind::Io);
        let lifetime = node.payload_lifetime();
        for claim in &mut node.claims {
            claim.lifetime = lifetime.clone();
        }
        for allocation in &mut node.allocations {
            allocation.lifetime = lifetime.clone();
        }
        for allocation in &mut spec.logical_allocations {
            allocation
                .lifetime
                .release_after
                .insert(crate::WorkDependency::Fence(crate::FenceId::new(
                    node.id.clone(),
                    FenceKind::Io,
                )));
        }
        let measurement_set = casa_imaging_model::MeasurementSetIdentity::new(
            casa_imaging_model::LogicalIdentity::from_sha256([1; 32]),
        );
        for resource in [
            LeaseResource::MeasurementSetLock { measurement_set },
            LeaseResource::Queue {
                demand_id: "reader-queue".into(),
            },
            LeaseResource::Rate {
                demand_id: "reader-rate".into(),
            },
        ] {
            node.claims.push(ResourceClaim {
                resource,
                amount: 1,
                lifetime: lifetime.clone(),
            });
        }
        // The source/CPU slot is deliberately not GPU-visible.
        let host_views = BTreeSet::from([CapacityViewId::new("host")]);
        spec.logical_allocations[1].compatibility.storage_mode = StorageMode::Host;
        spec.logical_allocations[1].compatibility.views = host_views.clone();
        spec.physical_slots[1].compatibility = spec.logical_allocations[1].compatibility.clone();
        let demand = &mut spec.resource_alternative.demand;
        demand.memory[1].views = host_views.into_iter().collect();
        demand.locks = CountDemand::new(1, 1);
        let queue = QueueResourceId::new("reader");
        let rate = crate::RateResourceId::new("reader");
        demand.queues.push(QueueDemand {
            demand_id: "reader-queue".into(),
            resource: queue.clone(),
            slots: CountDemand::new(1, 1),
        });
        demand.rates.push(crate::RateDemand {
            demand_id: "reader-rate".into(),
            resource: rate.clone(),
            amount: CountDemand::new(1, 1),
        });
        topology.queue_resources.push(QueueResource::new(queue, 1));
        topology.rate_resources.push(crate::RateResource::new(
            rate,
            crate::RateUnit::BytesPerSecond,
            1,
        ));
        (spec, topology)
    }

    fn authority(topology: ResourceTopology) -> crate::ResourceAuthority {
        let pressure = ExternalPressure {
            memory_available_bytes: topology
                .memory_domains
                .iter()
                .map(|d| (d.id.clone(), d.capacity_bytes))
                .collect(),
            available_cpu_threads: topology.logical_cpu_threads,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: topology
                .rate_resources
                .iter()
                .map(|r| (r.id.clone(), 1))
                .collect(),
            queue_available_slots: topology
                .queue_resources
                .iter()
                .map(|q| (q.id.clone(), q.slots))
                .collect(),
            accelerator_available_slots: topology
                .accelerators
                .iter()
                .map(|a| (a.id.clone(), a.occupancy_slots))
                .collect(),
            cache_available_bytes: topology.cache_capacity_bytes,
            available_locks: topology.lock_capacity,
            available_file_descriptors: topology.file_descriptor_capacity,
        };
        crate::ResourceAuthority::with_inventory(HostInventory { topology, pressure })
            .expect("fixture inventory")
    }

    #[test]
    fn mixed_observation_retains_host_and_device_claims_through_both_fence_orders() {
        for order in [
            [FenceKind::Io, FenceKind::Device],
            [FenceKind::Device, FenceKind::Io],
        ] {
            let (spec, topology) = mixed_specification();
            let plan = ExecutionDag::new(spec).expect("mixed observation DAG");
            let decision = MetalExecutionDecision::bind(&plan, &topology).expect("mixed decision");
            assert_eq!(decision.physical_slots().len(), 1);
            assert_eq!(
                decision
                    .nodes()
                    .values()
                    .next()
                    .unwrap()
                    .allocations()
                    .len(),
                1
            );
            let authority = authority(topology);
            let mut scheduler =
                ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &authority, None)
                    .expect("mixed admission");
            let SchedulerAction::Work(work) = scheduler.next_action().expect("read") else {
                panic!("read work");
            };
            assert!(
                work.metal_execution().is_some(),
                "I/O-only DAG must bind its declared accelerator"
            );
            let id = work.node().id.clone();
            scheduler
                .finish_work(id.clone(), crate::execution::WorkResult::Succeeded)
                .expect("work returns");
            scheduler
                .complete_fence(crate::FenceId::new(id.clone(), order[0]))
                .expect("first fence");
            assert!(matches!(
                scheduler.next_action().unwrap(),
                SchedulerAction::Waiting { .. }
            ));
            assert_eq!(work.for_fence(order[1]).allocations().len(), 2);
            scheduler
                .complete_fence(crate::FenceId::new(id.clone(), order[1]))
                .expect("second fence");
            drop(scheduler.take_observation_completion_permits(&id));
            assert!(matches!(
                scheduler.next_action().unwrap(),
                SchedulerAction::Complete(_)
            ));
        }
    }

    #[test]
    fn mixed_observation_rejects_missing_fences_and_mismatched_accelerator_claims() {
        for mutation in 0..5 {
            let (mut spec, _) = mixed_specification();
            let node = &mut spec.nodes[0];
            match mutation {
                0 => {
                    node.fences.remove(&FenceKind::Device);
                }
                1 => {
                    node.fences.remove(&FenceKind::Io);
                }
                2 => {
                    node.claims.retain(|c| {
                        !matches!(c.resource, LeaseResource::AcceleratorCommandQueue { .. })
                    });
                }
                3 => {
                    node.claims[1].resource = LeaseResource::AcceleratorCommandQueue {
                        demand_id: "wrong".into(),
                    };
                }
                _ => {
                    node.domain = WorkDomain::Cpu;
                }
            }
            assert!(ExecutionDag::new(spec).is_err(), "mutation {mutation}");
        }
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    #[ignore = "requires a process-accessible Apple Metal device"]
    fn mixed_observation_executes_repeated_batches_before_one_terminal_device_fence() {
        let (mut spec, mut topology) = mixed_specification();
        spec.logical_allocations[0].bytes = 4096;
        spec.physical_slots[0].capacity_bytes = 4096;
        spec.resource_alternative.demand.memory[0].hard_bytes = 4096;
        spec.resource_alternative.demand.memory[0].preferred_bytes = 4096;
        topology.memory_domains[0].capacity_bytes = 8192;
        let plan = ExecutionDag::new(spec).expect("mixed GPU arena");
        let authority = authority(topology);
        let mut scheduler =
            ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &authority, None)
                .expect("admit");
        let SchedulerAction::Work(work) = scheduler.next_action().unwrap() else {
            panic!("read");
        };
        let runtime = work.metal_execution().expect("runtime");
        let problem = crate::execution::tests::compiled_problem();
        let completed = BTreeMap::new();
        let prediction = crate::StagePrediction::new(work.node().id.clone(), 1);
        let context = WorkExecutionContext::for_test(
            ExecutionAttemptId::from_sha256([1; 32]),
            crate::execution_bindings::WorkExecutionTestBindings::new(
                &problem,
                crate::ImplementationRegistryId::from_sha256([1; 32]),
                &completed,
            ),
            &work,
            &[],
            &prediction,
            plan.resource_alternative(),
        );
        runtime.prepare(context).expect("prepare");
        let stale = WorkExecutionContext::for_test(
            ExecutionAttemptId::from_sha256([2; 32]),
            crate::execution_bindings::WorkExecutionTestBindings::new(
                &problem,
                crate::ImplementationRegistryId::from_sha256([1; 32]),
                &completed,
            ),
            &work,
            &[],
            &prediction,
            plan.resource_alternative(),
        );
        assert_eq!(
            runtime.prepare(stale),
            Err(MetalRuntimeError::LeaseMismatch)
        );
        assert!(
            !runtime.submitted(&work.node().id).unwrap(),
            "preparation is not a science dispatch"
        );
        let access = runtime.batch_access(context).expect("scoped batch access");
        assert!(access.execute(&[]).is_err());
        let allocation = AllocationId::new("allocation-0");
        let region = |offset, bytes| MetalBufferRegion {
            allocation: &allocation,
            offset,
            bytes,
        };
        access
            .with_bytes(region(0, 4096), |bytes| {
                bytes.fill(0);
                for (index, value) in [3_u32, 4, 0, 0].iter().enumerate() {
                    bytes[index * 4..index * 4 + 4].copy_from_slice(&value.to_ne_bytes());
                }
                bytes[16..20].copy_from_slice(&1.25_f32.to_ne_bytes());
                bytes[20..24].copy_from_slice(&(-0.5_f32).to_ne_bytes());
                for value in bytes[64..92].chunks_exact_mut(4) {
                    value.copy_from_slice(&1.0_f32.to_ne_bytes());
                }
            })
            .expect("mapped arena");
        let mut dispatch = CubeDispatch {
            kind: CubeDispatchKind::Grid,
            samples: region(0, 24),
            weights: region(64, 28),
            grid: region(128, 2048),
            count: 1,
            width: 16,
            height: 16,
        };
        let mut oversized = dispatch;
        oversized.count = 2;
        assert!(access.execute(&[oversized]).is_err());
        let mut aliased = dispatch;
        aliased.kind = CubeDispatchKind::Degrid {
            predicted: region(128, 8),
        };
        assert!(access.execute(&[aliased]).is_err());
        assert!(!runtime.submitted(&work.node().id).unwrap());
        for _ in 0..2 {
            let stats = access.execute(&[dispatch]).expect("grid batch");
            assert_eq!(stats.batches, 1);
            assert_eq!(stats.grid_samples, 1);
        }
        dispatch.kind = CubeDispatchKind::Degrid {
            predicted: region(2176, 8),
        };
        access.execute(&[dispatch]).expect("degrid batch");
        access
            .with_bytes(region(2176, 8), |bytes| {
                assert_eq!(f32::from_ne_bytes(bytes[0..4].try_into().unwrap()), 122.5);
                assert_eq!(f32::from_ne_bytes(bytes[4..8].try_into().unwrap()), -49.0);
            })
            .expect("completed prediction");
        assert!(runtime.submitted(&work.node().id).unwrap());
        assert_eq!(
            runtime.inner.lock().unwrap().nodes[&work.node().id]
                .stats
                .batches,
            3
        );
        let slot = PhysicalSlotId::new("slot-0");
        let identity = runtime.buffer_identity(&slot);
        assert!(identity.is_some());
        let id = work.node().id.clone();
        scheduler
            .finish_work(id.clone(), crate::execution::WorkResult::Succeeded)
            .unwrap();
        scheduler
            .complete_fence(crate::FenceId::new(id.clone(), FenceKind::Io))
            .unwrap();
        assert_eq!(
            runtime.buffer_identity(&slot),
            identity,
            "I/O completion must not release device payload"
        );
        let fence = work.for_fence(FenceKind::Device);
        let fence_context = WorkExecutionContext::for_test(
            ExecutionAttemptId::from_sha256([1; 32]),
            crate::execution_bindings::WorkExecutionTestBindings::new(
                &problem,
                crate::ImplementationRegistryId::from_sha256([1; 32]),
                &completed,
            ),
            &fence,
            &[],
            &prediction,
            plan.resource_alternative(),
        );
        runtime
            .finish(fence_context)
            .expect("terminal evidence after prior batch waits");
        assert_eq!(
            runtime.finish(fence_context),
            Err(MetalRuntimeError::FenceAlreadySettled)
        );
        assert!(matches!(
            access.execute(&[dispatch]),
            Err(MetalRuntimeError::NodeFinished(_))
        ));
        scheduler
            .complete_fence(crate::FenceId::new(id.clone(), FenceKind::Device))
            .unwrap();
        assert_eq!(runtime.buffer_identity(&slot), None);
        drop(scheduler.take_observation_completion_permits(&id));
        assert!(matches!(
            scheduler.next_action().unwrap(),
            SchedulerAction::Complete(_)
        ));
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
                    disposition: crate::AllocationDisposition::Release,
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

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    #[ignore = "requires a process-accessible Apple Metal device; explicit T55/T57 numerics discriminator"]
    fn t57_native_f64_kernel_capability_discriminator() {
        let (plan, topology) = metal_plan(1);
        let decision = MetalExecutionDecision::bind(&plan, &topology).expect("Metal decision");
        let platform =
            open_platform_state(&decision).expect("discriminator requires the actual device");
        let source = objc2_foundation::NSString::from_str(
            "#include <metal_stdlib>\nusing namespace metal;\nkernel void f64_probe(device double *values [[buffer(0)]], uint index [[thread_position_in_grid]]) { values[index] = values[index] * 0.5; }",
        );
        match platform
            .device
            .newLibraryWithSource_options_error(&source, None)
        {
            Ok(_) => println!(
                "t57_native_f64_compile supported=true device={}",
                platform.device.name()
            ),
            Err(error) => {
                let diagnostic = error.localizedDescription().to_string();
                println!(
                    "t57_native_f64_compile supported=false device={} diagnostic={diagnostic}",
                    platform.device.name()
                );
                assert!(
                    diagnostic.contains("double") && diagnostic.contains("not supported"),
                    "unexpected compiler failure is not F64 capability evidence"
                );
            }
        }
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
    #[ignore = "requires a process-accessible Apple Metal device"]
    fn apple_slots_materialize_on_use_and_retire_at_ledger_release() {
        let (plan, topology) = metal_plan_with_nodes(1, 2);
        let state = MetalExecutionState::bind(&plan, &topology, 1).expect("Metal execution");
        let platform = open_platform_state(&state.decision).expect("actual Metal device");
        assert!(platform.buffers.is_empty());
        state.inner.lock().expect("runtime state").platform = Some(platform);
        let slot = PhysicalSlotId::new("slot-0");
        let mut identity = None;
        for index in 0..2 {
            let node = WorkNodeId::new(format!("metal-work-{index}"));
            let mut inner = state.inner.lock().expect("runtime state");
            prepare_platform(&state.decision, &mut inner, &node).expect("admitted residency");
            drop(inner);
            if index == 0 {
                identity = state.buffer_identity(&slot);
            }
            assert!(identity.is_some());
            assert_eq!(state.buffer_identity(&slot), identity);
        }
        state
            .release_allocation(&AllocationId::new("allocation-0"))
            .expect("ledger release");
        assert_eq!(state.buffer_identity(&slot), None);
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
