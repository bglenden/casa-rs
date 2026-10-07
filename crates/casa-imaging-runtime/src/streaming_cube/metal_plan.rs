// SPDX-License-Identifier: LGPL-3.0-or-later

//! Declare a shared spatial arena on an existing observation or replay stage.

use crate::*;
use std::{collections::BTreeSet, io};

// Initial qualification envelopes for the system Metal driver/compiler and one
// synchronous command. Scientific buffers use exact shape/capacity formulas.
const DRIVER_BYTES: u64 = 96 << 20;
const JIT_BYTES: u64 = 32 << 20;
const COMMAND_BYTES: u64 = 16 << 20;

pub(crate) fn compose(
    base: PhysicalWorkBinding,
    authority: &ResourceAuthority,
    read: &WorkNodeId,
    reconcile: &WorkNodeId,
    allocation: &AllocationId,
    bytes: u64,
) -> io::Result<PhysicalWorkBinding> {
    let topology = authority.topology();
    let accelerator = topology
        .accelerators
        .iter()
        .find(|a| a.kind == AcceleratorKind::Metal)
        .ok_or_else(|| {
            io::Error::other("explicit Metal cube requires an available Metal device")
        })?;
    let metal = topology
        .memory_views
        .iter()
        .find(|v| v.id == accelerator.memory_view)
        .ok_or_else(|| io::Error::other("Metal memory view missing"))?;
    let host = topology
        .memory_views
        .iter()
        .find(|v| v.kind == MemoryViewKind::Host && v.domain == metal.domain)
        .ok_or_else(|| io::Error::other("Metal cube requires unified host/device memory"))?;
    let mut alternative = base.execution_dag().resource_alternative().clone();
    let demand_id = format!("{}-spatial-metal", read.as_str());
    alternative.demand.accelerators.push(AcceleratorDemand {
        demand_id: demand_id.clone(),
        accelerator: accelerator.id.clone(),
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    });
    let overhead = &mut alternative.demand.overhead;
    overhead.driver_bytes += DRIVER_BYTES;
    overhead.jit_bytes += JIT_BYTES;
    overhead.command_buffer_bytes += COMMAND_BYTES;
    let slot = PhysicalSlotId::new(format!("{}-slot", allocation.as_str()));
    let compatibility = SlotCompatibility {
        memory_domain: metal.domain.clone(),
        views: BTreeSet::from([host.id.clone(), metal.id.clone()]),
        alignment_bytes: 64,
        storage_mode: StorageMode::MetalShared,
        layout: AllocationLayout::new("spatial-arena"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: slot.as_str().to_string(),
        hard_bytes: bytes,
        preferred_bytes: bytes,
        views: compatibility.views.iter().cloned().collect(),
    });
    let lifetime = ClaimLifetime::Fences(BTreeSet::from([FenceKind::Io, FenceKind::Device]));
    let device = WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Device));
    let io = WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io));
    let mut nodes: Vec<_> = base.execution_dag().nodes().values().cloned().collect();
    let owner = nodes
        .iter_mut()
        .find(|n| n.id == *read)
        .expect("observation owner");
    owner.fences.insert(FenceKind::Device);
    for claim in &mut owner.claims {
        if claim.lifetime == ClaimLifetime::through_fence(FenceKind::Io) {
            claim.lifetime = lifetime.clone();
        }
    }
    for usage in &mut owner.allocations {
        if usage.lifetime == ClaimLifetime::through_fence(FenceKind::Io) {
            usage.lifetime = lifetime.clone();
        }
    }
    for (resource, amount) in [
        (
            LeaseResource::Accelerator {
                demand_id: demand_id.clone(),
            },
            1,
        ),
        (LeaseResource::AcceleratorCommandQueue { demand_id }, 1),
        (
            LeaseResource::RuntimeOverhead(RuntimeOverheadKind::Driver),
            DRIVER_BYTES,
        ),
        (
            LeaseResource::RuntimeOverhead(RuntimeOverheadKind::Jit),
            JIT_BYTES,
        ),
        (
            LeaseResource::RuntimeOverhead(RuntimeOverheadKind::CommandBuffer),
            COMMAND_BYTES,
        ),
    ] {
        owner.claims.push(ResourceClaim {
            resource,
            amount,
            lifetime: lifetime.clone(),
        });
    }
    owner.allocations.push(AllocationUse {
        allocation: allocation.clone(),
        lifetime,
    });
    nodes
        .iter_mut()
        .find(|n| n.id == *reconcile)
        .expect("reconciliation owner")
        .dependencies
        .insert(device.clone());
    let mut logical: Vec<_> = base
        .execution_dag()
        .logical_allocations()
        .values()
        .cloned()
        .collect();
    for item in &mut logical {
        if item.lifetime.release_after.contains(&io) {
            item.lifetime.release_after.insert(device.clone());
        }
    }
    logical.push(LogicalAllocation {
        id: allocation.clone(),
        bytes,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: read.clone(),
            release_after: BTreeSet::from([io, device]),
            disposition: AllocationDisposition::Release,
        },
    });
    let mut slots: Vec<_> = base
        .execution_dag()
        .physical_slots()
        .values()
        .cloned()
        .collect();
    slots.push(PhysicalSlot {
        id: slot.clone(),
        lease_resource: LeaseResource::Memory {
            allocation_id: slot.as_str().to_string(),
        },
        capacity_bytes: bytes,
        compatibility,
    });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base
            .execution_dag()
            .required_resource_capabilities()
            .clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations: logical,
        physical_slots: slots,
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })
    .map_err(io::Error::other)?;
    PhysicalWorkBinding::with_implementation_contract(
        base.implementation_contract()
            .for_execution_dag(&dag)
            .map_err(io::Error::other)?,
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
        base.product_publication_plan(),
    )
    .map_err(io::Error::other)
}
