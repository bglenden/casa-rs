// SPDX-License-Identifier: LGPL-3.0-or-later

use super::{
    tests::{cpu_node, io_authority_with_workers_and_memory, plan_spec},
    *,
};
use crate::{
    MemoryDemand, QueueDemand, QueueResourceId, RateDemand, RateResourceId, ResourcePolicy,
};

fn artifact_specification() -> ExecutionDagSpecification {
    let owner = WorkNodeId::new("seal");
    let mut node = cpu_node(owner.as_str(), BTreeSet::new());
    node.domain = WorkDomain::Io;
    node.fences.insert(FenceKind::Io);
    node.claims.extend([
        ResourceClaim {
            resource: LeaseResource::Queue {
                demand_id: "export-queue".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: LeaseResource::Rate {
                demand_id: "export-rate".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
    ]);
    let mut specification = plan_spec(vec![node]);
    specification
        .resource_alternative
        .demand
        .queues
        .push(QueueDemand {
            demand_id: "export-queue".to_string(),
            resource: QueueResourceId::new("io-queue"),
            slots: CountDemand::new(1, 1),
        });
    specification
        .resource_alternative
        .demand
        .rates
        .push(RateDemand {
            demand_id: "export-rate".to_string(),
            resource: RateResourceId::new("io-rate"),
            amount: CountDemand::new(1, 1),
        });
    for (name, bytes, disposition) in [
        (
            "metadata",
            200,
            AllocationDisposition::ExportImmutableArtifact {
                owner_node: owner.clone(),
            },
        ),
        ("transient", 600, AllocationDisposition::Release),
    ] {
        let compatibility = SlotCompatibility {
            memory_domain: CapacityDomainId::new("host-memory"),
            views: BTreeSet::from([CapacityViewId::new("host-memory")]),
            alignment_bytes: 8,
            storage_mode: StorageMode::Host,
            layout: AllocationLayout::new(name),
            initialization: InitializationPolicy::OverwriteBeforeRead,
            access: AllocationAccess::ReadOnly,
        };
        specification
            .resource_alternative
            .demand
            .memory
            .push(MemoryDemand {
                allocation_id: name.to_string(),
                hard_bytes: bytes,
                preferred_bytes: bytes,
                views: compatibility.views.iter().cloned().collect(),
            });
        specification.nodes[0].allocations.push(AllocationUse {
            allocation: AllocationId::new(name),
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        });
        specification.logical_allocations.push(LogicalAllocation {
            id: AllocationId::new(name),
            bytes,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new(name),
            lifetime: AllocationLifetime {
                acquire_at: owner.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Work(owner.clone()),
                    WorkDependency::Fence(FenceId::new(owner.clone(), FenceKind::Io)),
                ]),
                disposition,
            },
        });
        specification.physical_slots.push(PhysicalSlot {
            id: PhysicalSlotId::new(name),
            lease_resource: LeaseResource::Memory {
                allocation_id: name.to_string(),
            },
            capacity_bytes: bytes,
            compatibility,
        });
    }
    specification
}

fn can_admit(authority: &ResourceAuthority, bytes: u64) -> bool {
    let mut demand = plan_spec(Vec::new()).resource_alternative;
    demand.demand.memory.push(MemoryDemand {
        allocation_id: "competing".to_string(),
        hard_bytes: bytes,
        preferred_bytes: bytes,
        views: vec![CapacityViewId::new("host-memory")],
    });
    authority
        .acquire(
            ResourcePolicy::Exclusive,
            DemandAlternatives {
                required_capabilities: BTreeSet::new(),
                alternatives: vec![demand],
            },
        )
        .is_ok()
}

#[test]
fn t55_artifact_export_preserves_memory_until_the_final_owning_alias() {
    let dag = ExecutionDag::new(artifact_specification()).expect("immutable export plan");
    let authority = io_authority_with_workers_and_memory(2, 1024);
    let mut scheduler =
        ExecutionScheduler::start(&dag, &ResourcePolicy::Exclusive, &authority, None)
            .expect("admitted export plan");
    let SchedulerAction::Work(work) = scheduler.next_action().expect("dispatch") else {
        panic!("producer must dispatch");
    };
    let owner = work.node().id.clone();
    let epoch = work.lease_epoch();
    assert!(can_admit(&authority, 224));
    assert!(!can_admit(&authority, 225));
    scheduler
        .finish_work(owner.clone(), WorkResult::Succeeded)
        .expect("producer returns");
    assert!(
        scheduler
            .take_artifact_permit(&owner)
            .expect("fence still pending")
            .is_none()
    );
    scheduler
        .complete_fence(FenceId::new(owner.clone(), FenceKind::Io))
        .expect("I/O settles");
    assert!(
        !can_admit(&authority, 225),
        "no release/reacquire gap before sealing"
    );
    let artifact = scheduler
        .take_artifact_permit(&owner)
        .expect("successful scientific sealing")
        .expect("exported metadata");
    assert_eq!(artifact.lease_epoch(), epoch);
    assert!(artifact.covers_exact_immutable_allocation(
        &owner,
        &dag.logical_allocations[&AllocationId::new("metadata")]
    ));
    assert!(
        scheduler.take_artifact_permit(&owner).is_err(),
        "one-shot export"
    );
    assert_eq!(
        scheduler.next_action().expect("terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Succeeded)
    );
    assert!(can_admit(&authority, 824));
    assert!(!can_admit(&authority, 825));
    let artifact = Arc::new(artifact);
    let reader = Arc::clone(&artifact);
    let worker = Arc::clone(&reader);
    drop(artifact);
    drop(reader);
    assert!(
        !can_admit(&authority, 825),
        "last worker owns the original reservation"
    );
    drop(worker);
    assert!(can_admit(&authority, 1024));
}

#[test]
fn t55_artifact_export_proof_binds_every_allocation_identity_field() {
    let dag = ExecutionDag::new(artifact_specification()).expect("immutable export plan");
    let authority = io_authority_with_workers_and_memory(2, 1024);
    let mut scheduler =
        ExecutionScheduler::start(&dag, &ResourcePolicy::Exclusive, &authority, None)
            .expect("admitted plan");
    let SchedulerAction::Work(work) = scheduler.next_action().expect("dispatch") else {
        panic!("producer");
    };
    let owner = work.node().id.clone();
    scheduler
        .finish_work(owner.clone(), WorkResult::Succeeded)
        .expect("work");
    scheduler
        .complete_fence(FenceId::new(owner.clone(), FenceKind::Io))
        .expect("fence");
    let permit = scheduler
        .take_artifact_permit(&owner)
        .expect("export")
        .expect("metadata");
    let allocation = &dag.logical_allocations[&AllocationId::new("metadata")];
    assert!(
        !permit.covers_exact_immutable_allocation(&WorkNodeId::new("another-producer"), allocation)
    );
    for field in 0..10 {
        let mut changed = allocation.clone();
        match field {
            0 => changed.id = AllocationId::new("other"),
            1 => changed.bytes += 1,
            2 => changed.physical_slot = PhysicalSlotId::new("other"),
            3 => changed.compatibility.layout = AllocationLayout::new("other-program"),
            4 => changed.compatibility.memory_domain = CapacityDomainId::new("other"),
            5 => changed.compatibility.views = BTreeSet::from([CapacityViewId::new("other")]),
            6 => changed.compatibility.access = AllocationAccess::ReadWrite,
            7 => changed.lifetime.acquire_at = WorkNodeId::new("other"),
            8 => changed.lifetime.release_after.clear(),
            9 => changed.lifetime.disposition = AllocationDisposition::Release,
            _ => unreachable!(),
        }
        assert!(
            !permit.covers_exact_immutable_allocation(&owner, &changed),
            "changed field {field}"
        );
    }
    drop(permit);
    assert_eq!(
        scheduler.next_action().expect("early-drop finalization"),
        SchedulerAction::Complete(SchedulerTerminal::Succeeded)
    );
    assert!(can_admit(&authority, 1024));
}

#[test]
fn t55_artifact_export_rejects_mutable_nonhost_oversized_and_reused_slots() {
    for violation in 0..6 {
        let mut specification = artifact_specification();
        match violation {
            0 => {
                specification.logical_allocations[0].compatibility.access =
                    AllocationAccess::ReadWrite;
                specification.physical_slots[0].compatibility.access = AllocationAccess::ReadWrite;
            }
            1 => {
                specification.logical_allocations[0]
                    .compatibility
                    .storage_mode = StorageMode::MetalShared;
                specification.physical_slots[0].compatibility.storage_mode =
                    StorageMode::MetalShared;
            }
            2 => specification.logical_allocations[0].bytes -= 1,
            3 => {
                let mut alias = specification.logical_allocations[0].clone();
                alias.id = AllocationId::new("alias");
                alias.lifetime.disposition = AllocationDisposition::Release;
                specification.nodes[0].allocations.push(AllocationUse {
                    allocation: alias.id.clone(),
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                });
                specification.logical_allocations.push(alias);
            }
            4 => {
                specification.logical_allocations[0].lifetime.disposition =
                    AllocationDisposition::ExportImmutableArtifact {
                        owner_node: WorkNodeId::new("absent"),
                    }
            }
            5 => specification.nodes[0].allocations[0].lifetime = ClaimLifetime::Artifact,
            _ => unreachable!(),
        }
        assert!(
            ExecutionDag::new(specification).is_err(),
            "violation {violation}"
        );
    }
}

#[test]
fn t55_artifact_claim_rejects_workers_but_accepts_file_descriptors() {
    let mut workers = artifact_specification();
    workers.nodes[0].claims.push(ResourceClaim {
        resource: LeaseResource::Workers,
        amount: 1,
        lifetime: ClaimLifetime::Artifact,
    });
    assert!(
        ExecutionDag::new(workers).is_err(),
        "workers cannot be retained by an immutable artifact"
    );

    let mut file_descriptors = artifact_specification();
    file_descriptors
        .resource_alternative
        .demand
        .file_descriptors = CountDemand::new(1, 1);
    file_descriptors.nodes[0].claims.push(ResourceClaim {
        resource: LeaseResource::FileDescriptors,
        amount: 1,
        lifetime: ClaimLifetime::Artifact,
    });
    assert!(
        ExecutionDag::new(file_descriptors).is_ok(),
        "file descriptors are valid artifact-retained external handles"
    );
}

#[test]
fn t55_artifact_export_drains_work_fence_sealing_and_transfer_failures() {
    for failed_stage in 0..4 {
        let dag = ExecutionDag::new(artifact_specification()).expect("immutable export plan");
        let authority = io_authority_with_workers_and_memory(2, 1024);
        let mut scheduler =
            ExecutionScheduler::start(&dag, &ResourcePolicy::Exclusive, &authority, None)
                .expect("admitted plan");
        let SchedulerAction::Work(work) = scheduler.next_action().expect("dispatch") else {
            panic!("producer");
        };
        let owner = work.node().id.clone();
        scheduler
            .finish_work(
                owner.clone(),
                if failed_stage == 0 {
                    WorkResult::Failed {
                        message: "producer failure".to_string(),
                    }
                } else {
                    WorkResult::Succeeded
                },
            )
            .expect("work settles");
        let fence = FenceId::new(owner.clone(), FenceKind::Io);
        if failed_stage == 1 {
            scheduler
                .fail_fence(fence, "I/O failure".to_string())
                .expect("fence failure drains");
        } else {
            scheduler.complete_fence(fence).expect("I/O settles");
        }
        if failed_stage == 3 {
            drop(
                scheduler
                    .take_artifact_permit(&owner)
                    .expect("transfer")
                    .expect("metadata"),
            );
        }
        scheduler.cancel_after_error();
        assert!(scheduler.take_artifact_permit(&owner).is_err());
        assert!(matches!(
            scheduler.next_action().expect("failed attempt drains"),
            SchedulerAction::Complete(
                SchedulerTerminal::Failed { .. } | SchedulerTerminal::Cancelled
            )
        ));
        assert!(can_admit(&authority, 1024), "failed stage {failed_stage}");
    }
}

#[test]
fn t55_nonexporting_completion_preserves_the_original_drain_error() {
    let mut specification = artifact_specification();
    specification.logical_allocations[0].lifetime.disposition = AllocationDisposition::Release;
    let dag = ExecutionDag::new(specification).unwrap();
    let authority = io_authority_with_workers_and_memory(2, 1024);
    let mut scheduler =
        ExecutionScheduler::start(&dag, &ResourcePolicy::Exclusive, &authority, None).unwrap();
    let SchedulerAction::Work(work) = scheduler.next_action().unwrap() else {
        panic!("producer");
    };
    let owner = work.node().id.clone();
    scheduler
        .finish_work(
            owner.clone(),
            WorkResult::Failed {
                message: "original failure".to_string(),
            },
        )
        .unwrap();
    scheduler
        .complete_fence(FenceId::new(owner.clone(), FenceKind::Io))
        .unwrap();
    assert!(scheduler.take_artifact_permit(&owner).unwrap().is_none());
    assert!(matches!(scheduler.next_action().unwrap(),
        SchedulerAction::Complete(SchedulerTerminal::Failed { message, .. })
            if message == "original failure"));
    assert!(can_admit(&authority, 1024));
}

#[test]
fn t55_artifact_terminal_disposition_is_part_of_the_physical_work_identity() {
    let retained = ExecutionDag::new(artifact_specification()).expect("retained plan");
    let mut specification = artifact_specification();
    specification.logical_allocations[0].lifetime.disposition = AllocationDisposition::Release;
    let transient = ExecutionDag::new(specification).expect("transient plan");
    assert_ne!(retained.physical_work_id(), transient.physical_work_id());
}

#[test]
fn t55_artifact_export_stays_charged_when_a_later_node_fails() {
    let mut specification = artifact_specification();
    let owner = WorkNodeId::new("seal");
    specification.nodes.push(cpu_node(
        "later",
        BTreeSet::from([WorkDependency::Fence(FenceId::new(
            owner.clone(),
            FenceKind::Io,
        ))]),
    ));
    let dag = ExecutionDag::new(specification).expect("export followed by later work");
    let authority = io_authority_with_workers_and_memory(2, 1024);
    let mut scheduler =
        ExecutionScheduler::start(&dag, &ResourcePolicy::Exclusive, &authority, None)
            .expect("admitted plan");
    assert!(matches!(
        scheduler.next_action().unwrap(),
        SchedulerAction::Work(_)
    ));
    scheduler
        .finish_work(owner.clone(), WorkResult::Succeeded)
        .unwrap();
    scheduler
        .complete_fence(FenceId::new(owner.clone(), FenceKind::Io))
        .unwrap();
    let artifact = scheduler.take_artifact_permit(&owner).unwrap().unwrap();
    let SchedulerAction::Work(later) = scheduler.next_action().unwrap() else {
        panic!("later work");
    };
    scheduler
        .finish_work(
            later.node().id.clone(),
            WorkResult::Failed {
                message: "later failure".to_string(),
            },
        )
        .unwrap();
    assert!(matches!(
        scheduler.next_action().unwrap(),
        SchedulerAction::Complete(SchedulerTerminal::Failed { .. })
    ));
    assert!(can_admit(&authority, 824));
    assert!(!can_admit(&authority, 825));
    drop(artifact);
    assert!(can_admit(&authority, 1024));
}
