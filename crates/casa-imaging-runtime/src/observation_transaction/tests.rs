// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_imaging_model::{LogicalIdentity, MeasurementSetIdentity};

use crate::{
    AllocationAccess, AllocationId, AllocationLayout, AllocationLifetime, AllocationPurpose,
    AllocationUse, AlternativeId, CacheDemand, CapabilityPredicate, CapacityDomainId,
    CapacityViewId, ClaimLifetime, CountDemand, DemandAlternative, DemandEnvelope,
    ExecutionDagSpecification, ExecutionKnobs, FenceId, FenceKind, InitializationPolicy,
    IoBufferDemand, IoBufferKind, LeaseResource, LogicalAllocation, MemoryDemand, PhysicalSlot,
    PhysicalSlotId, QueueDemand, QueueResourceId, QuiescencePoint, RateDemand, RateResourceId,
    ResourceClaim, ResourceHeadroom, RuntimeOverheadDemand, ScalingMetadata, SlotCompatibility,
    StorageDemand, StorageDomainId, StorageMode, StorageUseKind, WorkDependency, WorkDomain,
    WorkImplementationId, WorkKind, WorkNode, WorkNodeId,
};

use super::*;

fn claim(resource: LeaseResource) -> ResourceClaim {
    ResourceClaim {
        resource,
        amount: 1,
        lifetime: ClaimLifetime::Work,
    }
}

fn measurement_set(byte: u8) -> MeasurementSetIdentity {
    MeasurementSetIdentity::new(LogicalIdentity::from_sha256([byte; 32]))
}

fn measurement_set_lock(byte: u8) -> LeaseResource {
    LeaseResource::MeasurementSetLock {
        measurement_set: measurement_set(byte),
    }
}

fn node(
    id: &str,
    kind: WorkKind,
    dependencies: BTreeSet<WorkDependency>,
    claims: Vec<ResourceClaim>,
    fences: BTreeSet<FenceKind>,
) -> WorkNode {
    let domain = match kind {
        WorkKind::Io | WorkKind::ObservationRead | WorkKind::Publication => WorkDomain::Io,
        _ => WorkDomain::Cpu,
    };
    let lifetime = match &domain {
        WorkDomain::Io => ClaimLifetime::through_fences(fences.iter().copied()),
        _ => ClaimLifetime::Work,
    };
    let mut claims = claims;
    for claim in &mut claims {
        claim.lifetime = lifetime.clone();
    }
    match &domain {
        WorkDomain::Cpu => claims.push(ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }),
        WorkDomain::Io => claims.extend([
            ResourceClaim {
                resource: LeaseResource::Rate {
                    demand_id: "transaction-io-rate".to_string(),
                },
                amount: 1,
                lifetime: lifetime.clone(),
            },
            ResourceClaim {
                resource: LeaseResource::Queue {
                    demand_id: "transaction-io-queue".to_string(),
                },
                amount: 1,
                lifetime,
            },
        ]),
        WorkDomain::Control | WorkDomain::Metal { .. } => unreachable!("fixture domain"),
    }
    WorkNode {
        id: WorkNodeId::new(id),
        kind,
        domain,
        implementation: WorkImplementationId::new("transaction-test"),
        dependencies,
        claims,
        allocations: match kind {
            WorkKind::Publication => vec![AllocationUse {
                allocation: AllocationId::new("publication-buffer"),
                lifetime: ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]),
            }],
            _ => Vec::new(),
        },
        fences,
        quiescence_after: BTreeSet::new(),
    }
}

fn transaction_nodes() -> (BTreeMap<WorkNodeId, WorkNode>, ObservationTransactionWork) {
    let initial = WorkNodeId::new("check-initial");
    let read = WorkNodeId::new("read-observation");
    let reconciliation = WorkNodeId::new("post-replay-reconciliation");
    let product = WorkNodeId::new("stage-products");
    let commit = WorkNodeId::new("commit-side-effects");
    let staged_storage = || LeaseResource::Storage {
        demand_id: "atomic-output".to_string(),
        use_kind: StorageUseKind::StagedOutput,
    };
    let product_completion = WorkDependency::Work(product.clone());
    let read_completion = WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io));
    let nodes = [
        node(
            initial.as_str(),
            WorkKind::DataCensus,
            BTreeSet::new(),
            vec![claim(measurement_set_lock(1))],
            BTreeSet::new(),
        ),
        node(
            read.as_str(),
            WorkKind::ObservationRead,
            BTreeSet::from([WorkDependency::Work(initial.clone())]),
            vec![claim(measurement_set_lock(1))],
            BTreeSet::from([FenceKind::Io]),
        ),
        node(
            reconciliation.as_str(),
            WorkKind::Compute,
            BTreeSet::from([read_completion]),
            Vec::new(),
            BTreeSet::new(),
        ),
        node(
            product.as_str(),
            WorkKind::Serialization,
            BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
            vec![claim(staged_storage())],
            BTreeSet::new(),
        ),
        node(
            commit.as_str(),
            WorkKind::Publication,
            BTreeSet::from([
                product_completion.clone(),
                WorkDependency::Work(reconciliation.clone()),
            ]),
            vec![
                claim(measurement_set_lock(1)),
                claim(staged_storage()),
                claim(LeaseResource::IoBuffer(IoBufferKind::Publication)),
            ],
            BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
        ),
    ]
    .into_iter()
    .collect::<Vec<_>>();
    let publication_compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 1,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("publication-buffer"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
    };
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("transaction-test"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: vec![MemoryDemand {
                    allocation_id: "publication-slot".to_string(),
                    hard_bytes: 1,
                    preferred_bytes: 1,
                    views: vec![CapacityViewId::new("host-memory")],
                }],
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: vec![StorageDemand {
                    demand_id: "atomic-output".to_string(),
                    domain: StorageDomainId::new("atomic-output"),
                    temporary_bytes: 0,
                    staged_output_bytes: 2,
                    final_output_bytes: 0,
                    persistent_cache_bytes: 0,
                    read_rate: CountDemand::zero(),
                    write_rate: CountDemand::zero(),
                    operations_rate: CountDemand::zero(),
                    queue_slots: CountDemand::zero(),
                }],
                rates: vec![RateDemand {
                    demand_id: "transaction-io-rate".to_string(),
                    resource: RateResourceId::new("transaction-io-rate"),
                    amount: CountDemand::new(1, 1),
                }],
                caches: CacheDemand::zero(),
                locks: CountDemand::new(1, 1),
                file_descriptors: CountDemand::zero(),
                queues: vec![QueueDemand {
                    demand_id: "transaction-io-queue".to_string(),
                    resource: QueueResourceId::new("transaction-io-queue"),
                    slots: CountDemand::new(1, 1),
                }],
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand {
                    publication_bytes: 1,
                    ..IoBufferDemand::zero()
                },
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: 1,
                maximum_batch_size: 1,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
        },
        nodes,
        logical_allocations: vec![LogicalAllocation {
            id: AllocationId::new("publication-buffer"),
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
            compatibility: publication_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("publication-slot"),
            lifetime: AllocationLifetime {
                disposition: crate::AllocationDisposition::Release,
                acquire_at: commit.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
                ]),
            },
        }],
        physical_slots: vec![PhysicalSlot {
            id: PhysicalSlotId::new("publication-slot"),
            lease_resource: LeaseResource::Memory {
                allocation_id: "publication-slot".to_string(),
            },
            capacity_bytes: 1,
            compatibility: publication_compatibility,
        }],
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: Vec::new(),
    })
    .expect("canonical transaction test DAG");
    let mut work =
        ObservationTransactionWork::new_product_publication(initial, reconciliation, commit);
    work.product_staging = BTreeSet::from([product_completion]);
    (dag.nodes().clone(), work)
}

#[test]
fn observation_reads_form_the_mutation_and_failure_cut() {
    let (nodes, work) = transaction_nodes();
    let observation_reads =
        validate_transaction_nodes(1, &nodes, &work).expect("complete transaction cut");
    assert_eq!(
        observation_reads,
        BTreeSet::from([WorkDependency::Fence(FenceId::new(
            WorkNodeId::new("read-observation"),
            FenceKind::Io,
        ))])
    );

    let mut read_before_check = nodes.clone();
    read_before_check
        .get_mut(&WorkNodeId::new("read-observation"))
        .expect("observation read")
        .dependencies
        .clear();
    assert!(validate_transaction_nodes(1, &read_before_check, &work).is_err());

    let mut unlocked_read = nodes.clone();
    unlocked_read
        .get_mut(&WorkNodeId::new("read-observation"))
        .expect("observation read")
        .claims
        .clear();
    assert!(
        validate_transaction_nodes(1, &unlocked_read, &work).is_err(),
        "every observation read must hold all source locks"
    );

    let mut reconcile_before_read = nodes;
    reconcile_before_read
        .get_mut(&WorkNodeId::new("post-replay-reconciliation"))
        .expect("post-replay reconciliation")
        .dependencies
        .clear();
    assert!(validate_transaction_nodes(1, &reconcile_before_read, &work).is_err());
}

#[test]
fn transaction_boundary_rejects_untyped_source_io() {
    let (mut nodes, work) = transaction_nodes();
    let hidden = WorkNodeId::new("hidden-observation-read");
    let hidden_completion = WorkDependency::Fence(FenceId::new(hidden.clone(), FenceKind::Io));
    nodes.insert(
        hidden.clone(),
        node(
            hidden.as_str(),
            WorkKind::Io,
            BTreeSet::from([WorkDependency::Work(WorkNodeId::new("check-initial"))]),
            vec![claim(measurement_set_lock(1))],
            BTreeSet::from([FenceKind::Io]),
        ),
    );
    nodes
        .get_mut(&WorkNodeId::new("post-replay-reconciliation"))
        .expect("post-replay reconciliation")
        .dependencies
        .insert(hidden_completion);

    assert!(
        validate_transaction_nodes(1, &nodes, &work).is_err(),
        "a lock-bearing generic I/O node cannot hide an observation read outside the transaction cut"
    );
}

#[test]
fn transaction_boundary_requires_terminal_atomic_commit() {
    let (mut nodes, work) = transaction_nodes();
    nodes.insert(
        WorkNodeId::new("post-commit-fallible-io"),
        node(
            "post-commit-fallible-io",
            WorkKind::Io,
            BTreeSet::from([WorkDependency::Work(WorkNodeId::new("commit-side-effects"))]),
            Vec::new(),
            BTreeSet::from([FenceKind::Io]),
        ),
    );

    assert!(
        validate_transaction_nodes(1, &nodes, &work).is_err(),
        "the atomic commit cannot precede another fallible completion"
    );
}

#[test]
fn asynchronous_observation_read_cannot_use_its_launch_as_completion() {
    let (mut nodes, work) = transaction_nodes();
    let read = WorkNodeId::new("read-observation");
    let reconciliation = nodes
        .get_mut(&WorkNodeId::new("post-replay-reconciliation"))
        .expect("post-replay reconciliation");
    reconciliation
        .dependencies
        .remove(&WorkDependency::Fence(FenceId::new(
            read.clone(),
            FenceKind::Io,
        )));
    reconciliation
        .dependencies
        .insert(WorkDependency::Work(read));

    assert!(
        validate_transaction_nodes(1, &nodes, &work).is_err(),
        "a fenced observation read must name every terminal fence, not its launch"
    );
}

#[test]
fn asynchronous_initial_check_and_reconciliation_gate_their_terminal_fences() {
    let (mut nodes, work) = transaction_nodes();
    let initial = WorkNodeId::new("check-initial");
    nodes
        .get_mut(&initial)
        .expect("initial check")
        .fences
        .insert(FenceKind::Io);
    assert!(
        validate_transaction_nodes(1, &nodes, &work).is_err(),
        "observation reads cannot start from an asynchronous initial-check launch"
    );

    let (mut nodes, work) = transaction_nodes();
    let reconciliation = WorkNodeId::new("post-replay-reconciliation");
    nodes
        .get_mut(&reconciliation)
        .expect("post-replay reconciliation")
        .fences
        .insert(FenceKind::Device);
    assert!(
        validate_transaction_nodes(1, &nodes, &work).is_err(),
        "product staging cannot start from an asynchronous reconciliation launch"
    );
}

#[test]
fn mutation_cancellation_and_precommit_failures_cannot_reach_visibility() {
    let (mut nodes, work) = transaction_nodes();
    let commit = WorkNodeId::new("commit-side-effects");
    for (failure, event) in [
        (
            "input mutation",
            WorkDependency::Work(WorkNodeId::new("check-initial")),
        ),
        (
            "observation read",
            WorkDependency::Fence(FenceId::new(
                WorkNodeId::new("read-observation"),
                FenceKind::Io,
            )),
        ),
        (
            "numerical reconciliation",
            WorkDependency::Work(WorkNodeId::new("post-replay-reconciliation")),
        ),
        (
            "product output",
            WorkDependency::Work(WorkNodeId::new("stage-products")),
        ),
    ] {
        assert!(
            event_precedes(&nodes, &event, &commit, &mut BTreeSet::new()),
            "{failure} and cancellation at that cut must block publication"
        );
    }

    nodes.insert(
        WorkNodeId::new("rogue-publication"),
        node(
            "rogue-publication",
            WorkKind::Publication,
            BTreeSet::from([WorkDependency::Work(WorkNodeId::new("check-initial"))]),
            Vec::new(),
            BTreeSet::new(),
        ),
    );
    assert!(
        validate_transaction_nodes(1, &nodes, &work).is_err(),
        "no failure path may expose a partial generation through another publication node"
    );
}

#[test]
fn atomic_transaction_requires_declared_resources() {
    let (nodes, work) = transaction_nodes();
    validate_transaction_nodes(1, &nodes, &work).expect("complete transaction resources");

    for (node_id, resource) in [
        ("check-initial", measurement_set_lock(1)),
        (
            "commit-side-effects",
            LeaseResource::IoBuffer(IoBufferKind::Publication),
        ),
    ] {
        let mut incomplete = nodes.clone();
        incomplete
            .get_mut(&WorkNodeId::new(node_id))
            .expect("fixture node")
            .claims
            .retain(|claim| claim.resource != resource);
        assert!(
            validate_transaction_nodes(1, &incomplete, &work).is_err(),
            "removing {resource:?} from {node_id} must fail"
        );
    }

    let mut no_commit_fence = nodes;
    no_commit_fence
        .get_mut(&WorkNodeId::new("commit-side-effects"))
        .expect("commit node")
        .fences
        .remove(&FenceKind::Publication);
    assert!(validate_transaction_nodes(1, &no_commit_fence, &work).is_err());
}

#[test]
fn atomic_commit_waits_for_complete_staging_in_one_domain() {
    let (nodes, work) = transaction_nodes();
    validate_transaction_nodes(1, &nodes, &work).expect("complete transaction ordering");

    let mut unstaged_product = nodes.clone();
    unstaged_product
        .get_mut(&WorkNodeId::new("stage-products"))
        .expect("product node")
        .claims
        .clear();
    assert!(validate_transaction_nodes(1, &unstaged_product, &work).is_err());

    let mut split_staging_domains = nodes.clone();
    let commit = split_staging_domains
        .get_mut(&WorkNodeId::new("commit-side-effects"))
        .expect("commit node");
    let storage = commit
        .claims
        .iter_mut()
        .find(|claim| {
            matches!(
                &claim.resource,
                LeaseResource::Storage {
                    use_kind: StorageUseKind::StagedOutput,
                    ..
                }
            )
        })
        .expect("staged storage claim");
    storage.resource = LeaseResource::Storage {
        demand_id: "different-output-domain".to_string(),
        use_kind: StorageUseKind::StagedOutput,
    };
    assert!(validate_transaction_nodes(1, &split_staging_domains, &work).is_err());

    let mut early_commit = nodes;
    early_commit
        .get_mut(&WorkNodeId::new("commit-side-effects"))
        .expect("commit node")
        .dependencies
        .clear();
    assert!(validate_transaction_nodes(1, &early_commit, &work).is_err());
}

#[test]
fn multi_ms_transactions_reserve_every_concurrent_table_lock() {
    let (mut nodes, work) = transaction_nodes();
    assert!(validate_transaction_nodes(2, &nodes, &work).is_err());

    for node_id in ["check-initial", "read-observation", "commit-side-effects"] {
        nodes
            .get_mut(&WorkNodeId::new(node_id))
            .expect("lock-owning node")
            .claims
            .push(claim(measurement_set_lock(2)));
    }
    validate_transaction_nodes(2, &nodes, &work)
        .expect("one concurrent table lock per MeasurementSet");
}

#[test]
fn transaction_lock_claims_are_exact_and_unambiguous() {
    let (mut excess, work) = transaction_nodes();
    excess
        .get_mut(&WorkNodeId::new("check-initial"))
        .expect("initial check")
        .claims
        .iter_mut()
        .find(|claim| matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }))
        .expect("lock claim")
        .amount = 2;
    assert!(
        validate_transaction_nodes(1, &excess, &work).is_err(),
        "one MeasurementSet cannot be represented by an excess lock claim"
    );

    let (mut ambiguous, work) = transaction_nodes();
    ambiguous
        .get_mut(&WorkNodeId::new("check-initial"))
        .expect("initial check")
        .claims
        .push(claim(measurement_set_lock(1)));
    assert!(
        validate_transaction_nodes(1, &ambiguous, &work).is_err(),
        "multiple aggregate lock claims do not identify one exact per-MS lock set"
    );

    let (mut wrong_identity, work) = transaction_nodes();
    for node_id in ["check-initial", "read-observation", "commit-side-effects"] {
        let claim = wrong_identity
            .get_mut(&WorkNodeId::new(node_id))
            .expect("lock-owning node")
            .claims
            .iter_mut()
            .find(|claim| matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }))
            .expect("MeasurementSet lock claim");
        claim.resource = measurement_set_lock(9);
    }
    assert!(
        validate_measurement_set_lock_identities(
            &BTreeSet::from([measurement_set(1)]),
            &wrong_identity,
            &work,
        )
        .is_err(),
        "lock count alone cannot substitute another MeasurementSet identity"
    );
}

#[test]
fn commit_cannot_bypass_reconciliation() {
    let (nodes, work) = transaction_nodes();
    let mut bypass = nodes.clone();
    bypass
        .get_mut(&WorkNodeId::new("stage-products"))
        .expect("pre-commit node")
        .dependencies
        .clear();
    assert!(validate_transaction_nodes(1, &bypass, &work).is_err());

    let mut premature_product_visibility = nodes;
    premature_product_visibility
        .get_mut(&WorkNodeId::new("stage-products"))
        .expect("product staging node")
        .kind = WorkKind::Publication;
    assert!(validate_transaction_nodes(1, &premature_product_visibility, &work).is_err());
}

#[test]
fn product_staging_names_every_asynchronous_completion() {
    let (mut nodes, work) = transaction_nodes();
    nodes
        .get_mut(&WorkNodeId::new("stage-products"))
        .expect("product staging node")
        .fences
        .insert(FenceKind::Io);

    assert!(
        validate_transaction_nodes(1, &nodes, &work).is_err(),
        "a synchronous work event cannot stand in for a live product fence"
    );
}
