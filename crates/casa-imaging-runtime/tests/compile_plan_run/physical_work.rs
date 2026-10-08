// SPDX-License-Identifier: LGPL-3.0-or-later

//! The transaction-bound physical work the plan/run tests bind: a source
//! read, an initial consistency check, a post-replay reconciliation, product
//! staging and the atomic publication commit.

use super::*;

pub(crate) fn physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("default physical-work problem");
    physical_work_with_transaction_staging(
        &problem,
        implementation_byte,
        product_participants(&problem),
        false,
        true,
    )
}

pub(crate) fn physical_work_with_synchronous_observation_read(
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("synchronous observation-read problem");
    physical_work_with_transaction_staging(
        &problem,
        implementation_byte,
        product_participants(&problem),
        false,
        false,
    )
}

pub(crate) fn product_participants(
    problem: &casa_imaging_model::CompiledProblem,
) -> Vec<PublicationParticipant> {
    let graph_id = problem.product_graph().graph_id();
    problem
        .product_graph()
        .publication()
        .members()
        .iter()
        .copied()
        .map(|node_id| PublicationParticipant::Product { graph_id, node_id })
        .collect()
}

pub(crate) fn default_product_participants() -> Vec<PublicationParticipant> {
    product_participants(&compile(request(1)).expect("default physical-work problem"))
}

pub(crate) fn physical_work_for_problem(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let graph_id = problem.product_graph().graph_id();
    let participants = problem
        .product_graph()
        .publication()
        .members()
        .iter()
        .copied()
        .map(|node_id| PublicationParticipant::Product { graph_id, node_id })
        .collect();
    let base = physical_work_with_transaction_staging(
        problem,
        implementation_byte,
        participants,
        false,
        true,
    );
    let measurement_sets = problem
        .observation_transaction()
        .read_set()
        .sources()
        .iter()
        .map(|source| source.measurement_set())
        .collect::<Vec<_>>();
    assert!(
        !measurement_sets.is_empty(),
        "compiled read set is non-empty"
    );
    let mut nodes = base
        .execution_dag()
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for node in &mut nodes {
        let mut claims = Vec::with_capacity(
            node.claims
                .len()
                .saturating_add(measurement_sets.len().saturating_sub(1)),
        );
        for claim in std::mem::take(&mut node.claims) {
            if matches!(claim.resource, LeaseResource::MeasurementSetLock { .. }) {
                claims.extend(measurement_sets.iter().copied().map(|measurement_set| {
                    ResourceClaim {
                        resource: LeaseResource::MeasurementSetLock { measurement_set },
                        amount: claim.amount,
                        lifetime: claim.lifetime.clone(),
                    }
                }));
            } else {
                claims.push(claim);
            }
        }
        node.claims = claims;
    }
    let mut alternative = base.execution_dag().resource_alternative().clone();
    let lock_count = u64::try_from(measurement_sets.len()).expect("test lock count fits u64");
    alternative.demand.locks = CountDemand::new(lock_count, lock_count);
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base
            .execution_dag()
            .required_resource_capabilities()
            .clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations: base
            .execution_dag()
            .logical_allocations()
            .values()
            .cloned()
            .collect(),
        physical_slots: base
            .execution_dag()
            .physical_slots()
            .values()
            .cloned()
            .collect(),
        initial_knobs: base.execution_dag().initial_knobs().clone(),
        adaptations: base
            .execution_dag()
            .adaptations()
            .values()
            .cloned()
            .collect(),
    })
    .expect("problem-bound transaction DAG");
    native_product_physical_work(
        problem,
        implementation_catalog(problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("problem-bound physical work")
}

/// Bind a problem whose model input cannot be sealed by the empty-model
/// publication fixture (for example an aligned seed) to a reconstruction-only
/// transaction over the same physical transaction DAG.
pub(crate) fn reconstruction_physical_work_for_problem(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let base = physical_work(implementation_byte);
    let artifacts = base
        .artifacts()
        .iter()
        .filter(|artifact| artifact.role() != ArtifactRole::Output)
        .cloned()
        .collect();
    let transaction = ObservationTransactionWork::new_reconstruction(
        base.observation_transaction()
            .initial_consistency_check()
            .expect("observation consistency check")
            .clone(),
        base.observation_transaction()
            .post_replay_reconciliation()
            .expect("product transaction has reconciliation")
            .clone(),
        base.observation_transaction().commit().clone(),
    );
    PhysicalWorkBinding::new_reconstruction(
        implementation_catalog(problem, base.execution_dag()),
        base.execution_dag().clone(),
        base.prediction().clone(),
        artifacts,
        transaction,
        PublicationLayoutLedger::empty(),
    )
    .expect("problem-bound reconstruction physical work")
}

pub(crate) fn physical_work_with_product_staging(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
    participants: Vec<PublicationParticipant>,
) -> Result<PhysicalWorkBinding, PhysicalWorkBindingError> {
    let publication = publication_plan_for_problem(problem);
    physical_work_with_optional_seal(
        problem,
        implementation_byte,
        participants,
        false,
        true,
        &publication,
    )
}

pub(crate) fn physical_work_with_early_publication_buffer(
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("early-publication physical-work problem");
    let graph_id = problem.product_graph().graph_id();
    physical_work_with_transaction_staging(
        &problem,
        implementation_byte,
        problem
            .product_graph()
            .publication()
            .members()
            .iter()
            .copied()
            .map(|node_id| PublicationParticipant::Product { graph_id, node_id })
            .collect(),
        true,
        true,
    )
}

pub(crate) fn physical_work_with_transaction_staging(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
    participants: Vec<PublicationParticipant>,
    acquire_publication_early: bool,
    fenced_observation_read: bool,
) -> PhysicalWorkBinding {
    let publication = publication_plan_for_problem(problem);
    physical_work_with_optional_seal(
        problem,
        implementation_byte,
        participants,
        acquire_publication_early,
        fenced_observation_read,
        &publication,
    )
    .expect("native product publication binding")
}

pub(crate) fn physical_work_with_optional_seal(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
    participants: Vec<PublicationParticipant>,
    acquire_publication_early: bool,
    fenced_observation_read: bool,
    sealed: &ProductPublicationPlan,
) -> Result<PhysicalWorkBinding, PhysicalWorkBindingError> {
    let work_implementation = implementation(implementation_byte);
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("test-cpu"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: Vec::new(),
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: Vec::new(),
                rates: vec![RateDemand {
                    demand_id: "io-rate".to_string(),
                    resource: RateResourceId::new("io-rate"),
                    amount: CountDemand::new(1, 1),
                }],
                caches: CacheDemand::zero(),
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: vec![QueueDemand {
                    demand_id: "io-queue".to_string(),
                    resource: QueueResourceId::new("io-queue"),
                    slots: CountDemand::new(1, 1),
                }],
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand::zero(),
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
        nodes: vec![
            WorkNode {
                id: WorkNodeId::new("read"),
                kind: WorkKind::Io,
                domain: WorkDomain::Io,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::new(),
                claims: vec![
                    ResourceClaim {
                        resource: LeaseResource::Rate {
                            demand_id: "io-rate".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                    ResourceClaim {
                        resource: LeaseResource::Queue {
                            demand_id: "io-queue".to_string(),
                        },
                        amount: 1,
                        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                    },
                ],
                allocations: Vec::new(),
                fences: BTreeSet::from([FenceKind::Io]),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: WorkNodeId::new("execute"),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: work_implementation,
                dependencies: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    WorkNodeId::new("read"),
                    FenceKind::Io,
                ))]),
                claims: vec![ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                }],
                allocations: Vec::new(),
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
        ],
        logical_allocations: Vec::new(),
        physical_slots: Vec::new(),
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: Vec::new(),
    };
    transaction_binding_with_seal(
        problem,
        specification,
        implementation(implementation_byte),
        participants,
        acquire_publication_early,
        fenced_observation_read,
        sealed,
    )
}

pub(crate) fn transaction_binding(
    problem: &casa_imaging_model::CompiledProblem,
    specification: ExecutionDagSpecification,
    work_implementation: WorkImplementationId,
    participants: Vec<PublicationParticipant>,
    acquire_publication_early: bool,
    fenced_observation_read: bool,
) -> PhysicalWorkBinding {
    let publication = publication_plan_for_problem(problem);
    transaction_binding_with_seal(
        problem,
        specification,
        work_implementation,
        participants,
        acquire_publication_early,
        fenced_observation_read,
        &publication,
    )
    .expect("native product publication binding")
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn transaction_binding_with_seal(
    problem: &casa_imaging_model::CompiledProblem,
    mut specification: ExecutionDagSpecification,
    work_implementation: WorkImplementationId,
    participants: Vec<PublicationParticipant>,
    acquire_publication_early: bool,
    fenced_observation_read: bool,
    sealed: &ProductPublicationPlan,
) -> Result<PhysicalWorkBinding, PhysicalWorkBindingError> {
    let product_count = participants
        .iter()
        .filter(|participant| matches!(participant, PublicationParticipant::Product { .. }))
        .count() as u64;
    let member_count = participants.len() as u64;
    let initial = WorkNodeId::new("transaction-check");
    let read = WorkNodeId::new("transaction-read");
    let reconciliation = WorkNodeId::new("post-replay-reconciliation");
    let product = WorkNodeId::new("transaction-stage-psf");
    let commit = WorkNodeId::new("transaction-commit");
    let publication_allocation = AllocationId::new("transaction-publication-buffer");
    let publication_slot = PhysicalSlotId::new("transaction-publication-slot");
    let commit_allocation = AllocationId::new("transaction-commit-buffer");
    let commit_slot = PhysicalSlotId::new("transaction-commit-slot");
    let observation_read_lifetime = if fenced_observation_read {
        ClaimLifetime::through_fence(FenceKind::Io)
    } else {
        ClaimLifetime::Work
    };
    let observation_read_fences = if fenced_observation_read {
        BTreeSet::from([FenceKind::Io])
    } else {
        BTreeSet::new()
    };
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let publication_compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 1,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("transaction-publication-buffer"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
    };
    let product_writer_allocation = AllocationId::new("transaction-product-writer-buffer");
    let product_writer_slot = PhysicalSlotId::new("transaction-product-writer-slot");
    let product_writer_compatibility = SlotCompatibility {
        layout: AllocationLayout::new("transaction-product-writer-buffer"),
        ..publication_compatibility.clone()
    };
    let commit_compatibility = SlotCompatibility {
        layout: AllocationLayout::new("transaction-commit-buffer"),
        ..publication_compatibility.clone()
    };

    let predecessors = specification
        .nodes
        .iter()
        .flat_map(|node| {
            node.dependencies.iter().map(|dependency| match dependency {
                WorkDependency::Work(node) => node,
                WorkDependency::Fence(fence) => fence.node(),
            })
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let terminals = specification
        .nodes
        .iter()
        .filter(|node| !predecessors.contains(&node.id))
        .flat_map(|node| {
            if node.fences.is_empty() {
                vec![WorkDependency::Work(node.id.clone())]
            } else {
                node.fences
                    .iter()
                    .map(|fence| WorkDependency::Fence(FenceId::new(node.id.clone(), *fence)))
                    .collect()
            }
        })
        .collect::<BTreeSet<_>>();
    let read_completion = if fenced_observation_read {
        WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io))
    } else {
        WorkDependency::Work(read.clone())
    };
    for node in specification
        .nodes
        .iter_mut()
        .filter(|node| node.dependencies.is_empty())
    {
        node.dependencies.insert(read_completion.clone());
    }

    specification
        .resource_alternative
        .demand
        .memory
        .push(MemoryDemand {
            allocation_id: "transaction-publication-slot".to_string(),
            hard_bytes: 1,
            preferred_bytes: 1,
            views: vec![CapacityViewId::new("host-memory")],
        });
    specification
        .resource_alternative
        .demand
        .memory
        .push(MemoryDemand {
            allocation_id: "transaction-product-writer-slot".to_string(),
            hard_bytes: product_count,
            preferred_bytes: product_count,
            views: vec![CapacityViewId::new("host-memory")],
        });
    if acquire_publication_early {
        specification
            .resource_alternative
            .demand
            .memory
            .push(MemoryDemand {
                allocation_id: "transaction-commit-slot".to_string(),
                hard_bytes: 1,
                preferred_bytes: 1,
                views: vec![CapacityViewId::new("host-memory")],
            });
    }
    specification
        .resource_alternative
        .demand
        .storage
        .push(casa_imaging_runtime::StorageDemand {
            demand_id: "transaction-output".to_string(),
            domain: casa_imaging_runtime::StorageDomainId::new("atomic-output"),
            temporary_bytes: 0,
            staged_output_bytes: member_count,
            final_output_bytes: member_count,
            persistent_cache_bytes: 0,
            read_rate: CountDemand::zero(),
            write_rate: CountDemand::zero(),
            operations_rate: CountDemand::zero(),
            queue_slots: CountDemand::zero(),
        });
    specification
        .resource_alternative
        .demand
        .rates
        .push(RateDemand {
            demand_id: "transaction-io-rate".to_string(),
            resource: RateResourceId::new("transaction-io-rate"),
            amount: CountDemand::new(1, 1),
        });
    specification
        .resource_alternative
        .demand
        .queues
        .push(QueueDemand {
            demand_id: "transaction-io-queue".to_string(),
            resource: QueueResourceId::new("transaction-io-queue"),
            slots: CountDemand::new(1, 1),
        });
    specification.resource_alternative.demand.locks = CountDemand::new(1, 1);
    specification
        .resource_alternative
        .demand
        .io_buffers
        .serialization_bytes = product_count;
    specification
        .resource_alternative
        .demand
        .io_buffers
        .publication_bytes = 1;

    let commit_dependencies = BTreeSet::from([WorkDependency::Work(product.clone())]);
    let reconciliation_dependencies = terminals;
    let product_claims = vec![
        ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::Storage {
                demand_id: "transaction-output".to_string(),
                use_kind: casa_imaging_runtime::StorageUseKind::StagedOutput,
            },
            amount: product_count,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: LeaseResource::IoBuffer(IoBufferKind::Serialization),
            amount: product_count,
            lifetime: ClaimLifetime::Work,
        },
    ];
    let mut product_allocations = vec![AllocationUse {
        allocation: product_writer_allocation.clone(),
        lifetime: ClaimLifetime::Work,
    }];
    if acquire_publication_early {
        product_allocations.push(AllocationUse {
            allocation: publication_allocation.clone(),
            lifetime: ClaimLifetime::Work,
        });
    }

    specification.nodes.extend([
        WorkNode {
            id: initial.clone(),
            kind: WorkKind::DataCensus,
            domain: WorkDomain::Cpu,
            implementation: work_implementation.clone(),
            dependencies: BTreeSet::new(),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: LeaseResource::MeasurementSetLock {
                        measurement_set: casa_imaging_model::MeasurementSetIdentity::new(identity(
                            1,
                        )),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: Vec::new(),
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: read.clone(),
            kind: WorkKind::ObservationRead,
            domain: WorkDomain::Io,
            implementation: work_implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(initial.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: observation_read_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: observation_read_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::MeasurementSetLock {
                        measurement_set: casa_imaging_model::MeasurementSetIdentity::new(identity(
                            1,
                        )),
                    },
                    amount: 1,
                    lifetime: observation_read_lifetime,
                },
            ],
            allocations: Vec::new(),
            fences: observation_read_fences,
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: reconciliation.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: work_implementation.clone(),
            dependencies: reconciliation_dependencies,
            claims: vec![ResourceClaim {
                resource: LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }],
            allocations: Vec::new(),
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: product.clone(),
            kind: WorkKind::Serialization,
            domain: WorkDomain::Cpu,
            implementation: work_implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
            claims: product_claims,
            allocations: product_allocations,
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: commit.clone(),
            kind: WorkKind::Publication,
            domain: WorkDomain::Io,
            implementation: work_implementation.clone(),
            dependencies: commit_dependencies,
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::MeasurementSetLock {
                        measurement_set: casa_imaging_model::MeasurementSetIdentity::new(identity(
                            1,
                        )),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: casa_imaging_runtime::StorageUseKind::StagedOutput,
                    },
                    amount: member_count,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: casa_imaging_runtime::StorageUseKind::FinalOutput,
                    },
                    amount: member_count,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
            ],
            allocations: if acquire_publication_early {
                vec![
                    AllocationUse {
                        allocation: publication_allocation.clone(),
                        lifetime: publication_lifetime.clone(),
                    },
                    AllocationUse {
                        allocation: commit_allocation.clone(),
                        lifetime: publication_lifetime.clone(),
                    },
                ]
            } else {
                vec![AllocationUse {
                    allocation: publication_allocation.clone(),
                    lifetime: publication_lifetime.clone(),
                }]
            },
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
            quiescence_after: BTreeSet::new(),
        },
    ]);
    specification.logical_allocations.push(LogicalAllocation {
        id: product_writer_allocation.clone(),
        bytes: product_count,
        purpose: AllocationPurpose::IoBuffer(IoBufferKind::Serialization),
        compatibility: product_writer_compatibility.clone(),
        physical_slot: product_writer_slot.clone(),
        lifetime: AllocationLifetime {
            disposition: casa_imaging_runtime::AllocationDisposition::Release,
            acquire_at: product.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(product.clone())]),
        },
    });
    specification.physical_slots.push(PhysicalSlot {
        id: product_writer_slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "transaction-product-writer-slot".to_string(),
        },
        capacity_bytes: product_count,
        compatibility: product_writer_compatibility,
    });
    specification.logical_allocations.push(LogicalAllocation {
        id: publication_allocation,
        bytes: 1,
        purpose: if acquire_publication_early {
            AllocationPurpose::Data
        } else {
            AllocationPurpose::IoBuffer(IoBufferKind::Publication)
        },
        compatibility: publication_compatibility.clone(),
        physical_slot: publication_slot.clone(),
        lifetime: AllocationLifetime {
            disposition: casa_imaging_runtime::AllocationDisposition::Release,
            acquire_at: if acquire_publication_early {
                product.clone()
            } else {
                commit.clone()
            },
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
            ]),
        },
    });
    specification.physical_slots.push(PhysicalSlot {
        id: publication_slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "transaction-publication-slot".to_string(),
        },
        capacity_bytes: 1,
        compatibility: publication_compatibility,
    });
    if acquire_publication_early {
        specification.logical_allocations.push(LogicalAllocation {
            id: commit_allocation,
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
            compatibility: commit_compatibility.clone(),
            physical_slot: commit_slot.clone(),
            lifetime: AllocationLifetime {
                disposition: casa_imaging_runtime::AllocationDisposition::Release,
                acquire_at: commit.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
                ]),
            },
        });
        specification.physical_slots.push(PhysicalSlot {
            id: commit_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "transaction-commit-slot".to_string(),
            },
            capacity_bytes: 1,
            compatibility: commit_compatibility,
        });
    }
    let dag = ExecutionDag::new(specification).expect("valid transaction-bound physical work");
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let io = node
                .claims
                .iter()
                .filter_map(|claim| match claim.resource {
                    LeaseResource::IoBuffer(kind) => Some(IoPrediction::new(kind, claim.amount, 1)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let stage = StagePrediction::new(node.id.clone(), 100);
            if io.is_empty() {
                stage
            } else {
                stage.with_io(io)
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete transaction prediction");
    let sealed_artifact = |participant: &PublicationParticipant| -> Option<ArtifactIdentity> {
        let PublicationParticipant::Product { node_id, .. } = participant;
        sealed.artifact(*node_id)
    };
    let catalog = implementation_catalog(problem, &dag);
    let artifacts = participants
        .iter()
        .enumerate()
        .map(|(index, participant)| {
            let identity = sealed_artifact(participant)
                .unwrap_or_else(|| ArtifactIdentity::from_sha256([34 + index as u8; 32]));
            PlannedArtifact::new(identity, commit.clone(), ArtifactRole::Output, None)
        })
        .collect();
    let layouts = PublicationLayoutLedger::new(
        participants
            .into_iter()
            .enumerate()
            .map(|(index, participant)| {
                let PublicationParticipant::Product { .. } = participant;
                let (producer, terminal, kind, allocation) = (
                    product.clone(),
                    WorkDependency::Work(product.clone()),
                    IoBufferKind::Serialization,
                    product_writer_allocation.clone(),
                );
                let identity = sealed_artifact(&participant)
                    .unwrap_or_else(|| ArtifactIdentity::from_sha256([34 + index as u8; 32]));
                PublicationPhysicalLayout::new(
                    participant,
                    identity,
                    PhysicalLayoutId::from_sha256([150 + index as u8; 32]),
                    PublicationStaging::new(producer, terminal, kind, allocation)
                        .expect("valid publication staging"),
                    PublicationResourceBounds::new(1, 1, 1, 0).expect("valid publication bounds"),
                )
            })
            .collect(),
    )
    .expect("complete publication layout ledger");
    let transaction_work =
        ObservationTransactionWork::new_product_publication(initial, reconciliation, commit);
    PhysicalWorkBinding::new_with_product_publication(
        catalog,
        dag,
        prediction,
        artifacts,
        transaction_work,
        layouts,
        sealed,
    )
}
