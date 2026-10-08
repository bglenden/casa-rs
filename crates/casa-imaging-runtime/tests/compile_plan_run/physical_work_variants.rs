// SPDX-License-Identifier: LGPL-3.0-or-later

//! Variants of the transaction-bound physical work: evidenced I/O and
//! artifacts, run-boundary and major-cycle adaptations, conditional routes,
//! audited allocations, release failures and mapped publication staging.

use super::*;

pub(crate) fn evidenced_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("evidenced physical-work problem");
    let base = physical_work(implementation_byte);
    let base_dag = base.execution_dag();
    let read = WorkNodeId::new("read");
    let publish = WorkNodeId::new("transaction-commit");
    let source_buffer = AllocationId::new("source-read-ahead-buffer");
    let source_slot = PhysicalSlotId::new("source-read-ahead-slot");
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("source-read-ahead-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut alternative = base_dag.resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "source-read-ahead-memory".to_string(),
        hard_bytes: 32,
        preferred_bytes: 32,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.io_buffers.source_read_ahead_bytes = 32;
    let mut nodes = base_dag.nodes().values().cloned().collect::<Vec<_>>();
    let read_node = nodes
        .iter_mut()
        .find(|node| node.id == read)
        .expect("source read node");
    read_node.kind = WorkKind::Prefetch;
    read_node.claims.push(ResourceClaim {
        resource: LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead),
        amount: 32,
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    });
    read_node.allocations.push(AllocationUse {
        allocation: source_buffer.clone(),
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    });
    let mut logical_allocations = base_dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    logical_allocations.push(LogicalAllocation {
        id: source_buffer,
        bytes: 32,
        purpose: AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead),
        compatibility: compatibility.clone(),
        physical_slot: source_slot.clone(),
        lifetime: AllocationLifetime {
            disposition: casa_imaging_runtime::AllocationDisposition::Release,
            acquire_at: read.clone(),
            release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                read.clone(),
                FenceKind::Io,
            ))]),
        },
    });
    let mut physical_slots = base_dag
        .physical_slots()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    physical_slots.push(PhysicalSlot {
        id: source_slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "source-read-ahead-memory".to_string(),
        },
        capacity_bytes: 32,
        compatibility,
    });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base_dag.required_resource_capabilities().clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations,
        physical_slots,
        initial_knobs: base_dag.initial_knobs().clone(),
        adaptations: base_dag.adaptations().values().cloned().collect(),
    })
    .expect("valid evidenced transaction DAG");
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let stage = StagePrediction::new(node.id.clone(), 100);
            if node.id == read {
                stage.with_io(vec![IoPrediction::new(
                    IoBufferKind::SourceReadAhead,
                    8_192,
                    4,
                )])
            } else if node.id == publish {
                stage.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 2_048, 1)])
            } else {
                let io = node
                    .claims
                    .iter()
                    .filter_map(|claim| match claim.resource {
                        LeaseResource::IoBuffer(kind) => {
                            Some(IoPrediction::new(kind, claim.amount, 1))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if io.is_empty() {
                    stage
                } else {
                    stage.with_io(io)
                }
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete evidence prediction");
    native_product_physical_work(
        &problem,
        implementation_catalog(&problem, &dag),
        dag,
        prediction,
        vec![
            PlannedArtifact::new(
                ArtifactIdentity::from_sha256([31; 32]),
                read.clone(),
                ArtifactRole::Input,
                None,
            ),
            PlannedArtifact::new(
                ArtifactIdentity::from_sha256([32; 32]),
                read,
                ArtifactRole::Cache,
                Some(CacheIdentity::from_sha256([33; 32])),
            ),
        ]
        .into_iter()
        .chain(base.artifacts().iter().cloned())
        .collect(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("bound evidenced transaction work")
}

pub(crate) fn adaptive_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("adaptive physical-work problem");
    adaptive_physical_work_for_problem(&problem, implementation_byte)
}

pub(crate) fn adaptive_physical_work_for_problem(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let work_implementation = implementation(implementation_byte);
    let first_id = WorkNodeId::new("first-major-work");
    let boundary_id = WorkNodeId::new("major-boundary");
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 2;
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("adaptive-cpu"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: Vec::new(),
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: Vec::new(),
                rates: Vec::new(),
                caches: CacheDemand::zero(),
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: Vec::new(),
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand::zero(),
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: 1,
                maximum_batch_size: 2,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([
                QuiescencePoint::RunBoundary,
                QuiescencePoint::MajorCycle,
            ]),
        },
        nodes: vec![
            WorkNode {
                id: first_id.clone(),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::new(),
                claims: vec![ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                }],
                allocations: Vec::new(),
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: boundary_id.clone(),
                kind: WorkKind::Synchronization,
                domain: WorkDomain::Control,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::from([WorkDependency::Work(first_id)]),
                claims: Vec::new(),
                allocations: Vec::new(),
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::from([QuiescencePoint::MajorCycle]),
            },
            WorkNode {
                id: WorkNodeId::new("minor-work"),
                kind: WorkKind::Compute,
                domain: WorkDomain::Cpu,
                implementation: work_implementation,
                dependencies: BTreeSet::from([WorkDependency::Work(boundary_id)]),
                claims: vec![ResourceClaim {
                    resource: casa_imaging_runtime::LeaseResource::Workers,
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
        adaptations: vec![AdaptationTransition {
            id: AdaptationId::new("larger-batch"),
            from: ExecutionKnobs::serial(),
            to: adapted,
            at: QuiescencePoint::MajorCycle,
            activate_nodes: BTreeSet::new(),
            deactivate_nodes: BTreeSet::new(),
        }],
    };
    transaction_binding(
        problem,
        specification,
        implementation(implementation_byte),
        product_participants(problem),
        false,
        true,
    )
}

pub(crate) fn conditional_adaptive_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("conditional physical-work problem");
    let work_implementation = implementation(implementation_byte);
    let retained = WorkNodeId::new("retained-route");
    let streamed = WorkNodeId::new("streamed-route");
    let retained_allocation = AllocationId::new("retained-route-buffer");
    let streamed_allocation = AllocationId::new("streamed-route-buffer");
    let retained_slot = PhysicalSlotId::new("retained-route-slot");
    let streamed_slot = PhysicalSlotId::new("streamed-route-slot");
    let route_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let route_claims = || {
        vec![
            ResourceClaim {
                resource: LeaseResource::Rate {
                    demand_id: "conditional-io-rate".to_string(),
                },
                amount: 1,
                lifetime: route_lifetime.clone(),
            },
            ResourceClaim {
                resource: LeaseResource::Queue {
                    demand_id: "conditional-io-queue".to_string(),
                },
                amount: 1,
                lifetime: route_lifetime.clone(),
            },
            ResourceClaim {
                resource: LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead),
                amount: 8,
                lifetime: route_lifetime.clone(),
            },
        ]
    };
    let compatibility = |layout| SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 8,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(layout),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let retained_compatibility = compatibility("retained-route-buffer");
    let streamed_compatibility = compatibility("streamed-route-buffer");
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 2;
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("conditional-adaptive-cpu"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: vec![
                    MemoryDemand {
                        allocation_id: "retained-route-slot".to_string(),
                        hard_bytes: 8,
                        preferred_bytes: 8,
                        views: vec![CapacityViewId::new("host-memory")],
                    },
                    MemoryDemand {
                        allocation_id: "streamed-route-slot".to_string(),
                        hard_bytes: 8,
                        preferred_bytes: 8,
                        views: vec![CapacityViewId::new("host-memory")],
                    },
                ],
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: Vec::new(),
                rates: vec![RateDemand {
                    demand_id: "conditional-io-rate".to_string(),
                    resource: RateResourceId::new("io-rate"),
                    amount: CountDemand::new(1, 1),
                }],
                caches: CacheDemand::zero(),
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: vec![QueueDemand {
                    demand_id: "conditional-io-queue".to_string(),
                    resource: QueueResourceId::new("io-queue"),
                    slots: CountDemand::new(1, 1),
                }],
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand {
                    source_read_ahead_bytes: 8,
                    ..IoBufferDemand::zero()
                },
            },
            headroom: ResourceHeadroom::default(),
            scaling: ScalingMetadata {
                minimum_workers: 1,
                maximum_workers: 1,
                maximum_batch_size: 2,
                maximum_tile_width: 1,
                maximum_tile_height: 1,
                maximum_slab_depth: 1,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
        },
        nodes: vec![
            WorkNode {
                id: retained.clone(),
                kind: WorkKind::Prefetch,
                domain: WorkDomain::Io,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::new(),
                claims: route_claims(),
                allocations: vec![AllocationUse {
                    allocation: retained_allocation.clone(),
                    lifetime: route_lifetime.clone(),
                }],
                fences: BTreeSet::from([FenceKind::Io]),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: streamed.clone(),
                kind: WorkKind::Prefetch,
                domain: WorkDomain::Io,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::new(),
                claims: route_claims(),
                allocations: vec![AllocationUse {
                    allocation: streamed_allocation.clone(),
                    lifetime: route_lifetime.clone(),
                }],
                fences: BTreeSet::from([FenceKind::Io]),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: WorkNodeId::new("conditional-route-join"),
                kind: WorkKind::Synchronization,
                domain: WorkDomain::Control,
                implementation: work_implementation.clone(),
                dependencies: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(retained.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(streamed.clone(), FenceKind::Io)),
                ]),
                claims: Vec::new(),
                allocations: Vec::new(),
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
        ],
        logical_allocations: vec![
            LogicalAllocation {
                id: retained_allocation.clone(),
                bytes: 8,
                purpose: AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead),
                compatibility: retained_compatibility.clone(),
                physical_slot: retained_slot.clone(),
                lifetime: AllocationLifetime {
                    disposition: casa_imaging_runtime::AllocationDisposition::Release,
                    acquire_at: retained.clone(),
                    release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                        retained.clone(),
                        FenceKind::Io,
                    ))]),
                },
            },
            LogicalAllocation {
                id: streamed_allocation.clone(),
                bytes: 8,
                purpose: AllocationPurpose::IoBuffer(IoBufferKind::SourceReadAhead),
                compatibility: streamed_compatibility.clone(),
                physical_slot: streamed_slot.clone(),
                lifetime: AllocationLifetime {
                    disposition: casa_imaging_runtime::AllocationDisposition::Release,
                    acquire_at: streamed.clone(),
                    release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                        streamed.clone(),
                        FenceKind::Io,
                    ))]),
                },
            },
        ],
        physical_slots: vec![
            PhysicalSlot {
                id: retained_slot,
                lease_resource: LeaseResource::Memory {
                    allocation_id: "retained-route-slot".to_string(),
                },
                capacity_bytes: 8,
                compatibility: retained_compatibility,
            },
            PhysicalSlot {
                id: streamed_slot,
                lease_resource: LeaseResource::Memory {
                    allocation_id: "streamed-route-slot".to_string(),
                },
                capacity_bytes: 8,
                compatibility: streamed_compatibility,
            },
        ],
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: vec![AdaptationTransition {
            id: AdaptationId::new("select-streamed-route"),
            from: ExecutionKnobs::serial(),
            to: adapted,
            at: QuiescencePoint::RunBoundary,
            activate_nodes: BTreeSet::from([streamed]),
            deactivate_nodes: BTreeSet::from([retained]),
        }],
    };
    transaction_binding(
        &problem,
        specification,
        work_implementation,
        default_product_participants(),
        false,
        true,
    )
}

pub(crate) fn auditable_physical_work(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
) -> PhysicalWorkBinding {
    let base = adaptive_physical_work_for_problem(problem, implementation_byte);
    let base_dag = base.execution_dag();
    let allocation = AllocationId::new("audit-generation");
    let slot = PhysicalSlotId::new("audit-slot");
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("audit-layout"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let mut alternative = base_dag.resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "audit-memory".to_string(),
        hard_bytes: 64,
        preferred_bytes: 32,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.headroom.memory_bytes =
        BTreeMap::from([(CapacityDomainId::new("host-memory"), 16)]);
    alternative.capabilities.supported =
        BTreeSet::from([casa_imaging_runtime::CapabilityId::new("audit-capability")]);
    alternative.scaling.memory_bytes_per_worker =
        BTreeMap::from([(CapacityDomainId::new("host-memory"), 8)]);
    let mut nodes = base_dag.nodes().values().cloned().collect::<Vec<_>>();
    nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("first-major-work"))
        .expect("audit allocation acquisition node")
        .allocations
        .push(AllocationUse {
            allocation: allocation.clone(),
            lifetime: ClaimLifetime::Work,
        });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::from([casa_imaging_runtime::CapabilityId::new(
            "audit-capability",
        )]),
        resource_alternative: alternative,
        nodes,
        logical_allocations: base_dag
            .logical_allocations()
            .values()
            .cloned()
            .chain([LogicalAllocation {
                id: allocation,
                bytes: 64,
                purpose: AllocationPurpose::Data,
                compatibility: compatibility.clone(),
                physical_slot: slot.clone(),
                lifetime: AllocationLifetime {
                    disposition: casa_imaging_runtime::AllocationDisposition::Release,
                    acquire_at: WorkNodeId::new("first-major-work"),
                    release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new(
                        "minor-work",
                    ))]),
                },
            }])
            .collect(),
        physical_slots: base_dag
            .physical_slots()
            .values()
            .cloned()
            .chain([PhysicalSlot {
                id: slot,
                lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                    allocation_id: "audit-memory".to_string(),
                },
                capacity_bytes: 64,
                compatibility,
            }])
            .collect(),
        initial_knobs: base_dag.initial_knobs().clone(),
        adaptations: base_dag.adaptations().values().cloned().collect(),
    })
    .expect("valid auditable physical work DAG");
    native_product_physical_work(
        problem,
        implementation_catalog(problem, &dag),
        dag,
        base.prediction().clone(),
        [PlannedArtifact::new(
            ArtifactIdentity::from_sha256([51; 32]),
            WorkNodeId::new("first-major-work"),
            ArtifactRole::Cache,
            Some(CacheIdentity::from_sha256([52; 32])),
        )]
        .into_iter()
        .chain(base.artifacts().iter().cloned())
        .collect(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("auditable physical work binding")
}

pub(crate) fn release_failure_physical_work(
    implementation_byte: u8,
    release_implementation_byte: u8,
    fail_at_fence: bool,
) -> PhysicalWorkBinding {
    let problem = compile(request(1)).expect("release-failure physical-work problem");
    let (independent_id, prepare_id, release_id, allocation_name, slot_name) = if fail_at_fence {
        (
            WorkNodeId::new("z-independent-io"),
            WorkNodeId::new("0-prepare-mapping"),
            WorkNodeId::new("a-release-mapping"),
            "fence-failed-mapping",
            "fence-failed-slot",
        )
    } else {
        (
            WorkNodeId::new("0-independent-io"),
            WorkNodeId::new("1-prepare-mapping"),
            WorkNodeId::new("2-release-mapping"),
            "execute-failed-mapping",
            "execute-failed-slot",
        )
    };
    let allocation_id = AllocationId::new(allocation_name);
    let physical_slot_id = PhysicalSlotId::new(slot_name);
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(allocation_name),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let asynchronous = ClaimLifetime::through_fence(FenceKind::Io);
    let release_lifetime = if fail_at_fence {
        asynchronous.clone()
    } else {
        ClaimLifetime::Work
    };
    let release_domain = if fail_at_fence {
        WorkDomain::Io
    } else {
        WorkDomain::Cpu
    };
    let mut release_claims = if fail_at_fence {
        vec![
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::Rate {
                    demand_id: "io-rate".to_string(),
                },
                amount: 1,
                lifetime: asynchronous.clone(),
            },
            ResourceClaim {
                resource: casa_imaging_runtime::LeaseResource::Queue {
                    demand_id: "io-queue".to_string(),
                },
                amount: 1,
                lifetime: asynchronous.clone(),
            },
        ]
    } else {
        vec![ResourceClaim {
            resource: casa_imaging_runtime::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }]
    };
    release_claims.push(ResourceClaim {
        resource: casa_imaging_runtime::LeaseResource::IoBuffer(IoBufferKind::MappedPageCache),
        amount: 100,
        lifetime: release_lifetime.clone(),
    });
    let release_fences = if fail_at_fence {
        BTreeSet::from([FenceKind::Io])
    } else {
        BTreeSet::new()
    };
    let release_after = if fail_at_fence {
        BTreeSet::from([WorkDependency::Fence(FenceId::new(
            release_id.clone(),
            FenceKind::Io,
        ))])
    } else {
        BTreeSet::from([WorkDependency::Work(release_id.clone())])
    };
    let mut initial_knobs = ExecutionKnobs::serial();
    if fail_at_fence {
        initial_knobs.io_depth = 2;
    }
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new(if fail_at_fence {
                "release-fence-failure"
            } else {
                "release-execute-failure"
            }),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory: vec![MemoryDemand {
                    allocation_id: slot_name.to_string(),
                    hard_bytes: 100,
                    preferred_bytes: 100,
                    views: vec![CapacityViewId::new("host-memory")],
                }],
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: Vec::new(),
                rates: vec![RateDemand {
                    demand_id: "io-rate".to_string(),
                    resource: RateResourceId::new("io-rate"),
                    amount: CountDemand::new(2, 1),
                }],
                caches: CacheDemand {
                    hard_resident_bytes: 100,
                    preferred_resident_bytes: 100,
                },
                locks: CountDemand::zero(),
                file_descriptors: CountDemand::zero(),
                queues: vec![QueueDemand {
                    demand_id: "io-queue".to_string(),
                    resource: QueueResourceId::new("io-queue"),
                    slots: CountDemand::new(2, 1),
                }],
                transfers: Vec::new(),
                accelerators: Vec::new(),
                io_buffers: IoBufferDemand {
                    mapped_page_cache_bytes: 100,
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
        nodes: vec![
            WorkNode {
                id: independent_id,
                kind: WorkKind::Io,
                domain: WorkDomain::Io,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::new(),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Rate {
                            demand_id: "io-rate".to_string(),
                        },
                        amount: 1,
                        lifetime: asynchronous.clone(),
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Queue {
                            demand_id: "io-queue".to_string(),
                        },
                        amount: 1,
                        lifetime: asynchronous,
                    },
                ],
                allocations: Vec::new(),
                fences: BTreeSet::from([FenceKind::Io]),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: prepare_id.clone(),
                kind: WorkKind::Cache,
                domain: WorkDomain::Cpu,
                implementation: implementation(implementation_byte),
                dependencies: BTreeSet::new(),
                claims: vec![
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::Workers,
                        amount: 1,
                        lifetime: ClaimLifetime::Work,
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::ResidentCache,
                        amount: 100,
                        lifetime: ClaimLifetime::Work,
                    },
                    ResourceClaim {
                        resource: casa_imaging_runtime::LeaseResource::IoBuffer(
                            IoBufferKind::MappedPageCache,
                        ),
                        amount: 100,
                        lifetime: ClaimLifetime::Work,
                    },
                ],
                allocations: vec![AllocationUse {
                    allocation: allocation_id.clone(),
                    lifetime: ClaimLifetime::Work,
                }],
                fences: BTreeSet::new(),
                quiescence_after: BTreeSet::new(),
            },
            WorkNode {
                id: release_id.clone(),
                kind: WorkKind::Release,
                domain: release_domain,
                implementation: implementation(release_implementation_byte),
                dependencies: BTreeSet::from([WorkDependency::Work(prepare_id.clone())]),
                claims: release_claims,
                allocations: vec![AllocationUse {
                    allocation: allocation_id.clone(),
                    lifetime: release_lifetime,
                }],
                fences: release_fences,
                quiescence_after: BTreeSet::new(),
            },
        ],
        logical_allocations: vec![LogicalAllocation {
            id: allocation_id,
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::MappedPageCache),
            compatibility: compatibility.clone(),
            physical_slot: physical_slot_id.clone(),
            lifetime: AllocationLifetime {
                disposition: casa_imaging_runtime::AllocationDisposition::Release,
                acquire_at: prepare_id.clone(),
                release_after,
            },
        }],
        physical_slots: vec![PhysicalSlot {
            id: physical_slot_id,
            lease_resource: casa_imaging_runtime::LeaseResource::Memory {
                allocation_id: slot_name.to_string(),
            },
            capacity_bytes: 100,
            compatibility,
        }],
        initial_knobs: ExecutionKnobs {
            cache_retention_bytes: 100,
            ..initial_knobs
        },
        adaptations: Vec::new(),
    };
    transaction_binding(
        &problem,
        specification,
        implementation(implementation_byte),
        default_product_participants(),
        false,
        true,
    )
}

pub(crate) fn mapped_publication_candidate(
    producer: WorkNodeId,
    terminal: WorkDependency,
    allocation: AllocationId,
) -> Result<PhysicalWorkBinding, PhysicalWorkBindingError> {
    let problem = compile(request(1)).expect("mapped-publication physical-work problem");
    let base = release_failure_physical_work(6, 8, false);
    let mapped = PublicationMappedStaging::new(producer, terminal, allocation)
        .expect("mapped producer and release differ");
    let layouts = PublicationLayoutLedger::new(
        base.publication_layouts()
            .entries()
            .iter()
            .map(|layout| {
                let writer = layout.staging();
                PublicationPhysicalLayout::new(
                    layout.participant(),
                    layout.artifact(),
                    layout.layout_id(),
                    PublicationStaging::new(
                        writer.producer().clone(),
                        writer.terminal().clone(),
                        writer.writer_buffer_kind(),
                        writer.writer_allocation().clone(),
                    )
                    .expect("existing writer staging")
                    .with_mapped_page_cache(mapped.clone()),
                    PublicationResourceBounds::new(
                        layout.resource_bounds().staged_storage_bytes(),
                        layout.resource_bounds().final_storage_bytes(),
                        layout.resource_bounds().writer_buffer_bytes(),
                        100,
                    )
                    .expect("mapped publication bounds"),
                )
            })
            .collect(),
    )
    .expect("one mapped layout per participant");
    let dag = base.execution_dag().clone();
    native_product_physical_work(
        &problem,
        implementation_catalog(&problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        layouts,
    )
}
