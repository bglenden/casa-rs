// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_imaging_model::AwProjectionContract;
use casa_imaging_reconstruction::{
    AwKernelLayout, AwOperatorError, AwPreparedCatalog, AwPreparedCellLease,
    AwPreparedCellMetadata, AwPreparedCellProvider, PreparedAwProjection,
};
use casa_imaging_runtime::PreparedArtifactReaderFactory;

const DECODED_BYTES: usize = 4096;

#[derive(Clone)]
struct UnreadProvider;

impl AwPreparedCellProvider for UnreadProvider {
    fn load(
        &mut self,
        _metadata: &AwPreparedCellMetadata,
        _resident_byte_ceiling: usize,
    ) -> Result<AwPreparedCellLease, AwOperatorError> {
        panic!("physical plan composition must not read CF payloads")
    }
}

fn aw_problem() -> casa_imaging_model::CompiledProblem {
    let geometry =
        geometry_with_shape_and_increment([4.0, 4.0], ImageShape::new(8, 8), [-1.0e-6, 1.0e-6]);
    let base = compile(request_with_geometry(1, geometry.clone())).expect("base problem");
    let aw = AwProjectionContract::new(
        3.0,
        std::num::NonZeroUsize::new(1).unwrap(),
        false,
        false,
        false,
        true,
        false,
        [0.0, 0.0],
        360.0,
        360.0,
    )
    .expect("AW contract");
    let science = ScientificContract::new(
        base.science().spectral(),
        MeasurementEquationContract::new(
            InstrumentResponse::PrimaryBeam,
            base.science().measurement_equation().inner_products(),
        )
        .with_aw_projection(aw),
    )
    .with_instrument_model(InstrumentModel::CasaEvlaWidebandAwV1);
    compile(ImagingRequest::new(
        ProblemSpecification::new(
            science,
            base.reconstruction().clone(),
            WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
            base.products().clone(),
            ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
            base.numerics().clone(),
        ),
        geometry,
        problem_inputs(
            1,
            vec![
                (ReferenceDataKind::Measures, identity(90)),
                (ReferenceDataKind::Instrument, identity(91)),
            ],
            ModelStateIdentity::Empty,
        ),
        model_lifecycle(ModelStateIdentity::Empty),
    ))
    .expect("compiled AW problem")
}

fn projection() -> PreparedAwProjection {
    let metadata = AwPreparedCellMetadata::new(
        prepared_cell(1.0e9),
        1.0e9,
        3.0,
        1.0,
        1,
        0.125,
        AwKernelLayout::new([1, 1], 1, [5, 5], [2, 2]).unwrap(),
        AwKernelLayout::new([2, 2], 1, [7, 7], [3, 3]).unwrap(),
    )
    .expect("paired CF metadata");
    PreparedAwProjection::new(
        AwPreparedCatalog::new(vec![metadata]).expect("AW catalog"),
        UnreadProvider,
        true,
        DECODED_BYTES,
    )
    .expect("bounded AW projection")
}

fn fragment(problem: &casa_imaging_model::CompiledProblem) -> CompleteDataPlanFragment {
    CompleteDataPlanFragment::new(
        problem,
        4,
        WorkNodeId::new("execute"),
        SpectralOperatorPass::InitialMajor,
    )
    .expect("AW complete-data fragment")
}

fn reconstruction_base(problem: &casa_imaging_model::CompiledProblem) -> PhysicalWorkBinding {
    let check = WorkNodeId::new("aw-check");
    let replay = WorkNodeId::new("execute");
    let reconcile = WorkNodeId::new("aw-reconcile");
    let commit = WorkNodeId::new("aw-commit");
    let io_resources = serial_storage_io();
    let measurement_sets = problem
        .observation_transaction()
        .read_set()
        .sources()
        .iter()
        .map(|source| source.measurement_set())
        .collect::<BTreeSet<_>>();
    let mut nodes = [
        (
            check.clone(),
            WorkKind::DataCensus,
            WorkDomain::Cpu,
            BTreeSet::new(),
            BTreeSet::new(),
        ),
        (
            replay.clone(),
            WorkKind::ObservationRead,
            WorkDomain::Io,
            BTreeSet::from([WorkDependency::Work(check.clone())]),
            BTreeSet::from([FenceKind::Io]),
        ),
        (
            reconcile.clone(),
            WorkKind::Compute,
            WorkDomain::Cpu,
            BTreeSet::from([WorkDependency::Fence(FenceId::new(
                replay.clone(),
                FenceKind::Io,
            ))]),
            BTreeSet::new(),
        ),
        (
            commit.clone(),
            WorkKind::Publication,
            WorkDomain::Io,
            BTreeSet::from([WorkDependency::Work(reconcile.clone())]),
            BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
        ),
    ]
    .into_iter()
    .map(|(id, kind, domain, dependencies, fences)| {
        let lifetime = if fences.is_empty() {
            ClaimLifetime::Work
        } else {
            ClaimLifetime::through_fences(fences.iter().copied())
        };
        let mut claims = vec![ResourceClaim {
            resource: LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }];
        if id != reconcile {
            claims.extend(
                measurement_sets
                    .iter()
                    .map(|&measurement_set| ResourceClaim {
                        resource: LeaseResource::MeasurementSetLock { measurement_set },
                        amount: 1,
                        lifetime: lifetime.clone(),
                    }),
            );
        }
        if domain == WorkDomain::Io {
            claims.extend([
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "aw-fixture-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: "aw-fixture-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
            ]);
        }
        if id == commit {
            claims.push(ResourceClaim {
                resource: LeaseResource::Storage {
                    demand_id: "aw-fixture-output".to_string(),
                    use_kind: StorageUseKind::StagedOutput,
                },
                amount: 1,
                lifetime,
            });
        }
        WorkNode {
            id,
            kind,
            domain,
            implementation: implementation(6),
            dependencies,
            claims,
            allocations: vec![],
            fences,
            quiescence_after: BTreeSet::new(),
        }
    })
    .collect::<Vec<_>>();
    let mut memory = Vec::new();
    let mut logical_allocations = Vec::new();
    let mut physical_slots = Vec::new();
    for (node_id, name, kind) in [
        (
            &replay,
            "aw-fixture-read-buffer",
            IoBufferKind::SourceReadAhead,
        ),
        (
            &commit,
            "aw-fixture-commit-buffer",
            IoBufferKind::Publication,
        ),
    ] {
        let node = nodes.iter_mut().find(|node| &node.id == node_id).unwrap();
        let lifetime = ClaimLifetime::through_fences(node.fences.iter().copied());
        let allocation = AllocationId::new(name);
        let slot = PhysicalSlotId::new(name);
        let compatibility = SlotCompatibility {
            memory_domain: CapacityDomainId::new("host-memory"),
            views: BTreeSet::from([CapacityViewId::new("host-memory")]),
            alignment_bytes: 1,
            storage_mode: StorageMode::Host,
            layout: AllocationLayout::new(name),
            initialization: InitializationPolicy::OverwriteBeforeRead,
            access: AllocationAccess::ReadWrite,
        };
        node.claims.push(ResourceClaim {
            resource: LeaseResource::IoBuffer(kind),
            amount: 1,
            lifetime: lifetime.clone(),
        });
        node.allocations.push(AllocationUse {
            allocation: allocation.clone(),
            lifetime,
        });
        memory.push(MemoryDemand {
            allocation_id: name.to_string(),
            hard_bytes: 1,
            preferred_bytes: 1,
            views: vec![CapacityViewId::new("host-memory")],
        });
        logical_allocations.push(LogicalAllocation {
            id: allocation,
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(kind),
            compatibility: compatibility.clone(),
            physical_slot: slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: node_id.clone(),
                release_after: node
                    .fences
                    .iter()
                    .map(|&fence| WorkDependency::Fence(FenceId::new(node_id.clone(), fence)))
                    .collect(),
            },
        });
        physical_slots.push(PhysicalSlot {
            id: slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: name.to_string(),
            },
            capacity_bytes: 1,
            compatibility,
        });
    }
    let lock_count = measurement_sets.len() as u64;
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("aw-fixture-reconstruction"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: CapacityViewId::new("host-memory"),
                memory,
                workers: CountDemand::new(1, 1),
                overhead: RuntimeOverheadDemand::zero(),
                storage: vec![casa_imaging_runtime::StorageDemand {
                    demand_id: "aw-fixture-output".to_string(),
                    domain: prepared_storage_domain().id.clone(),
                    temporary_bytes: 0,
                    staged_output_bytes: 1,
                    final_output_bytes: 0,
                    persistent_cache_bytes: 0,
                    read_rate: CountDemand::zero(),
                    write_rate: CountDemand::zero(),
                    operations_rate: CountDemand::zero(),
                    queue_slots: CountDemand::zero(),
                }],
                rates: vec![RateDemand {
                    demand_id: "aw-fixture-io-rate".to_string(),
                    resource: io_resources.read_rate().clone(),
                    amount: CountDemand::new(1, 1),
                }],
                caches: CacheDemand::zero(),
                locks: CountDemand::new(lock_count, lock_count),
                file_descriptors: CountDemand::zero(),
                queues: vec![QueueDemand {
                    demand_id: "aw-fixture-io-queue".to_string(),
                    resource: io_resources.queue().clone(),
                    slots: CountDemand::new(1, 1),
                }],
                transfers: vec![],
                accelerators: vec![],
                io_buffers: IoBufferDemand {
                    source_read_ahead_bytes: 1,
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
        logical_allocations,
        physical_slots,
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: vec![],
    })
    .expect("reconstruction-stage fixture DAG");
    let prediction = PlanPrediction::new(
        4_000,
        PredictionConfidence::new(900_000).unwrap(),
        vec![],
        dag.nodes()
            .values()
            .map(|node| {
                StagePrediction::new(node.id.clone(), 1_000).with_io(
                    node.claims
                        .iter()
                        .filter_map(|claim| match claim.resource {
                            LeaseResource::IoBuffer(kind) => Some(IoPrediction::new(kind, 1, 1)),
                            _ => None,
                        })
                        .collect(),
                )
            })
            .collect(),
    )
    .expect("reconstruction-stage predictions");
    PhysicalWorkBinding::new_reconstruction(
        implementation_catalog(problem, &dag),
        dag,
        prediction,
        vec![],
        ObservationTransactionWork::new_reconstruction(check, reconcile, commit),
        PublicationLayoutLedger::empty(),
    )
    .expect("AW-problem-bound reconstruction stage")
}

fn assert_metadata_lifetime(
    physical: &PhysicalWorkBinding,
    fragment: &CompleteDataPlanFragment,
    bytes: usize,
) {
    let dag = physical.execution_dag();
    let metadata = dag
        .logical_allocations()
        .values()
        .find(|allocation| {
            allocation
                .id
                .as_str()
                .starts_with("spectral-operator-aw-catalog-metadata-")
        })
        .expect("separate AW catalog allocation");
    assert_eq!(metadata.bytes, bytes as u64);
    let reconciliation = fragment.reconciliation_node().expect("reconciliation");
    assert_eq!(metadata.lifetime.acquire_at, *fragment.preparation_node());
    assert_eq!(
        metadata.lifetime.release_after,
        BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
    );
    for (node, lifetime) in [
        (fragment.preparation_node().clone(), ClaimLifetime::Work),
        (
            WorkNodeId::new("execute"),
            ClaimLifetime::through_fence(FenceKind::Io),
        ),
        (reconciliation.clone(), ClaimLifetime::Work),
    ] {
        assert!(
            dag.nodes()[&node]
                .allocations
                .iter()
                .any(|usage| { usage.allocation == metadata.id && usage.lifetime == lifetime }),
            "metadata use missing from {node:?}"
        );
    }
    let slot = &dag.physical_slots()[&metadata.physical_slot];
    assert_eq!(slot.capacity_bytes, bytes as u64);
    assert!(matches!(slot.lease_resource, LeaseResource::Memory { .. }));
}

#[test]
fn t51_aw_projection_accounts_for_catalog_beside_decoded_pool() {
    let problem = aw_problem();
    let unbound = fragment(&problem);
    let baseline = unbound.residency();
    let projection = projection();
    let metadata_bytes = projection.catalog_resident_bytes();
    assert!(metadata_bytes > 0);
    let bound = unbound
        .with_aw_projection(projection)
        .expect("bind AW projection");
    assert_eq!(bound.residency().grid_bytes(), baseline.grid_bytes());
    assert_eq!(bound.residency().aw_prepared_pool_bytes(), DECODED_BYTES);
    assert_eq!(
        bound.residency().aw_catalog_metadata_bytes(),
        metadata_bytes
    );
    assert_eq!(
        bound.residency().peak_bytes(),
        baseline.peak_bytes() + DECODED_BYTES + metadata_bytes,
    );
    let (physical, bound) = bound
        .compose(&reconstruction_base(&problem))
        .expect("compose AW physical allocations");
    assert_metadata_lifetime(&physical, &bound, metadata_bytes);
}

#[test]
fn t51_aw_reader_replaces_only_pool_and_preserves_catalog_reservation() {
    let problem = aw_problem();
    let cache = prepared_tempdir();
    let store = Arc::new(
        PreparedArtifactStore::open(cache.path(), prepared_storage_domain(), prepared_budget())
            .expect("prepared store"),
    );
    let registration = PreparedArtifactRegistration::new(
        "casa-rs-imaging-v1",
        "native-awproject",
        "3.1.0",
        implementation(6),
    )
    .expect("consumer registration");
    let (catalog, implementation_id) = prepared_catalog(registration.clone());
    let descriptor = PreparedArtifactDescriptor::convolution_function(
        &store,
        &catalog,
        &implementation_id,
        &problem,
        prepared_cell(1.0e9),
        PreparedArtifactPlaneDescriptor::new(
            [5, 5],
            [1, 1],
            1,
            prepared_uv(5),
            PreparedArtifactPrecision::ComplexF32,
            PreparedArtifactOrder::Axis0ContiguousLittleEndian,
        )
        .expect("padded imaging plane"),
        PreparedArtifactPlaneDescriptor::new(
            [7, 7],
            [2, 2],
            1,
            prepared_uv(7),
            PreparedArtifactPrecision::ComplexF32,
            PreparedArtifactOrder::LastAxisContiguousLittleEndian,
        )
        .expect("padded weight plane"),
    )
    .expect("compiled-problem-derived padded descriptor");
    let adapter = PreparedOperationAdapter::new(
        PreparedArtifactOperation::Generate,
        PreparedArtifactStore::open(cache.path(), prepared_storage_domain(), prepared_budget())
            .expect("generation store"),
        descriptor.clone(),
    );
    let id = adapter.id.clone();
    let registry = PreparedSuiteRegistry {
        id: registry(3),
        metadata: Some(ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        )),
        implementations: BTreeMap::from([
            (
                implementation(6),
                PreparedSuiteImplementation::Base(Box::new(recording_executor(6, None, None))),
            ),
            (
                id.clone(),
                PreparedSuiteImplementation::Prepared(Box::new(adapter)),
            ),
        ]),
        prepared: BTreeMap::from([(implementation(6), registration)]),
    };
    let base = PreparedArtifactPlanFragment::standalone_base(
        &problem,
        &registry,
        implementation(6),
        &descriptor,
        &store,
        1_000,
        900_000,
    )
    .expect("cache-only generation base");
    let setup = PreparedArtifactPlanFragment::new(
        &descriptor,
        &store,
        PreparedArtifactOperation::Generate,
        WorkNodeId::new("prepared-phase-producer"),
        WorkNodeId::new("prepared-phase-commit"),
        implementation(6),
    )
    .compose(&base)
    .expect("cache-only generation plan");
    let plan = plan(
        &problem,
        PlanningBindings::new(registry.id, ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(setup),
    )
    .expect("prepared generation plan");
    run_prepared(
        &problem,
        &plan,
        &registry,
        plan.receipt_store().bind(execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([241; 32]),
            BuildIdentity::from_sha256([242; 32]),
        )),
    )
    .expect("actual prepared generation");
    let PreparedSuiteImplementation::Prepared(adapter) = &registry.implementations[&id] else {
        panic!("prepared generation adapter");
    };
    let artifact = adapter
        .retained_artifact
        .lock()
        .unwrap()
        .take()
        .expect("generated handle");
    let reader = PreparedArtifactReaderFactory::new(
        store,
        vec![(descriptor, artifact)],
        implementation(6),
        DECODED_BYTES as u64,
        64,
    )
    .expect("real reader plan");
    let projection = projection();
    let metadata_bytes = projection.catalog_resident_bytes();
    let bound = fragment(&problem)
        .with_aw_projection(projection)
        .expect("AW projection");
    let before_reader = bound.residency();
    let reader_bytes = reader.plan().total_resident_bytes() as usize;
    let bound = bound
        .with_prepared_artifact_reader(reader.plan().clone())
        .expect("reader binding");
    assert_eq!(
        bound.residency().aw_catalog_metadata_bytes(),
        metadata_bytes
    );
    assert_eq!(bound.residency().aw_prepared_pool_bytes(), reader_bytes);
    assert_eq!(
        bound.residency().peak_bytes(),
        before_reader.peak_bytes() - DECODED_BYTES + reader_bytes,
    );
    let (physical, bound) = bound
        .compose(&reconstruction_base(&problem))
        .expect("compose reader and operator allocations");
    assert_metadata_lifetime(&physical, &bound, metadata_bytes);
    let pool = physical
        .execution_dag()
        .logical_allocations()
        .values()
        .find(|allocation| {
            allocation
                .id
                .as_str()
                .starts_with("spectral-operator-aw-prepared-pool-")
        })
        .expect("reader pool allocation");
    assert_eq!(pool.bytes, reader_bytes as u64);
    assert_eq!(pool.lifetime.acquire_at, *reader.plan().node());
    assert_eq!(
        pool.lifetime.release_after,
        BTreeSet::from([WorkDependency::Work(reader.plan().release_node().clone())]),
    );
}
