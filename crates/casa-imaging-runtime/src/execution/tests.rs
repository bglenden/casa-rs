// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    InstrumentResponse, MeasurementEquationContract, ModelColumnWrite, ModelInnerProduct,
    ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw, PolarizationContract,
    PolarizationCoordinate, ProblemSpecification, ProductKind, ProductNormalization,
    ProductRequirements, Projection, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, RestFrequency,
    RestoringBeamPolicy, ScientificContract, SkyDirection, SpectralContract,
    SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor, SpectralSamplingLaw,
    SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct, WeightDensityScope,
    WeightingContract, WeightingScheme, compile,
};
use sha2::{Digest, Sha256};

#[path = "../../tests/common/mod.rs"]
mod common;

use common::{identity, model_lifecycle, problem_inputs};

use super::*;
use crate::{
    Accelerator, AcceleratorDemand, AcceleratorId, AcceleratorKind, AlternativeId, CacheDemand,
    CapabilityPredicate, CapacityDomainId, CapacityViewId, CountDemand, CpuClassCapacity,
    CpuDataWorkingSetCapacity, DemandAlternative, DemandEnvelope, ExternalPressure, HostInventory,
    ImplementationContractCatalog, ImplementationContractMetadata, ImplementationRegistryId,
    IoBufferDemand, IoBufferKind, MemoryCapacityDomain, MemoryCapacityKind, MemoryDemand,
    MemoryView, MemoryViewKind, ObservationTransactionWork, PhysicalWorkBinding,
    PlannerCostModelProfileId, PlannerCostModelProfileRecord, PlanningBindings, QueueDemand,
    QueueResource, QueueResourceId, QuiescencePoint, RateDemand, RateResource, RateResourceId,
    RateUnit, ResourceAuthority, ResourceHeadroom, ResourcePolicy, ResourceTopology,
    RuntimeOverheadDemand, ScalingMetadata, StorageDemand, StorageDomain, StorageDomainId,
    plan as authority_plan,
};

fn product_validity() -> casa_imaging_model::ProductValidityPolicies {
    casa_imaging_model::ProductValidityPolicies::new(
        casa_imaging_model::PrimaryBeamValidityPolicy::new(
            0.2,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid PB policy"),
        casa_imaging_model::TaylorValidityPolicy::new(
            casa_imaging_model::TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            0.1,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("valid Taylor policy"),
    )
}

fn plan<E>(
    problem: &casa_imaging_model::CompiledProblem,
    bindings: PlanningBindings,
    planner: impl FnOnce(
        &casa_imaging_model::CompiledProblem,
        &PlanningBindings,
    ) -> Result<PhysicalWorkBinding, E>,
) -> Result<crate::ExecutionPlan, crate::PlanError<E>> {
    let receipts = empty_receipt_store();
    plan_with_receipts(problem, bindings, &receipts, planner)
}

fn plan_with_receipts<E>(
    problem: &casa_imaging_model::CompiledProblem,
    bindings: PlanningBindings,
    receipts: &crate::ExecutionReceiptStore,
    planner: impl FnOnce(
        &casa_imaging_model::CompiledProblem,
        &PlanningBindings,
    ) -> Result<PhysicalWorkBinding, E>,
) -> Result<crate::ExecutionPlan, crate::PlanError<E>> {
    let candidate = planner(problem, &bindings).map_err(crate::PlanError::Planner)?;
    let demand = &candidate.execution_dag().resource_alternative().demand;
    let uses_unified_domain = candidate
        .execution_dag()
        .physical_slots()
        .values()
        .any(|slot| slot.compatibility.memory_domain.as_str() == "unified-memory");
    let authority = if !demand.accelerators.is_empty() || uses_unified_domain {
        unified_authority()
    } else if !demand.rates.is_empty() || !demand.queues.is_empty() {
        io_authority()
    } else {
        cpu_authority()
    };
    let implementation_ids = candidate
        .execution_dag()
        .nodes()
        .values()
        .map(|node| node.implementation.clone());
    let registry = ContractOnlyRegistry::new(
        bindings.implementation_registry_id(),
        ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        ),
        implementation_ids,
    );
    match authority_plan(
        problem,
        bindings,
        &authority,
        &registry,
        receipts,
        |_, _| Ok::<_, std::convert::Infallible>(vec![candidate]),
    ) {
        Ok(plan) => Ok(plan),
        Err(crate::PlanError::InvalidCandidate(error)) => {
            Err(crate::PlanError::InvalidCandidate(error))
        }
        Err(crate::PlanError::Receipt(error)) => Err(crate::PlanError::Receipt(error)),
        Err(crate::PlanError::Resource(error)) => Err(crate::PlanError::Resource(error)),
        Err(crate::PlanError::ObservationTransaction(error)) => {
            Err(crate::PlanError::ObservationTransaction(error))
        }
        Err(crate::PlanError::Planner(error)) => match error {},
    }
}

fn compiled_problem() -> casa_imaging_model::CompiledProblem {
    compiled_problem_with_reference_data(Vec::new())
}

fn compiled_problem_with_reference_data(
    reference_data: Vec<(
        casa_imaging_model::ReferenceDataKind,
        casa_imaging_model::LogicalIdentity,
    )>,
) -> casa_imaging_model::CompiledProblem {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [31.0, 31.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    let geometry = GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(64, 64),
            direction,
            FacetLayout::Single,
            AxisOrder::new([
                ImageAxis::DirectionLongitude,
                ImageAxis::DirectionLatitude,
                ImageAxis::Polarization,
                ImageAxis::Spectral,
            ]),
        )],
        CentreLaws::new(
            PhaseCentreLaw::Observation,
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::PhaseTrackingCentre,
        ),
        UvwCoordinateLaw::PhaseTrackingCentre,
        SpectralCoordinateSpec::new(
            FrequencyFrame::Topocentric,
            FrequencyFrame::Topocentric,
            SpectralFrameAnchor::NotApplicable,
            SpectralWcs::Linear {
                channels: 1,
                reference_pixel: 0.0,
                reference_frequency_hz: 1.4e9,
                increment_hz: 1.0e6,
            },
            RestFrequency::NotApplicable,
            DopplerConvention::NotApplicable,
        ),
    );
    let specification = ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
            MeasurementEquationContract::new(
                InstrumentResponse::Scalar,
                DeclaredInnerProducts::new(
                    ModelInnerProduct::HermitianEuclidean,
                    VisibilityInnerProduct::HermitianEuclidean,
                ),
            ),
        ),
        ReconstructionContract::new(
            ReconstructionBasis::Constant,
            ReconstructionAlgorithm::Hogbom,
            ReconstructionControls::new(10, 0.1, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        ProductRequirements::new(
            vec![ProductKind::Psf, ProductKind::Residual, ProductKind::Model],
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
            product_validity(),
        ),
        ObservationTransactionRequirements::new(ModelColumnWrite::Disabled),
        NumericsContract::new(
            vec![NumericPrecision::F64],
            ReductionPolicy::DeterministicPairwise,
            FiniteValuePolicy::FlagInputRejectGenerated,
            NumericalStage::ALL
                .into_iter()
                .map(|stage| (stage, StageErrorBudget::new(1.0e-12, 1.0e-3)))
                .collect(),
        ),
    );
    let inputs = problem_inputs(1, reference_data, ModelStateIdentity::Empty);
    compile(ImagingRequest::new(
        specification,
        geometry,
        inputs,
        model_lifecycle(ModelStateIdentity::Empty),
    ))
    .expect("valid scheduler test problem")
}

fn cpu_node(id: &str, dependencies: BTreeSet<WorkDependency>) -> WorkNode {
    WorkNode {
        id: WorkNodeId::new(id),
        kind: WorkKind::Compute,
        domain: WorkDomain::Cpu,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies,
        claims: vec![ResourceClaim {
            resource: crate::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }],
        allocations: Vec::new(),
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    }
}

fn synchronization_node(id: &str, dependencies: BTreeSet<WorkDependency>) -> WorkNode {
    WorkNode {
        id: WorkNodeId::new(id),
        kind: WorkKind::Synchronization,
        domain: WorkDomain::Control,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies,
        claims: Vec::new(),
        allocations: Vec::new(),
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    }
}

fn plan_spec(nodes: Vec<WorkNode>) -> ExecutionDagSpecification {
    let host = CapacityViewId::new("host-memory");
    ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::new(),
        resource_alternative: DemandAlternative {
            id: AlternativeId::new("cpu-reference"),
            capabilities: CapabilityPredicate::default(),
            demand: DemandEnvelope {
                host_memory_view: host,
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
                maximum_batch_size: 8,
                maximum_tile_width: 8,
                maximum_tile_height: 8,
                maximum_slab_depth: 8,
                memory_bytes_per_worker: BTreeMap::new(),
            },
            quiescence_points: BTreeSet::from([QuiescencePoint::RunBoundary]),
        },
        nodes,
        logical_allocations: Vec::new(),
        physical_slots: Vec::new(),
        initial_knobs: ExecutionKnobs::serial(),
        adaptations: Vec::new(),
    }
}

fn cpu_authority() -> ResourceAuthority {
    cpu_authority_with_workers(2)
}

fn cpu_authority_with_workers(workers: u64) -> ResourceAuthority {
    let domain = CapacityDomainId::new("host-memory");
    let view = CapacityViewId::new("host-memory");
    ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1_024,
            }],
            memory_views: vec![MemoryView {
                id: view,
                domain: domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: Vec::new(),
            queue_resources: Vec::new(),
            logical_cpu_threads: workers,
            performance_cpu_cores: CpuClassCapacity::Known(workers),
            cpu_data_working_set: CpuDataWorkingSetCapacity::Unknown,
            cache_capacity_bytes: 1_024,
            lock_capacity: 8,
            file_descriptor_capacity: 8,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_024)]),
            available_cpu_threads: workers,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::new(),
            queue_available_slots: BTreeMap::new(),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_024,
            available_locks: 8,
            available_file_descriptors: 8,
        },
    })
    .expect("valid scheduler test inventory")
}

fn unified_authority() -> ResourceAuthority {
    let domain = CapacityDomainId::new("unified-memory");
    let host = CapacityViewId::new("host-memory");
    let metal = CapacityViewId::new("metal-memory");
    let accelerator = AcceleratorId::new("metal-0");
    let command_queue = QueueResourceId::new("metal-command-queue");
    let io_rate = RateResourceId::new("io-rate");
    let io_queue = QueueResourceId::new("io-queue");
    ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
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
                    domain: domain.clone(),
                    kind: MemoryViewKind::Metal,
                },
            ],
            accelerators: vec![Accelerator {
                id: accelerator.clone(),
                kind: AcceleratorKind::Metal,
                memory_view: metal,
                command_queue: command_queue.clone(),
                occupancy_slots: 1,
            }],
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: vec![RateResource::new(
                io_rate.clone(),
                RateUnit::BytesPerSecond,
                100,
            )],
            queue_resources: vec![
                QueueResource::new(command_queue.clone(), 1),
                QueueResource::new(io_queue.clone(), 1),
            ],
            logical_cpu_threads: 2,
            performance_cpu_cores: CpuClassCapacity::Known(2),
            cpu_data_working_set: CpuDataWorkingSetCapacity::Unknown,
            cache_capacity_bytes: 1_024,
            lock_capacity: 8,
            file_descriptor_capacity: 8,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_024)]),
            available_cpu_threads: 2,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::from([(io_rate, 100)]),
            queue_available_slots: BTreeMap::from([(command_queue, 1), (io_queue, 1)]),
            accelerator_available_slots: BTreeMap::from([(accelerator, 1)]),
            cache_available_bytes: 1_024,
            available_locks: 8,
            available_file_descriptors: 8,
        },
    })
    .expect("valid Apple-style unified scheduler inventory")
}

fn io_authority() -> ResourceAuthority {
    io_authority_with_workers(2)
}

fn io_authority_with_workers(workers: u64) -> ResourceAuthority {
    let domain = CapacityDomainId::new("host-memory");
    let view = CapacityViewId::new("host-memory");
    let rate = RateResourceId::new("io-rate");
    let queue = QueueResourceId::new("io-queue");
    let transaction_rate = RateResourceId::new("transaction-io-rate");
    let transaction_queue = QueueResourceId::new("transaction-io-queue");
    let transaction_storage = StorageDomainId::new("transaction-output");
    ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1_024,
            }],
            memory_views: vec![MemoryView {
                id: view,
                domain: domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: vec![StorageDomain {
                id: transaction_storage.clone(),
                root: std::path::PathBuf::from("/tmp/casa-rs-runtime-transaction-output"),
                capacity_bytes: 1_024,
                read_rate: transaction_rate.clone(),
                write_rate: transaction_rate.clone(),
                operations_rate: None,
                queue: transaction_queue.clone(),
            }],
            rate_resources: vec![
                RateResource::new(rate.clone(), RateUnit::BytesPerSecond, 100),
                RateResource::new(transaction_rate.clone(), RateUnit::BytesPerSecond, 100),
            ],
            queue_resources: vec![
                QueueResource::new(queue.clone(), 1),
                QueueResource::new(transaction_queue.clone(), 1),
            ],
            logical_cpu_threads: workers,
            performance_cpu_cores: CpuClassCapacity::Known(workers),
            cpu_data_working_set: CpuDataWorkingSetCapacity::Unknown,
            cache_capacity_bytes: 1_024,
            lock_capacity: 8,
            file_descriptor_capacity: 8,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_024)]),
            available_cpu_threads: workers,
            storage_available_bytes: BTreeMap::from([(transaction_storage, 1_024)]),
            rate_available_per_second: BTreeMap::from([(rate, 100), (transaction_rate, 100)]),
            queue_available_slots: BTreeMap::from([(queue, 1), (transaction_queue, 1)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_024,
            available_locks: 8,
            available_file_descriptors: 8,
        },
    })
    .expect("valid scheduler I/O inventory")
}

fn bound_plan(dag: ExecutionDag) -> crate::ExecutionPlan {
    bound_plan_with_authority(dag, &io_authority())
}

fn bound_plan_with_authority(
    dag: ExecutionDag,
    authority: &ResourceAuthority,
) -> crate::ExecutionPlan {
    let problem = compiled_problem();
    let implementation_ids = dag.nodes().values().map(|node| node.implementation.clone());
    let registry = ContractOnlyRegistry::new(
        ImplementationRegistryId::from_sha256([7; 32]),
        ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        ),
        implementation_ids,
    );
    authority_plan(
        &problem,
        PlanningBindings::new(
            ImplementationRegistryId::from_sha256([7; 32]),
            ResourcePolicy::Exclusive,
            PlannerCostModelProfileRecord::initial(PlannerCostModelProfileId::from_sha256([8; 32])),
        ),
        authority,
        &registry,
        &empty_receipt_store(),
        |_, _| Ok::<_, std::convert::Infallible>(vec![physical_work_binding(dag)]),
    )
    .expect("physical planning succeeds")
}

fn empty_receipt_store() -> crate::ExecutionReceiptStore {
    let directory = tempfile::tempdir().expect("empty receipt directory");
    let root = directory.keep();
    crate::ExecutionReceiptStore::new(
        root,
        crate::ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("empty receipt store")
}

fn execution_provenance(
    attempt: crate::ExecutionAttemptId,
    build: crate::BuildIdentity,
) -> crate::ExecutionProvenance {
    crate::ExecutionProvenance::new(attempt, build)
}

fn physical_work_binding(dag: ExecutionDag) -> PhysicalWorkBinding {
    physical_work_binding_with_artifacts(dag, Vec::new())
}

fn physical_work_binding_with_artifacts(
    dag: ExecutionDag,
    artifacts: Vec<crate::PlannedArtifact>,
) -> PhysicalWorkBinding {
    let problem = compiled_problem();
    physical_work_binding_with_problem(&problem, dag, artifacts)
}

fn physical_work_binding_with_problem(
    problem: &casa_imaging_model::CompiledProblem,
    dag: ExecutionDag,
    artifacts: Vec<crate::PlannedArtifact>,
) -> PhysicalWorkBinding {
    let initial = WorkNodeId::new("transaction-check");
    let read = WorkNodeId::new("transaction-read");
    let reconciliation = WorkNodeId::new("post-replay-reconciliation");
    let stage = WorkNodeId::new("transaction-stage-products");
    let commit = WorkNodeId::new("transaction-commit");
    let implementation = WorkImplementationId::new("cpu-reference");
    let writer_allocation = AllocationId::new("transaction-product-writer");
    let writer_slot = PhysicalSlotId::new("transaction-product-writer-slot");
    let publication_allocation = AllocationId::new("transaction-publication-buffer");
    let publication_slot = PhysicalSlotId::new("transaction-publication-slot");
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 1,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("transaction-publication-buffer"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
    };
    let predecessors = dag
        .nodes()
        .values()
        .flat_map(|node| node.dependencies.iter())
        .map(|dependency| match dependency {
            WorkDependency::Work(node) => node.clone(),
            WorkDependency::Fence(fence) => fence.node().clone(),
        })
        .collect::<BTreeSet<_>>();
    let terminals = dag
        .nodes()
        .values()
        .filter(|node| !predecessors.contains(&node.id))
        .flat_map(|node| {
            if node.fences.is_empty() {
                vec![WorkDependency::Work(node.id.clone())]
            } else {
                node.fences
                    .iter()
                    .map(|kind| WorkDependency::Fence(FenceId::new(node.id.clone(), *kind)))
                    .collect()
            }
        })
        .collect::<BTreeSet<_>>();
    let read_completion = WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io));
    let mut nodes = dag.nodes().values().cloned().collect::<Vec<_>>();
    for node in nodes.iter_mut().filter(|node| node.dependencies.is_empty()) {
        node.dependencies.insert(read_completion.clone());
    }
    let measurement_set = casa_imaging_model::MeasurementSetIdentity::new(identity(1));
    nodes.extend([
        WorkNode {
            id: initial.clone(),
            kind: WorkKind::DataCensus,
            domain: WorkDomain::Cpu,
            implementation: implementation.clone(),
            dependencies: BTreeSet::new(),
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: crate::LeaseResource::MeasurementSetLock { measurement_set },
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
            implementation: implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(initial.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::MeasurementSetLock { measurement_set },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
            ],
            allocations: Vec::new(),
            fences: BTreeSet::from([FenceKind::Io]),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: reconciliation.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: implementation.clone(),
            dependencies: terminals,
            claims: vec![ResourceClaim {
                resource: crate::LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            }],
            allocations: Vec::new(),
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: stage.clone(),
            kind: WorkKind::Serialization,
            domain: WorkDomain::Cpu,
            implementation: implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::Workers,
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: crate::StorageUseKind::StagedOutput,
                    },
                    amount: 3,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: crate::LeaseResource::IoBuffer(IoBufferKind::Serialization),
                    amount: 3,
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: vec![AllocationUse {
                allocation: writer_allocation.clone(),
                lifetime: ClaimLifetime::Work,
            }],
            fences: BTreeSet::new(),
            quiescence_after: BTreeSet::new(),
        },
        WorkNode {
            id: commit.clone(),
            kind: WorkKind::Publication,
            domain: WorkDomain::Io,
            implementation,
            dependencies: BTreeSet::from([WorkDependency::Work(stage.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::MeasurementSetLock { measurement_set },
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: crate::StorageUseKind::StagedOutput,
                    },
                    amount: 3,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: crate::StorageUseKind::FinalOutput,
                    },
                    amount: 3,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::Publication),
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: publication_allocation.clone(),
                lifetime: publication_lifetime,
            }],
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
            quiescence_after: BTreeSet::new(),
        },
    ]);
    let mut alternative = dag.resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "transaction-publication-slot".to_string(),
        hard_bytes: 1,
        preferred_bytes: 1,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "transaction-product-writer-slot".to_string(),
        hard_bytes: 3,
        preferred_bytes: 3,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.storage.push(StorageDemand {
        demand_id: "transaction-output".to_string(),
        domain: StorageDomainId::new("transaction-output"),
        temporary_bytes: 0,
        staged_output_bytes: 3,
        final_output_bytes: 3,
        persistent_cache_bytes: 0,
        read_rate: CountDemand::zero(),
        write_rate: CountDemand::zero(),
        operations_rate: CountDemand::zero(),
        queue_slots: CountDemand::zero(),
    });
    alternative.demand.rates.push(RateDemand {
        demand_id: "transaction-io-rate".to_string(),
        resource: RateResourceId::new("transaction-io-rate"),
        amount: CountDemand::new(1, 1),
    });
    alternative.demand.queues.push(QueueDemand {
        demand_id: "transaction-io-queue".to_string(),
        resource: QueueResourceId::new("transaction-io-queue"),
        slots: CountDemand::new(1, 1),
    });
    alternative.demand.locks = CountDemand::new(1, 1);
    alternative.demand.io_buffers.publication_bytes = 1;
    alternative.demand.io_buffers.serialization_bytes = 3;
    let mut logical_allocations = dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    logical_allocations.push(LogicalAllocation {
        id: publication_allocation,
        bytes: 1,
        purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::Publication),
        compatibility: compatibility.clone(),
        physical_slot: publication_slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: commit.clone(),
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Io)),
                WorkDependency::Fence(FenceId::new(commit.clone(), FenceKind::Publication)),
            ]),
        },
    });
    logical_allocations.push(LogicalAllocation {
        id: writer_allocation.clone(),
        bytes: 3,
        purpose: AllocationPurpose::IoBuffer(IoBufferKind::Serialization),
        compatibility: compatibility.clone(),
        physical_slot: writer_slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: stage.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(stage.clone())]),
        },
    });
    let mut physical_slots = dag.physical_slots().values().cloned().collect::<Vec<_>>();
    physical_slots.push(PhysicalSlot {
        id: publication_slot,
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "transaction-publication-slot".to_string(),
        },
        capacity_bytes: 1,
        compatibility: compatibility.clone(),
    });
    physical_slots.push(PhysicalSlot {
        id: writer_slot,
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "transaction-product-writer-slot".to_string(),
        },
        capacity_bytes: 3,
        compatibility,
    });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: dag.required_resource_capabilities().clone(),
        resource_alternative: alternative,
        nodes,
        logical_allocations,
        physical_slots,
        initial_knobs: dag.initial_knobs().clone(),
        adaptations: dag.adaptations().values().cloned().collect(),
    })
    .expect("valid transaction-bound test plan");
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let mut stage = crate::StagePrediction::new(node.id.clone(), 1);
            let mut buffers = BTreeMap::<crate::IoBufferKind, u64>::new();
            for claim in &node.claims {
                if let crate::LeaseResource::IoBuffer(kind) = claim.resource {
                    let bytes = buffers.entry(kind).or_default();
                    *bytes = bytes
                        .checked_add(claim.amount)
                        .expect("validated I/O-buffer claims fit u64");
                }
            }
            if !buffers.is_empty() {
                stage = stage.with_io(
                    buffers
                        .into_iter()
                        .map(|(kind, bytes)| crate::IoPrediction::new(kind, bytes, 1))
                        .collect(),
                );
            }
            stage
        })
        .collect();
    let prediction = crate::PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count"),
        crate::PredictionConfidence::new(1_000_000).expect("confidence"),
        Vec::new(),
        stages,
    )
    .expect("complete test prediction");
    let layouts = crate::PublicationLayoutLedger::empty();
    let catalog = implementation_catalog(problem, &dag);
    PhysicalWorkBinding::new_reconstruction(
        catalog,
        dag,
        prediction,
        artifacts,
        ObservationTransactionWork::new_reconstruction(initial, reconciliation, commit),
        layouts,
    )
    .expect("bound physical work")
}

fn implementation_catalog(
    problem: &casa_imaging_model::CompiledProblem,
    dag: &ExecutionDag,
) -> ImplementationContractCatalog {
    let registry = ContractOnlyRegistry::new(
        ImplementationRegistryId::from_sha256([7; 32]),
        ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        ),
        [WorkImplementationId::new("cpu-reference")],
    );
    ImplementationContractCatalog::from_registry(
        &registry,
        dag.nodes().values().map(|node| node.implementation.clone()),
    )
    .expect("registry publishes every physical implementation contract")
}

#[test]
fn publication_layout_ledger_names_every_atomic_member_and_staging_event() {
    let problem = compiled_problem();
    let product_graph_id = problem.product_graph().graph_id();
    let producer = WorkNodeId::new("stage-products");
    let terminal = WorkDependency::Work(producer.clone());
    let allocation = AllocationId::new("product-writer");
    let bounds =
        crate::PublicationResourceBounds::new(128, 96, 32, 0).expect("nonzero publication bounds");
    let layouts = problem
        .product_graph()
        .publication()
        .members()
        .iter()
        .enumerate()
        .map(|(index, product_node)| {
            crate::PublicationPhysicalLayout::new(
                crate::PublicationParticipant::Product {
                    graph_id: product_graph_id,
                    node_id: *product_node,
                },
                crate::ArtifactIdentity::from_sha256([u8::try_from(index + 1).unwrap(); 32]),
                crate::PhysicalLayoutId::from_sha256([u8::try_from(index + 11).unwrap(); 32]),
                crate::PublicationStaging::new(
                    producer.clone(),
                    terminal.clone(),
                    IoBufferKind::Serialization,
                    allocation.clone(),
                )
                .expect("producer-owned staging"),
                bounds,
            )
        })
        .collect::<Vec<_>>();
    let ledger = crate::PublicationLayoutLedger::new(layouts).expect("complete atomic ledger");
    assert_eq!(
        ledger
            .entries()
            .iter()
            .filter(|entry| matches!(
                entry.participant(),
                crate::PublicationParticipant::Product { .. }
            ))
            .count(),
        problem.product_graph().publication().members().len()
    );
    assert_eq!(ledger.staged_storage_bytes(), 128 * 3);
    assert_eq!(ledger.final_storage_bytes(), 96 * 3);
    assert_eq!(ledger.writer_buffer_bytes(), 32 * 3);
}

#[test]
fn publication_layout_ledger_sums_asynchronous_exposure_across_producers() {
    let problem = compiled_problem();
    let graph_id = problem.product_graph().graph_id();
    let members = problem.product_graph().publication().members();
    let layout = |index: usize, writer_bytes, mapped_bytes| {
        let writer = WorkNodeId::new(format!("writer-{index}"));
        let mapped = WorkNodeId::new(format!("mapped-{index}"));
        let release = WorkNodeId::new(format!("release-{index}"));
        crate::PublicationPhysicalLayout::new(
            crate::PublicationParticipant::Product {
                graph_id,
                node_id: members[index],
            },
            crate::ArtifactIdentity::from_sha256([u8::try_from(index + 1).unwrap(); 32]),
            crate::PhysicalLayoutId::from_sha256([u8::try_from(index + 11).unwrap(); 32]),
            crate::PublicationStaging::new(
                writer.clone(),
                WorkDependency::Work(writer),
                IoBufferKind::Serialization,
                AllocationId::new(format!("writer-allocation-{index}")),
            )
            .expect("writer staging")
            .with_mapped_page_cache(
                crate::PublicationMappedStaging::new(
                    mapped,
                    WorkDependency::Work(release),
                    AllocationId::new(format!("mapped-allocation-{index}")),
                )
                .expect("mapped exposure retained through a distinct release"),
            ),
            crate::PublicationResourceBounds::new(1, 1, writer_bytes, mapped_bytes)
                .expect("publication bounds"),
        )
    };
    let ledger = crate::PublicationLayoutLedger::new(vec![layout(0, 20, 30), layout(1, 40, 50)])
        .expect("two-producer publication ledger");

    assert_eq!(ledger.writer_buffer_bytes(), 60);
    assert_eq!(ledger.mapped_page_cache_bytes(), 80);
}

struct MalformedRejectionImplementation {
    id: WorkImplementationId,
    node: WorkNodeId,
    artifact: crate::ArtifactIdentity,
    ledger: crate::ArtifactIdentity,
    observed: Option<crate::ArtifactIdentity>,
    selected_observation_completion:
        std::sync::Mutex<Option<casa_ms::SelectedObservationCompletion>>,
}

impl crate::WorkImplementation for MalformedRejectionImplementation {
    type Error = std::io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(
        &self,
        context: crate::WorkExecutionContext<'_>,
    ) -> Result<crate::WorkMeasurements, Self::Error> {
        if context.node().kind == WorkKind::ObservationRead {
            let problem = context
                .selected_observation()
                .ok_or_else(|| std::io::Error::other("missing selected-observation authority"))?;
            let bindings = problem
                .inputs()
                .observation_snapshot()
                .sources()
                .iter()
                .map(|source| {
                    casa_ms::ObservationSourceBinding::new(
                        casa_imaging_model::ObservationSourceState::new(
                            source.identity(),
                            source.selection().rows().clone(),
                            source.generations().clone(),
                        ),
                        casa_ms::SelectedObservationContentBudget::new(4 * 1024 * 1024, 1, 4),
                    )
                })
                .collect();
            let measures_identity = problem
                .inputs()
                .reference_data()
                .iter()
                .find_map(|(kind, identity)| {
                    (*kind == casa_imaging_model::ReferenceDataKind::Measures).then_some(*identity)
                })
                .ok_or_else(|| std::io::Error::other("missing Measures identity"))?;
            let measures = casa_ms::SelectedObservationMeasures::new(
                casa_test_support::deterministic_measures_provider_for_identity(
                    measures_identity.as_bytes(),
                ),
            )
            .map_err(std::io::Error::other)?;
            let mut observation =
                casa_ms::BoundSelectedObservation::open(problem, measures, bindings)
                    .map_err(std::io::Error::other)?;
            let completion = observation
                .traverse(problem, |_| Ok::<_, std::io::Error>(()))
                .map_err(std::io::Error::other)?;
            *self
                .selected_observation_completion
                .lock()
                .expect("selected-observation completion lock") = Some(completion);
        }
        let resources = context
            .node()
            .claims
            .iter()
            .map(|claim| {
                crate::ResourceMeasurement::new(claim.resource.clone(), claim.lifetime.clone(), 0)
            })
            .collect();
        let io = context
            .stage_prediction()
            .io()
            .iter()
            .map(|prediction| crate::IoMeasurement::new(prediction.kind(), 0, 1))
            .collect();
        let artifacts = if context.node().id == self.node {
            vec![
                crate::ArtifactMeasurement::new_store_owned(
                    self.artifact,
                    self.observed,
                    crate::ArtifactDisposition::RejectedStale,
                    0,
                    None,
                ),
                crate::ArtifactMeasurement::new_store_owned(
                    self.ledger,
                    Some(crate::ArtifactIdentity::from_sha256([213; 32])),
                    crate::ArtifactDisposition::Loaded,
                    0,
                    None,
                ),
            ]
        } else {
            Vec::new()
        };
        Ok(crate::WorkMeasurements::new(resources, io, artifacts))
    }

    fn failure_measurements<'error>(
        &'error self,
        _error: &'error Self::Error,
    ) -> Option<&'error crate::WorkMeasurements> {
        None
    }

    fn wait_for_fence(
        &self,
        _context: crate::WorkExecutionContext<'_>,
        _fence: FenceKind,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn complete_observation_read(
        &self,
        completion: crate::ObservationReadCompletionContext,
    ) -> Result<crate::AttemptBoundObservationCompletion, Self::Error> {
        let owner_completion = self
            .selected_observation_completion
            .lock()
            .expect("selected-observation completion lock")
            .take()
            .ok_or_else(|| std::io::Error::other("ObservationRead produced no completion"))?;
        completion
            .bind(owner_completion)
            .map_err(std::io::Error::other)
    }

    fn publish(&self, _context: crate::WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct MalformedRejectionRegistry {
    id: ImplementationRegistryId,
    metadata: ImplementationContractMetadata,
    implementation: MalformedRejectionImplementation,
}

struct ContractOnlyRegistry {
    id: ImplementationRegistryId,
    metadata: ImplementationContractMetadata,
    implementations: BTreeMap<WorkImplementationId, MalformedRejectionImplementation>,
}

impl ContractOnlyRegistry {
    fn new(
        id: ImplementationRegistryId,
        metadata: ImplementationContractMetadata,
        implementation_ids: impl IntoIterator<Item = WorkImplementationId>,
    ) -> Self {
        let implementations = implementation_ids
            .into_iter()
            .map(|implementation_id| {
                (
                    implementation_id.clone(),
                    MalformedRejectionImplementation {
                        id: implementation_id,
                        node: WorkNodeId::new("contract-only"),
                        artifact: crate::ArtifactIdentity::from_sha256([0; 32]),
                        ledger: crate::ArtifactIdentity::from_sha256([1; 32]),
                        observed: None,
                        selected_observation_completion: std::sync::Mutex::new(None),
                    },
                )
            })
            .collect();
        Self {
            id,
            metadata,
            implementations,
        }
    }
}

impl crate::ImplementationRegistry for ContractOnlyRegistry {
    type Implementation = MalformedRejectionImplementation;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        self.implementations.get(id)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        self.implementations
            .contains_key(id)
            .then(|| self.metadata.clone())
    }
}

impl crate::ImplementationRegistry for MalformedRejectionRegistry {
    type Implementation = MalformedRejectionImplementation;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        (id == &self.implementation.id).then_some(&self.implementation)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        (id == &self.implementation.id).then(|| self.metadata.clone())
    }
}

#[test]
fn malformed_store_owned_rejection_is_rejected_without_partial_receipt_mutation() {
    let problem = compiled_problem_with_reference_data(vec![(
        casa_imaging_model::ReferenceDataKind::Measures,
        identity(90),
    )]);
    let node_id = WorkNodeId::new("malformed-rejection-cache");
    let mut cache_node = cpu_node(node_id.as_str(), BTreeSet::new());
    cache_node.kind = WorkKind::Cache;
    cache_node.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 1,
        lifetime: ClaimLifetime::Work,
    });
    let mut specification = plan_spec(vec![cache_node]);
    specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 1,
        preferred_resident_bytes: 1,
    };
    specification.initial_knobs.cache_retention_bytes = 1;
    let dag = ExecutionDag::new(specification).expect("malformed rejection physical work");
    let artifact = crate::ArtifactIdentity::from_sha256([210; 32]);
    let ledger = crate::ArtifactIdentity::from_sha256([211; 32]);
    let cache = crate::CacheIdentity::from_sha256([212; 32]);
    let physical = physical_work_binding_with_problem(
        &problem,
        dag,
        vec![
            crate::PlannedArtifact::new(
                artifact,
                node_id.clone(),
                crate::ArtifactRole::Cache,
                Some(cache),
            ),
            crate::PlannedArtifact::new(ledger, node_id.clone(), crate::ArtifactRole::Input, None),
        ],
    );
    let cost_model = PlannerCostModelProfileId::from_sha256([8; 32]);
    let receipts_directory = tempfile::tempdir().expect("malformed rejection receipts");
    let receipts = crate::ExecutionReceiptStore::new(
        receipts_directory.path(),
        crate::ReceiptRetention::new(4, 1_000_000).expect("receipt retention"),
    )
    .expect("malformed rejection receipt store");
    let plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(
            ImplementationRegistryId::from_sha256([7; 32]),
            ResourcePolicy::Exclusive,
            PlannerCostModelProfileRecord::initial(cost_model),
        ),
        &receipts,
        |_, _| Ok::<_, std::convert::Infallible>(physical),
    )
    .expect("malformed rejection plan");
    let current = crate::RunBindings::new(
        problem.inputs().clone(),
        plan.resource_policy(),
        plan.planner_cost_model_profile_id(),
    );
    let malformed = [
        None,
        Some(crate::ArtifactIdentity::from_sha256([214; 32])),
        Some(
            crate::prepared_artifact::PreparedArtifactRejection::Missing.evidence_identity(ledger),
        ),
    ];

    for (index, observed) in malformed.into_iter().enumerate() {
        let registry = MalformedRejectionRegistry {
            id: ImplementationRegistryId::from_sha256([7; 32]),
            metadata: ImplementationContractMetadata::new(
                problem.problem_id(),
                problem.numerics_id(),
                problem.required_capabilities().clone(),
            ),
            implementation: MalformedRejectionImplementation {
                id: WorkImplementationId::new("cpu-reference"),
                node: node_id.clone(),
                artifact,
                ledger,
                observed,
                selected_observation_completion: std::sync::Mutex::new(None),
            },
        };
        let attempt_byte = 153 + u8::try_from(index).expect("small malformed-evidence index");
        let build_byte = 157 + u8::try_from(index).expect("small malformed-evidence index");
        let attempt = crate::ExecutionAttemptId::from_sha256([attempt_byte; 32]);
        let provenance =
            execution_provenance(attempt, crate::BuildIdentity::from_sha256([build_byte; 32]));
        let authority = io_authority();
        let mut controller = crate::RunToCompletion;
        let executable =
            casa_imaging_reconstruction::ExecutableModelProblem::from_compiled(problem.clone())
                .expect("direct executable problem");
        let error = crate::run(
            &executable,
            &plan,
            &current,
            &registry,
            &authority,
            &mut controller,
            receipts.bind(provenance),
        )
        .expect_err("malformed store-owned rejection identity must fail closed");
        assert!(
            matches!(
                &error,
                crate::RunError::Evidence(crate::ExecutionEvidenceError::ArtifactDispositionMismatch {
                    artifact: rejected,
                    ..
                }) if *rejected == artifact
            ),
            "unexpected malformed-rejection failure: {error:?}"
        );

        let receipt = receipts.open(attempt).expect("malformed rejection receipt");
        assert_eq!(receipt.status(), crate::ReceiptStatus::Failed);
        assert_eq!(
            receipt.failure_kind(),
            Some(crate::ReceiptFailureKind::EvidenceContract)
        );
        assert!(receipt.stage_actual_elapsed_nanos(&node_id).is_some());
        assert_eq!(receipt.artifact_actual_bytes(artifact), None);
        assert_eq!(receipt.artifact_actual_bytes(ledger), None);
        assert_eq!(
            receipt.actual_resource_peak(
                &node_id,
                &crate::LeaseResource::Workers,
                &ClaimLifetime::Work,
            ),
            None,
            "invalid rejection identity must not be checkpointed partially"
        );
    }
}

fn inactive_release_predecessor_plan(fenced_predecessor: bool) -> (ExecutionDag, WorkNodeId) {
    let active_prepare_id = WorkNodeId::new("0-prepare-active");
    let inactive_prepare_id = WorkNodeId::new("z-prepare-inactive");
    let inactive_release_id = WorkNodeId::new("m-release-inactive");
    let active_release_id = WorkNodeId::new("a-release-active");
    let active_allocation = AllocationId::new("active-pages");
    let inactive_allocation = AllocationId::new("inactive-pages");
    let work_claim = |kind| ResourceClaim {
        resource: crate::LeaseResource::IoBuffer(kind),
        amount: 100,
        lifetime: ClaimLifetime::Work,
    };
    let work_use = |allocation| AllocationUse {
        allocation,
        lifetime: ClaimLifetime::Work,
    };
    let mut active_prepare = cpu_node(active_prepare_id.as_str(), BTreeSet::new());
    active_prepare.kind = WorkKind::Cache;
    active_prepare.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    active_prepare
        .claims
        .push(work_claim(crate::IoBufferKind::MappedPageCache));
    active_prepare
        .allocations
        .push(work_use(active_allocation.clone()));
    let mut inactive_prepare = cpu_node(inactive_prepare_id.as_str(), BTreeSet::new());
    inactive_prepare.kind = WorkKind::Cache;
    inactive_prepare.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    inactive_prepare
        .claims
        .push(work_claim(crate::IoBufferKind::MappedPageCache));
    inactive_prepare
        .allocations
        .push(work_use(inactive_allocation.clone()));
    let mut inactive_release = cpu_node(
        inactive_release_id.as_str(),
        BTreeSet::from([WorkDependency::Work(inactive_prepare_id.clone())]),
    );
    inactive_release.kind = WorkKind::Release;
    if fenced_predecessor {
        inactive_release.domain = WorkDomain::Io;
        inactive_release.claims = vec![
            ResourceClaim {
                resource: crate::LeaseResource::Rate {
                    demand_id: "io-rate".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            },
            ResourceClaim {
                resource: crate::LeaseResource::Queue {
                    demand_id: "io-queue".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            },
            ResourceClaim {
                resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::MappedPageCache),
                amount: 100,
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            },
        ];
        inactive_release.allocations = vec![AllocationUse {
            allocation: inactive_allocation.clone(),
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        }];
        inactive_release.fences = BTreeSet::from([FenceKind::Io]);
    } else {
        inactive_release
            .claims
            .push(work_claim(crate::IoBufferKind::MappedPageCache));
        inactive_release
            .allocations
            .push(work_use(inactive_allocation.clone()));
    }
    let inactive_dependency = if fenced_predecessor {
        WorkDependency::Fence(FenceId::new(inactive_release_id.clone(), FenceKind::Io))
    } else {
        WorkDependency::Work(inactive_release_id.clone())
    };
    let mut active_release = cpu_node(
        active_release_id.as_str(),
        BTreeSet::from([
            WorkDependency::Work(active_prepare_id.clone()),
            inactive_dependency,
        ]),
    );
    active_release.kind = WorkKind::Release;
    active_release
        .claims
        .push(work_claim(crate::IoBufferKind::MappedPageCache));
    active_release
        .allocations
        .push(work_use(active_allocation.clone()));
    let mut specification = plan_spec(vec![
        active_prepare,
        inactive_prepare,
        inactive_release,
        active_release,
    ]);
    specification.resource_alternative.demand.memory = ["active-slot", "inactive-slot"]
        .map(|allocation_id| MemoryDemand {
            allocation_id: allocation_id.to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![CapacityViewId::new("host-memory")],
        })
        .to_vec();
    specification
        .resource_alternative
        .demand
        .io_buffers
        .mapped_page_cache_bytes = 100;
    specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 100,
        preferred_resident_bytes: 100,
    };
    specification.initial_knobs.cache_retention_bytes = 100;
    if fenced_predecessor {
        specification.resource_alternative.demand.rates = vec![RateDemand {
            demand_id: "io-rate".to_string(),
            resource: RateResourceId::new("io-rate"),
            amount: CountDemand::new(1, 1),
        }];
        specification.resource_alternative.demand.queues = vec![QueueDemand {
            demand_id: "io-queue".to_string(),
            resource: QueueResourceId::new("io-queue"),
            slots: CountDemand::new(1, 1),
        }];
    }
    let compatibility = |layout| SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(layout),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let active_compatibility = compatibility("active-pages");
    let inactive_compatibility = compatibility("inactive-pages");
    let inactive_release_after = if fenced_predecessor {
        WorkDependency::Fence(FenceId::new(inactive_release_id, FenceKind::Io))
    } else {
        WorkDependency::Work(inactive_release_id)
    };
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: active_allocation,
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::MappedPageCache),
            compatibility: active_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("active-slot"),
            lifetime: AllocationLifetime {
                acquire_at: active_prepare_id,
                release_after: BTreeSet::from([WorkDependency::Work(active_release_id.clone())]),
            },
        },
        LogicalAllocation {
            id: inactive_allocation,
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::MappedPageCache),
            compatibility: inactive_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("inactive-slot"),
            lifetime: AllocationLifetime {
                acquire_at: inactive_prepare_id,
                release_after: BTreeSet::from([inactive_release_after]),
            },
        },
    ];
    specification.physical_slots = vec![
        PhysicalSlot {
            id: PhysicalSlotId::new("active-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "active-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: active_compatibility,
        },
        PhysicalSlot {
            id: PhysicalSlotId::new("inactive-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "inactive-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: inactive_compatibility,
        },
    ];
    (
        ExecutionDag::new(specification).expect("valid cleanup projection plan"),
        active_release_id,
    )
}

#[test]
fn execution_plan_rejects_a_dependency_cycle() {
    let first = cpu_node(
        "first",
        BTreeSet::from([WorkDependency::Work(WorkNodeId::new("second"))]),
    );
    let second = cpu_node(
        "second",
        BTreeSet::from([WorkDependency::Work(WorkNodeId::new("first"))]),
    );

    let error = ExecutionDag::new(plan_spec(vec![first, second]))
        .expect_err("cyclic work is not an executable plan");

    assert!(matches!(error, ExecutionError::InvalidPlan(message) if message.contains("cycle")));
}

#[test]
fn physical_work_identity_is_canonical_and_changes_with_work() {
    let first = cpu_node("first", BTreeSet::new());
    let second = cpu_node("second", BTreeSet::new());
    let rate = |id: &str| RateDemand {
        demand_id: id.to_string(),
        resource: RateResourceId::new(format!("{id}-resource")),
        amount: CountDemand::new(1, 1),
    };
    let mut canonical_specification = plan_spec(vec![first.clone(), second.clone()]);
    canonical_specification.resource_alternative.demand.rates =
        vec![rate("a-rate"), rate("z-rate")];
    let canonical = ExecutionDag::new(canonical_specification).expect("canonical physical work");
    let mut reordered_specification = plan_spec(vec![second, first]);
    reordered_specification.resource_alternative.demand.rates =
        vec![rate("z-rate"), rate("a-rate")];
    let reordered = ExecutionDag::new(reordered_specification).expect("reordered physical work");
    assert_eq!(canonical.physical_work_id(), reordered.physical_work_id());
    assert_eq!(canonical, reordered);

    let mut changed = cpu_node("first", BTreeSet::new());
    changed.kind = WorkKind::DataCensus;
    let changed = ExecutionDag::new(plan_spec(vec![changed])).expect("changed physical work");
    assert_ne!(canonical.physical_work_id(), changed.physical_work_id());
}

#[test]
fn explicit_work_kinds_cannot_hide_their_resource_contracts() {
    for kind in [
        WorkKind::Cache,
        WorkKind::FftPlanning,
        WorkKind::Jit,
        WorkKind::Transfer,
        WorkKind::Spill,
        WorkKind::Prefetch,
        WorkKind::Io,
        WorkKind::Writeback,
        WorkKind::Publication,
        WorkKind::Release,
        WorkKind::Synchronization,
    ] {
        let mut node = cpu_node("work", BTreeSet::new());
        node.kind = kind;
        assert!(
            ExecutionDag::new(plan_spec(vec![node])).is_err(),
            "{kind:?} must declare its typed domain and resources"
        );
    }
}

#[test]
fn execution_plan_owns_the_bound_physical_work_dag() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let bindings = PlanningBindings::new(
        ImplementationRegistryId::from_sha256([7; 32]),
        ResourcePolicy::Exclusive,
        PlannerCostModelProfileRecord::initial(PlannerCostModelProfileId::from_sha256([8; 32])),
    );

    let physical = physical_work_binding(dag);
    let expected_dag = physical.execution_dag().clone();
    let execution_plan = plan(&problem, bindings, |_, _| {
        Ok::<_, std::convert::Infallible>(physical)
    })
    .expect("physical planning succeeds");

    assert_eq!(execution_plan.execution_dag(), &expected_dag);
    assert_eq!(
        execution_plan.physical_work_id(),
        expected_dag.physical_work_id()
    );
}

#[test]
fn execution_plan_owns_the_resource_policy_selected_during_planning() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(
            ImplementationRegistryId::from_sha256([7; 32]),
            ResourcePolicy::Balanced,
            PlannerCostModelProfileRecord::initial(PlannerCostModelProfileId::from_sha256([8; 32])),
        ),
        |_, _| Ok::<_, std::convert::Infallible>(physical_work_binding(dag)),
    )
    .expect("outer planning succeeds");

    assert_eq!(execution_plan.resource_policy(), &ResourcePolicy::Balanced);
    ExecutionScheduler::start(
        execution_plan.execution_dag(),
        execution_plan.resource_policy(),
        &io_authority(),
        Some(execution_plan.observation_transaction().work().commit()),
    )
    .expect("scheduler admits the plan under its sealed resource policy");
}

#[test]
fn observation_completion_guard_retains_measurement_set_permit_after_fence() {
    let core = ExecutionDag::new(plan_spec(vec![cpu_node("core", BTreeSet::new())]))
        .expect("valid core work");
    let physical = physical_work_binding(core);
    let dag = physical.execution_dag();
    let authority = io_authority();
    let mut scheduler = ExecutionScheduler::start(
        dag,
        &ResourcePolicy::Exclusive,
        &authority,
        Some(physical.observation_transaction().commit()),
    )
    .expect("transaction scheduler admits");

    let SchedulerAction::Work(initial) = scheduler.next_action().expect("initial check dispatch")
    else {
        panic!("initial transaction check must dispatch");
    };
    assert_eq!(initial.node().id, WorkNodeId::new("transaction-check"));
    scheduler
        .finish_work(initial.node().id.clone(), WorkResult::Succeeded)
        .expect("initial check completes");

    let SchedulerAction::Work(read) = scheduler.next_action().expect("observation read dispatch")
    else {
        panic!("observation read must dispatch");
    };
    let read_id = WorkNodeId::new("transaction-read");
    assert_eq!(read.node().id, read_id);
    scheduler
        .finish_work(read_id.clone(), WorkResult::Succeeded)
        .expect("observation read launches its fence");
    scheduler
        .complete_fence(FenceId::new(read_id.clone(), FenceKind::Io))
        .expect("observation fence settles");

    assert_eq!(
        scheduler
            .observation_completion_permits
            .get(&read_id)
            .map(Vec::len),
        Some(1),
        "fence settlement must transfer rather than release the exact MS permit"
    );
    let permits = scheduler.take_observation_completion_permits(&read_id);
    assert_eq!(permits.permits.len(), 1);
    assert!(scheduler.observation_completion_permits.is_empty());
    permits
        .release()
        .expect("terminal completion releases the retained MS permit");
}

#[test]
fn planning_seals_the_first_resource_authority_feasible_candidate() {
    let problem = compiled_problem();
    let registry = ContractOnlyRegistry::new(
        ImplementationRegistryId::from_sha256([7; 32]),
        ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        ),
        [WorkImplementationId::new("cpu-reference")],
    );
    let mut infeasible = plan_spec(vec![cpu_node("work", BTreeSet::new())]);
    infeasible.resource_alternative.id = AlternativeId::new("parallel");
    infeasible.resource_alternative.demand.workers = CountDemand::new(3, 3);
    infeasible.resource_alternative.scaling.minimum_workers = 3;
    infeasible.resource_alternative.scaling.maximum_workers = 3;
    infeasible.initial_knobs.workers = 3;
    let mut feasible = plan_spec(vec![cpu_node("work", BTreeSet::new())]);
    feasible.resource_alternative.id = AlternativeId::new("serial");
    let candidates = vec![
        physical_work_binding(ExecutionDag::new(infeasible).expect("parallel candidate")),
        physical_work_binding(ExecutionDag::new(feasible).expect("serial candidate")),
    ];

    let plan = authority_plan(
        &problem,
        PlanningBindings::new(
            ImplementationRegistryId::from_sha256([7; 32]),
            ResourcePolicy::Exclusive,
            PlannerCostModelProfileRecord::initial(PlannerCostModelProfileId::from_sha256([8; 32])),
        ),
        &io_authority(),
        &registry,
        &empty_receipt_store(),
        |_, _| Ok::<_, std::convert::Infallible>(candidates),
    )
    .expect("serial candidate is feasible");

    assert_eq!(
        plan.execution_dag().resource_alternative().id,
        AlternativeId::new("serial")
    );
}

#[test]
fn planning_fails_before_sealing_when_no_candidate_is_feasible() {
    let problem = compiled_problem();
    let registry = ContractOnlyRegistry::new(
        ImplementationRegistryId::from_sha256([7; 32]),
        ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        ),
        [WorkImplementationId::new("cpu-reference")],
    );
    let mut infeasible = plan_spec(vec![cpu_node("work", BTreeSet::new())]);
    infeasible.resource_alternative.id = AlternativeId::new("parallel");
    infeasible.resource_alternative.demand.workers = CountDemand::new(3, 3);
    infeasible.resource_alternative.scaling.minimum_workers = 3;
    infeasible.resource_alternative.scaling.maximum_workers = 3;
    infeasible.initial_knobs.workers = 3;
    let candidate =
        physical_work_binding(ExecutionDag::new(infeasible).expect("parallel candidate"));

    let error = authority_plan(
        &problem,
        PlanningBindings::new(
            ImplementationRegistryId::from_sha256([7; 32]),
            ResourcePolicy::Exclusive,
            PlannerCostModelProfileRecord::initial(PlannerCostModelProfileId::from_sha256([8; 32])),
        ),
        &cpu_authority(),
        &registry,
        &empty_receipt_store(),
        |_, _| Ok::<_, std::convert::Infallible>(vec![candidate]),
    )
    .expect_err("no candidate fits the authority inventory");

    assert!(matches!(error, crate::PlanError::Resource(_)));
}

#[test]
fn recorded_quantitative_failures_constrain_planning_through_resource_authority() {
    let problem = compiled_problem();
    let registry = ContractOnlyRegistry::new(
        ImplementationRegistryId::from_sha256([7; 32]),
        ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        ),
        [WorkImplementationId::new("cpu-reference")],
    );
    let mut constrained_spec = plan_spec(vec![cpu_node("work", BTreeSet::new())]);
    constrained_spec.resource_alternative.demand.workers = CountDemand::new(1, 1);
    constrained_spec
        .resource_alternative
        .scaling
        .minimum_workers = 1;
    constrained_spec
        .resource_alternative
        .scaling
        .maximum_workers = 1;
    constrained_spec.initial_knobs.workers = 1;
    let sealed_dag = ExecutionDag::new(constrained_spec).expect("valid constrained physical work");
    let alternative_id = sealed_dag.resource_alternative().id.clone();
    let sealed = bound_plan_with_authority(sealed_dag.clone(), &io_authority_with_workers(4));

    let directory = tempfile::tempdir().expect("receipt directory");
    let store = crate::ExecutionReceiptStore::new(
        directory.path(),
        crate::ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    for (attempt, build, status, failure) in [
        (
            91_u8,
            92_u8,
            crate::ReceiptStatus::Failed,
            Some(crate::receipt::ReceiptFailure::infeasible(
                &crate::ResourceError::Infeasible {
                    resource: "workers".to_string(),
                    required: 3,
                    available: 2,
                },
            )),
        ),
        (
            93,
            94,
            crate::ReceiptStatus::Aborted,
            Some(crate::receipt::ReceiptFailure::infeasible(
                &crate::ResourceError::NoCapableAlternative,
            )),
        ),
        (95, 96, crate::ReceiptStatus::Cancelled, None),
        (
            97,
            98,
            crate::ReceiptStatus::Failed,
            Some(crate::receipt::ReceiptFailure::new(
                crate::ReceiptFailureKind::Interrupted,
                None,
                Some("synthetic terminal failure".to_string()),
            )),
        ),
    ] {
        let mut recorder = store
            .begin(
                execution_provenance(
                    crate::ExecutionAttemptId::from_sha256([attempt; 32]),
                    crate::BuildIdentity::from_sha256([build; 32]),
                ),
                &problem,
                &sealed,
            )
            .expect("begin receipt");
        recorder
            .finish(status, failure)
            .expect("finish terminal receipt");
    }

    // Mirror the shared fixture's authority selection for this DAG shape.
    let demand = &sealed.execution_dag().resource_alternative().demand;
    let uses_unified_domain = sealed
        .execution_dag()
        .physical_slots()
        .values()
        .any(|slot| slot.compatibility.memory_domain.as_str() == "unified-memory");
    let authority = if !demand.accelerators.is_empty() || uses_unified_domain {
        unified_authority()
    } else if !demand.rates.is_empty() || !demand.queues.is_empty() {
        io_authority()
    } else {
        cpu_authority()
    };
    let bindings = || {
        PlanningBindings::new(
            ImplementationRegistryId::from_sha256([7; 32]),
            ResourcePolicy::Exclusive,
            PlannerCostModelProfileRecord::initial(PlannerCostModelProfileId::from_sha256([8; 32])),
        )
    };
    authority_plan(
        &problem,
        bindings(),
        &authority,
        &registry,
        &empty_receipt_store(),
        |_, _| Ok::<_, std::convert::Infallible>(vec![physical_work_binding(sealed_dag.clone())]),
    )
    .expect(
        "current Resource Authority would admit the declared demand without the receipt region",
    );

    let error = authority_plan(
        &problem,
        bindings(),
        &authority,
        &registry,
        &store,
        |_, _| Ok::<_, std::convert::Infallible>(vec![physical_work_binding(sealed_dag.clone())]),
    )
    .expect_err("recorded quantitative failure must constrain the alternative");
    let certificate = match &error {
        crate::PlanError::Resource(crate::ResourceError::NoFeasibleAlternative(certificate)) => {
            certificate
        }
        other => panic!("expected a recorded-infeasibility refusal, got {other:?}"),
    };
    assert_eq!(certificate.rejections().len(), 1);
    assert_eq!(certificate.rejections()[0].alternative(), &alternative_id);
    assert_eq!(
        certificate.rejections()[0].reason(),
        &crate::AlternativeRejectionReason::RecordedFailure {
            attempt: crate::ExecutionAttemptId::from_sha256([91; 32]),
            status: crate::ReceiptStatus::Failed,
        }
    );

    // The recorded region reopens once current availability exceeds the
    // failure's recorded availability; current Resource Authority admission
    // remains the sole feasibility decision.
    let recovered_authority = io_authority_with_workers(4);
    let recovered = authority_plan(
        &problem,
        bindings(),
        &recovered_authority,
        &registry,
        &store,
        |_, _| Ok::<_, std::convert::Infallible>(vec![physical_work_binding(sealed_dag.clone())]),
    )
    .expect("increased current capacity reopens the recorded pressure region");
    assert_eq!(
        recovered.execution_dag().resource_alternative().id,
        alternative_id
    );
}

#[test]
fn scheduler_rejects_discrete_metal_memory_instead_of_inventing_a_mac_model() {
    let host_domain = CapacityDomainId::new("host-memory");
    let device_domain = CapacityDomainId::new("device-memory");
    let host_view = CapacityViewId::new("host-memory");
    let metal_view = CapacityViewId::new("metal-memory");
    let accelerator = AcceleratorId::new("metal-0");
    let command_queue = QueueResourceId::new("metal-command-queue");
    let authority = ResourceAuthority::with_inventory(HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![
                MemoryCapacityDomain {
                    id: host_domain.clone(),
                    kind: MemoryCapacityKind::Host,
                    capacity_bytes: 1_024,
                },
                MemoryCapacityDomain {
                    id: device_domain.clone(),
                    kind: MemoryCapacityKind::DevicePrivate,
                    capacity_bytes: 1_024,
                },
            ],
            memory_views: vec![
                MemoryView {
                    id: host_view,
                    domain: host_domain.clone(),
                    kind: MemoryViewKind::Host,
                },
                MemoryView {
                    id: metal_view.clone(),
                    domain: device_domain.clone(),
                    kind: MemoryViewKind::Metal,
                },
            ],
            accelerators: vec![Accelerator {
                id: accelerator.clone(),
                kind: AcceleratorKind::Metal,
                memory_view: metal_view,
                command_queue: command_queue.clone(),
                occupancy_slots: 1,
            }],
            transfer_links: Vec::new(),
            storage_domains: Vec::new(),
            rate_resources: Vec::new(),
            queue_resources: vec![QueueResource::new(command_queue.clone(), 1)],
            logical_cpu_threads: 2,
            performance_cpu_cores: CpuClassCapacity::Known(2),
            cpu_data_working_set: CpuDataWorkingSetCapacity::Unknown,
            cache_capacity_bytes: 1_024,
            lock_capacity: 8,
            file_descriptor_capacity: 8,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(host_domain, 1_024), (device_domain, 1_024)]),
            available_cpu_threads: 2,
            storage_available_bytes: BTreeMap::new(),
            rate_available_per_second: BTreeMap::new(),
            queue_available_slots: BTreeMap::from([(command_queue, 1)]),
            accelerator_available_slots: BTreeMap::from([(accelerator.clone(), 1)]),
            cache_available_bytes: 1_024,
            available_locks: 8,
            available_file_descriptors: 8,
        },
    })
    .expect("resource layer can describe a topology the Apple scheduler rejects");
    let lifetime = ClaimLifetime::through_fence(FenceKind::Device);
    let node = WorkNode {
        id: WorkNodeId::new("metal-work"),
        kind: WorkKind::Compute,
        domain: WorkDomain::Metal {
            demand_id: "metal".to_string(),
        },
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::new(),
        claims: vec![
            ResourceClaim {
                resource: crate::LeaseResource::Accelerator {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: lifetime.clone(),
            },
            ResourceClaim {
                resource: crate::LeaseResource::AcceleratorCommandQueue {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime,
            },
        ],
        allocations: Vec::new(),
        fences: BTreeSet::from([FenceKind::Device]),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![node]);
    specification.resource_alternative.demand.accelerators = vec![AcceleratorDemand {
        demand_id: "metal".to_string(),
        accelerator,
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    }];
    let plan = ExecutionDag::new(specification).expect("valid declared Metal work");

    assert!(matches!(
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &authority, None),
        Err(ExecutionError::InvalidPlan(message)) if message.contains("unified")
    ));
}

#[test]
fn scheduler_dispatches_ready_work_deterministically_under_lease_limits() {
    let mut specification = plan_spec(vec![
        cpu_node("z-last", BTreeSet::new()),
        cpu_node("a-first", BTreeSet::new()),
    ]);
    specification.resource_alternative.demand.workers = CountDemand::new(2, 2);
    specification.resource_alternative.scaling.maximum_workers = 2;
    let dag = ExecutionDag::new(specification).expect("valid concurrent work");
    let plan = dag;
    let authority = cpu_authority();
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &authority, None)
            .expect("admitted scheduler");
    assert!(scheduler.lease_epoch().is_some());
    assert_eq!(scheduler.knobs(), &ExecutionKnobs::serial());

    let SchedulerAction::Work(first) = scheduler.next_action().expect("first dispatch") else {
        panic!("first scheduler action must dispatch work");
    };
    assert_eq!(first.node().id, WorkNodeId::new("a-first"));
    assert_eq!(first.lease_epoch(), scheduler.lease_epoch().unwrap());
    assert_eq!(first.resources().len(), 1);
    assert_eq!(
        first.resources()[0].resource(),
        &crate::LeaseResource::Workers
    );
    assert_eq!(first.resources()[0].amount(), 1);
    assert_eq!(first.resources()[0].lifetime(), &ClaimLifetime::Work);
    assert!(first.allocations().is_empty());
    assert_eq!(
        first.node().implementation,
        WorkImplementationId::new("cpu-reference")
    );

    assert_eq!(
        scheduler.next_action().expect("capacity wait"),
        SchedulerAction::Waiting {
            running_work: 1,
            pending_fences: 0,
        }
    );
    scheduler
        .finish_work(first.node().id.clone(), WorkResult::Succeeded)
        .expect("first work succeeds");

    let SchedulerAction::Work(second) = scheduler.next_action().expect("second dispatch") else {
        panic!("second scheduler action must dispatch work");
    };
    assert_eq!(second.node().id, WorkNodeId::new("z-last"));
    scheduler
        .finish_work(second.node().id.clone(), WorkResult::Succeeded)
        .expect("second work succeeds");
    assert_eq!(
        scheduler.next_action().expect("terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Succeeded)
    );
}

#[test]
fn fence_context_exposes_only_capabilities_live_for_that_fence() {
    let node = cpu_node("fenced", BTreeSet::new());
    let context = WorkExecutionContext {
        node,
        knobs: ExecutionKnobs::serial(),
        lease_epoch: 7,
        cleanup: false,
        resources: vec![
            WorkResourceCapability {
                resource: crate::LeaseResource::Workers,
                amount: 1,
                lifetime: ClaimLifetime::Work,
            },
            WorkResourceCapability {
                resource: crate::LeaseResource::Queue {
                    demand_id: "io-queue".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            },
        ],
        allocations: vec![
            WorkAllocationCapability {
                allocation: AllocationId::new("work-buffer"),
                physical_slot: PhysicalSlotId::new("work-slot"),
                capacity_bytes: 64,
                lifetime: ClaimLifetime::Work,
            },
            WorkAllocationCapability {
                allocation: AllocationId::new("io-buffer"),
                physical_slot: PhysicalSlotId::new("io-slot"),
                capacity_bytes: 128,
                lifetime: ClaimLifetime::through_fence(FenceKind::Io),
            },
        ],
    };

    let fence = context.for_fence(FenceKind::Io);

    assert_eq!(fence.resources().len(), 1);
    assert_eq!(
        fence.resources()[0].resource(),
        &crate::LeaseResource::Queue {
            demand_id: "io-queue".to_string(),
        }
    );
    assert_eq!(fence.allocations().len(), 1);
    assert_eq!(
        fence.allocations()[0].allocation(),
        &AllocationId::new("io-buffer")
    );
}

#[test]
fn unified_physical_slot_reuse_waits_for_every_declared_fence() {
    let domain = CapacityDomainId::new("unified-memory");
    let views = BTreeSet::from([
        CapacityViewId::new("host-memory"),
        CapacityViewId::new("metal-memory"),
    ]);
    let compatibility = SlotCompatibility {
        memory_domain: domain,
        views: views.clone(),
        alignment_bytes: 64,
        storage_mode: StorageMode::MetalShared,
        layout: AllocationLayout::new("f32-grid-row-major"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let lease_resource = crate::LeaseResource::Memory {
        allocation_id: "shared-slot".to_string(),
    };
    let compute_id = WorkNodeId::new("a-compute");
    let io_id = WorkNodeId::new("b-io");
    let writeback_id = WorkNodeId::new("c-writeback");
    let publication_id = WorkNodeId::new("d-publication");
    let reuse_id = WorkNodeId::new("e-reuse");
    let device_fence = FenceId::new(compute_id.clone(), FenceKind::Device);
    let io_fence = FenceId::new(io_id.clone(), FenceKind::Io);
    let writeback_io_fence = FenceId::new(writeback_id.clone(), FenceKind::Io);
    let writeback_fence = FenceId::new(writeback_id.clone(), FenceKind::Writeback);
    let publication_io_fence = FenceId::new(publication_id.clone(), FenceKind::Io);
    let publication_fence = FenceId::new(publication_id.clone(), FenceKind::Publication);
    let compute = WorkNode {
        id: compute_id.clone(),
        kind: WorkKind::Compute,
        domain: WorkDomain::Metal {
            demand_id: "metal".to_string(),
        },
        implementation: WorkImplementationId::new("cpu-reference"),
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
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("first-grid"),
            lifetime: ClaimLifetime::through_fence(FenceKind::Device),
        }],
        fences: BTreeSet::from([FenceKind::Device]),
        quiescence_after: BTreeSet::new(),
    };
    let io_claims = |lifetime: ClaimLifetime| {
        vec![
            ResourceClaim {
                resource: crate::LeaseResource::Rate {
                    demand_id: "output-rate".to_string(),
                },
                amount: 1,
                lifetime: lifetime.clone(),
            },
            ResourceClaim {
                resource: crate::LeaseResource::Queue {
                    demand_id: "output-queue".to_string(),
                },
                amount: 1,
                lifetime,
            },
        ]
    };
    let io = WorkNode {
        id: io_id.clone(),
        kind: WorkKind::Io,
        domain: WorkDomain::Io,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([WorkDependency::Fence(device_fence.clone())]),
        claims: io_claims(ClaimLifetime::through_fence(FenceKind::Io)),
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("first-grid"),
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        }],
        fences: BTreeSet::from([FenceKind::Io]),
        quiescence_after: BTreeSet::new(),
    };
    let writeback_lifetime = ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Writeback]);
    let writeback = WorkNode {
        id: writeback_id.clone(),
        kind: WorkKind::Writeback,
        domain: WorkDomain::Io,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([WorkDependency::Fence(io_fence.clone())]),
        claims: io_claims(writeback_lifetime.clone()),
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("first-grid"),
            lifetime: writeback_lifetime,
        }],
        fences: BTreeSet::from([FenceKind::Io, FenceKind::Writeback]),
        quiescence_after: BTreeSet::new(),
    };
    let publication_lifetime =
        ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Publication]);
    let publication = WorkNode {
        id: publication_id.clone(),
        kind: WorkKind::Publication,
        domain: WorkDomain::Io,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([
            WorkDependency::Fence(writeback_io_fence.clone()),
            WorkDependency::Fence(writeback_fence.clone()),
        ]),
        claims: io_claims(publication_lifetime.clone()),
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("first-grid"),
            lifetime: publication_lifetime,
        }],
        fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
        quiescence_after: BTreeSet::new(),
    };
    let reuse = WorkNode {
        id: reuse_id.clone(),
        kind: WorkKind::Compute,
        domain: WorkDomain::Cpu,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([
            WorkDependency::Fence(publication_io_fence.clone()),
            WorkDependency::Fence(publication_fence.clone()),
        ]),
        claims: vec![ResourceClaim {
            resource: crate::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }],
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("second-grid"),
            lifetime: ClaimLifetime::Work,
        }],
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![compute, io, writeback, publication, reuse]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "shared-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: views.into_iter().collect(),
    }];
    specification.resource_alternative.demand.accelerators = vec![AcceleratorDemand {
        demand_id: "metal".to_string(),
        accelerator: AcceleratorId::new("metal-0"),
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "output-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "output-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    let first_release = BTreeSet::from([
        WorkDependency::Fence(device_fence.clone()),
        WorkDependency::Fence(io_fence.clone()),
        WorkDependency::Fence(writeback_io_fence.clone()),
        WorkDependency::Fence(writeback_fence.clone()),
        WorkDependency::Fence(publication_io_fence.clone()),
        WorkDependency::Fence(publication_fence.clone()),
    ]);
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: AllocationId::new("first-grid"),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("reused-slot"),
            lifetime: AllocationLifetime {
                acquire_at: compute_id.clone(),
                release_after: first_release,
            },
        },
        LogicalAllocation {
            id: AllocationId::new("second-grid"),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("reused-slot"),
            lifetime: AllocationLifetime {
                acquire_at: reuse_id.clone(),
                release_after: BTreeSet::from([WorkDependency::Work(reuse_id)]),
            },
        },
    ];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("reused-slot"),
        lease_resource,
        capacity_bytes: 100,
        compatibility,
    }];
    let plan = ExecutionDag::new(specification).expect("valid reuse plan");
    let mut scheduler = ExecutionScheduler::start(
        &plan,
        &ResourcePolicy::Exclusive,
        &unified_authority(),
        None,
    )
    .expect("admitted reuse plan");

    for (node_id, fences) in [
        (compute_id, vec![device_fence]),
        (io_id, vec![io_fence]),
        (writeback_id, vec![writeback_io_fence, writeback_fence]),
        (
            publication_id,
            vec![publication_io_fence, publication_fence],
        ),
    ] {
        let SchedulerAction::Work(work) = scheduler.next_action().expect("pipeline dispatch")
        else {
            panic!("pipeline node must dispatch");
        };
        assert_eq!(work.node().id, node_id);
        assert_eq!(work.allocations().len(), 1);
        assert_eq!(
            work.allocations()[0].allocation(),
            &AllocationId::new("first-grid")
        );
        assert_eq!(
            work.allocations()[0].physical_slot(),
            &PhysicalSlotId::new("reused-slot")
        );
        assert_eq!(work.allocations()[0].capacity_bytes(), 100);
        let declared = scheduler
            .finish_work(node_id.clone(), WorkResult::Succeeded)
            .expect("pipeline work completes");
        assert_eq!(declared, fences.iter().cloned().collect());
        for (index, fence) in fences.into_iter().enumerate() {
            scheduler
                .complete_fence(fence)
                .expect("pipeline fence completes");
            if node_id == WorkNodeId::new("d-publication") && index == 0 {
                assert!(matches!(
                    scheduler.next_action().expect("slot remains fenced"),
                    SchedulerAction::Waiting { .. }
                ));
            }
        }
    }
    let SchedulerAction::Work(second) = scheduler.next_action().expect("slot reuse dispatch")
    else {
        panic!("slot may be reused only after every fence");
    };
    assert_eq!(second.node().id, WorkNodeId::new("e-reuse"));
    assert_eq!(
        second.allocations()[0].allocation(),
        &AllocationId::new("second-grid")
    );
    scheduler
        .finish_work(second.node().id.clone(), WorkResult::Succeeded)
        .expect("consumer completes");
    assert_eq!(
        scheduler.next_action().expect("terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Succeeded)
    );
}

#[test]
fn disjoint_io_buffer_purposes_share_one_physical_memory_charge() {
    let first_id = WorkNodeId::new("read-ahead");
    let second_id = WorkNodeId::new("writeback");
    let third_id = WorkNodeId::new("publication");
    let first_fences = BTreeSet::from([FenceKind::Io]);
    let second_fences = BTreeSet::from([FenceKind::Io, FenceKind::Writeback]);
    let third_fences = BTreeSet::from([FenceKind::Io, FenceKind::Publication]);
    let make_node = |id: WorkNodeId,
                     kind: WorkKind,
                     buffer: crate::IoBufferKind,
                     dependencies: BTreeSet<WorkDependency>,
                     fences: BTreeSet<FenceKind>| {
        let lifetime = ClaimLifetime::through_fences(fences.iter().copied());
        WorkNode {
            id: id.clone(),
            kind,
            domain: WorkDomain::Io,
            implementation: WorkImplementationId::new("cpu-reference"),
            dependencies,
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::IoBuffer(buffer),
                    amount: 600,
                    lifetime: lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Rate {
                        demand_id: "output-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Queue {
                        demand_id: "output-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: lifetime.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: AllocationId::new(format!("{}-buffer", id.as_str())),
                lifetime,
            }],
            fences,
            quiescence_after: BTreeSet::new(),
        }
    };
    let first = make_node(
        first_id.clone(),
        WorkKind::Prefetch,
        crate::IoBufferKind::SourceReadAhead,
        BTreeSet::new(),
        first_fences.clone(),
    );
    let second = make_node(
        second_id.clone(),
        WorkKind::Writeback,
        crate::IoBufferKind::Writeback,
        BTreeSet::from([WorkDependency::Fence(FenceId::new(
            first_id.clone(),
            FenceKind::Io,
        ))]),
        second_fences.clone(),
    );
    let third = make_node(
        third_id.clone(),
        WorkKind::Publication,
        crate::IoBufferKind::Publication,
        second_fences
            .iter()
            .map(|kind| WorkDependency::Fence(FenceId::new(second_id.clone(), *kind)))
            .collect(),
        third_fences.clone(),
    );
    let mut specification = plan_spec(vec![first, second, third]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "io-slot".to_string(),
        hard_bytes: 600,
        preferred_bytes: 600,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "output-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "output-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.io_buffers = IoBufferDemand {
        source_read_ahead_bytes: 600,
        writeback_bytes: 600,
        publication_bytes: 600,
        ..IoBufferDemand::zero()
    };
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("unified-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("byte-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let allocation = |id: &WorkNodeId,
                      purpose: crate::IoBufferKind,
                      fences: &BTreeSet<FenceKind>| LogicalAllocation {
        id: AllocationId::new(format!("{}-buffer", id.as_str())),
        bytes: 600,
        purpose: AllocationPurpose::IoBuffer(purpose),
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("io-slot"),
        lifetime: AllocationLifetime {
            acquire_at: id.clone(),
            release_after: fences
                .iter()
                .map(|kind| WorkDependency::Fence(FenceId::new(id.clone(), *kind)))
                .collect(),
        },
    };
    specification.logical_allocations = vec![
        allocation(
            &first_id,
            crate::IoBufferKind::SourceReadAhead,
            &first_fences,
        ),
        allocation(&second_id, crate::IoBufferKind::Writeback, &second_fences),
        allocation(&third_id, crate::IoBufferKind::Publication, &third_fences),
    ];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("io-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "io-slot".to_string(),
        },
        capacity_bytes: 600,
        compatibility,
    }];
    let plan = ExecutionDag::new(specification).expect("valid I/O-buffer reuse plan");
    let mut scheduler = ExecutionScheduler::start(
        &plan,
        &ResourcePolicy::Exclusive,
        &unified_authority(),
        None,
    )
    .expect("three logical 600-byte buffers admit as one 600-byte physical slot");

    for (expected, fences) in [
        (first_id, first_fences),
        (second_id, second_fences),
        (third_id, third_fences),
    ] {
        let SchedulerAction::Work(work) = scheduler.next_action().expect("I/O dispatch") else {
            panic!("the next buffer stage must dispatch");
        };
        assert_eq!(work.node().id, expected);
        scheduler
            .finish_work(expected.clone(), WorkResult::Succeeded)
            .expect("I/O work returns");
        for kind in fences {
            scheduler
                .complete_fence(FenceId::new(expected.clone(), kind))
                .expect("I/O fence completes before slot reuse");
        }
    }
    assert_eq!(
        scheduler.next_action().expect("terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Succeeded)
    );
}

#[test]
fn io_buffer_claims_and_logical_allocations_match_exactly() {
    let mut node = cpu_node("prepare", BTreeSet::new());
    node.kind = WorkKind::Preparation;
    node.claims.push(ResourceClaim {
        resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::Preparation),
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    node.allocations = vec![AllocationUse {
        allocation: AllocationId::new("prepare-buffer"),
        lifetime: ClaimLifetime::Work,
    }];
    let mut specification = plan_spec(vec![node]);
    specification
        .resource_alternative
        .demand
        .io_buffers
        .preparation_bytes = 100;
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "prepare-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("byte-buffer"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("prepare-buffer"),
        bytes: 100,
        purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::Preparation),
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("prepare-slot"),
        lifetime: AllocationLifetime {
            acquire_at: WorkNodeId::new("prepare"),
            release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new("prepare"))]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("prepare-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "prepare-slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];
    let canonical =
        ExecutionDag::new(specification.clone()).expect("exact buffer accounting is valid");
    let mut changed_purpose = specification.clone();
    changed_purpose.nodes[0].claims[1].resource =
        crate::LeaseResource::IoBuffer(crate::IoBufferKind::Decode);
    changed_purpose.logical_allocations[0].purpose =
        AllocationPurpose::IoBuffer(crate::IoBufferKind::Decode);
    changed_purpose
        .resource_alternative
        .demand
        .io_buffers
        .preparation_bytes = 0;
    changed_purpose
        .resource_alternative
        .demand
        .io_buffers
        .decode_bytes = 100;
    let changed =
        ExecutionDag::new(changed_purpose).expect("changed typed buffer purpose is valid");
    assert_ne!(canonical.physical_work_id(), changed.physical_work_id());
    let mut orphan_claim = specification.clone();
    orphan_claim.nodes[0].allocations.clear();
    let mut amount_mismatch = specification.clone();
    amount_mismatch.nodes[0].claims[1].amount = 99;
    let mut kind_mismatch = specification.clone();
    kind_mismatch.nodes[0].claims[1].resource =
        crate::LeaseResource::IoBuffer(crate::IoBufferKind::Decode);
    kind_mismatch
        .resource_alternative
        .demand
        .io_buffers
        .preparation_bytes = 0;
    kind_mismatch
        .resource_alternative
        .demand
        .io_buffers
        .decode_bytes = 100;
    let mut unused_ceiling = specification;
    unused_ceiling
        .resource_alternative
        .demand
        .io_buffers
        .decode_bytes = 1;

    for invalid in [orphan_claim, amount_mismatch, kind_mismatch] {
        let error = ExecutionDag::new(invalid)
            .expect_err("buffer claim kind, amount, lifetime, and use must match");
        assert!(
            matches!(error, ExecutionError::InvalidPlan(message) if message.contains("exactly match"))
        );
    }
    let error = ExecutionDag::new(unused_ceiling)
        .expect_err("nonzero buffer demand must be used by the work graph");
    assert!(matches!(error, ExecutionError::InvalidPlan(message) if message.contains("unused")));
}

#[test]
fn every_io_buffer_kind_has_exact_supported_and_unsupported_work_semantics() {
    let all_work_kinds = [
        WorkKind::DataCensus,
        WorkKind::Preparation,
        WorkKind::Cache,
        WorkKind::ConvolutionFunction,
        WorkKind::FftPlanning,
        WorkKind::Jit,
        WorkKind::Compute,
        WorkKind::Transfer,
        WorkKind::Spill,
        WorkKind::Prefetch,
        WorkKind::Io,
        WorkKind::ObservationRead,
        WorkKind::Serialization,
        WorkKind::Writeback,
        WorkKind::Publication,
        WorkKind::Release,
        WorkKind::Synchronization,
    ];
    let mappings = [
        (
            crate::IoBufferKind::SourceReadAhead,
            &[
                WorkKind::Prefetch,
                WorkKind::Cache,
                WorkKind::ObservationRead,
                WorkKind::Release,
            ][..],
        ),
        (crate::IoBufferKind::Decode, &[WorkKind::Preparation][..]),
        (
            crate::IoBufferKind::Preparation,
            &[WorkKind::Preparation][..],
        ),
        (
            crate::IoBufferKind::HostToDeviceTransfer,
            &[WorkKind::Transfer][..],
        ),
        (
            crate::IoBufferKind::DeviceToHostTransfer,
            &[WorkKind::Transfer][..],
        ),
        (
            crate::IoBufferKind::SpillRead,
            &[WorkKind::Spill, WorkKind::Prefetch][..],
        ),
        (
            crate::IoBufferKind::SpillWrite,
            &[WorkKind::Spill, WorkKind::ObservationRead][..],
        ),
        (
            crate::IoBufferKind::Serialization,
            &[WorkKind::Serialization][..],
        ),
        (
            crate::IoBufferKind::StorageManager,
            &[WorkKind::Cache, WorkKind::Io, WorkKind::Release][..],
        ),
        (crate::IoBufferKind::TiledColumnWriter, &[WorkKind::Io][..]),
        (crate::IoBufferKind::ScalarColumnWriter, &[WorkKind::Io][..]),
        (
            crate::IoBufferKind::Writeback,
            &[WorkKind::Cache, WorkKind::Writeback][..],
        ),
        (
            crate::IoBufferKind::Publication,
            &[WorkKind::Publication][..],
        ),
        (
            crate::IoBufferKind::MappedPageCache,
            &[WorkKind::Cache, WorkKind::Release][..],
        ),
    ];
    assert_eq!(
        mappings.map(|(kind, _)| kind),
        crate::IoBufferKind::ALL,
        "the semantic mapping must enumerate every typed I/O buffer exactly once"
    );

    for (io_kind, supported) in mappings {
        for work_kind in all_work_kinds {
            assert_eq!(
                io_buffer_kind_supports_work_kind(io_kind, work_kind),
                supported.contains(&work_kind),
                "{io_kind:?} support for {work_kind:?}"
            );
        }
    }
}

#[test]
fn allocation_lifetime_rejects_release_before_an_async_use_fence() {
    let domain = CapacityDomainId::new("unified-memory");
    let views = BTreeSet::from([
        CapacityViewId::new("host-memory"),
        CapacityViewId::new("metal-memory"),
    ]);
    let compatibility = SlotCompatibility {
        memory_domain: domain,
        views: views.clone(),
        alignment_bytes: 64,
        storage_mode: StorageMode::MetalShared,
        layout: AllocationLayout::new("f32-grid-row-major"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let node_id = WorkNodeId::new("metal-use");
    let node = WorkNode {
        id: node_id.clone(),
        kind: WorkKind::Compute,
        domain: WorkDomain::Metal {
            demand_id: "metal".to_string(),
        },
        implementation: WorkImplementationId::new("cpu-reference"),
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
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("grid"),
            lifetime: ClaimLifetime::through_fence(FenceKind::Device),
        }],
        fences: BTreeSet::from([FenceKind::Device]),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![node]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: views.into_iter().collect(),
    }];
    specification.resource_alternative.demand.accelerators = vec![AcceleratorDemand {
        demand_id: "metal".to_string(),
        accelerator: AcceleratorId::new("metal-0"),
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    }];
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("grid"),
        bytes: 100,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("slot"),
        lifetime: AllocationLifetime {
            acquire_at: node_id.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(node_id.clone())]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];

    let mut synchronous_use = specification.clone();
    synchronous_use.nodes[0].allocations[0].lifetime = ClaimLifetime::Work;
    synchronous_use.logical_allocations[0]
        .lifetime
        .release_after = BTreeSet::from([WorkDependency::Fence(FenceId::new(
        node_id.clone(),
        FenceKind::Device,
    ))]);
    let error = ExecutionDag::new(synchronous_use)
        .expect_err("Metal allocation uses must remain live through device completion");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("exact asynchronous lifetime"))
    );

    let error = ExecutionDag::new(specification)
        .expect_err("slot release cannot precede the declared device use fence");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("released before"))
    );
}

#[test]
fn asynchronous_payload_claims_cannot_end_with_synchronous_work() {
    let node = WorkNode {
        id: WorkNodeId::new("metal-work"),
        kind: WorkKind::Compute,
        domain: WorkDomain::Metal {
            demand_id: "metal".to_string(),
        },
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::new(),
        claims: vec![
            ResourceClaim {
                resource: crate::LeaseResource::Accelerator {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::Work,
            },
            ResourceClaim {
                resource: crate::LeaseResource::AcceleratorCommandQueue {
                    demand_id: "metal".to_string(),
                },
                amount: 1,
                lifetime: ClaimLifetime::Work,
            },
        ],
        allocations: Vec::new(),
        fences: BTreeSet::from([FenceKind::Device]),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![node]);
    specification.resource_alternative.demand.accelerators = vec![AcceleratorDemand {
        demand_id: "metal".to_string(),
        accelerator: AcceleratorId::new("metal-0"),
        slots: CountDemand::new(1, 1),
        command_queue_slots: CountDemand::new(1, 1),
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("Metal payload permits must remain live through device completion");

    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("asynchronous lifetime"))
    );

    for (kind, fences) in [
        (WorkKind::Io, BTreeSet::from([FenceKind::Io])),
        (
            WorkKind::Writeback,
            BTreeSet::from([FenceKind::Io, FenceKind::Writeback]),
        ),
        (
            WorkKind::Publication,
            BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
        ),
    ] {
        let node = WorkNode {
            id: WorkNodeId::new("io-work"),
            kind,
            domain: WorkDomain::Io,
            implementation: WorkImplementationId::new("cpu-reference"),
            dependencies: BTreeSet::new(),
            claims: vec![
                ResourceClaim {
                    resource: crate::LeaseResource::Rate {
                        demand_id: "io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
                ResourceClaim {
                    resource: crate::LeaseResource::Queue {
                        demand_id: "io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::Work,
                },
            ],
            allocations: Vec::new(),
            fences,
            quiescence_after: BTreeSet::new(),
        };
        let mut specification = plan_spec(vec![node]);
        specification.resource_alternative.demand.rates = vec![RateDemand {
            demand_id: "io-rate".to_string(),
            resource: RateResourceId::new("io-rate"),
            amount: CountDemand::new(1, 1),
        }];
        specification.resource_alternative.demand.queues = vec![QueueDemand {
            demand_id: "io-queue".to_string(),
            resource: QueueResourceId::new("io-queue"),
            slots: CountDemand::new(1, 1),
        }];

        let error = ExecutionDag::new(specification)
            .expect_err("I/O payload permits must remain live through exact completion fences");
        assert!(
            matches!(error, ExecutionError::InvalidPlan(message) if message.contains("asynchronous lifetime"))
        );
    }
}

#[test]
fn mutable_allocation_use_waits_for_every_prior_async_use_fence() {
    let first_id = WorkNodeId::new("first-use");
    let second_id = WorkNodeId::new("second-use");
    let mut first = cpu_node(first_id.as_str(), BTreeSet::new());
    first.kind = WorkKind::Io;
    first.domain = WorkDomain::Io;
    first.claims = vec![
        ResourceClaim {
            resource: crate::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::Queue {
                demand_id: "io-queue".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
    ];
    first.allocations = vec![AllocationUse {
        allocation: AllocationId::new("grid"),
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    }];
    first.fences = BTreeSet::from([FenceKind::Io]);
    let mut second = cpu_node(
        second_id.as_str(),
        BTreeSet::from([WorkDependency::Work(first_id.clone())]),
    );
    second.allocations = vec![AllocationUse {
        allocation: AllocationId::new("grid"),
        lifetime: ClaimLifetime::Work,
    }];

    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("mutable-grid"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut specification = plan_spec(vec![first, second]);
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "io-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "io-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("grid"),
        bytes: 100,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("slot"),
        lifetime: AllocationLifetime {
            acquire_at: first_id,
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(WorkNodeId::new("first-use"), FenceKind::Io)),
                WorkDependency::Work(second_id),
            ]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];

    let mut read_only_initialization = specification.clone();
    read_only_initialization.logical_allocations[0]
        .compatibility
        .access = AllocationAccess::ReadOnly;
    read_only_initialization.physical_slots[0]
        .compatibility
        .access = AllocationAccess::ReadOnly;
    let mut concurrent_preserved_reads = read_only_initialization.clone();
    concurrent_preserved_reads.logical_allocations[0]
        .compatibility
        .initialization = InitializationPolicy::Preserve;
    concurrent_preserved_reads.physical_slots[0]
        .compatibility
        .initialization = InitializationPolicy::Preserve;
    let error = ExecutionDag::new(specification)
        .expect_err("mutable reuse cannot race a predecessor's async access");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("unordered mutable uses"))
    );
    let error = ExecutionDag::new(read_only_initialization)
        .expect_err("read-only use must still wait for async initialization");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("asynchronous initialization"))
    );
    ExecutionDag::new(concurrent_preserved_reads)
        .expect("preserved read-only contents may be consumed concurrently through exact fences");
}

#[test]
fn cancellation_prevents_pending_publication_from_starting() {
    let compute_id = WorkNodeId::new("compute");
    let publication_id = WorkNodeId::new("publish");
    let mut first = cpu_node(compute_id.as_str(), BTreeSet::new());
    first.allocations = vec![AllocationUse {
        allocation: AllocationId::new("cancel-grid"),
        lifetime: ClaimLifetime::Work,
    }];
    let publication_fences = BTreeSet::from([FenceKind::Io, FenceKind::Publication]);
    let publication_lifetime = ClaimLifetime::through_fences(publication_fences.iter().copied());
    let publication = WorkNode {
        id: publication_id.clone(),
        kind: WorkKind::Publication,
        domain: WorkDomain::Io,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::from([WorkDependency::Work(first.id.clone())]),
        claims: vec![
            ResourceClaim {
                resource: crate::LeaseResource::Rate {
                    demand_id: "output-rate".to_string(),
                },
                amount: 1,
                lifetime: publication_lifetime.clone(),
            },
            ResourceClaim {
                resource: crate::LeaseResource::Queue {
                    demand_id: "output-queue".to_string(),
                },
                amount: 1,
                lifetime: publication_lifetime.clone(),
            },
        ],
        allocations: vec![AllocationUse {
            allocation: AllocationId::new("cancel-grid"),
            lifetime: publication_lifetime,
        }],
        fences: publication_fences,
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![first, publication]);
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "output-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "output-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("cancel-grid"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "cancel-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("cancel-grid"),
        bytes: 100,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("cancel-slot"),
        lifetime: AllocationLifetime {
            acquire_at: compute_id,
            release_after: BTreeSet::from([
                WorkDependency::Fence(FenceId::new(publication_id.clone(), FenceKind::Io)),
                WorkDependency::Fence(FenceId::new(publication_id, FenceKind::Publication)),
            ]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("cancel-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "cancel-slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];
    let plan = ExecutionDag::new(specification).expect("valid publication plan");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &io_authority(), None)
            .expect("admitted publication plan");

    let SchedulerAction::Work(compute) = scheduler.next_action().expect("compute dispatch") else {
        panic!("compute must dispatch first");
    };
    scheduler.cancel().expect("cancellation starts draining");
    scheduler
        .finish_work(compute.node().id.clone(), WorkResult::Succeeded)
        .expect("launched compute settles");
    assert_eq!(
        scheduler.next_action().expect("cancelled terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Cancelled)
    );
    assert_eq!(scheduler.lease_epoch(), None);
}

#[test]
fn failed_work_cancels_pending_nodes_and_releases_the_lease() {
    let failed = cpu_node("a-failed", BTreeSet::new());
    let pending = cpu_node(
        "b-pending",
        BTreeSet::from([WorkDependency::Work(failed.id.clone())]),
    );
    let plan = ExecutionDag::new(plan_spec(vec![failed, pending])).expect("valid failure plan");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &cpu_authority(), None)
            .expect("admitted failure plan");
    let SchedulerAction::Work(work) = scheduler.next_action().expect("failed work dispatch") else {
        panic!("first work must dispatch");
    };

    scheduler
        .finish_work(
            work.node().id.clone(),
            WorkResult::Failed {
                message: "kernel failure".to_string(),
            },
        )
        .expect("failure enters draining state");
    assert_eq!(
        scheduler.next_action().expect("failed terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Failed {
            node: WorkNodeId::new("a-failed"),
            message: "kernel failure".to_string(),
        })
    );
    assert_eq!(scheduler.lease_epoch(), None);
}

#[test]
fn adaptation_requires_the_listed_transition_at_its_quiescence_point() {
    let first = cpu_node("major-cycle-work", BTreeSet::new());
    let mut boundary = synchronization_node(
        "major-cycle-boundary",
        BTreeSet::from([WorkDependency::Work(first.id.clone())]),
    );
    boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let second = cpu_node(
        "minor-cycle",
        BTreeSet::from([WorkDependency::Work(boundary.id.clone())]),
    );
    let mut specification = plan_spec(vec![first, boundary, second]);
    specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 2;
    specification.adaptations = vec![AdaptationTransition {
        id: AdaptationId::new("larger-batch"),
        from: ExecutionKnobs::serial(),
        to: adapted.clone(),
        at: QuiescencePoint::MajorCycle,
    }];
    let plan = ExecutionDag::new(specification).expect("valid adaptive plan");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &cpu_authority(), None)
            .expect("admitted adaptive plan");

    assert!(scheduler.adapt(&AdaptationId::new("larger-batch")).is_err());
    let SchedulerAction::Work(first) = scheduler.next_action().expect("major-cycle dispatch")
    else {
        panic!("major-cycle node must dispatch first");
    };
    scheduler
        .finish_work(first.node().id.clone(), WorkResult::Succeeded)
        .expect("major-cycle work settles");
    let SchedulerAction::Work(boundary) = scheduler.next_action().expect("boundary dispatch")
    else {
        panic!("major-cycle synchronization must dispatch second");
    };
    scheduler
        .finish_work(boundary.node().id.clone(), WorkResult::Succeeded)
        .expect("major-cycle boundary settles");
    assert!(scheduler.adapt(&AdaptationId::new("unlisted")).is_err());
    scheduler
        .adapt(&AdaptationId::new("larger-batch"))
        .expect("listed transition at exact boundary");
    let SchedulerAction::Work(second) = scheduler.next_action().expect("minor-cycle dispatch")
    else {
        panic!("minor-cycle node must dispatch");
    };
    assert_eq!(second.knobs(), &adapted);
    assert_eq!(
        scheduler.applied_adaptations(),
        &[AdaptationId::new("larger-batch")]
    );
}

#[test]
fn quiescence_marker_must_form_a_global_synchronization_cut() {
    let prior = cpu_node("prior", BTreeSet::new());
    let unrelated = cpu_node("unrelated", BTreeSet::new());
    let mut boundary = synchronization_node(
        "boundary",
        BTreeSet::from([WorkDependency::Work(prior.id.clone())]),
    );
    boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let mut specification = plan_spec(vec![prior, unrelated, boundary]);
    specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);

    let error = ExecutionDag::new(specification)
        .expect_err("a quiescence marker cannot leave unrelated work in flight");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("global execution cut"))
    );
}

#[test]
fn adaptation_transition_must_be_reachable_in_boundary_order() {
    let prior = cpu_node("prior", BTreeSet::new());
    let mut boundary = synchronization_node(
        "major-boundary",
        BTreeSet::from([WorkDependency::Work(prior.id.clone())]),
    );
    boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let mut specification = plan_spec(vec![prior, boundary]);
    specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);
    let mut after_major = ExecutionKnobs::serial();
    after_major.batch_size = 2;
    let mut impossible = after_major.clone();
    impossible.batch_size = 3;
    specification.adaptations = vec![
        AdaptationTransition {
            id: AdaptationId::new("at-major"),
            from: ExecutionKnobs::serial(),
            to: after_major.clone(),
            at: QuiescencePoint::MajorCycle,
        },
        AdaptationTransition {
            id: AdaptationId::new("back-at-run-start"),
            from: after_major,
            to: impossible,
            at: QuiescencePoint::RunBoundary,
        },
    ];

    let error = ExecutionDag::new(specification)
        .expect_err("a later configuration cannot return to an earlier boundary");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("unreachable at every declared boundary"))
    );
}

#[test]
fn adaptation_cannot_enable_undeclared_spill_work() {
    let mut adapted = ExecutionKnobs::serial();
    adapted.spill = true;
    let mut specification = plan_spec(vec![cpu_node("compute", BTreeSet::new())]);
    specification.adaptations = vec![AdaptationTransition {
        id: AdaptationId::new("invent-spill"),
        from: ExecutionKnobs::serial(),
        to: adapted,
        at: QuiescencePoint::RunBoundary,
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("adaptation cannot create spill work absent from the immutable DAG");

    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("spill work node"))
    );
}

#[test]
fn adaptation_shape_must_fit_the_selected_scaling_envelope() {
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 9;
    let mut specification = plan_spec(vec![cpu_node("compute", BTreeSet::new())]);
    specification.adaptations = vec![AdaptationTransition {
        id: AdaptationId::new("oversized-batch"),
        from: ExecutionKnobs::serial(),
        to: adapted,
        at: QuiescencePoint::RunBoundary,
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("adaptation cannot exceed the plan-sealed batch envelope");

    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("hard bounds"))
    );
}

#[test]
fn execution_knobs_must_admit_every_reachable_mandatory_claim() {
    let mut two_worker_node = cpu_node("two-worker-kernel", BTreeSet::new());
    two_worker_node.claims[0].amount = 2;
    let mut worker_specification = plan_spec(vec![two_worker_node]);
    worker_specification.resource_alternative.demand.workers = CountDemand::new(2, 2);
    worker_specification
        .resource_alternative
        .scaling
        .maximum_workers = 2;

    let error = ExecutionDag::new(worker_specification)
        .expect_err("initial knobs cannot leave a mandatory worker claim undispatchable");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("mandatory claims"))
    );

    let mut cache_node = cpu_node("cache-kernel", BTreeSet::new());
    cache_node.kind = WorkKind::Cache;
    cache_node.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 8,
        lifetime: ClaimLifetime::Work,
    });
    let mut cache_specification = plan_spec(vec![cache_node]);
    cache_specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 8,
        preferred_resident_bytes: 8,
    };

    let error = ExecutionDag::new(cache_specification)
        .expect_err("initial knobs cannot leave a mandatory cache claim undispatchable");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("mandatory claims"))
    );

    let first = cpu_node("first", BTreeSet::new());
    let mut boundary = synchronization_node(
        "major-boundary",
        BTreeSet::from([WorkDependency::Work(first.id.clone())]),
    );
    boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let mut after = cpu_node(
        "after-boundary",
        BTreeSet::from([WorkDependency::Work(boundary.id.clone())]),
    );
    after.claims[0].amount = 2;
    let mut transition_specification = plan_spec(vec![first, boundary, after]);
    transition_specification.resource_alternative.demand.workers = CountDemand::new(2, 2);
    transition_specification
        .resource_alternative
        .scaling
        .maximum_workers = 2;
    transition_specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);
    let mut initial = ExecutionKnobs::serial();
    initial.workers = 2;
    transition_specification.initial_knobs = initial.clone();
    transition_specification.adaptations = vec![AdaptationTransition {
        id: AdaptationId::new("starve-later-work"),
        from: initial,
        to: ExecutionKnobs::serial(),
        at: QuiescencePoint::MajorCycle,
    }];

    let error = ExecutionDag::new(transition_specification)
        .expect_err("an adaptation cannot starve work reachable after its boundary");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("mandatory claims"))
    );
}

#[test]
fn adaptation_feasibility_uses_the_exact_repeated_boundary_occurrence() {
    let first = cpu_node("before-first-major", BTreeSet::new());
    let mut first_boundary = synchronization_node(
        "first-major-boundary",
        BTreeSet::from([WorkDependency::Work(first.id.clone())]),
    );
    first_boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let mut expensive = cpu_node(
        "expensive-between-majors",
        BTreeSet::from([WorkDependency::Work(first_boundary.id.clone())]),
    );
    expensive.claims[0].amount = 2;
    let mut second_boundary = synchronization_node(
        "second-major-boundary",
        BTreeSet::from([WorkDependency::Work(expensive.id.clone())]),
    );
    second_boundary.quiescence_after = BTreeSet::from([QuiescencePoint::MajorCycle]);
    let cheap = cpu_node(
        "cheap-after-second-major",
        BTreeSet::from([WorkDependency::Work(second_boundary.id.clone())]),
    );
    let mut specification = plan_spec(vec![
        first,
        first_boundary,
        expensive,
        second_boundary,
        cheap,
    ]);
    specification.resource_alternative.demand.workers = CountDemand::new(2, 2);
    specification.resource_alternative.scaling.maximum_workers = 2;
    specification
        .resource_alternative
        .quiescence_points
        .insert(QuiescencePoint::MajorCycle);
    let mut initial = ExecutionKnobs::serial();
    initial.workers = 2;
    specification.initial_knobs = initial.clone();
    let mut after_first = initial.clone();
    after_first.batch_size = 2;
    let mut after_second = after_first.clone();
    after_second.workers = 1;
    specification.adaptations = vec![
        AdaptationTransition {
            id: AdaptationId::new("select-late-path"),
            from: initial,
            to: after_first.clone(),
            at: QuiescencePoint::MajorCycle,
        },
        AdaptationTransition {
            id: AdaptationId::new("shrink-after-second-major"),
            from: after_first,
            to: after_second,
            at: QuiescencePoint::MajorCycle,
        },
    ];

    ExecutionDag::new(specification).expect(
        "the one-worker target is reachable only at the second major-cycle occurrence, after the two-worker node",
    );
}

#[test]
fn release_node_can_atomically_own_multiple_external_allocations() {
    let first_prepare_id = WorkNodeId::new("prepare-first");
    let second_prepare_id = WorkNodeId::new("prepare-second");
    let release_id = WorkNodeId::new("release-both");
    let first_allocation = AllocationId::new("first-data");
    let second_allocation = AllocationId::new("second-data");
    let allocation_use = |allocation| AllocationUse {
        allocation,
        lifetime: ClaimLifetime::Work,
    };
    let mut first_prepare = cpu_node(first_prepare_id.as_str(), BTreeSet::new());
    first_prepare
        .allocations
        .push(allocation_use(first_allocation.clone()));
    let mut second_prepare = cpu_node(second_prepare_id.as_str(), BTreeSet::new());
    second_prepare
        .allocations
        .push(allocation_use(second_allocation.clone()));
    let mut release = cpu_node(
        release_id.as_str(),
        BTreeSet::from([
            WorkDependency::Work(first_prepare_id.clone()),
            WorkDependency::Work(second_prepare_id.clone()),
        ]),
    );
    release.kind = WorkKind::Release;
    release
        .allocations
        .extend([first_allocation.clone(), second_allocation.clone()].map(allocation_use));
    let mut specification = plan_spec(vec![first_prepare, second_prepare, release]);
    specification.resource_alternative.demand.memory = ["first-slot", "second-slot"]
        .map(|allocation_id| MemoryDemand {
            allocation_id: allocation_id.to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![CapacityViewId::new("host-memory")],
        })
        .to_vec();
    let compatibility = |layout| SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(layout),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let first_compatibility = compatibility("first-data");
    let second_compatibility = compatibility("second-data");
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: first_allocation,
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: first_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("first-slot"),
            lifetime: AllocationLifetime {
                acquire_at: first_prepare_id,
                release_after: BTreeSet::from([WorkDependency::Work(release_id.clone())]),
            },
        },
        LogicalAllocation {
            id: second_allocation,
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: second_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("second-slot"),
            lifetime: AllocationLifetime {
                acquire_at: second_prepare_id,
                release_after: BTreeSet::from([WorkDependency::Work(release_id)]),
            },
        },
    ];
    specification.physical_slots = vec![
        PhysicalSlot {
            id: PhysicalSlotId::new("first-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "first-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: first_compatibility,
        },
        PhysicalSlot {
            id: PhysicalSlotId::new("second-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "second-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: second_compatibility,
        },
    ];

    ExecutionDag::new(specification)
        .expect("one Release may atomically drop every allocation owned by one adapter state");
}

#[test]
fn externally_retained_io_buffer_release_is_terminal_after_every_use() {
    let prepare_id = WorkNodeId::new("prepare-mapping");
    let release_id = WorkNodeId::new("release-mapping");
    let later_id = WorkNodeId::new("later-use");
    let buffer_claim = || ResourceClaim {
        resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::MappedPageCache),
        amount: 100,
        lifetime: ClaimLifetime::Work,
    };
    let buffer_use = || AllocationUse {
        allocation: AllocationId::new("mapped-pages"),
        lifetime: ClaimLifetime::Work,
    };
    let mut prepare = cpu_node(prepare_id.as_str(), BTreeSet::new());
    prepare.kind = WorkKind::Cache;
    prepare.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    prepare.claims.push(buffer_claim());
    prepare.allocations.push(buffer_use());
    let mut release = cpu_node(
        release_id.as_str(),
        BTreeSet::from([WorkDependency::Work(prepare_id.clone())]),
    );
    release.kind = WorkKind::Release;
    release.claims.push(buffer_claim());
    release.allocations.push(buffer_use());
    let mut later = cpu_node(
        later_id.as_str(),
        BTreeSet::from([WorkDependency::Work(release_id.clone())]),
    );
    later.kind = WorkKind::Cache;
    later.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    later.claims.push(buffer_claim());
    later.allocations.push(buffer_use());
    let mut specification = plan_spec(vec![prepare, release, later]);
    specification
        .resource_alternative
        .demand
        .io_buffers
        .mapped_page_cache_bytes = 100;
    specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 100,
        preferred_resident_bytes: 100,
    };
    specification.initial_knobs.cache_retention_bytes = 100;
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "mapped-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: vec![CapacityViewId::new("host-memory")],
    }];
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("mapped-pages"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    specification.logical_allocations = vec![LogicalAllocation {
        id: AllocationId::new("mapped-pages"),
        bytes: 100,
        purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::MappedPageCache),
        compatibility: compatibility.clone(),
        physical_slot: PhysicalSlotId::new("mapped-slot"),
        lifetime: AllocationLifetime {
            acquire_at: prepare_id.clone(),
            release_after: BTreeSet::from([WorkDependency::Work(later_id.clone())]),
        },
    }];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("mapped-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "mapped-slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];

    let mut valid = specification.clone();
    valid
        .nodes
        .iter_mut()
        .find(|node| node.id == later_id)
        .expect("later use")
        .dependencies = BTreeSet::from([WorkDependency::Work(prepare_id.clone())]);
    valid
        .nodes
        .iter_mut()
        .find(|node| node.id == release_id)
        .expect("release work")
        .dependencies = BTreeSet::from([WorkDependency::Work(later_id)]);
    valid.logical_allocations[0].lifetime.release_after =
        BTreeSet::from([WorkDependency::Work(release_id.clone())]);
    let plan = ExecutionDag::new(valid).expect("valid terminal mapping release");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &cpu_authority(), None)
            .expect("admitted mapping plan");
    let SchedulerAction::Work(prepare) = scheduler.next_action().expect("mapping preparation")
    else {
        panic!("mapping preparation must dispatch first");
    };
    scheduler
        .finish_work(prepare.node().id.clone(), WorkResult::Succeeded)
        .expect("mapping preparation settles");
    scheduler.cancel().expect("cancellation enters cleanup");
    let SchedulerAction::Work(release) = scheduler.next_action().expect("release cleanup") else {
        panic!("active mapped storage must dispatch its release during cancellation");
    };
    assert_eq!(release.node().id, release_id);
    scheduler
        .finish_work(release.node().id.clone(), WorkResult::Succeeded)
        .expect("release cleanup settles");
    assert_eq!(
        scheduler.next_action().expect("cancelled terminal action"),
        SchedulerAction::Complete(SchedulerTerminal::Cancelled)
    );

    let error = ExecutionDag::new(specification)
        .expect_err("unmap work cannot precede a later use of externally retained storage");
    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("terminal release"))
    );
}

#[test]
fn cancellation_cleanup_respects_release_to_release_dependencies() {
    let mapped_prepare_id = WorkNodeId::new("prepare-mapped");
    let storage_prepare_id = WorkNodeId::new("prepare-storage");
    let first_release_id = WorkNodeId::new("z-release-first");
    let second_release_id = WorkNodeId::new("a-release-second");
    let mapped_id = AllocationId::new("mapped-pages");
    let storage_id = AllocationId::new("storage-manager");
    let buffer_claim = |kind| ResourceClaim {
        resource: crate::LeaseResource::IoBuffer(kind),
        amount: 100,
        lifetime: ClaimLifetime::Work,
    };
    let buffer_use = |allocation| AllocationUse {
        allocation,
        lifetime: ClaimLifetime::Work,
    };
    let mut mapped_prepare = cpu_node(mapped_prepare_id.as_str(), BTreeSet::new());
    mapped_prepare.kind = WorkKind::Cache;
    mapped_prepare.claims.push(ResourceClaim {
        resource: crate::LeaseResource::ResidentCache,
        amount: 100,
        lifetime: ClaimLifetime::Work,
    });
    mapped_prepare
        .claims
        .push(buffer_claim(crate::IoBufferKind::MappedPageCache));
    mapped_prepare
        .allocations
        .push(buffer_use(mapped_id.clone()));
    let mut storage_prepare = cpu_node(storage_prepare_id.as_str(), BTreeSet::new());
    storage_prepare.kind = WorkKind::Io;
    storage_prepare.domain = WorkDomain::Io;
    storage_prepare.claims = vec![
        ResourceClaim {
            resource: crate::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::Queue {
                demand_id: "io-queue".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::StorageManager),
            amount: 100,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
    ];
    storage_prepare.allocations = vec![AllocationUse {
        allocation: storage_id.clone(),
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    }];
    storage_prepare.fences = BTreeSet::from([FenceKind::Io]);
    let mut first_release = cpu_node(
        first_release_id.as_str(),
        BTreeSet::from([WorkDependency::Work(mapped_prepare_id.clone())]),
    );
    first_release.kind = WorkKind::Release;
    first_release.domain = WorkDomain::Io;
    first_release.claims = vec![
        ResourceClaim {
            resource: crate::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::Queue {
                demand_id: "io-queue".to_string(),
            },
            amount: 1,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
        ResourceClaim {
            resource: crate::LeaseResource::IoBuffer(crate::IoBufferKind::MappedPageCache),
            amount: 100,
            lifetime: ClaimLifetime::through_fence(FenceKind::Io),
        },
    ];
    first_release.allocations.push(AllocationUse {
        allocation: mapped_id.clone(),
        lifetime: ClaimLifetime::through_fence(FenceKind::Io),
    });
    first_release.fences = BTreeSet::from([FenceKind::Io]);
    let mut second_release = cpu_node(
        second_release_id.as_str(),
        BTreeSet::from([
            WorkDependency::Fence(FenceId::new(storage_prepare_id.clone(), FenceKind::Io)),
            WorkDependency::Fence(FenceId::new(first_release_id.clone(), FenceKind::Io)),
        ]),
    );
    second_release.kind = WorkKind::Release;
    second_release
        .claims
        .push(buffer_claim(crate::IoBufferKind::StorageManager));
    second_release
        .allocations
        .push(buffer_use(storage_id.clone()));
    let mut specification = plan_spec(vec![
        mapped_prepare,
        storage_prepare,
        first_release,
        second_release,
    ]);
    specification.resource_alternative.demand.memory = vec![
        MemoryDemand {
            allocation_id: "mapped-slot".to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![CapacityViewId::new("host-memory")],
        },
        MemoryDemand {
            allocation_id: "storage-slot".to_string(),
            hard_bytes: 100,
            preferred_bytes: 100,
            views: vec![CapacityViewId::new("host-memory")],
        },
    ];
    specification.resource_alternative.demand.rates = vec![RateDemand {
        demand_id: "io-rate".to_string(),
        resource: RateResourceId::new("io-rate"),
        amount: CountDemand::new(1, 1),
    }];
    specification.resource_alternative.demand.queues = vec![QueueDemand {
        demand_id: "io-queue".to_string(),
        resource: QueueResourceId::new("io-queue"),
        slots: CountDemand::new(1, 1),
    }];
    specification
        .resource_alternative
        .demand
        .io_buffers
        .mapped_page_cache_bytes = 100;
    specification
        .resource_alternative
        .demand
        .io_buffers
        .storage_manager_bytes = 100;
    specification.resource_alternative.demand.caches = CacheDemand {
        hard_resident_bytes: 100,
        preferred_resident_bytes: 100,
    };
    specification.initial_knobs.cache_retention_bytes = 100;
    let compatibility = |layout| SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new(layout),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadOnly,
    };
    let mapped_compatibility = compatibility("mapped-pages");
    let storage_compatibility = compatibility("storage-manager");
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: mapped_id.clone(),
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::MappedPageCache),
            compatibility: mapped_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("mapped-slot"),
            lifetime: AllocationLifetime {
                acquire_at: mapped_prepare_id,
                release_after: BTreeSet::from([WorkDependency::Fence(FenceId::new(
                    first_release_id.clone(),
                    FenceKind::Io,
                ))]),
            },
        },
        LogicalAllocation {
            id: storage_id.clone(),
            bytes: 100,
            purpose: AllocationPurpose::IoBuffer(crate::IoBufferKind::StorageManager),
            compatibility: storage_compatibility.clone(),
            physical_slot: PhysicalSlotId::new("storage-slot"),
            lifetime: AllocationLifetime {
                acquire_at: storage_prepare_id.clone(),
                release_after: BTreeSet::from([WorkDependency::Work(second_release_id.clone())]),
            },
        },
    ];
    specification.physical_slots = vec![
        PhysicalSlot {
            id: PhysicalSlotId::new("mapped-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "mapped-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: mapped_compatibility,
        },
        PhysicalSlot {
            id: PhysicalSlotId::new("storage-slot"),
            lease_resource: crate::LeaseResource::Memory {
                allocation_id: "storage-slot".to_string(),
            },
            capacity_bytes: 100,
            compatibility: storage_compatibility,
        },
    ];
    let plan = ExecutionDag::new(specification).expect("valid ordered cleanup plan");
    let mut scheduler =
        ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &io_authority(), None)
            .expect("admitted cleanup plan");

    for expected in [&mapped_id, &storage_id] {
        let SchedulerAction::Work(work) = scheduler.next_action().expect("preparation dispatch")
        else {
            panic!("both external allocations must be prepared before cancellation");
        };
        assert_eq!(&work.node().allocations[0].allocation, expected);
        scheduler
            .finish_work(work.node().id.clone(), WorkResult::Succeeded)
            .expect("preparation settles");
        if work.node().id == storage_prepare_id {
            scheduler
                .complete_fence(FenceId::new(storage_prepare_id.clone(), FenceKind::Io))
                .expect("storage-manager preparation fence settles");
        }
    }
    scheduler.cancel().expect("cancellation enters cleanup");

    let SchedulerAction::Work(first_release) =
        scheduler.next_action().expect("first cleanup dispatch")
    else {
        panic!("the predecessor release must dispatch first");
    };
    assert_eq!(first_release.node().id, first_release_id);
    scheduler
        .finish_work(first_release.node().id.clone(), WorkResult::Succeeded)
        .expect("first release launches its cleanup fence");
    assert!(matches!(
        scheduler
            .next_action()
            .expect("dependent cleanup waits for predecessor fence"),
        SchedulerAction::Waiting {
            pending_fences: 1,
            ..
        }
    ));
    scheduler
        .complete_fence(FenceId::new(first_release_id, FenceKind::Io))
        .expect("first release fence settles");
    let SchedulerAction::Work(second_release) =
        scheduler.next_action().expect("dependent cleanup dispatch")
    else {
        panic!("the dependent release must dispatch second");
    };
    assert_eq!(second_release.node().id, second_release_id);
}

#[test]
fn cancellation_cleanup_projects_out_inactive_release_work_and_fences() {
    for fenced_predecessor in [false, true] {
        let (plan, active_release_id) = inactive_release_predecessor_plan(fenced_predecessor);
        let mut scheduler =
            ExecutionScheduler::start(&plan, &ResourcePolicy::Exclusive, &io_authority(), None)
                .expect("admitted cleanup projection plan");
        let SchedulerAction::Work(prepare) = scheduler.next_action().expect("active preparation")
        else {
            panic!("the active external allocation must be acquired first");
        };
        assert_eq!(prepare.node().id, WorkNodeId::new("0-prepare-active"));
        scheduler
            .finish_work(prepare.node().id.clone(), WorkResult::Succeeded)
            .expect("active preparation settles");
        scheduler.cancel().expect("cancellation enters cleanup");

        let SchedulerAction::Work(release) = scheduler
            .next_action()
            .expect("inactive predecessor is projected out")
        else {
            panic!("the still-possible active release must dispatch");
        };
        assert_eq!(release.node().id, active_release_id);
        scheduler
            .finish_work(release.node().id.clone(), WorkResult::Succeeded)
            .expect("active release settles");
        assert_eq!(
            scheduler.next_action().expect("cancelled terminal action"),
            SchedulerAction::Complete(SchedulerTerminal::Cancelled)
        );
    }
}

#[test]
fn temporal_reuse_rejects_preserved_contents_from_another_allocation() {
    let domain = CapacityDomainId::new("unified-memory");
    let views = BTreeSet::from([CapacityViewId::new("host-memory")]);
    let compatibility = SlotCompatibility {
        memory_domain: domain,
        views: views.clone(),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("preserved-grid"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
    };
    let lease_resource = crate::LeaseResource::Memory {
        allocation_id: "shared-slot".to_string(),
    };
    let make_node = |id: &str, allocation: &str| WorkNode {
        id: WorkNodeId::new(id),
        kind: WorkKind::Compute,
        domain: WorkDomain::Cpu,
        implementation: WorkImplementationId::new("cpu-reference"),
        dependencies: BTreeSet::new(),
        claims: vec![ResourceClaim {
            resource: crate::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        }],
        allocations: vec![AllocationUse {
            allocation: AllocationId::new(allocation),
            lifetime: ClaimLifetime::Work,
        }],
        fences: BTreeSet::new(),
        quiescence_after: BTreeSet::new(),
    };
    let mut specification = plan_spec(vec![
        make_node("first", "first-grid"),
        make_node("second", "second-grid"),
    ]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "shared-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: views.into_iter().collect(),
    }];
    specification.logical_allocations = [("first-grid", "first"), ("second-grid", "second")]
        .into_iter()
        .map(|(allocation, node)| LogicalAllocation {
            id: AllocationId::new(allocation),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("reused-slot"),
            lifetime: AllocationLifetime {
                acquire_at: WorkNodeId::new(node),
                release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new(node))]),
            },
        })
        .collect();
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("reused-slot"),
        lease_resource,
        capacity_bytes: 100,
        compatibility,
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("preserved bytes cannot be rebound to another logical allocation");
    assert!(matches!(error, ExecutionError::InvalidPlan(message) if message.contains("Preserve")));
}

#[test]
fn temporal_reuse_requires_release_strictly_before_the_next_acquisition() {
    let views = BTreeSet::from([CapacityViewId::new("host-memory")]);
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: views.clone(),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("temporary-grid"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let first = cpu_node("first", BTreeSet::new());
    let second = cpu_node(
        "second",
        BTreeSet::from([WorkDependency::Work(first.id.clone())]),
    );
    let mut first = first;
    first.allocations = vec![AllocationUse {
        allocation: AllocationId::new("first-grid"),
        lifetime: ClaimLifetime::Work,
    }];
    let mut second = second;
    second.allocations = vec![AllocationUse {
        allocation: AllocationId::new("second-grid"),
        lifetime: ClaimLifetime::Work,
    }];
    let mut specification = plan_spec(vec![first, second]);
    specification.resource_alternative.demand.memory = vec![MemoryDemand {
        allocation_id: "shared-slot".to_string(),
        hard_bytes: 100,
        preferred_bytes: 100,
        views: views.into_iter().collect(),
    }];
    specification.logical_allocations = vec![
        LogicalAllocation {
            id: AllocationId::new("first-grid"),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("shared-slot"),
            lifetime: AllocationLifetime {
                acquire_at: WorkNodeId::new("first"),
                release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new("second"))]),
            },
        },
        LogicalAllocation {
            id: AllocationId::new("second-grid"),
            bytes: 100,
            purpose: AllocationPurpose::Data,
            compatibility: compatibility.clone(),
            physical_slot: PhysicalSlotId::new("shared-slot"),
            lifetime: AllocationLifetime {
                acquire_at: WorkNodeId::new("second"),
                release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new("second"))]),
            },
        },
    ];
    specification.physical_slots = vec![PhysicalSlot {
        id: PhysicalSlotId::new("shared-slot"),
        lease_resource: crate::LeaseResource::Memory {
            allocation_id: "shared-slot".to_string(),
        },
        capacity_bytes: 100,
        compatibility,
    }];

    let error = ExecutionDag::new(specification)
        .expect_err("a slot cannot be released by the work that is waiting to acquire it");

    assert!(
        matches!(error, ExecutionError::InvalidPlan(message) if message.contains("strictly ordered"))
    );
}

#[test]
fn receipt_store_checkpoints_atomically_rejects_corruption_and_enforces_retention() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let provenance = |attempt, build| {
        execution_provenance(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([build; 32]),
        )
    };

    let directory = tempfile::tempdir().expect("receipt directory");
    let store = crate::ExecutionReceiptStore::new(
        directory.path(),
        crate::ReceiptRetention::new(2, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    let first = provenance(61, 62);
    let mut recorder = store
        .begin(first.clone(), &problem, &plan)
        .expect("begin receipt");
    assert_eq!(
        store
            .open(first.attempt_id())
            .expect("initial receipt")
            .status(),
        crate::ReceiptStatus::Running
    );
    assert!(matches!(
        store.begin(first.clone(), &problem, &plan),
        Err(crate::ReceiptError::AttemptAlreadyExists)
    ));

    let work = WorkNodeId::new("work");
    recorder
        .work_started(&work)
        .expect("atomically checkpoint started work");
    let checkpoint = store
        .open(first.attempt_id())
        .expect("reopen intermediate checkpoint");
    assert_eq!(
        checkpoint.node_status(&work),
        Some(crate::ReceiptStatus::Running)
    );
    drop(recorder);
    assert_eq!(
        store
            .open(first.attempt_id())
            .expect("aborted receipt")
            .status(),
        crate::ReceiptStatus::Aborted
    );
    assert!(
        fs::read_dir(directory.path())
            .expect("receipt entries")
            .all(|entry| entry
                .expect("receipt entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".receipt.json"))
    );

    let path = directory
        .path()
        .join(format!("{}.receipt.json", first.attempt_id()));
    let original = fs::read(&path).expect("serialized receipt");
    let mut corrupted: serde_json::Value = serde_json::from_slice(&original).expect("receipt JSON");
    corrupted["receipt"]["revision"] = serde_json::Value::from(999_u64);
    fs::write(
        &path,
        serde_json::to_vec_pretty(&corrupted).expect("corrupt JSON"),
    )
    .expect("write corrupt receipt");
    assert!(matches!(
        store.open(first.attempt_id()),
        Err(crate::ReceiptError::IntegrityMismatch)
    ));

    let mut foreign_node: serde_json::Value =
        serde_json::from_slice(&original).expect("receipt JSON");
    foreign_node["receipt"]["plan"]["nodes"][0]["kind"] =
        serde_json::Value::from("foreign-work-kind");
    let payload = serde_json::to_vec(&foreign_node["receipt"]).expect("receipt payload");
    foreign_node["payload_sha256"] = serde_json::Value::from(
        Sha256::digest(payload)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    );
    fs::write(
        &path,
        serde_json::to_vec_pretty(&foreign_node).expect("foreign-node JSON"),
    )
    .expect("write foreign-node receipt");
    assert!(matches!(
        store.open(first.attempt_id()),
        Err(crate::ReceiptError::IntegrityMismatch)
    ));

    let mut unsupported: serde_json::Value =
        serde_json::from_slice(&original).expect("receipt JSON");
    unsupported["schema"]["version"] = serde_json::Value::from(999_u64);
    fs::write(
        &path,
        serde_json::to_vec_pretty(&unsupported).expect("unsupported JSON"),
    )
    .expect("write unsupported receipt");
    assert!(matches!(
        store.open(first.attempt_id()),
        Err(crate::ReceiptError::UnsupportedSchema { version: 999, .. })
    ));
    fs::write(&path, original).expect("restore receipt");

    let pruning_directory = tempfile::tempdir().expect("pruning directory");
    let pruning_store = crate::ExecutionReceiptStore::new(
        pruning_directory.path(),
        crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("pruning store");
    let pruned = provenance(63, 64);
    drop(
        pruning_store
            .begin(pruned.clone(), &problem, &plan)
            .expect("first retained receipt"),
    );
    let retained = provenance(65, 66);
    let retained_recorder = pruning_store
        .begin(retained, &problem, &plan)
        .expect("terminal evidence can be pruned within the count ceiling");
    assert!(matches!(
        pruning_store.open(pruned.attempt_id()),
        Err(crate::ReceiptError::Io { .. })
    ));
    drop(retained_recorder);

    let active_directory = tempfile::tempdir().expect("active directory");
    let active_store = crate::ExecutionReceiptStore::new(
        active_directory.path(),
        crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("active store");
    let active = provenance(67, 68);
    let active_recorder = active_store
        .begin(active.clone(), &problem, &plan)
        .expect("active receipt");
    assert!(matches!(
        active_store.begin(provenance(69, 70), &problem, &plan),
        Err(crate::ReceiptError::RetentionExceeded)
    ));
    assert_eq!(
        active_store
            .open(active.attempt_id())
            .expect("active evidence preserved")
            .status(),
        crate::ReceiptStatus::Running
    );
    drop(active_recorder);

    let byte_directory = tempfile::tempdir().expect("byte-bound directory");
    let byte_store = crate::ExecutionReceiptStore::new(
        byte_directory.path(),
        crate::ReceiptRetention::new(1, 1).expect("retention"),
    )
    .expect("byte-bound store");
    assert!(matches!(
        byte_store.begin(provenance(71, 72), &problem, &plan),
        Err(crate::ReceiptError::RetentionExceeded)
    ));
}

#[test]
fn active_receipt_revisions_use_the_reservation_admitted_at_begin() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let provenance = |attempt| {
        execution_provenance(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([93; 32]),
        )
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let store = crate::ExecutionReceiptStore::new(
        directory.path(),
        crate::ReceiptRetention::new(3, 1_048_576).expect("retention"),
    )
    .expect("receipt store");

    let retained = provenance(91);
    drop(
        store
            .begin(retained.clone(), &problem, &plan)
            .expect("begin retained receipt"),
    );
    let active = provenance(92);
    let mut recorder = store
        .begin(active.clone(), &problem, &plan)
        .expect("begin reserves active receipt capacity");

    let retained_path = directory
        .path()
        .join(format!("{}.receipt.json", retained.attempt_id()));
    std::fs::write(&retained_path, b"corrupted after active admission")
        .expect("corrupt unrelated retained history");

    let work = WorkNodeId::new("work");
    recorder
        .work_started(&work)
        .expect("active revision remains inside its admitted reservation");
    assert_eq!(
        store
            .open(active.attempt_id())
            .expect("reopen active checkpoint")
            .node_status(&work),
        Some(crate::ReceiptStatus::Running)
    );
}

#[test]
fn reopened_reconstruction_receipt_has_no_publication_layout_evidence() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let directory = tempfile::tempdir().expect("receipt directory");
    let store = crate::ExecutionReceiptStore::new(
        directory.path(),
        crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    let provenance = execution_provenance(
        crate::ExecutionAttemptId::from_sha256([91; 32]),
        crate::BuildIdentity::from_sha256([92; 32]),
    );
    let recorder = store
        .begin(provenance.clone(), &problem, &plan)
        .expect("begin receipt");
    let reopened = store.open(provenance.attempt_id()).expect("reopen receipt");

    assert_eq!(reopened.publication_layout_count(), 0);
    drop(recorder);
}

#[test]
fn receipt_store_rejects_an_initial_checkpoint_that_cannot_hold_terminal_evidence() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let provenance = |attempt, build| {
        execution_provenance(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([build; 32]),
        )
    };

    let sizing_directory = tempfile::tempdir().expect("sizing directory");
    let sizing_store = crate::ExecutionReceiptStore::new(
        sizing_directory.path(),
        crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("sizing store");
    let sizing_provenance = provenance(73, 74);
    let recorder = sizing_store
        .begin(sizing_provenance.clone(), &problem, &plan)
        .expect("initial checkpoint");
    let receipt_path = sizing_directory
        .path()
        .join(format!("{}.receipt.json", sizing_provenance.attempt_id()));
    let running_bytes = fs::metadata(&receipt_path).expect("running receipt").len();
    drop(recorder);
    let terminal_bytes = fs::metadata(&receipt_path).expect("terminal receipt").len();
    assert!(
        terminal_bytes > running_bytes,
        "the fixture must expose the Running-to-terminal growth hazard"
    );

    let constrained_directory = tempfile::tempdir().expect("constrained directory");
    let constrained_store = crate::ExecutionReceiptStore::new(
        constrained_directory.path(),
        crate::ReceiptRetention::new(1, terminal_bytes - 1).expect("retention"),
    )
    .expect("constrained store");

    assert!(matches!(
        constrained_store.begin(provenance(75, 76), &problem, &plan),
        Err(crate::ReceiptError::RetentionExceeded)
    ));
    assert!(
        fs::read_dir(constrained_directory.path())
            .expect("constrained receipt directory")
            .next()
            .is_none(),
        "failed preflight must not leave durable Running evidence"
    );
}

#[test]
fn receipt_store_reserves_json_escaped_terminal_evidence_before_begin() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let provenance = |attempt| {
        execution_provenance(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([83; 32]),
        )
    };
    let terminal_size = |attempt, resource: String| {
        let directory = tempfile::tempdir().expect("terminal sizing directory");
        let store = crate::ExecutionReceiptStore::new(
            directory.path(),
            crate::ReceiptRetention::new(1, 1_048_576).expect("retention"),
        )
        .expect("terminal sizing store");
        let identity = provenance(attempt);
        let mut recorder = store
            .begin(identity.clone(), &problem, &plan)
            .expect("initial checkpoint");
        recorder
            .finish(
                crate::ReceiptStatus::Infeasible,
                Some(crate::receipt::ReceiptFailure::infeasible(
                    &crate::ResourceError::Infeasible {
                        resource,
                        required: u64::MAX,
                        available: 0,
                    },
                )),
            )
            .expect("terminal checkpoint");
        fs::metadata(
            directory
                .path()
                .join(format!("{}.receipt.json", identity.attempt_id())),
        )
        .expect("terminal receipt")
        .len()
    };
    let plain_bytes = terminal_size(84, "x".repeat(128));
    let escaped_bytes = terminal_size(85, "\0".repeat(128));
    assert!(
        escaped_bytes > plain_bytes,
        "control characters must expose JSON escaping growth"
    );
    let between = plain_bytes + (escaped_bytes - plain_bytes) / 2;
    let constrained_directory = tempfile::tempdir().expect("constrained directory");
    let constrained_store = crate::ExecutionReceiptStore::new(
        constrained_directory.path(),
        crate::ReceiptRetention::new(1, between).expect("retention"),
    )
    .expect("constrained store");

    assert!(matches!(
        constrained_store.begin(provenance(86), &problem, &plan),
        Err(crate::ReceiptError::RetentionExceeded)
    ));
    assert!(
        fs::read_dir(constrained_directory.path())
            .expect("constrained receipt directory")
            .next()
            .is_none(),
        "escaped terminal evidence must be reserved before Running is persisted"
    );
}

#[test]
fn receipts_reopen_machine_readable_infeasibility_certificates() {
    let problem = compiled_problem();
    let dag = ExecutionDag::new(plan_spec(vec![cpu_node("work", BTreeSet::new())]))
        .expect("valid physical work");
    let plan = bound_plan(dag);
    let directory = tempfile::tempdir().expect("receipt directory");
    let store = crate::ExecutionReceiptStore::new(
        directory.path(),
        crate::ReceiptRetention::new(3, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    let provenance = |attempt| {
        execution_provenance(
            crate::ExecutionAttemptId::from_sha256([attempt; 32]),
            crate::BuildIdentity::from_sha256([77; 32]),
        )
    };

    let no_capable = provenance(78);
    let mut recorder = store
        .begin(no_capable.clone(), &problem, &plan)
        .expect("begin no-capable receipt");
    recorder
        .finish(
            crate::ReceiptStatus::Infeasible,
            Some(crate::receipt::ReceiptFailure::infeasible(
                &crate::ResourceError::NoCapableAlternative,
            )),
        )
        .expect("finish no-capable receipt");
    assert_eq!(
        store
            .open(no_capable.attempt_id())
            .expect("no-capable receipt")
            .infeasibility_certificate(),
        Some(crate::ReceiptInfeasibilityCertificate::NoCapableAlternative)
    );

    let insufficient = provenance(79);
    let mut recorder = store
        .begin(insufficient.clone(), &problem, &plan)
        .expect("begin quantitative receipt");
    recorder
        .finish(
            crate::ReceiptStatus::Infeasible,
            Some(crate::receipt::ReceiptFailure::infeasible(
                &crate::ResourceError::Infeasible {
                    resource: "host-memory".to_string(),
                    required: 4_096,
                    available: 1_024,
                },
            )),
        )
        .expect("finish quantitative receipt");
    assert_eq!(
        store
            .open(insufficient.attempt_id())
            .expect("quantitative receipt")
            .infeasibility_certificate(),
        Some(crate::ReceiptInfeasibilityCertificate::Infeasible {
            resource: "host-memory".to_string(),
            resource_identity: crate::ResourceIdentity::new("host-memory"),
            required: 4_096,
            available: 1_024,
        })
    );

    let path_shaped = provenance(80);
    let mut recorder = store
        .begin(path_shaped.clone(), &problem, &plan)
        .expect("begin path-shaped quantitative receipt");
    recorder
        .finish(
            crate::ReceiptStatus::Infeasible,
            Some(crate::receipt::ReceiptFailure::infeasible(
                &crate::ResourceError::Infeasible {
                    resource: "storage-domain:/private/data".to_string(),
                    required: 8_192,
                    available: 2_048,
                },
            )),
        )
        .expect("finish path-shaped quantitative receipt");
    let certificate = store
        .open(path_shaped.attempt_id())
        .expect("path-shaped quantitative receipt")
        .infeasibility_certificate()
        .expect("path-shaped infeasibility certificate");
    let crate::ReceiptInfeasibilityCertificate::Infeasible {
        resource,
        resource_identity,
        required,
        available,
    } = certificate
    else {
        panic!("expected quantitative path-shaped certificate");
    };
    assert!(resource.starts_with("redacted:"));
    assert!(resource_identity.as_str().starts_with("redacted:"));
    assert!(!resource.contains('/'));
    assert!(!resource_identity.as_str().contains('/'));
    assert_eq!((required, available), (8_192, 2_048));
}
