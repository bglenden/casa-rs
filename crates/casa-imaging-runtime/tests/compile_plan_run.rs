// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    io,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use casa_imaging_model::{
    AxisOrder, CentreLaws, DeclaredInnerProducts, DelayCentreLaw, DirectionCoordinateSpec,
    DirectionFrame, DopplerConvention, FacetLayout, FiniteValuePolicy, FrequencyFrame,
    GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape, ImagingRequest,
    ImagingRequestVersion, InstrumentResponse, MeasurementEquationContract, MetadataTableKind,
    MissingPointingPolicy, ModelColumnWrite, ModelInnerProduct, ModelStateIdentity, MsColumnKind,
    NumericPrecision, NumericalStage, NumericsContract, ObservationPointingLaw,
    ObservationTransactionRequirements, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, PolarizationContract, PolarizationCoordinate,
    ProblemSpecification, ProductKind, ProductNormalization, ProductRequirements, Projection,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, ReferenceDataKind, RestFrequency, RestoringBeamPolicy, ScientificContract,
    SkyDirection, SpectralContract, SpectralCoordinateSpec, SpectralCoupling, SpectralFrameAnchor,
    SpectralSampling, SpectralWcs, StageErrorBudget, UvwCoordinateLaw, VisibilityInnerProduct,
    WeightDensityScope, WeightingContract, WeightingScheme, compile,
};
use casa_imaging_runtime::{
    AdaptationId, AdaptationTransition, AllocationAccess, AllocationId, AllocationLayout,
    AllocationLifetime, AllocationPurpose, AllocationUse, AlternativeId, ArtifactDisposition,
    ArtifactIdentity, ArtifactMeasurement, ArtifactRole, BindingKind, BuildIdentity, CacheDemand,
    CacheIdentity, CapabilityPredicate, CapacityDomainId, CapacityViewId, ClaimLifetime,
    CompiledProblemEvidence, CountDemand, CpuClassCapacity, DemandAlternative, DemandEnvelope,
    ExecutionDag, ExecutionDagSpecification, ExecutionError, ExecutionEvidenceError,
    ExecutionKnobs, ExecutionOutcome, ExecutionPlanId, ExecutionProvenance, ExecutionReceiptStore,
    ExecutionStatus, ExternalPressure, FenceId, FenceKind, HostInventory, ImplementationRegistry,
    ImplementationRegistryId, InitializationPolicy, IoBufferDemand, IoBufferKind, IoMeasurement,
    IoPrediction, LeaseResource, LogicalAllocation, MemoryCapacityDomain, MemoryCapacityKind,
    MemoryDemand,
    MemoryView, MemoryViewKind, ObservationTransactionWork, PhysicalSlot, PhysicalSlotId,
    PhysicalWorkBinding, PhysicalWorkBindingError, PlanError, PlanPrediction, PlannedArtifact,
    PlannerCostModelProfileId, PlanningBindings, PredictionConfidence, PredictionUncertainty,
    QueueDemand, QueueResource,
    QueueResourceId, QuiescencePoint, RateDemand, RateResource, RateResourceId, RateUnit,
    ReceiptFailureKind, ReceiptRetention, ReceiptStatus, RedactedPath, ResourceAuthority,
    ResourceClaim, ResourceError, ResourceHeadroom, ResourceMeasurement, ResourceOverride,
    ResourcePolicy,
    ResourceTopology, RunBindings, RunController, RunDirective, RunError, RunToCompletion,
    RuntimeOverheadDemand, ScalingMetadata, ScheduledWork, SlotCompatibility, StagePrediction,
    StorageDemand, StorageDomain, StorageDomainId, StorageMode, StorageUseKind, WorkDependency,
    WorkDomain, WorkExecutionContext, WorkImplementation, WorkImplementationId, WorkKind,
    WorkMeasurements, WorkNode, WorkNodeId, plan, run as run_receipted,
};

mod common;

use common::{identity, problem_inputs};

fn geometry(reference_pixel: f64) -> GeometryInput {
    let direction = DirectionCoordinateSpec::new(
        Projection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
        [reference_pixel, 255.0],
        [-4.848_136_811_095_36e-6, 4.848_136_811_095_36e-6],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 0.0],
    );
    GeometryInput::new(
        vec![ImageDomainSpec::new(
            ImageDomainRole::Main,
            ImageShape::new(512, 512),
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
            PhaseCentreLaw::Fixed(direction.reference_direction()),
            DelayCentreLaw::PhaseTrackingCentre,
            PointingCentreLaw::Observation(ObservationPointingLaw::new(
                PointingDirectionColumn::Direction,
                PointingDirectionSemantic::AntennaBoresight,
                PointingTimeSampling::VisibilityTimeCentroid,
                PointingInterpolation::GreatCircleShortestArc,
                PointingExtrapolation::Reject,
                MissingPointingPolicy::Reject,
            )),
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
    )
}

fn request(observation: u8) -> ImagingRequest {
    request_with_geometry_and_references(observation, geometry(255.0), Vec::new())
}

fn request_with_geometry(observation: u8, geometry: GeometryInput) -> ImagingRequest {
    request_with_geometry_and_references(observation, geometry, Vec::new())
}

fn request_with_geometry_and_references(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
) -> ImagingRequest {
    request_with_geometry_references_and_weighting(
        observation,
        geometry,
        references,
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
    )
}

fn request_with_geometry_references_and_weighting(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        references,
        weighting,
        vec![ProductKind::Psf],
        ModelColumnWrite::Disabled,
    )
}

fn request_with_products(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        Vec::new(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        ModelColumnWrite::Disabled,
    )
}

fn request_with_model_write(observation: u8) -> ImagingRequest {
    request_with_products_and_model(
        observation,
        geometry(255.0),
        vec![ProductKind::Psf],
        ModelColumnWrite::SelectedRows,
    )
}

fn request_with_products_and_model(
    observation: u8,
    geometry: GeometryInput,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
) -> ImagingRequest {
    request_with_geometry_references_weighting_products_and_model(
        observation,
        geometry,
        Vec::new(),
        WeightingContract::new(WeightingScheme::Natural, WeightDensityScope::NotApplicable),
        products,
        model_column_write,
    )
}

fn request_with_geometry_references_weighting_products_and_model(
    observation: u8,
    geometry: GeometryInput,
    references: Vec<(ReferenceDataKind, casa_imaging_model::LogicalIdentity)>,
    weighting: WeightingContract,
    products: Vec<ProductKind>,
    model_column_write: ModelColumnWrite,
) -> ImagingRequest {
    let numerics = NumericsContract::new(
        vec![NumericPrecision::F64],
        ReductionPolicy::Compensated,
        FiniteValuePolicy::FlagInputRejectGenerated,
        NumericalStage::ALL
            .into_iter()
            .map(|stage| (stage, StageErrorBudget::new(1.0e-7, 1.0e-3)))
            .collect(),
    );
    let specification = ProblemSpecification::new(
        ScientificContract::new(
            SpectralContract::new(SpectralSampling::Identity, SpectralCoupling::Independent),
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
            ReconstructionAlgorithm::Dirty,
            ReconstructionControls::new(0, 1.0, 0.0),
            PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
        ),
        weighting,
        ProductRequirements::new(
            products,
            ProductNormalization::UnitResponse,
            RestoringBeamPolicy::None,
        ),
        ObservationTransactionRequirements::new(model_column_write),
        numerics,
    );
    ImagingRequest::new(
        specification,
        geometry,
        problem_inputs(observation, references, ModelStateIdentity::Empty),
    )
}

fn registry(byte: u8) -> ImplementationRegistryId {
    ImplementationRegistryId::from_sha256([byte; 32])
}

fn cost_model(byte: u8) -> PlannerCostModelProfileId {
    PlannerCostModelProfileId::from_sha256([byte; 32])
}

fn implementation(byte: u8) -> WorkImplementationId {
    WorkImplementationId::new(format!("test-cpu-{byte}"))
}

fn recording_executor(
    byte: u8,
    failure: Option<&'static str>,
    fence_failure: Option<&'static str>,
) -> RecordingExecutor {
    RecordingExecutor {
        id: implementation(byte),
        failure,
        fence_failure,
        fail_only_fence: None,
        calls: AtomicUsize::new(0),
        fence_waits: AtomicUsize::new(0),
        observed_knobs: Mutex::new(Vec::new()),
        measurements: BTreeMap::new(),
        panic_on_execute: false,
        publication_launched: None,
        visible_generation: None,
        failure_node: None,
        fence_failure_event: None,
        publication_failure: None,
        generic_source_access: None,
        visibility_during_fence_settlement: None,
        publication_buffer_held: None,
    }
}

#[derive(Debug)]
struct RecordingExecutor {
    id: WorkImplementationId,
    failure: Option<&'static str>,
    fence_failure: Option<&'static str>,
    fail_only_fence: Option<FenceKind>,
    calls: AtomicUsize,
    fence_waits: AtomicUsize,
    observed_knobs: Mutex<Vec<ExecutionKnobs>>,
    measurements: BTreeMap<WorkNodeId, (Vec<IoMeasurement>, Vec<ArtifactMeasurement>)>,
    panic_on_execute: bool,
    publication_launched: Option<Arc<AtomicBool>>,
    visible_generation: Option<Arc<AtomicUsize>>,
    failure_node: Option<&'static str>,
    fence_failure_event: Option<(&'static str, FenceKind)>,
    publication_failure: Option<&'static str>,
    generic_source_access: Option<Arc<AtomicBool>>,
    visibility_during_fence_settlement: Option<Arc<AtomicBool>>,
    publication_buffer_held: Option<Arc<AtomicBool>>,
}

impl WorkImplementation for RecordingExecutor {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(
        &self,
        context: WorkExecutionContext<'_>,
        work: &ScheduledWork,
    ) -> Result<WorkMeasurements, Self::Error> {
        assert!(!self.panic_on_execute, "interrupted adapter");
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed_knobs
            .lock()
            .expect("recording executor knobs lock")
            .push(work.knobs().clone());
        if let Some(message) = self.failure {
            return Err(io::Error::other(message));
        }
        if self.failure_node == Some(work.node().id.as_str()) {
            return Err(io::Error::other("stateful transaction execute failure"));
        }
        if work.node().kind == WorkKind::Io
            && context.observation_reads().is_some()
            && let Some(accessed) = &self.generic_source_access
        {
            accessed.store(true, Ordering::SeqCst);
        }
        if work.node().kind == WorkKind::Publication
            && let Some(launched) = &self.publication_launched
        {
            launched.store(true, Ordering::SeqCst);
        }
        let resources = work
            .node()
            .claims
            .iter()
            .map(|claim| {
                ResourceMeasurement::new(
                    claim.resource.clone(),
                    claim.lifetime.clone(),
                    claim.amount,
                )
            })
            .collect();
        let (io, artifacts) = self
            .measurements
            .get(&work.node().id)
            .cloned()
            .unwrap_or_default();
        Ok(WorkMeasurements::new(resources, io, artifacts))
    }

    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        work: &ScheduledWork,
        fence: FenceKind,
    ) -> Result<(), Self::Error> {
        self.fence_waits.fetch_add(1, Ordering::SeqCst);
        if let Some(message) = self.fence_failure
            && self.fail_only_fence.is_none_or(|kind| kind == fence)
        {
            return Err(io::Error::other(message));
        }
        if self.fence_failure_event == Some((work.node().id.as_str(), fence)) {
            return Err(io::Error::other("stateful transaction fence failure"));
        }
        if self
            .visible_generation
            .as_ref()
            .is_some_and(|visible| visible.load(Ordering::SeqCst) == 1)
            && let Some(observed) = &self.visibility_during_fence_settlement
        {
            observed.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn publish(
        &self,
        context: WorkExecutionContext<'_>,
        work: &ScheduledWork,
    ) -> Result<(), Self::Error> {
        if work.node().kind != WorkKind::Publication || context.publication().is_none() {
            return Err(io::Error::other(
                "publication requires the transaction-bound Publication node",
            ));
        }
        if let Some(message) = self.publication_failure {
            return Err(io::Error::other(message));
        }
        if let Some(observed) = &self.publication_buffer_held {
            let allocation = AllocationId::new("transaction-publication-buffer");
            let slot = PhysicalSlotId::new("transaction-publication-slot");
            observed.store(
                context.publication_resources().is_some_and(|resources| {
                    resources.lease_epoch() > 0
                        && resources.allocation_slot(&allocation) == Some(&slot)
                }),
                Ordering::SeqCst,
            );
        }
        if let Some(visible) = &self.visible_generation {
            visible.store(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

fn publication_recording_executor(
    byte: u8,
    launched: Arc<AtomicBool>,
    visible_generation: Arc<AtomicUsize>,
) -> RecordingExecutor {
    RecordingExecutor {
        id: implementation(byte),
        failure: None,
        fence_failure: None,
        fail_only_fence: None,
        calls: AtomicUsize::new(0),
        fence_waits: AtomicUsize::new(0),
        observed_knobs: Mutex::new(Vec::new()),
        measurements: BTreeMap::new(),
        panic_on_execute: false,
        publication_launched: Some(launched),
        visible_generation: Some(visible_generation),
        failure_node: None,
        fence_failure_event: None,
        publication_failure: None,
        generic_source_access: None,
        visibility_during_fence_settlement: None,
        publication_buffer_held: None,
    }
}

fn failing_transaction_executor(
    byte: u8,
    visible_generation: Arc<AtomicUsize>,
    failure_node: Option<&'static str>,
    fence_failure_event: Option<(&'static str, FenceKind)>,
    publication_failure: Option<&'static str>,
) -> RecordingExecutor {
    RecordingExecutor {
        id: implementation(byte),
        failure: None,
        fence_failure: None,
        fail_only_fence: None,
        calls: AtomicUsize::new(0),
        fence_waits: AtomicUsize::new(0),
        observed_knobs: Mutex::new(Vec::new()),
        measurements: BTreeMap::new(),
        panic_on_execute: false,
        publication_launched: None,
        visible_generation: Some(visible_generation),
        failure_node,
        fence_failure_event,
        publication_failure,
        generic_source_access: None,
        visibility_during_fence_settlement: None,
        publication_buffer_held: None,
    }
}

fn physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    physical_work_with_transaction_staging(implementation_byte, None, false, false)
}

fn physical_work_with_product_staging(
    implementation_byte: u8,
    required_product_staging: Option<BTreeMap<ProductKind, BTreeSet<WorkDependency>>>,
) -> PhysicalWorkBinding {
    physical_work_with_transaction_staging(
        implementation_byte,
        required_product_staging,
        false,
        false,
    )
}

fn physical_work_with_model_staging(implementation_byte: u8) -> PhysicalWorkBinding {
    physical_work_with_transaction_staging(implementation_byte, None, true, false)
}

fn physical_work_with_early_publication_buffer(implementation_byte: u8) -> PhysicalWorkBinding {
    physical_work_with_transaction_staging(implementation_byte, None, false, true)
}

fn physical_work_with_transaction_staging(
    implementation_byte: u8,
    required_product_staging: Option<BTreeMap<ProductKind, BTreeSet<WorkDependency>>>,
    include_model_staging: bool,
    acquire_publication_early: bool,
) -> PhysicalWorkBinding {
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
    transaction_binding(
        specification,
        implementation(implementation_byte),
        required_product_staging,
        include_model_staging,
        acquire_publication_early,
    )
}

fn bind_physical_work(
    dag: ExecutionDag,
    observation_transaction: ObservationTransactionWork,
    artifacts: Vec<PlannedArtifact>,
) -> PhysicalWorkBinding {
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let mut io = BTreeMap::<IoBufferKind, u64>::new();
            for claim in &node.claims {
                if let LeaseResource::IoBuffer(kind) = claim.resource {
                    *io.entry(kind).or_default() += claim.amount;
                }
            }
            let stage = StagePrediction::new(node.id.clone(), 100);
            if io.is_empty() {
                stage
            } else {
                stage.with_io(
                    io.into_iter()
                        .map(|(kind, bytes)| IoPrediction::new(kind, bytes, 1))
                        .collect(),
                )
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete prediction");
    PhysicalWorkBinding::new(dag, prediction, artifacts, observation_transaction)
        .expect("bound physical work")
}

fn transaction_binding(
    mut specification: ExecutionDagSpecification,
    work_implementation: WorkImplementationId,
    required_product_staging: Option<BTreeMap<ProductKind, BTreeSet<WorkDependency>>>,
    include_model_staging: bool,
    acquire_publication_early: bool,
) -> PhysicalWorkBinding {
    let initial = WorkNodeId::new("transaction-check");
    let read = WorkNodeId::new("transaction-read");
    let reconciliation = WorkNodeId::new("transaction-reconciliation");
    let product = WorkNodeId::new("transaction-stage-psf");
    let model = WorkNodeId::new("transaction-stage-model");
    let commit = WorkNodeId::new("transaction-commit");
    let publication_allocation = AllocationId::new("transaction-publication-buffer");
    let publication_slot = PhysicalSlotId::new("transaction-publication-slot");
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
    let writeback_allocation = AllocationId::new("transaction-writeback-buffer");
    let writeback_slot = PhysicalSlotId::new("transaction-writeback-slot");
    let writeback_lifetime = ClaimLifetime::through_fences([FenceKind::Io, FenceKind::Writeback]);
    let writeback_compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 1,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("transaction-writeback-buffer"),
        initialization: InitializationPolicy::Preserve,
        access: AllocationAccess::ReadWrite,
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
    let read_completion = WorkDependency::Fence(FenceId::new(read.clone(), FenceKind::Io));
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
    if include_model_staging {
        specification
            .resource_alternative
            .demand
            .memory
            .push(MemoryDemand {
                allocation_id: "transaction-writeback-slot".to_string(),
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
            staged_output_bytes: if include_model_staging { 2 } else { 1 },
            final_output_bytes: 0,
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
        .publication_bytes = 1;
    if include_model_staging {
        specification
            .resource_alternative
            .demand
            .io_buffers
            .writeback_bytes = 1;
    }

    let mut commit_dependencies = BTreeSet::from([WorkDependency::Work(product.clone())]);
    if include_model_staging {
        commit_dependencies.extend([
            WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Io)),
            WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Writeback)),
        ]);
    }
    let mut product_claims = vec![
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
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
    ];
    let product_allocations = if acquire_publication_early {
        product_claims.push(ResourceClaim {
            resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
            amount: 1,
            lifetime: ClaimLifetime::Work,
        });
        vec![AllocationUse {
            allocation: publication_allocation.clone(),
            lifetime: ClaimLifetime::Work,
        }]
    } else {
        Vec::new()
    };

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
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: ClaimLifetime::through_fence(FenceKind::Io),
                },
                ResourceClaim {
                    resource: LeaseResource::MeasurementSetLock {
                        measurement_set: casa_imaging_model::MeasurementSetIdentity::new(identity(
                            1,
                        )),
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
            id: reconciliation.clone(),
            kind: WorkKind::Compute,
            domain: WorkDomain::Cpu,
            implementation: work_implementation.clone(),
            dependencies: terminals,
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
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Publication),
                    amount: 1,
                    lifetime: publication_lifetime.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: publication_allocation.clone(),
                lifetime: publication_lifetime.clone(),
            }],
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Publication]),
            quiescence_after: BTreeSet::new(),
        },
    ]);
    if include_model_staging {
        specification.nodes.push(WorkNode {
            id: model.clone(),
            kind: WorkKind::Writeback,
            domain: WorkDomain::Io,
            implementation: work_implementation.clone(),
            dependencies: BTreeSet::from([WorkDependency::Work(reconciliation.clone())]),
            claims: vec![
                ResourceClaim {
                    resource: LeaseResource::Rate {
                        demand_id: "transaction-io-rate".to_string(),
                    },
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Queue {
                        demand_id: "transaction-io-queue".to_string(),
                    },
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::Storage {
                        demand_id: "transaction-output".to_string(),
                        use_kind: casa_imaging_runtime::StorageUseKind::StagedOutput,
                    },
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
                ResourceClaim {
                    resource: LeaseResource::IoBuffer(IoBufferKind::Writeback),
                    amount: 1,
                    lifetime: writeback_lifetime.clone(),
                },
            ],
            allocations: vec![AllocationUse {
                allocation: writeback_allocation.clone(),
                lifetime: writeback_lifetime,
            }],
            fences: BTreeSet::from([FenceKind::Io, FenceKind::Writeback]),
            quiescence_after: BTreeSet::new(),
        });
    }
    specification.logical_allocations.push(LogicalAllocation {
        id: publication_allocation,
        bytes: 1,
        purpose: AllocationPurpose::IoBuffer(IoBufferKind::Publication),
        compatibility: publication_compatibility.clone(),
        physical_slot: publication_slot.clone(),
        lifetime: AllocationLifetime {
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
    if include_model_staging {
        specification.logical_allocations.push(LogicalAllocation {
            id: writeback_allocation,
            bytes: 1,
            purpose: AllocationPurpose::IoBuffer(IoBufferKind::Writeback),
            compatibility: writeback_compatibility.clone(),
            physical_slot: writeback_slot.clone(),
            lifetime: AllocationLifetime {
                acquire_at: model.clone(),
                release_after: BTreeSet::from([
                    WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Io)),
                    WorkDependency::Fence(FenceId::new(model.clone(), FenceKind::Writeback)),
                ]),
            },
        });
        specification.physical_slots.push(PhysicalSlot {
            id: writeback_slot,
            lease_resource: LeaseResource::Memory {
                allocation_id: "transaction-writeback-slot".to_string(),
            },
            capacity_bytes: 1,
            compatibility: writeback_compatibility,
        });
    }

    let dag = ExecutionDag::new(specification).expect("valid transaction-bound physical work");
    let observation_transaction = ObservationTransactionWork::new(
            initial,
            reconciliation,
            required_product_staging.unwrap_or_else(|| {
                BTreeMap::from([(
                    ProductKind::Psf,
                    BTreeSet::from([WorkDependency::Work(product)]),
                )])
            }),
            include_model_staging.then_some(model),
            commit,
        );
    bind_physical_work(dag, observation_transaction, Vec::new())
}

fn evidenced_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let base = physical_work(implementation_byte);
    let base_dag = base.execution_dag();
    let read = WorkNodeId::new("read");
    let commit = base.observation_transaction().commit().clone();
    let source_buffer = AllocationId::new("source-read-ahead-buffer");
    let source_slot = PhysicalSlotId::new("source-read-ahead-slot");
    let source_lifetime = ClaimLifetime::through_fence(FenceKind::Io);
    let compatibility = SlotCompatibility {
        memory_domain: CapacityDomainId::new("host-memory"),
        views: BTreeSet::from([CapacityViewId::new("host-memory")]),
        alignment_bytes: 64,
        storage_mode: StorageMode::Host,
        layout: AllocationLayout::new("source-read-ahead"),
        initialization: InitializationPolicy::OverwriteBeforeRead,
        access: AllocationAccess::ReadWrite,
    };
    let mut nodes = base_dag.nodes().values().cloned().collect::<Vec<_>>();
    let read_node = nodes
        .iter_mut()
        .find(|node| node.id == read)
        .expect("generic I/O node");
    read_node.claims.push(ResourceClaim {
        resource: LeaseResource::IoBuffer(IoBufferKind::SourceReadAhead),
        amount: 32,
        lifetime: source_lifetime.clone(),
    });
    read_node.allocations.push(AllocationUse {
        allocation: source_buffer.clone(),
        lifetime: source_lifetime,
    });
    let mut alternative = base_dag.resource_alternative().clone();
    alternative.demand.memory.push(MemoryDemand {
        allocation_id: "source-read-ahead-slot".to_string(),
        hard_bytes: 32,
        preferred_bytes: 32,
        views: vec![CapacityViewId::new("host-memory")],
    });
    alternative.demand.io_buffers.source_read_ahead_bytes = 32;
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
            allocation_id: "source-read-ahead-slot".to_string(),
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
    .expect("valid evidenced transaction work");
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        dag.nodes()
            .values()
            .map(|node| {
                let stage = StagePrediction::new(node.id.clone(), 100);
                if node.id == read {
                    stage.with_io(vec![IoPrediction::new(
                        IoBufferKind::SourceReadAhead,
                        8_192,
                        4,
                    )])
                } else if node.id == commit {
                    stage.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 2_048, 1)])
                } else if node.kind == WorkKind::Writeback {
                    stage.with_io(vec![IoPrediction::new(IoBufferKind::Writeback, 1, 1)])
                } else {
                    stage
                }
            })
            .collect(),
    )
    .expect("evidenced prediction");
    let artifacts = vec![
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
        PlannedArtifact::new(
            ArtifactIdentity::from_sha256([34; 32]),
            commit,
            ArtifactRole::Output,
            None,
        ),
    ];
    PhysicalWorkBinding::new(
        dag,
        prediction,
        artifacts,
        base.observation_transaction().clone(),
    )
    .expect("bound evidenced physical work")
}

fn adaptive_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
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
        }],
    };
    transaction_binding(
        specification,
        implementation(implementation_byte),
        None,
        false,
        false,
    )
}

fn auditable_physical_work(implementation_byte: u8) -> PhysicalWorkBinding {
    let base = adaptive_physical_work(implementation_byte);
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
    let mut logical_allocations = base_dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    logical_allocations.push(LogicalAllocation {
        id: allocation,
        bytes: 64,
        purpose: AllocationPurpose::Data,
        compatibility: compatibility.clone(),
        physical_slot: slot.clone(),
        lifetime: AllocationLifetime {
            acquire_at: WorkNodeId::new("first-major-work"),
            release_after: BTreeSet::from([WorkDependency::Work(WorkNodeId::new(
                "minor-work",
            ))]),
        },
    });
    let mut physical_slots = base_dag
        .physical_slots()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    physical_slots.push(PhysicalSlot {
        id: slot,
        lease_resource: LeaseResource::Memory {
            allocation_id: "audit-memory".to_string(),
        },
        capacity_bytes: 64,
        compatibility,
    });
    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: BTreeSet::from([
            casa_imaging_runtime::CapabilityId::new("audit-capability"),
        ]),
        resource_alternative: alternative,
        nodes,
        logical_allocations,
        physical_slots,
        initial_knobs: base_dag.initial_knobs().clone(),
        adaptations: base_dag.adaptations().values().cloned().collect(),
    })
    .expect("valid auditable transaction work");
    bind_physical_work(
        dag,
        base.observation_transaction().clone(),
        vec![PlannedArtifact::new(
            ArtifactIdentity::from_sha256([51; 32]),
            WorkNodeId::new("first-major-work"),
            ArtifactRole::Cache,
            Some(CacheIdentity::from_sha256([52; 32])),
        )],
    )
}

fn release_failure_physical_work(
    implementation_byte: u8,
    release_implementation_byte: u8,
    fail_at_fence: bool,
) -> PhysicalWorkBinding {
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
    let binding = transaction_binding(
        specification,
        implementation(implementation_byte),
        None,
        false,
        false,
    );
    let dag = binding.execution_dag().clone();
    let stages = dag
        .nodes()
        .values()
        .map(|node| {
            let mut io = BTreeMap::<IoBufferKind, u64>::new();
            for claim in &node.claims {
                if let LeaseResource::IoBuffer(kind) = claim.resource {
                    *io.entry(kind).or_default() += claim.amount;
                }
            }
            let stage = StagePrediction::new(node.id.clone(), 100);
            if io.is_empty() {
                stage
            } else {
                stage.with_io(
                    io.into_iter()
                        .map(|(kind, bytes)| {
                            let predicted = if node.id == prepare_id || node.id == release_id {
                                100
                            } else {
                                bytes
                            };
                            IoPrediction::new(kind, predicted, 1)
                        })
                        .collect(),
                )
            }
        })
        .collect();
    let prediction = PlanPrediction::new(
        u64::try_from(dag.nodes().len()).expect("node count") * 100,
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete release-failure prediction");
    PhysicalWorkBinding::new(
        dag,
        prediction,
        binding.artifacts().to_vec(),
        binding.observation_transaction().clone(),
    )
    .expect("bound external-release failure work")
}

fn test_registry(
    registry_byte: u8,
    implementation_byte: u8,
    failure: Option<&'static str>,
) -> TestRegistry {
    TestRegistry {
        id: registry(registry_byte),
        executors: BTreeMap::from([(
            implementation(implementation_byte),
            recording_executor(implementation_byte, failure, None),
        )]),
    }
}

struct TestRegistry {
    id: ImplementationRegistryId,
    executors: BTreeMap<WorkImplementationId, RecordingExecutor>,
}

#[derive(Default)]
struct RejectAfterLaunch {
    polls: usize,
}

impl RunController for RejectAfterLaunch {
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective {
        self.polls += 1;
        if self.polls <= 2 {
            RunDirective::Continue
        } else {
            assert!(status.eligible_adaptations().is_empty());
            RunDirective::Adapt(casa_imaging_runtime::AdaptationId::new("not-eligible"))
        }
    }
}

#[derive(Default)]
struct AdaptAtMajorBoundary {
    applied: bool,
}

impl RunController for AdaptAtMajorBoundary {
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective {
        let adaptation = AdaptationId::new("larger-batch");
        if !self.applied
            && status
                .eligible_adaptations()
                .iter()
                .any(|transition| transition.id == adaptation)
        {
            self.applied = true;
            RunDirective::Adapt(adaptation)
        } else {
            RunDirective::Continue
        }
    }
}

#[derive(Default)]
struct CancelAfterLaunch {
    polls: usize,
}

impl RunController for CancelAfterLaunch {
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective {
        self.polls += 1;
        if self.polls == 1 {
            RunDirective::Continue
        } else {
            assert!(status.eligible_adaptations().is_empty());
            RunDirective::Cancel
        }
    }
}

struct CancelAtPublicationState {
    publication_launched: Arc<AtomicBool>,
    visible_generation: Arc<AtomicUsize>,
    after_fence: bool,
    requested: bool,
}

struct AdaptAtPublicationLaunch {
    publication_launched: Arc<AtomicBool>,
    requested: bool,
}

impl RunController for AdaptAtPublicationLaunch {
    fn directive(&mut self, _status: &ExecutionStatus) -> RunDirective {
        if self.publication_launched.load(Ordering::SeqCst) && !self.requested {
            self.requested = true;
            RunDirective::Adapt(AdaptationId::new("post-publication-adaptation"))
        } else {
            RunDirective::Continue
        }
    }
}

impl RunController for CancelAtPublicationState {
    fn directive(&mut self, _status: &ExecutionStatus) -> RunDirective {
        let reached = if self.after_fence {
            self.visible_generation.load(Ordering::SeqCst) == 1
        } else {
            self.publication_launched.load(Ordering::SeqCst)
        };
        if reached && !self.requested {
            self.requested = true;
            RunDirective::Cancel
        } else {
            RunDirective::Continue
        }
    }
}

impl ImplementationRegistry for TestRegistry {
    type Implementation = RecordingExecutor;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        self.executors.get(id)
    }
}

fn authority() -> &'static ResourceAuthority {
    static AUTHORITY: OnceLock<&'static ResourceAuthority> = OnceLock::new();
    AUTHORITY.get_or_init(|| {
        ResourceAuthority::install_production_inventory(runtime_inventory(4))
            .expect("install deterministic combined receipt and transaction inventory")
    })
}

fn runtime_inventory(available_locks: u64) -> HostInventory {
    let domain = CapacityDomainId::new("host-memory");
    let view = CapacityViewId::new("host-memory");
    let rate = RateResourceId::new("io-rate");
    let queue = QueueResourceId::new("io-queue");
    let transaction_rate = RateResourceId::new("transaction-io-rate");
    let transaction_queue = QueueResourceId::new("transaction-io-queue");
    let storage = StorageDomainId::new("atomic-output");
    HostInventory {
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
                id: storage.clone(),
                root: PathBuf::from("/tmp/casa-rs-imaging-runtime-tests"),
                capacity_bytes: 1_024,
                read_rate: rate.clone(),
                write_rate: rate.clone(),
                operations_rate: None,
                queue: queue.clone(),
            }],
            rate_resources: vec![
                RateResource::new(rate.clone(), RateUnit::BytesPerSecond, 16),
                RateResource::new(transaction_rate.clone(), RateUnit::BytesPerSecond, 16),
            ],
            queue_resources: vec![
                QueueResource::new(queue.clone(), 4),
                QueueResource::new(transaction_queue.clone(), 4),
            ],
            logical_cpu_threads: 4,
            performance_cpu_cores: CpuClassCapacity::Known(4),
            cache_capacity_bytes: 1_024,
            lock_capacity: 4,
            file_descriptor_capacity: 16,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 1_024)]),
            available_cpu_threads: 4,
            storage_available_bytes: BTreeMap::from([(storage, 1_024)]),
            rate_available_per_second: BTreeMap::from([(rate, 16), (transaction_rate, 16)]),
            queue_available_slots: BTreeMap::from([(queue, 4), (transaction_queue, 4)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_024,
            available_locks,
            available_file_descriptors: 16,
        },
    }
}

fn run_lock() -> &'static Mutex<()> {
    static RUN_LOCK: Mutex<()> = Mutex::new(());
    &RUN_LOCK
}

fn run<C: RunController>(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
    authority: &ResourceAuthority,
    controller: &mut C,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    run_receipted(
        problem,
        plan,
        current,
        registry,
        authority,
        controller,
        receipts.bind(ExecutionProvenance::new(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([241; 32]),
            BuildIdentity::from_sha256([242; 32]),
        )),
    )
}

fn execute_plan(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let _guard = run_lock().lock().expect("runtime test lock");
    let mut controller = RunToCompletion;
    run(
        problem,
        plan,
        current,
        registry,
        authority(),
        &mut controller,
    )
}

#[test]
fn physical_work_binding_rejects_io_and_publication_evidence_outside_plan_semantics() {
    let io_dag = physical_work(6).execution_dag().clone();
    let io_prediction = PlanPrediction::new(
        200,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        io_dag
            .nodes()
            .keys()
            .map(|node| {
                let prediction = StagePrediction::new(node.clone(), 100);
                if node.as_str() == "execute" {
                    prediction.with_io(vec![IoPrediction::new(
                        IoBufferKind::SourceReadAhead,
                        8_192,
                        4,
                    )])
                } else {
                    prediction
                }
            })
            .collect(),
    )
    .expect("well-formed prediction ledger");

    assert!(matches!(
        PhysicalWorkBinding::new(io_dag, io_prediction, Vec::new()),
        Err(PhysicalWorkBindingError::IoKindMismatch {
            kind: IoBufferKind::SourceReadAhead,
            work_kind: WorkKind::Compute,
            ..
        })
    ));

    let contract_base = physical_work(6);
    let contract_base_dag = contract_base.execution_dag();
    let mut contract_nodes = contract_base_dag
        .nodes()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    contract_nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("read"))
        .expect("read node")
        .kind = WorkKind::Prefetch;
    let contract_dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: contract_base_dag.required_resource_capabilities().clone(),
        resource_alternative: contract_base_dag.resource_alternative().clone(),
        nodes: contract_nodes,
        logical_allocations: Vec::new(),
        physical_slots: Vec::new(),
        initial_knobs: contract_base_dag.initial_knobs().clone(),
        adaptations: contract_base_dag.adaptations().values().cloned().collect(),
    })
    .expect("Prefetch node may be zero-copy without I/O evidence");
    let contract_prediction = PlanPrediction::new(
        200,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        contract_dag
            .nodes()
            .keys()
            .map(|node| {
                let prediction = StagePrediction::new(node.clone(), 100);
                if node.as_str() == "read" {
                    prediction.with_io(vec![IoPrediction::new(
                        IoBufferKind::SourceReadAhead,
                        8_192,
                        4,
                    )])
                } else {
                    prediction
                }
            })
            .collect(),
    )
    .expect("well-formed prediction ledger");
    assert!(matches!(
        PhysicalWorkBinding::new(contract_dag, contract_prediction, Vec::new()),
        Err(PhysicalWorkBindingError::MissingIoContract {
            kind: IoBufferKind::SourceReadAhead,
            ..
        })
    ));

    let publication_dag = physical_work(6).execution_dag().clone();
    let publication_prediction = PlanPrediction::new(
        200,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        publication_dag
            .nodes()
            .keys()
            .cloned()
            .map(|node| StagePrediction::new(node, 100))
            .collect(),
    )
    .expect("well-formed prediction ledger");
    let output = PlannedArtifact::new(
        ArtifactIdentity::from_sha256([79; 32]),
        WorkNodeId::new("execute"),
        ArtifactRole::Output,
        None,
    );

    assert!(matches!(
        PhysicalWorkBinding::new(publication_dag, publication_prediction, vec![output]),
        Err(PhysicalWorkBindingError::MissingPublicationContract { .. })
    ));
}

#[test]
fn physical_work_binding_rejects_typed_io_contracts_without_predictions() {
    let dag = evidenced_physical_work(6).execution_dag().clone();
    let prediction = PlanPrediction::new(
        300,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        dag.nodes()
            .keys()
            .cloned()
            .map(|node| StagePrediction::new(node, 100))
            .collect(),
    )
    .expect("complete stage ledger with no typed I/O evidence");

    assert!(matches!(
        PhysicalWorkBinding::new(dag, prediction, Vec::new()),
        Err(PhysicalWorkBindingError::MissingIoPrediction { .. })
    ));
}

#[test]
fn physical_work_binding_rejects_cpu_io_buffer_contracts_without_predictions() {
    let base = evidenced_physical_work(6);
    let base_dag = base.execution_dag();
    let prepare = WorkNodeId::new("read");
    let source_buffer = AllocationId::new("source-read-ahead-buffer");
    let mut nodes = base_dag.nodes().values().cloned().collect::<Vec<_>>();
    let prepare_node = nodes
        .iter_mut()
        .find(|node| node.id == prepare)
        .expect("preparation node");
    prepare_node.kind = WorkKind::Preparation;
    prepare_node.domain = WorkDomain::Cpu;
    prepare_node.claims = vec![
        ResourceClaim {
            resource: casa_imaging_runtime::LeaseResource::Workers,
            amount: 1,
            lifetime: ClaimLifetime::Work,
        },
        ResourceClaim {
            resource: casa_imaging_runtime::LeaseResource::IoBuffer(IoBufferKind::Preparation),
            amount: 32,
            lifetime: ClaimLifetime::Work,
        },
    ];
    prepare_node.allocations[0].lifetime = ClaimLifetime::Work;
    prepare_node.fences.clear();
    nodes
        .iter_mut()
        .find(|node| node.id == WorkNodeId::new("execute"))
        .expect("compute node")
        .dependencies = BTreeSet::from([WorkDependency::Work(prepare.clone())]);

    let mut resource_alternative = base_dag.resource_alternative().clone();
    resource_alternative
        .demand
        .io_buffers
        .source_read_ahead_bytes = 0;
    resource_alternative.demand.io_buffers.preparation_bytes = 32;
    let mut logical_allocations = base_dag
        .logical_allocations()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let allocation = logical_allocations
        .iter_mut()
        .find(|allocation| allocation.id == source_buffer)
        .expect("preparation allocation");
    allocation.purpose = AllocationPurpose::IoBuffer(IoBufferKind::Preparation);
    allocation.lifetime.release_after = BTreeSet::from([WorkDependency::Work(prepare.clone())]);

    let dag = ExecutionDag::new(ExecutionDagSpecification {
        required_resource_capabilities: base_dag.required_resource_capabilities().clone(),
        resource_alternative,
        nodes,
        logical_allocations,
        physical_slots: base_dag.physical_slots().values().cloned().collect(),
        initial_knobs: base_dag.initial_knobs().clone(),
        adaptations: base_dag.adaptations().values().cloned().collect(),
    })
    .expect("CPU preparation may own a typed preparation buffer");
    let prediction = PlanPrediction::new(
        300,
        PredictionConfidence::new(900_000).expect("confidence"),
        Vec::new(),
        dag.nodes()
            .keys()
            .map(|node| {
                let stage = StagePrediction::new(node.clone(), 100);
                if node.as_str() == "publish" {
                    stage.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 2_048, 1)])
                } else {
                    stage
                }
            })
            .collect(),
    )
    .expect("complete prediction ledger without preparation I/O evidence");

    assert!(matches!(
        PhysicalWorkBinding::new(dag, prediction, Vec::new()),
        Err(PhysicalWorkBindingError::MissingIoPrediction {
            kind: IoBufferKind::Preparation,
            ..
        })
    ));
}

#[test]
fn run_rejects_artifact_dispositions_that_contradict_plan_semantics() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(evidenced_physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let input = ArtifactIdentity::from_sha256([31; 32]);
    let cache = ArtifactIdentity::from_sha256([32; 32]);
    let mut executor = recording_executor(6, None, None);
    executor.measurements = BTreeMap::from([(
        WorkNodeId::new("read"),
        (
            vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
            vec![
                ArtifactMeasurement::new(
                    input,
                    Some(input),
                    ArtifactDisposition::Published,
                    4_096,
                    None,
                ),
                ArtifactMeasurement::new(cache, None, ArtifactDisposition::RejectedStale, 0, None),
            ],
        ),
    )]);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    let error = execute_plan(&problem, &execution_plan, &current, &registry)
        .expect_err("an input artifact cannot claim publication");

    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::ArtifactDispositionMismatch {
            node,
            artifact,
            role: ArtifactRole::Input,
            disposition: ArtifactDisposition::Published,
        }) if node == WorkNodeId::new("read") && artifact == input
    ));
}

#[test]
fn run_can_invoke_only_the_implementation_identity_sealed_by_plan() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let selected = recording_executor(6, None, None);
    let different = recording_executor(7, None, None);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([
            (implementation(6), selected),
            (implementation(7), different),
        ]),
    };
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let output =
        execute_plan(&problem, &execution_plan, &current, &registry).expect("bound execution");

    assert_eq!(output, ExecutionOutcome::Succeeded);
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        7,
        "two planned nodes and five mandatory transaction nodes must execute"
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        4,
        "planned and transaction reads plus both publication fences must settle"
    );
    assert_eq!(
        registry.executors[&implementation(7)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn generic_io_cannot_receive_observation_sources() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let generic_source_access = Arc::new(AtomicBool::new(false));
    let mut executor = recording_executor(6, None, None);
    executor.generic_source_access = Some(Arc::clone(&generic_source_access));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    execute_plan(&problem, &execution_plan, &current, &registry).expect("bound execution");

    assert!(
        !generic_source_access.load(Ordering::SeqCst),
        "generic Io must not receive the MeasurementSet observation source set"
    );
}

#[test]
fn run_rejects_a_registry_that_cannot_resolve_the_bound_implementation() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let registry = test_registry(3, 7, None);
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let result = execute_plan(&problem, &execution_plan, &current, &registry);

    assert!(matches!(
        result,
        Err(RunError::ImplementationUnavailable { implementation: id })
            if id == implementation(6)
    ));
    assert_eq!(
        registry.executors[&implementation(7)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn run_rejects_a_different_implementation_returned_under_the_bound_key() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let mut registry = test_registry(3, 6, None);
    registry
        .executors
        .get_mut(&implementation(6))
        .expect("registered key")
        .id = implementation(7);
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

    let result = execute_plan(&problem, &execution_plan, &current, &registry);

    assert!(matches!(
        result,
        Err(RunError::ImplementationMismatch { planned, observed })
            if planned == implementation(6) && observed == implementation(7)
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn versioned_request_compiles_before_physical_planning() {
    let request = request(1);
    assert_eq!(request.version(), ImagingRequestVersion::V2);

    let problem = compile(request).expect("logical compilation");
    assert_eq!(problem.numerics_id().as_bytes().len(), 32);
}

#[test]
fn plan_seals_physical_work_and_every_required_binding() {
    assert_eq!(ExecutionPlanId::SCHEMA_VERSION, 6);
    let problem = compile(request(1)).expect("logical compilation");
    let expected_problem_id = problem.problem_id();
    let bindings = PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let execution_plan = plan(&problem, bindings.clone(), |problem, bindings| {
        assert_eq!(problem.problem_id(), expected_problem_id);
        assert_eq!(bindings.resource_policy(), &ResourcePolicy::Balanced);
        Ok::<_, ()>(physical_work(6))
    })
    .expect("physical planning");

    assert_eq!(execution_plan.problem_id(), problem.problem_id());
    assert_eq!(
        execution_plan.geometry_id(),
        problem.geometry().geometry_id()
    );
    assert_eq!(
        execution_plan.observation_snapshot_id(),
        problem.inputs().observation()
    );
    assert_eq!(execution_plan.numerics_id(), problem.numerics_id());
    assert_eq!(execution_plan.implementation_registry_id(), registry(3));
    assert_eq!(
        execution_plan.resource_policy_id(),
        bindings.resource_policy_id()
    );
    assert_eq!(
        execution_plan.planner_cost_model_profile_id(),
        cost_model(4)
    );
    assert_eq!(
        execution_plan.physical_work_id(),
        execution_plan.execution_dag().physical_work_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().problem_id(),
        problem.problem_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().transaction_id(),
        problem.observation_transaction().transaction_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().physical_work_id(),
        execution_plan.physical_work_id()
    );

    let repeated = plan(&problem, bindings, |_, _| Ok::<_, ()>(physical_work(6)))
        .expect("repeat physical planning");
    assert_eq!(execution_plan.plan_id(), repeated.plan_id());
    assert_eq!(
        execution_plan.plan_id().as_bytes(),
        [
            50, 82, 44, 253, 147, 108, 140, 234, 107, 182, 127, 134, 177, 23, 66, 27, 6, 38, 149,
            24, 253, 180, 183, 8, 196, 76, 220, 189, 186, 72, 78, 163,
        ]
    );
}

#[test]
fn transaction_seal_rejects_omitted_required_product_staging() {
    let problem = compile(request_with_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual],
    ))
    .expect("two-product logical compilation");
    let product_completion = || {
        BTreeSet::from([WorkDependency::Work(WorkNodeId::new(
            "transaction-stage-psf",
        ))])
    };
    plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, io::Error>(physical_work_with_product_staging(
                6,
                Some(BTreeMap::from([
                    (ProductKind::Psf, product_completion()),
                    (ProductKind::Residual, product_completion()),
                ])),
            ))
        },
    )
    .expect("canonical complete two-product transaction seal");

    let result = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, io::Error>(physical_work_with_product_staging(
                6,
                Some(BTreeMap::from([(ProductKind::Psf, product_completion())])),
            ))
        },
    );

    let error = result.expect_err("one omitted product must fail the exact plan seal");
    assert_eq!(
        error.to_string(),
        "invalid observation transaction plan: required product staging keys {Psf} do not match compiled product requirements {Psf, Residual}"
    );
}

#[test]
fn transaction_seal_blocks_unbound_transaction_work() {
    let problem = compile(request(1)).expect("logical compilation");
    let result = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| {
            Ok::<_, io::Error>(physical_work_with_product_staging(
                6,
                Some(BTreeMap::from([(
                    ProductKind::Psf,
                    BTreeSet::from([WorkDependency::Work(WorkNodeId::new("execute"))]),
                )])),
            ))
        },
    );

    assert!(
        matches!(result, Err(PlanError::ObservationTransaction(_))),
        "the public plan boundary must reject physical work that bypasses transaction sealing"
    );
}

#[test]
fn run_rejects_changed_registry_policy_and_cost_model_bindings() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current =
        |policy, cost_model_id| RunBindings::new(problem.inputs().clone(), policy, cost_model_id);
    let reject = |bindings, registry| execute_plan(&problem, &execution_plan, &bindings, registry);
    let wrong_registry = test_registry(9, 6, None);
    let correct_registry = test_registry(3, 6, None);

    assert!(matches!(
        reject(
            current(&ResourcePolicy::Balanced, cost_model(4)),
            &wrong_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::ImplementationRegistry
        })
    ));
    assert!(matches!(
        reject(
            current(&ResourcePolicy::Exclusive, cost_model(4)),
            &correct_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::ResourcePolicy
        })
    ));
    assert!(matches!(
        reject(
            current(&ResourcePolicy::Balanced, cost_model(9)),
            &correct_registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::PlannerCostModelProfile
        })
    ));
}

#[test]
fn run_rejects_every_stale_problem_input_before_calling_the_executor() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings = PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let execution_plan = plan(&problem, bindings.clone(), |_, _| {
        Ok::<_, ()>(physical_work(6))
    })
    .expect("physical planning");
    let registry = test_registry(3, 6, None);
    let stale_inputs = [
        (
            problem_inputs(9, Vec::new(), ModelStateIdentity::Empty),
            BindingKind::ObservationSnapshot,
        ),
        (
            problem_inputs(
                1,
                vec![(ReferenceDataKind::Measures, identity(9))],
                ModelStateIdentity::Empty,
            ),
            BindingKind::ReferenceDataSnapshots,
        ),
        (
            problem_inputs(1, Vec::new(), ModelStateIdentity::Seed(identity(9))),
            BindingKind::ModelState,
        ),
    ];

    for (inputs, expected) in stale_inputs {
        let stale = RunBindings::new(inputs, &ResourcePolicy::Balanced, cost_model(4));
        let result = execute_plan(&problem, &execution_plan, &stale, &registry);
        assert!(matches!(
            result,
            Err(RunError::BindingMismatch { binding }) if binding == expected
        ));
    }

    let changed_problem = compile(request(9)).expect("changed logical problem");
    let current = RunBindings::new(
        changed_problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let result = execute_plan(&changed_problem, &execution_plan, &current, &registry);
    assert!(matches!(
        result,
        Err(RunError::BindingMismatch {
            binding: BindingKind::CompiledProblem
        })
    ));
    let changed_geometry_problem =
        compile(request_with_geometry(1, geometry(254.0))).expect("changed compiled geometry");
    assert_ne!(
        execution_plan.geometry_id(),
        changed_geometry_problem.geometry().geometry_id()
    );
    let current = RunBindings::new(
        changed_geometry_problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    assert!(matches!(
        execute_plan(
            &changed_geometry_problem,
            &execution_plan,
            &current,
            &registry
        ),
        Err(RunError::BindingMismatch {
            binding: BindingKind::CompiledProblem
        })
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn run_executes_one_exactly_bound_plan_without_routing_or_replanning() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings = PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let execution_plan = plan(&problem, bindings.clone(), |_, _| {
        Ok::<_, ()>(physical_work(6))
    })
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);

    let output =
        execute_plan(&problem, &execution_plan, &current, &registry).expect("exact execution");

    assert_eq!(output, ExecutionOutcome::Succeeded);
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        7,
        "two planned nodes and five mandatory transaction nodes must execute"
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        4,
        "planned and transaction reads plus both publication fences must settle"
    );
}

#[test]
fn run_preserves_the_selected_executors_error_chain() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, Some("selected executor failed"));

    let error =
        execute_plan(&problem, &execution_plan, &current, &registry).expect_err("executor failure");

    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some("selected executor failed")
    );
}

#[test]
fn rejected_post_launch_adaptation_drains_fences_before_returning() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let mut controller = RejectAfterLaunch::default();
    let _guard = run_lock().lock().expect("runtime test lock");

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("an ineligible adaptation must be rejected after safe drain");

    assert!(matches!(error, RunError::Scheduler(_)));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        2
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        1,
        "the launched I/O fence must settle before run returns"
    );
}

#[test]
fn run_applies_an_eligible_transition_to_later_scheduled_work() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(adaptive_physical_work(6)),
    )
    .expect("adaptive physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let mut controller = AdaptAtMajorBoundary::default();
    let _guard = run_lock().lock().expect("runtime test lock");

    let outcome = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("eligible adaptation executes");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert!(controller.applied);
    let observed = registry.executors[&implementation(6)]
        .observed_knobs
        .lock()
        .expect("recorded knobs");
    assert_eq!(observed.len(), 8);
    assert!(
        observed[..4]
            .iter()
            .all(|knobs| knobs == &ExecutionKnobs::serial())
    );
    let mut adapted = ExecutionKnobs::serial();
    adapted.batch_size = 2;
    assert!(observed[4..].iter().all(|knobs| knobs == &adapted));
}

#[test]
fn run_cancellation_at_the_pre_read_cut_releases_authority_capacity() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(
            implementation(6),
            publication_recording_executor(
                6,
                publication_launched,
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let mut cancelling = CancelAfterLaunch::default();
    let _guard = run_lock().lock().expect("runtime test lock");

    let cancelled = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut cancelling,
    )
    .expect("cancellation drains safely");

    assert_eq!(cancelled, ExecutionOutcome::Cancelled);
    assert_eq!(
        visible_generation.load(Ordering::SeqCst),
        0,
        "pre-read cancellation must retain the old visible generation"
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        0,
        "cancellation after consistency checking must prevent the observation read from launching"
    );
    let mut completion = RunToCompletion;
    assert_eq!(
        run(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut completion,
        )
        .expect("released authority admits the same plan again"),
        ExecutionOutcome::Succeeded
    );
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        8
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .fence_waits
            .load(Ordering::SeqCst),
        4
    );
}

#[test]
fn cancellation_cannot_report_cancelled_after_atomic_publication_is_irrevocable() {
    for after_fence in [false, true] {
        let problem = compile(request(1)).expect("logical compilation");
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
        .expect("physical planning");
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let publication_launched = Arc::new(AtomicBool::new(false));
        let visible_generation = Arc::new(AtomicUsize::new(0));
        let registry = TestRegistry {
            id: registry(3),
            executors: BTreeMap::from([(
                implementation(6),
                publication_recording_executor(
                    6,
                    Arc::clone(&publication_launched),
                    Arc::clone(&visible_generation),
                ),
            )]),
        };
        let mut controller = CancelAtPublicationState {
            publication_launched,
            visible_generation: Arc::clone(&visible_generation),
            after_fence,
            requested: false,
        };
        let _guard = run_lock().lock().expect("runtime test lock");

        let outcome = run(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut controller,
        )
        .expect("late cancellation must settle the committed transaction");

        assert!(
            !controller.requested,
            "controller polling must stop once atomic publication launches"
        );
        assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
        assert_eq!(
            outcome,
            ExecutionOutcome::Succeeded,
            "a cancellation request cannot report Cancelled once publication {}",
            if after_fence {
                "has completed"
            } else {
                "has launched"
            }
        );
    }
}

#[test]
fn controller_cannot_adapt_after_atomic_publication_is_irrevocable() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(
            implementation(6),
            publication_recording_executor(
                6,
                Arc::clone(&publication_launched),
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let mut controller = AdaptAtPublicationLaunch {
        publication_launched,
        requested: false,
    };
    let _guard = run_lock().lock().expect("runtime test lock");

    let outcome = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("publication launch must close the controller seam");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
    assert!(
        !controller.requested,
        "the controller cannot adapt after atomic publication launches"
    );
}

#[test]
fn publication_visibility_is_final_after_fence_and_scheduler_settlement() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let visible_during_settlement = Arc::new(AtomicBool::new(false));
    let mut executor =
        publication_recording_executor(6, publication_launched, Arc::clone(&visible_generation));
    executor.visibility_during_fence_settlement = Some(Arc::clone(&visible_during_settlement));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    let outcome = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("atomic publication executes after settlement");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
    assert!(
        !visible_during_settlement.load(Ordering::SeqCst),
        "the new generation cannot become visible during fallible fence settlement"
    );
}

#[test]
fn earlier_acquired_publication_buffer_is_held_through_publish_and_then_released() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work_with_early_publication_buffer(6)),
    )
    .expect("earlier-acquired publication buffer is valid physical work");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let held_during_publish = Arc::new(AtomicBool::new(false));
    let mut executor = recording_executor(6, None, None);
    executor.publication_buffer_held = Some(Arc::clone(&held_during_publish));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let _guard = run_lock().lock().expect("runtime test lock");

    for attempt in 1..=2 {
        held_during_publish.store(false, Ordering::SeqCst);
        let mut controller = RunToCompletion;
        let outcome = run(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut controller,
        )
        .unwrap_or_else(|error| panic!("publication attempt {attempt} failed: {error}"));

        assert_eq!(outcome, ExecutionOutcome::Succeeded);
        assert!(
            held_during_publish.load(Ordering::SeqCst),
            "the earlier-acquired Publication buffer, slot, permit, and lease must remain held through publish"
        );
    }
}

#[test]
fn transaction_failures_leave_the_old_generation_visible() {
    for (label, failure_node, fence_failure_event, publication_failure) in [
        ("input mutation", Some("transaction-check"), None, None),
        (
            "numerical reconciliation",
            Some("transaction-reconciliation"),
            None,
            None,
        ),
        ("product output", Some("transaction-stage-psf"), None, None),
        (
            "atomic commit publication fence",
            None,
            Some(("transaction-commit", FenceKind::Publication)),
            None,
        ),
        (
            "atomic commit I/O fence",
            None,
            Some(("transaction-commit", FenceKind::Io)),
            None,
        ),
        (
            "atomic publication",
            None,
            None,
            Some("publication failure"),
        ),
    ] {
        let problem = compile(request(1)).expect("logical compilation");
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
        .expect("physical planning");
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let visible_generation = Arc::new(AtomicUsize::new(0));
        let registry = TestRegistry {
            id: registry(3),
            executors: BTreeMap::from([(
                implementation(6),
                failing_transaction_executor(
                    6,
                    Arc::clone(&visible_generation),
                    failure_node,
                    fence_failure_event,
                    publication_failure,
                ),
            )]),
        };
        let mut completion = RunToCompletion;
        let _guard = run_lock().lock().expect("runtime test lock");

        run(
            &problem,
            &execution_plan,
            &current,
            &registry,
            authority(),
            &mut completion,
        )
        .expect_err(label);

        assert_eq!(
            visible_generation.load(Ordering::SeqCst),
            0,
            "{label} cannot expose staged output"
        );
    }

    let problem = compile(request_with_model_write(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work_with_model_staging(6)),
    )
    .expect("model-write physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let model_registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(
            implementation(6),
            failing_transaction_executor(
                6,
                Arc::clone(&visible_generation),
                None,
                Some(("transaction-stage-model", FenceKind::Writeback)),
                None,
            ),
        )]),
    };
    let mut completion = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run(
        &problem,
        &execution_plan,
        &current,
        &model_registry,
        authority(),
        &mut completion,
    )
    .expect_err("model-column writeback failure");

    assert_eq!(
        visible_generation.load(Ordering::SeqCst),
        0,
        "model-column writeback failure cannot expose staged output"
    );

    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let admission_registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(
            implementation(6),
            failing_transaction_executor(6, Arc::clone(&visible_generation), None, None, None),
        )]),
    };
    let mut completion = RunToCompletion;
    authority()
        .update_external_pressure(runtime_inventory(0).pressure)
        .expect("install zero-lock external pressure");

    let result = run(
        &problem,
        &execution_plan,
        &current,
        &admission_registry,
        authority(),
        &mut completion,
    );
    authority()
        .update_external_pressure(runtime_inventory(4).pressure)
        .expect("restore external pressure");
    let error = result.expect_err("resource admission must fail before transaction work");

    assert!(matches!(
        error,
        RunError::Scheduler(ExecutionError::Resource(ResourceError::Infeasible {
            ref resource,
            required: 1,
            available: 0,
        })) if resource == "locks"
    ));
    assert_eq!(visible_generation.load(Ordering::SeqCst), 0);
    assert_eq!(
        admission_registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0,
        "failed admission cannot launch mutation or publication work"
    );
}

#[test]
fn release_failures_drain_independent_fences_and_quarantine_only_failed_slots() {
    let problem = compile(request(1)).expect("logical compilation");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let _guard = run_lock().lock().expect("runtime test lock");

    for fail_at_fence in [false, true] {
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
            |_, _| Ok::<_, ()>(release_failure_physical_work(6, 8, fail_at_fence)),
        )
        .expect("external-release failure planning");
        let mut release_executor = recording_executor(
            8,
            (!fail_at_fence).then_some("release execute failed"),
            fail_at_fence.then_some("release fence failed"),
        );
        if fail_at_fence {
            release_executor.measurements.insert(
                WorkNodeId::new("a-release-mapping"),
                (
                    vec![IoMeasurement::new(IoBufferKind::MappedPageCache, 100, 1)],
                    Vec::new(),
                ),
            );
        }
        let mut prepare_executor = recording_executor(6, None, None);
        prepare_executor.measurements.insert(
            WorkNodeId::new(if fail_at_fence {
                "0-prepare-mapping"
            } else {
                "1-prepare-mapping"
            }),
            (
                vec![IoMeasurement::new(IoBufferKind::MappedPageCache, 100, 1)],
                Vec::new(),
            ),
        );
        let executor_registry = TestRegistry {
            id: registry(3),
            executors: BTreeMap::from([
                (implementation(6), prepare_executor),
                (implementation(8), release_executor),
            ]),
        };
        let mut controller = RunToCompletion;

        let error = run(
            &problem,
            &execution_plan,
            &current,
            &executor_registry,
            authority(),
            &mut controller,
        )
        .expect_err("failed external release must remain the primary error after drain");

        assert!(
            matches!(
                &error,
                RunError::Execution { node, .. }
                    if node.as_str().contains("release-mapping")
            ),
            "unexpected release failure: {error:?}"
        );
        assert_eq!(
            executor_registry.executors[&implementation(6)]
                .fence_waits
                .load(Ordering::SeqCst),
            2,
            "the transaction-read and independent I/O fences must drain after a Release failure"
        );

        let readmission_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
        .expect("post-quarantine planning");
        let mut completion = RunToCompletion;
        assert_eq!(
            run(
                &problem,
                &readmission_plan,
                &current,
                &executor_registry,
                authority(),
                &mut completion,
            )
            .expect("only the failed physical slot remains quarantined"),
            ExecutionOutcome::Succeeded
        );
    }
}

#[test]
fn run_persists_a_reopenable_receipt_with_exact_identities_and_every_plan_node() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([9; 32]),
        BuildIdentity::from_sha256([10; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    let outcome = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen durable receipt");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert_eq!(receipt.schema_version(), 1);
    assert_eq!(receipt.status(), ReceiptStatus::Completed);
    assert_eq!(receipt.plan_identity(), execution_plan.plan_id().as_bytes());
    assert_eq!(receipt.problem_identity(), problem.problem_id().as_bytes());
    assert_eq!(
        receipt.geometry_identity(),
        problem.geometry().geometry_id().as_bytes()
    );
    assert_eq!(
        receipt.observation_identity(),
        problem.inputs().observation().identity().as_bytes()
    );
    assert_eq!(
        receipt.implementation_registry_identity(),
        registry.registry_id().as_bytes()
    );
    assert_eq!(
        receipt.resource_policy_identity(),
        execution_plan.resource_policy_id().as_bytes()
    );
    assert_eq!(receipt.cost_model_identity(), cost_model(4).as_bytes());
    assert_eq!(
        receipt.dag_identity(),
        execution_plan.physical_work_id().as_bytes()
    );
    assert_eq!(
        receipt.plan_node_count(),
        execution_plan.execution_dag().nodes().len()
    );
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Completed)
    );
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Completed)
    );
}

#[test]
fn effective_problem_projection_normalizes_signed_zero_like_canonical_identities() {
    let compile_with_robust = |robust| {
        compile(request_with_geometry_references_and_weighting(
            81,
            geometry(255.0),
            Vec::new(),
            WeightingContract::new(
                WeightingScheme::Briggs { robust },
                WeightDensityScope::GlobalSelection,
            ),
        ))
        .expect("logical compilation")
    };
    let positive_zero = compile_with_robust(0.0);
    let negative_zero = compile_with_robust(-0.0);

    assert_eq!(positive_zero.problem_id(), negative_zero.problem_id());
    assert_eq!(
        positive_zero.weighting().generation_id(),
        negative_zero.weighting().generation_id()
    );

    let positive_projection = CompiledProblemEvidence::project(&positive_zero);
    let negative_projection = CompiledProblemEvidence::project(&negative_zero);
    assert_eq!(
        positive_projection.field("weighting.scheme.robust"),
        Some("f64:0000000000000000")
    );
    assert_eq!(positive_projection, negative_projection);
}

#[test]
fn receipt_reopens_the_complete_versioned_effective_problem_projection() {
    let problem = compile(request_with_geometry_and_references(
        81,
        geometry(255.0),
        vec![(ReferenceDataKind::Measures, identity(82))],
    ))
    .expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([83; 32]),
        BuildIdentity::from_sha256([84; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let reopened = receipts.open(provenance.attempt_id()).expect("receipt");
    let projected = reopened.compiled_problem_evidence();
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let data_generation = source
        .generations()
        .columns()
        .generation(MsColumnKind::Data)
        .expect("data generation")
        .to_string();
    let antenna_generation = source
        .generations()
        .metadata(MetadataTableKind::Antenna)
        .expect("antenna generation")
        .to_string();

    assert_eq!(projected.schema_version(), 2);
    assert_eq!(projected, &CompiledProblemEvidence::project(&problem));
    assert_eq!(
        projected.field("science.spectral.sampling"),
        Some("identity")
    );
    assert_eq!(
        projected.field("science.measurement_equation.instrument_response"),
        Some("scalar")
    );
    assert_eq!(
        projected.field("science.measurement_equation.inner_products.model"),
        Some("hermitian_euclidean")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.0.kind"),
        Some("spectral_basis")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.1.kind"),
        Some("polarization")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.2.kind"),
        Some("direction_dependent_response")
    );
    assert_eq!(
        projected.field("science.measurement_equation.operator.transforms.3.kind"),
        Some("phase")
    );
    assert_eq!(
        projected.field("science.normal_equation.output.normalization"),
        Some("unnormalized")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.0"),
        Some("right_hand_side_a_star_w_d")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.1"),
        Some("residual_a_star_w_d_minus_a_x")
    );
    assert_eq!(
        projected.field("science.normal_equation.forms.2"),
        Some("normal_operator_a_star_w_a")
    );
    assert_eq!(
        projected.field("reconstruction.algorithm.kind"),
        Some("dirty")
    );
    assert_eq!(projected.field("weighting.scheme.kind"), Some("natural"));
    assert_eq!(
        projected.field("weighting.generation.identity"),
        Some(
            problem
                .normal_equation()
                .weighting()
                .generation_id()
                .to_string()
                .as_str()
        )
    );
    assert_eq!(
        projected.field("weighting.sources.0.flag_policy"),
        Some("flag_or_flag_row")
    );
    assert_eq!(
        projected.field("weighting.generation.snapshot_identity"),
        Some(problem.inputs().observation().to_string().as_str())
    );
    assert_eq!(
        projected.field("weighting.sources.0.input_weight_column"),
        Some("weight")
    );
    assert_eq!(projected.field("products.requested.0"), Some("psf"));
    assert_eq!(
        projected.field("products.normalization_boundary.input"),
        Some("unnormalized")
    );
    assert_eq!(
        projected.field("products.normalization_boundary.operations.0.kind"),
        Some("normalize")
    );
    assert_eq!(
        projected.field("products.normalization_boundary.operations.1.kind"),
        Some("convert_units")
    );
    assert_eq!(projected.field("numerics.reduction"), Some("compensated"));
    assert_eq!(
        projected.field("geometry.domains.0.direction.projection"),
        Some("sin")
    );
    assert_eq!(
        projected.field("observation.sources.0.selection.rows.selected_count"),
        Some("1")
    );
    assert_eq!(
        projected.field("observation.sources.0.generations.columns.data"),
        Some(data_generation.as_str())
    );
    assert_eq!(
        projected.field("observation.sources.0.generations.metadata.antenna"),
        Some(antenna_generation.as_str())
    );
    assert_eq!(
        projected.field("observation.reference_data.measures"),
        Some(identity(82).to_string().as_str())
    );
    assert_eq!(projected.field("observation.model.kind"), Some("empty"));
    assert!(projected.fields().len() > 80);
}

#[test]
fn receipt_reopens_the_complete_selected_plan_projection() {
    let problem = compile(request_with_geometry_and_references(
        1,
        geometry(255.0),
        vec![(ReferenceDataKind::Measures, identity(71))],
    ))
    .expect("logical compilation");
    let policy = ResourcePolicy::Explicit(ResourceOverride {
        memory_bytes: BTreeMap::from([(CapacityDomainId::new("host-memory"), 512)]),
        workers: Some(2),
        storage_bytes: BTreeMap::new(),
        rates_per_second: BTreeMap::from([(RateResourceId::new("io-rate"), 8)]),
        cache_bytes: Some(256),
        locks: Some(2),
        file_descriptors: Some(8),
        queue_slots: BTreeMap::from([(QueueResourceId::new("io-queue"), 2)]),
        accelerator_slots: BTreeMap::new(),
    });
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), policy.clone(), cost_model(4)),
        |_, _| Ok::<_, ()>(auditable_physical_work(6)),
    )
    .expect("auditable physical planning");
    let current = RunBindings::new(problem.inputs().clone(), &policy, cost_model(4));
    let cache_artifact = ArtifactIdentity::from_sha256([51; 32]);
    let mut executor = recording_executor(6, None, None);
    executor.measurements = BTreeMap::from([(
        WorkNodeId::new("first-major-work"),
        (
            Vec::new(),
            vec![ArtifactMeasurement::new(
                cache_artifact,
                Some(cache_artifact),
                ArtifactDisposition::Built,
                64,
                None,
            )],
        ),
    )]);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([61; 32]),
        BuildIdentity::from_sha256([62; 32]),
    );
    let mut controller = AdaptAtMajorBoundary::default();
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let dag = execution_plan.execution_dag();
    let adaptation_id = AdaptationId::new("larger-batch");
    let adaptation = receipt
        .adaptation_projection(&adaptation_id)
        .expect("adaptation projection");

    assert_eq!(receipt.attempt_id(), provenance.attempt_id());
    assert_eq!(receipt.build_identity(), provenance.build_identity());
    assert_eq!(
        receipt.reference_identity(ReferenceDataKind::Measures),
        Some(identity(71).as_bytes())
    );
    assert_eq!(receipt.model_identity(), ModelStateIdentity::Empty);
    assert_eq!(
        receipt.numerics_identity(),
        problem.numerics_id().as_bytes()
    );
    assert_eq!(receipt.projected_resource_policy(), policy);
    assert_eq!(
        receipt.selected_alternative_projection(),
        dag.resource_alternative().clone()
    );
    assert_eq!(
        receipt.required_resource_capability_identities(),
        dag.required_resource_capabilities().clone()
    );
    assert_eq!(
        receipt.selected_implementation_identities(),
        dag.selected_implementations().clone()
    );
    assert_eq!(
        receipt.plan_node_identities(),
        dag.nodes().keys().cloned().collect()
    );
    assert_eq!(
        receipt.allocation_generation_identities(),
        dag.logical_allocations().keys().cloned().collect()
    );
    for node in dag.nodes().values() {
        let expected = node
            .allocations
            .iter()
            .map(|usage| (usage.allocation.clone(), usage.lifetime.clone()))
            .collect();
        assert_eq!(receipt.allocation_uses(&node.id), Some(expected));
    }
    assert_eq!(
        receipt.physical_slot_identities(),
        dag.physical_slots().keys().cloned().collect()
    );
    assert_eq!(
        receipt.artifact_identities(),
        BTreeSet::from([cache_artifact])
    );
    assert_eq!(
        receipt.cache_identities(),
        BTreeSet::from([CacheIdentity::from_sha256([52; 32])])
    );
    assert_eq!(
        receipt.initial_execution_knobs(),
        dag.initial_knobs().clone()
    );
    assert_eq!(
        receipt.adaptation_identities(),
        dag.adaptations().keys().cloned().collect()
    );
    assert_eq!(adaptation.transition(), &dag.adaptations()[&adaptation_id]);
    assert!(adaptation.was_applied());
    assert!(adaptation.applied_revision().is_some());
}

#[test]
fn receipt_compares_plan_predictions_with_actual_stage_resource_and_fence_use() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([11; 32]),
        BuildIdentity::from_sha256([12; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let read = WorkNodeId::new("read");
    let io_fence = FenceId::new(read.clone(), FenceKind::Io);

    assert_eq!(receipt.predicted_elapsed_nanos(), 200);
    assert_eq!(receipt.prediction_confidence_ppm(), 900_000);
    assert_eq!(receipt.prediction_uncertainty_count(), 1);
    assert_eq!(receipt.stage_predicted_elapsed_nanos(&read), Some(100));
    assert!(receipt.stage_actual_elapsed_nanos(&read).is_some());
    assert_eq!(
        receipt.planned_resource_amount(
            &read,
            &casa_imaging_runtime::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            &ClaimLifetime::through_fence(FenceKind::Io),
        ),
        Some(1)
    );
    assert_eq!(
        receipt.actual_resource_peak(
            &read,
            &casa_imaging_runtime::LeaseResource::Rate {
                demand_id: "io-rate".to_string(),
            },
            &ClaimLifetime::through_fence(FenceKind::Io),
        ),
        Some(1)
    );
    assert_eq!(
        receipt.fence_status(&io_fence),
        Some(ReceiptStatus::Completed)
    );
    assert!(receipt.fence_actual_elapsed_nanos(&io_fence).is_some());
}

#[test]
fn receipt_compares_planned_and_actual_io_artifacts_and_never_persists_paths() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(evidenced_physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let input = ArtifactIdentity::from_sha256([31; 32]);
    let cache = ArtifactIdentity::from_sha256([32; 32]);
    let output = ArtifactIdentity::from_sha256([34; 32]);
    let input_path = RedactedPath::from_path("/Users/private/secret-source.ms");
    let output_path = RedactedPath::from_path("/Volumes/private/secret-image.table");
    let mut executor = recording_executor(6, None, None);
    executor.measurements = BTreeMap::from([
        (
            WorkNodeId::new("read"),
            (
                vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
                vec![
                    ArtifactMeasurement::new(
                        input,
                        Some(input),
                        ArtifactDisposition::Loaded,
                        4_096,
                        Some(input_path),
                    ),
                    ArtifactMeasurement::new(
                        cache,
                        Some(ArtifactIdentity::from_sha256([35; 32])),
                        ArtifactDisposition::RejectedStale,
                        1_024,
                        None,
                    ),
                ],
            ),
        ),
        (
            WorkNodeId::new("publish"),
            (
                vec![IoMeasurement::new(IoBufferKind::Publication, 2_048, 1)],
                vec![ArtifactMeasurement::new(
                    output,
                    Some(ArtifactIdentity::from_sha256([36; 32])),
                    ArtifactDisposition::Published,
                    2_048,
                    Some(output_path),
                )],
            ),
        ),
    ]);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([37; 32]),
        BuildIdentity::from_sha256([38; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let read = WorkNodeId::new("read");
    let publish = WorkNodeId::new("publish");

    assert_eq!(
        receipt.stage_predicted_io(&read, IoBufferKind::SourceReadAhead),
        Some((8_192, 4))
    );
    assert_eq!(
        receipt.stage_actual_io(&read, IoBufferKind::SourceReadAhead),
        Some((4_096, 2))
    );
    assert_eq!(
        receipt.stage_predicted_io(&publish, IoBufferKind::Publication),
        Some((2_048, 1))
    );
    assert_eq!(
        receipt.stage_actual_io(&publish, IoBufferKind::Publication),
        Some((2_048, 1))
    );
    assert_eq!(receipt.artifact_count(), 3);
    assert_eq!(
        receipt.artifact_disposition(input),
        Some(ArtifactDisposition::Loaded)
    );
    assert_eq!(receipt.artifact_role(output), Some(ArtifactRole::Output));
    assert_eq!(
        receipt.artifact_node(output),
        Some(WorkNodeId::new("publish"))
    );
    assert_eq!(receipt.artifact_actual_bytes(output), Some(2_048));
    assert_eq!(
        receipt.artifact_disposition(cache),
        Some(ArtifactDisposition::RejectedStale)
    );
    assert_eq!(
        receipt.artifact_cache_identity(cache),
        Some(CacheIdentity::from_sha256([33; 32]).as_bytes())
    );
    assert_eq!(
        receipt.artifact_observed_identity(output),
        Some(ArtifactIdentity::from_sha256([36; 32]).as_bytes())
    );
    assert_eq!(
        receipt.artifact_path_identity(input),
        Some(input_path.as_bytes())
    );
    assert_eq!(
        receipt.artifact_path_identity(output),
        Some(output_path.as_bytes())
    );
    let persisted = std::fs::read_to_string(
        std::fs::read_dir(directory.path())
            .expect("receipt directory listing")
            .next()
            .expect("receipt file")
            .expect("receipt entry")
            .path(),
    )
    .expect("serialized receipt");
    assert!(!persisted.contains("secret-source.ms"));
    assert!(!persisted.contains("secret-image.table"));
}

#[test]
fn failed_publication_fence_never_records_a_published_output() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(evidenced_physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let input = ArtifactIdentity::from_sha256([31; 32]);
    let cache = ArtifactIdentity::from_sha256([32; 32]);
    let output = ArtifactIdentity::from_sha256([34; 32]);
    let staged_output = ArtifactIdentity::from_sha256([36; 32]);
    let mut executor = recording_executor(6, None, Some("publication fence failed"));
    executor.fail_only_fence = Some(FenceKind::Publication);
    executor.measurements = BTreeMap::from([
        (
            WorkNodeId::new("read"),
            (
                vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
                vec![
                    ArtifactMeasurement::new(
                        input,
                        Some(input),
                        ArtifactDisposition::Loaded,
                        4_096,
                        None,
                    ),
                    ArtifactMeasurement::new(
                        cache,
                        None,
                        ArtifactDisposition::RejectedStale,
                        0,
                        None,
                    ),
                ],
            ),
        ),
        (
            WorkNodeId::new("publish"),
            (
                vec![IoMeasurement::new(IoBufferKind::Publication, 2_048, 1)],
                vec![ArtifactMeasurement::new(
                    output,
                    Some(staged_output),
                    ArtifactDisposition::Published,
                    2_048,
                    None,
                )],
            ),
        ),
    ]);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("receipt directory");
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(1, 1_048_576).expect("retention"),
    )
    .expect("receipt store");
    let provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([87; 32]),
        BuildIdentity::from_sha256([88; 32]),
    );
    let mut controller = RunToCompletion;
    let _guard = run_lock().lock().expect("runtime test lock");

    let error = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance),
    )
    .expect_err("publication fence failure must fail the run");
    assert!(matches!(
        error,
        RunError::Execution { ref node, .. } if node == &WorkNodeId::new("publish")
    ));

    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("failed receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(receipt.artifact_disposition(output), None);
    assert_eq!(
        receipt.artifact_observed_identity(output),
        Some(staged_output.as_bytes())
    );
    assert_eq!(receipt.artifact_actual_bytes(output), Some(2_048));
    assert_eq!(
        receipt.fence_status(&FenceId::new(
            WorkNodeId::new("publish"),
            FenceKind::Publication,
        )),
        Some(ReceiptStatus::Failed)
    );
}

#[test]
fn receipts_preserve_typed_terminal_outcomes_and_every_node_state() {
    let problem = compile(request(1)).expect("logical compilation");
    let balanced_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let store = |root: &std::path::Path| {
        ExecutionReceiptStore::new(
            root,
            ReceiptRetention::new(8, 1_048_576).expect("bounded retention"),
        )
        .expect("receipt store")
    };
    let _guard = run_lock().lock().expect("runtime test lock");

    let failed_directory = tempfile::tempdir().expect("failed receipt directory");
    let failed_receipts = store(failed_directory.path());
    let failed_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([41; 32]),
        BuildIdentity::from_sha256([42; 32]),
    );
    let failed_registry = test_registry(3, 6, Some("adapter failed"));
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &failed_registry,
            authority(),
            &mut completion,
            failed_receipts.bind(failed_provenance),
        ),
        Err(RunError::Execution { .. })
    ));
    let failed = failed_receipts
        .open(failed_provenance.attempt_id())
        .expect("failed receipt");
    assert_eq!(failed.status(), ReceiptStatus::Failed);
    assert_eq!(failed.failure_kind(), Some(ReceiptFailureKind::Adapter));
    assert_eq!(failed.failure_node(), Some(WorkNodeId::new("read")));
    assert_eq!(
        failed.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Failed)
    );
    assert_eq!(
        failed.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Cancelled)
    );

    let cancelled_directory = tempfile::tempdir().expect("cancelled receipt directory");
    let cancelled_receipts = store(cancelled_directory.path());
    let cancelled_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([43; 32]),
        BuildIdentity::from_sha256([44; 32]),
    );
    let successful_registry = test_registry(3, 6, None);
    let mut cancellation = CancelAfterLaunch::default();
    assert_eq!(
        run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &successful_registry,
            authority(),
            &mut cancellation,
            cancelled_receipts.bind(cancelled_provenance),
        )
        .expect("cancelled execution"),
        ExecutionOutcome::Cancelled
    );
    let cancelled = cancelled_receipts
        .open(cancelled_provenance.attempt_id())
        .expect("cancelled receipt");
    assert_eq!(cancelled.status(), ReceiptStatus::Cancelled);
    assert_eq!(cancelled.failure_kind(), None);
    assert_eq!(
        cancelled.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Completed)
    );
    assert_eq!(
        cancelled.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Cancelled)
    );

    let mutation_directory = tempfile::tempdir().expect("mutation receipt directory");
    let mutation_receipts = store(mutation_directory.path());
    let mutation_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([45; 32]),
        BuildIdentity::from_sha256([46; 32]),
    );
    let changed = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Interactive,
        cost_model(4),
    );
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &balanced_plan,
            &changed,
            &successful_registry,
            authority(),
            &mut completion,
            mutation_receipts.bind(mutation_provenance),
        ),
        Err(RunError::BindingMismatch { .. })
    ));
    let mutation = mutation_receipts
        .open(mutation_provenance.attempt_id())
        .expect("mutation receipt");
    assert_eq!(mutation.status(), ReceiptStatus::Mutation);
    assert_eq!(
        mutation.failure_kind(),
        Some(ReceiptFailureKind::BindingMutation)
    );
    assert_eq!(
        mutation.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::NotStarted)
    );

    let constrained_policy = ResourcePolicy::Explicit(ResourceOverride {
        workers: Some(0),
        ..ResourceOverride::default()
    });
    let infeasible_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), constrained_policy.clone(), cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("infeasible physical plan remains recordable");
    let infeasible_current =
        RunBindings::new(problem.inputs().clone(), &constrained_policy, cost_model(4));
    let infeasible_directory = tempfile::tempdir().expect("infeasible receipt directory");
    let infeasible_receipts = store(infeasible_directory.path());
    let infeasible_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([47; 32]),
        BuildIdentity::from_sha256([48; 32]),
    );
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &infeasible_plan,
            &infeasible_current,
            &successful_registry,
            authority(),
            &mut completion,
            infeasible_receipts.bind(infeasible_provenance),
        ),
        Err(RunError::Scheduler(
            casa_imaging_runtime::ExecutionError::Resource(_)
        ))
    ));
    let infeasible = infeasible_receipts
        .open(infeasible_provenance.attempt_id())
        .expect("infeasible receipt");
    assert_eq!(infeasible.status(), ReceiptStatus::Infeasible);
    assert_eq!(
        infeasible.failure_kind(),
        Some(ReceiptFailureKind::ResourceInfeasible)
    );
    assert_eq!(
        infeasible.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::NotStarted)
    );

    let aborted_directory = tempfile::tempdir().expect("aborted receipt directory");
    let aborted_receipts = store(aborted_directory.path());
    let aborted_provenance = ExecutionProvenance::new(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([49; 32]),
        BuildIdentity::from_sha256([50; 32]),
    );
    let mut interrupted_executor = recording_executor(6, None, None);
    interrupted_executor.panic_on_execute = true;
    let interrupted_registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), interrupted_executor)]),
    };
    let mut completion = RunToCompletion;
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &interrupted_registry,
            authority(),
            &mut completion,
            aborted_receipts.bind(aborted_provenance),
        );
    }));
    assert!(interrupted.is_err());
    let aborted = aborted_receipts
        .open(aborted_provenance.attempt_id())
        .expect("aborted receipt");
    assert_eq!(aborted.status(), ReceiptStatus::Aborted);
    assert_eq!(
        aborted.failure_kind(),
        Some(ReceiptFailureKind::Interrupted)
    );
    assert_eq!(aborted.failure_node(), Some(WorkNodeId::new("read")));
    assert_eq!(
        aborted.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Aborted)
    );
    assert_eq!(
        aborted.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::NotStarted)
    );
}
