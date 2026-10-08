// SPDX-License-Identifier: LGPL-3.0-or-later

//! Registries, run controllers, the deterministic resource authority and
//! the plan/run entry points the compile/plan/run suite drives.

use super::*;

pub(crate) fn publication_capable_executor(
    problem: &casa_imaging_model::CompiledProblem,
    implementation_byte: u8,
) -> RecordingExecutor {
    let mut executor = recording_executor(implementation_byte, None, None);
    executor.major_cycle_problem = Some(problem.clone());
    executor
}

pub(crate) fn test_registry(
    problem: &casa_imaging_model::CompiledProblem,
    registry_byte: u8,
    implementation_byte: u8,
    failure: Option<&'static str>,
) -> TestRegistry {
    let mut executor = publication_capable_executor(problem, implementation_byte);
    executor.failure = failure;
    TestRegistry {
        id: registry(registry_byte),
        metadata: implementation_metadata(problem),
        executors: BTreeMap::from([(implementation(implementation_byte), executor)]),
    }
}

pub(crate) struct TestRegistry {
    pub(crate) id: ImplementationRegistryId,
    pub(crate) metadata: ImplementationContractMetadata,
    pub(crate) executors: BTreeMap<WorkImplementationId, RecordingExecutor>,
}

impl TestRegistry {
    pub(crate) fn metadata_for(
        problem: &casa_imaging_model::CompiledProblem,
    ) -> ImplementationContractMetadata {
        implementation_metadata(problem)
    }
}

pub(crate) struct ContractOnlyRegistry {
    id: ImplementationRegistryId,
    metadata: ImplementationContractMetadata,
    pub(crate) executors: BTreeMap<WorkImplementationId, RecordingExecutor>,
}

impl ContractOnlyRegistry {
    pub(crate) fn new(
        id: ImplementationRegistryId,
        metadata: ImplementationContractMetadata,
        implementation_ids: impl IntoIterator<Item = WorkImplementationId>,
    ) -> Self {
        let executors = implementation_ids
            .into_iter()
            .map(|implementation_id| {
                let mut executor = recording_executor(0, None, None);
                executor.id = implementation_id.clone();
                (implementation_id, executor)
            })
            .collect();
        Self {
            id,
            metadata,
            executors,
        }
    }
}

#[derive(Default)]
pub(crate) struct RejectAfterLaunch {
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
pub(crate) struct AdaptAtMajorBoundary {
    pub(crate) applied: bool,
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
pub(crate) struct SelectStreamedRoute {
    pub(crate) applied: bool,
}

impl RunController for SelectStreamedRoute {
    fn directive(&mut self, status: &ExecutionStatus) -> RunDirective {
        let adaptation = AdaptationId::new("select-streamed-route");
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
pub(crate) struct CancelAfterLaunch {
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

pub(crate) struct CancelAtPublicationState {
    pub(crate) publication_launched: Arc<AtomicBool>,
    pub(crate) visible_generation: Arc<AtomicUsize>,
    pub(crate) after_fence: bool,
    pub(crate) requested: bool,
}

pub(crate) struct AdaptAtPublicationLaunch {
    pub(crate) publication_launched: Arc<AtomicBool>,
    pub(crate) requested: bool,
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

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        self.executors
            .contains_key(id)
            .then(|| self.metadata.clone())
    }
}

impl ImplementationRegistry for ContractOnlyRegistry {
    type Implementation = RecordingExecutor;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        self.executors.get(id)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        self.executors
            .contains_key(id)
            .then(|| self.metadata.clone())
    }
}

pub(crate) fn authority() -> &'static ResourceAuthority {
    static AUTHORITY: OnceLock<&'static ResourceAuthority> = OnceLock::new();
    AUTHORITY.get_or_init(|| {
        ResourceAuthority::install_production_inventory(runtime_inventory(4))
            .expect("install deterministic runtime inventory")
    })
}

pub(crate) fn runtime_inventory(available_locks: u64) -> HostInventory {
    let domain = CapacityDomainId::new("host-memory");
    let view = CapacityViewId::new("host-memory");
    let rate = RateResourceId::new("io-rate");
    let operations_rate = RateResourceId::new("io-operations-rate");
    let queue = QueueResourceId::new("io-queue");
    let transaction_rate = RateResourceId::new("transaction-io-rate");
    let transaction_queue = QueueResourceId::new("transaction-io-queue");
    let storage = StorageDomainId::new("atomic-output");
    let source_storage = StorageDomainId::new("prepared-source-secondary");
    HostInventory {
        topology: ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 2 * 1_048_576,
            }],
            memory_views: vec![MemoryView {
                id: view,
                domain: domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: vec![
                StorageDomain {
                    id: storage.clone(),
                    root: PathBuf::from("/tmp/casa-rs-imaging-runtime-tests"),
                    capacity_bytes: 1_048_576,
                    read_rate: rate.clone(),
                    write_rate: rate.clone(),
                    operations_rate: Some(operations_rate.clone()),
                    queue: queue.clone(),
                },
                StorageDomain {
                    id: source_storage.clone(),
                    root: PathBuf::from("/tmp/casa-rs-imaging-runtime-source-tests"),
                    capacity_bytes: 1_048_576,
                    read_rate: rate.clone(),
                    write_rate: rate.clone(),
                    operations_rate: Some(operations_rate.clone()),
                    queue: queue.clone(),
                },
            ],
            rate_resources: vec![
                RateResource::new(rate.clone(), RateUnit::BytesPerSecond, 16),
                RateResource::new(operations_rate.clone(), RateUnit::OperationsPerSecond, 16),
                RateResource::new(transaction_rate.clone(), RateUnit::BytesPerSecond, 16),
            ],
            queue_resources: vec![
                QueueResource::new(queue.clone(), 8),
                QueueResource::new(transaction_queue.clone(), 4),
            ],
            logical_cpu_threads: 4,
            native_thread_stack_bytes: 512 << 10,
            page_bytes: 16 << 10,
            performance_cpu_cores: CpuClassCapacity::Known(4),
            cache_capacity_bytes: 1_048_576,
            lock_capacity: 4,
            file_descriptor_capacity: 32,
        },
        pressure: ExternalPressure {
            memory_available_bytes: BTreeMap::from([(domain, 2 * 1_048_576)]),
            available_cpu_threads: 4,
            storage_available_bytes: BTreeMap::from([
                (storage, 1_048_576),
                (source_storage, 1_048_576),
            ]),
            rate_available_per_second: BTreeMap::from([
                (rate, 16),
                (operations_rate, 16),
                (transaction_rate, 16),
            ]),
            queue_available_slots: BTreeMap::from([(queue, 8), (transaction_queue, 4)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1_048_576,
            available_locks,
            available_file_descriptors: 32,
        },
    }
}

pub(crate) fn run_lock() -> &'static Mutex<()> {
    static RUN_LOCK: Mutex<()> = Mutex::new(());
    &RUN_LOCK
}

pub(crate) fn plan<E>(
    problem: &casa_imaging_model::CompiledProblem,
    bindings: PlanningBindings,
    planner: impl FnOnce(
        &casa_imaging_model::CompiledProblem,
        &PlanningBindings,
    ) -> Result<PhysicalWorkBinding, E>,
) -> Result<casa_imaging_runtime::ExecutionPlan, PlanError<E>> {
    let directory = tempfile::tempdir().expect("empty receipt directory");
    let root = directory.keep();
    let receipts = ExecutionReceiptStore::new(
        root,
        ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("empty receipt store");
    plan_with_receipts(problem, bindings, &receipts, planner)
}

pub(crate) fn plan_with_receipts<E>(
    problem: &casa_imaging_model::CompiledProblem,
    bindings: PlanningBindings,
    receipts: &ExecutionReceiptStore,
    planner: impl FnOnce(
        &casa_imaging_model::CompiledProblem,
        &PlanningBindings,
    ) -> Result<PhysicalWorkBinding, E>,
) -> Result<casa_imaging_runtime::ExecutionPlan, PlanError<E>> {
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let candidate = planner(problem, &bindings).map_err(PlanError::Planner)?;
    let implementation_ids = candidate
        .execution_dag()
        .nodes()
        .values()
        .map(|node| node.implementation.clone());
    let registry = ContractOnlyRegistry::new(
        bindings.implementation_registry_id(),
        implementation_metadata(problem),
        implementation_ids,
    );
    let candidates = vec![candidate];
    runtime_plan(
        problem,
        bindings,
        authority(),
        &registry,
        receipts,
        move |_, _| Ok(candidates),
    )
}

pub(crate) fn execution_provenance(
    attempt: casa_imaging_runtime::ExecutionAttemptId,
    build: BuildIdentity,
) -> ExecutionProvenance {
    ExecutionProvenance::new(attempt, build)
}

pub(crate) fn native_product_physical_work(
    problem: &casa_imaging_model::CompiledProblem,
    catalog: ImplementationContractCatalog,
    execution_dag: ExecutionDag,
    prediction: PlanPrediction,
    artifacts: Vec<PlannedArtifact>,
    observation_transaction: ObservationTransactionWork,
    publication_layouts: PublicationLayoutLedger,
) -> Result<PhysicalWorkBinding, PhysicalWorkBindingError> {
    let publication = publication_plan_for_problem(problem);
    PhysicalWorkBinding::new_with_product_publication(
        catalog,
        execution_dag,
        prediction,
        artifacts,
        observation_transaction,
        publication_layouts,
        &publication,
    )
}

pub(crate) fn run<C: RunController>(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
    authority: &ResourceAuthority,
    controller: &mut C,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let receipts = plan.receipt_store();
    static NEXT_RUN_ATTEMPT: AtomicUsize = AtomicUsize::new(241);
    let attempt_seed =
        u8::try_from(NEXT_RUN_ATTEMPT.fetch_add(1, Ordering::SeqCst) % usize::from(u8::MAX))
            .expect("bounded synthetic attempt seed");
    run_receipted(
        problem,
        plan,
        current,
        registry,
        authority,
        controller,
        receipts.bind(execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([attempt_seed; 32]),
            BuildIdentity::from_sha256([242; 32]),
        )),
    )
}

pub(crate) fn run_receipted<C: RunController>(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
    authority: &ResourceAuthority,
    controller: &mut C,
    receipt: ExecutionReceiptBinding<'_>,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem");
    runtime_run(
        &executable,
        plan,
        current,
        registry,
        authority,
        controller,
        receipt,
    )
}

pub(crate) fn execute_plan(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &casa_imaging_runtime::ExecutionPlan,
    current: &RunBindings,
    registry: &TestRegistry,
) -> Result<ExecutionOutcome, RunError<io::Error>> {
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
