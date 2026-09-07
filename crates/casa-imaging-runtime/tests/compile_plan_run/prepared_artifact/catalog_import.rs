// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_imaging_runtime::{
    PreparedArtifactImportSegment, PreparedArtifactImportSource, PreparedArtifactImporter,
};

pub(super) struct CatalogImport {
    sources: Vec<Option<PreparedArtifactImportSource>>,
    started: Arc<AtomicBool>,
    completed: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    fail_at: Option<usize>,
    disrupt_receipt_root: Option<PathBuf>,
}

impl CatalogImport {
    pub(super) fn execute(
        &self,
        store: &PreparedArtifactStore,
        descriptors: &[PreparedArtifactDescriptor],
        context: WorkExecutionContext<'_>,
    ) -> Result<WorkMeasurements, io::Error> {
        self.started.store(true, Ordering::SeqCst);
        let (_, measurements) = store
            .import_catalog(&context, descriptors, &self.sources, |index| {
                assert_eq!(
                    self.active.load(Ordering::SeqCst),
                    0,
                    "the preceding importer is dropped before opening another"
                );
                assert_eq!(
                    self.completed.load(Ordering::SeqCst),
                    self.sources[..index].iter().flatten().count()
                );
                if self.fail_at == Some(index) {
                    return Err(PreparedArtifactError::Io(io::Error::other(
                        "injected catalog factory failure",
                    )));
                }
                self.active.fetch_add(1, Ordering::SeqCst);
                Ok(Importer {
                    completed: Arc::clone(&self.completed),
                    active: Arc::clone(&self.active),
                })
            })
            .map_err(prepared_io_error)?;
        if let Some(root) = &self.disrupt_receipt_root {
            fs::rename(root, root.with_extension("preserved"))?;
            fs::write(root, b"injected receipt persistence failure")?;
        }
        Ok(measurements)
    }
}

struct Importer {
    completed: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
}

impl PreparedArtifactImporter for Importer {
    fn fill_segment(
        &mut self,
        segment: &PreparedArtifactSegmentDescriptor,
        byte_offset: u64,
        output: &mut [u8],
    ) -> Result<u64, PreparedArtifactError> {
        fill_prepared_segment(segment, byte_offset, output)?;
        Ok(1)
    }
}

impl Drop for Importer {
    fn drop(&mut self) {
        assert_eq!(self.active.fetch_sub(1, Ordering::SeqCst), 1);
        self.completed.fetch_add(1, Ordering::SeqCst);
    }
}

struct Fixture {
    _source: tempfile::TempDir,
    cache: tempfile::TempDir,
    store: PreparedArtifactStore,
    descriptors: Vec<PreparedArtifactDescriptor>,
    suite: PreparedSuiteRegistry,
    physical: PhysicalWorkBinding,
    catalog_node: WorkNodeId,
    started: Arc<AtomicBool>,
    completed: Arc<AtomicUsize>,
}

fn fixture(
    problem: &casa_imaging_model::CompiledProblem,
    count: usize,
    fail_at: Option<usize>,
) -> Fixture {
    fixture_with_reuse(problem, count, fail_at, &[])
}

fn fixture_with_reuse(
    problem: &casa_imaging_model::CompiledProblem,
    count: usize,
    fail_at: Option<usize>,
    reused: &[usize],
) -> Fixture {
    let source = prepared_tempdir();
    let cache = prepared_tempdir();
    let budget =
        PreparedArtifactBudget::new((count as u64 * 20_000).max(100_000), count.max(4), 4_096)
            .expect("catalog budget");
    let store = PreparedArtifactStore::open(cache.path(), prepared_storage_domain(), budget)
        .expect("catalog store");
    let mut descriptors = (0..count)
        .map(|index| {
            prepared_descriptor_with_registration_and_cell(
                &store,
                problem,
                prepared_registration(),
                prepared_cell(1.0e9 + index as f64),
            )
        })
        .collect::<Vec<_>>();
    descriptors.sort_unstable_by_key(PreparedArtifactDescriptor::identity);
    if reused == [0] {
        let (catalog, implementation) = prepared_catalog(prepared_registration());
        descriptors[0] = (1_000..1_128)
            .map(|index| {
                PreparedArtifactDescriptor::convolution_function(
                    &store,
                    &catalog,
                    &implementation,
                    problem,
                    prepared_cell(1.0e9 + index as f64),
                    PreparedArtifactPlaneDescriptor::new(
                        [49, 49],
                        [24, 24],
                        1,
                        prepared_uv(49),
                        PreparedArtifactPrecision::ComplexF32,
                        PreparedArtifactOrder::Axis0ContiguousLittleEndian,
                    )
                    .expect("large warm imaging plane"),
                    PreparedArtifactPlaneDescriptor::new(
                        [5, 5],
                        [2, 2],
                        1,
                        prepared_uv(5),
                        PreparedArtifactPrecision::ComplexF32,
                        PreparedArtifactOrder::LastAxisContiguousLittleEndian,
                    )
                    .expect("warm weight plane"),
                )
                .expect("large warm descriptor")
            })
            .find(|descriptor| descriptor.identity() < descriptors[1].identity())
            .expect("large warm member sorts before cold members");
        assert!(
            (49 * 49 + 5 * 5) * 8
                > store
                    .reservation(&descriptors[1], PreparedArtifactOperation::Load)
                    .expect("cold reservation")
                    .entry_bytes()
        );
    }
    let producer = WorkNodeId::new("prepared-phase-producer");
    let sources = descriptors
        .iter()
        .enumerate()
        .map(|(index, descriptor)| {
            if reused.contains(&index) {
                return None;
            }
            Some(
                PreparedArtifactImportSource::new(
                    descriptor,
                    producer.clone(),
                    descriptor
                        .segments()
                        .iter()
                        .map(|segment| {
                            let bytes = if segment.name() == "imaging" {
                                9 * 8
                            } else {
                                25 * 8
                            };
                            PreparedArtifactImportSegment::new(
                                segment.name(),
                                source.path(),
                                descriptor.identity(),
                                bytes,
                                1,
                                prepared_storage_domain(),
                            )
                            .expect("source segment")
                        })
                        .collect(),
                )
                .expect("source binding"),
            )
        })
        .collect::<Vec<_>>();
    let fragment = PreparedArtifactCatalogPlanFragment::new(
        &descriptors,
        &store,
        producer.clone(),
        WorkNodeId::new("prepared-phase-commit"),
        implementation(6),
    )
    .expect("fragment")
    .with_import_sources(&sources)
    .expect("cold fragment");
    let id = fragment.work_implementation_id().expect("catalog id");
    let catalog_node = fragment.work_node_id().expect("catalog node");
    let mut base_executor = recording_executor(6, None, None);
    base_executor.measurements.insert(
        producer,
        (
            Vec::new(),
            sources
                .iter()
                .flatten()
                .map(|source| {
                    artifact_measurement(
                        source.identity(),
                        Some(source.identity()),
                        ArtifactDisposition::Loaded,
                        source.source_read_bytes(),
                        None,
                    )
                })
                .collect(),
        ),
    );
    let started = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicUsize::new(0));
    let suite = PreparedSuiteRegistry {
        id: registry(3),
        metadata: Some(ImplementationContractMetadata::new(
            problem.problem_id(),
            problem.numerics_id(),
            problem.required_capabilities().clone(),
        )),
        implementations: BTreeMap::from([
            (
                implementation(6),
                PreparedSuiteImplementation::Base(Box::new(base_executor)),
            ),
            (
                id.clone(),
                PreparedSuiteImplementation::Catalog(Box::new(PreparedCatalogAdapter {
                    id,
                    store: PreparedArtifactStore::open(
                        cache.path(),
                        prepared_storage_domain(),
                        budget,
                    )
                    .expect("execution store"),
                    descriptors: descriptors.clone(),
                    observed: Mutex::new(None),
                    import: Some(CatalogImport {
                        sources: sources.clone(),
                        started: Arc::clone(&started),
                        completed: Arc::clone(&completed),
                        active: Arc::new(AtomicUsize::new(0)),
                        fail_at,
                        disrupt_receipt_root: None,
                    }),
                })),
            ),
        ]),
        prepared: BTreeMap::from([(
            prepared_registration().implementation().clone(),
            prepared_registration(),
        )]),
    };
    let base = PreparedArtifactPlanFragment::standalone_base(
        problem,
        &suite,
        implementation(6),
        &descriptors[0],
        &store,
        1_000,
        900_000,
    )
    .expect("base");
    let physical = fragment.compose(&base).expect("composition");
    Fixture {
        _source: source,
        cache,
        store,
        descriptors,
        suite,
        physical,
        catalog_node,
        started,
        completed,
    }
}

struct CancelAfterCells {
    started: Arc<AtomicBool>,
    completed: Arc<AtomicUsize>,
    count: usize,
}
impl RunController for CancelAfterCells {
    fn directive(&mut self, _: &ExecutionStatus) -> RunDirective {
        if self.started.load(Ordering::SeqCst)
            && self.completed.load(Ordering::SeqCst) >= self.count
        {
            RunDirective::Cancel
        } else {
            RunDirective::Continue
        }
    }
}

#[test]
fn catalog_cold_import_cancellation_retains_only_the_durable_prefix_and_restarts() {
    for completed in [0, 1, 2] {
        let problem = compile(request(1)).expect("problem");
        let fixture = fixture(&problem, 3, None);
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(fixture.physical.clone()),
        )
        .expect("plan");
        let executable =
            ExecutableModelProblem::from_compiled(problem.clone()).expect("executable");
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([201; 32]);
        let mut controller = CancelAfterCells {
            started: Arc::clone(&fixture.started),
            completed: Arc::clone(&fixture.completed),
            count: completed,
        };
        let outcome = {
            let _guard = run_lock()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            runtime_run(
                &executable,
                &execution_plan,
                &current,
                &fixture.suite,
                authority(),
                &mut controller,
                execution_plan.bind_receipt(execution_provenance(
                    attempt,
                    BuildIdentity::from_sha256([202; 32]),
                )),
            )
            .expect("cooperative cancellation")
        };
        assert_eq!(outcome, ExecutionOutcome::Cancelled);
        let receipt = execution_plan
            .receipt_store()
            .open(attempt)
            .expect("cancelled receipt");
        assert_eq!(receipt.status(), ReceiptStatus::Cancelled);
        assert_eq!(
            receipt.node_status(&fixture.catalog_node),
            Some(ReceiptStatus::Cancelled)
        );
        assert_eq!(fixture.completed.load(Ordering::SeqCst), completed);
        for (index, descriptor) in fixture.descriptors.iter().enumerate() {
            assert_eq!(
                receipt.artifact_disposition(descriptor.identity()),
                (index < completed).then_some(ArtifactDisposition::Loaded)
            );
            assert_eq!(
                fixture
                    .cache
                    .path()
                    .join("objects-v3")
                    .join(descriptor.identity().to_string())
                    .exists(),
                index < completed
            );
        }
        // A process kill cannot write a terminal receipt. Reconcile its private
        // staging residue independently of that old receipt's status.
        let staging = fixture
            .cache
            .path()
            .join("objects-v3/.staging-interrupted-import");
        fs::create_dir(&staging).expect("orphan staging");
        fs::write(staging.join("payload.bin"), [1_u8; 16]).expect("partial payload");
        let budget = PreparedArtifactBudget::new(100_000, 4, 4_096).expect("restart budget");
        let (suite, id) = catalog_registry(
            &problem,
            PreparedArtifactStore::open(fixture.cache.path(), prepared_storage_domain(), budget)
                .expect("restart store"),
            fixture.descriptors.clone(),
        );
        let physical =
            catalog_physical_work(&problem, &suite, &fixture.store, &fixture.descriptors);
        let warm_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(physical),
        )
        .expect("restart plan");
        let result = run_prepared(
            &problem,
            &warm_plan,
            &suite,
            warm_plan.bind_receipt(execution_provenance(
                casa_imaging_runtime::ExecutionAttemptId::from_sha256([203; 32]),
                BuildIdentity::from_sha256([202; 32]),
            )),
        );
        assert!(matches!(
            result,
            Err(RunError::Evidence(
                ExecutionEvidenceError::RejectedArtifact { .. }
            ))
        ));
        let observed = catalog_adapter_observed(&suite, &id).expect("reconciled catalog");
        assert_eq!(
            observed
                .iter()
                .filter(|value| matches!(value, PreparedObserved::Materialized { .. }))
                .count(),
            completed
        );
        assert!(!staging.exists());
        assert_eq!(
            execution_plan
                .receipt_store()
                .open(attempt)
                .expect("unchanged old receipt")
                .status(),
            ReceiptStatus::Cancelled
        );
    }
}

#[test]
fn catalog_cold_import_failure_retains_prefix_and_current_io_without_later_claims() {
    for failed in [0, 1, 2] {
        let problem = compile(request(1)).expect("problem");
        let fixture = fixture(&problem, 3, Some(failed));
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(fixture.physical),
        )
        .expect("plan");
        let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([204; 32]);
        let error = run_prepared(
            &problem,
            &execution_plan,
            &fixture.suite,
            execution_plan.bind_receipt(execution_provenance(
                attempt,
                BuildIdentity::from_sha256([205; 32]),
            )),
        )
        .expect_err("factory failure");
        assert!(
            error
                .to_string()
                .contains("injected catalog factory failure"),
            "{error}"
        );
        let receipt = execution_plan
            .receipt_store()
            .open(attempt)
            .expect("failure receipt");
        assert_eq!(receipt.status(), ReceiptStatus::Failed);
        assert_eq!(fixture.completed.load(Ordering::SeqCst), failed);
        for (index, descriptor) in fixture.descriptors.iter().enumerate() {
            assert_eq!(
                receipt.artifact_disposition(descriptor.identity()),
                (index < failed).then_some(ArtifactDisposition::Loaded)
            );
        }
        assert!(
            receipt
                .stage_actual_io(&fixture.catalog_node, IoBufferKind::StorageManager)
                .is_some_and(
                    |(bytes, operations)| bytes >= PREPARED_PAYLOAD_BYTES * failed as u64
                        && (failed == 0 || operations > 0)
                )
        );
    }
}

#[test]
fn catalog_cold_import_eviction_preserves_selected_members_on_failure_and_success() {
    for (fail_at, reused) in [Some(2), None]
        .into_iter()
        .flat_map(|fail_at| [false, true].map(|reused| (fail_at, reused)))
    {
        let problem = compile(request(1)).expect("problem");
        let fixture = fixture_with_reuse(&problem, 3, fail_at, if reused { &[0] } else { &[] });
        let budget = PreparedArtifactBudget::new(100_000, 4, 4_096).expect("seed budget");
        let seeds = (3..128)
            .map(|index| {
                prepared_descriptor_with_registration_and_cell(
                    &fixture.store,
                    &problem,
                    prepared_registration(),
                    prepared_cell(1.0e9 + index as f64),
                )
            })
            .filter(|descriptor| descriptor.identity() > fixture.descriptors[0].identity())
            .take(if reused { 3 } else { 4 })
            .collect::<Vec<_>>();
        assert_eq!(seeds.len(), if reused { 3 } else { 4 });
        for (index, descriptor) in seeds
            .iter()
            .chain(reused.then_some(&fixture.descriptors[0]))
            .enumerate()
        {
            let store = PreparedArtifactStore::open(
                fixture.cache.path(),
                prepared_storage_domain(),
                budget,
            )
            .expect("seed store");
            let adapter = PreparedOperationAdapter::new(
                PreparedArtifactOperation::Generate,
                store,
                descriptor.clone(),
            );
            let (mut suite, _) = prepared_registry(&problem, adapter);
            suite.implementations.insert(
                implementation(6),
                PreparedSuiteImplementation::Base(Box::new(recording_executor(6, None, None))),
            );
            let base = PreparedArtifactPlanFragment::standalone_base(
                &problem,
                &suite,
                implementation(6),
                descriptor,
                &fixture.store,
                1_000,
                900_000,
            )
            .expect("seed base");
            let physical = PreparedArtifactPlanFragment::new(
                descriptor,
                &fixture.store,
                PreparedArtifactOperation::Generate,
                WorkNodeId::new("prepared-phase-producer"),
                WorkNodeId::new("prepared-phase-commit"),
                implementation(6),
            )
            .compose(&base)
            .expect("seed composition");
            let seed_plan = plan(
                &problem,
                PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
                |_, _| Ok::<_, ()>(physical),
            )
            .expect("seed plan");
            run_prepared(
                &problem,
                &seed_plan,
                &suite,
                seed_plan.bind_receipt(execution_provenance(
                    casa_imaging_runtime::ExecutionAttemptId::from_sha256([230 + index as u8; 32]),
                    BuildIdentity::from_sha256([234; 32]),
                )),
            )
            .expect("seed publication");
        }
        let objects = fixture.cache.path().join("objects-v3");
        assert_eq!(directory_entry_names(&objects).len(), 4);
        let execution_plan = plan(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            |_, _| Ok::<_, ()>(fixture.physical),
        )
        .expect("catalog plan");
        let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([235; 32]);
        let result = run_prepared(
            &problem,
            &execution_plan,
            &fixture.suite,
            execution_plan.bind_receipt(execution_provenance(
                attempt,
                BuildIdentity::from_sha256([236; 32]),
            )),
        );
        if fail_at.is_some() {
            let error = result.expect_err("late factory failure");
            assert!(
                error
                    .to_string()
                    .contains("injected catalog factory failure"),
                "{error:?}"
            );
        } else {
            result.expect("complete catalog import");
        }
        let completed = fail_at.unwrap_or(3);
        let receipt = execution_plan
            .receipt_store()
            .open(attempt)
            .expect("receipt");
        for (index, descriptor) in fixture.descriptors.iter().enumerate() {
            assert_eq!(
                receipt.artifact_disposition(descriptor.identity()),
                (index < completed).then_some(if reused && index == 0 {
                    ArtifactDisposition::Reused
                } else {
                    ArtifactDisposition::Loaded
                })
            );
            assert_eq!(
                objects.join(descriptor.identity().to_string()).exists(),
                index < completed,
                "completed catalog member must remain reusable"
            );
        }
        assert_eq!(directory_entry_names(&objects).len(), 4);
        assert_eq!(
            seeds
                .iter()
                .filter(|descriptor| objects.join(descriptor.identity().to_string()).exists())
                .count(),
            4 - completed
        );
    }
}

#[test]
fn catalog_cold_import_does_not_import_a_missing_plan_selected_warm_member() {
    let problem = compile(request(1)).expect("problem");
    let fixture = fixture_with_reuse(&problem, 3, None, &[0]);
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(fixture.physical),
    )
    .expect("mixed plan");
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([237; 32]);
    let error = run_prepared(
        &problem,
        &execution_plan,
        &fixture.suite,
        execution_plan.bind_receipt(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([238; 32]),
        )),
    )
    .expect_err("warm member disappeared");
    assert!(
        error
            .to_string()
            .contains("prepared artifact is incomplete"),
        "{error}"
    );
    assert_eq!(
        fixture.completed.load(Ordering::SeqCst),
        0,
        "no implicit import"
    );
    let receipt = execution_plan
        .receipt_store()
        .open(attempt)
        .expect("failed receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    for descriptor in &fixture.descriptors {
        assert_eq!(receipt.artifact_disposition(descriptor.identity()), None);
    }
}

#[test]
fn catalog_cold_import_rejects_an_oversized_receipt_before_dispatch() {
    let problem = compile(request(1)).expect("problem");
    let fixture = fixture(&problem, 3, None);
    let directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        directory.path(),
        ReceiptRetention::new(4, 1_024).expect("tiny retention"),
    )
    .expect("receipt store");
    let execution_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |_, _| Ok::<_, ()>(fixture.physical),
    )
    .expect("plan");
    let error = run_prepared(
        &problem,
        &execution_plan,
        &fixture.suite,
        execution_plan.bind_receipt(execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([206; 32]),
            BuildIdentity::from_sha256([207; 32]),
        )),
    )
    .expect_err("oversized terminal reservation");
    assert!(matches!(
        error,
        RunError::Receipt(casa_imaging_runtime::ReceiptError::RetentionExceeded)
    ));
    assert!(!fixture.started.load(Ordering::SeqCst));
    assert_eq!(fixture.completed.load(Ordering::SeqCst), 0);
}

#[test]
fn catalog_cold_import_receipt_failure_preserves_objects_without_claiming_completion() {
    let problem = compile(request(1)).expect("problem");
    let mut fixture = fixture(&problem, 3, None);
    let directory = prepared_tempdir();
    let receipts = ExecutionReceiptStore::new(
        directory.path().join("receipts"),
        ReceiptRetention::new(4, 1 << 20).expect("retention"),
    )
    .expect("receipts");
    for adapter in fixture.suite.implementations.values_mut() {
        if let PreparedSuiteImplementation::Catalog(adapter) = adapter {
            adapter
                .import
                .as_mut()
                .expect("cold import")
                .disrupt_receipt_root = Some(receipts.root_path().to_path_buf());
        }
    }
    let execution_plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |_, _| Ok::<_, ()>(fixture.physical),
    )
    .expect("plan");
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([210; 32]);
    let error = run_prepared(
        &problem,
        &execution_plan,
        &fixture.suite,
        execution_plan.bind_receipt(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([211; 32]),
        )),
    )
    .expect_err("receipt persistence fails");
    assert!(matches!(error, RunError::Receipt(_)), "{error}");
    assert_eq!(fixture.completed.load(Ordering::SeqCst), 3);
    assert!(fixture.descriptors.iter().all(|descriptor| {
        fixture
            .cache
            .path()
            .join("objects-v3")
            .join(descriptor.identity().to_string())
            .join("payload.bin")
            .is_file()
    }));
    fs::remove_file(receipts.root_path()).expect("remove injected test file");
    fs::rename(
        receipts.root_path().with_extension("preserved"),
        receipts.root_path(),
    )
    .expect("restore original receipt directory");
    let receipt = receipts
        .open(attempt)
        .expect("last successfully persisted checkpoint");
    assert_eq!(receipt.status(), ReceiptStatus::Running);
    assert_eq!(
        receipt.node_status(&fixture.catalog_node),
        Some(ReceiptStatus::Running)
    );
    assert!(fixture.descriptors.iter().all(|descriptor| {
        receipt
            .artifact_disposition(descriptor.identity())
            .is_none()
    }));
    let recovery = self::fixture(&problem, 3, None);
    plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(recovery.physical),
    )
    .expect("verified cleanup returns capacity even when receipt persistence failed");
}

#[test]
fn catalog_cold_import_revalidates_source_inode_before_opening_any_importer() {
    let problem = compile(request(1)).expect("problem");
    let fixture = fixture(&problem, 3, None);
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        |_, _| Ok::<_, ()>(fixture.physical),
    )
    .expect("plan");
    let replaced = fixture._source.path().with_extension("replaced");
    fs::rename(fixture._source.path(), &replaced).expect("move original source inode");
    fs::create_dir(fixture._source.path()).expect("different inode at same locator");
    let error = run_prepared(
        &problem,
        &execution_plan,
        &fixture.suite,
        execution_plan.bind_receipt(execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([208; 32]),
            BuildIdentity::from_sha256([209; 32]),
        )),
    )
    .expect_err("changed source inode");
    assert!(error.to_string().contains("source"), "{error}");
    assert_eq!(fixture.completed.load(Ordering::SeqCst), 0);
    fs::remove_dir(&replaced).expect("remove empty moved test directory");
}

#[test]
fn catalog_cold_import_admits_one_workspace_and_two_queue_slots_at_all_catalog_sizes() {
    let problem = compile(request(1)).expect("problem");
    let mut prior_memory = 0;
    for count in [1, 32, 1024] {
        let fixture = fixture(&problem, count, None);
        let dag = fixture.physical.execution_dag();
        assert_eq!(dag.nodes().len(), 6);
        let demand = &dag.resource_alternative().demand;
        assert_eq!(
            demand
                .storage
                .iter()
                .map(|storage| storage.queue_slots.hard())
                .sum::<u64>(),
            2
        );
        assert_eq!(
            demand.memory.len(),
            2,
            "one catalog workspace and unchanged commit workspace"
        );
        let memory = demand
            .memory
            .iter()
            .map(|memory| memory.hard_bytes)
            .sum::<u64>();
        assert!(
            memory > prior_memory,
            "catalog metadata is explicitly O(N), not a constant-space claim"
        );
        assert!(
            memory < 128 * 1024 * 1024,
            "1024 metadata entries and one buffer remain bounded"
        );
        prior_memory = memory;
    }
}
