// SPDX-License-Identifier: LGPL-3.0-or-later

//! Receipt evidence: the selected plan projection, conditional routes,
//! predicted versus actual stage, resource, I/O and artifact use, and typed
//! terminal outcomes.

use super::*;

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
        |_, _| Ok::<_, ()>(auditable_physical_work(&problem, 6)),
    )
    .expect("auditable physical planning");
    let current = RunBindings::new(problem.inputs().clone(), &policy, cost_model(4));
    let cache_artifact = ArtifactIdentity::from_sha256([51; 32]);
    let mut executor = publication_capable_executor(&problem, 6);
    executor.measurements = BTreeMap::from([(
        WorkNodeId::new("first-major-work"),
        (
            Vec::new(),
            vec![artifact_measurement(
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
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([61; 32]),
        BuildIdentity::from_sha256([62; 32]),
    );
    let mut controller = AdaptAtMajorBoundary::default();

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
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
        execution_plan
            .artifacts()
            .iter()
            .map(PlannedArtifact::identity)
            .collect()
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
fn receipt_records_only_the_atomically_selected_conditional_route() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(conditional_adaptive_physical_work(6)),
    )
    .expect("conditional adaptive physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(
            implementation(6),
            product_publication_recording_executor(
                &problem,
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicUsize::new(0)),
            ),
        )]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([65; 32]),
        BuildIdentity::from_sha256([66; 32]),
    );
    let mut controller = SelectStreamedRoute::default();

    let outcome = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("conditional receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let retained = WorkNodeId::new("retained-route");
    let streamed = WorkNodeId::new("streamed-route");
    let retained_fence = FenceId::new(retained.clone(), FenceKind::Io);
    let streamed_fence = FenceId::new(streamed.clone(), FenceKind::Io);
    let adaptation = receipt
        .adaptation_projection(&AdaptationId::new("select-streamed-route"))
        .expect("conditional transition projection");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert!(controller.applied);
    assert_eq!(receipt.status(), ReceiptStatus::Completed);
    assert!(adaptation.was_applied());
    assert_eq!(
        adaptation.transition().activate_nodes,
        BTreeSet::from([streamed.clone()])
    );
    assert_eq!(
        adaptation.transition().deactivate_nodes,
        BTreeSet::from([retained.clone()])
    );
    assert_eq!(
        receipt.node_status(&streamed),
        Some(ReceiptStatus::Completed)
    );
    assert_eq!(
        receipt.fence_status(&streamed_fence),
        Some(ReceiptStatus::Completed)
    );
    assert_eq!(
        receipt.node_status(&retained),
        Some(ReceiptStatus::NotStarted)
    );
    assert_eq!(
        receipt.fence_status(&retained_fence),
        Some(ReceiptStatus::NotStarted)
    );
    assert_eq!(
        receipt.stage_predicted_io(&streamed, IoBufferKind::SourceReadAhead),
        Some((8, 1))
    );
    assert_eq!(
        receipt.stage_actual_io(&streamed, IoBufferKind::SourceReadAhead),
        Some((8, 1))
    );
    assert_eq!(
        receipt.stage_actual_io(&retained, IoBufferKind::SourceReadAhead),
        None
    );
    assert!(receipt.stage_actual_elapsed_nanos(&streamed).is_some());
    assert_eq!(receipt.stage_actual_elapsed_nanos(&retained), None);

    let path = only_receipt_path(receipts.root_path());
    let original = fs::read_to_string(&path).expect("serialized conditional receipt");
    for (case, forged) in [
        (
            "inactive route reported complete",
            with_node_receipt_status(
                original.clone(),
                retained.as_str(),
                "not_started",
                "completed",
            ),
        ),
        (
            "selected route reported not started",
            with_node_receipt_status(
                original.clone(),
                streamed.as_str(),
                "completed",
                "not_started",
            ),
        ),
    ] {
        fs::write(&path, forged).expect("rewrite checksum-valid conditional receipt");
        assert!(
            matches!(
                receipts.open(provenance.attempt_id()),
                Err(casa_imaging_runtime::ReceiptError::IntegrityMismatch)
            ),
            "{case} must fail reconstructed route validation"
        );
    }
}

#[test]
fn receipt_compares_plan_predictions_with_actual_stage_resource_and_fence_use() {
    let problem = compile(request(1)).expect("logical compilation");
    let read = WorkNodeId::new("read");
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
    let mut executor = product_publication_recording_executor(
        &problem,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    executor.fence_measurement_node = Some(read.clone());
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([11; 32]),
        BuildIdentity::from_sha256([12; 32]),
    );
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let io_fence = FenceId::new(read.clone(), FenceKind::Io);

    assert_eq!(receipt.predicted_elapsed_nanos(), 700);
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
    let publication = publication_plan_for_problem(&problem);
    let authorization = &publication;
    let first_output = &authorization.entries()[0];
    let output = first_output.artifact();
    let output_bytes = first_output.payload_bytes();
    let input_path = RedactedPath::from_path("/Users/private/secret-source.ms");
    let output_path = RedactedPath::from_path("/Volumes/private/secret-image.table");
    let mut executor = publication_capable_executor(&problem, 6);
    executor.publication_path = Some(output_path);
    executor.sealed_measurements = Some(
        authorization
            .entries()
            .iter()
            .map(|entry| {
                artifact_measurement(
                    entry.artifact(),
                    None,
                    ArtifactDisposition::Staged,
                    entry.payload_bytes(),
                    (entry.node() == first_output.node()).then_some(output_path),
                )
            })
            .collect(),
    );
    executor.measurements = BTreeMap::from([
        (
            WorkNodeId::new("read"),
            (
                vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
                vec![
                    artifact_measurement(
                        input,
                        Some(input),
                        ArtifactDisposition::Loaded,
                        4_096,
                        Some(input_path),
                    ),
                    artifact_measurement(
                        cache,
                        Some(cache),
                        ArtifactDisposition::Reused,
                        1_024,
                        None,
                    ),
                ],
            ),
        ),
        (
            WorkNodeId::new("transaction-commit"),
            (
                vec![IoMeasurement::new(IoBufferKind::Publication, 2_048, 1)],
                Vec::new(),
            ),
        ),
    ]);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([37; 32]),
        BuildIdentity::from_sha256([38; 32]),
    );
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect("receipted execution");
    let receipt = receipts.open(provenance.attempt_id()).expect("receipt");
    let read = WorkNodeId::new("read");
    let publish = WorkNodeId::new("transaction-commit");

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
        Some(WorkNodeId::new("transaction-commit"))
    );
    assert_eq!(receipt.artifact_actual_bytes(output), Some(output_bytes));
    assert_eq!(
        receipt.artifact_disposition(output),
        Some(ArtifactDisposition::Published)
    );
    assert_eq!(
        receipt.artifact_disposition(cache),
        Some(ArtifactDisposition::Reused)
    );
    assert_eq!(
        receipt.artifact_cache_identity(cache),
        Some(CacheIdentity::from_sha256([33; 32]).as_bytes())
    );
    assert_eq!(receipt.artifact_observed_identity(output), None);
    assert_eq!(
        receipt.artifact_path_identity(input),
        Some(input_path.as_bytes())
    );
    assert_eq!(
        receipt.artifact_path_identity(output),
        Some(output_path.as_bytes())
    );
    let persisted = std::fs::read_to_string(only_receipt_path(receipts.root_path()))
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
    let publication = publication_plan_for_problem(&problem);
    let authorization = &publication;
    let first_output = &authorization.entries()[0];
    let output = first_output.artifact();
    let output_bytes = first_output.payload_bytes();
    let mut executor = product_measurement_executor(&publication);
    executor.major_cycle_problem = Some(problem.clone());
    executor.fence_failure = Some("publication fence failed");
    executor.fail_only_fence = Some(FenceKind::Publication);
    executor.measurements = BTreeMap::from([(
        WorkNodeId::new("read"),
        (
            vec![IoMeasurement::new(IoBufferKind::SourceReadAhead, 4_096, 2)],
            vec![
                artifact_measurement(input, Some(input), ArtifactDisposition::Loaded, 4_096, None),
                artifact_measurement(cache, Some(cache), ArtifactDisposition::Reused, 0, None),
            ],
        ),
    )]);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let receipts = execution_plan.receipt_store();
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([87; 32]),
        BuildIdentity::from_sha256([88; 32]),
    );
    let mut controller = RunToCompletion;

    let error = run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(provenance.clone()),
    )
    .expect_err("publication fence failure must fail the run");
    assert!(matches!(
        error,
        RunError::Execution { ref node, .. } if node == &WorkNodeId::new("transaction-commit")
    ));

    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("failed receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        receipt.artifact_disposition(output),
        Some(ArtifactDisposition::Staged)
    );
    assert_eq!(receipt.artifact_observed_identity(output), None);
    assert_eq!(receipt.artifact_actual_bytes(output), Some(output_bytes));
    assert_eq!(
        receipt.fence_status(&FenceId::new(
            WorkNodeId::new("transaction-commit"),
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
    let failed_receipts = balanced_plan.receipt_store();
    let failed_provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([41; 32]),
        BuildIdentity::from_sha256([42; 32]),
    );
    let failed_registry = test_registry(&problem, 3, 6, Some("adapter failed"));
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &failed_registry,
            authority(),
            &mut completion,
            failed_receipts.bind(failed_provenance.clone()),
        ),
        Err(RunError::Execution { .. })
    ));
    let failed = failed_receipts
        .open(failed_provenance.attempt_id())
        .expect("failed receipt");
    assert_eq!(failed.status(), ReceiptStatus::Failed);
    assert_eq!(failed.failure_kind(), Some(ReceiptFailureKind::Adapter));
    assert_eq!(
        failed.failure_node(),
        Some(WorkNodeId::new("transaction-check"))
    );
    assert_eq!(
        failed.node_status(&WorkNodeId::new("transaction-check")),
        Some(ReceiptStatus::Failed)
    );
    assert_eq!(
        failed.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::Cancelled)
    );

    let cancelled_receipts = balanced_plan.receipt_store();
    let cancelled_provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([43; 32]),
        BuildIdentity::from_sha256([44; 32]),
    );
    let successful_registry = test_registry(&problem, 3, 6, None);
    let mut cancellation = CancelAfterLaunch::default();
    assert_eq!(
        run_receipted(
            &problem,
            &balanced_plan,
            &current,
            &successful_registry,
            authority(),
            &mut cancellation,
            cancelled_receipts.bind(cancelled_provenance.clone()),
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
        Some(ReceiptStatus::Cancelled)
    );
    assert_eq!(
        cancelled.node_status(&WorkNodeId::new("execute")),
        Some(ReceiptStatus::Cancelled)
    );

    let mutation_receipts = balanced_plan.receipt_store();
    let mutation_provenance = execution_provenance(
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
            mutation_receipts.bind(mutation_provenance.clone()),
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

    let aborted_receipts = balanced_plan.receipt_store();
    let aborted_provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([49; 32]),
        BuildIdentity::from_sha256([50; 32]),
    );
    let mut interrupted_executor = recording_executor(6, None, None);
    interrupted_executor.panic_on_execute = true;
    let interrupted_registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
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
            aborted_receipts.bind(aborted_provenance.clone()),
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
    assert_eq!(
        aborted.failure_node(),
        Some(WorkNodeId::new("transaction-check"))
    );
    assert_eq!(
        aborted.node_status(&WorkNodeId::new("transaction-check")),
        Some(ReceiptStatus::Aborted)
    );
    assert_eq!(
        aborted.node_status(&WorkNodeId::new("read")),
        Some(ReceiptStatus::NotStarted)
    );
}

#[test]
fn stale_binding_uses_the_plan_receipt_store_for_mutation_evidence() {
    let problem = compile(request(1)).expect("logical compilation");
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Interactive,
        cost_model(4),
    );
    let canonical = plan.receipt_store();
    let alternate_directory = tempfile::tempdir().expect("alternate receipt directory");
    let alternate = ExecutionReceiptStore::new(
        alternate_directory.path(),
        ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("alternate receipt store");
    let provenance = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([47; 32]),
        BuildIdentity::from_sha256([48; 32]),
    );
    let registry = test_registry(&problem, 3, 6, None);
    let mut completion = RunToCompletion;
    assert!(matches!(
        run_receipted(
            &problem,
            &plan,
            &current,
            &registry,
            authority(),
            &mut completion,
            alternate.bind(provenance.clone()),
        ),
        Err(RunError::BindingMismatch { .. })
    ));
    let mutation = canonical
        .open(provenance.attempt_id())
        .expect("mutation receipt is canonicalized to the plan store");
    assert_eq!(mutation.status(), ReceiptStatus::Mutation);
    assert!(matches!(
        alternate.open(provenance.attempt_id()),
        Err(casa_imaging_runtime::ReceiptError::Io { .. })
    ));
}
