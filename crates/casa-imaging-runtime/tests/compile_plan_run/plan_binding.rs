// SPDX-License-Identifier: LGPL-3.0-or-later

//! Physical-work binding contracts, implementation identity, plan sealing
//! and the observation-transaction seal.

use super::*;

#[test]
fn physical_work_binding_rejects_io_and_publication_evidence_outside_plan_semantics() {
    let problem = compile(request(1)).expect("physical-work contract problem");
    let io_base = physical_work(6);
    let io_dag = io_base.execution_dag().clone();
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
    let io_transaction = ObservationTransactionWork::new_reconstruction(
        io_base
            .observation_transaction()
            .initial_consistency_check()
            .expect("observation consistency check")
            .clone(),
        io_base
            .observation_transaction()
            .post_replay_reconciliation()
            .expect("reconstruction has reconciliation")
            .clone(),
        io_base.observation_transaction().commit().clone(),
    );

    assert!(matches!(
        PhysicalWorkBinding::new_reconstruction(
            implementation_catalog(&problem, &io_dag),
            io_dag,
            io_prediction,
            Vec::new(),
            io_transaction,
            PublicationLayoutLedger::empty(),
        ),
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
        logical_allocations: contract_base_dag
            .logical_allocations()
            .values()
            .cloned()
            .collect(),
        physical_slots: contract_base_dag
            .physical_slots()
            .values()
            .cloned()
            .collect(),
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
    let contract_transaction = ObservationTransactionWork::new_reconstruction(
        contract_base
            .observation_transaction()
            .initial_consistency_check()
            .expect("observation consistency check")
            .clone(),
        contract_base
            .observation_transaction()
            .post_replay_reconciliation()
            .expect("reconstruction has reconciliation")
            .clone(),
        contract_base.observation_transaction().commit().clone(),
    );
    assert!(matches!(
        PhysicalWorkBinding::new_reconstruction(
            implementation_catalog(&problem, &contract_dag),
            contract_dag,
            contract_prediction,
            Vec::new(),
            contract_transaction,
            PublicationLayoutLedger::empty(),
        ),
        Err(PhysicalWorkBindingError::MissingIoContract {
            kind: IoBufferKind::SourceReadAhead,
            ..
        })
    ));

    let publication_base = physical_work(6);
    let publication_dag = publication_base.execution_dag().clone();
    let publication_prediction = publication_base.prediction().clone();
    let output = PlannedArtifact::new(
        ArtifactIdentity::from_sha256([79; 32]),
        WorkNodeId::new("execute"),
        ArtifactRole::Output,
        None,
    );
    let publication_transaction = ObservationTransactionWork::new_reconstruction(
        publication_base
            .observation_transaction()
            .initial_consistency_check()
            .expect("observation consistency check")
            .clone(),
        publication_base
            .observation_transaction()
            .post_replay_reconciliation()
            .expect("reconstruction has reconciliation")
            .clone(),
        publication_base.observation_transaction().commit().clone(),
    );

    assert!(matches!(
        PhysicalWorkBinding::new_reconstruction(
            implementation_catalog(&problem, &publication_dag),
            publication_dag,
            publication_prediction,
            vec![output],
            publication_transaction,
            PublicationLayoutLedger::empty(),
        ),
        Err(PhysicalWorkBindingError::MissingPublicationContract { .. })
    ));
}

#[test]
fn physical_work_binding_rejects_typed_io_contracts_without_predictions() {
    let problem = compile(request(1)).expect("physical-work contract problem");
    let base = evidenced_physical_work(6);
    let dag = base.execution_dag().clone();
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
    let transaction = ObservationTransactionWork::new_reconstruction(
        base.observation_transaction()
            .initial_consistency_check()
            .expect("observation consistency check")
            .clone(),
        base.observation_transaction()
            .post_replay_reconciliation()
            .expect("transaction has reconciliation")
            .clone(),
        base.observation_transaction().commit().clone(),
    );

    assert!(matches!(
        PhysicalWorkBinding::new_reconstruction(
            implementation_catalog(&problem, &dag),
            dag,
            prediction,
            Vec::new(),
            transaction,
            PublicationLayoutLedger::empty(),
        ),
        Err(PhysicalWorkBindingError::MissingIoPrediction { .. })
    ));
}

#[test]
fn physical_work_binding_rejects_cpu_io_buffer_contracts_without_predictions() {
    let problem = compile(request(1)).expect("physical-work contract problem");
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
                if node.as_str() == "transaction-commit" {
                    stage.with_io(vec![IoPrediction::new(IoBufferKind::Publication, 2_048, 1)])
                } else {
                    stage
                }
            })
            .collect(),
    )
    .expect("complete prediction ledger without preparation I/O evidence");

    assert!(matches!(
        native_product_physical_work(
            &problem,
            implementation_catalog(&problem, &dag),
            dag,
            prediction,
            Vec::new(),
            base.observation_transaction().clone(),
            PublicationLayoutLedger::empty(),
        ),
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
                artifact_measurement(input, Some(input), ArtifactDisposition::Staged, 4_096, None),
                artifact_measurement(cache, Some(cache), ArtifactDisposition::Reused, 0, None),
            ],
        ),
    )]);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
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
            disposition: ArtifactDisposition::Staged,
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
    let selected = publication_capable_executor(&problem, 6);
    let different = recording_executor(7, None, None);
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
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
fn initial_consistency_check_receives_the_exact_observation_transaction() {
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
    let observed = Arc::new(AtomicBool::new(false));
    let mut executor = publication_capable_executor(&problem, 6);
    executor.initial_consistency_expected = Some((
        problem.observation_transaction().transaction_id(),
        Arc::clone(&observed),
    ));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };

    execute_plan(&problem, &execution_plan, &current, &registry)
        .expect("initial consistency check executes with its capability");

    assert!(
        observed.load(Ordering::SeqCst),
        "the initial consistency node must receive the exact transaction state"
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
    let mut executor = publication_capable_executor(&problem, 6);
    executor.generic_source_access = Some(Arc::clone(&generic_source_access));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
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
    let registry = test_registry(&problem, 3, 7, None);
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
    let mut registry = test_registry(&problem, 3, 6, None);
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
    assert_eq!(request.version(), ImagingRequestVersion::V3);

    let problem = compile(request).expect("logical compilation");
    assert_eq!(problem.numerics_id().as_bytes().len(), 32);
}

#[test]
fn plan_seals_physical_work_and_every_required_binding() {
    assert_eq!(ExecutionPlanId::SCHEMA_VERSION, 12);
    // The golden binds the physical plan and publication metadata, not image content.
    let problem = compile(request_with_geometry(
        1,
        geometry_with_shape_and_increment([0.0, 0.0], ImageShape::new(1, 1), [-1.0e-6, 1.0e-6]),
    ))
    .expect("logical compilation");
    let expected_problem_id = problem.problem_id();
    let bindings = PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let receipts = ExecutionReceiptStore::new(
        "/tmp/casa-rs-imaging-plan-id-regression",
        ReceiptRetention::new(4, 1_048_576).expect("plan-id retention"),
    )
    .expect("plan-id receipt store");
    let execution_plan = plan_with_receipts(
        &problem,
        bindings.clone(),
        &receipts,
        |problem, bindings| {
            assert_eq!(problem.problem_id(), expected_problem_id);
            assert_eq!(bindings.resource_policy(), &ResourcePolicy::Balanced);
            Ok::<_, ()>(physical_work_with_transaction_staging(
                problem,
                6,
                product_participants(problem),
                false,
                true,
            ))
        },
    )
    .expect("physical planning");

    assert_eq!(execution_plan.problem_id(), problem.problem_id());
    assert_eq!(
        execution_plan.product_graph_id(),
        problem.product_graph().graph_id()
    );
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
        execution_plan.observation_transaction().product_graph_id(),
        problem.product_graph().graph_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().transaction_id(),
        problem.observation_transaction().transaction_id()
    );
    assert_eq!(
        execution_plan.observation_transaction().physical_work_id(),
        execution_plan.physical_work_id()
    );

    let repeated = plan_with_receipts(&problem, bindings, &receipts, |problem, _| {
        Ok::<_, ()>(physical_work_with_transaction_staging(
            problem,
            6,
            product_participants(problem),
            false,
            true,
        ))
    })
    .expect("repeat physical planning");
    assert_eq!(execution_plan.plan_id(), repeated.plan_id());
}

#[test]
fn reconstruction_only_transaction_scope_rejects_product_publication() {
    let problem = compile(request(1)).expect("logical compilation");
    let base = physical_work_for_problem(&problem, 6);
    let dag = base.execution_dag().clone();

    let error = PhysicalWorkBinding::new_reconstruction(
        implementation_catalog(&problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect_err("Product layouts require explicit sealed authority");

    assert!(matches!(
        error,
        PhysicalWorkBindingError::InvalidProductPublication { .. }
    ));
}

#[test]
fn native_product_publication_rejects_reconstruction_only_transaction_scope() {
    let problem = compile(sealed_products_request(239)).expect("continuum compilation");
    let publication = publication_plan_for_problem(&problem);
    let base = problem_bound_sealed_work(&problem, &publication);
    let work = base.observation_transaction();
    let reconstruction = ObservationTransactionWork::new_reconstruction(
        work.initial_consistency_check()
            .expect("observation consistency check")
            .clone(),
        work.post_replay_reconciliation()
            .expect("reconstruction has reconciliation")
            .clone(),
        work.commit().clone(),
    );
    let error = PhysicalWorkBinding::new_with_product_publication(
        implementation_catalog(&problem, base.execution_dag()),
        base.execution_dag().clone(),
        base.prediction().clone(),
        base.artifacts().to_vec(),
        reconstruction,
        base.publication_layouts().clone(),
        &publication,
    )
    .expect_err("product publication cannot consume reconstruction-only transaction scope");
    assert!(matches!(
        error,
        PhysicalWorkBindingError::InvalidProductPublication { .. }
    ));
}

#[test]
fn receipt_store_location_does_not_change_the_logical_plan_identity() {
    let problem = compile(request(1)).expect("logical compilation");
    let first_directory = tempfile::tempdir().expect("first receipt directory");
    let second_directory = tempfile::tempdir().expect("second receipt directory");
    let retention = ReceiptRetention::new(4, 1_048_576).expect("plan-id retention");
    let first_receipts =
        ExecutionReceiptStore::new(first_directory.path(), retention).expect("first receipt store");
    let second_receipts = ExecutionReceiptStore::new(second_directory.path(), retention)
        .expect("second receipt store");
    let first = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        &first_receipts,
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("first physical planning");
    let second = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        &second_receipts,
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("second physical planning");

    assert_eq!(first.plan_id(), second.plan_id());
    assert_ne!(first.receipt_store(), second.receipt_store());
}

#[test]
fn transaction_seal_rejects_omitted_product_graph_publication_member() {
    let problem = compile(request_with_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual],
    ))
    .expect("two-product logical compilation");
    plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |problem, _| Ok::<_, io::Error>(physical_work_for_problem(problem, 6)),
    )
    .expect("canonical complete two-product transaction seal");

    let omitted = product_participants(&problem).into_iter().take(1).collect();
    let error = physical_work_with_product_staging(&problem, 6, omitted)
        .expect_err("one omitted product must fail the exact plan seal");
    assert!(
        error
            .to_string()
            .contains("product layouts do not exactly cover the planned member set")
    );
}

#[test]
fn transaction_seal_rejects_matching_ordinals_from_a_foreign_product_graph() {
    let problem = compile(request_with_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual],
    ))
    .expect("expected product graph");
    let foreign = compile(request_with_products(
        1,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Model],
    ))
    .expect("foreign product graph with matching node ordinals");
    assert_ne!(
        problem.product_graph().graph_id(),
        foreign.product_graph().graph_id()
    );
    assert_eq!(
        problem.product_graph().publication().members(),
        foreign.product_graph().publication().members()
    );

    let error = physical_work_with_product_staging(&problem, 6, product_participants(&foreign))
        .expect_err("foreign product graph must fail the transaction seal");
    assert!(
        error
            .to_string()
            .contains("has no exact publication layout")
    );
}

#[test]
fn mapped_publication_staging_binds_its_producer_release_allocation_and_plan_identity() {
    let problem = compile(request(1)).expect("logical compilation");
    let bindings = || PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4));
    let valid = mapped_publication_candidate(
        WorkNodeId::new("1-prepare-mapping"),
        WorkDependency::Work(WorkNodeId::new("2-release-mapping")),
        AllocationId::new("execute-failed-mapping"),
    )
    .expect("producer-owned mapped staging retained through its release");
    let mapped_plan =
        plan(&problem, bindings(), |_, _| Ok::<_, ()>(valid)).expect("mapped publication planning");
    let unmapped_plan = plan(&problem, bindings(), |_, _| {
        Ok::<_, ()>(release_failure_physical_work(6, 8, false))
    })
    .expect("otherwise identical unmapped publication planning");
    assert_ne!(mapped_plan.plan_id(), unmapped_plan.plan_id());

    for (producer, terminal, allocation, expected) in [
        (
            WorkNodeId::new("0-independent-io"),
            WorkDependency::Work(WorkNodeId::new("2-release-mapping")),
            AllocationId::new("execute-failed-mapping"),
            "not acquired by its producer",
        ),
        (
            WorkNodeId::new("1-prepare-mapping"),
            WorkDependency::Fence(FenceId::new(
                WorkNodeId::new("0-independent-io"),
                FenceKind::Io,
            )),
            AllocationId::new("execute-failed-mapping"),
            "not acquired by its producer",
        ),
        (
            WorkNodeId::new("1-prepare-mapping"),
            WorkDependency::Work(WorkNodeId::new("2-release-mapping")),
            AllocationId::new("transaction-product-writer-buffer"),
            "not acquired by its producer",
        ),
    ] {
        let error = mapped_publication_candidate(producer, terminal, allocation)
            .expect_err("mismatched mapped staging must be rejected");
        assert!(
            matches!(
                error,
                PhysicalWorkBindingError::InvalidPublicationLayout { .. }
            ),
            "{error}"
        );
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn transaction_seal_blocks_unbound_transaction_work() {
    let problem = compile(request(1)).expect("logical compilation");
    let base = physical_work_for_problem(&problem, 6);
    let dag = base.execution_dag().clone();
    let unbound = native_product_physical_work(
        &problem,
        implementation_catalog(&problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        ObservationTransactionWork::new_product_publication(
            WorkNodeId::new("execute"),
            base.observation_transaction()
                .post_replay_reconciliation()
                .expect("reconstruction has reconciliation")
                .clone(),
            base.observation_transaction().commit().clone(),
        ),
        base.publication_layouts().clone(),
    )
    .expect("physically valid but transaction-unbound candidate");
    let result = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, io::Error>(unbound),
    );

    assert!(
        matches!(result, Err(PlanError::ObservationTransaction(_))),
        "the public plan boundary must reject physical work that bypasses transaction sealing"
    );
}
