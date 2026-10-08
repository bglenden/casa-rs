// SPDX-License-Identifier: LGPL-3.0-or-later

//! Attempt-bound selected-observation completions and the failure cut of
//! the observation transaction.

use super::*;

#[test]
fn observation_completion_is_attempt_node_and_fence_bound_after_successful_settlement() {
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
    let receipts = execution_plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([157; 32]);
    let completions = Arc::new(Mutex::new(Vec::new()));
    let mut executor = publication_capable_executor(&problem, 6);
    executor.observation_completions = Some(Arc::clone(&completions));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([158; 32]),
        )),
    )
    .expect("observation completion must bind after its fence settles");

    let completions = completions.lock().expect("observation completion lock");
    assert_eq!(completions.len(), 1);
    let completion = &completions[0];
    assert_eq!(completion.attempt_id, attempt);
    assert_eq!(completion.owner_node, WorkNodeId::new("transaction-read"));
    assert_eq!(completion.settled_fences, BTreeSet::from([FenceKind::Io]));
    assert!(completion.lease_epoch > 0);
}

#[test]
fn settled_observation_completion_is_delivered_only_to_explicit_predecessor_consumers() {
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
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let mut executor = publication_capable_executor(&problem, 6);
    executor.delivered_observation_completions = Some(Arc::clone(&delivered));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect("settled selected-observation completion unlocks dependent work");

    let delivered = delivered
        .lock()
        .expect("delivered observation completion lock");
    assert!(
        delivered.iter().any(|(consumer, owner)| {
            consumer == &WorkNodeId::new("read") && owner == &WorkNodeId::new("transaction-read")
        }),
        "the first physical consumer must receive its scheduler-retained T17 predecessor evidence"
    );
    assert!(
        delivered
            .iter()
            .all(|(_, owner)| owner == &WorkNodeId::new("transaction-read"))
    );
}

#[test]
fn synchronous_observation_completion_is_exactly_once_attempt_node_and_lease_bound() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work_with_synchronous_observation_read(6)),
    )
    .expect("synchronous ObservationRead is valid physical work");
    let read = &execution_plan.execution_dag().nodes()[&WorkNodeId::new("transaction-read")];
    assert!(read.fences.is_empty());
    assert!(
        read.claims
            .iter()
            .all(|claim| claim.lifetime == ClaimLifetime::Work),
        "a fence-free read must retain each source claim through synchronous work completion"
    );
    assert!(
        execution_plan
            .observation_transaction()
            .work()
            .observation_reads()
            .contains(&WorkDependency::Work(read.id.clone()))
    );
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let receipts = execution_plan.receipt_store();
    let attempt = casa_imaging_runtime::ExecutionAttemptId::from_sha256([159; 32]);
    let completions = Arc::new(Mutex::new(Vec::new()));
    let mut executor = publication_capable_executor(&problem, 6);
    executor.observation_completions = Some(Arc::clone(&completions));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run_receipted(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([160; 32]),
        )),
    )
    .expect("synchronous ObservationRead completion must precede dependent work");

    let completions = completions.lock().expect("observation completion lock");
    assert_eq!(completions.len(), 1);
    let completion = &completions[0];
    assert_eq!(completion.attempt_id, attempt);
    assert_eq!(completion.owner_node, WorkNodeId::new("transaction-read"));
    assert!(completion.settled_fences.is_empty());
    assert!(completion.lease_epoch > 0);
}

#[test]
fn completion_from_a_different_compiled_observation_cannot_unlock_dependents() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work_with_synchronous_observation_read(6)),
    )
    .expect("synchronous ObservationRead is valid physical work");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let mut executor = recording_executor(6, None, None);
    executor.bind_foreign_observation_completion = true;
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("a foreign owner completion must not satisfy this ObservationRead node");

    assert!(matches!(
        error,
        RunError::Execution { node, .. } if node == WorkNodeId::new("transaction-read")
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        2,
        "no dependent numerical or publication work may launch"
    );
}

#[test]
fn failed_synchronous_observation_completion_prevents_dependent_work() {
    let problem = compile(request(1)).expect("logical compilation");
    let execution_plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        |_, _| Ok::<_, ()>(physical_work_with_synchronous_observation_read(6)),
    )
    .expect("synchronous ObservationRead is valid physical work");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let completions = Arc::new(Mutex::new(Vec::new()));
    let mut executor = recording_executor(6, None, None);
    executor.observation_completions = Some(Arc::clone(&completions));
    executor.observation_completion_failure = Some("selected-observation completion failed");
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    let error = run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("failed synchronous selected-observation completion must fail the attempt");

    assert!(matches!(
        error,
        RunError::Execution { node, .. } if node == WorkNodeId::new("transaction-read")
    ));
    assert_eq!(
        completions
            .lock()
            .expect("observation completion lock")
            .len(),
        1,
        "the failing affine completion hook is still invoked exactly once"
    );
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        2,
        "only the initial check and synchronous ObservationRead may execute"
    );
}

#[test]
fn failed_observation_fence_cannot_mint_attempt_bound_completion() {
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
    let completions = Arc::new(Mutex::new(Vec::new()));
    let mut executor = failing_transaction_executor(
        6,
        Arc::new(AtomicUsize::new(0)),
        None,
        Some(("transaction-read", FenceKind::Io)),
        None,
    );
    executor.observation_completions = Some(Arc::clone(&completions));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;

    run(
        &problem,
        &execution_plan,
        &current,
        &registry,
        authority(),
        &mut controller,
    )
    .expect_err("failed observation fence must fail the attempt");

    assert!(
        completions
            .lock()
            .expect("observation completion lock")
            .is_empty(),
        "physical fence failure cannot mint selected-observation completion"
    );
}

#[test]
fn transaction_failures_leave_the_old_generation_visible() {
    for (label, failure_node, fence_failure_event, publication_failure) in [
        ("input mutation", Some("transaction-check"), None, None),
        (
            "numerical reconciliation",
            Some("post-replay-reconciliation"),
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
            metadata: implementation_metadata(&problem),
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
        if label != "input mutation" {
            assert!(
                registry.executors[&implementation(6)]
                    .aborted_nodes
                    .lock()
                    .expect("recorded aborts")
                    .iter()
                    .any(|node| node.as_str() == "transaction-read"),
                "{label} must abort already-completed upstream I/O state"
            );
        }
    }

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
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(
            implementation(6),
            failing_transaction_executor(6, Arc::clone(&visible_generation), None, None, None),
        )]),
    };
    let mut completion = RunToCompletion;
    let admission_receipts = execution_plan.receipt_store();
    let pressure_guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    authority()
        .update_external_pressure(runtime_inventory(0).pressure)
        .expect("install zero-lock external pressure");

    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem");
    let result = runtime_run(
        &executable,
        &execution_plan,
        &current,
        &admission_registry,
        authority(),
        &mut completion,
        admission_receipts.bind(execution_provenance(
            casa_imaging_runtime::ExecutionAttemptId::from_sha256([243; 32]),
            BuildIdentity::from_sha256([244; 32]),
        )),
    );
    authority()
        .update_external_pressure(runtime_inventory(4).pressure)
        .expect("restore external pressure");
    drop(pressure_guard);
    let error = result.expect_err("resource admission must fail before transaction work");

    assert!(matches!(
        error,
        RunError::Scheduler(ExecutionError::Resource(
            ResourceError::NoFeasibleAlternative(certificate),
        )) if matches!(certificate.rejections(), [rejection]
            if rejection.alternative() == &AlternativeId::new("test-cpu")
                && matches!(rejection.reason(),
                    AlternativeRejectionReason::Infeasible { resource, required: 1, available: 0 }
                    if resource == "locks"))
    ));
    assert_eq!(visible_generation.load(Ordering::SeqCst), 0);
    assert_eq!(
        admission_registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0,
        "failed admission cannot launch mutation or publication work"
    );

    // Admission remains a current-capacity decision even when the same store
    // contains an earlier quantitative failure.
    let pressure_guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    authority()
        .update_external_pressure(runtime_inventory(0).pressure)
        .expect("reinstall zero-lock pressure for receipt replay");
    let replay_registry = ContractOnlyRegistry::new(
        registry(3),
        implementation_metadata(&problem),
        [implementation(6)],
    );
    let replay = runtime_plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(4)),
        authority(),
        &replay_registry,
        &admission_receipts,
        |_, _| Ok::<_, io::Error>(vec![physical_work(6)]),
    );
    authority()
        .update_external_pressure(runtime_inventory(4).pressure)
        .expect("restore pressure after receipt replay");
    drop(pressure_guard);
    assert!(matches!(
        replay,
        Err(PlanError::Resource(ResourceError::NoFeasibleAlternative(certificate)))
            if matches!(certificate.rejections(), [rejection]
                if matches!(rejection.reason(), AlternativeRejectionReason::Infeasible {
                    resource, required: 1, available: 0,
                } if resource == "locks"))
    ));
}

#[test]
fn release_failures_drain_independent_fences_and_quarantine_only_failed_slots() {
    let problem = compile(request(1)).expect("logical compilation");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );

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
        let mut prepare_executor = publication_capable_executor(&problem, 6);
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
            metadata: implementation_metadata(&problem),
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
