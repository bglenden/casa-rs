// SPDX-License-Identifier: LGPL-3.0-or-later

//! Run-time binding checks, adaptation, cancellation and the atomic
//! publication seam.

use super::*;

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
    let wrong_registry = test_registry(&problem, 9, 6, None);
    let correct_registry = test_registry(&problem, 3, 6, None);

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
    let registry = test_registry(&problem, 3, 6, None);
    let stale_inputs = [
        (
            problem_inputs(9, default_references(), ModelStateIdentity::Empty),
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
            problem_inputs(
                1,
                default_references(),
                ModelStateIdentity::Seed(identity(9)),
            ),
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
    let registry = test_registry(&problem, 3, 6, None);

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
    let registry = test_registry(&problem, 3, 6, Some("selected executor failed"));

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
    let registry = test_registry(&problem, 3, 6, None);
    let mut controller = RejectAfterLaunch::default();

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
    let registry = test_registry(&problem, 3, 6, None);
    let mut controller = AdaptAtMajorBoundary::default();

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
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(
            implementation(6),
            product_publication_recording_executor(
                &problem,
                publication_launched,
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let mut cancelling = CancelAfterLaunch::default();

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
            metadata: implementation_metadata(&problem),
            executors: BTreeMap::from([(
                implementation(6),
                product_publication_recording_executor(
                    &problem,
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
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(
            implementation(6),
            product_publication_recording_executor(
                &problem,
                Arc::clone(&publication_launched),
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let mut controller = AdaptAtPublicationLaunch {
        publication_launched,
        requested: false,
    };
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
    let mut executor = product_publication_recording_executor(
        &problem,
        publication_launched,
        Arc::clone(&visible_generation),
    );
    executor.visibility_during_fence_settlement = Some(Arc::clone(&visible_during_settlement));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut controller = RunToCompletion;
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
fn publication_does_not_lock_receipt_store_and_active_attempt_remains_exclusive() {
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
    // The two intentional overlapping runs below must not overlap an unrelated
    // test's lease on the shared deterministic authority.
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let max_bytes = 1_048_576;
    let receipts = execution_plan.receipt_store();
    let pause = Arc::new(PublicationPause::default());
    let mut executor = product_publication_recording_executor(
        &problem,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    executor.publication_pause = Some(Arc::clone(&pause));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let first = execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([93; 32]),
        BuildIdentity::from_sha256([94; 32]),
    );
    let second = first.clone();
    let executable =
        ExecutableModelProblem::from_compiled(problem.clone()).expect("direct executable problem");

    std::thread::scope(|scope| {
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let first_provenance = first.clone();
        let problem = &executable;
        let execution_plan = &execution_plan;
        let current = &current;
        let registry = &registry;
        let receipts = &receipts;
        scope.spawn(move || {
            let mut controller = RunToCompletion;
            first_tx
                .send(runtime_run(
                    problem,
                    execution_plan,
                    current,
                    registry,
                    authority(),
                    &mut controller,
                    receipts.bind(first_provenance),
                ))
                .expect("first run result receiver");
        });

        assert!(
            pause.wait_until_entered(Duration::from_secs(5)),
            "first run exited before publication pause: {:?}",
            first_rx.try_recv()
        );
        let retained_bytes = fs::read_dir(receipts.root_path())
            .expect("receipt root")
            .map(|entry| {
                entry
                    .expect("receipt entry")
                    .metadata()
                    .expect("receipt metadata")
                    .len()
            })
            .sum::<u64>();
        assert!(
            retained_bytes <= max_bytes,
            "active reservation must remain within retention"
        );

        let (second_tx, second_rx) = std::sync::mpsc::channel();
        let second_provenance = second.clone();
        scope.spawn(move || {
            let mut controller = RunToCompletion;
            second_tx
                .send(runtime_run(
                    problem,
                    execution_plan,
                    current,
                    registry,
                    authority(),
                    &mut controller,
                    receipts.bind(second_provenance),
                ))
                .expect("second run result receiver");
        });
        match second_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Err(RunError::Receipt(
                casa_imaging_runtime::ReceiptError::AttemptAlreadyExists,
            ))) => {}
            unexpected => {
                pause.release();
                panic!(
                    "duplicate attempt must fail promptly while publication is paused: {unexpected:?}"
                );
            }
        }

        pause.release();
        assert_eq!(
            first_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("first run result")
                .expect("first run"),
            ExecutionOutcome::Succeeded
        );
    });

    assert_eq!(
        receipts
            .open(first.attempt_id())
            .expect("first receipt")
            .status(),
        ReceiptStatus::Completed
    );
    assert_eq!(
        receipts
            .open(second.attempt_id())
            .expect("second receipt")
            .status(),
        ReceiptStatus::Completed
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
    let mut executor = product_publication_recording_executor(
        &problem,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    executor.publication_buffer_held = Some(Arc::clone(&held_during_publish));
    let registry = TestRegistry {
        id: registry(3),
        metadata: implementation_metadata(&problem),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
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
