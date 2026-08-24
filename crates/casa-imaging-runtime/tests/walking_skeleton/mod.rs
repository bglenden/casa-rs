// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use casa_imaging_model::{ModelColumnWrite, ModelStateIdentity, ProductKind, compile};
use casa_imaging_runtime::{
    ArtifactDisposition, ArtifactIdentity, ArtifactMeasurement, BindingKind, BuildIdentity,
    ExecutionEvidenceError, ExecutionOutcome, ExecutionProvenance, ExecutionReceiptStore,
    LeaseResource, PlanningBindings, PublicationParticipant, ReceiptFailureKind, ReceiptRetention,
    ReceiptStatus, ResourcePolicy, RunBindings, RunError, RunToCompletion, WorkNodeId,
};

mod support;

use self::support::{
    CancelAfterLaunch, PublicationProbe, TestRegistry, authority, cost_model, execution_provenance,
    failing_transaction_executor, geometry, implementation, physical_work_for_problem, plan,
    problem_inputs, publication_recording_executor, recording_executor, registry,
    request_with_products_and_model, run_receipted, test_registry,
};

struct WalkingSkeleton {
    problem: casa_imaging_model::CompiledProblem,
    plan: casa_imaging_runtime::ExecutionPlan,
    current: RunBindings,
}

fn walking_skeleton() -> WalkingSkeleton {
    let problem = compile(request_with_products_and_model(
        15,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual, ProductKind::Model],
        ModelColumnWrite::SelectedRows,
    ))
    .expect("private synthetic logical compilation");
    let plan = plan(
        &problem,
        PlanningBindings::new(
            registry(3),
            ResourcePolicy::Balanced,
            cost_model(4).initial_record(),
        ),
        |problem, _| Ok::<_, io::Error>(physical_work_for_problem(problem, 6)),
    )
    .expect("Resource Authority-backed physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    WalkingSkeleton {
        problem,
        plan,
        current,
    }
}

fn receipt_store(root: &Path, max_bytes: u64) -> Arc<ExecutionReceiptStore> {
    Arc::new(
        ExecutionReceiptStore::new(
            root,
            ReceiptRetention::new(1, max_bytes).expect("bounded retention"),
        )
        .expect("private receipt store"),
    )
}

fn provenance(seed: u8) -> ExecutionProvenance {
    execution_provenance(
        casa_imaging_runtime::ExecutionAttemptId::from_sha256([seed; 32]),
        BuildIdentity::from_sha256([seed.wrapping_add(1); 32]),
    )
}

fn receipt_participant(
    participant: PublicationParticipant,
) -> casa_imaging_runtime::ReceiptPublicationParticipant {
    match participant {
        PublicationParticipant::Product { graph_id, node_id } => {
            casa_imaging_runtime::ReceiptPublicationParticipant::Product {
                graph_identity: graph_id.as_bytes(),
                node_ordinal: node_id.ordinal(),
            }
        }
        PublicationParticipant::ModelData(measurement_set) => {
            casa_imaging_runtime::ReceiptPublicationParticipant::ModelData(measurement_set)
        }
    }
}

#[test]
fn private_synthetic_request_crosses_the_complete_compile_plan_run_seam() {
    let skeleton = walking_skeleton();
    assert_eq!(
        skeleton.problem.normal_equation().output().normalization(),
        casa_imaging_model::NormalStateNormalization::Unnormalized
    );
    assert_eq!(
        skeleton.problem.product_graph().normalization_boundary(),
        skeleton.problem.products().normalization_boundary()
    );
    let protocol = skeleton.problem.product_graph().publication().protocol();
    assert!(protocol.requires_durable_prepare());
    assert!(protocol.has_one_visibility_operation());
    assert!(protocol.has_infallible_terminal_promotion());
    assert_eq!(
        skeleton
            .problem
            .observation_transaction()
            .write_set()
            .model_columns()
            .len(),
        1
    );
    let snapshot = skeleton.problem.inputs().observation_snapshot();
    assert_eq!(snapshot.sources().len(), 1);
    assert_eq!(
        snapshot.sources()[0].selection().rows().source_row_count(),
        1
    );
    assert_eq!(
        snapshot.sources()[0]
            .selection()
            .rows()
            .selected_row_count(),
        1
    );
    assert_eq!(skeleton.plan.problem_id(), skeleton.problem.problem_id());
    assert_eq!(
        skeleton.plan.product_graph_id(),
        skeleton.problem.product_graph().graph_id()
    );
    assert_eq!(
        skeleton.plan.observation_transaction().transaction_id(),
        skeleton.problem.observation_transaction().transaction_id()
    );
    assert_eq!(
        skeleton.plan.publication_layouts().entries().len(),
        skeleton
            .problem
            .product_graph()
            .publication()
            .members()
            .len()
            + 1,
        "the atomic set contains every Product Graph member and MODEL_DATA"
    );
    let dag_nodes = skeleton
        .plan
        .execution_dag()
        .nodes()
        .keys()
        .collect::<BTreeSet<_>>();
    for required in [
        "read",
        "execute",
        "transaction-check",
        "transaction-read",
        "transaction-reconciliation",
        "transaction-stage-psf",
        "transaction-stage-model",
        "transaction-commit",
    ] {
        assert!(
            dag_nodes.contains(&WorkNodeId::new(required)),
            "the sole plan omitted required work {required}"
        );
    }

    let directory = tempfile::tempdir().expect("success receipt directory");
    let receipts = receipt_store(directory.path(), 1_048_576);
    let provenance = provenance(151);
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let prepared_observed = Arc::new(AtomicBool::new(false));
    let publication_calls = Arc::new(AtomicUsize::new(0));
    let publication_lease_observed = Arc::new(AtomicBool::new(false));
    let mut executor = publication_recording_executor(
        6,
        Arc::clone(&publication_launched),
        Arc::clone(&visible_generation),
    );
    executor.publication_probe = Some(PublicationProbe {
        receipts: Arc::clone(&receipts),
        attempt: provenance.attempt_id(),
        prepared_observed: Arc::clone(&prepared_observed),
        publication_calls: Arc::clone(&publication_calls),
    });
    executor.publication_buffer_held = Some(Arc::clone(&publication_lease_observed));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut completion = RunToCompletion;

    let outcome = run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &registry,
        authority(),
        &mut completion,
        receipts.bind(provenance.clone()),
    )
    .expect("private synthetic execution");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert!(publication_launched.load(Ordering::SeqCst));
    assert!(
        prepared_observed.load(Ordering::SeqCst),
        "durable prepared evidence must precede visibility"
    );
    assert_eq!(publication_calls.load(Ordering::SeqCst), 1);
    assert!(
        publication_lease_observed.load(Ordering::SeqCst),
        "the visibility operation must retain its plan-bound lease and allocation"
    );
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);

    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen completed private receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Completed);
    assert_eq!(
        receipt.problem_identity(),
        skeleton.problem.problem_id().as_bytes()
    );
    assert_eq!(
        receipt.product_graph_identity(),
        skeleton.problem.product_graph().graph_id().as_bytes()
    );
    assert_eq!(receipt.plan_identity(), skeleton.plan.plan_id().as_bytes());
    assert_eq!(
        receipt.plan_node_identities(),
        skeleton
            .plan
            .execution_dag()
            .nodes()
            .keys()
            .cloned()
            .collect()
    );
    assert_eq!(
        receipt.publication_layout_count(),
        skeleton.plan.publication_layouts().entries().len()
    );
    for layout in skeleton.plan.publication_layouts().entries() {
        let artifact = layout.artifact();
        assert_eq!(
            receipt.publication_participant(artifact),
            Some(receipt_participant(layout.participant()))
        );
        assert_eq!(
            receipt.publication_layout_identity(artifact),
            Some(layout.layout_id())
        );
        assert_eq!(
            receipt.publication_producer(artifact).as_ref(),
            Some(layout.staging().producer())
        );
        assert_eq!(
            receipt.publication_terminal(artifact).as_ref(),
            Some(layout.staging().terminal())
        );
        assert_eq!(
            receipt.publication_writer_buffer_kind(artifact),
            Some(layout.staging().writer_buffer_kind())
        );
        assert_eq!(
            receipt.publication_writer_allocation(artifact).as_ref(),
            Some(layout.staging().writer_allocation())
        );
        assert_eq!(
            receipt.publication_resource_bounds(artifact),
            Some(layout.resource_bounds())
        );
        assert_eq!(receipt.publication_mapped_producer(artifact), None);
        assert_eq!(receipt.publication_mapped_terminal(artifact), None);
        assert_eq!(receipt.publication_mapped_allocation(artifact), None);
        assert_eq!(
            receipt.artifact_disposition(artifact),
            Some(ArtifactDisposition::Published)
        );
    }
}

#[test]
fn stale_synthetic_binding_stops_before_the_plan_can_launch() {
    let skeleton = walking_skeleton();
    let stale = RunBindings::new(
        problem_inputs(
            16,
            skeleton.problem.inputs().reference_data().to_vec(),
            ModelStateIdentity::Empty,
        ),
        &ResourcePolicy::Balanced,
        cost_model(4),
    );
    let registry = test_registry(3, 6, None);
    let directory = tempfile::tempdir().expect("stale receipt directory");
    let receipts = receipt_store(directory.path(), 1_048_576);
    let provenance = provenance(157);
    let mut completion = RunToCompletion;

    let error = run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &stale,
        &registry,
        authority(),
        &mut completion,
        receipts.bind(provenance.clone()),
    )
    .expect_err("stale Observation Snapshot binding");

    assert!(matches!(
        error,
        RunError::BindingMismatch {
            binding: BindingKind::ObservationSnapshot
        }
    ));
    assert_eq!(
        registry.executors[&implementation(6)]
            .calls
            .load(Ordering::SeqCst),
        0
    );
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen stale-binding receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Mutation);
    assert_eq!(
        receipt.failure_kind(),
        Some(ReceiptFailureKind::BindingMutation)
    );
    assert!(
        receipt
            .plan_node_identities()
            .into_iter()
            .all(|node| receipt.node_status(&node) == Some(ReceiptStatus::NotStarted))
    );
}

#[test]
fn synthetic_adapter_cannot_report_unlisted_work() {
    let skeleton = walking_skeleton();
    let unlisted = ArtifactIdentity::from_sha256([201; 32]);
    let mut executor = recording_executor(6, None, None);
    executor.measurements.insert(
        WorkNodeId::new("execute"),
        (
            Vec::new(),
            vec![ArtifactMeasurement::new(
                unlisted,
                Some(unlisted),
                ArtifactDisposition::Built,
                1,
                None,
            )],
        ),
    );
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("unlisted-work receipt directory");
    let receipts = receipt_store(directory.path(), 1_048_576);
    let provenance = provenance(159);
    let mut completion = RunToCompletion;

    let error = run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &registry,
        authority(),
        &mut completion,
        receipts.bind(provenance.clone()),
    )
    .expect_err("unlisted adapter artifact");

    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::UnplannedArtifact {
            node,
            artifact,
        }) if node == WorkNodeId::new("execute") && artifact == unlisted
    ));
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen unlisted-work receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(
        receipt.failure_kind(),
        Some(ReceiptFailureKind::EvidenceContract)
    );
    assert_eq!(receipt.failure_node(), Some(WorkNodeId::new("execute")));
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("transaction-commit")),
        Some(ReceiptStatus::Cancelled)
    );
}

#[test]
fn synthetic_allocation_peak_cannot_overflow_its_plan_claim() {
    let skeleton = walking_skeleton();
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let mut executor = publication_recording_executor(
        6,
        Arc::new(AtomicBool::new(false)),
        Arc::clone(&visible_generation),
    );
    executor
        .resource_peak_overrides
        .insert(WorkNodeId::new("execute"), 2);
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let directory = tempfile::tempdir().expect("overflow receipt directory");
    let receipts = receipt_store(directory.path(), 1_048_576);
    let provenance = provenance(161);
    let mut completion = RunToCompletion;

    let error = run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &registry,
        authority(),
        &mut completion,
        receipts.bind(provenance.clone()),
    )
    .expect_err("observed allocation peak exceeds the plan claim");

    assert!(matches!(
        error,
        RunError::Evidence(ExecutionEvidenceError::ResourcePeakExceeded {
            node,
            resource: LeaseResource::Workers,
            planned: 1,
            actual: 2,
        }) if node == WorkNodeId::new("execute")
    ));
    assert_eq!(visible_generation.load(Ordering::SeqCst), 0);
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen allocation-overflow receipt");
    assert_eq!(
        receipt.failure_kind(),
        Some(ReceiptFailureKind::EvidenceContract)
    );
    assert_eq!(receipt.failure_node(), Some(WorkNodeId::new("execute")));
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("transaction-commit")),
        Some(ReceiptStatus::Cancelled)
    );
}

#[test]
fn synthetic_cancellation_rolls_back_before_visibility() {
    let skeleton = walking_skeleton();
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(
            implementation(6),
            publication_recording_executor(
                6,
                Arc::new(AtomicBool::new(false)),
                Arc::clone(&visible_generation),
            ),
        )]),
    };
    let directory = tempfile::tempdir().expect("cancelled receipt directory");
    let receipts = receipt_store(directory.path(), 1_048_576);
    let provenance = provenance(163);
    let mut cancellation = CancelAfterLaunch::default();

    let outcome = run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &registry,
        authority(),
        &mut cancellation,
        receipts.bind(provenance.clone()),
    )
    .expect("cancellation drains planned work and fences");

    assert_eq!(outcome, ExecutionOutcome::Cancelled);
    assert_eq!(visible_generation.load(Ordering::SeqCst), 0);
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen cancelled receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Cancelled);
    assert_eq!(receipt.failure_kind(), None);
    assert_eq!(
        receipt.node_status(&WorkNodeId::new("transaction-commit")),
        Some(ReceiptStatus::Cancelled)
    );
    assert!(
        receipt
            .artifact_identities()
            .into_iter()
            .all(|artifact| receipt.artifact_disposition(artifact)
                != Some(ArtifactDisposition::Published))
    );
}

#[test]
fn synthetic_mutation_output_and_publication_failures_remain_private() {
    let skeleton = walking_skeleton();
    for (seed, label, failure_node, publication_failure, expected_node) in [
        (
            165,
            "input mutation",
            Some("transaction-check"),
            None,
            "transaction-check",
        ),
        (
            167,
            "product output",
            Some("transaction-stage-psf"),
            None,
            "transaction-stage-psf",
        ),
        (
            169,
            "sole publication operation",
            None,
            Some("publication failure"),
            "transaction-commit",
        ),
    ] {
        let visible_generation = Arc::new(AtomicUsize::new(0));
        let registry = TestRegistry {
            id: registry(3),
            executors: BTreeMap::from([(
                implementation(6),
                failing_transaction_executor(
                    6,
                    Arc::clone(&visible_generation),
                    failure_node,
                    None,
                    publication_failure,
                ),
            )]),
        };
        let directory = tempfile::tempdir().expect("failure receipt directory");
        let receipts = receipt_store(directory.path(), 1_048_576);
        let provenance = provenance(seed);
        let mut completion = RunToCompletion;

        let error = run_receipted(
            &skeleton.problem,
            &skeleton.plan,
            &skeleton.current,
            &registry,
            authority(),
            &mut completion,
            receipts.bind(provenance.clone()),
        )
        .expect_err(label);

        assert!(
            matches!(error, RunError::Execution { ref node, .. } if node == &WorkNodeId::new(expected_node)),
            "{label} stopped at the wrong work node: {error}"
        );
        assert_eq!(
            visible_generation.load(Ordering::SeqCst),
            0,
            "{label} cannot expose staged output"
        );
        let receipt = receipts
            .open(provenance.attempt_id())
            .unwrap_or_else(|error| panic!("reopen {label} receipt: {error}"));
        assert_eq!(receipt.status(), ReceiptStatus::Failed, "{label}");
        assert_eq!(
            receipt.failure_kind(),
            Some(ReceiptFailureKind::Adapter),
            "{label}"
        );
        assert_eq!(
            receipt.failure_node(),
            Some(WorkNodeId::new(expected_node)),
            "{label}"
        );
        assert!(receipt.artifact_identities().into_iter().all(|artifact| {
            receipt.artifact_disposition(artifact) != Some(ArtifactDisposition::Published)
        }));
    }
}

#[test]
fn durable_prepare_failure_prevents_the_visibility_operation() {
    let skeleton = walking_skeleton();
    let directory = tempfile::tempdir().expect("prepare-failure receipt directory");
    let receipts = receipt_store(directory.path(), 80_000);
    let provenance = provenance(171);
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let prepared_observed = Arc::new(AtomicBool::new(false));
    let publication_calls = Arc::new(AtomicUsize::new(0));
    let mut executor = publication_recording_executor(
        6,
        Arc::clone(&publication_launched),
        Arc::clone(&visible_generation),
    );
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), {
            executor.publication_probe = Some(PublicationProbe {
                receipts: Arc::clone(&receipts),
                attempt: provenance.attempt_id(),
                prepared_observed: Arc::clone(&prepared_observed),
                publication_calls: Arc::clone(&publication_calls),
            });
            executor
        })]),
    };
    let mut completion = RunToCompletion;

    let error = run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &registry,
        authority(),
        &mut completion,
        receipts.bind(provenance.clone()),
    )
    .expect_err("prepared plus terminal evidence exceeds its joint reservation");

    assert!(matches!(
        error,
        RunError::Receipt(casa_imaging_runtime::ReceiptError::RetentionExceeded)
    ));
    assert!(publication_launched.load(Ordering::SeqCst));
    assert!(!prepared_observed.load(Ordering::SeqCst));
    assert_eq!(publication_calls.load(Ordering::SeqCst), 0);
    assert_eq!(visible_generation.load(Ordering::SeqCst), 0);
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen prepare-failure receipt");
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(receipt.failure_kind(), Some(ReceiptFailureKind::Scheduler));
    assert!(receipt.artifact_identities().into_iter().all(|artifact| {
        receipt.artifact_disposition(artifact) != Some(ArtifactDisposition::Published)
    }));
}

#[test]
fn receipt_promotion_failure_retains_prepared_reconciliation_evidence() {
    let skeleton = walking_skeleton();
    let directory = tempfile::tempdir().expect("promotion-failure receipt directory");
    let receipts = receipt_store(directory.path(), 1_048_576);
    let provenance = provenance(173);
    let publication_launched = Arc::new(AtomicBool::new(false));
    let visible_generation = Arc::new(AtomicUsize::new(0));
    let prepared_observed = Arc::new(AtomicBool::new(false));
    let publication_calls = Arc::new(AtomicUsize::new(0));
    let mut executor = publication_recording_executor(
        6,
        Arc::clone(&publication_launched),
        Arc::clone(&visible_generation),
    );
    executor.publication_probe = Some(PublicationProbe {
        receipts: Arc::clone(&receipts),
        attempt: provenance.attempt_id(),
        prepared_observed: Arc::clone(&prepared_observed),
        publication_calls: Arc::clone(&publication_calls),
    });
    executor.receipt_root_to_disrupt = Some(directory.path().to_owned());
    let registry = TestRegistry {
        id: registry(3),
        executors: BTreeMap::from([(implementation(6), executor)]),
    };
    let mut completion = RunToCompletion;

    let outcome = run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &registry,
        authority(),
        &mut completion,
        receipts.bind(provenance.clone()),
    )
    .expect("visibility is irreversible after the terminal candidate was prepared");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    assert!(publication_launched.load(Ordering::SeqCst));
    assert!(prepared_observed.load(Ordering::SeqCst));
    assert_eq!(publication_calls.load(Ordering::SeqCst), 1);
    assert_eq!(visible_generation.load(Ordering::SeqCst), 1);
    let receipt = receipts
        .open(provenance.attempt_id())
        .expect("reopen prepared reconciliation receipt");
    assert_eq!(receipt.status(), ReceiptStatus::PublicationPrepared);
    for layout in skeleton.plan.publication_layouts().entries() {
        assert_eq!(
            receipt.artifact_disposition(layout.artifact()),
            Some(ArtifactDisposition::Staged)
        );
        assert_eq!(
            receipt.publication_participant(layout.artifact()),
            Some(receipt_participant(layout.participant()))
        );
        assert_eq!(
            receipt.publication_layout_identity(layout.artifact()),
            Some(layout.layout_id())
        );
    }
}
