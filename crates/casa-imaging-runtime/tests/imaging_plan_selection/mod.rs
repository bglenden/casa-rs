// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T16 evidence for ADR-0010 lexicographic planning order and
//! machine-readable infeasibility certificates.

use std::io;

use casa_imaging_runtime::{
    AlternativeRejectionReason, ExecutionPlan, ExecutionReceiptStore,
    PlannerCostModelProfileBootstrap, ReceiptRetention,
};

mod support;

use self::support::{
    AlternativeId, ContractOnlyRegistry, ExecutionDag, ExecutionDagSpecification,
    PhysicalWorkBinding, PlanError, PlanPrediction, PlanningBindings, PredictionConfidence,
    PredictionUncertainty, ResourcePolicy, StagePrediction, WorkImplementationId, authority,
    compile, cost_model, implementation_catalog, implementation_metadata, physical_work, registry,
    request, run_lock, runtime_plan,
};

fn candidate(
    problem: &casa_imaging_model::CompiledProblem,
    base: &PhysicalWorkBinding,
    alternative: &str,
    stage_nanos: u64,
    memory_scale: u64,
) -> PhysicalWorkBinding {
    let dag = base.execution_dag();
    let mut alternative_decl = dag.resource_alternative().clone();
    alternative_decl.id = AlternativeId::new(alternative);
    if memory_scale > 1 {
        for demand in &mut alternative_decl.demand.memory {
            demand.hard_bytes *= memory_scale;
            demand.preferred_bytes *= memory_scale;
        }
    }
    let stages = base
        .prediction()
        .stages()
        .iter()
        .map(|(node, stage)| {
            StagePrediction::new(node.clone(), stage_nanos).with_io(stage.io().to_vec())
        })
        .collect();
    let prediction = PlanPrediction::new(
        stage_nanos * u64::try_from(base.prediction().stages().len()).expect("stage count"),
        PredictionConfidence::new(900_000).expect("confidence"),
        vec![PredictionUncertainty::new("source-throughput", 50)],
        stages,
    )
    .expect("complete variant prediction");
    let mut slots = dag.physical_slots().values().cloned().collect::<Vec<_>>();
    if memory_scale > 1 {
        for slot in &mut slots {
            slot.capacity_bytes *= memory_scale;
        }
    }
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: dag.required_resource_capabilities().clone(),
        resource_alternative: alternative_decl,
        nodes: dag.nodes().values().cloned().collect(),
        logical_allocations: dag.logical_allocations().values().cloned().collect(),
        physical_slots: slots,
        initial_knobs: dag.initial_knobs().clone(),
        adaptations: dag.adaptations().values().cloned().collect(),
    };
    let dag = ExecutionDag::new(specification).expect("variant physical DAG");
    super::native_product_physical_work(
        problem,
        implementation_catalog(problem, &dag),
        dag,
        prediction,
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("variant physical work")
}

fn multi_candidate_plan(
    problem: &casa_imaging_model::CompiledProblem,
    candidates: Vec<PhysicalWorkBinding>,
) -> Result<ExecutionPlan, PlanError<io::Error>> {
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let directory = tempfile::tempdir().expect("empty receipt directory");
    let root = directory.keep();
    let receipts = ExecutionReceiptStore::new(
        root,
        ReceiptRetention::new(4, 1_048_576).expect("retention"),
    )
    .expect("empty receipt store");
    let implementation_ids = candidates.iter().flat_map(|candidate| {
        candidate
            .execution_dag()
            .nodes()
            .values()
            .map(|node| node.implementation.clone())
            .collect::<Vec<_>>()
    });
    let contract_registry = ContractOnlyRegistry::new(
        registry(3),
        implementation_metadata(problem),
        implementation_ids,
    );
    runtime_plan(
        problem,
        PlanningBindings::new(
            registry(3),
            ResourcePolicy::Exclusive,
            PlannerCostModelProfileBootstrap::new(cost_model(4)),
        ),
        authority(),
        &contract_registry,
        &receipts,
        |_, _| Ok(candidates),
    )
}

#[test]
fn planning_commits_to_the_minimum_conservative_predicted_time_among_feasible_candidates() {
    let problem = compile(request(1)).expect("logical compilation");
    let base = physical_work(6);
    let slow = candidate(&problem, &base, "alt-slow", 300, 1);
    let fast = candidate(&problem, &base, "alt-fast", 100, 1);
    assert!(
        fast.prediction().conservative_nanos() < slow.prediction().conservative_nanos(),
        "the fixture must offer a strictly slower conservative prediction"
    );

    let selected = multi_candidate_plan(&problem, vec![slow, fast])
        .expect("one feasible candidate admits the plan");

    assert_eq!(
        selected.execution_dag().resource_alternative().id,
        AlternativeId::new("alt-fast"),
        "planning commits to the minimum conservative predicted wall time"
    );
    // The sealed legal plan exposes prediction confidence and dominant
    // uncertainty terms.
    assert_eq!(
        selected.prediction().confidence().parts_per_million(),
        900_000
    );
    let uncertainty = selected.prediction().uncertainty();
    assert_eq!(uncertainty.len(), 1);
    assert_eq!(uncertainty[0].identity(), "source-throughput");
    assert_eq!(uncertainty[0].predicted_nanos(), 50);
}

#[test]
fn hard_feasibility_precedes_predicted_time_in_lexicographic_planning() {
    let problem = compile(request(1)).expect("logical compilation");
    let base = physical_work(6);
    let fast_infeasible = candidate(&problem, &base, "alt-fast-infeasible", 100, 2_000_000);
    let slow_feasible = candidate(&problem, &base, "alt-slow-feasible", 400, 1);

    let selected = multi_candidate_plan(&problem, vec![fast_infeasible, slow_feasible])
        .expect("the feasible candidate admits the plan");

    assert_eq!(
        selected.execution_dag().resource_alternative().id,
        AlternativeId::new("alt-slow-feasible"),
        "hard capacity feasibility outranks predicted time"
    );
}

#[test]
fn planning_compares_distinct_registry_implementation_alternatives() {
    let problem = compile(request(1)).expect("logical compilation");
    let base = physical_work(6);
    let slow = candidate(&problem, &base, "alt-slow", 300, 1);
    let fast = candidate(&problem, &base, "alt-fast", 100, 1);

    // A physically valid variant that swaps one node's implementation: same
    // capabilities and resource shape, different numerics-bearing semantics.
    let dag = fast.execution_dag();
    let mut nodes = dag.nodes().values().cloned().collect::<Vec<_>>();
    let swapped = WorkImplementationId::new("variant-deconvolver".to_string());
    assert_ne!(nodes[0].implementation, swapped);
    nodes[0].implementation = swapped.clone();
    let specification = ExecutionDagSpecification {
        required_resource_capabilities: dag.required_resource_capabilities().clone(),
        resource_alternative: dag.resource_alternative().clone(),
        nodes,
        logical_allocations: dag.logical_allocations().values().cloned().collect(),
        physical_slots: dag.physical_slots().values().cloned().collect(),
        initial_knobs: dag.initial_knobs().clone(),
        adaptations: dag.adaptations().values().cloned().collect(),
    };
    let divergent_dag = ExecutionDag::new(specification).expect("variant physical DAG");
    let divergent = super::native_product_physical_work(
        &problem,
        implementation_catalog(&problem, &divergent_dag),
        divergent_dag,
        fast.prediction().clone(),
        fast.artifacts().to_vec(),
        fast.observation_transaction().clone(),
        fast.publication_layouts().clone(),
    )
    .expect("variant physical work");

    let selected = multi_candidate_plan(&problem, vec![slow, divergent])
        .expect("distinct registry implementations may compete on timing");
    assert_eq!(
        selected.execution_dag().resource_alternative().id,
        AlternativeId::new("alt-fast"),
        "implementation identity does not override the lexicographic resource/time order"
    );
}

#[test]
fn planning_rejects_an_explicit_science_contract_mismatch() {
    let problem = compile(request(1)).expect("logical compilation");
    let other_problem = compile(request(2)).expect("distinct logical compilation");
    assert_ne!(problem.problem_id(), other_problem.problem_id());
    let base = physical_work(6);
    let dag = base.execution_dag().clone();
    let divergent = super::native_product_physical_work(
        &problem,
        implementation_catalog(&other_problem, &dag),
        dag,
        base.prediction().clone(),
        base.artifacts().to_vec(),
        base.observation_transaction().clone(),
        base.publication_layouts().clone(),
    )
    .expect("candidate binding accepts an explicit registry declaration");

    let error = multi_candidate_plan(&problem, vec![divergent])
        .expect_err("the compiled numerics contract must be checked before timing");
    assert!(matches!(error, PlanError::InvalidCandidate(_)));
}

#[test]
fn planning_reports_machine_readable_infeasibility_when_no_candidate_fits() {
    let problem = compile(request(1)).expect("logical compilation");
    let base = physical_work(6);
    // Both candidates exceed host memory once their plan-owned slots are
    // scaled; the slower one sorts later so rejection order is deterministic.
    let oversized_fast = candidate(&problem, &base, "alt-oversized-fast", 100, 2_000_000);
    let oversized_slow = candidate(&problem, &base, "alt-oversized-slow", 400, 2_000_000);

    let error = multi_candidate_plan(&problem, vec![oversized_slow, oversized_fast])
        .expect_err("no candidate fits current policy, pressure, and reservations");

    let certificate = error
        .infeasibility_certificate()
        .expect("a refused plan exposes its admission certificate");
    let rejections = certificate.rejections();
    assert_eq!(rejections.len(), 2);
    for (index, expected) in [(0, "alt-oversized-fast"), (1, "alt-oversized-slow")] {
        assert_eq!(
            rejections[index].alternative(),
            &AlternativeId::new(expected)
        );
        match rejections[index].reason() {
            AlternativeRejectionReason::Infeasible {
                resource,
                required,
                available,
            } => {
                assert!(resource.starts_with("memory-domain:host-memory"));
                assert!(*required > *available);
            }
            other => panic!("expected a hard-capacity refusal, got {other:?}"),
        }
    }
    let rendered = certificate.to_string();
    assert!(rendered.contains("alt-oversized-fast"));
    assert!(rendered.contains("alt-oversized-slow"));
}
