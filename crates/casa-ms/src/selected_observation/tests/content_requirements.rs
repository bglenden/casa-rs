// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::SelectedObservationContentPlanError;

#[test]
fn t51_content_requirements_admit_the_exact_minimum_and_bound_live_blocks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("requirements.ms");
    generate_fixture(&path);
    initialize_measurement_set_owner_manifest(&path).unwrap();
    let (problem, access) = owner_problem_and_access(owner_resolution_request(&path, 2));
    let requirements = access.content_requirements(&problem).unwrap();
    let minimum = requirements.minimum_bytes(2).unwrap();
    assert!(
        requirements
            .plan(SelectedObservationContentBudget::new(minimum - 1, 2, 4))
            .is_err()
    );
    let budget = SelectedObservationContentBudget::new(minimum, 2, 4);
    let plan = requirements.plan(budget).unwrap();
    assert!(plan.rows_per_block() >= 1);
    assert_eq!(plan.maximum_resident_bytes(), minimum);
    assert_eq!(
        requirements.bytes_for_rows(usize::MAX, 2).unwrap(),
        requirements.bytes_for_rows(2, 2).unwrap()
    );
    assert!(requirements.minimum_bytes(3).unwrap() >= minimum);
    assert!(matches!(
        requirements.bytes_for_rows(0, 2),
        Err(SelectedObservationContentPlanError::InvalidBudget)
    ));
    assert!(matches!(
        requirements.minimum_bytes(0),
        Err(SelectedObservationContentPlanError::InvalidBudget)
    ));
    assert!(matches!(
        requirements.plan(SelectedObservationContentBudget::new(minimum, 2, 3)),
        Err(SelectedObservationContentPlanError::InvalidBudget)
    ));

    let access = access
        .with_content_budget(&problem, &requirements, budget)
        .unwrap();
    assert_eq!(access.source_binding().content_budget(), budget);
    let deferred = access.into_deferred();
    let certificate = deferred.certify_residency(&problem).unwrap();
    let mut opened = deferred.open(&problem).unwrap();
    assert_eq!(opened.residency_certificate(), &certificate);
    opened
        .traverse(&problem, |_| Ok::<_, Infallible>(()))
        .unwrap();
}

#[test]
fn t51_content_requirements_release_locks_and_reject_stale_owner_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("stale-requirements.ms");
    generate_fixture(&path);
    initialize_measurement_set_owner_manifest(&path).unwrap();
    let (problem, access) = owner_problem_and_access(owner_resolution_request(&path, 2));
    let requirements = access.content_requirements(&problem).unwrap();
    external_locked_keyword_mutation(&path);
    let error = access.content_requirements(&problem).unwrap_err();
    assert!(matches!(
        error,
        super::super::BoundSelectedObservationError::Source { .. }
    ));
    let budget =
        SelectedObservationContentBudget::new(requirements.minimum_bytes(1).unwrap(), 1, 4);
    let finalized = access
        .with_content_budget(&problem, &requirements, budget)
        .unwrap();
    assert!(
        finalized.into_deferred().open(&problem).is_err(),
        "fresh execution validation must close the sizing/open mutation gap"
    );
}

#[test]
fn t51_content_requirements_cannot_finalize_another_source() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.ms");
    let second = directory.path().join("second.ms");
    for path in [&first, &second] {
        generate_fixture(path);
        initialize_measurement_set_owner_manifest(path).unwrap();
    }
    let (first_problem, first_access) =
        owner_problem_and_access(owner_resolution_request(&first, 2));
    let (second_problem, second_access) =
        owner_problem_and_access(owner_resolution_request(&second, 2));
    let requirements = first_access.content_requirements(&first_problem).unwrap();
    let budget =
        SelectedObservationContentBudget::new(requirements.minimum_bytes(1).unwrap(), 1, 4);
    assert!(matches!(
        second_access.with_content_budget(&second_problem, &requirements, budget),
        Err(super::super::BoundSelectedObservationError::ProblemMismatch)
    ));
}

#[test]
fn t51_content_requirements_reject_equal_science_with_different_provenance() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("provenance.ms");
    generate_fixture(&path);
    initialize_measurement_set_owner_manifest(&path).unwrap();
    let (first_problem, first_access) = owner_problem_and_access(
        owner_resolution_request_with_identity(&path, 2, identity(211)),
    );
    let (second_problem, second_access) = owner_problem_and_access(
        owner_resolution_request_with_identity(&path, 2, identity(212)),
    );
    assert_eq!(first_problem.problem_id(), second_problem.problem_id());
    assert_ne!(
        first_problem
            .inputs()
            .observation_snapshot()
            .provenance_id(),
        second_problem
            .inputs()
            .observation_snapshot()
            .provenance_id(),
    );
    let requirements = first_access.content_requirements(&first_problem).unwrap();
    let budget =
        SelectedObservationContentBudget::new(requirements.minimum_bytes(1).unwrap(), 1, 4);
    assert!(matches!(
        second_access.with_content_budget(&second_problem, &requirements, budget),
        Err(super::super::BoundSelectedObservationError::ProblemMismatch),
    ));
    first_access
        .with_content_budget(&first_problem, &requirements, budget)
        .unwrap();
}

#[test]
fn t51_content_requirements_catalog_budget_charges_shared_source_plan_once() {
    use super::super::content_plan::{
        SelectedObservationSharedBytes, selected_content_requirements,
        selected_pointing_catalog_budget,
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("shared-source-plan.ms");
    generate_fixture(&path);
    let problem = compiled_problem(&path, 2);
    let source = &problem.inputs().observation_snapshot().sources()[0];
    let measurement_set = MeasurementSet::open_retained_read(&path).unwrap();
    let budget = SelectedObservationContentBudget::new(1 << 20, 2, 4);
    let base = SelectedObservationSharedBytes::new(97, 31, 211, 79);
    let with_source = base.with_source_plan_retained_bytes(4096);
    let unreserved =
        selected_pointing_catalog_budget(&measurement_set, &problem, source, base, budget).unwrap();
    let reserved =
        selected_pointing_catalog_budget(&measurement_set, &problem, source, with_source, budget)
            .unwrap();
    assert_eq!(unreserved - reserved, 4096);
    let requirements = |shared| {
        selected_content_requirements(&measurement_set, &problem, source, shared, 4, None, 0)
            .unwrap()
    };
    assert_eq!(
        requirements(with_source).minimum_bytes(2).unwrap()
            - requirements(base).minimum_bytes(2).unwrap(),
        4096
    );
    assert!(
        requirements(base).minimum_bytes(2).unwrap()
            > requirements(SelectedObservationSharedBytes::NONE)
                .minimum_bytes(2)
                .unwrap()
    );
}
