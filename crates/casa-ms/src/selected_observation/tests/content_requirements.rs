// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::SelectedObservationContentPlanError;

#[test]
fn t51_content_requirements_admit_the_exact_minimum() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("requirements.ms");
    generate_fixture(&path);
    let (problem, access) = owner_problem_and_access(owner_resolution_request(&path, 2));
    let requirements = access.content_requirements(&problem).unwrap();
    let minimum = requirements.minimum_bytes().unwrap();
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
        requirements.bytes_for_rows(usize::MAX).unwrap(),
        requirements.bytes_for_rows(2).unwrap()
    );
    assert!(matches!(
        requirements.bytes_for_rows(0),
        Err(SelectedObservationContentPlanError::InvalidBudget)
    ));
    assert!(matches!(
        requirements.plan(SelectedObservationContentBudget::new(minimum, 2, 3)),
        Err(SelectedObservationContentPlanError::InvalidBudget)
    ));

    let access = access.with_content_budget(budget);
    assert_eq!(access.source_binding().content_budget(), budget);
    let opened = access.into_deferred().open(&problem).unwrap();
    let (_, samples) = stream(&problem, opened).unwrap();
    assert_eq!(samples.len(), 8);
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
    let base = SelectedObservationSharedBytes::new(97, 31);
    let with_source = base.with_source_plan_retained_bytes(4096);
    let unreserved =
        selected_pointing_catalog_budget(&measurement_set, source, base, budget).unwrap();
    let reserved =
        selected_pointing_catalog_budget(&measurement_set, source, with_source, budget).unwrap();
    assert_eq!(unreserved - reserved, 4096);
    let requirements = |shared| {
        selected_content_requirements(&measurement_set, &problem, source, shared, 4, None, 0)
            .unwrap()
    };
    assert_eq!(
        requirements(with_source).minimum_bytes().unwrap()
            - requirements(base).minimum_bytes().unwrap(),
        4096
    );
    assert!(
        requirements(base).minimum_bytes().unwrap()
            > requirements(SelectedObservationSharedBytes::NONE)
                .minimum_bytes()
                .unwrap()
    );
}
