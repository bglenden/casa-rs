// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::SelectedObservationContentPlanError;

#[test]
fn t51_content_requirements_admit_the_exact_minimum_and_bound_live_blocks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("requirements.ms");
    generate_fixture(&path);
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
fn t51_content_requirements_cannot_finalize_another_source() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.ms");
    let second = directory.path().join("second.ms");
    for path in [&first, &second] {
        generate_fixture(path);
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

/// Copy the generated `source` MeasurementSet to `destination` with MAIN
/// `column` stored by `manager`, as CASA may have chosen.
fn copy_with_main_column_stored_by(
    source: &std::path::Path,
    destination: &std::path::Path,
    column: &str,
    manager: casa_tables::DataManagerKind,
) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &destination.join(entry.file_name()));
        }
    }
    let mut measurement_set = MeasurementSet::open(source).unwrap();
    let mut bindings = crate::ms::measurement_set_main_table_bindings(measurement_set.main_table());
    bindings.insert(
        column.to_string(),
        casa_tables::ColumnBinding {
            data_manager: manager,
            tile_shape: None,
        },
    );
    measurement_set
        .main_table_mut()
        .prepare_write()
        .save_with_bindings(
            crate::ms::measurement_set_table_options(destination),
            &bindings,
        )
        .unwrap();
}

fn copy_tree(source: &std::path::Path, destination: &std::path::Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn main_column_manager(path: &std::path::Path, column: &str) -> String {
    let measurement_set = MeasurementSet::open(path).unwrap();
    measurement_set
        .main_table()
        .data_manager_info()
        .iter()
        .find(|manager| manager.columns.iter().any(|name| name == column))
        .map(|manager| manager.dm_type.clone())
        .unwrap()
}

/// A column its data manager reads cell by cell (here StandardStMan DATA)
/// holds every selected row's whole stored cell before the selected channels
/// are packed. The plan charges that cell, so a narrow selection of a wide
/// column admits fewer rows per block than the channel-bounded
/// TiledShapeStMan layout, whose charge is unchanged.
#[test]
fn whole_cell_reads_charge_the_stored_cell_and_admit_fewer_rows() {
    const CHANNELS: usize = 1024;
    const ROWS: usize = 8;
    let directory = tempfile::tempdir().unwrap();
    let tiled = directory.path().join("tiled.ms");
    generate_fixture_with_channel_count(&tiled, ROWS, CHANNELS);
    let standard = directory.path().join("standard.ms");
    copy_with_main_column_stored_by(
        &tiled,
        &standard,
        "DATA",
        casa_tables::DataManagerKind::StandardStMan,
    );
    assert_eq!(main_column_manager(&tiled, "DATA"), "TiledShapeStMan");
    assert_eq!(main_column_manager(&standard, "DATA"), "StandardStMan");

    let full_cell_bytes = {
        let measurement_set = MeasurementSet::open(&standard).unwrap();
        let cell = measurement_set
            .main_table()
            .cell_accessor(0, "DATA")
            .and_then(|cell| cell.array())
            .unwrap()
            .clone();
        assert_eq!(cell.shape()[1], CHANNELS);
        cell.len() * size_of::<casa_types::Complex32>()
    };
    let resolved = |path: &std::path::Path| {
        owner_problem_and_access(owner_resolution_request_with_channels(path, ROWS, vec![0]))
    };
    let (tiled_problem, tiled_access) = resolved(&tiled);
    let (standard_problem, standard_access) = resolved(&standard);
    let tiled = tiled_access.content_requirements(&tiled_problem).unwrap();
    let standard = standard_access
        .content_requirements(&standard_problem)
        .unwrap();
    let per_row = |requirements: crate::SelectedObservationContentRequirements| {
        requirements.bytes_for_rows(ROWS, 1).unwrap()
            - requirements.bytes_for_rows(ROWS - 1, 1).unwrap()
    };
    assert!(
        per_row(standard) >= full_cell_bytes,
        "a whole-cell read charges {} bytes per row for a {full_cell_bytes}-byte stored cell",
        per_row(standard)
    );
    assert!(per_row(tiled) < full_cell_bytes);

    let budget =
        SelectedObservationContentBudget::new(standard.bytes_for_rows(2, 1).unwrap(), 1, 4);
    let standard_rows = standard.plan(budget).unwrap().rows_per_block();
    let tiled_rows = tiled.plan(budget).unwrap().rows_per_block();
    assert_eq!(standard_rows, 2);
    assert!(
        standard_rows < tiled_rows,
        "whole-cell DATA admits {standard_rows} rows, channel-bounded DATA {tiled_rows}"
    );

    // The whole-cell layout reads through the bounded traversal it was
    // planned for.
    let mut opened = standard_access
        .with_content_budget(&standard_problem, &standard, budget)
        .unwrap()
        .into_deferred()
        .open(&standard_problem)
        .unwrap();
    opened
        .traverse(&standard_problem, |_| Ok::<_, Infallible>(()))
        .unwrap();
}
