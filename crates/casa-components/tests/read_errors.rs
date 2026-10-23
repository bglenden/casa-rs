// SPDX-License-Identifier: LGPL-3.0-or-later
//! What casa-rs refuses when it reads a component list, and the lists it
//! writes at the edges (simulation Phase 2, #841).

mod common;

use casa_components::{ComponentError, ComponentList, ComponentListError};
use casa_tables::{DataManagerKind, Table, TableOptions};
use casa_types::{ScalarValue, Value};
use common::{copy_table, fixture_dir};

/// The fixture with one cell or column changed, saved as a new table.
fn altered_fixture(
    scratch: &std::path::Path,
    alter: impl FnOnce(&mut Table),
) -> std::path::PathBuf {
    let source = copy_table(&fixture_dir().join("old_format.cl"), scratch);
    let mut table = Table::open(TableOptions::new(&source)).expect("open");
    alter(&mut table);
    let altered = scratch.join("altered.cl");
    table
        .save(TableOptions::new(&altered).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save altered");
    altered
}

fn set_string(table: &mut Table, row: usize, column: &str, value: &str) {
    table
        .cell_accessor_mut(row, column)
        .expect("cell")
        .set(Value::Scalar(ScalarValue::String(value.to_string())))
        .expect("set");
}

#[test]
fn frames_casa_types_lacks_are_named() {
    let scratch = tempfile::tempdir().expect("scratch");
    let altered = altered_fixture(scratch.path(), |table| {
        set_string(table, 0, "Direction_Frame", "MOON")
    });
    let error = ComponentList::read_table(&altered).expect_err("MOON is not a casa-types frame");
    assert!(
        matches!(&error, ComponentListError::Cell { row: 0, column, reason, .. }
            if column == "Reference_Direction" && reason.contains("MOON")),
        "{error}"
    );
}

#[test]
fn unknown_names_are_refused_as_casa_refuses_them() {
    let scratch = tempfile::tempdir().expect("scratch");
    let altered = altered_fixture(scratch.path(), |table| {
        set_string(table, 1, "Shape", "ring")
    });
    let error = ComponentList::read_table(&altered).expect_err("unknown shape");
    assert!(
        matches!(
            &error,
            ComponentListError::Component { row: 1, source: ComponentError::UnknownShape(name), .. } if name == "ring"
        ),
        "{error}"
    );
    // A minimum match is a shape, as in CASA.
    let scratch = tempfile::tempdir().expect("scratch");
    let altered = altered_fixture(scratch.path(), |table| {
        set_string(table, 1, "Shape", "gaus")
    });
    assert!(ComponentList::read_table(&altered).is_ok());
}

#[test]
fn tables_without_required_columns_are_not_component_lists() {
    let scratch = tempfile::tempdir().expect("scratch");
    let altered = altered_fixture(scratch.path(), |table| {
        table.remove_column("Shape_Parameters").expect("remove");
    });
    let error = ComponentList::read_table(&altered).expect_err("no Shape_Parameters");
    assert!(
        matches!(
            error,
            ComponentListError::MissingColumn {
                column: "Shape_Parameters",
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn empty_lists_round_trip() {
    let scratch = tempfile::tempdir().expect("scratch");
    let path = scratch.path().join("empty.cl");
    ComponentList::new().write_table(&path).expect("write");
    let back = ComponentList::read_table(&path).expect("read");
    assert!(back.is_empty());
    let table = Table::open(TableOptions::new(&path)).expect("open");
    let schema = table.schema().expect("schema");
    assert!(schema.contains_column("Spectral_Record"));
    assert!(!schema.contains_column("Optional_Parameters"));
}
