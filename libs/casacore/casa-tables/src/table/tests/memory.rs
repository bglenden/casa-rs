// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

fn memory_schema() -> TableSchema {
    TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
    ])
    .unwrap()
}

fn memory_row(id: i32, name: &str) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
        RecordField::new("name", Value::Scalar(ScalarValue::String(name.into()))),
    ])
}

#[test]
fn new_memory_creates_transient_table() {
    let table = Table::new_memory();
    assert!(table.is_memory());
    assert_eq!(table.table_kind(), super::super::TableKind::Memory);
    assert_eq!(table.row_count(), 0);
    assert!(table.path().is_none());
}

#[test]
fn with_schema_memory_validates_rows() {
    let mut table = Table::with_schema_memory(memory_schema());
    assert!(table.is_memory());
    table.add_row(memory_row(1, "alice")).unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn from_rows_memory_basic() {
    let rows = vec![memory_row(1, "a"), memory_row(2, "b")];
    let table = Table::from_rows_memory(rows);
    assert!(table.is_memory());
    assert_eq!(table.row_count(), 2);
}

#[test]
fn from_rows_with_schema_memory_validates() {
    let rows = vec![memory_row(1, "a")];
    let table = Table::from_rows_with_schema_memory(rows, memory_schema()).unwrap();
    assert!(table.is_memory());
    assert_eq!(table.row_count(), 1);
}

#[test]
fn memory_table_full_crud_cycle() {
    let mut table = Table::with_schema_memory(memory_schema());
    // add_row
    table.add_row(memory_row(1, "alice")).unwrap();
    table.add_row(memory_row(2, "bob")).unwrap();
    assert_eq!(table.row_count(), 2);

    // set_cell
    table_set_cell(
        &mut table,
        0,
        "name",
        Value::Scalar(ScalarValue::String("ALICE".into())),
    )
    .unwrap();
    assert_eq!(
        table_cell(&table, 0, "name"),
        Ok(Some(&Value::Scalar(ScalarValue::String("ALICE".into()))))
    );

    // remove_rows
    table.remove_rows(&[1]).unwrap();
    assert_eq!(table.row_count(), 1);

    // add_column
    table
        .add_column(
            ColumnSchema::scalar("score", PrimitiveType::Float64),
            Some(Value::Scalar(ScalarValue::Float64(0.0))),
        )
        .unwrap();
    assert!(table.schema().unwrap().contains_column("score"));

    // remove_column
    table.remove_column("score").unwrap();
    assert!(!table.schema().unwrap().contains_column("score"));
}

#[test]
fn memory_table_save_materializes_to_disk() {
    let mut table = Table::with_schema_memory(memory_schema());
    table.add_row(memory_row(42, "test")).unwrap();
    table.keywords_mut().push(RecordField::new(
        "origin",
        Value::Scalar(ScalarValue::String("memory".into())),
    ));

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("materialized.tbl");
    table.save(TableOptions::new(&path)).unwrap();

    // Reopen as a plain table.
    let reopened = Table::open(TableOptions::new(&path)).unwrap();
    assert!(!reopened.is_memory());
    assert_eq!(reopened.row_count(), 1);
    assert_eq!(
        table_cell(&reopened, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(42))))
    );
    assert_eq!(
        reopened.keywords().get("origin"),
        Some(&Value::Scalar(ScalarValue::String("memory".into())))
    );
}

#[test]
fn memory_table_save_with_both_data_managers() {
    for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let mut table = Table::with_schema_memory(memory_schema());
        table.add_row(memory_row(1, "a")).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(format!("test_{dm:?}.tbl"));
        table
            .save(TableOptions::new(&path).with_data_manager(dm))
            .unwrap();

        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        assert_eq!(reopened.row_count(), 1);
    }
}

#[test]
fn to_memory_copies_all_data() {
    let mut plain = Table::with_schema(memory_schema());
    plain.add_row(memory_row(1, "orig")).unwrap();
    plain.keywords_mut().push(RecordField::new(
        "key",
        Value::Scalar(ScalarValue::Int32(99)),
    ));

    let mem = plain.to_memory().unwrap();
    assert!(mem.is_memory());
    assert!(mem.path().is_none());
    assert_eq!(mem.row_count(), 1);
    assert_eq!(
        table_cell(&mem, 0, "id"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(1))))
    );
    assert_eq!(
        mem.keywords().get("key"),
        Some(&Value::Scalar(ScalarValue::Int32(99)))
    );
    assert!(mem.schema().is_some());
}

#[test]
fn to_memory_from_disk_table() {
    let mut table = Table::with_schema(memory_schema());
    table.add_row(memory_row(5, "disk")).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("source.tbl");
    table.save(TableOptions::new(&path)).unwrap();

    let disk = Table::open(TableOptions::new(&path)).unwrap();
    let mem = disk.to_memory().unwrap();
    assert!(mem.is_memory());
    assert!(mem.path().is_none());
    assert_eq!(mem.row_count(), 1);
}

#[test]
fn memory_table_sort_and_select() {
    let mut table = Table::with_schema_memory(memory_schema());
    for (id, name) in [(3, "c"), (1, "a"), (2, "b")] {
        table.add_row(memory_row(id, name)).unwrap();
    }

    // Sort.
    let sorted = table
        .sort(&[("id", super::super::SortOrder::Ascending)])
        .unwrap();
    assert_eq!(sorted.row_count(), 3);
    assert_eq!(
        sorted.cell(0, "id").unwrap(),
        Some(&Value::Scalar(ScalarValue::Int32(1)))
    );
    drop(sorted);

    // Select by predicate.
    let view = table
        .select(
            |row| matches!(row.get("id"), Some(Value::Scalar(ScalarValue::Int32(i))) if *i >= 2),
        )
        .unwrap();
    assert_eq!(view.row_count(), 2);
}

#[test]
fn memory_table_iter_groups() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("group", PrimitiveType::String),
        ColumnSchema::scalar("val", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema_memory(schema);
    for (g, v) in [("a", 1), ("b", 2), ("a", 3)] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("group", Value::Scalar(ScalarValue::String(g.into()))),
                RecordField::new("val", Value::Scalar(ScalarValue::Int32(v))),
            ]))
            .unwrap();
    }

    let groups: Vec<_> = table
        .iter_groups(&[("group", super::super::SortOrder::Ascending)])
        .unwrap()
        .collect();
    assert_eq!(groups.len(), 2);
}

#[cfg(unix)]
#[test]
fn memory_table_lock_is_noop() {
    use crate::lock::LockType;

    let table = Table::new_memory();
    assert!(table.has_lock(LockType::Write));
    assert!(table.has_lock(LockType::Read));
    assert!(!table.is_multi_used());
}

#[cfg(unix)]
#[test]
fn memory_table_lock_unlock_succeed() {
    use crate::lock::LockType;

    let mut table = Table::new_memory();
    assert!(table.lock(LockType::Write, 1).unwrap());
    table.unlock().unwrap();
}

#[test]
fn plain_table_kind_is_default() {
    let table = Table::new();
    assert!(!table.is_memory());
    assert_eq!(table.table_kind(), super::super::TableKind::Plain);
}
