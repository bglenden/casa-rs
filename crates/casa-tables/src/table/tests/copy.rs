// SPDX-License-Identifier: LGPL-3.0-or-later
//! Copies of a table written afresh (`Table::shallow_copy`) and the byte
//! order a table is stored in (`Table::stored_endian_format`).

use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use super::unique_test_dir;
use crate::schema::{ColumnSchema, TableSchema};
use crate::storage::table_control::{TableDatResult, read_table_dat_dispatch};
use crate::table::{DataManagerKind, EndianFormat, Table, TableOptions};

/// A table of three rows in one `StManAipsIO` group named `group`, stored in
/// `format`, and its directory.
fn grouped_table(prefix: &str, group: &str, format: EndianFormat) -> std::path::PathBuf {
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).expect("schema");
    let mut table = Table::with_schema(schema);
    for row in 0..3 {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(row)),
            )]))
            .expect("push row");
    }
    let root = unique_test_dir(prefix);
    std::fs::create_dir_all(&root).expect("create test dir");
    let path = root.join("grouped.table");
    table
        .save(
            TableOptions::new(&path)
                .with_data_manager(DataManagerKind::StManAipsIO)
                .with_data_manager_group(group)
                .with_endian_format(format),
        )
        .expect("save");
    path
}

fn group_of(path: &std::path::Path, column: &str) -> String {
    let TableDatResult::Plain(table_dat) =
        read_table_dat_dispatch(&path.join("table.dat")).expect("table.dat")
    else {
        panic!("not a plain table");
    };
    table_dat
        .table_desc
        .columns
        .iter()
        .find(|description| description.col_name == column)
        .expect("column")
        .data_manager_group
        .clone()
}

#[test]
fn a_table_reports_the_byte_order_it_is_stored_in() {
    for (format, big) in [
        (EndianFormat::BigEndian, true),
        (EndianFormat::LittleEndian, false),
    ] {
        let path = grouped_table("stored_endian", "", format);
        let table = Table::open(TableOptions::new(&path)).expect("open");
        let expected = if big {
            EndianFormat::BigEndian
        } else {
            EndianFormat::LittleEndian
        };
        assert_eq!(table.stored_endian_format().expect("read"), Some(expected));
    }
    assert_eq!(Table::new().stored_endian_format().expect("none"), None);
}

#[test]
fn a_shallow_copy_keeps_the_group_name_and_the_byte_order_it_is_given() {
    let source = grouped_table("shallow_group", "MyGroup", EndianFormat::BigEndian);
    let table = Table::open(TableOptions::new(&source)).expect("open");
    assert_eq!(group_of(&source, "id"), "MyGroup");
    let options = table
        .single_stman_aipsio_options()
        .expect("layout")
        .expect("one StManAipsIO group");
    let copy = source.with_file_name("copy.table");
    table
        .shallow_copy(
            options
                .with_path(&copy)
                .with_endian_format(EndianFormat::LittleEndian),
        )
        .expect("shallow copy");

    let copied = Table::open(TableOptions::new(&copy)).expect("open the copy");
    assert_eq!(copied.row_count(), 0);
    assert_eq!(group_of(&copy, "id"), "MyGroup");
    assert_eq!(
        copied.stored_endian_format().expect("read"),
        Some(EndianFormat::LittleEndian)
    );
    // The source is as it was.
    assert_eq!(table.row_count(), 3);
    assert_eq!(group_of(&source, "id"), "MyGroup");
}

#[test]
fn a_reference_table_has_the_byte_order_of_the_table_it_refers_to() {
    let parent = grouped_table("ref_endian", "", EndianFormat::BigEndian);
    let table = Table::open(TableOptions::new(&parent)).expect("open");
    assert!(!table.is_reference_table().expect("kind"));
    let view = parent.with_file_name("view.table");
    // The view's own header says little endian; the table it refers to says
    // big, which is what casacore's `RefTable` reports.
    table
        .select_rows(&[0, 2])
        .expect("select")
        .save(
            TableOptions::new(&view)
                .with_data_manager(DataManagerKind::StManAipsIO)
                .with_endian_format(EndianFormat::LittleEndian),
        )
        .expect("save the view");

    let opened = Table::open(TableOptions::new(&view)).expect("open the view");
    assert!(opened.is_reference_table().expect("kind"));
    assert_eq!(opened.row_count(), 2);
    assert_eq!(
        opened.stored_endian_format().expect("read"),
        Some(EndianFormat::BigEndian)
    );
}
