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

#[test]
fn root_row_numbers_compose_reference_maps_and_keep_duplicates() {
    let parent = grouped_table("root_rows", "", EndianFormat::BigEndian);
    let table = Table::open(TableOptions::new(&parent)).expect("open parent");
    assert_eq!(table.root_row_numbers().expect("plain rows"), [0, 1, 2]);
    let view = parent.with_file_name("view.table");
    table
        .select_rows(&[2, 0, 2])
        .expect("select")
        .save(TableOptions::new(&view))
        .expect("save view");
    let first = Table::open(TableOptions::new(&view)).expect("open view");
    assert_eq!(first.root_row_numbers().expect("view rows"), [2, 0, 2]);
    let nested = parent.with_file_name("nested.table");
    first
        .select_rows(&[2, 1])
        .expect("nested select")
        .save(TableOptions::new(&nested))
        .expect("save nested");
    let second = Table::open(TableOptions::new(&nested)).expect("open nested");
    assert_eq!(second.root_row_numbers().expect("nested rows"), [2, 0]);
}

#[test]
fn root_row_numbers_refuse_a_reference_cycle() {
    let parent = grouped_table("root_cycle", "", EndianFormat::BigEndian);
    let table = Table::open(TableOptions::new(&parent)).expect("open parent");
    let view = parent.with_file_name("view.table");
    table
        .select_rows(&[2])
        .expect("select")
        .save(TableOptions::new(&view))
        .expect("save view");
    let opened = Table::open(TableOptions::new(&view)).expect("open view");
    let control = view.join("table.dat");
    let TableDatResult::Ref(mut reference) = read_table_dat_dispatch(&control).expect("control")
    else {
        panic!("expected reference table");
    };
    reference.parent_relative_path = "./view.table".into();
    crate::storage::table_control::write_ref_table_dat(&control, &reference).expect("write cycle");
    assert!(
        opened
            .root_row_numbers()
            .expect_err("cycle refused")
            .to_string()
            .contains("cycle")
    );
    assert!(
        opened
            .simple_reference_root()
            .expect_err("cycle refused")
            .to_string()
            .contains("cycle")
    );
    assert!(
        opened
            .cache_plain_table_paths()
            .expect_err("cycle refused")
            .to_string()
            .contains("cycle")
    );
}

#[test]
fn cache_plain_table_paths_follow_references_and_concatenations() {
    let first = grouped_table("cache_first", "", EndianFormat::BigEndian);
    let second = grouped_table("cache_second", "", EndianFormat::BigEndian);
    let a = Table::open(TableOptions::new(&first)).expect("open first");
    assert_eq!(
        a.cache_plain_table_paths().expect("plain cache"),
        [crate::table::table_cache_name(&first).unwrap()]
    );
    let view = first.with_file_name("view.table");
    a.select_rows(&[2, 0])
        .expect("select")
        .save(TableOptions::new(&view))
        .expect("save view");
    let view_table = Table::open(TableOptions::new(&view)).expect("open view");
    assert_eq!(
        view_table
            .cache_plain_table_paths()
            .expect("reference cache"),
        [crate::table::table_cache_name(&first).unwrap()]
    );
    let b = Table::open(TableOptions::new(&second)).expect("open second");
    let concat = Table::concat(vec![a, b]).expect("concatenate");
    let path = first.with_file_name("joined.table");
    concat.save(TableOptions::new(&path)).expect("save concat");
    let opened = Table::open(TableOptions::new(&path)).expect("open concat");
    let mut expected = vec![
        crate::table::table_cache_name(&first).unwrap(),
        crate::table::table_cache_name(&second).unwrap(),
    ];
    expected.sort();
    assert_eq!(
        opened.cache_plain_table_paths().expect("concat cache"),
        expected
    );
}

#[cfg(unix)]
#[test]
fn cache_plain_table_paths_preserve_a_symbolic_link_name() {
    let original = grouped_table("cache_link", "", EndianFormat::BigEndian);
    let alias = original.with_file_name("alias.table");
    assert!(alias.is_absolute());
    std::os::unix::fs::symlink(&original, &alias).expect("link");
    let table = Table::open(TableOptions::new(&alias)).expect("open alias");
    assert_eq!(table.cache_plain_table_paths().expect("cache"), [alias]);
}
