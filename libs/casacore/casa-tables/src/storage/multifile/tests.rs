// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashMap;
use std::fs;

use casa_core::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use super::*;
use crate::{ColumnBinding, ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};

#[test]
fn table_reads_packed_cells_and_ignores_shadow_manager_files() {
    let root = tempfile::tempdir().unwrap();
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("ID", PrimitiveType::Int32),
        ColumnSchema::array_variable("SAMPLES", PrimitiveType::Int32, Some(1)),
    ])
    .unwrap();
    for (manager, block_size) in [
        (DataManagerKind::StandardStMan, 96),
        (DataManagerKind::IncrementalStMan, 4096),
    ] {
        let path = root.path().join(format!("{manager:?}"));
        let mut source = Table::with_schema(schema.clone());
        let data = [5, 17, 42].map(|index| {
            RecordValue::new(vec![
                RecordField::new("ID", Value::Scalar(ScalarValue::Int32(index))),
                RecordField::new(
                    "SAMPLES",
                    Value::Array(ArrayValue::Int32(
                        ndarray::array![index, index + 1].into_dyn(),
                    )),
                ),
            ])
        });
        for row in &data {
            source.add_row(row.clone()).unwrap();
        }
        source
            .save(TableOptions::new(&path).with_data_manager(manager))
            .unwrap();
        Table::pack_staged_multifile(&path, block_size).unwrap();
        let archive = path.join("table.mf");
        assert!(archive.is_file());
        if block_size == 96 {
            assert_ne!(u64_at(&fs::read(archive).unwrap(), 8), 0);
        }
        assert!(!path.join("table.f0").exists());
        fs::write(path.join("table.f0"), b"stale manager").unwrap();
        fs::write(path.join("table.f0i"), b"stale array file").unwrap();
        let table = Table::open(TableOptions::new(&path)).unwrap();
        if manager == DataManagerKind::IncrementalStMan {
            assert_eq!(
                table.check_incremental_layout("ID").unwrap(),
                crate::IncrementalLayoutCheck::Valid
            );
        }
        let selected = table
            .get_array_cells_owned_uncached("SAMPLES", &[2, 0, 2])
            .unwrap();
        let expected = [2, 0, 2].map(|row| match data[row].get("SAMPLES").unwrap() {
            Value::Array(value) => Some(value.clone()),
            _ => unreachable!("array in fixture"),
        });
        assert_eq!(selected, expected);
        for (index, row) in data.iter().enumerate() {
            assert_eq!(table.row(index).unwrap(), row);
        }
    }
}

#[test]
fn packed_row_edit_preserves_the_archive_and_refuses_mixed_manager_updates() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("PACKED", PrimitiveType::Int32),
        ColumnSchema::scalar("LOOSE", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut original = Table::with_schema(schema);
    original
        .add_row(RecordValue::new(vec![
            RecordField::new("PACKED", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("LOOSE", Value::Scalar(ScalarValue::Int32(2))),
        ]))
        .unwrap();
    let bindings = HashMap::from([
        (
            "PACKED".into(),
            ColumnBinding {
                data_manager: DataManagerKind::StandardStMan,
                tile_shape: None,
            },
        ),
        (
            "LOOSE".into(),
            ColumnBinding {
                data_manager: DataManagerKind::StManAipsIO,
                tile_shape: None,
            },
        ),
    ]);
    original
        .save_with_bindings(TableOptions::new(path), &bindings)
        .unwrap();
    Table::pack_staged_multifile(path, 4096).unwrap();
    let archive = path.join("table.mf");
    let control = path.join("table.dat");
    let old_descriptor = fs::read(&control).unwrap();
    let mut table = Table::open(TableOptions::new(path)).unwrap();
    table
        .set_cell(0, "PACKED", Value::Scalar(ScalarValue::Int32(6)))
        .unwrap();
    table
        .prepare_write()
        .save_selected_rows(&["PACKED"], &[0])
        .unwrap();
    let saved = Table::open(TableOptions::new(path)).unwrap();
    assert_eq!(
        saved
            .column_accessor("PACKED")
            .unwrap()
            .scalar_cell(0)
            .unwrap(),
        &ScalarValue::Int32(6)
    );
    assert_eq!(
        saved
            .column_accessor("LOOSE")
            .unwrap()
            .scalar_cell(0)
            .unwrap(),
        &ScalarValue::Int32(2)
    );
    let saved_bytes = fs::read(&archive).unwrap();
    table
        .set_cell(0, "PACKED", Value::Scalar(ScalarValue::Int32(8)))
        .unwrap();
    table
        .set_cell(0, "LOOSE", Value::Scalar(ScalarValue::Int32(9)))
        .unwrap();
    assert!(
        table
            .prepare_write()
            .save_selected_rows(&["PACKED", "LOOSE"], &[0])
            .is_err()
    );
    assert!(table.flush().is_err());
    assert_eq!(fs::read(&archive).unwrap(), saved_bytes);
    assert_eq!(fs::read(&control).unwrap(), old_descriptor);
}

#[test]
fn rejects_unbounded_directory_field_before_allocating_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path();
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(7)),
        )]))
        .unwrap();
    table
        .save(TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan))
        .unwrap();
    Table::pack_staged_multifile(path, 4096).unwrap();
    let container = path.join("table.mf");
    let mut bytes = fs::read(&container).unwrap();
    let name_length = bytes
        .windows(8)
        .position(|window| window == b"table.f0")
        .unwrap()
        - 4;
    bytes[name_length..name_length + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    fs::write(&container, bytes).unwrap();
    let error = MultiFile::open(&container)
        .err()
        .expect("invalid string length");
    assert!(error.to_string().contains("field exceeds its object"));
}
