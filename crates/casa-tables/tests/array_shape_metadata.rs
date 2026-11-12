// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shapes come from storage headers, with row order and undefinedness intact.
use casa_tables::{ColumnBinding, ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use std::path::Path;

fn table(shapes: &[Option<Vec<usize>>], fixed: bool) -> Table {
    let column = if fixed {
        ColumnSchema::array_fixed("DATA", PrimitiveType::Float32, vec![2, 3])
    } else {
        ColumnSchema::array_variable("DATA", PrimitiveType::Float32, Some(2))
    };
    let mut table = Table::with_schema(TableSchema::new(vec![column]).unwrap());
    for shape in shapes {
        let fields = shape
            .as_ref()
            .map(|shape| {
                vec![RecordField::new(
                    "DATA",
                    Value::Array(ArrayValue::Float32(ArrayD::from_elem(
                        IxDyn(shape).f(),
                        7.0,
                    ))),
                )]
            })
            .unwrap_or_default();
        table.add_row(RecordValue::new(fields)).unwrap();
    }
    table
}

fn save(table: &Table, path: &Path, manager: DataManagerKind) {
    let tile_shape = if manager == DataManagerKind::TiledCellStMan {
        vec![2, 4]
    } else {
        vec![2, 4, 2]
    };
    let bindings = std::collections::HashMap::from([(
        "DATA".to_string(),
        ColumnBinding {
            data_manager: manager,
            tile_shape: Some(tile_shape),
        },
    )]);
    table
        .save_with_bindings(
            TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .unwrap_or_else(|error| panic!("{manager:?} input: {error}"));
}

#[test]
fn metadata_shapes_preserve_rows_duplicates_and_undefined_cells() {
    for manager in [
        DataManagerKind::StandardStMan,
        DataManagerKind::TiledShapeStMan,
        DataManagerKind::TiledCellStMan,
        DataManagerKind::TiledColumnStMan,
    ] {
        let fixed = manager == DataManagerKind::TiledColumnStMan;
        let undefined = matches!(
            manager,
            DataManagerKind::StandardStMan | DataManagerKind::TiledShapeStMan
        );
        let shapes = if fixed {
            vec![Some(vec![2, 3]); 4]
        } else {
            vec![
                Some(vec![2, 1]),
                if undefined { None } else { Some(vec![3, 2]) },
                Some(vec![1, 5]),
                Some(vec![2, 4]),
            ]
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arrays");
        save(&table(&shapes, fixed), &path, manager);
        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        let column = reopened.column_accessor("DATA").unwrap();
        let selected = [3, 0, 1, 3, 2];
        let expected = selected
            .iter()
            .map(|&row| shapes[row].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            column.array_shapes_uncached(&selected).unwrap(),
            expected,
            "{manager:?}"
        );
        assert_eq!(
            column.array_shapes_uncached(&[]).unwrap(),
            Vec::<Option<Vec<usize>>>::new()
        );
        assert!(column.array_shapes_uncached(&[4]).is_err());
        for (row, shape) in shapes.iter().enumerate() {
            assert_eq!(
                reopened.array_shape(row, "DATA").unwrap(),
                *shape,
                "{manager:?} row {row}"
            );
        }
    }
}

#[test]
fn tiled_shape_metadata_never_reads_the_numerical_files() {
    for manager in [
        DataManagerKind::TiledColumnStMan,
        DataManagerKind::TiledShapeStMan,
        DataManagerKind::TiledCellStMan,
    ] {
        let fixed = manager == DataManagerKind::TiledColumnStMan;
        let shapes = if fixed {
            vec![Some(vec![2, 3]); 2]
        } else {
            vec![Some(vec![2, 4096]), Some(vec![1, 4096])]
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arrays");
        save(&table(&shapes, fixed), &path, manager);
        let mut removed = 0;
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_string_lossy().contains("_TSM") {
                std::fs::remove_file(entry.path()).unwrap();
                removed += 1;
            }
        }
        assert!(removed > 0);
        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        assert_eq!(
            reopened
                .column_accessor("DATA")
                .unwrap()
                .array_shapes_uncached(&[1, 0])
                .unwrap(),
            vec![shapes[1].clone(), shapes[0].clone()],
            "{manager:?}"
        );
        assert!(
            reopened
                .column_accessor("DATA")
                .unwrap()
                .array_cell(0)
                .is_err(),
            "{manager:?}: missing payload must not be readable"
        );
    }
}

#[test]
fn pending_array_edits_cannot_be_mistaken_for_persisted_shapes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("arrays");
    table(&[Some(vec![2, 3])], false)
        .save(TableOptions::new(&path).with_data_manager(DataManagerKind::StandardStMan))
        .unwrap();
    let mut reopened = Table::open(TableOptions::new(&path)).unwrap();
    reopened
        .cell_accessor_mut(0, "DATA")
        .unwrap()
        .set(Value::Array(ArrayValue::Float32(ArrayD::from_elem(
            IxDyn(&[2, 4]).f(),
            1.0,
        ))))
        .unwrap();
    assert!(
        reopened
            .column_accessor("DATA")
            .unwrap()
            .array_shapes_uncached(&[0])
            .is_err()
    );
}
