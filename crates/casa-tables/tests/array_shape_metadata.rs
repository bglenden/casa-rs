// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shapes come from storage headers, with row order and undefinedness intact.
use casa_tables::{
    ColumnBinding, ColumnOptions, ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use std::path::Path;

fn table(shapes: &[Option<Vec<usize>>], fixed: bool) -> Table {
    let column = if fixed {
        ColumnSchema::array_fixed("DATA", PrimitiveType::Float32, vec![2, 3])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .unwrap()
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
        DataManagerKind::StManAipsIO,
        DataManagerKind::IncrementalStMan,
        DataManagerKind::TiledShapeStMan,
        DataManagerKind::TiledCellStMan,
        DataManagerKind::TiledColumnStMan,
    ] {
        let fixed = matches!(
            manager,
            DataManagerKind::TiledColumnStMan | DataManagerKind::IncrementalStMan
        );
        let undefined = matches!(
            manager,
            DataManagerKind::StandardStMan
                | DataManagerKind::StManAipsIO
                | DataManagerKind::TiledShapeStMan
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

#[test]
fn cpp_incremental_shapes_include_carried_forward_and_undefined_rows() {
    let directory = tempfile::tempdir().unwrap();
    for (name, bytes) in [
        (
            "table.dat",
            &include_bytes!("data/ism_variable_shapes/table.dat")[..],
        ),
        (
            "table.f0",
            &include_bytes!("data/ism_variable_shapes/table.f0")[..],
        ),
        (
            "table.f0i",
            &include_bytes!("data/ism_variable_shapes/table.f0i")[..],
        ),
        (
            "table.info",
            &include_bytes!("data/ism_variable_shapes/table.info")[..],
        ),
    ] {
        std::fs::write(directory.path().join(name), bytes).unwrap();
    }
    let table = Table::open(TableOptions::new(directory.path())).unwrap();
    let expected = vec![
        None,
        Some(vec![2, 1]),
        Some(vec![2, 1]),
        Some(vec![1, 5]),
        Some(vec![2, 4]),
    ];
    assert_eq!(
        table
            .column_accessor("DATA")
            .unwrap()
            .array_shapes_uncached(&[0, 1, 2, 3, 4])
            .unwrap(),
        expected
    );
    assert_eq!(
        table
            .column_accessor("DATA")
            .unwrap()
            .array_shapes_uncached(&[4, 2, 0, 2])
            .unwrap(),
        vec![
            expected[4].clone(),
            expected[2].clone(),
            None,
            expected[2].clone()
        ]
    );
    for (row, shape) in expected.iter().enumerate() {
        assert_eq!(&table.array_shape(row, "DATA").unwrap(), shape);
    }
}

/// Historical payload length and active marker+i64 offsets must not make
/// small live cells unreadable. Sparse holes keep the fixture physically small.
#[test]
fn large_aipsio_files_keep_shape_headers_and_active_64_bit_offsets_readable() {
    use std::io::{Seek, SeekFrom, Write};
    for active_large_offset in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = if let Some(root) = std::env::var_os("CASA_RS_LARGE_AIPSIO_OUT") {
            let path = std::path::PathBuf::from(root).join(if active_large_offset {
                "large-offset"
            } else {
                "historical-length"
            });
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            path
        } else {
            directory.path().join("arrays")
        };
        let shapes = vec![Some(vec![2, 1]), Some(vec![1, 5])];
        save(&table(&shapes, false), &path, DataManagerKind::StManAipsIO);
        let array_path = std::fs::read_dir(&path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.file_name().unwrap().to_string_lossy().ends_with('i'))
            .unwrap();
        let original = std::fs::read(&array_path).unwrap();
        let large_offset = (1u64 << 31) + 64;
        let mut array = std::fs::OpenOptions::new()
            .write(true)
            .open(&array_path)
            .unwrap();
        array.seek(SeekFrom::Start(large_offset)).unwrap();
        // Independently encoded version1 rank2 [2,1] Float32 cell,
        // exactly the original first cell, at an active 64-bit file position.
        array.write_all(&original[16..40]).unwrap();
        let length = large_offset + 24;
        array.set_len(length).unwrap();
        array.seek(SeekFrom::Start(4)).unwrap();
        array.write_all(&(length as i64).to_be_bytes()).unwrap();
        drop(array);
        if active_large_offset {
            let main_path =
                std::path::PathBuf::from(array_path.to_string_lossy().strip_suffix('i').unwrap());
            let mut bytes = std::fs::read(&main_path).unwrap();
            let pattern = [2u32.to_be_bytes(), 16u32.to_be_bytes(), 40u32.to_be_bytes()].concat();
            let matches = bytes
                .windows(pattern.len())
                .enumerate()
                .filter_map(|(index, value)| (value == pattern).then_some(index))
                .collect::<Vec<_>>();
            assert_eq!(matches.len(), 1);
            let slot = matches[0] + 4;
            // AipsIO object lengths include the added i64; all three
            // enclosing objects are patched, so the existing full reader
            // and actual C++ can consume the same independently encoded file.
            for tag in [
                "StManAipsIO",
                "StManColumnIndArrayAipsIO",
                "StManColumnAipsIO",
            ] {
                let offsets = bytes
                    .windows(tag.len())
                    .enumerate()
                    .filter_map(|(index, value)| (value == tag.as_bytes()).then_some(index))
                    .collect::<Vec<_>>();
                assert_eq!(offsets.len(), 1);
                let start = offsets[0] - 8;
                let old = u32::from_be_bytes(bytes[start..start + 4].try_into().unwrap());
                bytes[start..start + 4].copy_from_slice(&(old + 8).to_be_bytes());
            }
            let marker = (1u32 << 31) + 1;
            bytes.splice(
                slot..slot + 4,
                [
                    marker.to_be_bytes().to_vec(),
                    (large_offset as i64).to_be_bytes().to_vec(),
                ]
                .concat(),
            );
            std::fs::write(main_path, bytes).unwrap();
        }
        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        // The ordinary reader already accepts this format before the new
        // shape API: this check distinguishes a regression from new scope.
        for (row, expected) in shapes.iter().enumerate() {
            assert_eq!(reopened.array_shape(row, "DATA").unwrap(), *expected);
            let ArrayValue::Float32(values) = reopened
                .cell_accessor(row, "DATA")
                .unwrap()
                .array()
                .unwrap()
            else {
                panic!("float")
            };
            assert!(values.iter().all(|&value| value == 7.0));
        }
        assert_eq!(
            reopened
                .column_accessor("DATA")
                .unwrap()
                .array_shapes_uncached(&[1, 0, 1])
                .unwrap(),
            vec![shapes[1].clone(), shapes[0].clone(), shapes[1].clone()]
        );
    }
}
