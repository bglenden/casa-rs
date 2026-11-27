// SPDX-License-Identifier: LGPL-3.0-or-later
//! Whole-cell packed reads retain numeric values, axes and selection order.

use casa_core::{ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, Value};
use casa_tables::{
    ColumnOptions, ColumnSchema, DataManagerKind, EndianFormat, SelectedArray2DCells, Table,
    TableOptions, TableSchema,
};
use ndarray::ArrayD;

fn values(row: usize) -> Vec<f64> {
    (0..3)
        .flat_map(|axis| (0..3).map(move |channel| (row * 100 + channel * 10 + axis) as f64))
        .collect()
}

fn flag(value: f64) -> bool {
    let value = value as usize;
    (value / 100 + value / 10 % 10 + value % 10).is_multiple_of(2)
}

fn array<T>(values: Vec<T>) -> ArrayD<T> {
    ArrayD::from_shape_vec(vec![3, 3], values).unwrap()
}

#[test]
fn packed_cells_preserve_axes_duplicate_rows_and_endianness() {
    let root = tempfile::tempdir().unwrap();
    for (manager, direct) in [
        (DataManagerKind::StandardStMan, false),
        (DataManagerKind::StandardStMan, true),
        (DataManagerKind::StManAipsIO, false),
    ] {
        for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
            let primitives = [
                ("F32", PrimitiveType::Float32),
                ("F64", PrimitiveType::Float64),
                ("C32", PrimitiveType::Complex32),
                ("C64", PrimitiveType::Complex64),
                ("FLAGS", PrimitiveType::Bool),
            ];
            let schema = TableSchema::new(
                primitives
                    .iter()
                    .map(|&(name, primitive)| {
                        if direct {
                            ColumnSchema::array_fixed(name, primitive, vec![3, 3])
                                .with_options(ColumnOptions {
                                    direct: true,
                                    undefined: false,
                                })
                                .unwrap()
                        } else {
                            ColumnSchema::array_variable(name, primitive, Some(2))
                        }
                    })
                    .collect(),
            )
            .unwrap();
            let mut table = Table::with_schema(schema);
            for row in 0..2000 {
                let source = values(row);
                table
                    .add_row(RecordValue::new(vec![
                        RecordField::new(
                            "F32",
                            Value::Array(ArrayValue::Float32(array(
                                source.iter().map(|&x| x as f32).collect(),
                            ))),
                        ),
                        RecordField::new(
                            "F64",
                            Value::Array(ArrayValue::Float64(array(source.clone()))),
                        ),
                        RecordField::new(
                            "C32",
                            Value::Array(ArrayValue::Complex32(array(
                                source
                                    .iter()
                                    .map(|&x| Complex32::new(x as f32, -x as f32))
                                    .collect(),
                            ))),
                        ),
                        RecordField::new(
                            "C64",
                            Value::Array(ArrayValue::Complex64(array(
                                source.iter().map(|&x| Complex64::new(x, -x)).collect(),
                            ))),
                        ),
                        RecordField::new(
                            "FLAGS",
                            Value::Array(ArrayValue::Bool(array(
                                source.iter().map(|&x| flag(x)).collect(),
                            ))),
                        ),
                    ]))
                    .unwrap();
            }
            let path = root.path().join(format!("{manager:?}-{direct}-{endian:?}"));
            table
                .save(
                    TableOptions::new(&path)
                        .with_data_manager(manager)
                        .with_endian_format(endian),
                )
                .unwrap();
            let table = Table::open(TableOptions::new(&path)).unwrap();
            let rows = [1999, 3, 0, 3, 818, 819];
            let expected = (0..3)
                .flat_map(|channel| {
                    rows.iter().flat_map(move |&row| {
                        (0..3).map(move |axis| (row * 100 + channel * 10 + axis) as f64)
                    })
                })
                .collect::<Vec<_>>();
            for (name, _) in primitives {
                let found = table
                    .column_accessor(name)
                    .unwrap()
                    .array_cells_fixed_2d_typed_uncached(&rows, [3, 3])
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    (
                        found.row_count(),
                        found.axis0_count(),
                        found.channel_count()
                    ),
                    (rows.len(), 3, 3)
                );
                match found {
                    SelectedArray2DCells::Float32(x) => assert_eq!(
                        x.values(),
                        &expected.iter().map(|&x| x as f32).collect::<Vec<_>>()
                    ),
                    SelectedArray2DCells::Float64(x) => assert_eq!(x.values(), &expected),
                    SelectedArray2DCells::Complex32(x) => assert_eq!(
                        x.values(),
                        &expected
                            .iter()
                            .map(|&x| Complex32::new(x as f32, -x as f32))
                            .collect::<Vec<_>>()
                    ),
                    SelectedArray2DCells::Complex64(x) => assert_eq!(
                        x.values(),
                        &expected
                            .iter()
                            .map(|&x| Complex64::new(x, -x))
                            .collect::<Vec<_>>()
                    ),
                    SelectedArray2DCells::Bool(x) => assert_eq!(
                        x.values(),
                        &expected.iter().map(|&x| flag(x)).collect::<Vec<_>>()
                    ),
                }
            }
        }
    }
}

#[test]
fn whole_cell_reads_do_not_slice_extra_channels_or_hide_undefined_cells() {
    let root = tempfile::tempdir().unwrap();
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "DATA",
        PrimitiveType::Float32,
        Some(2),
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for shape in [Some(vec![2, 1]), Some(vec![2, 2]), None] {
        let row = shape.map_or_else(RecordValue::default, |shape| {
            RecordValue::new(vec![RecordField::new(
                "DATA",
                Value::Array(ArrayValue::Float32(ArrayD::zeros(shape))),
            )])
        });
        table.add_row(row).unwrap();
    }
    table
        .save(TableOptions::new(root.path()).with_data_manager(DataManagerKind::StandardStMan))
        .unwrap();
    let mut table = Table::open(TableOptions::new(root.path())).unwrap();
    assert!(
        table
            .column_accessor("DATA")
            .unwrap()
            .array_cells_fixed_2d_typed_uncached(&[0], [usize::MAX, 1])
            .is_err()
    );
    for rows in [&[0, 1][..], &[0, 2][..], &[][..]] {
        assert!(
            table
                .column_accessor("DATA")
                .unwrap()
                .array_cells_fixed_2d_typed_uncached(rows, [2, 1])
                .unwrap()
                .is_none()
        );
    }
    assert!(
        table
            .column_accessor("DATA")
            .unwrap()
            .array_cells_fixed_2d_typed_uncached(&[3], [2, 1])
            .is_err()
    );
    {
        let mut write = table.row_accessor_mut().prepare(&["DATA"]).unwrap();
        write.seek(0).unwrap();
        write
            .set_value_at(
                0,
                Value::Array(ArrayValue::Float32(ArrayD::ones(vec![2, 1]))),
            )
            .unwrap();
    }
    assert!(
        table
            .column_accessor("DATA")
            .unwrap()
            .array_cells_fixed_2d_typed_uncached(&[0], [2, 1])
            .unwrap()
            .is_none()
    );
}

#[test]
fn sparse_array_saves_keep_latest_updates_axes_and_unselected_rows() {
    let root = tempfile::tempdir().unwrap();
    let cell = |base: f32| {
        Value::Array(ArrayValue::Float32(
            ArrayD::from_shape_vec(vec![2, 3], (0..6).map(|axis| base + axis as f32).collect())
                .unwrap(),
        ))
    };
    for manager in [
        DataManagerKind::StandardStMan,
        DataManagerKind::TiledShapeStMan,
    ] {
        for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
            for ordered in [true, false] {
                let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
                    "DATA",
                    PrimitiveType::Float32,
                    vec![2, 3],
                )])
                .unwrap();
                let mut table = Table::with_schema(schema);
                for row in 0..4 {
                    table
                        .add_row(RecordValue::new(vec![RecordField::new(
                            "DATA",
                            cell(row as f32 * 100.0),
                        )]))
                        .unwrap();
                }
                let path = root
                    .path()
                    .join(format!("sparse-{manager:?}-{endian:?}-{ordered}"));
                table
                    .save(
                        TableOptions::new(&path)
                            .with_data_manager(manager)
                            .with_endian_format(endian),
                    )
                    .unwrap();
                let mut table = Table::open(TableOptions::new(&path)).unwrap();
                let updates = if ordered {
                    vec![(0, 10.0), (1, 20.0)]
                } else {
                    vec![(0, 10.0), (1, 20.0), (0, 40.0)]
                };
                for (row, value) in updates {
                    table
                        .column_accessor_mut("DATA")
                        .unwrap()
                        .set(row, cell(value))
                        .unwrap();
                }
                table
                    .prepare_write()
                    .save_selected_rows(&["DATA"], if ordered { &[0, 1] } else { &[1, 0] })
                    .unwrap();
                drop(table);
                let table = Table::open(TableOptions::new(&path)).unwrap();
                for (row, expected) in [
                    (0, if ordered { 10.0 } else { 40.0 }),
                    (1, 20.0),
                    (2, 200.0),
                    (3, 300.0),
                ] {
                    assert_eq!(
                        table
                            .column_accessor("DATA")
                            .unwrap()
                            .get(row)
                            .unwrap()
                            .unwrap(),
                        &cell(expected),
                        "{manager:?} {endian:?} ordered={ordered} row={row}"
                    );
                }
            }
        }
    }
}
