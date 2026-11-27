// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn slicer_contiguous_2d() {
    use super::super::Slicer;
    let s = Slicer::contiguous(vec![0, 1], vec![2, 3]).unwrap();
    assert_eq!(s.ndim(), 2);
    assert_eq!(s.start(), &[0, 1]);
    assert_eq!(s.end(), &[2, 3]);
    assert_eq!(s.stride(), &[1, 1]);
}

#[test]
fn slicer_rejects_zero_stride() {
    use super::super::Slicer;
    assert!(Slicer::new(vec![0], vec![5], vec![0]).is_err());
}

#[test]
fn slicer_rejects_start_gt_end() {
    use super::super::Slicer;
    assert!(Slicer::new(vec![5], vec![3], vec![1]).is_err());
}

#[test]
fn get_cell_slice_2d() {
    use super::super::Slicer;
    use casa_core::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![3, 4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    // 3x4 array filled with value = row*10 + col
    let arr: ArrayD<f64> =
        ArrayD::from_shape_fn(IxDyn(&[3, 4]), |idx| (idx[0] * 10 + idx[1]) as f64);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float64(arr)),
        )]))
        .unwrap();

    // Slice rows 1..3, cols 2..4
    let slicer = Slicer::contiguous(vec![1, 2], vec![3, 4]).unwrap();
    let sliced = table.get_cell_slice("data", 0, &slicer).unwrap();

    match sliced {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.shape(), &[2, 2]);
            assert_eq!(a[[0, 0]], 12.0); // row=1, col=2
            assert_eq!(a[[0, 1]], 13.0); // row=1, col=3
            assert_eq!(a[[1, 0]], 22.0); // row=2, col=2
            assert_eq!(a[[1, 1]], 23.0); // row=2, col=3
        }
        other => panic!("expected Float64 array, got {other:?}"),
    }
}

#[test]
fn put_cell_slice_2d() {
    use super::super::Slicer;
    use casa_core::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![3, 4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[3, 4]));
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Float64(arr)),
        )]))
        .unwrap();

    // Write 99.0 into the [1..3, 0..2] sub-region
    let patch: ArrayD<f64> = ArrayD::from_elem(IxDyn(&[2, 2]), 99.0);
    let slicer = Slicer::contiguous(vec![1, 0], vec![3, 2]).unwrap();
    table
        .put_cell_slice("data", 0, &slicer, &ArrayValue::Float64(patch))
        .unwrap();

    match table_cell(&table, 0, "data").unwrap().unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a[[0, 0]], 0.0); // untouched
            assert_eq!(a[[1, 0]], 99.0); // patched
            assert_eq!(a[[2, 1]], 99.0); // patched
            assert_eq!(a[[0, 3]], 0.0); // untouched
        }
        other => panic!("expected Float64 array, got {other:?}"),
    }
}

#[test]
fn get_cell_slice_with_stride() {
    use super::super::Slicer;
    use casa_core::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Int32,
        vec![6],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    let arr: ArrayD<i32> = ArrayD::from_shape_fn(IxDyn(&[6]), |idx| idx[0] as i32);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::Int32(arr)),
        )]))
        .unwrap();

    // Every other element: [0, 2, 4]
    let slicer = Slicer::new(vec![0], vec![6], vec![2]).unwrap();
    let sliced = table.get_cell_slice("data", 0, &slicer).unwrap();

    match sliced {
        Value::Array(ArrayValue::Int32(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0, 2, 4]);
        }
        other => panic!("expected Int32 array, got {other:?}"),
    }
}

#[test]
fn get_cell_slice_scalar_cell_fails() {
    use super::super::Slicer;

    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(42)),
        )]))
        .unwrap();

    let slicer = Slicer::contiguous(vec![0], vec![1]).unwrap();
    assert!(table.get_cell_slice("x", 0, &slicer).is_err());
}

#[test]
fn slice_errors_cover_missing_columns_and_invalid_bounds() {
    use super::super::Slicer;
    use casa_core::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![4]),
        ColumnSchema::scalar("x", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "data",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[4]), vec![0, 1, 2, 3]).unwrap(),
                )),
            ),
            RecordField::new("x", Value::Scalar(ScalarValue::Int32(42))),
        ]))
        .unwrap();

    let slicer = Slicer::contiguous(vec![0], vec![2]).unwrap();
    let missing_read = table.get_cell_slice("missing", 0, &slicer).unwrap_err();
    assert!(matches!(
        missing_read,
        TableError::ColumnNotFound {
            row_index: 0,
            ref column
        } if column == "missing"
    ));

    let missing_write = table
        .put_cell_slice(
            "missing",
            0,
            &slicer,
            &ArrayValue::Int32(ArrayD::from_shape_vec(IxDyn(&[2]), vec![9, 9]).unwrap()),
        )
        .unwrap_err();
    assert!(matches!(
        missing_write,
        TableError::ColumnNotFound {
            row_index: 0,
            ref column
        } if column == "missing"
    ));

    let scalar_write = table
        .put_cell_slice(
            "x",
            0,
            &slicer,
            &ArrayValue::Int32(ArrayD::from_shape_vec(IxDyn(&[2]), vec![9, 9]).unwrap()),
        )
        .unwrap_err();
    assert!(matches!(
        scalar_write,
        TableError::CellNotArray { row: 0, ref column } if column == "x"
    ));

    let mismatch = table
        .get_cell_slice(
            "data",
            0,
            &Slicer::contiguous(vec![0, 0], vec![1, 1]).unwrap(),
        )
        .unwrap_err();
    assert!(matches!(
        mismatch,
        TableError::SlicerDimensionMismatch {
            start_ndim: 2,
            end_ndim: 1,
            stride_ndim: 2
        }
    ));

    let out_of_bounds = table
        .get_cell_slice("data", 0, &Slicer::contiguous(vec![3], vec![5]).unwrap())
        .unwrap_err();
    assert!(matches!(
        out_of_bounds,
        TableError::SlicerOutOfBounds {
            axis: 0,
            index: 5,
            extent: 4
        }
    ));
}

#[test]
fn get_column_slice_multiple_rows() {
    use super::super::{RowRange, Slicer};
    use casa_core::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Int32,
        vec![4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    for i in 0..3 {
        let arr = ArrayD::from_shape_fn(IxDyn(&[4]), |idx| (i * 10 + idx[0]) as i32);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Int32(arr)),
            )]))
            .unwrap();
    }

    // Slice elements [1..3] from rows 0 and 1
    let slicer = Slicer::contiguous(vec![1], vec![3]).unwrap();
    let results = table
        .get_column_slice("data", RowRange::new(0, 2), &slicer)
        .unwrap();

    assert_eq!(results.len(), 2);
    match &results[0] {
        Value::Array(ArrayValue::Int32(a)) => assert_eq!(a.as_slice().unwrap(), &[1, 2]),
        other => panic!("expected Int32 array, got {other:?}"),
    }
    match &results[1] {
        Value::Array(ArrayValue::Int32(a)) => assert_eq!(a.as_slice().unwrap(), &[11, 12]),
        other => panic!("expected Int32 array, got {other:?}"),
    }
}

#[test]
fn put_column_slice_multiple_rows() {
    use super::super::{RowRange, Slicer};
    use casa_core::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    for _ in 0..3 {
        let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[4]));
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(arr)),
            )]))
            .unwrap();
    }

    // Patch elements [1..3] in rows 0 and 2 (stride=2)
    let slicer = Slicer::contiguous(vec![1], vec![3]).unwrap();
    let patches = vec![
        ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 11.0)),
        ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 22.0)),
    ];
    table
        .put_column_slice("data", RowRange::with_stride(0, 3, 2), &slicer, &patches)
        .unwrap();

    // Row 0: [0, 11, 11, 0]
    match table_cell(&table, 0, "data").unwrap().unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 11.0, 11.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
    // Row 1: untouched
    match table_cell(&table, 1, "data").unwrap().unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 0.0, 0.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
    // Row 2: [0, 22, 22, 0]
    match table_cell(&table, 2, "data").unwrap().unwrap() {
        Value::Array(ArrayValue::Float64(a)) => {
            assert_eq!(a.as_slice().unwrap(), &[0.0, 22.0, 22.0, 0.0]);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn put_column_slice_length_mismatch() {
    use super::super::{RowRange, Slicer};
    use casa_core::ArrayD;
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        vec![4],
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);

    for _ in 0..2 {
        let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[4]));
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(arr)),
            )]))
            .unwrap();
    }

    let slicer = Slicer::contiguous(vec![0], vec![2]).unwrap();
    // 2 rows selected but only 1 data element
    let patches = vec![ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 1.0))];
    let result = table.put_column_slice("data", RowRange::new(0, 2), &slicer, &patches);
    assert!(result.is_err());
}

#[test]
fn slice_helpers_cover_remaining_array_variants() {
    use super::super::Slicer;
    use casa_core::{ArrayD, Complex32, Complex64};
    use ndarray::IxDyn;

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("c32", PrimitiveType::Complex32, vec![2]),
        ColumnSchema::array_fixed("c64", PrimitiveType::Complex64, vec![2]),
        ColumnSchema::array_fixed("text", PrimitiveType::String, vec![2]),
        ColumnSchema::array_fixed("ints", PrimitiveType::Int32, vec![2]),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "c32",
                Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2]),
                        vec![Complex32::new(1.0, 2.0), Complex32::new(3.0, 4.0)],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "c64",
                Value::Array(ArrayValue::Complex64(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2]),
                        vec![Complex64::new(5.0, 6.0), Complex64::new(7.0, 8.0)],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "text",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(
                        IxDyn(&[2]),
                        vec!["alpha".to_string(), "beta".to_string()],
                    )
                    .unwrap(),
                )),
            ),
            RecordField::new(
                "ints",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[2]), vec![1, 2]).unwrap(),
                )),
            ),
        ]))
        .unwrap();

    let slicer = Slicer::contiguous(vec![0], vec![2]).unwrap();

    match table.get_cell_slice("c32", 0, &slicer).unwrap() {
        Value::Array(ArrayValue::Complex32(values)) => {
            assert_eq!(values[[1]], Complex32::new(3.0, 4.0));
        }
        other => panic!("unexpected sliced complex32 value: {other:?}"),
    }

    match table.get_cell_slice("c64", 0, &slicer).unwrap() {
        Value::Array(ArrayValue::Complex64(values)) => {
            assert_eq!(values[[0]], Complex64::new(5.0, 6.0));
        }
        other => panic!("unexpected sliced complex64 value: {other:?}"),
    }

    match table.get_cell_slice("text", 0, &slicer).unwrap() {
        Value::Array(ArrayValue::String(values)) => {
            assert_eq!(values[[1]], "beta");
        }
        other => panic!("unexpected sliced string value: {other:?}"),
    }

    table
        .put_cell_slice(
            "c32",
            0,
            &slicer,
            &ArrayValue::Complex32(
                ArrayD::from_shape_vec(
                    IxDyn(&[2]),
                    vec![Complex32::new(9.0, 1.0), Complex32::new(8.0, 2.0)],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    match table_cell(&table, 0, "c32").unwrap().unwrap() {
        Value::Array(ArrayValue::Complex32(values)) => {
            assert_eq!(values[[0]], Complex32::new(9.0, 1.0));
        }
        other => panic!("unexpected put complex32 value: {other:?}"),
    }

    table
        .put_cell_slice(
            "c64",
            0,
            &slicer,
            &ArrayValue::Complex64(
                ArrayD::from_shape_vec(
                    IxDyn(&[2]),
                    vec![Complex64::new(6.0, 5.0), Complex64::new(8.0, 7.0)],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    match table_cell(&table, 0, "c64").unwrap().unwrap() {
        Value::Array(ArrayValue::Complex64(values)) => {
            assert_eq!(values[[1]], Complex64::new(8.0, 7.0));
        }
        other => panic!("unexpected put complex64 value: {other:?}"),
    }

    table
        .put_cell_slice(
            "text",
            0,
            &slicer,
            &ArrayValue::String(
                ArrayD::from_shape_vec(IxDyn(&[2]), vec!["delta".to_string(), "gamma".to_string()])
                    .unwrap(),
            ),
        )
        .unwrap();
    match table_cell(&table, 0, "text").unwrap().unwrap() {
        Value::Array(ArrayValue::String(values)) => {
            assert_eq!(values[[0]], "delta");
        }
        other => panic!("unexpected put string value: {other:?}"),
    }

    table
        .put_cell_slice(
            "ints",
            0,
            &slicer,
            &ArrayValue::Float64(ArrayD::from_shape_vec(IxDyn(&[2]), vec![9.0, 9.0]).unwrap()),
        )
        .unwrap();
    match table_cell(&table, 0, "ints").unwrap().unwrap() {
        Value::Array(ArrayValue::Int32(values)) => assert_eq!(values.as_slice().unwrap(), &[1, 2]),
        other => panic!("unexpected mismatch target value: {other:?}"),
    }
}

// ---- Row copy and fill tests ----

#[test]
fn copy_rows_appends_all() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut dst = Table::with_schema(schema.clone());
    dst.add_row(RecordValue::new(vec![RecordField::new(
        "x",
        Value::Scalar(ScalarValue::Int32(0)),
    )]))
    .unwrap();

    let src = Table::from_rows_with_schema(
        vec![
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(1)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(2)),
            )]),
        ],
        schema,
    )
    .unwrap();

    dst.copy_rows(&src).unwrap();
    assert_eq!(dst.row_count(), 3);
    assert_eq!(
        table_cell(&dst, 2, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(2))))
    );
}

#[test]
fn copy_rows_schema_mismatch() {
    let s1 = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let s2 = TableSchema::new(vec![ColumnSchema::scalar("y", PrimitiveType::Int32)]).unwrap();
    let mut dst = Table::with_schema(s1);
    let src = Table::with_schema(s2);
    assert!(dst.copy_rows(&src).is_err());
}

#[test]
fn copy_rows_with_mapping_selects_rows() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let src = Table::from_rows_with_schema(
        vec![
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(10)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(20)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(30)),
            )]),
        ],
        schema.clone(),
    )
    .unwrap();

    let mut dst = Table::with_schema(schema);
    dst.copy_rows_with_mapping(&src, &[2, 0]).unwrap();
    assert_eq!(dst.row_count(), 2);
    assert_eq!(
        table_cell(&dst, 0, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(30))))
    );
    assert_eq!(
        table_cell(&dst, 1, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(10))))
    );
}

#[test]
fn copy_info_transfers_metadata() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut src = Table::with_schema(schema.clone());
    src.set_info(crate::TableInfo {
        table_type: "MeasurementSet".into(),
        sub_type: "".into(),
        readme: Vec::new(),
    });

    let mut dst = Table::with_schema(schema);
    dst.copy_info(&src);
    assert_eq!(dst.info().table_type, "MeasurementSet");
}

#[test]
fn fill_column_sets_all_cells() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::from_rows_with_schema(
        vec![
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(1)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(2)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(3)),
            )]),
        ],
        schema,
    )
    .unwrap();

    table
        .fill_column("x", Value::Scalar(ScalarValue::Int32(99)))
        .unwrap();
    for i in 0..3 {
        assert_eq!(
            table_cell(&table, i, "x"),
            Ok(Some(&Value::Scalar(ScalarValue::Int32(99))))
        );
    }
}

#[test]
fn fill_column_range_sets_subset() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::from_rows_with_schema(
        vec![
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(0)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(0)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(0)),
            )]),
            RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(0)),
            )]),
        ],
        schema,
    )
    .unwrap();

    // Fill only rows 1 and 3 (stride=2 starting at 1)
    table
        .fill_column_range(
            "x",
            super::super::RowRange::with_stride(1, 4, 2),
            Value::Scalar(ScalarValue::Int32(77)),
        )
        .unwrap();

    assert_eq!(
        table_cell(&table, 0, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(0))))
    );
    assert_eq!(
        table_cell(&table, 1, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(77))))
    );
    assert_eq!(
        table_cell(&table, 2, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(0))))
    );
    assert_eq!(
        table_cell(&table, 3, "x"),
        Ok(Some(&Value::Scalar(ScalarValue::Int32(77))))
    );
}
