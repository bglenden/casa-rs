// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_tables::{
    ColumnOptions, ColumnSchema, DataManagerKind, EndianFormat, SelectedArray1DCells,
    SelectedArray1DCellsMut, Table, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

#[test]
fn selected_incremental_vectors_preserve_sparse_order_and_reuse_without_materialization() {
    for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
        let root = tempfile::tempdir().expect("fixture root");
        let path = root.path().join("vectors.table");
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("SCAN", PrimitiveType::Int32),
            ColumnSchema::array_fixed("UVW", PrimitiveType::Float64, vec![3])
                .with_options(ColumnOptions {
                    direct: true,
                    ..ColumnOptions::default()
                })
                .expect("direct vector"),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        for row in 0..256 {
            let value = f64::from(row / 2);
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("SCAN", Value::Scalar(ScalarValue::Int32(row / 7))),
                    RecordField::new(
                        "UVW",
                        Value::Array(ArrayValue::from_f64_vec(vec![
                            value,
                            -value - 0.5,
                            2048.0 + value,
                        ])),
                    ),
                ]))
                .expect("fixture row");
        }
        table
            .save(
                TableOptions::new(&path)
                    .with_data_manager(DataManagerKind::IncrementalStMan)
                    .with_endian_format(endian),
            )
            .expect("write native incremental vectors");
        let table = Table::open(TableOptions::new(&path)).expect("open lazy table");
        let managers = table.data_manager_info();
        assert_eq!(managers.len(), 1);
        assert_eq!(managers[0].dm_type, "IncrementalStMan");
        assert_eq!(managers[0].columns, ["SCAN", "UVW"]);
        let metadata_bytes = table
            .retained_read_metadata_bytes()
            .expect("unmaterialized table metadata");
        let column = table.column_accessor("UVW").expect("UVW accessor");
        let SelectedArray1DCells::Float64(selected) = column
            .array_cells_1d_typed_uncached(&[255, 2, 65, 2, 130, 0])
            .expect("sparse typed read")
        else {
            panic!("expected Float64 vectors");
        };
        assert_eq!((selected.row_count(), selected.axis0_count()), (6, 3));
        assert_eq!(
            selected.values(),
            [
                127.0, -127.5, 2175.0, 1.0, -1.5, 2049.0, 32.0, -32.5, 2080.0, 1.0, -1.5, 2049.0,
                65.0, -65.5, 2113.0, 0.0, -0.5, 2048.0,
            ]
        );
        let mut values = selected.into_values();
        let shape = column
            .fill_array_cells_1d_typed_uncached(
                &[0, 255, 254],
                SelectedArray1DCellsMut::Float64(&mut values),
            )
            .expect("reuse output for another sparse selection");
        assert_eq!((shape.row_count, shape.axis0_count), (3, 3));
        assert_eq!(
            values,
            [
                0.0, -0.5, 2048.0, 127.0, -127.5, 2175.0, 127.0, -127.5, 2175.0
            ]
        );
        column
            .fill_array_cells_1d_typed_uncached(&[], SelectedArray1DCellsMut::Float64(&mut values))
            .expect("empty selection");
        assert!(values.is_empty());
        assert_eq!(table.retained_read_metadata_bytes(), Some(metadata_bytes));
    }
}
