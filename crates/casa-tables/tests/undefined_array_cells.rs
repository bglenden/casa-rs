// SPDX-License-Identifier: LGPL-3.0-or-later
//! An undefined array cell reads back as no value (#852, undefined
//! indirect cells).
//!
//! casacore has no array to give for a variable-shape cell no row wrote:
//! `ArrayColumn::get` throws "no array in row" and `isDefined` is false. A
//! table casa-rs opened once read such a cell as a placeholder array (of
//! rank 0 and one element when the column's rank is unknown) while
//! `is_cell_defined` said it was undefined. Now the cell reads as a table
//! built from rows holds it: no field in the row, `None` from a cell read,
//! a missing-cell error from an array read.

use std::collections::HashMap;
use std::path::Path;

use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, Table, TableError, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

const ROWS: usize = 3;

/// The storage of a case: the table's manager, and FLAGS's own when it has
/// one.
type Storage = (DataManagerKind, Option<DataManagerKind>);

/// StManAipsIO and StandardStMan keep undefined cells as array-file offsets
/// of 0, TiledShapeStMan as rows no hypercube holds.
const STORAGES: [Storage; 3] = [
    (DataManagerKind::StManAipsIO, None),
    (DataManagerKind::StandardStMan, None),
    (
        DataManagerKind::StandardStMan,
        Some(DataManagerKind::TiledShapeStMan),
    ),
];

fn schema() -> TableSchema {
    TableSchema::new(vec![
        // Of unknown rank: a placeholder was a rank-0 array.
        ColumnSchema::array_variable("DATA", PrimitiveType::Float64, None),
        ColumnSchema::scalar("ID", PrimitiveType::Int32),
        ColumnSchema::array_variable("FLAGS", PrimitiveType::Bool, Some(2)),
    ])
    .expect("schema")
}

/// DATA is undefined in row 0 and FLAGS in row 1.
fn cell(column: &str, row: usize) -> Option<Value> {
    match (column, row) {
        ("DATA", 0) | ("FLAGS", 1) => None,
        ("DATA", _) => Some(Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[row]), (0..row).map(|i| i as f64).collect())
                .expect("shape"),
        ))),
        ("FLAGS", _) => Some(Value::Array(ArrayValue::Bool(ArrayD::from_elem(
            IxDyn(&[2, row + 1]),
            true,
        )))),
        _ => Some(Value::Scalar(ScalarValue::Int32(10 * row as i32))),
    }
}

/// Save `table` at `path` with `storage`.
fn save(table: &Table, path: &Path, (manager, flags): Storage) {
    let bindings = flags
        .map(|data_manager| {
            let binding = ColumnBinding {
                data_manager,
                tile_shape: Some(vec![2, 4, 2]),
            };
            HashMap::from([("FLAGS".to_string(), binding)])
        })
        .unwrap_or_default();
    table
        .save_with_bindings(
            TableOptions::new(path).with_data_manager(manager),
            &bindings,
        )
        .expect("save");
}

/// The table of [`cell`]'s cells.
fn table() -> Table {
    let mut table = Table::with_schema(schema());
    for row in 0..ROWS {
        let fields = ["DATA", "ID", "FLAGS"]
            .into_iter()
            .filter_map(|column| cell(column, row).map(|value| RecordField::new(column, value)))
            .collect();
        table.add_row(RecordValue::new(fields)).expect("add row");
    }
    table
}

/// Every read of `table` sees the cells of [`cell`], the undefined ones as
/// no value.
fn assert_reads(table: &Table, case: &str) {
    for column in ["DATA", "FLAGS"] {
        let read = table
            .column_accessor(column)
            .and_then(|cells| cells.array_cells_owned(&[0, 1, 2]))
            .expect("column read");
        for (row, column_read) in read.into_iter().enumerate() {
            let expected = cell(column, row);
            let cell_read = table.cell_accessor(row, column).expect("cell");
            assert_eq!(
                column_read.map(Value::Array),
                expected,
                "{case}: {column} row {row} column read"
            );
            assert_eq!(
                cell_read.value().expect("value"),
                expected.as_ref(),
                "{case}: {column} row {row}"
            );
            assert_eq!(
                cell_read.is_defined(),
                Ok(expected.is_some()),
                "{case}: {column} row {row} definedness"
            );
            if expected.is_none() {
                assert!(
                    matches!(cell_read.array(), Err(TableError::ColumnNotFound { .. })),
                    "{case}: {column} row {row} array read"
                );
                assert_eq!(
                    cell_read.array_shape(),
                    Ok(None),
                    "{case}: {column} row {row}"
                );
            }
        }
    }
}

#[test]
fn undefined_array_cells_read_back_as_no_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    for storage in STORAGES {
        let case = format!("{storage:?}");
        let path = dir.path().join(&case);
        save(&table(), &path, storage);

        // A prepared row of the freshly opened table, whose cached rows
        // leave out undefined cells, finds each slot after one.
        let opened = Table::open(TableOptions::new(&path)).expect("open");
        let flags_manager = opened
            .data_manager_info()
            .iter()
            .find(|manager| manager.columns.iter().any(|column| column == "FLAGS"))
            .map(|manager| manager.dm_type.clone());
        assert_eq!(
            flags_manager,
            Some(format!("{:?}", storage.1.unwrap_or(storage.0))),
            "{case}: FLAGS's manager"
        );
        let mut row = opened
            .row_accessor()
            .prepare(&["DATA", "ID", "FLAGS"])
            .expect("prepare");
        row.load(0).expect("load row 0");
        assert!(row.array_at(0).is_err(), "{case}: DATA row 0");
        assert_eq!(row.scalar_at(1), Ok(&ScalarValue::Int32(0)), "{case}");
        assert_eq!(
            row.array_at(2).map(|array| array.shape().to_vec()),
            Ok(vec![2, 1]),
            "{case}"
        );
        assert!(row.row().is_none(), "{case}: row 0 has an undefined cell");

        // Column reads of a freshly opened table, then the loaded rows.
        let table = Table::open(TableOptions::new(&path)).expect("open");
        assert_reads(&table, &case);
        let rows = table.rows().expect("rows");
        assert!(rows[0].get("DATA").is_none(), "{case}: row 0 has no DATA");
        assert!(rows[1].get("FLAGS").is_none(), "{case}: row 1 has no FLAGS");
        assert_reads(&table, &format!("{case}, loaded"));

        // Saving the opened table keeps the cells undefined.
        let copy = dir.path().join(format!("{case}-copy"));
        save(&table, &copy, storage);
        let reopened = Table::open(TableOptions::new(&copy)).expect("reopen");
        assert_reads(&reopened, &format!("{case}, saved again"));
    }
}
