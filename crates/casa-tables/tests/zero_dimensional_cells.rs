// SPDX-License-Identifier: LGPL-3.0-or-later
//! A 0-dimensional cell keeps its element on disk (#923).
//!
//! A casa-types array of no dimensions holds one element. casacore's
//! 0-dimensional shape, the empty `IPosition`, holds none, and casacore and
//! casa-rs read a cell stored with it as empty. Such a cell is therefore
//! stored as `[1]`, numeric or string, by StManAipsIO and StandardStMan,
//! and reads back as one element. ISM and the tiled managers refuse such a
//! cell, except TiledCellStMan, which drops it (#930).

use casa_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn};

#[test]
fn a_zero_dimensional_cell_is_stored_with_its_element() {
    for manager in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("table");
        let mut table = Table::with_schema(
            TableSchema::new(vec![
                ColumnSchema::array_variable("VALUES", PrimitiveType::Float64, None),
                ColumnSchema::array_variable("NAMES", PrimitiveType::String, None),
            ])
            .expect("schema"),
        );
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "VALUES",
                    Value::Array(ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[]), 7.0))),
                ),
                RecordField::new(
                    "NAMES",
                    Value::Array(ArrayValue::String(ArrayD::from_elem(
                        IxDyn(&[]),
                        "line".to_string(),
                    ))),
                ),
            ]))
            .expect("row");
        table
            .save(TableOptions::new(&path).with_data_manager(manager))
            .expect("save");
        let reopened = Table::open(TableOptions::new(&path)).expect("reopen");
        let row = reopened.row_accessor().row(0).expect("row");
        match row.get("VALUES") {
            Some(Value::Array(ArrayValue::Float64(values))) => {
                assert_eq!(values.shape(), [1], "{manager:?}");
                assert_eq!(values[[0]], 7.0, "{manager:?}");
            }
            other => panic!("{manager:?}: VALUES {other:?}"),
        }
        match row.get("NAMES") {
            Some(Value::Array(ArrayValue::String(names))) => {
                assert_eq!(names.shape(), [1], "{manager:?}");
                assert_eq!(names[[0]], "line", "{manager:?}");
            }
            other => panic!("{manager:?}: NAMES {other:?}"),
        }
    }
}
