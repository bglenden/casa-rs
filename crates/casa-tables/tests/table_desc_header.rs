// SPDX-License-Identifier: LGPL-3.0-or-later
//! The name, version and comment of a table description persist in
//! `table.dat`, and a table opened from disk keeps them when re-saved, as
//! casacore's do. Row copies ignore them, and the `Undefined` option that
//! component lists put on array and record columns persists without changing
//! what a record cell holds.
//!
//! casa-components writes CASA's `ComponentListDescription`, version 4; its
//! CASA fixture tests read the header of a table CASA wrote, and its CASA
//! oracle checks that CASA reads the lists casa-rs writes.

use casa_tables::{
    ColumnOptions, ColumnSchema, DataManagerKind, Table, TableDescHeader, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

fn header() -> TableDescHeader {
    TableDescHeader {
        name: "ComponentListDescription".to_string(),
        version: "4".to_string(),
        comment: "A description of a component list".to_string(),
    }
}

fn table_with_header() -> Table {
    let direct = ColumnOptions {
        direct: true,
        undefined: false,
    };
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("Label", PrimitiveType::String)
            .with_options(direct)
            .expect("direct scalar"),
        ColumnSchema::array_variable("Optional_Parameters", PrimitiveType::Float64, Some(1))
            .with_options(ColumnOptions {
                direct: false,
                undefined: true,
            })
            .expect("undefined array"),
    ])
    .expect("schema")
    .with_header(header());
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("Label", Value::Scalar(ScalarValue::String("a".to_string()))),
            RecordField::new(
                "Optional_Parameters",
                Value::Array(ArrayValue::from_f64_vec(vec![0.7])),
            ),
        ]))
        .expect("row with optional parameters");
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "Label",
            Value::Scalar(ScalarValue::String("b".to_string())),
        )]))
        .expect("row without");
    table
}

#[test]
fn headers_persist_and_survive_a_resave() {
    let scratch = tempfile::tempdir().expect("scratch");
    for manager in [DataManagerKind::StandardStMan, DataManagerKind::StManAipsIO] {
        let first = scratch.path().join(format!("{manager:?}.tab"));
        table_with_header()
            .save(TableOptions::new(&first).with_data_manager(manager))
            .expect("save");
        let opened = Table::open(TableOptions::new(&first)).expect("open");
        let schema = opened.schema().expect("schema");
        assert_eq!(schema.header(), &header(), "{manager:?}");
        let optional = schema.column("Optional_Parameters").expect("column");
        assert!(
            optional.options().undefined,
            "{manager:?}: Undefined persists on an array"
        );
        assert!(schema.column("Label").expect("column").options().direct);

        let second = scratch.path().join(format!("{manager:?}-resaved.tab"));
        opened
            .save(TableOptions::new(&second).with_data_manager(manager))
            .expect("re-save");
        let reopened = Table::open(TableOptions::new(&second)).expect("reopen");
        assert_eq!(
            reopened.schema().expect("schema").header(),
            &header(),
            "{manager:?}: re-save"
        );
    }
}

#[test]
fn schemas_built_in_rust_have_empty_headers() {
    let scratch = tempfile::tempdir().expect("scratch");
    let path = scratch.path().join("plain.tab");
    let schema =
        TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).expect("schema");
    assert_eq!(schema.header(), &TableDescHeader::default());
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(1)),
        )]))
        .expect("row");
    table.save(TableOptions::new(&path)).expect("save");
    let opened = Table::open(TableOptions::new(&path)).expect("open");
    assert_eq!(
        opened.schema().expect("schema").header(),
        &TableDescHeader::default()
    );
}

fn int_row(value: i32) -> RecordValue {
    RecordValue::new(vec![RecordField::new(
        "x",
        Value::Scalar(ScalarValue::Int32(value)),
    )])
}

#[test]
fn row_copies_compare_columns_not_headers() {
    // casacore's `TableCopy::copyRows` copies the columns the two tables
    // share and never compares their descriptions' names.
    let columns = || vec![ColumnSchema::scalar("x", PrimitiveType::Int32)];
    let mut source = Table::with_schema(
        TableSchema::new(columns())
            .expect("schema")
            .with_header(header()),
    );
    source.add_row(int_row(7)).expect("row");
    let mut target = Table::with_schema(TableSchema::new(columns()).expect("schema"));
    target.copy_rows(&source).expect("copy_rows");
    target
        .copy_rows_with_mapping(&source, &[0])
        .expect("copy_rows_with_mapping");
    assert_eq!(target.rows().expect("rows"), &[int_row(7), int_row(7)]);
    assert_eq!(
        target.schema().expect("schema").header(),
        &TableDescHeader::default(),
        "the target keeps its own header"
    );
}

#[test]
fn record_cells_stay_defined_under_the_undefined_option() {
    // casacore keeps `Undefined` on a record column but gives it no meaning
    // there: `ScalarRecordColumnData::isDefined` is always true, and a record
    // never written reads as an empty one.
    let scratch = tempfile::tempdir().expect("scratch");
    for manager in [DataManagerKind::StandardStMan, DataManagerKind::StManAipsIO] {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("x", PrimitiveType::Int32),
            ColumnSchema::record("r")
                .with_options(ColumnOptions {
                    direct: false,
                    undefined: true,
                })
                .expect("undefined record"),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        table.add_row(int_row(1)).expect("row without r");
        assert!(
            table.is_cell_defined(0, "r").expect("defined"),
            "{manager:?}: before save"
        );
        assert_eq!(
            table.record_cell(0, "r").expect("r"),
            RecordValue::default()
        );

        let path = scratch.path().join(format!("{manager:?}.tab"));
        table
            .save(TableOptions::new(&path).with_data_manager(manager))
            .expect("save");
        let reopened = Table::open(TableOptions::new(&path)).expect("open");
        let column = reopened.schema().expect("schema").column("r").expect("r");
        assert!(column.options().undefined, "{manager:?}: the bit persists");
        assert!(
            reopened.is_cell_defined(0, "r").expect("defined"),
            "{manager:?}: after reopen"
        );
        assert_eq!(
            reopened.record_cell(0, "r").expect("r"),
            RecordValue::default()
        );
    }
}
