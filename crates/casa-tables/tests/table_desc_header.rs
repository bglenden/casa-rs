// SPDX-License-Identifier: LGPL-3.0-or-later
//! The name, version and comment of a table description persist in
//! `table.dat`, and a table opened from disk keeps them when re-saved, as
//! casacore's do.
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
