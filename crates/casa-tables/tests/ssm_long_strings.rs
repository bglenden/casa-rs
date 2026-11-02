// SPDX-License-Identifier: LGPL-3.0-or-later
//! StandardStMan stores a string longer than a string bucket over several
//! buckets, each linked from the one before, as casacore's
//! `SSMStringHandler` does, and reads it back whole: a scalar string, and
//! the elements of a fixed and a variable string array, in either byte
//! order, with multi-byte characters split across buckets.

use casa_tables::{
    ColumnOptions, ColumnSchema, DataManagerKind, EndianFormat, Table, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

/// About `bytes` bytes of text, a run of one- and two-byte characters
/// starting at character `seed`.
fn text(bytes: usize, seed: usize) -> String {
    let characters = ['a', 'é', 'b', 'ç', 'c', '-', 'ü', 'd'];
    let mut text = String::with_capacity(bytes + 2);
    let mut index = seed;
    while text.len() < bytes {
        text.push(characters[index % characters.len()]);
        index += 1;
    }
    text
}

fn strings(shape: &[usize], values: Vec<String>) -> Value {
    Value::Array(ArrayValue::String(
        ArrayD::from_shape_vec(IxDyn(shape), values).expect("shape"),
    ))
}

/// Rows of long and short strings: a name of 100 kB, one just over a
/// bucket and a short one inline; array elements of tens of kB beside
/// short and empty ones.
fn rows() -> Vec<RecordValue> {
    let row = |name: String, fixed: [String; 3], variable: Vec<String>| {
        let count = variable.len();
        RecordValue::new(vec![
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name))),
            RecordField::new("FIXED", strings(&[3], fixed.to_vec())),
            RecordField::new("VARIABLE", strings(&[count], variable)),
        ])
    };
    vec![
        row(
            text(100_000, 0),
            [text(70_000, 1), "x".to_string(), text(40_000, 2)],
            vec![text(33_000, 3), text(5, 4)],
        ),
        row(
            "short".to_string(),
            [String::new(), String::new(), String::new()],
            vec!["a".to_string()],
        ),
        row(
            text(32_800, 5),
            ["fixed".to_string(), text(32_769, 6), String::new()],
            vec![text(65_536, 7), String::new(), text(1_000, 8)],
        ),
    ]
}

#[test]
fn strings_longer_than_a_bucket_round_trip() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("NAME", PrimitiveType::String),
        ColumnSchema::array_fixed("FIXED", PrimitiveType::String, vec![3])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("direct fixed array column"),
        ColumnSchema::array_variable("VARIABLE", PrimitiveType::String, Some(1)),
    ])
    .expect("schema");
    let written = rows();
    for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("table");
        let mut table = Table::with_schema(schema.clone());
        for row in &written {
            table.add_row(row.clone()).expect("row");
        }
        table
            .save(
                TableOptions::new(&path)
                    .with_data_manager(DataManagerKind::StandardStMan)
                    .with_endian_format(endian),
            )
            .expect("save");
        let reopened = Table::open(TableOptions::new(&path)).expect("reopen");
        let rows = reopened.row_accessor();
        assert_eq!(rows.row_count(), written.len());
        for (index, expected) in written.iter().enumerate() {
            let read = rows.row(index).expect("row");
            for column in ["NAME", "FIXED", "VARIABLE"] {
                assert!(
                    read.get(column) == expected.get(column),
                    "{endian:?}: row {index} {column} differs"
                );
            }
        }
    }
}
