// SPDX-License-Identifier: LGPL-3.0-or-later
//! casa-rs forms the records CASA's `cl.getcomponent` and `cl.torecord`
//! return (simulation Phase 2, #841).
//!
//! `tests/data/casa-6.7.6/components.json` holds CASA 6.7.6's
//! `getcomponent(i)` for every component of `components.cl`
//! (`tools/simulation/componentlist_oracle.py fixture`). casa-rs reads the
//! same table and forms each record: the same fields, strings and units, and
//! the numbers to 1e-13 relative (CASA converts its display units with other
//! roundings).

mod common;

use casa_components::ComponentList;
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use common::{copy_table, fixture_dir, load_json};
use serde_json::Value as Json;

fn fixture_list(scratch: &std::path::Path) -> ComponentList {
    let table = copy_table(&fixture_dir().join("components.cl"), scratch);
    ComponentList::read_table(&table).expect("read components.cl")
}

#[test]
fn a_components_record_is_the_one_casa_returns_for_the_same_table() {
    let scratch = tempfile::tempdir().expect("scratch");
    let list = fixture_list(scratch.path());
    let document = load_json(&fixture_dir().join("components.json"));
    let casa = document["components"].as_array().expect("components");
    assert_eq!(list.len(), casa.len());
    for (row, (component, expected)) in list.iter().zip(casa).enumerate() {
        let record = component.to_record().expect("record");
        assert_same(&record_json(&record), expected, &format!("row {row}"));
    }
}

#[test]
fn a_lists_record_numbers_its_components() {
    let scratch = tempfile::tempdir().expect("scratch");
    let list = fixture_list(scratch.path());
    let record = list.to_record().expect("record");
    let json = record_json(&record);
    assert_eq!(json["nelements"], Json::from(list.len()));
    let fields: Vec<&str> = record
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect();
    let mut expected = vec!["nelements".to_string()];
    expected.extend((0..list.len()).map(|index| format!("component{index}")));
    assert_eq!(fields, expected);
    for (index, component) in list.iter().enumerate() {
        assert_eq!(
            json[format!("component{index}")],
            record_json(&component.to_record().expect("record"))
        );
    }
    let empty = ComponentList::new().to_record().expect("record");
    assert_eq!(record_json(&empty), serde_json::json!({"nelements": 0}));
}

/// `actual` equals CASA's `expected`: the same keys and strings, and numbers
/// to 1e-13 relative.
fn assert_same(actual: &Json, expected: &Json, path: &str) {
    match (actual, expected) {
        (Json::Object(actual), Json::Object(expected)) => {
            let keys = |object: &serde_json::Map<String, Json>| {
                let mut keys: Vec<String> = object.keys().cloned().collect();
                keys.sort();
                keys
            };
            assert_eq!(keys(actual), keys(expected), "{path}: fields");
            for (key, value) in expected {
                assert_same(&actual[key], value, &format!("{path}.{key}"));
            }
        }
        (Json::Array(actual), Json::Array(expected)) => {
            assert_eq!(actual.len(), expected.len(), "{path}: length");
            for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
                assert_same(actual, expected, &format!("{path}[{index}]"));
            }
        }
        (Json::Number(actual), Json::Number(expected)) => {
            let (actual, expected) = (
                actual.as_f64().expect("f64"),
                expected.as_f64().expect("f64"),
            );
            assert!(
                (actual - expected).abs() <= 1e-13 * expected.abs(),
                "{path}: {actual} against CASA's {expected}"
            );
        }
        _ => assert_eq!(actual, expected, "{path}"),
    }
}

/// A record as the oracle's JSON writes CASA's: complex numbers as
/// `[re, im]`.
fn record_json(record: &RecordValue) -> Json {
    Json::Object(
        record
            .fields()
            .iter()
            .map(|field| (field.name.clone(), value_json(&field.value)))
            .collect(),
    )
}

fn value_json(value: &Value) -> Json {
    match value {
        Value::Scalar(ScalarValue::String(text)) => Json::String(text.clone()),
        Value::Scalar(ScalarValue::Float64(number)) => Json::from(*number),
        Value::Scalar(ScalarValue::Int32(number)) => Json::from(*number),
        Value::Array(ArrayValue::Float64(numbers)) => {
            Json::Array(numbers.iter().map(|number| Json::from(*number)).collect())
        }
        Value::Array(ArrayValue::Complex64(numbers)) => Json::Array(
            numbers
                .iter()
                .map(|number| Json::Array(vec![Json::from(number.re), Json::from(number.im)]))
                .collect(),
        ),
        Value::Record(record) => record_json(record),
        other => panic!("unexpected record value {other:?}"),
    }
}
