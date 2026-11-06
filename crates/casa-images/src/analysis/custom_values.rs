// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's metadata history rendering for typed custom values.
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use super::projection_header::cpp_number_precision;

pub(super) fn format_array(array: &ArrayValue) -> String {
    let values: Vec<String> = match array {
        ArrayValue::Bool(values) => values
            .iter()
            .map(|v| if *v { "1" } else { "0" }.to_string())
            .collect(),
        ArrayValue::Int32(values) => values.iter().map(ToString::to_string).collect(),
        ArrayValue::Float64(values) => values
            .iter()
            .map(|value| cpp_number_precision(*value, 6))
            .collect(),
        ArrayValue::String(values) => values.iter().cloned().collect(),
        _ => unreachable!("only parameter-converted arrays are formatted"),
    };
    format!("[{}]", values.join(", "))
}

/// Render a parameter-derived record as casacore's ValueHolder does in logs.
pub(super) fn format_record(record: &RecordValue) -> String {
    fn lines(record: &RecordValue, depth: usize) -> String {
        let indent = " ".repeat(depth);
        record
            .fields()
            .iter()
            .map(|field| {
                let prefix = format!("{indent}{}: ", field.name);
                match &field.value {
                    Value::Scalar(ScalarValue::Bool(value)) => {
                        format!("{prefix}Bool {}", u8::from(*value))
                    }
                    Value::Scalar(ScalarValue::Int32(value)) => format!("{prefix}Int {value}"),
                    Value::Scalar(ScalarValue::Float64(value)) => {
                        format!("{prefix}Double {}", cpp_number_precision(*value, 6))
                    }
                    Value::Scalar(ScalarValue::String(value)) => {
                        format!("{prefix}String \"{value}\"")
                    }
                    Value::Array(array) => {
                        let kind = match array {
                            ArrayValue::Bool(_) => "Bool",
                            ArrayValue::Int32(_) => "Int",
                            ArrayValue::Float64(_) => "Double",
                            ArrayValue::String(_) => "String",
                            _ => unreachable!("only parameter-derived records are formatted"),
                        };
                        format!(
                            "{prefix}{kind} array with shape [{}]\n{}{}",
                            array.len(),
                            " ".repeat(depth + 2),
                            format_array(array)
                        )
                    }
                    Value::Record(nested) => {
                        let inner = lines(nested, depth + 2);
                        if inner.is_empty() {
                            format!("{prefix}{{\n{indent}}}")
                        } else {
                            format!("{prefix}{{\n{inner}\n{indent}}}")
                        }
                    }
                    _ => unreachable!("only parameter-derived records are formatted"),
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
    lines(record, 2)
}
