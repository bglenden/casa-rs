// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's metadata history rendering for typed custom values.
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use super::projection_header::cpp_number_precision;

fn array_elements(array: &ArrayValue, column_major: bool) -> Vec<String> {
    macro_rules! collect {
        ($values:expr, $render:expr) => {
            if column_major {
                $values.view().reversed_axes().iter().map($render).collect()
            } else {
                $values.iter().map($render).collect()
            }
        };
    }
    match array {
        ArrayValue::Bool(values) => collect!(values, |value: &bool| if *value { "1" } else { "0" }
            .to_string()),
        ArrayValue::Int32(values) => collect!(values, |value: &i32| value.to_string()),
        ArrayValue::Float64(values) => {
            collect!(values, |value: &f64| cpp_number_precision(*value, 6))
        }
        ArrayValue::Complex64(values) => collect!(values, |value: &casa_types::Complex64| format!(
            "({},{})",
            cpp_number_precision(value.re, 6),
            cpp_number_precision(value.im, 6)
        )),
        ArrayValue::String(values) => collect!(values, |value: &String| value.clone()),
        _ => unreachable!("only parameter-converted arrays are formatted"),
    }
}

pub(super) fn format_array(array: &ArrayValue) -> String {
    let shape = array.shape();
    if shape.len() > 2 {
        let values = array_elements(array, true);
        let dimensions = shape
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let lines = values
            .chunks(shape[0])
            .enumerate()
            .map(|(index, row)| {
                let mut index = index;
                let mut position = vec![0];
                for size in &shape[1..] {
                    position.push(index % size);
                    index /= size;
                }
                let position = position
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{position}][{}]", row.join(", "))
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "Ndim={} Axis Lengths: [{dimensions}] \n{lines}",
            shape.len()
        )
    } else if let [rows, columns] = shape {
        let values = array_elements(array, false);
        let lines = values
            .chunks(*columns)
            .map(|row| row.join(", "))
            .collect::<Vec<_>>()
            .join("\n ");
        format!("Axis Lengths: [{rows}, {columns}]  (NB: Matrix in Row/Column order)\n[{lines}]")
    } else {
        format!("[{}]", array_elements(array, false).join(", "))
    }
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
                    Value::Scalar(ScalarValue::Complex64(value)) => {
                        format!(
                            "{prefix}DComplex ({},{})",
                            cpp_number_precision(value.re, 6),
                            cpp_number_precision(value.im, 6)
                        )
                    }
                    Value::Scalar(ScalarValue::String(value)) => {
                        format!("{prefix}String \"{value}\"")
                    }
                    Value::Array(array) => {
                        let kind = match array {
                            ArrayValue::Bool(_) => "Bool",
                            ArrayValue::Int32(_) => "Int",
                            ArrayValue::Float64(_) => "Double",
                            ArrayValue::Complex64(_) => "DComplex",
                            ArrayValue::String(_) => "String",
                            _ => unreachable!("only parameter-derived records are formatted"),
                        };
                        let shape = array
                            .shape()
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ");
                        let values = array_elements(array, true).join(", ");
                        format!(
                            "{prefix}{kind} array with shape [{shape}]\n{}[{values}]",
                            " ".repeat(depth + 2)
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
