// SPDX-License-Identifier: LGPL-3.0-or-later
//! Internal helpers for parsing coordinate records.

use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use crate::error::CoordinateError;

pub(crate) fn get_optional_string(rec: &RecordValue, field: &str) -> Option<String> {
    match rec.get(field) {
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.clone()),
        _ => None,
    }
}

pub(crate) fn get_optional_f64(rec: &RecordValue, field: &str) -> Option<f64> {
    match rec.get(field) {
        Some(Value::Scalar(scalar)) => scalar_to_f64(scalar),
        Some(Value::Array(array)) if array.len() == 1 => array_to_f64_vec(array).ok()?.pop(),
        _ => None,
    }
}

pub(crate) fn get_required_f64(rec: &RecordValue, field: &str) -> Result<f64, CoordinateError> {
    get_optional_f64(rec, field)
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing or invalid {field}")))
}

pub(crate) fn get_optional_i32(rec: &RecordValue, field: &str) -> Option<i32> {
    match rec.get(field) {
        Some(Value::Scalar(scalar)) => scalar_to_i32(scalar),
        Some(Value::Array(array)) if array.len() == 1 => array_to_i32_vec(array).ok()?.pop(),
        _ => None,
    }
}

pub(crate) fn get_required_vec_f64(
    rec: &RecordValue,
    field: &str,
) -> Result<Vec<f64>, CoordinateError> {
    match rec.get(field) {
        Some(Value::Array(array)) => array_to_f64_vec(array)
            .map_err(|message| CoordinateError::InvalidRecord(format!("{field}: {message}"))),
        Some(Value::Scalar(scalar)) => scalar_to_f64(scalar)
            .map(|value| vec![value])
            .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing or invalid {field}"))),
        _ => Err(CoordinateError::InvalidRecord(format!(
            "missing or invalid {field}"
        ))),
    }
}

pub(crate) fn get_optional_vec_f64(rec: &RecordValue, field: &str) -> Option<Vec<f64>> {
    match rec.get(field) {
        Some(Value::Array(array)) => array_to_f64_vec(array).ok(),
        Some(Value::Scalar(scalar)) => scalar_to_f64(scalar).map(|value| vec![value]),
        _ => None,
    }
}

pub(crate) fn get_required_vec_i32(
    rec: &RecordValue,
    field: &str,
) -> Result<Vec<i32>, CoordinateError> {
    match rec.get(field) {
        Some(Value::Array(array)) => array_to_i32_vec(array)
            .map_err(|message| CoordinateError::InvalidRecord(format!("{field}: {message}"))),
        Some(Value::Scalar(scalar)) => scalar_to_i32(scalar)
            .map(|value| vec![value])
            .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing or invalid {field}"))),
        _ => Err(CoordinateError::InvalidRecord(format!(
            "missing or invalid {field}"
        ))),
    }
}

pub(crate) fn get_optional_vec_string(rec: &RecordValue, field: &str) -> Option<Vec<String>> {
    match rec.get(field) {
        Some(Value::Array(ArrayValue::String(array))) => Some(array.iter().cloned().collect()),
        Some(Value::Scalar(ScalarValue::String(value))) => Some(vec![value.clone()]),
        _ => None,
    }
}

fn scalar_to_f64(scalar: &ScalarValue) -> Option<f64> {
    match scalar {
        ScalarValue::UInt8(value) => Some(f64::from(*value)),
        ScalarValue::UInt16(value) => Some(f64::from(*value)),
        ScalarValue::UInt32(value) => Some(*value as f64),
        ScalarValue::Int16(value) => Some(f64::from(*value)),
        ScalarValue::Int32(value) => Some(f64::from(*value)),
        ScalarValue::Int64(value) => Some(*value as f64),
        ScalarValue::Float32(value) => Some(f64::from(*value)),
        ScalarValue::Float64(value) => Some(*value),
        _ => None,
    }
}

fn scalar_to_i32(scalar: &ScalarValue) -> Option<i32> {
    match scalar {
        ScalarValue::UInt8(value) => Some(i32::from(*value)),
        ScalarValue::UInt16(value) => Some(i32::from(*value)),
        ScalarValue::UInt32(value) => i32::try_from(*value).ok(),
        ScalarValue::Int16(value) => Some(i32::from(*value)),
        ScalarValue::Int32(value) => Some(*value),
        ScalarValue::Int64(value) => i32::try_from(*value).ok(),
        _ => None,
    }
}

fn array_to_f64_vec(array: &ArrayValue) -> Result<Vec<f64>, &'static str> {
    match array {
        ArrayValue::UInt8(values) => Ok(values.iter().map(|&value| f64::from(value)).collect()),
        ArrayValue::UInt16(values) => Ok(values.iter().map(|&value| f64::from(value)).collect()),
        ArrayValue::UInt32(values) => Ok(values.iter().map(|&value| value as f64).collect()),
        ArrayValue::Int16(values) => Ok(values.iter().map(|&value| f64::from(value)).collect()),
        ArrayValue::Int32(values) => Ok(values.iter().map(|&value| f64::from(value)).collect()),
        ArrayValue::Int64(values) => Ok(values.iter().map(|&value| value as f64).collect()),
        ArrayValue::Float32(values) => Ok(values.iter().map(|&value| f64::from(value)).collect()),
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        _ => Err("expected numeric array"),
    }
}

fn array_to_i32_vec(array: &ArrayValue) -> Result<Vec<i32>, &'static str> {
    match array {
        ArrayValue::UInt8(values) => Ok(values.iter().map(|&value| i32::from(value)).collect()),
        ArrayValue::UInt16(values) => Ok(values.iter().map(|&value| i32::from(value)).collect()),
        ArrayValue::UInt32(values) => values
            .iter()
            .copied()
            .map(i32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "integer value out of range"),
        ArrayValue::Int16(values) => Ok(values.iter().map(|&value| i32::from(value)).collect()),
        ArrayValue::Int32(values) => Ok(values.iter().copied().collect()),
        ArrayValue::Int64(values) => values
            .iter()
            .copied()
            .map(i32::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "integer value out of range"),
        _ => Err("expected integer array"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::{RecordField, ScalarValue};

    fn record(fields: Vec<(&str, Value)>) -> RecordValue {
        RecordValue::new(
            fields
                .into_iter()
                .map(|(name, value)| RecordField::new(name, value))
                .collect(),
        )
    }

    #[test]
    fn optional_scalar_helpers_accept_numeric_and_string_shapes() {
        let rec = record(vec![
            (
                "name",
                Value::Scalar(ScalarValue::String("J2000".to_string())),
            ),
            ("double", Value::Scalar(ScalarValue::Float64(12.5))),
            ("single", Value::Array(ArrayValue::from_f32_vec(vec![2.25]))),
            ("int", Value::Scalar(ScalarValue::UInt16(7))),
            ("int_array", Value::Array(ArrayValue::from_i64_vec(vec![8]))),
            ("bad", Value::Scalar(ScalarValue::Bool(true))),
        ]);

        assert_eq!(get_optional_string(&rec, "name").as_deref(), Some("J2000"));
        assert_eq!(get_optional_f64(&rec, "double"), Some(12.5));
        assert_eq!(get_optional_f64(&rec, "single"), Some(2.25));
        assert_eq!(
            get_required_f64(&rec, "double").expect("required f64"),
            12.5
        );
        assert_eq!(get_optional_i32(&rec, "int"), Some(7));
        assert_eq!(get_optional_i32(&rec, "int_array"), Some(8));
        assert_eq!(get_optional_f64(&rec, "bad"), None);
        assert!(get_required_f64(&rec, "missing").is_err());
    }

    #[test]
    fn vector_helpers_accept_scalars_arrays_and_report_type_errors() {
        let rec = record(vec![
            (
                "f64s",
                Value::Array(ArrayValue::from_i32_vec(vec![1, 2, 3])),
            ),
            ("f64_scalar", Value::Scalar(ScalarValue::UInt8(4))),
            ("i32s", Value::Array(ArrayValue::from_u16_vec(vec![5, 6]))),
            ("i32_scalar", Value::Scalar(ScalarValue::Int16(-7))),
            (
                "strings",
                Value::Array(ArrayValue::from_string_vec(vec![
                    "XX".to_string(),
                    "YY".to_string(),
                ])),
            ),
            (
                "string_scalar",
                Value::Scalar(ScalarValue::String("I".to_string())),
            ),
            (
                "bad_numeric",
                Value::Array(ArrayValue::from_string_vec(vec!["nope".to_string()])),
            ),
            (
                "bad_i32",
                Value::Array(ArrayValue::from_u32_vec(vec![u32::MAX])),
            ),
        ]);

        assert_eq!(
            get_required_vec_f64(&rec, "f64s").expect("numeric array"),
            vec![1.0, 2.0, 3.0]
        );
        assert_eq!(
            get_optional_vec_f64(&rec, "f64_scalar").expect("numeric scalar"),
            vec![4.0]
        );
        assert_eq!(
            get_required_vec_i32(&rec, "i32s").expect("integer array"),
            vec![5, 6]
        );
        assert_eq!(
            get_required_vec_i32(&rec, "i32_scalar").expect("integer scalar"),
            vec![-7]
        );
        assert_eq!(
            get_optional_vec_string(&rec, "strings").expect("string array"),
            vec!["XX".to_string(), "YY".to_string()]
        );
        assert_eq!(
            get_optional_vec_string(&rec, "string_scalar").expect("string scalar"),
            vec!["I".to_string()]
        );
        assert!(get_required_vec_f64(&rec, "bad_numeric").is_err());
        assert!(get_required_vec_i32(&rec, "bad_i32").is_err());
        assert!(get_required_vec_i32(&rec, "missing").is_err());
    }
}
