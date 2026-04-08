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
