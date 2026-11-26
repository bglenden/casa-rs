// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed access to the scalar and array values stored in an ASAP scan table.

use std::path::Path;

use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions};

use crate::{MsError, MsResult};

pub(super) fn subtable(input: &Path, header: &RecordValue, name: &str) -> MsResult<Table> {
    let relative = match header.get(name) {
        Some(Value::TableRef(path)) => path,
        _ => return Err(invalid(name)),
    };
    Ok(Table::open(TableOptions::new(casa_tables::add_directory(
        relative, input,
    )))?)
}

pub(super) fn string<'a>(row: &'a RecordValue, name: &str) -> MsResult<&'a str> {
    match row.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value),
        _ => Err(invalid(name)),
    }
}

pub(super) fn integer(row: &RecordValue, name: &str) -> MsResult<i32> {
    match row.get(name) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::UInt32(value))) => {
            i32::try_from(*value).map_err(|_| invalid(name))
        }
        _ => Err(invalid(name)),
    }
}

pub(super) fn number(row: &RecordValue, name: &str) -> MsResult<f64> {
    match row.get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Ok(f64::from(*value)),
        _ => Err(invalid(name)),
    }
}

pub(super) fn doubles(row: &RecordValue, name: &str) -> MsResult<Vec<f64>> {
    match row.get(name) {
        Some(Value::Array(ArrayValue::Float64(values))) if values.ndim() == 1 => {
            Ok(values.iter().copied().collect())
        }
        _ => Err(invalid(name)),
    }
}

pub(super) fn floats(row: &RecordValue, name: &str) -> MsResult<Vec<f32>> {
    match row.get(name) {
        Some(Value::Array(ArrayValue::Float32(values))) if values.ndim() == 1 => {
            Ok(values.iter().copied().collect())
        }
        _ => Err(invalid(name)),
    }
}

pub(super) fn flags(row: &RecordValue, name: &str) -> MsResult<Vec<bool>> {
    match row.get(name) {
        Some(Value::Array(ArrayValue::UInt8(values))) if values.ndim() == 1 => {
            Ok(values.iter().map(|&value| value != 0).collect())
        }
        _ => Err(invalid(name)),
    }
}

pub(super) fn invalid(name: &str) -> MsError {
    MsError::InvalidInput(format!(
        "ASAP scan table has an invalid or missing {name} value"
    ))
}
