// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rows of MeasurementSet subtables built from their schema: every required
//! column at its default, with the values a writer gives overriding them.
//! The simulator and the converter fill subtables through these.

use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use num_complex::Complex32;

use crate::MeasurementSet;
use crate::column_def::{ColumnDef, ColumnKind};
use crate::error::{MsError, MsResult};
use crate::schema::SubtableId;

/// Subtable `id` of `ms`, writable.
pub(crate) fn subtable_mut(
    ms: &mut MeasurementSet,
    id: SubtableId,
) -> MsResult<&mut casa_tables::Table> {
    ms.subtable_mut(id)
        .ok_or_else(|| MsError::MissingSubtable(id.name().to_string()))
}

/// A row of a subtable whose required columns are `defs`: each column's
/// value from `overrides`, or its default (zero, false, empty).
pub(crate) fn row_from_defs(defs: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
    let fields = defs
        .iter()
        .map(|definition| {
            if let Some((_, value)) = overrides.iter().find(|(name, _)| *name == definition.name) {
                RecordField::new(definition.name, value.clone())
            } else {
                RecordField::new(definition.name, default_value(definition))
            }
        })
        .collect();
    RecordValue::new(fields)
}

fn default_value(definition: &ColumnDef) -> Value {
    match definition.column_kind {
        ColumnKind::Scalar => match definition.data_type {
            casa_types::PrimitiveType::Bool => b(false),
            casa_types::PrimitiveType::Int32 => i(0),
            casa_types::PrimitiveType::Float64 => f(0.0),
            casa_types::PrimitiveType::String => s(""),
            casa_types::PrimitiveType::Float32 => Value::Scalar(ScalarValue::Float32(0.0)),
            casa_types::PrimitiveType::Complex32 => {
                Value::Scalar(ScalarValue::Complex32(Complex32::new(0.0, 0.0)))
            }
            _ => i(0),
        },
        ColumnKind::FixedArray { shape } => {
            let total = shape.iter().product();
            match definition.data_type {
                casa_types::PrimitiveType::Bool => bool_array(&vec![false; total], shape.to_vec()),
                casa_types::PrimitiveType::Float32 => f32_array(&vec![0.0; total], shape.to_vec()),
                casa_types::PrimitiveType::Int32 => i32_array(&vec![0; total], shape.to_vec()),
                casa_types::PrimitiveType::String => string_array(&vec![""; total], shape.to_vec()),
                _ => f64_array(&vec![0.0; total], shape.to_vec()),
            }
        }
        ColumnKind::VariableArray { ndim } => {
            let shape = vec![1; ndim];
            let total = shape.iter().product();
            match definition.data_type {
                casa_types::PrimitiveType::Bool => bool_array(&vec![false; total], shape),
                casa_types::PrimitiveType::Float32 => f32_array(&vec![0.0; total], shape),
                casa_types::PrimitiveType::Int32 => i32_array(&vec![0; total], shape),
                casa_types::PrimitiveType::String => string_array(&vec![""; total], shape),
                casa_types::PrimitiveType::Complex32 => {
                    complex_array(&vec![Complex32::new(0.0, 0.0); total], shape)
                }
                _ => f64_array(&vec![0.0; total], shape),
            }
        }
    }
}

pub(crate) fn s(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

pub(crate) fn i(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

pub(crate) fn f(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

pub(crate) fn b(value: bool) -> Value {
    Value::Scalar(ScalarValue::Bool(value))
}

pub(crate) fn bool_array(values: &[bool], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Bool(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

pub(crate) fn i32_array(values: &[i32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Int32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

pub(crate) fn f32_array(values: &[f32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Float32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

pub(crate) fn f64_array(values: &[f64], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

pub(crate) fn complex_array(values: &[Complex32], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::Complex32(
        ArrayD::from_shape_vec(shape, values.to_vec()).unwrap(),
    ))
}

pub(crate) fn string_array(values: &[&str], shape: Vec<usize>) -> Value {
    Value::Array(ArrayValue::String(
        ArrayD::from_shape_vec(
            shape,
            values.iter().map(|value| value.to_string()).collect(),
        )
        .unwrap(),
    ))
}
