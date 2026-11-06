// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversions between parameter values and casacore's typed values.

use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

use crate::{CasaFloat, CasaValue, ComplexDtype, FloatDtype, IntDtype, ParameterValue};

/// Convert parameter data to casacore metadata values, including records.
///
/// Record keys use the parameter model's sorted order. Array conversion
/// follows [`parameter_array_to_casa`].
///
/// # Errors
///
/// Returns an error for unsupported values, integer overflow, or nesting
/// beyond 32 records (#977).
pub fn parameter_value_to_casacore(value: &ParameterValue) -> Result<Value, &'static str> {
    fn convert(value: &ParameterValue, depth: usize) -> Result<Value, &'static str> {
        if depth > 32 {
            return Err("record nesting is too deep (#977)");
        }
        Ok(match value {
            ParameterValue::Bool(value) => Value::Scalar(ScalarValue::Bool(*value)),
            ParameterValue::Integer(value) => Value::Scalar(ScalarValue::Int32(
                i32::try_from(*value).map_err(|_| "record integer is out of range (#977)")?,
            )),
            ParameterValue::Float(value) if value.is_finite() => {
                Value::Scalar(ScalarValue::Float64(*value))
            }
            ParameterValue::String(value) if !value.contains('\0') => {
                Value::Scalar(ScalarValue::String(value.clone()))
            }
            ParameterValue::Array(values) => {
                let array = parameter_array_to_casa(values)?;
                // CASA's record conversion flattens string matrices in
                // column-major order, while standalone string arrays retain
                // their two-dimensional shape.
                match array {
                    ArrayValue::String(values) if depth > 0 && values.ndim() == 2 => {
                        Value::Array(ArrayValue::from_string_vec(
                            values.view().reversed_axes().iter().cloned().collect(),
                        ))
                    }
                    array => Value::Array(array),
                }
            }
            ParameterValue::Table(fields) => Value::Record(RecordValue::new(
                fields
                    .iter()
                    .map(|(key, value)| {
                        if key.contains('\0') {
                            return Err("record field contains NUL (#977)");
                        }
                        Ok(RecordField::new(key, convert(value, depth + 1)?))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            _ => return Err("record value is unsupported (#977)"),
        })
    }
    convert(value, 0)
}

/// Convert supported casacore metadata values to CASA task return values.
///
/// # Errors
///
/// Returns an error for unsupported stored types or nesting beyond 32 records.
pub fn casacore_value_to_casa(value: &Value) -> Result<CasaValue, &'static str> {
    fn convert(value: &Value, depth: usize) -> Result<CasaValue, &'static str> {
        if depth > 32 {
            return Err("record nesting is too deep (#977)");
        }
        Ok(match value {
            Value::Scalar(ScalarValue::Bool(value)) => CasaValue::Bool { value: *value },
            Value::Scalar(ScalarValue::UInt8(value)) => CasaValue::Int {
                value: i64::from(*value),
            },
            Value::Scalar(ScalarValue::UInt32(value)) => CasaValue::Int {
                value: i64::from(*value),
            },
            Value::Scalar(ScalarValue::Int16(value)) => CasaValue::Int {
                value: i64::from(*value),
            },
            Value::Scalar(ScalarValue::Int32(value)) => CasaValue::Int {
                value: i64::from(*value),
            },
            Value::Scalar(ScalarValue::Int64(value)) => CasaValue::Int { value: *value },
            Value::Scalar(ScalarValue::Float32(value)) => CasaValue::float(f64::from(*value)),
            Value::Scalar(ScalarValue::Float64(value)) => CasaValue::float(*value),
            Value::Scalar(ScalarValue::String(value)) => CasaValue::string(value),
            Value::Array(ArrayValue::Bool(values)) => CasaValue::BoolArray {
                shape: values.shape().to_vec(),
                data: values.view().reversed_axes().iter().copied().collect(),
            },
            Value::Array(ArrayValue::Int32(values)) => CasaValue::IntArray {
                dtype: IntDtype::Int64,
                shape: values.shape().to_vec(),
                data: values
                    .view()
                    .reversed_axes()
                    .iter()
                    .copied()
                    .map(i64::from)
                    .collect(),
            },
            Value::Array(ArrayValue::Int64(values)) => CasaValue::IntArray {
                dtype: IntDtype::Int64,
                shape: values.shape().to_vec(),
                data: values.view().reversed_axes().iter().copied().collect(),
            },
            Value::Array(ArrayValue::Float32(values)) => CasaValue::FloatArray {
                dtype: FloatDtype::Float64,
                shape: values.shape().to_vec(),
                data: values
                    .view()
                    .reversed_axes()
                    .iter()
                    .copied()
                    .map(f64::from)
                    .map(CasaFloat)
                    .collect(),
            },
            Value::Array(ArrayValue::Float64(values)) => CasaValue::FloatArray {
                dtype: FloatDtype::Float64,
                shape: values.shape().to_vec(),
                data: values
                    .view()
                    .reversed_axes()
                    .iter()
                    .copied()
                    .map(CasaFloat)
                    .collect(),
            },
            Value::Array(ArrayValue::Complex32(values)) => CasaValue::ComplexArray {
                dtype: ComplexDtype::Complex128,
                shape: values.shape().to_vec(),
                data: values
                    .view()
                    .reversed_axes()
                    .iter()
                    .map(|value| {
                        [
                            CasaFloat(f64::from(value.re)),
                            CasaFloat(f64::from(value.im)),
                        ]
                    })
                    .collect(),
            },
            Value::Array(ArrayValue::Complex64(values)) => CasaValue::ComplexArray {
                dtype: ComplexDtype::Complex128,
                shape: values.shape().to_vec(),
                data: values
                    .view()
                    .reversed_axes()
                    .iter()
                    .map(|value| [CasaFloat(value.re), CasaFloat(value.im)])
                    .collect(),
            },
            Value::Array(ArrayValue::String(values)) => CasaValue::StrArray {
                shape: values.shape().to_vec(),
                data: values.view().reversed_axes().iter().cloned().collect(),
            },
            Value::Record(record) => CasaValue::dict(
                record
                    .fields()
                    .iter()
                    .map(|field| Ok((field.name.clone(), convert(&field.value, depth + 1)?)))
                    .collect::<Result<Vec<_>, &'static str>>()?,
            ),
            _ => return Err("stored record value is unsupported (#977)"),
        })
    }
    convert(value, 0)
}

/// Convert a flat array or rectangular matrix into a casacore array, using CASA's numeric
/// promotion from booleans to 32-bit integers to doubles. Empty arrays have
/// boolean element type, as in CASA's Python variant conversion.
///
/// # Errors
///
/// Returns an error for higher-dimensional, empty-row, ragged or mixed-string
/// arrays, non-finite values, or
/// integer-only values outside the supported 32-bit range (#977).
pub fn parameter_array_to_casa(values: &[ParameterValue]) -> Result<ArrayValue, &'static str> {
    if values
        .iter()
        .any(|value| matches!(value, ParameterValue::Array(_)))
    {
        let mut flat = Vec::new();
        let mut columns = None;
        for row in values {
            let ParameterValue::Array(row) = row else {
                return Err("ragged arrays are unsupported (#977)");
            };
            if row.is_empty() || columns.is_some_and(|n| n != row.len()) {
                return Err("empty-row or ragged arrays are unsupported (#977)");
            }
            columns = Some(row.len());
            if row
                .iter()
                .any(|value| matches!(value, ParameterValue::Array(_)))
            {
                return Err("arrays with more than two dimensions are unsupported (#977)");
            }
            flat.extend_from_slice(row);
        }
        let shape = vec![values.len(), columns.expect("nonempty matrix")];
        return match parameter_array_to_casa(&flat)? {
            ArrayValue::Bool(v) => Ok(ArrayValue::Bool(
                v.into_shape_with_order(shape).expect("rectangular matrix"),
            )),
            ArrayValue::Int32(v) => Ok(ArrayValue::Int32(
                v.into_shape_with_order(shape).expect("rectangular matrix"),
            )),
            ArrayValue::Float64(v) => Ok(ArrayValue::Float64(
                v.into_shape_with_order(shape).expect("rectangular matrix"),
            )),
            ArrayValue::String(v) => Ok(ArrayValue::String(
                v.into_shape_with_order(shape).expect("rectangular matrix"),
            )),
            _ => unreachable!("parameter arrays use supported primitive types"),
        };
    }
    let mut rank = 0u8;
    for value in values {
        rank = rank.max(match value {
            ParameterValue::Bool(_) => 0,
            ParameterValue::Integer(_) => 1,
            ParameterValue::Float(value) if value.is_finite() => 2,
            ParameterValue::String(value) if !value.contains('\0') => 3,
            _ => return Err("array element is not supported (#977)"),
        });
    }
    match rank {
        0 => Ok(ArrayValue::from_bool_vec(
            values
                .iter()
                .map(|value| match value {
                    ParameterValue::Bool(value) => *value,
                    _ => unreachable!(),
                })
                .collect(),
        )),
        1 => Ok(ArrayValue::from_i32_vec(
            values
                .iter()
                .map(|value| match value {
                    ParameterValue::Bool(value) => Ok(i32::from(*value)),
                    ParameterValue::Integer(value) => {
                        i32::try_from(*value).map_err(|_| "array integer is out of range (#977)")
                    }
                    _ => unreachable!(),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        2 => Ok(ArrayValue::from_f64_vec(
            values
                .iter()
                .map(|value| match value {
                    ParameterValue::Bool(value) => f64::from(u8::from(*value)),
                    ParameterValue::Integer(value) => *value as f64,
                    ParameterValue::Float(value) => *value,
                    _ => unreachable!(),
                })
                .collect(),
        )),
        _ => {
            let strings = values
                .iter()
                .map(|value| match value {
                    ParameterValue::String(value) => Ok(value.clone()),
                    _ => Err("mixed string arrays are not supported (#977)"),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ArrayValue::from_string_vec(strings))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arrays_preserve_types_and_promote_numbers() {
        use ParameterValue::{Bool, Float, Integer, String as Text};
        assert!(matches!(
            parameter_array_to_casa(&[]),
            Ok(ArrayValue::Bool(_))
        ));
        assert_eq!(
            parameter_array_to_casa(&[Bool(true), Integer(2)]).unwrap(),
            ArrayValue::from_i32_vec(vec![1, 2])
        );
        assert_eq!(
            parameter_array_to_casa(&[Integer(1), Float(2.5)]).unwrap(),
            ArrayValue::from_f64_vec(vec![1.0, 2.5])
        );
        assert_eq!(
            parameter_array_to_casa(&[Integer(2_147_483_648), Float(1.0)]).unwrap(),
            ArrayValue::from_f64_vec(vec![2_147_483_648.0, 1.0])
        );
        assert_eq!(
            parameter_array_to_casa(&[Text("x".into())]).unwrap(),
            ArrayValue::from_string_vec(vec!["x".into()])
        );
        assert!(parameter_array_to_casa(&[Integer(i64::MAX)]).is_err());
        assert!(parameter_array_to_casa(&[Integer(1), Text("x".into())]).is_err());
    }

    #[test]
    fn rectangular_matrices_preserve_shape_and_reject_ragged_or_empty_rows() {
        use ParameterValue::{Array, Integer};
        let matrix = [
            Array(vec![Integer(1), Integer(2)]),
            Array(vec![Integer(3), Integer(4)]),
        ];
        let converted = parameter_array_to_casa(&matrix).unwrap();
        assert_eq!(converted.shape(), &[2, 2]);
        assert_eq!(
            casacore_value_to_casa(&Value::Array(converted)).unwrap(),
            CasaValue::IntArray {
                dtype: IntDtype::Int64,
                shape: vec![2, 2],
                data: vec![1, 3, 2, 4],
            }
        );
        assert!(parameter_array_to_casa(&[Array(vec![])]).is_err());
        assert!(
            parameter_array_to_casa(&[
                Array(vec![Integer(1)]),
                Array(vec![Integer(1), Integer(2)])
            ])
            .is_err()
        );
        assert!(parameter_array_to_casa(&[Array(vec![Array(vec![Integer(1)])])]).is_err());
    }

    #[test]
    fn record_arrays_use_fortran_order_and_casa_numeric_promotions() {
        let array = casa_types::ArrayD::from_shape_vec(vec![2, 3], vec![1_i32, 2, 3, 4, 5, 6])
            .unwrap()
            .reversed_axes();
        let value = casacore_value_to_casa(&Value::Array(ArrayValue::Int32(array))).unwrap();
        assert_eq!(
            value,
            CasaValue::IntArray {
                dtype: IntDtype::Int64,
                shape: vec![3, 2],
                data: vec![1, 2, 3, 4, 5, 6]
            }
        );
        let floats = Value::Array(ArrayValue::from_f32_vec(vec![1.25, 2.5]));
        assert_eq!(
            casacore_value_to_casa(&floats).unwrap(),
            CasaValue::FloatArray {
                dtype: FloatDtype::Float64,
                shape: vec![2],
                data: vec![CasaFloat(1.25), CasaFloat(2.5)]
            }
        );
        let complex = Value::Array(ArrayValue::from_complex32_vec(vec![
            casa_types::Complex32::new(1.5, -2.0),
        ]));
        assert_eq!(
            casacore_value_to_casa(&complex).unwrap(),
            CasaValue::ComplexArray {
                dtype: ComplexDtype::Complex128,
                shape: vec![1],
                data: vec![[CasaFloat(1.5), CasaFloat(-2.0)]]
            }
        );
        let unsigned = Value::Array(ArrayValue::from_u32_vec(vec![1, 2]));
        assert!(
            casacore_value_to_casa(&unsigned)
                .unwrap_err()
                .contains("#977")
        );
    }
}
