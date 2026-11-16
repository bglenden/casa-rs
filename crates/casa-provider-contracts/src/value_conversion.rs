// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversions between parameter values and casacore's typed values.

use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

mod multidimensional;

/// Convert a Python variant integer as CASA does: retain its low 32 bits.
///
/// CASA's scalar and integer-array variant conversions use signed 32-bit
/// storage, including wraparound for values outside that range.
pub fn casa_variant_integer(value: i64) -> i32 {
    value as i32
}

use crate::{
    CasaFloat, CasaValue, ComplexDtype, EmptyArrayType, FloatDtype, IntDtype, NumpyScalarType,
    ParameterValue,
};

/// Convert a top-level NumPy scalar with CASA's task-parameter rules.
///
/// # Errors
/// Returns an error for scalar dtypes CASA does not accept at the top level.
pub fn casa_numpy_scalar_root(value: &ParameterValue) -> Result<ParameterValue, &'static str> {
    match value {
        ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Int32 | NumpyScalarType::Float32,
            value,
        } => Ok(value.as_ref().clone()),
        ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Int64,
            value,
        } => match value.as_ref() {
            ParameterValue::Integer(value) => Ok(ParameterValue::Float(*value as f64)),
            _ => Err("NumPy scalar dtype and value do not match (#977)"),
        },
        ParameterValue::NumpyScalar { .. } => {
            Err("NumPy scalar dtype is not accepted by CASA at the top level")
        }
        _ => Ok(value.clone()),
    }
}

/// Convert a zero-dimensional NumPy array as CASA's variant bridge does.
/// Numeric values become empty arrays; a string becomes a one-element array.
///
/// # Errors
/// Returns an error if the supplied value is not a supported scalar.
pub fn scalar_parameter_array(value: &ParameterValue) -> Result<ArrayValue, &'static str> {
    Ok(match value {
        ParameterValue::Bool(_) => ArrayValue::from_bool_vec(vec![]),
        ParameterValue::Integer(_) => ArrayValue::from_i32_vec(vec![]),
        ParameterValue::Float(_) => ArrayValue::from_f64_vec(vec![]),
        ParameterValue::Complex(_) => ArrayValue::from_complex64_vec(vec![]),
        ParameterValue::String(value) if !value.contains('\0') => {
            ArrayValue::from_string_vec(vec![value.clone()])
        }
        _ => return Err("zero-dimensional array must contain a supported scalar (#977)"),
    })
}

/// Construct an empty casacore array while retaining its shape and type.
///
/// # Errors
///
/// Returns an error when the shape is invalid or cannot be represented.
pub fn empty_parameter_array(
    dtype: EmptyArrayType,
    shape: &[usize],
) -> Result<ArrayValue, &'static str> {
    if shape.is_empty()
        || shape.len() > 32
        || !shape.contains(&0)
        || shape.iter().any(|&size| i32::try_from(size).is_err())
    {
        return Err("invalid empty array shape (#977)");
    }
    macro_rules! reshape {
        ($array:expr, $variant:ident) => {
            match $array {
                ArrayValue::$variant(values) => Ok(ArrayValue::$variant(
                    values
                        .into_shape_with_order(shape.to_vec())
                        .map_err(|_| "empty array shape is too large (#977)")?,
                )),
                _ => unreachable!(),
            }
        };
    }
    match dtype {
        EmptyArrayType::Bool => reshape!(ArrayValue::from_bool_vec(vec![]), Bool),
        EmptyArrayType::Integer => reshape!(ArrayValue::from_i32_vec(vec![]), Int32),
        EmptyArrayType::Float => reshape!(ArrayValue::from_f64_vec(vec![]), Float64),
        EmptyArrayType::Complex => reshape!(ArrayValue::from_complex64_vec(vec![]), Complex64),
        EmptyArrayType::String => reshape!(ArrayValue::from_string_vec(vec![]), String),
    }
}

/// Convert parameter data to casacore metadata values, including records.
///
/// Record keys use the parameter model's sorted order. Array conversion
/// follows [`parameter_array_to_casa`].
///
/// # Errors
///
/// Returns an error for unsupported values or nesting beyond 32 records (#977).
pub fn parameter_value_to_casacore(value: &ParameterValue) -> Result<Value, &'static str> {
    fn convert(value: &ParameterValue, depth: usize) -> Result<Value, &'static str> {
        if depth > 32 {
            return Err("record nesting is too deep (#977)");
        }
        Ok(match value {
            ParameterValue::Bool(value) => Value::Scalar(ScalarValue::Bool(*value)),
            ParameterValue::Integer(value) => {
                Value::Scalar(ScalarValue::Int32(casa_variant_integer(*value)))
            }
            ParameterValue::Float(value) => Value::Scalar(ScalarValue::Float64(*value)),
            ParameterValue::Complex([real, imag]) => Value::Scalar(ScalarValue::Complex64(
                casa_types::Complex64::new(real.0, imag.0),
            )),
            ParameterValue::EmptyArray { dtype, shape } => {
                let array = empty_parameter_array(*dtype, shape)?;
                match array {
                    ArrayValue::String(values) if depth > 0 && values.ndim() > 1 => {
                        Value::Array(ArrayValue::from_string_vec(vec![]))
                    }
                    array => Value::Array(array),
                }
            }
            ParameterValue::ScalarArray(value) => Value::Array(scalar_parameter_array(value)?),
            ParameterValue::NumpyScalar { dtype: NumpyScalarType::Complex128, value } => convert(value, depth)?,
            ParameterValue::String(value) if !value.contains('\0') => {
                Value::Scalar(ScalarValue::String(value.clone()))
            }
            ParameterValue::Array(values) => {
                let array = parameter_array_to_casa(values)?;
                // CASA's record conversion flattens string matrices in
                // column-major order, while standalone string arrays retain
                // their two-dimensional shape.
                match array {
                    ArrayValue::String(values) if depth > 0 && values.ndim() > 1 => {
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
                    .filter(|(_, value)| !matches!(value, ParameterValue::NumpyScalar { dtype, .. } if *dtype != NumpyScalarType::Complex128))
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
            Value::Scalar(ScalarValue::Complex32(value)) => CasaValue::Complex {
                real: CasaFloat(f64::from(value.re)),
                imag: CasaFloat(f64::from(value.im)),
            },
            Value::Scalar(ScalarValue::Complex64(value)) => CasaValue::Complex {
                real: CasaFloat(value.re),
                imag: CasaFloat(value.im),
            },
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
            Value::Array(ArrayValue::UInt32(values)) => CasaValue::UInt64Array {
                shape: values.shape().to_vec(),
                data: values
                    .view()
                    .reversed_axes()
                    .iter()
                    .copied()
                    .map(u64::from)
                    .collect(),
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

fn array_parameter_element(value: &ParameterValue) -> Option<ParameterValue> {
    match value {
        ParameterValue::ScalarArray(_) => None,
        ParameterValue::NumpyScalar { dtype, value } => match dtype {
            NumpyScalarType::Int8
            | NumpyScalarType::Int16
            | NumpyScalarType::Int32
            | NumpyScalarType::Int64
            | NumpyScalarType::Float32
            | NumpyScalarType::Complex128 => Some(value.as_ref().clone()),
            _ => None,
        },
        value => Some(value.clone()),
    }
}

fn casa_array_string(value: &ParameterValue) -> String {
    let float = |value: f64| {
        if value.is_nan() {
            if value.is_sign_negative() {
                "-nan".to_string()
            } else {
                "nan".to_string()
            }
        } else {
            format!("{value:.6}")
        }
    };
    match value {
        ParameterValue::Bool(value) => value.to_string(),
        ParameterValue::Integer(value) => value.to_string(),
        ParameterValue::Float(value) => float(*value),
        ParameterValue::Complex([real, imag]) => format!("({},{})", float(real.0), float(imag.0)),
        ParameterValue::String(value) => value.clone(),
        _ => unreachable!("only scalar array elements are formatted"),
    }
}

/// Convert a rectangular parameter array into a casacore array, using CASA's numeric
/// promotion from booleans to 32-bit integers to doubles and complex values. Empty arrays have
/// boolean element type, as in CASA's Python variant conversion.
///
/// # Errors
///
/// Returns an error for empty nested dimensions, ragged or deeply nested
/// arrays (#977).
pub fn parameter_array_to_casa(values: &[ParameterValue]) -> Result<ArrayValue, &'static str> {
    if values
        .iter()
        .any(|value| matches!(value, ParameterValue::Array(_)))
    {
        fn flatten(
            values: &[ParameterValue],
            depth: usize,
            shape: &mut Vec<usize>,
            leaf_depth: &mut Option<usize>,
            out: &mut Vec<ParameterValue>,
        ) -> Result<(), &'static str> {
            if depth >= 32 {
                return Err("array nesting is too deep (#977)");
            }
            if values.is_empty() {
                return Err("empty nested array dimensions are unsupported (#977)");
            }
            if let Some(expected) = shape.get(depth) {
                if *expected != values.len() {
                    return Err("ragged arrays are unsupported (#977)");
                }
            } else {
                shape.push(values.len());
            }
            for value in values {
                match value {
                    ParameterValue::Array(nested) => {
                        flatten(nested, depth + 1, shape, leaf_depth, out)?
                    }
                    value => {
                        if leaf_depth.is_some_and(|expected| expected != depth + 1) {
                            return Err("ragged arrays are unsupported (#977)");
                        }
                        *leaf_depth = Some(depth + 1);
                        out.push(value.clone());
                    }
                }
            }
            Ok(())
        }
        let mut flat = Vec::new();
        let mut shape = Vec::new();
        flatten(values, 0, &mut shape, &mut None, &mut flat)?;
        let normalized = multidimensional::convert(values, &shape)?;
        let converted = parameter_array_to_casa(&normalized)?;
        return match converted {
            ArrayValue::Bool(v) => Ok(ArrayValue::Bool(
                v.into_shape_with_order(shape).expect("rectangular array"),
            )),
            ArrayValue::Int32(v) => Ok(ArrayValue::Int32(
                v.into_shape_with_order(shape).expect("rectangular array"),
            )),
            ArrayValue::Float64(v) => Ok(ArrayValue::Float64(
                v.into_shape_with_order(shape).expect("rectangular array"),
            )),
            ArrayValue::Complex64(v) => Ok(ArrayValue::Complex64(
                v.into_shape_with_order(shape).expect("rectangular array"),
            )),
            ArrayValue::String(v) => Ok(ArrayValue::String(
                v.into_shape_with_order(shape).expect("rectangular array"),
            )),
            _ => unreachable!("parameter arrays use supported primitive types"),
        };
    }
    let normalized = multidimensional::flat(values)?;
    let values = normalized.as_slice();
    let mut rank = 0u8;
    for value in values {
        rank = rank.max(match value {
            ParameterValue::Bool(_) => 0,
            ParameterValue::Integer(_) => 1,
            ParameterValue::Float(_) => 2,
            ParameterValue::Complex(_) => 3,
            ParameterValue::String(value) if !value.contains('\0') => 4,
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
                    ParameterValue::Bool(value) => i32::from(*value),
                    ParameterValue::Integer(value) => casa_variant_integer(*value),
                    _ => unreachable!(),
                })
                .collect(),
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
        3 => Ok(ArrayValue::from_complex64_vec(
            values
                .iter()
                .map(|value| match value {
                    ParameterValue::Bool(value) => {
                        casa_types::Complex64::new(f64::from(u8::from(*value)), 0.0)
                    }
                    ParameterValue::Integer(value) => {
                        casa_types::Complex64::new(*value as f64, 0.0)
                    }
                    ParameterValue::Float(value) => casa_types::Complex64::new(*value, 0.0),
                    ParameterValue::Complex([real, imag]) => {
                        casa_types::Complex64::new(real.0, imag.0)
                    }
                    _ => unreachable!(),
                })
                .collect(),
        )),
        _ => {
            // casatools converts scalar elements to strings when any element
            // is a string, using C++'s lower-case booleans and fixed six
            // decimal places for doubles.
            let strings = values.iter().map(casa_array_string).collect();
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
        assert_eq!(
            parameter_array_to_casa(&[Integer(i64::MAX)]).unwrap(),
            ArrayValue::from_i32_vec(vec![-1])
        );
        assert_eq!(
            parameter_array_to_casa(&[Integer(1), Text("x".into())]).unwrap(),
            ArrayValue::from_string_vec(vec!["1".into(), "x".into()])
        );
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
        let cube = [Array(vec![Array(vec![Integer(1), Integer(2)])])];
        assert_eq!(parameter_array_to_casa(&cube).unwrap().shape(), &[1, 1, 2]);
        assert!(
            parameter_array_to_casa(&[Array(vec![Integer(1), Array(vec![Integer(2)])])]).is_err()
        );
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
        let unsigned = Value::Array(ArrayValue::from_u32_vec(vec![1, u32::MAX]));
        assert_eq!(
            casacore_value_to_casa(&unsigned).unwrap(),
            CasaValue::UInt64Array {
                shape: vec![2],
                data: vec![1, u64::from(u32::MAX)],
            }
        );
        let unsigned_matrix =
            casa_types::ArrayD::from_shape_vec(vec![2, 3], vec![1_u32, 2, 3, 4, 5, u32::MAX])
                .unwrap();
        assert_eq!(
            casacore_value_to_casa(&Value::Array(ArrayValue::UInt32(unsigned_matrix))).unwrap(),
            CasaValue::UInt64Array {
                shape: vec![2, 3],
                data: vec![1, 4, 2, 5, 3, u64::from(u32::MAX)],
            }
        );
    }

    #[test]
    fn record_complex_scalars_preserve_components_and_promote_float32() {
        let single = Value::Scalar(ScalarValue::Complex32(casa_types::Complex32::new(
            1.25, -2.5,
        )));
        assert_eq!(
            casacore_value_to_casa(&single).unwrap(),
            CasaValue::Complex {
                real: CasaFloat(1.25),
                imag: CasaFloat(-2.5),
            }
        );
        let double = Value::Scalar(ScalarValue::Complex64(casa_types::Complex64::new(
            f64::INFINITY,
            f64::NEG_INFINITY,
        )));
        let value = casacore_value_to_casa(&double).unwrap();
        let json = serde_json::to_value(&value).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"kind": "complex", "real": "inf", "imag": "-inf"})
        );
        assert_eq!(serde_json::from_value::<CasaValue>(json).unwrap(), value);
    }
}
