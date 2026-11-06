// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversions between parameter values and casacore's typed values.

use casa_types::ArrayValue;

use crate::ParameterValue;

/// Convert a flat parameter array into a casacore array, using CASA's numeric
/// promotion from booleans to 32-bit integers to doubles. Empty arrays have
/// boolean element type, as in CASA's Python variant conversion.
///
/// # Errors
///
/// Returns an error for nested or mixed string arrays, non-finite values, or
/// integer-only values outside the supported 32-bit range (#977).
pub fn parameter_array_to_casa(values: &[ParameterValue]) -> Result<ArrayValue, &'static str> {
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
}
