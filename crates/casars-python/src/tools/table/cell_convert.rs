// SPDX-License-Identifier: LGPL-3.0-or-later
//! The conversions `copyrows` makes between the cells of two columns of the
//! same name and different types, as casacore's `TableRow` makes them
//! (`RecordInterface::asInt`, `asDouble`, ... for a scalar cell and
//! `TableRecordRep::get_pointer` for an array cell). Which pairs CASA
//! 6.7.6.14 accepts was probed pair by pair (`copyrows_types` in
//! `tools/casa-compat/table_tool_oracle.py`); the rules differ for scalars
//! and arrays:
//!
//! - a Boolean goes to a Boolean only, and so does a string to a string;
//! - an integer goes to every numeric type, but to an `Int64` only from an
//!   `Int64` and not to a narrower integer than `Int` from one. CASA wraps
//!   a value that does not fit as a C cast does (Short -5 to uChar is 251);
//!   casa-rs refuses it, as it refuses an out-of-range `putcell`, rather than
//!   store a wrong number;
//! - a floating point number goes to the floating point and complex types,
//!   a complex number to the complex types;
//! - a scalar `Int` also goes to a Boolean (non-zero is true).

use casa_tables::ColumnType;
use casa_types::{ArrayValue, Complex32, Complex64, PrimitiveType, ScalarValue, Value};
use ndarray::ArrayD;

use super::state::Fail;

/// A number, held wide enough for every type a cell can be converted from.
enum Number {
    Integer(i128),
    Real(f64),
    Complex(f64, f64),
}

/// Whether a cell of type `from` can be put into a column of type `to`
/// (the types are different).
pub(super) fn convertible(column: &ColumnType, from: PrimitiveType, to: PrimitiveType) -> bool {
    use PrimitiveType as T;
    let scalar = matches!(column, ColumnType::Scalar);
    let wide = matches!(
        to,
        T::Int32 | T::UInt32 | T::Float32 | T::Float64 | T::Complex32 | T::Complex64
    );
    // The narrower integers, and an `Int64`, take a scalar only.
    let scalar_only = scalar && matches!(to, T::UInt8 | T::Int16 | T::Int64);
    match from {
        T::UInt8 => wide || to == T::Int16 || (scalar && to == T::Int64),
        T::Int16 => wide || to == T::Int16 || (scalar && matches!(to, T::UInt8 | T::Int64)),
        T::Int32 => wide || to == T::Bool || scalar_only,
        T::UInt32 => wide || scalar_only,
        T::Int64 => matches!(
            to,
            T::Int32 | T::UInt32 | T::Int64 | T::Float32 | T::Float64 | T::Complex32 | T::Complex64
        ),
        T::Float32 | T::Float64 => {
            matches!(to, T::Float32 | T::Float64 | T::Complex32 | T::Complex64)
        }
        T::Complex32 | T::Complex64 => matches!(to, T::Complex32 | T::Complex64),
        T::Bool | T::UInt16 | T::String => from == to,
    }
}

/// How CASA names a type in `RecordInterface::as...`.
fn type_name(to: PrimitiveType) -> &'static str {
    match to {
        PrimitiveType::Bool => "Bool",
        PrimitiveType::UInt8 => "uChar",
        PrimitiveType::UInt16 => "uShort",
        PrimitiveType::Int16 => "Short",
        PrimitiveType::Int32 => "Int",
        PrimitiveType::UInt32 => "uInt",
        PrimitiveType::Int64 => "Int64",
        PrimitiveType::Float32 => "Float",
        PrimitiveType::Float64 => "Double",
        PrimitiveType::Complex32 => "Complex",
        PrimitiveType::Complex64 => "DComplex",
        PrimitiveType::String => "String",
    }
}

/// What CASA raises when a cell of another type is put into a column of
/// type `to`.
pub(super) fn refusal(column: &ColumnType, name: &str, to: PrimitiveType) -> Fail {
    if matches!(column, ColumnType::Scalar) && to != PrimitiveType::String {
        Fail::Runtime(format!(
            "RecordInterface::as{} - invalid data type",
            type_name(to)
        ))
    } else {
        Fail::Runtime(format!(
            "TableRecordRep::get_pointer - incorrect data type used for field {name}"
        ))
    }
}

/// A cell holds a number the column's type cannot hold.
fn out_of_range(to: PrimitiveType) -> Fail {
    Fail::Runtime(format!(
        "a value is out of range for a column of {}",
        type_name(to)
    ))
}

/// `value` as a cell of type `to`, which [`convertible`] has allowed; an
/// integer that does not fit `to` is refused.
pub(super) fn convert(value: Value, to: PrimitiveType) -> Result<Value, Fail> {
    match value {
        Value::Scalar(scalar) => from_number(number_of(&scalar), to)
            .map(Value::Scalar)
            .ok_or_else(|| out_of_range(to)),
        Value::Array(array) => convert_array(&array, to).map(Value::Array),
        other => Ok(other),
    }
}

fn number_of(scalar: &ScalarValue) -> Number {
    match scalar {
        ScalarValue::Bool(v) => Number::Integer(i128::from(*v)),
        ScalarValue::UInt8(v) => Number::Integer(i128::from(*v)),
        ScalarValue::UInt16(v) => Number::Integer(i128::from(*v)),
        ScalarValue::UInt32(v) => Number::Integer(i128::from(*v)),
        ScalarValue::Int16(v) => Number::Integer(i128::from(*v)),
        ScalarValue::Int32(v) => Number::Integer(i128::from(*v)),
        ScalarValue::Int64(v) => Number::Integer(i128::from(*v)),
        ScalarValue::Float32(v) => Number::Real(f64::from(*v)),
        ScalarValue::Float64(v) => Number::Real(*v),
        ScalarValue::Complex32(v) => Number::Complex(f64::from(v.re), f64::from(v.im)),
        ScalarValue::Complex64(v) => Number::Complex(v.re, v.im),
        // A string is converted to a string only, which needs no conversion.
        ScalarValue::String(_) => Number::Integer(0),
    }
}

/// The scalar of type `to` for `number`, or `None` for an integer `to` cannot
/// hold.
#[allow(clippy::cast_possible_truncation)]
fn from_number(number: Number, to: PrimitiveType) -> Option<ScalarValue> {
    Some(match (number, to) {
        (Number::Integer(v), PrimitiveType::Bool) => ScalarValue::Bool(v != 0),
        (Number::Integer(v), PrimitiveType::UInt8) => ScalarValue::UInt8(v.try_into().ok()?),
        (Number::Integer(v), PrimitiveType::UInt16) => ScalarValue::UInt16(v.try_into().ok()?),
        (Number::Integer(v), PrimitiveType::UInt32) => ScalarValue::UInt32(v.try_into().ok()?),
        (Number::Integer(v), PrimitiveType::Int16) => ScalarValue::Int16(v.try_into().ok()?),
        (Number::Integer(v), PrimitiveType::Int32) => ScalarValue::Int32(v.try_into().ok()?),
        (Number::Integer(v), PrimitiveType::Int64) => ScalarValue::Int64(v.try_into().ok()?),
        (Number::Integer(v), PrimitiveType::Float32) => ScalarValue::Float32(v as f32),
        (Number::Integer(v), PrimitiveType::Float64) => ScalarValue::Float64(v as f64),
        (Number::Integer(v), PrimitiveType::Complex32) => {
            ScalarValue::Complex32(Complex32::new(v as f32, 0.0))
        }
        (Number::Integer(v), PrimitiveType::Complex64) => {
            ScalarValue::Complex64(Complex64::new(v as f64, 0.0))
        }
        (Number::Real(v), PrimitiveType::Float32) => ScalarValue::Float32(v as f32),
        (Number::Real(v), PrimitiveType::Float64) => ScalarValue::Float64(v),
        (Number::Real(v), PrimitiveType::Complex32) => {
            ScalarValue::Complex32(Complex32::new(v as f32, 0.0))
        }
        (Number::Real(v), PrimitiveType::Complex64) => {
            ScalarValue::Complex64(Complex64::new(v, 0.0))
        }
        (Number::Complex(re, im), PrimitiveType::Complex32) => {
            ScalarValue::Complex32(Complex32::new(re as f32, im as f32))
        }
        (Number::Complex(re, im), PrimitiveType::Complex64) => {
            ScalarValue::Complex64(Complex64::new(re, im))
        }
        _ => unreachable!("convertible() allows only the pairs handled above"),
    })
}

/// The numbers of `array` in storage order.
fn numbers_of(array: &ArrayValue) -> Vec<Number> {
    macro_rules! integers {
        ($a:expr) => {
            $a.iter().map(|v| Number::Integer(i128::from(*v))).collect()
        };
    }
    match array {
        ArrayValue::Bool(a) => integers!(a),
        ArrayValue::UInt8(a) => integers!(a),
        ArrayValue::UInt16(a) => integers!(a),
        ArrayValue::UInt32(a) => integers!(a),
        ArrayValue::Int16(a) => integers!(a),
        ArrayValue::Int32(a) => integers!(a),
        ArrayValue::Int64(a) => integers!(a),
        ArrayValue::Float32(a) => a.iter().map(|v| Number::Real(f64::from(*v))).collect(),
        ArrayValue::Float64(a) => a.iter().map(|v| Number::Real(*v)).collect(),
        ArrayValue::Complex32(a) => a
            .iter()
            .map(|v| Number::Complex(f64::from(v.re), f64::from(v.im)))
            .collect(),
        ArrayValue::Complex64(a) => a.iter().map(|v| Number::Complex(v.re, v.im)).collect(),
        ArrayValue::String(_) => Vec::new(),
    }
}

/// `array` with its elements converted to `to`, in the same shape.
fn convert_array(array: &ArrayValue, to: PrimitiveType) -> Result<ArrayValue, Fail> {
    let shape = array.shape().to_vec();
    let cells = numbers_of(array)
        .into_iter()
        .map(|number| from_number(number, to).ok_or_else(|| out_of_range(to)));
    macro_rules! collect {
        ($variant:ident) => {
            ArrayValue::$variant(
                ArrayD::from_shape_vec(
                    shape,
                    cells
                        .map(|cell| {
                            cell.map(|cell| match cell {
                                ScalarValue::$variant(v) => v,
                                _ => unreachable!("from_number gives the type asked for"),
                            })
                        })
                        .collect::<Result<Vec<_>, Fail>>()?,
                )
                .expect("the shape of the source holds as many elements"),
            )
        };
    }
    Ok(match to {
        PrimitiveType::Bool => collect!(Bool),
        PrimitiveType::UInt8 => collect!(UInt8),
        PrimitiveType::UInt16 => collect!(UInt16),
        PrimitiveType::UInt32 => collect!(UInt32),
        PrimitiveType::Int16 => collect!(Int16),
        PrimitiveType::Int32 => collect!(Int32),
        PrimitiveType::Int64 => collect!(Int64),
        PrimitiveType::Float32 => collect!(Float32),
        PrimitiveType::Float64 => collect!(Float64),
        PrimitiveType::Complex32 => collect!(Complex32),
        PrimitiveType::Complex64 => collect!(Complex64),
        PrimitiveType::String => unreachable!("convertible() sends a string to a string only"),
    })
}

#[cfg(test)]
mod tests {
    //! The conversions CASA 6.7.6.14 accepted, pair by pair, for a scalar and
    //! for an array cell (`probe` of `copyrows` between columns `v` of two
    //! types: `tools/casa-compat/table_tool_oracle.py`, `copyrows_types`).

    use super::*;
    use PrimitiveType as T;

    const TYPES: [PrimitiveType; 11] = [
        T::Bool,
        T::UInt8,
        T::Int16,
        T::Int32,
        T::UInt32,
        T::Int64,
        T::Float32,
        T::Float64,
        T::Complex32,
        T::Complex64,
        T::String,
    ];

    /// For each source type, the other types CASA accepted a scalar for.
    const SCALARS: [(PrimitiveType, &[PrimitiveType]); 11] = [
        (T::Bool, &[]),
        (
            T::UInt8,
            &[
                T::Int16,
                T::Int32,
                T::UInt32,
                T::Int64,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (
            T::Int16,
            &[
                T::UInt8,
                T::Int32,
                T::UInt32,
                T::Int64,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (
            T::Int32,
            &[
                T::Bool,
                T::UInt8,
                T::Int16,
                T::UInt32,
                T::Int64,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (
            T::UInt32,
            &[
                T::UInt8,
                T::Int16,
                T::Int32,
                T::Int64,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (
            T::Int64,
            &[
                T::Int32,
                T::UInt32,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (T::Float32, &[T::Float64, T::Complex32, T::Complex64]),
        (T::Float64, &[T::Float32, T::Complex32, T::Complex64]),
        (T::Complex32, &[T::Complex64]),
        (T::Complex64, &[T::Complex32]),
        (T::String, &[]),
    ];

    /// The same for an array cell: no narrower integer than `Int`, and no
    /// `Int64`.
    const ARRAYS: [(PrimitiveType, &[PrimitiveType]); 11] = [
        (T::Bool, &[]),
        (
            T::UInt8,
            &[
                T::Int16,
                T::Int32,
                T::UInt32,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (
            T::Int16,
            &[
                T::Int32,
                T::UInt32,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (
            T::Int32,
            &[
                T::Bool,
                T::UInt32,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (
            T::UInt32,
            &[T::Int32, T::Float32, T::Float64, T::Complex32, T::Complex64],
        ),
        (
            T::Int64,
            &[
                T::Int32,
                T::UInt32,
                T::Float32,
                T::Float64,
                T::Complex32,
                T::Complex64,
            ],
        ),
        (T::Float32, &[T::Float64, T::Complex32, T::Complex64]),
        (T::Float64, &[T::Float32, T::Complex32, T::Complex64]),
        (T::Complex32, &[T::Complex64]),
        (T::Complex64, &[T::Complex32]),
        (T::String, &[]),
    ];

    #[test]
    fn the_pairs_of_types_are_those_casa_accepted() {
        let array = ColumnType::Array(casa_tables::ArrayShapeContract::Fixed { shape: vec![2] });
        for (column, rows) in [(&ColumnType::Scalar, SCALARS), (&array, ARRAYS)] {
            for (from, accepted) in rows {
                for to in TYPES {
                    if from == to {
                        continue;
                    }
                    assert_eq!(
                        convertible(column, from, to),
                        accepted.contains(&to),
                        "{column:?} {from:?} -> {to:?}"
                    );
                }
            }
        }
    }

    fn scalar(value: ScalarValue, to: PrimitiveType) -> ScalarValue {
        match convert(Value::Scalar(value), to).expect("converts") {
            Value::Scalar(converted) => converted,
            other => panic!("not a scalar: {other:?}"),
        }
    }

    #[test]
    fn an_integer_that_does_not_fit_is_refused_where_casa_wraps_it() {
        // CASA 6.7.6.14 puts Short -5 into a uChar column as 251 and into a
        // uInt column as 4294967291.
        for (value, to) in [
            (ScalarValue::Int16(-5), T::UInt8),
            (ScalarValue::Int16(-5), T::UInt32),
            (ScalarValue::Int32(300), T::UInt8),
            (ScalarValue::UInt32(4_000_000_000), T::Int32),
            (ScalarValue::Int64(1 << 40), T::Int32),
        ] {
            let Err(Fail::Runtime(message)) = convert(Value::Scalar(value.clone()), to) else {
                panic!("{value:?} -> {to:?} was not refused");
            };
            assert!(
                message.starts_with("a value is out of range for a column of "),
                "{message}"
            );
        }
        let fits = ArrayD::from_shape_vec(vec![2], vec![1_i32, 70_000]).expect("array");
        assert!(convert(Value::Array(ArrayValue::Int32(fits)), T::UInt32).is_ok());
        let negative = ArrayD::from_shape_vec(vec![2], vec![1_i32, -1]).expect("array");
        assert!(convert(Value::Array(ArrayValue::Int32(negative)), T::UInt32).is_err());
    }

    #[test]
    fn an_integer_that_fits_converts_and_an_int_is_true_when_it_is_not_zero() {
        assert_eq!(
            scalar(ScalarValue::Int16(5), T::UInt8),
            ScalarValue::UInt8(5)
        );
        assert_eq!(
            scalar(ScalarValue::Int32(3), T::UInt8),
            ScalarValue::UInt8(3)
        );
        assert_eq!(
            scalar(ScalarValue::Int32(0), T::Bool),
            ScalarValue::Bool(false)
        );
        assert_eq!(
            scalar(ScalarValue::Int32(-2), T::Bool),
            ScalarValue::Bool(true)
        );
        assert_eq!(
            scalar(ScalarValue::Int64(6), T::Float64),
            ScalarValue::Float64(6.0)
        );
        assert_eq!(
            scalar(ScalarValue::Int64(6), T::Complex64),
            ScalarValue::Complex64(Complex64::new(6.0, 0.0))
        );
        assert_eq!(
            scalar(
                ScalarValue::Complex64(Complex64::new(3.0, -4.0)),
                T::Complex32
            ),
            ScalarValue::Complex32(Complex32::new(3.0, -4.0))
        );
        assert_eq!(
            scalar(ScalarValue::Float64(2.25), T::Float32),
            ScalarValue::Float32(2.25)
        );
    }

    #[test]
    fn an_array_keeps_its_shape_and_order() {
        let source = ArrayD::from_shape_vec(vec![2, 3], vec![1_i32, 2, 3, 4, 5, 6]).expect("array");
        let Ok(Value::Array(ArrayValue::Float64(converted))) =
            convert(Value::Array(ArrayValue::Int32(source)), T::Float64)
        else {
            panic!("not a double array");
        };
        assert_eq!(converted.shape(), &[2, 3]);
        assert_eq!(
            converted.iter().copied().collect::<Vec<_>>(),
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        );
    }

    #[test]
    fn a_refusal_names_what_casa_names() {
        let array = ColumnType::Array(casa_tables::ArrayShapeContract::Fixed { shape: vec![2] });
        let message = |fail: Fail| match fail {
            Fail::Runtime(message) => message,
            other => panic!("not a runtime error: {other:?}"),
        };
        assert_eq!(
            message(refusal(&ColumnType::Scalar, "v", T::UInt8)),
            "RecordInterface::asuChar - invalid data type"
        );
        assert_eq!(
            message(refusal(&ColumnType::Scalar, "v", T::Complex64)),
            "RecordInterface::asDComplex - invalid data type"
        );
        assert_eq!(
            message(refusal(&ColumnType::Scalar, "v", T::String)),
            "TableRecordRep::get_pointer - incorrect data type used for field v"
        );
        assert_eq!(
            message(refusal(&array, "v", T::Int32)),
            "TableRecordRep::get_pointer - incorrect data type used for field v"
        );
    }
}
