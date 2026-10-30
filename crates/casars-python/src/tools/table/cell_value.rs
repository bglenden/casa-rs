// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python values as the cells of a column, the way casatools' `putcell` and
//! `putcol` coerce them (`tools/casa-compat/table_tool_oracle.py`, the
//! `cells_write` and `ms_cells_write` scenarios, CASA 6.7.6.14).
//!
//! A value is converted to the column's declared type, silently where
//! casatools does: a float into an integer column (truncated), an integer
//! into a floating-point or bool column, a real value into a complex one.
//! What it will not convert (text for a number, a number for text, a
//! non-record for a record) is refused with casatools' wording.

use casa_types::{ArrayValue, PrimitiveType, ScalarValue, Value};
use num_complex::{Complex32, Complex64};

use super::keyword_value::{Data, Incoming, Wide, build};
use super::state::Fail;

/// casatools' name for what a `ValueHolder` is asked to be.
fn holder_target(ty: Option<PrimitiveType>) -> &'static str {
    match ty {
        None => "Record",
        Some(PrimitiveType::Bool) => "Bool",
        Some(PrimitiveType::UInt8) => "uChar",
        Some(PrimitiveType::UInt16) => "uShort",
        Some(PrimitiveType::UInt32) => "uInt",
        Some(PrimitiveType::Int16) => "Short",
        Some(PrimitiveType::Int32) => "Int",
        Some(PrimitiveType::Int64) => "Int64",
        Some(PrimitiveType::Float32) => "Float",
        Some(PrimitiveType::Float64) => "Double",
        Some(PrimitiveType::Complex32) => "Complex",
        Some(PrimitiveType::Complex64) => "DComplex",
        Some(PrimitiveType::String) => "String",
    }
}

/// casatools' name for what a Python value is to a `ValueHolder`.
fn holder_source(data: &Data) -> &'static str {
    match data {
        Data::Bools(_) => "Bool",
        Data::Ints(_) => "Int",
        Data::Floats(..) => "Double",
        Data::Complexes(..) => "DComplex",
        Data::Texts(_) => "String",
    }
}

fn refusal(ty: Option<PrimitiveType>, data: &Data) -> Fail {
    Fail::Runtime(format!(
        "ValueHolderRep::as{} - invalid data type {}",
        holder_target(ty),
        holder_source(data)
    ))
}

fn out_of_range(ty: PrimitiveType) -> Fail {
    Fail::Runtime(format!(
        "a value is out of range for a column of {}",
        holder_target(Some(ty))
    ))
}

/// The values as integers of the column's type.
fn integers<T: TryFrom<i64>>(data: &Data, ty: PrimitiveType) -> Result<Vec<T>, Fail> {
    let wide: Vec<i64> = match data {
        Data::Ints(values) => values.clone(),
        Data::Floats(values, _) => values.iter().map(|value| *value as i64).collect(),
        Data::Bools(values) => values.iter().map(|flag| i64::from(*flag)).collect(),
        _ => return Err(refusal(Some(ty), data)),
    };
    wide.into_iter()
        .map(|value| T::try_from(value).map_err(|_| out_of_range(ty)))
        .collect()
}

/// The values as reals.
fn reals(data: &Data, ty: PrimitiveType) -> Result<Vec<f64>, Fail> {
    match data {
        Data::Ints(values) => Ok(values.iter().map(|value| *value as f64).collect()),
        Data::Floats(values, _) => Ok(values.clone()),
        Data::Bools(values) => Ok(values
            .iter()
            .map(|flag| f64::from(u8::from(*flag)))
            .collect()),
        _ => Err(refusal(Some(ty), data)),
    }
}

/// The values as complex numbers.
fn complexes(data: &Data, ty: PrimitiveType) -> Result<Vec<Complex64>, Fail> {
    match data {
        Data::Complexes(values, _) => Ok(values.clone()),
        _ => Ok(reals(data, ty)?
            .into_iter()
            .map(|value| Complex64::new(value, 0.0))
            .collect()),
    }
}

impl Wide {
    /// The value as a cell of a column of `ty`: a scalar, or an array of the
    /// value's shape (a scalar is an array of one element) when `array`.
    pub(super) fn into_cell(self, ty: PrimitiveType, array: bool) -> Result<Value, Fail> {
        let one = matches!(&self.shape, Some(shape) if shape.iter().product::<usize>() == 1);
        let shape = match (array, self.shape.clone()) {
            (false, None) => None,
            (false, Some(_)) if one => None,
            (false, Some(_)) => {
                return Err(Fail::Runtime(
                    "ScalarColumn::put: Table conformance error (an array for a scalar cell)"
                        .into(),
                ));
            }
            (true, Some(shape)) => Some(shape),
            (true, None) => Some(vec![1]),
        };
        let data = &self.data;
        macro_rules! whole {
            ($ty:ty, $scalar:ident, $array:ident) => {
                build(
                    shape,
                    integers::<$ty>(data, ty)?,
                    ScalarValue::$scalar,
                    ArrayValue::$array,
                )
            };
        }
        match ty {
            PrimitiveType::Bool => match data {
                Data::Bools(values) => {
                    build(shape, values.clone(), ScalarValue::Bool, ArrayValue::Bool)
                }
                Data::Ints(values) => {
                    let flags = values.iter().map(|value| *value != 0).collect();
                    build(shape, flags, ScalarValue::Bool, ArrayValue::Bool)
                }
                _ => Err(refusal(Some(ty), data)),
            },
            PrimitiveType::UInt8 => whole!(u8, UInt8, UInt8),
            PrimitiveType::UInt16 => whole!(u16, UInt16, UInt16),
            PrimitiveType::UInt32 => whole!(u32, UInt32, UInt32),
            PrimitiveType::Int16 => whole!(i16, Int16, Int16),
            PrimitiveType::Int32 => whole!(i32, Int32, Int32),
            PrimitiveType::Int64 => whole!(i64, Int64, Int64),
            PrimitiveType::Float32 => {
                let values = reals(data, ty)?
                    .into_iter()
                    .map(|value| value as f32)
                    .collect();
                build(shape, values, ScalarValue::Float32, ArrayValue::Float32)
            }
            PrimitiveType::Float64 => build(
                shape,
                reals(data, ty)?,
                ScalarValue::Float64,
                ArrayValue::Float64,
            ),
            PrimitiveType::Complex32 => {
                let values = complexes(data, ty)?
                    .into_iter()
                    .map(|value| Complex32::new(value.re as f32, value.im as f32))
                    .collect();
                build(shape, values, ScalarValue::Complex32, ArrayValue::Complex32)
            }
            PrimitiveType::Complex64 => build(
                shape,
                complexes(data, ty)?,
                ScalarValue::Complex64,
                ArrayValue::Complex64,
            ),
            PrimitiveType::String => match data {
                Data::Texts(values) => build(
                    shape,
                    values.clone(),
                    ScalarValue::String,
                    ArrayValue::String,
                ),
                _ => Err(refusal(Some(ty), data)),
            },
        }
    }
}

impl Incoming {
    /// The value as a cell of a column: of `ty` (`None`: a record column),
    /// an array cell or a scalar one.
    pub(super) fn into_cell(self, ty: Option<PrimitiveType>, array: bool) -> Result<Value, Fail> {
        match (self, ty) {
            (Self::Wide(wide), Some(ty)) => wide.into_cell(ty, array),
            (Self::Record(fields), None) => Self::Record(fields).natural(),
            (Self::Wide(wide), None) => Err(refusal(None, &wide.data)),
            (Self::Record(_), Some(ty)) => Err(Fail::Runtime(format!(
                "ValueHolderRep::as{} - invalid data type Record",
                holder_target(Some(ty))
            ))),
        }
    }
}
