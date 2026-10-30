// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python values as table keywords, the way casatools stores them
//! (`tools/casa-compat/table_tool_oracle.py`, CASA 6.7.6.14).
//!
//! A value arrives in its widest form ([`Incoming`]): a `bool`, `int`,
//! `float`, `complex` or `str`, a NumPy array or scalar, a list (read as the
//! NumPy array it makes), or a `dict` for a sub-record. A new keyword takes
//! the natural type: an integer is a 32-bit `Int` (a 64-bit one only when a
//! value does not fit, where CASA truncates it to 0), a float a `Double`
//! (`Float` for a NumPy `float32`), a complex a `DComplex`.
//!
//! An existing keyword keeps its type, as casacore's `defineDataField` does:
//! the new value must be of the same kind (boolean, integer, real, complex,
//! string, record), scalar or array, and is stored in the keyword's own
//! width (a `float32` into a `Double` is fine; an integer that does not fit
//! an `Int` is refused, where CASA truncates it). A one-element array is
//! accepted for a scalar keyword.

use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};
use num_complex::{Complex32, Complex64};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyComplex, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use super::state::Fail;

/// The elements of a value, in the widest type of their kind.
#[derive(Debug, Clone)]
pub(super) enum Data {
    Bools(Vec<bool>),
    Ints(Vec<i64>),
    /// Reals, and whether they came as single precision.
    Floats(Vec<f64>, bool),
    /// Complex numbers, and whether they came as single precision.
    Complexes(Vec<Complex64>, bool),
    Texts(Vec<String>),
}

/// A scalar (`shape` is `None`) or an array of `shape` in C order.
#[derive(Debug, Clone)]
pub(super) struct Wide {
    pub(super) shape: Option<Vec<usize>>,
    pub(super) data: Data,
}

/// A keyword value as Python gave it.
#[derive(Debug, Clone)]
pub(super) enum Incoming {
    Wide(Wide),
    Record(Vec<(String, Incoming)>),
}

fn mismatch(name: &str) -> Fail {
    Fail::Runtime(format!(
        "TableRecordRep::defineDataField - incorrect data type used for field {name}"
    ))
}

fn unsupported(obj: &Bound<'_, PyAny>) -> PyErr {
    let kind = obj
        .get_type()
        .name()
        .map_or_else(|_| "object".into(), |name| name.to_string());
    PyTypeError::new_err(format!("cannot store a {kind} as a table keyword"))
}

impl Incoming {
    /// A Python value as a keyword value.
    pub(super) fn from_python(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(dict) = obj.cast::<PyDict>() {
            let mut fields = Vec::with_capacity(dict.len());
            for (key, value) in dict.iter() {
                fields.push((key.extract::<String>()?, Self::from_python(&value)?));
            }
            return Ok(Self::Record(fields));
        }
        Wide::from_python(obj).map(Self::Wide)
    }

    /// The value as a new keyword.
    pub(super) fn natural(self) -> Result<Value, Fail> {
        match self {
            Self::Wide(wide) => wide.natural(),
            Self::Record(fields) => {
                let mut record = Vec::with_capacity(fields.len());
                for (name, value) in fields {
                    record.push(RecordField::new(name, value.natural()?));
                }
                Ok(Value::Record(RecordValue::new(record)))
            }
        }
    }

    /// The value as the existing keyword `old` holds one.
    pub(super) fn like(self, old: &Value, name: &str) -> Result<Value, Fail> {
        match (&self, old) {
            (Self::Wide(_), Value::Scalar(_) | Value::Array(_)) => match self {
                Self::Wide(wide) => wide.like(old, name),
                Self::Record(_) => Err(mismatch(name)),
            },
            (Self::Record(_), Value::Record(_)) => self.natural(),
            _ => Err(mismatch(name)),
        }
    }
}

impl Wide {
    fn from_python(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let scalar = |data| Self { shape: None, data };
        if obj.is_instance_of::<PyBool>() {
            return Ok(scalar(Data::Bools(vec![obj.extract()?])));
        }
        if obj.is_instance_of::<PyInt>() {
            return Ok(scalar(Data::Ints(vec![obj.extract()?])));
        }
        if obj.is_instance_of::<PyFloat>() {
            return Ok(scalar(Data::Floats(vec![obj.extract()?], false)));
        }
        if let Ok(number) = obj.cast::<PyComplex>() {
            let value = Complex64::new(number.real(), number.imag());
            return Ok(scalar(Data::Complexes(vec![value], false)));
        }
        if obj.is_instance_of::<PyString>() {
            return Ok(scalar(Data::Texts(vec![obj.extract()?])));
        }
        if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
            let array = obj.py().import("numpy")?.call_method1("asarray", (obj,))?;
            return Self::from_numpy(&array);
        }
        if obj.hasattr("dtype")? && obj.hasattr("shape")? {
            return Self::from_numpy(obj);
        }
        Err(unsupported(obj))
    }

    /// A NumPy array, or a NumPy scalar, by its dtype's kind and size.
    fn from_numpy(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let numpy = obj.py().import("numpy")?;
        let is_array = obj.is_instance(&numpy.getattr("ndarray")?)?;
        let dtype = obj.getattr("dtype")?;
        let kind: String = dtype.getattr("kind")?.extract()?;
        let single: bool = dtype.getattr("itemsize")?.extract::<usize>()? <= 4;
        let (shape, flat) = if is_array {
            let shape: Vec<usize> = obj.getattr("shape")?.extract()?;
            let flat = obj.call_method1("ravel", ("C",))?.call_method0("tolist")?;
            (Some(shape), flat)
        } else {
            (
                None,
                PyList::new(obj.py(), [obj.call_method0("item")?])?.into_any(),
            )
        };
        let data = match kind.as_str() {
            "b" => Data::Bools(flat.extract()?),
            "i" | "u" => Data::Ints(flat.extract()?),
            "f" => Data::Floats(flat.extract()?, single),
            "c" => {
                let mut values = Vec::new();
                for item in flat.try_iter()? {
                    let number = item?.cast_into::<PyComplex>()?;
                    values.push(Complex64::new(number.real(), number.imag()));
                }
                Data::Complexes(values, dtype.getattr("itemsize")?.extract::<usize>()? <= 8)
            }
            "U" | "S" => Data::Texts(flat.extract()?),
            _ => return Err(unsupported(obj)),
        };
        Ok(Self { shape, data })
    }

    fn natural(self) -> Result<Value, Fail> {
        let Self { shape, data } = self;
        match data {
            Data::Bools(values) => build(shape, values, ScalarValue::Bool, ArrayValue::Bool),
            Data::Ints(values) if values.iter().all(|v| i32::try_from(*v).is_ok()) => {
                let narrowed = values.iter().map(|v| *v as i32).collect();
                build(shape, narrowed, ScalarValue::Int32, ArrayValue::Int32)
            }
            Data::Ints(values) => build(shape, values, ScalarValue::Int64, ArrayValue::Int64),
            Data::Floats(values, true) => {
                let narrowed = values.iter().map(|v| *v as f32).collect();
                build(shape, narrowed, ScalarValue::Float32, ArrayValue::Float32)
            }
            Data::Floats(values, false) => {
                build(shape, values, ScalarValue::Float64, ArrayValue::Float64)
            }
            Data::Complexes(values, true) => {
                let narrowed = values
                    .iter()
                    .map(|v| Complex32::new(v.re as f32, v.im as f32))
                    .collect();
                build(
                    shape,
                    narrowed,
                    ScalarValue::Complex32,
                    ArrayValue::Complex32,
                )
            }
            Data::Complexes(values, false) => {
                build(shape, values, ScalarValue::Complex64, ArrayValue::Complex64)
            }
            Data::Texts(values) => build(shape, values, ScalarValue::String, ArrayValue::String),
        }
    }

    /// The value in the type and width of `old`, or the refusal casacore
    /// gives a value of another type.
    fn like(self, old: &Value, name: &str) -> Result<Value, Fail> {
        let Self { shape, data } = self;
        let scalar_old = matches!(old, Value::Scalar(_));
        let single_element = matches!(&shape, Some(shape) if shape.iter().product::<usize>() == 1);
        let shape = match (scalar_old, shape) {
            (true, None) => None,
            (true, Some(_)) if single_element => None,
            (false, Some(shape)) => Some(shape),
            _ => return Err(mismatch(name)),
        };
        let overflow = |value: i64| {
            Fail::Runtime(format!(
                "{value} does not fit the integer keyword {name}, which holds fewer bits"
            ))
        };
        macro_rules! ints {
            ($values:expr, $ty:ty, $scalar:ident, $array:ident) => {{
                let mut narrowed: Vec<$ty> = Vec::with_capacity($values.len());
                for value in &$values {
                    narrowed.push(<$ty>::try_from(*value).map_err(|_| overflow(*value))?);
                }
                build(shape, narrowed, ScalarValue::$scalar, ArrayValue::$array)
            }};
        }
        let kind = element_kind(old);
        match (kind, data) {
            (Kind::Bool, Data::Bools(values)) => {
                build(shape, values, ScalarValue::Bool, ArrayValue::Bool)
            }
            (Kind::UInt8, Data::Ints(values)) => ints!(values, u8, UInt8, UInt8),
            (Kind::UInt16, Data::Ints(values)) => ints!(values, u16, UInt16, UInt16),
            (Kind::UInt32, Data::Ints(values)) => ints!(values, u32, UInt32, UInt32),
            (Kind::Int16, Data::Ints(values)) => ints!(values, i16, Int16, Int16),
            (Kind::Int32, Data::Ints(values)) => ints!(values, i32, Int32, Int32),
            (Kind::Int64, Data::Ints(values)) => {
                build(shape, values, ScalarValue::Int64, ArrayValue::Int64)
            }
            (Kind::Float32, Data::Floats(values, _)) => {
                let narrowed = values.iter().map(|v| *v as f32).collect();
                build(shape, narrowed, ScalarValue::Float32, ArrayValue::Float32)
            }
            (Kind::Float64, Data::Floats(values, _)) => {
                build(shape, values, ScalarValue::Float64, ArrayValue::Float64)
            }
            (Kind::Complex32, Data::Complexes(values, _)) => {
                let narrowed = values
                    .iter()
                    .map(|v| Complex32::new(v.re as f32, v.im as f32))
                    .collect();
                build(
                    shape,
                    narrowed,
                    ScalarValue::Complex32,
                    ArrayValue::Complex32,
                )
            }
            (Kind::Complex64, Data::Complexes(values, _)) => {
                build(shape, values, ScalarValue::Complex64, ArrayValue::Complex64)
            }
            (Kind::Text, Data::Texts(values)) => {
                build(shape, values, ScalarValue::String, ArrayValue::String)
            }
            _ => Err(mismatch(name)),
        }
    }
}

/// An existing keyword's element type.
#[derive(Debug, Clone, Copy)]
enum Kind {
    Bool,
    UInt8,
    UInt16,
    UInt32,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Complex32,
    Complex64,
    Text,
}

fn element_kind(old: &Value) -> Kind {
    match old {
        Value::Scalar(scalar) => match scalar {
            ScalarValue::Bool(_) => Kind::Bool,
            ScalarValue::UInt8(_) => Kind::UInt8,
            ScalarValue::UInt16(_) => Kind::UInt16,
            ScalarValue::UInt32(_) => Kind::UInt32,
            ScalarValue::Int16(_) => Kind::Int16,
            ScalarValue::Int32(_) => Kind::Int32,
            ScalarValue::Int64(_) => Kind::Int64,
            ScalarValue::Float32(_) => Kind::Float32,
            ScalarValue::Float64(_) => Kind::Float64,
            ScalarValue::Complex32(_) => Kind::Complex32,
            ScalarValue::Complex64(_) => Kind::Complex64,
            ScalarValue::String(_) => Kind::Text,
        },
        Value::Array(array) => match array {
            ArrayValue::Bool(_) => Kind::Bool,
            ArrayValue::UInt8(_) => Kind::UInt8,
            ArrayValue::UInt16(_) => Kind::UInt16,
            ArrayValue::UInt32(_) => Kind::UInt32,
            ArrayValue::Int16(_) => Kind::Int16,
            ArrayValue::Int32(_) => Kind::Int32,
            ArrayValue::Int64(_) => Kind::Int64,
            ArrayValue::Float32(_) => Kind::Float32,
            ArrayValue::Float64(_) => Kind::Float64,
            ArrayValue::Complex32(_) => Kind::Complex32,
            ArrayValue::Complex64(_) => Kind::Complex64,
            ArrayValue::String(_) => Kind::Text,
        },
        _ => Kind::Text,
    }
}

/// A scalar from the one element of `values`, or an array of `shape` (in C
/// order: element `[i, j]` of a NumPy array is element `(i, j)` of the
/// casacore array).
pub(super) fn build<T>(
    shape: Option<Vec<usize>>,
    mut values: Vec<T>,
    scalar: fn(T) -> ScalarValue,
    array: fn(ArrayD<T>) -> ArrayValue,
) -> Result<Value, Fail> {
    match shape {
        None => Ok(Value::Scalar(scalar(values.swap_remove(0)))),
        Some(shape) => ArrayD::from_shape_vec(IxDyn(&shape), values)
            .map(|cells| Value::Array(array(cells)))
            .map_err(|error| Fail::Runtime(format!("array of shape {shape:?}: {error}"))),
    }
}
