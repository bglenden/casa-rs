// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python values as table keywords, the way casatools stores them
//! (`tools/casa-compat/table_tool_oracle.py`, CASA 6.7.6.14).
//!
//! A value arrives in its widest form ([`Incoming`]): a `bool`, `int`,
//! `float`, `complex` or `str`, a NumPy array or scalar, a list (read as the
//! NumPy array it makes), or a `dict` for a sub-record. A new keyword takes
//! the natural type, casatools': an integer is a 32-bit `Int` (a 64-bit one
//! only when a value does not fit, where CASA truncates it to 0), a float a
//! `Double` (also a NumPy `float32`, scalar or array), a complex a `DComplex`
//! (also a `complex64`), a string a `String`, or a table reference when it
//! is `Table: ` and the path of a readable table.
//!
//! An existing keyword keeps its type, as casacore's `defineDataField` does:
//! the new value must be of exactly the type the keyword has (casatools gives
//! a value one of `Bool`, `Int`, `Double`, `DComplex`, `String` or a table,
//! so a keyword of another width takes none), scalar or array. An integer
//! that does not fit an `Int` is refused, where CASA truncates it. A
//! one-element array is accepted for a scalar keyword.

use std::path::Path;

use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};
use num_complex::Complex64;
use numpy::PyReadonlyArrayDyn;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyComplex, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use super::state::Fail;

/// The elements of the NumPy array `array`, in C order, cast to `dtype` (the
/// widest of their kind; a cast that could lose a value, such as `uint64` to
/// `int64`, is refused) and copied in one pass.
fn bulk<T: numpy::Element + Clone>(array: &Bound<'_, PyAny>, dtype: &str) -> PyResult<Vec<T>> {
    let options = PyDict::new(array.py());
    options.set_item("casting", "safe")?;
    options.set_item("copy", false)?;
    let cast = array.call_method("astype", (dtype,), Some(&options))?;
    let typed = cast.extract::<PyReadonlyArrayDyn<'_, T>>()?;
    Ok(typed.as_array().iter().cloned().collect())
}

/// The elements of a value, in the widest type of their kind.
#[derive(Debug, Clone)]
pub(super) enum Data {
    Bools(Vec<bool>),
    Ints(Vec<i64>),
    /// Reals; casatools takes single precision ones as double.
    Floats(Vec<f64>),
    /// Complex numbers; casatools takes single precision ones as double.
    Complexes(Vec<Complex64>),
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

    /// The value as one string, when it is one.
    pub(super) fn text(&self) -> Option<&str> {
        match self {
            Self::Wide(Wide {
                shape: None,
                data: Data::Texts(texts),
            }) if texts.len() == 1 => Some(&texts[0]),
            _ => None,
        }
    }

    /// The value as one integer, as `Record::asInt` takes it: an integer
    /// scalar, and not a real, a Boolean, a string or an array.
    pub(super) fn scalar_integer(&self) -> Option<i64> {
        match self {
            Self::Wide(Wide {
                shape: None,
                data: Data::Ints(ints),
            }) if ints.len() == 1 => Some(ints[0]),
            _ => None,
        }
    }

    /// The value as one Boolean, as `Record::asBool` takes it.
    pub(super) fn scalar_flag(&self) -> Option<bool> {
        match self {
            Self::Wide(Wide {
                shape: None,
                data: Data::Bools(flags),
            }) if flags.len() == 1 => Some(flags[0]),
            _ => None,
        }
    }

    /// The value as integers, as `Record::toArrayInt64` takes it: an integer
    /// or integers, an empty array of any type, and not reals.
    pub(super) fn integer_elements(&self) -> Option<Vec<i64>> {
        match self {
            Self::Wide(Wide {
                data: Data::Ints(ints),
                ..
            }) => Some(ints.clone()),
            Self::Wide(Wide {
                shape: Some(shape), ..
            }) if shape.iter().product::<usize>() == 0 => Some(Vec::new()),
            _ => None,
        }
    }

    /// The fields of a sub-record, when the value is one.
    pub(super) fn fields(&self) -> Option<&[(String, Incoming)]> {
        match self {
            Self::Record(fields) => Some(fields),
            Self::Wide(_) => None,
        }
    }

    /// casacore's name of the value's type, padded to the eight characters
    /// `RecordRep`'s messages give it.
    pub(super) fn type_label(&self) -> &'static str {
        match self {
            Self::Record(_) => "Record  ",
            Self::Wide(Wide { shape: Some(_), .. }) => "Array   ",
            Self::Wide(Wide { data, .. }) => match data {
                Data::Bools(_) => "Bool    ",
                Data::Ints(_) => "Int     ",
                Data::Floats(_) => "Double  ",
                Data::Complexes(_) => "DComplex",
                Data::Texts(_) => "String  ",
            },
        }
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
            (Self::Wide(_), Value::Scalar(_) | Value::Array(_) | Value::TableRef(_)) => {
                match self {
                    Self::Wide(wide) => wide.like(old, name),
                    Self::Record(_) => Err(mismatch(name)),
                }
            }
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
            return Ok(scalar(Data::Floats(vec![obj.extract()?])));
        }
        if let Ok(number) = obj.cast::<PyComplex>() {
            let value = Complex64::new(number.real(), number.imag());
            return Ok(scalar(Data::Complexes(vec![value])));
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
    ///
    /// An array of numbers is copied once, in bulk, in the widest type of
    /// its kind (`bulk`): a large complex `DATA` array is not expanded into
    /// a Python object per element.
    fn from_numpy(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let numpy = obj.py().import("numpy")?;
        let is_array = obj.is_instance(&numpy.getattr("ndarray")?)?;
        let dtype = obj.getattr("dtype")?;
        let kind: String = dtype.getattr("kind")?.extract()?;
        if is_array {
            let shape: Vec<usize> = obj.getattr("shape")?.extract()?;
            let data = match kind.as_str() {
                "b" => Some(Data::Bools(bulk::<bool>(obj, "bool")?)),
                "i" | "u" => Some(Data::Ints(bulk::<i64>(obj, "int64")?)),
                "f" => Some(Data::Floats(bulk::<f64>(obj, "float64")?)),
                "c" => Some(Data::Complexes(bulk::<Complex64>(obj, "complex128")?)),
                _ => None,
            };
            if let Some(data) = data {
                return Ok(Self {
                    shape: Some(shape),
                    data,
                });
            }
        }
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
            "f" => Data::Floats(flat.extract()?),
            "c" => {
                let mut values = Vec::new();
                for item in flat.try_iter()? {
                    let number = item?.cast_into::<PyComplex>()?;
                    values.push(Complex64::new(number.real(), number.imag()));
                }
                Data::Complexes(values)
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
            Data::Floats(values) => build(shape, values, ScalarValue::Float64, ArrayValue::Float64),
            Data::Complexes(values) => {
                build(shape, values, ScalarValue::Complex64, ArrayValue::Complex64)
            }
            // A string that names a readable table is a reference to it
            // (`TableRecord::defineFromValueHolder`); any other is a string.
            Data::Texts(mut values) if shape.is_none() => {
                let text = values.swap_remove(0);
                Ok(match table_reference(&text) {
                    Some(path) => Value::TableRef(path.to_string()),
                    None => Value::Scalar(ScalarValue::String(text)),
                })
            }
            Data::Texts(values) => build(shape, values, ScalarValue::String, ArrayValue::String),
        }
    }

    /// The value as the keyword `old` holds it, or the refusal casacore
    /// gives a value of another type. `defineDataField` takes a value of
    /// exactly the type the field has, and casatools gives every value one
    /// of five (`Bool`, `Int`, `Double`, `DComplex`, `String`, or a table),
    /// so a field of another width (`Float`, `Short`, ...) takes none. The
    /// exception is an integer beyond 32 bits, which casa-rs keeps (casatools
    /// truncates it to 0) in a field of 64 bits.
    fn like(self, old: &Value, name: &str) -> Result<Value, Fail> {
        let Self { shape, data } = self;
        if let Value::TableRef(_) = old {
            return match (&shape, &data) {
                (None, Data::Texts(values)) => table_reference(&values[0])
                    .map(|path| Value::TableRef(path.to_string()))
                    .ok_or_else(|| mismatch(name)),
                _ => Err(mismatch(name)),
            };
        }
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
        match (element_kind(old), data) {
            (Kind::Bool, Data::Bools(values)) => {
                build(shape, values, ScalarValue::Bool, ArrayValue::Bool)
            }
            (Kind::Int32, Data::Ints(values)) => ints!(values, i32, Int32, Int32),
            (Kind::Int64, Data::Ints(values))
                if values.iter().any(|v| i32::try_from(*v).is_err()) =>
            {
                build(shape, values, ScalarValue::Int64, ArrayValue::Int64)
            }
            (Kind::Float64, Data::Floats(values)) => {
                build(shape, values, ScalarValue::Float64, ArrayValue::Float64)
            }
            (Kind::Complex64, Data::Complexes(values)) => {
                build(shape, values, ScalarValue::Complex64, ArrayValue::Complex64)
            }
            // A string that names a readable table is a table to casacore, and
            // a table takes no string field's place.
            (Kind::Text, Data::Texts(values))
                if shape.is_some() || table_reference(&values[0]).is_none() =>
            {
                build(shape, values, ScalarValue::String, ArrayValue::String)
            }
            _ => Err(mismatch(name)),
        }
    }
}

/// The path of the table a string names as casacore reads it
/// (`TableRecord::defineFromValueHolder`): `Table: ` and the path of a
/// readable table.
fn table_reference(text: &str) -> Option<&str> {
    let path = text.strip_prefix("Table: ")?;
    std::fs::File::open(Path::new(path).join("table.dat"))
        .ok()
        .map(|_| path)
}

/// The type of an existing keyword, as far as a Python value can take its
/// place.
#[derive(Debug, Clone, Copy)]
enum Kind {
    Bool,
    Int32,
    Int64,
    Float64,
    Complex64,
    Text,
    /// Any other width (`Float`, `Short`, ...): casatools gives no value
    /// that type.
    Other,
}

fn element_kind(old: &Value) -> Kind {
    match old {
        Value::Scalar(scalar) => match scalar {
            ScalarValue::Bool(_) => Kind::Bool,
            ScalarValue::Int32(_) => Kind::Int32,
            ScalarValue::Int64(_) => Kind::Int64,
            ScalarValue::Float64(_) => Kind::Float64,
            ScalarValue::Complex64(_) => Kind::Complex64,
            ScalarValue::String(_) => Kind::Text,
            _ => Kind::Other,
        },
        Value::Array(array) => match array {
            ArrayValue::Bool(_) => Kind::Bool,
            ArrayValue::Int32(_) => Kind::Int32,
            ArrayValue::Int64(_) => Kind::Int64,
            ArrayValue::Float64(_) => Kind::Float64,
            ArrayValue::Complex64(_) => Kind::Complex64,
            ArrayValue::String(_) => Kind::Text,
            _ => Kind::Other,
        },
        _ => Kind::Other,
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

#[cfg(test)]
mod tests {
    //! The types a keyword is stored with (Astra's review of #1034, findings
    //! 7 and 8). A reader that compares only the values a Python call returns
    //! cannot see them: casatools promotes `float32` to `Double` on the way
    //! back too.

    use super::*;
    use num_complex::Complex32;

    fn scalar(data: Data) -> Wide {
        Wide { shape: None, data }
    }

    fn array(shape: &[usize], data: Data) -> Wide {
        Wide {
            shape: Some(shape.to_vec()),
            data,
        }
    }

    fn text(value: &str) -> Wide {
        scalar(Data::Texts(vec![value.to_string()]))
    }

    /// A directory that is a readable table: it has a `table.dat`.
    fn readable_table() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("table.dat"), b"").expect("table.dat");
        dir
    }

    #[test]
    fn a_new_keyword_takes_casatools_type_not_the_width_of_the_numpy_value() {
        // np.float32(1.25) arrives as a real, a float32 array as an array of
        // reals: CASA 6.7.6.14 stores TpDouble and TpArrayDouble.
        assert_eq!(
            scalar(Data::Floats(vec![1.25])).natural().unwrap(),
            Value::Scalar(ScalarValue::Float64(1.25))
        );
        let reals = array(&[2], Data::Floats(vec![1.0, 2.0])).natural().unwrap();
        assert!(
            matches!(reals, Value::Array(ArrayValue::Float64(_))),
            "{reals:?}"
        );
        let complexes = array(&[1], Data::Complexes(vec![Complex64::new(1.0, 2.0)]))
            .natural()
            .unwrap();
        assert!(
            matches!(complexes, Value::Array(ArrayValue::Complex64(_))),
            "{complexes:?}"
        );
        // An integer is an Int unless it does not fit.
        assert_eq!(
            scalar(Data::Ints(vec![7])).natural().unwrap(),
            Value::Scalar(ScalarValue::Int32(7))
        );
        assert_eq!(
            scalar(Data::Ints(vec![1 << 40])).natural().unwrap(),
            Value::Scalar(ScalarValue::Int64(1 << 40))
        );
    }

    #[test]
    fn a_keyword_takes_a_value_of_exactly_its_type() {
        let double = Value::Scalar(ScalarValue::Float64(1.0));
        assert_eq!(
            scalar(Data::Floats(vec![2.5])).like(&double, "k").unwrap(),
            Value::Scalar(ScalarValue::Float64(2.5))
        );
        // casatools gives no value the type Float (or Short, ...), so a
        // keyword of that type takes none, as defineDataField refuses.
        for old in [
            Value::Scalar(ScalarValue::Float32(1.0)),
            Value::Scalar(ScalarValue::Int16(1)),
            Value::Scalar(ScalarValue::UInt8(1)),
            Value::Scalar(ScalarValue::Complex32(Complex32::new(1.0, 0.0))),
        ] {
            let value = match old {
                Value::Scalar(ScalarValue::Float32(_)) => Data::Floats(vec![2.5]),
                Value::Scalar(ScalarValue::Complex32(_)) => {
                    Data::Complexes(vec![Complex64::new(1.0, 1.0)])
                }
                _ => Data::Ints(vec![2]),
            };
            let refused = scalar(value).like(&old, "k").unwrap_err();
            assert!(
                matches!(&refused, Fail::Runtime(m) if m.contains("incorrect data type")),
                "{old:?}: {refused:?}"
            );
        }
        // A 64-bit keyword takes only an integer beyond 32 bits, the
        // exception to the rule; a small one is an Int to casacore.
        let wide = Value::Scalar(ScalarValue::Int64(1));
        assert!(scalar(Data::Ints(vec![7])).like(&wide, "k").is_err());
        assert_eq!(
            scalar(Data::Ints(vec![1 << 40])).like(&wide, "k").unwrap(),
            Value::Scalar(ScalarValue::Int64(1 << 40))
        );
        // A 32-bit keyword refuses one that does not fit, where CASA wraps.
        let int = Value::Scalar(ScalarValue::Int32(1));
        assert!(scalar(Data::Ints(vec![1 << 40])).like(&int, "k").is_err());
    }

    #[test]
    fn a_string_naming_a_readable_table_is_a_table_reference() {
        let table = readable_table();
        let named = format!("Table: {}", table.path().display());
        assert_eq!(
            text(&named).natural().unwrap(),
            Value::TableRef(table.path().display().to_string())
        );
        // Not readable, or not so named: an ordinary string.
        for plain in ["Table: /no/such/table", "plain", "table: x"] {
            assert_eq!(
                text(plain).natural().unwrap(),
                Value::Scalar(ScalarValue::String(plain.to_string()))
            );
        }
        // A string array is never a table.
        assert!(matches!(
            array(&[1], Data::Texts(vec![named.clone()]))
                .natural()
                .unwrap(),
            Value::Array(ArrayValue::String(_))
        ));

        // An existing reference takes another readable one and nothing else.
        let old = Value::TableRef("././ANTENNA".to_string());
        assert_eq!(
            text(&named).like(&old, "k").unwrap(),
            Value::TableRef(table.path().display().to_string())
        );
        assert!(text("plain text").like(&old, "k").is_err());
        assert!(scalar(Data::Ints(vec![5])).like(&old, "k").is_err());
        // And an existing string takes a string, not a table.
        let string = Value::Scalar(ScalarValue::String("x".into()));
        assert!(text(&named).like(&string, "k").is_err());
        assert!(text("other").like(&string, "k").is_ok());
    }
}
