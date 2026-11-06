// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parameter values between Python and the contract's [`ParameterValue`].
//!
//! Python passes a parameter as `bool`, `int`, `float`, `str`, an
//! `os.PathLike` naming text, a `list` or `tuple`, or a mapping with string
//! keys, nested freely; [`parameter_value`] converts it, refusing anything
//! else with a `TypeError`. The Rust runtime then validates the value
//! against the parameter's type. Values come back to Python as the same
//! plain types, a list for an array and a `dict` for a table
//! ([`NativeValue`]).

use std::collections::BTreeMap;

use casa_provider_contracts::{EmptyArrayType, NumpyScalarType, ParameterValue};
use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyComplex, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple, PyType};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

static PATH_LIKE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static MAPPING: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// Convert one Python parameter value.
pub(crate) fn parameter_value(value: &Bound<'_, PyAny>) -> PyResult<ParameterValue> {
    let py = value.py();
    if let Ok(flag) = value.cast::<PyBool>() {
        return Ok(ParameterValue::Bool(flag.is_true()));
    }
    if value.is_instance_of::<PyInt>() {
        return value
            .extract::<i64>()
            .map(ParameterValue::Integer)
            .map_err(|error| {
                if error.is_instance_of::<PyOverflowError>(py) {
                    PyValueError::new_err(format!(
                        "i64 requires {} <= value < {}",
                        i64::MIN,
                        i128::from(i64::MAX) + 1
                    ))
                } else {
                    error
                }
            });
    }
    if value.is_instance_of::<PyFloat>() {
        return value.extract::<f64>().map(ParameterValue::Float);
    }
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(ParameterValue::String(text.to_str()?.to_owned()));
    }
    if value.hasattr("dtype")? && value.is_instance(&py.import("numpy")?.getattr("generic")?)? {
        let dtype = value.getattr("dtype")?;
        let kind: String = dtype.getattr("kind")?.extract()?;
        let size: usize = dtype.getattr("itemsize")?.extract()?;
        let dtype = match (kind.as_str(), size) {
            ("b", _) => NumpyScalarType::Bool,
            ("i", 1) => NumpyScalarType::Int8,
            ("i", 2) => NumpyScalarType::Int16,
            ("i", 4) => NumpyScalarType::Int32,
            ("i", 8) => NumpyScalarType::Int64,
            ("u", 1) => NumpyScalarType::UInt8,
            ("u", 2) => NumpyScalarType::UInt16,
            ("u", 4) => NumpyScalarType::UInt32,
            ("u", 8) => NumpyScalarType::UInt64,
            ("f", 4) => NumpyScalarType::Float32,
            ("c", 8) => NumpyScalarType::Complex64,
            ("c", 16) => NumpyScalarType::Complex128,
            _ => return Err(PyTypeError::new_err("unsupported NumPy parameter dtype")),
        };
        let scalar = parameter_value(&value.call_method0("item")?)?;
        return Ok(ParameterValue::NumpyScalar {
            dtype,
            value: Box::new(scalar),
        });
    }
    if let Ok(value) = value.cast::<PyComplex>() {
        return Ok(ParameterValue::Complex([
            casa_provider_contracts::CasaFloat(value.real()),
            casa_provider_contracts::CasaFloat(value.imag()),
        ]));
    }
    if value.is_instance(PATH_LIKE.import(py, "os", "PathLike")?)? {
        let path = py.import("os")?.call_method1("fspath", (value,))?;
        let Ok(path) = path.cast::<PyString>() else {
            return Err(PyTypeError::new_err(
                "parameter paths must resolve to text, not bytes",
            ));
        };
        return Ok(ParameterValue::String(path.to_str()?.to_owned()));
    }
    if value.hasattr("dtype")? && value.hasattr("shape")? && value.hasattr("tolist")? {
        let dtype = value.getattr("dtype")?;
        let kind: String = dtype.getattr("kind")?.extract()?;
        let size: usize = dtype.getattr("itemsize")?.extract()?;
        let dtype = match (kind.as_str(), size) {
            ("b", _) => EmptyArrayType::Bool,
            ("i" | "u", 1 | 2 | 4 | 8) => EmptyArrayType::Integer,
            ("f", 4 | 8) => EmptyArrayType::Float,
            ("c", 8 | 16) => EmptyArrayType::Complex,
            ("U", _) => EmptyArrayType::String,
            _ => return Err(PyTypeError::new_err("unsupported NumPy parameter dtype")),
        };
        if value.hasattr("size")? && value.getattr("size")?.extract::<usize>()? == 0 {
            return Ok(ParameterValue::EmptyArray {
                dtype,
                shape: value.getattr("shape")?.extract()?,
            });
        }
        let converted = value.call_method0("tolist")?;
        if converted.is(value) || converted.get_type().is(value.get_type()) {
            return Err(PyTypeError::new_err("unsupported NumPy parameter dtype"));
        }
        let converted = parameter_value(&converted)?;
        if value.is_instance(&py.import("numpy")?.getattr("ndarray")?)?
            && value.getattr("ndim")?.extract::<usize>()? == 0
        {
            return Ok(ParameterValue::ScalarArray(Box::new(converted)));
        }
        return Ok(converted);
    }
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        return value
            .try_iter()?
            .map(|item| parameter_value(&item?))
            .collect::<PyResult<Vec<_>>>()
            .map(ParameterValue::Array);
    }
    if value.is_instance(MAPPING.import(py, "collections.abc", "Mapping")?)? {
        let mut entries = BTreeMap::new();
        for item in value.call_method0("items")?.try_iter()? {
            let (key, item) = item?.extract::<(Bound<'_, PyAny>, Bound<'_, PyAny>)>()?;
            let Ok(key) = key.cast::<PyString>() else {
                return Err(PyTypeError::new_err("parameter table keys must be strings"));
            };
            entries.insert(key.to_str()?.to_owned(), parameter_value(&item)?);
        }
        return Ok(ParameterValue::Table(entries));
    }
    Err(PyTypeError::new_err(format!(
        "unsupported parameter value {}",
        value.repr()?
    )))
}

/// Convert a mapping of parameter names to values.
pub(crate) fn parameter_values(
    values: &Bound<'_, PyDict>,
) -> PyResult<BTreeMap<String, ParameterValue>> {
    values
        .iter()
        .map(|(name, value)| Ok((name.extract::<String>()?, parameter_value(&value)?)))
        .collect()
}

/// A [`ParameterValue`] that serializes as its plain value: a boolean,
/// number or string, a sequence for an array and a map for a table.
pub(crate) struct NativeValue<'a>(pub(crate) &'a ParameterValue);

impl Serialize for NativeValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            ParameterValue::Bool(value) => serializer.serialize_bool(*value),
            ParameterValue::Integer(value) => serializer.serialize_i64(*value),
            ParameterValue::Float(value) => serializer.serialize_f64(*value),
            ParameterValue::Complex(value) => value.serialize(serializer),
            ParameterValue::EmptyArray { .. } => serializer.serialize_seq(Some(0))?.end(),
            ParameterValue::ScalarArray(value) => NativeValue(value).serialize(serializer),
            ParameterValue::NumpyScalar { value, .. } => NativeValue(value).serialize(serializer),
            ParameterValue::String(value) => serializer.serialize_str(value),
            ParameterValue::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&NativeValue(value))?;
                }
                sequence.end()
            }
            ParameterValue::Table(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (name, value) in entries {
                    map.serialize_entry(name, &NativeValue(value))?;
                }
                map.end()
            }
        }
    }
}

/// Convert typed parameter values into their natural Python representations.
pub(crate) fn value_to_python<'py>(
    py: Python<'py>,
    value: &ParameterValue,
) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        ParameterValue::Complex([real, imag]) => {
            PyComplex::from_doubles(py, real.0, imag.0).into_any()
        }
        ParameterValue::EmptyArray { dtype, shape } => {
            let dtype = match dtype {
                EmptyArrayType::Bool => "bool",
                EmptyArrayType::Integer => "int64",
                EmptyArrayType::Float => "float64",
                EmptyArrayType::Complex => "complex128",
                EmptyArrayType::String => "U5",
            };
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", dtype)?;
            py.import("numpy")?
                .call_method("empty", (shape.clone(),), Some(&kwargs))?
        }
        ParameterValue::ScalarArray(value) => py
            .import("numpy")?
            .call_method1("array", (value_to_python(py, value)?,))?,
        ParameterValue::NumpyScalar { dtype, value } => {
            let name = format!("{dtype:?}").to_ascii_lowercase();
            py.import("numpy")?
                .getattr(name.as_str())?
                .call1((value_to_python(py, value)?,))?
        }
        ParameterValue::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(value_to_python(py, value)?)?;
            }
            list.into_any()
        }
        ParameterValue::Table(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, value_to_python(py, value)?)?;
            }
            dict.into_any()
        }
        other => pythonize::pythonize(py, &NativeValue(other)).map_err(PyErr::from)?,
    })
}

/// Serialize an optional [`ParameterValue`] as its plain value.
pub(crate) fn serialize_native<S: Serializer>(
    value: &Option<ParameterValue>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => NativeValue(value).serialize(serializer),
        None => serializer.serialize_none(),
    }
}
