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

use casa_provider_contracts::ParameterValue;
use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple, PyType};
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
    if value.is_instance(PATH_LIKE.import(py, "os", "PathLike")?)? {
        let path = py.import("os")?.call_method1("fspath", (value,))?;
        let Ok(path) = path.cast::<PyString>() else {
            return Err(PyTypeError::new_err(
                "parameter paths must resolve to text, not bytes",
            ));
        };
        return Ok(ParameterValue::String(path.to_str()?.to_owned()));
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
