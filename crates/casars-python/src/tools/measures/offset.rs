// SPDX-License-Identifier: LGPL-3.0-or-later
//! Inspect scalar measure offsets without losing record values.

use std::collections::HashSet;

use pyo3::exceptions::{PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyComplex, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use super::PyMeasures;
use crate::convert::holder::Measure;
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant};
use crate::tools::LogLine;

const UNSUPPORTED: &str =
    "measures.getoffset array or unsupported record value is not implemented (#1053)";

enum Visit<'py> {
    Enter(Bound<'py, PyAny>, usize),
    Leave(usize),
}

/// Detect recursive records before the shared Python-to-record conversion.
fn validate_containers(value: &Bound<'_, PyAny>) -> PyResult<()> {
    let numpy = value.py().import("numpy")?;
    let ndarray = numpy.getattr("ndarray")?;
    let mut active = HashSet::new();
    let mut pending = vec![Visit::Enter(value.clone(), 0)];
    while let Some(visit) = pending.pop() {
        let (value, depth) = match visit {
            Visit::Leave(id) => {
                active.remove(&id);
                continue;
            }
            Visit::Enter(value, depth) => (value, depth),
        };
        if depth > 128 {
            return Err(PyValueError::new_err(
                "measure record is too deeply nested (#1053)",
            ));
        }
        if value.is_instance(&ndarray)? {
            return Err(PyNotImplementedError::new_err(UNSUPPORTED));
        }
        let entries: Vec<Bound<'_, PyAny>> = if let Ok(dict) = value.cast::<PyDict>() {
            dict.iter().map(|(_, item)| item).collect()
        } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            return Err(PyNotImplementedError::new_err(UNSUPPORTED));
        } else {
            continue;
        };
        let id = value.as_ptr() as usize;
        if !active.insert(id) {
            return Err(PyValueError::new_err(
                "cyclic measure record refused (#1053)",
            ));
        }
        pending.push(Visit::Leave(id));
        pending.extend(
            entries
                .into_iter()
                .map(|entry| Visit::Enter(entry, depth + 1)),
        );
    }
    Ok(())
}

/// Copy the supported scalar record without converting integer values to f64.
fn copy_record<'py>(value: &Bound<'py, PyDict>, depth: usize) -> PyResult<Bound<'py, PyDict>> {
    if depth > 128 {
        return Err(PyValueError::new_err(
            "measure record is too deeply nested (#1053)",
        ));
    }
    let out = PyDict::new(value.py());
    let mut entries = value
        .iter()
        .map(|(key, item)| Ok((key.extract::<String>()?, item)))
        .collect::<PyResult<Vec<_>>>()?;
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    for (key, item) in entries {
        if let Ok(record) = item.cast::<PyDict>() {
            out.set_item(key, copy_record(record, depth + 1)?)?;
        } else if item.is_exact_instance_of::<PyString>()
            || item.is_exact_instance_of::<PyBool>()
            || item.is_exact_instance_of::<PyInt>()
            || item.is_exact_instance_of::<PyFloat>()
            || item.is_exact_instance_of::<PyComplex>()
        {
            out.set_item(key, item)?;
        } else {
            return Err(PyNotImplementedError::new_err(UNSUPPORTED));
        }
    }
    Ok(out)
}

#[pymethods]
impl PyMeasures {
    /// Return a supported scalar measure's offset, or an empty record.
    #[pyo3(signature = (v), text_signature = "($self, v)")]
    fn getoffset(&self, py: Python<'_>, v: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let record = v
            .cast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("_v is not a dictionary"))?;
        validate_containers(v)?;
        // Snapshot the offset before self.call releases the GIL, so concurrent
        // mutation cannot change the record after its validation.
        let copied_offset = match record.get_item("offset")? {
            Some(value) => match value.cast::<PyDict>() {
                Ok(offset) => Some(copy_record(offset, 0)?.into_any().unbind()),
                Err(_) => None,
            },
            None => None,
        };
        let RecordArg(mut fields) = v.extract()?;
        let offset = fields.remove("offset");
        let result = self.call(py, move |_, lines| {
            let base = match Measure::from_record(&fields) {
                Ok(value) => value.measure,
                Err(error) => {
                    let message = error.message();
                    if message.contains("not yet implemented") {
                        return Err(PyNotImplementedError::new_err(format!("{message} (#1053)")));
                    }
                    lines.push(LogLine::warning(format!("{message}Incorrect input type for getoffset()")));
                    return Ok(CasaValue::Bool(false));
                }
            };
            let Some(offset) = offset else {
                lines.push(LogLine::info("Measures does not have an offset"));
                return Ok(CasaValue::Bool(false));
            };
            let Variant::Record(offset) = offset else {
                lines.push(LogLine::error("Incorrect data type for offset"));
                return Ok(CasaValue::Bool(false));
            };
            if offset.contains_key("offset") {
                return Err(PyNotImplementedError::new_err("nested measure offsets are not implemented (#1053)"));
            }
            match Measure::from_record(&offset) {
                Ok(value) if value.measure.kind() == base.kind() => Ok(CasaValue::Bool(true)),
                Ok(_) => { lines.push(LogLine::warning("Unmatched offset type in MeasureHolder::fromRecord\nIncorrect input type for getoffset()")); Ok(CasaValue::Bool(false)) }
                Err(error) => { lines.push(LogLine::warning(format!("{}Incorrect input type for getoffset()",error.message()))); Ok(CasaValue::Bool(false)) }
            }
        })?;
        if !result.bind(py).extract::<bool>()? {
            return Ok(PyDict::new(py).into_any().unbind());
        }
        copied_offset.ok_or_else(|| PyTypeError::new_err("offset is not a dictionary"))
    }
}
